package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrCSVGenerationPruningUnsupported = errors.New("CSV generation pruning is unsupported on this platform")
	ErrInvalidCSVGeneration            = errors.New("invalid CSV generation")
)

type CSVGenerationRetention struct {
	KeepLatest uint
	MinAge     time.Duration
}

type CSVGenerationLease struct {
	Generation string
	NodesPath  string
	EdgesPath  string

	mu   sync.Mutex
	file *os.File
	info os.FileInfo
}

var csvGenerationLeases = struct {
	sync.Mutex
	entries map[*CSVGenerationLease]struct{}
}{entries: make(map[*CSVGenerationLease]struct{})}

func OpenCSVGenerationContext(ctx context.Context, manifestPath string) (*CSVGenerationLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	manifestPath, err := canonicalExportOutputPath(manifestPath)
	if err != nil {
		return nil, err
	}
	unlock, err := acquireExportLockContext(ctx, manifestPath)
	if err != nil {
		return nil, err
	}
	defer unlock()
	generation, err := readCSVGeneration(manifestPath)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(generation.path, ".lease-")
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil {
		var locked bool
		locked, err = tryLockExportFile(file)
		if err == nil && !locked {
			err = errors.New("lock CSV generation lease")
		}
	}
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	lease := &CSVGenerationLease{Generation: generation.name, NodesPath: generation.nodes, EdgesPath: generation.edges, file: file, info: info}
	csvGenerationLeases.Lock()
	csvGenerationLeases.entries[lease] = struct{}{}
	csvGenerationLeases.Unlock()
	return lease, nil
}

func (lease *CSVGenerationLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	file := lease.file
	lease.file = nil
	if file == nil {
		return nil
	}
	csvGenerationLeases.Lock()
	defer csvGenerationLeases.Unlock()
	delete(csvGenerationLeases.entries, lease)
	_ = unlockExportFile(file)
	err := file.Close()
	removeErr := os.Remove(file.Name())
	if err != nil {
		return err
	}
	return removeErr
}

func PruneCSVGenerationsContext(ctx context.Context, manifestPath string, retention CSVGenerationRetention) (int, error) {
	if !csvGenerationPruningSupported() {
		return 0, ErrCSVGenerationPruningUnsupported
	}
	if retention.MinAge < 0 {
		return 0, errors.New("CSV generation MinAge must not be negative")
	}
	if retention.KeepLatest == 0 && retention.MinAge == 0 {
		return 0, errors.New("CSV generation pruning requires KeepLatest or MinAge")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	manifestPath, err := canonicalExportOutputPath(manifestPath)
	if err != nil {
		return 0, err
	}
	unlock, err := acquireExportLockContext(ctx, manifestPath)
	if err != nil {
		return 0, err
	}
	defer unlock()
	current, err := readCSVGeneration(manifestPath)
	if err != nil {
		return 0, err
	}
	if err := syncCSVGenerationForPrune(manifestPath, current); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(current.root)
	if err != nil {
		return 0, err
	}
	var generations []csvGeneration
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".building-") {
			continue
		}
		if !strings.HasPrefix(name, "generation-") {
			return 0, fmt.Errorf("%w: unexpected namespace entry %q", ErrInvalidCSVGeneration, name)
		}
		generation, err := inspectCSVGeneration(current.root, name)
		if err != nil {
			return 0, err
		}
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(left, right int) bool {
		if generations[left].mtime.Equal(generations[right].mtime) {
			return generations[left].name > generations[right].name
		}
		return generations[left].mtime.After(generations[right].mtime)
	})
	now := time.Now()
	removed := 0
	for index, generation := range generations {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if generation.name == current.name || uint(index) < retention.KeepLatest {
			continue
		}
		if retention.MinAge > 0 && now.Sub(generation.mtime) < retention.MinAge {
			continue
		}
		active, err := cleanupCSVLeases(generation.path)
		if err != nil {
			return removed, err
		}
		if active {
			continue
		}
		if err := os.RemoveAll(generation.path); err != nil {
			return removed, err
		}
		removed++
	}
	if removed != 0 {
		if err := syncOutputDirectory(current.root); err != nil {
			return removed, err
		}
		if err := syncOutputDirectory(filepath.Dir(manifestPath)); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

type csvGeneration struct {
	name         string
	root, path   string
	nodes, edges string
	mtime        time.Time
}

func readCSVGeneration(manifestPath string) (csvGeneration, error) {
	file, err := os.Open(manifestPath)
	if err != nil {
		return csvGeneration{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 4097))
	closeErr := file.Close()
	if readErr != nil {
		return csvGeneration{}, readErr
	}
	if closeErr != nil {
		return csvGeneration{}, closeErr
	}
	if len(data) > 4096 {
		return csvGeneration{}, fmt.Errorf("%w: manifest exceeds 4096 bytes", ErrInvalidCSVGeneration)
	}
	var manifest csvManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return csvGeneration{}, fmt.Errorf("%w: manifest JSON", ErrInvalidCSVGeneration)
	}
	root := manifestPath + "_generations"
	generation, err := inspectCSVGeneration(root, manifest.Generation)
	if err != nil {
		return csvGeneration{}, err
	}
	wantNodes := filepath.Join(filepath.Base(root), generation.name, "nodes.csv")
	wantEdges := filepath.Join(filepath.Base(root), generation.name, "edges.csv")
	if manifest.Nodes != wantNodes || manifest.Edges != wantEdges {
		return csvGeneration{}, fmt.Errorf("%w: manifest paths", ErrInvalidCSVGeneration)
	}
	return generation, nil
}

func inspectCSVGeneration(root, name string) (csvGeneration, error) {
	if filepath.Base(name) != name || !strings.HasPrefix(name, "generation-") || len(name) == len("generation-") {
		return csvGeneration{}, fmt.Errorf("%w: generation name", ErrInvalidCSVGeneration)
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return csvGeneration{}, fmt.Errorf("%w: generation root", ErrInvalidCSVGeneration)
	}
	path := filepath.Join(root, name)
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return csvGeneration{}, fmt.Errorf("%w: generation directory", ErrInvalidCSVGeneration)
	}
	nodes := filepath.Join(path, "nodes.csv")
	edges := filepath.Join(path, "edges.csv")
	nodesInfo, err := regularCSVGenerationFile(nodes)
	if err != nil {
		return csvGeneration{}, err
	}
	edgesInfo, err := regularCSVGenerationFile(edges)
	if err != nil {
		return csvGeneration{}, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return csvGeneration{}, err
	}
	for _, entry := range entries {
		if entry.Name() == "nodes.csv" || entry.Name() == "edges.csv" {
			continue
		}
		if !strings.HasPrefix(entry.Name(), ".lease-") {
			return csvGeneration{}, fmt.Errorf("%w: unexpected generation entry %q", ErrInvalidCSVGeneration, entry.Name())
		}
		leaseInfo, err := os.Lstat(filepath.Join(path, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !leaseInfo.Mode().IsRegular() || leaseInfo.Mode()&os.ModeSymlink != 0 {
			return csvGeneration{}, fmt.Errorf("%w: lease file", ErrInvalidCSVGeneration)
		}
	}
	mtime := nodesInfo.ModTime()
	if edgesInfo.ModTime().After(mtime) {
		mtime = edgesInfo.ModTime()
	}
	return csvGeneration{name: name, root: root, path: path, nodes: nodes, edges: edges, mtime: mtime}, nil
}

func regularCSVGenerationFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: regular generation file", ErrInvalidCSVGeneration)
	}
	multiple, err := exportOutputHasMultipleLinks(path, info)
	if err != nil || multiple {
		return nil, fmt.Errorf("%w: linked generation file", ErrInvalidCSVGeneration)
	}
	return info, nil
}

func cleanupCSVLeases(generationPath string) (bool, error) {
	csvGenerationLeases.Lock()
	defer csvGenerationLeases.Unlock()
	entries, err := os.ReadDir(generationPath)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".lease-") {
			continue
		}
		path := filepath.Join(generationPath, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("%w: lease file", ErrInvalidCSVGeneration)
		}
		activeHere := false
		for lease := range csvGenerationLeases.entries {
			if os.SameFile(info, lease.info) {
				activeHere = true
				break
			}
		}
		if activeHere {
			return true, nil
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return false, err
		}
		locked, lockErr := tryLockExportFile(file)
		if lockErr != nil {
			_ = file.Close()
			return false, lockErr
		}
		if !locked {
			_ = file.Close()
			return true, nil
		}
		_ = unlockExportFile(file)
		if err := file.Close(); err != nil {
			return false, err
		}
		if err := os.Remove(path); err != nil {
			return false, err
		}
	}
	return false, nil
}

func syncCSVGenerationForPrune(manifestPath string, current csvGeneration) error {
	for _, path := range []string{manifestPath, current.nodes, current.edges} {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		err = file.Sync()
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	for _, path := range []string{current.path, current.root, filepath.Dir(manifestPath)} {
		if err := syncOutputDirectory(path); err != nil {
			return err
		}
	}
	return nil
}
