package exporter

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestSingleFileExportsPublishWithoutResultBuffer(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Labels: []string{"Item"}})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2})
	graph.Edges.Set(1, &store.EdgeRecord{ID: 1, SourceID: 1, TargetID: 2, Type: "LINK"})

	for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatDOT} {
		t.Run(string(format), func(t *testing.T) {
			var want bytes.Buffer
			if err := ExportGraphContextTo(context.Background(), graph, format, &want); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "graph."+string(format))
			if err := ExportGraphFileContext(context.Background(), graph, format, path); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, want.Bytes()) {
				t.Fatalf("published output differs:\n%s\n%s", data, want.Bytes())
			}
		})
	}
}

func TestExportGraphPreservesReturnedBytes(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1})
	for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatDOT} {
		t.Run(string(format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "graph."+string(format))
			data, err := ExportGraph(graph, format, path)
			if err != nil {
				t.Fatal(err)
			}
			written, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, written) {
				t.Fatal("ExportGraph return value no longer matches the published file")
			}
		})
	}
}

func TestCSVConcurrentSubprocessPublicationAndOwnerDeath(t *testing.T) {
	if os.Getenv("LATTICEDB_EXPORT_HELPER") != "" {
		runExportHelper(t)
		return
	}
	output := filepath.Join(t.TempDir(), "graph.csv")
	commands := make([]*exec.Cmd, 3)
	for index := range commands {
		commands[index] = exportHelperCommand(t, "export", output, "")
		if err := commands[index].Start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("export helper: %v", err)
		}
	}
	assertCSVManifestPathsExist(t, output)

	ready := filepath.Join(t.TempDir(), "ready")
	holder := exportHelperCommand(t, "hold", output, ready)
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = holder.Process.Kill()
			t.Fatal("lock holder did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	manifestBefore, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	graph := store.NewGraphState()
	if _, err := ExportGraphContext(ctx, graph, ExportFormatCSV, output); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context waiter error = %v", err)
	}
	manifestAfter, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestAfter) != string(manifestBefore) {
		t.Fatal("cancelled waiter changed the published manifest")
	}
	waiter := exportHelperCommand(t, "export", output, "")
	if err := waiter.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- waiter.Wait() }()
	select {
	case err := <-waited:
		t.Fatalf("waiter did not block on the OS lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.Wait()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not recover after lock owner death")
	}
	assertCSVManifestPathsExist(t, output)
}

func TestExportNamespacesAndPublicationContracts(t *testing.T) {
	base := t.TempDir()
	firstOutput := filepath.Join(base, "graph.csv")
	secondOutput := filepath.Join(base, "graph.backup")
	first := store.NewGraphState()
	first.Nodes.Set(1, &store.NodeRecord{ID: 1, Labels: []string{"A|B"}})
	second := store.NewGraphState()
	second.Nodes.Set(2, &store.NodeRecord{ID: 2, Labels: []string{"A", "B"}})

	firstManifest, err := ExportGraph(first, ExportFormatCSV, firstOutput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ExportGraph(second, ExportFormatCSV, secondOutput); err != nil {
		t.Fatal(err)
	}
	var manifest csvManifest
	if err := json.Unmarshal(firstManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{manifest.Nodes, manifest.Edges} {
		if _, err := os.Stat(filepath.Join(base, path)); err != nil {
			t.Fatalf("first manifest was invalidated by same-stem export: %v", err)
		}
	}

	stale := filepath.Join(firstOutput+"_generations", ".building-stale")
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := ExportGraph(first, ExportFormatCSV, firstOutput); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale build was not removed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(base, manifest.Nodes))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var labels []string
	if err := json.Unmarshal([]byte(rows[1][1]), &labels); err != nil {
		t.Fatalf("labels are not an unambiguous JSON array: %v", err)
	}
	if len(labels) != 1 || labels[0] != "A|B" {
		t.Fatalf("labels changed during CSV export: %#v", labels)
	}
}

func TestAllFileExportsHonorPathLock(t *testing.T) {
	output := filepath.Join(t.TempDir(), "shared.out")
	unlock, err := acquireExportLock(output)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := ExportGraphContext(ctx, store.NewGraphState(), ExportFormatJSON, output); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("JSON writer bypassed common output lock: %v", err)
	}
}

func TestPublishReportsRenameBeforeDirectorySyncFailure(t *testing.T) {
	directory := t.TempDir()
	temp := filepath.Join(directory, "temp")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(temp, []byte("published"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("sync failed")
	published, err := publishTempOutputWithSync(temp, target, func(string) error { return wantErr })
	if !published || !errors.Is(err, wantErr) {
		t.Fatalf("published=%v error=%v", published, err)
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "published" {
		t.Fatalf("renamed output missing after sync failure: %q, %v", data, err)
	}
}

func TestExportPathLockRegistryReclaimsEntries(t *testing.T) {
	base := t.TempDir()
	for index := 0; index < 256; index++ {
		unlock, err := acquireExportLock(filepath.Join(base, fmt.Sprintf("path-%d", index)))
		if err != nil {
			t.Fatal(err)
		}
		unlock()
	}
	exportPathLocks.Lock()
	entries := len(exportPathLocks.entries)
	exportPathLocks.Unlock()
	if entries != 0 {
		t.Fatalf("registry retained %d high-cardinality entries", entries)
	}
	path := filepath.Join(base, "shared")
	unlock, err := acquireExportLock(path)
	if err != nil {
		t.Fatal(err)
	}
	waiter, waiterCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waiterCancel()
	if _, err := acquireExportLockContext(waiter, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v", err)
	}
	exportPathLocks.Lock()
	entries, refs := len(exportPathLocks.entries), exportPathLocks.entries[path].refs
	exportPathLocks.Unlock()
	if entries != 1 || refs != 1 {
		t.Fatalf("registry while owned: entries=%d refs=%d", entries, refs)
	}
	unlock()
	exportPathLocks.Lock()
	entries = len(exportPathLocks.entries)
	exportPathLocks.Unlock()
	if entries != 0 {
		t.Fatalf("registry retained %d entries", entries)
	}
}

func TestExportContextCancelsBodyBeforeManifest(t *testing.T) {
	output := filepath.Join(t.TempDir(), "cancel-body.csv")
	base := store.NewGraphState()
	base.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"version": int64(1)}})
	if _, err := ExportGraph(base, ExportFormatCSV, output); err != nil {
		t.Fatal(err)
	}
	manifestBefore, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	large := store.NewGraphState()
	for id := uint64(1); id <= 2_000; id++ {
		large.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"value": int64(id)}})
	}
	ctx := &cancelExportAfterChecks{limit: 20}
	if _, err := ExportGraphContext(ctx, large, ExportFormatCSV, output); !errors.Is(err, context.Canceled) {
		t.Fatalf("body cancellation error = %v", err)
	}
	manifestAfter, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestAfter) != string(manifestBefore) {
		t.Fatal("canceled body changed the manifest")
	}
	building, err := filepath.Glob(filepath.Join(output+"_generations", ".building-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(building) != 0 {
		t.Fatalf("canceled body retained building directories: %v", building)
	}
	if _, err := DumpGraphContext(&cancelExportAfterChecks{limit: 20}, large); !errors.Is(err, context.Canceled) {
		t.Fatalf("dump cancellation error = %v", err)
	}
}

type cancelExportAfterChecks struct {
	checks atomic.Int32
	limit  int32
}

func (*cancelExportAfterChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelExportAfterChecks) Done() <-chan struct{}       { return nil }
func (ctx *cancelExportAfterChecks) Err() error {
	if ctx.checks.Add(1) >= ctx.limit {
		return context.Canceled
	}
	return nil
}
func (*cancelExportAfterChecks) Value(any) any { return nil }

func exportHelperCommand(t *testing.T, mode, output, ready string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCSVConcurrentSubprocessPublicationAndOwnerDeath$")
	command.Env = append(os.Environ(), "LATTICEDB_EXPORT_HELPER="+mode, "LATTICEDB_EXPORT_OUTPUT="+output, "LATTICEDB_EXPORT_READY="+ready)
	return command
}

func runExportHelper(t *testing.T) {
	output := os.Getenv("LATTICEDB_EXPORT_OUTPUT")
	if os.Getenv("LATTICEDB_EXPORT_HELPER") == "hold" {
		unlock, err := acquireExportLock(output)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		if err := os.WriteFile(os.Getenv("LATTICEDB_EXPORT_READY"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{}})
	if _, err := ExportGraph(graph, ExportFormatCSV, output); err != nil {
		t.Fatal(err)
	}
}

func assertCSVManifestPathsExist(t *testing.T, output string) {
	t.Helper()
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var manifest csvManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{manifest.Nodes, manifest.Edges} {
		if _, err := os.Stat(filepath.Join(filepath.Dir(output), path)); err != nil {
			t.Fatal(err)
		}
	}
}
