package latticedb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type csvGenerationManifest struct {
	Generation string `json:"generation"`
	Nodes      string `json:"nodes"`
	Edges      string `json:"edges"`
}

func exportCSVGeneration(t *testing.T, db *DB, output string) csvGenerationManifest {
	t.Helper()
	data, err := db.Export(ExportFormatCSV, output)
	if err != nil {
		t.Fatal(err)
	}
	var manifest csvGenerationManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestCSVGenerationLeasesProtectReadersAndPruneOldGenerations(t *testing.T) {
	requireCSVPruning(t)
	db := openExportLimitDB(t)
	output := filepath.Join(t.TempDir(), "graph.csv")
	first := exportCSVGeneration(t, db, output)
	lease, err := OpenCSVGenerationContext(context.Background(), output)
	if err != nil {
		if errors.Is(err, ErrCSVGenerationPruningUnsupported) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	if lease.Generation != first.Generation {
		t.Fatalf("lease generation = %q, want %q", lease.Generation, first.Generation)
	}
	_ = exportCSVGeneration(t, db, output)
	current := exportCSVGeneration(t, db, output)
	removed, err := PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{KeepLatest: 1})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed with active lease = %d", removed)
	}
	if _, err := os.Stat(lease.NodesPath); err != nil {
		t.Fatalf("leased generation removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(output), current.Nodes)); err != nil {
		t.Fatalf("current generation removed: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	removed, err = PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{KeepLatest: 1})
	if err != nil || removed != 1 {
		t.Fatalf("removed after lease close = %d, %v", removed, err)
	}
}

func TestCSVGenerationLeaseCloseIsConcurrentAndIdempotent(t *testing.T) {
	db := openExportLimitDB(t)
	output := filepath.Join(t.TempDir(), "graph.csv")
	_ = exportCSVGeneration(t, db, output)
	lease, err := OpenCSVGenerationContext(context.Background(), output)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- lease.Close()
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCSVGenerationLeaseOwnerDeathReleasesGeneration(t *testing.T) {
	if os.Getenv("LATTICEDB_CSV_LEASE_HELPER") != "" {
		lease, err := OpenCSVGenerationContext(context.Background(), os.Getenv("LATTICEDB_CSV_LEASE_MANIFEST"))
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Close()
		if err := os.WriteFile(os.Getenv("LATTICEDB_CSV_LEASE_READY"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "js" || runtime.GOOS == "plan9" || runtime.GOOS == "wasip1" {
		t.Skip("CSV generation pruning is unsupported on this platform")
	}
	db := openExportLimitDB(t)
	output := filepath.Join(t.TempDir(), "graph.csv")
	_ = exportCSVGeneration(t, db, output)
	ready := filepath.Join(t.TempDir(), "ready")
	child := exec.Command(os.Args[0], "-test.run=^TestCSVGenerationLeaseOwnerDeathReleasesGeneration$")
	child.Env = append(os.Environ(), "LATTICEDB_CSV_LEASE_HELPER=1", "LATTICEDB_CSV_LEASE_MANIFEST="+output, "LATTICEDB_CSV_LEASE_READY="+ready)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease child did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	first, err := OpenCSVGenerationContext(context.Background(), output)
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenCSVGenerationContext(context.Background(), output)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = exportCSVGeneration(t, db, output)
	_ = exportCSVGeneration(t, db, output)
	prune := func(want int) {
		t.Helper()
		removed, err := PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{KeepLatest: 1})
		if err != nil || removed != want {
			t.Fatalf("prune removed=%d err=%v want=%d", removed, err, want)
		}
	}
	prune(1)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	prune(0)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	prune(0)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	prune(1)
}

func TestCSVGenerationPruneAgeAndUnsafeLayout(t *testing.T) {
	requireCSVPruning(t)
	db := openExportLimitDB(t)
	output := filepath.Join(t.TempDir(), "graph.csv")
	old := exportCSVGeneration(t, db, output)
	oldTime := time.Now().Add(-2 * time.Hour)
	for _, path := range []string{filepath.Join(filepath.Dir(output), old.Nodes), filepath.Join(filepath.Dir(output), old.Edges)} {
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
	}
	_ = exportCSVGeneration(t, db, output)
	removed, err := PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{MinAge: time.Hour})
	if errors.Is(err, ErrCSVGenerationPruningUnsupported) {
		t.Skip(err)
	}
	if err != nil || removed != 1 {
		t.Fatalf("age prune = %d, %v", removed, err)
	}
	unsafe := filepath.Join(output+"_generations", "generation-unsafe")
	if err := os.Mkdir(unsafe, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{KeepLatest: 1}); err == nil {
		t.Fatalf("unsafe layout error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PruneCSVGenerationsContext(canceled, output, CSVGenerationRetention{KeepLatest: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled prune = %v", err)
	}
}

func requireCSVPruning(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" || runtime.GOOS == "js" || runtime.GOOS == "plan9" || runtime.GOOS == "wasip1" {
		if _, err := PruneCSVGenerationsContext(context.Background(), "", CSVGenerationRetention{KeepLatest: 1}); !errors.Is(err, ErrCSVGenerationPruningUnsupported) {
			t.Fatalf("unsupported prune = %v", err)
		}
		t.Skip("native durable pruning unsupported")
	}
}

func TestCSVGenerationRetentionValidatesBounds(t *testing.T) {
	requireCSVPruning(t)
	db := openExportLimitDB(t)
	output := filepath.Join(t.TempDir(), "graph.csv")
	exportCSVGeneration(t, db, output)
	if _, err := PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{KeepLatest: 1, MinAge: -1}); err == nil {
		t.Fatal("negative age accepted")
	}
	if n, err := PruneCSVGenerationsContext(context.Background(), output, CSVGenerationRetention{KeepLatest: ^uint(0)}); err != nil || n != 0 {
		t.Fatalf("large keep = %d, %v", n, err)
	}
}
