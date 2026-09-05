package latticedb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func TestCSVGenerationPruneAgeAndUnsafeLayout(t *testing.T) {
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
