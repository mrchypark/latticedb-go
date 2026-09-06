package latticedb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	internalexporter "github.com/mrchypark/latticedb-go/internal/exporter"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestExportRejectsDatabaseOwnedDestinationsBeforePublication(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "source.ltdb")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	for _, output := range []string{filepath.Join(dbPath, "state.json"), filepath.Join(dbPath, "state.json.layout")} {
		t.Run(filepath.Base(output), func(t *testing.T) {
			before, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			checks := []struct {
				name   string
				want   error
				export func() error
			}{
				{"db export", ErrInvalidArgument, func() error { _, err := db.Export(ExportFormatJSON, output); return err }},
				{"db file export", ErrInvalidArgument, func() error { return db.ExportFileContext(context.Background(), ExportFormatJSON, output) }},
				{"path export", ErrInvalidArgument, func() error { _, err := Export(dbPath, ExportFormatJSON, output); return err }},
				{"path file export", ErrInvalidArgument, func() error { return ExportFile(dbPath, ExportFormatJSON, output) }},
				{"internal path export", store.ErrExportDestinationConflict, func() error {
					_, err := internalexporter.Export(dbPath, internalexporter.ExportFormatJSON, output)
					return err
				}},
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					err := check.export()
					if err == nil || !errors.Is(err, check.want) {
						t.Fatalf("export error = %v", err)
					}
					if after, err := os.ReadFile(output); err != nil || string(after) != string(before) {
						t.Fatalf("database-owned output changed: %q, %v", after, err)
					}
				})
			}
		})
	}
}

func TestExportAllowsIndependentOutputBesideDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "source.ltdb")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	output := filepath.Join(dbPath, "graph.json")
	if err := db.ExportFile(ExportFormatJSON, output); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatalf("independent output was not published: %v", err)
	}
}

func TestExportGuardResolvesAliasesAndFlatDatabasePaths(t *testing.T) {
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "source.ltdb")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	alias := filepath.Join(directory, "state-alias.json")
	if err := os.Symlink(filepath.Join(dbPath, "state.json"), alias); err != nil {
		t.Skipf("create source alias: %v", err)
	}
	if err := db.ExportFile(ExportFormatJSON, alias); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("source alias export error = %v", err)
	}
	if info, err := os.Lstat(alias); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("source alias changed: %#v, %v", info, err)
	}

	output := filepath.Join(directory, "graph.json")
	if err := os.Symlink(filepath.Join(dbPath, "wal.base"), output+".lock"); err != nil {
		t.Skipf("create output lock alias: %v", err)
	}
	if err := db.ExportFile(ExportFormatJSON, output); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("output sidecar alias error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dbPath, "wal.base")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing WAL base changed: %v", err)
	}

	flatPath := filepath.Join(directory, "source-flat.ltdb")
	if err := os.WriteFile(flatPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	flat, err := Open(flatPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flat.Close() })
	before, err := os.ReadFile(flatPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := flat.ExportFile(ExportFormatJSON, flatPath); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("flat output collision error = %v", err)
	}
	if after, err := os.ReadFile(flatPath); err != nil || string(after) != string(before) {
		t.Fatalf("flat database changed: %q, %v", after, err)
	}
	if err := flat.ExportFile(ExportFormatJSON, filepath.Join(directory, "flat-graph.json")); err != nil {
		t.Fatalf("independent flat output error = %v", err)
	}
}
