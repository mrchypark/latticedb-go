package latticedb

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func TestGenerationLeasesBoundAdmissionAndRelease(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "leases.ltdb"), OpenOptions{Create: true, MaxGenerationLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("second lease error = %v, want ErrResourceLimit", err)
	}
	stats, err := db.GenerationRetentionStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ActiveLeases != 1 || stats.RetainedGenerations != 1 || stats.RetainedLogicalBytes == 0 {
		t.Fatalf("leased stats = %#v", stats)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	stats, err = db.GenerationRetentionStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ActiveLeases != 0 || stats.RetainedGenerations != 0 || stats.RetainedLogicalBytes != 0 {
		t.Fatalf("released stats = %#v", stats)
	}
	if tx, err := db.Begin(true); err != nil {
		t.Fatal(err)
	} else if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationLeaseDistinctLogicalByteLimit(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bytes.ltdb"), OpenOptions{Create: true, MaxRetainedGenerationLogicalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginSnapshot(); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("new generation lease error = %v, want ErrResourceLimit", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExportLimitsReleaseGenerationLease(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "export.ltdb"), OpenOptions{Create: true, MaxGenerationLeases: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, export := range []func() error{
		func() error {
			_, err := db.ExportContextWithOptions(ctx, ExportFormatJSON, "", ExportOptions{MaxBytes: 1})
			return err
		},
		func() error {
			return db.ExportFileContextWithOptions(ctx, ExportFormatCSV, filepath.Join(t.TempDir(), "manifest.json"), ExportOptions{MaxBytes: 1})
		},
		func() error { _, err := db.DumpContextWithOptions(ctx, ExportOptions{MaxBytes: 1}); return err },
		func() error { return db.DumpToContextWithOptions(ctx, io.Discard, ExportOptions{MaxBytes: 1}) },
		func() error {
			return db.ExportToContextWithOptions(ctx, ExportFormatJSON, io.Discard, ExportOptions{MaxBytes: 1})
		},
	} {
		if err := export(); !errors.Is(err, ErrExportOutputLimit) {
			t.Fatalf("export = %v", err)
		}
		stats, err := db.GenerationRetentionStats()
		if err != nil || stats.ActiveLeases != 0 || stats.RetainedLogicalBytes != 0 {
			t.Fatalf("after export: %+v, %v", stats, err)
		}
	}
}
