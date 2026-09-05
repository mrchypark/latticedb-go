package latticedb

import (
	"errors"
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
