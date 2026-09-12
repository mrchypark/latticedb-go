package latticedb

import (
	"path/filepath"
	"testing"
)

func TestChangefeedRecoveryPreservesMaximumPropertyDepth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "depth.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var value any = int64(1)
	for range 64 {
		value = []any{value}
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"nested": value}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	records, err := db.Changes(0, 10, 0)
	if err != nil || len(records) == 0 {
		t.Fatalf("recovered changes=%d err=%v", len(records), err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	after, err := db.Changes(0, 10, 0)
	if err != nil || len(after) != len(records) {
		t.Fatalf("checkpoint changes=%d err=%v", len(after), err)
	}
}
