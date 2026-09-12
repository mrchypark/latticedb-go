package engine

import (
	"path/filepath"
	"testing"
)

func TestFTSPropertyLedgerMatchesReopenAfterUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts-ledger")
	opts := OpenOptions{Create: true, FTSProperties: []string{"text", "other"}}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}, Properties: map[string]any{"text": "repeated repeated term", "other": "second property"}})
		if err == nil {
			id = node.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{"short", "한글 words repeated repeated", int64(1), "", "last text"} {
		if err := db.Update(func(tx *Tx) error { return tx.SetProperty(id, "text", value) }); err != nil {
			t.Fatal(err)
		}
		wantWork, wantBytes := db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		opts.Create = false
		db, err = Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		if db.graph.DerivedIndexWork != wantWork || db.graph.DerivedIndexLogicalBytes != wantBytes {
			t.Fatalf("value=%v before=%d/%d reopened=%d/%d", value, wantWork, wantBytes, db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes)
		}
	}
}
