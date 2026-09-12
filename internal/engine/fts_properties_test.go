package engine

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestFTSPropertyPostingsRebuildUpdateDeleteAndManualIndependence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts-properties")
	opts := OpenOptions{Create: true, FTSProperties: []string{"body", "summary"}}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	var first, second uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"body": "Alpha beta alpha", "summary": "brief"}})
		if err != nil {
			return err
		}
		first = node.ID
		node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"body": "gamma", "summary": int64(7)}})
		if err != nil {
			return err
		}
		second = node.ID
		return tx.FTSIndex(first, "manual-only")
	}); err != nil {
		t.Fatal(err)
	}
	check := func(property, token string, want []uint64) {
		t.Helper()
		var got []uint64
		if err := db.View(func(tx *Tx) error {
			got = tx.graph.FTSProperties[property].Get(token)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s:%s=%v want %v", property, token, got, want)
		}
	}
	check("body", "alpha", []uint64{first})
	check("body", "gamma", []uint64{second})
	check("summary", "brief", []uint64{first})
	check("summary", "manual", nil)
	if got, err := db.FTSSearch("manual-only", FTSSearchOptions{Limit: 10}); err != nil || len(got) != 1 || got[0].NodeID != first {
		t.Fatalf("manual FTS=%v error=%v", got, err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(first, "body", "delta"); err != nil {
			return err
		}
		return tx.DeleteNode(second)
	}); err != nil {
		t.Fatal(err)
	}
	check("body", "alpha", nil)
	check("body", "delta", []uint64{first})
	check("body", "gamma", nil)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	check("body", "delta", []uint64{first})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, OpenOptions{Create: false}); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		if tx.graph.FTSProperties != nil {
			t.Fatal("unconfigured property postings were retained")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.FTSSearch("manual-only", FTSSearchOptions{Limit: 10}); err != nil || len(got) != 1 || got[0].NodeID != first {
		t.Fatalf("manual FTS after reopen=%v error=%v", got, err)
	}
}
