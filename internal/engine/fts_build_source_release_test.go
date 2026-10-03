package engine

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestFTSBuildReleasesSourceBytesAndRetainsWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source-release")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
	if err := db.Update(func(tx *Tx) error {
		for range 64 {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"text": "needle", "body": strings.Repeat("x", 4096)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts := OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 8192, DerivedIndexBuildMaxLogicalBytes: 128 << 10}
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	def := FTSIndexDefinition{Name: "text", Kind: FTSIndexNode, Scope: "Item", Property: "text"}
	before := db.commitID
	if err := db.CreateFTSIndex(def); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("cumulative work: %v", err)
	}
	if db.commitID != before {
		t.Fatal("failed build published")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.DerivedIndexBuildMaxWork = 1 << 20
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Source bodies total 256KiB, but each source record plus retained postings fits 128KiB.
	if err := db.CreateFTSIndex(def); err != nil {
		t.Fatal(err)
	}
	got, err := db.FTSSearchIndex("text", "needle", FTSSearchOptions{Limit: 100})
	if err != nil || len(got) != 64 {
		t.Fatalf("results=%d err=%v", len(got), err)
	}
}
