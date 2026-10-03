package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageSearchGenerationRejectsStaleDerivedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	var id uint64
	if err = db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		id = node.ID
		return tx.FTSIndex(id, "old")
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a writer that changes canonical rows and history but leaves the
	// new derived search records untouched, as an older engine would.
	pages, err := pagestore.Open(filepath.Join(path, "state.json"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	if err = page.PutFTSContext(context.Background(), id, &store.FTSRecord{Text: "new", Tokens: []string{"new"}}); err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{store.PageFTSManualStandard, store.PageFTSManualPorter} {
		if err = page.ReplaceFTSDocument(context.Background(), index, id, []string{"old"}); err != nil {
			t.Fatal(err)
		}
	}
	catalog, err := page.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	catalog.CommitID++
	catalog.History[0] ^= 1
	if err = page.PutCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if err = write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = pages.Close(); err != nil {
		t.Fatal(err)
	}

	for _, readOnly := range []bool{true, false} {
		db, err = Open(path, OpenOptions{ReadOnly: readOnly, PageStorage: true})
		if err != nil {
			t.Fatal(err)
		}
		if db.graph.PageBase.SearchIndexesCurrent == readOnly {
			t.Fatalf("current flag=%t readOnly=%t", db.graph.PageBase.SearchIndexesCurrent, readOnly)
		}
		hits, err := db.FTSSearch("new", FTSSearchOptions{})
		if err != nil || len(hits) != 1 || hits[0].NodeID != id {
			t.Fatalf("new text: %+v %v", hits, err)
		}
		hits, err = db.FTSSearch("old", FTSSearchOptions{})
		if err != nil || len(hits) != 0 {
			t.Fatalf("stale text: %+v %v", hits, err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
