package engine

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestPageFTSRejectsUndecodableDenseTextBeforeCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
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
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err == nil {
			id = node.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.FTSIndex(id, "durable original") }); err != nil {
		t.Fatal(err)
	}

	dense := strings.Repeat("a ", 1<<20)
	err = db.Update(func(tx *Tx) error { return tx.FTSIndex(id, dense) })
	if !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("dense FTS replacement should be rejected before durability: %v", err)
	}
	assertFTSHit := func(query string, found bool) {
		t.Helper()
		hits, err := db.FTSSearch(query, FTSSearchOptions{Limit: 10})
		if err != nil {
			t.Fatalf("search %q: %v", query, err)
		}
		if got := len(hits) == 1 && hits[0].NodeID == id; got != found {
			t.Fatalf("search %q hit=%v want %v (results %v)", query, got, found, hits)
		}
	}
	assertFTSHit("durable", true)
	assertFTSHit("a", false)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	assertFTSHit("durable", true)
	assertFTSHit("a", false)

	if err := db.Update(func(tx *Tx) error { return tx.FTSIndex(id, "replacement") }); err != nil {
		t.Fatal(err)
	}
	assertFTSHit("durable", false)
	assertFTSHit("replacement", true)
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(id) }); err != nil {
		t.Fatal(err)
	}
	assertFTSHit("replacement", false)
}
