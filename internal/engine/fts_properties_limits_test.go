package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestFTSBudgetRejectPreservesLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts-budget")
	db, err := Open(path, OpenOptions{
		Create: true, FTSProperties: []string{"body"},
		DerivedIndexBuildMaxWork: 1024, DerivedIndexBuildMaxLogicalBytes: 1024,
	})
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
		n, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"body": "hello"}})
		if err == nil {
			id = n.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var savedWork, savedBytes uint64
	if err := db.View(func(tx *Tx) error {
		savedWork = tx.graph.DerivedIndexWork
		savedBytes = tx.graph.DerivedIndexLogicalBytes
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx *Tx) error {
		return tx.SetProperty(id, "body", strings.Repeat("x", 400))
	})
	if err == nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("want ErrResourceLimit, got %v", err)
	}

	if err := db.View(func(tx *Tx) error {
		if tx.graph.DerivedIndexWork != savedWork {
			t.Fatalf("work changed: %d -> %d", savedWork, tx.graph.DerivedIndexWork)
		}
		if tx.graph.DerivedIndexLogicalBytes != savedBytes {
			t.Fatalf("bytes changed: %d -> %d", savedBytes, tx.graph.DerivedIndexLogicalBytes)
		}
		ids := tx.graph.FTSProperties["body"].Get("hello")
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("postings lost: %v", ids)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{
		Create: false, FTSProperties: []string{"body"},
		DerivedIndexBuildMaxWork: 1024, DerivedIndexBuildMaxLogicalBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		if tx.graph.DerivedIndexWork != savedWork {
			t.Fatalf("reopen work: %d != %d", tx.graph.DerivedIndexWork, savedWork)
		}
		if tx.graph.DerivedIndexLogicalBytes != savedBytes {
			t.Fatalf("reopen bytes: %d != %d", tx.graph.DerivedIndexLogicalBytes, savedBytes)
		}
		ids := tx.graph.FTSProperties["body"].Get("hello")
		if len(ids) != 1 || ids[0] != id {
			t.Fatalf("reopen postings: %v", ids)
		}
		n := tx.graph.Nodes.Get(id)
		if n == nil || n.Properties["body"] != "hello" {
			t.Fatalf("reopen node body: %v", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFTSPostBuildBudgetFailsAfterLockAcquired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fts-lock")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	var savedID uint64
	if err := db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"v": int64(1)}})
		if err == nil {
			savedID = n.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path, OpenOptions{
		Create: false, FTSProperties: []string{"v"},
		DerivedIndexBuildMaxLogicalBytes: 1,
	})
	if err == nil || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("want ErrResourceLimit after lock acquired, got %v", err)
	}

	db, err = Open(path, OpenOptions{Create: false})
	if err != nil {
		t.Fatalf("lock not released: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		n := tx.graph.Nodes.Get(savedID)
		if n == nil {
			t.Fatal("canonical node missing after failed FTS open")
		}
		if n.Properties["v"] != int64(1) {
			t.Fatalf("node property lost: %v", n.Properties["v"])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFTSCancellationPropagatesFromTokenizer(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"body": "alpha"}})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2, Properties: map[string]any{"body": "beta"}})
	db := &DB{derivedIndexBuildMaxWork: 4096, derivedIndexBuildMaxLogicalBytes: 4096}
	ctx := &cancelAfterQueryChecks{remaining: 1, done: make(chan struct{})}

	_, _, _, err := buildFTSPropertyPostings(ctx, graph, []string{"body"}, db)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
