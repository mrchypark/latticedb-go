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
		if n == nil || n.Properties.Get("body") != "hello" {
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
		if n.Properties.Get("v") != int64(1) {
			t.Fatalf("node property lost: %v", n.Properties.Get("v"))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFTSCancellationPropagatesFromTokenizer(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: store.PropertiesFromMap(map[string]any{"body": "alpha"})})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2, Properties: store.PropertiesFromMap(map[string]any{"body": "beta"})})
	db := &DB{derivedIndexBuildMaxWork: 4096, derivedIndexBuildMaxLogicalBytes: 4096}
	ctx := &cancelAfterQueryChecks{remaining: 1, done: make(chan struct{})}

	_, _, _, err := buildFTSPropertyPostings(ctx, graph, []string{"body"}, db)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestFTSPropertyBuildCountsAllNodeVisits(t *testing.T) {
	graph := store.NewGraphState()
	for id := uint64(1); id <= 3; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: store.PropertiesFromMap(map[string]any{"body": int64(id)})})
	}
	for _, maxWork := range []uint64{7, 8} {
		db := &DB{derivedIndexBuildMaxWork: maxWork, derivedIndexBuildMaxLogicalBytes: 4096}
		postings, work, _, err := buildFTSPropertyPostings(t.Context(), graph, []string{"body", "missing"}, db)
		if maxWork == 7 {
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("seven work units: error=%v", err)
			}
		} else if err != nil || len(postings) != 2 || work != 2 {
			t.Fatalf("eight work units: postings=%v persistent work=%d error=%v", postings, work, err)
		}
		if graph.DerivedIndexWork != 0 {
			t.Fatal("build modified input ledger")
		}
	}
}

func TestFTSPropertyScanBudgetReopenAndLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts-scans")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var firstID uint64
	if err := db.Update(func(tx *Tx) error {
		for i := range 32 {
			n, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"body": int64(i)}})
			if err != nil {
				return err
			}
			if i == 0 {
				firstID = n.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts := OpenOptions{FTSProperties: []string{"body", "missing"}}
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	wantWork, wantBytes := db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Two property scans over 32 nodes are transient; definition costs are
	// already in wantWork. A failed open must release the database lock.
	opts.DerivedIndexBuildMaxWork = wantWork + 64 - 1
	failed, err := Open(path, opts)
	if failed != nil {
		_ = failed.Close()
	}
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("under-budget reopen: %v", err)
	}
	opts.DerivedIndexBuildMaxWork++
	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("exact-budget reopen / lock release: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	for _, value := range []any{"hello", int64(0)} {
		if err := db.Update(func(tx *Tx) error { return tx.SetProperty(firstID, "body", value) }); err != nil {
			t.Fatal(err)
		}
	}
	if db.graph.DerivedIndexWork != wantWork || db.graph.DerivedIndexLogicalBytes != wantBytes {
		t.Fatalf("scan work leaked into live ledger: (%d,%d), want (%d,%d)", db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes, wantWork, wantBytes)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if db.graph.DerivedIndexWork != wantWork || db.graph.DerivedIndexLogicalBytes != wantBytes {
		t.Fatal("reopen changed persistent ledger")
	}
}
