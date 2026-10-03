package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestManualPageFTSRebuildAdmitsCanonicalSource(t *testing.T) {
	pages, err := pagestore.Open(filepath.Join(t.TempDir(), "rebuild-source"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	page := &store.PageGraph{Tx: write}
	if err := page.PutNode(&store.NodeRecord{ID: 1}); err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("x", 1<<20)
	if err := page.PutFTS(1, &store.FTSRecord{Text: text, Tokens: []string{text}}); err != nil {
		t.Fatal(err)
	}
	for _, index := range []string{store.PageFTSManualStandard, store.PageFTSManualPorter} {
		if err := page.DropFTSIndex(context.Background(), index); err != nil {
			t.Fatal(err)
		}
	}
	graph := store.NewGraphState()
	graph.PageBase = page
	low := &ftsIndexBudget{maxWork: 32 << 20, maxBytes: 256}
	ctx := store.WithFTSMaintenanceBudget(context.Background(), low)
	if err := rebuildManualPageFTS(ctx, page, graph, store.PageFTSManualStandard, low); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized source error=%v", err)
	}
	if ready, err := page.FTSIndexReady(store.PageFTSManualStandard); err != nil || ready {
		t.Fatalf("rejected rebuild published readiness=%v err=%v", ready, err)
	}
	if low.bytes > 256 {
		t.Fatalf("admitted %d bytes beyond allowance", low.bytes)
	}
	for _, index := range []string{store.PageFTSManualStandard, store.PageFTSManualPorter} {
		enough := &ftsIndexBudget{maxWork: 32 << 20, maxBytes: 32 << 20}
		ctx := store.WithFTSMaintenanceBudget(context.Background(), enough)
		if err := rebuildManualPageFTS(ctx, page, graph, index, enough); err != nil {
			t.Fatal(err)
		}
		var hits int
		if err := page.VisitFTSPostings(context.Background(), index, text, func(posting store.PageFTSPosting) error {
			hits++
			if posting.DocumentID != 1 {
				t.Fatalf("document=%d", posting.DocumentID)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if hits != 1 {
			t.Fatalf("%s hits=%d", index, hits)
		}
	}
}

func TestNamedFTSWholeDropBudgetRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drop-budget")
	high := OpenOptions{Create: true, PageStorage: true, DerivedIndexBuildMaxWork: 4 << 20, DerivedIndexBuildMaxLogicalBytes: 4 << 20}
	db, err := Open(path, high)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
	term := strings.Repeat("x", 33<<10)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"body": term}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateFTSIndex(FTSIndexDefinition{Name: "docs", Kind: FTSIndexNode, Scope: "Item", Property: "body"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 100, DerivedIndexBuildMaxLogicalBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	before := db.commitID
	if err := db.DropFTSIndex("docs"); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("low-budget drop=%v", err)
	}
	if db.commitID != before {
		t.Fatal("rejected drop advanced commit")
	}
	hits, err := db.FTSSearchIndex("docs", term, FTSSearchOptions{})
	if err != nil || len(hits) != 1 {
		t.Fatalf("drop rollback: hits=%v err=%v", hits, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	high.Create = false
	db, err = Open(path, high)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropFTSIndex("docs"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FTSSearchIndex("docs", term, FTSSearchOptions{}); err == nil {
		t.Fatal("dropped index remains available")
	}
}
