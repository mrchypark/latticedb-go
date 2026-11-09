package engine

import (
	"context"
	"testing"
)

func TestFTSPinnedCoreUsesSuppliedGraphForPageIndex(t *testing.T) {
	_, graph := pageFTSFixture(t, 8)
	ctx := context.Background()
	budget, err := newDirectSearchBudget(ctx, ^uint64(0), 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.add(uint64(len("rare"))); err != nil {
		t.Fatal(err)
	}
	got, err := new(DB).ftsSearchGraph(ctx, graph, "rare", FTSSearchOptions{Limit: 2, Scoring: FTSScoringBM25}, budget)
	if err != nil || len(got) != 2 || got[0].NodeID != 1 || got[1].NodeID != 2 {
		t.Fatalf("pinned page index results=%v error=%v", got, err)
	}
}

func TestFTSPinnedCoreUsesPinnedGraphForCanonicalFallback(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var oldID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		oldID = node.ID
		return tx.FTSIndex(oldID, "pinned")
	}); err != nil {
		t.Fatal(err)
	}
	pinned, lease, err := db.SnapshotGraph()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	pinned.PageBase.SearchIndexesCurrent = false
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		return tx.FTSIndex(node.ID, "pinned")
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	budget, err := newDirectSearchBudget(ctx, ^uint64(0), 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.add(uint64(len("pinned"))); err != nil {
		t.Fatal(err)
	}
	got, err := db.ftsSearchGraph(ctx, pinned, "pinned", FTSSearchOptions{Limit: 1, Scoring: FTSScoringBM25}, budget)
	if err != nil || len(got) != 1 || got[0].NodeID != oldID {
		t.Fatalf("pinned canonical results=%v error=%v", got, err)
	}
}
