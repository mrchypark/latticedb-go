package engine

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageVectorSelectionReusesOnlyCurrentVectors(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	index := newPageVectorAccess(store.PageVectorIndex{Tx: page.Tx, Namespace: "selection"}, nil, false)
	defer index.close()
	vectors := map[uint64][]float32{1: {0, 0}, 2: {1, 0}, 3: {2, 0}, 4: {-1, 0}, 5: {3, 0}}
	for id, vector := range vectors {
		neighbors := []uint64(nil)
		if id == 1 {
			neighbors = []uint64{2, 3}
		}
		if err := index.Put(id, &store.PageVectorNode{Neighbors: [][]uint64{neighbors}, Vector: vector}); err != nil {
			t.Fatal(err)
		}
	}
	candidates := []vectorCandidate{{id: 2, distance: 1}, {id: 4, distance: 1}, {id: 3, distance: 4}}
	newBudget := func() *directSearchBudget {
		return &directSearchBudget{ctx: context.Background(), maxWork: 1000, maxBytes: 1 << 20}
	}
	cachedBudget, diskBudget := newBudget(), newBudget()
	cached, err := selectPageVectorNeighbors(context.Background(), index, slices.Clone(candidates), 2, vectors, cachedBudget)
	if err != nil {
		t.Fatal(err)
	}
	fromDisk, err := selectPageVectorNeighbors(context.Background(), index, slices.Clone(candidates), 2, nil, diskBudget)
	if err != nil || !slices.Equal(cached, fromDisk) || cachedBudget.work != diskBudget.work {
		t.Fatalf("selection/work changed: cached=%v disk=%v work=%d/%d err=%v", cached, fromDisk, cachedBudget.work, diskBudget.work, err)
	}
	budget := newBudget()
	if err := connectPageVectorNeighbor(context.Background(), index, 1, 4, vectors[4], 0, 2, budget); err != nil {
		t.Fatal(err)
	}
	node, err := index.Get(1)
	if err != nil || !slices.Equal(node.Neighbors[0], []uint64{2, 4}) || budget.bytes != 0 {
		t.Fatalf("first connection: node=%+v bytes=%d err=%v", node, budget.bytes, err)
	}
	if err := index.Put(2, &store.PageVectorNode{Neighbors: [][]uint64{nil}, Vector: []float32{-1, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := connectPageVectorNeighbor(context.Background(), index, 1, 5, vectors[5], 0, 2, budget); err != nil {
		t.Fatal(err)
	}
	node, err = index.Get(1)
	if err != nil || !slices.Equal(node.Neighbors[0], []uint64{2, 5}) || budget.bytes != 0 {
		t.Fatalf("stale vector reused: node=%+v bytes=%d err=%v", node, budget.bytes, err)
	}
	budget.maxBytes = 1
	if err := connectPageVectorNeighbor(context.Background(), index, 1, 4, vectors[4], 0, 2, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("scratch memory not admitted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := connectPageVectorNeighbor(ctx, index, 1, 4, vectors[4], 0, 2, newBudget()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
	budget = newBudget()
	if err := connectPageVectorNeighbor(context.Background(), index, 1, 4, []float32{1}, 0, 2, budget); err == nil {
		t.Fatal("mismatched new vector accepted")
	}
	if err := index.Put(2, &store.PageVectorNode{Neighbors: [][]uint64{nil}, Vector: []float32{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	if err := connectPageVectorNeighbor(context.Background(), index, 1, 4, vectors[4], 0, 2, budget); err == nil {
		t.Fatal("mismatched stored vector accepted")
	}
	node, err = index.Get(1)
	if err != nil || !slices.Equal(node.Neighbors[0], []uint64{2, 5}) || budget.bytes != 0 {
		t.Fatalf("rejection changed links or leaked budget: node=%+v bytes=%d err=%v", node, budget.bytes, err)
	}
}
