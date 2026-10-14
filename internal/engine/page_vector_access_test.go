package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageVectorAccessWriteThroughAndFailedWrite(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	disk := store.PageVectorIndex{Tx: page.Tx, Namespace: "cache", MaxRecordBytes: pageVectorNodeReadBudget(2, 2)}
	for id, v := range map[uint64][]float32{1: {0, 0}, 2: {1, 0}, 3: {2, 0}} {
		if err := disk.Put(id, &store.PageVectorNode{Neighbors: [][]uint64{{}}, Vector: v}); err != nil {
			t.Fatal(err)
		}
	}
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 10000, maxBytes: 1 << 20}
	access := newPageVectorAccess(disk, budget, false)
	original, err := access.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := access.Get(1)
	if again != original {
		t.Fatal("read not reused")
	}
	if err := connectPageVectorNeighbor(context.Background(), access, 1, 2, []float32{1, 0}, 0, 2, budget); err != nil {
		t.Fatal(err)
	}
	entry := access.nodes[1]
	if len(original.Neighbors[0]) != 0 {
		t.Fatal("cached snapshot mutated")
	}
	got, _ := access.Get(1)
	stored, _ := disk.Get(1)
	if !reflect.DeepEqual(got, stored) || len(got.Neighbors[0]) != 1 {
		t.Fatalf("stale read: %+v", got)
	}
	rejected := errors.New("write rejected")
	access.BeforeWrite = func(uint64) error { return rejected }
	if err := connectPageVectorNeighbor(context.Background(), access, 1, 3, []float32{2, 0}, 0, 2, budget); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	if access.nodes[1] != nil || access.used != 0 {
		t.Fatal("failed write retained cache entry")
	}
	after, _ := access.Get(1)
	if access.nodes[1] == entry || !reflect.DeepEqual(after, stored) {
		t.Fatal("failed write cache invalidation changed disk view")
	}
	access.close()
	if budget.bytes != 0 {
		t.Fatalf("budget leak: %d", budget.bytes)
	}
}

func TestPageVectorAccessEvictionMatchesDisk(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	disk := store.PageVectorIndex{Tx: page.Tx, Namespace: "eviction", MaxRecordBytes: pageVectorNodeReadBudget(2, 2)}
	for id := uint64(1); id <= 40; id++ {
		if err := disk.Put(id, &store.PageVectorNode{Neighbors: [][]uint64{{}}, Vector: []float32{float32(id), 0}}); err != nil {
			t.Fatal(err)
		}
	}
	budget := &directSearchBudget{ctx: context.Background(), maxBytes: 65536}
	access := newPageVectorAccess(disk, budget, false)
	hot, err := access.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		for id := uint64(1); id <= 40; id++ {
			if got, err := access.Get(1); err != nil || got != hot {
				t.Fatal("hot node evicted during cold scan")
			}
			got, err := access.Get(id)
			want, _ := disk.Get(id)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("id=%d err=%v", id, err)
			}
			if access.used > access.limit/2 {
				t.Fatal("cache exceeded reservation")
			}
		}
	}
	access.close()
	if budget.bytes != 0 {
		t.Fatal("budget leak")
	}
}

func TestPageVectorCachedBuildMatchesUncachedRecords(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 80)
	defer db.Close()
	defer page.Tx.Rollback()
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 20_000_000, maxBytes: 64 << 20}
	selectVector := func(n *store.NodeRecord) ([]float32, bool) { return n.Properties.FirstVector() }
	if err := rebuildPageVectorIndex(context.Background(), page, "cached", selectVector, 2, 4, 64<<20, budget); err != nil {
		t.Fatal(err)
	}
	if err := rebuildPageVectorIndex(context.Background(), page, "uncached", selectVector, 2, 4, 64<<20, nil); err != nil {
		t.Fatal(err)
	}
	cached := store.PageVectorIndex{Tx: page.Tx, Namespace: "cached"}
	uncached := store.PageVectorIndex{Tx: page.Tx, Namespace: "uncached"}
	for id := uint64(1); id <= 80; id++ {
		a, err := cached.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		b, err := uncached.Get(id)
		if err != nil || !reflect.DeepEqual(a, b) {
			t.Fatalf("cached graph changed node %d: err=%v", id, err)
		}
	}
	if budget.bytes != 0 {
		t.Fatalf("cache reservation leaked: %d", budget.bytes)
	}
}

func TestPageVectorAccessReservation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		building        bool
		remaining, want uint64
	}{
		{"query-share", false, 16 << 20, 1 << 20},
		{"build-share", true, 16 << 20, 4 << 20},
		{"query-ceiling", false, 16 << 30, 64 << 20},
		{"build-ceiling", true, 16 << 30, 256 << 20},
		{"small-query", false, 65535, 0},
		{"small-build", true, 16383, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := &directSearchBudget{ctx: context.Background(), maxBytes: tc.remaining + 123, bytes: 123}
			a := newPageVectorAccess(store.PageVectorIndex{}, budget, tc.building)
			if a.limit != tc.want || budget.bytes != 123+tc.want {
				t.Fatalf("reservation=%d charged=%d want=%d", a.limit, budget.bytes, tc.want)
			}
			a.close()
			if budget.bytes != 123 {
				t.Fatalf("reservation release changed prior charges: %d", budget.bytes)
			}
		})
	}
}

func TestPageVectorLayerReusesBoundedScratch(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	disk := store.PageVectorIndex{Tx: page.Tx, Namespace: "scratch"}
	for id := uint64(1); id <= 3; id++ {
		if err := disk.Put(id, &store.PageVectorNode{Vector: []float32{float32(id)}, Neighbors: [][]uint64{{1, 2, 3}}}); err != nil {
			t.Fatal(err)
		}
	}
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 10000, maxBytes: 64 << 20}
	access := newPageVectorAccess(disk, budget, true)
	defer access.close()
	for _, q := range []float32{1, 3, 2, 1} {
		got, err := pageVectorLayer(context.Background(), access, []float32{q}, 1, 0, 3, 0, budget)
		if err != nil || len(got) != 3 || got[0].id != uint64(q) {
			t.Fatalf("query=%v got=%v err=%v", q, got, err)
		}
		if access.scratch == nil || len(access.scratch.visited) != 0 || access.scratch.visitedCapacity != 3 {
			t.Fatal("scratch not reused or visited IDs leaked between layers")
		}
		if budget.bytes != access.limit {
			t.Fatalf("temporary reservation leaked: %d != %d", budget.bytes, access.limit)
		}
	}
	access.scratch.frontier = make([]vectorCandidate, 0, 4097)
	if _, err := pageVectorLayer(context.Background(), access, []float32{1}, 1, 0, 3, 0, budget); err != nil {
		t.Fatal(err)
	}
	if access.scratch.frontier != nil || access.scratch.visited != nil {
		t.Fatal("oversized scratch retained")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pageVectorLayer(canceled, access, []float32{1}, 1, 0, 3, 0, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled layer: %v", err)
	}
	if len(access.scratch.visited) != 0 || budget.bytes != access.limit {
		t.Fatal("canceled layer retained visited IDs or temporary charges")
	}
	access.reset()
	if access.scratch.visited != nil || access.scratch.best != nil || access.scratch.frontier != nil {
		t.Fatal("reset retained scratch")
	}
}

func TestPageVectorAccessOwnsWrittenSnapshot(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	budget := &directSearchBudget{ctx: context.Background(), maxBytes: 1 << 20}
	access := newPageVectorAccess(store.PageVectorIndex{Tx: page.Tx, Namespace: "owned-write"}, budget, false)
	defer access.close()
	node := &store.PageVectorNode{Vector: []float32{1, 2}, Neighbors: [][]uint64{{3}}}
	if err := access.Put(1, node); err != nil {
		t.Fatal(err)
	}
	node.Vector[0], node.Neighbors[0][0] = 99, 99
	got, err := access.Get(1)
	if err != nil || got.Vector[0] != 1 || got.Neighbors[0][0] != 3 {
		t.Fatalf("caller changed cached write: %+v, %v", got, err)
	}
	updated := *got
	updated.Neighbors = [][]uint64{{4}}
	if err := access.Put(1, &updated); err != nil {
		t.Fatal(err)
	}
	after, err := access.Get(1)
	if err != nil || &after.Vector[0] != &got.Vector[0] || got.Neighbors[0][0] != 3 {
		t.Fatal("immutable vector was copied or old adjacency changed")
	}
	updated.Neighbors[0][0] = 99
	stored, err := access.PageVectorIndex.Get(1)
	if err != nil || !reflect.DeepEqual(after, stored) {
		t.Fatalf("cached write differs from disk: %+v %+v %v", after, stored, err)
	}
}

func TestPageVectorAccessReplacesCachedEntryAndUpdatesCost(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	budget := &directSearchBudget{ctx: context.Background(), maxBytes: 1 << 20}
	access := newPageVectorAccess(store.PageVectorIndex{Tx: page.Tx, Namespace: "replacement"}, budget, false)
	defer access.close()
	access.limit = 1024
	first := &store.PageVectorNode{Vector: []float32{1}, Neighbors: [][]uint64{{2}}}
	if err := access.Put(1, first); err != nil {
		t.Fatal(err)
	}
	entry, oldCost := access.nodes[1], access.used
	replacement := &store.PageVectorNode{
		Vector:    make([]float32, 2, 8),
		Neighbors: [][]uint64{make([]uint64, 1, 4)},
	}
	replacement.Vector[0], replacement.Vector[1], replacement.Neighbors[0][0] = 3, 4, 2
	if err := access.Put(1, replacement); err != nil {
		t.Fatal(err)
	}
	if access.nodes[1] != entry || access.used != entry.cost || entry.cost == oldCost {
		t.Fatalf("replacement did not reuse entry or update cost: entry=%p want=%p used=%d cost=%d old=%d", access.nodes[1], entry, access.used, entry.cost, oldCost)
	}
	if got, err := access.Get(1); err != nil || got.Vector[0] != 3 || got.Neighbors[0][0] != 2 {
		t.Fatalf("replacement cache = %+v, %v", got, err)
	}
}

func TestPageVectorAccessInvalidWriteEvictsPriorSnapshot(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	budget := &directSearchBudget{ctx: context.Background(), maxBytes: 1 << 20}
	access := newPageVectorAccess(store.PageVectorIndex{Tx: page.Tx, Namespace: "invalidation"}, budget, false)
	defer access.close()
	if err := access.Put(1, &store.PageVectorNode{Vector: []float32{1}, Neighbors: [][]uint64{{2}}}); err != nil {
		t.Fatal(err)
	}
	if err := access.Put(1, &store.PageVectorNode{Vector: []float32{2}, Neighbors: [][]uint64{{0}}}); err != nil {
		t.Fatal(err)
	}
	if access.nodes[1] != nil || access.used != 0 {
		t.Fatalf("invalid cached write retained prior snapshot: nodes=%v used=%d", access.nodes, access.used)
	}
	if _, err := access.Get(1); err == nil {
		t.Fatal("invalid disk record returned from cache")
	}
}

func TestPageVectorCachedWritePreservesReadValidation(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 0)
	defer db.Close()
	defer page.Tx.Rollback()
	for _, tc := range []struct {
		name  string
		limit uint64
		node  store.PageVectorNode
	}{
		{"read-bound", 3, store.PageVectorNode{Vector: []float32{1}, Neighbors: [][]uint64{{}}}},
		{"zero-neighbor", 0, store.PageVectorNode{Vector: []float32{1}, Neighbors: [][]uint64{{0}}}},
		{"empty-vector", 0, store.PageVectorNode{Neighbors: [][]uint64{{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := &directSearchBudget{ctx: context.Background(), maxBytes: 1 << 20}
			access := newPageVectorAccess(store.PageVectorIndex{Tx: page.Tx, Namespace: tc.name, MaxRecordBytes: tc.limit}, budget, false)
			defer access.close()
			if err := access.Put(1, &tc.node); err != nil {
				t.Fatal(err)
			}
			if _, err := access.Get(1); err == nil {
				t.Fatal("cached write bypassed disk validation")
			}
		})
	}
}
