package store

import (
	"slices"
	"testing"
	"unsafe"
)

var adjacencyAllocationSink PagedMap[*EdgeList]

func TestAdjacencyLayoutBudget(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("64-bit layout guard")
	}
	if got := unsafe.Sizeof(PagedMap[uint64]{}); got > 56 {
		t.Fatalf("PagedMap size = %d, want <= 56", got)
	}
	if got := unsafe.Sizeof(EdgeList{}); got > 152 {
		t.Fatalf("EdgeList size = %d, want <= 152", got)
	}
}

func TestGraphAdjacencyForkAppendAllocationBudget(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= 1_000; id++ {
		list = list.Append(id)
	}
	base := NewPagedMap[*EdgeList]()
	base.Set(1, list)
	allocations := testing.AllocsPerRun(100, func() {
		fork := base.Fork()
		fork.CloneShardOnce(1)
		fork.Set(1, fork.Get(1).Append(1_001))
		adjacencyAllocationSink = fork
	})
	if allocations > 10 {
		t.Fatalf("fork+append allocations = %.0f, want <= 10", allocations)
	}
}

func TestEdgeListInlineChurnDoesNotCreateChunkGaps(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= 10_000; id++ {
		list = list.Append(id)
		list = list.Remove(id)
	}
	for id := uint64(1); id <= adjacencyInlineLimit+1; id++ {
		list = list.Append(id)
	}
	if list.Len() != adjacencyInlineLimit+1 || list.total != adjacencyInlineLimit+1 {
		t.Fatalf("length/total = %d/%d", list.Len(), list.total)
	}
	chunks := 0
	for chunk := range list.Chunks() {
		if len(chunk) == 0 {
			t.Fatal("empty adjacency chunk")
		}
		chunks++
	}
	if want := (adjacencyInlineLimit + 1 + adjacencyChunkSize - 1) / adjacencyChunkSize; chunks != want {
		t.Fatalf("chunks = %d, want %d", chunks, want)
	}
}

func TestEdgeListChunkBoundaryPreservesOrder(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= 63; id++ {
		list = list.Append(id)
	}
	if !list.IsInline() {
		t.Fatal("63-entry adjacency should stay inline")
	}
	list = list.Append(64)
	if list.IsInline() {
		t.Fatal("64-entry adjacency should be chunked")
	}
	list = list.Append(65)
	got := list.IDs()
	if len(got) != 65 {
		t.Fatalf("IDs length = %d, want 65", len(got))
	}
	for index, id := range got {
		if id != uint64(index+1) {
			t.Fatalf("IDs[%d] = %d, want %d", index, id, index+1)
		}
	}
}

func TestEdgeListChunkedAppendDoesNotMutateSnapshot(t *testing.T) {
	var snapshot *EdgeList
	for id := uint64(1); id <= 64; id++ {
		snapshot = snapshot.Append(id)
	}
	updated := snapshot.Append(65)
	got := snapshot.IDs()
	if len(got) != 64 {
		t.Fatalf("snapshot IDs length = %d, want 64", len(got))
	}
	for index, id := range got {
		if id != uint64(index+1) {
			t.Fatalf("snapshot IDs[%d] = %d, want %d", index, id, index+1)
		}
	}
	if updated.Len() != 65 || !slices.Contains(updated.IDs(), 65) {
		t.Fatalf("updated adjacency = %v", updated.IDs())
	}
}

func TestEdgeListChunkedRemoveMissingID(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= adjacencyInlineLimit+1; id++ {
		list = list.Append(id)
	}
	if got := list.Remove(adjacencyInlineLimit + 2); got != list {
		t.Fatal("missing ID changed chunked adjacency")
	}
	if got := list.Len(); got != adjacencyInlineLimit+1 {
		t.Fatalf("length = %d, want %d", got, adjacencyInlineLimit+1)
	}
}

func TestEdgeListRemoveKnownPreservesFastChunkedPath(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= adjacencyInlineLimit+1; id++ {
		list = list.Append(id)
	}
	list = list.RemoveKnown(adjacencyInlineLimit + 1)
	values := slices.Collect(list.All())
	if list.Len() != adjacencyInlineLimit || slices.Contains(values, adjacencyInlineLimit+1) {
		t.Fatalf("RemoveKnown left invalid list: len=%d values=%v", list.Len(), values)
	}
}

func TestEdgeListRemoveKnownRetainsTombstones(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= adjacencySyncCompactLimit+1; id++ {
		list = list.Append(id)
	}
	for id := uint64(1); id <= adjacencySyncCompactLimit-6; id++ {
		list = list.RemoveKnown(id)
	}
	if !list.HasRemovals() || list.removed.Len() != adjacencySyncCompactLimit-6 {
		t.Fatalf("tombstones = %d, want %d", list.removed.Len(), adjacencySyncCompactLimit-6)
	}
	list = list.Append(adjacencySyncCompactLimit + 2)
	got := list.IDs()
	if len(got) != 8 {
		t.Fatalf("remaining IDs length = %d, want 8", len(got))
	}
	for index, id := range got {
		want := uint64(adjacencySyncCompactLimit - 5 + index)
		if id != want {
			t.Fatalf("remaining IDs[%d] = %d, want %d", index, id, want)
		}
	}
	if list.removed.Len() != adjacencySyncCompactLimit-6 {
		t.Fatalf("tombstones after append = %d, want %d", list.removed.Len(), adjacencySyncCompactLimit-6)
	}
}

func TestEdgeListRemoveKnownCompactsWithinBound(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= 128; id++ {
		list = list.Append(id)
	}
	for id := uint64(1); id <= 100; id++ {
		list = list.RemoveKnown(id)
	}
	got := list.IDs()
	if list.HasRemovals() || len(got) != 28 {
		t.Fatalf("within-bound compaction failed: len=%d tombstones=%d", list.Len(), list.removed.Len())
	}
	for index, id := range got {
		if id != uint64(index+101) {
			t.Fatalf("compacted IDs[%d] = %d, want %d", index, id, index+101)
		}
	}
}

func TestEdgeListNilAndMissingRemoval(t *testing.T) {
	var nilList *EdgeList
	if nilList.Remove(1) != nil || nilList.RemoveKnown(1) != nil || len(nilList.IDs()) != 0 {
		t.Fatal("nil adjacency removal semantics changed")
	}
	list := (&EdgeList{}).Append(1).Append(2)
	if got := list.Remove(3); got != list || !slices.Equal(got.IDs(), []uint64{1, 2}) {
		t.Fatalf("missing removal changed adjacency: %v", got.IDs())
	}
}

func TestCompactGraphAdjacencyBoundsWorkAndPreservesOrder(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= 10_000; id++ {
		list = list.Append(id)
	}
	for id := uint64(1); id <= 250; id++ {
		list = list.RemoveKnown(id)
	}
	for id := uint64(9_001); id <= 9_250; id++ {
		list = list.RemoveKnown(id)
	}
	graph := NewGraphState()
	graph.Outgoing.Set(1, list)

	compactor := NewAdjacencyCompactor(graph, 0, 1)
	done, changed := compactor.Step(64)
	if done {
		t.Fatalf("first pass done=%v changed=%v", done, changed)
	}
	for !done {
		done, _ = compactor.Step(64)
	}
	compacted := compactor.Result().Outgoing.Get(1)
	if compacted.Len() != 9_500 || compacted.HasRemovals() {
		t.Fatalf("compaction len/tombstones = %d/%v", compacted.Len(), compacted.HasRemovals())
	}
	var want []uint64
	for id := uint64(251); id <= 9_000; id++ {
		want = append(want, id)
	}
	for id := uint64(9_251); id <= 10_000; id++ {
		want = append(want, id)
	}
	if got := slices.Collect(compacted.All()); !slices.Equal(got, want) {
		t.Fatalf("live IDs changed by compaction")
	}
	if !list.HasRemovals() || !slices.Equal(list.IDs(), slices.Collect(list.All())) {
		t.Fatal("active source adjacency was modified")
	}
	appended := compacted.Append(10_001)
	if got := appended.IDs(); len(got) != 9_501 || got[len(got)-1] != 10_001 {
		t.Fatalf("append order after compaction changed: last=%d len=%d", got[len(got)-1], len(got))
	}
}

func TestAdjacencyCompactionDeletesAllRemovedList(t *testing.T) {
	var list *EdgeList
	for id := uint64(1); id <= 512; id++ {
		list = list.Append(id)
	}
	for id := uint64(1); id <= 512; id++ {
		list = list.RemoveKnown(id)
	}
	graph := NewGraphState()
	graph.Outgoing.Set(1, list)
	compactor := NewAdjacencyCompactor(graph, 0, 1)
	done, _ := compactor.Step(adjacencyChunkSize)
	if !done || compactor.Result().Outgoing.Has(1) || compactor.Result().Outgoing.Get(1) != nil {
		t.Fatal("all-removed adjacency was retained")
	}
}

func TestAdjacencyCompactionDirectionsPreserveOrder(t *testing.T) {
	for _, direction := range []uint8{0, 1} {
		t.Run(map[uint8]string{0: "outgoing", 1: "incoming"}[direction], func(t *testing.T) {
			var list *EdgeList
			for id := uint64(1); id <= 512; id++ {
				list = list.Append(id)
			}
			for id := uint64(1); id <= 100; id++ {
				list = list.RemoveKnown(id)
			}
			graph := NewGraphState()
			if direction == 0 {
				graph.Outgoing.Set(1, list)
			} else {
				graph.Incoming.Set(1, list)
			}
			compactor := NewAdjacencyCompactor(graph, direction, 1)
			done, _ := compactor.Step(adjacencyChunkSize)
			if !done {
				t.Fatal("compaction did not finish")
			}
			var compacted *EdgeList
			if direction == 0 {
				compacted = compactor.Result().Outgoing.Get(1)
			} else {
				compacted = compactor.Result().Incoming.Get(1)
			}
			want := make([]uint64, 0, 412)
			for id := uint64(101); id <= 512; id++ {
				want = append(want, id)
			}
			if got := slices.Collect(compacted.All()); !slices.Equal(got, want) {
				t.Fatal("compaction changed edge order")
			}
		})
	}
}
