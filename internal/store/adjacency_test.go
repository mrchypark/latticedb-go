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
	if chunks != 5 {
		t.Fatalf("chunks = %d, want 5", chunks)
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
