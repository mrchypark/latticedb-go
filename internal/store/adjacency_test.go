package store

import "testing"

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
