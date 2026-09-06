package engine

import (
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestEdgesByTypeKeepsLocalOrderAndLimit(t *testing.T) {
	graph := store.NewGraphState()
	var local *store.EdgeList
	for id := uint64(1); id <= 200; id++ {
		kind := "LINK"
		if id%2 == 0 {
			kind = "OTHER"
		}
		graph.Edges.Set(id, &store.EdgeRecord{ID: id, Type: kind})
		graph.EdgeTypes.Add(kind, id)
		local = local.Append(id)
	}
	local = local.RemoveKnown(1)
	tx := &Tx{graph: graph}
	for _, limit := range []uint{0, 1, 3, 1000} {
		var want []uint64
		for id := uint64(3); id <= 200; id += 2 {
			want = append(want, id)
			if limit != 0 && uint(len(want)) == limit {
				break
			}
		}
		var got []uint64
		for _, edge := range tx.edgesByType(local, "LINK", limit) {
			got = append(got, edge.ID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("limit %d: got %v, want %v", limit, got, want)
		}
	}
	if got := tx.edgesByType(local, "MISSING", 0); len(got) != 0 {
		t.Fatalf("missing type: %v", got)
	}
}

func BenchmarkLocalEdgesByTypeGlobalCardinality(b *testing.B) {
	for _, count := range []int{10, 10000} {
		name := "small"
		if count == 10000 {
			name = "large"
		}
		b.Run(name, func(b *testing.B) {
			graph := store.NewGraphState()
			var local *store.EdgeList
			local = local.Append(1)
			graph.Edges.Set(1, &store.EdgeRecord{ID: 1, Type: "LINK"})
			for id := 1; id <= count; id++ {
				graph.EdgeTypes.Add("LINK", uint64(id))
			}
			tx := &Tx{graph: graph}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := tx.edgesByType(local, "LINK", 1); len(got) != 1 {
					b.Fatal(got)
				}
			}
		})
	}
}
