package engine

import (
	"fmt"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func BenchmarkDeleteHighDegreeNode(b *testing.B) {
	for _, degree := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("degree_%d", degree), func(b *testing.B) {
			base := highDegreeGraph(degree)
			db := &DB{graph: base}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				graph := store.CloneGraphStateShallow(base)
				tx := &Tx{db: db, base: base, graph: graph, changes: newTxChanges(0)}
				b.StartTimer()
				if err := tx.DeleteNode(1); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if graph.Nodes.Len() != 1 || graph.Edges.Len() != 0 || graph.EdgeTypes.Len("LINK") != 0 ||
					graph.Outgoing.Get(1) != nil || graph.Incoming.Get(1) != nil || graph.Outgoing.Get(2) != nil || graph.Incoming.Get(2) != nil {
					b.Fatalf("deleted graph is not empty: nodes=%d edges=%d type=%d", graph.Nodes.Len(), graph.Edges.Len(), graph.EdgeTypes.Len("LINK"))
				}
				b.StartTimer()
			}
		})
	}
}

func highDegreeGraph(degree int) *store.GraphState {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2})
	var outgoing, incoming *store.EdgeList
	for id := uint64(1); id <= uint64(degree); id++ {
		graph.Edges.Set(id, &store.EdgeRecord{ID: id, SourceID: 1, TargetID: 2, Type: "LINK"})
		graph.EdgeTypes.Add("LINK", id)
		outgoing = outgoing.Append(id)
		incoming = incoming.Append(id)
	}
	graph.Outgoing.Set(1, outgoing)
	graph.Incoming.Set(2, incoming)
	return graph
}
