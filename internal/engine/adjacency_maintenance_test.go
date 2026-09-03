package engine

import (
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestBackgroundAdjacencyMaintenanceRejectsStaleGeneration(t *testing.T) {
	oldGraph := store.NewGraphState()
	var list *store.EdgeList
	for id := uint64(1); id <= 512; id++ {
		list = list.Append(id)
	}
	for id := uint64(1); id <= 100; id++ {
		list = list.RemoveKnown(id)
	}
	oldGraph.Outgoing.Set(1, list)
	db := &DB{graph: oldGraph, commitID: 1}

	compactor := store.NewAdjacencyCompactor(oldGraph, 0, 1)
	done, changed := compactor.Step(store.AdjacencyCompactionChunkBudget)
	if !done || !changed {
		t.Fatalf("maintenance pass done=%v changed=%v", done, changed)
	}
	newGraph := store.NewGraphState()
	db.graph = newGraph
	if db.publishBackgroundAdjacency(compactor.Result(), oldGraph, 1) {
		t.Fatal("stale adjacency generation was published")
	}
	if db.graph != newGraph {
		t.Fatal("stale publication replaced current graph")
	}
}

func TestBackgroundAdjacencyMaintenancePublishesDenseReplacement(t *testing.T) {
	graph := store.NewGraphState()
	var list *store.EdgeList
	for id := uint64(1); id <= 10_000; id++ {
		list = list.Append(id)
	}
	for id := uint64(1); id <= 500; id++ {
		list = list.RemoveKnown(id)
	}
	graph.Outgoing.Set(1, list)
	db := &DB{graph: graph, commitID: 1}
	db.adjacencyMaintenanceNeeded.Store(true)
	db.adjacencyMaintenanceQueue = []adjacencyCandidate{{direction: 0, nodeID: 1}}
	db.adjacencyMaintenanceQueued = map[adjacencyCandidate]struct{}{{direction: 0, nodeID: 1}: {}}

	for db.adjacencyMaintenanceNeeded.Load() {
		db.runBackgroundAdjacencyMaintenance()
	}
	if got := db.graph.Outgoing.Get(1); got.HasRemovals() || got.Len() != 9_500 {
		t.Fatalf("published adjacency len/tombstones = %d/%v", got.Len(), got.HasRemovals())
	}
	if list.HasRemovals() == false {
		t.Fatal("active source adjacency was modified")
	}
	if db.adjacencyCompactor != nil || db.adjacencyCompactorGraph != nil || db.adjacencyCompactorCommit != 0 || db.adjacencyCompactorActive || db.adjacencyCompactorCandidate != (adjacencyCandidate{}) {
		t.Fatal("completed maintenance retained the source generation")
	}
}

func TestBackgroundAdjacencyMaintenanceRequeuesStaleJob(t *testing.T) {
	oldGraph := store.NewGraphState()
	var list *store.EdgeList
	for id := uint64(1); id <= 10_000; id++ {
		list = list.Append(id)
	}
	for id := uint64(9_001); id <= 9_100; id++ {
		list = list.RemoveKnown(id)
	}
	oldGraph.Outgoing.Set(1, list)
	newGraph := store.NewGraphState()
	newGraph.Outgoing.Set(1, list)
	db := &DB{graph: oldGraph, commitID: 1}
	db.adjacencyMaintenanceNeeded.Store(true)
	db.adjacencyMaintenanceQueue = []adjacencyCandidate{{direction: 0, nodeID: 1}}
	db.adjacencyMaintenanceQueued = map[adjacencyCandidate]struct{}{{direction: 0, nodeID: 1}: {}}
	db.runBackgroundAdjacencyMaintenance()
	db.graph, db.commitID = newGraph, 2
	db.adjacencyMaintenanceNeeded.Store(true)
	db.runBackgroundAdjacencyMaintenance()
	if db.adjacencyCompactor == nil || !db.adjacencyCompactorActive || db.adjacencyCompactorGraph != newGraph {
		t.Fatal("stale active job was not requeued for the new generation")
	}
}

func BenchmarkBackgroundAdjacencyCompactionChurn(b *testing.B) {
	var list *store.EdgeList
	for id := uint64(1); id <= 10_000; id++ {
		list = list.Append(id)
	}
	nextID := uint64(10_001)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		removed := 0
		for id := range list.All() {
			list = list.RemoveKnown(id)
			removed++
			if removed == 128 {
				break
			}
		}
		for id := uint64(0); id < 128; id++ {
			list = list.Append(nextID)
			nextID++
		}
		graph := store.NewGraphState()
		graph.Outgoing.Set(1, list)
		compactor := store.NewAdjacencyCompactor(graph, 0, 1)
		for done := false; !done; {
			done, _ = compactor.Step(store.AdjacencyCompactionChunkBudget)
		}
		list = compactor.Result().Outgoing.Get(1)
		for range list.All() {
		}
	}
}
