package engine

import (
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestGenerationRetentionRemovesFinalLeaseOutOfOrder(t *testing.T) {
	db := &DB{graph: store.NewGraphState(), generationLeases: map[*store.GraphState]*generationRetention{}}
	first, err := db.acquireGenerationLeaseLocked(db.graph, true)
	if err != nil {
		t.Fatal(err)
	}
	secondGraph := store.NewGraphState()
	secondGraph.SnapshotBytes = 8192
	second, err := db.acquireGenerationLeaseLocked(secondGraph, true)
	if err != nil {
		t.Fatal(err)
	}
	firstAgain, err := db.acquireGenerationLeaseLocked(db.graph, false)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if db.retainedGenerationLogicalBytes != db.graph.SnapshotBytes || db.generationOrder.Len() != 1 {
		t.Fatalf("after newer final release: bytes=%d generations=%d", db.retainedGenerationLogicalBytes, db.generationOrder.Len())
	}
	first.Release()
	if db.generationOrder.Len() != 1 {
		t.Fatalf("coalesced generation removed early: %d", db.generationOrder.Len())
	}
	firstAgain.Release()
	if db.retainedGenerationLogicalBytes != 0 || db.generationOrder.Len() != 0 {
		t.Fatalf("after final release: bytes=%d generations=%d", db.retainedGenerationLogicalBytes, db.generationOrder.Len())
	}
}
