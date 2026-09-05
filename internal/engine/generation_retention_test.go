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
	if db.retainedGenerationLogicalBytes != db.graph.SnapshotBytes || generationRetentionCount(db) != 1 {
		t.Fatalf("after newer final release: bytes=%d generations=%d", db.retainedGenerationLogicalBytes, generationRetentionCount(db))
	}
	first.Release()
	if generationRetentionCount(db) != 1 {
		t.Fatalf("coalesced generation removed early: %d", generationRetentionCount(db))
	}
	firstAgain.Release()
	if db.retainedGenerationLogicalBytes != 0 || generationRetentionCount(db) != 0 {
		t.Fatalf("after final release: bytes=%d generations=%d", db.retainedGenerationLogicalBytes, generationRetentionCount(db))
	}
}

func TestReleasedHandlesDropGenerationReferences(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.graph != nil || snapshot.lease.graph != nil {
		t.Fatal("closed snapshot retains generation")
	}
	for _, readOnly := range []bool{true, false} {
		tx, err := db.Begin(readOnly)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if tx.graph != nil || tx.base != nil || tx.changes != nil || tx.generationLease != nil {
			t.Fatal("closed transaction retains generation")
		}
	}
}

func generationRetentionCount(db *DB) int {
	count := 0
	for retention := db.generationOrderHead; retention != nil; retention = retention.next {
		count++
	}
	return count
}
