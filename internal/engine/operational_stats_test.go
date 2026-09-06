package engine

import (
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestOperationalStatsTracksOwnedLifetimes(t *testing.T) {
	now := time.Unix(1_000, 0)
	db := &DB{
		graph:              store.NewGraphState(),
		generationLeases:   map[*store.GraphState]*generationRetention{},
		activeTransactions: map[*Tx]time.Time{},
		activeLeases:       map[*GenerationLease]time.Time{},
		writerWaits:        map[uint64]time.Time{},
		now:                func() time.Time { return now },
	}
	db.writeMu.Lock()
	tx, err := db.beginAfterWriteLock(false)
	if err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	snapshot, err := db.acquireGenerationLeaseLocked(db.graph, true)
	db.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	waitID := db.beginWriterWait()
	now = now.Add(3 * time.Second)

	stats, err := db.OperationalStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.WriterWaits != 1 || stats.ActiveWriterWaits != 1 || stats.OldestWriterWaitAge != 3*time.Second {
		t.Fatalf("writer stats = %#v", stats)
	}
	if stats.ActiveTransactions != 1 || stats.OldestTransactionAge != 3*time.Second {
		t.Fatalf("transaction stats = %#v", stats)
	}
	if stats.ActiveSnapshots != 1 || stats.OldestSnapshotAge != 3*time.Second {
		t.Fatalf("snapshot stats = %#v", stats)
	}
	if stats.RetainedGenerations != 1 || stats.RetainedLogicalBytes != db.graph.SnapshotBytes {
		t.Fatalf("retention stats = %#v", stats)
	}

	snapshot.Release()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	stats, err = db.OperationalStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ActiveWriterWaits != 1 || stats.ActiveTransactions != 0 || stats.ActiveSnapshots != 0 || stats.RetainedGenerations != 0 {
		t.Fatalf("released stats = %#v", stats)
	}
	db.finishWriterWait(waitID)
}
