package engine

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestBackgroundCheckpointWaitsForWriterUnlockAfterPublicationContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint-publication-contention.ltdb")
	prepared := make(chan struct{})
	releasePrepare := make(chan struct{})
	runDone := make(chan struct{})
	var prepareOnce, releasePrepareOnce sync.Once
	releasePrepareNow := func() { releasePrepareOnce.Do(func() { close(releasePrepare) }) }
	var runStarted bool
	var foreground *Tx
	failures := 0
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			prepareOnce.Do(func() { close(prepared) })
			<-releasePrepare
			return nil
		},
		checkpointTryLockFailed: func() {
			failures++
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	db.stopCheckpointWorker()
	db.checkpointWorkerMu.Lock()
	db.checkpointWake = make(chan struct{}, 1)
	db.checkpointStop = make(chan struct{})
	db.checkpointDone = nil
	db.checkpointQueued = false
	db.checkpointWorkerMu.Unlock()
	t.Cleanup(func() {
		releasePrepareNow()
		if foreground != nil {
			_ = foreground.Rollback()
		}
		if runStarted {
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Error("background checkpoint did not return during cleanup")
			}
		}
		_ = db.Close()
	})

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db.checkpointWorkerMu.Lock()
	if !db.checkpointQueued {
		db.checkpointWorkerMu.Unlock()
		t.Fatal("commit did not queue a checkpoint")
	}
	<-db.checkpointWake
	db.checkpointQueued = false
	db.checkpointWorkerMu.Unlock()

	runStarted = true
	go func() {
		db.runBackgroundCheckpoint()
		close(runDone)
	}()
	select {
	case <-prepared:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not reach preparation")
	}
	foreground, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	releasePrepareNow()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not return after writer contention")
	}
	db.checkpointWorkerMu.Lock()
	queued := db.checkpointQueued
	db.checkpointWorkerMu.Unlock()
	if queued {
		t.Fatal("background checkpoint queued a retry before the writer released its slot")
	}
	if failures != 1 {
		t.Fatalf("publication lock failures = %d, want 1", failures)
	}

	if err := foreground.Rollback(); err != nil {
		t.Fatal(err)
	}
	foreground = nil
	db.checkpointWorkerMu.Lock()
	if !db.checkpointQueued {
		db.checkpointWorkerMu.Unlock()
		t.Fatal("rollback did not queue a checkpoint retry")
	}
	<-db.checkpointWake
	db.checkpointQueued = false
	db.checkpointWorkerMu.Unlock()
	db.runBackgroundCheckpoint()
	db.mu.RLock()
	count, dirty := db.checkpointCount, db.dirty
	db.mu.RUnlock()
	if count != 1 || dirty {
		t.Fatalf("checkpoint after rollback: count=%d dirty=%v", count, dirty)
	}
}
