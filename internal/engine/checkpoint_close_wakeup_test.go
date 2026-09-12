package engine

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestCloseRequeuesCheckpointAfterSnapshotRejection(t *testing.T) {
	for _, closeName := range []string{"Close", "CloseContext"} {
		t.Run(closeName, func(t *testing.T) {
			checkpointStarted := make(chan struct{})
			checkpointRelease := make(chan struct{})
			checkpointTryLockFailed := make(chan struct{})
			checkpointDone := make(chan struct{}, 8)
			var startOnce, tryLockOnce sync.Once
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(checkpointRelease) }) }
			db, err := Open(filepath.Join(t.TempDir(), "close-wakeup.ltdb"), OpenOptions{
				Create:                      true,
				WALCheckpointThresholdBytes: 1,
				checkpointComplete:          checkpointDone,
				checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
					startOnce.Do(func() { close(checkpointStarted) })
					<-checkpointRelease
					return nil
				},
				checkpointTryLockFailed: func() {
					tryLockOnce.Do(func() { close(checkpointTryLockFailed) })
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			defer release()

			snapshot, err := db.BeginSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-checkpointStarted:
			case <-time.After(time.Second):
				t.Fatal("background checkpoint did not start")
			}

			db.mu.Lock()
			closeResult := make(chan error, 1)
			go func() {
				if closeName == "Close" {
					closeResult <- db.Close()
					return
				}
				closeResult <- db.CloseContext(context.Background())
			}()
			acquired := false
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if db.writeMu.TryLock() {
					db.writeMu.Unlock()
					runtime.Gosched()
					continue
				}
				acquired = true
				break
			}
			if !acquired {
				db.mu.Unlock()
				t.Fatal("close did not acquire writeMu")
			}
			release()
			select {
			case <-checkpointTryLockFailed:
			case <-time.After(time.Second):
				db.mu.Unlock()
				t.Fatal("checkpoint worker did not observe close contention")
			}
			db.mu.Unlock()

			var closeErr error
			select {
			case closeErr = <-closeResult:
			case <-time.After(time.Second):
				t.Fatal("close did not return after snapshot rejection")
			}
			if !errors.Is(closeErr, ErrSnapshotActive) {
				t.Fatalf("close with active snapshot = %v", closeErr)
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
		})
	}
}
