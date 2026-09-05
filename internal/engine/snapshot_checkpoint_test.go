package engine

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestBeginSnapshotWaitsForBackgroundCheckpointPublication(t *testing.T) {
	published := make(chan struct{})
	contended := make(chan struct{})
	release := make(chan struct{})
	var publishOnce, contentionOnce, releaseOnce sync.Once
	releaseCheckpoint := func() { releaseOnce.Do(func() { close(release) }) }
	db, err := Open(filepath.Join(t.TempDir(), "snapshot-checkpoint.ltdb"), OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointPublish: func() {
			publishOnce.Do(func() { close(published) })
			<-release
		},
		checkpointTryLockFailed: func() {
			select {
			case <-published:
				contentionOnce.Do(func() { close(contended) })
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseCheckpoint()
		_ = db.Close()
	}()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not reach publication")
	}

	type snapshotResult struct {
		snapshot *Snapshot
		err      error
	}
	result := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := db.BeginSnapshot()
		result <- snapshotResult{snapshot, err}
	}()
	select {
	case <-contended:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not observe checkpoint contention")
	}
	select {
	case got := <-result:
		t.Fatalf("snapshot returned while checkpoint publication held the writer slot: %v", got.err)
	case <-time.After(25 * time.Millisecond):
	}
	releaseCheckpoint()
	var got snapshotResult
	select {
	case got = <-result:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not acquire after checkpoint publication")
	}
	if got.err != nil {
		t.Fatalf("snapshot after checkpoint publication = %v", got.err)
	}
	if err := got.snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBeginSnapshotStillRejectsApplicationWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "snapshot-writer.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	writer, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback() }()
	type snapshotResult struct {
		snapshot *Snapshot
		err      error
	}
	result := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := db.BeginSnapshot()
		result <- snapshotResult{snapshot, err}
	}()
	select {
	case got := <-result:
		if got.snapshot != nil {
			defer got.snapshot.Close()
		}
		if !errors.Is(got.err, ErrWriteTxActive) {
			t.Fatalf("snapshot with active application writer = %v, want ErrWriteTxActive", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot blocked on active application writer")
	}
}
