package engine

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestAppMetadataCheckpointGenerationAndWALTailRemainIsolated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata-checkpoint.ltdb")
	prepared := make(chan *store.GraphState, 1)
	release := make(chan struct{})
	checkpointDone := make(chan struct{}, 1)
	var releaseOnce sync.Once
	releaseCheckpoint := func() { releaseOnce.Do(func() { close(release) }) }
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 64 << 10,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(_ string, graph *store.GraphState, _ uint64, _ uint64, _ uint64) error {
			prepared <- graph
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseCheckpoint()
		_ = db.Close()
	}()

	old, deleted, added := []byte("old"), []byte("deleted"), []byte("added")
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(old, bytes.Repeat([]byte("x"), 100<<10)); err != nil {
			return err
		}
		return tx.PutAppMetadata(deleted, []byte("present"))
	}); err != nil {
		t.Fatal(err)
	}
	var checkpointGraph *store.GraphState
	select {
	case checkpointGraph = <-prepared:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not capture its generation")
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(old, []byte("new")); err != nil {
			return err
		}
		if err := tx.DeleteAppMetadata(deleted); err != nil {
			return err
		}
		return tx.PutAppMetadata(added, []byte("tail"))
	}); err != nil {
		t.Fatal(err)
	}
	if value, ok := checkpointGraph.AppMetadata.Get(string(old)); !ok || len(value) != 100<<10 {
		t.Fatalf("checkpoint generation metadata changed: %d bytes, present=%t", len(value), ok)
	}
	if value, ok := checkpointGraph.AppMetadata.Get(string(deleted)); !ok || string(value) != "present" {
		t.Fatalf("checkpoint generation delete leaked: %q, %t", value, ok)
	}
	if _, ok := checkpointGraph.AppMetadata.Get(string(added)); ok {
		t.Fatal("checkpoint generation insert leaked")
	}
	releaseCheckpoint()
	select {
	case <-checkpointDone:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not finish")
	}

	recovered := recoverAppMetadataFiles(t, db, path)
	defer recovered.Close()
	assertAppMetadataRecoveryState(t, recovered, old, []byte("new"), true)
	assertAppMetadataRecoveryState(t, recovered, deleted, nil, false)
	assertAppMetadataRecoveryState(t, recovered, added, []byte("tail"), true)
	pinned, err := recovered.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Rollback()
	if err := recovered.Update(func(tx *Tx) error { return tx.PutAppMetadata(old, []byte("after-recovery")) }); err != nil {
		t.Fatal(err)
	}
	value, ok, err := pinned.GetAppMetadata(old)
	if err != nil || !ok || !bytes.Equal(value, []byte("new")) {
		t.Fatalf("pinned recovered generation = %q, %t, %v", value, ok, err)
	}
}
