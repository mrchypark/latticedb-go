package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestBackgroundCheckpointRetiresPreparedCandidateSupersededByForeground(t *testing.T) {
	files := store.DirectoryDatabaseFiles(filepath.Join(t.TempDir(), "db"))
	graph := store.NewGraphState()
	if err := store.EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	prepared, err := store.PrepareCheckpointStateFiles(files, graph, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Cleanup()
	db := &DB{files: files, graph: graph, commitID: 2, checkpointPublicationEpoch: 1}
	old := checkpointGeneration{graph: graph, commitID: 1, epoch: 0}
	if err := db.publishBackgroundCheckpoint(prepared, old); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("stale publish = %v", err)
	}
}

func TestCheckpointContextCanceledBeforeStagedPreparationLeavesDatabaseUsable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, err := Open(filepath.Join(t.TempDir(), "checkpoint.ltdb"), OpenOptions{Create: true, checkpoint: func(string, *store.GraphState, uint64, uint64, uint64) error {
		cancel()
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkpoint = %v", err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("database unusable after canceled prepare: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close = %v", err)
	}
}

func TestCheckpointContextCancellationAfterPublishBoundaryCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, err := Open(filepath.Join(t.TempDir(), "checkpoint.ltdb"), OpenOptions{Create: true, checkpointContextPublish: cancel})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointContext(ctx); err != nil {
		t.Fatalf("checkpoint = %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("publish hook did not cancel context")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
