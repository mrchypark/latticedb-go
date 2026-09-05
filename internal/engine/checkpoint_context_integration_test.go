package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestForegroundCheckpointRetiresOldCandidateWithoutLosingTail(t *testing.T) {
	for _, withContext := range []bool{false, true} {
		name := "legacy"
		if withContext {
			name = "context"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "checkpoint.ltdb")
			db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.stopCheckpointWorker()
			if _, err := db.Query("CREATE (:Item {value: 1})", nil); err != nil {
				t.Fatal(err)
			}
			db.walCheckpointThresholdBytes = 1
			generation, ok := db.rotateBackgroundCheckpoint()
			if !ok {
				t.Fatal("did not rotate WAL")
			}
			db.walCheckpointThresholdBytes = ^uint64(0)
			prepared, err := store.PrepareCheckpointStateFiles(db.files, generation.graph, generation.nextNodeID, generation.nextEdgeID, generation.commitID)
			if err != nil {
				t.Fatal(err)
			}
			db.checkpointPending, db.checkpointPrepared = &generation, prepared
			db.checkpointInFlight.Store(true)
			if _, err := db.Query("CREATE (:Item {value: 2})", nil); err != nil {
				t.Fatal(err)
			}
			if withContext {
				err = db.CheckpointContext(context.Background())
			} else {
				err = db.Checkpoint()
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Query("CREATE (:Item {value: 3})", nil); err != nil {
				t.Fatal(err)
			}
			db.runBackgroundCheckpoint()
			if db.checkpointPending != nil || db.checkpointPrepared != nil || db.checkpointInFlight.Load() {
				t.Fatal("superseded candidate not retired")
			}
			// Copy durable files without Close writing a newer checkpoint that could hide corruption.
			recovered, err := Open(copyRecoveryFiles(t, path), OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			result, err := recovered.Query("MATCH (n:Item) RETURN n.value ORDER BY n.value", nil)
			if err != nil || len(result.Rows) != 3 {
				t.Fatalf("recovered rows = %+v, %v", result.Rows, err)
			}
			for i, row := range result.Rows {
				if row["n.value"] != int64(i+1) {
					t.Fatalf("recovered value = %v", row)
				}
			}
		})
	}
}

func TestCloseContextFinishesAfterClosedTransition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := Open(filepath.Join(t.TempDir(), "close.ltdb"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0), checkpoint: func(string, *store.GraphState, uint64, uint64, uint64) error { cancel(); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query("CREATE (:Item)", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil || db.IsOpen() {
		t.Fatal("close did not finish after cancellation at committed close boundary")
	}
}
