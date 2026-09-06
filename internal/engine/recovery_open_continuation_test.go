package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestOpenContextContinuesFromBudgetedWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-open.ltdb")
	graph := store.NewGraphState()
	if err := store.EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckpointGraphState(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 2; id++ {
		next := store.NewGraphState()
		next.DatabaseID = graph.DatabaseID
		next.Nodes.Set(id, &store.NodeRecord{ID: id})
		if err := store.AppendWALCommit(path, next, id+1, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	opts := OpenOptions{RecoveryMaxFrames: 2, RecoveryMaxWork: 10}
	db, err := OpenContext(context.Background(), path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if db.graph.Nodes.Get(2) == nil {
		t.Fatal("budgeted recovery did not retain the WAL tail")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenContext(context.Background(), path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.graph.Nodes.Get(2) == nil {
		t.Fatal("reopened database lost the recovered WAL tail")
	}
}
