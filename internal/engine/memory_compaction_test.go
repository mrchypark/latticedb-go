package engine

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestMemoryAdjacencyCompactionRunsWithoutWAL(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	t.Setenv("TMPDIR", work)
	t.Setenv("TMP", work)
	t.Setenv("TEMP", work)
	fail := func() error {
		t.Error("in-memory database called a disk persistence hook")
		return errors.New("unexpected persistence hook")
	}
	db, err := Open(":memory:", OpenOptions{
		walSync: func(*os.File) error { return fail() },
		walWrite: func(*os.File, []byte) (int, error) {
			return 0, fail()
		},
		walTruncate:    func(*os.File, int64) error { return fail() },
		walCleanupSync: func(*os.File) error { return fail() },
		reserveIDs:     func(store.DatabaseFiles, string, uint64, uint64) error { return fail() },
		checkpoint:     func(string, *store.GraphState, uint64, uint64, uint64) error { return fail() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if db.wal != nil || db.pathLock != nil || db.files != (store.DatabaseFiles{}) {
		t.Fatalf("memory DB has persistence resources: wal=%v lock=%v files=%+v", db.wal, db.pathLock, db.files)
	}

	const liveEdges = 512
	setup, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	source, err := setup.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	targets := make([]Node, liveEdges)
	edgeTargets := make([]uint64, liveEdges)
	for i := 0; i < liveEdges; i++ {
		targets[i], err = setup.CreateNode(CreateNodeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		edge, err := setup.CreateEdge(source.ID, targets[i].ID, "LINK", CreateEdgeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		edgeTargets[i] = targets[i].ID
		_ = edge
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldGraph := snapshot.graph

	for i := 0; i < 256; i++ {
		tx, err := beginMemoryChurnWrite(db)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.DeleteEdge(source.ID, edgeTargets[0], "LINK"); err != nil {
			t.Fatal(err)
		}
		targetID := targets[i%liveEdges].ID
		if _, err := tx.CreateEdge(source.ID, targetID, "LINK", CreateEdgeOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		copy(edgeTargets, edgeTargets[1:])
		edgeTargets[len(edgeTargets)-1] = targetID
	}

	completed := false
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for !completed {
		db.mu.RLock()
		list := db.graph.Outgoing.Get(source.ID)
		completed = list != nil && !list.HasRemovals() && list.Len() == liveEdges
		db.mu.RUnlock()
		if completed {
			break
		}
		select {
		case <-db.adjacencyMaintenanceComplete:
		case <-timeout.C:
			t.Fatal("memory adjacency compaction did not complete")
		}
	}
	oldList := oldGraph.Outgoing.Get(source.ID)
	if oldList == nil || oldList.Len() != liveEdges || oldList.HasRemovals() {
		t.Fatalf("pinned snapshot adjacency changed: %#v", oldList)
	}
	oldEdgeCount := 0
	for range oldGraph.Edges.All() {
		oldEdgeCount++
	}
	if oldEdgeCount != liveEdges {
		t.Fatalf("pinned snapshot has %d edges, want %d", oldEdgeCount, liveEdges)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(work)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("memory compaction created files: %v", entries)
	}
}

func beginMemoryChurnWrite(db *DB) (*Tx, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		tx, err := db.Begin(false)
		if !errors.Is(err, ErrWriteTxActive) || time.Now().After(deadline) {
			return tx, err
		}
		time.Sleep(time.Millisecond)
	}
}
