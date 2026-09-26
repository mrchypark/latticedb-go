package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestPageAPIReadsAndMutatesDiskRecords(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var source, target, edge uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Page"}})
		if err != nil {
			return err
		}
		source = node.ID
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Page"}})
		if err != nil {
			return err
		}
		target = node.ID
		created, err := tx.CreateEdge(source, target, "LINK", CreateEdgeOptions{Properties: map[string]any{"rank": int64(1)}})
		if err != nil {
			return err
		}
		edge = created.ID
		return tx.FTSIndex(source, "old searchable text")
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		outgoing, err := tx.GetOutgoingEdges(source)
		if err != nil {
			return err
		}
		if len(outgoing) != 1 || outgoing[0].ID != edge {
			t.Fatalf("outgoing edges: %+v", outgoing)
		}
		byType, err := tx.GetOutgoingEdgesByType(source, "LINK", 1)
		if err != nil {
			return err
		}
		if len(byType) != 1 || byType[0].ID != edge {
			t.Fatalf("outgoing edges by type: %+v", byType)
		}
		if err := tx.SetProperty(source, "name", "updated"); err != nil {
			return err
		}
		if err := tx.SetEdgeProperty(edge, "rank", int64(2)); err != nil {
			return err
		}
		return tx.FTSIndex(source, "replacement searchable text")
	}); err != nil {
		t.Fatal(err)
	}

	fts, err := db.graph.ReadFTS(source)
	if err != nil {
		t.Fatal(err)
	}
	if fts == nil || fts.Text != "replacement searchable text" {
		t.Fatalf("FTS record: %+v", fts)
	}
	labels, err := db.GetNodesByLabel("Page")
	if err != nil || len(labels) != 2 {
		t.Fatalf("label nodes %v, error %v", labels, err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteEdge(source, target, "LINK") }); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(source) }); err != nil {
		t.Fatal(err)
	}
	fts, err = db.graph.ReadFTS(source)
	if err != nil {
		t.Fatal(err)
	}
	if fts != nil {
		t.Fatalf("deleted node retained FTS record: %+v", fts)
	}
	count, err := db.graph.EdgeCount()
	if err != nil || count != 0 {
		t.Fatalf("edge count %d, error %v", count, err)
	}
}

func TestPageCommitRecoveryReloadsIndexAndStreamSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": int64(7)}}); err != nil {
			return err
		}
		return tx.PublishStream("events", "seed", map[string]any{"n": int64(1)})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}

	// Keep an older page generation alive while advancing the current graph.
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := db.Update(func(tx *Tx) error {
		return tx.PublishStream("events", "advance", map[string]any{"n": int64(2)})
	}); err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}

	writeErr := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Large"}, Properties: map[string]any{"payload": strings.Repeat("x", 2<<20)}})
		return err
	})
	if !errors.Is(writeErr, ErrResourceLimit) || !errors.Is(writeErr, pagestore.ErrSnapshotGrowth) {
		_ = snapshot.Close()
		t.Fatalf("commit with pinned older generation = %v, want snapshot growth resource error", writeErr)
	}

	assertCurrent := func(wantEvents int, wantIndexed []int64) {
		t.Helper()
		if err := db.View(func(tx *Tx) error {
			for _, key := range wantIndexed {
				ids, err := tx.FindNodesByLabelProperty("Item", "key", key, 10)
				if err != nil {
					return err
				}
				if len(ids) != 1 {
					return fmt.Errorf("index lookup for %d returned %v", key, ids)
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("property index after rejected commit: %v", err)
		}
		records, err := db.ReadStream("events", 0, 10, 0)
		if err != nil {
			t.Fatalf("stream read after rejected commit: %v", err)
		}
		if len(records) != wantEvents {
			t.Fatalf("stream records = %d, want %d", len(records), wantEvents)
		}
	}
	assertCurrent(2, []int64{7})

	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": int64(9)}}); err != nil {
			return err
		}
		return tx.PublishStream("events", "retry", map[string]any{"n": int64(3)})
	}); err != nil {
		t.Fatalf("retry after releasing pinned generation: %v", err)
	}
	assertCurrent(3, []int64{7, 9})
}

func TestPageDatabaseSnapshotLimitOnCommitAndOpen(t *testing.T) {
	const limit = uint64(4096)
	commitPath := filepath.Join(t.TempDir(), "commit-limit")
	db, err := Open(commitPath, OpenOptions{Create: true, PageStorage: true, MaxDatabaseSnapshotBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Limited"}, Properties: map[string]any{"value": "over limit"}})
		return err
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("page commit above explicit snapshot limit = %v, want ErrResourceLimit", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(commitPath, OpenOptions{PageStorage: true, MaxDatabaseSnapshotBytes: limit})
	if err != nil {
		t.Fatalf("reopen after rejected commit: %v", err)
	}
	count, err := db.graph.NodeCount()
	if err != nil || count != 0 {
		t.Fatalf("node count after rejected commit = %d, %v; want 0", count, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	openPath := filepath.Join(t.TempDir(), "open-limit")
	db, err = Open(openPath, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Stored"}, Properties: map[string]any{"value": strings.Repeat("x", 1024)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if db.graph.SnapshotBytes <= limit {
		t.Fatalf("fixture snapshot size = %d, want greater than %d", db.graph.SnapshotBytes, limit)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(openPath, OpenOptions{PageStorage: true, MaxDatabaseSnapshotBytes: limit}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("open above explicit snapshot limit = %v, want ErrResourceLimit", err)
	}
}

func TestPageQueryDeletePersistedNodeAndFailedStatementRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": "original"}})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Query(`MATCH (n:Item) SET n.value = 'changed' RETURN 1 / 0 AS failure`, nil); err == nil {
			return errors.New("expected statement failure")
		}
		value, ok, err := tx.GetProperty(nodeID, "value")
		if err != nil {
			return err
		}
		if !ok || value != "original" {
			return errors.New("failed statement changed the transaction view")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.Query(`MATCH (n:Item) DELETE n`, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	count, err := db.graph.NodeCount()
	if err != nil || count != 0 {
		t.Fatalf("node count %d, error %v", count, err)
	}
}

func TestPageMigrationHonorsRecoveryBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Legacy"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{PageStorage: true, RecoveryMaxDecodedBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("migration byte budget: %v", err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	count, err := db.graph.NodeCount()
	if err != nil || count != 1 {
		t.Fatalf("retry migration count %d: %v", count, err)
	}
}
