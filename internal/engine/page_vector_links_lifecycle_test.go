package engine

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageVectorLinkedRecordsSurviveCompactAndSnapshotBackup(t *testing.T) {
	root := t.TempDir()
	opts := OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous}
	db, err := Open(filepath.Join(root, "source"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 80; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{float32(i), float32(i % 7)}}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Require the split layout so this cannot pass using only legacy records.
	links := 0
	if err := db.graph.PageBase.Tx.ScanKeys(context.Background(), "vector-hnsw", []byte{2}, []byte{3}, func([]byte) error {
		links++
		return nil
	}); err != nil || links == 0 {
		t.Fatalf("linked records=%d err=%v", links, err)
	}
	readRecords := func() map[uint64]*store.PageVectorNode {
		t.Helper()
		records := make(map[uint64]*store.PageVectorNode)
		index := store.PageVectorIndex{Tx: db.graph.PageBase.Tx, Namespace: "default"}
		if err := index.Visit(context.Background(), func(id uint64, node *store.PageVectorNode) error {
			records[id] = node
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(records) != 80 {
			t.Fatalf("record count=%d", len(records))
		}
		return records
	}
	before := readRecords()
	query := []float32{25, 4}
	searchOpts := VectorSearchOptions{K: 5, EfSearch: 64}
	want, err := db.VectorSearch(query, searchOpts)
	if err != nil || len(want) != 5 {
		t.Fatalf("search=%v err=%v", want, err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := readRecords(); !reflect.DeepEqual(got, before) {
		t.Fatal("compaction changed persisted HNSW records")
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(root, "backup")
	if err := snapshot.Backup(backup); err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	restored, err := Open(backup, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, err := restored.VectorSearch(query, searchOpts)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("restored search=%v want=%v err=%v", got, want, err)
	}
	if err := restored.View(func(tx *Tx) error {
		for id, node := range before {
			stored, err := tx.GetNode(id)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(stored.Properties["embedding"], node.Vector) {
				t.Fatalf("restored vector %d changed", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
