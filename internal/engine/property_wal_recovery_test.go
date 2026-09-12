package engine

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPropertyWALDeltaRecoversMixedDerivedStateAndChangefeed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, WALCheckpointThresholdBytes: ^uint64(0)}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	var node, other Node
	var edge Edge
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": "old", "embedding": []float32{1, 0}, "keep": map[string]any{"list": []any{int64(7), []byte{1, 2, 3}}}}})
		if err != nil {
			return err
		}
		other, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		edge, err = tx.CreateEdge(node.ID, other.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"weight": int64(1)}})
		if err != nil {
			return err
		}
		return tx.FTSIndex(node.ID, "oldtoken")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "weight"); err != nil {
		t.Fatal(err)
	}
	frozen, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer frozen.Rollback()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(node.ID, "key", "new"); err != nil {
			return err
		}
		if err := tx.SetVector(node.ID, "embedding", []float32{0, 1}); err != nil {
			return err
		}
		if err := tx.SetEdgeProperty(edge.ID, "weight", int64(2)); err != nil {
			return err
		}
		if err := tx.FTSIndex(node.ID, "newtoken"); err != nil {
			return err
		}
		return tx.PublishStream("events", "mixed", map[string]any{"ok": true})
	}); err != nil {
		t.Fatal(err)
	}
	old, ok, err := frozen.GetProperty(node.ID, "key")
	if err != nil || !ok || old != "old" {
		t.Fatalf("frozen key=%v ok=%v error=%v", old, ok, err)
	}
	if err := frozen.Rollback(); err != nil {
		t.Fatal(err)
	}
	feed, err := db.ReadStream(changeStreamName, 0, 1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := os.ReadFile(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	foundPatch := false
	for offset := 0; offset < len(wal); {
		if len(wal)-offset < 64 {
			t.Fatal("incomplete WAL header")
		}
		header := wal[offset : offset+64]
		length := binary.BigEndian.Uint64(header[20:28])
		if length > uint64(len(wal)-offset-64) {
			t.Fatal("incomplete WAL payload")
		}
		// WAL v4 payload tag 3 identifies a property delta.
		if string(header[:8]) == "LDBWAL4\x00" && length > 0 && wal[offset+64] == 3 {
			foundPatch = true
		}
		offset += 64 + int(length)
	}
	if !foundPatch {
		t.Fatal("property mutation used no patch WAL frame")
	}
	db.mu.Lock()
	db.recoveryRequired = true
	db.mu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		ids, err := tx.FindNodesByLabelProperty("Item", "key", "new", 10)
		if err != nil || !reflect.DeepEqual(ids, []uint64{node.ID}) {
			t.Fatalf("node index=%v error=%v", ids, err)
		}
		edges, err := tx.FindEdgesByTypeProperty("LINK", "weight", int64(2), 10)
		if err != nil || !reflect.DeepEqual(edges, []uint64{edge.ID}) {
			t.Fatalf("edge index=%v error=%v", edges, err)
		}
		keep, ok, err := tx.GetProperty(node.ID, "keep")
		if err != nil || !ok || !reflect.DeepEqual(keep, node.Properties["keep"]) {
			t.Fatalf("unchanged nested property=%v error=%v", keep, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	vectors, err := db.VectorSearch([]float32{0, 1}, VectorSearchOptions{K: 1, Exact: true})
	if err != nil || len(vectors) != 1 || vectors[0].NodeID != node.ID || vectors[0].Distance != 0 {
		t.Fatalf("vectors=%v error=%v", vectors, err)
	}
	terms, err := db.FTSSearch("newtoken", FTSSearchOptions{Limit: 10})
	if err != nil || len(terms) != 1 || terms[0].NodeID != node.ID {
		t.Fatalf("FTS=%v error=%v", terms, err)
	}
	recovered, err := db.ReadStream(changeStreamName, 0, 1000, 0)
	if err != nil || !reflect.DeepEqual(recovered, feed) {
		t.Fatalf("recovered changefeed differs: error=%v", err)
	}
	events, err := db.ReadStream("events", 0, 10, 0)
	if err != nil || len(events) != 1 || events[0].Kind != "mixed" {
		t.Fatalf("mixed stream=%v error=%v", events, err)
	}
}
