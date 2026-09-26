package store

import (
	"context"
	"errors"
	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"io"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPageGraphOverlayAndCorruption(t *testing.T) {
	ctx := context.Background()
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	for _, id := range []uint64{1, 3, 5} {
		if err := page.PutNode(&NodeRecord{ID: id, Labels: []string{"A"}, Properties: PropertiesFromMap(map[string]any{"bytes": []byte{1, 2}, "nested": []any{int64(2), "text"}})}); err != nil {
			t.Fatal(err)
		}
	}
	if err := page.PutEdge(&EdgeRecord{ID: 1, SourceID: 1, TargetID: 3, Type: "R"}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph := NewGraphState()
	graph.PageBase = &PageGraph{Tx: read}
	graph.Nodes.Set(2, &NodeRecord{ID: 2, Labels: []string{"A"}})
	graph.Nodes.Set(3, &NodeRecord{ID: 3, Labels: []string{"B"}})
	graph.DeletedNodes.Set(5, true)
	var ids []uint64
	if err := graph.VisitNodes(ctx, func(n *NodeRecord) error { ids = append(ids, n.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []uint64{1, 2, 3}) {
		t.Fatal(ids)
	}
	ids = nil
	if err := graph.VisitLabel(ctx, "A", func(id uint64) error { ids = append(ids, id); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []uint64{1, 2}) {
		t.Fatal(ids)
	}
	count, err := graph.NodeCount()
	if err != nil || count != 3 {
		t.Fatalf("count %d %v", count, err)
	}
	visited := 0
	if err := graph.VisitNodes(ctx, func(*NodeRecord) error { visited++; return io.EOF }); err != nil || visited != 1 {
		t.Fatalf("stop %d %v", visited, err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.ReadNode(1); !errors.Is(err, pagestore.ErrClosed) {
		t.Fatalf("closed read %v", err)
	}
	write, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page = &PageGraph{Tx: write}
	if err := page.DeleteNode(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if edge, err := page.GetEdge(1); err != nil || edge != nil {
		t.Fatalf("edge %v %v", edge, err)
	}
	if err := write.Put(pageNodes, pageID(3), []byte{1, 1, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	page = &PageGraph{Tx: read}
	if err := page.VisitNodes(ctx, func(*NodeRecord) error { return nil }); err == nil {
		t.Fatal("corruption was hidden")
	}
}
