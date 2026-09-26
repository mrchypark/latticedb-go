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

func TestPageGraphUpdatesSkipUnchangedPostings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "updates.pages")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	for _, node := range []*NodeRecord{
		{ID: 1, Labels: []string{"A", "B"}},
		{ID: 2, Labels: []string{"A"}},
		{ID: 3},
	} {
		if err := page.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range []*EdgeRecord{
		{ID: 10, SourceID: 1, TargetID: 2, Type: "R"},
		{ID: 11, SourceID: 1, TargetID: 3, Type: "S"},
	} {
		if err := page.PutEdge(edge); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}

	write, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page = &PageGraph{Tx: write}
	assertRecordOnlyWrite := func(key []byte, encoded []byte, update func() error) {
		t.Helper()
		before, err := write.BufferedBytes()
		if err != nil {
			t.Fatal(err)
		}
		if err := update(); err != nil {
			t.Fatal(err)
		}
		after, err := write.BufferedBytes()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := after-before, uint64(len(key)+len(encoded)); got != want {
			t.Fatalf("update staged %d bytes, want record-only %d", got, want)
		}
	}
	unchangedNode := &NodeRecord{ID: 1, Labels: []string{"B", "A"}}
	encodedNode, err := encodePageNode(unchangedNode)
	if err != nil {
		t.Fatal(err)
	}
	assertRecordOnlyWrite(pageID(unchangedNode.ID), encodedNode, func() error { return page.PutNode(unchangedNode) })
	changedNode := &NodeRecord{ID: 2, Labels: []string{"C"}}
	encodedNode, err = encodePageNode(changedNode)
	if err != nil {
		t.Fatal(err)
	}
	before, err := write.BufferedBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := page.PutNode(changedNode); err != nil {
		t.Fatal(err)
	}
	after, err := write.BufferedBytes()
	if err != nil {
		t.Fatal(err)
	}
	if after-before <= uint64(len(pageID(changedNode.ID))+len(encodedNode)) {
		t.Fatal("label change did not stage its new posting")
	}

	badEndpoint := &EdgeRecord{ID: 10, SourceID: 99, TargetID: 2, Type: "R"}
	before, err = write.BufferedBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := page.PutEdge(badEndpoint); err == nil {
		t.Fatal("edge update accepted a missing endpoint")
	}
	after, err = write.BufferedBytes()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("invalid endpoint update staged writes")
	}
	unchangedEdge := &EdgeRecord{ID: 10, SourceID: 1, TargetID: 2, Type: "R"}
	encodedEdge, err := encodePageEdge(unchangedEdge)
	if err != nil {
		t.Fatal(err)
	}
	assertRecordOnlyWrite(pageID(unchangedEdge.ID), encodedEdge, func() error { return page.PutEdge(unchangedEdge) })
	changedEdge := &EdgeRecord{ID: 11, SourceID: 2, TargetID: 3, Type: "T"}
	encodedEdge, err = encodePageEdge(changedEdge)
	if err != nil {
		t.Fatal(err)
	}
	before, err = write.BufferedBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := page.PutEdge(changedEdge); err != nil {
		t.Fatal(err)
	}
	after, err = write.BufferedBytes()
	if err != nil {
		t.Fatal(err)
	}
	if after-before <= uint64(len(pageID(changedEdge.ID))+len(encodedEdge)) {
		t.Fatal("topology change did not stage changed postings")
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = pagestore.Open(path, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	page = &PageGraph{Tx: read}
	assertIDs := func(name string, visit func(func(uint64) error) error, want []uint64) {
		t.Helper()
		var got []uint64
		if err := visit(func(id uint64) error { got = append(got, id); return nil }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s postings = %v, want %v", name, got, want)
		}
	}
	assertIDs("label A", func(visit func(uint64) error) error { return page.VisitLabel(context.Background(), "A", visit) }, []uint64{1})
	assertIDs("label B", func(visit func(uint64) error) error { return page.VisitLabel(context.Background(), "B", visit) }, []uint64{1})
	assertIDs("label C", func(visit func(uint64) error) error { return page.VisitLabel(context.Background(), "C", visit) }, []uint64{2})
	assertIDs("outgoing 1", func(visit func(uint64) error) error { return page.VisitOutgoing(context.Background(), 1, visit) }, []uint64{10})
	assertIDs("outgoing 2", func(visit func(uint64) error) error { return page.VisitOutgoing(context.Background(), 2, visit) }, []uint64{11})
	assertIDs("incoming 2", func(visit func(uint64) error) error { return page.VisitIncoming(context.Background(), 2, visit) }, []uint64{10})
	assertIDs("incoming 3", func(visit func(uint64) error) error { return page.VisitIncoming(context.Background(), 3, visit) }, []uint64{11})
	assertIDs("edge type R", func(visit func(uint64) error) error { return page.VisitEdgeType(context.Background(), "R", visit) }, []uint64{10})
	assertIDs("edge type S", func(visit func(uint64) error) error { return page.VisitEdgeType(context.Background(), "S", visit) }, nil)
	assertIDs("edge type T", func(visit func(uint64) error) error { return page.VisitEdgeType(context.Background(), "T", visit) }, []uint64{11})
	updatedNode, err := page.GetNode(1)
	if err != nil || updatedNode == nil || !reflect.DeepEqual(updatedNode.Labels, []string{"B", "A"}) {
		t.Fatalf("reopened updated node = %v, %v", updatedNode, err)
	}
	updatedEdge, err := page.GetEdge(11)
	if err != nil || updatedEdge == nil || updatedEdge.SourceID != 2 || updatedEdge.TargetID != 3 || updatedEdge.Type != "T" {
		t.Fatalf("reopened updated edge = %v, %v", updatedEdge, err)
	}
	var canonicalIDs []uint64
	if err := page.VisitCanonicalEdges(context.Background(), func(edge *EdgeRecord) error {
		canonicalIDs = append(canonicalIDs, edge.ID)
		return nil
	}); err != nil {
		t.Fatalf("visit canonical edges after topology update: %v", err)
	}
	if !reflect.DeepEqual(canonicalIDs, []uint64{10, 11}) {
		t.Fatalf("canonical edge order = %v, want [10 11] after source change", canonicalIDs)
	}
}
