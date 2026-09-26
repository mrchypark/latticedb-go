package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageBackedExportMatchesInMemoryGraph(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	pageGraph := &store.PageGraph{Tx: writeTx}
	nodes := []*store.NodeRecord{
		{ID: 1, Labels: []string{"Z", "A"}, Properties: store.PropertiesFromMap(map[string]any{"name": "one"})},
		{ID: 2, Labels: []string{"B"}, Properties: store.PropertiesFromMap(map[string]any{"name": "two"})},
	}
	edges := []*store.EdgeRecord{
		{ID: 9, SourceID: 2, TargetID: 1, Type: "Z"},
		{ID: 7, SourceID: 1, TargetID: 2, Type: "B"},
		{ID: 3, SourceID: 1, TargetID: 1, Type: "A"},
	}
	for _, node := range nodes {
		if err := pageGraph.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range edges {
		if err := pageGraph.PutEdge(edge); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeTx.Commit(); err != nil {
		t.Fatal(err)
	}
	readTx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = readTx.Rollback()
		_ = db.Close()
	})

	disk := store.NewGraphState()
	disk.PageBase = &store.PageGraph{Tx: readTx}
	memory := store.NewGraphState()
	for _, node := range nodes {
		memory.Nodes.Set(node.ID, node)
	}
	for _, edge := range edges {
		memory.Edges.Set(edge.ID, edge)
	}
	ctx := context.Background()
	for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatDOT} {
		var want, got bytes.Buffer
		if err := ExportGraphContextTo(ctx, memory, format, &want); err != nil {
			t.Fatalf("memory %s: %v", format, err)
		}
		if err := ExportGraphContextTo(ctx, disk, format, &got); err != nil {
			t.Fatalf("disk %s: %v", format, err)
		}
		if !bytes.Equal(got.Bytes(), want.Bytes()) {
			t.Errorf("%s output differs\n got: %s\nwant: %s", format, got.Bytes(), want.Bytes())
		}
	}

	for _, graph := range []*store.GraphState{memory, disk} {
		if _, err := DumpGraphContextWithOptions(ctx, graph, ExportOptions{MaxRecords: 4}); err != ErrOutputLimit {
			t.Fatalf("MaxRecords on %T graph = %v, want %v", graph.PageBase, err, ErrOutputLimit)
		}
	}

	csvFiles := func(graph *store.GraphState, outputPath string) ([]byte, []byte) {
		t.Helper()
		manifestData, err := ExportGraphContextWithOptions(ctx, graph, ExportFormatCSV, outputPath, ExportOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var manifest csvManifest
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			t.Fatal(err)
		}
		nodes, err := os.ReadFile(filepath.Join(filepath.Dir(outputPath), manifest.Nodes))
		if err != nil {
			t.Fatal(err)
		}
		edges, err := os.ReadFile(filepath.Join(filepath.Dir(outputPath), manifest.Edges))
		if err != nil {
			t.Fatal(err)
		}
		return nodes, edges
	}
	wantNodes, wantEdges := csvFiles(memory, filepath.Join(t.TempDir(), "memory.csv"))
	gotNodes, gotEdges := csvFiles(disk, filepath.Join(t.TempDir(), "disk.csv"))
	if !bytes.Equal(gotNodes, wantNodes) || !bytes.Equal(gotEdges, wantEdges) {
		t.Fatalf("CSV differs: nodes equal=%v edges equal=%v", bytes.Equal(gotNodes, wantNodes), bytes.Equal(gotEdges, wantEdges))
	}
}
