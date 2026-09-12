package exporter

import (
	"bytes"
	"cmp"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestForEachCanonicalEdgeUsesBoundedBatches(t *testing.T) {
	graph := store.NewGraphState()
	for id := uint64(1); id <= orderedEdgeBatchSize+904; id++ {
		graph.Edges.Set(id, &store.EdgeRecord{
			ID:       id,
			SourceID: id % 17,
			TargetID: (id * 7) % 23,
			Type:     string(rune('a' + id%5)),
		})
	}

	want := store.SortedEdgeIDs(graph)
	sort.Slice(want, func(i, j int) bool {
		left, right := graph.Edges.Get(want[i]), graph.Edges.Get(want[j])
		return cmp.Or(
			cmp.Compare(left.SourceID, right.SourceID),
			cmp.Compare(left.TargetID, right.TargetID),
			cmp.Compare(left.Type, right.Type),
			cmp.Compare(left.ID, right.ID),
		) < 0
	})
	var got []uint64
	if err := forEachCanonicalEdge(context.Background(), graph, func(edge *store.EdgeRecord) error {
		got = append(got, edge.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("canonical IDs differ: got %d, want %d", len(got), len(want))
	}
}

type cancelAfterChecks struct{ checks, cancelAt int }

func (ctx *cancelAfterChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterChecks) Done() <-chan struct{}       { return nil }
func (ctx *cancelAfterChecks) Err() error {
	ctx.checks++
	if ctx.checks >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}
func (*cancelAfterChecks) Value(any) any { return nil }

func TestForEachCanonicalEdgeChecksCancellationDuringScan(t *testing.T) {
	graph := store.NewGraphState()
	for id := uint64(1); id <= orderedEdgeBatchSize; id++ {
		graph.Edges.Set(id, &store.EdgeRecord{ID: id})
	}
	ctx := &cancelAfterChecks{cancelAt: 20}
	if err := forEachCanonicalEdge(ctx, graph, func(*store.EdgeRecord) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

type errorWriter struct{ writes, allow int }

func (writer *errorWriter) Write(value []byte) (int, error) {
	if writer.writes >= writer.allow {
		return 0, io.ErrClosedPipe
	}
	writer.writes++
	return len(value), nil
}

func TestDumpGraphPropagatesWriterErrorWithOrderedTraversal(t *testing.T) {
	graph := store.NewGraphState()
	graph.Edges.Set(1, &store.EdgeRecord{ID: 1})
	if err := DumpGraphContextTo(context.Background(), graph, &errorWriter{allow: 2}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("error = %v, want io.ErrClosedPipe", err)
	}
}

func TestExportFormatsPreserveTheirOrderingContracts(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2})
	graph.Edges.Set(1, &store.EdgeRecord{ID: 1, SourceID: 2, TargetID: 1, Type: "A"})
	graph.Edges.Set(2, &store.EdgeRecord{ID: 2, SourceID: 1, TargetID: 2, Type: "B"})

	jsonData, err := DumpGraphContext(context.Background(), graph)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON := `{"nodes":[{"id":"1","labels":null,"properties":{}},{"id":"2","labels":null,"properties":{}}],"edges":[{"id":"2","source":"1","target":"2","type":"B","properties":{}},{"id":"1","source":"2","target":"1","type":"A","properties":{}}]}`
	if !bytes.Equal(jsonData, []byte(wantJSON)) {
		t.Fatalf("JSON ordering changed:\n%s", jsonData)
	}

	var jsonl bytes.Buffer
	if err := ExportGraphContextTo(context.Background(), graph, ExportFormatJSONL, &jsonl); err != nil {
		t.Fatal(err)
	}
	wantJSONL := "{\"id\":\"1\",\"kind\":\"node\",\"labels\":null,\"properties\":{}}\n" +
		"{\"id\":\"2\",\"kind\":\"node\",\"labels\":null,\"properties\":{}}\n" +
		"{\"id\":\"1\",\"kind\":\"edge\",\"properties\":{},\"source\":\"2\",\"target\":\"1\",\"type\":\"A\"}\n" +
		"{\"id\":\"2\",\"kind\":\"edge\",\"properties\":{},\"source\":\"1\",\"target\":\"2\",\"type\":\"B\"}\n"
	if jsonl.String() != wantJSONL {
		t.Fatalf("JSONL ordering changed:\n%s", jsonl.String())
	}

	var dot bytes.Buffer
	if err := ExportGraphContextTo(context.Background(), graph, ExportFormatDOT, &dot); err != nil {
		t.Fatal(err)
	}
	wantDOT := "digraph G {\n  n1 [label=\"1\"];\n  n2 [label=\"2\"];\n  n2 -> n1 [label=\"A\"];\n  n1 -> n2 [label=\"B\"];\n}\n"
	if dot.String() != wantDOT {
		t.Fatalf("DOT ordering changed:\n%s", dot.String())
	}

	output := filepath.Join(t.TempDir(), "graph.csv")
	manifestData, err := ExportGraph(graph, ExportFormatCSV, output)
	if err != nil {
		t.Fatal(err)
	}
	var manifest csvManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	edgesData, err := os.ReadFile(filepath.Join(filepath.Dir(output), manifest.Edges))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(edgesData))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	wantRows := [][]string{
		{"id", "source", "target", "type", "properties"},
		{"1", "2", "1", "A", "{}"},
		{"2", "1", "2", "B", "{}"},
	}
	if !slices.EqualFunc(rows, wantRows, func(left, right []string) bool { return slices.Equal(left, right) }) {
		t.Fatalf("CSV ordering changed: %#v", rows)
	}
}
