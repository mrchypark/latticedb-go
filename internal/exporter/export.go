package exporter

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/store"
)

type ExportFormat string

const (
	ExportFormatJSON  ExportFormat = "json"
	ExportFormatJSONL ExportFormat = "jsonl"
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatDOT   ExportFormat = "dot"
)

// ExportOptions bounds one export's emitted records and bytes. Zero leaves the
// corresponding limit unset.
type ExportOptions struct {
	MaxRecords uint64
	MaxBytes   uint64
}

var ErrOutputLimit = errors.New("export output limit exceeded")

type csvManifest struct {
	Generation string `json:"generation"`
	Nodes      string `json:"nodes"`
	Edges      string `json:"edges"`
}

type exportedNode struct {
	ID         string                   `json:"id"`
	Labels     []string                 `json:"labels"`
	Properties map[string]exportedValue `json:"properties"`
}

type exportedEdge struct {
	ID         string                   `json:"id"`
	Source     string                   `json:"source"`
	Target     string                   `json:"target"`
	Type       string                   `json:"type"`
	Properties map[string]exportedValue `json:"properties"`
}

type exportedValue struct {
	Kind   string                   `json:"kind"`
	Bool   bool                     `json:"bool,omitempty"`
	Int    int64                    `json:"int,omitempty"`
	Float  float64                  `json:"float,omitempty"`
	String string                   `json:"string,omitempty"`
	Bytes  []byte                   `json:"bytes,omitempty"`
	Vector []float32                `json:"vector,omitempty"`
	List   []exportedValue          `json:"list,omitempty"`
	Map    map[string]exportedValue `json:"map,omitempty"`
}

func Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	graph, _, _, _, err := store.LoadGraphState(dbPath)
	if err != nil {
		return nil, err
	}

	return ExportGraph(graph, format, outputPath)
}

func ExportGraph(graph *store.GraphState, format ExportFormat, outputPath string) ([]byte, error) {
	return ExportGraphContext(context.Background(), graph, format, outputPath)
}

func ExportGraphContext(ctx context.Context, graph *store.GraphState, format ExportFormat, outputPath string) ([]byte, error) {
	return ExportGraphContextWithOptions(ctx, graph, format, outputPath, ExportOptions{})
}

func ExportGraphContextWithOptions(ctx context.Context, graph *store.GraphState, format ExportFormat, outputPath string, opts ExportOptions) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.checkRecords(graph); err != nil {
		return nil, err
	}
	outputPath, err := canonicalExportOutputPath(outputPath)
	if err != nil {
		return nil, err
	}
	unlock, err := acquireExportLockContext(ctx, outputPath)
	if err != nil {
		return nil, err
	}
	defer unlock()
	switch format {
	case ExportFormatJSON:
		return exportJSONContextWithOptions(ctx, graph, outputPath, opts)
	case ExportFormatJSONL:
		return exportJSONLContextWithOptions(ctx, graph, outputPath, opts)
	case ExportFormatCSV:
		return exportCSVWithOptions(ctx, graph, outputPath, opts)
	case ExportFormatDOT:
		return exportDOTContextWithOptions(ctx, graph, outputPath, opts)
	default:
		return nil, fmt.Errorf("unsupported export format %q", format)
	}
}

func ExportGraphFileContext(ctx context.Context, graph *store.GraphState, format ExportFormat, outputPath string) error {
	return ExportGraphFileContextWithOptions(ctx, graph, format, outputPath, ExportOptions{})
}

func ExportGraphFileContextWithOptions(ctx context.Context, graph *store.GraphState, format ExportFormat, outputPath string, opts ExportOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := opts.checkRecords(graph); err != nil {
		return err
	}
	outputPath, err := canonicalExportOutputPath(outputPath)
	if err != nil {
		return err
	}
	unlock, err := acquireExportLockContext(ctx, outputPath)
	if err != nil {
		return err
	}
	defer unlock()
	switch format {
	case ExportFormatJSON:
		_, err = writeAtomicStreamContextWithOptions(ctx, outputPath, opts, func(output io.Writer) error { return dumpGraphContextTo(ctx, graph, output) })
	case ExportFormatJSONL:
		_, err = writeAtomicStreamContextWithOptions(ctx, outputPath, opts, func(output io.Writer) error { return exportJSONLContextTo(ctx, graph, output) })
	case ExportFormatCSV:
		_, err = exportCSVWithOptions(ctx, graph, outputPath, opts)
	case ExportFormatDOT:
		_, err = writeAtomicStreamContextWithOptions(ctx, outputPath, opts, func(output io.Writer) error { return exportDOTContextTo(ctx, graph, output) })
	default:
		err = fmt.Errorf("unsupported export format %q", format)
	}
	return err
}

func Dump(dbPath string) ([]byte, error) {
	graph, _, _, _, err := store.LoadGraphState(dbPath)
	if err != nil {
		return nil, err
	}
	return DumpGraph(graph)
}

func DumpGraph(graph *store.GraphState) ([]byte, error) {
	return DumpGraphContext(context.Background(), graph)
}

func DumpGraphTo(graph *store.GraphState, output io.Writer) error {
	return DumpGraphContextTo(context.Background(), graph, output)
}

func DumpGraphContext(ctx context.Context, graph *store.GraphState) ([]byte, error) {
	return DumpGraphContextWithOptions(ctx, graph, ExportOptions{})
}

func DumpGraphContextWithOptions(ctx context.Context, graph *store.GraphState, opts ExportOptions) ([]byte, error) {
	if err := opts.checkRecords(graph); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := DumpGraphContextToWithOptions(ctx, graph, &output, opts); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func DumpGraphContextTo(ctx context.Context, graph *store.GraphState, output io.Writer) error {
	return DumpGraphContextToWithOptions(ctx, graph, output, ExportOptions{})
}

func DumpGraphContextToWithOptions(ctx context.Context, graph *store.GraphState, output io.Writer, opts ExportOptions) error {
	if err := opts.checkRecords(graph); err != nil {
		return err
	}
	return dumpGraphContextTo(ctx, graph, newExportOutputWriter(ctx, output, opts))
}

func dumpGraphContextTo(ctx context.Context, graph *store.GraphState, output io.Writer) error {
	if err := writeString(output, `{"nodes":[`); err != nil {
		return err
	}
	for index, nodeID := range store.SortedNodeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if index > 0 {
			if err := writeString(output, ","); err != nil {
				return err
			}
		}
		node := graph.Nodes.Get(nodeID)
		properties, err := exportPropertyMap(node.Properties)
		if err != nil {
			return err
		}
		data, err := json.Marshal(exportedNode{ID: strconv.FormatUint(node.ID, 10), Labels: sortedLabels(node.Labels), Properties: properties})
		if err != nil {
			return err
		}
		if err := writeBytes(output, data); err != nil {
			return err
		}
	}
	if err := writeString(output, `],"edges":[`); err != nil {
		return err
	}
	for index, edgeID := range sortedCanonicalEdgeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if index > 0 {
			if err := writeString(output, ","); err != nil {
				return err
			}
		}
		edge := graph.Edges.Get(edgeID)
		properties, err := exportPropertyMap(edge.Properties)
		if err != nil {
			return err
		}
		data, err := json.Marshal(exportedEdge{ID: strconv.FormatUint(edge.ID, 10), Source: strconv.FormatUint(edge.SourceID, 10), Target: strconv.FormatUint(edge.TargetID, 10), Type: edge.Type, Properties: properties})
		if err != nil {
			return err
		}
		if err := writeBytes(output, data); err != nil {
			return err
		}
	}
	return writeString(output, "]}")
}

func ExportGraphTo(graph *store.GraphState, format ExportFormat, output io.Writer) error {
	return ExportGraphContextTo(context.Background(), graph, format, output)
}

func ExportGraphContextTo(ctx context.Context, graph *store.GraphState, format ExportFormat, output io.Writer) error {
	return ExportGraphContextToWithOptions(ctx, graph, format, output, ExportOptions{})
}

func ExportGraphContextToWithOptions(ctx context.Context, graph *store.GraphState, format ExportFormat, output io.Writer, opts ExportOptions) error {
	if err := opts.checkRecords(graph); err != nil {
		return err
	}
	output = newExportOutputWriter(ctx, output, opts)
	switch format {
	case ExportFormatJSON:
		return dumpGraphContextTo(ctx, graph, output)
	case ExportFormatJSONL:
		return exportJSONLContextTo(ctx, graph, output)
	case ExportFormatDOT:
		return exportDOTContextTo(ctx, graph, output)
	case ExportFormatCSV:
		return errors.New("CSV export requires a filesystem output path for its generation manifest")
	default:
		return fmt.Errorf("unsupported export format %q", format)
	}
}

func SimulateCrash(dbPath string) error {
	return store.SimulateCrash(dbPath)
}

func exportJSONContext(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportJSONContextWithOptions(ctx, graph, outputPath, ExportOptions{})
}

func exportJSONContextWithOptions(ctx context.Context, graph *store.GraphState, outputPath string, opts ExportOptions) ([]byte, error) {
	data, err := DumpGraphContextWithOptions(ctx, graph, opts)
	if err != nil {
		return nil, err
	}
	if _, err := writeAtomicContext(ctx, outputPath, data); err != nil {
		return nil, err
	}
	return data, nil
}

func exportJSONLContext(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportJSONLContextWithOptions(ctx, graph, outputPath, ExportOptions{})
}

func exportJSONLContextWithOptions(ctx context.Context, graph *store.GraphState, outputPath string, opts ExportOptions) ([]byte, error) {
	var output bytes.Buffer
	if err := exportJSONLContextTo(ctx, graph, newExportOutputWriter(ctx, &output, opts)); err != nil {
		return nil, err
	}
	data := output.Bytes()
	if _, err := writeAtomicContext(ctx, outputPath, data); err != nil {
		return nil, err
	}
	return data, nil
}

func exportJSONLContextTo(ctx context.Context, graph *store.GraphState, output io.Writer) error {
	output = contextOutputWriter{ctx: ctx, output: output}
	for index, nodeID := range store.SortedNodeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		node := graph.Nodes.Get(nodeID)
		props, err := exportPropertyMap(node.Properties)
		if err != nil {
			return err
		}
		line, err := json.Marshal(map[string]any{
			"kind":       "node",
			"id":         strconv.FormatUint(node.ID, 10),
			"labels":     sortedLabels(node.Labels),
			"properties": props,
		})
		if err != nil {
			return err
		}
		if err := writeBytes(output, append(line, '\n')); err != nil {
			return err
		}
	}
	for index, edgeID := range store.SortedEdgeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		edge := graph.Edges.Get(edgeID)
		props, err := exportPropertyMap(edge.Properties)
		if err != nil {
			return err
		}
		line, err := json.Marshal(map[string]any{
			"kind":       "edge",
			"id":         strconv.FormatUint(edge.ID, 10),
			"source":     strconv.FormatUint(edge.SourceID, 10),
			"target":     strconv.FormatUint(edge.TargetID, 10),
			"type":       edge.Type,
			"properties": props,
		})
		if err != nil {
			return err
		}
		if err := writeBytes(output, append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func exportCSV(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportCSVWithOptions(ctx, graph, outputPath, ExportOptions{})
}

func exportCSVWithOptions(ctx context.Context, graph *store.GraphState, outputPath string, opts ExportOptions) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	generationsPath := outputPath + "_generations"
	if err := os.MkdirAll(generationsPath, 0o700); err != nil {
		return nil, err
	}
	if err := removeStaleCSVBuilds(generationsPath); err != nil {
		return nil, err
	}
	buildingPath, err := os.MkdirTemp(generationsPath, ".building-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(buildingPath)
	generationPath := filepath.Join(generationsPath, "generation-"+strings.TrimPrefix(filepath.Base(buildingPath), ".building-"))
	generationPublished := false
	defer func() {
		if !generationPublished {
			_ = os.RemoveAll(generationPath)
		}
	}()
	generationNodes := filepath.Join(buildingPath, "nodes.csv")
	generationEdges := filepath.Join(buildingPath, "edges.csv")
	budget := newExportBudget(opts)
	if err := writeNodesCSVContextWithBudget(ctx, graph, generationNodes, budget); err != nil {
		return nil, err
	}
	if err := writeEdgesCSVContextWithBudget(ctx, graph, generationEdges, budget); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := replaceOutput(buildingPath, generationPath); err != nil {
		return nil, err
	}
	generationNodes = filepath.Join(generationPath, "nodes.csv")
	generationEdges = filepath.Join(generationPath, "edges.csv")
	if err := syncOutputDirectory(generationsPath); err != nil {
		return nil, err
	}
	manifestNodes, err := filepath.Rel(filepath.Dir(outputPath), generationNodes)
	if err != nil {
		return nil, err
	}
	manifestEdges, err := filepath.Rel(filepath.Dir(outputPath), generationEdges)
	if err != nil {
		return nil, err
	}
	manifestValue := csvManifest{Generation: filepath.Base(generationPath), Nodes: manifestNodes, Edges: manifestEdges}
	manifest, err := json.Marshal(manifestValue)
	if err != nil {
		return nil, err
	}
	published, err := writeAtomicContextWithBudget(ctx, outputPath, manifest, budget)
	if published {
		generationPublished = true
	}
	if err != nil {
		return nil, err
	}
	// ponytail: generations remain immutable because readers have no lease;
	// add explicit reader leases before reclaiming them automatically.
	return manifest, nil
}

func exportDOTContext(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportDOTContextWithOptions(ctx, graph, outputPath, ExportOptions{})
}

func exportDOTContextWithOptions(ctx context.Context, graph *store.GraphState, outputPath string, opts ExportOptions) ([]byte, error) {
	var builder strings.Builder
	if err := exportDOTContextTo(ctx, graph, newExportOutputWriter(ctx, &builder, opts)); err != nil {
		return nil, err
	}
	data := []byte(builder.String())
	if _, err := writeAtomicContext(ctx, outputPath, data); err != nil {
		return nil, err
	}
	return data, nil
}

func exportDOTContextTo(ctx context.Context, graph *store.GraphState, output io.Writer) error {
	output = contextOutputWriter{ctx: ctx, output: output}
	if err := writeString(output, "digraph G {\n"); err != nil {
		return err
	}
	for index, nodeID := range store.SortedNodeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		node := graph.Nodes.Get(nodeID)
		label := strconv.FormatUint(node.ID, 10)
		if len(node.Labels) > 0 {
			label += " " + strings.Join(node.Labels, ",")
		}
		if _, err := fmt.Fprintf(output, "  n%d [label=%q];\n", node.ID, label); err != nil {
			return err
		}
	}
	for index, edgeID := range store.SortedEdgeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		edge := graph.Edges.Get(edgeID)
		if _, err := fmt.Fprintf(output, "  n%d -> n%d [label=%q];\n", edge.SourceID, edge.TargetID, edge.Type); err != nil {
			return err
		}
	}
	return writeString(output, "}\n")
}

func writeString(output io.Writer, value string) error {
	return writeBytes(output, []byte(value))
}

func writeBytes(output io.Writer, value []byte) error {
	written, err := output.Write(value)
	if err == nil && written != len(value) {
		return io.ErrShortWrite
	}
	return err
}

type contextOutputWriter struct {
	ctx    context.Context
	output io.Writer
}

type exportBudget struct {
	maxBytes uint64
	written  uint64
}

func newExportBudget(opts ExportOptions) *exportBudget {
	return &exportBudget{maxBytes: opts.MaxBytes}
}

func newExportOutputWriter(ctx context.Context, output io.Writer, opts ExportOptions) io.Writer {
	return contextOutputWriter{ctx: ctx, output: &limitedExportWriter{output: output, budget: newExportBudget(opts)}}
}

type limitedExportWriter struct {
	output io.Writer
	budget *exportBudget
}

func (writer *limitedExportWriter) Write(value []byte) (int, error) {
	if writer.budget.maxBytes != 0 {
		if writer.budget.written > writer.budget.maxBytes || uint64(len(value)) > writer.budget.maxBytes-writer.budget.written {
			return 0, ErrOutputLimit
		}
	}
	written, err := writer.output.Write(value)
	writer.budget.written += uint64(written)
	return written, err
}

func (opts ExportOptions) checkRecords(graph *store.GraphState) error {
	if opts.MaxRecords == 0 {
		return nil
	}
	nodes := uint64(graph.Nodes.Len())
	edges := uint64(graph.Edges.Len())
	if nodes > opts.MaxRecords || edges > opts.MaxRecords-nodes {
		return ErrOutputLimit
	}
	return nil
}

func (writer contextOutputWriter) Write(value []byte) (int, error) {
	if err := writer.ctx.Err(); err != nil {
		return 0, err
	}
	written, err := writer.output.Write(value)
	if err == nil {
		err = writer.ctx.Err()
	}
	return written, err
}

func sortedCanonicalEdgeIDs(graph *store.GraphState) []uint64 {
	edgeIDs := store.SortedEdgeIDs(graph)
	slices.SortFunc(edgeIDs, func(leftID uint64, rightID uint64) int {
		left := graph.Edges.Get(leftID)
		right := graph.Edges.Get(rightID)

		switch {
		case left.SourceID < right.SourceID:
			return -1
		case left.SourceID > right.SourceID:
			return 1
		case left.TargetID < right.TargetID:
			return -1
		case left.TargetID > right.TargetID:
			return 1
		case left.Type < right.Type:
			return -1
		case left.Type > right.Type:
			return 1
		case left.ID < right.ID:
			return -1
		case left.ID > right.ID:
			return 1
		default:
			return 0
		}
	})
	return edgeIDs
}

func exportPropertyMap(in map[string]any) (map[string]exportedValue, error) {
	if len(in) == 0 {
		return map[string]exportedValue{}, nil
	}
	out := make(map[string]exportedValue, len(in))
	for key, value := range in {
		encoded, err := exportValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = encoded
	}
	return out, nil
}

func sortedLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := slices.Clone(labels)
	slices.Sort(out)
	return out
}

func exportValue(value any) (exportedValue, error) {
	switch v := value.(type) {
	case nil, bool, int64, float64, string:
		switch value := v.(type) {
		case nil:
			return exportedValue{Kind: "null"}, nil
		case bool:
			return exportedValue{Kind: "bool", Bool: value}, nil
		case int64:
			return exportedValue{Kind: "int", Int: value}, nil
		case float64:
			return exportedValue{Kind: "float", Float: value}, nil
		case string:
			return exportedValue{Kind: "string", String: value}, nil
		}
	case []byte:
		return exportedValue{Kind: "bytes", Bytes: append([]byte(nil), v...)}, nil
	case []float32:
		return exportedValue{Kind: "vector", Vector: append([]float32(nil), v...)}, nil
	case []any:
		out := make([]exportedValue, len(v))
		for i, item := range v {
			encoded, err := exportValue(item)
			if err != nil {
				return exportedValue{}, err
			}
			out[i] = encoded
		}
		return exportedValue{Kind: "list", List: out}, nil
	case map[string]any:
		mapped, err := exportPropertyMap(v)
		if err != nil {
			return exportedValue{}, err
		}
		return exportedValue{Kind: "map", Map: mapped}, nil
	default:
		normalized, err := store.NormalizeValue(value)
		if err != nil {
			return exportedValue{}, err
		}
		if reflect.TypeOf(normalized) == reflect.TypeOf(value) {
			return exportedValue{}, fmt.Errorf("unsupported export value type %T", value)
		}
		return exportValue(normalized)
	}
	return exportedValue{}, fmt.Errorf("unsupported export value type %T", value)
}

func writeNodesCSVContext(ctx context.Context, graph *store.GraphState, path string) error {
	return writeNodesCSVContextWithBudget(ctx, graph, path, newExportBudget(ExportOptions{}))
}

func writeNodesCSVContextWithBudget(ctx context.Context, graph *store.GraphState, path string, budget *exportBudget) error {
	file, tempPath, err := createTempOutput(path)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	defer file.Close()

	writer := csv.NewWriter(contextOutputWriter{ctx: ctx, output: &limitedExportWriter{output: file, budget: budget}})

	if err := writer.Write([]string{"id", "labels", "properties"}); err != nil {
		return err
	}
	for index, nodeID := range store.SortedNodeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		node := graph.Nodes.Get(nodeID)
		props, err := exportPropertyMap(node.Properties)
		if err != nil {
			return err
		}
		propsJSON, err := json.Marshal(props)
		if err != nil {
			return err
		}
		labels, err := json.Marshal(sortedLabels(node.Labels))
		if err != nil {
			return err
		}
		if err := writer.Write([]string{
			strconv.FormatUint(node.ID, 10),
			string(labels),
			string(propsJSON),
		}); err != nil {
			return err
		}
	}
	return finishCSVOutput(file, writer, tempPath, path)
}

func writeEdgesCSVContext(ctx context.Context, graph *store.GraphState, path string) error {
	return writeEdgesCSVContextWithBudget(ctx, graph, path, newExportBudget(ExportOptions{}))
}

func writeEdgesCSVContextWithBudget(ctx context.Context, graph *store.GraphState, path string, budget *exportBudget) error {
	file, tempPath, err := createTempOutput(path)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	defer file.Close()

	writer := csv.NewWriter(contextOutputWriter{ctx: ctx, output: &limitedExportWriter{output: file, budget: budget}})

	if err := writer.Write([]string{"id", "source", "target", "type", "properties"}); err != nil {
		return err
	}
	for index, edgeID := range store.SortedEdgeIDs(graph) {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		edge := graph.Edges.Get(edgeID)
		props, err := exportPropertyMap(edge.Properties)
		if err != nil {
			return err
		}
		propsJSON, err := json.Marshal(props)
		if err != nil {
			return err
		}
		if err := writer.Write([]string{
			strconv.FormatUint(edge.ID, 10),
			strconv.FormatUint(edge.SourceID, 10),
			strconv.FormatUint(edge.TargetID, 10),
			edge.Type,
			string(propsJSON),
		}); err != nil {
			return err
		}
	}
	return finishCSVOutput(file, writer, tempPath, path)
}

func writeAtomicContext(ctx context.Context, path string, data []byte) (bool, error) {
	return writeAtomicContextWithBudget(ctx, path, data, newExportBudget(ExportOptions{}))
}

func writeAtomicContextWithBudget(ctx context.Context, path string, data []byte, budget *exportBudget) (bool, error) {
	if budget.maxBytes != 0 && (budget.written > budget.maxBytes || uint64(len(data)) > budget.maxBytes-budget.written) {
		return false, ErrOutputLimit
	}
	return writeAtomicStreamContext(ctx, path, func(output io.Writer) error {
		return writeBytes(&limitedExportWriter{output: output, budget: budget}, data)
	})
}

func writeAtomicStreamContext(ctx context.Context, path string, write func(io.Writer) error) (bool, error) {
	return writeAtomicStreamContextWithOptions(ctx, path, ExportOptions{}, write)
}

func writeAtomicStreamContextWithOptions(ctx context.Context, path string, opts ExportOptions, write func(io.Writer) error) (bool, error) {
	file, tempPath, err := createTempOutput(path)
	if err != nil {
		return false, err
	}
	defer os.Remove(tempPath)
	if err := write(newExportOutputWriter(ctx, file, opts)); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return publishTempOutput(tempPath, path)
}

func createTempOutput(path string) (*os.File, string, error) {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".latticedb-export-*.tmp")
	if err != nil {
		return nil, "", err
	}
	return file, file.Name(), nil
}

func finishCSVOutput(file *os.File, writer *csv.Writer, tempPath string, path string) error {
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	_, err := publishTempOutput(tempPath, path)
	return err
}

func publishTempOutput(tempPath string, path string) (bool, error) {
	return publishTempOutputWithSync(tempPath, path, syncOutputDirectory)
}

func publishTempOutputWithSync(tempPath string, path string, syncDirectory func(string) error) (bool, error) {
	if err := replaceOutput(tempPath, path); err != nil {
		return false, err
	}
	return true, syncDirectory(filepath.Dir(path))
}

func removeStaleCSVBuilds(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() && strings.HasPrefix(name, ".building-") {
			if err := os.RemoveAll(filepath.Join(path, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func syncOutputDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}
