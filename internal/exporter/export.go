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

type exportedGraph struct {
	Nodes []exportedNode `json:"nodes"`
	Edges []exportedEdge `json:"edges"`
}

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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch format {
	case ExportFormatJSON:
		return exportJSONContext(ctx, graph, outputPath)
	case ExportFormatJSONL:
		return exportJSONLContext(ctx, graph, outputPath)
	case ExportFormatCSV:
		return exportCSV(ctx, graph, outputPath)
	case ExportFormatDOT:
		return exportDOTContext(ctx, graph, outputPath)
	default:
		return nil, fmt.Errorf("unsupported export format %q", format)
	}
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
	var output bytes.Buffer
	if err := DumpGraphContextTo(ctx, graph, &output); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func DumpGraphContextTo(ctx context.Context, graph *store.GraphState, output io.Writer) error {
	output = contextOutputWriter{ctx: ctx, output: output}
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
	switch format {
	case ExportFormatJSON:
		return DumpGraphContextTo(ctx, graph, output)
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

func exportJSON(graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportJSONContext(context.Background(), graph, outputPath)
}

func exportJSONContext(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	data, err := DumpGraphContext(ctx, graph)
	if err != nil {
		return nil, err
	}
	if err := writeAtomicContext(ctx, outputPath, data); err != nil {
		return nil, err
	}
	return data, nil
}

func exportJSONL(graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportJSONLContext(context.Background(), graph, outputPath)
}

func exportJSONLContext(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	var output bytes.Buffer
	if err := exportJSONLContextTo(ctx, graph, &output); err != nil {
		return nil, err
	}
	data := output.Bytes()
	if err := writeAtomicContext(ctx, outputPath, data); err != nil {
		return nil, err
	}
	return data, nil
}

func exportJSONLTo(graph *store.GraphState, output io.Writer) error {
	return exportJSONLContextTo(context.Background(), graph, output)
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
	unlock, err := acquireExportLockContext(ctx, outputPath)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base := strings.TrimSuffix(outputPath, filepath.Ext(outputPath))
	nodesPath := base + "_nodes.csv"
	edgesPath := base + "_edges.csv"
	generationsPath := base + "_csv_generations"
	if err := os.MkdirAll(generationsPath, 0o700); err != nil {
		return nil, err
	}
	var previous csvManifest
	if data, err := os.ReadFile(outputPath); err == nil {
		_ = json.Unmarshal(data, &previous)
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
	if err := writeNodesCSVContext(ctx, graph, generationNodes); err != nil {
		return nil, err
	}
	if err := writeEdgesCSVContext(ctx, graph, generationEdges); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.Rename(buildingPath, generationPath); err != nil {
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
	if err := writeAtomicContext(ctx, outputPath, manifest); err != nil {
		return nil, err
	}
	generationPublished = true
	_ = publishCompatibility(generationNodes, nodesPath)
	_ = publishCompatibility(generationEdges, edgesPath)
	retainCSVGenerations(generationsPath, manifestValue.Generation, previous.Generation)
	return manifest, nil
}

func exportDOT(graph *store.GraphState, outputPath string) ([]byte, error) {
	return exportDOTContext(context.Background(), graph, outputPath)
}

func exportDOTContext(ctx context.Context, graph *store.GraphState, outputPath string) ([]byte, error) {
	var builder strings.Builder
	if err := exportDOTContextTo(ctx, graph, &builder); err != nil {
		return nil, err
	}

	data := []byte(builder.String())
	if err := writeAtomicContext(ctx, outputPath, data); err != nil {
		return nil, err
	}
	return data, nil
}

func exportDOTTo(graph *store.GraphState, output io.Writer) error {
	return exportDOTContextTo(context.Background(), graph, output)
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

func marshalExportGraph(graph *store.GraphState) ([]byte, error) {
	exported := exportedGraph{
		Nodes: make([]exportedNode, 0, graph.Nodes.Len()),
		Edges: make([]exportedEdge, 0, graph.Edges.Len()),
	}

	for _, nodeID := range store.SortedNodeIDs(graph) {
		node := graph.Nodes.Get(nodeID)
		props, err := exportPropertyMap(node.Properties)
		if err != nil {
			return nil, err
		}
		exported.Nodes = append(exported.Nodes, exportedNode{
			ID:         strconv.FormatUint(node.ID, 10),
			Labels:     sortedLabels(node.Labels),
			Properties: props,
		})
	}
	for _, edgeID := range sortedCanonicalEdgeIDs(graph) {
		edge := graph.Edges.Get(edgeID)
		props, err := exportPropertyMap(edge.Properties)
		if err != nil {
			return nil, err
		}
		exported.Edges = append(exported.Edges, exportedEdge{
			ID:         strconv.FormatUint(edge.ID, 10),
			Source:     strconv.FormatUint(edge.SourceID, 10),
			Target:     strconv.FormatUint(edge.TargetID, 10),
			Type:       edge.Type,
			Properties: props,
		})
	}
	return json.Marshal(exported)
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

func writeNodesCSV(graph *store.GraphState, path string) error {
	return writeNodesCSVContext(context.Background(), graph, path)
}

func writeNodesCSVContext(ctx context.Context, graph *store.GraphState, path string) error {
	file, tempPath, err := createTempOutput(path)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	defer file.Close()

	writer := csv.NewWriter(contextOutputWriter{ctx: ctx, output: file})

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
		if err := writer.Write([]string{
			strconv.FormatUint(node.ID, 10),
			strings.Join(node.Labels, "|"),
			string(propsJSON),
		}); err != nil {
			return err
		}
	}
	return finishCSVOutput(file, writer, tempPath, path)
}

func writeEdgesCSV(graph *store.GraphState, path string) error {
	return writeEdgesCSVContext(context.Background(), graph, path)
}

func writeEdgesCSVContext(ctx context.Context, graph *store.GraphState, path string) error {
	file, tempPath, err := createTempOutput(path)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	defer file.Close()

	writer := csv.NewWriter(contextOutputWriter{ctx: ctx, output: file})

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

func writeAtomic(path string, data []byte) error {
	return writeAtomicContext(context.Background(), path, data)
}

func writeAtomicContext(ctx context.Context, path string, data []byte) error {
	file, tempPath, err := createTempOutput(path)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	if _, err := (contextOutputWriter{ctx: ctx, output: file}).Write(data); err != nil {
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
	if err := ctx.Err(); err != nil {
		return err
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
	return publishTempOutput(tempPath, path)
}

func publishTempOutput(tempPath string, path string) error {
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return syncOutputDirectory(filepath.Dir(path))
}

func publishLink(source string, path string) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".latticedb-export-link-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		return err
	}
	defer os.Remove(tempPath)
	if err := os.Remove(tempPath); err != nil {
		return err
	}
	if err := os.Link(source, tempPath); err != nil {
		return err
	}
	return publishTempOutput(tempPath, path)
}

func publishCompatibility(source string, path string) error {
	if err := publishLink(source, path); err == nil {
		return nil
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, tempPath, err := createTempOutput(path)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	return publishTempOutput(tempPath, path)
}

func retainCSVGenerations(path string, current string, previous string) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() && strings.HasPrefix(name, "generation-") && name != current && name != previous {
			_ = os.RemoveAll(filepath.Join(path, name))
		}
	}
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
