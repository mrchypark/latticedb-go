package store

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

const (
	binaryWALCheckpoint byte = iota
	binaryWALSnapshot
	binaryWALDelta
	binaryWALPropertyDelta
)

func (e *binaryEncoder) state(state persistedState) {
	e.str(state.DatabaseID)
	e.u(uint64(state.VectorDimensions))
	e.u(state.CommitID)
	e.u(state.NextNodeID)
	e.u(state.NextEdgeID)
	e.metadata(state.AppMetadata)
	encodeBinarySlice(e, state.Nodes, e.node)
	encodeBinarySlice(e, state.Edges, e.edge)
	encodeBinarySlice(e, state.FTS, e.fts)
	e.indexes(state.NodeIndexes)
	e.indexes(state.EdgeIndexes)
	e.streams(state.Streams)
}

func (d *binaryDecoder) state() persistedState {
	var state persistedState
	state.DatabaseID = d.str()
	dimensions := d.u()
	if dimensions > 65535 {
		d.err = errors.New("binary vector dimensions overflow")
		return state
	}
	state.VectorDimensions = uint16(dimensions)
	state.CommitID = d.u()
	state.NextNodeID = d.u()
	state.NextEdgeID = d.u()
	state.AppMetadata = d.metadata()
	state.Nodes = decodeBinarySlice(d, 40, d.node)
	state.Edges = decodeBinarySlice(d, 48, d.edge)
	state.FTS = decodeBinarySlice(d, 24, d.fts)
	state.NodeIndexes = d.indexes()
	state.EdgeIndexes = d.indexes()
	state.Streams = d.streams()
	return state
}

// Checkpoints write one entity at a time, without materializing a second graph
// or an encoded whole-state byte buffer.
func writePersistedStateBinary(output io.Writer, graph *GraphState, nextNodeID, nextEdgeID, commitID uint64) error {
	nextNodeID, nextEdgeID = max(nextNodeID, 1), max(nextEdgeID, 1)
	if err := ValidateIDHighWater(nextNodeID); err != nil {
		return err
	}
	if err := ValidateIDHighWater(nextEdgeID); err != nil {
		return err
	}
	metadata, err := buildPersistedAppMetadata(graph.AppMetadata)
	if err != nil {
		return err
	}
	buffered := bufio.NewWriterSize(output, 64<<10)
	e := binaryEncoder{out: buffered}
	e.str(graph.DatabaseID)
	e.u(uint64(graph.VectorDimensions))
	e.u(commitID)
	e.u(nextNodeID)
	e.u(nextEdgeID)
	e.metadata(metadata)
	nodes, err := graph.NodeCount()
	if err != nil {
		return err
	}
	e.u(nodes + 1)
	if err := graph.VisitNodes(context.Background(), func(node *NodeRecord) error {
		if err := ValidateEntityID(node.ID); err != nil {
			return err
		}
		if err := ValidateCreateLabels(node.Labels); err != nil {
			return err
		}
		properties, err := encodePropertyStorage(node.Properties)
		if err != nil {
			return err
		}
		e.node(persistedNode{ID: node.ID, Labels: node.Labels, Properties: properties})
		return e.err
	}); err != nil {
		return err
	}
	edges, err := graph.EdgeCount()
	if err != nil {
		return err
	}
	e.u(edges + 1)
	if err := graph.VisitEdges(context.Background(), func(edge *EdgeRecord) error {
		for _, id := range []uint64{edge.ID, edge.SourceID, edge.TargetID} {
			if err := ValidateEntityID(id); err != nil {
				return err
			}
		}
		if err := ValidateEdgeType(edge.Type); err != nil {
			return err
		}
		properties, err := encodePropertyStorage(edge.Properties)
		if err != nil {
			return err
		}
		e.edge(persistedEdge{ID: edge.ID, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: properties})
		return e.err
	}); err != nil {
		return err
	}
	ftsCount, err := graph.FTSCount()
	if err != nil {
		return err
	}
	e.u(ftsCount + 1)
	if err := graph.VisitFTS(context.Background(), func(id uint64, record *FTSRecord) error {
		if err := ValidateEntityID(id); err != nil {
			return err
		}
		node, err := graph.ReadNode(id)
		if err != nil {
			return err
		}
		if record == nil || node == nil {
			return fmt.Errorf("invalid FTS record for node %d", id)
		}
		if err := ValidateFTSText(record.Text); err != nil {
			return err
		}
		e.fts(persistedFTS{NodeID: id, Text: record.Text})
		return e.err
	}); err != nil {
		return err
	}
	e.indexes(persistedPropertyIndexes(graph.NodeProperties))
	e.indexes(persistedPropertyIndexes(graph.EdgeProperties))
	e.streamStore(graph.Streams)
	if e.err != nil {
		return e.err
	}
	return buffered.Flush()
}

func writeBinaryWALPayload(output io.Writer, value walPayload) error {
	e := binaryEncoder{out: output}
	switch value.Kind {
	case "checkpoint":
		e.tag(binaryWALCheckpoint)
	case "snapshot":
		if value.Snapshot == nil {
			return errors.New("binary WAL snapshot missing")
		}
		e.tag(binaryWALSnapshot)
		e.state(*value.Snapshot)
	case "delta", "property_delta":
		if value.Delta == nil {
			return errors.New("binary WAL delta missing")
		}
		if value.Kind == "delta" {
			e.tag(binaryWALDelta)
		} else {
			e.tag(binaryWALPropertyDelta)
		}
		e.delta(*value.Delta)
	default:
		return fmt.Errorf("unknown binary WAL kind %q", value.Kind)
	}
	return e.err
}

func encodeBinaryWALPayload(value walPayload) ([]byte, error) {
	var output bytes.Buffer
	if err := writeBinaryWALPayload(&output, value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func decodeBinaryWALPayload(input io.Reader, length, maxBytes uint64) (walPayload, error) {
	d := newBinaryDecoder(input, length, maxBytes)
	var value walPayload
	kind, err := d.ReadByte()
	if err != nil {
		return value, err
	}
	switch kind {
	case binaryWALCheckpoint:
		value.Kind = "checkpoint"
	case binaryWALSnapshot:
		value.Kind = "snapshot"
		state := d.state()
		value.Snapshot = &state
	case binaryWALDelta, binaryWALPropertyDelta:
		value.Kind = "delta"
		if kind == binaryWALPropertyDelta {
			value.Kind = "property_delta"
		}
		delta := d.delta()
		value.Delta = &delta
	default:
		return value, fmt.Errorf("unknown binary WAL payload tag %d", kind)
	}
	return value, d.finish()
}

func decodeBinaryStatePayload(input io.Reader, length, maxBytes uint64) (*persistedState, error) {
	d := newBinaryDecoder(input, length, maxBytes)
	state := d.state()
	if err := d.finish(); err != nil {
		return nil, err
	}
	return &state, nil
}

func decodeWALPayloadBytes(ctx context.Context, header, payload []byte, maxBytes uint64, out *walPayload) error {
	if !isBinaryWALHeader(header) {
		return unmarshalContext(ctx, payload, out)
	}
	decoded, err := decodeBinaryWALPayload(&contextReader{ctx: ctx, reader: bytes.NewReader(payload)}, uint64(len(payload)), maxBytes)
	if err == nil {
		*out = decoded
	}
	return err
}
