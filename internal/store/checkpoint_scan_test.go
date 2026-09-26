package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"
)

func checkpointScanFixture() persistedState {
	return persistedState{
		DatabaseID:       "0123456789abcdef0123456789abcdef",
		VectorDimensions: 8,
		CommitID:         17,
		NextNodeID:       3,
		NextEdgeID:       2,
		AppMetadata:      []persistedAppMetadata{{Key: []byte("app-key"), Value: []byte("app-value")}},
		Nodes: []persistedNode{{
			ID: 1, Labels: []string{"Item"},
			Properties: map[string]persistedValue{"name": {Kind: "string", String: "first"}},
		}, {ID: 2, Properties: map[string]persistedValue{}}},
		Edges: []persistedEdge{{
			ID: 1, SourceID: 1, TargetID: 2, Type: "LINK",
			Properties: map[string]persistedValue{"weight": {Kind: "int", Int: 4}},
		}},
		FTS:         []persistedFTS{{NodeID: 1, Text: "search text"}},
		NodeIndexes: []persistedPropertyIndexDefinition{{Scope: "Item", Property: "name"}},
		EdgeIndexes: []persistedPropertyIndexDefinition{{Scope: "LINK", Property: "weight"}},
		Streams: persistedStreams{
			Streams: []persistedStream{{Name: "events", Next: 3, Records: []persistedStreamRecord{
				{Sequence: 1, Kind: "created", Payload: persistedValue{Kind: "string", String: "one"}},
				{Sequence: 2, Kind: "created", Payload: persistedValue{Kind: "int", Int: 2}},
			}}},
			Offsets: []persistedStreamOffset{{Stream: "events", Consumer: "worker", Sequence: 1}},
		},
	}
}

func checkpointScanFixtureBytes(t *testing.T, state persistedState) []byte {
	t.Helper()
	var payload bytes.Buffer
	graph := NewGraphState()
	graph.DatabaseID = state.DatabaseID
	graph.VectorDimensions = state.VectorDimensions
	for _, entry := range state.AppMetadata {
		graph.AppMetadata.Set(string(entry.Key), entry.Value)
	}
	for _, node := range state.Nodes {
		properties, err := decodePropertyStorage(node.Properties)
		if err != nil {
			t.Fatal(err)
		}
		graph.Nodes.Set(node.ID, &NodeRecord{ID: node.ID, Labels: node.Labels, Properties: properties})
	}
	for _, edge := range state.Edges {
		properties, err := decodePropertyStorage(edge.Properties)
		if err != nil {
			t.Fatal(err)
		}
		graph.Edges.Set(edge.ID, &EdgeRecord{ID: edge.ID, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: properties})
	}
	for _, record := range state.FTS {
		graph.FTS.Set(record.NodeID, &FTSRecord{Text: record.Text})
	}
	for _, definition := range state.NodeIndexes {
		graph.NodeProperties.Create(PropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property})
	}
	for _, definition := range state.EdgeIndexes {
		graph.EdgeProperties.Create(PropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property})
	}
	for _, stream := range state.Streams.Streams {
		for _, record := range stream.Records {
			value, err := decodeStreamValue(stream.Name, record.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if got := graph.Streams.Publish(stream.Name, record.Kind, value); got != record.Sequence {
				t.Fatalf("stream sequence = %d, want %d", got, record.Sequence)
			}
		}
	}
	for _, offset := range state.Streams.Offsets {
		graph.Streams.SetOffset(offset.Stream, offset.Consumer, offset.Sequence)
	}
	if err := writePersistedStateBinary(&payload, graph, state.NextNodeID, state.NextEdgeID, state.CommitID); err != nil {
		t.Fatal(err)
	}
	header, err := encodeStateHeader(graph.DatabaseID, state.CommitID, uint64(payload.Len()), crc32.ChecksumIEEE(payload.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return append(header[:], payload.Bytes()...)
}

func TestScanCheckpointV5MatchesCheckpointRecords(t *testing.T) {
	want := checkpointScanFixture()
	data := checkpointScanFixtureBytes(t, want)
	var got persistedState
	var streamByName = map[string]int{}
	header, counts, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), CheckpointScanLimits{}, CheckpointScanVisitor{
		Header: func(h CheckpointScanHeader) error {
			got.DatabaseID, got.VectorDimensions, got.CommitID = h.DatabaseID, h.VectorDimensions, h.CommitID
			got.NextNodeID, got.NextEdgeID = h.NextNodeID, h.NextEdgeID
			return nil
		},
		Metadata: func(v persistedAppMetadata) error { got.AppMetadata = append(got.AppMetadata, v); return nil },
		Node:     func(v persistedNode) error { got.Nodes = append(got.Nodes, v); return nil },
		Edge:     func(v persistedEdge) error { got.Edges = append(got.Edges, v); return nil },
		FTS:      func(v persistedFTS) error { got.FTS = append(got.FTS, v); return nil },
		NodeIndex: func(v persistedPropertyIndexDefinition) error {
			got.NodeIndexes = append(got.NodeIndexes, v)
			return nil
		},
		EdgeIndex: func(v persistedPropertyIndexDefinition) error {
			got.EdgeIndexes = append(got.EdgeIndexes, v)
			return nil
		},
		Stream: func(v persistedStream) error {
			streamByName[v.Name] = len(got.Streams.Streams)
			got.Streams.Streams = append(got.Streams.Streams, v)
			return nil
		},
		StreamRecord: func(name string, v persistedStreamRecord) error {
			index, ok := streamByName[name]
			if !ok {
				t.Fatalf("stream record arrived before stream %q", name)
			}
			got.Streams.Streams[index].Records = append(got.Streams.Streams[index].Records, v)
			return nil
		},
		StreamOffset: func(v persistedStreamOffset) error {
			got.Streams.Offsets = append(got.Streams.Offsets, v)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(header, CheckpointScanHeader{DatabaseID: want.DatabaseID, VectorDimensions: want.VectorDimensions, CommitID: want.CommitID, NextNodeID: want.NextNodeID, NextEdgeID: want.NextEdgeID}) {
		t.Fatalf("header = %#v", header)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scanned state mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
	if counts != (CheckpointScanCounts{Metadata: 1, Nodes: 2, Edges: 1, FTS: 1, NodeIndexes: 1, EdgeIndexes: 1, Streams: 1, StreamRecords: 2, StreamOffsets: 1}) {
		t.Fatalf("counts = %#v", counts)
	}
}

func TestScanCheckpointV5ChecksumFailureComesAfterCallbacks(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	data[28] ^= 1
	visited := false
	_, _, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), CheckpointScanLimits{}, CheckpointScanVisitor{
		Node: func(persistedNode) error { visited = true; return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("scan error = %v, want checksum failure", err)
	}
	if !visited {
		t.Fatal("visitor was not called before final checksum validation")
	}
}

func TestScanCheckpointV5TruncationAndCallbackError(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	if _, _, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data[:len(data)-1]), CheckpointScanLimits{}, CheckpointScanVisitor{}); err == nil {
		t.Fatal("truncated checkpoint unexpectedly succeeded")
	}
	wantErr := errors.New("staging write failed")
	_, _, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), CheckpointScanLimits{}, CheckpointScanVisitor{
		Node: func(persistedNode) error { return wantErr },
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("callback error = %v, want %v", err, wantErr)
	}
}

func TestScanCheckpointV5Cancellation(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	ctx, cancel := context.WithCancel(context.Background())
	_, _, err := ScanCheckpointV5(ctx, bytes.NewReader(data), CheckpointScanLimits{}, CheckpointScanVisitor{
		Node: func(persistedNode) error { cancel(); return nil },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("scan error = %v, want context.Canceled", err)
	}
}

func TestScanCheckpointV5ManyRecordsStayWithinPerRecordBudget(t *testing.T) {
	const entries = 20_000
	state := persistedState{
		DatabaseID: "0123456789abcdef0123456789abcdef",
		NextNodeID: entries + 1,
		NextEdgeID: 1,
		Nodes:      make([]persistedNode, entries),
	}
	for i := range state.Nodes {
		state.Nodes[i] = persistedNode{ID: uint64(i + 1)}
	}
	data := checkpointScanFixtureBytes(t, state)
	visited := uint64(0)
	_, counts, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), CheckpointScanLimits{
		MaxPayloadBytes: uint64(len(data)),
		MaxEntries:      entries + 1,
		MaxRecordBytes:  64,
	}, CheckpointScanVisitor{
		Node: func(node persistedNode) error {
			if node.ID != visited+1 {
				t.Fatalf("node id %d at offset %d", node.ID, visited)
			}
			visited++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if visited != entries || counts.Nodes != entries {
		t.Fatalf("visited %d nodes, count=%d, want %d", visited, counts.Nodes, entries)
	}
}

func TestScanCheckpointV5RejectsRecordAndEntryLimits(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	for name, limits := range map[string]CheckpointScanLimits{
		"record":  {MaxRecordBytes: 4},
		"entries": {MaxEntries: 1},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), limits, CheckpointScanVisitor{})
			if !errors.Is(err, ErrLoadResourceLimit) {
				t.Fatalf("scan error = %v, want ErrLoadResourceLimit", err)
			}
		})
	}
}

func TestScanCheckpointV5RejectsTrailingBytes(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	data = append(data, 0)
	if _, _, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), CheckpointScanLimits{}, CheckpointScanVisitor{}); err == nil {
		t.Fatal("checkpoint with trailing bytes unexpectedly succeeded")
	}
}

func TestScanCheckpointV5ValidatesHeaderCommit(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	binary.BigEndian.PutUint64(data[12:20], 99)
	_, _, err := ScanCheckpointV5(context.Background(), bytes.NewReader(data), CheckpointScanLimits{}, CheckpointScanVisitor{})
	if err == nil || !strings.Contains(err.Error(), "metadata mismatch") {
		t.Fatalf("scan error = %v, want header metadata mismatch", err)
	}
}
