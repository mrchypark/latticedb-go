package store

import (
	"bytes"
	"math"
	"reflect"
	"strings"
	"testing"
)

func binaryFormatStateFixture() persistedState {
	return persistedState{
		DatabaseID:       "db-001",
		VectorDimensions: 128,
		CommitID:         7,
		NextNodeID:       50,
		NextEdgeID:       80,
		AppMetadata: []persistedAppMetadata{
			{Key: []byte("k1"), Value: []byte("v1")},
		},
		Nodes: []persistedNode{{
			ID: 1, Labels: []string{"L"},
			Properties: map[string]persistedValue{"p": {Kind: "int", Int: 99}},
		}},
		Edges: []persistedEdge{{
			ID: 10, SourceID: 1, TargetID: 2, Type: "REL",
			Properties: map[string]persistedValue{"w": {Kind: "float", Float: 3.14}},
		}},
		FTS:         []persistedFTS{{NodeID: 1, Text: "hello world"}},
		NodeIndexes: []persistedPropertyIndexDefinition{{Scope: "node", Property: "age"}},
		EdgeIndexes: []persistedPropertyIndexDefinition{{Scope: "edge", Property: "weight"}},
		Streams: persistedStreams{
			Streams: []persistedStream{{
				Name: "s1", Next: 5,
				Records: []persistedStreamRecord{{
					Sequence: 1, Kind: "evt", Payload: persistedValue{Kind: "string", String: "data"},
				}},
			}},
			Offsets: []persistedStreamOffset{{Stream: "s1", Consumer: "c1", Sequence: 3}},
		},
	}
}

func binaryFormatDeltaFixture() persistedDelta {
	return persistedDelta{
		DatabaseID: "db-002", CommitID: 12, NextNodeID: 60, NextEdgeID: 90,
		UpsertNodes: []persistedNode{{
			ID: 3, Labels: []string{"X", "Y"},
			Properties: map[string]persistedValue{"n": {Kind: "int", Int: 42}},
		}},
		DeleteNodes: []uint64{1, 2},
		UpsertEdges: []persistedEdge{{
			ID: 20, SourceID: 3, TargetID: 4, Type: "LINK",
			Properties: map[string]persistedValue{"v": {Kind: "vector", Vector: []float32{1, 2}}},
		}},
		DeleteEdges: []uint64{10},
		UpsertFTS:   []persistedFTS{{NodeID: 3, Text: "indexed"}},
		DeleteFTS:   []uint64{1},
		AppMetadata: []persistedAppMetadataChange{
			{Key: []byte("a"), Value: []byte("b"), Delete: false},
			{Key: []byte("c"), Delete: true},
		},
		Streams: &persistedStreams{
			Streams: []persistedStream{{Name: "log", Next: 10}},
		},
		StreamOperations: []persistedStreamOperation{{
			Type: "publish", Stream: "log", Consumer: "worker", Sequence: 10,
			Kind: "msg", Payload: persistedValue{Kind: "bool", Bool: true},
		}, {Type: "trim", Stream: "log", Sequence: 3}},
		CreateNodeIndexes: []persistedPropertyIndexDefinition{{Scope: "n", Property: "x"}},
		DropNodeIndexes:   []persistedPropertyIndexDefinition{{Scope: "n", Property: "old"}},
		CreateEdgeIndexes: []persistedPropertyIndexDefinition{{Scope: "e", Property: "y"}},
		DropEdgeIndexes:   []persistedPropertyIndexDefinition{{Scope: "e", Property: "stale"}},
		NodePropertyChanges: []persistedPropertyChange{{
			ID: 3, Set: map[string]persistedValue{"m": {Kind: "string", String: "new"}}, Remove: []string{"old"},
		}},
		EdgePropertyChanges: []persistedPropertyChange{{
			ID: 20, Set: map[string]persistedValue{"k": {Kind: "int", Int: 7}}, Remove: []string{"deprecated"},
		}},
	}
}

func TestBinaryFormatWALSnapshotRoundtrip(t *testing.T) {
	s := binaryFormatStateFixture()
	payload := walPayload{Kind: "snapshot", Snapshot: &s}
	data, err := encodeBinaryWALPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBinaryWALPayload(bytes.NewReader(data), uint64(len(data)), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "snapshot" {
		t.Fatalf("Kind = %q, want snapshot", got.Kind)
	}
	if !reflect.DeepEqual(*got.Snapshot, s) {
		t.Fatal("snapshot roundtrip mismatch")
	}
}

func TestBinaryFormatWALDeltaRoundtrip(t *testing.T) {
	d := binaryFormatDeltaFixture()
	payload := walPayload{Kind: "delta", Delta: &d}
	data, err := encodeBinaryWALPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBinaryWALPayload(bytes.NewReader(data), uint64(len(data)), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "delta" {
		t.Fatalf("Kind = %q, want delta", got.Kind)
	}
	if !reflect.DeepEqual(*got.Delta, d) {
		t.Fatal("delta roundtrip mismatch")
	}
}

func TestBinaryFormatWALCheckpointRoundtrip(t *testing.T) {
	data, err := encodeBinaryWALPayload(walPayload{Kind: "checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBinaryWALPayload(bytes.NewReader(data), uint64(len(data)), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "checkpoint" {
		t.Fatalf("Kind = %q, want checkpoint", got.Kind)
	}
}

func TestBinaryFormatWALPropertyDeltaRoundtrip(t *testing.T) {
	d := binaryFormatDeltaFixture()
	d.UpsertNodes = nil
	data, err := encodeBinaryWALPayload(walPayload{Kind: "property_delta", Delta: &d})
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBinaryWALPayload(bytes.NewReader(data), uint64(len(data)), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "property_delta" {
		t.Fatalf("Kind = %q, want property_delta", got.Kind)
	}
	if !reflect.DeepEqual(*got.Delta, d) {
		t.Fatal("property_delta roundtrip mismatch")
	}
}

// --- Malformed bytes ---

func buildMinimalDeltaBytes(t *testing.T) []byte {
	t.Helper()
	d := persistedDelta{
		DatabaseID: "db", CommitID: 1, NextNodeID: 1, NextEdgeID: 1,
		AppMetadata: []persistedAppMetadataChange{{
			Key: []byte("k"), Value: []byte("v"),
		}},
		Streams: &persistedStreams{},
	}
	data, err := encodeBinaryWALPayload(walPayload{Kind: "delta", Delta: &d})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestBinaryFormatMalformedAppMetadataDeleteFlag(t *testing.T) {
	data := buildMinimalDeltaBytes(t)
	keyVal := []byte{0x02, 'k', 0x02, 'v'}
	off := bytes.Index(data, keyVal)
	if off < 0 {
		t.Fatal("fixture key/value missing")
	}
	tagOff := off + len(keyVal)
	if data[tagOff] != 0 {
		t.Fatalf("expected delete flag 0 at %d, got %d", tagOff, data[tagOff])
	}
	data[tagOff] = 5
	_, err := decodeBinaryWALPayload(bytes.NewReader(data), uint64(len(data)), 1<<20)
	if err == nil || !strings.Contains(err.Error(), "invalid binary app metadata delete flag") {
		t.Fatalf("expected app metadata flag error, got: %v", err)
	}
}

func TestBinaryFormatMalformedStreamsPresenceFlag(t *testing.T) {
	data := buildMinimalDeltaBytes(t)
	keyVal := []byte{0x02, 'k', 0x02, 'v'}
	off := bytes.Index(data, keyVal)
	if off < 0 {
		t.Fatal("fixture key/value missing")
	}
	presenceOff := off + len(keyVal) + 1
	if data[presenceOff] != 1 {
		t.Fatalf("expected streams presence 1 at %d, got %d", presenceOff, data[presenceOff])
	}
	data[presenceOff] = 3
	_, err := decodeBinaryWALPayload(bytes.NewReader(data), uint64(len(data)), 1<<20)
	if err == nil || !strings.Contains(err.Error(), "invalid binary streams presence flag") {
		t.Fatalf("expected streams presence flag error, got: %v", err)
	}
}

func TestBinaryFormatStateTooShort(t *testing.T) {
	_, err := decodeBinaryStatePayload(bytes.NewReader([]byte{0x01}), 1, 1<<20)
	if err == nil {
		t.Fatal("expected error for truncated state")
	}
}

func TestBinaryFormatStateVectorOverflow(t *testing.T) {
	var buf bytes.Buffer
	e := &binaryEncoder{out: &buf}
	e.str("db")
	e.u(math.MaxUint64)
	_, err := decodeBinaryStatePayload(&buf, uint64(buf.Len()), 1<<20)
	if err == nil || !strings.Contains(err.Error(), "vector dimensions overflow") {
		t.Fatalf("expected vector dimensions overflow, got: %v", err)
	}
}
