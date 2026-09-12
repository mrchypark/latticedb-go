package store

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWALPayloadBufferPreservesFramesAndReleasesLargeValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := &WALWriter{file: file}
	defer writer.Close()
	const id = "00000000000000000000000000000001"
	var expected []byte
	for i, text := range []string{"<&>\u2028한글", strings.Repeat("x", 128<<10), "final"} {
		value := walPayload{Kind: "delta", Delta: &persistedDelta{DatabaseID: id, CommitID: uint64(i + 1),
			UpsertNodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"text": {Kind: "string", String: text}, "flag": {Kind: "bool", Bool: false}}}},
		}}
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		header, err := encodeWALHeader(id, uint64(i+1), payload)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, header[:]...)
		expected = append(expected, payload...)
		if err := writer.appendJSON(id, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		if writer.encodeBuffer.Cap() > 64<<10 || writer.encodeValue.Delta != nil {
			t.Fatal("large payload or source value retained")
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatal("buffer reuse changed Marshal-compatible WAL frames")
	}
	invalid := walPayload{Kind: "delta", Delta: &persistedDelta{UpsertNodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"bad": {Kind: "float", Float: math.NaN()}}}}}}
	if err := writer.appendJSON(id, 4, invalid); err == nil {
		t.Fatal("nonfinite value accepted")
	}
	got, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatal("encoding error wrote a partial frame")
	}
	if err := writer.appendJSON(id, 4, walPayload{Kind: "delta", Delta: &persistedDelta{DatabaseID: id, CommitID: 4}}); err != nil {
		t.Fatalf("writer failed after encoding error: %v", err)
	}
}

func TestAppendDeltaClearsReusableDeltaOnSuccessAndError(t *testing.T) {
	const id = "00000000000000000000000000000001"
	newGraph := func() *GraphState {
		graph := NewGraphState()
		graph.DatabaseID = id
		graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"value": "ok"})})
		return graph
	}
	changes := GraphDelta{UpsertNodes: []uint64{1}}

	successFile, err := os.CreateTemp(t.TempDir(), "wal-success")
	if err != nil {
		t.Fatal(err)
	}
	successWriter := &WALWriter{file: successFile, fullSync: true}
	if err := successWriter.AppendDelta(newGraph(), 2, 1, 1, changes); err != nil {
		t.Fatal(err)
	}
	if successWriter.encodeDelta.DatabaseID != "" || successWriter.encodeDelta.UpsertNodes != nil {
		t.Fatal("successful AppendDelta retained its reusable delta")
	}
	if err := successWriter.Close(); err != nil {
		t.Fatal(err)
	}

	errorFile, err := os.CreateTemp(t.TempDir(), "wal-error")
	if err != nil {
		t.Fatal(err)
	}
	errorWriter := &WALWriter{
		file: errorFile,
		writeFn: func(*os.File, []byte) (int, error) {
			return 0, os.ErrPermission
		},
	}
	if err := errorWriter.AppendDelta(newGraph(), 2, 1, 1, changes); err == nil {
		t.Fatal("failing AppendDelta unexpectedly succeeded")
	}
	if errorWriter.encodeDelta.DatabaseID != "" || errorWriter.encodeDelta.UpsertNodes != nil {
		t.Fatal("failing AppendDelta retained its reusable delta")
	}
	if err := errorWriter.Close(); err != nil {
		t.Fatal(err)
	}
}
