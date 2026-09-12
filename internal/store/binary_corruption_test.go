package store

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"testing"
)

func TestBinaryCorruptStateLengthStillFallsBackToValidWAL(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"kept": "value"})})
	if err := CheckpointGraphStateAndWALFiles(files, graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(files.State)
	if err != nil {
		t.Fatal(err)
	}
	data[stateHeaderSize] = 0xff // damaged DB-ID length; checksum intentionally stale
	if err := os.WriteFile(files.State, data, 0600); err != nil {
		t.Fatal(err)
	}
	recovered, _, _, commit, err := LoadGraphState(files.Directory)
	if err != nil {
		t.Fatal(err)
	}
	if commit != 1 || recovered.Nodes.Get(1).Properties.Get("kept") != "value" {
		t.Fatal("valid WAL fallback lost state")
	}
}

func TestBinaryLogicalEOFDoesNotDiscardCompleteWALFrame(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := AppendWALCommitFiles(files, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: graph.DatabaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 1,
		UpsertNodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{
			"a": {Kind: "vector", Vector: []float32{1, 2}},
			"z": {Kind: "string", String: strings.Repeat("z", 8192)},
		}}}}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: "delta", Delta: &delta})
	if err != nil {
		t.Fatal(err)
	}
	pattern := []byte{1, 'a', binaryVectorValue, 3, 0x3f, 0x80, 0, 0, 0x40, 0, 0, 0}
	offset := bytes.Index(payload, pattern)
	if offset < 0 {
		t.Fatal("vector fixture missing")
	}
	offset += 3
	// Count fits the remaining byte count, but not its four-byte elements.
	// The later string keeps the frame larger than the decoder's read buffer.
	malformed := append([]byte(nil), payload[:offset]...)
	malformed = binary.AppendUvarint(malformed, 3001)
	malformed = append(malformed, payload[offset+1:]...)
	header, err := encodeWALHeader(graph.DatabaseID, 1, malformed)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(files.WAL, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(append(header[:], malformed...))
	closeErr := file.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if state, err := loadLatestWALSnapshot(files.Directory); err == nil {
		t.Fatalf("complete malformed frame silently discarded; recovered commit %d", state.CommitID)
	}
}
