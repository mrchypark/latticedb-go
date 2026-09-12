package engine

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestChangefeedSequencePreflightBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		next    uint64
		pending uint64
		wantErr bool
	}{
		{"empty batch at exhausted tail", math.MaxUint64, 0, false},
		{"one event fits", math.MaxUint64 - 1, 1, false},
		{"one event overflows", math.MaxUint64 - 1, 2, true},
		{"event at exhausted tail", math.MaxUint64, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := preflightChangefeedSequence(tt.next, tt.pending)
			if tt.wantErr {
				if !errors.Is(err, ErrResourceLimit) {
					t.Fatalf("error = %v, want ErrResourceLimit", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func persistedStreamAtSequenceForTest(t *testing.T, data []byte, next uint64) []byte {
	t.Helper()
	state := map[string]any{}
	if err := json.Unmarshal(data[64:], &state); err != nil {
		t.Fatal(err)
	}
	state["streams"] = map[string]any{
		"streams": []any{map[string]any{"name": changeStreamName, "next": next}},
	}
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	result := append([]byte(nil), data[:64]...)
	binary.BigEndian.PutUint64(result[20:28], uint64(len(payload)))
	binary.BigEndian.PutUint32(result[28:32], crc32.ChecksumIEEE(payload))
	return append(result, payload...)
}

func TestChangefeedSequencePreflightRejectsTransactionWithoutPartialState(t *testing.T) {
	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sequence-boundary.ltdb")
	if err := os.WriteFile(path, persistedStreamAtSequenceForTest(t, snapshot, math.MaxUint64-1), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	walPath := path + "-wal"
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"key": "value"}}); err != nil {
		t.Fatal(err)
	}
	txGraph, txChanges := tx.graph, tx.changes
	if err := tx.Commit(); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("commit error = %v, want ErrResourceLimit", err)
	}
	if txGraph.Streams.NextSequence(changeStreamName) != math.MaxUint64-1 || txChanges.streamsChanged || len(txChanges.streamOperations) != 0 {
		t.Fatalf("transaction changed on rejected commit: next=%d changed=%v operations=%d", txGraph.Streams.NextSequence(changeStreamName), txChanges.streamsChanged, len(txChanges.streamOperations))
	}

	db.mu.RLock()
	next := db.graph.Streams.NextSequence(changeStreamName)
	records := db.graph.Streams.Read(changeStreamName, 0, 10)
	db.mu.RUnlock()
	if next != math.MaxUint64-1 || len(records) != 0 {
		t.Fatalf("partial changefeed state: next=%d records=%#v", next, records)
	}
	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("WAL grew from %d to %d on rejected commit", before.Size(), after.Size())
	}
}
