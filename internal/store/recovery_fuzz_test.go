package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

const (
	fuzzMaxInputBytes = 64 << 10
	fuzzMaxWork       = 100_000
)

func FuzzDeserializeGraphState(f *testing.F) {
	graph := NewGraphState()
	graph.DatabaseID = "0123456789abcdef0123456789abcdef"
	checkpoint, err := SerializeGraphState(graph, 1, 1, 0)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(checkpoint)
	f.Add([]byte("not a checkpoint"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInputBytes {
			return
		}
		graph, nextNodeID, nextEdgeID, commitID, err := DeserializeGraphStateWithRecoveryLimits(data, fuzzMaxInputBytes, fuzzMaxWork, fuzzMaxInputBytes, RecoveryLimits{MaxDecodedBytes: fuzzMaxInputBytes, MaxWork: fuzzMaxWork})
		if err != nil {
			return
		}
		encoded, err := SerializeGraphState(graph, nextNodeID, nextEdgeID, commitID)
		if err != nil {
			t.Fatal(err)
		}
		roundTripped, gotNodeID, gotEdgeID, gotCommitID, err := DeserializeGraphStateWithRecoveryLimits(encoded, fuzzMaxInputBytes, fuzzMaxWork, fuzzMaxInputBytes, RecoveryLimits{MaxDecodedBytes: fuzzMaxInputBytes, MaxWork: fuzzMaxWork})
		if err != nil {
			t.Fatal(err)
		}
		if gotNodeID != nextNodeID || gotEdgeID != nextEdgeID || gotCommitID != commitID {
			t.Fatalf("round trip IDs = %d/%d/%d, want %d/%d/%d", gotNodeID, gotEdgeID, gotCommitID, nextNodeID, nextEdgeID, commitID)
		}
		encodedAgain, err := SerializeGraphState(roundTripped, gotNodeID, gotEdgeID, gotCommitID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encodedAgain, encoded) {
			t.Fatal("round trip checkpoint is not canonical")
		}
	})
}

func FuzzLoadLatestWALFrames(f *testing.F) {
	graph := NewGraphState()
	graph.DatabaseID = "0123456789abcdef0123456789abcdef"
	snapshot, err := buildPersistedState(graph, 1, 1, 0)
	if err != nil {
		f.Fatal(err)
	}
	payload, err := json.Marshal(walPayload{Kind: "snapshot", Snapshot: &snapshot})
	if err != nil {
		f.Fatal(err)
	}
	record, err := encodeWALRecord(snapshot, payload)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(record)
	f.Add([]byte("not a WAL frame"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInputBytes {
			return
		}
		file, err := os.CreateTemp("", "latticedb-wal-fuzz-*")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			file.Close()
			os.Remove(file.Name())
		}()
		if _, err := file.Write(data); err != nil {
			t.Fatal(err)
		}
		if _, err := file.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		state, err := loadLatestWALV2ContextWithRecoveryBudget(context.Background(), file, fuzzMaxInputBytes, nil, &recoveryBudget{limits: RecoveryLimits{MaxDecodedBytes: fuzzMaxInputBytes, MaxFrames: 4, MaxWork: fuzzMaxWork}})
		if err != nil {
			return
		}
		if _, _, _, _, err := decodePersistedStateContext(context.Background(), *state, fuzzMaxWork, fuzzMaxInputBytes); err != nil && !errors.Is(err, ErrLoadResourceLimit) {
			t.Fatal(err)
		}
	})
}

func FuzzNestedValueRoundTrip(f *testing.F) {
	for _, seed := range [][]byte{{0}, {1, 2, 3}, {7, 6, 5, 4, 3, 2, 1}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInputBytes {
			return
		}
		want := fuzzNestedValue(data)
		encoded, err := encodeValue(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeValue(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip = %#v, want %#v", got, want)
		}
	})
}

func fuzzNestedValue(data []byte) any {
	index := 0
	next := func() byte {
		if len(data) == 0 {
			return 0
		}
		value := data[index%len(data)]
		index++
		return value
	}
	var value func(depth int) any
	value = func(depth int) any {
		if depth == 8 {
			return string([]byte{next()})
		}
		switch next() % 8 {
		case 0:
			return nil
		case 1:
			return next()%2 == 0
		case 2:
			return int64(next())
		case 3:
			return float64(next()) / 3
		case 4:
			return string([]byte{next(), next()})
		case 5:
			return []byte{next(), next()}
		case 6:
			if next()%2 == 0 {
				list := make([]any, int(next()%3))
				for index := range list {
					list[index] = value(depth + 1)
				}
				return list
			}
			mapped := make(map[string]any, int(next()%3))
			for count := int(next() % 3); count > 0; count-- {
				mapped[string(rune('a'+next()%26))] = value(depth + 1)
			}
			return mapped
		case 7:
			return []float32{float32(next()) / 3, float32(next()) / 3}
		}
		return nil
	}
	return value(0)
}
