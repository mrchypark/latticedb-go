package store

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
)

func BenchmarkLoadLatestWALV2(b *testing.B) {
	for _, frames := range []uint64{8, 256} {
		b.Run("delta_history/"+strconv.FormatUint(frames, 10), func(b *testing.B) {
			file := benchmarkWALV2File(b, frames)
			defer file.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := file.Seek(0, 0); err != nil {
					b.Fatal(err)
				}
				if _, err := loadLatestWALV2Context(context.Background(), file, maxWALFrameBytes); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkWALV2File(b *testing.B, frames uint64) *os.File {
	b.Helper()
	file, err := os.CreateTemp(b.TempDir(), "wal-*.ltdb")
	if err != nil {
		b.Fatal(err)
	}
	const databaseID = "0123456789abcdef0123456789abcdef"
	for commitID := uint64(1); commitID <= frames; commitID++ {
		var payloadValue walPayload
		if commitID == 1 {
			payloadValue = walPayload{Kind: "snapshot", Snapshot: &persistedState{DatabaseID: databaseID, CommitID: commitID, NextNodeID: 2, NextEdgeID: 1, Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{}}}}}
		} else {
			payloadValue = walPayload{Kind: "delta", Delta: &persistedDelta{DatabaseID: databaseID, CommitID: commitID, NextNodeID: 2, NextEdgeID: 1, UpsertNodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"version": {Kind: "int", Int: int64(commitID)}}}}}}
		}
		payload, err := json.Marshal(payloadValue)
		if err != nil {
			b.Fatal(err)
		}
		header, err := encodeWALHeader(databaseID, commitID, payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := file.Write(header[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := file.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	return file
}
