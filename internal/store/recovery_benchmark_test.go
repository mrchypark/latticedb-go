package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func BenchmarkLoadLatestWALV2(b *testing.B) {
	const frames = 256
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
}

func benchmarkWALV2File(b *testing.B, frames uint64) *os.File {
	b.Helper()
	file, err := os.CreateTemp(b.TempDir(), "wal-*.ltdb")
	if err != nil {
		b.Fatal(err)
	}
	const databaseID = "0123456789abcdef0123456789abcdef"
	for commitID := uint64(1); commitID <= frames; commitID++ {
		payload, err := json.Marshal(walPayload{Kind: "snapshot", Snapshot: &persistedState{DatabaseID: databaseID, CommitID: commitID}})
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
