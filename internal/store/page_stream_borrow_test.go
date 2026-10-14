package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestPageStreamMaintenanceReadOwnsPayloadAndPreservesErrors(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "stream.db"), pagestore.Options{CacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	want := StreamRecord{Sequence: 1, Kind: "event", Payload: map[string]any{"bytes": []byte("owned"), "text": "retained"}}
	data, err := encodePageStreamRecord(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Put(pageStreamRecords, pageStreamRecordKey("events", 1), data); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	source := &pageStreamSource{graph: &PageGraph{Tx: read}}
	got, err := source.record(context.Background(), "events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.record(context.Background(), "events", 2); err == nil {
		t.Fatal("missing stream record accepted")
	}
	source.graph.MaxRecordBytes = 6
	_, wantErr := decodePageStreamRecord(data, "events", 1, 6)
	if _, err := source.record(context.Background(), "events", 1); err == nil || err.Error() != wantErr.Error() {
		t.Fatalf("read limit: got=%v want=%v", err, wantErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.record(ctx, "events", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload changed after unmap: got=%#v want=%#v", got, want)
	}
}
