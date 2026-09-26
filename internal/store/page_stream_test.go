package store

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestPageStreamsAppendTrimForkReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: write}
	seed := []StreamOperation{
		{Type: "publish", Stream: "events", Sequence: 1, Kind: "created", Payload: map[string]any{"id": int64(1)}},
		{Type: "publish", Stream: "events", Sequence: 2, Kind: "updated", Payload: map[string]any{"id": int64(2)}},
		{Type: "publish", Stream: "events", Sequence: 3, Kind: "deleted", Payload: map[string]any{"id": int64(3)}},
		{Type: "offset", Stream: "events", Consumer: "worker", Sequence: 2},
	}
	if err := graph.ApplyStreamOperations(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}

	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := (&PageGraph{Tx: read}).LoadStreams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := streams.Read("events", 0, 10); len(got) != 3 {
		t.Fatalf("initial read returned %d records", len(got))
	}
	if offset, ok := streams.GetOffset("events", "worker"); !ok || offset != 2 {
		t.Fatalf("offset = %d, %v", offset, ok)
	}
	memory := NewStreamStore()
	for _, op := range seed[:3] {
		memory.Publish(op.Stream, op.Kind, op.Payload)
	}
	memory.SetOffset("events", "worker", 2)
	var diskEncoding, memoryEncoding bytes.Buffer
	diskEncoder := binaryEncoder{out: &diskEncoding}
	diskEncoder.streamStore(streams)
	memoryEncoder := binaryEncoder{out: &memoryEncoding}
	memoryEncoder.streamStore(memory)
	if diskEncoder.err != nil || memoryEncoder.err != nil || !bytes.Equal(diskEncoding.Bytes(), memoryEncoding.Bytes()) {
		t.Fatalf("page checkpoint differs from memory: disk err=%v memory err=%v", diskEncoder.err, memoryEncoder.err)
	}
	statement := streams.Fork()
	if got := statement.Publish("events", "rolled-back", map[string]any{"id": int64(4)}); got != 4 {
		t.Fatalf("fork publish sequence = %d", got)
	}
	if got, err := streams.ReadBoundedContext(context.Background(), "events", 2, 10, 0); err != nil || len(got.Records) != 1 || got.Records[0].Sequence != 3 {
		t.Fatalf("base after fork publish = %+v, %v", got, err)
	}
	if got, err := statement.ReadBoundedContext(context.Background(), "events", 2, 10, 0); err != nil || len(got.Records) != 2 || got.Records[1].Sequence != 4 {
		t.Fatalf("fork after publish = %+v, %v", got, err)
	}
	if _, err := ApplyPersistedStreamOperations(streams, []persistedStreamOperation{
		{Type: "trim", Stream: "events", Sequence: 2},
		{Type: "offset", Stream: "events", Consumer: "", Sequence: 2},
	}); err == nil {
		t.Fatal("invalid statement unexpectedly succeeded")
	}
	if got, err := streams.ReadBoundedContext(context.Background(), "events", 0, 10, 0); err != nil || len(got.Records) != 3 {
		t.Fatalf("failed statement changed base = %+v, %v", got, err)
	}
	retained := streams.Fork()
	if through, trimmed, err := retained.TrimToBytesContext(context.Background(), "events", 1); err != nil || !trimmed || through == 0 {
		t.Fatalf("page TrimToBytes = %d, %v, %v", through, trimmed, err)
	}
	if got, err := streams.ReadBoundedContext(context.Background(), "events", 0, 10, 0); err != nil || len(got.Records) != 3 {
		t.Fatalf("retention fork changed base = %+v, %v", got, err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	write, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&PageGraph{Tx: write}).ApplyStreamOperations(context.Background(), []StreamOperation{
		{Type: "trim", Stream: "events", Sequence: 1},
		{Type: "publish", Stream: "events", Sequence: 4, Kind: "created", Payload: map[string]any{"id": int64(4)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	streams, err = (&PageGraph{Tx: read}).LoadStreams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := streams.ReadBoundedContext(context.Background(), "events", 0, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []StreamRecord{
		{Sequence: 2, Kind: "updated", Payload: map[string]any{"id": int64(2)}},
		{Sequence: 3, Kind: "deleted", Payload: map[string]any{"id": int64(3)}},
		{Sequence: 4, Kind: "created", Payload: map[string]any{"id": int64(4)}},
	}
	if !reflect.DeepEqual(got.Records, want) {
		t.Fatalf("reopened records = %#v, want %#v", got.Records, want)
	}
	if streams.NextSequence("events") != 5 || streams.StreamBytes("events") == 0 {
		t.Fatalf("reopened next=%d bytes=%d", streams.NextSequence("events"), streams.StreamBytes("events"))
	}
}

func TestPageStreamReadAndTrimPropagateErrors(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "streams.db"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: write}
	if err := graph.ApplyStreamOperations(context.Background(), []StreamOperation{{Type: "publish", Stream: "events", Sequence: 1, Kind: "event", Payload: "payload"}}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := (&PageGraph{Tx: read}).LoadStreams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := streams.ReadBoundedContext(canceled, "events", 0, 1, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error = %v", err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := streams.ReadBoundedContext(context.Background(), "events", 0, 1, 0); !errors.Is(err, pagestore.ErrClosed) {
		t.Fatalf("closed transaction read error = %v", err)
	}
	if _, _, err := streams.TrimToBytesContext(context.Background(), "events", 0); !errors.Is(err, pagestore.ErrClosed) {
		t.Fatalf("closed transaction trim error = %v", err)
	}
}

func TestPageStreamInitialImport(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "streams.db"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	memory := NewStreamStore()
	memory.Publish("events", "event", "one")
	memory.Publish("events", "event", "two")
	memory.Trim("events", 1)
	memory.SetOffset("events", "worker", 2)
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: write}
	if err := graph.ImportStreams(context.Background(), memory); err != nil {
		t.Fatal(err)
	}
	if err := graph.ImportStreams(context.Background(), memory); err == nil {
		t.Fatal("second initial import unexpectedly succeeded")
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	loaded, err := (&PageGraph{Tx: read}).LoadStreams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := loaded.ReadContext(context.Background(), "events", 0, 10)
	if err != nil || len(got) != 1 || got[0].Sequence != 2 || got[0].Payload != "two" {
		t.Fatalf("imported streams = %#v, %v", got, err)
	}
	if offset, ok := loaded.GetOffset("events", "worker"); !ok || offset != 2 {
		t.Fatalf("imported offset = %d, %v", offset, ok)
	}
}
