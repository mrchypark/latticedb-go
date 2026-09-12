package store

import (
	"context"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStreamDeltaSurvivesWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.ltdb")
	base := NewGraphState()
	if err := CheckpointGraphStateAndWAL(path, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	updated := CloneGraphState(base)
	if sequence := updated.Streams.Publish("events", "created", map[string]any{"id": int64(1)}); sequence != 1 {
		t.Fatalf("sequence = %d", sequence)
	}
	updated.Streams.SetOffset("events", "worker-a", 1)
	updated.Streams.SetOffset("offset-only", "worker-b", 42)
	if err := AppendWALDelta(path, updated, 1, 1, 1, GraphDelta{StreamsChanged: true}); err != nil {
		t.Fatal(err)
	}
	if err := SimulateCrash(path); err != nil {
		t.Fatal(err)
	}
	loaded, _, _, commitID, err := LoadGraphState(path)
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 1 {
		t.Fatalf("commit ID = %d", commitID)
	}
	records := loaded.Streams.Read("events", 0, 10)
	if len(records) != 1 || records[0].Sequence != 1 || records[0].Kind != "created" || !reflect.DeepEqual(records[0].Payload, map[string]any{"id": int64(1)}) {
		t.Fatalf("records = %#v", records)
	}
	if offset, ok := loaded.Streams.GetOffset("events", "worker-a"); !ok || offset != 1 {
		t.Fatalf("offset = %d, %v", offset, ok)
	}
	if offset, ok := loaded.Streams.GetOffset("offset-only", "worker-b"); !ok || offset != 42 {
		t.Fatalf("offset-only = %d, %v", offset, ok)
	}
}

func TestStreamChunkForkTrimAndOffsetIsolation(t *testing.T) {
	base := NewStreamStore()
	for index := range 130 {
		base.Publish("events", "event", int64(index))
	}
	fork := base.Fork()
	fork.Publish("events", "event", int64(130))
	fork.SetOffset("events", "worker", 100)
	fork.Trim("events", 64)
	if records := base.Read("events", 0, 200); len(records) != 130 || records[0].Sequence != 1 {
		t.Fatalf("base records changed: first=%d len=%d", records[0].Sequence, len(records))
	}
	if _, ok := base.GetOffset("events", "worker"); ok {
		t.Fatal("fork offset changed base")
	}
	records := fork.Read("events", 64, 200)
	if len(records) != 67 || records[0].Sequence != 65 || records[len(records)-1].Sequence != 131 {
		t.Fatalf("fork records = first %d last %d len %d", records[0].Sequence, records[len(records)-1].Sequence, len(records))
	}
	if streamStoreBytes(base) != calculateStreamStoreBytes(base) || streamStoreBytes(fork) != calculateStreamStoreBytes(fork) {
		t.Fatal("incremental stream size accounting drifted")
	}
}

func TestStreamOffsetWithoutRecordsPersists(t *testing.T) {
	store := NewStreamStore()
	store.SetOffset("events", "worker", 42)
	state, err := buildPersistedStreams(store)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodePersistedStreams(state)
	if err != nil {
		t.Fatal(err)
	}
	if offset, ok := restored.GetOffset("events", "worker"); !ok || offset != 42 {
		t.Fatalf("offset = %d, %v", offset, ok)
	}
	if streamStoreBytes(restored) != calculateStreamStoreBytes(restored) {
		t.Fatal("restored stream size accounting drifted")
	}
}

func TestStreamReadChunkBoundaries(t *testing.T) {
	store := NewStreamStore()
	for index := range 130 {
		store.Publish("events", "event", int64(index))
	}
	for _, after := range []uint64{0, 63, 64, 65, 127, 128, 129} {
		records := store.Read("events", after, 2)
		if len(records) == 0 || records[0].Sequence != after+1 {
			t.Fatalf("after %d: records = %#v", after, records)
		}
	}
	if records := store.Read("events", 130, 2); len(records) != 0 {
		t.Fatalf("tail read = %#v", records)
	}
	if records := store.Read("events", math.MaxUint64, 2); len(records) != 0 {
		t.Fatalf("overflow read = %#v", records)
	}
}

func TestStreamBulkReadAndByteRetention(t *testing.T) {
	store := NewStreamStore()
	for index := range 10_000 {
		store.Publish("events", "event", int64(index))
	}
	records := store.Read("events", 0, 10_000)
	if len(records) != 10_000 || records[0].Sequence != 1 || records[len(records)-1].Sequence != 10_000 {
		t.Fatalf("bulk records = first %d last %d len %d", records[0].Sequence, records[len(records)-1].Sequence, len(records))
	}
	before := store.StreamBytes("events")
	through, trimmed := store.TrimToBytes("events", before/2)
	if !trimmed || through == 0 || store.StreamBytes("events") > before/4+streamRecordBytes(records[len(records)-1]) {
		t.Fatalf("trim = through %d, bytes %d -> %d", through, before, store.StreamBytes("events"))
	}
	retained := store.Read("events", 0, 10_000)
	if len(retained) == 0 || retained[0].Sequence != through+1 || retained[len(retained)-1].Sequence != 10_000 {
		t.Fatalf("retained range after %d = %#v", through, retained)
	}
}

func TestStreamReadBoundedStopsBeforeOversizedRecord(t *testing.T) {
	store := NewStreamStore()
	store.Publish("events", "event", strings.Repeat("x", 1_024))
	store.Publish("events", "event", "small")
	result := store.ReadBounded("events", 0, 2, streamRecordBytes(store.streams["events"].tail.records[0])-1)
	if len(result.Records) != 0 || result.LastSequence != 0 || !result.ByteLimited {
		t.Fatalf("oversized result = %#v", result)
	}
	result = store.ReadBounded("events", 0, 2, streamRecordBytes(store.streams["events"].tail.records[0]))
	if len(result.Records) != 1 || result.LastSequence != 1 || !result.ByteLimited {
		t.Fatalf("boundary result = %#v", result)
	}
}

func TestStreamByteRetentionRemovesOversizedNewestRecord(t *testing.T) {
	store := NewStreamStore()
	store.Publish("events", "event", strings.Repeat("x", 10_000))
	through, trimmed := store.TrimToBytes("events", 1_000)
	if !trimmed || through != 1 || store.StreamBytes("events") != 0 || len(store.Read("events", 0, 1)) != 0 {
		t.Fatalf("oversized trim = through %d, trimmed %v, bytes %d", through, trimmed, store.StreamBytes("events"))
	}
}

type streamCancelChecks struct {
	context.Context
	calls, cancelAt int
}

func (c *streamCancelChecks) Err() error {
	c.calls++
	if c.calls >= c.cancelAt {
		return context.Canceled
	}
	return nil
}
func TestStreamReadCancellationDuringWork(t *testing.T) {
	t.Run("chunk traversal", func(t *testing.T) {
		s := NewStreamStore()
		for range 130 {
			s.Publish("events", "event", "x")
		}
		ctx := &streamCancelChecks{Context: context.Background(), cancelAt: 2}
		_, err := s.ReadBoundedContext(ctx, "events", 0, 130, 0)
		if err != context.Canceled {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("payload estimation", func(t *testing.T) {
		s := NewStreamStore()
		s.Publish("events", "event", []any{"a", "b", "c", "d"})
		ctx := &streamCancelChecks{Context: context.Background(), cancelAt: 4}
		result, err := s.ReadBoundedContext(ctx, "events", 0, 1, 0)
		if err != context.Canceled || len(result.Records) != 0 {
			t.Fatalf("result=%v err=%v", result, err)
		}
	})
	for _, value := range []any{make([]byte, 16384), make([]float32, 16384)} {
		ctx := &streamCancelChecks{Context: context.Background(), cancelAt: 4}
		if _, err := CloneValueContext(ctx, value); err != context.Canceled {
			t.Fatalf("clone %T: %v", value, err)
		}
	}
}
func TestReadBoundedByteLimitPreservesOrder(t *testing.T) {
	s := NewStreamStore()
	s.Publish("events", "event", "first")
	s.Publish("events", "event", strings.Repeat("big", 512))
	s.Publish("events", "event", "third")
	maxBytes := streamRecordBytes(s.streams["events"].tail.records[0])
	a := s.ReadBounded("events", 0, 3, maxBytes)
	b, err := s.ReadBoundedContext(nil, "events", 0, 3, maxBytes)
	if err != nil || !reflect.DeepEqual(a, b) || !a.ByteLimited || len(a.Records) != 1 || a.LastSequence != 1 {
		t.Fatalf("plain=%v context=%v err=%v", a, b, err)
	}
}

func TestContextStreamClonePreservesValuesAndSize(t *testing.T) {
	value := map[string]any{"items": []any{[]byte{1, 2}, []float32{3, 4}, "text", int64(5)}}
	bytes, err := estimateValueBytesContext(context.Background(), value)
	if err != nil || bytes != estimateValueBytes(value) {
		t.Fatalf("bytes=%d want=%d err=%v", bytes, estimateValueBytes(value), err)
	}
	cloned, err := CloneValueContext(context.Background(), value)
	if err != nil || !reflect.DeepEqual(cloned, CloneValue(value)) {
		t.Fatalf("clone=%v err=%v", cloned, err)
	}
	items := cloned.(map[string]any)["items"].([]any)
	items[0].([]byte)[0] = 9
	items[1].([]float32)[0] = 9
	original := value["items"].([]any)
	if original[0].([]byte)[0] != 1 || original[1].([]float32)[0] != 3 {
		t.Fatal("clone aliases stored payload")
	}
}
