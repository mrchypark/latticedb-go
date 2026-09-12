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

func TestTrimmedStreamDeltaSurvivesWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trimmed-stream-wal.ltdb")
	base := NewGraphState()
	if err := CheckpointGraphStateAndWAL(path, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	updated := CloneGraphState(base)
	for index := range 130 {
		updated.Streams.Publish("events", "event", int64(index))
	}
	updated.Streams.SetOffset("events", "worker-a", 10)
	updated.Streams.SetOffset("offset-only", "worker-b", 42)
	updated.Streams.Trim("events", 10)
	updated.Streams.Publish("events", "event", int64(130))
	updated.Streams.Trim("events", 20)
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
	records := loaded.Streams.Read("events", 0, 200)
	if len(records) != 111 {
		t.Fatalf("recovered record count = %d", len(records))
	}
	first, last := records[0].Sequence, records[len(records)-1].Sequence
	if first != 21 || last != 131 {
		t.Fatalf("recovered record range = %d..%d", first, last)
	}
	if offset, ok := loaded.Streams.GetOffset("events", "worker-a"); !ok || offset != 10 {
		t.Fatalf("recovered event offset = %d, %v", offset, ok)
	}
	if offset, ok := loaded.Streams.GetOffset("offset-only", "worker-b"); !ok || offset != 42 {
		t.Fatalf("recovered offset-only = %d, %v", offset, ok)
	}
	assertStreamAccounting(t, loaded.Streams)
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
	if records := base.Read("events", 0, 200); len(records) != 130 {
		t.Fatalf("base record count changed: len=%d", len(records))
	} else if records[0].Sequence != 1 {
		first := records[0].Sequence
		t.Fatalf("base first sequence changed: %d", first)
	}
	if _, ok := base.GetOffset("events", "worker"); ok {
		t.Fatal("fork offset changed base")
	}
	records := fork.Read("events", 64, 200)
	if len(records) != 67 {
		t.Fatalf("fork record count = %d", len(records))
	}
	first, last := records[0].Sequence, records[len(records)-1].Sequence
	if first != 65 || last != 131 {
		t.Fatalf("fork record range = %d..%d", first, last)
	}
	if streamStoreBytes(base) != calculateStreamStoreBytes(base) || streamStoreBytes(fork) != calculateStreamStoreBytes(fork) {
		t.Fatal("incremental stream size accounting drifted")
	}
}

func TestStreamForkCompactionFullClearRepublishIsolation(t *testing.T) {
	base := NewStreamStore()
	for index := range 130 {
		base.Publish("events", "event", int64(index))
	}
	fork := base.Fork()
	oldTail := fork.streams["events"].tail
	fork.Trim("events", 100)
	log := fork.streams["events"]
	if log.tail == nil || log.tail == oldTail {
		t.Fatalf("fork trim did not compact: first=%d count=%d bytes=%d tail=%v", log.first, log.count, log.bytes, log.tail != nil)
	}
	for chunk := log.tail; chunk != nil; chunk = chunk.previous {
		if len(chunk.records) == 0 || chunk.records[0].Sequence < 101 {
			t.Fatalf("compacted prefix contains hidden records: first=%d", log.first)
		}
	}
	fork.Trim("events", 130)
	if log := fork.streams["events"]; log.tail != nil || log.count != 0 || log.first != 0 {
		t.Fatalf("fork full clear = tail %v count %d first %d", log.tail, log.count, log.first)
	}
	if sequence := fork.Publish("events", "event", int64(130)); sequence != 131 {
		t.Fatalf("republished sequence = %d", sequence)
	}
	records := fork.Read("events", 0, 2)
	if len(records) != 1 {
		t.Fatalf("republished record count = %d", len(records))
	}
	sequence := records[0].Sequence
	if sequence != 131 {
		t.Fatalf("republished sequence = %d", sequence)
	}
	baseRecords := base.Read("events", 0, 200)
	if len(baseRecords) != 130 {
		t.Fatalf("base record count changed = %d", len(baseRecords))
	}
	first, last := baseRecords[0].Sequence, baseRecords[len(baseRecords)-1].Sequence
	if first != 1 || last != 130 {
		t.Fatalf("base record range changed = %d..%d", first, last)
	}
	assertStreamAccounting(t, base)
	assertStreamAccounting(t, fork)
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
	if len(records) != 10_000 {
		t.Fatalf("bulk record count = %d", len(records))
	}
	first, last := records[0].Sequence, records[len(records)-1].Sequence
	if first != 1 || last != 10_000 {
		t.Fatalf("bulk record range = %d..%d", first, last)
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

func assertStreamAccounting(t *testing.T, store StreamStore) {
	t.Helper()
	if streamStoreBytes(store) != calculateStreamStoreBytes(store) {
		t.Fatalf("logical bytes = %d, calculated = %d", streamStoreBytes(store), calculateStreamStoreBytes(store))
	}
	if streamStoreSnapshotBytes(store) != calculateStreamStoreSnapshotBytes(store) {
		t.Fatalf("snapshot bytes = %d, calculated = %d", streamStoreSnapshotBytes(store), calculateStreamStoreSnapshotBytes(store))
	}
}

func TestStreamRepeatedHeadTrimsHidePrefix(t *testing.T) {
	store := NewStreamStore()
	for index := range 10_000 {
		store.Publish("events", "event", int64(index))
	}
	originalTail := store.streams["events"].tail
	for sequence := uint64(1); sequence <= 100; sequence++ {
		store.Trim("events", sequence)
	}
	log := store.streams["events"]
	if log.first != 101 || log.count != 9_900 || log.tail != originalTail {
		t.Fatalf("trimmed log = first %d count %d tail changed %v", log.first, log.count, log.tail != originalTail)
	}
	records := store.Read("events", 0, 10_000)
	if len(records) != 9_900 {
		t.Fatalf("visible record count = %d", len(records))
	}
	first := records[0].Sequence
	if first != 101 {
		t.Fatalf("visible first sequence = %d", first)
	}
	state, err := buildPersistedStreams(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Streams) != 1 {
		t.Fatalf("persisted stream count = %d", len(state.Streams))
	}
	persistedRecords := state.Streams[0].Records
	if len(persistedRecords) != 9_900 {
		t.Fatalf("persisted record count = %d", len(persistedRecords))
	}
	first = persistedRecords[0].Sequence
	if first != 101 {
		t.Fatalf("persisted first sequence = %d", first)
	}
	assertStreamAccounting(t, store)
}

func TestStreamTrimCompactsLargeRemovedPayload(t *testing.T) {
	store := NewStreamStore()
	store.Publish("events", "event", strings.Repeat("x", 1<<20))
	for index := 1; index < 10; index++ {
		store.Publish("events", "event", int64(index))
	}
	oldTail := store.streams["events"].tail
	store.Trim("events", 1)
	log := store.streams["events"]
	if log.tail == oldTail || log.tail == nil || log.tail.previous != nil || log.tail.records[0].Sequence != 2 {
		t.Fatalf("large payload was not compacted: %#v", log.tail)
	}
	if got := store.StreamBytes("events"); got != calculateStreamStoreBytes(store)-uint64(len("events"))-64 {
		t.Fatalf("stream bytes = %d", got)
	}
	assertStreamAccounting(t, store)
}

func TestTrimmedStreamSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trimmed-stream.ltdb")
	graph := NewGraphState()
	for index := range 130 {
		graph.Streams.Publish("events", "event", int64(index))
	}
	graph.Streams.Trim("events", 10)
	if err := CheckpointGraphStateAndWAL(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	restored, _, _, _, err := LoadGraphState(path)
	if err != nil {
		t.Fatal(err)
	}
	records := restored.Streams.Read("events", 0, 200)
	if len(records) != 120 {
		t.Fatalf("restored record count = %d", len(records))
	}
	first, last := records[0].Sequence, records[len(records)-1].Sequence
	if first != 11 || last != 130 {
		t.Fatalf("restored record range = %d..%d", first, last)
	}
	assertStreamAccounting(t, restored.Streams)
}

func TestStreamTrimAccountingAcrossFullClear(t *testing.T) {
	store := NewStreamStore()
	for index := range 130 {
		store.Publish("events", "event", int64(index))
	}
	assertStreamAccounting(t, store)
	store.Trim("events", 64)
	assertStreamAccounting(t, store)
	store.Trim("events", 130)
	if records := store.Read("events", 0, 1); len(records) != 0 || store.StreamBytes("events") != 0 {
		t.Fatalf("full clear records = %#v bytes = %d", records, store.StreamBytes("events"))
	}
	assertStreamAccounting(t, store)
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
