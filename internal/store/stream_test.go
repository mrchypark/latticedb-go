package store

import (
	"path/filepath"
	"reflect"
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
}
