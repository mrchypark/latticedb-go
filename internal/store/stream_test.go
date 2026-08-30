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
