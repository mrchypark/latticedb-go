package store

import "testing"

func TestTrimSaturatedAccountingPreservesOffsetOnlyStream(t *testing.T) {
	s := NewStreamStore()
	for range 130 {
		s.Publish("events", "event", nil)
	}
	s.SetOffset("offset-only", "worker", 42)
	if s.logicalBytes != calculateStreamStoreBytes(s) {
		t.Fatalf("offset-only metadata lost: incremental=%d recalculated=%d", s.logicalBytes, calculateStreamStoreBytes(s))
	}
	s.logicalBytes, s.snapshotBytes = ^uint64(0), ^uint64(0)
	s.Trim("events", 10)
	encoded, err := buildPersistedStreams(s)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodePersistedStreams(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if s.logicalBytes != restored.logicalBytes || s.snapshotBytes != restored.snapshotBytes {
		t.Fatalf("trim fallback totals = %d/%d, recovered = %d/%d", s.logicalBytes, s.snapshotBytes, restored.logicalBytes, restored.snapshotBytes)
	}
}

func TestTrimRecountsSaturatedPhysicalPrefix(t *testing.T) {
	s := NewStreamStore()
	for range 130 {
		s.Publish("events", "event", nil)
	}
	log := s.streams["events"]
	log.tail.bytes, log.tail.snapshotBytes = ^uint64(0), ^uint64(0)
	s.Trim("events", 10)
	if s.streams["events"].tail == log.tail {
		t.Fatal("saturated physical totals were not compacted")
	}
	if s.logicalBytes != calculateStreamStoreBytes(s) || s.snapshotBytes != calculateStreamStoreSnapshotBytes(s) {
		t.Fatal("saturated physical totals changed visible accounting")
	}
}
