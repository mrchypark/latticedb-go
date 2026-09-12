package store

import "testing"

func TestWALAccumulatorPersistedStatePropagatesStreamSerializationError(t *testing.T) {
	streams := NewStreamStore()
	streams.Publish("events", "", nil)
	accumulator := &walAccumulator{streams: streams}

	_, err := accumulator.persistedState()
	if err == nil {
		t.Fatal("persistedState succeeded with an invalid stream kind")
	}
}
