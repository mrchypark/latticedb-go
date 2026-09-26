package store

import (
	"bytes"
	"errors"
	"testing"
)

func TestStreamCheckpointEncodingMatchesCanonicalAfterTrim(t *testing.T) {
	s := NewStreamStore()
	for i := 0; i < 257; i++ {
		s.Publish("events", "item", map[string]any{"sequence": int64(i), "bytes": []byte{0, 1, 255}})
	}
	s.Trim("events", 91)
	s.SetOffset("events", "consumer", 128)
	s.SetOffset("offset-only", "consumer", 999)
	s.Publish("trimmed", "item", nil)
	s.Trim("trimmed", 100)
	state, err := buildPersistedStreams(s)
	if err != nil {
		t.Fatal(err)
	}
	var want, got bytes.Buffer
	reference, streamed := binaryEncoder{out: &want}, binaryEncoder{out: &got}
	reference.streams(state)
	streamed.streamStore(s)
	if reference.err != nil || streamed.err != nil {
		t.Fatalf("encode errors %v / %v", reference.err, streamed.err)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatal("streaming encoder changed the existing checkpoint representation")
	}
	decoder := newBinaryDecoder(bytes.NewReader(got.Bytes()), uint64(got.Len()), 1<<20)
	recovered, err := decodePersistedStreams(decoder.streams())
	if err != nil {
		t.Fatal(err)
	}
	if err := decoder.finish(); err != nil {
		t.Fatal(err)
	}
	if records := recovered.Read("events", 0, 300); len(records) != 166 || records[0].Sequence != 92 || records[len(records)-1].Sequence != 257 {
		t.Fatalf("trimmed stream restored incorrectly: %d records", len(records))
	}
}

type rejectingCheckpointWriter struct{ err error }

func (w rejectingCheckpointWriter) Write([]byte) (int, error) { return 0, w.err }

func TestStreamCheckpointStopsAtFirstWriteFailure(t *testing.T) {
	s := NewStreamStore()
	s.Publish("events", "item", make(chan int)) // Would fail encoding if visited.
	want := errors.New("disk full")
	encoder := binaryEncoder{out: rejectingCheckpointWriter{want}}
	encoder.streamStore(s)
	if !errors.Is(encoder.err, want) {
		t.Fatalf("got %v, want original I/O error", encoder.err)
	}
}
