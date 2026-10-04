package store

import (
	"bytes"
	"errors"
	"testing"
)

func TestPageEncoderMatchesDecoderAllocations(t *testing.T) {
	values := []any{nil, "text", []byte{1, 2}, []float32{1, 2}, []any{nil, "x", map[string]any{"k": nil}}, map[string]any{"a": nil, "b": []any{1, 2}}}
	for _, v := range values {
		p, err := encodeValue(v)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		e := binaryEncoder{out: &out, allocationLimit: 1 << 20}
		e.value(p, 0)
		if e.err != nil {
			t.Fatal(e.err)
		}
		d := newBinaryDecoder(bytes.NewReader(out.Bytes()), uint64(out.Len()), 1<<19)
		d.value(0)
		if err := d.finish(); err != nil {
			t.Fatal(err)
		}
		if want := (uint64(1) << 20) - d.allocationLeft; e.allocationBytes != want {
			t.Fatalf("%T allocation=%d decoder=%d", v, e.allocationBytes, want)
		}
	}
}

func TestPageWritesRejectDecoderExpansion(t *testing.T) {
	// A tiny wire record can require a much larger decoded map. Test all three
	// canonical record writers with the same small, deterministic read bound.
	value := map[string]any{}
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		value[key] = nil
	}
	props := PropertiesFromMap(map[string]any{"map": value})
	for _, limit := range []uint64{256, 4096} {
		node := &NodeRecord{ID: 1, Properties: props}
		edge := &EdgeRecord{ID: 1, SourceID: 1, TargetID: 2, Type: "R", Properties: props}
		stream := StreamRecord{Sequence: 1, Kind: "event", Payload: value}
		n, ne := encodePageNodeBounded(node, limit)
		e, ee := encodePageEdgeBounded(edge, limit)
		s, se := encodePageStreamRecordBounded(stream, limit)
		if limit == 256 {
			for _, err := range []error{ne, ee, se} {
				if !errors.Is(err, ErrLoadResourceLimit) {
					t.Fatalf("expected rejection: %v", err)
				}
			}
			continue
		}
		for _, err := range []error{ne, ee, se} {
			if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := decodePageNode(n, 1, limit); err != nil {
			t.Fatal(err)
		}
		if _, err := decodePageEdge(e, 1, limit); err != nil {
			t.Fatal(err)
		}
		if _, err := decodePageStreamRecord(s, "events", 1, limit); err != nil {
			t.Fatal(err)
		}
	}
}
