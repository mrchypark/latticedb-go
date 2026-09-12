package store

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestStreamRecoveryValueLimitsMatchLive(t *testing.T) {
	deep := func(depth int) (any, persistedValue) {
		var live any = int64(1)
		stored := persistedValue{Kind: "int", Int: 1}
		for range depth {
			live = []any{live}
			stored = persistedValue{Kind: "list", List: []persistedValue{stored}}
		}
		return live, stored
	}
	validDepth, storedValidDepth := deep(maxValueDepth)
	invalidDepth, storedInvalidDepth := deep(maxValueDepth + 1)
	// Shared strings exercise the aggregate byte boundary without large copies.
	half := strings.Repeat("x", maxValueBytes/2)
	tests := []struct {
		name   string
		live   any
		stored persistedValue
	}{
		{"depth_limit", validDepth, storedValidDepth},
		{"depth_over", invalidDepth, storedInvalidDepth},
		{"bytes_limit", []any{half, half}, persistedValue{Kind: "list", List: []persistedValue{{Kind: "string", String: half}, {Kind: "string", String: half}}}},
		{"bytes_over", []any{half, half, "x"}, persistedValue{Kind: "list", List: []persistedValue{{Kind: "string", String: half}, {Kind: "string", String: half}, {Kind: "string", String: "x"}}}},
		{"map_key_bytes", map[string]any{half: half + "x"}, persistedValue{Kind: "map", Map: map[string]persistedValue{half: {Kind: "string", String: half + "x"}}}},
		{"bytes", []byte{1, 2}, persistedValue{Kind: "bytes", Bytes: []byte{1, 2}}},
		{"vector", []float32{1, 2}, persistedValue{Kind: "vector", Vector: []float32{1, 2}}},
		{"nonfinite_vector", []float32{float32(math.Inf(1))}, persistedValue{Kind: "vector", Vector: []float32{float32(math.Inf(1))}}},
		{"invalid_utf8", "\xff", persistedValue{Kind: "string", String: "\xff"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, liveErr := NormalizeValue(tt.live)
			check := func(name string, recovered StreamStore, err error) {
				t.Helper()
				if (err == nil) != (liveErr == nil) || errors.Is(err, ErrValueLimit) != errors.Is(liveErr, ErrValueLimit) {
					t.Fatalf("%s: live=%v recovery=%v", name, liveErr, err)
				}
				if err == nil {
					got := recovered.streams["events"].tail.records[0].Payload
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("%s changed payload", name)
					}
				}
			}
			snapshot, err := decodePersistedStreams(persistedStreams{Streams: []persistedStream{{Name: "events", Next: 2, Records: []persistedStreamRecord{{Sequence: 1, Kind: "event", Payload: tt.stored}}}}})
			check("snapshot", snapshot, err)
			wal, err := ApplyPersistedStreamOperations(NewStreamStore(), []persistedStreamOperation{{Type: "publish", Stream: "events", Sequence: 1, Kind: "event", Payload: tt.stored}})
			check("WAL", wal, err)
		})
	}
}

func TestStreamRecoveryAggregateElements(t *testing.T) {
	leaf := persistedValue{Kind: "list", List: make([]persistedValue, 1000)}
	for i := range leaf.List {
		leaf.List[i].Kind = "null"
	}
	for _, count := range []int{999, 1000} {
		value := persistedValue{Kind: "list", List: make([]persistedValue, count)}
		for i := range value.List {
			value.List[i] = leaf
		}
		err := validateStreamValue(value, 0, &valueWalk{})
		if errors.Is(err, ErrValueLimit) != (count == 1000) {
			t.Fatalf("%d child lists: %v", count, err)
		}
	}
}

func TestChangefeedEnvelopeCountsMetadataAgainstLimits(t *testing.T) {
	list := func(count int) persistedValue {
		items := make([]persistedValue, count)
		for i := range items {
			items[i].Kind = "null"
		}
		return persistedValue{Kind: "list", List: items}
	}
	newValue := func(value persistedValue) persistedValue {
		return persistedValue{Kind: "map", Map: map[string]persistedValue{
			"key":               {Kind: "string", String: "x"},
			"node_id":           {Kind: "int", Int: 1},
			"new_value":         value,
			"new_value_omitted": {Kind: "bool", Bool: false},
		}}
	}

	for _, test := range []struct {
		name string
		good persistedValue
		bad  persistedValue
	}{
		{
			name: "elements",
			good: newValue(list(maxValueElements - 1)),
			bad:  newValue(list(maxValueElements)),
		},
		{
			name: "bytes",
			good: newValue(persistedValue{Kind: "string", String: strings.Repeat("x", maxValueBytes-1)}),
			bad:  newValue(persistedValue{Kind: "string", String: strings.Repeat("x", maxValueBytes)}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateChangefeedEnvelope(test.good.Map); err != nil {
				t.Fatalf("boundary payload rejected: %v", err)
			}
			if err := validateChangefeedEnvelope(test.bad.Map); !errors.Is(err, ErrValueLimit) {
				t.Fatalf("over-boundary payload error = %v, want ErrValueLimit", err)
			}
		})
	}
}

func TestChangefeedRecoveryPreservesMaximumPropertyKey(t *testing.T) {
	key := strings.Repeat("k", maxValueBytes)
	properties, err := NormalizeProperties(map[string]any{key: nil})
	if err != nil {
		t.Fatalf("live property normalization failed: %v", err)
	}
	if properties[key] != nil {
		t.Fatal("live property value changed")
	}
	payload := persistedValue{Kind: "map", Map: map[string]persistedValue{
		"key":               {Kind: "string", String: key},
		"node_id":           {Kind: "int", Int: 1},
		"new_value":         {Kind: "null"},
		"new_value_omitted": {Kind: "bool", Bool: false},
	}}
	if _, err := decodeStreamValue("__lattice_changes", payload); err != nil {
		t.Fatalf("recovery rejected generated maximum-key event: %v", err)
	}
	if _, err := decodeStreamValue("events", payload); !errors.Is(err, ErrValueLimit) {
		t.Fatalf("ordinary payload error = %v, want ErrValueLimit", err)
	}
}

func TestChangefeedRecoveryRejectsInvalidPropertyKeyUTF8(t *testing.T) {
	payload := persistedValue{Kind: "map", Map: map[string]persistedValue{
		"key":               {Kind: "string", String: "\xff"},
		"node_id":           {Kind: "int", Int: 1},
		"new_value":         {Kind: "null"},
		"new_value_omitted": {Kind: "bool", Bool: false},
	}}
	if _, err := decodeStreamValue("__lattice_changes", payload); err == nil {
		t.Fatal("invalid property key accepted")
	}
}
