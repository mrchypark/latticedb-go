package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

func bpEncode(t *testing.T, v persistedValue) []byte {
	t.Helper()
	var buf bytes.Buffer
	e := &binaryEncoder{out: &buf}
	e.value(v, 0)
	if e.err != nil {
		t.Fatalf("encode error: %v", e.err)
	}
	return buf.Bytes()
}

func bpDecode(t *testing.T, data []byte) persistedValue {
	t.Helper()
	d := newBinaryDecoder(bytes.NewReader(data), uint64(len(data)), 1<<20)
	v := d.value(0)
	if d.err != nil {
		t.Fatalf("decode error: %v", d.err)
	}
	if err := d.finish(); err != nil {
		t.Fatalf("finish error: %v", err)
	}
	return v
}

func TestBinaryPayloadRoundtrip(t *testing.T) {
	unicode := "한글테스트 ABC✓"
	cases := []struct {
		name  string
		value persistedValue
	}{
		{"empty", persistedValue{}},
		{"null", persistedValue{Kind: "null"}},
		{"bool_false", persistedValue{Kind: "bool", Bool: false}},
		{"bool_true", persistedValue{Kind: "bool", Bool: true}},
		{"int_zero", persistedValue{Kind: "int", Int: 0}},
		{"int_max", persistedValue{Kind: "int", Int: math.MaxInt64}},
		{"int_min", persistedValue{Kind: "int", Int: math.MinInt64}},
		{"int_neg", persistedValue{Kind: "int", Int: -42}},
		{"float_zero", persistedValue{Kind: "float", Float: 0}},
		{"float_neg", persistedValue{Kind: "float", Float: -3.14}},
		{"float_tiny", persistedValue{Kind: "float", Float: math.SmallestNonzeroFloat64}},
		{"float_max", persistedValue{Kind: "float", Float: math.MaxFloat64}},
		{"string_empty", persistedValue{Kind: "string", String: ""}},
		{"string_hello", persistedValue{Kind: "string", String: "hello"}},
		{"string_unicode", persistedValue{Kind: "string", String: unicode}},
		{"bytes_nil", persistedValue{Kind: "bytes", Bytes: nil}},
		{"bytes_data", persistedValue{Kind: "bytes", Bytes: []byte{0, 1, 0xff}}},
		{"vector_nil", persistedValue{Kind: "vector", Vector: nil}},
		{"vector_data", persistedValue{Kind: "vector", Vector: []float32{1.5, -2.5, 0}}},
		{"list_nil", persistedValue{Kind: "list", List: nil}},
		{"list_empty", persistedValue{Kind: "list", List: []persistedValue{}}},
		{"list_mixed", persistedValue{Kind: "list", List: []persistedValue{
			{Kind: "int", Int: 1},
			{Kind: "string", String: "two"},
			{Kind: "bool", Bool: true},
		}}},
		{"map_nil", persistedValue{Kind: "map", Map: nil}},
		{"map_empty", persistedValue{Kind: "map", Map: map[string]persistedValue{}}},
		{"map_data", persistedValue{Kind: "map", Map: map[string]persistedValue{
			"a": {Kind: "int", Int: 1},
			"b": {Kind: "string", String: "two"},
		}}},
		{"nested_list_of_maps", persistedValue{Kind: "list", List: []persistedValue{
			{Kind: "map", Map: map[string]persistedValue{"k": {Kind: "float", Float: 9.81}}},
		}}},
		{"nested_map_with_list", persistedValue{Kind: "map", Map: map[string]persistedValue{
			"nums": {Kind: "list", List: []persistedValue{
				{Kind: "int", Int: 10},
				{Kind: "int", Int: 20},
			}},
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := bpEncode(t, tc.value)
			got := bpDecode(t, data)
			assertPersistedValueEqual(t, got, tc.value)
		})
	}
}

func assertPersistedValueEqual(t *testing.T, got, want persistedValue) {
	t.Helper()
	if got.Kind != want.Kind {
		t.Errorf("Kind = %q, want %q", got.Kind, want.Kind)
		return
	}
	switch want.Kind {
	case "null", "":
	case "bool":
		if got.Bool != want.Bool {
			t.Errorf("Bool = %v, want %v", got.Bool, want.Bool)
		}
	case "int":
		if got.Int != want.Int {
			t.Errorf("Int = %d, want %d", got.Int, want.Int)
		}
	case "float":
		if got.Float != want.Float {
			t.Errorf("Float = %v, want %v", got.Float, want.Float)
		}
	case "string":
		if got.String != want.String {
			t.Errorf("String = %q, want %q", got.String, want.String)
		}
	case "bytes":
		if !bytes.Equal(got.Bytes, want.Bytes) {
			t.Errorf("Bytes = %v, want %v", got.Bytes, want.Bytes)
		}
	case "vector":
		if len(got.Vector) != len(want.Vector) {
			t.Errorf("Vector len = %d, want %d", len(got.Vector), len(want.Vector))
		} else {
			for i := range got.Vector {
				if got.Vector[i] != want.Vector[i] {
					t.Errorf("Vector[%d] = %v, want %v", i, got.Vector[i], want.Vector[i])
				}
			}
		}
	case "list":
		if len(got.List) != len(want.List) {
			t.Errorf("List len = %d, want %d", len(got.List), len(want.List))
		} else {
			for i := range got.List {
				assertPersistedValueEqual(t, got.List[i], want.List[i])
			}
		}
	case "map":
		if len(got.Map) != len(want.Map) {
			t.Errorf("Map len = %d, want %d", len(got.Map), len(want.Map))
		} else {
			for k, wv := range want.Map {
				gv, ok := got.Map[k]
				if !ok {
					t.Errorf("Map missing key %q", k)
				} else {
					assertPersistedValueEqual(t, gv, wv)
				}
			}
		}
	}
}

func putUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

func putVarint(buf *bytes.Buffer, v int64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	buf.Write(tmp[:n])
}

// bpDecodeRaw decodes without checking finish(); used for trailing-bytes test.
func bpDecodeRaw(t *testing.T, data []byte) (persistedValue, error) {
	t.Helper()
	d := newBinaryDecoder(bytes.NewReader(data), uint64(len(data)), 1<<20)
	v := d.value(0)
	if d.err != nil {
		return v, d.err
	}
	return v, d.finish()
}

func TestBinaryPayloadMalformed(t *testing.T) {
	cases := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{"unknown_tag", []byte{99}, "unknown binary property tag"},
		{"truncated_bool", []byte{binaryBoolValue}, "unexpected EOF"},
		{
			"invalid_boolean_flag",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryBoolValue)
				buf.WriteByte(5)
				return buf.Bytes()
			}(),
			"invalid binary boolean",
		},
		{"truncated_int", []byte{binaryIntValue}, "unexpected EOF"},
		{"truncated_float", []byte{binaryFloatValue, 0, 0, 0, 0, 0, 0}, "unexpected EOF"},
		{
			"nonfinite_float_nan",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryFloatValue)
				var tmp [8]byte
				binary.BigEndian.PutUint64(tmp[:], math.Float64bits(math.NaN()))
				buf.Write(tmp[:])
				return buf.Bytes()
			}(),
			"non-finite binary property",
		},
		{
			"nonfinite_float_inf",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryFloatValue)
				var tmp [8]byte
				binary.BigEndian.PutUint64(tmp[:], math.Float64bits(math.Inf(1)))
				buf.Write(tmp[:])
				return buf.Bytes()
			}(),
			"non-finite binary property",
		},
		{"truncated_string", []byte{binaryStringValue, 5, 'h', 'i'}, "invalid binary string length"},
		{
			"truncated_vector",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryVectorValue)
				putUvarint(&buf, 3)
				var tmp [4]byte
				binary.BigEndian.PutUint32(tmp[:], math.Float32bits(1.0))
				buf.Write(tmp[:])
				return buf.Bytes()
			}(),
			"unexpected EOF",
		},
		{
			"nonfinite_vector",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryVectorValue)
				putUvarint(&buf, 2)
				var tmp [4]byte
				binary.BigEndian.PutUint32(tmp[:], math.Float32bits(float32(math.Inf(1))))
				buf.Write(tmp[:])
				return buf.Bytes()
			}(),
			"non-finite binary vector",
		},
		{
			"duplicate_map_key",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryMapValue)
				putUvarint(&buf, 3)
				putUvarint(&buf, 3)
				buf.WriteString("dup")
				buf.WriteByte(binaryIntValue)
				putVarint(&buf, 1)
				putUvarint(&buf, 3)
				buf.WriteString("dup")
				buf.WriteByte(binaryIntValue)
				putVarint(&buf, 2)
				return buf.Bytes()
			}(),
			"duplicate binary property key",
		},
		{
			"oversized_string_length",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryStringValue)
				putUvarint(&buf, ^uint64(0))
				return buf.Bytes()
			}(),
			"invalid binary string length",
		},
		{
			"oversized_collection_length",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryListValue)
				putUvarint(&buf, ^uint64(0))
				return buf.Bytes()
			}(),
			"invalid binary collection length",
		},
		{
			"depth_max_enforced",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryListValue)
				putUvarint(&buf, 2)
				for range maxBinaryValueDepth {
					buf.WriteByte(binaryMapValue)
					putUvarint(&buf, 2)
					putUvarint(&buf, 1)
					buf.WriteByte('k')
				}
				buf.WriteByte(binaryIntValue)
				putVarint(&buf, 42)
				return buf.Bytes()
			}(),
			"nesting",
		},
		{
			"trailing_bytes",
			[]byte{binaryEmptyValue, 0xff},
			"trailing data",
		},
		{
			"truncated_map_no_data",
			func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(binaryMapValue)
				putUvarint(&buf, 2)
				putUvarint(&buf, 5)
				return buf.Bytes()
			}(),
			"invalid binary string length",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bpDecodeRaw(t, tc.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tc.wantErr != "" && !contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Exercise the parser without the outer CRC rejecting mutations first.
func FuzzBinaryStatePayload(f *testing.F) {
	var seed bytes.Buffer
	encoder := binaryEncoder{out: &seed}
	encoder.state(persistedState{DatabaseID: "0123456789abcdef0123456789abcdef", NextNodeID: 1, NextEdgeID: 1})
	f.Add(seed.Bytes())
	f.Add([]byte{255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > fuzzMaxInputBytes {
			return
		}
		state, err := decodeBinaryStatePayload(bytes.NewReader(data), uint64(len(data)), fuzzMaxInputBytes)
		if err != nil {
			return
		}
		var output bytes.Buffer
		encoder := binaryEncoder{out: &output}
		encoder.state(*state)
		if encoder.err != nil {
			t.Fatal(encoder.err)
		}
		decoded, err := decodeBinaryStatePayload(bytes.NewReader(output.Bytes()), uint64(output.Len()), fuzzMaxInputBytes)
		if err != nil || !reflect.DeepEqual(state, decoded) {
			t.Fatalf("binary payload round trip: %v", err)
		}
	})
}
