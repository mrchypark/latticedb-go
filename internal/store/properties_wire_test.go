package store

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCompactPropertiesPreserveCanonicalWire(t *testing.T) {
	cases := []map[string]any{
		nil, {},
		{"null": nil, "bool": false, "int": int(42), "float": 1.25, "string": "한글"},
		{"bytes": []byte{0, 255}, "vector": []float32{1, -2}, "list": []any{nil, 3, map[string]any{"nested": true}}, "map": map[string]any{"key": "value"}},
		{"bytes": []byte(nil), "vector": []float32(nil), "list": []any(nil), "map": map[string]any(nil)},
	}
	for _, input := range cases {
		old, err := encodePropertyMap(input)
		if err != nil {
			t.Fatal(err)
		}
		compact, err := encodePropertyStorage(PropertiesFromMap(input))
		if err != nil {
			t.Fatal(err)
		}
		oldJSON, err := json.Marshal(old)
		if err != nil {
			t.Fatal(err)
		}
		compactJSON, err := json.Marshal(compact)
		if err != nil {
			t.Fatal(err)
		}
		if string(oldJSON) != string(compactJSON) {
			t.Fatalf("wire changed: %s != %s", oldJSON, compactJSON)
		}
		want, err := decodePropertyMap(old)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodePropertyStorage(old)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.CloneMap(), want) {
			t.Fatalf("recovery changed: %#v != %#v", got.CloneMap(), want)
		}
	}
}
