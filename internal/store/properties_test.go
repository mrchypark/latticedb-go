package store

import (
	"math"
	"reflect"
	"slices"
	"testing"
)

func TestPropertiesMutation(t *testing.T) {
	var p Properties
	if v := p.Get("x"); v != nil {
		t.Fatalf("absent: want nil, got %v", v)
	}
	p.Set("x", nil)
	if _, ok := p.Lookup("x"); !ok {
		t.Fatal("nil-valued: Lookup true")
	}
	for _, k := range []string{"z", "a", "m", "b", "y"} {
		p.Set(k, int64(1))
	}
	keys := make([]string, 0)
	for k := range p.All() {
		keys = append(keys, k)
	}
	if !slices.IsSorted(keys) {
		t.Fatalf("not sorted: %v", keys)
	}
	p.Delete("a")
	if _, ok := p.Lookup("a"); ok {
		t.Fatal("deleted key present")
	}
	p.Delete("missing")
	if p.Len() != 5 {
		t.Fatalf("want 5, got %d", p.Len())
	}
}
func TestPropertiesCloneCOWContainer(t *testing.T) {
	var orig Properties
	for i, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		orig.Set(k, int64(i))
	}
	clone := orig.Clone()
	clone.Set("a", int64(999))
	clone.Set("zzz", int64(-1))
	clone.Delete("j")
	if v := orig.Get("a"); v != int64(0) {
		t.Fatalf("Set on clone leaked: orig a=%v", v)
	}
	if _, ok := orig.Lookup("zzz"); ok {
		t.Fatal("insert on clone leaked")
	}
	if _, ok := orig.Lookup("j"); !ok {
		t.Fatal("delete on clone leaked")
	}
	if orig.Len() != 10 {
		t.Fatalf("orig len changed to %d", orig.Len())
	}
}
func TestPropertiesDeepClone(t *testing.T) {
	nested := map[string]any{"x": int64(1)}
	vec := []float32{1, 2, 3}
	var p Properties
	p.Set("m", nested)
	p.Set("v", vec)
	deep := p.CloneDeep()
	nested["x"] = int64(99)
	vec[0] = 99
	if deep.Get("m").(map[string]any)["x"] != int64(1) {
		t.Fatal("deep clone map not independent")
	}
	if deep.Get("v").([]float32)[0] != 1 {
		t.Fatal("deep clone vector not independent")
	}
}
func TestPropertiesCloneMapIsolation(t *testing.T) {
	var p Properties
	p.Set("k", map[string]any{"inner": int64(1)})
	m := p.CloneMap()
	m["k"].(map[string]any)["inner"] = int64(99)
	m["new"] = int64(2)
	if p.Get("k").(map[string]any)["inner"] != int64(1) {
		t.Fatal("CloneMap nested mutation leaked")
	}
	if _, ok := p.Lookup("new"); ok {
		t.Fatal("CloneMap insert leaked")
	}
}
func TestPropertiesFirstVector(t *testing.T) {
	var p Properties
	if _, ok := p.FirstVector(); ok {
		t.Fatal("empty false")
	}
	p.Set("z", []float32{3, 4, 5})
	p.Set("a", []float32{1, 2, 3})
	p.Set("n", int64(1))
	v, ok := p.FirstVector()
	if !ok || !slices.Equal(v, []float32{1, 2, 3}) {
		t.Fatalf("want [1 2 3], got %v", v)
	}
	p.Set("0", ([]float32)(nil))
	p.Set("b", []float32{9})
	v, ok = p.FirstVector()
	if !ok || !slices.Equal(v, []float32{1, 2, 3}) {
		t.Fatalf("nil skip: want [1 2 3], got %v", v)
	}
	if v, ok := p.Vector("a"); !ok || !slices.Equal(v, []float32{1, 2, 3}) {
		t.Fatal("Vector lookup failed")
	}
	if _, ok := p.Vector("n"); ok {
		t.Fatal("Vector on non-vector false")
	}
}
func TestNormalizePropertyStorageParity(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   map[string]any
	}{
		{"NaN", map[string]any{"f": math.NaN()}},
		{"+Inf", map[string]any{"f": math.Inf(1)}},
		{"bad key", map[string]any{string([]byte{0xff}): int64(1)}},
		{"bad string", map[string]any{"k": string([]byte{0xff})}},
	} {
		if _, err := NormalizePropertyStorage(tc.in); err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
	}
	for _, tc := range []struct {
		in   any
		want any
	}{
		{int(42), int64(42)}, {int32(7), int64(7)},
		{uint16(3), int64(3)}, {float32(1.5), float64(1.5)},
	} {
		p, err := NormalizePropertyStorage(map[string]any{"k": tc.in})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(p.Get("k"), tc.want) {
			t.Fatalf("%T: want %v, got %v", tc.in, tc.want, p.Get("k"))
		}
	}
	input := map[string]any{
		"s": "hello", "n": int64(7), "f": float64(2.5),
		"b": []byte{1, 2}, "v": []float32{1, 2},
		"l": []any{int64(1), "x"},
	}
	normMap, err := NormalizeProperties(input)
	if err != nil {
		t.Fatal(err)
	}
	normProps, err := NormalizePropertyStorage(input)
	if err != nil {
		t.Fatal(err)
	}
	for key, mapVal := range normMap {
		propsVal, ok := normProps.Lookup(key)
		if !ok {
			t.Fatalf("key %q missing", key)
		}
		if !reflect.DeepEqual(mapVal, propsVal) {
			t.Fatalf("%s: map=%v props=%v", key, mapVal, propsVal)
		}
	}
	p, err := NormalizePropertyStorage(map[string]any{"z": int64(1), "a": nil, "m": int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0)
	for k := range p.All() {
		keys = append(keys, k)
	}
	if !slices.IsSorted(keys) {
		t.Fatalf("not sorted: %v", keys)
	}
	if _, ok := p.Lookup("a"); !ok {
		t.Fatal("nil-valued present")
	}
	if _, ok := p.Lookup("missing"); ok {
		t.Fatal("absent absent")
	}
}
