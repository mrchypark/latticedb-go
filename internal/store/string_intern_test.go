package store

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"unique"
	"unsafe"
)

func TestInternStringIdentity(t *testing.T) {
	val := "hello"
	handle := unique.Make(val)
	got := InternString(strings.Clone(val))
	if got != handle.Value() {
		t.Fatalf("InternString = %q, want %q", got, handle.Value())
	}
	if unsafe.StringData(got) != unsafe.StringData(handle.Value()) {
		t.Fatal("backing pointer mismatch: want shared data")
	}
	runtime.KeepAlive(handle)
}

func TestInternStringSkipsEmptyAndLong(t *testing.T) {
	if got := InternString(""); got != "" {
		t.Fatalf("empty: got %q", got)
	}
	long := strings.Repeat("x", 1025)
	if got := InternString(long); unsafe.StringData(got) != unsafe.StringData(long) {
		t.Fatal("long string should not be interned")
	}
}

func TestInternStringsClone(t *testing.T) {
	src := []string{"a", "b", ""}
	out := InternStrings(src)
	if len(out) != 3 {
		t.Fatalf("len = %d", len(out))
	}
	for i := range src {
		if out[i] != src[i] {
			t.Fatalf("out[%d] = %q, want %q", i, out[i], src[i])
		}
	}
	src[0] = "changed"
	if out[0] == "changed" {
		t.Fatal("should be independent slice")
	}
}

func TestInternStringsNilEmptyPreserved(t *testing.T) {
	if out := InternStrings(nil); out != nil {
		t.Fatalf("InternStrings(nil) = %v, want nil", out)
	}
	out := InternStrings([]string{})
	if out == nil {
		t.Fatal("InternStrings(empty) should be non-nil, not nil")
	}
}

func TestNormalizePropertiesKeysAndNesting(t *testing.T) {
	handles := map[string]unique.Handle[string]{
		"k1":     unique.Make("k1"),
		"list":   unique.Make("list"),
		"nested": unique.Make("nested"),
	}

	in := map[string]any{
		"k1":     int64(1),
		"list":   []any{"a", "b"},
		"nested": map[string]any{"inner": int64(42)},
	}
	out, err := NormalizeProperties(in)
	if err != nil {
		t.Fatal(err)
	}
	for key := range out {
		if unsafe.StringData(key) != unsafe.StringData(handles[key].Value()) {
			t.Fatalf("key %q not shared", key)
		}
	}
	list := out["list"].([]any)
	if list[0] != "a" || list[1] != "b" {
		t.Fatalf("nested list = %v", list)
	}
	if out["nested"].(map[string]any)["inner"] != int64(42) {
		t.Fatal("nested map mismatch")
	}
	runtime.KeepAlive(handles)
}

func TestInternNormalizationReservesCopiesBeforeAdmission(t *testing.T) {
	denied := errors.New("copy budget denied")
	for _, value := range []any{"key", map[string]any{"key": int64(1)}, map[string]int{"key": 1}} {
		got, err := NormalizeValueWithReserve(value, func(count uint64) error {
			if count == 3 {
				return denied
			}
			return nil
		})
		if !errors.Is(err, denied) || got != nil {
			t.Fatalf("%T: got %v, error %v", value, got, err)
		}
	}
}
