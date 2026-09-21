package engine

import (
	"reflect"
	"testing"
)

// aggregateInput is one folded input: a value plus whether the row had a value
// for the aggregate argument at all.
type aggregateInput struct {
	value   any
	present bool
}

func aggregateValue(value any) aggregateInput { return aggregateInput{value: value, present: true} }

// aggregateNull marks a row whose argument produced no value, which is how
// count(expr) and min/max skip NULL.
func aggregateNull() aggregateInput { return aggregateInput{} }

func aggregateFold(t *testing.T, kind aggregateKind, inputs ...aggregateInput) any {
	t.Helper()
	acc := newAggregateAccumulator(kind)
	for index, input := range inputs {
		if err := acc.add(input.value, input.present); err != nil {
			t.Fatalf("%s add input %d: %v", kind, index, err)
		}
	}
	return acc.result()
}

func TestAggregateKindFor(t *testing.T) {
	cases := []struct {
		name string
		kind aggregateKind
		ok   bool
	}{
		{"count", aggregateCount, true},
		{"COUNT", aggregateCount, true},
		{"Sum", aggregateSum, true},
		{"AVG", aggregateAvg, true},
		{"min", aggregateMin, true},
		{"Max", aggregateMax, true},
		{"collect", aggregateCollect, true},
		{"id", "", false},
		{"counts", "", false},
		{"", "", false},
	}
	for _, testCase := range cases {
		kind, ok := aggregateKindFor(testCase.name)
		if ok != testCase.ok || kind != testCase.kind {
			t.Errorf("aggregateKindFor(%q) = (%q, %v), want (%q, %v)", testCase.name, kind, ok, testCase.kind, testCase.ok)
		}
	}
}

func TestAggregateCount(t *testing.T) {
	// count(expr) counts only rows that produced a value.
	if got := aggregateFold(t, aggregateCount,
		aggregateValue(int64(1)), aggregateValue(int64(2)), aggregateNull(), aggregateValue(int64(3)),
	); got != int64(3) {
		t.Errorf("count(expr) = %v (%T), want int64(3)", got, got)
	}

	// count(*) counts every row: the parent passes present=true for each one,
	// even when the row carries no argument value.
	if got := aggregateFold(t, aggregateCount,
		aggregateValue(nil), aggregateValue(nil), aggregateValue("x"),
	); got != int64(3) {
		t.Errorf("count(*) = %v (%T), want int64(3)", got, got)
	}

	// Empty input and NULL-only input both count zero.
	if got := aggregateFold(t, aggregateCount); got != int64(0) {
		t.Errorf("count of no rows = %v (%T), want int64(0)", got, got)
	}
	if got := aggregateFold(t, aggregateCount, aggregateNull(), aggregateNull()); got != int64(0) {
		t.Errorf("count of NULL rows = %v (%T), want int64(0)", got, got)
	}

	// Values of any type count; the reference only checks NULL.
	if got := aggregateFold(t, aggregateCount,
		aggregateValue([]any{int64(1)}), aggregateValue(map[string]any{"a": int64(1)}),
		aggregateValue([]byte("b")), aggregateValue(true), aggregateValue(1.5),
	); got != int64(5) {
		t.Errorf("count of mixed types = %v (%T), want int64(5)", got, got)
	}
}

func TestAggregateSum(t *testing.T) {
	// Integers and floats both fold into a float result.
	got := aggregateFold(t, aggregateSum, aggregateValue(int64(10)), aggregateValue(20), aggregateValue(5.5))
	if sum, ok := got.(float64); !ok || sum != 35.5 {
		t.Errorf("sum = %v (%T), want float64(35.5)", got, got)
	}

	// Integer-only input still returns a float, like upstream.
	got = aggregateFold(t, aggregateSum, aggregateValue(int64(10)), aggregateValue(int64(20)))
	if sum, ok := got.(float64); !ok || sum != 30 {
		t.Errorf("sum of integers = %v (%T), want float64(30)", got, got)
	}

	// Empty input and NULL-only input sum to float zero, not NULL.
	if got := aggregateFold(t, aggregateSum); got != float64(0) {
		t.Errorf("sum of no rows = %v (%T), want float64(0)", got, got)
	}
	if got := aggregateFold(t, aggregateSum, aggregateNull(), aggregateNull()); got != float64(0) {
		t.Errorf("sum of NULL rows = %v (%T), want float64(0)", got, got)
	}

	// Non-numeric values are skipped rather than reported as errors.
	got = aggregateFold(t, aggregateSum,
		aggregateValue(7.0), aggregateValue("abc"), aggregateValue(true),
		aggregateValue([]any{int64(1)}), aggregateValue([]byte("b")), aggregateValue(nil),
	)
	if got != float64(7) {
		t.Errorf("sum with non-numeric inputs = %v (%T), want float64(7)", got, got)
	}
}

func TestAggregateAvg(t *testing.T) {
	got := aggregateFold(t, aggregateAvg, aggregateValue(int64(10)), aggregateValue(int64(20)), aggregateValue(int64(30)))
	if avg, ok := got.(float64); !ok || avg != 20 {
		t.Errorf("avg = %v (%T), want float64(20)", got, got)
	}

	// Mixed integer and float inputs average as a float.
	got = aggregateFold(t, aggregateAvg, aggregateValue(int64(1)), aggregateValue(2.0))
	if avg, ok := got.(float64); !ok || avg != 1.5 {
		t.Errorf("avg of mixed numerics = %v (%T), want float64(1.5)", got, got)
	}

	// No rows at all is NULL.
	if got := aggregateFold(t, aggregateAvg); got != nil {
		t.Errorf("avg of no rows = %v (%T), want nil", got, got)
	}

	// Upstream parity: the divisor counts every input, so NULL-only input
	// averages to float zero instead of NULL, and a skipped non-numeric value
	// still widens the divisor.
	if got := aggregateFold(t, aggregateAvg, aggregateNull(), aggregateNull()); got != float64(0) {
		t.Errorf("avg of NULL rows = %v (%T), want float64(0)", got, got)
	}
	got = aggregateFold(t, aggregateAvg, aggregateValue(int64(10)), aggregateValue("x"))
	if avg, ok := got.(float64); !ok || avg != 5 {
		t.Errorf("avg with a skipped string = %v (%T), want float64(5)", got, got)
	}
}

func TestAggregateMinMax(t *testing.T) {
	// Integers and floats compare numerically, and the winning value keeps its
	// own type.
	if got := aggregateFold(t, aggregateMin,
		aggregateValue(int64(3)), aggregateValue(1.5), aggregateValue(int64(2)),
	); got != 1.5 {
		t.Errorf("min of mixed numerics = %v (%T), want float64(1.5)", got, got)
	}
	if got := aggregateFold(t, aggregateMax,
		aggregateValue(int64(3)), aggregateValue(1.5), aggregateValue(int64(2)),
	); got != int64(3) {
		t.Errorf("max of mixed numerics = %v (%T), want int64(3)", got, got)
	}
	if got := aggregateFold(t, aggregateMin, aggregateValue(int64(30)), aggregateValue(int64(10)), aggregateValue(int64(20))); got != int64(10) {
		t.Errorf("min of integers = %v (%T), want int64(10)", got, got)
	}
	if got := aggregateFold(t, aggregateMax, aggregateValue(int64(30)), aggregateValue(int64(10)), aggregateValue(int64(20))); got != int64(30) {
		t.Errorf("max of integers = %v (%T), want int64(30)", got, got)
	}

	// Strings order lexicographically.
	if got := aggregateFold(t, aggregateMin, aggregateValue("b"), aggregateValue("a"), aggregateValue("c")); got != "a" {
		t.Errorf("min of strings = %v (%T), want %q", got, got, "a")
	}
	if got := aggregateFold(t, aggregateMax, aggregateValue("b"), aggregateValue("a"), aggregateValue("c")); got != "c" {
		t.Errorf("max of strings = %v (%T), want %q", got, got, "c")
	}

	// Empty input and NULL-only input are NULL.
	if got := aggregateFold(t, aggregateMin); got != nil {
		t.Errorf("min of no rows = %v (%T), want nil", got, got)
	}
	if got := aggregateFold(t, aggregateMax); got != nil {
		t.Errorf("max of no rows = %v (%T), want nil", got, got)
	}
	if got := aggregateFold(t, aggregateMin, aggregateNull(), aggregateNull()); got != nil {
		t.Errorf("min of NULL rows = %v (%T), want nil", got, got)
	}
	if got := aggregateFold(t, aggregateMax, aggregateNull(), aggregateNull()); got != nil {
		t.Errorf("max of NULL rows = %v (%T), want nil", got, got)
	}

	// Unordered type pairs keep the first value seen instead of erroring.
	if got := aggregateFold(t, aggregateMin, aggregateValue("x"), aggregateValue(int64(5))); got != "x" {
		t.Errorf("min of incomparable types = %v (%T), want %q", got, got, "x")
	}
	if got := aggregateFold(t, aggregateMax, aggregateValue(int64(5)), aggregateValue("x")); got != int64(5) {
		t.Errorf("max of incomparable types = %v (%T), want int64(5)", got, got)
	}
	if got := aggregateFold(t, aggregateMin, aggregateValue(true), aggregateValue(false)); got != true {
		t.Errorf("min of booleans = %v (%T), want true", got, got)
	}

	// Bytes order lexicographically.
	if got := aggregateFold(t, aggregateMin, aggregateValue([]byte("b")), aggregateValue([]byte("a"))); !reflect.DeepEqual(got, []byte("a")) {
		t.Errorf("min of bytes = %v (%T), want %q", got, got, "a")
	}
}

func TestAggregateCollect(t *testing.T) {
	// Input order is preserved.
	got := aggregateFold(t, aggregateCollect, aggregateValue(int64(3)), aggregateValue(int64(1)), aggregateValue(int64(2)))
	if want := []any{int64(3), int64(1), int64(2)}; !reflect.DeepEqual(got, want) {
		t.Errorf("collect = %v, want %v", got, want)
	}

	// NULL and missing values are kept, in order, and mixed types stay as they
	// were evaluated.
	got = aggregateFold(t, aggregateCollect,
		aggregateValue(int64(1)), aggregateNull(), aggregateValue("x"), aggregateValue(nil), aggregateValue(true),
	)
	if want := []any{int64(1), nil, "x", nil, true}; !reflect.DeepEqual(got, want) {
		t.Errorf("collect with NULLs = %v, want %v", got, want)
	}

	// Empty input collects an empty list, not NULL.
	got = aggregateFold(t, aggregateCollect)
	list, ok := got.([]any)
	if !ok || list == nil || len(list) != 0 {
		t.Errorf("collect of no rows = %v (%T), want empty []any", got, got)
	}

	// Wrong-type inputs are appended rather than reported as errors.
	got = aggregateFold(t, aggregateCollect, aggregateValue([]any{int64(1)}), aggregateValue(map[string]any{"a": int64(1)}), aggregateValue([]byte("b")))
	if want := []any{[]any{int64(1)}, map[string]any{"a": int64(1)}, []byte("b")}; !reflect.DeepEqual(got, want) {
		t.Errorf("collect of non-scalars = %v, want %v", got, want)
	}
}

func TestAggregateUnknownKind(t *testing.T) {
	acc := newAggregateAccumulator(aggregateKind("median"))
	err := acc.add(int64(1), true)
	if err == nil {
		t.Fatal("add on unknown kind: want error, got nil")
	}
	if want := "unknown aggregate \"median\""; err.Error() != want {
		t.Errorf("add on unknown kind error = %q, want %q", err.Error(), want)
	}
}
