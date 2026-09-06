package engine

import (
	"context"
	"errors"
	"testing"
)

func TestDistinctKeepsNestedNullEquality(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, value := range []any{[]any{nil}, map[string]any{"nested": []any{nil}}} {
		result, err := db.Query("UNWIND $values AS value RETURN DISTINCT value", map[string]any{"values": []any{value, value}})
		if err != nil || len(result.Rows) != 1 {
			t.Fatalf("DISTINCT %#v: rows=%v err=%v", value, result.Rows, err)
		}
	}
}

func TestBudgetedEqualityPreservesNilSliceSemantics(t *testing.T) {
	for _, pair := range [][2]any{
		{[]byte(nil), []byte{}}, {[]float32(nil), []float32{}},
		{[]byte(nil), []byte(nil)}, {[]float32{}, []float32{}},
	} {
		budget := newQueryBudget(context.Background(), QueryOptions{})
		got, err := queryValuesEqualWithBudget(pair[0], pair[1], budget)
		releaseQueryBudget(budget)
		if err != nil || got != queryValuesEqual(pair[0], pair[1]) {
			t.Fatalf("%#v: equal=%v err=%v", pair, got, err)
		}
	}
}

func TestIntersectionWorkChecksCannotSkipCursorBoundaries(t *testing.T) {
	left, right := []uint64{0}, []uint64{}
	for id := uint64(1); id <= 130; id++ {
		left = append(left, id)
		right = append(right, id)
	}
	budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: 1})
	defer releaseQueryBudget(budget)
	if _, err := intersectSortedIDs(left, right, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("intersection error=%v", err)
	}
}

func TestVectorEqualityChecksCancellationInsideComparison(t *testing.T) {
	ctx := &cancelAfterQueryChecks{remaining: 2, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)
	if _, err := queryValuesEqualWithBudget(make([]float32, 256), make([]float32, 256), budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("comparison error=%v", err)
	}
}
