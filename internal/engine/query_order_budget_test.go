package engine

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestBudgetedOrderComparisonStopsLargeVector(t *testing.T) {
	left, right := make([]float32, queryOrderComparisonChunk), make([]float32, queryOrderComparisonChunk)
	right[len(right)-1] = 1
	budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 64})
	defer releaseQueryBudget(budget)

	if _, err := compareOrderValuesWithBudget(left, right, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("comparison error = %v, want resource limit", err)
	}
}

func TestBudgetedOrderComparisonChecksCancellation(t *testing.T) {
	left, right := make([]float32, queryOrderComparisonChunk), make([]float32, queryOrderComparisonChunk)
	right[len(right)-1] = 1
	ctx := &cancelAfterQueryChecks{remaining: 1, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)

	if _, err := compareOrderValuesWithBudget(left, right, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("comparison error = %v, want cancellation", err)
	}
}

func TestBudgetedOrderComparisonChargesMapKeyBuffers(t *testing.T) {
	left, right := make(map[string]any, 2), make(map[string]any, 2)
	left["a"], left["b"] = int64(1), int64(2)
	right["a"], right["b"] = int64(1), int64(2)
	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 32})
	defer releaseQueryBudget(budget)

	if _, err := compareOrderValuesWithBudget(left, right, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("comparison error = %v, want resource limit", err)
	}
}

func TestBudgetedOrderComparisonPreservesVectorNaNOrder(t *testing.T) {
	left, right := []float32{float32(math.NaN()), 1}, []float32{0, 1}
	budget := newQueryBudget(t.Context(), QueryOptions{})
	defer releaseQueryBudget(budget)

	got, err := compareOrderValuesWithBudget(left, right, budget)
	if err != nil {
		t.Fatal(err)
	}
	if want := compareOrderValues(left, right); got != want {
		t.Fatalf("comparison = %d, want %d", got, want)
	}
}

func TestBudgetedOrderTieBreakChargesBindingScans(t *testing.T) {
	left := queryRow{slots: make([]boundValue, 65)}
	right := queryRow{slots: make([]boundValue, 65)}
	budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 3})
	defer releaseQueryBudget(budget)

	if _, err := (&queryPlan{}).compareOrderedRowsWithBudget(left, right, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("comparison error = %v, want resource limit", err)
	}
}

func TestBudgetedOrderTieBreakChecksCancellation(t *testing.T) {
	left := queryRow{slots: make([]boundValue, 1)}
	right := queryRow{slots: make([]boundValue, 1)}
	ctx := &cancelAfterQueryChecks{remaining: -1, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)

	if _, err := (&queryPlan{}).compareOrderedRowsWithBudget(left, right, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("comparison error = %v, want cancellation", err)
	}
}

func TestQueryOrderingPathsLimitLargeVectorComparisons(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	left, right := make([]float32, 1<<20), make([]float32, 1<<20)
	right[len(right)-1] = 1
	params := map[string]any{"values": []any{left, right}}
	for _, query := range []string{
		`UNWIND $values AS v RETURN v ORDER BY v`,
		`UNWIND $values AS v RETURN v ORDER BY v LIMIT 1`,
		`UNWIND $values AS v WITH v ORDER BY v RETURN v`,
		`UNWIND $values AS v RETURN v, count(*) AS n ORDER BY v`,
	} {
		t.Run(query, func(t *testing.T) {
			_, err := db.QueryContext(t.Context(), query, params, QueryOptions{MaxWork: 33000, MaxBytes: 64 << 20})
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("query error = %v, want resource limit", err)
			}
		})
	}
}
