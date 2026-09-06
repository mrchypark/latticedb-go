package engine

import (
	"context"
	"errors"
	"testing"
)

func TestNormalizeQueryParamsReservesCopiesBeforeAllocation(t *testing.T) {
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 38})
	defer releaseQueryBudget(budget)
	_, _, err := normalizeQueryParamsWithBudget(map[string]any{"payload": []byte("payload")}, budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("normalization error = %v", err)
	}
	if budget.bytes != 0 {
		t.Fatalf("failed normalization retained %d bytes", budget.bytes)
	}
}

func TestUnwindSharesNormalizedPayloadWithoutAnotherCopy(t *testing.T) {
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 1024})
	defer releaseQueryBudget(budget)
	params, paramBytes, err := normalizeQueryParamsWithBudget(map[string]any{"items": []any{[]byte("payload")}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.releaseTemporary(paramBytes)
	if err := budget.chargeRows(1); err != nil {
		t.Fatal(err)
	}
	clause := unwindClause{Expr: paramExpr{Name: "items"}, Var: "item"}
	rows, err := clause.apply([]queryRow{{slots: make([]boundValue, 1), index: map[string]int{"item": 0}}}, params, budget)
	if err != nil || len(rows) != 1 {
		t.Fatalf("UNWIND rows=%d err=%v", len(rows), err)
	}
	item, ok := rows[0].get("item")
	if !ok || &item.Value.([]byte)[0] != &params["items"].([]any)[0].([]byte)[0] {
		t.Fatal("UNWIND copied the normalized payload")
	}
	if uint64(budget.bytes) != paramBytes+queryRowBytes {
		t.Fatalf("UNWIND bytes = %d, want parameter plus row bytes %d", budget.bytes, paramBytes+queryRowBytes)
	}
	budget.releaseRows(1)
}

func TestQueryValueComparisonsConsumeWorkPerNestedValue(t *testing.T) {
	row := queryRow{slots: []boundValue{{Value: map[string]any{"value": map[string]any{"scores": []any{int64(1), int64(2)}}}, HasValue: true, Bound: true}}, index: map[string]int{"n": 0}}
	clause := whereClause{Kind: whereEquals, Var: "n", Property: "value", Expr: literalExpr{Value: map[string]any{"scores": []any{int64(1), int64(2)}}}}
	for _, maxWork := range []uint64{4, 5} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: maxWork})
		_, err := clause.apply(nil, []queryRow{row}, nil, budget)
		if maxWork == 4 && !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("below nested-equality boundary = %v", err)
		}
		if maxWork == 5 && err != nil {
			t.Fatalf("at nested-equality boundary = %v", err)
		}
		releaseQueryBudget(budget)
	}
}

func TestQueryInComparisonConsumesWorkPerCandidate(t *testing.T) {
	row := queryRow{slots: []boundValue{{Value: map[string]any{"value": int64(3)}, HasValue: true, Bound: true}}, index: map[string]int{"n": 0}}
	clause := whereClause{Kind: whereIn, Var: "n", Property: "value", Expr: literalExpr{Value: []any{int64(1), int64(2), int64(3)}}}
	for _, maxWork := range []uint64{3, 4} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: maxWork})
		rows, err := clause.apply(nil, []queryRow{row}, nil, budget)
		if maxWork == 3 && !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("below IN boundary = %v", err)
		}
		if maxWork == 4 && (err != nil || len(rows) != 1) {
			t.Fatalf("at IN boundary rows=%d err=%v", len(rows), err)
		}
		releaseQueryBudget(budget)
	}
}

func TestQueryPatternPropertiesConsumeNestedComparisonWork(t *testing.T) {
	properties := map[string]any{"value": map[string]any{"items": []any{int64(1)}}}
	required := map[string]any{"value": map[string]any{"items": []any{int64(1)}}}
	for _, maxWork := range []uint64{2, 3} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: maxWork})
		matched, err := queryPropertiesMatchWithBudget(properties, required, budget)
		if maxWork == 2 && !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("below pattern-property boundary = %v", err)
		}
		if maxWork == 3 && (err != nil || !matched) {
			t.Fatalf("at pattern-property boundary matched=%v err=%v", matched, err)
		}
		releaseQueryBudget(budget)
	}
}

func TestDistinctCollisionConsumesNestedComparisonWork(t *testing.T) {
	row := map[string]any{"value": []any{int64(1), int64(2)}}
	for _, maxWork := range []uint64{2, 3} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: maxWork})
		matched, err := resultRowsEqualWithBudget([]string{"value"}, row, row, budget)
		if maxWork == 2 && !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("below DISTINCT boundary = %v", err)
		}
		if maxWork == 3 && (err != nil || !matched) {
			t.Fatalf("at DISTINCT boundary matched=%v err=%v", matched, err)
		}
		releaseQueryBudget(budget)
	}
}
