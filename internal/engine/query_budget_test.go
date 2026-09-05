package engine

import (
	"context"
	"errors"
	"testing"
)

func TestQueryIteratorCloseReleasesUnreadRows(t *testing.T) {
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 512})
	defer releaseQueryBudget(budget)
	if err := budget.chargeRows(3); err != nil {
		t.Fatal(err)
	}
	iterator := &sliceQueryIterator{rows: make([]queryRow, 3), budget: budget}
	if _, ok, err := iterator.Next(); err != nil || !ok {
		t.Fatalf("first row = ok %v, err %v", ok, err)
	}
	iterator.Close()
	if budget.bytes != queryRowBytes {
		t.Fatalf("live bytes after transferred row = %d, want %d", budget.bytes, queryRowBytes)
	}
	budget.releaseRows(1)
	if budget.bytes != 0 {
		t.Fatalf("live bytes after release = %d", budget.bytes)
	}
}

func TestQueryBudgetTransferChecksOverlap(t *testing.T) {
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: queryRowBytes})
	defer releaseQueryBudget(budget)
	if err := budget.chargeRows(1); err != nil {
		t.Fatal(err)
	}
	if err := budget.transferRows(1, 1); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("overlapping transfer error = %v", err)
	}
	if budget.bytes != queryRowBytes {
		t.Fatalf("failed transfer changed ownership: %d", budget.bytes)
	}
	budget.releaseRows(1)
}

func TestQueryCountResultAccountsForLiveRows(t *testing.T) {
	db, err := Open(t.TempDir()+"/count-budget.ltdb", OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := `UNWIND $values AS value RETURN count(*) AS count`
	params := map[string]any{"values": []any{}}
	if _, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxBytes: queryRowBytes - 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("count below query boundary = %v", err)
	}
	result, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxBytes: queryRowBytes})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("count at query boundary = %#v, %v", result.Rows, err)
	}
}

func TestCountRenderAccountsForLiveRowsAtBoundary(t *testing.T) {
	clause := &returnClause{CountVar: "*", CountAlias: "count"}
	row := queryRow{}
	for _, maxBytes := range []uint64{queryRowBytes + 64 + 32 - 1, queryRowBytes + 64 + 32} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: maxBytes})
		if err := budget.chargeRows(1); err != nil {
			t.Fatal(err)
		}
		_, err := clause.render([]queryRow{row}, budget)
		budget.releaseRows(1)
		if maxBytes == queryRowBytes+64+32-1 {
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("count below row/result boundary = %v", err)
			}
		} else if err != nil {
			t.Fatalf("count at row/result boundary = %v", err)
		}
		releaseQueryBudget(budget)
	}
}
