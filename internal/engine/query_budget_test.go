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
