package engine

import (
	"context"
	"testing"
)

func TestQuerySourceScopesReleaseOnlyClosedScope(t *testing.T) {
	db := querySourceFixture(t, 2, 128)
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 1 << 20})
	defer releaseQueryBudget(budget)
	outer := budget.sourceScope()
	if _, err := budget.readNode(tx.graph, 1); err != nil {
		t.Fatal(err)
	}
	outerBytes := budget.sourceBytes
	if outerBytes == 0 {
		t.Fatal("outer source not charged")
	}
	for i := 0; i < 3; i++ {
		inner := budget.sourceScope()
		if _, err := budget.readNode(tx.graph, 2); err != nil {
			t.Fatal(err)
		}
		if budget.sourceBytes <= outerBytes {
			t.Fatal("inner source not charged")
		}
		inner.close()
		if budget.sourceBytes != outerBytes {
			t.Fatalf("inner close released outer source: got %d want %d", budget.sourceBytes, outerBytes)
		}
	}
	outer.close()
	if budget.sourceBytes != 0 {
		t.Fatalf("source bytes after close: %d", budget.sourceBytes)
	}
}
