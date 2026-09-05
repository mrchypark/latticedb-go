package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
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

func TestQueryClauseScratchReleasesBetweenRows(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"text": "alpha beta", "embedding": []float32{1, 0}}})
	tx := &Tx{graph: graph}
	row := queryRow{slots: []boundValue{{Node: graph.Nodes.Get(1)}}, bound: []bool{true}, index: map[string]int{"n": 0}}
	cases := []struct {
		name   string
		clause whereClause
		params map[string]any
	}{
		{"fts", whereClause{Kind: whereFTS, Var: "n", Property: "text", Expr: literalExpr{Value: "alpha"}}, nil},
		{"vector", whereClause{Kind: whereVector, Var: "n", Property: "embedding", Expr: literalExpr{Value: []float32{1, 0}}}, nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			budget := newQueryBudget(context.Background(), QueryOptions{})
			defer releaseQueryBudget(budget)
			if err := budget.chargeRows(1); err != nil {
				t.Fatal(err)
			}
			rows, err := test.clause.apply(tx, []queryRow{row}, test.params, budget)
			if err != nil || len(rows) != 1 {
				t.Fatalf("apply rows=%d err=%v", len(rows), err)
			}
			if budget.bytes != queryRowBytes {
				t.Fatalf("live bytes after %s = %d, want row token only", test.name, budget.bytes)
			}
			budget.releaseRows(1)
			if budget.bytes != 0 {
				t.Fatalf("live bytes after %s release = %d", test.name, budget.bytes)
			}
		})
	}
}

func TestNodePatternFullScanCandidateScratchIsScoped(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Labels: []string{"Item"}})
	tx := &Tx{graph: graph}
	pattern := nodePattern{Var: "n"}
	row := queryRow{slots: make([]boundValue, 1), bound: make([]bool, 1), index: map[string]int{"n": 0}}
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 2*queryRowBytes + 8})
	defer releaseQueryBudget(budget)
	if err := budget.chargeRows(1); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		rows, err := pattern.apply(tx, []queryRow{row}, budget)
		if err != nil || len(rows) != 1 {
			t.Fatalf("attempt %d rows=%d err=%v", attempt, len(rows), err)
		}
		if budget.bytes != queryRowBytes {
			t.Fatalf("attempt %d candidate scratch retained %d bytes", attempt, budget.bytes)
		}
	}
	budget.releaseRows(1)
}

func TestIndexedCandidateScratchIsScopedAtBoundary(t *testing.T) {
	db, err := Open(t.TempDir()+"/indexed.ltdb", OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query("CREATE (:Item {key: 1})", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	plan, err := parseQuery("MATCH (n:Item) WHERE n.key = 1 RETURN n.key")
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []uint64{2*queryRowBytes + 7, 2*queryRowBytes + 8} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: limit})
		if err := budget.chargeRows(1); err != nil {
			t.Fatal(err)
		}
		pattern := nodePattern{Var: "n", Labels: []string{"Item"}}
		if _, found, err := plan.indexedNodeIDs(tx, pattern, nil, 1, budget); err != nil || !found {
			t.Fatalf("index not used: %v, %v", found, err)
		}
		iterator := &patternQueryIterator{plan: plan, tx: tx, pattern: pattern, budget: budget, limit: 1}
		row := queryRow{slots: make([]boundValue, 1), bound: make([]bool, 1), index: map[string]int{"n": 0}}
		for range 2 {
			rows, err := iterator.apply(row)
			if limit == 2*queryRowBytes+7 {
				if !errors.Is(err, ErrResourceLimit) {
					t.Fatalf("below boundary = %v", err)
				}
			} else if err != nil || len(rows) != 1 {
				t.Fatalf("at boundary rows=%d, %v", len(rows), err)
			}
			if budget.bytes != queryRowBytes {
				t.Fatalf("candidate scratch retained: %d", budget.bytes)
			}
		}
		budget.releaseRows(1)
		releaseQueryBudget(budget)
	}
}
