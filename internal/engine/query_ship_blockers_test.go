package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWithShipBlockerParsing(t *testing.T) {
	for _, query := range []string{
		"MATCH (a), (b) WITH a AS b, `b` RETURN b",
		"MATCH (n) WITH `n`, n.age AS n RETURN n",
		"MATCH (n) WITH count(*) RETURN `count(*)`",
	} {
		if _, err := parseQuery(query); err == nil {
			t.Fatalf("accepted %s", query)
		}
	}
	db := openWithDB(t)
	for _, test := range []struct {
		query string
		want  int64
	}{
		{"MATCH (n:Person) WITH count(*) AS c RETURN c", 3},
		{"MATCH (n:Person) WITH count(*) RETURN count(*) AS c", 1},
	} {
		result, err := db.Query(test.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rows[0]["c"] != test.want {
			t.Fatalf("%s: %v", test.query, result.Rows)
		}
	}
}

func TestWithCreateCardinality(t *testing.T) {
	db := openWithDB(t)
	for _, test := range []struct {
		query string
		want  int64
	}{
		{"MATCH (n:Person) WITH n.name AS name LIMIT 2 CREATE (m:Copy {name: name}) RETURN count(*) AS c", 2},
		{"MATCH (n:Person) WITH n.name AS name LIMIT 0 CREATE (m:Copy {name: name}) RETURN count(*) AS c", 0},
		{"MATCH (n:Missing) WITH n.name AS name CREATE (m:Copy {name: name}) RETURN count(*) AS c", 0},
	} {
		result, err := db.Query(test.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Rows[0]["c"] != test.want {
			t.Fatalf("%s: %v", test.query, result.Rows)
		}
	}
	result, err := db.Query("MATCH (n:Copy) RETURN count(*) AS c", nil)
	if err != nil || result.Rows[0]["c"] != int64(2) {
		t.Fatalf("created extra nodes: %v, %v", result, err)
	}
}

func TestDistinctKeyBudgetBeforeAllocation(t *testing.T) {
	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 4096})
	defer releaseQueryBudget(budget)
	var builder strings.Builder
	err := writeDistinctValueKey(&builder, strings.Repeat("x", 1<<20), budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("error = %v", err)
	}
	if builder.Cap() != 0 {
		t.Fatalf("allocated %d bytes before rejection", builder.Cap())
	}
}

func TestWithComputedAggregateBudget(t *testing.T) {
	db := openWithDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Big"}, Properties: map[string]any{"blob": make([]byte, 1<<20)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"MATCH (n:Big) WITH collect(coalesce(n.blob)) AS xs RETURN count(*) AS c",
		"MATCH (n:Big) WITH properties(n) AS p RETURN count(*) AS c",
		"WITH range(1, 1000000000) AS xs RETURN count(*) AS c",
		"MATCH (n:Person) WITH n WHERE n.age IN range(1, 1000000000) RETURN count(*) AS c",
		"UNWIND range(1, 1000000000) AS x RETURN count(*) AS c",
		"WITH coalesce({p: 0}) AS n WHERE n.p IN range(0, 1000000) RETURN count(*) AS c",
		"WITH coalesce(1) AS n UNWIND range(1, 1000000000) AS x RETURN count(*) AS c",
		"WITH coalesce(1) AS n CREATE (m:BudgetCopy {xs: range(1, 1000000000)}) RETURN count(*) AS c",
		"MATCH (n:Person) SET n.xs = range(1, 1000000000) RETURN n",
	} {
		_, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxBytes: 4096})
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("%s: %v", query, err)
		}
	}
	result, err := db.Query("MATCH (n:BudgetCopy) RETURN count(*) AS c", nil)
	if err != nil || result.Rows[0]["c"] != int64(0) {
		t.Fatalf("unexpected mutation: %v, %v", result, err)
	}
}

// Measure bytes, not allocation count: one prohibited blob copy is one allocation
// regardless of whether the blob is 1 KiB or 1 MiB.
func TestWithPayloadRejectedBeforeClone(t *testing.T) {
	db := openWithDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Big"}, Properties: map[string]any{"blob": make([]byte, 1<<20)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"MATCH (n:Big) WITH n, count(*) AS c RETURN count(*) AS groups",
		"MATCH (n:Big) WITH DISTINCT n RETURN count(*) AS groups",
		"MATCH (n:Big) WITH collect(coalesce(n.blob)) AS blobs RETURN count(*) AS groups",
	} {
		_, _ = db.QueryContext(t.Context(), query, nil, QueryOptions{MaxBytes: 4096})
		result := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := db.QueryContext(b.Context(), query, nil, QueryOptions{MaxBytes: 4096})
				if !errors.Is(err, ErrResourceLimit) {
					b.Fatalf("error = %v", err)
				}
			}
		})
		if got := result.AllocedBytesPerOp(); got > 64<<10 {
			t.Fatalf("%s allocated %d bytes before rejection", query, got)
		}
	}
}

func TestWithExpressionBudgetControls(t *testing.T) {
	db := openWithDB(t)
	query := "WITH coalesce({p: 0}) AS n WHERE n.p IN range(0, 3) RETURN count(*) AS c"
	result, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxBytes: 4096})
	if err != nil || result.Rows[0]["c"] != int64(1) {
		t.Fatalf("bounded expression: %v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := db.QueryContext(ctx, query, nil, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
