package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestDistinctAggregates(t *testing.T) {
	db := openAggregateDB(t)
	params := map[string]any{"values": []any{nil, nil, int64(1), float64(1), map[string]any{"a": []any{int64(2)}}, map[string]any{"a": []any{float64(2)}}}}
	result, err := db.Query(`UNWIND $values AS value RETURN count(DISTINCT value) AS count, sum(DISTINCT value) AS sum, avg(DISTINCT value) AS avg, min(DISTINCT value) AS min, max(DISTINCT value) AS max, collect(DISTINCT value) AS values`, params)
	if err != nil {
		t.Fatal(err)
	}
	row := result.Rows[0]
	if row["count"] != int64(2) || row["sum"] != float64(1) || row["avg"] != float64(1.0/3.0) || row["min"] != int64(1) || row["max"] != int64(1) {
		t.Fatalf("DISTINCT aggregate scalar results = %#v", row)
	}
	if got := fmt.Sprint(row["values"]); got != "[<nil> 1 map[a:[2]]]" {
		t.Fatalf("DISTINCT collect = %s", got)
	}

	result, err = db.Query(`UNWIND $values AS value WITH count(DISTINCT value) AS count RETURN count`, params)
	if err != nil || result.Rows[0]["count"] != int64(2) {
		t.Fatalf("WITH DISTINCT aggregate = %#v, %v", result.Rows, err)
	}
	result, err = db.Query(`MATCH (n:Missing) RETURN count(DISTINCT n) AS count, sum(DISTINCT n) AS sum, avg(DISTINCT n) AS avg, min(DISTINCT n) AS min, max(DISTINCT n) AS max, collect(DISTINCT n) AS values`, nil)
	if err != nil {
		t.Fatal(err)
	}
	row = result.Rows[0]
	if row["count"] != int64(0) || row["sum"] != float64(0) || row["avg"] != nil || row["min"] != nil || row["max"] != nil || len(row["values"].([]any)) != 0 {
		t.Fatalf("empty DISTINCT aggregate = %#v", row)
	}
	for _, query := range []string{`UNWIND $values AS value RETURN count(DISTINCT *)`, `UNWIND $values AS value RETURN sum(DISTINCT *)`} {
		if _, err := db.Query(query, params); err == nil {
			t.Fatalf("accepted %q", query)
		}
	}
}

func TestDistinctAggregateBudgetAndMutationRollback(t *testing.T) {
	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 1})
	acc := newAggregateAccumulator(aggregateCount, true)
	if _, err := acc.accept(int64(1), budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("dedup state budget error = %v", err)
	}
	releaseQueryBudget(budget)

	db := openAggregateDB(t)
	_, err := db.QueryContext(t.Context(), `CREATE (n:DistinctMutation) WITH count(DISTINCT range(1, 10000)) AS count RETURN count`, nil, QueryOptions{MaxBytes: 4096})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("mutation query error = %v", err)
	}
	result, err := db.Query(`MATCH (n:DistinctMutation) RETURN count(*) AS count`, nil)
	if err != nil || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("failed DISTINCT aggregate mutation persisted: %#v, %v", result.Rows, err)
	}
}

func TestDistinctAggregateKeysBoundRelationshipLists(t *testing.T) {
	budget := newQueryBudget(t.Context(), QueryOptions{})
	defer releaseQueryBudget(budget)
	acc := newAggregateAccumulator(aggregateCount, true)
	edge := boundValue{Edge: &store.EdgeRecord{ID: 1, SourceID: 2, TargetID: 3, Type: "REL"}}
	for _, value := range []any{[]any{edge}, []any{edge}} {
		accepted, err := acc.accept(value, budget)
		if err != nil {
			t.Fatal(err)
		}
		if accepted {
			if err := acc.add(value, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := acc.result(); got != int64(1) {
		t.Fatalf("count(DISTINCT relationship list) = %v", got)
	}
	acc.releaseDistinct(budget)
}

func TestDistinctAggregateBoundValueReservesBeforeMaterializing(t *testing.T) {
	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 64})
	defer releaseQueryBudget(budget)
	acc := newAggregateAccumulator(aggregateCount, true)
	value := []any{boundValue{Edge: &store.EdgeRecord{ID: 1, Properties: store.PropertiesFromMap(map[string]any{"blob": make([]byte, 1<<20)})}}}
	if _, err := acc.accept(value, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("large bound DISTINCT key error = %v", err)
	}
	if budget.bytes != 0 {
		t.Fatalf("large bound DISTINCT key retained %d bytes", budget.bytes)
	}
}
