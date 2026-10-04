package engine

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestWithDistinctReleasesWorkspace(t *testing.T) {
	clause, err := parseReturnClause("DISTINCT n")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		opts QueryOptions
		ctx  context.Context
		want error
	}{
		{"duplicates", QueryOptions{MaxBytes: 256 << 10}, t.Context(), nil},
		{"byte limit", QueryOptions{MaxBytes: 128}, t.Context(), ErrResourceLimit},
		{"work limit", QueryOptions{MaxWork: 10}, t.Context(), ErrResourceLimit},
		{"cancellation", QueryOptions{}, &cancelAfterQueryChecks{remaining: 2, done: make(chan struct{})}, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := newQueryBudget(tc.ctx, tc.opts)
			defer releaseQueryBudget(budget)
			if err := budget.chargeResult(64); err != nil {
				t.Fatal(err)
			}
			row := callExprRow()
			row.slots[0].Node.Labels = []string{strings.Repeat("x", 64<<10)}
			rows := make([]queryRow, 20)
			for i := range rows {
				rows[i] = row
			}
			got, err := clause.distinctWithRows(rows, budget)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if tc.want == nil && len(got) != 1 {
				t.Fatalf("rows = %d", len(got))
			}
			if budget.bytes != 64 {
				t.Fatalf("retained bytes = %d, want 64", budget.bytes)
			}
		})
	}
}

func TestAbsIntegerOverflowAndMutationAtomicity(t *testing.T) {
	db := openWithDB(t)
	minValue := int64(math.MinInt64)
	values := []any{minValue}
	if strconv.IntSize == 64 {
		values = append(values, int(minValue))
	}
	for _, value := range values {
		_, err := fnCall("abs", fnLit(value)).eval(queryRow{}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "integer arithmetic overflow") {
			t.Fatalf("abs(%v) error = %v", value, err)
		}
	}
	for _, tc := range []struct {
		value any
		want  any
	}{
		{int64(math.MinInt64 + 1), int64(math.MaxInt64)},
		{int(-7), int64(7)}, {int64(0), int64(0)}, {float64(-1.5), float64(1.5)},
	} {
		got, err := fnCall("abs", fnLit(tc.value)).eval(queryRow{}, nil, nil)
		if err != nil || got != tc.want {
			t.Fatalf("abs(%v) = %v, %v", tc.value, got, err)
		}
	}
	for _, query := range []string{
		"RETURN abs($min)",
		"CREATE (:Overflow {value: abs($min)})",
		"MATCH (n:Person) SET n.age = 99, n.name = abs($min)",
	} {
		if _, err := db.Query(query, map[string]any{"min": int64(math.MinInt64)}); err == nil || !strings.Contains(err.Error(), "overflow") {
			t.Fatalf("%s: %v", query, err)
		}
	}
	result, err := db.Query("MATCH (n:Overflow) RETURN count(*) AS n", nil)
	if err != nil || result.Rows[0]["n"] != int64(0) {
		t.Fatalf("failed CREATE retained nodes: %v, %v", result, err)
	}
	result, err = db.Query("MATCH (n:Person) WHERE n.age = 99 RETURN count(*) AS n", nil)
	if err != nil || result.Rows[0]["n"] != int64(0) {
		t.Fatalf("failed SET retained changes: %v, %v", result, err)
	}
}

func TestWithDistinctUniqueKeyWorkspace(t *testing.T) {
	clause, err := parseReturnClause("DISTINCT v")
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []uint64{1024, 64 << 10} {
		budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: limit})
		budget.sourceBytes = 32
		if err := budget.chargeResult(64); err != nil {
			t.Fatal(err)
		}
		rows := make([]queryRow, 50)
		for i := range rows {
			rows[i] = queryRow{index: map[string]int{"v": 0}, slots: []boundValue{{Value: strings.Repeat("x", 100) + strconv.Itoa(i), HasValue: true, Bound: true}}}
		}
		got, err := clause.distinctWithRows(rows, budget)
		if limit == 1024 {
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("unique keys must exceed small budget: %v", err)
			}
		} else if err != nil || len(got) != len(rows) {
			t.Fatalf("unique rows = %d, error = %v", len(got), err)
		}
		if budget.bytes != 64 || budget.sourceBytes != 32 {
			t.Fatalf("caller ledger changed: bytes=%d, sourceBytes=%d", budget.bytes, budget.sourceBytes)
		}
		releaseQueryBudget(budget)
	}
}

func TestQueryMapEntityFieldProjection(t *testing.T) {
	db := openWithDB(t)
	for _, tail := range []string{
		"RETURN m.node AS v LIMIT 1",
		"RETURN DISTINCT m.node AS v LIMIT 1",
		"RETURN m.node AS v, count(*) AS total LIMIT 1",
	} {
		result, err := db.Query("MATCH (n:Person) WITH {node:n} AS m "+tail, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := result.Rows[0]["v"].(Node); !ok {
			t.Fatalf("%s: node = %T", tail, result.Rows[0]["v"])
		}
	}
	if _, err := db.Query("CREATE (:MapSource {name:'source'})-[:MapLink {weight:7}]->(:MapTarget)", nil); err != nil {
		t.Fatal(err)
	}
	for _, tail := range []string{
		"RETURN box.edge AS v, box.nested AS nested",
		"RETURN DISTINCT box.edge AS v, box.nested AS nested",
		"RETURN box.edge AS v, box.nested AS nested, count(*) AS total",
	} {
		result, err := db.Query("MATCH (n:MapSource)-[r:MapLink]->(m) WITH {edge:r, nested:{items:[n,r]}} AS box "+tail, nil)
		if err != nil {
			t.Fatal(err)
		}
		edge, ok := result.Rows[0]["v"].(Edge)
		if !ok {
			t.Fatalf("%s: edge = %T", tail, result.Rows[0]["v"])
		}
		items := result.Rows[0]["nested"].(map[string]any)["items"].([]any)
		node, ok := items[0].(Node)
		if !ok {
			t.Fatalf("nested node = %T", items[0])
		}
		if _, ok := items[1].(Edge); !ok {
			t.Fatalf("nested edge = %T", items[1])
		}
		node.Properties["name"] = "changed"
		edge.Properties["weight"] = int64(99)
		fresh, err := db.Query("MATCH (n:MapSource)-[r:MapLink]->(m) RETURN n.name AS name, r.weight AS weight", nil)
		if err != nil || fresh.Rows[0]["name"] != "source" || fresh.Rows[0]["weight"] != int64(7) {
			t.Fatalf("public result changed storage: %#v, %v", fresh.Rows, err)
		}
	}
}

func TestQueryMapsRetainEntityBindings(t *testing.T) {
	db := openWithDB(t)
	for _, query := range []string{
		"MATCH (n:Person) RETURN {node:n, nested:{items:[n]}} AS v LIMIT 1",
		"MATCH (n:Person) WITH {node:n, nested:{items:[n]}} AS m RETURN m AS v LIMIT 1",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := result.Rows[0]["v"].(map[string]any)
		if _, ok := m["node"].(Node); !ok {
			t.Fatalf("node = %T", m["node"])
		}
		items := m["nested"].(map[string]any)["items"].([]any)
		if !reflect.DeepEqual(m["node"], items[0]) {
			t.Fatalf("nested node = %#v", items[0])
		}
	}
	row := callExprRow()
	expr, err := parseValueExpr("{node:n, edge:r, items:[n,r]}")
	if err != nil {
		t.Fatal(err)
	}
	value, err := expr.eval(row, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := publicProjectionValue(value).(map[string]any)
	if _, ok := m["edge"].(Edge); !ok {
		t.Fatalf("edge = %T", m["edge"])
	}
	if _, err := db.Query("MATCH (n:Person) SET n.invalid = {node:n}", nil); err == nil {
		t.Fatal("entity accepted as persisted property")
	}
}

func TestProjectionKeywordLiterals(t *testing.T) {
	db := openWithDB(t)
	for _, tc := range []struct {
		query string
		want  any
	}{
		{"RETURN true AS v", true},
		{"RETURN false AS v", false},
		{"RETURN null AS v", nil},
		{"WITH true AS v RETURN v", true},
		{"WITH false AS v RETURN v", false},
		{"WITH null AS v RETURN v", nil},
		{"RETURN count(null) AS v", int64(0)},
		{"RETURN count(true) AS v", int64(1)},
		{"RETURN count(false) AS v", int64(1)},
		{"RETURN count(null) AS v, count(*) AS rows", int64(0)},
		{"WITH 7 AS `null` RETURN `null` AS v", int64(7)},
		{"WITH 7 AS `true` RETURN count(`true`) AS v", int64(1)},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := db.Query(tc.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || !reflect.DeepEqual(result.Rows[0]["v"], tc.want) {
				t.Fatalf("rows = %#v, want v=%#v", result.Rows, tc.want)
			}
		})
	}
}

func TestWithDistinctDuplicateEntityFitsLiveByteBudget(t *testing.T) {
	db := openWithDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}, Properties: map[string]any{"text": strings.Repeat("x", 64<<10)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.QueryContext(t.Context(), "UNWIND range(1,20) AS i MATCH (n:Doc) WITH DISTINCT n RETURN id(n) AS id", nil, QueryOptions{MaxBytes: 1 << 20})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("result = %v, error = %v", result, err)
	}
}
