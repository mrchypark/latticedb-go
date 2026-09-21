package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestWithReviewRegressions pins the defects reported by the independent review.
func TestWithReviewRegressions(t *testing.T) {
	db := openWithDB(t)
	people := func(params map[string]any) map[string]any {
		if params == nil {
			params = map[string]any{}
		}
		return params
	}
	rejected := []struct {
		name  string
		query string
		param map[string]any
	}{
		{"unknown with item", "MATCH (p:Person) WITH missing AS x RETURN x", nil},
		{"dropped binding reused", "MATCH (p:Person) WITH p AS x WITH p AS y RETURN y", nil},
		{"filter uses dropped binding", "MATCH (p:Person) WITH p AS x WHERE p.age > 1 RETURN x", nil},
		{"scalar used as node", "UNWIND $xs AS x WITH x RETURN id(x) AS i", map[string]any{"xs": []any{int64(1)}}},
	}
	for _, testCase := range rejected {
		t.Run("reject/"+testCase.name, func(t *testing.T) {
			if _, err := db.Query(testCase.query, testCase.param); err == nil {
				t.Fatalf("query %q unexpectedly succeeded", testCase.query)
			}
		})
	}

	accepted := []struct {
		name  string
		query string
		param map[string]any
		want  string
	}{
		{"order by alias", "UNWIND $xs AS xs WITH xs AS y ORDER BY y LIMIT 1 RETURN y AS y", map[string]any{"xs": []any{int64(5), int64(3), int64(9)}}, "[3]"},
		{"order by property alias", "MATCH (p:Person) WITH p.age AS age ORDER BY age DESC LIMIT 1 RETURN age AS age", nil, "[40]"},
		{"null projection fallback", "UNWIND $xs AS xs WITH coalesce(null) AS x RETURN coalesce(x, 7) AS y", map[string]any{"xs": []any{int64(1)}}, "[7]"},
		{"distinct unnamed projection", "UNWIND $rows AS x WITH DISTINCT x.k RETURN count(*) AS c", map[string]any{"rows": []any{map[string]any{"k": int64(1)}, map[string]any{"k": int64(2)}}}, "[2]"},
		{"count skips null", "UNWIND $rows AS x WITH count(x.missing) AS c RETURN c AS c", map[string]any{"rows": []any{map[string]any{"k": int64(1)}}}, "[0]"},
		{"starts with in filter", "MATCH (p:Person) WITH p WHERE p.name STARTS WITH 'A' RETURN p.name AS name", nil, "[Ada]"},
		{"ends with identifier", "MATCH (p:Person) WHERE p.name = 'WEEKENDS' WITH p RETURN p.name AS name", nil, "[]"},
		{"three chained with", "MATCH (p:Person) WITH p AS a WITH a AS b WITH b AS c RETURN c.name AS name ORDER BY name", nil, "[Ada | Bob | Cy]"},
		{"count in item list", "UNWIND $xs AS x WITH count(*) AS c, sum(x) AS s RETURN c AS c, s AS s", map[string]any{"xs": []any{int64(1), int64(2)}}, "[2 3]"},
		{"unwind with binding", "MATCH (p:Person) WITH collect(p.name) AS xs UNWIND xs AS x RETURN count(*) AS c", nil, "[3]"},
	}
	for _, testCase := range accepted {
		t.Run("accept/"+testCase.name, func(t *testing.T) {
			result, err := db.Query(testCase.query, people(testCase.param))
			if err != nil {
				t.Fatalf("query %q: %v", testCase.query, err)
			}
			var parts []string
			for _, row := range result.Rows {
				var values []string
				for _, column := range result.Columns {
					values = append(values, fmt.Sprint(row[column]))
				}
				parts = append(parts, strings.Join(values, " "))
			}
			if got := "[" + strings.Join(parts, " | ") + "]"; got != testCase.want {
				t.Fatalf("query %q rows = %s, want %s", testCase.query, got, testCase.want)
			}
		})
	}
}

func TestWithGroupingByEntityKeepsDistinctGroups(t *testing.T) {
	db := openWithDB(t)
	result, err := db.Query("MATCH (p:Person) WITH p, count(*) AS c RETURN c AS c", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("grouped rows = %v, want one row per person", result.Rows)
	}
	for _, row := range result.Rows {
		if got := fmt.Sprint(row["c"]); got != "1" {
			t.Fatalf("group count = %q, want 1", got)
		}
	}
}

func TestWithMutationAfterWithUsesWritePath(t *testing.T) {
	db := openWithDB(t)
	if _, err := db.Query("MATCH (p:Person) WITH p MATCH (p) SET p.flag = true RETURN p.name AS name", nil); err != nil {
		t.Fatalf("mutation after WITH: %v", err)
	}
	result, err := db.Query("MATCH (p:Person) WHERE p.flag = true RETURN count(*) AS c", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(result.Rows[0]["c"]); got != "3" {
		t.Fatalf("flagged rows = %q, want 3", got)
	}
}

func TestWithRoleSurvivesChainedParts(t *testing.T) {
	db := openWithDB(t)
	result, err := db.Query("MATCH (p:Person) WITH p WITH p MATCH (p)-[:WORKS_AT]->(c:Company) RETURN count(*) AS c", nil)
	if err != nil {
		t.Fatalf("chained entity traversal: %v", err)
	}
	if got := fmt.Sprint(result.Rows[0]["c"]); got != "2" {
		t.Fatalf("chained traversal count = %q, want 2", got)
	}
}

func TestWithMaterializationRespectsByteBudget(t *testing.T) {
	db := openWithDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Big"}, Properties: map[string]any{"blob": make([]byte, 1<<20)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	queries := []string{
		"MATCH (n:Big) WITH n, count(*) AS c RETURN count(*) AS groups",
		"MATCH (n:Big) WITH DISTINCT n RETURN count(*) AS groups",
		"MATCH (n:Big) WITH collect(n.blob) AS blobs RETURN count(*) AS groups",
	}
	for _, query := range queries {
		_, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxBytes: 4096})
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("query %q error = %v, want ErrResourceLimit", query, err)
		}
	}
}
