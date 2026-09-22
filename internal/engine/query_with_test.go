package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func openWithDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "with.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		var people []Node
		for _, person := range []struct {
			name string
			team string
			age  int64
		}{
			{"Ada", "red", 30},
			{"Bob", "red", 40},
			{"Cy", "blue", 25},
		} {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"name": person.name, "team": person.team, "age": person.age}})
			if err != nil {
				return err
			}
			people = append(people, node)
		}
		company, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Company"}, Properties: map[string]any{"name": "LatticeDB"}})
		if err != nil {
			return err
		}
		for _, person := range people[:2] {
			if _, err := tx.CreateEdge(person.ID, company.ID, "WORKS_AT", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		_, err = tx.CreateEdge(people[0].ID, people[2].ID, "KNOWS", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func withRows(t *testing.T, db *DB, query string) []map[string]any {
	t.Helper()
	result, err := db.Query(query, nil)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return result.Rows
}

func TestWithPassThroughKeepsTraversal(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MATCH (p:Person) WITH p MATCH (p)-[:WORKS_AT]->(c:Company) RETURN count(*) AS total")
	if got := fmt.Sprint(rows[0]["total"]); got != "2" {
		t.Fatalf("pass-through traversal total = %q, want 2", got)
	}
}

func TestWithAliasAndInlineFilter(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MATCH (p:Person) WITH p AS q WHERE q.age >= 30 RETURN q.name AS name ORDER BY name")
	if len(rows) != 2 || rows[0]["name"] != "Ada" || rows[1]["name"] != "Bob" {
		t.Fatalf("alias filter rows = %v", rows)
	}
}

func TestWithAggregation(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MATCH (p:Person) WITH p.team AS team, count(*) AS total RETURN team AS team, total AS total ORDER BY team")
	if len(rows) != 2 {
		t.Fatalf("aggregate rows = %v", rows)
	}
	if rows[0]["team"] != "blue" || fmt.Sprint(rows[0]["total"]) != "1" || rows[1]["team"] != "red" || fmt.Sprint(rows[1]["total"]) != "2" {
		t.Fatalf("aggregate rows = %v", rows)
	}
}

func TestWithAggregationOverEmptyMatch(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MATCH (p:Missing) WITH count(*) AS total RETURN total AS total")
	if len(rows) != 1 || fmt.Sprint(rows[0]["total"]) != "0" {
		t.Fatalf("empty aggregate rows = %v", rows)
	}
}

func TestWithOrderSkipLimitBeforeNextPart(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MATCH (p:Person) WITH p ORDER BY p.age DESC LIMIT 2 MATCH (p)-[:WORKS_AT]->(c:Company) RETURN count(*) AS total")
	if got := fmt.Sprint(rows[0]["total"]); got != "2" {
		t.Fatalf("ordered limit total = %q, want 2", got)
	}
	rows = withRows(t, db, "MATCH (p:Person) WITH p ORDER BY p.age DESC SKIP 1 LIMIT 1 MATCH (p)-[:WORKS_AT]->(c:Company) RETURN count(*) AS total")
	if got := fmt.Sprint(rows[0]["total"]); got != "1" {
		t.Fatalf("skipped limit total = %q, want 1", got)
	}
}

func TestWithChainedParts(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MATCH (p:Person) WITH p.team AS team, collect(p.age) AS ages WITH team AS team, ages AS ages RETURN team AS team, ages AS ages ORDER BY team")
	if len(rows) != 2 || rows[0]["team"] != "blue" || rows[1]["team"] != "red" {
		t.Fatalf("chained with rows = %v", rows)
	}
}

func TestWithRejections(t *testing.T) {
	db := openWithDB(t)
	cases := []struct {
		query string
		want  string
	}{
		{"MATCH (p:Person) WITH p.name AS name RETURN p.name AS value", "unknown binding"},
		{"MATCH (p:Person) WITH p", "WITH must be followed by another clause"},
		{"MATCH (p:Person) WITH p.name RETURN value", "unknown binding"},
	}
	for _, testCase := range cases {
		t.Run(testCase.query, func(t *testing.T) {
			_, err := db.Query(testCase.query, nil)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("query %q error = %v, want substring %q", testCase.query, err, testCase.want)
			}
		})
	}
}
