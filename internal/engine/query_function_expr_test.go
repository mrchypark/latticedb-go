package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func openFunctionExprDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "functions.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		person, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"name": "Ada"}})
		if err != nil {
			return err
		}
		company, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Company"}, Properties: map[string]any{"name": "LatticeDB"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(person.ID, company.ID, "WORKS_AT", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestFunctionExpressionsInQueryClauses(t *testing.T) {
	db := openFunctionExprDB(t)
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"lower", `MATCH (n:Person) RETURN toLower(n.name) AS v`, "ada"},
		{"upper", `MATCH (n:Person) RETURN toUpper(n.name) AS v`, "ADA"},
		{"labels", `MATCH (n:Person) RETURN labels(n) AS v`, "[Person]"},
		{"size", `MATCH (n:Person) RETURN size(n.name) AS v`, "3"},
		{"coalesce missing property", `MATCH (n:Person) RETURN coalesce(n.missing, "fallback") AS v`, "fallback"},
		{"nested call", `MATCH (n:Person) RETURN size(split(n.name, "a")) AS v`, "2"},
		{"edge type", `MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN type(r) AS v`, "WORKS_AT"},
		{"conversion", `MATCH (n:Person) RETURN toString(size(n.name)) AS v`, "3"},
		{"range", `MATCH (n:Person) RETURN range(1, 3) AS v`, "[1 2 3]"},
		{"substring", `MATCH (n:Person) RETURN substring(n.name, 1) AS v`, "da"},
		{"abs", `MATCH (n:Person) RETURN abs(-2) AS v`, "2"},
		{"parameter argument", `MATCH (n:Person) RETURN replace(n.name, $from, $to) AS v`, "Ada"},
		{"where clause", `MATCH (n:Person) WHERE n.name = replace($name, "X", "") RETURN n.name AS v`, "Ada"},
		{"set clause", `MATCH (n:Person) SET n.slug = toLower(n.name) RETURN n.slug AS v`, "ada"},
		{"create properties", `CREATE (n:Tag {name: toLower("Mixed")}) RETURN n.name AS v`, "mixed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			params := map[string]any{"from": "Ada", "to": "Ada", "name": "Ada"}
			result, err := db.Query(testCase.query, params)
			if err != nil {
				t.Fatalf("query %q: %v", testCase.query, err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("query %q returned %d rows, want 1", testCase.query, len(result.Rows))
			}
			if got := fmt.Sprint(result.Rows[0]["v"]); got != testCase.want {
				t.Fatalf("query %q value = %q, want %q", testCase.query, got, testCase.want)
			}
		})
	}
}

func TestFunctionExpressionWidthProjectionColumn(t *testing.T) {
	db := openFunctionExprDB(t)
	result, err := db.Query(`MATCH (n:Person) RETURN n.name AS name, size(n.name) AS width`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Columns) != 2 || result.Columns[1] != "width" {
		t.Fatalf("columns = %v", result.Columns)
	}
	if got := fmt.Sprint(result.Rows[0]["width"]); got != "3" {
		t.Fatalf("width = %q, want 3", got)
	}
}

func TestFunctionExpressionRejections(t *testing.T) {
	db := openFunctionExprDB(t)
	cases := []struct {
		query string
		want  string
	}{
		{`MATCH (n:Person) RETURN nope(n.name) AS v`, "unknown function"},
		{`MATCH (n:Person) RETURN toLower() AS v`, "toLower expects 1 argument, got 0"},
		{`MATCH (n:Person) RETURN size(n.name, n.name) AS v`, "size expects 1 argument, got 2"},
		{`MATCH (n:Person) RETURN toLower(n.name) AS v ORDER BY v`, "ORDER BY on a computed projection"},
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

func TestFunctionExpressionTypeErrorIsReported(t *testing.T) {
	db := openFunctionExprDB(t)
	_, err := db.Query(`MATCH (n:Person) RETURN head(n.name) AS v`, nil)
	if err == nil || !strings.Contains(err.Error(), "head expects a list") {
		t.Fatalf("error = %v, want a list type error", err)
	}
}
