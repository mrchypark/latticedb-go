package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestQueryListLiterals(t *testing.T) {
	db := openFunctionExprDB(t)
	for _, tc := range []struct{ query, want string }{
		{`UNWIND [1, 2, 3] AS x RETURN sum(x) AS v`, "6"},
		{`MATCH (n:Person) WHERE n.name IN ["Bob", $name] RETURN n.name AS v`, "Ada"},
		{`MATCH (n:Person) RETURN [n.name, [null, size(n.name)], head([7, 8])] AS v`, "[Ada [<nil> 3] 7]"},
		{`MATCH (n:Person) WITH [n.name, "Bob"] AS names UNWIND names AS v RETURN v ORDER BY v LIMIT 1`, "Ada"},
		{`MATCH (n:Person) RETURN size([]) AS v`, "0"},
		{`CREATE (n:List {items: [1, [2], null]}) RETURN n.items AS v`, "[1 [2] <nil>]"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := db.Query(tc.query, map[string]any{"name": "Ada"})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("rows = %v", result.Rows)
			}
			if fmt.Sprint(result.Rows[0]["v"]) != tc.want {
				t.Fatalf("got %v, want %s", result.Rows, tc.want)
			}
		})
	}
	result, err := db.Query(`MATCH (n:Person) RETURN [n, [n]] AS v`, nil)
	if err != nil {
		t.Fatal(err)
	}
	items := result.Rows[0]["v"].([]any)
	first, ok := items[0].(Node)
	if !ok || first.Properties["name"] != "Ada" {
		t.Fatalf("public node = %#v", items[0])
	}
	nested, ok := items[1].([]any)[0].(Node)
	if !ok || nested.ID != first.ID {
		t.Fatalf("nested public node = %#v", items[1])
	}

	for _, query := range []string{
		`MATCH (n:Person) RETURN [missing]`,
		`MATCH (n:Missing) RETURN [[unknown]]`,
		`UNWIND [1,] AS n RETURN n`,
		`UNWIND [1][2] AS n RETURN n`,
		`UNWIND [,1] AS n RETURN n`,
		`UNWIND [head()] AS n RETURN n`,
		`UNWIND [$missing] AS n RETURN n`,
	} {
		if _, err := db.Query(query, nil); err == nil {
			t.Errorf("accepted %s", query)
		}
	}
}

func TestListLiteralBudget(t *testing.T) {
	expr, err := parseValueExpr(`[1, [2, 3]]`)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []uint64{87, 88} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: limit})
		_, err := expr.eval(queryRow{}, nil, budget)
		releaseQueryBudget(budget)
		if (limit == 87) != errors.Is(err, ErrResourceLimit) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	db := openFunctionExprDB(t)
	_, err = db.QueryContext(context.Background(), `UNWIND [1, [2, 3]] AS x RETURN x`, nil, QueryOptions{MaxBytes: 1})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("got %v", err)
	}
}
