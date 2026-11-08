package engine

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestQueryUnionSemantics(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "union.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cases := []struct {
		name  string
		query string
		want  []any
	}{
		{
			name:  "all preserves branch order and multiplicity",
			query: `UNWIND [1, 2, 1] AS v RETURN v AS x UNION ALL UNWIND [2, 3] AS v RETURN v AS x`,
			want:  []any{int64(1), int64(2), int64(1), int64(2), int64(3)},
		},
		{
			name:  "union keeps the first equal row",
			query: `UNWIND [1, 2, 1] AS v RETURN v AS x UNION UNWIND [2, 3] AS v RETURN v AS x`,
			want:  []any{int64(1), int64(2), int64(3)},
		},
		{
			name:  "tails remain branch local",
			query: `UNWIND [3, 1, 2] AS v RETURN v AS x ORDER BY x SKIP 1 LIMIT 1 UNION ALL UNWIND [4, 2] AS v RETURN v AS x ORDER BY x SKIP 1`,
			want:  []any{int64(2), int64(4)},
		},
		{
			name:  "quoted union is text",
			query: `RETURN 'UNION ALL' AS x UNION ALL RETURN 'UNION' AS x`,
			want:  []any{"UNION ALL", "UNION"},
		},
		{
			name:  "column types may differ",
			query: `RETURN 1 AS x UNION ALL RETURN 1.5 AS x`,
			want:  []any{int64(1), float64(1.5)},
		},
		{
			name:  "one terminal semicolon",
			query: `RETURN 1 AS x UNION ALL RETURN 2 AS x;`,
			want:  []any{int64(1), int64(2)},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result, err := db.Query(test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]any, 0, len(result.Rows))
			for _, row := range result.Rows {
				got = append(got, row["x"])
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("values = %v, want %v", got, test.want)
			}
		})
	}
}

func TestQueryUnionCanonicalEquality(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "union-equality.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"name": "Ada"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	left := []any{
		int64(1),
		[]any{nil, map[string]any{"items": []any{int64(2), nil}}},
		nil,
		"same",
	}
	right := []any{
		float64(1),
		[]any{nil, map[string]any{"items": []any{int64(2), nil}}},
		nil,
		"same",
	}
	result, err := db.Query(
		`UNWIND $left AS v RETURN v AS x UNION UNWIND $right AS v RETURN v AS x`,
		map[string]any{"left": left, "right": right},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("canonical distinct rows = %d, want 4", len(result.Rows))
	}

	result, err = db.Query(
		`MATCH (n:Person) RETURN n AS x UNION MATCH (n:Person) RETURN n AS x`,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("entity distinct rows = %d, want 1", len(result.Rows))
	}
}

func TestQueryUnionPreservesUppercaseUnionIdentifiers(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "union-identifier.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Named"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		query string
		check func(QueryResult) bool
	}{
		{`MATCH (UNION:Named) RETURN UNION`, func(result QueryResult) bool { return len(result.Rows) == 1 }},
		{`RETURN 1 AS UNION`, func(result QueryResult) bool { return result.Rows[0]["UNION"] == int64(1) }},
		{`WITH 1 AS UNION RETURN [1, UNION ] AS value`, func(result QueryResult) bool {
			return reflect.DeepEqual(result.Rows[0]["value"], []any{int64(1), int64(1)})
		}},
	}
	for _, test := range cases {
		result, err := db.Query(test.query, nil)
		if err != nil {
			t.Fatalf("%s: %v", test.query, err)
		}
		if !test.check(result) {
			t.Fatalf("%s returned %#v", test.query, result.Rows)
		}
	}
}

func TestQueryUnionRejectsInvalidGrammarAtParseTime(t *testing.T) {
	queries := []string{
		`UNION ALL RETURN 1 AS x`,
		`RETURN 1 AS x UNION ALL`,
		`RETURN 1 AS x UNION ALL RETURN 2 AS x UNION RETURN 3 AS x`,
		`RETURN 1 AS x UNION DISTINCT RETURN 1 AS x`,
		`RETURN 1 AS x UNION ALL RETURN 1 AS y`,
		`RETURN 1 AS x, 2 AS y UNION ALL RETURN 2 AS y, 1 AS x`,
		`RETURN [1 UNION ALL 2] AS x`,
		`CREATE (n:UnionMutation) RETURN id(n) AS x UNION ALL RETURN 1 AS x`,
		`UNWIND [1] AS x UNION ALL RETURN 1 AS x`,
		`RETURN 1 AS x;;`,
		`RETURN 1 AS x; RETURN 2 AS x`,
	}
	for _, query := range queries {
		if _, err := parseQuery(query); err == nil {
			t.Fatalf("parseQuery accepted %q", query)
		}
	}

	db, err := Open(filepath.Join(t.TempDir(), "union-parse.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := db.Query(`UNWIND $missing AS x RETURN x AS x UNION ALL RETURN 1 AS y`, nil)
	var queryErr *QueryError
	if !errors.As(err, &queryErr) || queryErr.Stage != QueryErrorStageParse {
		t.Fatalf("schema error = %v, want parse error", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("parse error returned rows: %#v", result.Rows)
	}
}

func TestQueryUnionMaxRowsCountsBeforeDeduplication(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "union-rows.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	query := `UNWIND $left AS x RETURN x AS x UNION UNWIND $right AS x RETURN x AS x`
	params := map[string]any{
		"left":  []any{int64(1), int64(1), int64(1)},
		"right": []any{int64(1), int64(1), int64(1)},
	}
	result, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxRows: 5})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("MaxRows error = %v, want resource limit", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("MaxRows error returned partial rows: %#v", result.Rows)
	}
	result, err = db.QueryContext(context.Background(), query, params, QueryOptions{MaxRows: 6})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("MaxRows boundary rows=%d err=%v", len(result.Rows), err)
	}
}
