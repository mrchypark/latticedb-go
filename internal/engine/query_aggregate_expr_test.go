package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func openAggregateDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "aggregates.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		for _, person := range []struct {
			team string
			age  int64
		}{
			{"red", 30},
			{"red", 40},
			{"blue", 25},
		} {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"team": person.team, "age": person.age}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func aggregateRows(t *testing.T, db *DB, query string) []map[string]any {
	t.Helper()
	result, err := db.Query(query, nil)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return result.Rows
}

func TestAggregateProjections(t *testing.T) {
	db := openAggregateDB(t)
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"sum", `MATCH (n:Person) RETURN sum(n.age) AS v`, "[95]"},
		{"min", `MATCH (n:Person) RETURN min(n.age) AS v`, "[25]"},
		{"max", `MATCH (n:Person) RETURN max(n.age) AS v`, "[40]"},
		{"collect", `MATCH (n:Person) RETURN collect(n.team) AS v`, "[[red red blue]]"},
		{"count projection", `MATCH (n:Person) RETURN count(n.age) AS v`, "[3]"},
		{"grouped count", `MATCH (n:Person) RETURN n.team AS team, count(*) AS v ORDER BY team`, "[blue 1 | red 2]"},
		{"grouped sum", `MATCH (n:Person) RETURN n.team AS team, sum(n.age) AS v ORDER BY team`, "[blue 25 | red 70]"},
		{"function plus aggregate", `MATCH (n:Person) RETURN toLower(n.team) AS team, count(*) AS v ORDER BY team`, "[blue 1 | red 2]"},
		{"order and limit", `MATCH (n:Person) RETURN n.team AS team, count(*) AS v ORDER BY v DESC, team LIMIT 1`, "[red 2]"},
		{"skip", `MATCH (n:Person) RETURN n.team AS team, count(*) AS v ORDER BY team SKIP 1`, "[red 2]"},
		{"empty aggregate input", `MATCH (n:Missing) RETURN sum(n.age) AS v`, "[0]"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			rows := aggregateRows(t, db, testCase.query)
			var parts []string
			for _, row := range rows {
				var columns []string
				for _, column := range resultColumns(t, db, testCase.query) {
					columns = append(columns, fmt.Sprint(row[column]))
				}
				parts = append(parts, strings.Join(columns, " "))
			}
			if got := "[" + strings.Join(parts, " | ") + "]"; got != testCase.want {
				t.Fatalf("query %q rows = %s, want %s", testCase.query, got, testCase.want)
			}
		})
	}
}

func resultColumns(t *testing.T, db *DB, query string) []string {
	t.Helper()
	result, err := db.Query(query, nil)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return result.Columns
}

func TestAggregateGroupingWithoutRows(t *testing.T) {
	db := openAggregateDB(t)
	rows := aggregateRows(t, db, `MATCH (n:Missing) RETURN n.team AS team, count(*) AS total`)
	if len(rows) != 0 {
		t.Fatalf("grouped aggregate over no rows = %v, want no rows", rows)
	}
}

// The upstream accumulator appends every value, including NULL, so a missing
// property surfaces as a nil element rather than being dropped.
func TestAggregateCollectKeepsNullInputs(t *testing.T) {
	db := openAggregateDB(t)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"team": "green"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	rows := aggregateRows(t, db, `MATCH (n:Person) RETURN n.team AS team, collect(n.age) AS ages ORDER BY team`)
	for _, row := range rows {
		if row["team"] != "green" {
			continue
		}
		if got := fmt.Sprint(row["ages"]); got != "[<nil>]" {
			t.Fatalf("collect over missing property = %q, want [<nil>]", got)
		}
		return
	}
	t.Fatalf("green group missing from %v", rows)
}
