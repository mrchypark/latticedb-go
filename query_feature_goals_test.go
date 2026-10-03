package latticedb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// Exercise the feature goals through the public API, including nested edge
// conversion and the shared WITH/mutation pipeline.
func TestQueryFeatureGoalsTogether(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "goals.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`MATCH (a:Person) WHERE b.x = 1 MATCH (b) RETURN a`, nil); err == nil {
		t.Fatal("WHERE accepted a binding introduced by a later MATCH")
	}
	if _, err := db.Query("MERGE (a:Goal {key: 1})-[:NEXT]->(b:Goal {key: 2})", nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("UNWIND [2, 2] AS key MERGE (n:Goal {key: key}) ON MATCH SET n.visits = coalesce(n.visits, 0) + 1 RETURN sum(DISTINCT key ^ 2) AS total", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != float64(4) {
		t.Fatalf("MERGE/arithmetic/DISTINCT: %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (a:Goal {key: 1})-[path:NEXT*0..1]->(b) WITH b, path, size(path) AS hops RETURN b.key AS key, path, hops ORDER BY hops", nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("variable path/WITH: %#v, %v", result.Rows, err)
	}
	if result.Rows[0]["hops"] != int64(0) || result.Rows[1]["hops"] != int64(1) {
		t.Fatalf("path lengths: %#v", result.Rows)
	}
	path := result.Rows[1]["path"].([]any)
	if len(path) != 1 {
		t.Fatalf("path edges: %#v", path)
	}
	if edge, ok := path[0].(Edge); !ok || edge.Type != "NEXT" {
		t.Fatalf("public relationship conversion: %#v", path[0])
	}
	result, err = db.Query("RETURN (2 + 3) * 4 AS value", nil)
	if err != nil || result.Rows[0]["value"] != int64(20) {
		t.Fatalf("standalone RETURN: %#v, %v", result.Rows, err)
	}
}

func TestQueryAnonymousCreatePathUsesFreshNodes(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "anonymous-create.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (a:A)-[:R]->(b:B)`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(`MATCH (a:A)-[:R]->() CREATE (a)-[:S]->()`, nil); err != nil {
		t.Fatalf("anonymous path CREATE: %v", err)
	}
	result, err := db.Query(`MATCH (a:A)-[r:R]->(b:B) RETURN id(a) AS a, id(b) AS b`, nil)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("seed path: %#v, %v", result.Rows, err)
	}
	seedA, aOK := result.Rows[0]["a"].(int64)
	seedB, bOK := result.Rows[0]["b"].(int64)
	if !aOK || !bOK || seedA == seedB {
		t.Fatalf("seed path endpoints: %#v", result.Rows[0])
	}
	result, err = db.Query(`MATCH (a:A)-[r:S]->(b) RETURN id(a) AS a, id(b) AS b`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["a"] != seedA {
		t.Fatalf("created S path: %#v, %v", result.Rows, err)
	}
	createdTarget, ok := result.Rows[0]["b"].(int64)
	if !ok || createdTarget == seedB {
		t.Fatalf("anonymous CREATE endpoint reused matched node: seedB=%d S-target=%#v", seedB, result.Rows[0]["b"])
	}
	result, err = db.Query(`MATCH (n) RETURN count(n) AS count`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["count"] != int64(3) {
		t.Fatalf("CREATE reused matched anonymous nodes: rows=%#v err=%v", result.Rows, err)
	}
	if _, err := db.Query(`CREATE ()<-[:INCOMING {weight: 1}]-()`, nil); err != nil {
		t.Fatalf("incoming anonymous CREATE path with relationship properties: %v", err)
	}
	result, err = db.Query(`MATCH (a)-[r:INCOMING {weight: 1}]->(b) RETURN id(a) AS a, id(b) AS b`, nil)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("incoming CREATE direction/property: %#v, %v", result.Rows, err)
	}
}

func TestQuerySequentialAnonymousPropertyMatchesAreIndependent(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "anonymous-property-scopes.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:A {x: 1}), (:B {x: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (:A {x: 1}) MATCH (:B {x: 2}) RETURN count(*) AS n`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"] != int64(1) {
		t.Fatalf("sequential anonymous node patterns collided: %#v, %v", result.Rows, err)
	}
}

func TestQuerySequentialMatchSkipsLaterWhereAfterEmptyScope(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sequential-where.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:A {x: 2}), (:B {x: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (a:A) WHERE a.x = 0 OR a.x = 1 MATCH (b:B) WHERE b.x = toLower(1) RETURN b`
	result, err := db.Query(query, nil)
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("later WHERE ran after prior MATCH scope was empty: rows=%#v err=%v", result.Rows, err)
	}
	if _, err := db.Query(`CREATE (:A {x: 0})`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(query, nil); err == nil {
		t.Fatal("later WHERE error was hidden despite a retained row")
	}
}

func TestQueryLaterIndexedPredicateDoesNotPruneEarlierMatchErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sequential-index-scope.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:Person {name: 42, x: 2}), (:Person {name: 'Ada', x: 1})`, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "x"); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (a:Person) WHERE toLower(a.name) = 'ada' MATCH (a:Person) WHERE a.x = 1 RETURN a`
	if _, err := db.Query(query, nil); err == nil {
		t.Fatalf("later indexed equality pruned the row before the earlier WHERE error: %s", query)
	}
}

func TestQueryLaterIndexedBindingIDDoesNotPruneEarlierMatchErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sequential-index-bindingid.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:A {x: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("A", "x"); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (a:A) WHERE a.x = 0 MATCH (a) WHERE id(a) = $bad RETURN a`
	result, err := db.Query(query, map[string]any{"bad": "notanint"})
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("later indexed binding-id comparison with empty first scope: rows=%#v err=%v", result.Rows, err)
	}
}

func TestQueryLaterIndexedMissingParamDoesNotPruneEarlierMatchErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sequential-index-missing-param.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:A {x: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (a:A) WHERE a.x = 0 MATCH (a:A) WHERE a.x = $missing RETURN a`
	result, err := db.Query(query, nil)
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("later indexed equality with missing param and empty first scope: rows=%#v err=%v", result.Rows, err)
	}
}

func TestQuerySequentialMatchRetainedRowErrorControl(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sequential-retained-row-error.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:A {x: 1}), (:A {x: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (a:A) WHERE a.x = 1 MATCH (a:A) WHERE a.x = toLower(1) RETURN a`
	if _, err := db.Query(query, nil); err == nil {
		t.Fatalf("later WHERE error was hidden despite a retained row: %s", query)
	}
}

func TestQuerySequentialMatchLimitDoesNotTruncateIntermediateScope(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sequential-limit-intermediate.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:A {x: 1}), (:A {x: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (a:A) MATCH (a) WHERE a.x = 2 RETURN a.x AS x LIMIT 1`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["x"] != int64(2) {
		t.Fatalf("terminal LIMIT truncated intermediate MATCH: got %#v, want x=2", result.Rows)
	}
	result, err = db.Query(`MATCH (a:A) MATCH (a) WHERE a.x = 2 RETURN a.x AS x`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["x"] != int64(2) {
		t.Fatalf("intermediate MATCH scope was truncated: got %#v, want x=2", result.Rows)
	}
}

func TestQueryWithWhereValidatesExpressionBindings(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "with-where-expression.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Query(`UNWIND [1, 2] AS x WITH x WHERE abs(x) > 1 RETURN x`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["x"] != int64(2) {
		t.Fatalf("WITH WHERE function expression: %#v, %v", result.Rows, err)
	}
	for _, query := range []string{
		`UNWIND [1] AS x WITH x AS z WHERE abs(x) > 0 RETURN z`,
		`UNWIND [1] AS x WITH x AS z WHERE z > x RETURN z`,
	} {
		if _, err := db.Query(query, nil); err == nil {
			t.Errorf("WITH WHERE accepted non-projected expression binding: %s", query)
		}
	}
}

func TestQueryComputedOrderedComparisonsPreserveUnknown(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "comparison-unknown.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(`CREATE (:Number {x: 2}), (:NullValue {x: null})`, nil); err != nil {
		t.Fatal(err)
	}
	for _, operator := range []string{"<", "<=", ">", ">="} {
		for _, negated := range []bool{false, true} {
			prefix := ""
			if negated {
				prefix = "NOT "
			}
			for _, test := range []struct {
				name  string
				query string
			}{
				{name: "property incomparable", query: `MATCH (n:Number) WHERE ` + prefix + `n.x ` + operator + ` 'bad' RETURN count(n) AS n`},
				{name: "expression incomparable", query: `MATCH (n:Number) WHERE ` + prefix + `abs(n.x) ` + operator + ` 'bad' RETURN count(n) AS n`},
				{name: "property null", query: `MATCH (n:NullValue) WHERE ` + prefix + `n.x ` + operator + ` 2 RETURN count(n) AS n`},
			} {
				result, err := db.Query(test.query, nil)
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"] != int64(0) {
					t.Errorf("%s, operator %q negated=%t: %#v, %v", test.name, operator, negated, result.Rows, err)
				}
			}
		}
	}
	for _, test := range []struct {
		operator string
		want     int64
	}{
		{operator: "<", want: 0}, {operator: "<=", want: 1},
		{operator: ">", want: 0}, {operator: ">=", want: 1},
	} {
		for _, negated := range []bool{false, true} {
			prefix := ""
			want := test.want
			if negated {
				prefix = "NOT "
				want = 1 - want
			}
			for _, expression := range []string{"n.x", "abs(n.x)"} {
				result, err := db.Query(`MATCH (n:Number) WHERE `+prefix+expression+` `+test.operator+` 2 RETURN count(n) AS n`, nil)
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"] != want {
					t.Errorf("ordered numeric %s %s negated=%t: %#v, %v; want %d", expression, test.operator, negated, result.Rows, err, want)
				}
			}
		}
	}
}

func TestQueryCreatePathsSequentialMatchAndExpressionPredicate(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-create-paths.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Query(`CREATE (a:Person {name: 'Ada', rank: 5})-[:KNOWS]->(b:Person {name: 'Bob'}), (team:Team {name: 'Ops'})`, nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (a:Person {name: 'Ada'}) WHERE abs(toInteger(a.rank) + 1) = abs(toInteger(2) * toInteger(3)) MATCH (a)-[:KNOWS]->(b:Person) WHERE toLower(b.name) = toLower('Bob') RETURN b.name AS name`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Bob" {
		t.Fatalf("sequential MATCH/separate expression predicates: %#v, %v", result.Rows, err)
	}

	if _, err := db.QueryContext(context.Background(), `MATCH (a:Person {name: 'Ada'}) CREATE (a)-[:KNOWS]->(c:Person {name: $name})`, map[string]any{"name": "Cara"}, QueryOptions{}); err != nil {
		t.Fatalf("CREATE with existing binding and parameter: %v", err)
	}
	result, err = db.Query(`MATCH (a:Person {name: 'Ada'})-[:KNOWS]->(c:Person {name: 'Cara'}) RETURN a.name AS from, c.name AS to`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["from"] != "Ada" || result.Rows[0]["to"] != "Cara" {
		t.Fatalf("CREATE existing node binding: %#v, %v", result.Rows, err)
	}

	if _, err := db.Query(`CREATE (x:Rollback {value: 1}), (y:Rollback {value: toLower(1)})`, nil); err == nil {
		t.Fatal("later CREATE expression error unexpectedly succeeded")
	}
	if _, err := db.Query(`CREATE (repeated:Repeated)-[:R]->(repeated {x: 1})`, nil); err == nil {
		t.Fatal("repeated CREATE node silently discarded its property constraint")
	}
	result, err = db.Query(`MATCH (n:Rollback) RETURN count(n) AS count`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("later CREATE error rollback: %#v, %v", result.Rows, err)
	}

	if _, err := db.QueryContext(context.Background(), `CREATE (x:Rollback {value: 1})-[:LINK]->(y:Rollback)`, nil, QueryOptions{MaxWork: 2}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("limited CREATE error = %v, want resource limit", err)
	}
	result, err = db.Query(`MATCH (n:Rollback) RETURN count(n) AS count`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("failed CREATE rollback: %#v, %v", result.Rows, err)
	}

	if _, err := db.QueryContext(context.Background(), `UNWIND [1, 2] AS i CREATE (n:Limited {i: i})`, nil, QueryOptions{MaxRows: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("limited row count error = %v, want resource limit", err)
	}
	result, err = db.Query(`MATCH (n:Limited) RETURN count(n) AS count`, nil)
	if err != nil || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("limited rows rollback: %#v, %v", result.Rows, err)
	}
}
