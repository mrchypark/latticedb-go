package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseExistsPredicate(t *testing.T) {
	predicate, err := parseExistsPredicate(`EXISTS { MATCH (p:Person)-[:LINK]->(x:Item) WHERE x.tag = p.tag }`)
	if err != nil {
		t.Fatal(err)
	}
	if len(predicate.patterns) != 1 || predicate.wherePredicate != nil || len(predicate.whereClauses) == 0 {
		t.Fatalf("parsed EXISTS = %#v", predicate)
	}
	edge, ok := predicate.patterns[0].(edgePattern)
	if !ok || edge.EdgeType != "LINK" || edge.VariableLength {
		t.Fatalf("parsed edge = %#v", predicate.patterns[0])
	}
	for _, name := range []string{edge.Left.Var, edge.Right.Var} {
		if strings.HasPrefix(name, "\x00") && !strings.HasPrefix(name, "\x00exists:") {
			t.Fatalf("inner hidden binding %q is not namespaced", name)
		}
	}

	if _, err := parseExistsPredicate(`EXISTS { MATCH (n) WHERE n.name STARTS WITH "A" }`); err != nil {
		t.Fatalf("STARTS WITH was rejected: %v", err)
	}
	for _, text := range []string{
		`EXISTS { MATCH (n)-[:LINK*1..2]->(m) }`,
		`EXISTS { MATCH (n) WITH n }`,
		`EXISTS { MATCH (n) RETURN n }`,
		`EXISTS { MATCH (n) SET n.changed = true }`,
		`EXISTS { MATCH (n) } trailing`,
		`EXISTS { RETURN 1 }`,
		`EXISTS { MATCH (n) WHERE n.embedding <=> $vector }`,
		`EXISTS { MATCH (n) WHERE n.text @@ "graph" }`,
		`EXISTS { MATCH (n) WHERE EXISTS { MATCH (n) } }`,
	} {
		if _, err := parseExistsPredicate(text); err == nil {
			t.Fatalf("accepted unsupported EXISTS %q", text)
		}
	}
}

func TestValidateExistsPredicateScope(t *testing.T) {
	valid, err := parseExistsPredicate(`EXISTS { MATCH (p)-[:LINK]->(x:Item) WHERE x.tag = p.tag }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateExistsPredicateScope(valid, map[string]bindingRole{"p": bindingNode}); err != nil {
		t.Fatal(err)
	}

	unknown, err := parseExistsPredicate(`EXISTS { MATCH (p)-[:LINK]->(x:Item) WHERE x.tag = q.tag }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateExistsPredicateScope(unknown, map[string]bindingRole{"p": bindingNode}); err == nil {
		t.Fatal("accepted an unknown correlated binding")
	}

	conflict, err := parseExistsPredicate(`EXISTS { MATCH (p)-[p:LINK]->(x:Item) }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateExistsPredicateScope(conflict, map[string]bindingRole{"p": bindingNode}); err == nil {
		t.Fatal("accepted a node-to-edge binding role conflict")
	}

	local, err := parseExistsPredicate(`EXISTS { MATCH (n:Item) }`)
	if err != nil {
		t.Fatal(err)
	}
	leak := booleanPredicate{Operator: "AND", Items: []wherePredicate{local, &whereClause{Kind: whereIsNull, Var: "n"}}}
	if err := validateExistsPredicateScope(leak, nil); err == nil {
		t.Fatal("inner binding leaked into the outer lexical scope")
	}
}

func TestExistsEvalCorrelationNullAndPackageState(t *testing.T) {
	for _, backend := range []struct {
		name string
		page bool
	}{{"memory", false}, {"page", true}} {
		t.Run(backend.name, func(t *testing.T) {
			db, people, _ := openExistsFixture(t, backend.page)
			defer db.Close()
			correlated, err := parseExistsPredicate(`EXISTS { MATCH (p)-[:LINK]->(x:Item) WHERE x.tag = p.tag }`)
			if err != nil {
				t.Fatal(err)
			}
			missing, err := parseExistsPredicate(`EXISTS { MATCH (p)-[:LINK]->(x:Item) }`)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.View(func(tx *Tx) error {
				budget := newQueryBudget(t.Context(), QueryOptions{})
				defer releaseQueryBudget(budget)
				budget.activeTx = tx
				budget.residentSources = tx.graph.PageBase == nil
				for index, want := range []bool{true, true, false} {
					got, err := evalExistsTestRow(budget, tx, "p", people[index], correlated)
					if err != nil || got != want {
						t.Fatalf("person %d = %v, %v; want %v", people[index], got, err, want)
					}
				}
				got, err := evalExistsTestRow(budget, tx, "p", people[0], correlated)
				if err != nil || !got {
					t.Fatalf("cached plan evaluation = %v, %v", got, err)
				}
				got, err = evalExistsTestRow(budget, tx, "p", 0, missing)
				if err != nil || got {
					t.Fatalf("referenced NULL entity = %v, %v", got, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExistsQueryIntegration(t *testing.T) {
	for _, backend := range []struct {
		name string
		page bool
	}{{"memory", false}, {"page", true}} {
		t.Run(backend.name, func(t *testing.T) {
			db, _, _ := openExistsFixture(t, backend.page)
			defer db.Close()
			cases := []struct {
				name  string
				query string
				want  []any
			}{
				{
					"correlated",
					`MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LINK]->(x:Item) WHERE x.tag = p.tag } RETURN p.name AS name ORDER BY name`,
					[]any{"A", "B"},
				},
				{
					"negated",
					`MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:LINK]->(:Item) } RETURN p.name AS name ORDER BY name`,
					[]any{"C"},
				},
				{
					"boolean",
					`MATCH (p:Person) WHERE p.name = "C" OR EXISTS { MATCH (p)-[:LINK]->(x) WHERE x.tag = 1 } RETURN p.name AS name ORDER BY name`,
					[]any{"A", "C"},
				},
				{
					"parallel-edge-is-existence",
					`MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LINK]->(x:Item) } RETURN p.name AS name ORDER BY name`,
					[]any{"A", "B"},
				},
				{
					"outer-null-referenced",
					`MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) WITH p, x WHERE EXISTS { MATCH (x)-[:LINK]->(y) } RETURN p.name AS name ORDER BY name`,
					nil,
				},
				{
					"unrelated-null",
					`MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) WITH p, x WHERE EXISTS { MATCH (p)-[:LINK]->(y:Item) WHERE y.tag = p.tag } RETURN p.name AS name ORDER BY name`,
					[]any{"A", "B", "B"},
				},
				{
					"inner-alias-dropped",
					`MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LINK]->(x:Item) WHERE x.tag = p.tag } WITH p AS person RETURN person.name AS name ORDER BY name`,
					[]any{"A", "B"},
				},
				{
					"empty-input",
					`MATCH (p:Absent) WHERE EXISTS { MATCH (p)-[:LINK]->(x) } RETURN p.name AS name`,
					nil,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					check := func(result QueryResult, err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
						var got []any
						for _, row := range result.Rows {
							got = append(got, row["name"])
						}
						if len(got) != len(tc.want) {
							t.Fatalf("names = %v, want %v", got, tc.want)
						}
						for index := range got {
							if got[index] != tc.want[index] {
								t.Fatalf("names = %v, want %v", got, tc.want)
							}
						}
					}
					check(db.Query(tc.query, nil))
					check(db.Query(tc.query, nil))
					if err := db.View(func(tx *Tx) error { check(tx.Query(tc.query, nil)); return nil }); err != nil {
						t.Fatal(err)
					}
				})
			}

			if _, err := db.Query(`MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LINK]->(x) } RETURN x`, nil); err == nil {
				t.Fatal("inner binding escaped into RETURN scope")
			}
			if _, err := db.Query(`MATCH (p:Person) WHERE EXISTS { MATCH (p) WITH p RETURN p } RETURN p`, nil); err == nil {
				t.Fatal("accepted inner WITH")
			}
		})
	}
}

func TestExistsEvalUnrelatedNullDoesNotFail(t *testing.T) {
	db, people, _ := openExistsFixture(t, false)
	defer db.Close()
	predicate, err := parseExistsPredicate(`EXISTS { MATCH (p)-[:LINK]->(x:Item) }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		budget := newQueryBudget(t.Context(), QueryOptions{})
		defer releaseQueryBudget(budget)
		budget.activeTx = tx
		budget.residentSources = true
		node, err := budget.readNode(tx.graph, people[0])
		if err != nil {
			return err
		}
		row := queryRow{
			slots: []boundValue{{Node: node, Bound: true}, {Bound: true}},
			index: map[string]int{"p": 0, "unused": 1},
		}
		root := budget.rowSources([]queryRow{row})
		defer root.close(budget)
		budget.escapeSource(row)
		truth, err := predicate.eval(row, nil, budget)
		if err != nil || truth != predicateTrue {
			t.Fatalf("unrelated NULL changed EXISTS: %v, %v", truth, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExistsStageRowCounterIsCumulative(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "exists-counter.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var person uint64
	if err := db.Update(func(tx *Tx) error {
		created, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		person = created.ID
		_, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	predicate, err := parseExistsPredicate(`EXISTS { MATCH (x:Item) }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		budget := newQueryBudget(t.Context(), QueryOptions{MaxRows: 1})
		defer releaseQueryBudget(budget)
		budget.activeTx = tx
		budget.residentSources = true
		if got, err := evalExistsTestRow(budget, tx, "p", person, predicate); err != nil || !got {
			t.Fatalf("first invocation = %v, %v", got, err)
		}
		if _, err := evalExistsTestRow(budget, tx, "p", person, predicate); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("second invocation = %v, want cumulative row-limit error", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExistsStopsBeforeUnneededCandidates(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "exists-tail.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"tag": int64(1)}}); err != nil {
			return err
		}
		for index := 0; index < 2_000; index++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"tag": int64(2)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	predicate, err := parseExistsPredicate(`EXISTS { MATCH (x:Item) WHERE x.tag = 1 }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 32})
		defer releaseQueryBudget(budget)
		budget.activeTx = tx
		budget.residentSources = true
		row := queryRow{slots: []boundValue{}, index: map[string]int{}}
		truth, err := predicate.eval(row, nil, budget)
		if err != nil || truth != predicateTrue {
			t.Fatalf("first candidate should satisfy EXISTS within work budget: %v, %v", truth, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExistsBudgetAndCancellationErrorsPropagate(t *testing.T) {
	db, people, _ := openExistsFixture(t, false)
	defer db.Close()
	predicate, err := parseExistsPredicate(`EXISTS { MATCH (p)-[:LINK]->(x:Item) }`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 1})
		budget.activeTx = tx
		budget.residentSources = true
		if got, err := evalExistsTestRow(budget, tx, "p", people[0], predicate); !errors.Is(err, ErrResourceLimit) || got {
			t.Fatalf("work limit = %v, %v", got, err)
		}
		releaseQueryBudget(budget)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		budget = newQueryBudget(ctx, QueryOptions{})
		defer releaseQueryBudget(budget)
		budget.activeTx = tx
		budget.residentSources = true
		if _, err := evalExistsTestRow(budget, tx, "p", people[0], predicate); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func openExistsFixture(t *testing.T, page bool) (*DB, []uint64, []uint64) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "exists.ltdb"), OpenOptions{Create: true, PageStorage: page})
	if err != nil {
		t.Fatal(err)
	}
	people := make([]uint64, 0, 3)
	items := make([]uint64, 0, 3)
	if err := db.Update(func(tx *Tx) error {
		for index, tag := range []int64{1, 2, 3} {
			person, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"name": string(rune('A' + index)), "tag": tag}})
			if err != nil {
				return err
			}
			people = append(people, person.ID)
		}
		for _, tag := range []any{int64(1), int64(2), nil} {
			item, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"tag": tag}})
			if err != nil {
				return err
			}
			items = append(items, item.ID)
		}
		if _, err := tx.CreateEdge(people[0], items[0], "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		for range 2 {
			if _, err := tx.CreateEdge(people[1], items[1], "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, people, items
}

func evalExistsTestRow(budget *queryBudget, tx *Tx, name string, nodeID uint64, predicate *existsPredicate) (bool, error) {
	row := queryRow{slots: make([]boundValue, 1), index: map[string]int{name: 0}}
	if nodeID != 0 {
		node, err := budget.readNode(tx.graph, nodeID)
		if err != nil {
			return false, err
		}
		row.set(name, boundValue{Node: node})
	} else {
		row.set(name, boundValue{})
	}
	root := budget.rowSources([]queryRow{row})
	defer root.close(budget)
	budget.escapeSource(row)
	truth, err := predicate.eval(row, nil, budget)
	return truth == predicateTrue, err
}
