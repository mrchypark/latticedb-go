package latticedb_test

import (
	"context"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
)

func TestQueryReturnsPublicEntities(t *testing.T) {
	db, err := latticedb.Open(t.TempDir(), latticedb.OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var left, right latticedb.Node
	var edge latticedb.Edge
	err = db.Update(func(tx *latticedb.Tx) error {
		var err error
		left, err = tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}})
		if err != nil {
			return err
		}
		right, err = tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		edge, err = tx.CreateEdge(left.ID, right.ID, "KNOWS", latticedb.CreateEdgeOptions{Properties: map[string]any{"since": int64(2020)}})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	query := "MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a, e, b, a.name AS name"
	calls := map[string]func() (latticedb.QueryResult, error){
		"DB.Query": func() (latticedb.QueryResult, error) { return db.Query(query, nil) },
		"DB.QueryContext": func() (latticedb.QueryResult, error) {
			return db.QueryContext(context.Background(), query, nil, latticedb.QueryOptions{})
		},
		"Tx.Query": func() (latticedb.QueryResult, error) { return tx.Query(query, nil) },
		"Tx.QueryContext": func() (latticedb.QueryResult, error) {
			return tx.QueryContext(context.Background(), query, nil, latticedb.QueryOptions{})
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			result, err := call()
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("rows = %v", result.Rows)
			}
			row := result.Rows[0]
			a, ok := row["a"].(latticedb.Node)
			if !ok || a.ID != left.ID || a.Properties["name"] != "Alice" {
				t.Fatalf("node = %#v", row["a"])
			}
			b, ok := row["b"].(latticedb.Node)
			if !ok || b.ID != right.ID {
				t.Fatalf("target = %#v", row["b"])
			}
			e, ok := row["e"].(latticedb.Edge)
			if !ok || e.ID != edge.ID || e.SourceID != left.ID || e.TargetID != right.ID || e.Type != "KNOWS" || e.Properties["since"] != int64(2020) {
				t.Fatalf("edge = %#v", row["e"])
			}
			if row["name"] != "Alice" {
				t.Fatalf("scalar = %#v", row["name"])
			}
			a.Properties["name"] = "changed"
			e.Properties["since"] = int64(0)
		})
	}
}

func TestQueryReturnsNestedPublicEntities(t *testing.T) {
	db, err := latticedb.Open(t.TempDir(), latticedb.OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *latticedb.Tx) error {
		n, err := tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Nested"}, Properties: map[string]any{"name": "original"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(n.ID, n.ID, "SELF", latticedb.CreateEdgeOptions{Properties: map[string]any{"name": "original"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, query := range []string{
		"MATCH (n:Nested)-[e:SELF]->(n) RETURN collect(n) AS ns, collect(e) AS es",
		"MATCH (n:Nested)-[e:SELF]->(n) WITH collect(n) AS ns, collect(e) AS es RETURN collect(ns) AS ns, collect(es) AS es",
	} {
		calls := map[string]func() (latticedb.QueryResult, error){
			"DB": func() (latticedb.QueryResult, error) { return db.Query(query, nil) },
			"DBContext": func() (latticedb.QueryResult, error) {
				return db.QueryContext(context.Background(), query, nil, latticedb.QueryOptions{})
			},
			"Tx": func() (latticedb.QueryResult, error) { return tx.Query(query, nil) },
			"TxContext": func() (latticedb.QueryResult, error) {
				return tx.QueryContext(context.Background(), query, nil, latticedb.QueryOptions{})
			},
		}
		for name, call := range calls {
			t.Run(name+query, func(t *testing.T) {
				result, err := call()
				if err != nil {
					t.Fatal(err)
				}
				leaf := func(value any) any {
					for {
						list, ok := value.([]any)
						if !ok {
							return value
						}
						if len(list) != 1 {
							t.Fatalf("list=%v", list)
						}
						value = list[0]
					}
				}
				n, ok := leaf(result.Rows[0]["ns"]).(latticedb.Node)
				if !ok {
					t.Fatalf("node type=%T", leaf(result.Rows[0]["ns"]))
				}
				e, ok := leaf(result.Rows[0]["es"]).(latticedb.Edge)
				if !ok {
					t.Fatalf("edge type=%T", leaf(result.Rows[0]["es"]))
				}
				if n.Properties["name"] != "original" || e.Properties["name"] != "original" {
					t.Fatal("result mutation leaked")
				}
				n.Properties["name"] = "changed"
				e.Properties["name"] = "changed"
			})
		}
	}
}
