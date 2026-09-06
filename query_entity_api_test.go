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
