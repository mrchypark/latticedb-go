package latticedb_test

import (
	"path/filepath"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
)

func TestEntityMapsPublicDBAndTransactionQueries(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name, path := "disk", filepath.Join(t.TempDir(), "db")
		if memory {
			name, path = "memory", ":memory:"
		}
		t.Run(name, func(t *testing.T) {
			db, err := latticedb.Open(path, latticedb.OpenOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query("CREATE (:Source {name:'source'})-[:Link]->(:Target)", nil); err != nil {
				t.Fatal(err)
			}
			query := "MATCH (n:Source)-[e:Link]->(m) WITH {node:n, edge:e, items:[n,e], nested:{node:n}} AS v RETURN v"
			check := func(result latticedb.QueryResult, err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				value := result.Rows[0]["v"].(map[string]any)
				if _, ok := value["node"].(latticedb.Node); !ok {
					t.Fatalf("node = %T", value["node"])
				}
				if _, ok := value["edge"].(latticedb.Edge); !ok {
					t.Fatalf("edge = %T", value["edge"])
				}
				items := value["items"].([]any)
				if _, ok := items[0].(latticedb.Node); !ok {
					t.Fatalf("item node = %T", items[0])
				}
				if _, ok := items[1].(latticedb.Edge); !ok {
					t.Fatalf("item edge = %T", items[1])
				}
				if _, ok := value["nested"].(map[string]any)["node"].(latticedb.Node); !ok {
					t.Fatal("nested node type")
				}
			}
			// The second DB call executes the cached parsed plan. There is no
			// separate public Prepare API in this version.
			check(db.Query(query, nil))
			check(db.Query(query, nil))
			if err := db.View(func(tx *latticedb.Tx) error { check(tx.Query(query, nil)); return nil }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
