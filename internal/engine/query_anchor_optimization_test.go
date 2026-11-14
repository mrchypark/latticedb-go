package engine

import (
	"path/filepath"
	"testing"
)

func TestOrderedConnectedQueryWithinWorkLimitAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		var previous uint64
		for i := 0; i < 501; i++ {
			n, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			if previous != 0 {
				if _, err = tx.CreateEdge(previous, n.ID, "NEXT", CreateEdgeOptions{}); err != nil {
					return err
				}
			}
			previous = n.ID
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := db.QueryContext(t.Context(), `MATCH (a)-[:NEXT]->(b) WHERE id(a) = 250 RETURN id(b) AS id ORDER BY id(a), id(b)`, nil, QueryOptions{MaxWork: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(251) {
		t.Fatalf("unexpected rows: %v", result.Rows)
	}
}
