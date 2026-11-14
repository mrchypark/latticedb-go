package latticedb

import (
	"path/filepath"
	"testing"
)

// BenchmarkGraphTraversalDisk measures warm Cypher traversal after disk reopen.
func BenchmarkGraphTraversalDisk(b *testing.B) {
	const nodes = 10000
	path := filepath.Join(b.TempDir(), "graph.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	var previous uint64
	for offset := 0; offset < nodes; offset += 1000 {
		if err := db.Update(func(tx *Tx) error {
			for i := 0; i < 1000; i++ {
				node, err := tx.CreateNode(CreateNodeOptions{})
				if err != nil {
					return err
				}
				if previous != 0 {
					if _, err := tx.CreateEdge(previous, node.ID, "NEXT", CreateEdgeOptions{}); err != nil {
						return err
					}
				}
				previous = node.ID
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	for _, tc := range []struct {
		name, query  string
		rows         int
		minID, maxID int64
	}{
		{"one_hop", `MATCH (a)-[:NEXT]->(b) RETURN id(b) AS id`, nodes - 1, 2, nodes},
		{"two_hop", `MATCH (a)-[:NEXT]->(b)-[:NEXT]->(c) RETURN id(c) AS id`, nodes - 2, 3, nodes},
		{"anchored_one_hop", `MATCH (a)-[:NEXT]->(b) WHERE id(a) = 5000 RETURN id(b) AS id`, 1, 5001, 5001},
		{"anchored_two_hop", `MATCH (a)-[:NEXT]->(b)-[:NEXT]->(c) WHERE id(a) = 5000 RETURN id(c) AS id`, 1, 5002, 5002},
		{"anchored_one_hop_ordered", `MATCH (a)-[:NEXT]->(b) WHERE id(a) = 5000 RETURN id(b) AS id ORDER BY id(a), id(b)`, 1, 5001, 5001},
		{"anchored_two_hop_ordered", `MATCH (a)-[:NEXT]->(b)-[:NEXT]->(c) WHERE id(a) = 5000 RETURN id(c) AS id ORDER BY id(a), id(b), id(c)`, 1, 5002, 5002},
	} {
		b.Run(tc.name, func(b *testing.B) {
			result, err := db.Query(tc.query, nil)
			if err != nil || len(result.Rows) != tc.rows {
				b.Fatalf("warm rows=%d want=%d err=%v", len(result.Rows), tc.rows, err)
			}
			seen := make(map[any]bool, len(result.Rows))
			for _, row := range result.Rows {
				id, ok := row["id"].(int64)
				if !ok || id < tc.minID || id > tc.maxID {
					b.Fatalf("unexpected id %v", row["id"])
				}
				if seen[id] {
					b.Fatalf("duplicate id %v", id)
				}
				seen[id] = true
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := db.Query(tc.query, nil)
				if err != nil || len(result.Rows) != tc.rows {
					b.Fatalf("rows=%d err=%v", len(result.Rows), err)
				}
			}
		})
	}
}
