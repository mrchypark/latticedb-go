package engine

import (
	"path/filepath"
	"testing"
)

func BenchmarkQueryMultiHopSlots(b *testing.B) {
	const query = `MATCH (a)-[:NEXT]->(b)-[:NEXT]->(c) RETURN id(c) AS id`
	db, err := Open(filepath.Join(b.TempDir(), "query-slots-bench.ltdb"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		var previous uint64
		for i := 0; i < 100; i++ {
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
	b.ReportAllocs()
	if _, err := db.Query(query, nil); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Query(query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
