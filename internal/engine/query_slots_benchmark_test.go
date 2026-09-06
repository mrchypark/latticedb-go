package engine

import (
	"path/filepath"
	"testing"
)

// Keep the 100-node benchmark name stable for the historical performance gate.
func BenchmarkQueryMultiHopSlots(b *testing.B) {
	benchmarkQueryMultiHopSlots(b, 100)
}

func BenchmarkQueryMultiHopSlots100K(b *testing.B) {
	benchmarkQueryMultiHopSlots(b, 100_000)
}

func benchmarkQueryMultiHopSlots(b *testing.B, nodes int) {
	const query = `MATCH (a)-[:NEXT]->(b)-[:NEXT]->(c) RETURN id(c) AS id`
	db, err := Open(filepath.Join(b.TempDir(), "query-slots-bench.ltdb"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		var previous uint64
		for range nodes {
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
	for range b.N {
		if _, err := db.Query(query, nil); err != nil {
			b.Fatal(err)
		}
	}
}
