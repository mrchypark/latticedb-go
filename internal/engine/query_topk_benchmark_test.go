package engine

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkQueryOrderLimitTopK(b *testing.B) {
	for _, size := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("top_k_10/%d", size), func(b *testing.B) {
			db := benchmarkOrderDB(b, size)
			defer db.Close()
			query := `MATCH (n:Item) RETURN n.value AS value ORDER BY value LIMIT 10`
			if _, err := db.Query(query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Query(query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("full_sort_all_rows_reference/%d", size), func(b *testing.B) {
			db := benchmarkOrderDB(b, size)
			defer db.Close()
			query := `MATCH (n:Item) RETURN n.value AS value ORDER BY value`
			if _, err := db.Query(query, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Query(query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkOrderDB(b *testing.B, size int) *DB {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "query-topk-benchmark.ltdb"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		b.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for value := size - 1; value >= 0; value-- {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": int64(value)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return db
}
