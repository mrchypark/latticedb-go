package engine

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkQueryLanguage measures warm execution, excluding fixture creation.
func BenchmarkQueryLanguage(b *testing.B) {
	for _, size := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("nodes_%d", size), func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "language"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := db.Close(); err != nil {
					b.Error(err)
				}
			})
			if err := db.Update(func(tx *Tx) error {
				for i := range size {
					if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": int64(i), "bucket": int64(i % 100), "name": "Sample"}}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			for _, tc := range []struct {
				name, query string
				rows        int
			}{
				{"count", `MATCH (n:Item) RETURN count(*) AS total`, 1},
				{"sum_avg", `MATCH (n:Item) RETURN sum(n.value) AS total, avg(n.value) AS mean`, 1},
				{"group_100", `MATCH (n:Item) RETURN n.bucket AS bucket, sum(n.value) AS total ORDER BY bucket`, 100},
				{"with_group_top10", `MATCH (n:Item) WITH n.bucket AS bucket, sum(n.value) AS total ORDER BY total DESC LIMIT 10 RETURN bucket, total`, 10},
				{"collect", `MATCH (n:Item) RETURN collect(n.value) AS values`, 1},
				{"nested_functions", `MATCH (n:Item) RETURN sum(size(toLower(n.name))) AS total`, 1},
			} {
				b.Run(tc.name, func(b *testing.B) {
					result, err := db.Query(tc.query, nil)
					if err != nil || len(result.Rows) != tc.rows {
						b.Fatalf("warm rows=%d err=%v", len(result.Rows), err)
					}
					switch tc.name {
					case "count":
						if result.Rows[0]["total"] != int64(size) {
							b.Fatal(result.Rows)
						}
					case "sum_avg":
						if result.Rows[0]["total"] != float64(size*(size-1)/2) || result.Rows[0]["mean"] != float64(size-1)/2 {
							b.Fatal(result.Rows)
						}
					case "collect":
						if len(result.Rows[0]["values"].([]any)) != size {
							b.Fatal("collect cardinality")
						}
					case "nested_functions":
						if result.Rows[0]["total"] != float64(size*6) {
							b.Fatal(result.Rows)
						}
					case "group_100", "with_group_top10":
						for i, row := range result.Rows {
							bucket := i
							if tc.name == "with_group_top10" {
								bucket = 99 - i
							}
							n := size / 100
							want := float64(n*bucket + 100*n*(n-1)/2)
							if row["bucket"] != int64(bucket) || row["total"] != want {
								b.Fatalf("row=%v bucket=%d total=%v", row, bucket, want)
							}
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						result, err := db.Query(tc.query, nil)
						if err != nil || len(result.Rows) != tc.rows {
							b.Fatalf("rows=%d err=%v", len(result.Rows), err)
						}
					}
				})
			}
		})
	}
}

// Exact search uses the same seeded, normalized 128-D fixture as the ANN benchmark.
func BenchmarkVectorSearchClusteredExact128D(b *testing.B) {
	for _, scale := range []struct {
		name string
		n    int
	}{{"1K", 1000}, {"10K", 10000}, {"100K", 100000}} {
		b.Run(scale.name, func(b *testing.B) {
			graph, queries := zigHarnessGraph(b, scale.n)
			db := &DB{graph: graph, enableVector: true, vectorDimensions: 128, queryCache: map[string]*queryPlan{}}
			for _, query := range queries[:10] {
				results, err := db.VectorSearch(query, VectorSearchOptions{K: 10, Exact: true})
				if err != nil || len(results) != 10 {
					b.Fatalf("results=%d err=%v", len(results), err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := db.VectorSearch(queries[i%len(queries)], VectorSearchOptions{K: 10, Exact: true}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
