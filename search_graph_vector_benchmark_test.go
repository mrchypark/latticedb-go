package latticedb

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/benchvector"
)

func BenchmarkSearchGraphVectorRecall(b *testing.B) {
	vectors, queries := benchvector.Generate(512)
	db, err := Open(filepath.Join(b.TempDir(), "db"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: uint16(len(vectors[0])),
		VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		target, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Keep"}})
		if err != nil {
			return err
		}
		for i, vector := range vectors {
			source, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"embedding": vector}})
			if err != nil {
				return err
			}
			for range i % 3 {
				if _, err := tx.CreateEdge(source.ID, target.ID, "LINK", CreateEdgeOptions{}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	for _, expand := range []bool{false, true} {
		b.Run(fmt.Sprintf("expand=%v", expand), func(b *testing.B) {
			opts := SearchGraphOptions{Vector: &SearchGraphVector{EfSearch: 32}, CandidateK: 20, ResultK: 20}
			if expand {
				opts.Expand = &SearchGraphExpansion{EdgeType: "LINK", TargetLabel: "Keep"}
			}
			reference := make([]map[SearchGraphRow]struct{}, len(queries))
			for i, query := range queries {
				opts.Vector.Embedding = query
				exact, err := db.SearchGraphContext(b.Context(), opts)
				if err != nil || len(exact.Rows) == 0 {
					b.Fatalf("exact query %d: rows=%d err=%v", i, len(exact.Rows), err)
				}
				reference[i] = make(map[SearchGraphRow]struct{}, len(exact.Rows))
				for _, row := range exact.Rows {
					// Identity determines recall. Exact and approximate paths can round differently.
					row.Value = 0
					reference[i][row] = struct{}{}
				}
			}
			opts.Vector.Approximate = true
			b.ReportAllocs()
			b.ResetTimer()
			var matched, total, fallbacks int
			for i := 0; i < b.N; i++ {
				for j, query := range queries {
					opts.Vector.Embedding = query
					result, err := db.SearchGraphContext(b.Context(), opts)
					if err != nil {
						b.Fatal(err)
					}
					if result.Exact {
						fallbacks++
					}
					for _, row := range result.Rows {
						row.Value = 0
						if _, found := reference[j][row]; found {
							matched++
						}
					}
					total += len(reference[j])
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(matched)/float64(total), "recall")
			b.ReportMetric(float64(fallbacks)/float64(b.N*len(queries)), "fallback_ratio")
			b.ReportMetric(float64(len(queries)), "queries/op")
		})
	}
}
