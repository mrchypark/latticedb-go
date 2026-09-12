package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// Both cases use the same 10K documents and query; only the per-open index differs.
func BenchmarkQueryFTSCandidates10K(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		name := "scan"
		if indexed {
			name = "postings"
		}
		b.Run(name, func(b *testing.B) {
			opts := OpenOptions{Create: true}
			if indexed {
				opts.FTSProperties = []string{"text"}
			}
			db, err := Open(filepath.Join(b.TempDir(), "fts"), opts)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := db.Close(); err != nil {
					b.Error(err)
				}
			})
			if err := db.Update(func(tx *Tx) error {
				for i := 0; i < 10_000; i++ {
					value := "ordinary document with common words"
					if i%100 == 0 {
						value = "rareterm document with common words"
					}
					if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}, Properties: map[string]any{"text": value}}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			const query = "MATCH (n:Document) WHERE n.text @@ $q RETURN id(n) AS id LIMIT 10"
			params := map[string]any{"q": "rareterm"}
			result, err := db.QueryContext(context.Background(), query, params, QueryOptions{})
			if err != nil || len(result.Rows) != 10 {
				b.Fatalf("warm query rows=%d err=%v", len(result.Rows), err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := db.QueryContext(context.Background(), query, params, QueryOptions{})
				if err != nil || len(result.Rows) != 10 {
					b.Fatalf("query rows=%d err=%v", len(result.Rows), err)
				}
			}
		})
	}
}

func BenchmarkQueryVectorCandidates10K(b *testing.B) {
	db, queries := openVectorNamespaceBenchmarkDB(b)
	for _, approximate := range []bool{false, true} {
		name := "exact"
		if approximate {
			name = "ann"
		}
		b.Run(name, func(b *testing.B) {
			opts := QueryOptions{VectorNamespace: &vectorNamespaceBenchmarkA, ApproximateVector: approximate, VectorEfSearch: 64}
			const query = "MATCH (n:GroupA) WHERE n.embedding_a <=> $q RETURN id(n) AS id LIMIT 10"
			params := map[string]any{"q": queries.a}
			result, err := db.QueryContext(context.Background(), query, params, opts)
			if err != nil || len(result.Rows) != 10 {
				b.Fatalf("warm query rows=%d error=%v", len(result.Rows), err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := db.QueryContext(context.Background(), query, params, opts)
				if err != nil || len(result.Rows) != 10 {
					b.Fatalf("query rows=%d error=%v", len(result.Rows), err)
				}
			}
		})
	}
}
