package engine

import (
	"fmt"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func BenchmarkVectorSearchScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("records_%d", size), func(b *testing.B) {
			db := benchmarkSearchDB(size, true)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFTSSearchScaling(b *testing.B) {
	queries := []struct {
		name  string
		query string
		opts  FTSSearchOptions
	}{
		{name: "rare", query: "rare"},
		{name: "common", query: "common"},
		{name: "multi_rare", query: "rare absent"},
		{name: "fuzzy_rare", query: "rarf", opts: FTSSearchOptions{MaxDistance: 1, MinTermLength: 3}},
	}
	for _, size := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("records_%d", size), func(b *testing.B) {
			db := benchmarkSearchDB(size, false)
			for _, benchmark := range queries {
				b.Run(benchmark.name, func(b *testing.B) {
					b.ReportAllocs()
					benchmark.opts.MaxWork = ^uint64(0)
					for range b.N {
						if _, err := db.FTSSearch(benchmark.query, benchmark.opts); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkVectorSearchExactMillion(b *testing.B) {
	db := benchmarkSearchDB(1_000_000, false)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10, Exact: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVectorSearchMatched10K(b *testing.B) {
	db := benchmarkSearchDBWithDimensions(10_000, 128, true)
	query := make([]float32, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := db.VectorSearch(query, VectorSearchOptions{K: 10}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSearchDB(size int, indexed bool) *DB {
	return benchmarkSearchDBWithDimensions(size, 16, indexed)
}

func benchmarkSearchDBWithDimensions(size, dimensions int, indexed bool) *DB {
	graph := store.NewGraphState()
	graph.VectorDimensions = uint16(dimensions)
	for index := 1; index <= size; index++ {
		id := uint64(index)
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"embedding": make([]float32, dimensions)}})
		text := "common token"
		if index == 1 {
			text = "common rare token"
		}
		tokens := search.Tokenize(text)
		graph.FTS.Set(id, &store.FTSRecord{Text: text, Tokens: tokens})
		for _, token := range tokens {
			graph.FTSTokens.Add(token, id)
		}
	}
	if indexed {
		rebuildVectorIndex(graph)
	}
	return &DB{graph: graph, enableVector: true, vectorDimensions: uint16(dimensions), queryCache: map[string]*queryPlan{}}
}
