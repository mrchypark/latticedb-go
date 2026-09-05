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

func BenchmarkFTSFuzzyVocabularyPruning(b *testing.B) {
	for _, size := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("tokens_%d", size), func(b *testing.B) {
			graph := store.NewGraphState()
			for index := 0; index < size; index++ {
				token := fmt.Sprintf("vocabulary-token-%08d-suffix", index)
				id := uint64(index + 1)
				graph.FTS.Set(id, &store.FTSRecord{Text: token, Tokens: []string{token}})
				graph.FTSTokens.Add(token, id)
			}
			db := &DB{graph: graph, queryCache: map[string]*queryPlan{}}
			opts := FTSSearchOptions{Limit: 10, MaxDistance: 1, MinTermLength: 1, MaxWork: ^uint64(0)}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := db.FTSSearch("x", opts); err != nil {
					b.Fatal(err)
				}
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

func BenchmarkVectorSearchANNFallback10K(b *testing.B) {
	db := benchmarkSearchDB(10_000, true)
	entry := db.graph.VectorIndex.EntryID
	db.graph.VectorIndex = store.NewVectorIndex()
	db.graph.VectorIndex.EntryID = entry
	db.graph.VectorIndex.Nodes.Set(entry, &store.VectorIndexNode{Level: 0, Neighbors: [][]uint64{nil}})
	query := make([]float32, 16)
	before, err := db.VectorIndexStats()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := db.VectorSearch(query, VectorSearchOptions{K: 10}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	after, err := db.VectorIndexStats()
	if err != nil {
		b.Fatal(err)
	}
	if got := after.ExactFallbacks - before.ExactFallbacks; got != uint64(b.N) {
		b.Fatalf("exact fallback count delta = %d, want %d", got, b.N)
	}
}

func BenchmarkVectorSearchSparseComplete10K(b *testing.B) {
	graph := store.NewGraphState()
	graph.VectorDimensions = 16
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"embedding": make([]float32, 16)}})
	for id := uint64(2); id <= 10_000; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"name": "non-vector"}})
	}
	rebuildVectorIndex(graph)
	db := &DB{graph: graph, enableVector: true, vectorDimensions: 16, queryCache: map[string]*queryPlan{}}
	query := make([]float32, 16)
	before, err := db.VectorIndexStats()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := db.VectorSearch(query, VectorSearchOptions{K: 10}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	after, err := db.VectorIndexStats()
	if err != nil {
		b.Fatal(err)
	}
	if after.ExactFallbacks != before.ExactFallbacks {
		b.Fatalf("complete sparse ANN unexpectedly fell back %d times", after.ExactFallbacks-before.ExactFallbacks)
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
