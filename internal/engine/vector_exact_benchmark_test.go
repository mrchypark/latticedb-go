package engine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func BenchmarkExactSquaredScan(b *testing.B) {
	const dims = 8
	for _, size := range []int{10_000, 100_000} {
		for _, mode := range []string{"exact", "fallback"} {
			b.Run(fmt.Sprintf("records_%d/mode_%s/dims_%d", size, mode, dims), func(b *testing.B) {
				graph := store.NewGraphState()
				graph.VectorDimensions = uint16(dims)
				random := rand.New(rand.NewPCG(116, 32))
				for i := 1; i <= size; i++ {
					id := uint64(i)
					vec := make([]float32, dims)
					for d := range vec {
						vec[d] = random.Float32()*2 - 1
					}
					graph.Nodes.Set(id, &store.NodeRecord{
						ID:         id,
						Properties: store.PropertiesFromMap(map[string]any{"embedding": vec}),
					})
				}

				if mode == "fallback" {
					// Singleton HNSW fixture: VectorLiveCount = total ensures capacity K,
					// isolated single-node index forces exact fallback.
					graph.VectorLiveCount = uint64(size)
					entry := uint64(1)
					graph.VectorIndex = store.NewVectorIndex()
					graph.VectorIndex.EntryID = entry
					graph.VectorIndex.Nodes.Set(entry, &store.VectorIndexNode{
						Level:     0,
						Neighbors: [][]uint64{nil},
					})

				}

				query := make([]float32, dims)
				opts := VectorSearchOptions{K: 10, Exact: mode == "exact"}
				warmBudget, err := newDirectSearchBudget(context.Background(), ^uint64(0), ^uint64(0), 10)
				if err != nil {
					b.Fatal(err)
				}
				warm, fallback, err := searchVectorGraph(graph, query, opts, warmBudget, false)
				if err != nil || len(warm) != 10 || fallback != (mode == "fallback") {
					b.Fatalf("warmup: results=%d fallback=%v err=%v", len(warm), fallback, err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					budget, err := newDirectSearchBudget(context.Background(), ^uint64(0), ^uint64(0), 10)
					if err != nil {
						b.Fatal(err)
					}
					if _, _, err := searchVectorGraph(graph, query, opts, budget, false); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
