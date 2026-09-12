package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// oracleExactScan independently computes the exact K-nearest-neighbors by
// scanning every node and using search.VectorDistance + pushVectorResult +
// compareVectorResult. This is the reference against which searchVectorGraph
// exact-path output is compared.
func oracleExactScan(graph *store.GraphState, query []float32, k int) []VectorSearchResult {
	if k <= 0 {
		k = 10
	}
	results := make([]VectorSearchResult, 0, k)
	for _, node := range graph.Nodes.All() {
		vector, ok := selectedVector(graph, node)
		if !ok {
			continue
		}
		dist, err := search.VectorDistance(vector, query)
		if err != nil {
			panic(err)
		}
		results = pushVectorResult(results, VectorSearchResult{NodeID: node.ID, Distance: dist}, k)
	}
	slices.SortFunc(results, compareVectorResult)
	return results
}

// buildExactGraph creates a small deterministic graph with 100 vector-bearing
// nodes (2-d, seeded RNG) and returns the graph plus a fixed query vector.
func buildExactGraph(t *testing.T) (*store.GraphState, []float32) {
	t.Helper()
	const dims = 2
	const count = 100
	rng := rand.New(rand.NewPCG(42, 7))
	graph := store.NewGraphState()
	graph.VectorDimensions = dims
	for i := range count {
		id := uint64(i + 1)
		v := []float32{rng.Float32(), rng.Float32()}
		graph.Nodes.Set(id, &store.NodeRecord{
			ID:         id,
			Properties: store.PropertiesFromMap(map[string]any{"embedding": v}),
		})
	}
	graph.VectorLiveCount = count
	query := []float32{0.5, 0.5}
	return graph, query
}

func assertExactOracle(t *testing.T, graph *store.GraphState, query []float32, opts VectorSearchOptions, disable, wantFallback bool) {
	t.Helper()
	budget, err := newDirectSearchBudget(context.Background(), ^uint64(0), ^uint64(0), opts.K)
	if err != nil {
		t.Fatal(err)
	}
	got, fallback, err := searchVectorGraph(graph, query, opts, budget, disable)
	if err != nil || fallback != wantFallback {
		t.Fatalf("fallback=%v err=%v", fallback, err)
	}
	want := oracleExactScan(graph, query, int(opts.K))
	if len(got) != len(want) {
		t.Fatalf("results=%d want=%d", len(got), len(want))
	}
	for i := range got {
		if got[i].NodeID != want[i].NodeID || math.Float32bits(got[i].Distance) != math.Float32bits(want[i].Distance) {
			t.Fatalf("result[%d]=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestExactSquaredKValues(t *testing.T) {
	graph, query := buildExactGraph(t)
	for _, k := range []uint32{0, 1, 2, 10, 64, 65, 1000} {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			assertExactOracle(t, graph, query, VectorSearchOptions{K: k, Exact: true}, false, false)
		})
	}
}

func TestExactSquaredRoundedTies(t *testing.T) {
	for _, scale := range []float32{1, math.SmallestNonzeroFloat32} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) {
			graph := store.NewGraphState()
			graph.VectorDimensions = 2
			second := float32(1e-4)
			if scale != 1 {
				second = scale
			}
			// ID 1 has greater squared distance, but both public distances round equally.
			vectors := [][]float32{{scale, second}, {scale, 0}, {-scale, 0}, {4 * scale, 0}}
			for i, vector := range vectors {
				id := uint64(i + 1)
				graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: store.PropertiesFromMap(map[string]any{"embedding": vector})})
			}
			for _, k := range []uint32{1, 2, 3, 4} {
				assertExactOracle(t, graph, []float32{0, 0}, VectorSearchOptions{K: k, Exact: true}, false, false)
			}
		})
	}
}

func TestExactSquaredMixedAndFallback(t *testing.T) {
	graph, query := buildExactGraph(t)
	for id := uint64(51); id <= 100; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: store.PropertiesFromMap(map[string]any{"name": "no vector"})})
	}
	graph.VectorLiveCount = 50
	assertExactOracle(t, graph, query, VectorSearchOptions{K: 10, Exact: true}, false, false)
	graph.VectorIndex = store.NewVectorIndex()
	graph.VectorIndex.EntryID = 1
	graph.VectorIndex.Nodes.Set(1, &store.VectorIndexNode{Level: 0, Neighbors: [][]uint64{nil}})
	assertExactOracle(t, graph, query, VectorSearchOptions{K: 10}, false, true)
	assertExactOracle(t, graph, query, VectorSearchOptions{K: 10}, true, false)
}

func TestExactSquaredValidationAndScratchBudget(t *testing.T) {
	graph, query := buildExactGraph(t)
	for _, invalid := range [][]float32{{1, 2, 3}, {float32(math.NaN()), 0}, {float32(math.Inf(1)), 0}} {
		budget, _ := newDirectSearchBudget(context.Background(), 10000, 10000, 1)
		if _, _, err := searchVectorGraph(graph, invalid, VectorSearchOptions{K: 1, Exact: true}, budget, false); err == nil {
			t.Fatal("invalid vector accepted")
		}
	}
	for _, maxBytes := range []uint64{16, 32} {
		budget, _ := newDirectSearchBudget(context.Background(), 10000, maxBytes, 1)
		_, _, err := searchVectorGraph(graph, query, VectorSearchOptions{K: 1, Exact: true}, budget, false)
		if (err == nil) != (maxBytes == 32) {
			t.Fatalf("budget %d: %v", maxBytes, err)
		}
	}
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: store.PropertiesFromMap(map[string]any{"embedding": []float32{math.MaxFloat32, 0}})})
	budget, _ := newDirectSearchBudget(context.Background(), 10000, 10000, 1)
	if _, _, err := searchVectorGraph(graph, []float32{-math.MaxFloat32, 0}, VectorSearchOptions{K: 1, Exact: true}, budget, false); err == nil {
		t.Fatal("L2 overflow accepted")
	}
}
