package engine

import (
	"math"
	"math/bits"
	"slices"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// BenchmarkVectorSearchClustered128D uses the same workload as the upstream Zig
// benchmark without its storage/buffer-pool layer: 128-D normalized clustered vectors,
// M=16, M0=32, construction ef=200, search ef=64, and K=10.
// Select a scale with, for example, -bench='BenchmarkVectorSearchClustered128D/10K$'.
func BenchmarkVectorSearchClustered128D(b *testing.B) {
	for _, scale := range []struct {
		name string
		n    int
	}{
		{"1K", 1_000},
		{"10K", 10_000},
		{"100K", 100_000},
		{"1M", 1_000_000},
	} {
		b.Run(scale.name, func(b *testing.B) {
			graph, queries := zigHarnessGraph(b, scale.n)
			buildStarted := time.Now()
			db := zigHarnessIndexedDB(b, graph, scale.n)
			buildTime := time.Since(buildStarted)
			recall := zigHarnessRecallAt10(b, db, queries[:10])
			for _, query := range queries[:10] { // Match the Zig harness warmup.
				if _, err := db.VectorSearch(query, VectorSearchOptions{K: 10, EfSearch: 64}); err != nil {
					b.Fatal(err)
				}
			}
			mean, p99 := zigHarnessLatency(b, db, queries)

			b.ReportAllocs()
			b.SetBytes(128 * 4)
			b.ResetTimer()
			b.ReportMetric(float64(buildTime.Nanoseconds())/1e6, "index-build-ms")
			b.ReportMetric(float64(mean.Nanoseconds()), "mean-ns")
			b.ReportMetric(recall*100, "recall@10")
			b.ReportMetric(float64(p99.Nanoseconds()), "p99-ns")
			for i := 0; i < b.N; i++ {
				if _, err := db.VectorSearch(queries[i%len(queries)], VectorSearchOptions{K: 10, EfSearch: 64}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func zigHarnessLatency(tb testing.TB, db *DB, queries [][]float32) (time.Duration, time.Duration) {
	tb.Helper()
	timings := make([]time.Duration, len(queries))
	var total time.Duration
	for index, query := range queries {
		started := time.Now()
		if _, err := db.VectorSearch(query, VectorSearchOptions{K: 10, EfSearch: 64}); err != nil {
			tb.Fatal(err)
		}
		timings[index] = time.Since(started)
		total += timings[index]
	}
	slices.Sort(timings)
	return total / time.Duration(len(timings)), timings[(len(timings)-1)*99/100]
}

func TestVectorIndexClusteredRecall(t *testing.T) {
	db, queries := zigHarnessDB(t, 1_000)
	if recall := zigHarnessRecallAt10(t, db, queries[:10]); recall < 0.99 {
		t.Fatalf("recall@10 = %.0f%%, want at least 99%%", recall*100)
	}
	results, err := db.VectorSearch(queries[0], VectorSearchOptions{K: 10, EfSearch: 64})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		vector, ok := vectorForNode(db.graph, result.NodeID)
		if !ok {
			t.Fatalf("result node %d has no vector", result.NodeID)
		}
		want, err := search.VectorDistance(queries[0], vector)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(float64(result.Distance-want)) > 1e-6 {
			t.Fatalf("node %d distance = %g, want %g", result.NodeID, result.Distance, want)
		}
	}
}

func BenchmarkVectorIndexBuildClustered128D(b *testing.B) {
	template, _ := zigHarnessGraph(b, 1_000)
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		graph := store.NewGraphState()
		graph.VectorDimensions = 128
		for id, node := range template.Nodes.All() {
			graph.Nodes.Set(id, node)
		}
		scratch := &vectorSearchScratch{visited: make(map[uint64]struct{}, vectorIndexConstructionEF*vectorIndexM)}
		b.StartTimer()
		for id := uint64(1); id <= 1_000; id++ {
			if err := insertVectorIndexMode(graph, id, true, scratch); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func zigHarnessDB(tb testing.TB, count int) (*DB, [][]float32) {
	tb.Helper()
	graph, queries := zigHarnessGraph(tb, count)
	return zigHarnessIndexedDB(tb, graph, count), queries
}

func zigHarnessIndexedDB(tb testing.TB, graph *store.GraphState, count int) *DB {
	tb.Helper()
	// The production builder uses this mutable path while assembling one fresh index.
	scratch := &vectorSearchScratch{visited: make(map[uint64]struct{}, vectorIndexConstructionEF*vectorIndexM)}
	for id := uint64(1); id <= uint64(count); id++ {
		if err := insertVectorIndexMode(graph, id, true, scratch); err != nil {
			tb.Fatal(err)
		}
	}
	return &DB{graph: graph, enableVector: true, vectorDimensions: 128, queryCache: map[string]*queryPlan{}}
}

func zigHarnessGraph(tb testing.TB, count int) (*store.GraphState, [][]float32) {
	tb.Helper()
	graph := store.NewGraphState()
	graph.VectorDimensions = 128
	rng := newZigHarnessRNG(42)
	clusters := max(count/1_000, 10)
	centers := make([]float32, clusters*128)
	for cluster := range clusters {
		zigHarnessUnitVector(centers[cluster*128:(cluster+1)*128], &rng)
	}
	for i := 0; i < count; i++ {
		vector := make([]float32, 128)
		center := centers[(i%clusters)*128 : (i%clusters+1)*128]
		for dimension := range vector {
			vector[dimension] = center[dimension] + zigHarnessGaussian(&rng)*0.1
		}
		zigHarnessNormalize(vector)
		id := uint64(i + 1)
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"embedding": vector}})
	}

	queries := make([][]float32, 100)
	for i := range queries {
		baseID := rng.lessThan(uint64(count)) + 1
		base := graph.Nodes.Get(baseID).Properties["embedding"].([]float32)
		query := make([]float32, 128)
		copy(query, base)
		for dimension := range query {
			query[dimension] += zigHarnessGaussian(&rng) * 0.05
		}
		zigHarnessNormalize(query)
		queries[i] = query
	}
	return graph, queries
}

func zigHarnessRecallAt10(tb testing.TB, db *DB, queries [][]float32) float64 {
	tb.Helper()
	hits := 0
	for _, query := range queries {
		exact, err := db.VectorSearch(query, VectorSearchOptions{K: 10, Exact: true})
		if err != nil {
			tb.Fatal(err)
		}
		approximate, err := db.VectorSearch(query, VectorSearchOptions{K: 10, EfSearch: 64})
		if err != nil {
			tb.Fatal(err)
		}
		if len(exact) != 10 || len(approximate) != 10 {
			tb.Fatalf("recall@10 result length = %d/%d", len(approximate), len(exact))
		}
		expected := make(map[uint64]struct{}, len(exact))
		for _, result := range exact {
			expected[result.NodeID] = struct{}{}
		}
		for _, result := range approximate {
			if _, ok := expected[result.NodeID]; ok {
				hits++
			}
		}
	}
	return float64(hits) / float64(len(queries)*10)
}

// zigHarnessRNG matches Zig 0.16 std.Random.DefaultPrng (Xoshiro256++).
type zigHarnessRNG struct{ state [4]uint64 }

func TestZigHarnessRNGMatchesZig016(t *testing.T) {
	rng := newZigHarnessRNG(0)
	for i, want := range [...]uint64{
		0x53175d61490b23df,
		0x61da6f3dc380d507,
		0x5c0fdf91ec9a7bfc,
		0x02eebf8c3bbe5e1a,
	} {
		if got := rng.next(); got != want {
			t.Fatalf("value %d = %#x, want %#x", i, got, want)
		}
	}
}

func newZigHarnessRNG(seed uint64) zigHarnessRNG {
	var rng zigHarnessRNG
	for i := range rng.state {
		seed += 0x9e3779b97f4a7c15
		value := seed
		value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
		value = (value ^ value>>27) * 0x94d049bb133111eb
		rng.state[i] = value ^ value>>31
	}
	return rng
}

func (r *zigHarnessRNG) next() uint64 {
	result := bits.RotateLeft64(r.state[0]+r.state[3], 23) + r.state[0]
	t := r.state[1] << 17
	r.state[2] ^= r.state[0]
	r.state[3] ^= r.state[1]
	r.state[1] ^= r.state[2]
	r.state[0] ^= r.state[3]
	r.state[2] ^= t
	r.state[3] = bits.RotateLeft64(r.state[3], 45)
	return result
}

func (r *zigHarnessRNG) unit() float32 {
	random := r.next()
	leading := bits.LeadingZeros64(random)
	if leading >= 41 {
		leading = 41 + bits.LeadingZeros64(r.next())
		if leading == 105 {
			leading += bits.LeadingZeros32(uint32(r.next()) | 0x7ff)
		}
	}
	mantissa := uint32(random) & (1<<23 - 1)
	exponent := uint32(126-leading) << 23
	return math.Float32frombits(exponent|mantissa)*2 - 1
}

func (r *zigHarnessRNG) lessThan(limit uint64) uint64 {
	threshold := -limit % limit
	for {
		high, low := bits.Mul64(r.next(), limit)
		if low >= threshold {
			return high
		}
	}
}

func zigHarnessGaussian(rng *zigHarnessRNG) float32 {
	for {
		u, v := rng.unit(), rng.unit()
		s := u*u + v*v
		if s > 0 && s < 1 {
			return u * float32(math.Sqrt(-2*math.Log(float64(s))/float64(s)))
		}
	}
}

func zigHarnessUnitVector(vector []float32, rng *zigHarnessRNG) {
	for i := range vector {
		vector[i] = rng.unit()
	}
	zigHarnessNormalize(vector)
}

func zigHarnessNormalize(vector []float32) {
	var norm float32
	for _, value := range vector {
		norm += value * value
	}
	norm = float32(math.Sqrt(float64(norm)))
	for i := range vector {
		vector[i] /= norm
	}
}
