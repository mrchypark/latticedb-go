package engine

import (
	"math"
	"path/filepath"
	"testing"
)

var vectorNamespaceBenchmarkA = VectorNamespace{Property: "embedding_a", Scope: "GroupA", Dimensions: 16, Metric: VectorMetricL2}
var vectorNamespaceBenchmarkB = VectorNamespace{Property: "embedding_b", Scope: "GroupB", Dimensions: 16, Metric: VectorMetricL2}

func BenchmarkVectorNamespaceSearch10K(b *testing.B) {
	db, queries := openVectorNamespaceBenchmarkDB(b)
	for _, test := range []struct {
		name      string
		namespace *VectorNamespace
		query     []float32
		exact     bool
		pool      uint64
	}{
		{name: "legacy/exact", query: queries.legacy, exact: true, pool: 10_000},
		{name: "legacy/ann", query: queries.legacy, pool: 10_000},
		{name: "namespace_a/exact", namespace: &vectorNamespaceBenchmarkA, query: queries.a, exact: true, pool: 5_000},
		{name: "namespace_a/ann", namespace: &vectorNamespaceBenchmarkA, query: queries.a, pool: 5_000},
		{name: "namespace_b/exact", namespace: &vectorNamespaceBenchmarkB, query: queries.b, exact: true, pool: 5_000},
		{name: "namespace_b/ann", namespace: &vectorNamespaceBenchmarkB, query: queries.b, pool: 5_000},
	} {
		b.Run(test.name, func(b *testing.B) {
			options := VectorSearchOptions{K: 10, EfSearch: 64, Exact: test.exact, Namespace: test.namespace}
			b.ResetTimer()
			b.ReportAllocs()
			b.ReportMetric(float64(test.pool), "pool-nodes/op")
			for range b.N {
				if _, err := db.VectorSearch(test.query, options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type vectorNamespaceBenchmarkQueries struct {
	legacy []float32
	a      []float32
	b      []float32
}

func openVectorNamespaceBenchmarkDB(b *testing.B) (*DB, vectorNamespaceBenchmarkQueries) {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "vector-namespaces"), OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 16,
		VectorIndexMode:  VectorIndexHNSWSynchronous,
		VectorNamespaces: []VectorNamespace{
			vectorNamespaceBenchmarkA,
			vectorNamespaceBenchmarkB,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := db.Update(func(tx *Tx) error {
		for id := uint64(1); id <= 10_000; id++ {
			groupA := id <= 5_000
			properties := map[string]any{"group": groupA}
			labels := []string{"GroupB"}
			if groupA {
				properties["embedding_a"] = vectorNamespaceBenchmarkVector(id, true)
				labels = []string{"GroupA"}
			} else {
				properties["embedding_b"] = vectorNamespaceBenchmarkVector(id, false)
			}
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: labels, Properties: properties}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	return db, vectorNamespaceBenchmarkQueries{
		legacy: vectorNamespaceBenchmarkVector(1, true),
		a:      vectorNamespaceBenchmarkVector(1, true),
		b:      vectorNamespaceBenchmarkVector(5_001, false),
	}
}

func vectorNamespaceBenchmarkVector(id uint64, groupA bool) []float32 {
	vector := make([]float32, 16)
	phase := float64(id%97) * 0.01
	if groupA {
		vector[0] = 1
		vector[1] = float32(math.Sin(phase))
	} else {
		vector[1] = 1
		vector[0] = float32(math.Cos(phase))
	}
	for dimension := 2; dimension < len(vector); dimension++ {
		vector[dimension] = float32((id*uint64(dimension+3))%101) / 101
	}
	return vector
}
