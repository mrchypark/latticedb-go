# Vector namespaces

A vector namespace selects one vector property, one label scope, one dimension
count, and one metric for a derived index:

```go
namespace := latticedb.VectorNamespace{
	Property:   "embedding",
	Scope:      "Article",
	Dimensions: 16,
	Metric:     latticedb.VectorMetricL2,
}

db, err := latticedb.Open(path, latticedb.OpenOptions{
	Create:          true,
	EnableVector:    true,
	VectorDimensions: 16,
	VectorIndexMode: latticedb.VectorIndexHNSWSynchronous,
	VectorNamespaces: []latticedb.VectorNamespace{namespace},
})
if err != nil {
	return err
}

results, err := db.VectorSearch(query, latticedb.VectorSearchOptions{
	K:         10,
	EfSearch: 64,
	Namespace: &namespace,
})
```

`VectorMetricL2` is the only supported metric. `EnableVector` is required when
creating vector-enabled storage, but need not be resupplied when reopening a
database that is already vector-enabled. `VectorDimensions` is the stored
global dimension setting: a namespace descriptor with `Dimensions: 0`
normalizes to that global dimension, while a nonzero value must match it.
`VectorNamespaces` configures derived indexes for that open; namespace
definitions are not persisted in snapshots. Callers must resupply the same
descriptors when reopening, coordinated with the persistence work in #48.

The descriptor must match a configured property and label scope exactly,
including dimensions and metric. An explicit namespace still requires at most
one vector property per node. An empty `Scope` matches all nodes. An unknown
descriptor or a property/dimension mismatch returns an error even when the
matching scope has no rows. Direct search does not silently fall back to the
global index. A nil
`VectorSearchOptions.Namespace` preserves the legacy global behavior: one
vector property per node, the existing global dimensions, and the existing
global index.

Query vector comparisons use the same explicit descriptor through
`QueryOptions.VectorNamespace`; the comparison remains exact. Approximate
search is available through direct `VectorSearch` with an explicit namespace.
Use `RebuildVectorIndexNamespaceContext(ctx, namespace)` to rebuild one derived
index and `VectorIndexNamespaceStats(namespace)` to inspect it. Entry counts
and mutation debt describe the selected namespace; fallback, rebuild count,
and rebuild duration counters remain database-wide. The reported build estimate
is target-local; admission also includes retained sibling indexes.

The bounded benchmark uses exactly 10,000 nodes and 16-dimensional vectors.
The fixture has two vector properties and two label scopes: 5,000 nodes match
the `embedding_a`/`GroupA` namespace and 5,000 match the
`embedding_b`/`GroupB` namespace. The nil legacy cases use the 10,000-node
global pool. Each namespace and legacy case uses its own deterministic query;
pool size is reported with each result so these are separate exact-versus-ANN
measurements, not a speedup claim across different pools or queries.

Run the benchmark after the namespace implementation is stable:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkVectorNamespaceSearch10K$' -benchmem -benchtime=3x -count=3
```

Candidate medians from darwin/arm64, Apple M3:

| Case | Pool nodes | Time/op | Heap B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| legacy / exact | 10,000 | 765,472 ns | 1,093 | 5 |
| legacy / ANN | 10,000 | 48,208 ns | 6,728 | 15 |
| namespace A / exact | 5,000 | 303,542 ns | 1,141 | 6 |
| namespace A / ANN | 5,000 | 41,375 ns | 6,776 | 16 |
| namespace B / exact | 5,000 | 285,736 ns | 1,141 | 6 |
| namespace B / ANN | 5,000 | 51,806 ns | 10,626 | 17 |

These are separate measurements with different pool sizes and deterministic
queries, so the table does not claim a namespace or ANN speedup over another
row. The 10K legacy global pool and the two matched 5K namespace pools are
reported explicitly to make that distinction visible.

The baseline reference is `a80165757e57275ff91461d24b44bd89bea33cde`.
Namespace-specific comparisons require the new API; legacy nil behavior can
be compared with the pre-namespace vector benchmark at that reference.
The benchmark intentionally does not scale beyond 10K nodes.

Raw candidate output: `/tmp/latticedb-issue70-candidate-benchmark.txt`.
