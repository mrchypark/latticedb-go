# Query search candidates

Query execution remains exact by default. `QueryOptions.ApproximateVector`
opts into the ANN candidate path for a narrow vector query shape; set
`VectorEfSearch` to control the ANN search breadth. An explicit, configured
`VectorNamespace` is required for an eligible ANN query.

```go
namespace := latticedb.VectorNamespace{
	Property: "embedding", Scope: "Article", Dimensions: 16,
	Metric: latticedb.VectorMetricL2,
}

result, err := db.QueryContext(ctx,
	`MATCH (n:Article) WHERE n.embedding <=> $vector RETURN id(n) AS id LIMIT 10`,
	map[string]latticedb.Value{"vector": queryVector},
	latticedb.QueryOptions{
		VectorNamespace:   &namespace,
		ApproximateVector: true,
		VectorEfSearch:    64,
	})
```

The ANN candidate path is eligible only for a single node pattern using the
configured namespace, a positive `LIMIT`, and no `SKIP`, joins, additional
filters, ordering, distinct projection, or mutation. For page-backed storage,
the namespace's persisted HNSW index must also be valid and the snapshot's
search-index generation must match the graph catalog. The page-backed ANN
search uses the persisted index directly; it does not require an in-memory
vector-node map. An ineligible query or stale/unbackfilled page snapshot uses
the exact path. A nil namespace keeps the legacy global vector behavior and is
not an ANN opt-in. `Tx.QueryContext` accepts the same options. The exact
default and query result semantics are unchanged.

`OpenOptions.FTSProperties` configures complete postings for the named,
top-level node-string properties for that open. It is separate from manual
`FTSIndex` calls. Include the property named by a query to configure its
posting candidate path:

```go
db, err := latticedb.Open(path, latticedb.OpenOptions{
	Create:       true,
	FTSProperties: []string{"body"},
})
```

Configured FTS postings preserve exact query results. On memory-backed stores,
the candidate path uses postings from `FTSProperties`. On page-backed stores,
it requires a current search-index generation and a complete, ready persistent
index. It can use a configured property index, or a declared node FTS index
whose property matches the predicate and whose label scope is guaranteed by
the pattern. A label-scoped index is not used when the pattern does not require
that label. The page index readiness marker alone is not enough: the persisted
search-index generation must also match the graph catalog. Read-only opens of
old or unbackfilled stores therefore use exact evaluation until a writable open
prepares the indexes.

FTS candidate planning is limited to a single-node read query with an invariant
FTS expression and the FTS predicate first. Earlier predicates keep the scan
path so their errors are not hidden. The candidate set is the complete union
of postings for the query terms. Normal predicate evaluation still checks each
candidate and computes its score; later filters and `LIMIT` are applied after
candidate generation. If postings cannot narrow the pattern, an eligible index
is unavailable, or the transaction has uncommitted changes, query execution
uses the exact scan so the result and error semantics remain unchanged. Query
candidate acceleration does not apply to edge patterns; they retain the safe
scan fallback.

The FTS property list is per-open configuration and should be provided again
when opening or deserializing a database if the optimization is wanted for
that handle. It configures top-level node-string properties; manual `FTSIndex`
calls and scoped FTS index definitions remain separate indexes. Empty or
unconfigured properties use exact evaluation.

The FTS path remains exact. The vector ANN path is approximate by request and
returns a bounded candidate set from the configured namespace. Its planner
restrictions keep extra filters from discarding results after that approximate
set is selected. Exact vector query behavior remains the default.

## Bounded measurements

The following measurements predate page-backed search and remain historical
evidence for the earlier resident implementation. They do not measure the
current page-backed implementation.

Candidate medians on darwin/arm64 (Apple M3), 10 iterations per sample and
three samples. FTS cases use identical 10K documents, with 100 matching
properties and a limit of 10. Vector cases use the same 10K-node fixture and
5K-node namespace, limit 10, and `VectorEfSearch: 64`.

| Query path | Time/op | Heap B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| FTS full scan | 7,866,192 ns | 7,053,218 | 100,090 |
| FTS property postings | 73,821 ns | 62,384 | 1,073 |
| Vector exact | 1,196,358 ns | 1,775,864 | 5,082 |
| Vector explicit ANN | 34,371 ns | 8,024 | 71 |

FTS results are checked against the full scan. ANN is approximate and these
measurements do not establish exact recall on arbitrary datasets. Index build
and update costs are outside these timed query loops. An unconfigured legacy
query retained 14 allocations/op and approximately 972 B/op at the parent
revision `de1716884adba74b7d5bd0ce93443ae63f319629` and this candidate.

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkQuery(FTS|Vector)Candidates10K$' -benchmem -benchtime=10x -count=3
```

Raw local outputs: `/tmp/latticedb-issue50-fts-benchmark.txt` and
`/tmp/latticedb-issue50-vector-benchmark.txt`.
