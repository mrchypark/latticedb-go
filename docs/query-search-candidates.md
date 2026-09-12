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
filters, ordering, distinct projection, or mutation. An ineligible query uses
the exact path. A nil namespace keeps the legacy global vector behavior and is
not an ANN opt-in. `Tx.QueryContext` accepts the same options. The exact
default and query result semantics are unchanged.

`OpenOptions.FTSProperties` configures complete postings for the named,
top-level node-string properties for that open. It is separate from manual
`FTSIndex` calls and must include the property named by a query to enable the
posting candidate path:

```go
db, err := latticedb.Open(path, latticedb.OpenOptions{
	Create:       true,
	FTSProperties: []string{"body"},
})
```

Configured FTS postings preserve exact query results. The candidate path uses
a single-node read query with the FTS predicate first; earlier predicates retain
the scan path so their errors are not hidden. It also keeps the label scan when
that is narrower than the postings union. A dirty transaction uses
the full exact fallback so uncommitted values remain visible with the same
semantics. The FTS property list is per-open configuration and should be
provided again when opening or deserializing a database if the optimization is
wanted for that handle. Empty or unconfigured properties use exact evaluation.

## Bounded measurements

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
