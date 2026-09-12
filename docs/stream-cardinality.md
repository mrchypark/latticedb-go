# Stream first-write cost

`StreamStore.Fork` shares immutable maps. The first publication copies the stream
and next-sequence maps; the first offset update copies the outer offset map and
the selected stream's consumer map. Retained record count alone therefore does
not predict the cost of a small transaction.

Measured on 2026-09-10 with Go 1.27.1, darwin/arm64, Apple M3, against
`02dcf35` plus `BenchmarkStreamFirstWriteCardinality`:

```sh
go test ./internal/store -run '^$' -bench '^BenchmarkStreamFirstWriteCardinality$' -benchmem -benchtime=100ms -count=3
```

Each operation forks the same base and performs one mutation. All cases retain
zero records before the mutation; the publication cases create then trim one
record per stream during untimed setup. Consumer cases vary consumers in one
stream; offset-stream cases vary streams with one consumer each.

| First mutation | Cardinality | Median ns/op | Median B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Publish | 1 | 471.9 | 1,152 | 8 |
| Publish | 100 | 1,191 | 12,192 | 12 |
| Publish | 10,000 | 207,069 | 1,486,547 | 72 |
| Offset, consumers | 1 | 249.8 | 768 | 6 |
| Offset, consumers | 100 | 495.6 | 4,056 | 8 |
| Offset, consumers | 10,000 | 55,113 | 437,425 | 38 |
| Offset, streams | 1 | 237.3 | 768 | 6 |
| Offset, streams | 100 | 528.1 | 4,056 | 8 |
| Offset, streams | 10,000 | 64,663 | 437,424 | 38 |

These short local runs quantify allocation growth, not a throughput guarantee;
other development work was running concurrently. Keep stream and consumer
cardinality small where per-transaction allocation matters. Batch mutations in
one transaction to amortize each map's first copy. Graph transactions that emit
the changefeed also pay the stream-map copy cost. A 10,000-stream publication
allocates about 1.49 MB even with no retained records, so high-cardinality,
frequent small writes warrant a persistent/sharded-map follow-up measured with
the application's workload. This measurement does not change the representation
or establish a universal supported-cardinality limit.
