# Compact entity properties

Nodes and edges store a sorted slice of typed entries internally. Small sets use
linear lookup; larger sets use binary search. Mutations clone the outer slice
before changing a published record. Nested values remain immutable internally
and are deep-cloned for the public map API.

Scalar values retain their existing boxed payloads so property reads do not
allocate. Packing those payloads further would require typed query evaluation.
Normalization, null/absent distinctions, index equality, canonical JSON, WAL
property deltas, and recovery remain compatible. No migration is required.

## Measurements

Apple M3, darwin/arm64, Go 1.27.1; medians of three runs. Baseline:
`a0f978b92a4817faeeb262feb5c51a4cd0fadb38`. CPU measurements ran after the test
processes finished. All graphs are at or below 100,000 nodes.

| Integrated workload | Baseline | Compact |
| --- | ---: | ---: |
| 10K repeated-string graph, retained heap | 10,901,032 B | 7,941,064 B |
| Same graph, open/close time | 236.0 ms | 231.4 ms |
| Same graph, allocated bytes | 357,971,944 B | 349,972,632 B |
| Same graph, allocations | 1,199,142 | 1,154,144 |
| Node lookup | 439.0 ns | 385.8 ns |
| Representative query | 782.8 ns | 761.6 ns |
| Representative query allocations | 937 B / 14 allocs | 937 B / 14 allocs |
| Single-record commit in 100K nodes | 45.5 µs | 36.1 µs |
| Same commit allocations | 12,024 B / 63 allocs | 11,617 B / 62 allocs |

The repeated-string graph has four properties per node and one per edge, with
5,000 edges. Retained heap decreased 27.2%. This measures live Go heap after GC;
it does not establish a peak RSS or cold-device improvement. Open/close timing
uses fresh handles with a warm filesystem cache. Short commit samples include
filesystem variability.

Container microbenchmarks share existing keys and payloads. Retained memory
includes 10,000 container descriptors and their backing storage:

| Properties/entity | Map retained B | Compact retained B | Map hit ns | Compact hit ns | Map clone+set ns | Compact clone+set ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,441,920 | 725,760 | 5.56 | 5.04 | 55.0 | 21.2 |
| 4 | 3,441,920 | 1,845,760 | 5.58 | 10.47 | 56.5 | 42.1 |
| 8 | 3,441,920 | 3,445,760 | 7.91 | 12.82 | 55.4 | 57.6 |
| 16 | 12,481,920 | 7,285,760 | 5.60 | 21.91 | 149.2 | 87.6 |
| 64 | 49,602,032 | 27,125,872 | 5.61 | 30.04 | 387.4 | 243.1 |

All isolated lookups allocate zero bytes. Eight-property containers have
essentially unchanged retained memory. Sorted construction and multi-property
lookups cost more CPU: constructing 64 entries takes 2,568 ns versus 1,200 ns
for a map; a missing lookup takes 28.9 ns versus 4.4 ns. The integrated query
and commit checks above passed with the memory reduction. Workloads dominated
by large property sets should measure this tradeoff.

## Reproduce

Run the integrated commands on both revisions. Run the new container benchmarks
on the candidate; they include both map and compact implementations.

```sh
go test . -run '^$' -bench '^BenchmarkReadRequests$/^(node_lookup|query)$' -benchtime=200ms -count=3 -benchmem
go test . -run '^$' -bench '^BenchmarkSingleRecordCommitScaling$/^nodes_100000$/^direct$' -benchtime=20x -count=3 -benchmem
go test ./internal/engine -run '^$' -bench '^BenchmarkRepeatedStringGraphRetained$/^nodes_10000$' -benchtime=1x -count=3 -benchmem
go test ./internal/store -run '^$' -bench '^BenchmarkPropertyContainer(Construct|Lookup|CloneSet)$' -benchtime=100ms -count=3 -benchmem
go test ./internal/store -run '^$' -bench '^BenchmarkPropertyContainerRetained$' -benchtime=1x -count=3 -benchmem
```

Coverage includes canonical wire equivalence, old-format recovery, normalization,
nested public-copy isolation, snapshot readers, property indexes, changefeeds,
vectors, and query mutation budgets. The full suite, race suite, and conformance
suite are required before publication.
