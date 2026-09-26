# Page-engine allocation baseline

This baseline separates the public disk engine from the previous resident-memory engine. It does not disable gates or change their thresholds. Only three public benchmarks use this file; internal memory graph, adjacency, and WAL allocation gates still use the previous main result.

## Provenance

- Candidate: `f3518b711ba8c0aa0d422aa44a2e6fe52c6bb747`.
- Source: [GitHub Actions run 36258284773](https://github.com/mrchypark/latticedb-go/actions/runs/36258284773/job/108449284361), completed benchmark measurements, failed old cross-engine gate.
- Environment: ubuntu-latest, Linux amd64, Go 1.27.1, GOMAXPROCS=2.
- Public benchmark commands: workflow `benchmark.yml`, `-benchmem -benchtime=100ms -count=3`.
- `baseline.txt` contains the nine original benchmark rows, not rounded limits or measurements of the candidate being checked. Comparison uses medians.

| Benchmark | B/op median | allocs/op median |
|---|---:|---:|
| Public query | 2440 | 47 |
| Public write commit | 88179 | 676 |
| 100K-node single-record commit | 88566 | 567 |

## Why the engines need separate comparisons

The old engine reads resident decoded records and updates an in-memory graph plus WAL. The page engine decodes owned records on demand and commits bbolt copy-on-write pages, allocator/catalog state, source history and stream changes atomically. Making the whole decoded graph resident again or removing durable writes would violate the page-engine goal.

A local allocation profile of the unoptimized write benchmark (Darwin arm64, Apple M3; not used as the Linux baseline) attributes 67.27% of measured-loop allocation bytes to bbolt inode creation, node insertion and metadata-page writes. bbolt v1.4.3 `Tx.writeMeta` allocates one physical page buffer per commit; its node update path materializes modified page inodes. These costs do not exist in the old resident graph path. Page size and operation counts differ by OS, so local measurements are not substituted for CI values.

Avoidable costs are optimized separately: unchanged label/type/adjacency postings need not be rewritten, and the page wrapper need not duplicate bbolt's key copy or recreate an existing bucket. Durability, history, read snapshots and corruption checks remain required.

The byte threshold remains +1%; query allocations allow no increase, write allocations allow +2. The baseline is fixed and versioned: CI must not regenerate it from the candidate or silently accept missing metrics. A future engine/toolchain change needs an explicit reviewed baseline update with provenance. Latency remains informational under the existing policy.
