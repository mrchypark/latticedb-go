# Zig v0.17.0 and Go v0.13.0

Measured 2026-11-14 on Apple M3, macOS arm64, Go 1.27.1 and Zig 0.16.0.
Go base: a4220c31dd02cdc2b017aa81d4d9bdd38f4acc78. Zig v0.17.0:
9995f484b24374ee8c96e7fbf1ef6a9fe0ec0196.

The public disk API benchmark creates vectors in batches of 1,000, closes and
reopens the database, verifies persisted vectors and runs 100 searches with
k=10 and ef=64. Recall uses ten queries. The 100K fixture SHA-256 is
ea39865747e8ddfbaf179e266420ea096acb6ac419efb030e5ae741f7344a4cc.
Go uses L2, Zig uses cosine on unit vectors. Their neighbor ordering is
comparable, but their distance values are different. Go cache is 64 MiB; Zig
uses its configured default buffer pool. This is one local observation, not an
isolated multi-run statistical comparison.

| Vectors | Engine | Build ms | Reopen ms | Mean ns | p99 ns | Recall@10 | File bytes |
|---:|---|---:|---:|---:|---:|---:|---:|
| 10,000 | Go | 12,375 | 577.8 | 1,257,922 | 1,927,917 | 99% | 59,015,168 |
| 10,000 | Zig | 4,957 | 11 | 153,852 | 829,416 | 99% | 26,370,048 |
| 100,000 | Go | 233,243 | 8,592 | 4,489,985 | 9,775,042 | 100% | 419,856,384 |
| 100,000 | Zig | 133,696 | 289 | 542,078 | 1,249,875 | 100% | 267,268,096 |

At 10K the Go and first Zig runs overlapped; that Zig run is excluded. The
reported Zig run followed Go. At 100K a preliminary Go run overlapped an
accidentally started test and was discarded. The reported Go run and the
subsequent Zig run were separate. Other machine activity was not controlled.

Go's benchmark now rejects duplicate or out-of-range IDs and non-finite
distances before computing recall. The 10K Go result predates this validation;
the 100K Go result uses it. These checks execute outside the timed search.

## Capability comparison

Zig v0.17 adds CASE, CALL subqueries, FOREACH, named and shortest paths,
comprehensions, RETURN *, RDF Turtle/N-Triples, range indexes, three vector
metrics and configurable changefeed retention. Go v0.13 has a narrower Cypher
subset, L2 namespaces and an automatic changefeed byte limit. Both have
OPTIONAL MATCH, EXISTS, UNION, full-text and vector search, with different
supported shapes. Go also exposes transactionally validated required/type/unique
constraints, query work/row/byte limits, and frozen-generation backup. This is
a source and documentation comparison, not an exhaustive compatibility claim.

The change in this work makes Go's toLower/toUpper use Unicode simple case
mapping; this changes results for non-ASCII strings. Zig documents its own
one-to-one mappings, so the sets of characters must not be assumed identical.

Reproduction: run BenchmarkVectorSearchDisk128D with
LATTICEDB_VECTOR_FIXTURE_DIR set to a new directory and GOMAXPROCS=2, then build
scripts/vector-disk-zig.zig with the pinned Zig source as in
.github/workflows/benchmark.yml. Run each implementation without another
benchmark or test on the machine.

## Follow-up: unsure and metadata lookups

The 10K build CPU profile sampled SquaredVectorDistance at 6.26 seconds of
13.49 seconds total. A Bloom filter in place of HNSW's exact visited set can
skip an unseen node on a false positive and change recall or graph topology.
No miss-heavy authoritative lookup was established as a safe Bloom-filter
target. No probabilistic dependency was added.

The page HNSW search now reads and decodes its metadata once instead of checking
presence and decoding it twice. The same snapshot, missing-index and corruption
errors, and underfill fallback remain in place. A 10K end-to-end run returned
99% recall and 7,840 allocations/search versus 7,860 in the previous run.
The follow-up run overlapped heavy unrelated machine activity (load average
above 300); its latency and build time cannot establish a speed improvement.
Repeat a paired comparison on an idle machine before claiming a speedup.

## Follow-up: distance kernel and search profile

On the same 10K disk workload, an ARM64 distance kernel and the existing
experimental SIMD implementation did not establish a repeatable end-to-end
build speedup. Neither was enabled by default. A search-only profile of 2,000
queries with the trial ARM64 kernel showed bbolt cursor search and record
decoding as significant costs; runtime memory scavenging also appeared in the
samples. This profile identifies investigation targets, not the share of
wall-clock query latency. The trial kernel was removed after measurement.
No 100K follow-up was run because the 10K speedup was not established.
