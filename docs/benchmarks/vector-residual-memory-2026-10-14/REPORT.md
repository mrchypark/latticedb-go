# Remaining Go/Zig memory difference

This follow-up separates live objects, reusable storage buffers, runtime spare heap, and resident database mappings. It does not treat maximum RSS as an allocation census or assume the remaining difference is an inherent Go language limit.

## Controlled workload

The same persistent public API workload as [the previous CPU/memory report](../vector-memory-2026-10-14/REPORT.md): Apple M3/macOS26.3, Go1.27.1, GOMAXPROCS=2, scalar code, PGO off, 128 dimensions, M16, efConstruction200, efSearch64, K10, synchronous batches of 1,000, close/reopen verification, 100 timed queries and recall against exact L2. No benchmark runs overlap tests, compilation, or other benchmark processes. The host is shared and the OS cache is warm.

At 100K each diagnostic candidate logs batch memory and writes a pre-close heap profile after one explicit GC. This GC is diagnostic-only, included in reported build time, and never enabled by library code. One GC may retain sync.Pool's victim cache; this is deliberately not described as the minimum possible application live set. Profile byte totals are sampled estimates. The 16KiB baseline also has two external vmmap snapshots. Small timing differences from single 100K runs are not statistical proof.

## What remains in memory

* The baseline 100K post-GC profile attributes about 153.37MiB to bbolt's page pool and 47.84MiB to the benchmark input fixture. The entire sampled profile is 202.22MiB. The prior pre-GC profile's decoded HNSW records were largely transaction-scoped garbage, not a permanent index-wide resident graph.
* Exact runtime counters at this diagnostic point report HeapAlloc 230,713,976 bytes, HeapInuse 237,264,896, HeapSys 603,619,328, HeapIdle 366,354,432 and HeapReleased 190,636,032. Idle address space is not equivalent to RSS; some has already been released to the OS.
* Around 78K vectors, vmmap reports 421.6MiB of resident mapped-file pages and 364.6MiB physical footprint. These are a phase snapshot, not additive estimates of the final peak. The early snapshot is around 15K and must not be compared as if it sampled the same phase.
* bbolt 1.5.0 allocates one-page write buffers through sync.Pool (`db.go`, Open and allocate), holds dirty pages through commit, and returns them to the pool after writing and syncing (`tx.go`, write). Go's pool cleanup moves primary entries to a victim cache for a subsequent GC cycle. It is reusable memory, not a demonstrated leak.
* Zig's configured 64MiB buffer pool bounds storage page frames. Go's configured 64MiB record cache covers copied read records; it does not cover bbolt write buffers, the separate decoded HNSW construction cache, runtime headroom, or mapped database pages. Equal settings therefore do not mean equal memory budgets.

Zig's October13 100K reference is maximum RSS 127.88MiB, peak physical footprint about 132.52MiB and database size 416,518,144 bytes. It is historical, not a fresh paired run. Native graph randomness and native L2/cosine arithmetic differences remain as documented in the previous report. The Go/Zig gap combines runtime behavior and storage architecture; these measurements do not establish a language-imposed lower bound.

## Page-size experiments

The original benchmark uses PageSize=0. On this host, bbolt's native default is 16,384 bytes; Zig's native default is 4,096 bytes. Existing public PageSize options were sufficient for experiments. No page format, sync mode, commit batch, degree, recall target, or read validation was relaxed.

| 10K, three runs, median | Native16KiB | 4KiB | 8KiB |
|---|---:|---:|---:|
| Build | 11.113s | 11.338s | 11.434s |
| Process user + system CPU | 12.22s | 12.34s | 12.46s |
| Maximum RSS | 134.39MiB | 137.50MiB | 128.48MiB |
| Peak physical footprint | 82.41MiB | 88.86MiB | 89.97MiB |
| Database bytes | 83,378,176 | 99,463,168 | 85,549,056 |
| Recall@10 | 99% | 99% | 99% |

Native16KiB and 8KiB were alternated; 4KiB's three runs preceded them. RSS and physical footprint do not move together. The 8KiB option has no reliable memory-and-CPU win and was not promoted to a default or extended to 100K.

| 100K, one diagnostic run | Native16KiB | 4KiB |
|---|---:|---:|
| Build | 248.450s | 253.853s |
| Process user + system CPU | 260.22s | 264.66s |
| Maximum RSS | 1,109.81MiB | 1,024.17MiB |
| Peak physical footprint | 593.45MiB | 482.39MiB |
| Database bytes | 662,355,968 | 602,365,952 |
| Recall@10 | 100% | 100% |

The 4KiB post-GC sampled profile attributes 113.44MiB to bbolt’s page pool. The run has lower peak footprint, but uses 1.7% more CPU in this single diagnostic sample, including system time rising from 6.15s to 9.61s. Its 10K database grows by 19%, whereas its 100K database is smaller; file-size effects depend on dataset size and insertion shape. The experiment is not adopted: it does not establish the requested memory reduction without a CPU cost. Existing databases and creation defaults retain their prior behavior.

## GC experiments

A 10K GOGC=50 probe uses 13.26s CPU and maximum RSS 115.55MiB. Compared with the nearby default-GC median, it reduces memory at a CPU cost. No process-wide GC setting is added to the library.

The existing Go runtime setting `GOMEMLIMIT=512MiB` is the successful candidate for this measured process. It changes runtime pacing/scavenging, not the database transaction, storage format or search parameters. It is an application-level soft runtime limit, not an RSS cap; bbolt's mmap lies outside it. The library must not silently change the embedding application's process-wide GC policy.

| 100K scalar diagnostic | Default runtime | GOMEMLIMIT=512MiB |
|---|---:|---:|
| Build | 248.450s | 243.298s |
| Process user + system CPU | 260.22s | 257.31s |
| Maximum RSS | 1,109.81MiB | 1,008.63MiB |
| Peak physical footprint | 593.45MiB | 491.22MiB |
| Mean / P99 query | 2.125 / 2.487ms | 2.133 / 2.448ms |
| Reopen | 8.786s | 8.740s |
| Recall@10 | 100% | 100% |
| Database bytes | 662,355,968 | 662,355,968 |

This sample reduces maximum RSS by 9.1% and peak physical footprint by 17.2%, with no observed CPU regression. Instructions retired are essentially unchanged (3.966 versus 3.968 trillion), so the small timing difference should not be sold as a CPU optimization. Post-GC HeapAlloc is essentially identical at 230.7MB, while HeapSys falls from 603.6MB to 498.8MB. This is reduced runtime headroom, not less logical data or a removed leak.

Do not generalize 512MiB to arbitrary dimensions, concurrency, retained read snapshots or transaction sizes. Lowering a soft limit near the workload's live set can increase GC cost. This run still has over 1,000MiB maximum RSS because the setting does not include mapped storage.

The previously built SIMD+PGO binary, with the same production source and training profile as the preceding report, also completes under this limit without a CPU regression in the observed run:

| 100K SIMD+PGO | Previous default-runtime run | 512MiB confirmation |
|---|---:|---:|
| Build | 252.988s | 227.261s |
| Process user + system CPU | 257.25s | 241.28s |
| Maximum RSS | 1,069.20MiB | 996.94MiB |
| Peak physical footprint | 559.39MiB | 478.08MiB |
| Mean / P99 query | 2.289 / 4.160ms | 2.151 / 2.696ms |
| Reopen | 9.035s | 8.811s |
| Recall@10 | 100% | 100% |
| Database bytes | 662,355,968 | 662,355,968 |

Both fast runs omit continuous profiling, heap snapshots and memory telemetry; they retain progress logging. This is a historical comparison of one run per configuration, not a repeated paired performance experiment. The lower wall/CPU time cannot be attributed solely to the limit. The memory improvement is consistent with the scalar experiment, but still leaves a large difference from historical Zig RSS. The benchmark binary is unchanged, identified in both reports by SHA-256.

## Reproduce and scope

Compile the scalar benchmark from this repository, then use a fresh process per run:

```sh
out=/tmp/latticedb-residual-memory
mkdir -p "$out"
go test -pgo=off -c -o "$out/scalar.test" .
GOMAXPROCS=2 GOMEMLIMIT=512MiB /usr/bin/time -l "$out/scalar.test" \
  -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^100K$' \
  -test.benchtime=1x -test.timeout=9m
```

The same environment variable can be supplied when starting an application with this measured workload. It is not installed as a library default and must be remeasured for a different workload. Omit GOMEMLIMIT for the default-runtime comparison. Do not run benchmark processes in parallel.

`LATTICEDB_VECTOR_MEMORY_STATS=1` logs batch heap statistics. Set `LATTICEDB_VECTOR_HEAP_PROFILE` to an output profile filename and additionally `LATTICEDB_VECTOR_PROFILE_GC=1` to collect after an explicit GC; this new flag has no effect without a requested profile. The profile output filename is overwritten. Diagnostic profiles belong in separate runs from ordinary timing samples.

The 4KiB/8KiB binaries changed only the benchmark's existing OpenOptions.PageSize field to 4096/8192. Those trial settings were removed after measurement. Their binaries are identified by SHA-256; no production-default change, engine fork, unsafe allocator, reduced fsync frequency or smaller commit batch is introduced. Production Go source matches the previous verified report. Only the benchmark diagnostic hook and documentation are added in this follow-up.

There is no evidence that Go must use this much memory by language design. There is evidence that this bbolt-backed implementation has different memory ownership and caching than Zig. Matching Zig's bounded page-buffer footprint would require a storage-engine/buffer-lifecycle change or less work per atomic transaction; neither has been demonstrated to preserve this workload's CPU and commit contract. No such rewrite is claimed here.


## Final verification and limitations

The final 10K SIMD+PGO groups each have three fresh-process runs: 512MiB limit build median 9.977s, CPU 11.10s, maximum RSS 133.38MiB and footprint 82.49MiB; nearby default-runtime medians are 9.875s, 10.98s, 137.42MiB and 83.58MiB. All retain Recall@10 99% and the same file size. The limit group precedes the default group, not an alternating pair. At this smaller size there is no meaningful demonstrated benefit; the roughly 1% CPU/build difference is reported rather than hidden. Do not claim universal CPU neutrality from these samples.

Focused race tests under GOMEMLIMIT=512MiB pass for persisted vector reopen, vector cache/build behavior, owned writes, stream reads, recovery and compaction across the public root, engine, store and pagestore packages. Root vet and diff whitespace checks pass. The optional post-GC profile path executed successfully in the 100K diagnostic runs and the resulting profiles were read by pprof. `verify.sh` and `verify-status.txt` record commands and results.

A SHA-256 comparison of all 389 source/module files in the preceding verified report found only vector_disk_benchmark_test.go changed. The library implementation remains the previously verified candidate. Whole-tree, cross-platform, conformance and allocation-gate suites were not repeated for this diagnostic/documentation-only follow-up; their preceding results are linked above. No physical power-loss, cold-device or sustained concurrent-load tests were performed. Zig was not rerun. No Git changes were published.

An independent read-only review checked page-size compatibility and challenged the report's causal/performance claims. Candidate settings and runtime tuning remain workload-specific; this investigation is not proof that no further implementation optimization is possible.
