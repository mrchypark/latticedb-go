# Persistent vector optimization — 2026-10-13

The target is the native Zig disk path, with correctness and synchronous persistence preserved. The initial correctness-fixed Go implementation took 5,312 seconds to build 100K vectors, compared with Zig’s 359 seconds. A subsequent partially optimized 100K run was stopped at the user’s request after 19m27s overall; it has no completed 100K result. Further work used 10K iterations before another 100K attempt.

## Completed final 100K measurements

| Build | Construction | Reopen | Mean search | P99 | Recall@10 | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| Default scalar, `-pgo=off` | 322.688s | 9.258s | 2.221ms | 2.939ms | 100% | 1,250MiB |
| ARM64 `GOEXPERIMENT=simd` + explicit PGO | 262.128s | 8.878s | 2.099ms | 2.422ms | 100% | 1,221MiB |
| Zig ReleaseFast, fresh rerun | 478.963s | 1.827s | 2.015ms | 5.881ms | 100% | 128MiB |

Both final Go paths meet the approximately six-minute construction target on this host. These are one completed run per 100K variant, not a controlled estimate of the isolated SIMD or PGO contribution. The final SIMD+PGO construction is 20.3× faster than the historical correctness-fixed Go run; the default build is 16.5× faster. Both database files are 662,355,968 bytes. Whole-process elapsed time is 336.68s (default) and 274.94s (SIMD+PGO), including fixture generation, reopen validation and queries. Peak RSS includes mapped database pages and is not the cache reservation.

The fresh Zig database is 416,518,144 bytes. Its whole-process time is 481.75s, with 358.56s user CPU and 31.90s system CPU. This Zig run is slower than the previous 359.326s construction measurement: the host is shared and Zig randomizes graph levels, so the 1.83× wall-clock advantage of final SIMD+PGO Go over this fresh run is descriptive, not a stable cross-language speedup estimate. Against the historical Zig time, Go's final build also remains below six minutes.

Memory is **not matched**: final Go peak RSS is about 9.5× Zig's. Go's mapped bbolt database, heap, transaction state and decoded reuse have different coverage from Zig's native buffer pool. The Go logical cache reservation is not an overall process memory limit. The optimization reduces repeated allocation and decoding while spending bounded retained cache memory; it does not establish Zig-equivalent memory efficiency. Go also remains slower to reopen, and its main database is about 1.59× larger. No format redesign was introduced to remove this gap.

## Final 10K repeated measurements

Each value is the median of three unprofiled runs. Each run creates a fresh database. All runs returned Recall@10 99%.

| Build | Construction | Reopen | Mean search | P99 |
|---|---:|---:|---:|---:|
| Go default scalar, PGO off | 13.196s | 719.9ms | 1.484ms | 1.908ms |
| Go ARM64 SIMD + PGO | 11.768s | 704.8ms | 1.419ms | 1.713ms |
| Zig ReleaseFast | 6.245s | 90ms | 0.114ms | 0.140ms |

At 10K, Zig remains faster at construction and roughly twelve times faster at search than optimized Go. Go stores 83,378,176 bytes; Zig's three files range from 41,316,352 to 41,349,120 bytes. Zig uses randomized native graph construction, so file sizes vary. These are different native layouts, not a shared file format.

## Changes supported by profiles

- Decode page varints directly from validated bytes. Keep the streaming/WAL decoder and malformed-varint consumption/error behavior. The first decoder representation failed the existing WAL allocation gate; the retained optional-pointer representation passed without changing thresholds.
- Decode HNSW records inside a transaction-locked borrowed-byte callback. Returned nodes own all vector and adjacency storage. No bbolt bytes escape. Size, header, CRC, finite-value, degree, and trailing-byte checks remain.
- Reuse immutable decoded nodes inside an actual rebuild/update transaction, or one read search. The accessor belongs to one transaction and namespace; switching namespaces closes it. Nested mutation helpers share it. Every node write still goes through the existing page transaction and invalidates its cached ID. Modified adjacency is copied before editing. Full index deletion and changes in read bounds clear cached reads. The cache never survives a transaction, rollback, or snapshot boundary.
- Reserve at most 256MiB and 1/4 of the remaining operation memory budget during construction; read searches retain the smaller 64MiB and 1/16 limits. The benchmark uses a 1GiB operation budget. This upfront reservation can reject a workload that previously fit an exact, tight budget; it is charged explicitly and released at operation end. Entries use LRU eviction, so a full cache retains frequently read upper-layer nodes. Half covers admitted nodes/list entries; half covers retained map storage. Small reservations disable reuse. This is a logical retained-memory budget, not an RSS cap; uncached reads and algorithm scratch keep their existing admission checks. Multi-namespace ID-major updates switch the single cache, so the across-insert speedup is strongest for a single namespace.
- Compact neighbor candidates in place and keep bounded selection arrays on the stack. Reuse the accessor instead of allocating a per-connection map of all vectors. Compiler escape diagnostics confirm the small new-neighbor map does not escape.
- Preallocate the validated maximum encoded HNSW record size. The existing encoder, CRC, allocation admission, owned transaction buffers, page format, and commit path remain. No encoder buffer pooling occurs before commit. Oversized degree is now rejected by writers even without a write-charge hook; the corruption test injects malformed on-disk bytes directly.

No unsafe access, new dependency, on-disk format change, memory-only index, delayed index flush, or sync relaxation was introduced. Distance arithmetic and candidate ordering are unchanged. Cache-enabled and uncached builds are compared record by record in tests.

## Go memory and compiler evaluation

The installed Go 1.27.1 toolchain enables Green Tea GC and size-specialized small allocations by default (verified in the local toolchain’s `internal/buildcfg/exp.go`). See the [Go 1.26 runtime notes](https://go.dev/doc/go1.26) and [Go 1.27 allocation notes](https://go.dev/doc/go1.27).

PGO is evaluated with a profile from the optimized end-to-end 10K workload, against an explicit `-pgo=off` binary. The profile is supplied explicitly; this library does not install a workload-specific default profile for all consumers. [Go PGO documentation](https://go.dev/doc/pgo).

A separate `GOGC=200 GOMEMLIMIT=512MiB` experiment used the pre-batch-cache candidate: 32.51s build versus its 35.41s profiled default run. These are not a controlled paired timing result, so this is not an accepted speedup claim. Final primary timings use default GC settings. The library does not change process-wide GC settings. GOMEMLIMIT is soft and excludes mapped database memory; RSS must be measured separately. [Go GC guide](https://go.dev/doc/gc-guide).

Explicit distance-loop bounds proofs were tested with bitwise parity and compiler inspection. At 128D, candidate and reference microbenchmarks were both about 40ns with no allocation; the production change was discarded because it showed no reliable gain. Pointer-free vector/ID arrays are already used. General pooling, arenas, and a new packed layout were not added without evidence that their benefits outweigh ownership and lifetime costs.

The Go 1.27 experimental ARM64 SIMD path is opt-in with `GOEXPERIMENT=simd`; other builds keep the scalar implementation. It uses float64 NEON accumulators with the same four-lane reduction order. The scalar ARM64 compiler uses fused multiply-add, so the SIMD path explicitly uses `MulAdd`. A non-fused prototype failed a deterministic rounding regression by one ULP and was rejected. The retained path passes 10,000 exponent-varied 128D cases, signed-zero/subnormal/extreme values, dimension tails, and error comparisons. This experimental API is toolchain-dependent; it is not enabled globally. See [Go 1.27 SIMD notes](https://go.dev/doc/go1.27).

## Intermediate 10K evidence

| Candidate | Build | Mean search | Recall@10 | Notes |
|---|---:|---:|---:|---|
| Correctness-fixed original | 470.532s | 10.610ms | 99% | Previous completed report |
| Slice decoder + connection reuse | 65.932s | 2.164ms | 99% | Original partial optimization |
| Per-operation decoded reuse | 39.050s | 1.799ms | 99% | Before borrowed decoding |
| Borrowed decoding | 35.409s | 1.616ms | 99% | CPU/allocation profiling enabled |
| Transaction reuse | 14.904s | 1.601ms | 99% | CPU/allocation profiling enabled |
| Transaction reuse + initial PGO | 12.408s | 1.503ms | 99% | Single preliminary sample |
| Stack selection + encoding capacity | 12.700s | — | 99% | Profile run; noisy query timings excluded |

Sampled cumulative allocation for the same 10K profile workload fell from 24,564MiB (borrowed decoding with per-operation cache), to 7,394MiB (transaction cache), to 5,332MiB (stack selection and encoding capacity). These are allocation totals, not live heap or peak RSS. The original 1K profile showed `GetBounded` at 75.8% cumulative CPU and the per-byte reader varint path at 62.9%; overlapping percentages must not be added.

A clear-all cache policy passed 10K but slowed beyond 15K in the 100K fixture (which has 100 clusters, versus ten at 10K). That attempt was stopped at about 42K/171s because its later batches predicted more than six minutes. The final policy uses LRU rather than clearing all entries on capacity pressure. A completed 64MiB construction-cache run took 428.463s at 100K. Raising only the bounded construction reservation to 256MiB reduced the subsequent 100K SIMD+PGO run to 262.128s; search reservations and synchronous commit behavior stayed unchanged. This is why the 10K results were not extrapolated linearly.

## Measurement contract

Baseline Go binary SHA-256: `eedf4a4b6e891964b6303446f448c91076610f91ec2e0eba4f4f68f8e38a4105`, based on `21d788c9cc1c399f63bab99a958aff0d64ff4058`. Zig main is pinned to `f8e778359a54c4a79d7a49210bf75aa152f47820`, with the corrected native cosine normalization check. This is a native persistent API comparison, not interoperability testing.

Both engines use the same binary normalized clustered fixture: seed42, 128D, M16, construction ef200, search ef64, K10, and synchronous batches of 1,000 nodes. Both close/reopen and verify counts plus endpoint properties/index data. Go rejects exact fallback. Search measures 100 queries after ten warmups. Recall uses the first ten queries against exact L2. Go uses L2 and Zig native cosine; mathematical ordering on unit vectors is equivalent, but arithmetic and graph layouts differ.

Host: Apple M3, macOS26.3 arm64, Go1.27.1, Zig0.16.0 ReleaseFast. Go GOMAXPROCS=2. Native cache/pool setting:64MiB each, with different coverage; Go additionally reserves the bounded decoded cache described above. No equal RSS cap is claimed. OS cache is warm and the host is not isolated. Primary measurement runs are unprofiled and do not overlap our tests or other benchmark runs. Build includes commits and close. File size excludes sidecars.

## Verification status

The final candidate passed `go test ./...`, `go test -race ./...`, and `go vet ./...`. The SIMD build passed the whole-tree normal suite and vet. The measured SIMD+PGO configuration passed focused distance, persistent-vector and public-vector API tests. All existing graph-core allocation/byte gates passed against the original stable baseline, with no threshold changes. Allocation gate runs overlapped correctness tests; their latency is not used in the timing comparison. SIMD race tests passed for search, engine, store, pagestore and the public root package. The separate conformance module passed race tests and vet, including committed/rolled-back crash recovery. All ten Python tests passed. Root test binaries compiled with CGO disabled for Linux/amd64, Windows/amd64, Solaris/amd64, AIX/ppc64, Plan9/amd64, js/wasm and wasip1/wasm; Linux/arm64 also compiled with SIMD enabled. These are compilation checks, not runtime tests on those targets. Exact commands and exit statuses are retained in `verify-final.sh`, `verify-status.txt` and the `verify-*.txt` logs. Focused tests cover borrowed-read ownership, missing/oversized/corrupt/closed reads, cached write-through and failed writes, eviction, exact cached/uncached graph equivalence, decoder error parity, and encoding equivalence across capacity hints. Existing mutation/deletion, namespace, cancellation, rollback, recovery, and disk reopen tests remain mandatory.

An independent Pro consultation challenged aliasing, nested invalidation, borrowed-byte lifetimes, cache accounting, encoder-buffer ownership, GC configuration, and benchmark evidence. Its advice was checked against local code; it is not a substitute for tests.

Physical power-loss and cold-device tests are not included. Cross-compilation is not execution on another OS. No remote branch, PR, release, or issue state is published.

## Reproduction and artifacts

From the repository root, on the pinned toolchain:

```sh
out=/tmp/latticedb-vector-opt-20261013
# Profile was recorded from the optimized 10K disk workload.
go test -pgo=off -c -o "$out/build-cache-default.test" .
GOEXPERIMENT=simd go test \
  -pgo=docs/benchmarks/vector-build-optimization-2026-10-13/lru-simd-fma.cpu \
  -c -o "$out/build-cache-fast.test" .
GOMAXPROCS=2 LATTICEDB_VECTOR_BUILD_PROGRESS=1 \
  /usr/bin/time -l "$out/build-cache-fast.test" -test.run='^$' \
  -test.bench='^BenchmarkVectorSearchDisk128D$/^100K$' \
  -test.benchtime=1x -test.timeout=8m
```

For 10K, change the anchored sub-benchmark to `^10K$` and add `-test.count=3`. Final Go timing uses no `GOGC`, `GOMEMLIMIT`, CPU-profile or heap-profile overrides. The PGO training run used `-test.cpuprofile` and `-test.memprofile` on the optimized 10K SIMD candidate, with `-pgo=off`; the cache at that stage was 64MiB, enough for that 10K dataset. The final construction-cache ceiling is 256MiB.

Set `LATTICEDB_VECTOR_FIXTURE_DIR` to a new empty directory to export the fixture during a Go run; omit it for repeat runs. The fixture writer rejects overwriting files. Build `scripts/vector-disk-zig.zig` against the pinned Zig repository as shown in the previous [disk comparison](../disk-vector-2026-10-13/REPORT.md). Pass the fixture file and a new database path to the adapter. Clear `LATTICE_BUFFER_POOL_MB` so the adapter's explicit 64MiB setting applies.

Raw final measurements use the `build-cache-*` and `zig-final-*` names. `final-fast-100k.txt` is the completed **intermediate 64MiB-cache** result. `clear-cache-100k-stopped.txt` is an incomplete intermediate run, not a benchmark result. `gates-after-stable.txt` is historical decoder-stage evidence; final gate evidence is separately named. `source.sha256`, `binaries.sha256`, and `fixtures.sha256` identify the source snapshot, measured binaries and fixtures. Binaries and database files are not committed. Profiling files are retained to reproduce PGO and inspect CPU/allocation evidence.
