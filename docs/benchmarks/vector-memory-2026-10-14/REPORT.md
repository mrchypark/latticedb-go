# Persistent vector CPU and memory follow-up — 2026-10-14

This follow-up compares with the completed October 13 optimized Go implementation, not the original 88-minute build. The public disk API, synchronous batches of 1,000, file format, fixture, construction/search parameters, and correctness checks are unchanged. No process-wide GC settings are changed by the library.

## Profile-led changes

1. Reuse the existing vector search scratch structure within one build transaction. Reserve 512KiB from the decoded-cache budget, reduce the node allowance accordingly, clear visited IDs after each layer, and discard buffers above the bounded capacities. No global pool or state shared between transactions is introduced. Neighbor-selection candidates use bounded stack storage.
2. Size encoded vector records from their actual varint widths. Transfer the fresh owned buffer to bbolt through an internal `PutOwned` method, avoiding a second whole-record copy. General `Put` still copies caller buffers. Validation, transaction locking, write accounting, commit and rollback remain shared.
3. After a successful vector write, cache an owned adjacency snapshot. Reuse a previous immutable vector only when its backing storage is identical. Otherwise copy it. Failed writes invalidate the old entry. Conservatively skip cache publication when bounded reading or decoder invariants could fail; tests cover small bounds, empty vectors, zero neighbor IDs, caller mutation and disk equivalence.
4. Decode stream-maintenance reads directly inside a borrowed transaction callback. These reads previously filled the generic cache with obsolete changefeed records during retention trimming. The live-allocation profile attributed 87.25MiB to that cache path. The existing decoder still validates the record limit, CRC, kind, sequence and payload, and returns owned values. Changefeed contents and retention policy are unchanged.

The intermediate 100K run after changes 1–3 reduced total CPU time from 297.13s to 288.07s and maximum RSS from 1,303.47MiB to 1,233.09MiB. That was not enough: the pre-close heap profile exposed the stream-maintenance cache above, which motivated change 4. An in-use profile reports allocations visible to the last GC, not an exact census of reachable objects at the instant of capture. No forced GC was used.

## Intermediate allocation evidence

Continuous profiling was enabled only for these 10K diagnostic runs. They are separate from unprofiled timing comparisons.

| Candidate | Sampled total allocation |
|---|---:|
| Previous optimized implementation | 5,091.92MiB |
| Reuse bounded search scratch | 3,798.87MiB |
| Exact-capacity owned vector writes | 3,211.15MiB |
| Publish owned decoded-write snapshots | 2,852.49MiB |

Reduced total allocation did not itself guarantee reduced peak RSS. Three alternating unprofiled 10K before/after pairs for changes 1–3 reduced median CPU time from 14.05s to 12.62s, but increased median RSS from 131.41MiB to 134.92MiB. All raw samples are retained, including this unfavorable result.

## Method and limits

Apple M3, macOS26.3 arm64, Go1.27.1, GOMAXPROCS=2. Primary comparisons use scalar Go with `-pgo=off`, default GC settings, the native 64MiB record-cache setting, and the existing bounded construction cache. The host is shared and OS cache is warm. CPU time means user + system seconds from `/usr/bin/time -l`, not peak CPU percentage. Maximum RSS includes resident file mappings, not just Go heap. Physical footprint and Go heap statistics are different metrics and are reported separately.

The fixture is the same normalized 128D clustered data used in the [previous comparison](../vector-build-optimization-2026-10-13/REPORT.md): M16, construction ef200, search ef64, K10, 100 timed queries and recall on the first ten queries. Each timing run uses a fresh database, synchronous commits and close/reopen checks; exact-search fallback is forbidden. At 10K there are ten clusters; at 100K there are 100, so timing is not extrapolated linearly.

The 100K diagnostic runs collect batch `runtime.MemStats`; the previous and first candidate run each include one external `vmmap` snapshot. The candidates also write a pre-close heap profile without forcing GC. No continuous CPU profiler runs during these 100K measurements. Ordinary unprofiled 10K comparisons omit all diagnostic options. Tests and compilation do not overlap benchmark processes.

Zig measurements from October 13 remain a historical reference; Zig is unchanged and is not remeasured in this follow-up. Physical power-loss, cold-device, and sustained production-load tests are outside this measurement. No Git changes are published.

## Final measurements and verification

Default scalar Go, PGO off; 100K is one completed diagnostic run per candidate.

| Metric | Previous optimized Go | Final scalar Go | Change |
|---|---:|---:|---:|
| Build, synchronous commits + close | 280.278s | 252.089s | −10.1% |
| Whole-process user + system CPU | 297.13s | 262.16s | −11.8% |
| Maximum RSS | 1,303.47MiB | 1,108.88MiB | −14.9% |
| Peak physical footprint | 785.05MiB | 590.53MiB | −24.8% |
| Reopen | 8.717s | 8.913s | +2.2% |
| Mean query | 2.105ms | 2.148ms | +2.0% |
| P99 query | 2.620ms | 2.790ms | +6.5% |
| Recall@10 | 100% | 100% | unchanged |
| Main database bytes | 662,355,968 | 662,355,968 | unchanged |

The read-query path was not optimized by this follow-up. Small query-latency differences from these single runs do not establish a regression or improvement. Reopen and database size remain limitations. Final scalar 100K whole-process elapsed time was 265.09s, including fixture generation, validation, reopen and queries.

After the stream-maintenance change, the pre-close sampled in-use allocation profile fell from 242.39MiB (intermediate candidate) to 168.19MiB; the obsolete-record cache path disappeared. About 54MiB of the latter sample is the benchmark's retained input fixture. This does not make the application memory-free: decoded HNSW nodes, transaction buffers, the Go heap and database mappings remain.

Final scalar 10K unprofiled results (three separate fresh-process runs): build median 10.946s, CPU median 12.04s, maximum-RSS median 138.81MiB. The previous implementation's alternating-pair baseline medians were 12.694s, 14.05s and 131.41MiB. At this smaller scale CPU/build improve, but RSS does not; the larger-scale memory improvement must not be generalized to every database size.

Optional ARM64 SIMD+PGO uses a fresh profile from this final implementation's 10K disk workload. Its three unprofiled 10K runs have a build median of 9.810s and CPU-time median of 10.94s. The library still defaults to scalar code; the experimental SIMD flag and an explicit application PGO profile are opt-in.

The optional SIMD+PGO 100K run completed: build 252.988s, user + system CPU 257.25s, maximum RSS 1,069.20MiB, peak physical footprint 559.39MiB, reopen 9.035s, mean search 2.289ms, P99 4.160ms, Recall@10 100%, main database 662,355,968 bytes. Whole-process elapsed time was 266.25s. At 100K it does not show a build-time improvement over scalar Go in these single runs; the CPU and RSS measurements are slightly lower. Its 100K run has progress logging but no heap telemetry, heap snapshot, external vmmap or continuous CPU profiling.

Final scalar `go test ./...`, `go test -race ./...` and `go vet ./...` passed. All existing graph-core allocation and byte gates passed against the unchanged stable baseline; no threshold was relaxed. Gate allocation samples ran alongside correctness tests, and their latency is excluded from performance claims. The SIMD whole-tree suite and vet passed, as did SIMD race tests for search, engine, store, pagestore and the public root package. The measured SIMD+PGO configuration passed focused distance, vector, stream, changefeed and ownership tests. The separate conformance module passed race tests and vet; all ten Python tests passed. Root test binaries compiled with CGO disabled for Linux/amd64, Windows/amd64, Solaris/amd64, AIX/ppc64, Plan9/amd64, js/wasm and wasip1/wasm, plus Linux/arm64 with SIMD. Cross-compilation is not execution on those targets. All command exit statuses are recorded in `verify-status.txt`.

## Reproduce

From the repository root, with Go1.27.1:

```sh
out=/tmp/latticedb-memory-20261014
mkdir -p "$out"
go test -pgo=off -c -o "$out/scalar.test" .
GOMAXPROCS=2 /usr/bin/time -l "$out/scalar.test" \
  -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^100K$' \
  -test.benchtime=1x -test.timeout=9m
```

For diagnostic sampling, set `LATTICEDB_VECTOR_MEMORY_STATS=1`. For a pre-close heap snapshot, set `LATTICEDB_VECTOR_HEAP_PROFILE` to a writable output filename; this overwrites that profile file, never the database. These options are off by default. Progress output is separately enabled by `LATTICEDB_VECTOR_BUILD_PROGRESS=1`.

`measure-fast.sh` records the exact local SIMD training, explicit PGO compilation and sequential benchmark commands. `verify-final.sh` and `gates-final.sh` record the verification commands; these captured scripts use this machine's absolute repository and artifact paths. Raw logs and profiles are retained beside this report. The original 2026-10-13 report and source hashes remain a historical snapshot, not the final source of this follow-up.


## Evidence provenance

`before-*` is the previous optimized production implementation plus diagnostic benchmark hooks. `scratch-*`, `owned-*`, `write-cache-*`, and `candidate-*` are intermediate candidates. `stream-*` is the final scalar implementation, and `fast-*` is the final optional SIMD+PGO implementation. `final-simd.cpu` is the explicit PGO training profile. The measured binaries themselves and large fixture/database files are not added to the repository. SHA-256 manifests identify the measured binaries, final Go source snapshot and unchanged fixtures.

An independent bounded review confirmed the ownership-transfer and immutable-cache contracts and reviewed the new transaction tests. Local tests and measurements, not that review alone, determine acceptance. Broad bbolt fill-factor tuning and process-wide GC tuning were not needed for the identified retention fix.
