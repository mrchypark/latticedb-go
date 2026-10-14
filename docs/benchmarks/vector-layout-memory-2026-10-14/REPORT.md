# HNSW storage layout and cache memory optimization

This follow-up changes library code. It keeps the default Go GC policy, native bbolt page size, cache reservations, synchronous commit batches, HNSW parameters, and vector precision. The preceding [remaining-memory investigation](../vector-residual-memory-2026-10-14/REPORT.md) identified bbolt's dirty-page/pool working set as a major source of memory use; it did not establish a language limit.

## Changes

* Separate immutable HNSW vector payloads from mutable level, deletion, and neighbor data. An update to the same immutable cached vector writes only the small links record. Existing full records convert lazily on their first eligible update. Changed vectors still use the full-record writer, which removes obsolete links in the same transaction.
* Store converted vectors as fixed-width little-endian float32 bits. No quantization or arithmetic change is introduced. This also avoids decoding 128 float bit patterns as variable-length integers on every disk read. The fixed vector header uses bounded direct parsing after shared checksum validation, avoiding generic decoder allocations for this payload.
* Replace `container/list` and boxed cache values with typed entries and intrusive links. Replacing a node reuses its cache entry while publishing a separately owned immutable node snapshot. Existing readers keep their old snapshot.
* Read the two records under one transaction lock and check their combined stored size before decoding. Reuse one bbolt cursor's search stack for both key lookups. Returned node data is owned; borrowed bbolt bytes never escape the callback.

These changes reduce the pages that bbolt must copy and retain for a transaction. They do not alter bbolt's allocator or page-pool lifecycle. No fork, unsafe allocator, explicit library GC, deferred fsync, or smaller atomic transaction is introduced.

## Persistence and compatibility

The existing `vector-hnsw` bucket keeps prefix-1 base keys. Prefix-2 keys hold links for the same namespace and node ID. This separate key range packs mutable links away from the larger vector bases, so connection updates copy fewer bbolt pages. Legacy kind-5 full records remain readable. Converted kind-8 bases and kind-6 links have distinct checksummed payload tags and node IDs. Missing links, mismatched IDs, corrupt checksums, non-finite vectors, and malformed lengths are errors. A converted base cannot be relabeled as a legacy record and successfully decoded.

Both writes belong to the existing bbolt transaction. All required staged-byte charges occur before writes; a size fallback changes neither record. Any write error propagates to the engine's rollback path. The existing synchronous commit and recovery policy remains unchanged.

**Do not open a converted database with an older Go binary.** The older vector decoder rejects the new record kind, but there is no new whole-file downgrade gate. This change does not claim mixed-version writer support or Go/Zig file compatibility. Keep a pre-upgrade backup if an older binary must remain usable.

## Measurement method

Apple M3, macOS 26.3, Go 1.27.1, scalar code and PGO off, GOMAXPROCS=2. GOGC and GOMEMLIMIT are unset. The public disk API writes normalized 128D vectors with seed 42, M16, efConstruction200, efSearch64, K10, synchronous batches of 1,000, 64MiB record-cache configuration and the same 1GiB construction logical-byte budget. The construction cache reservation ceiling remains 256MiB. It closes and reopens the database, verifies stored endpoint vectors and index metadata, warms up ten queries, times 100 queries, and computes Recall@10 for ten queries against exact L2. Exact-search fallback is rejected.

Each timing process uses a fresh database. Tests and compilation do not overlap performance measurements. The host is shared and OS cache is warm; maximum RSS, physical footprint, heap allocation, and file size are different measurements. CPU time is whole-process user plus system time, including reopen and queries.

The 100K diagnostic records batch memory counters and a pre-close heap profile after one explicit diagnostic GC. This option is confined to the benchmark. A one-GC profile may retain sync.Pool victim entries; it is not a minimum live-set census. Heap profile byte totals are sampled estimates.

## Results

### 10K, final code, three alternating pairs

| Median, fresh process per run | Previous code | Final code |
|---|---:|---:|
| Build | 12.689s | 11.544s |
| Process CPU | 13.14s | 11.98s |
| Maximum RSS | 130.89MiB | 101.77MiB |
| Peak physical footprint | 80.84MiB | 65.06MiB |
| Mean query | 1.531ms | 1.248ms |
| P99 query | 2.066ms | 1.839ms |
| Reopen | 816.2ms | 583.0ms |
| Main database bytes | 83,378,176 | 59,015,168 |
| Recall@10 | 99% | 99% |
| Separate harness query B/op | 1,381,256 | 1,360,360 |
| Separate harness query allocs/op | 14,532 | 12,864 |

Maximum RSS falls 22.3%, CPU time 8.8%, and mean query time 18.4% in these samples. Query allocations also fall: bytes by 1.5% and object count by 11.5%. The single-query Go `ns/op` sample is separate from the mean of 100 queries; the comparison uses the latter.

All samples are retained. One final run has P99 7.229ms, compared with 1.719ms and 1.839ms in the other two runs, and an unusually slow reopen. The table shows medians, not a guarantee that every tail latency improves on the shared host.

Raw alternating pairs: [baseline 1](baseline-direct-pair-10k-1.txt), [final 1](final-direct-pair-10k-1.txt), [baseline 2](baseline-direct-pair-10k-2.txt), [final 2](final-direct-pair-10k-2.txt), [baseline 3](baseline-direct-pair-10k-3.txt), [final 3](final-direct-pair-10k-3.txt).

### 100K and retained page buffers

The previous implementation was rerun on the same host with the same diagnostic options. Each column below is one fresh-process run; the final implementation was measured after its nearby baseline. Large-scale single-run timings are not statistical proof of a universal speedup.

| 100K diagnostic | Previous code | Final code |
|---|---:|---:|
| Build | 261.600s | 261.709s |
| Process CPU | 275.02s | 260.52s |
| Instructions retired | 3.967 trillion | 3.447 trillion |
| Maximum RSS | 1,106.47MiB | 642.89MiB |
| Peak physical footprint | 587.39MiB | 360.99MiB |
| Mean / P99 query | 2.211 / 2.837ms | 2.372 / 8.749ms |
| Reopen | 9.724s | 9.464s |
| Main database bytes | 662,355,968 | 419,856,384 |
| Recall@10 | 100% | 100% |
| Post-GC HeapAlloc | 230,714,312 bytes | 97,885,856 bytes |
| Sampled retained bbolt page-pool allocation | about 159.47MiB | about 42.15MiB |

Maximum RSS falls **41.9%**, physical footprint 38.5%, and total CPU time 5.3%. Retired instructions fall 13.1%, while build wall time is effectively unchanged in this pair. The retained page-pool estimate falls about 74%; sampled allocation is not an exact count of pages. The input fixture still accounts for roughly 49MiB of the final sampled heap. Actual post-GC HeapAlloc falls 57.6%. This is evidence of a smaller storage working set, not merely a different GC limit.

The short 100-query latency sample is worse in the final run, including two large tail samples. It is reported rather than hidden. The independent repeated-query experiment below checks CPU cost without build/reopen carryover; it does not erase these observed latency tails or establish a tail-latency guarantee.

Raw [baseline](baseline-100k-fresh.txt), [final](final-direct-100k.txt), [baseline heap profile summary](baseline-100k-live-top.txt), and [final heap profile summary](final-direct-100k-live-top.txt) retain the evidence. [Baseline](baseline-100k-live.mem) and [final](final-direct-100k-live.mem) heap profiles are also included.

### Repeated query CPU on the same 100K graph

A separate final-code build was kept on disk. A closed copy was re-encoded into legacy full-node records in synchronous batches of 1,000. Every one of the **100,000 logical HNSW nodes** was read back and compared after conversion; vectors, levels, deletion flags and neighbors all matched. This changes storage representation without rebuilding or changing the graph. See the [conversion check](query-materialize.txt).

Both query variants use the final typed LRU and key allocator. The legacy variant restores the earlier single-record reader/decoder; the split variant uses the final pair reader/direct float decoder. This is a storage/reader ablation, not a second measurement of the complete old binary. The legacy file also has the physical layout produced by re-encoding the copied database, rather than the original construction layout.

Each fresh process opens the existing database, keeps the same generated input fixture alive, warms all 100 query vectors, then runs 2,000 queries. `getrusage(RUSAGE_SELF)` measures user plus system CPU across only that query loop, including query-triggered GC. Open, warm-up and exact-recall validation occur outside that CPU interval. Three legacy/final pairs run serially with unchanged runtime settings; all reject exact fallback and retain Recall@10 100%.

| Median of three processes | Legacy layout/reader, final LRU | Final split layout/reader |
|---|---:|---:|
| CPU per query | 2.288ms | 2.188ms |
| Mean elapsed query | 2.562ms | 2.095ms |
| P99 elapsed query | 5.847ms | 3.526ms |
| Recall@10 | 100% | 100% |

Query CPU is lower in all three pairs; the median reduction is **4.4%**. This supports CPU preservation for the storage change in the measured workload. Both variants show latency outliers on the shared host, so this experiment does not attribute every short-run delay to the implementation.

Raw pairs: [legacy 1](query-legacy-1.txt), [final 1](query-final-1.txt), [legacy 2](query-legacy-2.txt), [final 2](query-final-2.txt), [legacy 3](query-legacy-3.txt), [final 3](query-final-3.txt). The [Darwin-only diagnostic harness](query-benchmark.go.txt), [legacy reader variant](query-legacy-reader.go.txt), and [reproduction script](query-reproduce.sh) are benchmark artifacts; no query-test option or platform-specific CPU counter was added to the library.

### Zig reference

Zig uses the unchanged pinned `f8e778359a54c4a79d7a49210bf75aa152f47820` reference and the same compiled adapter/float32 fixtures as the prior report. The binary SHA-256 matches the archived reference. Three fresh 10K runs were performed after the final Go timing run. The 100K Zig column is the earlier same-fixture measurement, not a fresh paired run.

| Native persistent API | Final Go 10K, median | Zig 10K, fresh median | Final Go 100K, diagnostic | Zig 100K, historical |
|---|---:|---:|---:|---:|
| Build | 11.544s | 5.885s | 261.709s | 478.963s |
| Process CPU | 11.98s | 5.78s | 260.52s | 390.46s |
| Maximum RSS | 101.77MiB | 83.03MiB | 642.89MiB | 127.88MiB |
| Peak physical footprint | 65.06MiB | 82.33MiB | 360.99MiB | 132.52MiB |
| Mean query | 1.248ms | 0.114ms | 2.372ms | 2.015ms |
| Main database bytes | 59,015,168 | 41,308,160 | 419,856,384 | 416,518,144 |
| Recall@10 | 99% | 99% | 100% | 100% |

Fresh Zig logs: [1](zig-final-10k-1.txt), [2](zig-final-10k-2.txt), [3](zig-final-10k-3.txt). The [historical 100K log](../vector-build-optimization-2026-10-13/zig-final-100k.txt) and earlier report describe native L2 versus cosine arithmetic, HNSW randomness and persistence-policy differences. The Go record cache and Zig page buffer pool both have a 64MiB setting but cover different memory. Native page sizes also differ (Go/bbolt 16KiB on this host, Zig 4KiB).

The 100K RSS gap remains substantial. Go retains runtime heap headroom and maps the bbolt database; the lower page-pool profile does not decompose every part of peak RSS or prove that the remainder is a language-imposed minimum. Physical footprint and RSS should not be interchanged. These results do not claim equal power-loss behavior or file interoperability.

## Verification

The final candidate passes full `go test ./...`, full `go test -race ./...`, `go vet ./...`, targeted SIMD tests, conformance race/vet checks, and root cross-compilation for Linux/amd64, Windows/amd64, Plan9/amd64, JavaScript/wasm and WASI/wasm. The existing strict allocation regression suite also passes. See [verification commands](verify.sh), [exit statuses](verify-status.txt), and [allocation gate commands](gates.sh).

Focused coverage checks legacy conversion, float32 bit preservation (including signed zero and subnormals), CRC-valid malformed payloads, missing/mismatched links, combined read limits, size fallback without mutations, rejected staged-byte reservations, full replacement cleanup, namespace deletion, commit/reopen, rollback, immutable cached snapshots, and exact cached-versus-uncached HNSW record equality. A new lifecycle test requires split records to exist, verifies that compaction preserves every logical HNSW node, then backs up and reopens the database and compares vectors and search results. Pair-reader tests cover committed/writable records, missing versus empty values, key ordering, same-key reads, combined limits and callback errors.

An independent read-only review checked storage ownership, rollback propagation and maintenance paths. The persistence guarantees follow the existing synchronous bbolt transaction contract; no physical power-loss experiment or equal-durability claim against Zig is made. Cold-device and sustained concurrent-load tests were not run. No Git changes were published. The query reproduction script was syntax-checked; its build, conversion, and measurement steps were executed individually rather than rerunning the assembled script.

## Reproduce

```sh
go test -c -pgo=off -o /tmp/layout-vector.test .
env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 /usr/bin/time -l \
  /tmp/layout-vector.test -test.run='^$' \
  -test.bench='^BenchmarkVectorSearchDisk128D$/^10K$' \
  -test.benchtime=1x -test.timeout=9m
```

Run three fresh processes per code version, alternating the versions. Change the sub-benchmark to `100K` for the larger workload. For the separate diagnostic mode, additionally set `LATTICEDB_VECTOR_BUILD_PROGRESS=1`, `LATTICEDB_VECTOR_MEMORY_STATS=1`, `LATTICEDB_VECTOR_HEAP_PROFILE` to a new output filename, and `LATTICEDB_VECTOR_PROFILE_GC=1`. The profile filename is overwritten. No runtime-limit or GC-frequency setting is installed in the library.

[Final source hashes](final-source.sha256), [binary hashes](binaries.sha256), [fixture hashes](fixtures.sha256), and [Go build metadata](build-info.txt) identify the measured inputs. The previous implementation is the locally verified candidate from the preceding memory report, not an unmodified Git HEAD. The generator matches that earlier source manifest.

An initial split-record implementation retained variable-length float encoding and regressed query time; it was not selected. The fixed-width candidate restored 10K query performance. Its intermediate 100K diagnostic is retained as [raw evidence](candidate-100k-diagnostic.txt). Later candidates reuse the paired-read cursor and remove generic decoding from the fixed vector header. [The earlier pair reader](candidate4-get_bounded.go.txt) is a component snapshot, not the complete final source.
