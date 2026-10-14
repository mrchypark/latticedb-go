# File mappings, Go heap, and cursor allocation — 2026-10-14

This investigation follows [the vector layout change](../vector-layout-memory-2026-10-14/REPORT.md). It separates phase-aligned file mapping residency from Go runtime memory, and tests transaction-local reuse of a bbolt search cursor.

The remaining difference is chiefly resident database mappings plus anonymous runtime memory, not all live Go objects. In the final matched stop/capture experiment, the 100K build-end Go mapping is **276.2MiB**, while anonymous VM_ALLOCATE is **369.9MiB resident**. Zig has essentially no mapped DB pages and **179.3MiB anonymous resident plus SWAPPED**. The approximately **466.8MiB region-accounting difference** separates into 276.2MiB file-backed and 190.6MiB anonymous memory. This is not an exact decomposition of peak RSS or physical RAM pressure. A separate post-GC diagnostic finds only **93.35MiB HeapAlloc**, including benchmark input and bbolt buffers.

The code change reduces query allocations and measured query CPU by **5.3%**, but does **not** establish an additional peak-memory reduction. Full tests, race checks, allocation gates and persistence checks pass.

## Measurement boundaries

The diagnostic Go benchmark collects `runtime.MemStats`, then invokes macOS `vmmap -summary` on itself before continuing the workload. Snapshots occur after the 50,000th and 100,000th vector commits, after close, and immediately after reopen. Counters and VM capture are adjacent, not an atomic kernel/runtime snapshot. A small amount of subprocess setup and runtime activity can occur between them. Diagnostics are separate from uninstrumented timing pairs.

`mapped file` resident bytes measure resident file-backed pages, not the entire virtual mapping. `VM_ALLOCATE` includes the Go heap and other anonymous runtime regions; it is not synonymous with HeapAlloc. HeapAlloc includes objects that have not yet been collected. HeapInuse minus HeapAlloc is unused space inside in-use spans. HeapIdle minus HeapReleased is idle heap that Go has not released. HeapSys minus HeapReleased is a runtime accounting quantity, not an exact resident-page census.

On this Darwin runtime, released pages use `MADV_FREE_REUSABLE`. A page can remain resident after the runtime releases it. Therefore the resident VM_ALLOCATE count can remain high while dirty bytes and physical footprint fall. The total vmmap resident count also includes shared system libraries and must not be substituted for `/usr/bin/time -l` maximum RSS. Peak RSS, peak footprint, and snapshots are different measurements.

The 100K input fixtures themselves occupy approximately 51MiB of Go heap (float data plus slice headers), versus approximately 49MiB of flat Zig float data. They are benchmark input, not database caches. The database stores raw graph vectors and an HNSW representation as in the preceding comparison.

## Code change

`pagestore.Tx` retains one cursor and its bucket name for ReadPairBounded. Every lookup starts with Seek, which rebuilds the traversal from the current root. Bucket switching replaces the cursor. The mutex covers cursor use, and commit/rollback releases its reference. The wrapper has no bucket-deletion API. Read-only bbolt bucket objects are not cached by pointer identity, so the key is the bucket name within the same transaction.

This removes repeated cursor search-stack allocation and repeated read-only bucket materialization. No page-buffer pooling, mmap advice, page size, cache budget, GC setting, transaction boundary, fsync, record format, float representation, or ANN parameter changes are involved.

Tests cover reads after insertion, replacement, deletion and reinsertion, bucket switching, absence, combined admission limits, callback errors, and transaction lifetime. The previous storage correctness and immutable-cache tests remain applicable.

## Repeated 10K results

Three serial alternating pairs, default GOGC/GOMEMLIMIT, GOMAXPROCS=2, scalar Go 1.27.1 and PGO disabled. Same fixture, batches of 1,000 synchronous commits, 16KiB native bbolt pages, 64MiB configured record cache, M=16, efConstruction=200, efSearch=64. Medians:

| Metric | Previous code | Cursor reuse |
|---|---:|---:|
| Build | 10.508s | 11.012s |
| Total process CPU | 11.47s | 11.44s |
| Maximum RSS | 100.16MiB | 102.78MiB |
| Peak physical footprint | 63.66MiB | 66.33MiB |
| Mean query | 1.187ms | 1.082ms |
| P99 query | 1.438ms | 1.336ms |
| Query bytes/op | 1,360,360 | 1,133,512 |
| Query allocations/op | 12,864 | 7,860 |
| Recall@10 | 99% | 99% |
| Database bytes | 59,015,168 | 59,015,168 |

This is a query-allocation improvement, not evidence of lower 10K peak RSS. CPU is essentially unchanged, while build wall time increases 4.8% in these samples. Before RSS ranges 99.23–109.67MiB; after ranges 102.11–103.64MiB. Preserve the individual samples and do not infer a deterministic GC/RSS benefit from fewer allocated bytes.

## 100K Go diagnostic

| Metric | Previous code, fresh | Cursor reuse, fresh |
|---|---:|---:|
| Build, including diagnostic capture | 224.584s | 225.950s |
| Total process CPU | 237.05s | 234.75s |
| Maximum RSS | 655.27MiB | 647.53MiB |
| Peak physical footprint | 365.53MiB | 366.41MiB |
| Mean / P99 query | 2.151 / 2.699ms | 1.827 / 2.378ms |
| Recall@10 | 100% | 100% |
| Database bytes | 419,856,384 | 419,856,384 |
| Query bytes / allocations | 1,804,504 / 16,529 | 1,525,432 / 10,373 |

The candidate also requests one explicit benchmark-only GC and a heap profile after the final build snapshot, before close. The baseline diagnostic does not. Therefore especially close/reopen snapshots and total run timings are not an exact paired causal experiment. Neither production code nor uninstrumented timing samples force GC. The approximately 1% peak RSS/CPU differences do not establish a meaningful peak-memory reduction; physical footprint is effectively unchanged.

The candidate's exact post-GC HeapAlloc is 97,886,432 bytes, effectively identical to the preceding layout candidate's 97,885,856 bytes. Its sampled retained profile attributes approximately 47.7MiB to bbolt page buffers and 46.8MiB to fixture generation. Sampling varies from the previous profile's 42.2MiB and 48.8MiB respectively; do not interpret the difference as an exact retained-buffer count. `sync.Pool` can retain victim entries after one GC.

Sampled build allocations fall from 48.05GiB in the preceding layout profile to 43.96GiB. Cursor.search falls from 5.92GiB to 1.91GiB. Remaining cursor allocation comes from other read/write paths. The underlying live vector/page working set is unchanged.

### Same-phase memory, MiB

| Go phase | File mapping resident | VM_ALLOCATE resident | VM_ALLOCATE dirty | HeapAlloc | HeapInuse | HeapSys | HeapReleased |
|---|---:|---:|---:|---:|---:|---:|---:|
| Previous, built 50K | 143.6 | 264.1 | 232.1 | 91.9 | 109.9 | 255.6 | 35.1 |
| Previous, built 100K | 275.1 | 353.0 | 340.7 | 251.9 | 321.3 | 351.6 | 24.9 |
| Previous, closed | ~0 | 353.0 | 340.7 | 252.0 | 321.4 | 351.6 | 24.9 |
| Previous, reopened | 289.8 | 353.0 | 129.7 | 86.4 | 92.0 | 351.6 | 235.9 |
| Cursor reuse, built 50K | 145.2 | 282.4 | 269.8 | 203.6 | 217.3 | 275.7 | 18.3 |
| Cursor reuse, built 100K | 275.8 | 345.2 | 264.9 | 115.7 | 138.2 | 355.6 | 104.5 |
| Cursor reuse, closed after diagnostic GC | 0 | 345.2 | 260.6 | 94.9 | 102.2 | 355.7 | 104.5 |
| Cursor reuse, reopened | 289.8 | 278.6 | 183.0 | 94.1 | 100.0 | 355.7 | 247.4 |

The same commit count does not imply the same GC phase. The live workload is unchanged, but HeapAlloc fluctuates with collection. The close snapshot directly demonstrates that roughly 275MiB belongs to the file mapping: it disappears when the DB closes, while anonymous residency remains. Reopen touches roughly 290MiB of the file while heap reclamation proceeds independently. There is no basis for labeling all of the measured 648MiB peak RSS as live Go objects.

## Interpretation and rejected shortcuts

The measured gap is not a proven Go language lower bound. There are distinct implementation costs: resident file-backed bbolt pages, private copy-on-write page buffers, transient decoded HNSW objects, and GC/runtime headroom. bbolt uses native 16KiB pages here, while Zig uses 4KiB pages with its explicit buffer pool. Equal cache-setting numbers do not imply equal memory coverage.

The previous page-layout change reduced the dirty working set substantially. This follow-up removes avoidable search-stack churn, but leaves that working set and the input fixture unchanged. The ordinary Go runtime eventually reclaims dead objects and releases idle pages. Forcing GC, reducing GOGC/GOMEMLIMIT, or advising away mmap pages would change collection/paging behavior and can trade CPU or I/O latency for reported memory; none is installed in the library. Dirty pages cannot be dropped or reused before the synchronous commit permits it.

A substantially lower standing memory footprint needs a different page-buffer/representation lifecycle or a smaller transaction working set. This investigation does not demonstrate a safe CPU-neutral replacement, and does not claim that no further code optimization is possible. It retains the bounded cursor improvement without claiming another large peak-memory reduction.

## Reproduction and source evidence

The measurement scripts record this host's absolute paths under `/tmp/latticedb-mmap-20261014`; use a fresh output directory for another experiment. Zig rejects existing DB output paths. `instrument.py` creates a Go overlay containing the snapshot hook. Build with `go test -c -pgo=off -overlay=...`, then run BenchmarkVectorSearchDisk128D/100K with SNAPSHOT_DIR set. The before versions of the two changed production files are saved as `pagestore-before.go.txt` and `get_bounded-before.go.txt`; add them to the overlay to rebuild the previous implementation without changing the worktree. The previous query-only harness and retained-DB preparation are in the [preceding report](../vector-layout-memory-2026-10-14/REPORT.md).

`zig-instrument.py` adds process-stop markers to the existing disk adapter. Compile against the pinned Zig source using the preceding report's command. `zig-capture.py` waits for SIGSTOP, records vmmap, and resumes the process. This is Darwin-only diagnostic code, not a production adapter or a cold-device latency benchmark. The 1K smoke run verifies the stop/capture/resume sequence and persistence checks.

Local primary-source checks: bbolt 1.5.0 Cursor documents repositioning after mutation, and Cursor.seek resets its stack before searching from the root. Tx.write syncs dirty pages before returning single-page buffers to its pool. Go 1.27.1 runtime/mem_darwin.go uses MADV_FREE_REUSABLE when releasing pages. Zig's pinned storage/buffer_pool.zig owns fixed-size page frames, and storage/page_manager.zig performs explicit file I/O. These implementation differences, not just language names, explain why the same numeric cache option has different coverage.

## Verification

The final production patch passes full `go test ./...`, full `go test -race ./...`, `go vet ./...`, the existing SIMD package tests, conformance race/vet, and root compilation for Linux/amd64, Windows/amd64, Plan9/amd64, JS/wasm and WASI/wasm. The strict allocation regression gates also pass. Commands and exit statuses are retained in `verify.sh`, `verify-status.txt` and `gates.sh`.

An independent review checked cursor lifetime, repositioning, bucket identity, cleanup, the new regression tests and the distinction between runtime counters and OS memory. It found no additional issue. No physical power-loss, cold-device or sustained concurrent-load test was performed. No Git changes were published. The previous converted-record downgrade limitation remains unchanged.

## Query CPU on the same 100K database

Three alternating fresh-process pairs each execute 2,000 queries on the same retained, closed 100K split-layout DB. This compares the previous split-layout reader directly with the cursor candidate, not legacy records. Query-loop getrusage CPU excludes opening, warmup and recall evaluation, and includes query GC. The harness rejects exact fallback. All runs have Recall@10 100%.

| Median metric | Previous code | Cursor reuse |
|---|---:|---:|
| CPU/query | 2.191753ms | 2.074954ms |
| Wall mean/query | 2.099877ms | 2.001573ms |
| P99 | 3.796709ms | 3.636833ms |

CPU/query decreases 5.3%, and improves in each pair. Tail latency still varies on the shared host; the third baseline P99 is 6.76ms. These are evidence for keeping the small allocation optimization, not for an RSS reduction or a universal latency guarantee.

## Fresh Zig measurement and paging

Zig was rebuilt from the unchanged adapter source and the clean pinned source tree at `f8e778359a54c4a79d7a49210bf75aa152f47820`, using Zig 0.16.0 ReleaseFast. The adapter SHA-256 still matches the preceding report. Only diagnostic stop markers were added. The 100K run completes with build 336.989s, reopen 1.403s, mean query 1.964ms, P99 5.661ms, Recall@10 100%, and 416,559,104 database bytes. Build includes the snapshot pauses and is not an uninstrumented timing result.

| Zig phase | File mapping resident | VM_ALLOCATE resident | VM_ALLOCATE SWAPPED | Physical footprint |
|---|---:|---:|---:|---:|
| Built 50K | 0.012MiB | 176.8MiB | 0 | 178.2MiB |
| Built 100K | ~0 | 83.7MiB | 95.6MiB | 180.8MiB |
| Closed | ~0 | 77.8MiB | 95.5MiB | 174.7MiB |
| Reopened | ~0 | 86.8MiB | 93.1MiB | 181.3MiB |

SWAPPED includes nonresident memory; this output alone does not distinguish compressed memory from disk swap. The host has both compressor use and disk swap. Calling all of this column in-RAM compressed memory would be unsupported. A mid-build check also records 92.3MiB resident and 87.5MiB SWAPPED in VM_ALLOCATE. The fresh peak physical footprint reaches 183.9MiB by reopen, versus the historical run's approximately 132.5MiB. Do not subtract historical maximum RSS from a fresh Go phase snapshot and label the result an exact language overhead.

For the final cross-runtime phase comparison, Go also uses an external SIGSTOP/vmmap/SIGCONT collector. This avoids comparing Go's self-launched corpse snapshot with Zig's stopped live-task snapshot. `external.go.txt` contains the benchmark-only hook and `external-capture.py` records the runtime counters, VM map and final wait4 process resource usage. No explicit GC is requested in this final comparison.

## Final matched external-capture comparison

Both processes stop at the same build-count/close/reopen boundaries and are inspected as live tasks by an external collector. Go uses the final cursor code and no explicit GC. Zig is the fresh run above. Units are MiB. The mapping column excludes the negligible non-DB mapping in Zig.

| Phase | Go DB mapping resident | Go anonymous resident | Go anonymous SWAPPED | Zig anonymous resident | Zig anonymous SWAPPED |
|---|---:|---:|---:|---:|---:|
| Built 50K | 143.6 | 273.1 | 0 | 176.8 | 0 |
| Built 100K | 276.2 | 369.9 | 0 | 83.7 | 95.6 |
| Closed | 0 | 369.9 | 0 | 77.8 | 95.5 |
| Reopened | 289.8 | 369.9 | 0 | 86.8 | 93.1 |

At build end, physical footprint is **343.3MiB Go versus 180.8MiB Zig**. Snapshot peak footprint is **373.1MiB versus 183.8MiB** at that point. Go's reopen footprint falls to **137.2MiB**, although anonymous residency stays at 369.9MiB, demonstrating why residency and footprint must not be conflated. Zig's reopen footprint is 181.3MiB. These different GC/paging states also prevent one phase from establishing a universal process-memory ratio.

Go build-end counters are HeapAlloc 239.7MiB, HeapInuse 316.9MiB, HeapSys 359.6MiB and HeapReleased 34.0MiB. HeapAlloc is not all reachable/live objects. The separate forced-GC profile's 93.35MiB is useful evidence about the retained set, but must not be substituted into this snapshot to fabricate an exact simultaneous breakdown. At reopen, HeapAlloc is 77.0MiB and HeapReleased 244.0MiB.

The final Go run completes with build **213.589s**, reopen **7.813s**, mean query **1.879ms**, P99 **2.461ms**, Recall@10 **100%**, and unchanged 419,856,384 database bytes. wait4 records 224.479s CPU and 672.88MiB maximum RSS. This larger RSS than the earlier 647.53MiB candidate run further rules out a confident peak-RSS improvement claim. The diagnostic build times include snapshot pauses; the three alternating uninstrumented 10K pairs and repeated 100K query CPU experiment are the performance evidence for the code change.

All 391 final source/module hashes remain unchanged after verification. This investigation is recorded as run-specific evidence; it does not establish a Go language lower bound or a portable memory-sizing rule.

Raw final captures: [Go counters and results](external-100k.txt), [Go build-end VM map](external-built-100000.vmmap), [Zig results](zig-100k.txt), [Zig build-end VM map](zig-2-built.vmmap).
