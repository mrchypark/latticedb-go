# Public disk vector comparison — 2026-10-13

This run compares the public persistent database APIs. It replaces the resident-Go versus Zig-component comparison in CI. It does not test file-format or API interoperability.

## Results

| Vectors | Engine | Build (s) | Reopen (ms) | Mean (ms) | P99 (ms) | Recall@10 | Primary file (MiB) |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1,000 | Go | 41.392 | 414.1 | 4.639 | 5.626 | 100.0% | 8.00 |
| 1,000 | Zig | 0.450 | 32.0 | 0.058 | 0.160 | 100.0% | 3.84 |
| 10,000 | Go | 470.532 | 4,579.0 | 10.610 | 12.419 | 99.0% | 79.52 |
| 10,000 | Zig | 6.625 | 92.0 | 0.133 | 0.212 | 99.0% | 39.43 |
| 100,000 | Go | 5312.288 | 34,622.0 | 9.826 | 19.021 | 100.0% | 631.67 |
| 100,000 | Zig | 359.326 | 1,511.0 | 2.259 | 6.563 | 100.0% | 397.25 |

At 100K, Go took **14.78×** as long to build and **4.35×** as long for mean search. Reopen was 22.91× as long; P99 was 2.90× as long. Both measured 100% Recall@10 on the 10 recall queries. The Go primary file was 1.59× the Zig primary file. These are workload/implementation results, not a comparison of the languages in general.

Raw evidence: [Go](go.txt), [Zig 1K](zig-1000.txt), [Zig 10K](zig-10000.txt), [Zig 100K](zig-100000.txt), [generated 100K comparison](comparison-100k.md), [fixture/binary hashes](measurement-inputs.sha256), [final source hashes](final-source.sha256).

## Revisions and environment

- Go: working-tree fixes based on `21d788c9cc1c399f63bab99a958aff0d64ff4058` (v0.11.3).
- Zig measurement: `jeffhajewski/latticedb` main `f8e778359a54c4a79d7a49210bf75aa152f47820`.
- Zig release adapter check: v0.15.0, `9800159e22e200f2b6888c7c6be1810adb695506`. CI resolves the latest release, so its reference can differ from this local main comparison.
- Apple M3, macOS 26.3 arm64, Go 1.27.1, Zig 0.16.0; Go `GOMAXPROCS=2`, Zig `ReleaseFast`.
- One local run per scale. The host was not isolated. These results are descriptive, not a statistical performance guarantee.

## Source provenance

[`go-build-source.sha256`](go-build-source.sha256) records the source snapshot used to compile the measured Go binary (`eedf4a4b6e891964b6303446f448c91076610f91ec2e0eba4f4f68f8e38a4105`). Later changes add a public FTS regression, strengthen the exporter reserved-directory case check, and finalize the reporting/Zig adapter. They do not change the measured Go vector implementation. The final candidate is tested separately.

## Measurement contract

Both adapters use the same binary float32 fixture: normalized 128-dimensional clustered vectors, seed 42, and 100 queries. Both create `Vector` nodes with a `vector` property and a persistent HNSW index, commit batches of 1,000, close the database, reopen it, and verify stored endpoint vectors and index presence/count. Zig cosine HNSW normalizes vectors before storage: its index endpoints are compared with the same native normalization, while the graph property must retain the original float32 bits. Go rejects exact-search fallback. Zig's public `vectorSearch` requires its HNSW index and returns a materialized result slice.

M=16, construction ef=200, search ef=64, K=10. Go uses L2; Zig's native public database uses cosine. Unit-normalized input makes their mathematical neighbor ordering equivalent, although floating-point arithmetic and HNSW construction still differ. Recall uses an exact L2 reference over the same input in both adapters, for the first 10 queries.

Build time includes opening, node/property/index writes, commits, synchronization, and close. Go uses normal synchronous bbolt commits. Zig commits synchronize the WAL and the adapter also calls `sync` before close. This compares each engine's native persistence policy; it does not prove equal power-loss guarantees. Reopen time measures `Open` only. After validation and 10 warm-up searches, 100 materialized searches supply mean and nearest-rank P99. Go's separate `ns/op` sample is not the mean used in the comparison.

Go's record cache and Zig's buffer pool are each configured to 64 MiB. They have different coverage. The input remains in RAM, OS cache is not flushed, and RSS is not capped. In particular, this does not require either engine to fetch every HNSW record from the device for each search. File bytes refer only to the primary database file after build-close, excluding WAL and other sidecars.

The first Zig 10K/100K attempts failed an overly strict adapter check that required raw bits from cosine HNSW storage. Read-only diagnosis found the 10K raw graph property intact and only a normalization rounding difference in the index (0.001549297 versus 0.0015492968). The corrected adapter checks the native normalized value exactly and retains the exact raw-property check. Final measurements use fresh database paths after this repair.

## Storage-path differences

Go's [`PageVectorIndex.GetBounded`](../../../internal/store/page_vector.go) decodes a persisted record on each call, including every neighbor ID and vector float, and allocates the resulting slices. Neighbor selection makes repeated calls. Zig keeps fixed-size HNSW node metadata in a resident hash map and loads it at reopen; vectors and connections use separate page storage. See the pinned Zig [`getNode` and reopen loader](https://github.com/jeffhajewski/latticedb/blob/f8e778359a54c4a79d7a49210bf75aa152f47820/src/vector/hnsw.zig#L719).

A one-second sample during Go's 10K build observed `selectPageVectorNeighbors → PageVectorIndex.GetBounded → binaryDecoder.u → encoding/binary.ReadUvarint`. Repeated record decode/allocation is a concrete follow-up profiling target. This sample does not establish a percentage of total runtime or isolate the effect of caching from distance arithmetic. No performance redesign is included in this correctness change.

## Correctness repairs

| Issue | Change and regression coverage |
|---|---|
| #230 | Export locks move into the reserved `.latticedb-export-locks` directory. Exports cannot replace a live lock through an `X.lock` output or a reserved destination. Symlink publication still uses the canonical target; persistent lock files are not unlinked on release. |
| #231 | New WAL payloads stop at 1 GiB minus the 68-byte header. Header/preflight tests cover the limit without a 1 GiB allocation. Legacy readers retain the old accepted size. |
| #235 | Updating a lower-level HNSW node now descends the upper levels and reconnects its own levels. The regression closes and reopens the page store before checking links. |
| #236 | Fuzzy Frequency takes the largest matching token frequency per query term. Regressions cover resident, indexed page, and stale scan paths; BM25 aggregation remains additive. |
| #237 | Open, rebuild, and update share admission for all retained HNSW namespaces plus new records. A rejected operation preserves usable indexes. |
| #238 | FTS term matching, posting/DF aggregation, sorting admission, and scoring now charge work and check cancellation. The FTS fixture releases its old read snapshot before commit to avoid mmap-growth rejection. |
| #248 | An empty retained sparse chunk directory can create its first chunk again. The 65-remove/re-add regression covers direct chunks, page-overlay deltas, and resident postings. |
| #245 / #246 | Archived reports use explicit UTF-8. The comparison writer returns write/flush/close errors. Disk samples require complete, finite metrics and a valid disk-contract marker. |

The lock namespace is an upgrade boundary: stop older exporters and CSV generation readers that use `<output>.lock` before running the new version. Do not remove lock files while users are active. Stricter search/index accounting can reject workloads that previously escaped their configured budgets.

#247's backup-head rehash remains unchanged. Removing that I/O requires a separate integrity-policy decision; it is not a proven corruption fix. #233 already had bounded stream capacity in the inspected baseline.

## Verification

- Final candidate: `go test ./...`, `go test -race ./...`, and `go vet ./...` all passed. The earlier FTS fixture failure was repaired before this final whole-tree run.
- Conformance module race tests passed.
- Concurrent vector reader/rebuild/cancellation and exporter process/lock tests passed 20 repetitions with race detection.
- Linux, Windows, Solaris, AIX, Plan 9, JS/Wasm, and WASI test binaries cross-compiled; this is not execution on those platforms.
- Python FTS, fresh Zig configuration, and ASCII-locale archived report tests passed.
- The final Zig adapter compiled against main and v0.15.0; the release passed a 1K persistence smoke test. Malformed/oversize inputs and an existing database were rejected without replacing data.
- Same-host allocation gates passed against the unchanged Go baseline. Read-query samples use 10,000 iterations; the other gate samples use 10 iterations, three repetitions each. Ten-iteration query samples were too small to amortize setup allocations. Historical Linux B/op values were not used as the local Mac baseline.

The independent Pro consultation challenged lock identity, lock-file lifetime, execution mode, durability, and size accounting. Source inspection confirmed canonical-target publication, persistent lock files, materialized HNSW results, and WAL synchronization; primary-file labeling and parameter checks address the remaining measurement concerns. Its conditional P0 labels were not treated as demonstrated defects. [Consultation](https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6ace0714-d518-83ee-979d-c027779e502c).

Physical power-loss testing, cold-device runs, constrained-RSS runs, and production soak tests were not performed. No remote branch, PR, release, or issue state was published.

## Reproduction

```sh
mkdir -p results/fixtures
LATTICEDB_VECTOR_FIXTURE_DIR="$PWD/results/fixtures" GOMAXPROCS=2 \
  go test -timeout 120m -run '^$' \
  -bench '^BenchmarkVectorSearchDisk128D$/^(1K|10K|100K)$' \
  -benchtime=1x -count=1 . > results/go.txt

zig build-exe -O ReleaseFast -lc \
  --dep lattice --dep compat -Mroot=scripts/vector-disk-zig.zig \
  --dep compat --dep lattice -Mlattice=zig-reference/src/main.zig \
  -Mcompat=zig-reference/src/compat.zig -femit-bin=results/vector-disk-zig

# Use a new database path for each scale; never replace an existing database.
env -u LATTICE_BUFFER_POOL_MB results/vector-disk-zig \
  results/fixtures/vectors-100000.bin results/zig-100000.db \
  > results/zig-100000.txt 2>&1
```

Fixture creation rejects an existing output file. Use a fresh results directory for each run. The old resident benchmark remains available under `BenchmarkVectorSearchClustered128D`; it is a separate workload.
