# Feature completion design review

The [Pro consultation](https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6ac0ca0a-c5a0-83ee-80a1-803caaa7cc2b) reviewed two design boundaries: true-memory execution and physical compaction, then page-backed HNSW and scoped full-text indexes. It inspected the public v0.10.0 source (`da60461721b8fcf725ce9d187bfc4b906326d709`) and the proposed design. It did not execute or approve the local patch.

The implementation and local review use these acceptance conditions:

- Memory mode bypasses filesystem persistence explicitly. WAL failures in disk mode retain their existing error behavior. Bounded adjacency maintenance remains active without a WAL, and pinned generations remain immutable.
- Compaction retains the outer path lock, refuses unlocked databases and active leases, releases the internal page reader, copies all buckets, publishes and reopens the actual page path, clears record caches, and recalculates mmap capacity. Publication uncertainty fences further work.
- Nondefault page size reaches the migration staging file before publication. Either native meta page can identify each supported size.
- Page-backed index reads and read-modify-write operations use the same native transaction as their graph view or staged changes. Derived-only rebuilds publish a fresh native reader without changing logical history.
- A versioned search-generation stamp binds disposable indexes to source history. An unaware older writer changes that history, so stale readiness cannot authorize search. Writable open rebuilds; read-only paths fall back or report required maintenance.
- FTS definitions use tracked metadata mutations. Schema-only changes therefore enter incremental frames. Full snapshot replay clears every derived search bucket and the generation stamp.
- Search and build bounds include scans, decoder allocations, candidates, old-record cleanup, and repeated staged writes across all configured indexes. A final index-size estimate alone is insufficient.
- Public page FTS scoring is the comparison oracle for new memory and indexed paths. Fuzzy frequency sums matching occurrences; fuzzy BM25 counts each document once per query term. Query candidates must be complete before later filtering and LIMIT.

Local fault and differential tests provide the implementation evidence. Tests run before a later change do not validate that later change; the final candidate must pass the repository checks separately. Process interruption tests do not prove physical power-loss durability.

## Local candidate validation

On 2026-10-03, the root normal/race suites and vet passed on macOS arm64.
The conformance module passed normal and race runs. CI's vector and exporter
race repetitions passed 20 runs each. Parser fuzzing ran for five seconds with
one worker without failure. Root test binaries cross-compiled for Linux,
Windows, Solaris, AIX, Plan 9, JS/wasm, and WASI. Both benchmark provenance
checks and `git diff --check` passed. These results validate the local candidate;
they are not remote CI or a release approval.

## PR preflight ledger

| Risk | Reviewer concern | Local evidence and action | Status |
|---|---|---|---|
| Storage replacement | Lose history, IDs, snapshots or path lock during compaction | Fault, lease, ID and archive-chain tests; retain outer lock and fence uncertain publication | fixed |
| Cache and index invalidation | Use stale generation or indexes left by an older writer | Generation cache tests and source-history mismatch regression; invalidate inactive configurations | fixed |
| Memory execution | Hidden files or missing adjacency maintenance | File-free lifecycle and maintenance normal/race tests | fixed |
| Search bounds | Allocate before checking budgets or publish partial builds | Bounded native reads, decoder tests, aggregate build rollback and pinned-reader retry tests | fixed |
| Query contracts | Hide predicate errors or create partial paths | Grammar, clause-order, mutation rollback, indexed/scan comparisons and conformance tests | fixed |
| Schema recovery | Omit schema-only deltas or retain derived state after restore | Incremental schema-only restore and Serialize/Deserialize tests; clear derived buckets | fixed |
| Portability | Unsupported target breaks package compilation | Seven-target cross-compilation; native Linux/Windows execution delegated to CI | accepted residual risk |
| Scale evidence | Treat logical limits or old benchmark data as new RSS evidence | Documentation separates old 256 MiB run and resident benchmarks from new search code | accepted residual risk |

The user authorized the PR, review fixes, merge after clean review and CI, and
release publication on 2026-10-03. A fresh Pro review of the published candidate
is required before merge; the earlier design consultation does not satisfy it.
