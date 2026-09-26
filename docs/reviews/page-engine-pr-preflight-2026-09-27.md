# Page engine PR preflight

Scope: PR #220 page-engine/incremental-backup candidate, together with the earlier feature-goal changes. No new search features in the paging work.

| Risk | Reviewer concern | Local evidence and action | Status |
|---|---|---|---|
| Two durable authorities | State/WAL disagreement | Records, history, and outbox share one bbolt commit; page path bypasses native WAL | fixed |
| Resident graph | Larger-than-RAM claim without enforcement | 1,418,768,384-byte DB passed creation/index/reopen/mutation/backup/migration/incremental restore under 256 MiB, no swap | verified |
| Snapshot invalidation | Remap deadlock or stale index/stream sources after failed commit | Conservative precommit growth rejection, full graph reload, pinned-reader and retry race regressions | fixed |
| Restore corruption/overwrite | Partial publication, wrong history, existing sidecars | Staged validation, create-only publication, history mismatch and occupied sidecar regressions | fixed |
| Recovery limits | Page migration silently ignores budgets | Byte/frame/work limits enforced; failed migration leaves destination unpublished and retry succeeds | fixed |
| Query semantics | Page lookup changes ordering/budgets | Full query/public/conformance suites; rare-posting and stable-order regressions retain original budgets | fixed |
| Memory boundaries | Arbitrary transaction size or all metadata claimed lazy | Transaction/delta and resident catalog limits explicitly documented; 65 MiB record bound remains | documented residual limit |
| Search scope | Existing HNSW rebuilt into resident memory | Exact page scans preserve results; page HNSW explicitly deferred by user | verified out of scope |
| Portability | bbolt unsupported runtime targets | Runtime stub plus Windows/Solaris/AIX/Plan 9/JS/WASI compile checks | fixed |
| Cleanup and concurrent access | Shared bbolt read transaction race | Per-transaction synchronization, owned scan values, rollback/close error propagation; race suite passes | fixed |

Local root normal/race/vet, conformance normal/race, and CI concurrency regressions (20 repetitions) pass. Native Linux constrained-memory execution passes. No physical power-loss or long-soak claim. Independent review and remote CI remain a separate merge-readiness gate.
