# PR #220 independent Pro review

Reviewed candidate: `562a659296221127b4b563f34713f2c93af59459`, base `bf00f8a711d8615988e008c9d167f9a3a6edd00f`.

[Review conversation](https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46/c/6ab7e023-5fc0-83e8-afdb-0d949f571b9b).

The reviewer received the complete source archive and PR diff, verified 108 postimage blob hashes, and returned **do not merge** with three P1 and six P2 findings. Its environment could not obtain the pinned Go toolchain, so its compatibility-harness tests are advisory evidence, not page-engine integration verification. Local reproduction and final-candidate validation remain required.

| ID | Priority | Finding | Resolution |
|---|---|---|---|
| R1 | P1 | Create then property update emits a patch without the entity creation in incremental backups | Fixed; public create/update, MERGE, nested query, and incremental-restore regressions pass |
| R2 | P1 | Dense FTS text passes write admission but exceeds the persisted decoder tokenization budget | Fixed; exact encoded-size/token expansion admission, contextual writes/import, and unpublished failed-import regressions pass |
| R3 | P1 | Missing checkpoint bypasses recoverable legacy WAL/base during page migration | Fixed; WAL snapshot/base recovery passes with Create false/true; source preserved |
| R4 | P2 | Snapshot backup destination checks omit page sidecars | Fixed; shared destination check and valid stale-sidecar preservation tests pass |
| R5 | P2 | Page posting/cardinality scans bypass query work and cancellation limits | Fixed; raw-posting work charges, cancellation, label/type/degree, and LIMIT regressions pass |
| R6 | P2 | Page property-index construction omits configured build budgets | Fixed; node/edge work and byte exhaustion roll back definitions and postings |
| R7 | P2 | Reopening a non-vector database cannot initialize requested/default vector dimensions | Original initialization repaired; follow-up found R7a/R7b below |
| R8 | P2 | Variable-path delimiter parsing misreads asterisks inside quoted identifiers | Fixed; quote-aware delimiter scan, grammar matrix, and audited parser digest updated |
| R9 | P2 | Streaming migration accepts allocation counters below existing entity IDs | Fixed; checkpoint/WAL observed IDs repair allocation counters, including exhaustion |

## Performance gate

At `b46e321`, all six Linux/macOS/Windows test jobs passed. The concurrent-reader benchmark now measures a successful retry after snapshot backpressure, including retry time. The benchmark suite completes, but unchanged allocation gates fail against the previous resident-memory engine:

| Benchmark | Main B/op | Candidate B/op |
|---|---:|---:|
| Public single-node query | 936 | 2,488 |
| Public write commit | 10,811 | 86,063 |
| Public single-record commit, 100K nodes | 10,614 | 88,870 |
| Internal memory-engine multi-hop query | 83,231 | 114,673 |

The memory-engine callback regression is fixed: local final measurements are 83,298–83,306 B/op and 616 allocs/op, versus the 83,231 B/op and 616 allocs/op baseline. Current-head CI remains authoritative for the gates. Local profiling attributes much of the public page-engine cost to record decoding and bbolt durable page updates; local and CI measurements are different platforms and must not be mixed as a baseline. No gate thresholds or required checks have been relaxed. This review does not establish merge readiness.

At `470c753`, local root normal/race, conformance normal/race, vet, and final focused query normal/race checks passed. All six OS CI test jobs passed. The constrained-memory check passed again in 171.41 seconds (1,418,768,384-byte DB, 256 MiB/no swap, exit 0, OOM false). The memory-engine gate passed at exactly 83,231 B/op and 616 allocs/op. Public page-engine allocation gates remained red.

## Focused Pro review of 470c753

The reviewer marked R1–R6, R8, and R9 resolved, but identified two P1 defects in R7's configuration/backup recovery path:

- **R7a:** Empty configuration delta frames give different vector-dimension transitions the same source history. A same-commit conflicting base can be published before the late head replacement rejects it, damaging the existing archive. Bind the configuration transition into history and reject occupied-commit conflicts before publication.
- **R7b:** Recognizing an already-published pending base returns success without repairing its durable head anchor. Anchor successfully before clearing the pending marker; test stale/missing anchors and missing-tip detection after recovery.

Both repairs are implemented. Configuration commits atomically hash the previous history, commit ID, old/new dimensions, and encoded allocator frame under a separate domain; occupied-commit conflicts are rejected before publication. Pending-base resume repairs the durable head before reporting success. Regression coverage includes copied-source configuration forks, byte-for-byte archive preservation, stale/missing anchors, anchoring failure, and missing-tip rejection. The two reviewer-supplied public-API reproductions also pass independently against the actual bbolt backend through a Go overlay. The reviewer could not obtain the required toolchain for its own compatibility harness; no clean merge verdict has yet been issued.

The R7a/R7b repair candidate passes root normal/race/vet and conformance normal/race suites. The cross-platform anchor-failure fixture additionally passes a focused race run. A focused Pro closure check is pending for these two repairs; performance-gate policy remains unchanged.

## Closure and allocation-baseline decision

The focused Pro review verified the six-file repair manifest and exact diff, and closed R7a/R7b with no actionable residual defect. The production repair is `0024d20`; `f3518b7` changes only the Windows archive-comparison test helper. All six OS test jobs pass at `f3518b7`. Pro inspected code and supplied execution logs; it did not run the pinned Go toolchain itself.

The completed `f3518b7` benchmark still failed the three public allocation gates against the old resident-memory engine. The user authorized a separate baseline if structural costs were established. A write-loop profile identifies bbolt inode/node/meta-page allocation as 67.27% of allocated bytes; public reads decode owned records instead of using a resident graph. The separately versioned CI baseline and evidence are in `docs/benchmarks/page-engine-2026-09-27/`. Existing thresholds and internal memory-engine gates remain unchanged. Avoidable duplicate key copies and unchanged posting rewrites are removed separately.

The performance follow-up found no gate-integrity or ownership defect, but identified a P1 quadratic label-membership scan in the initial posting optimization. Commit `090598d` replaces both membership loops with one `slices.Equal` check; unchanged lists skip posting maintenance and changed/reordered lists use the original full replacement passes. This bounds label iteration linearly without adding a set/cache. Focused actual-bbolt normal/race checks pass; the focused Pro follow-up closed the quadratic-scan finding with no actionable residual defect after inspecting the exact code delta. The reviewer did not independently fetch the pushed commit or execute backend tests in that closure; final CI and large-label execution are recorded in PR qualification evidence.

The 32,768-label actual-bbolt regression passes normally and under race detection: property-only updates stage only the record, reordered labels and the updated scalar property survive reopen, and every label posting is validated. No timing assertions are used.
