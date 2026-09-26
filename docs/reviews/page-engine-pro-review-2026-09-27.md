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
| R7 | P2 | Reopening a non-vector database cannot initialize requested/default vector dimensions | Fixed; validate before atomic configuration commit; durable base marker resumes publication without bypassing archive ancestry checks |
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

Final-candidate root normal/race, conformance normal/race, and vet validation are in progress. A focused Pro re-review of the repairs is pending.
