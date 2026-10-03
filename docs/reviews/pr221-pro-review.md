# PR221 implementation review

The [independent Pro review](https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6ac0e3c9-beb0-83e8-bc69-fbf3ee0d5365) inspected the full diff from `da60461721b8fcf725ce9d187bfc4b906326d709` to `b64db587e893cb9c59f9933baaf716220e090598`. It covered all 33 changed production Go files and relevant callers, and reported 13 executed Linux/amd64 regressions for 12 actionable findings. This supersedes the earlier design-only consultation as implementation evidence. The initial candidate was not approved.

| ID | Defect | Required correction |
|---|---|---|
| LDB221-01 | Synthetic bindings collide across MATCH/CREATE clauses | Give anonymous elements query-part-wide identities; retain deliberate named reuse |
| LDB221-02 | NUL-delimited namespace tuples collide | Use versioned unambiguous tuple encoding |
| LDB221-03 | Reinsertion into an all-deleted HNSW index is unreachable | Establish a live entry with consistent topology and metadata |
| LDB221-04 | Sequential MATCH filters run after later expansion | Apply each clause's complete WHERE before the next MATCH |
| LDB221-05 | Normal HNSW mutations bypass byte bounds | Account for record reads and cumulative staging before mutation |
| LDB221-06 | Namespace invalidation copies all vector payloads | Visit bounded metadata without reading node payloads |
| LDB221-07 | Old FTS term removal bypasses maintenance bounds | Bound old-record reads/decoding and charge cancellable removal work |
| LDB221-08 | Named FTS scan skips work charges for ineligible entities | Charge every visited entity in each scan pass |
| LDB221-09 | Wide FTS candidates exhaust budgets before label fallback | Compare candidate selectivity with required-label populations before materialization |
| LDB221-10 | WITH rejects general expression comparisons | Validate both expression operands against projected bindings |
| LDB221-11 | Computed incomparable comparisons lose unknown state | Preserve the existing three-valued comparison semantics |
| LDB221-12 | Memory initialization skips snapshot-size admission | Enforce the estimate before optional index builds or worker publication |

All corrections require local regression evidence and a fresh review at the pushed candidate before merge. CI also found a Windows FTS test cleanup-order defect, fixed in `f2796407cb1151fb2f64eeb4a836ca53322395da`, and an intermittent memory-adjacency maintenance test failure. The latter required zero residual tombstones even though the worker correctly stops scheduling at one chunk. The corrected test requires settled maintenance, the expected live edges, and at most one chunk of tombstones; 256 deleted edges ensure this still proves that background compaction ran. Focused normal tests passed 20 repetitions, and the focused race test passed. The final candidate still requires CI on all native targets.

## Correction evidence

The follow-up candidate adds public query regressions for anonymous CREATE endpoints, independent anonymous MATCH bindings, clause-order error visibility, WITH expression bindings, and three-valued ordered comparisons. The parser audit was refreshed after the grammar matrix passed.

Page HNSW namespace identifiers use length-prefixed fields. Empty live indexes reset their entry topology before reinsertion. A shared mutation budget accounts for active namespaces and charges actual record writes before encoding and staging. Invalidation scans metadata keys without copying vector payloads. Public tests cover namespace mutation, deletion, reinsertion, and reopen.

Page FTS commits and rebuilds share one maintenance budget. Canonical deletion retains the caller context; old values are admitted before copying, and term decoding and posting changes consume the same budget. Manual and named 1,024-term regressions verify low-budget rejection and atomic rollback. Named scans charge out-of-scope entities. Query candidate tests preserve selective acceleration and release partial unions before label/scan fallback.

Memory initialization checks the logical snapshot estimate before optional index construction and worker startup. Public tests cover fresh memory, deserialized empty/populated databases, read-only admission, and a successful admitted no-op commit.

Final integrated checks and the review verdict are recorded on PR #221 at the exact pushed candidate. These correction notes are not a clean-review claim.

## Second implementation review

Pro verified all 405 repository blobs at `8ce935553982cdded2e5e05e8402df49b0e0d2e5`. All 13 original reproduction tests passed. The expanded review retained LDB221-04 and added three findings, with six failing expanded tests:

| ID | Remaining defect | Follow-up correction |
|---|---|---|
| LDB221-04 | Later MATCH predicates still enter earlier ID/property lookup planning | Give each scope its own lookup predicates while retaining full-plan pagination eligibility |
| LDB221-13 | Selective labeled FTS queries count the entire label posting first | Bound label probes by the candidate information needed to choose a path |
| LDB221-14 | Whole-index drop copies payloads without byte admission; manual rebuild copies and tokenizes canonical data before admission | Use key-only drop traversal and admit source bytes, decoded text, and token storage before allocation |
| LDB221-15 | Partial deletion of duplicate vectors leaves page HNSW results empty or short despite live matches | Restore bounded underfill handling without hiding corruption errors or resetting the query budget |

The reviewer confirmed no actionable defect in the corrected Windows maintenance test. All native CI jobs and the benchmark passed for `8ce9355`; that did not override the failed review gate. The follow-up changes require a new exact-head review and final CI before merge.

The second-round correction isolates lookup predicates per MATCH scope, uses bounded term-frequency reads and label probes for FTS planning, and admits maintenance keys before copying them. Whole-index deletion now reads keys only and charges cumulative deletion staging. Manual rebuild admits the canonical value, decode copies, and tokenizer storage before use. HNSW underfill uses the existing streaming exact path with the same budget; index-read errors still propagate.

Regression coverage includes empty earlier scopes with invalid later parameters, retained-row errors and terminal LIMIT, one and ten selective hits in a 1,000-node label, a 1 MiB canonical FTS source under a 256-byte allowance, whole-index drop rollback, and duplicate-vector deletion/reopen. These are correction evidence, not a clean-review verdict.

## Third implementation review

Pro verified all 407 repository blobs at `020c8a6b2c1bb389b0185100c6dd70cf408083e6`. All six unchanged second-round reproductions and nineteen central correction controls passed. Key admission and 800-record whole-drop controls passed in normal and race modes. LDB221-04, -13, -14, and -15 are closed. The property-index fixture adjustment was accepted. Native CI and the 100K benchmark also passed for this candidate.

Two new findings remain, supported by three failing reproductions:

| ID | Defect | Required correction |
|---|---|---|
| LDB221-16 | Page vector exact/fallback scans and stale manual FTS scans allocate canonical source records outside the request budget | Admit raw copies, decoder/property storage, and FTS tokenization before allocation; hold source bytes through the visitor, then release them; retain cumulative work |
| LDB221-17 | FTS namespace and stale-history readiness invalidation bypass maintenance budgets | Admit visited keys and deletion staging; share one open budget across cleanup and preparation; preserve rollback |

These findings keep the merge gate open. Final correction evidence and the exact-head review verdict are recorded on the PR.

The follow-up source reader admits key/raw storage before copying, uses decoder allocation hooks before constructing strings/collections, and reserves conservative property-normalization copies. Search contexts retain source storage through each visitor and release it afterward. Work remains cumulative across ANN fallback and both BM25 passes. The same source reader covers named node/edge FTS scan paths. Resident records do not incur a page-read allocation charge.

FTS namespace invalidation now uses key-only preadmission, charges every visited key and deletion staging, and serves stale-history cleanup too. Writable open supplies one maintenance budget to cleanup and preparation. The property-index budget fixture admits the now-budgeted readiness inspection, rejects larger node/edge definitions, and admits a small definition; production allowances were not weakened.

## Round 4 follow-up

The independent fourth review verified all 412 blobs and the Git tree at `ead438fa729b47a7c235b1ed03f79ba0ce89a58a`. Its three unchanged reproductions and relevant controls passed, closing LDB221-16 and LDB221-17. It reported one new actionable finding:

| ID | Finding | Correction |
| --- | --- | --- |
| LDB221-18 | Named and configured-property FTS builds copy and decode canonical records before maintenance-byte admission, then read them again | Share the maintenance allowance with admitted source readers; consume each decoded record directly; use admitted point visitors for incremental maintenance |

The follow-up uses the same FTS budget for transient source storage and retained posting staging. Only the source scope's bytes are released after its visitor; work and posting staging remain cumulative. Named node/edge and configured-property rebuilds reuse their admitted records. Incremental updates use admitted point visitors, including missing-record callbacks for derived deletion.

Regressions cover all three public builds with a 1 MiB unrelated property under insufficient and sufficient allowances, named-build commit rollback, 64 records whose combined source size exceeds the peak allowance, cumulative work rejection, and six rebuild/incremental paths with a malformed 1 MiB source. The latter reject on resource admission before decoding at 16 KiB and expose the decode error at 32 MiB. Lower-level node/edge point-read controls also reject before corrupt-source decoding. These are correction evidence; the final exact-head review and CI verdict remain recorded on the PR.

## Round 5 follow-up

The fifth review verified all 414 blobs and the Git tree at `07be25aa0114a0a2150b8d43a8b1b27abac872f0`. Its unchanged source-admission reproduction and 30 selected controls passed, closing LDB221-18. It found one additional writer-side omission:

| ID | Finding | Correction |
| --- | --- | --- |
| LDB221-19 | Repeated physical index-name prefixes and retained encoded values are missing from FTS staging charges | Admit complete keys and bounded encoded values before posting, vocabulary, document, statistics, and readiness mutations; preserve the shared maintenance budget |

The public reproduction uses a 4,096-character configured property with 64 distinct terms. It previously published 546,922 derived key/value bytes under a 128 KiB allowance. The writer now charges full keys and encoded-value upper bounds before allocation/staging, including incremental insertion/removal. Vocabulary reads and decoder allocations use the same budget. Source-scope release remains separate from retained staging.

The existing 1,024-term deletion fixtures prepare their data under an 8 MiB success allowance instead of 1 MiB; their low-budget rejection and rollback assertions remain unchanged. The 64-record source-release control uses 192 KiB instead of 128 KiB to include all retained writer costs, still below its 256 KiB aggregate source bodies. Production limits were not relaxed. Final exact-head review and CI results are recorded on the PR.

Long-prefix regressions now cover 128 KiB rejection, 4 MiB success, short-prefix success at 128 KiB, no partial records in all five derived buckets, unchanged source history after rejected open, incremental update/deletion rollback, and `BufferedBytes()` bounded by both charged bytes and the configured allowance.
