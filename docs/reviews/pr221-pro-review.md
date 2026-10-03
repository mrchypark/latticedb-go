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
