# Remaining issue acceptance audit

Scope: open issues #224, #247, #252 and #253 after v0.12.1. This is a
closure audit of named acceptance criteria, not a new whole-codebase review.

## #224: original review ledger

All original follow-up issues were checked through GitHub state and merged
closing references. Historical evidence in #224 is retained as history.

| Issues | Merged implementation |
| --- | --- |
| #222, #225–#229 | #249, `e6a5ae03eda1111393a42825ac076324f5a3bd88` |
| #232, #234, #239–#244 | #250, `21d788c9cc1c399f63bab99a958aff0d64ff4058` |
| #230, #231, #235–#238, #245, #246, #248 | #251, `14bb22611b893d8645308d48ac164b75d37b6730` |
| #233 | #255, `a315593971e25cf14d3a0792a4b76896e91e6837` |
| #40 | #191, `aecd60df725fc4a5112c7776ad312502d920f7f1`; current page LookupLimit stops its visitor at the requested result count |
| #247 | Existing full-rehash policy retained; this follow-up supplies actual hashing byte counts |

Current regression anchors include `TestPageBackupRestoresStreamOperations`,
`TestPageStreamReadPinsGeneration`, `TestDistinctMapKeysNonPowerOfTwo`,
`TestPageRestoreIgnoresPrunedIndependentChain`, `TestMemoryCloseReleasesReferences`,
`TestPropertyIndexChunksReaddAfterEmpty`, and
`TestPropertyIndexLookupLimitReturnsOrderedPrefixAcrossMutations`.
The last two cover the internal sparse-posting failure and ordered lookup contract;
they do not claim every historical internal path is exposed through public APIs.

Closing #224 records completion of its tracked fixes and policy decision. It does
not promise a defect-free database, adversarial cross-user export safety, power-loss
parity with Zig, or new persistent restore-resume functionality.

## #247: integrity policy and measured cost

See [backup integrity policy](../backup-integrity.md). Full rehash remains mandatory
when replacing an existing archive head. `hashArchiveReader` is shared by production
file hashing and the benchmark; the latter reports actual `io.Copy` byte counts.
The metric counts bytes delivered to SHA-256, not physical storage reads. The
corruption test still verifies refusal to replace the old head.

## #253: metadata reload optimization

The optimization shipped in v0.12.1. See
[benchmark and acceptance evidence](../benchmarks/issue-253-2026-11-08/REPORT.md).
This follow-up strengthens failure/recovery, unchanged-metadata and full changefeed
parity coverage. The reported v0.11.3 scaling issue was reproduced separately.
The released immutable commit is available for Rhiza integration. The issue asks
for that candidate, not a claim that Rhiza E2E ran in this repository.

## #252: roadmap deliverable

See [query extension contracts](../query-extension-contracts.md). These are proposed
contracts and bounded follow-up implementation scopes. They do not announce new
query features. Existing #115 planner and #219 corpus evidence are reused; both
are closed, so proposed work must identify its additional scope rather than reopen
completed work without evidence. Future implementations must satisfy their own
backend/API/budget/rollback tests before the supported-feature contract changes.
