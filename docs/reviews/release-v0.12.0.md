# v0.12.0 release preflight

Base: v0.11.3 / 21d788c9cc1c399f63bab99a958aff0d64ff4058.
Scope: accumulated correctness fixes, durable vector indexing improvements,
public disk comparison tooling, and measured benchmark reports. Unaccepted bbolt
internals experiments are excluded.

| Risk | Reviewer concern | Local evidence / action | Status |
|---|---|---|---|
| Compact HNSW records | Old records, mutation failure, backup/reopen, float bits | Legacy/direct/split record, CRC/identity, rollback, rebuild/backup/compaction and non-finite input regressions | fixed |
| Cache/cursor ownership | Borrowed values, stale mutation, bucket switching | Owned cache entries; transaction mutex and fresh Seek; mutation and lifetime tests | fixed |
| Rebuild admission | Existing target charged twice before replacement | Track retained bytes by namespace; release only deleted target; explicit rebuild and invalid-metadata open-preparation regressions | fixed |
| Work/byte admission | FTS cells and vector validation bypass limits | Per-loop charging; public budget and cancellation regressions | fixed |
| Search correctness | Fuzzy Frequency rank and lower-level HNSW updates | Memory/disk differential, reconnect and revival tests | fixed |
| Persistence limits | Payload plus header exceeds WAL frame maximum | Shared payload limit and boundary tests | fixed |
| Export synchronization | Output path replaces another export's lock inode | Reserved lock namespace, alias/reserved path and subprocess tests | fixed |
| Sparse property posting | Add after complete deletion panics | Empty directory initialization and chunk-boundary regressions | fixed |
| Benchmark contract | Resident Go versus durable Zig comparison | Public disk APIs, normalized shared binary fixtures, close/reopen, ground truth, fresh/cache provenance | fixed |
| Portability | Optional SIMD and unsupported storage platforms | Scalar default; separate SIMD and portable target builds | fixed |
| Upgrade/downgrade | New HNSW record kinds unreadable by old Go | README/release warning; pre-upgrade backup required for downgrade | accepted residual risk |
| Memory claims | RSS, mmap residency and live heap conflated | Phase-aligned report; no peak-memory claim for cursor change; failed bbolt experiments excluded | fixed |

The preceding benchmark source manifest covers the restored production candidate
before the release-review rebuild-budget correction. Benchmark results describe
that recorded snapshot, not a claim that every release byte was re-benchmarked.
Fresh full-suite, race, vet, checkptr, SIMD, conformance, seven portable-target
compiles and all three Python tool suites passed locally. Remote CI remains a
release gate.
The remote review loop must inspect unresolved and outdated threads, not only
flat comments. Record the final PR, exact revision and CI links in release notes.

## Independent Pro review of 1713918

- R251-1: the parent-wide 0700 lock directory denied a second UID's distinct
  output in a writable shared parent. Reproduced with the public exporter under
  two non-root Linux UIDs. Use sticky shared directory permissions, set through
  the opened directory only on creation, preserve owner-private lock files,
  and reject incompatible existing permissions. Restrictive-umask regression
  checks actual permissions and successful sequential exports.
- R251-2: a pre-existing lock-directory symlink allowed a public export to
  replace an active lock inode. Reproduced through the public exporter. Reject
  reserved lexical destinations before symlink resolution, reject symlinked
  directories and lock files, validate opened-directory identity, and open lock
  files relative to that directory. Regression checks preserve inode identity
  and same-output exclusion.

Both findings failed against the reviewed candidate and pass in the repaired
Linux tests. Existing case/Unicode alias and subprocess locking tests remain.
The shared-directory owner and processes able to replace the namespace must be
trusted to cooperate; the directory is not an adversarial cross-user boundary.
Different UIDs can export distinct names; owner-private locks do not permit
same-name ownership handoff. The README records these limits.

GitHub Codex review was unavailable because the account's code-review quota was
exhausted. Thread-aware inspection found zero review threads; this is not a
fresh clean Codex result. Independent Pro source re-review is required for the
exporter repair; final remote status is recorded on PR #251 and in release notes.
