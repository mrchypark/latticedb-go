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
