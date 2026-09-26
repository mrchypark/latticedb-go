# Page-backed storage status and completion criteria

Public `Open` and `OpenContext` select the bbolt page backend. Graph records
and postings are read on demand, so total database size can exceed RAM. The
legacy resident engine remains an internal migration and testing helper.

## Current storage path

A new directory database stores its bbolt file at `state.json`. Nodes, edges,
labels, edge types, adjacency entries, property-index postings, stream records,
and consumer offsets are persisted in page buckets. Property-index definitions
and stream names, offsets, and retention counters load as small catalogs;
posting and stream-record payloads are read from bbolt on demand. Application
metadata is persisted in bbolt but is currently loaded into the in-memory graph
catalog, so this path does not claim that all metadata is lazy.

The transaction graph keeps only changed nodes and edges in write overlays.
Individual transactions and their deltas still need to fit in RAM; total database
size does not. Application metadata and schema/stream catalogs also occupy RAM.
PageCommit applies records and their derived entries, catalog history, and the
archive outbox in one bbolt write transaction. A bbolt commit publishes them
atomically. Property-index lookup streams ordered posting IDs and checks each
candidate against its actual record, preserving exact results when hashed keys
collide. Existing exact search paths can scan page records; no new search
features or page-backed HNSW implementation is included.

An older v5 state/WAL database is migrated to a staged `state.json.pages`
sidecar, then the staged file is published after import and validation. The
original state and WAL files remain in place. A read-only open with no existing
page sidecar imports into a private temporary directory and leaves the source
untouched. New databases use `state.json` directly for bbolt.

An online snapshot pins its committed bbolt generation while a writer proceeds
independently. If a write could require mmap growth while the snapshot is
pinned, the store conservatively rejects the commit with `ErrResourceLimit`
before publication. The transaction is rolled back; replay the operation in a
new transaction after the snapshot closes. This guard favors snapshot safety
over accepting every write.

The bbolt implementation is currently targeted at Linux, macOS, and Windows.
AIX and Solaris cross-compilation passes; runtime validation is not available. JS/WASI and Plan 9 use an
explicit unsupported-platform stub so the package cross-compiles; opening a
page-backed database there returns an unsupported-platform error.

## Constrained-memory evidence

On 2026-09-27, `TestPageDBMemoryCheck` passed under a 256 MiB Linux cgroup
limit with swap disabled (`GOMEMLIMIT=128MiB`). It created a 1,418,768,384-byte
database with 100,000 small nodes, 99,999 edges, and 1,024 large-value nodes.
The 298.27-second run covered:

- Persistent property-index creation and lookup after close/reopen.
- Random reads, indexed updates, deletion, and snapshot backup.
- Streaming migration of the larger-than-RAM checkpoint into a page database.
- Full-base plus incremental archive restore, including updated index lookup
  and confirmation that a deleted node stays deleted.

The process exited successfully without OOM. Heap after closing the source was
118,496 bytes; that is a post-close observation, not peak RSS. The enforced
cgroup cap is the evidence for the process's memory ceiling during the run.

The opt-in check lives in `internal/engine/page_db_test.go`. Compile that test
package for the target Linux architecture and run its binary with
`LATTICEDB_PAGING_CHECK=1 -test.run '^TestPageDBMemoryCheck$'` inside a container
configured with `--memory 256m --memory-swap 256m`. Ordinary test runs skip it.

## Verified contracts and limits

Focused tests cover old-generation snapshot reads, writer backpressure and
retry, concurrent readers under the race detector, abrupt process exit after
commit, corrupt-record error propagation, property-index/stream reopen,
source-preserving migration, and backup outbox/head interruption and resume.
The public suite and conformance suite exercise the page backend by default.

Individual records, transaction overlays, decoded delta frames, application
metadata, and schema/stream catalogs still consume memory. Large transactions
must fit in available RAM; break bulk ingestion into batches. Migration batches
checkpoint records and index postings, but one legacy delta frame is decoded
as a unit. `Serialize` and APIs returning complete slices also require memory
for their returned values. Use streamed snapshots/exports for large databases.

`MaxDatabaseSnapshotBytes=0` does not cap total database size. An explicit value
is checked at open and commit. Query/search budgets remain logical work and
allocation limits. bbolt reuses freed pages but `Checkpoint` does not shrink
the file. Search optimization and page-backed HNSW remain deferred.
