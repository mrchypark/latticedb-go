# Group commit

`DB.Batch` and `DB.BatchContext` coalesce independent concurrent write
requests into a single WAL commit. A successful group shares one WAL
commit and synchronization; a callback failure rolls back the group in full.

## Coalescing rules

- A batch collects pending callbacks during a first-collection window of
  up to 1 ms after the first call joins. Reaching 32 queued triggers an
  immediate flush. The queue holds up to **32 waiting** requests even if
  zero are executing; beyond this limit → `ErrResourceLimit`.
- Callbacks in a group run sequentially against the same managed transaction
  and observe earlier callbacks’ mutations. A committed group shares one commit ID.

## Durability

Success is returned only after WAL synchronization finishes. `DurabilityFull` uses the platform's
stronger persistence primitive; `DurabilityStandard` uses the standard one.
The configured durability guarantee is identical to `Update`.

## Error and cancellation semantics

- **Any callback error, cancellation observed before WAL commit, or
  `runtime.Goexit` aborts the whole group.** Errors may originate from a peer. No automatic retries.
- **Cancel after WAL write begins** may still return `nil` or
  `ErrCommitOutcomeUnknown` — same as `Update`. An unknown outcome fences
  further writes until reopen; recovery may find the entire group or none of it.
- **Context cancelled but callback running:** the caller must wait until
  the callback returns; cancellation does not interrupt in-flight work.
- **Callback panic:** reraised on the own-Batch caller after rollback.
  Peer callbacks receive a group-level error.

## Relationship to existing API

- `Begin(false)`, `Update`, `UpdateContext` fail fast
  with `ErrWriteTxActive` when a write transaction is already active.
  Readers continue to see the previous committed snapshot during the batch.
- `BeginWriteContext` retains its waiting-for-writer contract; callers
  may block until ctx is cancelled.
- `Batch`/`BatchContext` callbacks must not retain the provided `Tx`
  after the callback returns or start a nested batch on the same `DB`.
  Callbacks must not perform irreversible external side-effects because
  the group may be rolled back by a peer.

## Queue-full and close

- Queue full → immediate `ErrResourceLimit`.
- When `Close` wins the writer slot, pending batch requests receive
  `ErrDatabaseClosed` without invoking their callback. Active batches
  count as `activeTx`; normal close behaviour applies.

## Single-caller path

A sequential single caller still incurs the 1 ms collection delay with
no amortisation benefit. Use `Update` or `UpdateContext` instead.

## Measured batching effect

Apple M3, darwin/arm64, median of three 100-iteration runs. Each iteration
creates 32 nodes, either through 32 sequential `Update` calls or 32 concurrent
`Batch` calls. Both modes use the same `os.File.Sync` WAL hook to count actual
synchronizations. These filesystem-dependent timings do not predict every
platform's default synchronization latency.

| Metric | Individual | Batch |
|---|---:|---:|
| Time per request | 2,789,671 ns | 136,311 ns |
| WAL synchronizations per request | 1 | 0.03125 |
| Allocated bytes per 32 requests | 424,602 | 266,183 |
| Allocations per 32 requests | 2,017 | 1,347 |

Timer scheduling can split a group: one run used 101 synchronizations for
3,200 requests rather than 100. All requests were acknowledged durably.

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkGroupCommit$' -benchtime=100x -count=3 -benchmem
```
