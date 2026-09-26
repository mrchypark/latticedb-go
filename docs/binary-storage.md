# Binary storage format

Public `Open` now always uses the page backend. A new directory database stores
bbolt pages at `state.json`; the `LDBSTAT5` and `LDBWAL5` formats below describe
legacy native files used for migration and the binary checkpoint interchange
path, not the ordinary runtime store.

When a legacy v5 state/WAL database has no page sidecar, `Open` imports it into
a staged `state.json.pages` file and publishes that file after validation. The
source state and WAL remain untouched. A read-only open without a page sidecar
uses a private temporary directory for the import. Subsequent opens use the
published page sidecar. New databases use `state.json` directly for bbolt.

State files use a 64-byte header. WAL v5 frames use a 68-byte header followed
by a streaming binary payload. The WAL header carries magic, version, payload
length, a payload CRC-32 IEEE checksum, and a header CRC-32 IEEE checksum.
Recovery verifies the v5 header checksum before trusting the payload length,
then decodes through a TeeReader that computes and verifies the payload CRC.
The in-memory Deserialize API verifies CRC before structural decoding.

## Header layout

State and WAL v2–v4 headers are 64 bytes: 0–7 magic, 8–9 payload version
uint16 BE, 10–11 header size uint16 BE, 12–19 commit ID uint64 BE, 20–27
payload length uint64 BE, 28–31 payload CRC-32 IEEE, and 32–63 database ID
(exactly 32 hex ASCII bytes). WAL v5 appends bytes 64–67: CRC-32 IEEE of
bytes 0–63. It is checked before any use of the payload length.

**State** magic `LDBSTAT5` v5. **WAL** magic `LDBWAL5` v5.

## Payload encoding

Integers are unsigned varints; property int values use signed varints.
Strings: uvarint byte count + raw UTF-8.  Byte slices and collections
use a presence length: 0 = nil, N+1 = N elements.  Element-size hints
for `d.length` are conservative fixed-struct estimates; variable-length
fields are reserved separately by the decoding primitives.  Property
values use a single-byte tag: 0 empty, 1 null, 2 bool, 3 int64 (signed
varint), 4 float64 (8-byte BE), 5 string, 6 bytes, 7 vector (float32
BE), 8 list, 9 map (sorted keys).  Flags are strict 0/1; values > 1
set `d.err` and abort decoding.

## Bounded decoding

Every collection decode calls `d.length(elementBytes)` which subtracts
from the pre-reserved allocation budget.  Exceeding the budget sets
`ErrLoadResourceLimit` without allocating.  `d.finish()` rejects trailing
data.  The budget is a logical allocation estimate — it does not measure
actual decoded byte counts or process heap pressure.

## Migration and compatibility

| Input format | Current binary version | Older binary versions |
|------|---------|-----------------|
| State checkpoint | `LDBSTAT5` v5 | v4 `LDBSTAT4`, v3 `LDBSTAT3` |
| WAL segment | `LDBWAL5` v5 | v4 `LDBWAL4`, v3 `LDBWAL3`, v2 `LDBWAL2` |

State v3/v4 and WAL v2/v3 payloads deserialize through the JSON path. WAL v4
is a legacy binary payload format. The page importer accepts the v5 checkpoint
and supported WAL history for automatic migration; do not infer that every
older binary version is automatically migrated by public `Open`. Binary format
is **not** readable by pre-v5 state or pre-v4 WAL readers; v5 WAL frames are
not readable by earlier WAL readers. No downgrade safety net. Binary
serialization continues to emit the current version.

## Opt-in backup archive

`OpenOptions.BackupDirectory` captures a full base at open, then stores the exact
committed WAL frame for each successful transaction. A successful commit means
both the page transaction and archive publication completed durably. Records,
commit history, and the pending archive frame share one atomic page transaction.
An archive failure after page durability returns `ErrCommitOutcomeUnknown` and
fences the handle; reopen uses the durable outbox to finish publication.

Bases and WAL segments use create-only publication. Each segment binds its commit
range, capture timestamp, SHA-256 and predecessor digest. Restore validates the
selected dependency chain and restored commit/history before installing a new
page database without overwriting an existing database. Missing or corrupt required
objects are errors, never a reason to silently choose an older commit. These
checks detect corruption, not an attacker who can rewrite the whole archive.
Legacy standalone full-checkpoint archives remain readable.

`CommitID` selects an exact recorded commit (a pointer permits commit 0); `Before`
selects a recorded capture at or before that time. The selectors are mutually
exclusive; omitting both selects the latest point. Returned metadata reports the
selected commit and capture time. Timestamps increase monotonically across
restart and backward clock movement. Capture time is not an exact historical
transaction timestamp; no point exists before the first capture. Independent
resume bases include a checksummed coverage-gap start time. A `Before` selector
inside that interval fails rather than silently returning the previous point.

The archive is locked to one source path/database identity. Reopening at the
same commit verifies its contents and allocation counters and preserves its
capture timestamp. A source behind the archive or a conflicting state at the same
commit is rejected. When the source has advanced beyond the archive, a new full
base records its current state independently. Older recovery points remain intact;
missing intermediate commits are not fabricated. WAL v5 has no persistent history
hash or original commit timestamp, so replaying retained frames to an equal final
state cannot prove historical ancestry. This version therefore does not claim
historical catch-up across an unarchived interval. Restored databases use their
own archive.

Normal commit I/O scales with the committed WAL frame rather than the full DB.
Page archives carry immutable source-history proofs. Restore streams the base
and replays deltas into an unpublished page database, preserving that history.
Legacy archives without proofs retain their existing recovery path. There is
no automatic retention. A recovery point requires its base and all predecessor segments, so
deleting individual files can invalidate later points. Archive exhaustion stops
successful commits. Use a separate storage failure domain when protection against
losing the source device is required.

## Public API

`Serialize` emits the current binary checkpoint and `Deserialize` reads
supported versions. Public JSON and CSV exports retain their existing formats.
The public file-backed `Open` path stores new databases as bbolt pages and
imports a serialized/legacy checkpoint when opening it as a file path.

## Benchmarks

The following measurements describe the earlier binary/resident-engine
candidate. They are historical format and allocation benchmarks, not
performance measurements of the current page-backed public `Open` path.

Baseline `1fa1f72` vs candidate, Apple M3, medians of 3.

```sh
go test . -run '^$' -bench '^BenchmarkDeserialize$/^nodes_100000$' -benchtime=1x -count=3 -benchmem
go test ./internal/store -run '^$' -bench 'BenchmarkLoadLatestWALV2/delta_history/256' -benchtime=100ms -count=3 -benchmem
go test . -run '^$' -bench 'BenchmarkSingleRecordCommitScaling/nodes_100000/direct' -benchtime=20x -count=3 -benchmem
```

**100 K-node deserialize** (`BenchmarkDeserialize nodes_100000`):

| Metric | Baseline | Candidate | Δ |
|--------|----------|-----------|---|
| Time | 598 616 000 ns | 180 420 000 ns | −70 % |
| Bytes/op | 415 226 808 B | 181 045 936 B | −56 % |
| Allocs/op | 2 212 490 | 1 612 332 | −27 % |
| Serialized | 15 267 055 B | 3 358 863 B | −78 % |

**256-frame WAL load** (`BenchmarkLoadLatestWALV2/delta_history/256`):

| Metric | Baseline | Candidate | Δ |
|--------|----------|-----------|---|
| Time | 868 110 ns | 524 797 ns | −40 % |
| Bytes/op | 724 613 B | 453 328 B | −37 % |
| Allocs/op | 5 390 | 4 620 | −14 % |

**100 K-node direct commit, 20×** (`-benchtime=20x`):

| Metric | Baseline | Candidate | Δ |
|--------|----------|-----------|---|
| Time | 43 854 ns | 33 508 ns | −24 % |
| Bytes/op | 11 617 B | 10 507 B | −10 % |
| Allocs/op | 61 | 53 | −13 % |

Timing noisy at 20 iterations; allocation and size improvements stable.
