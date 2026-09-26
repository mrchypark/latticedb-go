# Binary storage format

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

| File | Current | Read-only legacy |
|------|---------|-----------------|
| State | `LDBSTAT5` v5 | v4 `LDBSTAT4`, v3 `LDBSTAT3` |
| WAL | `LDBWAL5` v5 | v4 `LDBWAL4`, v3 `LDBWAL3`, v2 `LDBWAL2` |

State v3/v4 and WAL v2/v3 payloads deserialize through the JSON path. WAL v4
is a legacy binary payload format. Checkpoints write the new state format; all
legacy WALs migrate to a full v5 binary base before appending. Binary format is
**not** readable by pre-v5 state or pre-v4 WAL readers; v5 WAL frames are not
readable by earlier WAL readers. No downgrade safety net. Write always emits
the current version.

## Opt-in backup archive

`OpenOptions.BackupDirectory` captures the recovered generation at open and
stores a standalone full checkpoint for each successful commit. A successful
commit means both WAL and archive publication completed durably. An archive
failure after WAL durability returns `ErrCommitOutcomeUnknown` and fences the
handle; rollback/close must preserve the WAL for recovery. Reopen without that
archive, or with a new archive, when the source has advanced beyond its head.

Each point uses create-only publication. Its filename binds the commit ID,
capture timestamp, and content SHA-256 with a metadata checksum. Restore checks
selector metadata before choosing a point, then validates the selected file,
owner/database ID, canonical snapshot limit, and checkpoint integrity. A damaged
selected point is an error, not a reason to silently choose an older commit.
These checks detect corruption; they are not authentication against an attacker
who can rewrite the entire archive.

`RestoreBackup` publishes to a new path without overwriting an existing database.
`CommitID` selects an exact recorded commit (a pointer permits commit 0); `Before`
selects the latest recorded capture at or before that time. The selectors are
mutually exclusive; omitting both selects the latest point. Returned metadata
reports the selected commit and capture time. Timestamps increase monotonically
across restart and backward clock movement. They describe capture time, not an
exact historical transaction timestamp; no point exists before the first capture.

The archive is locked to one source path/database identity. On reopen, its head
must match the recovered source's committed content and allocation counters may
only have advanced. Same-generation reopen preserves the point and its timestamp.
A source ahead of or behind the archive is rejected: use a new archive after a
disabled-backup period or an outcome-unknown commit that has no matching point.
Restored databases use a different path and must use their own new archive.

This is full-snapshot recovery: every captured commit writes the whole database
synchronously. Storage grows with the number and size of retained points; there
is no automatic retention or incremental WAL shipping. Archive exhaustion stops
successful commits. Keep the archive in a separate storage failure domain when
protection against losing the source device is required. `ponytail:` retain the
native checkpoint format until measured storage or write latency justifies an
incremental archive format.

## Public API

`Serialize` emits the current binary checkpoint and `Deserialize` reads
supported versions. Public JSON and CSV exports retain their existing formats.

## Benchmarks

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
