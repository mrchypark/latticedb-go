# Binary storage format

State files and WAL frames use a 64-byte header followed by a streaming
binary payload.  The header carries magic, version, payload length, and
a CRC-32 IEEE checksum. File recovery decodes through a TeeReader that
computes CRC incrementally and verifies it before accepting the payload.
The in-memory Deserialize API verifies CRC before structural decoding.

## Header layout (64 bytes)

Bytes 0–7: magic.  8–9: payload version uint16 BE.  10–11: header size
(uint16, always 64).  12–19: commit ID uint64 BE.  20–27: payload byte
length uint64 BE.  28–31: CRC-32 IEEE of payload.  32–63: database ID,
exactly 32 hex ASCII bytes.

**State** magic `LDBSTAT5` v5.  **WAL** magic `LDBWAL4` v4.

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
| WAL | `LDBWAL4` v4 | v3 `LDBWAL3`, v2 `LDBWAL2` |

Legacy payloads deserialize through the JSON path. Checkpoints write the
new state format; legacy WALs migrate to a full binary base before appending.  Binary format is **not** readable by pre-v5 state or pre-v4
WAL readers; no downgrade safety net.  Write always emits current version.

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
