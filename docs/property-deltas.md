# Property delta benchmark

`internal/engine/property_delta_benchmark_test.go` is identical in the
candidate checkout and the baseline archive at
`/tmp/latticedb-issue67-baseline`. The baseline is commit
`66bbf00c79976cfed59b6661eb032bc3f971dacf`.

The benchmark creates one existing node for every case and adds an existing
edge for the width-10 edge cases. Each entity has a `counter` property and
either 128-byte flat string properties or nested values of the form
`{"payload":{"text":"<128 bytes>"}}`. Each timed operation calls the real
`DB.Update` and `Tx.SetProperty` or `Tx.SetEdgeProperty` APIs to update only
`counter`.

`WALCheckpointThresholdBytes` is set to `^uint64(0)` so automatic checkpoints
do not enter the measurement. `Durability` is omitted, preserving the default
`DurabilityStandard` mode (`fullSync=false`). The benchmark reads
`db.wal.TailSize()` before and after the timed update loop, outside the timer,
and reports the actual appended
`wal-bytes/op`. The bounded runs use 100 iterations for width 10 and 3
iterations for width 1000; they do not create an unbounded WAL.

The relevant commands are:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkPropertyDelta/(node|edge)/(flat|nested)/width_10$' -benchmem -benchtime=100x -count=3
go test ./internal/engine -run '^$' -bench '^BenchmarkPropertyDelta/node/(flat|nested)/width_1000$' -benchmem -benchtime=3x -count=3
```

Baseline medians on darwin/arm64, Apple M3:

| Entity | Shape | Width | Time/op | Heap B/op | Allocs/op | WAL bytes/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| node | flat | 10 | 147,172 ns | 16,639 | 105 | 2,211 |
| node | nested | 10 | 306,591 ns | 40,660 | 375 | 2,778 |
| edge | flat | 10 | 148,600 ns | 16,747 | 105 | 2,240 |
| edge | nested | 10 | 254,252 ns | 40,871 | 373 | 2,802 |
| node | flat | 1,000 | 3,052,792 ns | 846,029 | 3,100 | 174,466 |
| node | nested | 1,000 | 8,744,361 ns | 4,755,826 | 33,109 | 237,403 |

Candidate medians on darwin/arm64, Apple M3:

| Entity | Shape | Width | Time baseline → candidate | Heap B/op baseline → candidate | Allocs/op baseline → candidate | WAL bytes/op baseline → candidate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| node | flat | 10 | 147,172 → 166,106 ns (+12.9%) | 16,639 → 12,241 (-26.4%) | 105 → 68 (-35.2%) | 2,211 → 637.7 (-71.2%) |
| node | nested | 10 | 306,591 → 158,004 ns (-48.5%) | 40,660 → 12,241 (-69.9%) | 375 → 68 (-81.9%) | 2,778 → 637.7 (-77.0%) |
| edge | flat | 10 | 148,600 → 197,896 ns (+33.2%) | 16,747 → 12,296 (-26.6%) | 105 → 68 (-35.2%) | 2,240 → 642.5 (-71.3%) |
| edge | nested | 10 | 254,252 → 137,383 ns (-46.0%) | 40,871 → 12,427 (-69.6%) | 373 → 67 (-82.0%) | 2,802 → 637.9 (-77.2%) |
| node | flat | 1,000 | 3,052,792 → 1,081,083 ns (-64.6%) | 846,029 → 95,173 (-88.8%) | 3,100 → 75 (-97.6%) | 174,466 → 633.3 (-99.6%) |
| node | nested | 1,000 | 8,744,361 → 1,357,361 ns (-84.5%) | 4,755,826 → 94,328 (-98.0%) | 33,109 → 70 (-99.8%) | 237,403 → 633.3 (-99.7%) |

The candidate WAL size is effectively independent of unchanged property width:
the width-10 and width-1000 node updates append about 638 and 633 bytes per
operation. The outer property map is still copied in `O(width)` time and
space; unchanged nested values are reused. The benchmark shows substantial
allocation reduction without claiming zero allocation. The small edge-flat row
is from the sequential 1000x/count3 run. Its wall-clock time increased, while
its WAL and heap metrics decreased.

The new WAL property-delta kind requires a current reader; an old reader is
expected to reject that new kind. Existing old-format WAL remains readable by
the current reader. Nested values are reused when unchanged, but this is not a
zero-allocation update guarantee:
the changed value, outer map, WAL encoding, and normal transaction structures
still allocate.

Before writing a patch, aggregate limit validation still walks the final property
structure without copying it, so commit work is not independent of nested
element count. String contents are validated as UTF-8 and vector values are
checked for finiteness without allocating normalized copies.

At most 64 distinct property keys per entity are tracked in one transaction.
Larger sets use the full-record path, bounding duplicate-key tracking work
while preserving the resulting values and changefeed events.

Recovery computes lazy aggregate totals: unchanged nested values are scanned
once per entity between full replacements, while each property patch clones
the outer map and its touched values. This keeps repeated unchanged nested
value scans out of successive patches without changing full replacement
handling.

Raw measurement outputs:

- `/tmp/latticedb-issue67-baseline-small.txt`
- `/tmp/latticedb-issue67-baseline-wide.txt`
- `/tmp/latticedb-issue67-candidate-sealed-small.txt`
- `/tmp/latticedb-issue67-candidate-sealed-wide.txt`
- `/tmp/latticedb-issue67-edgeflat-baseline-1000x.txt`
- `/tmp/latticedb-issue67-candidate-sealed-edgeflat-1000x.txt`
