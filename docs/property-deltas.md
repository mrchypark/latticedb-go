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
| node | flat | 10 | 147,172 → 97,105 ns (-34.0%) | 16,639 → 13,905 (-16.4%) | 105 → 83 (-21.0%) | 2,211 → 637.7 (-71.2%) |
| node | nested | 10 | 306,591 → 207,444 ns (-32.3%) | 40,660 → 13,917 (-65.8%) | 375 → 83 (-77.9%) | 2,778 → 637.7 (-77.0%) |
| edge | flat | 10 | 148,600 → 118,044 ns (-20.6%) | 16,747 → 13,971 (-16.6%) | 105 → 84 (-20.0%) | 2,240 → 642.5 (-71.3%) |
| edge | nested | 10 | 254,252 → 121,465 ns (-52.2%) | 40,871 → 14,091 (-65.5%) | 373 → 82 (-78.0%) | 2,802 → 637.9 (-77.2%) |
| node | flat | 1,000 | 3,052,792 → 1,061,861 ns (-65.2%) | 846,029 → 95,989 (-88.7%) | 3,100 → 85 (-97.3%) | 174,466 → 633.3 (-99.6%) |
| node | nested | 1,000 | 8,744,361 → 544,264 ns (-93.8%) | 4,755,826 → 96,834 (-98.0%) | 33,109 → 90 (-99.7%) | 237,403 → 633.3 (-99.7%) |

The candidate WAL size is effectively independent of unchanged property width:
the width-10 and width-1000 node updates append about 638 and 633 bytes per
operation. The outer property map is still copied in `O(width)` time and
space; unchanged nested values are reused. The benchmark shows substantial
allocation reduction without claiming zero allocation. The small edge-flat row
is from the sequential 1000x/count3 run. Its wall-clock time decreased, while
its WAL and heap metrics also decreased.

The new WAL property-delta kind requires a current reader; an old reader is
expected to reject that new kind. Existing old-format WAL remains readable by
the current reader. Nested values are reused when unchanged, but this is not a
zero-allocation update guarantee:
the changed value, outer map, WAL encoding, and normal transaction structures
still allocate.

Before writing a patch, aggregate limit validation still walks the final property
structure without copying it, so commit work is not independent of nested
element count. Unchanged strings and byte payloads are counted by length.

Recovery computes lazy aggregate totals: unchanged nested values are scanned
once per entity between full replacements, while each property patch clones
the outer map and its touched values. This keeps repeated unchanged nested
value scans out of successive patches without changing full replacement
handling.

Raw measurement outputs:

- `/tmp/latticedb-issue67-baseline-small.txt`
- `/tmp/latticedb-issue67-baseline-wide.txt`
- `/tmp/latticedb-issue67-candidate-final-small.txt`
- `/tmp/latticedb-issue67-candidate-final-wide.txt`
- `/tmp/latticedb-issue67-edgeflat-baseline-1000x.txt`
- `/tmp/latticedb-issue67-edgeflat-candidate-final-1000x.txt`
