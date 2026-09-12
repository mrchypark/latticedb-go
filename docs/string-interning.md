# Repeated graph strings

Property keys, nested string values, node labels, and edge types can share
backing storage through Go's `unique` package. Direct writes, query mutations,
and canonical recovery retain ordinary strings; public values and serialized
formats are unchanged. Empty-label behavior is preserved.

The standard library's weak table can reclaim entries during GC. Sharing is
best effort across GC cycles, not a permanent identity guarantee. Strings over
1 KiB bypass interning to avoid cloning large one-off values. Query mutation
normalization reserves short-string/key copies before admission, conservatively
including cache hits. There is no new database option or persistent dictionary.

## Measurements

Apple M3, baseline `0d25642`, the same benchmark source on both revisions.
Each fixture has two repeated labels, four distinct repeated 512-byte values
per node, and an edge with repeated type/property text for every two nodes.
Medians of three runs, three iterations each:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkRepeatedStringGraph(Open|Retained)$' -benchtime=3x -count=3 -benchmem
```

| Nodes | Metric | Baseline | Interned |
|---:|---|---:|---:|
| 1,000 | Go heap retained after GC, bytes | 8,950,744 | 6,650,216 |
| 10,000 | Go heap retained after GC, bytes | 42,385,840 | 19,350,256 |
| 1,000 | Read-only Open + Close, ns/op | 70,177,014 | 67,723,528 |
| 10,000 | Read-only Open + Close, ns/op | 230,961,542 | 249,896,139 |
| 1,000 | Open + Close, B/op | 96,821,362 | 96,824,565 |
| 10,000 | Open + Close, B/op | 357,961,589 | 357,966,237 |
| 1,000 | Open + Close, allocs/op | 334,556 | 334,662 |
| 10,000 | Open + Close, allocs/op | 1,199,036 | 1,199,150 |

The live heap decreased 25.7% and 54.3%. Total decode allocation stayed almost
unchanged; JSON decoding still creates transient strings before normalization.
The 10K reopen median increased 8.2%, while the 1K median decreased 3.5%.
These synthetic repeated-string results do not predict unique-string workloads.

For a separate maximum-RSS check, compile each revision's engine test binary
and run `/usr/bin/time -l` with only the 10K retained-heap benchmark, one
iteration in each fresh process. Three alternating baseline/candidate runs gave
median maximum RSS of 540,065,792 / 545,112,064 bytes. Peak RSS did **not** improve;
it includes fixture construction and temporary loading buffers, unlike the
post-GC live-heap measurement. All fixtures stay below the 100K benchmark limit.
