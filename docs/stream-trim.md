# Stream trim

`StreamStore.Trim` advances the retained sequence boundary logically. It
subtracts the removed records' logical and snapshot byte totals and keeps the
immutable chunks shared until physical tail bytes minus retained log bytes
reach the retained logical bytes. At that point the visible records are
compacted into fresh chunks. Trimming the final record clears the tail
immediately.

The byte threshold prevents a small record count from retaining a large removed
payload indefinitely. Between compactions, the hidden prefix is bounded below
the current retained logical bytes; chunk slices hold at most 64 records each.
Physical tail-byte saturation forces the guarded compaction path. Reads,
persistence, recovery, and both accounting paths apply the logical first
sequence and never serialize or count the hidden prefix.

The issue workload is 100 successive head trims per operation. The supplied
parent baseline was measured on darwin/arm64, Apple M3, with Go 1.27.1 using a
100 ms benchmark window:

| Records | Time/op | Bytes/op |
| ---: | ---: | ---: |
| 1,000 | ~1 ms | 4.2 MB |
| 10,000 | ~14 ms | 44.9 MB |
| 100,000 | ~152 ms | 467 MB |

For a bounded comparison, the parent and worktree were then run sequentially
with `-benchtime=3x -count=3`:

| Records | Parent median | Worktree median | Parent B/op | Worktree B/op |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1.454 ms | 54.8 µs | 4,192,301 | 1,040 |
| 10,000 | 19.407 ms | 48.9 µs | 44,866,216 | 1,040 |
| 100,000 | 220.877 ms | 91.7 µs | 467,158,765 | 1,040 |

The publish scaling benchmark was run sequentially with
`-benchtime=1s -count=3`. Medians remained at 9 allocs/op:

| Base records | Parent ns/op | Worktree ns/op | Parent B/op | Worktree B/op |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 370.8 | 374.2 | 2,904 | 2,920 |
| 10,000 | 384.8 | 373.5 | 1,816 | 1,832 |

The two cumulative counters add 16 B per new chunk. An initial 10,000-iteration
run showed 1.67–2.05x slower publish timing; the longer runs above did not
reproduce that increase (medians +0.9% and -2.9%). These local timings are
host-sensitive; allocation counts are unchanged.

Trim benchmark command:

```sh
go test ./internal/store -run '^$' -bench '^BenchmarkRepeatedTrim$' -benchmem -benchtime=3x -count=3
```

Publish benchmark command:

```sh
go test ./internal/store -run '^$' -bench '^BenchmarkStreamScaling/(publish_1000|publish_10000)$' -benchmem -benchtime=1s -count=3
```
