# Bounded graph export measurements

`internal/exporter/bounded_export_benchmark_test.go` is copied byte-for-byte
between the baseline checkout and the candidate checkout. The baseline is
`origin/main` at commit `1a1b0bd13ca5f9e735bcc9c1628b392d943bd673`.

The deterministic fixture uses 10,000 and 100,000 nodes and the same number of
edges. Edges are inserted with source IDs descending while edge IDs ascend, so
the stored order is the reverse of the canonical `(source, target, type, id)`
order. Nodes have the fixed label `Node` and empty properties; edges have the
fixed type `edge` and empty properties. The exact first JSON node fixture is
`{"id":"1","labels":["Node"],"properties":{}}`.

The throughput benchmark calls `ExportGraphContextTo` with `io.Discard` for
JSON, JSONL, and DOT. Standard `-benchmem` `B/op` is Go heap allocation per
export; `output-bytes/op` is the exact emitted byte count. CSV is excluded
because the writer API requires a filesystem output path and generation
manifest. Buffered `[]byte` APIs are excluded because they are intentionally
not bounded-output compatibility paths.

First-byte latency is measured at the first nonempty writer call. First-record
latency is measured at the first complete fixture record: the first JSON node
is validated with `json.Valid` after wrapping the emitted node fragment in an
array, the first JSONL line ends at its newline, and the first DOT node line
ends at its newline. The latency writer stops calling the clock after both
markers are captured.

The live-heap diagnostic runs separately at `-benchtime=1x`. It calls
`runtime.GC` and reads `HeapAlloc` before each export, then samples during the
export on the first write and every 4,096 writes, calling `runtime.GC` before
each sample. It takes one final sample and calls `runtime.KeepAlive(graph)`
after that sample. This reports sampled live heap above the pre-export baseline;
it is not a true transient peak because allocations between samples can be
missed.

The selected ordered edge batch holds at most 16,384 edge pointers: 128 KiB on
a 64-bit machine. Sparse ordering roots have a separate 4,096-entry, at most
32 KiB bound per sequential phase; that sparse bound is not concurrent with the
edge batch. Per-record encoding memory is variable with record and payload
size, so these are structural bounds rather than a constant total-memory
claim.

Commands:

```sh
go test ./internal/exporter -run '^$' -bench '^BenchmarkBoundedExport$' -benchmem -benchtime=3x -count=3
go test ./internal/exporter -run '^$' -bench '^BenchmarkBoundedExportLatency$' -benchmem -benchtime=3x -count=3
go test ./internal/exporter -run '^$' -bench '^BenchmarkBoundedExportLiveHeap$' -benchmem -benchtime=1x -count=3
```

Throughput medians on darwin/arm64, Apple M3. JSONL and DOT retain their
previously measured candidate medians because only JSON segments were rerun.
JSON throughput was confirmed with longer sequential baseline/candidate runs
using `-benchtime=1s -count=3` and the anchored selector
`^BenchmarkBoundedExport$/^json$/^(10000|100000)$`.

| Format | Size | Output bytes/op | Time baseline → candidate | Heap B/op baseline → candidate |
| --- | ---: | ---: | ---: | ---: |
| JSON | 10K | 1,235,597 | 6.84 → 6.89 ms (+0.8%) | 5,525,273 → 5,443,195 (-1.5%) |
| JSONL | 10K | 1,515,576 | 21.38 → 21.43 ms (+0.3%) | 15,138,514 → 14,975,960 (-1.1%) |
| DOT | 10K | 615,590 | 2.84 → 2.70 ms (-4.7%) | 949,546 → 785,610 (-17.3%) |
| JSON | 100K | 12,755,601 | 71.18 → 111.66 ms (+56.9%, 1.57x) | 55,994,264 → 54,520,149 (-2.6%) |
| JSONL | 100K | 15,555,580 | 214.91 → 211.27 ms (-1.7%) | 151,749,618 → 150,145,261 (-1.1%) |
| DOT | 100K | 6,555,594 | 46.59 → 28.18 ms (-39.5%) | 9,591,730 → 7,986,098 (-16.7%) |

The 100K JSON result is materially slower than baseline because canonical
ordering costs CPU on the deliberately reverse-ordered fixture. It improves
from about 210 ms with the earlier 4K batch to about 112 ms with 16K, while keeping
the ordering workspace bounded.

First-byte and first-record latency, in nanoseconds/op:

| Format | Size | First byte baseline → candidate | First record baseline → candidate |
| --- | ---: | ---: | ---: |
| JSON | 10K | 5,764 → 1,805 (-68.7%) | 45,416 → 5,208 (-88.5%) |
| JSONL | 10K | 76,375 → 5,514 (-92.8%) | 76,375 → 5,514 (-92.8%) |
| DOT | 10K | 1,625 → 1,792 (+10.3%) | 17,639 → 2,708 (-84.6%) |
| JSON | 100K | 3,056 → 2,861 (-6.4%) | 215,472 → 8,778 (-95.9%) |
| JSONL | 100K | 185,944 → 8,764 (-95.3%) | 185,944 → 8,764 (-95.3%) |
| DOT | 100K | 3,639 → 2,611 (-28.2%) | 263,250 → 4,847 (-98.2%) |

Sampled live-heap medians from the separate 1x diagnostic:

| Format | Size | Sampled live peak baseline → candidate |
| --- | ---: | ---: |
| JSON | 10K | 83,752 → 83,792 B (+0.0%) |
| JSONL | 10K | 86,360 → 4,440 B (-94.9%) |
| DOT | 10K | 83,464 → 1,560 B (-98.1%) |
| JSON | 100K | 804,648 → 133,040 B (-83.5%) |
| JSONL | 100K | 805,976 → 3,272 B (-99.6%) |
| DOT | 100K | 804,360 → 1,544 B (-99.8%) |

Raw outputs:

- Baseline throughput and latency: `/tmp/latticedb-issue111-baseline-bench.txt`
- Baseline live heap: `/tmp/latticedb-issue111-baseline-liveheap.txt`
- Selected 16K candidate run: `/tmp/latticedb-issue111-batch16k.txt`
- Anchored JSON throughput: `/tmp/latticedb-issue111-json-final.txt`
- Anchored JSON latency: `/tmp/latticedb-issue111-json-latency-final.txt`
- Anchored JSON live heap: `/tmp/latticedb-issue111-json-live-final.txt`

- Confirmed JSON throughput: `/tmp/latticedb-issue111-json-stable-before.txt` and `/tmp/latticedb-issue111-json-stable-after.txt`
