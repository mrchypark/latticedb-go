# Disk graph traversal (2026-11-14)

Apple M3, macOS arm64, Go 1.27.1, GOMAXPROCS=2. Go base a4220c3
plus the uncommitted changes in this worktree. A chain of 10,000 nodes and
9,999 directed NEXT relationships was committed in batches, closed and
reopened. These are warm-cache public Cypher queries, including result
materialization, not isolated adjacency operations. Fixture setup is excluded.

`go test -run '^$' -bench '^BenchmarkGraphTraversalDisk$' -benchtime=3x -count=3 .`

| Query | Result rows | Median time | Go bytes/op | Go allocs/op |
|---|---:|---:|---:|---:|
| All paths, one hop | 9,999 | 26.67 ms | 27,578,104 | 599,917 |
| All paths, two hops | 9,998 | 73.59 ms | 61,559,224 | 1,389,909 |
| ID 5000, one hop | 1 | 24.93 ms | 24,310,509 | 580,160 |
| ID 5000, two hops | 1 | 98.70 ms | 58,291,906 | 1,370,153 |
| ID 5000, one hop, ORDER BY both IDs | 1 | 4.87 ms | 3,850,336 | 120,218 |
| ID 5000, two hops, ORDER BY all three IDs | 1 | 9.14 ms | 7,695,797 | 240,335 |

Each measurement is an average of three timed iterations; the table reports
the median of three runs. These runs were not isolated from all machine activity.
The benchmark verifies exact result IDs and cardinality after reopen. Ordered
and unordered cases have different query semantics and must not be treated as
interchangeable in general. The query planner only enables connected-path
reordering when the order includes every visible path ID; this is a candidate
explanation for the large difference, not proof of all costs. CPU profiles
also include setup and runtime activity and do not establish wall-time shares.
The raw Go output is in go.txt.

## Go and Zig v0.17.0

Zig source is 9995f484b24374ee8c96e7fbf1ef6a9fe0ec0196, compiled with
Zig 0.16.0 `-O ReleaseFast`. Its adapter uses the public disk API, commits
one transaction, syncs, closes, reopens, warms each query and measures three
individual invocations. Go commits ten batches and averages three invocations
per reported run. Neither setup nor result validation is timed. Zig's explicit
result deallocation is outside its timed interval; Go garbage collection may
occur inside a timed interval. Zig uses DebugAllocator and its default buffer
pool; Go uses the runtime allocator and its own default cache. The source sets
are equivalent chains, but independently generated and not byte-identical files.

| Query | Go median | Zig median | Go / Zig |
|---|---:|---:|---:|
| All paths, one hop | 26.67 ms | 2.07 ms | 12.9x |
| All paths, two hops | 73.59 ms | 48.95 ms | 1.50x |

Zig verifies expected row count and unique integer IDs, but does not compare
the IDs against the complete expected set. The comparison is preliminary;
it does not establish anchored traversal, cold-cache, fan-out, concurrent,
or end-to-end workload performance. Raw Zig output is in zig.txt.

Reproduce Zig from its source root:

```
zig build-exe -O ReleaseFast -lc --dep lattice --dep compat \
  -Mroot=/path/to/latticedb-go/scripts/graph-traversal-zig.zig \
  --dep compat --dep lattice -Mlattice=src/main.zig -Mcompat=src/compat.zig \
  -femit-bin=/tmp/latticedb-graph-zig
/tmp/latticedb-graph-zig /path/to/nonexistent/database
```

## Single-component planning follow-up

When there is only one connected MATCH component, its cardinality is not needed
to rank it against other components. The planner now skips that unused ranking
pass. Connected-path ordering, query evaluation and persistence are unchanged.
The same disk benchmark and settings gave these medians across three runs:

| Ordered query, ID 5000 | Before | After | Allocations before / after |
|---|---:|---:|---:|
| One hop | 4.87 ms | 2.66 ms | 120,218 / 60,196 |
| Two hops | 9.14 ms | 5.22 ms | 240,335 / 120,305 |

Raw follow-up output is in go-optimized.txt. These are local samples, not an
isolated statistical comparison. The optimization applies to eligible ordered
single-component queries. The unordered anchored queries retain their prior
execution path. Removing planning work can allow a query near its MaxWork limit
to succeed where it previously failed during redundant scans.

## Source-scope reuse follow-up

Source ownership scopes are reused within one query, with at most eight spare
scopes and at most four retained record slots per spare scope. Closing a scope
still releases its unescaped leases; live, escaped and pinned sources retain
their existing accounting. The change does not skip posting scans or record
validation. Focused race and nested ownership tests passed.

In a 10K, warm-cache, unordered ID-anchored workload, three runs of ten
iterations each gave the following per-query allocation counts:

| Query | Before | After | Bytes/op before | Bytes/op after |
|---|---:|---:|---:|---:|
| One hop | 580,159 | 530,171 | 24,310,484 | 23,030,816 |
| Two hops | 1,370,150 | 1,280,172 | 58,331,814 | 55,612,539 |

These are allocated bytes, not peak resident memory. Sequential before/after
and reverse-order samples were affected by unrelated machine load and gave
widely varying latency. They do not establish a CPU speedup or exclude a
regression. Raw files: scope-before.txt, scope-after.txt and the corresponding
scope-*-reverse.txt. The CPU profile and benchmark fixture include setup, so
the profile's allocation shares are not query-only.

## Alternating follow-up

Separate Go test binaries built from the same worktree with and without
source-scope reuse were run in the order before, after, after, before, before,
after, after, before. Each run used 10 timed iterations per query after the
fixture was committed and reopened. The four per-run medians are:

| Query | Before | After | Range before | Range after |
|---|---:|---:|---:|---:|
| One hop | 40.07 ms | 46.23 ms | 26.80–62.46 ms | 34.93–62.98 ms |
| Two hops | 107.51 ms | 118.30 ms | 96.88–126.26 ms | 71.01–147.11 ms |

The spread is large relative to the differences; these observations show no
reliable latency improvement and do not rule out regression. Other machine
activity was not isolated. One-hop allocations were 580,160 before and
530,172 after; two-hop allocations were 1,370,153 and about 1,280,175.
Raw runs are scope-paired-before.txt and scope-paired-after.txt.

## Read-only scan cursor follow-up

Read-only pagestore scans retain a private cursor across callbacks; writable
scans retain the original seek-per-record implementation. If a callback
modifies the owned key bytes, the read-only scan re-seeks using that key to
preserve existing semantics. A focused test covers nested Get and key mutation.
Full tests, go vet and pagestore race tests passed.

In alternating before/after runs (two runs of five iterations), allocations
per unordered anchored query were 530,172 before versus 480,178 after for one
hop, and 1,280,175 versus 1,200,188 for two hops. Times varied greatly:
one hop 43–228 ms before and 65–72 ms after; two hops 109–243 ms before
and 91–178 ms after. These samples do not establish a latency improvement or
exclude regression. Raw files: cursor-before.txt and cursor-after.txt.
