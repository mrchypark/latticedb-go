# Successful page commit metadata reuse

Baseline: `14bb22611b893d8645308d48ac164b75d37b6730`.
Candidate: `2770e3159fcf5d5d0ef12652d1d4fe8b0257d4fb` (production and benchmark source).
Host: Apple M3, darwin/arm64, Go 1.27.1.

The baseline was extracted with `git archive` into an isolated temporary directory.
Only `internal/engine/page_metadata_benchmark_test.go` was copied from the candidate.
Each operation commits a property change on one node with unchanged application metadata.
Both versions use durable page storage. Metadata sizes are 0, 256, and 4096 entries.

Command in each source directory:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkPageCommitWithGrowingMetadata$' -benchtime=1s -count=1 -benchmem
```

Raw output: [baseline](baseline.txt), [candidate](candidate.txt).
These two runs overlapped. Wall time includes filesystem synchronization and resource contention.
Do not use the single timing sample as a general speedup claim.
At 4096 entries, allocation falls from 4,335,498 to 96,146 B/op and from 47,665 to 383 allocs/op.

Source inspection establishes that a successful commit skips one metadata-bucket scan.
For 4096 entries, this avoids decoding 4096 metadata records. These are source-derived counts, not instrumented scan counters.
Catalog, search-index, property-index, and stream validation remain in the shared load path.
Commit errors and reopen retain the full metadata scan.

Regression coverage checks changed and deleted metadata, an older reader, unchanged metadata commits,
reopen, property-index and stream results. Existing page commit recovery tests cover rejected commits.
Focused race tests and independent source review passed.

Rhiza SQL/KV/Graph × BeforeAck/Async E2E is not run here.
This report does not establish power-loss safety or cross-platform results.

## CPU profiles

Separate sequential 4096-entry profile runs used the same isolated baseline and candidate:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkPageCommitWithGrowingMetadata/metadata_4096$' -benchtime=1s -count=1 -benchmem -cpuprofile=/tmp/metadata.pprof
go tool pprof -top -cum -nodecount=15 /tmp/metadata.pprof
```

[Baseline run](baseline-profile.txt), [candidate run](candidate-profile.txt),
[baseline CPU top](baseline-cpu.txt), [candidate CPU top](candidate-cpu.txt).
These profile runs overlapped other test activity. They are diagnostic samples, not controlled latency measurements.
The binary profiles remain local in `/tmp/latticedb-issues-{baseline,candidate}-verified.pprof`.

## Reported-version reproduction and acceptance coverage

The original reported release `v0.11.3` resolves to `21d788c9cc1c399f63bab99a958aff0d64ff4058`.
It was extracted with `git archive v0.11.3` into a separate directory. Only the
same benchmark harness was copied into `internal/engine/`. Command:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkPageCommitWithGrowingMetadata$' -benchtime=1x -count=1 -benchmem
```

Observed allocations for 0 / 256 / 4096 metadata entries were respectively
50,200 / 385,944 / 4,299,232 B/op and 291 / 3,821 / 47,574 allocs/op.
This single-operation reproduction confirms the reported release's scaling
problem. Its wall time is not a stable latency estimate. The longer paired
baseline/candidate runs and CPU samples above remain the performance evidence.

Additional acceptance coverage:

- `TestPageCommitReusesCommittedMetadataAndPreservesReloadedState` verifies
  changed/deleted metadata, unchanged-metadata pointer reuse, older readers,
  no-op changefeed parity, index and stream results, and reopen parity.
- `TestPageCommitRecoveryReloadsIndexAndStreamSources` forces a page commit
  rejection with a pinned generation. It asserts a fresh authoritative metadata
  view, the old durable metadata value, unchanged changefeed, and preserved
  index/stream state before a successful retry.
- `TestPageMetadataCommitChangefeedMatchesMemory` applies the same ordered graph
  changes while updating and deleting metadata on memory and page backends,
  then compares all changefeed records, including payloads and sequence order.

Released optimization source is reachable at `a315593971e25cf14d3a0792a4b76896e91e6837`
(`v0.12.1`), for Rhiza's independent integration checks. Providing that immutable
candidate is this issue's integration deliverable. This repository does not claim
that Rhiza's separate SQL/KV/Graph × BeforeAck/Async E2E ran here.
