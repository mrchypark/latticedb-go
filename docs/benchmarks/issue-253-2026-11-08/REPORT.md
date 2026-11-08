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
