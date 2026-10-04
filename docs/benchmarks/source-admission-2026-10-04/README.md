# Query source admission contract

PR #249 fixes #222 by reserving disk buffers and decoded payload before allocation, retaining the reservation through live query rows and mutation dependencies, and reclaiming temporary reads. The previous disk query benchmark omitted this required ownership work. Comparing it as if the implementation contract were unchanged would penalize the correction.

Paired local Go 1.27.1 Darwin/arm64 measurements against v0.11.1, using the existing fixture and query, show `BenchmarkReadRequests/query` moving from 43 allocs/op and 2392 B/op to 63 allocs/op and 3568 B/op. These are allocation measurements, not latency or RSS claims. Resident `BenchmarkQueryMultiHopSlots` remains at 616 allocs/op, with median bytes moving from 83231 to 83391 (+0.19%), within the existing 1% gate. Unnecessary resident tracking, boxed rows, and repeated context creation were removed before changing the disk comparison policy.

The disk query emits `1 source-admission-contract`. Comparing a legacy result without that marker to contract 1 allows at most 20 extra allocations and 1280 extra bytes per query. This is a fixed, bounded transition allowance with byte headroom for portability. It applies only to the disk query, only when moving from legacy to contract 1. Allocation counts and bytes remain enforced; excess migration cost fails CI. Unchanged contract 1 comparisons immediately restore the original zero-allocation-growth and +1% byte gates. Removing or changing an existing marker fails instead of returning to the legacy policy. No other performance gate changes.

Reports continue to show the full unadjusted differences against historical page-engine results. Raw CI artifacts retain the contract marker for the next main comparison. Unit tests cover both transition ceilings, strict subsequent comparisons, marker downgrade/removal, and invalid or mixed marker samples. Merge still requires the full benchmark workflow and independent review of this policy.

## Reproduce the paired allocation samples

The four raw text files in this directory contain three samples per benchmark. The legacy checkout is exactly `a6f28b5fd06b638dd7626d741151dff4a425aeb6` (v0.11.1), extracted with `git archive`. The admitted measurements use the PR #249 candidate that commits these files, after the R249-08 through R249-12 corrections. Both checkouts used Go 1.27.1 on Darwin/arm64, Apple M3, with `GOMAXPROCS=2`. Legacy and admitted measurements ran sequentially on the same host. No source mutation tests ran during these measurements.

Run these commands in each checkout:

```sh
GOMAXPROCS=2 go test . -run '^$' -bench '^BenchmarkReadRequests$/^query$' -benchmem -benchtime=1s -count=3
GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench '^BenchmarkQueryMultiHopSlots$' -benchmem -benchtime=1s -count=3
```

Reported allocation and byte figures are medians of the three raw samples. Timing is retained for transparency but is not evidence for a latency improvement or gate change. These samples demonstrate measured contract cost, not proof of a minimum possible cost. Historical reports retain the archived disk baseline. The actual CLI gate chooses an admitted previous-main query when available; its per-sample marker must be valid and consistent, so a static legacy report baseline cannot make the transition allowance recur.
