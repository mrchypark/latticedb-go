# Performance benchmark report

Current: `Go working tree based on 21d788c`  
Previous: `No historical disk baseline`

Values are medians of the samples shown in each row; sample counts are current / previous. Δ is current versus previous; ns/op is informational because shared-runner latency is noisy, while graph-core B/op and allocs/op remain enforced (including multi-hop). WAL recovery latency is informational; its allocation and byte metrics remain gated.

## Go vs Zig public disk APIs (100K)

Zig reference: `Zig main f8e7783`

| Metric | pure-Go | Zig | pure-Go vs Zig |
|---|---:|---:|---:|
| Durable build + close (ms) | 5312288 | 359326 | +1378.4% |
| Mean search (ns) | 9825810 | 2258672 | +335.0% |
| P99 search (ns) | 19020583 | 6562792 | +189.8% |
| Reopen (ms) | 34622 | 1511 | +2191.3% |
| Primary database file bytes (sidecars excluded) | 662355968 | 416546816 | +59.0% |
| Recall@10 | 100.0% | 100.0% | +0.0 pp |

Both paths use public persistent DB APIs, 1000-vector commit batches, close/reopen, identical binary input, and warm single-client ANN search. Build includes commit and close. Go record cache and Zig buffer pool are both configured to 64 MiB but do not have identical semantics; OS cache and process RSS are not capped. This is not an OS-cold test. Compare recall alongside latency; do not infer language-wide superiority.
Go uses L2 and Zig uses cosine on unit-normalized input; both recalls use exact L2 ground truth. Native distance arithmetic, index layout, and durability implementations still differ. Primary-file bytes exclude sidecars.

| Benchmark | n cur/prev | ns/op current | previous | Δ | B/op current | previous | Δ | allocs/op current | previous | Δ | Other current / previous (Δ) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `BenchmarkVectorSearchDisk128D/100K` | 1 / 0 | 10453041 | — | — | 4731240 | — | — | 30118 | — | — | 1 disk-api-contract, 662355968 file-bytes, 5312288 index-build-ms, 9825810 mean-ns, 19020583 p99-ns, 100 recall@10, 34622 reopen-ms |
| `BenchmarkVectorSearchDisk128D/10K` | 1 / 0 | 9721708 | — | — | 2770640 | — | — | 19804 | — | — | 1 disk-api-contract, 83378176 file-bytes, 470532 index-build-ms, 10609559 mean-ns, 12418875 p99-ns, 99 recall@10, 4579 reopen-ms |
| `BenchmarkVectorSearchDisk128D/1K` | 1 / 0 | 4348042 | — | — | 1245752 | — | — | 8781 | — | — | 1 disk-api-contract, 8388608 file-bytes, 41392 index-build-ms, 4638969 mean-ns, 5626292 p99-ns, 100 recall@10, 414 reopen-ms |
