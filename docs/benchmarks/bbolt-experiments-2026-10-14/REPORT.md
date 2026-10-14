# bbolt buffer experiments — not adopted

Optimization stopped at the user's request to prepare a release. No bbolt fork,
anonymous page arena, free-page advice, or bounded replacement pool is included
in the release. The production source was restored to the preceding verified
cursor-reuse candidate; its 391-file source manifest matched after restoration.

Temporary prototypes replaced bbolt v1.5.0's single-page sync.Pool with
writer-owned buffers. They preserved data sync, metadata sync, rollback and
snapshot ordering. Tested variants included anonymous mmap slabs, releasing all
but 1 MiB at transaction close, bounded Go heap pages/slabs, and available-page
MADV_DONTNEED advice. Repeated 10K runs passed recall/reopen checks but did not
show a consistent peak-memory benefit. The file-advice variant also changed free
page release timing; it was not retained.

## Completed 100K measurements

macOS arm64, Go 1.27.1, GOMAXPROCS=2, default GOGC/GOMEMLIMIT, PGO off,
128 dimensions, 1,000-vector synchronous batches, 64 MiB configured cache.
Each row is one fresh process. SIGSTOP/vmmap captures occur after 50K/100K,
close and reopen; wall time includes capture pauses. No forced GC.

| Variant | Build seconds | Process CPU seconds | Peak RSS MiB | Recall@10 |
|---|---:|---:|---:|---:|
| Retained production candidate | 224.648 | 232.317 | 661.125 | 100% |
| Anonymous slabs, 1 MiB idle retention, no file advice | 220.303 | 229.367 | 666.109 | 100% |
| Go heap pages, 1 MiB idle retention | 235.354 | 237.546 | 657.922 | 100% |

All three completed close/reopen verification and produced 419,856,384-byte
files. The heap-page prototype traded approximately 0.5% lower peak RSS for
2.3% more measured CPU; anonymous slabs did not reduce peak RSS against the
fresh production control. These small, single-run differences do not establish
statistical equivalence. Lower retained heap alone would not prove lower total
process memory.

Focused prototype tests passed snapshot reuse, rollback, failed/partial data and
metadata writes, callback/panic cleanup, upstream bbolt reopen, and process exit
after commit. They do not prove physical power-loss behavior. A final
zero-initialization refinement was still being benchmarked when the user stopped
optimization; it has no completed 100K result and is not included.

The preceding [mmap/heap report](../vector-mmap-heap-2026-10-14/REPORT.md)
remains the completed Go/Zig comparison. No new Zig run was started in this
experiment. Remaining memory differences are not established as a hard language
limit. Changing the mapped-file working set or GC ownership has architectural
and CPU costs; these prototypes did not justify a maintained storage-engine fork.
