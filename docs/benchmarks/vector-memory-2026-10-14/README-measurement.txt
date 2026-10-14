Parent work: memory/CPU follow-up, 2026-10-14, default GOGC/GOMEMLIMIT.
All timings run sequentially, GOMAXPROCS=2, no tests/compiles alongside benchmarks.
Before binary: /tmp/latticedb-memory-20261014/before.test (previous final production source plus optional memory telemetry).
Before100K had batch runtime.MemStats and one vmmap snapshot during build.
Candidate100K had the same batch telemetry, one vmmap snapshot and a final pre-close heap profile (no forced GC).
Neither diagnostic100K process had continuous CPU profiling.
Paired10K before/after: three alternating pairs, no runtime telemetry and no CPU/heap profiles.
Candidates: scratch reuse; exact-capacity owned vector writes; owned decoded-write cache; stream maintenance borrowed decode avoiding record-cache admission.
All use prior normalized fixtures, durable batches1000, M16/efConstruction200/efSearch64, count100queries, recallfirst10queries.
Previous Zig results remain historical reference, not rerun in this follow-up.
