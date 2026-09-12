# Persisted HNSW cache

HNSW opens can restore a disposable index cache at `<state-path>-hnsw` instead
of rebuilding every index. Directory databases use `state.json-hnsw`; flat
files use `<database-file>-hnsw`. The canonical snapshot and WAL formats are
unchanged, and neither serialization nor backup requires the cache.

The `LDBHNSW1` format binds a commit ID and SHA-256 fingerprint to the database
ID, vector dimensions, sorted namespace definitions, and canonical selected
node IDs/vector bits. A separate checksum covers the serialized topology.
Decoding checks live vectors, routing tombstones, mutation debt, entry/level
metadata, and neighbor references before publishing the complete index set.
Index algorithm changes require a cache version bump. Continue supplying
`VectorNamespaces` on each open; the cache does not persist their configuration.

Missing, stale, corrupt, incompatible, or over-budget caches fall back to the
existing rebuild. Cancellation still aborts Open. Cache validation observes
`VectorIndexBuildMaxWork` and `VectorIndexBuildMaxLogicalBytes`, including file
size and conservative sparse-index allocation bounds. Rebuilding may produce
a different topology from an incrementally maintained index, so approximate
search results need not be identical.

Successful writable `Close`, `Checkpoint`, and `CheckpointContext` refresh the
cache, including when a background checkpoint already cleared the dirty flag.
Cache writes use a synchronized temporary file and atomic rename; failures do
not invalidate a successful canonical checkpoint. Background checkpointing
does not write caches. Read-only and temporary Deserialize handles never write
one; Deserialize rebuilds from its canonical payload.

Publication refuses symlinks, nonregular files, multiple hard links, and
changed destination identities. It replaces existing files only when they
have the seven-byte `LDBHNSW` prefix, protecting unrelated files with a
colliding name. An unrecognized or damaged magic prefix is left untouched.
If that file is known to be a corrupt cache, removing it allows regeneration
at the next writable Close or explicit checkpoint.

## Reopening benchmark

Apple M3, 16-dimensional vectors, one transaction to seed each fixture.
Medians of three runs with three opens per sample:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkVectorCacheColdOpen$' -benchtime=3x -count=3
```

| Nodes | Open path | ns/op | B/op | allocs/op |
|---:|---|---:|---:|---:|
| 1,000 | Cached | 21,446,694 | 20,361,888 | 107,678 |
| 1,000 | Rebuild | 228,510,431 | 18,622,005 | 100,975 |
| 10,000 | Cached | 204,229,695 | 198,328,373 | 1,082,534 |
| 10,000 | Rebuild | 2,725,189,958 | 179,976,320 | 1,013,658 |

Both paths open fresh read-only handles and close them in the same process,
including canonical data loading. Setup and cache removal are outside timing;
the OS page cache is warm. This is not a cold-device or process-start benchmark.
Cached opens were about 10.7× and 13.3× faster on these fixtures, while allocated
bytes increased about 9.3% and 10.2%, respectively. These are reopen-time
measurements, not steady-state memory or ANN recall comparisons.
