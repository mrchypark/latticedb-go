# Value Model

`latticedb-go` targets the same logical value model as the reference engine:

- `NULL`
- `BOOL`
- `INT`
- `FLOAT`
- `STRING`
- `BYTES`
- `VECTOR`
- `LIST`
- `MAP`

Required semantics:

- Node, edge, and endpoint IDs are uint64 values in `1..MaxInt64`; `MaxInt64+1`
  is reserved only as the persisted allocation-exhaustion sentinel.

- lists are ordered and heterogeneous
- maps are string-keyed nested values
- nested values may contain other nested values recursively
- duplicate map keys are invalid input at public API boundaries
- bytes remain bytes
- vectors remain vectors
- query materialization must not coerce bytes into strings
- `UNWIND` and query projection must preserve nested values and vectors
- JSON-based dump/export surfaces must preserve the full logical value model with explicit recursive type tags so `BYTES` vs `STRING`, `VECTOR` vs `LIST`, and `INT` vs `FLOAT` remain distinguishable
- `NULL` remains distinct from property absence in the direct property APIs
- callers must not depend on a stable map entry order across engines or bindings

Behavioral notes:

- The same value model should be accepted by direct property APIs and query parameter binding.
- The same value model should be observable through direct property APIs and query results.
- Query property access on a missing property yields `NULL`.
- Query results may therefore contain `NULL` for both an explicitly stored `NULL` and a missing property referenced through query evaluation.
- That distinction is only guaranteed through the direct property APIs, not through query projection alone.

Compatibility boundary:

- This value model is part of the required engine/query contract.
- No JSON import format is currently part of the required value-model contract.
- A future import/export interchange format should be specified explicitly rather than inferred from any single engine implementation.

Vector search contract:

- When vector search is enabled, each node may have at most one direct `[]float32` property. Adding a second vector property is rejected, because the unqualified vector-search API cannot safely choose between fields.
- When vector search is disabled, `[]float32` values are ordinary properties and multiple vector-valued properties remain allowed. Reopening that database with vector search enabled requires removing the ambiguity first.
