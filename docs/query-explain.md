# EXPLAIN v1 contract (#258)

Expose `DB.ExplainContext` and `Tx.ExplainContext` as structured Go APIs.
No query keyword, PROFILE mode, or iterator execution is introduced. The DB API
opens one read transaction; the Tx API requires an active read-only transaction.
Capture the database ID and commit ID with the graph snapshot. A concurrent commit
must not change the snapshot identity returned by an older reader.

The result has schema version 1, snapshot identity, and a root with stable
operator names and children. Optional candidate estimates are integers. An absent
estimate means unknown; zero means a known empty candidate source. Estimates do
not describe emitted row counts. Direction is outgoing, incoming, or undirected for expansion. A global directed
edge scan has no expansion direction.
Report sort and top-K separately; a top-K operation is shown only when the
executor can use it for this query shape and pagination. Otherwise show sort and
separate pagination. Never publish internal Go type names.

Use the actual ordered-match planner and its budget for the supported subset.
An access path chosen from bindings or index metadata may be reported as a
candidate. Mark it conditional when execution still checks parameters, postings,
selectivity or search results; include a stable reason and scan fallback. Do not
materialize posting candidates or run FTS or vector search to choose a displayed
path. On disk, the existing planner may count entries in an index or label
posting list under the planning work budget; it does not return candidate rows. Do not
invent an estimate for a dynamic branch. Basic scalar read queries and existing
connected MATCH plans are the first scope. Report unsupported read shapes as
plan-stage errors rather than misrepresenting them; mutation is always rejected.

Use the existing QueryOptions defaults, cancellation, resource errors and parse
errors. Charge plan objects and bounded statistics reads to the request budget.
Planning must not change graph contents, commit ID, change feed, metadata, or
index state. Tests compare plan ordering and direction on memory/page snapshots,
indexed and fallback paths, known zero vs unknown, old read Tx after a commit,
cancellation, low budgets, and mutation rejection without side effects.
