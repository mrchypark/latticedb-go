# Engine Conformance Spec

This document defines the engine-level contract that `latticedb-go` should match.

The goal is not to freeze Zig internals. The goal is to freeze the observable database behavior above the storage engine and below the language bindings.

This document sits alongside the local value-model spec in [value_model.md](value_model.md).

## Purpose

The current Zig engine is the reference implementation. `latticedb-go` should be considered conformant if an application using the public database semantics cannot distinguish between the two engines except in areas this document explicitly leaves unspecified.

This spec is intentionally written in terms of:

- logical graph state
- transactions and visibility
- query and search behavior
- persistence and recovery behavior
- public management surfaces such as query cache stats

This spec does not require:

- on-disk format compatibility
- ABI compatibility
- identical planner internals
- identical scoring implementation details where only ordering matters

## Scope

In scope:

- nodes, edges, labels, and properties
- stable edge identity
- the public value model, including nested values
- transaction visibility and durability semantics
- query result semantics
- vector and full-text search behavior at the API/query level
- export/dump invariants that affect logical graph state
- query cache functional behavior

Out of scope:

- binary file-format compatibility across engines
- exact memory layout or ABI details
- exact internal timestamps, WAL record shapes, or page layouts
- exact query plan shapes
- exact text of human-readable error messages

## Normative Interpretation

This document is the intended contract.

The conformance suite in [`conformance/go`](../conformance/go/README.md) is the executable example of this contract. If the implementation and this document disagree, the disagreement should be resolved explicitly rather than silently allowing drift.

The suite currently runs against local adapters for driver, export, and recovery behavior.

## Compatibility Boundaries

The project has four different compatibility surfaces. They should be treated separately:

1. Engine semantics
2. Binding/API semantics
3. Query-language semantics
4. On-disk format

`latticedb-go` should target semantic compatibility first. It does not automatically inherit obligations around the current C ABI or file format unless those are chosen explicitly in a later phase.

## Core Data Model

### Nodes

- A node is identified by a uint64 node ID in the inclusive range `1..MaxInt64`.
- Node IDs are stable within a database and remain valid across close/reopen for surviving nodes.
- Nodes may have zero or more labels.
- Unlabeled nodes are valid.
- Labels are strings.
- Label matching in queries is set-based and conjunctive:
  - `MATCH (n:Person:Employee)` means the node must have both labels.
- Direct node label enumeration preserves insertion order as exposed today by the direct APIs and bindings.
- Query semantics must not depend on label order.

### Edges

- An edge is directed.
- Multiple parallel edges with the same `(source, target, type)` are valid.
- Every edge has a stable edge ID distinct from its `(source, target, type)` triple.
- Edge IDs are:
  - in the inclusive range `1..MaxInt64` (source and target IDs use the same range)
  - unique
  - stable across close/reopen
  - monotonic
  - never reused after delete or rollback
- Mutations addressed by edge ID apply to exactly one edge instance, even when parallel edges exist.

Entity IDs remain uint64 in the public and storage APIs for compatibility. The
high-water counters use `MaxInt64+1` only as an exhaustion sentinel; IDs are
never allocated from that value.

### Properties And Values

The logical value model is:

- `NULL`
- `BOOL`
- `INT`
- `FLOAT`
- `STRING`
- `BYTES`
- `VECTOR`
- `LIST`
- `MAP`

The detailed nested-value contract for this repo lives in [value_model.md](value_model.md). At the engine level the important points are:

- lists are ordered and heterogeneous
- maps are string-keyed nested values
- nested values may contain other nested values recursively
- duplicate map keys are invalid input at public API boundaries
- bytes remain bytes
- vectors remain vectors
- query materialization must not coerce bytes into strings
- `UNWIND` and query projection must preserve nested values and vectors

### Missing Versus `NULL`

Missing property and stored `NULL` are distinct concepts.

- Direct property APIs must preserve the distinction.
- High-level bindings should expose a way to distinguish them.
- Query property access on a missing property yields `NULL`, not an error.
- Query results may therefore contain `NULL` for both:
  - an explicitly stored `NULL`
  - a missing property referenced through query evaluation

That distinction is only guaranteed through the direct property APIs, not through query projection alone.

### Property Mutation Semantics

The direct property APIs and Cypher mutation syntax are intentionally different:

- Direct property setters may store an explicit `NULL` value.
- In query mutation semantics, `SET n.prop = null` behaves like property removal.
- `REMOVE n.prop` also removes the property.

This distinction is part of the public contract and should not be normalized away by a future engine.

## Transaction Semantics

### Modes

- The database exposes read-only and read-write transactions.
- Read-only transactions must reject writes.

### Visibility

The current public transaction contract locks in these guarantees:

- a transaction sees its own uncommitted writes
- after a write transaction commits, a newly started transaction sees the committed state
- rolled-back changes are not visible after rollback

The current public contract does not freeze:

- cross-transaction visibility before commit
- the behavior of a long-lived transaction across concurrent commits
- concurrent writer conflict resolution, including whether contending writers fail, block, expose uncommitted changes to each other, or resolve by mutation order or commit order

Portable applications must not rely on overlapping write transactions touching the same logical record. If two live write transactions can both mutate the same node, edge, or property, the caller should serialize them with a higher-level lock or restrict itself to one active writer at a time per database.

> Current reference note (non-normative): the Zig engine's current public database surface allows one live writer to observe another live writer's uncommitted mutation, and same-property conflicts currently resolve by mutation order rather than commit order. That behavior is an implementation detail, not the required cross-engine contract.

The internal transaction manager uses snapshot-oriented MVCC machinery, but that should not be treated as the required public engine contract yet because the end-to-end database APIs are not fully wired to expose those stronger guarantees consistently. Implementations may provide stronger isolation than the guarantees listed above, but callers must not depend on that stronger behavior for cross-engine portability.

### Atomicity

Transactions are all-or-nothing.

- A committed transaction makes all its writes visible.
- A rolled-back or aborted transaction leaves no visible logical changes.

Query execution also carries a statement-level atomicity requirement:

- if a mutation query fails during execution, it must not leave partial logical side effects behind
- this applies to cases such as invalid non-property values inside `CREATE`, `SET`, or `MERGE` patterns

### Durability

- Committed transactions must survive close/reopen.
- Committed transactions must survive crash/recovery.
- Uncommitted or aborted transactions must not become visible after recovery.

## Graph Semantics

### Labels

- Nodes may have multiple labels.
- Multi-label queries are conjunctions, not disjunctions.
- Removing a label affects only that label.
- Export/import and query logic must not duplicate a node merely because it has multiple labels.

### Edge Identity

Parallel edges are first-class graph state, not an implementation accident.

The following must hold:

- deleting one edge by stable edge ID leaves other parallel edges intact
- query mutations on a bound edge variable apply only to the matched edge instance
- `DELETE r` and `DELETE` by `id(r)` must be able to target one parallel edge without removing all siblings

### Directionality

- Edge existence and traversal are directional.
- `(a)-[:REL]->(b)` is not interchangeable with `(b)-[:REL]->(a)`.

## Query Semantics

LatticeDB implements a deliberately small, case-sensitive Cypher subset. It does not claim full openCypher compatibility. [`internal/engine/testdata/query_grammar.ebnf`](../internal/engine/testdata/query_grammar.ebnf) is the canonical grammar, `TestQueryGrammarMatrix` is the executable syntax inventory, and `TestSupportedCypherGrammarContract` locks both to the parser surface so parser changes require an explicit grammar and boundary-case audit. The extracted conformance suite separately verifies runtime semantics.

### Supported Cypher Subset

Structural keywords are uppercase. Bindings, property names, labels, relationship types, aliases, and parameters use either unquoted ASCII identifiers matching `[A-Za-z_][A-Za-z0-9_]*` or backtick-quoted identifiers. Quoted identifiers preserve valid UTF-8 exactly, including spaces and structural delimiters; a backtick is written as doubled backticks, and backslashes are literal. Empty, NUL-containing, invalid UTF-8, and unterminated identifiers are rejected. Keywords are not reserved when they occur in an identifier position. Integers are signed base-10 values, and floats use decimal or scientific notation. Strings may use single or double quotes with Go-style escapes.

<!-- BEGIN supported-cypher-grammar -->
```text
query          = (match-query | create-node-query | unwind-query) [";"]
match-query    = MATCH patterns [WHERE predicates] [match-terminal]
match-terminal = RETURN return-tail
                | SET assignments [RETURN return-tail]
                | CREATE edge-create [RETURN return-tail]
                | REMOVE removals [RETURN return-tail]
                | [DETACH] DELETE bindings
create-node-query
                = CREATE node-pattern [RETURN return-tail]
unwind-query   = UNWIND value AS binding RETURN return-tail
                | UNWIND value AS binding CREATE node-pattern [RETURN return-tail]
                | UNWIND value AS binding match-query

patterns       = pattern {"," pattern}
pattern        = node-pattern | node-pattern {relationship node-pattern}
relationship   = "-[" [binding] [":" type] [properties] "]->"
                | "<-[" [binding] [":" type] [properties] "]-"
                | "-[" [binding] [":" type] [properties] "]-"
node-pattern   = "(" [binding] {":" label} [properties] ")"
properties     = "{" [property ":" expression {"," property ":" expression}] "}"

predicates     = and-expression {OR and-expression}
and-expression = not-expression {AND not-expression}
not-expression = [NOT] ("(" predicates ")" | predicate)
predicate      = property-access ("=" | "<>" | "<" | "<=" | ">" | ">=") expression
                | property-access IN expression
                | property-access (STARTS WITH | ENDS WITH | CONTAINS) expression
                | id-access "=" expression
                | property-access IS NULL
                | property-access IS NOT NULL
                | property-access "<=>" expression
                | property-access "@@" expression

assignments    = assignment {"," assignment}
assignment     = property-access "=" expression
                | binding "=" expression
                | binding "+=" expression
                | binding ":" label
edge-create    = "(" binding ")-[" [binding] ":" type [properties] "]->(" binding ")"
                | "(" binding ")<-[" [binding] ":" type [properties] "]-(" binding ")"
removals       = (property-access | binding ":" label)
                 {"," (property-access | binding ":" label)}
bindings       = binding {"," binding}

return-tail    = [DISTINCT] (count-return | projection {"," projection})
                 [ORDER BY order {"," order}]
                 [SKIP pagination] [LIMIT pagination]
count-return   = "count(" ("*" | binding) ")" [AS alias]
projection     = (binding | property-access | id-access) [AS alias]
order          = (binding | property-access | id-access | alias) [ASC | DESC]
pagination     = non-negative-integer | parameter

value          = null | true | false | integer | float | string | parameter | map
map            = "{" [property ":" expression {"," property ":" expression}] "}"
expression     = value | binding | property-access

identifier     = unquoted-identifier | quoted-identifier
unquoted-identifier
               = ("A"..."Z" | "a"..."z" | "_")
                 {"A"..."Z" | "a"..."z" | "0"..."9" | "_"}
quoted-identifier
               = "`" {quoted-character} "`"
quoted-character
               = UTF-8-non-NUL-character-except-backtick | "``"
binding        = identifier
label          = identifier
type           = identifier
property       = identifier
alias          = identifier
parameter      = "$" identifier
```
<!-- END supported-cypher-grammar -->

Fixed-length `MATCH` paths may be incoming, outgoing, or undirected; relationship creation remains directed. Variable-length paths remain unsupported. Vector and full-text search predicates may be joined by `AND`, but not placed under `OR` or `NOT` because those operators carry ranking semantics. `OPTIONAL MATCH`, `MERGE`, `WITH`, `UNION`, and list literals remain unsupported. Values unsupported by the literal grammar, including lists, bytes, and vectors, remain available through parameters.

An undirected relationship produces one row per matching orientation. A non-self edge can therefore produce two rows when both endpoints are unbound; a self-loop produces one.

### General Rules

- Query parameters accept the same logical value model as direct property APIs.
- ASCII whitespace outside string literals is interchangeable, so multiline and tab-indented queries are accepted without changing string contents.
- Query comments are not supported.
- Binary predicates and `SET` assignment operators require surrounding whitespace after normalization; compact spellings such as `n.age=1` are not supported.
- One optional trailing semicolon is accepted; multiple statements remain invalid.
- Query results return the same logical value model, including nested values.
- Explicit `RETURN ... AS alias` controls the output column name.
- Portable code should use explicit aliases for result column names.
- `LIMIT` applies to the produced rows for a `RETURN` clause.
- `SKIP` is applied after ordering and before `LIMIT`.
- `ORDER BY` defines one ascending total order for query values: numbers (with exact mixed integer/float comparison), booleans, strings, bytes, vectors, lists, maps, then `NULL`. Values of the same vector or list type compare lexicographically; maps compare lexicographically by sorted `(key, value)` entries, recursively. `NULL` therefore sorts last with `ASC` and first with `DESC`.
- `DISTINCT` removes duplicate projected rows before `SKIP` and `LIMIT`.
- With `RETURN DISTINCT`, every `ORDER BY` expression must also be projected.
- `SKIP` and `LIMIT` accept a non-negative integer literal or parameter; other runtime parameter values are execution errors.
- `LIMIT 0` returns zero rows.
- Negative `LIMIT` values are invalid.
- Without an explicit alias, the current derived-name behavior is not a required cross-engine compatibility guarantee.
- Unknown relationship types in `MATCH` produce an empty result, not an error.

### Property Access

- Property access on a node or edge may return `NULL`.
- Bound node, edge, and map properties may be used in property maps, as mutation values, or on the right side of predicates; a missing property evaluates to `NULL`.
- An expression may only reference a binding available at its evaluation point. In particular, an `UNWIND` source and a newly created node or relationship cannot reference the binding they are about to introduce.
- `IS NULL` and `IS NOT NULL` operate on the resulting query value, not on direct-storage presence metadata.
- Boolean predicates use three-valued logic; only `true` rows pass `WHERE`, and comparisons involving `NULL` or a missing property remain unknown under `NOT`.
- Ordered comparisons accept numeric values, including mixed integers and floats, or two strings. Incomparable values do not pass `WHERE`.
- `IN` accepts any supported expression that evaluates to a list and follows three-valued logic when the list contains `NULL`.
- `STARTS WITH`, `ENDS WITH`, and `CONTAINS` compare strings without case folding.

### Mutation Semantics

The following behaviors are part of the contract:

- `CREATE` creates nodes and edges with labels and property maps
- `UNWIND` may feed `CREATE`, or a `MATCH` followed by its supported mutation clauses.
- `SET target.prop = expr` updates or removes a property depending on whether `expr` evaluates to non-`NULL` or `NULL`
- `SET target = {...}` replaces the property map on the target
- `SET target += {...}` merges into the property map on the target
- `SET target:Label` adds a label idempotently and updates label lookup state.
- Multiple comma-separated `SET` items execute in source order.
- `REMOVE target.prop` removes a property
- `REMOVE target:Label` removes a label
- `SET`, `REMOVE`, and relationship `CREATE` may be followed by `RETURN`; a created relationship binding is available to that projection.
- Plain `DELETE` rejects nodes with incident relationships; `DETACH DELETE` removes incident relationships when deleting a node.
- On mutation queries with `RETURN`, `SKIP` and `LIMIT` truncate the returned rows but do not suppress earlier statement side effects.
- mutation against a bound edge variable targets the matched stable edge instance, not all parallel edges with the same endpoints

### Search Operators Inside Queries

The query-level vector and full-text operators must preserve row semantics:

- additional `MATCH` bindings are preserved
- input row multiplicity is preserved
- filtering around the search operator still constrains the candidate rows correctly

This matters more than exact internal planning strategy.

## Search API Semantics

### Vector Search

- Vector search is nearest-neighbor search over stored vectors.
- Result arrays are ordered by distance ascending.
- Lower distance is better.
- When one stored vector is an exact match for the query vector and another is not, the exact match should rank ahead.
- Exact floating-point distances are not part of the cross-engine contract.
- Tie order is not currently specified.

### Full-Text Search

- Full-text search returns scored matches for indexed text.
- Result arrays are ordered by score descending.
- Higher score is better.
- Exact score values are not part of the cross-engine contract.
- Tie order is not currently specified.
- Fuzzy search should be at least as permissive as a stricter exact configuration when given looser distance settings.
- In a query predicate such as `n.title @@ "term"`, only the named string property is searched; a missing or misspelled property does not fall back to a node-level index.
- Direct `FTSSearch` searches the explicitly indexed node text and is independent of query property predicates.

## Query Cache Semantics

The query cache is a public management surface even though it is not part of logical database state.

Required behavior:

- clearing the cache on an empty database succeeds
- fresh cache statistics report zero entries, hits, and misses
- executing a previously uncached query text increases misses by at least one
- executing the same query text again without clearing or reopening increases hits by at least one
- clearing the cache resets the entry count to zero

Not required:

- resetting cumulative hit/miss counters on clear
- durability across reopen
- exact cache eviction strategy
- exact cache size or internal keying details beyond current public behavior

## Persistence, Recovery, And Export Invariants

### Close/Reopen

The following must survive normal close/reopen:

- nodes
- edges
- labels
- properties
- stable edge IDs

### Crash Recovery

The following must hold after recovery:

- committed transactions are visible
- aborted or in-progress transactions are not made visible
- symbol/label resolution remains consistent for recovered records

### Export Invariants

Current export behavior establishes several logical invariants worth preserving across engines:

- multi-label nodes are exported once, not once per label
- parallel edges are preserved as distinct edges
- edge properties remain attached to the correct edge instance
- the public `dump` command emits canonical JSON for cross-engine state comparison
- JSON-based dump/export encode each logical value with an explicit recursive type tag (`null`, `bool`, `int`, `float`, `string`, `bytes`, `vector`, `list`, `map`) so ambiguous pairs such as bytes-versus-string, vector-versus-list, and int-versus-integral-float remain distinguishable
- canonical dump includes unlabeled nodes
- canonical dump orders nodes by node ID ascending
- canonical dump orders edges by source ID, target ID, type name, then edge ID
- canonical dump sorts labels, property keys, and nested map keys lexicographically
- canonical dump includes stable edge IDs so parallel edges remain distinguishable in state comparisons
- repeated dumps of unchanged logical state are byte-stable
- CSV export writes a JSON manifest at the requested output path; its `nodes` and `edges` fields are paths relative to the manifest directory
- each CSV publication uses an immutable directory under `<output>_generations`, and node labels are encoded as a JSON array in the CSV field
- published CSV generations are retained because readers have no lease protocol; callers that export repeatedly must manage retention only after they know no reader references an older manifest

### Import Compatibility Boundary

No JSON import format is currently part of the required engine contract.

- The current compatibility target is the direct property/query APIs plus canonical dump/export behavior.
- A future import format may be added, but it should be specified explicitly rather than inferred from the current reference-engine CLI importer.

## Deliberately Unspecified Areas

The following are intentionally left open for now:

- on-disk format compatibility
- concurrent writer conflict resolution for overlapping live write transactions, including pre-commit visibility and whether conflicts resolve by abort, blocking, mutation order, or commit order
- exact map iteration order
- exact vector distance values
- exact BM25/FTS score values
- exact tie-breaking when scores or distances are equal
- non-aliased result column naming
- exact human-readable error wording
- exact planner/operator tree shape

These areas may be tightened later if real cross-engine compatibility pressure appears.

## Current Extracted Coverage

The current extracted suite already covers black-box cases drawn from these sources:

- `tests/integration/database_test.zig`
  - persistence across reopen
  - multi-label semantics
  - stable monotonic edge IDs
  - exact deletion of one parallel edge
- `tests/integration/mvcc_test.zig`
  - internal MVCC visibility rules
  - own-write visibility
  - deleted-version invisibility
- `conformance/go/suite_test.go`
  - read-only rejection
  - own-write visibility
  - newly started transactions observe committed state
  - rollback cleanup
  - overlapping live writer behavior intentionally left out of the portable contract
- `tests/integration/query_mutation_test.zig`
  - mutation atomicity
  - edge-specific mutation semantics
  - alias propagation
  - vector and full-text operator row semantics
  - `NULL` behavior in query execution
- `tests/integration/import_export_test.zig`
  - export deduplication and parallel-edge preservation
  - dump/export logical-state invariants through the public CLI
- `tests/crash/crash_test.zig`
  - committed graph-state recovery through WAL replay
  - multi-label and secondary-label lookup recovery through WAL replay
  - committed node-property update recovery through WAL replay
  - committed edge-property update recovery through WAL replay
  - aborted tail inserts remaining invisible after replay
  - post-recovery edge-ID monotonicity relative to committed state
- binding integration tests
  - missing-versus-`NULL` distinction in direct property APIs
  - query cache surface behavior
  - nested value round-trips through public APIs

The extracted suite keeps its assertions black-box:

- no direct access to Zig internals
- assertions written in terms of public behavior only

Some adapters are still engine-specific:

- the current export adapter calls the local public export/dump surface
- the current recovery adapter simulates crash/reset using the current engine's file layout to force recovery

That adapter-specific knowledge is intentional; it lets the suite stay engine-neutral at the behavioral level while still exercising crash/export behavior on the current engine.

## Immediate Follow-Ups

1. Widen the canonical dump conformance coverage as new value shapes or exported fields are added.
2. Write a cleaner engine-neutral crash-injection interface if a future storage layout needs a different recovery trigger than the current file-reset harness.
3. Decide and extract black-box coverage for concurrent writer conflict semantics if cross-engine portability needs them.
