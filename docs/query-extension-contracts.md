# Query extension contracts and implementation scopes

Status: design proposal for #252, based on v0.12.1. None of the proposed syntax
or APIs below is advertised as implemented. README and engine_conformance.md
remain authoritative for shipped behavior. Implementations need separate review.
This is independent property-graph design, not Fluree compatibility or a source port.

## Common contract

Initial query extensions are read-only. Reject any statement that combines an
extension with mutation before executing it. Mutation support requires a separate
contract and tests; parsing a statement does not establish support. Existing
statements retain their current grammar and result contracts.

Use the current query value model: a missing property evaluates to NULL; predicates
use three-valued logic and only true passes WHERE. Preserve the direct API's
missing-versus-null distinction. Do not add SPARQL-style unbound variables.

Every operator consumes the same request's MaxRows, MaxWork and MaxBytes budget,
including planning, correlated inner scans, deduplication and buffered results.
Context cancellation must interrupt these paths. Do not reset the budget per
branch or inner query. On resource failure return the existing error contract;
never silently truncate an exact result or return partial success. Do not loosen
existing mutation rollback guarantees for statements outside the new subset.

Required delivery matrix: memory/page storage × DB/Tx/prepared query, plus reopen
for persistent definitions. Compare values, row cardinality and errors, not just
parser acceptance. Tests must include empty input and tiny budgets/cancellation.

## 1. OPTIONAL MATCH, correlated EXISTS and UNION

Implement these in independently reviewable stages.

| Operator | Initial scope | Required behavior |
| --- | --- | --- |
| OPTIONAL MATCH | Read-only fixed-length patterns, existing WITH and projections | Per input row, produce all matches; if none, emit exactly one row with new bindings NULL. Never null out a pre-existing binding. |
| EXISTS / NOT EXISTS | Correlated read-only MATCH subquery in WHERE | Return a non-null boolean. Reuse outer bindings without overwriting them; inner bindings never escape. NOT EXISTS negates the boolean. |
| UNION / UNION ALL | Top-level read-only branches with explicit RETURN columns | Require identical column names, order and count. ALL concatenates bags; UNION removes duplicates with existing canonical nested-value equality. |

OPTIONAL's attached WHERE filters candidate matches before null extension. A
WHERE after WITH filters the resulting rows, so it can remove an unmatched row.
Chained OPTIONAL preserves a NULL outer binding; a later pattern cannot rebind
that NULL into a real node. On zero input rows OPTIONAL emits no rows. With a
bound NULL, an EXISTS pattern has no match and returns false. Empty input to an
outer query does not invent an EXISTS row.

An inner WITH follows the existing alias/drop rules; dropped names cannot be
used later. Reject references to unknown names and attempts to redefine an outer
binding with a different entity. Nested EXISTS must use distinct lexical scopes;
if the initial parser cannot enforce this, reject nested EXISTS explicitly.

For UNION, first-stage ORDER BY/SKIP/LIMIT belongs to an individual branch.
Ordering applies only where explicitly requested. A global union-tail ordering
or pagination construct is not introduced in stage one: reject ambiguous tails,
mismatched columns, mixed UNION/UNION ALL chains and nested UNION subqueries.
Document this grammar restriction before parser changes. Do not move a branch's
LIMIT across deduplication. Entity equality follows existing identity rules;
numeric and nested values use the existing canonical key semantics.

Acceptance cases: zero/one/many optional matches, parallel edges, chained NULL,
WHERE inside versus after OPTIONAL, nullable properties, outer aliases and drops,
EXISTS short-circuit with no inner variable leakage, negation, nested-scope
rejection, duplicate nested maps/lists, column mismatch, and branch pagination.
Assert budget/cancellation errors on inner scans and deduplication. Assert rejected
mutation combinations leave nodes, indexes, metadata and changefeed unchanged.

## 2. EXPLAIN observability

Candidate API: a structured read-only explanation of a prepared query using a
specific transaction snapshot. Reuse current planner decisions from closed #115;
do not rewrite the executor or claim a new cost model. Reject mutation statements
in the initial API. Planning may inspect bounded statistics but cannot execute
query mutations or create/drop derived indexes.

A versioned plan describes operator kind, scan/index identity, traversal direction,
known cardinality estimate, search seed, sort/top-K, fallback reason and child
operators. Represent an unavailable estimate as unknown, not zero. Do not expose
unstable Go pointers or internal struct layouts as the public contract. Plan
ordering is deterministic for the same schema/statistics snapshot.

PROFILE is a separate opt-in proposal and is not part of this delivery. Runtime
visited rows/edges and charged work/bytes can first remain internal test metrics.
Tests compare broad-first/selective-first plans, indexed/unindexed alternatives,
directions, tied estimates, search seeds, budget exhaustion and snapshot changes.
Assert EXPLAIN does not change commit ID, changefeed, metadata or graph contents.

## 3. Transactional graph constraints

Start with named node-label/property required, scalar-type and uniqueness
constraints. Numeric ranges and relationship constraints are explicit later
candidates; reject definitions outside this first set. Do not implement SHACL/RDF.

Required means the property exists and is non-null. A type constraint permits a
missing value unless paired with required, but rejects explicit null and a list
where a scalar type is required. Uniqueness ignores missing/null values, compares
non-null supported scalars with existing typed property-key equality, and rejects
list/map/vector definitions. Deleting a node removes it from uniqueness checks.

Definition changes and data changes validate atomically against the final proposed
transaction state, so a transaction can swap two unique values. Creating a
constraint validates existing data; failure publishes neither definition nor
partial index. Constraint definitions must not be stored as ordinary user-visible
application metadata keys that callers can overwrite.

Validate every write path, including bulk/import, under a bounded validation
budget; there is no silent trusted-bulk bypass in stage one. Backup serializes
constraints, and restore validates both definitions and data before activation.
Legacy backups without constraints restore with an empty constraint catalog.
Dropping a constraint is transactional and preserves older snapshot views.
Violation details contain constraint name, entity ID, property and reason without
unbounded value dumps. Preserve the public error wrapping convention.

Acceptance: add/drop/reopen, existing-data violation, null/missing/type cases,
within-batch duplicates, atomic unique-value swaps, rollback of definitions/data,
old reader isolation, backup/restore and all public import/write surfaces.

## 4. Search, score and path composition

Add only a small read-only search-result source surface before new path algorithms.
A candidate row carries entity identity, score/distance, ranking direction and
exact/approximate status. Joining and projecting it must retain the score without
recomputing it from a different snapshot. Existing @@ and vector predicate
semantics do not change.

Name candidate-k and final result-k separately. Approximate postfiltering may
produce fewer than result-k rows; do not promise exact global top-K. Declare
whether each filter is pre- or post-candidate selection. Stable ties use entity ID.
Charge candidate acquisition, graph expansion, filtering and ranking to one budget.
Reuse closed #219 corpus evidence as a baseline, then measure the added composed
queries' recall/work/memory; do not re-open completed corpus collection as new work.

Shortest/all-shortest paths need a separate implementation decision. Proposed
behavior is directed unweighted shortest paths with edge identity distinguishing
parallel paths, zero-hop success when endpoints coincide, and no result when
unreachable. Cycles must not create infinite enumeration. Exceeding path/work
limits returns an error rather than silently omitting shortest paths. Weighted
paths are outside this proposal. Existing variable-length path behavior stays intact.

## 5. Internal sources and historical capabilities

First deliver a local design spike, not a plugin framework or remote adapter.
Compare the existing graph and search inputs against a minimal descriptor for
identity/schema, snapshot/watermark, capabilities, optional pushdown, cancellation
and shared cost/result limits. Introduce an interface only if two real local
implementations can use it without weakening their contracts.

Historical-read capability is explicit. An input without it rejects an as-of
request; a head-only search source cannot pretend to share an older graph snapshot.
Reject cross-source operations unless a common snapshot/watermark relation can be
proved. Live SQL/Iceberg, remote source atomicity and cross-engine history are
outside the initial scope. Rhiza owns distributed branches, authenticated commit
ordering and cross-engine recovery/GC.

RDF/IRI/JSON-LD/SPARQL, reasoning, geospatial, fine-grained policy and signed
provenance stay deferred pending a concrete consumer and independent design.

## Independent follow-up units

Each unit must keep the common test matrix and unsupported-combination rules above.

1. Read-only OPTIONAL MATCH and correlated EXISTS, with lexical scope and NULL tests.
2. Read-only top-level UNION/UNION ALL, with explicit branch grammar and canonical deduplication.
3. Structured EXPLAIN over the existing planner, without PROFILE or query execution.
4. Required/type/unique node-property constraints across every persistent write path.
5. Search-result score projection and graph composition; shortest-path work remains a separate decision.
6. Local-source snapshot/capability design spike; external adapters require a concrete consumer.

These are implementation candidates with acceptance criteria, not claims of
implementation, compatibility, performance superiority or a delivery schedule.

Tracked follow-ups: #256 OPTIONAL/EXISTS, #257 UNION, #258 EXPLAIN,
#259 constraints, #260 search composition, and #261 source design spike.
