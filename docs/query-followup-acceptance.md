# Preimplementation decisions for query follow-ups

These decisions refine the roadmap. They do not extend the supported query
syntax. Implement and validate #256, #257, #258, #259, #260 and #261 in order.
Review each section against the code at the start of its issue.

## #257: UNION row accounting and grammar

A top-level unquoted UNION delimiter separates complete read-only statements.
Each branch has one explicit RETURN schema. Its ORDER BY, SKIP and LIMIT are
branch-local. There is no global tail syntax. A branch uses one read snapshot
and the same normalized parameters as all the others. Validate all branches and
schemas before evaluating any branch. All operators draw from one request budget.
Keep existing per-stage row caps in each branch; also cap the combined count of
rows submitted to the union collector across all branches, including duplicates.
Charge deduplication and ordered result buffering to MaxWork and MaxBytes.
On overflow return an error and no result. ALL emits in branch order and retains
multiplicity. DISTINCT retains the first equal row in that order. Test duplicate
rows across branches at the row limit, even when the unique result would fit.

## #258: structured plan contract

Start with schema version 1. Identify the observed snapshot by database identity
and commit ID; an uncommitted writable transaction is outside the initial API.
Use an optional integer estimate so absent differs from estimated zero. Operator
kind and children are always present. Other fields are present only for operators
that use them. Use stable operator identifiers, not Go type names. EXPLAIN must
use the same planning logic and budget as execution without running operators or
mutating state. Unsupported or parameter-dependent choices must be identified
explicitly; a missing estimate must not turn into a guessed row count.

## #259: constraint scope and values

A definition name is unique within one database. Each constraint applies to one
label and property. A node with multiple labels participates in each applicable
constraint. Required means present and non-null. Type supports the existing
scalar bool, int64, float64 and string kinds as distinct types; no implicit numeric
coercion. Unique applies to those scalar types and ignores null and missing.
Uniqueness uses existing typed key equality within the label, across all matching
nodes. Test same values on other labels, overlapping labels, null/missing cases,
name collisions, and swaps in a single transaction. Validate the final state
before publication; restore/import follow the same validation rules. No implicit
legacy reader or migration path.

## #260: score and path multiplicity

A search candidate is one entity and its native metric value with a direction
(higher relevance or lower distance), metric identity, and exactness. Do not
compare values from different metrics or normalize them implicitly. One expansion
row per qualifying path preserves bag semantics; projecting just the entity can
return duplicates. A later explicit DISTINCT uses existing value equality.
Rank by the source metric, then candidate entity ID, then stable path edge IDs
where needed. Candidate-k applies before graph expansion, result-k after it.
Measure recalls against a reference with the same filter order and multiplicity.

## #261: design spike boundary

Compare graph reads and at least one real local FTS or vector input. Map both to
snapshot identity, historical capability, filter/limit pushdown and shared
budget behavior. Prove one common-snapshot case and one rejected incompatible
case using existing APIs or small experiments. Keep an internal interface only
if it removes demonstrated duplication without weakening snapshot or admission
checks. No remote adapter, migration, or public plugin API is needed to finish
this design spike.
