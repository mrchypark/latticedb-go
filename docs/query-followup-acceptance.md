# Preimplementation decisions for query follow-ups

These decisions refine the roadmap. They do not extend the supported query
syntax. Implement and validate #256, #257, #258, #259, #260 and #261 in order.
Review each section against the code at the start of its issue.

## #257: UNION row accounting and grammar

A top-level unquoted `UNION` or `UNION ALL` separates complete read-only
statements. Reject empty branches, nested unions, mixed operator chains, and
`UNION DISTINCT`. Allow only one terminal semicolon. Each branch has an explicit
RETURN with identical column count and names in order. Column types may differ.
ORDER BY, SKIP and LIMIT apply to their own branch, including the last branch;
there is no global tail syntax. Validate every branch before evaluation.

Branches run on one read snapshot with a shared request budget. Existing row caps
apply inside each branch. The union collector counts every post-pagination row,
including duplicates, before deduplication. Fail before accepting a row beyond
MaxRows. Charge all retained output, deduplication keys and working buffers to
the shared byte budget and all equality work to MaxWork. Cancellation and
resource errors return the usual execution-stage error with no partial result.

ALL retains multiplicity and branch order. Bare UNION retains the first equal
row in that order. Compare projected values in declared column order using the
existing canonical key and equality rules, including nested values, NULL,
numeric values and entity identity. Neither branch provenance nor sorting
fields are part of equality. Test mismatched schemas before evaluation, quoted
UNION text, branch-local tails, and a duplicate-heavy result whose inputs exceed
MaxRows even though its unique output fits.

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
