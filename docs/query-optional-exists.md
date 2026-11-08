# Initial OPTIONAL MATCH and EXISTS contract (#256)

Proposed implementation contract. Support is announced only after the backend
and API parity tests pass. This feature does not change stored data or formats.

- An OPTIONAL clause evaluates its fixed-length pattern and attached WHERE for
  each input row. Keep all surviving matches. If none survive, keep the input
  and bind every newly introduced name to NULL. Preserve multiplicity, including
  parallel edges, and never replace a pre-existing binding. No input rows means
  no output rows. A leading OPTIONAL starts with the usual single empty row.
- An unmatched entity is a **bound NULL**, not an absent scope name. A later
  pattern referencing it has no match and cannot rebind it. An existing matched
  entity with a missing or explicit-null property remains an entity. Projecting
  that property or a property of a NULL binding produces NULL; projecting the
  entity distinguishes the two cases. Only the direct property API distinguishes
  missing from stored NULL. Fixed-length pattern maps use the existing equality
  filter: `{p: null}` and `WHERE n.p = null` do not match, even when the
  property stores NULL. An unknown or dropped reference is
  an error.
- Attached OPTIONAL WHERE runs before null extension. WITH WHERE runs after
  projection and can filter out that null-extended row. WITH aliases preserve
  the binding value; names excluded by WITH are out of scope.
- Initial EXISTS syntax is `EXISTS { MATCH <fixed-length pattern> [WHERE <scalar predicate>] }`,
  with `NOT EXISTS` and ordinary Boolean composition. It returns a two-valued
  Boolean; stop at the first surviving inner match. Correlation uses in-scope
  outer bindings. An inner pattern name already in outer scope reuses that
  binding and cannot shadow it. A role conflict is a validation error; a
  referenced NULL entity or label mismatch yields no match. Unrelated NULL
  bindings have no effect. Inner variables do not escape. Reject nested EXISTS, inner WITH/RETURN and mutations in this
  initial subset. Reject ranked search within the new operators initially.
- Preserve one cumulative emitted-row counter per syntactic producing stage.
  Distinct stages have distinct counters, each capped by MaxRows. Real and
  null-extended OPTIONAL outputs count at the OPTIONAL boundary. Inner MATCH
  candidates count at their own MATCH boundary, even if a later filter rejects
  them. The same stage counter persists across all correlated invocations. Rows examined and rejected still consume MaxWork.
  Intermediate rows and retained sources consume MaxBytes until released.
  Do not create a new budget for each outer row. On a limit or cancellation
  error, return the existing query error contract; never turn the error into
  an unmatched row or false EXISTS result.
- Reject mutation anywhere in the statement (including across WITH), variable
  length inside the new operators, and unsupported nested combinations before
  execution. Keep all previously supported statements unchanged.

Acceptance: memory/page × DB/Tx, including cold and cached plans. There is no
public prepared-query API in this version; adding one is outside #256. Test zero, one and many matches; parallel
edges; missing/explicit NULL on matched entities versus unmatched entities;
chained OPTIONAL and later mandatory MATCH; attached versus post-WITH WHERE;
outer alias/drop and inner non-leakage; EXISTS/NOT EXISTS and short-circuit;
empty input; limits/cancellation and rejected mutation with no state change.
