# Transactional node constraints v1 (#259)

- Definitions are database-wide named and belong to one node label/property. Create,
  drop and list via Go transactions. Duplicate names and absent drops are errors.
  Drop then create is allowed in one transaction. Definitions and graph changes
  publish together. An earlier read transaction keeps its original definitions
  and graph snapshot.
- Required fails for absent or null. Type allows absent, rejects explicit null,
  and accepts exactly the declared bool, int64, float64 or string kind. Unique
  skips absent and null and rejects other non-null kinds. Unique uses
  makePropertyValueKey: kinds are distinct and float signed zero is canonical.
  No cross-label uniqueness. A multilabel node participates in every match.
- Check final state before both memory and page publication. That allows swaps in
  one transaction. Failed commits expose neither new definitions, graph data nor
  changefeed events. Return a stable violation sentinel and structured name,
  node ID, property and reason; never include values. Check definitions by name
  and conflicting node IDs in deterministic order.
- Use store-owned definitions; if encoded using the metadata machinery, reserve the
  prefix at the engine boundary, including raw metadata mutation paths. Persist
  definitions in snapshots, page catalog/records and deltas. The on-disk required
  feature marker must make v0.12.2 refuse constrained images, never silently open
  them as unconstrained. Leave the existing unconstrained formats readable as
  they are; do not add a migration step for them.
- Validate staged final state on snapshot/backup import and restore before any
  destination is published. Reuse the same checks; definitions-only commits and
  deltas must preserve the catalog and snapshot size accounting. Never skip
  constraint checks on vector batches, bulk imports, or query label mutation.
- Use one work and byte budget for commit validation. A managed query uses its
  remaining request budget through commit; manual transactions and other writers
  use a default limit of 16,777,216 work units and 256 MiB of validation working
  storage. Context-aware commit and restore APIs check cancellation. Charge
  scanned entities, decoded values and retained uniqueness keys; fail with a
  resource error without writing on exhaustion. Avoid scanning for transactions
  that do not change relevant graph data or definitions.
- Verify both backends, failed old-data admission, overlapping labels, typed
  equality, null and wrong-type failures, batch duplicates, same-transaction swap,
  definition/data atomicity, reopen and backup/restore, and corrupt raw import
  refusal before publication. Test failure leaves changefeed unchanged.
