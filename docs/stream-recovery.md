# Stream payload recovery

Snapshot and WAL stream payloads now use the same per-value limits as live
`PublishStream`: nesting depth 64, one million aggregate list/map entries, and
64 MiB of aggregate string, key, byte-slice, and vector data. Vector bytes count
as four bytes per element. Invalid UTF-8 and non-finite numbers are rejected.
The persisted value tree is checked before allocating its decoded copy.

Reserved automatic property-change events use the graph-property depth
convention: each old/new property value starts at depth zero. Other envelopes
use ordinary stream depth limits.
This preserves existing events containing a maximum-depth graph property;
ordinary streams count their root value at depth zero. Aggregate limits still
apply to envelope metadata independently. Property change events validate each
`key` and `old_value`/`new_value` pair with the same property budget as live
normalization, including UTF-8 validation, so a maximum-length property key
with a nil value remains valid.

Valid stored payloads retain their representation. Legacy or manually produced
payloads outside these limits fail recovery; they are not silently dropped or
truncated. No automatic migration or relaxed recovery mode is introduced.
