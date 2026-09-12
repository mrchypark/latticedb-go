# Changefeed value omission

Automatic changefeed property events always include a value marker:

- `new_value_omitted` or `old_value_omitted` is `false` when the value is present.
- The marker is `true` when the value exceeds the inline limit and the value is
  replaced with an omission envelope.

Consumers must use the marker instead of inferring omission from the value's
map shape. A user value may itself be a map matching the legacy omission
envelope; its marker remains `false` and its map is preserved unchanged.

Events written before these markers existed have no marker and therefore have
unknown omission status. Consumers should treat a missing marker as legacy
unknown rather than as either `true` or `false`.
