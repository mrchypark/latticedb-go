package store

import (
	"slices"
	"unique"
)

const maxInternedStringBytes = 1024

// InternString shares short strings using the standard library's weak table.
// Sharing is best effort across GC cycles; public values remain plain strings.
func InternString(value string) string {
	// ponytail: cap entries at 1 KiB; raise only for measured larger repeated values.
	if len(value) == 0 || len(value) > maxInternedStringBytes {
		return value
	}
	return unique.Make(value).Value()
}

func InternStrings(values []string) []string {
	out := slices.Clone(values)
	for i, value := range values {
		out[i] = InternString(value)
	}
	return out
}
