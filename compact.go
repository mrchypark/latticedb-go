package latticedb

import "context"

// Compact rewrites live disk pages to reclaim unused file space. Close active
// transactions and snapshots first. Memory databases require no compaction.
func (db *DB) Compact() error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.Compact())
}

// CompactContext waits for the writer slot. Cancellation before publication
// preserves the original file. File publication and reopening are not canceled.
func (db *DB) CompactContext(ctx context.Context) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CompactContext(ctx))
}
