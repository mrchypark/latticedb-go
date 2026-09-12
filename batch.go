package latticedb

import (
	"context"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

// Batch coalesces concurrent callbacks into one managed transaction. Success
// means the group's WAL commit has been synchronized with the configured
// durability. Any callback failure rolls back the whole group; errors can come
// from a peer. Callbacks run sequentially, without automatic retries.
//
// Collection waits up to 1ms or 32 requests. At most 32 further requests may
// wait during execution; a full queue returns ErrResourceLimit. Other write
// APIs retain their existing writer contention behavior.
//
// A callback panic is re-raised in its own Batch caller after rollback; peers
// receive an error. Do not retain the Tx or call Batch on this DB from inside
// the callback. A nil callback returns an error.
func (db *DB) Batch(fn func(*Tx) error) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	if fn == nil {
		return wrapError(inner.Batch(nil))
	}
	var callbackErr error
	err = inner.Batch(func(tx *engine.Tx) error {
		callbackErr = fn(&Tx{inner: tx})
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return wrapError(err)
}

// BatchContext is Batch with an explicit context for cancellation.
// Cancellation observed before WAL commit aborts the collected group. Once
// the WAL write begins, the commit can still succeed. The caller waits for
// execution to finish even when ctx is canceled; callbacks are not interrupted.
func (db *DB) BatchContext(ctx context.Context, fn func(*Tx) error) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	if fn == nil {
		return wrapError(inner.BatchContext(ctx, nil))
	}
	var callbackErr error
	err = inner.BatchContext(ctx, func(tx *engine.Tx) error {
		callbackErr = fn(&Tx{inner: tx})
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return wrapError(err)
}
