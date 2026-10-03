package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// Compact returns unused disk space to the filesystem. Active readers and
// snapshots must close first. Memory databases have no disk space to reclaim.
func (db *DB) Compact() error {
	if !db.writeMu.TryLock() {
		return ErrWriteTxActive
	}
	defer db.writeMu.Unlock()
	return db.compactWithWriterHeld(context.Background())
}

func (db *DB) CompactContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for !db.writeMu.TryLock() {
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer db.writeMu.Unlock()
	return db.compactWithWriterHeld(ctx)
}

func (db *DB) compactWithWriterHeld(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.closed {
		return ErrDatabaseClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if db.recoveryRequired {
		return ErrRecoveryRequired
	}
	if db.activeTx.Load() != 0 || db.activeGenerationLeases != 0 {
		return ErrTransactionsActive
	}
	if db.pages == nil {
		return nil
	}
	if db.pathLock == nil {
		return fmt.Errorf("%w: compaction requires database locking", ErrUnsupportedOption)
	}
	// Release the engine-owned read transaction only after excluding every lease.
	old := db.graph
	if err := old.PageBase.Tx.Rollback(); err != nil {
		db.recoveryRequired = true
		return err
	}
	compactErr := db.pages.Compact(ctx)
	read, err := db.pages.Begin(false)
	if err != nil {
		db.recoveryRequired = true
		return errors.Join(compactErr, err)
	}
	graph, _, err := (&store.PageGraph{Tx: read}).LoadGraph(context.Background())
	if err != nil {
		_ = read.Rollback()
		db.recoveryRequired = true
		return errors.Join(compactErr, err)
	}
	graph.VectorIndexM = old.VectorIndexM
	graph.VectorNamespaces = old.VectorNamespaces
	graph.FTSProperties = old.FTSProperties
	db.graph = graph
	if compactErr != nil && !errors.Is(compactErr, context.Canceled) && !errors.Is(compactErr, context.DeadlineExceeded) {
		db.recoveryRequired = true
	}
	return compactErr
}
