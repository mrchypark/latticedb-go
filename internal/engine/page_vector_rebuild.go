package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// rebuildPageVectorTargetContext publishes an index-only transaction. The
// existing graph remains available to pinned leases; an unpinned reader is
// replaced only after the write transaction has committed.
func (db *DB) rebuildPageVectorTargetContext(ctx context.Context, namespace *VectorNamespace) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for !db.writeMu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer db.writeMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if db.recoveryRequired {
		return ErrRecoveryRequired
	}
	if !db.enableVector || db.disableVectorIndex {
		return fmt.Errorf("%w: synchronous HNSW mode is not enabled", ErrUnsupportedOption)
	}
	old := db.graph
	if old == nil || old.PageBase == nil {
		return fmt.Errorf("%w: no page graph", ErrUnsupportedOption)
	}
	write, err := db.pages.Begin(true)
	if err != nil {
		return err
	}
	defer write.Rollback()
	page := &store.PageGraph{Tx: write}
	build := *old
	build.PageBase = page
	view := &build
	name := "default"
	var selectVector func(*store.NodeRecord) ([]float32, bool)
	dimensions := build.VectorDimensions
	if namespace != nil {
		resolved, e := resolveVectorNamespace(old, namespace)
		if e != nil {
			_ = write.Rollback()
			return e
		}
		name = pageVectorNamespaceKey(resolved)
		view = vectorNamespaceFacade(&build, *resolved)
		view.VectorDimensions = resolved.Dimensions
		dimensions = resolved.Dimensions
		selectVector = func(n *store.NodeRecord) ([]float32, bool) { return selectedVector(view, n) }
	} else {
		selectVector = func(n *store.NodeRecord) ([]float32, bool) { return selectedVector(view, n) }
	}
	budget := &directSearchBudget{ctx: ctx, maxWork: db.vectorIndexBuildMaxWork, maxBytes: db.vectorIndexBuildMaxLogicalBytes, annVisitedLimit: ^uint64(0)}
	maxPersistent := db.vectorIndexBuildMaxLogicalBytes
	if err = rebuildPageVectorIndex(ctx, page, name, selectVector, dimensions, configuredVectorIndexM(&build), maxPersistent, budget); err != nil {
		_ = write.Rollback()
		return err
	}
	if err = ctx.Err(); err != nil {
		_ = write.Rollback()
		return err
	}
	if old.PageBase.SearchIndexesCurrent {
		catalog, e := page.Catalog()
		if e != nil {
			_ = write.Rollback()
			return e
		}
		if err = page.MarkSearchIndexesCurrent(catalog.History); err != nil {
			_ = write.Rollback()
			return err
		}
	}
	unpinned := db.generationLeases[old] == nil
	if unpinned {
		if err = old.PageBase.Tx.Rollback(); err != nil {
			_ = write.Rollback()
			return err
		}
	}
	if err = write.Commit(); err != nil {
		if errors.Is(err, pagestore.ErrSnapshotGrowth) || errors.Is(err, pagestore.ErrSnapshotWriteLimit) {
			if unpinned {
				read, reopenErr := db.pages.Begin(false)
				if reopenErr == nil {
					fresh, _, loadErr := (&store.PageGraph{Tx: read}).LoadGraph(context.Background())
					if loadErr == nil {
						fresh.VectorIndexM, fresh.VectorNamespaces, fresh.FTSProperties = old.VectorIndexM, old.VectorNamespaces, old.FTSProperties
						db.graph = fresh
					} else {
						_ = read.Rollback()
						reopenErr = loadErr
					}
				}
				if reopenErr != nil {
					db.recoveryRequired = true
					return errors.Join(fmt.Errorf("%w: %w", ErrResourceLimit, err), reopenErr)
				}
			}
			return fmt.Errorf("%w: %w", ErrResourceLimit, err)
		}
		db.recoveryRequired = true
		return errors.Join(ErrCommitOutcomeUnknown, err)
	}
	read, err := db.pages.Begin(false)
	if err != nil {
		db.recoveryRequired = true
		return errors.Join(ErrCommitOutcomeUnknown, err)
	}
	fresh, _, err := (&store.PageGraph{Tx: read}).LoadGraph(context.Background())
	if err != nil {
		_ = read.Rollback()
		db.recoveryRequired = true
		return errors.Join(ErrCommitOutcomeUnknown, err)
	}
	fresh.VectorIndexM = old.VectorIndexM
	fresh.VectorNamespaces = old.VectorNamespaces
	fresh.FTSProperties = old.FTSProperties
	db.graph = fresh
	db.vectorRebuilds.Add(1)
	return nil
}
