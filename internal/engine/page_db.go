package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func openPageDB(ctx context.Context, path string, files store.DatabaseFiles, lock *pathLock, opts OpenOptions) (result *DB, err error) {
	pagePath := files.State + ".pages"
	direct, err := pagestore.IsFile(files.State)
	if err != nil {
		_ = lock.close()
		return nil, pageStorageOpenError(err)
	}
	if direct {
		pagePath = files.State
	}
	var temporary string
	defer func() {
		if err != nil && temporary != "" {
			_ = os.RemoveAll(temporary)
		}
	}()
	_, statErr := os.Stat(pagePath)
	fresh := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !fresh {
		_ = lock.close()
		return nil, statErr
	}
	if fresh {
		_, stateErr := os.Stat(files.State)
		if stateErr != nil && !errors.Is(stateErr, os.ErrNotExist) {
			_ = lock.close()
			return nil, stateErr
		}
		hasRecoveryEvidence := stateErr == nil
		for _, evidence := range []string{files.WALBase, files.WAL} {
			if _, e := os.Stat(evidence); e == nil {
				hasRecoveryEvidence = true
			} else if !errors.Is(e, os.ErrNotExist) {
				_ = lock.close()
				return nil, e
			}
		}
		if hasRecoveryEvidence {
			if opts.ReadOnly {
				temporary, err = os.MkdirTemp("", "latticedb-page-read-*")
				if err != nil {
					_ = lock.close()
					return nil, err
				}
				pagePath = filepath.Join(temporary, "database.pages")
			}
			if err := store.MigrateToPages(ctx, files, pagePath, store.RecoveryLimits{MaxDecodedBytes: opts.RecoveryMaxDecodedBytes, MaxFrames: opts.RecoveryMaxFrames, MaxWork: opts.RecoveryMaxWork}); err != nil {
				_ = lock.close()
				return nil, pageStorageOpenError(err)
			}
			fresh = false
		}
	}
	if fresh && (!opts.Create || opts.ReadOnly) {
		_ = lock.close()
		return nil, os.ErrNotExist
	}
	if pagePath != files.State && temporary == "" {
		if e := checkLayoutOwner(pagePath, false, ""); e != nil {
			_ = lock.close()
			return nil, e
		}
	}
	if fresh {
		pagePath = files.State
	}
	pages, err := pagestore.Open(pagePath, pagestore.Options{ReadOnly: opts.ReadOnly})
	if err != nil {
		_ = lock.close()
		return nil, pageStorageOpenError(err)
	}
	var read *pagestore.Tx
	defer func() {
		if err != nil {
			if read != nil {
				_ = read.Rollback()
			}
			_ = pages.Close()
			_ = lock.close()
		}
	}()
	if fresh {
		graph := store.NewGraphState()
		if err = store.EnsureDatabaseID(graph); err != nil {
			return nil, err
		}
		dimensions := opts.VectorDimensions
		if opts.EnableVector && dimensions == 0 {
			dimensions = 128
		}
		write, e := pages.Begin(true)
		if e != nil {
			return nil, e
		}
		page := &store.PageGraph{Tx: write}
		c := store.PageCatalog{DatabaseID: graph.DatabaseID, VectorDimensions: dimensions, NextNodeID: 1, NextEdgeID: 1, History: sha256.Sum256([]byte(graph.DatabaseID))}
		if e = page.PutCatalog(c); e == nil {
			e = write.Put("commit-history", make([]byte, 8), c.History[:])
		}
		if e == nil {
			e = write.Commit()
		}
		if e != nil {
			_ = write.Rollback()
			return nil, e
		}
	}
	read, err = pages.Begin(false)
	if err != nil {
		return nil, err
	}
	graph, catalog, err := (&store.PageGraph{Tx: read}).LoadGraph(ctx)
	if err != nil {
		return nil, err
	}
	if opts.MaxDatabaseSnapshotBytes != 0 && graph.SnapshotBytes > opts.MaxDatabaseSnapshotBytes {
		return nil, fmt.Errorf("%w: database snapshot requires %d bytes, limit is %d", ErrResourceLimit, graph.SnapshotBytes, opts.MaxDatabaseSnapshotBytes)
	}
	if pagePath != files.State {
		if _, e := os.Stat(files.State); e == nil {
			sourceID, e := store.CheckpointDatabaseID(files.State)
			if e != nil {
				return nil, e
			}
			if sourceID != catalog.DatabaseID {
				return nil, ErrDatabaseLayoutConflict
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
	}
	if opts.VectorDimensions != 0 && catalog.VectorDimensions != 0 && opts.VectorDimensions != catalog.VectorDimensions {
		return nil, fmt.Errorf("vector dimensions do not match stored dimensions")
	}
	dimensionsChanged := opts.EnableVector && catalog.VectorDimensions == 0
	if dimensionsChanged {
		dimensions := opts.VectorDimensions
		if dimensions == 0 {
			dimensions = 128
		}
		catalog.VectorDimensions = dimensions
		catalog.ArchiveBasePending = true
		graph.VectorDimensions = dimensions
	}
	if opts.EnableVector || graph.VectorDimensions != 0 {
		var vectorErr error
		if graph.PageBase != nil {
			vectorErr = validatePageGraphVectorsContext(ctx, graph.PageBase, graph.VectorDimensions)
		} else {
			vectorErr = validateGraphVectorsContext(ctx, graph)
		}
		if vectorErr != nil {
			return nil, vectorErr
		}
	}
	graph.VectorIndexM = effectiveVectorIndexM(opts.VectorM)
	namespaces, e := normalizeVectorNamespaces(opts.VectorNamespaces, graph.VectorDimensions)
	if e != nil {
		return nil, e
	}
	if len(namespaces) != 0 && !(opts.EnableVector || graph.VectorDimensions != 0) {
		return nil, fmt.Errorf("%w: vector namespaces require vector support", ErrUnsupportedOption)
	}
	graph.VectorNamespaces = emptyVectorNamespaceStates(namespaces)
	if _, e := normalizeFTSProperties(opts.FTSProperties); e != nil {
		return nil, e
	}
	if !opts.ReadOnly {
		if pagePath != files.State {
			if e := ensureLayoutOwner(pagePath, false, graph.DatabaseID); e != nil {
				return nil, e
			}
		}
		if e := ensureLayoutOwner(files.State, files.State == path, graph.DatabaseID); e != nil {
			return nil, e
		}
	}
	reservedNode, reservedEdge, err := store.LoadIDReservationFiles(files, graph.DatabaseID)
	if err != nil {
		return nil, err
	}
	if dimensionsChanged && !opts.ReadOnly {
		_ = read.Rollback()
		read = nil
		write, e := pages.Begin(true)
		if e != nil {
			return nil, e
		}
		if catalog.CommitID == ^uint64(0) {
			_ = write.Rollback()
			return nil, errors.New("commit id space exhausted")
		}
		catalog.VectorDimensions = graph.VectorDimensions
		if e = (&store.PageGraph{Tx: write}).PutCatalog(catalog); e != nil {
			_ = write.Rollback()
			return nil, e
		}
		catalog, _, e = (&store.PageGraph{Tx: write}).PageCommit(ctx, graph, max(catalog.NextNodeID, reservedNode), max(catalog.NextEdgeID, reservedEdge), catalog.CommitID+1, store.GraphDelta{}, opts.BackupDirectory != "")
		if e == nil {
			e = write.Commit()
		}
		if e != nil {
			_ = write.Rollback()
			return nil, e
		}
		read, e = pages.Begin(false)
		if e != nil {
			return nil, e
		}
		graph, catalog, err = (&store.PageGraph{Tx: read}).LoadGraph(ctx)
		if err != nil {
			return nil, err
		}
		graph.VectorIndexM = effectiveVectorIndexM(opts.VectorM)
		graph.VectorNamespaces = emptyVectorNamespaceStates(namespaces)
	}
	db := &DB{
		path: path, files: files, graph: graph, pages: pages, pathLock: lock, pageTemporary: temporary,
		nextNodeID: max(catalog.NextNodeID, reservedNode), nextEdgeID: max(catalog.NextEdgeID, reservedEdge),
		reservedNodeID: reservedNode, reservedEdgeID: reservedEdge, commitID: catalog.CommitID, readOnly: opts.ReadOnly,
		enableVector: opts.EnableVector || catalog.VectorDimensions != 0, disableVectorIndex: opts.VectorIndexMode == VectorIndexExactOnly, vectorDimensions: catalog.VectorDimensions,
		changefeedMaxBytes: opts.ChangefeedMaxBytes, maxDatabaseSnapshotBytes: opts.MaxDatabaseSnapshotBytes,
		vectorIndexBuildMaxWork: opts.VectorIndexBuildMaxWork, vectorIndexBuildMaxLogicalBytes: opts.VectorIndexBuildMaxLogicalBytes,
		derivedIndexBuildMaxWork: opts.DerivedIndexBuildMaxWork, derivedIndexBuildMaxLogicalBytes: opts.DerivedIndexBuildMaxLogicalBytes,
		maxGenerationLeases: opts.MaxGenerationLeases, maxRetainedGenerationLogicalBytes: opts.MaxRetainedGenerationLogicalBytes,
		generationLeases: map[*store.GraphState]*generationRetention{}, activeTransactions: map[*Tx]time.Time{}, activeLeases: map[*GenerationLease]time.Time{}, writerWaits: map[uint64]time.Time{},
		queryCache: map[string]*queryPlan{}, streamNotify: map[string]*streamSubscription{}, fullSync: opts.Durability == DurabilityFull,
	}
	db.checkpointAttemptCond.L = &db.checkpointWorkerMu
	if opts.BackupDirectory != "" {
		if opts.ReadOnly {
			return nil, fmt.Errorf("%w: BackupDirectory requires a writable open", ErrInvalidArgument)
		}
		archive, e := openBackupArchive(opts.BackupDirectory, files.State, graph.DatabaseID)
		if e != nil {
			return nil, e
		}
		if catalog.ArchiveBasePending {
			_, e = archive.capturePage(ctx, db.timeNow(), graph, db.nextNodeID, db.nextEdgeID, db.commitID)
			if e == nil {
				e = clearPageArchiveBasePending(pages, &read, &graph, &catalog, opts.VectorM, namespaces)
				if e == nil {
					db.graph = graph
					db.commitID = catalog.CommitID
				}
			}
		} else {
			_, e = archive.capturePage(ctx, db.timeNow(), graph, db.nextNodeID, db.nextEdgeID, db.commitID)
		}
		if e != nil {
			_ = archive.close()
			return nil, e
		}
		db.backupArchive = archive
	}
	return db, nil
}

func validatePageGraphVectorsContext(ctx context.Context, page *store.PageGraph, dimensions uint16) error {
	return page.VisitNodes(ctx, func(node *store.NodeRecord) error {
		return validateNodeVectors(dimensions, node)
	})
}

func clearPageArchiveBasePending(pages *pagestore.DB, read **pagestore.Tx, graph **store.GraphState, catalog *store.PageCatalog, vectorM uint16, namespaces []VectorNamespace) error {
	if *read != nil {
		if err := (*read).Rollback(); err != nil {
			return err
		}
		*read = nil
	}
	write, err := pages.Begin(true)
	if err != nil {
		return err
	}
	page := &store.PageGraph{Tx: write}
	current, err := page.Catalog()
	if err == nil && current.ArchiveBasePending {
		current.ArchiveBasePending = false
		err = page.PutCatalog(current)
	}
	if err == nil {
		err = write.Commit()
	}
	if err != nil {
		_ = write.Rollback()
		return err
	}
	*read, err = pages.Begin(false)
	if err != nil {
		return err
	}
	*graph, *catalog, err = (&store.PageGraph{Tx: *read}).LoadGraph(context.Background())
	if err != nil {
		_ = (*read).Rollback()
		*read = nil
		return err
	}
	(*graph).VectorIndexM = effectiveVectorIndexM(vectorM)
	(*graph).VectorNamespaces = emptyVectorNamespaceStates(namespaces)
	return nil
}

func (tx *Tx) commitPages(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !tx.changefeedApplied {
		base := store.CloneGraphStateShallow(tx.base)
		for _, ids := range []map[uint64]struct{}{tx.changes.upsertNodes, tx.changes.deleteNodes} {
			for id := range ids {
				old, err := tx.base.ReadNode(id)
				if err != nil {
					return err
				}
				if old != nil {
					base.Nodes.CloneShardOnce(id)
					base.Nodes.Set(id, old)
				}
			}
		}
		for _, ids := range []map[uint64]struct{}{tx.changes.upsertEdges, tx.changes.deleteEdges} {
			for id := range ids {
				old, err := tx.base.ReadEdge(id)
				if err != nil {
					return err
				}
				if old != nil {
					base.Edges.CloneShardOnce(id)
					base.Edges.Set(id, old)
				}
			}
		}
		for _, ids := range []map[uint64]struct{}{tx.changes.upsertFTS, tx.changes.deleteFTS} {
			for id := range ids {
				old, err := tx.base.ReadFTS(id)
				if err != nil {
					return err
				}
				if old != nil {
					base.FTS.CloneShardOnce(id)
					base.FTS.Set(id, old)
				}
			}
		}
		tx.base = base
		if err := tx.appendChangefeed(); err != nil {
			return err
		}
		tx.changefeedApplied = true
	}
	delta := store.GraphDelta{
		UpsertNodes: mapKeys(tx.changes.upsertNodes), DeleteNodes: mapKeys(tx.changes.deleteNodes),
		UpsertEdges: mapKeys(tx.changes.upsertEdges), DeleteEdges: mapKeys(tx.changes.deleteEdges),
		UpsertFTS: mapKeys(tx.changes.upsertFTS), DeleteFTS: mapKeys(tx.changes.deleteFTS),
		AppMetadata:       persistedAppMetadataChanges(tx.changes.appMetadata),
		CreateNodeIndexes: propertyIndexKeys(tx.changes.createNodeIndexes), DropNodeIndexes: propertyIndexKeys(tx.changes.dropNodeIndexes),
		CreateEdgeIndexes: propertyIndexKeys(tx.changes.createEdgeIndexes), DropEdgeIndexes: propertyIndexKeys(tx.changes.dropEdgeIndexes),
		NodePropertyKeys: propertyKeyDeltas(tx.changes.upsertNodes, tx.changes.nodePropertyKeys), EdgePropertyKeys: propertyKeyDeltas(tx.changes.upsertEdges, tx.changes.edgePropertyKeys),
		StreamsChanged: tx.changes.streamsChanged, StreamOperations: tx.changes.streamOperations,
	}
	size, err := store.ApplyDeltaSnapshotBytes(tx.base, tx.graph, delta)
	if err != nil {
		return err
	}
	if limit := tx.db.maxDatabaseSnapshotBytes; limit != 0 && size > limit {
		return fmt.Errorf("%w: database snapshot requires %d bytes, limit is %d", ErrResourceLimit, size, limit)
	}
	tx.graph.SnapshotBytes = size
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if db.recoveryRequired {
		return ErrRecoveryRequired
	}
	if db.commitID != tx.changes.baseCommitID {
		return ErrWriteConflict
	}
	if db.commitID == ^uint64(0) {
		return errors.New("commit id space exhausted")
	}
	write, err := db.pages.Begin(true)
	if err != nil {
		return err
	}
	defer write.Rollback()
	page := &store.PageGraph{Tx: write}
	catalog, _, err := page.PageCommit(ctx, tx.graph, db.nextNodeID, db.nextEdgeID, db.commitID+1, delta, db.backupArchive != nil)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	old := db.graph
	unpinned := db.generationLeases[old] == nil
	if unpinned {
		if err = old.PageBase.Tx.Rollback(); err != nil {
			return err
		}
	}
	if err = write.Commit(); err != nil {
		if unpinned {
			read, e := db.pages.Begin(false)
			if e != nil {
				db.recoveryRequired = true
				return errors.Join(err, e)
			}
			reloaded, _, e := (&store.PageGraph{Tx: read}).LoadGraph(context.Background())
			if e != nil {
				_ = read.Rollback()
				db.recoveryRequired = true
				return errors.Join(err, e)
			}
			reloaded.VectorIndexM = old.VectorIndexM
			reloaded.VectorNamespaces = old.VectorNamespaces
			db.graph = reloaded
		}
		if errors.Is(err, pagestore.ErrSnapshotGrowth) || errors.Is(err, pagestore.ErrSnapshotWriteLimit) {
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
	graph, _, err := (&store.PageGraph{Tx: read}).LoadGraph(context.Background())
	if err != nil {
		_ = read.Rollback()
		db.recoveryRequired = true
		return errors.Join(ErrCommitOutcomeUnknown, err)
	}
	graph.VectorIndexM = old.VectorIndexM
	graph.VectorNamespaces = old.VectorNamespaces
	db.graph = graph
	db.commitID = catalog.CommitID
	if archive := db.backupArchive; archive != nil {
		if _, err := archive.capturePage(context.Background(), db.timeNow(), graph, db.nextNodeID, db.nextEdgeID, db.commitID); err != nil {
			db.recoveryRequired = true
			return errors.Join(ErrCommitOutcomeUnknown, err)
		}
	}
	db.notifyStreamsLocked(delta.StreamOperations)
	return nil
}

func pageStorageOpenError(err error) error {
	if errors.Is(err, store.ErrLoadResourceLimit) {
		return fmt.Errorf("%w: %w", ErrResourceLimit, err)
	}
	if errors.Is(err, pagestore.ErrUnsupportedPlatform) {
		return fmt.Errorf("%w: %w", ErrUnsupportedOption, err)
	}
	return err
}

func (db *DB) checkpointPages(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
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
	if db.activeTx.Load() != 0 {
		return ErrTransactionsActive
	}
	if err := db.pages.Sync(); err != nil {
		return err
	}
	db.checkpointCount++
	return nil
}
