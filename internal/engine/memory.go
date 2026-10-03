package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func openMemoryDB(ctx context.Context, opts OpenOptions) (*DB, error) {
	if opts.BackupDirectory != "" {
		return nil, fmt.Errorf("%w: BackupDirectory with :memory:", ErrUnsupportedOption)
	}
	if opts.Durability != DurabilityStandard {
		return nil, fmt.Errorf("%w: durability with :memory:", ErrUnsupportedOption)
	}
	graph := opts.preloadedGraph
	nextNodeID, nextEdgeID, commitID := opts.preloadedNextNodeID, opts.preloadedNextEdgeID, opts.preloadedCommitID
	if !opts.preloaded {
		graph = store.NewGraphState()
		if err := store.EnsureDatabaseID(graph); err != nil {
			return nil, err
		}
		if opts.EnableVector {
			graph.VectorDimensions = opts.VectorDimensions
			if graph.VectorDimensions == 0 {
				graph.VectorDimensions = 128
			}
		}
		nextNodeID, nextEdgeID = 1, 1
	}
	return newMemoryDB(ctx, opts, graph, nextNodeID, nextEdgeID, commitID)
}

func newMemoryDB(ctx context.Context, opts OpenOptions, graph *store.GraphState, nextNodeID, nextEdgeID, commitID uint64) (*DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.BackupDirectory != "" {
		return nil, fmt.Errorf("%w: BackupDirectory with in-memory database", ErrUnsupportedOption)
	}
	if opts.Durability != DurabilityStandard {
		return nil, fmt.Errorf("%w: durability with in-memory database", ErrUnsupportedOption)
	}
	if opts.WALCheckpointThresholdBytes == 0 {
		opts.WALCheckpointThresholdBytes = defaultWALCheckpointThresholdBytes
	}
	if opts.MaxDatabaseSnapshotBytes == 0 {
		opts.MaxDatabaseSnapshotBytes = defaultMaxDatabaseSnapshotBytes
	}
	if opts.ChangefeedMaxBytes == 0 {
		opts.ChangefeedMaxBytes = min(defaultChangefeedMaxBytes, max(uint64(1), opts.MaxDatabaseSnapshotBytes/8))
	}
	if opts.VectorIndexBuildMaxWork == 0 {
		opts.VectorIndexBuildMaxWork = defaultVectorBuildMaxWork
	}
	if opts.VectorIndexBuildMaxLogicalBytes == 0 {
		opts.VectorIndexBuildMaxLogicalBytes = defaultVectorBuildMaxLogicalBytes
	}
	if opts.DerivedIndexBuildMaxWork == 0 {
		opts.DerivedIndexBuildMaxWork = defaultDerivedBuildMaxWork
	}
	if opts.DerivedIndexBuildMaxLogicalBytes == 0 {
		opts.DerivedIndexBuildMaxLogicalBytes = defaultDerivedBuildMaxLogicalBytes
	}
	if opts.VectorDimensions != 0 && graph.VectorDimensions != 0 && opts.VectorDimensions != graph.VectorDimensions {
		return nil, fmt.Errorf("vector dimensions %d do not match stored dimensions %d", opts.VectorDimensions, graph.VectorDimensions)
	}
	if opts.EnableVector && graph.VectorDimensions == 0 {
		graph.VectorDimensions = opts.VectorDimensions
		if graph.VectorDimensions == 0 {
			graph.VectorDimensions = 128
		}
	}
	if _, err := ftsIndexDefinitions(graph); err != nil {
		return nil, err
	}
	graph.VectorIndexM = effectiveVectorIndexM(opts.VectorM)
	effectiveEnableVector := opts.EnableVector || graph.VectorDimensions != 0
	if len(opts.VectorNamespaces) != 0 && !effectiveEnableVector {
		return nil, fmt.Errorf("%w: vector namespaces require vector support", ErrUnsupportedOption)
	}
	namespaces, err := normalizeVectorNamespaces(opts.VectorNamespaces, graph.VectorDimensions)
	if err != nil {
		return nil, err
	}
	graph.VectorNamespaces = emptyVectorNamespaceStates(namespaces)
	graph.VectorNamespace = nil
	if effectiveEnableVector {
		if err := validateGraphVectorsContext(ctx, graph); err != nil {
			return nil, err
		}
		refreshVectorLiveCount(graph)
		if opts.VectorIndexMode == VectorIndexHNSWSynchronous {
			if err := rebuildAllVectorIndexesBudget(ctx, graph, opts.VectorIndexBuildMaxWork, opts.VectorIndexBuildMaxLogicalBytes); err != nil {
				return nil, err
			}
		}
	}
	ftsProperties, err := normalizeFTSProperties(opts.FTSProperties)
	if err != nil {
		return nil, err
	}
	graph.FTSProperties = nil
	if len(ftsProperties) != 0 {
		postings, work, logicalBytes, err := buildFTSPropertyPostings(ctx, graph, ftsProperties, &DB{derivedIndexBuildMaxWork: opts.DerivedIndexBuildMaxWork, derivedIndexBuildMaxLogicalBytes: opts.DerivedIndexBuildMaxLogicalBytes})
		if err != nil {
			return nil, err
		}
		graph.FTSProperties = postings
		graph.DerivedIndexWork = saturatingAdd(graph.DerivedIndexWork, work)
		graph.DerivedIndexLogicalBytes = saturatingAdd(graph.DerivedIndexLogicalBytes, logicalBytes)
		if graph.DerivedIndexWork > opts.DerivedIndexBuildMaxWork || graph.DerivedIndexLogicalBytes > opts.DerivedIndexBuildMaxLogicalBytes {
			return nil, fmt.Errorf("%w: FTS property index build exceeds derived-index budget", ErrResourceLimit)
		}
	}
	if graph.SnapshotBytes == 0 {
		graph.SnapshotBytes, err = store.EstimateSnapshotBytes(graph)
		if err != nil {
			return nil, err
		}
	}
	db := &DB{
		memory: true, path: ":memory:", graph: graph, nextNodeID: nextNodeID, nextEdgeID: nextEdgeID,
		readOnly:       opts.ReadOnly,
		reservedNodeID: nextNodeID, reservedEdgeID: nextEdgeID, commitID: commitID,
		enableVector: effectiveEnableVector, disableVectorIndex: opts.VectorIndexMode == VectorIndexExactOnly,
		vectorDimensions: graph.VectorDimensions, queryCache: map[string]*queryPlan{},
		walCheckpointThresholdBytes: opts.WALCheckpointThresholdBytes, changefeedMaxBytes: opts.ChangefeedMaxBytes,
		maxDatabaseSnapshotBytes: opts.MaxDatabaseSnapshotBytes, vectorIndexBuildMaxWork: opts.VectorIndexBuildMaxWork,
		vectorIndexBuildMaxLogicalBytes: opts.VectorIndexBuildMaxLogicalBytes,
		derivedIndexBuildMaxWork:        opts.DerivedIndexBuildMaxWork, derivedIndexBuildMaxLogicalBytes: opts.DerivedIndexBuildMaxLogicalBytes,
		maxGenerationLeases: opts.MaxGenerationLeases, maxRetainedGenerationLogicalBytes: opts.MaxRetainedGenerationLogicalBytes,
		generationLeases: map[*store.GraphState]*generationRetention{}, activeTransactions: map[*Tx]time.Time{},
		activeLeases: map[*GenerationLease]time.Time{}, writerWaits: map[uint64]time.Time{},
		streamNotify: map[string]*streamSubscription{}, adjacencyMaintenanceComplete: make(chan struct{}, 1),
	}
	db.checkpointAttemptCond.L = &db.checkpointWorkerMu
	if !db.readOnly {
		db.startCheckpointWorker()
	}
	return db, nil
}
