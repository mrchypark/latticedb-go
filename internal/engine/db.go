package engine

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

var (
	ErrReadOnly                       = errors.New("database is read-only")
	ErrWriteTxActive                  = errors.New("write transaction is already active")
	ErrManagedTransaction             = errors.New("managed transaction cannot be completed directly")
	ErrInactiveTx                     = errors.New("transaction is inactive")
	ErrDatabaseClosed                 = errors.New("database is closed")
	ErrTransactionsActive             = errors.New("database has active transactions")
	ErrSnapshotActive                 = errors.New("database has an active snapshot")
	ErrWriteConflict                  = errors.New("write transaction conflicts with current commit")
	ErrRecoveryRequired               = errors.New("database requires close and recovery")
	ErrResourceLimit                  = errors.New("resource limit exceeded")
	ErrAlreadyExists                  = errors.New("already exists")
	ErrInvalidArgument                = errors.New("invalid argument")
	ErrVectorIndexMaintenanceRequired = errors.New("vector index maintenance required")
	ErrUnsupportedOption              = errors.New("unsupported option")
	ErrCommitOutcomeUnknown           = store.ErrCommitOutcomeUnknown
)

type QueryErrorStage uint8

const (
	QueryErrorStageParse QueryErrorStage = iota + 1
	QueryErrorStageExecution
)

type QueryError struct {
	Stage QueryErrorStage
	Err   error
}

func (e *QueryError) Error() string { return e.Err.Error() }
func (e *QueryError) Unwrap() error { return e.Err }

const queryCacheEntries = 128
const maxQueryBytes = 32 << 10
const idReservationBlock = 1024
const defaultWALCheckpointThresholdBytes = 64 << 20
const defaultChangefeedMaxBytes = 64 << 20
const defaultMaxDatabaseSnapshotBytes = 512 << 20
const defaultRecoveryMaxDecodedBytes = 4 << 30
const defaultRecoveryMaxFrames = 1_000_000
const defaultRecoveryMaxWork = 1_000_000_000
const defaultSearchMaxWork = 10_000_000
const defaultSearchMaxBytes = 64 << 20
const maxSearchResults = 1_000_000
const defaultVectorBuildMaxWork = 100_000_000_000
const defaultVectorBuildMaxLogicalBytes = 16 << 30
const defaultDerivedBuildMaxWork = 100_000_000_000
const defaultDerivedBuildMaxLogicalBytes = 16 << 30

type OpenOptions struct {
	Create                           bool
	ReadOnly                         bool
	DisableLock                      bool
	CacheSizeMB                      uint32
	PageSize                         uint32
	EnableVector                     bool
	VectorIndexMode                  VectorIndexMode
	VectorDimensions                 uint16
	Durability                       DurabilityMode
	WALCheckpointThresholdBytes      uint64
	ChangefeedMaxBytes               uint64
	MaxDatabaseSnapshotBytes         uint64
	RecoveryMaxDecodedBytes          uint64
	RecoveryMaxFrames                uint64
	RecoveryMaxWork                  uint64
	VectorIndexBuildMaxWork          uint64
	VectorIndexBuildMaxLogicalBytes  uint64
	DerivedIndexBuildMaxWork         uint64
	DerivedIndexBuildMaxLogicalBytes uint64
	walSync                          func(*os.File) error
	walWrite                         func(*os.File, []byte) (int, error)
	walTruncate                      func(*os.File, int64) error
	walCleanupSync                   func(*os.File) error
	reserveIDs                       func(store.DatabaseFiles, string, uint64, uint64) error
	checkpointPrepare                func(string, *store.GraphState, uint64, uint64, uint64) error
	checkpointPublish                func()
	checkpointTryLockFailed          func()
	checkpointBeforeFinalTryLock     func()
	checkpointComplete               chan struct{}
	checkpoint                       func(string, *store.GraphState, uint64, uint64, uint64) error
	preloadedGraph                   *store.GraphState
	preloadedNextNodeID              uint64
	preloadedNextEdgeID              uint64
	preloadedCommitID                uint64
	preloaded                        bool
}

type DurabilityMode uint8
type VectorIndexMode uint8

const (
	DurabilityStandard DurabilityMode = iota
	DurabilityFull
)

const (
	VectorIndexExactOnly VectorIndexMode = iota
	VectorIndexHNSWSynchronous
)

type CreateNodeOptions struct {
	Labels     []string
	Properties map[string]any
}

type CreateEdgeOptions struct {
	Properties map[string]any
}

type Node struct {
	ID         uint64
	Labels     []string
	Properties map[string]any
}

type Edge struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]any
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]any
}

type QueryOptions struct {
	MaxRows  uint64
	MaxWork  uint64
	MaxBytes uint64
}

type VectorSearchOptions struct {
	K        uint32
	EfSearch uint16
	Exact    bool
	MaxWork  uint64
	MaxBytes uint64
}

type FTSSearchOptions struct {
	Limit         uint32
	MaxDistance   uint32
	MinTermLength uint32
	MaxWork       uint64
	MaxBytes      uint64
}

type QueryCacheStats struct {
	Entries uint32
	Hits    uint64
	Misses  uint64
}

type VectorIndexStats struct {
	LiveEntries                uint64
	IndexEntries               uint64
	Tombstones                 uint64
	TombstoneBytes             uint64
	TombstoneBytesUntilRebuild uint64
	MutationDebt               uint64
	RebuildThreshold           uint64
	DebtUntilRebuild           uint64
	EstimatedBuildLogicalBytes uint64
	ExactFallbacks             uint64
	Rebuilds                   uint64
	RebuildNanoseconds         uint64
}

type VectorSearchResult struct {
	NodeID   uint64
	Distance float32
}

type FTSSearchResult struct {
	NodeID uint64
	Score  float32
}

type DB struct {
	mu                               sync.RWMutex
	writeMu                          sync.Mutex
	cacheMu                          sync.RWMutex
	path                             string
	files                            store.DatabaseFiles
	graph                            *store.GraphState
	nextNodeID                       uint64
	nextEdgeID                       uint64
	reservedNodeID                   uint64
	reservedEdgeID                   uint64
	commitID                         uint64
	readOnly                         bool
	enableVector                     bool
	disableVectorIndex               bool
	vectorDimensions                 uint16
	queryCache                       map[string]*queryPlan
	queryCacheKeys                   [queryCacheEntries]string
	queryCacheNext                   uint32
	cacheHits                        atomic.Uint64
	cacheMisses                      atomic.Uint64
	vectorExactFallbacks             atomic.Uint64
	vectorRebuilds                   atomic.Uint64
	vectorRebuildNanos               atomic.Uint64
	vectorRebuild                    *vectorRebuildState
	vectorRebuildBeforeBuild         func()
	activeTx                         atomic.Int64
	closed                           bool
	recoveryRequired                 bool
	dirty                            bool
	checkpointCount                  uint64
	walCheckpointThresholdBytes      uint64
	changefeedMaxBytes               uint64
	maxDatabaseSnapshotBytes         uint64
	vectorIndexBuildMaxWork          uint64
	vectorIndexBuildMaxLogicalBytes  uint64
	derivedIndexBuildMaxWork         uint64
	derivedIndexBuildMaxLogicalBytes uint64
	fullSync                         bool
	walSync                          func(*os.File) error
	walWrite                         func(*os.File, []byte) (int, error)
	walTruncate                      func(*os.File, int64) error
	walCleanupSync                   func(*os.File) error
	reserveIDs                       func(store.DatabaseFiles, string, uint64, uint64) error
	checkpointPrepare                func(string, *store.GraphState, uint64, uint64, uint64) error
	checkpointPublish                func()
	checkpointTryLockFailed          func()
	checkpointBeforeFinalTryLock     func()
	checkpointComplete               chan struct{}
	checkpoint                       func(string, *store.GraphState, uint64, uint64, uint64) error
	checkpointWorkerMu               sync.Mutex
	checkpointQueued                 bool
	checkpointNeeded                 atomic.Bool
	adjacencyMaintenanceNeeded       atomic.Bool
	adjacencyCompactor               *store.AdjacencyCompactor
	adjacencyCompactorGraph          *store.GraphState
	adjacencyCompactorCommit         uint64
	adjacencyMaintenanceQueue        []adjacencyCandidate
	adjacencyMaintenanceQueued       map[adjacencyCandidate]struct{}
	adjacencyCompactorCandidate      adjacencyCandidate
	adjacencyCompactorActive         bool
	checkpointAttemptCond            sync.Cond
	checkpointAttemptActive          bool
	checkpointAttemptEpoch           uint64
	checkpointWake                   chan struct{}
	checkpointStop                   chan struct{}
	checkpointStopOnce               sync.Once
	checkpointDone                   chan struct{}
	checkpointPending                *checkpointGeneration
	checkpointPrepared               *store.PreparedCheckpoint
	checkpointInFlight               atomic.Bool
	pathLock                         *pathLock
	wal                              *store.WALWriter
	temporary                        bool
	streamNotify                     chan struct{}
	activeSnapshot                   bool
}

type checkpointGeneration struct {
	graph      *store.GraphState
	nextNodeID uint64
	nextEdgeID uint64
	commitID   uint64
}

type vectorRebuildDelta struct {
	id            uint64
	before, after []float32
}

type vectorRebuildState struct {
	graph             *store.GraphState
	dimensions        uint16
	maxWork, maxBytes uint64
	buildBytes        uint64
	deltas            []vectorRebuildDelta
	logWork, logBytes uint64
	err               error
	done              chan struct{}
	cancel            context.CancelFunc
}

type Tx struct {
	db                     *DB
	readOnly               bool
	base                   *store.GraphState
	graph                  *store.GraphState
	writeLocked            bool
	managed                bool
	closed                 bool
	changes                *txChanges
	vectorIndexApplied     bool
	vectorLiveCountApplied bool
	propertyIndexesApplied bool
	changefeedApplied      bool
	appMetadataWritable    bool
	queryIndexesDisabled   bool
}

type adjacencyCandidate struct {
	direction uint8
	nodeID    uint64
}

type txChanges struct {
	upsertNodes       map[uint64]struct{}
	deleteNodes       map[uint64]struct{}
	upsertEdges       map[uint64]struct{}
	deleteEdges       map[uint64]struct{}
	upsertFTS         map[uint64]struct{}
	deleteFTS         map[uint64]struct{}
	appMetadata       map[string]appMetadataChange
	createNodeIndexes map[store.PropertyIndexDefinition]struct{}
	dropNodeIndexes   map[store.PropertyIndexDefinition]struct{}
	createEdgeIndexes map[store.PropertyIndexDefinition]struct{}
	dropEdgeIndexes   map[store.PropertyIndexDefinition]struct{}
	streamsChanged    bool
	streamOperations  []store.StreamOperation
	baseCommitID      uint64
}

func newTxChanges(baseCommitID uint64) *txChanges {
	return &txChanges{
		baseCommitID:      baseCommitID,
		createNodeIndexes: map[store.PropertyIndexDefinition]struct{}{},
		dropNodeIndexes:   map[store.PropertyIndexDefinition]struct{}{},
		createEdgeIndexes: map[store.PropertyIndexDefinition]struct{}{},
		dropEdgeIndexes:   map[store.PropertyIndexDefinition]struct{}{},
	}
}

func Open(path string, opts OpenOptions) (*DB, error) {
	return OpenContext(context.Background(), path, opts)
}

func OpenContext(ctx context.Context, path string, opts OpenOptions) (*DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.ReadOnly && opts.Create {
		return nil, errors.New("read-only database cannot be created")
	}
	if opts.CacheSizeMB != 0 {
		return nil, fmt.Errorf("%w: CacheSizeMB", ErrUnsupportedOption)
	}
	if opts.PageSize != 0 {
		return nil, fmt.Errorf("%w: PageSize", ErrUnsupportedOption)
	}
	if opts.VectorDimensions != 0 && !opts.EnableVector {
		return nil, errors.New("VectorDimensions requires EnableVector")
	}
	if opts.Durability != DurabilityStandard && opts.Durability != DurabilityFull {
		return nil, fmt.Errorf("invalid durability mode %d", opts.Durability)
	}
	if opts.VectorIndexMode != VectorIndexExactOnly && opts.VectorIndexMode != VectorIndexHNSWSynchronous {
		return nil, fmt.Errorf("invalid vector index mode %d", opts.VectorIndexMode)
	}
	if opts.WALCheckpointThresholdBytes == 0 {
		opts.WALCheckpointThresholdBytes = defaultWALCheckpointThresholdBytes
	}
	if opts.MaxDatabaseSnapshotBytes == 0 {
		opts.MaxDatabaseSnapshotBytes = defaultMaxDatabaseSnapshotBytes
	}
	if opts.RecoveryMaxDecodedBytes == 0 {
		opts.RecoveryMaxDecodedBytes = defaultRecoveryMaxDecodedBytes
	}
	if opts.RecoveryMaxFrames == 0 {
		opts.RecoveryMaxFrames = defaultRecoveryMaxFrames
	}
	if opts.RecoveryMaxWork == 0 {
		opts.RecoveryMaxWork = defaultRecoveryMaxWork
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
	var lock *pathLock
	var flat bool
	var err error
	if opts.DisableLock {
		path, err = canonicalDBPath(path)
		if err == nil {
			flat, err = prepareDBPath(path, opts.Create)
		}
	} else {
		lock, path, flat, err = acquirePathLock(path, opts.Create, opts.ReadOnly)
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = lock.close()
		return nil, err
	}
	files := store.DirectoryDatabaseFiles(path)
	if flat {
		files = store.FlatDatabaseFiles(path)
	}
	if !opts.ReadOnly && !opts.DisableLock {
		if err := store.CleanupDatabaseTempFiles(files, !flat); err != nil {
			_ = lock.close()
			return nil, err
		}
	}
	if opts.DisableLock {
		if err := checkLayoutOwner(files.State, flat, ""); err != nil {
			return nil, err
		}
	}
	var graph *store.GraphState
	var nextNodeID, nextEdgeID, commitID uint64
	if opts.preloaded {
		graph, nextNodeID, nextEdgeID, commitID = opts.preloadedGraph, opts.preloadedNextNodeID, opts.preloadedNextEdgeID, opts.preloadedCommitID
	} else {
		graph, nextNodeID, nextEdgeID, commitID, err = store.LoadGraphStateFilesContextWithRecoveryLimits(ctx, files, opts.MaxDatabaseSnapshotBytes, opts.DerivedIndexBuildMaxWork, opts.DerivedIndexBuildMaxLogicalBytes, store.RecoveryLimits{
			MaxDecodedBytes: opts.RecoveryMaxDecodedBytes,
			MaxFrames:       opts.RecoveryMaxFrames,
			MaxWork:         opts.RecoveryMaxWork,
		})
	}
	if err != nil {
		if errors.Is(err, store.ErrLoadResourceLimit) || errors.Is(err, store.ErrDerivedIndexResourceLimit) {
			err = fmt.Errorf("%w: %v", ErrResourceLimit, err)
		}
		if !errors.Is(err, os.ErrNotExist) {
			_ = lock.close()
			return nil, err
		}
		if !opts.Create {
			_ = lock.close()
			return nil, err
		}
		graph = store.NewGraphState()
		nextNodeID = 1
		nextEdgeID = 1
		commitID = 0
		if opts.EnableVector {
			graph.VectorDimensions = opts.VectorDimensions
			if graph.VectorDimensions == 0 {
				graph.VectorDimensions = 128
			}
		}
		if err := store.EnsureDatabaseID(graph); err != nil {
			_ = lock.close()
			return nil, err
		}
		if err := ensureLayoutOwner(files.State, flat, graph.DatabaseID); err != nil {
			_ = lock.close()
			return nil, err
		}
		if err := store.CheckpointGraphStateFiles(files, graph, nextNodeID, nextEdgeID, commitID); err != nil {
			_ = lock.close()
			return nil, err
		}
	}
	if !opts.ReadOnly {
		if err := ensureLayoutOwner(files.State, flat, graph.DatabaseID); err != nil {
			_ = lock.close()
			return nil, err
		}
	}
	if !opts.ReadOnly && graph.SnapshotBytes > opts.MaxDatabaseSnapshotBytes {
		_ = lock.close()
		return nil, fmt.Errorf("%w: database snapshot requires %d bytes, limit is %d", ErrResourceLimit, graph.SnapshotBytes, opts.MaxDatabaseSnapshotBytes)
	}
	if opts.VectorDimensions != 0 && graph.VectorDimensions != 0 && opts.VectorDimensions != graph.VectorDimensions {
		_ = lock.close()
		return nil, fmt.Errorf("vector dimensions %d do not match stored dimensions %d", opts.VectorDimensions, graph.VectorDimensions)
	}
	if opts.EnableVector && graph.VectorDimensions == 0 {
		graph.VectorDimensions = opts.VectorDimensions
		if graph.VectorDimensions == 0 {
			graph.VectorDimensions = 128
		}
	}
	if opts.EnableVector || graph.VectorDimensions != 0 {
		if err := validateGraphVectorsContext(ctx, graph); err != nil {
			_ = lock.close()
			return nil, err
		}
		refreshVectorLiveCount(graph)
		if opts.VectorIndexMode == VectorIndexHNSWSynchronous {
			if err := rebuildVectorIndexBudget(ctx, graph, opts.VectorIndexBuildMaxWork, opts.VectorIndexBuildMaxLogicalBytes); err != nil {
				_ = lock.close()
				return nil, err
			}
		}
	}
	reservedNodeID, reservedEdgeID, err := store.LoadIDReservationFiles(files, graph.DatabaseID)
	if err != nil {
		_ = lock.close()
		return nil, err
	}
	nextNodeID = max(nextNodeID, reservedNodeID)
	nextEdgeID = max(nextEdgeID, reservedEdgeID)
	if !opts.ReadOnly {
		if !store.WALFilesReadyForAppend(files) {
			if err := store.CheckpointGraphStateAndWALFiles(files, graph, nextNodeID, nextEdgeID, commitID); err != nil {
				_ = lock.close()
				return nil, err
			}
		}
		if _, err := os.Stat(files.WALBase); err == nil {
			_, markerErr := store.WALFilesHaveCheckpointMarker(files)
			if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
				_ = lock.close()
				return nil, markerErr
			}
			// A marker WAL depends on wal.base for its recovered base. Materialize
			// the recovered generation before removing the base; this also safely
			// compacts any other complete active WAL left by an interrupted rotate.
			if err := store.CheckpointGraphStateAndWALFiles(files, graph, nextNodeID, nextEdgeID, commitID); err != nil {
				_ = lock.close()
				return nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = lock.close()
			return nil, err
		}
	}
	var wal *store.WALWriter
	if !opts.ReadOnly {
		wal, err = store.OpenWALWriterFiles(files, opts.Durability == DurabilityFull, opts.walSync, opts.walWrite, opts.walTruncate, opts.walCleanupSync)
		if err != nil {
			_ = lock.close()
			return nil, err
		}
	}

	db := &DB{
		path:                             path,
		files:                            files,
		graph:                            graph,
		nextNodeID:                       nextNodeID,
		nextEdgeID:                       nextEdgeID,
		reservedNodeID:                   reservedNodeID,
		reservedEdgeID:                   reservedEdgeID,
		commitID:                         commitID,
		readOnly:                         opts.ReadOnly,
		enableVector:                     opts.EnableVector || graph.VectorDimensions != 0,
		disableVectorIndex:               opts.VectorIndexMode == VectorIndexExactOnly,
		vectorDimensions:                 graph.VectorDimensions,
		queryCache:                       map[string]*queryPlan{},
		walCheckpointThresholdBytes:      opts.WALCheckpointThresholdBytes,
		changefeedMaxBytes:               opts.ChangefeedMaxBytes,
		maxDatabaseSnapshotBytes:         opts.MaxDatabaseSnapshotBytes,
		vectorIndexBuildMaxWork:          opts.VectorIndexBuildMaxWork,
		vectorIndexBuildMaxLogicalBytes:  opts.VectorIndexBuildMaxLogicalBytes,
		derivedIndexBuildMaxWork:         opts.DerivedIndexBuildMaxWork,
		derivedIndexBuildMaxLogicalBytes: opts.DerivedIndexBuildMaxLogicalBytes,
		fullSync:                         opts.Durability == DurabilityFull,
		walSync:                          opts.walSync,
		walWrite:                         opts.walWrite,
		walTruncate:                      opts.walTruncate,
		walCleanupSync:                   opts.walCleanupSync,
		reserveIDs:                       opts.reserveIDs,
		checkpointPrepare:                opts.checkpointPrepare,
		checkpointPublish:                opts.checkpointPublish,
		checkpointTryLockFailed:          opts.checkpointTryLockFailed,
		checkpointBeforeFinalTryLock:     opts.checkpointBeforeFinalTryLock,
		checkpointComplete:               opts.checkpointComplete,
		checkpoint:                       opts.checkpoint,
		pathLock:                         lock,
		wal:                              wal,
		streamNotify:                     make(chan struct{}),
	}
	db.checkpointAttemptCond.L = &db.checkpointWorkerMu
	if !db.readOnly {
		db.startCheckpointWorker()
	}
	return db, nil
}

func (db *DB) startCheckpointWorker() {
	db.checkpointWake = make(chan struct{}, 1)
	db.checkpointStop = make(chan struct{})
	db.checkpointDone = make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		db.checkpointWorker()
	}()
	<-started
}

func (db *DB) stopCheckpointWorker() {
	if db.checkpointDone == nil {
		return
	}
	db.checkpointStopOnce.Do(func() { close(db.checkpointStop) })
	<-db.checkpointDone
}

func (db *DB) clearCheckpointState() {
	if db.checkpointPrepared != nil {
		_ = db.checkpointPrepared.Cleanup()
		db.checkpointPrepared = nil
	}
	db.checkpointPending = nil
	db.checkpointInFlight.Store(false)
}

func (db *DB) requestBackgroundCheckpoint() {
	if db.checkpointWake == nil {
		return
	}
	db.mu.RLock()
	if db.closed || db.readOnly || db.recoveryRequired || db.wal == nil || (!db.dirty && !db.adjacencyMaintenanceNeeded.Load()) {
		db.mu.RUnlock()
		return
	}
	size, err := db.wal.TailSize()
	threshold := db.walCheckpointThresholdBytes
	pending := db.checkpointInFlight.Load()
	db.mu.RUnlock()
	if !db.adjacencyMaintenanceNeeded.Load() && !pending && (err != nil || size < 0 || uint64(size) < threshold) {
		return
	}
	db.checkpointWorkerMu.Lock()
	select {
	case <-db.checkpointStop:
		db.checkpointWorkerMu.Unlock()
		return
	default:
	}
	if db.checkpointQueued {
		db.checkpointWorkerMu.Unlock()
		return
	}
	db.checkpointQueued = true
	select {
	case db.checkpointWake <- struct{}{}:
	default:
	}
	db.checkpointWorkerMu.Unlock()
}

func (db *DB) checkpointWorker() {
	defer close(db.checkpointDone)
	for {
		select {
		case <-db.checkpointStop:
			return
		case <-db.checkpointWake:
			db.checkpointWorkerMu.Lock()
			db.checkpointQueued = false
			db.checkpointWorkerMu.Unlock()
			select {
			case <-db.checkpointStop:
				return
			default:
			}
			db.runBackgroundAdjacencyMaintenance()
			checkpointRequested := db.checkpointNeeded.Load()
			db.runBackgroundCheckpoint()
			if checkpointRequested && db.checkpointComplete != nil {
				select {
				case db.checkpointComplete <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (db *DB) runBackgroundAdjacencyMaintenance() {
	db.mu.RLock()
	if db.closed || db.readOnly || db.recoveryRequired || !db.adjacencyMaintenanceNeeded.Load() {
		db.mu.RUnlock()
		return
	}
	graph, commitID := db.graph, db.commitID
	db.mu.RUnlock()

	if db.adjacencyCompactorGraph != graph || db.adjacencyCompactorCommit != commitID {
		if db.adjacencyCompactorActive {
			db.mu.Lock()
			db.enqueueAdjacencyCandidateLocked(db.adjacencyCompactorCandidate)
			db.mu.Unlock()
		}
		db.clearAdjacencyCompactor()
	}
	if db.adjacencyCompactor == nil {
		candidate, ok := db.nextAdjacencyCandidate(graph)
		if !ok {
			db.clearAdjacencyCompactor()
			db.mu.Lock()
			if db.graph == graph && db.commitID == commitID {
				db.adjacencyMaintenanceNeeded.Store(false)
			}
			db.mu.Unlock()
			return
		}
		db.adjacencyCompactorGraph = graph
		db.adjacencyCompactorCommit = commitID
		db.adjacencyCompactor = store.NewAdjacencyCompactor(graph, candidate.direction, candidate.nodeID)
		db.adjacencyCompactorCandidate = candidate
		db.adjacencyCompactorActive = true
		if db.adjacencyCompactor == nil {
			db.clearAdjacencyCompactor()
			db.mu.Lock()
			more := len(db.adjacencyMaintenanceQueue) != 0
			if !more && db.graph == graph && db.commitID == commitID {
				db.adjacencyMaintenanceNeeded.Store(false)
			}
			db.mu.Unlock()
			if more {
				db.requestBackgroundCheckpoint()
			}
			return
		}
	}
	done, changed := db.adjacencyCompactor.Step(store.AdjacencyCompactionChunkBudget)
	if !done {
		db.requestBackgroundCheckpoint()
		return
	}
	if !changed {
		db.clearAdjacencyCompactor()
		db.mu.Lock()
		if db.graph == graph && db.commitID == commitID {
			db.adjacencyMaintenanceNeeded.Store(false)
		}
		db.mu.Unlock()
		return
	}
	if db.publishBackgroundAdjacency(db.adjacencyCompactor.Result(), graph, commitID) {
		db.clearAdjacencyCompactor()
		if db.adjacencyMaintenanceNeeded.Load() {
			db.requestBackgroundCheckpoint()
		}
	}
}

func (db *DB) clearAdjacencyCompactor() {
	db.adjacencyCompactor = nil
	db.adjacencyCompactorGraph = nil
	db.adjacencyCompactorCommit = 0
	db.adjacencyCompactorCandidate = adjacencyCandidate{}
	db.adjacencyCompactorActive = false
}

func (db *DB) nextAdjacencyCandidate(graph *store.GraphState) (adjacencyCandidate, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for len(db.adjacencyMaintenanceQueue) != 0 {
		candidate := db.adjacencyMaintenanceQueue[0]
		db.adjacencyMaintenanceQueue = db.adjacencyMaintenanceQueue[1:]
		delete(db.adjacencyMaintenanceQueued, candidate)
		if len(db.adjacencyMaintenanceQueue) == 0 {
			db.adjacencyMaintenanceQueue = nil
			db.adjacencyMaintenanceQueued = nil
		}
		if graph != db.graph {
			db.enqueueAdjacencyCandidateLocked(candidate)
			return adjacencyCandidate{}, false
		}
		return candidate, true
	}
	return adjacencyCandidate{}, false
}

func (db *DB) enqueueAdjacencyCandidatesLocked(graph, base *store.GraphState, delta store.GraphDelta) bool {
	if db.adjacencyMaintenanceQueued == nil {
		db.adjacencyMaintenanceQueued = make(map[adjacencyCandidate]struct{})
	}
	added := false
	add := func(direction uint8, nodeID uint64) {
		if nodeID == 0 {
			return
		}
		if !store.AdjacencyNeedsCompaction(graph, direction, nodeID) {
			return
		}
		db.enqueueAdjacencyCandidateLocked(adjacencyCandidate{direction: direction, nodeID: nodeID})
		added = true
	}
	for _, edgeID := range delta.DeleteEdges {
		if base == nil {
			continue
		}
		edge := base.Edges.Get(edgeID)
		if edge != nil {
			add(0, edge.SourceID)
			add(1, edge.TargetID)
		}
	}
	return added
}

func (db *DB) enqueueAdjacencyCandidateLocked(candidate adjacencyCandidate) {
	if candidate.nodeID == 0 {
		return
	}
	if db.adjacencyMaintenanceQueued == nil {
		db.adjacencyMaintenanceQueued = make(map[adjacencyCandidate]struct{})
	}
	if _, ok := db.adjacencyMaintenanceQueued[candidate]; ok {
		return
	}
	db.adjacencyMaintenanceQueued[candidate] = struct{}{}
	db.adjacencyMaintenanceQueue = append(db.adjacencyMaintenanceQueue, candidate)
}

func (db *DB) publishBackgroundAdjacency(compacted, graph *store.GraphState, commitID uint64) bool {
	if !db.writeMu.TryLock() {
		return false
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	defer db.writeMu.Unlock()
	if db.closed || db.readOnly || db.recoveryRequired || db.graph != graph || db.commitID != commitID {
		return false
	}
	db.graph = compacted
	db.adjacencyMaintenanceNeeded.Store(len(db.adjacencyMaintenanceQueue) != 0)
	return true
}

func (db *DB) announceCheckpointAttempt() {
	db.checkpointWorkerMu.Lock()
	db.checkpointAttemptEpoch++
	db.checkpointAttemptActive = true
	db.checkpointWorkerMu.Unlock()
}

func (db *DB) finishCheckpointAttempt() {
	db.checkpointWorkerMu.Lock()
	db.checkpointAttemptActive = false
	db.checkpointAttemptCond.Broadcast()
	db.checkpointWorkerMu.Unlock()
}

func (db *DB) waitForCheckpointAttempt() {
	db.checkpointWorkerMu.Lock()
	for db.checkpointAttemptActive {
		db.checkpointAttemptCond.Wait()
	}
	db.checkpointWorkerMu.Unlock()
}

func (db *DB) checkpointAttemptEpochState() uint64 {
	db.checkpointWorkerMu.Lock()
	epoch := db.checkpointAttemptEpoch
	db.checkpointWorkerMu.Unlock()
	return epoch
}

func (db *DB) checkpointAttemptEpochChanged(epoch uint64) bool {
	db.checkpointWorkerMu.Lock()
	changed := db.checkpointAttemptEpoch != epoch
	db.checkpointWorkerMu.Unlock()
	return changed
}

func (db *DB) runBackgroundCheckpoint() {
	if db.checkpointPending == nil {
		generation, ok := db.rotateBackgroundCheckpoint()
		if !ok {
			return
		}
		db.checkpointPending = &generation
		db.checkpointInFlight.Store(true)
	}
	generation := *db.checkpointPending
	if db.checkpointPrepared == nil {
		if db.checkpointPrepare != nil {
			if err := db.checkpointPrepare(db.path, generation.graph, generation.nextNodeID, generation.nextEdgeID, generation.commitID); err != nil {
				return
			}
		}
		prepared, err := store.PrepareCheckpointStateFiles(db.files, generation.graph, generation.nextNodeID, generation.nextEdgeID, generation.commitID)
		if err != nil {
			return
		}
		db.checkpointPrepared = prepared
	}

	db.announceCheckpointAttempt()
	if !db.writeMu.TryLock() {
		if db.checkpointTryLockFailed != nil {
			db.checkpointTryLockFailed()
		}
		db.finishCheckpointAttempt()
		db.requestBackgroundCheckpoint()
		return
	}
	defer func() {
		db.writeMu.Unlock()
		db.finishCheckpointAttempt()
	}()
	if db.checkpointPublish != nil {
		db.checkpointPublish()
	}
	if err := db.publishBackgroundCheckpoint(db.checkpointPrepared, generation); err != nil {
		return
	}
	_ = db.checkpointPrepared.Cleanup()
	db.checkpointPrepared = nil
	db.checkpointPending = nil
	db.checkpointInFlight.Store(false)
	if db.checkpointNeeded.Load() {
		db.requestBackgroundCheckpoint()
	}
}

// rotateBackgroundCheckpoint establishes a durable WAL boundary while the
// writer lock is held. Snapshot serialization starts only after this lock is
// released, so commits cannot become stale candidates.
func (db *DB) rotateBackgroundCheckpoint() (checkpointGeneration, bool) {
	db.announceCheckpointAttempt()
	if !db.writeMu.TryLock() {
		if db.checkpointTryLockFailed != nil {
			db.checkpointTryLockFailed()
		}
		db.finishCheckpointAttempt()
		return checkpointGeneration{}, false
	}
	defer func() {
		db.writeMu.Unlock()
		db.finishCheckpointAttempt()
	}()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || db.readOnly || db.recoveryRequired || db.wal == nil || !db.dirty {
		return checkpointGeneration{}, false
	}
	size, err := db.wal.TailSize()
	if err != nil || size < 0 || uint64(size) < db.walCheckpointThresholdBytes {
		return checkpointGeneration{}, false
	}
	generation := checkpointGeneration{graph: db.graph, nextNodeID: db.nextNodeID, nextEdgeID: db.nextEdgeID, commitID: db.commitID}
	if err := db.wal.Close(); err != nil {
		db.recoveryRequired = true
		return checkpointGeneration{}, false
	}
	if err := store.RotateWALFiles(db.files, generation.graph.DatabaseID, generation.commitID); err != nil {
		db.recoveryRequired = true
		return checkpointGeneration{}, false
	}
	wal, err := store.OpenWALWriterFiles(db.files, db.fullSync, db.walSync, db.walWrite, db.walTruncate, db.walCleanupSync)
	if err != nil {
		db.recoveryRequired = true
		return checkpointGeneration{}, false
	}
	db.wal = wal
	return generation, true
}

func (db *DB) publishBackgroundCheckpoint(prepared *store.PreparedCheckpoint, generation checkpointGeneration) error {
	if err := prepared.PublishCheckpointStateFiles(db.files); err != nil {
		db.mu.Lock()
		db.recoveryRequired = true
		db.mu.Unlock()
		return err
	}
	if err := store.RemoveWALBaseFiles(db.files); err != nil {
		db.mu.Lock()
		db.recoveryRequired = true
		db.mu.Unlock()
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || db.readOnly || db.recoveryRequired {
		return ErrDatabaseClosed
	}
	db.dirty = db.graph != generation.graph || db.commitID != generation.commitID
	db.checkpointNeeded.Store(false)
	if db.dirty {
		if size, err := db.wal.TailSize(); err == nil && size >= 0 && uint64(size) >= db.walCheckpointThresholdBytes {
			db.checkpointNeeded.Store(true)
		}
	}
	db.checkpointCount++
	return nil
}

func (db *DB) Close() error {
	if !db.writeMu.TryLock() {
		if db.activeTx.Load() != 0 {
			return ErrWriteTxActive
		}
		// A checkpoint worker may own writeMu only for its short publication
		// phase. Wait for that phase to finish, but preserve the existing
		// non-blocking behavior when a writer transaction arrives.
		for !db.writeMu.TryLock() {
			if db.activeTx.Load() != 0 {
				return ErrWriteTxActive
			}
			time.Sleep(time.Millisecond)
		}
	}
	defer db.writeMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	if db.activeTx.Load() != 0 {
		db.mu.Unlock()
		return ErrTransactionsActive
	}
	if db.activeSnapshot {
		db.mu.Unlock()
		return ErrSnapshotActive
	}
	if db.vectorRebuild != nil {
		db.vectorRebuild.cancel()
	}
	db.closed = true
	db.notifyStreamsLocked()
	db.mu.Unlock()

	db.stopCheckpointWorker()
	db.clearCheckpointState()
	db.clearAdjacencyCompactor()
	db.mu.Lock()

	var closeErr error
	var reserveDatabaseID string
	var reserveNodeID, reserveEdgeID uint64
	reserve := false
	if !db.readOnly {
		if err := db.wal.Close(); err != nil {
			closeErr = err
		} else if db.recoveryRequired || !db.dirty {
			// The WAL may or may not contain the last frame. Recovery must decide; compaction could destroy that evidence.
		} else if err := db.writeCheckpoint(db.graph, db.nextNodeID, db.nextEdgeID, db.commitID); err != nil {
			closeErr = err
		} else {
			reserveDatabaseID = db.graph.DatabaseID
			reserveNodeID, reserveEdgeID = db.nextNodeID, db.nextEdgeID
			reserve = true
		}
	}
	db.mu.Unlock()
	if reserve {
		if err := db.reserveIDsToDisk(reserveDatabaseID, reserveNodeID, reserveEdgeID); err != nil {
			closeErr = err
		}
	}
	closeErr = errors.Join(closeErr, db.pathLock.close())
	if db.temporary {
		closeErr = errors.Join(closeErr, os.RemoveAll(db.path))
	}
	return closeErr
}

func (db *DB) IsOpen() bool {
	if db == nil {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return !db.closed
}

// Serialize returns a standalone database file. Writing the bytes to a regular
// file produces a path that Open can read and update.
func (db *DB) Serialize() ([]byte, error) {
	if db == nil {
		return nil, ErrDatabaseClosed
	}
	if !db.writeMu.TryLock() {
		return nil, ErrWriteTxActive
	}
	defer func() {
		db.writeMu.Unlock()
		db.requestBackgroundCheckpoint()
	}()
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if db.recoveryRequired {
		return nil, ErrRecoveryRequired
	}
	if db.activeTx.Load() != 0 {
		return nil, ErrTransactionsActive
	}
	return store.SerializeGraphState(db.graph, db.nextNodeID, db.nextEdgeID, db.commitID)
}

// Deserialize opens a database from bytes returned by Serialize.
func Deserialize(data []byte, opts OpenOptions) (*DB, error) {
	maxRecoveryBytes := opts.RecoveryMaxDecodedBytes
	if maxRecoveryBytes == 0 {
		maxRecoveryBytes = defaultRecoveryMaxDecodedBytes
	}
	maxRecoveryWork := opts.RecoveryMaxWork
	if maxRecoveryWork == 0 {
		maxRecoveryWork = defaultRecoveryMaxWork
	}
	maxCanonicalBytes := opts.MaxDatabaseSnapshotBytes
	if maxCanonicalBytes == 0 {
		maxCanonicalBytes = defaultMaxDatabaseSnapshotBytes
	}
	maxDerivedWork := opts.DerivedIndexBuildMaxWork
	if maxDerivedWork == 0 {
		maxDerivedWork = defaultDerivedBuildMaxWork
	}
	maxDerivedBytes := opts.DerivedIndexBuildMaxLogicalBytes
	if maxDerivedBytes == 0 {
		maxDerivedBytes = defaultDerivedBuildMaxLogicalBytes
	}
	graph, nextNodeID, nextEdgeID, commitID, err := store.DeserializeGraphStateWithRecoveryLimits(data, maxCanonicalBytes, maxDerivedWork, maxDerivedBytes, store.RecoveryLimits{
		MaxDecodedBytes: maxRecoveryBytes,
		MaxWork:         maxRecoveryWork,
	})
	if err != nil {
		if errors.Is(err, store.ErrLoadResourceLimit) || errors.Is(err, store.ErrDerivedIndexResourceLimit) {
			err = fmt.Errorf("%w: %v", ErrResourceLimit, err)
		}
		return nil, err
	}
	path, err := os.MkdirTemp("", "latticedb-")
	if err != nil {
		return nil, err
	}
	if err := store.CheckpointGraphStateAndWAL(path, graph, nextNodeID, nextEdgeID, commitID); err != nil {
		_ = os.RemoveAll(path)
		return nil, err
	}
	if err := store.ReserveIDs(path, graph.DatabaseID, nextNodeID, nextEdgeID); err != nil {
		_ = os.RemoveAll(path)
		return nil, err
	}
	// The generated checkpoint/WAL are an implementation detail; the caller's
	// recovery budgets have already been enforced on the standalone input.
	opts.RecoveryMaxDecodedBytes = 0
	opts.RecoveryMaxFrames = 0
	opts.RecoveryMaxWork = 0
	opts.preloaded = true
	opts.preloadedGraph = graph
	opts.preloadedNextNodeID = nextNodeID
	opts.preloadedNextEdgeID = nextEdgeID
	opts.preloadedCommitID = commitID
	db, err := Open(path, opts)
	if err != nil {
		_ = os.RemoveAll(path)
		return nil, err
	}
	db.temporary = true
	return db, nil
}

func (db *DB) writeCheckpoint(graph *store.GraphState, nextNodeID, nextEdgeID, commitID uint64) error {
	if db.checkpoint == nil {
		return store.CheckpointGraphStateAndWALFiles(db.files, graph, nextNodeID, nextEdgeID, commitID)
	}
	if err := db.checkpoint(db.path, graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return err
	}
	return store.RewriteWALSnapshotFiles(db.files, graph, nextNodeID, nextEdgeID, commitID)
}

func (db *DB) Checkpoint() error {
	if !db.writeMu.TryLock() {
		return ErrWriteTxActive
	}
	defer func() {
		db.writeMu.Unlock()
		db.requestBackgroundCheckpoint()
	}()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDatabaseClosed
	}
	if db.readOnly {
		db.mu.RUnlock()
		return ErrReadOnly
	}
	if db.recoveryRequired {
		db.mu.RUnlock()
		return ErrRecoveryRequired
	}
	if db.activeTx.Load() != 0 {
		db.mu.RUnlock()
		return ErrTransactionsActive
	}
	db.mu.RUnlock()
	return db.checkpointWithWriterHeld(nil)
}

func (db *DB) checkpointWithWriterHeld(ctx context.Context) error {
	db.mu.RLock()
	graph, nextNodeID, nextEdgeID, commitID, wal := db.graph, db.nextNodeID, db.nextEdgeID, db.commitID, db.wal
	db.mu.RUnlock()
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if db.checkpoint != nil {
		if err := db.checkpoint(db.path, graph, nextNodeID, nextEdgeID, commitID); err != nil {
			return err
		}
	}
	if err := wal.Close(); err != nil {
		db.mu.Lock()
		db.recoveryRequired = true
		db.mu.Unlock()
		return err
	}
	var checkpointErr error
	if db.checkpoint == nil {
		checkpointErr = store.CheckpointGraphStateAndCompactWALFiles(db.files, graph, nextNodeID, nextEdgeID, commitID, db.walCheckpointThresholdBytes)
	} else {
		checkpointErr = store.RewriteWALSnapshotFiles(db.files, graph, nextNodeID, nextEdgeID, commitID)
	}
	if checkpointErr != nil {
		return db.reopenWALAfterCheckpointError(checkpointErr)
	}
	wal, err := store.OpenWALWriterFiles(db.files, db.fullSync, db.walSync, db.walWrite, db.walTruncate, db.walCleanupSync)
	if err != nil {
		db.mu.Lock()
		db.recoveryRequired = true
		db.mu.Unlock()
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.commitID != commitID || db.graph != graph {
		_ = wal.Close()
		db.recoveryRequired = true
		return ErrWriteConflict
	}
	db.wal = wal
	db.dirty = false
	db.checkpointNeeded.Store(false)
	db.checkpointCount++
	return nil
}

func (db *DB) reopenWALAfterCheckpointError(checkpointErr error) error {
	wal, err := store.OpenWALWriterFiles(db.files, db.fullSync, db.walSync, db.walWrite, db.walTruncate, db.walCleanupSync)
	db.mu.Lock()
	defer db.mu.Unlock()
	if err != nil {
		db.recoveryRequired = true
		return errors.Join(checkpointErr, err)
	}
	db.wal = wal
	return checkpointErr
}

func (db *DB) SnapshotGraph() (*store.GraphState, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if db.recoveryRequired {
		return nil, ErrRecoveryRequired
	}
	return db.graph, nil
}

func (db *DB) Begin(readOnly bool) (*Tx, error) {
	if !readOnly && !db.writeMu.TryLock() {
		attemptEpoch := db.checkpointAttemptEpochState()
		if db.checkpointTryLockFailed != nil {
			db.checkpointTryLockFailed()
		}
		db.waitForCheckpointAttempt()
		if db.checkpointBeforeFinalTryLock != nil {
			db.checkpointBeforeFinalTryLock()
		}
		if !db.writeMu.TryLock() {
			if db.checkpointAttemptEpochChanged(attemptEpoch) {
				db.waitForCheckpointAttempt()
				if !db.writeMu.TryLock() {
					return nil, ErrWriteTxActive
				}
			} else {
				return nil, ErrWriteTxActive
			}
		}
	}
	return db.beginAfterWriteLock(readOnly)
}

// BeginWriteContext waits for the single writer slot until ctx is canceled.
// Begin(false) remains the nonblocking compatibility API.
func (db *DB) BeginWriteContext(ctx context.Context) (*Tx, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for !db.writeMu.TryLock() {
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return db.beginAfterWriteLock(false)
}

func (db *DB) beginAfterWriteLock(readOnly bool) (*Tx, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		if !readOnly {
			db.writeMu.Unlock()
		}
		return nil, ErrDatabaseClosed
	}
	if db.recoveryRequired {
		if !readOnly {
			db.writeMu.Unlock()
		}
		return nil, ErrRecoveryRequired
	}
	if !readOnly && db.readOnly {
		db.writeMu.Unlock()
		return nil, ErrReadOnly
	}

	graph := db.graph
	var base *store.GraphState
	var changes *txChanges
	if !readOnly {
		base = graph
		graph = store.CloneGraphStateShallow(graph)
		changes = newTxChanges(db.commitID)
	}
	db.activeTx.Add(1)

	return &Tx{
		db:          db,
		readOnly:    readOnly,
		base:        base,
		graph:       graph,
		writeLocked: !readOnly,
		changes:     changes,
	}, nil
}

func (db *DB) View(fn func(*Tx) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	tx.managed = true
	defer tx.rollbackInternal()
	return fn(tx)
}

func (db *DB) Update(fn func(*Tx) error) error {
	return db.updateContext(nil, fn)
}

func (db *DB) UpdateContext(ctx context.Context, fn func(*Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return db.updateContext(ctx, fn)
}

func (db *DB) updateContext(ctx context.Context, fn func(*Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	tx.managed = true
	defer tx.rollbackInternal()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.commitInternalContext(ctx)
}

func (db *DB) Query(query string, params map[string]any) (QueryResult, error) {
	return db.queryContext(nil, query, params, QueryOptions{})
}

func (db *DB) QueryContext(ctx context.Context, query string, params map[string]any, opts QueryOptions) (QueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return db.queryContext(ctx, query, params, opts)
}

func (db *DB) queryContext(ctx context.Context, query string, params map[string]any, opts QueryOptions) (QueryResult, error) {
	if len(query) > maxQueryBytes {
		return QueryResult{}, fmt.Errorf("%w: query exceeds %d bytes", ErrResourceLimit, maxQueryBytes)
	}
	plan, err := db.cachedQueryPlan(query)
	if err != nil {
		return QueryResult{}, &QueryError{Stage: QueryErrorStageParse, Err: err}
	}

	budget := newQueryBudget(ctx, opts)
	defer releaseQueryBudget(budget)
	if plan.mutates() {
		var result QueryResult
		err := db.updateContext(ctx, func(tx *Tx) error {
			var execErr error
			result, execErr = plan.execute(tx, params, budget)
			return execErr
		})
		if err != nil {
			err = &QueryError{Stage: QueryErrorStageExecution, Err: err}
		}
		return result, err
	}

	var result QueryResult
	err = db.View(func(tx *Tx) error {
		var execErr error
		result, execErr = plan.execute(tx, params, budget)
		return execErr
	})
	if err != nil {
		err = &QueryError{Stage: QueryErrorStageExecution, Err: err}
	}
	return result, err
}

func (db *DB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	return db.VectorSearchContext(context.Background(), vector, opts)
}

func (db *DB) VectorSearchContext(ctx context.Context, vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	if !db.enableVector {
		return nil, fmt.Errorf("%w: vector search is disabled", ErrUnsupportedOption)
	}
	if db.vectorDimensions == 0 {
		return nil, errors.New("vector search requires configured dimensions")
	}
	limit := uint64(opts.K)
	if limit == 0 {
		limit = 10
	}
	if opts.MaxWork == 0 {
		opts.MaxWork = ^uint64(0)
	}
	budget, err := newDirectSearchBudget(ctx, opts.MaxWork, opts.MaxBytes, uint32(limit))
	if err != nil {
		return nil, err
	}
	if db.vectorDimensions > 0 && len(vector) != int(db.vectorDimensions) {
		return nil, fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), db.vectorDimensions)
	}
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("vector contains non-finite value")
		}
	}
	queryVector := vector
	var results []VectorSearchResult

	err = db.View(func(tx *Tx) error {
		capacity := limit
		if !opts.Exact && !db.disableVectorIndex && tx.graph.VectorIndex.Nodes.Len() > 0 {
			capacity = min(limit, tx.graph.VectorLiveCount)
		} else if nodeCount := uint64(tx.graph.Nodes.Len()); capacity > nodeCount {
			capacity = nodeCount
		}
		results = make([]VectorSearchResult, 0, int(capacity))
		if !opts.Exact && !db.disableVectorIndex && tx.graph.VectorIndex.Nodes.Len() > 0 {
			entry := tx.graph.VectorIndex.EntryID
			for level := tx.graph.VectorIndex.MaxLevel; level > 0; level-- {
				var err error
				entry, err = vectorGreedyBudget(tx.graph, queryVector, entry, level, budget)
				if err != nil {
					return err
				}
			}
			ef := int(opts.EfSearch)
			if ef == 0 {
				ef = vectorIndexSearchEF
			}
			ef = max(ef, int(capacity))
			maxVisited := uint64(tx.graph.VectorIndex.Nodes.Len())
			if byWork := budget.maxWork/uint64(max(1, len(queryVector))) + 1; maxVisited > byWork {
				maxVisited = byWork
			}
			// Visited entries are charged as they are discovered so small-Ef searches are
			// not rejected solely because the index is large.
			scratchBytes := saturatingAdd(256, saturatingMul(uint64(ef), 32))
			if err := budget.reserveBytes(scratchBytes); err != nil {
				return err
			}
			budget.annVisitedLimit = (budget.maxBytes - budget.bytes) / 80
			if budget.annVisitedLimit == 0 {
				return fmt.Errorf("%w: search memory exceeds budget", ErrResourceLimit)
			}
			annBytesBefore := budget.bytes
			scratch := acquireVectorSearchScratch(int(maxVisited), int(maxVisited), ef*2)
			candidates, searchErr := vectorSearchLayerBudget(tx.graph, queryVector, entry, 0, ef, 0, scratch, budget)
			if searchErr != nil {
				releaseVectorSearchScratch(scratch)
				return searchErr
			}
			budget.bytes += uint64(len(scratch.visited)) * 80
			for _, candidate := range candidates {
				results = pushVectorResult(results, VectorSearchResult{NodeID: candidate.id, Distance: float32(math.Sqrt(candidate.distance))}, int(capacity))
			}
			releaseVectorSearchScratch(scratch)
			if len(results) == int(capacity) {
				return budget.check()
			}
			budget.releaseBytes(budget.bytes - annBytesBefore + scratchBytes)
			// ponytail: disconnected or degenerate ANN graphs fall back to exact search to honor K.
			db.vectorExactFallbacks.Add(1)
			results = results[:0]
		}
		for _, node := range tx.graph.Nodes.All() {
			vectorValue, ok := search.FirstVectorProperty(node.Properties)
			if !ok {
				if err := budget.add(1); err != nil {
					return err
				}
				continue
			}
			if err := budget.add(uint64(len(queryVector))); err != nil {
				return err
			}
			var distance float32
			var err error
			if len(queryVector) < 256 {
				distance, err = search.VectorDistance(vectorValue, queryVector)
			} else {
				distance, err = search.VectorDistanceContext(budget.ctx, vectorValue, queryVector)
			}
			if err != nil {
				return err
			}
			results = pushVectorResult(results, VectorSearchResult{NodeID: node.ID, Distance: distance}, int(capacity))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(results, compareVectorResult)
	return results, nil
}

func pushVectorResult(heap []VectorSearchResult, value VectorSearchResult, limit int) []VectorSearchResult {
	if limit == 0 {
		return heap
	}
	if len(heap) < limit {
		heap = append(heap, value)
		for child := len(heap) - 1; child > 0; {
			parent := (child - 1) / 2
			if compareVectorResult(heap[child], heap[parent]) <= 0 {
				break
			}
			heap[child], heap[parent] = heap[parent], heap[child]
			child = parent
		}
		return heap
	}
	if compareVectorResult(value, heap[0]) >= 0 {
		return heap
	}
	heap[0] = value
	for parent := 0; ; {
		worst := parent
		left := parent*2 + 1
		right := left + 1
		if left < len(heap) && compareVectorResult(heap[left], heap[worst]) > 0 {
			worst = left
		}
		if right < len(heap) && compareVectorResult(heap[right], heap[worst]) > 0 {
			worst = right
		}
		if worst == parent {
			return heap
		}
		heap[parent], heap[worst] = heap[worst], heap[parent]
		parent = worst
	}
}

func compareVectorResult(a VectorSearchResult, b VectorSearchResult) int {
	if a.Distance < b.Distance {
		return -1
	}
	if a.Distance > b.Distance {
		return 1
	}
	if a.NodeID < b.NodeID {
		return -1
	}
	if a.NodeID > b.NodeID {
		return 1
	}
	return 0
}

func (db *DB) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	return db.FTSSearchContext(context.Background(), query, opts)
}

func (db *DB) FTSSearchContext(ctx context.Context, query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	limit := uint64(opts.Limit)
	if limit == 0 {
		limit = 10
	}
	budget, err := newDirectSearchBudget(ctx, opts.MaxWork, opts.MaxBytes, uint32(limit))
	if err != nil {
		return nil, err
	}
	if err := budget.add(uint64(len(query))); err != nil {
		return nil, err
	}
	var results []FTSSearchResult
	terms, err := search.TokenizeContextWithLimit(ctx, query, budget.maxBytes-budget.bytes)
	if err != nil {
		if errors.Is(err, search.ErrTokenizationLimit) {
			return nil, fmt.Errorf("%w: search query exceeds memory budget", ErrResourceLimit)
		}
		return nil, err
	}
	logicalBytes := saturatingMul(uint64(len(terms)), 24)
	for _, term := range terms {
		logicalBytes = saturatingAdd(logicalBytes, uint64(len(term)))
	}
	if err := budget.reserveBytes(logicalBytes); err != nil {
		return nil, err
	}
	if len(terms) == 0 {
		return []FTSSearchResult{}, nil
	}

	err = db.View(func(tx *Tx) error {
		capacity := limit
		if recordCount := uint64(tx.graph.FTS.Len()); capacity > recordCount {
			capacity = recordCount
		}
		results = make([]FTSSearchResult, 0, int(capacity))
		if opts.MaxDistance == 0 {
			for termIndex, term := range terms {
				if slices.Contains(terms[:termIndex], term) {
					continue
				}
				for nodeID := range tx.graph.FTSTokens.All(term) {
					if err := budget.add(1); err != nil {
						return err
					}
					record := tx.graph.FTS.Get(nodeID)
					if record == nil {
						continue
					}
					score, seen, work := ftsExactScore(record.Tokens, terms, termIndex)
					if err := budget.add(work); err != nil {
						return err
					}
					if seen {
						continue
					}
					results = pushFTSResult(results, FTSSearchResult{NodeID: nodeID, Score: score}, int(capacity))
				}
			}
			return nil
		}
		for token := range tx.graph.FTSTokens.Keys() {
			if err := budget.add(1); err != nil {
				return err
			}
			matches, err := recordMatchesAnyTokenBudget([]string{token}, terms, opts.MaxDistance, opts.MinTermLength, budget)
			if err != nil {
				return err
			}
			if !matches {
				continue
			}
			for nodeID := range tx.graph.FTSTokens.All(token) {
				if err := budget.add(1); err != nil {
					return err
				}
				record := tx.graph.FTS.Get(nodeID)
				if record == nil {
					continue
				}
				first, err := firstMatchingTokenBudget(record.Tokens, terms, opts.MaxDistance, opts.MinTermLength, budget)
				if err != nil {
					return err
				}
				if first != token {
					continue
				}
				score, err := ftsScoreBudget(record.Tokens, terms, opts.MaxDistance, opts.MinTermLength, budget)
				if err != nil {
					return err
				}
				results = pushFTSResult(results, FTSSearchResult{NodeID: nodeID, Score: score}, int(capacity))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(results, compareFTSResult)
	return results, nil
}

type directSearchBudget struct {
	ctx             context.Context
	maxWork         uint64
	maxBytes        uint64
	work            uint64
	checked         uint64
	bytes           uint64
	annVisitedLimit uint64
}

func newDirectSearchBudget(ctx context.Context, maxWork, maxBytes uint64, results uint32) (*directSearchBudget, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if maxWork == 0 {
		maxWork = defaultSearchMaxWork
	}
	if maxBytes == 0 {
		maxBytes = defaultSearchMaxBytes
	}
	if results > maxSearchResults || uint64(results)*16 > maxBytes {
		return nil, fmt.Errorf("%w: search result limit exceeds budget", ErrResourceLimit)
	}
	budget := &directSearchBudget{ctx: ctx, maxWork: maxWork, maxBytes: maxBytes, bytes: uint64(results) * 16}
	return budget, budget.check()
}

func (budget *directSearchBudget) add(work uint64) error {
	if budget.work > budget.maxWork || work > budget.maxWork-budget.work {
		return fmt.Errorf("%w: search work exceeds budget", ErrResourceLimit)
	}
	budget.work += work
	if budget.work-budget.checked >= 4096 {
		budget.checked = budget.work
		return budget.check()
	}
	return nil
}

func (budget *directSearchBudget) check() error { return budget.ctx.Err() }

func (budget *directSearchBudget) reserveBytes(bytes uint64) error {
	if budget.bytes > budget.maxBytes || bytes > budget.maxBytes-budget.bytes {
		return fmt.Errorf("%w: search memory exceeds budget", ErrResourceLimit)
	}
	budget.bytes += bytes
	return nil
}

func (budget *directSearchBudget) releaseBytes(bytes uint64) {
	budget.bytes -= min(bytes, budget.bytes)
}

func (budget *directSearchBudget) checkTempBytes(bytes uint64) error {
	if budget.bytes > budget.maxBytes || bytes > budget.maxBytes-budget.bytes {
		return fmt.Errorf("%w: search memory exceeds budget", ErrResourceLimit)
	}
	return nil
}

func fuzzyTokenMatchBudget(term, token string, maxDistance, minTermLength uint32, budget *directSearchBudget) (bool, error) {
	term = strings.ToLower(term)
	if token == term {
		return true, budget.add(uint64(max(1, min(len(term), len(token)))))
	}
	if maxDistance == 0 || uint32(utf8.RuneCountInString(term)) < minTermLength {
		return false, budget.add(uint64(max(1, min(len(term), len(token)))))
	}
	distance, err := levenshteinDistanceBudget(term, token, maxDistance, budget)
	if err != nil {
		return false, err
	}
	return uint64(distance) <= uint64(maxDistance), nil
}

func levenshteinDistanceBudget(left, right string, maxDistance uint32, budget *directSearchBudget) (int, error) {
	leftCount, rightCount := uint64(utf8.RuneCountInString(left)), uint64(utf8.RuneCountInString(right))
	shortCount := min(leftCount, rightCount)
	tempBytes := saturatingAdd(saturatingMul(saturatingAdd(leftCount, rightCount), 4), saturatingMul(saturatingAdd(shortCount, 1), 16))
	if err := budget.checkTempBytes(tempBytes); err != nil {
		return 0, err
	}
	lengthGap := max(leftCount, rightCount) - shortCount
	if lengthGap > uint64(maxDistance) {
		if err := budget.check(); err != nil {
			return 0, err
		}
		if err := budget.add(saturatingAdd(leftCount, rightCount)); err != nil {
			return 0, err
		}
		return int(lengthGap), nil
	}
	// ponytail: equal-length vocabulary still scans keys with budgeted full DP; add an index only with workload evidence.
	leftRunes, rightRunes := []rune(left), []rune(right)
	if len(rightRunes) > len(leftRunes) {
		leftRunes, rightRunes = rightRunes, leftRunes
	}
	if len(rightRunes) == 0 {
		return len(leftRunes), budget.add(uint64(max(1, len(leftRunes))))
	}
	prev := make([]int, len(rightRunes)+1)
	curr := make([]int, len(rightRunes)+1)
	for j := range prev {
		prev[j] = j
	}
	for i, leftRune := range leftRunes {
		if err := budget.add(uint64(len(rightRunes))); err != nil {
			return 0, err
		}
		curr[0] = i + 1
		rowMin := curr[0]
		for j, rightRune := range rightRunes {
			cost := 0
			if leftRune != rightRune {
				cost = 1
			}
			curr[j+1] = min(prev[j+1]+1, curr[j]+1, prev[j]+cost)
			rowMin = min(rowMin, curr[j+1])
		}
		if uint64(rowMin) > uint64(maxDistance) {
			return rowMin, nil
		}
		prev, curr = curr, prev
	}
	return prev[len(rightRunes)], nil
}

func ftsScoreBudget(tokens, terms []string, maxDistance, minTermLength uint32, budget *directSearchBudget) (float32, error) {
	if err := budget.checkTempBytes(saturatingMul(uint64(len(tokens)), 48)); err != nil {
		return 0, err
	}
	freq := make(map[string]int, len(tokens))
	for _, token := range tokens {
		freq[token]++
	}
	var score float32
	for _, term := range terms {
		best := 0
		for token, count := range freq {
			match, err := fuzzyTokenMatchBudget(term, token, maxDistance, minTermLength, budget)
			if err != nil {
				return 0, err
			}
			if match && count > best {
				best = count
			}
		}
		score += float32(best)
	}
	return score, nil
}

func saturatingAdd(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}

func saturatingMul(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}

func recordMatchesAnyToken(tokens, terms []string, maxDistance, minTermLength uint32) bool {
	for _, token := range tokens {
		for _, term := range terms {
			if search.FuzzyTokenMatch(term, token, maxDistance, minTermLength) {
				return true
			}
		}
	}
	return false
}

func recordMatchesAnyTokenBudget(tokens, terms []string, maxDistance, minTermLength uint32, budget *directSearchBudget) (bool, error) {
	for _, token := range tokens {
		for _, term := range terms {
			match, err := fuzzyTokenMatchBudget(term, token, maxDistance, minTermLength, budget)
			if err != nil || match {
				return match, err
			}
		}
	}
	return false, nil
}

func firstMatchingToken(tokens, terms []string, maxDistance, minTermLength uint32) string {
	first := ""
	for _, token := range tokens {
		if recordMatchesAnyToken([]string{token}, terms, maxDistance, minTermLength) && (first == "" || token < first) {
			first = token
		}
	}
	return first
}

func firstMatchingTokenBudget(tokens, terms []string, maxDistance, minTermLength uint32, budget *directSearchBudget) (string, error) {
	first := ""
	for _, token := range tokens {
		match, err := recordMatchesAnyTokenBudget([]string{token}, terms, maxDistance, minTermLength, budget)
		if err != nil {
			return "", err
		}
		if match && (first == "" || token < first) {
			first = token
		}
	}
	return first, nil
}

func ftsScoreWork(tokens, terms []string, maxDistance uint32) uint64 {
	var work uint64
	for _, token := range tokens {
		for _, term := range terms {
			var delta uint64
			if maxDistance == 0 {
				delta = uint64(max(1, min(len(token), len(term))))
			} else {
				left, right := uint64(len(token)), uint64(len(term))
				if left != 0 && right > ^uint64(0)/left {
					return ^uint64(0)
				}
				delta = max(uint64(1), left*right)
			}
			if delta > ^uint64(0)-work {
				return ^uint64(0)
			}
			work += delta
		}
	}
	return work
}

func ftsExactScore(tokens, terms []string, priorTerms int) (float32, bool, uint64) {
	var score int
	var seen bool
	var work uint64
	for _, token := range tokens {
		for termIndex, term := range terms {
			work = saturatingAdd(work, uint64(max(1, min(len(token), len(term)))))
			if token == term {
				score++
				seen = seen || termIndex < priorTerms
			}
		}
	}
	return float32(score), seen, work
}

func compareFTSResult(a FTSSearchResult, b FTSSearchResult) int {
	if a.Score > b.Score {
		return -1
	}
	if a.Score < b.Score {
		return 1
	}
	if a.NodeID < b.NodeID {
		return -1
	}
	if a.NodeID > b.NodeID {
		return 1
	}
	return 0
}

func pushFTSResult(heap []FTSSearchResult, value FTSSearchResult, limit int) []FTSSearchResult {
	if limit == 0 {
		return heap
	}
	if len(heap) < limit {
		heap = append(heap, value)
		for child := len(heap) - 1; child > 0; {
			parent := (child - 1) / 2
			if compareFTSResult(heap[child], heap[parent]) <= 0 {
				break
			}
			heap[child], heap[parent] = heap[parent], heap[child]
			child = parent
		}
		return heap
	}
	if compareFTSResult(value, heap[0]) >= 0 {
		return heap
	}
	heap[0] = value
	for parent := 0; ; {
		worst := parent
		left := parent*2 + 1
		right := left + 1
		if left < len(heap) && compareFTSResult(heap[left], heap[worst]) > 0 {
			worst = left
		}
		if right < len(heap) && compareFTSResult(heap[right], heap[worst]) > 0 {
			worst = right
		}
		if worst == parent {
			return heap
		}
		heap[parent], heap[worst] = heap[worst], heap[parent]
		parent = worst
	}
}

func (db *DB) CacheClear() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()

	db.queryCache = map[string]*queryPlan{}
	db.queryCacheKeys = [queryCacheEntries]string{}
	db.queryCacheNext = 0
	db.cacheHits.Store(0)
	db.cacheMisses.Store(0)
	return nil
}

func (db *DB) CacheStats() (QueryCacheStats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return QueryCacheStats{}, ErrDatabaseClosed
	}
	db.cacheMu.RLock()
	defer db.cacheMu.RUnlock()

	return QueryCacheStats{
		Entries: uint32(len(db.queryCache)),
		Hits:    db.cacheHits.Load(),
		Misses:  db.cacheMisses.Load(),
	}, nil
}

func (db *DB) CreateNodePropertyIndex(label, property string) error {
	return db.CreateNodePropertyIndexContext(context.Background(), label, property)
}

func (db *DB) CreateNodePropertyIndexContext(ctx context.Context, label, property string) error {
	return db.createPropertyIndexContext(ctx, true, label, property)
}

func (db *DB) DropNodePropertyIndex(label, property string) error {
	return db.updatePropertyIndex(true, false, label, property)
}

func (db *DB) CreateEdgePropertyIndex(edgeType, property string) error {
	return db.CreateEdgePropertyIndexContext(context.Background(), edgeType, property)
}

func (db *DB) CreateEdgePropertyIndexContext(ctx context.Context, edgeType, property string) error {
	return db.createPropertyIndexContext(ctx, false, edgeType, property)
}

func (db *DB) DropEdgePropertyIndex(edgeType, property string) error {
	return db.updatePropertyIndex(false, false, edgeType, property)
}

var errPropertyIndexGenerationChanged = errors.New("property index build generation changed")

func (db *DB) createPropertyIndexContext(ctx context.Context, node bool, scope, property string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if scope == "" || property == "" {
		return fmt.Errorf("%w: property index scope and property must be non-empty", ErrInvalidArgument)
	}
	if node {
		if err := store.ValidateCreateLabels([]string{scope}); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
	} else if err := store.ValidateEdgeType(scope); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if err := store.ValidatePropertyKey(property); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	definition := store.PropertyIndexDefinition{Scope: scope, Property: property}
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return ErrDatabaseClosed
		}
		if db.readOnly {
			db.mu.RUnlock()
			return ErrReadOnly
		}
		source, baseCommit := db.graph, db.commitID
		db.mu.RUnlock()
		indexes, work, logicalBytes, err := buildPropertyIndex(ctx, source, node, definition, db)
		if err != nil {
			return err
		}
		err = db.UpdateContext(ctx, func(tx *Tx) error {
			if tx.base != source || tx.changes.baseCommitID != baseCommit {
				return errPropertyIndexGenerationChanged
			}
			target := &tx.graph.EdgeProperties
			if node {
				target = &tx.graph.NodeProperties
			}
			if target.Has(definition) {
				return fmt.Errorf("%w: property index already exists", ErrAlreadyExists)
			}
			*target = indexes
			tx.graph.DerivedIndexWork = saturatingAdd(tx.graph.DerivedIndexWork, work)
			tx.graph.DerivedIndexLogicalBytes = saturatingAdd(tx.graph.DerivedIndexLogicalBytes, logicalBytes)
			if exceedsDerivedBudget(tx.graph, db, 0, 0) {
				return fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
			}
			if node {
				tx.changes.createNodeIndexes[definition] = struct{}{}
			} else {
				tx.changes.createEdgeIndexes[definition] = struct{}{}
			}
			return nil
		})
		if !errors.Is(err, errPropertyIndexGenerationChanged) && !errors.Is(err, ErrWriteTxActive) {
			return err
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrWriteConflict
}

func buildPropertyIndex(ctx context.Context, source *store.GraphState, node bool, definition store.PropertyIndexDefinition, db *DB) (store.PropertyIndexes, uint64, uint64, error) {
	indexes := source.NodeProperties.Fork()
	if !node {
		indexes = source.EdgeProperties.Fork()
	}
	if !indexes.Create(definition) {
		return indexes, 0, 0, fmt.Errorf("%w: property index already exists", ErrAlreadyExists)
	}
	work := uint64(1)
	logicalBytes := saturatingAdd(uint64(len(definition.Scope)+len(definition.Property)), 192)
	if exceedsDerivedBudget(source, db, work, logicalBytes) {
		return indexes, 0, 0, fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
	}
	var ids iter.Seq[uint64]
	if node {
		ids = source.Labels.All(definition.Scope)
	} else {
		ids = source.EdgeTypes.All(definition.Scope)
	}
	for id := range ids {
		if err := ctx.Err(); err != nil {
			return indexes, 0, 0, err
		}
		work = saturatingAdd(work, 1)
		if exceedsDerivedBudget(source, db, work, logicalBytes) {
			return indexes, 0, 0, fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
		}
		var properties map[string]any
		if node {
			properties = source.Nodes.Get(id).Properties
		} else {
			properties = source.Edges.Get(id).Properties
		}
		value, ok := properties[definition.Property]
		if !ok {
			continue
		}
		valueBytes := store.EstimatePropertyIndexValueBytes(value)
		work = saturatingAdd(work, max(uint64(1), valueBytes))
		logicalBytes = saturatingAdd(logicalBytes, saturatingAdd(valueBytes, 192))
		if exceedsDerivedBudget(source, db, work, logicalBytes) {
			return indexes, 0, 0, fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
		}
		if err := indexes.Add(definition, value, id); err != nil {
			return indexes, 0, 0, err
		}
	}
	return indexes, work, logicalBytes, nil
}

func (db *DB) updatePropertyIndex(node, create bool, scope, property string) error {
	if scope == "" || property == "" {
		return fmt.Errorf("%w: property index scope and property must be non-empty", ErrInvalidArgument)
	}
	var err error
	if node {
		err = store.ValidateCreateLabels([]string{scope})
	} else {
		err = store.ValidateEdgeType(scope)
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if err := store.ValidatePropertyKey(property); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	definition := store.PropertyIndexDefinition{Scope: scope, Property: property}
	return db.Update(func(tx *Tx) error {
		indexes := &tx.graph.EdgeProperties
		if node {
			indexes = &tx.graph.NodeProperties
		}
		if !create {
			if !indexes.Drop(definition) {
				return fmt.Errorf("%w: property index does not exist", ErrUnsupportedOption)
			}
			if node {
				for id := range tx.base.Labels.All(scope) {
					record := tx.base.Nodes.Get(id)
					value, ok := record.Properties[property]
					adjustPropertyIndexBudget(tx.graph, definition, value, ok, false)
				}
			} else {
				for id := range tx.base.EdgeTypes.All(scope) {
					record := tx.base.Edges.Get(id)
					value, ok := record.Properties[property]
					adjustPropertyIndexBudget(tx.graph, definition, value, ok, false)
				}
			}
			adjustPropertyIndexBudget(tx.graph, definition, nil, false, false)
			if node {
				tx.changes.dropNodeIndexes[definition] = struct{}{}
			} else {
				tx.changes.dropEdgeIndexes[definition] = struct{}{}
			}
			return nil
		}
		if !indexes.Create(definition) {
			return fmt.Errorf("%w: property index already exists", ErrAlreadyExists)
		}
		work := uint64(1)
		logicalBytes := saturatingAdd(uint64(len(scope)+len(property)), 192)
		if exceedsDerivedBudget(tx.graph, db, work, logicalBytes) {
			return fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
		}
		if node {
			for id := range tx.graph.Labels.All(scope) {
				work = saturatingAdd(work, 1)
				if exceedsDerivedBudget(tx.graph, db, work, 0) {
					return fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
				}
				record := tx.graph.Nodes.Get(id)
				value, ok := record.Properties[property]
				if !ok {
					continue
				}
				valueBytes := store.EstimatePropertyIndexValueBytes(value)
				work = saturatingAdd(work, max(uint64(1), valueBytes))
				logicalBytes = saturatingAdd(logicalBytes, saturatingAdd(valueBytes, 192))
				if exceedsDerivedBudget(tx.graph, db, work, logicalBytes) {
					return fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
				}
				if err := indexes.Add(definition, value, id); err != nil {
					return err
				}
			}
		} else {
			for id := range tx.graph.EdgeTypes.All(scope) {
				work = saturatingAdd(work, 1)
				if exceedsDerivedBudget(tx.graph, db, work, 0) {
					return fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
				}
				record := tx.graph.Edges.Get(id)
				value, ok := record.Properties[property]
				if !ok {
					continue
				}
				valueBytes := store.EstimatePropertyIndexValueBytes(value)
				work = saturatingAdd(work, max(uint64(1), valueBytes))
				logicalBytes = saturatingAdd(logicalBytes, saturatingAdd(valueBytes, 192))
				if exceedsDerivedBudget(tx.graph, db, work, logicalBytes) {
					return fmt.Errorf("%w: property index build exceeds derived-index budget", ErrResourceLimit)
				}
				if err := indexes.Add(definition, value, id); err != nil {
					return err
				}
			}
		}
		tx.graph.DerivedIndexWork = saturatingAdd(tx.graph.DerivedIndexWork, work)
		tx.graph.DerivedIndexLogicalBytes = saturatingAdd(tx.graph.DerivedIndexLogicalBytes, logicalBytes)
		if node {
			tx.changes.createNodeIndexes[definition] = struct{}{}
		} else {
			tx.changes.createEdgeIndexes[definition] = struct{}{}
		}
		return nil
	})
}

func exceedsDerivedBudget(graph *store.GraphState, db *DB, work, bytes uint64) bool {
	return work > db.derivedIndexBuildMaxWork || bytes > db.derivedIndexBuildMaxLogicalBytes || graph.DerivedIndexWork > db.derivedIndexBuildMaxWork-work || graph.DerivedIndexLogicalBytes > db.derivedIndexBuildMaxLogicalBytes-bytes
}

func propertyIndexCost(def store.PropertyIndexDefinition, value any, hasValue bool) (uint64, uint64) {
	work, bytes := uint64(1), uint64(0)
	if hasValue {
		valueBytes := store.EstimatePropertyIndexValueBytes(value)
		work = saturatingAdd(work, max(uint64(1), valueBytes))
		bytes = saturatingAdd(valueBytes, 192)
	}
	return work, bytes
}

func adjustPropertyIndexBudget(graph *store.GraphState, def store.PropertyIndexDefinition, value any, present bool, add bool) {
	work, bytes := propertyIndexCost(def, value, present)
	if add {
		graph.DerivedIndexWork = saturatingAdd(graph.DerivedIndexWork, work)
		graph.DerivedIndexLogicalBytes = saturatingAdd(graph.DerivedIndexLogicalBytes, bytes)
	} else {
		graph.DerivedIndexWork -= min(work, graph.DerivedIndexWork)
		graph.DerivedIndexLogicalBytes -= min(bytes, graph.DerivedIndexLogicalBytes)
	}
}

func adjustDerivedCost(graph *store.GraphState, work, bytes uint64, add bool) {
	if add {
		graph.DerivedIndexWork = saturatingAdd(graph.DerivedIndexWork, work)
		graph.DerivedIndexLogicalBytes = saturatingAdd(graph.DerivedIndexLogicalBytes, bytes)
		return
	}
	graph.DerivedIndexWork -= min(work, graph.DerivedIndexWork)
	graph.DerivedIndexLogicalBytes -= min(bytes, graph.DerivedIndexLogicalBytes)
}

func adjustNodeDerivedCost(graph, source *store.GraphState, id uint64, add bool) {
	n := source.Nodes.Get(id)
	if n == nil {
		return
	}
	for _, label := range n.Labels {
		adjustDerivedCost(graph, 1, uint64(len(label))+128, add)
	}
}

func adjustEdgeDerivedCost(graph, source *store.GraphState, id uint64, add bool) {
	e := source.Edges.Get(id)
	if e != nil {
		adjustDerivedCost(graph, 3, uint64(len(e.Type))+160, add)
	}
}

func adjustFTSDerivedCost(graph, source *store.GraphState, id uint64, add bool) {
	f := source.FTS.Get(id)
	if f == nil {
		return
	}
	var bytes uint64
	for _, token := range f.Tokens {
		bytes = saturatingAdd(bytes, uint64(len(token))+128)
	}
	bytes = saturatingAdd(bytes, uint64(len(f.Tokens))*16)
	adjustDerivedCost(graph, saturatingAdd(uint64(len(f.Text))*2, uint64(len(f.Text))+uint64(len(f.Tokens))), bytes, add)
}

func adjustNodePropertyBudget(graph, source *store.GraphState, defs store.PropertyIndexes, id uint64, add bool) {
	record := source.Nodes.Get(id)
	if record == nil {
		return
	}
	for def := range defs.DefinitionsFor(record.Labels, record.Properties) {
		adjustPropertyIndexBudget(graph, def, record.Properties[def.Property], true, add)
	}
}

func adjustEdgePropertyBudget(graph, source *store.GraphState, defs store.PropertyIndexes, id uint64, add bool) {
	record := source.Edges.Get(id)
	if record == nil {
		return
	}
	for def := range defs.DefinitionsFor([]string{record.Type}, record.Properties) {
		adjustPropertyIndexBudget(graph, def, record.Properties[def.Property], true, add)
	}
}

func (db *DB) VectorIndexStats() (VectorIndexStats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return VectorIndexStats{}, ErrDatabaseClosed
	}
	live := db.graph.VectorLiveCount
	threshold := uint64(vectorRebuildThreshold(db.graph))
	debt := uint64(db.graph.VectorTombstones.Len()) + db.graph.VectorMutations
	remaining := uint64(0)
	triggerDebt := saturatingAdd(threshold, 1)
	if debt < triggerDebt {
		remaining = triggerDebt - debt
	}
	tombstoneBytes := saturatingMul(uint64(db.graph.VectorTombstones.Len()), uint64(db.graph.VectorDimensions)*4)
	tombstoneRemaining := uint64(0)
	if tombstoneBytes <= 64<<20 {
		tombstoneRemaining = (64 << 20) + 1 - tombstoneBytes
	}
	return VectorIndexStats{
		LiveEntries:                live,
		IndexEntries:               uint64(db.graph.VectorIndex.Nodes.Len()),
		Tombstones:                 uint64(db.graph.VectorTombstones.Len()),
		TombstoneBytes:             tombstoneBytes,
		TombstoneBytesUntilRebuild: tombstoneRemaining,
		MutationDebt:               db.graph.VectorMutations,
		RebuildThreshold:           threshold,
		DebtUntilRebuild:           remaining,
		EstimatedBuildLogicalBytes: estimateVectorBuildLogicalBytes(db.graph, live),
		ExactFallbacks:             db.vectorExactFallbacks.Load(),
		Rebuilds:                   db.vectorRebuilds.Load(),
		RebuildNanoseconds:         db.vectorRebuildNanos.Load(),
	}, nil
}

func (db *DB) RebuildVectorIndexContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.enableVector || db.disableVectorIndex {
		db.mu.Unlock()
		return fmt.Errorf("%w: synchronous HNSW mode is not enabled", ErrUnsupportedOption)
	}
	if state := db.vectorRebuild; state != nil && state.err == nil {
		done := state.done
		db.mu.Unlock()
		select {
		case <-done:
			return state.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	buildCtx, cancel := context.WithCancel(ctx)
	graph := store.CloneGraphStateShallow(db.graph)
	state := &vectorRebuildState{graph: graph, dimensions: db.vectorDimensions, maxWork: db.vectorIndexBuildMaxWork, maxBytes: db.vectorIndexBuildMaxLogicalBytes, buildBytes: estimateVectorBuildLogicalBytes(graph, graph.VectorLiveCount), done: make(chan struct{}), cancel: cancel}
	db.vectorRebuild = state
	hook := db.vectorRebuildBeforeBuild
	db.mu.Unlock()
	if hook != nil {
		hook()
	}
	started := time.Now()
	err := db.runVectorRebuild(buildCtx, state)
	cancel()
	db.mu.Lock()
	if state.err != nil {
		err = state.err
	}
	if db.closed && errors.Is(err, context.Canceled) {
		err = ErrDatabaseClosed
	}
	if db.vectorRebuild == state {
		db.vectorRebuild = nil
	}
	state.err = err
	close(state.done)
	db.mu.Unlock()
	if err == nil {
		db.vectorRebuilds.Add(1)
		db.vectorRebuildNanos.Add(uint64(time.Since(started)))
		db.requestBackgroundCheckpoint()
	}
	return err
}

func (db *DB) runVectorRebuild(ctx context.Context, state *vectorRebuildState) error {
	budget := &directSearchBudget{ctx: ctx, maxWork: state.maxWork, maxBytes: state.maxBytes, annVisitedLimit: ^uint64(0)}
	if err := rebuildVectorIndexWithBudget(ctx, state.graph, budget); err != nil {
		return err
	}
	for {
		db.mu.Lock()
		if db.closed || db.vectorRebuild != state || state.err != nil || db.disableVectorIndex || db.vectorDimensions != state.dimensions {
			err := state.err
			db.mu.Unlock()
			if err != nil {
				return err
			}
			if db.closed {
				return ErrDatabaseClosed
			}
			return ErrWriteConflict
		}
		deltas := state.deltas
		state.deltas = nil
		db.mu.Unlock()
		for _, delta := range deltas {
			if delta.before == nil && delta.after != nil {
				state.graph.VectorLiveCount++
			} else if delta.before != nil && delta.after == nil && state.graph.VectorLiveCount > 0 {
				state.graph.VectorLiveCount--
			}
			if delta.after != nil {
				if err := insertVectorIndexVectorBudget(state.graph, delta.id, delta.after, true, nil, budget); err != nil {
					return err
				}
			} else {
				tombstoneVectorIndex(state.graph, delta.id, delta.before)
			}
		}
		if !db.writeMu.TryLock() {
			if err := ctx.Err(); err != nil {
				return err
			}
			time.Sleep(time.Millisecond)
			continue
		}
		db.mu.Lock()
		if db.closed || db.vectorRebuild != state || state.err != nil || db.disableVectorIndex || db.vectorDimensions != state.dimensions {
			db.mu.Unlock()
			db.writeMu.Unlock()
			continue
		}
		if len(state.deltas) != 0 {
			db.mu.Unlock()
			db.writeMu.Unlock()
			continue
		}
		published := store.CloneGraphStateShallow(db.graph)
		published.VectorIndex = state.graph.VectorIndex
		published.VectorTombstones = state.graph.VectorTombstones
		published.VectorLiveCount = state.graph.VectorLiveCount
		published.VectorMutations = 0
		db.graph = published
		db.mu.Unlock()
		db.writeMu.Unlock()
		return nil
	}
}

func (db *DB) vectorRebuildActive() bool {
	db.mu.RLock()
	active := db.vectorRebuild != nil && db.vectorRebuild.err == nil
	db.mu.RUnlock()
	return active
}

func (tx *Tx) vectorRebuildDeltas() []vectorRebuildDelta {
	if tx.base == nil || tx.changes == nil {
		return nil
	}
	deltas := make([]vectorRebuildDelta, 0, len(tx.changes.upsertNodes)+len(tx.changes.deleteNodes))
	for _, id := range mapKeys(tx.changes.upsertNodes) {
		before, beforeOK := selectedVector(tx.base, tx.base.Nodes.Get(id))
		after, afterOK := selectedVector(tx.graph, tx.graph.Nodes.Get(id))
		if beforeOK == afterOK && slices.Equal(before, after) {
			continue
		}
		delta := vectorRebuildDelta{id: id}
		if beforeOK {
			delta.before = slices.Clone(before)
		}
		if afterOK {
			delta.after = slices.Clone(after)
		}
		deltas = append(deltas, delta)
	}
	for _, id := range mapKeys(tx.changes.deleteNodes) {
		if before, ok := selectedVector(tx.base, tx.base.Nodes.Get(id)); ok {
			deltas = append(deltas, vectorRebuildDelta{id: id, before: slices.Clone(before)})
		}
	}
	return deltas
}

func (db *DB) appendVectorRebuildDeltasLocked(deltas []vectorRebuildDelta) {
	state := db.vectorRebuild
	if state == nil || state.err != nil || len(deltas) == 0 {
		return
	}
	for _, delta := range deltas {
		bytes := saturatingAdd(32, saturatingMul(uint64(len(delta.before)+len(delta.after)), 4))
		work := saturatingAdd(uint64(len(delta.before)), uint64(len(delta.after)))
		if state.buildBytes > state.maxBytes || state.logBytes > state.maxBytes-state.buildBytes || bytes > state.maxBytes-state.buildBytes-state.logBytes || state.logWork > state.maxWork || work > state.maxWork-state.logWork {
			state.err = ErrResourceLimit
			state.cancel()
			return
		}
		state.logBytes += bytes
		state.logWork += work
		state.deltas = append(state.deltas, delta)
	}
}

func (db *DB) cachedQueryPlan(query string) (*queryPlan, error) {
	db.cacheMu.RLock()
	plan, ok := db.queryCache[query]
	if ok {
		db.cacheHits.Add(1)
	}
	db.cacheMu.RUnlock()
	if ok {
		return plan, nil
	}

	plan, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	db.cacheMu.Lock()
	if cached, loaded := db.queryCache[query]; loaded {
		db.cacheHits.Add(1)
		plan = cached
	} else {
		if len(db.queryCache) == queryCacheEntries {
			// ponytail: FIFO bounds memory without LRU bookkeeping; add LRU only if measured hit rate suffers.
			delete(db.queryCache, db.queryCacheKeys[db.queryCacheNext])
		}
		db.queryCacheKeys[db.queryCacheNext] = query
		db.queryCacheNext = (db.queryCacheNext + 1) % queryCacheEntries
		db.queryCache[query] = plan
		db.cacheMisses.Add(1)
	}
	db.cacheMu.Unlock()
	return plan, nil
}

func (tx *Tx) Commit() error {
	return tx.CommitContext(context.Background())
}

func (tx *Tx) CommitContext(ctx context.Context) error {
	if tx.managed {
		return ErrManagedTransaction
	}
	err := tx.commitInternalContext(ctx)
	if db := tx.finish(); db != nil {
		db.requestBackgroundCheckpoint()
	}
	return err
}

func (tx *Tx) commitInternalContext(ctx context.Context) error {
	if tx.closed {
		return ErrInactiveTx
	}
	if tx.readOnly {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if tx.db.enableVector {
		for id := range tx.changes.upsertNodes {
			if err := validateNodeVectors(tx.graph.VectorDimensions, tx.graph.Nodes.Get(id)); err != nil {
				return err
			}
		}
		if !tx.vectorLiveCountApplied {
			tx.applyVectorLiveCountChanges()
			tx.vectorLiveCountApplied = true
		}
	}
	if !tx.propertyIndexesApplied {
		if err := tx.applyPropertyIndexChanges(); err != nil {
			return err
		}
		tx.propertyIndexesApplied = true
	}
	if tx.db.enableVector && !tx.db.disableVectorIndex && !tx.vectorIndexApplied {
		if err := tx.applyVectorIndexChanges(); err != nil {
			return err
		}
		tx.vectorIndexApplied = true
	}
	if tx.graph.DerivedIndexWork > tx.db.derivedIndexBuildMaxWork || tx.graph.DerivedIndexLogicalBytes > tx.db.derivedIndexBuildMaxLogicalBytes {
		return fmt.Errorf("%w: derived-index budget exceeded", ErrResourceLimit)
	}

	tx.db.mu.Lock()
	if tx.db.closed {
		tx.db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if tx.db.readOnly {
		tx.db.mu.Unlock()
		return ErrReadOnly
	}
	if tx.db.recoveryRequired {
		tx.db.mu.Unlock()
		return ErrRecoveryRequired
	}
	if tx.db.commitID != tx.changes.baseCommitID {
		tx.db.mu.Unlock()
		return ErrWriteConflict
	}
	if tx.db.commitID == ^uint64(0) {
		tx.db.mu.Unlock()
		return errors.New("commit id space exhausted")
	}
	nextCommitID := tx.db.commitID + 1
	nextNodeID, nextEdgeID, wal := tx.db.nextNodeID, tx.db.nextEdgeID, tx.db.wal
	if tx.db.checkpointInFlight.Load() || tx.db.checkpointNeeded.Load() {
		tail, tailErr := wal.TailSize()
		if tailErr == nil && tail >= 0 && uint64(tail) >= tx.db.walCheckpointThresholdBytes {
			tx.db.mu.Unlock()
			return fmt.Errorf("%w: WAL checkpoint is in progress", ErrResourceLimit)
		}
	}
	tx.db.mu.Unlock()

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !tx.changefeedApplied {
		if err := tx.appendChangefeed(); err != nil {
			return err
		}
		tx.changefeedApplied = true
	}
	delta := store.GraphDelta{
		UpsertNodes:       mapKeys(tx.changes.upsertNodes),
		DeleteNodes:       mapKeys(tx.changes.deleteNodes),
		UpsertEdges:       mapKeys(tx.changes.upsertEdges),
		DeleteEdges:       mapKeys(tx.changes.deleteEdges),
		UpsertFTS:         mapKeys(tx.changes.upsertFTS),
		DeleteFTS:         mapKeys(tx.changes.deleteFTS),
		AppMetadata:       persistedAppMetadataChanges(tx.changes.appMetadata),
		CreateNodeIndexes: propertyIndexKeys(tx.changes.createNodeIndexes),
		DropNodeIndexes:   propertyIndexKeys(tx.changes.dropNodeIndexes),
		CreateEdgeIndexes: propertyIndexKeys(tx.changes.createEdgeIndexes),
		DropEdgeIndexes:   propertyIndexKeys(tx.changes.dropEdgeIndexes),
		StreamsChanged:    tx.changes.streamsChanged,
		StreamOperations:  slices.Clone(tx.changes.streamOperations),
	}
	var snapshotBytes uint64
	var sizeErr error
	if tx.base == nil {
		snapshotBytes, sizeErr = store.EstimateSnapshotBytes(tx.graph)
	} else {
		snapshotBytes, sizeErr = store.ApplyDeltaSnapshotBytes(tx.base, tx.graph, delta)
	}
	if sizeErr != nil {
		return sizeErr
	}
	if snapshotBytes > tx.db.maxDatabaseSnapshotBytes {
		return fmt.Errorf("%w: database snapshot requires %d bytes, limit is %d", ErrResourceLimit, snapshotBytes, tx.db.maxDatabaseSnapshotBytes)
	}
	tx.graph.SnapshotBytes = snapshotBytes
	var err error
	if tx.base == nil {
		err = wal.AppendSnapshot(tx.graph, nextNodeID, nextEdgeID, nextCommitID)
	} else {
		err = wal.AppendDelta(tx.graph, nextNodeID, nextEdgeID, nextCommitID, delta)
	}
	if err != nil {
		if errors.Is(err, store.ErrCommitOutcomeUnknown) {
			tx.db.mu.Lock()
			tx.db.recoveryRequired = true
			tx.db.mu.Unlock()
		}
		return err
	}
	vectorDeltas := tx.vectorRebuildDeltas()
	tx.db.mu.Lock()
	tx.db.graph = tx.graph
	tx.db.commitID = nextCommitID
	tx.db.appendVectorRebuildDeltasLocked(vectorDeltas)
	tx.db.dirty = true
	if len(delta.DeleteEdges) != 0 {
		if tx.db.enqueueAdjacencyCandidatesLocked(tx.graph, tx.base, delta) {
			tx.db.adjacencyMaintenanceNeeded.Store(true)
		}
	}
	if size, sizeErr := wal.TailSize(); sizeErr == nil && size >= 0 && uint64(size) >= tx.db.walCheckpointThresholdBytes {
		tx.db.checkpointNeeded.Store(true)
	}
	tx.db.notifyStreamsLocked()
	tx.db.mu.Unlock()
	return nil
}

func (tx *Tx) applyPropertyIndexChanges() error {
	if tx.base == nil || tx.changes == nil {
		return nil
	}
	for id := range tx.changes.deleteNodes {
		adjustNodeDerivedCost(tx.graph, tx.base, id, false)
		adjustNodePropertyBudget(tx.graph, tx.base, tx.base.NodeProperties, id, false)
		if err := removeNodePropertyIndexes(&tx.graph.NodeProperties, tx.base.Nodes.Get(id)); err != nil {
			return err
		}
	}
	for id := range tx.changes.upsertNodes {
		adjustNodeDerivedCost(tx.graph, tx.base, id, false)
		adjustNodePropertyBudget(tx.graph, tx.base, tx.base.NodeProperties, id, false)
		if err := removeNodePropertyIndexes(&tx.graph.NodeProperties, tx.base.Nodes.Get(id)); err != nil {
			return err
		}
		if err := addNodePropertyIndexes(&tx.graph.NodeProperties, tx.graph.Nodes.Get(id)); err != nil {
			return err
		}
		adjustNodePropertyBudget(tx.graph, tx.graph, tx.base.NodeProperties, id, true)
		adjustNodeDerivedCost(tx.graph, tx.graph, id, true)
	}
	for id := range tx.changes.deleteEdges {
		adjustEdgeDerivedCost(tx.graph, tx.base, id, false)
		adjustEdgePropertyBudget(tx.graph, tx.base, tx.base.EdgeProperties, id, false)
		if err := removeEdgePropertyIndexes(&tx.graph.EdgeProperties, tx.base.Edges.Get(id)); err != nil {
			return err
		}
	}
	for id := range tx.changes.upsertEdges {
		adjustEdgeDerivedCost(tx.graph, tx.base, id, false)
		adjustEdgePropertyBudget(tx.graph, tx.base, tx.base.EdgeProperties, id, false)
		if err := removeEdgePropertyIndexes(&tx.graph.EdgeProperties, tx.base.Edges.Get(id)); err != nil {
			return err
		}
		if err := addEdgePropertyIndexes(&tx.graph.EdgeProperties, tx.graph.Edges.Get(id)); err != nil {
			return err
		}
		adjustEdgePropertyBudget(tx.graph, tx.graph, tx.base.EdgeProperties, id, true)
		adjustEdgeDerivedCost(tx.graph, tx.graph, id, true)
	}
	for id := range tx.changes.deleteFTS {
		adjustFTSDerivedCost(tx.graph, tx.base, id, false)
	}
	for id := range tx.changes.upsertFTS {
		adjustFTSDerivedCost(tx.graph, tx.base, id, false)
		adjustFTSDerivedCost(tx.graph, tx.graph, id, true)
	}
	return nil
}

func addNodePropertyIndexes(indexes *store.PropertyIndexes, node *store.NodeRecord) error {
	if node == nil {
		return nil
	}
	for definition := range indexes.DefinitionsFor(node.Labels, node.Properties) {
		if err := indexes.Add(definition, node.Properties[definition.Property], node.ID); err != nil {
			return err
		}
	}
	return nil
}

func removeNodePropertyIndexes(indexes *store.PropertyIndexes, node *store.NodeRecord) error {
	if node == nil {
		return nil
	}
	for definition := range indexes.DefinitionsFor(node.Labels, node.Properties) {
		if err := indexes.Remove(definition, node.Properties[definition.Property], node.ID); err != nil {
			return err
		}
	}
	return nil
}

func addEdgePropertyIndexes(indexes *store.PropertyIndexes, edge *store.EdgeRecord) error {
	if edge == nil {
		return nil
	}
	for definition := range indexes.DefinitionsFor([]string{edge.Type}, edge.Properties) {
		if err := indexes.Add(definition, edge.Properties[definition.Property], edge.ID); err != nil {
			return err
		}
	}
	return nil
}

func removeEdgePropertyIndexes(indexes *store.PropertyIndexes, edge *store.EdgeRecord) error {
	if edge == nil {
		return nil
	}
	for definition := range indexes.DefinitionsFor([]string{edge.Type}, edge.Properties) {
		if err := indexes.Remove(definition, edge.Properties[definition.Property], edge.ID); err != nil {
			return err
		}
	}
	return nil
}

func (tx *Tx) Rollback() error {
	if tx.managed {
		return ErrManagedTransaction
	}
	return tx.rollbackInternal()
}

func (tx *Tx) rollbackInternal() error {
	if tx.closed {
		return ErrInactiveTx
	}
	if db := tx.finish(); db != nil {
		db.requestBackgroundCheckpoint()
	}
	return nil
}

func (tx *Tx) finish() *DB {
	if tx.closed {
		return nil
	}
	tx.closed = true
	tx.db.activeTx.Add(-1)
	requestCheckpoint := tx.writeLocked && (tx.db.checkpointNeeded.Load() || tx.db.adjacencyMaintenanceNeeded.Load())
	if tx.writeLocked {
		tx.writeLocked = false
		tx.db.writeMu.Unlock()
	}
	if requestCheckpoint {
		return tx.db
	}
	return nil
}

func (tx *Tx) CreateNode(opts CreateNodeOptions) (Node, error) {
	if err := tx.ensureWritable(); err != nil {
		return Node{}, err
	}
	if err := store.ValidateCreateLabels(opts.Labels); err != nil {
		return Node{}, err
	}

	props, err := store.NormalizeProperties(opts.Properties)
	if err != nil {
		return Node{}, err
	}
	if tx.db.enableVector {
		if err := validateVectorProperties(props); err != nil {
			return Node{}, err
		}
		for _, value := range props {
			if vector, ok := value.([]float32); ok && tx.db.vectorDimensions > 0 && len(vector) != int(tx.db.vectorDimensions) {
				return Node{}, fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), tx.db.vectorDimensions)
			}
		}
	}

	id, err := tx.db.allocateNodeID()
	if err != nil {
		return Node{}, err
	}
	record := &store.NodeRecord{
		ID:         id,
		Labels:     slices.Clone(opts.Labels),
		Properties: props,
	}
	tx.ensureNodesWritable(id)
	tx.graph.Nodes.Set(id, record)
	for _, label := range record.Labels {
		tx.graph.Labels.Add(label, id)
	}
	tx.markUpsert(&tx.changes.upsertNodes, &tx.changes.deleteNodes, id)
	return publicNode(record), nil
}

func (tx *Tx) DeleteNode(nodeID uint64) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateEntityID(nodeID); err != nil {
		return err
	}
	if node := tx.graph.Nodes.Get(nodeID); node != nil {
		for _, label := range node.Labels {
			tx.graph.Labels.Remove(label, nodeID)
		}
	}
	tx.ensureNodesWritable(nodeID)
	tx.graph.Nodes.Delete(nodeID)
	tx.markDelete(&tx.changes.upsertNodes, &tx.changes.deleteNodes, idExists(tx.base, nodeID, true), nodeID)
	if record := tx.graph.FTS.Get(nodeID); record != nil {
		for _, token := range record.Tokens {
			tx.graph.FTSTokens.Remove(token, nodeID)
		}
		tx.ensureFTSWritable(nodeID)
		tx.graph.FTS.Delete(nodeID)
		tx.markDelete(&tx.changes.upsertFTS, &tx.changes.deleteFTS, tx.base != nil && tx.base.FTS.Get(nodeID) != nil, nodeID)
	}
	edges := make(map[uint64]struct{})
	for _, adjacency := range []*store.EdgeList{tx.graph.Outgoing.Get(nodeID), tx.graph.Incoming.Get(nodeID)} {
		for chunk := range adjacency.Chunks() {
			for _, edgeID := range chunk {
				if !adjacency.IsRemoved(edgeID) {
					edges[edgeID] = struct{}{}
				}
			}
		}
	}
	tx.deleteEdgesBatch(edges)
	tx.ensureOutgoingWritable(nodeID)
	tx.ensureIncomingWritable(nodeID)
	tx.graph.Outgoing.Delete(nodeID)
	tx.graph.Incoming.Delete(nodeID)
	return nil
}

func (tx *Tx) NodeExists(nodeID uint64) (bool, error) {
	if tx.closed {
		return false, ErrInactiveTx
	}
	if err := validateEntityID(nodeID); err != nil {
		return false, err
	}
	return tx.graph.Nodes.Get(nodeID) != nil, nil
}

func (tx *Tx) GetNode(nodeID uint64) (*Node, error) {
	node, ok, err := tx.GetNodeValue(nodeID)
	if err != nil || !ok {
		return nil, err
	}
	return &node, nil
}

func (tx *Tx) GetNodeValue(nodeID uint64) (Node, bool, error) {
	if tx.closed {
		return Node{}, false, ErrInactiveTx
	}
	if err := validateEntityID(nodeID); err != nil {
		return Node{}, false, err
	}
	node := tx.graph.Nodes.Get(nodeID)
	if node == nil {
		return Node{}, false, nil
	}
	return publicNode(node), true, nil
}

func (tx *Tx) SetProperty(nodeID uint64, key string, value any) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := store.ValidatePropertyKey(key); err != nil {
		return err
	}
	node, err := tx.writableNode(nodeID)
	if err != nil {
		return err
	}
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return err
	}
	if vector, ok := normalized.([]float32); ok && tx.db.enableVector && tx.db.vectorDimensions > 0 && len(vector) != int(tx.db.vectorDimensions) {
		return fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), tx.db.vectorDimensions)
	}
	if tx.db.enableVector {
		if err := validateVectorPropertyUpdate(node, key, normalized); err != nil {
			return err
		}
	}
	node.Properties[key] = normalized
	return nil
}

func (tx *Tx) GetProperty(nodeID uint64, key string) (any, bool, error) {
	node, err := tx.requireNode(nodeID)
	if err != nil {
		return nil, false, err
	}
	value, ok := node.Properties[key]
	if !ok {
		return nil, false, nil
	}
	return store.CloneValue(value), true, nil
}

func (tx *Tx) FindNodesByLabelProperty(label, property string, value any, limit uint) ([]uint64, error) {
	if tx.closed {
		return nil, ErrInactiveTx
	}
	if limit == 0 {
		return nil, fmt.Errorf("%w: property index lookup limit must be positive", ErrInvalidArgument)
	}
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return nil, err
	}
	definition := store.PropertyIndexDefinition{Scope: label, Property: property}
	results, exists, err := tx.findNodePropertyIndex(definition, normalized, limit)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: node property index does not exist", ErrUnsupportedOption)
	}
	if tx.changes != nil {
		for id := range tx.changes.upsertNodes {
			if nodeMatchesPropertyIndex(tx.graph.Nodes.Get(id), definition, normalized) {
				results = insertPropertyIndexID(results, id, limit)
			}
		}
	}
	return results, nil
}

func (tx *Tx) findNodePropertyIndex(definition store.PropertyIndexDefinition, value any, limit uint) ([]uint64, bool, error) {
	if limit == ^uint(0) {
		ids, exists, err := tx.graph.NodeProperties.Lookup(definition, value)
		if err != nil || !exists {
			return ids, exists, err
		}
		results := ids[:0]
		for _, id := range ids {
			if nodeMatchesPropertyIndex(tx.graph.Nodes.Get(id), definition, value) {
				results = append(results, id)
			}
		}
		return results, true, nil
	}
	if tx.changes == nil {
		return tx.graph.NodeProperties.LookupLimit(definition, value, limit)
	}
	capacity := 64
	if limit < uint(capacity) {
		capacity = int(limit)
	}
	results := make([]uint64, 0, capacity)
	exists, err := tx.graph.NodeProperties.Visit(definition, value, func(id uint64) bool {
		if nodeMatchesPropertyIndex(tx.graph.Nodes.Get(id), definition, value) {
			results = insertPropertyIndexID(results, id, limit)
		}
		return true
	})
	return results, exists, err
}

func nodeMatchesPropertyIndex(node *store.NodeRecord, definition store.PropertyIndexDefinition, value any) bool {
	if node == nil || !slices.Contains(node.Labels, definition.Scope) {
		return false
	}
	stored, ok := node.Properties[definition.Property]
	return ok && store.PropertyValuesEqual(stored, value)
}

func (tx *Tx) SetVector(nodeID uint64, key string, vector []float32) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if !tx.db.enableVector {
		return fmt.Errorf("%w: vector support is disabled", ErrUnsupportedOption)
	}
	if err := store.ValidatePropertyKey(key); err != nil {
		return err
	}
	if tx.db.vectorDimensions > 0 && len(vector) != int(tx.db.vectorDimensions) {
		return fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), tx.db.vectorDimensions)
	}
	node, err := tx.writableNode(nodeID)
	if err != nil {
		return err
	}
	normalized, err := store.NormalizeValue(vector)
	if err != nil {
		return err
	}
	if err := validateVectorPropertyUpdate(node, key, normalized); err != nil {
		return err
	}
	node.Properties[key] = normalized
	return nil
}

// BatchInsertVectors inserts vector-bearing nodes with label. Vectors are stored
// in the "vector" property for compatibility with the public API.
func (tx *Tx) BatchInsertVectors(label string, vectors [][]float32) ([]uint64, error) {
	if err := tx.ensureWritable(); err != nil {
		return nil, err
	}
	if err := store.ValidateCreateLabels([]string{label}); err != nil {
		return nil, err
	}
	if !tx.db.enableVector {
		return nil, fmt.Errorf("%w: vector support is disabled", ErrUnsupportedOption)
	}
	for _, vector := range vectors {
		if tx.db.vectorDimensions > 0 && len(vector) != int(tx.db.vectorDimensions) {
			return nil, fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), tx.db.vectorDimensions)
		}
		if _, err := store.NormalizeValue(vector); err != nil {
			return nil, err
		}
	}

	ids := make([]uint64, len(vectors))
	for i, vector := range vectors {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{label}})
		if err != nil {
			return nil, err
		}
		if err := tx.SetVector(node.ID, "vector", vector); err != nil {
			return nil, err
		}
		ids[i] = node.ID
	}
	return ids, nil
}

// Deprecated: use BatchInsertVectors. Earliest removal is v0.6.0.
func (tx *Tx) BatchInsert(label string, vectors [][]float32) ([]uint64, error) {
	return tx.BatchInsertVectors(label, vectors)
}

func (tx *Tx) FTSIndex(nodeID uint64, text string) error {
	return tx.FTSIndexContext(context.Background(), nodeID, text)
}

func (tx *Tx) FTSIndexContext(ctx context.Context, nodeID uint64, text string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.ValidateFTSText(text); err != nil {
		return err
	}
	if saturatingMul(uint64(len(text)), 2) > tx.db.derivedIndexBuildMaxWork {
		return fmt.Errorf("%w: FTS text exceeds derived-index build budget", ErrResourceLimit)
	}
	if _, err := tx.requireNode(nodeID); err != nil {
		return err
	}
	tokens, err := search.TokenizeContextWithLimit(ctx, text, tx.db.derivedIndexBuildMaxLogicalBytes)
	if err != nil {
		if errors.Is(err, search.ErrTokenizationLimit) {
			return fmt.Errorf("%w: FTS tokenization exceeds derived-index build budget", ErrResourceLimit)
		}
		return err
	}
	var logicalBytes uint64
	for _, token := range tokens {
		logicalBytes = saturatingAdd(logicalBytes, uint64(len(token))+144)
	}
	if uint64(len(tokens)) > tx.db.derivedIndexBuildMaxWork-uint64(len(text)) || logicalBytes > tx.db.derivedIndexBuildMaxLogicalBytes {
		return fmt.Errorf("%w: FTS postings exceed derived-index build budget", ErrResourceLimit)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if old := tx.graph.FTS.Get(nodeID); old != nil {
		for _, token := range old.Tokens {
			tx.graph.FTSTokens.Remove(token, nodeID)
		}
	}
	tx.ensureFTSWritable(nodeID)
	tx.graph.FTS.Set(nodeID, &store.FTSRecord{Text: text, Tokens: tokens})
	for _, token := range tokens {
		tx.graph.FTSTokens.Add(token, nodeID)
	}
	tx.markUpsert(&tx.changes.upsertFTS, &tx.changes.deleteFTS, nodeID)
	return nil
}

func (tx *Tx) CreateEdge(sourceID uint64, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	if err := tx.ensureWritable(); err != nil {
		return Edge{}, err
	}
	if err := store.ValidateEdgeType(edgeType); err != nil {
		return Edge{}, err
	}
	if _, err := tx.requireNode(sourceID); err != nil {
		return Edge{}, err
	}
	if _, err := tx.requireNode(targetID); err != nil {
		return Edge{}, err
	}

	props, err := store.NormalizeProperties(opts.Properties)
	if err != nil {
		return Edge{}, err
	}

	id, err := tx.db.allocateEdgeID()
	if err != nil {
		return Edge{}, err
	}
	record := &store.EdgeRecord{
		ID:         id,
		SourceID:   sourceID,
		TargetID:   targetID,
		Type:       edgeType,
		Properties: props,
	}
	tx.ensureEdgesWritable(id)
	tx.ensureOutgoingWritable(sourceID)
	tx.ensureIncomingWritable(targetID)
	tx.graph.Edges.Set(id, record)
	tx.graph.EdgeTypes.Add(edgeType, id)
	tx.markUpsert(&tx.changes.upsertEdges, &tx.changes.deleteEdges, id)
	tx.graph.Outgoing.Set(sourceID, tx.graph.Outgoing.Get(sourceID).Append(id))
	tx.graph.Incoming.Set(targetID, tx.graph.Incoming.Get(targetID).Append(id))
	return publicEdge(record), nil
}

func (tx *Tx) GetEdgeProperty(edgeID uint64, key string) (any, bool, error) {
	edge, err := tx.requireEdge(edgeID)
	if err != nil {
		return nil, false, err
	}
	value, ok := edge.Properties[key]
	if !ok {
		return nil, false, nil
	}
	return store.CloneValue(value), true, nil
}

func (tx *Tx) FindEdgesByTypeProperty(edgeType, property string, value any, limit uint) ([]uint64, error) {
	if tx.closed {
		return nil, ErrInactiveTx
	}
	if limit == 0 {
		return nil, fmt.Errorf("%w: property index lookup limit must be positive", ErrInvalidArgument)
	}
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return nil, err
	}
	definition := store.PropertyIndexDefinition{Scope: edgeType, Property: property}
	results, exists, err := tx.findEdgePropertyIndex(definition, normalized, limit)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: edge property index does not exist", ErrUnsupportedOption)
	}
	if tx.changes != nil {
		for id := range tx.changes.upsertEdges {
			if edgeMatchesPropertyIndex(tx.graph.Edges.Get(id), definition, normalized) {
				results = insertPropertyIndexID(results, id, limit)
			}
		}
	}
	return results, nil
}

func (tx *Tx) findEdgePropertyIndex(definition store.PropertyIndexDefinition, value any, limit uint) ([]uint64, bool, error) {
	if limit == ^uint(0) {
		ids, exists, err := tx.graph.EdgeProperties.Lookup(definition, value)
		if err != nil || !exists {
			return ids, exists, err
		}
		results := ids[:0]
		for _, id := range ids {
			if edgeMatchesPropertyIndex(tx.graph.Edges.Get(id), definition, value) {
				results = append(results, id)
			}
		}
		return results, true, nil
	}
	if tx.changes == nil {
		return tx.graph.EdgeProperties.LookupLimit(definition, value, limit)
	}
	capacity := 64
	if limit < uint(capacity) {
		capacity = int(limit)
	}
	results := make([]uint64, 0, capacity)
	exists, err := tx.graph.EdgeProperties.Visit(definition, value, func(id uint64) bool {
		if edgeMatchesPropertyIndex(tx.graph.Edges.Get(id), definition, value) {
			results = insertPropertyIndexID(results, id, limit)
		}
		return true
	})
	return results, exists, err
}

func insertPropertyIndexID(ids []uint64, id uint64, limit uint) []uint64 {
	if uint(len(ids)) == limit && id >= ids[len(ids)-1] {
		return ids
	}
	index, found := slices.BinarySearch(ids, id)
	if found {
		return ids
	}
	if uint(len(ids)) == limit {
		ids = ids[:len(ids)-1]
	}
	ids = append(ids, 0)
	copy(ids[index+1:], ids[index:])
	ids[index] = id
	return ids
}

func edgeMatchesPropertyIndex(edge *store.EdgeRecord, definition store.PropertyIndexDefinition, value any) bool {
	if edge == nil || edge.Type != definition.Scope {
		return false
	}
	stored, ok := edge.Properties[definition.Property]
	return ok && store.PropertyValuesEqual(stored, value)
}

func (tx *Tx) SetEdgeProperty(edgeID uint64, key string, value any) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := store.ValidatePropertyKey(key); err != nil {
		return err
	}
	edge, err := tx.writableEdge(edgeID)
	if err != nil {
		return err
	}
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return err
	}
	edge.Properties[key] = normalized
	return nil
}

func (tx *Tx) RemoveEdgeProperty(edgeID uint64, key string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := store.ValidatePropertyKey(key); err != nil {
		return err
	}
	edge, err := tx.writableEdge(edgeID)
	if err != nil {
		return err
	}
	delete(edge.Properties, key)
	return nil
}

func (tx *Tx) GetOutgoingEdges(nodeID uint64) ([]Edge, error) {
	if _, err := tx.requireNode(nodeID); err != nil {
		return nil, err
	}
	outgoing := tx.graph.Outgoing.Get(nodeID)
	results := make([]Edge, 0, outgoing.Len())
	if outgoing.IsInline() {
		for _, edgeID := range outgoing.InlineIDs() {
			results = append(results, publicEdge(tx.graph.Edges.Get(edgeID)))
		}
		return results, nil
	}
	for chunk := range outgoing.Chunks() {
		for _, edgeID := range chunk {
			if !outgoing.IsRemoved(edgeID) {
				results = append(results, publicEdge(tx.graph.Edges.Get(edgeID)))
			}
		}
	}
	return results, nil
}

func (tx *Tx) GetIncomingEdges(nodeID uint64) ([]Edge, error) {
	if _, err := tx.requireNode(nodeID); err != nil {
		return nil, err
	}
	incoming := tx.graph.Incoming.Get(nodeID)
	results := make([]Edge, 0, incoming.Len())
	if incoming.IsInline() {
		for _, edgeID := range incoming.InlineIDs() {
			results = append(results, publicEdge(tx.graph.Edges.Get(edgeID)))
		}
		return results, nil
	}
	for chunk := range incoming.Chunks() {
		for _, edgeID := range chunk {
			if !incoming.IsRemoved(edgeID) {
				results = append(results, publicEdge(tx.graph.Edges.Get(edgeID)))
			}
		}
	}
	return results, nil
}

func (tx *Tx) GetOutgoingEdgesByType(nodeID uint64, edgeType string, limit uint) ([]Edge, error) {
	if _, err := tx.requireNode(nodeID); err != nil {
		return nil, err
	}
	return tx.edgesByType(tx.graph.Outgoing.Get(nodeID), edgeType, limit), nil
}

func (tx *Tx) GetIncomingEdgesByType(nodeID uint64, edgeType string, limit uint) ([]Edge, error) {
	if _, err := tx.requireNode(nodeID); err != nil {
		return nil, err
	}
	return tx.edgesByType(tx.graph.Incoming.Get(nodeID), edgeType, limit), nil
}

func (tx *Tx) edgesByType(edgeIDs *store.EdgeList, edgeType string, limit uint) []Edge {
	typedIDs := tx.graph.EdgeTypes.Get(edgeType)
	results := make([]Edge, 0, min(edgeIDs.Len(), len(typedIDs)))
	typedIndex := 0
	for chunk := range edgeIDs.Chunks() {
		for _, edgeID := range chunk {
			if edgeIDs.IsRemoved(edgeID) {
				continue
			}
			for typedIndex < len(typedIDs) && typedIDs[typedIndex] < edgeID {
				typedIndex++
			}
			if typedIndex == len(typedIDs) {
				return results
			}
			if typedIDs[typedIndex] == edgeID {
				results = append(results, publicEdge(tx.graph.Edges.Get(edgeID)))
				if limit != 0 && uint(len(results)) == limit {
					return results
				}
				typedIndex++
			}
		}
	}
	return results
}

func (tx *Tx) deleteEdge(edgeID uint64) {
	edge := tx.graph.Edges.Get(edgeID)
	if edge == nil {
		return
	}
	tx.ensureEdgesWritable(edgeID)
	tx.ensureOutgoingWritable(edge.SourceID)
	tx.ensureIncomingWritable(edge.TargetID)
	tx.graph.Edges.Delete(edgeID)
	tx.graph.EdgeTypes.Remove(edge.Type, edgeID)
	tx.markDelete(&tx.changes.upsertEdges, &tx.changes.deleteEdges, tx.base != nil && tx.base.Edges.Get(edgeID) != nil, edgeID)
	outgoing := tx.graph.Outgoing.Get(edge.SourceID).RemoveKnown(edgeID)
	if outgoing.Len() == 0 {
		tx.graph.Outgoing.Delete(edge.SourceID)
	} else {
		tx.graph.Outgoing.Set(edge.SourceID, outgoing)
	}
	incoming := tx.graph.Incoming.Get(edge.TargetID).RemoveKnown(edgeID)
	if incoming.Len() == 0 {
		tx.graph.Incoming.Delete(edge.TargetID)
	} else {
		tx.graph.Incoming.Set(edge.TargetID, incoming)
	}
}

func (tx *Tx) deleteEdgesBatch(edgeIDs map[uint64]struct{}) {
	outgoing := map[uint64][]uint64{}
	incoming := map[uint64][]uint64{}
	for edgeID := range edgeIDs {
		edge := tx.graph.Edges.Get(edgeID)
		if edge == nil {
			continue
		}
		tx.ensureEdgesWritable(edgeID)
		tx.graph.Edges.Delete(edgeID)
		tx.graph.EdgeTypes.Remove(edge.Type, edgeID)
		tx.markDelete(&tx.changes.upsertEdges, &tx.changes.deleteEdges, tx.base != nil && tx.base.Edges.Get(edgeID) != nil, edgeID)
		outgoing[edge.SourceID] = append(outgoing[edge.SourceID], edgeID)
		incoming[edge.TargetID] = append(incoming[edge.TargetID], edgeID)
	}
	for nodeID, ids := range outgoing {
		tx.ensureOutgoingWritable(nodeID)
		list := tx.graph.Outgoing.Get(nodeID).RemoveKnownBatch(ids)
		if list.Len() == 0 {
			tx.graph.Outgoing.Delete(nodeID)
		} else {
			tx.graph.Outgoing.Set(nodeID, list)
		}
	}
	for nodeID, ids := range incoming {
		tx.ensureIncomingWritable(nodeID)
		list := tx.graph.Incoming.Get(nodeID).RemoveKnownBatch(ids)
		if list.Len() == 0 {
			tx.graph.Incoming.Delete(nodeID)
		} else {
			tx.graph.Incoming.Set(nodeID, list)
		}
	}
}

func (tx *Tx) ensureWritable() error {
	if tx.closed {
		return ErrInactiveTx
	}
	if tx.readOnly {
		return ErrReadOnly
	}
	if tx.db.readOnly {
		return ErrReadOnly
	}
	return nil
}

func (tx *Tx) requireNode(nodeID uint64) (*store.NodeRecord, error) {
	if tx.closed {
		return nil, ErrInactiveTx
	}
	if err := validateEntityID(nodeID); err != nil {
		return nil, err
	}
	node := tx.graph.Nodes.Get(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}
	return node, nil
}

func (tx *Tx) requireEdge(edgeID uint64) (*store.EdgeRecord, error) {
	if tx.closed {
		return nil, ErrInactiveTx
	}
	if err := validateEntityID(edgeID); err != nil {
		return nil, err
	}
	edge := tx.graph.Edges.Get(edgeID)
	if edge == nil {
		return nil, fmt.Errorf("edge %d not found", edgeID)
	}
	return edge, nil
}

func validateEntityID(id uint64) error {
	if err := store.ValidateEntityID(id); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return nil
}

func (tx *Tx) writableNode(nodeID uint64) (*store.NodeRecord, error) {
	node, err := tx.requireNode(nodeID)
	if err != nil {
		return nil, err
	}
	if tx.base != nil && node == tx.base.Nodes.Get(nodeID) {
		node = &store.NodeRecord{
			ID:         node.ID,
			Labels:     slices.Clone(node.Labels),
			Properties: store.ClonePropertyMap(node.Properties),
		}
		tx.ensureNodesWritable(nodeID)
		tx.graph.Nodes.Set(nodeID, node)
		tx.markUpsert(&tx.changes.upsertNodes, &tx.changes.deleteNodes, nodeID)
	}
	return node, nil
}

func (tx *Tx) writableEdge(edgeID uint64) (*store.EdgeRecord, error) {
	edge, err := tx.requireEdge(edgeID)
	if err != nil {
		return nil, err
	}
	if tx.base != nil && edge == tx.base.Edges.Get(edgeID) {
		edge = &store.EdgeRecord{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: store.ClonePropertyMap(edge.Properties),
		}
		tx.ensureEdgesWritable(edgeID)
		tx.graph.Edges.Set(edgeID, edge)
		tx.markUpsert(&tx.changes.upsertEdges, &tx.changes.deleteEdges, edgeID)
	}
	return edge, nil
}

func (tx *Tx) ensureNodesWritable(id uint64) {
	tx.graph.Nodes.CloneShardOnce(id)
}

func (tx *Tx) ensureEdgesWritable(id uint64) {
	tx.graph.Edges.CloneShardOnce(id)
}

func (tx *Tx) ensureFTSWritable(id uint64) {
	tx.graph.FTS.CloneShardOnce(id)
}

func (tx *Tx) ensureOutgoingWritable(id uint64) {
	tx.graph.Outgoing.CloneShardOnce(id)
}

func (tx *Tx) ensureIncomingWritable(id uint64) {
	tx.graph.Incoming.CloneShardOnce(id)
}

func (tx *Tx) markUpsert(upserts *map[uint64]struct{}, deletes *map[uint64]struct{}, id uint64) {
	if tx.base == nil {
		return
	}
	if *upserts == nil {
		*upserts = map[uint64]struct{}{}
	}
	(*upserts)[id] = struct{}{}
	delete(*deletes, id)
}

func (tx *Tx) markDelete(upserts *map[uint64]struct{}, deletes *map[uint64]struct{}, existed bool, id uint64) {
	if tx.base == nil {
		return
	}
	delete(*upserts, id)
	if !existed {
		return
	}
	if *deletes == nil {
		*deletes = map[uint64]struct{}{}
	}
	(*deletes)[id] = struct{}{}
}

func idExists(base *store.GraphState, id uint64, node bool) bool {
	if base == nil {
		return false
	}
	if node {
		return base.Nodes.Get(id) != nil
	}
	return base.Edges.Get(id) != nil
}

func mapKeys(values map[uint64]struct{}) []uint64 {
	keys := make([]uint64, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func propertyIndexKeys(values map[store.PropertyIndexDefinition]struct{}) []store.PropertyIndexDefinition {
	keys := make([]store.PropertyIndexDefinition, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b store.PropertyIndexDefinition) int {
		if a.Scope != b.Scope {
			return strings.Compare(a.Scope, b.Scope)
		}
		return strings.Compare(a.Property, b.Property)
	})
	return keys
}

func (db *DB) allocateNodeID() (uint64, error) {
	db.mu.Lock()
	if db.nextNodeID >= store.EntityIDExhausted {
		db.mu.Unlock()
		return 0, errors.New("node id space exhausted")
	}
	if db.nextNodeID < db.reservedNodeID {
		id := db.nextNodeID
		db.nextNodeID++
		db.mu.Unlock()
		return id, nil
	}
	databaseID := db.graph.DatabaseID
	reservedNodeID := reserveIDBlock(db.nextNodeID)
	reservedEdgeID := db.reservedEdgeID
	db.mu.Unlock()
	if err := db.reserveIDsToDisk(databaseID, reservedNodeID, reservedEdgeID); err != nil {
		db.mu.Lock()
		db.recoveryRequired = true
		db.mu.Unlock()
		return 0, err
	}
	db.mu.Lock()
	db.reservedNodeID = reservedNodeID
	id := db.nextNodeID
	db.nextNodeID++
	db.mu.Unlock()
	return id, nil
}

func (db *DB) allocateEdgeID() (uint64, error) {
	db.mu.Lock()
	if db.nextEdgeID >= store.EntityIDExhausted {
		db.mu.Unlock()
		return 0, errors.New("edge id space exhausted")
	}
	if db.nextEdgeID < db.reservedEdgeID {
		id := db.nextEdgeID
		db.nextEdgeID++
		db.mu.Unlock()
		return id, nil
	}
	databaseID := db.graph.DatabaseID
	reservedNodeID := db.reservedNodeID
	reservedEdgeID := reserveIDBlock(db.nextEdgeID)
	db.mu.Unlock()
	if err := db.reserveIDsToDisk(databaseID, reservedNodeID, reservedEdgeID); err != nil {
		db.mu.Lock()
		db.recoveryRequired = true
		db.mu.Unlock()
		return 0, err
	}
	db.mu.Lock()
	db.reservedEdgeID = reservedEdgeID
	id := db.nextEdgeID
	db.nextEdgeID++
	db.mu.Unlock()
	return id, nil
}

func (db *DB) reserveIDsToDisk(databaseID string, nextNodeID, nextEdgeID uint64) error {
	if db.reserveIDs != nil {
		return db.reserveIDs(db.files, databaseID, nextNodeID, nextEdgeID)
	}
	return store.ReserveIDsFiles(db.files, databaseID, nextNodeID, nextEdgeID)
}

func reserveIDBlock(next uint64) uint64 {
	if next >= store.MaxEntityID-idReservationBlock {
		return store.EntityIDExhausted
	}
	return next + idReservationBlock
}

func publicNode(node *store.NodeRecord) Node {
	return Node{
		ID:         node.ID,
		Labels:     slices.Clone(node.Labels),
		Properties: store.ClonePropertyMap(node.Properties),
	}
}

func publicEdge(edge *store.EdgeRecord) Edge {
	return Edge{
		ID:         edge.ID,
		SourceID:   edge.SourceID,
		TargetID:   edge.TargetID,
		Type:       edge.Type,
		Properties: store.ClonePropertyMap(edge.Properties),
	}
}
