package latticedb

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

// Version returns the Go module version embedded in the calling binary.
func Version() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Path == "github.com/mrchypark/latticedb-go" {
			return info.Main.Version
		}
		for _, dependency := range info.Deps {
			if dependency.Path == "github.com/mrchypark/latticedb-go" {
				return dependency.Version
			}
		}
	}
	return "(devel)"
}

// PlatformPersistenceCapabilities reports the persistence primitives implemented
// by the active build target, independent of per-open settings such as
// DisableLock. It does not guarantee physical-media durability.
func PlatformPersistenceCapabilities() PersistenceCapabilities {
	return platformPersistenceCapabilities()
}

var (
	ErrReadOnly                       = engine.ErrReadOnly
	ErrWriteTxActive                  = engine.ErrWriteTxActive
	ErrManagedTransaction             = engine.ErrManagedTransaction
	ErrInactiveTx                     = engine.ErrInactiveTx
	ErrDatabaseLocked                 = engine.ErrDatabaseLocked
	ErrDatabaseLayoutConflict         = engine.ErrDatabaseLayoutConflict
	ErrDatabaseClosed                 = engine.ErrDatabaseClosed
	ErrTransactionsActive             = engine.ErrTransactionsActive
	ErrSnapshotActive                 = engine.ErrSnapshotActive
	ErrWriteConflict                  = engine.ErrWriteConflict
	ErrRecoveryRequired               = engine.ErrRecoveryRequired
	ErrResourceLimit                  = engine.ErrResourceLimit
	ErrAlreadyExists                  = engine.ErrAlreadyExists
	ErrInvalidArgument                = engine.ErrInvalidArgument
	ErrVectorIndexMaintenanceRequired = engine.ErrVectorIndexMaintenanceRequired
	ErrUnsupportedOption              = engine.ErrUnsupportedOption
	ErrCommitOutcomeUnknown           = engine.ErrCommitOutcomeUnknown
)

type DB struct {
	inner *engine.DB
	path  string
}

func (db *DB) requireOpen() (*engine.DB, error) {
	if db == nil || db.inner == nil || !db.inner.IsOpen() {
		return nil, ErrDatabaseClosed
	}
	return db.inner, nil
}

// Tx is a single-owner transaction handle. Its methods must not be called
// concurrently; use separate read transactions for concurrent work.
type Tx struct {
	inner *engine.Tx
}

// Snapshot is one fixed committed database generation.
type Snapshot struct {
	inner *engine.Snapshot
}

// Open opens a directory-backed database or a regular database file previously
// returned by Serialize.
func Open(path string, opts OpenOptions) (*DB, error) {
	return OpenContext(context.Background(), path, opts)
}

func OpenContext(ctx context.Context, path string, opts OpenOptions) (*DB, error) {
	if err := validateOpenOptions(opts); err != nil {
		return nil, wrapError(err)
	}
	inner, err := engine.OpenContext(ctx, path, engine.OpenOptions{
		Create:                            opts.Create,
		ReadOnly:                          opts.ReadOnly,
		DisableLock:                       opts.DisableLock,
		CacheSizeMB:                       opts.CacheSizeMB,
		PageSize:                          opts.PageSize,
		EnableVector:                      opts.EnableVector || opts.EnableVectors,
		VectorIndexMode:                   engine.VectorIndexMode(opts.VectorIndexMode),
		VectorDimensions:                  opts.VectorDimensions,
		Durability:                        engine.DurabilityMode(opts.Durability),
		WALCheckpointThresholdBytes:       opts.WALCheckpointThresholdBytes,
		ChangefeedMaxBytes:                opts.ChangefeedMaxBytes,
		MaxDatabaseSnapshotBytes:          opts.MaxDatabaseSnapshotBytes,
		RecoveryMaxDecodedBytes:           opts.RecoveryMaxDecodedBytes,
		RecoveryMaxFrames:                 opts.RecoveryMaxFrames,
		RecoveryMaxWork:                   opts.RecoveryMaxWork,
		VectorIndexBuildMaxWork:           opts.VectorIndexBuildMaxWork,
		VectorIndexBuildMaxLogicalBytes:   opts.VectorIndexBuildMaxLogicalBytes,
		DerivedIndexBuildMaxWork:          opts.DerivedIndexBuildMaxWork,
		DerivedIndexBuildMaxLogicalBytes:  opts.DerivedIndexBuildMaxLogicalBytes,
		MaxGenerationLeases:               opts.MaxGenerationLeases,
		MaxRetainedGenerationLogicalBytes: opts.MaxRetainedGenerationLogicalBytes,
	})
	if err != nil {
		return nil, wrapError(err)
	}
	return &DB{inner: inner, path: path}, nil
}

// Deserialize opens a database from bytes returned by Serialize.
func Deserialize(data []byte, opts OpenOptions) (*DB, error) {
	if err := validateOpenOptions(opts); err != nil {
		return nil, wrapError(err)
	}
	inner, err := engine.Deserialize(data, engine.OpenOptions{
		ReadOnly:                          opts.ReadOnly,
		CacheSizeMB:                       opts.CacheSizeMB,
		PageSize:                          opts.PageSize,
		EnableVector:                      opts.EnableVector || opts.EnableVectors,
		VectorIndexMode:                   engine.VectorIndexMode(opts.VectorIndexMode),
		VectorDimensions:                  opts.VectorDimensions,
		Durability:                        engine.DurabilityMode(opts.Durability),
		WALCheckpointThresholdBytes:       opts.WALCheckpointThresholdBytes,
		ChangefeedMaxBytes:                opts.ChangefeedMaxBytes,
		MaxDatabaseSnapshotBytes:          opts.MaxDatabaseSnapshotBytes,
		RecoveryMaxDecodedBytes:           opts.RecoveryMaxDecodedBytes,
		RecoveryMaxFrames:                 opts.RecoveryMaxFrames,
		RecoveryMaxWork:                   opts.RecoveryMaxWork,
		VectorIndexBuildMaxWork:           opts.VectorIndexBuildMaxWork,
		VectorIndexBuildMaxLogicalBytes:   opts.VectorIndexBuildMaxLogicalBytes,
		DerivedIndexBuildMaxWork:          opts.DerivedIndexBuildMaxWork,
		DerivedIndexBuildMaxLogicalBytes:  opts.DerivedIndexBuildMaxLogicalBytes,
		MaxGenerationLeases:               opts.MaxGenerationLeases,
		MaxRetainedGenerationLogicalBytes: opts.MaxRetainedGenerationLogicalBytes,
	})
	if err != nil {
		return nil, wrapError(err)
	}
	return &DB{inner: inner, path: "<deserialized>"}, nil
}

func validateOpenOptions(opts OpenOptions) error {
	if opts.CacheSizeMB != 0 {
		return fmt.Errorf("%w: CacheSizeMB", ErrUnsupportedOption)
	}
	if opts.PageSize != 0 {
		return fmt.Errorf("%w: PageSize", ErrUnsupportedOption)
	}
	if opts.DisableWAL {
		return fmt.Errorf("%w: disabling WAL is unavailable", ErrUnsupportedOption)
	}
	if opts.EnableWAL {
		return fmt.Errorf("%w: EnableWAL", ErrUnsupportedOption)
	}
	if opts.EnableAdjacencyCache {
		return fmt.Errorf("%w: EnableAdjacencyCache", ErrUnsupportedOption)
	}
	return nil
}

func (db *DB) Close() error {
	if db == nil || db.inner == nil {
		return nil
	}
	return wrapError(db.inner.Close())
}

// CloseContext waits for the writer slot until ctx is canceled. Once closing
// starts, it completes teardown so the database is either open or closed.
func (db *DB) CloseContext(ctx context.Context) error {
	if db == nil || db.inner == nil {
		return nil
	}
	return wrapError(db.inner.CloseContext(ctx))
}

func (db *DB) IsOpen() bool {
	return db != nil && db.inner != nil && db.inner.IsOpen()
}

func (db *DB) Path() string {
	if db == nil {
		return ""
	}
	return db.path
}

// Serialize returns a standalone database file. Writing the bytes to a regular
// file produces a path that Open can read and update.
func (db *DB) Serialize() ([]byte, error) {
	if db == nil || db.inner == nil {
		return nil, wrapError(ErrDatabaseClosed)
	}
	data, err := db.inner.Serialize()
	return data, wrapError(err)
}

func (db *DB) Checkpoint() error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.Checkpoint())
}

// CheckpointContext waits for the writer slot until ctx is canceled. Once
// checkpoint publication starts, it completes without observing cancellation.
func (db *DB) CheckpointContext(ctx context.Context) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CheckpointContext(ctx))
}

// GenerationRetentionStats reports logical immutable-generation pins. It does
// not report process RSS or force reclamation of active leases.
func (db *DB) GenerationRetentionStats() (GenerationRetentionStats, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return GenerationRetentionStats{}, wrapError(err)
	}
	stats, err := inner.GenerationRetentionStats()
	if err != nil {
		return GenerationRetentionStats{}, wrapError(err)
	}
	return GenerationRetentionStats{
		ActiveLeases:         stats.ActiveLeases,
		ActiveSnapshotLeases: stats.ActiveSnapshotLeases,
		RetainedGenerations:  stats.RetainedGenerations,
		RetainedLogicalBytes: stats.RetainedLogicalBytes,
		OldestLeaseAge:       stats.OldestLeaseAge,
	}, nil
}

// BeginSnapshot pins one committed generation while database writes continue.
// Multiple snapshots may be active at once. Close releases the pin.
func (db *DB) BeginSnapshot() (*Snapshot, error) {
	if db == nil || db.inner == nil {
		return nil, wrapError(ErrDatabaseClosed)
	}
	snapshot, err := db.inner.BeginSnapshot()
	if err != nil {
		return nil, wrapError(err)
	}
	return &Snapshot{inner: snapshot}, nil
}

// Backup writes the frozen generation as a standalone regular database file.
func (snapshot *Snapshot) Backup(path string) error {
	if snapshot == nil || snapshot.inner == nil {
		return ErrDatabaseClosed
	}
	return wrapError(snapshot.inner.Backup(path))
}

// Close releases the frozen generation. Close is idempotent.
func (snapshot *Snapshot) Close() error {
	if snapshot == nil || snapshot.inner == nil {
		return nil
	}
	if err := snapshot.inner.Close(); err != nil {
		return wrapError(err)
	}
	snapshot.inner = nil
	return nil
}

func (db *DB) Begin(readOnly bool) (*Tx, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	tx, err := inner.Begin(readOnly)
	if err != nil {
		return nil, wrapError(err)
	}
	return &Tx{inner: tx}, nil
}

// BeginWriteContext waits for the single writer slot until ctx is canceled.
func (db *DB) BeginWriteContext(ctx context.Context) (*Tx, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	tx, err := inner.BeginWriteContext(ctx)
	if err != nil {
		return nil, wrapError(err)
	}
	return &Tx{inner: tx}, nil
}

func (db *DB) View(fn func(*Tx) error) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	var callbackErr error
	err = inner.View(func(tx *engine.Tx) error {
		callbackErr = fn(&Tx{inner: tx})
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return wrapError(err)
}

func (db *DB) Update(fn func(*Tx) error) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	var callbackErr error
	err = inner.Update(func(tx *engine.Tx) error {
		callbackErr = fn(&Tx{inner: tx})
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return wrapError(err)
}

func (db *DB) UpdateContext(ctx context.Context, fn func(*Tx) error) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	var callbackErr error
	err = inner.UpdateContext(ctx, func(tx *engine.Tx) error {
		callbackErr = fn(&Tx{inner: tx})
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return wrapError(err)
}

func (db *DB) Query(query string, params map[string]Value) (QueryResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return QueryResult{}, wrapError(err)
	}
	result, err := inner.Query(query, params)
	if err != nil {
		return QueryResult{}, wrapError(err)
	}
	return convertQueryResult(result), nil
}

func (db *DB) QueryContext(ctx context.Context, query string, params map[string]Value, opts QueryOptions) (QueryResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return QueryResult{}, wrapError(err)
	}
	result, err := inner.QueryContext(ctx, query, params, engine.QueryOptions{MaxRows: opts.MaxRows, MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes})
	if err != nil {
		return QueryResult{}, wrapError(err)
	}
	return convertQueryResult(result), nil
}

func (db *DB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	results, err := inner.VectorSearch(vector, engine.VectorSearchOptions{
		K:        opts.K,
		EfSearch: opts.EfSearch,
		Exact:    opts.Exact,
		MaxWork:  opts.MaxWork,
		MaxBytes: opts.MaxBytes,
	})
	if err != nil {
		return nil, wrapError(err)
	}
	return results, nil
}

func (db *DB) VectorSearchContext(ctx context.Context, vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	results, err := inner.VectorSearchContext(ctx, vector, engine.VectorSearchOptions{K: opts.K, EfSearch: opts.EfSearch, Exact: opts.Exact, MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes})
	if err != nil {
		return nil, wrapError(err)
	}
	return results, nil
}

func (db *DB) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	results, err := inner.FTSSearch(query, engine.FTSSearchOptions{
		Limit:         opts.Limit,
		MaxDistance:   opts.MaxDistance,
		MinTermLength: opts.MinTermLength,
		MaxWork:       opts.MaxWork,
		MaxBytes:      opts.MaxBytes,
	})
	if err != nil {
		return nil, wrapError(err)
	}
	return results, nil
}

func (db *DB) FTSSearchContext(ctx context.Context, query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	results, err := inner.FTSSearchContext(ctx, query, engine.FTSSearchOptions{Limit: opts.Limit, MaxDistance: opts.MaxDistance, MinTermLength: opts.MinTermLength, MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes})
	if err != nil {
		return nil, wrapError(err)
	}
	return results, nil
}

func (db *DB) ReadStream(stream string, afterSequence uint64, limit uint, timeoutMS uint32) ([]StreamRecord, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	records, err := inner.ReadStream(stream, afterSequence, limit, timeoutMS)
	return records, wrapError(err)
}

// ReadStreamContext reads stream records until a record is available, the byte
// budget is reached, or ctx is canceled. A zero MaxBytes disables the byte limit.
func (db *DB) ReadStreamContext(ctx context.Context, stream string, afterSequence uint64, opts StreamReadOptions) (StreamReadResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return StreamReadResult{}, wrapError(err)
	}
	result, err := inner.ReadStreamContext(ctx, stream, afterSequence, engine.StreamReadOptions{Limit: opts.Limit, MaxBytes: opts.MaxBytes})
	return StreamReadResult{Records: result.Records, LastSequence: result.LastSequence, ByteLimited: result.ByteLimited}, wrapError(err)
}

func (db *DB) GetStreamOffset(stream, consumer string) (uint64, bool, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return 0, false, wrapError(err)
	}
	offset, ok, err := inner.GetStreamOffset(stream, consumer)
	return offset, ok, wrapError(err)
}

func (db *DB) Changes(afterSequence uint64, limit uint, timeoutMS uint32) ([]StreamRecord, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	records, err := inner.Changes(afterSequence, limit, timeoutMS)
	return records, wrapError(err)
}

// ChangesContext is ReadStreamContext for the automatic changefeed.
func (db *DB) ChangesContext(ctx context.Context, afterSequence uint64, opts StreamReadOptions) (StreamReadResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return StreamReadResult{}, wrapError(err)
	}
	result, err := inner.ChangesContext(ctx, afterSequence, engine.StreamReadOptions{Limit: opts.Limit, MaxBytes: opts.MaxBytes})
	return StreamReadResult{Records: result.Records, LastSequence: result.LastSequence, ByteLimited: result.ByteLimited}, wrapError(err)
}

func (db *DB) CacheClear() error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CacheClear())
}

func (db *DB) CacheStats() (QueryCacheStats, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return QueryCacheStats{}, wrapError(err)
	}
	stats, err := inner.CacheStats()
	if err != nil {
		return QueryCacheStats{}, wrapError(err)
	}
	return QueryCacheStats{
		Entries: stats.Entries,
		Hits:    stats.Hits,
		Misses:  stats.Misses,
	}, nil
}

func (db *DB) CreateNodePropertyIndex(label, property string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CreateNodePropertyIndex(label, property))
}

func (db *DB) CreateNodePropertyIndexContext(ctx context.Context, label, property string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CreateNodePropertyIndexContext(ctx, label, property))
}

func (db *DB) DropNodePropertyIndex(label, property string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.DropNodePropertyIndex(label, property))
}

func (db *DB) CreateEdgePropertyIndex(edgeType, property string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CreateEdgePropertyIndex(edgeType, property))
}

func (db *DB) CreateEdgePropertyIndexContext(ctx context.Context, edgeType, property string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CreateEdgePropertyIndexContext(ctx, edgeType, property))
}

func (db *DB) DropEdgePropertyIndex(edgeType, property string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.DropEdgePropertyIndex(edgeType, property))
}

func (db *DB) VectorIndexStats() (VectorIndexStats, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return VectorIndexStats{}, wrapError(err)
	}
	stats, err := inner.VectorIndexStats()
	if err != nil {
		return VectorIndexStats{}, wrapError(err)
	}
	return VectorIndexStats{
		LiveEntries:                stats.LiveEntries,
		IndexEntries:               stats.IndexEntries,
		Tombstones:                 stats.Tombstones,
		TombstoneBytes:             stats.TombstoneBytes,
		TombstoneBytesUntilRebuild: stats.TombstoneBytesUntilRebuild,
		MutationDebt:               stats.MutationDebt,
		RebuildThreshold:           stats.RebuildThreshold,
		DebtUntilRebuild:           stats.DebtUntilRebuild,
		EstimatedBuildLogicalBytes: stats.EstimatedBuildLogicalBytes,
		ExactFallbacks:             stats.ExactFallbacks,
		Rebuilds:                   stats.Rebuilds,
		RebuildNanoseconds:         stats.RebuildNanoseconds,
	}, nil
}

func (db *DB) RebuildVectorIndexContext(ctx context.Context) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.RebuildVectorIndexContext(ctx))
}

// Commit makes the transaction inactive, whether it succeeds or fails.
func (tx *Tx) Commit() error {
	if tx == nil || tx.inner == nil {
		return ErrInactiveTx
	}
	return wrapError(tx.inner.Commit())
}

// CommitContext makes the transaction inactive, whether it succeeds or fails.
func (tx *Tx) CommitContext(ctx context.Context) error {
	if tx == nil || tx.inner == nil {
		return ErrInactiveTx
	}
	return wrapError(tx.inner.CommitContext(ctx))
}

func (tx *Tx) Rollback() error {
	if tx == nil || tx.inner == nil {
		return nil
	}
	if !tx.inner.IsActive() {
		return nil
	}
	return wrapError(tx.inner.Rollback())
}

func (tx *Tx) CreateNode(opts CreateNodeOptions) (Node, error) {
	node, err := tx.inner.CreateNode(engine.CreateNodeOptions{
		Labels:     opts.Labels,
		Properties: opts.Properties,
	})
	if err != nil {
		return Node{}, wrapError(err)
	}
	return convertNode(node), nil
}

func (tx *Tx) DeleteNode(nodeID uint64) error {
	return wrapError(tx.inner.DeleteNode(nodeID))
}

func (tx *Tx) NodeExists(nodeID uint64) (bool, error) {
	exists, err := tx.inner.NodeExists(nodeID)
	return exists, wrapError(err)
}

func (tx *Tx) GetNode(nodeID uint64) (*Node, error) {
	node, ok, err := tx.inner.GetNodeValue(nodeID)
	if err != nil || !ok {
		return nil, wrapError(err)
	}
	converted := convertNode(node)
	return &converted, nil
}

func (tx *Tx) SetProperty(nodeID uint64, key string, value Value) error {
	return wrapError(tx.inner.SetProperty(nodeID, key, value))
}

func (tx *Tx) GetProperty(nodeID uint64, key string) (Value, bool, error) {
	value, ok, err := tx.inner.GetProperty(nodeID, key)
	return value, ok, wrapError(err)
}

func (tx *Tx) FindNodesByLabelProperty(label, property string, value Value, limit uint) ([]uint64, error) {
	ids, err := tx.inner.FindNodesByLabelProperty(label, property, value, limit)
	return ids, wrapError(err)
}

func (tx *Tx) SetVector(nodeID uint64, key string, vector []float32) error {
	return wrapError(tx.inner.SetVector(nodeID, key, vector))
}

// BatchInsertVectors inserts multiple vector-bearing nodes in a single call.
func (tx *Tx) BatchInsertVectors(label string, vectors [][]float32) ([]uint64, error) {
	ids, err := tx.inner.BatchInsertVectors(label, vectors)
	return ids, wrapError(err)
}

// Deprecated: use BatchInsertVectors. Earliest removal is v0.6.0.
func (tx *Tx) BatchInsert(label string, vectors [][]float32) ([]uint64, error) {
	return tx.BatchInsertVectors(label, vectors)
}

func (tx *Tx) FTSIndex(nodeID uint64, text string) error {
	return wrapError(tx.inner.FTSIndex(nodeID, text))
}

func (tx *Tx) FTSIndexContext(ctx context.Context, nodeID uint64, text string) error {
	return wrapError(tx.inner.FTSIndexContext(ctx, nodeID, text))
}

func (tx *Tx) PublishStream(stream, kind string, payload Value) error {
	return wrapError(tx.inner.PublishStream(stream, kind, payload))
}

func (tx *Tx) PublishStreamGetSequence(stream, kind string, payload Value) (uint64, error) {
	sequence, err := tx.inner.PublishStreamGetSequence(stream, kind, payload)
	return sequence, wrapError(err)
}

func (tx *Tx) SetStreamOffset(stream, consumer string, sequence uint64) error {
	return wrapError(tx.inner.SetStreamOffset(stream, consumer, sequence))
}

func (tx *Tx) TrimStream(stream string, beforeSequence uint64) error {
	return wrapError(tx.inner.TrimStream(stream, beforeSequence))
}

func (tx *Tx) CreateEdge(sourceID uint64, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	edge, err := tx.inner.CreateEdge(sourceID, targetID, edgeType, engine.CreateEdgeOptions{
		Properties: opts.Properties,
	})
	if err != nil {
		return Edge{}, wrapError(err)
	}
	return convertEdge(edge), nil
}

func (tx *Tx) GetEdgeProperty(edgeID uint64, key string) (Value, bool, error) {
	value, ok, err := tx.inner.GetEdgeProperty(edgeID, key)
	return value, ok, wrapError(err)
}

func (tx *Tx) FindEdgesByTypeProperty(edgeType, property string, value Value, limit uint) ([]uint64, error) {
	ids, err := tx.inner.FindEdgesByTypeProperty(edgeType, property, value, limit)
	return ids, wrapError(err)
}

func (tx *Tx) SetEdgeProperty(edgeID uint64, key string, value Value) error {
	return wrapError(tx.inner.SetEdgeProperty(edgeID, key, value))
}

func (tx *Tx) RemoveEdgeProperty(edgeID uint64, key string) error {
	return wrapError(tx.inner.RemoveEdgeProperty(edgeID, key))
}

func (tx *Tx) GetOutgoingEdges(nodeID uint64) ([]Edge, error) {
	edges, err := tx.inner.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, wrapError(err)
	}
	out := make([]Edge, len(edges))
	for i, edge := range edges {
		out[i] = convertEdge(edge)
	}
	return out, nil
}

func (tx *Tx) GetIncomingEdges(nodeID uint64) ([]Edge, error) {
	edges, err := tx.inner.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, wrapError(err)
	}
	out := make([]Edge, len(edges))
	for i, edge := range edges {
		out[i] = convertEdge(edge)
	}
	return out, nil
}

func (tx *Tx) GetOutgoingEdgesByType(nodeID uint64, edgeType string, limit uint) ([]Edge, error) {
	edges, err := tx.inner.GetOutgoingEdgesByType(nodeID, edgeType, limit)
	if err != nil {
		return nil, wrapError(err)
	}
	out := make([]Edge, len(edges))
	for i, edge := range edges {
		out[i] = convertEdge(edge)
	}
	return out, nil
}

func (tx *Tx) GetIncomingEdgesByType(nodeID uint64, edgeType string, limit uint) ([]Edge, error) {
	edges, err := tx.inner.GetIncomingEdgesByType(nodeID, edgeType, limit)
	if err != nil {
		return nil, wrapError(err)
	}
	out := make([]Edge, len(edges))
	for i, edge := range edges {
		out[i] = convertEdge(edge)
	}
	return out, nil
}

func convertNode(node engine.Node) Node {
	return Node{
		ID:         node.ID,
		Labels:     node.Labels,
		Properties: node.Properties,
	}
}

func convertEdge(edge engine.Edge) Edge {
	return Edge{
		ID:         edge.ID,
		SourceID:   edge.SourceID,
		TargetID:   edge.TargetID,
		Type:       edge.Type,
		Properties: edge.Properties,
	}
}

func convertQueryResult(result engine.QueryResult) QueryResult {
	return QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}
}
