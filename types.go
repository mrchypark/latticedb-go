package latticedb

import "github.com/mrchypark/latticedb-go/internal/engine"

type Value = any

type DurabilityMode uint8
type VectorIndexMode uint8

// PersistenceCapabilities reports the persistence primitives implemented by
// this build target. A true value means LatticeDB uses that primitive; it does
// not guarantee physical-media or power-loss durability from the filesystem or
// hardware.
type PersistenceCapabilities struct {
	// FileLocking reports cross-process shared and exclusive database path locks.
	FileLocking bool
	// LinkIdentityProtection reports symbolic-link resolution and detection and
	// rejection of multi-linked regular database files.
	LinkIdentityProtection bool
	// DirectorySync reports directory synchronization after persistence metadata changes.
	DirectorySync bool
	// FullDurability reports whether DurabilityFull has all required persistence primitives on this target.
	FullDurability bool
}

const (
	DurabilityStandard DurabilityMode = iota
	DurabilityFull
)

const (
	// VectorIndexExactOnly is the safe zero-value: no derived index build or memory overhead.
	VectorIndexExactOnly VectorIndexMode = iota
	// VectorIndexHNSWSynchronous builds the approximate index before Open returns.
	VectorIndexHNSWSynchronous
)

type OpenOptions struct {
	Create   bool
	ReadOnly bool
	// CacheSizeMB is reserved for source compatibility. Nonzero values are unsupported and return ErrUnsupportedOption.
	CacheSizeMB uint32
	// PageSize is reserved for source compatibility. Nonzero values are unsupported and return ErrUnsupportedOption.
	PageSize uint32
	// EnableWAL is reserved for compatibility; true is unsupported because WAL is always enabled. Leave false (the default).
	EnableWAL bool
	// DisableWAL requests an unsupported mode because WAL is always enabled. Leave false (the default).
	DisableWAL bool
	// EnableAdjacencyCache is reserved for compatibility; true is unsupported. Leave false (the default).
	EnableAdjacencyCache        bool
	EnableVectors               bool
	EnableVector                bool
	DisableLock                 bool
	VectorIndexMode             VectorIndexMode
	VectorDimensions            uint16
	Durability                  DurabilityMode
	WALCheckpointThresholdBytes uint64
	// ChangefeedMaxBytes bounds retained automatic change records. Zero uses the
	// smaller of 64 MiB and one eighth of MaxDatabaseSnapshotBytes.
	ChangefeedMaxBytes uint64
	// MaxDatabaseSnapshotBytes is a conservative upper bound for the canonical streamed snapshot payload.
	MaxDatabaseSnapshotBytes uint64
	// RecoveryMaxDecodedBytes bounds all checkpoint and WAL bytes decoded during Open. Zero uses 4 GiB.
	RecoveryMaxDecodedBytes uint64
	// RecoveryMaxFrames bounds all complete WAL frames read during Open. Zero uses 1,000,000.
	RecoveryMaxFrames uint64
	// RecoveryMaxWork bounds all replayed snapshot entries and WAL operations during Open. Zero uses 1,000,000,000.
	RecoveryMaxWork uint64
	// VectorIndexBuildMaxWork bounds synchronous HNSW distance work.
	VectorIndexBuildMaxWork uint64
	// VectorIndexBuildMaxLogicalBytes bounds conservative current+new index metadata, not process RSS.
	VectorIndexBuildMaxLogicalBytes uint64
	// DerivedIndexBuildMaxWork bounds label, edge, adjacency, and FTS index rebuild work during Open.
	DerivedIndexBuildMaxWork uint64
	// DerivedIndexBuildMaxLogicalBytes bounds conservative derived posting metadata, not process RSS.
	DerivedIndexBuildMaxLogicalBytes uint64
}

type CreateNodeOptions struct {
	Labels     []string
	Properties map[string]Value
}

type CreateEdgeOptions struct {
	Properties map[string]Value
}

type Node struct {
	ID         uint64
	Labels     []string
	Properties map[string]Value
}

type Edge struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]Value
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]Value
}

type QueryOptions struct {
	MaxRows uint64
	MaxWork uint64
	// MaxBytes limits logical query materialization, not the process RSS.
	MaxBytes uint64
}

type VectorSearchOptions struct {
	K        uint32
	EfSearch uint16
	// Exact disables the approximate index and scans every vector.
	Exact bool
	// MaxWork and MaxBytes bound one direct search request's scalar work and logical scratch/result bytes, not process RSS. Zero MaxWork means no caller-requested work limit; zero MaxBytes uses a 64 MiB logical scratch limit.
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

// StreamReadOptions limits records and their logical size. Zero MaxBytes
// preserves the legacy unbounded byte behavior.
type StreamReadOptions struct {
	Limit    uint
	MaxBytes uint64
}

type StreamReadResult struct {
	Records      []StreamRecord
	LastSequence uint64
	ByteLimited  bool
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

type VectorSearchResult = engine.VectorSearchResult
type FTSSearchResult = engine.FTSSearchResult
type StreamRecord = engine.StreamRecord
