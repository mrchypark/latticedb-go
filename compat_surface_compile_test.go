package latticedb

// Keep the public names and method signatures used by the upstream Go
// binding compile-checked without importing its cgo implementation.
var (
	_ func(*DB) (*Tx, error)                                         = (*DB).BeginRead
	_ func(*DB) (*Tx, error)                                         = (*DB).BeginWrite
	_ func(*DB, string) ([]NodeID, error)                            = (*DB).GetNodesByLabel
	_ func(*DB, string, FTSSearchOptions) ([]FTSSearchResult, error) = (*DB).FTSSearchFuzzy
	_ func(*Tx) bool                                                 = (*Tx).IsReadOnly
	_ func(*Tx) bool                                                 = (*Tx).IsActive
	_ func(*Tx, NodeID, NodeID, string) error                        = (*Tx).DeleteEdge
	_ func(*Tx, string, map[string]Value) (QueryResult, error)       = (*Tx).Query
)

var _ = OpenOptions{
	Create:               true,
	ReadOnly:             true,
	EnableWAL:            true,
	DisableWAL:           false,
	EnableAdjacencyCache: true,
	EnableVectors:        true,
	EnableVector:         true,
	DisableLock:          false,
	VectorDimensions:     128,
}

var (
	_ ErrorCode       = ErrorOK
	_ QueryErrorStage = QueryErrorStageExecution
	_ error           = (*Error)(nil)
	_ error           = (*QueryError)(nil)
	_                 = QueryErrorLocation{Line: 1, Column: 1, Length: 1}
)

var compatUpstreamMethods = []any{
	Version, Open, Deserialize,
	(*DB).Serialize, (*DB).Close, (*DB).IsOpen, (*DB).Path, (*DB).Checkpoint,
	(*DB).BeginRead, (*DB).BeginWrite, (*DB).Begin, (*DB).View, (*DB).Update,
	(*DB).Query, (*DB).CacheClear, (*DB).CacheStats,
	(*DB).CreateNodePropertyIndex, (*DB).DropNodePropertyIndex,
	(*DB).CreateEdgePropertyIndex, (*DB).DropEdgePropertyIndex,
	(*DB).GetNodesByLabel, (*DB).VectorSearch, (*DB).FTSSearch,
	(*DB).FTSSearchFuzzy, (*DB).ReadStream, (*DB).GetStreamOffset, (*DB).Changes,
	(*Tx).IsReadOnly, (*Tx).IsActive, (*Tx).Commit, (*Tx).Rollback,
	(*Tx).CreateNode, (*Tx).DeleteNode, (*Tx).NodeExists, (*Tx).GetNode,
	(*Tx).SetProperty, (*Tx).GetProperty, (*Tx).FindNodesByLabelProperty,
	(*Tx).SetVector, (*Tx).BatchInsertVectors, (*Tx).BatchInsert, (*Tx).FTSIndex,
	(*Tx).PublishStream, (*Tx).PublishStreamGetSequence, (*Tx).SetStreamOffset,
	(*Tx).TrimStream, (*Tx).CreateEdge, (*Tx).DeleteEdge,
	(*Tx).SetEdgeProperty, (*Tx).GetEdgeProperty, (*Tx).RemoveEdgeProperty,
	(*Tx).FindEdgesByTypeProperty, (*Tx).GetOutgoingEdges, (*Tx).GetIncomingEdges,
	(*Tx).GetOutgoingEdgesByType, (*Tx).GetIncomingEdgesByType, (*Tx).Query,
}
