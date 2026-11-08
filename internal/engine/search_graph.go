package engine

import (
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"math/bits"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/store"
)

type SearchGraphFTS struct {
	Text     string
	Scoring  FTSScoring
	Analyzer FTSAnalyzer
}

type SearchGraphVector struct {
	Embedding   []float32
	Namespace   *VectorNamespace
	Approximate bool
	EfSearch    uint16
}

type SearchGraphExpansion struct {
	EdgeType    string
	TargetLabel string
}

type SearchGraphOptions struct {
	FTS                        *SearchGraphFTS
	Vector                     *SearchGraphVector
	Expand                     *SearchGraphExpansion
	CandidateK, ResultK        uint32
	MaxRows, MaxWork, MaxBytes uint64
}

type SearchGraphMetric string

const (
	SearchGraphFrequency SearchGraphMetric = "fts_frequency"
	SearchGraphBM25      SearchGraphMetric = "fts_bm25"
	SearchGraphL2        SearchGraphMetric = "vector_l2"
)

type SearchGraphRankDirection string

const (
	SearchGraphHigherFirst SearchGraphRankDirection = "higher_first"
	SearchGraphLowerFirst  SearchGraphRankDirection = "lower_first"
)

type SearchGraphRow struct {
	NodeID, TargetID, EdgeID uint64
	Value                    float32
}

type SearchGraphResult struct {
	DatabaseID string
	CommitID   uint64
	Metric     SearchGraphMetric
	Direction  SearchGraphRankDirection
	Exact      bool
	Analyzer   FTSAnalyzer
	Namespace  *VectorNamespace
	Rows       []SearchGraphRow
}

func (db *DB) SearchGraphContext(ctx context.Context, opts SearchGraphOptions) (SearchGraphResult, error) {
	var result SearchGraphResult
	err := db.View(func(tx *Tx) error {
		var err error
		result, err = tx.SearchGraphContext(ctx, opts)
		return err
	})
	return result, err
}

func (tx *Tx) SearchGraphContext(ctx context.Context, opts SearchGraphOptions) (SearchGraphResult, error) {
	if tx == nil || tx.closed {
		return SearchGraphResult{}, ErrInactiveTx
	}
	if !tx.readOnly {
		return SearchGraphResult{}, fmt.Errorf("%w: search composition requires a read-only transaction", ErrUnsupportedOption)
	}
	if (opts.FTS == nil) == (opts.Vector == nil) || opts.CandidateK == 0 || opts.ResultK == 0 {
		return SearchGraphResult{}, fmt.Errorf("%w: supply one search source and nonzero CandidateK and ResultK", ErrInvalidArgument)
	}
	if opts.ResultK > maxSearchResults {
		return SearchGraphResult{}, fmt.Errorf("%w: graph result limit exceeds %d", ErrResourceLimit, maxSearchResults)
	}
	budget, err := newDirectSearchBudget(ctx, opts.MaxWork, opts.MaxBytes, opts.CandidateK)
	if err != nil {
		return SearchGraphResult{}, err
	}
	if opts.Expand != nil {
		if err := budget.add(uint64(len(opts.Expand.EdgeType)) + uint64(len(opts.Expand.TargetLabel))); err != nil {
			return SearchGraphResult{}, err
		}
		if err := store.ValidateEdgeType(opts.Expand.EdgeType); err != nil {
			return SearchGraphResult{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
		}
		if opts.Expand.TargetLabel != "" {
			if err := store.ValidateCreateLabels([]string{opts.Expand.TargetLabel}); err != nil {
				return SearchGraphResult{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
			}
		}
	}
	ctx = store.WithPageReadBudget(budget.ctx, budget)
	remaining := opts.MaxRows
	if remaining == 0 {
		remaining = maxSearchResults
	}
	chargeRows := func(count uint64) error {
		if count > remaining {
			return fmt.Errorf("%w: search graph row budget exceeded", ErrResourceLimit)
		}
		remaining -= count
		return nil
	}
	result := SearchGraphResult{
		DatabaseID: tx.graph.DatabaseID,
		CommitID:   tx.commitID,
		Exact:      true,
	}
	limit := int(opts.ResultK)
	if opts.Expand == nil {
		limit = min(limit, int(opts.CandidateK))
	}
	if err := budget.reserveBytes(uint64(limit) * 32); err != nil {
		return SearchGraphResult{}, err
	}
	result.Rows = make([]SearchGraphRow, 0, limit)
	emit := func(id uint64, value float32) error {
		if err := budget.check(); err != nil {
			return err
		}
		if err := budget.add(1); err != nil {
			return err
		}
		if opts.Expand == nil {
			if err := chargeRows(1); err != nil {
				return err
			}
			result.Rows = append(result.Rows, SearchGraphRow{NodeID: id, Value: value})
			return nil
		}
		start := len(result.Rows)
		if err := appendSearchGraphPaths(ctx, tx.graph, id, value, *opts.Expand, limit, &result.Rows, chargeRows, budget); err != nil {
			return err
		}
		return chargeRows(uint64(len(result.Rows) - start))
	}

	if opts.FTS != nil {
		ftsOpts := FTSSearchOptions{Limit: opts.CandidateK, Scoring: opts.FTS.Scoring, Analyzer: opts.FTS.Analyzer}
		if err := validateFTSSearchScoring(ftsOpts); err != nil {
			return SearchGraphResult{}, err
		}
		matches, err := tx.db.ftsSearchGraph(ctx, tx.graph, opts.FTS.Text, ftsOpts, budget)
		if err != nil {
			return SearchGraphResult{}, pageStorageOpenError(err)
		}
		result.Metric = SearchGraphFrequency
		if opts.FTS.Scoring == FTSScoringBM25 {
			result.Metric = SearchGraphBM25
		}
		result.Analyzer = opts.FTS.Analyzer
		result.Direction = SearchGraphHigherFirst
		if err := chargeRows(uint64(len(matches))); err != nil {
			return SearchGraphResult{}, err
		}
		for _, match := range matches {
			if len(result.Rows) == limit {
				break
			}
			if err := emit(match.NodeID, match.Score); err != nil {
				return SearchGraphResult{}, pageStorageOpenError(err)
			}
		}
	} else {
		if !tx.db.enableVector || tx.graph.VectorDimensions == 0 {
			return SearchGraphResult{}, fmt.Errorf("%w: vector search is disabled", ErrUnsupportedOption)
		}
		result.Namespace, err = resolveVectorNamespace(tx.graph, opts.Vector.Namespace)
		if err != nil {
			return SearchGraphResult{}, err
		}
		graph := tx.graph
		if result.Namespace != nil {
			graph = vectorNamespaceFacade(graph, *result.Namespace)
		}
		if err := budget.add(uint64(len(opts.Vector.Embedding))); err != nil {
			return SearchGraphResult{}, err
		}
		matches, fallback, err := searchVectorGraph(graph, opts.Vector.Embedding, VectorSearchOptions{
			K: opts.CandidateK, Exact: !opts.Vector.Approximate, EfSearch: opts.Vector.EfSearch,
		}, budget, tx.db.disableVectorIndex)
		if err != nil {
			return SearchGraphResult{}, pageStorageOpenError(err)
		}
		if fallback {
			tx.db.vectorExactFallbacks.Add(1)
		}
		result.Metric, result.Direction = SearchGraphL2, SearchGraphLowerFirst
		result.Exact = !opts.Vector.Approximate || tx.db.disableVectorIndex || fallback ||
			(graph.PageBase == nil && graph.VectorIndex.Nodes.Len() == 0)
		if err := chargeRows(uint64(len(matches))); err != nil {
			return SearchGraphResult{}, err
		}
		for _, match := range matches {
			if len(result.Rows) == limit {
				break
			}
			if err := emit(match.NodeID, match.Distance); err != nil {
				return SearchGraphResult{}, pageStorageOpenError(err)
			}
		}
	}
	if err := budget.check(); err != nil {
		return SearchGraphResult{}, err
	}
	return result, nil
}

func appendSearchGraphPaths(ctx context.Context, graph *store.GraphState, id uint64, value float32, expansion SearchGraphExpansion, limit int, rows *[]SearchGraphRow, chargeRows func(uint64) error, budget *directSearchBudget) error {
	start := len(*rows)
	take := limit - start
	log := uint64(bits.Len(uint(take)))
	var pathsHeap searchGraphPathHeap
	visitOutgoing := graph.VisitOutgoing
	if page := graph.PageBase; page != nil {
		if graph.Edges.Len() != 0 || graph.DeletedEdges.Len() != 0 {
			return fmt.Errorf("%w: search composition requires committed page adjacency", ErrUnsupportedOption)
		}
		// The cursor holds the previous and current fixed-width adjacency keys.
		const adjacencyScratch = 64
		if err := budget.reserveBytes(adjacencyScratch); err != nil {
			return err
		}
		defer budget.releaseBytes(adjacencyScratch)
		visitOutgoing = page.VisitOutgoing
	}
	err := visitOutgoing(ctx, id, func(edgeID uint64) error {
		if err := chargeRows(1); err != nil {
			return err
		}
		if err := budget.add(1); err != nil {
			return err
		}
		edge, lease, err := graph.ReadEdgeOwned(ctx, edgeID)
		if lease != nil {
			defer lease.Release()
		}
		if err != nil || edge == nil || edge.Type != expansion.EdgeType {
			return err
		}
		if expansion.TargetLabel != "" {
			target, lease, err := graph.ReadNodeOwned(ctx, edge.TargetID)
			if lease != nil {
				defer lease.Release()
			}
			if err != nil || target == nil {
				return err
			}
			if err := budget.add(uint64(len(target.Labels))); err != nil {
				return err
			}
			if !slices.Contains(target.Labels, expansion.TargetLabel) {
				return nil
			}
		}
		row := SearchGraphRow{NodeID: id, TargetID: edge.TargetID, EdgeID: edgeID, Value: value}
		if len(*rows) < limit {
			*rows = append(*rows, row)
			if len(*rows) == limit {
				if err := budget.add(uint64(take) * 2); err != nil {
					return err
				}
				pathsHeap.rows = (*rows)[start:]
				heap.Init(&pathsHeap)
			}
			return nil
		}
		paths := (*rows)[start:]
		if err := budget.add(1); err != nil {
			return err
		}
		if compareSearchGraphPath(row, paths[0]) < 0 {
			if err := budget.add(log * 2); err != nil {
				return err
			}
			paths[0] = row
			heap.Fix(&pathsHeap, 0)
		}
		return nil
	})
	if err != nil {
		return err
	}
	paths := (*rows)[start:]
	if err := budget.add(uint64(len(paths)) * log); err != nil {
		return err
	}
	slices.SortFunc(paths, compareSearchGraphPath)
	return budget.check()
}

func compareSearchGraphPath(a, b SearchGraphRow) int {
	if order := cmp.Compare(a.TargetID, b.TargetID); order != 0 {
		return order
	}
	return cmp.Compare(a.EdgeID, b.EdgeID)
}

type searchGraphPathHeap struct{ rows []SearchGraphRow }

func (h *searchGraphPathHeap) Len() int { return len(h.rows) }
func (h *searchGraphPathHeap) Less(i, j int) bool {
	return compareSearchGraphPath(h.rows[i], h.rows[j]) > 0
}
func (h *searchGraphPathHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *searchGraphPathHeap) Push(row any)  { h.rows = append(h.rows, row.(SearchGraphRow)) }
func (h *searchGraphPathHeap) Pop() any {
	row := h.rows[len(h.rows)-1]
	h.rows = h.rows[:len(h.rows)-1]
	return row
}
