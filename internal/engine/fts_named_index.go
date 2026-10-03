package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/latticedb-go/internal/search"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// FTSIndexSearchResult identifies a node or edge according to the named index definition.
type FTSIndexSearchResult struct {
	EntityID uint64
	Score    float32
}

func (db *DB) FTSSearchIndex(name, query string, opts FTSSearchOptions) ([]FTSIndexSearchResult, error) {
	return db.FTSSearchIndexContext(context.Background(), name, query, opts)
}

// FTSSearchIndexContext searches the text property selected by a named scoped definition.
// Declared indexes maintain standard-token postings; Porter analysis uses the bounded scan path.
func (db *DB) FTSSearchIndexContext(ctx context.Context, name, query string, opts FTSSearchOptions) ([]FTSIndexSearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateFTSSearchScoring(opts); err != nil {
		return nil, err
	}
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
	var results []FTSIndexSearchResult
	err = db.View(func(tx *Tx) error {
		var def FTSIndexDefinition
		var found bool
		defs, err := ftsIndexDefinitions(tx.graph)
		if err != nil {
			return err
		}
		for _, candidate := range defs {
			if candidate.Name == name {
				def, found = candidate, true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: FTS index %q not found", ErrInvalidArgument, name)
		}
		terms, termBytes, err := ftsAnalyzeQuery(ctx, query, opts.Analyzer, budget)
		if err != nil {
			return err
		}
		defer budget.releaseBytes(termBytes)
		if opts.Scoring == FTSScoringBM25 {
			terms, err = uniqueFTSTerms(terms, budget)
			if err != nil {
				return err
			}
		}
		if len(terms) == 0 {
			results = []FTSIndexSearchResult{}
			return nil
		}
		page := tx.graph.PageBase
		physical := declaredFTSPageIndexName(name)
		ready := false
		if page != nil && page.SearchIndexesCurrent && opts.Analyzer == FTSAnalyzerStandard {
			ready, err = page.FTSIndexReady(physical)
			if err != nil {
				return err
			}
		}
		if ready {
			stats, e := page.FTSIndexStats(physical)
			if e != nil {
				return e
			}
			postingTerms, held, e := pageFTSPostingTerms(ctx, page, physical, terms, opts, budget)
			if e != nil {
				return e
			}
			defer budget.releaseBytes(held)
			indexed, e := collectPageFTSResults(ctx, page, physical, terms, postingTerms, opts, stats, budget)
			if e != nil {
				return e
			}
			results = make([]FTSIndexSearchResult, len(indexed))
			for i, r := range indexed {
				results[i] = FTSIndexSearchResult{EntityID: r.NodeID, Score: r.Score}
			}
			return nil
		}
		return scanNamedFTSDefinition(ctx, tx.graph, def, terms, opts, limit, budget, &results)
	})
	if err != nil {
		return nil, pageStorageOpenError(err)
	}
	return results, nil
}

func scanNamedFTSDefinition(ctx context.Context, graph *store.GraphState, def FTSIndexDefinition, terms []string, opts FTSSearchOptions, limit uint64, budget *directSearchBudget, out *[]FTSIndexSearchResult) error {
	visitDocs := func(fn func(uint64, store.Properties) error) error {
		if def.Kind == FTSIndexNode {
			return graph.VisitNodes(store.WithPageReadBudget(ctx, budget), func(n *store.NodeRecord) error {
				if err := budget.add(1); err != nil {
					return err
				}
				if containsFTSString(n.Labels, def.Scope) {
					return fn(n.ID, n.Properties)
				}
				return nil
			})
		}
		return graph.VisitEdges(store.WithPageReadBudget(ctx, budget), func(e *store.EdgeRecord) error {
			if err := budget.add(1); err != nil {
				return err
			}
			if e.Type == def.Scope {
				return fn(e.ID, e.Properties)
			}
			return nil
		})
	}
	analyze := func(text string) ([]string, uint64, error) {
		if err := budget.add(saturatingMul(uint64(len(text)), 3)); err != nil {
			return nil, 0, err
		}
		maxBytes := budget.maxBytes - budget.bytes
		var tokens []string
		var err error
		if opts.Analyzer == FTSAnalyzerEnglishPorter {
			tokens, err = search.AnalyzeEnglishPorterContextWithLimit(ctx, text, maxBytes)
		} else {
			tokens, err = search.TokenizeContextWithLimit(ctx, text, maxBytes)
		}
		if errors.Is(err, search.ErrTokenizationLimit) {
			return nil, 0, fmt.Errorf("%w: scoped FTS tokenization exceeds memory budget", ErrResourceLimit)
		}
		if err != nil {
			return nil, 0, err
		}
		bytes := ftsTokenBytes(tokens)
		if err = budget.reserveBytes(bytes); err != nil {
			return nil, 0, err
		}
		return tokens, bytes, nil
	}
	var documentCount, totalLength uint64
	var df []uint64
	if opts.Scoring == FTSScoringBM25 {
		dfBytes := saturatingMul(uint64(len(terms)), 8)
		if err := budget.reserveBytes(dfBytes); err != nil {
			return err
		}
		defer budget.releaseBytes(dfBytes)
		df = make([]uint64, len(terms))
		if err := visitDocs(func(_ uint64, props store.Properties) error {
			text, ok := props.Get(def.Property).(string)
			if !ok {
				return nil
			}
			tokens, b, err := analyze(text)
			if err != nil {
				return err
			}
			defer budget.releaseBytes(b)
			freq, fb, err := ftsTermFrequencies(tokens, terms, opts, budget)
			if err != nil {
				return err
			}
			defer budget.releaseBytes(fb)
			documentCount++
			totalLength = saturatingAdd(totalLength, uint64(len(tokens)))
			for i, n := range freq {
				if n > 0 {
					df[i]++
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	capacity := int(limit)
	if capacity > int(documentCount) && opts.Scoring == FTSScoringBM25 {
		capacity = int(documentCount)
	}
	if capacity < 1 {
		capacity = 1
	}
	resultBytes := saturatingMul(uint64(capacity), 16)
	if err := budget.reserveBytes(resultBytes); err != nil {
		return err
	}
	defer budget.releaseBytes(resultBytes)
	var results []FTSSearchResult
	err := visitDocs(func(id uint64, props store.Properties) error {
		text, ok := props.Get(def.Property).(string)
		if !ok {
			return nil
		}
		tokens, b, err := analyze(text)
		if err != nil {
			return err
		}
		defer budget.releaseBytes(b)
		freq, fb, err := ftsTermFrequencies(tokens, terms, opts, budget)
		if err != nil {
			return err
		}
		defer budget.releaseBytes(fb)
		score := frequencyScore(freq)
		if opts.Scoring == FTSScoringBM25 {
			avg := float64(0)
			if documentCount > 0 {
				avg = float64(totalLength) / float64(documentCount)
			}
			score = bm25Score(freq, df, uint64(len(tokens)), documentCount, avg)
		}
		if score <= 0 {
			return nil
		}
		results, err = pushFTSResultBudget(results, FTSSearchResult{NodeID: id, Score: float32(score)}, int(limit), budget)
		return err
	})
	if err != nil {
		return err
	}
	if err = sortFTSResultsBudget(results, budget); err != nil {
		return err
	}
	converted := make([]FTSIndexSearchResult, len(results))
	for i, r := range results {
		converted[i] = FTSIndexSearchResult{EntityID: r.NodeID, Score: r.Score}
	}
	*out = converted
	return nil
}

func containsFTSString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
