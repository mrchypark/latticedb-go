package engine

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// FTSScoring selects direct FTSSearch ranking. Frequency preserves the legacy
// term-frequency score; BM25 is an explicit opt-in.
type FTSScoring uint8

const (
	FTSScoringFrequency FTSScoring = iota
	FTSScoringBM25
)

// FTSAnalyzer selects direct FTSSearch analysis. Standard preserves the
// stored-token behavior; EnglishPorter is an explicit scan fallback.
type FTSAnalyzer uint8

const (
	FTSAnalyzerStandard FTSAnalyzer = iota
	FTSAnalyzerEnglishPorter
)

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

func validateFTSSearchScoring(opts FTSSearchOptions) error {
	if opts.Scoring != FTSScoringFrequency && opts.Scoring != FTSScoringBM25 {
		return fmt.Errorf("%w: invalid FTS scoring mode", ErrInvalidArgument)
	}
	if opts.Analyzer != FTSAnalyzerStandard && opts.Analyzer != FTSAnalyzerEnglishPorter {
		return fmt.Errorf("%w: invalid FTS analyzer", ErrInvalidArgument)
	}
	return nil
}

func (db *DB) ftsSearchBM25Context(ctx context.Context, query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	// BM25 needs live corpus statistics. Porter has no maintained analyzed
	// postings, so both are bounded scan paths. Add persisted analyzed postings
	// only when measurement demonstrates this ceiling is insufficient.
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
	terms, termBytes, err := ftsAnalyzeQuery(ctx, query, opts.Analyzer, budget)
	if err != nil {
		return nil, err
	}
	defer budget.releaseBytes(termBytes)
	if opts.Scoring == FTSScoringBM25 {
		terms, err = uniqueFTSTerms(terms, budget)
		if err != nil {
			return nil, err
		}
	}
	if len(terms) == 0 {
		return []FTSSearchResult{}, nil
	}
	termStateBytes := saturatingMul(uint64(len(terms)), 24)
	if err := budget.reserveBytes(termStateBytes); err != nil {
		return nil, err
	}
	defer budget.releaseBytes(termStateBytes)

	var results []FTSSearchResult
	err = db.View(func(tx *Tx) error {
		var documentCount, totalLength uint64
		var documentFrequency []uint64
		if opts.Scoring == FTSScoringBM25 {
			var err error
			documentCount, totalLength, documentFrequency, err = ftsBM25CorpusStats(ctx, tx.graph, terms, opts, budget)
			if err != nil {
				return err
			}
			if documentCount == 0 || totalLength == 0 {
				return nil
			}
		}
		capacity := min(limit, uint64(tx.graph.FTS.Len()))
		results = make([]FTSSearchResult, 0, int(capacity))
		averageLength := float64(0)
		if documentCount != 0 {
			averageLength = float64(totalLength) / float64(documentCount)
		}
		for nodeID, record := range tx.graph.FTS.All() {
			tokens, tokenBytes, err := ftsRecordTokens(ctx, record, opts.Analyzer, budget)
			if err != nil {
				return err
			}
			frequencies, frequencyBytes, err := ftsTermFrequencies(tokens, terms, opts, budget)
			budget.releaseBytes(tokenBytes)
			if err != nil {
				return err
			}
			score := frequencyScore(frequencies)
			if opts.Scoring == FTSScoringBM25 {
				score = bm25Score(frequencies, documentFrequency, uint64(len(tokens)), documentCount, averageLength)
			}
			budget.releaseBytes(frequencyBytes)
			if score > 0 {
				var pushErr error
				results, pushErr = pushFTSResultBudget(results, FTSSearchResult{NodeID: nodeID, Score: float32(score)}, int(capacity), budget)
				if pushErr != nil {
					return pushErr
				}
			}
		}
		return sortFTSResultsBudget(results, budget)
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func ftsBM25CorpusStats(ctx context.Context, graph *store.GraphState, terms []string, opts FTSSearchOptions, budget *directSearchBudget) (uint64, uint64, []uint64, error) {
	frequencies := make([]uint64, len(terms))
	var documentCount, totalLength uint64
	for _, record := range graph.FTS.All() {
		if err := budget.add(1); err != nil {
			return 0, 0, nil, err
		}
		tokens, tokenBytes, err := ftsRecordTokens(ctx, record, opts.Analyzer, budget)
		if err != nil {
			return 0, 0, nil, err
		}
		matches, matchBytes, err := ftsTermFrequencies(tokens, terms, opts, budget)
		budget.releaseBytes(tokenBytes)
		if err != nil {
			return 0, 0, nil, err
		}
		for index, count := range matches {
			if count != 0 {
				frequencies[index]++
			}
		}
		budget.releaseBytes(matchBytes)
		totalLength = saturatingAdd(totalLength, uint64(len(tokens)))
		documentCount++
	}
	return documentCount, totalLength, frequencies, budget.check()
}

func ftsAnalyzeQuery(ctx context.Context, query string, analyzer FTSAnalyzer, budget *directSearchBudget) ([]string, uint64, error) {
	var tokens []string
	var err error
	if analyzer == FTSAnalyzerEnglishPorter {
		tokens, err = search.AnalyzeEnglishPorterContextWithLimit(ctx, query, budget.maxBytes-budget.bytes)
	} else {
		tokens, err = search.TokenizeContextWithLimit(ctx, query, budget.maxBytes-budget.bytes)
	}
	if errors.Is(err, search.ErrTokenizationLimit) {
		return nil, 0, fmt.Errorf("%w: search query exceeds memory budget", ErrResourceLimit)
	}
	if err != nil {
		return nil, 0, err
	}
	bytes := ftsTokenBytes(tokens)
	if err := budget.reserveBytes(bytes); err != nil {
		return nil, 0, err
	}
	return tokens, bytes, nil
}

func ftsRecordTokens(ctx context.Context, record *store.FTSRecord, analyzer FTSAnalyzer, budget *directSearchBudget) ([]string, uint64, error) {
	if record == nil {
		return nil, 0, nil
	}
	if analyzer == FTSAnalyzerStandard {
		if err := budget.add(uint64(max(1, len(record.Tokens)))); err != nil {
			return nil, 0, err
		}
		return record.Tokens, 0, nil
	}
	if err := budget.add(max(uint64(1), (uint64(len(record.Text))+63)/64)); err != nil {
		return nil, 0, err
	}
	tokens, err := search.AnalyzeEnglishPorterContextWithLimit(ctx, record.Text, budget.maxBytes-budget.bytes)
	if errors.Is(err, search.ErrTokenizationLimit) {
		return nil, 0, fmt.Errorf("%w: FTS analyzer scratch exceeds memory budget", ErrResourceLimit)
	}
	if err != nil {
		return nil, 0, err
	}
	bytes := ftsTokenBytes(tokens)
	if err := budget.reserveBytes(bytes); err != nil {
		return nil, 0, err
	}
	if err := budget.add(uint64(max(1, len(tokens)))); err != nil {
		budget.releaseBytes(bytes)
		return nil, 0, err
	}
	return tokens, bytes, nil
}

func ftsTokenBytes(tokens []string) uint64 {
	bytes := saturatingMul(uint64(len(tokens)), 32)
	for _, token := range tokens {
		bytes = saturatingAdd(bytes, uint64(len(token)))
	}
	return bytes
}

func ftsTermFrequencies(tokens, terms []string, opts FTSSearchOptions, budget *directSearchBudget) ([]uint64, uint64, error) {
	bytes := saturatingMul(uint64(len(terms)), 8)
	if err := budget.reserveBytes(bytes); err != nil {
		return nil, 0, err
	}
	frequencies := make([]uint64, len(terms))
	for termIndex, term := range terms {
		for _, token := range tokens {
			match, err := fuzzyTokenMatchBudget(term, token, opts.MaxDistance, opts.MinTermLength, budget)
			if err != nil {
				budget.releaseBytes(bytes)
				return nil, 0, err
			}
			if match {
				frequencies[termIndex]++
			}
		}
	}
	return frequencies, bytes, nil
}

func uniqueFTSTerms(terms []string, budget *directSearchBudget) ([]string, error) {
	bytes := saturatingMul(uint64(len(terms)), 48)
	for _, term := range terms {
		bytes = saturatingAdd(bytes, uint64(len(term)))
	}
	if err := budget.reserveBytes(bytes); err != nil {
		return nil, err
	}
	defer budget.releaseBytes(bytes)
	seen := make(map[string]struct{}, len(terms))
	out := terms[:0]
	for _, term := range terms {
		if err := budget.add(uint64(max(1, len(term)))); err != nil {
			return nil, err
		}
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		out = append(out, term)
	}
	return out, nil
}

func bm25Score(frequencies, documentFrequency []uint64, documentLength, documentCount uint64, averageLength float64) float64 {
	if documentLength == 0 || documentCount == 0 || averageLength == 0 {
		return 0
	}
	var score float64
	for index, frequency := range frequencies {
		if frequency == 0 || documentFrequency[index] == 0 {
			continue
		}
		idf := math.Log(1 + (float64(documentCount-documentFrequency[index])+0.5)/(float64(documentFrequency[index])+0.5))
		denominator := float64(frequency) + bm25K1*(1-bm25B+bm25B*float64(documentLength)/averageLength)
		score += idf * float64(frequency) * (bm25K1 + 1) / denominator
	}
	return score
}

func frequencyScore(frequencies []uint64) float64 {
	var score uint64
	for _, frequency := range frequencies {
		score = saturatingAdd(score, frequency)
	}
	return float64(score)
}

func pushFTSResultBudget(heap []FTSSearchResult, value FTSSearchResult, limit int, budget *directSearchBudget) ([]FTSSearchResult, error) {
	steps := uint64(1)
	for size := len(heap); size > 1; size >>= 1 {
		steps++
	}
	if err := budget.add(steps); err != nil {
		return nil, err
	}
	return pushFTSResult(heap, value, limit), nil
}

func sortFTSResultsBudget(results []FTSSearchResult, budget *directSearchBudget) error {
	if len(results) < 2 {
		return budget.check()
	}
	bytes := saturatingMul(uint64(len(results)), 16)
	if err := budget.reserveBytes(bytes); err != nil {
		return err
	}
	defer budget.releaseBytes(bytes)
	scratch := make([]FTSSearchResult, len(results))
	for width := 1; width < len(results); width *= 2 {
		for left := 0; left < len(results); left += 2 * width {
			middle := min(left+width, len(results))
			right := min(left+2*width, len(results))
			i, j, out := left, middle, left
			for i < middle && j < right {
				if err := budget.add(1); err != nil {
					return err
				}
				if compareFTSResult(results[i], results[j]) <= 0 {
					scratch[out] = results[i]
					i++
				} else {
					scratch[out] = results[j]
					j++
				}
				out++
			}
			out += copy(scratch[out:], results[i:middle])
			copy(scratch[out:], results[j:right])
		}
		copy(results, scratch)
		if err := budget.check(); err != nil {
			return err
		}
	}
	return nil
}
