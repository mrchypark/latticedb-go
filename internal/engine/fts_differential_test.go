package engine

import (
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestFuzzyPostingSearchMatchesBruteForce(t *testing.T) {
	texts := []string{
		"book back bake bake",
		"books book boon",
		"back buck",
		"한글 한굴",
		"exact exact fuzzy",
	}
	graph := store.NewGraphState()
	for index, text := range texts {
		id := uint64(index + 1)
		tokens := search.Tokenize(text)
		graph.FTS.Set(id, &store.FTSRecord{Text: text, Tokens: tokens})
		for _, token := range tokens {
			graph.FTSTokens.Add(token, id)
		}
	}
	db := &DB{graph: graph, queryCache: map[string]*queryPlan{}}
	queries := []string{"book", "book book", "boon back", "bake buck", "한글", "exact fuzzx"}
	for _, query := range queries {
		opts := FTSSearchOptions{Limit: 100, MaxDistance: 1, MinTermLength: 2}
		got, err := db.FTSSearch(query, opts)
		if err != nil {
			t.Fatal(err)
		}
		terms := search.Tokenize(query)
		want := make([]FTSSearchResult, 0, len(texts))
		for id, record := range graph.FTS.All() {
			score := search.FTSScoreTokensWithOptions(record.Tokens, terms, opts.MaxDistance, opts.MinTermLength)
			if score > 0 {
				want = append(want, FTSSearchResult{NodeID: id, Score: score})
			}
		}
		slices.SortFunc(want, compareFTSResult)
		if !slices.Equal(got, want) {
			t.Fatalf("query %q: optimized=%#v brute=%#v", query, got, want)
		}
	}
}
