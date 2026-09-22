package engine

import (
	"context"
	"errors"
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
	budget, err := newDirectSearchBudget(context.Background(), ^uint64(0), ^uint64(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	if matched, err := fuzzyTokenMatchBudget("a", "b", ^uint32(0), 0, budget); err != nil || !matched {
		t.Fatalf("max-distance match = %v, %v", matched, err)
	}
}

func TestFuzzyTokenMatchBudgetMatchesOracle(t *testing.T) {
	values := []string{""}
	frontier := values
	for length := 1; length <= 3; length++ {
		next := make([]string, 0, len(frontier)*3)
		for _, prefix := range frontier {
			for _, letter := range []string{"a", "b", "한"} {
				next = append(next, prefix+letter)
			}
		}
		values = append(values, next...)
		frontier = next
	}
	for _, term := range values {
		for _, token := range values {
			for maxDistance := uint32(0); maxDistance <= 3; maxDistance++ {
				budget, err := newDirectSearchBudget(context.Background(), ^uint64(0), ^uint64(0), 0)
				if err != nil {
					t.Fatal(err)
				}
				got, err := fuzzyTokenMatchBudget(term, token, maxDistance, 0, budget)
				if err != nil {
					t.Fatalf("match %q %q distance %d: %v", term, token, maxDistance, err)
				}
				if want := search.FuzzyTokenMatch(term, token, maxDistance, 0); got != want {
					t.Fatalf("match %q %q distance %d = %v, want %v", term, token, maxDistance, got, want)
				}
			}
		}
	}
}

func TestFuzzyTokenMatchBudgetLengthPruneRespectsCancellationAndBudget(t *testing.T) {
	budget, err := newDirectSearchBudget(context.Background(), ^uint64(0), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	matched, err := fuzzyTokenMatchBudget("aaaaaaaaaa", "b", 1, 1, budget)
	if matched || !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("length-pruned budget error = %v, match = %v", err, matched)
	}
	budget, err = newDirectSearchBudget(context.Background(), 10, ^uint64(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fuzzyTokenMatchBudget("aaaaaaaaaa", "b", 1, 1, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("length-pruned work budget error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	budget, err = newDirectSearchBudget(ctx, ^uint64(0), ^uint64(0), 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := fuzzyTokenMatchBudget("aaaaaaaaaa", "b", 1, 1, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled length-pruned match error = %v", err)
	}
}

func TestFuzzyScoreReservesFrequencyMap(t *testing.T) {
	// Result+query=56, frequency map=96, edit-distance scratch=400 bytes.
	tokens := []string{"abcdefghijklmnop", "abcdefghijklmnoq"}
	for _, test := range []struct {
		name              string
		maxBytes, maxWork uint64
		wantError         bool
	}{
		{"map reservation", 100, 10000, true},
		{"overlapping scratch", 512, 10000, true},
		{"exact fit", 552, 10000, false},
		{"work failure", 552, 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := &directSearchBudget{ctx: t.Context(), maxWork: test.maxWork, maxBytes: test.maxBytes, bytes: 56}
			score, err := ftsScoreBudget(tokens, []string{tokens[0]}, 1, 1, budget)
			if test.wantError {
				if !errors.Is(err, ErrResourceLimit) {
					t.Fatalf("error=%v, want resource limit", err)
				}
			} else if err != nil || score != 1 {
				t.Fatalf("score=%v error=%v", score, err)
			}
			if budget.bytes != 56 {
				t.Fatalf("retained bytes=%d, want 56", budget.bytes)
			}
		})
	}
}

func TestFuzzySearchOverlappingMemoryBudget(t *testing.T) {
	graph := store.NewGraphState()
	tokens := []string{"abcdefghijklmnop", "abcdefghijklmnoq"}
	graph.FTS.Set(1, &store.FTSRecord{Text: tokens[0] + " " + tokens[1], Tokens: tokens})
	for _, token := range tokens {
		graph.FTSTokens.Add(token, 1)
	}
	db := &DB{graph: graph, queryCache: map[string]*queryPlan{}}
	for _, maxBytes := range []uint64{512, 552} {
		got, err := db.FTSSearch(tokens[0], FTSSearchOptions{Limit: 1, MaxDistance: 1, MinTermLength: 1, MaxBytes: maxBytes})
		if maxBytes == 512 {
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("512-byte search=%v error=%v", got, err)
			}
		} else if err != nil || len(got) != 1 || got[0].NodeID != 1 || got[0].Score != 1 {
			t.Fatalf("exact-fit search=%v error=%v", got, err)
		}
	}
}
