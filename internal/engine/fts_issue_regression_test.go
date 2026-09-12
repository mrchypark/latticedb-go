package engine

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

type ftsCancelChecks struct {
	context.Context
	calls int
}

func (c *ftsCancelChecks) Err() error {
	c.calls++
	if c.calls >= 2 {
		return context.Canceled
	}
	return nil
}

func TestFTSExactScoringStopsWithinBudget(t *testing.T) {
	for _, counts := range [][2]int{{10, 1000}, {1000, 10}} {
		tokens, terms := make([]string, counts[0]), make([]string, counts[1])
		for i := range tokens {
			tokens[i] = "word"
		}
		for i := range terms {
			terms[i] = "word"
		}
		budget := &directSearchBudget{ctx: context.Background(), maxWork: 64}
		if _, _, err := ftsExactScore(budget, tokens, terms, 0); !errors.Is(err, ErrResourceLimit) || budget.work > 64 {
			t.Fatalf("counts=%v work=%d err=%v", counts, budget.work, err)
		}
		ctx := &ftsCancelChecks{Context: context.Background()}
		budget = &directSearchBudget{ctx: ctx, maxWork: ^uint64(0)}
		if _, _, err := ftsExactScore(budget, tokens, terms, 0); !errors.Is(err, context.Canceled) || budget.work > 8192 {
			t.Fatalf("cancel counts=%v work=%d err=%v", counts, budget.work, err)
		}
	}
}

func TestFTSExactDuplicateTermsPreserveScoresAndOrder(t *testing.T) {
	graph := store.NewGraphState()
	for i, text := range []string{"hello hello world", "hello world world", "world"} {
		id := uint64(i + 1)
		tokens := search.Tokenize(text)
		graph.FTS.Set(id, &store.FTSRecord{Text: text, Tokens: tokens})
		for _, token := range tokens {
			graph.FTSTokens.Add(token, id)
		}
	}
	db := &DB{graph: graph, queryCache: map[string]*queryPlan{}}
	for _, query := range []string{"hello hello", "hello world", strings.Repeat("hello ", 100)} {
		want := []FTSSearchResult{}
		for id, record := range graph.FTS.All() {
			score := search.FTSScoreTokensWithOptions(record.Tokens, search.Tokenize(query), 0, 0)
			if score > 0 {
				want = append(want, FTSSearchResult{NodeID: id, Score: score})
			}
		}
		slices.SortFunc(want, compareFTSResult)
		for _, limit := range []uint32{1, 10} {
			got, err := db.FTSSearch(query, FTSSearchOptions{Limit: limit})
			if err != nil || !slices.Equal(got, want[:min(len(want), int(limit))]) {
				t.Fatalf("query=%q K=%d got=%v want=%v err=%v", query, limit, got, want, err)
			}
		}
	}
}

func TestFTSLedgerMatchesLiveAfterReopenReplaceAndDelete(t *testing.T) {
	create := func(path string) (*DB, uint64, [2]uint64) {
		db, err := Open(path, OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		var ftsID uint64
		if err := db.Update(func(tx *Tx) error {
			n, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			ftsID = n.ID
			a, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}, Properties: map[string]any{"name": "one"}})
			if err != nil {
				return err
			}
			b, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}})
			if err != nil {
				return err
			}
			_, err = tx.CreateEdge(a.ID, b.ID, "LINK", CreateEdgeOptions{})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateNodePropertyIndex("P", "name"); err != nil {
			t.Fatal(err)
		}
		base := [2]uint64{db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes}
		if err := db.Update(func(tx *Tx) error { return tx.FTSIndex(ftsID, "original searchable words") }); err != nil {
			t.Fatal(err)
		}
		return db, ftsID, base
	}
	live, id, baseline := create(filepath.Join(t.TempDir(), "live"))
	defer live.Close()
	path := filepath.Join(t.TempDir(), "reopen")
	recovered, otherID, _ := create(path)
	if id != otherID {
		t.Fatal("fixture IDs differ")
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	check := func() {
		t.Helper()
		a := [2]uint64{live.graph.DerivedIndexWork, live.graph.DerivedIndexLogicalBytes}
		b := [2]uint64{recovered.graph.DerivedIndexWork, recovered.graph.DerivedIndexLogicalBytes}
		if a != b {
			t.Fatalf("live=%v recovered=%v", a, b)
		}
	}
	check()
	for _, db := range []*DB{live, recovered} {
		if err := db.Update(func(tx *Tx) error { return tx.FTSIndex(id, "replacement replacement") }); err != nil {
			t.Fatal(err)
		}
	}
	check()
	for _, db := range []*DB{live, recovered} {
		if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(id) }); err != nil {
			t.Fatal(err)
		}
	}
	check()
	got := [2]uint64{recovered.graph.DerivedIndexWork, recovered.graph.DerivedIndexLogicalBytes}
	if got != baseline || got[0] == 0 {
		t.Fatalf("other index costs=%v want=%v", got, baseline)
	}
}
