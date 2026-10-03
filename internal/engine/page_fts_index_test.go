package engine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func pageFTSFixture(t *testing.T, count int) (*pagestore.DB, *store.GraphState) {
	t.Helper()
	ctx := context.Background()
	pages, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	graph := store.NewGraphState()
	graph.AppMetadata.Set(string(ftsDefinitionKey("manual-standard")), mustFTSDefinitionValue(t, FTSIndexDefinition{Name: "manual-standard", Kind: FTSIndexNode, Scope: "Item", Property: "body"}))
	graph.FTSProperties = map[string]store.StringPostings{"body": store.NewStringPostings()}
	for i := 1; i <= count; i++ {
		id := uint64(i)
		text := "common trailing"
		if i == 1 {
			text = "rare running"
		}
		if i == 2 {
			text = "rare runs"
		}
		labels := []string{"Other"}
		if i%2 == 1 {
			labels = []string{"Item"}
		}
		node := &store.NodeRecord{ID: id, Labels: labels, Properties: store.PropertiesFromMap(map[string]any{"body": text})}
		if err := page.PutNode(node); err != nil {
			t.Fatal(err)
		}
		tokens, err := search.TokenizeContext(ctx, text)
		if err != nil {
			t.Fatal(err)
		}
		if err := page.PutFTS(id, &store.FTSRecord{Text: text, Tokens: tokens}); err != nil {
			t.Fatal(err)
		}
	}
	graph.PageBase = page
	if err := preparePageFTSIndexes(ctx, page, graph, ^uint64(0), ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := pages.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph.PageBase = &store.PageGraph{Tx: read, SearchIndexesCurrent: true}
	t.Cleanup(func() { _ = read.Rollback(); _ = pages.Close() })
	return pages, graph
}

func mustFTSDefinitionValue(t *testing.T, def FTSIndexDefinition) []byte {
	t.Helper()
	data, err := jsonMarshalFTSDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func jsonMarshalFTSDefinition(def FTSIndexDefinition) ([]byte, error) {
	return json.Marshal(persistedFTSIndexDefinition{Version: 1, Definition: def})
}

func TestPageFTSIndexesUseDeclaredScopeAndBuildBudgets(t *testing.T) {
	pages, graph := pageFTSFixture(t, 32)
	_ = pages
	var declared, configured []uint64
	if err := graph.PageBase.VisitFTSPostings(context.Background(), declaredFTSPageIndexName("manual-standard"), "rare", func(p store.PageFTSPosting) error { declared = append(declared, p.DocumentID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(declared) != 1 || declared[0] != 1 {
		t.Fatalf("label-scoped postings=%v", declared)
	}
	if err := graph.PageBase.VisitFTSPostings(context.Background(), configuredPropertyFTSPageIndexName("body"), "rare", func(p store.PageFTSPosting) error { configured = append(configured, p.DocumentID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(configured) != 2 || configured[0] != 1 || configured[1] != 2 {
		t.Fatalf("configured property postings=%v", configured)
	}

	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	if err := page.PutNode(&store.NodeRecord{ID: 1, Labels: []string{"Other"}, Properties: store.PropertiesFromMap(map[string]any{"body": "replacement"})}); err != nil {
		t.Fatal(err)
	}
	after := *graph
	after.PageBase = page
	delta := store.GraphDelta{UpsertNodes: []uint64{1}}
	if err := applyPageFTSIndexDelta(context.Background(), page, graph, &after, delta, ^uint64(0), ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = graph.PageBase.Tx.Rollback()
	read, err := pages.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph.PageBase = &store.PageGraph{Tx: read, SearchIndexesCurrent: true}
	defer read.Rollback()
	declared = nil
	configured = nil
	if err := graph.PageBase.VisitFTSPostings(context.Background(), declaredFTSPageIndexName("manual-standard"), "rare", func(p store.PageFTSPosting) error { declared = append(declared, p.DocumentID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(declared) != 0 {
		t.Fatalf("out-of-scope update retained postings: %v", declared)
	}
	if err := graph.PageBase.VisitFTSPostings(context.Background(), configuredPropertyFTSPageIndexName("body"), "replacement", func(p store.PageFTSPosting) error { configured = append(configured, p.DocumentID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(configured) != 1 || configured[0] != 1 {
		t.Fatalf("configured property update=%v", configured)
	}
}

func TestPageFTSSearchRankingAndSelectiveWorkMatchScanOracle(t *testing.T) {
	_, graph := pageFTSFixture(t, 256)
	db := new(DB)
	for _, test := range []struct {
		query string
		opts  FTSSearchOptions
	}{
		{"rare", FTSSearchOptions{Limit: 5, Scoring: FTSScoringBM25}},
		{"rarf rarf", FTSSearchOptions{Limit: 5, MaxDistance: 1, MinTermLength: 1}},
		{"rarf rarf", FTSSearchOptions{Limit: 5, MaxDistance: 1, MinTermLength: 1, Scoring: FTSScoringBM25}},
		{"run", FTSSearchOptions{Limit: 5, Analyzer: FTSAnalyzerEnglishPorter, Scoring: FTSScoringBM25}},
	} {
		limit := test.opts.Limit
		if limit == 0 {
			limit = 10
		}
		budget, err := newDirectSearchBudget(context.Background(), ^uint64(0), ^uint64(0), limit)
		if err != nil {
			t.Fatal(err)
		}
		if err := budget.add(uint64(len(test.query))); err != nil {
			t.Fatal(err)
		}
		got, used, err := db.pageIndexedFTSSearch(context.Background(), graph, test.query, test.opts, budget)
		if err != nil || !used {
			t.Fatalf("indexed search used=%v err=%v", used, err)
		}
		want, err := pageFTSScanOracle(context.Background(), graph, test.query, test.opts)
		if err != nil {
			t.Fatal(err)
		}
		if !sameFTSResults(got, want) {
			t.Fatalf("%q indexed=%v scan=%v", test.query, got, want)
		}
	}
	selective := FTSSearchOptions{Limit: 5, MaxWork: 80}
	budget, err := newDirectSearchBudget(context.Background(), selective.MaxWork, 0, selective.Limit)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.add(4); err != nil {
		t.Fatal(err)
	}
	if got, used, err := db.pageIndexedFTSSearch(context.Background(), graph, "rare", selective, budget); err != nil || !used || len(got) != 2 {
		t.Fatalf("selective indexed work got=%v used=%v err=%v", got, used, err)
	}
	low := FTSSearchOptions{Limit: 10, MaxBytes: 220}
	budget, err = newDirectSearchBudget(context.Background(), 0, low.MaxBytes, low.Limit)
	if err != nil {
		t.Fatal(err)
	}
	_ = budget.add(4)
	if _, _, err := db.pageIndexedFTSSearch(context.Background(), graph, "common", low, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("low candidate memory budget error=%v", err)
	}
}

func pageFTSScanOracle(ctx context.Context, graph *store.GraphState, query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	limit := opts.Limit
	if limit == 0 {
		limit = 10
	}
	budget, err := newDirectSearchBudget(ctx, ^uint64(0), ^uint64(0), limit)
	if err != nil {
		return nil, err
	}
	terms, _, err := ftsAnalyzeQuery(ctx, query, opts.Analyzer, budget)
	if err != nil {
		return nil, err
	}
	if opts.Scoring == FTSScoringBM25 {
		terms, err = uniqueFTSTerms(terms, budget)
		if err != nil {
			return nil, err
		}
	}
	if len(terms) == 0 {
		return []FTSSearchResult{}, nil
	}
	var docs, total uint64
	df := make([]uint64, len(terms))
	if opts.Scoring == FTSScoringBM25 {
		docs, total, df, err = ftsBM25CorpusStats(ctx, graph, terms, opts, budget)
		if err != nil {
			return nil, err
		}
	}
	var results []FTSSearchResult
	avg := float64(0)
	if docs > 0 {
		avg = float64(total) / float64(docs)
	}
	err = graph.VisitFTS(ctx, func(id uint64, record *store.FTSRecord) error {
		tokens, _, err := ftsRecordTokens(ctx, record, opts.Analyzer, budget)
		if err != nil {
			return err
		}
		freq, _, err := ftsTermFrequencies(tokens, terms, opts, budget)
		if err != nil {
			return err
		}
		score := frequencyScore(freq)
		if opts.Scoring == FTSScoringBM25 {
			score = bm25Score(freq, df, uint64(len(tokens)), docs, avg)
		}
		if score > 0 {
			results = pushFTSResult(results, FTSSearchResult{NodeID: id, Score: float32(score)}, int(limit))
		}
		return nil
	})
	if err == nil {
		err = sortFTSResultsBudget(results, budget)
	}
	return results, err
}

func sameFTSResults(a, b []FTSSearchResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].NodeID != b[i].NodeID || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}

func TestNamedPageFTSIndexSearchesNodesAndEdges(t *testing.T) {
	ctx := context.Background()
	pages, err := pagestore.Open(filepath.Join(t.TempDir(), "named-pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pages.Close(); err != nil {
			t.Error(err)
		}
	}()
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	page := &store.PageGraph{Tx: write}
	graph := store.NewGraphState()
	graph.PageBase = page
	graph.AppMetadata.Set(string(ftsDefinitionKey("nodes")), mustFTSDefinitionValue(t, FTSIndexDefinition{Name: "nodes", Kind: FTSIndexNode, Scope: "Item", Property: "body"}))
	graph.AppMetadata.Set(string(ftsDefinitionKey("edges")), mustFTSDefinitionValue(t, FTSIndexDefinition{Name: "edges", Kind: FTSIndexEdge, Scope: "LINK", Property: "body"}))
	for _, n := range []*store.NodeRecord{
		{ID: 1, Labels: []string{"Item"}, Properties: store.PropertiesFromMap(map[string]any{"body": "shared node"})},
		{ID: 2, Labels: []string{"Other"}, Properties: store.PropertiesFromMap(map[string]any{"body": "shared node"})},
	} {
		if err := page.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := page.PutEdge(&store.EdgeRecord{ID: 7, SourceID: 1, TargetID: 2, Type: "LINK", Properties: store.PropertiesFromMap(map[string]any{"body": "shared edge"})}); err != nil {
		t.Fatal(err)
	}
	if err := preparePageFTSIndexes(ctx, page, graph, ^uint64(0), ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := pages.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	graph.PageBase = &store.PageGraph{Tx: read, SearchIndexesCurrent: true}
	db := &DB{graph: graph}
	for _, tc := range []struct {
		name string
		id   uint64
	}{{"nodes", 1}, {"edges", 7}} {
		got, err := db.FTSSearchIndex(tc.name, "shared", FTSSearchOptions{Scoring: FTSScoringBM25})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].EntityID != tc.id {
			t.Fatalf("%s search=%v", tc.name, got)
		}
		fuzzy, err := db.FTSSearchIndex(tc.name, "shard", FTSSearchOptions{MaxDistance: 1, MinTermLength: 1})
		if err != nil || len(fuzzy) != 1 || fuzzy[0].EntityID != tc.id {
			t.Fatalf("%s fuzzy search=%v err=%v", tc.name, fuzzy, err)
		}
	}
}

func TestDeclaredFTSBuildChargesOutOfScopeRows(t *testing.T) {
	ctx := context.Background()
	pages, err := pagestore.Open(filepath.Join(t.TempDir(), "budget-pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	tx, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: tx}
	for id := uint64(1); id <= 24; id++ {
		if err := page.PutNode(&store.NodeRecord{ID: id, Labels: []string{"Unrelated"}, Properties: store.PropertiesFromMap(map[string]any{"body": "text"})}); err != nil {
			t.Fatal(err)
		}
	}
	budget := ftsIndexBudget{maxWork: 8, maxBytes: 1 << 20}
	err = rebuildDeclaredPageFTS(ctx, page, FTSIndexDefinition{Name: "selective", Kind: FTSIndexNode, Scope: "Wanted", Property: "body"}, declaredFTSPageIndexName("selective"), &budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("out-of-scope row scan error=%v", err)
	}
	_ = tx.Rollback()
}

func TestNamedFTSScanChargesOutOfScopeRows(t *testing.T) {
	ctx := context.Background()
	pages, err := pagestore.Open(filepath.Join(t.TempDir(), "named-scan-budget"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	graph := store.NewGraphState()
	graph.PageBase = page
	for id := uint64(1); id <= 64; id++ {
		if err := page.PutNode(&store.NodeRecord{ID: id, Labels: []string{"Elsewhere"}, Properties: store.PropertiesFromMap(map[string]any{"body": "value"})}); err != nil {
			t.Fatal(err)
		}
	}
	for _, scoring := range []FTSScoring{FTSScoringFrequency, FTSScoringBM25} {
		budget, err := newDirectSearchBudget(ctx, 4, 1<<20, 10)
		if err != nil {
			t.Fatal(err)
		}
		out := []FTSIndexSearchResult{}
		err = scanNamedFTSDefinition(ctx, graph, FTSIndexDefinition{Name: "scoped", Kind: FTSIndexNode, Scope: "Wanted", Property: "body"}, []string{"value"}, FTSSearchOptions{Limit: 10, Scoring: scoring}, 10, budget, &out)
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("scoring=%d out-of-scope scan error=%v", scoring, err)
		}
	}
	_ = write.Rollback()
}

func TestPageFTSDeltaChargesManualPostingDeletion(t *testing.T) {
	ctx := context.Background()
	pages, err := pagestore.Open(filepath.Join(t.TempDir(), "manual-delete-budget"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	tokens := make([]string, 1024)
	for i := range tokens {
		tokens[i] = "term" + base26Word(uint64(i))
	}
	if err := page.ReplaceFTSDocument(ctx, store.PageFTSManualStandard, 1, tokens); err != nil {
		t.Fatal(err)
	}
	low := &ftsIndexBudget{maxWork: 4, maxBytes: 256}
	err = page.DeleteFTSDocument(store.WithFTSMaintenanceBudget(ctx, low), store.PageFTSManualStandard, 1)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("1024 unique-term posting deletion under four units: %v", err)
	}
	_ = write.Rollback()
}

func TestPageFTSDeleteBudgetRejectsAndRollsBackLargePostingSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete-budget-reopen")
	high := OpenOptions{Create: true, PageStorage: true, DerivedIndexBuildMaxWork: 1 << 20, DerivedIndexBuildMaxLogicalBytes: 1 << 20}
	db, err := Open(path, high)
	if err != nil {
		t.Fatal(err)
	}
	var id uint64
	words := make([]string, 1024)
	for i := range words {
		words[i] = "term" + base26Word(uint64(i))
	}
	text := strings.Join(words, " ")
	if err := db.Update(func(tx *Tx) error {
		n, e := tx.CreateNode(CreateNodeOptions{})
		if e != nil {
			return e
		}
		id = n.ID
		return tx.FTSIndex(id, text)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	low := OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 4, DerivedIndexBuildMaxLogicalBytes: 256}
	db, err = Open(path, low)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := db.commitID
	record, readErr := db.graph.ReadFTS(id)
	if readErr != nil || record == nil || len(record.Tokens) == 0 {
		t.Fatalf("read FTS record=%v err=%v", record, readErr)
	}
	initialHits, initialErr := db.FTSSearch(record.Tokens[0], FTSSearchOptions{})
	if initialErr != nil || len(initialHits) != 1 {
		t.Fatalf("initial indexed term %v err=%v", initialHits, initialErr)
	}
	err = db.Update(func(tx *Tx) error { return tx.DeleteNode(id) })
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("delete with four-unit budget error=%v", err)
	}
	if db.commitID != before {
		t.Fatalf("rejected delete advanced commit %d -> %d", before, db.commitID)
	}
	if hits, e := db.FTSSearch(record.Tokens[0], FTSSearchOptions{}); e != nil || len(hits) != 1 || hits[0].NodeID != id {
		t.Fatalf("post-rollback manual posting=%v err=%v", hits, e)
	}
}

func TestNamedPageFTSDeleteBudgetRejectsLargePostingSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "named-delete-budget-reopen")
	high := OpenOptions{Create: true, PageStorage: true, DerivedIndexBuildMaxWork: 1 << 20, DerivedIndexBuildMaxLogicalBytes: 1 << 20}
	db, err := Open(path, high)
	if err != nil {
		t.Fatal(err)
	}
	words := make([]string, 1024)
	for i := range words {
		words[i] = "term" + base26Word(uint64(i))
	}
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"body": strings.Join(words, " ")}})
		if err != nil {
			return err
		}
		id = n.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateFTSIndex(FTSIndexDefinition{Name: "named", Kind: FTSIndexNode, Scope: "Item", Property: "body"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	low := OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 4, DerivedIndexBuildMaxLogicalBytes: 256}
	db, err = Open(path, low)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := db.commitID
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(id) }); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("named delete under four-unit budget error=%v", err)
	}
	if db.commitID != before {
		t.Fatalf("rejected named delete advanced commit %d -> %d", before, db.commitID)
	}
	hits, err := db.FTSSearchIndex("named", words[0], FTSSearchOptions{})
	if err != nil || len(hits) != 1 || hits[0].EntityID != id {
		t.Fatalf("named posting after rollback=%v err=%v", hits, err)
	}
}

func base26Word(n uint64) string {
	var b [16]byte
	i := len(b)
	for {
		i--
		b[i] = byte('a' + n%26)
		n /= 26
		if n == 0 {
			break
		}
	}
	return string(b[i:])
}
