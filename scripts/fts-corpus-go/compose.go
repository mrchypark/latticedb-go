package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"time"

	lattice "github.com/mrchypark/latticedb-go"
)

const (
	composeSourceLabel = "SciFactDoc"
	composeNoiseLabel  = "SciFactNoise"
	composeNoiseNodes  = 64
)

type compositionOptions struct {
	QrelsPath   string
	DBPath      string
	EdgeType    string
	TargetLabel string
	CandidateK  uint32
	ResultK     uint32
	Repeats     int
}

type compositionMeasurement struct {
	Repeat     int    `json:"repeat"`
	ElapsedNS  int64  `json:"elapsed_ns"`
	AllocBytes uint64 `json:"alloc_bytes"`
}

type compositionQuery struct {
	QueryID      string                   `json:"query_id"`
	RecallAtK    float64                  `json:"recall_at_k"`
	Candidates   int                      `json:"candidates"`
	Positives    int                      `json:"positives"`
	Rows         int                      `json:"rows"`
	MatchedRows  bool                     `json:"matched_rows"`
	Measurements []compositionMeasurement `json:"measurements"`
}

type compositionSummary struct {
	Queries         int     `json:"queries"`
	MatchedQueries  int     `json:"matched_queries"`
	MeanRecallAtK   float64 `json:"mean_recall_at_k"`
	TotalElapsedNS  int64   `json:"total_elapsed_ns"`
	MeanElapsedNS   float64 `json:"mean_elapsed_ns"`
	P95ElapsedNS    int64   `json:"p95_elapsed_ns"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	MeanAllocBytes  float64 `json:"mean_alloc_bytes"`
}

type compositionBudgetSample struct {
	QueryID    string `json:"query_id"`
	Rows       int    `json:"rows"`
	MinWork    uint64 `json:"min_work"`
	MinBytes   uint64 `json:"min_bytes"`
	MaxWork    uint64 `json:"max_work"`
	MaxBytes   uint64 `json:"max_bytes"`
	MaxRows    uint64 `json:"max_rows"`
	SearchCall string `json:"search_call"`
}

type compositionOutput struct {
	Mode        string                  `json:"mode"`
	Documents   int                     `json:"documents"`
	QueryCount  int                     `json:"query_count"`
	Repeats     int                     `json:"repeats"`
	CandidateK  uint32                  `json:"candidate_k"`
	ResultK     uint32                  `json:"result_k"`
	EdgeType    string                  `json:"edge_type"`
	TargetLabel string                  `json:"target_label"`
	BuildNS     int64                   `json:"build_ns"`
	ReopenNS    int64                   `json:"reopen_ns"`
	MaxRows     uint64                  `json:"max_rows"`
	MaxWork     uint64                  `json:"max_work"`
	MaxBytes    uint64                  `json:"max_bytes"`
	Reference   string                  `json:"reference"`
	Summary     compositionSummary      `json:"summary"`
	Budget      compositionBudgetSample `json:"budget_sample"`
	Queries     []compositionQuery      `json:"queries"`
}

type compositionCase struct {
	query     record
	direct    []lattice.FTSSearchResult
	expected  []lattice.SearchGraphRow
	recall    float64
	positives int
}

func loadQrels(path string) (map[string]map[string]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var qrels map[string]map[string]int
	if err = json.NewDecoder(f).Decode(&qrels); err != nil {
		return nil, fmt.Errorf("decode qrels: %w", err)
	}
	if len(qrels) == 0 {
		return nil, fmt.Errorf("empty qrels %s", path)
	}
	return qrels, nil
}

func buildCompositionGraph(db *lattice.DB, docs []record, edgeType, targetLabel string) (map[uint64]string, error) {
	ids := make(map[uint64]string, len(docs))
	nodes := make([]uint64, len(docs))
	for start := 0; start < len(docs); start += 100 {
		end := min(start+100, len(docs))
		if err := db.Update(func(tx *lattice.Tx) error {
			for i, doc := range docs[start:end] {
				node, err := tx.CreateNode(lattice.CreateNodeOptions{
					Labels: []string{composeSourceLabel, targetLabel},
				})
				if err != nil {
					return err
				}
				if err = tx.FTSIndex(node.ID, doc.Text); err != nil {
					return err
				}
				nodes[start+i] = node.ID
				ids[node.ID] = doc.ID
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("index corpus nodes %d:%d: %w", start, end, err)
		}
	}

	noise := make([]uint64, composeNoiseNodes)
	if err := db.Update(func(tx *lattice.Tx) error {
		for i := range noise {
			node, err := tx.CreateNode(lattice.CreateNodeOptions{Labels: []string{composeNoiseLabel}})
			if err != nil {
				return err
			}
			noise[i] = node.ID
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("create filtered targets: %w", err)
	}

	type edge struct{ source, target uint64 }
	edges := make([]edge, 0, len(nodes)*4)
	for i, source := range nodes {
		first := nodes[(i+1)%len(nodes)]
		second := nodes[(i+7)%len(nodes)]
		edges = append(edges,
			edge{source, first},
			edge{source, second},
			edge{source, first}, // Parallel edge; bag semantics must retain it.
			edge{source, noise[i%len(noise)]},
		)
	}
	for start := 0; start < len(edges); start += 500 {
		end := min(start+500, len(edges))
		if err := db.Update(func(tx *lattice.Tx) error {
			for _, edge := range edges[start:end] {
				if _, err := tx.CreateEdge(edge.source, edge.target, edgeType, lattice.CreateEdgeOptions{}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("create graph edges %d:%d: %w", start, end, err)
		}
	}
	return ids, nil
}

func referenceAdjacency(ctx context.Context, db *lattice.DB, edgeType, targetLabel string) (map[uint64][]lattice.SearchGraphRow, error) {
	query := fmt.Sprintf("MATCH (s:%s)-[r:%s]->(t:%s) RETURN id(s) AS source, id(t) AS target, id(r) AS edge", composeSourceLabel, edgeType, targetLabel)
	result, err := db.QueryContext(ctx, query, nil, lattice.QueryOptions{
		MaxRows:  1 << 20,
		MaxWork:  1 << 40,
		MaxBytes: 256 << 20,
	})
	if err != nil {
		return nil, fmt.Errorf("reference join: %w", err)
	}
	adjacency := make(map[uint64][]lattice.SearchGraphRow)
	for _, row := range result.Rows {
		value, err := graphRow(row)
		if err != nil {
			return nil, err
		}
		adjacency[value.NodeID] = append(adjacency[value.NodeID], value)
	}
	for source := range adjacency {
		edges := adjacency[source]
		sort.Slice(edges, func(i, j int) bool {
			if edges[i].TargetID != edges[j].TargetID {
				return edges[i].TargetID < edges[j].TargetID
			}
			return edges[i].EdgeID < edges[j].EdgeID
		})
		adjacency[source] = edges
	}
	return adjacency, nil
}

func referenceJoin(direct []lattice.FTSSearchResult, adjacency map[uint64][]lattice.SearchGraphRow, resultK uint32) []lattice.SearchGraphRow {
	rows := make([]lattice.SearchGraphRow, 0, min(int(resultK), len(direct)*3))
	for _, candidate := range direct {
		edges := adjacency[candidate.NodeID]
		for _, edge := range edges {
			if uint32(len(rows)) == resultK {
				return rows
			}
			edge.Value = candidate.Score
			rows = append(rows, edge)
		}
	}
	return rows
}

func graphRow(row map[string]lattice.Value) (lattice.SearchGraphRow, error) {
	source, err := uint64Value(row["source"])
	if err != nil {
		return lattice.SearchGraphRow{}, fmt.Errorf("reference source: %w", err)
	}
	target, err := uint64Value(row["target"])
	if err != nil {
		return lattice.SearchGraphRow{}, fmt.Errorf("reference target: %w", err)
	}
	edge, err := uint64Value(row["edge"])
	if err != nil {
		return lattice.SearchGraphRow{}, fmt.Errorf("reference edge: %w", err)
	}
	return lattice.SearchGraphRow{NodeID: source, TargetID: target, EdgeID: edge}, nil
}

func uint64Value(value lattice.Value) (uint64, error) {
	switch value := value.(type) {
	case uint64:
		return value, nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("negative ID %d", value)
		}
		return uint64(value), nil
	case int:
		if value < 0 {
			return 0, fmt.Errorf("negative ID %d", value)
		}
		return uint64(value), nil
	default:
		return 0, fmt.Errorf("unexpected ID type %T", value)
	}
}

func sameGraphRows(got, want []lattice.SearchGraphRow) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].NodeID != want[i].NodeID || got[i].TargetID != want[i].TargetID || got[i].EdgeID != want[i].EdgeID || math.Float32bits(got[i].Value) != math.Float32bits(want[i].Value) {
			return false
		}
	}
	return true
}

func recallAtK(nodeIDs []uint64, idByNode map[uint64]string, judgments map[string]int, k uint32) (float64, int) {
	if len(judgments) == 0 {
		return 0, 0
	}
	found := 0
	for i, nodeID := range nodeIDs {
		if uint32(i) == k {
			break
		}
		docID, ok := idByNode[nodeID]
		if ok && judgments[docID] > 0 {
			found++
		}
	}
	return float64(found) / float64(len(judgments)), len(judgments)
}

func searchGraphOptions(text, edgeType, targetLabel string, candidateK, resultK uint32, maxRows, maxWork, maxBytes uint64) lattice.SearchGraphOptions {
	return lattice.SearchGraphOptions{
		FTS: &lattice.SearchGraphFTS{
			Text:     text,
			Scoring:  lattice.FTSScoringBM25,
			Analyzer: lattice.FTSAnalyzerStandard,
		},
		Expand: &lattice.SearchGraphExpansion{
			EdgeType:    edgeType,
			TargetLabel: targetLabel,
		},
		CandidateK: candidateK,
		ResultK:    resultK,
		MaxRows:    maxRows,
		MaxWork:    maxWork,
		MaxBytes:   maxBytes,
	}
}

func minimumAcceptedBudget(high uint64, accepts func(uint64) (bool, error)) (uint64, error) {
	ok, err := accepts(high)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("maximum budget does not reproduce the reference rows")
	}
	low := uint64(1)
	for low < high {
		mid := low + (high-low)/2
		ok, err = accepts(mid)
		if err != nil {
			return 0, err
		}
		if ok {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low, nil
}

func measureComposition(docs, queries []record, opts compositionOptions) (out compositionOutput, err error) {
	if len(docs) != 5183 || len(queries) != 300 {
		return out, fmt.Errorf("expected 5183 SciFact documents and 300 queries, got %d and %d", len(docs), len(queries))
	}
	if opts.CandidateK == 0 || opts.ResultK == 0 || opts.Repeats < 1 {
		return out, fmt.Errorf("candidate-k, result-k, and repeats must be positive")
	}
	if opts.EdgeType == "" || opts.TargetLabel == "" {
		return out, fmt.Errorf("edge type and target label are required")
	}
	if opts.DBPath == "" || opts.DBPath == ":memory:" {
		return out, fmt.Errorf("new disk path required")
	}
	if _, err = os.Lstat(opts.DBPath); !os.IsNotExist(err) {
		return out, fmt.Errorf("database path must not exist: %s", opts.DBPath)
	}
	qrels, err := loadQrels(opts.QrelsPath)
	if err != nil {
		return out, err
	}

	const maxRows = uint64(1 << 20)
	const maxWork = uint64(1 << 40)
	const maxBytes = uint64(256 << 20)
	started := time.Now()
	db, err := lattice.Open(opts.DBPath, lattice.OpenOptions{Create: true})
	if err != nil {
		return out, err
	}
	defer func() {
		if db != nil {
			if closeErr := db.Close(); err == nil {
				err = closeErr
			}
		}
	}()
	idByNode, err := buildCompositionGraph(db, docs, opts.EdgeType, opts.TargetLabel)
	if err != nil {
		return out, err
	}
	if err = db.Close(); err != nil {
		return out, err
	}
	db = nil
	out = compositionOutput{
		Mode:        "scifact-composed-bm25",
		Documents:   len(docs),
		QueryCount:  len(queries),
		Repeats:     opts.Repeats,
		CandidateK:  opts.CandidateK,
		ResultK:     opts.ResultK,
		EdgeType:    opts.EdgeType,
		TargetLabel: opts.TargetLabel,
		BuildNS:     time.Since(started).Nanoseconds(),
		MaxRows:     maxRows,
		MaxWork:     maxWork,
		MaxBytes:    maxBytes,
		Reference:   "direct FTSSearchContext BM25 candidates plus target-label-filtered Cypher join",
		Queries:     make([]compositionQuery, len(queries)),
	}
	started = time.Now()
	db, err = lattice.Open(opts.DBPath, lattice.OpenOptions{})
	if err != nil {
		return out, err
	}
	out.ReopenNS = time.Since(started).Nanoseconds()

	ctx := context.Background()
	directOptions := lattice.FTSSearchOptions{
		Limit:    opts.CandidateK,
		Scoring:  lattice.FTSScoringBM25,
		Analyzer: lattice.FTSAnalyzerStandard,
		MaxWork:  maxWork,
		MaxBytes: maxBytes,
	}
	adjacency, err := referenceAdjacency(ctx, db, opts.EdgeType, opts.TargetLabel)
	if err != nil {
		return out, err
	}
	cases := make([]compositionCase, len(queries))
	for i, query := range queries {
		direct, err := db.FTSSearchContext(ctx, query.Text, directOptions)
		if err != nil {
			return out, fmt.Errorf("direct query %s: %w", query.ID, err)
		}
		judgments, ok := qrels[query.ID]
		if !ok || len(judgments) == 0 {
			return out, fmt.Errorf("query %s has no qrels", query.ID)
		}
		for docID := range judgments {
			if !containsDocID(idByNode, docID) {
				return out, fmt.Errorf("query %s qrels reference unknown document %s", query.ID, docID)
			}
		}
		expected := referenceJoin(direct, adjacency, opts.ResultK)
		nodeIDs := make([]uint64, len(direct))
		for j, hit := range direct {
			nodeIDs[j] = hit.NodeID
		}
		recall, positives := recallAtK(nodeIDs, idByNode, judgments, opts.CandidateK)
		cases[i] = compositionCase{query: query, direct: direct, expected: expected, recall: recall, positives: positives}
	}

	baseOptions := func(c compositionCase) lattice.SearchGraphOptions {
		return searchGraphOptions(c.query.Text, opts.EdgeType, opts.TargetLabel, opts.CandidateK, opts.ResultK, maxRows, maxWork, maxBytes)
	}
	for i := range cases {
		got, err := db.SearchGraphContext(ctx, baseOptions(cases[i]))
		if err != nil {
			return out, fmt.Errorf("warmup query %s: %w", cases[i].query.ID, err)
		}
		if !sameGraphRows(got.Rows, cases[i].expected) {
			return out, fmt.Errorf("query %s composition mismatch\n got: %#v\nwant: %#v", cases[i].query.ID, got.Rows, cases[i].expected)
		}
	}

	var elapsed []int64
	matched := 0
	var totalAlloc uint64
	for repeat := 0; repeat < opts.Repeats; repeat++ {
		for i := range cases {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			started = time.Now()
			got, err := db.SearchGraphContext(ctx, baseOptions(cases[i]))
			duration := time.Since(started).Nanoseconds()
			runtime.ReadMemStats(&after)
			if err != nil {
				return out, fmt.Errorf("measured query %s repeat %d: %w", cases[i].query.ID, repeat, err)
			}
			match := sameGraphRows(got.Rows, cases[i].expected)
			if repeat == 0 {
				out.Queries[i] = compositionQuery{
					QueryID:     cases[i].query.ID,
					RecallAtK:   cases[i].recall,
					Candidates:  len(cases[i].direct),
					Positives:   cases[i].positives,
					Rows:        len(cases[i].expected),
					MatchedRows: match,
				}
			}
			if !match {
				out.Queries[i].MatchedRows = false
			}
			allocation := after.TotalAlloc - before.TotalAlloc
			out.Queries[i].Measurements = append(out.Queries[i].Measurements, compositionMeasurement{Repeat: repeat, ElapsedNS: duration, AllocBytes: allocation})
			elapsed = append(elapsed, duration)
			totalAlloc += allocation
		}
	}
	for i := range out.Queries {
		if out.Queries[i].MatchedRows {
			matched++
		}
		out.Summary.MeanRecallAtK += out.Queries[i].RecallAtK
	}
	out.Summary.Queries = len(out.Queries)
	out.Summary.MatchedQueries = matched
	out.Summary.MeanRecallAtK /= float64(len(out.Queries))
	out.Summary.TotalAllocBytes = totalAlloc
	out.Summary.MeanAllocBytes = float64(totalAlloc) / float64(len(elapsed))
	for _, duration := range elapsed {
		out.Summary.TotalElapsedNS += duration
	}
	out.Summary.MeanElapsedNS = float64(out.Summary.TotalElapsedNS) / float64(len(elapsed))
	sortedElapsed := append([]int64(nil), elapsed...)
	sort.Slice(sortedElapsed, func(i, j int) bool { return sortedElapsed[i] < sortedElapsed[j] })
	out.Summary.P95ElapsedNS = sortedElapsed[(len(sortedElapsed)*95+99)/100-1]
	if matched != len(out.Queries) {
		return out, fmt.Errorf("composition rows changed during measurement: %d/%d matched", matched, len(out.Queries))
	}

	sample := 0
	for i := range cases {
		if len(cases[i].expected) > len(cases[sample].expected) {
			sample = i
		}
	}
	expected := cases[sample].expected
	accept := func(maxWork, maxBytes uint64) (bool, error) {
		result, err := db.SearchGraphContext(ctx, searchGraphOptions(cases[sample].query.Text, opts.EdgeType, opts.TargetLabel, opts.CandidateK, opts.ResultK, maxRows, maxWork, maxBytes))
		if err != nil {
			return false, nil
		}
		return sameGraphRows(result.Rows, expected), nil
	}
	minWork, err := minimumAcceptedBudget(maxWork, func(budget uint64) (bool, error) { return accept(budget, maxBytes) })
	if err != nil {
		return out, fmt.Errorf("work budget sample: %w", err)
	}
	minBytes, err := minimumAcceptedBudget(maxBytes, func(budget uint64) (bool, error) { return accept(maxWork, budget) })
	if err != nil {
		return out, fmt.Errorf("byte budget sample: %w", err)
	}
	out.Budget = compositionBudgetSample{
		QueryID:    cases[sample].query.ID,
		Rows:       len(expected),
		MinWork:    minWork,
		MinBytes:   minBytes,
		MaxWork:    maxWork,
		MaxBytes:   maxBytes,
		MaxRows:    maxRows,
		SearchCall: "DB.SearchGraphContext",
	}
	return out, nil
}

func containsDocID(idByNode map[uint64]string, docID string) bool {
	for _, candidate := range idByNode {
		if candidate == docID {
			return true
		}
	}
	return false
}
