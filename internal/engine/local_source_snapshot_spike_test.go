package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type probeSourceDecision string

const (
	probeSameSnapshot    probeSourceDecision = "same_snapshot"
	probeOtherDatabase   probeSourceDecision = "different_database"
	probeOtherCommit     probeSourceDecision = "different_commit"
	probeUnverified      probeSourceDecision = "unverified_index"
	probeNoArbitraryAsOf probeSourceDecision = "unsupported_as_of"
)

// This is the spike's admission rule, not a production source adapter.
func compareProbeSources(graph QueryExplanation, search SearchGraphResult, indexVerified bool, asOf *uint64) probeSourceDecision {
	switch {
	case asOf != nil && *asOf != graph.CommitID:
		return probeNoArbitraryAsOf
	case graph.DatabaseID == "" || search.DatabaseID == "" || !indexVerified:
		return probeUnverified
	case graph.DatabaseID != search.DatabaseID:
		return probeOtherDatabase
	case graph.CommitID != search.CommitID:
		return probeOtherCommit
	default:
		return probeSameSnapshot
	}
}

func TestLocalSourceSnapshotSpike(t *testing.T) {
	ctx := t.Context()
	db, err := Open(filepath.Join(t.TempDir(), "graph"), OpenOptions{Create: true, PageStorage: true, PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var doc, original, replacement uint64
	if err := db.Update(func(tx *Tx) error {
		for i, label := range []string{"Doc", "Target", "Target"} {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{label}})
			if err != nil {
				return err
			}
			switch i {
			case 0:
				doc = node.ID
			case 1:
				original = node.ID
			case 2:
				replacement = node.ID
			}
		}
		if err := tx.FTSIndex(doc, "needle"); err != nil {
			return err
		}
		_, err := tx.CreateEdge(doc, original, "LINK", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	old, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Rollback()
	ready, err := old.graph.PageBase.FTSIndexReady(manualFTSPageIndex(FTSAnalyzerStandard))
	if err != nil || !ready || !old.graph.PageBase.SearchIndexesCurrent {
		t.Fatalf("expected ready persisted FTS postings: %v, %v", ready, err)
	}

	const graphQuery = "MATCH (d:Doc)-[:LINK]->(t:Target) RETURN id(t) AS target"
	options := SearchGraphOptions{
		FTS:        &SearchGraphFTS{Text: "needle", Scoring: FTSScoringBM25},
		Expand:     &SearchGraphExpansion{EdgeType: "LINK", TargetLabel: "Target"},
		CandidateK: 1, ResultK: 1,
	}
	originalPlan, err := old.ExplainContext(ctx, graphQuery, nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.FTSIndex(doc, "replacement"); err != nil {
			return err
		}
		if err := tx.DeleteEdge(doc, original, "LINK"); err != nil {
			return err
		}
		_, err := tx.CreateEdge(doc, replacement, "LINK", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	oldGraphRows, err := old.QueryContext(ctx, graphQuery, nil, QueryOptions{})
	if err != nil || len(oldGraphRows.Rows) != 1 || oldGraphRows.Rows[0]["target"] != int64(original) {
		t.Fatalf("old canonical adjacency: %+v, %v", oldGraphRows, err)
	}
	oldSearch, err := old.SearchGraphContext(ctx, options)
	if err != nil || len(oldSearch.Rows) != 1 || oldSearch.Rows[0].TargetID != original ||
		compareProbeSources(originalPlan, oldSearch, ready, nil) != probeSameSnapshot {
		t.Fatalf("old indexed result must match old adjacency: %+v, %v", oldSearch, err)
	}
	newSearch, err := db.SearchGraphContext(ctx, options)
	if err != nil || len(newSearch.Rows) != 0 {
		t.Fatalf("old FTS term survived in new head: %+v, %v", newSearch, err)
	}
	options.FTS.Text = "replacement"
	newSearch, err = db.SearchGraphContext(ctx, options)
	if err != nil || len(newSearch.Rows) != 1 || newSearch.Rows[0].TargetID != replacement {
		t.Fatalf("new head search must follow new adjacency: %+v, %v", newSearch, err)
	}
	if got := compareProbeSources(originalPlan, newSearch, ready, nil); got != probeOtherCommit {
		t.Fatalf("old graph joined to head search: %s", got)
	}

	other, err := Open(":memory:", OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	otherPlan, err := other.ExplainContext(ctx, "RETURN 1 AS marker", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if otherPlan.CommitID != originalPlan.CommitID || otherPlan.DatabaseID == originalPlan.DatabaseID {
		t.Fatalf("expected equal commit numbers in distinct databases: original=%+v other=%+v", originalPlan, otherPlan)
	}
	if got := compareProbeSources(otherPlan, oldSearch, true, nil); got != probeOtherDatabase {
		t.Fatalf("foreign database accepted: %s", got)
	}
	if got := compareProbeSources(originalPlan, oldSearch, false, nil); got != probeUnverified {
		t.Fatalf("unverified watermark accepted: %s", got)
	}
	newPlan, err := db.ExplainContext(ctx, graphQuery, nil, QueryOptions{})
	if err != nil || newPlan.CommitID != newSearch.CommitID || newPlan.CommitID <= originalPlan.CommitID {
		t.Fatalf("expected a newer head for historical comparison: %+v, %v", newPlan, err)
	}
	asOf := originalPlan.CommitID
	if got := compareProbeSources(newPlan, oldSearch, ready, &asOf); got != probeNoArbitraryAsOf {
		t.Fatalf("arbitrary historical request accepted: %s", got)
	}

	options.MaxRows = 2 // One FTS candidate plus one row fit; expansion costs another row.
	onlySearch := options
	onlySearch.Expand = nil
	if got, err := db.SearchGraphContext(ctx, onlySearch); err != nil || len(got.Rows) != 1 {
		t.Fatalf("standalone acquisition should fit: %+v, %v", got, err)
	}
	if got, err := db.SearchGraphContext(ctx, options); !errors.Is(err, ErrResourceLimit) || len(got.Rows) != 0 {
		t.Fatalf("composition must exhaust the shared limit without rows: %+v, %v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if got, err := old.SearchGraphContext(canceled, options); !errors.Is(err, context.Canceled) || len(got.Rows) != 0 {
		t.Fatalf("canceled composition: %+v, %v", got, err)
	}
}
