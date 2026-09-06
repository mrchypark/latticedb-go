package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestConnectedPathOrderingSeedsSelectiveEndpoint(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "connected-order.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		rare, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"key": "seed"}})
		if err != nil {
			return err
		}
		for i := range 32 {
			broad, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad"}, Properties: map[string]any{"id": int64(i)}})
			if err != nil {
				return err
			}
			if _, err := tx.CreateEdge(broad.ID, rare.ID, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Rare", "key"); err != nil {
		t.Fatal(err)
	}

	query := `MATCH (b:Broad)-[e:LINK]->(r:Rare) WHERE r.key = "seed" RETURN b.id AS bid, id(e) AS eid, r.key AS key ORDER BY id(b), id(e), id(r)`
	plan, err := parseMatchQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		ordered, err := plan.orderedMatchPatterns(tx, nil)
		if err != nil {
			return err
		}
		if len(ordered) != 2 {
			t.Fatalf("ordered patterns = %d, want seed plus edge", len(ordered))
		}
		seed, ok := ordered[0].(nodePattern)
		if !ok || seed.Var != "r" {
			t.Fatalf("first pattern = %#v, want selective r seed", ordered[0])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	forward, err := db.Query(query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(forward.Rows) != 32 {
		t.Fatalf("seeded query rows = %d, want 32", len(forward.Rows))
	}
	reversed, err := db.Query(`MATCH (r:Rare)<-[e:LINK]-(b:Broad) WHERE r.key = "seed" RETURN b.id AS bid, id(e) AS eid, r.key AS key ORDER BY id(b), id(e), id(r)`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forward, reversed) {
		t.Fatalf("directed seed changed result:\nforward=%#v\nreversed=%#v", forward, reversed)
	}
}

func TestConnectedPathOrderingKeepsTiedOrDynamicPlansInSourceOrder(t *testing.T) {
	for _, query := range []string{
		`MATCH (b:Broad)-[e:LINK]->(r:Rare) RETURN b, r ORDER BY b.id`,
		`MATCH (b:Broad)-[e:LINK]->(r:Rare {key: b.id}) RETURN b, r ORDER BY id(b), id(e), id(r)`,
	} {
		plan, err := parseMatchQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		ordered, err := plan.orderedMatchPatterns(&Tx{graph: newPlannerGraph()}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ordered, plan.matchPatterns) {
			t.Fatalf("plan reordered unexpectedly for %q", query)
		}
	}
}

func TestConnectedPathOrderingDefersMissingIndexedEndpointParamOnEmptySource(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "connected-order-missing-param.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateNodePropertyIndex("Rare", "key"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (b:Absent)-[:LINK]->(r:Rare {key: $missing}) RETURN b, r ORDER BY id(b), id(r)`, nil)
	if err != nil {
		t.Fatalf("empty source eagerly evaluated endpoint parameter: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want empty", result.Rows)
	}
}

func TestConnectedPathOrderingDefersInvalidEndpointIDOnEmptySource(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "connected-order-invalid-id.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"key": "seed"}}); err != nil {
			return err
		}
		for range 3 {
			left, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}})
			if err != nil {
				return err
			}
			right, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}})
			if err != nil {
				return err
			}
			if _, err := tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Rare", "key"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (b:Absent)-[:LINK]->(r:Rare {key: "seed"}) WHERE id(r) = "bad" RETURN b, r ORDER BY id(b), id(r)`, nil)
	if err != nil {
		t.Fatalf("empty source eagerly evaluated invalid endpoint ID: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want empty", result.Rows)
	}
}

func TestConnectedPathPlanningConsumesSharedWorkBudget(t *testing.T) {
	plan, err := parseMatchQuery(`MATCH (b:Broad)-[e:LINK]->(r:Rare) RETURN b, r, e ORDER BY id(b), id(e), id(r)`)
	if err != nil {
		t.Fatal(err)
	}
	budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 1})
	defer releaseQueryBudget(budget)
	_, err = plan.orderedMatchPatterns(&Tx{graph: store.NewGraphState()}, nil, budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("planning error = %v, want resource limit", err)
	}
}

func TestConnectedPathPlanningChecksCancellation(t *testing.T) {
	plan, err := parseMatchQuery(`MATCH (b:Broad)-[e:LINK]->(r:Rare) RETURN b, r, e ORDER BY id(b), id(e), id(r)`)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)
	_, err = plan.orderedMatchPatterns(&Tx{graph: store.NewGraphState()}, nil, budget)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("planning error = %v, want cancellation", err)
	}
}

func TestConnectedPathOrderingPreservesMatchMultiplicity(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "connected-order-multiplicity.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		rare, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"key": "seed"}})
		if err != nil {
			return err
		}
		broad, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad"}, Properties: map[string]any{"code": "same"}})
		if err != nil {
			return err
		}
		for range 2 {
			if _, err := tx.CreateEdge(broad.ID, rare.ID, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		second, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad"}, Properties: map[string]any{"code": "same"}})
		if err != nil {
			return err
		}
		if _, err := tx.CreateEdge(second.ID, rare.ID, "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		loop, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad", "Rare"}, Properties: map[string]any{"key": "loop"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(loop.ID, loop.ID, "LOOP", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		for range 3 {
			other, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}})
			if err != nil {
				return err
			}
			if _, err := tx.CreateEdge(other.ID, other.ID, "LOOP", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Rare", "key"); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{
			`MATCH (b:Broad)-[:LINK]->(r:Rare {key: "seed"}) RETURN b.code AS code ORDER BY b.code`,
			`MATCH (b:Broad)-[:LINK]->(r:Rare {key: "seed"}) RETURN b.code AS code ORDER BY id(b), id(r)`,
		},
		{
			`MATCH (b:Broad)-[:LOOP]-(r:Rare {key: "loop"}) RETURN b.key AS key ORDER BY b.key`,
			`MATCH (b:Broad)-[:LOOP]-(r:Rare {key: "loop"}) RETURN b.key AS key ORDER BY id(b), id(r)`,
		},
		{
			`MATCH (b:Broad)-[:LINK]->(r:Rare {key: "seed"}) RETURN id(b) AS bid, id(r) AS rid ORDER BY bid DESC`,
			`MATCH (b:Broad)-[:LINK]->(r:Rare {key: "seed"}) RETURN id(b) AS bid, id(r) AS rid ORDER BY bid DESC, rid DESC`,
		},
	} {
		source, err := db.Query(pair[0], nil)
		if err != nil {
			t.Fatal(err)
		}
		optimized, err := db.Query(pair[1], nil)
		if err != nil {
			t.Fatal(err)
		}
		if !sameResultRowMultiset(source.Rows, optimized.Rows) {
			t.Fatalf("optimized match multiplicity changed:\nsource=%#v\noptimized=%#v", source.Rows, optimized.Rows)
		}
	}
}

func TestConnectedPathOrderingUsesKnownIDDegreeTieBreak(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "connected-order-degree.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var highID, lowID uint64
	if err := db.Update(func(tx *Tx) error {
		high, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		low, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		highID, lowID = high.ID, low.ID
		if _, err := tx.CreateEdge(high.ID, low.ID, "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		for range 16 {
			other, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}})
			if err != nil {
				return err
			}
			if _, err := tx.CreateEdge(high.ID, other.ID, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	plan, err := parseMatchQuery(fmt.Sprintf(`MATCH (a:Item)-[:LINK]-(b:Item) WHERE id(a) = %d AND id(b) = %d RETURN a, b ORDER BY id(a), id(b)`, highID, lowID))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		ordered, err := plan.orderedMatchPatterns(tx, nil)
		if err != nil {
			return err
		}
		seed, ok := ordered[0].(nodePattern)
		if !ok || seed.Var != "b" {
			t.Fatalf("first pattern = %#v, want lower-degree b seed", ordered[0])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func sameResultRowMultiset(left, right []map[string]any) bool {
	counts := map[string]int{}
	for _, row := range left {
		counts[fmt.Sprintf("%#v", row)]++
	}
	for _, row := range right {
		counts[fmt.Sprintf("%#v", row)]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func TestIndependentComponentPlanningChargesStableSort(t *testing.T) {
	plan, err := parseMatchQuery(`MATCH (a:A), (b:B), (c:C) RETURN a, b, c ORDER BY id(a), id(b), id(c)`)
	if err != nil {
		t.Fatal(err)
	}
	budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 16})
	defer releaseQueryBudget(budget)
	_, err = plan.orderedMatchPatterns(&Tx{graph: store.NewGraphState()}, nil, budget)
	if !errors.Is(err, ErrResourceLimit) || budget.work != 16 {
		t.Fatalf("planning error=%v work=%d, want sort-stage resource limit at 16", err, budget.work)
	}
}

func BenchmarkConnectedPathSelectiveSeed100K(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "connected-order-benchmark.ltdb"), OpenOptions{Create: true, WALCheckpointThresholdBytes: 1 << 40})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		rare, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"key": "seed"}})
		if err != nil {
			return err
		}
		common, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Common"}})
		if err != nil {
			return err
		}
		for i := range 100_000 {
			broad, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad"}, Properties: map[string]any{"id": int64(i)}})
			if err != nil {
				return err
			}
			target := common
			if i == 0 {
				target = rare
			}
			if _, err := tx.CreateEdge(broad.ID, target.ID, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Rare", "key"); err != nil {
		b.Fatal(err)
	}
	for _, benchmark := range []struct {
		name  string
		query string
	}{
		{"source_order", `MATCH (b:Broad)-[e:LINK]->(r:Rare) WHERE r.key = "seed" RETURN b.id AS bid ORDER BY b.id`},
		{"optimized_broad_first", `MATCH (b:Broad)-[e:LINK]->(r:Rare) WHERE r.key = "seed" RETURN b.id AS bid ORDER BY id(b), id(e), id(r)`},
		{"optimized_selective_first", `MATCH (r:Rare)<-[e:LINK]-(b:Broad) WHERE r.key = "seed" RETURN b.id AS bid ORDER BY id(b), id(e), id(r)`},
	} {
		_, err := parseMatchQuery(benchmark.query)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(benchmark.name, func(b *testing.B) {
			var rows int
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := db.QueryContext(b.Context(), benchmark.query, nil, QueryOptions{MaxBytes: 128 << 20})
				if err != nil {
					b.Fatal(err)
				}
				rows = len(result.Rows)
			}
			b.ReportMetric(float64(rows), "rows_emitted")
		})
	}
}

func newPlannerGraph() *store.GraphState {
	return store.NewGraphState()
}
