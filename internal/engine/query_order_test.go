package engine

import (
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestOrderValuesFormTotalOrder(t *testing.T) {
	values := []any{
		math.NaN(), int64(-1), float64(0), int64(1), int64(2), int64(10),
		false, true,
		"11", "a", "b",
		[]byte{0}, []byte{1},
		[]float32{0}, []float32{1},
		[]any{int64(0)}, []any{int64(1)},
		map[string]any{"a": int64(0)}, map[string]any{"a": int64(1)},
		nil,
	}
	for left := range values {
		for right := range values {
			comparison := compareOrderValues(values[left], values[right])
			reverse := compareOrderValues(values[right], values[left])
			if comparison != -reverse {
				t.Fatalf("comparison %d,%d = %d, reverse = %d", left, right, comparison, reverse)
			}
			if left < right && comparison >= 0 {
				t.Fatalf("comparison %d,%d = %d, want negative", left, right, comparison)
			}
		}
	}
	for first := range values {
		for second := first + 1; second < len(values); second++ {
			for third := second + 1; third < len(values); third++ {
				if compareOrderValues(values[first], values[second]) >= 0 || compareOrderValues(values[second], values[third]) >= 0 || compareOrderValues(values[first], values[third]) >= 0 {
					t.Fatalf("order is not transitive at %d, %d, %d", first, second, third)
				}
			}
		}
	}
	if comparison := compareOrderValues(map[string]any{"b": int64(2), "a": int64(1)}, map[string]any{"a": int64(1), "b": int64(2)}); comparison != 0 {
		t.Fatalf("equal maps compare as %d", comparison)
	}
}

func TestQueryOrderValueContract(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-order-values.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := db.Query(`UNWIND $values AS value RETURN value ORDER BY value`, map[string]any{"values": []any{
		map[string]any{"a": int64(1)}, []any{int64(1)}, []float32{1}, []byte{1}, "a", true, false, int64(1), nil,
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]any, len(result.Rows))
	for index, row := range result.Rows {
		got[index] = row["value"]
	}
	want := []any{int64(1), false, true, "a", []byte{1}, []float32{1}, []any{int64(1)}, map[string]any{"a": int64(1)}, nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ORDER BY values = %#v, want %#v", got, want)
	}
}

func TestIndependentMatchPatternsStartWithSmallestLabel(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-order.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad"}, Properties: map[string]any{"id": int64(i)}}); err != nil {
				return err
			}
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"id": int64(100)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (b:Broad), (r:Rare) RETURN r.id AS rid, b.id AS bid ORDER BY rid, bid`
	result, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 102})
	if err != nil {
		t.Fatalf("rare-first query failed: %v", err)
	}
	if len(result.Rows) != 100 {
		t.Fatalf("rows = %d, want 100", len(result.Rows))
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (b:Broad), (r:Rare) RETURN r.id AS rid, b.id AS bid`, nil, QueryOptions{MaxWork: 102}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unordered query unexpectedly succeeded: %v", err)
	}
}

func TestMatchPatternOrderingKeepsConnectedAndMutatingPlans(t *testing.T) {
	plan, err := parseMatchQuery(`MATCH (a:Broad)-[:LINK]->(b:Rare), (c:Rare) RETURN a, b, c ORDER BY a`)
	if err != nil {
		t.Fatal(err)
	}
	tx := &Tx{graph: store.NewGraphState()}
	ordered, err := plan.orderedMatchPatterns(tx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordered, plan.matchPatterns) {
		t.Fatal("connected path was reordered")
	}
	mutating, err := parseMatchQuery(`MATCH (b:Broad), (r:Rare) SET b.active = true RETURN b ORDER BY b`)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := mutating.orderedMatchPatterns(tx, nil); err != nil || !reflect.DeepEqual(got, mutating.matchPatterns) {
		t.Fatalf("mutating patterns reordered: %v", err)
	}
}

func TestMatchPatternOrderingSkipsCrossBindingPropertyCardinality(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-order-property.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for _, name := range []string{"a", "b"} {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"name": name}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "name"); err != nil {
		t.Fatal(err)
	}
	result, err := db.QueryContext(t.Context(), `MATCH (source:Item), (copy:Item {name: source.name}) RETURN source.name AS sourceName, copy.name AS copyName ORDER BY sourceName, copyName`, nil, QueryOptions{MaxWork: 100})
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"sourceName": "a", "copyName": "a"}, {"sourceName": "b", "copyName": "b"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("cross-binding property rows = %#v, want %#v", result.Rows, want)
	}
}

func TestMatchPatternOrderingDoesNotEagerlyRequireUnreachedParameter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-order-missing-param.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateNodePropertyIndex("Item", "name"); err != nil {
		t.Fatal(err)
	}
	result, err := db.QueryContext(t.Context(), `MATCH (x:Missing), (n:Item {name: $missing}) RETURN n ORDER BY n`, nil, QueryOptions{MaxWork: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("unreached parameter rows = %#v, want empty", result.Rows)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (n:Item {name: $missing}), (x:Missing) RETURN n ORDER BY n`, nil, QueryOptions{MaxWork: 10}); err == nil {
		t.Fatal("reached missing parameter unexpectedly succeeded")
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (x:Missing), (n:Item) WHERE id(n) = $missing RETURN n ORDER BY n`, nil, QueryOptions{MaxWork: 10}); err != nil {
		t.Fatalf("unreached binding parameter failed: %v", err)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (n:Item), (x:Missing) WHERE id(n) = $missing RETURN n ORDER BY n`, nil, QueryOptions{MaxWork: 10}); err == nil {
		t.Fatal("reached binding parameter unexpectedly succeeded")
	}
}

func TestMatchPatternOrderingSkipsPagination(t *testing.T) {
	plan, err := parseMatchQuery(`MATCH (b:Broad), (r:Rare), (c:Common) RETURN b, r, c ORDER BY b LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := plan.orderedMatchPatterns(&Tx{graph: store.NewGraphState()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordered, plan.matchPatterns) {
		t.Fatal("paginated patterns were reordered")
	}
}

func TestIndependentMatchPatternsUseWherePropertyCardinality(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-order-where.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"kind": "common", "id": int64(i)}}); err != nil {
				return err
			}
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"kind": "rare", "id": int64(100)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "kind"); err != nil {
		t.Fatal(err)
	}
	result, err := db.QueryContext(t.Context(), `MATCH (b:Item), (r:Item) WHERE r.kind = "rare" RETURN r.id AS rid, b.id AS bid ORDER BY rid, bid`, nil, QueryOptions{MaxWork: 250})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 101 || result.Rows[0]["rid"] != int64(100) {
		t.Fatalf("where cardinality rows = %#v", result.Rows)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (b:Item), (r:Item) WHERE r.kind = "rare" RETURN r.id AS rid, b.id AS bid`, nil, QueryOptions{MaxWork: 250}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("source-order query unexpectedly succeeded: %v", err)
	}
}
