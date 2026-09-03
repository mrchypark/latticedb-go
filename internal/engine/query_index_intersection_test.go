package engine

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestEqualityIndexIntersectionUsesRarePosting(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-index-intersection.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var nodes []Node
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 256; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{
				Labels: []string{"Item"},
				Properties: map[string]any{
					"broad": "all",
					"rare":  "other",
				},
			})
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
		if err := tx.SetProperty(nodes[63].ID, "rare", "target"); err != nil {
			return err
		}
		return tx.SetProperty(nodes[200].ID, "rare", "target")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "broad"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "rare"); err != nil {
		t.Fatal(err)
	}

	query := `MATCH (n:Item) WHERE n.broad = $broad AND n.rare = 'target' RETURN id(n) AS id`
	params := map[string]any{"broad": "all"}
	want := []map[string]any{{"id": int64(nodes[63].ID)}, {"id": int64(nodes[200].ID)}}
	result, err := db.QueryContext(t.Context(), query, params, QueryOptions{MaxWork: 32})
	if err != nil || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed node intersection = %#v, %v; want %#v", result.Rows, err, want)
	}

	if err := db.DropNodePropertyIndex("Item", "broad"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropNodePropertyIndex("Item", "rare"); err != nil {
		t.Fatal(err)
	}
	result, err = db.QueryContext(t.Context(), query, params, QueryOptions{MaxWork: 10000})
	if err != nil || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("unindexed node intersection = %#v, %v; want %#v", result.Rows, err, want)
	}
	if _, err := db.QueryContext(t.Context(), query, params, QueryOptions{MaxWork: 32}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unindexed node query error = %v, want resource limit", err)
	}
}

func TestEdgeEqualityIndexIntersectionUsesRarePosting(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "edge-index-intersection.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var edges []Edge
	if err := db.Update(func(tx *Tx) error {
		from, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		to, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		for i := 0; i < 256; i++ {
			edge, err := tx.CreateEdge(from.ID, to.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{
				"broad": "all",
				"rare":  "other",
			}})
			if err != nil {
				return err
			}
			edges = append(edges, edge)
		}
		if err := tx.SetEdgeProperty(edges[63].ID, "rare", "target"); err != nil {
			return err
		}
		return tx.SetEdgeProperty(edges[200].ID, "rare", "target")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "broad"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "rare"); err != nil {
		t.Fatal(err)
	}

	query := `MATCH ()-[r:LINK]->() WHERE r.broad = 'all' AND r.rare = 'target' RETURN id(r) AS id`
	want := []map[string]any{{"id": int64(edges[63].ID)}, {"id": int64(edges[200].ID)}}
	result, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 32})
	if err != nil || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed edge intersection = %#v, %v; want %#v", result.Rows, err, want)
	}

	if err := db.DropEdgePropertyIndex("LINK", "broad"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropEdgePropertyIndex("LINK", "rare"); err != nil {
		t.Fatal(err)
	}
	result, err = db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 10000})
	if err != nil || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("unindexed edge intersection = %#v, %v; want %#v", result.Rows, err, want)
	}
	if _, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 32}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unindexed edge query error = %v, want resource limit", err)
	}
}

func TestEqualityIndexIntersectionKeepsResidualLimitCandidates(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-index-intersection-residual.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var nodes []Node
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 256; i++ {
			props := map[string]any{"a": "all", "b": "match", "c": "other"}
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: props})
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
		return tx.SetProperty(nodes[200].ID, "c", "target")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "a"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "b"); err != nil {
		t.Fatal(err)
	}

	query := `MATCH (n:Item) WHERE n.a = 'all' AND n.b = 'match' AND n.c = 'target' RETURN id(n) AS id LIMIT 1`
	result, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 4096})
	want := []map[string]any{{"id": int64(nodes[200].ID)}}
	if err != nil || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("indexed residual limit = %#v, %v; want %#v", result.Rows, err, want)
	}
}
