package engine

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestOptionalMatchContract(t *testing.T) {
	for _, backend := range []struct {
		name string
		page bool
	}{{"memory", false}, {"page", true}} {
		t.Run(backend.name, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true, PageStorage: backend.page})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			err = db.Update(func(tx *Tx) error {
				for _, opts := range []CreateNodeOptions{
					{Labels: []string{"Person"}, Properties: map[string]any{"name": "A"}},
					{Labels: []string{"Person"}, Properties: map[string]any{"name": "B"}},
					{Labels: []string{"Item"}, Properties: map[string]any{"tag": nil}},
					{Labels: []string{"Item"}},
				} {
					if _, err := tx.CreateNode(opts); err != nil {
						return err
					}
				}
				for _, id := range []uint64{3, 3, 4} {
					if _, err := tx.CreateEdge(1, id, "LINK", CreateEdgeOptions{}); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			cases := []struct {
				name, query string
				want        []any
			}{
				{"multiplicity", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) RETURN id(x) AS v ORDER BY p.name, v`, []any{int64(3), int64(3), int64(4), nil}},
				{"missing-and-null", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) RETURN x.tag AS v`, []any{nil, nil, nil, nil}},
				{"explicit-null-pattern", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item {tag: null}) RETURN id(x) AS v ORDER BY p.name, v`, []any{nil, nil}},
				{"null-equality", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) WHERE x.tag = null RETURN id(x) AS v`, []any{nil, nil}},
				{"attached-filter", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) WHERE id(x) = 99 RETURN id(x) AS v`, []any{nil, nil}},
				{"outer-filter", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) WITH x AS y WHERE id(y) = 3 RETURN id(y) AS v`, []any{int64(3), int64(3)}},
				{"chained-null", `MATCH (p:Person) WHERE p.name = 'B' OPTIONAL MATCH (p)-[:LINK]->(x) OPTIONAL MATCH (x)<-[:LINK]-(q) RETURN id(q) AS v`, []any{nil}},
				{"mandatory-null", `MATCH (p:Person) WHERE p.name = 'B' OPTIONAL MATCH (p)-[:LINK]->(x) MATCH (x:Item) RETURN id(x) AS v`, nil},
				{"empty-input", `MATCH (p:Absent) OPTIONAL MATCH (p)-[:LINK]->(x) RETURN id(x) AS v`, nil},
				{"leading", `OPTIONAL MATCH (p:Absent) RETURN id(p) AS v`, []any{nil}},
				{"exists", `MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LINK]->(x:Item) } RETURN p.name AS v`, []any{"A"}},
				{"not-exists", `MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:LINK]->(x:Item) } RETURN p.name AS v`, []any{"B"}},
				{"exists-null", `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x:Item) WITH p, x WHERE NOT EXISTS { MATCH (x:Item) } RETURN p.name AS v`, []any{"B"}},
				{"exists-unrelated-null", `MATCH (p:Person) WHERE p.name = 'B' OPTIONAL MATCH (p)-[:LINK]->(x:Item) WITH p, x WHERE EXISTS { MATCH (p:Person) } RETURN id(x) AS v`, []any{nil}},
				{"exists-alias", `MATCH (p:Person) WITH p AS q WHERE EXISTS { MATCH (q)-[:LINK]->(x:Item) } RETURN q.name AS v`, []any{"A"}},
				{"exists-empty-input", `MATCH (p:Absent) WHERE EXISTS { MATCH (p) } RETURN id(p) AS v`, nil},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					check := func(result QueryResult, err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
						var got []any
						for _, row := range result.Rows {
							got = append(got, row["v"])
						}
						if !reflect.DeepEqual(got, tc.want) {
							t.Fatalf("rows = %v, want %v", got, tc.want)
						}
					}
					check(db.Query(tc.query, nil))
					check(db.Query(tc.query, nil))
					if err := db.View(func(tx *Tx) error { check(tx.Query(tc.query, nil)); return nil }); err != nil {
						t.Fatal(err)
					}
				})
			}
			for _, query := range []string{
				`MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK*1..2]->(x) RETURN x`,
				`MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x) WITH p SET p.name = 'changed' RETURN p`,
				`MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x) WITH p RETURN x`,
				`MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LINK]->(x) } RETURN x`,
				`MATCH (p:Person) WITH p AS q WHERE EXISTS { MATCH (q) WHERE p.name = 'A' } RETURN q`,
				`MATCH (p:Person) WHERE EXISTS { MATCH (p) RETURN p } RETURN p`,
				`MATCH (p:Person) WHERE EXISTS { MATCH (p) WHERE EXISTS { MATCH (p) } } RETURN p`,
				`MATCH (p:Person) WHERE EXISTS { MATCH (p) } WITH p SET p.name = 'changed' RETURN p`,
			} {
				if _, err := db.Query(query, nil); err == nil {
					t.Fatalf("accepted unsupported query %q", query)
				}
			}
			result, err := db.Query(`MATCH (p:Person) WHERE p.name = 'changed' RETURN p`, nil)
			if err != nil || len(result.Rows) != 0 {
				t.Fatalf("rejected mutation changed data: %v, %v", result, err)
			}
			query := `MATCH (p:Person) OPTIONAL MATCH (p)-[:LINK]->(x) RETURN x`
			if _, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxRows: 2}); !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("row budget: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := db.QueryContext(ctx, query, nil, QueryOptions{}); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
		})
	}
}
