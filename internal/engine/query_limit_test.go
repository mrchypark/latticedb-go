package engine

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestIndexedLimitMatchesUnindexedQueries(t *testing.T) {
	openDB := func(indexed bool) *DB {
		db, err := Open(filepath.Join(t.TempDir(), "limit-pushdown.ltdb"), OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *Tx) error {
			for _, node := range []struct {
				labels []string
				props  map[string]any
			}{
				{[]string{"Item"}, map[string]any{"bucket": "order", "value": int64(3)}},
				{[]string{"Item"}, map[string]any{"bucket": "order", "value": int64(1)}},
				{[]string{"Item"}, map[string]any{"bucket": "order", "value": int64(2)}},
				{[]string{"Item"}, map[string]any{"bucket": "labels", "value": "wrong"}},
				{[]string{"Item", "Featured"}, map[string]any{"bucket": "labels", "value": "right"}},
				{[]string{"Item"}, map[string]any{"bucket": "pattern", "kind": "wrong", "value": "wrong"}},
				{[]string{"Item"}, map[string]any{"bucket": "pattern", "kind": "wanted", "value": "right"}},
			} {
				if _, err := tx.CreateNode(CreateNodeOptions{Labels: node.labels, Properties: node.props}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if indexed {
			if err := db.CreateNodePropertyIndex("Item", "bucket"); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}

	indexed, unindexed := openDB(true), openDB(false)
	defer indexed.Close()
	defer unindexed.Close()
	for _, test := range []struct {
		name  string
		query string
		want  []map[string]any
	}{
		{
			name:  "order by",
			query: `MATCH (n:Item) WHERE n.bucket = 'order' RETURN n.value AS value ORDER BY value LIMIT 2`,
			want:  []map[string]any{{"value": int64(1)}, {"value": int64(2)}},
		},
		{
			name:  "extra label",
			query: `MATCH (n:Item:Featured) WHERE n.bucket = 'labels' RETURN n.value AS value LIMIT 1`,
			want:  []map[string]any{{"value": "right"}},
		},
		{
			name:  "pattern property",
			query: `MATCH (n:Item {kind: 'wanted'}) WHERE n.bucket = 'pattern' RETURN n.value AS value LIMIT 1`,
			want:  []map[string]any{{"value": "right"}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			indexedResult, err := indexed.Query(test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			unindexedResult, err := unindexed.Query(test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(indexedResult.Rows, unindexedResult.Rows) {
				t.Fatalf("indexed rows = %#v, unindexed rows = %#v", indexedResult.Rows, unindexedResult.Rows)
			}
			if !reflect.DeepEqual(indexedResult.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", indexedResult.Rows, test.want)
			}
		})
	}
}

func TestIndexedLimitPreservesAggregateInput(t *testing.T) {
	indexed, unindexed := openWithDB(t), openWithDB(t)
	if err := indexed.CreateNodePropertyIndex("Person", "team"); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "WITH coalesce(0) AS seed "} {
		for _, projection := range []string{
			"sum(n.age) AS total",
			"count(*) AS c, sum(n.age) AS total",
			"n.team AS team, sum(n.age) AS total",
			"avg(n.age) AS average, sum(n.age) AS total",
		} {
			query := prefix + "MATCH (n:Person) WHERE n.team = 'red' RETURN " + projection
			expected, err := unindexed.Query(query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(expected.Rows) != 1 || !queryValuesEqual(expected.Rows[0]["total"], int64(70)) {
				t.Fatalf("wrong control: %#v", expected.Rows)
			}
			for _, db := range []*DB{indexed, unindexed} {
				for _, suffix := range []string{"", " LIMIT 1"} {
					result, err := db.Query(query+suffix, nil)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(result.Rows, expected.Rows) {
						t.Fatalf("%s%s: %#v, want %#v", query, suffix, result.Rows, expected.Rows)
					}
				}
			}
		}
	}
}
