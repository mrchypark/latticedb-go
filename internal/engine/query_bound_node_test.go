package engine

import (
	"context"
	"testing"
)

func TestRepeatedNodeBindingUsesLinearWork(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const count = 40
	if err := db.Update(func(tx *Tx) error {
		for i := range count {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": int64(i)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"MATCH (n:Item), (n:Item) RETURN n.value AS value",
		"MATCH (n:Item), (n) RETURN n.value AS value",
	} {
		result, err := db.QueryContext(context.Background(), query, nil, QueryOptions{MaxWork: count * 8})
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if len(result.Rows) != count {
			t.Fatalf("rows = %d", len(result.Rows))
		}
		seen := map[any]bool{}
		for _, row := range result.Rows {
			seen[row["value"]] = true
		}
		for i := range count {
			if !seen[int64(i)] {
				t.Fatalf("missing value %d", i)
			}
		}
	}
}

func TestRepeatedNodeBindingStillChecksConstraints(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"MATCH (n:Item), (n:Other) RETURN n",
		"MATCH (n:Item), (n {value: 2}) RETURN n",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if len(result.Rows) != 0 {
			t.Fatalf("%s: rows = %v", query, result.Rows)
		}
	}
	if _, err := db.Query("UNWIND $values AS n MATCH (n:Item) RETURN n", map[string]any{"values": []any{int64(1)}}); err == nil {
		t.Fatal("conflicting value/node binding was accepted")
	}
}
