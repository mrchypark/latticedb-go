package latticedb_test

import (
	"context"
	"errors"
	latticedb "github.com/mrchypark/latticedb-go"
	"testing"
)

func TestQueryExpressionScratchLifetime(t *testing.T) {
	db, err := latticedb.Open(t.TempDir(), latticedb.OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *latticedb.Tx) error {
		for i := 0; i < 100; i++ {
			if _, err := tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Big"}, Properties: map[string]any{"blob": make([]byte, 8192)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query string
		rows  int
		want  any
	}{
		{"MATCH (n:Big) RETURN size(n.blob) AS value", 100, int64(8192)},
		{"MATCH (n:Big) WITH size(n.blob) AS value RETURN value", 100, int64(8192)},
		{"MATCH (n:Big) RETURN sum(size(n.blob)) AS value", 1, float64(819200)},
		{"MATCH (n:Big) WITH sum(size(n.blob)) AS value RETURN value", 1, float64(819200)},
		{"MATCH (n:Big) RETURN min(size(n.blob)) AS value", 1, int64(8192)},
		{"MATCH (n:Big) RETURN max(size(n.blob)) AS value", 1, int64(8192)},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r, err := db.QueryContext(context.Background(), tc.query, nil, latticedb.QueryOptions{MaxBytes: 256 << 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != tc.rows {
				t.Fatalf("rows=%d", len(r.Rows))
			}
			for _, row := range r.Rows {
				if row["value"] != tc.want {
					t.Fatalf("value=%v", row["value"])
				}
			}
		})
	}
	for _, query := range []string{
		"MATCH (n:Big) RETURN n.blob AS blob, count(*) AS count",
		"MATCH (n:Big) WITH n.blob AS blob, count(*) AS count RETURN blob, count",
		"MATCH (n:Big) RETURN min(n.blob) AS blob",
		"MATCH (n:Big) WITH max(n.blob) AS blob RETURN blob",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := db.QueryContext(context.Background(), query, nil, latticedb.QueryOptions{MaxBytes: 256 << 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]["blob"].([]byte)) != 8192 {
				t.Fatalf("result=%v", result)
			}
			if count, ok := result.Rows[0]["count"]; ok && count != int64(100) {
				t.Fatalf("count=%v", count)
			}
		})
	}
	// Retained collection payloads and a single oversized temporary must still fail.
	for _, query := range []string{
		"MATCH (n:Big) RETURN collect(n.blob) AS value",
		"MATCH (n:Big) WITH collect(n.blob) AS value RETURN value",
	} {
		if _, err := db.QueryContext(context.Background(), query, nil, latticedb.QueryOptions{MaxBytes: 256 << 10}); !errors.Is(err, latticedb.ErrResourceLimit) {
			t.Fatalf("%s: %v", query, err)
		}
	}
	if _, err := db.QueryContext(context.Background(), "MATCH (n:Big) RETURN size(n.blob) AS value LIMIT 1", nil, latticedb.QueryOptions{MaxBytes: 4096}); !errors.Is(err, latticedb.ErrResourceLimit) {
		t.Fatalf("oversized temporary: %v", err)
	}
}
