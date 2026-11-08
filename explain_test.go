package latticedb_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	dbapi "github.com/mrchypark/latticedb-go"
)

func TestExplainPublicSnapshotAndErrors(t *testing.T) {
	for _, backend := range []string{"memory", "page"} {
		t.Run(backend, func(t *testing.T) {
			path := ":memory:"
			if backend == "page" {
				path = filepath.Join(t.TempDir(), "db")
			}
			db, err := dbapi.Open(path, dbapi.OpenOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			read, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			defer read.Rollback()
			const query = "MATCH (n:Doc) RETURN n"
			before, err := read.ExplainContext(ctx, query, nil, dbapi.QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *dbapi.Tx) error {
				_, err := tx.CreateNode(dbapi.CreateNodeOptions{Labels: []string{"Doc"}})
				if err != nil {
					return err
				}
				return tx.PutAppMetadata([]byte("test-key"), []byte("original"))
			}); err != nil {
				t.Fatal(err)
			}
			after, err := read.ExplainContext(ctx, query, nil, dbapi.QueryOptions{})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("old plan changed: before=%+v after=%+v err=%v", before, after, err)
			}
			changes, err := db.Changes(0, 10, 0)
			if err != nil || len(changes) == 0 {
				t.Fatalf("seed changefeed: %v, %v", changes, err)
			}
			current, err := db.ExplainContext(ctx, query, nil, dbapi.QueryOptions{})
			if err != nil || current.SchemaVersion != 1 || current.DatabaseID == "" || current.DatabaseID != before.DatabaseID || current.CommitID != before.CommitID+1 {
				t.Fatalf("expected new snapshot: old=%+v current=%+v err=%v", before, current, err)
			}
			for _, test := range []struct {
				query string
				stage dbapi.QueryErrorStage
			}{
				{"CREATE (n:Doc)", dbapi.QueryErrorStagePlan},
				{"MATCH (", dbapi.QueryErrorStageParse},
			} {
				_, err := db.ExplainContext(ctx, test.query, nil, dbapi.QueryOptions{})
				var queryErr *dbapi.QueryError
				if !errors.As(err, &queryErr) || queryErr.Stage != test.stage {
					t.Fatalf("%q: expected stage %d, got %v", test.query, test.stage, err)
				}
			}
			unchanged, err := db.ExplainContext(ctx, query, nil, dbapi.QueryOptions{})
			if err != nil || !reflect.DeepEqual(unchanged, current) {
				t.Fatalf("explain changed snapshot: before=%+v after=%+v err=%v", current, unchanged, err)
			}
			rows, err := db.Query(query, nil)
			if err != nil || len(rows.Rows) != 1 {
				t.Fatalf("explain changed graph: %v, %v", rows, err)
			}
			if err := db.View(func(tx *dbapi.Tx) error {
				value, found, err := tx.GetAppMetadata([]byte("test-key"))
				if err != nil || !found || string(value) != "original" {
					t.Fatalf("explain changed metadata: %q, %v, %v", value, found, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			afterChanges, err := db.Changes(0, 10, 0)
			if err != nil || !reflect.DeepEqual(changes, afterChanges) {
				t.Fatalf("explain changed changefeed: %v, %v", afterChanges, err)
			}
		})
	}
}
