package latticedb

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestPageRejectsUnreadableLargeCollectionBeforeCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("large public write admission boundary")
	}
	payload := make(map[string]any, 700000)
	for i := range 700000 {
		payload[fmt.Sprintf("k%06d", i)] = nil
	}
	for _, kind := range []string{"node", "edge", "stream"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := Open(path, OpenOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.Update(func(tx *Tx) error {
				for range 2 {
					if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *Tx) error {
				switch kind {
				case "node":
					_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"payload": payload}})
					return err
				case "edge":
					_, err := tx.CreateEdge(1, 2, "R", CreateEdgeOptions{Properties: map[string]any{"payload": payload}})
					return err
				default:
					return tx.PublishStream("events", "event", payload)
				}
			})
			if err == nil {
				t.Fatal("unreadable collection committed")
			}
			if err = db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"ok": true}})
				return err
			}); err != nil {
				t.Fatalf("database not usable after rejected write: %v", err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			result, err := db.Query("MATCH (n) RETURN count(n) AS n", nil)
			if err != nil || result.Rows[0]["n"] != int64(3) {
				t.Fatalf("reopen count=%v error=%v", result, err)
			}
		})
	}
}
