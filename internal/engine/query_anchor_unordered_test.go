package engine

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestSourceIDAnchoredUnorderedTraversal(t *testing.T) {
	for _, disk := range []bool{false, true} {
		name := "resident"
		if disk {
			name = "disk reopen"
		}
		t.Run(name, func(t *testing.T) {
			db, sourceID, edgeIDs := sourceIDAnchoredUnorderedFixture(t, disk)
			defer db.Close()

			query := `MATCH (source)-[edge:LINK]->(target) WHERE id(source) = $id RETURN id(edge) AS edgeID, target.name AS name`
			result, err := db.Query(query, map[string]any{"id": int64(sourceID)})
			if err != nil {
				t.Fatal(err)
			}
			want := []map[string]any{
				{"edgeID": int64(edgeIDs[0]), "name": "third"},
				{"edgeID": int64(edgeIDs[1]), "name": "first"},
				{"edgeID": int64(edgeIDs[2]), "name": "third"},
				{"edgeID": int64(edgeIDs[3]), "name": "second"},
			}
			if !reflect.DeepEqual(result.Rows, want) {
				t.Fatalf("unordered rows = %#v, want %#v", result.Rows, want)
			}

			result, err = db.Query(query+` SKIP $skip LIMIT $limit`, map[string]any{"id": int64(sourceID), "skip": 1, "limit": 2})
			if err != nil {
				t.Fatal(err)
			}
			if want := want[1:3]; !reflect.DeepEqual(result.Rows, want) {
				t.Fatalf("paginated unordered rows = %#v, want %#v", result.Rows, want)
			}

			for _, params := range []map[string]any{
				nil,
				{"id": "not an ID"},
			} {
				if _, err := db.Query(query, params); err == nil {
					t.Fatalf("query accepted ID parameters %#v", params)
				}
			}
			for _, id := range []int64{-1, int64(sourceID + 100)} {
				result, err := db.Query(query, map[string]any{"id": id})
				if err != nil || len(result.Rows) != 0 {
					t.Fatalf("empty source ID %d rows=%#v err=%v", id, result.Rows, err)
				}
			}
		})
	}
}

func sourceIDAnchoredUnorderedFixture(t *testing.T, disk bool) (*DB, uint64, []uint64) {
	t.Helper()
	path := ":memory:"
	opts := OpenOptions{}
	if disk {
		path = filepath.Join(t.TempDir(), "graph")
		opts = OpenOptions{Create: true, PageStorage: true}
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	var sourceID uint64
	edgeIDs := make([]uint64, 0, 4)
	if err := db.Update(func(tx *Tx) error {
		source, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		sourceID = source.ID
		targets := map[string]uint64{}
		for _, name := range []string{"first", "second", "third"} {
			target, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": name}})
			if err != nil {
				return err
			}
			targets[name] = target.ID
		}
		for _, name := range []string{"third", "first", "third", "second"} {
			edge, err := tx.CreateEdge(source.ID, targets[name], "LINK", CreateEdgeOptions{})
			if err != nil {
				return err
			}
			edgeIDs = append(edgeIDs, edge.ID)
		}
		return nil
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if !disk {
		return db, sourceID, edgeIDs
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, sourceID, edgeIDs
}
