package store

import (
	"context"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestPagePropertyIndexesBuildUpdateReopenAndFork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	for _, record := range []*NodeRecord{
		{ID: 1, Labels: []string{"Item"}, Properties: PropertiesFromMap(map[string]any{"name": "same"})},
		{ID: 2, Labels: []string{"Item"}, Properties: PropertiesFromMap(map[string]any{"name": "same"})},
		{ID: 3, Labels: []string{"Other"}, Properties: PropertiesFromMap(map[string]any{"name": "same"})},
	} {
		if err := graph.PutNode(record); err != nil {
			t.Fatal(err)
		}
	}
	definition := PropertyIndexDefinition{Scope: "Item", Property: "name"}
	if err := graph.CreatePropertyIndex(context.Background(), true, definition); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph = &PageGraph{Tx: tx}
	indexes, err := graph.LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	ids, found, err := indexes.Lookup(definition, "same")
	if err != nil || !found || !slices.Equal(ids, []uint64{1, 2}) {
		t.Fatalf("reopened lookup = %v, %v, %v", ids, found, err)
	}
	if count, found, err := indexes.Cardinality(definition, "same"); err != nil || !found || count != 2 {
		t.Fatalf("cardinality = %d, %v, %v", count, found, err)
	}
	limited, found, err := indexes.LookupLimit(definition, "same", 1)
	if err != nil || !found || !slices.Equal(limited, []uint64{1}) {
		t.Fatalf("limit = %v, %v, %v", limited, found, err)
	}
	var visited []uint64
	if found, err := indexes.Visit(definition, "same", func(id uint64) bool { visited = append(visited, id); return true }); err != nil || !found || !slices.Equal(visited, ids) {
		t.Fatalf("visit = %v, %v, %v", visited, found, err)
	}

	old, err := graph.GetNode(1)
	if err != nil {
		t.Fatal(err)
	}
	updated := &NodeRecord{ID: 1, Labels: []string{"Other"}, Properties: PropertiesFromMap(map[string]any{"name": "new"})}
	if err := graph.PutNode(updated); err != nil {
		t.Fatal(err)
	}
	if err := graph.UpdateNodePropertyIndexes(old, updated); err != nil {
		t.Fatal(err)
	}
	fork := indexes.Fork()
	if err := fork.Remove(definition, "same", 2); err != nil {
		t.Fatal(err)
	}
	if err := fork.Add(definition, "same", 1); err != nil {
		t.Fatal(err)
	}
	baseIDs, _, err := indexes.Lookup(definition, "same")
	if err != nil || !slices.Equal(baseIDs, []uint64{2}) {
		t.Fatalf("base fork view = %v, %v", baseIDs, err)
	}
	forkIDs, _, err := fork.Lookup(definition, "same")
	if err != nil || len(forkIDs) != 0 {
		t.Fatalf("fork view = %v, %v", forkIDs, err)
	}
	backend := pagePropertyBackend{graph: graph, node: true}
	if err := backend.remove(definition, "same", 2); err != nil {
		t.Fatal(err)
	}
	addedFork := indexes.Fork()
	if err := addedFork.Add(definition, "same", 2); err != nil {
		t.Fatal(err)
	}
	addedIDs, _, err := addedFork.Lookup(definition, "same")
	if err != nil || !slices.Equal(addedIDs, []uint64{2}) {
		t.Fatalf("added overlay = %v, %v", addedIDs, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err = (&PageGraph{Tx: tx}).LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	ids, _, err = indexes.Lookup(definition, "same")
	if err != nil || !slices.Equal(ids, []uint64{1, 2}) {
		t.Fatalf("rollback lookup = %v, %v", ids, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph = &PageGraph{Tx: tx}
	old, err = graph.GetNode(1)
	if err != nil {
		t.Fatal(err)
	}
	updated = &NodeRecord{ID: 1, Labels: []string{"Item"}, Properties: PropertiesFromMap(map[string]any{"name": "changed"})}
	if err := graph.PutNode(updated); err != nil {
		t.Fatal(err)
	}
	if err := graph.UpdateNodePropertyIndexes(old, updated); err != nil {
		t.Fatal(err)
	}
	deleted, err := graph.GetNode(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.DeleteNode(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if err := graph.UpdateNodePropertyIndexes(deleted, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err = (&PageGraph{Tx: tx}).LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if ids, _, err := indexes.Lookup(definition, "changed"); err != nil || !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("updated lookup = %v, %v", ids, err)
	}
	if ids, _, err := indexes.Lookup(definition, "same"); err != nil || len(ids) != 0 {
		t.Fatalf("deleted lookup after reopen = %v, %v", ids, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPagePropertyIndexDeleteAndReadError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	definition := PropertyIndexDefinition{Scope: "Item", Property: "value"}
	record := &NodeRecord{ID: 7, Labels: []string{"Item"}, Properties: PropertiesFromMap(map[string]any{"value": []byte("payload")})}
	if err := graph.PutNode(record); err != nil {
		t.Fatal(err)
	}
	if err := graph.CreatePropertyIndex(context.Background(), true, definition); err != nil {
		t.Fatal(err)
	}
	indexes, err := graph.LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if ids, _, err := indexes.Lookup(definition, []byte("payload")); err != nil || !reflect.DeepEqual(ids, []uint64{7}) {
		t.Fatalf("bytes lookup = %v, %v", ids, err)
	}
	if err := graph.DeleteNode(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if err := graph.UpdateNodePropertyIndexes(record, nil); err != nil {
		t.Fatal(err)
	}
	if ids, _, err := indexes.Lookup(definition, []byte("payload")); err != nil || len(ids) != 0 {
		t.Fatalf("deleted lookup = %v, %v", ids, err)
	}
	if err := graph.DropPropertyIndex(context.Background(), true, definition); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := (&PageGraph{Tx: tx}).LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Has(definition) {
		t.Fatal("dropped index survived reopen")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := indexes.Lookup(definition, []byte("payload")); err == nil || !found {
		t.Fatalf("closed transaction lookup found=%v err=%v", found, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageEdgePropertyIndexUpdates(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages.db"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	for _, id := range []uint64{1, 2} {
		if err := graph.PutNode(&NodeRecord{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	definition := PropertyIndexDefinition{Scope: "LINK", Property: "weight"}
	old := &EdgeRecord{ID: 10, SourceID: 1, TargetID: 2, Type: "LINK", Properties: PropertiesFromMap(map[string]any{"weight": int64(5)})}
	if err := graph.PutEdge(old); err != nil {
		t.Fatal(err)
	}
	if err := graph.CreatePropertyIndex(context.Background(), false, definition); err != nil {
		t.Fatal(err)
	}
	indexes, err := graph.LoadPropertyIndexes(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if ids, _, err := indexes.Lookup(definition, int64(5)); err != nil || !slices.Equal(ids, []uint64{10}) {
		t.Fatalf("edge lookup = %v, %v", ids, err)
	}
	updated := &EdgeRecord{ID: 10, SourceID: 1, TargetID: 2, Type: "OTHER", Properties: old.Properties}
	if err := graph.PutEdge(updated); err != nil {
		t.Fatal(err)
	}
	if err := graph.UpdateEdgePropertyIndexes(old, updated); err != nil {
		t.Fatal(err)
	}
	if ids, _, err := indexes.Lookup(definition, int64(5)); err != nil || len(ids) != 0 {
		t.Fatalf("edge type update lookup = %v, %v", ids, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPagePropertyLookupLimitStopsBeforeOverlayTail(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages.db"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	for _, id := range []uint64{2, 3, 4} {
		record := &NodeRecord{ID: id, Labels: []string{"Item"}, Properties: PropertiesFromMap(map[string]any{"value": "same"})}
		if err := graph.PutNode(record); err != nil {
			t.Fatal(err)
		}
	}
	definition := PropertyIndexDefinition{Scope: "Item", Property: "value"}
	if err := graph.CreatePropertyIndex(context.Background(), true, definition); err != nil {
		t.Fatal(err)
	}
	indexes, err := graph.LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	backend := pagePropertyBackend{graph: graph, node: true}
	for _, id := range []uint64{3, 4} {
		if err := backend.remove(definition, "same", id); err != nil {
			t.Fatal(err)
		}
		if err := indexes.Add(definition, "same", id); err != nil {
			t.Fatal(err)
		}
	}
	ids, found, err := indexes.LookupLimit(definition, "same", 1)
	if err != nil || !found || !slices.Equal(ids, []uint64{2}) {
		t.Fatalf("limited lookup = %v, %v, %v", ids, found, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
