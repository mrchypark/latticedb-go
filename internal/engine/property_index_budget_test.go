package engine

import (
	"path/filepath"
	"testing"
)

func TestPropertyIndexBudgetMatchesDataFirstAndIndexFirstConstruction(t *testing.T) {
	dataFirst, err := Open(filepath.Join(t.TempDir(), "data-first.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer dataFirst.Close()
	addUnindexedPropertyBudgetEntities(t, dataFirst)
	if err := dataFirst.CreateNodePropertyIndex("P", "indexed"); err != nil {
		t.Fatal(err)
	}
	if err := dataFirst.CreateEdgePropertyIndex("LINK", "indexed"); err != nil {
		t.Fatal(err)
	}

	indexFirst, err := Open(filepath.Join(t.TempDir(), "index-first.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer indexFirst.Close()
	if err := indexFirst.CreateNodePropertyIndex("P", "indexed"); err != nil {
		t.Fatal(err)
	}
	if err := indexFirst.CreateEdgePropertyIndex("LINK", "indexed"); err != nil {
		t.Fatal(err)
	}
	addUnindexedPropertyBudgetEntities(t, indexFirst)

	if dataFirst.graph.DerivedIndexWork != indexFirst.graph.DerivedIndexWork || dataFirst.graph.DerivedIndexLogicalBytes != indexFirst.graph.DerivedIndexLogicalBytes {
		t.Fatalf("derived budgets differ: data-first=(%d, %d), index-first=(%d, %d)", dataFirst.graph.DerivedIndexWork, dataFirst.graph.DerivedIndexLogicalBytes, indexFirst.graph.DerivedIndexWork, indexFirst.graph.DerivedIndexLogicalBytes)
	}
}

func TestPropertyIndexCreateDropReturnsDerivedBudgetToBaseline(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "churn.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	addUnindexedPropertyBudgetEntities(t, db)
	baseWork, baseBytes := db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes

	for range 3 {
		if err := db.CreateNodePropertyIndex("P", "indexed"); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateEdgePropertyIndex("LINK", "indexed"); err != nil {
			t.Fatal(err)
		}
		if err := db.DropNodePropertyIndex("P", "indexed"); err != nil {
			t.Fatal(err)
		}
		if err := db.DropEdgePropertyIndex("LINK", "indexed"); err != nil {
			t.Fatal(err)
		}
		if db.graph.DerivedIndexWork != baseWork || db.graph.DerivedIndexLogicalBytes != baseBytes {
			t.Fatalf("derived budget after create/drop = (%d, %d), want (%d, %d)", db.graph.DerivedIndexWork, db.graph.DerivedIndexLogicalBytes, baseWork, baseBytes)
		}
	}
}

func addUnindexedPropertyBudgetEntities(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		left, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}, Properties: map[string]any{"other": "left"}})
		if err != nil {
			return err
		}
		right, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}, Properties: map[string]any{"other": "right"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"other": "edge"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
