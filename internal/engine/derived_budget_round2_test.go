package engine

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestPropertyIndexBudgetCumulativeBoundaryRejectsBeforeWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "boundary.ltdb")
	db, err := Open(path, OpenOptions{Create: true, DerivedIndexBuildMaxWork: 10000, DerivedIndexBuildMaxLogicalBytes: 10000})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}, Properties: map[string]any{"k": "v"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("P", "k"); err != nil {
		t.Fatal(err)
	}
	before := db.commitID
	db.derivedIndexBuildMaxWork = db.graph.DerivedIndexWork
	db.derivedIndexBuildMaxLogicalBytes = db.graph.DerivedIndexLogicalBytes
	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}, Properties: map[string]any{"k": "next"}})
		return err
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("mutation error = %v, want resource limit", err)
	}
	if db.commitID != before {
		t.Fatalf("commit advanced after rejected mutation: %d -> %d", before, db.commitID)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDerivedBudgetMultipleIndexesDropAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexes.ltdb")
	opts := OpenOptions{Create: true, DerivedIndexBuildMaxWork: 100000, DerivedIndexBuildMaxLogicalBytes: 100000}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}, Properties: map[string]any{"a": "a", "b": "b"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("P", "a"); err != nil {
		t.Fatal(err)
	}
	first := db.graph.DerivedIndexLogicalBytes
	if err := db.CreateNodePropertyIndex("P", "b"); err != nil {
		t.Fatal(err)
	}
	second := db.graph.DerivedIndexLogicalBytes
	if second <= first {
		t.Fatal("second index did not increase budget")
	}
	if err := db.DropNodePropertyIndex("P", "a"); err != nil {
		t.Fatal(err)
	}
	if db.graph.DerivedIndexLogicalBytes >= second {
		t.Fatal("drop did not decrease budget")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{DerivedIndexBuildMaxWork: 100000, DerivedIndexBuildMaxLogicalBytes: 100000})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDerivedBudgetReopenAfterLabelEdgeFTSGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "growth.ltdb")
	opts := OpenOptions{Create: true, DerivedIndexBuildMaxWork: 100000, DerivedIndexBuildMaxLogicalBytes: 100000}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		a, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"A"}})
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"B"}})
		if err != nil {
			return err
		}
		if _, err = tx.CreateEdge(a.ID, b.ID, "E", CreateEdgeOptions{}); err != nil {
			return err
		}
		return tx.FTSIndex(a.ID, "alpha beta")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{DerivedIndexBuildMaxWork: 100000, DerivedIndexBuildMaxLogicalBytes: 100000})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
