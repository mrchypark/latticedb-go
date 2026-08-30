package latticedb

import (
	"errors"
	"reflect"
	"testing"
)

func TestGraphAndBatchInsertPublicAPI(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var source, target uint64
	if err := db.Update(func(tx *Tx) error {
		ids, err := tx.BatchInsertVectors("Person", [][]float32{{1, 0}})
		if err != nil {
			return err
		}
		source = ids[0]
		ids, err = tx.BatchInsert("Person", [][]float32{{0, 1}})
		if err != nil {
			return err
		}
		target = ids[0]
		if _, err = tx.CreateEdge(source, target, "KNOWS", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err = tx.CreateEdge(source, target, "KNOWS", CreateEdgeOptions{}); err != nil {
			return err
		}
		_, err = tx.CreateEdge(source, target, "FOLLOWS", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		outgoing, err := tx.GetOutgoingEdgesByType(source, "KNOWS", 0)
		if err != nil || len(outgoing) != 2 || outgoing[0].TargetID != target || outgoing[1].TargetID != target {
			t.Fatalf("unlimited typed outgoing = %#v, %v", outgoing, err)
		}
		incoming, err := tx.GetIncomingEdges(target)
		if err != nil || len(incoming) != 3 {
			t.Fatalf("incoming = %#v, %v", incoming, err)
		}
		incoming, err = tx.GetIncomingEdgesByType(target, "KNOWS", 1)
		if err != nil || len(incoming) != 1 || incoming[0].SourceID != source {
			t.Fatalf("limited typed incoming = %#v, %v", incoming, err)
		}
		vector, ok, err := tx.GetProperty(source, "vector")
		if err != nil || !ok || !reflect.DeepEqual(vector, []float32{1, 0}) {
			t.Fatalf("batch vector = %#v, %v, %v", vector, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.BatchInsertVectors("Person", [][]float32{{1, 0}, {1}}); err == nil {
			t.Fatal("BatchInsertVectors accepted invalid dimensions")
		}
		exists, err := tx.NodeExists(3)
		if err != nil || exists {
			t.Fatalf("invalid batch mutated transaction: exists=%v err=%v", exists, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	readOnly, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Rollback()
	if _, err := readOnly.BatchInsertVectors("Person", [][]float32{{1, 0}}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only batch error = %v", err)
	}
}
