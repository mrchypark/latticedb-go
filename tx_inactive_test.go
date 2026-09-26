package latticedb

import (
	"context"
	"errors"
	"testing"
)

func TestTxNilAndZeroValueMethodsReturnInactive(t *testing.T) {
	for _, tx := range []*Tx{nil, {}} {
		for _, call := range txInactiveCalls() {
			t.Run(call.name, func(t *testing.T) {
				assertInactiveTxError(t, call.call(tx))
			})
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() = %v, want nil", err)
		}
		if tx.IsActive() || tx.IsReadOnly() {
			t.Fatalf("zero-value transaction reports active or read-only")
		}
	}
}

func TestTxClosedMethodsReturnInactive(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if !tx.IsActive() || tx.IsReadOnly() {
		t.Fatal("write transaction does not report its active writable state")
	}
	if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
		t.Fatalf("CreateNode on active transaction = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for _, call := range txInactiveCalls() {
		t.Run(call.name, func(t *testing.T) {
			assertInactiveTxError(t, call.call(tx))
		})
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() after Commit = %v, want nil", err)
	}
	if tx.IsActive() {
		t.Fatal("committed transaction reports active")
	}
}

type txInactiveCall struct {
	name string
	call func(*Tx) error
}

func txInactiveCalls() []txInactiveCall {
	return []txInactiveCall{
		{"Commit", func(tx *Tx) error { return tx.Commit() }},
		{"CommitContext", func(tx *Tx) error { return tx.CommitContext(context.Background()) }},
		{"CreateNode", func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }},
		{"DeleteNode", func(tx *Tx) error { return tx.DeleteNode(1) }},
		{"NodeExists", func(tx *Tx) error { _, err := tx.NodeExists(1); return err }},
		{"GetNode", func(tx *Tx) error { _, err := tx.GetNode(1); return err }},
		{"SetProperty", func(tx *Tx) error { return tx.SetProperty(1, "key", "value") }},
		{"GetProperty", func(tx *Tx) error { _, _, err := tx.GetProperty(1, "key"); return err }},
		{"FindNodesByLabelProperty", func(tx *Tx) error { _, err := tx.FindNodesByLabelProperty("Label", "key", "value", 1); return err }},
		{"SetVector", func(tx *Tx) error { return tx.SetVector(1, "embedding", []float32{1}) }},
		{"BatchInsertVectors", func(tx *Tx) error { _, err := tx.BatchInsertVectors("Label", [][]float32{{1}}); return err }},
		{"BatchInsert", func(tx *Tx) error { _, err := tx.BatchInsert("Label", [][]float32{{1}}); return err }},
		{"FTSIndex", func(tx *Tx) error { return tx.FTSIndex(1, "text") }},
		{"FTSIndexContext", func(tx *Tx) error { return tx.FTSIndexContext(context.Background(), 1, "text") }},
		{"PublishStream", func(tx *Tx) error { return tx.PublishStream("stream", "event", nil) }},
		{"PublishStreamGetSequence", func(tx *Tx) error { _, err := tx.PublishStreamGetSequence("stream", "event", nil); return err }},
		{"SetStreamOffset", func(tx *Tx) error { return tx.SetStreamOffset("stream", "consumer", 1) }},
		{"TrimStream", func(tx *Tx) error { return tx.TrimStream("stream", 1) }},
		{"CreateEdge", func(tx *Tx) error { _, err := tx.CreateEdge(1, 2, "REL", CreateEdgeOptions{}); return err }},
		{"GetEdgeProperty", func(tx *Tx) error { _, _, err := tx.GetEdgeProperty(1, "key"); return err }},
		{"FindEdgesByTypeProperty", func(tx *Tx) error { _, err := tx.FindEdgesByTypeProperty("REL", "key", "value", 1); return err }},
		{"SetEdgeProperty", func(tx *Tx) error { return tx.SetEdgeProperty(1, "key", "value") }},
		{"RemoveEdgeProperty", func(tx *Tx) error { return tx.RemoveEdgeProperty(1, "key") }},
		{"GetOutgoingEdges", func(tx *Tx) error { _, err := tx.GetOutgoingEdges(1); return err }},
		{"GetIncomingEdges", func(tx *Tx) error { _, err := tx.GetIncomingEdges(1); return err }},
		{"GetOutgoingEdgesByType", func(tx *Tx) error { _, err := tx.GetOutgoingEdgesByType(1, "REL", 1); return err }},
		{"GetIncomingEdgesByType", func(tx *Tx) error { _, err := tx.GetIncomingEdgesByType(1, "REL", 1); return err }},
		{"DeleteEdge", func(tx *Tx) error { return tx.DeleteEdge(1, 2, "REL") }},
		{"Query", func(tx *Tx) error { _, err := tx.Query("RETURN 1", nil); return err }},
		{"QueryContext", func(tx *Tx) error {
			_, err := tx.QueryContext(context.Background(), "RETURN 1", nil, QueryOptions{})
			return err
		}},
		{"GetAppMetadata", func(tx *Tx) error { _, _, err := tx.GetAppMetadata([]byte("key")); return err }},
		{"PutAppMetadata", func(tx *Tx) error { return tx.PutAppMetadata([]byte("key"), []byte("value")) }},
		{"DeleteAppMetadata", func(tx *Tx) error { return tx.DeleteAppMetadata([]byte("key")) }},
	}
}

func assertInactiveTxError(t *testing.T, err error) {
	t.Helper()
	var latticeErr *Error
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorInvalidArg || !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("error = %v, want structured ErrInactiveTx", err)
	}
}
