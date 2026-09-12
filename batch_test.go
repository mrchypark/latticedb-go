package latticedb

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestBatchCallbackErrorIdentity(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sentinelErr := fmt.Errorf("callback-sentinel")
	err = db.Batch(func(tx *Tx) error {
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("callback error identity = %T, want sentinel", err)
	}
	var structured *Error
	if errors.As(err, &structured) {
		t.Fatalf("callback error wrapped as structured Error")
	}
}

func TestBatchContextCallbackErrorIdentity(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sentinelErr := errors.New("callback-sentinel")
	err = db.BatchContext(context.Background(), func(tx *Tx) error {
		return sentinelErr
	})
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("callback error identity = %T, want sentinel", err)
	}
}

func TestBatchNilCallback(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Batch(nil)
	if err == nil {
		t.Fatal("nil callback returned nil error")
	}
}

func TestBatchContextNilCallback(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.BatchContext(context.Background(), nil)
	if err == nil {
		t.Fatal("nil callback returned nil error")
	}
}

func TestBatchClosedDB(t *testing.T) {
	db := closedTestDB(t)
	err := db.Batch(func(tx *Tx) error {
		t.Fatal("closed Batch callback invoked")
		return nil
	})
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed Batch error = %v", err)
	}
}

func TestBatchContextClosedDB(t *testing.T) {
	db := closedTestDB(t)
	err := db.BatchContext(context.Background(), func(tx *Tx) error {
		t.Fatal("closed BatchContext callback invoked")
		return nil
	})
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed BatchContext error = %v", err)
	}
}

func TestBatchCanceledContext(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = db.BatchContext(ctx, func(tx *Tx) error {
		return nil
	})
	if err == nil {
		t.Fatal("canceled context returned nil error")
	}
}

func TestBatchPersistsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Batch(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"BatchNode"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	nodes, err := db2.GetNodesByLabel("BatchNode")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes after reopen, want 1", len(nodes))
	}
}

func TestBatchNilDB(t *testing.T) {
	var db *DB
	err := db.Batch(func(tx *Tx) error {
		t.Fatal("nil DB Batch callback invoked")
		return nil
	})
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("nil DB error = %v", err)
	}
}

func TestBatchContextNilDB(t *testing.T) {
	var db *DB
	err := db.BatchContext(context.Background(), func(tx *Tx) error {
		t.Fatal("nil DB BatchContext callback invoked")
		return nil
	})
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("nil DB error = %v", err)
	}
}

func TestBatchManagedCommitRejected(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Batch(func(tx *Tx) error {
		if err := tx.Commit(); !errors.Is(err, ErrManagedTransaction) {
			t.Fatalf("Commit error = %v, want ErrManagedTransaction", err)
		}
		if err := tx.Rollback(); !errors.Is(err, ErrManagedTransaction) {
			t.Fatalf("Rollback error = %v, want ErrManagedTransaction", err)
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"M"}})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBatchContextManagedCommitRejected(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.BatchContext(context.Background(), func(tx *Tx) error {
		if err := tx.Commit(); !errors.Is(err, ErrManagedTransaction) {
			t.Fatalf("Commit error = %v, want ErrManagedTransaction", err)
		}
		if err := tx.Rollback(); !errors.Is(err, ErrManagedTransaction) {
			t.Fatalf("Rollback error = %v, want ErrManagedTransaction", err)
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"M"}})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}
