package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestBeginWriteContextWaitsAndReusesWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	held, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan *Tx, 1)
	failed := make(chan error, 1)
	go func() {
		tx, err := db.BeginWriteContext(context.Background())
		if err != nil {
			failed <- err
			return
		}
		acquired <- tx
	}()
	select {
	case <-acquired:
		t.Fatal("writer acquired before release")
	case err := <-failed:
		t.Fatalf("writer wait failed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := held.Rollback(); err != nil {
		t.Fatal(err)
	}
	var tx *Tx
	select {
	case tx = <-acquired:
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire after release")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	reusable, err := db.BeginWriteContext(context.Background())
	if err != nil {
		t.Fatalf("writer slot was not reusable: %v", err)
	}
	if err := reusable.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestBeginWriteContextCancellationAndStateErrors(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	held, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.BeginWriteContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled acquisition = %v", err)
	}
	timeout, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, err := db.BeginWriteContext(timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out acquisition = %v", err)
	}
	if err := held.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginWriteContext(context.Background()); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed acquisition = %v", err)
	}
	readOnlyPath := filepath.Join(t.TempDir(), "readonly")
	readOnly, err := Open(readOnlyPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err = Open(readOnlyPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.BeginWriteContext(context.Background()); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only acquisition = %v", err)
	}
}
