package latticedb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestLifecycleContextCancelsWhileWriterHeld(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "lifecycle.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func(context.Context) error{db.CheckpointContext, db.CloseContext} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := operation(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lifecycle writer wait = %v", err)
		}
	}
	if !db.IsOpen() {
		t.Fatal("canceled close closed database")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
