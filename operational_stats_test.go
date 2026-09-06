package latticedb

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// writerWaitContext reports the first wait select after BeginWriteContext has
// registered its waiter. It keeps lifecycle assertions deterministic.
type writerWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *writerWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func TestOperationalStatsPublicLifecycle(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stats.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.OperationalStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.ActiveSnapshots != 1 || stats.RetainedGenerations != 1 {
		t.Fatalf("active stats = %#v", stats)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.OperationalStats(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed stats error = %v", err)
	}
}

func TestOperationalStatsBeginWriteContextWaiterLifecycle(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		db, err := Open(filepath.Join(t.TempDir(), "stats.ltdb"), OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		writer, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		base, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := &writerWaitContext{Context: base, entered: make(chan struct{})}
		result := make(chan struct {
			tx  *Tx
			err error
		}, 1)
		go func() {
			tx, err := db.BeginWriteContext(ctx)
			result <- struct {
				tx  *Tx
				err error
			}{tx, err}
		}()
		<-ctx.entered
		stats, err := db.OperationalStats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.WriterWaits != 1 || stats.ActiveWriterWaits != 1 {
			t.Fatalf("waiting stats = %#v", stats)
		}
		if err := writer.Rollback(); err != nil {
			t.Fatal(err)
		}
		got := <-result
		if got.err != nil {
			t.Fatal(got.err)
		}
		if err := got.tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		stats, err = db.OperationalStats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.WriterWaits != 1 || stats.ActiveWriterWaits != 0 || stats.ActiveTransactions != 0 {
			t.Fatalf("released stats = %#v", stats)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		db, err := Open(filepath.Join(t.TempDir(), "stats.ltdb"), OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		writer, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		base, cancel := context.WithCancel(context.Background())
		ctx := &writerWaitContext{Context: base, entered: make(chan struct{})}
		result := make(chan error, 1)
		go func() {
			_, err := db.BeginWriteContext(ctx)
			result <- err
		}()
		<-ctx.entered
		stats, err := db.OperationalStats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.WriterWaits != 1 || stats.ActiveWriterWaits != 1 {
			t.Fatalf("waiting stats = %#v", stats)
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v", err)
		}
		stats, err = db.OperationalStats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.WriterWaits != 1 || stats.ActiveWriterWaits != 0 {
			t.Fatalf("canceled stats = %#v", stats)
		}
		if err := writer.Rollback(); err != nil {
			t.Fatal(err)
		}
	})
}
