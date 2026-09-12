package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// Install the collected group directly so correctness never depends on timer timing.
func launchTestBatch(db *DB, ctx context.Context, callbacks ...func(*Tx) error) []*batchRequest {
	requests := make([]*batchRequest, len(callbacks))
	for i, fn := range callbacks {
		requests[i] = &batchRequest{ctx: ctx, fn: fn, done: make(chan error, 1)}
	}
	db.batchMu.Lock()
	db.batchPending, db.batchRunning = requests, true
	db.batchMu.Unlock()
	go db.runBatch()
	return requests
}

func batchResult(t *testing.T, request *batchRequest) error {
	t.Helper()
	select {
	case err := <-request.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not finish")
		return nil
	}
}

func TestBatchAcknowledgesOnlyAfterOneSync(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var syncs atomic.Int32
	path := filepath.Join(t.TempDir(), "batch")
	db, err := Open(path, OpenOptions{Create: true, walSync: func(f *os.File) error {
		syncs.Add(1)
		close(started)
		<-release
		return f.Sync()
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	callbacks := make([]func(*Tx) error, 3)
	for i := range callbacks {
		callbacks[i] = func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := launchTestBatch(db, ctx, callbacks...)
	<-started
	for _, request := range requests {
		select {
		case err := <-request.done:
			t.Fatalf("early acknowledgement: %v", err)
		default:
		}
	}
	db.mu.RLock()
	visible := db.graph.Nodes.Len()
	db.mu.RUnlock()
	if visible != 0 {
		t.Fatal("batch visible before durable commit")
	}
	// Cancellation after entering WAL sync cannot turn a durable commit into a rollback.
	cancel()
	close(release)
	for _, request := range requests {
		if err := batchResult(t, request); err != nil {
			t.Fatal(err)
		}
	}
	if syncs.Load() != 1 {
		t.Fatalf("sync count=%d", syncs.Load())
	}
	db.mu.RLock()
	count, commit := db.graph.Nodes.Len(), db.commitID
	db.mu.RUnlock()
	if count != 3 || commit != 1 {
		t.Fatalf("nodes=%d commit=%d", count, commit)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.graph.Nodes.Len() != 3 {
		t.Fatal("batch lost on reopen")
	}
}

func TestBatchFailureRollsBackEveryCallback(t *testing.T) {
	for _, kind := range []string{"error", "cancel", "panic", "goexit"} {
		t.Run(kind, func(t *testing.T) {
			var syncs atomic.Int32
			db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true, walSync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("callback failed")
			requests := launchTestBatch(db, ctx, func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }, func(tx *Tx) error {
				switch kind {
				case "error":
					return sentinel
				case "cancel":
					cancel()
				case "panic":
					panic(sentinel)
				case "goexit":
					runtime.Goexit()
				}
				return nil
			})
			for _, request := range requests {
				if err := batchResult(t, request); err == nil {
					t.Fatal("failed batch acknowledged")
				}
			}
			if kind == "panic" && (!requests[1].panicked || requests[1].panicValue != sentinel || requests[0].panicked) {
				t.Fatal("panic ownership lost")
			}
			if db.graph.Nodes.Len() != 0 || db.commitID != 0 || syncs.Load() != 0 {
				t.Fatal("aborted batch changed durable state")
			}
			if err := db.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); err != nil {
				t.Fatalf("writer not released: %v", err)
			}
		})
	}
}

func TestBatchSyncFailureFencesAllCallers(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true, walSync: func(*os.File) error { return errors.New("sync failure") }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fn := func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }
	for _, request := range launchTestBatch(db, context.Background(), fn, fn) {
		if err := batchResult(t, request); !errors.Is(err, ErrCommitOutcomeUnknown) {
			t.Fatalf("outcome=%v", err)
		}
	}
	if err := db.Batch(fn); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("fence=%v", err)
	}
	if db.graph.Nodes.Len() != 0 {
		t.Fatal("unknown commit published")
	}
}

func TestBatchQueueAndWriterBoundaries(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fn := func(*Tx) error { t.Error("rejected callback ran"); return nil }
	db.batchMu.Lock()
	db.batchPending = make([]*batchRequest, maxBatchRequests)
	db.batchRunning = true
	db.batchMu.Unlock()
	if err := db.Batch(fn); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("queue limit=%v", err)
	}
	db.batchMu.Lock()
	db.batchPending = nil
	db.batchRunning = false
	db.batchMu.Unlock()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Batch(fn); !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("writer conflict=%v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, request := range launchTestBatch(db, context.Background(), fn, fn) {
		if err := batchResult(t, request); !errors.Is(err, ErrDatabaseClosed) {
			t.Fatalf("closed queue=%v", err)
		}
	}
}

func TestBatchPanicReturnsToCallingGoroutine(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sentinel := errors.New("panic owner")
	func() {
		defer func() {
			if recover() != sentinel {
				t.Error("panic not returned to caller")
			}
		}()
		_ = db.Batch(func(*Tx) error { panic(sentinel) })
	}()
	if err := db.Batch(func(*Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestBatchFullQueueFlushesBeforeTimer(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fn := func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }
	requests := make([]*batchRequest, maxBatchRequests-1)
	for i := range requests {
		requests[i] = &batchRequest{ctx: context.Background(), fn: fn, done: make(chan error, 1)}
	}
	db.batchMu.Lock()
	db.batchPending, db.batchRunning = requests, true
	db.batchTimer = time.AfterFunc(time.Hour, db.runBatch)
	timer := db.batchTimer
	db.batchMu.Unlock()
	defer timer.Stop()
	done := make(chan error, 1)
	go func() { done <- db.Batch(fn) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("full queue did not flush")
	}
	for _, request := range requests {
		if err := batchResult(t, request); err != nil {
			t.Fatal(err)
		}
	}
	if db.graph.Nodes.Len() != maxBatchRequests || db.commitID != 1 {
		t.Fatal("full batch split or lost")
	}
}

func TestBatchQueuedDuringSyncRunsNext(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var syncs atomic.Int32
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true, walSync: func(f *os.File) error {
		if syncs.Add(1) == 1 {
			close(started)
			<-release
		}
		return f.Sync()
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fn := func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }
	first := make(chan error, 1)
	go func() { first <- db.Batch(fn) }()
	<-started
	// Install a waiting request while the first group owns the writer slot.
	queued := &batchRequest{ctx: context.Background(), fn: fn, done: make(chan error, 1)}
	db.batchMu.Lock()
	db.batchPending = append(db.batchPending, queued)
	db.batchMu.Unlock()
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := batchResult(t, queued); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != 2 || db.graph.Nodes.Len() != 2 {
		t.Fatal("queued group lost")
	}
}

func TestBatchPreservesQueuedDeadlineCause(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	requests := launchTestBatch(db, ctx, func(*Tx) error { t.Error("expired callback ran"); return nil })
	if err := batchResult(t, requests[0]); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline cause=%v", err)
	}
}

func TestBatchCancellationNeverHidesUnknownCommitOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := Open(filepath.Join(t.TempDir(), "batch"), OpenOptions{Create: true, walSync: func(*os.File) error { cancel(); return context.Canceled }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	request := launchTestBatch(db, ctx, func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err })[0]
	if err := batchResult(t, request); !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown outcome hidden: %v", err)
	}
}
