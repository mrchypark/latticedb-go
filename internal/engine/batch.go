package engine

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const maxBatchRequests = 32

type batchRequest struct {
	ctx        context.Context
	fn         func(*Tx) error
	done       chan error
	panicked   bool
	panicValue any
}

// Batch groups concurrent callbacks into one durable managed transaction.
func (db *DB) Batch(fn func(*Tx) error) error {
	return db.BatchContext(context.Background(), fn)
}

// BatchContext waits for execution to finish even if ctx is canceled. Callbacks
// share a transaction: any callback error or pre-commit cancellation aborts it.
func (db *DB) BatchContext(ctx context.Context, fn func(*Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("nil batch callback")
	}
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrDatabaseClosed
	}
	request := &batchRequest{ctx: ctx, fn: fn, done: make(chan error, 1)}
	db.batchMu.Lock()
	if len(db.batchPending) == maxBatchRequests {
		db.batchMu.Unlock()
		return fmt.Errorf("%w: batch queue is full", ErrResourceLimit)
	}
	db.batchPending = append(db.batchPending, request)
	if !db.batchRunning {
		db.batchRunning = true
		// ponytail: fixed 1ms/32-request coalescing; tune only with workload evidence.
		db.batchTimer = time.AfterFunc(time.Millisecond, db.runBatch)
	} else if len(db.batchPending) == maxBatchRequests && db.batchTimer != nil && db.batchTimer.Stop() {
		go db.runBatch()
	}
	db.batchMu.Unlock()
	err := <-request.done
	if request.panicked {
		panic(request.panicValue)
	}
	return err
}

func (db *DB) runBatch() {
	db.batchMu.Lock()
	requests := db.batchPending
	db.batchPending = nil
	db.batchTimer = nil
	db.batchMu.Unlock()
	var current *batchRequest
	// This default also releases waiters if a callback calls runtime.Goexit.
	err := errors.New("batch callback exited without returning")
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("batch callback panicked: %v", value)
			if current != nil {
				current.panicked, current.panicValue = true, value
			}
		}
		db.batchMu.Lock()
		if len(db.batchPending) != 0 {
			go db.runBatch()
		} else {
			db.batchRunning = false
		}
		db.batchMu.Unlock()
		for _, request := range requests {
			request.done <- err
		}
	}()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	stops := make([]func() bool, 0, len(requests))
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	for _, request := range requests {
		if request.ctx.Err() != nil {
			cancel(request.ctx.Err())
		}
		stops = append(stops, context.AfterFunc(request.ctx, func() { cancel(request.ctx.Err()) }))
	}
	err = db.UpdateContext(ctx, func(tx *Tx) error {
		for _, request := range requests {
			if err := request.ctx.Err(); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			current = request
			callbackErr := request.fn(tx)
			current = nil
			if callbackErr != nil {
				return callbackErr
			}
		}
		// AfterFunc is asynchronous: check each source before entering commit.
		for _, request := range requests {
			if err := request.ctx.Err(); err != nil {
				return err
			}
		}
		return nil
	})
	if err == context.Canceled && ctx.Err() != nil {
		err = context.Cause(ctx)
	}
}
