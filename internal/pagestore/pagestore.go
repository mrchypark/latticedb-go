//go:build aix || android || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

// Package pagestore provides a small transactional, disk-backed ordered key/value store.
package pagestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const defaultSnapshotWriteLimit int64 = 16 << 20
const initialMmapSize int64 = 1 << 20

// DB is a single bbolt database. Transactions must not be used concurrently.
type DB struct {
	mu       sync.Mutex
	db       *bolt.DB
	path     string
	readOnly bool
	limit    int64
	readers  int
	writers  int
	mapSize  int64
	closed   bool
}

// Open opens or creates the bbolt file at path. It uses a bounded 1 MiB initial
// mapping cushion; on Windows bbolt sizes the file to that mapping on open.
func Open(path string, opts Options) (*DB, error) {
	if path == "" || opts.MaxSnapshotWriteBytes < 0 {
		return nil, ErrInvalidOptions
	}
	limit := opts.MaxSnapshotWriteBytes
	if limit == 0 {
		limit = defaultSnapshotWriteLimit
	}
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: opts.ReadOnly, Timeout: 100 * time.Millisecond, InitialMmapSize: int(initialMmapSize)})
	if err != nil {
		return nil, fmt.Errorf("pagestore: open %q: %w", path, err)
	}
	mapSize, err := mappedCapacity(path)
	if err != nil {
		_ = bdb.Close()
		return nil, fmt.Errorf("pagestore: inspect mmap capacity: %w", err)
	}
	if mapSize < initialMmapSize {
		mapSize = initialMmapSize
	}
	return &DB{db: bdb, path: path, readOnly: opts.ReadOnly, limit: limit, mapSize: mapSize}, nil
}

// Begin opens a read snapshot or a writable transaction.
func (db *DB) Begin(writable bool) (*Tx, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	if writable && db.readOnly {
		return nil, ErrReadOnly
	}
	if writable && db.writers != 0 {
		return nil, ErrWriterActive
	}
	bt, err := db.db.Begin(writable)
	if err != nil {
		return nil, fmt.Errorf("pagestore: begin: %w", err)
	}
	tx := &Tx{db: db, tx: bt, writable: writable, baseSize: bt.Size()}
	if writable {
		db.writers++
	} else {
		db.readers++
	}
	return tx, nil
}

// Close closes the database once every transaction has ended.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	if db.readers != 0 || db.writers != 0 {
		return ErrActiveTransactions
	}
	if err := db.db.Close(); err != nil {
		return fmt.Errorf("pagestore: close: %w", err)
	}
	db.closed = true
	return nil
}

// Tx serializes access to its bbolt transaction. Scans release the mutex
// between owned records so callbacks can perform nested reads or writes.
type Tx struct {
	mu         sync.Mutex
	db         *DB
	tx         *bolt.Tx
	writable   bool
	closed     bool
	baseSize   int64
	writeBytes int64
	writeOps   int64
}

func (tx *Tx) check() error {
	if tx == nil || tx.closed || tx.tx == nil {
		return ErrClosed
	}
	return nil
}

// Get returns an owned copy of the value. A missing bucket or key returns nil.
func (tx *Tx) Get(bucket string, key []byte) ([]byte, error) {
	if tx == nil {
		return nil, ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.check(); err != nil {
		return nil, err
	}
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil
	}
	return bytes.Clone(b.Get(key)), nil
}

// Put creates bucket if needed and copies key and value into the transaction.
func (tx *Tx) Put(bucket string, key, value []byte) error {
	if tx == nil {
		return ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrReadOnly
	}
	b, err := tx.tx.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return fmt.Errorf("pagestore: create bucket %q: %w", bucket, err)
	}
	keyCopy, valueCopy := bytes.Clone(key), bytes.Clone(value)
	if err := b.Put(keyCopy, valueCopy); err != nil {
		return fmt.Errorf("pagestore: put in bucket %q: %w", bucket, err)
	}
	if int64(len(key))+int64(len(value)) > int64(^uint64(0)>>1)-tx.writeBytes {
		tx.writeBytes = int64(^uint64(0) >> 1)
	} else {
		tx.writeBytes += int64(len(key)) + int64(len(value))
	}
	tx.writeOps++
	return nil
}

// Delete removes key. Deleting a missing bucket or key succeeds.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if tx == nil {
		return ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrReadOnly
	}
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil
	}
	if err := b.Delete(key); err != nil {
		return fmt.Errorf("pagestore: delete from bucket %q: %w", bucket, err)
	}
	tx.writeOps++
	return nil
}

// Scan visits keys in bytewise order in [start,end), with nil bounds unbounded.
// Callback key/value slices own their bytes and remain valid after the
// callback returns. Returning io.EOF stops normally;
// any other callback error is returned wrapped. A callback may call Get.
func (tx *Tx) Scan(ctx context.Context, bucket string, start, end []byte, visit func([]byte, []byte) error) error {
	if ctx == nil {
		return errors.New("pagestore: nil scan context")
	}
	if visit == nil {
		return errors.New("pagestore: nil scan callback")
	}
	seek, inclusive := start, true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, value, err := tx.scanEntry(bucket, seek, inclusive)
		if err != nil {
			return err
		}
		if key == nil || end != nil && bytes.Compare(key, end) >= 0 {
			return nil
		}
		if err := visit(key, value); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("pagestore: scan bucket %q: %w", bucket, err)
		}
		seek, inclusive = key, false
	}
}

func (tx *Tx) scanEntry(bucket string, seek []byte, inclusive bool) ([]byte, []byte, error) {
	if tx == nil {
		return nil, nil, ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.check(); err != nil {
		return nil, nil, err
	}
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil, nil
	}
	c := b.Cursor()
	var key, value []byte
	if seek == nil {
		key, value = c.First()
	} else {
		key, value = c.Seek(seek)
		if !inclusive && bytes.Equal(key, seek) {
			key, value = c.Next()
		}
	}
	return bytes.Clone(key), bytes.Clone(value), nil
}

// Commit atomically commits a writable transaction. A snapshot growth error
// leaves the transaction open so the caller can close readers and retry.
func (tx *Tx) Commit() error {
	if tx == nil {
		return ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrReadOnly
	}
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	if tx.db.closed {
		return ErrClosed
	}
	if tx.db.readers > 0 {
		if tx.writeBytes > tx.db.limit {
			return ErrSnapshotWriteLimit
		}
		// Reserve a full rewrite of every existing page (including the worst-case
		// dirty B-tree paths), staged payload, one leaf-page allowance and one
		// cascading branch-page allowance per mutation, plus full old-page-ID
		// freelist accounting and fixed bucket metadata. The full existing-page
		// reserve also covers pending pages held by snapshots. This may reject a
		// transaction that fits, but avoids relying on bbolt's private spill plan.
		pageSize := int64(tx.db.db.Info().PageSize)
		growth := tx.baseSize + tx.writeBytes + (2*tx.writeOps+8)*pageSize
		if growth < tx.baseSize || tx.baseSize > tx.db.mapSize-growth {
			return ErrSnapshotGrowth
		}
	}
	if err := tx.tx.Commit(); err != nil {
		tx.finishLocked()
		return fmt.Errorf("pagestore: commit: %w", err)
	}
	// Recover the now-current high-water mark through the public transaction API.
	// bbolt may have remapped during the commit; keep the larger known capacity.
	if current, err := tx.db.db.Begin(false); err == nil {
		if capacity := mmapSizeFor(current.Size()); capacity > tx.db.mapSize {
			tx.db.mapSize = capacity
		}
		_ = current.Rollback()
	}
	tx.finishLocked()
	return nil
}

// Rollback discards all changes and releases the transaction.
func (tx *Tx) Rollback() error {
	if tx == nil {
		return ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.check(); err != nil {
		return err
	}
	err := tx.tx.Rollback()
	tx.db.mu.Lock()
	tx.finishLocked()
	tx.db.mu.Unlock()
	if err != nil {
		return fmt.Errorf("pagestore: rollback: %w", err)
	}
	return nil
}

func (tx *Tx) finishLocked() {
	if tx.closed {
		return
	}
	tx.closed = true
	tx.tx = nil
	if tx.writable {
		tx.db.writers--
	} else {
		tx.db.readers--
	}
}

// mappedCapacity mirrors bbolt v1.4 mmap sizing when opening an existing file.
func mappedCapacity(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return mmapSizeFor(info.Size()), nil
}

func mmapSizeFor(size int64) int64 {
	for capacity := int64(1 << 15); capacity < 1<<30; capacity <<= 1 {
		if size <= capacity {
			return capacity
		}
	}
	const step = int64(1 << 30)
	return ((size + step - 1) / step) * step
}

// Sync flushes the current committed page file. Commits already sync by default.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	return db.db.Sync()
}

// BufferedBytes bounds bulk-import batches by their staged key/value bytes.
func (tx *Tx) BufferedBytes() (uint64, error) {
	if tx == nil {
		return 0, ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.check(); err != nil {
		return 0, err
	}
	return uint64(tx.writeBytes), nil
}
