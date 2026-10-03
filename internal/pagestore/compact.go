//go:build aix || android || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

package pagestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Compact rewrites live pages into a same-directory file. It requires exclusive
// ownership of the database path and no active transactions. Cancellation is
// checked before publication. Publication and reopening then finish together.
func (db *DB) Compact(ctx context.Context) error {
	return db.compact(ctx, os.Rename)
}

func (db *DB) compact(ctx context.Context, rename func(string, string) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if db.readers != 0 || db.writers != 0 {
		return ErrActiveTransactions
	}
	f, err := os.CreateTemp(filepath.Dir(db.path), ".latticedb-compact-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Close(); err != nil {
		return err
	}
	options := &bolt.Options{Timeout: 100 * time.Millisecond, PageSize: db.db.Info().PageSize}
	dst, err := bolt.Open(name, 0o600, options)
	if err != nil {
		return err
	}
	err = bolt.Compact(dst, db.db, 4<<20)
	if err == nil {
		err = dst.Sync()
	}
	err = errors.Join(err, dst.Close())
	if err != nil {
		return fmt.Errorf("pagestore: build compact file: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = db.db.Close(); err != nil {
		db.closed = true
		return err
	}
	// The outer path lock remains held while the bbolt handle is closed.
	publicationErr := rename(name, db.path)
	if publicationErr == nil && runtime.GOOS != "windows" {
		directory, e := os.Open(filepath.Dir(db.path))
		if e == nil {
			e = errors.Join(directory.Sync(), directory.Close())
		}
		publicationErr = e
	}
	reopened, openErr := bolt.Open(db.path, 0o600, options)
	if openErr != nil {
		db.closed = true
		return errors.Join(publicationErr, openErr)
	}
	db.db = reopened
	db.cache.clear()
	db.mapSize, err = mappedCapacity(db.path)
	if err != nil {
		db.closed = true
		_ = reopened.Close()
	}
	return errors.Join(publicationErr, err)
}
