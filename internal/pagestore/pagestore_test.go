package pagestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pages.db")
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func put(t *testing.T, tx *Tx, bucket, key, value string) {
	t.Helper()
	if err := tx.Put(bucket, []byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func TestCommitRollbackOwnedGetAndReopen(t *testing.T) {
	db, path := openTestDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	key, value := []byte("key"), []byte("before")
	if err := tx.Put("records", key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 'X', 'X'
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := read.Get("records", []byte("key"))
	if err != nil || string(got) != "before" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	got[0] = 'X'
	got, err = read.Get("records", []byte("key"))
	if err != nil || string(got) != "before" {
		t.Fatalf("Get did not return an owned copy: %q, %v", got, err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	put(t, tx, "records", "key", "rolled back")
	put(t, tx, "records", "other", "discarded")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	got, err = read.Get("records", []byte("key"))
	if err != nil || string(got) != "before" {
		t.Fatalf("reopened Get = %q, %v", got, err)
	}
	got, err = read.Get("records", []byte("other"))
	if err != nil || got != nil {
		t.Fatalf("rolled back key = %q, %v", got, err)
	}
}

func TestSnapshotGrowthReturnsBeforeCommitAndCanRetry(t *testing.T) {
	db, path := openTestDB(t)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Put("payload", []byte("large"), bytes.Repeat([]byte("x"), 2<<20)); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); !errors.Is(err, ErrSnapshotGrowth) {
		t.Fatalf("Commit with pinned reader = %v, want ErrSnapshotGrowth", err)
	}
	if _, err := read.Get("payload", []byte("large")); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatalf("retry after snapshot close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() <= before.Size() {
		t.Fatalf("database did not grow: before %d, after %d", before.Size(), after.Size())
	}
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	got, err := read.Get("payload", []byte("large"))
	if err != nil || len(got) != 2<<20 {
		t.Fatalf("reopened payload len = %d, %v", len(got), err)
	}
}

func TestSnapshotKeepsOldViewAndCloseIsIndependent(t *testing.T) {
	db, _ := openTestDB(t)
	seed, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Put("b", []byte("a"), bytes.Repeat([]byte("o"), 32<<10)); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}
	old, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	put(t, write, "b", "a", "new")
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := old.Get("b", []byte("a"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("o"), 32<<10)) {
		t.Fatalf("pinned snapshot value length = %d, %v", len(got), err)
	}
	current, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	got, err = current.Get("b", []byte("a"))
	if err != nil || string(got) != "new" {
		t.Fatalf("new snapshot = %q, %v", got, err)
	}
	if err := current.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := old.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestScanOrderBoundsNestedGetDeleteAndCallbackErrors(t *testing.T) {
	db, _ := openTestDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"d", "a", "c", "b"} {
		put(t, tx, "b", key, "v"+key)
	}
	var keys []string
	err = tx.Scan(context.Background(), "b", []byte("b"), []byte("d"), func(key, value []byte) error {
		got, err := tx.Get("b", key)
		if err != nil || !bytes.Equal(got, value) {
			return errors.New("nested Get mismatch")
		}
		keys = append(keys, string(key))
		return tx.Delete("b", key)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"b", "c"}) {
		t.Fatalf("Scan order/range = %v", keys)
	}
	var remaining []string
	err = tx.Scan(context.Background(), "b", nil, nil, func(key, value []byte) error {
		remaining = append(remaining, string(key))
		return nil
	})
	if err != nil || !reflect.DeepEqual(remaining, []string{"a", "d"}) {
		t.Fatalf("remaining = %v, %v", remaining, err)
	}
	stopErr := errors.New("stop")
	err = tx.Scan(context.Background(), "b", nil, nil, func(_, _ []byte) error { return stopErr })
	if !errors.Is(err, stopErr) {
		t.Fatalf("callback error = %v", err)
	}
	err = tx.Scan(context.Background(), "b", nil, nil, func(_, _ []byte) error { return io.EOF })
	if err != nil {
		t.Fatalf("early stop = %v", err)
	}
	err = tx.Scan(context.Background(), "b", nil, nil, func(_, _ []byte) error {
		return fmt.Errorf("decode: %w", io.EOF)
	})
	if err == nil {
		t.Fatal("wrapped io.EOF must propagate as a callback error")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestScanContextReadOnlyAndTransactionErrors(t *testing.T) {
	db, _ := openTestDB(t)
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	put(t, write, "b", "a", "v")
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Put("b", []byte("x"), []byte("v")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Put = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := read.Scan(ctx, "b", nil, nil, func(_, _ []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Scan = %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrActiveTransactions) {
		t.Fatalf("Close with active transaction = %v", err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := read.Get("b", []byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after rollback = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyOpenRejectsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly.db")
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Begin(true); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Begin writable = %v", err)
	}
}

func TestSecondWriterReturnsWithoutBlockingFirstCommit(t *testing.T) {
	db, _ := openTestDB(t)
	first, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrWriterActive) {
		t.Fatalf("second Begin(true) = %v", err)
	}
	put(t, first, "b", "k", "v")
	if err := first.Commit(); err != nil {
		t.Fatalf("first writer commit after rejected Begin: %v", err)
	}
}
