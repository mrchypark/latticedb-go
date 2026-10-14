//go:build aix || android || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

package pagestore

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestReadPairBoundedAtomicAdmission(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pairs"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for key, value := range map[string]string{"a": "abc", "b": "defg"} {
		if err := tx.Put("pairs", []byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	called := false
	read := func(a, b []byte) error {
		called = true
		if string(a) != "abc" || string(b) != "defg" {
			t.Fatalf("pair %q %q", a, b)
		}
		return nil
	}
	if err := tx.ReadPairBounded("pairs", []byte("a"), []byte("b"), 6, read); !errors.Is(err, ErrValueTooLarge) || called {
		t.Fatalf("admission %v callback=%v", err, called)
	}
	if err := tx.ReadPairBounded("pairs", []byte("a"), []byte("b"), 7, read); err != nil || !called {
		t.Fatal(err)
	}
	called = false
	if err := tx.ReadPairBounded("pairs", []byte("absent"), []byte("also-absent"), 0, read); err != nil || called {
		t.Fatalf("missing %v callback=%v", err, called)
	}
	if err := tx.ReadPairBounded("pairs", []byte("absent"), []byte("b"), 4, func(a, b []byte) error {
		if a != nil || string(b) != "defg" {
			t.Fatalf("partial pair %q %q", a, b)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("reader")
	if err := tx.ReadPairBounded("pairs", []byte("a"), []byte("b"), 7, func([]byte, []byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := tx.ReadPairBounded("pairs", nil, nil, 7, nil); err == nil {
		t.Fatal("nil callback accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.ReadPairBounded("pairs", nil, nil, 7, read); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	var closed *Tx
	if err := closed.ReadPairBounded("pairs", nil, nil, 7, read); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestReadPairBoundedCommittedCursorValues(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pairs"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	for key, value := range map[string]string{"a": "first", "c": "second", "empty": ""} {
		if err := write.Put("pairs", []byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	for _, pair := range []struct{ first, second, left, right string }{
		{"a", "c", "first", "second"},
		{"c", "a", "second", "first"},
		{"a", "a", "first", "first"},
		{"b", "c", "", "second"},
		{"a", "b", "first", ""},
		{"empty", "a", "", "first"},
	} {
		if err := read.ReadPairBounded("pairs", []byte(pair.first), []byte(pair.second), 12, func(left, right []byte) error {
			if string(left) != pair.left || string(right) != pair.right {
				t.Fatalf("%+v: got %q %q", pair, left, right)
			}
			if (pair.first == "empty" && left == nil) || (pair.first == "b" && left != nil) || (pair.second == "b" && right != nil) {
				t.Fatal("missing and empty values were confused")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadPairBoundedReusesCursorAfterWritableMutations(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pairs"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Put("pairs", []byte("left"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("pairs", []byte("right"), []byte("second")); err != nil {
		t.Fatal(err)
	}
	read := func(wantLeft, wantRight string) {
		t.Helper()
		called := false
		if err := tx.ReadPairBounded("pairs", []byte("left"), []byte("right"), 64, func(left, right []byte) error {
			called = true
			if string(left) != wantLeft || string(right) != wantRight {
				t.Fatalf("pair = %q %q, want %q %q", left, right, wantLeft, wantRight)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("missing pair callback")
		}
	}
	read("first", "second")
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := tx.Put("pairs", key, key); err != nil {
			t.Fatal(err)
		}
		read("first", "second")
	}
	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := tx.Delete("pairs", key); err != nil {
			t.Fatal(err)
		}
		if err := tx.Put("pairs", key, []byte("reinserted")); err != nil {
			t.Fatal(err)
		}
		read("first", "second")
	}
	if err := tx.Put("pairs", []byte("left"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	read("updated", "second")
	if err := tx.Delete("pairs", []byte("right")); err != nil {
		t.Fatal(err)
	}
	if err := tx.ReadPairBounded("pairs", []byte("left"), []byte("right"), 64, func(left, right []byte) error {
		if string(left) != "updated" || right != nil {
			t.Fatalf("after delete = %q %q", left, right)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("pairs", []byte("right"), []byte("reinserted")); err != nil {
		t.Fatal(err)
	}
	read("updated", "reinserted")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if tx.pairCursor != nil || tx.pairBucket != "" {
		t.Fatal("commit retained cursor")
	}
	committed, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer committed.Rollback()
	called := false
	if err := committed.ReadPairBounded("pairs", []byte("left"), []byte("right"), 64, func(left, right []byte) error {
		called = true
		if string(left) != "updated" || string(right) != "reinserted" {
			t.Fatalf("committed pair = %q %q", left, right)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("missing committed pair")
	}
}

func TestReadPairBoundedAlternatesBucketsAndReleasesCursor(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pairs"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for bucket, value := range map[string]string{"first": "one", "second": "two"} {
		if err := tx.Put(bucket, []byte("key"), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	read := func(bucket, want string) {
		t.Helper()
		called := false
		if err := tx.ReadPairBounded(bucket, []byte("key"), []byte("key"), 16, func(left, right []byte) error {
			called = true
			if string(left) != want || string(right) != want {
				t.Fatalf("%s = %q %q, want %q", bucket, left, right, want)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("missing pair callback")
		}
	}
	read("first", "one")
	read("second", "two")
	called := false
	if err := tx.ReadPairBounded("missing", []byte("key"), []byte("key"), 16, func([]byte, []byte) error {
		called = true
		return nil
	}); err != nil || called {
		t.Fatalf("missing bucket = %v, callback=%v", err, called)
	}
	read("first", "one")
	if tx.pairCursor == nil || tx.pairBucket != "first" {
		t.Fatalf("cursor cache = %p %q", tx.pairCursor, tx.pairBucket)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if tx.pairCursor != nil || tx.pairBucket != "" {
		t.Fatalf("closed cursor cache = %p %q", tx.pairCursor, tx.pairBucket)
	}
}
