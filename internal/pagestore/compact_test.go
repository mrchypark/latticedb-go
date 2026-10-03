//go:build aix || android || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

package pagestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCompactPreservesRecordsAndShrinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	db, err := Open(path, Options{PageSize: 8192, CacheBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, _ := db.Begin(true)
	for i := 0; i < 80; i++ {
		if err = tx.Put("data", []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 32<<10)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, _ = db.Begin(true)
	for i := 1; i < 80; i++ {
		if err = tx.Delete("data", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	reader, _ := db.Begin(false)
	if err = db.Compact(context.Background()); !errors.Is(err, ErrActiveTransactions) {
		t.Fatalf("active reader: %v", err)
	}
	_ = reader.Rollback()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err = db.Compact(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	injected := errors.New("rename failed")
	if err = db.compact(context.Background(), func(string, string) error { return injected }); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	reader, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.Get("data", []byte{0})
	if err != nil || len(value) != 32<<10 {
		t.Fatalf("preservation %d %v", len(value), err)
	}
	_ = reader.Rollback()
	if err = db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Put("data", []byte("new"), bytes.Repeat([]byte{1}, 2<<20)); err != nil {
		t.Fatal(err)
	}
	if err = writer.Commit(); !errors.Is(err, ErrSnapshotGrowth) {
		t.Fatalf("post-compact pinned growth: %v", err)
	}
	_ = writer.Rollback()
	_ = reader.Rollback()
	after, _ := os.Stat(path)
	if after.Size() >= before.Size()/2 {
		t.Fatalf("before %d after %d", before.Size(), after.Size())
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{PageSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reader, _ = db.Begin(false)
	defer reader.Rollback()
	value, err = reader.Get("data", []byte{0})
	if err != nil || len(value) != 32<<10 {
		t.Fatal(err)
	}
	value, err = reader.Get("data", []byte{1})
	if err != nil || value != nil {
		t.Fatal("deleted data returned")
	}
}

func TestPageCachePreservesSnapshotsAndOwnedValues(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pages"), Options{CacheBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, _ := db.Begin(true)
	_ = tx.Put("b", []byte("k"), []byte("old"))
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	old, _ := db.Begin(false)
	defer old.Rollback()
	value, _ := old.Get("b", []byte("k"))
	value[0] = 'X'
	value, _ = old.Get("b", []byte("k"))
	if string(value) != "old" {
		t.Fatal("caller changed cached value")
	}
	tx, _ = db.Begin(true)
	_ = tx.Put("b", []byte("k"), []byte("new"))
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	newer, _ := db.Begin(false)
	defer newer.Rollback()
	value, _ = newer.Get("b", []byte("k"))
	if string(value) != "new" {
		t.Fatal("stale cache")
	}
	value, _ = old.Get("b", []byte("k"))
	if string(value) != "old" {
		t.Fatal("snapshot changed")
	}
	for i := 0; i < 20; i++ {
		db.cache.put(recordCacheKey{key: string(rune(i))}, bytes.Repeat([]byte{1}, 100))
	}
	if db.cache.used > 512 {
		t.Fatal("cache exceeded limit")
	}
}

func TestPageSizesRecoverFromSecondMeta(t *testing.T) {
	for _, size := range []int{1024, 2048, 8192, 16384, 32768, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pages")
			db, err := Open(path, Options{PageSize: size})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				tx, err := db.Begin(true)
				if err != nil {
					t.Fatal(err)
				}
				if err = tx.Put("data", []byte("kept"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = file.WriteAt(make([]byte, 24), 0)
			if closeErr := file.Close(); err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			if found, err := IsFile(path); err != nil || !found {
				t.Fatalf("recognition: %t %v", found, err)
			}
			db, err = Open(path, Options{PageSize: size})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tx, err := db.Begin(false)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			value, err := tx.Get("data", []byte("kept"))
			if err != nil || string(value) != "value" {
				t.Fatalf("recovery: %q %v", value, err)
			}
		})
	}
}

func TestCompactReopenFailureClosesHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Put("data", []byte("key"), []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	published := path + ".published"
	err = db.compact(context.Background(), func(source, target string) error {
		if err := os.Rename(source, target); err != nil {
			return err
		}
		if err := os.Rename(target, published); err != nil {
			return err
		}
		return os.Mkdir(target, 0700) // Force reopen to fail after successful publication.
	})
	if err == nil {
		t.Fatal("expected reopen failure")
	}
	if _, err = db.Begin(false); !errors.Is(err, ErrClosed) {
		t.Fatalf("failed handle stayed usable: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(published, path); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	read, err := recovered.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	value, err := read.Get("data", []byte("key"))
	if err != nil || string(value) != "kept" {
		t.Fatalf("published data: %q %v", value, err)
	}
}
