package pagestore

import (
	"errors"
	"testing"
)

func TestReadBoundedChecksValueBeforeCallback(t *testing.T) {
	db, _ := openTestDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	put(t, tx, "records", "small", "ok")
	put(t, tx, "records", "large", "large")
	if err := tx.ReadBounded("records", []byte("missing"), 2, func([]byte) error {
		t.Fatal("missing value called callback")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.ReadBounded("records", []byte("large"), 2, func([]byte) error {
		t.Fatal("oversized value called callback")
		return nil
	}); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("oversized value error = %v", err)
	}
	want := errors.New("read failed")
	if err := tx.ReadBounded("records", []byte("small"), 2, func(value []byte) error {
		if string(value) != "ok" {
			t.Fatalf("value = %q", value)
		}
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("callback error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.ReadBounded("records", []byte("small"), 2, func([]byte) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed transaction error = %v", err)
	}
	if err := (*Tx)(nil).ReadBounded("records", []byte("small"), 2, func([]byte) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil transaction error = %v", err)
	}
}
