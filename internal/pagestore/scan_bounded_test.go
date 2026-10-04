package pagestore

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestScanBoundedChecksBeforeCallbackAndRespectsEnd(t *testing.T) {
	db, _ := openTestDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	put(t, tx, "records", "a", "ok")
	put(t, tx, "records", "z", strings.Repeat("x", 1024))
	calls := 0
	visit := func(k, v []byte) error { calls++; return nil }
	if err := tx.ScanBounded(context.Background(), "records", nil, []byte("z"), 3, visit); err != nil || calls != 1 {
		t.Fatalf("bounded range: calls=%d err=%v", calls, err)
	}
	calls = 0
	if err := tx.ScanBounded(context.Background(), "records", nil, nil, 3, visit); !errors.Is(err, ErrValueTooLarge) || calls != 1 {
		t.Fatalf("oversized record: calls=%d err=%v", calls, err)
	}
	calls = 0
	if err := tx.Scan(context.Background(), "records", nil, nil, visit); err != nil || calls != 2 {
		t.Fatalf("ordinary scan: calls=%d err=%v", calls, err)
	}
}

func TestHasChecksPresenceWithoutValueCopy(t *testing.T) {
	db, _ := openTestDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	put(t, tx, "records", "large", strings.Repeat("x", 1<<20))
	put(t, tx, "records", "empty", "")
	for _, test := range []struct {
		bucket, key string
		want        bool
	}{
		{"records", "large", true}, {"records", "empty", true},
		{"records", "missing", false}, {"missing", "large", false},
	} {
		if got, err := tx.Has(test.bucket, []byte(test.key)); err != nil || got != test.want {
			t.Fatalf("%s/%s: got %v err %v", test.bucket, test.key, got, err)
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Has("records", []byte("large")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := (*Tx)(nil).Has("records", []byte("large")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
