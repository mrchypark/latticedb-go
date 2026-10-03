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
