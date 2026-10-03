package pagestore

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestScanKeysAdmitsBeforeVisitAndRespectsEnd(t *testing.T) {
	db, _ := openTestDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	put(t, tx, "records", "a", strings.Repeat("x", 1<<20))
	put(t, tx, "records", "zzzz", "tail")
	var charged uint64
	visits := 0
	charge := func(size uint64) error { charged += size; return nil }
	visit := func(key []byte) error { visits++; return nil }
	if err := tx.ScanKeysWithCharge(context.Background(), "records", nil, []byte("z"), 1, charge, visit); err != nil || visits != 1 || charged != 1 {
		t.Fatalf("range: visits=%d bytes=%d err=%v", visits, charged, err)
	}
	charged = 0
	visits = 0
	if err := tx.ScanKeysWithCharge(context.Background(), "records", nil, nil, 1, charge, visit); !errors.Is(err, ErrValueTooLarge) || visits != 1 || charged != 1 {
		t.Fatalf("key limit: visits=%d bytes=%d err=%v", visits, charged, err)
	}
	visits = 0
	reject := errors.New("key admission rejected")
	if err := tx.ScanKeysWithCharge(context.Background(), "records", nil, nil, 4, func(uint64) error { return reject }, visit); !errors.Is(err, reject) || visits != 0 {
		t.Fatalf("admission: visits=%d err=%v", visits, err)
	}
	// An admission failure must release the transaction mutex.
	if _, err := tx.Get("records", []byte("a")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tx.ScanKeysWithCharge(ctx, "records", nil, nil, 4, charge, visit); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}
