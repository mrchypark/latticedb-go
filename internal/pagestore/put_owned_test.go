package pagestore

import (
	"errors"
	"testing"
)

func TestPutOwnedCommitReopenRollbackAndKeyCopy(t *testing.T) {
	db, path := openTestDB(t)
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	ownedKey, ownedValue := []byte("owned"), []byte("value")
	if err := write.PutOwned("records", ownedKey, ownedValue); err != nil {
		t.Fatal(err)
	}
	ownedKey[0] = 'X'
	normalKey, normalValue := []byte("normal"), []byte("value")
	if err := write.Put("records", normalKey, normalValue); err != nil {
		t.Fatal(err)
	}
	normalKey[0], normalValue[0] = 'X', 'X'
	if err := write.Commit(); err != nil {
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
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct{ key, value string }{{"owned", "value"}, {"normal", "value"}} {
		got, err := read.Get("records", []byte(want.key))
		if err != nil || string(got) != want.value {
			t.Fatalf("reopened %q = %q, %v", want.key, got, err)
		}
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	write, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := write.PutOwned("records", []byte("discarded"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := write.Rollback(); err != nil {
		t.Fatal(err)
	}
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	if got, err := read.Get("records", []byte("discarded")); err != nil || got != nil {
		t.Fatalf("rolled back owned value = %q, %v", got, err)
	}
}

func TestPutOwnedRejectsNilClosedAndReadOnlyTransactions(t *testing.T) {
	if err := (*Tx)(nil).PutOwned("records", []byte("key"), []byte("value")); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil PutOwned = %v", err)
	}
	db, _ := openTestDB(t)
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := write.PutOwned("records", []byte("key"), []byte("value")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed PutOwned = %v", err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	if err := read.PutOwned("records", []byte("key"), []byte("value")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only PutOwned = %v", err)
	}
}
