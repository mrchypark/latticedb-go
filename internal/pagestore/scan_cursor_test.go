package pagestore

import (
	"context"
	"reflect"
	"testing"
)

func TestReadOnlyScanReentrantAndMutableKey(t *testing.T) {
	db, _ := openTestDB(t)
	w, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		put(t, w, "records", k, k)
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var keys []string
	err = tx.Scan(context.Background(), "records", nil, nil, func(k, v []byte) error {
		keys = append(keys, string(k))
		if _, err := tx.Get("records", k); err != nil {
			return err
		}
		if string(k) == "a" {
			k[0] = 'c'
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "d"}) {
		t.Fatalf("keys %v", keys)
	}
}
