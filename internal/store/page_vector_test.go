package store

import (
	"encoding/binary"
	"errors"
	"math"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestUvarintBytesMatchesEncoder(t *testing.T) {
	var buf [binary.MaxVarintLen64]byte
	for _, value := range []uint64{0, 1, 126, 127, 128, 129, 16383, 16384, math.MaxUint32, math.MaxUint64} {
		if got, want := uvarintBytes(value), binary.PutUvarint(buf[:], value); got != want {
			t.Fatalf("value=%d size=%d want=%d", value, got, want)
		}
	}
}

func TestPageVectorGetBoundedDecodesOwnedNode(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "vectors"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	index := PageVectorIndex{Tx: tx, Namespace: "test"}
	if node, err := index.GetBounded(1, 1<<20); err != nil || node != nil {
		t.Fatalf("missing node = %#v, %v", node, err)
	}
	original := &PageVectorNode{Level: 1, Neighbors: [][]uint64{{2}, {3}}, Vector: []float32{1, -2}}
	if err := index.Put(1, original); err != nil {
		t.Fatal(err)
	}
	got, err := index.GetBounded(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	replacement := &PageVectorNode{Level: 0, Neighbors: [][]uint64{{4}}, Vector: []float32{3}}
	if err := index.Put(1, replacement); err != nil {
		t.Fatal(err)
	}
	if got.Level != original.Level || got.Deleted != original.Deleted || len(got.Neighbors) != 2 || got.Neighbors[0][0] != 2 || len(got.Vector) != 2 || got.Vector[0] != 1 {
		t.Fatalf("decoded node changed after write: %#v", got)
	}
	if err := index.Put(2, &PageVectorNode{Level: 0, Neighbors: [][]uint64{{1}}, Vector: []float32{1, 2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	if _, err := index.GetBounded(2, 30); !errors.Is(err, pagestore.ErrValueTooLarge) {
		t.Fatalf("oversized node error = %v", err)
	}
	raw, err := tx.Get(pageVectorBucket, index.key(1))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err := tx.Put(pageVectorBucket, index.key(1), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := index.GetBounded(1, 1<<20); err == nil {
		t.Fatal("corrupt node decoded successfully")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := index.GetBounded(1, 1<<20); !errors.Is(err, pagestore.ErrClosed) {
		t.Fatalf("closed transaction error = %v", err)
	}
}

func TestPageVectorPutRejectsOversizedDegreeWithoutWriteHook(t *testing.T) {
	index := PageVectorIndex{}
	if err := index.Put(1, &PageVectorNode{Neighbors: [][]uint64{make([]uint64, 129)}, Vector: []float32{1}}); err == nil {
		t.Fatal("oversized degree accepted without a write hook")
	}
}
