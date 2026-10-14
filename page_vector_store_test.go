package latticedb

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageVectorRecordsReopenAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	index := store.PageVectorIndex{Tx: w, Namespace: "default"}
	if err := index.PutMeta(store.PageVectorMeta{EntryID: 7, MaxLevel: 1, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := index.Put(7, &store.PageVectorNode{Level: 1, Neighbors: [][]uint64{{8}, {8}}, Vector: []float32{1.25, -2}}); err != nil {
		t.Fatal(err)
	}
	if err := index.Put(8, &store.PageVectorNode{Level: 0, Neighbors: [][]uint64{{7}}, Vector: []float32{0, -0.0}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = read
	meta, err := index.Meta()
	if err != nil || meta.EntryID != 7 || meta.Count != 1 {
		t.Fatalf("metadata=%+v err=%v", meta, err)
	}
	node, err := index.Get(7)
	if err != nil || node == nil || node.Level != 1 || node.Vector[0] != 1.25 || node.Neighbors[1][0] != 8 {
		t.Fatalf("node=%+v err=%v", node, err)
	}
	zero, err := index.Get(8)
	if err != nil || zero == nil || len(zero.Vector) != 2 || zero.Vector[0] != 0 || zero.Vector[1] != 0 {
		t.Fatalf("zero vector=%+v err=%v", zero, err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	w, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = w
	if err := index.Put(7, &store.PageVectorNode{Level: 1, Neighbors: [][]uint64{{}, {8}}, Vector: []float32{9, 9}, Deleted: true}); err != nil {
		t.Fatal(err)
	}
	if err := w.Rollback(); err != nil {
		t.Fatal(err)
	}
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = read
	node, err = index.Get(7)
	if err != nil || node == nil || node.Deleted || node.Vector[0] != 1.25 {
		t.Fatalf("rollback failed: node=%+v err=%v", node, err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = read
	node, err = index.Get(7)
	if err != nil || node == nil || node.Vector[1] != -2 {
		t.Fatalf("reopen node=%+v err=%v", node, err)
	}
	_ = read.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorRejectsOversizedDecodedDegree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad-vector-degree.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	// Inject the malformed record directly: normal writers reject this degree.
	data := []byte{1, 5, 0, 0, 0, 0, 0, 0, 2}
	data = binary.AppendUvarint(data, 130)
	for id := uint64(1); id <= 129; id++ {
		data = binary.AppendUvarint(data, id)
	}
	data = append(data, 3, 0, 0)
	binary.BigEndian.PutUint32(data[2:6], crc32.ChecksumIEEE(data[6:]))
	prefix := sha256.Sum256([]byte("degree"))
	key := append([]byte{1}, prefix[:]...)
	key = binary.BigEndian.AppendUint64(key, 1)
	if err = w.Put("vector-hnsw", key, data); err != nil {
		t.Fatal(err)
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	r, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (store.PageVectorIndex{Tx: r, Namespace: "degree"}).Get(1); err == nil {
		t.Fatal("oversized adjacency allocation accepted")
	}
	if err = r.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}
