package store

import (
	"bytes"
	"context"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func pageVectorLinksFixture(t *testing.T) (*pagestore.DB, *pagestore.Tx, PageVectorIndex) {
	t.Helper()
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "vectors"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = tx.Rollback()
		_ = db.Close()
	})
	return db, tx, PageVectorIndex{Tx: tx, Namespace: "links"}
}

func pageVectorLinksNode() *PageVectorNode {
	return &PageVectorNode{Level: 1, Neighbors: [][]uint64{{2, 3}, {4}}, Vector: []float32{1, -2}}
}

func pageVectorLinksReplacement() *PageVectorNode {
	return &PageVectorNode{Level: 1, Deleted: true, Neighbors: [][]uint64{{3}, {2, 4}}, Vector: []float32{1, -2}}
}

func pageVectorRaw(t *testing.T, index PageVectorIndex, id uint64, links bool) []byte {
	t.Helper()
	key := index.key(id)
	if links {
		key = index.linksKey(id)
	}
	data, err := index.Tx.Get(pageVectorBucket, key)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func pageVectorFloat32s(bits ...uint32) []float32 {
	vector := make([]float32, len(bits))
	for i := range vector {
		vector[i] = math.Float32frombits(bits[i])
	}
	return vector
}

func pageVectorBitsEqual(got []float32, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if math.Float32bits(got[i]) != want[i] {
			return false
		}
	}
	return true
}

func pageVectorTaggedBase(t *testing.T, id, dimensions uint64, payload []byte) []byte {
	t.Helper()
	data, err := encodePageRecord(pageVectorLinkedBase, func(e *binaryEncoder) {
		e.u(17)
		e.u(id)
		e.u(dimensions)
		e.write(payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPageVectorLinksPreserveLegacyNodeAndRejectTaggedBase(t *testing.T) {
	_, _, index := pageVectorLinksFixture(t)
	original, replacement := pageVectorLinksNode(), pageVectorLinksReplacement()
	if err := index.Put(1, original); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(1, replacement); err != nil || !ok {
		t.Fatalf("PutLinks = %v, %v", ok, err)
	}
	got, err := index.Get(1)
	if err != nil || !reflect.DeepEqual(got, replacement) {
		t.Fatalf("linked node = %#v, %v", got, err)
	}
	base := pageVectorRaw(t, index, 1, false)
	if len(base) < 2 || base[1] != pageVectorLinkedBase {
		t.Fatalf("linked base tag = %v", base)
	}
	if _, err := decodePageVectorNode(base, 1, 1<<20); err == nil {
		t.Fatal("old decoder accepted tagged base")
	}
	if links := pageVectorRaw(t, index, 1, true); len(links) < 2 || links[1] != pageVectorLinks {
		t.Fatalf("links tag = %v", links)
	}
}

func TestPageVectorLinksRepeatAndFullPutResetOverlay(t *testing.T) {
	_, _, index := pageVectorLinksFixture(t)
	original, first := pageVectorLinksNode(), pageVectorLinksReplacement()
	if err := index.Put(1, original); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(1, first); err != nil || !ok {
		t.Fatalf("first PutLinks = %v, %v", ok, err)
	}
	base := pageVectorRaw(t, index, 1, false)
	second := &PageVectorNode{Level: 1, Neighbors: [][]uint64{{4}, {2}}, Vector: []float32{1, -2}}
	if ok, err := index.PutLinks(1, second); err != nil || !ok {
		t.Fatalf("second PutLinks = %v, %v", ok, err)
	}
	if got := pageVectorRaw(t, index, 1, false); !bytes.Equal(got, base) {
		t.Fatal("repeated links write changed base vector record")
	}
	if got, err := index.Get(1); err != nil || !reflect.DeepEqual(got, second) {
		t.Fatalf("repeated linked node = %#v, %v", got, err)
	}
	if err := index.Put(1, original); err != nil {
		t.Fatal(err)
	}
	if links := pageVectorRaw(t, index, 1, true); links != nil {
		t.Fatalf("full Put retained links record: %v", links)
	}
	if base = pageVectorRaw(t, index, 1, false); len(base) < 2 || base[1] != 5 {
		t.Fatalf("full Put did not restore legacy base: %v", base)
	}
}

func TestPageVectorLinksCommitReopenAndRollbackAreAtomic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	index := PageVectorIndex{Tx: tx, Namespace: "links"}
	original, replacement := pageVectorLinksNode(), pageVectorLinksReplacement()
	if err := index.Put(1, original); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = tx
	if ok, err := index.PutLinks(1, replacement); err != nil || !ok {
		t.Fatalf("rollback PutLinks = %v, %v", ok, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = tx
	if got, err := index.Get(1); err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("rollback node = %#v, %v", got, err)
	}
	if ok, err := index.PutLinks(1, replacement); err != nil || !ok {
		t.Fatalf("commit PutLinks = %v, %v", ok, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	index.Tx = tx
	defer tx.Rollback()
	if got, err := index.Get(1); err != nil || !reflect.DeepEqual(got, replacement) {
		t.Fatalf("reopened node = %#v, %v", got, err)
	}
}

func TestPageVectorLinksPreserveExactFloatBitsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	index := PageVectorIndex{Tx: tx, Namespace: "bits"}
	bits := []uint32{0, 0x80000000, 1, 0x80000001, 0x007fffff, 0x807fffff, 0x3f800000}
	vector := pageVectorFloat32s(bits...)
	original := &PageVectorNode{Neighbors: [][]uint64{{}}, Vector: vector}
	replacement := &PageVectorNode{Neighbors: [][]uint64{{2}}, Vector: vector}
	if err := index.Put(1, original); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(1, replacement); err != nil || !ok {
		t.Fatalf("PutLinks = %v, %v", ok, err)
	}
	if got, err := index.Get(1); err != nil || !pageVectorBitsEqual(got.Vector, bits) {
		t.Fatalf("converted vector = %#v, %v", got, err)
	}
	if !pageVectorBitsEqual(vector, bits) {
		t.Fatal("PutLinks changed caller vector")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	index.Tx = tx
	if got, err := index.Get(1); err != nil || !pageVectorBitsEqual(got.Vector, bits) {
		t.Fatalf("reopened vector = %#v, %v", got, err)
	}
}

func TestPageVectorLinksDeleteAndDeleteAllStayInNamespace(t *testing.T) {
	_, _, index := pageVectorLinksFixture(t)
	other := index
	other.Namespace = "other"
	for _, tc := range []struct {
		index PageVectorIndex
		id    uint64
	}{
		{index, 1}, {index, 2}, {other, 1},
	} {
		if err := tc.index.Put(tc.id, pageVectorLinksNode()); err != nil {
			t.Fatal(err)
		}
		if ok, err := tc.index.PutLinks(tc.id, pageVectorLinksReplacement()); err != nil || !ok {
			t.Fatalf("PutLinks(%q, %d) = %v, %v", tc.index.Namespace, tc.id, ok, err)
		}
	}
	if err := index.Delete(1); err != nil {
		t.Fatal(err)
	}
	if node, err := index.Get(1); err != nil || node != nil || pageVectorRaw(t, index, 1, true) != nil {
		t.Fatalf("Delete left overlay: node=%#v err=%v", node, err)
	}
	if err := index.DeleteAll(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if node, err := index.Get(2); err != nil || node != nil || pageVectorRaw(t, index, 2, true) != nil {
		t.Fatalf("DeleteAll left overlay: node=%#v err=%v", node, err)
	}
	if node, err := other.Get(1); err != nil || node == nil {
		t.Fatalf("DeleteAll changed other namespace: node=%#v err=%v", node, err)
	}
}

func TestPageVectorLinksMissingOrCorruptOverlayFails(t *testing.T) {
	_, _, index := pageVectorLinksFixture(t)
	if err := index.Put(1, pageVectorLinksNode()); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(1, pageVectorLinksReplacement()); err != nil || !ok {
		t.Fatalf("PutLinks = %v, %v", ok, err)
	}
	if err := index.Tx.Delete(pageVectorBucket, index.linksKey(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Get(1); err == nil {
		t.Fatal("missing links decoded successfully")
	}
	if err := index.Put(2, pageVectorLinksNode()); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(2, pageVectorLinksReplacement()); err != nil || !ok {
		t.Fatalf("PutLinks = %v, %v", ok, err)
	}
	if err := index.Tx.Put(pageVectorBucket, index.linksKey(2), []byte{1, pageVectorLinks}); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Get(2); err == nil {
		t.Fatal("corrupt links decoded successfully")
	}
	if err := index.Put(3, pageVectorLinksNode()); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(3, pageVectorLinksReplacement()); err != nil || !ok {
		t.Fatalf("PutLinks(3) = %v, %v", ok, err)
	}
	base := pageVectorRaw(t, index, 3, false)
	base[1] = 5
	if err := index.Tx.Put(pageVectorBucket, index.key(3), base); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Get(3); err == nil {
		t.Fatal("kind-8 base accepted after kind-5 substitution")
	}
	if err := index.Put(4, pageVectorLinksNode()); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(4, pageVectorLinksReplacement()); err != nil || !ok {
		t.Fatalf("PutLinks(4) = %v, %v", ok, err)
	}
	if err := index.Put(5, pageVectorLinksNode()); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(5, pageVectorLinksReplacement()); err != nil || !ok {
		t.Fatalf("PutLinks(5) = %v, %v", ok, err)
	}
	if err := index.Tx.Put(pageVectorBucket, index.linksKey(5), pageVectorRaw(t, index, 4, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Get(5); err == nil {
		t.Fatal("cross-ID links substitution decoded successfully")
	}
}

func TestPageVectorLinksRejectMalformedTaggedBasePayload(t *testing.T) {
	cases := []struct {
		name       string
		dimensions uint64
		payload    []byte
		maxBytes   uint64
	}{
		{"nan", 2, []byte{0, 0, 0xc0, 0x7f}, 0},
		{"infinity", 2, []byte{0, 0, 0x80, 0x7f}, 0},
		{"truncated", 3, []byte{0, 0, 0, 0}, 0},
		{"extra", 2, make([]byte, 8), 0},
		{"oversized", math.MaxUint16 + 1, nil, 300},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, index := pageVectorLinksFixture(t)
			if err := index.Put(1, pageVectorLinksNode()); err != nil {
				t.Fatal(err)
			}
			if ok, err := index.PutLinks(1, pageVectorLinksReplacement()); err != nil || !ok {
				t.Fatalf("PutLinks = %v, %v", ok, err)
			}
			if err := index.Tx.Put(pageVectorBucket, index.key(1), pageVectorTaggedBase(t, 1, tc.dimensions, tc.payload)); err != nil {
				t.Fatal(err)
			}
			if tc.maxBytes != 0 {
				index.MaxRecordBytes = tc.maxBytes
			}
			if _, err := index.Get(1); err == nil {
				t.Fatal("malformed tagged base decoded successfully")
			}
		})
	}
}

func TestPageVectorLinksFallbackAndWriteHookLeaveRecordsUnchanged(t *testing.T) {
	t.Run("combined-read-budget", func(t *testing.T) {
		_, _, index := pageVectorLinksFixture(t)
		if err := index.Put(1, pageVectorLinksNode()); err != nil {
			t.Fatal(err)
		}
		base := bytes.Clone(pageVectorRaw(t, index, 1, false))
		index.MaxRecordBytes = uint64(len(base)) * 3
		if ok, err := index.PutLinks(1, pageVectorLinksReplacement()); err != nil || ok {
			t.Fatalf("PutLinks = %v, %v", ok, err)
		}
		if got := pageVectorRaw(t, index, 1, false); !bytes.Equal(got, base) || pageVectorRaw(t, index, 1, true) != nil {
			t.Fatal("budget fallback changed records")
		}
	})
	t.Run("fixed-width-zero-vector-expansion", func(t *testing.T) {
		_, _, index := pageVectorLinksFixture(t)
		tiny := &PageVectorNode{Neighbors: [][]uint64{{}}, Vector: make([]float32, 16)}
		replacement := &PageVectorNode{Neighbors: [][]uint64{{2}}, Vector: tiny.Vector}
		if err := index.Put(1, tiny); err != nil {
			t.Fatal(err)
		}
		legacy := bytes.Clone(pageVectorRaw(t, index, 1, false))
		if err := index.Put(2, tiny); err != nil {
			t.Fatal(err)
		}
		if ok, err := index.PutLinks(2, replacement); err != nil || !ok {
			t.Fatalf("sizing PutLinks = %v, %v", ok, err)
		}
		tagged, links := pageVectorRaw(t, index, 2, false), pageVectorRaw(t, index, 2, true)
		if len(tagged) <= len(legacy) {
			t.Fatalf("fixed-width base did not expand: legacy=%d tagged=%d", len(legacy), len(tagged))
		}
		index.MaxRecordBytes = uint64(3 * (len(tagged) + len(links) - 1))
		if ok, err := index.PutLinks(1, replacement); err != nil || ok {
			t.Fatalf("expansion fallback = %v, %v", ok, err)
		}
		if got := pageVectorRaw(t, index, 1, false); !bytes.Equal(got, legacy) || pageVectorRaw(t, index, 1, true) != nil {
			t.Fatal("expansion fallback changed records")
		}
	})
	t.Run("before-write", func(t *testing.T) {
		_, _, index := pageVectorLinksFixture(t)
		if err := index.Put(1, pageVectorLinksNode()); err != nil {
			t.Fatal(err)
		}
		base := bytes.Clone(pageVectorRaw(t, index, 1, false))
		rejected := errors.New("rejected")
		index.BeforeWrite = func(uint64) error { return rejected }
		if ok, err := index.PutLinks(1, pageVectorLinksReplacement()); ok || !errors.Is(err, rejected) {
			t.Fatalf("PutLinks = %v, %v", ok, err)
		}
		if got := pageVectorRaw(t, index, 1, false); !bytes.Equal(got, base) || pageVectorRaw(t, index, 1, true) != nil {
			t.Fatal("BeforeWrite rejection changed records")
		}
	})
}

func TestPageVectorLinksRejectMalformedBaseHeader(t *testing.T) {
	_, _, index := pageVectorLinksFixture(t)
	if err := index.Put(1, pageVectorLinksNode()); err != nil {
		t.Fatal(err)
	}
	if ok, err := index.PutLinks(1, pageVectorLinksReplacement()); err != nil || !ok {
		t.Fatalf("PutLinks = %v, %v", ok, err)
	}
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"wrong-tag", []byte{18, 1, 2, 0, 0, 0, 0}},
		{"missing-id", []byte{17}},
		{"truncated-id", []byte{17, 0x80}},
		{"overflow-id", append([]byte{17}, bytes.Repeat([]byte{0xff}, 10)...)},
		{"wrong-id", []byte{17, 2, 2, 0, 0, 0, 0}},
		{"missing-dimensions", []byte{17, 1}},
		{"truncated-dimensions", []byte{17, 1, 0x80}},
		{"overflow-dimensions", append([]byte{17, 1}, bytes.Repeat([]byte{0xff}, 10)...)},
		{"null-dimensions", []byte{17, 1, 0}},
		{"empty-vector", []byte{17, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := encodePageRecord(pageVectorLinkedBase, func(e *binaryEncoder) { e.write(tc.payload) })
			if err != nil {
				t.Fatal(err)
			}
			if err := index.Tx.Put(pageVectorBucket, index.key(1), data); err != nil {
				t.Fatal(err)
			}
			if _, err := index.Get(1); err == nil {
				t.Fatal("malformed base header accepted")
			}
		})
	}
}
