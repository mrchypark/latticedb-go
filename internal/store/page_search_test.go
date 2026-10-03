package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestSnapshotReplayClearsDerivedSearchState(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	buckets := []string{pageVectorBucket, pageFTSPostings, pageFTSDocuments, pageFTSStats, pageFTSReady, pageFTSTerms, "search-generation"}
	for _, bucket := range buckets {
		if err := write.Put(bucket, []byte("stale"), []byte("old generation")); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	importer := &pageImporter{db: db, ctx: context.Background()}
	if err := importer.clear(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	for _, bucket := range buckets {
		value, err := read.Get(bucket, []byte("stale"))
		if err != nil || value != nil {
			t.Fatalf("%s retained stale data: %q %v", bucket, value, err)
		}
	}
}
