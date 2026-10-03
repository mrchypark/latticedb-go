package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func inspectFTSReadiness(t *testing.T, path string, properties []string, wantProperties bool) {
	t.Helper()
	pages, err := pagestore.Open(path, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	read, err := pages.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	page := &store.PageGraph{Tx: read}
	for _, property := range properties {
		ready, err := page.FTSIndexReady(configuredPropertyFTSPageIndexName(property))
		if err != nil || ready != wantProperties {
			t.Fatalf("property %s ready=%v want=%v err=%v", property, ready, wantProperties, err)
		}
	}
	for _, index := range []string{store.PageFTSManualStandard, store.PageFTSManualPorter} {
		ready, err := page.FTSIndexReady(index)
		if err != nil || !ready {
			t.Fatalf("manual %s ready=%v err=%v", index, ready, err)
		}
	}
}

func TestFTSNamespaceInvalidationUsesOpenBudget(t *testing.T) {
	properties := make([]string, 800)
	for i := range properties {
		properties[i] = fmt.Sprintf("property_%04d", i)
	}
	path := filepath.Join(t.TempDir(), "invalidation")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true, FTSProperties: properties})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
	pagePath := db.files.State
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	inspectFTSReadiness(t, pagePath, properties, true)
	db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 1, DerivedIndexBuildMaxLogicalBytes: 1})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("low-budget reopen=%v", err)
	}
	inspectFTSReadiness(t, pagePath, properties, true)
	db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 10000, DerivedIndexBuildMaxLogicalBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	inspectFTSReadiness(t, pagePath, properties, false)
}

func TestStaleFTSCleanupAndPreparationShareOpenBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale-invalidation")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
	pagePath := db.files.State
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	pages, err := pagestore.Open(pagePath, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := pages.Begin(true)
	if err != nil {
		pages.Close()
		t.Fatal(err)
	}
	if err := write.Delete("search-generation", []byte("history")); err != nil {
		write.Rollback()
		pages.Close()
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		pages.Close()
		t.Fatal(err)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	// The two-marker cleanup alone fits three units. Rebuilding and then
	// inspecting the two new markers needs a shared total of four units.
	pages, err = pagestore.Open(pagePath, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err = pages.Begin(true)
	if err != nil {
		pages.Close()
		t.Fatal(err)
	}
	cleanupBudget := &ftsIndexBudget{maxWork: 3, maxBytes: 1 << 20}
	ctx := store.WithFTSMaintenanceBudget(context.Background(), cleanupBudget)
	if err := (&store.PageGraph{Tx: write}).InvalidateFTSIndexReadiness(ctx); err != nil {
		write.Rollback()
		pages.Close()
		t.Fatal(err)
	}
	if cleanupBudget.work != 2 {
		write.Rollback()
		pages.Close()
		t.Fatalf("cleanup work=%d", cleanupBudget.work)
	}
	if err := write.Rollback(); err != nil {
		pages.Close()
		t.Fatal(err)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 3, DerivedIndexBuildMaxLogicalBytes: 1 << 20})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("cleanup and rebuild did not share budget: %v", err)
	}
	inspectFTSReadiness(t, pagePath, nil, false)
	db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 100, DerivedIndexBuildMaxLogicalBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !db.graph.PageBase.SearchIndexesCurrent {
		t.Fatal("successful rebuild did not refresh source history")
	}
}
