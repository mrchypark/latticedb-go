package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestPutFTSUsesConfiguredPageRecordLimit(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	node, err := encodePageNode(&NodeRecord{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(pageNodes, pageID(1), node); err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: tx, MaxRecordBytes: 20}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := page.PutFTSContext(canceled, 1, &FTSRecord{Text: "cancelled"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled FTS write error = %v, want context.Canceled", err)
	}
	if err := page.PutFTS(1, &FTSRecord{Text: strings.Repeat(" ", 20)}); !errors.Is(err, ErrLoadResourceLimit) {
		t.Fatalf("custom page record limit error = %v, want ErrLoadResourceLimit", err)
	}
	stored, err := tx.Get("fts", pageID(1))
	if err != nil || stored != nil {
		t.Fatalf("rejected FTS record persisted: %v, %v", stored, err)
	}
}

func TestImportPageCheckpointRejectsUndecodableFTSWithoutPublishing(t *testing.T) {
	state := checkpointScanFixture()
	state.FTS = []persistedFTS{{NodeID: 1, Text: strings.Repeat("a ", 1<<20)}}
	input := checkpointScanFixtureBytes(t, state)
	target := filepath.Join(t.TempDir(), "dense-fts.pages")

	err := ImportPageCheckpoint(context.Background(), bytes.NewReader(input), target)
	if !errors.Is(err, ErrLoadResourceLimit) {
		t.Fatalf("dense FTS migration error = %v, want ErrLoadResourceLimit", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed import published target: %v", err)
	}
}
