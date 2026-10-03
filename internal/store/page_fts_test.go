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

// This budget uses a distinct error so the test can distinguish admission from
// a later checksum/decoder failure in the deliberately damaged source record.
type sourceFTSReadBudget struct {
	work, bytes, maxWork, maxBytes uint64
}

var errSourceFTSReadBudget = errors.New("FTS source admission rejected")

func (b *sourceFTSReadBudget) ChargePageFTS(work, bytes uint64) error {
	if work > b.maxWork-b.work || bytes > b.maxBytes-b.bytes {
		return errSourceFTSReadBudget
	}
	b.work += work
	b.bytes += bytes
	return nil
}
func (b *sourceFTSReadBudget) RemainingPageFTSBytes() uint64 { return b.maxBytes - b.bytes }

func TestVisitFTSAdmitsMaintenanceSourceBeforeDecode(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "source-budget"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	data, err := encodePageRecord(4, func(e *binaryEncoder) {
		e.fts(persistedFTS{NodeID: 1, Text: strings.Repeat("term ", 16<<10)})
	})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(data)
	corrupt[2] ^= 1
	if err := tx.Put("fts", pageID(1), corrupt); err != nil {
		t.Fatal(err)
	}
	graph := NewGraphState()
	graph.PageBase = &PageGraph{Tx: tx}
	for _, test := range []struct {
		name        string
		work, bytes uint64
		want        error
	}{
		{"work", 1, 16 << 20, errSourceFTSReadBudget},
		{"bytes", 16 << 20, 256, ErrLoadResourceLimit},
		{"decode scratch", 16 << 20, uint64(len(data))*2 + 8, ErrLoadResourceLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := &sourceFTSReadBudget{maxWork: test.work, maxBytes: test.bytes}
			visited := false
			err := graph.VisitFTS(WithFTSMaintenanceBudget(context.Background(), budget), func(uint64, *FTSRecord) error { visited = true; return nil })
			if !errors.Is(err, test.want) || visited {
				t.Fatalf("source admission err=%v visited=%v; want %v before decode", err, visited, test.want)
			}
		})
	}
	if err := tx.Put("fts", pageID(1), data); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := graph.PageBase.decodeFTSContext(canceled, 1, data); !errors.Is(err, context.Canceled) {
		t.Fatalf("source decode ignored caller cancellation: %v", err)
	}
	budget := &sourceFTSReadBudget{maxWork: 16 << 20, maxBytes: 16 << 20}
	visited := false
	if err := graph.VisitFTS(WithFTSMaintenanceBudget(context.Background(), budget), func(id uint64, record *FTSRecord) error {
		visited = true
		if id != 1 || len(record.Tokens) != 16<<10 {
			t.Fatalf("id=%d tokens=%d", id, len(record.Tokens))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !visited {
		t.Fatal("admitted source was not visited")
	}
}
