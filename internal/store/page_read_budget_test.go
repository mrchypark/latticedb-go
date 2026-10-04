package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

type recordReadTestBudget struct{ work, bytes, peak, maxWork, maxBytes uint64 }

var errRecordReadTestBudget = errors.New("canonical record admission rejected")

func (b *recordReadTestBudget) ReservePageRead(work, bytes uint64) error {
	if work > b.maxWork-b.work || bytes > b.maxBytes-b.bytes {
		return errRecordReadTestBudget
	}
	b.work += work
	b.bytes += bytes
	b.peak = max(b.peak, b.bytes)
	return nil
}
func (b *recordReadTestBudget) ReleasePageRead(bytes uint64)   { b.bytes -= bytes }
func (b *recordReadTestBudget) RemainingPageReadBytes() uint64 { return b.maxBytes - b.bytes }

func TestPageReadBudgetAdmitsBeforeCanonicalDecode(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "reads"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	page := &PageGraph{Tx: tx}
	body := strings.Repeat("x", 1<<20)
	if err := page.PutNode(&NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"body": body})}); err != nil {
		t.Fatal(err)
	}
	if err := page.PutEdge(&EdgeRecord{ID: 1, SourceID: 1, TargetID: 1, Type: "SELF", Properties: PropertiesFromMap(map[string]any{"body": body})}); err != nil {
		t.Fatal(err)
	}
	// The wire value fits, but its decoded strings do not. Admission must
	// reject the decoder allocation before the visitor observes a record.
	decoded := &recordReadTestBudget{maxWork: 8 << 20, maxBytes: 2 << 20}
	if err := page.VisitNodes(WithPageReadBudget(context.Background(), decoded), func(*NodeRecord) error {
		t.Fatal("decoded oversized record reached visitor")
		return nil
	}); !errors.Is(err, errRecordReadTestBudget) || decoded.bytes != 0 || decoded.peak > decoded.maxBytes {
		t.Fatalf("decoder admission=%v budget=%+v", err, decoded)
	}
	for _, bucket := range []string{pageNodes, pageEdges} {
		raw, err := tx.Get(bucket, pageID(1))
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 1
		if err := tx.Put(bucket, pageID(1), raw); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"node", "edge", "node-point", "edge-point"} {
		t.Run(kind, func(t *testing.T) {
			low := &recordReadTestBudget{maxWork: 8 << 20, maxBytes: 128 << 10}
			ctx := WithPageReadBudget(context.Background(), low)
			visits := 0
			var err error
			if kind == "node" {
				err = page.VisitNodes(ctx, func(*NodeRecord) error { visits++; return nil })
			} else if kind == "edge" {
				err = page.VisitEdges(ctx, func(*EdgeRecord) error { visits++; return nil })
			} else if kind == "node-point" {
				err = page.VisitNode(ctx, 1, func(*NodeRecord) error { visits++; return nil })
			} else {
				err = page.VisitEdge(ctx, 1, func(*EdgeRecord) error { visits++; return nil })
			}
			if !errors.Is(err, errRecordReadTestBudget) || visits != 0 || low.bytes != 0 {
				t.Fatalf("admission err=%v visits=%d retained=%d", err, visits, low.bytes)
			}
		})
	}
}

func TestPageReadBudgetReleasesEachRecordAndVisitorErrors(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "reads"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	page := &PageGraph{Tx: tx}
	for id := uint64(1); id <= 100; id++ {
		if err := page.PutNode(&NodeRecord{ID: id, Properties: PropertiesFromMap(map[string]any{"x": int64(id)})}); err != nil {
			t.Fatal(err)
		}
	}
	budget := &recordReadTestBudget{maxWork: 1 << 20, maxBytes: 2048}
	ctx := WithPageReadBudget(context.Background(), budget)
	visits := 0
	if err := page.VisitNodes(ctx, func(*NodeRecord) error {
		visits++
		if budget.bytes == 0 {
			t.Fatal("source bytes released before visitor")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visits != 100 || budget.bytes != 0 || budget.work < 100 || budget.peak > 2048 {
		t.Fatalf("visits=%d budget=%+v", visits, budget)
	}
	visitorErr := errors.New("visitor stopped")
	if err := page.VisitNodes(ctx, func(*NodeRecord) error { return visitorErr }); !errors.Is(err, visitorErr) || budget.bytes != 0 {
		t.Fatalf("visitor error=%v held=%d", err, budget.bytes)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := page.VisitNodes(canceled, func(*NodeRecord) error { return nil }); !errors.Is(err, context.Canceled) || budget.bytes != 0 {
		t.Fatalf("cancel=%v held=%d", err, budget.bytes)
	}
}

func TestPropertyPostingsAdmitPrefixRawAndDecode(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "postings"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	page := &PageGraph{Tx: tx}
	text := strings.Repeat("x", 1<<20)
	if err := page.PutNode(&NodeRecord{ID: 1, Labels: []string{"Doc"}, Properties: PropertiesFromMap(map[string]any{"body": text})}); err != nil {
		t.Fatal(err)
	}
	if err := page.PutEdge(&EdgeRecord{ID: 1, SourceID: 1, TargetID: 1, Type: "LINK", Properties: PropertiesFromMap(map[string]any{"body": text})}); err != nil {
		t.Fatal(err)
	}
	for _, node := range []bool{true, false} {
		scope := "Doc"
		if !node {
			scope = "LINK"
		}
		definition := PropertyIndexDefinition{Scope: scope, Property: "body"}
		if err := page.CreatePropertyIndex(context.Background(), node, definition); err != nil {
			t.Fatal(err)
		}
		indexes, err := page.LoadPropertyIndexes(context.Background(), node)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range []bool{true, false} {
			key := text
			if !hit {
				key = strings.Repeat("y", len(text))
			}
			for _, limit := range []uint64{4096, 2 << 20, 3 << 20, 16 << 20} {
				b := &recordReadTestBudget{maxBytes: limit, maxWork: 64 << 20}
				calls := 0
				_, err := indexes.VisitContext(WithPageReadBudget(context.Background(), b), definition, key, nil, func(id uint64) error { calls++; return nil })
				// Misses still need prefix scratch admission, but do not read a posting.
				rejected := limit < 16<<20 && hit || limit <= 2<<20 && !hit
				if rejected {
					if !errors.Is(err, errRecordReadTestBudget) || calls != 0 {
						t.Fatalf("node=%v hit=%v limit=%d calls=%d err=%v", node, hit, limit, calls, err)
					}
				} else if err != nil || hit && calls != 1 || !hit && calls != 0 {
					t.Fatalf("node=%v hit=%v limit=%d calls=%d err=%v", node, hit, limit, calls, err)
				}
				if b.bytes != 0 {
					t.Fatalf("posting source retained %d bytes", b.bytes)
				}
			}
		}
		for _, canceled := range []bool{false, true} {
			b := &recordReadTestBudget{maxBytes: 16 << 20, maxWork: 64 << 20}
			ctx, cancel := context.WithCancel(WithPageReadBudget(context.Background(), b))
			if canceled {
				cancel()
			}
			stop := errors.New("visitor stop")
			_, err := indexes.VisitContext(ctx, definition, text, nil, func(uint64) error { return stop })
			cancel()
			expected := stop
			if canceled {
				expected = context.Canceled
			}
			if !errors.Is(err, expected) || b.bytes != 0 {
				t.Fatalf("cleanup canceled=%v err=%v bytes=%d", canceled, err, b.bytes)
			}
		}
	}
}
