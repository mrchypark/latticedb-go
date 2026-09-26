package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func openLifecyclePageDB(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, db.files.State
}

func TestPageSnapshotKeepsOldReadAndReleasesPageTransaction(t *testing.T) {
	db, _ := openLifecyclePageDB(t)
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": "old"}})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldGraph, oldPageTx := snapshot.graph, snapshot.graph.PageBase.Tx
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(nodeID, "value", "new") }); err != nil {
		t.Fatal(err)
	}
	oldNode, err := oldGraph.ReadNode(nodeID)
	if err != nil || oldNode == nil || oldNode.Properties.Get("value") != "old" {
		t.Fatalf("pinned generation node = %#v, %v", oldNode, err)
	}
	newNode, err := db.graph.ReadNode(nodeID)
	if err != nil || newNode == nil || newNode.Properties.Get("value") != "new" {
		t.Fatalf("current generation node = %#v, %v", newNode, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := oldPageTx.Get("catalog", []byte("state")); !errors.Is(err, pagestore.ErrClosed) {
		t.Fatalf("released generation page transaction error = %v, want ErrClosed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageSnapshotGrowthBackpressureReleasesWriter(t *testing.T) {
	db, _ := openLifecyclePageDB(t)
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		result <- db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Large"}, Properties: map[string]any{"payload": strings.Repeat("x", 2<<20)}})
			return err
		})
	}()
	var writeErr error
	select {
	case writeErr = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("page write did not return under pinned-snapshot growth backpressure")
	}
	if !errors.Is(writeErr, ErrResourceLimit) || !errors.Is(writeErr, pagestore.ErrSnapshotGrowth) {
		t.Fatalf("large write with pinned snapshot = %v, want snapshot growth resource error", writeErr)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"AfterRelease"}})
		return err
	}); err != nil {
		t.Fatalf("write after snapshot release: %v", err)
	}
	count, err := db.graph.NodeCount()
	if err != nil || count != 1 {
		t.Fatalf("node count after rejected write and retry = %d, %v", count, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageConcurrentReadTransactionsSharePinnedGeneration(t *testing.T) {
	db, _ := openLifecyclePageDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": "stable"}})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const readers = 8
	ready := make(chan struct{}, readers)
	start := make(chan struct{})
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- db.View(func(tx *Tx) error {
				ready <- struct{}{}
				<-start
				value, ok, err := tx.GetProperty(nodeID, "value")
				if err != nil {
					return err
				}
				if !ok || value != "stable" {
					return fmt.Errorf("read value = %v, present=%t", value, ok)
				}
				return nil
			})
		}()
	}
	for range readers {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(start)
			t.Fatal("concurrent readers did not all enter their transactions")
		}
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent page reads did not finish")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent page read: %v", err)
		}
	}
}

func TestPageReadPropagatesCorruptRecordError(t *testing.T) {
	db, stateFile := openLifecyclePageDB(t)
	path := db.path
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	pages, err := pagestore.Open(stateFile, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], nodeID)
	record, err := write.Get("nodes", key[:])
	if err != nil || len(record) < 6 {
		_ = write.Rollback()
		_ = pages.Close()
		t.Fatalf("read page node record: length=%d error=%v", len(record), err)
	}
	record[2] ^= 1
	if err := write.Put("nodes", key[:], record); err != nil {
		_ = write.Rollback()
		_ = pages.Close()
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.graph.ReadNode(nodeID)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("corrupt page record read error = %v", err)
	}
	if closeErr := db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
}
