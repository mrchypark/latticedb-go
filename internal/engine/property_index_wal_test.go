package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPropertyIndexSchemaDeltasRecoverAfterCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property-index-schema-delta.ltdb")
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	var alice, bob Node
	var knows Edge
	if err := db.Update(func(tx *Tx) error {
		var err error
		alice, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "alice@example.com"}})
		if err != nil {
			return err
		}
		bob, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "bob@example.com"}})
		if err != nil {
			return err
		}
		knows, err = tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{Properties: map[string]any{"since": int64(2024)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(bob.ID, "email", "alice@example.com"); err != nil {
			return err
		}
		return tx.SetEdgeProperty(knows.ID, "since", int64(2025))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.DropNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(bob.ID, "email", "carol@example.com"); err != nil {
			return err
		}
		return tx.SetEdgeProperty(knows.ID, "since", int64(2026))
	}); err != nil {
		t.Fatal(err)
	}

	// Close without compaction, then add an incomplete frame as a crash tail.
	db.mu.Lock()
	db.recoveryRequired = true
	db.mu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wal, err := os.OpenFile(filepath.Join(path, "wal.log"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Write([]byte{0, 1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.SimulateCrash(path); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		email, ok, err := tx.GetProperty(bob.ID, "email")
		if err != nil || !ok || email != "carol@example.com" {
			t.Fatalf("recovered email = %#v, ok=%v, err=%v", email, ok, err)
		}
		since, ok, err := tx.GetEdgeProperty(knows.ID, "since")
		if err != nil || !ok || since != int64(2026) {
			t.Fatalf("recovered edge property = %#v, ok=%v, err=%v", since, ok, err)
		}
		if _, err := tx.FindNodesByLabelProperty("Person", "email", "carol@example.com", 1); !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("dropped node index lookup = %v", err)
		}
		if _, err := tx.FindEdgesByTypeProperty("KNOWS", "since", int64(2026), 1); !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("dropped edge index lookup = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.SetProperty(alice.ID, "reopened", true)
	}); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.recoveryRequired = true
	db.mu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("second reopen after partial WAL recovery: %v", err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		value, ok, err := tx.GetProperty(alice.ID, "reopened")
		if err != nil || !ok || value != true {
			t.Fatalf("post-recovery commit = %#v, ok=%v, err=%v", value, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPropertyIndexDropWALDeltaIsSchemaSized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property-index-schema-size.ltdb")
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for range 128 {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": "shared", "payload": strings.Repeat("x", 4096)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if growth := after.Size() - before.Size(); growth > 512 {
		t.Fatalf("drop WAL growth = %d bytes, want schema-sized delta", growth)
	}
}
