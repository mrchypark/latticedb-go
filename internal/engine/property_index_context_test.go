package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

type overlapContext struct {
	context.Context
	started sync.Once
	ready   chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (c *overlapContext) Err() error {
	if c.calls.Add(1) != 2 {
		return nil
	}
	c.started.Do(func() {
		close(c.ready)
		<-c.release
	})
	return nil
}

func TestCreatePropertyIndexContextCancellationLeavesStateUnchanged(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "a"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.CreateNodePropertyIndexContext(ctx, "Person", "email"); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateNodePropertyIndexContext error = %v, want cancellation", err)
	}
	if err := db.View(func(tx *Tx) error {
		if tx.graph.NodeProperties.Has(store.PropertyIndexDefinition{Scope: "Person", Property: "email"}) {
			t.Fatal("canceled build published an index")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCreatePropertyIndexContextRetriesGenerationChange(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "old"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}})
		_ = node
		return err
	}); err != nil {
		t.Fatal(err)
	}
	ctx := &overlapContext{Context: context.Background(), ready: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- db.CreateNodePropertyIndexContext(ctx, "Person", "email") }()
	<-ctx.ready
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(1, "email", "new") }); err != nil {
		t.Fatal(err)
	}
	close(ctx.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		ids, ok, err := tx.graph.NodeProperties.Lookup(store.PropertyIndexDefinition{Scope: "Person", Property: "email"}, "new")
		if err != nil || !ok || len(ids) != 1 || ids[0] != 1 {
			t.Fatalf("rebuilt index = ids:%v exists:%v err:%v", ids, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateEdgePropertyIndexContext(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
			return err
		}
		_, err := tx.CreateEdge(1, 2, "LINK", CreateEdgeOptions{Properties: map[string]any{"weight": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndexContext(context.Background(), "LINK", "weight"); err != nil {
		t.Fatal(err)
	}
}
