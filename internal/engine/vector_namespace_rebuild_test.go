package engine

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVectorNamespaceRebuildReplaysScopeChangesAndSerializesTargets(t *testing.T) {
	a := VectorNamespace{Property: "embedding", Scope: "A", Dimensions: 2}
	b := VectorNamespace{Property: "embedding", Scope: "B", Dimensions: 2}
	opts := OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{a, b}}
	path := filepath.Join(t.TempDir(), "namespaces")
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	var moved, existingB, added uint64
	if err := db.Update(func(tx *Tx) error {
		for _, item := range []struct {
			scope string
			id    *uint64
		}{{"A", &moved}, {"B", &existingB}} {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{item.scope}, Properties: map[string]any{"embedding": []float32{1, 0}}})
			if err != nil {
				return err
			}
			*item.id = node.ID
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	frozen, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer frozen.Rollback()
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var attempts atomic.Int32
	db.vectorRebuildBeforeBuild = func() {
		if attempts.Add(1) == 1 {
			close(started)
			<-release
		}
	}
	first := make(chan error, 1)
	go func() { first <- db.RebuildVectorIndexNamespaceContext(context.Background(), a) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first namespace rebuild did not start")
	}
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Query("MATCH (n) WHERE id(n) = $id REMOVE n:A", map[string]any{"id": int64(moved)}); err != nil {
			return err
		}
		if _, err := tx.Query("MATCH (n) WHERE id(n) = $id SET n:B", map[string]any{"id": int64(moved)}); err != nil {
			return err
		}
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"A"}, Properties: map[string]any{"embedding": []float32{1, 0}}})
		added = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	second := make(chan error, 1)
	waiting := &rebuildWaitContext{Context: context.Background(), entered: make(chan struct{})}
	go func() { second <- db.RebuildVectorIndexNamespaceContext(waiting, b) }()
	select {
	case <-waiting.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second namespace rebuild did not wait")
	}
	unblock()
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("namespace rebuild did not finish")
		}
	}
	if attempts.Load() != 2 {
		t.Fatalf("rebuild attempts=%d; different namespaces must each rebuild", attempts.Load())
	}
	if frozen.graph.VectorNamespaces[a].LiveCount != 1 || frozen.graph.VectorNamespaces[b].LiveCount != 1 {
		t.Fatal("namespace mutation changed a pinned generation")
	}
	check := func() {
		t.Helper()
		for _, test := range []struct {
			namespace VectorNamespace
			ids       []uint64
		}{{a, []uint64{added}}, {b, []uint64{moved, existingB}}} {
			for _, exact := range []bool{false, true} {
				got, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{Namespace: &test.namespace, K: 10, Exact: exact})
				if err != nil {
					t.Fatal(err)
				}
				ids := make([]uint64, len(got))
				for i, result := range got {
					ids[i] = result.NodeID
				}
				if !slices.Equal(ids, test.ids) {
					t.Fatalf("scope=%s exact=%v IDs=%v want=%v", test.namespace.Scope, exact, ids, test.ids)
				}
			}
		}
		legacy, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{K: 10})
		if err != nil || len(legacy) != 3 {
			t.Fatalf("legacy search=%v error=%v", legacy, err)
		}
	}
	check()
	if err := frozen.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	check()
}
