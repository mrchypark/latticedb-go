package engine

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestVectorNamespaceRebuildRejectsConcurrentSiblingGrowth(t *testing.T) {
	a := VectorNamespace{Property: "embedding", Scope: "A", Dimensions: 2}
	b := VectorNamespace{Property: "embedding", Scope: "B", Dimensions: 2}
	db, err := Open(filepath.Join(t.TempDir(), "namespace-concurrent-budget"), OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 2,
		VectorIndexMode:  VectorIndexHNSWSynchronous,
		VectorNamespaces: []VectorNamespace{a, b},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var bIDs []uint64
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"A"}, Properties: map[string]any{"embedding": []float32{1, 0}}}); err != nil {
			return err
		}
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"B"}, Properties: map[string]any{"embedding": []float32{1, 0}}})
		if err == nil {
			bIDs = append(bIDs, node.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}

	db.mu.RLock()
	aBefore := captureVectorNamespaceIndex(db.graph, a)
	aView := vectorNamespaceFacade(db.graph, a)
	initialBuild := saturatingAdd(
		retainedVectorIndexBytes(db.graph, &a),
		estimateVectorBuildLogicalBytes(aView, aView.VectorLiveCount),
	)
	db.mu.RUnlock()
	// Leave enough room for the target's build scratch, but less than the
	// persistent bytes added by the concurrent sibling growth.
	db.vectorIndexBuildMaxLogicalBytes = saturatingAdd(initialBuild, vectorBuildScratchBytes+128<<10)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRebuild := func() { releaseOnce.Do(func() { close(release) }) }
	var startOnce sync.Once
	defer releaseRebuild()
	db.vectorRebuildBeforeBuild = func() {
		startOnce.Do(func() { close(started) })
		<-release
	}

	rebuildResult := make(chan error, 1)
	go func() {
		rebuildResult <- db.RebuildVectorIndexNamespaceContext(context.Background(), a)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("namespace A rebuild did not reach before-build gate")
	}

	const siblingGrowth = 64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < siblingGrowth; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{
				Labels:     []string{"B"},
				Properties: map[string]any{"embedding": []float32{float32(i + 2), 0}},
			})
			if err != nil {
				return err
			}
			bIDs = append(bIDs, node.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	releaseRebuild()

	select {
	case err := <-rebuildResult:
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("namespace A rebuild error = %v, want ErrResourceLimit", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("namespace A rebuild did not finish")
	}

	db.mu.RLock()
	aAfter := captureVectorNamespaceIndex(db.graph, a)
	db.mu.RUnlock()
	if aAfter.entryID != aBefore.entryID || aAfter.maxLevel != aBefore.maxLevel || !slices.Equal(aAfter.ids, aBefore.ids) {
		t.Fatalf("namespace A index changed after rejected rebuild: before=%+v after=%+v", aBefore, aAfter)
	}

	results, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{Namespace: &b, K: uint32(len(bIDs))})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(bIDs) {
		t.Fatalf("namespace B search returned %d results, want %d", len(results), len(bIDs))
	}
	gotIDs := make([]uint64, len(results))
	for i, result := range results {
		gotIDs[i] = result.NodeID
	}
	sort.Slice(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] })
	sort.Slice(bIDs, func(i, j int) bool { return bIDs[i] < bIDs[j] })
	for i := range bIDs {
		if gotIDs[i] != bIDs[i] {
			t.Fatalf("namespace B search IDs = %v, want %v", gotIDs, bIDs)
		}
	}
}

type vectorNamespaceIndexSnapshot struct {
	entryID  uint64
	maxLevel int
	ids      []uint64
}

func captureVectorNamespaceIndex(graph *store.GraphState, namespace VectorNamespace) vectorNamespaceIndexSnapshot {
	view := vectorNamespaceFacade(graph, namespace)
	ids := make([]uint64, 0, view.VectorIndex.Nodes.Len())
	for id := range view.VectorIndex.Nodes.All() {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return vectorNamespaceIndexSnapshot{entryID: view.VectorIndex.EntryID, maxLevel: view.VectorIndex.MaxLevel, ids: ids}
}
