package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestVectorCacheRejectsCorruptionWithoutPublication(t *testing.T) {
	graph := store.NewGraphState()
	graph.DatabaseID, graph.VectorDimensions = "cache-test", 2
	for id := uint64(1); id <= 8; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: store.PropertiesFromMap(map[string]any{"vector": []float32{float32(id), 0}})})
		if err := insertVectorIndex(graph, id); err != nil {
			t.Fatal(err)
		}
	}
	refreshVectorLiveCount(graph)
	graph.VectorMutations = 3
	var encoded bytes.Buffer
	if err := encodeVectorCache(context.Background(), &encoded, graph, 7, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
		t.Fatal(err)
	}
	original := encoded.Bytes()
	corruptWord := func(offset int, value uint64) func([]byte) []byte {
		return func(b []byte) []byte { binary.LittleEndian.PutUint64(b[offset:], value); return b }
	}
	cases := []struct {
		name     string
		change   func([]byte) []byte
		checksum bool
	}{
		{"version", func(b []byte) []byte { b[7] = '2'; return b }, true},
		{"commit", corruptWord(8, 8), true},
		{"fingerprint", func(b []byte) []byte { b[16] ^= 1; return b }, true},
		{"target-count", corruptWord(48, 2), true},
		{"entry", corruptWord(56, math.MaxUint64), true},
		{"max-level", corruptWord(64, 17), true},
		{"live-count", corruptWord(72, 0), true},
		{"mutation-debt", corruptWord(80, 4097), true},
		{"node-count", corruptWord(88, math.MaxUint64), true},
		{"node-id", corruptWord(96, 0), true},
		{"node-level", corruptWord(104, 17), true},
		{"live-marked-ghost", corruptWord(112, 1), true},
		{"vector", corruptWord(120, uint64(math.Float32bits(77))), true},
		{"nan", corruptWord(120, uint64(math.Float32bits(float32(math.NaN())))), true},
		{"degree", corruptWord(136, 33), true},
		{"self-edge", corruptWord(144, 1), true},
		{"checksum", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }, false},
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }, false},
		{"trailing", func(b []byte) []byte { return append(b, 0) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.change(bytes.Clone(original))
			if tc.checksum {
				sum := sha256.Sum256(data[:len(data)-32])
				copy(data[len(data)-32:], sum[:])
			}
			target := store.CloneGraphState(graph)
			before := store.CloneGraphState(target)
			if err := decodeVectorCache(context.Background(), bytes.NewReader(data), target, 7, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err == nil {
				t.Fatal("corruption accepted")
			}
			if !reflect.DeepEqual(before, target) {
				t.Fatal("rejected cache changed graph")
			}
		})
	}
	t.Run("budgets", func(t *testing.T) {
		for _, limits := range [][2]uint64{{1, defaultVectorBuildMaxLogicalBytes}, {defaultVectorBuildMaxWork, 128 << 10}} {
			target := store.CloneGraphState(graph)
			if err := decodeVectorCache(context.Background(), bytes.NewReader(original), target, 7, limits[0], limits[1]); !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("budget: %v", err)
			}
		}
	})
	target := store.CloneGraphState(graph)
	target.VectorIndex = store.NewVectorIndex()
	target.VectorMutations = 0
	if err := decodeVectorCache(context.Background(), bytes.NewReader(original), target, 7, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
		t.Fatal(err)
	}
	if target.VectorIndex.EntryID != graph.VectorIndex.EntryID || target.VectorIndex.MaxLevel != graph.VectorIndex.MaxLevel || target.VectorIndex.Nodes.Len() != graph.VectorIndex.Nodes.Len() || target.VectorMutations != 3 {
		t.Fatal("valid topology/debt not restored")
	}
	for id, node := range graph.VectorIndex.Nodes.All() {
		loaded := target.VectorIndex.Nodes.Get(id)
		if loaded == nil || node.Level != loaded.Level || !slices.Equal(node.Vector, loaded.Vector) {
			t.Fatalf("node %d differs", id)
		}
		for level, neighbors := range node.Neighbors {
			if !slices.Equal(neighbors, loaded.Neighbors[level]) {
				t.Fatalf("node %d level %d topology differs", id, level)
			}
		}
	}
}

func TestVectorCacheGhostsNamespacesAndCancellation(t *testing.T) {
	graph := store.NewGraphState()
	graph.DatabaseID, graph.VectorDimensions = "ghosts", 2
	key := VectorNamespace{Property: "vector", Scope: "A", Dimensions: 2}
	graph.VectorNamespaces = emptyVectorNamespaceStates([]VectorNamespace{key})
	for id := uint64(1); id <= 8; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Labels: []string{"A"}, Properties: store.PropertiesFromMap(map[string]any{"vector": []float32{float32(id), 1}})})
	}
	refreshVectorLiveCount(graph)
	if err := rebuildAllVectorIndexesBudget(context.Background(), graph, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
		t.Fatal(err)
	}
	deleted := graph.VectorIndex.EntryID
	_, views := vectorCacheViews(graph)
	for _, view := range views {
		tombstoneVectorIndex(view, deleted, nil)
		view.VectorMutations = 2
	}
	graph.VectorTombstones, graph.VectorMutations = views[0].VectorTombstones, 2
	writeVectorNamespaceFacade(graph, key, views[1])
	graph.Nodes.Delete(deleted)
	refreshVectorLiveCount(graph)
	var data bytes.Buffer
	if err := encodeVectorCache(context.Background(), &data, graph, 11, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
		t.Fatal(err)
	}
	target := store.CloneGraphState(graph)
	target.VectorIndex = store.NewVectorIndex()
	target.VectorTombstones = store.NewPagedMap[[]float32]()
	target.VectorNamespaces = emptyVectorNamespaceStates([]VectorNamespace{key})
	refreshVectorLiveCount(target)
	if err := decodeVectorCache(context.Background(), bytes.NewReader(data.Bytes()), target, 11, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
		t.Fatal(err)
	}
	_, loaded := vectorCacheViews(target)
	for _, view := range loaded {
		if view.VectorIndex.EntryID != deleted || view.VectorTombstones.Get(deleted) == nil || view.VectorMutations != 2 {
			t.Fatal("ghost routing entry/debt lost")
		}
	}
	db := &DB{graph: target, enableVector: true, vectorDimensions: 2}
	for _, namespace := range []*VectorNamespace{nil, &key} {
		result, err := db.VectorSearch([]float32{float32(deleted), 1}, VectorSearchOptions{K: 8, Namespace: namespace})
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 7 {
			t.Fatalf("results=%d", len(result))
		}
		for _, item := range result {
			if item.NodeID == deleted {
				t.Fatal("routing ghost returned as live")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := store.CloneGraphState(target)
	reader := &vectorCacheCancelReader{Reader: bytes.NewReader(data.Bytes()), cancel: cancel}
	if err := decodeVectorCache(ctx, reader, target, 11, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if !reflect.DeepEqual(before, target) {
		t.Fatal("cancellation published cache")
	}
}

type vectorCacheCancelReader struct {
	*bytes.Reader
	cancel context.CancelFunc
}

func (r *vectorCacheCancelReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.cancel()
	return n, err
}

func TestVectorCacheCheckpointAndCleanClose(t *testing.T) {
	for _, withContext := range []bool{false, true} {
		t.Run(fmt.Sprint(withContext), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			opts := OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous}
			db, err := Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"v": []float32{1, 0}}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "v", []float32{2, 0}) }); err != nil {
				t.Fatal(err)
			}
			if withContext {
				err = db.CheckpointContext(context.Background())
			} else {
				err = db.Checkpoint()
			}
			if err != nil {
				t.Fatal(err)
			}
			sidecar := db.files.State + "-hnsw"
			data, err := os.ReadFile(sidecar)
			if err != nil {
				t.Fatalf("explicit checkpoint did not cache: %v", err)
			}
			graph := store.CloneGraphState(db.graph)
			graph.VectorMutations = 0
			if err := decodeVectorCache(context.Background(), bytes.NewReader(data), graph, db.commitID, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
				t.Fatal(err)
			}
			if graph.VectorMutations != 1 {
				t.Fatal("checkpoint debt not preserved")
			}
			if db.dirty {
				t.Fatal("checkpoint left dirty graph")
			}
			if err := os.Remove(sidecar); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			if _, err := os.Stat(sidecar); err != nil {
				t.Fatalf("clean Close did not refresh cache: %v", err)
			}
			db, err = Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			if db.graph.VectorMutations != 1 {
				t.Fatal("clean Close cache not used")
			}
		})
	}
}
