package engine

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestVectorIndexMDefaultsAndRejectsUnsafeValues(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "default"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := db.graph.VectorIndexM; got != defaultVectorIndexM {
		t.Fatalf("default VectorIndexM = %d, want %d", got, defaultVectorIndexM)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, m := range []uint16{1, maxVectorIndexM + 1} {
		if _, err := Open(filepath.Join(t.TempDir(), "invalid"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorM: m}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("VectorM=%d error = %v, want invalid argument", m, err)
		}
	}
}

func TestVectorIndexMConfiguresGlobalAndNamespaceIndexes(t *testing.T) {
	namespace := VectorNamespace{Property: "embedding", Scope: "A", Dimensions: 2}
	db, err := Open(filepath.Join(t.TempDir(), "configured"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2, VectorM: 3,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 16; i++ {
			labels := []string(nil)
			if i%2 == 0 {
				labels = []string{"A"}
			}
			_, err := tx.CreateNode(CreateNodeOptions{Labels: labels, Properties: map[string]any{"embedding": []float32{float32(i), float32(i % 3)}}})
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertConfiguredVectorDegree(t, db.graph)
	view := vectorNamespaceFacade(db.graph, namespace)
	assertConfiguredVectorDegree(t, view)
}

func TestVectorCacheFingerprintIncludesM(t *testing.T) {
	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	graph.VectorIndexM = 3
	for id := uint64(1); id <= 8; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: store.PropertiesFromMap(map[string]any{"vector": []float32{float32(id), 0}})})
	}
	refreshVectorLiveCount(graph)
	if err := rebuildVectorIndexContext(context.Background(), graph); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := encodeVectorCache(context.Background(), &encoded, graph, 1, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); err != nil {
		t.Fatal(err)
	}
	target := store.CloneGraphState(graph)
	target.VectorIndexM = 4
	before := store.CloneGraphState(target)
	if err := decodeVectorCache(context.Background(), bytes.NewReader(encoded.Bytes()), target, 1, defaultVectorBuildMaxWork, defaultVectorBuildMaxLogicalBytes); !errors.Is(err, errVectorCache) {
		t.Fatalf("decode error = %v, want invalid cache", err)
	}
	if !reflect.DeepEqual(target, before) {
		t.Fatal("cache fingerprint mismatch mutated graph")
	}
}

func TestVectorIndexEstimateScalesWithM(t *testing.T) {
	defaultBytes := estimateVectorIndexBytesForM(1, 2, defaultVectorIndexM)
	if got := estimateVectorIndexBytesForM(1, 2, maxVectorIndexM); got <= defaultBytes {
		t.Fatalf("M=%d estimate = %d, want greater than default %d", maxVectorIndexM, got, defaultBytes)
	}
	if got := vectorBuildScratchBytesForM(maxVectorIndexM); got <= vectorBuildScratchBytesForM(defaultVectorIndexM) {
		t.Fatalf("M=%d scratch = %d, want greater than default", maxVectorIndexM, got)
	}
}

func assertConfiguredVectorDegree(t *testing.T, graph *store.GraphState) {
	t.Helper()
	if err := validateVectorIndex(graph); err != nil {
		t.Fatal(err)
	}
	for _, node := range graph.VectorIndex.Nodes.All() {
		for level, neighbors := range node.Neighbors {
			if got, limit := len(neighbors), vectorIndexMaxNeighbors(graph, level); got > limit {
				t.Fatalf("level %d degree = %d, limit = %d", level, got, limit)
			}
		}
	}
}
