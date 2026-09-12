package engine

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// Bump the version when the topology or distance algorithm changes. This file
// is disposable derived state; the canonical snapshot and WAL remain authoritative.
const vectorCacheMagic = "LDBHNSW1"

var errVectorCache = errors.New("invalid vector cache")

// Fixed-width fields avoid attacker-controlled decoder allocations.
type vectorCacheStream struct {
	r    io.Reader
	w    io.Writer
	err  error
	word [8]byte
}

func (s *vectorCacheStream) put(n uint64) {
	if s.err != nil {
		return
	}
	binary.LittleEndian.PutUint64(s.word[:], n)
	_, s.err = s.w.Write(s.word[:])
}
func (s *vectorCacheStream) get() uint64 {
	if s.err != nil {
		return 0
	}
	_, s.err = io.ReadFull(s.r, s.word[:])
	return binary.LittleEndian.Uint64(s.word[:])
}

func vectorCacheViews(graph *store.GraphState) ([]VectorNamespace, []*store.GraphState) {
	keys := sortedVectorNamespaces(graph.VectorNamespaces)
	views := make([]*store.GraphState, 1, len(keys)+1)
	global := *graph
	global.VectorNamespaces = nil
	views[0] = &global
	for _, key := range keys {
		views = append(views, vectorNamespaceFacade(graph, key))
	}
	return keys, views
}

func vectorCacheFingerprint(graph *store.GraphState, keys []VectorNamespace, views []*store.GraphState, budget *directSearchBudget) ([32]byte, error) {
	h := sha256.New()
	s := vectorCacheStream{w: h}
	writeString := func(value string) { s.put(uint64(len(value))); _, _ = io.WriteString(h, value) }
	writeString(graph.DatabaseID)
	s.put(uint64(graph.VectorDimensions))
	s.put(uint64(len(keys)))
	for _, key := range keys {
		writeString(key.Scope)
		writeString(key.Property)
		s.put(uint64(key.Dimensions))
		s.put(uint64(key.Metric))
	}
	for _, view := range views {
		s.put(view.VectorLiveCount)
		for id, node := range graph.Nodes.Ordered() {
			if err := budget.add(1); err != nil {
				return [32]byte{}, err
			}
			vector, ok := selectedVector(view, node)
			if !ok {
				continue
			}
			if err := budget.add(uint64(len(vector))); err != nil {
				return [32]byte{}, err
			}
			s.put(id)
			for _, value := range vector {
				s.put(uint64(math.Float32bits(value)))
			}
		}
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, budget.check()
}

func encodeVectorCache(ctx context.Context, w io.Writer, graph *store.GraphState, commitID, maxWork, maxBytes uint64) error {
	budget := &directSearchBudget{ctx: ctx, maxWork: maxWork, maxBytes: maxBytes}
	keys, views := vectorCacheViews(graph)
	fingerprint, err := vectorCacheFingerprint(graph, keys, views, budget)
	if err != nil {
		return err
	}
	buffered := bufio.NewWriter(w)
	h := sha256.New()
	out := io.MultiWriter(buffered, h)
	if _, err := io.WriteString(out, vectorCacheMagic); err != nil {
		return err
	}
	s := vectorCacheStream{w: out}
	s.put(commitID)
	if _, err := out.Write(fingerprint[:]); err != nil {
		return err
	}
	s.put(uint64(len(views)))
	for _, view := range views {
		s.put(view.VectorIndex.EntryID)
		s.put(uint64(view.VectorIndex.MaxLevel))
		s.put(view.VectorLiveCount)
		s.put(view.VectorMutations)
		s.put(uint64(view.VectorIndex.Nodes.Len()))
		for id, node := range view.VectorIndex.Nodes.Ordered() {
			if err := budget.check(); err != nil {
				return err
			}
			s.put(id)
			s.put(uint64(node.Level))
			ghost := uint64(0)
			if view.VectorTombstones.Get(id) != nil {
				ghost = 1
			}
			s.put(ghost)
			vector, ok := vectorForNode(view, id)
			if !ok || len(vector) != int(graph.VectorDimensions) {
				return errVectorCache
			}
			if err := budget.add(uint64(len(vector)) + 3); err != nil {
				return err
			}
			for _, value := range vector {
				s.put(uint64(math.Float32bits(value)))
			}
			for _, neighbors := range node.Neighbors {
				if err := budget.add(uint64(len(neighbors)) + 1); err != nil {
					return err
				}
				s.put(uint64(len(neighbors)))
				for _, neighbor := range neighbors {
					s.put(neighbor)
				}
			}
		}
	}
	if s.err != nil {
		return s.err
	}
	if _, err := buffered.Write(h.Sum(nil)); err != nil {
		return err
	}
	return buffered.Flush()
}

// Decode into private views and publish only after the complete cache validates.
func decodeVectorCache(ctx context.Context, r io.Reader, graph *store.GraphState, commitID, maxWork, maxBytes uint64) error {
	budget := &directSearchBudget{ctx: ctx, maxWork: maxWork, maxBytes: maxBytes}
	if err := budget.reserveBytes(128 << 10); err != nil {
		return err
	}
	keys, views := vectorCacheViews(graph)
	buffered := bufio.NewReader(r)
	h := sha256.New()
	in := io.TeeReader(buffered, h)
	var magic [8]byte
	if _, err := io.ReadFull(in, magic[:]); err != nil {
		return err
	}
	if string(magic[:]) != vectorCacheMagic {
		return errVectorCache
	}
	s := vectorCacheStream{r: in}
	if s.get() != commitID {
		return errVectorCache
	}
	var recorded [32]byte
	if _, err := io.ReadFull(in, recorded[:]); err != nil {
		return err
	}
	if s.get() != uint64(len(views)) {
		return errVectorCache
	}
	fingerprint, err := vectorCacheFingerprint(graph, keys, views, budget)
	if err != nil {
		return err
	}
	if recorded != fingerprint {
		return errVectorCache
	}
	for _, view := range views {
		index := store.NewVectorIndex()
		index.EntryID = s.get()
		level, live, mutations, count := s.get(), s.get(), s.get(), s.get()
		if s.err != nil {
			return s.err
		}
		if level > vectorIndexMaxLevel || live != view.VectorLiveCount || count < live || count-live > 65536 || mutations > 65536 {
			return errVectorCache
		}
		// Unlike a rebuild, cached IDs and levels are untrusted. Admit sparse
		// radix pages and the second tombstone map before allocating either.
		logicalBytes := saturatingAdd(estimateVectorIndexBytes(count, graph.VectorDimensions), saturatingMul(count, 2048))
		logicalBytes = saturatingAdd(logicalBytes, saturatingMul(count-live, 4096))
		if err := budget.reserveBytes(logicalBytes); err != nil {
			return err
		}
		index.MaxLevel = int(level)
		tombstones := store.NewPagedMap[[]float32]()
		var previous, liveSeen uint64
		for i := uint64(0); i < count; i++ {
			if err := budget.check(); err != nil {
				return err
			}
			id, level, ghost := s.get(), s.get(), s.get()
			if s.err != nil {
				return s.err
			}
			if id <= previous || store.ValidateEntityID(id) != nil || level > vectorIndexMaxLevel || ghost > 1 {
				return errVectorCache
			}
			previous = id
			if err := budget.add(uint64(graph.VectorDimensions) + 3); err != nil {
				return err
			}
			node := &store.VectorIndexNode{Level: int(level), Vector: make([]float32, int(graph.VectorDimensions)), Neighbors: make([][]uint64, int(level)+1)}
			canonical, eligible := selectedVector(view, graph.Nodes.Get(id))
			if (ghost == 0) != eligible {
				return errVectorCache
			}
			for j := range node.Vector {
				bits := s.get()
				value := math.Float32frombits(uint32(bits))
				if bits > math.MaxUint32 || math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) || (eligible && uint32(bits) != math.Float32bits(canonical[j])) {
					return errVectorCache
				}
				node.Vector[j] = value
			}
			for j := range node.Neighbors {
				n := s.get()
				cap := uint64(vectorIndexM)
				if j == 0 {
					cap = vectorIndexM0
				}
				if n > cap {
					return errVectorCache
				}
				if err := budget.add(n + 1); err != nil {
					return err
				}
				node.Neighbors[j] = make([]uint64, int(n))
				for k := range node.Neighbors[j] {
					node.Neighbors[j][k] = s.get()
				}
			}
			if s.err != nil {
				return s.err
			}
			index.Nodes.Set(id, node)
			if ghost == 1 {
				tombstones.Set(id, node.Vector)
			} else {
				liveSeen++
			}
		}
		if liveSeen != live {
			return errVectorCache
		}
		view.VectorIndex, view.VectorTombstones, view.VectorMutations = index, tombstones, mutations
		if uint64(tombstones.Len())+mutations > uint64(vectorRebuildThreshold(view)) {
			return errVectorCache
		}
		if err := validateVectorIndexContext(ctx, view); err != nil {
			return err
		}
		if index.Nodes.Len() != 0 && index.Nodes.Get(index.EntryID).Level != index.MaxLevel {
			return errVectorCache
		}
	}
	expected := h.Sum(nil)
	if _, err := io.ReadFull(buffered, recorded[:]); err != nil {
		return err
	}
	if string(recorded[:]) != string(expected) {
		return errVectorCache
	}
	if _, err := buffered.ReadByte(); err != io.EOF {
		return errVectorCache
	}
	if err := budget.check(); err != nil {
		return err
	}
	graph.VectorIndex, graph.VectorTombstones, graph.VectorMutations = views[0].VectorIndex, views[0].VectorTombstones, views[0].VectorMutations
	for i, key := range keys {
		writeVectorNamespaceFacade(graph, key, views[i+1])
	}
	return nil
}

func loadVectorCache(ctx context.Context, files store.DatabaseFiles, graph *store.GraphState, commitID, maxWork, maxBytes uint64) bool {
	f, err := openVectorCacheFile(files, maxBytes)
	if err != nil {
		return false
	}
	defer f.Close()
	return decodeVectorCache(ctx, f, graph, commitID, maxWork, maxBytes) == nil
}

func (db *DB) saveVectorCache(ctx context.Context, graph *store.GraphState, commitID uint64) {
	if db.readOnly || db.temporary || db.disableVectorIndex || graph.VectorDimensions == 0 {
		return
	}
	// Cache failures must never turn a successful canonical checkpoint into an error.
	_ = publishVectorCacheFile(ctx, db.files, db.vectorIndexBuildMaxLogicalBytes, func(w io.Writer) error {
		return encodeVectorCache(ctx, w, graph, commitID, db.vectorIndexBuildMaxWork, db.vectorIndexBuildMaxLogicalBytes)
	})
}
