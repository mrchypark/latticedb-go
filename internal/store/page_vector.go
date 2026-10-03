package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

const pageVectorBucket = "vector-hnsw"

// PageVectorIndex is a transaction-scoped HNSW view. Each lookup decodes one
// node record; no index-wide resident map is built.
type PageVectorIndex struct {
	Tx             *pagestore.Tx
	Namespace      string
	MaxRecordBytes uint64
}

type PageVectorNode struct {
	Level     int
	Neighbors [][]uint64
	Vector    []float32
	Deleted   bool
}

type PageVectorMeta struct {
	EntryID      uint64
	LastID       uint64
	MaxLevel     int
	Count        uint64
	Mutations    uint64
	DeletedCount uint64
	M            uint16
	Dimensions   uint16
	Valid        bool
}

func (index PageVectorIndex) prefix() []byte {
	h := sha256.Sum256([]byte(index.Namespace))
	return h[:]
}

func (index PageVectorIndex) key(id uint64) []byte {
	key := append([]byte(nil), index.prefix()...)
	key = append(key, 1)
	return append(key, pageID(id)...)
}

func (index PageVectorIndex) Meta() (PageVectorMeta, error) {
	var result PageVectorMeta
	data, err := index.Tx.GetBounded(pageVectorBucket, append(index.prefix(), 0), 4096)
	if err != nil || data == nil {
		return result, err
	}
	d, err := decodePageRecord(data, 4, 4096)
	if err != nil {
		return result, err
	}
	result.EntryID = d.u()
	level := d.u()
	result.Count, result.Mutations = d.u(), d.u()
	result.LastID, result.DeletedCount = d.u(), d.u()
	m, dimensions := d.u(), d.u()
	valid := d.u()
	if d.err != nil {
		return PageVectorMeta{}, d.err
	}
	if level > 16 || m > math.MaxUint16 || dimensions > math.MaxUint16 || valid > 1 {
		return PageVectorMeta{}, errors.New("invalid page vector metadata integer")
	}
	result.MaxLevel, result.M, result.Dimensions, result.Valid = int(level), uint16(m), uint16(dimensions), valid == 1
	if err := d.finish(); err != nil {
		return PageVectorMeta{}, fmt.Errorf("decode page vector metadata: %w", err)
	}
	if (result.Count == 0 && (result.EntryID != 0 || result.MaxLevel != 0 || result.DeletedCount != 0)) || (result.Count != 0 && (result.EntryID == 0 || result.DeletedCount > result.Count)) || (result.Valid && (result.M < 2 || result.M > 64 || result.Dimensions == 0)) {
		return PageVectorMeta{}, errors.New("invalid page vector metadata")
	}
	return result, nil
}

func (index PageVectorIndex) HasMeta() (bool, error) {
	data, err := index.Tx.Get(pageVectorBucket, append(index.prefix(), 0))
	return data != nil, err
}

func (index PageVectorIndex) PutMeta(meta PageVectorMeta) error {
	data, err := encodePageRecord(4, func(e *binaryEncoder) {
		e.u(meta.EntryID)
		e.u(uint64(meta.MaxLevel))
		e.u(meta.Count)
		e.u(meta.Mutations)
		e.u(meta.LastID)
		e.u(meta.DeletedCount)
		e.u(uint64(meta.M))
		e.u(uint64(meta.Dimensions))
		if meta.Valid {
			e.u(1)
		} else {
			e.u(0)
		}
	})
	if err != nil {
		return err
	}
	return index.Tx.Put(pageVectorBucket, append(index.prefix(), 0), data)
}

func (index PageVectorIndex) Get(id uint64) (*PageVectorNode, error) {
	maxBytes := index.MaxRecordBytes
	if maxBytes == 0 {
		maxBytes = 1 << 20
	}
	return index.GetBounded(id, maxBytes)
}

func (index PageVectorIndex) GetBounded(id uint64, maxBytes uint64) (*PageVectorNode, error) {
	if maxBytes < 3 {
		return nil, errors.New("page vector record exceeds read budget")
	}
	rawLimit := maxBytes / 3
	data, err := index.Tx.GetBounded(pageVectorBucket, index.key(id), rawLimit)
	if err != nil || data == nil {
		return nil, err
	}
	d, err := decodePageRecord(data, 5, rawLimit)
	if err != nil {
		return nil, err
	}
	level := d.u()
	deleted := d.u()
	if d.err != nil {
		return nil, d.err
	}
	if level > 16 || deleted > 1 {
		return nil, errors.New("invalid page vector node header")
	}
	countEncoded := d.u()
	if countEncoded == 0 || countEncoded-1 != level+1 {
		return nil, errors.New("invalid page vector level count")
	}
	count := int(countEncoded - 1)
	// Bound slice-header allocation independently from encoded payload bytes.
	if uint64(count)*24 > rawLimit {
		return nil, pagestore.ErrValueTooLarge
	}
	n := &PageVectorNode{Level: int(level), Deleted: deleted == 1, Neighbors: make([][]uint64, count)}
	maxNeighbors := 128
	for i := range n.Neighbors {
		encoded := d.u()
		if d.err != nil {
			return nil, d.err
		}
		if encoded > uint64(maxNeighbors+1) {
			return nil, fmt.Errorf("page vector degree exceeds limit: encoded=%d", encoded)
		}
		degree := 0
		if encoded > 0 {
			degree = int(encoded - 1)
		}
		neighbors := make([]uint64, degree)
		for j := range neighbors {
			neighbors[j] = d.u()
			if d.err != nil {
				return nil, d.err
			}
			if neighbors[j] == 0 {
				return nil, errors.New("invalid page vector neighbor id")
			}
		}
		n.Neighbors[i] = neighbors
	}
	dimensionsEncoded := d.u()
	if d.err != nil {
		return nil, d.err
	}
	if dimensionsEncoded == 0 || dimensionsEncoded-1 == 0 || dimensionsEncoded-1 > math.MaxUint16 {
		return nil, errors.New("invalid page vector dimensions")
	}
	dimensions := int(dimensionsEncoded - 1)
	if uint64(dimensions)*4 > rawLimit {
		return nil, pagestore.ErrValueTooLarge
	}
	n.Vector = make([]float32, dimensions)
	for i := range n.Vector {
		value := d.u()
		if d.err != nil {
			return nil, d.err
		}
		if value > math.MaxUint32 {
			return nil, errors.New("invalid page vector float encoding")
		}
		n.Vector[i] = math.Float32frombits(uint32(value))
		if math.IsNaN(float64(n.Vector[i])) || math.IsInf(float64(n.Vector[i]), 0) {
			return nil, errors.New("non-finite page vector value")
		}
	}
	if err := d.finish(); err != nil {
		return nil, fmt.Errorf("decode page vector node %d: %w", id, err)
	}
	return n, nil
}

func (index PageVectorIndex) Put(id uint64, node *PageVectorNode) error {
	if node == nil || node.Level < 0 || node.Level >= len(node.Neighbors) {
		return errors.New("invalid page vector node")
	}
	data, err := encodePageRecord(5, func(e *binaryEncoder) {
		e.u(uint64(node.Level))
		if node.Deleted {
			e.u(1)
		} else {
			e.u(0)
		}
		e.u(uint64(len(node.Neighbors)) + 1)
		for _, neighbors := range node.Neighbors {
			e.ids(neighbors)
		}
		e.u(uint64(len(node.Vector)) + 1)
		for _, value := range node.Vector {
			e.u(uint64(math.Float32bits(value)))
		}
	})
	if err != nil {
		return err
	}
	return index.Tx.Put(pageVectorBucket, index.key(id), data)
}

func (index PageVectorIndex) Delete(id uint64) error {
	return index.Tx.Delete(pageVectorBucket, index.key(id))
}

func (index PageVectorIndex) Visit(ctx context.Context, visit func(uint64, *PageVectorNode) error) error {
	return index.VisitIDs(ctx, func(id uint64) error {
		node, err := index.Get(id)
		if err != nil {
			return err
		}
		if node == nil {
			return errors.New("missing page vector record")
		}
		return visit(id, node)
	})
}

func (index PageVectorIndex) VisitIDs(ctx context.Context, visit func(uint64) error) error {
	prefix := index.prefix()
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			end = end[:i+1]
			break
		}
	}
	return index.Tx.ScanKeys(ctx, pageVectorBucket, append(prefix, 1), end, func(key []byte) error {
		if len(key) != sha256.Size+9 || key[sha256.Size] != 1 {
			return errors.New("invalid page vector key")
		}
		id := binary.BigEndian.Uint64(key[sha256.Size+1:])
		return visit(id)
	})
}

func (index PageVectorIndex) DeleteAll(ctx context.Context, visit func() error) error {
	return index.VisitIDs(ctx, func(id uint64) error {
		if visit != nil {
			if err := visit(); err != nil {
				return err
			}
		}
		return index.Delete(id)
	})
}

// InvalidatePageVectorIndexesExcept marks every persisted namespace except the
// supplied names invalid. Only metadata records are scanned; node records stay on disk.
func InvalidatePageVectorIndexesExcept(ctx context.Context, tx *pagestore.Tx, active []string) error {
	keep := make(map[[sha256.Size]byte]struct{}, len(active))
	for _, name := range active {
		keep[sha256.Sum256([]byte(name))] = struct{}{}
	}
	return tx.Scan(ctx, pageVectorBucket, nil, nil, func(key, _ []byte) error {
		if len(key) != sha256.Size+1 || key[sha256.Size] != 0 {
			return nil
		}
		var hash [sha256.Size]byte
		copy(hash[:], key[:sha256.Size])
		if _, ok := keep[hash]; ok {
			return nil
		}
		data, err := tx.Get(pageVectorBucket, key)
		if err != nil {
			return err
		}
		d, err := decodePageRecord(data, 4, 4096)
		if err != nil {
			return err
		}
		meta := PageVectorMeta{EntryID: d.u(), MaxLevel: int(d.u()), Count: d.u(), Mutations: d.u(), LastID: d.u(), DeletedCount: d.u(), M: uint16(d.u()), Dimensions: uint16(d.u())}
		valid := d.u()
		if valid > 1 {
			return errors.New("invalid page vector validity flag")
		}
		meta.Valid = valid == 1
		if err := d.finish(); err != nil {
			return err
		}
		if !meta.Valid {
			return nil
		}
		encoded, err := encodePageRecord(4, func(e *binaryEncoder) {
			e.u(meta.EntryID)
			e.u(uint64(meta.MaxLevel))
			e.u(meta.Count)
			e.u(meta.Mutations)
			e.u(meta.LastID)
			e.u(meta.DeletedCount)
			e.u(uint64(meta.M))
			e.u(uint64(meta.Dimensions))
			e.u(0)
		})
		if err != nil {
			return err
		}
		return tx.Put(pageVectorBucket, key, encoded)
	})
}
