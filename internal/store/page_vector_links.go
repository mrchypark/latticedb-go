package store

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func (index PageVectorIndex) linksKey(id uint64) []byte {
	key := index.key(id)
	key[0] = 2
	return key
}

// PutLinks preserves the stored vector. Call only for an unchanged immutable
// vector; false requests a full Put. Any error requires transaction rollback.
func (index PageVectorIndex) PutLinks(id uint64, node *PageVectorNode) (bool, error) {
	if node == nil || node.Level < 0 || node.Level >= 17 || node.Level+1 != len(node.Neighbors) {
		return false, errors.New("invalid page vector node")
	}
	encodedBytes := 7 + uvarintBytes(id) + uvarintBytes(uint64(node.Level)) + 1 + uvarintBytes(uint64(len(node.Neighbors)+1))
	for _, neighbors := range node.Neighbors {
		if len(neighbors) > 128 {
			return false, errors.New("page vector degree exceeds limit")
		}
		encodedBytes += uvarintBytes(uint64(len(neighbors) + 1))
		for _, neighbor := range neighbors {
			if neighbor == 0 {
				return false, nil
			}
			encodedBytes += uvarintBytes(neighbor)
		}
	}
	readLimit := index.MaxRecordBytes
	if readLimit == 0 {
		readLimit = 1 << 20
	}
	rawLimit := readLimit / 3
	if uint64(encodedBytes) >= rawLimit {
		return false, nil
	}
	key := index.key(id)
	var taggedBase []byte
	var found, tagBase, fits bool
	var baseBytes uint64
	err := index.Tx.ReadBounded(pageVectorBucket, key, rawLimit, func(data []byte) error {
		found = true
		if len(data) < 2 || (data[1] != 5 && data[1] != pageVectorLinkedBase) {
			return errors.New("invalid page vector base")
		}
		d, err := decodePageRecord(data, data[1], rawLimit)
		if err != nil {
			return err
		}
		baseBytes = uint64(len(data))
		tagBase = data[1] == 5
		if tagBase {
			if _, err := decodePageVectorHeader(d, rawLimit); err != nil {
				return err
			}
			dimensions := d.u()
			if d.err != nil {
				return d.err
			}
			if dimensions <= 1 || dimensions-1 > math.MaxUint16 {
				return errors.New("invalid page vector dimensions")
			}
			baseBytes = uint64(7+uvarintBytes(id)+uvarintBytes(dimensions)) + 4*(dimensions-1)
		} else if d.u() != 17 || d.u() != id || d.err != nil {
			return errors.New("invalid page vector base identity")
		}
		fits = baseBytes <= rawLimit-uint64(encodedBytes)
		return nil
	})
	if errors.Is(err, pagestore.ErrValueTooLarge) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !found {
		return false, errors.New("missing page vector base")
	}
	if !fits {
		return false, nil
	}
	charge := uint64(encodedBytes + 64)
	if tagBase {
		charge += baseBytes + 64
	}
	if index.BeforeWrite != nil {
		if err := index.BeforeWrite(charge); err != nil {
			return false, err
		}
	}
	if tagBase {
		err = index.Tx.ReadBounded(pageVectorBucket, key, rawLimit, func(data []byte) error {
			d, err := decodePageRecord(data, 5, rawLimit)
			if err != nil {
				return err
			}
			if _, err := decodePageVectorHeader(d, rawLimit); err != nil {
				return err
			}
			vector, err := decodePageVectorValues(d, rawLimit)
			if err != nil {
				return err
			}
			taggedBase, err = encodePageRecordCapacity(pageVectorLinkedBase, (&PageGraph{}).recordLimit(), int(baseBytes), func(e *binaryEncoder) {
				// The checksummed tag is outside legacy's valid level range.
				e.u(17)
				e.u(id)
				e.u(uint64(len(vector)) + 1)
				for _, value := range vector {
					binary.LittleEndian.PutUint32(e.scratch[:4], math.Float32bits(value))
					e.write(e.scratch[:4])
				}
			})
			return err
		})
		if err != nil {
			return false, err
		}
	}
	data, err := encodePageRecordCapacity(pageVectorLinks, (&PageGraph{}).recordLimit(), encodedBytes, func(e *binaryEncoder) {
		e.u(18)
		e.u(id)
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
	})
	if err != nil {
		return false, err
	}
	if taggedBase != nil {
		if err := index.Tx.PutOwned(pageVectorBucket, key, taggedBase); err != nil {
			return false, err
		}
	}
	key[0] = 2
	if err := index.Tx.PutOwned(pageVectorBucket, key, data); err != nil {
		return false, err
	}
	return true, nil
}
