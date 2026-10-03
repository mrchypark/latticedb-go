//go:build aix || android || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

package pagestore

import (
	"bytes"
	"fmt"
)

// GetBounded returns an owned value only when its stored size is within maxBytes.
// It checks the bbolt slice length before cloning or admitting it to the cache.
func (tx *Tx) GetBounded(bucket string, key []byte, maxBytes uint64) ([]byte, error) {
	return tx.GetBoundedWithCharge(bucket, key, maxBytes, nil)
}

// GetBoundedWithCharge admits the value size before caching or cloning it.
// The callback runs only after the stored value is known to fit maxBytes.
func (tx *Tx) GetBoundedWithCharge(bucket string, key []byte, maxBytes uint64, charge func(uint64) error) ([]byte, error) {
	if tx == nil {
		return nil, ErrClosed
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.check(); err != nil {
		return nil, err
	}
	b := tx.tx.Bucket([]byte(bucket))
	if b == nil {
		return nil, nil
	}
	value := b.Get(key)
	if value == nil {
		return nil, nil
	}
	if uint64(len(value)) > maxBytes {
		return nil, fmt.Errorf("%w: bucket %q has %d bytes, limit is %d", ErrValueTooLarge, bucket, len(value), maxBytes)
	}
	if charge != nil {
		if err := charge(uint64(len(value))); err != nil {
			return nil, err
		}
	}
	var ck recordCacheKey
	if !tx.writable && tx.db.cache.limit != 0 {
		ck = recordCacheKey{generation: tx.tx.ID(), bucket: bucket, key: string(key)}
		if cached, ok := tx.db.cache.get(ck); ok {
			return cached, nil
		}
		tx.db.cache.put(ck, value)
	}
	return bytes.Clone(value), nil
}
