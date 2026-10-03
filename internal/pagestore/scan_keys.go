//go:build aix || android || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

package pagestore

import (
	"bytes"
	"context"
	"errors"
	"io"
)

// ScanKeys visits keys in bytewise order without cloning stored values. It is
// intended for bounded metadata/index walks that subsequently use GetBounded.
func (tx *Tx) ScanKeys(ctx context.Context, bucket string, start, end []byte, visit func([]byte) error) error {
	if ctx == nil || visit == nil {
		return errors.New("pagestore: nil scan context or callback")
	}
	seek, inclusive := start, true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tx.mu.Lock()
		if err := tx.check(); err != nil {
			tx.mu.Unlock()
			return err
		}
		b := tx.tx.Bucket([]byte(bucket))
		var key []byte
		if b != nil {
			c := b.Cursor()
			var value []byte
			if seek == nil {
				key, value = c.First()
			} else {
				key, value = c.Seek(seek)
				if !inclusive && bytes.Equal(key, seek) {
					key, value = c.Next()
				}
			}
			_ = value
			key = bytes.Clone(key)
		}
		tx.mu.Unlock()
		if key == nil || end != nil && bytes.Compare(key, end) >= 0 {
			return nil
		}
		if err := visit(key); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		seek, inclusive = key, false
	}
}
