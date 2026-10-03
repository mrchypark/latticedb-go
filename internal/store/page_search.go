package store

import (
	"bytes"
	"context"
	"io"
)

// The stamp binds disposable search records to the source history. An older
// writer can update the graph without knowing these buckets; its next commit
// then makes this stamp stale instead of silently preserving a valid index.
func (page *PageGraph) MarkSearchIndexesCurrent(history [32]byte) error {
	value := append([]byte{1}, history[:]...)
	return page.Tx.Put("search-generation", []byte("history"), value)
}

func (page *PageGraph) searchIndexesMatch(history [32]byte) (bool, error) {
	value, err := page.Tx.Get("search-generation", []byte("history"))
	return len(value) == 33 && value[0] == 1 && bytes.Equal(value[1:], history[:]), err
}

// InvalidateFTSIndexReadiness preserves postings until a bounded rebuild can
// replace them. No incomplete index is admitted for candidate selection.
func (page *PageGraph) InvalidateFTSIndexReadiness(ctx context.Context) error {
	for {
		keys := make([][]byte, 0, 128)
		err := page.Tx.Scan(ctx, pageFTSReady, nil, nil, func(key, _ []byte) error {
			keys = append(keys, bytes.Clone(key))
			if len(keys) == cap(keys) {
				return io.EOF
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		for _, key := range keys {
			if err := page.Tx.Delete(pageFTSReady, key); err != nil {
				return err
			}
		}
	}
}
