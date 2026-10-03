package store

import (
	"bytes"
	"context"
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
	return page.InvalidateFTSIndexNamespace(ctx, "", nil)
}
