package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/mrchypark/latticedb-go/internal/search"
)

func (page *PageGraph) PutFTS(id uint64, record *FTSRecord) error {
	if err := ValidateEntityID(id); err != nil {
		return err
	}
	old, err := page.Tx.Get("fts", pageID(id))
	if err != nil {
		return err
	}
	if record == nil {
		if old == nil {
			return nil
		}
		if err := page.changeCount("fts", false); err != nil {
			return err
		}
		return page.Tx.Delete("fts", pageID(id))
	}
	node, err := page.GetNode(id)
	if err != nil {
		return err
	}
	if node == nil {
		return errors.New("FTS node is missing")
	}
	if err := ValidateFTSText(record.Text); err != nil {
		return err
	}
	data, err := encodePageRecord(4, func(e *binaryEncoder) { e.fts(persistedFTS{NodeID: id, Text: record.Text}) })
	if err != nil {
		return err
	}
	if old == nil {
		if err := page.changeCount("fts", true); err != nil {
			return err
		}
	}
	return page.Tx.Put("fts", pageID(id), data)
}
func (page *PageGraph) decodeFTS(id uint64, data []byte) (*FTSRecord, error) {
	d, err := decodePageRecord(data, 4, page.recordLimit())
	if err != nil {
		return nil, err
	}
	record := d.fts()
	if err := d.finish(); err != nil {
		return nil, fmt.Errorf("decode page FTS: %w", err)
	}
	if record.NodeID != id {
		return nil, errors.New("FTS page key mismatch")
	}
	tokens, err := search.TokenizeContextWithLimit(context.Background(), record.Text, page.recordLimit())
	if err != nil {
		return nil, err
	}
	return &FTSRecord{Text: record.Text, Tokens: tokens}, nil
}
func (graph *GraphState) ReadFTS(id uint64) (*FTSRecord, error) {
	if graph.DeletedNodes.Get(id) || graph.DeletedFTS.Get(id) {
		return nil, nil
	}
	if record := graph.FTS.Get(id); record != nil {
		return record, nil
	}
	if graph.PageBase == nil {
		return nil, nil
	}
	data, err := graph.PageBase.Tx.Get("fts", pageID(id))
	if err != nil || data == nil {
		return nil, err
	}
	return graph.PageBase.decodeFTS(id, data)
}
func (graph *GraphState) VisitFTS(ctx context.Context, visit func(uint64, *FTSRecord) error) error {
	// FTS overlays are bounded by the current write transaction.
	var overlay PagedMap[pageFTSEntry]
	for id, record := range graph.FTS.All() {
		overlay.Set(id, pageFTSEntry{id, record})
	}
	var base func(func(uint64, pageFTSEntry) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, pageFTSEntry) error) error {
			return graph.PageBase.Tx.Scan(ctx, "fts", nil, nil, func(key, value []byte) error {
				if len(key) != 8 {
					return errors.New("invalid FTS page key")
				}
				id := binary.BigEndian.Uint64(key)
				record, err := graph.PageBase.decodeFTS(id, value)
				if err != nil {
					return err
				}
				return fn(id, pageFTSEntry{id, record})
			})
		}
	}
	return visitOverlay(ctx, &overlay, &graph.DeletedFTS, base, func(entry pageFTSEntry) error {
		if graph.DeletedNodes.Get(entry.id) {
			return nil
		}
		return visit(entry.id, entry.record)
	})
}

type pageFTSEntry struct {
	id     uint64
	record *FTSRecord
}

func (graph *GraphState) FTSCount() (uint64, error) {
	var count uint64
	err := graph.VisitFTS(context.Background(), func(uint64, *FTSRecord) error { count++; return nil })
	return count, err
}
