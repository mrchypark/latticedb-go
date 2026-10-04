package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/search"
)

func (page *PageGraph) PutFTS(id uint64, record *FTSRecord) error {
	return page.PutFTSContext(context.Background(), id, record)
}

func (page *PageGraph) PutFTSContext(ctx context.Context, id uint64, record *FTSRecord) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateEntityID(id); err != nil {
		return err
	}
	old, err := page.getFTSMaintenanceValue(ctx, "fts", pageID(id))
	if err != nil {
		return err
	}
	if record == nil {
		if old == nil {
			return nil
		}
		if err := page.indexManualFTSDocument(ctx, id, nil); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
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
	budget := FTSMaintenanceBudgetFromContext(ctx)
	maxTokenBytes := page.recordLimit()
	if budget != nil {
		if err := budget.ChargePageFTS(uint64(len(record.Text))*5, uint64(len(record.Text))); err != nil {
			return err
		}
		maxTokenBytes = min(maxTokenBytes, budget.RemainingPageFTSBytes())
	}
	tokens, err := page.TokenizeFTSContext(ctx, id, record.Text, maxTokenBytes)
	if err != nil {
		if errors.Is(err, search.ErrTokenizationLimit) {
			return fmt.Errorf("%w: FTS tokenization exceeds page decode limit", ErrLoadResourceLimit)
		}
		return err
	}
	if budget != nil {
		bytes := uint64(len(tokens)) * 48
		for _, token := range tokens {
			bytes += uint64(len(token))
		}
		if err := budget.ChargePageFTS(uint64(len(tokens)), bytes); err != nil {
			return err
		}
	}
	data, err := encodePageRecord(4, func(e *binaryEncoder) { e.fts(persistedFTS{NodeID: id, Text: record.Text}) })
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := page.indexManualFTSDocument(ctx, id, record); err != nil {
		return err
	}
	if old == nil {
		if err := page.changeCount("fts", true); err != nil {
			return err
		}
	}
	return page.Tx.Put("fts", pageID(id), data)
}

func (page *PageGraph) indexManualFTSDocument(ctx context.Context, id uint64, record *FTSRecord) error {
	for _, index := range []struct {
		name   string
		tokens []string
	}{{PageFTSManualStandard, nil}, {PageFTSManualPorter, nil}} {
		if record == nil {
			if err := page.DeleteFTSDocument(ctx, index.name, id); err != nil {
				return err
			}
			continue
		}
		tokens := record.Tokens
		if index.name == PageFTSManualPorter {
			budget := FTSMaintenanceBudgetFromContext(ctx)
			limit := page.recordLimit()
			if budget != nil {
				if err := budget.ChargePageFTS(uint64(len(record.Text))*5, uint64(len(record.Text))); err != nil {
					return err
				}
				limit = min(limit, budget.RemainingPageFTSBytes())
			}
			var err error
			tokens, err = search.AnalyzeEnglishPorterContextWithLimit(ctx, record.Text, limit)
			if err != nil {
				if errors.Is(err, search.ErrTokenizationLimit) {
					return fmt.Errorf("%w: FTS Porter tokenization exceeds maintenance budget", ErrLoadResourceLimit)
				}
				return err
			}
			if budget != nil {
				bytes := uint64(len(tokens)) * 48
				for _, token := range tokens {
					bytes += uint64(len(token))
				}
				if err := budget.ChargePageFTS(uint64(len(tokens)), bytes); err != nil {
					return err
				}
			}
		}
		if err := page.ReplaceFTSDocument(ctx, index.name, id, tokens); err != nil {
			return err
		}
	}
	return nil
}

// TokenizeFTSContext applies both the caller's indexing budget and this page
// transaction's record limits, keeping admission consistent with PutFTS and decodeFTS.
func (page *PageGraph) TokenizeFTSContext(ctx context.Context, id uint64, text string, maxLogicalBytes uint64) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if page.ftsRecordSize(id, text) > page.recordLimit() {
		return nil, ErrLoadResourceLimit
	}
	return search.TokenizeContextWithLimit(ctx, text, min(maxLogicalBytes, page.recordLimit()))
}

func (page *PageGraph) ftsRecordSize(id uint64, text string) uint64 {
	var scratch [binary.MaxVarintLen64]byte
	size := uint64(6) // page-record header
	size += uint64(binary.PutUvarint(scratch[:], id))
	size += uint64(binary.PutUvarint(scratch[:], uint64(len(text))))
	return size + uint64(len(text))
}

func (page *PageGraph) decodeFTS(id uint64, data []byte) (*FTSRecord, error) {
	return page.decodeFTSContext(context.Background(), id, data)
}

func (page *PageGraph) decodeFTSContext(ctx context.Context, id uint64, data []byte) (*FTSRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	budget := FTSMaintenanceBudgetFromContext(ctx)
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
	limit := page.recordLimit()
	if budget != nil {
		limit = min(limit, budget.RemainingPageFTSBytes())
	}
	tokens, err := page.TokenizeFTSContext(ctx, id, record.Text, limit)
	if errors.Is(err, search.ErrTokenizationLimit) {
		return nil, fmt.Errorf("%w: FTS source tokenization exceeds maintenance limit", ErrLoadResourceLimit)
	}
	if err != nil {
		return nil, err
	}
	if budget != nil {
		bytes := saturatingFTSBytes(uint64(len(tokens)), 48)
		for _, token := range tokens {
			bytes = saturatingFTSBytesAdd(bytes, uint64(len(token)))
		}
		if err := budget.ChargePageFTS(uint64(len(tokens)), bytes); err != nil {
			return nil, err
		}
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

func (graph *GraphState) HasFTS(id uint64) (bool, error) {
	if graph.DeletedNodes.Get(id) || graph.DeletedFTS.Get(id) {
		return false, nil
	}
	if graph.FTS.Get(id) != nil {
		return true, nil
	}
	if graph.PageBase == nil {
		return false, nil
	}
	return graph.PageBase.Tx.Has("fts", pageID(id))
}

// VisitFTSRecord keeps the decoded text and token reservations through callback.
func (graph *GraphState) VisitFTSRecord(ctx context.Context, id uint64, visit func(*FTSRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if graph.DeletedNodes.Get(id) || graph.DeletedFTS.Get(id) {
		return visit(nil)
	}
	if record := graph.FTS.Get(id); record != nil {
		return visit(record)
	}
	if graph.PageBase == nil {
		return visit(nil)
	}
	if pageReadBudgetFromContext(ctx) == nil {
		record, err := graph.ReadFTS(id)
		if err != nil {
			return err
		}
		return visit(record)
	}
	page := graph.PageBase
	return page.visitReadRecord(ctx, "fts", id, pageID(id), func(id uint64, data []byte, scope *pageReadScope) error {
		if data == nil {
			return visit(nil)
		}
		record, err := page.decodeFTSRead(ctx, id, data, scope)
		if err != nil {
			return err
		}
		return visit(record)
	})
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
			if pageReadBudgetFromContext(ctx) != nil {
				return graph.PageBase.scanReadRecords(ctx, "fts", func(id uint64, data []byte, scope *pageReadScope) error {
					record, err := graph.PageBase.decodeFTSRead(ctx, id, data, scope)
					if err != nil {
						return err
					}
					return fn(id, pageFTSEntry{id, record})
				})
			}
			var admitKey func(uint64) error
			if budget := FTSMaintenanceBudgetFromContext(ctx); budget != nil {
				admitKey = func(size uint64) error { return budget.ChargePageFTS(1, size) }
			}
			return graph.PageBase.Tx.ScanKeysWithCharge(ctx, "fts", nil, nil, 8, admitKey, func(key []byte) error {
				if len(key) != 8 {
					return errors.New("invalid FTS page key")
				}
				id := binary.BigEndian.Uint64(key)
				limit := graph.PageBase.recordLimit()
				var admit func(uint64) error
				if budget := FTSMaintenanceBudgetFromContext(ctx); budget != nil {
					// Admit raw bytes, the temporary decode buffer, owned text, and source work
					// before the page store copies the value or the decoder runs.
					limit = min(limit, budget.RemainingPageFTSBytes()/3)
					admit = func(size uint64) error {
						return budget.ChargePageFTS(saturatingFTSBytes(size, 5), saturatingFTSBytes(size, 3))
					}
				}
				value, err := graph.PageBase.Tx.GetBoundedWithCharge("fts", key, limit, admit)
				if errors.Is(err, pagestore.ErrValueTooLarge) {
					return fmt.Errorf("%w: FTS source record exceeds maintenance limit", ErrLoadResourceLimit)
				}
				if err != nil {
					return err
				}
				record, err := graph.PageBase.decodeFTSContext(ctx, id, value)
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
