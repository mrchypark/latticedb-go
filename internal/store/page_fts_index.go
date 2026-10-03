package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/search"
)

const (
	PageFTSManualStandard = "manual:standard"
	PageFTSManualPorter   = "manual:porter"
	pageFTSPostings       = "fts-postings"
	pageFTSDocuments      = "fts-documents"
	pageFTSStats          = "fts-stats"
	pageFTSReady          = "fts-ready"
	pageFTSTerms          = "fts-terms"
)

// PageFTSPosting is one term/document frequency from a persistent FTS index.
type PageFTSPosting struct{ DocumentID, Frequency, Length uint64 }

// PageFTSStats describes the live documents in one page-backed FTS index.
type PageFTSStats struct{ Documents, TotalLength uint64 }

func (page *PageGraph) FTSIndexTokenLimit() uint64 { return page.recordLimit() }

func pageFTSIndexPrefix(index string) []byte {
	return append([]byte{byte(len(index) >> 8), byte(len(index))}, index...)
}
func pageFTSTermPrefix(index, term string) []byte {
	prefix := pageFTSIndexPrefix(index)
	digest := sha256.Sum256([]byte(term))
	return append(prefix, digest[:]...)
}
func pageFTSPostingKey(index, term string, id uint64) []byte {
	return append(pageFTSTermPrefix(index, term), pageID(id)...)
}
func pageFTSDocumentKey(index string, id uint64) []byte {
	return append(pageFTSIndexPrefix(index), pageID(id)...)
}

func (page *PageGraph) FTSIndexReady(index string) (bool, error) {
	data, err := page.Tx.Get(pageFTSReady, pageFTSIndexPrefix(index))
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil
	}
	if len(data) == 1 && data[0] == 1 {
		return false, nil
	} // old term-key layout; rebuild lazily.
	if len(data) != 1 || data[0] != 2 {
		return false, errors.New("invalid FTS index readiness marker")
	}
	return true, nil
}

func (page *PageGraph) SetFTSIndexReady(index string, ready bool) error {
	key := pageFTSIndexPrefix(index)
	if !ready {
		return page.Tx.Delete(pageFTSReady, key)
	}
	return page.Tx.Put(pageFTSReady, key, []byte{2})
}

// InvalidateFTSIndexNamespace makes indexes under prefix unavailable unless
// their physical names are explicitly retained by the active configuration.
func (page *PageGraph) InvalidateFTSIndexNamespace(ctx context.Context, prefix string, retain map[string]struct{}) error {
	return page.Tx.Scan(ctx, pageFTSReady, nil, nil, func(key, _ []byte) error {
		if len(key) < 2 {
			return errors.New("invalid FTS index readiness key")
		}
		n := int(binary.BigEndian.Uint16(key[:2]))
		if len(key) != n+2 {
			return errors.New("invalid FTS index readiness key")
		}
		name := string(key[2:])
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		if _, ok := retain[name]; ok {
			return nil
		}
		return page.Tx.Delete(pageFTSReady, key)
	})
}

func (page *PageGraph) FTSIndexStats(index string) (PageFTSStats, error) {
	var stats PageFTSStats
	data, err := page.Tx.Get(pageFTSStats, pageFTSIndexPrefix(index))
	if err != nil || data == nil {
		return stats, err
	}
	d, err := decodePageRecord(data, 10, 64)
	if err != nil {
		return stats, err
	}
	stats = PageFTSStats{Documents: d.u(), TotalLength: d.u()}
	return stats, d.finish()
}

func (page *PageGraph) ftsDocumentTerms(index string, id uint64) (map[string]uint64, uint64, error) {
	data, err := page.Tx.Get(pageFTSDocuments, pageFTSDocumentKey(index, id))
	if err != nil || data == nil {
		return nil, 0, err
	}
	d, err := decodePageRecord(data, 9, page.recordLimit())
	if err != nil {
		return nil, 0, err
	}
	length, n := d.u(), d.u()
	if n > uint64(page.recordLimit()/2) {
		return nil, 0, ErrLoadResourceLimit
	}
	terms := make(map[string]uint64, int(n))
	for i := uint64(0); i < n; i++ {
		term, frequency := d.str(), d.u()
		if term == "" || frequency == 0 {
			return nil, 0, errors.New("invalid FTS document term")
		}
		if _, exists := terms[term]; exists {
			return nil, 0, errors.New("duplicate FTS document term")
		}
		terms[term] = frequency
	}
	return terms, length, d.finish()
}

// ReplaceFTSDocument updates one document's postings and aggregate statistics
// in the caller's page transaction. The transaction must roll back on error.
func (page *PageGraph) ReplaceFTSDocument(ctx context.Context, index string, id uint64, tokens []string) error {
	return page.replaceFTSDocument(ctx, index, id, tokens, false)
}

func (page *PageGraph) DeleteFTSDocument(ctx context.Context, index string, id uint64) error {
	return page.replaceFTSDocument(ctx, index, id, nil, true)
}

func (page *PageGraph) replaceFTSDocument(ctx context.Context, index string, id uint64, tokens []string, remove bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if index == "" || len(index) > 65535 {
		return errors.New("invalid FTS index name")
	}
	oldTerms, oldLength, err := page.ftsDocumentTerms(index, id)
	if err != nil {
		return err
	}
	stats, err := page.FTSIndexStats(index)
	if err != nil {
		return err
	}
	if stats.TotalLength < oldLength {
		return errors.New("invalid FTS aggregate length")
	}
	newTerms := make(map[string]uint64, len(tokens))
	for _, token := range tokens {
		if token != "" {
			newTerms[token]++
		}
	}
	for term, frequency := range newTerms {
		if err := ctx.Err(); err != nil {
			return err
		}
		if oldTerms == nil || oldTerms[term] == 0 {
			if err := page.changeFTSTermDocuments(index, term, 1); err != nil {
				return err
			}
		}
		value, err := encodePageRecord(11, func(e *binaryEncoder) { e.u(frequency); e.u(uint64(len(tokens))) })
		if err != nil {
			return err
		}
		if err := page.Tx.Put(pageFTSPostings, pageFTSPostingKey(index, term, id), value); err != nil {
			return err
		}
	}
	for term := range oldTerms {
		if newTerms[term] == 0 {
			if err := page.Tx.Delete(pageFTSPostings, pageFTSPostingKey(index, term, id)); err != nil {
				return err
			}
			if err := page.changeFTSTermDocuments(index, term, -1); err != nil {
				return err
			}
		}
	}
	if oldTerms == nil && !remove {
		stats.Documents++
	}
	if oldTerms != nil && remove {
		if stats.Documents == 0 {
			return errors.New("invalid FTS document count")
		}
		stats.Documents--
	}
	stats.TotalLength = stats.TotalLength - oldLength + uint64(len(tokens))
	if remove {
		if err := page.Tx.Delete(pageFTSDocuments, pageFTSDocumentKey(index, id)); err != nil {
			return err
		}
	} else {
		terms := make([]string, 0, len(newTerms))
		for term := range newTerms {
			terms = append(terms, term)
		}
		slices.Sort(terms)
		data, err := encodePageRecord(9, func(e *binaryEncoder) {
			e.u(uint64(len(tokens)))
			e.u(uint64(len(terms)))
			for _, term := range terms {
				e.str(term)
				e.u(newTerms[term])
			}
		})
		if err != nil {
			return err
		}
		if uint64(len(data)) > page.recordLimit() {
			return ErrLoadResourceLimit
		}
		if err := page.Tx.Put(pageFTSDocuments, pageFTSDocumentKey(index, id), data); err != nil {
			return err
		}
	}
	statsData, err := encodePageRecord(10, func(e *binaryEncoder) { e.u(stats.Documents); e.u(stats.TotalLength) })
	if err != nil {
		return err
	}
	return page.Tx.Put(pageFTSStats, pageFTSIndexPrefix(index), statsData)
}

func (page *PageGraph) VisitFTSPostings(ctx context.Context, index, term string, visit func(PageFTSPosting) error) error {
	prefix := pageFTSTermPrefix(index, term)
	return page.Tx.Scan(ctx, pageFTSPostings, prefix, pagePrefixEnd(prefix), func(key, value []byte) error {
		if len(key) != len(prefix)+8 {
			return errors.New("invalid FTS posting key")
		}
		d, err := decodePageRecord(value, 11, 64)
		if err != nil {
			return err
		}
		frequency, length := d.u(), d.u()
		if err := d.finish(); err != nil {
			return err
		}
		if frequency == 0 {
			return errors.New("invalid FTS posting frequency")
		}
		return visit(PageFTSPosting{binary.BigEndian.Uint64(key[len(prefix):]), frequency, length})
	})
}

func (page *PageGraph) VisitFTSVocabulary(ctx context.Context, index string, visit func(string) error) error {
	return page.VisitFTSVocabularyWithLimit(ctx, index, page.recordLimit(), visit)
}

// VisitFTSVocabularyWithLimit checks encoded term size before allocating its string.
func (page *PageGraph) VisitFTSVocabularyWithLimit(ctx context.Context, index string, maxTermBytes uint64, visit func(string) error) error {
	prefix := pageFTSIndexPrefix(index)
	// Bound the raw key/value copy by the caller's remaining term budget plus
	// fixed hash-key and wire-format overhead. The visitor still checks the
	// encoded term length before decoding it into an allocated string.
	termBudget := maxTermBytes
	overhead := uint64(len(prefix) + sha256.Size + 6 + 20)
	if termBudget > ^uint64(0)-overhead {
		maxTermBytes = ^uint64(0)
	} else {
		maxTermBytes = termBudget + overhead
	}
	err := page.Tx.ScanBounded(ctx, pageFTSTerms, prefix, pagePrefixEnd(prefix), maxTermBytes, func(key, value []byte) error {
		if len(key) != len(prefix)+sha256.Size {
			return errors.New("invalid FTS vocabulary key")
		}
		if len(value) < 7 {
			return errors.New("invalid FTS vocabulary value")
		}
		termLength, n := binary.Uvarint(value[6:])
		if n <= 0 || termLength == 0 || termLength > uint64(len(value)-6-n) {
			return errors.New("invalid FTS vocabulary term length")
		}
		if termLength > termBudget {
			return ErrLoadResourceLimit
		}
		decodeLimit := page.recordLimit()
		if decodeLimit <= ^uint64(0)-64 {
			decodeLimit += 64
		}
		d, err := decodePageRecord(value, 12, decodeLimit)
		if err != nil {
			return err
		}
		term := d.str()
		if count := d.u(); count == 0 {
			return errors.New("invalid FTS term document count")
		}
		if err := d.finish(); err != nil {
			return err
		}
		digest := sha256.Sum256([]byte(term))
		if uint64(len(term)) != termLength || !bytes.Equal(digest[:], key[len(prefix):]) {
			return errors.New("FTS vocabulary hash mismatch")
		}
		return visit(term)
	})
	if errors.Is(err, pagestore.ErrValueTooLarge) {
		return ErrLoadResourceLimit
	}
	return err
}

// DropFTSIndex removes all persistent records for an index in bounded batches.
func (page *PageGraph) DropFTSIndex(ctx context.Context, index string) error {
	return page.DropFTSIndexWithCharge(ctx, index, nil)
}

// DropFTSIndexWithCharge charges each physical record removed to the caller's budget.
func (page *PageGraph) DropFTSIndexWithCharge(ctx context.Context, index string, charge func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	prefix := pageFTSIndexPrefix(index)
	for _, bucket := range []string{pageFTSPostings, pageFTSDocuments, pageFTSTerms} {
		for {
			keys := make([][]byte, 0, 128)
			err := page.Tx.Scan(ctx, bucket, prefix, pagePrefixEnd(prefix), func(k, _ []byte) error {
				keys = append(keys, append([]byte(nil), k...))
				if len(keys) == cap(keys) {
					return io.EOF
				}
				return nil
			})
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				break
			}
			for _, key := range keys {
				if charge != nil {
					if err := charge(); err != nil {
						return err
					}
				}
				if err := page.Tx.Delete(bucket, key); err != nil {
					return err
				}
			}
		}
	}
	for _, bucket := range []string{pageFTSStats, pageFTSReady} {
		data, err := page.Tx.Get(bucket, prefix)
		if err != nil {
			return err
		}
		if data == nil {
			continue
		}
		if charge != nil {
			if err := charge(); err != nil {
				return err
			}
		}
		if err := page.Tx.Delete(bucket, prefix); err != nil {
			return err
		}
	}
	return nil
}

func (page *PageGraph) changeFTSTermDocuments(index, term string, delta int64) error {
	key := pageFTSTermPrefix(index, term)
	data, err := page.Tx.Get(pageFTSTerms, key)
	if err != nil {
		return err
	}
	var count uint64
	if data != nil {
		d, err := decodePageRecord(data, 12, page.recordLimit())
		if err != nil {
			return err
		}
		storedTerm := d.str()
		count = d.u()
		if storedTerm != term {
			return errors.New("FTS vocabulary hash collision")
		}
		if err = d.finish(); err != nil {
			return err
		}
	}
	if delta < 0 {
		if count < uint64(-delta) {
			return errors.New("invalid FTS term document count")
		}
		count -= uint64(-delta)
	} else {
		if uint64(delta) > ^uint64(0)-count {
			return errors.New("FTS term document count overflow")
		}
		count += uint64(delta)
	}
	if count == 0 {
		return page.Tx.Delete(pageFTSTerms, key)
	}
	encoded, err := encodePageRecord(12, func(e *binaryEncoder) { e.str(term); e.u(count) })
	if err != nil {
		return err
	}
	return page.Tx.Put(pageFTSTerms, key, encoded)
}

// RebuildManualFTSIndexes explicitly reconstructs the standard and Porter
// postings from legacy FTS records, then marks both indexes complete.
func (page *PageGraph) RebuildManualFTSIndexes(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for _, name := range []string{PageFTSManualStandard, PageFTSManualPorter} {
		if err := page.DropFTSIndex(ctx, name); err != nil {
			return err
		}
	}
	err := page.Tx.Scan(ctx, "fts", nil, nil, func(key, value []byte) error {
		if len(key) != 8 {
			return errors.New("invalid FTS page key")
		}
		id := binary.BigEndian.Uint64(key)
		record, err := page.decodeFTS(id, value)
		if err != nil {
			return err
		}
		if err := page.ReplaceFTSDocument(ctx, PageFTSManualStandard, id, record.Tokens); err != nil {
			return err
		}
		tokens, err := search.AnalyzeEnglishPorterContextWithLimit(ctx, record.Text, page.recordLimit())
		if err != nil {
			return err
		}
		return page.ReplaceFTSDocument(ctx, PageFTSManualPorter, id, tokens)
	})
	if err != nil {
		return err
	}
	for _, name := range []string{PageFTSManualStandard, PageFTSManualPorter} {
		if err := page.SetFTSIndexReady(name, true); err != nil {
			return err
		}
	}
	return nil
}

func (stats PageFTSStats) AverageLength() float64 {
	if stats.Documents == 0 {
		return 0
	}
	return float64(stats.TotalLength) / float64(stats.Documents)
}
