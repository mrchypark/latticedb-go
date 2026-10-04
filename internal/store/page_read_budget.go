package store

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/search"
)

// PageReadBudget accounts for source storage owned by a scan, rather than
// resident graph data. Work is cumulative; record storage lasts through visit.
type PageReadBudget interface {
	ReservePageRead(work, bytes uint64) error
	ReleasePageRead(bytes uint64)
	RemainingPageReadBytes() uint64
}

type pageReadBudgetKey struct{}

func WithPageReadBudget(ctx context.Context, budget PageReadBudget) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, pageReadBudgetKey{}, budget)
}

func pageReadBudgetFromContext(ctx context.Context) PageReadBudget {
	if ctx == nil {
		return nil
	}
	budget, _ := ctx.Value(pageReadBudgetKey{}).(PageReadBudget)
	return budget
}

type pageReadScope struct {
	budget PageReadBudget
	held   uint64
}

func (s *pageReadScope) reserve(work, bytes uint64) error {
	if err := s.budget.ReservePageRead(work, bytes); err != nil {
		return err
	}
	s.held += bytes
	return nil
}

func (s *pageReadScope) decoded(bytes uint64) error {
	// In addition to binary decoding, property normalization can own two
	// further representations of arrays/maps. Admit all three before decode.
	return s.reserve(0, multiplySaturated(bytes, 3))
}

// scanReadRecords admits keys, raw values, and decoder allocations before use.
// The visitor must not retain a decoded record beyond its call.
func (page *PageGraph) scanReadRecords(ctx context.Context, bucket string, visit func(uint64, []byte, *pageReadScope) error) error {
	budget := pageReadBudgetFromContext(ctx)
	return page.Tx.ScanKeysWithCharge(ctx, bucket, nil, nil, 8, func(size uint64) error {
		return budget.ReservePageRead(1, 2*size)
	}, func(key []byte) error {
		defer budget.ReleasePageRead(uint64(2 * len(key)))
		if len(key) != 8 {
			return errors.New("invalid page record key")
		}
		return page.visitReadRecord(ctx, bucket, binary.BigEndian.Uint64(key), key, visit)
	})
}

func (page *PageGraph) visitReadRecord(ctx context.Context, bucket string, id uint64, key []byte, visit func(uint64, []byte, *pageReadScope) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	budget := pageReadBudgetFromContext(ctx)
	scope := &pageReadScope{budget: budget}
	defer func() { budget.ReleasePageRead(scope.held) }()
	if err := scope.reserve(0, 256); err != nil {
		return err
	}
	data, err := page.Tx.GetBoundedWithCharge(bucket, key, page.recordLimit(), func(size uint64) error {
		return scope.reserve(size, size)
	})
	if errors.Is(err, pagestore.ErrValueTooLarge) {
		return ErrLoadResourceLimit
	}
	if err != nil {
		return err
	}
	return visit(id, data, scope)
}

func (page *PageGraph) decodeFTSRead(ctx context.Context, id uint64, data []byte, scope *pageReadScope) (*FTSRecord, error) {
	d, err := decodePageRecord(data, 4, page.recordLimit())
	if err != nil {
		return nil, err
	}
	// FTS text does not pass through property normalization.
	d.admitAllocation = func(bytes uint64) error { return scope.reserve(0, bytes) }
	record := d.fts()
	if err := d.finish(); err != nil {
		return nil, err
	}
	if record.NodeID != id {
		return nil, errors.New("FTS page key mismatch")
	}
	// The raw read charged one source pass; admit the tokenizer's remaining
	// passes before its preflight scan or token allocation.
	if err := scope.reserve(multiplySaturated(uint64(len(record.Text)), 4), 0); err != nil {
		return nil, err
	}
	tokens, err := page.TokenizeFTSContext(ctx, id, record.Text, scope.budget.RemainingPageReadBytes())
	if errors.Is(err, search.ErrTokenizationLimit) {
		return nil, ErrLoadResourceLimit
	}
	if err != nil {
		return nil, err
	}
	bytes := multiplySaturated(uint64(len(tokens)), 48)
	for _, token := range tokens {
		bytes = addSaturated(bytes, uint64(len(token)))
	}
	if err := scope.reserve(0, bytes); err != nil {
		return nil, err
	}
	return &FTSRecord{Text: record.Text, Tokens: tokens}, nil
}

// PageReadLease owns a conservative decoder allowance until the caller drops
// the decoded record. It never pins a storage transaction. Release is idempotent.
type PageReadLease struct {
	budget PageReadBudget
	bytes  uint64
}

func (lease *PageReadLease) Release() {
	if lease == nil || lease.budget == nil {
		return
	}
	lease.budget.ReleasePageRead(lease.bytes)
	lease.budget = nil
	lease.bytes = 0
}
func (scope *pageReadScope) take(rawBytes uint64) *PageReadLease {
	// Decoded strings/collections own their backing storage, not the raw buffer.
	scope.budget.ReleasePageRead(rawBytes)
	lease := &PageReadLease{budget: scope.budget, bytes: scope.held - rawBytes}
	scope.held = 0
	return lease
}

func (page *PageGraph) ReadNodeOwned(ctx context.Context, id uint64) (*NodeRecord, *PageReadLease, error) {
	if pageReadBudgetFromContext(ctx) == nil {
		n, err := page.GetNode(id)
		return n, nil, err
	}
	var node *NodeRecord
	var lease *PageReadLease
	err := page.visitReadRecord(ctx, pageNodes, id, pageID(id), func(id uint64, data []byte, scope *pageReadScope) error {
		if data == nil {
			return nil
		}
		var err error
		node, err = decodePageNodeAdmitted(data, id, page.recordLimit(), scope.decoded)
		if err != nil {
			return err
		}
		lease = scope.take(uint64(len(data)))
		return nil
	})
	return node, lease, err
}
func (page *PageGraph) ReadEdgeOwned(ctx context.Context, id uint64) (*EdgeRecord, *PageReadLease, error) {
	if pageReadBudgetFromContext(ctx) == nil {
		e, err := page.GetEdge(id)
		return e, nil, err
	}
	var edge *EdgeRecord
	var lease *PageReadLease
	err := page.visitReadRecord(ctx, pageEdges, id, pageID(id), func(id uint64, data []byte, scope *pageReadScope) error {
		if data == nil {
			return nil
		}
		var err error
		edge, err = decodePageEdgeAdmitted(data, id, page.recordLimit(), scope.decoded)
		if err != nil {
			return err
		}
		lease = scope.take(uint64(len(data)))
		return nil
	})
	return edge, lease, err
}
