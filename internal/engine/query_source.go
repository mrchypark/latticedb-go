package engine

import (
	"fmt"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// Source reads share the query's byte/work ceiling, but not its expression
// scratch counter. Existing expression checkpoints may reset only scratch.
func (budget *queryBudget) ReservePageRead(work, bytes uint64) error {
	// Charge source traversal in 64-byte chunks, as with query comparisons.
	if work != 0 {
		work = 1 + (work-1)/64
	}
	if err := budget.check(work, 0); err != nil {
		return err
	}
	if bytes > budget.RemainingPageReadBytes() {
		return fmt.Errorf("%w: query source bytes exceed %d", ErrResourceLimit, budget.maxBytes)
	}
	budget.sourceBytes += bytes
	return nil
}
func (budget *queryBudget) ReleasePageRead(bytes uint64) { budget.sourceBytes -= bytes }
func (budget *queryBudget) RemainingPageReadBytes() uint64 {
	used := uint64(budget.bytes) + budget.sourceBytes
	return uint64(budget.maxBytes) - min(used, uint64(budget.maxBytes))
}

// A source is keyed by allocation identity, not entity ID: reading the same ID
// twice allocates two records. Borrow scopes cover synchronous use; escaped
// records stay charged until the live row/path containers no longer reach them.
type querySource struct {
	lease   *store.PageReadLease
	escaped bool
	pinned  bool
	mark    bool
}
type querySources struct {
	records map[any]*querySource
	scopes  []*querySourceScope
	roots   map[*querySourceRoot]struct{}
}
type querySourceScope struct {
	budget  *queryBudget
	records []any
}
type querySourceRoot struct{ visit func(func(any)) }

func (b *queryBudget) sources() *querySources {
	if b.source == nil {
		b.source = &querySources{records: make(map[any]*querySource), roots: make(map[*querySourceRoot]struct{})}
	}
	return b.source
}
func (b *queryBudget) sourceScope() *querySourceScope {
	s := &querySourceScope{budget: b}
	b.sources().scopes = append(b.source.scopes, s)
	return s
}
func (s *querySourceScope) reset() {
	src := s.budget.source
	for _, key := range s.records {
		if r := src.records[key]; r != nil && !r.escaped && !r.pinned {
			r.lease.Release()
			delete(src.records, key)
		}
	}
	clear(s.records)
	s.records = s.records[:0]
}
func (s *querySourceScope) close() {
	s.reset()
	src := s.budget.source
	src.scopes[len(src.scopes)-1] = nil
	src.scopes = src.scopes[:len(src.scopes)-1]
}
func (b *queryBudget) ownSource(key any, lease *store.PageReadLease) {
	if lease == nil {
		return
	}
	src := b.sources()
	src.records[key] = &querySource{lease: lease}
	if len(src.scopes) > 0 {
		s := src.scopes[len(src.scopes)-1]
		s.records = append(s.records, key)
	}
}
func (b *queryBudget) readNode(g *store.GraphState, id uint64) (*store.NodeRecord, error) {
	if b == nil {
		return g.ReadNode(id)
	}
	n, l, e := g.ReadNodeOwned(queryScanContext(b), id)
	if e == nil && n != nil {
		b.ownSource(n, l)
	}
	return n, e
}
func (b *queryBudget) readEdge(g *store.GraphState, id uint64) (*store.EdgeRecord, error) {
	if b == nil {
		return g.ReadEdge(id)
	}
	n, l, e := g.ReadEdgeOwned(queryScanContext(b), id)
	if e == nil && n != nil {
		b.ownSource(n, l)
	}
	return n, e
}
func visitQuerySourceValue(value any, visit func(any)) {
	walkQuerySourceValue(value, visit, nil)
}
func walkQuerySourceValue(value any, visit func(any), check func() bool) {
	if check != nil && !check() {
		return
	}
	switch v := value.(type) {
	case *store.NodeRecord:
		if v != nil {
			visit(v)
		}
	case *store.EdgeRecord:
		if v != nil {
			visit(v)
		}
	case boundValue:
		walkQuerySourceValue(v.Node, visit, check)
		walkQuerySourceValue(v.Edge, visit, check)
		if v.HasValue {
			walkQuerySourceValue(v.Value, visit, check)
		}
	case queryRow:
		for _, x := range v.slots {
			walkQuerySourceValue(x, visit, check)
		}
	case []any:
		for _, x := range v {
			walkQuerySourceValue(x, visit, check)
		}
	case map[string]any:
		for _, x := range v {
			walkQuerySourceValue(x, visit, check)
		}
	}
}
func (b *queryBudget) escapeSource(value any) {
	if b.source == nil {
		return
	}
	visitQuerySourceValue(value, func(key any) {
		if r := b.source.records[key]; r != nil {
			r.escaped = true
		}
	})
}
func (b *queryBudget) pinSource(value any) {
	if b == nil || b.source == nil {
		return
	}
	visitQuerySourceValue(value, func(key any) {
		if r := b.source.records[key]; r != nil {
			r.pinned = true
		}
	})
}
func (b *queryBudget) sourceRoot(visit func(func(any))) func() {
	root := &querySourceRoot{visit: visit}
	b.sources().roots[root] = struct{}{}
	return func() { delete(b.source.roots, root) }
}
func (b *queryBudget) sourceRows(rows func() []queryRow) func() {
	return b.sourceRoot(func(visit func(any)) {
		for _, row := range rows() {
			visit(row)
		}
	})
}
func (b *queryBudget) sweepSources() error {
	if b.source == nil || len(b.source.records) == 0 {
		return nil
	}
	src := b.source
	if err := b.check(uint64(len(src.records))/64, 0); err != nil {
		return err
	}
	for _, r := range src.records {
		r.mark = r.pinned
	}
	var err error
	var traversed uint64
	check := func() bool {
		if err != nil {
			return false
		}
		if traversed%64 == 0 {
			err = b.check(1, 0)
		}
		traversed++
		return err == nil
	}
	visit := func(v any) {
		walkQuerySourceValue(v, func(key any) {
			if r := src.records[key]; r != nil {
				r.mark = true
			}
		}, check)
	}

	for root := range src.roots {
		root.visit(visit)
	}
	for _, scope := range src.scopes {
		for _, key := range scope.records {
			if r := src.records[key]; r != nil {
				r.mark = true
			}
		}
	}
	if err != nil {
		return err
	} // An interrupted mark must not release live sources.
	for key, r := range src.records {
		if !r.mark {
			r.lease.Release()
			delete(src.records, key)
		}
	}
	return nil
}
func (b *queryBudget) closeSources() {
	if b.mutationBytes != 0 {
		b.ReleasePageRead(b.mutationBytes)
		b.mutationBytes = 0
	}
	if b.source != nil {
		for _, r := range b.source.records {
			r.lease.Release()
		}
		b.source = nil
	}
}
func (b *queryBudget) visitNodes(g *store.GraphState, visit func(*store.NodeRecord) error) error {
	return g.VisitNodeIDs(queryScanContext(b), func(id uint64) error {
		scope := b.sourceScope()
		defer scope.close()
		n, e := b.readNode(g, id)
		if e != nil || n == nil {
			return e
		}
		return visit(n)
	})
}
func (b *queryBudget) visitEdges(g *store.GraphState, visit func(*store.EdgeRecord) error) error {
	return g.VisitEdgeIDs(queryScanContext(b), func(id uint64) error {
		scope := b.sourceScope()
		defer scope.close()
		n, e := b.readEdge(g, id)
		if e != nil || n == nil {
			return e
		}
		return visit(n)
	})
}

func visitIteratorSources(it queryIterator, visit func(any)) {
	switch it := it.(type) {
	case *sliceQueryIterator:
		visit(it.handoff)
		for _, r := range it.rows {
			visit(r)
		}
	case *patternQueryIterator:
		visit(it.handoff)
		for _, r := range it.pending {
			visit(r)
		}
		visitIteratorSources(it.input, visit)
	case *whereQueryIterator:
		visit(it.handoff)
		for _, r := range it.pending {
			visit(r)
		}
		visitIteratorSources(it.input, visit)
	case *filterQueryIterator:
		visitIteratorSources(it.input, visit)
	case *limitQueryIterator:
		visitIteratorSources(it.input, visit)
	}
}

// Values installed into the transaction may outlive the source binding. Detach
// their payload and keep its allowance separate from expression checkpoints.
func normalizeRetainedMutationValue(value any, budget *queryBudget) (any, uint64, error) {
	normalized, temporary, err := normalizeMutationValue(value, budget)
	if err != nil || budget.sourceBytes == 0 {
		return normalized, temporary, err
	}
	bytes := queryValueBytes(normalized)
	if err := budget.ReservePageRead(0, bytes); err != nil {
		budget.releaseTemporary(temporary)
		return nil, 0, err
	}
	budget.mutationBytes += bytes
	return cloneRetainedQueryValue(normalized), temporary, nil
}
