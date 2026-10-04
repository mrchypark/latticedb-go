package engine

import (
	"errors"
	"fmt"
	"github.com/mrchypark/latticedb-go/internal/store"
)

var errQuerySourceBytes = fmt.Errorf("%w: source byte admission", ErrResourceLimit)

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
		return fmt.Errorf("%w: query source bytes exceed %d", errQuerySourceBytes, budget.maxBytes)
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
	records         map[any]*querySource
	scopes          []*querySourceScope
	roots           map[*querySourceRoot]struct{}
	addedSinceSweep uint64
	sweepInterval   uint64
}
type querySourceScope struct {
	budget  *queryBudget
	records []any
}
type querySourceRoot struct {
	visit    func(func(any))
	rows     []queryRow
	iterator queryIterator
}

func (b *queryBudget) sources() *querySources {
	if b.source == nil {
		b.source = &querySources{records: make(map[any]*querySource), roots: make(map[*querySourceRoot]struct{})}
	}
	return b.source
}

var residentSourceScope querySourceScope

func noSourceCleanup() {}

func (b *queryBudget) sourceScope() *querySourceScope {
	if b.residentSources {
		return &residentSourceScope
	}
	s := &querySourceScope{budget: b}
	b.sources().scopes = append(b.source.scopes, s)
	return s
}
func (s *querySourceScope) reset() {
	if s.budget == nil {
		return
	}
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
	if s.budget == nil {
		return
	}
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
	src.addedSinceSweep++
	if len(src.scopes) > 0 {
		s := src.scopes[len(src.scopes)-1]
		s.records = append(s.records, key)
	}
}
func (b *queryBudget) readNode(g *store.GraphState, id uint64) (*store.NodeRecord, error) {
	if b == nil || b.residentSources {
		return g.ReadNode(id)
	}
	n, l, e := g.ReadNodeOwned(queryScanContext(b), id)
	if retry, err := b.reclaimSourceAdmission(e); err != nil {
		return nil, err
	} else if retry {
		n, l, e = g.ReadNodeOwned(queryScanContext(b), id)
	}
	if e == nil && n != nil {
		b.ownSource(n, l)
	}
	return n, e
}
func (b *queryBudget) readEdge(g *store.GraphState, id uint64) (*store.EdgeRecord, error) {
	if b == nil || b.residentSources {
		return g.ReadEdge(id)
	}
	n, l, e := g.ReadEdgeOwned(queryScanContext(b), id)
	if retry, err := b.reclaimSourceAdmission(e); err != nil {
		return nil, err
	} else if retry {
		n, l, e = g.ReadEdgeOwned(queryScanContext(b), id)
	}
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

// Mutable typed roots avoid making resident evaluator locals escape solely for
// disk tracking. Setters run at each container handoff before another read.
func (b *queryBudget) rowSources(rows []queryRow) *querySourceRoot {
	if b.residentSources {
		return nil
	}
	r := &querySourceRoot{rows: rows}
	b.sources().roots[r] = struct{}{}
	return r
}
func (b *queryBudget) iteratorSources(it queryIterator) *querySourceRoot {
	if b.residentSources {
		return nil
	}
	r := &querySourceRoot{iterator: it}
	b.sources().roots[r] = struct{}{}
	return r
}
func (r *querySourceRoot) setRows(rows []queryRow) {
	if r != nil {
		r.rows = rows
	}
}
func (r *querySourceRoot) setIterator(it queryIterator) {
	if r != nil {
		r.iterator = it
	}
}
func (r *querySourceRoot) close(b *queryBudget) {
	if r != nil {
		delete(b.source.roots, r)
		r.visit = nil
		r.rows = nil
		r.iterator = nil
	}
}

func (b *queryBudget) sourceRoot(visit func(func(any))) func() {
	if b.residentSources {
		return noSourceCleanup
	}
	root := &querySourceRoot{visit: visit}
	b.sources().roots[root] = struct{}{}
	return func() { root.close(b) }
}
func (b *queryBudget) sourceRows(rows func() []queryRow) func() {
	return b.sourceRoot(func(visit func(any)) {
		for _, row := range rows() {
			visit(row)
		}
	})
}

// Pace opportunistic collection by new allocations, rather than row handoffs.
// A fully live materialized batch is marked once, not once per consumed row.
func (b *queryBudget) maybeSweepSources() error {
	if b.source == nil || b.sourceBytes <= uint64(b.maxBytes)/2 || b.source.addedSinceSweep < max(1, b.source.sweepInterval) {
		return nil
	}
	return b.sweepSources()
}

// Retry only a pure owned read after its partial reservations have unwound.
// Never collect from the pagestore charge callback, which holds its lock.
func (b *queryBudget) reclaimSourceAdmission(err error) (bool, error) {
	if !errors.Is(err, errQuerySourceBytes) {
		return false, nil
	}
	before := b.sourceBytes
	if sweepErr := b.sweepSources(); sweepErr != nil {
		return false, sweepErr
	}
	return b.sourceBytes < before, nil
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
		if root.visit != nil {
			root.visit(visit)
		}
		for _, row := range root.rows {
			visit(row)
		}
		if root.iterator != nil {
			visitIteratorSources(root.iterator, visit)
		}
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
	src.addedSinceSweep = 0
	src.sweepInterval = uint64(len(src.records))
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
	if g.PageBase == nil {
		return g.VisitNodes(b.ctx, visit)
	}
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
	if g.PageBase == nil {
		return g.VisitEdges(b.ctx, visit)
	}
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
