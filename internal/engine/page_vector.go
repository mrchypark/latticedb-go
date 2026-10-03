package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// pageVectorSearch walks the persisted graph through point reads. Its memory
// use is bounded by EfSearch and the query budget, independent of index size.
func pageVectorSearch(ctx context.Context, index store.PageVectorIndex, query []float32, k, ef, m int, budget *directSearchBudget) ([]VectorSearchResult, error) {
	index.MaxRecordBytes = pageVectorNodeReadBudget(len(query), m)
	meta, err := index.Meta()
	if err != nil {
		return nil, err
	}
	if !meta.Valid || int(meta.Dimensions) != len(query) || int(meta.M) != m {
		return nil, fmt.Errorf("page HNSW index configuration is invalid")
	}
	if meta.Count == 0 {
		return nil, nil
	}
	if k < 1 {
		k = 10
	}
	if ef < k {
		ef = k
	}
	if ef < 1 {
		ef = vectorIndexSearchEF
	}
	entry := meta.EntryID
	for level := meta.MaxLevel; level > 0; level-- {
		entry, err = pageVectorGreedy(ctx, index, query, entry, level, budget)
		if err != nil {
			return nil, err
		}
	}
	candidates, err := pageVectorLayer(ctx, index, query, entry, 0, ef, 0, budget)
	if err != nil {
		return nil, err
	}
	resultCount := min(k, len(candidates))
	if budget != nil {
		if err := budget.reserveBytes(uint64(resultCount) * 16); err != nil {
			return nil, err
		}
		defer budget.releaseBytes(uint64(resultCount) * 16)
	}
	results := make([]VectorSearchResult, 0, resultCount)
	for _, candidate := range candidates[:resultCount] {
		results = append(results, VectorSearchResult{NodeID: candidate.id, Distance: float32(math.Sqrt(candidate.distance))})
	}
	slices.SortFunc(results, compareVectorResult)
	if len(results) > k {
		results = results[:k]
	}
	return results, nil
}

func pageVectorDistance(ctx context.Context, query, vector []float32, budget *directSearchBudget) (float64, error) {
	if budget != nil {
		if err := budget.add(uint64(len(query))); err != nil {
			return 0, err
		}
	}
	if len(query) >= 256 {
		return search.SquaredVectorDistanceContext(ctx, query, vector)
	}
	return search.SquaredVectorDistance(query, vector)
}

func pageVectorNodeReadBudget(dimensions, m int) uint64 {
	// Float and ID varints may use five and ten bytes per value. This reserves
	// three times the largest valid payload for the owned payload and decoder.
	raw := saturatingAdd(4096, saturatingAdd(saturatingMul(uint64(dimensions), 5), saturatingMul(uint64(m), 180)))
	return saturatingMul(raw, 3)
}

func pageVectorGreedy(ctx context.Context, index store.PageVectorIndex, query []float32, entry uint64, level int, budget *directSearchBudget) (uint64, error) {
	if level < 0 {
		return entry, fmt.Errorf("corrupt page HNSW level %d", level)
	}
	if budget != nil {
		reserve := index.MaxRecordBytes * 2
		if err := budget.reserveBytes(reserve); err != nil {
			return entry, err
		}
		defer budget.releaseBytes(reserve)
	}
	if err := ctx.Err(); err != nil {
		return entry, err
	}
	node, err := index.Get(entry)
	if err != nil {
		return entry, err
	}
	if node == nil {
		return entry, fmt.Errorf("page HNSW entry node %d is missing", entry)
	}
	best, err := pageVectorDistance(ctx, query, node.Vector, budget)
	if err != nil {
		return entry, err
	}
	for {
		if level >= len(node.Neighbors) {
			return entry, fmt.Errorf("corrupt page HNSW node %d lacks level %d", entry, level)
		}
		changed := false
		for _, id := range node.Neighbors[level] {
			candidate, err := index.Get(id)
			if err != nil {
				return entry, err
			}
			if candidate == nil {
				return entry, fmt.Errorf("page HNSW neighbor %d is missing", id)
			}
			if candidate.Level < level {
				return entry, fmt.Errorf("corrupt page HNSW link at level %d targets lower-level node %d", level, id)
			}
			distance, err := pageVectorDistance(ctx, query, candidate.Vector, budget)
			if err != nil {
				return entry, err
			}
			if distance < best {
				entry, best, node, changed = id, distance, candidate, true
			}
		}
		if !changed {
			return entry, nil
		}
	}
}

func pageVectorLayer(ctx context.Context, index store.PageVectorIndex, query []float32, entry uint64, level, ef int, exclude uint64, budget *directSearchBudget) ([]vectorCandidate, error) {
	visitedBytes := uint64(0)
	if budget != nil {
		scratchBytes := saturatingMul(uint64(max(ef, 1)), 128)
		if err := budget.reserveBytes(scratchBytes); err != nil {
			return nil, err
		}
		defer budget.releaseBytes(scratchBytes)
		defer func() { budget.releaseBytes(visitedBytes) }()
	}
	reserveVisited := func() error {
		if budget == nil {
			return nil
		}
		if err := budget.reserveBytes(80 + index.MaxRecordBytes); err != nil {
			return err
		}
		visitedBytes += 80 + index.MaxRecordBytes
		return nil
	}
	if err := reserveVisited(); err != nil {
		return nil, err
	}
	first, err := index.Get(entry)
	if err != nil {
		return nil, err
	}
	if first == nil {
		return nil, fmt.Errorf("page HNSW entry node %d is missing", entry)
	}
	distance, err := pageVectorDistance(ctx, query, first.Vector, budget)
	if err != nil {
		return nil, err
	}
	frontier := vectorCandidateHeap{}
	frontier.push(vectorCandidate{id: entry, distance: distance})
	best := vectorCandidateHeap{max: true}
	if entry != exclude && !first.Deleted {
		best.push(frontier.items[0])
	}
	visited := map[uint64]struct{}{entry: {}}
	for frontier.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := frontier.pop()
		if best.Len() >= ef && current.distance > best.items[0].distance {
			break
		}
		node, err := index.Get(current.id)
		if err != nil {
			return nil, err
		}
		if node == nil {
			return nil, fmt.Errorf("page HNSW node %d is missing", current.id)
		}
		if level >= len(node.Neighbors) {
			return nil, fmt.Errorf("corrupt page HNSW node %d lacks level %d", current.id, level)
		}
		for _, id := range node.Neighbors[level] {
			if _, ok := visited[id]; ok {
				continue
			}
			if err := reserveVisited(); err != nil {
				return nil, err
			}
			visited[id] = struct{}{}
			candidate, err := index.Get(id)
			if err != nil {
				return nil, err
			}
			if candidate == nil {
				return nil, fmt.Errorf("page HNSW neighbor %d is missing", id)
			}
			if candidate.Level < level {
				return nil, fmt.Errorf("corrupt page HNSW link at level %d targets lower-level node %d", level, id)
			}
			d, err := pageVectorDistance(ctx, query, candidate.Vector, budget)
			if err != nil {
				return nil, err
			}
			if best.Len() < ef || d < best.items[0].distance {
				frontier.push(vectorCandidate{id: id, distance: d})
				if id != exclude && !candidate.Deleted {
					best.push(vectorCandidate{id: id, distance: d})
					if best.Len() > ef {
						best.pop()
					}
				}
			}
		}
	}
	slices.SortFunc(best.items, compareVectorCandidate)
	return best.items, nil
}

func selectPageVectorNeighbors(ctx context.Context, index store.PageVectorIndex, candidates []vectorCandidate, limit int, overrideID uint64, overrideVector []float32, budget *directSearchBudget) ([]vectorCandidate, error) {
	selected := make([]vectorCandidate, 0, limit)
	selectedVectors := make([][]float32, 0, limit)
	rejected := make([]vectorCandidate, 0, min(len(candidates), vectorIndexConstructionEF))
	for _, candidate := range candidates {
		vector := overrideVector
		if overrideVector == nil || candidate.id != overrideID {
			node, err := index.Get(candidate.id)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, fmt.Errorf("page HNSW neighbor %d is missing", candidate.id)
			}
			vector = node.Vector
		}
		keep := true
		for _, neighbor := range selectedVectors {
			distance, err := pageVectorDistance(ctx, vector, neighbor, budget)
			if err != nil {
				return nil, err
			}
			if distance <= candidate.distance {
				keep = false
				break
			}
		}
		if keep {
			selected = append(selected, candidate)
			selectedVectors = append(selectedVectors, vector)
			if len(selected) == limit {
				break
			}
		} else {
			rejected = append(rejected, candidate)
		}
	}
	for i := 0; len(selected) < limit && i < len(rejected); i++ {
		vector := overrideVector
		if overrideVector == nil || rejected[i].id != overrideID {
			node, err := index.Get(rejected[i].id)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, fmt.Errorf("page HNSW neighbor %d is missing", rejected[i].id)
			}
			vector = node.Vector
		}
		selected = append(selected, rejected[i])
		selectedVectors = append(selectedVectors, vector)
	}
	return selected, nil
}

func connectPageVectorNeighbor(ctx context.Context, index store.PageVectorIndex, id, neighbor uint64, neighborVector []float32, level, maxNeighbors int, budget *directSearchBudget) error {
	node, err := index.Get(id)
	if err != nil {
		return err
	}
	if node == nil || level >= len(node.Neighbors) || slices.Contains(node.Neighbors[level], neighbor) {
		return err
	}
	if len(node.Neighbors[level]) < maxNeighbors {
		node.Neighbors[level] = append(node.Neighbors[level], neighbor)
		return index.Put(id, node)
	}
	var candidates []vectorCandidate
	for _, candidateID := range node.Neighbors[level] {
		candidate, err := index.Get(candidateID)
		if err != nil {
			return err
		}
		if candidate == nil {
			return fmt.Errorf("page HNSW neighbor %d is missing", candidateID)
		}
		distance, err := pageVectorDistance(ctx, node.Vector, candidate.Vector, budget)
		if err != nil {
			return err
		}
		candidates = append(candidates, vectorCandidate{id: candidateID, distance: distance})
	}
	distance, err := pageVectorDistance(ctx, node.Vector, neighborVector, budget)
	if err != nil {
		return err
	}
	candidates = append(candidates, vectorCandidate{id: neighbor, distance: distance})
	slices.SortFunc(candidates, compareVectorCandidate)
	selected, err := selectPageVectorNeighbors(ctx, index, candidates, maxNeighbors, neighbor, neighborVector, budget)
	if err != nil {
		return err
	}
	node.Neighbors[level] = node.Neighbors[level][:0]
	for _, item := range selected {
		node.Neighbors[level] = append(node.Neighbors[level], item.id)
	}
	return index.Put(id, node)
}

// rebuildPageVectorIndex replaces one namespace atomically in the caller's
// page transaction. The work and temporary graph are bounded by supplied limits.
type pageVectorBuildBudget struct{ persistentBytes, stagedBytes, maxPersistentBytes, maxStagedBytes uint64 }

func (b *pageVectorBuildBudget) chargePersistent(n uint64) error {
	if b.persistentBytes > b.maxPersistentBytes || n > b.maxPersistentBytes-b.persistentBytes {
		return fmt.Errorf("%w: page HNSW persistent size exceeds %d bytes", ErrResourceLimit, b.maxPersistentBytes)
	}
	b.persistentBytes += n
	return nil
}
func (b *pageVectorBuildBudget) chargeStaged(n uint64) error {
	if b.stagedBytes > b.maxStagedBytes || n > b.maxStagedBytes-b.stagedBytes {
		return fmt.Errorf("%w: page HNSW transaction staging exceeds %d bytes", ErrResourceLimit, b.maxStagedBytes)
	}
	b.stagedBytes += n
	return nil
}

func rebuildPageVectorIndex(ctx context.Context, graph *store.PageGraph, namespace string, selectVector func(*store.NodeRecord) ([]float32, bool), dimensions uint16, m int, maxPersistentBytes uint64, budget *directSearchBudget) error {
	return rebuildPageVectorIndexWithBudget(ctx, graph, namespace, selectVector, dimensions, m, &pageVectorBuildBudget{maxPersistentBytes: maxPersistentBytes, maxStagedBytes: maxPersistentBytes}, budget)
}

func rebuildPageVectorIndexWithBudget(ctx context.Context, graph *store.PageGraph, namespace string, selectVector func(*store.NodeRecord) ([]float32, bool), dimensions uint16, m int, buildBudget *pageVectorBuildBudget, budget *directSearchBudget) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if m < minVectorIndexM || m > maxVectorIndexM {
		return fmt.Errorf("invalid HNSW M %d", m)
	}
	index := store.PageVectorIndex{Tx: graph.Tx, Namespace: namespace}
	index.MaxRecordBytes = pageVectorNodeReadBudget(int(dimensions), m)
	index.BeforeWrite = buildBudget.chargeStaged
	if err := index.DeleteAll(ctx, func() error {
		if budget != nil {
			if e := budget.add(1); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		return err
	}
	meta := store.PageVectorMeta{}
	meta.M, meta.Dimensions, meta.Valid = uint16(m), dimensions, true
	var buildErr error
	err := graph.VisitNodes(ctx, func(node *store.NodeRecord) error {
		if buildErr != nil {
			return buildErr
		}
		if budget != nil {
			if e := budget.add(1); e != nil {
				return e
			}
		}
		vector, ok := selectVector(node)
		if !ok {
			return nil
		}
		entryBytes := estimateVectorIndexBytesForM(1, dimensions, uint16(m))
		if e := buildBudget.chargePersistent(entryBytes); e != nil {
			return e
		}
		if budget != nil {
			if e := budget.add(uint64(len(vector))); e != nil {
				buildErr = e
				return e
			}
		}
		buildErr = insertPageVector(ctx, index, &meta, node.ID, vector, m, budget)
		return buildErr
	})
	if err != nil {
		return err
	}
	if buildErr != nil {
		return buildErr
	}
	meta.Mutations = 0
	meta.Valid = true
	return index.PutMeta(meta)
}

// applyPageVectorChanges is called inside the same writable page transaction
// as node changes so rollback and snapshot publication cover both atomically.
func applyPageVectorChanges(ctx context.Context, page *store.PageGraph, before, after *store.GraphState, ids []uint64, budget *directSearchBudget) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m := configuredVectorIndexM(after)
	type target struct {
		name       string
		dimensions uint16
	}
	targets := []target{{name: "default", dimensions: after.VectorDimensions}}
	for _, ns := range sortedVectorNamespaces(after.VectorNamespaces) {
		targets = append(targets, target{name: pageVectorNamespaceKey(&ns), dimensions: ns.Dimensions})
	}
	buildBudget := &pageVectorBuildBudget{maxPersistentBytes: ^uint64(0), maxStagedBytes: ^uint64(0)}
	if budget != nil {
		buildBudget.maxPersistentBytes = budget.maxBytes
		buildBudget.maxStagedBytes = budget.maxBytes
	}
	// Include every active namespace's current persistent footprint before
	// reserving additions and staged neighbor rewrites for this transaction.
	for _, target := range targets {
		if budget != nil {
			if err := budget.add(1); err != nil {
				return err
			}
		}
		index := store.PageVectorIndex{Tx: page.Tx, Namespace: target.name}
		meta, err := index.Meta()
		if err != nil {
			return err
		}
		entryBytes := estimateVectorIndexBytesForM(1, target.dimensions, uint16(m))
		if err = buildBudget.chargePersistent(saturatingMul(meta.Count, entryBytes)); err != nil {
			return err
		}
	}
	chargeChange := func(name string, dimensions uint16, id uint64, adding bool) error {
		if budget != nil {
			if err := budget.add(1); err != nil {
				return err
			}
		}
		entryBytes := estimateVectorIndexBytesForM(1, dimensions, uint16(m))
		if adding {
			if err := buildBudget.chargePersistent(entryBytes); err != nil {
				return err
			}
		}
		return nil
	}
	// Preflight all source changes and aggregate costs before invalidation or
	// any HNSW record is staged in the page transaction.
	namespaces := sortedVectorNamespaces(after.VectorNamespaces)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if budget != nil {
			if err := budget.add(1); err != nil {
				return err
			}
		}
		oldNode, err := before.ReadNode(id)
		if err != nil {
			return err
		}
		newNode, err := after.ReadNode(id)
		if err != nil {
			return err
		}
		oldVector, oldOK := selectedVector(before, oldNode)
		newVector, newOK := selectedVector(after, newNode)
		if oldOK != newOK || !slices.Equal(oldVector, newVector) {
			if err := chargeChange("default", after.VectorDimensions, id, !oldOK && newOK); err != nil {
				return err
			}
		}
		for _, ns := range namespaces {
			if budget != nil {
				if err := budget.add(1); err != nil {
					return err
				}
			}
			oldView, newView := vectorNamespaceFacade(before, ns), vectorNamespaceFacade(after, ns)
			oldView.VectorDimensions, newView.VectorDimensions = ns.Dimensions, ns.Dimensions
			oldVector, oldOK = selectedVector(oldView, oldNode)
			newVector, newOK = selectedVector(newView, newNode)
			if oldOK != newOK || !slices.Equal(oldVector, newVector) {
				if err := chargeChange(pageVectorNamespaceKey(&ns), ns.Dimensions, id, !oldOK && newOK); err != nil {
					return err
				}
			}
		}
	}
	if err := invalidateInactivePageVectorIndexesBudget(ctx, page, sortedVectorNamespaces(after.VectorNamespaces), budget, buildBudget); err != nil {
		return err
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if budget != nil {
			if err := budget.add(1); err != nil {
				return err
			}
		}
		oldNode, err := before.ReadNode(id)
		if err != nil {
			return err
		}
		newNode, err := after.ReadNode(id)
		if err != nil {
			return err
		}
		oldVector, oldOK := selectedVector(before, oldNode)
		newVector, newOK := selectedVector(after, newNode)
		if oldOK != newOK || !slices.Equal(oldVector, newVector) {
			if err := applyPageVectorChange(ctx, page, "default", id, newVector, newOK, m, budget, buildBudget); err != nil {
				return err
			}
		}
		for _, ns := range namespaces {
			if budget != nil {
				if err := budget.add(1); err != nil {
					return err
				}
			}
			newView := vectorNamespaceFacade(after, ns)
			newView.VectorDimensions = ns.Dimensions
			oldView := vectorNamespaceFacade(before, ns)
			oldView.VectorDimensions = ns.Dimensions
			oldVector, oldOK = selectedVector(oldView, oldNode)
			newVector, newOK = selectedVector(newView, newNode)
			if oldOK == newOK && slices.Equal(oldVector, newVector) {
				continue
			}
			if err := applyPageVectorChange(ctx, page, pageVectorNamespaceKey(&ns), id, newVector, newOK, m, budget, buildBudget); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyPageVectorChange(ctx context.Context, page *store.PageGraph, namespace string, id uint64, vector []float32, present bool, m int, budget *directSearchBudget, buildBudget *pageVectorBuildBudget) error {
	index := store.PageVectorIndex{Tx: page.Tx, Namespace: namespace, BeforeWrite: buildBudget.chargeStaged}
	meta, err := index.Meta()
	if err != nil {
		return err
	}
	dimensions := meta.Dimensions
	if present {
		dimensions = uint16(len(vector))
	}
	if dimensions != 0 {
		index.MaxRecordBytes = pageVectorNodeReadBudget(int(dimensions), m)
	}
	if ok, err := index.HasMeta(); err != nil {
		return err
	} else if !ok {
		meta.M, meta.Dimensions, meta.Valid = uint16(m), uint16(len(vector)), true
		if err := index.PutMeta(meta); err != nil {
			return err
		}
	}
	if err := mutatePageVectorIndex(ctx, index, &meta, id, vector, present, m, budget); err != nil {
		return err
	}
	return nil
}

// preparePageVectorIndexes builds absent indexes and validates existing node
// records in the transaction used by page open. Corruption is returned to the
// caller; it must not trigger an exact-search fallback.
func preparePageVectorIndexes(ctx context.Context, page *store.PageGraph, graph *store.GraphState, maxWork, maxBytes uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	budget := &directSearchBudget{ctx: ctx, maxWork: maxWork, maxBytes: maxBytes, annVisitedLimit: ^uint64(0)}
	targets := []struct {
		name         string
		dimensions   uint16
		selectVector func(*store.NodeRecord) ([]float32, bool)
	}{{name: "default", dimensions: graph.VectorDimensions, selectVector: func(n *store.NodeRecord) ([]float32, bool) { return selectedVector(graph, n) }}}
	for _, namespace := range sortedVectorNamespaces(graph.VectorNamespaces) {
		ns := namespace
		view := vectorNamespaceFacade(graph, ns)
		view.VectorDimensions = ns.Dimensions
		targets = append(targets, struct {
			name         string
			dimensions   uint16
			selectVector func(*store.NodeRecord) ([]float32, bool)
		}{name: pageVectorNamespaceKey(&ns), dimensions: ns.Dimensions, selectVector: func(n *store.NodeRecord) ([]float32, bool) { return selectedVector(view, n) }})
	}
	buildBudget := &pageVectorBuildBudget{maxPersistentBytes: maxBytes, maxStagedBytes: maxBytes}
	for _, target := range targets {
		index := store.PageVectorIndex{Tx: page.Tx, Namespace: target.name}
		index.MaxRecordBytes = pageVectorNodeReadBudget(int(target.dimensions), configuredVectorIndexM(graph))
		has, err := index.HasMeta()
		if err != nil {
			return err
		}
		if !has {
			if err := rebuildPageVectorIndexWithBudget(ctx, page, target.name, target.selectVector, target.dimensions, configuredVectorIndexM(graph), buildBudget, budget); err != nil {
				return err
			}
			continue
		}
		meta, err := index.Meta()
		if err != nil {
			return err
		}
		if !meta.Valid || meta.M != uint16(configuredVectorIndexM(graph)) || meta.Dimensions != target.dimensions {
			if err := rebuildPageVectorIndexWithBudget(ctx, page, target.name, target.selectVector, target.dimensions, configuredVectorIndexM(graph), buildBudget, budget); err != nil {
				return err
			}
			continue
		}
		var recordCount uint64
		if err := index.VisitIDs(ctx, func(id uint64) error {
			if err := budget.reserveBytes(index.MaxRecordBytes); err != nil {
				return err
			}
			defer budget.releaseBytes(index.MaxRecordBytes)
			if err := budget.add(1); err != nil {
				return err
			}
			node, e := index.GetBounded(id, index.MaxRecordBytes)
			if e != nil {
				return e
			}
			if node == nil {
				return fmt.Errorf("missing page HNSW node %d", id)
			}
			recordCount++
			if node.Level < 0 || len(node.Vector) != int(target.dimensions) {
				return fmt.Errorf("corrupt page HNSW node")
			}
			for level, links := range node.Neighbors {
				for _, neighborID := range links {
					if err := budget.add(1); err != nil {
						return err
					}
					if err := budget.reserveBytes(index.MaxRecordBytes); err != nil {
						return err
					}
					other, e := index.GetBounded(neighborID, index.MaxRecordBytes)
					budget.releaseBytes(index.MaxRecordBytes)
					if e != nil {
						return e
					}
					if other == nil {
						return fmt.Errorf("page HNSW neighbor %d is missing", neighborID)
					}
					if other.Level < level {
						return fmt.Errorf("corrupt page HNSW link at level %d targets lower-level node %d", level, neighborID)
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if meta.Count == 0 {
			continue
		}
		if recordCount != meta.Count {
			return fmt.Errorf("corrupt page HNSW count: metadata=%d records=%d", meta.Count, recordCount)
		}
		if meta.LastID != 0 {
			last, e := index.Get(meta.LastID)
			if e != nil {
				return e
			}
			if last == nil {
				return fmt.Errorf("corrupt page HNSW last id")
			}
		}
		entry, err := index.Get(meta.EntryID)
		if err != nil {
			return err
		}
		if entry == nil || entry.Level != meta.MaxLevel {
			return fmt.Errorf("corrupt page HNSW entry")
		}
	}
	return nil
}

func invalidatePageVectorIndexesBudget(ctx context.Context, page *store.PageGraph, namespaces []VectorNamespace, work *directSearchBudget, staged *pageVectorBuildBudget) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var active []string
	if namespaces != nil {
		active = append(active, "default")
		for i := range namespaces {
			ns := namespaces[i]
			active = append(active, pageVectorNamespaceKey(&ns))
		}
	}
	return store.InvalidatePageVectorIndexesExceptBudget(ctx, page.Tx, active, pageStoreInvalidationBudget(work, staged))
}

func invalidateInactivePageVectorIndexesBudget(ctx context.Context, page *store.PageGraph, namespaces []VectorNamespace, work *directSearchBudget, staged *pageVectorBuildBudget) error {
	active := []string{"default"}
	for i := range namespaces {
		ns := namespaces[i]
		active = append(active, pageVectorNamespaceKey(&ns))
	}
	return store.InvalidatePageVectorIndexesExceptBudget(ctx, page.Tx, active, pageStoreInvalidationBudget(work, staged))
}

func pageStoreInvalidationBudget(work *directSearchBudget, staged *pageVectorBuildBudget) *store.PageVectorInvalidationBudget {
	if work == nil && staged == nil {
		return nil
	}
	var b store.PageVectorInvalidationBudget
	if work != nil {
		b.Work = work.add
		b.ReserveBytes, b.ReleaseBytes = work.reserveBytes, work.releaseBytes
	}
	if staged != nil {
		b.StageBytes = staged.chargeStaged
	}
	return &b
}

func insertPageVector(ctx context.Context, index store.PageVectorIndex, meta *store.PageVectorMeta, id uint64, vector []float32, m int, budget *directSearchBudget) error {
	level := vectorLevel(id)
	node := &store.PageVectorNode{Level: level, Neighbors: make([][]uint64, level+1), Vector: slices.Clone(vector)}
	if meta.Count == 0 {
		if err := index.Put(id, node); err != nil {
			return err
		}
		meta.EntryID = id
		meta.MaxLevel = level
		meta.Count++
		meta.LastID = id
		return nil
	}
	entry := meta.EntryID
	for l := meta.MaxLevel; l > level; l-- {
		var err error
		entry, err = pageVectorGreedy(ctx, index, vector, entry, l, budget)
		if err != nil {
			return err
		}
	}
	for l := min(level, meta.MaxLevel); l >= 0; l-- {
		candidates, err := pageVectorLayer(ctx, index, vector, entry, l, vectorIndexConstructionEF, id, budget)
		if err != nil {
			return err
		}
		maxNeighbors := m
		if l == 0 {
			maxNeighbors = 2 * m
		}
		if len(candidates) > maxNeighbors {
			candidates, err = selectPageVectorNeighbors(ctx, index, candidates, maxNeighbors, 0, nil, budget)
			if err != nil {
				return err
			}
		}
		for _, c := range candidates {
			node.Neighbors[l] = append(node.Neighbors[l], c.id)
			if err := connectPageVectorNeighbor(ctx, index, c.id, id, vector, l, maxNeighbors, budget); err != nil {
				return err
			}
		}
		if len(candidates) > 0 {
			entry = candidates[0].id
		}
	}
	if err := index.Put(id, node); err != nil {
		return err
	}
	if level > meta.MaxLevel {
		meta.EntryID = id
		meta.MaxLevel = level
	}
	meta.Count++
	if id > meta.LastID {
		meta.LastID = id
	}
	return nil
}

func mutatePageVectorIndex(ctx context.Context, index store.PageVectorIndex, meta *store.PageVectorMeta, id uint64, vector []float32, present bool, m int, budget *directSearchBudget) error {
	old, err := index.Get(id)
	if err != nil {
		return err
	}
	if !present {
		if old != nil && !old.Deleted {
			old.Deleted = true
			if err := index.Put(id, old); err != nil {
				return err
			}
			meta.Mutations++
			meta.DeletedCount++
			return index.PutMeta(*meta)
		}
		return nil
	}
	if old != nil && !old.Deleted && slices.Equal(old.Vector, vector) {
		return nil
	}
	if old != nil {
		wasDeleted := old.Deleted
		old.Vector = slices.Clone(vector)
		old.Deleted = false
		if wasDeleted && meta.DeletedCount > 0 {
			meta.DeletedCount--
		}
		meta.Mutations++
		if err := index.Put(id, old); err != nil {
			return err
		}
		if err := reconnectPageVectorNode(ctx, index, meta, id, old, m, budget); err != nil {
			return err
		}
		return index.PutMeta(*meta)
	}
	if meta.Count != 0 && meta.DeletedCount == meta.Count {
		if err := index.DeleteAll(ctx, func() error {
			if budget != nil {
				return budget.add(1)
			}
			return ctx.Err()
		}); err != nil {
			return err
		}
		meta.EntryID, meta.LastID, meta.MaxLevel = 0, 0, 0
		meta.Count, meta.DeletedCount, meta.Mutations = 0, 0, 0
	}
	if err := insertPageVector(ctx, index, meta, id, vector, m, budget); err != nil {
		return err
	}
	meta.Mutations++
	return index.PutMeta(*meta)
}

func reconnectPageVectorNode(ctx context.Context, index store.PageVectorIndex, meta *store.PageVectorMeta, id uint64, node *store.PageVectorNode, m int, budget *directSearchBudget) error {
	entry := meta.EntryID
	for level := meta.MaxLevel; level >= 0 && level <= node.Level; level-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidates, err := pageVectorLayer(ctx, index, node.Vector, entry, level, vectorIndexConstructionEF, id, budget)
		if err != nil {
			return err
		}
		maxNeighbors := m
		if level == 0 {
			maxNeighbors = 2 * m
		}
		if len(candidates) > maxNeighbors {
			candidates, err = selectPageVectorNeighbors(ctx, index, candidates, maxNeighbors, 0, nil, budget)
			if err != nil {
				return err
			}
		}
		node.Neighbors[level] = node.Neighbors[level][:0]
		for _, candidate := range candidates {
			node.Neighbors[level] = append(node.Neighbors[level], candidate.id)
			if err := connectPageVectorNeighbor(ctx, index, candidate.id, id, node.Vector, level, maxNeighbors, budget); err != nil {
				return err
			}
		}
		if len(candidates) > 0 {
			entry = candidates[0].id
		}
	}
	return index.Put(id, node)
}

func pageVectorNamespaceKey(namespace *VectorNamespace) string {
	if namespace == nil {
		return "default"
	}
	// Versioned length-prefix encoding avoids collisions when valid scope or
	// property strings contain the old NUL delimiter.
	key := []byte("\x00page-vector-namespace\x01")
	key = binary.AppendUvarint(key, uint64(len(namespace.Scope)))
	key = append(key, namespace.Scope...)
	key = binary.AppendUvarint(key, uint64(len(namespace.Property)))
	key = append(key, namespace.Property...)
	key = binary.AppendUvarint(key, uint64(namespace.Dimensions))
	key = binary.AppendUvarint(key, uint64(namespace.Metric))
	return string(key)
}
