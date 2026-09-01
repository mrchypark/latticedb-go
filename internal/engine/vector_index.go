package engine

import (
	"context"
	"fmt"
	"math/bits"
	"slices"
	"sync"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

const (
	vectorIndexM              = 16
	vectorIndexM0             = 32
	vectorIndexConstructionEF = 200
	vectorIndexSearchEF       = 64
	vectorIndexMaxLevel       = 16
)

type vectorCandidate struct {
	id       uint64
	distance float64
}

type vectorCandidateHeap struct {
	items []vectorCandidate
	max   bool
}

func (h vectorCandidateHeap) Len() int { return len(h.items) }

func (h vectorCandidateHeap) before(left, right vectorCandidate) bool {
	order := compareVectorCandidate(left, right)
	if h.max {
		return order > 0
	}
	return order < 0
}

func (h *vectorCandidateHeap) push(item vectorCandidate) {
	h.items = append(h.items, item)
	for index := len(h.items) - 1; index > 0; {
		parent := (index - 1) / 2
		if !h.before(h.items[index], h.items[parent]) {
			break
		}
		h.items[index], h.items[parent] = h.items[parent], h.items[index]
		index = parent
	}
}

func (h *vectorCandidateHeap) pop() vectorCandidate {
	item := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items[last] = vectorCandidate{}
	h.items = h.items[:last]
	for index := 0; ; {
		left := 2*index + 1
		if left >= len(h.items) {
			break
		}
		child := left
		if right := left + 1; right < len(h.items) && h.before(h.items[right], h.items[left]) {
			child = right
		}
		if !h.before(h.items[child], h.items[index]) {
			break
		}
		h.items[index], h.items[child] = h.items[child], h.items[index]
		index = child
	}
	return item
}

type vectorSearchScratch struct {
	frontier        []vectorCandidate
	best            []vectorCandidate
	visited         map[uint64]struct{}
	visitedCapacity int
}

var vectorSearchScratchPool = sync.Pool{New: func() any {
	return &vectorSearchScratch{visited: make(map[uint64]struct{})}
}}

func acquireVectorSearchScratch(maxVisited, maxFrontier, maxBest int) *vectorSearchScratch {
	scratch := vectorSearchScratchPool.Get().(*vectorSearchScratch)
	if !vectorSearchScratchFits(scratch, maxVisited, maxFrontier, maxBest) {
		return &vectorSearchScratch{visited: make(map[uint64]struct{})}
	}
	return scratch
}

func vectorSearchScratchFits(scratch *vectorSearchScratch, maxVisited, maxFrontier, maxBest int) bool {
	return scratch.visitedCapacity <= maxVisited && cap(scratch.frontier) <= maxFrontier && cap(scratch.best) <= maxBest
}

func releaseVectorSearchScratch(scratch *vectorSearchScratch) {
	visited := scratch.visitedCapacity
	clear(scratch.visited)
	scratch.frontier = scratch.frontier[:0]
	scratch.best = scratch.best[:0]
	if visited > 4096 || cap(scratch.frontier) > 4096 || cap(scratch.best) > 4096 {
		return
	}
	vectorSearchScratchPool.Put(scratch)
}

func rebuildVectorIndex(graph *store.GraphState) {
	_ = rebuildVectorIndexContext(context.Background(), graph)
}

func rebuildVectorIndexContext(ctx context.Context, graph *store.GraphState) error {
	return rebuildVectorIndexBudget(ctx, graph, ^uint64(0), ^uint64(0))
}

func rebuildVectorIndexBudget(ctx context.Context, graph *store.GraphState, maxWork, maxBytes uint64) error {
	var live uint64
	for _, node := range graph.Nodes.All() {
		if _, ok := selectedVector(graph, node); ok {
			live++
		}
	}
	estimatedBytes := estimateVectorBuildLogicalBytes(graph, live)
	if estimatedBytes > maxBytes {
		return fmt.Errorf("%w: vector index build requires approximately %d bytes, limit is %d", ErrResourceLimit, estimatedBytes, maxBytes)
	}
	target := store.CloneGraphStateShallow(graph)
	target.VectorIndex = store.NewVectorIndex()
	target.VectorTombstones = store.NewPagedMap[[]float32]()
	target.VectorMutations = 0
	scratch := &vectorSearchScratch{visited: make(map[uint64]struct{}, vectorIndexConstructionEF*vectorIndexM)}
	ids := make([]uint64, 0, target.Nodes.Len())
	for id := range target.Nodes.All() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	budget := &directSearchBudget{ctx: ctx, maxWork: maxWork, maxBytes: maxBytes, bytes: estimatedBytes, annVisitedLimit: ^uint64(0)}
	for index, id := range ids {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := insertVectorIndexModeBudget(target, id, true, scratch, budget); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	graph.VectorIndex = target.VectorIndex
	graph.VectorTombstones = target.VectorTombstones
	graph.VectorMutations = 0
	return nil
}

func insertVectorIndex(graph *store.GraphState, id uint64) error {
	return insertVectorIndexMode(graph, id, false, nil)
}

func insertVectorIndexMode(graph *store.GraphState, id uint64, mutable bool, scratch *vectorSearchScratch) error {
	return insertVectorIndexModeBudget(graph, id, mutable, scratch, nil)
}

func insertVectorIndexModeBudget(graph *store.GraphState, id uint64, mutable bool, scratch *vectorSearchScratch, budget *directSearchBudget) error {
	node := graph.Nodes.Get(id)
	if node == nil {
		return nil
	}
	vector, ok := selectedVector(graph, node)
	if !ok {
		tombstoneVectorIndex(graph, id, nil)
		return nil
	}
	graph.VectorTombstones.CloneShardOnce(id)
	graph.VectorTombstones.Delete(id)
	level := vectorLevel(id)
	newNode := &store.VectorIndexNode{Level: level, Neighbors: make([][]uint64, level+1)}
	if graph.VectorIndex.Nodes.Len() == 0 {
		if !mutable {
			graph.VectorIndex.Nodes.CloneShardOnce(id)
		}
		graph.VectorIndex.Nodes.Set(id, newNode)
		graph.VectorIndex.EntryID = id
		graph.VectorIndex.MaxLevel = level
		return nil
	}

	entry := graph.VectorIndex.EntryID
	for l := graph.VectorIndex.MaxLevel; l > level; l-- {
		if budget == nil {
			entry = vectorGreedy(graph, vector, entry, l)
		} else {
			var err error
			entry, err = vectorGreedyBudget(graph, vector, entry, l, budget)
			if err != nil {
				return err
			}
		}
	}
	for l := min(level, graph.VectorIndex.MaxLevel); l >= 0; l-- {
		maxNeighbors := vectorIndexM
		if l == 0 {
			maxNeighbors = vectorIndexM0
		}
		var candidates []vectorCandidate
		if budget == nil {
			candidates = vectorSearchLayerScratch(graph, vector, entry, l, vectorIndexConstructionEF, id, scratch)
		} else {
			var err error
			candidates, err = vectorSearchLayerBudget(graph, vector, entry, l, vectorIndexConstructionEF, id, scratch, budget)
			if err != nil {
				return err
			}
		}
		if len(candidates) > maxNeighbors {
			var err error
			candidates, err = selectVectorNeighborsHeuristic(graph, candidates, maxNeighbors, budget)
			if err != nil {
				return err
			}
		}
		newNode.Neighbors[l] = make([]uint64, len(candidates))
		for i, candidate := range candidates {
			newNode.Neighbors[l][i] = candidate.id
			if err := connectVectorNeighbor(graph, candidate.id, id, l, maxNeighbors, mutable, budget); err != nil {
				return err
			}
		}
		if len(candidates) > 0 {
			entry = candidates[0].id
		}
	}
	if !mutable {
		graph.VectorIndex.Nodes.CloneShardOnce(id)
	}
	graph.VectorIndex.Nodes.Set(id, newNode)
	if level > graph.VectorIndex.MaxLevel {
		graph.VectorIndex.EntryID = id
		graph.VectorIndex.MaxLevel = level
	}
	return nil
}

func tombstoneVectorIndex(graph *store.GraphState, id uint64, vector []float32) {
	if graph.VectorIndex.Nodes.Get(id) == nil {
		return
	}
	if vector == nil {
		vector, _ = vectorForNode(graph, id)
	}
	if vector != nil {
		graph.VectorTombstones.CloneShardOnce(id)
		graph.VectorTombstones.Set(id, slices.Clone(vector))
	}
}

func connectVectorNeighbor(graph *store.GraphState, id, neighbor uint64, level, maxNeighbors int, mutable bool, budget *directSearchBudget) error {
	node := graph.VectorIndex.Nodes.Get(id)
	if node == nil || level >= len(node.Neighbors) || slices.Contains(node.Neighbors[level], neighbor) {
		return nil
	}
	copyNode := node
	if !mutable {
		copyNode = &store.VectorIndexNode{Level: node.Level, Neighbors: slices.Clone(node.Neighbors)}
		copyNode.Neighbors[level] = slices.Clone(node.Neighbors[level])
	}
	if len(copyNode.Neighbors[level]) < maxNeighbors {
		copyNode.Neighbors[level] = append(copyNode.Neighbors[level], neighbor)
		if !mutable {
			graph.VectorIndex.Nodes.CloneShardOnce(id)
		}
		graph.VectorIndex.Nodes.Set(id, copyNode)
		return nil
	}
	vector, ok := vectorForNode(graph, id)
	if !ok {
		return nil
	}
	var storage [vectorIndexM0 + 1]vectorCandidate
	candidates := storage[:0]
	for _, candidateID := range copyNode.Neighbors[level] {
		candidateVector, exists := vectorForNode(graph, candidateID)
		if !exists {
			continue
		}
		distance, err := vectorDistanceWithBudget(vector, candidateVector, budget)
		if err != nil {
			return err
		}
		candidates = append(candidates, vectorCandidate{id: candidateID, distance: distance})
	}
	neighborVector, exists := vectorForNode(graph, neighbor)
	if !exists {
		return nil
	}
	distance, err := vectorDistanceWithBudget(vector, neighborVector, budget)
	if err != nil {
		return err
	}
	candidates = append(candidates, vectorCandidate{id: neighbor, distance: distance})
	slices.SortFunc(candidates, compareVectorCandidate)
	selected, err := selectVectorNeighborsHeuristic(graph, candidates, maxNeighbors, budget)
	if err != nil {
		return err
	}
	if len(selected) == 0 {
		return nil
	}
	copyNode.Neighbors[level] = copyNode.Neighbors[level][:len(selected)]
	for index := range selected {
		copyNode.Neighbors[level][index] = selected[index].id
	}
	if !mutable {
		graph.VectorIndex.Nodes.CloneShardOnce(id)
	}
	graph.VectorIndex.Nodes.Set(id, copyNode)
	return nil
}

func vectorDistanceWithBudget(left, right []float32, budget *directSearchBudget) (float64, error) {
	if budget == nil {
		return search.SquaredVectorDistance(left, right)
	}
	if err := budget.add(uint64(len(left))); err != nil {
		return 0, err
	}
	if len(left) < 256 {
		return search.SquaredVectorDistance(left, right)
	}
	return search.SquaredVectorDistanceContext(budget.ctx, left, right)
}

func vectorGreedy(graph *store.GraphState, query []float32, entry uint64, level int) uint64 {
	entry, _ = vectorGreedyBudget(graph, query, entry, level, nil)
	return entry
}

func vectorGreedyBudget(graph *store.GraphState, query []float32, entry uint64, level int, budget *directSearchBudget) (uint64, error) {
	bestVector, ok := vectorForNode(graph, entry)
	if !ok {
		return entry, nil
	}
	if budget != nil {
		if err := budget.add(uint64(len(query))); err != nil {
			return entry, err
		}
	}
	var bestDistance float64
	if budget == nil || len(query) < 256 {
		bestDistance, _ = search.SquaredVectorDistance(query, bestVector)
	} else {
		bestDistance, _ = search.SquaredVectorDistanceContext(budget.ctx, query, bestVector)
	}
	for {
		changed := false
		node := graph.VectorIndex.Nodes.Get(entry)
		if node == nil || level >= len(node.Neighbors) {
			return entry, nil
		}
		for _, id := range node.Neighbors[level] {
			if budget != nil {
				if err := budget.add(uint64(len(query))); err != nil {
					return entry, err
				}
			}
			vector, exists := vectorForNode(graph, id)
			if !exists {
				continue
			}
			var distance float64
			var err error
			if budget == nil || len(query) < 256 {
				distance, err = search.SquaredVectorDistance(query, vector)
			} else {
				distance, err = search.SquaredVectorDistanceContext(budget.ctx, query, vector)
			}
			if err != nil {
				return entry, err
			}
			if distance < bestDistance {
				entry, bestDistance, changed = id, distance, true
			}
		}
		if !changed {
			return entry, nil
		}
	}
}

func vectorSearchLayer(graph *store.GraphState, query []float32, entry uint64, level, ef int, exclude uint64) []vectorCandidate {
	result, _ := vectorSearchLayerBudget(graph, query, entry, level, ef, exclude, nil, nil)
	return result
}

func vectorSearchLayerScratch(graph *store.GraphState, query []float32, entry uint64, level, ef int, exclude uint64, scratch *vectorSearchScratch) []vectorCandidate {
	result, _ := vectorSearchLayerBudget(graph, query, entry, level, ef, exclude, scratch, nil)
	return result
}

func vectorSearchLayerBudget(graph *store.GraphState, query []float32, entry uint64, level, ef int, exclude uint64, scratch *vectorSearchScratch, budget *directSearchBudget) ([]vectorCandidate, error) {
	entryVector, ok, entryEligible := vectorForSearchCandidate(graph, entry)
	if !ok {
		return nil, nil
	}
	if scratch == nil {
		scratch = &vectorSearchScratch{visited: make(map[uint64]struct{})}
	}
	for id := range scratch.visited {
		delete(scratch.visited, id)
	}
	if budget != nil {
		if err := budget.add(uint64(len(query))); err != nil {
			return nil, err
		}
	}
	var distance float64
	var err error
	if budget == nil || len(query) < 256 {
		distance, err = search.SquaredVectorDistance(query, entryVector)
	} else {
		distance, err = search.SquaredVectorDistanceContext(budget.ctx, query, entryVector)
	}
	if err != nil {
		return nil, err
	}
	frontier := vectorCandidateHeap{items: append(scratch.frontier[:0], vectorCandidate{id: entry, distance: distance})}
	best := vectorCandidateHeap{items: scratch.best[:0], max: true}
	if entry != exclude && entryEligible {
		best.push(frontier.items[0])
	}
	if budget != nil {
		if uint64(len(scratch.visited)) >= budget.annVisitedLimit {
			return nil, fmt.Errorf("%w: search memory exceeds budget", ErrResourceLimit)
		}
	}
	scratch.visited[entry] = struct{}{}
	scratch.visitedCapacity = max(scratch.visitedCapacity, len(scratch.visited))
	for frontier.Len() > 0 {
		current := frontier.pop()
		if best.Len() >= ef && current.distance > best.items[0].distance {
			break
		}
		node := graph.VectorIndex.Nodes.Get(current.id)
		if node == nil || level >= len(node.Neighbors) {
			continue
		}
		for _, id := range node.Neighbors[level] {
			if budget != nil {
				if err := budget.add(uint64(len(query))); err != nil {
					scratch.frontier, scratch.best = frontier.items, best.items
					return nil, err
				}
			}
			if _, seen := scratch.visited[id]; seen {
				continue
			}
			if budget != nil {
				if uint64(len(scratch.visited)) >= budget.annVisitedLimit {
					scratch.frontier, scratch.best = frontier.items, best.items
					return nil, fmt.Errorf("%w: search memory exceeds budget", ErrResourceLimit)
				}
			}
			scratch.visited[id] = struct{}{}
			scratch.visitedCapacity = max(scratch.visitedCapacity, len(scratch.visited))
			vector, exists, eligible := vectorForSearchCandidate(graph, id)
			if !exists {
				continue
			}
			var distance float64
			var err error
			if budget == nil || len(query) < 256 {
				distance, err = search.SquaredVectorDistance(query, vector)
			} else {
				distance, err = search.SquaredVectorDistanceContext(budget.ctx, query, vector)
			}
			if err != nil {
				scratch.frontier, scratch.best = frontier.items, best.items
				return nil, err
			}
			candidate := vectorCandidate{id: id, distance: distance}
			if best.Len() < ef || distance < best.items[0].distance {
				frontier.push(candidate)
				if id != exclude && eligible {
					best.push(candidate)
					if best.Len() > ef {
						best.pop()
					}
				}
			}
		}
	}
	slices.SortFunc(best.items, compareVectorCandidate)
	scratch.frontier = frontier.items
	scratch.best = best.items
	return best.items, nil
}

func vectorForNode(graph *store.GraphState, id uint64) ([]float32, bool) {
	vector, ok, _ := vectorForSearchCandidate(graph, id)
	return vector, ok
}

func vectorForSearchCandidate(graph *store.GraphState, id uint64) ([]float32, bool, bool) {
	node := graph.Nodes.Get(id)
	if node != nil {
		vector, ok := search.FirstVectorProperty(node.Properties)
		if ok {
			valid := graph.VectorDimensions == 0 || len(vector) == int(graph.VectorDimensions)
			return vector, valid, valid
		}
	}
	vector := graph.VectorTombstones.Get(id)
	return vector, vector != nil, false
}

func selectedVector(graph *store.GraphState, node *store.NodeRecord) ([]float32, bool) {
	if graph == nil || node == nil {
		return nil, false
	}
	vector, ok := search.FirstVectorProperty(node.Properties)
	return vector, ok && (graph.VectorDimensions == 0 || len(vector) == int(graph.VectorDimensions))
}

func validateGraphVectors(graph *store.GraphState) error {
	return validateGraphVectorsContext(context.Background(), graph)
}

func validateGraphVectorsContext(ctx context.Context, graph *store.GraphState) error {
	index := 0
	for _, node := range graph.Nodes.All() {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		index++
		if err := validateNodeVectors(graph.VectorDimensions, node); err != nil {
			return err
		}
	}
	return nil
}

func validateNodeVectors(dimensions uint16, node *store.NodeRecord) error {
	if dimensions == 0 || node == nil {
		return nil
	}
	for _, value := range node.Properties {
		if vector, ok := value.([]float32); ok && len(vector) != int(dimensions) {
			return fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), dimensions)
		}
	}
	return nil
}

func (tx *Tx) applyVectorIndexChanges() error {
	changed := false
	for _, id := range mapKeys(tx.changes.upsertNodes) {
		after := tx.graph.Nodes.Get(id)
		if err := validateNodeVectors(tx.graph.VectorDimensions, after); err != nil {
			return err
		}
		beforeVector, beforeOK := selectedVector(tx.base, tx.base.Nodes.Get(id))
		afterVector, afterOK := selectedVector(tx.graph, after)
		if beforeOK == afterOK && slices.Equal(beforeVector, afterVector) {
			continue
		}
		changed = true
		if afterOK {
			if err := insertVectorIndex(tx.graph, id); err != nil {
				return err
			}
			if beforeOK {
				tx.graph.VectorMutations++
			}
		} else if beforeOK {
			tombstoneVectorIndex(tx.graph, id, beforeVector)
		}
	}
	for _, id := range mapKeys(tx.changes.deleteNodes) {
		if vector, ok := selectedVector(tx.base, tx.base.Nodes.Get(id)); ok {
			changed = true
			tombstoneVectorIndex(tx.graph, id, vector)
		}
	}
	threshold := vectorRebuildThreshold(tx.graph)
	tombstoneBytes := saturatingMul(uint64(tx.graph.VectorTombstones.Len()), uint64(tx.graph.VectorDimensions)*4)
	debt := uint64(tx.graph.VectorTombstones.Len()) + tx.graph.VectorMutations
	if changed && (debt > uint64(threshold) || tombstoneBytes > 64<<20) {
		return ErrVectorIndexMaintenanceRequired
	}
	return nil
}

func estimateVectorIndexBytes(live uint64, dimensions uint16) uint64 {
	// 4 KiB covers the hard max-level node, all capped neighbor arrays, and map metadata.
	return saturatingMul(live, saturatingAdd(4096, uint64(dimensions)*4))
}

func estimateVectorBuildLogicalBytes(graph *store.GraphState, live uint64) uint64 {
	newBytes := estimateVectorIndexBytes(live, graph.VectorDimensions)
	oldBytes := estimateVectorIndexBytes(uint64(graph.VectorIndex.Nodes.Len()), graph.VectorDimensions)
	tombstoneBytes := saturatingMul(uint64(graph.VectorTombstones.Len()), uint64(graph.VectorDimensions)*4)
	idsBytes := saturatingMul(uint64(graph.Nodes.Len()), 8)
	return saturatingAdd(saturatingAdd(newBytes, oldBytes), saturatingAdd(tombstoneBytes, saturatingAdd(idsBytes, 128<<10)))
}

func vectorRebuildThreshold(graph *store.GraphState) int {
	liveVectors := max(0, graph.VectorIndex.Nodes.Len()-graph.VectorTombstones.Len())
	return min(65_536, max(4096, liveVectors/10))
}

func validateVectorIndex(graph *store.GraphState) error {
	if graph.VectorIndex.Nodes.Len() == 0 {
		if graph.VectorIndex.EntryID != 0 || graph.VectorIndex.MaxLevel != 0 {
			return fmt.Errorf("empty vector index has entry %d at level %d", graph.VectorIndex.EntryID, graph.VectorIndex.MaxLevel)
		}
		return nil
	}
	if graph.VectorIndex.Nodes.Get(graph.VectorIndex.EntryID) == nil {
		return fmt.Errorf("vector entry %d does not exist", graph.VectorIndex.EntryID)
	}
	maxLevel := 0
	for id, node := range graph.VectorIndex.Nodes.All() {
		if node == nil || node.Level < 0 || node.Level > vectorIndexMaxLevel || len(node.Neighbors) != node.Level+1 {
			return fmt.Errorf("vector node %d has invalid level metadata", id)
		}
		maxLevel = max(maxLevel, node.Level)
		if _, ok := vectorForNode(graph, id); !ok {
			return fmt.Errorf("vector node %d has no live or tombstone vector", id)
		}
		for level, neighbors := range node.Neighbors {
			maxNeighbors := vectorIndexM
			if level == 0 {
				maxNeighbors = vectorIndexM0
			}
			if len(neighbors) > maxNeighbors {
				return fmt.Errorf("vector node %d level %d exceeds degree cap", id, level)
			}
			seen := make(map[uint64]struct{}, len(neighbors))
			for _, neighborID := range neighbors {
				if neighborID == id {
					return fmt.Errorf("vector node %d has a self edge", id)
				}
				if _, duplicate := seen[neighborID]; duplicate {
					return fmt.Errorf("vector node %d has duplicate neighbor %d", id, neighborID)
				}
				seen[neighborID] = struct{}{}
				neighbor := graph.VectorIndex.Nodes.Get(neighborID)
				if neighbor == nil || neighbor.Level < level {
					return fmt.Errorf("vector node %d has invalid level-%d neighbor %d", id, level, neighborID)
				}
			}
		}
	}
	if graph.VectorIndex.MaxLevel != maxLevel {
		return fmt.Errorf("vector max level = %d, want %d", graph.VectorIndex.MaxLevel, maxLevel)
	}
	return nil
}

func vectorLevel(id uint64) int {
	hash := id + 0x9e3779b97f4a7c15
	hash = (hash ^ hash>>30) * 0xbf58476d1ce4e5b9
	hash = (hash ^ hash>>27) * 0x94d049bb133111eb
	hash ^= hash >> 31
	return min(bits.TrailingZeros64(hash|1<<63), vectorIndexMaxLevel)
}

func selectVectorNeighborsHeuristic(graph *store.GraphState, candidates []vectorCandidate, limit int, budget *directSearchBudget) ([]vectorCandidate, error) {
	var selected [vectorIndexM0]vectorCandidate
	var selectedVectors [vectorIndexM0][]float32
	var rejected [vectorIndexConstructionEF]vectorCandidate
	selectedCount, rejectedCount := 0, 0
	for _, candidate := range candidates {
		candidateVector, ok := vectorForNode(graph, candidate.id)
		if !ok {
			continue
		}
		keep := true
		for _, neighborVector := range selectedVectors[:selectedCount] {
			distance, err := vectorDistanceWithBudget(candidateVector, neighborVector, budget)
			if err != nil {
				return nil, err
			}
			if distance <= candidate.distance {
				keep = false
				break
			}
		}
		if keep {
			selected[selectedCount] = candidate
			selectedVectors[selectedCount] = candidateVector
			selectedCount++
			if selectedCount == limit {
				break
			}
		} else {
			rejected[rejectedCount] = candidate
			rejectedCount++
		}
	}
	for index := 0; selectedCount < limit && index < rejectedCount; index++ {
		selected[selectedCount] = rejected[index]
		selectedCount++
	}
	return append(candidates[:0], selected[:selectedCount]...), nil
}

func compareVectorCandidate(left, right vectorCandidate) int {
	if left.distance < right.distance {
		return -1
	}
	if left.distance > right.distance {
		return 1
	}
	if left.id < right.id {
		return -1
	}
	if left.id > right.id {
		return 1
	}
	return 0
}
