package store

const AdjacencyCompactionChunkBudget = 64

const adjacencyBackgroundMinTombstones = adjacencyChunkSize

// AdjacencyCompactor incrementally builds dense replacements for adjacency
// lists. The source graph remains immutable until Result is complete.
type AdjacencyCompactor struct {
	source        *GraphState
	result        *GraphState
	direction     uint8
	nodeID        uint64
	chunk         int
	list          *EdgeList
	builderChunks PagedMap[[]uint64]
	builderTail   []uint64
	builderChunk  uint64
	builderCount  int
	listID        uint64
}

// NewAdjacencyCompactor starts a deterministic, bounded adjacency rebuild for
// one deletion-affected node and direction.
func NewAdjacencyCompactor(graph *GraphState, direction uint8, nodeID uint64) *AdjacencyCompactor {
	if graph == nil {
		return nil
	}
	c := &AdjacencyCompactor{source: graph, direction: direction, nodeID: nodeID}
	if !c.selectList() {
		return nil
	}
	return c
}

// AdjacencyNeedsCompaction reports whether a deletion-affected list has
// accumulated enough tombstones to amortize a background rebuild.
func AdjacencyNeedsCompaction(graph *GraphState, direction uint8, nodeID uint64) bool {
	if graph == nil {
		return false
	}
	var lists PagedMap[*EdgeList]
	if direction == 0 {
		lists = graph.Outgoing
	} else {
		lists = graph.Incoming
	}
	list := lists.Get(nodeID)
	return list != nil && !list.IsInline() && list.total > adjacencySyncCompactLimit && list.removed.Len() > adjacencyBackgroundMinTombstones
}

// Step copies at most maxChunks source chunks into one dense replacement. It
// returns done after that list has been rebuilt.
func (c *AdjacencyCompactor) Step(maxChunks int) (done, changed bool) {
	if c == nil || c.source == nil || maxChunks <= 0 {
		return true, false
	}
	for maxChunks > 0 {
		count := (c.list.total + adjacencyChunkSize - 1) / adjacencyChunkSize
		for c.chunk < count && maxChunks > 0 {
			for _, id := range c.list.chunks.Get(uint64(c.chunk)) {
				if !c.list.removed.Has(id) {
					c.appendLive(id)
				}
			}
			c.chunk++
			maxChunks--
		}
		if c.chunk != count {
			return false, changed
		}
		if c.result == nil {
			c.result = CloneGraphStateShallow(c.source)
		}
		compacted := c.finishBuilder()
		if c.direction == 0 {
			c.result.Outgoing = replaceAdjacency(c.result.Outgoing, c.listID, compacted)
		} else {
			c.result.Incoming = replaceAdjacency(c.result.Incoming, c.listID, compacted)
		}
		changed = true
		return true, true
	}
	return false, changed
}

func (c *AdjacencyCompactor) appendLive(id uint64) {
	if len(c.builderTail) == adjacencyChunkSize {
		c.builderChunks.Set(c.builderChunk, c.builderTail)
		c.builderChunk++
		c.builderTail = nil
	}
	if c.builderTail == nil {
		c.builderTail = make([]uint64, 0, adjacencyChunkSize)
	}
	c.builderTail = append(c.builderTail, id)
	c.builderCount++
}

func (c *AdjacencyCompactor) finishBuilder() *EdgeList {
	if len(c.builderTail) != 0 {
		c.builderChunks.Set(c.builderChunk, c.builderTail)
	}
	if c.builderCount == 0 {
		return nil
	}
	return &EdgeList{chunks: c.builderChunks, count: c.builderCount, total: c.builderCount}
}

// Result returns the dense graph after Step reports done.
func (c *AdjacencyCompactor) Result() *GraphState {
	if c == nil {
		return nil
	}
	return c.result
}

func (c *AdjacencyCompactor) selectList() bool {
	var lists PagedMap[*EdgeList]
	if c.direction == 0 {
		lists = c.source.Outgoing
	} else {
		lists = c.source.Incoming
	}
	list := lists.Get(c.nodeID)
	if list == nil || list.IsInline() || list.removed.Len() <= adjacencyBackgroundMinTombstones || list.total <= adjacencySyncCompactLimit {
		return false
	}
	c.listID, c.list = c.nodeID, list
	return true
}

func replaceAdjacency(adjacency PagedMap[*EdgeList], nodeID uint64, list *EdgeList) PagedMap[*EdgeList] {
	if list == nil {
		adjacency.CloneShardOnce(nodeID)
		adjacency.Delete(nodeID)
		return adjacency
	}
	return adjacency.ForkSet(nodeID, list)
}
