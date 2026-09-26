package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// PageCommit applies one logical transaction inside the caller's single page
// write transaction. The caller must roll back after any error.
func (page *PageGraph) PageCommit(ctx context.Context, graph *GraphState, nextNodeID, nextEdgeID, commitID uint64, delta GraphDelta, archive bool) (PageCatalog, []byte, error) {
	return page.pageCommit(ctx, graph, nextNodeID, nextEdgeID, commitID, delta, archive, nil)
}

// PageCommitVectorConfiguration records a vector-dimension transition as a
// logical commit. The transition is bound into source history but is not
// represented as a replayable graph delta; ArchiveBasePending requires a full
// archive base before the archive can advance past this commit.
func (page *PageGraph) PageCommitVectorConfiguration(ctx context.Context, graph *GraphState, nextNodeID, nextEdgeID, commitID uint64, dimensions uint16) (PageCatalog, error) {
	catalog, _, err := page.pageCommit(ctx, graph, nextNodeID, nextEdgeID, commitID, GraphDelta{}, false, &dimensions)
	return catalog, err
}

func (page *PageGraph) pageCommit(ctx context.Context, graph *GraphState, nextNodeID, nextEdgeID, commitID uint64, delta GraphDelta, archive bool, vectorDimensions *uint16) (PageCatalog, []byte, error) {
	catalog, err := page.Catalog()
	if err != nil {
		return catalog, nil, err
	}
	if catalog.DatabaseID != graph.DatabaseID || commitID != catalog.CommitID+1 {
		return catalog, nil, fmt.Errorf("page commit identity mismatch")
	}
	persisted, err := buildPersistedDelta(graph, nextNodeID, nextEdgeID, commitID, delta)
	if err != nil {
		return catalog, nil, err
	}
	kind := "delta"
	if hasPropertyDelta(persisted) {
		kind = "property_delta"
	}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: kind, Delta: &persisted})
	if err != nil {
		return catalog, nil, err
	}
	header, err := encodeWALHeader(graph.DatabaseID, commitID, payload)
	if err != nil {
		return catalog, nil, err
	}
	frame := append(header[:], payload...)
	for _, id := range delta.DeleteEdges {
		if err := ctx.Err(); err != nil {
			return catalog, nil, err
		}
		if err := page.DeleteEdge(id); err != nil {
			return catalog, nil, err
		}
	}
	for _, id := range delta.DeleteNodes {
		if err := page.DeleteNode(ctx, id); err != nil {
			return catalog, nil, err
		}
	}
	for _, id := range delta.UpsertNodes {
		if err := ctx.Err(); err != nil {
			return catalog, nil, err
		}
		node, err := graph.ReadNode(id)
		if err != nil {
			return catalog, nil, err
		}
		if err := page.PutNode(node); err != nil {
			return catalog, nil, err
		}
	}
	for _, id := range delta.UpsertEdges {
		if err := ctx.Err(); err != nil {
			return catalog, nil, err
		}
		edge, err := graph.ReadEdge(id)
		if err != nil {
			return catalog, nil, err
		}
		if err := page.PutEdge(edge); err != nil {
			return catalog, nil, err
		}
	}
	for _, id := range delta.DeleteFTS {
		if err := page.PutFTSContext(ctx, id, nil); err != nil {
			return catalog, nil, err
		}
	}
	for _, id := range delta.UpsertFTS {
		if err := page.PutFTSContext(ctx, id, graph.FTS.Get(id)); err != nil {
			return catalog, nil, err
		}
	}
	for _, change := range delta.AppMetadata {
		if err := page.PutMetadata(change.Key, change.Value, change.Delete); err != nil {
			return catalog, nil, err
		}
	}

	for _, definition := range delta.DropNodeIndexes {
		if err := page.DropPropertyIndex(ctx, true, definition); err != nil {
			return catalog, nil, err
		}
	}
	for _, definition := range delta.DropEdgeIndexes {
		if err := page.DropPropertyIndex(ctx, false, definition); err != nil {
			return catalog, nil, err
		}
	}
	for _, definition := range delta.CreateNodeIndexes {
		if err := page.CreatePropertyIndex(ctx, true, definition); err != nil {
			return catalog, nil, err
		}
	}
	for _, definition := range delta.CreateEdgeIndexes {
		if err := page.CreatePropertyIndex(ctx, false, definition); err != nil {
			return catalog, nil, err
		}
	}
	if err := page.ApplyStreamOperations(ctx, delta.StreamOperations); err != nil {
		return catalog, nil, err
	}
	if vectorDimensions != nil {
		if *vectorDimensions == catalog.VectorDimensions {
			return catalog, nil, fmt.Errorf("vector configuration transition has no change")
		}
		catalog.History = pageVectorConfigurationHistory(catalog.History, commitID, catalog.VectorDimensions, *vectorDimensions, frame)
		catalog.VectorDimensions = *vectorDimensions
		catalog.ArchiveBasePending = true
	} else {
		hash := sha256.New()
		hash.Write(catalog.History[:])
		hash.Write(frame)
		copy(catalog.History[:], hash.Sum(nil))
	}
	catalog.SnapshotBytes = graph.SnapshotBytes
	catalog.CommitID = commitID
	catalog.NextNodeID = nextNodeID
	catalog.NextEdgeID = nextEdgeID
	if err := page.PutCatalog(catalog); err != nil {
		return catalog, nil, err
	}
	// Keep the latest durable frame until its archive publication has been acknowledged.
	if err := page.Tx.Delete("archive-outbox", pageID(commitID-1)); err != nil {
		return catalog, nil, err
	}
	if err := page.Tx.Put("commit-history", pageID(commitID), catalog.History[:]); err != nil {
		return catalog, nil, err
	}
	if archive {
		if err := page.Tx.Put("archive-outbox", pageID(commitID), frame); err != nil {
			return catalog, nil, err
		}
	}
	return catalog, frame, ctx.Err()
}

func pageVectorConfigurationHistory(previous [sha256.Size]byte, commitID uint64, oldDimensions, newDimensions uint16, frame []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("latticedb/page/vector-dimensions/v1\x00"))
	_, _ = hash.Write(previous[:])
	var encoded [12]byte
	binary.BigEndian.PutUint64(encoded[:8], commitID)
	binary.BigEndian.PutUint16(encoded[8:10], oldDimensions)
	binary.BigEndian.PutUint16(encoded[10:12], newDimensions)
	_, _ = hash.Write(encoded[:])
	// The frame also commits the allocator high-water marks persisted alongside
	// this configuration transition.
	_, _ = hash.Write(frame)
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
