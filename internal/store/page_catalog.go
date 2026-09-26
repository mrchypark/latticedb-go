package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

const pageCatalogBucket = "catalog"
const pageMetadataBucket = "metadata"

type PageCatalog struct {
	SnapshotBytes                    uint64
	DatabaseID                       string
	VectorDimensions                 uint16
	CommitID, NextNodeID, NextEdgeID uint64
	Nodes, Edges                     uint64
	History                          [32]byte
	ArchiveBasePending               bool
}

func (graph *PageGraph) Catalog() (PageCatalog, error) {
	var result PageCatalog
	data, err := graph.Tx.Get(pageCatalogBucket, []byte("state"))
	if err != nil {
		return result, err
	}
	if data == nil {
		return result, errors.New("page catalog is missing")
	}
	d, err := decodePageRecord(data, 3, 4096)
	if err != nil {
		return result, err
	}
	result.DatabaseID = d.str()
	if err := validateDatabaseID(result.DatabaseID); err != nil {
		return result, err
	}
	dimensions := d.u()
	if dimensions > 65535 {
		return result, errors.New("invalid page vector dimensions")
	}
	result.VectorDimensions = uint16(dimensions)
	result.CommitID = d.u()
	result.NextNodeID = d.u()
	result.NextEdgeID = d.u()
	result.Nodes = d.u()
	result.Edges = d.u()
	result.SnapshotBytes = d.u()
	history := d.bytes()
	if len(history) != 32 {
		return result, errors.New("invalid page history identity")
	}
	copy(result.History[:], history)
	if d.remaining != 0 {
		pending, err := d.ReadByte()
		if err != nil || pending > 1 {
			return result, errors.New("invalid page archive-base marker")
		}
		result.ArchiveBasePending = pending == 1
	}
	if err := d.finish(); err != nil {
		return result, fmt.Errorf("decode page catalog: %w", err)
	}
	if err := ValidateIDHighWater(result.NextNodeID); err != nil {
		return result, err
	}
	if err := ValidateIDHighWater(result.NextEdgeID); err != nil {
		return result, err
	}
	return result, nil
}
func (graph *PageGraph) PutCatalog(c PageCatalog) error {
	if c.DatabaseID == "" {
		return errors.New("empty page database identity")
	}
	if err := ValidateIDHighWater(c.NextNodeID); err != nil {
		return err
	}
	if err := ValidateIDHighWater(c.NextEdgeID); err != nil {
		return err
	}
	data, err := encodePageRecord(3, func(e *binaryEncoder) {
		e.str(c.DatabaseID)
		e.u(uint64(c.VectorDimensions))
		e.u(c.CommitID)
		e.u(c.NextNodeID)
		e.u(c.NextEdgeID)
		e.u(c.Nodes)
		e.u(c.Edges)
		e.u(max(c.SnapshotBytes, 4096))
		e.bytes(c.History[:])
		if c.ArchiveBasePending {
			e.tag(1)
		} else {
			e.tag(0)
		}
	})
	if err != nil {
		return err
	}
	return graph.Tx.Put(pageCatalogBucket, []byte("state"), data)
}
func (graph *PageGraph) LoadGraph(ctx context.Context) (*GraphState, PageCatalog, error) {
	c, err := graph.Catalog()
	if err != nil {
		return nil, c, err
	}
	state := NewGraphState()
	state.PageBase = graph
	state.DatabaseID = c.DatabaseID
	state.VectorDimensions = c.VectorDimensions
	// This is generation bookkeeping, not a resident-memory DB size ceiling.
	state.SnapshotBytes = max(c.SnapshotBytes, 4096)
	err = graph.Tx.Scan(ctx, pageMetadataBucket, nil, nil, func(key, value []byte) error {
		metadataKey, metadataValue, err := graph.decodeMetadata(key, value)
		if err != nil {
			return err
		}
		state.AppMetadata.Set(string(metadataKey), metadataValue)
		return nil
	})
	if err != nil {
		return nil, c, err
	}
	state.NodeProperties, err = graph.LoadPropertyIndexes(ctx, true)
	if err != nil {
		return nil, c, err
	}
	state.EdgeProperties, err = graph.LoadPropertyIndexes(ctx, false)
	if err != nil {
		return nil, c, err
	}
	state.Streams, err = graph.LoadStreams(ctx)
	if err != nil {
		return nil, c, err
	}
	return state, c, nil
}

// CheckpointDatabaseID reads only the native checkpoint envelope. A migrated
// source remains an identity anchor while its page sidecar is authoritative.
func CheckpointDatabaseID(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var header [stateHeaderSize]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return "", err
	}
	if !validStateHeader(header[:]) {
		return "", errors.New("invalid source checkpoint header")
	}
	id := string(header[32:])
	if err := validateDatabaseID(id); err != nil {
		return "", err
	}
	return id, nil
}
