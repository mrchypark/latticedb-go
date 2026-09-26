package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Page records use the existing typed value encoding, with a record-local
// checksum so a damaged record cannot turn into a successful missing lookup.
const pageRecordVersion = 1
const pageNodeRecord = 1
const pageEdgeRecord = 2

func encodePageRecord(kind byte, encode func(*binaryEncoder)) ([]byte, error) {
	var output bytes.Buffer
	output.Write([]byte{pageRecordVersion, kind, 0, 0, 0, 0})
	encoder := binaryEncoder{out: &output}
	encode(&encoder)
	if encoder.err != nil {
		return nil, encoder.err
	}
	data := output.Bytes()
	binary.BigEndian.PutUint32(data[2:6], crc32.ChecksumIEEE(data[6:]))
	return data, nil
}

func decodePageRecord(data []byte, kind byte, maxBytes uint64) (*binaryDecoder, error) {
	if uint64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: page record exceeds limit", ErrLoadResourceLimit)
	}
	if len(data) < 6 || data[0] != pageRecordVersion || data[1] != kind {
		return nil, errors.New("invalid page record header")
	}
	if binary.BigEndian.Uint32(data[2:6]) != crc32.ChecksumIEEE(data[6:]) {
		return nil, errors.New("page record checksum mismatch")
	}
	return newBinaryDecoder(bytes.NewReader(data[6:]), uint64(len(data)-6), maxBytes), nil
}

func encodePageNode(node *NodeRecord) ([]byte, error) {
	if node == nil {
		return nil, errors.New("nil page node")
	}
	if err := ValidateEntityID(node.ID); err != nil {
		return nil, err
	}
	if err := ValidateCreateLabels(node.Labels); err != nil {
		return nil, err
	}
	properties, err := encodePropertyStorage(node.Properties)
	if err != nil {
		return nil, err
	}
	return encodePageRecord(pageNodeRecord, func(e *binaryEncoder) {
		e.node(persistedNode{ID: node.ID, Labels: node.Labels, Properties: properties})
	})
}

func decodePageNode(data []byte, id, maxBytes uint64) (*NodeRecord, error) {
	d, err := decodePageRecord(data, pageNodeRecord, maxBytes)
	if err != nil {
		return nil, err
	}
	node := d.node()
	if err := d.finish(); err != nil {
		return nil, fmt.Errorf("decode page record: %w", err)
	}
	if node.ID != id {
		return nil, errors.New("page node key does not match record")
	}
	if err := ValidateEntityID(id); err != nil {
		return nil, err
	}
	if err := ValidateCreateLabels(node.Labels); err != nil {
		return nil, err
	}
	properties, err := decodePropertyStorage(node.Properties)
	if err != nil {
		return nil, err
	}
	return &NodeRecord{ID: id, Labels: node.Labels, Properties: properties}, nil
}

func encodePageEdge(edge *EdgeRecord) ([]byte, error) {
	if edge == nil {
		return nil, errors.New("nil page edge")
	}
	for _, id := range []uint64{edge.ID, edge.SourceID, edge.TargetID} {
		if err := ValidateEntityID(id); err != nil {
			return nil, err
		}
	}
	if err := ValidateEdgeType(edge.Type); err != nil {
		return nil, err
	}
	properties, err := encodePropertyStorage(edge.Properties)
	if err != nil {
		return nil, err
	}
	return encodePageRecord(pageEdgeRecord, func(e *binaryEncoder) {
		e.edge(persistedEdge{ID: edge.ID, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: properties})
	})
}

func decodePageEdge(data []byte, id, maxBytes uint64) (*EdgeRecord, error) {
	d, err := decodePageRecord(data, pageEdgeRecord, maxBytes)
	if err != nil {
		return nil, err
	}
	edge := d.edge()
	if err := d.finish(); err != nil {
		return nil, fmt.Errorf("decode page record: %w", err)
	}
	if edge.ID != id {
		return nil, errors.New("page edge key does not match record")
	}
	for _, id := range []uint64{edge.ID, edge.SourceID, edge.TargetID} {
		if err := ValidateEntityID(id); err != nil {
			return nil, err
		}
	}
	if err := ValidateEdgeType(edge.Type); err != nil {
		return nil, err
	}
	properties, err := decodePropertyStorage(edge.Properties)
	if err != nil {
		return nil, err
	}
	return &EdgeRecord{ID: id, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: properties}, nil
}
