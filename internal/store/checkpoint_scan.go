package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// CheckpointScanLimits bounds the checkpoint payload, total visited entries,
// and decoded bytes for any one record. Zero fields use conservative defaults.
type CheckpointScanLimits struct {
	MaxPayloadBytes uint64
	MaxEntries      uint64
	MaxRecordBytes  uint64
}

type CheckpointScanHeader struct {
	DatabaseID       string
	VectorDimensions uint16
	CommitID         uint64
	NextNodeID       uint64
	NextEdgeID       uint64
}

type CheckpointScanCounts struct {
	Metadata, Nodes, Edges, FTS, NodeIndexes, EdgeIndexes uint64
	Streams, StreamRecords, StreamOffsets                 uint64
}

// CheckpointScanVisitor receives entries in wire order. Callbacks write only
// to an unpublished staging target: they run before the final checksum check.
// Publish that target only after ScanCheckpointV5 returns nil.
type CheckpointScanVisitor struct {
	Header       func(CheckpointScanHeader) error
	Metadata     func(persistedAppMetadata) error
	Node         func(persistedNode) error
	Edge         func(persistedEdge) error
	FTS          func(persistedFTS) error
	NodeIndex    func(persistedPropertyIndexDefinition) error
	EdgeIndex    func(persistedPropertyIndexDefinition) error
	Stream       func(persistedStream) error // Records is always nil.
	StreamRecord func(string, persistedStreamRecord) error
	StreamOffset func(persistedStreamOffset) error
}

type checkpointContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r checkpointContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// ScanCheckpointV5 streams one current binary v5 checkpoint. It retains no
// collection slices; the largest decoded object is bounded by MaxRecordBytes.
// It rejects trailing bytes and reports success only after checksum validation.
func ScanCheckpointV5(ctx context.Context, input io.Reader, limits CheckpointScanLimits, visitor CheckpointScanVisitor) (CheckpointScanHeader, CheckpointScanCounts, error) {
	var header CheckpointScanHeader
	var counts CheckpointScanCounts
	if ctx == nil {
		ctx = context.Background()
	}
	if input == nil {
		return header, counts, errors.New("nil checkpoint input")
	}
	if err := ctx.Err(); err != nil {
		return header, counts, err
	}
	if limits.MaxPayloadBytes == 0 {
		limits.MaxPayloadBytes = maxStateFileBytes - stateHeaderSize
	}
	if limits.MaxEntries == 0 {
		limits.MaxEntries = maxStateFileBytes
	}
	if limits.MaxRecordBytes == 0 {
		limits.MaxRecordBytes = maxValueBytes + 1<<20
	}
	var err error

	reader := checkpointContextReader{ctx: ctx, r: input}
	var rawHeader [stateHeaderSize]byte
	if _, err := io.ReadFull(reader, rawHeader[:]); err != nil {
		return header, counts, err
	}
	if string(rawHeader[:8]) != string(stateBinaryMagic[:]) || binary.BigEndian.Uint16(rawHeader[8:10]) != stateVersion || !validStateHeader(rawHeader[:]) {
		return header, counts, errors.New("unsupported checkpoint format: expected binary v5")
	}
	payloadBytes := binary.BigEndian.Uint64(rawHeader[20:28])
	// The default retains the historical 1 GiB safety cap. Explicit callers
	// may raise it for streamed imports, but io.LimitReader takes int64.
	if payloadBytes > limits.MaxPayloadBytes || payloadBytes > uint64(^uint64(0)>>1)-stateHeaderSize {
		return header, counts, fmt.Errorf("%w: checkpoint payload exceeds scan limit", ErrLoadResourceLimit)
	}
	expectedChecksum := binary.BigEndian.Uint32(rawHeader[28:32])
	expectedDatabaseID := string(rawHeader[32:stateHeaderSize])
	if err := validateDatabaseID(expectedDatabaseID); err != nil {
		return header, counts, err
	}
	header.CommitID = binary.BigEndian.Uint64(rawHeader[12:20])
	checksum := crc32.NewIEEE()
	payload := io.TeeReader(io.LimitReader(reader, int64(payloadBytes)), checksum)
	d := newBinaryDecoder(payload, payloadBytes, limits.MaxRecordBytes)
	var totalEntries uint64
	addEntries := func(n uint64) error {
		if n > limits.MaxEntries-totalEntries {
			return fmt.Errorf("%w: checkpoint entry count exceeds scan limit", ErrLoadResourceLimit)
		}
		totalEntries += n
		return nil
	}
	collection := func() (uint64, error) {
		encoded := d.u()
		if d.err != nil {
			return 0, d.err
		}
		if encoded == 0 {
			return 0, nil
		}
		n := encoded - 1
		if n > d.remaining || n > uint64(^uint(0)>>1) {
			return 0, fmt.Errorf("%w: invalid binary collection length", ErrLoadResourceLimit)
		}
		if err := addEntries(n); err != nil {
			return 0, err
		}
		return n, nil
	}
	beginRecord := func() (uint64, error) {
		if err := ctx.Err(); err != nil {
			return d.remaining, err
		}
		d.allocationLeft = multiplySaturated(limits.MaxRecordBytes, 2)
		return d.remaining, nil
	}
	endRecord := func(start uint64) error {
		if d.err != nil {
			return d.err
		}
		if start-d.remaining > limits.MaxRecordBytes {
			return fmt.Errorf("%w: checkpoint record exceeds scan limit", ErrLoadResourceLimit)
		}
		return nil
	}
	call := func(fn func() error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn()
	}

	header.DatabaseID = d.str()
	dimensions := d.u()
	commitID := d.u()
	header.NextNodeID = d.u()
	header.NextEdgeID = d.u()
	if d.err != nil {
		return header, counts, d.err
	}
	if err := validateDatabaseID(header.DatabaseID); err != nil {
		return header, counts, err
	}
	if header.DatabaseID != expectedDatabaseID || commitID != header.CommitID {
		return header, counts, errors.New("checkpoint header metadata mismatch")
	}
	if dimensions > 65535 {
		return header, counts, errors.New("binary vector dimensions overflow")
	}
	header.VectorDimensions = uint16(dimensions)
	if err := ValidateIDHighWater(header.NextNodeID); err != nil {
		return header, counts, err
	}
	if err := ValidateIDHighWater(header.NextEdgeID); err != nil {
		return header, counts, err
	}
	if err := call(func() error {
		if visitor.Header != nil {
			return visitor.Header(header)
		}
		return nil
	}); err != nil {
		return header, counts, err
	}

	if counts.Metadata, err = collection(); err != nil {
		return header, counts, err
	}
	for range counts.Metadata {
		start, err := beginRecord()
		if err != nil {
			return header, counts, err
		}
		entry := persistedAppMetadata{Key: d.bytes(), Value: d.bytes()}
		if err := endRecord(start); err != nil {
			return header, counts, err
		}
		if len(entry.Key) == 0 || len(entry.Key) > maxAppMetadataKeyBytes {
			return header, counts, errors.New("invalid stored application metadata key length")
		}
		if err := call(func() error {
			if visitor.Metadata != nil {
				return visitor.Metadata(entry)
			}
			return nil
		}); err != nil {
			return header, counts, err
		}
	}
	if counts.Nodes, err = collection(); err != nil {
		return header, counts, err
	}
	for range counts.Nodes {
		start, err := beginRecord()
		if err != nil {
			return header, counts, err
		}
		node := d.node()
		if err := endRecord(start); err != nil {
			return header, counts, err
		}
		if err := ValidateEntityID(node.ID); err != nil {
			return header, counts, err
		}
		if err := ValidateCreateLabels(node.Labels); err != nil {
			return header, counts, err
		}
		if _, err := decodePropertyStorage(node.Properties); err != nil {
			return header, counts, err
		}
		if err := call(func() error {
			if visitor.Node != nil {
				return visitor.Node(node)
			}
			return nil
		}); err != nil {
			return header, counts, err
		}
	}
	if counts.Edges, err = collection(); err != nil {
		return header, counts, err
	}
	for range counts.Edges {
		start, err := beginRecord()
		if err != nil {
			return header, counts, err
		}
		edge := d.edge()
		if err := endRecord(start); err != nil {
			return header, counts, err
		}
		for _, id := range []uint64{edge.ID, edge.SourceID, edge.TargetID} {
			if err := ValidateEntityID(id); err != nil {
				return header, counts, err
			}
		}
		if err := ValidateEdgeType(edge.Type); err != nil {
			return header, counts, err
		}
		if _, err := decodePropertyStorage(edge.Properties); err != nil {
			return header, counts, err
		}
		if err := call(func() error {
			if visitor.Edge != nil {
				return visitor.Edge(edge)
			}
			return nil
		}); err != nil {
			return header, counts, err
		}
	}
	if counts.FTS, err = collection(); err != nil {
		return header, counts, err
	}
	for range counts.FTS {
		start, err := beginRecord()
		if err != nil {
			return header, counts, err
		}
		record := d.fts()
		if err := endRecord(start); err != nil {
			return header, counts, err
		}
		if err := ValidateEntityID(record.NodeID); err != nil {
			return header, counts, err
		}
		if err := ValidateFTSText(record.Text); err != nil {
			return header, counts, err
		}
		if err := call(func() error {
			if visitor.FTS != nil {
				return visitor.FTS(record)
			}
			return nil
		}); err != nil {
			return header, counts, err
		}
	}
	if counts.NodeIndexes, err = scanCheckpointIndexes(d, limits, ctx, visitor.NodeIndex, addEntries); err != nil {
		return header, counts, err
	}
	if counts.EdgeIndexes, err = scanCheckpointIndexes(d, limits, ctx, visitor.EdgeIndex, addEntries); err != nil {
		return header, counts, err
	}
	counts.Streams, err = scanCheckpointStreams(d, limits, ctx, visitor, addEntries, &counts)
	if err != nil {
		return header, counts, err
	}
	if err := d.finish(); err != nil {
		return header, counts, err
	}
	if uint64(checksum.Sum32()) != uint64(expectedChecksum) {
		return header, counts, errors.New("state checksum mismatch")
	}
	var trailing [1]byte
	n, err := reader.Read(trailing[:])
	if n != 0 {
		return header, counts, errors.New("checkpoint has trailing data")
	}
	if err != io.EOF {
		if err == nil {
			return header, counts, io.ErrNoProgress
		}
		return header, counts, err
	}
	return header, counts, nil
}

func scanCheckpointIndexes(d *binaryDecoder, limits CheckpointScanLimits, ctx context.Context, visit func(persistedPropertyIndexDefinition) error, addEntries func(uint64) error) (uint64, error) {
	encoded := d.u()
	if d.err != nil {
		return 0, d.err
	}
	if encoded == 0 {
		return 0, nil
	}
	n := encoded - 1
	if n > d.remaining || n > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("%w: invalid binary collection length", ErrLoadResourceLimit)
	}
	if err := addEntries(n); err != nil {
		return 0, err
	}
	for range n {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		start := d.remaining
		d.allocationLeft = multiplySaturated(limits.MaxRecordBytes, 2)
		definition := persistedPropertyIndexDefinition{Scope: d.str(), Property: d.str()}
		if d.err != nil {
			return 0, d.err
		}
		if start-d.remaining > limits.MaxRecordBytes {
			return 0, fmt.Errorf("%w: checkpoint record exceeds scan limit", ErrLoadResourceLimit)
		}
		if definition.Scope == "" || definition.Property == "" {
			return 0, errors.New("stored property index has an empty definition")
		}
		if visit != nil {
			if err := visit(definition); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

func scanCheckpointStreams(d *binaryDecoder, limits CheckpointScanLimits, ctx context.Context, visitor CheckpointScanVisitor, addEntries func(uint64) error, counts *CheckpointScanCounts) (uint64, error) {
	streams, err := checkpointCollection(d, addEntries)
	if err != nil {
		return 0, err
	}
	for range streams {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		start := d.remaining
		d.allocationLeft = multiplySaturated(limits.MaxRecordBytes, 2)
		stream := persistedStream{Name: d.str(), Next: d.u()}
		if d.err != nil {
			return 0, d.err
		}
		if start-d.remaining > limits.MaxRecordBytes {
			return 0, fmt.Errorf("%w: checkpoint record exceeds scan limit", ErrLoadResourceLimit)
		}
		if err := ValidateStreamName(stream.Name, true); err != nil || stream.Next == 0 {
			return 0, errors.New("invalid persisted stream")
		}
		if visitor.Stream != nil {
			if err := visitor.Stream(stream); err != nil {
				return 0, err
			}
		}
		recordCount, err := checkpointCollection(d, addEntries)
		if err != nil {
			return 0, err
		}
		var previous uint64
		for range recordCount {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			start = d.remaining
			d.allocationLeft = multiplySaturated(limits.MaxRecordBytes, 2)
			record := persistedStreamRecord{Sequence: d.u(), Kind: d.str(), Payload: d.value(0)}
			if d.err != nil {
				return 0, d.err
			}
			if start-d.remaining > limits.MaxRecordBytes {
				return 0, fmt.Errorf("%w: checkpoint record exceeds scan limit", ErrLoadResourceLimit)
			}
			if record.Sequence == 0 || record.Sequence <= previous || record.Sequence >= stream.Next || previous != 0 && record.Sequence != previous+1 {
				return 0, errors.New("invalid persisted stream sequence")
			}
			if err := ValidateStreamKind(record.Kind); err != nil {
				return 0, err
			}
			if _, err := decodeStreamValue(stream.Name, record.Payload); err != nil {
				return 0, err
			}
			if visitor.StreamRecord != nil {
				if err := visitor.StreamRecord(stream.Name, record); err != nil {
					return 0, err
				}
			}
			previous = record.Sequence
			counts.StreamRecords++
		}
		if previous != 0 && stream.Next != previous+1 {
			return 0, errors.New("invalid persisted stream next sequence")
		}
		counts.Streams++
	}
	offsets, err := checkpointCollection(d, addEntries)
	if err != nil {
		return 0, err
	}
	for range offsets {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		start := d.remaining
		d.allocationLeft = multiplySaturated(limits.MaxRecordBytes, 2)
		offset := persistedStreamOffset{Stream: d.str(), Consumer: d.str(), Sequence: d.u()}
		if d.err != nil {
			return 0, d.err
		}
		if start-d.remaining > limits.MaxRecordBytes {
			return 0, fmt.Errorf("%w: checkpoint record exceeds scan limit", ErrLoadResourceLimit)
		}
		if err := ValidateStreamName(offset.Stream, true); err != nil {
			return 0, err
		}
		if err := ValidateStreamName(offset.Consumer, true); err != nil {
			return 0, err
		}
		if visitor.StreamOffset != nil {
			if err := visitor.StreamOffset(offset); err != nil {
				return 0, err
			}
		}
		counts.StreamOffsets++
	}
	return streams, nil
}

func checkpointCollection(d *binaryDecoder, addEntries func(uint64) error) (uint64, error) {
	encoded := d.u()
	if d.err != nil {
		return 0, d.err
	}
	if encoded == 0 {
		return 0, nil
	}
	n := encoded - 1
	if n > d.remaining || n > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("%w: invalid binary collection length", ErrLoadResourceLimit)
	}
	if err := addEntries(n); err != nil {
		return 0, err
	}
	return n, nil
}
