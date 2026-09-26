package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

const pageMigrationBatch = 128
const pageMigrationBatchBytes = 4 << 20
const pageMigrationBufferedBytes = 8 << 20

// ImportPageCheckpoint streams one binary v5 checkpoint into a privately
// staged page database and publishes it at pagePath only after validation.
func ImportPageCheckpoint(ctx context.Context, input io.Reader, pagePath string) error {
	return importPageFiles(ctx, input, nil, pagePath, 0, 0, false, false, false, nil, nil, nil)
}

// ImportPageCheckpointWithWAL is the shared streaming importer used by restore
// and native-store migration. maxBytes, when nonzero, bounds total checkpoint
// plus WAL input bytes. expectedCommitID, when nonzero, must match the result.
func ImportPageCheckpointWithWAL(ctx context.Context, input io.Reader, segments []string, pagePath string, expectedCommitID, maxBytes uint64) error {
	return importPageFiles(ctx, input, segments, pagePath, expectedCommitID, maxBytes, true, expectedCommitID != 0, false, nil, nil, nil)
}

// MigrateToPages copies the current native checkpoint and its WAL prefix into
// a new page database. The source files are opened read-only and left intact.
func MigrateToPages(ctx context.Context, files DatabaseFiles, pagePath string, limits ...RecoveryLimits) error {
	if len(limits) > 1 {
		return errors.New("migration accepts at most one recovery limit set")
	}
	var recovery *recoveryBudget
	if len(limits) == 1 {
		recovery = &recoveryBudget{limits: limits[0]}
	}
	file, err := os.Open(files.State)
	if err != nil {
		return fmt.Errorf("open migration checkpoint: %w", err)
	}
	defer file.Close()
	segments := make([]string, 0, 2)
	for _, path := range []string{files.WALBase, files.WAL} {
		if _, err := os.Stat(path); err == nil {
			segments = append(segments, path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	var reservation *DatabaseFiles
	if files.IDs != "" {
		reservation = &files
	}
	return importPageFiles(ctx, file, segments, pagePath, 0, 0, true, false, false, reservation, nil, recovery)
}

// RestorePageBackup installs a checkpoint and ordered WAL segments using the
// same staged importer as native migration. maxBytes bounds their aggregate
// on-disk size; the restored catalog must reach commitID exactly.
func RestorePageBackup(ctx context.Context, basePath string, segments []string, pagePath string, commitID, maxBytes uint64, histories ...[32]byte) error {
	if len(histories) != 0 && len(histories) != 2 {
		return errors.New("restore requires either zero or two history values")
	}
	base, err := os.Open(basePath)
	if err != nil {
		return fmt.Errorf("open backup checkpoint: %w", err)
	}
	defer base.Close()
	return importPageFiles(ctx, base, segments, pagePath, commitID, maxBytes, true, true, true, nil, histories, nil)
}

func importPageFiles(ctx context.Context, checkpoint io.Reader, segments []string, target string, expectedCommitID, maxBytes uint64, replay, verifyCommit, strictReplay bool, reservation *DatabaseFiles, histories [][32]byte, recovery *recoveryBudget) (result error) {
	if ctx == nil {
		return errors.New("nil page import context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target == "" || checkpoint == nil {
		return errors.New("page import requires checkpoint input and target path")
	}
	var segmentBytes uint64
	if maxBytes != 0 {
		for _, path := range segments {
			info, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("stat WAL segment: %w", err)
			}
			if info.Size() < 0 || uint64(info.Size()) > maxBytes-segmentBytes {
				return fmt.Errorf("%w: backup inputs exceed %d bytes", ErrLoadResourceLimit, maxBytes)
			}
			segmentBytes += uint64(info.Size())
		}
	}
	checkpointBudget := uint64(0)
	segmentBudget := uint64(0)
	if maxBytes != 0 {
		checkpointBudget = maxBytes - segmentBytes
		segmentBudget = segmentBytes
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create page database directory: %w", err)
	}
	stage, err := os.CreateTemp(dir, ".latticedb-page-stage-*")
	if err != nil {
		return fmt.Errorf("create page staging file: %w", err)
	}
	stagePath := stage.Name()
	if err := stage.Close(); err != nil {
		_ = os.Remove(stagePath)
		return err
	}
	if err := os.Remove(stagePath); err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(stagePath)
	}()
	db, err := pagestore.Open(stagePath, pagestore.Options{})
	if err != nil {
		return err
	}
	closed := false
	importer := &pageImporter{ctx: ctx, db: db, recovery: recovery}
	defer func() {
		if !closed {
			if importer.tx != nil {
				_ = importer.tx.Rollback()
				importer.tx, importer.graph = nil, nil
			}
			if err := db.Close(); result == nil && err != nil {
				result = err
			}
		}
	}()
	checkpointHash := sha256.New()
	boundedInput := io.Reader(checkpoint)
	var checkpointLimit *migrationBudgetReader
	if maxBytes != 0 {
		checkpointLimit = &migrationBudgetReader{reader: checkpoint, remaining: checkpointBudget}
		boundedInput = checkpointLimit
	}
	header, _, err := importer.scanCheckpoint(io.TeeReader(boundedInput, checkpointHash), checkpointScanUnlimited(), true)
	if err != nil {
		return fmt.Errorf("import checkpoint: %w", err)
	}
	if err := importer.finishCheckpoint(header, checkpointHash.Sum(nil)); err != nil {
		return err
	}
	if len(histories) == 2 {
		if err := importer.setHistory(histories[0], header.CommitID); err != nil {
			return err
		}
	}
	if replay {
		var segmentLimit *migrationBudgetReader
		if maxBytes != 0 {
			segmentLimit = &migrationBudgetReader{remaining: segmentBudget}
		}
		for _, path := range segments {
			if err := importer.replaySegment(path, strictReplay, segmentLimit); err != nil {
				return fmt.Errorf("replay WAL %q: %w", path, err)
			}
		}
	}
	if err := importer.closeBatch(); err != nil {
		return err
	}
	if reservation != nil {
		read, err := db.Begin(false)
		if err != nil {
			return err
		}
		catalog, err := (&PageGraph{Tx: read}).Catalog()
		_ = read.Rollback()
		if err != nil {
			return err
		}
		nodeID, edgeID, err := LoadIDReservationFiles(*reservation, catalog.DatabaseID)
		if err != nil {
			return err
		}
		if nodeID > catalog.NextNodeID || edgeID > catalog.NextEdgeID {
			write, err := db.Begin(true)
			if err != nil {
				return err
			}
			catalog.NextNodeID = max(catalog.NextNodeID, nodeID)
			catalog.NextEdgeID = max(catalog.NextEdgeID, edgeID)
			if err := (&PageGraph{Tx: write}).PutCatalog(catalog); err != nil {
				_ = write.Rollback()
				return err
			}
			if err := write.Commit(); err != nil {
				return err
			}
		}
	}
	read, err := db.Begin(false)
	if err != nil {
		return err
	}
	finalGraph, finalCatalog, err := (&PageGraph{Tx: read}).LoadGraph(ctx)
	var finalSnapshotBytes uint64
	if err == nil {
		finalSnapshotBytes, err = EstimateSnapshotBytes(finalGraph)
	}
	_ = read.Rollback()
	if err != nil {
		return err
	}
	write, err := db.Begin(true)
	if err != nil {
		return err
	}
	finalCatalog.SnapshotBytes = finalSnapshotBytes
	if err := (&PageGraph{Tx: write}).PutCatalog(finalCatalog); err != nil {
		_ = write.Rollback()
		return err
	}
	if err := write.Commit(); err != nil {
		return err
	}
	if verifyCommit && finalCatalog.CommitID != expectedCommitID {
		return fmt.Errorf("backup commit mismatch: got %d, want %d", finalCatalog.CommitID, expectedCommitID)
	}
	if len(histories) == 2 && finalCatalog.History != histories[1] {
		return errors.New("backup history mismatch")
	}
	if err := db.Close(); err != nil {
		return err
	}
	closed = true
	file, err := os.OpenFile(stagePath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync staged page database: %w", err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Linking is atomic and fails if target already exists, unlike Rename.
	if err := os.Link(stagePath, target); err != nil {
		return fmt.Errorf("publish page database without overwrite: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		_ = os.Remove(target)
		return fmt.Errorf("sync page database directory: %w", err)
	}
	return nil
}

func (p *pageImporter) setHistory(history [32]byte, commitID uint64) error {
	if err := p.closeBatch(); err != nil {
		return err
	}
	tx, err := p.db.Begin(true)
	if err != nil {
		return err
	}
	graph := &PageGraph{Tx: tx}
	catalog, err := graph.Catalog()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if catalog.CommitID != commitID {
		_ = tx.Rollback()
		return errors.New("base history commit does not match checkpoint")
	}
	catalog.History = history
	if err := graph.PutCatalog(catalog); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Put("commit-history", pageID(commitID), history[:]); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type migrationBudgetReader struct {
	reader    io.Reader
	remaining uint64
}

func (r *migrationBudgetReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n != 0 {
			return 0, fmt.Errorf("%w: checkpoint and WAL inputs exceed configured byte limit", ErrLoadResourceLimit)
		}
		return 0, err
	}
	if uint64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= uint64(n)
	return n, err
}

func checkpointScanUnlimited() CheckpointScanLimits {
	return CheckpointScanLimits{MaxPayloadBytes: uint64(math.MaxInt64) - stateHeaderSize, MaxEntries: math.MaxUint64}
}

type pageImporter struct {
	ctx        context.Context
	db         *pagestore.DB
	tx         *pagestore.Tx
	graph      *PageGraph
	entries    int
	batchBytes uint64
	catalog    PageCatalog
	streamName string
	streamNext uint64
	recovery   *recoveryBudget
}

func (p *pageImporter) begin() error {
	if p.tx != nil {
		return nil
	}
	tx, err := p.db.Begin(true)
	if err != nil {
		return err
	}
	p.tx, p.graph = tx, &PageGraph{Tx: tx}
	return nil
}

func (p *pageImporter) batch(size uint64) error {
	p.entries++
	p.batchBytes = snapshotAdd(p.batchBytes, size)
	buffered, err := p.tx.BufferedBytes()
	if err != nil {
		return err
	}
	if p.entries < pageMigrationBatch && p.batchBytes < pageMigrationBatchBytes && buffered < pageMigrationBufferedBytes {
		return nil
	}
	return p.closeBatch()
}

func (p *pageImporter) closeBatch() error {
	if p.tx == nil {
		return nil
	}
	if err := p.tx.Commit(); err != nil {
		p.tx, p.graph = nil, nil
		return err
	}
	p.tx, p.graph = nil, nil
	p.entries = 0
	p.batchBytes = 0
	return nil
}

func (p *pageImporter) clear() error {
	if err := p.closeBatch(); err != nil {
		return err
	}
	buckets := []string{"nodes", "edges", "outgoing", "incoming", "edge-types", "edge-order", "labels", "counts", "fts", "metadata", pageNodePropertyIndexes, pageEdgePropertyIndexes, pageNodePropertyPostings, pageEdgePropertyPostings, pageStreamCatalog, pageStreamRecords, pageStreamOffsets, "archive-outbox", "commit-history", pageCatalogBucket}
	for _, bucket := range buckets {
		for {
			rtx, err := p.db.Begin(false)
			if err != nil {
				return err
			}
			keys := make([][]byte, 0, 128)
			err = rtx.Scan(p.ctx, bucket, nil, nil, func(key, _ []byte) error {
				keys = append(keys, append([]byte(nil), key...))
				if len(keys) == cap(keys) {
					return io.EOF
				}
				return nil
			})
			_ = rtx.Rollback()
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				break
			}
			wtx, err := p.db.Begin(true)
			if err != nil {
				return err
			}
			for _, key := range keys {
				if err := wtx.Delete(bucket, key); err != nil {
					_ = wtx.Rollback()
					return err
				}
			}
			if err := wtx.Commit(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *pageImporter) scanCheckpoint(input io.Reader, limits CheckpointScanLimits, chargePayload bool) (CheckpointScanHeader, CheckpointScanCounts, error) {
	if p.recovery != nil && chargePayload {
		var raw [stateHeaderSize]byte
		if _, err := io.ReadFull(input, raw[:]); err != nil {
			return CheckpointScanHeader{}, CheckpointScanCounts{}, err
		}
		payloadBytes := binary.BigEndian.Uint64(raw[20:28])
		if err := p.recovery.decodedBytes(payloadBytes); err != nil {
			return CheckpointScanHeader{}, CheckpointScanCounts{}, err
		}
		input = io.MultiReader(bytes.NewReader(raw[:]), input)
	}
	if err := p.clear(); err != nil {
		return CheckpointScanHeader{}, CheckpointScanCounts{}, err
	}
	visitor := CheckpointScanVisitor{
		Header: func(h CheckpointScanHeader) error {
			if err := p.begin(); err != nil {
				return err
			}
			p.catalog = PageCatalog{DatabaseID: h.DatabaseID, VectorDimensions: h.VectorDimensions, CommitID: h.CommitID, NextNodeID: h.NextNodeID, NextEdgeID: h.NextEdgeID}
			return p.graph.PutCatalog(p.catalog)
		},
		Metadata: func(record persistedAppMetadata) error {
			if err := p.begin(); err != nil {
				return err
			}
			digest := sha256.Sum256(record.Key)
			old, err := p.tx.Get(pageMetadataBucket, digest[:])
			if err != nil {
				return err
			}
			if old != nil {
				return errors.New("duplicate checkpoint metadata key")
			}
			if err := p.graph.PutMetadata(record.Key, record.Value, false); err != nil {
				return err
			}
			return p.batch(uint64(len(record.Key)+len(record.Value)) + 64)
		},
		Node: func(record persistedNode) error {
			if err := p.begin(); err != nil {
				return err
			}
			if old, err := p.graph.GetNode(record.ID); err != nil {
				return err
			} else if old != nil {
				return fmt.Errorf("duplicate checkpoint node %d", record.ID)
			}
			props, err := decodePropertyStorage(record.Properties)
			if err != nil {
				return err
			}
			if err := p.graph.PutNode(&NodeRecord{ID: record.ID, Labels: record.Labels, Properties: props}); err != nil {
				return err
			}
			size, err := nodeSnapshotBytes(&NodeRecord{ID: record.ID, Labels: record.Labels, Properties: props})
			if err != nil {
				return err
			}
			return p.batch(size)
		},
		Edge: func(record persistedEdge) error {
			if err := p.begin(); err != nil {
				return err
			}
			if old, err := p.graph.GetEdge(record.ID); err != nil {
				return err
			} else if old != nil {
				return fmt.Errorf("duplicate checkpoint edge %d", record.ID)
			}
			props, err := decodePropertyStorage(record.Properties)
			if err != nil {
				return err
			}
			if err := p.graph.PutEdge(&EdgeRecord{ID: record.ID, SourceID: record.SourceID, TargetID: record.TargetID, Type: record.Type, Properties: props}); err != nil {
				return err
			}
			size, err := edgeSnapshotBytes(&EdgeRecord{ID: record.ID, SourceID: record.SourceID, TargetID: record.TargetID, Type: record.Type, Properties: props})
			if err != nil {
				return err
			}
			return p.batch(size)
		},
		FTS: func(record persistedFTS) error {
			if err := p.begin(); err != nil {
				return err
			}
			old, err := p.tx.Get("fts", pageID(record.NodeID))
			if err != nil {
				return err
			}
			if old != nil {
				return fmt.Errorf("duplicate checkpoint FTS record %d", record.NodeID)
			}
			if err := p.graph.PutFTS(record.NodeID, &FTSRecord{Text: record.Text}); err != nil {
				return err
			}
			return p.batch(uint64(len(record.Text)) + 64)
		},
		NodeIndex: func(record persistedPropertyIndexDefinition) error { return p.importIndex(true, record) },
		EdgeIndex: func(record persistedPropertyIndexDefinition) error { return p.importIndex(false, record) },
		Stream: func(record persistedStream) error {
			if err := p.finishStream(); err != nil {
				return err
			}
			if err := p.begin(); err != nil {
				return err
			}
			old, err := p.tx.Get(pageStreamCatalog, []byte(record.Name))
			if err != nil {
				return err
			}
			if old != nil {
				return fmt.Errorf("duplicate checkpoint stream %q", record.Name)
			}
			if err := p.tx.Put(pageStreamCatalog, []byte(record.Name), encodePageStreamMeta(pageStreamMeta{next: 1})); err != nil {
				return err
			}
			p.streamName, p.streamNext = record.Name, record.Next
			return p.batch(uint64(len(record.Name)) + 64)
		},
		StreamRecord: func(name string, record persistedStreamRecord) error {
			if err := p.begin(); err != nil {
				return err
			}
			payload, err := decodeStreamValue(name, record.Payload)
			if err != nil {
				return err
			}
			encoded, err := encodePageStreamRecord(StreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: payload})
			if err != nil {
				return err
			}
			key := pageStreamRecordKey(name, record.Sequence)
			if old, err := p.tx.Get(pageStreamRecords, key); err != nil {
				return err
			} else if old != nil {
				return errors.New("duplicate checkpoint stream record")
			}
			if err := p.tx.Put(pageStreamRecords, key, encoded); err != nil {
				return err
			}
			metaBytes, err := p.tx.Get(pageStreamCatalog, []byte(name))
			if err != nil {
				return err
			}
			meta, err := decodePageStreamMeta(metaBytes)
			if err != nil {
				return err
			}
			decoded := StreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: payload}
			if meta.count == 0 {
				meta.first = record.Sequence
			} else if meta.next != record.Sequence {
				return errors.New("checkpoint stream sequence is not contiguous")
			}
			meta.count++
			meta.next = record.Sequence + 1
			meta.bytes = snapshotAdd(meta.bytes, streamRecordBytes(decoded))
			meta.snapshotBytes = snapshotAdd(meta.snapshotBytes, streamRecordSnapshotBytes(decoded))
			if err := p.tx.Put(pageStreamCatalog, []byte(name), encodePageStreamMeta(meta)); err != nil {
				return err
			}
			return p.batch(uint64(len(encoded)) + 64)
		},
		StreamOffset: func(record persistedStreamOffset) error {
			if err := p.finishStream(); err != nil {
				return err
			}
			if err := p.begin(); err != nil {
				return err
			}
			if _, err := p.tx.Get(pageStreamCatalog, []byte(record.Stream)); err != nil {
				return err
			} else {
				data, _ := p.tx.Get(pageStreamCatalog, []byte(record.Stream))
				if data == nil {
					return errors.New("checkpoint offset references missing stream")
				}
			}
			key := pageStreamOffsetKey(record.Stream, record.Consumer)
			if old, err := p.tx.Get(pageStreamOffsets, key); err != nil {
				return err
			} else if old != nil {
				return errors.New("duplicate checkpoint stream offset")
			}
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], record.Sequence)
			if err := p.tx.Put(pageStreamOffsets, key, value[:]); err != nil {
				return err
			}
			return p.batch(uint64(len(record.Stream)+len(record.Consumer)) + 64)
		},
	}
	visitor = p.budgetCheckpointVisitor(visitor)
	header, counts, err := ScanCheckpointV5(p.ctx, input, limits, visitor)
	if err == nil {
		err = p.finishStream()
	}
	return header, counts, err
}

func (p *pageImporter) budgetCheckpointVisitor(v CheckpointScanVisitor) CheckpointScanVisitor {
	charge := func(fn func() error) error {
		if p.recovery != nil {
			if err := p.recovery.replayWork(1); err != nil {
				return err
			}
		}
		return fn()
	}
	metadata, node, edge, fts := v.Metadata, v.Node, v.Edge, v.FTS
	nodeIndex, edgeIndex := v.NodeIndex, v.EdgeIndex
	stream, streamRecord, streamOffset := v.Stream, v.StreamRecord, v.StreamOffset
	v.Metadata = func(x persistedAppMetadata) error { return charge(func() error { return metadata(x) }) }
	v.Node = func(x persistedNode) error { return charge(func() error { return node(x) }) }
	v.Edge = func(x persistedEdge) error { return charge(func() error { return edge(x) }) }
	v.FTS = func(x persistedFTS) error { return charge(func() error { return fts(x) }) }
	v.NodeIndex = func(x persistedPropertyIndexDefinition) error { return charge(func() error { return nodeIndex(x) }) }
	v.EdgeIndex = func(x persistedPropertyIndexDefinition) error { return charge(func() error { return edgeIndex(x) }) }
	v.Stream = func(x persistedStream) error { return charge(func() error { return stream(x) }) }
	v.StreamRecord = func(name string, x persistedStreamRecord) error {
		return charge(func() error { return streamRecord(name, x) })
	}
	v.StreamOffset = func(x persistedStreamOffset) error { return charge(func() error { return streamOffset(x) }) }
	return v
}

func (p *pageImporter) finishStream() error {
	if p.streamName == "" {
		return nil
	}
	if err := p.begin(); err != nil {
		return err
	}
	data, err := p.tx.Get(pageStreamCatalog, []byte(p.streamName))
	if err != nil {
		return err
	}
	meta, err := decodePageStreamMeta(data)
	if err != nil {
		return err
	}
	meta.next = p.streamNext
	if err := p.tx.Put(pageStreamCatalog, []byte(p.streamName), encodePageStreamMeta(meta)); err != nil {
		return err
	}
	p.streamName, p.streamNext = "", 0
	return nil
}

func (p *pageImporter) importIndex(node bool, record persistedPropertyIndexDefinition) error {
	return p.createPropertyIndexBounded(node, PropertyIndexDefinition{Scope: record.Scope, Property: record.Property})
}

type pageIndexBatchRecord struct {
	id         uint64
	properties Properties
	scopes     []string
}

// createPropertyIndexBounded separates the definition write from index build
// and reads/writes postings in small batches, never pinning a read view while
// committing the corresponding write batch.
func (p *pageImporter) createPropertyIndexBounded(node bool, definition PropertyIndexDefinition) error {
	if err := p.closeBatch(); err != nil {
		return err
	}
	defBucket, _ := pagePropertyBuckets(node)
	key := pagePropertyDefinitionHash(definition)
	rtx, err := p.db.Begin(false)
	if err != nil {
		return err
	}
	old, err := rtx.Get(defBucket, key)
	_ = rtx.Rollback()
	if err != nil {
		return err
	}
	if old != nil {
		return fmt.Errorf("duplicate checkpoint property index %q.%q", definition.Scope, definition.Property)
	}
	wtx, err := p.db.Begin(true)
	if err != nil {
		return err
	}
	if err := wtx.Put(defBucket, key, encodePagePropertyDefinition(definition)); err != nil {
		_ = wtx.Rollback()
		return err
	}
	if err := wtx.Commit(); err != nil {
		return err
	}
	entityBucket := pageEdges
	if node {
		entityBucket = pageNodes
	}
	var last []byte
	for {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		read, err := p.db.Begin(false)
		if err != nil {
			return err
		}
		batch := make([]pageIndexBatchRecord, 0, 128)
		var readBytes uint64
		err = read.Scan(p.ctx, entityBucket, last, nil, func(rowKey, rowValue []byte) error {
			if len(last) != 0 && bytes.Equal(rowKey, last) {
				return nil
			}
			var record pageIndexBatchRecord
			if node {
				if len(rowKey) != 8 {
					return errors.New("invalid page node key")
				}
				n, err := decodePageNode(rowValue, binary.BigEndian.Uint64(rowKey), maxValueBytes+1<<20)
				if err != nil {
					return err
				}
				record = pageIndexBatchRecord{id: n.ID, properties: n.Properties, scopes: n.Labels}
			} else {
				if len(rowKey) != 8 {
					return errors.New("invalid page edge key")
				}
				e, err := decodePageEdge(rowValue, binary.BigEndian.Uint64(rowKey), maxValueBytes+1<<20)
				if err != nil {
					return err
				}
				record = pageIndexBatchRecord{id: e.ID, properties: e.Properties, scopes: []string{e.Type}}
			}
			batch = append(batch, record)
			last = bytes.Clone(rowKey)
			readBytes += uint64(len(rowValue))
			if len(batch) >= 128 || readBytes >= pageMigrationBufferedBytes {
				return io.EOF
			}
			return nil
		})
		_ = read.Rollback()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		var write *pagestore.Tx
		for _, record := range batch {
			if err := p.ctx.Err(); err != nil {
				if write != nil {
					_ = write.Rollback()
				}
				return err
			}
			if !propertyIndexMatches(definition, record.scopes, record.properties) {
				continue
			}
			value, _ := record.properties.Lookup(definition.Property)
			if write == nil {
				write, err = p.db.Begin(true)
				if err != nil {
					return err
				}
			}
			backend := pagePropertyBackend{graph: &PageGraph{Tx: write}, node: node}
			if err := backend.put(definition, value, record.id); err != nil {
				_ = write.Rollback()
				return err
			}
			buffered, err := write.BufferedBytes()
			if err != nil {
				_ = write.Rollback()
				return err
			}
			if buffered >= pageMigrationBufferedBytes {
				if err := write.Commit(); err != nil {
					return err
				}
				write = nil
			}
		}
		if write != nil {
			if err := write.Commit(); err != nil {
				return err
			}
		}
	}
}

func (p *pageImporter) finishCheckpoint(header CheckpointScanHeader, checkpointHash []byte) error {
	if err := p.begin(); err != nil {
		return err
	}
	var err error
	p.catalog.Nodes, err = p.graph.count(pageNodes)
	if err != nil {
		return err
	}
	p.catalog.Edges, err = p.graph.count(pageEdges)
	if err != nil {
		return err
	}
	copy(p.catalog.History[:], checkpointHash)
	if err := p.graph.PutCatalog(p.catalog); err != nil {
		return err
	}
	if err := p.tx.Put("commit-history", pageID(header.CommitID), p.catalog.History[:]); err != nil {
		return err
	}
	return p.closeBatch()
}

func (p *pageImporter) replaySegment(path string, strict bool, budget *migrationBudgetReader) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var source io.Reader = file
	if budget != nil {
		budget.reader = file
		source = budget
	}
	reader := checkpointContextReader{ctx: p.ctx, r: source}
	var total uint64
	for {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		var raw [walHeaderSize]byte
		header, err := readWALHeader(reader, &raw)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if strict {
				return err
			}
			return nil
		}
		if err != nil {
			return err
		}
		if !validWALHeader(header) {
			return errors.New("invalid WAL header")
		}
		if !isBinaryWALHeader(header) {
			return errors.New("unsupported WAL format: expected binary frames")
		}
		length := binary.BigEndian.Uint64(header[20:28])
		if length > maxWALFrameBytes {
			return fmt.Errorf("%w: WAL frame exceeds %d bytes", ErrLoadResourceLimit, maxWALFrameBytes)
		}
		if length > math.MaxInt64-total {
			return fmt.Errorf("%w: WAL segment length overflow", ErrLoadResourceLimit)
		}
		total += uint64(len(header)) + length
		if length == 0 {
			return errors.New("empty WAL frame")
		}
		// Only complete frames consume recovery budgets. Check the file length
		// before any payload allocation or streamed spool write.
		offset, seekErr := file.Seek(0, io.SeekCurrent)
		info, statErr := file.Stat()
		if seekErr != nil {
			return seekErr
		}
		if statErr != nil {
			return statErr
		}
		if offset < 0 || info.Size() < offset || length > uint64(info.Size()-offset) {
			if strict {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
		if p.recovery != nil {
			if err := p.recovery.frame(); err != nil {
				return err
			}
			if err := p.recovery.decodedBytes(length); err != nil {
				return err
			}
		}
		databaseID := strings.TrimRight(string(header[walDatabaseIDAt:legacyWALHeaderSize]), "\x00")
		if err := validateDatabaseID(databaseID); err != nil {
			return err
		}
		commitID := binary.BigEndian.Uint64(header[12:20])
		catalog, err := p.catalogNow()
		if err != nil {
			return err
		}
		if databaseID != catalog.DatabaseID {
			return errors.New("WAL database ID mismatch")
		}
		covered := commitID <= catalog.CommitID
		var tag [1]byte
		if _, err := io.ReadFull(reader, tag[:]); err != nil {
			return incompleteWALResult(err, strict)
		}
		frameCRC := crc32.NewIEEE()
		_, _ = frameCRC.Write(tag[:])
		bodyLength := length - 1
		var payload []byte
		var snapshotFile *os.File
		var snapshotCRC uint32
		if covered {
			if _, err := io.CopyN(frameCRC, reader, int64(bodyLength)); err != nil {
				return incompleteWALResult(err, strict)
			}
		} else if tag[0] == binaryWALSnapshot {
			snapshotFile, err = os.CreateTemp("", "latticedb-snapshot-*")
			if err != nil {
				return err
			}
			crc := crc32.NewIEEE()
			n, copyErr := io.CopyN(io.MultiWriter(snapshotFile, frameCRC, crc), reader, int64(bodyLength))
			if copyErr != nil || uint64(n) != bodyLength {
				_ = snapshotFile.Close()
				_ = os.Remove(snapshotFile.Name())
				if copyErr != nil {
					return incompleteWALResult(copyErr, strict)
				}
				return incompleteWALResult(io.ErrUnexpectedEOF, strict)
			}
			snapshotCRC = crc.Sum32()
		} else {
			if length > uint64(maxInt()) {
				return fmt.Errorf("%w: WAL frame too large for this platform", ErrLoadResourceLimit)
			}
			payload = make([]byte, int(length))
			payload[0] = tag[0]
			if _, err := io.ReadFull(reader, payload[1:]); err != nil {
				return incompleteWALResult(err, strict)
			}
			_, _ = frameCRC.Write(payload[1:])
		}
		if frameCRC.Sum32() != binary.BigEndian.Uint32(header[28:32]) {
			if snapshotFile != nil {
				_ = snapshotFile.Close()
				_ = os.Remove(snapshotFile.Name())
			}
			return errors.New("WAL frame checksum mismatch")
		}
		if covered {
			if snapshotFile != nil {
				_ = snapshotFile.Close()
				_ = os.Remove(snapshotFile.Name())
			}
			continue
		}
		if commitID != catalog.CommitID+1 {
			if snapshotFile != nil {
				_ = snapshotFile.Close()
				_ = os.Remove(snapshotFile.Name())
			}
			return fmt.Errorf("WAL commit gap: got %d after %d", commitID, catalog.CommitID)
		}
		if tag[0] == binaryWALSnapshot {
			if snapshotFile == nil {
				return errors.New("missing streamed WAL snapshot")
			}
			if err := p.importSnapshotFile(snapshotFile, bodyLength, uint32(snapshotCRC), databaseID, commitID, header, tag[:]); err != nil {
				_ = snapshotFile.Close()
				_ = os.Remove(snapshotFile.Name())
				return err
			}
			if err := snapshotFile.Close(); err != nil {
				_ = os.Remove(snapshotFile.Name())
				return err
			}
			_ = os.Remove(snapshotFile.Name())
			continue
		}
		decoded, err := decodeBinaryWALPayload(bytes.NewReader(payload), length, maxWALFrameBytes)
		if err != nil {
			return err
		}
		switch decoded.Kind {
		case "checkpoint":
			return fmt.Errorf("WAL checkpoint marker %d is ahead of imported checkpoint %d", commitID, catalog.CommitID)
		case "delta", "property_delta":
			if decoded.Delta == nil || decoded.Delta.DatabaseID != databaseID || decoded.Delta.CommitID != commitID {
				return errors.New("WAL delta metadata mismatch")
			}
			if err := p.applyDelta(*decoded.Delta, header, payload); err != nil {
				return err
			}
		default:
			return errors.New("unsupported WAL record")
		}
	}
}

func incompleteWALResult(err error, strict bool) error {
	if !strict && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return nil
	}
	if strict && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func maxInt() int { return int(^uint(0) >> 1) }

func (p *pageImporter) catalogNow() (PageCatalog, error) {
	if err := p.closeBatch(); err != nil {
		return PageCatalog{}, err
	}
	tx, err := p.db.Begin(false)
	if err != nil {
		return PageCatalog{}, err
	}
	catalog, err := (&PageGraph{Tx: tx}).Catalog()
	_ = tx.Rollback()
	return catalog, err
}

func (p *pageImporter) importSnapshotFile(payload *os.File, length uint64, checksum uint32, databaseID string, commitID uint64, frameHeader, tag []byte) error {
	previous, err := p.catalogNow()
	if err != nil {
		return err
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header, err := encodeStateHeader(databaseID, commitID, length, checksum)
	if err != nil {
		return err
	}
	checkpoint := io.MultiReader(bytes.NewReader(header[:]), payload)
	checkpointHash := sha256.New()
	_, _ = checkpointHash.Write(header[:])
	if _, err := io.Copy(checkpointHash, io.NewSectionReader(payload, 0, int64(length))); err != nil {
		return err
	}
	h, _, err := p.scanCheckpoint(checkpoint, checkpointScanUnlimited(), false)
	if err != nil {
		return err
	}
	if err := p.finishCheckpoint(h, checkpointHash.Sum(nil)); err != nil {
		return err
	}
	if err := p.begin(); err != nil {
		return err
	}
	p.catalog.History = previous.History
	if err := p.graph.PutCatalog(p.catalog); err != nil {
		return err
	}
	if err := p.closeBatch(); err != nil {
		return err
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return p.chainHistory(func(out io.Writer) error {
		if _, err := out.Write(frameHeader); err != nil {
			return err
		}
		if _, err := out.Write(tag); err != nil {
			return err
		}
		_, err := io.CopyN(out, payload, int64(length))
		return err
	})
}

func (p *pageImporter) chainHistory(writeFrame func(io.Writer) error) error {
	if err := p.begin(); err != nil {
		return err
	}
	catalog, err := p.graph.Catalog()
	if err != nil {
		return err
	}
	h := sha256.New()
	_, _ = h.Write(catalog.History[:])
	if err := writeFrame(h); err != nil {
		return err
	}
	copy(catalog.History[:], h.Sum(nil))
	if err := p.graph.PutCatalog(catalog); err != nil {
		return err
	}
	if err := p.tx.Put("commit-history", pageID(catalog.CommitID), catalog.History[:]); err != nil {
		return err
	}
	return p.closeBatch()
}

func (p *pageImporter) applyDelta(delta persistedDelta, frameHeader, framePayload []byte) error {
	if err := p.closeBatch(); err != nil {
		return err
	}
	if err := p.begin(); err != nil {
		return err
	}
	g := p.graph
	catalog, err := g.Catalog()
	if err != nil {
		return err
	}
	if delta.CommitID != catalog.CommitID+1 || delta.NextNodeID < catalog.NextNodeID || delta.NextEdgeID < catalog.NextEdgeID {
		return errors.New("WAL delta history or ID high-water regression")
	}
	if err := ValidateIDHighWater(delta.NextNodeID); err != nil {
		return err
	}
	if err := ValidateIDHighWater(delta.NextEdgeID); err != nil {
		return err
	}
	if err := validateUniqueDeltaIDs(delta.DeleteNodes, delta.UpsertNodes, func(n persistedNode) uint64 { return n.ID }); err != nil {
		return err
	}
	if err := validateUniqueDeltaIDs(delta.DeleteEdges, delta.UpsertEdges, func(e persistedEdge) uint64 { return e.ID }); err != nil {
		return err
	}
	if err := validateUniqueDeltaIDs(delta.DeleteFTS, delta.UpsertFTS, func(f persistedFTS) uint64 { return f.NodeID }); err != nil {
		return err
	}
	if delta.Streams != nil && len(delta.StreamOperations) != 0 {
		return errors.New("WAL delta mixes full and incremental streams")
	}
	for _, node := range delta.UpsertNodes {
		if node.ID >= delta.NextNodeID {
			return fmt.Errorf("node ID %d reaches high-water mark %d", node.ID, delta.NextNodeID)
		}
	}
	for _, edge := range delta.UpsertEdges {
		if edge.ID >= delta.NextEdgeID {
			return fmt.Errorf("edge ID %d reaches high-water mark %d", edge.ID, delta.NextEdgeID)
		}
	}
	if err := validatePropertyChanges(delta.NodePropertyChanges, func(id uint64) (map[string]persistedValue, bool) {
		node, err := g.GetNode(id)
		if err != nil || node == nil {
			return nil, false
		}
		values, err := encodePropertyStorage(node.Properties)
		return values, err == nil
	}); err != nil {
		return fmt.Errorf("node property changes: %w", err)
	}
	if err := validatePropertyChanges(delta.EdgePropertyChanges, func(id uint64) (map[string]persistedValue, bool) {
		edge, err := g.GetEdge(id)
		if err != nil || edge == nil {
			return nil, false
		}
		values, err := encodePropertyStorage(edge.Properties)
		return values, err == nil
	}); err != nil {
		return fmt.Errorf("edge property changes: %w", err)
	}
	for _, change := range delta.NodePropertyChanges {
		for _, id := range delta.DeleteNodes {
			if change.ID == id {
				return fmt.Errorf("node %d has both delete and property change", id)
			}
		}
		for _, node := range delta.UpsertNodes {
			if change.ID == node.ID {
				return fmt.Errorf("node %d has full upsert and property change", node.ID)
			}
		}
	}
	for _, change := range delta.EdgePropertyChanges {
		for _, id := range delta.DeleteEdges {
			if change.ID == id {
				return fmt.Errorf("edge %d has both delete and property change", id)
			}
		}
		for _, edge := range delta.UpsertEdges {
			if change.ID == edge.ID {
				return fmt.Errorf("edge %d has full upsert and property change", edge.ID)
			}
		}
	}
	if err := p.validateIndexDelta(true, delta.CreateNodeIndexes, delta.DropNodeIndexes); err != nil {
		return err
	}
	if err := p.validateIndexDelta(false, delta.CreateEdgeIndexes, delta.DropEdgeIndexes); err != nil {
		return err
	}
	if p.recovery != nil {
		work, err := p.deltaReplayWork(g, delta)
		if err != nil {
			return err
		}
		if err := p.recovery.replayWork(work); err != nil {
			return err
		}
	}
	if err := p.dropIndexes(delta.CreateNodeIndexes, delta.DropNodeIndexes, true); err != nil {
		return err
	}
	if err := p.dropIndexes(delta.CreateEdgeIndexes, delta.DropEdgeIndexes, false); err != nil {
		return err
	}
	for _, id := range delta.DeleteEdges {
		edge, err := g.GetEdge(id)
		if err != nil {
			return err
		}
		if edge == nil {
			return fmt.Errorf("delete missing edge %d", id)
		}
		if err := g.DeleteEdge(id); err != nil {
			return err
		}
	}
	for _, node := range delta.UpsertNodes {
		props, err := decodePropertyStorage(node.Properties)
		if err != nil {
			return err
		}
		if err := g.PutNode(&NodeRecord{ID: node.ID, Labels: node.Labels, Properties: props}); err != nil {
			return err
		}
	}
	for _, edge := range delta.UpsertEdges {
		props, err := decodePropertyStorage(edge.Properties)
		if err != nil {
			return err
		}
		if err := g.PutEdge(&EdgeRecord{ID: edge.ID, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: props}); err != nil {
			return err
		}
	}
	for _, change := range delta.NodePropertyChanges {
		node, err := g.GetNode(change.ID)
		if err != nil || node == nil {
			return errors.New("property patch references missing node")
		}
		if err := patchNode(node, change); err != nil {
			return err
		}
		if err := g.PutNode(node); err != nil {
			return err
		}
	}
	for _, change := range delta.EdgePropertyChanges {
		edge, err := g.GetEdge(change.ID)
		if err != nil || edge == nil {
			return errors.New("property patch references missing edge")
		}
		if err := patchEdge(edge, change); err != nil {
			return err
		}
		if err := g.PutEdge(edge); err != nil {
			return err
		}
	}
	for _, id := range delta.DeleteFTS {
		old, err := g.Tx.Get("fts", pageID(id))
		if err != nil {
			return err
		}
		if old == nil {
			return fmt.Errorf("delete missing FTS record %d", id)
		}
		if err := g.PutFTS(id, nil); err != nil {
			return err
		}
	}
	for _, id := range delta.DeleteNodes {
		var found bool
		for _, bucket := range []string{pageOutgoing, pageIncoming} {
			err := g.visitIDs(p.ctx, bucket, pageID(id), func(uint64) error { found = true; return io.EOF })
			if err != nil {
				return err
			}
			if found {
				return fmt.Errorf("delete node %d with incident edges", id)
			}
		}
		node, err := g.GetNode(id)
		if err != nil {
			return err
		}
		if node == nil {
			return fmt.Errorf("delete missing node %d", id)
		}
		if err := g.DeleteNode(p.ctx, id); err != nil {
			return err
		}
	}
	for _, record := range delta.UpsertFTS {
		if err := g.PutFTS(record.NodeID, &FTSRecord{Text: record.Text}); err != nil {
			return err
		}
	}
	seenMeta := map[string]bool{}
	for _, change := range delta.AppMetadata {
		if len(change.Key) == 0 || len(change.Key) > maxAppMetadataKeyBytes || seenMeta[string(change.Key)] {
			return errors.New("invalid or duplicate WAL metadata change")
		}
		seenMeta[string(change.Key)] = true
		if change.Delete {
			if err := g.PutMetadata(change.Key, nil, true); err != nil {
				return err
			}
		} else {
			if err := g.PutMetadata(change.Key, change.Value, false); err != nil {
				return err
			}
		}
	}
	if delta.Streams != nil {
		if err := p.replaceStreams(*delta.Streams); err != nil {
			return err
		}
	} else {
		ops := make([]StreamOperation, 0, len(delta.StreamOperations))
		for _, op := range delta.StreamOperations {
			payload, err := decodeStreamValue(op.Stream, op.Payload)
			if err != nil {
				return err
			}
			ops = append(ops, StreamOperation{Type: op.Type, Stream: op.Stream, Consumer: op.Consumer, Sequence: op.Sequence, Kind: op.Kind, Payload: payload})
		}
		if err := g.ApplyStreamOperations(p.ctx, ops); err != nil {
			return err
		}
	}
	for _, definition := range delta.CreateNodeIndexes {
		if err := g.CreatePropertyIndex(p.ctx, true, PropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property}); err != nil {
			return err
		}
	}
	for _, definition := range delta.CreateEdgeIndexes {
		if err := g.CreatePropertyIndex(p.ctx, false, PropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property}); err != nil {
			return err
		}
	}
	catalog.CommitID = delta.CommitID
	catalog.NextNodeID = delta.NextNodeID
	catalog.NextEdgeID = delta.NextEdgeID
	catalog.Nodes, err = g.count(pageNodes)
	if err != nil {
		return err
	}
	catalog.Edges, err = g.count(pageEdges)
	if err != nil {
		return err
	}
	hash := sha256.New()
	_, _ = hash.Write(catalog.History[:])
	_, _ = hash.Write(frameHeader)
	_, _ = hash.Write(framePayload)
	copy(catalog.History[:], hash.Sum(nil))
	if err := g.PutCatalog(catalog); err != nil {
		return err
	}
	if err := g.Tx.Put("commit-history", pageID(catalog.CommitID), catalog.History[:]); err != nil {
		return err
	}
	if err := g.Tx.Delete("archive-outbox", pageID(catalog.CommitID-1)); err != nil {
		return err
	}
	return p.closeBatch()
}

// deltaReplayWork mirrors recovery's persistedDeltaWork and property patch
// accounting while reading prior properties from the staged page graph.
func (p *pageImporter) deltaReplayWork(g *PageGraph, delta persistedDelta) (uint64, error) {
	work := persistedDeltaWork(delta)
	addChanges := func(changes []persistedPropertyChange, node bool) error {
		for _, change := range changes {
			if err := p.ctx.Err(); err != nil {
				return err
			}
			var properties *Properties
			if node {
				record, err := g.GetNode(change.ID)
				if err != nil {
					return err
				}
				if record == nil {
					return errors.New("property patch references missing node")
				}
				properties = &record.Properties
			} else {
				record, err := g.GetEdge(change.ID)
				if err != nil {
					return err
				}
				if record == nil {
					return errors.New("property patch references missing edge")
				}
				properties = &record.Properties
			}
			values, err := encodePropertyStorage(*properties)
			if err != nil {
				return err
			}
			_, priorWork, err := derivePropertyTotals(p.ctx, values)
			if err != nil {
				return err
			}
			work = addSaturated(work, addSaturated(1, priorWork))
			work = addSaturated(work, uint64(len(change.Set)+len(change.Remove)))
			for _, value := range change.Set {
				structure, err := persistedValueStructureWork(p.ctx, value, 0)
				if err != nil {
					return err
				}
				work = addSaturated(work, structure)
			}
		}
		return nil
	}
	if err := addChanges(delta.NodePropertyChanges, true); err != nil {
		return 0, err
	}
	if err := addChanges(delta.EdgePropertyChanges, false); err != nil {
		return 0, err
	}
	return work, nil
}

func (p *pageImporter) validateIndexDelta(node bool, creates, drops []persistedPropertyIndexDefinition) error {
	indexes, err := p.graph.LoadPropertyIndexes(p.ctx, node)
	if err != nil {
		return err
	}
	definitions := make([]persistedPropertyIndexDefinition, 0)
	for definition := range indexes.Definitions() {
		definitions = append(definitions, persistedPropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property})
	}
	if err := applyPropertyIndexDelta(&definitions, creates, drops); err != nil {
		return err
	}
	return nil
}

func (p *pageImporter) dropIndexes(creates, drops []persistedPropertyIndexDefinition, node bool) error {
	for _, def := range drops {
		if err := p.graph.DropPropertyIndex(p.ctx, node, PropertyIndexDefinition{Scope: def.Scope, Property: def.Property}); err != nil {
			return err
		}
	}
	return nil
}

func patchNode(node *NodeRecord, change persistedPropertyChange) error {
	return patchProperties(&node.Properties, change)
}
func patchEdge(edge *EdgeRecord, change persistedPropertyChange) error {
	return patchProperties(&edge.Properties, change)
}
func patchProperties(properties *Properties, change persistedPropertyChange) error {
	if err := ValidateEntityID(change.ID); err != nil {
		return err
	}
	seen := map[string]bool{}
	for key, value := range change.Set {
		if err := ValidatePropertyKey(key); err != nil {
			return err
		}
		if seen[key] {
			return errors.New("duplicate property patch key")
		}
		seen[key] = true
		decoded, err := decodeValue(value)
		if err != nil {
			return err
		}
		properties.Set(key, decoded)
	}
	for _, key := range change.Remove {
		if err := ValidatePropertyKey(key); err != nil {
			return err
		}
		if seen[key] {
			return errors.New("property patch both sets and removes key")
		}
		seen[key] = true
		properties.Delete(key)
	}
	return nil
}

func (p *pageImporter) replaceStreams(streams persistedStreams) error {
	for _, bucket := range []string{pageStreamCatalog, pageStreamRecords, pageStreamOffsets} {
		for {
			keys := make([][]byte, 0, 128)
			err := p.tx.Scan(p.ctx, bucket, nil, nil, func(k, _ []byte) error {
				keys = append(keys, append([]byte(nil), k...))
				if len(keys) == cap(keys) {
					return io.EOF
				}
				return nil
			})
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				break
			}
			for _, k := range keys {
				if err := p.tx.Delete(bucket, k); err != nil {
					return err
				}
			}
		}
	}
	store, err := decodePersistedStreams(streams)
	if err != nil {
		return err
	}
	return p.graph.ImportStreams(p.ctx, store)
}
