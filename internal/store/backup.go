package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WriteGraphState streams a checkpoint through a temporary payload file so
// callers can hash and publish large bases without retaining them in memory.
func WriteGraphState(output io.Writer, graph *GraphState, nextNodeID, nextEdgeID, commitID uint64) error {
	if err := ensureDatabaseID(graph); err != nil {
		return err
	}
	payload, err := os.CreateTemp("", "latticedb-backup-state-*")
	if err != nil {
		return err
	}
	name := payload.Name()
	defer os.Remove(name)
	defer payload.Close()
	checksum := crc32.NewIEEE()
	if err := writePersistedStateBinary(io.MultiWriter(payload, checksum), graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return err
	}
	info, err := payload.Stat()
	if err != nil {
		return err
	}
	header, err := encodeStateHeader(graph.DatabaseID, commitID, uint64(info.Size()), checksum.Sum32())
	if err != nil {
		return err
	}
	written, err := output.Write(header[:])
	if err != nil {
		return err
	}
	if written != len(header) {
		return io.ErrShortWrite
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = io.Copy(output, payload)
	return err
}

func backupRangeChecksum(file *os.File, offset, size int64) (uint32, error) {
	hash := crc32.NewIEEE()
	if _, err := io.CopyN(hash, io.NewSectionReader(file, offset, size), size); err != nil {
		return 0, err
	}
	return hash.Sum32(), nil
}

// CopyCommittedWALRangeTo streams just the frames committed between TailSize
// readings, validating each frame before copying it to output.
func (writer *WALWriter) CopyCommittedWALRangeTo(output io.Writer, startTail, endTail int64, databaseID string, afterCommit, throughCommit uint64) error {
	if writer == nil || writer.file == nil || startTail < 0 || endTail <= startTail {
		return errors.New("invalid committed WAL range")
	}
	var base [walHeaderSize]byte
	if _, err := writer.file.ReadAt(base[:], 0); err != nil || !validCurrentWALHeader(base[:]) {
		return errors.New("invalid WAL base header")
	}
	if string(bytes.TrimRight(base[walDatabaseIDAt:legacyWALHeaderSize], "\x00")) != databaseID {
		return errors.New("WAL base database ID mismatch")
	}
	baseLength := binary.BigEndian.Uint64(base[20:28])
	if baseLength > maxWALFrameBytes || baseLength > uint64(^uint64(0)>>1)-walHeaderSize {
		return errors.New("invalid WAL base frame length")
	}
	start := int64(walHeaderSize) + int64(baseLength) + startTail
	end := int64(walHeaderSize) + int64(baseLength) + endTail
	info, err := writer.file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL committed range: %w", err)
	}
	if end > info.Size() {
		return errors.New("committed WAL range exceeds file size")
	}
	var header [walHeaderSize]byte
	commit, offset := afterCommit, start
	for offset < end {
		if end-offset < walHeaderSize {
			return errors.New("truncated committed WAL frame")
		}
		if _, err := writer.file.ReadAt(header[:], offset); err != nil {
			return fmt.Errorf("read committed WAL frame header: %w", err)
		}
		length := binary.BigEndian.Uint64(header[20:28])
		if !validCurrentWALHeader(header[:]) || length > maxWALFrameBytes || int64(length) > end-offset-walHeaderSize {
			return errors.New("invalid committed WAL frame")
		}
		frameCommit := binary.BigEndian.Uint64(header[12:20])
		if string(bytes.TrimRight(header[walDatabaseIDAt:legacyWALHeaderSize], "\x00")) != databaseID || frameCommit != commit+1 {
			return errors.New("committed WAL database ID or commit sequence mismatch")
		}
		checksum, err := backupRangeChecksum(writer.file, offset+walHeaderSize, int64(length))
		if err != nil || checksum != binary.BigEndian.Uint32(header[28:32]) {
			return errors.Join(errors.New("committed WAL frame checksum mismatch"), err)
		}
		written, err := output.Write(header[:])
		if err != nil {
			return err
		}
		if written != len(header) {
			return io.ErrShortWrite
		}
		if _, err := io.CopyN(output, io.NewSectionReader(writer.file, offset+walHeaderSize, int64(length)), int64(length)); err != nil {
			return err
		}
		offset += walHeaderSize + int64(length)
		commit = frameCommit
	}
	if offset != end || commit != throughCommit {
		return errors.New("committed WAL range does not end at requested commit")
	}
	return nil
}

// ValidateBackupWALSegmentFile verifies segment frames with bounded payload
// memory, independent of the total WAL history represented by the archive.
func ValidateBackupWALSegmentFile(path, databaseID string, afterCommit, throughCommit uint64) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open backup WAL segment: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	var header [walHeaderSize]byte
	offset, commit := int64(0), afterCommit
	for offset < info.Size() {
		if info.Size()-offset < walHeaderSize {
			return errors.New("truncated backup WAL frame header")
		}
		if _, err := file.ReadAt(header[:], offset); err != nil {
			return err
		}
		length := binary.BigEndian.Uint64(header[20:28])
		if !validCurrentWALHeader(header[:]) || length > maxWALFrameBytes || length > uint64(info.Size()-offset-walHeaderSize) {
			return errors.New("invalid backup WAL frame")
		}
		frameCommit := binary.BigEndian.Uint64(header[12:20])
		if string(bytes.TrimRight(header[walDatabaseIDAt:legacyWALHeaderSize], "\x00")) != databaseID || frameCommit != commit+1 {
			return errors.New("backup WAL database ID or commit sequence mismatch")
		}
		checksum, err := backupRangeChecksum(file, offset+walHeaderSize, int64(length))
		if err != nil || checksum != binary.BigEndian.Uint32(header[28:32]) {
			return errors.Join(errors.New("backup WAL frame checksum mismatch"), err)
		}
		offset += walHeaderSize + int64(length)
		commit = frameCommit
	}
	if commit != throughCommit {
		return errors.New("backup WAL segment ends at unexpected commit")
	}
	return nil
}

func appendBackupFiles(ctx context.Context, wal *os.File, paths []string) error {
	for _, path := range paths {
		segment, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(wal, &contextReader{ctx: ctx, reader: segment})
		closeErr := segment.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
	}
	return nil
}

func replayBackupWALFiles(ctx context.Context, basePath string, segmentPaths []string, maxSnapshotBytes uint64) (*GraphState, uint64, uint64, uint64, error) {
	baseGraph, baseNextNodeID, baseNextEdgeID, baseCommitID, err := LoadGraphStateFilesContext(ctx, FlatDatabaseFiles(basePath), maxSnapshotBytes, ^uint64(0), ^uint64(0))
	if err != nil {
		return nil, 0, 0, 0, err
	}
	if len(segmentPaths) == 0 {
		return baseGraph, baseNextNodeID, baseNextEdgeID, baseCommitID, nil
	}
	directory, err := os.MkdirTemp("", "latticedb-backup-replay-*")
	if err != nil {
		return nil, 0, 0, 0, err
	}
	defer os.RemoveAll(directory)
	files := DirectoryDatabaseFiles(directory)
	if err := CheckpointGraphStateAndWALFilesContext(ctx, files, baseGraph, baseNextNodeID, baseNextEdgeID, baseCommitID); err != nil {
		return nil, 0, 0, 0, err
	}
	wal, err := os.OpenFile(files.WAL, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	writeErr := appendBackupFiles(ctx, wal, segmentPaths)
	if writeErr == nil {
		writeErr = wal.Sync()
	}
	if closeErr := wal.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return nil, 0, 0, 0, writeErr
	}
	return LoadGraphStateFilesContext(ctx, files, maxSnapshotBytes, ^uint64(0), ^uint64(0))
}

// ValidateBackupWALChain checks that an archived base and its linked frames
// recover natively through the advertised endpoint before restart rebasing.
func ValidateBackupWALChain(basePath string, segmentPaths []string, commitID, maxSnapshotBytes uint64) error {
	_, _, _, actualCommit, err := replayBackupWALFiles(context.Background(), basePath, segmentPaths, maxSnapshotBytes)
	if err == nil && actualCommit != commitID {
		return errors.New("backup WAL chain ends at unexpected commit")
	}
	return err
}

// ValidateBackupWALFiles validates a persisted archive point against the
// recovered source when reopening at the same commit. It is not used on the
// per-commit path and does not claim ancestry for a missing WAL range.
func ValidateBackupWALFiles(basePath string, segmentPaths []string, expected *GraphState, sourceNextNodeID, sourceNextEdgeID, commitID, maxSnapshotBytes uint64) error {
	replayed, replayNextNodeID, replayNextEdgeID, replayedCommit, err := replayBackupWALFiles(context.Background(), basePath, segmentPaths, maxSnapshotBytes)
	if err != nil {
		return err
	}
	if replayedCommit != commitID || replayed.DatabaseID != expected.DatabaseID || replayNextNodeID > sourceNextNodeID || replayNextEdgeID > sourceNextEdgeID {
		return errors.New("backup archive counters or commit do not match recovered source")
	}
	actual, err := SerializeGraphState(replayed, replayNextNodeID, replayNextEdgeID, commitID)
	if err != nil {
		return err
	}
	want, err := SerializeGraphState(expected, replayNextNodeID, replayNextEdgeID, commitID)
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, want) {
		return errors.New("backup archive state differs from recovered source")
	}
	return nil
}

// CreateBackupRecoveryFilesFrom restores a base and segments as native
// checkpoint+WAL files for normal recovery on the next Open.
func CreateBackupRecoveryFilesFrom(ctx context.Context, files DatabaseFiles, basePath string, segmentPaths []string, maxSnapshotBytes, expectedCommitID uint64) error {
	graph, nextNodeID, nextEdgeID, commitID, err := replayBackupWALFiles(ctx, basePath, segmentPaths, maxSnapshotBytes)
	if err == nil && commitID != expectedCommitID {
		err = errors.New("restored backup WAL ends at unexpected commit")
	}
	if err != nil {
		return err
	}
	return checkpointGraphStateFilesContext(ctx, files, graph, nextNodeID, nextEdgeID, commitID, true)
}
