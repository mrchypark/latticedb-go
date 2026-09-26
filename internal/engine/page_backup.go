package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// capturePage uses the committed page catalog and outbox as the page backend's
// ancestry proof. It never replays the archive into a native in-memory graph.
func (archive *backupArchive) capturePage(ctx context.Context, now time.Time, graph *store.GraphState, nextNodeID, nextEdgeID, commitID uint64) (BackupMetadata, error) {
	if archive == nil {
		return BackupMetadata{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return BackupMetadata{}, err
	}
	if graph == nil || graph.PageBase == nil {
		return BackupMetadata{}, errors.New("page backup requires a page-backed graph")
	}
	page := graph.PageBase
	catalog, err := page.Catalog()
	if err != nil {
		return BackupMetadata{}, err
	}
	if catalog.DatabaseID == "" || catalog.DatabaseID != graph.DatabaseID || catalog.CommitID != commitID {
		return BackupMetadata{}, errors.New("page backup catalog identity mismatch")
	}
	identity := backupSourceHistory{databaseID: catalog.DatabaseID, history: catalog.History}
	currentHistory, err := pageCommitHistory(page, commitID)
	if err != nil {
		return BackupMetadata{}, err
	}
	if currentHistory != catalog.History {
		return BackupMetadata{}, errors.New("page catalog history does not match commit history")
	}
	if archive.headPath == "" {
		metadata, err := archive.publishBase(now, graph, nextNodeID, nextEdgeID, commitID, identity)
		if err == nil {
			archive.ready = true
		}
		return metadata, err
	}
	if !archive.ready {
		base, _, err := archive.chainFor(backupEntry{path: archive.headPath})
		if err != nil {
			return BackupMetadata{}, err
		}
		if err := validatePageArchiveFile(ctx, base.path, base.metadata, catalog.DatabaseID); err != nil {
			return BackupMetadata{}, fmt.Errorf("backup archive base is invalid: %w", err)
		}
	}
	if commitID < archive.head.CommitID {
		return BackupMetadata{}, fmt.Errorf("backup archive tip %d is ahead of recovered commit %d", archive.head.CommitID, commitID)
	}
	if !archive.hasHeadSourceHistory {
		return BackupMetadata{}, errors.New("backup archive head has no page source history proof")
	}
	archivedSourceHistory, err := pageCommitHistory(page, archive.head.CommitID)
	if err != nil {
		return BackupMetadata{}, err
	}
	if archivedSourceHistory != archive.headSourceHistory {
		return BackupMetadata{}, errors.New("backup archive source ancestry differs from page database")
	}
	if commitID == archive.head.CommitID {
		if catalog.History != archive.headSourceHistory {
			return BackupMetadata{}, errors.New("backup archive source ancestry differs at the current commit")
		}
		if err := archive.anchorPageHead(); err != nil {
			return BackupMetadata{}, err
		}
		archive.ready = true
		return archive.head, nil
	}
	if commitID != archive.head.CommitID+1 {
		metadata, err := archive.publishBase(now, graph, nextNodeID, nextEdgeID, commitID, identity)
		if err == nil {
			archive.ready = true
		}
		return metadata, err
	}
	frame, err := pageOutboxFrame(page, commitID)
	if err != nil {
		return BackupMetadata{}, err
	}
	if frame == nil {
		metadata, err := archive.publishBase(now, graph, nextNodeID, nextEdgeID, commitID, identity)
		if err == nil {
			archive.ready = true
		}
		return metadata, err
	}
	history := extendPageHistory(archive.headSourceHistory, frame)
	if history != catalog.History {
		return BackupMetadata{}, errors.New("page archive outbox does not extend the archived ancestry")
	}
	if historyAtCommit, err := pageCommitHistory(page, commitID); err != nil {
		return BackupMetadata{}, err
	} else if historyAtCommit != history {
		return BackupMetadata{}, errors.New("page outbox history does not match commit history")
	}
	metadata, err := archive.publishPageSegment(ctx, now, commitID, frame, backupSourceHistory{databaseID: catalog.DatabaseID, history: history})
	if err != nil {
		return BackupMetadata{}, err
	}
	archive.ready = true
	return metadata, nil
}

func validatePageArchiveFile(ctx context.Context, path string, metadata BackupMetadata, databaseID string) error {
	maxInt64 := int64(^uint64(0) >> 1)
	return validateArchiveFileWithLimits(ctx, path, metadata, databaseID, maxInt64, store.CheckpointScanLimits{
		MaxPayloadBytes: uint64(maxInt64),
		MaxEntries:      ^uint64(0),
		MaxRecordBytes:  65 << 20,
	})
}

func pageCommitKey(commitID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, commitID)
	return key
}

func pageCommitHistory(page *store.PageGraph, commitID uint64) ([sha256.Size]byte, error) {
	var history [sha256.Size]byte
	data, err := page.Tx.Get("commit-history", pageCommitKey(commitID))
	if err != nil {
		return history, err
	}
	if len(data) != len(history) {
		return history, fmt.Errorf("page commit history %d is missing or invalid", commitID)
	}
	copy(history[:], data)
	return history, nil
}

func pageOutboxFrame(page *store.PageGraph, commitID uint64) ([]byte, error) {
	return page.Tx.Get("archive-outbox", pageCommitKey(commitID))
}

func extendPageHistory(previous [sha256.Size]byte, frame []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write(previous[:])
	_, _ = hash.Write(frame)
	var history [sha256.Size]byte
	copy(history[:], hash.Sum(nil))
	return history
}

func (archive *backupArchive) publishPageSegment(ctx context.Context, now time.Time, commitID uint64, frame []byte, identity backupSourceHistory) (BackupMetadata, error) {
	if err := ctx.Err(); err != nil {
		return BackupMetadata{}, err
	}
	if uint64(len(frame)) > maxBackupSegmentBytes {
		return BackupMetadata{}, fmt.Errorf("%w: backup WAL segment exceeds size limit", ErrResourceLimit)
	}
	stage, err := os.CreateTemp(archive.directory, ".latticedb-backup-page-range-*")
	if err != nil {
		return BackupMetadata{}, err
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	written, writeErr := stage.Write(frame)
	if writeErr == nil && written != len(frame) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = stage.Sync()
	}
	if closeErr := stage.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return BackupMetadata{}, writeErr
	}
	if err := store.ValidateBackupWALSegmentFile(stagePath, identity.databaseID, commitID-1, commitID); err != nil {
		return BackupMetadata{}, fmt.Errorf("backup WAL commit frame is invalid: %w", err)
	}
	captured, err := archive.captureTime(now)
	if err != nil {
		return BackupMetadata{}, err
	}
	metadata := BackupMetadata{CommitID: commitID, CapturedAt: captured}
	entry, err := publishStagedSegment(archive.directory, stagePath, commitID, metadata, archive.headDigest, identity)
	if err != nil {
		return BackupMetadata{}, err
	}
	archive.last, archive.head, archive.headPath, archive.headDigest = captured, metadata, entry.path, entry.digest
	archive.headSourceHistory, archive.hasHeadSourceHistory = entry.sourceHistory, entry.hasSourceHistory
	return metadata, nil
}

func (archive *backupArchive) anchorPageHead() error {
	if archive.headPath == "" || !archive.hasHeadSourceHistory {
		return errors.New("page backup head has no source history proof")
	}
	path := filepath.Join(archive.directory, backupHeadFile)
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 64<<10 {
			return errors.New("invalid backup archive head anchor")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var current backupHead
		if err := json.Unmarshal(data, &current); err != nil {
			return errors.New("invalid backup archive head anchor")
		}
		if current.CommitID == archive.head.CommitID && current.Entry == filepath.Base(archive.headPath) && current.Digest == hex.EncodeToString(archive.headDigest[:]) {
			return nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entry := backupEntry{
		metadata:         archive.head,
		digest:           archive.headDigest,
		sourceHistory:    archive.headSourceHistory,
		hasSourceHistory: true,
		path:             archive.headPath,
	}
	return writeArchiveHead(archive.directory, entry)
}
