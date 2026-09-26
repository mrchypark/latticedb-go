package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

const backupOwnerFile = ".latticedb-backup-owner.json"
const backupLockFile = ".latticedb-backup.lock"
const backupPrefix = "commit-"
const backupSuffix = ".ltdb"
const archiveStateHeaderBytes = 64

type BackupRestoreOptions struct {
	CommitID                 *uint64
	Before                   time.Time
	MaxDatabaseSnapshotBytes uint64
}

type BackupMetadata struct {
	CommitID   uint64
	CapturedAt time.Time
}

type backupArchive struct {
	directory string
	lock      *os.File
	last      time.Time
	head      BackupMetadata
	headPath  string
	ready     bool
}

type backupOwner struct {
	DatabaseID string `json:"database_id"`
	Source     string `json:"source"`
}

func backupMetadataChecksum(metadata BackupMetadata, contentDigest [sha256.Size]byte) [sha256.Size]byte {
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[:8], metadata.CommitID)
	binary.BigEndian.PutUint64(encoded[8:], uint64(metadata.CapturedAt.UnixNano()))
	hash := sha256.New()
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write(contentDigest[:])
	var checksum [sha256.Size]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

func backupFilename(metadata BackupMetadata, contentDigest [sha256.Size]byte) string {
	metadataChecksum := backupMetadataChecksum(metadata, contentDigest)
	return backupPrefix + fmt.Sprintf("%020d-%020d-%x-%x", metadata.CommitID, metadata.CapturedAt.UnixNano(), contentDigest, metadataChecksum) + backupSuffix
}

func parseBackupEntry(name string) (BackupMetadata, [sha256.Size]byte, error) {
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, backupPrefix), backupSuffix), "-")
	if len(parts) != 4 || len(parts[2]) != sha256.Size*2 || len(parts[3]) != sha256.Size*2 {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	commit, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	nanos, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	contentDigestBytes, err := hex.DecodeString(parts[2])
	if err != nil {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	metadataChecksumBytes, err := hex.DecodeString(parts[3])
	if err != nil {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	var contentDigest, metadataChecksum [sha256.Size]byte
	copy(contentDigest[:], contentDigestBytes)
	copy(metadataChecksum[:], metadataChecksumBytes)
	metadata := BackupMetadata{CommitID: commit, CapturedAt: time.Unix(0, nanos)}
	if metadataChecksum != backupMetadataChecksum(metadata, contentDigest) {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	return metadata, contentDigest, nil
}

func writeArchiveCheckpoint(path string, data []byte) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".latticedb-backup-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	_, err = temp.Write(data)
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Link(tempPath, path); err != nil {
		return err
	}
	cleanup := func(cause error) error {
		removeErr := os.Remove(path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		return errors.Join(cause, removeErr, syncPathDirectory(filepath.Dir(path)))
	}
	if err := syncPathDirectory(filepath.Dir(path)); err != nil {
		return cleanup(err)
	}
	if err := os.Remove(tempPath); err != nil {
		return cleanup(err)
	}
	if err := syncPathDirectory(filepath.Dir(path)); err != nil {
		return cleanup(err)
	}
	return nil
}

func archiveFileSizeLimit(maxSnapshotBytes uint64) (int64, error) {
	if maxSnapshotBytes > uint64(^uint64(0))-archiveStateHeaderBytes || maxSnapshotBytes+archiveStateHeaderBytes >= uint64(^uint(0)>>1) {
		return 0, errors.New("backup archive snapshot size limit is too large")
	}
	return int64(maxSnapshotBytes + archiveStateHeaderBytes), nil
}

func validateArchiveFile(ctx context.Context, path string, metadata BackupMetadata, maxSnapshotBytes uint64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("backup archive checkpoint is not a regular file")
	}
	limit, err := archiveFileSizeLimit(maxSnapshotBytes)
	if err != nil {
		return nil, err
	}
	if info.Size() < archiveStateHeaderBytes || info.Size() > limit {
		return nil, fmt.Errorf("%w: backup archive checkpoint exceeds snapshot size limit", ErrResourceLimit)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var dataBuffer bytes.Buffer
	hash := sha256.New()
	reader := io.LimitReader(file, limit+1)
	chunk := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, readErr := reader.Read(chunk)
		if int64(dataBuffer.Len())+int64(n) > limit {
			return nil, fmt.Errorf("%w: backup archive checkpoint exceeds snapshot size limit", ErrResourceLimit)
		}
		_, _ = dataBuffer.Write(chunk[:n])
		_, _ = hash.Write(chunk[:n])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	data := dataBuffer.Bytes()
	var contentDigest [sha256.Size]byte
	copy(contentDigest[:], hash.Sum(nil))
	if filepath.Base(path) != backupFilename(metadata, contentDigest) {
		return nil, errors.New("backup archive filename checksum mismatch")
	}
	return data, nil
}

// Read directory entries in bounded batches: archive size need not fit in memory.
func walkBackupEntries(ctx context.Context, directory string, visit func(BackupMetadata, string)) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := file.ReadDir(128)
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("backup archive contains symlink %q", entry.Name())
			}
			if !strings.HasPrefix(entry.Name(), backupPrefix) || !strings.HasSuffix(entry.Name(), backupSuffix) {
				continue
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("invalid backup archive entry %q", entry.Name())
			}
			metadata, _, parseErr := parseBackupEntry(entry.Name())
			if parseErr != nil {
				return parseErr
			}
			visit(metadata, filepath.Join(directory, entry.Name()))
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func openBackupArchive(directory, source, databaseID string) (*backupArchive, error) {
	archive, err := canonicalSnapshotPath(directory)
	if err != nil {
		return nil, err
	}
	source, err = canonicalSnapshotPath(source)
	if err != nil {
		return nil, err
	}
	if archive == source || strings.HasPrefix(archive, source+string(os.PathSeparator)) {
		return nil, fmt.Errorf("%w: backup directory belongs to the source database", ErrInvalidArgument)
	}
	if err := os.MkdirAll(archive, 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(archive, backupLockFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err == nil {
		err = tryLockFile(lock, false)
	}
	if err != nil {
		if lock != nil {
			_ = lock.Close()
		}
		return nil, fmt.Errorf("%w: backup archive: %v", ErrDatabaseLocked, err)
	}
	ownerPath := filepath.Join(archive, backupOwnerFile)
	owner := backupOwner{DatabaseID: databaseID, Source: source}
	data, err := json.Marshal(owner)
	if err != nil {
		_ = unlockFile(lock)
		_ = lock.Close()
		return nil, err
	}
	file, err := os.OpenFile(ownerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		_, err = file.Write(data)
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = syncPathDirectory(archive)
		}
	} else if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(ownerPath)
		if statErr != nil {
			err = statErr
		} else if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 64<<10 {
			err = errors.New("invalid backup archive owner metadata")
		} else {
			data, err = os.ReadFile(ownerPath)
		}
		if err == nil {
			var existing backupOwner
			err = json.Unmarshal(data, &existing)
			if err == nil && existing != owner {
				err = fmt.Errorf("%w: backup archive belongs to another database source", ErrInvalidArgument)
			}
		}
	}
	if err != nil {
		_ = unlockFile(lock)
		_ = lock.Close()
		return nil, err
	}
	for directory := archive; ; directory = filepath.Dir(directory) {
		if err := syncPathDirectory(directory); err != nil {
			_ = unlockFile(lock)
			_ = lock.Close()
			return nil, err
		}
		if filepath.Dir(directory) == directory {
			break
		}
	}
	result := &backupArchive{directory: archive, lock: lock}
	err = walkBackupEntries(context.Background(), archive, func(metadata BackupMetadata, path string) {
		if metadata.CapturedAt.After(result.last) {
			result.last = metadata.CapturedAt
		}
		if result.headPath == "" || metadata.CommitID > result.head.CommitID || metadata.CommitID == result.head.CommitID && metadata.CapturedAt.After(result.head.CapturedAt) {
			result.head, result.headPath = metadata, path
		}
	})
	if err != nil {
		_ = result.close()
		return nil, err
	}
	return result, nil
}

func (archive *backupArchive) close() error {
	if archive == nil || archive.lock == nil {
		return nil
	}
	err := unlockFile(archive.lock)
	closeErr := archive.lock.Close()
	archive.lock = nil
	return errors.Join(err, closeErr)
}

func (archive *backupArchive) captureAt(now time.Time, graph *store.GraphState, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes uint64) (BackupMetadata, error) {
	if archive == nil {
		return BackupMetadata{}, nil
	}
	if !archive.ready {
		if err := archive.initialize(graph, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes); err != nil {
			return BackupMetadata{}, err
		}
	}
	if archive.headPath != "" {
		if commitID == archive.head.CommitID {
			return archive.head, nil
		}
		if commitID != archive.head.CommitID+1 {
			return BackupMetadata{}, fmt.Errorf("backup archive cannot prove commit %d follows archived commit %d", commitID, archive.head.CommitID)
		}
	}
	data, err := store.SerializeGraphState(graph, nextNodeID, nextEdgeID, commitID)
	if err != nil {
		return BackupMetadata{}, err
	}
	// ponytail: one native full checkpoint per durable commit is the explicit
	// archive ceiling; add incremental shipping only with measured pressure.
	captured := now
	if captured.Before(time.Unix(0, 0)) {
		captured = time.Unix(0, 0)
	}
	if !captured.After(archive.last) {
		captured = archive.last.Add(time.Nanosecond)
	}
	if captured.After(time.Unix(0, math.MaxInt64)) {
		return BackupMetadata{}, errors.New("backup capture timestamp is out of range")
	}
	metadata := BackupMetadata{CommitID: commitID, CapturedAt: captured}
	path := filepath.Join(archive.directory, backupFilename(metadata, sha256.Sum256(data)))
	if err := writeArchiveCheckpoint(path, data); err != nil {
		return BackupMetadata{}, err
	}
	archive.last, archive.head, archive.headPath = captured, metadata, path
	return metadata, nil
}

// ponytail: only an identical recovered head may resume this archive. Gaps
// require a new archive; add a persistent lineage anchor if gap resumption is needed.
func (archive *backupArchive) initialize(graph *store.GraphState, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes uint64) error {
	if archive.headPath == "" {
		archive.ready = true
		return nil
	}
	if archive.head.CommitID != commitID {
		return fmt.Errorf("backup archive tip %d does not match recovered commit %d", archive.head.CommitID, commitID)
	}
	actual, err := validateArchiveFile(context.Background(), archive.headPath, archive.head, maxSnapshotBytes)
	if err != nil {
		return err
	}
	archivedGraph, archivedNextNodeID, archivedNextEdgeID, archivedCommitID, err := store.DeserializeGraphState(actual, maxSnapshotBytes, defaultDerivedBuildMaxWork, defaultDerivedBuildMaxLogicalBytes)
	if err != nil {
		return err
	}
	if archivedCommitID != commitID || nextNodeID < archivedNextNodeID || nextEdgeID < archivedNextEdgeID {
		return fmt.Errorf("backup archive commit %d does not match recovered counters", commitID)
	}
	expected, err := store.SerializeGraphState(graph, archivedNextNodeID, archivedNextEdgeID, commitID)
	if err != nil {
		return err
	}
	if archivedGraph.DatabaseID != graph.DatabaseID || !bytes.Equal(actual, expected) {
		return fmt.Errorf("backup archive commit %d does not match the recovered generation", commitID)
	}
	archive.ready = true
	return nil
}

func RestoreBackup(ctx context.Context, directory, destination string, opts BackupRestoreOptions) (BackupMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return BackupMetadata{}, err
	}
	if opts.CommitID != nil && !opts.Before.IsZero() {
		return BackupMetadata{}, fmt.Errorf("%w: CommitID and Before are mutually exclusive", ErrInvalidArgument)
	}
	archive, err := canonicalSnapshotPath(directory)
	if err != nil {
		return BackupMetadata{}, err
	}
	destination, err = canonicalSnapshotPath(destination)
	if err != nil {
		return BackupMetadata{}, err
	}
	if archive == destination || strings.HasPrefix(destination, archive+string(os.PathSeparator)) {
		return BackupMetadata{}, fmt.Errorf("%w: restore destination belongs to the archive", ErrInvalidArgument)
	}
	maxBytes := opts.MaxDatabaseSnapshotBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxDatabaseSnapshotBytes
	}
	var selected BackupMetadata
	var selectedPath string
	err = walkBackupEntries(ctx, archive, func(meta BackupMetadata, path string) {
		if opts.CommitID != nil && meta.CommitID != *opts.CommitID || !opts.Before.IsZero() && meta.CapturedAt.After(opts.Before) {
			return
		}
		if selectedPath == "" || meta.CommitID > selected.CommitID || meta.CommitID == selected.CommitID && meta.CapturedAt.After(selected.CapturedAt) {
			selected, selectedPath = meta, path
		}
	})
	if err != nil {
		return BackupMetadata{}, err
	}
	if selectedPath == "" {
		return BackupMetadata{}, os.ErrNotExist
	}
	if _, err := validateArchiveFile(ctx, selectedPath, selected, maxBytes); err != nil {
		return BackupMetadata{}, err
	}
	for _, path := range []string{destination, destination + "-wal", destination + "-wal.base", destination + "-ids", destination + ".layout"} {
		if _, err := os.Lstat(path); err == nil {
			return BackupMetadata{}, fmt.Errorf("%w: restore destination already exists", ErrInvalidArgument)
		} else if !errors.Is(err, os.ErrNotExist) {
			return BackupMetadata{}, err
		}
	}
	graph, nextNodeID, nextEdgeID, commitID, err := store.LoadGraphStateFilesContext(ctx, store.FlatDatabaseFiles(selectedPath), maxBytes, defaultDerivedBuildMaxWork, defaultDerivedBuildMaxLogicalBytes)
	if err != nil {
		return BackupMetadata{}, err
	}
	if commitID != selected.CommitID {
		return BackupMetadata{}, errors.New("backup archive commit metadata mismatch")
	}
	ownerPath := filepath.Join(archive, backupOwnerFile)
	ownerInfo, err := os.Lstat(ownerPath)
	if err != nil {
		return BackupMetadata{}, err
	}
	if ownerInfo.Mode()&os.ModeSymlink != 0 || !ownerInfo.Mode().IsRegular() || ownerInfo.Size() > 64<<10 {
		return BackupMetadata{}, errors.New("invalid backup archive owner metadata")
	}
	ownerData, err := os.ReadFile(ownerPath)
	if err != nil {
		return BackupMetadata{}, err
	}
	var owner backupOwner
	if err := json.Unmarshal(ownerData, &owner); err != nil || owner.DatabaseID != graph.DatabaseID {
		return BackupMetadata{}, errors.New("backup archive owner does not match checkpoint database")
	}
	if err := ctx.Err(); err != nil {
		return BackupMetadata{}, err
	}
	if err := store.CreateCheckpointGraphStateFiles(store.FlatDatabaseFiles(destination), graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return BackupMetadata{}, err
	}
	return selected, nil
}
