package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

const backupOwnerFile = ".latticedb-backup-owner.json"
const backupLockFile = ".latticedb-backup.lock"
const backupPrefix = "commit-"
const backupSuffix = ".ltdb"
const backupSegmentSuffix = ".wal"
const backupHeadFile = ".latticedb-backup-head.json"
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
	directory            string
	lock                 *os.File
	last                 time.Time
	head                 BackupMetadata
	headPath             string
	ready                bool
	entries              []backupEntry
	headDigest           [sha256.Size]byte
	headSourceHistory    [sha256.Size]byte
	hasHeadSourceHistory bool
	rebaseOnOpen         bool
}

type backupEntry struct {
	metadata         BackupMetadata
	start            uint64
	digest           [sha256.Size]byte
	previous         [sha256.Size]byte
	sourceHistory    [sha256.Size]byte
	hasSourceHistory bool
	path             string
	segment          bool
	gapStart         time.Time
}

type backupSourceHistory struct {
	databaseID string
	history    [sha256.Size]byte
}

type backupSourceHistoryProof struct {
	DatabaseID string `json:"database_id"`
	Entry      string `json:"entry"`
	CommitID   uint64 `json:"commit_id"`
	Digest     string `json:"digest"`
	History    string `json:"history"`
}

const backupSourceHistoryPrefix = ".latticedb-backup-source-history-"

type backupHead struct {
	CommitID uint64 `json:"commit_id"`
	Entry    string `json:"entry"`
	Digest   string `json:"digest"`
}

type backupOwner struct {
	DatabaseID string `json:"database_id"`
	Source     string `json:"source"`
}

func backupMetadataChecksum(metadata BackupMetadata, contentDigest [sha256.Size]byte, gapStart ...time.Time) [sha256.Size]byte {
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[:8], metadata.CommitID)
	binary.BigEndian.PutUint64(encoded[8:], uint64(metadata.CapturedAt.UnixNano()))
	hash := sha256.New()
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write(contentDigest[:])
	if len(gapStart) != 0 && !gapStart[0].IsZero() {
		_, _ = hash.Write([]byte("coverage-gap"))
		binary.BigEndian.PutUint64(encoded[:8], uint64(gapStart[0].UnixNano()))
		_, _ = hash.Write(encoded[:8])
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

func backupSegmentChecksum(start uint64, metadata BackupMetadata, contentDigest, previous [sha256.Size]byte) [sha256.Size]byte {
	var encoded [24]byte
	binary.BigEndian.PutUint64(encoded[:8], start)
	binary.BigEndian.PutUint64(encoded[8:16], metadata.CommitID)
	binary.BigEndian.PutUint64(encoded[16:], uint64(metadata.CapturedAt.UnixNano()))
	hash := sha256.New()
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write(contentDigest[:])
	_, _ = hash.Write(previous[:])
	var checksum [sha256.Size]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

func backupFilename(metadata BackupMetadata, contentDigest [sha256.Size]byte, gapStart ...time.Time) string {
	checksum := backupMetadataChecksum(metadata, contentDigest, gapStart...)
	name := backupPrefix + fmt.Sprintf("%020d-%020d-%x-%x", metadata.CommitID, metadata.CapturedAt.UnixNano(), contentDigest, checksum)
	if len(gapStart) != 0 && !gapStart[0].IsZero() {
		name += fmt.Sprintf("-%020d", gapStart[0].UnixNano())
	}
	return name + backupSuffix
}

func backupGapStart(name string) (time.Time, error) {
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, backupPrefix), backupSuffix), "-")
	if len(parts) == 4 {
		return time.Time{}, nil
	}
	if len(parts) != 5 {
		return time.Time{}, errors.New("invalid backup coverage gap")
	}
	nanos, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil || nanos < 0 {
		return time.Time{}, errors.New("invalid backup coverage gap timestamp")
	}
	return time.Unix(0, nanos), nil
}

func backupSegmentFilename(start uint64, metadata BackupMetadata, contentDigest, previous [sha256.Size]byte) string {
	checksum := backupSegmentChecksum(start, metadata, contentDigest, previous)
	encode := base64.RawURLEncoding.EncodeToString
	return backupPrefix + fmt.Sprintf("%020d.%020d.%020d.%s.%s.%s", start, metadata.CommitID, metadata.CapturedAt.UnixNano(), encode(contentDigest[:]), encode(previous[:]), encode(checksum[:])) + backupSegmentSuffix
}

func parseBackupEntry(name string) (BackupMetadata, [sha256.Size]byte, error) {
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, backupPrefix), backupSuffix), "-")
	if (len(parts) != 4 && len(parts) != 5) || len(parts[2]) != sha256.Size*2 || len(parts[3]) != sha256.Size*2 {
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
	gapStart, gapErr := backupGapStart(name)
	if gapErr != nil || !gapStart.IsZero() && !gapStart.Before(metadata.CapturedAt) || metadataChecksum != backupMetadataChecksum(metadata, contentDigest, gapStart) {
		return BackupMetadata{}, [sha256.Size]byte{}, fmt.Errorf("invalid backup archive entry %q", name)
	}
	return metadata, contentDigest, nil
}

func parseBackupSegment(name string) (backupEntry, error) {
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(name, backupPrefix), backupSegmentSuffix), ".")
	if len(parts) != 6 {
		return backupEntry{}, fmt.Errorf("invalid backup segment entry %q", name)
	}
	start, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || start == 0 {
		return backupEntry{}, fmt.Errorf("invalid backup segment entry %q", name)
	}
	commit, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || commit < start {
		return backupEntry{}, fmt.Errorf("invalid backup segment entry %q", name)
	}
	nanos, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return backupEntry{}, fmt.Errorf("invalid backup segment entry %q", name)
	}
	decode := func(value string) ([sha256.Size]byte, error) {
		var result [sha256.Size]byte
		decoded, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size {
			return result, fmt.Errorf("invalid backup segment entry %q", name)
		}
		copy(result[:], decoded)
		return result, nil
	}
	digest, err := decode(parts[3])
	if err != nil {
		return backupEntry{}, err
	}
	previous, err := decode(parts[4])
	if err != nil {
		return backupEntry{}, err
	}
	checksum, err := decode(parts[5])
	if err != nil {
		return backupEntry{}, err
	}
	metadata := BackupMetadata{CommitID: commit, CapturedAt: time.Unix(0, nanos)}
	if checksum != backupSegmentChecksum(start, metadata, digest, previous) {
		return backupEntry{}, fmt.Errorf("invalid backup segment entry %q", name)
	}
	return backupEntry{metadata: metadata, start: start, digest: digest, previous: previous, segment: true}, nil
}

func sourceHistoryProofPath(directory, entry string) string {
	key := sha256.Sum256([]byte(entry))
	return filepath.Join(directory, backupSourceHistoryPrefix+hex.EncodeToString(key[:])+".json")
}

func sourceHistoryProofData(entry backupEntry, identity backupSourceHistory) ([]byte, error) {
	if identity.databaseID == "" {
		return nil, errors.New("empty backup source database identity")
	}
	return json.Marshal(backupSourceHistoryProof{
		DatabaseID: identity.databaseID,
		Entry:      filepath.Base(entry.path),
		CommitID:   entry.metadata.CommitID,
		Digest:     hex.EncodeToString(entry.digest[:]),
		History:    hex.EncodeToString(identity.history[:]),
	})
}

// writeSourceHistoryProof fsyncs an immutable identity record before its entry
// is linked into the archive, so a visible entry can never lack its ancestry.
func writeSourceHistoryProof(directory string, entry backupEntry, identity backupSourceHistory) error {
	data, err := sourceHistoryProofData(entry, identity)
	if err != nil {
		return err
	}
	path := sourceHistoryProofPath(directory, filepath.Base(entry.path))
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 4096 {
			return errors.New("invalid backup source history proof")
		}
		old, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(old, data) {
			return errors.New("backup entry has conflicting source history")
		}
		return syncPathDirectory(directory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temp, err := os.CreateTemp(directory, ".latticedb-backup-history-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	written, writeErr := temp.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = temp.Sync()
	}
	if closeErr := temp.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return writeErr
	}
	if err := os.Link(tempPath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		old, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(old, data) {
			return errors.Join(errors.New("backup entry has conflicting source history"), readErr)
		}
	}
	return syncPathDirectory(directory)
}

func readSourceHistoryProof(entry backupEntry, databaseID string) ([sha256.Size]byte, bool, error) {
	var history [sha256.Size]byte
	path := sourceHistoryProofPath(filepath.Dir(entry.path), filepath.Base(entry.path))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return history, false, nil
	}
	if err != nil {
		return history, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 4096 {
		return history, false, errors.New("invalid backup source history proof")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return history, false, err
	}
	var proof backupSourceHistoryProof
	if err := json.Unmarshal(data, &proof); err != nil {
		return history, false, errors.New("invalid backup source history proof")
	}
	digest, digestErr := hex.DecodeString(proof.Digest)
	decodedHistory, historyErr := hex.DecodeString(proof.History)
	if proof.DatabaseID != databaseID || proof.Entry != filepath.Base(entry.path) || proof.CommitID != entry.metadata.CommitID || digestErr != nil || len(digest) != sha256.Size || !bytes.Equal(digest, entry.digest[:]) || historyErr != nil || len(decodedHistory) != sha256.Size {
		return history, false, errors.New("backup source history proof does not match its entry")
	}
	copy(history[:], decodedHistory)
	return history, true, nil
}

func writeArchiveHead(directory string, entry backupEntry) error {
	data, err := json.Marshal(backupHead{CommitID: entry.metadata.CommitID, Entry: filepath.Base(entry.path), Digest: hex.EncodeToString(entry.digest[:])})
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".latticedb-backup-head-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	headPath := filepath.Join(directory, backupHeadFile)
	if info, statErr := os.Lstat(headPath); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			_ = temp.Close()
			return errors.New("backup archive head is not a regular file")
		}
		oldData, readErr := os.ReadFile(headPath)
		var old backupHead
		if readErr != nil || json.Unmarshal(oldData, &old) != nil || old.Entry == "" || filepath.Base(old.Entry) != old.Entry || old.CommitID >= entry.metadata.CommitID {
			_ = temp.Close()
			return errors.Join(errors.New("refusing to replace an unrecognized backup archive head"), readErr)
		}
		oldEntryPath := filepath.Join(directory, old.Entry)
		oldInfo, oldStatErr := os.Lstat(oldEntryPath)
		oldDigest, decodeErr := hex.DecodeString(old.Digest)
		actualDigest, hashErr := hashArchiveFile(oldEntryPath)
		entryMetadata, entryDigest, parseErr := parseBackupEntry(old.Entry)
		if strings.HasSuffix(old.Entry, backupSegmentSuffix) {
			parsed, err := parseBackupSegment(old.Entry)
			parseErr = err
			entryMetadata, entryDigest = parsed.metadata, parsed.digest
		}
		if oldStatErr != nil || oldInfo.Mode()&os.ModeSymlink != 0 || !oldInfo.Mode().IsRegular() || decodeErr != nil || len(oldDigest) != sha256.Size || hashErr != nil || parseErr != nil || entryMetadata.CommitID != old.CommitID || entryDigest != actualDigest || !bytes.Equal(oldDigest, actualDigest[:]) {
			_ = temp.Close()
			return errors.Join(errors.New("refusing to replace a backup head with a missing or invalid entry"), oldStatErr, hashErr, parseErr)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		_ = temp.Close()
		return statErr
	}
	written, writeErr := temp.Write(data)
	if writeErr == nil && written != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = temp.Sync()
	}
	if closeErr := temp.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return writeErr
	}
	if err := os.Rename(name, filepath.Join(directory, backupHeadFile)); err != nil {
		return err
	}
	return syncPathDirectory(directory)
}

func publishArchiveEntry(directory string, metadata BackupMetadata, start uint64, previous [sha256.Size]byte, segment bool, gapStart time.Time, write func(io.Writer) error, sourceHistory ...backupSourceHistory) (backupEntry, error) {
	temp, err := os.CreateTemp(directory, ".latticedb-backup-entry-*")
	if err != nil {
		return backupEntry{}, err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	hash := sha256.New()
	if err := write(io.MultiWriter(temp, hash)); err != nil {
		_ = temp.Close()
		return backupEntry{}, err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return backupEntry{}, err
	}
	if err := temp.Close(); err != nil {
		return backupEntry{}, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	name := backupFilename(metadata, digest, gapStart)
	if segment {
		name = backupSegmentFilename(start, metadata, digest, previous)
	}
	path := filepath.Join(directory, name)
	entry := backupEntry{metadata: metadata, start: start, digest: digest, previous: previous, path: path, segment: segment}
	if len(sourceHistory) > 1 {
		return backupEntry{}, errors.New("multiple backup source histories")
	}
	if len(sourceHistory) == 1 {
		entry.sourceHistory = sourceHistory[0].history
		entry.hasSourceHistory = true
		if err := writeSourceHistoryProof(directory, entry, sourceHistory[0]); err != nil {
			return backupEntry{}, err
		}
	}
	if err := os.Link(tempPath, path); err != nil && !errors.Is(err, os.ErrExist) {
		return backupEntry{}, err
	} else if errors.Is(err, os.ErrExist) {
		actual, readErr := hashArchiveFile(path)
		if readErr != nil || actual != digest {
			return backupEntry{}, errors.Join(errors.New("backup entry collision"), readErr)
		}
	}
	if err := syncPathDirectory(directory); err != nil {
		return backupEntry{}, err
	}
	if err := writeArchiveHead(directory, entry); err != nil {
		return backupEntry{}, err
	}
	return entry, nil
}

func hashArchiveFile(path string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	file, err := os.Open(path)
	if err != nil {
		return digest, err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return digest, err
	}
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func publishStagedSegment(directory, stagedPath string, start uint64, metadata BackupMetadata, previous [sha256.Size]byte, sourceHistory ...backupSourceHistory) (backupEntry, error) {
	info, err := os.Stat(stagedPath)
	if err != nil {
		return backupEntry{}, err
	}
	if info.Size() <= 0 || info.Size() > maxBackupSegmentBytes {
		return backupEntry{}, fmt.Errorf("%w: backup WAL segment size is out of range", ErrResourceLimit)
	}
	digest, err := hashArchiveFile(stagedPath)
	if err != nil {
		return backupEntry{}, err
	}
	path := filepath.Join(directory, backupSegmentFilename(start, metadata, digest, previous))
	entry := backupEntry{metadata: metadata, start: start, digest: digest, previous: previous, path: path, segment: true}
	if len(sourceHistory) > 1 {
		return backupEntry{}, errors.New("multiple backup source histories")
	}
	if len(sourceHistory) == 1 {
		entry.sourceHistory = sourceHistory[0].history
		entry.hasSourceHistory = true
		if err := writeSourceHistoryProof(directory, entry, sourceHistory[0]); err != nil {
			return backupEntry{}, err
		}
	}
	if err := os.Link(stagedPath, path); err != nil && !errors.Is(err, os.ErrExist) {
		return backupEntry{}, err
	} else if errors.Is(err, os.ErrExist) {
		actual, err := hashArchiveFile(path)
		if err != nil || actual != digest {
			return backupEntry{}, errors.Join(errors.New("backup segment collision"), err)
		}
	}
	if err := syncPathDirectory(directory); err != nil {
		return backupEntry{}, err
	}
	if err := writeArchiveHead(directory, entry); err != nil {
		return backupEntry{}, err
	}
	return entry, nil
}

func archiveFileSizeLimit(maxSnapshotBytes uint64) (int64, error) {
	if maxSnapshotBytes > uint64(^uint64(0))-archiveStateHeaderBytes || maxSnapshotBytes+archiveStateHeaderBytes >= uint64(^uint(0)>>1) {
		return 0, errors.New("backup archive snapshot size limit is too large")
	}
	return int64(maxSnapshotBytes + archiveStateHeaderBytes), nil
}

func validateArchiveFile(ctx context.Context, path string, metadata BackupMetadata, maxSnapshotBytes uint64, databaseID string) error {
	limit, err := archiveFileSizeLimit(maxSnapshotBytes)
	if err != nil {
		return err
	}
	return validateArchiveFileWithLimits(ctx, path, metadata, databaseID, limit, store.CheckpointScanLimits{
		MaxPayloadBytes: maxSnapshotBytes,
		MaxRecordBytes:  maxSnapshotBytes,
	})
}

func validateArchiveFileWithLimits(ctx context.Context, path string, metadata BackupMetadata, databaseID string, maxFileBytes int64, limits store.CheckpointScanLimits) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("backup archive checkpoint is not a regular file")
	}
	if info.Size() < archiveStateHeaderBytes || info.Size() > maxFileBytes {
		return fmt.Errorf("%w: backup archive checkpoint exceeds snapshot size limit", ErrResourceLimit)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	header, _, err := store.ScanCheckpointV5(ctx, io.TeeReader(file, hash), limits, store.CheckpointScanVisitor{})
	if err != nil {
		return err
	}
	if header.DatabaseID != databaseID || header.CommitID != metadata.CommitID {
		return errors.New("backup checkpoint identity mismatch")
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	gapStart, err := backupGapStart(filepath.Base(path))
	if err != nil || filepath.Base(path) != backupFilename(metadata, digest, gapStart) {
		return errors.New("backup archive filename checksum mismatch")
	}
	return nil
}

func readArchiveEntries(ctx context.Context, directory, databaseID string) ([]backupEntry, error) {
	file, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var entries []backupEntry
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, readErr := file.ReadDir(128)
		for _, item := range batch {
			name := item.Name()
			if !strings.HasPrefix(name, backupPrefix) || !strings.HasSuffix(name, backupSuffix) && !strings.HasSuffix(name, backupSegmentSuffix) {
				continue
			}
			if item.Type()&os.ModeSymlink != 0 || !item.Type().IsRegular() {
				return nil, fmt.Errorf("invalid backup archive entry %q", name)
			}
			path := filepath.Join(directory, name)
			entry := backupEntry{path: path}
			if strings.HasSuffix(name, backupSegmentSuffix) {
				entry, err = parseBackupSegment(name)
				if err != nil {
					return nil, err
				}
				entry.path = path
				info, statErr := os.Lstat(path)
				if statErr != nil {
					return nil, statErr
				}
				if info.Size() <= 0 || info.Size() > maxBackupSegmentBytes {
					return nil, fmt.Errorf("%w: invalid backup WAL segment size", ErrResourceLimit)
				}
				actual, readErr := hashArchiveFile(path)
				if readErr != nil {
					return nil, readErr
				}
				if actual != entry.digest {
					return nil, errors.New("backup WAL segment checksum mismatch")
				}
				if err := store.ValidateBackupWALSegmentFile(path, databaseID, entry.start-1, entry.metadata.CommitID); err != nil {
					return nil, err
				}
			} else {
				entry.metadata, entry.digest, err = parseBackupEntry(name)
				entry.gapStart, _ = backupGapStart(name)
				if err != nil {
					return nil, err
				}
			}
			entry.sourceHistory, entry.hasSourceHistory, err = readSourceHistoryProof(entry, databaseID)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return entries, nil
}

const maxBackupSegmentBytes = 1 << 30

func validateBackupChain(entries []backupEntry) error {
	known := make(map[string]struct{}, len(entries))
	var previous uint64
	for i, entry := range entries {
		if i != 0 && entry.metadata.CommitID <= previous {
			return errors.New("backup archive has conflicting commit entries")
		}
		previous = entry.metadata.CommitID
	}
	for _, entry := range entries {
		if !entry.segment {
			known[fmt.Sprintf("%d:%x", entry.metadata.CommitID, entry.digest)] = struct{}{}
			continue
		}
		parent := fmt.Sprintf("%d:%x", entry.start-1, entry.previous)
		if _, ok := known[parent]; !ok {
			return fmt.Errorf("backup segment %s has a missing or mismatched predecessor", filepath.Base(entry.path))
		}
		known[fmt.Sprintf("%d:%x", entry.metadata.CommitID, entry.digest)] = struct{}{}
	}
	return nil
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
	result.entries, err = readArchiveEntries(context.Background(), archive, databaseID)
	if err == nil {
		sort.Slice(result.entries, func(i, j int) bool {
			if result.entries[i].metadata.CommitID != result.entries[j].metadata.CommitID {
				return result.entries[i].metadata.CommitID < result.entries[j].metadata.CommitID
			}
			return result.entries[i].metadata.CapturedAt.Before(result.entries[j].metadata.CapturedAt)
		})
		err = validateBackupChain(result.entries)
	}
	if err == nil {
		for _, entry := range result.entries {
			if entry.metadata.CapturedAt.After(result.last) {
				result.last = entry.metadata.CapturedAt
			}
			if result.headPath == "" || entry.metadata.CommitID > result.head.CommitID || entry.metadata.CommitID == result.head.CommitID && entry.metadata.CapturedAt.After(result.head.CapturedAt) {
				result.head, result.headPath, result.headDigest = entry.metadata, entry.path, entry.digest
				result.headSourceHistory, result.hasHeadSourceHistory = entry.sourceHistory, entry.hasSourceHistory
			}
		}
	}
	if err == nil {
		headPath := filepath.Join(archive, backupHeadFile)
		if data, readErr := os.ReadFile(headPath); readErr == nil {
			info, statErr := os.Lstat(headPath)
			if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 64<<10 {
				err = errors.New("invalid backup archive head anchor")
			} else {
				var anchor backupHead
				if err = json.Unmarshal(data, &anchor); err == nil {
					var found bool
					for _, entry := range result.entries {
						if filepath.Base(entry.path) == anchor.Entry && entry.metadata.CommitID == anchor.CommitID && hex.EncodeToString(entry.digest[:]) == anchor.Digest {
							found = true
						}
					}
					if !found {
						err = errors.New("backup archive head anchor dependency is missing")
					}
				}
			}
		} else if !errors.Is(readErr, os.ErrNotExist) {
			err = readErr
		} else if len(result.entries) != 0 {
			for _, entry := range result.entries {
				if entry.segment {
					err = errors.New("backup WAL segments require a head anchor")
					break
				}
			}
		}
	}
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

func (archive *backupArchive) chainFor(head backupEntry) (backupEntry, []string, error) {
	for _, entry := range archive.entries {
		if entry.path == head.path {
			head = entry
			break
		}
	}
	byCommitDigest := make(map[string]backupEntry, len(archive.entries))
	for _, entry := range archive.entries {
		byCommitDigest[fmt.Sprintf("%d:%x", entry.metadata.CommitID, entry.digest)] = entry
	}
	var reverse []string
	current := head
	for current.segment {
		reverse = append(reverse, current.path)
		parent, ok := byCommitDigest[fmt.Sprintf("%d:%x", current.start-1, current.previous)]
		if !ok {
			return backupEntry{}, nil, errors.New("backup segment predecessor is missing")
		}
		current = parent
	}
	segments := make([]string, len(reverse))
	for i := range reverse {
		segments[len(reverse)-1-i] = reverse[i]
	}
	return current, segments, nil
}

func (archive *backupArchive) publishBase(now time.Time, graph *store.GraphState, nextNodeID, nextEdgeID, commitID uint64, sourceHistory ...backupSourceHistory) (BackupMetadata, error) {
	captured, err := archive.captureTime(now)
	if err != nil {
		return BackupMetadata{}, err
	}
	metadata := BackupMetadata{CommitID: commitID, CapturedAt: captured}
	var gapStart time.Time
	if archive.headPath != "" {
		gapStart = archive.head.CapturedAt
	}
	entry, err := publishArchiveEntry(archive.directory, metadata, 0, [sha256.Size]byte{}, false, gapStart, func(output io.Writer) error {
		return store.WriteGraphState(output, graph, nextNodeID, nextEdgeID, commitID)
	}, sourceHistory...)
	if err != nil {
		return BackupMetadata{}, err
	}
	archive.last, archive.head, archive.headPath, archive.headDigest = captured, metadata, entry.path, entry.digest
	archive.headSourceHistory, archive.hasHeadSourceHistory = entry.sourceHistory, entry.hasSourceHistory
	return metadata, nil
}

func (archive *backupArchive) captureTime(now time.Time) (time.Time, error) {
	captured := now
	if captured.Before(time.Unix(0, 0)) {
		captured = time.Unix(0, 0)
	}
	if !captured.After(archive.last) {
		captured = archive.last.Add(time.Nanosecond)
	}
	if captured.After(time.Unix(0, math.MaxInt64)) {
		return time.Time{}, errors.New("backup capture timestamp is out of range")
	}
	return captured, nil
}

// captureAt accepts a range copier so the WAL bytes can flow directly into the
// durable staging file. A nil/absent copier means the source WAL cannot prove
// continuity and starts a separate full-base generation.
func (archive *backupArchive) captureAt(now time.Time, graph *store.GraphState, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes uint64, frameRanges ...func(io.Writer) (bool, error)) (BackupMetadata, error) {
	if archive == nil {
		return BackupMetadata{}, nil
	}
	if !archive.ready {
		if err := archive.initialize(graph, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes); err != nil {
			return BackupMetadata{}, err
		}
	}
	if archive.rebaseOnOpen {
		metadata, err := archive.publishBase(now, graph, nextNodeID, nextEdgeID, commitID)
		if err == nil {
			archive.rebaseOnOpen = false
		}
		return metadata, err
	}
	if archive.headPath != "" {
		if commitID == archive.head.CommitID {
			return archive.head, nil
		}
		if commitID < archive.head.CommitID {
			return BackupMetadata{}, fmt.Errorf("backup archive tip %d is ahead of recovered commit %d", archive.head.CommitID, commitID)
		}
	}
	if archive.headPath == "" || len(frameRanges) == 0 || frameRanges[0] == nil {
		return archive.publishBase(now, graph, nextNodeID, nextEdgeID, commitID)
	}
	if commitID != archive.head.CommitID+1 {
		return BackupMetadata{}, fmt.Errorf("backup commit %d does not immediately follow archive head %d", commitID, archive.head.CommitID)
	}
	stage, err := os.CreateTemp(archive.directory, ".latticedb-backup-range-*")
	if err != nil {
		return BackupMetadata{}, err
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	present, copyErr := frameRanges[0](stage)
	if copyErr != nil {
		_ = stage.Close()
		return BackupMetadata{}, copyErr
	}
	if !present {
		_ = stage.Close()
		return BackupMetadata{}, errors.New("committed WAL range is unavailable")
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		return BackupMetadata{}, err
	}
	if err := stage.Close(); err != nil {
		return BackupMetadata{}, err
	}
	if err := store.ValidateBackupWALSegmentFile(stagePath, graph.DatabaseID, archive.head.CommitID, commitID); err != nil {
		return BackupMetadata{}, fmt.Errorf("backup WAL commit frame is invalid: %w", err)
	}
	segmentStart := archive.head.CommitID + 1
	captured, err := archive.captureTime(now)
	if err != nil {
		return BackupMetadata{}, err
	}
	metadata := BackupMetadata{CommitID: commitID, CapturedAt: captured}
	entry, err := publishStagedSegment(archive.directory, stagePath, segmentStart, metadata, archive.headDigest)
	if err != nil {
		return BackupMetadata{}, err
	}
	archive.last, archive.head, archive.headPath, archive.headDigest = captured, metadata, entry.path, entry.digest
	return metadata, nil
}

func (archive *backupArchive) initialize(graph *store.GraphState, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes uint64) error {
	if archive.headPath == "" {
		archive.ready = true
		return nil
	}
	if commitID < archive.head.CommitID {
		return fmt.Errorf("backup archive tip %d is ahead of recovered commit %d", archive.head.CommitID, commitID)
	}
	base, segments, err := archive.chainFor(backupEntry{path: archive.headPath})
	if err != nil {
		return err
	}
	if err := validateArchiveFile(context.Background(), base.path, base.metadata, maxSnapshotBytes, graph.DatabaseID); err != nil {
		return err
	}
	if commitID > archive.head.CommitID {
		if err := store.ValidateBackupWALChain(base.path, segments, archive.head.CommitID, maxSnapshotBytes); err != nil {
			return fmt.Errorf("backup archive WAL chain is corrupt: %w", err)
		}
		archive.rebaseOnOpen = true
	} else if err := store.ValidateBackupWALFiles(base.path, segments, graph, nextNodeID, nextEdgeID, commitID, maxSnapshotBytes); err != nil {
		return fmt.Errorf("backup archive head differs from recovered source: %w", err)
	}
	archive.ready = true
	archive.entries = nil
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
	owner, err := readBackupOwner(archive)
	if err != nil {
		return BackupMetadata{}, err
	}
	entries, err := readArchiveEntries(ctx, archive, owner.DatabaseID)
	if err != nil {
		return BackupMetadata{}, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].metadata.CommitID != entries[j].metadata.CommitID {
			return entries[i].metadata.CommitID < entries[j].metadata.CommitID
		}
		return entries[i].metadata.CapturedAt.Before(entries[j].metadata.CapturedAt)
	})
	if err := validateBackupChain(entries); err != nil {
		return BackupMetadata{}, err
	}
	if err := verifyBackupHeadAnchor(archive, entries); err != nil {
		return BackupMetadata{}, err
	}
	var selected BackupMetadata
	var selectedEntry backupEntry
	for _, entry := range entries {
		meta := entry.metadata
		if !opts.Before.IsZero() && !entry.gapStart.IsZero() && opts.Before.After(entry.gapStart) && opts.Before.Before(meta.CapturedAt) {
			return BackupMetadata{}, fmt.Errorf("%w: requested capture time lies in a backup coverage gap", ErrInvalidArgument)
		}
		if opts.CommitID != nil && meta.CommitID != *opts.CommitID || !opts.Before.IsZero() && meta.CapturedAt.After(opts.Before) {
			continue
		}
		if selectedEntry.path == "" || meta.CommitID > selected.CommitID || meta.CommitID == selected.CommitID && meta.CapturedAt.After(selected.CapturedAt) {
			selected, selectedEntry = meta, entry
		}
	}
	if selectedEntry.path == "" {
		return BackupMetadata{}, os.ErrNotExist
	}
	archiveState := &backupArchive{directory: archive, entries: entries}
	base, segments, err := archiveState.chainFor(selectedEntry)
	if err != nil {
		return BackupMetadata{}, err
	}
	pageArchive := selectedEntry.hasSourceHistory
	if pageArchive {
		if !base.hasSourceHistory {
			return BackupMetadata{}, errors.New("page backup chain has no base source history proof")
		}
		if err := validatePageArchiveFile(ctx, base.path, base.metadata, owner.DatabaseID); err != nil {
			return BackupMetadata{}, err
		}
	} else if err := validateArchiveFile(ctx, base.path, base.metadata, maxBytes, owner.DatabaseID); err != nil {
		return BackupMetadata{}, err
	}
	lock, err := acquireFlatDestinationLock(destination)
	if err != nil {
		return BackupMetadata{}, err
	}
	defer lock.close()
	for _, path := range []string{destination, destination + "-wal", destination + "-wal.base", destination + "-ids", destination + ".layout", destination + ".pages", destination + ".pages.layout"} {
		if _, err := os.Lstat(path); err == nil {
			return BackupMetadata{}, fmt.Errorf("%w: restore destination already exists", ErrInvalidArgument)
		} else if !errors.Is(err, os.ErrNotExist) {
			return BackupMetadata{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return BackupMetadata{}, err
	}
	if pageArchive {
		var histories [][32]byte
		if base.hasSourceHistory && selectedEntry.hasSourceHistory {
			histories = [][32]byte{base.sourceHistory, selectedEntry.sourceHistory}
		}
		if err := store.RestorePageBackup(ctx, base.path, segments, destination, selected.CommitID, opts.MaxDatabaseSnapshotBytes, histories...); err != nil {
			if errors.Is(err, store.ErrLoadResourceLimit) {
				return BackupMetadata{}, fmt.Errorf("%w: %w", ErrResourceLimit, err)
			}
			return BackupMetadata{}, err
		}
	} else if err := store.CreateBackupRecoveryFilesFrom(ctx, store.FlatDatabaseFiles(destination), base.path, segments, maxBytes, selected.CommitID); err != nil {
		return BackupMetadata{}, err
	}
	return selected, nil
}

func readBackupOwner(directory string) (backupOwner, error) {
	path := filepath.Join(directory, backupOwnerFile)
	info, err := os.Lstat(path)
	if err != nil {
		return backupOwner{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return backupOwner{}, errors.New("invalid backup archive owner metadata")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return backupOwner{}, err
	}
	var owner backupOwner
	if err := json.Unmarshal(data, &owner); err != nil || owner.DatabaseID == "" {
		return backupOwner{}, errors.New("invalid backup archive owner metadata")
	}
	return owner, nil
}

func verifyBackupHeadAnchor(directory string, entries []backupEntry) error {
	path := filepath.Join(directory, backupHeadFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		for _, entry := range entries {
			if entry.segment {
				return errors.New("backup WAL segments require a head anchor")
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return errors.New("invalid backup archive head anchor")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var anchor backupHead
	if err := json.Unmarshal(data, &anchor); err != nil {
		return errors.New("invalid backup archive head anchor")
	}
	for _, entry := range entries {
		if filepath.Base(entry.path) == anchor.Entry && entry.metadata.CommitID == anchor.CommitID && hex.EncodeToString(entry.digest[:]) == anchor.Digest {
			return nil
		}
	}
	return errors.New("backup archive head anchor dependency is missing")
}
