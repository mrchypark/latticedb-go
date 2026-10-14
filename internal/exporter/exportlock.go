package exporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type exportPathLock struct {
	semaphore chan struct{}
	refs      int
	file      *os.File
	info      os.FileInfo
}

const exportLockDirectory = ".latticedb-export-locks"

// ponytail: scan only active locks by file identity; add an inode index if
// concurrent exports are ever numerous enough for this bounded registry to matter.
var exportPathLocks = struct {
	sync.Mutex
	entries map[string]*exportPathLock
}{entries: make(map[string]*exportPathLock)}

func acquireExportLock(path string) (func(), error) {
	return acquireExportLockContext(context.Background(), path)
}

func acquireExportLockContext(ctx context.Context, path string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := canonicalExportOutputPath(path)
	if err != nil {
		return nil, err
	}
	lockPath, err := exportLockPath(path)
	if err != nil {
		return nil, err
	}
	exportPathLocks.Lock()
	entry, err := findExportPathLock(lockPath)
	if err != nil {
		exportPathLocks.Unlock()
		return nil, err
	}
	if entry == nil {
		file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			exportPathLocks.Unlock()
			return nil, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			exportPathLocks.Unlock()
			return nil, err
		}
		entry = &exportPathLock{file: file, info: info, semaphore: make(chan struct{}, 1)}
		entry.semaphore <- struct{}{}
		exportPathLocks.entries[lockPath] = entry
	}
	entry.refs++
	exportPathLocks.Unlock()
	releaseRef := func() {
		exportPathLocks.Lock()
		entry.refs--
		if entry.refs == 0 {
			for key, candidate := range exportPathLocks.entries {
				if candidate == entry {
					delete(exportPathLocks.entries, key)
					break
				}
			}
			_ = entry.file.Close()
		}
		exportPathLocks.Unlock()
	}
	select {
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	case <-entry.semaphore:
	}
	retry := time.NewTicker(10 * time.Millisecond)
	defer retry.Stop()
	for {
		locked, lockErr := tryLockExportFile(entry.file)
		if lockErr != nil {
			entry.semaphore <- struct{}{}
			releaseRef()
			return nil, fmt.Errorf("lock export output: %w", lockErr)
		}
		if locked {
			break
		}
		select {
		case <-ctx.Done():
			entry.semaphore <- struct{}{}
			releaseRef()
			return nil, ctx.Err()
		case <-retry.C:
		}
	}
	return func() {
		_ = unlockExportFile(entry.file)
		entry.semaphore <- struct{}{}
		releaseRef()
	}, nil
}

func exportLockPath(path string) (string, error) {
	directory := filepath.Join(filepath.Dir(path), exportLockDirectory)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create export lock directory: %w", err)
	}
	return filepath.Join(directory, filepath.Base(path)+".lock"), nil
}

// findExportPathLock runs while exportPathLocks is held. It checks the lock
// file before opening anything, because closing a redundant descriptor can
// release this process's POSIX record lock.
func findExportPathLock(lockPath string) (*exportPathLock, error) {
	info, err := os.Stat(lockPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	for _, entry := range exportPathLocks.entries {
		if os.SameFile(info, entry.info) {
			return entry, nil
		}
	}
	return nil, nil
}

func canonicalExportOutputPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve export output path: %w", err)
	}
	current := abs
	var missing []string
	for {
		canonical, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				canonical = filepath.Join(canonical, missing[index])
			}
			info, err := os.Stat(canonical)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
			if err == nil && info.Mode().IsRegular() {
				multiple, err := exportOutputHasMultipleLinks(canonical, info)
				if err != nil {
					return "", err
				}
				if multiple {
					return "", errors.New("export outputs with multiple hard links are unsupported")
				}
			}
			if exportPathIsReserved(canonical) {
				return "", errors.New("export destination is inside the reserved lock directory")
			}
			return canonical, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("resolve export output path: %w", err)
		}
		info, lstatErr := os.Lstat(current)
		if lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(current)
			if err != nil {
				return "", fmt.Errorf("resolve export output path: %w", err)
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(current), target)
			}
			current = filepath.Clean(target)
			continue
		}
		if lstatErr != nil && !errors.Is(lstatErr, os.ErrNotExist) {
			return "", fmt.Errorf("resolve export output path: %w", lstatErr)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("resolve export output path: %w", err)
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func exportPathIsReserved(path string) bool {
	for directory := path; ; directory = filepath.Dir(directory) {
		if strings.EqualFold(filepath.Base(directory), exportLockDirectory) {
			return true
		}
		if filepath.Dir(directory) == directory {
			return false
		}
	}
}
