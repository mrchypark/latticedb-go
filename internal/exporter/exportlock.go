package exporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
	directory, err := os.OpenRoot(filepath.Dir(lockPath))
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	opened, err := directory.Stat(".")
	if err != nil {
		return nil, err
	}
	current, err := os.Lstat(filepath.Dir(lockPath))
	if err != nil || !current.IsDir() || !os.SameFile(opened, current) {
		return nil, errors.New("export lock directory identity changed")
	}
	exportPathLocks.Lock()
	entry, err := findExportPathLock(directory, filepath.Base(lockPath))
	if err != nil {
		exportPathLocks.Unlock()
		return nil, err
	}
	if entry == nil {
		file, err := directory.OpenFile(filepath.Base(lockPath), os.O_CREATE|os.O_RDWR, 0o600)
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
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("create export parent directory: %w", err)
	}
	mode := os.FileMode(0o777) | os.ModeSticky
	if runtime.GOOS == "windows" {
		mode = 0o777
	}
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		staging, err := os.MkdirTemp(filepath.Dir(path), exportLockDirectory+".init-")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(staging)
		// A published directory stays nonempty, so competing renames cannot replace it.
		if err := os.WriteFile(filepath.Join(staging, ".ready"), nil, 0o600); err != nil {
			return "", err
		}
		root, err := os.OpenRoot(staging)
		if err != nil {
			return "", err
		}
		chmodErr := root.Chmod(".", mode)
		closeErr := root.Close()
		if err := errors.Join(chmodErr, closeErr); err != nil {
			return "", fmt.Errorf("set export lock directory permissions: %w", err)
		}
		if err := os.Rename(staging, directory); err != nil {
			if _, statErr := os.Lstat(directory); statErr != nil {
				return "", fmt.Errorf("publish export lock directory: %w", err)
			}
		}
	} else if err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("export lock directory must be a real directory")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return "", err
	}
	defer root.Close()
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return "", errors.New("export lock directory identity changed")
	}
	if runtime.GOOS != "windows" && (opened.Mode().Perm() != 0o777 || opened.Mode()&os.ModeSticky == 0) {
		return "", errors.New("export lock directory requires sticky shared permissions")
	}
	return filepath.Join(directory, filepath.Base(path)+".lock"), nil
}

// findExportPathLock runs while exportPathLocks is held. It checks the lock
// file before opening anything, because closing a redundant descriptor can
// release this process's POSIX record lock.
func findExportPathLock(directory *os.Root, name string) (*exportPathLock, error) {
	info, err := directory.Lstat(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("export lock must be a regular file")
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
	if exportPathIsReserved(abs) {
		return "", errors.New("export destination is inside the reserved lock directory")
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
		if name := strings.ToLower(filepath.Base(directory)); name == exportLockDirectory || strings.HasPrefix(name, exportLockDirectory+".init-") {
			return true
		}
		if filepath.Dir(directory) == directory {
			return false
		}
	}
}
