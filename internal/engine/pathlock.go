package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	ErrDatabaseLocked         = errors.New("database is locked")
	ErrDatabaseLayoutConflict = errors.New("database layout conflicts with existing owner")
	pathLocks                 = struct {
		sync.Mutex
		paths map[string]struct{}
	}{paths: map[string]struct{}{}}
)

type pathLock struct {
	path string
	file *os.File
}

type layoutOwner struct {
	Version    int    `json:"version"`
	Mode       string `json:"mode"`
	DatabaseID string `json:"database_id"`
}

func acquirePathLock(path string, create bool) (*pathLock, string, bool, error) {
	canonical, err := canonicalDBPath(path)
	if err != nil {
		return nil, "", false, err
	}
	flat, err := prepareDBPath(canonical, create)
	if err != nil {
		return nil, "", false, err
	}

	statePath := filepath.Join(canonical, "state.json")
	if flat {
		statePath = canonical
	}
	if err := checkLayoutOwner(statePath, flat, ""); err != nil {
		return nil, "", false, err
	}

	pathLocks.Lock()
	if _, exists := pathLocks.paths[statePath]; exists {
		pathLocks.Unlock()
		return nil, "", false, ErrDatabaseLocked
	}
	pathLocks.paths[statePath] = struct{}{}
	pathLocks.Unlock()

	lockPath := statePath + ".lock"
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err == nil {
		err = tryLockFile(file)
	}
	if err == nil {
		err = checkLayoutOwner(statePath, flat, "")
	}
	if err != nil {
		if file != nil {
			_ = unlockFile(file)
			_ = file.Close()
		}
		pathLocks.Lock()
		delete(pathLocks.paths, statePath)
		pathLocks.Unlock()
		if errors.Is(err, ErrDatabaseLayoutConflict) {
			return nil, "", false, err
		}
		return nil, "", false, fmt.Errorf("%w: %v", ErrDatabaseLocked, err)
	}
	return &pathLock{path: statePath, file: file}, canonical, flat, nil
}

func ensureLayoutOwner(statePath string, flat bool, databaseID string) error {
	if err := checkLayoutOwner(statePath, flat, databaseID); err != nil {
		return err
	}
	markerPath := statePath + ".layout"
	owner, exists, err := readLayoutOwner(markerPath)
	if err != nil {
		return err
	}
	mode := "directory"
	if flat {
		mode = "flat"
	}
	if exists && owner.DatabaseID == databaseID {
		return nil
	}
	return writeLayoutOwner(markerPath, layoutOwner{Version: 1, Mode: mode, DatabaseID: databaseID})
}

func writeLayoutOwner(markerPath string, owner layoutOwner) error {
	data, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(markerPath), ".latticedb-layout-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(data)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(temporaryPath, markerPath)
	}
	if err == nil {
		err = syncPathDirectory(filepath.Dir(markerPath))
	}
	return err
}

func checkLayoutOwner(statePath string, flat bool, databaseID string) error {
	owner, exists, err := readLayoutOwner(statePath + ".layout")
	if err != nil {
		return err
	}
	if exists {
		if owner.Version != 1 || owner.Mode != layoutMode(flat) || databaseID != "" && owner.DatabaseID != databaseID {
			return ErrDatabaseLayoutConflict
		}
	}
	return nil
}

func readLayoutOwner(markerPath string) (layoutOwner, bool, error) {
	marker, err := os.Open(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return layoutOwner{}, false, nil
	}
	if err != nil {
		return layoutOwner{}, false, err
	}
	data, readErr := io.ReadAll(io.LimitReader(marker, 4097))
	closeErr := marker.Close()
	if readErr != nil {
		return layoutOwner{}, false, readErr
	}
	if closeErr != nil {
		return layoutOwner{}, false, closeErr
	}
	if len(data) > 4096 {
		return layoutOwner{}, false, ErrDatabaseLayoutConflict
	}
	var owner layoutOwner
	if json.Unmarshal(data, &owner) != nil {
		return layoutOwner{}, false, ErrDatabaseLayoutConflict
	}
	return owner, true, nil
}

func layoutMode(flat bool) string {
	if flat {
		return "flat"
	}
	return "directory"
}

func prepareDBPath(path string, create bool) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return false, nil
		}
		if info.Mode().IsRegular() {
			if err := validateFlatStatePath(path); err != nil {
				return false, err
			}
			multiple, err := regularFileHasMultipleLinks(path, info)
			if err != nil {
				return false, err
			}
			if multiple {
				return false, errors.New("database files with multiple hard links are unsupported")
			}
			return true, nil
		}
		return false, fmt.Errorf("%s is not a regular file or directory", path)
	}
	if !errors.Is(err, os.ErrNotExist) || !create {
		return false, err
	}
	if err := durableMkdirAll(path); err != nil {
		return false, fmt.Errorf("create db directory: %w", err)
	}
	return false, nil
}

func validateFlatStatePath(path string) error {
	name := strings.ToLower(filepath.Base(path))
	for _, reserved := range []string{"wal.log", "wal.base", "ids.json"} {
		if name == reserved {
			return ErrDatabaseLayoutConflict
		}
	}
	for _, suffix := range []string{".lock", ".layout", "-wal", "-wal.base", "-ids", ".tmp"} {
		if strings.HasSuffix(name, suffix) {
			return ErrDatabaseLayoutConflict
		}
	}
	for _, prefix := range []string{".state-", ".snapshot-payload-", ".wal-", ".wal-payload-", ".ids-", ".latticedb-layout-"} {
		if strings.HasPrefix(name, prefix) {
			return ErrDatabaseLayoutConflict
		}
	}
	return nil
}

func canonicalDBPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve database path: %w", err)
	}
	current := abs
	var missing []string
	for {
		canonical, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				canonical = filepath.Join(canonical, missing[index])
			}
			return canonical, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("resolve database path: %w", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("resolve database path: %w", err)
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func durableMkdirAll(path string) error {
	current := path
	var missing []string
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return err
		}
		current = parent
	}
	for index := len(missing) - 1; index >= 0; index-- {
		directory := missing[index]
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := syncPathDirectory(directory); err != nil {
			return err
		}
		if err := syncPathDirectory(filepath.Dir(directory)); err != nil {
			return err
		}
	}
	return nil
}

func syncPathDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}

func (lock *pathLock) close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := unlockFile(lock.file)
	if closeErr := lock.file.Close(); err == nil {
		err = closeErr
	}
	pathLocks.Lock()
	delete(pathLocks.paths, lock.path)
	pathLocks.Unlock()
	lock.file = nil
	return err
}
