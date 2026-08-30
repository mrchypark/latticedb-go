package exporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type exportPathLock struct {
	semaphore chan struct{}
	refs      int
}

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
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	exportPathLocks.Lock()
	entry := exportPathLocks.entries[path]
	if entry == nil {
		entry = &exportPathLock{semaphore: make(chan struct{}, 1)}
		entry.semaphore <- struct{}{}
		exportPathLocks.entries[path] = entry
	}
	entry.refs++
	exportPathLocks.Unlock()
	releaseRef := func() {
		exportPathLocks.Lock()
		entry.refs--
		if entry.refs == 0 && exportPathLocks.entries[path] == entry {
			delete(exportPathLocks.entries, path)
		}
		exportPathLocks.Unlock()
	}
	select {
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	case <-entry.semaphore:
	}
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		entry.semaphore <- struct{}{}
		releaseRef()
		return nil, err
	}
	retry := time.NewTicker(10 * time.Millisecond)
	defer retry.Stop()
	for {
		locked, lockErr := tryLockExportFile(file)
		if lockErr != nil {
			_ = file.Close()
			entry.semaphore <- struct{}{}
			releaseRef()
			return nil, fmt.Errorf("lock export output: %w", lockErr)
		}
		if locked {
			break
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			entry.semaphore <- struct{}{}
			releaseRef()
			return nil, ctx.Err()
		case <-retry.C:
		}
	}
	return func() {
		_ = unlockExportFile(file)
		_ = file.Close()
		entry.semaphore <- struct{}{}
		releaseRef()
	}, nil
}
