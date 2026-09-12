package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// openVectorCacheFile opens the vector cache sidecar at files.State+"-hnsw".
// Returns os.ErrNotExist when absent. Rejects symlinks, non-regular files,
// multiple hard links, oversized files, and identity changes during open.
func openVectorCacheFile(files store.DatabaseFiles, maxBytes uint64) (*os.File, error) {
	path := files.State + "-hnsw"
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if err := validateSidecarFile(path, info); err != nil {
		return nil, err
	}
	if uint64(info.Size()) > maxBytes {
		return nil, fmt.Errorf("vector cache sidecar exceeds size limit: %d > %d", info.Size(), maxBytes)
	}
	// Windows may resolve a path-based FileInfo's identity lazily. Freeze it
	// before opening, rather than resolving both paths after a replacement.
	if !os.SameFile(info, info) {
		return nil, errors.New("cannot capture vector cache file identity")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	postInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !os.SameFile(info, postInfo) {
		f.Close()
		return nil, fmt.Errorf("vector cache sidecar replaced during open: %s", path)
	}
	if !postInfo.Mode().IsRegular() || uint64(postInfo.Size()) > maxBytes {
		f.Close()
		return nil, fmt.Errorf("vector cache sidecar changed size or type: %s", path)
	}
	if err := validateSidecarFile(path, postInfo); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// publishVectorCacheFile writes a vector cache sidecar atomically. encode is
// called with a bounded writer that enforces maxBytes and observes ctx. On
// success the sidecar is durably in place; on any failure the temp is removed.
func publishVectorCacheFile(ctx context.Context, files store.DatabaseFiles, maxBytes uint64, encode func(io.Writer) error) error {
	dest := files.State + "-hnsw"

	// Capture destination identity before any write.
	destInfo, destErr := os.Lstat(dest)
	if destErr == nil {
		if err := validateSidecarFile(dest, destInfo); err != nil {
			return err
		}
		// A derived filename may collide with a pre-existing user file. Only
		// replace recognizable caches, including unsupported cache versions.
		previous, err := openVectorCacheFile(files, maxBytes)
		if err != nil {
			return err
		}
		// Descriptor Stat captures identity now, including on Windows.
		destInfo, err = previous.Stat()
		if err != nil {
			previous.Close()
			return err
		}
		var prefix [7]byte
		_, readErr := io.ReadFull(previous, prefix[:])
		closeErr := previous.Close()
		if readErr != nil || string(prefix[:]) != vectorCacheMagic[:7] {
			return errors.New("vector cache destination is not a recognized cache")
		}
		if closeErr != nil {
			return closeErr
		}
	} else if !errors.Is(destErr, os.ErrNotExist) {
		return fmt.Errorf("stat vector cache destination: %w", destErr)
	}

	pattern := store.DatabaseTempPattern(files, "vector-cache")
	tmp, err := os.CreateTemp(files.Directory, pattern)
	if err != nil {
		return fmt.Errorf("create vector cache temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	tmpInfo, err := tmp.Stat()
	if err != nil {
		return fmt.Errorf("stat vector cache temp: %w", err)
	}

	bw := &vectorCacheWriter{w: tmp, remaining: maxBytes, ctx: ctx}
	if err := encode(bw); err != nil {
		return fmt.Errorf("encode vector cache: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync vector cache temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close vector cache temp: %w", err)
	}

	// Verify temp identity unchanged after write.
	postInfo, err := os.Lstat(tmpPath)
	if err != nil {
		return fmt.Errorf("stat vector cache temp after write: %w", err)
	}
	if !os.SameFile(tmpInfo, postInfo) {
		return errors.New("vector cache temp identity changed during write")
	}

	// Check context before touching destination.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Verify destination identity unchanged since capture, revalidating
	// in case same-inode acquired new hard links during encode.
	if destErr == nil {
		postDest, err := os.Lstat(dest)
		if err != nil {
			return fmt.Errorf("stat vector cache destination before rename: %w", err)
		}
		if !os.SameFile(destInfo, postDest) {
			return errors.New("vector cache destination changed during write")
		}
		if err := validateSidecarFile(dest, postDest); err != nil {
			return err
		}
	} else {
		if _, err := os.Lstat(dest); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return fmt.Errorf("vector cache destination appeared during write: %w", err)
			}
			return errors.New("vector cache destination appeared during write")
		}
	}

	if err := os.Rename(tmpPath, dest); err != nil {
		return fmt.Errorf("publish vector cache: %w", err)
	}
	return syncPathDirectory(files.Directory)
}

// validateSidecarFile checks that a sidecar file is safe to overwrite: must
// be a regular file with a single hard link.
func validateSidecarFile(path string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("vector cache sidecar is a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("vector cache sidecar is not a regular file: %s", path)
	}
	multi, err := regularFileHasMultipleLinks(path, info)
	if err != nil {
		return fmt.Errorf("vector cache sidecar link check: %w", err)
	}
	if multi {
		return fmt.Errorf("vector cache sidecar has multiple hard links: %s", path)
	}
	return nil
}

// vectorCacheWriter wraps an io.Writer with a byte limit and context check.
type vectorCacheWriter struct {
	w         io.Writer
	remaining uint64
	ctx       context.Context
}

func (w *vectorCacheWriter) Write(p []byte) (int, error) {
	if w.ctx != nil {
		if err := w.ctx.Err(); err != nil {
			return 0, err
		}
	}
	n := uint64(len(p))
	if n > w.remaining {
		return 0, fmt.Errorf("vector cache sidecar exceeds size limit")
	}
	written, err := w.w.Write(p)
	w.remaining -= uint64(written)
	return written, err
}
