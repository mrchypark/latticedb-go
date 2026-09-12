package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func testFiles(t *testing.T) store.DatabaseFiles {
	t.Helper()
	return store.DirectoryDatabaseFiles(t.TempDir())
}

func TestOpenVectorCacheFile_Missing(t *testing.T) {
	files := testFiles(t)
	_, err := openVectorCacheFile(files, 1<<20)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want os.ErrNotExist, got %v", err)
	}
}

func TestPublishAndOpen_RoundTrip(t *testing.T) {
	files := testFiles(t)
	payload := []byte("vector-cache-payload-v1")

	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	f, err := openVectorCacheFile(files, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("content mismatch: got %q, want %q", got, payload)
	}
}

func TestOpenVectorCacheFile_SymlinkReject(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	if err := os.Symlink("target-nonexistent", sidecar); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}

	_, err := openVectorCacheFile(files, 1<<20)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("want symlink error, got %v", err)
	}
}

func TestOpenVectorCacheFile_NonRegularReject(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	if err := os.Mkdir(sidecar, 0o700); err != nil {
		t.Fatal(err)
	}

	_, err := openVectorCacheFile(files, 1<<20)
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("want not-regular error, got %v", err)
	}
}

func TestOpenVectorCacheFile_ByteLimit(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	data := make([]byte, 200)
	if err := os.WriteFile(sidecar, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := openVectorCacheFile(files, 100)
	if err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("want size limit error, got %v", err)
	}
}

func TestPublishVectorCacheFile_ByteLimit(t *testing.T) {
	files := testFiles(t)

	big := make([]byte, 200)
	err := publishVectorCacheFile(context.Background(), files, 100, func(w io.Writer) error {
		_, werr := w.Write(big)
		return werr
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds size limit") {
		t.Fatalf("want size limit error, got %v", err)
	}

	entries, readErr := os.ReadDir(files.Directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".latticedb-") && strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temp file not cleaned up: %s", e.Name())
		}
	}
}

func TestPublishVectorCacheFile_ContextCanceled(t *testing.T) {
	files := testFiles(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := publishVectorCacheFile(ctx, files, 1<<20, func(w io.Writer) error {
		_, werr := w.Write([]byte("x"))
		return werr
	})
	if err == nil {
		t.Fatal("want error from canceled context, got nil")
	}

	entries, readErr := os.ReadDir(files.Directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".latticedb-") && strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temp file not cleaned up: %s", e.Name())
		}
	}
}

func TestPublishVectorCacheFile_DestinationSymlinkReject(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	if err := os.Symlink("target", sidecar); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}

	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error {
		_, werr := w.Write([]byte("data"))
		return werr
	})
	if err == nil || !strings.Contains(err.Error(), "vector cache sidecar is a symlink") {
		t.Fatalf("want destination symlink error, got %v", err)
	}
}

func TestPublishVectorCacheFile_DestinationMultipleLinks(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	target := filepath.Join(files.Directory, "link-source")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(target, sidecar); err != nil {
		t.Fatal(err)
	}

	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error {
		_, werr := w.Write([]byte("data"))
		return werr
	})
	if err == nil || !strings.Contains(err.Error(), "multiple hard links") {
		t.Fatalf("want multiple links error, got %v", err)
	}
}

func TestPublishVectorCacheFile_ReplacesExisting(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	if err := os.WriteFile(sidecar, []byte(vectorCacheMagic+"old"), 0o600); err != nil {
		t.Fatal(err)
	}

	newPayload := []byte("new-payload")
	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error {
		_, werr := w.Write(newPayload)
		return werr
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(newPayload) {
		t.Fatalf("content: got %q, want %q", got, newPayload)
	}
}

func TestPublishVectorCacheFile_CleansTempOnEncodeError(t *testing.T) {
	files := testFiles(t)

	encodeErr := errors.New("encode failed")
	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error {
		return encodeErr
	})
	if !errors.Is(err, encodeErr) {
		t.Fatalf("want wrapped encode error, got %v", err)
	}

	entries, readErr := os.ReadDir(files.Directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".latticedb-") && strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temp file not cleaned up after encode error: %s", e.Name())
		}
	}
}

func TestOpenVectorCacheFile_FlatDBPath(t *testing.T) {
	dir := t.TempDir()
	flatPath := filepath.Join(dir, "mydb")
	files := store.FlatDatabaseFiles(flatPath)

	sidecar := files.State + "-hnsw"
	want := flatPath + "-hnsw"
	if sidecar != want {
		t.Fatalf("flat sidecar path: got %q, want %q", sidecar, want)
	}

	data := []byte("flat-db-sidecar")
	if err := os.WriteFile(sidecar, data, 0o600); err != nil {
		t.Fatal(err)
	}

	f, err := openVectorCacheFile(files, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("content mismatch: got %q, want %q", got, data)
	}
}

func TestPublishVectorCacheFile_DestinationReplacedDuringEncode(t *testing.T) {
	files := testFiles(t)
	sidecar := files.State + "-hnsw"

	// Plant initial valid sidecar.
	if err := os.WriteFile(sidecar, []byte(vectorCacheMagic+"old"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Deterministic: replace destination inside encode callback after write.
	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error {
		if _, err := w.Write([]byte("new")); err != nil {
			return err
		}
		// Destination must change between our capture and the post-encode check.
		if err := os.Rename(sidecar, sidecar+".old"); err != nil {
			return err
		}
		return os.WriteFile(sidecar, []byte("replacement"), 0o600)
	})
	if err == nil || !strings.Contains(err.Error(), "destination changed") {
		t.Fatalf("want destination changed error, got %v", err)
	}

	got, err := os.ReadFile(sidecar)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("replacement overwritten: %q, %v", got, err)
	}
}

func TestPublishVectorCacheFile_ContextCanceledAfterEncode(t *testing.T) {
	files := testFiles(t)

	ctx, cancel := context.WithCancel(context.Background())
	err := publishVectorCacheFile(ctx, files, 1<<20, func(w io.Writer) error {
		// Write succeeds, then cancel before publish checks context.
		_, werr := w.Write([]byte("data"))
		cancel()
		return werr
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	entries, readErr := os.ReadDir(files.Directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".latticedb-") && strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temp file not cleaned up: %s", e.Name())
		}
	}
}

func TestPublishVectorCacheFilePreservesUnrelatedFile(t *testing.T) {
	files := testFiles(t)
	path := files.State + "-hnsw"
	original := []byte("unrelated database or user file")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	err := publishVectorCacheFile(context.Background(), files, 1<<20, func(w io.Writer) error { _, err := io.WriteString(w, vectorCacheMagic); return err })
	if err == nil {
		t.Fatal("unrelated file replaced")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(original) {
		t.Fatalf("unrelated file changed: %q %v", got, err)
	}
}
