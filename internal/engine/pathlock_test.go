package engine

import (
	"os"
	"testing"
)

func TestWriteLayoutOwnerContentsReturnsWriteError(t *testing.T) {
	directory, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()

	if err := writeLayoutOwnerContents(directory, []byte("owner")); err == nil {
		t.Fatal("writeLayoutOwnerContents succeeded for a directory")
	}
}
