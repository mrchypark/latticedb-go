package latticedb

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenForwardsVectorMValidation(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "invalid-m"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorM: 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Open error = %v, want invalid argument", err)
	}
}
