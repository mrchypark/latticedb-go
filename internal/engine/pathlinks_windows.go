//go:build windows

package engine

import (
	"github.com/mrchypark/latticedb-go/internal/store"
	"os"
)

func regularFileHasMultipleLinks(path string, _ os.FileInfo) (bool, error) {
	return store.FileHasMultipleLinks(path)
}
