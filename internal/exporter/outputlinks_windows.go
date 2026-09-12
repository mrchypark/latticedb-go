//go:build windows

package exporter

import (
	"github.com/mrchypark/latticedb-go/internal/store"
	"os"
)

func exportOutputHasMultipleLinks(path string, _ os.FileInfo) (bool, error) {
	return store.FileHasMultipleLinks(path)
}
