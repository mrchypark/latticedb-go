package engine

import (
	"errors"
	"fmt"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func (db *DB) ValidateExportDestination(outputPath string) error {
	if db == nil {
		return ErrDatabaseClosed
	}
	db.mu.RLock()
	files := db.files
	path := db.path
	db.mu.RUnlock()
	if err := store.ValidateExportDestination(files, files.State != path, outputPath); err != nil {
		if errors.Is(err, store.ErrExportDestinationConflict) {
			return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
		return err
	}
	return nil
}
