package exporter

import "github.com/mrchypark/latticedb-go/internal/store"

func validateDirectoryExportDestination(dbPath, outputPath string) error {
	dbPath, err := canonicalExportOutputPath(dbPath)
	if err != nil {
		return err
	}
	return store.ValidateExportDestination(store.DirectoryDatabaseFiles(dbPath), true, outputPath)
}
