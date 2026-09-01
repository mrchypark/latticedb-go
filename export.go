package latticedb

import (
	"context"
	"io"

	"github.com/mrchypark/latticedb-go/internal/exporter"
)

type ExportFormat string

const (
	ExportFormatJSON  ExportFormat = "json"
	ExportFormatJSONL ExportFormat = "jsonl"
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatDOT   ExportFormat = "dot"
)

func Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	return ExportContext(context.Background(), dbPath, format, outputPath)
}

func ExportContext(ctx context.Context, dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	db, err := OpenContext(ctx, dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return db.ExportContext(ctx, format, outputPath)
}

func ExportFile(dbPath string, format ExportFormat, outputPath string) error {
	return ExportFileContext(context.Background(), dbPath, format, outputPath)
}

func ExportFileContext(ctx context.Context, dbPath string, format ExportFormat, outputPath string) error {
	db, err := OpenContext(ctx, dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.ExportFileContext(ctx, format, outputPath)
}

func Dump(dbPath string) ([]byte, error) {
	return DumpContext(context.Background(), dbPath)
}

func DumpContext(ctx context.Context, dbPath string) ([]byte, error) {
	db, err := OpenContext(ctx, dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return db.DumpContext(ctx)
}

func (db *DB) Export(format ExportFormat, outputPath string) ([]byte, error) {
	return db.ExportContext(context.Background(), format, outputPath)
}

func (db *DB) ExportContext(ctx context.Context, format ExportFormat, outputPath string) ([]byte, error) {
	graph, err := db.inner.SnapshotGraph()
	if err != nil {
		return nil, err
	}
	return exporter.ExportGraphContext(ctx, graph, exporter.ExportFormat(format), outputPath)
}

func (db *DB) ExportFile(format ExportFormat, outputPath string) error {
	return db.ExportFileContext(context.Background(), format, outputPath)
}

func (db *DB) ExportFileContext(ctx context.Context, format ExportFormat, outputPath string) error {
	graph, err := db.inner.SnapshotGraph()
	if err != nil {
		return err
	}
	return exporter.ExportGraphFileContext(ctx, graph, exporter.ExportFormat(format), outputPath)
}

func (db *DB) Dump() ([]byte, error) {
	return db.DumpContext(context.Background())
}

func (db *DB) DumpContext(ctx context.Context) ([]byte, error) {
	graph, err := db.inner.SnapshotGraph()
	if err != nil {
		return nil, err
	}
	return exporter.DumpGraphContext(ctx, graph)
}

func (db *DB) DumpTo(output io.Writer) error {
	return db.DumpToContext(context.Background(), output)
}

// DumpToContext observes cancellation between writes. It cannot interrupt an
// output writer that is itself blocked in Write.
func (db *DB) DumpToContext(ctx context.Context, output io.Writer) error {
	graph, err := db.inner.SnapshotGraph()
	if err != nil {
		return err
	}
	return exporter.DumpGraphContextTo(ctx, graph, output)
}

func (db *DB) ExportTo(format ExportFormat, output io.Writer) error {
	return db.ExportToContext(context.Background(), format, output)
}

// ExportToContext observes cancellation between writes. It cannot interrupt an
// output writer that is itself blocked in Write.
func (db *DB) ExportToContext(ctx context.Context, format ExportFormat, output io.Writer) error {
	graph, err := db.inner.SnapshotGraph()
	if err != nil {
		return err
	}
	return exporter.ExportGraphContextTo(ctx, graph, exporter.ExportFormat(format), output)
}

func SimulateCrash(dbPath string) error {
	return exporter.SimulateCrash(dbPath)
}
