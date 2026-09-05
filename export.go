package latticedb

import (
	"context"
	"io"
	"time"

	"github.com/mrchypark/latticedb-go/internal/exporter"
)

type ExportFormat string

// ErrExportOutputLimit reports that an ExportOptions limit was reached.
var ErrExportOutputLimit = exporter.ErrOutputLimit

var ErrCSVGenerationPruningUnsupported = exporter.ErrCSVGenerationPruningUnsupported

// CSVGenerationRetention controls explicit pruning of old CSV export
// generations. Readers must use OpenCSVGenerationContext while pruning is in
// use; legacy readers have no lease and are not protected.
type CSVGenerationRetention struct {
	// KeepLatest preserves this many newest generations in addition to active pins and the current generation.
	KeepLatest uint
	// MinAge preserves generations younger than this duration. Negative values are invalid.
	// At least one retention setting must be positive.
	MinAge time.Duration
}

// CSVGenerationLease pins one immutable CSV generation until Close.
type CSVGenerationLease struct {
	Generation string
	NodesPath  string
	EdgesPath  string
	inner      *exporter.CSVGenerationLease
}

func OpenCSVGenerationContext(ctx context.Context, manifestPath string) (*CSVGenerationLease, error) {
	lease, err := exporter.OpenCSVGenerationContext(ctx, manifestPath)
	if err != nil {
		return nil, wrapError(err)
	}
	return &CSVGenerationLease{Generation: lease.Generation, NodesPath: lease.NodesPath, EdgesPath: lease.EdgesPath, inner: lease}, nil
}

// Close is idempotent.
func (lease *CSVGenerationLease) Close() error {
	if lease == nil || lease.inner == nil {
		return nil
	}
	return wrapError(lease.inner.Close())
}

func PruneCSVGenerationsContext(ctx context.Context, manifestPath string, retention CSVGenerationRetention) (int, error) {
	removed, err := exporter.PruneCSVGenerationsContext(ctx, manifestPath, exporter.CSVGenerationRetention(retention))
	return removed, wrapError(err)
}

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
	return ExportContextWithOptions(ctx, dbPath, format, outputPath, ExportOptions{})
}

func ExportContextWithOptions(ctx context.Context, dbPath string, format ExportFormat, outputPath string, opts ExportOptions) ([]byte, error) {
	db, err := OpenContext(ctx, dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return db.ExportContextWithOptions(ctx, format, outputPath, opts)
}

func ExportFile(dbPath string, format ExportFormat, outputPath string) error {
	return ExportFileContext(context.Background(), dbPath, format, outputPath)
}

func ExportFileContext(ctx context.Context, dbPath string, format ExportFormat, outputPath string) error {
	return ExportFileContextWithOptions(ctx, dbPath, format, outputPath, ExportOptions{})
}

func ExportFileContextWithOptions(ctx context.Context, dbPath string, format ExportFormat, outputPath string, opts ExportOptions) error {
	db, err := OpenContext(ctx, dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.ExportFileContextWithOptions(ctx, format, outputPath, opts)
}

func Dump(dbPath string) ([]byte, error) {
	return DumpContext(context.Background(), dbPath)
}

func DumpContext(ctx context.Context, dbPath string) ([]byte, error) {
	return DumpContextWithOptions(ctx, dbPath, ExportOptions{})
}

func DumpContextWithOptions(ctx context.Context, dbPath string, opts ExportOptions) ([]byte, error) {
	db, err := OpenContext(ctx, dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return db.DumpContextWithOptions(ctx, opts)
}

func (db *DB) Export(format ExportFormat, outputPath string) ([]byte, error) {
	return db.ExportContext(context.Background(), format, outputPath)
}

func (db *DB) ExportContext(ctx context.Context, format ExportFormat, outputPath string) ([]byte, error) {
	return db.ExportContextWithOptions(ctx, format, outputPath, ExportOptions{})
}

func (db *DB) ExportContextWithOptions(ctx context.Context, format ExportFormat, outputPath string, opts ExportOptions) ([]byte, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	graph, lease, err := inner.SnapshotGraph()
	if err != nil {
		return nil, wrapError(err)
	}
	defer lease.Release()
	data, err := exporter.ExportGraphContextWithOptions(ctx, graph, exporter.ExportFormat(format), outputPath, exporter.ExportOptions(opts))
	return data, wrapError(err)
}

func (db *DB) ExportFile(format ExportFormat, outputPath string) error {
	return db.ExportFileContext(context.Background(), format, outputPath)
}

func (db *DB) ExportFileContext(ctx context.Context, format ExportFormat, outputPath string) error {
	return db.ExportFileContextWithOptions(ctx, format, outputPath, ExportOptions{})
}

func (db *DB) ExportFileContextWithOptions(ctx context.Context, format ExportFormat, outputPath string, opts ExportOptions) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	graph, lease, err := inner.SnapshotGraph()
	if err != nil {
		return wrapError(err)
	}
	defer lease.Release()
	return wrapError(exporter.ExportGraphFileContextWithOptions(ctx, graph, exporter.ExportFormat(format), outputPath, exporter.ExportOptions(opts)))
}

func (db *DB) Dump() ([]byte, error) {
	return db.DumpContext(context.Background())
}

func (db *DB) DumpContext(ctx context.Context) ([]byte, error) {
	return db.DumpContextWithOptions(ctx, ExportOptions{})
}

func (db *DB) DumpContextWithOptions(ctx context.Context, opts ExportOptions) ([]byte, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	graph, lease, err := inner.SnapshotGraph()
	if err != nil {
		return nil, wrapError(err)
	}
	defer lease.Release()
	data, err := exporter.DumpGraphContextWithOptions(ctx, graph, exporter.ExportOptions(opts))
	return data, wrapError(err)
}

func (db *DB) DumpTo(output io.Writer) error {
	return db.DumpToContext(context.Background(), output)
}

// DumpToContext observes cancellation between writes. It cannot interrupt an
// output writer that is itself blocked in Write.
func (db *DB) DumpToContext(ctx context.Context, output io.Writer) error {
	return db.DumpToContextWithOptions(ctx, output, ExportOptions{})
}

// DumpToContextWithOptions can leave already-written bytes in output when a
// limit, cancellation, or writer error occurs; arbitrary writers cannot roll
// those bytes back.
func (db *DB) DumpToContextWithOptions(ctx context.Context, output io.Writer, opts ExportOptions) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	graph, lease, err := inner.SnapshotGraph()
	if err != nil {
		return wrapError(err)
	}
	defer lease.Release()
	return wrapError(exporter.DumpGraphContextToWithOptions(ctx, graph, output, exporter.ExportOptions(opts)))
}

func (db *DB) ExportTo(format ExportFormat, output io.Writer) error {
	return db.ExportToContext(context.Background(), format, output)
}

// ExportToContext observes cancellation between writes. It cannot interrupt an
// output writer that is itself blocked in Write.
func (db *DB) ExportToContext(ctx context.Context, format ExportFormat, output io.Writer) error {
	return db.ExportToContextWithOptions(ctx, format, output, ExportOptions{})
}

// ExportToContextWithOptions can leave already-written bytes in output when a
// limit, cancellation, or writer error occurs; arbitrary writers cannot roll
// those bytes back.
func (db *DB) ExportToContextWithOptions(ctx context.Context, format ExportFormat, output io.Writer, opts ExportOptions) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	graph, lease, err := inner.SnapshotGraph()
	if err != nil {
		return wrapError(err)
	}
	defer lease.Release()
	return wrapError(exporter.ExportGraphContextToWithOptions(ctx, graph, exporter.ExportFormat(format), output, exporter.ExportOptions(opts)))
}

func SimulateCrash(dbPath string) error {
	return exporter.SimulateCrash(dbPath)
}
