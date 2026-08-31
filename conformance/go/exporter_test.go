package conformance

import "testing"

type ExportFormat string

const (
	ExportFormatJSON  ExportFormat = "json"
	ExportFormatJSONL ExportFormat = "jsonl"
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatDOT   ExportFormat = "dot"
)

type Exporter interface {
	Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error)
	Dump(dbPath string) ([]byte, error)
}

var testExporter Exporter

func currentExporter(t *testing.T) Exporter {
	t.Helper()
	if testExporter == nil {
		t.Fatal("conformance exporter adapter not configured")
	}
	return testExporter
}
