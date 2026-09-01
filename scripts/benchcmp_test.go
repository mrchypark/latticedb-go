package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestReportUsesMediansAndComparesMetrics(t *testing.T) {
	current, err := parse(strings.NewReader("BenchmarkLookup-8 1 120 ns/op 8 B/op 1 allocs/op\nBenchmarkLookup-8 1 100 ns/op 8 B/op 1 allocs/op\nBenchmarkLookup-8 1 110 ns/op 8 B/op 1 allocs/op\n"))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := parse(strings.NewReader("BenchmarkLookup-4 1 100 ns/op 16 B/op 2 allocs/op\n"))
	if err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	writeReport(&report, current, previous, "head", "base")
	for _, want := range []string{"`BenchmarkLookup`", "| 110 | 100 | +10.0% | 8 | 16 | -50.0% | 1 | 2 | -50.0% |"} {
		if !strings.Contains(report.String(), want) {
			t.Fatalf("report does not contain %q:\n%s", want, report.String())
		}
	}
}
