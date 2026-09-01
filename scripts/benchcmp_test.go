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
	writeReport(&report, current, previous, "head", "base", nil, "")
	for _, want := range []string{"`BenchmarkLookup`", "| 110 | 100 | +10.0% | 8 | 16 | -50.0% | 1 | 2 | -50.0% |"} {
		if !strings.Contains(report.String(), want) {
			t.Fatalf("report does not contain %q:\n%s", want, report.String())
		}
	}
}

func TestReportComparesPureGoWithZig100K(t *testing.T) {
	current, err := parse(strings.NewReader("BenchmarkVectorSearchZigHarness/100K-8 1 900000 ns/op 1234 index-build-ms 800000 mean-ns 1100000 p99-ns 99 recall@10\n"))
	if err != nil {
		t.Fatal(err)
	}
	zig, err := parseZig(strings.NewReader("│  100000 │     1000.00 │      500.00 │      700.00 │    98.0%  │        42.0 │\n"))
	if err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	writeReport(&report, current, result{}, "head", "base", zig, "upstream@abc")
	for _, want := range []string{
		"## pure-Go vs Zig reference (100K)",
		"| Index build / insert (ms) | 1234 | 1000 | +23.4% |",
		"| Mean search (ns) | 800000 | 500000 | +60.0% |",
		"| Recall@10 | 99.0% | 98.0% | +1.0 pp |",
		"Zig reports 42.0 MB",
	} {
		if !strings.Contains(report.String(), want) {
			t.Fatalf("report does not contain %q:\n%s", want, report.String())
		}
	}
}
