package main

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
)

func gateFixture(overrides map[string]float64) result {
	var output strings.Builder
	seen := map[string]bool{}
	for _, gate := range blockingGates {
		if seen[gate.benchmark] {
			continue
		}
		seen[gate.benchmark] = true
		fmt.Fprintf(&output, "%s-2 1", gate.benchmark)
		units := map[string]bool{}
		for _, candidate := range blockingGates {
			if candidate.benchmark == gate.benchmark {
				units[candidate.unit] = true
			}
		}
		for unit := range units {
			value := 100.0
			if override, ok := overrides[gate.benchmark+" "+unit]; ok {
				value = override
			}
			fmt.Fprintf(&output, " %.0f %s", value, unit)
		}
		output.WriteByte('\n')
	}
	parsed, err := parse(strings.NewReader(output.String()))
	if err != nil {
		panic(err)
	}
	return parsed
}

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
	for _, want := range []string{"`BenchmarkLookup`", "| 110 | 100 | +10.0% | 8 | 16 | -50.0% | 1 | 2 | -50.0% |", "ns/op is informational because shared-runner latency is noisy"} {
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

func TestParseZigRejectsInvalidValues(t *testing.T) {
	for _, output := range []string{
		"│  100000 │          NaN │      500.00 │      700.00 │    98.0%  │        42.0 │\n",
		"│  100000 │         +Inf │      500.00 │      700.00 │    98.0%  │        42.0 │\n",
		"│  100000 │     1000.00 │     -500.00 │      700.00 │    98.0%  │        42.0 │\n",
		"│  100000 │     1000.00 │      500.00 │      700.00 │   100.1%  │        42.0 │\n",
	} {
		if _, err := parseZig(strings.NewReader(output)); err == nil {
			t.Fatalf("parseZig accepted invalid output: %q", output)
		}
	}
}

func TestValidateGoResultRequiresBenchmarkSuiteSentinels(t *testing.T) {
	for name, input := range map[string]string{
		"garbage": "Benchmark garbage 1 foo\n",
		"partial": "BenchmarkReadRequests/query-8 1 100 ns/op\n",
	} {
		benchmarks, err := parse(strings.NewReader(input))
		if err != nil {
			t.Fatalf("%s parse: %v", name, err)
		}
		if err := validateGoResult(benchmarks); err == nil {
			t.Fatalf("%s result passed validation", name)
		}
	}

	benchmarks, err := parse(strings.NewReader("BenchmarkReadRequests/query-8 1 100 ns/op\nBenchmarkCheckpoint-8 1 100 ns/op\nBenchmarkColdOpen-8 1 100 ns/op\nBenchmarkReaderDuringCommit-8 1 100 ns/op\nBenchmarkFTSSearchScaling/records_100000/fuzzy_rare-8 1 100 ns/op\nBenchmarkVectorSearchScaling/records_100000-8 1 100 ns/op\nBenchmarkVectorSearchANNFallback10K-8 1 100 ns/op\nBenchmarkVectorSearchClustered128D/100K-8 1 200 ns/op 300 index-build-ms 99 recall@10\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGoResult(benchmarks); err != nil {
		t.Fatalf("full result failed validation: %v", err)
	}
}

func TestParseRejectsInvalidGoMetrics(t *testing.T) {
	for _, metric := range []string{"NaN", "+Inf", "-1"} {
		if _, err := parse(strings.NewReader("BenchmarkReadRequests/query-8 1 " + metric + " B/op\n")); err == nil {
			t.Fatalf("parse accepted invalid metric %q", metric)
		}
	}
}

func TestCheckGatesRejectsInvalidMetrics(t *testing.T) {
	for _, metric := range []float64{math.NaN(), math.Inf(1), -1} {
		current := gateFixture(nil)
		current["BenchmarkReadRequests/query"]["B/op"] = []float64{metric}
		if err := checkGates(current, gateFixture(nil), new(bytes.Buffer)); err == nil {
			t.Fatalf("checkGates accepted invalid metric %v", metric)
		}
	}
}

func TestCheckGatesRejectsAllocationRegressions(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(map[string]float64{
		"BenchmarkReadRequests/query B/op":             102,
		"BenchmarkReadRequests/write_commit allocs/op": 103,
	})
	var diagnostics bytes.Buffer
	err := checkGates(current, previous, &diagnostics)
	if err == nil || !strings.Contains(err.Error(), "BenchmarkReadRequests/query B/op") || !strings.Contains(err.Error(), "write_commit allocs/op") {
		t.Fatalf("checkGates error = %v, want allocation failures", err)
	}
}

func TestCheckGatesAllowsTwoAllocationDriftForWrites(t *testing.T) {
	previous := gateFixture(nil)
	previous["BenchmarkReadRequests/write_commit"]["allocs/op"] = []float64{81}
	previous["BenchmarkSingleRecordCommitScaling/nodes_100000/direct"]["allocs/op"] = []float64{81}
	current := gateFixture(map[string]float64{
		"BenchmarkReadRequests/write_commit allocs/op":                     83,
		"BenchmarkSingleRecordCommitScaling/nodes_100000/direct allocs/op": 83,
	})
	if err := checkGates(current, previous, new(bytes.Buffer)); err != nil {
		t.Fatalf("two allocation drift failed gate: %v", err)
	}

	current["BenchmarkReadRequests/write_commit"]["allocs/op"] = []float64{84}
	current["BenchmarkSingleRecordCommitScaling/nodes_100000/direct"]["allocs/op"] = []float64{84}
	if err := checkGates(current, previous, new(bytes.Buffer)); err == nil ||
		!strings.Contains(err.Error(), "write_commit allocs/op") ||
		!strings.Contains(err.Error(), "nodes_100000/direct allocs/op") ||
		!strings.Contains(err.Error(), "limit +2.0 allocations") {
		t.Fatalf("three allocation drift error = %v, want write allocation failures with absolute limit", err)
	}
}

func TestCheckGatesAllowsStableOrLowerAllocations(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(map[string]float64{
		"BenchmarkReadRequests/query B/op":      100,
		"BenchmarkReadRequests/query allocs/op": 99,
	})
	if err := checkGates(current, previous, new(bytes.Buffer)); err != nil {
		t.Fatalf("stable or lower allocation metrics failed gate: %v", err)
	}
}

func TestCheckGatesAllowsOnePercentBytesButRejectsMore(t *testing.T) {
	previous := gateFixture(nil)
	for name, bytesPerOp := range map[string]float64{
		"one percent":      101,
		"over one percent": 102,
	} {
		current := gateFixture(map[string]float64{"BenchmarkReadRequests/query B/op": bytesPerOp})
		err := checkGates(current, previous, new(bytes.Buffer))
		if name == "one percent" && err != nil {
			t.Fatalf("1%% B/op drift failed gate: %v", err)
		}
		if name == "over one percent" && (err == nil || !strings.Contains(err.Error(), "BenchmarkReadRequests/query B/op")) {
			t.Fatalf("greater than 1%% B/op drift error = %v", err)
		}
	}
}

func TestCheckGatesTreatsMultiHopLatencyAsInformational(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(nil)
	current["BenchmarkQueryMultiHopSlots"]["ns/op"] = []float64{1000}
	previous["BenchmarkQueryMultiHopSlots"]["ns/op"] = []float64{100}
	if err := checkGates(current, previous, new(bytes.Buffer)); err != nil {
		t.Fatalf("multi-hop latency regression was incorrectly gated: %v", err)
	}
}

func TestCheckGatesKeepsMultiHopAllocationsBlocking(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(map[string]float64{"BenchmarkQueryMultiHopSlots allocs/op": 101})
	current["BenchmarkQueryMultiHopSlots"]["ns/op"] = []float64{1000}
	if err := checkGates(current, previous, new(bytes.Buffer)); err == nil || !strings.Contains(err.Error(), "BenchmarkQueryMultiHopSlots allocs/op") {
		t.Fatalf("multi-hop allocation regression was not gated: %v", err)
	}
}

func TestCheckGatesReportsButDoesNotBlockWALLatency(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(map[string]float64{
		"BenchmarkLoadLatestWALV2/delta_history/256 allocs/op": 101,
	})
	current["BenchmarkLoadLatestWALV2/delta_history/256"]["ns/op"] = []float64{200}

	err := checkGates(current, previous, new(bytes.Buffer))
	if err == nil || !strings.Contains(err.Error(), "BenchmarkLoadLatestWALV2/delta_history/256 allocs/op") {
		t.Fatalf("WAL allocation regression was not gated: %v", err)
	}
	if strings.Contains(err.Error(), "ns/op") {
		t.Fatalf("WAL latency regression was incorrectly gated: %v", err)
	}
}

func TestCheckGatesSkipsNewRowsUntilBaselineExists(t *testing.T) {
	var diagnostics bytes.Buffer
	if err := checkGates(gateFixture(nil), result{}, &diagnostics); err != nil {
		t.Fatalf("new benchmark without baseline failed gate: %v", err)
	}
	if !strings.Contains(diagnostics.String(), "no compatible baseline") {
		t.Fatalf("diagnostics = %q, want baseline skip", diagnostics.String())
	}
}
