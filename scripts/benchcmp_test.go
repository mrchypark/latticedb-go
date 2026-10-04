package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
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

func diskBaselineFixture() result {
	input := strings.Join([]string{
		"BenchmarkReadRequests/query-8 1 200 ns/op 200 B/op 20 allocs/op",
		"BenchmarkReadRequests/write_commit-8 1 300 ns/op 300 B/op 30 allocs/op",
		"BenchmarkSingleRecordCommitScaling/nodes_100000/direct-8 1 400 ns/op 400 B/op 40 allocs/op",
	}, "\n")
	parsed, err := parse(strings.NewReader(input))
	if err != nil {
		panic(err)
	}
	return parsed
}

func cloneMetricSet(metrics map[string][]float64) map[string][]float64 {
	cloned := make(map[string][]float64, len(metrics))
	for unit, values := range metrics {
		cloned[unit] = append([]float64(nil), values...)
	}
	return cloned
}

func TestValidateDiskBaselineRequiresAllThreeMetrics(t *testing.T) {
	baseline := diskBaselineFixture()
	if err := validateDiskBaseline(baseline); err != nil {
		t.Fatalf("complete disk baseline rejected: %v", err)
	}
	delete(baseline[diskBaselineBenchmarks[1]], "allocs/op")
	if err := validateDiskBaseline(baseline); err == nil || !strings.Contains(err.Error(), diskBaselineBenchmarks[1]+" missing or invalid allocs/op") {
		t.Fatalf("missing disk metric error = %v", err)
	}
	if _, err := parse(strings.NewReader("BenchmarkReadRequests/query-8 1 NaN ns/op 200 B/op 20 allocs/op\n")); err == nil {
		t.Fatal("invalid disk metric parsed")
	}
}

func TestDiskBaselineReplacesOnlySelectedComparisonSources(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(nil)
	disk := diskBaselineFixture()
	for _, benchmark := range diskBaselineBenchmarks {
		current[benchmark] = cloneMetricSet(disk[benchmark])
	}
	current[diskBaselineBenchmarks[0]]["B/op"] = []float64{201}
	if err := checkGates(current, withDiskBaseline(previous, disk), new(bytes.Buffer)); err != nil {
		t.Fatalf("disk baseline was not used for selected query metric: %v", err)
	}

	current[diskBaselineBenchmarks[0]]["B/op"] = []float64{203}
	if err := checkGates(current, withDiskBaseline(previous, disk), new(bytes.Buffer)); err == nil || !strings.Contains(err.Error(), diskBaselineBenchmarks[0]+" B/op") {
		t.Fatalf("disk regression was not checked: %v", err)
	}

	current = gateFixture(nil)
	for _, benchmark := range diskBaselineBenchmarks {
		current[benchmark] = cloneMetricSet(disk[benchmark])
	}
	current["BenchmarkQueryMultiHopSlots"]["B/op"] = []float64{102}
	if err := checkGates(current, withDiskBaseline(previous, disk), new(bytes.Buffer)); err == nil || !strings.Contains(err.Error(), "BenchmarkQueryMultiHopSlots B/op") {
		t.Fatalf("non-disk memory gate changed when disk baseline was provided: %v", err)
	}
}

func TestDiskBaselineReportIdentifiesSourceAndValues(t *testing.T) {
	current := diskBaselineFixture()
	previous := gateFixture(nil)
	current[diskBaselineBenchmarks[0]]["B/op"] = []float64{210}
	var report bytes.Buffer
	writeReport(&report, current, previous, "candidate", "memory-reference", nil, "", false, diskBaselineInput{
		metrics: diskBaselineFixture(), source: "baseline.txt", label: "f3518b7 run36258284773",
	})
	for _, want := range []string{
		"Disk baseline for `BenchmarkReadRequests/query`, `BenchmarkReadRequests/write_commit`, and `BenchmarkSingleRecordCommitScaling/nodes_100000/direct`: `f3518b7 run36258284773` (input `baseline.txt`)",
		"| `BenchmarkReadRequests/query` | 1 / 1 | 200 | 200 | +0.0% | 210 | 200 | +5.0% |",
	} {
		if !strings.Contains(report.String(), want) {
			t.Fatalf("disk baseline report does not contain %q:\n%s", want, report.String())
		}
	}
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
	writeReport(&report, current, previous, "head", "base", nil, "", false)
	for _, want := range []string{"`BenchmarkLookup`", "| `BenchmarkLookup` | 3 / 1 | 110 | 100 | +10.0% | 8 | 16 | -50.0% | 1 | 2 | -50.0% |", "sample counts are current / previous", "ns/op is informational because shared-runner latency is noisy"} {
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
	writeReport(&report, current, result{}, "head", "base", zig, "upstream@abc", true)
	for _, want := range []string{
		"## pure-Go vs Zig reference (100K)",
		"| Index build / insert (ms) | 1234 | 1000 | +23.4% |",
		"| Mean search (ns) | 800000 | 500000 | +60.0% |",
		"| Recall@10 | 99.0% | 98.0% | +1.0 pp |",
		"Zig reports 42.0 MB",
		"Both are measured in the same CI run",
	} {
		if !strings.Contains(report.String(), want) {
			t.Fatalf("report does not contain %q:\n%s", want, report.String())
		}
	}

	report.Reset()
	writeReport(&report, current, result{}, "head", "base", zig, "upstream@abc", false)
	if !strings.Contains(report.String(), "The Zig result was not measured in this CI run") {
		t.Fatalf("reused Zig report omitted provenance:\n%s", report.String())
	}
}

func TestReportShowsRowSamplesAndZigProvenance(t *testing.T) {
	current, err := parse(strings.NewReader("BenchmarkVectorSearchClustered128D/1K-8 1 100 ns/op\nBenchmarkVectorSearchClustered128D/10K-8 1 200 ns/op\nBenchmarkVectorSearchClustered128D/100K-8 1 300 ns/op\n"))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := parse(strings.NewReader("BenchmarkVectorSearchClustered128D/1K-8 1 90 ns/op\nBenchmarkVectorSearchClustered128D/10K-8 1 190 ns/op\nBenchmarkVectorSearchClustered128D/100K-8 1 290 ns/op\n"))
	if err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	writeReport(&report, current, previous, "head", "base", nil, "", false)
	for _, want := range []string{
		"| `BenchmarkVectorSearchClustered128D/1K` | 1 / 1 |",
		"| `BenchmarkVectorSearchClustered128D/10K` | 1 / 1 |",
		"| `BenchmarkVectorSearchClustered128D/100K` | 1 / 1 |",
		"Values are medians of the samples shown in each row",
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
	if err := checkGates(current, previous, new(bytes.Buffer)); err != nil {
		t.Fatalf("measured one-allocation WAL drift was blocked: %v", err)
	}
	current["BenchmarkLoadLatestWALV2/delta_history/256"]["allocs/op"] = []float64{102}

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

func TestSourceAdmissionContractTransitionIsBoundedAndOneTime(t *testing.T) {
	previous := gateFixture(nil)
	current := gateFixture(map[string]float64{"BenchmarkReadRequests/query B/op": 1380, "BenchmarkReadRequests/query allocs/op": 120})
	current["BenchmarkReadRequests/query"]["source-admission-contract"] = []float64{1, 1}
	if err := checkGates(current, previous, new(bytes.Buffer)); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []string{"B/op", "allocs/op"} {
		current["BenchmarkReadRequests/query"][unit][0]++
		if err := checkGates(current, previous, new(bytes.Buffer)); err == nil {
			t.Fatalf("unbounded transition %s", unit)
		}
		current["BenchmarkReadRequests/query"][unit][0]--
	}
	// A versioned main baseline immediately restores the original strict gates.
	previous["BenchmarkReadRequests/query"]["source-admission-contract"] = []float64{1}
	if err := checkGates(current, previous, new(bytes.Buffer)); err == nil {
		t.Fatal("migration allowance reapplied within contract 1")
	}
	current["BenchmarkReadRequests/query"]["B/op"] = []float64{100}
	current["BenchmarkReadRequests/query"]["allocs/op"] = []float64{100}
	if err := checkGates(current, previous, new(bytes.Buffer)); err != nil {
		t.Fatal(err)
	}
	delete(current["BenchmarkReadRequests/query"], "source-admission-contract")
	if err := checkGates(current, previous, new(bytes.Buffer)); err == nil {
		t.Fatal("marker removal bypassed admission gates")
	}
	for _, samples := range [][]float64{{0}, {2}, {1, 0}, {math.NaN()}} {
		current["BenchmarkReadRequests/query"]["source-admission-contract"] = samples
		if err := checkGates(current, previous, new(bytes.Buffer)); err == nil {
			t.Fatalf("accepted invalid contract %v", samples)
		}
	}
}

func TestParseSourceAdmissionMarkerPerSample(t *testing.T) {
	legacy := "BenchmarkReadRequests/query-2 1 100 ns/op 3500 B/op 60 allocs/op"
	admitted := legacy + " 1 source-admission-contract"
	for _, input := range []string{legacy + "\n" + legacy, admitted + "\n" + admitted} {
		if _, err := parse(strings.NewReader(input)); err != nil {
			t.Fatalf("valid samples: %v", err)
		}
	}
	for _, input := range []string{
		legacy + " broken source-admission-contract",
		legacy + " 0 source-admission-contract",
		legacy + " 2 source-admission-contract",
		legacy + " 1.5 source-admission-contract",
		legacy + " NaN source-admission-contract",
		legacy + " +Inf source-admission-contract",
		admitted + " 1 source-admission-contract",
		admitted + "\n" + legacy,
		legacy + "\n" + admitted,
		admitted + " 1 source-admission-contract\n" + legacy,
		legacy + " source-admission-contract",
	} {
		if _, err := parse(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted malformed/mixed sample: %s", input)
		}
	}
}

func TestSourceAdmissionWorkflowCLIUsesCompatibleGateBaseline(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "benchcmp")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	if output, err := exec.Command("go", "build", "-o", executable, "benchcmp.go").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v %s", err, output)
	}
	text := func(metrics result) string {
		names := make([]string, 0, len(metrics))
		for name := range metrics {
			names = append(names, name)
		}
		sort.Strings(names)
		var out strings.Builder
		for _, name := range names {
			fmt.Fprintf(&out, "%s-2 1 100 ns/op", name)
			units := make([]string, 0, len(metrics[name]))
			for unit := range metrics[name] {
				if unit != "ns/op" {
					units = append(units, unit)
				}
			}
			sort.Strings(units)
			for _, unit := range units {
				v, _ := value(metrics[name], unit)
				fmt.Fprintf(&out, " %g %s", v, unit)
			}
			out.WriteByte('\n')
		}
		return out.String()
	}
	disk := diskBaselineFixture()
	previous := gateFixture(nil)
	current := gateFixture(nil)
	for _, name := range diskBaselineBenchmarks {
		current[name] = cloneMetricSet(disk[name])
	}
	write := func(name, content string) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	diskPath := write("disk.txt", text(disk))
	run := func(previousText, currentText string, passes bool) {
		t.Helper()
		output, err := exec.Command(executable, "-current", write("current.txt", currentText), "-previous", write("previous.txt", previousText), "-disk-baseline", diskPath, "-check").CombinedOutput()
		if (err == nil) != passes {
			t.Fatalf("passes=%v err=%v output=%s", passes, err, output)
		}
	}
	query := "BenchmarkReadRequests/query"
	current[query] = map[string][]float64{"B/op": {1480}, "allocs/op": {40}, "source-admission-contract": {1}}
	run(text(previous), text(current), true)
	current[query]["B/op"][0]++
	run(text(previous), text(current), false)
	current[query]["B/op"][0]--
	current[query]["allocs/op"][0]++
	run(text(previous), text(current), false)
	previous[query] = map[string][]float64{"B/op": {3500}, "allocs/op": {60}, "source-admission-contract": {1}}
	current[query] = map[string][]float64{"B/op": {3600}, "allocs/op": {65}, "source-admission-contract": {1}}
	// These figures fit the legacy migration ceiling, but regress admitted main.
	run(text(previous), text(current), false)
	current[query]["B/op"] = []float64{3535}
	current[query]["allocs/op"] = []float64{60}
	run(text(previous), text(current), true)
	current[query]["B/op"][0]++
	run(text(previous), text(current), false)
	current[query]["B/op"] = []float64{3500}
	current[query]["allocs/op"][0]++
	run(text(previous), text(current), false)
	current[query] = map[string][]float64{"B/op": {10}, "allocs/op": {10}}
	run(text(previous), text(current), false)
	current[query] = cloneMetricSet(previous[query])
	run(text(previous), text(current), true)
	legacy := "BenchmarkReadRequests/query-2 1 100 ns/op 3500 B/op 60 allocs/op\n"
	run(text(previous)+legacy, text(current), false)
	run(text(previous), text(current)+legacy, false)
	run(strings.Replace(text(previous), "1 source-admission-contract", "broken source-admission-contract", 1), text(current), false)
	delete(previous[query], "B/op")
	run(text(previous), text(current), false)
	// Reporting still compares against the archived disk fixture, not gate main.
	reportPath := filepath.Join(directory, "report.md")
	if output, err := exec.Command(executable, "-current", write("current.txt", text(current)), "-previous", write("previous.txt", text(gateFixture(nil))), "-disk-baseline", diskPath, "-output", reportPath).CombinedOutput(); err != nil {
		t.Fatalf("report CLI: %v %s", err, output)
	}
	report, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(report), "| `BenchmarkReadRequests/query` | 1 / 1 | 100 | 100 | +0.0% | 3500 | 200 | +1650.0% |") {
		t.Fatalf("historical report changed: %s", report)
	}
}
