package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type result map[string]map[string][]float64

type zigResult struct {
	insertMS float64
	meanNS   float64
	p99NS    float64
	recall   float64
	memoryMB float64
}

type performanceGate struct {
	benchmark       string
	unit            string
	maxRise         float64
	maxAbsoluteRise float64
}

// Keep blocking gates to stable graph-core metrics. Most latency remains
// informational because shared-runner noise makes it unsuitable as a blocker;
// WAL loading is especially sensitive to filesystem/cache variance.
var blockingGates = []performanceGate{
	{benchmark: "BenchmarkReadRequests/query", unit: "B/op", maxRise: 0.01},
	{benchmark: "BenchmarkReadRequests/query", unit: "allocs/op"},
	{benchmark: "BenchmarkReadRequests/write_commit", unit: "B/op", maxRise: 0.01},
	// Write paths permit up to two measured allocation drift; larger changes block.
	{benchmark: "BenchmarkReadRequests/write_commit", unit: "allocs/op", maxAbsoluteRise: 2},
	{benchmark: "BenchmarkSingleRecordCommitScaling/nodes_100000/direct", unit: "B/op", maxRise: 0.01},
	{benchmark: "BenchmarkSingleRecordCommitScaling/nodes_100000/direct", unit: "allocs/op", maxAbsoluteRise: 2},
	{benchmark: "BenchmarkQueryMultiHopSlots", unit: "B/op", maxRise: 0.01},
	{benchmark: "BenchmarkQueryMultiHopSlots", unit: "allocs/op"},
	{benchmark: "BenchmarkAdjacencyReadScaling/chunked_10000", unit: "B/op", maxRise: 0.01},
	{benchmark: "BenchmarkAdjacencyReadScaling/chunked_10000", unit: "allocs/op"},
	{benchmark: "BenchmarkAdjacencyAppendScaling/chunked_10000", unit: "B/op", maxRise: 0.01},
	{benchmark: "BenchmarkAdjacencyAppendScaling/chunked_10000", unit: "allocs/op"},
	{benchmark: "BenchmarkLoadLatestWALV2/delta_history/256", unit: "B/op", maxRise: 0.01},
	{benchmark: "BenchmarkLoadLatestWALV2/delta_history/256", unit: "allocs/op"},
}

var cpuSuffix = regexp.MustCompile(`-\d+$`)

func parse(r io.Reader) (result, error) {
	results := result{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		name := cpuSuffix.ReplaceAllString(fields[0], "")
		name = strings.Replace(name, "BenchmarkVectorSearchZigHarness", "BenchmarkVectorSearchClustered128D", 1)
		name = strings.Replace(name, "BenchmarkVectorIndexBuildZigHarness", "BenchmarkVectorIndexBuildClustered128D", 1)
		if results[name] == nil {
			results[name] = map[string][]float64{}
		}
		for i := 2; i+1 < len(fields); i += 2 {
			value, err := strconv.ParseFloat(fields[i], 64)
			if err == nil {
				if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
					return nil, fmt.Errorf("benchmark %s %s: invalid metric %q", name, fields[i+1], fields[i])
				}
				results[name][fields[i+1]] = append(results[name][fields[i+1]], value)
			}
		}
	}
	return results, scanner.Err()
}

func parseZig(r io.Reader) (*zigResult, error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "│")
		if len(parts) != 8 || strings.TrimSpace(parts[1]) != "100000" {
			continue
		}
		values := make([]float64, 5)
		for i, part := range parts[2:7] {
			value, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(part), "%"), 64)
			if err != nil {
				return nil, fmt.Errorf("parse Zig 100K column %d: %w", i+2, err)
			}
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				return nil, fmt.Errorf("parse Zig 100K column %d: invalid value", i+2)
			}
			values[i] = value
		}
		if values[3] > 100 {
			return nil, fmt.Errorf("parse Zig 100K recall outside 0..100")
		}
		return &zigResult{insertMS: values[0], meanNS: values[1] * 1e3, p99NS: values[2] * 1e3, recall: values[3], memoryMB: values[4]}, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("Zig 100K result not found")
}

func median(values []float64) float64 {
	values = slices.Clone(values)
	slices.Sort(values)
	mid := len(values) / 2
	if len(values)%2 == 0 {
		return (values[mid-1] + values[mid]) / 2
	}
	return values[mid]
}

func change(current, previous float64) string {
	if previous == 0 {
		return "—"
	}
	return fmt.Sprintf("%+.1f%%", (current/previous-1)*100)
}

func value(metrics map[string][]float64, unit string) (float64, bool) {
	values, ok := metrics[unit]
	if !ok || len(values) == 0 {
		return 0, false
	}
	v := median(values)
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, false
	}
	return v, true
}

func display(metrics map[string][]float64, unit string) string {
	v, ok := value(metrics, unit)
	if !ok {
		return "—"
	}
	if v < 10 && v != float64(int64(v)) {
		return fmt.Sprintf("%.2f", v)
	}
	return fmt.Sprintf("%.0f", v)
}

func delta(current, previous map[string][]float64, unit string) string {
	c, cok := value(current, unit)
	p, pok := value(previous, unit)
	if !cok || !pok {
		return "—"
	}
	return change(c, p)
}

func checkGates(current, previous result, stderr io.Writer) error {
	var failures []string
	for _, gate := range blockingGates {
		currentValue, currentOK := value(current[gate.benchmark], gate.unit)
		if !currentOK {
			failures = append(failures, fmt.Sprintf("%s %s missing from current result", gate.benchmark, gate.unit))
			continue
		}
		previousValue, previousOK := value(previous[gate.benchmark], gate.unit)
		if !previousOK {
			fmt.Fprintf(stderr, "performance gate skipped: %s %s has no compatible baseline\n", gate.benchmark, gate.unit)
			continue
		}
		if math.IsNaN(currentValue) || math.IsInf(currentValue, 0) || currentValue < 0 || math.IsNaN(previousValue) || math.IsInf(previousValue, 0) || previousValue < 0 {
			failures = append(failures, fmt.Sprintf("%s %s has invalid metric values", gate.benchmark, gate.unit))
			continue
		}
		if gate.maxAbsoluteRise > 0 {
			if currentValue > previousValue+gate.maxAbsoluteRise {
				failures = append(failures, fmt.Sprintf("%s %s regressed %.1f allocations (limit +%.1f allocations)", gate.benchmark, gate.unit, currentValue-previousValue, gate.maxAbsoluteRise))
			}
		} else if currentValue > previousValue*(1+gate.maxRise) {
			failures = append(failures, fmt.Sprintf("%s %s regressed %.1f%% (limit +%.1f%%)", gate.benchmark, gate.unit, (currentValue/previousValue-1)*100, gate.maxRise*100))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("performance gates failed:\n- %s", strings.Join(failures, "\n- "))
}

func otherMetrics(current, previous map[string][]float64) string {
	units := make([]string, 0, len(current))
	for unit := range current {
		if unit != "ns/op" && unit != "B/op" && unit != "allocs/op" {
			units = append(units, unit)
		}
	}
	slices.Sort(units)
	parts := make([]string, 0, len(units))
	for _, unit := range units {
		part := display(current, unit)
		if _, ok := value(previous, unit); ok {
			part += " / " + display(previous, unit)
		}
		part += " " + unit
		if d := delta(current, previous, unit); d != "—" {
			part += " (" + d + ")"
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, ", ")
}

func writeZigComparison(w io.Writer, current result, zig *zigResult, zigLabel string) {
	goMetrics := current["BenchmarkVectorSearchClustered128D/100K"]
	if goMetrics == nil {
		return
	}
	fmt.Fprintln(w, "\n## pure-Go vs Zig reference (100K)")
	fmt.Fprintf(w, "\nZig reference: `%s`\n", zigLabel)
	fmt.Fprintln(w, "\n| Metric | pure-Go | Zig | pure-Go vs Zig |")
	fmt.Fprintln(w, "|---|---:|---:|---:|")
	rows := []struct {
		label string
		unit  string
		zig   float64
	}{
		{"Index build / insert (ms)", "index-build-ms", zig.insertMS},
		{"Mean search (ns)", "mean-ns", zig.meanNS},
		{"P99 search (ns)", "p99-ns", zig.p99NS},
	}
	for _, row := range rows {
		goValue, ok := value(goMetrics, row.unit)
		if !ok {
			continue
		}
		fmt.Fprintf(w, "| %s | %.0f | %.0f | %s |\n", row.label, goValue, row.zig, change(goValue, row.zig))
	}
	if goRecall, ok := value(goMetrics, "recall@10"); ok {
		fmt.Fprintf(w, "| Recall@10 | %.1f%% | %.1f%% | %+.1f pp |\n", goRecall, zig.recall, goRecall-zig.recall)
	}
	fmt.Fprintf(w, "\nPositive latency/build Δ means pure-Go is slower. Zig reports %.1f MB of index memory; it is not compared with Go B/op because they measure different things. Both run on the same CI runner with the same 128-D clustered workload and HNSW parameters, but the Zig benchmark includes its storage layer.\n", zig.memoryMB)
}

func writeReport(w io.Writer, current, previous result, currentLabel, previousLabel string, zig *zigResult, zigLabel string) {
	names := make([]string, 0, len(current))
	for name := range current {
		names = append(names, name)
	}
	slices.Sort(names)

	fmt.Fprintln(w, "# Performance benchmark report")
	fmt.Fprintf(w, "\nCurrent: `%s`  \nPrevious: `%s`\n", currentLabel, previousLabel)
	fmt.Fprintln(w, "\nValues are medians of three runs except the 100K clustered-vector workload, which runs once. Δ is current versus previous; ns/op is informational because shared-runner latency is noisy, while graph-core B/op and allocs/op remain enforced (including multi-hop). WAL recovery latency is informational; its allocation and byte metrics remain gated.")
	if zig != nil {
		writeZigComparison(w, current, zig, zigLabel)
	}
	fmt.Fprintln(w, "\n| Benchmark | ns/op current | previous | Δ | B/op current | previous | Δ | allocs/op current | previous | Δ | Other current / previous (Δ) |")
	fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, name := range names {
		cur := current[name]
		prev := previous[name]
		fmt.Fprintf(w, "| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			name,
			display(cur, "ns/op"), display(prev, "ns/op"), delta(cur, prev, "ns/op"),
			display(cur, "B/op"), display(prev, "B/op"), delta(cur, prev, "B/op"),
			display(cur, "allocs/op"), display(prev, "allocs/op"), delta(cur, prev, "allocs/op"),
			otherMetrics(cur, prev),
		)
	}
}

func read(path string) (result, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parse(file)
}

func validateGoResult(benchmarks result) error {
	required := map[string][]string{
		"BenchmarkReadRequests/query":                         {"ns/op"},
		"BenchmarkCheckpoint":                                 {"ns/op"},
		"BenchmarkColdOpen":                                   {"ns/op"},
		"BenchmarkReaderDuringCommit":                         {"ns/op"},
		"BenchmarkFTSSearchScaling/records_100000/fuzzy_rare": {"ns/op"},
		"BenchmarkVectorSearchScaling/records_100000":         {"ns/op"},
		"BenchmarkVectorSearchANNFallback10K":                 {"ns/op"},
		"BenchmarkVectorSearchClustered128D/100K":             {"ns/op", "index-build-ms", "recall@10"},
	}
	for name, units := range required {
		metrics := benchmarks[name]
		for _, unit := range units {
			if _, ok := value(metrics, unit); !ok {
				return fmt.Errorf("Go benchmark %s missing %s", name, unit)
			}
		}
	}
	return nil
}

func main() {
	currentPath := flag.String("current", "", "current go test benchmark output")
	previousPath := flag.String("previous", "", "previous go test benchmark output")
	outputPath := flag.String("output", "", "markdown report path")
	currentLabel := flag.String("current-label", "current", "current revision label")
	previousLabel := flag.String("previous-label", "previous", "previous revision label")
	zigPath := flag.String("zig", "", "Zig vector benchmark output")
	zigLabel := flag.String("zig-label", "Zig reference", "Zig revision label")
	validateGoPath := flag.String("validate-go", "", "validate Go benchmark output")
	validateZigPath := flag.String("validate-zig", "", "validate Zig vector benchmark output")
	checkPath := flag.Bool("check", false, "enforce stable performance gates")
	flag.Parse()
	if *validateGoPath != "" {
		benchmarks, err := read(*validateGoPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := validateGoResult(benchmarks); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *validateZigPath != "" {
		file, openErr := os.Open(*validateZigPath)
		if openErr != nil {
			fmt.Fprintln(os.Stderr, openErr)
			os.Exit(1)
		}
		_, err := parseZig(file)
		file.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *checkPath {
		current, err := read(*currentPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		previous, err := read(*previousPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := checkGates(current, previous, os.Stderr); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	current, err := read(*currentPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	previous, err := read(*previousPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var zig *zigResult
	if *zigPath != "" {
		file, openErr := os.Open(*zigPath)
		if openErr != nil {
			fmt.Fprintln(os.Stderr, openErr)
			os.Exit(1)
		}
		zig, err = parseZig(file)
		file.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	output, err := os.Create(*outputPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer output.Close()
	writeReport(output, current, previous, *currentLabel, *previousLabel, zig, *zigLabel)
}
