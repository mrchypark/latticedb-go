package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type result map[string]map[string][]float64

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
		if results[name] == nil {
			results[name] = map[string][]float64{}
		}
		for i := 2; i+1 < len(fields); i += 2 {
			value, err := strconv.ParseFloat(fields[i], 64)
			if err == nil {
				results[name][fields[i+1]] = append(results[name][fields[i+1]], value)
			}
		}
	}
	return results, scanner.Err()
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
	return median(values), true
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

func writeReport(w io.Writer, current, previous result, currentLabel, previousLabel string) {
	names := make([]string, 0, len(current))
	for name := range current {
		names = append(names, name)
	}
	slices.Sort(names)

	fmt.Fprintln(w, "# Performance benchmark report")
	fmt.Fprintf(w, "\nCurrent: `%s`  \nPrevious: `%s`\n", currentLabel, previousLabel)
	fmt.Fprintln(w, "\nValues are medians of three runs except the 100K-capped Zig harness, which runs once. Δ is current versus previous; this report does not fail CI on variance.")
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

func main() {
	currentPath := flag.String("current", "", "current go test benchmark output")
	previousPath := flag.String("previous", "", "previous go test benchmark output")
	outputPath := flag.String("output", "", "markdown report path")
	currentLabel := flag.String("current-label", "current", "current revision label")
	previousLabel := flag.String("previous-label", "previous", "previous revision label")
	flag.Parse()

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
	output, err := os.Create(*outputPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer output.Close()
	writeReport(output, current, previous, *currentLabel, *previousLabel)
}
