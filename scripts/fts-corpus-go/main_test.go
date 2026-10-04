package main

import (
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestReopenedBM25AndTiming(t *testing.T) {
	docs := []record{{"a", "rare common common a " + strings.Repeat("x", 64)}, {"b", "rare"}, {"c", "common"}}
	queries := []record{{"q", "rare"}, {"empty", "absent"}, {"one", "a"}, {"long", strings.Repeat("x", 64)}}
	out, e := measure(docs, queries, filepath.Join(t.TempDir(), "db"), "bm25", 1)
	if e != nil {
		t.Fatal(e)
	}
	if out.Documents != 3 || len(out.Runs) != 4 || out.Runs[0].Results[0].ID != "b" || len(out.Runs[1].Results) != 0 || len(out.Runs[2].Results) != 1 || len(out.Runs[3].Results) != 1 {
		t.Fatalf("%+v", out)
	}
	// N=3, df=2, doc lengths5 and1, avgdl=7/3. Independent score for the short document.
	expected := math.Log(1+(3.-2.+.5)/(2.+.5)) * 2.2 / (1 + 1.2*(.25+.75/(7./3.)))
	if math.Abs(float64(out.Runs[0].Results[0].Score)-expected) > 1e-6 {
		t.Fatal(out.Runs[0].Results, expected)
	}
	for _, r := range out.Runs {
		if r.ElapsedNS <= 0 {
			t.Fatal("missing timing")
		}
	}
}
func TestRejectExistingPath(t *testing.T) {
	if _, e := measure(nil, nil, t.TempDir(), "bm25", 1); e == nil {
		t.Fatal("accepted existing DB")
	}
}
