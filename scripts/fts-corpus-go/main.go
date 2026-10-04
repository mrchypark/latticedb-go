// Command fts-corpus-go measures the public disk FTS API on prepared JSONL inputs.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	lattice "github.com/mrchypark/latticedb-go"
)

type record struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}
type hit struct {
	ID    string  `json:"id"`
	Score float32 `json:"score"`
}
type run struct {
	QueryID   string `json:"query_id"`
	Repeat    int    `json:"repeat"`
	ElapsedNS int64  `json:"elapsed_ns"`
	Results   []hit  `json:"results"`
}
type output struct {
	Mode       string `json:"mode"`
	Documents  int    `json:"documents"`
	QueryCount int    `json:"query_count"`
	Repeats    int    `json:"repeats"`
	BuildNS    int64  `json:"build_ns"`
	ReopenNS   int64  `json:"reopen_ns"`
	MaxWork    uint64 `json:"max_work"`
	MaxBytes   uint64 `json:"max_bytes"`
	Runs       []run  `json:"runs"`
}

func load(path string) ([]record, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 16<<20)
	var rows []record
	seen := map[string]bool{}
	for s.Scan() {
		var r record
		if e = json.Unmarshal(s.Bytes(), &r); e != nil {
			return nil, e
		}
		if r.ID == "" || r.Text == "" || seen[r.ID] {
			return nil, fmt.Errorf("empty record or duplicate ID %q", r.ID)
		}
		seen[r.ID] = true
		rows = append(rows, r)
	}
	if e = s.Err(); e != nil {
		return nil, e
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("empty input %s", path)
	}
	return rows, nil
}
func measure(corpus, queries []record, path, mode string, repeats int) (out output, err error) {
	if repeats < 1 {
		return out, fmt.Errorf("repeats must be positive")
	}
	opts := lattice.FTSSearchOptions{Limit: 10, MaxWork: 1 << 40, MaxBytes: 256 << 20}
	switch mode {
	case "frequency":
	case "bm25":
		opts.Scoring = lattice.FTSScoringBM25
	case "porter-bm25":
		opts.Scoring = lattice.FTSScoringBM25
		opts.Analyzer = lattice.FTSAnalyzerEnglishPorter
	default:
		return out, fmt.Errorf("unknown mode %q", mode)
	}
	if path == "" || path == ":memory:" {
		return out, fmt.Errorf("new disk path required")
	}
	if _, e := os.Lstat(path); !os.IsNotExist(e) {
		return out, fmt.Errorf("database path must not exist: %s", path)
	}
	started := time.Now()
	db, e := lattice.Open(path, lattice.OpenOptions{Create: true})
	if e != nil {
		return out, e
	}
	defer func() {
		if db != nil {
			errClose := db.Close()
			if err == nil {
				err = errClose
			}
		}
	}()
	ids := map[uint64]string{}
	for start := 0; start < len(corpus); start += 100 {
		end := min(start+100, len(corpus))
		if e = db.Update(func(tx *lattice.Tx) error {
			for _, doc := range corpus[start:end] {
				n, e := tx.CreateNode(lattice.CreateNodeOptions{})
				if e != nil {
					return e
				}
				ids[n.ID] = doc.ID
				if e = tx.FTSIndex(n.ID, doc.Text); e != nil {
					return e
				}
			}
			return nil
		}); e != nil {
			return out, e
		}
	}
	if e = db.Close(); e != nil {
		return out, e
	}
	db = nil
	out = output{Mode: mode, Documents: len(corpus), QueryCount: len(queries), Repeats: repeats, BuildNS: time.Since(started).Nanoseconds(), MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes}
	started = time.Now()
	db, e = lattice.Open(path, lattice.OpenOptions{})
	if e != nil {
		return out, e
	}
	out.ReopenNS = time.Since(started).Nanoseconds()
	count, e := db.Query("MATCH (n) RETURN count(*) AS count", nil)
	if e != nil {
		return out, e
	}
	if len(count.Rows) != 1 || count.Rows[0]["count"] != int64(len(corpus)) {
		return out, fmt.Errorf("reopened document count mismatch: %v", count.Rows)
	}
	ctx := context.Background()
	for _, q := range queries {
		if _, e = db.FTSSearchContext(ctx, q.Text, opts); e != nil {
			return out, fmt.Errorf("warmup query %s: %w", q.ID, e)
		}
	}
	for rep := 0; rep < repeats; rep++ {
		for _, q := range queries {
			started = time.Now()
			found, e := db.FTSSearchContext(ctx, q.Text, opts)
			elapsed := time.Since(started).Nanoseconds()
			if e != nil {
				return out, fmt.Errorf("query %s: %w", q.ID, e)
			}
			r := run{QueryID: q.ID, Repeat: rep, ElapsedNS: elapsed, Results: make([]hit, 0, len(found))}
			for _, h := range found {
				id, ok := ids[h.NodeID]
				if !ok {
					return out, fmt.Errorf("unknown result ID %d", h.NodeID)
				}
				r.Results = append(r.Results, hit{id, h.Score})
			}
			out.Runs = append(out.Runs, r)
		}
	}
	return out, nil
}
func execute() error {
	corpusPath := flag.String("corpus", "", "prepared JSONL corpus")
	queryPath := flag.String("queries", "", "prepared JSONL queries")
	outPath := flag.String("output", "", "new JSON output")
	dbPath := flag.String("db", "", "new disk database path")
	mode := flag.String("mode", "bm25", "frequency, bm25, porter-bm25")
	repeats := flag.Int("repeats", 3, "measured rounds after one complete warmup")
	flag.Parse()
	docs, e := load(*corpusPath)
	if e != nil {
		return e
	}
	queries, e := load(*queryPath)
	if e != nil {
		return e
	}
	out, e := measure(docs, queries, *dbPath, *mode, *repeats)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(*outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if e = enc.Encode(out); e != nil {
		f.Close()
		return e
	}
	return f.Close()
}
func main() {
	if e := execute(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
