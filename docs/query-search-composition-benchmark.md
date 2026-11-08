# SciFact one-hop search composition benchmark (#260)

This harness checks `DB.SearchGraphContext` against a separately computed
direct-search-plus-join reference on the pinned SciFact corpus. The synthetic
graph tests one-hop composition, multiplicity, target filtering, ordering, and
score propagation. It does not add graph relevance labels to SciFact.

## Fixture

Prepare the input with the pinned archive and commands from the
[existing SciFact evaluation](benchmarks/fts-scifact-2026-10-04/REPRODUCE.md).
The harness reads these files:

- `data/corpus-raw.jsonl`: 5,183 title-and-abstract documents.
- `data/queries-raw.jsonl`: 300 test queries.
- `data/qrels.json`: the pinned relevance judgments.

Each corpus document becomes a manually FTS-indexed `SciFactDoc` node with the
`SciFactTarget` label. For corpus position `i`, the harness creates these
outgoing `SCIFACT_REF` edges in this order:

1. to position `(i+1) % N`;
2. to position `(i+7) % N`;
3. a parallel edge to position `(i+1) % N`; and
4. to a `SciFactNoise` node, which the target-label postfilter must reject.

Edge IDs therefore depend only on corpus order. The first candidate keeps three
accepted paths, while the rejected fourth edge proves that filtering happens
after graph work without fetching replacement candidates.

The measured request uses BM25 with the standard analyzer, `CandidateK=10`,
`ResultK=30`, and no vector source. The result contains up to three accepted
rows for each of ten candidates. Candidate order is the native BM25 score in
descending order, then source node ID. Edge rows are ordered by target node ID,
then edge ID.

## Reference and recall

The reference starts with `FTSSearchContext` using the same BM25 options and
candidate depth as the composed request. A separate Cypher join reads the
`SciFactDoc` to `SciFactTarget` adjacency. The harness applies the direct FTS
candidate order and the documented target/edge tiebreaks, then compares every
row's source ID, target ID, and edge ID. It compares BM25 values with
`math.Float32bits`, so the score comparison includes the exact float32 bits.

SciFact relevance remains document relevance. Ranked document recall uses the
direct candidate order and candidate depth: it counts relevant documents in
the top ten candidates over all relevant documents, then averages all 300
queries. The acceptance check requires exactly 5,183 documents and 300 labeled
queries. A missing qrels entry, unknown document, changed row, or changed score
fails the run.

## Timing, allocation, and budgets

The harness builds a new disk database, closes it, and reopens it. It computes
the full reference before timing, runs every query once as a complete warmup,
then runs three measured rounds over all 300 queries.

Each point in `queries[].measurements[]` records wall-clock time. Allocation is
the increase in Go's `runtime.MemStats.TotalAlloc` across the measured
`SearchGraphContext` call. This is allocated bytes, not live heap or RSS.
The aggregate mean and nearest-rank P95 use all 900 measured calls.

The budget sample uses the query with the most reference rows and a bounded
binary search over each limit. The other limits stay at the values in the run.
An attempt counts as accepted only when it returns the complete reference row
sequence with exact float32 values. `MinWork` and `MinBytes` are therefore
implementation-specific logical thresholds. They are not process memory or a
portable minimum for another query, database, or platform.

## Measured result

The following is one run on macOS arm64 with Go 1.27.1, using the command
below. Timings are local observations from three rounds and are not a CPU-
isolated or frequency-pinned result.

| Measurement | Result |
| --- | ---: |
| Documents / queries | 5,183 / 300 |
| `CandidateK` / `ResultK` | 10 / 30 |
| Queries matching reference rows and score bits | 300 / 300 |
| Mean ranked document Recall@10 | 0.790944 |
| Disk graph build | 14.2886 s |
| Reopen | 4.897791 ms |
| Mean composed call | 7.213158 ms |
| P95 composed call | 13.2595 ms |
| Mean allocated bytes per call | 7,575,191 B |
| Sample query / accepted rows | query 1 / 30 |
| Sample minimum accepted work | 57,839 |
| Sample minimum accepted logical bytes | 338,095 |

The measured JSON contains all 300 query records and all 900 latency and
allocation samples. In the recorded run, its SHA-256 was
`aa92a31ff9f665360951fb5d9797fb91e62436b86d674a734a233d2fc6fe7b86`.

## Reproduce

From the repository root, point `dataset` at the directory containing the
prepared `data` directory:

```sh
dataset=/path/to/prepared/scifact
run="$(mktemp -d /tmp/latticedb-260-compose.XXXXXX)"
go build -o "$run/fts-corpus-go" ./scripts/fts-corpus-go
mkdir "$run/output"
"$run/fts-corpus-go" \
  -compose \
  -corpus "$dataset/data/corpus-raw.jsonl" \
  -queries "$dataset/data/queries-raw.jsonl" \
  -qrels "$dataset/data/qrels.json" \
  -db "$run/scifact-composed.db" \
  -output "$run/output/scifact-composed.json" \
  -candidate-k 10 \
  -result-k 30 \
  -repeats 3
```

Inspect the aggregate and per-query evidence with:

```sh
jq '{summary, budget_sample}' "$run/output/scifact-composed.json"
jq '.queries[] | {query_id, recall_at_k, matched_rows, measurements}' \
  "$run/output/scifact-composed.json"
```

## Synthetic vector control

SciFact provides text judgments. The vector control uses the repository's
deterministic fixture of 512 vectors and 100 queries, with zero, one or two
parallel outgoing edges per vector. Both paths use `CandidateK=20` and
`ResultK=20`; the ANN run uses `EfSearch=32`. Recall compares row identities to
an exhaustive L2 reference with the same expansion and limits.

```sh
go test . -run '^$' -bench '^BenchmarkSearchGraphVectorRecall$' -benchtime=1x -count=1
```

One Apple M3 run matched every reference candidate and expanded row (recall
1.000 in both cases, no exact fallback). The 100-query batch took 38.68 ms
without expansion and 42.48 ms with expansion. Allocations were 24,408,520 B
and 27,933,720 B for the batch. This small control checks ranking and
multiplicity; it does not predict ANN recall on other vector collections.
