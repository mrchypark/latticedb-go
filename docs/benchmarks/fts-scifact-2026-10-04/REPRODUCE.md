# SciFact FTS evaluation reproduction

This experiment measures the public Go disk FTS API and the original Zig FTS component. It does not compare whole databases or language runtimes.

## Inputs

- Go engine: v0.11.0, `0e5a119173bae1301d9d776828e426a52ef004dd`.
- Zig source: `jeffhajewski/latticedb`, `827891e2c6fd55d13aa8f8284a7c7043f68b60fd` (the reference in issue #219).
- Toolchain used: Go 1.27.1, Zig 0.16.0, Python 3 standard library.
- Dataset: [BEIR SciFact](https://github.com/beir-cellar/beir/wiki/Datasets-available), test split. The official archive has MD5 `5f7d1de60b170fc8027bb7898e2efca1` and SHA256 `536e14446a0ba56ed1398ab1055f39fe852686ecad24a6306c80c490fa8e0165`.
- All 5,183 documents and all 300 judged test queries. Document text is title + one space + abstract. IDs are sorted lexically and mapped to 1-based IDs. The manifest retains the mapping and prepared-file hashes. Corpus text is downloaded separately and is not included in this repository.

## Commands

Run from the Go repository root. Each run needs new database and result paths. The runner refuses an existing output directory. Both adapters refuse an existing database path.

```sh
bench_root=$(mktemp -d)
curl -fL https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip -o "$bench_root/scifact.zip"
PYTHONDONTWRITEBYTECODE=1 python3 scripts/fts_corpus.py prepare "$bench_root/scifact.zip" "$bench_root/data"
git clone https://github.com/jeffhajewski/latticedb.git "$bench_root/zig"
git -C "$bench_root/zig" checkout --detach 827891e2c6fd55d13aa8f8284a7c7043f68b60fd
go build -o "$bench_root/fts-go" ./scripts/fts-corpus-go
zig build-exe -O ReleaseFast -lc \
  --dep lattice --dep compat -Mroot=scripts/fts-corpus-zig.zig \
  --dep compat --dep lattice -Mlattice="$bench_root/zig/src/main.zig" \
  -Mcompat="$bench_root/zig/src/compat.zig" -femit-bin="$bench_root/fts-zig"
PYTHONDONTWRITEBYTECODE=1 python3 scripts/run_fts_corpus.py \
  --go "$bench_root/fts-go" --zig "$bench_root/fts-zig" \
  --data "$bench_root/data" --output "$bench_root/results"
PYTHONDONTWRITEBYTECODE=1 python3 scripts/fts_corpus.py evaluate \
  "$bench_root/data/qrels.json" "$bench_root/data/manifest.json" \
  "$bench_root/results/go-common-bm25.json" "$bench_root/results/zig-common-bm25.json" \
  "$bench_root/results/go-raw-frequency.json" "$bench_root/results/go-raw-bm25.json" \
  "$bench_root/results/go-raw-porter-bm25.json" > "$bench_root/summary.json"
```

Focused harness checks:

```sh
go test ./scripts/fts-corpus-go
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_fts_corpus.py
```

## Measurement contract

The common-input experiment extracts lowercase ASCII `[a-z0-9]+` tokens and drops entire tokens longer than 64 characters. Document token multiplicity is preserved. Both query inputs remove `and`, `or`, and `not`, which the Zig query parser treats as keywords regardless of its stopword setting. Query terms are deduplicated in input order, then limited to 32. No query in this dataset exceeds that limit. No other stopwords, stemming, or fuzzy matching are used in the common-input comparison. BM25 uses OR semantics, k1=1.2, b=0.75 and IDF `log(1+(N-df+0.5)/(df+0.5))`.

Go's raw-input configurations use the original Unicode text and original query. These measure frequency, BM25 and BM25 with English Porter stemming as product choices. They are not presented as tokenizer-equivalent to Zig. No default scoring or fuzzy behavior changes are made.

Each of three rounds constructs a new disk index, closes it, reopens it, performs one complete 300-query warmup, then measures each query once. Configurations run sequentially in rotated order. The runner sets `GOMAXPROCS=2` for Go. The resulting latency sample count is 900 per configuration. Timing starts immediately before the search API call and ends after the ranked top-10 result slice is returned. Mapping result IDs, serialization and output are outside the timer. Build/close and reopen time are recorded separately. This is a warm, read-only, single-client workload.

Go uses the public `DB.FTSSearchContext`, manual `FTSIndex`, the default disabled record cache, and a fresh page database. Search limits are explicitly `MaxWork=2^40` and `MaxBytes=256 MiB`; these are evaluation allowances, not default-limit evidence. Zig uses the original `FtsIndex.searchOr` with BTree/PageManager and a 64 MiB buffer pool. It carries two root page IDs across reopen in the adapter; it does not exercise the database's graph, catalog, transaction or recovery API. Buffer/cache and API boundaries differ. Do not interpret a latency ratio as an equivalent full-database speedup.

Metrics are macro-averaged over all 300 queries. nDCG@10 uses gain `2^relevance-1` and log2 rank discount. Recall@10 divides by every positively judged document for that query, not the returned count. MRR@10 uses the first positively judged hit. Empty results contribute zero. Unjudged hits count as nonrelevant under the supplied judgments. Missing queries, duplicate/unknown hits, API errors, nonfinite scores and incomplete rounds fail evaluation. Quality is scored once per query after verifying the three ranking lists agree. The evaluator reports pooled median/nearest-rank p95 and separate per-round medians/p95. This sample cannot establish a service SLO.

A three-document fixture (`rare common common a <64-char-token>`, `rare`, `common`) checks multiplicity, one-character and 64-character tokens, no-hit output and BM25's independent known-answer score. Both engines return document2 then document1 for `rare`; N=3 and total tokens=7. Python tests cover 65-character exclusion, keyword removal, duplicates, the 32-term boundary, graded relevance, multiple relevant documents and missing/no-hit queries.

The first diagnostic run did not remove Zig's query keywords. Its common-input rankings differed for 22 queries, so it was excluded from the final results. The final run repeats every configuration after correcting preparation. It does not silently mix diagnostic and final samples.
