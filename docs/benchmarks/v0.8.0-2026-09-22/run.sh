#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
out=docs/benchmarks/v0.8.0-2026-09-22
export GOMAXPROCS=2
{ git rev-parse HEAD; git describe --tags --always; go version; uname -sm; sysctl -n machdep.cpu.brand_string hw.memsize; } > "$out/environment.txt"
go test ./internal/engine -run 'TestSupportedCypherGrammarContract|TestQueryGrammar|TestQuery.*(With|Aggregate|Function)|Test.*(With|Aggregate|Function)' -count=1 -json > "$out/grammar-tests.jsonl"
go test ./... -run '^$' -bench '^(BenchmarkQueryLanguage|BenchmarkQueryOrderLimitTopK|BenchmarkQueryMultiHopSlots|BenchmarkQueryMultiHopSlots100K|BenchmarkPropertyEquality10K|BenchmarkPropertyIndexCommonValue10K|BenchmarkQueryFTSCandidates10K|BenchmarkQueryVectorCandidates10K)$' -benchmem -benchtime=200ms -count=3 > "$out/query.txt" 2>&1
go test ./internal/engine -run '^$' -bench '^BenchmarkFTSSearchScaling$/^records_(1000|10000|100000)$' -benchmem -benchtime=200ms -count=3 > "$out/fts.txt" 2>&1
go test ./internal/engine -run '^$' -bench '^BenchmarkVectorSearchClusteredExact128D$' -benchmem -benchtime=200ms -count=3 > "$out/vector-exact.txt" 2>&1
go test ./internal/engine -run '^$' -bench '^BenchmarkVectorSearchClustered128D$/^(1K|10K|100K)$' -benchmem -benchtime=1x -count=3 > "$out/vector-ann.txt" 2>&1
go test ./internal/engine -run '^$' -bench '^BenchmarkFTSFuzzyVocabularyPruning$' -benchmem -benchtime=200ms -count=3 > "$out/fuzzy-vocabulary.txt" 2>&1
python3 "$out/summarize.py"
