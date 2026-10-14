#!/bin/bash
set -euo pipefail
cd /Users/cypark/Documents/project/latticedb-go
out=/tmp/latticedb-vector-opt-20261013
export GOMAXPROCS=2
{
go test -pgo=off -run '^$' -bench '^BenchmarkReadRequests$/^query$' -benchtime=10000x -count=3 .
go test -pgo=off -run '^$' -bench '^BenchmarkReadRequests$/^write_commit$' -benchtime=1000x -count=3 .
go test -pgo=off -run '^$' -bench '^BenchmarkSingleRecordCommitScaling$/^nodes_100000$/^direct$' -benchtime=1000x -count=3 .
go test -pgo=off -run '^$' -bench '^BenchmarkQueryMultiHopSlots$' -benchtime=10x -count=3 ./internal/engine
go test -pgo=off -run '^$' -bench '^BenchmarkAdjacency(Read|Append)Scaling$/^chunked_10000$' -benchtime=10x -count=3 ./internal/store
go test -pgo=off -run '^$' -bench '^BenchmarkLoadLatestWALV2$/^delta_history$/^256$' -benchtime=10x -count=3 ./internal/store
} > "$out/gates-final-candidate.txt" 2>&1
go run ./scripts/benchcmp.go -current "$out/gates-final-candidate.txt" -previous "$out/gates-before-stable.txt" -check > "$out/gates-final-result.txt" 2>&1
