#!/bin/bash
set -euo pipefail
cd /Users/cypark/Documents/project/latticedb-go
out=/tmp/latticedb-memory-20261014
GOEXPERIMENT=simd go test -pgo=off -c -o "$out/simd-training.test" .
GOMAXPROCS=2 "$out/simd-training.test" -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^10K$' -test.benchtime=1x -test.cpuprofile="$out/final-simd.cpu" -test.memprofile="$out/final-simd.mem" > "$out/simd-training-10k.txt" 2>&1
GOEXPERIMENT=simd go test -pgo="$out/final-simd.cpu" -c -o "$out/final-fast.test" .
for run in 1 2 3; do
 GOMAXPROCS=2 /usr/bin/time -l "$out/final-fast.test" -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^10K$' -test.benchtime=1x > "$out/fast-10k-$run.txt" 2>&1
done
GOMAXPROCS=2 LATTICEDB_VECTOR_BUILD_PROGRESS=1 /usr/bin/time -l "$out/final-fast.test" -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^100K$' -test.benchtime=1x -test.timeout=8m > "$out/fast-100k.txt" 2>&1
