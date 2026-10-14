#!/bin/bash
set -euo pipefail
out=/tmp/latticedb-vector-opt-20261013
GOMAXPROCS=2 /usr/bin/time -l "$out/build-cache-default.test" -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^10K$' -test.benchtime=1x -test.count=3 -test.timeout=3m > "$out/build-cache-default-10k.txt" 2>&1
for run in 1 2 3; do
  env -u LATTICE_BUFFER_POOL_MB /usr/bin/time -l /tmp/vector-disk-zig-normalized "$out/fixtures/vectors-10000.bin" "$out/zig-final-10k-$run.db" > "$out/zig-final-10k-$run.txt" 2>&1
done
env -u LATTICE_BUFFER_POOL_MB /usr/bin/time -l /tmp/vector-disk-zig-normalized "$out/fixtures/vectors-100000.bin" "$out/zig-final-100k.db" > "$out/zig-final-100k.txt" 2>&1
