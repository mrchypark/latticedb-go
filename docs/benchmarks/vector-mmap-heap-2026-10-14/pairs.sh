#!/bin/bash
set -e
out=/tmp/latticedb-mmap-20261014
for i in 1 2 3; do
 env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 /usr/bin/time -l /tmp/latticedb-structures-20261014/final-direct.test -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^10K$' -test.benchtime=1x -test.timeout=2m > "$out/before-10k-$i.txt" 2>&1
 env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 /usr/bin/time -l "$out/cursor.test" -test.run='^$' -test.bench='^BenchmarkVectorSearchDisk128D$/^10K$' -test.benchtime=1x -test.timeout=2m > "$out/after-10k-$i.txt" 2>&1
done
