#!/bin/bash
set -e
out=/tmp/latticedb-mmap-20261014
for i in 1 2 3; do
 env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 LATTICEDB_QUERY_PATH=/tmp/latticedb-structures-20261014/query-linked-data /usr/bin/time -l /tmp/latticedb-structures-20261014/query-final.test -test.run='^$' -test.bench='^BenchmarkVectorQueryOnly100K$' -test.benchtime=1x -test.timeout=2m > "$out/query-before-$i.txt" 2>&1
 env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 LATTICEDB_QUERY_PATH=/tmp/latticedb-structures-20261014/query-linked-data /usr/bin/time -l "$out/query-cursor.test" -test.run='^$' -test.bench='^BenchmarkVectorQueryOnly100K$' -test.benchtime=1x -test.timeout=2m > "$out/query-after-$i.txt" 2>&1
done
