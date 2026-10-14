#!/bin/bash
set -euo pipefail
if [ "$(uname -s)" != Darwin ]; then
  echo 'This CPU measurement script was validated on macOS.' >&2
  exit 1
fi
repo=$(git rev-parse --show-toplevel)
bench_out=$(mktemp -d "${TMPDIR:-/tmp}/latticedb-query.XXXXXX")
export LATTICE_QUERY_REPO="$repo" LATTICE_QUERY_OUT="$bench_out"
python3 - <<'PY'
import json, os, shutil
from pathlib import Path
repo=Path(os.environ['LATTICE_QUERY_REPO'])
out=Path(os.environ['LATTICE_QUERY_OUT'])
source=repo/'docs/benchmarks/vector-layout-memory-2026-10-14'
for name in ('query-benchmark.go','query-legacy-reader.go'):
    shutil.copyfile(source/(name+'.txt'),out/name)
replacements={str(repo/'vector_disk_benchmark_test.go'):str(out/'query-benchmark.go')}
assert all(Path(x).is_file() for x in [*replacements, *replacements.values()])
(out/'query-overlay.json').write_text(json.dumps({'Replace':replacements}))
replacements[str(repo/'internal/store/page_vector.go')]=str(out/'query-legacy-reader.go')
assert all(Path(x).is_file() for x in [*replacements, *replacements.values()])
(out/'query-legacy-overlay.json').write_text(json.dumps({'Replace':replacements}))
PY
cd "$repo"
env GOEXPERIMENT= go test -c -pgo=off -overlay="$bench_out/query-overlay.json" -o "$bench_out/query-final.test" .
env GOEXPERIMENT= go test -c -pgo=off -overlay="$bench_out/query-legacy-overlay.json" -o "$bench_out/query-legacy.test" .
env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 \
  LATTICEDB_KEEP_VECTOR_PATH="$bench_out/split-db" \
  "$bench_out/query-final.test" -test.run='^$' \
  -test.bench='^BenchmarkVectorSearchDisk128D$/^100K$' \
  -test.benchtime=1x -test.timeout=9m > "$bench_out/build.txt" 2>&1
cp -R "$bench_out/split-db" "$bench_out/legacy-db"
env GOMAXPROCS=2 LATTICEDB_QUERY_PATH="$bench_out/legacy-db" \
  "$bench_out/query-final.test" -test.run='^TestMaterializeLegacyVectorFixture$' \
  -test.v -test.timeout=2m > "$bench_out/materialize.txt" 2>&1
for run in 1 2 3; do
  env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 LATTICEDB_QUERY_PATH="$bench_out/legacy-db" \
    /usr/bin/time -l "$bench_out/query-legacy.test" -test.run='^$' \
    -test.bench='^BenchmarkVectorQueryOnly100K$' -test.benchtime=1x -test.timeout=2m \
    > "$bench_out/legacy-$run.txt" 2>&1
  env -u GOGC -u GOMEMLIMIT GOMAXPROCS=2 LATTICEDB_QUERY_PATH="$bench_out/split-db" \
    /usr/bin/time -l "$bench_out/query-final.test" -test.run='^$' \
    -test.bench='^BenchmarkVectorQueryOnly100K$' -test.benchtime=1x -test.timeout=2m \
    > "$bench_out/final-$run.txt" 2>&1
done
printf '%s\n' "$bench_out"
