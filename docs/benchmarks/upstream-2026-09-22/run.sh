#!/bin/sh
set -eu
# Run from repository root, with an already cloned original checkout as argument.
upstream=$(cd "${1:?pass original Zig checkout path}" && pwd)
expected=827891e2c6fd55d13aa8f8284a7c7043f68b60fd
[ "$(git -C "$upstream" rev-parse HEAD)" = "$expected" ]
[ -z "$(git -C "$upstream" status --porcelain --untracked-files=no)" ]
out=docs/benchmarks/upstream-2026-09-22
GOMAXPROCS=2 go test ./internal/engine -run '^$' -bench '^BenchmarkVectorSearchClustered128D/10K$' -benchtime=1x -count=3 > "$out/go-10k.txt" 2>&1
for n in 1 2 3; do
 (cd "$upstream" && zig build vector-benchmark -Doptimize=ReleaseFast -- --scale 10000 --scale-only) > "$out/zig-10k-$n.txt" 2>&1
done
