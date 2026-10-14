#!/bin/bash
set -u
cd /Users/cypark/Documents/project/latticedb-go
out=/tmp/latticedb-mmap-20261014
run_check() {
  name=$1
  shift
  "$@" > "$out/verify-$name.txt" 2>&1
  result=$?
  printf '%s %s\n' "$name" "$result" >> "$out/verify-status.txt"
}
: > "$out/verify-status.txt"
run_check focused go test ./internal/pagestore ./internal/store ./internal/engine . -run 'TestReadPair|TestPageVector|TestPublicPageVector|TestPublicVector|TestDiskVector' -count=1
run_check normal go test ./...
run_check race go test -race ./...
run_check vet go vet ./...
run_check simd env GOEXPERIMENT=simd go test ./internal/search ./internal/engine ./internal/store ./internal/pagestore .
run_check conformance bash -c 'cd conformance/go && go test -race ./... && go vet ./...'
for pair in linux/amd64 windows/amd64 plan9/amd64 js/wasm wasip1/wasm; do
  os=${pair%/*}
  arch=${pair#*/}
  run_check "compile-$os" env CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go test -c -o "$out/final-$os.test" .
done
cat "$out/verify-status.txt"
