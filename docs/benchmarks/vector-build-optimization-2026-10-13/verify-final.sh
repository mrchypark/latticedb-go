#!/bin/bash
set -u
cd /Users/cypark/Documents/project/latticedb-go
out=/tmp/latticedb-vector-opt-20261013
run_check() {
  name=$1
  shift
  "$@" > "$out/verify-$name.txt" 2>&1
  result=$?
  printf '%s %s\n' "$name" "$result" >> "$out/verify-status.txt"
}
: > "$out/verify-status.txt"
run_check normal go test ./...
run_check race go test -race ./...
run_check vet go vet ./...
run_check simd env GOEXPERIMENT=simd go test ./...
run_check simd-vet env GOEXPERIMENT=simd go vet ./...
run_check simd-race env GOEXPERIMENT=simd go test -race ./internal/search ./internal/engine ./internal/store ./internal/pagestore .
run_check conformance bash -c 'cd conformance/go && go test -race ./... && go vet ./...'
run_check python python3 -m unittest discover -s scripts -p 'test_*.py'
for pair in linux/amd64 windows/amd64 solaris/amd64 aix/ppc64 plan9/amd64 js/wasm wasip1/wasm; do
  os=${pair%/*}
  arch=${pair#*/}
  run_check "compile-$os" env CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go test -c -o "$out/final-$os.test" .
done
run_check compile-simd-arm64 env CGO_ENABLED=0 GOEXPERIMENT=simd GOOS=linux GOARCH=arm64 go test -c -o "$out/final-linux-simd.test" .
cat "$out/verify-status.txt"
