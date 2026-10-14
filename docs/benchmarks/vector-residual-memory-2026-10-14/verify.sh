#!/bin/bash
set -u
cd /Users/cypark/Documents/project/latticedb-go
out=/tmp/latticedb-residual-20261014
: > "$out/verify-status.txt"
GOMEMLIMIT=512MiB go test -race . ./internal/engine ./internal/store ./internal/pagestore -run 'TestDiskVectorBenchmarkReopensPersistedIndex|TestPageVector|TestPublicVector|TestPutOwned|Test.*Recover|Test.*Compact|TestPageStream' > "$out/verify-focused-race.txt" 2>&1
printf 'focused-race %s\n' "$?" >> "$out/verify-status.txt"
go vet . > "$out/verify-vet.txt" 2>&1
printf 'vet-root %s\n' "$?" >> "$out/verify-status.txt"
git diff --check > "$out/verify-diff.txt" 2>&1
printf 'diff-check %s\n' "$?" >> "$out/verify-status.txt"
cat "$out/verify-status.txt"
