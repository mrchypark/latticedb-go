# Exact vector ranking

Exact vector scans, including incomplete-ANN fallback, keep squared L2
candidates in the top-K heap. Candidates clearly outside the current worst
rounding interval are discarded without a square root. Only final returned
exact results call `math.Sqrt`, preserving the public float32 L2 scores.
ANN index contents, its distance calculations, and successful ANN results
retain their existing semantics.

## Rounded ties

Raw squared ordering alone is insufficient: `[1, 0]` and `[1, 0.0001]`
have different squared distances but both produce float32 L2 distance 1.
The existing smaller-node-ID tiebreak must still apply at the K boundary.

Normal squared distances separated by more than the conservative relative
margin `2^-19` cannot share an L2 rounding bin. Potential ties use an exact
integer comparison against squared float32 rounding boundaries. These
boundaries include the intermediate float64 rounding performed by
`math.Sqrt`; the comparison uses a 108-bit product, not an approximate root.
Subnormal float32 scores use a wider conservative cutoff and the same exact
boundary classification. Dense rounding ties can cost more than ordinary data.

## Memory and validation

The existing result budget remains in place. Exact selection additionally
reserves 16 logical bytes per candidate slot (up to the requested K, clamped
to graph size). A request with a very tight MaxBytes may therefore require a
larger budget. Up to 64 candidate slots use stack scratch; larger selections
can allocate. Query finite-value validation, distance-overflow rejection,
context cancellation, and scan-work accounting are preserved.

## Measurement

Apple M3, darwin/arm64; median of five 200ms samples, baseline `a0ae4fe`.
Both versions use identical seeded random 8-dimensional vectors and K=10.
The fallback fixture has a disconnected singleton ANN index; both modes are
warmed and the fallback condition is asserted before timing.

| Nodes | Mode | Before ns/op | After ns/op | Reduction |
|---:|---|---:|---:|---:|
| 10,000 | Exact | 127,141 | 112,405 | 11.6% |
| 10,000 | Fallback | 126,168 | 112,343 | 11.0% |
| 100,000 | Exact | 1,323,388 | 1,074,809 | 18.8% |
| 100,000 | Fallback | 1,272,057 | 1,051,866 | 17.3% |

All cases retain 2 allocations/op. Median allocated bytes are 224 B/op,
except the candidate 100K fallback at 225 B/op (pool/sample variation).

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkExactSquaredScan$' -benchtime=200ms -count=5 -benchmem
```

Compatibility checks compare result IDs, order, and score bits against the
previous full-L2 scan. Separate tests exercise random squared distances,
float32 midpoint neighborhoods, subnormal values, equal-distance K boundaries,
large K, invalid queries, overflow, and candidate-memory limits.
