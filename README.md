# LatticeDB Go

An embedded graph database written entirely in Go. It provides transactional graph operations, Cypher-style queries, full-text search, vector search, durable page commits, streams, exports, and online snapshots without cgo.

## Install

LatticeDB Go requires Go 1.27 or newer.

```sh
go get github.com/mrchypark/latticedb-go@v0.9.0
```

`v0.9.0` is the latest tagged release in this checkout. The working tree also
contains unreleased arithmetic/general `RETURN`, aggregate `DISTINCT`, `MERGE`,
variable-length paths, BM25/English stemming, configurable HNSW `M`, continuous
backup recovery points, and the legacy binary WAL v5 format; the contracts below describe that working tree.

## Quick start

```go
package main

import (
	"fmt"
	"log"

	latticedb "github.com/mrchypark/latticedb-go"
)

func main() {
	db, err := latticedb.Open("app.ltdb", latticedb.OpenOptions{Create: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *latticedb.Tx) error {
		_, err := tx.CreateNode(latticedb.CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]latticedb.Value{"name": "Ada"},
		})
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Query(
		"MATCH (n:Person) WHERE n.name = $name RETURN n.name AS name",
		map[string]latticedb.Value{"name": "Ada"},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Rows)
}
```

## Highlights

- Pure Go on Linux, macOS, and Windows
- ACID write transactions with crash recovery and page checkpoints
- Transaction-scoped queries and binary-safe application metadata
- Property indexes, full-text search, and exact or HNSW vector search; page-backed opens preserve exact results, while HNSW is not page-backed
- Online frozen-generation backups through `BeginSnapshot` and opt-in per-commit backup archives
- JSON, JSONL, CSV, and DOT export
- Context cancellation and row, work, and logical-byte budgets

The Zig project supplies feature goals; this Go implementation defines its own APIs, query semantics, and storage format. See the [feature goal assessment](docs/feature-goals.md) for coverage and remaining gaps.

## Query support

LatticeDB supports a small, case-sensitive Cypher subset, not full openCypher or Neo4j query compatibility. Use uppercase clause keywords and the documented case-sensitive built-in function names.

| Area | Supported scope |
| --- | --- |
| Matching | Nodes, multiple conjunctive labels, property maps, comma-separated patterns, and fixed or variable-length paths with outgoing, incoming, or undirected relationships |
| Filtering | Property comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`), `id(binding) = expression`, `AND` / `OR` / `NOT`, parentheses, `IN`, `IS NULL`, `IS NOT NULL`, `STARTS WITH`, `ENDS WITH`, `CONTAINS` |
| Results | Standalone `RETURN`, bindings, scalar/list/map and arithmetic expressions, built-in functions, `AS`, `DISTINCT`, multi-key `ORDER BY`, `SKIP`, `LIMIT` |
| Aggregation | `count`, `sum`, `avg`, `min`, `max`, and `collect`, including aggregate `DISTINCT`; non-aggregate projections form grouping keys |
| Query parts | `WITH` projection and scoped names, filtering, aggregation, `DISTINCT`, ordering, and pagination before the next query part |
| Built-in functions | Graph metadata (`id`, `labels`, `type`, `properties`), list/string helpers, conversions, `abs`, and `coalesce`; see the [grammar contract](docs/engine_conformance.md) for the complete list |
| Writes | Node `CREATE`; directed relationship `CREATE` between matched bindings; node/path `MERGE` with `ON CREATE SET` and `ON MATCH SET`; property and label `SET` / `REMOVE`; map replacement (`SET n = $props`) and merge (`SET n += $props`); `DELETE` / `DETACH DELETE` |
| Batch input | `UNWIND` over list parameters or expressions, including chained query parts through `WITH` |
| Search | Vector ranking with `n.embedding <=> $vector`; property-scoped full-text search with `n.text @@ $query`. Search predicates can be combined with `AND`, but cannot occur under `OR` or `NOT`. |
| Values | Scalar, list, and map literals; parameters also carry lists, bytes, and vectors. Backtick-quoted identifiers support Unicode and spaces. |

For example, pass a list as a parameter (list literals such as `["a", "b"]` also work):

```go
result, err := db.Query(
	"MATCH (n:Person) WHERE n.name IN $names RETURN n.name AS name ORDER BY name LIMIT $limit",
	map[string]latticedb.Value{
		"names": []string{"Ada", "Grace"},
		"limit": 10,
	},
)
```

Important boundaries:

- No `OPTIONAL MATCH`, `UNION`, query comments, or multiple statements. One trailing semicolon is allowed.
- No user-defined function calls or `RETURN *`. Numeric arithmetic supports `+`, `-`, `*`, `/`, `%`, `^`, unary signs, and parentheses; invalid numeric operations return an error.
- `WITH` exports explicit aliases and plain binding names; dropped names cannot be referenced by later parts. A `WITH` must be followed by another query part. See the [grammar contract](docs/engine_conformance.md) for expression and ordering restrictions.
- Use spaces around binary predicate and assignment operators: `n.age = 1`, not `n.age=1`. Inequality is `<>`, not `!=`.
- Within each query part, a `MATCH` has one terminal clause. `SET`, `REMOVE`, and relationship `CREATE` may be followed by `RETURN`; `DELETE` cannot. Top-level `CREATE` creates a node, not an entire path, and cannot be followed by `SET`.
- Missing properties evaluate to `NULL`. `SET n.prop = null` removes a property. Plain `DELETE` rejects nodes with incident edges; use `DETACH DELETE` to remove those edges too.
- On mutation queries, `SKIP` and `LIMIT` restrict returned rows, not writes. With `RETURN DISTINCT`, every `ORDER BY` expression must also be projected.
- An undirected match can return both orientations of an edge; a self-loop returns one. Use explicit `ORDER BY` when application behavior depends on result order.

Query search predicates score candidate rows and sort matches before `LIMIT`. Configured `FTSProperties` can accelerate eligible `@@` predicates; `ApproximateVector` opts eligible queries into HNSW candidates. See [query search candidates](docs/query-search-candidates.md) for exact defaults and fallback rules. Direct `VectorSearch` supports the global vector space and [configured namespaces](docs/vector-namespaces.md); direct `FTSSearch` uses explicitly indexed node text. HNSW is a validated, disposable in-memory cache; page-backed opens currently preserve exact vector results by scanning records and do not persist HNSW state.

Direct full-text search can opt into BM25 and English stemming without changing
existing frequency ranking or query `@@` semantics:

```go
hits, err := db.FTSSearch("running", latticedb.FTSSearchOptions{
    Scoring:  latticedb.FTSScoringBM25,
    Analyzer: latticedb.FTSAnalyzerEnglishPorter,
    Limit:    10,
})
```

BM25 uses `k1=1.2`, `b=0.75`, and all explicitly indexed text as its corpus.
It scans the corpus for statistics; stemming analyzes original text on demand.
Use `FTSSearchContext` and work/byte limits for large corpora. HNSW construction
accepts `OpenOptions.VectorM` in `2..64` (zero selects 16); larger values increase
index memory and construction work. Changing M rebuilds the disposable cache.

Use parameters for application values and explicit aliases for result columns. Queries default to 1,000,000 rows, 10,000,000 work units, and 64 MiB of live logical bytes. Use `QueryContext` with a deadline and explicit `QueryOptions` limits for your workload; logical byte budgets do not measure process RSS. Query text is limited to 32 KiB.

See the [query semantics and full grammar](docs/engine_conformance.md#query-semantics) for exact clause combinations and behavior. The [canonical grammar](internal/engine/testdata/query_grammar.ebnf) and [accepted/rejected syntax tests](internal/engine/query_grammar_test.go) are checked against the parser.

## Storage and transaction contract

### Opening and sizing

- The database uses one active writer. `BeginWriteContext` waits for the writer slot until cancellation; `Begin(false)` and `Update` can return `ErrWriteTxActive` on contention.
- Opt-in `Batch`/`BatchContext` groups concurrent callbacks into one durable transaction. Peer failure rolls back the group; see the [group commit contract](docs/group-commit.md).
- `MaxDatabaseSnapshotBytes`, when nonzero, limits canonical snapshot bytes at open and commit. Zero leaves total page-backed database size unlimited. This option does not bound process RSS.

- Entity IDs (nodes, edges, and edge endpoints) are uint64 values in `1..MaxInt64`.
  `MaxInt64+1` is reserved as the high-water exhaustion sentinel and is never
  allocated.
- The legacy native-engine options `OpenOptions.EnableWAL`, `DisableWAL`, and `EnableAdjacencyCache` must remain false (their default); true requests return `ErrUnsupportedOption`. Public page-backed commits use atomic bbolt transactions.
- `OpenOptions.CacheSizeMB` and `PageSize` are compatibility fields only. They must remain zero (their default); every nonzero request, including former `100` and `4096` values, returns `ErrUnsupportedOption`.
- Public `Open` always uses the page backend. A new directory database stores bbolt pages in `state.json`. A legacy v5 state/WAL database is imported into a staged `state.json.pages` sidecar while preserving its source files; a read-only open without that sidecar imports to a private temporary directory. The v5 state/WAL formats remain migration and interchange formats, not the normal page-backed runtime. See [storage format and legacy limits](docs/binary-storage.md).
- Page-backed storage persists graph records, label/type/adjacency postings, equality property postings, stream records, and consumer offsets in bbolt. Existing exact search paths scan pages as needed. HNSW page storage and new search work remain pending. Application metadata is persisted but is still loaded into the in-memory graph catalog.
- Page storage currently runs on Linux, macOS, and Windows. AIX and Solaris cross-compilation is checked; runtime validation on those systems is not yet available. JS/WASI and Plan 9 compile through an explicit unsupported stub; opening a database there returns an unsupported-platform error.

### Continuous backup and restore

Set `OpenOptions.BackupDirectory` to an exclusively owned archive directory to
capture an initial full checkpoint and each successful commit's WAL delta.
This requires a writable, locked database. Restore to a new path:

```go
metadata, err := latticedb.RestoreBackup(ctx, "app-archive", "restored.ltdb",
    latticedb.BackupRestoreOptions{}) // latest recorded recovery point
```

`CommitID: &commit` selects an exact commit, including zero. `Before: timestamp`
selects a recorded capture at or before that time; these selectors are mutually
exclusive. The returned metadata reports the actual selected commit and capture
time. Capture time is not a historical transaction timestamp.

Normal commits synchronously archive only their exact WAL frames. Recovery
points depend on their base and preceding segments; retain that entire chain.
There is no automatic retention policy. Archive failure after page-transaction durability
returns `ErrCommitOutcomeUnknown` and fences further use until recovery.
Reopening at the same archived state is idempotent. If the source has advanced
while backup was disabled, resumption creates an independent full base and keeps
the older points. Intermediate commits from that interval are unavailable:
WAL v5 does not persist enough ancestry and timestamp information to prove them.
Time selectors inside a recorded coverage gap return an error.
See the [archive storage contract](docs/binary-storage.md#opt-in-backup-archive).

The page backend passed creation, indexing, reopen, updates, deletion, backup,
streaming migration, and incremental restore for a 1.42 GB database under a
256 MiB memory cap with swap disabled. Individual transactions and resident
metadata/catalogs still need to fit in RAM. HNSW remains deferred. See
[page storage and constrained-memory evidence](docs/disk-storage.md).

### Transactions, snapshots, and maintenance

- A `Tx` is single-owner and must not be used concurrently.
- `Commit` and `CommitContext` are one-shot: the transaction becomes inactive whether the commit succeeds or fails.
- Multiple online snapshots may be active per database. A writer can proceed independently, but a page commit that may require bbolt map growth while an older snapshot is pinned fails conservatively with `ErrResourceLimit` before commit. Close snapshots when finished; replay the operation in a new transaction after the growth constraint clears.
- `BeginSnapshot` retries internal checkpoint contention using the same bounded acquisition as write transactions. An active application writer still returns `ErrWriteTxActive` without waiting for the transaction.
- Application metadata is persisted in bbolt but loaded into the in-memory graph catalog. Updates copy the affected shard instead of the complete key map, preserving immutable read and snapshot generations; the fixed shard count reduces copying but does not guarantee constant cost for arbitrarily large or skewed key sets.
- `MaxGenerationLeases` and `MaxRetainedGenerationLogicalBytes` optionally bound admission of public read, snapshot, and export pins. Internal checkpoint and index-maintenance candidates are outside these counters. They never evict an active pin; retained bytes are canonical snapshot bytes, not RSS.
- On Linux, macOS, and Windows, writer opens take an exclusive database-path lock and `ReadOnly` opens take a shared lock. Page storage is explicitly unsupported on js/wasm, wasip1/wasm, and Plan 9.
- `DisableLock` is explicitly unsafe; callers must ensure that the database has a single owner.
- Direct vector search supports configured property/scope namespaces. A nil namespace selects the legacy global index and dimensions. Vector-enabled nodes store one vector property per node; use explicit namespaces to separate embedding spaces.
- `RebuildVectorIndexContext` builds off the writer lock and replays bounded vector changes before publication. The initiating context owns a shared attempt; another caller may cancel its own wait. Existing maintenance limits still apply, and log exhaustion aborts the rebuild without rejecting an otherwise valid commit.
- The internal legacy native engine bounds its active WAL append tail during background checkpoints by `WALCheckpointThresholdBytes` plus one permitted WAL frame. Once reached, commits return `ErrResourceLimit` before WAL mutation and can be retried after checkpoint progress. Public page-backed commits use the bbolt snapshot-growth guard described above.

The detailed behavioral contract is documented in [docs/engine_conformance.md](docs/engine_conformance.md), with the value model in [docs/value_model.md](docs/value_model.md).

CSV export returns and atomically publishes a JSON manifest whose `nodes` and `edges` paths point into `<output>_generations`. Published generations remain immutable and are not reclaimed automatically. For explicit pruning, every reader must hold an `OpenCSVGenerationContext` lease until it finishes reading; `PruneCSVGenerationsContext` protects current and leased generations. Legacy readers are not protected during pruning. Pruning requires native Unix locking and directory sync; unsupported platforms return an error.

## Development

```sh
go test ./...
go test -race ./...
(cd conformance/go && go test ./...)

# Bounded fuzz smoke (each target caps inputs below 64 KiB and recovery work at 100K; WAL covers current v5 and readable legacy v2/v3/v4 frames)
go test ./internal/engine -run '^$' -fuzz '^FuzzParseQuery$' -fuzztime=5s -parallel=1
for target in FuzzDeserializeGraphState FuzzLoadLatestWALFrames FuzzNestedValueRoundTrip; do
  go test ./internal/store -run '^$' -fuzz "^${target}$" -fuzztime=5s -parallel=1
done
```

## License

[MIT](LICENSE)
