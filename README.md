# LatticeDB Go

An embedded graph database written entirely in Go. It provides transactional graph operations, Cypher-style queries, full-text search, vector search, durable WAL recovery, streams, exports, and online snapshots without cgo.

## Install

LatticeDB Go requires Go 1.27 or newer.

```sh
go get github.com/mrchypark/latticedb-go@v0.6.0
```

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
- ACID write transactions with WAL recovery and checkpoints
- Transaction-scoped queries and binary-safe application metadata
- Property indexes, full-text search, and exact or HNSW vector search
- Online frozen-generation backups through `BeginSnapshot`
- JSON, JSONL, CSV, and DOT export
- Context cancellation and row, work, and logical-byte budgets

## Query support

LatticeDB supports a small, case-sensitive Cypher subset, not full openCypher or Neo4j query compatibility. Use uppercase clause keywords and lowercase `id(...)` and `count(...)`.

| Area | Supported scope |
| --- | --- |
| Matching | Nodes, multiple conjunctive labels, property maps, comma-separated patterns, and fixed-length paths with outgoing, incoming, or undirected relationships |
| Filtering | Property comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`), `id(binding) = expression`, `AND` / `OR` / `NOT`, parentheses, `IN`, `IS NULL`, `IS NOT NULL`, `STARTS WITH`, `ENDS WITH`, `CONTAINS` |
| Results | Bindings, properties, `id(binding)`, `AS`, `DISTINCT`, multi-key `ORDER BY`, `SKIP`, `LIMIT` |
| Aggregation | A single `count(*)` or `count(binding)` result, optionally aliased; no grouped or mixed aggregate projections |
| Writes | Node `CREATE`; directed relationship `CREATE` between matched bindings; property and label `SET` / `REMOVE`; map replacement (`SET n = $props`) and merge (`SET n += $props`); `DELETE` / `DETACH DELETE` |
| Batch input | One `UNWIND` feeding `RETURN`, node `CREATE`, or `MATCH` with its supported terminal clause |
| Search | Vector ranking with `n.embedding <=> $vector`; property-scoped full-text search with `n.text @@ $query`. Search predicates can be combined with `AND`, but cannot occur under `OR` or `NOT`. |
| Values | Scalar and map literals; parameters also carry lists, bytes, and vectors. Backtick-quoted identifiers support Unicode and spaces. |

For example, pass a list as a parameter instead of writing a list literal:

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

- No `OPTIONAL MATCH`, `MERGE`, `WITH`, `UNION`, variable-length paths, query comments, or multiple statements. One trailing semicolon is allowed.
- No list literals, arithmetic expressions, general function calls, `RETURN *`, or literal projections such as `RETURN 1`. `count(n.name)`, `count(DISTINCT n)`, `sum`, `avg`, and `collect` are unsupported.
- Use spaces around binary predicate and assignment operators: `n.age = 1`, not `n.age=1`. Inequality is `<>`, not `!=`.
- A `MATCH` has one terminal clause. `SET`, `REMOVE`, and relationship `CREATE` may be followed by `RETURN`; `DELETE` cannot. Top-level `CREATE` creates a node, not an entire path, and cannot be followed by `SET`.
- Missing properties evaluate to `NULL`. `SET n.prop = null` removes a property. Plain `DELETE` rejects nodes with incident edges; use `DETACH DELETE` to remove those edges too.
- On mutation queries, `SKIP` and `LIMIT` restrict returned rows, not writes. With `RETURN DISTINCT`, every `ORDER BY` expression must also be projected.
- An undirected match can return both orientations of an edge; a self-loop returns one. Use explicit `ORDER BY` when application behavior depends on result order.

Query search predicates score candidate rows and sort matches before `LIMIT`. Configured `FTSProperties` can accelerate eligible `@@` predicates; `ApproximateVector` opts eligible queries into HNSW candidates. See [query search candidates](docs/query-search-candidates.md) for exact defaults and fallback rules. Direct `VectorSearch` supports the global vector space and [configured namespaces](docs/vector-namespaces.md); direct `FTSSearch` uses explicitly indexed node text. HNSW state is a [validated, disposable cache](docs/vector-cache.md) that accelerates reopening a database.

Use parameters for application values and explicit aliases for result columns. Queries default to 1,000,000 rows, 10,000,000 work units, and 64 MiB of live logical bytes. Use `QueryContext` with a deadline and explicit `QueryOptions` limits for your workload; logical byte budgets do not measure process RSS. Query text is limited to 32 KiB.

See the [query semantics and full grammar](docs/engine_conformance.md#query-semantics) for exact clause combinations and behavior. The [canonical grammar](internal/engine/testdata/query_grammar.ebnf) and [accepted/rejected syntax tests](internal/engine/query_grammar_test.go) are checked against the parser.

## Storage and transaction contract

### Opening and sizing

- The database uses one active writer. `BeginWriteContext` waits for the writer slot until cancellation; `Begin(false)` and `Update` can return `ErrWriteTxActive` on contention.
- `MaxDatabaseSnapshotBytes` defaults to 512 MiB of canonical snapshot data. Set it explicitly for larger databases; exceeding it rejects a commit with `ErrResourceLimit`. This is not an RSS limit or an on-disk paging cache.

- Entity IDs (nodes, edges, and edge endpoints) are uint64 values in `1..MaxInt64`.
  `MaxInt64+1` is reserved as the high-water exhaustion sentinel and is never
  allocated.
- WAL is always enabled: `OpenOptions.EnableWAL`, `DisableWAL`, and `EnableAdjacencyCache` must remain false (their default); true requests return `ErrUnsupportedOption`.
- `OpenOptions.CacheSizeMB` and `PageSize` are compatibility fields only. They must remain zero (their default); every nonzero request, including former `100` and `4096` values, returns `ErrUnsupportedOption`.
- Current files use state v4 and WAL v3 formats. Older metadata-free state v3 and WAL v2 files are readable, but new files intentionally fail closed in older binaries.

### Transactions, snapshots, and maintenance

- A `Tx` is single-owner and must not be used concurrently.
- `Commit` and `CommitContext` are one-shot: the transaction becomes inactive whether the commit succeeds or fails.
- Multiple online snapshots may be active per database. Writers can continue after each snapshot captures its generation; callers must close snapshots when finished.
- `BeginSnapshot` retries internal checkpoint contention using the same bounded acquisition as write transactions. An active application writer still returns `ErrWriteTxActive` without waiting for the transaction.
- Application metadata updates copy the affected shard instead of the complete key map. This preserves immutable read and snapshot generations; the fixed shard count reduces copying but does not guarantee constant cost for arbitrarily large or skewed key sets.
- `MaxGenerationLeases` and `MaxRetainedGenerationLogicalBytes` optionally bound admission of public read, snapshot, and export pins. Internal checkpoint and index-maintenance candidates are outside these counters. They never evict an active pin; retained bytes are canonical snapshot bytes, not RSS.
- On Linux, macOS, and Windows, writer opens take an exclusive database-path lock and `ReadOnly` opens take a shared lock. On js, Plan 9, and WASI, this lock is process-local only.
- `DisableLock` is explicitly unsafe; callers must ensure that the database has a single owner.
- Direct vector search has one global index and no property selector. Use one consistently named vector property and one embedding space per database. Each node contributes its lexicographically first vector-valued property; multiple vector properties do not create separate searchable namespaces.
- `RebuildVectorIndexContext` builds off the writer lock and replays bounded vector changes before publication. The initiating context owns a shared attempt; another caller may cancel its own wait. Existing maintenance limits still apply, and log exhaustion aborts the rebuild without rejecting an otherwise valid commit.
- During a background checkpoint, the active WAL append tail is bounded by `WALCheckpointThresholdBytes` plus one permitted WAL frame; once the bound is reached, commits return `ErrResourceLimit` before WAL mutation and must be retried as a new transaction after checkpoint progress. The marker frame's fixed file overhead is separate from that tail measurement.

The detailed behavioral contract is documented in [docs/engine_conformance.md](docs/engine_conformance.md), with the value model in [docs/value_model.md](docs/value_model.md).

CSV export returns and atomically publishes a JSON manifest whose `nodes` and `edges` paths point into `<output>_generations`. Published generations remain immutable and are not reclaimed automatically. For explicit pruning, every reader must hold an `OpenCSVGenerationContext` lease until it finishes reading; `PruneCSVGenerationsContext` protects current and leased generations. Legacy readers are not protected during pruning. Pruning requires native Unix locking and directory sync; unsupported platforms return an error.

## Development

```sh
go test ./...
go test -race ./...
(cd conformance/go && go test ./...)

# Bounded fuzz smoke (each target caps inputs below 64 KiB and recovery work at 100K; WAL covers current v3 and readable legacy v2 frames)
go test ./internal/engine -run '^$' -fuzz '^FuzzParseQuery$' -fuzztime=5s -parallel=1
for target in FuzzDeserializeGraphState FuzzLoadLatestWALFrames FuzzNestedValueRoundTrip; do
  go test ./internal/store -run '^$' -fuzz "^${target}$" -fuzztime=5s -parallel=1
done
```

## License

[MIT](LICENSE)
