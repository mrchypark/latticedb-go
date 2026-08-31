# LatticeDB Go

An embedded graph database written entirely in Go. It provides transactional graph operations, Cypher-style queries, full-text search, vector search, durable WAL recovery, streams, exports, and online snapshots without cgo.

## Install

LatticeDB Go requires Go 1.27 or newer.

```sh
go get github.com/mrchypark/latticedb-go@v0.2.1
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

	result, err := db.Query("MATCH (n:Person) RETURN n.name", nil)
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

## Storage and transaction contract

- v0.1 uses the new state v4 and WAL v3 formats. Older metadata-free state v3 and WAL v2 files are readable, but new files intentionally fail closed in older binaries.
- A `Tx` is single-owner and must not be used concurrently.
- `Commit` and `CommitContext` are one-shot: the transaction becomes inactive whether the commit succeeds or fails.
- Only one online snapshot may be active per database. Writers can continue after the snapshot generation is captured.

The detailed behavioral contract is documented in [docs/engine_conformance.md](docs/engine_conformance.md), with the value model in [docs/value_model.md](docs/value_model.md).

CSV export returns and atomically publishes a JSON manifest whose `nodes` and `edges` paths point into `<output>_generations`. Published generations remain immutable and are not reclaimed automatically because readers do not hold leases.

## Development

```sh
go test ./...
go test -race ./...
(cd conformance/go && go test ./...)
```

## License

[MIT](LICENSE)
