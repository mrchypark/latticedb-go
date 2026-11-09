# Search score projection and one-hop composition (#260)

The Go API runs against one active read transaction. The database method opens
the transaction; the transaction method uses the caller's pinned snapshot.
Start with exactly one native search: manually indexed FTS (frequency or BM25,
higher is better) or configured L2 vector (lower is better). Report the native
score/distance, FTS analyzer or resolved vector namespace, and whether the
result ranking used exact search or ANN. An
exact fallback is reported as exact. Keep existing Cypher predicates and direct
search semantics unchanged.

CandidateK bounds ranked candidates before graph work. Optional expansion
follows one outgoing edge type, optionally checking the target label. These
are postfilters: no extra candidates are fetched to compensate for filtering.
ResultK bounds final rows after expansion. Without expansion, emit one row per
candidate; with expansion, emit one row per edge, including parallel edges.
Sort by native metric, then source node ID, then target node ID and edge ID. An empty expansion can
produce no rows. Never mix or normalize metrics in one request.

The search index and adjacency are read from the same snapshot. Use a single
work and memory meter for tokenization/vector search, candidate retention, page
decoding, edge/target checks, tie handling, and result retention. Charge MaxRows cumulatively for acquired candidate rows, examined edge rows,
and emitted rows. CandidateK and ResultK do not reset the meter. The default
limits are one million rows, ten million work units, and 64 MiB of working
storage. Cancel or fail
without partial rows. Reject invalid source selection, zero K, or malformed
filters. No arbitrary predicates, relationship lengths, weighted paths,
shortest paths, or public source framework.

Tests compare DB and Tx on memory and disk, prove old-reader pinning, exact/ANN
score identity and deterministic ties, parallel edges, filtering underfill, and
failures during graph work. Reuse the pinned SciFact inputs and existing BM25
baseline. Compose deterministic graph links in the fixture and compare full
row identities/scores against direct native search plus a reference join at
the same candidate depth; measure ranked document recall, logical work/bytes,
and Go allocation cost. Report exact-vs-ANN recall separately; SciFact has no
native vector labels, so any vector fixture is a separate synthetic control. The synthetic links test composition and do not add
relevance labels to SciFact.
