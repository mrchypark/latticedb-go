# Local source snapshot spike (#261)

Use two real page-backed sources: canonical nodes/adjacency and persisted FTS
postings. Record database identity, commit identity and whether a source can
prove its index belongs to that commit. An independently acquired head result
does not prove it belongs to a pinned older graph. Compare capabilities without
introducing a production interface.

The probe must pin a transaction, change both FTS text and an outgoing edge in
a later writer, and demonstrate that the old transaction still sees both old
values while a new request sees both new values. Use the #260 composed request
to prove one budget covers both phases. Use exact identity checks in the probe
to reject a different database, a different commit, an unverifiable watermark,
and an arbitrary historical request; distinguish these probe rejections from
production APIs. The experiment pins the current head using the existing
read transaction API. It adds no historical query capability. Backup restore
is a separate facility.

Graph can use label/property postings and adjacency; FTS candidate generation
can use the ready same-snapshot postings. Qualifying target label and final
row limits apply after candidate selection. An unavailable derived index may
only fall back to canonical data from the same snapshot. Verify cancellation,
no partial results, and exhaustion after acquisition but before graph results.
No remote adapter, generic source protocol, or public as-of API.

## Compared sources

| Capability | Canonical graph | Manually indexed local FTS |
| --- | --- | --- |
| Data and output | Nodes, labels, edges; one row per qualifying outgoing edge | Persisted term postings and frequencies; ranked node ID and native score |
| Snapshot identity | Database ID and commit ID pinned by `BeginRead` | Same transaction and page generation in `SearchGraphContext`; direct `DB.FTSSearchContext` opens its own head |
| Watermark and fallback | Canonical data defines the commit | Ready index must belong to the pinned page generation. If unavailable, search canonical FTS records on that generation |
| Historical read | Existing reader can retain its pinned head across a writer commit | Same retained generation through the read Tx. No arbitrary as-of commit request |
| Work pushed to the source | Label/property access where applicable; outgoing adjacency scan for composition | Analyzer, ranked search, and CandidateK |
| Work applied after acquisition | Edge type and target label, edge multiplicity, ResultK | No refill after graph filters |
| Limits and cancellation | Shares the composed call's meter, including examined edges and output | Candidate retention and scoring consume that same meter; separate API calls have separate budgets |

Run the experiment with:

```sh
go test -race ./internal/engine -run '^TestLocalSourceSnapshotSpike$' -count=1
```

The test uses 1 KiB pages so the small writer fits inside the mmap cushion
while a read snapshot is pinned. It inspects persisted FTS readiness, pins a
read transaction, updates both
the indexed text and edge in a writer, and checks both generations. The probe
rejects different database IDs, different commits, absent watermark proof, and
a historical request for the actual earlier commit. The two databases have the
same commit number in the mismatch test.
A paired request with `MaxRows=2` fits without expansion and fails without
partial rows with expansion. A canceled request returns no rows. `TestFTSPinnedCoreUsesPinnedGraphForCanonicalFallback`
checks that unavailable FTS index access falls back on the same pinned graph.

The probe's identity comparison is test code. The production composition API
accepts no foreign source handles, and does not expose a public mismatch
classifier or arbitrary commit lookup. Both source reads already share the
same `GraphState` within one production request. An interface would add a
second admission path without removing duplicate production code, so this
spike recommends leaving the existing API in place.
