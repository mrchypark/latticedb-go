# Backup archive integrity policy

LatticeDB rehashes the current archive head file before it publishes a newer
head. This is the current integrity policy. It remains unchanged.

## Commit cost

The cost applies only when `writeArchiveHead` replaces an existing head. It
opens the previous entry and hashes every byte before it writes and renames the
new head file. A first archive entry has no previous entry to rehash.

For a full backup of size `B`, the next archive-head update reads and hashes
`B` bytes. A small WAL segment does not reduce this part of the operation. For
example, a 1 KiB segment after a 100 GiB full backup causes a 100 GiB previous
file hash. This does not mean that every commit reads the whole database. It
means that a commit which advances an archive head rehashes its selected
previous head file.

For page-backed commits, this work runs after the page transaction commits and
before the writer releases `db.mu`. It adds read and SHA-256 work to that
commit's latency. The exact elapsed time depends on the filesystem cache,
storage device, and CPU. The benchmark does not report read system-call counts
or physical disk reads.

Run the focused benchmark with:

```sh
go test ./internal/engine -run '^$' -bench '^BenchmarkWriteArchiveHeadPreviousFullRehashBytes$' -benchmem
```

The benchmark derives `previous_rehash_bytes` from the fixture file size. It is
1024 for the 1 KiB case and 1048576 for the 1 MiB case. The metric documents
the input that the current unconditional `io.Copy` passes to SHA-256. It is not
an observed read-byte counter. It will not detect a future change that skips the
rehash.

## Recorded benchmark result

The benchmark produced this result on 2026-11-08 with Go on darwin/arm64 and an
Apple M3. It is one local timing sample, not a disk-read measurement.

```text
BenchmarkWriteArchiveHeadPreviousFullRehashBytes/1024B-8        177   6882136 ns/op      1024 previous_rehash_bytes   38770 B/op  47 allocs/op
BenchmarkWriteArchiveHeadPreviousFullRehashBytes/1048576B-8     170   8215178 ns/op   1048576 previous_rehash_bytes   38849 B/op  47 allocs/op
```

The corruption test uses synthetic bytes. It proves that the current digest
comparison rejects changed bytes and preserves the old head. It does not prove
that the synthetic file is a valid backup checkpoint.

## Why LatticeDB does not use a cached digest

The archive head stores a digest. Reusing that stored digest would avoid the
full read. It would also trust metadata that can outlive a file modification.
An external process, storage fault, or direct file change could change the
previous entry while its head metadata still contains the old digest. A cached
digest policy would not detect that corruption when it advances the head.

The current rehash compares the bytes on disk with both the entry filename and
the old head digest before it replaces the head. If the comparison fails,
`writeArchiveHead` returns an error and preserves the old head. Full validation
on archive open and recovery provides separate integrity checks. Those checks
do not replace the commit-time detection policy.
