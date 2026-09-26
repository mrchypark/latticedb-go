package pagestore

import "errors"

var (
	ErrClosed             = errors.New("pagestore: transaction or database is closed")
	ErrReadOnly           = errors.New("pagestore: read-only transaction")
	ErrActiveTransactions = errors.New("pagestore: active transactions")
	ErrWriterActive       = errors.New("pagestore: writable transaction is already active")
	ErrSnapshotGrowth     = errors.New("pagestore: write may require mmap growth while read snapshots are open")
	ErrSnapshotWriteLimit = errors.New("pagestore: write exceeds snapshot growth limit")
	ErrInvalidOptions     = errors.New("pagestore: invalid options")
)

var ErrUnsupportedPlatform = errors.New("page storage is unavailable on this platform")

// Options configures Open. MaxSnapshotWriteBytes bounds the key and value bytes
// staged by one writable transaction while read snapshots are open. Zero uses
// a 16 MiB default; negative values are invalid. It bounds preflight work, not
// the size of the database. The on-disk database remains subject to bbolt's
// platform size limit.
type Options struct {
	ReadOnly              bool
	MaxSnapshotWriteBytes int64
}
