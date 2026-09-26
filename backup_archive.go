package latticedb

import (
	"context"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

// RestoreBackup restores one immutable full-checkpoint archive entry. Archive
// capture is opt-in through OpenOptions.BackupDirectory; it is full-snapshot
// PITR, not incremental WAL or delta shipping.
func RestoreBackup(ctx context.Context, directory, destination string, opts BackupRestoreOptions) (BackupMetadata, error) {
	metadata, err := engine.RestoreBackup(ctx, directory, destination, engine.BackupRestoreOptions{CommitID: opts.CommitID, Before: opts.Before, MaxDatabaseSnapshotBytes: opts.MaxDatabaseSnapshotBytes})
	return BackupMetadata{CommitID: metadata.CommitID, CapturedAt: metadata.CapturedAt}, wrapError(err)
}
