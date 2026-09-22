package latticedb_test

import (
	"errors"
	lattice "github.com/mrchypark/latticedb-go"
	"path/filepath"
	"testing"
)

func TestSnapshotConcurrentCloseAndBackup(t *testing.T) {
	for _, backup := range []bool{false, true} {
		name := "CloseClose"
		if backup {
			name = "BackupClose"
		}
		t.Run(name, func(t *testing.T) {
			db, err := lattice.Open(t.TempDir(), lattice.OpenOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query("CREATE (:Item {value: 42})", nil); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i++ {
				snapshot, err := db.BeginSnapshot()
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "backup.ltdb")
				start := make(chan struct{})
				closed := make(chan error, 1)
				other := make(chan error, 1)
				go func() { <-start; closed <- snapshot.Close() }()
				go func() {
					<-start
					if backup {
						other <- snapshot.Backup(path)
					} else {
						other <- snapshot.Close()
					}
				}()
				close(start)
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				err = <-other
				if !backup && err != nil || backup && err != nil && !errors.Is(err, lattice.ErrDatabaseClosed) {
					t.Fatal(err)
				}
				if backup && err == nil {
					copyDB, err := lattice.Open(path, lattice.OpenOptions{ReadOnly: true})
					if err != nil {
						t.Fatal(err)
					}
					result, err := copyDB.Query("MATCH (n:Item) RETURN n.value AS value", nil)
					closeErr := copyDB.Close()
					if err != nil || closeErr != nil || len(result.Rows) != 1 || result.Rows[0]["value"] != int64(42) {
						t.Fatalf("backup=%v err=%v close=%v", result, err, closeErr)
					}
				}
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
				if err := snapshot.Backup(path); !errors.Is(err, lattice.ErrDatabaseClosed) {
					t.Fatalf("after close: %v", err)
				}
				stats, err := db.OperationalStats()
				if err != nil {
					t.Fatal(err)
				}
				if stats.ActiveSnapshots != 0 {
					t.Fatalf("active snapshots=%d", stats.ActiveSnapshots)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
