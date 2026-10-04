package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPageBackupRestoresStreamOperations(t *testing.T) {
	for _, mode := range []string{"offset", "trim", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			archive := filepath.Join(root, "archive")
			db, err := Open(filepath.Join(root, "db"), OpenOptions{Create: true, PageStorage: true, BackupDirectory: archive})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			if mode != "offset" {
				if err = db.Update(func(tx *Tx) error {
					for range 3 {
						if err := tx.PublishStream("events", "event", "payload"); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err = db.Update(func(tx *Tx) error {
				if mode != "trim" {
					if err := tx.SetStreamOffset("events", "worker", 42); err != nil {
						return err
					}
				}
				if mode != "offset" {
					return tx.TrimStream("events", 2)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(root, "restored")
			if _, err = RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{}); err != nil {
				t.Fatal(err)
			}
			restored, err := Open(destination, OpenOptions{PageStorage: true})
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			if mode != "trim" {
				offset, ok, err := restored.GetStreamOffset("events", "worker")
				if err != nil || !ok || offset != 42 {
					t.Fatalf("offset=%d present=%v error=%v", offset, ok, err)
				}
			}
			if mode != "offset" {
				records, err := restored.ReadStream("events", 0, 10, 0)
				if err != nil || len(records) != 1 || records[0].Sequence != 3 || records[0].Payload != "payload" {
					t.Fatalf("records=%v error=%v", records, err)
				}
			}
		})
	}
}

func TestPageStreamReadPinsGeneration(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error { return tx.PublishStream("events", "event", "original") }); err != nil {
		t.Fatal(err)
	}
	ctx := &streamReadWindowContext{Context: context.Background(), onSecondErr: func() {
		if db.activeGenerationLeases != 1 {
			t.Fatalf("read has %d leases", db.activeGenerationLeases)
		}
		if err := db.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); err != nil {
			t.Fatal(err)
		}
	}}
	got, err := db.ReadStreamContext(ctx, "events", 0, StreamReadOptions{Limit: 1})
	if err != nil || len(got.Records) != 1 || got.Records[0].Payload != "original" {
		t.Fatalf("result=%v error=%v", got, err)
	}
	if db.activeGenerationLeases != 0 {
		t.Fatal("leaked read lease")
	}
	ctx2, cancel := context.WithCancel(context.Background())
	window := &streamReadWindowContext{Context: ctx2, onSecondErr: cancel}
	if _, err = db.ReadStreamContext(window, "events", 0, StreamReadOptions{Limit: 1}); err != context.Canceled {
		t.Fatalf("cancel error=%v", err)
	}
	if db.activeGenerationLeases != 0 {
		t.Fatal("leaked canceled lease")
	}
}

func TestDistinctMapKeysNonPowerOfTwo(t *testing.T) {
	for _, n := range []int{3, 5, 9, 17} {
		keys := make([]string, n)
		for i := range keys {
			keys[i] = string(rune('z' - i))
		}
		for rotation := range n {
			b := newQueryBudget(context.Background(), QueryOptions{})
			got, err := sortDistinctMapKeys(keys, make([]string, n), b)
			releaseQueryBudget(b)
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i < n; i++ {
				if got[i-1] >= got[i] {
					t.Fatalf("n=%d unsorted=%v", n, got)
				}
			}
			keys = append(keys[1:], keys[0])
			_ = rotation
		}
	}
}

func TestPageRestoreIgnoresPrunedIndependentChain(t *testing.T) {
	r := t.TempDir()
	p := filepath.Join(r, "db")
	a := filepath.Join(r, "archive")
	open := func(archive string) *DB {
		d, e := Open(p, OpenOptions{Create: true, PageStorage: true, BackupDirectory: archive})
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	add := func(d *DB) {
		if e := d.Update(func(tx *Tx) error { _, e := tx.CreateNode(CreateNodeOptions{}); return e }); e != nil {
			t.Fatal(e)
		}
	}
	d := open(a)
	add(d)
	add(d)
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	d = open("")
	add(d)
	add(d)
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	d = open(a)
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	files, e := os.ReadDir(a)
	if e != nil {
		t.Fatal(e)
	}
	removed := false
	for _, f := range files {
		if strings.HasSuffix(f.Name(), backupSegmentSuffix) {
			entry, e := parseBackupSegment(f.Name())
			if e != nil {
				t.Fatal(e)
			}
			if entry.metadata.CommitID == 1 {
				if e = os.Remove(filepath.Join(a, f.Name())); e != nil {
					t.Fatal(e)
				}
				removed = true
			}
		}
	}
	if !removed {
		t.Fatal("no commit1 WAL")
	}
	_, e = RestoreBackup(context.Background(), a, filepath.Join(r, "restore"), BackupRestoreOptions{CommitID: func() *uint64 { v := uint64(4); return &v }()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = RestoreBackup(context.Background(), a, filepath.Join(r, "missing-parent"), BackupRestoreOptions{CommitID: func() *uint64 { v := uint64(2); return &v }()})
	if e == nil {
		t.Fatal("required missing predecessor accepted")
	}
	files, e = os.ReadDir(a)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), backupSegmentSuffix) {
			if e = os.WriteFile(filepath.Join(a, f.Name()), []byte("corrupt"), 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	_, e = RestoreBackup(context.Background(), a, filepath.Join(r, "restore-after-corruption"), BackupRestoreOptions{})
	if e != nil {
		t.Fatal(e)
	}
	// Continuing this source needs base4 and its future WALs, not the old chain.
	d = open(a)
	add(d)
	if e := d.Close(); e != nil {
		t.Fatal(e)
	}
	metadata, e := RestoreBackup(context.Background(), a, filepath.Join(r, "restore-after-resume"), BackupRestoreOptions{})
	if e != nil || metadata.CommitID != 5 {
		t.Fatalf("resumed restore=%+v err=%v", metadata, e)
	}
	files, e = os.ReadDir(a)
	if e != nil {
		t.Fatal(e)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), backupSegmentSuffix) {
			continue
		}
		entry, e := parseBackupSegment(file.Name())
		if e != nil {
			t.Fatal(e)
		}
		if entry.metadata.CommitID == 5 {
			if e := os.WriteFile(filepath.Join(a, file.Name()), []byte("corrupt"), 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	if reopened, err := Open(p, OpenOptions{PageStorage: true, BackupDirectory: a}); err == nil {
		reopened.Close()
		t.Fatal("source reopen accepted required-chain corruption")
	}
}

func TestPageBackupRestoresAutomaticChangefeedTrim(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "archive")
	db, err := Open(filepath.Join(root, "db"), OpenOptions{Create: true, PageStorage: true, BackupDirectory: archive, ChangefeedMaxBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for range 24 {
		if err = db.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := db.Changes(0, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) == 0 || expected[0].Sequence == 1 {
		t.Fatal("fixture did not auto-trim")
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "restored")
	if _, err = RestoreBackup(context.Background(), archive, dest, BackupRestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	r, err := Open(dest, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.Changes(0, 100, 0)
	if err != nil || len(got) != len(expected) || got[0].Sequence != expected[0].Sequence {
		t.Fatalf("restored=%v expected=%v error=%v", got, expected, err)
	}
}

func TestDistinctMapQueryCanonicalResults(t *testing.T) {
	db, err := Open(":memory:", OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	values := make([]any, 32)
	for i := range values {
		values[i] = map[string]any{"c": int64(3), "a": int64(1), "b": int64(2)}
	}
	for _, query := range []string{"UNWIND $values AS v RETURN DISTINCT v", "UNWIND $values AS v WITH DISTINCT v RETURN v", "UNWIND $values AS v RETURN v, count(*) AS n", "UNWIND $values AS v RETURN count(DISTINCT v) AS n"} {
		result, err := db.Query(query, map[string]any{"values": values})
		if err != nil || len(result.Rows) != 1 {
			t.Fatalf("query=%s rows=%v error=%v", query, result.Rows, err)
		}
		if query == "UNWIND $values AS v RETURN v, count(*) AS n" && result.Rows[0]["n"] != int64(32) {
			t.Fatalf("groups=%v", result.Rows)
		}
		if query == "UNWIND $values AS v RETURN count(DISTINCT v) AS n" && result.Rows[0]["n"] != int64(1) {
			t.Fatalf("distinct=%v", result.Rows)
		}
	}
}

func TestPageStreamLongPollReleasesGeneration(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := db.ReadStreamContext(ctx, "empty", 0, StreamReadOptions{Limit: 1}); done <- err }()
	waitForStreamWaiters(t, db, "empty", 1)
	db.mu.Lock()
	leases := db.activeGenerationLeases
	db.mu.Unlock()
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatal(err)
	}
	if leases != 0 {
		t.Fatalf("long poll retained %d leases", leases)
	}
}

func TestPageRestoreRejectsDuplicateInventoryCommit(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	archive := filepath.Join(root, "archive")
	db, err := Open(source, OpenOptions{Create: true, PageStorage: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := readArchiveEntries(context.Background(), archive, db.graph.DatabaseID, false)
	if err != nil {
		t.Fatal(err)
	}
	var old backupEntry
	for _, entry := range entries {
		if !entry.segment {
			old = entry
			break
		}
	}
	if old.path == "" {
		t.Fatal("missing full base")
	}
	data, err := os.ReadFile(old.path)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := old.metadata
	duplicate.CapturedAt = duplicate.CapturedAt.Add(time.Nanosecond)
	if err := os.WriteFile(filepath.Join(archive, backupFilename(duplicate, old.digest)), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackup(context.Background(), archive, filepath.Join(root, "restore"), BackupRestoreOptions{}); err == nil {
		t.Fatal("unselected duplicate commit accepted")
	}
}
