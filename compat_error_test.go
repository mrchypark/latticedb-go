package latticedb

import (
	"errors"
	"syscall"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

func TestUpstreamStructuredErrorsAndDefaultStorageOptions(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true, CacheSizeMB: 100, PageSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	read, err := db.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	var latticeErr *Error
	if err := db.Close(); !errors.As(err, &latticeErr) || latticeErr.Code != ErrorInvalidArg || !errors.Is(err, ErrTransactionsActive) {
		t.Fatalf("Close with active transaction = %v", err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	write, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginWrite(); !errors.As(err, &latticeErr) || latticeErr.Code != ErrorLockTimeout || !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("second writer = %v", err)
	}
	if err := write.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		_, err := tx.FindNodesByLabelProperty("Person", "email", "a@example.com", 1)
		return err
	}); !errors.As(err, &latticeErr) || latticeErr.Code != ErrorUnsupported || !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("missing property index = %v", err)
	}
}

func TestQueryErrorsExposeStage(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var queryErr *QueryError
	if _, err := db.Query("NOT CYPHER", nil); !errors.As(err, &queryErr) || queryErr.Stage != QueryErrorStageParse || queryErr.Code != ErrorGeneric || queryErr.Location != nil || queryErr.DiagnosticCode != "" {
		t.Fatalf("parse error = %#v", err)
	}
	if _, err := db.Query("UNWIND $items AS x RETURN x", map[string]Value{"items": 1}); !errors.As(err, &queryErr) || queryErr.Stage != QueryErrorStageExecution || queryErr.Code != ErrorGeneric || queryErr.Location != nil || queryErr.DiagnosticCode != "" {
		t.Fatalf("execution error = %#v", err)
	}
}

func TestQueryErrorUnknownEngineStageIsNotExecution(t *testing.T) {
	err := wrapError(&engine.QueryError{Stage: engine.QueryErrorStage(99), Err: errors.New("future stage")})
	var queryErr *QueryError
	if !errors.As(err, &queryErr) || queryErr.Stage != QueryErrorStageNone {
		t.Fatalf("unknown stage = %#v", err)
	}
}

func TestCriticalSentinelWrappingPreservesRetrySemantics(t *testing.T) {
	var latticeErr *Error
	err := wrapError(ErrCommitOutcomeUnknown)
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorIO || !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("commit outcome error = %v", err)
	}
	err = wrapError(ErrWriteConflict)
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorTxnAborted || !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("write conflict = %v", err)
	}
	err = wrapError(ErrRecoveryRequired)
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorIO || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("recovery-required error = %v", err)
	}
}

func TestCompatibilityClassifiesSyscallErrnoAsIO(t *testing.T) {
	errNoSpace := syscall.Errno(28)
	var latticeErr *Error
	err := wrapError(errNoSpace)
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorIO || !errors.Is(err, errNoSpace) {
		t.Fatalf("ENOSPC error = %v, want ErrorIO preserving errno", err)
	}
}

func TestCompatibilityMethodsReturnStructuredErrors(t *testing.T) {
	var latticeErr *Error
	if _, err := Open(t.TempDir(), OpenOptions{Create: true, DisableWAL: true}); !errors.As(err, &latticeErr) || !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("DisableWAL Open error = %v", err)
	}
	if _, err := Deserialize(nil, OpenOptions{DisableWAL: true}); !errors.As(err, &latticeErr) || !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("DisableWAL Deserialize error = %v", err)
	}
	var db *DB
	if _, err := db.GetNodesByLabel("Person"); !errors.As(err, &latticeErr) || !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("nil GetNodesByLabel error = %v", err)
	}
	var tx *Tx
	if err := tx.DeleteEdge(1, 2, "KNOWS"); !errors.As(err, &latticeErr) || !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("nil DeleteEdge error = %v", err)
	}
	opened, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	read, err := opened.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	if err := read.DeleteEdge(1, 2, "KNOWS"); !errors.As(err, &latticeErr) || !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only DeleteEdge error = %v", err)
	}
}

func TestManagedCallbacksPreserveErrorIdentity(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	callbackErr := &callbackError{cause: ErrAlreadyExists}
	if err := db.View(func(*Tx) error { return callbackErr }); err != callbackErr {
		t.Fatalf("View callback error = %p, want %p", err, callbackErr)
	}
	if err := db.Update(func(*Tx) error { return callbackErr }); err != callbackErr {
		t.Fatalf("Update callback error = %p, want %p", err, callbackErr)
	}
}

type callbackError struct{ cause error }

func (e *callbackError) Error() string { return "callback: " + e.cause.Error() }
func (e *callbackError) Unwrap() error { return e.cause }
