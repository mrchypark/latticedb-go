package latticedb

import (
	"errors"
	"fmt"
	"os"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

// ErrorCode identifies a low-level database error. Values match the upstream
// Go binding so callers can keep using errors.As and code comparisons.
type ErrorCode int

const (
	ErrorOK              ErrorCode = 0
	ErrorGeneric         ErrorCode = -1
	ErrorIO              ErrorCode = -2
	ErrorCorruption      ErrorCode = -3
	ErrorNotFound        ErrorCode = -4
	ErrorAlreadyExists   ErrorCode = -5
	ErrorInvalidArg      ErrorCode = -6
	ErrorTxnAborted      ErrorCode = -7
	ErrorLockTimeout     ErrorCode = -8
	ErrorReadOnly        ErrorCode = -9
	ErrorFull            ErrorCode = -10
	ErrorVersionMismatch ErrorCode = -11
	ErrorChecksum        ErrorCode = -12
	ErrorOutOfMemory     ErrorCode = -13
	ErrorUnsupported     ErrorCode = -14
	ErrorValueTooLarge   ErrorCode = -15
	ErrorDatabaseLocked  ErrorCode = -16
)

// QueryErrorStage identifies the phase in which a query failed.
type QueryErrorStage int

const (
	QueryErrorStageNone      QueryErrorStage = 0
	QueryErrorStageParse     QueryErrorStage = 1
	QueryErrorStageSemantic  QueryErrorStage = 2
	QueryErrorStagePlan      QueryErrorStage = 3
	QueryErrorStageExecution QueryErrorStage = 4
)

var (
	// These aliases preserve the upstream sentinel names. Pure-Go errors may
	// still carry richer engine-specific sentinels where appropriate.
	ErrReadOnlyDatabase = ErrReadOnly
	ErrReadOnlyTx       = ErrReadOnly
)

// Error is a structured database error.
type Error struct {
	Code    ErrorCode
	Message string
	cause   error
}

func (e *Error) Error() string { return e.Message }

func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && e.Code == other.Code
}

func (e *Error) Unwrap() error { return e.cause }

// QueryErrorLocation identifies the source span associated with a query error.
type QueryErrorLocation struct {
	Line   uint32
	Column uint32
	Length uint32
}

// QueryError is a structured query parsing, planning, or execution error.
type QueryError struct {
	Code           ErrorCode
	Stage          QueryErrorStage
	Message        string
	DiagnosticCode string
	Location       *QueryErrorLocation
	cause          error
}

func (e *QueryError) Error() string {
	if e.DiagnosticCode != "" {
		return fmt.Sprintf("%s (%s)", e.Message, e.DiagnosticCode)
	}
	return e.Message
}

func (e *QueryError) Unwrap() error { return e.cause }

func wrapError(err error) error {
	if err == nil {
		return nil
	}
	return wrapErrorSlow(err)
}

func wrapErrorSlow(err error) error {
	var latticeErr *Error
	if errors.As(err, &latticeErr) {
		return err
	}
	var queryErr *engine.QueryError
	if errors.As(err, &queryErr) {
		// The engine currently emits only parse and execution errors. Keep an
		// unknown engine stage explicit instead of silently reporting execution.
		stage := QueryErrorStageNone
		switch queryErr.Stage {
		case engine.QueryErrorStageParse:
			stage = QueryErrorStageParse
		case engine.QueryErrorStageExecution:
			stage = QueryErrorStageExecution
		}
		code, _ := classifyError(queryErr.Err)
		return &QueryError{Code: code, Stage: stage, Message: queryErr.Error(), cause: err}
	}
	code, ok := classifyError(err)
	if !ok {
		return err
	}
	return &Error{Code: code, Message: err.Error(), cause: err}
}

func classifyError(err error) (ErrorCode, bool) {
	switch {
	case errors.Is(err, engine.ErrCommitOutcomeUnknown):
		return ErrorIO, true
	case errors.Is(err, engine.ErrWriteTxActive):
		return ErrorLockTimeout, true
	case errors.Is(err, engine.ErrWriteConflict):
		return ErrorTxnAborted, true
	case errors.Is(err, engine.ErrDatabaseLocked):
		return ErrorDatabaseLocked, true
	case errors.Is(err, engine.ErrDatabaseLayoutConflict):
		return ErrorInvalidArg, true
	case errors.Is(err, engine.ErrAlreadyExists):
		return ErrorAlreadyExists, true
	case errors.Is(err, engine.ErrReadOnly):
		return ErrorReadOnly, true
	case errors.Is(err, engine.ErrUnsupportedOption):
		return ErrorUnsupported, true
	case errors.Is(err, engine.ErrResourceLimit):
		return ErrorFull, true
	case errors.Is(err, engine.ErrRecoveryRequired):
		return ErrorIO, true
	case errors.Is(err, engine.ErrInvalidArgument),
		errors.Is(err, engine.ErrDatabaseClosed),
		errors.Is(err, engine.ErrInactiveTx),
		errors.Is(err, engine.ErrTransactionsActive),
		errors.Is(err, engine.ErrSnapshotActive),
		errors.Is(err, engine.ErrManagedTransaction):
		return ErrorInvalidArg, true
	case errors.Is(err, os.ErrNotExist):
		return ErrorNotFound, true
	case isOSError(err):
		return ErrorIO, true
	default:
		return ErrorGeneric, false
	}
}

func isOSError(err error) bool {
	var pathErr *os.PathError
	var linkErr *os.LinkError
	var syscallErr *os.SyscallError
	return errors.As(err, &pathErr) || errors.As(err, &linkErr) || errors.As(err, &syscallErr)
}
