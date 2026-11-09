package latticedb

import "github.com/mrchypark/latticedb-go/internal/store"

// GraphConstraint applies a requirement to one node label and property.
type GraphConstraint = store.GraphConstraint

// ConstraintKind selects the required, type, or unique rule.
type ConstraintKind = store.ConstraintKind

// ConstraintScalarType is the required scalar kind of a type constraint.
type ConstraintScalarType = store.ConstraintScalarType

const (
	ConstraintRequired = store.ConstraintKindRequired
	ConstraintType     = store.ConstraintKindType
	ConstraintUnique   = store.ConstraintKindUnique

	ConstraintBool    = store.ConstraintScalarBool
	ConstraintInt64   = store.ConstraintScalarInt64
	ConstraintFloat64 = store.ConstraintScalarFloat64
	ConstraintString  = store.ConstraintScalarString
)

// ConstraintViolation identifies the failing rule and node without exposing a value.
type ConstraintViolation = store.ConstraintViolation

// ErrConstraintViolation indicates that a graph change violates a constraint.
var ErrConstraintViolation = store.ErrConstraintViolation

// CreateGraphConstraint stages a rule for validation at commit.
func (tx *Tx) CreateGraphConstraint(definition GraphConstraint) error {
	if err := tx.requireInner(); err != nil {
		return err
	}
	return wrapError(tx.inner.CreateGraphConstraint(definition))
}

// DropGraphConstraint removes a rule from the transaction's pending state.
func (tx *Tx) DropGraphConstraint(name string) error {
	if err := tx.requireInner(); err != nil {
		return err
	}
	return wrapError(tx.inner.DropGraphConstraint(name))
}

// GraphConstraints lists rules from the transaction's snapshot, sorted by name.
func (tx *Tx) GraphConstraints() ([]GraphConstraint, error) {
	if err := tx.requireInner(); err != nil {
		return nil, err
	}
	definitions, err := tx.inner.GraphConstraints()
	return definitions, wrapError(err)
}
