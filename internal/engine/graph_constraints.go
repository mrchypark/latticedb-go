package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func isGraphConstraintKey(key []byte) bool {
	return string(key) == store.GraphConstraintsFeatureKey || strings.HasPrefix(string(key), store.GraphConstraintPrefix)
}

func (tx *Tx) CreateGraphConstraint(definition store.GraphConstraint) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	value, err := store.EncodeGraphConstraint(definition)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	key := store.GraphConstraintPrefix + definition.Name
	if _, exists := tx.graph.AppMetadata.Get(key); exists {
		return ErrAlreadyExists
	}
	tx.putAppMetadata(key, value)
	if _, exists := tx.graph.AppMetadata.Get(store.GraphConstraintsFeatureKey); !exists {
		tx.putAppMetadata(store.GraphConstraintsFeatureKey, []byte("1"))
	}
	tx.constraintsChanged = true
	return nil
}

func (tx *Tx) DropGraphConstraint(name string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	key := store.GraphConstraintPrefix + name
	if _, exists := tx.graph.AppMetadata.Get(key); !exists {
		return os.ErrNotExist
	}
	definitions, err := tx.GraphConstraints()
	if err != nil {
		return err
	}
	tx.deleteAppMetadata(key)
	if len(definitions) == 1 {
		tx.deleteAppMetadata(store.GraphConstraintsFeatureKey)
	}
	tx.constraintsChanged = true
	return nil
}

func (tx *Tx) GraphConstraints() ([]store.GraphConstraint, error) {
	if tx == nil || tx.closed {
		return nil, ErrInactiveTx
	}
	definitions, err := store.ReadGraphConstraints(tx.graph, nil)
	return definitions, graphConstraintError(err)
}

func (tx *Tx) validateGraphConstraints(ctx context.Context) error {
	if !tx.constraintsChanged && len(tx.changes.upsertNodes) == 0 && len(tx.changes.deleteNodes) == 0 {
		return nil
	}
	if !store.GraphConstraintsPresent(tx.graph) {
		return nil
	}
	if tx.constraintBudget != nil {
		return graphConstraintError(store.ValidateGraphConstraints(ctx, tx.graph, tx.constraintBudget))
	}
	return graphConstraintError(store.ValidateGraphConstraints(ctx, tx.graph, nil))
}

func graphConstraintError(err error) error {
	if errors.Is(err, store.ErrLoadResourceLimit) {
		return fmt.Errorf("%w: %w", ErrResourceLimit, err)
	}
	return err
}

func (budget *queryBudget) ChargeConstraintWork(work uint64) error {
	return budget.check(work, 0)
}

func (budget *queryBudget) ReserveConstraintBytes(bytes uint64) error {
	return budget.chargeTemporary(bytes)
}

func (budget *queryBudget) ReleaseConstraintBytes(bytes uint64) {
	budget.releaseTemporary(bytes)
}
