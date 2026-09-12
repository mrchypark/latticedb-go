package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func normalizeFTSProperties(properties []string) ([]string, error) {
	if len(properties) == 0 {
		return nil, nil
	}
	result := slices.Clone(properties)
	for _, property := range result {
		if property == "" {
			return nil, fmt.Errorf("%w: FTS property key is empty", ErrInvalidArgument)
		}
		if err := store.ValidatePropertyKey(property); err != nil {
			return nil, err
		}
	}
	slices.Sort(result)
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, fmt.Errorf("%w: duplicate FTS property %q", ErrInvalidArgument, result[i])
		}
	}
	return result, nil
}

func tokenizeFTSProperty(ctx context.Context, value string, maxWork, maxBytes uint64) ([]string, error) {
	if saturatingMul(uint64(len(value)), 3) > maxWork || uint64(len(value)) > maxBytes {
		return nil, fmt.Errorf("%w: FTS property tokenization exceeds derived-index budget", ErrResourceLimit)
	}
	tokens, err := search.TokenizeContextWithLimit(ctx, value, maxBytes)
	if errors.Is(err, search.ErrTokenizationLimit) {
		return nil, fmt.Errorf("%w: FTS property tokenization exceeds derived-index budget", ErrResourceLimit)
	}
	return tokens, err
}

func buildFTSPropertyPostings(ctx context.Context, graph *store.GraphState, properties []string, db *DB) (map[string]store.StringPostings, uint64, uint64, error) {
	if len(properties) == 0 {
		return nil, 0, 0, nil
	}
	postings := make(map[string]store.StringPostings, len(properties))
	var work, bytes uint64
	for _, property := range properties {
		postings[property] = store.NewStringPostings()
		work = saturatingAdd(work, 1)
		bytes = saturatingAdd(bytes, saturatingAdd(uint64(len(property)), 192))
		if exceedsDerivedBudget(graph, db, work, bytes) {
			return nil, 0, 0, fmt.Errorf("%w: FTS property index build exceeds derived-index budget", ErrResourceLimit)
		}
		for id, node := range graph.Nodes.Ordered() {
			if err := ctx.Err(); err != nil {
				return nil, 0, 0, err
			}
			value, ok := ftsPropertyValue(node, property)
			if !ok {
				continue
			}
			estimatedWork := saturatingMul(uint64(len(value)), 3)
			if exceedsDerivedBudget(graph, db, saturatingAdd(work, estimatedWork), bytes) {
				return nil, 0, 0, fmt.Errorf("%w: FTS property index build exceeds derived-index budget", ErrResourceLimit)
			}
			remainingWork := db.derivedIndexBuildMaxWork - min(db.derivedIndexBuildMaxWork, saturatingAdd(graph.DerivedIndexWork, work))
			remaining := db.derivedIndexBuildMaxLogicalBytes - min(db.derivedIndexBuildMaxLogicalBytes, saturatingAdd(graph.DerivedIndexLogicalBytes, bytes))
			tokens, err := tokenizeFTSProperty(ctx, value, remainingWork, remaining)
			if err != nil {
				return nil, 0, 0, err
			}
			entryWork, entryBytes := store.FTSDerivedCost(value, tokens)
			work = saturatingAdd(work, entryWork)
			bytes = saturatingAdd(bytes, entryBytes)
			if exceedsDerivedBudget(graph, db, work, bytes) {
				return nil, 0, 0, fmt.Errorf("%w: FTS property index build exceeds derived-index budget", ErrResourceLimit)
			}
			slices.Sort(tokens)
			tokens = slices.Compact(tokens)
			posting := postings[property]
			for _, token := range tokens {
				posting.Add(token, id)
			}
			postings[property] = posting
		}
	}
	return postings, work, bytes, nil
}

func ftsPropertyValue(node *store.NodeRecord, property string) (string, bool) {
	if node == nil {
		return "", false
	}
	value, ok := node.Properties.Get(property).(string)
	return value, ok
}

func (tx *Tx) applyFTSPropertyChanges(ctx context.Context) error {
	if tx.base == nil || len(tx.graph.FTSProperties) == 0 || tx.changes == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for property := range tx.graph.FTSProperties {
		for id := range tx.changes.deleteNodes {
			if value, ok := ftsPropertyValue(tx.base.Nodes.Get(id), property); ok {
				if err := tx.removeFTSPropertyValue(ctx, property, id, value); err != nil {
					return err
				}
			}
		}
		for id := range tx.changes.upsertNodes {
			oldValue, oldOK := ftsPropertyValue(tx.base.Nodes.Get(id), property)
			newValue, newOK := ftsPropertyValue(tx.graph.Nodes.Get(id), property)
			if oldOK && newOK && oldValue == newValue {
				continue
			}
			if oldOK {
				if err := tx.removeFTSPropertyValue(ctx, property, id, oldValue); err != nil {
					return err
				}
			}
			if newOK {
				if err := tx.addFTSPropertyValue(ctx, property, id, newValue); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (tx *Tx) removeFTSPropertyValue(ctx context.Context, property string, id uint64, value string) error {
	tokens, err := tokenizeFTSProperty(ctx, value, tx.db.derivedIndexBuildMaxWork, tx.db.derivedIndexBuildMaxLogicalBytes)
	if err != nil {
		return err
	}
	work, bytes := store.FTSDerivedCost(value, tokens)
	slices.Sort(tokens)
	tokens = slices.Compact(tokens)
	postings := tx.graph.FTSProperties[property]
	for _, token := range tokens {
		postings.Remove(token, id)
	}
	adjustDerivedCost(tx.graph, work, bytes, false)
	tx.graph.FTSProperties[property] = postings
	return nil
}

func (tx *Tx) addFTSPropertyValue(ctx context.Context, property string, id uint64, value string) error {
	remainingWork := tx.db.derivedIndexBuildMaxWork - min(tx.db.derivedIndexBuildMaxWork, tx.graph.DerivedIndexWork)
	remainingBytes := tx.db.derivedIndexBuildMaxLogicalBytes - min(tx.db.derivedIndexBuildMaxLogicalBytes, tx.graph.DerivedIndexLogicalBytes)
	tokens, err := tokenizeFTSProperty(ctx, value, remainingWork, remainingBytes)
	if err != nil {
		return err
	}
	work, bytes := store.FTSDerivedCost(value, tokens)
	if exceedsDerivedBudget(tx.graph, tx.db, work, bytes) {
		return fmt.Errorf("%w: FTS property update exceeds derived-index build budget", ErrResourceLimit)
	}
	slices.Sort(tokens)
	tokens = slices.Compact(tokens)
	postings := tx.graph.FTSProperties[property]
	for _, token := range tokens {
		postings.Add(token, id)
	}
	adjustDerivedCost(tx.graph, work, bytes, true)
	tx.graph.FTSProperties[property] = postings
	return nil
}
