package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

const ftsDefinitionPrefix = "latticedb:fts-index:"
const ftsDeclaredIndexPrefix = "declared:"
const ftsConfiguredPropertyPrefix = "configured-node-property:"

func declaredFTSPageIndexName(name string) string { return ftsDeclaredIndexPrefix + name }
func configuredPropertyFTSPageIndexName(property string) string {
	return ftsConfiguredPropertyPrefix + property
}

type FTSIndexKind string

const (
	FTSIndexNode FTSIndexKind = "node"
	FTSIndexEdge FTSIndexKind = "edge"
)

// FTSIndexDefinition is persisted through GraphState.AppMetadata and therefore
// follows checkpoints, migration, WAL deltas, and incremental archive frames.
type FTSIndexDefinition struct {
	Name     string       `json:"name"`
	Kind     FTSIndexKind `json:"kind"`
	Scope    string       `json:"scope"`
	Property string       `json:"property"`
}

type persistedFTSIndexDefinition struct {
	Version    uint8              `json:"version"`
	Definition FTSIndexDefinition `json:"definition"`
}

func ftsDefinitionKey(name string) []byte { return []byte(ftsDefinitionPrefix + name) }

func validateFTSIndexDefinition(def FTSIndexDefinition) error {
	if def.Name == "" || len(def.Name) > 200 || strings.ContainsAny(def.Name, "/\\\x00") {
		return fmt.Errorf("%w: invalid FTS index name", ErrInvalidArgument)
	}
	if def.Kind == FTSIndexNode {
		if err := store.ValidateCreateLabels([]string{def.Scope}); err != nil {
			return fmt.Errorf("%w: invalid node FTS scope", ErrInvalidArgument)
		}
	} else if def.Kind == FTSIndexEdge {
		if err := store.ValidateEdgeType(def.Scope); err != nil {
			return fmt.Errorf("%w: invalid edge FTS scope", ErrInvalidArgument)
		}
	} else {
		return fmt.Errorf("%w: FTS kind must be node or edge", ErrInvalidArgument)
	}
	if def.Property == "" {
		return fmt.Errorf("%w: FTS text property is empty", ErrInvalidArgument)
	}
	if err := store.ValidatePropertyKey(def.Property); err != nil {
		return err
	}
	return nil
}

func decodeFTSIndexDefinition(key, value []byte) (FTSIndexDefinition, error) {
	var def FTSIndexDefinition
	if len(key) <= len(ftsDefinitionPrefix) || string(key[:len(ftsDefinitionPrefix)]) != ftsDefinitionPrefix {
		return def, errors.New("invalid FTS definition metadata key")
	}
	var persisted persistedFTSIndexDefinition
	if err := json.Unmarshal(value, &persisted); err != nil {
		return def, fmt.Errorf("decode FTS definition: %w", err)
	}
	if persisted.Version != 1 {
		return def, errors.New("unsupported FTS definition version")
	}
	def = persisted.Definition
	if string(key[len(ftsDefinitionPrefix):]) != def.Name {
		return def, errors.New("FTS definition key/name mismatch")
	}
	return def, validateFTSIndexDefinition(def)
}

func ftsIndexDefinitions(graph *store.GraphState) ([]FTSIndexDefinition, error) {
	var definitions []FTSIndexDefinition
	for key, value := range graph.AppMetadata.All() {
		if len(key) < len(ftsDefinitionPrefix) || key[:len(ftsDefinitionPrefix)] != ftsDefinitionPrefix {
			continue
		}
		def, err := decodeFTSIndexDefinition([]byte(key), value)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, def)
	}
	slices.SortFunc(definitions, func(a, b FTSIndexDefinition) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return definitions, nil
}

func (tx *Tx) CreateFTSIndex(def FTSIndexDefinition) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateFTSIndexDefinition(def); err != nil {
		return err
	}
	key := ftsDefinitionKey(def.Name)
	if _, ok := tx.graph.AppMetadata.Get(string(key)); ok {
		return ErrAlreadyExists
	}
	value, err := json.Marshal(persistedFTSIndexDefinition{Version: 1, Definition: def})
	if err != nil {
		return err
	}
	return tx.PutAppMetadata(key, value)
}

func (tx *Tx) DropFTSIndex(name string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("%w: empty FTS index name", ErrInvalidArgument)
	}
	key := ftsDefinitionKey(name)
	if _, ok := tx.graph.AppMetadata.Get(string(key)); !ok {
		return nil
	}
	return tx.DeleteAppMetadata(key)
}

func (db *DB) CreateFTSIndex(def FTSIndexDefinition) error {
	return db.Update(func(tx *Tx) error { return tx.CreateFTSIndex(def) })
}
func (db *DB) CreateFTSIndexContext(ctx context.Context, def FTSIndexDefinition) error {
	return db.UpdateContext(ctx, func(tx *Tx) error { return tx.CreateFTSIndex(def) })
}
func (db *DB) DropFTSIndex(name string) error {
	return db.Update(func(tx *Tx) error { return tx.DropFTSIndex(name) })
}

func manualFTSPageIndex(analyzer FTSAnalyzer) string {
	if analyzer == FTSAnalyzerEnglishPorter {
		return store.PageFTSManualPorter
	}
	return store.PageFTSManualStandard
}

type ftsIndexBudget struct{ work, bytes, maxWork, maxBytes uint64 }

func (b *ftsIndexBudget) chargeUnit() error {
	return b.ChargePageFTS(1, 0)
}

func (b *ftsIndexBudget) ChargePageFTS(work, bytes uint64) error {
	if b.maxWork != 0 && (b.work > b.maxWork || work > b.maxWork-b.work) {
		return fmt.Errorf("%w: FTS index work budget exceeded", ErrResourceLimit)
	}
	if b.maxBytes != 0 && (b.bytes > b.maxBytes || bytes > b.maxBytes-b.bytes) {
		return fmt.Errorf("%w: FTS index byte budget exceeded", ErrResourceLimit)
	}
	b.work = saturatingAdd(b.work, work)
	b.bytes = saturatingAdd(b.bytes, bytes)
	return nil
}

func (b *ftsIndexBudget) RemainingPageFTSBytes() uint64 {
	if b.maxBytes == 0 {
		return ^uint64(0)
	}
	return b.maxBytes - min(b.maxBytes, b.bytes)
}

// Source buffers are transient; posting staging and work remain cumulative.
func (b *ftsIndexBudget) ReservePageRead(work, bytes uint64) error {
	return b.ChargePageFTS(work, bytes)
}
func (b *ftsIndexBudget) ReleasePageRead(bytes uint64)   { b.bytes -= min(b.bytes, bytes) }
func (b *ftsIndexBudget) RemainingPageReadBytes() uint64 { return b.RemainingPageFTSBytes() }

func dropPageFTSIndex(ctx context.Context, page *store.PageGraph, index string, budget *ftsIndexBudget) error {
	return page.DropFTSIndexWithCharge(ctx, index, budget.chargeUnit)
}

func (b *ftsIndexBudget) charge(text string, tokens []string) error {
	work := saturatingAdd(saturatingMul(uint64(len(text)), 5), uint64(len(tokens)))
	bytes := saturatingAdd(uint64(len(text)), saturatingMul(uint64(len(tokens)), 48))
	for _, token := range tokens {
		bytes = saturatingAdd(bytes, uint64(len(token)))
	}
	return b.ChargePageFTS(work, bytes)
}

func tokenizePageFTSBuild(ctx context.Context, page *store.PageGraph, text string, budget *ftsIndexBudget, porter bool) ([]string, error) {
	baseWork, baseBytes := saturatingMul(uint64(len(text)), 5), uint64(len(text))
	if err := budget.ChargePageFTS(baseWork, baseBytes); err != nil {
		return nil, err
	}
	limit := page.FTSIndexTokenLimit()
	remaining := budget.RemainingPageFTSBytes()
	if remaining < limit {
		limit = remaining
	}
	var tokens []string
	var err error
	if porter {
		tokens, err = search.AnalyzeEnglishPorterContextWithLimit(ctx, text, limit)
	} else {
		tokens, err = search.TokenizeContextWithLimit(ctx, text, limit)
	}
	if errors.Is(err, search.ErrTokenizationLimit) {
		return nil, fmt.Errorf("%w: FTS index tokenization exceeds build budget", ErrResourceLimit)
	}
	if err != nil {
		return nil, err
	}
	tokenBytes := saturatingMul(uint64(len(tokens)), 48)
	for _, token := range tokens {
		tokenBytes = saturatingAdd(tokenBytes, uint64(len(token)))
	}
	if err := budget.ChargePageFTS(uint64(len(tokens)), tokenBytes); err != nil {
		return nil, err
	}
	return tokens, nil
}

func preparePageFTSIndexes(ctx context.Context, page *store.PageGraph, graph *store.GraphState, maxWork, maxBytes uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	budget, ok := store.FTSMaintenanceBudgetFromContext(ctx).(*ftsIndexBudget)
	if !ok {
		budget = &ftsIndexBudget{maxWork: maxWork, maxBytes: maxBytes}
		ctx = store.WithFTSMaintenanceBudget(ctx, budget)
	}
	for _, index := range []string{store.PageFTSManualStandard, store.PageFTSManualPorter} {
		ready, err := page.FTSIndexReady(index)
		if err != nil {
			return err
		}
		if !ready {
			if err := rebuildManualPageFTS(ctx, page, graph, index, budget); err != nil {
				return err
			}
		}
	}
	for property := range graph.FTSProperties {
		index := configuredPropertyFTSPageIndexName(property)
		ready, err := page.FTSIndexReady(index)
		if err != nil {
			return err
		}
		if !ready {
			if err := rebuildConfiguredPropertyPageFTS(ctx, page, graph, property, budget); err != nil {
				return err
			}
		}
	}
	activeConfigured := make(map[string]struct{}, len(graph.FTSProperties))
	for property := range graph.FTSProperties {
		activeConfigured[configuredPropertyFTSPageIndexName(property)] = struct{}{}
	}
	if err := page.InvalidateFTSIndexNamespace(ctx, ftsConfiguredPropertyPrefix, activeConfigured); err != nil {
		return err
	}
	defs, err := ftsIndexDefinitions(graph)
	if err != nil {
		return err
	}
	for _, def := range defs {
		index := declaredFTSPageIndexName(def.Name)
		ready, err := page.FTSIndexReady(index)
		if err != nil {
			return err
		}
		if !ready {
			if err := rebuildDeclaredPageFTS(ctx, page, def, index, budget); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyPageFTSIndexDelta(ctx context.Context, page *store.PageGraph, before, after *store.GraphState, delta store.GraphDelta, maxWork, maxBytes uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	budget := &ftsIndexBudget{maxWork: maxWork, maxBytes: maxBytes}
	if shared, ok := store.FTSMaintenanceBudgetFromContext(ctx).(*ftsIndexBudget); ok {
		budget = shared
	} else {
		ctx = store.WithFTSMaintenanceBudget(ctx, budget)
	}
	oldDefs, err := ftsIndexDefinitions(before)
	if err != nil {
		return err
	}
	newDefs, err := ftsIndexDefinitions(after)
	if err != nil {
		return err
	}
	oldByName := make(map[string]FTSIndexDefinition, len(oldDefs))
	newByName := make(map[string]FTSIndexDefinition, len(newDefs))
	for _, d := range oldDefs {
		oldByName[d.Name] = d
	}
	for _, d := range newDefs {
		newByName[d.Name] = d
	}
	for name := range oldByName {
		if _, ok := newByName[name]; !ok {
			if err := dropPageFTSIndex(ctx, page, declaredFTSPageIndexName(name), budget); err != nil {
				return err
			}
		}
	}
	for _, def := range newDefs {
		index := declaredFTSPageIndexName(def.Name)
		ready, err := page.FTSIndexReady(index)
		if err != nil {
			return err
		}
		if !ready {
			if err := rebuildDeclaredPageFTS(ctx, page, def, index, budget); err != nil {
				return err
			}
			continue
		}
		if old, ok := oldByName[def.Name]; ok && old != def {
			if err := dropPageFTSIndex(ctx, page, index, budget); err != nil {
				return err
			}
			if err := rebuildDeclaredPageFTS(ctx, page, def, index, budget); err != nil {
				return err
			}
			continue
		}
		if def.Kind == FTSIndexNode {
			ids := uniqueIDs(append(slices.Clone(delta.UpsertNodes), delta.DeleteNodes...))
			for _, id := range ids {
				if err := indexDeclaredNode(ctx, page, def, index, id, budget); err != nil {
					return err
				}
			}
		} else {
			ids := uniqueIDs(append(slices.Clone(delta.UpsertEdges), delta.DeleteEdges...))
			for _, id := range ids {
				if err := indexDeclaredEdge(ctx, page, def, index, id, budget); err != nil {
					return err
				}
			}
		}
	}
	for property := range after.FTSProperties {
		index := configuredPropertyFTSPageIndexName(property)
		ready, err := page.FTSIndexReady(index)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		ids := uniqueIDs(append(slices.Clone(delta.UpsertNodes), delta.DeleteNodes...))
		for _, id := range ids {
			if err := indexConfiguredPropertyNode(ctx, page, property, index, id, budget); err != nil {
				return err
			}
		}
	}
	activeConfigured := make(map[string]struct{}, len(after.FTSProperties))
	for property := range after.FTSProperties {
		activeConfigured[configuredPropertyFTSPageIndexName(property)] = struct{}{}
	}
	if err := page.InvalidateFTSIndexNamespace(ctx, ftsConfiguredPropertyPrefix, activeConfigured); err != nil {
		return err
	}
	return nil
}

func rebuildManualPageFTS(ctx context.Context, page *store.PageGraph, graph *store.GraphState, index string, budget *ftsIndexBudget) error {
	if err := dropPageFTSIndex(ctx, page, index, budget); err != nil {
		return err
	}
	err := graph.VisitFTS(ctx, func(id uint64, record *store.FTSRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		tokens := record.Tokens
		if index == store.PageFTSManualPorter {
			var err error
			tokens, err = tokenizePageFTSBuild(ctx, page, record.Text, budget, true)
			if err != nil {
				return err
			}
		} else if err := budget.charge(record.Text, tokens); err != nil {
			return err
		}
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.ReplaceFTSDocument(ctx, index, id, tokens)
	})
	if err != nil {
		return pageStorageOpenError(err)
	}
	return page.SetFTSIndexReadyContext(ctx, index, true)
}

func rebuildDeclaredPageFTS(ctx context.Context, page *store.PageGraph, def FTSIndexDefinition, index string, budget *ftsIndexBudget) error {
	if err := dropPageFTSIndex(ctx, page, index, budget); err != nil {
		return err
	}
	if def.Kind == FTSIndexNode {
		if err := page.VisitNodes(store.WithPageReadBudget(ctx, budget), func(node *store.NodeRecord) error {
			if err := budget.chargeUnit(); err != nil {
				return err
			}
			return indexDeclaredNodeRecord(ctx, page, def, index, node.ID, node, budget)
		}); err != nil {
			return err
		}
	} else {
		if err := page.VisitEdges(store.WithPageReadBudget(ctx, budget), func(edge *store.EdgeRecord) error {
			if err := budget.chargeUnit(); err != nil {
				return err
			}
			return indexDeclaredEdgeRecord(ctx, page, def, index, edge.ID, edge, budget)
		}); err != nil {
			return err
		}
	}
	return page.SetFTSIndexReadyContext(ctx, index, true)
}

func rebuildConfiguredPropertyPageFTS(ctx context.Context, page *store.PageGraph, _ *store.GraphState, property string, budget *ftsIndexBudget) error {
	index := configuredPropertyFTSPageIndexName(property)
	if err := dropPageFTSIndex(ctx, page, index, budget); err != nil {
		return err
	}
	if err := page.VisitNodes(store.WithPageReadBudget(ctx, budget), func(node *store.NodeRecord) error {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return indexConfiguredPropertyNodeRecord(ctx, page, property, index, node.ID, node, budget)
	}); err != nil {
		return err
	}
	return page.SetFTSIndexReadyContext(ctx, index, true)
}

func indexConfiguredPropertyNode(ctx context.Context, page *store.PageGraph, property, index string, id uint64, budget *ftsIndexBudget) error {
	if err := budget.chargeUnit(); err != nil {
		return err
	}
	return page.VisitNode(store.WithPageReadBudget(ctx, budget), id, func(node *store.NodeRecord) error {
		return indexConfiguredPropertyNodeRecord(ctx, page, property, index, id, node, budget)
	})
}

func indexConfiguredPropertyNodeRecord(ctx context.Context, page *store.PageGraph, property, index string, id uint64, node *store.NodeRecord, budget *ftsIndexBudget) error {
	if node == nil {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.DeleteFTSDocument(ctx, index, id)
	}
	value, ok := node.Properties.Get(property).(string)
	if !ok {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.DeleteFTSDocument(ctx, index, id)
	}
	tokens, err := tokenizePageFTSBuild(ctx, page, value, budget, false)
	if err != nil {
		return err
	}
	if err := budget.chargeUnit(); err != nil {
		return err
	}
	return page.ReplaceFTSDocument(ctx, index, id, tokens)
}

func indexDeclaredNode(ctx context.Context, page *store.PageGraph, def FTSIndexDefinition, index string, id uint64, budget *ftsIndexBudget) error {
	if err := budget.chargeUnit(); err != nil {
		return err
	}
	return page.VisitNode(store.WithPageReadBudget(ctx, budget), id, func(node *store.NodeRecord) error {
		return indexDeclaredNodeRecord(ctx, page, def, index, id, node, budget)
	})
}

func indexDeclaredNodeRecord(ctx context.Context, page *store.PageGraph, def FTSIndexDefinition, index string, id uint64, node *store.NodeRecord, budget *ftsIndexBudget) error {
	if node == nil || !slices.Contains(node.Labels, def.Scope) {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.DeleteFTSDocument(ctx, index, id)
	}
	value, ok := node.Properties.Get(def.Property).(string)
	if !ok {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.DeleteFTSDocument(ctx, index, id)
	}
	tokens, err := tokenizePageFTSBuild(ctx, page, value, budget, false)
	if err != nil {
		return err
	}
	if err := budget.chargeUnit(); err != nil {
		return err
	}
	return page.ReplaceFTSDocument(ctx, index, id, tokens)
}

func indexDeclaredEdge(ctx context.Context, page *store.PageGraph, def FTSIndexDefinition, index string, id uint64, budget *ftsIndexBudget) error {
	if err := budget.chargeUnit(); err != nil {
		return err
	}
	return page.VisitEdge(store.WithPageReadBudget(ctx, budget), id, func(edge *store.EdgeRecord) error {
		return indexDeclaredEdgeRecord(ctx, page, def, index, id, edge, budget)
	})
}

func indexDeclaredEdgeRecord(ctx context.Context, page *store.PageGraph, def FTSIndexDefinition, index string, id uint64, edge *store.EdgeRecord, budget *ftsIndexBudget) error {
	if edge == nil || edge.Type != def.Scope {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.DeleteFTSDocument(ctx, index, id)
	}
	value, ok := edge.Properties.Get(def.Property).(string)
	if !ok {
		if err := budget.chargeUnit(); err != nil {
			return err
		}
		return page.DeleteFTSDocument(ctx, index, id)
	}
	tokens, err := tokenizePageFTSBuild(ctx, page, value, budget, false)
	if err != nil {
		return err
	}
	if err := budget.chargeUnit(); err != nil {
		return err
	}
	return page.ReplaceFTSDocument(ctx, index, id, tokens)
}

func uniqueIDs(ids []uint64) []uint64 { slices.Sort(ids); return slices.Compact(ids) }

func (db *DB) pageIndexedFTSSearch(ctx context.Context, graph *store.GraphState, query string, opts FTSSearchOptions, budget *directSearchBudget) ([]FTSSearchResult, bool, error) {
	page := graph.PageBase
	if page == nil || !page.SearchIndexesCurrent {
		return nil, false, nil
	}
	index := manualFTSPageIndex(opts.Analyzer)
	ready, err := page.FTSIndexReady(index)
	if err != nil {
		return nil, false, err
	}
	if !ready {
		return nil, false, nil
	}
	terms, termBytes, err := ftsAnalyzeQuery(ctx, query, opts.Analyzer, budget)
	if err != nil {
		return nil, true, err
	}
	defer budget.releaseBytes(termBytes)
	if opts.Scoring == FTSScoringBM25 {
		terms, err = uniqueFTSTerms(terms, budget)
		if err != nil {
			return nil, true, err
		}
	}
	if len(terms) == 0 {
		return []FTSSearchResult{}, true, nil
	}
	stats, err := page.FTSIndexStats(index)
	if err != nil {
		return nil, true, err
	}
	if stats.Documents == 0 {
		return []FTSSearchResult{}, true, nil
	}
	if opts.MaxDistance != 0 {
	}
	termsForPosting, heldBytes, err := pageFTSPostingTerms(ctx, page, index, terms, opts, budget)
	if err != nil {
		return nil, true, err
	}
	results, err := collectPageFTSResults(ctx, page, index, terms, termsForPosting, opts, stats, budget)
	budget.releaseBytes(heldBytes)
	return results, true, err
}

func pageFTSPostingTerms(ctx context.Context, page *store.PageGraph, index string, terms []string, opts FTSSearchOptions, budget *directSearchBudget) ([]string, uint64, error) {
	if opts.MaxDistance == 0 {
		out := slices.Clone(terms)
		held := saturatingMul(uint64(len(out)), 24)
		if err := budget.reserveBytes(held); err != nil {
			return nil, 0, err
		}
		slices.Sort(out)
		out = slices.Compact(out)
		return out, held, nil
	}
	const mapBaseBytes = uint64(64)
	if err := budget.reserveBytes(mapBaseBytes); err != nil {
		return nil, 0, err
	}
	vocab := make(map[string]struct{})
	held := mapBaseBytes
	err := page.VisitFTSVocabularyWithLimit(store.WithPageReadBudget(ctx, budget), index, budget.maxBytes-budget.bytes, func(token string) error {
		if err := budget.add(1); err != nil {
			return err
		}
		match, err := recordMatchesAnyTokenBudget([]string{token}, terms, opts.MaxDistance, opts.MinTermLength, budget)
		if err != nil || !match {
			return err
		}
		entry := saturatingAdd(48, uint64(len(token)))
		if err := budget.reserveBytes(entry); err != nil {
			return err
		}
		held = saturatingAdd(held, entry)
		vocab[token] = struct{}{}
		return nil
	})
	if err != nil {
		budget.releaseBytes(held)
		if errors.Is(err, store.ErrLoadResourceLimit) {
			return nil, 0, fmt.Errorf("%w: FTS vocabulary exceeds memory budget", ErrResourceLimit)
		}
		return nil, 0, err
	}
	listBytes := saturatingMul(uint64(len(vocab)), 24)
	if err := budget.reserveBytes(listBytes); err != nil {
		budget.releaseBytes(held)
		return nil, 0, err
	}
	out := make([]string, 0, len(vocab))
	for term := range vocab {
		out = append(out, term)
	}
	slices.Sort(out)
	return out, saturatingAdd(held, listBytes), nil
}

func collectPageFTSResults(ctx context.Context, page *store.PageGraph, index string, terms, postingTerms []string, opts FTSSearchOptions, stats store.PageFTSStats, budget *directSearchBudget) ([]FTSSearchResult, error) {
	// Map document IDs to the same per-query-term frequencies used by the legacy scorer.
	type docScore struct {
		length      uint64
		frequencies []uint64
	}
	var bytes uint64
	mapBase := uint64(64)
	if err := budget.reserveBytes(mapBase); err != nil {
		return nil, err
	}
	bytes = mapBase
	docs := make(map[uint64]*docScore)
	for _, token := range postingTerms {
		queryTermBytes := saturatingMul(uint64(len(terms)), 8)
		if err := budget.reserveBytes(queryTermBytes); err != nil {
			budget.releaseBytes(bytes)
			return nil, err
		}
		queryTerms := make([]int, 0, len(terms))
		for i, term := range terms {
			match, err := fuzzyTokenMatchBudget(term, token, opts.MaxDistance, opts.MinTermLength, budget)
			if err != nil {
				budget.releaseBytes(queryTermBytes)
				budget.releaseBytes(bytes)
				return nil, err
			}
			if match {
				queryTerms = append(queryTerms, i)
			}
		}
		if len(queryTerms) == 0 {
			budget.releaseBytes(queryTermBytes)
			continue
		}
		if err := page.VisitFTSPostings(ctx, index, token, func(posting store.PageFTSPosting) error {
			if err := budget.add(1); err != nil {
				return err
			}
			doc := docs[posting.DocumentID]
			if doc == nil {
				docBytes := saturatingAdd(112, saturatingMul(uint64(len(terms)), 8))
				if err := budget.reserveBytes(docBytes); err != nil {
					return err
				}
				bytes = saturatingAdd(bytes, docBytes)
				doc = &docScore{length: posting.Length, frequencies: make([]uint64, len(terms))}
				docs[posting.DocumentID] = doc
			}
			for _, queryTerm := range queryTerms {
				doc.frequencies[queryTerm] = saturatingAdd(doc.frequencies[queryTerm], posting.Frequency)
			}
			return nil
		}); err != nil {
			budget.releaseBytes(queryTermBytes)
			budget.releaseBytes(bytes)
			return nil, err
		}
		budget.releaseBytes(queryTermBytes)
	}
	defer budget.releaseBytes(bytes)
	limit := int(opts.Limit)
	if limit == 0 {
		limit = 10
	}
	resultBytes := saturatingMul(uint64(min(limit, len(docs))), 16)
	if err := budget.reserveBytes(resultBytes); err != nil {
		return nil, err
	}
	defer budget.releaseBytes(resultBytes)
	results := make([]FTSSearchResult, 0, min(limit, len(docs)))
	dfBytes := saturatingMul(uint64(len(terms)), 8)
	if err := budget.reserveBytes(dfBytes); err != nil {
		return nil, err
	}
	defer budget.releaseBytes(dfBytes)
	df := make([]uint64, len(terms))
	for _, doc := range docs {
		for i, count := range doc.frequencies {
			if count != 0 {
				df[i]++
			}
		}
	}
	for id, doc := range docs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := budget.add(1); err != nil {
			return nil, err
		}
		score := frequencyScore(doc.frequencies)
		if opts.Scoring == FTSScoringBM25 {
			score = bm25Score(doc.frequencies, df, doc.length, stats.Documents, stats.AverageLength())
		}
		if score > 0 {
			var err error
			results, err = pushFTSResultBudget(results, FTSSearchResult{NodeID: id, Score: float32(score)}, limit, budget)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := sortFTSResultsBudget(results, budget); err != nil {
		return nil, err
	}
	return results, nil
}
