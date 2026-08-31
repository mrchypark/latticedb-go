package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

type queryPlan struct {
	unwindClause   *unwindClause
	matchPatterns  []matchPattern
	whereClauses   []*whereClause
	wherePredicate wherePredicate
	createNode     *createNodeClause
	setClauses     []*setClause
	createClause   *createClause
	removeClause   *removeClause
	deleteClause   *deleteClause
	returnClause   *returnClause
	orderClauses   []orderClause
	skipExpr       valueExpr
	limitExpr      valueExpr
}

type matchPattern interface {
	apply(tx *Tx, rows []queryRow, budget *queryBudget) ([]queryRow, error)
}

type queryBudget struct {
	ctx      context.Context
	maxRows  uint32
	maxWork  uint32
	maxBytes uint32
	work     uint32
	bytes    uint32
}

var queryBudgetPool = sync.Pool{New: func() any { return new(queryBudget) }}

func newQueryBudget(ctx context.Context, opts QueryOptions) *queryBudget {
	if opts.MaxRows == 0 {
		opts.MaxRows = 1_000_000
	}
	if opts.MaxWork == 0 {
		opts.MaxWork = 10_000_000
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 64 << 20
	}
	budget := queryBudgetPool.Get().(*queryBudget)
	*budget = queryBudget{
		ctx:      ctx,
		maxRows:  uint32(min(opts.MaxRows, uint64(^uint32(0)))),
		maxWork:  uint32(min(opts.MaxWork, uint64(^uint32(0)))),
		maxBytes: uint32(min(opts.MaxBytes, uint64(^uint32(0)))),
	}
	return budget
}

func releaseQueryBudget(budget *queryBudget) {
	*budget = queryBudget{}
	queryBudgetPool.Put(budget)
}

func (budget *queryBudget) check(work uint64, rows int) error {
	if budget.ctx != nil {
		if err := budget.ctx.Err(); err != nil {
			return err
		}
	}
	if work > uint64(budget.maxWork-budget.work) {
		return fmt.Errorf("%w: query work exceeds %d", ErrResourceLimit, budget.maxWork)
	}
	budget.work += uint32(work)
	if uint64(rows) > uint64(budget.maxRows) {
		return fmt.Errorf("%w: query rows exceed %d", ErrResourceLimit, budget.maxRows)
	}
	if uint64(rows) > uint64(budget.maxBytes-budget.bytes)/128 {
		return fmt.Errorf("%w: query temporary bytes exceed %d", ErrResourceLimit, budget.maxBytes)
	}
	return nil
}

func (budget *queryBudget) chargeResult(bytes uint64) error {
	return budget.chargeBytes(bytes, "result")
}

func (budget *queryBudget) chargeTemporary(bytes uint64) error {
	return budget.chargeBytes(bytes, "temporary")
}

func (budget *queryBudget) chargeBytes(bytes uint64, kind string) error {
	if bytes > uint64(budget.maxBytes-budget.bytes) {
		return fmt.Errorf("%w: query %s bytes exceed %d", ErrResourceLimit, kind, budget.maxBytes)
	}
	budget.bytes += uint32(bytes)
	return nil
}

type nodePattern struct {
	Var           string
	Labels        []string
	Properties    map[string]any
	PropertyExprs map[string]valueExpr
}

type edgePattern struct {
	Left          nodePattern
	EdgeVar       string
	EdgeType      string
	Properties    map[string]any
	PropertyExprs map[string]valueExpr
	Right         nodePattern
	Undirected    bool
}

type whereClause struct {
	Kind     whereKind
	Var      string
	Property string
	Expr     valueExpr
}

type whereKind string

const (
	whereEquals       whereKind = "equals"
	whereNotEquals    whereKind = "not_equals"
	whereLess         whereKind = "less"
	whereLessEqual    whereKind = "less_equal"
	whereGreater      whereKind = "greater"
	whereGreaterEqual whereKind = "greater_equal"
	whereIn           whereKind = "in"
	whereStartsWith   whereKind = "starts_with"
	whereEndsWith     whereKind = "ends_with"
	whereContains     whereKind = "contains"
	whereIsNull       whereKind = "is_null"
	whereIsNotNull    whereKind = "is_not_null"
	whereVector       whereKind = "vector"
	whereFTS          whereKind = "fts"
	whereBindingID    whereKind = "binding_id"
)

type wherePredicate interface {
	eval(queryRow, map[string]any) (predicateTruth, error)
}

type predicateTruth int8

const (
	predicateUnknown predicateTruth = iota
	predicateFalse
	predicateTrue
)

type booleanPredicate struct {
	Operator string
	Items    []wherePredicate
}

type notPredicate struct {
	Item wherePredicate
}

type setClause struct {
	Kind     setKind
	Var      string
	Property string
	Label    string
	Expr     valueExpr
}

type createClause struct {
	SourceVar string
	TargetVar string
	EdgeVar   string
	EdgeType  string
	Props     map[string]valueExpr
}

type createNodeClause struct {
	Var    string
	Labels []string
	Props  map[string]valueExpr
}

type setKind string

const (
	setProperty setKind = "property"
	setReplace  setKind = "replace"
	setMerge    setKind = "merge"
	setLabel    setKind = "label"
)

type removeClause struct {
	Items []removeItem
}

type removeItem struct {
	Var      string
	Property string
	Label    string
}

type deleteClause struct {
	Vars []string
}

type unwindClause struct {
	Expr valueExpr
	Var  string
}

type returnClause struct {
	CountVar    string
	CountAlias  string
	Projections []projection
	Distinct    bool
}

type projection struct {
	Kind     projectionKind
	Var      string
	Property string
	Alias    string
}

type orderClause struct {
	Kind     projectionKind
	Var      string
	Property string
	Desc     bool
}

type projectionKind string

const (
	projectionProperty  projectionKind = "property"
	projectionBindingID projectionKind = "binding_id"
	projectionValue     projectionKind = "value"
)

type queryRow struct {
	Bindings map[string]boundValue
	Order    float64
}

type boundValue struct {
	Node     *store.NodeRecord
	Edge     *store.EdgeRecord
	Value    any
	HasValue bool
}

type valueExpr interface {
	eval(row queryRow, params map[string]any) (any, error)
}

type literalExpr struct {
	Value any
}

type mapLiteralExpr struct {
	Entries map[string]valueExpr
}

type paramExpr struct {
	Name string
}

type variableExpr struct {
	Name string
}

type propertyExpr struct {
	Var      string
	Property string
}

func parseQuery(query string) (*queryPlan, error) {
	query = normalizeQueryText(query)
	var plan *queryPlan
	var err error
	switch {
	case strings.HasPrefix(query, "MATCH "):
		plan, err = parseMatchQuery(query)
	case strings.HasPrefix(query, "CREATE "):
		plan, err = parseCreateQuery(query)
	case strings.HasPrefix(query, "UNWIND "):
		plan, err = parseUnwindQuery(query)
	default:
		return nil, fmt.Errorf("unsupported query %q", query)
	}
	if err != nil {
		return nil, err
	}
	if err := plan.validateBindings(); err != nil {
		return nil, err
	}
	return plan, nil
}

func normalizeQueryText(query string) string {
	query = strings.TrimSpace(query)
	if strings.HasSuffix(query, ";") {
		query = strings.TrimSpace(strings.TrimSuffix(query, ";"))
	}

	var normalized strings.Builder
	normalized.Grow(len(query))
	var quote byte
	pendingSpace := false
	for index := 0; index < len(query); index++ {
		char := query[index]
		if quote == 0 && isQuerySpace(char) {
			pendingSpace = normalized.Len() != 0
			continue
		}
		if pendingSpace {
			normalized.WriteByte(' ')
			pendingSpace = false
		}
		normalized.WriteByte(char)
		scanQueryString(query, index, &quote)
	}
	return normalized.String()
}

func isQuerySpace(char byte) bool {
	switch char {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

type bindingRole uint8

const (
	bindingNode bindingRole = iota + 1
	bindingEdge
	bindingValue
)

func (plan *queryPlan) validateBindings() error {
	bindings := map[string]bindingRole{}
	bind := func(name string, role bindingRole) error {
		if name == "" {
			return nil
		}
		if name[0] != 0 && !isQueryIdentifier(name) {
			return fmt.Errorf("invalid binding %q", name)
		}
		if existing, ok := bindings[name]; ok && existing != role {
			return fmt.Errorf("binding %q has conflicting roles", name)
		}
		bindings[name] = role
		return nil
	}
	if plan.unwindClause != nil {
		if names := valueExprBindings(plan.unwindClause.Expr); len(names) != 0 {
			return fmt.Errorf("unknown binding %q", names[0])
		}
		if err := bind(plan.unwindClause.Var, bindingValue); err != nil {
			return err
		}
	}
	if plan.createNode != nil {
		if err := bind(plan.createNode.Var, bindingNode); err != nil {
			return err
		}
	}
	for _, pattern := range plan.matchPatterns {
		switch pattern := pattern.(type) {
		case nodePattern:
			if err := bind(pattern.Var, bindingNode); err != nil {
				return err
			}
		case edgePattern:
			for _, item := range []struct {
				name string
				role bindingRole
			}{{pattern.Left.Var, bindingNode}, {pattern.EdgeVar, bindingEdge}, {pattern.Right.Var, bindingNode}} {
				if err := bind(item.name, item.role); err != nil {
					return err
				}
			}
		}
	}
	require := func(name string, roles ...bindingRole) error {
		role, ok := bindings[name]
		if !ok {
			return fmt.Errorf("unknown binding %q", name)
		}
		for _, allowed := range roles {
			if role == allowed {
				return nil
			}
		}
		return fmt.Errorf("binding %q has incompatible role", name)
	}
	requireExpr := func(expr valueExpr) error {
		for _, name := range valueExprBindings(expr) {
			if err := require(name, bindingNode, bindingEdge, bindingValue); err != nil {
				return err
			}
		}
		return nil
	}
	for _, clause := range plan.whereClauses {
		if err := require(clause.Var, bindingNode, bindingEdge, bindingValue); err != nil {
			return err
		}
		if err := requireExpr(clause.Expr); err != nil {
			return err
		}
	}
	for _, clause := range wherePredicateClauses(plan.wherePredicate) {
		if err := require(clause.Var, bindingNode, bindingEdge, bindingValue); err != nil {
			return err
		}
		if err := requireExpr(clause.Expr); err != nil {
			return err
		}
	}
	for _, clause := range plan.setClauses {
		roles := []bindingRole{bindingNode, bindingEdge}
		if clause.Kind == setLabel {
			roles = []bindingRole{bindingNode}
		}
		if err := require(clause.Var, roles...); err != nil {
			return err
		}
		if err := requireExpr(clause.Expr); err != nil {
			return err
		}
	}
	if plan.createNode != nil {
		for _, expr := range plan.createNode.Props {
			for _, name := range valueExprBindings(expr) {
				if name == plan.createNode.Var {
					return fmt.Errorf("binding %q is not available while it is created", name)
				}
			}
			if err := requireExpr(expr); err != nil {
				return err
			}
		}
	}
	if plan.createClause != nil {
		if err := require(plan.createClause.SourceVar, bindingNode); err != nil {
			return err
		}
		if err := require(plan.createClause.TargetVar, bindingNode); err != nil {
			return err
		}
		for _, expr := range plan.createClause.Props {
			if err := requireExpr(expr); err != nil {
				return err
			}
		}
		if err := bind(plan.createClause.EdgeVar, bindingEdge); err != nil {
			return err
		}
	}
	if plan.removeClause != nil {
		for _, item := range plan.removeClause.Items {
			if err := require(item.Var, bindingNode, bindingEdge); err != nil {
				return err
			}
		}
	}
	if plan.deleteClause != nil {
		for _, name := range plan.deleteClause.Vars {
			if err := require(name, bindingNode, bindingEdge); err != nil {
				return err
			}
		}
	}
	if plan.returnClause != nil {
		if plan.returnClause.CountAlias != "" && plan.returnClause.CountVar != "*" {
			if err := require(plan.returnClause.CountVar, bindingNode, bindingEdge, bindingValue); err != nil {
				return err
			}
		}
		for _, projection := range plan.returnClause.Projections {
			roles := []bindingRole{bindingNode, bindingEdge}
			if projection.Kind == projectionValue || projection.Kind == projectionProperty {
				roles = []bindingRole{bindingNode, bindingEdge, bindingValue}
			}
			if err := require(projection.Var, roles...); err != nil {
				return err
			}
		}
	}
	for _, clause := range plan.orderClauses {
		if err := require(clause.Var, bindingNode, bindingEdge, bindingValue); err != nil {
			return err
		}
	}
	return nil
}

func valueExprBindings(expr valueExpr) []string {
	switch expr := expr.(type) {
	case nil, literalExpr, paramExpr:
		return nil
	case variableExpr:
		return []string{expr.Name}
	case propertyExpr:
		return []string{expr.Var}
	case mapLiteralExpr:
		var names []string
		for _, entry := range expr.Entries {
			names = append(names, valueExprBindings(entry)...)
		}
		return names
	default:
		panic("unsupported value expression")
	}
}

func parseMatchQuery(query string) (*queryPlan, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(query, "MATCH "))
	matchText, nextKeyword, tail := splitOnNextClause(rest, " WHERE ", " RETURN ", " SET ", " CREATE ", " REMOVE ", " DETACH DELETE ", " DELETE ")
	patterns, err := parseMatchPatterns(matchText)
	if err != nil {
		return nil, err
	}

	inlineWhere := patternPropertyClauses(patterns)
	plan := &queryPlan{matchPatterns: patterns, whereClauses: inlineWhere}

	switch nextKeyword {
	case " WHERE ":
		whereText, whereNext, afterWhere := splitOnNextClause(tail, " RETURN ", " SET ", " CREATE ", " REMOVE ", " DETACH DELETE ", " DELETE ")
		predicate, err := parseWherePredicate(whereText)
		if err != nil {
			return nil, err
		}
		if clauses, ok := flattenWhereConjunction(predicate); ok {
			plan.whereClauses = append(plan.whereClauses, clauses...)
		} else {
			for _, clause := range wherePredicateClauses(predicate) {
				if clause.Kind == whereVector || clause.Kind == whereFTS {
					return nil, errors.New("vector and full-text search cannot be combined with OR or NOT")
				}
			}
			plan.wherePredicate = predicate
		}
		nextKeyword = whereNext
		tail = afterWhere
	case " RETURN ", " SET ", " CREATE ", " REMOVE ", " DETACH DELETE ", " DELETE ", "":
	default:
		return nil, fmt.Errorf("unsupported clause after MATCH: %q", nextKeyword)
	}

	switch nextKeyword {
	case " RETURN ":
		if err := parsePlanReturn(plan, tail); err != nil {
			return nil, err
		}
	case " SET ":
		setText, returnKeyword, returnTail := splitOnNextClause(tail, " RETURN ")
		setClauses, err := parseSetClauses(setText)
		if err != nil {
			return nil, err
		}
		plan.setClauses = setClauses
		if returnKeyword != "" {
			if err := parsePlanReturn(plan, returnTail); err != nil {
				return nil, err
			}
		}
	case " CREATE ":
		createText, returnKeyword, returnTail := splitOnNextClause(tail, " RETURN ")
		createClause, err := parseCreateClause(createText)
		if err != nil {
			return nil, err
		}
		plan.createClause = createClause
		if returnKeyword != "" {
			if err := parsePlanReturn(plan, returnTail); err != nil {
				return nil, err
			}
		}
	case " REMOVE ":
		removeText, returnKeyword, returnTail := splitOnNextClause(tail, " RETURN ")
		removeClause, err := parseRemoveClause(removeText)
		if err != nil {
			return nil, err
		}
		plan.removeClause = removeClause
		if returnKeyword != "" {
			if err := parsePlanReturn(plan, returnTail); err != nil {
				return nil, err
			}
		}
	case " DETACH DELETE ", " DELETE ":
		deleteClause, err := parseDeleteClause(tail)
		if err != nil {
			return nil, err
		}
		plan.deleteClause = deleteClause
	case "":
	default:
		return nil, fmt.Errorf("unsupported terminal clause %q", nextKeyword)
	}

	return plan, nil
}

func parseUnwindQuery(query string) (*queryPlan, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(query, "UNWIND "))
	exprText, keyword, tail := splitOnNextClause(rest, " AS ")
	if keyword == "" {
		return nil, fmt.Errorf("invalid UNWIND clause %q", query)
	}
	expr, err := parseValueExpr(exprText)
	if err != nil {
		return nil, err
	}

	varName, nextKeyword, afterVar := splitOnNextClause(tail, " MATCH ", " CREATE ", " RETURN ")
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return nil, fmt.Errorf("invalid UNWIND binding %q", query)
	}
	var plan *queryPlan
	switch nextKeyword {
	case " MATCH ":
		plan, err = parseMatchQuery("MATCH " + afterVar)
	case " CREATE ":
		plan, err = parseCreateQuery("CREATE " + afterVar)
	case " RETURN ":
		plan = &queryPlan{}
		err = parsePlanReturn(plan, afterVar)
	default:
		return nil, fmt.Errorf("unsupported terminal clause %q", nextKeyword)
	}
	if err != nil {
		return nil, err
	}
	plan.unwindClause = &unwindClause{Expr: expr, Var: varName}
	return plan, nil
}

func parseCreateQuery(query string) (*queryPlan, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(query, "CREATE "))
	createText, nextKeyword, tail := splitOnNextClause(rest, " RETURN ")
	createNode, err := parseCreateNodeClause(createText)
	if err != nil {
		return nil, err
	}

	plan := &queryPlan{createNode: createNode}
	switch nextKeyword {
	case "":
		return plan, nil
	case " RETURN ":
		if err := parsePlanReturn(plan, tail); err != nil {
			return nil, err
		}
		return plan, nil
	default:
		return nil, fmt.Errorf("unsupported terminal clause %q", nextKeyword)
	}
}

func (plan *queryPlan) mutates() bool {
	return plan.createNode != nil || len(plan.setClauses) != 0 || plan.createClause != nil || plan.removeClause != nil || plan.deleteClause != nil
}

func parsePlanReturn(plan *queryPlan, text string) error {
	returnText, orderClauses, skipExpr, limitExpr, err := parseReturnTail(text)
	if err != nil {
		return err
	}
	returnClause, err := parseReturnClause(returnText)
	if err != nil {
		return err
	}
	plan.returnClause = returnClause
	plan.orderClauses = resolveOrderAliases(returnClause, orderClauses)
	if returnClause.Distinct {
		for _, order := range plan.orderClauses {
			if !slices.ContainsFunc(returnClause.Projections, func(projection projection) bool {
				return projection.Kind == order.Kind && projection.Var == order.Var && projection.Property == order.Property
			}) {
				return errors.New("ORDER BY expressions must be projected when RETURN DISTINCT is used")
			}
		}
	}
	plan.skipExpr = skipExpr
	plan.limitExpr = limitExpr
	return nil
}

func (plan *queryPlan) execute(tx *Tx, params map[string]any, budget *queryBudget) (QueryResult, error) {
	skip, err := paginationValue("SKIP", plan.skipExpr, params)
	if err != nil {
		return QueryResult{}, err
	}
	limit, err := paginationValue("LIMIT", plan.limitExpr, params)
	if err != nil {
		return QueryResult{}, err
	}
	rows := []queryRow{{}}
	if plan.unwindClause != nil {
		var err error
		rows, err = plan.unwindClause.apply(rows, params, budget)
		if err != nil {
			return QueryResult{}, err
		}
	}
	for _, pattern := range plan.matchPatterns {
		var err error
		if node, ok := pattern.(nodePattern); ok {
			if nodeID, found, lookupErr := plan.bindingNodeID(node.Var, params); lookupErr != nil {
				return QueryResult{}, lookupErr
			} else if found {
				rows = node.applyID(tx, rows, nodeID)
				if err := budget.check(uint64(len(rows)), len(rows)); err != nil {
					return QueryResult{}, err
				}
				continue
			}
			if nodeIDs, found, lookupErr := plan.indexedNodeIDs(tx, node, params, plan.indexedNodeLookupLimit(node, limit, skip), budget); lookupErr != nil {
				return QueryResult{}, lookupErr
			} else if found {
				if err := budget.chargeTemporary(uint64(len(nodeIDs)) * 8); err != nil {
					return QueryResult{}, err
				}
				nextRows := make([]queryRow, 0, len(nodeIDs)*len(rows))
				for _, nodeID := range nodeIDs {
					nextRows = append(nextRows, node.applyID(tx, rows, nodeID)...)
					if err := budget.check(uint64(len(rows)), len(nextRows)); err != nil {
						return QueryResult{}, err
					}
				}
				rows = nextRows
				continue
			}
		}
		if edge, ok := pattern.(edgePattern); ok {
			if edgeIDs, found, lookupErr := plan.indexedEdgeIDs(tx, edge, params); lookupErr != nil {
				return QueryResult{}, lookupErr
			} else if found {
				rows, err = edge.applyIDs(tx, rows, edgeIDs, budget)
				if err != nil {
					return QueryResult{}, err
				}
				continue
			}
		}
		rows, err = pattern.apply(tx, rows, budget)
		if err != nil {
			return QueryResult{}, err
		}
	}

	if len(plan.whereClauses) > 0 {
		for _, clause := range plan.whereClauses {
			var err error
			rows, err = clause.apply(tx, rows, params, budget)
			if err != nil {
				return QueryResult{}, err
			}
		}
	}
	if plan.wherePredicate != nil {
		filtered := rows[:0]
		predicateWork := uint64(len(wherePredicateClauses(plan.wherePredicate)))
		for _, row := range rows {
			if err := budget.check(predicateWork, len(filtered)); err != nil {
				return QueryResult{}, err
			}
			match, err := plan.wherePredicate.eval(row, params)
			if err != nil {
				return QueryResult{}, err
			}
			if match == predicateTrue {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	if plan.createNode != nil {
		var err error
		rows, err = plan.createNode.apply(tx, rows, params)
		if err != nil {
			return QueryResult{}, err
		}
	}
	if plan.createClause != nil {
		if err := plan.createClause.apply(tx, rows, params); err != nil {
			return QueryResult{}, err
		}
	}
	for _, clause := range plan.setClauses {
		if err := clause.apply(tx, rows, params); err != nil {
			return QueryResult{}, err
		}
	}
	if plan.removeClause != nil {
		if err := plan.removeClause.apply(tx, rows); err != nil {
			return QueryResult{}, err
		}
	}
	if plan.deleteClause != nil {
		if err := plan.deleteClause.apply(tx, rows); err != nil {
			return QueryResult{}, err
		}
	}
	if plan.mutates() {
		for index := range rows {
			refreshRowBindings(tx, &rows[index])
		}
	}

	if plan.returnClause == nil {
		return QueryResult{}, nil
	}
	if len(plan.orderClauses) != 0 {
		slices.SortStableFunc(rows, func(left, right queryRow) int {
			for _, clause := range plan.orderClauses {
				comparison := compareOrderValues(clause.value(left), clause.value(right))
				if clause.Desc {
					comparison = -comparison
				}
				if comparison != 0 {
					return comparison
				}
			}
			return compareRowBindings(left, right)
		})
	}
	result, err := plan.returnClause.render(rows, budget)
	if err != nil {
		return QueryResult{}, err
	}
	if plan.returnClause.Distinct {
		result.Rows, err = distinctResultRows(result.Columns, result.Rows, budget)
		if err != nil {
			return QueryResult{}, err
		}
	}
	if skip > len(result.Rows) {
		skip = len(result.Rows)
	}
	result.Rows = result.Rows[skip:]
	if plan.limitExpr != nil && len(result.Rows) > limit {
		result.Rows = result.Rows[:limit]
	}
	return result, nil
}

func (plan *queryPlan) indexedNodeIDs(tx *Tx, pattern nodePattern, params map[string]any, limit uint, budget *queryBudget) ([]uint64, bool, error) {
	if tx.queryIndexesDisabled || hasGraphChanges(tx.changes) {
		return nil, false, nil
	}
	if pattern.Var == "" || len(pattern.Labels) == 0 {
		return nil, false, nil
	}
	for _, clause := range plan.whereClauses {
		if clause.Kind != whereEquals || clause.Var != pattern.Var {
			continue
		}
		switch clause.Expr.(type) {
		case literalExpr, paramExpr:
		default:
			continue
		}
		value, err := clause.Expr.eval(queryRow{}, params)
		if err != nil {
			return nil, false, err
		}
		for _, label := range pattern.Labels {
			definition := store.PropertyIndexDefinition{Scope: label, Property: clause.Property}
			if !tx.graph.NodeProperties.Has(definition) {
				continue
			}
			if limit != ^uint(0) && len(plan.whereClauses) > 1 {
				ids, err := plan.boundedFilteredNodeIDs(tx, pattern, definition, value, params, limit, budget)
				return ids, true, err
			}
			ids, err := tx.FindNodesByLabelProperty(label, clause.Property, value, limit)
			if err != nil {
				return nil, false, err
			}
			if alternate, ok := alternateNumericIndexValue(value); ok {
				more, err := tx.FindNodesByLabelProperty(label, clause.Property, alternate, limit)
				if err != nil {
					return nil, false, err
				}
				ids = append(ids, more...)
				slices.Sort(ids)
				ids = slices.Compact(ids)
			}
			return ids, true, nil
		}
	}
	return nil, false, nil
}

func (plan *queryPlan) indexedNodeLookupLimit(pattern nodePattern, limit, skip int) uint {
	if plan.limitExpr == nil || limit == 0 || skip != 0 || plan.wherePredicate != nil || plan.returnClause == nil || plan.returnClause.CountAlias != "" || plan.returnClause.Distinct || len(plan.matchPatterns) != 1 || plan.mutates() {
		return ^uint(0)
	}
	for _, clause := range plan.whereClauses {
		if clause.Var != pattern.Var {
			return ^uint(0)
		}
		switch clause.Kind {
		case whereEquals, whereBindingID:
			switch clause.Expr.(type) {
			case literalExpr, paramExpr:
			default:
				return ^uint(0)
			}
		case whereIsNull, whereIsNotNull:
		default:
			return ^uint(0)
		}
	}
	return uint(limit)
}

func (plan *queryPlan) boundedFilteredNodeIDs(tx *Tx, pattern nodePattern, definition store.PropertyIndexDefinition, value any, params map[string]any, limit uint, budget *queryBudget) ([]uint64, error) {
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return nil, err
	}
	expected := make([]any, len(plan.whereClauses))
	for i, clause := range plan.whereClauses {
		if clause.Kind == whereEquals || clause.Kind == whereBindingID {
			expected[i], err = clause.Expr.eval(queryRow{}, params)
			if err != nil {
				return nil, err
			}
		}
	}
	results := make([]uint64, 0, min(limit, 64))
	var visitErr error
	visit := func(indexValue any) error {
		_, err := tx.graph.NodeProperties.Visit(definition, indexValue, func(id uint64) bool {
			if visitErr = budget.check(1, len(results)); visitErr != nil {
				return false
			}
			if tx.changes != nil {
				if _, changed := tx.changes.upsertNodes[id]; changed {
					return true
				}
			}
			node := tx.graph.Nodes.Get(id)
			if nodeMatchesPropertyIndex(node, definition, indexValue) && plan.nodeMatchesBoundedFilter(node, pattern, expected) {
				results = insertPropertyIndexID(results, id, limit)
			}
			return true
		})
		if err != nil {
			return err
		}
		return visitErr
	}
	if err := visit(normalized); err != nil {
		return nil, err
	}
	if alternate, ok := alternateNumericIndexValue(normalized); ok {
		if err := visit(alternate); err != nil {
			return nil, err
		}
	}
	if tx.changes != nil {
		for id := range tx.changes.upsertNodes {
			if err := budget.check(1, len(results)); err != nil {
				return nil, err
			}
			node := tx.graph.Nodes.Get(id)
			if nodeMatchesQueryProperty(node, definition, normalized) && plan.nodeMatchesBoundedFilter(node, pattern, expected) {
				results = insertPropertyIndexID(results, id, limit)
			}
		}
	}
	return results, nil
}

func (plan *queryPlan) nodeMatchesBoundedFilter(node *store.NodeRecord, pattern nodePattern, expected []any) bool {
	if node == nil || !store.LabelsMatch(node, pattern.Labels) || !store.PropertiesMatch(node.Properties, pattern.Properties) {
		return false
	}
	binding := boundValue{Node: node}
	for i, clause := range plan.whereClauses {
		value, exists := propertyFromBinding(binding, clause.Property)
		switch clause.Kind {
		case whereEquals:
			if !exists || !queryValuesEqual(value, expected[i]) {
				return false
			}
		case whereIsNull:
			if exists && value != nil {
				return false
			}
		case whereIsNotNull:
			if !exists || value == nil {
				return false
			}
		case whereBindingID:
			expectedID, ok := normalizeInt64(expected[i])
			if !ok || expectedID != int64(node.ID) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func nodeMatchesQueryProperty(node *store.NodeRecord, definition store.PropertyIndexDefinition, value any) bool {
	if node == nil || !slices.Contains(node.Labels, definition.Scope) {
		return false
	}
	stored, ok := node.Properties[definition.Property]
	return ok && queryValuesEqual(stored, value)
}

func (plan *queryPlan) indexedEdgeIDs(tx *Tx, pattern edgePattern, params map[string]any) ([]uint64, bool, error) {
	if tx.queryIndexesDisabled || hasGraphChanges(tx.changes) {
		return nil, false, nil
	}
	if pattern.EdgeVar == "" || pattern.EdgeType == "" {
		return nil, false, nil
	}
	for _, clause := range plan.whereClauses {
		if clause.Kind != whereEquals || clause.Var != pattern.EdgeVar {
			continue
		}
		switch clause.Expr.(type) {
		case literalExpr, paramExpr:
		default:
			continue
		}
		value, err := clause.Expr.eval(queryRow{}, params)
		if err != nil {
			return nil, false, err
		}
		definition := store.PropertyIndexDefinition{Scope: pattern.EdgeType, Property: clause.Property}
		if !tx.graph.EdgeProperties.Has(definition) {
			continue
		}
		ids, err := tx.FindEdgesByTypeProperty(pattern.EdgeType, clause.Property, value, ^uint(0))
		if err != nil {
			return nil, false, err
		}
		if alternate, ok := alternateNumericIndexValue(value); ok {
			more, err := tx.FindEdgesByTypeProperty(pattern.EdgeType, clause.Property, alternate, ^uint(0))
			if err != nil {
				return nil, false, err
			}
			ids = append(ids, more...)
			slices.Sort(ids)
			ids = slices.Compact(ids)
		}
		return ids, true, nil
	}
	return nil, false, nil
}

func alternateNumericIndexValue(value any) (any, bool) {
	switch value := value.(type) {
	case int64:
		converted := float64(value)
		return converted, int64(converted) == value
	case float64:
		if value >= -9223372036854775808 && value < 9223372036854775808 && value == math.Trunc(value) {
			return int64(value), true
		}
	}
	return nil, false
}

func (plan *queryPlan) bindingNodeID(name string, params map[string]any) (uint64, bool, error) {
	for _, clause := range plan.whereClauses {
		if clause.Kind != whereBindingID || clause.Var != name {
			continue
		}
		switch clause.Expr.(type) {
		case literalExpr, paramExpr:
		default:
			continue
		}
		value, err := clause.Expr.eval(queryRow{}, params)
		if err != nil {
			return 0, false, err
		}
		id, ok := normalizeInt64(value)
		if !ok {
			return 0, false, fmt.Errorf("id comparison requires integer, got %T", value)
		}
		if id < 0 {
			return 0, true, nil
		}
		return uint64(id), true, nil
	}
	return 0, false, nil
}

func (clause *unwindClause) apply(rows []queryRow, params map[string]any, budget *queryBudget) ([]queryRow, error) {
	nextRows := make([]queryRow, 0)
	for _, row := range rows {
		value, err := clause.Expr.eval(row, params)
		if err != nil {
			return nil, err
		}
		list, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("UNWIND requires list value, got %T", value)
		}
		for _, item := range list {
			if err := budget.check(1, len(nextRows)+1); err != nil {
				return nil, err
			}
			nextRow := row.clone()
			nextRow.Bindings[clause.Var] = boundValue{
				Value:    store.CloneValue(item),
				HasValue: true,
			}
			nextRows = append(nextRows, nextRow)
		}
	}
	return nextRows, nil
}

func parseMatchPatterns(text string) ([]matchPattern, error) {
	parts := splitTopLevel(text, ',')
	patterns := make([]matchPattern, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty MATCH pattern in %q", text)
		}
		if findTopLevelToken(part, "-[") >= 0 || findTopLevelToken(part, "<-[") >= 0 {
			path, err := parsePathPattern(part, len(patterns))
			if err != nil {
				return nil, err
			}
			patterns = append(patterns, path...)
			continue
		}
		pattern, err := parseNodePattern(part)
		if err != nil {
			return nil, err
		}
		if pattern.Var == "" && len(pattern.PropertyExprs) != 0 {
			pattern.Var = fmt.Sprintf("\x00node%d", len(patterns))
		}
		patterns = append(patterns, pattern)
	}
	return patterns, nil
}

func patternPropertyClauses(patterns []matchPattern) []*whereClause {
	var clauses []*whereClause
	appendNode := func(pattern nodePattern) {
		keys := make([]string, 0, len(pattern.PropertyExprs))
		for key := range pattern.PropertyExprs {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			clauses = append(clauses, &whereClause{Kind: whereEquals, Var: pattern.Var, Property: key, Expr: pattern.PropertyExprs[key]})
		}
	}
	for _, item := range patterns {
		switch pattern := item.(type) {
		case nodePattern:
			appendNode(pattern)
		case edgePattern:
			appendNode(pattern.Left)
			appendNode(pattern.Right)
			keys := make([]string, 0, len(pattern.PropertyExprs))
			for key := range pattern.PropertyExprs {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			for _, key := range keys {
				clauses = append(clauses, &whereClause{Kind: whereEquals, Var: pattern.EdgeVar, Property: key, Expr: pattern.PropertyExprs[key]})
			}
		}
	}
	return clauses
}

func parseNodePattern(text string) (nodePattern, error) {
	body, err := trimEnclosed(text, '(', ')')
	if err != nil {
		return nodePattern{}, err
	}

	props := map[string]any{}
	propertyExprs := map[string]valueExpr{}
	propStart := findTopLevelRune(body, '{')
	prefix := strings.TrimSpace(body)
	if propStart >= 0 {
		propEnd := findMatchingBrace(body, propStart, '{', '}')
		if propEnd < 0 {
			return nodePattern{}, fmt.Errorf("unterminated node property map in %q", text)
		}
		parsedProps, err := parsePropertyExprMap(body[propStart+1 : propEnd])
		if err != nil {
			return nodePattern{}, err
		}
		for key, expr := range parsedProps {
			if literal, ok := expr.(literalExpr); ok {
				props[key] = literal.Value
			} else {
				propertyExprs[key] = expr
			}
		}
		if strings.TrimSpace(body[propEnd+1:]) != "" {
			return nodePattern{}, fmt.Errorf("unexpected text after node properties in %q", text)
		}
		prefix = strings.TrimSpace(body[:propStart])
	}

	segments := strings.Split(prefix, ":")
	pattern := nodePattern{Properties: props, PropertyExprs: propertyExprs}
	if len(segments) > 0 {
		first := strings.TrimSpace(segments[0])
		if first != "" {
			pattern.Var = first
		}
	}
	for _, segment := range segments[1:] {
		label := strings.TrimSpace(segment)
		if !isQueryIdentifier(label) {
			return nodePattern{}, fmt.Errorf("invalid label %q", label)
		}
		pattern.Labels = append(pattern.Labels, label)
	}
	return pattern, nil
}

func parsePathPattern(text string, pathIndex int) ([]matchPattern, error) {
	rest := strings.TrimSpace(text)
	current, rest, err := parsePathNode(rest)
	if err != nil {
		return nil, err
	}
	if current.Var == "" {
		current.Var = fmt.Sprintf("\x00node%d-0", pathIndex)
	}

	var patterns []matchPattern
	for edgeIndex := 0; rest != ""; edgeIndex++ {
		incoming := strings.HasPrefix(rest, "<-[")
		undirected := !incoming && strings.HasPrefix(rest, "-[")
		bracket := 1
		if incoming {
			bracket = 2
		} else if !strings.HasPrefix(rest, "-[") {
			return nil, fmt.Errorf("invalid edge pattern %q", text)
		}
		closeBracket := findMatchingBrace(rest, bracket, '[', ']')
		if closeBracket < 0 {
			return nil, fmt.Errorf("unterminated edge pattern %q", text)
		}
		afterEdge := strings.TrimSpace(rest[closeBracket+1:])
		if incoming {
			if !strings.HasPrefix(afterEdge, "-") {
				return nil, fmt.Errorf("invalid incoming edge pattern %q", text)
			}
			afterEdge = strings.TrimSpace(afterEdge[1:])
		} else if strings.HasPrefix(afterEdge, "->") {
			undirected = false
			afterEdge = strings.TrimSpace(afterEdge[2:])
		} else if strings.HasPrefix(afterEdge, "-") {
			afterEdge = strings.TrimSpace(afterEdge[1:])
		} else {
			return nil, fmt.Errorf("invalid edge pattern %q", text)
		}

		next, remaining, err := parsePathNode(afterEdge)
		if err != nil {
			return nil, err
		}
		if next.Var == "" {
			next.Var = fmt.Sprintf("\x00node%d-%d", pathIndex, edgeIndex+1)
		}
		pattern, err := parseEdgeBody(rest[bracket+1 : closeBracket])
		if err != nil {
			return nil, err
		}
		if pattern.EdgeVar == "" && len(pattern.PropertyExprs) != 0 {
			pattern.EdgeVar = fmt.Sprintf("\x00edge%d-%d", pathIndex, edgeIndex)
		}
		if incoming {
			pattern.Left, pattern.Right = next, current
		} else {
			pattern.Left, pattern.Right = current, next
		}
		pattern.Undirected = undirected
		patterns = append(patterns, pattern)
		current = next
		rest = remaining
	}
	if len(patterns) == 0 {
		return nil, fmt.Errorf("invalid edge pattern %q", text)
	}
	return patterns, nil
}

func parsePathNode(text string) (nodePattern, string, error) {
	text = strings.TrimSpace(text)
	if text == "" || text[0] != '(' {
		return nodePattern{}, "", fmt.Errorf("expected node pattern in %q", text)
	}
	end := findMatchingBrace(text, 0, '(', ')')
	if end < 0 {
		return nodePattern{}, "", fmt.Errorf("unterminated node pattern in %q", text)
	}
	pattern, err := parseNodePattern(text[:end+1])
	return pattern, strings.TrimSpace(text[end+1:]), err
}

func parseEdgeBody(text string) (edgePattern, error) {
	edgeBody := strings.TrimSpace(text)
	props := map[string]any{}
	propertyExprs := map[string]valueExpr{}
	prefix := edgeBody
	if propStart := findTopLevelRune(edgeBody, '{'); propStart >= 0 {
		propEnd := findMatchingBrace(edgeBody, propStart, '{', '}')
		if propEnd < 0 {
			return edgePattern{}, fmt.Errorf("unterminated edge property map in %q", text)
		}
		parsed, err := parsePropertyExprMap(edgeBody[propStart+1 : propEnd])
		if err != nil {
			return edgePattern{}, err
		}
		for key, expr := range parsed {
			if literal, ok := expr.(literalExpr); ok {
				props[key] = literal.Value
			} else {
				propertyExprs[key] = expr
			}
		}
		if strings.TrimSpace(edgeBody[propEnd+1:]) != "" {
			return edgePattern{}, fmt.Errorf("unexpected text after edge properties in %q", text)
		}
		prefix = strings.TrimSpace(edgeBody[:propStart])
	}

	edgeSegments := strings.SplitN(prefix, ":", 2)
	pattern := edgePattern{Properties: props, PropertyExprs: propertyExprs}
	if len(edgeSegments) == 2 {
		pattern.EdgeVar = strings.TrimSpace(edgeSegments[0])
		pattern.EdgeType = strings.TrimSpace(edgeSegments[1])
		if pattern.EdgeType == "" {
			return edgePattern{}, errors.New("edge type must be non-empty after colon")
		}
	} else {
		pattern.EdgeVar = strings.TrimSpace(edgeSegments[0])
	}
	if pattern.EdgeVar != "" && !isQueryIdentifier(pattern.EdgeVar) {
		return edgePattern{}, fmt.Errorf("invalid edge binding %q", pattern.EdgeVar)
	}
	if pattern.EdgeType != "" && !isQueryIdentifier(pattern.EdgeType) {
		return edgePattern{}, fmt.Errorf("invalid edge type %q", pattern.EdgeType)
	}
	return pattern, nil
}

func parseWherePredicate(text string) (wherePredicate, error) {
	text = strings.TrimSpace(text)
	for len(text) >= 2 && text[0] == '(' && findMatchingBrace(text, 0, '(', ')') == len(text)-1 {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	if text == "" {
		return nil, errors.New("WHERE expression must be non-empty")
	}
	if parts := splitTopLevelKeyword(text, " OR "); len(parts) > 1 {
		items := make([]wherePredicate, 0, len(parts))
		for _, part := range parts {
			item, err := parseWherePredicate(part)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return booleanPredicate{Operator: "OR", Items: items}, nil
	}
	if parts := splitTopLevelKeyword(text, " AND "); len(parts) > 1 {
		items := make([]wherePredicate, 0, len(parts))
		for _, part := range parts {
			item, err := parseWherePredicate(part)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return booleanPredicate{Operator: "AND", Items: items}, nil
	}
	if strings.HasPrefix(text, "NOT ") {
		item, err := parseWherePredicate(strings.TrimSpace(strings.TrimPrefix(text, "NOT ")))
		if err != nil {
			return nil, err
		}
		return notPredicate{Item: item}, nil
	}
	return parseWhereClause(text)
}

func flattenWhereConjunction(predicate wherePredicate) ([]*whereClause, bool) {
	switch predicate := predicate.(type) {
	case *whereClause:
		return []*whereClause{predicate}, true
	case booleanPredicate:
		if predicate.Operator != "AND" {
			return nil, false
		}
		var clauses []*whereClause
		for _, item := range predicate.Items {
			flat, ok := flattenWhereConjunction(item)
			if !ok {
				return nil, false
			}
			clauses = append(clauses, flat...)
		}
		return clauses, true
	default:
		return nil, false
	}
}

func wherePredicateClauses(predicate wherePredicate) []*whereClause {
	switch predicate := predicate.(type) {
	case nil:
		return nil
	case *whereClause:
		return []*whereClause{predicate}
	case booleanPredicate:
		var clauses []*whereClause
		for _, item := range predicate.Items {
			clauses = append(clauses, wherePredicateClauses(item)...)
		}
		return clauses
	case notPredicate:
		return wherePredicateClauses(predicate.Item)
	default:
		panic("unsupported WHERE predicate")
	}
}

func parseWhereClause(text string) (*whereClause, error) {
	text = strings.TrimSpace(text)
	if strings.HasSuffix(text, " IS NOT NULL") {
		varName, property, err := parsePropertyAccess(strings.TrimSuffix(text, " IS NOT NULL"))
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereIsNotNull, Var: varName, Property: property}, nil
	}
	if strings.HasSuffix(text, " IS NULL") {
		varName, property, err := parsePropertyAccess(strings.TrimSuffix(text, " IS NULL"))
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereIsNull, Var: varName, Property: property}, nil
	}
	if left, right, ok := splitOperator(text, " <=> "); ok {
		varName, property, err := parsePropertyAccess(left)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereVector, Var: varName, Property: property, Expr: expr}, nil
	}
	if left, right, ok := splitOperator(text, " @@ "); ok {
		varName, property, err := parsePropertyAccess(left)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereFTS, Var: varName, Property: property, Expr: expr}, nil
	}
	for _, operator := range []struct {
		Token string
		Kind  whereKind
	}{
		{" STARTS WITH ", whereStartsWith},
		{" ENDS WITH ", whereEndsWith},
		{" CONTAINS ", whereContains},
		{" IN ", whereIn},
	} {
		if left, right, ok := splitOperator(text, operator.Token); ok {
			varName, property, err := parsePropertyAccess(left)
			if err != nil {
				return nil, err
			}
			expr, err := parseValueExpr(right)
			if err != nil {
				return nil, err
			}
			return &whereClause{Kind: operator.Kind, Var: varName, Property: property, Expr: expr}, nil
		}
	}
	if left, right, ok := splitOperator(text, " = "); ok {
		if varName, ok := parseBindingIDAccess(left); ok {
			expr, err := parseValueExpr(right)
			if err != nil {
				return nil, err
			}
			return &whereClause{Kind: whereBindingID, Var: varName, Expr: expr}, nil
		}
		varName, property, err := parsePropertyAccess(left)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereEquals, Var: varName, Property: property, Expr: expr}, nil
	}
	for _, operator := range []struct {
		Token string
		Kind  whereKind
	}{
		{" <> ", whereNotEquals},
		{" <= ", whereLessEqual},
		{" >= ", whereGreaterEqual},
		{" < ", whereLess},
		{" > ", whereGreater},
	} {
		if left, right, ok := splitOperator(text, operator.Token); ok {
			varName, property, err := parsePropertyAccess(left)
			if err != nil {
				return nil, err
			}
			expr, err := parseValueExpr(right)
			if err != nil {
				return nil, err
			}
			return &whereClause{Kind: operator.Kind, Var: varName, Property: property, Expr: expr}, nil
		}
	}
	return nil, fmt.Errorf("unsupported WHERE clause %q", text)
}

func parseSetClause(text string) (*setClause, error) {
	if !strings.Contains(text, " = ") && !strings.Contains(text, " += ") {
		left, right, ok := splitOperator(text, ":")
		if !ok || !isQueryIdentifier(left) || !isQueryIdentifier(right) {
			return nil, fmt.Errorf("unsupported SET clause %q", text)
		}
		return &setClause{Kind: setLabel, Var: left, Label: right}, nil
	}
	if left, right, ok := splitOperator(text, " += "); ok {
		name := strings.TrimSpace(left)
		if name == "" || strings.Contains(name, ".") {
			return nil, fmt.Errorf("invalid SET merge target %q", left)
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &setClause{Kind: setMerge, Var: name, Expr: expr}, nil
	}

	left, right, ok := splitOperator(text, " = ")
	if !ok {
		return nil, fmt.Errorf("unsupported SET clause %q", text)
	}
	expr, err := parseValueExpr(right)
	if err != nil {
		return nil, err
	}
	if varName, property, err := parsePropertyAccess(left); err == nil {
		return &setClause{Kind: setProperty, Var: varName, Property: property, Expr: expr}, nil
	}
	name := strings.TrimSpace(left)
	if name == "" {
		return nil, fmt.Errorf("invalid SET target %q", left)
	}
	return &setClause{Kind: setReplace, Var: name, Expr: expr}, nil
}

func parseSetClauses(text string) ([]*setClause, error) {
	parts := splitTopLevel(text, ',')
	clauses := make([]*setClause, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return nil, fmt.Errorf("invalid SET clause %q", text)
		}
		clause, err := parseSetClause(part)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}
	return clauses, nil
}

func parseCreateClause(text string) (*createClause, error) {
	patterns, err := parsePathPattern(text, 0)
	if err != nil || len(patterns) != 1 {
		return nil, fmt.Errorf("unsupported CREATE clause %q", text)
	}
	pattern := patterns[0].(edgePattern)
	validEndpoint := func(node nodePattern) bool {
		return node.Var != "" && node.Var[0] != 0 && len(node.Labels) == 0 && len(node.Properties) == 0 && len(node.PropertyExprs) == 0
	}
	if !validEndpoint(pattern.Left) || !validEndpoint(pattern.Right) || pattern.EdgeType == "" || pattern.Undirected {
		return nil, fmt.Errorf("invalid CREATE edge pattern %q", text)
	}

	props := map[string]valueExpr{}
	for key, value := range pattern.Properties {
		props[key] = literalExpr{Value: value}
	}
	for key, expr := range pattern.PropertyExprs {
		props[key] = expr
	}

	return &createClause{
		SourceVar: pattern.Left.Var,
		TargetVar: pattern.Right.Var,
		EdgeVar:   pattern.EdgeVar,
		EdgeType:  pattern.EdgeType,
		Props:     props,
	}, nil
}

func parseCreateNodeClause(text string) (*createNodeClause, error) {
	body, err := trimEnclosed(text, '(', ')')
	if err != nil {
		return nil, err
	}

	props := map[string]valueExpr{}
	propStart := findTopLevelRune(body, '{')
	prefix := strings.TrimSpace(body)
	if propStart >= 0 {
		propEnd := findMatchingBrace(body, propStart, '{', '}')
		if propEnd < 0 {
			return nil, fmt.Errorf("unterminated CREATE property map in %q", text)
		}
		parsedProps, err := parsePropertyExprMap(body[propStart+1 : propEnd])
		if err != nil {
			return nil, err
		}
		props = parsedProps
		if strings.TrimSpace(body[propEnd+1:]) != "" {
			return nil, fmt.Errorf("unexpected text after CREATE node properties in %q", text)
		}
		prefix = strings.TrimSpace(body[:propStart])
	}

	segments := strings.Split(prefix, ":")
	clause := &createNodeClause{Props: props}
	if len(segments) > 0 {
		first := strings.TrimSpace(segments[0])
		if first != "" {
			clause.Var = first
		}
	}
	for _, segment := range segments[1:] {
		label := strings.TrimSpace(segment)
		if !isQueryIdentifier(label) {
			return nil, fmt.Errorf("invalid label %q", label)
		}
		clause.Labels = append(clause.Labels, label)
	}
	return clause, nil
}

func parseDeleteClause(text string) (*deleteClause, error) {
	parts := splitTopLevel(text, ',')
	vars := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("invalid DELETE clause %q", text)
		}
		vars = append(vars, name)
	}
	if len(vars) == 0 {
		return nil, fmt.Errorf("invalid DELETE clause %q", text)
	}
	return &deleteClause{Vars: vars}, nil
}

func parseRemoveClause(text string) (*removeClause, error) {
	parts := splitTopLevel(text, ',')
	items := make([]removeItem, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid REMOVE clause %q", text)
		}
		if varName, property, err := parsePropertyAccess(part); err == nil {
			items = append(items, removeItem{Var: varName, Property: property})
			continue
		}
		left, right, ok := splitOperator(part, ":")
		if !ok {
			return nil, fmt.Errorf("invalid REMOVE clause item %q", part)
		}
		varName := strings.TrimSpace(left)
		label := strings.TrimSpace(right)
		if varName == "" || !isQueryIdentifier(label) {
			return nil, fmt.Errorf("invalid REMOVE clause item %q", part)
		}
		items = append(items, removeItem{Var: varName, Label: label})
	}
	return &removeClause{Items: items}, nil
}

func parseReturnClause(text string) (*returnClause, error) {
	text = strings.TrimSpace(text)
	distinct := strings.HasPrefix(text, "DISTINCT ")
	if distinct {
		text = strings.TrimSpace(strings.TrimPrefix(text, "DISTINCT "))
	}
	if strings.HasPrefix(text, "count(") {
		closeIdx := strings.Index(text, ")")
		if closeIdx < 0 {
			return nil, fmt.Errorf("invalid count return %q", text)
		}
		derivedAlias := strings.TrimSpace(text[:closeIdx+1])
		countVar := strings.TrimSpace(text[len("count("):closeIdx])
		rest := strings.TrimSpace(text[closeIdx+1:])
		switch {
		case rest == "":
			return &returnClause{
				CountVar:   countVar,
				CountAlias: derivedAlias,
				Distinct:   distinct,
			}, nil
		case !strings.HasPrefix(rest, "AS "):
			return nil, fmt.Errorf("invalid count return %q", text)
		}
		alias := strings.TrimSpace(strings.TrimPrefix(rest, "AS "))
		if !isQueryIdentifier(alias) {
			return nil, fmt.Errorf("invalid count alias %q", alias)
		}
		return &returnClause{
			CountVar:   countVar,
			CountAlias: alias,
			Distinct:   distinct,
		}, nil
	}

	parts := splitTopLevel(text, ',')
	projections := make([]projection, 0, len(parts))
	aliases := map[string]struct{}{}
	for _, part := range parts {
		exprText := strings.TrimSpace(part)
		if exprText == "" {
			return nil, errors.New("RETURN projection must be non-empty")
		}
		alias := exprText
		pieces := strings.SplitN(exprText, " AS ", 2)
		if len(pieces) == 2 {
			exprText = strings.TrimSpace(pieces[0])
			alias = strings.TrimSpace(pieces[1])
			if !isQueryIdentifier(alias) {
				return nil, fmt.Errorf("invalid RETURN alias %q", alias)
			}
		}
		if exprText == "" || alias == "" {
			return nil, fmt.Errorf("invalid RETURN projection %q", part)
		}
		if _, duplicate := aliases[alias]; duplicate {
			return nil, fmt.Errorf("duplicate RETURN alias %q", alias)
		}
		aliases[alias] = struct{}{}
		if varName, ok := parseBindingIDAccess(exprText); ok {
			projections = append(projections, projection{
				Kind:  projectionBindingID,
				Var:   varName,
				Alias: alias,
			})
			continue
		}
		if !strings.Contains(exprText, ".") {
			projections = append(projections, projection{
				Kind:  projectionValue,
				Var:   exprText,
				Alias: alias,
			})
			continue
		}
		varName, property, err := parsePropertyAccess(exprText)
		if err != nil {
			return nil, err
		}
		projections = append(projections, projection{
			Kind:     projectionProperty,
			Var:      varName,
			Property: property,
			Alias:    alias,
		})
	}
	return &returnClause{Projections: projections, Distinct: distinct}, nil
}

func parseReturnTail(text string) (string, []orderClause, valueExpr, valueExpr, error) {
	returnText, keyword, tail := splitOnNextClause(text, " ORDER BY ", " SKIP ", " LIMIT ")
	var clauses []orderClause
	if keyword == " ORDER BY " {
		orderText, next, afterOrder := splitOnNextClause(tail, " SKIP ", " LIMIT ")
		var err error
		clauses, err = parseOrderClauses(orderText)
		if err != nil {
			return "", nil, nil, nil, err
		}
		keyword, tail = next, afterOrder
	}

	var skipExpr, limitExpr valueExpr
	var err error
	if keyword == " SKIP " {
		skipText, next, afterSkip := splitOnNextClause(tail, " LIMIT ")
		skipExpr, err = parsePaginationExpr("SKIP", skipText)
		if err != nil {
			return "", nil, nil, nil, err
		}
		keyword, tail = next, afterSkip
	}
	if keyword == " LIMIT " {
		limitExpr, err = parsePaginationExpr("LIMIT", tail)
		if err != nil {
			return "", nil, nil, nil, err
		}
		keyword = ""
	}
	if keyword != "" {
		return "", nil, nil, nil, fmt.Errorf("unsupported RETURN clause %q", keyword)
	}
	return returnText, clauses, skipExpr, limitExpr, nil
}

func resolveOrderAliases(clause *returnClause, orders []orderClause) []orderClause {
	resolved := orders[:0]
	for _, order := range orders {
		if order.Kind != projectionValue {
			resolved = append(resolved, order)
			continue
		}
		if clause.CountAlias == order.Var {
			continue
		}
		for _, projection := range clause.Projections {
			if projection.Alias == order.Var {
				order.Kind = projection.Kind
				order.Var = projection.Var
				order.Property = projection.Property
				break
			}
		}
		resolved = append(resolved, order)
	}
	return resolved
}

func parseOrderClauses(text string) ([]orderClause, error) {
	parts := splitTopLevel(text, ',')
	clauses := make([]orderClause, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		clause := orderClause{}
		switch {
		case strings.HasSuffix(part, " DESC"):
			clause.Desc = true
			part = strings.TrimSpace(strings.TrimSuffix(part, " DESC"))
		case strings.HasSuffix(part, " ASC"):
			part = strings.TrimSpace(strings.TrimSuffix(part, " ASC"))
		}
		if name, ok := parseBindingIDAccess(part); ok {
			clause.Kind = projectionBindingID
			clause.Var = name
		} else if strings.Contains(part, ".") {
			name, property, err := parsePropertyAccess(part)
			if err != nil {
				return nil, err
			}
			clause.Kind = projectionProperty
			clause.Var = name
			clause.Property = property
		} else if isQueryIdentifier(part) {
			clause.Kind = projectionValue
			clause.Var = part
		} else {
			return nil, fmt.Errorf("invalid ORDER BY expression %q", part)
		}
		clauses = append(clauses, clause)
	}
	if len(clauses) == 0 {
		return nil, errors.New("ORDER BY expression must be non-empty")
	}
	return clauses, nil
}

func (clause orderClause) value(row queryRow) any {
	binding, ok := row.Bindings[clause.Var]
	if !ok {
		return nil
	}
	switch clause.Kind {
	case projectionBindingID:
		value, _ := bindingID(binding)
		return value
	case projectionProperty:
		value, _ := propertyFromBinding(binding, clause.Property)
		return value
	case projectionValue:
		if binding.HasValue {
			return binding.Value
		}
		return bindingIDValue(binding)
	default:
		return nil
	}
}

func bindingIDValue(binding boundValue) any {
	value, ok := bindingID(binding)
	if !ok {
		return nil
	}
	return value
}

func compareOrderValues(left, right any) int {
	if left == nil {
		if right == nil {
			return 0
		}
		return 1
	}
	if right == nil {
		return -1
	}
	if leftInt, ok := normalizeInt64(left); ok {
		if rightInt, ok := normalizeInt64(right); ok {
			return cmp.Compare(leftInt, rightInt)
		}
	}
	if leftFloat, ok := left.(float64); ok {
		if rightFloat, ok := right.(float64); ok {
			return cmp.Compare(leftFloat, rightFloat)
		}
	}
	if leftString, ok := left.(string); ok {
		if rightString, ok := right.(string); ok {
			return strings.Compare(leftString, rightString)
		}
	}
	if leftBool, ok := left.(bool); ok {
		if rightBool, ok := right.(bool); ok {
			switch {
			case leftBool == rightBool:
				return 0
			case !leftBool:
				return -1
			default:
				return 1
			}
		}
	}
	return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
}

func (pattern nodePattern) apply(tx *Tx, rows []queryRow, budget *queryBudget) ([]queryRow, error) {
	nextRows := make([]queryRow, 0)
	var nodeIDs []uint64
	if len(pattern.Labels) == 0 {
		nodeIDs = store.SortedNodeIDs(tx.graph)
	} else {
		nodeIDs = tx.graph.Labels.Get(pattern.Labels[0])
		for _, label := range pattern.Labels[1:] {
			if ids := tx.graph.Labels.Get(label); len(ids) < len(nodeIDs) {
				nodeIDs = ids
			}
		}
	}
	if err := budget.chargeTemporary(uint64(len(nodeIDs)) * 8); err != nil {
		return nil, err
	}
	for _, nodeID := range nodeIDs {
		nextRows = pattern.appendNodeRows(rows, tx.graph.Nodes.Get(nodeID), nextRows)
		if err := budget.check(uint64(len(rows)), len(nextRows)); err != nil {
			return nil, err
		}
	}
	return nextRows, nil
}

func (pattern nodePattern) applyID(tx *Tx, rows []queryRow, nodeID uint64) []queryRow {
	node := tx.graph.Nodes.Get(nodeID)
	if node == nil {
		return nil
	}
	return pattern.appendNodeRows(rows, node, nil)
}

func (pattern nodePattern) appendNodeRows(rows []queryRow, node *store.NodeRecord, nextRows []queryRow) []queryRow {
	for _, row := range rows {
		if !store.LabelsMatch(node, pattern.Labels) {
			continue
		}
		if !store.PropertiesMatch(node.Properties, pattern.Properties) {
			continue
		}
		if pattern.Var != "" {
			if existing, ok := row.Bindings[pattern.Var]; ok {
				if existing.Node == nil || existing.Node.ID != node.ID {
					continue
				}
				nextRows = append(nextRows, row)
				continue
			}
		}
		nextRow := row.clone()
		if pattern.Var != "" {
			nextRow.Bindings[pattern.Var] = boundValue{Node: node}
		}
		nextRows = append(nextRows, nextRow)
	}
	return nextRows
}

func (pattern edgePattern) apply(tx *Tx, rows []queryRow, budget *queryBudget) ([]queryRow, error) {
	var edgeIDs []uint64
	if pattern.EdgeType == "" {
		edgeIDs = store.SortedEdgeIDs(tx.graph)
	} else {
		edgeIDs = tx.graph.EdgeTypes.Get(pattern.EdgeType)
	}
	return pattern.applyIDs(tx, rows, edgeIDs, budget)
}

func (pattern edgePattern) applyIDs(tx *Tx, rows []queryRow, edgeIDs []uint64, budget *queryBudget) ([]queryRow, error) {
	if err := budget.chargeTemporary(uint64(len(edgeIDs)) * 8); err != nil {
		return nil, err
	}
	nextRows := make([]queryRow, 0)
	for _, row := range rows {
		for _, edgeID := range edgeIDs {
			if err := budget.check(1, len(nextRows)); err != nil {
				return nil, err
			}
			edge := tx.graph.Edges.Get(edgeID)
			if pattern.EdgeType != "" && edge.Type != pattern.EdgeType {
				continue
			}
			if !store.PropertiesMatch(edge.Properties, pattern.Properties) {
				continue
			}
			source := tx.graph.Nodes.Get(edge.SourceID)
			target := tx.graph.Nodes.Get(edge.TargetID)
			if source == nil || target == nil {
				continue
			}
			if pattern.EdgeVar != "" {
				if existing, ok := row.Bindings[pattern.EdgeVar]; ok && (existing.Edge == nil || existing.Edge.ID != edge.ID) {
					continue
				}
			}
			var err error
			nextRows, err = pattern.appendEdgeRow(row, edge, source, target, nextRows, budget)
			if err != nil {
				return nil, err
			}
			if pattern.Undirected && source.ID != target.ID {
				if err := budget.check(1, len(nextRows)); err != nil {
					return nil, err
				}
				nextRows, err = pattern.appendEdgeRow(row, edge, target, source, nextRows, budget)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return nextRows, nil
}

func (pattern edgePattern) appendEdgeRow(row queryRow, edge *store.EdgeRecord, left, right *store.NodeRecord, rows []queryRow, budget *queryBudget) ([]queryRow, error) {
	if pattern.Left.Var != "" && pattern.Left.Var == pattern.Right.Var && left.ID != right.ID {
		return rows, nil
	}
	if !store.LabelsMatch(left, pattern.Left.Labels) || !store.PropertiesMatch(left.Properties, pattern.Left.Properties) ||
		!store.LabelsMatch(right, pattern.Right.Labels) || !store.PropertiesMatch(right.Properties, pattern.Right.Properties) ||
		!bindingMatchesNode(row, pattern.Left.Var, left) || !bindingMatchesNode(row, pattern.Right.Var, right) {
		return rows, nil
	}
	if err := budget.check(0, len(rows)+1); err != nil {
		return nil, err
	}
	nextRow := row.clone()
	if pattern.Left.Var != "" {
		nextRow.Bindings[pattern.Left.Var] = boundValue{Node: left}
	}
	if pattern.Right.Var != "" {
		nextRow.Bindings[pattern.Right.Var] = boundValue{Node: right}
	}
	if pattern.EdgeVar != "" {
		nextRow.Bindings[pattern.EdgeVar] = boundValue{Edge: edge}
	}
	return append(rows, nextRow), nil
}

func (clause *whereClause) apply(tx *Tx, rows []queryRow, params map[string]any, budget *queryBudget) ([]queryRow, error) {
	filtered := rows[:0]
	for _, row := range rows {
		if err := budget.check(1, len(filtered)); err != nil {
			return nil, err
		}
		binding, ok := row.Bindings[clause.Var]
		if !ok {
			continue
		}
		if clause.Kind != whereVector && clause.Kind != whereFTS {
			match, err := clause.eval(row, params)
			if err != nil {
				return nil, err
			}
			if match == predicateTrue {
				filtered = append(filtered, row)
			}
			continue
		}
		value, exists := propertyFromBinding(binding, clause.Property)
		switch clause.Kind {
		case whereVector:
			if !exists {
				continue
			}
			vector, ok := value.([]float32)
			if !ok {
				continue
			}
			expected, err := clause.Expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			queryVector, ok := expected.([]float32)
			if !ok {
				return nil, fmt.Errorf("vector comparison requires []float32, got %T", expected)
			}
			distance, err := search.VectorDistance(vector, queryVector)
			if err != nil {
				return nil, err
			}
			row.Order = float64(distance)
			filtered = append(filtered, row)
		case whereFTS:
			expected, err := clause.Expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			queryText, ok := expected.(string)
			if !ok {
				return nil, fmt.Errorf("fts comparison requires string, got %T", expected)
			}
			terms := search.Tokenize(queryText)
			var score float32
			hasIndex := false
			if binding.Node != nil {
				if record := tx.graph.FTS.Get(binding.Node.ID); record != nil {
					hasIndex = true
					score = search.FTSScoreTokensWithOptions(record.Tokens, terms, 0, 0)
				}
			}
			if !hasIndex && exists {
				if text, ok := value.(string); ok {
					score = search.FTSScore(text, terms)
				}
			}
			if score <= 0 {
				continue
			}
			row.Order = -float64(score)
			filtered = append(filtered, row)
		default:
			return nil, fmt.Errorf("unsupported WHERE kind %q", clause.Kind)
		}
	}

	if clause.Kind == whereVector || clause.Kind == whereFTS {
		slices.SortFunc(filtered, func(a queryRow, b queryRow) int {
			if a.Order < b.Order {
				return -1
			}
			if a.Order > b.Order {
				return 1
			}
			return compareRowBindings(a, b)
		})
	}
	return filtered, nil
}

func (clause *whereClause) eval(row queryRow, params map[string]any) (predicateTruth, error) {
	binding, ok := row.Bindings[clause.Var]
	if !ok {
		return predicateUnknown, nil
	}
	if clause.Kind == whereBindingID {
		expected, err := clause.Expr.eval(row, params)
		if err != nil {
			return predicateUnknown, err
		}
		expectedID, ok := normalizeInt64(expected)
		if !ok {
			return predicateUnknown, fmt.Errorf("id comparison requires integer, got %T", expected)
		}
		gotID, ok := bindingID(binding)
		return predicateBool(ok && gotID == expectedID), nil
	}
	value, exists := propertyFromBinding(binding, clause.Property)
	switch clause.Kind {
	case whereIsNull:
		return predicateBool(!exists || value == nil), nil
	case whereIsNotNull:
		return predicateBool(exists && value != nil), nil
	case whereEquals:
		expected, err := clause.Expr.eval(row, params)
		if err != nil || !exists || value == nil || expected == nil {
			return predicateUnknown, err
		}
		return predicateBool(queryValuesEqual(value, expected)), nil
	case whereNotEquals:
		expected, err := clause.Expr.eval(row, params)
		if err != nil || !exists || value == nil || expected == nil {
			return predicateUnknown, err
		}
		return predicateBool(!queryValuesEqual(value, expected)), nil
	case whereLess, whereLessEqual, whereGreater, whereGreaterEqual:
		expected, err := clause.Expr.eval(row, params)
		if err != nil || !exists {
			return predicateUnknown, err
		}
		comparison, comparable := compareQueryValues(value, expected)
		if !comparable {
			return predicateUnknown, nil
		}
		return predicateBool(comparisonMatches(clause.Kind, comparison)), nil
	case whereIn:
		expected, err := clause.Expr.eval(row, params)
		if err != nil || !exists || value == nil || expected == nil {
			return predicateUnknown, err
		}
		items, ok := expected.([]any)
		if !ok {
			return predicateUnknown, fmt.Errorf("IN requires list value, got %T", expected)
		}
		unknown := false
		for _, item := range items {
			if item == nil {
				unknown = true
				continue
			}
			if queryValuesEqual(value, item) {
				return predicateTrue, nil
			}
		}
		if unknown {
			return predicateUnknown, nil
		}
		return predicateFalse, nil
	case whereStartsWith, whereEndsWith, whereContains:
		expected, err := clause.Expr.eval(row, params)
		if err != nil || !exists || value == nil || expected == nil {
			return predicateUnknown, err
		}
		actualText, actualOK := value.(string)
		expectedText, expectedOK := expected.(string)
		if !actualOK || !expectedOK {
			return predicateUnknown, nil
		}
		switch clause.Kind {
		case whereStartsWith:
			return predicateBool(strings.HasPrefix(actualText, expectedText)), nil
		case whereEndsWith:
			return predicateBool(strings.HasSuffix(actualText, expectedText)), nil
		default:
			return predicateBool(strings.Contains(actualText, expectedText)), nil
		}
	default:
		return predicateUnknown, fmt.Errorf("unsupported boolean WHERE kind %q", clause.Kind)
	}
}

func (predicate booleanPredicate) eval(row queryRow, params map[string]any) (predicateTruth, error) {
	unknown := false
	for _, item := range predicate.Items {
		match, err := item.eval(row, params)
		if err != nil {
			return predicateUnknown, err
		}
		if match == predicateUnknown {
			unknown = true
		}
		if predicate.Operator == "OR" && match == predicateTrue {
			return predicateTrue, nil
		}
		if predicate.Operator == "AND" && match == predicateFalse {
			return predicateFalse, nil
		}
	}
	if unknown {
		return predicateUnknown, nil
	}
	return predicateBool(predicate.Operator == "AND"), nil
}

func (predicate notPredicate) eval(row queryRow, params map[string]any) (predicateTruth, error) {
	match, err := predicate.Item.eval(row, params)
	if err != nil || match == predicateUnknown {
		return predicateUnknown, err
	}
	return predicateBool(match == predicateFalse), nil
}

func predicateBool(value bool) predicateTruth {
	if value {
		return predicateTrue
	}
	return predicateFalse
}

func compareQueryValues(left, right any) (int, bool) {
	if left == nil || right == nil {
		return 0, false
	}
	if leftInt, ok := normalizeInt64(left); ok {
		if rightInt, ok := normalizeInt64(right); ok {
			return cmp.Compare(leftInt, rightInt), true
		}
		if rightFloat, ok := right.(float64); ok {
			return compareIntegerFloat(leftInt, rightFloat)
		}
	}
	if leftFloat, ok := left.(float64); ok {
		if rightInt, ok := normalizeInt64(right); ok {
			comparison, comparable := compareIntegerFloat(rightInt, leftFloat)
			return -comparison, comparable
		}
		if rightFloat, ok := right.(float64); ok {
			return cmp.Compare(leftFloat, rightFloat), true
		}
	}
	if leftString, ok := left.(string); ok {
		rightString, ok := right.(string)
		if ok {
			return strings.Compare(leftString, rightString), true
		}
	}
	return 0, false
}

func compareIntegerFloat(integer int64, floating float64) (int, bool) {
	if math.IsNaN(floating) {
		return 0, false
	}
	if floating >= 9223372036854775808 {
		return -1, true
	}
	if floating < -9223372036854775808 {
		return 1, true
	}
	if comparison := cmp.Compare(integer, int64(floating)); comparison != 0 {
		return comparison, true
	}
	return cmp.Compare(float64(integer), floating), true
}

func comparisonMatches(kind whereKind, comparison int) bool {
	switch kind {
	case whereNotEquals:
		return comparison != 0
	case whereLess:
		return comparison < 0
	case whereLessEqual:
		return comparison <= 0
	case whereGreater:
		return comparison > 0
	case whereGreaterEqual:
		return comparison >= 0
	default:
		return false
	}
}

func queryValuesEqual(left any, right any) bool {
	if left == nil || right == nil {
		return false
	}
	leftInt, leftIsInt := left.(int64)
	rightInt, rightIsInt := right.(int64)
	if leftIsInt && rightIsInt {
		return leftInt == rightInt
	}
	leftFloat, leftIsFloat := left.(float64)
	rightFloat, rightIsFloat := right.(float64)
	switch {
	case leftIsFloat && rightIsFloat:
		return leftFloat == rightFloat
	case leftIsInt && rightIsFloat:
		return integerEqualsFloat(leftInt, rightFloat)
	case leftIsFloat && rightIsInt:
		return integerEqualsFloat(rightInt, leftFloat)
	default:
		return reflect.DeepEqual(left, right)
	}
}

func integerEqualsFloat(integer int64, floating float64) bool {
	return floating >= -9223372036854775808 && floating < 9223372036854775808 && floating == math.Trunc(floating) && int64(floating) == integer
}

func (clause *setClause) apply(tx *Tx, rows []queryRow, params map[string]any) error {
	for index := range rows {
		row := rows[index]
		refreshRowBindings(tx, &row)
		binding, ok := row.Bindings[clause.Var]
		if !ok {
			continue
		}
		var normalized any
		var err error
		if clause.Kind != setLabel {
			value, evalErr := clause.Expr.eval(row, params)
			if evalErr != nil {
				return evalErr
			}
			normalized, err = store.NormalizeValue(value)
			if err != nil {
				return err
			}
		}
		switch clause.Kind {
		case setProperty:
			switch {
			case binding.Node != nil:
				binding.Node, err = tx.writableNode(binding.Node.ID)
				if err != nil {
					return err
				}
				if normalized == nil {
					delete(binding.Node.Properties, clause.Property)
				} else {
					binding.Node.Properties[clause.Property] = normalized
				}
			case binding.Edge != nil:
				binding.Edge, err = tx.writableEdge(binding.Edge.ID)
				if err != nil {
					return err
				}
				if normalized == nil {
					delete(binding.Edge.Properties, clause.Property)
				} else {
					binding.Edge.Properties[clause.Property] = normalized
				}
			default:
				return fmt.Errorf("binding %q is neither node nor edge", clause.Var)
			}
		case setReplace:
			props, err := replacementPropertyMap(normalized)
			if err != nil {
				return err
			}
			switch {
			case binding.Node != nil:
				binding.Node, err = tx.writableNode(binding.Node.ID)
				if err != nil {
					return err
				}
				binding.Node.Properties = props
			case binding.Edge != nil:
				binding.Edge, err = tx.writableEdge(binding.Edge.ID)
				if err != nil {
					return err
				}
				binding.Edge.Properties = props
			default:
				return fmt.Errorf("binding %q is neither node nor edge", clause.Var)
			}
		case setMerge:
			props, err := mergePropertyMap(normalized)
			if err != nil {
				return err
			}
			switch {
			case binding.Node != nil:
				binding.Node, err = tx.writableNode(binding.Node.ID)
				if err != nil {
					return err
				}
				mergeMutationProperties(binding.Node.Properties, props)
			case binding.Edge != nil:
				binding.Edge, err = tx.writableEdge(binding.Edge.ID)
				if err != nil {
					return err
				}
				mergeMutationProperties(binding.Edge.Properties, props)
			default:
				return fmt.Errorf("binding %q is neither node nor edge", clause.Var)
			}
		case setLabel:
			if binding.Node == nil {
				return fmt.Errorf("binding %q is not a node", clause.Var)
			}
			binding.Node, err = tx.writableNode(binding.Node.ID)
			if err != nil {
				return err
			}
			if !slices.Contains(binding.Node.Labels, clause.Label) {
				binding.Node.Labels = append(binding.Node.Labels, clause.Label)
				tx.graph.Labels.Add(clause.Label, binding.Node.ID)
			}
		default:
			return fmt.Errorf("unsupported SET kind %q", clause.Kind)
		}
		row.Bindings[clause.Var] = binding
		rows[index] = row
	}
	return nil
}

func refreshRowBindings(tx *Tx, row *queryRow) {
	for name, binding := range row.Bindings {
		switch {
		case binding.Node != nil:
			if current := tx.graph.Nodes.Get(binding.Node.ID); current != nil {
				binding.Node = current
			}
		case binding.Edge != nil:
			if current := tx.graph.Edges.Get(binding.Edge.ID); current != nil {
				binding.Edge = current
			}
		}
		row.Bindings[name] = binding
	}
}

func (clause *createNodeClause) apply(tx *Tx, rows []queryRow, params map[string]any) ([]queryRow, error) {
	nextRows := make([]queryRow, 0, len(rows))
	for _, row := range rows {
		props := make(map[string]any, len(clause.Props))
		for key, expr := range clause.Props {
			value, err := expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			normalized, err := store.NormalizeValue(value)
			if err != nil {
				return nil, err
			}
			props[key] = normalized
		}

		node, err := tx.CreateNode(CreateNodeOptions{
			Labels:     slices.Clone(clause.Labels),
			Properties: props,
		})
		if err != nil {
			return nil, err
		}

		nextRow := row.clone()
		if clause.Var != "" {
			nextRow.Bindings[clause.Var] = boundValue{Node: tx.graph.Nodes.Get(node.ID)}
		}
		nextRows = append(nextRows, nextRow)
	}
	return nextRows, nil
}

func (clause *removeClause) apply(tx *Tx, rows []queryRow) error {
	for _, row := range rows {
		for _, item := range clause.Items {
			binding, ok := row.Bindings[item.Var]
			if !ok {
				return fmt.Errorf("unknown binding %q", item.Var)
			}
			switch {
			case item.Property != "":
				switch {
				case binding.Node != nil:
					var err error
					binding.Node, err = tx.writableNode(binding.Node.ID)
					if err != nil {
						return err
					}
					delete(binding.Node.Properties, item.Property)
				case binding.Edge != nil:
					var err error
					binding.Edge, err = tx.writableEdge(binding.Edge.ID)
					if err != nil {
						return err
					}
					delete(binding.Edge.Properties, item.Property)
				default:
					return fmt.Errorf("binding %q is neither node nor edge", item.Var)
				}
			case item.Label != "":
				if binding.Node == nil {
					return fmt.Errorf("binding %q is not a node", item.Var)
				}
				var err error
				binding.Node, err = tx.writableNode(binding.Node.ID)
				if err != nil {
					return err
				}
				if slices.Contains(binding.Node.Labels, item.Label) {
					tx.graph.Labels.Remove(item.Label, binding.Node.ID)
				}
				binding.Node.Labels = removeLabel(binding.Node.Labels, item.Label)
			default:
				return fmt.Errorf("invalid REMOVE item for binding %q", item.Var)
			}
			row.Bindings[item.Var] = binding
		}
	}
	return nil
}

func (clause *createClause) apply(tx *Tx, rows []queryRow, params map[string]any) error {
	for _, row := range rows {
		sourceBinding, ok := row.Bindings[clause.SourceVar]
		if !ok || sourceBinding.Node == nil {
			return fmt.Errorf("unknown source binding %q", clause.SourceVar)
		}
		targetBinding, ok := row.Bindings[clause.TargetVar]
		if !ok || targetBinding.Node == nil {
			return fmt.Errorf("unknown target binding %q", clause.TargetVar)
		}

		props := make(map[string]any, len(clause.Props))
		for key, expr := range clause.Props {
			value, err := expr.eval(row, params)
			if err != nil {
				return err
			}
			normalized, err := store.NormalizeValue(value)
			if err != nil {
				return err
			}
			props[key] = normalized
		}

		edge, err := tx.CreateEdge(sourceBinding.Node.ID, targetBinding.Node.ID, clause.EdgeType, CreateEdgeOptions{
			Properties: props,
		})
		if err != nil {
			return err
		}
		if clause.EdgeVar != "" {
			row.Bindings[clause.EdgeVar] = boundValue{Edge: tx.graph.Edges.Get(edge.ID)}
		}
	}
	return nil
}

func (clause *deleteClause) apply(tx *Tx, rows []queryRow) error {
	nodeIDs := map[uint64]struct{}{}
	edgeIDs := map[uint64]struct{}{}

	for _, row := range rows {
		for _, name := range clause.Vars {
			binding, ok := row.Bindings[name]
			if !ok {
				return fmt.Errorf("unknown binding %q", name)
			}
			switch {
			case binding.Edge != nil:
				edgeIDs[binding.Edge.ID] = struct{}{}
			case binding.Node != nil:
				nodeIDs[binding.Node.ID] = struct{}{}
			default:
				return fmt.Errorf("binding %q is neither node nor edge", name)
			}
		}
	}

	for edgeID := range edgeIDs {
		tx.deleteEdge(edgeID)
	}
	for nodeID := range nodeIDs {
		if err := tx.DeleteNode(nodeID); err != nil {
			return err
		}
	}
	return nil
}

func (clause *returnClause) render(rows []queryRow, budget *queryBudget) (QueryResult, error) {
	if clause.CountAlias != "" {
		count := int64(0)
		for _, row := range rows {
			if clause.CountVar == "*" {
				count++
				continue
			}
			binding, ok := row.Bindings[clause.CountVar]
			if !ok {
				return QueryResult{}, fmt.Errorf("unknown binding %q", clause.CountVar)
			}
			if binding.Node != nil || binding.Edge != nil || binding.HasValue && binding.Value != nil {
				count++
			}
		}
		return QueryResult{
			Columns: []string{clause.CountAlias},
			Rows: []map[string]any{
				{clause.CountAlias: count},
			},
		}, nil
	}

	result := QueryResult{Columns: make([]string, 0, len(clause.Projections))}
	for _, projection := range clause.Projections {
		result.Columns = append(result.Columns, projection.Alias)
	}

	for _, row := range rows {
		if err := budget.chargeResult(64 + uint64(len(clause.Projections))*32); err != nil {
			return QueryResult{}, err
		}
		resultRow := make(map[string]any, len(clause.Projections))
		for _, projection := range clause.Projections {
			switch projection.Kind {
			case projectionBindingID:
				binding, ok := row.Bindings[projection.Var]
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				value, ok := bindingID(binding)
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				resultRow[projection.Alias] = value
			case projectionProperty:
				binding, ok := row.Bindings[projection.Var]
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				value, exists := propertyFromBinding(binding, projection.Property)
				if !exists {
					resultRow[projection.Alias] = nil
					continue
				}
				if err := budget.chargeResult(queryValueBytes(value)); err != nil {
					return QueryResult{}, err
				}
				resultRow[projection.Alias] = store.CloneValue(value)
			case projectionValue:
				binding, ok := row.Bindings[projection.Var]
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				switch {
				case binding.Node != nil:
					if err := budget.chargeResult(queryPropertyBytes(binding.Node.Properties)); err != nil {
						return QueryResult{}, err
					}
					resultRow[projection.Alias] = publicNode(binding.Node)
				case binding.Edge != nil:
					if err := budget.chargeResult(queryPropertyBytes(binding.Edge.Properties)); err != nil {
						return QueryResult{}, err
					}
					resultRow[projection.Alias] = publicEdge(binding.Edge)
				case binding.HasValue:
					if err := budget.chargeResult(queryValueBytes(binding.Value)); err != nil {
						return QueryResult{}, err
					}
					resultRow[projection.Alias] = store.CloneValue(binding.Value)
				default:
					resultRow[projection.Alias] = nil
				}
			default:
				return QueryResult{}, fmt.Errorf("unsupported projection kind %q", projection.Kind)
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}
	return result, nil
}

func queryPropertyBytes(properties map[string]any) uint64 {
	bytes := uint64(len(properties)) * 32
	for key, value := range properties {
		bytes += uint64(len(key)) + queryValueBytes(value)
	}
	return bytes
}

func distinctResultRows(columns []string, rows []map[string]any, budget *queryBudget) ([]map[string]any, error) {
	if err := budget.chargeTemporary(uint64(len(rows)) * 32); err != nil {
		return nil, err
	}
	distinct := rows[:0]
	buckets := make(map[uint64][]int, len(rows))
	for _, row := range rows {
		if err := budget.check(uint64(len(columns)), len(distinct)); err != nil {
			return nil, err
		}
		key := fnv.New64a()
		for _, column := range columns {
			writeDistinctValueKey(key, row[column])
		}
		encoded := key.Sum64()
		duplicate := false
		for _, index := range buckets[encoded] {
			if err := budget.check(uint64(len(columns)), len(distinct)); err != nil {
				return nil, err
			}
			if resultRowsEqual(columns, row, distinct[index]) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		buckets[encoded] = append(buckets[encoded], len(distinct))
		distinct = append(distinct, row)
	}
	return distinct, nil
}

func writeDistinctValueKey(writer io.Writer, value any) {
	switch value := value.(type) {
	case nil:
		fmt.Fprint(writer, "nil;")
	case int64:
		fmt.Fprintf(writer, "number:%d;", value)
	case float64:
		if value >= -9223372036854775808 && value < 9223372036854775808 && integerEqualsFloat(int64(value), value) {
			fmt.Fprintf(writer, "number:%d;", int64(value))
			return
		}
		fmt.Fprintf(writer, "float:%x;", math.Float64bits(value))
	case []any:
		fmt.Fprintf(writer, "list:%d[", len(value))
		for _, item := range value {
			writeDistinctValueKey(writer, item)
		}
		fmt.Fprint(writer, "]")
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		fmt.Fprintf(writer, "map:%d{", len(keys))
		for _, key := range keys {
			fmt.Fprintf(writer, "%d:%s=", len(key), key)
			writeDistinctValueKey(writer, value[key])
		}
		fmt.Fprint(writer, "}")
	default:
		fmt.Fprintf(writer, "%T:%#v;", value, value)
	}
}

func resultRowsEqual(columns []string, left, right map[string]any) bool {
	for _, column := range columns {
		leftValue, rightValue := left[column], right[column]
		if !distinctValuesEqual(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func distinctValuesEqual(left, right any) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	switch left := left.(type) {
	case []any:
		right, ok := right.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for index := range left {
			if !distinctValuesEqual(left[index], right[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		right, ok := right.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, value := range left {
			rightValue, exists := right[key]
			if !exists || !distinctValuesEqual(value, rightValue) {
				return false
			}
		}
		return true
	default:
		return queryValuesEqual(left, right)
	}
}

func queryValueBytes(value any) uint64 {
	switch value := value.(type) {
	case nil, bool, int64, float64:
		return 8
	case string:
		return uint64(len(value))
	case []byte:
		return uint64(len(value))
	case []float32:
		return uint64(len(value)) * 4
	case []any:
		bytes := uint64(len(value)) * 16
		for _, item := range value {
			bytes += queryValueBytes(item)
		}
		return bytes
	case map[string]any:
		return queryPropertyBytes(value)
	default:
		return 16
	}
}

func (expr literalExpr) eval(_ queryRow, _ map[string]any) (any, error) {
	return store.CloneValue(expr.Value), nil
}

func (expr mapLiteralExpr) eval(row queryRow, params map[string]any) (any, error) {
	out := make(map[string]any, len(expr.Entries))
	for key, item := range expr.Entries {
		value, err := item.eval(row, params)
		if err != nil {
			return nil, err
		}
		normalized, err := store.NormalizeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = normalized
	}
	return out, nil
}

func (expr paramExpr) eval(_ queryRow, params map[string]any) (any, error) {
	value, ok := params[expr.Name]
	if !ok {
		return nil, fmt.Errorf("missing query parameter %q", expr.Name)
	}
	return store.NormalizeValue(value)
}

func (expr variableExpr) eval(row queryRow, _ map[string]any) (any, error) {
	value, ok := row.Bindings[expr.Name]
	if !ok {
		return nil, fmt.Errorf("unknown binding %q", expr.Name)
	}
	if value.HasValue {
		return store.CloneValue(value.Value), nil
	}
	return value, nil
}

func (expr propertyExpr) eval(row queryRow, _ map[string]any) (any, error) {
	binding, ok := row.Bindings[expr.Var]
	if !ok {
		return nil, fmt.Errorf("unknown binding %q", expr.Var)
	}
	value, _ := propertyFromBinding(binding, expr.Property)
	return store.CloneValue(value), nil
}

func parseValueExpr(text string) (valueExpr, error) {
	text = strings.TrimSpace(text)
	switch {
	case text == "":
		return nil, errors.New("value expression must be non-empty")
	case text == "null":
		return literalExpr{Value: nil}, nil
	case text == "true":
		return literalExpr{Value: true}, nil
	case text == "false":
		return literalExpr{Value: false}, nil
	case strings.HasPrefix(text, "$"):
		name := strings.TrimPrefix(text, "$")
		if !isQueryIdentifier(name) {
			return nil, fmt.Errorf("invalid parameter %q", text)
		}
		return paramExpr{Name: name}, nil
	case strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}"):
		entries, err := parsePropertyExprMap(strings.TrimSpace(text[1 : len(text)-1]))
		if err != nil {
			return nil, err
		}
		return mapLiteralExpr{Entries: entries}, nil
	case strings.HasPrefix(text, "\"") || strings.HasPrefix(text, "'"):
		unquoted, err := unquoteQueryString(text)
		if err != nil {
			return nil, err
		}
		return literalExpr{Value: unquoted}, nil
	default:
		if i, err := strconv.ParseInt(text, 10, 64); err == nil {
			return literalExpr{Value: i}, nil
		}
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			return literalExpr{Value: f}, nil
		}
		if strings.Contains(text, ".") {
			name, property, err := parsePropertyAccess(text)
			if err != nil {
				return nil, err
			}
			return propertyExpr{Var: name, Property: property}, nil
		}
		return variableExpr{Name: text}, nil
	}
}

func unquoteQueryString(text string) (string, error) {
	if strings.HasPrefix(text, "\"") {
		return strconv.Unquote(text)
	}
	if len(text) < 2 || text[len(text)-1] != '\'' {
		return "", fmt.Errorf("unterminated string literal %q", text)
	}
	remaining := text[1 : len(text)-1]
	var value strings.Builder
	for remaining != "" {
		char, _, tail, err := strconv.UnquoteChar(remaining, '\'')
		if err != nil {
			return "", err
		}
		value.WriteRune(char)
		remaining = tail
	}
	return value.String(), nil
}

func parsePropertyExprMap(text string) (map[string]valueExpr, error) {
	out := make(map[string]valueExpr)
	if strings.TrimSpace(text) == "" {
		return out, nil
	}
	for _, part := range splitTopLevel(text, ',') {
		key, rawValue, err := splitPropertyAssignment(part)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(rawValue)
		if err != nil {
			return nil, err
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate map key %q", key)
		}
		out[key] = expr
	}
	return out, nil
}

func splitPropertyAssignment(text string) (string, string, error) {
	key, value, ok := splitOperator(text, ":")
	if !ok {
		return "", "", fmt.Errorf("invalid property assignment %q", text)
	}
	key = strings.TrimSpace(key)
	if !isQueryIdentifier(key) {
		return "", "", fmt.Errorf("invalid property key %q", key)
	}
	return key, strings.TrimSpace(value), nil
}

func parsePropertyAccess(text string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(text), ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid property access %q", text)
	}
	variable := strings.TrimSpace(parts[0])
	property := strings.TrimSpace(parts[1])
	if !isQueryIdentifier(variable) || !isQueryIdentifier(property) {
		return "", "", fmt.Errorf("invalid property access %q", text)
	}
	return variable, property, nil
}

func parseBindingIDAccess(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "id(") || !strings.HasSuffix(text, ")") {
		return "", false
	}
	name := strings.TrimSpace(text[len("id(") : len(text)-1])
	if !isQueryIdentifier(name) {
		return "", false
	}
	return name, true
}

func isQueryIdentifier(value string) bool {
	for index, char := range []byte(value) {
		if char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || index > 0 && char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return value != ""
}

func splitOperator(text string, operator string) (string, string, bool) {
	index := findTopLevelToken(text, operator)
	if index < 0 {
		return "", "", false
	}
	return strings.TrimSpace(text[:index]), strings.TrimSpace(text[index+len(operator):]), true
}

func findTopLevelToken(text, token string) int {
	var quote byte
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	for index := 0; index < len(text); index++ {
		if scanQueryString(text, index, &quote) {
			continue
		}
		if braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 && strings.HasPrefix(text[index:], token) {
			return index
		}
		switch text[index] {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
	}
	return -1
}

func splitOnNextClause(input string, keywords ...string) (string, string, string) {
	var quote byte
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	for i := 0; i < len(input); i++ {
		if scanQueryString(input, i, &quote) {
			continue
		}
		switch input[i] {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		if braceDepth != 0 || bracketDepth != 0 || parenDepth != 0 {
			continue
		}
		for _, keyword := range keywords {
			if strings.HasPrefix(input[i:], keyword) {
				return strings.TrimSpace(input[:i]), keyword, strings.TrimSpace(input[i+len(keyword):])
			}
		}
	}
	return strings.TrimSpace(input), "", ""
}

func parsePaginationExpr(name, text string) (valueExpr, error) {
	expr, err := parseValueExpr(text)
	if err != nil {
		return nil, fmt.Errorf("invalid %s %q", name, text)
	}
	switch expr := expr.(type) {
	case paramExpr:
		return expr, nil
	case literalExpr:
		value, ok := normalizeInt64(expr.Value)
		if ok && value >= 0 && uint64(value) <= uint64(^uint(0)>>1) {
			return expr, nil
		}
	}
	return nil, fmt.Errorf("invalid %s %q", name, text)
}

func paginationValue(name string, expr valueExpr, params map[string]any) (int, error) {
	if expr == nil {
		return 0, nil
	}
	value, err := expr.eval(queryRow{}, params)
	if err != nil {
		return 0, err
	}
	integer, ok := normalizeInt64(value)
	if !ok || integer < 0 || uint64(integer) > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("%s requires a non-negative integer, got %v", name, value)
	}
	return int(integer), nil
}

func trimEnclosed(text string, open byte, close byte) (string, error) {
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != open || text[len(text)-1] != close {
		return "", fmt.Errorf("expected %c...%c, got %q", open, close, text)
	}
	if end := findMatchingBrace(text, 0, open, close); end != len(text)-1 {
		return "", fmt.Errorf("unexpected text after %c...%c in %q", open, close, text)
	}
	return strings.TrimSpace(text[1 : len(text)-1]), nil
}

func splitTopLevel(text string, separator rune) []string {
	parts := make([]string, 0)
	start := 0
	var quote byte
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0

	for i, r := range text {
		if scanQueryString(text, i, &quote) {
			continue
		}
		switch r {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}

		if r == separator && braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 {
			parts = append(parts, strings.TrimSpace(text[start:i]))
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(text[start:]))
	return parts
}

func splitTopLevelKeyword(text string, keyword string) []string {
	parts := make([]string, 0)
	start := 0
	var quote byte
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0

	for i := 0; i < len(text); i++ {
		if scanQueryString(text, i, &quote) {
			continue
		}
		switch text[i] {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}

		if braceDepth != 0 || bracketDepth != 0 || parenDepth != 0 {
			continue
		}
		if strings.HasPrefix(text[i:], keyword) {
			parts = append(parts, strings.TrimSpace(text[start:i]))
			start = i + len(keyword)
			i += len(keyword) - 1
		}
	}

	parts = append(parts, strings.TrimSpace(text[start:]))
	return parts
}

func findTopLevelRune(text string, target rune) int {
	var quote byte
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	for i, r := range text {
		if scanQueryString(text, i, &quote) {
			continue
		}
		if braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 && r == target {
			return i
		}
		switch r {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
	}
	return -1
}

func findMatchingBrace(text string, start int, open byte, close byte) int {
	depth := 0
	var quote byte
	for i := start; i < len(text); i++ {
		if scanQueryString(text, i, &quote) {
			continue
		}
		switch text[i] {
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func scanQueryString(text string, index int, quote *byte) bool {
	char := text[index]
	if *quote == 0 {
		if (char == '\'' || char == '"') && !isEscaped(text, index) {
			*quote = char
			return true
		}
		return false
	}
	if char == *quote && !isEscaped(text, index) {
		*quote = 0
	}
	return true
}

func bindingMatchesNode(row queryRow, name string, node *store.NodeRecord) bool {
	if name == "" {
		return true
	}
	binding, ok := row.Bindings[name]
	if !ok {
		return true
	}
	return binding.Node != nil && binding.Node.ID == node.ID
}

func propertyFromBinding(binding boundValue, property string) (any, bool) {
	switch {
	case binding.Node != nil:
		value, ok := binding.Node.Properties[property]
		return value, ok
	case binding.Edge != nil:
		value, ok := binding.Edge.Properties[property]
		return value, ok
	case binding.HasValue:
		value, ok := binding.Value.(map[string]any)
		if !ok {
			return nil, false
		}
		prop, ok := value[property]
		return prop, ok
	default:
		return nil, false
	}
}

func (row queryRow) clone() queryRow {
	bindings := make(map[string]boundValue, len(row.Bindings))
	for key, value := range row.Bindings {
		bindings[key] = value
	}
	return queryRow{Bindings: bindings, Order: row.Order}
}

func compareRowBindings(left queryRow, right queryRow) int {
	leftID := lowestBindingID(left.Bindings)
	rightID := lowestBindingID(right.Bindings)
	switch {
	case leftID < rightID:
		return -1
	case leftID > rightID:
		return 1
	default:
		return 0
	}
}

func lowestBindingID(bindings map[string]boundValue) uint64 {
	var lowest uint64
	for _, binding := range bindings {
		var candidate uint64
		switch {
		case binding.Node != nil:
			candidate = binding.Node.ID
		case binding.Edge != nil:
			candidate = binding.Edge.ID
		default:
			continue
		}
		if lowest == 0 || candidate < lowest {
			lowest = candidate
		}
	}
	return lowest
}

func bindingID(binding boundValue) (int64, bool) {
	switch {
	case binding.Node != nil:
		return int64(binding.Node.ID), true
	case binding.Edge != nil:
		return int64(binding.Edge.ID), true
	default:
		return 0, false
	}
}

func normalizeInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

func replacementPropertyMap(value any) (map[string]any, error) {
	props, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("property-map mutation requires map value, got %T", value)
	}
	out := make(map[string]any, len(props))
	for key, item := range props {
		if item == nil {
			continue
		}
		out[key] = item
	}
	return out, nil
}

func mergePropertyMap(value any) (map[string]any, error) {
	props, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("property-map mutation requires map value, got %T", value)
	}
	return props, nil
}

func mergeMutationProperties(dst map[string]any, src map[string]any) {
	for key, value := range src {
		if value == nil {
			delete(dst, key)
			continue
		}
		dst[key] = value
	}
}

func removeLabel(labels []string, target string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		if label != target {
			out = append(out, label)
		}
	}
	return out
}

func isEscaped(text string, index int) bool {
	backslashes := 0
	for index > backslashes && text[index-backslashes-1] == '\\' {
		backslashes++
	}
	return backslashes%2 == 1
}
