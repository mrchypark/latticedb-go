package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type queryUnionPlan struct {
	branches []*queryPlan
	all      bool
}

// trimQueryTerminator accepts at most one unquoted, terminal semicolon.
func trimQueryTerminator(query string) (string, error) {
	if err := validateQueryQuotes(query); err != nil {
		return "", err
	}
	query = strings.TrimSpace(query)
	var quote byte
	for index := 0; index < len(query); index++ {
		if scanQueryString(query, index, &quote) {
			continue
		}
		if query[index] != ';' {
			continue
		}
		if index != len(query)-1 {
			return "", errors.New("only one terminal semicolon is allowed")
		}
		return strings.TrimSpace(query[:index]), nil
	}
	return query, nil
}

type queryUnionSeparator struct {
	index int
	end   int
	all   bool
}

// splitQueryUnion finds unquoted UNION operators at the statement's top level.
func splitQueryUnion(query string) ([]string, bool, bool, error) {
	query = normalizeQueryText(query)
	var separators []queryUnionSeparator
	var quote byte
	braceDepth, bracketDepth, parenDepth := 0, 0, 0
	for index := 0; index < len(query); index++ {
		if scanQueryString(query, index, &quote) {
			continue
		}
		if braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 &&
			(index == 0 || query[index-1] == ' ') && strings.HasPrefix(query[index:], "UNION") {
			end := index + len("UNION")
			if end == len(query) || query[end] == ' ' {
				prefix := strings.TrimSpace(query[:index])
				prior := prefix
				if last := strings.LastIndexByte(prefix, ' '); last >= 0 {
					prior = prefix[last+1:]
				}
				switch prior {
				case "AS", "RETURN", "WITH", "DISTINCT":
					continue
				}
				separator := queryUnionSeparator{index: index, end: end}
				cursor := end
				if cursor < len(query) && query[cursor] == ' ' {
					cursor++
				}
				if hasQueryWord(query, cursor, "ALL") {
					separator.all = true
					separator.end = cursor + len("ALL")
				} else if hasQueryWord(query, cursor, "DISTINCT") {
					return nil, false, false, errors.New("UNION DISTINCT is unsupported")
				}
				// UNION may also be a binding or alias. Only a complete
				// branch starter after the token makes it a separator.
				next := strings.TrimSpace(query[separator.end:])
				switch {
				case strings.HasPrefix(next, "MATCH "), strings.HasPrefix(next, "OPTIONAL MATCH "),
					strings.HasPrefix(next, "RETURN "), strings.HasPrefix(next, "WITH "),
					strings.HasPrefix(next, "UNWIND "), strings.HasPrefix(next, "CREATE "), strings.HasPrefix(next, "MERGE "):
				default:
					continue
				}
				separators = append(separators, separator)
				index = separator.end - 1
				continue
			}
		}
		switch query[index] {
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
	if len(separators) == 0 {
		return []string{query}, false, false, nil
	}

	all := separators[0].all
	branches := make([]string, 0, len(separators)+1)
	start := 0
	for _, separator := range separators {
		if separator.all != all {
			return nil, false, false, errors.New("UNION and UNION ALL cannot be mixed")
		}
		branch := strings.TrimSpace(query[start:separator.index])
		if branch == "" {
			return nil, false, false, errors.New("UNION branch must not be empty")
		}
		branches = append(branches, branch)
		start = separator.end
	}
	last := strings.TrimSpace(query[start:])
	if last == "" {
		return nil, false, false, errors.New("UNION branch must not be empty")
	}
	branches = append(branches, last)
	return branches, all, true, nil
}

func hasQueryWord(text string, index int, word string) bool {
	if index < 0 || !strings.HasPrefix(text[index:], word) {
		return false
	}
	end := index + len(word)
	return end == len(text) || text[end] == ' '
}

func parseQueryUnion(branchTexts []string, all bool) (*queryPlan, error) {
	branches := make([]*queryPlan, 0, len(branchTexts))
	for _, text := range branchTexts {
		branch, err := parseQueryText(text)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}

	var columns []string
	for _, branch := range branches {
		if branch.mutates() {
			return nil, errors.New("UNION branches must be read-only")
		}
		if err := validateQueryPlan(branch); err != nil {
			return nil, err
		}
		branchColumns, err := queryUnionColumns(branch)
		if err != nil {
			return nil, err
		}
		if columns == nil {
			columns = branchColumns
			continue
		}
		if !slices.Equal(columns, branchColumns) {
			return nil, fmt.Errorf("UNION branches must return identical columns: %v and %v", columns, branchColumns)
		}
	}
	return &queryPlan{union: &queryUnionPlan{branches: branches, all: all}}, nil
}

func queryUnionColumns(plan *queryPlan) ([]string, error) {
	for plan.next != nil {
		plan = plan.next
	}
	if plan.returnClause == nil {
		return nil, errors.New("UNION branch must end with an explicit RETURN")
	}
	if plan.returnClause.CountAlias != "" {
		return []string{plan.returnClause.CountAlias}, nil
	}
	columns := make([]string, 0, len(plan.returnClause.Projections))
	for _, projection := range plan.returnClause.Projections {
		columns = append(columns, projection.Alias)
	}
	return columns, nil
}

func (plan *queryUnionPlan) execute(tx *Tx, params map[string]any, budget *queryBudget) (QueryResult, error) {
	columns, err := queryUnionColumns(plan.branches[0])
	if err != nil {
		return QueryResult{}, err
	}
	var rows []map[string]any
	var bufferBytes uint64
	defer func() { budget.releaseTemporary(bufferBytes) }()
	total := 0
	for _, branch := range plan.branches {
		if err := budget.check(0, 0); err != nil {
			return QueryResult{}, err
		}
		result, err := branch.executeWithInput(tx, params, budget, nil)
		if err != nil {
			return QueryResult{}, err
		}
		if err := budget.checkRows(len(result.Rows)); err != nil {
			return QueryResult{}, err
		}
		for _, row := range result.Rows {
			total++
			if err := budget.checkRows(total); err != nil {
				return QueryResult{}, err
			}
			if len(rows) == cap(rows) {
				capacity := 8
				if len(rows) != 0 {
					capacity = len(rows) * 2
				}
				reserved := uint64(capacity) * 8
				if err := budget.chargeTemporary(reserved); err != nil {
					return QueryResult{}, err
				}
				grown := make([]map[string]any, len(rows), capacity)
				copy(grown, rows)
				rows = grown
				budget.releaseTemporary(bufferBytes)
				bufferBytes = reserved
			}
			rows = append(rows, row)
		}
	}
	if err := budget.check(0, 0); err != nil {
		return QueryResult{}, err
	}
	if !plan.all {
		collected := rows
		rows, err = distinctResultRows(columns, rows, budget)
		if err != nil {
			return QueryResult{}, err
		}
		clear(collected[len(rows):])
	}
	// The output slice is retained by the caller. Its existing charge now
	// belongs to the result, so do not release it in the deferred cleanup.
	bufferBytes = 0
	return QueryResult{Columns: columns, Rows: rows}, nil
}
