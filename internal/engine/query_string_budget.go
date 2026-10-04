package engine

import "strings"

// String work is charged per 64 bytes/comparisons. Native operations are
// limited to 4 KiB; longer scans check cancellation at that same interval.
type queryStringScan struct {
	budget    *queryBudget
	remaining int
}

func (scan *queryStringScan) step() error {
	if scan.budget == nil {
		return nil
	}
	if scan.remaining == 0 {
		if err := scan.budget.check(queryOrderComparisonChunk/64, 0); err != nil {
			return err
		}
		scan.remaining = queryOrderComparisonChunk
	}
	scan.remaining--
	return nil
}

// visit receives non-overlapping byte offsets. Returning false stops the scan.
// KMP keeps long needles linear without a large uninterruptible native call.
func visitQueryStringMatches(text, needle string, budget *queryBudget, visit func(int) (bool, error)) error {
	if len(needle) == 0 || len(needle) > len(text) {
		return nil
	}
	if budget == nil || len(text) <= queryOrderComparisonChunk {
		if budget != nil {
			if err := budget.check(uint64((len(text)+63)/64), 0); err != nil {
				return err
			}
		}
		for start := 0; start <= len(text)-len(needle); {
			index := strings.Index(text[start:], needle)
			if index < 0 {
				break
			}
			index += start
			more, err := visit(index)
			if err != nil || !more {
				return err
			}
			start = index + len(needle)
		}
		return nil
	}
	bytes := uint64(len(needle)) * 8
	if err := budget.chargeTemporary(bytes); err != nil {
		return err
	}
	defer budget.releaseTemporary(bytes)
	prefix := make([]int, len(needle))
	scan := queryStringScan{budget: budget}
	for i, j := 1, 0; i < len(needle); i++ {
		if err := scan.step(); err != nil {
			return err
		}
		for j > 0 && needle[i] != needle[j] {
			if err := scan.step(); err != nil {
				return err
			}
			j = prefix[j-1]
		}
		if needle[i] == needle[j] {
			j++
		}
		prefix[i] = j
	}
	for i, j := 0, 0; i < len(text); i++ {
		if err := scan.step(); err != nil {
			return err
		}
		for j > 0 && text[i] != needle[j] {
			if err := scan.step(); err != nil {
				return err
			}
			j = prefix[j-1]
		}
		if text[i] == needle[j] {
			j++
		}
		if j == len(needle) {
			more, err := visit(i + 1 - j)
			if err != nil || !more {
				return err
			}
			j = 0
		}
	}
	return nil
}

// The scan is shared across output fragments so short replacements do not
// restart work rounding for each match.
func writeQueryString(out *strings.Builder, text string, scan *queryStringScan) error {
	if scan.budget == nil {
		out.WriteString(text)
		return nil
	}
	for len(text) != 0 {
		if err := scan.step(); err != nil {
			return err
		}
		n := min(len(text), scan.remaining+1)
		out.WriteString(text[:n])
		scan.remaining -= n - 1
		text = text[n:]
	}
	return nil
}

func trimQueryString(text string, budget *queryBudget) (string, error) {
	if budget == nil || len(text) <= queryOrderComparisonChunk {
		if budget != nil {
			if err := budget.check(uint64((len(text)+63)/64), 0); err != nil {
				return "", err
			}
		}
		return strings.Trim(text, " \t\n\r"), nil
	}
	scan := queryStringScan{budget: budget}
	isSpace := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	start, end := 0, len(text)
	for start < end {
		if err := scan.step(); err != nil {
			return "", err
		}
		if !isSpace(text[start]) {
			break
		}
		start++
	}
	for end > start {
		if err := scan.step(); err != nil {
			return "", err
		}
		if !isSpace(text[end-1]) {
			break
		}
		end--
	}
	return text[start:end], nil
}
