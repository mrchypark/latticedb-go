package engine

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

type arithmeticExpr struct {
	Op          byte
	Left, Right valueExpr
}

func (expr arithmeticExpr) eval(row queryRow, params map[string]any, budget *queryBudget) (any, error) {
	if budget != nil {
		if err := budget.check(1, 0); err != nil {
			return nil, err
		}
	}
	if expr.Left == nil {
		value, err := expr.Right.eval(row, params, budget)
		if err != nil {
			return nil, err
		}
		integer, floating, isFloat, err := arithmeticValue(expr.Op, value)
		if err != nil {
			return nil, err
		}
		if expr.Op == '+' {
			if isFloat {
				return floating, nil
			}
			return integer, nil
		}
		if isFloat {
			return -floating, nil
		}
		if integer == math.MinInt64 {
			return nil, fmt.Errorf("integer arithmetic overflow")
		}
		return -integer, nil
	}
	left, err := expr.Left.eval(row, params, budget)
	if err != nil {
		return nil, err
	}
	right, err := expr.Right.eval(row, params, budget)
	if err != nil {
		return nil, err
	}
	return evalArithmetic(expr.Op, left, right, budget)
}

func arithmeticValue(op byte, value any) (int64, float64, bool, error) {
	switch value := value.(type) {
	case int64:
		return value, 0, false, nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, 0, false, fmt.Errorf("arithmetic %c requires a finite number", op)
		}
		return 0, value, true, nil
	default:
		return 0, 0, false, fmt.Errorf("arithmetic %c requires numbers, got %s", op, valueKind(value))
	}
}

func evalArithmetic(op byte, left, right any, budget *queryBudget) (any, error) {
	leftInt, leftFloat, leftIsFloat, err := arithmeticValue(op, left)
	if err != nil {
		return nil, err
	}
	rightInt, rightFloat, rightIsFloat, err := arithmeticValue(op, right)
	if err != nil {
		return nil, err
	}
	if leftIsFloat || rightIsFloat || op == '/' || op == '^' && rightInt < 0 {
		if !leftIsFloat {
			leftFloat = float64(leftInt)
		}
		if !rightIsFloat {
			rightFloat = float64(rightInt)
		}
		var value float64
		switch op {
		case '+':
			value = leftFloat + rightFloat
		case '-':
			value = leftFloat - rightFloat
		case '*':
			value = leftFloat * rightFloat
		case '/':
			if rightFloat == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			value = leftFloat / rightFloat
		case '%':
			if rightFloat == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			value = math.Mod(leftFloat, rightFloat)
		case '^':
			value = math.Pow(leftFloat, rightFloat)
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("arithmetic result is not finite")
		}
		return value, nil
	}
	switch op {
	case '+':
		if rightInt > 0 && leftInt > math.MaxInt64-rightInt || rightInt < 0 && leftInt < math.MinInt64-rightInt {
			return nil, fmt.Errorf("integer arithmetic overflow")
		}
		return leftInt + rightInt, nil
	case '-':
		if rightInt < 0 && leftInt > math.MaxInt64+rightInt || rightInt > 0 && leftInt < math.MinInt64+rightInt {
			return nil, fmt.Errorf("integer arithmetic overflow")
		}
		return leftInt - rightInt, nil
	case '*':
		return checkedIntegerProduct(leftInt, rightInt)
	case '%':
		if rightInt == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		if leftInt == math.MinInt64 && rightInt == -1 {
			return int64(0), nil
		}
		return leftInt % rightInt, nil
	case '^':
		return integerPower(leftInt, rightInt, budget)
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator %q", op)
	}
}

func checkedIntegerProduct(left, right int64) (int64, error) {
	if left == math.MinInt64 && right == -1 || right == math.MinInt64 && left == -1 {
		return 0, fmt.Errorf("integer arithmetic overflow")
	}
	value := left * right
	if left != 0 && value/left != right {
		return 0, fmt.Errorf("integer arithmetic overflow")
	}
	return value, nil
}

func integerPower(base, exponent int64, budget *queryBudget) (any, error) {
	if exponent < 0 {
		value := math.Pow(float64(base), float64(exponent))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("arithmetic result is not finite")
		}
		return value, nil
	}
	value := int64(1)
	for exponent != 0 {
		if exponent&1 != 0 {
			if budget != nil {
				if err := budget.check(1, 0); err != nil {
					return nil, err
				}
			}
			var err error
			value, err = checkedIntegerProduct(value, base)
			if err != nil {
				return nil, err
			}
		}
		exponent >>= 1
		if exponent != 0 {
			if budget != nil {
				if err := budget.check(1, 0); err != nil {
					return nil, err
				}
			}
			var err error
			base, err = checkedIntegerProduct(base, base)
			if err != nil {
				return nil, err
			}
		}
	}
	return value, nil
}

type arithmeticParser struct {
	text string
	pos  int
	used bool
}

func parseArithmeticExpr(text string) (valueExpr, bool, error) {
	text = strings.TrimSpace(text)
	if !hasArithmeticSyntax(text) {
		return nil, false, nil
	}
	parser := arithmeticParser{text: text}
	expr, err := parser.sum()
	if err != nil {
		return nil, false, err
	}
	parser.space()
	if parser.pos != len(parser.text) {
		return nil, false, fmt.Errorf("invalid arithmetic expression %q", text)
	}
	return expr, parser.used, nil
}

func hasArithmeticSyntax(text string) bool {
	if strings.HasPrefix(text, "(") {
		return true
	}
	var quote byte
	depth := 0
	for index := 0; index < len(text); index++ {
		if scanQueryString(text, index, &quote) {
			continue
		}
		switch text[index] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case '+', '-', '*', '/', '%', '^':
			if depth == 0 && !arithmeticExponentSign(text, 0, index) {
				return true
			}
		}
	}
	return false
}

func (parser *arithmeticParser) sum() (valueExpr, error) {
	left, err := parser.product()
	if err != nil {
		return nil, err
	}
	for {
		parser.space()
		if parser.pos == len(parser.text) || parser.text[parser.pos] != '+' && parser.text[parser.pos] != '-' {
			return left, nil
		}
		op := parser.text[parser.pos]
		parser.pos++
		parser.used = true
		right, err := parser.product()
		if err != nil {
			return nil, err
		}
		left = arithmeticExpr{Op: op, Left: left, Right: right}
	}
}

func (parser *arithmeticParser) product() (valueExpr, error) {
	left, err := parser.unary()
	if err != nil {
		return nil, err
	}
	for {
		parser.space()
		if parser.pos == len(parser.text) || !strings.ContainsRune("*/%", rune(parser.text[parser.pos])) {
			return left, nil
		}
		op := parser.text[parser.pos]
		parser.pos++
		parser.used = true
		right, err := parser.unary()
		if err != nil {
			return nil, err
		}
		left = arithmeticExpr{Op: op, Left: left, Right: right}
	}
}

func (parser *arithmeticParser) unary() (valueExpr, error) {
	parser.space()
	if parser.pos < len(parser.text) && (parser.text[parser.pos] == '+' || parser.text[parser.pos] == '-') {
		op := parser.text[parser.pos]
		parser.pos++
		parser.used = true
		if op == '-' {
			parser.space()
			const minInt64Magnitude = "9223372036854775808"
			if strings.HasPrefix(parser.text[parser.pos:], minInt64Magnitude) {
				end := parser.pos + len(minInt64Magnitude)
				if end == len(parser.text) || isQuerySpace(parser.text[end]) || strings.ContainsRune("+-*/%)", rune(parser.text[end])) {
					parser.pos = end
					return literalExpr{Value: int64(math.MinInt64)}, nil
				}
			}
		}
		right, err := parser.unary()
		if err != nil {
			return nil, err
		}
		return arithmeticExpr{Op: op, Right: right}, nil
	}
	return parser.power()
}

func (parser *arithmeticParser) power() (valueExpr, error) {
	left, err := parser.atom()
	if err != nil {
		return nil, err
	}
	parser.space()
	if parser.pos == len(parser.text) || parser.text[parser.pos] != '^' {
		return left, nil
	}
	parser.pos++
	parser.used = true
	right, err := parser.unary()
	if err != nil {
		return nil, err
	}
	return arithmeticExpr{Op: '^', Left: left, Right: right}, nil
}

func (parser *arithmeticParser) atom() (valueExpr, error) {
	parser.space()
	if parser.pos == len(parser.text) {
		return nil, errors.New("arithmetic operand must be non-empty")
	}
	if parser.text[parser.pos] == '(' {
		parser.pos++
		parser.used = true
		expr, err := parser.sum()
		if err != nil {
			return nil, err
		}
		parser.space()
		if parser.pos == len(parser.text) || parser.text[parser.pos] != ')' {
			return nil, errors.New("unclosed arithmetic parenthesis")
		}
		parser.pos++
		return expr, nil
	}
	start := parser.pos
	var quote byte
	depth := 0
	for parser.pos < len(parser.text) {
		index := parser.pos
		if scanQueryString(parser.text, index, &quote) {
			parser.pos++
			continue
		}
		char := parser.text[index]
		if depth == 0 {
			if isQuerySpace(char) || char == ')' || strings.ContainsRune("+-*/%^", rune(char)) && !arithmeticExponentSign(parser.text, start, index) {
				break
			}
		}
		switch char {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		}
		parser.pos++
	}
	if start == parser.pos {
		return nil, fmt.Errorf("invalid arithmetic operand %q", parser.text[start:])
	}
	return parseValueExpr(parser.text[start:parser.pos])
}

func arithmeticExponentSign(text string, start, index int) bool {
	if index <= start+1 || (text[index] != '+' && text[index] != '-') || (text[index-1] != 'e' && text[index-1] != 'E') {
		return false
	}
	digits, dots := 0, 0
	for _, char := range text[start : index-1] {
		switch {
		case char >= '0' && char <= '9':
			digits++
		case char == '.':
			dots++
			if dots > 1 {
				return false
			}
		default:
			return false
		}
	}
	return digits != 0
}

func (parser *arithmeticParser) space() {
	for parser.pos < len(parser.text) && isQuerySpace(parser.text[parser.pos]) {
		parser.pos++
	}
}
