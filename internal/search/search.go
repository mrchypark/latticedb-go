package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"
)

var ErrTokenizationLimit = errors.New("tokenization logical byte limit exceeded")

func FirstVectorProperty(props map[string]any) ([]float32, bool) {
	if len(props) == 0 {
		return nil, false
	}
	var selected []float32
	selectedKey := ""
	for key, value := range props {
		if vector, ok := value.([]float32); ok && (selected == nil || key < selectedKey) {
			selected = vector
			selectedKey = key
		}
	}
	return selected, selected != nil
}

func VectorDistance(left []float32, right []float32) (float32, error) {
	squared, err := SquaredVectorDistance(left, right)
	if err != nil {
		return 0, err
	}
	return float32(math.Sqrt(squared)), nil
}

// SquaredVectorDistance avoids sqrt when callers only compare distances.
func SquaredVectorDistance(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("vector length mismatch: %d != %d", len(left), len(right))
	}
	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i+3 < len(left); i += 4 {
		diff := float64(left[i]) - float64(right[i])
		sum0 += diff * diff
		diff = float64(left[i+1]) - float64(right[i+1])
		sum1 += diff * diff
		diff = float64(left[i+2]) - float64(right[i+2])
		sum2 += diff * diff
		diff = float64(left[i+3]) - float64(right[i+3])
		sum3 += diff * diff
	}
	total := (sum0 + sum1) + (sum2 + sum3)
	for ; i < len(left); i++ {
		diff := float64(left[i]) - float64(right[i])
		total += diff * diff
	}
	if total > float64(math.MaxFloat32)*float64(math.MaxFloat32) {
		return 0, errors.New("vector distance exceeds float32 range")
	}
	return total, nil
}

func VectorDistanceContext(ctx context.Context, left []float32, right []float32) (float32, error) {
	squared, err := SquaredVectorDistanceContext(ctx, left, right)
	if err != nil {
		return 0, err
	}
	return float32(math.Sqrt(squared)), nil
}

// SquaredVectorDistanceContext is the cancellable comparison-only variant.
func SquaredVectorDistanceContext(ctx context.Context, left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("vector length mismatch: %d != %d", len(left), len(right))
	}
	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i+3 < len(left); i += 4 {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		diff := float64(left[i]) - float64(right[i])
		sum0 += diff * diff
		diff = float64(left[i+1]) - float64(right[i+1])
		sum1 += diff * diff
		diff = float64(left[i+2]) - float64(right[i+2])
		sum2 += diff * diff
		diff = float64(left[i+3]) - float64(right[i+3])
		sum3 += diff * diff
	}
	total := (sum0 + sum1) + (sum2 + sum3)
	for ; i < len(left); i++ {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		diff := float64(left[i]) - float64(right[i])
		total += diff * diff
	}
	if total > float64(math.MaxFloat32)*float64(math.MaxFloat32) {
		return 0, errors.New("vector distance exceeds float32 range")
	}
	return total, nil
}

func Tokenize(text string) []string {
	parts := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func TokenizeContext(ctx context.Context, text string) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	tokens := make([]string, 0)
	var token strings.Builder
	for offset, value := range text {
		if offset&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if unicode.IsLetter(value) || unicode.IsDigit(value) {
			token.WriteRune(unicode.ToLower(value))
			continue
		}
		if token.Len() != 0 {
			tokens = append(tokens, token.String())
			token.Reset()
		}
	}
	if token.Len() != 0 {
		tokens = append(tokens, token.String())
	}
	return tokens, ctx.Err()
}

// TokenizeContextWithLimit counts token headers and lowercase storage before
// allocating the token slice or strings.
func TokenizeContextWithLimit(ctx context.Context, text string, maxLogicalBytes uint64) ([]string, error) {
	logicalBytes, err := tokenizationLogicalBytes(ctx, text)
	if err != nil {
		return nil, err
	}
	if logicalBytes > maxLogicalBytes {
		return nil, ErrTokenizationLimit
	}
	return TokenizeContext(ctx, text)
}

func tokenizationLogicalBytes(ctx context.Context, text string) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var tokens, lowercaseBytes uint64
	inToken := false
	for offset, value := range text {
		if offset&255 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		if unicode.IsLetter(value) || unicode.IsDigit(value) {
			if !inToken {
				tokens++
				inToken = true
			}
			lowercaseBytes = searchAddSaturated(lowercaseBytes, uint64(utf8.RuneLen(unicode.ToLower(value))))
		} else {
			inToken = false
		}
	}
	if tokens == 0 {
		return 0, ctx.Err()
	}
	bytes := searchAddSaturated(256, searchMultiplySaturated(tokens, 64))
	bytes = searchAddSaturated(bytes, searchMultiplySaturated(lowercaseBytes, 4))
	return bytes, ctx.Err()
}

func searchAddSaturated(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}

func searchMultiplySaturated(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}

func FTSScore(text string, terms []string) float32 {
	return FTSScoreWithOptions(text, terms, 0, 0)
}

func FTSScoreWithOptions(text string, terms []string, maxDistance uint32, minTermLength uint32) float32 {
	return FTSScoreTokensWithOptions(Tokenize(text), terms, maxDistance, minTermLength)
}

func FTSScoreTokensWithOptions(tokens []string, terms []string, maxDistance uint32, minTermLength uint32) float32 {
	if len(terms) == 0 {
		return 0
	}
	if len(tokens) == 0 {
		return 0
	}
	if maxDistance == 0 {
		score := 0
		for _, term := range terms {
			term = strings.ToLower(term)
			for _, token := range tokens {
				if token == term {
					score++
				}
			}
		}
		return float32(score)
	}
	freq := map[string]int{}
	for _, token := range tokens {
		freq[token]++
	}
	score := float32(0)
	for _, term := range terms {
		normalized := strings.ToLower(term)
		best := freq[normalized]
		if maxDistance > 0 && uint32(len([]rune(normalized))) >= minTermLength {
			for token, count := range freq {
				if token == normalized {
					continue
				}
				if levenshteinDistance(normalized, token) <= int(maxDistance) && count > best {
					best = count
				}
			}
		}
		score += float32(best)
	}
	return score
}

func FuzzyTokenMatch(term, token string, maxDistance, minTermLength uint32) bool {
	term = strings.ToLower(term)
	if token == term {
		return true
	}
	return maxDistance > 0 && uint32(len([]rune(term))) >= minTermLength && levenshteinDistance(term, token) <= int(maxDistance)
}

func levenshteinDistance(left string, right string) int {
	leftRunes := []rune(left)
	rightRunes := []rune(right)
	if len(leftRunes) == 0 {
		return len(rightRunes)
	}
	if len(rightRunes) == 0 {
		return len(leftRunes)
	}

	prev := make([]int, len(rightRunes)+1)
	curr := make([]int, len(rightRunes)+1)
	for j := range prev {
		prev[j] = j
	}
	for i, leftRune := range leftRunes {
		curr[0] = i + 1
		for j, rightRune := range rightRunes {
			cost := 0
			if leftRune != rightRune {
				cost = 1
			}
			deletion := prev[j+1] + 1
			insertion := curr[j] + 1
			substitution := prev[j] + cost
			curr[j+1] = minInt(deletion, insertion, substitution)
		}
		prev, curr = curr, prev
	}
	return prev[len(rightRunes)]
}

func minInt(values ...int) int {
	best := values[0]
	for _, value := range values[1:] {
		if value < best {
			best = value
		}
	}
	return best
}
