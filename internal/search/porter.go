package search

import "context"

// StemEnglishPorter implements the English Porter stemming algorithm described
// by Martin Porter (1980), https://snowballstem.org/algorithms/porter/stemmer.html.
// It is an independent implementation of the published rules; non-ASCII
// tokens are left unchanged.
func StemEnglishPorter(token string) string {
	stemmed, _ := StemEnglishPorterContext(context.Background(), token)
	return stemmed
}

func StemEnglishPorterContext(ctx context.Context, token string) (string, error) {
	ascii, err := porterASCIIWordContext(ctx, token)
	if err != nil {
		return "", err
	}
	if len(token) < 3 || !ascii {
		return token, ctx.Err()
	}
	w := []byte(token)
	w = porterStep1a(w)
	flags, err := porterFlags(ctx, w)
	if err != nil {
		return "", err
	}
	w = porterStep1b(w, flags)
	flags, err = porterFlags(ctx, w)
	if err != nil {
		return "", err
	}
	w = porterStep1c(w, flags)
	flags, err = porterFlags(ctx, w)
	if err != nil {
		return "", err
	}
	w = porterReplace(w, flags, porterStep2)
	flags, err = porterFlags(ctx, w)
	if err != nil {
		return "", err
	}
	w = porterReplace(w, flags, porterStep3)
	flags, err = porterFlags(ctx, w)
	if err != nil {
		return "", err
	}
	w = porterStep4Apply(w, flags)
	flags, err = porterFlags(ctx, w)
	if err != nil {
		return "", err
	}
	w = porterStep5(w, flags)
	return string(w), ctx.Err()
}

func porterASCIIWordContext(ctx context.Context, word string) (bool, error) {
	for i := 0; i < len(word); i++ {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		if word[i] < 'a' || word[i] > 'z' {
			return false, nil
		}
	}
	return true, ctx.Err()
}

func porterFlags(ctx context.Context, word []byte) ([]bool, error) {
	flags := make([]bool, len(word))
	for i, letter := range word {
		if i&63 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		switch letter {
		case 'a', 'e', 'i', 'o', 'u':
			flags[i] = false
		case 'y':
			flags[i] = i == 0 || !flags[i-1]
		default:
			flags[i] = true
		}
	}
	return flags, ctx.Err()
}

func porterMeasure(flags []bool) int {
	measure := 0
	vowel := false
	for _, consonant := range flags {
		if consonant {
			if vowel {
				measure++
				vowel = false
			}
		} else {
			vowel = true
		}
	}
	return measure
}

func porterHasVowel(flags []bool) bool {
	for _, consonant := range flags {
		if !consonant {
			return true
		}
	}
	return false
}

func porterCVC(word []byte, flags []bool) bool {
	if len(word) < 3 {
		return false
	}
	i := len(word) - 1
	return flags[i] && !flags[i-1] && flags[i-2] && word[i] != 'w' && word[i] != 'x' && word[i] != 'y'
}

func porterSuffix(word []byte, suffix string) ([]byte, bool) {
	if len(word) < len(suffix) || string(word[len(word)-len(suffix):]) != suffix {
		return word, false
	}
	return word[:len(word)-len(suffix)], true
}

func porterStep1a(word []byte) []byte {
	for _, rule := range []struct{ suffix, replacement string }{{"sses", "ss"}, {"ies", "i"}, {"ss", "ss"}, {"s", ""}} {
		if stem, ok := porterSuffix(word, rule.suffix); ok {
			return append(stem, rule.replacement...)
		}
	}
	return word
}

func porterStep1b(word []byte, flags []bool) []byte {
	if stem, ok := porterSuffix(word, "eed"); ok {
		if porterMeasure(flags[:len(stem)]) > 0 {
			return append(stem, "ee"...)
		}
		return word
	}
	for _, suffix := range []string{"ed", "ing"} {
		if stem, ok := porterSuffix(word, suffix); ok && porterHasVowel(flags[:len(stem)]) {
			word = stem
			switch {
			case hasSuffix(word, "at"), hasSuffix(word, "bl"), hasSuffix(word, "iz"):
				return append(word, 'e')
			case len(word) >= 2 && word[len(word)-1] == word[len(word)-2] && flags[len(word)-1] && word[len(word)-1] != 'l' && word[len(word)-1] != 's' && word[len(word)-1] != 'z':
				return word[:len(word)-1]
			case porterMeasure(flags[:len(word)]) == 1 && porterCVC(word, flags[:len(word)]):
				return append(word, 'e')
			default:
				return word
			}
		}
	}
	return word
}

func porterStep1c(word []byte, flags []bool) []byte {
	if len(word) > 1 && word[len(word)-1] == 'y' && porterHasVowel(flags[:len(word)-1]) {
		word[len(word)-1] = 'i'
	}
	return word
}

type porterRule struct{ suffix, replacement string }

var porterStep2 = []porterRule{
	{"ational", "ate"}, {"tional", "tion"}, {"enci", "ence"}, {"anci", "ance"}, {"izer", "ize"}, {"abli", "able"}, {"alli", "al"}, {"entli", "ent"}, {"eli", "e"}, {"ousli", "ous"}, {"ization", "ize"}, {"ation", "ate"}, {"ator", "ate"}, {"alism", "al"}, {"iveness", "ive"}, {"fulness", "ful"}, {"ousness", "ous"}, {"aliti", "al"}, {"iviti", "ive"}, {"biliti", "ble"},
}

var porterStep3 = []porterRule{
	{"icate", "ic"}, {"ative", ""}, {"alize", "al"}, {"iciti", "ic"}, {"ical", "ic"}, {"ful", ""}, {"ness", ""},
}

var porterStep4 = []string{"al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ion", "ou", "ism", "ate", "iti", "ous", "ive", "ize"}

func porterReplace(word []byte, flags []bool, rules []porterRule) []byte {
	for _, rule := range rules {
		if stem, ok := porterSuffix(word, rule.suffix); ok {
			if porterMeasure(flags[:len(stem)]) > 0 {
				return append(stem, rule.replacement...)
			}
			return word
		}
	}
	return word
}

func porterStep4Apply(word []byte, flags []bool) []byte {
	for _, suffix := range porterStep4 {
		if stem, ok := porterSuffix(word, suffix); ok {
			if suffix == "ion" && (len(stem) == 0 || (stem[len(stem)-1] != 's' && stem[len(stem)-1] != 't')) {
				return word
			}
			if porterMeasure(flags[:len(stem)]) > 1 {
				return stem
			}
			return word
		}
	}
	return word
}

func porterStep5(word []byte, flags []bool) []byte {
	if stem, ok := porterSuffix(word, "e"); ok {
		measure := porterMeasure(flags[:len(stem)])
		if measure > 1 || (measure == 1 && !porterCVC(stem, flags[:len(stem)])) {
			word = stem
		}
	}
	if len(word) >= 2 && hasSuffix(word, "ll") && porterMeasure(flags[:len(word)]) > 1 {
		word = word[:len(word)-1]
	}
	return word
}

func hasSuffix(word []byte, suffix string) bool {
	_, ok := porterSuffix(word, suffix)
	return ok
}
