package runeHelper

func Split(line string, delimiter rune, expectedNumOfTokens int) []string {
	if expectedNumOfTokens < 0 {
		expectedNumOfTokens = 0
	}
	tokens := make([]string, 0, expectedNumOfTokens)
	lineRunes := []rune(line)
	start := 0
	for end, r := range lineRunes {
		if r == delimiter {
			tokens = append(tokens, string(lineRunes[start:end]))
			start = end + 1
		}
	}
	tokens = append(tokens, string(lineRunes[start:]))
	return tokens
}

func RemoveSuffix(line string, suffix rune) string {
	lineRunes := []rune(line)
	if len(lineRunes) > 0 && lineRunes[len(lineRunes)-1] == suffix {
		return string(lineRunes[0 : len(lineRunes)-1])
	} else {
		return line
	}
}
