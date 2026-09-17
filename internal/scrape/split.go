package scrape

import (
	"strconv"
	"strings"
)

// splitComparison attempts to split a Prometheus alerting-rule query of the
// form "<expr> <op> <number>" at its top-level comparison operator into the
// measurement expression, an emeland threshold operator (gt/ge/lt/le/eq/ne),
// and the numeric limit as a string.
//
// It only splits when there is exactly one top-level comparison operator
// (outside any parentheses/brackets) and the right-hand side is a bare numeric
// literal. Otherwise ok is false and the caller falls back to treating the
// whole query as a boolean measurement.
func splitComparison(query string) (expr, operator, limit string, ok bool) {
	// Operators are checked longest-first so ">=" is matched before ">".
	type opdef struct {
		sym string
		name string
	}
	ops := []opdef{
		{">=", "ge"}, {"<=", "le"}, {"==", "eq"}, {"!=", "ne"},
		{">", "gt"}, {"<", "lt"},
	}

	depth := 0
	found := -1
	var foundOp opdef
	runes := []rune(query)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		for _, op := range ops {
			if strings.HasPrefix(string(runes[i:]), op.sym) {
				// Reject a second top-level comparison: not cleanly splittable.
				if found >= 0 {
					return "", "", "", false
				}
				found = i
				foundOp = op
				i += len([]rune(op.sym)) - 1
				break
			}
		}
	}
	if found < 0 {
		return "", "", "", false
	}

	lhs := strings.TrimSpace(string(runes[:found]))
	rhs := strings.TrimSpace(string(runes[found+len([]rune(foundOp.sym)):]))
	if lhs == "" || rhs == "" {
		return "", "", "", false
	}
	// The right-hand side must be a bare numeric literal for a clean split.
	if _, err := strconv.ParseFloat(rhs, 64); err != nil {
		return "", "", "", false
	}
	return lhs, foundOp.name, rhs, true
}
