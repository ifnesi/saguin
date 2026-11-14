// Package acl compiles access rules and selects a proved user's permissions.
package acl

import (
	"regexp"
	"strings"
)

// Pattern compiles a whole-name pattern. Only * has special meaning, and it
// includes newlines as well as every other Unicode code point.
func Pattern(pattern string) (*regexp.Regexp, error) {
	parts := strings.Split(pattern, "*")
	for i, part := range parts {
		parts[i] = regexp.QuoteMeta(part)
	}
	return regexp.Compile(`\A(?s:` + strings.Join(parts, `.*`) + `)\z`)
}

// PatternsOverlap reports whether the two patterns share a name. This
// load-time dynamic program intersects their literal and * transitions over
// code points. Each row depends only on the preceding row and its left cell.
func PatternsOverlap(a, b string) bool {
	y := []rune(b)
	previous := make([]bool, len(y)+1)
	current := make([]bool, len(y)+1)
	previous[0] = true
	for j, r := range y {
		previous[j+1] = previous[j] && r == '*'
	}
	for _, r := range a {
		current[0] = previous[0] && r == '*'
		for j, s := range y {
			switch {
			case r == '*':
				current[j+1] = previous[j+1] || current[j]
			case s == '*':
				current[j+1] = current[j] || previous[j+1]
			default:
				current[j+1] = previous[j] && r == s
			}
		}
		previous, current = current, previous
	}
	return previous[len(y)]
}
