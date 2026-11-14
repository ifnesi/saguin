package acl_test

import (
	"math/big"
	"regexp"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/acl"
)

// words is every string of min to max symbols, each one of alphabet.
func words(alphabet []string, minLen, maxLen int) []string {
	var all []string
	last := []string{""}
	for n := 1; n <= maxLen; n++ {
		next := make([]string, 0, len(last)*len(alphabet))
		for _, w := range last {
			for _, s := range alphabet {
				next = append(next, w+s)
			}
		}
		if n >= minLen {
			all = append(all, next...)
		}
		last = next
	}
	return all
}

// matcher is pattern as design.md says it matches: "The broker matches a
// name against a pattern with Go's regular expressions, escaping every part
// of the pattern except *", "*
// match[ing] any run of characters", the whole name.
func matcher(pattern string) *regexp.Regexp {
	parts := strings.Split(pattern, "*")
	for i, p := range parts {
		parts[i] = regexp.QuoteMeta(p)
	}
	return regexp.MustCompile(`^(?s:` + strings.Join(parts, `.*`) + `)$`)
}

// design.md, Access and reload: "In the pattern, * matches any run of
// characters"; "Two patterns with the same count that can both match one
// name are refused at load"; "Characters are Unicode code points, not
// bytes"; "The broker matches a name against a pattern with Go's regular
// expressions, escaping every part of the pattern except *. When it reads
// the file, it decides whether two patterns can match the same name with the
// standard dynamic-programming check over code points" (item 0018c, 261030 085128: the check is tested alone, against a
// brute-force search).
//
// The check is
//
//	func PatternsOverlap(a, b string) bool
//
// reporting whether some name matches both patterns. Every ordered pair of
// patterns of one to four symbols over a, é, . and * overlaps exactly
// where a brute-force search finds a name of one to six characters over a,
// é and . that both match through Go's regular expressions, every part
// except * escaped. Six is enough: where two patterns share a name, they share one
// no longer than their characters other than * together, as a character
// both match through * can be left out; a pattern without * matches its
// own four characters or fewer alone, and two with * hold at most six
// between them. é is two bytes and one code point; . is a character, not a
// wildcard.
func TestPatternsOverlap(t *testing.T) {
	patterns := words([]string{"a", "é", ".", "*"}, 1, 4)
	names := words([]string{"a", "é", "."}, 1, 6)
	matched := make([]*big.Int, len(patterns)) // bit i: names[i] matches
	for i, p := range patterns {
		re := matcher(p)
		matched[i] = new(big.Int)
		for j, name := range names {
			if re.MatchString(name) {
				matched[i].SetBit(matched[i], j, 1)
			}
		}
	}
	both, failures := new(big.Int), 0
	for i, a := range patterns {
		for j, b := range patterns {
			want := both.And(matched[i], matched[j]).Sign() != 0
			if got := acl.PatternsOverlap(a, b); got != want {
				t.Errorf("PatternsOverlap(%q, %q) = %v; want %v", a, b, got, want)
				failures++
				if failures == 20 {
					t.Fatal("stopping after 20 disagreements")
				}
			}
		}
	}
}
