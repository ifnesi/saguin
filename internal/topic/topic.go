// Package topic checks and matches MQTT Topic Names and Topic Filters.
// Inputs must already be checked as MQTT UTF-8 Encoded Strings by the packet
// decoder; this package checks topic syntax, reserved prefixes and
// caller-supplied limits.
package topic

import (
	"errors"
	"strings"
)

// Validation errors distinguish malformed MQTT syntax from well-formed
// topics that exceed the broker's configured limits.
var (
	ErrMalformed = errors.New("malformed topic")
	ErrOverLimit = errors.New("topic exceeds limits")
)

// MQTT topic syntax (sections 4.7 and 4.8.2) and reserved topic prefixes.
const (
	levelSeparator      = '/'
	singleLevelWildcard = '+'
	multiLevelWildcard  = '#'
	dollarPrefix        = '$'
	sharedPrefix        = "$share/"
	saguinPrefix        = "$saguin/"
	multiLevelSuffix    = "/#"
)

// Limits bounds a topic name or filter. Callers supply the configuration's
// max_topic_length and max_topic_levels; no defaults are applied here.
type Limits struct {
	MaxLength int // Maximum length in bytes.
	MaxLevels int // Maximum number of levels, including empty levels.
}

// HasReservedPrefix reports whether value starts with $share/ or $saguin/.
// The trailing slash is required; the bare roots are not matched.
func HasReservedPrefix(value string) bool {
	return strings.HasPrefix(value, sharedPrefix) || strings.HasPrefix(value, saguinPrefix)
}

// CheckName checks that name is nonempty and contains no wildcards, then
// applies limits. Malformed syntax takes precedence over either limit.
func CheckName(name string, limits Limits) error {
	if name == "" {
		return ErrMalformed
	}
	levels := 1
	for i := range len(name) {
		switch name[i] {
		case singleLevelWildcard, multiLevelWildcard:
			return ErrMalformed
		case levelSeparator:
			levels++
		}
	}
	if len(name) > limits.MaxLength || levels > limits.MaxLevels {
		return ErrOverLimit
	}
	return nil
}

// CheckFilter checks a nonempty filter: + occupies a whole level, and #
// occupies the whole last level. Malformed syntax takes precedence over
// limits. Shared subscriptions must first be split with SplitShared.
func CheckFilter(filter string, limits Limits) error {
	if filter == "" {
		return ErrMalformed
	}
	levels := 1
	for i := range len(filter) {
		switch filter[i] {
		case levelSeparator:
			levels++
		case singleLevelWildcard:
			if (i > 0 && filter[i-1] != levelSeparator) ||
				(i+1 < len(filter) && filter[i+1] != levelSeparator) {
				return ErrMalformed
			}
		case multiLevelWildcard:
			if (i > 0 && filter[i-1] != levelSeparator) || i != len(filter)-1 {
				return ErrMalformed
			}
		}
	}
	if len(filter) > limits.MaxLength || levels > limits.MaxLevels {
		return ErrOverLimit
	}
	return nil
}

// SplitShared recognises $share/ and checks its nonempty ShareName, which
// contains no /, + or # and must be followed by /. It returns the group and
// remaining filter as parts of subscription, without copying. The caller
// checks the remaining filter with CheckFilter, even when it is empty.
// An ordinary subscription is returned whole with an empty group.
func SplitShared(subscription string) (group, filter string, err error) {
	after, shared := strings.CutPrefix(subscription, sharedPrefix)
	if !shared {
		return "", subscription, nil
	}
	// Find the group's end and check its syntax in the same pass.
	for i := range len(after) {
		switch after[i] {
		case singleLevelWildcard, multiLevelWildcard:
			return "", "", ErrMalformed
		case levelSeparator:
			if i == 0 {
				return "", "", ErrMalformed
			}
			return after[:i], after[i+1:], nil
		}
	}
	return "", "", ErrMalformed
}

// Match reports whether a checked filter matches a checked name, without
// normalization. For a shared subscription, use only SplitShared's filter.
// A leading wildcard never matches a name beginning with $ (MQTT-4.7.2-1).
func Match(filter, name string) bool {
	return MatchCaptures(filter, name, nil)
}

// Capture identifies a wildcard's byte range without retaining packet bytes.
type Capture struct{ Start, End int }

// Captures records the matched topic's wildcard levels. Plus is caller-owned
// scratch with room for the filter's + levels. HashPresent distinguishes an
// empty final level from # matching no level at all.
type Captures struct {
	Plus        []Capture
	Hash        Capture
	HashPresent bool
}

// MatchCaptures shares Match's traversal, optionally recording wildcard
// levels for a bridge template without splitting or matching the topic twice.
func MatchCaptures(filter, name string, captures *Captures) bool {
	if captures != nil {
		captures.Hash, captures.HashPresent = Capture{}, false
	}
	if name[0] == dollarPrefix && (filter[0] == singleLevelWildcard || filter[0] == multiLevelWildcard) {
		return false
	}
	namePos, plus := 0, 0
	for filterPos := range len(filter) {
		switch filter[filterPos] {
		case multiLevelWildcard:
			if captures != nil {
				captures.Hash, captures.HashPresent = Capture{namePos, len(name)}, true
			}
			return true
		case singleLevelWildcard:
			// + consumes one level, including an empty one; leave its
			// separator for the next filter byte to match.
			start := namePos
			end := strings.IndexByte(name[namePos:], levelSeparator)
			if end < 0 {
				namePos = len(name)
			} else {
				namePos += end
			}
			if captures != nil {
				captures.Plus[plus] = Capture{start, namePos}
				plus++
			}
		default:
			if namePos == len(name) {
				// # includes its parent: a remaining /# needs no further
				// separator or level in the name (MQTT 4.7.1.2).
				return filter[filterPos:] == multiLevelSuffix
			}
			if filter[filterPos] != name[namePos] {
				return false
			}
			namePos++
		}
	}
	return namePos == len(name)
}
