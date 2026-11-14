package topic

import (
	"slices"
	"strings"
)

// Filter associates a checked, expanded MQTT filter with a nonzero value.
// WalkFilter preserves input order, including routing precedence.
type Filter struct {
	Pattern string
	Value   int
}

type filterSuffix struct {
	value  int
	filter string
	end    bool
}

// WalkFilter visits the distinct matching sets within subject. It is the
// wildcard partition walk used by channel reach and access coverage: written
// levels and one residual class suffice, without enumerating topic names.
// Returning false stops the walk. matches is borrowed until visit returns.
func WalkFilter(subject string, filters []Filter, visit func(matches []int) bool) {
	remaining := make([]filterSuffix, 0, len(filters))
	for _, row := range filters {
		remaining = append(remaining, filterSuffix{value: row.Value, filter: row.Pattern})
	}
	matches := make([]int, 0, len(filters))
	emit := func(candidates []filterSuffix) bool {
		matches = matches[:0]
		for _, row := range candidates {
			if row.end || row.filter == "#" {
				matches = append(matches, row.value)
			}
		}
		return visit(matches)
	}
	var walk func(string, bool, bool, bool, []filterSuffix) bool
	walk = func(subject string, end, first, named bool, candidates []filterSuffix) bool {
		if end || (subject == "#" && !first) {
			if named && !emit(candidates) {
				return false
			}
			if end {
				return true
			}
		}
		level, rest, more := strings.Cut(subject, "/")
		if level == "#" {
			rest, more = "#", true
		}
		choices := []string{level}
		if level == "+" || level == "#" {
			choices = []string{"\x00"} // every level not written in a remaining filter
			for _, row := range candidates {
				written, _, _ := strings.Cut(row.filter, "/")
				if !row.end && written != "+" && written != "#" &&
					(!first || !strings.HasPrefix(written, "$")) && !slices.Contains(choices, written) {
					choices = append(choices, written)
				}
			}
		}
		for _, choice := range choices {
			next := make([]filterSuffix, 0, len(candidates))
			progress := false
			for _, row := range candidates {
				if row.end {
					continue
				}
				written, suffix, continues := strings.Cut(row.filter, "/")
				if written == "#" && (!first || !strings.HasPrefix(choice, "$")) {
					next = append(next, row)
				} else if written == choice || (written == "+" && (!first || !strings.HasPrefix(choice, "$"))) {
					next = append(next, filterSuffix{row.value, suffix, !continues})
					progress = true
				}
			}
			if rest == "#" && !progress {
				if !emit(next) {
					return false
				}
			} else if !walk(rest, !more, false, named || choice != "" || !first, next) {
				return false
			}
		}
		return true
	}
	walk(subject, false, true, false, remaining)
}
