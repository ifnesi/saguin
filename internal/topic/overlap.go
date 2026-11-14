package topic

import "strings"

// FiltersOverlap intersects two checked, expanded MQTT filters without
// allocating levels. A root wildcard excludes $, # includes its parent,
// and the intersection must contain a nonempty Topic Name.
func FiltersOverlap(first, second string) bool {
	if (first[0] == '$' && (second[0] == '+' || second[0] == '#')) ||
		(second[0] == '$' && (first[0] == '+' || first[0] == '#')) {
		return false
	}
	nonempty := false
	for {
		a, restA, moreA := strings.Cut(first, "/")
		b, restB, moreB := strings.Cut(second, "/")
		if a == "#" || b == "#" {
			// A remaining wildcard can choose a written, nonempty level.
			return nonempty || (a != "" && b != "") || moreA || moreB
		}
		if a != b && a != "+" && b != "+" {
			return false
		}
		nonempty = nonempty || (a != "" && b != "")
		if !moreA && !moreB {
			return nonempty
		}
		if !moreA {
			return nonempty && restB == "#"
		}
		if !moreB {
			return nonempty && restA == "#"
		}
		nonempty = true // The matched separator makes the name nonempty.
		first, second = restA, restB
	}
}
