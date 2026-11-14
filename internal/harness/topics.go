package harness

import (
	"regexp"
	"strconv"
	"strings"
)

// TopicHash is design.md, Flows, SUBSCRIBE, Partitioning's topic_hash:
// "FNV-1a-64 over the topic's UTF-8 bytes, with offset basis
// 14695981039346656037 and prime 1099511628211. It then applies x ^= x >> 30,
// x *= 0xbf58476d1ce4e5b9, x ^= x >> 27, x *= 0x94d049bb133111eb, x ^= x >>
// 31, all modulo 2^64". The broker computes the same value in
// internal/topic (Hash); the tests' oracle is written here from the design,
// apart from the code it checks.
func TopicHash(topic string) uint64 {
	x := uint64(14695981039346656037)
	for i := range len(topic) {
		x ^= uint64(topic[i])
		x *= 1099511628211
	}
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// declarationForm is design.md, Flows, SUBSCRIBE, Partitioning: "A
// declaration is topic_hash(count,index), with ASCII spaces or tabs allowed
// around the integers and comma. Integers are unsigned decimal without signs
// or leading zeroes, except zero itself."
var declarationForm = regexp.MustCompile(`^topic_hash\([ \t]*(0|[1-9][0-9]*)[ \t]*,[ \t]*(0|[1-9][0-9]*)[ \t]*\)$`)

// PartitionDeclaration reads one saguin-filter declaration by the design's
// rule: its form (declarationForm), then "Count is 1 through 4,294,967,295.
// Index is 0 through count minus one". It returns the count and index of a
// valid one, and ok false for any other.
func PartitionDeclaration(declaration string) (count, index uint64, ok bool) {
	m := declarationForm.FindStringSubmatch(declaration)
	if m == nil {
		return 0, 0, false
	}
	count, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil || count < 1 || count > 4_294_967_295 {
		return 0, 0, false // a count past 64 bits is past the bound too
	}
	index, err = strconv.ParseUint(m[2], 10, 64)
	if err != nil || index >= count {
		return 0, 0, false
	}
	return count, index, true
}

// MoreExact reports whether filter a spells a topic out more exactly than
// b, by design.md, Words: Channel, "Filters are compared level by level. The
// first level where they differ decides. A written level beats + and #, and
// + beats #. A filter ending there beats one going on with #", and, in
// ordered, whether that rule orders the two at all: two written levels that
// differ, or a filter ending where the other goes on with a level that is
// not #, leave them unordered. Two filters matching one topic are always
// ordered, unless they are the same.
func MoreExact(a, b string) (more, ordered bool) {
	la, lb := strings.Split(a, "/"), strings.Split(b, "/")
	for i := 0; ; i++ {
		switch {
		case i == len(la) && i == len(lb):
			return false, false
		case i == len(la):
			return lb[i] == "#", lb[i] == "#"
		case i == len(lb):
			return false, la[i] == "#"
		case la[i] == lb[i]:
			continue
		case levelRank(la[i]) != levelRank(lb[i]):
			return levelRank(la[i]) < levelRank(lb[i]), true
		default:
			return false, false
		}
	}
}

// levelRank ranks a filter level for MoreExact, the lower the more exact:
// "A written level beats + and #, and + beats #".
func levelRank(level string) int {
	switch level {
	case "+":
		return 1
	case "#":
		return 2
	}
	return 0
}
