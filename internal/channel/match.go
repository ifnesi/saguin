package channel

import "strings"

// Matches implements MQTT topic filter matching for filters whose channel
// prefix is literal.
// It walks both strings a level at a time rather than splitting them,
// because it is asked once per subscriber of a channel on every publish and
// then once per record: two allocations there were most of what a hundred
// subscribers cost, and they bought nothing but two slices thrown away
// immediately.
func Matches(filter, topic string) bool {
	for {
		fseg, frest, fmore := cut(filter)
		if fseg == "#" {
			// Stands in for nothing as well as for everything, so
			// events/# reaches events itself (MQTT 5 section 4.7.1.2).
			return true
		}
		tseg, trest, tmore := cut(topic)
		if fseg != "+" && fseg != tseg {
			return false
		}
		if !fmore && !tmore {
			return true // both ran out together, on a level that matched
		}
		if !fmore || !tmore {
			// One ran out first. The only thing that still matches is a
			// filter with exactly # left, which stands in for nothing.
			return fmore && frest == "#"
		}
		filter, topic = frest, trest
	}
}

// cut splits one topic level off the front: the level, what follows it, and
// whether there was a separator at all.
func cut(s string) (level, rest string, more bool) {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

// Exactness scores for one level of a filter, and the whole of how saguin
// decides which of two channels holds a topic (invariant 12, RFC 0002
// "Which channel a topic belongs to").
//
// A filter that has ended is the most exact thing a level position can
// hold, because it pins the topic's length: `iot/+/work` says the topic is
// three levels and `iot/+/work/#` does not. Both match `iot/hq/work`, and
// the first is the one that means it.
//
// Against a spelled-out level that comparison never arises, and it is
// worth saying why rather than reading exEnd > exLiteral as a claim that
// `iot/+/work` outranks `iot/+/work/urgent`. If one filter ends at a level
// position, every topic it matches stops there - so a filter needing a
// level at that position matches none of them, and the two are never
// weighed over the same topic. The only pairing that reaches this rule is
// an ended filter against a `#`.
const (
	exHash    = 1 // `#` - this level and every one after it
	exPlus    = 2 // `+` - this level, whatever it holds
	exLiteral = 3 // spelled out
	exEnd     = 4 // the filter ended here
)

// MoreExact orders two filters by how exactly they spell a topic out:
// positive when a is the more exact, negative when b is, zero when the two
// are the same filter.
//
// Compared level by level from the left, the first position at which the
// two differ decides. That is a total order over filters and it does not
// depend on any topic, which is what lets the routing table be sorted once
// at startup and resolved with a linear walk - the same shape resolution
// had when a channel claimed its name.
//
// **Zero is reachable only for two identical filters**, which is why the
// registry refuses those rather than breaking the tie. If two filters both
// match some topic and score the same at every level of it, then every
// spelled-out level of each equals that topic's own, so they are character
// for character the same string.
//
// This runs when the table is built and never on the publish path, so it
// splits rather than walking: clarity is worth more here than the two
// allocations, and Matches is the one that is asked per record.
func MoreExact(a, b string) int {
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	for i := 0; ; i++ {
		ax, bx := exactness(as, i), exactness(bs, i)
		if ax != bx {
			return ax - bx
		}
		// Both are sticky from here: a filter that ended stays ended, and
		// `#` is the last level so it answers for every position after it.
		if ax == exEnd || ax == exHash {
			return 0
		}
	}
}

// exactness scores what a filter offers at level position i.
func exactness(levels []string, i int) int {
	if i >= len(levels) {
		return exEnd
	}
	switch levels[i] {
	case "#":
		return exHash
	case "+":
		return exPlus
	}
	return exLiteral
}

// Intersects reports whether two filters can both match some topic - which
// is how a subscription finds the channels it reaches (RFC 0002 "What a
// filter reaches").
//
// It is deliberately not "does one fit inside the other". A subscriber to
// `iot/water/w-7/#` is served the device's readings and its location from
// two different channels, and neither channel's filter contains that one.
func Intersects(a, b string) bool {
	for {
		aseg, arest, amore := cut(a)
		bseg, brest, bmore := cut(b)
		if aseg == "#" || bseg == "#" {
			// Stands in for nothing as well as for everything, so whatever
			// is left on the other side is reachable (MQTT 5 section 4.7.1.2).
			return true
		}
		if aseg != "+" && bseg != "+" && aseg != bseg {
			return false
		}
		if !amore && !bmore {
			return true // both ran out together, on a level that agreed
		}
		if !amore || !bmore {
			// One ran out first. Only an exact `#` on the other side stands
			// in for the nothing that is left.
			if !amore {
				return brest == "#"
			}
			return arest == "#"
		}
		a, b = arest, brest
	}
}

// Contains reports whether every topic inner can match, outer matches too.
//
// This is the queue's question rather than the subscription's: a filter
// that lies wholly inside a queue's filter is asking for part of that
// queue, and is refused unless it is the pin (invariant 4). A filter that
// merely *crosses* a queue - `#`, `iot/#` - is not contained by it, is
// granted, and is served everything except the queue's records.
//
// Where several channels' filters contain one subscription filter, the
// most exact of them is the one it belongs to. That is what makes a reader
// of `iot/water/+/work/__dlq` a dead-letter consumer rather than somebody
// asking a queue for part of itself, when the queue's own filter ends in
// `#`.
func Contains(outer, inner string) bool {
	for {
		oseg, orest, omore := cut(outer)
		if oseg == "#" {
			return true // outer takes the rest, whatever inner still holds
		}
		iseg, irest, imore := cut(inner)
		if iseg == "#" {
			// inner spans any number of levels from here and outer, having
			// no `#` at this position, cannot cover all of them.
			return false
		}
		if oseg != "+" && oseg != iseg {
			return false
		}
		if !omore && !imore {
			return true
		}
		if !imore {
			// inner matched exactly here, so outer covers it only if what
			// outer has left stands in for nothing - an exact `#`. This is
			// the dead-letter reader's case, and getting it wrong refuses
			// `iot/water/+/work/__dlq` to a reader as though it were asking
			// a queue whose filter ends in `#` for part of itself.
			return orest == "#"
		}
		if !omore {
			return false // inner runs on where outer has nothing left
		}
		outer, inner = orest, irest
	}
}
