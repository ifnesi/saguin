package channel

import (
	"fmt"
	"strings"
)

// InnerFilter is the topic filter inside a shared subscription, or the
// filter itself. `$share/<group>/events/#` reaches the same topics as
// `events/#` and must answer every question about them the same way - so
// the stripping is one function rather than one copy per caller, which is
// how the two would come to disagree.
func InnerFilter(filter string) string {
	if !strings.HasPrefix(filter, "$share/") {
		return filter
	}
	if parts := strings.SplitN(filter, "/", 3); len(parts) == 3 {
		return parts[2]
	}
	return filter
}

// MisspelledShare reports whether a filter's first level is `$share` in
// some spelling other than the one MQTT defines.
//
// **MQTT topic filters are case sensitive** (MQTT-4.7.3), and a shared
// subscription is spelled `$share/` and nothing else (MQTT-4.8.2-1). So
// `$SHARE/grp/jobs/#` is not a shared subscription at all: it is an
// ordinary filter whose first level happens to be `$SHARE`, and it matches
// only topics literally under that level. Measured against mosquitto
// 2.0.22, which delivers such a subscriber nothing at all.
//
// **The substrate disagrees, and that is why this exists.** It compares
// both `$share` and `$SYS` with a case-insensitive match while comparing
// every ordinary level exactly, so it builds a working shared subscription
// out of `$SHARE/…` and then hands it records its filter does not match.
// Every rule saguin makes about a queue's canonical form and about shared
// subscriptions on other channel types reads the filter as MQTT spells it,
// so all of them were walked straight past: `$SHARE/workers/jobs/#` was
// granted where `$share/workers/jobs/#` is refused, and the client then
// received nothing, for ever, having been told it succeeded.
//
// **Refusing the spelling is not the same as adopting the substrate's
// reading of it.** saguin does not treat `$SHARE/…` as a shared
// subscription - it declines to serve the filter at all, which is one rule
// with one code, and leaves saguin's channel rules reading MQTT exactly as
// written. What it costs is a subscription that on any other broker
// already receives nothing.
func MisspelledShare(filter string) bool {
	level, _, _ := strings.Cut(filter, "/")
	return level != sharePrefix && strings.EqualFold(level, sharePrefix)
}

// ShareMalformed reports why a filter beginning `$share/` is not a shared
// subscription as MQTT 5 writes one, or "" - for a well-formed one and for
// any filter not beginning `$share/` at all.
//
// **`$share/<ShareName>/<filter>` and nothing looser** (MQTT 5 section
// 4.8.2): a ShareName of at least one character holding no `+` or `#`, then
// a `/`, then a topic filter. The substrate refuses some of these and grants
// `$share//events/#`, so the rule is written here in full rather than split
// between the two.
//
// **The filter may not be a shared subscription itself.** `$share/g/$share/h/x`
// is legal to MQTT - the inner filter is an ordinary one whose first level is
// `$share` - but it matches only topics under `$share/`, which saguin refuses
// to publish, so it is a grant nothing can ever be delivered to. Any casing
// is refused, as MisspelledShare refuses it alone.
func ShareMalformed(filter string) string {
	rest, ok := strings.CutPrefix(filter, sharePrefix+"/")
	if !ok {
		return ""
	}
	group, inner, ok := strings.Cut(rest, "/")
	switch {
	case group == "":
		return "a ShareName is at least one character (MQTT-4.8.2-1)"
	case strings.ContainsAny(group, "+#"):
		return "a ShareName holds no + or # (MQTT-4.8.2-2)"
	case !ok || inner == "":
		return "a shared subscription is $share/<ShareName>/<filter>, and this " +
			"names no filter after the ShareName (MQTT-4.8.2-2)"
	}
	if level, _, _ := strings.Cut(inner, "/"); strings.EqualFold(level, sharePrefix) {
		return "a shared subscription cannot hold another: the filter after the " +
			"ShareName begins $share, and nothing is ever published there"
	}
	return ""
}

// sharePrefix is the first level of a shared subscription, as MQTT spells
// it. The `$share/` literals elsewhere in this file predate it and read
// more clearly with the separator attached.
const sharePrefix = "$share"

// ValidWildcards reports what is wrong with where a filter puts `+` and
// `#`, or nil. These are MQTT's own rules about a topic filter and nothing
// besides, which is what makes them the ones to ask of a filter that is
// not a channel's - a rule in the acl_file naming a broadcast `topic:`,
// where `#` on its own is how an operator says "any broadcast topic" and
// has to go on working.
//
// Split out from ValidFilter rather than written twice: the acl_file used
// to ask neither, and a rule filter spelled `alerts/#/page` was accepted
// at startup and then granted every `alerts` topic, because a `#` the
// matcher meets in the middle of a filter stands in for everything after
// it. An operator wrote a narrowing and got the lot.
func ValidWildcards(filter string) error {
	if filter == "" {
		return fmt.Errorf("empty")
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		switch {
		case strings.Contains(level, "\x00"):
			return fmt.Errorf("level %d holds a NUL", i+1)
		case level == "#":
			if i != len(levels)-1 {
				return fmt.Errorf(
					"`#` is level %d of %d: it may only be the last level", i+1, len(levels))
			}
		case level == "+":
			// Fine anywhere. The first level has a rule of its own, and it
			// is a channel's rather than MQTT's, so ValidFilter asks it.
		case strings.ContainsAny(level, "+#"):
			return fmt.Errorf(
				"level %d is %q: `+` and `#` each take a whole level", i+1, level)
		}
	}
	return nil
}

// ValidFilter reports what is wrong with a channel's filter, or nil.
//
// It is ValidWildcards with one addition: the first level is spelled out
// and does not begin with `$`. A `+` or a `#` there would claim `$SYS/…`
// and `$saguin/…` along with everything else, and a channel that swallows
// the broker's own control topics is a broker with no control topics. It
// is refused at startup rather than special-cased in the matcher, so that
// the matcher stays the ordinary MQTT one (RFC 0002 "Validation").
//
// The `__dlq` rule is not here, because every filter carrying that level
// is one the broker derived. NoDLQLevel is what the registry asks of a
// filter an operator wrote.
func ValidFilter(filter string) error {
	if err := ValidWildcards(filter); err != nil {
		return err
	}
	switch first := strings.Split(filter, "/")[0]; {
	case first == "+" || first == "#":
		return fmt.Errorf(
			"begins with %q: the first level is spelled out, so that a channel cannot "+
				"claim $SYS and $saguin along with everything else", first)
	case strings.HasPrefix(first, "$"):
		return fmt.Errorf(
			"begins with %q: the `$` space is the broker's own", first)
	}
	return nil
}

// TooDeep reports a topic name or filter with more levels than max, which is
// limits.max_topic_levels, or nil. **One question for every place a topic or
// a filter enters** - a publish, a Will, a SUBSCRIBE, an UNSUBSCRIBE, and the
// filters of the configuration and the acl_file - so that none of them admits
// what another refuses. A level is what lies between two `/`, so a string has
// one more level than it has separators. mosquitto counts the separators
// instead and refuses more than 200, so at the same figure it accepts one
// level more than this does.
//
// A max below 1 bounds nothing. The configuration refuses one, so only a
// broker built without a configuration has it.
func TooDeep(s string, max int) error {
	if max < 1 {
		return nil
	}
	if n := strings.Count(s, "/") + 1; n > max {
		return fmt.Errorf("has %d levels, more than limits.max_topic_levels allows (%d)", n, max)
	}
	return nil
}

// NoDLQLevel reports whether a filter an operator wrote carries a `__dlq`
// level, which is reserved: every filter that holds one is derived from a
// queue's, and a channel claiming those topics would take the dead letters
// out of reach of the channel that is supposed to hold them.
func NoDLQLevel(filter string) error {
	for i, level := range strings.Split(filter, "/") {
		if level == DLQSuffix {
			return fmt.Errorf(
				"level %d is %q, which is reserved: the broker derives every filter "+
					"holding it, from the queue whose dead letters it carries", i+1, DLQSuffix)
		}
	}
	return nil
}

// Expand turns one written filter into the plain filters it stands for,
// resolving `{a,b}` levels. A filter with no braces expands to itself.
//
// A brace is one whole level and holds no `/`, so an alternative is a
// level and never a subtree. Several braced levels expand as every
// combination, in the order they would be read.
func Expand(filter string) ([]string, error) {
	out := []string{""}
	for i, level := range strings.Split(filter, "/") {
		alts, err := alternatives(level)
		if err != nil {
			return nil, fmt.Errorf("level %d: %w", i+1, err)
		}
		grown := make([]string, 0, len(out)*len(alts))
		for _, prefix := range out {
			for _, alt := range alts {
				if i == 0 {
					grown = append(grown, alt)
					continue
				}
				grown = append(grown, prefix+"/"+alt)
			}
		}
		out = grown
	}
	return out, nil
}

// HasBraces reports whether a filter is written with any `{a,b}` level. A
// queue's may not be: braces are two filters, two filters are two pinned
// strings, and two pinned strings are two consumer groups that each take a
// copy of every job (invariant 4).
func HasBraces(filter string) bool { return strings.ContainsAny(filter, "{}") }

// alternatives is the spellings one written level stands for.
func alternatives(level string) ([]string, error) {
	if !strings.ContainsAny(level, "{}") {
		return []string{level}, nil
	}
	if !strings.HasPrefix(level, "{") || !strings.HasSuffix(level, "}") {
		return nil, fmt.Errorf(
			"%q: a brace is a whole level, so write `{a,b}` and not `x{a,b}`", level)
	}
	inner := level[1 : len(level)-1]
	if strings.ContainsAny(inner, "{}") {
		return nil, fmt.Errorf("%q: braces do not nest", level)
	}
	if inner == "" {
		return nil, fmt.Errorf("%q: holds no alternatives", level)
	}
	alts := strings.Split(inner, ",")
	seen := make(map[string]bool, len(alts))
	for _, alt := range alts {
		if alt == "" {
			return nil, fmt.Errorf("%q: an alternative is empty", level)
		}
		// Refused here rather than as the two expanded filters meeting,
		// which named one channel as two claiming it.
		if seen[alt] {
			return nil, fmt.Errorf("%q: alternative %q is written twice", level, alt)
		}
		seen[alt] = true
		if strings.ContainsAny(alt, "+#") {
			return nil, fmt.Errorf(
				"%q: alternative %q holds a wildcard, which takes a whole level", level, alt)
		}
	}
	return alts, nil
}

// DLQFilter is the filter of the dead-letter channel derived from a queue
// whose filter is given: a `__dlq` level inserted where the queue's filter
// carries its `#`, or appended where it carries none.
//
// One rule for every shape of filter, which is why it won over a sibling
// level (`…/work__dlq`, today's `jobs__dlq` generalised): that one needs a
// spelled-out last level to append to, so a queue could not be
// `iot/water/+/+`.
//
// The result is always the more exact of the two (MoreExact), including
// where the queue's filter ends in `#` and the derived filter therefore
// lies inside it - `__dlq` is spelled out where the queue has `#`. That is
// what makes a dead letter readable at all: a subscriber finds a channel
// through the filters and nothing else, so a record still carrying the
// queue's own topic would belong to the queue and be refused to everyone
// who is not a worker.
func DLQFilter(queueFilter string) string {
	levels := strings.Split(queueFilter, "/")
	if last := len(levels) - 1; levels[last] == "#" {
		levels = append(levels[:last], DLQSuffix, "#")
		return strings.Join(levels, "/")
	}
	return queueFilter + "/" + DLQSuffix
}

// DLQTopic is the topic a dead-lettered record takes: the same insertion
// DLQFilter makes, applied to the topic the record was published under.
//
// The `+` levels keep what they captured because they are the same
// positions - one level goes in and nothing else moves - so the rewritten
// topic matches DLQFilter(queueFilter) by construction.
func DLQTopic(queueFilter, topic string) string {
	levels := strings.Split(queueFilter, "/")
	last := len(levels) - 1
	if levels[last] != "#" {
		return topic + "/" + DLQSuffix
	}
	// The `#` stands at level `last`, so that is where the record's own
	// tail begins and where the level is inserted.
	parts := strings.Split(topic, "/")
	if len(parts) < last {
		return topic + "/" + DLQSuffix
	}
	out := make([]string, 0, len(parts)+1)
	out = append(out, parts[:last]...)
	out = append(out, DLQSuffix)
	out = append(out, parts[last:]...)
	return strings.Join(out, "/")
}

// PartitionHash is the value a subscriber's share is taken from: FNV-1a
// over a topic's bytes, 64-bit, and then a mixing step. It is a specified
// constant of the protocol rather than an implementation detail (RFC 0003
// "Client-declared partitioning").
//
// **A client has to be able to compute this.** A subscriber declaring
// `saguin-filter: topic_hash(<partitions>, <index>)` needs to know which
// of its topics land in its slice, so both halves are written into the RFC
// in full and are ten lines in any language. That is why it is FNV-1a and
// not something faster: xxhash needs a library per language and is fifty
// lines to restate, and murmur3 has several incompatible variants people
// get wrong.
//
// **Explicitly not Go's maphash**, which is the fastest of the candidates
// and disqualified outright: it is randomly seeded per process, so a
// restart would reshuffle every assignment and a client could not predict
// its own slice at all.
func PartitionHash(topic string) uint64 {
	return mixed(fnv1a64(topic))
}

// fnv1a64 is FNV-1a exactly, which TestPartitionHashIsFNV1a64 holds
// against the standard library. Written out rather than taken from
// hash/fnv because that package's API allocates a hasher and takes
// []byte; this is the same arithmetic with neither.
func fnv1a64(topic string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(topic); i++ {
		h ^= uint64(topic[i])
		h *= prime64
	}
	return h
}

// mixed is the finalizer, and **without it a whole family of topic schemes
// is excluded from most of a group rather than spread across it.**
//
// FNV-1a stirs the top of its accumulator thoroughly and the bottom hardly
// at all, and the bottom is the half a modulus reads. Measured: flip one
// bit of a topic and FNV-1a's lowest bit changes 12.5% of the time where
// an even split needs 50%, the next three 19%, 25% and 31%. The
// consequence is not noise that averages out. Because the prime is odd,
// each byte either flips the low bit or does not, so a byte appearing
// twice cancels itself - and a topic that carries an identifier twice,
// `devices/<id>/messages/devicebound/<id>`, hashes every id to the same
// low bits. Four members split 4000 such topics 2454/0/1546/0: two
// members receive nothing, for ever, at any scale, and nothing anywhere
// says so. Eight other real conventions were driven and split evenly, so
// the trigger is narrow - which is what makes it worth removing rather
// than documenting, since the operator who meets it cannot diagnose it.
//
// **This is the standard shape rather than a repair.** Every modern hash
// is a mixing loop and then a finalizer; murmur3 and xxhash both end this
// way, and FNV-1a is the unusual one for stopping early. The constants are
// splitmix64's. After it every output bit lands within half a point of the
// ideal 50%, against 37.5 points out before, and the excluded family
// splits 1005/996/1011/988.
//
// The cost is two multiplications and three shifts per topic, against a
// per-publish cost measured in microseconds.
func mixed(h uint64) uint64 {
	h ^= h >> 30
	h *= 0xBF58476D1CE4E5B9
	h ^= h >> 27
	h *= 0x94D049BB133111EB
	h ^= h >> 31
	return h
}
