package channel

// Random filters, held to the rules RFC 0002 states about them.
//
// **These functions decide where every record goes**, and a subtle error in
// one is invisible and expensive: `MoreExact` settles which of two
// overlapping channels holds a topic, and `Contains` is half of what keeps
// a wildcard away from a queue. The table tests beside this file drive the
// cases somebody thought of. These generate filters nobody wrote and check
// the properties instead - which is a much stronger statement about a
// matcher than any table can make, and one worth being able to repeat after
// somebody edits either function.
//
// **The oracles are written from the prose, not from the code under test.**
// fuzzMatches is MQTT matching restated; fuzzRank and fuzzMoreExact are RFC
// 0002's "a spelled-out level beats `+`, `+` beats `#`, and the first level
// at which two filters differ decides", restated. Comparing an
// implementation against itself proves only that it is deterministic.
//
// **A fuzz target that is never fuzzed asserts almost nothing.** `go test`
// runs the seed corpus and stops; the fuzzing needs `-fuzz`, one target at a
// time. So these run for a few seconds each under `make fuzz`, and that
// target derives its list from the source rather than repeating it - a
// written-out list is one target short the day somebody adds the eighth.

import (
	"strings"
	"testing"
)

// fuzzLevels turns a fuzzer's bytes into a filter or topic over a small
// vocabulary. A wide alphabet would mostly generate filters that meet
// nothing; what these properties need is dense overlap, so the levels come
// from a handful of words plus the two wildcards.
func fuzzLevels(seed []byte, wildcards bool) string {
	words := []string{"a", "b", "c", "iot"}
	if len(seed) == 0 {
		return "a"
	}
	var out []string
	for i, b := range seed {
		if i >= 5 {
			break
		}
		switch {
		case wildcards && b%7 == 0:
			// `#` is legal only as the last level, so it ends the filter.
			return strings.Join(append(out, "#"), "/")
		case wildcards && b%7 == 1:
			out = append(out, "+")
		default:
			out = append(out, words[int(b)%len(words)])
		}
	}
	if len(out) == 0 {
		out = append(out, "a")
	}
	return strings.Join(out, "/")
}

// fuzzMatches is MQTT topic matching restated from the specification: `#`
// takes the level it sits at and everything below, `+` takes exactly one
// level, anything else is literal, and a filter that runs out of levels
// matches only a topic that has run out too.
func fuzzMatches(filter, topic string) bool {
	f, t := strings.Split(filter, "/"), strings.Split(topic, "/")
	for i := range f {
		if f[i] == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if f[i] != "+" && f[i] != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// fuzzRank is how exactly one position spells a topic out: a literal level,
// then `+`, then a filter that has simply ended, and `#` last.
//
// **Ending is more exact than `#`, and conflating the two was this oracle's
// own defect** - found by the fuzzer in seconds, on `a` against `a/#`. Both
// match the topic `a`, because `#` matches the parent level as well as
// everything below it (MQTT 5 section 4.7.1.2). But `a` claims exactly that
// one topic and `a/#` sweeps the whole tree under it, so `a` is plainly the
// more exact and the implementation says so. An oracle that called them
// equal would have reported the broker wrong about the one case where the
// answer is obvious.
//
// A position past the end can only meet a `#` on the other side, since two
// filters matching one topic of L levels each have L levels or end in `#`
// above it - so this ordering is only ever consulted for that pair.
func fuzzRank(levels []string, i int) int {
	if i >= len(levels) {
		return 1 // the filter ended: it claims this topic and nothing below
	}
	switch levels[i] {
	case "#":
		return 0
	case "+":
		return 2
	default:
		return 3
	}
}

// fuzzMoreExact is RFC 0002's rule restated: the first level at which the
// two differ decides.
func fuzzMoreExact(a, b string) int {
	al, bl := strings.Split(a, "/"), strings.Split(b, "/")
	n := len(al)
	if len(bl) > n {
		n = len(bl)
	}
	for i := 0; i < n; i++ {
		if ar, br := fuzzRank(al, i), fuzzRank(bl, i); ar != br {
			return ar - br
		}
	}
	return 0
}

// sign reduces a comparison to -1, 0 or 1: the rule is about which of two
// filters is more exact, never by how much.
func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}

// FuzzMatchesAgreesWithTheSpecification holds Matches to MQTT matching as
// the specification states it, over filters and topics nobody wrote.
func FuzzMatchesAgreesWithTheSpecification(f *testing.F) {
	f.Add([]byte("iot"), []byte("iot"))
	f.Add([]byte{0, 1, 2}, []byte{3, 4, 5})
	f.Fuzz(func(t *testing.T, fb, tb []byte) {
		filter := fuzzLevels(fb, true)
		topic := fuzzLevels(tb, false)
		if ValidFilter(filter) != nil {
			return
		}
		if got, want := Matches(filter, topic), fuzzMatches(filter, topic); got != want {
			t.Fatalf("Matches(%q, %q) = %v, the specification says %v",
				filter, topic, got, want)
		}
	})
}

// FuzzContainsImpliesEveryInnerTopicMatches holds the relationship the
// queue rule rests on: if the outer filter contains the inner, then every
// topic the inner matches the outer matches too. A Contains that says yes
// too readily is how a wildcard reaches a queue (invariant 11).
func FuzzContainsImpliesEveryInnerTopicMatches(f *testing.F) {
	f.Add([]byte("iot"), []byte("iot"), []byte("iot"))
	f.Fuzz(func(t *testing.T, ob, ib, tb []byte) {
		outer, inner := fuzzLevels(ob, true), fuzzLevels(ib, true)
		if ValidFilter(outer) != nil || ValidFilter(inner) != nil {
			return
		}
		if !Contains(outer, inner) {
			return
		}
		topic := fuzzLevels(tb, false)
		if fuzzMatches(inner, topic) && !fuzzMatches(outer, topic) {
			t.Fatalf("Contains(%q, %q) is true, but %q matches the inner and not "+
				"the outer - a filter inside a queue would escape it",
				outer, inner, topic)
		}
	})
}

// FuzzIntersectsIsSymmetricAndMeansWhatItSays holds Intersects to both
// halves of its meaning: it is symmetric, and where it says two filters
// meet, no topic may match one and not be reachable by the other's shape.
func FuzzIntersectsIsSymmetricAndMeansWhatItSays(f *testing.F) {
	f.Add([]byte("iot"), []byte("iot"), []byte("iot"))
	f.Fuzz(func(t *testing.T, ab, bb, tb []byte) {
		a, b := fuzzLevels(ab, true), fuzzLevels(bb, true)
		if ValidFilter(a) != nil || ValidFilter(b) != nil {
			return
		}
		if Intersects(a, b) != Intersects(b, a) {
			t.Fatalf("Intersects(%q, %q) = %v but Intersects(%q, %q) = %v: "+
				"two filters meet or they do not, whichever way round they are asked",
				a, b, Intersects(a, b), b, a, Intersects(b, a))
		}
		// A topic matching both is a witness that they meet. The converse
		// needs a search over every topic and is not what this asserts.
		topic := fuzzLevels(tb, false)
		if fuzzMatches(a, topic) && fuzzMatches(b, topic) && !Intersects(a, b) {
			t.Fatalf("%q and %q both match %q, and Intersects says they do not meet",
				a, b, topic)
		}
	})
}

// FuzzMoreExactAgreesWithTheRule holds MoreExact to RFC 0002's prose, and
// to being an ordering: antisymmetric, and zero only where the two filters
// are the same - which is what the registry's refusal of two channels
// carrying one filter rests on.
func FuzzMoreExactAgreesWithTheRule(f *testing.F) {
	f.Add([]byte("iot"), []byte("iot"), []byte("iot"))
	f.Fuzz(func(t *testing.T, ab, bb, tb []byte) {
		a, b := fuzzLevels(ab, true), fuzzLevels(bb, true)
		if ValidFilter(a) != nil || ValidFilter(b) != nil {
			return
		}
		// **Only where both filters match a topic**, which is the whole
		// domain of RFC 0002's rule: it decides which of two channels holds
		// a topic they both claim. Outside it the prose says nothing, and
		// this target first asserted there anyway - on `a` against `a/a`,
		// which match `a` and `a/a` and so compete for nothing. Asserting an
		// oracle where the document is silent is inventing a requirement.
		//
		// Two filters that both match a topic of L levels each either have
		// exactly L levels or end in `#` above it, so the case of a filter
		// simply running out never arises here.
		topic := fuzzLevels(tb, false)
		if !fuzzMatches(a, topic) || !fuzzMatches(b, topic) {
			return
		}
		got, want := sign(MoreExact(a, b)), sign(fuzzMoreExact(a, b))
		if got != want {
			t.Fatalf("MoreExact(%q, %q) is %d and the rule says %d, for two "+
				"filters that both match %q", a, b, got, want, topic)
		}
		if s := sign(MoreExact(b, a)); s != -got {
			t.Fatalf("MoreExact(%q, %q) = %d and MoreExact(%q, %q) = %d: "+
				"one of two filters is the more exact, not both and not neither",
				a, b, got, b, a, s)
		}
		// **Zero only for identical filters, and only where both match a
		// topic** - the second half matters and this target first asserted
		// without it, on `a` against `iot`. Those are equally *exact*, both
		// being one literal level, and they compete for nothing: no topic is
		// both. The rule is about which of two filters holds a topic, so it
		// says nothing about two that never meet.
		//
		// Where they do meet, zero would leave the registry no tie to break
		// and a record landing wherever a map iteration put it, which is
		// what invariant 12 refuses.
		if got == 0 && a != b {
			t.Fatalf("MoreExact(%q, %q) = 0 and both match %q: two different "+
				"filters compete for a topic and the registry has no tie to break",
				a, b, topic)
		}
	})
}

// FuzzDLQTopicStaysInsideTheDeadLetterChannel holds the derived topic to
// landing where the derived channel claims: whatever shape a queue's filter
// has, the dead-letter topic built from it must be one that channel's own
// filter matches, or a dead-lettered record goes somewhere nobody is
// looking.
func FuzzDLQTopicStaysInsideTheDeadLetterChannel(f *testing.F) {
	f.Add([]byte("iot"), []byte("iot"))
	f.Fuzz(func(t *testing.T, qb, tb []byte) {
		queue := fuzzLevels(qb, true)
		if ValidFilter(queue) != nil || NoDLQLevel(queue) != nil || HasBraces(queue) {
			return
		}
		topic := fuzzLevels(tb, false)
		if !Matches(queue, topic) {
			return
		}
		dlqFilter := DLQFilter(queue)
		dlqTopic := DLQTopic(queue, topic)
		if !fuzzMatches(dlqFilter, dlqTopic) {
			t.Fatalf("a record dead-lettered from %q at %q lands on %q, which the "+
				"derived channel's filter %q does not match",
				queue, topic, dlqTopic, dlqFilter)
		}
	})
}

// **The two parsers today added, against input nobody thought of.**
// `MisspelledShare` and `SeekReplyChannel` both decide something a client
// controls entirely - a subscription filter - and both gate a refusal, so a
// string that slips past either is a subscription served where it should be
// turned away or turned away where it should be served.
func FuzzMisspelledShareIsOnlyEverAMisspelling(f *testing.F) {
	for _, s := range []string{
		"$share/g/a/#", "$SHARE/g/a/#", "$Share/g/a/#", "$sharex/g", "$share",
		"$share/", "events/#", "", "/", "$SHAREPOINT/docs/#", "$share//a",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, filter string) {
		got := MisspelledShare(filter)

		// **The exact spelling is never a misspelling.** This is the half
		// that matters: a true here would refuse the canonical queue form
		// and every worker with it.
		if strings.HasPrefix(filter, "$share/") && got {
			t.Fatalf("MisspelledShare(%q) is true for the spelling MQTT defines", filter)
		}
		// And it is true only when the first level really is that word in
		// some other case - never for an unrelated filter.
		level, _, _ := strings.Cut(filter, "/")
		if got != (level != "$share" && strings.EqualFold(level, "$share")) {
			t.Fatalf("MisspelledShare(%q) = %v, and its first level is %q",
				filter, got, level)
		}
	})
}

// A seek reply topic is recognised exactly when the channel built it, and
// the name it yields is always one a channel could have.
func FuzzSeekReplyChannelRoundTrips(f *testing.F) {
	for _, s := range []string{"events", "a", "with-dash", "", "a/b", "$x"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		c := &Channel{Name: name, Type: Append}
		got, ok := SeekReplyChannel(c.SeekReplyTopic())

		// A name a channel may actually have round-trips; one that could
		// never be a channel name is not recognised, which is the same rule
		// SeekChannel keeps and the reason this asks it rather than parsing
		// again.
		valid := name != "" && !strings.Contains(name, "/")
		if ok != valid || (ok && got != name) {
			t.Fatalf("SeekReplyChannel(%q) = %q, %v for a channel named %q",
				c.SeekReplyTopic(), got, ok, name)
		}
		// And the seek topic itself is never mistaken for its own reply.
		if _, ok := SeekReplyChannel(c.SeekTopic()); ok && valid {
			t.Fatalf("the seek topic %q was read as a reply topic", c.SeekTopic())
		}
	})
}

// **A control topic's parser never panics, and recognises exactly the
// topics its channel builds**. SeekChannel and
// ResponseChannel asked HasPrefix and HasSuffix of the whole topic and
// sliced between, and on `$saguin/consumer/seek` - where the two share a `/`
// - that sliced from 17 to 16 and panicked: one PUBLISH from any client, and
// the broker exited. FuzzSeekReplyChannelRoundTrips could not find it,
// because it builds its topics from a channel name and so never writes the
// overlap. This feeds the parsers arbitrary strings instead: none may
// panic, and a topic any of them accepts must be the one its channel would
// build from the name it returned.
func FuzzControlTopicParsersNeverPanic(f *testing.F) {
	for _, s := range []string{
		"", "$", "$saguin/consumer/seek", "$saguin/consumer//seek", "$saguin/consumer/seek/reply",
		"$saguin/queue/response", "$saguin/queue//response", "$saguin/consumer/events/seek",
		"$saguin/consumer/events/seek/reply", "$saguin/queue/jobs/response", "$saguin/consumer/",
		"/seek", "$saguin/queue/", "/response",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, topic string) {
		if name, ok := SeekChannel(topic); ok {
			if want := (&Channel{Name: name}).SeekTopic(); topic != want || name == "" || strings.Contains(name, "/") {
				t.Fatalf("SeekChannel(%q) = %q, but that channel's seek topic is %q", topic, name, want)
			}
		}
		if name, ok := SeekReplyChannel(topic); ok {
			if want := (&Channel{Name: name}).SeekReplyTopic(); topic != want {
				t.Fatalf("SeekReplyChannel(%q) = %q, but that channel's reply topic is %q", topic, name, want)
			}
		}
		if name, ok := ResponseChannel(topic); ok {
			if want := (&Channel{Name: name}).ResponseTopic(); topic != want || name == "" || strings.Contains(name, "/") {
				t.Fatalf("ResponseChannel(%q) = %q, but that queue's response topic is %q", topic, name, want)
			}
		}
	})
}
