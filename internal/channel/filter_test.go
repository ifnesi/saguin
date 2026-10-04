package channel

import (
	"strings"
	"testing"
)

// RFC 0002 "Which channel a topic belongs to"
//
// The ordering that decides which of two channels holds a topic. A table
// rather than an end-to-end test because the cases that matter are the
// pairs nobody writes on purpose - a filter that ends against one carrying
// `#` at the same level, and `+` against a spelled-out level under it.
//
// **Every row asserts both directions**, because an ordering that is not
// antisymmetric sorts differently depending on which pair the sort compares
// first, and that is exactly the map-iteration failure invariant 12 exists
// to prevent.
func TestMoreExactOrdersTwoFilters(t *testing.T) {
	for _, tc := range []struct {
		more, less string
		why        string
	}{
		{"iot/water/+/location/#", "iot/water/+/+/#", "spelled-out level beats +"},
		{"iot/water/w-7/#", "iot/water/+/#", "spelled out beats + at level 3"},
		{"iot/water/+/#", "iot/water/#", "+ beats #"},
		{"iot/water/+/work", "iot/water/+/work/#", "a filter that ends beats one carrying #"},
		{"iot/water/+/work/__dlq/#", "iot/water/+/work/#",
			"the derived dead-letter filter beats the queue's own"},
		{"__dlq/#", "#", "and does so for a queue whose filter is # alone"},
		{"jobs/__dlq/#", "jobs/#", "and for the default filter a name gives"},
		{"a/b/c", "a/b/#", "the first level at which they differ decides"},
		{"a/b/#", "a/+/c", "and it is the first, not the most specific anywhere"},
	} {
		if got := MoreExact(tc.more, tc.less); got <= 0 {
			t.Errorf("MoreExact(%q, %q) = %d, want > 0 - %s", tc.more, tc.less, got, tc.why)
		}
		if got := MoreExact(tc.less, tc.more); got >= 0 {
			t.Errorf("MoreExact(%q, %q) = %d, want < 0 - %s reversed",
				tc.less, tc.more, got, tc.why)
		}
	}

	// Zero is reachable only for two identical filters, which is the claim
	// that lets the registry refuse those and break no other tie.
	for _, f := range []string{"iot/water/+/+/#", "a", "#", "a/b/c", "a/+/#"} {
		if got := MoreExact(f, f); got != 0 {
			t.Errorf("MoreExact(%q, %q) = %d, want 0", f, f, got)
		}
	}
}

// RFC 0002 "What a filter reaches"
//
// Whether a subscription filter and a channel's filter can both match some
// topic, which is how a subscriber finds the channels it is served from.
func TestIntersects(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"iot/water/w-7/#", "iot/water/+/location/#", true},
		{"iot/water/w-7/#", "iot/water/+/+/#", true},
		{"#", "iot/water/+/+/#", true},
		{"iot/water/w-7/location", "iot/water/+/location/#", true},
		{"iot/weather/#", "iot/water/+/+/#", false},
		{"iot/water/w-8/#", "iot/water/w-7/+", false},

		// A `#` stands in for nothing, so a filter that has run out still
		// meets one whose only remainder is `#` (MQTT 5 section 4.7.1.2).
		{"iot/water/w-7/location", "iot/water/+/location/#", true},
		{"a/b", "a/b/#", true},
		{"a/b", "a/b/c/#", false},
		{"a/b/c", "a/b", false},

		// Exact depth: a filter with no `#` matches its own length only.
		{"iot/weather/+/data/raw", "iot/weather/+/{data,events}", false},
	} {
		if got := Intersects(tc.a, tc.b); got != tc.want {
			t.Errorf("Intersects(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := Intersects(tc.b, tc.a); got != tc.want {
			t.Errorf("Intersects(%q, %q) = %v, want %v - it must be symmetric",
				tc.b, tc.a, got, tc.want)
		}
	}
}

// RFC 0002 the `queue` section
//
// Whether every topic a subscription filter can match lies inside a
// channel's filter - the question that separates a worker spelling its
// queue wrongly (refused 0x8F) from a filter that merely crosses the queue
// and is granted everything else.
func TestContains(t *testing.T) {
	const queue = "iot/water/+/inspect"
	for _, tc := range []struct {
		outer, inner string
		want         bool
	}{
		{queue, queue, true},
		{queue, "iot/water/w-7/inspect", true},  // narrower: asking for part of the queue
		{queue, "iot/water/+/inspect/#", false}, // wider
		{queue, "#", false},                     // merely crosses it
		{queue, "iot/#", false},                 // merely crosses it
		{queue, "iot/water/+/+", false},

		// The dead-letter filter lies inside a queue whose filter ends in
		// `#`, and both contain the reader's filter - which is why the
		// registry takes the MORE EXACT of the two and the reader is not
		// refused as a worker.
		{"iot/water/+/inspect/#", "iot/water/+/inspect/__dlq", true},
		{"iot/water/+/inspect/__dlq/#", "iot/water/+/inspect/__dlq", true},

		{"a/#", "a/b/c", true},
		{"a/+", "a/#", false},
		{"a/+", "a/b", true},
		{"a/b", "a/+", false},
	} {
		if got := Contains(tc.outer, tc.inner); got != tc.want {
			t.Errorf("Contains(%q, %q) = %v, want %v", tc.outer, tc.inner, got, tc.want)
		}
	}
}

// RFC 0002 "`{a,b}`: one level, several spellings"
func TestExpand(t *testing.T) {
	for _, tc := range []struct {
		filter string
		want   []string
	}{
		{"iot/water/+/+/#", []string{"iot/water/+/+/#"}},
		{"iot/weather/+/{data,events}", []string{"iot/weather/+/data", "iot/weather/+/events"}},
		{"iot/{water,air}/+/{data,events}", []string{
			"iot/water/+/data", "iot/water/+/events",
			"iot/air/+/data", "iot/air/+/events",
		}},
		{"{a}/b", []string{"a/b"}},
	} {
		got, err := Expand(tc.filter)
		if err != nil {
			t.Errorf("Expand(%q): %v", tc.filter, err)
			continue
		}
		if strings.Join(got, " ") != strings.Join(tc.want, " ") {
			t.Errorf("Expand(%q) = %v, want %v", tc.filter, got, tc.want)
		}
	}

	for _, bad := range []string{
		"x{a,b}/c",    // a brace is a whole level
		"{a,}/b",      // empty alternative
		"{}/b",        // no alternatives
		"{a,{b,c}}/d", // no nesting
		"{a,+}/b",     // a wildcard takes a whole level
		"{a,a}/b",     // an alternative written twice
	} {
		if got, err := Expand(bad); err == nil {
			t.Errorf("Expand(%q) = %v, want an error", bad, got)
		}
	}
	// A repeated alternative is named for what it is, not as the two filters
	// it expands to claiming one channel twice.
	if _, err := Expand("iot/{a,a}/events/#"); err == nil || !strings.Contains(err.Error(), `alternative "a" is written twice`) {
		t.Errorf("a repeated alternative was refused as %v", err)
	}
}

// RFC 0002 "Validation"
func TestValidFilter(t *testing.T) {
	for _, good := range []string{
		"iot/water/+/location/#", "iot/water/+/+/#", "jobs/#", "a", "a/#", "iot/+/+",
	} {
		if err := ValidFilter(good); err != nil {
			t.Errorf("ValidFilter(%q) = %v, want nil", good, err)
		}
	}
	for _, bad := range []string{
		"",             // empty
		"iot/#/water",  // # is not the last level
		"+/water/#",    // the first level is spelled out
		"#",            // same
		"$SYS/#",       // the $ space is the broker's own
		"iot/wa+ter/#", // a wildcard takes a whole level
		"iot/wa#ter",   // same
	} {
		if err := ValidFilter(bad); err == nil {
			t.Errorf("ValidFilter(%q) = nil, want an error", bad)
		}
	}

	if err := NoDLQLevel("iot/water/+/__dlq/#"); err == nil {
		t.Error("NoDLQLevel accepted an operator-written __dlq level")
	}
	if err := NoDLQLevel("iot/water/+/+/#"); err != nil {
		t.Errorf("NoDLQLevel(%q) = %v, want nil", "iot/water/+/+/#", err)
	}
}

// RFC 0002 "The dead-letter channel"
//
// One rule for every shape of filter: a `__dlq` level inserted where the
// queue's filter carries its `#`, or appended where it carries none.
//
// **Each row also asserts that the rewritten topic matches the derived
// filter**, because the two are separate functions and a dead letter whose
// topic does not match its own channel's filter is a record no subscriber
// can reach - stored, counted, and invisible.
func TestDeadLetterDerivation(t *testing.T) {
	for _, tc := range []struct {
		queue, topic    string
		filter, rewrote string
	}{
		{"iot/water/+/inspect", "iot/water/w-7/inspect",
			"iot/water/+/inspect/__dlq", "iot/water/w-7/inspect/__dlq"},
		{"iot/water/+/+", "iot/water/w-7/inspect",
			"iot/water/+/+/__dlq", "iot/water/w-7/inspect/__dlq"},
		{"iot/water/+/inspect/#", "iot/water/w-7/inspect/x",
			"iot/water/+/inspect/__dlq/#", "iot/water/w-7/inspect/__dlq/x"},
		{"iot/water/+/inspect/#", "iot/water/w-7/inspect",
			"iot/water/+/inspect/__dlq/#", "iot/water/w-7/inspect/__dlq"},
		{"jobs/#", "jobs/a/b", "jobs/__dlq/#", "jobs/__dlq/a/b"},
		{"#", "a/b", "__dlq/#", "__dlq/a/b"},
	} {
		if got := DLQFilter(tc.queue); got != tc.filter {
			t.Errorf("DLQFilter(%q) = %q, want %q", tc.queue, got, tc.filter)
		}
		got := DLQTopic(tc.queue, tc.topic)
		if got != tc.rewrote {
			t.Errorf("DLQTopic(%q, %q) = %q, want %q", tc.queue, tc.topic, got, tc.rewrote)
		}
		if !Matches(tc.filter, got) {
			t.Errorf("the rewritten topic %q does not match the derived filter %q, so no "+
				"subscriber could reach the dead letter", got, tc.filter)
		}
		if !Matches(tc.queue, tc.topic) {
			t.Fatalf("the row is wrong: %q does not match the queue filter %q",
				tc.topic, tc.queue)
		}
		// **The queue must not hold its own dead letters**, which takes one
		// of two things: either the queue's filter does not reach the
		// rewritten topic at all - the exact-depth shapes, disjoint by
		// construction - or the derived filter is the more exact of the two
		// and wins the topic. Asserting only the second would be wrong: for
		// a queue that ends at a level the derived filter runs one past, the
		// queue's filter is the more exact and the two never meet over any
		// topic, so the ordering between them decides nothing.
		if Matches(tc.queue, got) && MoreExact(tc.filter, tc.queue) <= 0 {
			t.Errorf("the queue filter %q reaches the dead letter at %q and is not less "+
				"exact than the derived %q, so the queue would hold its own dead letters",
				tc.queue, got, tc.filter)
		}
	}
}

// Invariant 12
//
// **The routing table is decided by the filters and never by a map walk.**
// The registry builds its routes by ranging over a map, and Go randomises
// that order per run, so anything about the table that a map order can
// reach is a thing that changes between two identical brokers.
//
// It cannot change *resolution*, because two filters that score alike at
// every level and both match one topic are the same filter and are refused
// at startup. What it reached was the printed order: `saguin --route`
// prints this table, and before the tie-break went in it printed a
// different one on each run - noise in the one output an operator diffs
// when they are trying to find out why a topic went somewhere.
//
// Two registries rather than one built twice, because the failure is
// between processes as much as within one.
func TestTheRoutingTableDoesNotDependOnAMapWalk(t *testing.T) {
	build := func() []string {
		chans := []*Channel{
			{Name: "water-location", Type: Latest, Filter: "iot/water/+/location/#"},
			{Name: "water-measurement", Type: Append, Filter: "iot/water/+/+/#"},
			{Name: "weather-measurement", Type: Append, Filter: "iot/weather/+/{data,events}"},
			{Name: "work", Type: Queue, Filter: "iot/water/+/inspect"},
			{Name: "sea", Type: Append, Filter: "iot/sea/+/depth"},
			{Name: "air", Type: Latest, Filter: "iot/air/+/status"},
		}
		r, err := NewRegistry(chans)
		if err != nil {
			t.Fatalf("registry: %v", err)
		}
		filters, _ := r.Routes()
		return filters
	}

	first := build()
	if len(first) < 6 {
		t.Fatalf("the table holds %d routes, so this proves very little: %v", len(first), first)
	}
	for range 20 {
		if got := build(); strings.Join(got, " ") != strings.Join(first, " ") {
			t.Fatalf("two registries built from the same channels ordered their routes\n"+
				"differently, so the order depends on a map walk:\n  %v\n  %v", first, got)
		}
	}
}
