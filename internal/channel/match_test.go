package channel

import "testing"

// RFC 0002 "What a filter reaches"
//
// Topic filter matching, which every delivery decision rests on: pumpAll
// asks it whether a subscriber can want a record before doing any work for
// that subscriber, and pumpBatch asks it again per record.
//
// A table rather than an end-to-end test, because the cases that matter are
// the ones nobody publishes by accident - a filter longer than the topic, a
// trailing wildcard standing in for nothing, an empty level.
func TestFilterMatching(t *testing.T) {
	for _, tc := range []struct {
		filter, topic string
		want          bool
	}{
		{"events/a", "events/a", true},
		{"events/a", "events/b", false},
		{"events/#", "events/a", true},
		{"events/#", "events/a/b/c", true},
		{"events/+", "events/a", true},
		{"events/+", "events/a/b", false},
		{"events/+/temp", "events/device/temp", true},
		{"events/+/temp", "events/device/humidity", false},
		{"events/device/+/temp", "events/device/7/temp", true},

		// A trailing # stands in for nothing at all: MQTT 5 section 4.7.1.2
		// makes "sport/#" match "sport" as well as everything under it.
		{"events/#", "events", true},

		// The filter runs out, or the topic does.
		{"events/a", "events/a/b", false},
		{"events/a/b", "events/a", false},
		{"events/+/+", "events/a", false},

		// An empty level is a level. "a//b" has three of them.
		{"events/+/b", "events//b", true},
		{"events/a", "events/a/", false},
	} {
		t.Run(tc.filter+"|"+tc.topic, func(t *testing.T) {
			if got := Matches(tc.filter, tc.topic); got != tc.want {
				t.Fatalf("Matches(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
			}
		})
	}
}

// BenchmarkMatches exists because a figure for this function - "about
// 140ns a call" - was quoted in prose, and where an index would start
// earning its keep was decided from it, and nothing measured it. A number nobody can re-run is
// a number nobody can correct.
//
// The shapes are the ones a channel actually holds. A whole-channel filter
// is what most consumers subscribe with; the deep literal is the worst case
// for a walk, since it compares every level; the miss at the first level is
// the cheapest and the commonest on a channel with many narrow subscribers,
// because it is the answer for all but one of them.
func BenchmarkMatches(b *testing.B) {
	for _, tc := range []struct{ name, filter, topic string }{
		{"whole channel", "events/#", "events/device/17/temperature"},
		{"one level wild", "events/+/17/temperature", "events/device/17/temperature"},
		{"deep literal", "events/device/17/temperature", "events/device/17/temperature"},
		{"miss at the first level", "events/device/18/#", "events/device/17/temperature"},
		{"miss at the last", "events/device/17/humidity", "events/device/17/temperature"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = Matches(tc.filter, tc.topic)
			}
		})
	}
}
