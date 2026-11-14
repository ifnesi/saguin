package topic_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
	"github.com/ifnesi/saguin/internal/topic"
)

// Package topic checks Topic Names and Topic Filters and matches one against
// the other (MQTT 5 4.7 and 4.8.2; MQTT 3.1.1 4.7). It is pure code: no
// state, no goroutine. Its limits come from the caller, as config.md's
// `broker.limits` sets them: `max_topic_length`, "the longest topic or
// filter, in bytes; 1 to 65,535", and `max_topic_levels`, "levels in any
// topic or filter".
//
// The input is a UTF-8 Encoded String the packet decoder has already
// checked: well-formed UTF-8 [MQTT-1.5.4-1] with no U+0000 [MQTT-1.5.4-2,
// MQTT-4.7.3-2] and at most 65,535 bytes [MQTT-4.7.3-3]. The busy path makes
// no second pass over the same data, so this package does not check UTF-8
// again; no test here gives it a string the decoder would refuse.
//
// Outcomes, and the design's reason code each serves:
//
//   - ErrMalformed: the string breaks MQTT's own syntax (4.7, and 4.8.2 for
//     `$share/`). For SUBSCRIBE, "A malformed filter is a malformed packet,
//     with 0x81" (design, Flows, SUBSCRIBE); for a Will topic, "0x90 a Will
//     topic that is not a valid topic name" (design, CONNECT).
//   - ErrOverLimit: well-formed, but over Limits.MaxLength or
//     Limits.MaxLevels. For PUBLISH, "0x90 a topic over
//     limits.max_topic_length or limits.max_topic_levels" (design, PUBLISH);
//     for SUBSCRIBE, 0x8F for a filter "over limits.max_topic_length or
//     limits.max_topic_levels" (design, SUBSCRIBE).
//
// A string both malformed and over a limit is malformed: MQTT 5 3.9.3 gives
// 0x8F as "The Topic Filter is correctly formed but is not allowed for this
// Client", and 3.4.2.1 gives 0x90 as "The Topic Name is not malformed, but
// is not accepted by this Client or Server". A limit is only ever the reason
// for a string that is otherwise well-formed.

// generous is a limit nothing in these tables reaches: the largest
// max_topic_length config.md allows, and as many levels as such a string can
// hold.
var generous = topic.Limits{MaxLength: 65_535, MaxLevels: 32_768}

// defaults are config.md's defaults: `max_topic_length` 1024,
// `max_topic_levels` 200.
var defaults = topic.Limits{MaxLength: 1024, MaxLevels: 200}

// The oracles below restate 4.7's rules as plainly as possible, for the
// fuzz targets to compare against. They allocate freely: they are not the
// code under test.

// within reports whether s keeps to lim: its length in bytes, and its
// levels. "The topic level separator ... divides the Topic Name into
// multiple “topic levels”", and "Adjacent Topic level separators indicate a
// zero-length topic level" (4.7.1.1), so a string has one level more than it
// has separators.
func within(s string, lim topic.Limits) bool {
	levels := strings.Count(s, "/") + 1
	return len(s) <= lim.MaxLength && levels <= lim.MaxLevels
}

// want returns the outcome the oracles give for s: nil, ErrMalformed or
// ErrOverLimit, malformed first (this file's header).
func want(wellFormed bool, s string, lim topic.Limits) error {
	switch {
	case !wellFormed:
		return topic.ErrMalformed
	case !within(s, lim):
		return topic.ErrOverLimit
	}
	return nil
}

// is reports whether err is exactly the outcome w: nil for nil, else that
// sentinel and not the other, so a caller mapping it to a reason code
// cannot pick the wrong one.
func is(err, w error) bool {
	if w == nil {
		return err == nil
	}
	other := topic.ErrOverLimit
	if errors.Is(w, topic.ErrOverLimit) {
		other = topic.ErrMalformed
	}
	return errors.Is(err, w) && !errors.Is(err, other)
}

// validNames are Topic Names 4.7 allows.
var validNames = []string{
	// 4.7.1.2 and 4.7.1.3's examples of names.
	"sport/tennis/player1",
	"sport/tennis/player1/ranking",
	"sport/tennis/player1/score/wimbledon",
	"sport",
	"sport/",
	// 4.7.3: "A leading or trailing ‘/’ creates a distinct Topic Name";
	// "“/finance” is different from “finance”".
	"/finance",
	"finance",
	// 4.7.3: "A Topic Name or Topic Filter consisting only of the ‘/’
	// character is valid".
	"/",
	// 4.7.1.1: "Adjacent Topic level separators indicate a zero-length
	// topic level".
	"a//b",
	"//",
	// 4.7.3: "Topic Names and Topic Filters can include the space
	// character"; "“Accounts payable” is a valid Topic Name"; "“ACCOUNTS”
	// and “Accounts” are two different Topic Names".
	"Accounts payable",
	"ACCOUNTS",
	"Accounts",
	" ",
	// 4.7.2: "Server implementations MAY use Topic Names that start with a
	// leading $ character for other purposes"; its examples. A `$` topic is
	// a name like any other here.
	"$SYS/monitor/Clients",
	"$SYS",
	"$",
	"$saguin/kv/get",
	// Any UTF-8 the decoder passes: 4.7.3, "Topic Names and Topic Filters
	// are UTF-8 Encoded Strings".
	"capteur/température/é",
	"日本/東京",
	"\U0001F600/x",
}

// TestCheckNameValid: each of validNames passes.
func TestCheckNameValid(t *testing.T) {
	for _, name := range validNames {
		if err := topic.CheckName(name, generous); err != nil {
			t.Errorf("CheckName(%q) = %v, want nil", name, err)
		}
	}
}

// validFilters are Topic Filters 4.7 allows.
var validFilters = []string{
	// 4.7.1.2: "“#” is valid", "“sport/tennis/#” is valid", and the
	// examples' "sport/tennis/player1/#" and "sport/#".
	"#",
	"sport/tennis/#",
	"sport/tennis/player1/#",
	"sport/#",
	// 4.7.1.3: "“+” is valid", "“+/tennis/#” is valid", "“sport/+/player1”
	// is valid", and the examples' "sport/tennis/+", "sport/+", "+/+" and
	// "/+". "The single-level wildcard can be used at any level in the
	// Topic Filter, including first and last levels."
	"+",
	"+/tennis/#",
	"sport/+/player1",
	"sport/tennis/+",
	"sport/+",
	"+/+",
	"/+",
	"+/+/+",
	"+/#",
	// 4.7.1.2: `#` "following a topic level separator", including one that
	// follows an empty level.
	"/#",
	"//#",
	// 4.7.2's examples.
	"$SYS/#",
	"$SYS/monitor/+",
	"+/monitor/Clients",
	// A filter with no wildcard, and 4.7.3's shapes.
	"sport/tennis/player1",
	"/finance",
	"/",
	"//",
	"a//b",
	"Accounts payable",
	"capteur/température/+",
}

// TestCheckFilterValid: each of validFilters passes; and every valid name is
// also a valid filter, having no wildcard to misplace.
func TestCheckFilterValid(t *testing.T) {
	for _, filter := range slices.Concat(validFilters, validNames) {
		if err := topic.CheckFilter(filter, generous); err != nil {
			t.Errorf("CheckFilter(%q) = %v, want nil", filter, err)
		}
	}
}

// TestCheckFilterMalformed: at least one character [MQTT-4.7.3-1]; `#` "MUST
// be specified either on its own or following a topic level separator. In
// either case it MUST be the last character specified in the Topic Filter"
// [MQTT-4.7.1-1; MQTT 3.1.1: MQTT-4.7.1-2], with 4.7.1.2's "“sport/tennis#”
// is not valid" and "“sport/tennis/#/ranking” is not valid"; `+` "MUST
// occupy an entire level of the filter" [MQTT-4.7.1-2; MQTT 3.1.1:
// MQTT-4.7.1-3], with 4.7.1.3's "“sport+” is not valid".
func TestCheckFilterMalformed(t *testing.T) {
	for _, filter := range []string{
		"",
		// `#` misplaced.
		"sport/tennis#",
		"sport/tennis/#/ranking",
		"##",
		"#/",
		"#/a",
		"a#",
		"a/#/",
		"a/##",
		"a/#b",
		"#+",
		"+#",
		// `+` not a whole level.
		"sport+",
		"+a",
		"a+",
		"a/b+",
		"a/+b/c",
		"++",
		"a/++/b",
		"+/+a",
	} {
		if err := topic.CheckFilter(filter, generous); !is(err, topic.ErrMalformed) {
			t.Errorf("CheckFilter(%q) = %v, want ErrMalformed", filter, err)
		}
	}
}

// TestLimits: `max_topic_length` is "the longest topic or filter, in bytes",
// and `max_topic_levels` the "levels in any topic or filter" (config.md,
// `broker.limits`). Levels are 4.7.1.1's: the separator divides a topic into
// levels, and "Adjacent Topic level separators indicate a zero-length topic
// level", so a leading, trailing or doubled `/` adds a level. A name at a
// limit keeps to it; one byte or one level more is over it (design, PUBLISH
// 0x90). A filter's limits (SUBSCRIBE 0x8F) are checked from the wire
// (conformance, TestFilterOverTheTopicLimitsIsRefused).
func TestLimits(t *testing.T) {
	small := topic.Limits{MaxLength: 5, MaxLevels: 3}
	cases := []struct {
		s    string
		lim  topic.Limits
		want error
	}{
		{"abcde", small, nil},
		{"abcdef", small, topic.ErrOverLimit},
		// Bytes, not characters: "é" is two bytes.
		{"ééa", small, nil},
		{"ééé", small, topic.ErrOverLimit},
		{"a/b/c", small, nil},
		{"a/b/c/d", topic.Limits{MaxLength: 100, MaxLevels: 3}, topic.ErrOverLimit},
		{"//", small, nil},
		{"///", small, topic.ErrOverLimit},
		{"/", topic.Limits{MaxLength: 1, MaxLevels: 2}, nil},
		{"/", topic.Limits{MaxLength: 1, MaxLevels: 1}, topic.ErrOverLimit},
		{"a", topic.Limits{MaxLength: 1, MaxLevels: 1}, nil},
		// config.md's defaults, at the limit and one past it.
		{strings.Repeat("a", 1024), defaults, nil},
		{strings.Repeat("a", 1025), defaults, topic.ErrOverLimit},
		{strings.Repeat("a/", 199) + "a", defaults, nil},
		{strings.Repeat("a/", 200) + "a", defaults, topic.ErrOverLimit},
		{strings.Repeat("/", 199), defaults, nil},
		{strings.Repeat("/", 200), defaults, topic.ErrOverLimit},
		// The largest max_topic_length config.md allows, and the longest
		// string MQTT carries [MQTT-4.7.3-3].
		{strings.Repeat("a", 65_535), generous, nil},
		{strings.Repeat("a", 65_535), topic.Limits{MaxLength: 65_534, MaxLevels: 1}, topic.ErrOverLimit},
	}
	for _, c := range cases {
		if err := topic.CheckName(c.s, c.lim); !is(err, c.want) {
			t.Errorf("CheckName(%.40q, %+v) = %v, want %v", c.s, c.lim, err, c.want)
		}
	}
}

// TestMalformedBeforeLimit: a name both malformed and over a limit is
// malformed (this file's header: 0x90 is for strings "not malformed"). A
// malformed SUBSCRIBE filter over a limit ending the connection (0x81)
// rather than refusing that filter alone (0x8F) is checked from the wire
// (harness.BadPackets).
func TestMalformedBeforeLimit(t *testing.T) {
	lim := topic.Limits{MaxLength: 5, MaxLevels: 2}
	for _, name := range []string{"a/b/c/#", "abcdef+", "a/b/c/d/e/f/+"} {
		if err := topic.CheckName(name, lim); !is(err, topic.ErrMalformed) {
			t.Errorf("CheckName(%q, %+v) = %v, want ErrMalformed", name, lim, err)
		}
	}
}

// FuzzCheckName compares CheckName with the oracles, for any string the
// decoder passes and any limits config.md allows.
func FuzzCheckName(f *testing.F) {
	fuzzCheck(f, "CheckName", topic.CheckName, testkit.TopicNameOK, slices.Concat(
		atDefaults(validNames...),
		atDefaults("", "sport/tennis/#", "a+b"),
		[]fuzzSeed{{"abcdef", 5, 3}, {"ééé", 5, 3}, {"a/b/c/d", 100, 3}, {"a/b/c/#", 5, 2}},
	))
}

// FuzzCheckFilter compares CheckFilter with the oracles, for any string the
// decoder passes and any limits config.md allows.
func FuzzCheckFilter(f *testing.F) {
	fuzzCheck(f, "CheckFilter", topic.CheckFilter, testkit.TopicFilterOK, slices.Concat(
		atDefaults(validFilters...),
		atDefaults("", "sport/tennis#", "sport/tennis/#/ranking", "sport+", "a/+b/c"),
		[]fuzzSeed{{"+/+/+/#", 100, 3}, {"a/b/c/d#", 5, 2}},
	))
}

// fuzzSeed is a string a fuzz target starts from, with the limits it is
// checked against.
type fuzzSeed struct {
	s                    string
	maxLength, maxLevels uint16
}

// atDefaults returns a seed of each of ss at config.md's default limits.
func atDefaults(ss ...string) []fuzzSeed {
	seeds := make([]fuzzSeed, len(ss))
	for i, s := range ss {
		seeds[i] = fuzzSeed{s, uint16(defaults.MaxLength), uint16(defaults.MaxLevels)}
	}
	return seeds
}

// fuzzCheck is FuzzCheckName's and FuzzCheckFilter's body: from seeds, it
// compares check, named what, with the oracles, wellFormed among them, for
// any string the decoder passes and any limits config.md allows.
func fuzzCheck(f *testing.F, what string, check func(string, topic.Limits) error, wellFormed func(string) bool, seeds []fuzzSeed) {
	for _, seed := range seeds {
		f.Add(seed.s, seed.maxLength, seed.maxLevels)
	}
	f.Fuzz(func(t *testing.T, s string, maxLength, maxLevels uint16) {
		if !testkit.UTF8StringOK(s) || maxLength == 0 || maxLevels == 0 {
			return
		}
		lim := topic.Limits{MaxLength: int(maxLength), MaxLevels: int(maxLevels)}
		w := want(wellFormed(s), s, lim)
		if err := check(s, lim); !is(err, w) {
			t.Fatalf("%s(%q, %+v) = %v, want %v", what, s, lim, err, w)
		}
	})
}
