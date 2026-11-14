package topic_test

import (
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
	"github.com/ifnesi/saguin/internal/topic"
)

// Match reports whether a Topic Filter matches a Topic Name, both already
// checked (CheckFilter, CheckName); a shared subscription matches with its
// filter part alone: "The $share and {ShareName} portions of the Topic
// Filter are not taken into account when matching against publications"
// (MQTT 5 4.8.2).

// matchOracle restates 4.7's matching: `#` "matches any number of levels
// within a topic", "the parent and any number of child levels" (4.7.1.2);
// `+` "matches only one topic level" (4.7.1.3); "Each non-wildcarded level
// in the Topic Filter has to match the corresponding level in the Topic Name
// character for character" (4.7.3); and "The Server MUST NOT match Topic
// Filters starting with a wildcard character (# or +) with Topic Names
// beginning with a $ character" [MQTT-4.7.2-1].
func matchOracle(filter, name string) bool {
	if (filter[0] == '#' || filter[0] == '+') && name[0] == '$' {
		return false
	}
	filterLevels, nameLevels := strings.Split(filter, "/"), strings.Split(name, "/")
	for i, level := range filterLevels {
		if level == "#" {
			return true
		}
		if i >= len(nameLevels) || (level != "+" && level != nameLevels[i]) {
			return false
		}
	}
	return len(filterLevels) == len(nameLevels)
}

type matchCase struct {
	filter, name string
	match        bool
}

// matchCases are the standard's own examples first, then the edges its rules
// decide: FuzzMatch's seeds, against which it holds its oracle. Match's own
// answers to the standard are checked on the wire, where a subscriber sees
// them (conformance: TestOneCopyCarriesEveryContributingIdentifier).
var matchCases = []matchCase{
	// 4.7.1.2: "if a Client subscribes to “sport/tennis/player1/#”, it
	// would receive messages published using these Topic Names".
	{"sport/tennis/player1/#", "sport/tennis/player1", true},
	{"sport/tennis/player1/#", "sport/tennis/player1/ranking", true},
	{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true},
	// 4.7.1.2: "“sport/#” also matches the singular “sport”, since #
	// includes the parent level"; "“#” is valid and will receive every
	// Application Message".
	{"sport/#", "sport", true},
	{"#", "sport", true},
	{"#", "sport/tennis/player1", true},
	{"#", "/", true},
	{"#", "/finance", true},
	// 4.7.1.3: "“sport/tennis/+” matches “sport/tennis/player1” and
	// “sport/tennis/player2”, but not “sport/tennis/player1/ranking”";
	// "“sport/+” does not match “sport” but it does match “sport/”";
	// "“/finance” matches “+/+” and “/+”, but not “+”".
	{"sport/tennis/+", "sport/tennis/player1", true},
	{"sport/tennis/+", "sport/tennis/player2", true},
	{"sport/tennis/+", "sport/tennis/player1/ranking", false},
	{"sport/+", "sport", false},
	{"sport/+", "sport/", true},
	{"+/+", "/finance", true},
	{"/+", "/finance", true},
	{"+", "/finance", false},
	// 4.7.2: "A subscription to “#” will not receive any messages published
	// to a topic beginning with a $"; "A subscription to
	// “+/monitor/Clients” will not receive any messages published to
	// “$SYS/monitor/Clients”"; "A subscription to “$SYS/#” will receive
	// messages published to topics beginning with “$SYS/”"; "A subscription
	// to “$SYS/monitor/+” will receive messages published to
	// “$SYS/monitor/Clients”" [MQTT-4.7.2-1].
	{"#", "$SYS/monitor/Clients", false},
	{"#", "$SYS", false},
	{"#", "$", false},
	{"#", "$saguin/kv/get", false},
	{"+/monitor/Clients", "$SYS/monitor/Clients", false},
	{"+", "$SYS", false},
	{"+/#", "$SYS/x", false},
	{"$SYS/#", "$SYS/monitor/Clients", true},
	{"$SYS/#", "$SYS/", true},
	{"$SYS/#", "$SYS", true},
	{"$SYS/monitor/+", "$SYS/monitor/Clients", true},
	{"$SYS/monitor/Clients", "$SYS/monitor/Clients", true},
	{"$", "$", true},
	// [MQTT-4.7.2-1] is about Topic Names "beginning with a $": a `$`
	// further in is an ordinary character.
	{"a/+", "a/$x", true},
	{"a/#", "a/$SYS/x", true},
	{"+/$x", "a/$x", true},
	// 4.7.3: "Topic Names and Topic Filters are case sensitive"; "“ACCOUNTS”
	// and “Accounts” are two different Topic Names"; "“Accounts payable”
	// is a valid Topic Name"; "“/finance” is different from “finance”"; "A
	// leading or trailing ‘/’ creates a distinct Topic Name or Topic
	// Filter".
	{"ACCOUNTS", "Accounts", false},
	{"Accounts", "Accounts", true},
	{"Accounts payable", "Accounts payable", true},
	{"/finance", "finance", false},
	{"finance", "/finance", false},
	{"sport/tennis", "sport/tennis/", false},
	{"sport/tennis/", "sport/tennis", false},
	{"/", "/", true},
	// "the Server MUST NOT perform any normalization of Topic Names or
	// Topic Filters" [MQTT-4.7.3-4]: "é" composed and decomposed are two
	// topics.
	{"café", "café", false},
	{"café", "café", true},
	// 4.7.1.1: "Adjacent Topic level separators indicate a zero-length
	// topic level", which `+` matches like any level, and `#` with its
	// parent.
	{"a/+/b", "a//b", true},
	{"+/+", "/", true},
	{"+", "/", false},
	{"/#", "/", true},
	{"/#", "/finance", true},
	{"sport/#", "sport/", true},
	{"+/#", "sport", true},
	{"a//b", "a//b", true},
	{"a//b", "a/b", false},
	// A level matches character for character, the whole level: a written
	// level that is a prefix of the name's level, or the reverse, does not
	// match.
	{"sport/tennis", "sport/tennisx", false},
	{"sport/tennisx", "sport/tennis", false},
	{"sport/ten", "sport/tennis", false},
	{"sport/+/x", "sport/a/xy", false},
	{"a/b/#", "a/bc", false},
	{"a/b/#", "a", false},
	{"a/#", "ab", false},
	{"a/#", "ab/c", false},
	// A filter with no wildcard matches only the same number of levels.
	{"sport/tennis", "sport/tennis/player1", false},
	{"sport/tennis/player1", "sport/tennis", false},
	{"+/+", "a", false},
	{"+", "a/b", false},
	{"a/+/c", "a/c", false},
	{"a/+/c", "a/b/c/d", false},
	{"+/+/+", "a/b/c", true},
	{"+/tennis/#", "sport/tennis", true},
	{"+/tennis/#", "sport/tennis/player1/ranking", true},
	{"+/tennis/#", "sport/squash", false},
	{"sport/+/player1", "sport/tennis/player1", true},
	{"sport/+/player1", "sport/tennis/player2", false},
	{"日本/+", "日本/東京", true},
}

// FuzzMatch first holds matchOracle to matchCases, then compares Match
// with it for any valid filter and name, and checks three consequences of 4.7's rules on filters built from the
// name itself, which random pairs rarely reach:
//   - a name matches the filter equal to itself, a valid name having no
//     wildcard to misplace (4.7.3: each level matches "character for
//     character");
//   - `+` in place of any of its levels still matches, unless the filter
//     then starts with a wildcard and the name with `$` [MQTT-4.7.2-1];
//   - its first k levels followed by `#` match, `#` taking "the parent and
//     any number of child levels" (4.7.1.2), again but for [MQTT-4.7.2-1];
//     `#` alone matching exactly the names not starting with `$`.
func FuzzMatch(f *testing.F) {
	for _, c := range matchCases {
		if got := matchOracle(c.filter, c.name); got != c.match {
			f.Fatalf("matchOracle(%q, %q) = %v, want %v; the oracle is wrong", c.filter, c.name, got, c.match)
		}
		f.Add(c.filter, c.name, uint64(0))
	}
	f.Add("#", "sport/tennis", uint64(1)<<63)
	f.Add("+", "$SYS/monitor/Clients", uint64(0b101))
	f.Add("+", "a/b/c/d", uint64(0x0300_0000_0000_000A))
	f.Fuzz(func(t *testing.T, filter, name string, mask uint64) {
		if !testkit.UTF8StringOK(name) || !testkit.TopicNameOK(name) {
			return
		}
		dollar := name[0] == '$'
		check := func(filter string, want bool) {
			t.Helper()
			if got := topic.Match(filter, name); got != want {
				t.Fatalf("Match(%q, %q) = %v, want %v", filter, name, got, want)
			}
		}
		if testkit.UTF8StringOK(filter) && testkit.TopicFilterOK(filter) {
			check(filter, matchOracle(filter, name))
		}
		check(name, true)
		check("#", !dollar)

		levels := strings.Split(name, "/")
		plus := make([]string, len(levels))
		for i, level := range levels {
			plus[i] = level
			if mask&(1<<(i%63)) != 0 {
				plus[i] = "+"
			}
		}
		withPlus := strings.Join(plus, "/")
		check(withPlus, !dollar || withPlus[0] != '+')

		k := int(mask>>56) % (len(levels) + 1)
		withHash := strings.Join(append(plus[:k:k], "#"), "/")
		check(withHash, !dollar || (withHash[0] != '+' && withHash[0] != '#'))
	})
}
