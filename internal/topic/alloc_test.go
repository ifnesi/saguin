package topic_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
	"github.com/ifnesi/saguin/internal/topic"
)

// design.md, Testing: "The busy path is anything run per packet, per message
// or per connection. It does no needless work. It makes no second pass over
// the same data and avoids every copy or allocation it can." A Topic Name is
// checked on every PUBLISH, a filter on every SUBSCRIBE, and Match runs on
// every delivery: none of them allocates, on success or on refusal, however
// long the string. SplitShared returns parts of its input, never copies.

// limited is a string and the limits it is checked against.
type limited struct {
	s   string
	lim topic.Limits
}

// checkNoAllocs fails the test if check, named what, allocates accepting
// each of valid under generous limits, or refusing each of refused under
// its own limits; or if it refuses one of valid, or accepts one of refused.
func checkNoAllocs(t *testing.T, what string, check func(string, topic.Limits) error, valid []string, refused []limited) {
	t.Helper()
	for _, s := range valid {
		testkit.NoAllocs(t, fmt.Sprintf("%s accepting %.32q", what, s), func() {
			if err := check(s, generous); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, c := range refused {
		testkit.NoAllocs(t, fmt.Sprintf("%s refusing %.32q", what, c.s), func() {
			if check(c.s, c.lim) == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestNoAllocations(t *testing.T) {
	long := strings.Repeat("level/", 199) + "end"
	longFilter := strings.Repeat("+/", 150) + strings.Repeat("level/", 49) + "#"
	small := topic.Limits{MaxLength: 5, MaxLevels: 2}
	checkNoAllocs(t, "CheckName", topic.CheckName,
		[]string{"sport/tennis/player1", "$SYS/monitor/Clients", long},
		[]limited{{"", defaults}, {"sport/tennis/#", defaults}, {long + "+", defaults}, {"a/b/c", small}, {"abcdef", small}})
	checkNoAllocs(t, "CheckFilter", topic.CheckFilter,
		[]string{"sport/tennis/+", "#", "+/monitor/Clients", longFilter},
		[]limited{{"", defaults}, {"sport/tennis#", defaults}, {longFilter + "/x", defaults}, {"+/+/#", small}, {"abcdef", small}})
	for _, s := range []string{"$share/consumer1/sport/tennis/+", "sport/tennis/+", "$share/" + long + "/x"} {
		testkit.NoAllocs(t, fmt.Sprintf("SplitShared on %.32q", s), func() {
			if _, _, err := topic.SplitShared(s); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, s := range []string{"$share/", "$share/g", "$share/+/x"} {
		testkit.NoAllocs(t, fmt.Sprintf("SplitShared refusing %q", s), func() {
			if _, _, err := topic.SplitShared(s); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	for _, c := range []matchCase{
		{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true},
		{"sport/tennis/+", "sport/tennis/player1/ranking", false},
		{"#", "$SYS/monitor/Clients", false},
		{"$SYS/monitor/+", "$SYS/monitor/Clients", true},
		{longFilter, long, true},
		{long, long, true},
		{long, long + "x", false},
	} {
		testkit.NoAllocs(t, fmt.Sprintf("Match(%.32q, %.32q)", c.filter, c.name), func() {
			if topic.Match(c.filter, c.name) != c.match {
				t.Fatalf("Match(%.40q, %.40q) != %v", c.filter, c.name, c.match)
			}
		})
	}
}
