package topic_test

import (
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
	"github.com/ifnesi/saguin/internal/topic"
)

// MQTT 5 4.8.2: a Shared Subscription's filter is `$share/{ShareName}/{filter}`.
// "A Shared Subscription's Topic Filter MUST start with $share/ and MUST
// contain a ShareName that is at least one character long [MQTT-4.8.2-1].
// The ShareName MUST NOT contain the characters "/", "+" or "#", but MUST be
// followed by a "/" character. This "/" character MUST be followed by a
// Topic Filter [MQTT-4.8.2-2] as described in section 4.7."
//
// SplitShared checks the `$share/` form and its ShareName, and returns the
// group and the rest of the string, unchecked; the caller checks the rest
// with CheckFilter, so no byte is checked twice. A string not starting with
// `$share/` is not a shared subscription: SplitShared returns an empty group
// and the string whole. Refusing `$share` to MQTT 3.1.1 is the SUBSCRIBE
// handler's (design, SUBSCRIBE table), not this package's.

// sharedOracle restates 4.8.2's split: the group, the rest, and whether the
// `$share/` form and its ShareName are well-formed.
func sharedOracle(s string) (group, rest string, ok bool) {
	after, found := strings.CutPrefix(s, "$share/")
	if !found {
		return "", s, true
	}
	group, rest, found = strings.Cut(after, "/")
	if !found || group == "" || strings.ContainsAny(group, "+#") {
		return "", "", false
	}
	return group, rest, true
}

// FuzzSplitShared compares SplitShared with 4.8.2's split, and the whole
// subscription check with the oracles.
func FuzzSplitShared(f *testing.F) {
	for _, s := range []string{
		"$share/consumer1/sport/tennis/+", "$share/consumer1//finance", "$share/g/",
		"$share", "$share/", "$share//x", "$share/g", "$share/+/x", "$share/a#b/x",
		"$SHARE/g/x", "sport/tennis/+", "$share/g/sport/tennis#",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if !testkit.UTF8StringOK(s) {
			return
		}
		wantGroup, wantRest, ok := sharedOracle(s)
		group, rest, err := topic.SplitShared(s)
		if !ok {
			if !is(err, topic.ErrMalformed) {
				t.Fatalf("SplitShared(%q) = %q, %q, %v; want ErrMalformed", s, group, rest, err)
			}
			return
		}
		if group != wantGroup || rest != wantRest || err != nil {
			t.Fatalf("SplitShared(%q) = %q, %q, %v; want %q, %q, nil", s, group, rest, err, wantGroup, wantRest)
		}
		if w := want(testkit.TopicFilterOK(rest), rest, generous); !is(topic.CheckFilter(rest, generous), w) {
			t.Fatalf("CheckFilter(%q), the filter of %q, is not %v", rest, s, w)
		}
	})
}
