package authz

import "testing"

// **patternsCanCollide against brute force, over every pair of patterns up
// to four characters from `a`, `b` and `*`.** Both match some string
// exactly when some string over {a, b} of at most eight bytes matches both:
// a `*` facing a literal can supply that literal, and one facing a `*` can
// be empty, so a shortest common string is no longer than the two patterns'
// literals together. Exhaustive rather than random, so there is no seed to
// print, and the report's pair - `ab*c` against `a*bc`, here `ab*b` against
// `a*bb` - is among them.
//
// Both answers are counted, because a sweep that found no collision at all,
// or nothing but collisions, would pass an implementation answering one
// constant.
func TestPatternsCollideExactlyWhenSomeIDMatchesBoth(t *testing.T) {
	var patterns []string
	var grow func(string)
	grow = func(p string) {
		patterns = append(patterns, p)
		if len(p) == 4 {
			return
		}
		for _, c := range "ab*" {
			grow(p + string(c))
		}
	}
	grow("")

	var ids []string
	var spell func(string)
	spell = func(s string) {
		ids = append(ids, s)
		if len(s) == 8 {
			return
		}
		spell(s + "a")
		spell(s + "b")
	}
	spell("")

	var pairs, collide, apart int
	for _, a := range patterns {
		for _, b := range patterns {
			pairs++
			want := false
			for _, id := range ids {
				if matchPattern(a, id) && matchPattern(b, id) {
					want = true
					break
				}
			}
			if got := patternsCanCollide(a, b); got != want {
				t.Errorf("patternsCanCollide(%q, %q) = %v; brute force over every id up "+
					"to eight bytes says %v", a, b, got, want)
			}
			if want {
				collide++
			} else {
				apart++
			}
		}
	}
	t.Logf("%d pairs of %d patterns against %d ids: %d collide, %d do not",
		pairs, len(patterns), len(ids), collide, apart)
	if pairs != 121*121 || collide == 0 || apart == 0 {
		t.Fatalf("the sweep examined %d pairs (%d colliding, %d apart), want 14641 with "+
			"both answers present", pairs, collide, apart)
	}
}

// substitute, driven directly: what each filter comes to for one identity and
// client id, the one-pass rule both ways round, and the syntax refusal. The
// expectations are the acl_file rules RFC 0002 states, not this function's
// arithmetic.
func TestSubstituteReplacesEachPlaceholderOnce(t *testing.T) {
	for _, tc := range []struct {
		what, filter, identity, clientID, want string
		ok                                     bool
	}{
		{"a literal filter", "events/telemetry/#", "vessel-7", "north-17", "events/telemetry/#", true},
		{"%u", "events/%u/#", "vessel-7", "north-17", "events/vessel-7/#", true},
		{"%c", "iot/health/%c", "vessel-7", "north-17", "iot/health/north-17", true},
		{"both", "fleet/%u/%c", "vessel-7", "north-17", "fleet/vessel-7/north-17", true},
		{"%c with no client id stands", "iot/health/%c", "vessel-7", "", "iot/health/%c", true},
		{"an identity holding %c is put in as written", "iot/%u/status", "%c", "north-17", "iot/%c/status", true},
		{"a client id holding %u is put in as written", "iot/%c/status", "vessel-7", "%u", "iot/%u/status", true},
		{"a doubled percent", "a/%%u", "vessel-7", "", "a/%vessel-7", true},
		{"a trailing percent", "a/%", "vessel-7", "north-17", "a/%", true},
		{"an identity holding a wildcard", "events/%u/#", "#", "north-17", "", false},
		{"a client id holding a separator", "iot/%c", "vessel-7", "a/b", "", false},
		{"a wildcard in a name the filter does not use", "events/telemetry/#", "#", "a/b", "events/telemetry/#", true},
	} {
		got, ok := substitute(tc.filter, tc.identity, tc.clientID)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: substitute(%q, %q, %q) = %q, %v; want %q, %v",
				tc.what, tc.filter, tc.identity, tc.clientID, got, ok, tc.want, tc.ok)
		}
	}
}

// A filter naming neither placeholder costs nothing: it is on every
// permission check, once per rule.
func TestALiteralFilterIsNotCopied(t *testing.T) {
	if n := testing.AllocsPerRun(1000, func() {
		_, _ = substitute("events/telemetry/#", "vessel-7", "north-17")
	}); n != 0 {
		t.Errorf("a filter with no placeholder allocated %.0f times a call, want 0: every permission "+
			"check pays it once per rule", n)
	}
}
