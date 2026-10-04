package broker

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// RFC 0003 "What a subscriber may declare" - the shape, generated
//
// **The rule is a shape, not a list of mistakes.** A table of the wrong
// spellings somebody thought of proves that those are refused and says
// nothing about the one nobody thought of, which is the one that gets
// through. So the space around the shape is produced here rather than
// written out, on the three axes a call has: what it is called, how many
// arguments it takes, and what an argument is made of.
//
// **The oracles are RFC 0003's own words**, never the parser's helpers and
// never strconv - `strconv.ParseInt` accepts a leading sign, which is
// exactly one of the questions being asked, so an oracle built on it would
// agree with any implementation that used it.
func TestOnlyTheShapeRFC0003StatesIsGranted(t *testing.T) {
	const name = "topic_hash"

	granted := func(val string) bool {
		p, why := partitioning(subscribeWith(val))
		if why == "" && !p.declared() {
			t.Fatalf("%q was granted and declared nothing: a saguin-filter is "+
				"present, so there is no third answer", val)
		}
		return why == ""
	}

	// **Every name one edit away from the right one**, which is the shape a
	// typo takes: a dropped character, a wrong character, an extra one.
	t.Run("the function name", func(t *testing.T) {
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-. $()"
		seen, names := map[string]bool{}, []string{}
		add := func(s string) {
			if !seen[s] {
				seen[s] = true
				names = append(names, s)
			}
		}
		for i := range name {
			add(name[:i] + name[i+1:])
			for _, c := range alphabet {
				add(name[:i] + string(c) + name[i+1:])
			}
		}
		for i := 0; i <= len(name); i++ {
			for _, c := range alphabet {
				add(name[:i] + string(c) + name[i:])
			}
		}
		refused := 0
		for _, n := range names {
			// A generated name that is the right name once the whitespace
			// RFC 0003 ignores is removed - `topic_hash ` and ` topic_hash`
			// among them - is not a wrong name, and is skipped for what it
			// is rather than by being listed.
			if strings.Trim(n, " \t\r\n") == name {
				continue
			}
			if granted(n + "(3, 0)") {
				t.Errorf("%s(3, 0) was granted, and topic_hash is the only "+
					"function RFC 0003 defines", n)
				continue
			}
			refused++
		}
		// The count is the check proving it looked: a generator that
		// produced nothing would pass every assertion above it.
		if refused < 500 {
			t.Fatalf("only %d names were generated, which proves little", refused)
		}
		if !granted(name + "(3, 0)") {
			t.Fatal("the exact name was refused, so this test refuses everything")
		}
		t.Logf("%d generated names refused; only %q is granted", refused, name)
	})

	// **Every arity from none to six**, with arguments that are individually
	// legal, so the only thing under test is how many there are.
	t.Run("the number of arguments", func(t *testing.T) {
		for n := 0; n <= 6; n++ {
			args := make([]string, n)
			for i := range args {
				args[i] = "1"
			}
			if n > 0 {
				args[0] = "8" // a count every index below it is legal in
			}
			val := name + "(" + strings.Join(args, ", ") + ")"
			if got, want := granted(val), n == 2; got != want {
				t.Errorf("%s: granted=%v, and RFC 0003 states the call takes "+
					"two arguments", val, got)
			}
		}
	})

	// **Every short string over an alphabet of the things people put in
	// numbers**, in each argument position. 8,420 tokens, both positions.
	t.Run("what an argument is made of", func(t *testing.T) {
		// RFC 0003: an argument is one or more ASCII decimal digits, with
		// the four ASCII whitespace characters ignored around it.
		digits := regexp.MustCompile(`^[0-9]+$`)
		legal := func(tok string, inCount bool) bool {
			s := strings.Trim(tok, " \t\r\n")
			if !digits.MatchString(s) {
				return false
			}
			v, err := strconv.Atoi(s) // the value, after the lexis is decided
			if err != nil || v > maxPartitionCount {
				return false
			}
			if inCount {
				return v >= 1 // the index beside it is 0, so any count of 1 or more
			}
			return v < 9 // the count beside it is 9
		}
		const chars = "0123456789+-. _eExX\t"
		var tokens []string
		for _, a := range chars {
			tokens = append(tokens, string(a))
			for _, b := range chars {
				tokens = append(tokens, string(a)+string(b))
				for _, c := range chars {
					tokens = append(tokens, string(a)+string(b)+string(c))
				}
			}
		}
		checked, ok, bad := 0, 0, 0
		for _, tok := range tokens {
			for _, inCount := range []bool{true, false} {
				val := name + "(9, " + tok + ")"
				if inCount {
					val = name + "(" + tok + ", 0)"
				}
				got, want := granted(val), legal(tok, inCount)
				checked++
				if want {
					ok++
				}
				if got != want {
					if bad++; bad <= 8 {
						t.Errorf("%q: granted=%v, RFC 0003 says %v", val, got, want)
					}
				}
			}
		}
		if bad > 8 {
			t.Errorf("...and %d more disagreements", bad-8)
		}
		// Both halves have to have happened, or the alphabet was wrong and
		// the test measured one answer over and over.
		if checked < 16000 || ok == 0 || ok == checked {
			t.Fatalf("%d checked, %d of them legal: the generator covered one answer",
				checked, ok)
		}
		t.Logf("%d argument spellings checked, %d legal, all agreeing with RFC 0003",
			checked, ok)
	})

	// **Unicode is neither whitespace nor a digit here**, and the runes are
	// taken from Go's own tables rather than named, so a character nobody
	// thought of is in the sweep by construction.
	t.Run("unicode is not whitespace and not a digit", func(t *testing.T) {
		spaces, nondigits := 0, 0
		for r := rune(1); r < 0x3000; r++ {
			if unicode.IsSpace(r) && !strings.ContainsRune(" \t\r\n", r) {
				spaces++
				if granted(name + "(3," + string(r) + "0)") {
					t.Errorf("U+%04X is being trimmed as whitespace, and RFC 0003 "+
						"names four ASCII characters: two implementations of that "+
						"document would answer this packet differently", r)
				}
			}
			if unicode.IsDigit(r) && r > unicode.MaxASCII {
				nondigits++
				if granted(name + "(" + string(r) + ", 0)") {
					t.Errorf("U+%04X is being read as a digit, and RFC 0003 says "+
						"ASCII decimal digits", r)
				}
			}
		}
		if spaces < 5 || nondigits < 50 {
			t.Fatalf("%d non-ASCII whitespace runes and %d non-ASCII digits "+
				"generated: the sweep examined almost nothing", spaces, nondigits)
		}
		t.Logf("%d non-ASCII whitespace runes and %d non-ASCII digits, all refused",
			spaces, nondigits)
	})
}

// RFC 0003 "The reserved prefix on a
// client's own packets"
//
// **The value was guarded to the character and the name in front of it was
// not.** An empty `saguin-filter` is refused because a client that asked
// for a slice and was served the channel has no way to notice; the same
// client mistake one token to the left - `saguin-fitler` - was granted and
// served everything. So was the retired pair, which is the migration case
// where silence is worst.
//
// **Generated from the allow-list rather than from the report.** Two spellings
// were reported; a test of those two would pass while the third got
// through. Every one-character edit of every name saguin reads is the
// class, and it covers `saguin-deletions` - which the report missed, and
// which the bridge sends on its own SUBSCRIBE, so an allow-list built from
// the report alone would have refused every inbound bridge.
func TestAnUnreadReservedPropertyIsNeverSilent(t *testing.T) {
	edits := func(name string) []string {
		const alphabet = "abcdefghijklmnopqrstuvwxyz-_"
		seen, out := map[string]bool{name: true}, []string{}
		add := func(s string) {
			if !seen[s] && strings.HasPrefix(s, reservedPrefix) {
				seen[s] = true
				out = append(out, s)
			}
		}
		for i := range name {
			add(name[:i] + name[i+1:])
			for _, c := range alphabet {
				add(name[:i] + string(c) + name[i+1:])
				add(name[:i] + string(c) + name[i:])
			}
		}
		return out
	}

	var names []string
	for _, allowed := range subscribeProps {
		names = append(names, edits(allowed)...)
	}
	// The retired pair, reached the same way: they are one-character edits
	// of nothing saguin reads, so they arrive here as ordinary members of
	// the class rather than as two rows somebody remembered.
	names = append(names, "saguin-partition-count", "saguin-partition-index")

	refused := 0
	for _, n := range names {
		pk := packets.Packet{Properties: packets.Properties{
			User: []packets.UserProperty{{Key: n, Val: "topic_hash(2, 0)"}}}}
		if key := unreadReserved(pk, subscribeProps); key == "" {
			t.Errorf("%q passes as a property saguin reads, and it reads none by "+
				"that name: a client sending it is granted and served everything",
				n)
			continue
		}
		refused++
	}
	if refused < 200 {
		t.Fatalf("only %d names were generated, which proves little", refused)
	}

	// **And every name saguin does read still passes**, or this rule breaks
	// the features it is protecting - the bridge's deletions among them.
	for _, allowed := range subscribeProps {
		pk := packets.Packet{Properties: packets.Properties{
			User: []packets.UserProperty{{Key: allowed, Val: "1"}}}}
		if key := unreadReserved(pk, subscribeProps); key != "" {
			t.Fatalf("%q is refused, and saguin reads it: the allow-list has "+
				"outgrown the code that fills it", key)
		}
	}

	// A property outside the prefix stays opaque, which is what MQTT says
	// and is the escape hatch for anything an older broker should tolerate.
	pk := packets.Packet{Properties: packets.Properties{
		User: []packets.UserProperty{{Key: "trace-id", Val: "x"},
			{Key: "x-saguin-filter", Val: "y"}}}}
	if key := unreadReserved(pk, subscribeProps); key != "" {
		t.Errorf("%q was claimed, and it is not under the reserved prefix", key)
	}
	t.Logf("%d generated names under %q refused; %d read names still pass",
		refused, reservedPrefix, len(subscribeProps))
}
