package broker

// RFC 0003 "Client-declared partitioning - What a subscriber may declare"
//
// **partitioning() is the one place in this feature that reads bytes a
// client chose**, and every other part of it trusts what comes back: the
// count reaches a modulus, the indices reach a binary search, and both are
// held for the life of a session. A table test covers the cases somebody
// thought of, and `count: 0` - a modulus by zero, which panics the broker
// from one SUBSCRIBE, from any client the listener admits - was one nobody
// had thought of until it was pointed out. That is the argument for
// fuzzing this rather than only tabulating it.
//
// **It fuzzes whole property values rather than two numbers**, because the
// value is now a call the broker has to parse: `topic_hash(3, 0)` with its
// own brackets, comma and spelling. The grammar is surface a pair of
// numeric properties did not have, so it is the part that needs the random
// input most.
//
// The oracle is RFC 0003's stated form rather than the parser's own
// expressions, so that a rewrite of the parser cannot make this agree with
// itself.
//
// **What it found, and it is not what it was written for.** Weakening the
// direct `count < 1` check leaves this green, and that is correct rather
// than a gap: a count of 0 needs an index below it, and a negative index is
// refused for being negative. Another rule closes it independently.
// Removing *that* does turn this red. So the target holds the property - a
// granted declaration is never one a modulus can crash on - rather than any
// one line, which is what a property test should do.

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// callForm is `topic_hash(<partitions>, <index>)` as RFC 0003 writes it,
// with the whitespace the RFC says is ignored. Written as a regular
// expression here precisely because the parser is not one: an oracle that
// shared the parser's structure would agree with its mistakes.
var callForm = regexp.MustCompile(`^topic_hash[ \t\r\n]*\(([^,()]*),([^,()]*)\)$`)

// declaredBySpec is what RFC 0003 says these saguin-filter values declare,
// and whether they are legal at all.
func declaredBySpec(vals ...string) (partition, bool) {
	var p partition
	seen := map[int]bool{}
	for _, v := range vals {
		m := callForm.FindStringSubmatch(trimSpec(v))
		if m == nil {
			return partition{}, false
		}
		count, ok := specInt(m[1])
		if !ok || count < 1 {
			return partition{}, false
		}
		index, ok := specInt(m[2])
		if !ok || index < 0 || index >= count {
			return partition{}, false
		}
		if p.count != 0 && p.count != int(count) {
			return partition{}, false // one subscription, one partition space
		}
		p.count = int(count)
		if !seen[int(index)] {
			seen[int(index)] = true
			p.indices = append(p.indices, int(index))
		}
	}
	sort.Ints(p.indices)
	return p, true
}

// specInt is an argument as RFC 0003 states it: one or more ASCII decimal
// digits, at most 2147483647, with the whitespace the RFC names ignored.
//
// **The lexis is a regular expression rather than strconv.ParseInt**, which
// accepts a leading `+` or `-`. Asking ParseInt whether the parser was right
// about signs is asking the parser's own question of the parser's own
// helper, and it answers yes either way.
func specInt(raw string) (int64, bool) {
	s := trimSpec(raw)
	if !specDigits.MatchString(s) {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n <= maxPartitionCount
}

var specDigits = regexp.MustCompile(`^[0-9]+$`)

// trimSpec removes the whitespace RFC 0003 names - the ASCII space, tab,
// carriage return and line feed - and nothing else. Not strings.TrimSpace,
// which also trims U+00A0 and every other rune Unicode calls whitespace.
func trimSpec(s string) string { return strings.Trim(s, " \t\r\n") }

func FuzzWhatASubscriberDeclaresIsNeverUnsafe(f *testing.F) {
	// Seeds: the shapes the table covers, plus the shapes a table does not
	// reach - whitespace, signs, bases, brackets, and numbers around every
	// boundary.
	for _, s := range [][2]string{
		{"topic_hash(3, 0)", "topic_hash(3, 1)"},
		{"topic_hash(3,0)", "topic_hash(3,0)"},
		{"  topic_hash ( 3 , 0 )  ", ""},
		{"topic_hash(1, 0)", "topic_hash(2, 0)"},
		{"topic_hash(0, 0)", "topic_hash(3, -1)"},
		{"topic_hash(3, 3)", "topic_hash(1, 8)"},
		{"topic_hash(2147483647, 2147483646)", "topic_hash(2147483648, 0)"},
		{"topic_hash(99999999999999999999999, 0)", "topic_hash(3)"},
		{"topic_hash(3, 0, 1)", "topic_hash()"},
		{"topic_hash", "TOPIC_HASH(3, 0)"},
		{"$topic_hash % 3 == 0", "offset_range(3, 0)"},
		{"topic_hash(+3, 0)", "topic_hash(0x3, 0)"},
		{"topic_hash(-1, 0)", "topic_hash(3, +0)"},
		{"topic_hash(3,\u00a00)", "topic_hash\u00a0(3, 0)"},
		{"topic_hash(\u0663, 0)", "topic_hash(3, \u0669)"},
		{"topic_hash(3\v, 0)", "topic_hash(3\f, 0)"},
		{"topic_hash(3.0, 0)", "topic_hash(03, 00)"},
		{"topic_hash((3), 0)", "topic_hash(3, 0))"},
		{"topic_hash(9223372036854775807, 0)", "topic_hash(-9223372036854775808, 0)"},
		{"topic_hash(\x00, 0)", "topic_hash(3, 0) or everything"},
	} {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, first, second string) {
		pk := packets.Packet{}
		for _, u := range []packets.UserProperty{
			{Key: "saguin-filter", Val: first},
			{Key: "saguin-filter", Val: second},
			// A property saguin does not read, present throughout: it must
			// change nothing, and its absence from every rule below is the
			// assertion.
			{Key: "something-else", Val: first},
		} {
			pk.Properties.User = append(pk.Properties.User, u)
		}

		p, why := partitioning(pk)

		if why != "" {
			// **A refusal must decide nothing else.** The zero partition
			// wants everything, and a refused declaration that came back
			// non-zero would be a client served a slice it was told it
			// could not have.
			if p.declared() || len(p.indices) != 0 {
				t.Fatalf("refused (%s) and still returned count=%d indices=%v",
					why, p.count, p.indices)
			}
		}

		// **A saguin-filter is present, so there is no third answer.** Both
		// values are always on the packet, so the parser either reads them
		// or refuses: granting an undeclared partition here would be a
		// client that asked for a slice and was silently served the channel.
		if why == "" && !p.declared() {
			t.Fatalf("granted with no declaration from %q and %q", first, second)
		}

		// **The oracle**: RFC 0003's form, not the parser's expressions.
		want, legal := declaredBySpec(first, second)
		switch {
		case legal && why != "":
			t.Fatalf("refused (%s) a declaration RFC 0003 states is legal: %q, %q",
				why, first, second)
		case !legal && why == "":
			t.Fatalf("granted count=%d indices=%v from %q, %q, which RFC 0003 refuses",
				p.count, p.indices, first, second)
		}
		if why != "" {
			return
		}
		if p.count != want.count {
			t.Fatalf("granted count=%d from %q, %q: RFC 0003 states %d",
				p.count, first, second, want.count)
		}
		if len(p.indices) != len(want.indices) {
			t.Fatalf("granted indices %v from %q, %q: RFC 0003 states %v",
				p.indices, first, second, want.indices)
		}
		for i := range want.indices {
			if p.indices[i] != want.indices[i] {
				t.Fatalf("granted indices %v, RFC 0003 states %v", p.indices, want.indices)
			}
		}

		// **Everything below is what the rest of the feature relies on**,
		// and it is asserted against the granted value rather than the
		// oracle: an oracle that was itself wrong would otherwise agree.
		switch {
		case p.count < 1:
			// The modulus in wants(). This is the panic.
			t.Fatalf("granted count=%d, and a modulus by it is a crash", p.count)
		case p.count > maxPartitionCount:
			t.Fatalf("granted count=%d, above the %d RFC 0003 states",
				p.count, maxPartitionCount)
		case len(p.indices) == 0:
			t.Fatal("granted a declaration that takes no slice: it would be fed nothing")
		}
		for i, n := range p.indices {
			if n < 0 || n >= p.count {
				t.Fatalf("granted index %d in a space of %d: it names a slice that "+
					"cannot exist, so nothing would ever match it", n, p.count)
			}
			if i > 0 && p.indices[i-1] >= n {
				t.Fatalf("indices %v are not sorted and deduplicated, and wants() "+
					"binary-searches them: a member would silently stop receiving "+
					"a slice it holds", p.indices)
			}
		}

		// **wants() runs for every delivery to this subscriber**, so it must
		// answer for any hash without panicking, and must agree with the
		// arithmetic RFC 0003 states.
		for _, h := range []uint64{0, 1, 1 << 63, ^uint64(0), 9321193355118713112} {
			got := p.wants(h)
			want := false
			for _, n := range p.indices {
				if int(h%uint64(p.count)) == n {
					want = true
				}
			}
			if got != want {
				t.Fatalf("wants(%d) is %v and %d mod %d = %d against indices %v",
					h, got, h, p.count, h%uint64(p.count), p.indices)
			}
		}
	})
}
