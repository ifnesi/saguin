package broker

import (
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/channel"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// subscribeWith is a SUBSCRIBE carrying one saguin-filter property per
// value given, which is how MQTT 5 spells an OR of predicates.
func subscribeWith(vals ...string) packets.Packet {
	var pk packets.Packet
	for _, v := range vals {
		pk.Properties.User = append(pk.Properties.User,
			packets.UserProperty{Key: "saguin-filter", Val: v})
	}
	return pk
}

// RFC 0003 "Client-declared partitioning"
//
// The whole validation set, decided rather than discovered - so that the
// cases nobody thought of at the time are visible as absences here rather
// than as behaviour somebody meets later.
func TestWhatASubscriberMayDeclareAboutItsSlice(t *testing.T) {
	for _, tc := range []struct {
		name    string
		vals    []string
		refused bool
		count   int
		indices []int
	}{
		{name: "nothing at all, which is every client that never heard of this"},
		{name: "one slice of three", vals: []string{"topic_hash(3, 0)"},
			count: 3, indices: []int{0}},
		{name: "several slices, the property repeated, which is an OR",
			vals:  []string{"topic_hash(8, 1)", "topic_hash(8, 5)"},
			count: 8, indices: []int{1, 5}},
		{name: "the same slice twice is deduplicated, not refused",
			vals:  []string{"topic_hash(4, 2)", "topic_hash(4, 2)"},
			count: 4, indices: []int{2}},
		{name: "indices arrive out of order and are sorted",
			vals:  []string{"topic_hash(4, 3)", "topic_hash(4, 1)"},
			count: 4, indices: []int{1, 3}},
		{name: "count 1 is valid and degenerate - one slice, holding everything",
			vals: []string{"topic_hash(1, 0)"}, count: 1, indices: []int{0}},
		{name: "a very large count is allowed rather than bounded arbitrarily",
			vals:  []string{"topic_hash(1000000, 999999)"},
			count: 1000000, indices: []int{999999}},

		// **Whitespace around the name, the parentheses and the comma is
		// ignored, and nothing else is.** A client that pretty-prints its
		// own declaration is not making a mistake; one that writes a second
		// operator into it is.
		{name: "no space after the comma", vals: []string{"topic_hash(3,0)"},
			count: 3, indices: []int{0}},
		{name: "space everywhere it can go",
			vals: []string{"  topic_hash ( 3 , 0 )  "}, count: 3, indices: []int{0}},
		{name: "a tab and a newline are whitespace too",
			vals: []string{"topic_hash(\t3,\n0)"}, count: 3, indices: []int{0}},

		// **The one that panics the broker.** A modulus by zero is a crash
		// from one SUBSCRIBE, from any client the listener admits, before
		// any authorization rule has an opinion on it.
		{name: "count 0 is a division by zero and is refused at the edge",
			vals: []string{"topic_hash(0, 0)"}, refused: true},
		{name: "a negative count", vals: []string{"topic_hash(-1, 0)"}, refused: true},
		{name: "a negative index", vals: []string{"topic_hash(3, -1)"}, refused: true},
		{name: "an index at the count names a slice that cannot exist",
			vals: []string{"topic_hash(3, 3)"}, refused: true},
		{name: "an index above the count", vals: []string{"topic_hash(3, 9)"},
			refused: true},

		// **The arguments are positional, and a swap cannot pass silently.**
		// An index must be below the count, so if topic_hash(8, 1) is valid
		// then topic_hash(1, 8) is refused - which holds for every valid
		// pair, not only this one, and is why the order needs no sigil.
		{name: "the arguments the wrong way round", vals: []string{"topic_hash(1, 8)"},
			refused: true},

		{name: "one argument", vals: []string{"topic_hash(3)"}, refused: true},
		{name: "three arguments", vals: []string{"topic_hash(3, 0, 1)"}, refused: true},
		{name: "no arguments", vals: []string{"topic_hash()"}, refused: true},
		{name: "a count that is not a number", vals: []string{"topic_hash(three, 0)"},
			refused: true},
		{name: "an index that is not a number", vals: []string{"topic_hash(3, first)"},
			refused: true},
		{name: "an empty argument", vals: []string{"topic_hash(, 0)"}, refused: true},

		// **A saguin-filter with nothing readable in it is refused, never
		// ignored.** Dropping it would serve the whole channel to a client
		// that asked for a slice of it, which is the failure this feature
		// exists to avoid and would carry no signal at all.
		{name: "an empty value", vals: []string{""}, refused: true},
		{name: "the expression form this replaced",
			vals: []string{"$topic_hash % 3 == 0"}, refused: true},
		{name: "a function saguin does not have",
			vals: []string{"offset_range(3, 0)"}, refused: true},
		{name: "the wrong spelling of the one it does",
			vals: []string{"TOPIC_HASH(3, 0)"}, refused: true},
		{name: "a call with something after it",
			vals: []string{"topic_hash(3, 0) or everything"}, refused: true},
		{name: "a bare function name", vals: []string{"topic_hash"}, refused: true},

		// **One subscription has one partition space**, so an OR of calls
		// that do not agree about its size is refused rather than resolved.
		{name: "two calls disagreeing about the count",
			vals: []string{"topic_hash(4, 0)", "topic_hash(6, 1)"}, refused: true},
		{name: "one good call and one that cannot be read",
			vals: []string{"topic_hash(4, 0)", "nonsense"}, refused: true},

		// **The bound is a type boundary, so every build agrees on it.**
		// Parsed into a platform `int` these rows would pass here and fail
		// on a 32-bit gateway, which is a rule RFC 0003 could not state.
		{name: "the largest count saguin admits, 2^31-1",
			vals:  []string{"topic_hash(2147483647, 2147483646)"},
			count: 2147483647, indices: []int{2147483646}},
		{name: "one above the bound", vals: []string{"topic_hash(2147483648, 0)"},
			refused: true},
		{name: "an index one above the bound",
			vals: []string{"topic_hash(2147483647, 2147483648)"}, refused: true},
		{name: "a count past what any int holds, a different mistake from a typo",
			vals: []string{"topic_hash(99999999999999999999999, 0)"}, refused: true},
		{name: "an index past what an int holds",
			vals: []string{"topic_hash(3, 99999999999999999999999)"}, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, why := partitioning(subscribeWith(tc.vals...))
			if tc.refused {
				if why == "" {
					t.Fatalf("granted, and declared count=%d indices=%v: %s",
						p.count, p.indices, tc.name)
				}
				return
			}
			if why != "" {
				t.Fatalf("refused with %q, and it is a legal declaration", why)
			}
			if p.count != tc.count {
				t.Errorf("count %d, want %d", p.count, tc.count)
			}
			if len(p.indices) != len(tc.indices) {
				t.Fatalf("indices %v, want %v", p.indices, tc.indices)
			}
			for i := range tc.indices {
				if p.indices[i] != tc.indices[i] {
					t.Errorf("indices %v, want %v", p.indices, tc.indices)
				}
			}
		})
	}
}

// **This function reads one property name and nothing else.** MQTT 5 User
// Properties are open, so a client is free to put its own on a SUBSCRIBE,
// and a parser that let one of them declare a slice would slice a channel
// nobody asked to slice.
//
// **What it does not do is decide whether the packet is acceptable.** A
// `saguin-` name saguin does not read is refused before this runs, by
// unreadReserved - the retired pair among them. This test held the opposite for a day, asserting that such a client
// "must be served everything", which is exactly the silence that finding
// was about; the rule now lives one layer out, and the assertion here is
// only that the parser stays narrow.
func TestOnlySaguinFilterDeclaresASlice(t *testing.T) {
	pk := packets.Packet{Properties: packets.Properties{User: []packets.UserProperty{
		{Key: "saguin-partition-count", Val: "3"},
		{Key: "saguin-partition-index", Val: "0"},
		{Key: "trace-id", Val: "topic_hash(0, 0)"},
		{Key: "saguin-deletions", Val: ""},
	}}}
	p, why := partitioning(pk)
	if why != "" {
		t.Fatalf("refused with %q, and nothing on this packet is a saguin-filter", why)
	}
	if p.declared() {
		t.Errorf("declared count=%d indices=%v from a packet carrying no "+
			"saguin-filter: this parser reads one name", p.count, p.indices)
	}
	// And the packet as a whole is refused, one layer out.
	if key := unreadReserved(pk, subscribeProps); key != "saguin-partition-count" {
		t.Errorf("unreadReserved found %q, want the retired pair to be caught: a "+
			"client sending yesterday's form must be told, not served everything", key)
	}
}

// **The refusal has to say what is wrong**, because one reason code covers
// every one of them: 0x83, the code saguin already answers for "your
// request is wrong in a way MQTT has no code for". The code points at the
// packet; only the reason points at the value.
func TestEveryPartitionRefusalNamesTheProperty(t *testing.T) {
	for _, vals := range [][]string{
		{"topic_hash(0, 0)"},
		{"topic_hash(3, 3)"},
		{"topic_hash(3)"},
		{"topic_hash(x, 0)"},
		{"$topic_hash % 3 == 0"},
		{""},
		{"topic_hash(4, 0)", "topic_hash(6, 0)"},
	} {
		_, why := partitioning(subscribeWith(vals...))
		if why == "" {
			t.Fatalf("%v was granted", vals)
		}
		if !strings.Contains(why, "saguin-filter") {
			t.Errorf("%v refused with %q, which does not name the property: a client "+
				"reading this cannot tell what saguin would not read", vals, why)
		}
	}
}

// **The form belongs in the refusal**, because a client that got the syntax
// wrong cannot be told what right looks like by a reason code. Every
// unreadable value answers with the shape saguin accepts.
func TestARefusalSaysWhatTheFormIs(t *testing.T) {
	for _, val := range []string{
		"", "topic_hash", "TOPIC_HASH(3, 0)", "offset_range(3, 0)",
		"$topic_hash % 3 == 0", "topic_hash(3, 0) or everything",
	} {
		_, why := partitioning(subscribeWith(val))
		if !strings.Contains(why, "topic_hash(<partitions>, <index>)") {
			t.Errorf("%q refused with %q, which does not say what the form is", val, why)
		}
	}
}

// **The bound must not move with the build**, which is the whole reason it
// is a type boundary rather than a number somebody chose. The threshold
// itself is asserted rather than a value near it, so a change of the
// constant - or a reader going back to strconv.Atoi, whose range is the
// platform's - is visible here rather than on somebody's gateway.
func TestThePartitionBoundIsTheSameOnEveryBuild(t *testing.T) {
	if maxPartitionCount != 1<<31-1 {
		t.Fatalf("maxPartitionCount is %d: RFC 0003 states 2147483647, and a "+
			"threshold the specification cannot name is one two saguins "+
			"disagree about", maxPartitionCount)
	}
	if _, why := partitionValue("the partition count", "2147483647"); why != "" {
		t.Errorf("2147483647 refused: %s", why)
	}
	if _, why := partitionValue("the partition count", "2147483648"); why == "" {
		t.Error("2147483648 accepted, and a 32-bit build cannot represent it")
	}
}

// The modulus that count 0 would have run. It is asserted directly because
// the refusal above is what stands between a client and this line, and a
// test that only checks the refusal does not show what it prevents.
func TestTheZeroValueWantsEverythingRatherThanDividing(t *testing.T) {
	var none partition
	if none.declared() {
		t.Fatal("the zero partition reads as a declaration")
	}
	// Would panic if wants took the modulus without asking declared() first.
	if !none.wants(channel.PartitionHash("iot/depot/events/dev-1")) {
		t.Error("a subscriber that declared nothing was not served a topic")
	}
}

// **A refusal reaches the operator's log, so the client cannot choose how
// much of it there is.** A property value is bounded only by Maximum Packet
// Size, and quoting one whole would let a client in a reconnect loop write
// as much of somebody's disk as it liked. Found by
// TestNoLogLineCarriesAnUnboundedClientString, which is the general form of
// this; asserted here too because that check reads the source and this
// reads what the function actually returns.
func TestAPartitionRefusalDoesNotEchoTheWholeValue(t *testing.T) {
	huge := strings.Repeat("9", 100_000)
	for _, val := range []string{
		"topic_hash(" + huge + ", 0)",
		"topic_hash(3, " + huge + ")",
		strings.Repeat("x", 50_000),
		"topic_hash(" + strings.Repeat("x", 50_000) + ", 0)",
	} {
		_, why := partitioning(subscribeWith(val))
		if why == "" {
			t.Fatal("a 100,000-character property value was accepted")
		}
		// Generous against the sentence around it, and nowhere near the
		// value's own length: what matters is that it does not scale.
		if len(why) > 300 {
			t.Errorf("the refusal is %d characters for a %d-character value: a client "+
				"chooses how much of the operator's log it writes", len(why), len(val))
		}
	}
}

// **A number that is too large and a value that is not a number get
// different sentences** (RFC 0003 "What a subscriber may declare"), because
// they send a reader looking in different places: told that 10^40 is not a
// number, somebody goes hunting for a stray character that is not there.
//
// This existed as a claim in a comment for a day while the code did the
// opposite - an argument past int64 failed strconv and took the spelling
// branch - so the claim is asserted here rather than described.
func TestTooLargeAndNotANumberAreDifferentRefusals(t *testing.T) {
	past := strings.Repeat("9", 40) // decimal digits, and far past int64
	for _, tc := range []struct{ val, want string }{
		{"topic_hash(" + past + ", 0)", "above the largest partition space"},
		{"topic_hash(3, " + past + ")", "above the largest partition space"},
		{"topic_hash(2147483648, 0)", "above the largest partition space"},
		{"topic_hash(3x, 0)", "decimal digits"},
		{"topic_hash(3, x)", "decimal digits"},
	} {
		_, why := partitioning(subscribeWith(tc.val))
		if !strings.Contains(why, tc.want) {
			t.Errorf("%s refused with %q, which does not say %q", tc.val, why, tc.want)
		}
	}
}
