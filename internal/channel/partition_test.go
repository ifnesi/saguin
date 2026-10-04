package channel

import (
	"fmt"
	"hash/fnv"
	"testing"
)

// The hash is a specified protocol constant, so the thing to hold it
// against is somebody else's implementation of the same specification
// rather than a table of values this package produced.
//
// **The standard library is that somebody else.** hash/fnv is the same
// algorithm with an allocating API, so if the inline arithmetic here ever
// drifts - a wrong offset basis, a multiply before the xor, a byte read as
// a rune - the two part company on the first topic that differs.
func TestPartitionHashIsFNV1a64(t *testing.T) {
	topics := []string{
		"", "a", "iot/depot/events/dev-1", "iot/water/w-7/inspect",
		"iot/hq/state/thermostat-42", "a/b/c/d/e/f/g/h",
		// Bytes above 0x7f, where reading a string as runes rather than
		// bytes would diverge and a table of ASCII topics never would.
		"iot/café/events/x", "iot/\xff\xfe/events/x",
	}
	for _, topic := range topics {
		h := fnv.New64a()
		h.Write([]byte(topic))
		if got, want := fnv1a64(topic), h.Sum64(); got != want {
			t.Errorf("fnv1a64(%q) = %d, hash/fnv says %d: the inline "+
				"arithmetic has drifted from the algorithm the RFC specifies, so a "+
				"client computing its own slice would disagree with the broker",
				topic, got, want)
		}
	}
}

// **A hash nobody can reproduce is the failure this constant exists to
// avoid**, so the two properties a client depends on are asserted rather
// than assumed: the same topic always lands in the same place, and the
// answer does not depend on anything the process chose at startup.
func TestPartitionHashIsStable(t *testing.T) {
	const topic = "iot/depot/events/dev-1"
	// The value is written out, which is what makes this a check against a
	// restart reshuffling assignments rather than a check that a function
	// returns what it returned a line ago. It is the mixed value, because
	// that is the one a slice is taken from.
	const want = uint64(16961261177379703000)
	for range 3 {
		if got := PartitionHash(topic); got != want {
			t.Fatalf("PartitionHash(%q) = %d then %d", topic, want, got)
		}
	}
}

// **The mixing step is what the slice is taken from**, and the two halves
// are asserted apart so that a drift in either is named rather than
// leaving `PartitionHash` merely "not FNV-1a".
func TestPartitionHashMixesTheFNVValue(t *testing.T) {
	const topic = "iot/depot/events/dev-1"
	h := fnv1a64(topic)
	if got := PartitionHash(topic); got == h {
		t.Fatalf("PartitionHash(%q) is the bare FNV-1a value %d: the mixing "+
			"step is not being applied, and a topic scheme carrying an "+
			"identifier twice would be excluded from most of a group",
			topic, got)
	}
	if got, want := PartitionHash(topic), mixed(h); got != want {
		t.Errorf("PartitionHash(%q) = %d, mixed(fnv1a64) = %d", topic, got, want)
	}
}

// **The defect the mixing step exists for, driven rather than described.**
// A topic scheme carrying an identifier twice cancels that identifier out
// of FNV-1a's low bits, so every one of these hashed to a strict subset of
// the shares and whole members were sent nothing at all - 2454/0/1546/0
// over four, at any scale. The oracle is a floor on the smallest share
// rather than the exact split, because the exact split is arithmetic on a
// hash and would pin this test to today's constants.
func TestATopicSchemeRepeatingAnIdentifierStillSpreads(t *testing.T) {
	const topics = 4000
	for _, count := range []int{2, 3, 4, 6, 8, 16} {
		held := make([]int, count)
		for i := range topics {
			topic := fmt.Sprintf("devices/dev-%d/messages/devicebound/dev-%d", i, i)
			held[PartitionHash(topic)%uint64(count)]++
		}
		even := topics / count
		for slice, got := range held {
			if got < even/2 {
				t.Errorf("over %d shares, share %d was sent %d of %d topics "+
					"and an even split is %d: %v.\n"+
					"  A scheme carrying an identifier twice is being excluded "+
					"from part of the group, which is what the mixing step in "+
					"PartitionHash exists to prevent",
					count, slice, got, topics, even, held)
				break
			}
		}
	}
}
