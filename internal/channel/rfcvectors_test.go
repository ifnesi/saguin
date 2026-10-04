package channel

// The worked examples in RFC 0003 are the one thing a third-party client
// implementer trusts absolutely and cannot check against anything else.
//
// **A wrong vector is worse than a missing one.** Somebody writes five
// lines of FNV-1a from the specification, checks them against the table,
// gets agreement, and ships a client that disagrees with the broker on
// which topics are theirs - and every existing test stays green, because
// the vectors are prose. That is rule 15 pointed at the sentences where it
// matters most: a behavioural claim needs a test that can fail when the
// claim stops being true, and these sentences are claims.
//
// So the document is the input. The table is parsed out of the RFC and each
// row recomputed, and the constants the RFC states are asserted against the
// ones the code uses. Neither can move without the other.

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const rfc0003 = "../../docs/rfcs/0003-delivery-semantics.md"

// | `topic` | FNV-1a-64 | after mixing | mod 3 | mod 8 |
//
// **Five columns since the mixing step, and both hash columns are
// checked.** An implementation that stops after FNV-1a agrees with the
// first and with nothing else, which is the mistake this table exists to
// let somebody find - so a table giving only the final value would leave
// them knowing they disagree and not which half is wrong.
var vectorRow = regexp.MustCompile(
	"^\\| `([^`]+)` \\| (\\d+) \\| (\\d+) \\| (\\d+) \\| (\\d+) \\|$")

func TestTheRFCsWorkedExamplesAreTrue(t *testing.T) {
	doc, err := os.ReadFile(rfc0003)
	if err != nil {
		t.Fatalf("read the RFC: %v", err)
	}

	rows := 0
	for _, line := range strings.Split(string(doc), "\n") {
		m := vectorRow.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		topic := m[1]
		wantHash, err := strconv.ParseUint(m[2], 10, 64)
		if err != nil {
			t.Errorf("the RFC's hash for %q is %q, which is not a 64-bit number",
				topic, m[2])
			continue
		}
		rows++

		if got := fnv1a64(topic); got != wantHash {
			t.Errorf("RFC 0003 says FNV-1a-64(%q) is %d and it is %d.\n"+
				"  A client implementing the algorithm from that document and "+
				"checking it against that table would agree with the table and "+
				"disagree with this broker about which topics are its own",
				topic, wantHash, got)
			continue
		}
		wantMixed, err := strconv.ParseUint(m[3], 10, 64)
		if err != nil {
			t.Errorf("the RFC's mixed value for %q is %q, which is not a "+
				"64-bit number", topic, m[3])
			continue
		}
		if got := PartitionHash(topic); got != wantMixed {
			t.Errorf("RFC 0003 says %q mixes to %d and it is %d.\n"+
				"  The slice is taken from this value, so a client agreeing "+
				"with the document disagrees with this broker about which "+
				"topics are its own", topic, wantMixed, got)
			continue
		}
		// And the two worked moduli, which are what an implementer actually
		// compares - the hash is an intermediate they may never print.
		for _, mod := range []struct {
			count int
			want  string
		}{{3, m[4]}, {8, m[5]}} {
			want, err := strconv.Atoi(mod.want)
			if err != nil {
				t.Errorf("the RFC's `mod %d` for %q is %q", mod.count, topic, mod.want)
				continue
			}
			// **From the mixed column, which is where the slice comes
			// from.** Taken from the FNV-1a column this would assert the
			// arithmetic the broker stopped doing.
			if got := int(wantMixed % uint64(mod.count)); got != want {
				t.Errorf("RFC 0003 says %q lands in slice %d of %d, and %d mod %d "+
					"is %d: the table disagrees with its own mixed column",
					topic, want, mod.count, wantMixed, mod.count, got)
			}
		}
	}

	// **Counted, because a table this found none of would pass.** A heading
	// renamed, a column added, the rows reflowed - any of those turns this
	// into a walk over nothing that reports success, which is the failure
	// the whole file is written against.
	if rows < 4 {
		t.Fatalf("found %d worked examples in %s and the section documents four: "+
			"this check has stopped matching the table it guards, so a wrong "+
			"vector would now ship unnoticed", rows, rfc0003)
	}
	t.Logf("%d worked examples in RFC 0003, each recomputed", rows)
}

// The same, more cheaply, for the algorithm itself: the RFC states the two
// constants as numbers, so a reader can implement it. If the code's
// constants ever differ from the stated ones, every vector above would move
// with them and agree with itself - so the constants are asserted against
// the document separately.
// hashBlock is the fenced code block under the RFC's hash heading - the
// one a client implements from - and fails the test if it cannot find
// exactly one. A block this could not locate would otherwise make every
// assertion against it vacuous.
func hashBlock(t *testing.T, text string) string {
	t.Helper()
	const heading = "### The hash, in full"
	i := strings.Index(text, heading)
	if i < 0 {
		t.Fatalf("%s no longer has a %q heading, so the algorithm block a client "+
			"implements from cannot be located and every constant below would be "+
			"checked against nothing", rfc0003, heading)
	}
	rest := text[i+len(heading):]
	open := strings.Index(rest, "```")
	if open < 0 {
		t.Fatalf("no fenced block follows %q", heading)
	}
	rest = rest[open+3:]
	end := strings.Index(rest, "```")
	if end < 0 {
		t.Fatalf("the block after %q is not closed", heading)
	}
	block := rest[:end]
	if !strings.Contains(block, "hash = hash XOR b") {
		t.Fatalf("the first block after %q does not look like the algorithm:\n%s",
			heading, block)
	}
	return block
}

func TestTheRFCsStatedConstantsAreTheOnesInUse(t *testing.T) {
	doc, err := os.ReadFile(rfc0003)
	if err != nil {
		t.Fatalf("read the RFC: %v", err)
	}
	text := string(doc)

	// **The algorithm block itself, not the document.** Matching anywhere in
	// the RFC would pass on a wrong block whenever the right number happened
	// to appear in prose somewhere else, and would look identical to a
	// working check. The relaxation this needed was about the alignment the
	// block is written with - the RFC pads `prime` to line its `=` up with
	// `offset basis`, and matching one exact spelling made this fail its own
	// document - so the padding is allowed and the location is not.
	block := hashBlock(t, text)

	for _, c := range []struct{ name, stated string }{
		{"offset basis", "14695981039346656037"},
		{"prime", "1099511628211"},
	} {
		stated := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(c.name) + ` *= *` +
			regexp.QuoteMeta(c.stated) + `$`)
		if !stated.MatchString(block) {
			t.Errorf("RFC 0003 no longer states `%s = %s`: the algorithm block a "+
				"client implements from has changed, and the vectors above would "+
				"still agree with each other", c.name, c.stated)
		}
	}

	// And that those are the numbers the code runs on. Computed rather than
	// read out of the source: one byte through the loop is the offset basis
	// XOR that byte, times the prime.
	// Variables rather than constants: Go evaluates a constant expression at
	// arbitrary precision and refuses the overflow, and the wrap at 2^64 is
	// exactly what the RFC's "modulo 2^64" describes.
	offset, prime := uint64(14695981039346656037), uint64(1099511628211)
	want := (offset ^ uint64('a')) * prime
	if got := fnv1a64("a"); got != want {
		t.Errorf("fnv1a64(\"a\") is %d and the constants RFC 0003 states give "+
			"%d: the code is running a different FNV-1a from the one the document "+
			"tells clients to implement", got, want)
	}

	// **The mixing step's constants, the same way**, because they are the
	// half a client is most likely to get wrong: the shifts are three
	// different numbers and the multipliers are twenty digits each. Stated
	// in the document and computed here, so neither can move alone.
	mixing := mixingBlock(t, text)
	for _, c := range []struct{ name, stated string }{
		{"first multiplier", "13787848793156543929"},
		{"second multiplier", "10723151780598845931"},
		{"first shift", "30"},
		{"second shift", "27"},
		{"third shift", "31"},
	} {
		if !strings.Contains(mixing, c.stated) {
			t.Errorf("RFC 0003's mixing block no longer states the %s %s, so a "+
				"client implementing from that block would disagree with this "+
				"broker about which topics are its own", c.name, c.stated)
		}
	}
	h := want
	h ^= h >> 30
	h *= 13787848793156543929
	h ^= h >> 27
	h *= 10723151780598845931
	h ^= h >> 31
	if got := PartitionHash("a"); got != h {
		t.Errorf("PartitionHash(\"a\") is %d and the two blocks RFC 0003 states "+
			"give %d: the code is mixing differently from the document", got, h)
	}
}

// mixingBlock is the second fenced block under the RFC's hash heading -
// the mixing step - and fails the test if it cannot find one that looks
// like it, for the same reason hashBlock does.
func mixingBlock(t *testing.T, text string) string {
	t.Helper()
	const heading = "### The hash, in full"
	i := strings.Index(text, heading)
	if i < 0 {
		t.Fatalf("%s no longer has a %q heading", rfc0003, heading)
	}
	rest := text[i+len(heading):]
	for n := 0; n < 2; n++ { // past the FNV-1a block, then into the next
		open := strings.Index(rest, "```")
		if open < 0 {
			t.Fatalf("fewer than two fenced blocks follow %q, so the mixing step "+
				"a client implements from cannot be located and every constant "+
				"below would be checked against nothing", heading)
		}
		rest = rest[open+3:]
		end := strings.Index(rest, "```")
		if end < 0 {
			t.Fatalf("a block after %q is not closed", heading)
		}
		if n == 1 {
			block := rest[:end]
			if !strings.Contains(block, "hash XOR (hash >>") {
				t.Fatalf("the second block after %q does not look like the mixing "+
					"step:\n%s", heading, block)
			}
			return block
		}
		rest = rest[end+3:]
	}
	panic("unreachable")
}
