package broker_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/ifnesi/saguin/internal/authz"
)

// ownerOf is which member of a space owns a topic, computed here from the
// algorithm RFC 0003 specifies rather than from the broker's own function.
//
// **This is the point of the whole test file.** Accepting whichever member
// a record arrived at proves that something happened; it does not prove the
// predicate is right, and it would pass against a broker whose hash
// disagreed with the specification - which is the exact failure a specified
// constant exists to prevent. So the expectation is computed independently,
// the way a client reimplementing the RFC's two blocks would - and
// **both** of them, because a reimplementation that stops after FNV-1a is
// the mistake the document's two hash columns exist to catch.
func ownerOf(topic string, count int) int {
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	h := offset64
	for i := 0; i < len(topic); i++ {
		h ^= uint64(topic[i])
		h *= prime64
	}
	h ^= h >> 30
	h *= 13787848793156543929
	h ^= h >> 27
	h *= 10723151780598845931
	h ^= h >> 31
	return int(h % uint64(count))
}

// took is what one member received, in arrival order, by topic.
func took(c *client) map[string][]string {
	out := map[string][]string{}
	for _, r := range c.All() {
		out[r.Topic] = append(out[r.Topic], r.Payload)
	}
	return out
}

// assertSliceIsExactlyOnce holds the two halves that matter, which are
// different bugs and each of which looks like success from one member's
// side: no record reaches two members, and none reaches none.
//
// It also proves it drove something. A partitioning test where nothing was
// published passes perfectly, so the count of what arrived is asserted
// against the count published rather than left implied.
func assertSliceIsExactlyOnce(t *testing.T, members []*client, count int, topics []string) {
	t.Helper()
	where := map[string][]int{}
	for i, m := range members {
		for topic := range took(m) {
			where[topic] = append(where[topic], i)
		}
	}
	delivered := 0
	for _, topic := range topics {
		got := where[topic]
		switch {
		case len(got) == 0:
			t.Errorf("%q reached no member of a space covering 0..%d: a partition "+
				"nobody receives is records nobody processes, and every member "+
				"looks healthy", topic, count-1)
			continue
		case len(got) > 1:
			t.Errorf("%q reached members %v: two members hold one record, which is "+
				"the duplicate half of the same bug", topic, got)
			continue
		}
		// **Which member, not just some member.**
		if want := ownerOf(topic, count); got[0] != want {
			t.Errorf("%q went to member %d, and FNV-1a over the topic puts it in "+
				"slice %d: the broker's hash disagrees with the one RFC 0003 "+
				"specifies, so a client cannot predict its own slice",
				topic, got[0], want)
		}
		delivered++
	}
	if delivered != len(topics) {
		t.Errorf("%d of %d topics were delivered exactly once", delivered, len(topics))
	}
	if delivered == 0 {
		t.Fatal("nothing was delivered at all, so this test asserted nothing")
	}
	t.Logf("%d topics, each to exactly one of %d members", delivered, len(members))
}

// topicsFor is a spread of topics under a prefix, chosen so that every slice
// of the space owns at least one - a set where one member owned nothing
// would be a test that passed while proving less than it looks.
func topicsFor(t *testing.T, prefix string, count, n int) []string {
	t.Helper()
	var topics []string
	seen := map[int]bool{}
	for i := 0; len(topics) < n; i++ {
		topic := fmt.Sprintf("%s/dev-%d", prefix, i)
		topics = append(topics, topic)
		seen[ownerOf(topic, count)] = true
	}
	if len(seen) != count {
		t.Fatalf("%d topics cover only %d of %d slices: a member owning nothing "+
			"makes the exactly-once assertion weaker than it reads", n, len(seen), count)
	}
	return topics
}

// RFC 0003 "Client-declared partitioning" - the `append` channel, live
//
// Three members covering the whole space over one filter. Every record
// reaches exactly one of them, and the one the specified hash names.
func TestPartitionedMembersSplitAnAppendChannelLive(t *testing.T) {
	const count = 3
	h := start(t)
	members := make([]*client, count)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("member-%d", i), true, false)
		if sa := members[i].SubSliced(t, "events/#", count, i); sa.Reasons[0] > 2 {
			t.Fatalf("member %d refused 0x%02X", i, sa.Reasons[0])
		}
	}
	time.Sleep(300 * time.Millisecond)

	topics := topicsFor(t, "events", count, 12)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	assertSliceIsExactlyOnce(t, members, count, topics)
}

// RFC 0003 "Client-declared partitioning" - the `append` channel, replay
//
// **The replay path is different code from the live one**, and it is the
// one a member meets first: a durable consumer that reconnects is served
// from its stored position, and a replay that ignored the declaration would
// hand a member the whole channel once and then only its share afterwards.
func TestPartitionedMembersSplitAnAppendChannelOnReplay(t *testing.T) {
	const count = 3
	h := start(t)

	// Published before anybody subscribes, so what arrives can only be a
	// replay from the channel's floor.
	topics := topicsFor(t, "events", count, 12)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(400 * time.Millisecond)

	members := make([]*client, count)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("replay-%d", i), false, false)
		members[i].SubSliced(t, "events/#", count, i)
	}
	time.Sleep(2 * time.Second)

	assertSliceIsExactlyOnce(t, members, count, topics)
}

// RFC 0003 "Client-declared partitioning" - the `latest` channel
//
// **Both halves, in one test, because splitting them is what hides the
// bug.** A member sent the whole of current state on subscribe and then
// only its share of the changes holds a copy of the key space that starts
// complete and drifts - which reads as correct on the day it is set up.
func TestPartitionedMembersSplitLatestStateAndChanges(t *testing.T) {
	const count = 3
	h := start(t)

	// Set before anybody subscribes: this half arrives as the snapshot.
	before := topicsFor(t, "state", count, 9)
	p := connect(t, h, "setter", true, false)
	for _, topic := range before {
		p.Pub(t, topic, "old")
	}
	time.Sleep(400 * time.Millisecond)

	members := make([]*client, count)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("state-%d", i), true, false)
		members[i].SubSliced(t, "state/#", count, i)
	}
	time.Sleep(1500 * time.Millisecond)

	t.Run("the snapshot on subscribe", func(t *testing.T) {
		assertSliceIsExactlyOnce(t, members, count, before)
	})

	// And the live half, on topics nobody has seen.
	for _, m := range members {
		drain(m)
	}
	after := topicsFor(t, "state/live", count, 9)
	for _, topic := range after {
		p.Pub(t, topic, "new")
	}
	time.Sleep(1500 * time.Millisecond)

	t.Run("the changes afterwards", func(t *testing.T) {
		assertSliceIsExactlyOnce(t, members, count, after)
	})
}

// RFC 0003 "Client-declared partitioning" - broadcast
//
// Broadcast is the one type saguin does not deliver itself, so this is the
// only path where the predicate is applied to the substrate's matched set
// rather than to saguin's own.
func TestPartitionedMembersSplitBroadcast(t *testing.T) {
	const count = 3
	h := start(t)
	members := make([]*client, count)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("bcast-%d", i), true, false)
		if sa := members[i].SubSliced(t, "loose/#", count, i); sa.Reasons[0] > 2 {
			t.Fatalf("member %d refused 0x%02X on a broadcast filter", i, sa.Reasons[0])
		}
	}
	time.Sleep(300 * time.Millisecond)

	topics := topicsFor(t, "loose", count, 12)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	assertSliceIsExactlyOnce(t, members, count, topics)
}

// RFC 0003 "Client-declared partitioning" - why the key is the topic
//
// **Per-topic order within a member is the whole reason the hash is over
// the topic rather than the offset.** Partitioning by `offset mod count`
// spreads perfectly evenly, needs no hash, and is trivial - and it lands
// consecutive records for one topic on different members, which is the one
// thing this design will not trade away. So the order a member sees within
// a topic is asserted rather than assumed.
func TestAPartitionedMemberSeesOneTopicsRecordsInOrder(t *testing.T) {
	const count = 3
	h := start(t)
	members := make([]*client, count)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("order-%d", i), true, false)
		members[i].SubSliced(t, "events/#", count, i)
	}
	time.Sleep(300 * time.Millisecond)

	// A handful of topics, each written several times. Every write of one
	// topic belongs to the same member, which is what makes order its
	// responsibility at all.
	topics := topicsFor(t, "events", count, 6)
	p := connect(t, h, "producer", true, false)
	const writes = 5
	for n := range writes {
		for _, topic := range topics {
			p.Pub(t, topic, strconv.Itoa(n))
		}
	}
	time.Sleep(2 * time.Second)

	checked := 0
	for i, m := range members {
		for topic, payloads := range took(m) {
			if want := ownerOf(topic, count); want != i {
				t.Errorf("%q reached member %d, want %d", topic, i, want)
				continue
			}
			if len(payloads) != writes {
				t.Errorf("member %d got %d of %d writes of %q: %v",
					i, len(payloads), writes, topic, payloads)
				continue
			}
			for n, got := range payloads {
				if got != strconv.Itoa(n) {
					t.Errorf("member %d saw %q out of order: %v", i, topic, payloads)
					break
				}
			}
			checked++
		}
	}
	if checked != len(topics) {
		t.Fatalf("checked the order of %d topics, want %d - a test that saw no "+
			"topic in full asserts nothing about order", checked, len(topics))
	}
	t.Logf("%d topics, %d writes each, every one in order within its member",
		checked, writes)
}

// RFC 0003 "Client-declared partitioning" - "declare nothing and you get
// everything"
//
// **Beside partitioned members rather than alone**, which is the case that
// discriminates: a predicate applied to the wrong subscriber, or applied
// because any subscriber declared one, shows up here and nowhere else.
func TestASubscriberDeclaringNothingStillGetsEverything(t *testing.T) {
	const count = 3
	h := start(t)

	watcher := connect(t, h, "watcher", true, false)
	watcher.Sub(t, "events/#", 1)

	members := make([]*client, count)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("beside-%d", i), true, false)
		members[i].SubSliced(t, "events/#", count, i)
	}
	time.Sleep(300 * time.Millisecond)

	topics := topicsFor(t, "events", count, 12)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	got := took(watcher)
	for _, topic := range topics {
		if len(got[topic]) != 1 {
			t.Errorf("the undeclared subscriber received %q %d times, want once: a "+
				"client that asked for no slice is served the whole channel",
				topic, len(got[topic]))
		}
	}
	if len(got) != len(topics) {
		t.Fatalf("the undeclared subscriber received %d of %d topics", len(got), len(topics))
	}
	// And the partitioned members beside it still split, so this did not
	// pass by the predicate never being applied at all.
	assertSliceIsExactlyOnce(t, members, count, topics)
}

// RFC 0003 "Client-declared partitioning" - the position consequence
//
// **Documented behaviour, so it gets a test that fails if it silently
// changes.** A partitioned consumer holds one cursor into the channel, so a
// record outside its slice is stepped over and the cursor moves past it.
// Widening the declaration later therefore does not recover what was
// skipped: those records are behind the consumer's position, and a
// partition nobody claimed ages out under retention like any other record.
//
// That is the accepted cost of having no group cursor. It is not a defect
// and it is not to be solved - but it is exactly the kind of behaviour that
// changes by accident, and a consumer that suddenly did receive its
// backlog would mean a cursor had stopped advancing.
func TestWideningASliceDoesNotRecoverWhatWasSkipped(t *testing.T) {
	const count = 3
	h := start(t)

	// A durable member taking one slice of three.
	m := connect(t, h, "widening", false, false)
	m.SubSliced(t, "events/#", count, 0)
	time.Sleep(300 * time.Millisecond)

	topics := topicsFor(t, "events", count, 12)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	var mine, theirs []string
	for _, topic := range topics {
		if ownerOf(topic, count) == 0 {
			mine = append(mine, topic)
		} else {
			theirs = append(theirs, topic)
		}
	}
	if len(mine) == 0 || len(theirs) == 0 {
		t.Fatalf("the spread gave slice 0 %d topics and the rest %d: this test needs "+
			"both", len(mine), len(theirs))
	}
	first := took(m)
	for _, topic := range mine {
		if len(first[topic]) == 0 {
			t.Errorf("slice 0 did not receive %q, which it owns", topic)
		}
	}
	if len(first) != len(mine) {
		t.Fatalf("slice 0 received %d topics, and owns %d", len(first), len(mine))
	}

	// It comes back declaring the whole space. Its position is already past
	// the records it stepped over.
	m.Close()
	again := connect(t, h, "widening", false, false)
	again.SubSliced(t, "events/#", 1, 0)
	time.Sleep(1500 * time.Millisecond)

	back := took(again)
	for _, topic := range theirs {
		if n := len(back[topic]); n != 0 {
			t.Errorf("the widened consumer was sent %q %d times: its cursor had "+
				"already moved past that record, so receiving it now means a "+
				"position stopped advancing over a skipped record - which is a "+
				"consumer that re-reads its whole channel on every widening",
				topic, n)
		}
	}

	// **And it is fed from here on**, which is what stops this passing
	// against a consumer that receives nothing ever again.
	fresh := "events/after-widening/1"
	p.Pub(t, fresh, "new")
	if r, ok := again.Await(t, 3*time.Second); !ok {
		t.Fatal("the widened consumer received nothing published after it returned: " +
			"it is not reading at all, and the silence above proved nothing")
	} else if r.Topic != fresh {
		t.Fatalf("received %q, want %q", r.Topic, fresh)
	}
	t.Logf("slice 0 took %d of %d topics, and widening to 1 recovered none of the "+
		"%d it had skipped", len(mine), len(topics), len(theirs))
}

// RFC 0003 "Client-declared partitioning" - a declaration does not outlive
// its session
//
// **The fourth route into the resume class, and the one the other three
// fixes opened.** A declaration now survives a disconnect, because a
// resumed session's subscriptions come back and its SUBSCRIBE does not. So
// the question is what ends it: a session that is discarded rather than
// resumed must take its declarations with it.
//
// The path that would leak: declare, drop the link with the session
// surviving, then reconnect with Clean Start = 1 and subscribe to a
// *different* filter. Nothing is restored, so OnSessionEstablished's clear
// sits below its early return and never runs, and the old filter's entry
// would stay in the index against a filter the client no longer holds.
//
// **What actually closes it is forgetFilters, and that was worth finding
// out rather than assuming.** Discarding a session unsubscribes its
// filters, and forgetFilters takes their declarations with them. The two
// clears that look like they cover this - discardPositions and
// OnSessionEstablish - do not: neutering both leaves this green, and only
// neutering forgetFilters turns it red. So this test guards that one
// mechanism, and the comment says which so the next reader does not
// simplify away the wrong line.
//
// It is a leak rather than mis-delivery: nothing is delivered without a
// subscription. That is exactly why it needs a window into the broker
// rather than an assertion about what arrived.
func TestADeclarationDoesNotOutliveItsSession(t *testing.T) {
	h := start(t)

	// A durable session that declares, so there is something to leak.
	first := connect(t, h, "clean-restart", false, false)
	first.SubSliced(t, "events/first/+", 4, 1)
	time.Sleep(300 * time.Millisecond)
	if got := h.B.DeclaredSlices("clean-restart"); len(got) != 1 {
		t.Fatalf("the broker holds %v for a client that declared one slice: this test "+
			"cannot show a leak it never created", got)
	}

	// The link goes, the session stays - the case that made the declaration
	// survive a disconnect in the first place.
	first.Close()
	time.Sleep(300 * time.Millisecond)

	// And it comes back clean, on a different filter.
	again := connect(t, h, "clean-restart", true, false)
	again.Sub(t, "events/second/+", 1)
	time.Sleep(300 * time.Millisecond)

	got := h.B.DeclaredSlices("clean-restart")
	if _, stale := got["events/first/+"]; stale {
		t.Errorf("the broker still holds a declaration for %q after a Clean Start = 1 "+
			"reconnect that subscribed to %q: the declaration outlived the session "+
			"that made it, against a filter this client no longer holds (%v)",
			"events/first/+", "events/second/+", got)
	}
	if len(got) != 0 {
		t.Errorf("the broker holds %v for a client that declared nothing on its "+
			"current session", got)
	}

	// **And the new subscription is served**, which is what stops this
	// passing against a reconnect that established nothing at all.
	p := connect(t, h, "producer", true, false)
	p.Pub(t, "events/second/1", "r")
	if r, ok := again.Await(t, 3*time.Second); !ok {
		t.Fatal("the reconnected client received nothing, so the empty index above " +
			"proves only that it never subscribed")
	} else if r.Topic != "events/second/1" {
		t.Fatalf("received %q", r.Topic)
	}
}

// RFC 0003 "What a subscriber may declare" - a malformed declaration is a
// SUBACK, not a hang-up
//
// "Anything else is 0x83 with the reason naming the property", and RFC 0002:
// "a refusal is total and the client stays connected ... and may subscribe
// again correctly". The unit tests drive partitioning() and the fuzz target
// drives its parser, but nothing drove a malformed declaration through a
// full server - which is where the enforcement sweep in OnSubscribed runs,
// on whatever the substrate did with the reason codes.
//
// **Every filter of the packet answers**, because the property is
// packet-level and there is no per-filter answer to give - so each case
// subscribes two filters and expects two 0x83s.
func TestAMalformedDeclarationIsARefusalNotADisconnect(t *testing.T) {
	h := start(t)
	const f = "saguin-filter"
	cases := []struct {
		name string
		ups  [][2]string
	}{
		{"count of zero", [][2]string{{f, "topic_hash(0, 0)"}}},
		{"index at the count", [][2]string{{f, "topic_hash(3, 3)"}}},
		{"negative index", [][2]string{{f, "topic_hash(3, -1)"}}},
		{"count that is not a number", [][2]string{{f, "topic_hash(abc, 0)"}}},
		{"one argument", [][2]string{{f, "topic_hash(3)"}}},
		{"three arguments", [][2]string{{f, "topic_hash(3, 0, 1)"}}},
		{"no arguments", [][2]string{{f, "topic_hash()"}}},
		{"an empty value, which is a client that built the property and " +
			"had nothing to put in it", [][2]string{{f, ""}}},
		// **The expression form this replaced.** A client written against
		// the first design must be told, not quietly served everything -
		// which is what dropping an unreadable value instead of refusing it
		// would do.
		{"the expression form", [][2]string{{f, "$topic_hash % 3 == 0"}}},
		{"a function saguin does not have", [][2]string{{f, "offset_range(3, 0)"}}},
		{"the wrong spelling of the one it does",
			[][2]string{{f, "TOPIC_HASH(3, 0)"}}},
		// **The hazard of positional arguments, closed by the index bound.**
		// If topic_hash(8, 1) is valid then topic_hash(1, 8) cannot be, so a
		// client that swaps them is told rather than served a slice it did
		// not ask for.
		{"the arguments the wrong way round", [][2]string{{f, "topic_hash(1, 8)"}}},
		{"two calls disagreeing about the count",
			[][2]string{{f, "topic_hash(3, 0)"}, {f, "topic_hash(4, 1)"}}},
		{"count above 2147483647", [][2]string{{f, "topic_hash(2147483648, 0)"}}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := connect(t, h, fmt.Sprintf("malformed-%d", i), true, false)
			sa, err := c.SubDeclaring(t, []string{"events/#", "state/#"}, tc.ups)
			if sa == nil {
				t.Fatalf("no SUBACK arrived (%v): RFC 0003 answers a malformed "+
					"declaration with 0x83 per filter, and RFC 0002 says the client "+
					"stays connected - a disconnect instead turns one bad packet "+
					"into a reconnect loop for any client that auto-resubscribes", err)
			}
			if len(sa.Reasons) != 2 {
				t.Fatalf("SUBACK carries %d reasons for 2 filters", len(sa.Reasons))
			}
			for j, r := range sa.Reasons {
				if r != 0x83 {
					t.Errorf("filter %d answered 0x%02X, want 0x83: a packet-level "+
						"declaration that cannot be read answers every filter in it", j, r)
				}
			}
			// **And the same connection may subscribe again correctly** - the
			// half of the rule a disconnect also breaks.
			if sa := c.Sub(t, "events/#", 1); sa.Reasons[0] > 2 {
				t.Fatalf("a correct subscribe after the refusal answered 0x%02X: the "+
					"refusal was not total-and-connected, it was terminal", sa.Reasons[0])
			}
		})
	}
}

// RFC 0003 "Client-declared partitioning" + RFC 0002 "Retained messages on
// a broadcast topic" - the retained pass honours the slice
//
// **The retained handover is a current-state pass, so the latest rule
// applies: both halves or neither.** A member handed the whole retained
// store on subscribe and then only its share of the live traffic holds a
// copy that starts complete and drifts - and across a group, every member
// processes every retained topic once, which is the duplicate class.
// deliverRetained is a fourth delivery site beside the three the feature
// covered, and this is the test that would have said so.
func TestTheRetainedPassHonoursASlice(t *testing.T) {
	const cnt = 2
	h := startRetaining(t, 0)

	// Retained before anybody subscribes, so what arrives can only be the
	// handover from the store.
	stored := topicsFor(t, "loose/kept", cnt, 8)
	p := connect(t, h, "setter", true, false)
	for _, topic := range stored {
		p.PubRetained(t, topic, "kept")
	}
	time.Sleep(400 * time.Millisecond)

	members := make([]*client, cnt)
	for i := range members {
		members[i] = connect(t, h, fmt.Sprintf("kept-%d", i), true, false)
		if sa := members[i].SubSliced(t, "loose/#", cnt, i); sa.Reasons[0] > 2 {
			t.Fatalf("member %d refused 0x%02X", i, sa.Reasons[0])
		}
	}
	time.Sleep(1500 * time.Millisecond)

	t.Run("the retained handover on subscribe", func(t *testing.T) {
		assertSliceIsExactlyOnce(t, members, cnt, stored)
	})

	// And the live half beside it, on fresh topics, so a fix that switched
	// the handover off entirely cannot pass as a fix.
	for _, m := range members {
		drain(m)
	}
	live := topicsFor(t, "loose/live", cnt, 8)
	for _, topic := range live {
		p.Pub(t, topic, "new")
	}
	time.Sleep(1500 * time.Millisecond)

	t.Run("the live half afterwards", func(t *testing.T) {
		assertSliceIsExactlyOnce(t, members, cnt, live)
	})
}

// RFC 0003 "Client-declared partitioning" - an unsubscribe takes its
// declaration with it
//
// The resume fixes made a declaration stick to its session, and this is the
// sibling that must not stick: a client that unsubscribes and later
// subscribes the same filter plainly has declared nothing on it, and a
// lingering slice would serve it less than everything - silently, which is
// the under-delivery half of the class the session tests guard the other
// half of.
func TestAnUnsubscribeTakesItsDeclarationWithIt(t *testing.T) {
	const cnt = 2
	h := start(t)

	c := connect(t, h, "unsub-clears", true, false)
	c.SubSliced(t, "events/#", cnt, 0)
	time.Sleep(300 * time.Millisecond)
	if got := h.B.DeclaredSlices("unsub-clears"); got["events/#"] != cnt {
		t.Fatalf("the broker holds %v after a declaration: this test cannot show a "+
			"leak it never created", got)
	}

	if _, err := c.C.Unsubscribe(context.Background(), &paho.Unsubscribe{
		Topics: []string{"events/#"},
	}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := h.B.DeclaredSlices("unsub-clears"); len(got) != 0 {
		t.Errorf("the broker still holds %v after the filter was unsubscribed: the "+
			"declaration outlived the subscription it was made on", got)
	}

	// **And the wire agrees**: subscribed again with no declaration, the
	// client is served everything - both slices, not the remembered one.
	c.Sub(t, "events/#", 1)
	time.Sleep(300 * time.Millisecond)
	topics := topicsFor(t, "events/afterunsub", cnt, 6)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)
	got := took(c)
	for _, topic := range topics {
		if len(got[topic]) == 0 {
			t.Errorf("%q (slice %d) never arrived after a plain re-subscribe: a "+
				"stale declaration is serving this client less than everything",
				topic, ownerOf(topic, cnt))
		}
	}
	if len(got) == 0 {
		t.Fatal("nothing arrived at all, so this test asserted nothing")
	}
}

// RFC 0003 "Client-declared partitioning" - one filter's declaration does
// not veto a record another subscription is owed
//
// The RFC blesses different slices on different filters ("a client wanting
// different slices for different filters sends different SUBSCRIBE
// packets"), and the invariants say a subscriber that declares nothing is
// served everything. Where two of one client's filters overlap, a record
// admitted by either must therefore still arrive: a declaration narrows
// what its own filter is served, and must not reach across to a
// neighbouring subscription. Only at-least-once is asserted, because MQTT 5
// leaves one-copy-or-one-per-subscription to the server.
//
// Broadcast and a channel are asserted separately, because they are
// delivered by different code - the substrate's matched set and saguin's
// own index - and a fix to one is not a fix to the other.
func TestADeclarationDoesNotVetoAnOverlappingSubscription(t *testing.T) {
	const cnt = 2
	h := start(t)

	check := func(t *testing.T, c *client, p *client, prefix string) {
		t.Helper()
		topics := topicsFor(t, prefix, cnt, 6)
		for _, topic := range topics {
			p.Pub(t, topic, "r")
		}
		time.Sleep(1500 * time.Millisecond)
		got := took(c)
		delivered := 0
		for _, topic := range topics {
			if len(got[topic]) == 0 {
				t.Errorf("%q (slice %d) never arrived, and a subscription this client "+
					"holds admits it: the overlapping filter's declaration vetoed a "+
					"record it was owed", topic, ownerOf(topic, cnt))
				continue
			}
			delivered++
		}
		if delivered == 0 {
			t.Fatal("nothing arrived at all, so this test asserted nothing")
		}
	}

	t.Run("an undeclared wide filter beside a declared exact one, broadcast", func(t *testing.T) {
		c := connect(t, h, "ovl-bcast", true, false)
		c.Sub(t, "loose/ovl/#", 1)
		c.SubSliced(t, "loose/ovl/+", cnt, 0)
		time.Sleep(300 * time.Millisecond)
		check(t, c, connect(t, h, "ovl-bcast-p", true, false), "loose/ovl")
	})

	t.Run("an undeclared wide filter beside a declared exact one, append", func(t *testing.T) {
		c := connect(t, h, "ovl-append", true, false)
		c.Sub(t, "events/#", 1)
		c.SubSliced(t, "events/ovl/+", cnt, 0)
		time.Sleep(300 * time.Millisecond)
		check(t, c, connect(t, h, "ovl-append-p", true, false), "events/ovl")
	})

	t.Run("two declarations that between them cover the space", func(t *testing.T) {
		// The wide filter takes slice 0 and the exact one slice 1, so every
		// topic is admitted by exactly one subscription - and each must
		// arrive through the one that owns it.
		c := connect(t, h, "ovl-split", true, false)
		c.SubSliced(t, "loose/split/#", cnt, 0)
		c.SubSliced(t, "loose/split/+", cnt, 1)
		time.Sleep(300 * time.Millisecond)
		check(t, c, connect(t, h, "ovl-split-p", true, false), "loose/split")
	})
}

// RFC 0003 "Client-declared partitioning" - a declaration survives a
// takeover
//
// A second connection arriving under a live session's client id takes the
// session over. It is the fifth route into the resume class - disconnect,
// resume, discard, forgetFilters, and this - and the only one where the old
// connection is still up when the subscriptions come back, so it exercises
// the rebuild against state the other four never see. A takeover that
// dropped the declaration would serve the returning client every other
// member's slice, exactly as the resume defect did.
func TestADeclarationSurvivesATakeover(t *testing.T) {
	const cnt = 2
	h := start(t)

	first := connect(t, h, "takeover", false, false)
	first.SubSliced(t, "events/#", cnt, 0)
	time.Sleep(300 * time.Millisecond)

	// The same client id, with the first connection still open.
	second := connect(t, h, "takeover", false, false)
	time.Sleep(500 * time.Millisecond)

	topics := topicsFor(t, "events/take", cnt, 8)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	got := took(second)
	seen := 0
	for _, topic := range topics {
		want := ownerOf(topic, cnt) == 0
		switch {
		case want && len(got[topic]) == 0:
			t.Errorf("%q is in the declared slice and never arrived after the takeover", topic)
		case !want && len(got[topic]) != 0:
			t.Errorf("%q is outside the declared slice and arrived %d times: the "+
				"takeover served this member another slice's records", topic, len(got[topic]))
		case want:
			seen++
		}
	}
	if seen == 0 {
		t.Fatal("no record of the declared slice arrived, so this test asserted nothing")
	}
}

// RFC 0002 "Retained messages", RFC 0003 "`latest`"
//
// **A second subscribe is not a request to re-send the first one's state.**
// A client holding a wide subscription on a `latest` channel that then
// subscribes a narrow one was handed the whole current-state pass again -
// every topic the wide filter reaches, carrying the wide subscription's
// identifier - because the pass is matched against the client's filters as
// a set rather than against the one that asked.
//
// State is idempotent, so this is re-delivery within one client rather than
// loss, which is why it is recorded rather than raised. It is
// still a client being sent what it did not ask for, on a channel where
// "on subscribe" is the whole of the promise.
//
// **The resume path deliberately keeps the old behaviour**, and the second
// half of this test holds that: a session coming back holds every restored
// filter and no single one of them asked, so the pass is matched against
// all of them.
func TestASecondSubscribeDoesNotResendTheFirstsState(t *testing.T) {
	h := start(t)
	p := connect(t, h, "setter", true, false)
	// Two topics the wide filter reaches and the narrow one does not, and
	// one they share.
	p.Pub(t, "state/wide/a", "wa")
	p.Pub(t, "state/wide/b", "wb")
	p.Pub(t, "state/narrow/c", "nc")
	time.Sleep(400 * time.Millisecond)

	c := connect(t, h, "two-filters", false, false)
	c.Sub(t, "state/wide/+", 1)

	wide := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(wide) < 2 {
		select {
		case r := <-c.Ch:
			wide[r.Topic] = true
		case <-deadline:
			t.Fatalf("the wide subscription was served %v of its two values", wide)
		}
	}

	// The second subscribe, on a filter that reaches one topic.
	c.Sub(t, "state/narrow/+", 1)
	got := map[string]int{}
	timer := time.After(2 * time.Second)
	for {
		done := false
		select {
		case r := <-c.Ch:
			got[r.Topic]++
		case <-timer:
			done = true
		}
		if done {
			break
		}
	}
	if got["state/narrow/c"] != 1 {
		t.Errorf("the new subscription was served %q %d times, want once",
			"state/narrow/c", got["state/narrow/c"])
	}
	for _, topic := range []string{"state/wide/a", "state/wide/b"} {
		if n := got[topic]; n != 0 {
			t.Errorf("subscribing %q re-sent %q %d time(s), which the first "+
				"subscription had already been given: a second subscribe asks for "+
				"its own filter's state, not for the whole client's again",
				"state/narrow/+", topic, n)
		}
	}

	// **And a resume still gets everything**, which is the half that stops
	// this being fixed by narrowing the pass everywhere.
	c.Close()
	p.Pub(t, "state/wide/a", "wa2")
	p.Pub(t, "state/narrow/c", "nc2")
	time.Sleep(400 * time.Millisecond)

	again := connect(t, h, "two-filters", false, false)
	back := map[string]string{}
	deadline = time.After(3 * time.Second)
	for len(back) < 2 {
		select {
		case r := <-again.Ch:
			back[r.Topic] = r.Payload
		case <-deadline:
			t.Fatalf("a resumed session was served %v, and it holds filters "+
				"reaching both changed topics: a resume has no single filter that "+
				"asked, so the pass is matched against all of them", back)
		}
	}
	if back["state/wide/a"] != "wa2" || back["state/narrow/c"] != "nc2" {
		t.Errorf("the resumed session was served %v, want both changes", back)
	}
}

// RFC 0003 "Client-declared partitioning"
//
// **A `$share` filter must have no say over the ordinary fan-out, in
// either direction.** Share deliveries do not travel through the set
// `slice` walks - the substrate merges the selected member in after this
// hook runs - so a shared subscription can neither veto an ordinary one
// nor entitle it.
//
// The defect this pins came out of the fix that
// stopped one filter's declaration vetoing another by asking whether
// *any* subscription the client holds wants the record; a `$share` filter
// matches through InnerFilter and can carry no declaration, so it always
// wanted everything, and a client holding one beside a declared ordinary
// filter had its declaration voided outright. Every member of a partition
// group that also held a share filter was served every slice - the
// duplicate class again, one arrangement over from the one being fixed.
//
// **Counted across the group rather than attributed per delivery.** The
// share group hands each record to exactly one member, so a group whose
// members between them receive more records than were published is being
// fed twice - which is the whole of the bug and needs no subscription
// identifiers to see.
func TestAShareFilterDoesNotVoidADeclarationOnTheSameClient(t *testing.T) {
	const count = 2
	h := start(t)

	// Topics all owned by slice 0, so a member declaring slice 1 owns none
	// of them and must receive none through its ordinary subscription.
	var topics []string
	for i := 0; len(topics) < 8; i++ {
		topic := fmt.Sprintf("loose/z/m%d", i)
		if ownerOf(topic, count) == 0 {
			topics = append(topics, topic)
		}
	}

	// X holds a declared ordinary filter AND a share filter over the same
	// topics. Y holds the share filter only, so the group has two members
	// and its picks are visible as Y's deliveries.
	x := connect(t, h, "x-share-and-declared", true, false)
	if sa := x.SubSliced(t, "loose/z/#", count, 1); sa.Reasons[0] > 2 {
		t.Fatalf("the declared ordinary filter was refused 0x%02X", sa.Reasons[0])
	}
	x.Sub(t, "$share/g3/loose/z/#", 1)
	y := connect(t, h, "y-share-only", true, false)
	y.Sub(t, "$share/g3/loose/z/#", 1)
	time.Sleep(400 * time.Millisecond)

	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	gotX, gotY := x.Count(), y.Count()
	if total := gotX + gotY; total != len(topics) {
		t.Errorf("%d published, %d delivered across the share group (x=%d, y=%d): "+
			"x declared slice 1 of %d and owns none of these topics, so every "+
			"record it received on its ordinary subscription is one its "+
			"declaration excluded - a `$share` filter answering for the client "+
			"voids the declaration on every other subscription it holds",
			len(topics), total, gotX, gotY, count)
	}

	// **And the share group still works**, which is what stops this passing
	// against a broker that had simply stopped delivering.
	if gotX+gotY == 0 {
		t.Fatal("the share group received nothing at all, so the count above " +
			"asserted nothing")
	}
}

// RFC 0002's `SUBACK` table and RFC 0003 "Where it applies"
//
// **A declaration is packet-level; refusing one is not always.** The table
// said 0x83 answers "on every filter of the packet" for a declaration saguin
// refuses, and listed `$share` and a queue's form beside a malformed one.
// Those are not the same case. A malformed or self-inconsistent declaration
// names no filter, so there is no per-filter answer to give and the packet
// is refused whole. A *valid* declaration on a `$share` filter names that
// filter exactly: it is refused and the packet's other filters are granted,
// with the declaration applied to them.
//
// So [0x01, 0x83] from one SUBSCRIBE is reachable, which is what the table
// now says and what nothing here drove. A client author reading the old
// sentence would have treated that answer as impossible.
//
// **Both halves are asserted**, because the reason codes alone would pass
// against a broker that granted the plain filter and then quietly dropped
// the declaration it was carrying, which is the more expensive failure: it
// serves a partitioned member every slice.
func TestAShareFilterInAMixedPacketIsRefusedAloneAndTheRestKeepTheSlice(t *testing.T) {
	const count = 2
	h := start(t)

	c := connect(t, h, "mixed-packet", true, false)
	sa, err := c.SubDeclaring(t,
		[]string{"events/#", "$share/gmix/events/#"},
		[][2]string{{"saguin-filter", fmt.Sprintf("topic_hash(%d, 0)", count)}})
	if sa == nil {
		t.Fatalf("subscribe: %v", err)
	}
	if len(sa.Reasons) != 2 {
		t.Fatalf("SUBACK carried %d reason codes for a SUBSCRIBE naming two "+
			"filters, want 2", len(sa.Reasons))
	}
	if sa.Reasons[0] > 2 {
		t.Fatalf("the plain filter answered 0x%02X: a declaration a `$share` "+
			"filter cannot carry refuses that filter, not the packet around it",
			sa.Reasons[0])
	}
	if sa.Reasons[1] != 0x83 {
		t.Fatalf("the `$share` filter answered 0x%02X, want 0x83", sa.Reasons[1])
	}
	time.Sleep(300 * time.Millisecond)

	// Publishing across both slices is what separates "the declaration was
	// applied to the granted filter" from "the refusal took it away and
	// everything arrived".
	topics := topicsFor(t, "events", count, 12)
	p := connect(t, h, "mixed-producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	var want []string
	for _, topic := range topics {
		if ownerOf(topic, count) == 0 {
			want = append(want, topic)
		}
	}
	if len(want) == 0 || len(want) == len(topics) {
		t.Fatalf("%d of %d topics fell in slice 0: a run where one slice is "+
			"empty asserts nothing about the declaration", len(want), len(topics))
	}
	got := map[string]bool{}
	for _, r := range c.All() {
		got[r.Topic] = true
	}
	if len(got) != len(want) {
		t.Fatalf("%d distinct topics arrived out of %d published, want the %d "+
			"in slice 0: a granted filter keeps the declaration its packet "+
			"carried even when another filter in the same packet was refused "+
			"for it", len(got), len(topics), len(want))
	}
	for _, topic := range want {
		if !got[topic] {
			t.Errorf("%s is in slice 0 and did not arrive", topic)
		}
	}
}

// RFC 0002 "Retained messages", RFC 0003 "`latest`"
//
// **The one claim that was reasoned about and never driven.** The
// `latest` current-state pass is scoped to the filter that asked, and the
// question left open was whether that narrows what an inbound bridge
// receives, since a bridge re-subscribes on every reconnect.
//
// Both the report and the response reasoned it from "a bridge holds one
// filter per channel". That is not quite what the code does - a bridge
// sends one filter per configured inbound rule, in a single SUBSCRIBE
// (internal/bridge/bridge.go) - so the shape to drive is a multi-filter
// SUBSCRIBE, not a single-filter one.
//
// It is unaffected for a better reason than the one given: the scoping is
// applied per *granted filter*, and every filter in a packet gets its own
// pass, so the union a multi-filter SUBSCRIBE receives is exactly what it
// received before. Only a *later* SUBSCRIBE no longer re-sends an earlier
// subscription's state.
func TestOneSubscribeWithSeveralFiltersIsServedStateForEachOfThem(t *testing.T) {
	h := start(t)
	p := connect(t, h, "setter", true, false)
	for topic, v := range map[string]string{
		"state/one/a": "va", "state/two/b": "vb", "state/three/c": "vc",
	} {
		p.Pub(t, topic, v)
	}
	time.Sleep(400 * time.Millisecond)

	// The bridge's shape: several filters, one SUBSCRIBE.
	c := connect(t, h, "bridge-shaped", true, false)
	codes := c.SubMany(t, []string{"state/one/+", "state/two/+", "state/three/+"}, 1)
	for i, code := range codes {
		if code > 2 {
			t.Fatalf("filter %d refused 0x%02X", i, code)
		}
	}

	got := map[string]string{}
	deadline := time.After(4 * time.Second)
	for len(got) < 3 {
		select {
		case r := <-c.Ch:
			got[r.Topic] = r.Payload
		case <-deadline:
			t.Fatalf("a SUBSCRIBE naming three filters was served state for %v of "+
				"them: scoping the pass to the filter that asked must not narrow a "+
				"packet that names several, because every filter in it asked",
				len(got))
		}
	}
	for topic, want := range map[string]string{
		"state/one/a": "va", "state/two/b": "vb", "state/three/c": "vc",
	} {
		if got[topic] != want {
			t.Errorf("%q served %q, want %q", topic, got[topic], want)
		}
	}
	t.Logf("three filters in one SUBSCRIBE, state for all three: %v", got)
}

// RFC 0003 "Client-declared partitioning" - repeating saguin-filter is an OR
//
// **The one semantic the call syntax added, and it needs its own drive.** A
// member covering a failed peer's share declares two calls in one partition
// space, and the two ways that can go wrong are opposite: it is served only
// one of its slices, or it is served everything because the second
// declaration replaced the first rather than joining it. Both look like
// success from the member's own side - one is a channel with less traffic
// than expected, the other is one member doing everybody's work.
//
// The unit table asserts what partitioning() returns; this asserts what
// comes out of a running broker, over the wire, for a member that declared
// two slices and nothing else.
func TestRepeatingTheFilterHoldsBothSlicesAndNoMore(t *testing.T) {
	const (
		count          = 4
		mine, alsoMine = 0, 2
	)
	h := start(t)

	c := connect(t, h, "two-slices", true, false)
	if sa := c.SubSliced(t, "events/#", count, mine, alsoMine); sa.Reasons[0] > 2 {
		t.Fatalf("a member declaring two slices of one space was refused 0x%02X",
			sa.Reasons[0])
	}
	time.Sleep(300 * time.Millisecond)

	topics := topicsFor(t, "events", count, 16)
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "r")
	}
	time.Sleep(1500 * time.Millisecond)

	got := took(c)
	held := 0
	for _, topic := range topics {
		owner := ownerOf(topic, count)
		_, arrived := got[topic]
		switch {
		case owner == mine || owner == alsoMine:
			if !arrived {
				t.Errorf("%q is in slice %d, which this member declared, and it did "+
					"not arrive: one saguin-filter replaced the other rather than "+
					"joining it", topic, owner)
				continue
			}
			held++
		case arrived:
			t.Errorf("%q is in slice %d, which this member did not declare, and it "+
				"arrived anyway: an OR that widened past what was asked for",
				topic, owner)
		}
	}

	// **Both slices have to have been driven**, or a member served only one
	// of them passes this by owning nothing in the other.
	covered := map[int]bool{}
	for topic := range got {
		covered[ownerOf(topic, count)] = true
	}
	if len(covered) != 2 {
		t.Fatalf("%d records arrived covering slices %v of %d: the OR was not "+
			"exercised, so this test asserted less than it reads",
			held, covered, count)
	}
	t.Logf("%d of %d topics arrived, and they are slices %d and %d",
		held, len(topics), mine, alsoMine)
}

// On the wire - RFC 0003 "The reserved
// prefix on a client's own packets"
//
// **A grant is an answer, and it was the wrong one.** Every shape below was
// answered `SUBACK [1]` and served the whole channel before this rule: the
// retired two-property pair, a typo of `saguin-filter`, a typo of
// `saguin-deletions`, and a name that never existed. The unit test holds
// the predicate; this holds what a client is actually told.
func TestAReservedPropertySaguinDoesNotReadIsRefusedOnTheWire(t *testing.T) {
	h := start(t)
	for i, tc := range []struct {
		name string
		ups  [][2]string
	}{
		{"the retired pair", [][2]string{
			{"saguin-partition-count", "2"}, {"saguin-partition-index", "0"}}},
		{"a typo of saguin-filter", [][2]string{{"saguin-fitler", "topic_hash(2, 0)"}}},
		{"a typo of saguin-deletions", [][2]string{{"saguin-deletion", "1"}}},
		{"a name that never existed", [][2]string{{"saguin-slice", "0/2"}}},
		{"beside a declaration that is itself valid", [][2]string{
			{"saguin-filter", "topic_hash(2, 0)"}, {"saguin-slice", "0/2"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := connect(t, h, fmt.Sprintf("reserved-%d", i), true, false)
			sa, err := c.SubDeclaring(t, []string{"events/#", "state/#"}, tc.ups)
			if sa == nil {
				t.Fatalf("no SUBACK arrived (%v): RFC 0002 says a refusal is total "+
					"and the client stays connected, so a disconnect here turns one "+
					"packet into a reconnect loop", err)
			}
			for j, r := range sa.Reasons {
				if r != 0x83 {
					t.Errorf("filter %d answered 0x%02X, want 0x83: a reserved "+
						"property saguin does not read is an instruction it cannot "+
						"carry out, and a grant says it did", j, r)
				}
			}
			// **And the connection is still usable**, which is the half a
			// disconnect would break.
			if sa := c.Sub(t, "events/#", 1); sa.Reasons[0] > 2 {
				t.Fatalf("a correct subscribe after the refusal answered 0x%02X",
					sa.Reasons[0])
			}
		})
	}

	// **The names saguin does read still work**, or this rule has broken the
	// features it protects - `saguin-deletions` is the bridge's own.
	t.Run("the names saguin reads are unaffected", func(t *testing.T) {
		c := connect(t, h, "reserved-ok", true, false)
		sa, err := c.SubDeclaring(t, []string{"state/#"}, [][2]string{
			{"saguin-filter", "topic_hash(2, 0)"}, {"saguin-deletions", "1"}})
		if sa == nil {
			t.Fatalf("no SUBACK: %v", err)
		}
		if sa.Reasons[0] > 2 {
			t.Fatalf("a subscription carrying only properties saguin reads answered "+
				"0x%02X: the allow-list has outgrown the code that fills it",
				sa.Reasons[0])
		}
	})
}

// The same rule on CONNECT, where saguin defines no property at all - which
// is why the rule goes in now rather than after one exists.
func TestAReservedPropertyOnConnectIsRefused(t *testing.T) {
	h := start(t)
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID: "reserved-connect", CleanStart: true, KeepAlive: 0,
		Properties: &paho.ConnectProperties{
			RequestProblemInfo: true,
			User:               paho.UserProperties{{Key: "saguin-filter", Value: "topic_hash(2, 0)"}},
		},
	})
	if ca == nil {
		t.Fatalf("no CONNACK arrived: %v", err)
	}
	if ca.ReasonCode != 0x83 {
		t.Errorf("CONNACK 0x%02X, want 0x83: saguin reads no property under its own "+
			"prefix on CONNECT, and taking the connection says it honoured one",
			ca.ReasonCode)
	}
}

// The UNSUBSCRIBE exception, told on the wire
//
// **The one packet where refusing is worse than the silence, so the
// silence is lifted instead.** A refused UNSUBSCRIBE leaves a client
// receiving what it asked to stop receiving; a log line tells only the
// operator. The UNSUBACK is the packet saguin already answers, and MQTT 5
// allows User Properties on it, so the client is told in the same round
// trip and nobody stays subscribed.
func TestAnUnreadPropertyOnUnsubscribeIsAnsweredNotRefused(t *testing.T) {
	h := start(t)
	// **Request Problem Information, because MQTT-3.1.2-29 gates this.** A
	// server must not put a Reason String or a User Property on any packet
	// but PUBLISH, CONNACK and DISCONNECT unless the client asked for
	// problem information - which is why the log line stays: a client that
	// did not ask cannot be told on the wire at all, whatever saguin does.
	pc := connectAsking(t, h, "unsub-told")
	if _, err := pc.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ua, err := pc.Unsubscribe(context.Background(), &paho.Unsubscribe{
		Topics: []string{"events/#"},
		Properties: &paho.UnsubscribeProperties{
			User: paho.UserProperties{
				{Key: "saguin-oops", Value: "1"},
				{Key: "trace-id", Value: "abc"},
			},
		},
	})
	if ua == nil {
		t.Fatalf("no UNSUBACK: %v", err)
	}
	// **Unsubscribed, not refused**: the whole argument for the exception.
	for i, r := range ua.Reasons {
		if r > 0x11 { // 0x00 success, 0x11 no subscription existed
			t.Errorf("filter %d answered 0x%02X: refusing leaves the client "+
				"subscribed to what it asked to stop receiving", i, r)
		}
	}

	told, echoed := "", false
	for _, u := range ua.Properties.User {
		if u.Key == "saguin-unread" {
			told = u.Value
		}
		if u.Key == "saguin-oops" {
			echoed = true
		}
	}
	if told != "saguin-oops" {
		t.Errorf("the UNSUBACK named %q as unread, want \"saguin-oops\": without it "+
			"the client is told nothing and only the operator's log knows", told)
	}
	// **And saguin does not hand back the client's own reserved name**, on a
	// packet saguin wrote, over the prefix it claims as its own.
	if echoed {
		t.Error("the UNSUBACK echoed the client's own saguin- property: the prefix " +
			"is the broker's, and a packet saguin writes must not carry a client's " +
			"name under it")
	}
}

// RFC 0003 "The reserved prefix on a
// client's own packets"
//
// **The prefix is saguin's on the packets saguin writes, too.** The
// substrate copies a request's User Properties onto the acknowledgement it
// answers with, so a client's own `saguin-filter` came back on the SUBACK -
// and, on a refusal, the very `saguin-slice` it had just been refused for.
// It was scrubbed from the UNSUBACK one commit earlier; this is the same
// mechanism on the acknowledgements either side of it.
//
// **Every acknowledgement saguin can reach, not the one that was reported.**
// The SUBACK was reported and the PUBACK could not be driven; driving it
// found the same echo on a publish saguin refuses. The list here is what a
// client can provoke, and each is asserted rather than reasoned about.
//
// Request Problem Information is set throughout, because MQTT-3.1.2-29
// suppresses these properties entirely without it - so a test that omitted
// it would pass while proving nothing.
func TestNoAcknowledgementEchoesAClientsReservedName(t *testing.T) {
	h := start(t)
	c := connectAsking(t, h, "ack-echo")

	// **Nothing the client sent comes back on an acknowledgement**, and the
	// rule is wider than the prefix this test was first written for.
	//
	// MQTT puts a property on an acknowledgement in a different category
	// from one on a PUBLISH: [MQTT-3.1.2-29] lets a client switch these off
	// with Request Problem Information, and exempts PUBLISH because a
	// publish's properties are application data being forwarded rather than
	// problem information about a failure. Section 3.4.2.2.3 says it from
	// the other end - the property is the *sender's*, and the sender of a
	// PUBACK is the broker. So an echo puts application data in the field
	// the protocol reserves for what the broker has to say.
	//
	// The `saguin-` half of the check stands and is the sharper of the two:
	// a reserved name on a packet saguin writes is a claim in saguin's own
	// vocabulary that saguin did not make.
	check := func(what string, user paho.UserProperties, _ bool) {
		t.Helper()
		for _, p := range user {
			if strings.HasPrefix(p.Key, "saguin-") && p.Key != "saguin-unread" {
				t.Errorf("the %s carries %s=%q, which the client wrote: it is a "+
					"packet saguin writes, under the prefix saguin claims",
					what, p.Key, p.Value)
			}
			if p.Key == "x-app" {
				t.Errorf("the %s carries the client's own x-app back at it. On an "+
					"acknowledgement a User Property is the sender's diagnostic "+
					"information ([MQTT-3.1.2-29] lets a client decline it), so a "+
					"publisher's own labels do not belong there", what)
			}
		}
	}

	// A granted SUBSCRIBE, whose declaration is legal and came back.
	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
		Properties: &paho.SubscribeProperties{User: paho.UserProperties{
			{Key: "saguin-filter", Value: "topic_hash(2, 0)"},
			{Key: "x-app", Value: "v2"}}},
	})
	if sa == nil {
		t.Fatalf("no SUBACK: %v", err)
	}
	check("granted SUBACK", sa.Properties.User, true)

	// A refused SUBSCRIBE, echoing the name it was refused for.
	sa, err = c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
		Properties: &paho.SubscribeProperties{User: paho.UserProperties{
			{Key: "saguin-slice", Value: "0/2"}, {Key: "x-app", Value: "v2"}}},
	})
	if sa == nil {
		t.Fatalf("no SUBACK for the refusal: %v", err)
	}
	if sa.Reasons[0] != 0x83 {
		t.Fatalf("the refusal answered 0x%02X, want 0x83: this case is only "+
			"interesting because the property is why it was refused", sa.Reasons[0])
	}
	check("refused SUBACK", sa.Properties.User, true)

	// An accepted publish into a channel.
	pa, err := c.Publish(context.Background(), &paho.Publish{
		Topic: "events/ack", QoS: 1, Payload: []byte("p"),
		Properties: &paho.PublishProperties{User: paho.UserProperties{
			{Key: "saguin-offset", Value: "999"}, {Key: "x-app", Value: "v2"}}},
	})
	if pa == nil {
		t.Fatalf("no PUBACK: %v", err)
	}
	check("PUBACK", pa.Properties.User, true)

	// And the UNSUBACK, which carries saguin's own answer and not the
	// client's name.
	ua, err := c.Unsubscribe(context.Background(), &paho.Unsubscribe{
		Topics: []string{"events/#"},
		Properties: &paho.UnsubscribeProperties{User: paho.UserProperties{
			{Key: "saguin-oops", Value: "1"}, {Key: "x-app", Value: "v2"}}},
	})
	if ua == nil {
		t.Fatalf("no UNSUBACK: %v", err)
	}
	check("UNSUBACK", ua.Properties.User, true)
}

// The member of its class that has no hook - a publish saguin refuses.
//
// **This is the one that has no hook.** An ACL denial is answered before
// OnPublish is ever called: the substrate builds the PUBACK from the raw
// packet and returns, so a fix at saguin's publish hook would have left
// exactly the case a client is most likely to inspect - the acknowledgement
// that says no, carrying the client's own reserved name as though saguin
// had put it there. It is closed at OnPacketEncode instead, which every
// outgoing packet passes.
func TestARefusedPublishDoesNotEchoAReservedName(t *testing.T) {
	h := start(t)
	acl := filepath.Join(t.TempDir(), "acl.yaml")
	if err := os.WriteFile(acl, []byte(`
roles:
  worker:
    - channel: jobs
      allow: [consume]
users:
  worker-1: [worker]
`), 0o600); err != nil {
		t.Fatalf("write acl: %v", err)
	}
	f, err := authz.Load(acl, h.B.Registry(), 0)
	if err != nil {
		t.Fatalf("load acl: %v", err)
	}
	h.B.Authorize(authz.New(f, h.B.Registry()))

	// connectAsking, not connect: without Request Problem Information
	// MQTT-3.1.2-29 hides every User Property on the acknowledgement and
	// this test would pass while proving nothing.
	c := connectAsking(t, h, "denied-echo")

	ack, err := c.Publish(context.Background(), &paho.Publish{
		Topic: "events/a", QoS: 1, Payload: []byte("x"),
		Properties: &paho.PublishProperties{User: paho.UserProperties{
			{Key: "saguin-offset", Value: "999"}, {Key: "x-app", Value: "v2"}}},
	})
	if ack == nil {
		t.Fatalf("no acknowledgement: %v", err)
	}
	if ack.ReasonCode != 0x87 {
		t.Fatalf("the publish answered 0x%02X, want 0x87: this case is only "+
			"interesting on a refusal, which is the path with no hook",
			ack.ReasonCode)
	}
	for _, p := range ack.Properties.User {
		if strings.HasPrefix(p.Key, "saguin-") {
			t.Errorf("the refused PUBACK carries %s=%q, which the client wrote: "+
				"an acknowledgement saguin sends must not name something saguin "+
				"does not read, under saguin's own prefix", p.Key, p.Value)
			continue
		}
		t.Errorf("the refused PUBACK carries %s=%q back at the client. On an "+
			"acknowledgement a User Property is the sender's diagnostic "+
			"information (section 3.4.2.2.3), and [MQTT-3.1.2-29] lets a client "+
			"decline it - so the publisher's own labels do not belong there",
			p.Key, p.Value)
	}

	// **The reason still reaches the client**, which is the half that would
	// go unnoticed if this only counted what was dropped: what an
	// acknowledgement is *for* is what saguin has to say about the refusal.
	if ack.Properties.ReasonString == "" {
		t.Error("the refused PUBACK carries no reason string, so the client is " +
			"told the code and nothing about why")
	}
}

// RFC 0003 "Client-declared partitioning" - a member's slice survives a
// restart with its session.
//
// **Nothing on the wire carries the declaration again.** It arrives on a
// SUBSCRIBE, and a session that comes back to Session Present 1 sends none:
// a member restored without its declaration is subscribed and undeclared,
// and is served the whole channel, including every other member's slice.
// That is the failure the link-churn soak found across a disconnect, and a
// restart is the same shape with the broker in between.
//
// Measured rather than asserted from a list: what slice 0 takes is read off
// this broker before the restart, and the test refuses to prove anything on
// a slice that took everything or nothing.
func TestARestoredMembersSliceComesBackWithIt(t *testing.T) {
	dir := t.TempDir()
	h := startDurable(t, dir)

	first := connect(t, h, "sliced", false, false)
	first.SubSliced(t, "events/+", 2, 0)
	time.Sleep(300 * time.Millisecond)
	if got := h.B.DeclaredSlices("sliced"); len(got) != 1 {
		t.Fatalf("the broker holds %v for a client that declared one slice, so this test "+
			"never had a declaration to lose", got)
	}

	topics := make([]string, 0, 30)
	for i := range 30 {
		topics = append(topics, fmt.Sprintf("events/k-%02d", i))
	}
	p := connect(t, h, "producer", true, false)
	for _, topic := range topics {
		p.Pub(t, topic, "one")
	}
	mine := map[string]bool{}
	for {
		r, ok := first.Await(t, 700*time.Millisecond)
		if !ok {
			break
		}
		mine[r.Topic] = true
	}
	if len(mine) == 0 || len(mine) == len(topics) {
		t.Fatalf("slice 0 of 2 took %d of %d topics: a slice that takes everything or "+
			"nothing cannot show a declaration being lost", len(mine), len(topics))
	}
	first.Close()

	h.Stop()
	h2 := startDurable(t, dir)

	if got := h2.B.DeclaredSlices("sliced"); len(got) != 1 || got["events/+"] != 2 {
		t.Fatalf("the restarted broker holds %v for the restored member, want its "+
			"declaration of 2 on events/+", got)
	}

	// Published while it is away, so what it gets is what the resume serves
	// it - filtered by the declaration it came back with, or not at all.
	p2 := connect(t, h2, "producer", true, false)
	for _, topic := range topics {
		p2.Pub(t, topic, "two")
	}

	again := connect(t, h2, "sliced", false, false)
	got := map[string]bool{}
	for {
		r, ok := again.Await(t, 700*time.Millisecond)
		if !ok {
			break
		}
		got[r.Topic] = true
	}
	for topic := range got {
		if !mine[topic] {
			t.Errorf("the restored member was served %q, which is not in its slice: it came "+
				"back subscribed and undeclared, so it is reading every other member's records",
				topic)
		}
	}
	for topic := range mine {
		if !got[topic] {
			t.Errorf("the restored member was not served %q, which is in its slice", topic)
		}
	}
	t.Logf("slice 0 of 2 took %d of %d topics, before and after the restart", len(mine), len(topics))
}
