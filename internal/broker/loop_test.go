package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/bridge"
	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// What a bridge does not do twice, and what it does not do at all.
//
// **Two sagüins rather than a sagüin and a stub**, because the guard has two
// halves and a stub exercises one: the mark is written by the broker that
// pulls a record in over its own bridge and read by the rule that would push
// it out again, so nothing about a loop can be shown with only one end real.
// Both are in-process brokers on their own ports, which is what test rule 6
// asks for - the clients between them are Paho and the brokers are sagüin.
//
// **The guarantee is per link, and it is whole there.** What a bridge pulled
// in is never pushed out; what the peer echoes is refused by No Local. Across
// links it is topology rather than mechanism: a record a peer *pushed* here
// arrives as an ordinary client publish - which is the whole of the "the far
// end needs no bridge configuration" promise - and no local fact can tell it
// from one a device published. A cycle therefore loops, and the operator's
// obligation to keep the bridge graph a tree is RFC 0002\'s, not a check
// sagüin can make. Both orientations of a chain are pinned below, each with
// its true outcome, because the pair is what says which.
//
// **What a failure looks like here**: nothing errors. A record multiplies, a
// channel fills, and the first sign is a disk. So each of these counts what
// arrived, and each waits on a *later* record rather than on a duration - an
// outbound rule drains a channel in offset order, so a record published after
// the suspected duplicate arriving at the far end proves the duplicate is not
// behind it. A sleep would be a guess that gets shorter every time the
// machine gets busier.

// holds asserts exactly what a reader received, in order.
//
// **Contents rather than a count, and the difference is a test that can
// pass while the thing it guards is broken.** Every assertion here follows
// an awaitCount, which returns as soon as enough records have arrived - so
// on a broker whose loop guard had failed, `[hello, probe, probe]` satisfies
// a wait for three and a length check passes on it. The record that would
// have exposed the duplicate lands after the assertion has run. Comparing
// elements closes the race rather than narrowing it: the wrong third record
// fails whenever the check fires.
func holds(t *testing.T, who string, c *client, want ...string) {
	t.Helper()
	got := payloads(c.All())
	if !slices.Equal(got, want) {
		t.Errorf("%s holds %v, want exactly %v", who, got, want)
	}
}

// **SAGUIN_TEST_STORAGE=sqlite is what runs these suites on sqlite**, so it
// has to be proved to do it: a knob that parsed and changed nothing would
// run the memory suite twice and call the second run sqlite.
func TestTheStorageKnobPutsAMemoryBrokerOnSQLite(t *testing.T) {
	t.Setenv("SAGUIN_TEST_STORAGE", "sqlite")
	if h := start(t); h.DB == nil {
		t.Error("SAGUIN_TEST_STORAGE=sqlite started a broker with no sqlite provider")
	}
	if h := startDurable(t, t.TempDir()); h.DB != nil {
		t.Error("SAGUIN_TEST_STORAGE=sqlite moved a broker with a snapshot directory off " +
			"the storage its test chose")
	}
}

// bridgeOn runs a bridge hosted by one broker against another, until the
// test ends. The rule is `direction: both` on one filter, identity mapping -
// the arrangement an operator writes first, and the one loops live in.
func bridgeOn(t *testing.T, host *harness, peerAddr, name, filter string) config.Bridge {
	t.Helper()
	yaml := fmt.Sprintf(`broker:
  id: t
`+memStorage+`channels:
  events:
    type: append
bridges:
  %s:
    peer: tcp://%s
    client_id: %s
    topics:
      - filter: %s
        direction: both
`, name, peerAddr, name, filter)
	path := filepath.Join(t.TempDir(), name+".yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write the bridge configuration: %v", err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own bridge configuration does not load: %v", err)
	}
	cfg := f.BridgeSet()[0]

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(newTestWriter(t), &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	link := host.B.NewBridgeClient(cfg.Name)
	br := bridge.New(cfg, link, limits, log, link)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = br.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the bridge did not stop")
		}
	})
	return cfg
}

// subscribedAt waits until the bridge has a subscription at its peer that
// covers topic. Publishing before it lands is not a record the bridge lost,
// it is one never sent to it, and a test that raced there would be flaky
// about the wrong thing.
func subscribedAt(t *testing.T, peer *mqtt.Server, clientID, topic string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if _, ok := peer.Clients.Get(clientID); !ok {
			continue
		}
		// This bridge's own subscription, not any: every one of these tests
		// has a reader subscribed to the same filter at the same broker, and
		// a check for "somebody is subscribed" would be satisfied by the
		// reader and return before the bridge had connected.
		if _, ok := peer.Topics.Subscribers(topic).Subscriptions[clientID]; ok {
			return
		}
	}
	t.Fatalf("the bridge %q never subscribed to anything covering %q at its peer", clientID, topic)
}

// **One link, `direction: both`, and nothing is duplicated** - the headline
// an operator sees, on a channel topic. One filter, both directions,
// identity mapping, one broker dialling the other.
//
// Two mechanisms hold it and each covers a different record here, which is
// why the payloads are asserted rather than a total. No Local stops the peer
// echoing this bridge's own publish back down the connection it arrived on,
// which is what would double `hello` - and it was ignored on every channel
// delivery until the flag was implemented, so this counted three records and
// found five. The mark stops a record the bridge *pulled in* going back out,
// which is what would double `probe`.
func TestOneLinkBothWaysDuplicatesNothingOnAChannelTopic(t *testing.T) {
	a, b := start(t), start(t)
	cfg := bridgeOn(t, a, b.Addr, "a-to-b", "events/#")

	atA := connect(t, a, "reader-a", true, false)
	atA.Sub(t, "events/#", 1)
	atB := connect(t, b, "reader-b", true, false)
	atB.Sub(t, "events/#", 1)
	subscribedAt(t, b.Srv, cfg.ClientID, "events/one")

	fromA := connect(t, a, "producer-a", true, false)
	fromB := connect(t, b, "producer-b", true, false)

	// Out: A's own record crosses to B.
	fromA.Pub(t, "events/one", "hello")
	awaitCount(t, atB, 1, 5*time.Second)
	if got := payloads(atB.All()); len(got) != 1 {
		t.Fatalf("the peer holds %v, want one copy of hello - with nothing across the "+
			"link the counts below would pass on a broken bridge", got)
	}

	// In: B's own record crosses to A, where it is marked.
	fromB.Pub(t, "events/probe", "probe")
	awaitCount(t, atA, 2, 5*time.Second)
	if got := payloads(atA.All()); len(got) != 2 {
		t.Fatalf("the origin holds %v, want hello and probe", got)
	}

	// The barrier. `after` is stored at A behind the marked `probe`, and an
	// outbound rule drains in offset order, so `after` arriving at B proves
	// a forwarded `probe` is not still on its way.
	fromA.Pub(t, "events/after", "after")
	awaitCount(t, atB, 3, 5*time.Second)

	// A second copy of `probe` is a record the bridge brought in and sent
	// back out; a second copy of `hello` is the peer echoing this bridge's
	// own publish back at it. Either one displaces a name below.
	holds(t, "the peer", atB, "hello", "probe", "after")
	holds(t, "the origin", atA, "hello", "probe", "after")
}

// **And on a broadcast topic, where both mechanisms are different code.**
//
// A channel stores its records, so the mark is a field on the record and the
// outbound rule reads it back out of the store. Nothing stores a broadcast
// one: the mark rides the in-process fan-out instead, and the outbound rule
// sees the record as it passes or never. A test of one says nothing about
// the other, so this is the same arrangement on a topic no channel claims.
func TestOneLinkBothWaysDuplicatesNothingOnABroadcastTopic(t *testing.T) {
	a, b := start(t), start(t)
	cfg := bridgeOn(t, a, b.Addr, "a-to-b", "weather/#")

	atA := connect(t, a, "reader-a", true, false)
	atA.Sub(t, "weather/#", 1)
	atB := connect(t, b, "reader-b", true, false)
	atB.Sub(t, "weather/#", 1)
	subscribedAt(t, b.Srv, cfg.ClientID, "weather/hallway")

	fromA := connect(t, a, "producer-a", true, false)
	fromB := connect(t, b, "producer-b", true, false)

	fromA.Pub(t, "weather/hallway", "21.5")
	awaitCount(t, atB, 1, 5*time.Second)
	if got := payloads(atB.All()); len(got) != 1 {
		t.Fatalf("the peer holds %v, want one reading", got)
	}

	fromB.Pub(t, "weather/roof", "probe")
	awaitCount(t, atA, 2, 5*time.Second)
	if got := payloads(atA.All()); len(got) != 2 {
		t.Fatalf("the origin holds %v, want the reading and probe", got)
	}

	// The same barrier, and the same ordering fact one layer along: the live
	// fan-out hands the outbound rule its records through one queue, drained
	// in the order they were published.
	fromA.Pub(t, "weather/cellar", "after")
	awaitCount(t, atB, 3, 5*time.Second)

	// On broadcast the mark is the in-process fan-out rather than a stored
	// field, so this fails on its own account rather than with the channel
	// case above.
	holds(t, "the peer", atB, "21.5", "probe", "after")
	holds(t, "the origin", atA, "21.5", "probe", "after")
}

// **A record this broker pulled in is not pushed onward**, which is one of
// the two chain orientations and the one that stops.
//
// The middle broker hosts both bridges: it dials A and pulls, dials C and
// pushes. The mark is a fact one broker holds about one record, so this is
// the arrangement where it can be read at all - and the record stops here.
//
// The rule is a blanket rather than a comparison against the bridge's own
// name, because a name permits a second hop and a second hop cannot be
// bounded: every broker in a ring would see a name that is not its own. What
// an operator does instead is re-origination - a broker meant to relay
// publishes the record again as its own, which is what dead-lettering does.
func TestAPulledRecordIsNotPushedOnward(t *testing.T) {
	a, b, c := start(t), start(t), start(t)
	// B is the middle, and hosts both bridges: that is where a relay would
	// happen, because the mark is a fact one broker holds about one record.
	toA := bridgeOn(t, b, a.Addr, "b-to-a", "events/#")
	toC := bridgeOn(t, b, c.Addr, "b-to-c", "events/#")

	atB := connect(t, b, "reader-b", true, false)
	atB.Sub(t, "events/#", 1)
	atC := connect(t, c, "reader-c", true, false)
	atC.Sub(t, "events/#", 1)
	subscribedAt(t, a.Srv, toA.ClientID, "events/one")
	subscribedAt(t, c.Srv, toC.ClientID, "events/one")

	fromA := connect(t, a, "producer-a", true, false)
	fromA.Pub(t, "events/one", "hello")
	awaitCount(t, atB, 1, 5*time.Second)
	if got := payloads(atB.All()); len(got) != 1 {
		t.Fatalf("the middle broker holds %v, want hello - with nothing across the first "+
			"link the assertion below is about a hop that never happened", got)
	}

	// The barrier, which doubles as the control: B's own record does cross,
	// so "did not relay" cannot pass by the second bridge being broken. It
	// is stored at B behind `hello`, and the rule drains in offset order.
	fromB := connect(t, b, "producer-b", true, false)
	fromB.Pub(t, "events/two", "from b")
	awaitCount(t, atC, 1, 5*time.Second)
	if got := payloads(atC.All()); len(got) == 0 || got[0] != "from b" {
		t.Fatalf("the third broker holds %v, want from b first - the bridge carries what "+
			"this broker originated", got)
	}

	if got := payloads(atC.All()); len(got) != 1 {
		t.Errorf("the third broker holds %v, want only from b: a bridge does not relay "+
			"what another bridge brought it, and a chain that worked here would be a "+
			"cycle nothing could stop three brokers later", got)
	}
}

// **A record a peer pushed here does forward onward**, which is the other
// orientation and the one that carries.
//
// Every link dials the next broker, so each hop is an outbound rule
// publishing to a peer that has no bridge configuration at all - and at that
// peer the record is an ordinary client publish, indistinguishable from a
// device\'s. It is therefore forwarded on, which is what makes edge to hub to
// cloud work when the links dial toward the cloud.
//
// **This is also why a cycle loops.** The mark cannot cross the wire - a
// record arrives here as a publish, and a broker that trusted a property
// saying otherwise would let anything on the wire stop its own records being
// forwarded. So the same fact that makes this chain carry makes a ring of
// three grow without bound: measured at forty thousand copies of one record
// in three seconds. Keeping the bridge graph a tree is the operator\'s
// obligation, stated in RFC 0002, and there is nothing decidable here to
// refuse it with.
func TestAPushedRecordForwardsOnward(t *testing.T) {
	a, b, c := start(t), start(t), start(t)
	// Each broker dials the next, so both hops are pushes.
	ab := bridgeOn(t, a, b.Addr, "a-to-b", "events/#")
	bc := bridgeOn(t, b, c.Addr, "b-to-c", "events/#")

	atB := connect(t, b, "reader-b", true, false)
	atB.Sub(t, "events/#", 1)
	atC := connect(t, c, "reader-c", true, false)
	atC.Sub(t, "events/#", 1)
	subscribedAt(t, b.Srv, ab.ClientID, "events/one")
	subscribedAt(t, c.Srv, bc.ClientID, "events/one")

	fromA := connect(t, a, "producer-a", true, false)
	fromA.Pub(t, "events/one", "hello")
	awaitCount(t, atC, 1, 5*time.Second)
	if got := payloads(atC.All()); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("the far end holds %v, want one copy of hello - a record pushed to the "+
			"middle broker is an ordinary publish there, and forwards on", got)
	}

	// The barrier, and it says the count above is final: `after` is stored at
	// A behind `hello` and the rule drains in offset order, so a second copy
	// of `hello` would have arrived before it.
	fromA.Pub(t, "events/after", "after")
	awaitCount(t, atC, 2, 5*time.Second)
	// Each hop carries a record once, and duplication over a chain is what a
	// loop looks like before it is one.
	holds(t, "the far end", atC, "hello", "after")
	holds(t, "the middle broker", atB, "hello", "after")
}

// RFC 0002 "Bridges", RFC 0003 "Retained messages"
//
// **A retained value crosses as retained, so it becomes state at the far
// end.** Before this the flag was dropped on the crossing: the peer stored
// nothing, and a subscriber arriving there later found nothing at all, with
// nothing said. mosquitto's bridge preserves the flag, so this was the
// difference an operator migrating a mosquitto topology hit silently.
//
// **The late subscriber is the whole point.** A subscriber already connected
// when the value crosses receives it either way, retained or not - so
// asserting on that one proves nothing about retained-ness. This one
// connects afterwards and can only be served from the peer's retained store.
//
// **And a link restart re-ships nothing**, which is the other half and the
// rule that must survive this change: a bridge ships records as they are
// written, never a retained *store* pass on subscribe or reconnect
// (RFC 0003). The link is cut and restored under a subscriber that stays
// connected, and it must see nothing more.
func TestABridgeCarriesRetainSoAValueBecomesStateAtThePeer(t *testing.T) {
	a, b := startRetaining(t, 300), startRetaining(t, 300)
	link := newProxy(t, b.Addr)
	cfg := bridgeOn(t, a, link.Addr, "a-to-b", "weather/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "weather/hallway")

	fromA := connect(t, a, "producer-a", true, false)
	fromA.PubRetainedAt(t, "weather/hallway", "21.5", 1)

	// A subscriber connected across the crossing, kept for the restart
	// assertion below. What it holds now is not the evidence.
	during := connect(t, b, "during-b", true, false)
	during.Sub(t, "weather/#", 1)
	awaitCount(t, during, 1, 5*time.Second)

	// The evidence: a subscriber that was not there when it crossed.
	late := connect(t, b, "late-b", true, false)
	late.Sub(t, "weather/#", 1)
	awaitCount(t, late, 1, 5*time.Second)

	got := late.All()
	if len(got) != 1 {
		t.Fatalf("the late subscriber holds %d deliveries, want exactly one: %v",
			len(got), payloads(got))
	}
	if got[0].Payload != "21.5" || !got[0].Retain {
		t.Errorf("the late subscriber was served %q retain=%v, want 21.5 with the "+
			"flag up: a value retained at the origin is state at the peer, and a "+
			"delivery from a retained store carries the flag", got[0].Payload, got[0].Retain)
	}

	// The restart. Nothing new may reach a subscriber that stayed.
	before := len(during.All())
	link.Cut()
	time.Sleep(300 * time.Millisecond)
	link.Restore()
	subscribedAt(t, b.Srv, cfg.ClientID, "weather/hallway")
	time.Sleep(1500 * time.Millisecond)

	if after := len(during.All()); after != before {
		t.Errorf("a link restart shipped %d more deliveries to a subscriber that "+
			"stayed connected (%d then %d): a bridge carries records as they are "+
			"written and never a retained store pass on reconnect",
			after-before, before, after)
	}
}

// RFC 0002 "Bridges", docs/invariants.md 13
//
// **A retained value on a `direction: both` link crosses once and stops.**
//
// The flag crossing is what makes this worth asserting again: a retained
// value lands in the peer's retained store, and a retained store is exactly
// the thing whose contents an outbound rule could pick up and send back. The
// mark on the record is what stops it - a record that arrived over a bridge
// is never forwarded out over one - and that mark is on the one
// thing a broadcast topic keeps so that this case holds.
//
// **The assertion is a bounded count, not eventual quiet.** A ping-pong that
// damps - each hop costing something until it peters out - satisfies "no
// traffic after a while" while having crossed a dozen times. Each end holds
// exactly one delivery or this fails.
func TestARetainedValueCrossesOnceOnABothWaysLink(t *testing.T) {
	a, b := startRetaining(t, 300), startRetaining(t, 300)
	cfg := bridgeOn(t, a, b.Addr, "a-to-b", "weather/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "weather/hallway")

	atA := connect(t, a, "reader-a", true, false)
	atA.Sub(t, "weather/#", 1)
	atB := connect(t, b, "reader-b", true, false)
	atB.Sub(t, "weather/#", 1)

	fromA := connect(t, a, "producer-a", true, false)
	fromA.PubRetainedAt(t, "weather/hallway", "21.5", 1)
	awaitCount(t, atB, 1, 5*time.Second)

	// Long enough for a damped ping-pong to show several hops. A value that
	// echoes at all echoes within this.
	time.Sleep(2 * time.Second)

	if n := len(atB.All()); n != 1 {
		t.Errorf("the peer holds %d deliveries of one retained value (%v), want 1: "+
			"a value that crossed is in the peer's retained store, and an outbound "+
			"rule there must not pick it up and send it back", n, payloads(atB.All()))
	}
	if n := len(atA.All()); n != 1 {
		t.Errorf("the origin holds %d deliveries of its own retained value (%v), "+
			"want 1: anything above one is the value having come back", n, payloads(atA.All()))
	}
}

// RFC 0002 "Bridges", RFC 0003 "Retained messages"
//
// **The other direction, which is a different mechanism.** Outbound, saguin
// builds the packet it sends the peer and sets the flag on it. Inbound, the
// peer's flag arrives on the fixed header of a publish paho hands over, and
// it has to be lifted out of there into the record - the properties struct
// that carries the rest is not where MQTT puts RETAIN, and a value arriving
// with no properties at all is the ordinary case for a deletion.
//
// So this is asserted separately rather than assumed symmetric: the two
// halves share no code, and the outbound test passing says nothing about
// this one.
func TestARetainedValueCrossesInboundToo(t *testing.T) {
	a, b := startRetaining(t, 300), startRetaining(t, 300)
	cfg := bridgeOn(t, a, b.Addr, "a-to-b", "weather/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "weather/roof")

	// Published at the peer, so it reaches this broker by being pulled in.
	fromB := connect(t, b, "producer-b", true, false)
	fromB.PubRetainedAt(t, "weather/roof", "gale", 1)

	// A subscriber here that was not connected when it crossed can only be
	// served from this broker's own retained store.
	settled := connect(t, a, "settle-a", true, false)
	settled.Sub(t, "weather/#", 1)
	awaitCount(t, settled, 1, 5*time.Second)

	late := connect(t, a, "late-a", true, false)
	late.Sub(t, "weather/#", 1)
	awaitCount(t, late, 1, 5*time.Second)

	got := late.All()
	if len(got) != 1 {
		t.Fatalf("the late subscriber here holds %d deliveries, want exactly one: %v",
			len(got), payloads(got))
	}
	if got[0].Payload != "gale" || !got[0].Retain {
		t.Errorf("a value retained at the peer arrived as %q retain=%v, want gale "+
			"with the flag up: the peer's RETAIN is on the fixed header of the "+
			"publish it sent, and it has to reach the record saguin stores",
			got[0].Payload, got[0].Retain)
	}
}

// RFC 0002 "Bridges", RFC 0003 "Retained messages", docs/invariants.md 13
//
// **A retained value the receiving broker will not keep is refused the way a
// local publisher's is, and does not wedge the link.**
//
// A bridge has no route of its own into the retained store: it publishes
// through `InjectPacket`, so a crossing record reaches `routePublish` and
// `keepRetained` exactly as a client's publish does, and meets the same
// refusals - no retained store attached, the `acl_file` denying retained
// values, the store's own bounds.
//
// **The part worth a test is what the bridge then does.** A refusal that can
// never change its mind must not be retried: a record offered again on every
// reconnect holds the link behind it for ever, which is the same class as a
// peer refusing the retain flag outbound. Here the receiving broker has no
// retained store at all, which is a refusal nothing will alter, and the link
// has to keep moving.
func TestARefusedRetainedValueDoesNotWedgeTheLink(t *testing.T) {
	// The bridge's host has no retained store; the peer it pulls from does.
	b := startRetaining(t, 300)
	a := start(t)
	cfg := bridgeOn(t, a, b.Addr, "a-to-b", "weather/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "weather/roof")

	atA := connect(t, a, "reader-a", true, false)
	atA.Sub(t, "weather/#", 1)

	fromB := connect(t, b, "producer-b", true, false)
	fromB.PubRetainedAt(t, "weather/roof", "refused", 1)
	time.Sleep(1500 * time.Millisecond)

	// Whatever became of that one, the link must still carry the next.
	// Counted from what has already arrived, not from one: the refused
	// value may itself have been delivered live, and waiting for a count it
	// has already met would check this before the next record could cross.
	was := len(atA.All())
	fromB.Pub(t, "weather/hallway", "after")
	awaitCount(t, atA, was+1, 5*time.Second)

	got := payloads(atA.All())
	if !slices.Contains(got, "after") {
		t.Fatalf("the record published after a refused retained value never "+
			"crossed; this broker holds %v. A refusal nothing can change must "+
			"not be retried, or it holds the link behind it for ever", got)
	}
}

// RFC 0003 "Retained messages", RFC 0002 "Bridges"
//
// **A link recovery with the session held re-ships nothing**, which is what
// RFC 0003 promises in as many words: "a bridge ships what is published
// while it runs and never a stored pass, so a link restart re-ships nothing
// and neither end's retained set is handed back to the other."
//
// **The value has to originate at the peer, by a third client.** A value
// the bridge itself pushed is suppressed on the peer's retained pass by No
// Local - the bridge's own subscription there carries it - so a test whose
// only retained value is the bridge's own passes whatever Retain Handling
// asks for. A `both` link exists to carry the far end's state too, and that
// is the uncovered case.
//
// What goes wrong without it: `connected` re-subscribes on every link
// recovery, and a SUBSCRIBE with Retain Handling 0 - paho's zero value -
// earns the peer's whole retained set under the rule any broker honours for
// any subscriber. The copies arrive as fresh publishes carrying no
// `saguin-id`, so a consumer deduplicating on identity (invariant 8) cannot
// recognise them, and into an `append` channel a flapping link appends the
// peer's retained set once per recovery for the life of the deployment.
//
// RFC 0002 documents that accumulation for **Session Present = 0** only,
// with a mitigation an operator can choose. This test is the other case:
// the session is held, and nothing may re-cross.
func TestALinkRecoveryDoesNotReimportThePeersRetainedSet(t *testing.T) {
	a, b := startRetaining(t, 300), startRetaining(t, 300)
	link := newProxy(t, b.Addr)
	cfg := bridgeOn(t, a, link.Addr, "a-to-b", "mirror/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "mirror/peer")

	// Originated at the peer, by somebody other than the bridge.
	atPeer := connect(t, b, "third-party", true, false)
	atPeer.PubRetainedAt(t, "mirror/peer", "from-peer", 1)

	// A subscriber at the origin that stays put across the recovery.
	watcher := connect(t, a, "watcher-a", true, false)
	watcher.Sub(t, "mirror/#", 1)
	awaitCount(t, watcher, 1, 5*time.Second)
	before := len(watcher.All())

	// The link goes and comes back. The session is held either side.
	wasDown := b.Disconnects.Count(cfg.ClientID)
	link.Cut()
	time.Sleep(400 * time.Millisecond)
	link.Restore()
	subscribedAt(t, b.Srv, cfg.ClientID, "mirror/peer")

	// **Waited for rather than slept past.** The first version of this test
	// settled for a fixed two seconds and passed while the defect was
	// present: the re-import landed after the window closed. Polling for the
	// unwanted delivery fails as soon as it appears and only spends the full
	// deadline when there is nothing to find.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(watcher.All()) > before {
			t.Errorf("a link recovery crossed %v a second time: the copies carry "+
				"no saguin-id, so nothing downstream can recognise them as "+
				"duplicates. NOTE the cause is not the peer's retained set - this "+
				"test arms the window with a retained value, but what crosses "+
				"twice is an unacknowledged record the upstream resends at session "+
				"resume, before any SUBSCRIBE. "+
				"TestAnUpstreamRedeliveryDoesNotCrossAsANewRecord is the "+
				"deterministic form", payloads(watcher.All()))
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// **The recovery has to have happened**, or this asserts that nothing
	// re-crossed a link that never went - a pass its own instrument cannot
	// fail.
	if now := b.Disconnects.Count(cfg.ClientID); now == wasDown {
		t.Fatalf("the peer never saw the bridge go (disconnects still %d), so the "+
			"link recovery this test is about did not happen", now)
	}
}

// RFC 0002 "Bridges"
//
// **MQTT requires the upstream to resend, and this is what stops the resend
// becoming a second record.** A QoS 1 publish whose PUBACK the upstream
// never saw is re-sent when the session resumes - before any SUBSCRIBE, so
// no retained pass and no Retain Handling is involved. Handed to the bridge
// twice, it is stored twice, given a second local offset, and carries no
// `saguin-id` either time: downstream there are two records and nothing can
// tell they are one (invariant 8).
//
// **Deterministic where the test above is an accident.** That one arms the
// window with a retained value and catches this about one run in six; this
// one cuts the link inside the acknowledgement's flight deliberately, and
// caught it 30 times in 30 before the guard existed. Three things make it
// fire every time:
//
//   - the watcher subscribes *before* the record exists, so it is served the
//     live fan-out. Served a retained backfill instead, the crossing can
//     land before the subscription does, the watcher sees nothing, and a
//     non-fatal wait then burns its whole timeout - so the cut falls seconds
//     after the acknowledgement landed and the run arms nothing while
//     passing. That shape cost this session 74 runs of false confidence.
//   - the publish is plain. Retain is not part of the defect; it only
//     widened the window.
//   - the wait is a 200us poll. The window is the bridge's acknowledgement
//     in transit, a few milliseconds, and a 10ms quantum steps over it.
func TestAnUpstreamRedeliveryDoesNotCrossAsANewRecord(t *testing.T) {
	a, b := startRetaining(t, 300), startRetaining(t, 300)
	link := newProxy(t, b.Addr)
	cfg := bridgeOn(t, a, link.Addr, "a-to-b", "mirror/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "mirror/peer")

	watcher := connect(t, a, "watcher-a", true, false)
	watcher.Sub(t, "mirror/#", 1)

	atPeer := connect(t, b, "third-party", true, false)
	atPeer.PubPlainAt(t, "mirror/peer", "from-peer", 1)

	armed := time.Now().Add(10 * time.Second)
	for len(watcher.All()) == 0 && time.Now().Before(armed) {
		time.Sleep(200 * time.Microsecond)
	}
	if len(watcher.All()) == 0 {
		t.Fatal("the record never crossed at all, so this run armed nothing and " +
			"proves nothing about a redelivery")
	}
	before := len(watcher.All())

	wasDown := b.Disconnects.Count(cfg.ClientID)
	link.Cut()
	time.Sleep(400 * time.Millisecond)
	link.Restore()
	subscribedAt(t, b.Srv, cfg.ClientID, "mirror/peer")

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(watcher.All()) > before {
			t.Errorf("an unacknowledged record the upstream resent at session resume "+
				"crossed again as a new record: %v. it carries no saguin-id, so "+
				"nothing downstream can recognise it as the one it already has",
				payloads(watcher.All()))
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// **The recovery has to have happened**, or this asserts that nothing
	// re-crossed a link that never went.
	if now := b.Disconnects.Count(cfg.ClientID); now == wasDown {
		t.Fatalf("the peer never saw the bridge go (disconnects still %d), so the "+
			"link recovery this test is about did not happen", now)
	}
}

// RFC 0002 "Bridges"
//
// **A bridge's first subscription still earns the peer's retained set**,
// which is the case RFC 0002 documents rather than forbids: "every
// reconnect that finds Session Present = 0 copies all of it in again,
// deterministically ... On an `append` channel those accumulate for the
// life of the deployment", and it names pointing the rule at a `latest`
// channel as the mitigation an operator chooses in advance.
//
// This is the other half of the recovery test above, and the reason the
// subscription asks for Retain Handling **1** rather than 2. Both stop the
// re-import when the session is held; 2 would also stop this, which is a
// documented behaviour and an operator's decision to work around - so
// changing it would be a decision, not a fix, and this test is what makes
// anybody making it do so deliberately.
func TestABridgesFirstSubscriptionStillTakesThePeersRetainedSet(t *testing.T) {
	a, b := startRetaining(t, 300), startRetaining(t, 300)

	// Retained at the peer before any bridge exists, so the only way it can
	// reach the origin is the pass a first subscribe earns.
	atPeer := connect(t, b, "third-party", true, false)
	atPeer.PubRetainedAt(t, "mirror/early", "before-the-link", 1)
	time.Sleep(300 * time.Millisecond)

	watcher := connect(t, a, "watcher-a", true, false)
	watcher.Sub(t, "mirror/#", 1)

	cfg := bridgeOn(t, a, b.Addr, "a-to-b", "mirror/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "mirror/early")

	if _, ok := watcher.Await(t, 8*time.Second); !ok {
		t.Fatal("a bridge's first subscription was answered with nothing: RFC 0002 " +
			"documents that a subscription earns the peer's retained set and names " +
			"the mitigation for it, so Retain Handling 2 here would change a " +
			"documented behaviour rather than fix the recovery defect")
	}
	if got := payloads(watcher.All()); got[0] != "before-the-link" {
		t.Errorf("the origin holds %v, want the peer's retained value first", got)
	}
}

// bridgeRules runs a bridge hosted by one broker against another with the
// rules written out, for a test whose rule is not bridgeOn's identity `both`.
func bridgeRules(t *testing.T, host *harness, peerAddr, name, rules string) config.Bridge {
	t.Helper()
	yaml := fmt.Sprintf(`broker:
  id: t
`+memStorage+`channels:
  events:
    type: append
  state:
    type: latest
bridges:
  %s:
    peer: tcp://%s
    client_id: %s
    topics:
%s`, name, peerAddr, name, rules)
	path := filepath.Join(t.TempDir(), name+".yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write the bridge configuration: %v", err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own bridge configuration does not load: %v", err)
	}
	cfg := f.BridgeSet()[0]
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(newTestWriter(t), &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	link := host.B.NewBridgeClient(cfg.Name)
	br := bridge.New(cfg, link, limits, log, link)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = br.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the bridge did not stop")
		}
	})
	return cfg
}

// holdsLast waits until a client's last value on a topic is want, and says
// whether it got there.
func holdsLast(c *client, topic, want string, within time.Duration) bool {
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		last := ""
		for _, r := range c.All() {
			if r.Topic == topic {
				last = r.Payload
			}
		}
		if last == want {
			return true
		}
	}
	return false
}

// RFC 0003: **a bridge forwards the live fan-out and never a stored pass.** An
// outbound rule on a latest channel carries each change published while it
// runs, and not the state the channel held when it started.
//
// It did the reverse, to every kind of peer: it sent the state it found at
// start, re-sent it at every start, and then waited for a wake that nothing
// on a latest channel ever sent - so the peer held the values of the moment
// the bridge came up, for good, with nothing logged and nothing counted.
// Reproduced to a Sagüin and to a foreign broker.
func TestAnOutRuleCarriesALatestChannelsChangesToASaguinPeer(t *testing.T) {
	a, b := start(t), start(t)
	pa := connect(t, a, "a-producer", true, false)
	pa.Pub(t, "state/x", "held-at-start") // before the rule runs: not carried

	sub := connect(t, b, "b-reader", true, false)
	sub.Sub(t, "state/#", 1)
	cfg := bridgeRules(t, a, b.Addr, "to-b",
		"      - filter: state/#\n        topic: state/from-a/$#\n        direction: out\n")
	awaitRegistered(t, b.Srv, cfg.ClientID, 10*time.Second)
	time.Sleep(200 * time.Millisecond) // the rule is watching once the link is up

	pa.Pub(t, "state/x", "v1")
	pa.Pub(t, "state/x", "v2")
	if !holdsLast(sub, "state/from-a/x", "v2", 5*time.Second) {
		t.Fatalf("the peer never settled on v2, the latest change: its reader holds %v",
			payloads(sub.All()))
	}
	for _, r := range sub.All() {
		if r.Payload == "held-at-start" {
			t.Errorf("the state held when the rule started was forwarded - a stored pass, which "+
				"a bridge never sends: the peer's reader holds %v", payloads(sub.All()))
		}
	}
}

// The same at a broker that has never heard of a channel.
func TestAnOutRuleCarriesALatestChannelsChangesToAForeignBroker(t *testing.T) {
	upstream, upstreamAddr := startUpstream(t)
	a := start(t)
	pa := connect(t, a, "a-producer", true, false)
	pa.Pub(t, "state/x", "held-at-start")

	var mu sync.Mutex
	var got []string
	if err := upstream.Subscribe("#", 1, func(_ *mqtt.Client, _ packets.Subscription, pk packets.Packet) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, pk.TopicName+" "+string(pk.Payload))
	}); err != nil {
		t.Fatalf("subscribe at the foreign broker: %v", err)
	}
	seen := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), got...) }

	cfg := bridgeRules(t, a, upstreamAddr, "to-foreign",
		"      - filter: state/#\n        topic: mirror/$#\n        direction: out\n")
	awaitRegistered(t, upstream, cfg.ClientID, 10*time.Second)
	time.Sleep(200 * time.Millisecond)

	pa.Pub(t, "state/x", "v1")
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(seen(), "mirror/x v1") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !slices.Contains(seen(), "mirror/x v1") {
		t.Fatalf("the foreign broker never received the change v1: it received %v", seen())
	}
	if slices.Contains(seen(), "mirror/x held-at-start") {
		t.Errorf("the state held when the rule started was forwarded, a stored pass: %v", seen())
	}
}

// **A link that drops is not a rule that stopped.** The rule runs beside the
// link, so it goes on taking the channel's changes while the link is down;
// each topic's newest crosses when it comes back, and nothing the rule would
// have sent is lost to an outage. Only what was published before the rule
// started is not carried.
func TestALatestChangeMadeWhileTheLinkIsDownCrossesWhenItComesBack(t *testing.T) {
	a, b := start(t), start(t)
	link := newProxy(t, b.Addr)
	pa := connect(t, a, "a-producer", true, false)
	sub := connect(t, b, "b-reader", true, false)
	sub.Sub(t, "state/#", 1)
	cfg := bridgeRules(t, a, link.Addr, "to-b",
		"      - filter: state/#\n        topic: state/from-a/$#\n        direction: out\n")
	awaitRegistered(t, b.Srv, cfg.ClientID, 10*time.Second)
	time.Sleep(200 * time.Millisecond)

	pa.Pub(t, "state/x", "before")
	if !holdsLast(sub, "state/from-a/x", "before", 5*time.Second) {
		t.Fatalf("the link never carried a change before the outage: %v", payloads(sub.All()))
	}

	link.Cut()
	for _, v := range []string{"during-1", "during-2", "during-3"} {
		pa.Pub(t, "state/x", v)
	}
	pa.Pub(t, "state/y", "y-during")
	time.Sleep(300 * time.Millisecond)
	link.Restore()

	if !holdsLast(sub, "state/from-a/x", "during-3", 15*time.Second) {
		t.Errorf("state/x never settled on during-3, its newest change while the link was down: "+
			"the peer's reader holds %v", payloads(sub.All()))
	}
	if !holdsLast(sub, "state/from-a/y", "y-during", 5*time.Second) {
		t.Errorf("state/y's change while the link was down never crossed: %v", payloads(sub.All()))
	}
}

// A `direction: both` link on a latest channel: a value crosses once and is
// not handed back. The mark a value takes arriving over a bridge is what stops
// it, and a live feed can echo as surely as a stored pass could.
func TestABothWaysLinkOnALatestChannelEchoesNothing(t *testing.T) {
	a, b := start(t), start(t)
	ra := connect(t, a, "a-reader", true, false)
	ra.Sub(t, "state/#", 1)
	rb := connect(t, b, "b-reader", true, false)
	rb.Sub(t, "state/#", 1)
	cfg := bridgeOn(t, a, b.Addr, "ab", "state/#")
	subscribedAt(t, b.Srv, cfg.ClientID, "state/x")

	// **Both directions carry before anything is asserted**: a probe each
	// way, on topics of their own, so the road an echo would take is shown
	// open rather than assumed.
	pa := connect(t, a, "a-producer", true, false)
	pb := connect(t, b, "b-producer", true, false)
	pa.Pub(t, "state/probe-a", "p")
	if !holdsLast(rb, "state/probe-a", "p", 5*time.Second) {
		t.Fatalf("nothing crosses from A to B: %v", payloads(rb.All()))
	}
	pb.Pub(t, "state/probe-b", "p")
	if !holdsLast(ra, "state/probe-b", "p", 5*time.Second) {
		t.Fatalf("nothing crosses from B to A: %v", payloads(ra.All()))
	}

	pa.Pub(t, "state/x", "from-a")
	if !holdsLast(rb, "state/x", "from-a", 5*time.Second) {
		t.Fatalf("the value never crossed to B: %v", payloads(rb.All()))
	}
	// **A marker published at B, after from-a reached it**, rather than a
	// wait long enough for an echo: B forwards state/x in the order it holds
	// it (RFC 0003 "Ordering"), so an echo of from-a crosses back to A ahead
	// of the marker.
	pb.Pub(t, "state/x", "marker")
	if !holdsLast(ra, "state/x", "marker", 5*time.Second) || !holdsLast(rb, "state/x", "marker", 5*time.Second) {
		t.Fatalf("the marker never settled on both sides: A %v, B %v", payloads(ra.All()), payloads(rb.All()))
	}
	onX := func(c *client) []string {
		var got []string
		for _, r := range c.All() {
			if r.Topic == "state/x" {
				got = append(got, r.Payload)
			}
		}
		return got
	}
	for who, c := range map[string]*client{"A's reader": ra, "B's reader": rb} {
		if got := onX(c); !slices.Equal(got, []string{"from-a", "marker"}) {
			t.Errorf("%s holds %v on state/x, want exactly [from-a marker]", who, got)
		}
	}
}

// A record a Sagüin peer sends over a bridge onto a local broadcast topic
// carries no `saguin-` property to anyone here - not to a subscriber
// connected as it arrives, and not to one that subscribes later and is served
// the retained copy.
//
// **The strip is decided by who wrote the properties, not by how the packet
// arrived.** fanOut exempts saguin's own in-process publishes - a queue's
// offer, a Will - because the stamps on those are ones saguin has just
// written. A bridge publishes through an in-process client too, and what it
// carries was written by another broker: that peer's `saguin-offset`, its
// receipt time and its Message ID, none of which mean anything here. A
// broadcast has no record identity and no position, and giving it one on the
// way out would make broadcast look like a channel; so a peer's stamps are
// stripped as any publisher's are. Before
// this a local subscriber was handed `saguin-offset:1` from the far end.
func TestABroadcastArrivingOverABridgeCarriesNoPeersStamps(t *testing.T) {
	// A keeps a retained store, so the retained copy exists to be served.
	a, b := startRetaining(t, 3600), start(t)
	cfg := bridgeRules(t, a, b.Addr, "from-b",
		"      - filter: events/#\n        topic: bcast/$#\n        direction: in\n")
	subscribedAt(t, b.Srv, cfg.ClientID, "events/x")

	live := connect(t, a, "a-live", true, false)
	live.Sub(t, "bcast/#", 1)
	pb := connect(t, b, "b-producer", true, false)
	pb.PubRetained(t, "events/x", "hello") // a channel record at B, retained as it crosses
	if !holdsLast(live, "bcast/x", "hello", 5*time.Second) {
		t.Fatalf("nothing crossed to A's live subscriber: %v", payloads(live.All()))
	}
	late := connect(t, a, "a-late", true, false)
	late.Sub(t, "bcast/#", 1)
	if !holdsLast(late, "bcast/x", "hello", 5*time.Second) {
		t.Fatalf("the late subscriber was not served the retained copy, so the retained half "+
			"of this proves nothing: %v", payloads(late.All()))
	}

	for who, c := range map[string]*client{"the live subscriber": live, "the late subscriber": late} {
		for _, r := range c.All() {
			for k, v := range r.User {
				if strings.HasPrefix(k, "saguin-") {
					t.Errorf("%s was handed %s=%s on %s: a stamp another broker wrote, on a "+
						"broadcast that has no position or identity here", who, k, v, r.Topic)
				}
			}
		}
	}
}

// RFC 0002 "Bridges", RFC 0003 "Ordering"
//
// **A bridge link keeps every record, once and in order, across a packet
// identifier wrap.** An identifier is sixteen bits, so a link carrying more
// than 65,535 QoS 1 records over one connection reuses every one of them,
// and each side's bookkeeping - the in-flight table, the sqlite session's
// (client, packet_id) rows, paho's own map on an out rule - has to let go of
// an identifier before it is handed out again. Nothing drove a link that far
// before; the allocators' wrap was proved only inside package mqtt.
//
// **No seam, on purpose.** The counters that would start a link near 65,535
// are unexported in internal/mqtt and paho's cannot be seeded, so the test
// sends enough records to wrap for real and proves the wrap by counting: the
// peer records no disconnect for the bridge's client id, so every record
// crossed one connection, and more records than there are identifiers means
// every identifier was reused at least once.
//
// Both directions, because they wrap different allocators: on an `in` rule
// the peer's session hands the identifiers out, on an `out` rule paho does,
// inside the bridge. Both providers, because on sqlite the peer's in-flight
// rows are stored, and a row not cleared before its identifier comes round
// again is a conflict the memory table cannot have.
func TestABridgeLinkKeepsEveryRecordAcrossAnIdentifierWrap(t *testing.T) {
	if testing.Short() {
		t.Skip("drives 66,535 records over each link; -short leaves it out")
	}
	const records = 65535 + 1000
	providers := []struct {
		name  string
		start func(t *testing.T, name string) *harness
	}{
		{"memory", func(t *testing.T, _ string) *harness { return start(t) }},
		{"sqlite", func(t *testing.T, name string) *harness {
			return startDurableSQLite(t, filepath.Join(t.TempDir(), name+".db"))
		}},
	}
	for _, p := range providers {
		for _, dir := range []string{"in", "out"} {
			t.Run(p.name+"/"+dir, func(t *testing.T) {
				peer, host := p.start(t, "peer"), p.start(t, "host")
				// Where the records are published and where they must land.
				from, to := peer, host
				if dir == "out" {
					from, to = host, peer
				}
				landed := broker.LogOf(to.B, "events")
				name := "wrap-" + dir
				bridgeRules(t, host, peer.Addr, name,
					"      - filter: events/wrap/#\n        topic: events/wrap/$#\n        direction: "+dir+"\n")
				if dir == "in" {
					subscribedAt(t, peer.Srv, name, "events/wrap/p")
				} else {
					awaitRegistered(t, peer.Srv, name, 10*time.Second)
					time.Sleep(200 * time.Millisecond) // the rule is watching once the link is up
				}
				base := landed.Next()

				pub := connect(t, from, "wrap-publisher", true, false)
				began := time.Now()
				for i := 1; i <= records; i++ {
					pub.Pub(t, "events/wrap/p", fmt.Sprint(i))
				}
				for deadline := time.Now().Add(2 * time.Minute); landed.Next()-base < records &&
					time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
				}
				t.Logf("%d records published and carried in %s", records, time.Since(began).Round(time.Millisecond))

				// Every record, once, in order: read back from the store the
				// link wrote into, not from a subscriber whose own session
				// would wrap too.
				next, dups, gaps, firstBad := 1, 0, 0, ""
				for off := base; off < landed.Next(); {
					batch, err := landed.ReadFromN(off, 1000)
					if err != nil || len(batch) == 0 {
						t.Fatalf("reading the landed records from %d: %v", off, err)
					}
					for _, r := range batch {
						var seq int
						if _, err := fmt.Sscan(string(r.Payload), &seq); err != nil {
							t.Fatalf("offset %d holds %q, which no publisher here sent", r.Offset, r.Payload)
						}
						switch {
						case seq == next:
							next++
						case seq < next:
							dups++
							if firstBad == "" {
								firstBad = fmt.Sprintf("seq %d again after %d, at offset %d", seq, next-1, r.Offset)
							}
						default:
							gaps++
							if firstBad == "" {
								firstBad = fmt.Sprintf("seq %d after %d, at offset %d", seq, next-1, r.Offset)
							}
							next = seq + 1
						}
					}
					off = batch[len(batch)-1].Offset + 1
				}
				if got := next - 1; got != records || dups != 0 || gaps != 0 {
					t.Errorf("the %s link landed up to seq %d of %d with %d duplicates and %d gaps; "+
						"first: %s", dir, got, records, dups, gaps, firstBad)
				}
				// The precondition that makes this a wrap: one connection
				// carried all of it.
				if n := peer.Disconnects.Count(name); n != 0 {
					t.Errorf("the peer recorded %d disconnects for %q, so the records did not all "+
						"cross one connection and the identifiers need not have wrapped", n, name)
				}
			})
		}
	}
}
