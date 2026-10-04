package broker

// Unit tests for the position rule. It is deliberately not an end-to-end
// test: a reconnecting MQTT session replays its own unacknowledged packets,
// so a record reaches the consumer over the wire whether or not saguin's
// stored position is right, and an end-to-end test cannot tell the two
// apart.

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

// storedPosition reads back what the channel kept for a reader, under the
// namespaced name the broker writes.
func storedPosition(t *testing.T, b *Broker, clientID string) uint64 {
	t.Helper()
	p, ok, err := b.logs["events"].Position(store.MQTTReader(clientID))
	if err != nil || !ok {
		t.Fatalf("reading the stored position of %q: ok=%v err=%v", clientID, ok, err)
	}
	return p.Offset
}

func testBroker(t *testing.T) *Broker {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// RFC 0003 "`append` - Durable consumers"
//
// The stored position is the lowest offset the consumer has not
// acknowledged, never the highest one sent.
//
// Storing the highest passes every in-order test: acknowledgements usually
// arrive in order, and then the two are the same number. They differ only
// when a consumer acknowledges out of order, and the record in the gap is
// then skipped on the next reconnect - silently, because the consumer
// resumes past it and reports success.
func TestPositionIsTheLowestUnacknowledgedOffset(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 4)
	cur := b.cursor("reader", "events", false)

	// Three records written and none acknowledged.
	for _, off := range []uint64{1, 2, 3} {
		cur.outstanding[off] = true
		cur.next = off + 1
	}
	cur.expiresIn = time.Hour
	b.flushPositions()
	if got := storedPosition(t, b, "reader"); got != 1 {
		t.Fatalf("position %d with offsets 1, 2 and 3 outstanding; want 1", got)
	}

	// Acknowledged out of order: the third, then the second. The first is
	// still outstanding, so the position must not move at all.
	for _, acked := range []uint64{3, 2} {
		delete(cur.outstanding, acked)
		cur.expiresIn = time.Hour
		b.flushPositions()
		if got := storedPosition(t, b, "reader"); got != 1 {
			t.Fatalf("position advanced to %d after acknowledging offset %d, while "+
				"offset 1 was still outstanding; offset 1 is now unreachable", got, acked)
		}
	}

	// The gap closes, and only now may the position pass all three.
	delete(cur.outstanding, 1)
	cur.expiresIn = time.Hour
	b.flushPositions()
	if got := storedPosition(t, b, "reader"); got != 4 {
		t.Fatalf("position %d after every record was acknowledged; want 4 - "+
			"acknowledged records will be redelivered for ever", got)
	}
}

// RFC 0003 "`append` - Durable consumers", and docs/invariants.md 1
//
// A consumer going away writes where it reached before its cursor goes,
// and writes nobody else's.
//
// **The first half had no test at all.** With the disconnect's flush
// removed outright, every broker and session test still passed: the tick
// writes positions too, and end-to-end tests reconnect within it. What the
// flush saves is the advance since the last tick, and losing it is a replay
// of records the consumer already acknowledged - no test counted those.
// No broker here is Run, so no tick can write the position on its behalf.
//
// **The second half is the scale fix.** Flushing every cursor on every
// disconnect made a disconnect storm quadratic: 47.6s for 20,000 consumers
// to settle, with the lock held against everyone else for up to 26s
// (BenchmarkDisconnectStorm). The consumers still connected lose nothing by
// waiting for the tick.
func TestADisconnectStoresItsOwnPositionAndNobodyElses(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 4)
	for _, id := range []string{"leaving", "staying"} {
		cur := b.cursor(id, "events", false)
		cur.next = 4 // read 1-3, every one acknowledged
		cur.expiresIn = time.Hour
		if cur.saved == cur.lowestUnacknowledged() {
			t.Fatalf("%s's cursor reads as already stored, so there is nothing to flush "+
				"and nothing below would mean anything", id)
		}
	}

	cl := &mqtt.Client{ID: "leaving"}
	cl.Properties.Props.SessionExpiryInterval = 3600
	b.owner["leaving"] = cl
	b.OnDisconnect(cl, io.EOF, false)

	p, ok, err := b.logs["events"].Position(store.MQTTReader("leaving"))
	if err != nil || !ok || p.Offset != 4 {
		t.Fatalf("the departed consumer's stored position is %d (stored=%v, err=%v), want 4: "+
			"it resumes further back than it reached and is sent acknowledged records again",
			p.Offset, ok, err)
	}
	if p, ok, _ := b.logs["events"].Position(store.MQTTReader("staying")); ok {
		t.Errorf("another consumer's disconnect stored the position of one still connected "+
			"(offset %d): every disconnect is walking every cursor again", p.Offset)
	}
}

// RFC 0003 "`append` - Durable consumers"
//
// A cursor is seeded from the stored position, so a consumer that reconnects
// resumes where it left off rather than at the beginning of the channel.
func TestCursorResumesFromTheStoredPosition(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 7)
	if err := b.logs["events"].SavePosition(store.Position{
		Reader: store.MQTTReader("reader"), Offset: 7, LastSeen: time.Now(), ExpiresIn: time.Hour,
	}); err != nil {
		t.Fatalf("save position: %v", err)
	}

	if got := b.cursor("reader", "events", false).next; got != 7 {
		t.Fatalf("cursor resumed at %d, want 7", got)
	}

	// A consumer with no stored position starts at the first offset, not at
	// zero: offsets begin at 1.
	if got := b.cursor("fresh", "events", false).next; got != 1 {
		t.Fatalf("a new consumer starts at %d, want 1", got)
	}
}

// RFC 0003 "`append` - Where a subscription starts": Retain Handling 2 asks
// for nothing older than now, and on an append channel that means the
// consumer is moved to the head - the same two writes a seek to `-1` makes.
//
// **The commit is the half worth a test, and it is why this one is here
// rather than on the wire.** A cursor lives as long as the session, so an
// end-to-end test that reconnects reuses the cursor already at the head and
// passes whether or not anything reached storage - measured, with the
// commit mutated out - while ending the session to force a rebuild drops
// the stored position along with it. Neither shape can tell a position that
// was written down from one a session happens to be holding, which is the
// distinction this whole rule turns on: a skip that is not committed comes
// back as a replay on the next connect, of exactly the records the consumer
// asked to skip.
func TestRetainHandling2CommitsTheConsumerAtTheHead(t *testing.T) {
	b := testBroker(t)
	lg := b.logs["events"]
	for range 5 {
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x",
			Timestamp: time.Now()}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// A consumer that read the first record and stopped, so there is a
	// backlog above it for this to skip.
	if err := lg.SavePosition(store.Position{
		Reader: store.MQTTReader("reader"), Offset: 2, LastSeen: time.Now(), ExpiresIn: time.Hour,
	}); err != nil {
		t.Fatalf("save position: %v", err)
	}

	head := lg.Next()
	if head <= 2 {
		t.Fatalf("the head is %d against a stored position of 2, so there is no backlog "+
			"here and nothing below would mean anything", head)
	}

	cl := &mqtt.Client{ID: "reader"}
	cl.Properties.Props.SessionExpiryInterval = 3600
	b.owner[cl.ID] = cl // a SUBSCRIBE comes from the connection holding its id
	b.skipToHead(cl, b.reg.Get("events"))

	if got := storedPosition(t, b, "reader"); got != head {
		t.Errorf("the stored position is %d, want the head %d: the backlog would come "+
			"back on the next connect, which is what this consumer asked not to happen", got, head)
	}
	if got := b.cursorLocked("reader", "events").next; got != head {
		t.Errorf("the live cursor is at %d, want the head %d: this connection would be "+
			"served the backlog it asked to skip", got, head)
	}
}

// The same rule for a session that does not outlive its connection: the
// cursor moves so the subscription is served live only, and nothing is
// written down - a row for a session nobody can come back to is a row the
// sweep removes, which is why handleSeek refuses a seek from one.
func TestRetainHandling2StoresNothingForASessionThatEndsWithItsConnection(t *testing.T) {
	b := testBroker(t)
	lg := b.logs["events"]
	for range 3 {
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x",
			Timestamp: time.Now()}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	cl := &mqtt.Client{ID: "passing"} // no Session Expiry Interval at all
	b.owner[cl.ID] = cl
	b.skipToHead(cl, b.reg.Get("events"))

	if _, ok, err := lg.Position(store.MQTTReader("passing")); err != nil {
		t.Fatalf("read the position: %v", err)
	} else if ok {
		t.Error("a position was stored for a session that ends with its connection")
	}
	if got := b.cursorLocked("passing", "events").next; got != lg.Next() {
		t.Errorf("the cursor is at %d, want the head %d", got, lg.Next())
	}
}

// Invariant 17: only the connection that owns a client id changes what the
// broker keeps for it - not one that has been taken over, and not one whose
// id nobody holds any more.
//
// Each of these runs after the connection's own lock has been let go - a
// SUBSCRIBE's retained or current-state pass, a no-replay subscription, a
// pump's refusal note - so a takeover or a teardown can land first. Each
// made delivery state for the id anyway: a consumer record nothing removed
// until the id next disconnected as owner, and, for the no-replay
// subscription, a cursor at the head handed to the successor's session.
// The two passes were reproduced on the wire by holding each between its
// read and its lock while a clean start took the id and left.
//
// The owner arm is the control: the same call on the connection holding the
// id does make the state, so an empty result below means refused rather
// than never reached.
func TestAConnectionThatNoLongerHoldsItsIDMakesNoDeliveryStateForIt(t *testing.T) {
	ops := []struct {
		name string
		do   func(b *Broker, cl *mqtt.Client)
	}{
		{"retained pass", func(b *Broker, cl *mqtt.Client) {
			b.deliverRetained(cl, packets.Subscription{Filter: "weather/#", Qos: 1}, false, partition{})
		}},
		{"current-state pass", func(b *Broker, cl *mqtt.Client) {
			b.subs[cl.ID] = []subscription{{filter: "state/#", channel: b.reg.Get("state"), qos: 1}}
			b.deliverLatest(cl, b.reg.Get("state"), 0, "")
		}},
		{"no-replay subscription", func(b *Broker, cl *mqtt.Client) {
			b.skipToHead(cl, b.reg.Get("events"))
		}},
		{"refusal note", func(b *Broker, cl *mqtt.Client) {
			b.noteRefusedDelivery(cl, "events", "events/a")
		}},
	}
	for _, op := range ops {
		for _, holder := range []string{"the connection itself", "a successor", "nobody"} {
			t.Run(op.name+", held by "+holder, func(t *testing.T) {
				reg, err := channel.NewRegistry([]*channel.Channel{
					{Name: "events", Type: channel.Append}, {Name: "state", Type: channel.Latest}})
				if err != nil {
					t.Fatalf("registry: %v", err)
				}
				b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
				rt := store.NewLatest()
				b.SetRetained("local", rt, 0)
				now := time.Now()
				for _, set := range []struct {
					lt    LatestStore
					topic string
				}{{rt, "weather/a"}, {b.latest["state"], "state/a"}} {
					if _, err := set.lt.Set(store.Record{Topic: set.topic, Payload: []byte("v"),
						Timestamp: now}); err != nil {
						t.Fatalf("set %s: %v", set.topic, err)
					}
				}
				appendUpTo(t, b.logs["events"], 3)

				srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
				cl := srv.NewClient(nil, mqtt.LocalListener, "dev", false)
				cl.Properties.ProtocolVersion = 5
				cl.Properties.Props.SessionExpiryInterval = 3600
				switch holder {
				case "the connection itself":
					b.owner[cl.ID] = cl
				case "a successor":
					b.owner[cl.ID] = &mqtt.Client{ID: "dev"}
				}
				op.do(b, cl)

				made := b.lookupConsumer("dev") != nil
				_, noted := b.refusedRead["dev\x00events"]
				if holder == "the connection itself" {
					if !made && !noted {
						t.Fatal("the owner's own call made nothing either, so this arm " +
							"never reached the state the others must not make")
					}
					return
				}
				if made {
					t.Errorf("a consumer record for dev was made by a connection held by %s", holder)
				}
				if noted {
					t.Errorf("a refusal note for dev was kept for a connection held by %s", holder)
				}
			})
		}
	}
}

// The no-replay subscription's own window, which the direct calls above
// cannot reach: its ownership was asked in one hold of b.mu and its cursor
// made in a later one, with the position written between. A clean start
// taking the id in that gap dropped the old session's delivery state, and
// the cursor was then made for the id anyway - at the head, in what was by
// then the successor's session (invariant 17). The store write is where the
// takeover is landed here, which is the gap as it was.
func TestANoReplaySubscriptionTakenOverAsItsPositionIsWrittenLeavesTheSuccessorNothing(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 3)
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	cl := srv.NewClient(nil, mqtt.LocalListener, "dev", false)
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.SessionExpiryInterval = 3600
	b.owner[cl.ID] = cl

	var took bool
	b.logs["events"] = &onSave{LogStore: b.logs["events"], fn: func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.owner[cl.ID] = srv.NewClient(nil, mqtt.LocalListener, "dev", false)
		b.forgetSessionDeliveriesLocked(cl.ID)
		took = true
	}}
	b.skipToHead(cl, b.reg.Get("events"))

	if !took {
		t.Fatal("the position was never written, so the takeover never landed in the gap")
	}
	if b.lookupConsumer("dev") != nil {
		t.Error("the successor's session holds delivery state the connection it took the id " +
			"from made after the takeover")
	}
}

// onSave runs fn at each position write, before the write itself.
type onSave struct {
	LogStore
	fn func()
}

func (s *onSave) SavePosition(p store.Position) error {
	s.fn()
	return s.LogStore.SavePosition(p)
}

// RFC 0003 "Ordering" - append: each subscriber receives records in offset
// order.
//
// One drain runs at a time per consumer and channel, and the flag saying so
// lives on the cursor. A disconnect landing inside a drain deletes the
// client's cursor, so a drain that re-reads the map mid-drain builds itself
// a fresh one with that flag clear - and the next pump then starts a second
// drain beside the one still running, which is the out-of-order delivery the
// flag exists to prevent, reached from underneath it.
//
// Not an end-to-end test, deliberately. The window needs a disconnect to
// land between two batches of a drain a *publisher's* goroutine started: a
// consumer's own drain runs on the goroutine that would process its
// disconnect, so that one cannot race itself. Driven from the wire with
// instrumentation it reproduced roughly once in fifteen attempts, which is a
// coin flip rather than an assertion.
func TestADrainStopsWhenItsConsumerHasGone(t *testing.T) {
	b := testBroker(t)
	c := b.reg.Get("events")
	cl := &mqtt.Client{ID: "device"}

	// Subscribed, so the drain gets past the check for anything to feed.
	// The channel is left empty: what is under test is the bookkeeping, and
	// a record here would have the drain write to a client with no socket.
	b.subs[cl.ID] = []subscription{{filter: "events/#", channel: c, qos: 0}}

	// A drain is under way: it holds the cursor it started on.
	held := b.cursor(cl.ID, c.Name, false)
	held.pumping = true

	// This connection owns the state under its id, which is what a real one
	// has claimed by the time it can disconnect - forgetConsumer removes
	// nothing for a connection that has been superseded.
	b.owner[cl.ID] = cl

	// The consumer disconnects between two of its batches.
	b.endSession(cl, "it disconnected and its session ended", endedSession{})

	b.pumpBatch(cl, c, held, nil)

	if rebuilt := len(b.cursorsOfLocked(cl.ID)) > 0; rebuilt {
		t.Fatal("a drain whose consumer had gone built itself a fresh cursor and " +
			"carried on. The flag saying one drain runs at a time lives on that " +
			"cursor and is clear on the new one, so the next pump starts a second " +
			"drain beside this one and the two interleave batches")
	}
}

// RFC 0003 "`append` - Durable consumers", invariant 1
//
// A drain that fails to write gives its records back, so they are sent again
// rather than skipped. It must give them back to the cursor it was draining
// and not to whichever one the map holds by then: a disconnect between
// reading a batch and failing to write it replaces that cursor, and the
// records the departed drain gives back are not the ones the live consumer
// is waiting on.
//
// Here the live consumer is holding offset 3 unacknowledged. A departed
// drain giving back its own offset 3 clears that, the stored position moves
// to 4, and record 3 is then delivered to nobody - the consumer resumes past
// it and reports success.
func TestAStaleDrainDoesNotMoveALaterConsumersPosition(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 9)
	cl := &mqtt.Client{ID: "device"}

	// A drain that had reached offset 9, then lost its consumer. It owns the
	// state under its id, as a live connection does; forgetConsumer removes
	// nothing for one that has been superseded.
	b.owner[cl.ID] = cl
	stale := b.cursor(cl.ID, "events", false)
	stale.next = 9
	stale.pumping = true
	b.endSession(cl, "it disconnected and its session ended", endedSession{})

	// A later consumer on the same client id, holding offset 3 in flight.
	live := b.cursor(cl.ID, "events", false)
	live.next = 4
	live.outstanding[3] = true
	live.expiresIn = time.Hour
	b.flushPositions()
	if got := storedPosition(t, b, cl.ID); got != 3 {
		t.Fatalf("setup: stored position %d, want 3", got)
	}

	// The departed drain's write fails and it gives back its offset 3.
	b.abandon(cl, "events", stale, 0, 3, stale.gen)

	if !live.outstanding[3] {
		t.Fatal("a departed consumer's drain cleared offset 3 from the live " +
			"consumer's in-flight set; that consumer has not acknowledged it")
	}
	if got := storedPosition(t, b, cl.ID); got != 3 {
		t.Fatalf("a departed consumer's drain moved the stored position to %d, "+
			"want 3 - the live consumer resumes past record 3, which it never "+
			"acknowledged and will now never receive", got)
	}
}

// RFC 0003 "Moving a consumer's position", invariant 1
//
// A drain that fails to write gives its records back to the cursor it was
// draining - but not across a seek.
//
// The window is real rather than theoretical: pumpBatch registers a batch
// under b.mu and releases the lock to write, and a seek lands in between.
// The cursor is then the same object in a later era, with what was
// outstanding already discarded and next where the consumer asked to be.
// Giving a record back to it rewinds it over the seek - and the seek may
// already have sent that offset again, so clearing it frees a delivery that
// is still live and the position advances past a record the consumer is
// holding, which is invariant 1's failure.
//
// The sibling above is the other way the drain's cursor stops being the one
// to rewind: it belongs to a consumer that has gone. This one is still live.
func TestADrainDoesNotGiveARecordBackAcrossASeek(t *testing.T) {
	b := testBroker(t)
	cl := &mqtt.Client{ID: "device"}

	// A drain that had reached offset 9 and registered offset 3 on the way.
	b.owner[cl.ID] = cl
	cur := b.cursor(cl.ID, "events", false)
	cur.next = 9
	era := cur.gen

	// The consumer seeks back to 3: what was outstanding is discarded, the
	// cursor moves, the era ends. Offset 3 is then sent again and is
	// unacknowledged.
	b.mu.Lock()
	cur.next = 4
	cur.gen++
	cur.outstanding[3] = true
	b.mu.Unlock()

	// And only now does the drain's write fail, giving back the copy of
	// offset 3 it registered before the seek.
	b.abandon(cl, "events", cur, 0, 3, era)

	b.mu.Lock()
	next, held := cur.next, cur.outstanding[3]
	b.mu.Unlock()
	if next != 4 {
		t.Fatalf("a drain from before the seek rewound the cursor to %d, want 4: "+
			"the consumer is dragged back over the place it asked to be", next)
	}
	if !held {
		t.Fatal("a drain from before the seek cleared offset 3, which the seek " +
			"has since sent again and the consumer has not acknowledged: the " +
			"position now advances past a record it is still holding")
	}
}

// docs/invariants.md 1, and RFC 0003 "`append` - Durable consumers"
//
// A superseded connection's teardown removes nothing.
//
// This is the takeover race made deterministic. End to end it is a coin
// flip - the damage needs the old connection's teardown to land after the
// new session has inherited, which it usually does not - so
// `TestTakeoverKeepsTheNewSessionSubscribed` runs fifteen rounds and still
// only catches it sometimes. Here the interleaving is not raced for: the
// state is built, a stale connection is handed to `forgetConsumer`, and the
// question is only whether ownership is what decides.
//
// What it guards against is the old answer coming back. Both substrate
// signals - `IsTakenOver`, and whether another client now holds this id -
// are read before the lock protecting these maps, so a teardown reading
// them in the gap between being disconnected and being marked taken over
// passes both and then deletes what the new session installed. Measured
// with a delay injected at exactly that point: 9 rounds in 10 failed.
func TestASupersededTeardownRemovesNothing(t *testing.T) {
	b := testBroker(t)
	c := b.reg.Get("events")

	old := &mqtt.Client{ID: "device"}
	current := &mqtt.Client{ID: "device"}

	// The new connection owns the id, as it does from OnSessionEstablish
	// onwards - which runs before the substrate disconnects the one it
	// supersedes, so this is true before any teardown can start.
	b.owner["device"] = current
	b.subs["device"] = []subscription{{filter: "events/#", channel: c, qos: 1}}
	live := b.cursor("device", "events", false)
	live.next = 7

	b.endSession(old, "a superseded teardown", endedSession{})

	if gone := len(b.cursorsOfLocked("device")) > 0; !gone {
		t.Fatal("a superseded teardown was allowed to delete the live session's cursor")
	}
	if got := b.cursor("device", "events", false); got.next != 7 {
		t.Fatalf("the live session's cursor was rebuilt at %d, so the position it had "+
			"reached went with the superseded connection; want 7", got.next)
	}

	// And the converse, or the check would pass by never deleting anything:
	// the connection that does own the id still tears its own state down.
	b.owner["device"] = current
	b.endSession(current, "its session ended", endedSession{})
	if kept := len(b.cursorsOfLocked("device")) > 0; kept {
		t.Fatal("the owning connection's teardown left its cursor behind")
	}
}

// docs/invariants.md 1 and 13, and RFC 0003 "`latest`"
//
// The same rule at the other site OnDisconnect deletes: the deliveries a
// SUBSCRIBE has earned and not yet sent.
//
// **Keyed by the connection, so the question of who owns the id never
// arises.** Keyed by client id, a superseded teardown deleted the live
// session's entry, and a SUBSCRIBE handled on a superseded connection
// replaced it. The bound it serves is real: a SUBACK whose write failed
// leaves a closure holding a client and its channels.
//
// The end-to-end halves are TestASupersededTeardownKeepsTheNewSubscribersCatchUp
// and TestASubscribeFromASupersededConnectionTakesNothingItsSuccessorIsOwed.
// This half is the rule on its own: whose entry a teardown deletes, and that
// every connection's own is deleted at its teardown.
func TestOnlyTheOwningConnectionClearsWhatASubscribeEarned(t *testing.T) {
	b := testBroker(t)

	current := &mqtt.Client{ID: "device"}
	stale := &mqtt.Client{ID: "device"}
	b.owner["device"] = current

	var ran int
	b.afterSuback[current] = func() { ran++ }
	b.afterSuback[stale] = func() {}

	b.OnDisconnect(stale, io.EOF, false)
	if _, kept := b.afterSuback[current]; !kept {
		t.Fatal("a superseded teardown deleted the live session's deliveries: its " +
			"SUBACK says granted and its catch-up never arrives")
	}
	if _, kept := b.afterSuback[stale]; kept {
		t.Fatal("a superseded connection's teardown left its own entry behind, holding a " +
			"closure for the life of the broker")
	}

	// And they still run when the SUBACK reaches the wire, which is the
	// thing the client is actually owed. Present in the map is not served.
	suback := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Suback}}
	b.OnPacketSent(current, suback, nil)
	if ran != 1 {
		t.Fatalf("the deliveries this SUBSCRIBE earned ran %d times, want 1", ran)
	}

	// And the converse, or the rule is satisfied by never deleting: the
	// connection that owns the id clears its own entry, which is the bound
	// that keeps a failed SUBACK write from holding a closure for the life
	// of the broker (invariant 13).
	b.afterSuback[current] = func() {}
	b.OnDisconnect(current, io.EOF, false)
	if _, kept := b.afterSuback[current]; kept {
		t.Fatal("the owning connection's teardown left behind the deliveries of a " +
			"SUBACK that never reached the wire")
	}
}

// docs/invariants.md 16, and RFC 0003 "Exactly once" /
// RFC 0004 "Exactly-once publishes"
//
// The third site in the same family, found by sweeping it: the exactly-once
// publishes a session was holding.
//
// **Its sibling one line above already asks.** Both paths that end a
// session call dropStoredPositions and then dropHeldPublishes, and the
// first stops where it is when the id is taken over mid-sweep - which is
// the window, because that sweep is a store transaction per channel. The
// second then ran on the successor regardless. What it deletes is a
// message the broker has already answered with PUBREC: the publisher was
// told it was held, the PUBREL that would complete it arrives at a broker
// with nothing left to complete, and the publish is gone having been
// acknowledged.
//
// Forcing the state is not evidence about how often a real broker reaches
// it, which is the same smaller claim TestASweepThatLosesTheIdPartwayStopsWhereItIs
// makes about its own guard. What it proves is that the guard fires, so it
// cannot be deleted in silence.
func TestASupersededTeardownKeepsTheLiveSessionsHeldPublishes(t *testing.T) {
	b := testBroker(t)
	b.SetQoS2(20, time.Minute)

	hold := func(t *testing.T, id uint16) {
		t.Helper()
		if err := b.holdIn("events", store.Exchange{Client: "device", PacketID: id},
			store.Record{MessageID: "m", Topic: "events/q2"}, time.Now()); err != nil {
			t.Fatalf("hold %d: %v", id, err)
		}
	}
	// What the broker records and what the channel's store holds, which
	// must agree.
	countFor := func(id string) int {
		n := b.heldCount(id)
		if in := b.storeHolds("events", store.Exchange{Client: id, PacketID: 9}); in != (n > 0) {
			t.Errorf("the broker records %d held for %s and the channel's store holding it is %v", n, id, in)
		}
		return n
	}

	// The connection that now holds the id, and the exchange it has opened
	// since: a publish the broker has answered with PUBREC and is keeping
	// until the PUBREL.
	current := &mqtt.Client{ID: "device"}
	stale := &mqtt.Client{ID: "device"}
	b.mu.Lock()
	b.owner["device"] = current
	b.mu.Unlock()
	hold(t, 9)

	b.dropHeldPublishes(stale, "its session expired")
	if n := countFor("device"); n != 1 {
		t.Fatalf("the live session holds %d unfinished exactly-once publishes, want 1: "+
			"a superseded teardown dropped a message the broker had already "+
			"answered with PUBREC, so nothing is left for the PUBREL to complete", n)
	}

	// And the converse, or the rule is satisfied by never dropping: the
	// connection that owns the id still takes its own with it, which is
	// what keeps a message nobody can ever finish from spending a
	// provider's bytes until `expires_after` (invariant 13).
	b.dropHeldPublishes(current, "its session expired")
	if n := countFor("device"); n != 0 {
		t.Fatalf("the owning connection's teardown left %d unfinished exactly-once "+
			"publishes behind, want 0", n)
	}
}

// docs/invariants.md 1, and RFC 0003 "`latest`"
//
// The third site in the family, and the one the field signature pointed at:
// a superseded connection's abandon must not delete the live session's
// in-flight record.
//
// **The collision is by construction, not by coincidence.** A consumer's
// `inflight` table is keyed by a client id and a packet identifier. Two connections hold the
// same id across a takeover, and each allocates identifiers from its own
// sequence starting at 1 - so a successor's first packets are exactly the
// ones its predecessor used first. Nothing rare has to happen for the keys
// to meet.
//
// **What the deletion costs is silence.** The live session's PUBACK then
// finds no record, so it is treated as a packet saguin never sent. That
// path frees the window slot and wakes the append pump, the queue offers
// and the shared-group drains - every waiter except the one that matters
// here, a queued current-state snapshot, which `drainPending` resumes and
// which only the recognised path calls. The subscriber is left holding a
// snapshot it will never be sent, with a free window to send it in, and
// the only trace is `undelivered=1` at teardown - which is the line the
// link-churn soak printed beside the failure this came from.
//
// Driven directly rather than raced for, as the two teardown rules above
// are: the state is built, a stale connection is handed the abandon, and
// the question is only whether the connection decides.
func TestAStaleAbandonKeepsTheLiveSessionsInflightRecord(t *testing.T) {
	b := testBroker(t)

	current := &mqtt.Client{ID: "device"}
	current.State.Inflight = mqtt.NewInflights()
	stale := &mqtt.Client{ID: "device"}
	stale.State.Inflight = mqtt.NewInflights()
	b.owner["device"] = current

	// The live session has been sent a record and holds packet 1 for it -
	// the first identifier any fresh connection allocates.
	b.setInflightLocked("device", 1, pending{
		conn: current, channel: "events", offset: 7})

	b.abandonLatest(stale, 1)

	p, ok := b.inflightLocked("device", 1)
	if !ok {
		t.Fatal("a superseded connection's abandon deleted the live session's " +
			"in-flight record: its PUBACK is now a packet saguin never sent, so " +
			"the window frees and the current-state snapshot waiting on it is " +
			"never resumed")
	}
	if p.conn != current || p.offset != 7 {
		t.Fatalf("the live session's record was replaced: conn_is_current=%v offset=%d, "+
			"want true and 7", p.conn == current, p.offset)
	}

	// And the converse, or the rule is satisfied by never deleting: the
	// connection the packet was registered on still takes its own out, which
	// is what keeps the map from growing per packet the broker gives up on.
	b.abandonLatest(current, 1)
	if _, kept := b.inflightLocked("device", 1); kept {
		t.Fatal("the registering connection's abandon left its own in-flight " +
			"record behind")
	}

	// The sibling on the append path takes the same rule, or half the family
	// is fixed and the other half is the one a replay goes through.
	b.setInflightLocked("device", 2, pending{
		conn: current, channel: "events", offset: 8})
	b.abandon(stale, "events", b.cursor("device", "events", false), 2, 8, 0)
	if _, ok := b.inflightLocked("device", 2); !ok {
		t.Fatal("a superseded connection's append abandon deleted the live " +
			"session's in-flight record")
	}
}

// docs/invariants.md 1 and 13, and RFC 0003 "How a stored position is dropped"
//
// A session that expires takes its stored position with it.
//
// **This is the only path that reaches one.** OnDisconnect drops a position
// on its `expire` branch, but the substrate raises that flag solely for a
// client whose Session Expiry Interval is zero - a session that ends with
// the connection. Every client that asks to keep its session disconnects
// with the flag clear, so the row is left for the sweep in each store's
// startup recovery, and a broker that stays up does not run one.
//
// It is invariant 1 before it is invariant 13. A client id whose session is
// gone is told Session Present = 0 on its next connect, and a replacement
// device taking that id over would be resumed at the departed consumer's
// offset - told it had missed nothing and served none of the records below
// it. Measured before the fix: a position with a two-second interval was
// still in the table twenty seconds later, and one survived a restart only
// because the restart is what swept it.
//
// Driven through the hook rather than through a session actually expiring,
// because the substrate's sweep runs on its own timer: what is under test
// is what saguin does when told, not when it is told.
func TestAnExpiredSessionLosesItsStoredPosition(t *testing.T) {
	b := testBroker(t)
	cl := &mqtt.Client{ID: "device"}
	reader := store.MQTTReader(cl.ID)

	lg := b.logs["events"]
	if lg == nil {
		t.Fatal("the test broker has no events channel")
	}
	appendUpTo(t, lg, 9)
	if err := lg.SavePosition(store.Position{
		Reader: reader, Offset: 7, LastSeen: time.Now(), ExpiresIn: time.Second,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, ok, _ := lg.Position(reader); !ok {
		t.Fatal("the position was not stored, so this test proves nothing")
	}

	b.OnClientExpired(cl)

	if _, ok, _ := lg.Position(reader); ok {
		t.Fatal("a session expired and its stored position stayed: the next client " +
			"to use that id is told Session Present = 0 and then resumed at this " +
			"offset, which is invariant 1 with the signal and the behaviour disagreeing")
	}

	// And not a live connection's, if one has taken the id over. The
	// substrate removes a superseded client from its table before it can be
	// swept, so this should be unreachable - the check is one map read
	// against deleting a live consumer's place in every channel.
	if err := lg.SavePosition(store.Position{
		Reader: reader, Offset: 9, LastSeen: time.Now(), ExpiresIn: time.Second,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	b.owner[cl.ID] = &mqtt.Client{ID: cl.ID}
	b.OnClientExpired(cl)
	if _, ok, _ := lg.Position(reader); !ok {
		t.Fatal("a superseded connection's expiry dropped the position of the " +
			"connection that now holds that id")
	}
}

// RFC 0003 "`queue` - States" - invariant 7
//
// Forgetting a delivery does not take a live one's packet mapping with it.
//
// byPacket is keyed by client id and packet identifier, and both are
// reused: the substrate's identifiers wrap back to zero and a new
// connection under the same client id starts its counter there. A delivery
// forgotten late therefore meets the same key as a delivery that is still
// running.
//
// What it costs is the state returnUnsent exists to end, reached through
// the thing that ends it: the live delivery's PUBACK finds no mapping in
// OnQosComplete, so no attempt is counted and no visibility deadline
// starts, and returnUnsent skips the record because its own guard is the
// presence of that mapping. The record sits in Delivering with nothing
// coming for it.
//
// Driven directly rather than raced. The window needs a teardown to land
// after a reconnect has been given a low-numbered packet, which is a
// interleaving to arrange rather than to wait for - and what is under test
// is the bookkeeping, which does not need the race to be wrong.
func TestForgettingADeliveryLeavesALiveOnesPacketMapping(t *testing.T) {
	b := testBroker(t)

	// Both under one client id and one packet identifier, which is what a
	// reconnect produces: the stale delivery from the old connection, and
	// the live one the new connection is working on.
	stale := &delivery{channel: "jobs", offset: 1, holder: "worker", packetID: 5}
	live := &delivery{channel: "jobs", offset: 2, holder: "worker", packetID: 5}

	b.deliveries["stale-id"] = stale
	b.deliveries["live-id"] = live
	b.byPacket[packetKey("worker", 5)] = "live-id"

	b.forget(stale)

	if got, ok := b.byPacket[packetKey("worker", 5)]; !ok || got != "live-id" {
		t.Fatalf("forgetting a stale delivery removed the live delivery's packet "+
			"mapping (%q, present=%v): its PUBACK now reaches nothing, so no "+
			"deadline starts and returnUnsent cannot see it either", got, ok)
	}
	if _, ok := b.deliveries["stale-id"]; ok {
		t.Fatal("the stale delivery itself was not forgotten")
	}

	// And the mapping does go when it is this delivery's own.
	b.byPacket[packetKey("worker", 5)] = "live-id"
	b.forget(live)
	if _, ok := b.byPacket[packetKey("worker", 5)]; ok {
		t.Fatal("a delivery's own packet mapping was left behind, which is the " +
			"unbounded map this check was added beside")
	}
}

// docs/invariants.md 1
//
// A position **already collected** for writing is not written back after
// its session has been swept.
//
// **This is the race that reopened an earlier fix.** flushPositions
// collects its batch under b.mu and writes outside it, on purpose - that
// write is what used to sit on the publish path. So a drop holding b.mu
// excluded nothing: a position collected before a session expired landed
// after the row was deleted, and nothing removed it again, because the
// client is gone from the substrate's table and its expiry never fires
// twice. A replacement device on that client id was then told Session
// Present = 0 and served none of the records below the ghost offset.
//
// **The interleaving is arranged rather than waited for**, and it has to
// be the one where the flush has already collected. A test that lets the
// sweep run first passes without the fix at all - the sweep deletes the
// cursors, so a later flush finds nothing to write and the row stays gone
// for the wrong reason. That version of this test was written first and
// passed against the defect, which is the whole reason this one holds the
// lock instead.
func TestAFlushDoesNotWriteBackASweptPosition(t *testing.T) {
	b := testBroker(t)
	cl := &mqtt.Client{ID: "device"}
	reader := store.MQTTReader(cl.ID)
	lg := b.logs["events"]
	// The records it read. Without them the flush's write of 6 is refused
	// (store.ErrPastNext), and the check below passes for that reason
	// instead of for the one under test.
	appendUpTo(t, lg, 6)

	// A consumer that has read to offset 5 and not yet had it flushed.
	b.owner[cl.ID] = cl
	cur := b.cursor(cl.ID, "events", false)
	cur.next = 6
	cur.expiresIn = time.Second

	// Hold the ordering lock, so the flush below collects its batch and then
	// stops exactly where the race is: after the read, before the write.
	b.positions.Lock()
	flushed := make(chan struct{})
	go func() {
		b.flushPositions()
		close(flushed)
	}()

	// Let it get as far as the lock. Not an assertion - if it has not got
	// there yet the test still exercises the same path, because the write
	// cannot happen without this lock either way.
	time.Sleep(50 * time.Millisecond)

	// The sweep, in the order dropStoredPositions does it: the cursor goes
	// under b.mu, which is what tells the waiting flush its value is stale.
	b.mu.Lock()
	b.dropCursorsLocked(cl.ID)
	b.mu.Unlock()
	if _, err := lg.DropPosition(reader); err != nil {
		t.Fatalf("drop: %v", err)
	}

	b.positions.Unlock()
	<-flushed

	if _, ok, _ := lg.Position(reader); ok {
		t.Fatal("a flush wrote back a position whose session had been swept: the " +
			"row is now permanent, since the client is gone from the substrate's " +
			"table and its expiry cannot fire twice - and the next client to use " +
			"that id is told Session Present = 0 and resumed at this offset")
	}

	// And a live consumer's flush still works, or the check above would be
	// passing by never writing anything.
	live := &mqtt.Client{ID: "live-one"}
	b.owner[live.ID] = live
	lc := b.cursor(live.ID, "events", false)
	lc.next = 4
	lc.expiresIn = time.Second
	b.flushPositions()
	if _, ok, _ := lg.Position(store.MQTTReader(live.ID)); !ok {
		t.Fatal("a live consumer's position was not flushed, so this test proves nothing")
	}
}

// RFC 0003 "`queue` - Dead-lettering" and "Attempts, timeout, and redelivery"
//
// A record dead-lettered by a worker that answered before its `PUBACK`
// still carries when it was first and last delivered.
//
// RFC 0003 says the attempt increments when the worker is known to have
// received the record - at the `PUBACK`, or when it answers, whichever
// comes first - and the two delivery times are the same fact recorded a
// different way. Only the counter obeyed it: `FirstSeen` and `LastSeen`
// were stamped in `Lease`, which runs at the `PUBACK`, so a worker that
// answers from inside its receive handler left both zero and
// `deadLettered` omitted the two headers it writes only when they are set.
//
// **That is the ordinary shape, not the exotic one.** Paho sends the
// `PUBACK` when the handler returns, so a worker answering inside it
// answers first - which is what `examples/support/worker` does and what the
// demonstration does. Measured with a control differing only in the order
// of two packets: five `saguin-dlq-*` headers against seven.
func TestADeadLetterFromAnAnsweringWorkerKeepsItsTimestamps(t *testing.T) {
	q := store.NewQueue()
	dlq := store.NewLog()
	if _, err := q.Enqueue(store.Record{Topic: "jobs/x", Payload: []byte("j")}); err != nil {
		t.Fatal(err)
	}

	dl := store.DeadLetter{Log: dlq, Record: func(it store.Item) store.Record {
		return store.Record{Topic: "jobs/__dlq/x", Payload: it.Payload}
	}}

	// Two returns, both answered from Delivering - the worker replies before
	// its PUBACK is processed, so Lease never runs. maxAttempts 2, so the
	// second spends the last attempt and dead-letters.
	var out store.Outcome
	for attempt := 1; attempt <= 2; attempt++ {
		offered, err := q.Offer(1, time.Now())
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer %d: %v %d", attempt, err, len(offered))
		}
		o := offered[0]
		var ok bool
		out, ok, err = q.Release(
			store.Held{Offset: o.Offset, Epoch: o.Epoch, Holder: "w"},
			time.Now(), true, 2, dl)
		if err != nil || !ok {
			t.Fatalf("release %d: %v ok=%v", attempt, err, ok)
		}
	}

	if !out.DeadLettered {
		t.Fatalf("the record was not dead-lettered after two answered returns: %+v", out.Item)
	}
	if out.Item.Attempts != 2 {
		t.Fatalf("attempts %d, want 2 - the counter already followed this rule",
			out.Item.Attempts)
	}
	if out.Item.FirstSeen.IsZero() {
		t.Fatal("no first delivery time on a record dead-lettered by an answering " +
			"worker: saguin-dlq-first is absent, and the first question anybody asks " +
			"of a dead-lettered record is when it started failing")
	}
	if out.Item.LastSeen.IsZero() {
		t.Fatal("no last delivery time on a record dead-lettered by an answering worker")
	}
}

// docs/invariants.md 1
//
// A session sweep that has lost the client id to a live connection removes
// nothing, and the check is made where the deletes are.
//
// The check existed one frame up, in OnClientExpired, and the deletes it
// guarded happen 40–55ms later at three hundred channels - after
// b.positions has been waited for and a store transaction per channel has
// run. The substrate does not remove the old client from its table until
// the hook returns, so a CONNECT inside that window inherits the session,
// is answered Session Present = 1, and would then have every position for
// that reader deleted underneath it.
func TestASweepThatLostTheIdRemovesNothing(t *testing.T) {
	b := testBroker(t)
	expiring := &mqtt.Client{ID: "device"}
	reader := store.MQTTReader(expiring.ID)
	lg := b.logs["events"]
	appendUpTo(t, lg, 11)

	if err := lg.SavePosition(store.Position{
		Reader: reader, Offset: 11, LastSeen: time.Now(), ExpiresIn: time.Second,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// A new connection took the id over while the sweep was on its way, and
	// is reading the channel: its place in it is a cursor in memory as well
	// as a stored position.
	b.owner[expiring.ID] = &mqtt.Client{ID: expiring.ID}
	b.cursor(expiring.ID, "events", false)
	reading := func() bool {
		con := b.lookupConsumer(expiring.ID)
		if con == nil {
			return false
		}
		con.cmu.Lock()
		defer con.cmu.Unlock()
		return con.cursors["events"] != nil
	}
	if !reading() {
		t.Fatal("the new connection has no cursor on the channel, so this proves nothing")
	}

	b.dropStoredPositions(expiring, "its session expired", nil)

	if _, ok, _ := lg.Position(reader); !ok {
		t.Fatal("a sweep for a superseded connection deleted the position of the " +
			"connection that now holds that id: it was told Session Present = 1 and " +
			"then had its place in every channel removed underneath it")
	}
	if !reading() {
		t.Fatal("a sweep for a superseded connection dropped the cursor of the connection " +
			"that now holds that id: it stops being served while it stays connected")
	}

	// And it still removes its own when nobody else holds the id.
	delete(b.owner, expiring.ID)
	b.dropStoredPositions(expiring, "its session expired", nil)
	if _, ok, _ := lg.Position(reader); ok {
		t.Fatal("the sweep left the position of a session nobody holds")
	}
	if reading() {
		t.Fatal("the sweep left the cursor of a session nobody holds")
	}
}

// RFC 0003 "Moving a consumer's position"
//
// A flush does not write the offset the consumer's stream had reached over
// the one it asked to be at.
//
// **The interleaving that produced five findings, ended by removing it
// rather than by guarding it.** flushPositions collects under b.mu and
// writes under b.positions, so anything that moves a consumer's position
// between the two is racing it, and a seek moves the *same* cursor object -
// so the check that asks whether the cursor is still in the map answers yes
// and the stale offset goes over the top. The flush now reads what the
// cursor says at write time, under b.positions, which is the lock handleSeek
// stores and moves the cursor under; there is no schedule left in which the
// two disagree.
//
// What it costs is the seek's own contract rather than a record: the flush
// writes the lowest unacknowledged offset, which never passes a record the
// consumer has not had, so invariant 1 holds. But the broker answers a seek
// with the offset it stored and would then silently un-store it - a consumer
// seeking back got no replay, one seeking forward past a poison stretch was
// handed the stretch again. It also made
// TestSeekSentinelsNeedNoKnowledgeOfTheChannel flake on a loaded machine,
// which is how it was caught.
//
// The assertion is what the flush does after the seek rather than an
// interleaving, because the interleaving is now unreachable by construction:
// the seek holds b.positions across both halves of its work, so no flush can
// read a cursor mid-seek. What a test can still catch is the seek failing to
// move the cursor to match what it stored, which puts the whole thing back.
func TestAFlushDoesNotOverwriteASeek(t *testing.T) {
	b := testBroker(t)
	cl := &mqtt.Client{ID: "device"}
	cl.Properties.Props.SessionExpiryInterval = 300

	// A consumer whose stream has reached offset 6, not yet flushed.
	b.owner[cl.ID] = cl
	cur := b.cursor(cl.ID, "events", false)
	cur.next = 6
	cur.expiresIn = time.Second

	// No Response Topic, so no reply is attempted and no socket is needed -
	// RFC 0003: a client that sets none gets no reply and the seek still
	// applies. Payload 0 is the floor sentinel, which every channel has.
	b.handleSeek(cl, packets.Packet{Payload: []byte("0")}, "events")
	if got := storedPosition(t, b, cl.ID); got != 1 {
		t.Fatalf("the seek stored %d, want the floor 1, so this test proves nothing", got)
	}

	b.flushPositions()

	if got := storedPosition(t, b, cl.ID); got != 1 {
		t.Fatalf("a flush wrote %d over a stored seek to 1: the broker confirmed "+
			"the seek and then silently undid it", got)
	}
}

// RFC 0003 "Moving a consumer's position", invariant 1
//
// An acknowledgement for a record sent before a seek cannot move the
// position past the seek.
//
// This is what a seek applied to a running stream costs, and the whole
// reason the cursor carries an era. Records are on the wire at QoS 1 when
// the seek arrives. The seek discards what is outstanding and feeds the
// consumer again from where it asked to be - which re-sends some of those
// very offsets under fresh packet identifiers. The late PUBACK for the first
// copy then names an offset that is outstanding again, and clearing it lets
// the position advance over a record the consumer is still holding and has
// not acknowledged: it resumes past the record, reports success, and nobody
// is told.
//
// Here offset 3 is in flight, the consumer seeks back to 2, and 3 goes out
// again. The PUBACK for the first copy arrives afterwards. The stored
// position must stay at 3.
func TestAStaleAcknowledgementCannotMoveThePositionPastASeek(t *testing.T) {
	b := testBroker(t)
	cl := &mqtt.Client{ID: "device"}
	cl.Properties.Props.SessionExpiryInterval = 300

	// Four records on the channel, so offset 2 is a place to seek to.
	for i := 1; i <= 4; i++ {
		if _, err := b.logs["events"].Append(store.Record{
			MessageID: "m", Topic: "events/x"}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Records 1 to 4 have been sent; 3 is still unacknowledged.
	b.owner[cl.ID] = cl
	cur := b.cursor(cl.ID, "events", false)
	cur.next = 5
	cur.outstanding[3] = true
	cur.expiresIn = time.Second
	b.setInflightLocked(cl.ID, 7, pending{channel: "events", offset: 3, gen: cur.gen})

	b.handleSeek(cl, packets.Packet{Payload: []byte("2")}, "events")
	b.mu.Lock()
	moved := cur.next
	b.mu.Unlock()
	if moved != 2 {
		t.Fatalf("the seek left the cursor at %d, want 2: it did not reach the "+
			"running stream, so this test proves nothing", moved)
	}

	// Fed again from 2, so offset 3 is sent a second time under a packet
	// identifier of its own. This is what the pump does; done here because
	// this client has no socket.
	b.mu.Lock()
	cur.next = 4
	cur.outstanding[3] = true
	b.setInflightLocked(cl.ID, 9, pending{channel: "events", offset: 3, gen: cur.gen})
	b.mu.Unlock()

	// And the PUBACK for the copy sent before the seek arrives late.
	b.OnQosComplete(cl, packets.Packet{PacketID: 7})

	b.flushPositions()
	if got := storedPosition(t, b, cl.ID); got != 3 {
		t.Fatalf("a PUBACK for a record sent before the seek moved the stored "+
			"position to %d, want 3: the consumer is still holding offset 3 "+
			"unacknowledged and now resumes past it", got)
	}
}

var errStorageFull = errors.New("disk full")

// failNthSave stores every position except one, so a seek can be refused at
// storage on a connection where an earlier seek was stored successfully.
type failNthSave struct {
	LogStore
	fail  int // 1-based call number to refuse
	calls int
}

func (s *failNthSave) SavePosition(p store.Position) error {
	s.calls++
	if s.calls == s.fail {
		return errStorageFull
	}
	return s.LogStore.SavePosition(p)
}

// RFC 0003 "Moving a consumer's position"
//
// A seek refused at storage changes nothing.
//
// The client is told, correctly, and the stream it is reading goes on from
// where it was - same position, same records outstanding, same era, so the
// acknowledgements already on their way still count. Storing first and
// moving the cursor only on success is what makes that true without a
// rollback: the flag this replaced had to be raised before the store and put
// back afterwards, and being right about *which* claim to put back - this
// seek's, not the cursor's, which an earlier confirmed seek may also have
// made - was two separate defects in it.
//
// The last assertion is the one a narrowing would drop: the consumer's
// position must still advance. A cursor left in a state the flusher declines
// to write is a position frozen for the life of the connection, and a
// restart then replays everything since the last flush before the failure.
// At-least-once permits that and it is still a consumer doing work twice for
// a seek that did not happen.
func TestASeekRefusedAtStorageChangesNothing(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 6)
	cl := &mqtt.Client{ID: "device"}
	cl.Properties.Props.SessionExpiryInterval = 300

	// The stream has reached 6, holding offset 4 unacknowledged.
	b.owner[cl.ID] = cl
	cur := b.cursor(cl.ID, "events", false)
	cur.next = 6
	cur.outstanding[4] = true
	cur.expiresIn = time.Second
	era := cur.gen

	// The first store fails, which is this seek's.
	probe := &failNthSave{LogStore: b.logs["events"], fail: 1}
	b.logs["events"] = probe

	b.handleSeek(cl, packets.Packet{Payload: []byte("0")}, "events")
	if probe.calls != 1 {
		t.Fatalf("the store was called %d times, so the refusal never happened", probe.calls)
	}

	b.mu.Lock()
	next, gen, held := cur.next, cur.gen, cur.outstanding[4]
	b.mu.Unlock()
	switch {
	case next != 6:
		t.Fatalf("a seek refused at storage moved the running stream to %d, want 6: "+
			"the client was told the seek failed and the broker did it anyway", next)
	case gen != era:
		t.Fatal("a seek refused at storage ended the cursor's era: the " +
			"acknowledgements already on their way stop counting, and the " +
			"records they are for are sent again")
	case !held:
		t.Fatal("a seek refused at storage discarded what was outstanding: the " +
			"position can now advance over a record this consumer has not " +
			"acknowledged")
	}

	b.flushPositions()
	if got := storedPosition(t, b, cl.ID); got != 4 {
		t.Fatalf("the flush after a refused seek stored %d, want 4: this "+
			"consumer's position is frozen for the life of the connection", got)
	}
}

// RFC 0003 "`queue` - Attempts, timeout, and redelivery"
//
// An acknowledgement that arrives before the `PUBACK` counts its attempt.
//
// RFC 0003's rule is that the attempt increments when the worker is known
// to have received the record - at the `PUBACK`, or when it answers,
// whichever comes first. `Lease` covers the first, `Release` covers the
// second for a return, and `Resolve` is the third site: the path an `ack`
// takes. It counted nothing, so the broker's own acked line reported
// `attempts=0` for a worker that had just done the work - and zero there
// says "never delivered".
//
// **This test exists because the fix shipped without one and reverting it
// stayed green.** The other two sites of the same rule each have a test
// that reddens; this one could be undone by any refactor with nothing
// noticing. Only the log line reads the value, which is why it is a
// cosmetic defect and not a correctness one - and exactly why nothing else
// would catch it.
func TestAnAcknowledgementBeforeThePubackCountsItsAttempt(t *testing.T) {
	q := store.NewQueue()
	if _, err := q.Enqueue(store.Record{Topic: "jobs/x", Payload: []byte("j")}); err != nil {
		t.Fatal(err)
	}

	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer: %v %d", err, len(offered))
	}
	o := offered[0]

	// No Lease call: the worker answered from Delivering, which is what a
	// client library that acknowledges when its handler returns produces -
	// the shape RFC 0003 recommends and examples/support/worker has.
	it, ok, err := q.Resolve(store.Held{Offset: o.Offset, Epoch: o.Epoch, Holder: "w"})
	if err != nil || !ok {
		t.Fatalf("resolve: %v ok=%v", err, ok)
	}
	if it.Attempts != 1 {
		t.Fatalf("attempts %d after an acknowledgement from Delivering, want 1: the "+
			"broker reports this on its acked line, and zero says the record was "+
			"never delivered when a worker had just done it", it.Attempts)
	}

	// And a worker that acknowledged transport first is not double-counted.
	if _, err := q.Enqueue(store.Record{Topic: "jobs/y", Payload: []byte("j")}); err != nil {
		t.Fatal(err)
	}
	offered, err = q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer: %v %d", err, len(offered))
	}
	o = offered[0]
	held := store.Held{Offset: o.Offset, Epoch: o.Epoch, Holder: "w"}
	if _, ok, err := q.Lease(held, time.Now(), time.Minute); err != nil || !ok {
		t.Fatalf("lease: %v ok=%v", err, ok)
	}
	it, ok, err = q.Resolve(held)
	if err != nil || !ok {
		t.Fatalf("resolve: %v ok=%v", err, ok)
	}
	if it.Attempts != 1 {
		t.Fatalf("attempts %d after a PUBACK and then an acknowledgement, want 1: "+
			"the attempt is counted once, at whichever came first", it.Attempts)
	}
}

// sweepGate holds the first delete of a session sweep still, so that a test
// can act while the walk is partway through it. One gate is shared by every
// channel's store, which is what makes "the first channel, whichever it is"
// the thing that pauses rather than one named channel.
type sweepGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

// pausingLog is a real store whose DropPosition can be held still. Only that
// method is intercepted - everything else is the store the broker would have
// used - so the sweep does its ordinary work and only its timing is under
// the test's control.
//
// One wrapper per channel, each keeping its own store: a single wrapper
// shared across the three would have every channel read and delete from
// whichever store was installed last, and the test would pass while
// measuring nothing.
type pausingLog struct {
	LogStore
	g *sweepGate
}

func (p *pausingLog) DropPosition(reader string) (bool, error) {
	p.g.once.Do(func() {
		close(p.g.entered)
		<-p.g.release
	})
	return p.LogStore.DropPosition(reader)
}

// docs/invariants.md 1
//
// **The per-channel re-check inside the sweep, seen firing.** Its sibling
// above covers the check at the function's entry, which returns before the
// walk begins - so until now nothing had reached the one inside the loop,
// and repeated review failed to reproduce it from outside the
// process.
//
// The reason it resisted is the window: the sweep walks one store
// transaction per channel, 42 to 54ms at three hundred channels, and a
// CONNECT has to land inside that. Trying to hit it by timing is what
// failed. This holds it still instead - the first channel's delete blocks on
// a channel the test owns, the id is taken over while it is held, and the
// walk is released. Deterministic, and the same technique that works for
// the shutdown measurements.
//
// **What this proves and what it does not.** It proves the guard fires and
// stops the walk, so the check cannot be deleted silently. It does not prove
// a real broker reaches that window - that is a race nobody here has
// reproduced, and forcing the state is not evidence about how often the
// state occurs. The two claims are different sizes and this is the smaller
// one.
func TestASweepThatLosesTheIdPartwayStopsWhereItIs(t *testing.T) {
	// Three channels, so the walk has somewhere to be stopped: one delete
	// before the takeover and two the guard has to save.
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "alpha", Type: channel.Append},
		{Name: "bravo", Type: channel.Append},
		{Name: "charlie", Type: channel.Append},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	var logged bytes.Buffer
	b := New(reg, slog.New(slog.NewTextHandler(&logged, nil)))

	expiring := &mqtt.Client{ID: "device"}
	reader := store.MQTTReader(expiring.ID)
	for name, lg := range b.logs {
		appendUpTo(t, lg, 11)
		if err := lg.SavePosition(store.Position{
			Reader: reader, Offset: 11, LastSeen: time.Now(), ExpiresIn: time.Second,
		}); err != nil {
			t.Fatalf("saving a position on %q: %v", name, err)
		}
	}

	g := &sweepGate{entered: make(chan struct{}), release: make(chan struct{})}
	for name, lg := range b.logs {
		b.logs[name] = &pausingLog{LogStore: lg, g: g}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.dropStoredPositions(expiring, "its session expired", nil)
	}()

	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep never reached a channel")
	}

	// The CONNECT the substrate has not told saguin about yet: a new
	// connection claiming the same id while the old session is being swept.
	b.mu.Lock()
	b.owner[expiring.ID] = &mqtt.Client{ID: expiring.ID}
	b.mu.Unlock()

	close(g.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep did not finish")
	}

	kept := 0
	for _, lg := range b.logs {
		if _, ok, _ := lg.Position(reader); ok {
			kept++
		}
	}
	// One channel was already past its delete when the id changed hands. The
	// other two are what the guard saved, and they are the difference between
	// a client told Session Present = 1 and handed all of its state and one
	// handed part of it.
	if kept != 2 {
		t.Errorf("%d of 3 channels kept the position of the connection that now holds "+
			"the id, want 2: the sweep carried on deleting after the takeover, so a "+
			"client answered Session Present = 1 resumes on some channels and replays "+
			"from the floor on the rest\n%s", kept, logged.String())
	}

	// And it said so. A guard that stops the walk silently leaves an operator
	// with a client that half-replayed and nothing to read about why.
	if !strings.Contains(logged.String(), "the client id was taken over") {
		t.Errorf("the sweep stopped without saying why:\n%s", logged.String())
	}
}

// RFC 0003 "`queue` - Attempts, timeout, and redelivery" - invariant 7
//
// **returnUnsent's backstop, made to fire deliberately.** Ten review
// rounds and repeated soaks passed without this warning ever appearing,
// leaving one open question: either the path is unreachable now that the
// substrate patch is carried, in which case the comment beside it describes
// a cost that no longer exists, or the tests could not make the server
// withhold a registered packet. Waiting for another soak answers neither.
//
// This builds the state instead. The substrate marks a packet it registered
// and declined to write by storing it again with a negative expiry, and
// returnUnsent reads exactly that - so a packet set that way is the condition
// itself rather than an imitation of it. If the substrate ever stops using
// that marker, this test and the broker stop agreeing at the same moment,
// which is the point of asking it here.
//
// **What it settles and what it leaves open.** It settles that the guard
// works and what it costs when it runs: the record comes back available,
// with no attempt spent, and the operator is told the worker is starved.
// It leaves open how often a real broker reaches the state - which is a
// question about the substrate's flow control rather than about this code,
// and a smaller one now that the answer to "does the backstop work" is not
// also unknown.
func TestReturnUnsentGivesBackARecordTheServerNeverPutOnTheWire(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "jobs", Type: channel.Queue, VisibilityTimeout: 30, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	var logged bytes.Buffer
	b := New(reg, slog.New(slog.NewTextHandler(&logged, nil)))
	srv := mqtt.New(&mqtt.Options{InlineClient: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)

	// A worker the server knows about. A hand-built client reads as live -
	// Closed() asks whether its context is cancelled, not whether it holds a
	// socket - which is what returnUnsent checks before touching anything.
	worker := srv.NewClient(nil, "test", "worker-1", false)
	srv.Clients.Add(worker)

	q := b.queues["jobs"]
	if _, err := q.Enqueue(store.Record{Topic: "jobs/a", MessageID: "m-1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer: %v (%d records)", err, len(offered))
	}

	// The record is Delivering: handed out, no PUBACK, and by design no
	// visibility deadline running (invariant 7). Nothing else will rescue it.
	const packetID = 7
	d := &delivery{channel: "jobs", offset: offered[0].Offset, epoch: offered[0].Epoch,
		holder: worker.ID, packetID: packetID}
	b.deliveries[offered[0].DeliveryID] = d
	b.byPacket[packetKey(worker.ID, packetID)] = offered[0].DeliveryID

	// The substrate's own marker for a packet it registered and then
	// declined to write, because the worker's send quota was spent.
	worker.State.Inflight.Set(packets.Packet{PacketID: packetID, Expiry: -1})

	b.returnUnsent()

	// The record is back, and back as available work rather than as a
	// delivery to somebody who never had it.
	again, err := q.Offer(1, time.Now())
	if err != nil || len(again) != 1 {
		t.Fatalf("the stranded record was not returned to the queue (%d records, err=%v): "+
			"it is held by a worker that never received it, with no deadline to rescue "+
			"it and no attempt spent - the job is lost until the session ends", len(again), err)
	}
	if again[0].Offset != offered[0].Offset {
		t.Fatalf("a different record came back: offset %d, want %d", again[0].Offset, offered[0].Offset)
	}

	// No attempt was spent. The worker never showed it had the record - it
	// never had it - so this is the send that did not happen rather than a
	// delivery that failed (RFC 0003). Spending one here would dead-letter a
	// job after three sends nobody ever made.
	if again[0].Attempt != 1 {
		t.Errorf("the returned record is on attempt %d, want 1: a send that never "+
			"happened spent one of the record's attempts", again[0].Attempt)
	}

	// And the operator is told: a worker whose window stayed full with work
	// registered to it has stopped acknowledging, and nothing else anywhere
	// says so.
	if !strings.Contains(logged.String(), "never carried") {
		t.Errorf("nothing was logged about a starved worker, which is what makes it "+
			"indistinguishable from an empty queue:\n%s", logged.String())
	}
}

// RFC 0003 "`queue` - Attempts, timeout, and redelivery" - invariant 7
//
// **A job whose delivery expired before it was ever sent is given back.** The
// expiry removes the worker's in-flight entry, so returnUnsent, which finds a
// never-sent job by deleting that entry, finds nothing; told only that the
// delivery was dropped, the broker kept the job Delivering, held by a worker
// that never had it and waiting for a PUBACK that cannot come. The delivery
// is registered the way the engine registers one (OnQosPublish), withheld for
// the worker's window, and expired by the engine's own sweep.
func TestAJobWhoseDeliveryExpiredUnsentIsGivenBack(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "jobs", Type: channel.Queue, VisibilityTimeout: 30, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := mqtt.New(&mqtt.Options{InlineClient: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)
	if err := srv.AddHook(b, nil); err != nil {
		t.Fatal(err)
	}
	worker := srv.NewClient(nil, "test", "worker-1", false)
	worker.Properties.ProtocolVersion = 5
	srv.Clients.Add(worker)

	q := b.queues["jobs"]
	if _, err := q.Enqueue(store.Record{Topic: "jobs/a", MessageID: "m-1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer: %v (%d records)", err, len(offered))
	}
	raw, err := hex.DecodeString(offered[0].DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	b.deliveries[offered[0].DeliveryID] = &delivery{channel: "jobs", offset: offered[0].Offset, epoch: offered[0].Epoch}
	now := time.Now().Unix()
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 7,
		ProtocolVersion: 5, TopicName: "jobs/a", Created: now, Expiry: now + 1,
		Properties: packets.Properties{CorrelationData: raw}}
	worker.State.Inflight.Set(pk)
	worker.State.Inflight.Withhold(pk.PacketID)
	b.OnQosPublish(worker, pk, now, 0)
	if _, ok := b.byPacket[packetKey(worker.ID, pk.PacketID)]; !ok {
		t.Fatal("the delivery was not registered to the worker, so this proves nothing")
	}

	expired := worker.ClearExpiredInflights(now+5, 0)
	if len(expired) != 1 {
		t.Fatalf("the sweep expired %v, want the one never-sent delivery: this proves nothing", expired)
	}
	b.returnUnsent() // the backstop, which is what this was left to

	b.mu.Lock()
	_, waiting := b.byPacket[packetKey(worker.ID, pk.PacketID)]
	holding := b.holding[heldBy{worker.ID, "jobs"}]
	b.mu.Unlock()
	again, err := q.Offer(1, time.Now())
	t.Logf("after the expiry: registered to the worker %v, holding %d; offered again %d records (err %v)",
		waiting, holding, len(again), err)
	if err != nil || len(again) != 1 {
		t.Fatalf("the job whose delivery expired unsent was not given back (%d records): it is held "+
			"by a worker that never received it, waiting for a PUBACK that cannot come", len(again))
	}
	if again[0].Offset != offered[0].Offset || again[0].Attempt != 1 {
		t.Errorf("offered again offset %d on attempt %d, want offset %d on attempt 1: a send that "+
			"never happened spent an attempt or moved the record", again[0].Offset, again[0].Attempt, offered[0].Offset)
	}
	if waiting || holding != 0 {
		t.Errorf("the worker still holds the job (registered %v, holding %d)", waiting, holding)
	}
}

// docs/invariants.md 13, RFC 0002 "publish_rate and publish_bytes"
//
// **A publish budget is kept past the disconnect and not for ever**, and
// both halves matter. Keeping it is what stops a client fetching a fresh
// one by reconnecting - that is the end-to-end test. What bounds it is that
// it goes as soon as it has refilled, which is the moment a kept budget
// stops saying anything a new one would not.
//
// A unit test because the thing being asserted is the absence of a map
// entry, and nothing on the wire reports one either way.
func TestADepartedClientsBudgetIsKeptUntilItHasRefilled(t *testing.T) {
	b := testBroker(t)
	b.limits.PublishRate = 5

	held := func() bool {
		_, ok := b.allowances.Load("device")
		return ok
	}

	start := time.Now()
	// Spends the whole bucket and then some, so it is plainly short of full.
	for range 8 {
		b.withinPublishRate("device", "device", 1)
	}
	if !held() {
		t.Fatal("setup: no budget was created for a client that published")
	}

	b.forgetAllowance("device")
	if !held() {
		t.Fatal("the budget went with the connection, so a client reconnecting inside " +
			"the same second is handed a full one and the limit bounds a connection " +
			"rather than a client")
	}

	// A sweep while it is still short of full leaves it alone.
	b.sweepAllowances(start)
	if !held() {
		t.Error("a budget that had not refilled was swept, which is the same hole by " +
			"another route: whatever the client had spent is forgiven")
	}

	// And once it has refilled it goes, because by then it says nothing a
	// fresh one would not.
	b.sweepAllowances(start.Add(2 * time.Second))
	if held() {
		t.Error("a refilled budget for a departed client was still held: one entry per " +
			"client id ever seen is unbounded state keyed by a string the client " +
			"chooses (invariant 13)")
	}
}

// The converse, or the test above would pass by sweeping everything: a
// budget a live connection is spending is never swept, however full it is.
func TestALiveClientsBudgetIsNeverSwept(t *testing.T) {
	b := testBroker(t)
	b.limits.PublishRate = 5

	now := time.Now()
	b.withinPublishRate("device", "device", 1)
	b.sweepAllowances(now.Add(time.Hour))
	if _, ok := b.allowances.Load("device"); !ok {
		t.Error("the budget of a client that had not disconnected was swept, so a " +
			"connected client publishing steadily is handed a new bucket by the sweep")
	}
}

// blockingLog is a channel log whose reads wait until they are released, as a
// read from a slow disk does.
type blockingLog struct {
	LogStore
	reading chan struct{}
	release chan struct{}
}

func (l *blockingLog) ReadFromN(from uint64, n int) ([]store.Record, error) {
	l.reading <- struct{}{}
	<-l.release
	return l.LogStore.ReadFromN(from, n)
}

// A drain reads the channel's store without holding the broker's lock, and a
// batch whose cursor moved while it read is read again rather than written.
//
// **Held across the read, the lock stalled everything else that takes it.**
// Measured in the link-churn soak on sqlite: a resuming client's SUBSCRIBE
// went unanswered for five seconds, and a goroutine dump at the timeout had
// its connection waiting for the lock behind drains reading channels for
// other consumers.
func TestADrainReadsTheStoreWithoutTheBrokerLock(t *testing.T) {
	b := testBroker(t)
	c := b.reg.Get("events")
	cl := &mqtt.Client{ID: "device"}
	b.subs[cl.ID] = []subscription{{filter: "events/#", channel: c, qos: 0}}
	b.reindex(cl.ID) // and with it the plan a batch is decided against
	lg := &blockingLog{LogStore: b.logs["events"], reading: make(chan struct{}), release: make(chan struct{})}
	b.logs["events"] = lg
	cur := b.cursor(cl.ID, c.Name, false)
	cur.pumping = true
	b.owner[cl.ID] = cl

	done := make(chan bool, 1)
	go func() { done <- b.pumpBatch(cl, c, cur, nil) }()
	select {
	case <-lg.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain never read the store, so this says nothing about the lock")
	}
	if !b.mu.TryLock() {
		close(lg.release)
		<-done
		t.Fatal("the drain holds the broker's lock while it reads the channel's store: " +
			"a slow read holds back every acknowledgement, subscription and re-send that needs it")
	}
	// Nor its own consumer's lock, which its acknowledgements need.
	if !cur.con.cmu.TryLock() {
		b.mu.Unlock()
		close(lg.release)
		<-done
		t.Fatal("the drain holds its consumer's lock while it reads the channel's store: " +
			"that consumer's acknowledgements wait for the read")
	}
	cur.con.cmu.Unlock()
	// Something moves the cursor while the read is out.
	moved := cur.next + 5
	cur.next = moved
	b.mu.Unlock()

	close(lg.release)
	select {
	case again := <-done:
		if !again {
			t.Error("a batch whose cursor moved during its read was not read again")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the drain did not finish once its read was released")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur.next != moved {
		t.Errorf("the cursor is at %d after a read taken from before it moved, want %d: "+
			"what was read for the old position was used", cur.next, moved)
	}
}

// RFC 0002 `broker.session`: when the broker starts it ends the sessions no
// client can have any more, with what they had on the wire - one whose
// expiry passed while it was stopped, counted, and one that ended with a
// connection the stop cut. A persistent session still inside its interval
// stays for its client, and so does one whose client was connected when the
// broker stopped.
func TestSessionsLeftByAStopAreEndedAtStart(t *testing.T) {
	b := testBroker(t)
	s := store.NewSessions()
	lg, _ := s.Log()
	rec, err := lg.Append(store.Record{Topic: "t", Payload: []byte("owed")})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, sess := range []store.Session{
		{Client: "expired", ExpiryInterval: 60, DisconnectedAt: now.Add(-2 * time.Minute)},
		{Client: "inside", ExpiryInterval: 600, DisconnectedAt: now.Add(-2 * time.Minute)},
		{Client: "connected", ExpiryInterval: 60},
		{Client: "ended", ExpiryInterval: 0},
	} {
		if err := s.Save(sess); err != nil {
			t.Fatal(err)
		}
		if err := s.SetInFlight(sess.Client, 10, store.InFlight{Offset: rec.Offset, PacketID: 1, QoS: 1,
			State: store.MessageSent}); err != nil {
			t.Fatal(err)
		}
	}
	b.SetSessions("mem", s)

	expired, ended, err := b.DropSessionsLeftByAStop(now)
	if err != nil {
		t.Fatal(err)
	}
	if expired != 1 || ended != 1 {
		t.Errorf("ended %d expired and %d that ended with their connection, want 1 and 1", expired, ended)
	}
	for _, id := range []string{"expired", "ended"} {
		if _, ok, _ := s.Get(id); ok {
			t.Errorf("session %s is still kept after the start", id)
		}
		if _, table, _ := s.InFlight(id); len(table) != 0 {
			t.Errorf("session %s still has %d messages in flight", id, len(table))
		}
	}
	for _, id := range []string{"inside", "connected"} {
		if _, ok, _ := s.Get(id); !ok {
			t.Errorf("session %s was ended at start, and a client can still have it", id)
		}
	}
	if got := b.counted.sessionsExpiredWhileStopped.Load(); got != 1 {
		t.Errorf("expired_while_stopped counts %d, want 1: only the expired session is a loss", got)
	}
}

// The session store is asked for on every delivery a session keeps, without
// the broker's lock: taken there, each of a resumed session's re-sends queued
// behind whatever held the lock, one re-send at a time.
func TestTheSessionStoreIsReadWithoutTheBrokerLock(t *testing.T) {
	b := testBroker(t)
	s := store.NewSessions()
	b.SetSessions("mem", s)
	b.mu.Lock()
	got := make(chan SessionStore, 1)
	go func() { got <- b.sessionStore() }()
	select {
	case r := <-got:
		b.mu.Unlock()
		// The one attached, behind the wrapper that counts its failures
		// (countingSessions).
		if w, ok := r.(countingSessions); !ok || w.SessionStore != s {
			t.Errorf("the session store read without the lock is a %T, not the one attached "+
				"behind its counting wrapper", r)
		}
	case <-time.After(2 * time.Second):
		b.mu.Unlock()
		<-got
		t.Fatal("reading the session store waits for the broker's lock")
	}
}

// RFC 0002 `broker.session`: a restored session's expiry is measured from the
// moment its client went away, not from the moment the broker started. A
// session does not get its interval back because it was restarted, or a
// broker restarting once an interval would keep every session for ever
// (invariant 13).
//
// The one exception is a session whose client was connected when the broker
// stopped: the store holds no moment for it, so it is away from this start.
func TestARestoredSessionExpiresFromWhenItsClientWentAway(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatal(err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)

	s := store.NewSessions()
	away := time.Now().Add(-90 * time.Second).Truncate(time.Second)
	for _, sess := range []store.Session{
		{Client: "went-away", ExpiryInterval: 600, DisconnectedAt: away,
			Subscriptions: []store.SessionSubscription{{Filter: "loose/#", QoS: 1}}},
		{Client: "was-connected", ExpiryInterval: 600},
	} {
		if err := s.Save(sess); err != nil {
			t.Fatal(err)
		}
	}
	b.SetSessions("mem", s)

	started := time.Now()
	sessions, err := b.RestoreSessions()
	if err != nil || sessions != 2 {
		t.Fatalf("restored %d sessions (%v), want 2", sessions, err)
	}

	cl, ok := srv.Clients.Get("went-away")
	if !ok {
		t.Fatal("the session was not restored")
	}
	if got := cl.StopTime(); got.Unix() != away.Unix() {
		t.Errorf("the restored session is away since %v, want %v: its expiry would run from "+
			"the restart rather than from when its client went", got, away)
	}
	back, ok := srv.Clients.Get("was-connected")
	if !ok {
		t.Fatal("a session whose client was connected at the stop was not restored")
	}
	if got := back.StopTime(); got.Before(started.Add(-time.Second)) || got.After(time.Now()) {
		t.Errorf("a session that was connected when the broker stopped is away since %v, "+
			"want this start", got)
	}
	// And it is a session, not a connection: nothing is connected to it, and
	// what it subscribed to is live.
	if !cl.Closed() {
		t.Error("the restored session reads as connected")
	}
	subs := cl.State.Subscriptions.GetAll()
	if len(subs) != 1 || subs["loose/#"].Qos != 1 {
		t.Errorf("the restored session holds %v, want its one subscription to loose/# at QoS 1", subs)
	}
	if n := len(srv.Topics.Subscribers("loose/x").Subscriptions); n != 1 {
		t.Errorf("%d subscribers to loose/x, want the restored session: a session restored "+
			"outside the substrate's index is sent nothing while it is away", n)
	}
}

// RFC 0002 `broker.session`: a session holding a subscription the rules this
// broker started with refuse is ended at the start, with what it had on the
// wire, and counted. The case it exists for is a channel that became a queue
// while the broker was stopped: a queue's records reach a worker and nobody
// else (invariant 4), so a session subscribed to its topics from before must
// not come back holding that subscription.
//
// The session goes rather than the subscription, because MQTT has no way to
// tell a resuming client that one of its subscriptions is gone: it comes back
// to Session Present 0 and is answered for each filter it asks for again.
func TestASessionHoldingARefusedSubscriptionIsEndedAtStart(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "jobs", Type: channel.Queue, Filter: "work/+", VisibilityTimeout: 30, MaxAttempts: 3},
		{Name: "events", Type: channel.Append, Filter: "iot/+/events/+"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	b := New(reg, slog.New(slog.NewTextHandler(&logged, nil)))
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)

	s := store.NewSessions()
	lg, _ := s.Log()
	rec, err := lg.Append(store.Record{Topic: "t", Payload: []byte("owed")})
	if err != nil {
		t.Fatal(err)
	}
	for _, sess := range []store.Session{
		// Subscribed to what is a queue's topics now. It was an ordinary
		// channel when this session subscribed, and the broker was stopped
		// and started on a configuration that made it a queue.
		{Client: "was-a-subscriber", ExpiryInterval: 600,
			Subscriptions: []store.SessionSubscription{{Filter: "work/+", QoS: 1}}},
		// And one holding nothing this broker refuses.
		{Client: "still-fine", ExpiryInterval: 600,
			Subscriptions: []store.SessionSubscription{{Filter: "iot/+/events/+", QoS: 1}}},
	} {
		if err := s.Save(sess); err != nil {
			t.Fatal(err)
		}
		if err := s.SetInFlight(sess.Client, 10, store.InFlight{Offset: rec.Offset, PacketID: 1, QoS: 1,
			State: store.MessageSent}); err != nil {
			t.Fatal(err)
		}
	}
	b.SetSessions("mem", s)

	sessions, err := b.RestoreSessions()
	if err != nil {
		t.Fatal(err)
	}
	if sessions != 1 {
		t.Errorf("restored %d sessions, want the one session this broker's rules admit", sessions)
	}
	if _, ok := srv.Clients.Get("was-a-subscriber"); ok {
		t.Error("the session holding a refused subscription was restored: it is subscribed to " +
			"a queue's topics, and a queue's records reach a worker and nobody else")
	}
	if _, ok := srv.Clients.Get("still-fine"); !ok {
		t.Error("the session this broker's rules admit was not restored")
	}
	if _, ok, _ := s.Get("was-a-subscriber"); ok {
		t.Error("the refused session is still in the store, so the next start would judge it again")
	}
	if _, table, _ := s.InFlight("was-a-subscriber"); len(table) != 0 {
		t.Errorf("%d messages of the ended session are still in flight", len(table))
	}
	if got := b.counted.sessionsRefusedAtStart.Load(); got != 1 {
		t.Errorf("subscription_refused counts %d, want 1", got)
	}
	if !strings.Contains(logged.String(), "it holds a subscription this broker refuses") {
		t.Errorf("nothing was logged about the session that was ended:\n%s", logged.String())
	}
}

// RFC 0003 "Exactly once" across a restart: the publish a session sent and
// never released comes back with that session, so the client finishes the
// exchange it started. One whose session did not come back cannot be
// finished by anybody and is dropped at the start, counted where every other
// abandoned exchange is counted.
//
// **The publisher is told nothing, because there is nobody to tell.** Its
// session is gone; what it does next is start again, and
// saguin_qos2_abandoned_total is the only place the loss appears.
func TestUnreleasedPublishesComeBackWithTheirSessionAndNoOther(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatal(err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)

	// Held in the channel's store, as a broker that stopped mid-exchange left
	// them: the start finds them there, and nowhere else.
	events := b.holdsFor("events")
	now := time.Now()
	for _, e := range []store.Exchange{
		{Client: "came-back", PacketID: 4},
		{Client: "came-back", PacketID: 9},
		{Client: "did-not", PacketID: 4},
	} {
		if err := events.Hold(e, store.Record{
			MessageID: fmt.Sprintf("m-%s-%d", e.Client, e.PacketID),
			Topic:     "events/x", Payload: []byte("half-done"), Timestamp: now}, now); err != nil {
			t.Fatal(err)
		}
	}
	b.SetQoS2(20, time.Hour)

	s := store.NewSessions()
	// Only one of the two sessions is still there to come back: the other
	// expired while the broker was stopped, or ended with its connection.
	if err := s.Save(store.Session{Client: "came-back", ExpiryInterval: 600,
		DisconnectedAt: now.Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	b.SetSessions("mem", s)

	if _, err := b.RestoreSessions(); err != nil {
		t.Fatal(err)
	}

	cl, ok := srv.Clients.Get("came-back")
	if !ok {
		t.Fatal("the session was not restored")
	}
	for _, id := range []uint16{4, 9} {
		held, ok := cl.State.Inflight.Get(id)
		if !ok {
			t.Errorf("the restored session holds nothing under identifier %d: the PUBREL its "+
				"client re-sends would be refused, and the message it acknowledged would be lost", id)
			continue
		}
		if held.FixedHeader.Type != packets.Pubrec {
			t.Errorf("identifier %d holds a %v, want the receipt the exchange is waiting on",
				id, held.FixedHeader.Type)
		}
	}
	if !b.storeHolds("events", store.Exchange{Client: "came-back", PacketID: 4}) {
		t.Error("the restored session's message was dropped from the store, so the release it " +
			"is waiting for would complete nothing")
	}
	if b.storeHolds("events", store.Exchange{Client: "did-not", PacketID: 4}) {
		t.Error("an exchange whose session did not come back is still held: nothing can ever " +
			"complete it, and it sits in the provider's bytes until it ages out")
	}
	if got := b.counted.qos2Abandoned.Load(); got != 1 {
		t.Errorf("qos2_abandoned counts %d, want the 1 exchange nobody can finish", got)
	}
}

// The third way a start ends a session: it holds a subscription this broker's
// rules now refuse. The session goes, and the Will it was holding is owed
// first - a device that died, waited out its delay and is now refused a
// subscription is still a device that died, and the refusal has nothing to do
// with the announcement.
//
// Here rather than from the wire, because what makes the session refusable is
// the broker's own configuration changing under it: a channel that became a
// queue while the broker was stopped.
func TestAStartPublishesTheWillOfASessionItRefuses(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "jobs", Type: channel.Queue, Filter: "work/+", VisibilityTimeout: 30, MaxAttempts: 3},
		{Name: "events", Type: channel.Append},
	})
	if err != nil {
		t.Fatal(err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)

	s := store.NewSessions()
	now := time.Now()
	if err := s.Save(store.Session{
		Client: "refused-with-a-will", ExpiryInterval: 600, DisconnectedAt: now.Add(-time.Minute),
		// Subscribed to what is a queue's topics now.
		Subscriptions: []store.SessionSubscription{{Filter: "work/+", QoS: 1}},
		Will: &store.SessionWill{
			Topic: "events/will/refused", Payload: []byte("i-died"), QoS: 1,
			Delay: 30, DueAt: now.Add(-30 * time.Second),
		},
	}); err != nil {
		t.Fatal(err)
	}
	b.SetSessions("mem", s)

	sessions, err := b.RestoreSessions()
	if err != nil {
		t.Fatal(err)
	}
	if sessions != 0 {
		t.Fatalf("restored %d sessions, want none: the one session here holds a refused "+
			"subscription", sessions)
	}
	if n := b.PublishDueWills(); n != 1 {
		t.Errorf("the start published %d Wills of the session it refused, want 1: the "+
			"announcement was dropped with the record", n)
	}
	if _, ok, _ := s.Get("refused-with-a-will"); ok {
		t.Error("the refused session is still in the store")
	}
	if got := b.counted.sessionsRefusedAtStart.Load(); got != 1 {
		t.Errorf("subscription_refused counts %d, want 1", got)
	}
}

// RFC 0003 "Last Will": a start that ends a session publishes its Will
// before its record goes - every way a start ends one. Its record is kept
// through the restore and dropped once the Will is published, as the running
// expiry does, so a start that stops in between - a session store it could
// not restore from - has lost nothing: the next start owes the Will again.
func TestAStartKeepsAnEndedSessionUntilItsWillIsPublished(t *testing.T) {
	now := time.Now()
	will := func(topic string) *store.SessionWill {
		return &store.SessionWill{Topic: topic, Payload: []byte("i-died"), QoS: 1,
			Delay: 30, DueAt: now.Add(-30 * time.Second)}
	}
	for _, tc := range []struct {
		name string
		sess store.Session
	}{
		{"expired while the broker was stopped", store.Session{Client: "dev", ExpiryInterval: 60,
			DisconnectedAt: now.Add(-2 * time.Minute), Will: will("events/will/dev")}},
		{"ended with a connection the stop cut", store.Session{Client: "dev", ExpiryInterval: 0,
			Will: will("events/will/dev")}},
		{"holding a subscription this broker refuses", store.Session{Client: "dev", ExpiryInterval: 600,
			DisconnectedAt: now.Add(-time.Minute),
			Subscriptions:  []store.SessionSubscription{{Filter: "work/+", QoS: 1}},
			Will:           will("events/will/dev")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, err := channel.NewRegistry([]*channel.Channel{
				{Name: "jobs", Type: channel.Queue, Filter: "work/+", VisibilityTimeout: 30, MaxAttempts: 3},
				{Name: "events", Type: channel.Append},
			})
			if err != nil {
				t.Fatal(err)
			}
			s := store.NewSessions()
			if err := s.Save(tc.sess); err != nil {
				t.Fatal(err)
			}
			// One start, as main runs it up to the publish: the store is
			// the only thing that outlives it.
			begin := func() *Broker {
				b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
				srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
				b.SetServer(srv)
				b.SetSessions("mem", s)
				if _, _, err := b.DropSessionsLeftByAStop(now); err != nil {
					t.Fatal(err)
				}
				if _, err := b.RestoreSessions(); err != nil {
					t.Fatal(err)
				}
				if _, ok := srv.Clients.Get("dev"); ok {
					t.Error("the session this start is ending was restored")
				}
				return b
			}

			// This start stops before it publishes.
			begin()
			if _, ok, _ := s.Get("dev"); !ok {
				t.Fatal("the record went before its Will was published: a start that stops " +
					"here has ended the session and lost the Will")
			}

			// The next one owes it again, publishes it, and then ends the
			// session.
			b := begin()
			if n := b.PublishDueWills(); n != 1 {
				t.Fatalf("the next start published %d Wills, want the one the stopped start owed", n)
			}
			if _, ok, _ := s.Get("dev"); ok {
				t.Error("the session is still in the store once its Will was published")
			}
			if n := b.PublishDueWills(); n != 0 {
				t.Errorf("a second pass published %d Wills, want none", n)
			}
		})
	}
}

// refusesOneDrop is a session store that refuses the next Drop, as a store
// failing at that moment would.
type refusesOneDrop struct {
	SessionStore
	armed bool
}

func (s *refusesOneDrop) Drop(id string, held []string) (store.Dropped, error) {
	if s.armed {
		s.armed = false
		return store.Dropped{}, errors.New("refused for the test")
	}
	return s.SessionStore.Drop(id, held)
}

// A Will is published once (RFC 0003 "Last Will"). A start publishes an
// ended session's Will and then drops its record; a drop the store refuses
// leaves the record holding the Will, which the next start would find owed
// and publish again. It is remembered, and written as the broker stops, so
// the next start ends a session with no Will on it - and keeps the
// session's own moment and expiry, so that ending is the one it was owed.
func TestAWillWhoseRecordTheStoreWouldNotDropIsPublishedOnce(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	away := now.Add(-2 * time.Minute)
	mem := store.NewSessions()
	if err := mem.Save(store.Session{Client: "dev", ExpiryInterval: 60, DisconnectedAt: away,
		Will: &store.SessionWill{Topic: "events/will/dev", Payload: []byte("i-died"), QoS: 1,
			Delay: 30, DueAt: away.Add(30 * time.Second)}}); err != nil {
		t.Fatal(err)
	}
	s := &refusesOneDrop{SessionStore: mem}
	begin := func() *Broker {
		b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		b.SetServer(mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}))
		b.SetSessions("mem", s)
		if _, _, err := b.DropSessionsLeftByAStop(now); err != nil {
			t.Fatal(err)
		}
		if _, err := b.RestoreSessions(); err != nil {
			t.Fatal(err)
		}
		return b
	}

	b := begin()
	s.armed = true
	if n := b.PublishDueWills(); n != 1 {
		t.Fatalf("the start published %d Wills, want the one owed", n)
	}
	if s.armed {
		t.Fatal("the store was never asked to drop the record, so this proves nothing")
	}
	rec, ok, _ := mem.Get("dev")
	if !ok || rec.Will == nil {
		t.Fatalf("after the refused drop the record is kept=%v with Will %+v, so this proves nothing", ok, rec.Will)
	}

	// The stop writes what was refused.
	b.retryUnwrittenDisconnects()
	rec, ok, _ = mem.Get("dev")
	if !ok || rec.Will != nil {
		t.Fatalf("after the stop the record is kept=%v with Will %+v, want kept with no Will", ok, rec.Will)
	}
	if !rec.DisconnectedAt.Equal(away) || rec.ExpiryInterval != 60 {
		t.Errorf("the stop's write left disconnected=%v expiry=%d, want the record's own %v and 60",
			rec.DisconnectedAt, rec.ExpiryInterval, away)
	}

	b = begin()
	if n := b.PublishDueWills(); n != 0 {
		t.Errorf("the next start published %d Wills, want none: the Will was published before", n)
	}
	if _, ok, _ := mem.Get("dev"); ok {
		t.Error("the next start did not end the expired session")
	}
}

// A Will whose moment passed while the broker was stopped is published by the
// start's own pass, not by a timer racing it.
//
// **The difference is the snapshot load.** A Will becomes a record in
// whatever channel claims its topic, and the channels are still the empty
// ones this broker built until their snapshots load - so a Will published in
// between is a record the load replaces. RestoreSessions therefore collects
// what is already owed and PublishDueWills publishes it afterwards, which is
// the order main.go runs. A timer armed for a moment in the past fires at
// once and lands in that window, which is why this is asserted here rather
// than from the wire: end to end the two are indistinguishable, and only one
// of them keeps the record.
func TestAWillAlreadyDueIsPublishedByTheStartRatherThanATimer(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatal(err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)

	s := store.NewSessions()
	now := time.Now()
	if err := s.Save(store.Session{
		Client: "gone", ExpiryInterval: 600, DisconnectedAt: now.Add(-time.Minute),
		Will: &store.SessionWill{
			Topic: "events/will/owed", Payload: []byte("i-died"), QoS: 1,
			Delay: 30, DueAt: now.Add(-30 * time.Second),
		},
	}); err != nil {
		t.Fatal(err)
	}
	b.SetSessions("mem", s)

	if _, err := b.RestoreSessions(); err != nil {
		t.Fatal(err)
	}
	// Nothing armed a timer for it: it is owed, not waiting.
	b.mu.Lock()
	waiting := len(b.pendingWills)
	b.mu.Unlock()
	if waiting != 0 {
		t.Errorf("the restart armed %d timer(s) for a Will that was already due: one fires "+
			"before the snapshots load, and the record it writes is replaced by them", waiting)
	}
	if n := b.PublishDueWills(); n != 1 {
		t.Fatalf("the start published %d Wills it found owed, want 1", n)
	}
	// Published once: the record no longer carries it.
	sess, ok, err := s.Get("gone")
	if err != nil || !ok {
		t.Fatalf("the session is gone: %v, %v", ok, err)
	}
	if sess.Will != nil {
		t.Errorf("the Will is still on the record after the start published it: %+v", sess.Will)
	}
	if n := b.PublishDueWills(); n != 0 {
		t.Errorf("a second pass found %d Wills owed, want none: the same death would be "+
			"announced at every start", n)
	}
}

// RFC 0003 "`latest`"
//
// Only the strictly superseded is taken back.
//
// **This is the half an end-to-end test cannot see.** The substrate re-sends
// a resumed session's packets before saguin is asked anything, so by the
// time the drop runs the client already holds the value either way -
// dropping one that is still current costs nothing a subscriber can observe
// in that moment. What it costs is later and quieter: saguin forgets a
// delivery the session is still going to acknowledge, so the mark never
// advances past it and the value is served again on every resume from then
// on. The rule has to be asserted where it is decided.
//
// Driven directly for the reason the two teardown rules above are: the state
// is built, and the question is only whether currency is what decides.
func TestOnlyASupersededLatestValueIsTakenBack(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "state", Type: channel.Latest, Filter: "state/#"},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	lt := store.NewLatest()
	b.mu.Lock()
	b.latest["state"] = lt
	b.mu.Unlock()

	// Two topics: one written once and still current, one written again
	// since the session was sent it.
	still, err := lt.Set(store.Record{Topic: "state/still", Payload: []byte("only")})
	if err != nil {
		t.Fatalf("set still: %v", err)
	}
	if _, err := lt.Set(store.Record{Topic: "state/stale", Payload: []byte("old")}); err != nil {
		t.Fatalf("set stale: %v", err)
	}
	newer, err := lt.Set(store.Record{Topic: "state/stale", Payload: []byte("new")})
	if err != nil {
		t.Fatalf("set newer: %v", err)
	}

	cl := &mqtt.Client{ID: "device"}
	cl.State.Inflight = mqtt.NewInflights()
	cl.Properties.Props.ReceiveMaximum = 8
	for _, p := range []struct {
		packetID uint16
		topic    string
		offset   uint64
	}{
		{1, "state/still", still.Offset},
		{2, "state/stale", newer.Offset - 1},
	} {
		cl.State.Inflight.Set(packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			PacketID:    p.packetID,
			TopicName:   p.topic,
			Payload:     []byte("x"),
		})
		b.mu.Lock()
		b.setInflightLocked("device", p.packetID, pending{
			conn: cl, channel: "state", offset: p.offset, latest: true})
		b.mu.Unlock()
	}

	b.dropSupersededLatest(cl)

	b.mu.Lock()
	_, keptStill := b.inflightLocked("device", 1)
	_, keptStale := b.inflightLocked("device", 2)
	b.mu.Unlock()

	if !keptStill {
		t.Error("the value that is still current was taken back: the session is " +
			"owed it, MQTT re-sends it, and forgetting it here means its " +
			"acknowledgement moves nothing and it is served again on every resume")
	}
	if keptStale {
		t.Error("the superseded value was kept: it holds a slot of the in-flight " +
			"window that nothing can free, and the current-state pass waits " +
			"behind it")
	}
	// **The quota is not touched, and it must not be.** This runs before the
	// substrate clones the table, and inherit resets both quotas from the
	// CONNECT's Receive Maximum - so a slot returned here would be returned
	// to a connection that is going away, and a slot *taken* here would be
	// taken from one. The packet leaves; the counters do not move.
	withheld := &mqtt.Client{ID: "withholder"}
	withheld.State.Inflight = mqtt.NewInflights()
	withheld.Properties.Props.ReceiveMaximum = 1
	withheld.State.Inflight.ResetSendQuota(1)
	withheld.State.Inflight.Set(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    9, TopicName: "state/stale", Payload: []byte("x"),
	})
	withheld.State.Inflight.DecreaseSendQuota() // what its write spent
	b.mu.Lock()
	b.setInflightLocked("withholder", 9, pending{
		conn: withheld, channel: "state", offset: newer.Offset - 1, latest: true})
	b.mu.Unlock()

	before := withheld.State.Inflight.SendQuota()
	b.dropSupersededLatest(withheld)
	if after := withheld.State.Inflight.SendQuota(); after != before {
		t.Errorf("the drop moved the send quota from %d to %d: it runs before the "+
			"successor's table is cloned and inherit resets the quotas, so moving "+
			"them here changes only the connection that is leaving", before, after)
	}
	if n := len(withheld.State.Inflight.GetAll(false)); n != 0 {
		t.Errorf("the superseded packet is still in the table (%d left), so the "+
			"clone would carry it to the successor", n)
	}

	// And the window actually opened, which is the whole point of the drop:
	// the stale packet left the session rather than only leaving saguin's
	// index. Length rather than an index, so a rule that removed everything
	// fails here instead of panicking.
	if n := len(cl.State.Inflight.GetAll(false)); n != 1 {
		t.Errorf("the session holds %d in-flight packets, want 1 - the value that "+
			"is still current and nothing else", n)
	}
}

// appendUpTo appends records to a channel until its next offset is next, so
// that a position a test stores there names an offset the log has reached: a
// position past next is refused as it is written (store.ErrPastNext), as a
// start refuses it.
func appendUpTo(t *testing.T, lg LogStore, next uint64) {
	t.Helper()
	for lg.Next() < next {
		if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-fill-", lg.Next()), Topic: "events/fill",
			Payload: []byte("x"), Timestamp: time.Now()}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
}

// RFC 0003 "A stored position may lag, and never leads": a connected
// consumer's position is stored every broker.session.ack_commit_interval,
// the interval a broadcast session's acknowledgements are stored on - one
// clock for a session's progress, whichever kind of channel it reads - and
// not on the queue's own 200ms tick, which is what stored it before.
func TestAPositionIsStoredOnTheAckCommitInterval(t *testing.T) {
	b := testBroker(t)
	appendUpTo(t, b.logs["events"], 4)
	cur := b.cursor("reader", "events", false)
	for _, off := range []uint64{1, 2, 3} {
		cur.outstanding[off] = true
		cur.next = off + 1
	}
	cur.expiresIn = time.Hour
	b.flushPositions()
	if got := storedPosition(t, b, "reader"); got != 1 {
		t.Fatalf("the starting position is %d, want 1, so this proves nothing", got)
	}

	b.SetAckCommitInterval(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go b.Run(ctx)
	t.Cleanup(func() { cancel(); <-b.stopped })

	con := b.lookupConsumer("reader")
	con.cmu.Lock()
	clear(cur.outstanding)
	cur.expiresIn = time.Hour
	con.cmu.Unlock()
	moved := time.Now()

	time.Sleep(500 * time.Millisecond)
	if got := storedPosition(t, b, "reader"); got != 1 {
		t.Fatalf("the position was stored as %d within %v of moving, under an interval of 1s: "+
			"it is still stored on another clock", got, time.Since(moved).Round(time.Millisecond))
	}
	for deadline := moved.Add(1500 * time.Millisecond); storedPosition(t, b, "reader") != 4; {
		if time.Now().After(deadline) {
			t.Fatalf("the position was not stored within 1.5s under an interval of 1s: still %d",
				storedPosition(t, b, "reader"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// leaseCounter counts what a queue store is asked to lease.
type leaseCounter struct {
	QueueStore
	mu     sync.Mutex
	leased []store.Held
}

func (q *leaseCounter) Lease(h store.Held, now time.Time, vis time.Duration) (store.Item, bool, error) {
	q.mu.Lock()
	q.leased = append(q.leased, h)
	q.mu.Unlock()
	return q.QueueStore.Lease(h, now, vis)
}

// **A late acknowledgement from a taken-over connection settles nothing of
// its successor's** (acksFor), at every exit of OnQosComplete. The engine
// retires an identifier on a taken-over connection and lets go of its
// handover lock before the hook runs; the takeover clones a table without
// it, and the successor registers a delivery of its own under the same
// identifier before the old connection's hook lands. Asked nothing, the hook
// took the successor's append record out of its cursor, advanced its latest
// mark, and leased its job. Each arm is the broker as it stands at that
// moment - an entry under the identifier, recorded on the successor - and
// the hook called for the predecessor, then for the successor as the control
// that the entry is live.
func TestALateAcknowledgementFromATakenOverConnectionSettlesNothingOfTheSuccessors(t *testing.T) {
	setup := func(t *testing.T) (*Broker, *mqtt.Client, *mqtt.Client) {
		t.Helper()
		reg, err := channel.NewRegistry([]*channel.Channel{
			{Name: "events", Type: channel.Append},
			{Name: "state", Type: channel.Latest},
			{Name: "jobs", Type: channel.Queue, VisibilityTimeout: 30, MaxAttempts: 3},
		})
		if err != nil {
			t.Fatalf("registry: %v", err)
		}
		b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		srv := mqtt.New(&mqtt.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		b.SetServer(srv)
		// The successor is the connection the engine registers under the id;
		// the predecessor is the one it took over, registered no longer.
		succ := srv.NewClient(nil, "test", "w", false)
		srv.Clients.Add(succ)
		pred := srv.NewClient(nil, "test", "w", false)
		return b, pred, succ
	}
	puback := func(id uint16) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: id}
	}

	t.Run("an append record in the successor's cursor", func(t *testing.T) {
		b, pred, succ := setup(t)
		b.mu.Lock()
		con := b.consumerLocked("w")
		b.mu.Unlock()
		con.cmu.Lock()
		cur := &cursor{next: 3, outstanding: map[uint64]bool{2: true}}
		con.cursors["events"] = cur
		con.inflight[1] = pending{conn: succ, channel: "events", offset: 2, gen: cur.gen}
		con.cmu.Unlock()

		b.OnQosComplete(pred, puback(1))
		con.cmu.Lock()
		_, still := con.inflight[1]
		outstanding := cur.outstanding[2]
		con.cmu.Unlock()
		if !still || !outstanding {
			t.Fatalf("the taken-over connection's late PUBACK settled the successor's record: in flight %v, "+
				"outstanding %v - the stored position would pass offset 2, which the successor never acknowledged",
				still, outstanding)
		}
		b.OnQosComplete(succ, puback(1))
		con.cmu.Lock()
		_, still = con.inflight[1]
		outstanding = cur.outstanding[2]
		con.cmu.Unlock()
		if still || outstanding {
			t.Fatal("the successor's own PUBACK did not settle its record, so the arm above proves nothing")
		}
	})

	// An entry recorded on no connection - one rebuilt at a start, as the
	// broadcast drain's are - is not anybody's to settle: the question falls
	// to which connection the engine registers now, as bdrain.takes asks it.
	// Taking it as settled by any connection let a late PUBACK from the one a
	// resumed session was then taken over from settle the successor's.
	t.Run("an entry recorded on no connection", func(t *testing.T) {
		b, pred, succ := setup(t)
		b.mu.Lock()
		con := b.consumerLocked("w")
		b.mu.Unlock()
		con.cmu.Lock()
		cur := &cursor{next: 3, outstanding: map[uint64]bool{2: true}}
		con.cursors["events"] = cur
		con.inflight[1] = pending{channel: "events", offset: 2, gen: cur.gen}
		con.cmu.Unlock()

		b.OnQosComplete(pred, puback(1))
		con.cmu.Lock()
		_, still := con.inflight[1]
		con.cmu.Unlock()
		if !still {
			t.Fatal("a late PUBACK from a connection the engine no longer registers settled an entry recorded on none")
		}
		b.OnQosComplete(succ, puback(1))
		con.cmu.Lock()
		_, still = con.inflight[1]
		con.cmu.Unlock()
		if still {
			t.Fatal("the registered connection's PUBACK did not settle an entry recorded on none")
		}
	})

	t.Run("a latest value's mark", func(t *testing.T) {
		b, pred, succ := setup(t)
		b.mu.Lock()
		con := b.consumerLocked("w")
		b.mu.Unlock()
		con.cmu.Lock()
		con.inflight[3] = pending{conn: succ, channel: "state", offset: 9, latest: true}
		con.cmu.Unlock()
		seen := func() uint64 {
			b.latestSeen.posMu.Lock()
			defer b.latestSeen.posMu.Unlock()
			return b.latestSeen.by["w"]["state"].seen
		}

		b.OnQosComplete(pred, puback(3))
		if got := seen(); got == 9 {
			t.Fatal("the taken-over connection's late PUBACK advanced the successor's latest mark to 9")
		}
		b.OnQosComplete(succ, puback(3))
		if got := seen(); got != 9 {
			t.Fatalf("the successor's own PUBACK left the mark at %d, want 9, so the arm above proves nothing", got)
		}
	})

	t.Run("a job the successor holds", func(t *testing.T) {
		b, pred, succ := setup(t)
		lc := &leaseCounter{QueueStore: b.queues["jobs"]}
		b.queues["jobs"] = lc
		if _, err := lc.Enqueue(store.Record{Topic: "jobs/a", MessageID: "m-1"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		offered, err := lc.Offer(1, time.Now())
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer: %v (%d records)", err, len(offered))
		}
		const packetID = 7
		b.mu.Lock()
		b.deliveries[offered[0].DeliveryID] = &delivery{channel: "jobs", offset: offered[0].Offset,
			epoch: offered[0].Epoch, holder: "w", conn: succ, packetID: packetID}
		b.byPacket[packetKey("w", packetID)] = offered[0].DeliveryID
		b.mu.Unlock()

		b.OnQosComplete(pred, puback(packetID))
		lc.mu.Lock()
		leased := len(lc.leased)
		lc.mu.Unlock()
		b.mu.Lock()
		_, kept := b.byPacket[packetKey("w", packetID)]
		b.mu.Unlock()
		if leased != 0 || !kept {
			t.Fatalf("the taken-over connection's late PUBACK leased the successor's job (%d leases, "+
				"its packet entry kept %v): the visibility clock ran on a job its worker had not acknowledged",
				leased, kept)
		}
		b.OnQosComplete(succ, puback(packetID))
		lc.mu.Lock()
		leased = len(lc.leased)
		lc.mu.Unlock()
		if leased != 1 {
			t.Fatalf("the successor's own PUBACK leased %d jobs, want 1, so the arm above proves nothing", leased)
		}
	})
}

// **A connection is hung up once**: the second hangUp for it answers false
// and starts nothing. Two hang-ups of one connection each wrote a DISCONNECT
// and each counted an ending, which is what hangUp's own comment measured as
// a defect.
func TestAConnectionIsHungUpOnce(t *testing.T) {
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	b := New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := mqtt.New(&mqtt.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	b.SetServer(srv)
	cl := srv.NewClient(nil, "test", "device-7", false)
	srv.Clients.Add(cl)

	if !b.hangUp(cl, packets.ErrAdministrativeAction) {
		t.Fatal("the first hang-up of a live connection answered false, so nothing below is checked")
	}
	if b.hangUp(cl, packets.ErrAdministrativeAction) {
		t.Error("a second hang-up of the same connection answered true: it would write a second DISCONNECT")
	}
	b.drains.Wait()
}
