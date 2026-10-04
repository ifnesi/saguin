package broker_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// A publish reaching the broadcast log, RFC 0003 "Broadcast": a QoS 1 or 2
// broadcast that a session outliving its connection wants is written once,
// to the log, and its publisher is answered after that write and nothing
// else; each session it is owed to is served from the log by its own drain.

// countsAppends is a broadcast log that counts its appends, and fails them
// while failing is set.
type countsAppends struct {
	drainReads
	appends atomic.Int32
	failing atomic.Bool
}

func (l *countsAppends) Append(r store.Record) (store.Record, error) {
	l.appends.Add(1)
	if l.failing.Load() {
		return store.Record{}, errors.New("the log is refusing writes")
	}
	return l.drainReads.Append(r)
}

// countsDrainWrites is a session store that counts what the drain writes to it.
type countsDrainWrites struct {
	drainSessions
	writes atomic.Int32
}

func (s *countsDrainWrites) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	s.writes.Add(1)
	return s.drainSessions.SetInFlightAll(client, window, fs)
}

func (s *countsDrainWrites) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	s.writes.Add(1)
	return s.drainSessions.Acknowledge(client, cursor, done)
}

// A publish to five sessions that outlive their connections, all away, is
// one write: one append to the log, and nothing written for any session.
// Each of the five is owed it. On both providers.
func TestAPublishToManyDurableSessionsIsOneWrite(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg, st := new(countsAppends), new(countsDrainWrites)
			attachDrainWith(t, h, func(l drainReads) drainReads {
				lg.drainReads = l
				return lg
			}, func(s drainSessions) drainSessions {
				st.drainSessions = s
				return st
			})
			var ids []string
			for i := range 5 {
				id := fmt.Sprintf("away-%d", i)
				c := connectRx(t, h, id, false, true, 10)
				c.Sub(t, "news/#", 1)
				c.Close()
				sessionGone(t, h, id)
				ids = append(ids, id)
			}
			pub := connect(t, h, "producer", true, false)
			lg.appends.Store(0)
			st.writes.Store(0)

			pub.Pub(t, "news/a", "once")

			if n := lg.appends.Load(); n != 1 {
				t.Errorf("the publish appended %d times, want once", n)
			}
			if n := st.writes.Load(); n != 0 {
				t.Errorf("the publish wrote %d times for the sessions it is owed to, want none", n)
			}
			for _, id := range ids {
				if owed, _ := owedOf(h, id); len(owed) != 1 {
					t.Errorf("%s owes %v, want the one message", id, owed)
				}
			}
		})
	}
}

// Invariant 16: a subscriber that stops reading its socket holds nothing of
// its publisher's. It never reads after subscribing, with a window and a
// bound far larger than what is published, so its drain's writes block on
// its socket; every publish is still acknowledged at once. That the drain was
// held is shown by its having written fewer than were published.
func TestAPublisherIsNotHeldByASubscriberThatStopsReading(t *testing.T) {
	const published, size = 300, 64 << 10
	capAt(t, 64<<20)
	h := start(t)
	deaf, _ := connectRaw(t, h, "deaf", 65535)
	deaf.subscribeRaw("news/#", 1)

	pub := connect(t, h, "producer", true, false)
	body := strings.Repeat("x", size)
	began := time.Now()
	for i := range published {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := pub.C.Publish(ctx, &paho.Publish{Topic: "news/a", QoS: 1, Payload: []byte(body)})
		cancel()
		if err != nil {
			t.Fatalf("publish %d was not acknowledged within 2s (%v): the publisher waited on the "+
				"subscriber that stopped reading", i, err)
		}
	}
	took := time.Since(began)
	written := len(h.B.FlyingBroadcast("deaf"))
	t.Logf("%d publishes of %d bytes acknowledged in %s; the drain wrote %d before the socket held it",
		published, size, took.Round(time.Millisecond), written)
	if written >= published {
		t.Fatalf("the drain wrote all %d, so the socket never held it and this proves nothing", written)
	}
}

// Invariant 18: a publish the log cannot write is refused rather than
// acknowledged: a publisher told its message was accepted is told it is
// kept. Nothing of it is owed to anyone. The code is 0x83, RFC 0002's for
// "the channel's storage, or the broadcast log's, could not keep the
// record".
func TestAPublishTheLogCannotKeepIsRefused(t *testing.T) {
	h := start(t)
	lg := new(countsAppends)
	log := attachDrainWith(t, h, func(l drainReads) drainReads {
		lg.drainReads = l
		return lg
	}, func(s drainSessions) drainSessions { return s })
	c := connectRx(t, h, "dev", false, true, 10)
	c.Sub(t, "news/#", 1)

	lg.failing.Store(true)
	pub := connect(t, h, "producer", true, false)
	pa, err := pub.C.Publish(context.Background(), &paho.Publish{Topic: "news/a", QoS: 1, Payload: []byte("lost")})
	if pa == nil || pa.ReasonCode != packets.ErrImplementationSpecificError.Code {
		t.Fatalf("the publish the log could not write was answered %+v (%v), want 0x83", pa, err)
	}
	if n := lg.appends.Load(); n != 1 {
		t.Fatalf("the log was asked %d times, so this proves nothing", n)
	}
	if owed, _ := owedOf(h, "dev"); len(owed) != 0 || log.Next() != 1 {
		t.Errorf("dev owes %v and the log's next offset is %d, want nothing owed and nothing kept",
			owed, log.Next())
	}
	if waitUntil(300*time.Millisecond, func() bool { return c.Count() > 0 }) {
		t.Errorf("the subscriber was sent %v", c.All())
	}
}

// RFC 0002's full-store table: a publish the full log has no room for, and
// nothing in it to give up, is still accepted, and each session it was for
// loses it, counted as storage_full.
func TestAPublishAFullLogCannotKeepIsAcknowledged(t *testing.T) {
	h, _, _ := boundedDrain(t, "memory", 4096+sessionCharge("away", "news/#"))
	ops := operationsAt(t, h)
	away(t, h, "away", "news/#")
	before := scrapeGauges(t, ops)[storageFull]

	pub := connect(t, h, "producer", true, false)
	pa, err := pub.C.Publish(context.Background(), &paho.Publish{
		Topic: "news/a", QoS: 1, Payload: []byte(strings.Repeat("x", 8192))})
	if err != nil || pa == nil || pa.ReasonCode != 0 {
		t.Fatalf("a publish the full log could not keep was answered %+v (%v), want 0x00", pa, err)
	}
	if got := scrapeGauges(t, ops)[storageFull] - before; got != 1 {
		t.Errorf("%s moved by %v, want 1: the one session it was for", storageFull, got)
	}
	if owed, _ := owedOf(h, "away"); len(owed) != 0 {
		t.Errorf("away owes %v, want nothing", owed)
	}
}

// Every append is counted - even one the selection finds nobody for - since a
// cursor never passes an offset nobody has counted. A session subscribed with
// No Local publishes on a topic only it holds, which is appended and owed to
// nobody, between messages another session is owed; once that session has
// acknowledged everything its cursor is at the log's next offset.
func TestEveryPublishedMessageIsCounted(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			self := connectRx(t, h, "self", false, false, 10)
			if _, err := self.C.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{{Topic: "self/#", QoS: 1, NoLocal: true}}}); err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			other := connectRx(t, h, "other", false, false, 10)
			other.Sub(t, "news/#", 1)
			pub := connect(t, h, "producer", true, false)

			before := lg.Next()
			pub.Pub(t, "news/a", "1")
			self.Pub(t, "self/x", "to nobody")
			self.Pub(t, "self/x", "to nobody again")
			pub.Pub(t, "news/a", "2")
			if appended := lg.Next() - before; appended != 4 {
				t.Fatalf("%d appended, want all four: two owed to nobody", appended)
			}
			awaitCount(t, other, 2, 5*time.Second)
			if self.Count() != 0 {
				t.Errorf("self was sent its own publishes: %v", self.All())
			}
			eventually(t, "other's cursor at the log's next offset", 5*time.Second, func() bool {
				pos, ok, _ := lg.Position(store.MQTTReader("other"))
				return ok && pos.Offset == lg.Next()
			})
		})
	}
}

// RFC 0003 "Broadcast": QoS 0 is served from memory alone. A session
// subscribed at QoS 0 is sent a QoS 1 publish live at QoS 0, and nothing is
// written to the log for it.
func TestAQoS0SubscriptionIsServedLiveNotFromTheLog(t *testing.T) {
	h := start(t)
	lg := attachDrain(t, h)
	c := connectRx(t, h, "live", false, false, 10)
	c.Sub(t, "news/#", 0)
	before := lg.Next()
	connect(t, h, "producer", true, false).Pub(t, "news/a", "live")
	awaitCount(t, c, 1, 5*time.Second)
	if got := c.All()[0]; got.QoS != 0 || got.Payload != "live" {
		t.Errorf("received %q at QoS %d, want it at QoS 0", got.Payload, got.QoS)
	}
	if lg.Next() != before {
		t.Errorf("the log's next offset moved from %d to %d: a publish only a QoS 0 session wanted was written",
			before, lg.Next())
	}

	// Beside a session that is owed it from the log, one subscribed at QoS 0
	// and away is owed nothing of it: QoS 0 is nobody's once the connection
	// is gone.
	away(t, h, "away-at-0", "news/#")
	if _, err := connectRx(t, h, "away-at-0", false, false, 10).C.Subscribe(context.Background(),
		&paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "news/#", QoS: 0}}}); err != nil {
		t.Fatalf("subscribe at QoS 0: %v", err)
	}
	cl0, _ := h.Srv.Clients.Get("away-at-0")
	_ = cl0.Net.Conn.Close()
	sessionGone(t, h, "away-at-0")
	logged := connectRx(t, h, "logged", false, true, 10)
	logged.Sub(t, "news/#", 1)
	connect(t, h, "producer-2", true, false).Pub(t, "news/b", "logged")
	awaitCount(t, logged, 1, 5*time.Second)
	if owed, _ := owedOf(h, "away-at-0"); len(owed) != 0 {
		t.Errorf("the session subscribed at QoS 0 and away is owed %v from the log", owed)
	}
}

// A session that outlives its connection and is made while the broker runs
// is held by the drain from the moment it is established, so the next
// broadcast it subscribes to is owed to it from the log: in its in-flight
// table on the log while it is on the wire.
func TestASessionMadeWhileTheBrokerRunsIsOwedFromTheLog(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			c := connectRx(t, h, "new", false, true, 10)
			c.Sub(t, "news/#", 1)
			connect(t, h, "producer", true, false).Pub(t, "news/a", "from the log")
			awaitCount(t, c, 1, 5*time.Second)
			table := tableOf(t, "new")
			if len(table) != 1 {
				t.Fatalf("the session's in-flight table on the log holds %+v, want the one delivery", table)
			}
			if owed, _ := owedOf(h, "new"); !slices.Equal(owed, []uint64{table[0].Offset}) {
				t.Errorf("the session owes %v, want the delivery at %d", owed, table[0].Offset)
			}
			c.All()[0].Ack()
			eventually(t, "the delivery acknowledged", 5*time.Second, func() bool {
				return len(tableOf(t, "new")) == 0
			})
		})
	}
}

// A session that ends lets go of what it was owed: a message only it was owed
// leaves the log. Its client comes back with Clean Start 1, which ends it.
func TestAnEndedSessionsMessagesLeaveTheLog(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			away(t, h, "gone", "news/#")
			connect(t, h, "producer", true, false).Pub(t, "news/a", "owed, then not")
			owed, _ := owedOf(h, "gone")
			if len(owed) != 1 {
				t.Fatalf("the session owes %v, want the one message", owed)
			}
			connectRx(t, h, "gone", true, false, 10)
			eventually(t, "the ended session's message gone from the log", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(owed[0])
				return len(held) == 0
			})
			if got, _ := owedOf(h, "gone"); len(got) != 0 {
				t.Errorf("the new session owes %v, want nothing of the ended one's", got)
			}
		})
	}
}

// MQTT-3.3.4-9: a client may take its whole Receive Maximum before it
// acknowledges anything. A session owed more than one batch while away is
// sent all of it once it is back, with no acknowledgement to call the drain
// back between batches.
func TestADrainFillsTheWindowWithoutAcknowledgements(t *testing.T) {
	const owed = 600
	// Half the bound is what may be on the wire, each delivery its message and
	// the in-flight PUBLISH held for it: under the default bound that is
	// about 430 of these, and the window is what this is about.
	capAt(t, 2<<20)
	h, lg, _ := drainHarness(t)
	c, _ := connectRaw(t, h, "slow", 10)
	c.subscribeRaw("news/#", 1)
	_ = c.conn.Close()
	sessionGone(t, h, "slow")
	for i := range owed {
		owe(t, h, lg, news("a", fmt.Sprint(i)), "slow")
	}
	back, _ := connectRaw(t, h, "slow", 1000)
	for i := range owed {
		if pk := back.read(); string(pk.Payload) != fmt.Sprint(i) {
			t.Fatalf("delivery %d was %q, want %q", i, pk.Payload, fmt.Sprint(i))
		}
	}
}

// RFC 0003 "Broadcast" and RFC 0005 saguin_deliveries_expired_total: a
// message whose Message Expiry Interval runs out while it waits for room in
// a session's window is skipped when the session reaches it, never sent, and
// counted as expired exactly once; the session goes on to what follows it.
func TestAMessageThatExpiresWhileItWaitsIsSkippedAndCountedOnce(t *testing.T) {
	h, lg, _ := drainHarness(t)
	ops := operationsAt(t, h)
	const series = "saguin_deliveries_expired_total"
	raw, _ := connectRaw(t, h, "raw", 1)
	raw.subscribeRaw("news/#", 1)
	before := metricValue(t, ops, series)

	owe(t, h, lg, news("a", "holds the window"), "raw")
	first := raw.read()
	expiring := news("a", "expires waiting")
	expiring.MessageExpiry, expiring.Timestamp = 1, time.Now()
	gone := owe(t, h, lg, expiring, "raw")
	after := owe(t, h, lg, news("a", "after it"), "raw")
	time.Sleep(1200 * time.Millisecond)

	raw.ack(first)
	if pk := raw.read(); string(pk.Payload) != string(after.Payload) {
		t.Fatalf("once the window had room the broker sent %q, want %q: the expired one skipped",
			pk.Payload, after.Payload)
	}
	awaitSeries(t, ops, series, before+1)
	// Within ack_commit_interval rather than at once: released messages are
	// removed together (bdrain.release), and RFC 0004 promises only that one
	// nobody owes leaves.
	eventually(t, "the expired message gone from the log, nobody else being owed it", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(gone.Offset)
		return len(held) == 0
	})
}
