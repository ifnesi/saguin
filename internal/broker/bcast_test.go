package broker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// The broadcast drain, fed a log directly: nothing publishes to it until the
// switch, so these tests append to the session provider's broadcast log and
// count each message for the sessions it is owed to, as the switch will. The
// clients are real, and every delivery and acknowledgement is on the wire.
// Oracles are RFC 0003 "Broadcast" and RFC 0002's
// limits.session_queue_bytes.

// drainHarness is a broker with its session provider's broadcast log
// attached to the drain, and the log and the store that owns it.
func drainHarness(t *testing.T) (*brokertest.Harness, *store.Log, *store.Sessions) {
	t.Helper()
	h := start(t)
	s, ok := brokertest.HarnessSessions.(*store.Sessions)
	if !ok {
		t.Fatalf("the harness's session store is a %T, not a memory store", brokertest.HarnessSessions)
	}
	lg, err := s.Log()
	if err != nil {
		t.Fatal(err)
	}
	return h, lg, s
}

// drainLog is what these tests ask of either provider's broadcast log.
type drainLog interface {
	Append(store.Record) (store.Record, error)
	ReadAt(offsets ...uint64) ([]store.Record, error)
	Remove(offsets ...uint64) (int, int64, error)
	Position(reader string) (store.Position, bool, error)
	Next() uint64
}

// owe appends a message to the log and counts it for the sessions given,
// answering it with its offset.
func owe(t *testing.T, h *brokertest.Harness, lg drainLog, r store.Record, to ...string) store.Record {
	t.Helper()
	if r.MessageID == "" {
		r.MessageID = store.NewDeliveryID()
	}
	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}
	got, err := lg.Append(r)
	if err != nil {
		t.Fatalf("append %q: %v", r.Payload, err)
	}
	h.B.CountBroadcast(got, to...)
	return got
}

// news is a QoS 1 message on news/<topic>.
func news(topic, payload string) store.Record {
	return store.Record{Topic: "news/" + topic, Payload: []byte(payload), QoS: 1}
}

// eventually waits for a condition, and fails naming it.
func eventually(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("%s: not within %v", what, within)
}

// A durable session on the log: attached, connected and subscribed.
func durable(t *testing.T, h *brokertest.Harness, id string, manualAck bool, rx uint16, filter string) *brokertest.Client {
	t.Helper()
	c := connectRx(t, h, id, false, manualAck, rx)
	c.Sub(t, filter, 1)
	h.B.AttachBroadcast(id)
	return c
}

// RFC 0003 "Broadcast": a session is sent what it is owed from the log, in the
// order the log holds it, each message as it was published - its properties
// and its User Properties, and nothing of saguin's added - and once every
// message is acknowledged, the log holds nothing it owes, its cursor is at
// the log's next offset, and its in-flight table is empty.
func TestADurableSessionIsSentWhatItIsOwedFromTheLog(t *testing.T) {
	h, lg, s := drainHarness(t)
	c := durable(t, h, "dev", false, 5, "news/#")

	const n = 20
	for i := range n {
		r := news("a", fmt.Sprint("m-", i))
		if i == 7 {
			r.ContentType, r.ResponseTopic, r.CorrelationData = "text/plain", "replies/dev", []byte{1, 2, 3}
			r.PayloadFormat, r.PayloadFormatFlag = 1, true
			r.Headers = []store.Header{{Key: "k", Value: "v"}}
		}
		owe(t, h, lg, r, "dev")
	}
	awaitCount(t, c, n, 5*time.Second)
	got := c.All()
	if len(got) != n {
		t.Fatalf("received %d of %d: %v", len(got), n, c.Payloads())
	}
	for i, m := range got {
		if m.Payload != fmt.Sprint("m-", i) || m.Topic != "news/a" || m.QoS != 1 {
			t.Errorf("delivery %d is %q on %q at QoS %d, want %q on news/a at QoS 1", i, m.Payload, m.Topic, m.QoS, fmt.Sprint("m-", i))
		}
	}
	p := got[7]
	if p.ContentType != "text/plain" || p.RespTopic != "replies/dev" || string(p.CorrData) != "\x01\x02\x03" ||
		p.PayloadFormat != "1" || len(p.User) != 1 || p.User["k"] != "v" {
		t.Errorf("the message with properties arrived as %+v, want them as published and no others", p)
	}

	eventually(t, "every message acknowledged and let go of", 5*time.Second, func() bool {
		return lg.Len() == 0
	})
	eventually(t, "the cursor at the log's next offset", 5*time.Second, func() bool {
		pos, ok, _ := lg.Position(store.MQTTReader("dev"))
		return ok && pos.Offset == lg.Next()
	})
	if _, table, _ := s.InFlight("dev"); len(table) != 0 {
		t.Errorf("the in-flight table still holds %+v", table)
	}
	if owed, bytes := h.B.OwedBroadcast("dev"); len(owed) != 0 || bytes != 0 {
		t.Errorf("still owed %v (%d bytes)", owed, bytes)
	}
}

// RFC 0003 "Broadcast": a session reads the log at its own pace, within its
// client's Receive Maximum, so a slow session lags its own cursor and
// nobody else's. One that acknowledges nothing is sent exactly its window
// and holds its messages in the log; another on the same log is sent every
// message meanwhile, and its own cursor passes them all.
func TestASlowSessionLagsOnlyItsOwnCursor(t *testing.T) {
	h, lg, s := drainHarness(t)
	slow := durable(t, h, "slow", true, 2, "news/#")
	fast := durable(t, h, "fast", false, 10, "news/#")

	const n = 30
	for i := range n {
		owe(t, h, lg, news("a", fmt.Sprint("m-", i)), "slow", "fast")
	}
	awaitCount(t, fast, n, 5*time.Second)
	if got := fast.Count(); got != n {
		t.Fatalf("the fast session received %d of %d while the slow one held its window", got, n)
	}
	eventually(t, "the fast session's cursor at the log's next offset", 5*time.Second, func() bool {
		pos, ok, _ := lg.Position(store.MQTTReader("fast"))
		return ok && pos.Offset == lg.Next()
	})
	// The slow one's window, and not a message more.
	awaitCount(t, slow, 3, 300*time.Millisecond)
	if got := slow.Count(); got != 2 {
		t.Fatalf("the slow session was sent %d with a Receive Maximum of 2 and nothing acknowledged", got)
	}
	if got := lg.Len(); got != n {
		t.Errorf("the log holds %d messages while the slow session owes all %d", got, n)
	}
	// What is on the wire is in its in-flight table, recorded before it was
	// written (RFC 0003 "Broadcast"): the two it was sent, at QoS 1.
	if window, table, err := s.InFlight("slow"); err != nil || window != 2 || len(table) != 2 ||
		table[0].Offset != 1 || table[1].Offset != 2 || table[0].QoS != 1 || table[0].PacketID == table[1].PacketID {
		t.Errorf("the slow session's in-flight table is %+v under a window of %d (%v), want offsets 1 and 2 "+
			"at QoS 1 under two identifiers and a window of 2", table, window, err)
	}

	// And the slow one, acknowledging as it goes, is sent the rest in order.
	for acked := 0; acked < n; {
		all := slow.All()
		for _, m := range all[acked:] {
			m.Ack()
		}
		acked = len(all)
		if acked < n {
			awaitCount(t, slow, acked+1, 5*time.Second)
			if slow.Count() == acked {
				t.Fatalf("the slow session stopped at %d of %d after acknowledging", acked, n)
			}
		}
	}
	for i, m := range slow.All() {
		if m.Payload != fmt.Sprint("m-", i) {
			t.Fatalf("the slow session's delivery %d is %q", i, m.Payload)
		}
	}
	eventually(t, "the log emptied once the slow session had acknowledged", 5*time.Second, func() bool {
		return lg.Len() == 0
	})
}

// RFC 0003 "Broadcast": a message leaves the log once every session that
// wants it has had it - and not before. Counted for nobody, it leaves at once.
func TestAMessageLeavesTheLogWithItsLastOwner(t *testing.T) {
	h, lg, _ := drainHarness(t)
	a := durable(t, h, "a", true, 10, "news/#")
	b := durable(t, h, "b", true, 10, "news/#")

	m := owe(t, h, lg, news("a", "both"), "a", "b")
	awaitCount(t, a, 1, 5*time.Second)
	awaitCount(t, b, 1, 5*time.Second)
	if got := h.B.BroadcastOwners(m.Offset); got != 2 {
		t.Fatalf("the message has %d owners, want 2", got)
	}
	a.All()[0].Ack()
	eventually(t, "a's acknowledgement counted", 5*time.Second, func() bool {
		return h.B.BroadcastOwners(m.Offset) == 1
	})
	if held, _ := lg.ReadAt(m.Offset); len(held) != 1 {
		t.Fatal("the message left the log while b still owed it")
	}
	b.All()[0].Ack()
	eventually(t, "the message gone with its last owner", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(m.Offset)
		return len(held) == 0
	})

	nobody := owe(t, h, lg, news("a", "nobody"))
	eventually(t, "a message owed to nobody gone", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(nobody.Offset)
		return len(held) == 0
	})
}

// RFC 0003 "Broadcast": a message whose Message Expiry Interval has run out
// when a session reaches it is skipped and counted as expired, and stays in
// the log only while another session still has to pass it (invariant 2). One
// still in time is sent with what is left of its interval.
func TestAnExpiredBroadcastIsSkippedAndLetGo(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := durable(t, h, "dev", false, 10, "news/#")

	gone := news("a", "stale")
	gone.MessageExpiry, gone.Timestamp = 1, time.Now().Add(-10*time.Second)
	fresh := news("a", "fresh")
	fresh.MessageExpiry = 3600
	stale := owe(t, h, lg, gone, "dev")
	owe(t, h, lg, fresh, "dev")

	awaitCount(t, c, 1, 5*time.Second)
	awaitCount(t, c, 2, 200*time.Millisecond)
	if got := c.Payloads(); len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("received %v, want only the message still in time", got)
	}
	if e := c.All()[0].Expiry; e == nil || *e > 3600 || *e < 3590 {
		t.Errorf("the message still in time arrived with expiry %v, want what is left of 3600", e)
	}
	if line := awaitScrape(t, h, "saguin_deliveries_expired_total", 5*time.Second); line != "saguin_deliveries_expired_total 1" {
		t.Errorf("the scrape says %q, want one expired", line)
	}
	eventually(t, "both let go of", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(stale.Offset)
		return len(held) == 0 && lg.Len() == 0
	})
}

// RFC 0003 "Broadcast": what a delivery carries is what the session's
// subscriptions ask of it now - its Subscription Identifier, the lower of
// the publish's QoS and the one granted, and RETAIN only where Retain As
// Published was asked for. A delivery the subscription takes at QoS 0 is let
// go of as it is written, and one no subscription reaches any more is let go
// of unsent.
func TestADrainServesEachDeliveryWhatItsSubscriptionsAsk(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := connectRx(t, h, "dev", false, false, 10)
	c.SubWithID(t, "news/#", 1, 7)
	c.Sub(t, "zero/#", 0)
	h.B.AttachBroadcast("dev")

	kept := news("a", "kept")
	kept.Retain = true
	owe(t, h, lg, kept, "dev")
	owe(t, h, lg, store.Record{Topic: "zero/a", Payload: []byte("at zero"), QoS: 1}, "dev")
	orphan := owe(t, h, lg, store.Record{Topic: "gone/a", Payload: []byte("no subscription"), QoS: 1}, "dev")

	awaitCount(t, c, 2, 5*time.Second)
	awaitCount(t, c, 3, 200*time.Millisecond)
	byPayload := map[string]brokertest.Received{}
	for _, m := range c.All() {
		byPayload[m.Payload] = m
	}
	if len(byPayload) != 2 {
		t.Fatalf("received %v, want the two its subscriptions reach", c.Payloads())
	}
	if m := byPayload["kept"]; m.QoS != 1 || m.Retain || len(m.SubIDs) != 1 || m.SubIDs[0] != 7 {
		t.Errorf("news/a arrived at QoS %d, RETAIN %v, identifiers %v; want 1, false (no Retain As Published) and [7]",
			m.QoS, m.Retain, m.SubIDs)
	}
	if m := byPayload["at zero"]; m.QoS != 0 {
		t.Errorf("a delivery its subscription takes at QoS 0 arrived at %d", m.QoS)
	}
	eventually(t, "all three let go of", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(orphan.Offset)
		return len(held) == 0 && lg.Len() == 0
	})
}

// RFC 0003 "Broadcast": where a session's overlapping subscriptions disagree
// about Retain As Published, its delivery carries RETAIN if any of them asks
// for it - whichever order the subscriptions are held in.
func TestRetainAsPublishedHoldsWhereAnyOverlappingSubscriptionAsksForIt(t *testing.T) {
	for _, c := range []struct {
		name       string
		wide, near bool // Retain As Published on news/# and on news/+
		want       bool
	}{
		{"the wider asks", true, false, true},
		{"the nearer asks", false, true, true},
		{"neither asks", false, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, lg, _ := drainHarness(t)
			cl := connectRx(t, h, "dev", false, false, 10)
			if _, err := cl.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
				{Topic: "news/#", QoS: 1, RetainAsPublished: c.wide},
				{Topic: "news/+", QoS: 1, RetainAsPublished: c.near},
			}}); err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			h.B.AttachBroadcast("dev")
			r := news("a", "retained")
			r.Retain = true
			owe(t, h, lg, r, "dev")
			awaitCount(t, cl, 1, 5*time.Second)
			awaitCount(t, cl, 2, 200*time.Millisecond)
			if got := cl.All(); len(got) != 1 || got[0].Retain != c.want {
				t.Fatalf("received %+v, want one delivery with RETAIN %v", got, c.want)
			}
		})
	}
}

// RFC 0002 limits.session_queue_bytes: what a session is owed counts against
// its bound as each message's size and its entry on the session's list, and
// the one on the wire the in-flight PUBLISH held for it besides. The count
// moves by exactly that as a message is counted, sent, acknowledged, skipped
// as expired, and when the session ends.
func TestWhatASessionIsOwedCountsEachMessageAndItsEntry(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := durable(t, h, "dev", true, 1, "news/#")
	// Owed the last message too, and holding it: sent and never acknowledged.
	durable(t, h, "other", true, 1, "news/#")

	charge := func(r store.Record) int64 { return store.RecordSize(r) + broker.OwedEntryCost }
	bytes := func() int64 {
		_, b := h.B.OwedBroadcast("dev")
		return b
	}

	first := owe(t, h, lg, news("a", "first, and a little longer"), "dev")
	second := owe(t, h, lg, news("b", "second"), "dev")
	expiring := news("c", "expiring")
	expiring.MessageExpiry, expiring.Timestamp = 2, time.Now().Add(-1500*time.Millisecond)
	third := owe(t, h, lg, expiring, "dev")
	shared := owe(t, h, lg, news("d", "shared"), "dev", "other")
	// The window of one holds the first, on the wire.
	awaitCount(t, c, 1, 5*time.Second)
	eventually(t, "four messages owed, the first on the wire", 5*time.Second, func() bool {
		return bytes() == charge(first)+broker.WireEntryCost+broker.WireTableCost+charge(second)+charge(third)+charge(shared)
	})

	// Its acknowledgement gives back its charge, and the second goes out.
	c.All()[0].Ack()
	eventually(t, "the first acknowledged", 5*time.Second, func() bool {
		return bytes() == charge(second)+broker.WireEntryCost+broker.WireTableCost+charge(third)+charge(shared)
	})
	awaitCount(t, c, 2, 5*time.Second)

	// The third expires while the second waits; acknowledging the second
	// lets the drain reach it, skip it, and send the fourth.
	time.Sleep(600 * time.Millisecond)
	c.All()[1].Ack()
	eventually(t, "the second acknowledged and the third skipped", 5*time.Second, func() bool {
		return bytes() == charge(shared)+broker.WireEntryCost+broker.WireTableCost
	})
	awaitCount(t, c, 3, 5*time.Second)
	if got := c.All()[2].Payload; got != "shared" {
		t.Fatalf("after the expired one the session was sent %q", got)
	}

	// Its ending gives back the rest, and the message another session owes
	// stays in the log for it. And it forgets where the log was when its
	// subscriptions were made, so what that holds is bounded by the sessions
	// the drain holds (invariant 13).
	if h.B.BroadcastSince("dev") == nil {
		t.Fatal("the drain holds no subscription places for the session, so its ending forgets nothing")
	}
	h.B.EndBroadcast("dev")
	if got := h.B.BroadcastSince("dev"); got != nil {
		t.Errorf("an ended session's subscription places are still held: %v", got)
	}
	if owed, b := h.B.OwedBroadcast("dev"); len(owed) != 0 || b != 0 {
		t.Fatalf("an ended session still counts %v (%d bytes)", owed, b)
	}
	if got := h.B.BroadcastOwners(shared.Offset); got != 1 {
		t.Errorf("the shared message has %d owners after one ended, want 1", got)
	}
	if held, _ := lg.ReadAt(shared.Offset); len(held) != 1 {
		t.Error("a message another session owes left the log when this one ended")
	}
	eventually(t, "what only the ended session owed gone", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(first.Offset, second.Offset, third.Offset)
		return len(held) == 0
	})
}

// RFC 0003 "Broadcast": a session is owed only what its subscriptions matched
// when it was published. A message counted for another session before this
// one subscribed is never sent to it, though its filter matches it now.
func TestAMessageIsOwedOnlyToTheSessionsItWasCountedFor(t *testing.T) {
	h, lg, _ := drainHarness(t)
	early := durable(t, h, "early", false, 10, "news/#")
	owe(t, h, lg, news("a", "before"), "early")
	awaitCount(t, early, 1, 5*time.Second)

	late := durable(t, h, "late", false, 10, "news/#")
	owe(t, h, lg, news("a", "after"), "early", "late")
	awaitCount(t, early, 2, 5*time.Second)
	awaitCount(t, late, 1, 5*time.Second)
	awaitCount(t, late, 2, 200*time.Millisecond)
	if got := late.Payloads(); len(got) != 1 || got[0] != "after" {
		t.Fatalf("the later session received %v, want only the message published after it subscribed", got)
	}
}

// invariant 16, and RFC 0003 "Broadcast": the acl_file is asked for each
// delivery, as the substrate asks it for a live broadcast, and one it refuses
// is not delivered - broadcast has no position to stall at - and is let go
// of; the rest are served.
func TestADrainDoesNotDeliverWhatTheACLRefuses(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := durable(t, h, "dev", false, 10, "news/#")
	writeACL(t, h, `
roles:
  open-news:
    - topic: "news/open/#"
      allow: [read]
users:
  "": [open-news]
`)
	refused := owe(t, h, lg, news("secret/a", "refused"), "dev")
	owe(t, h, lg, news("open/a", "allowed"), "dev")
	awaitCount(t, c, 1, 5*time.Second)
	awaitCount(t, c, 2, 200*time.Millisecond)
	if got := c.Payloads(); len(got) != 1 || got[0] != "allowed" {
		t.Fatalf("received %v, want only what the acl_file allows", got)
	}
	eventually(t, "the refused one let go of", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(refused.Offset)
		return len(held) == 0 && lg.Len() == 0
	})
}

// RFC 0003 "Broadcast": a session's broadcast shares its client's Receive
// Maximum with everything else it is sent, so room freed by any other
// delivery's acknowledgement is room for what the session is owed. A
// broadcast waiting behind an append record is sent once that record is
// acknowledged, and not before.
func TestABroadcastWaitingForRoomIsSentWhenAnotherDeliveryIsAcknowledged(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := connectRx(t, h, "dev", false, true, 1)
	c.Sub(t, "events/#", 1)
	c.Sub(t, "news/#", 1)
	h.B.AttachBroadcast("dev")

	p := connect(t, h, "producer", true, false)
	p.Pub(t, "events/a", "record")
	awaitCount(t, c, 1, 5*time.Second)
	if got := c.Payloads(); len(got) != 1 || got[0] != "record" {
		t.Fatalf("the client holds %v, want the append record filling its window of one", got)
	}

	owe(t, h, lg, news("a", "waiting"), "dev")
	awaitCount(t, c, 2, 300*time.Millisecond)
	if got := c.Count(); got != 1 {
		t.Fatalf("the broadcast was sent into a full window: %v", c.Payloads())
	}

	c.All()[0].Ack()
	awaitCount(t, c, 2, 5*time.Second)
	if got := c.Payloads(); len(got) != 2 || got[1] != "waiting" {
		t.Fatalf("after the record was acknowledged the client holds %v, want the broadcast too", got)
	}
}

// MQTT-3.1.2-25, and RFC 0003 "Broadcast": a message larger than the client's
// Maximum Packet Size is not sent to it and is let go of, and the link stays
// up for the rest, which are sent as if it had not been there. It is counted,
// under RFC 0005's too_large, because the client never had it.
func TestABroadcastLargerThanTheClientTakesIsDiscarded(t *testing.T) {
	h, lg, s := drainHarness(t)
	ops := operationsAt(t, h)
	c := connectMaxPacket(t, h, "dev", 256)
	c.Sub(t, "news/#", 1)
	h.B.AttachBroadcast("dev")

	big := owe(t, h, lg, store.Record{Topic: "news/a", Payload: make([]byte, 1024), QoS: 1}, "dev")
	owe(t, h, lg, news("a", "fits"), "dev")
	awaitCount(t, c, 1, 5*time.Second)
	awaitCount(t, c, 2, 200*time.Millisecond)
	if got := c.Payloads(); len(got) != 1 || got[0] != "fits" {
		t.Fatalf("received %d deliveries (%v), want only the one that fits", len(got), got)
	}
	eventually(t, "both let go of, and the discarded one's entry cleared", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(big.Offset)
		_, table, _ := s.InFlight("dev")
		return len(held) == 0 && lg.Len() == 0 && len(table) == 0
	})
	const tooLarge = `saguin_session_deliveries_dropped_total{cause="too_large"}`
	if got := scrapeGauges(t, ops)[tooLarge]; got != 1 {
		t.Errorf("%s reads %v, want the 1 discarded", tooLarge, got)
	}
}

// A broker that can be stopped and started again on the same storage, on
// each session provider: a memory provider through its snapshot directory, a
// sqlite one through its database.
type restartable struct {
	name  string
	start func(t *testing.T) *brokertest.Harness
}

func restartables(t *testing.T) []restartable {
	dir, path := t.TempDir(), filepath.Join(t.TempDir(), "saguin.db")
	return []restartable{
		{"memory", func(t *testing.T) *brokertest.Harness { return startDurable(t, dir) }},
		{"sqlite", func(t *testing.T) *brokertest.Harness { return startDurableSQLite(t, path) }},
	}
}

// attachDrain answers the harness's session provider's broadcast log, which
// the harness attached to the drain as it started, as main does.
func attachDrain(t *testing.T, h *brokertest.Harness) drainLog {
	t.Helper()
	switch s := brokertest.HarnessSessions.(type) {
	case *store.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		return lg
	case *sqlite.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		return lg
	}
	t.Fatalf("the harness's session store is a %T", brokertest.HarnessSessions)
	return nil
}

// drainSessions is what the drain asks of either provider's session store.
type drainSessions interface {
	All() ([]store.Session, error)
	InFlight(client string) (uint16, []store.InFlight, error)
	SetInFlightAll(client string, window uint16, fs []store.InFlight) error
	Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error)
	CreateShareCursor(group string, cursor uint64) error
	SetShareCursor(group string, cursor uint64, forget ...uint64) error
	HandOver(group string, cursor uint64, client string, window uint16, fs []store.InFlight) error
	Return(client, group string, fs []store.InFlight) error
	Lend(group string, cursor uint64, offsets []uint64) error
	EndShareCursorIfUnheld(group string) (bool, error)
	ShareCursors() (map[string]uint64, error)
	ShareReturned() (map[string][]uint64, error)
	EndedAtOpen() map[string]store.ShareGroupState
}

// attachDrainThrough is attachDrain with the drain given the session store
// wrap answers, so a test can make it fail. It starts a drain of its own in
// place of the harness's, so it is called before any session connects.
func attachDrainThrough(t *testing.T, h *brokertest.Harness, wrap func(drainSessions) drainSessions) drainLog {
	t.Helper()
	return attachDrainWith(t, h, func(l drainReads) drainReads { return l }, wrap)
}

// drainReads is what the drain asks of either provider's broadcast log.
type drainReads interface {
	Append(store.Record) (store.Record, error)
	ReadAt(offsets ...uint64) ([]store.Record, error)
	ReadFromN(offset uint64, max int) ([]store.Record, error)
	Remove(offsets ...uint64) (int, int64, error)
	Position(reader string) (store.Position, bool, error)
	Next() uint64
	Floor() uint64
}

// attachDrainWith is attachDrainThrough with the log wrapped too, so a test
// can hold or fail either.
func attachDrainWith(t *testing.T, h *brokertest.Harness, wrapLog func(drainReads) drainReads,
	wrap func(drainSessions) drainSessions) drainLog {
	t.Helper()
	opened := brokertest.HarnessSessions
	if w, ok := opened.(interface{ Unwrap() broker.SessionStore }); ok {
		opened = w.Unwrap() // a WrapSessions wrapper; the drain uses the provider's own store
	}
	switch s := opened.(type) {
	case *store.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		if err := h.B.UseBroadcastLog(wrapLog(lg), wrap(s)); err != nil {
			t.Fatalf("attach the broadcast log: %v", err)
		}
		return lg
	case *sqlite.Sessions:
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		if err := h.B.UseBroadcastLog(wrapLog(lg), wrap(s)); err != nil {
			t.Fatalf("attach the broadcast log: %v", err)
		}
		return lg
	}
	t.Fatalf("the harness's session store is a %T", brokertest.HarnessSessions)
	return nil
}

// owed is a session's owed list, and what it comes to.
func owedOf(h *brokertest.Harness, client string) ([]uint64, int64) { return h.B.OwedBroadcast(client) }

// RFC 0002 `broker.session` and RFC 0003 "Broadcast": a start ends the
// sessions no client can have any more, and what they were owed from the
// broadcast log goes with them. A session whose expiry passed while the
// broker was stopped owes nothing after the start: a message only it was
// owed leaves the log, and one another session is owed too stays, owed once.
// On both providers.
func TestASessionAStartEndsOwesNothingFromTheLog(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			c := brokertest.Dial(t, h, "expired", false, false, 10, 2, 0)
			c.Sub(t, "news/#", 1)
			h.B.AttachBroadcast("expired")
			before := h.Disconnects.Snapshot("expired")
			c.Close()
			sessionGoneAfter(t, h, "expired", before)
			away(t, h, "keeper", "news/b/#")
			shared := owe(t, h, lg, news("b/x", "shared"), "expired", "keeper")
			only := owe(t, h, lg, news("a/x", "only its own"), "expired")
			if _, kept, err := brokertest.HarnessSessions.Get("expired"); err != nil || !kept {
				t.Fatalf("the session ended before the stop (kept %v, %v), so the start ends nothing", kept, err)
			}
			h.Stop()
			time.Sleep(2100 * time.Millisecond) // past its two-second expiry

			h = p.start(t)
			lg = attachDrain(t, h)
			if _, kept, _ := brokertest.HarnessSessions.Get("expired"); kept {
				t.Fatal("the start kept the expired session, so this proves nothing")
			}
			if owed, _ := owedOf(h, "expired"); len(owed) != 0 {
				t.Errorf("after the start the session it ended still owes %v", owed)
			}
			if n := h.B.BroadcastOwners(shared.Offset); n != 1 {
				t.Errorf("the message keeper is owed too is counted for %d sessions, want keeper alone", n)
			}
			eventually(t, "the message only the ended session was owed out of the log", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(only.Offset)
				return len(held) == 0
			})
			if held, _ := lg.ReadAt(shared.Offset); len(held) != 1 {
				t.Error("the message keeper is still owed left the log")
			}
		})
	}
}

// RFC 0003 "Sessions" and "Broadcast": a session retention passed ends
// whole, what it was owed from the broadcast log included, though its record
// is kept for the connection that ended it. The client comes back to Session
// Present 0 and owes nothing the ended session was owed: its list is not the
// ended one's, which the resumed connection would otherwise be served from.
// A batch is held at its read, so a list carried over would still hold the
// message when it is looked at.
func TestASessionRetentionPassedOwesNothingItWasOwedBefore(t *testing.T) {
	h := startTrimming(t, map[string]int64{"events": 200})
	held := &holdsReads{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(held.let)
	lg := attachDrainWith(t, h, func(l drainReads) drainReads {
		held.drainReads = l
		return held
	}, func(s drainSessions) drainSessions { return s })
	p := connect(t, h, "producer", true, false)
	c := connect(t, h, "device", false, false)
	c.Sub(t, "events/#", 1)
	c.Sub(t, "news/#", 1)
	h.B.AttachBroadcast("device")
	p.Pub(t, "events/a/1", "first")
	if _, ok := c.Await(t, 3*time.Second); !ok {
		t.Fatal("no delivery before the link dropped")
	}
	awaitStoredPosition(t, h, "events", 2, 5*time.Second) // its position stored
	linkCut(t, c)
	sessionGone(t, h, "device")
	m := owe(t, h, lg, news("a", "owed to the ended session"), "device")
	for i := range 30 {
		p.Pub(t, "events/a/x", fmt.Sprintf("record-%02d-padded-out-to-push-the-floor-along", i))
	}
	awaitFloorPast(t, h, "events", 2, 5*time.Second) // the size sweep past the position

	held.armed.Store(true)
	again := connect(t, h, "device", false, false)
	if again.SessionPresent {
		t.Fatal("the session was resumed, so retention did not end it and this proves nothing")
	}
	if owed, _ := owedOf(h, "device"); len(owed) != 0 {
		t.Errorf("the session that came back owes %v, the ended session's list (message at %d)", owed, m.Offset)
	}
}

// Invariant 18's first exception, and RFC 0003 "Broadcast": a session's
// acknowledgements are stored at most broker.session.ack_commit_interval after
// they arrive, so an unclean stop sends again only what it acknowledged in the
// last interval - and that holds for a drain that always has more to send. It
// stored nothing until it went idle: 500 durable sessions on a sqlite
// provider, each owed 10,000 broadcasts and acknowledging as they came, held
// on average 5,659 entries each in the stored table, acknowledged and
// unwritten, and a stop took 41s to write them.
//
// A session with the default Receive Maximum, so the window never fills and
// the drain never stops to wait, resumes to a backlog it acknowledges as each
// message arrives, in order, under an interval of 20ms. While it is still
// owed messages, an entry for a message it had received three intervals
// before the table is read has been acknowledged for that long, and at most
// what one flush takes may still be on its way to the store.
func TestABusyDrainStoresItsAcknowledgementsAsItGoes(t *testing.T) {
	// A backlog long enough to catch the drain busy: under the default bound
	// an away session gives up all but about eight thousand of these.
	capAt(t, 2<<20)
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			const interval = 20 * time.Millisecond
			h.B.SetAckCommitInterval(interval)
			lg := attachDrain(t, h)
			away(t, h, "busy", "news/#")
			const n = 60000
			offsets := make([]uint64, n)
			for i := range n {
				offsets[i] = owe(t, h, lg, news("a", fmt.Sprint(i)), "busy").Offset
			}
			// Its bound gives up the oldest of so long a backlog, so what it is
			// owed is counted rather than assumed.
			start, _ := owedOf(h, "busy")
			total := len(start)

			// Paced rather than left to the machine's own speed: in-process, a
			// batch and its acknowledgement round-trip fast enough that the
			// whole backlog could clear before a single ack_commit_interval
			// elapses, especially on the memory provider, leaving nothing to
			// catch busy. Spreading batches half an interval apart guarantees
			// several intervals elapse while the backlog is still worked,
			// regardless of provider speed or machine load.
			restorePace := broker.SetAfterBatchWritten(func(client string, sent int) {
				if client == "busy" && sent > 0 {
					time.Sleep(interval / 2)
				}
			})
			defer restorePace()

			c := connectRx(t, h, "busy", false, false, 0)
			if !c.SessionPresent {
				t.Fatal("the session was not resumed, so it is owed nothing and this proves nothing")
			}

			// Sampled from the source rather than by an outside poll: this
			// runs on the drain's own goroutine every time its busy loop
			// checks whether to flush - as often as the drain actually makes
			// that check, not however many times a wall-clock poll happens to
			// land while it is busy, which a loaded machine can step around
			// entirely (the failure this replaces). The check itself is
			// unchanged: what had arrived grace ago, and so been acknowledged
			// for that long, still sitting in the stored table.
			type seen struct {
				at  time.Time
				got int
			}
			var (
				mu                        sync.Mutex
				history                   []seen
				worst, atReceived, midway int
			)
			grace := 3 * interval
			restore := broker.SetAfterBusyFlushCheck(func(client string) {
				if client != "busy" {
					return
				}
				owed, _ := owedOf(h, "busy")
				if len(owed) == 0 {
					return
				}
				now, got := time.Now(), c.Count()
				mu.Lock()
				history = append(history, seen{now, got})
				then := 0
				for _, s := range history {
					if s.at.After(now.Add(-grace)) {
						break
					}
					then = s.got
				}
				mu.Unlock()
				if then < total/4 || len(owed) < total/4 {
					return
				}
				all := c.All()
				if then == 0 || then > len(all) {
					return
				}
				last, err := strconv.Atoi(all[then-1].Payload)
				if err != nil {
					t.Errorf("delivery %d: %v", then, err)
					return
				}
				// Read inline rather than through tableOf: that helper calls
				// t.Fatalf on error, which only the test's own goroutine may
				// do, and this runs on the drain's.
				_, table, err := brokertest.HarnessSessions.(interface {
					InFlight(string) (uint16, []store.InFlight, error)
				}).InFlight("busy")
				if err != nil {
					t.Errorf("read busy's in-flight table: %v", err)
					return
				}
				acknowledged := 0
				for _, e := range table {
					if e.Offset <= offsets[last] {
						acknowledged++
					}
				}
				mu.Lock()
				midway++
				if acknowledged > worst {
					worst, atReceived = acknowledged, then
				}
				mu.Unlock()
			})
			defer restore()

			for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(5 * time.Millisecond) {
				owed, _ := owedOf(h, "busy")
				if len(owed) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the session was still owed %d messages after 60s, with %d received", len(owed), c.Count())
				}
			}
			prev := -1
			for i, r := range c.All() {
				if v, _ := strconv.Atoi(r.Payload); v <= prev {
					t.Fatalf("delivery %d was %q after %d: out of order, so the offsets above are not what "+
						"was received", i, r.Payload, prev)
				} else {
					prev = v
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if midway < 5 {
				t.Fatalf("the table was read %d times with a quarter of the backlog both received %v "+
					"before and still owed: the drain was never caught busy, so this proves nothing", midway, grace)
			}
			t.Logf("%d reads while the drain was busy; the stored table held at most %d entries "+
				"acknowledged %v before, at %d received", midway, worst, grace, atReceived)
			if worst > 2*broker.PumpBatchCap {
				t.Errorf("the stored table held %d entries the client had acknowledged %v before, at %d "+
					"received, while the drain was busy: its acknowledgements were not being written",
					worst, grace, atReceived)
			}
		})
	}
}

// RFC 0003 "Broadcast": a session lets go of each message it is owed once,
// and a message leaves the log with the last session that owed it. A session
// that ends lets go of its whole list, so a batch still running for it lets
// go of nothing more - not what it could not record, not what it wrote at
// QoS 0, not what it did not send for its size. Let go twice, a message two
// sessions were owed left the log while the other was still owed it.
func TestAnEndedSessionsBatchLetsGoOfNothingMore(t *testing.T) {
	for _, how := range []string{"drop", "passed", "discard"} {
		t.Run(how, func(t *testing.T) {
			h, lg, _ := drainHarness(t)
			away(t, h, "ended", "news/#")
			away(t, h, "other", "news/#")
			m := owe(t, h, lg, news("a", "owed twice"), "ended", "other")
			list := h.B.BroadcastList("ended")
			if list == nil || h.B.BroadcastOwners(m.Offset) != 2 {
				t.Fatalf("the message is owed by %d sessions, want both", h.B.BroadcastOwners(m.Offset))
			}
			h.B.EndBroadcast("ended")
			h.B.LetGoOnList(list, m.Offset, how)
			if n := h.B.BroadcastOwners(m.Offset); n != 1 {
				t.Errorf("the message is counted for %d sessions after the ended one let go, want the other", n)
			}
			// A release runs on its own goroutine and gathers for an
			// interval; a stopping broker removes what it gathered before
			// Stop returns, so a message still in the log now was never
			// released.
			h.Stop()
			if held, _ := lg.ReadAt(m.Offset); len(held) != 1 {
				t.Error("the message left the log while the other session is owed it")
			}
		})
	}
}

// RFC 0003 "Broadcast": a session's owed list and each message's count are
// held in memory, so a start counts them again from the log, each session's
// cursor, its in-flight table and its subscriptions - and they come back as
// they were: what each session was owed, the entries still on the wire under
// the identifiers they carried (MQTT-4.4.0-1), and every message's count. A
// session that was away is sent what it is owed once it is back. A message
// nobody is owed leaves the log. On both providers.
func TestAStartCountsAgainWhoOwesWhatTheLogHolds(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			a := durable(t, h, "a", true, 2, "news/#")
			b := durable(t, h, "b", true, 10, "news/#")
			away := durable(t, h, "away", true, 10, "news/#")
			away.Close()
			sessionGone(t, h, "away")
			var ms []store.Record
			for i := range 5 {
				ms = append(ms, owe(t, h, lg, news("a", fmt.Sprint("m-", i)), "a", "b", "away"))
			}
			awaitCount(t, a, 2, 5*time.Second)
			awaitCount(t, b, 5, 5*time.Second)
			for _, m := range b.All()[:2] {
				m.Ack()
			}
			eventually(t, "b's cursor past its first two", 5*time.Second, func() bool {
				pos, ok, _ := lg.Position(store.MQTTReader("b"))
				return ok && pos.Offset == ms[2].Offset
			})
			// Kept by the log and counted for nobody, as a message is whose
			// last owner let it go in the moment before a stop.
			stray, err := lg.Append(store.Record{MessageID: "stray", Topic: "elsewhere/x", Payload: []byte("stray"),
				QoS: 1, Timestamp: time.Now()})
			if err != nil {
				t.Fatal(err)
			}

			aOwed, aBytes := owedOf(h, "a")
			bOwed, bBytes := owedOf(h, "b")
			awayOwed, awayBytes := owedOf(h, "away")
			if len(aOwed) != 5 || len(bOwed) != 3 || len(awayOwed) != 5 {
				t.Fatalf("before the stop a owes %v, b %v and away %v; want five, three and five", aOwed, bOwed, awayOwed)
			}
			counts := map[uint64]uint32{}
			for _, m := range ms {
				counts[m.Offset] = h.B.BroadcastOwners(m.Offset)
			}
			_, aTable, err := brokertest.HarnessSessions.(interface {
				InFlight(string) (uint16, []store.InFlight, error)
			}).InFlight("a")
			if err != nil || len(aTable) != 2 {
				t.Fatalf("a's in-flight table before the stop is %+v (%v), want its two", aTable, err)
			}
			h.Stop()

			h = p.start(t)
			lg = attachDrain(t, h)
			if got, bytes := owedOf(h, "a"); !slices.Equal(got, aOwed) || bytes != aBytes {
				t.Errorf("after the start a owes %v (%d bytes), want %v (%d)", got, bytes, aOwed, aBytes)
			}
			if got, bytes := owedOf(h, "b"); !slices.Equal(got, bOwed) || bytes != bBytes {
				t.Errorf("after the start b owes %v (%d bytes), want %v (%d)", got, bytes, bOwed, bBytes)
			}
			if got, bytes := owedOf(h, "away"); !slices.Equal(got, awayOwed) || bytes != awayBytes {
				t.Errorf("after the start away owes %v (%d bytes), want %v (%d)", got, bytes, awayOwed, awayBytes)
			}
			for off, want := range counts {
				if got := h.B.BroadcastOwners(off); got != want {
					t.Errorf("after the start the message at %d has %d owners, want %d", off, got, want)
				}
			}
			_, table, err := brokertest.HarnessSessions.(interface {
				InFlight(string) (uint16, []store.InFlight, error)
			}).InFlight("a")
			if err != nil || !slices.Equal(table, aTable) {
				t.Errorf("a's in-flight table after the start is %+v (%v), want %+v", table, err, aTable)
			}
			eventually(t, "the message nobody owes gone", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(stray.Offset)
				return len(held) == 0
			})

			// What a held on the wire is sent again first, as its resume finds
			// it: the two it had, filling its window of two, and nothing past
			// them. (That they carry the identifiers they first went out under,
			// with DUP set, is read off the wire in
			// TestARestartResumeIsSentItsMessagesUnderTheirIdentifiers.)
			aBack := connectRx(t, h, "a", false, true, 2)
			awaitCount(t, aBack, 2, 5*time.Second)
			awaitCount(t, aBack, 3, 300*time.Millisecond)
			if got := aBack.Payloads(); !slices.Equal(got, []string{"m-0", "m-1"}) {
				t.Errorf("a was sent %v after the start, want what it held on the wire, m-0 and m-1, and no more", got)
			}

			// The session that was away, back, is sent all it was owed, in order.
			back := connectRx(t, h, "away", false, true, 10)
			h.B.WakeBroadcast("away")
			awaitCount(t, back, 5, 5*time.Second)
			awaitCount(t, back, 6, 200*time.Millisecond)
			if got := back.Payloads(); !slices.Equal(got, []string{"m-0", "m-1", "m-2", "m-3", "m-4"}) {
				t.Errorf("the session that was away was sent %v after the start, want m-0 to m-4", got)
			}
		})
	}
}

// RFC 0003 "Broadcast", at the maintainer's ruling: a subscription is owed
// only what was published after it was made, while the broker runs and after
// a start alike. A session whose cursor is behind a message, and which then
// subscribes to a filter matching it, is not owed it - before the restart
// because it was not counted for it, and after it because the subscription's
// Since is past it. On both providers.
func TestALaterSubscriptionReachesNothingEarlierAcrossARestart(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)

			away := connectRx(t, h, "late", false, true, 10)
			away.Sub(t, "news/a/#", 1)
			h.B.AttachBroadcast("late")
			away.Close()
			sessionGone(t, h, "late")
			// Owed while it is away, so its cursor stays behind everything below.
			a := owe(t, h, lg, news("a/1", "A"), "late")

			// Held in the log by a session that does not acknowledge it.
			holder := durable(t, h, "holder", true, 10, "news/#")
			x := owe(t, h, lg, news("b/1", "X"), "holder")
			awaitCount(t, holder, 1, 5*time.Second)

			back := connectRx(t, h, "late", false, true, 10)
			if !back.SessionPresent {
				t.Fatal("the session did not resume, so its cursor is not behind anything")
			}
			back.Sub(t, "news/b/#", 1)
			sess, ok, err := brokertest.HarnessSessions.Get("late")
			if err != nil || !ok {
				t.Fatalf("the session's record: %v, %v", ok, err)
			}
			var since uint64
			for _, sub := range sess.Subscriptions {
				if sub.Filter == "news/b/#" {
					since = sub.Since
				}
			}
			if since <= x.Offset {
				t.Fatalf("news/b/# was made after X (offset %d) and is stored as made at %d", x.Offset, since)
			}
			y := owe(t, h, lg, news("b/2", "Y"), "late", "holder")

			want := []uint64{a.Offset, y.Offset}
			if got, _ := owedOf(h, "late"); !slices.Equal(got, want) {
				t.Fatalf("while the broker runs the session owes %v, want A and Y (%v)", got, want)
			}
			awaitCount(t, back, 2, 5*time.Second)
			awaitCount(t, back, 3, 200*time.Millisecond)
			if got := back.Payloads(); !slices.Equal(got, []string{"A", "Y"}) {
				t.Fatalf("while the broker runs the session was sent %v, want A and Y", got)
			}
			if held, _ := lg.ReadAt(x.Offset); len(held) != 1 {
				t.Fatal("X left the log, so the start below has nothing to leave out")
			}
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			if got, _ := owedOf(h, "late"); !slices.Equal(got, want) {
				t.Errorf("after the start the session owes %v, want A and Y (%v) and not X (%d)", got, want, x.Offset)
			}
			if got, _ := owedOf(h, "holder"); !slices.Equal(got, []uint64{x.Offset, y.Offset}) {
				t.Errorf("after the start the holder owes %v, want X and Y", got)
			}
		})
	}
}

// RFC 0003 "Broadcast" and "Client-declared partitioning": a start counts a
// message for a session as the live broadcast would have - not to a No Local
// subscription for its own publish, and to a declared slice only where the
// topic's hash falls in it (the RFC's hash, computed here from its two
// blocks). A message nobody is owed leaves the log. On both providers.
func TestAStartAsksNoLocalAndTheSliceAsTheLiveBroadcastDoes(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			local := connectRx(t, h, "local", false, true, 10)
			if _, err := local.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
				{Topic: "news/#", QoS: 1, NoLocal: true},
			}}); err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			sliced := connectRx(t, h, "sliced", false, true, 10)
			sliced.SubSliced(t, "news/#", 2, 0)

			var recs []store.Record
			for i := range 12 {
				publisher := "somebody"
				if i%3 == 0 {
					publisher = "local"
				}
				r, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: fmt.Sprint("news/t", i),
					Payload: []byte("x"), QoS: 1, Publisher: publisher, Timestamp: time.Now()})
				if err != nil {
					t.Fatal(err)
				}
				recs = append(recs, r)
			}
			h.Stop()

			h = p.start(t)
			lg = attachDrain(t, h)
			var wantLocal, wantSliced, wantGone []uint64
			for _, r := range recs {
				l, s := r.Publisher != "local", ownerOf(r.Topic, 2) == 0
				if l {
					wantLocal = append(wantLocal, r.Offset)
				}
				if s {
					wantSliced = append(wantSliced, r.Offset)
				}
				if !l && !s {
					wantGone = append(wantGone, r.Offset)
				}
			}
			if len(wantGone) == 0 || len(wantSliced) == len(recs) || len(wantLocal) == len(recs) {
				t.Fatalf("the topics chosen do not exercise the rules: %d gone, %d sliced, %d local of %d",
					len(wantGone), len(wantSliced), len(wantLocal), len(recs))
			}
			if got, _ := owedOf(h, "local"); !slices.Equal(got, wantLocal) {
				t.Errorf("the No Local session is owed %v, want %v: none of its own", got, wantLocal)
			}
			if got, _ := owedOf(h, "sliced"); !slices.Equal(got, wantSliced) {
				t.Errorf("the sliced session is owed %v, want %v: its slice only", got, wantSliced)
			}
			eventually(t, "what nobody is owed gone", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(wantGone...)
				return len(held) == 0
			})
		})
	}
}

// RFC 0003 "Broadcast": each subscription stores the log's next offset when it
// is made. A filter held and subscribed again keeps its own; a new filter
// takes the log's place now; and a filter subscribed again after an
// UNSUBSCRIBE is a new subscription with a new place. On both providers.
func TestASubscriptionKeepsWhereTheLogWasWhenItWasMade(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			sinceOf := func(filter string) (uint64, bool) {
				t.Helper()
				sess, ok, err := brokertest.HarnessSessions.Get("dev")
				if err != nil || !ok {
					t.Fatalf("the session's record: %v, %v", ok, err)
				}
				for _, sub := range sess.Subscriptions {
					if sub.Filter == filter {
						return sub.Since, true
					}
				}
				return 0, false
			}
			c := connectRx(t, h, "dev", false, false, 10)
			first := lg.Next()
			c.Sub(t, "news/a/#", 1)
			if got, _ := sinceOf("news/a/#"); got != first {
				t.Fatalf("a first subscription is stored as made at %d, want the log's next %d", got, first)
			}
			// Published and counted, for nobody: the log moves on.
			for i := range 3 {
				owe(t, h, lg, news("a/x", fmt.Sprint("m-", i)))
			}
			c.Sub(t, "news/a/#", 1)
			if got, _ := sinceOf("news/a/#"); got != first {
				t.Errorf("subscribed again, the filter is stored as made at %d, want %d still", got, first)
			}
			later := lg.Next()
			c.Sub(t, "news/b/#", 1)
			if got, _ := sinceOf("news/b/#"); got != later || later == first {
				t.Errorf("a second filter is stored as made at %d, want the log's next %d", got, later)
			}
			if _, err := c.C.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{"news/a/#"}}); err != nil {
				t.Fatalf("unsubscribe: %v", err)
			}
			eventually(t, "the unsubscribed filter gone from the record", 5*time.Second, func() bool {
				_, held := sinceOf("news/a/#")
				return !held
			})
			c.Sub(t, "news/a/#", 1)
			if got, _ := sinceOf("news/a/#"); got != later {
				t.Errorf("subscribed again after an UNSUBSCRIBE, the filter is stored as made at %d, want %d", got, later)
			}
		})
	}
}

// RFC 0003 "Broadcast" and RFC 0004: a start reads the whole log, from its
// floor, so a message every session has passed leaves it too - as one does
// that a crash left behind between the last session letting it go and its
// removal; and an in-flight entry whose message the log no longer holds, as a
// crash between a removal and its acknowledgement leaves one, leaves the
// session's table with its next write. On both providers.
func TestAStartLetsGoOfWhatNoSessionCanBeSent(t *testing.T) {
	type acknowledging interface {
		Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error)
	}
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			durable(t, h, "passed", false, 10, "news/#")
			early, err := lg.Append(store.Record{MessageID: "early", Topic: "news/early", Payload: []byte("early"),
				QoS: 1, Timestamp: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			// The session's cursor past it, as its acknowledgement left it.
			if _, err := brokertest.HarnessSessions.(acknowledging).Acknowledge("passed", lg.Next(), nil); err != nil {
				t.Fatalf("place the cursor: %v", err)
			}

			ghost := durable(t, h, "ghost", true, 10, "ghost/#")
			m := owe(t, h, lg, store.Record{Topic: "ghost/a", Payload: []byte("gone"), QoS: 1}, "ghost")
			awaitCount(t, ghost, 1, 5*time.Second)
			// Its cursor at the message it holds, so that every session's cursor
			// is past the early one and only a walk from the floor reaches it.
			if _, err := brokertest.HarnessSessions.(acknowledging).Acknowledge("ghost", m.Offset, nil); err != nil {
				t.Fatalf("place the cursor: %v", err)
			}
			if n, _, err := lg.Remove(m.Offset); err != nil || n != 1 {
				t.Fatalf("remove the message under the entry: %d, %v", n, err)
			}
			h.Stop()

			h = p.start(t)
			lg = attachDrain(t, h)
			eventually(t, "the message every session had passed gone", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(early.Offset)
				return len(held) == 0
			})
			if got, _ := owedOf(h, "ghost"); len(got) != 0 {
				t.Errorf("the session is owed %v, whose message the log no longer holds", got)
			}
			connectRx(t, h, "ghost", false, true, 10)
			h.B.WakeBroadcast("ghost")
			table := brokertest.HarnessSessions.(interface {
				InFlight(string) (uint16, []store.InFlight, error)
			})
			eventually(t, "the entry for the message the log lost gone from the table", 5*time.Second, func() bool {
				_, entries, err := table.InFlight("ghost")
				return err == nil && len(entries) == 0
			})
		})
	}
}

// rawClient is an MQTT 5 client driven a packet at a time, for the one thing
// Paho will not do: answer a QoS 2 delivery's PUBLISH and leave its PUBREL
// unanswered, or answer it with a reason of its choosing. It writes what it is
// told and reads what it is asked to, and nothing else (test rule 9).
type rawClient struct {
	t    *testing.T
	conn net.Conn
}

// dialRaw connects a session that outlives its connection and subscribes it
// to filter at QoS 2.
func dialRaw(t *testing.T, h *brokertest.Harness, id, filter string) *rawClient {
	t.Helper()
	c, _ := connectRaw(t, h, id, 10)
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, ProtocolVersion: 5,
		PacketID: 1, Filters: packets.Subscriptions{{Filter: filter, Qos: 2}}})
	if pk := c.read(); pk.FixedHeader.Type != packets.Suback || len(pk.ReasonCodes) != 1 || pk.ReasonCodes[0] != 2 {
		t.Fatalf("subscribe: %+v", pk)
	}
	return c
}

// connectRaw connects a session that outlives its connection, with Clean
// Start 0 and the Receive Maximum given, and answers whether the broker said
// the session was present. It sends nothing more, since a resumed session's
// re-sends may come before anything it could ask for.
func connectRaw(t *testing.T, h *brokertest.Harness, id string, rx uint16) (*rawClient, bool) {
	t.Helper()
	return connectRawAs(t, h, id, rx, false)
}

// connectRawAs is connectRaw with the Clean Start flag given.
func connectRawAs(t *testing.T, h *brokertest.Harness, id string, rx uint16, clean bool) (*rawClient, bool) {
	t.Helper()
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &rawClient{t: t, conn: conn}
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
		Connect:    &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: id, Keepalive: 60, Clean: clean},
		Properties: packets.Properties{SessionExpiryInterval: 3600, SessionExpiryIntervalFlag: true, ReceiveMaximum: rx}})
	pk := c.read()
	if pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
		t.Fatalf("connect: %+v", pk)
	}
	return c, pk.SessionPresent
}

// quiet reports whether the broker sends nothing for a while.
func (c *rawClient) quiet(d time.Duration) bool {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	var b [1]byte
	_, err := c.conn.Read(b[:])
	if err == nil {
		c.t.Fatalf("the broker sent a packet (first byte %#x) where nothing was due", b[0])
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func (c *rawClient) write(pk packets.Packet) {
	c.t.Helper()
	pk.ProtocolVersion = 5
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	case packets.Pubrec:
		err = pk.PubrecEncode(&buf)
	case packets.Pubcomp:
		err = pk.PubcompEncode(&buf)
	default:
		c.t.Fatalf("the raw client does not send a packet of type %d", pk.FixedHeader.Type)
	}
	if err != nil {
		c.t.Fatalf("encode: %v", err)
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// read is the next packet the broker sent, decoded; it fails the test on a
// packet it does not know and after five seconds of silence.
func (c *rawClient) read() packets.Packet {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	first, body, err := brokertest.ReadRawPacket(c.conn)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	pk := packets.Packet{ProtocolVersion: 5}
	if err := pk.FixedHeader.Decode(first); err != nil {
		c.t.Fatalf("fixed header: %v", err)
	}
	switch pk.FixedHeader.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Pubrel:
		err = pk.PubrelDecode(body)
	default:
		c.t.Fatalf("the broker sent a packet of type %d the raw client does not read", pk.FixedHeader.Type)
	}
	if err != nil {
		c.t.Fatalf("decode type %d: %v", pk.FixedHeader.Type, err)
	}
	return pk
}

// tableOf is a session's in-flight table on either provider.
func tableOf(t *testing.T, client string) []store.InFlight {
	t.Helper()
	_, table, err := brokertest.HarnessSessions.(interface {
		InFlight(string) (uint16, []store.InFlight, error)
	}).InFlight(client)
	if err != nil {
		t.Fatalf("read %s's in-flight table: %v", client, err)
	}
	return table
}

// RFC 0003 "Exactly once": a QoS 2 broadcast to a QoS 2 subscription is
// delivered at QoS 2 and its exchange completes, after which the log holds
// nothing the session owed and its in-flight table is empty.
func TestAQoS2BroadcastIsDeliveredAtQoS2AndLetGo(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := connectRx(t, h, "dev", false, false, 10)
	c.Sub(t, "news/#", 2)
	h.B.AttachBroadcast("dev")
	r := news("a", "exactly once")
	r.QoS = 2
	owe(t, h, lg, r, "dev")
	awaitCount(t, c, 1, 5*time.Second)
	if got := c.All()[0]; got.QoS != 2 || got.Payload != "exactly once" {
		t.Fatalf("received %q at QoS %d, want it at QoS 2", got.Payload, got.QoS)
	}
	eventually(t, "the exchange complete and the message let go of", 5*time.Second, func() bool {
		return lg.Len() == 0 && len(tableOf(t, "dev")) == 0
	})
}

// RFC 0003 "Broadcast" and MQTT-4.3.3: a PUBREC moves the session's in-flight
// entry on to Released before the PUBREL is sent, so that a crash after the
// client has finished with the identifier re-sends the PUBREL and never the
// PUBLISH. The message stays owed until the PUBCOMP, and then leaves the log
// and the table. On both providers.
func TestAPUBRECMovesTheEntryOnBeforeThePUBRELGoesOut(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			c := dialRaw(t, h, "raw", "news/#")
			h.B.AttachBroadcast("raw")
			r := news("a", "twice would be wrong")
			r.QoS = 2
			m := owe(t, h, lg, r, "raw")

			pub := c.read()
			if pub.FixedHeader.Type != packets.Publish || pub.FixedHeader.Qos != 2 || pub.PacketID == 0 {
				t.Fatalf("the delivery is %+v, want a QoS 2 PUBLISH", pub.FixedHeader)
			}
			want := store.InFlight{Offset: m.Offset, PacketID: pub.PacketID, QoS: 2, State: store.MessageSent}
			if got := tableOf(t, "raw"); len(got) != 1 || got[0] != want {
				t.Fatalf("before the PUBREC the table holds %+v, want %+v", got, want)
			}

			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pub.PacketID})
			rel := c.read()
			if rel.FixedHeader.Type != packets.Pubrel || rel.PacketID != pub.PacketID {
				t.Fatalf("after the PUBREC the broker sent %+v, want the PUBREL for %d", rel.FixedHeader, pub.PacketID)
			}
			// Read as the PUBREL arrives: it was written first.
			want.State = store.MessageReleased
			if got := tableOf(t, "raw"); len(got) != 1 || got[0] != want {
				t.Fatalf("when the PUBREL arrived the table held %+v, want %+v", got, want)
			}
			if held, _ := lg.ReadAt(m.Offset); len(held) != 1 {
				t.Fatal("the message left the log before its PUBCOMP")
			}

			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubcomp}, PacketID: pub.PacketID})
			eventually(t, "the PUBCOMP letting the message and its entry go", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(m.Offset)
				return len(held) == 0 && len(tableOf(t, "raw")) == 0
			})
		})
	}
}

// refusesReleased is a session store that, while armed, refuses with err to
// record that an exchange reached its PUBREC.
type refusesReleased struct {
	drainSessions
	err     error
	armed   atomic.Bool
	refused atomic.Int32
}

func (s *refusesReleased) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	for _, f := range fs {
		if f.State == store.MessageReleased && s.armed.Load() {
			s.refused.Add(1)
			return s.err
		}
	}
	return s.drainSessions.SetInFlightAll(client, window, fs)
}

// Invariant 18 and RFC 0003 "Broadcast": the PUBREL goes out once the PUBREC
// is stored. Where the store refuses that write the client is sent no PUBREL
// and the connection ends with DISCONNECT 0x80. The exchange is still at its
// PUBLISH in the store and in the drain, the message is still owed, and when
// the session resumes the PUBLISH is sent again under its identifier, with
// DUP, and this time completes.
func TestAPUBRECTheStoreRefusesSendsNoPUBREL(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			st := &refusesReleased{err: errors.New("the store is refusing writes")}
			lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
				st.drainSessions = s
				return st
			})
			c := dialRaw(t, h, "raw", "news/#")
			h.B.AttachBroadcast("raw")
			r := news("a", "once, after a refusal")
			r.QoS = 2
			m := owe(t, h, lg, r, "raw")
			pub := c.read()
			if pub.FixedHeader.Type != packets.Publish || pub.FixedHeader.Qos != 2 || pub.PacketID == 0 {
				t.Fatalf("the delivery is %+v, want a QoS 2 PUBLISH", pub.FixedHeader)
			}
			want := store.InFlight{Offset: m.Offset, PacketID: pub.PacketID, QoS: 2, State: store.MessageSent}

			st.armed.Store(true)
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pub.PacketID})
			_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			first, body, err := brokertest.ReadRawPacket(c.conn)
			if err != nil {
				t.Fatalf("after the refused PUBREC nothing arrived (%v), want a DISCONNECT", err)
			}
			if first>>4 != packets.Disconnect || len(body) == 0 || body[0] != packets.ErrUnspecifiedError.Code {
				t.Fatalf("after the refused PUBREC the broker sent type %d, body % x; want DISCONNECT 0x80",
					first>>4, body)
			}
			if _, _, err := brokertest.ReadRawPacket(c.conn); err == nil {
				t.Fatal("the connection sent more after its DISCONNECT")
			}
			if n := st.refused.Load(); n != 1 {
				t.Fatalf("the store refused %d writes, want the one", n)
			}
			if got := tableOf(t, "raw"); len(got) != 1 || got[0] != want {
				t.Fatalf("after the refusal the table holds %+v, want %+v", got, want)
			}
			if got := h.B.FlyingBroadcast("raw"); len(got) != 1 || got[0] != want {
				t.Fatalf("after the refusal the drain holds %+v, want %+v", got, want)
			}
			if owed, _ := h.B.OwedBroadcast("raw"); !slices.Contains(owed, m.Offset) {
				t.Fatalf("the session owes %v, want it still owing %d", owed, m.Offset)
			}

			st.armed.Store(false)
			back, present := connectRaw(t, h, "raw", 10)
			if !present {
				t.Fatal("the session did not resume")
			}
			again := back.read()
			if got := sentOf(again); got.typ != packets.Publish || got.id != pub.PacketID || !got.dup ||
				again.FixedHeader.Qos != 2 {
				t.Fatalf("after the resume the broker sent %+v at QoS %d, want the PUBLISH again under %d with DUP",
					got, again.FixedHeader.Qos, pub.PacketID)
			}
			back.ack(again)
			eventually(t, "the completed exchange letting the message and its entry go", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(m.Offset)
				return len(held) == 0 && len(tableOf(t, "raw")) == 0
			})
		})
	}
}

// Invariant 18: a session the store no longer holds has nothing a restart
// could send again, so the store's answer that it holds none is not a
// refusal, and the PUBREL goes.
func TestAPUBRECForASessionTheStoreNoLongerHoldsIsAnswered(t *testing.T) {
	h := start(t)
	st := &refusesReleased{err: store.ErrNoSession}
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	r := news("a", "no session")
	r.QoS = 2
	owe(t, h, lg, r, "raw")
	pub := c.read()
	if pub.FixedHeader.Type != packets.Publish || pub.FixedHeader.Qos != 2 {
		t.Fatalf("the delivery is %+v, want a QoS 2 PUBLISH", pub.FixedHeader)
	}
	st.armed.Store(true)
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pub.PacketID})
	if rel := c.read(); rel.FixedHeader.Type != packets.Pubrel || rel.PacketID != pub.PacketID {
		t.Fatalf("after the PUBREC the broker sent %+v, want the PUBREL for %d", rel.FixedHeader, pub.PacketID)
	}
	if n := st.refused.Load(); n != 1 {
		t.Fatalf("the store answered %d writes with no session, want the one", n)
	}
}

// holdsAcknowledgements is a session store that, once armed, holds the next
// write of a session's acknowledgements until it is let go.
type holdsAcknowledgements struct {
	drainSessions
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *holdsAcknowledgements) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	if len(done) > 0 && s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return s.drainSessions.Acknowledge(client, cursor, done)
}

func (s *holdsAcknowledgements) let() { s.once.Do(func() { close(s.release) }) }

// Invariant 18 and RFC 0003 "Broadcast": the close that answers a clean
// DISCONNECT tells the client what it acknowledged is done, so the session's
// acknowledgements are stored before it. A consumer acknowledges its last
// message and disconnects; the acknowledgement's write is held, and the
// connection must not close while it is. Once it is let go the connection
// closes, and by then the in-flight table is empty and the cursor is past the
// message - QoS 1's PUBACK and QoS 2's PUBCOMP alike, on both providers.
//
// Two shapes. Together: the acknowledgement and the DISCONNECT in one write,
// as a consumer leaving sends them, where either the drain or the
// DISCONNECT may be the one writing. After: the DISCONNECT once the drain's
// own write of the acknowledgement is held, which the DISCONNECT has to wait
// for rather than find nothing left to write.
func TestACleanDisconnectHasItsBroadcastAcknowledgementsStored(t *testing.T) {
	for _, together := range []bool{true, false} {
		for _, qos := range []byte{1, 2} {
			for _, p := range restartables(t) {
				shape := map[bool]string{true: "together", false: "after"}[together]
				t.Run(fmt.Sprintf("%s/QoS %d/%s", shape, qos, p.name), func(t *testing.T) {
					aCleanDisconnectHasItsAcknowledgementsStored(t, p, qos, together)
				})
			}
		}
	}
}

func aCleanDisconnectHasItsAcknowledgementsStored(t *testing.T, p restartable, qos byte, together bool) {
	h := p.start(t)
	st := &holdsAcknowledgements{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(st.let)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	r := news("a", "acknowledged, then gone")
	r.QoS = qos
	m := owe(t, h, lg, r, "raw")
	pub := c.read()
	if pub.FixedHeader.Type != packets.Publish || pub.FixedHeader.Qos != qos {
		t.Fatalf("the delivery is %+v, want a QoS %d PUBLISH", pub.FixedHeader, qos)
	}
	last := []byte{0x40, 0x02, byte(pub.PacketID >> 8), byte(pub.PacketID)} // PUBACK
	if qos == 2 {
		c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pub.PacketID})
		if rel := c.read(); rel.FixedHeader.Type != packets.Pubrel || rel.PacketID != pub.PacketID {
			t.Fatalf("after the PUBREC the broker sent %+v, want the PUBREL", rel.FixedHeader)
		}
		last[0] = 0x70 // PUBCOMP
	}

	st.armed.Store(true)
	disconnect := []byte{0xE0, 0x00}
	if together {
		last, disconnect = append(last, disconnect...), nil
	}
	if _, err := c.conn.Write(last); err != nil {
		t.Fatalf("write the acknowledgement: %v", err)
	}
	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no acknowledgement was written, so this proves nothing")
	}
	if disconnect != nil {
		if _, err := c.conn.Write(disconnect); err != nil {
			t.Fatalf("write the DISCONNECT: %v", err)
		}
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	_, _, err := brokertest.ReadRawPacket(c.conn)
	var ne net.Error
	if err == nil || !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("while its acknowledgement was still being stored the connection read %v, "+
			"want it still open: a broker dying now sends the message again", err)
	}

	st.let()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := brokertest.ReadRawPacket(c.conn); err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		t.Fatalf("after the DISCONNECT the connection read %v, want it closed", err)
	}
	if got := tableOf(t, "raw"); len(got) != 0 {
		t.Errorf("when the connection closed the table held %+v", got)
	}
	if pos, ok, _ := lg.Position(store.MQTTReader("raw")); !ok || pos.Offset <= m.Offset {
		t.Errorf("when the connection closed the cursor was %+v (stored %v), want it past %d",
			pos, ok, m.Offset)
	}
}

// MQTT-4.3.3 and RFC 0003 "Broadcast": a PUBREC with a reason of 0x80 or more
// ends the exchange - the client has refused the message and no PUBREL
// follows - so the session lets the message go and its entry leaves the
// table.
func TestAPUBRECThatRefusesTheMessageLetsItGo(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	r := news("a", "refused")
	r.QoS = 2
	m := owe(t, h, lg, r, "raw")
	pub := c.read()
	if pub.FixedHeader.Type != packets.Publish || pub.FixedHeader.Qos != 2 {
		t.Fatalf("the delivery is %+v, want a QoS 2 PUBLISH", pub.FixedHeader)
	}
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pub.PacketID,
		ReasonCode: 0x80})
	eventually(t, "the refused message and its entry let go", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(m.Offset)
		return len(held) == 0 && len(tableOf(t, "raw")) == 0
	})
	if owed, _ := h.B.OwedBroadcast("raw"); len(owed) != 0 {
		t.Errorf("the session still owes %v after refusing it", owed)
	}
}

// ack answers a delivery as its QoS asks: PUBACK, or PUBREC then, on the
// PUBREL, PUBCOMP.
func (c *rawClient) ack(pk packets.Packet) {
	c.t.Helper()
	switch pk.FixedHeader.Type {
	case packets.Pubrel:
		c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubcomp}, PacketID: pk.PacketID})
	case packets.Publish:
		if pk.FixedHeader.Qos == 1 {
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: pk.PacketID})
			return
		}
		c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: pk.PacketID})
		rel := c.read()
		if rel.FixedHeader.Type != packets.Pubrel || rel.PacketID != pk.PacketID {
			c.t.Fatalf("after the PUBREC the broker sent %+v, want the PUBREL for %d", rel.FixedHeader, pk.PacketID)
		}
		c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubcomp}, PacketID: pk.PacketID})
	}
}

// sent is what a raw client reads of one delivery: the packet, its
// identifier, and whether it is a re-send.
type sent struct {
	typ     byte
	id      uint16
	dup     bool
	payload string
}

func sentOf(pk packets.Packet) sent {
	return sent{typ: pk.FixedHeader.Type, id: pk.PacketID, dup: pk.FixedHeader.Dup, payload: string(pk.Payload)}
}

// MQTT-4.4.0-1, RFC 0003 "Broadcast": a session resumed after a link cut is
// sent its unacknowledged messages again under the identifiers they first
// carried, with DUP set; the old connection's going lets none of them go; and
// the new connection's acknowledgements settle them - the message leaves the
// log and its entry the table.
func TestALinkCutResumeIsSentItsMessagesUnderTheirIdentifiers(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	for i := range 3 {
		owe(t, h, lg, news("a", fmt.Sprint("m-", i)), "raw")
	}
	var first []sent
	for range 3 {
		first = append(first, sentOf(c.read()))
	}
	_ = c.conn.Close()

	back, present := connectRaw(t, h, "raw", 10)
	if !present {
		t.Fatal("the session did not resume")
	}
	var again []sent
	for range 3 {
		again = append(again, sentOf(back.read()))
	}
	for i := range first {
		f, a := first[i], again[i]
		if a.typ != packets.Publish || a.id != f.id || a.payload != f.payload || !a.dup || f.dup {
			t.Errorf("delivery %d was %+v and came again as %+v, want the same identifier and payload with DUP set",
				i, f, a)
		}
	}
	if owed, _ := h.B.OwedBroadcast("raw"); len(owed) != 3 {
		t.Fatalf("after the link cut the session owes %v, want all three still", owed)
	}
	if got := tableOf(t, "raw"); len(got) != 3 {
		t.Fatalf("after the link cut the table holds %+v, want all three still", got)
	}
	for _, f := range again {
		back.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: f.id})
	}
	eventually(t, "the new connection's acknowledgements settling all three", 5*time.Second, func() bool {
		return lg.Len() == 0 && len(tableOf(t, "raw")) == 0
	})
}

// MQTT-4.4.0-1, MQTT-4.6.0-1 and RFC 0003 "Broadcast": a session resumed after
// a restart is sent what was on the wire again under the identifiers it first
// carried - each unacknowledged PUBLISH with DUP set, and the PUBREL for an
// exchange that had reached its PUBREC - in the order they were first sent,
// and only then what it was owed and never sent. The acknowledgements settle
// them all. On both providers.
func TestARestartResumeIsSentItsMessagesUnderTheirIdentifiers(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			c := dialRaw(t, h, "raw", "news/#")
			h.B.AttachBroadcast("raw")
			for i := range 2 {
				owe(t, h, lg, news("a", fmt.Sprint("m-", i)), "raw")
			}
			q2 := news("a", "exactly once")
			q2.QoS = 2
			owe(t, h, lg, q2, "raw")
			var first []sent
			for range 3 {
				first = append(first, sentOf(c.read()))
			}
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec}, PacketID: first[2].id})
			if rel := c.read(); rel.FixedHeader.Type != packets.Pubrel || rel.PacketID != first[2].id {
				t.Fatalf("the PUBREC was answered %+v", rel.FixedHeader)
			}
			_ = c.conn.Close()
			sessionGone(t, h, "raw")
			owe(t, h, lg, news("a", "never sent"), "raw")
			h.Stop()

			h = p.start(t)
			lg = attachDrain(t, h)
			back, present := connectRaw(t, h, "raw", 10)
			if !present {
				t.Fatal("the session did not resume")
			}
			var got []sent
			for range 4 {
				pk := back.read()
				got = append(got, sentOf(pk))
				back.ack(pk)
			}
			want := []sent{
				{packets.Publish, first[0].id, true, "m-0"},
				{packets.Publish, first[1].id, true, "m-1"},
				{packets.Pubrel, first[2].id, false, ""},
			}
			for i, w := range want {
				if got[i] != w {
					t.Errorf("after the start delivery %d was %+v, want %+v", i, got[i], w)
				}
			}
			if n := got[3]; n.typ != packets.Publish || n.payload != "never sent" || n.dup {
				t.Errorf("after what was on the wire came %+v, want the one never sent, as new", n)
			}
			eventually(t, "every one settled", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(lg.Next()-4, lg.Next()-3, lg.Next()-2, lg.Next()-1)
				return len(held) == 0 && len(tableOf(t, "raw")) == 0
			})
		})
	}
}

// MQTT-3.3.4-9 and MQTT-4.4.0-1: a session that resumes after a restart
// declaring a smaller Receive Maximum than it had is sent as many of its
// messages on the wire as the new one admits, under their identifiers, and the
// next each time one is acknowledged - never more than the window at once.
func TestAResumeUnderASmallerWindowIsSentWhatFits(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			c := dialRaw(t, h, "raw", "news/#")
			h.B.AttachBroadcast("raw")
			for i := range 3 {
				owe(t, h, lg, news("a", fmt.Sprint("m-", i)), "raw")
			}
			var first []sent
			for range 3 {
				first = append(first, sentOf(c.read()))
			}
			_ = c.conn.Close()
			sessionGone(t, h, "raw")
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			back, present := connectRaw(t, h, "raw", 1)
			if !present {
				t.Fatal("the session did not resume")
			}
			for i, f := range first {
				pk := back.read()
				if got := sentOf(pk); got.id != f.id || !got.dup || got.payload != f.payload {
					t.Fatalf("re-send %d was %+v, want %+v again with DUP", i, got, f)
				}
				if !back.quiet(200 * time.Millisecond) {
					t.Fatal("the connection failed while waiting")
				}
				back.ack(pk)
			}
		})
	}
}

// MQTT-4.6.0-1, MQTT-3.3.4-9 and MQTT-3.3.1-1: a session resumed after a link
// cut declaring a smaller Receive Maximum has what the engine could not send
// at once held back for room and sent as its re-sends, with DUP; and a message
// it is owed afterwards is not sent ahead of those: it comes after the last of
// them, as new.
func TestALinkCutResumeUnderASmallerWindowKeepsItsOrder(t *testing.T) {
	h, lg, _ := drainHarness(t)
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	for i := range 3 {
		owe(t, h, lg, news("a", fmt.Sprint("m-", i)), "raw")
	}
	var first []sent
	for range 3 {
		first = append(first, sentOf(c.read()))
	}
	_ = c.conn.Close()

	back, present := connectRaw(t, h, "raw", 1)
	if !present {
		t.Fatal("the session did not resume")
	}
	owe(t, h, lg, news("a", "later"), "raw")
	// All three again with DUP: the two the window did not admit at once were
	// written before the cut too, so each is a re-send when its turn comes
	// [MQTT-3.3.1-1].
	for i, f := range first {
		pk := back.read()
		if got := sentOf(pk); got.id != f.id || !got.dup || got.payload != f.payload {
			t.Fatalf("delivery %d after the resume was %+v, want %+v again with DUP", i, got, f)
		}
		back.ack(pk)
	}
	if got := sentOf(back.read()); got.payload != "later" || got.dup {
		t.Fatalf("after what was on the wire came %+v, want the later message, as new", got)
	}
}

// MQTT-3.1.2-4 and MQTT-4.4.0-1: a connection with Clean Start 1 begins a new
// session, so nothing of the one it ended is sent to it again, after a
// restart as after a link cut.
func TestACleanStartIsSentNothingAgain(t *testing.T) {
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			c := dialRaw(t, h, "raw", "news/#")
			h.B.AttachBroadcast("raw")
			owe(t, h, lg, news("a", "on the wire"), "raw")
			c.read()
			_ = c.conn.Close()
			sessionGone(t, h, "raw")
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			back, present := connectRawAs(t, h, "raw", 10, true)
			if present {
				t.Fatal("a clean start was told its session was present")
			}
			if !back.quiet(300 * time.Millisecond) {
				t.Fatal("the connection failed while waiting")
			}
		})
	}
}

// refusedInFlight refuses the in-flight writes of one client's drain, so what
// that session is owed stays owed and unsent.
type refusedInFlight struct {
	drainSessions
	client string
}

func (r *refusedInFlight) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	if client == r.client {
		return fmt.Errorf("refused for the test")
	}
	return r.drainSessions.SetInFlightAll(client, window, fs)
}

// RFC 0003 "Retained messages" and "Sessions", MQTT-4.4.0-1: a retained value
// sent on subscribe at QoS 1 to a session that outlives its connection, and
// not acknowledged, is sent again after a restart - under its identifier,
// with DUP, and with the RETAIN flag a delivery on subscribe carries - and
// one the session was owed and not yet sent is sent then. It is owed to that
// session alone: a durable "#" subscriber that took its own copy earlier,
// and a durable group over the same topics, are owed nothing of it.
func TestARetainedValueASessionHadNotAcknowledgedComesBackAfterARestart(t *testing.T) {
	n := int64(1 << 20)
	brokertest.Retaining = &n
	t.Cleanup(func() { brokertest.Retaining = nil })
	const group = "$share/g/loose/#"
	for _, sent := range []bool{true, false} {
		for _, p := range restartables(t) {
			name := map[bool]string{true: "sent and unacknowledged", false: "owed and not yet sent"}[sent]
			t.Run(name+"/"+p.name, func(t *testing.T) {
				h := p.start(t)
				if !sent {
					attachDrainThrough(t, h, func(s drainSessions) drainSessions {
						return &refusedInFlight{drainSessions: s, client: "d"}
					})
				}
				connect(t, h, "publisher", true, false).PubRetainedAt(t, "loose/r", "retained", 1)
				watcher := dial(t, h, "watcher", false, false, 0, 3600, 0)
				watcher.Sub(t, "#", 1)
				eventually(t, "the watcher is sent its own copy", 3*time.Second, func() bool { return watcher.Count() == 1 })
				// Its acknowledgement read, so the clean DISCONNECT below stores
				// it before the connection closes (invariant 18).
				acknowledged(t, watcher)
				gone := h.Disconnects.Snapshot("watcher")
				watcher.Close()
				sessionGoneAfter(t, h, "watcher", gone)
				awayMember(t, h, "member", group, 1)

				w, _ := windowDial(t, h.Addr, "d", false, 10)
				w.Subscribe("loose/r", 1)
				var id uint16
				if sent {
					for id == 0 {
						if pub, ok := w.Next(3 * time.Second).Content.(*pahopackets.Publish); ok {
							id = pub.PacketID
						}
					}
				} else if cp, err := w.Read(300 * time.Millisecond); err == nil {
					if _, ok := cp.Content.(*pahopackets.Publish); ok {
						t.Fatal("d was sent the value with its in-flight writes refused, so the arm is not the one named")
					}
				}
				w.Close()
				sessionGone(t, h, "d")
				if p.name == "sqlite" {
					h.Crash()
				} else {
					h.Stop()
				}

				h2 := p.start(t)
				back, ca := windowDial(t, h2.Addr, "d", false, 10)
				if !ca.SessionPresent {
					t.Fatal("d's session did not come back, so this says nothing about what it is owed")
				}
				var got *pahopackets.Publish
				for deadline := time.Now().Add(3 * time.Second); got == nil && time.Now().Before(deadline); {
					cp, err := back.Read(time.Until(deadline))
					if err != nil {
						break
					}
					got, _ = cp.Content.(*pahopackets.Publish)
				}
				if got == nil {
					t.Fatal("d was sent nothing after the restart: the retained value it was owed is lost")
				}
				if string(got.Payload) != "retained" || !got.Retain || got.QoS != 1 {
					t.Errorf("d was sent %q retain %v QoS %d, want the retained value, RETAIN set, at QoS 1",
						got.Payload, got.Retain, got.QoS)
				}
				if sent && (!got.Duplicate || got.PacketID != id) {
					t.Errorf("d was sent it again under id %d with DUP %v, want id %d with DUP [MQTT-4.4.0-1]",
						got.PacketID, got.Duplicate, id)
				}
				if owed, _ := h2.B.OwedBroadcast("watcher"); len(owed) != 0 {
					t.Errorf("the watcher is owed %v from the log after the restart: d's copy was counted for it", owed)
				}
				if n := backlogLen(t, h2, group); n != 0 {
					t.Errorf("the group holds %d after the restart: d's copy was counted for it", n)
				}
			})
		}
	}
}

// RFC 0003 "Broadcast": a subscription made later does not reach back to a
// message already in the log, and a restart sends a session again only what
// it had not acknowledged. One publish is held between its append and its
// count while 5,000 more are counted - a publisher descheduled there while
// the others go on - and then counted. The drain's count carries through all
// of them: a session subscribing after them is owed none, after a restart as
// before it. Past 4,096 counted above the gap the count used to stop for
// good, and after a restart that session was sent all 5,001.
func TestALateCountHoldsNothingBackOnceItLands(t *testing.T) {
	const n = 5000
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			awaySession := dial(t, h, "a", false, false, 0, 3600, 0)
			awaySession.Sub(t, "load/#", 1)
			gone := h.Disconnects.Snapshot("a")
			awaySession.Close()
			sessionGoneAfter(t, h, "a", gone)

			count := h.B.AppendUncounted("load/held", "held", "a")
			pub := connect(t, h, "pub", true, false)
			for i := range n {
				pub.Pub(t, "load/x", fmt.Sprint(i))
			}
			before := h.B.CountedThrough()
			count()
			if got := h.B.CountedThrough(); before != 0 || got != n+1 {
				t.Fatalf("the count stood at %d with the first message uncounted, and at %d once it was counted, "+
					"want 0 and %d", before, got, n+1)
			}

			late := dial(t, h, "late", false, false, 0, 3600, 0)
			late.Sub(t, "load/#", 1)
			gone = h.Disconnects.Snapshot("late")
			late.Close()
			sessionGoneAfter(t, h, "late", gone)
			if p.name == "sqlite" {
				h.Crash()
			} else {
				h.Stop()
			}

			h = p.start(t)
			back := dial(t, h, "late", false, false, 0, 3600, 0)
			got := 0
			for {
				if _, ok := back.Await(t, time.Second); !ok {
					break
				}
				got++
			}
			if got != 0 {
				t.Errorf("after the restart a session that subscribed after %d messages were published was sent "+
					"%d of them", n+1, got)
			}
		})
	}
}

// errInjected is a disk failing, as a store would report one.
var errInjected = errors.New("input/output error (injected)")

// failingDisconnects is a session store that cannot record a disconnect.
type failingDisconnects struct {
	broker.SessionStore
	calls atomic.Int32
}

func (s *failingDisconnects) Disconnected(string, time.Time, bool, uint32) error {
	s.calls.Add(1)
	return errInjected
}

// failingAppends is a broadcast log that cannot take a record.
type failingAppends struct {
	drainReads
	calls *atomic.Int32
}

func (l failingAppends) Append(store.Record) (store.Record, error) {
	l.calls.Add(1)
	return store.Record{}, errInjected
}

// RFC 0005 saguin_storage_errors_total: "Storage calls that failed".
//
// **Every store the broker holds, not only a channel's.** Only the stores
// SetStores attaches were counted by a wrapper, so a session store that
// could not record a disconnect, and a broadcast log that could not take a
// record, reached no series at all: 56 of the broker's 137 failing store
// calls. Each failure here is injected
// beneath the counting wrapper, where a real disk failure comes from.
func TestAFailureOfAStoreBehindSessionsIsCounted(t *testing.T) {
	t.Run("the session store", func(t *testing.T) {
		fs := &failingDisconnects{}
		brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
			fs.SessionStore = s
			return fs
		}
		t.Cleanup(func() { brokertest.WrapSessions = nil })
		h := start(t)
		ops := operationsAt(t, h)
		c := connectDurable(t, h, "leaver")
		c.Close()
		sessionGone(t, h, "leaver")
		if fs.calls.Load() == 0 {
			t.Fatal("Disconnected was never called, so nothing below is about the class")
		}
		if n := seriesSum(scrapeGauges(t, ops), "saguin_storage_errors_total"); n == 0 {
			t.Error("the session store failed to record a disconnect and saguin_storage_errors_total " +
				"has no series")
		}
	})
	t.Run("the broadcast log", func(t *testing.T) {
		h := start(t)
		ops := operationsAt(t, h)
		calls := new(atomic.Int32)
		attachDrainWith(t, h, func(l drainReads) drainReads { return failingAppends{l, calls} },
			func(s drainSessions) drainSessions { return s })
		c := brokertest.Dial(t, h, "listener", false, false, 10, 2, 0)
		c.Sub(t, "news/#", 1)
		h.B.AttachBroadcast("listener")
		p := connect(t, h, "pub", true, false)
		resp, _ := p.C.Publish(context.Background(), &paho.Publish{Topic: "news/x", QoS: 1, Payload: []byte("x")})
		if calls.Load() == 0 {
			t.Fatal("the broadcast log was never asked to take the record, so nothing below is " +
				"about the class")
		}
		// 0x83, RFC 0002's code for storage that could not keep the record;
		// it answered 0x80, a code RFC 0002 gives no publish refusal of this
		// kind.
		if resp == nil || resp.ReasonCode != 0x83 {
			t.Errorf("a broadcast publish the log could not keep was answered %v, want 0x83", resp)
		}
		if n := seriesSum(scrapeGauges(t, ops), "saguin_storage_errors_total"); n == 0 {
			t.Error("the broadcast log failed an append and saguin_storage_errors_total has no series")
		}
	})
}

// countsRemoves counts the log's removals, so a test can see how the
// released messages were gathered into them.
type countsRemoves struct {
	drainReads
	calls atomic.Int64
}

func (l *countsRemoves) Remove(offsets ...uint64) (int, int64, error) {
	l.calls.Add(1)
	return l.drainReads.Remove(offsets...)
}

// unowed keeps n messages nobody is owed, each released as it is counted,
// every gap apart.
func unowed(t *testing.T, h *brokertest.Harness, n int, gap time.Duration) []uint64 {
	t.Helper()
	offs := make([]uint64, n)
	for i := range n {
		time.Sleep(gap)
		got, kept, err := h.B.KeepBroadcast(news("nobody", fmt.Sprint(i)))
		if err != nil || !kept {
			t.Fatalf("message %d: kept %v (%v)", i, kept, err)
		}
		offs[i] = got.Offset
	}
	return offs
}

// removedAll reports whether the log holds none of offs.
func removedAll(t *testing.T, lg drainLog, offs []uint64) bool {
	t.Helper()
	recs, err := lg.ReadAt(offs...)
	if err != nil {
		t.Fatalf("read the log: %v", err)
	}
	return len(recs) == 0
}

// **Messages nobody owes are removed together, one removal an
// ack_commit_interval**, rather than a transaction each: a resumed
// session's drain paid about one for every two messages it delivered
// (bdrain.release). Twenty released 5ms apart, inside one interval, leave
// in at most a few removals, and all of them leave. Released one at a time,
// as they were, each was its own.
func TestReleasedMessagesLeaveTheLogTogether(t *testing.T) {
	h := start(t)
	h.B.SetAckCommitInterval(time.Second)
	lg := &countsRemoves{}
	lgv := attachDrainWith(t, h, func(l drainReads) drainReads { lg.drainReads = l; return lg },
		func(s drainSessions) drainSessions { return s })
	offs := unowed(t, h, 20, 5*time.Millisecond)
	eventually(t, "the twenty released messages gone from the log", 5*time.Second, func() bool { return removedAll(t, lgv, offs) })
	if n := lg.calls.Load(); n > 3 {
		t.Errorf("twenty messages released inside one interval took %d removals, want at most 3", n)
	}
	t.Logf("20 released messages removed in %d removals", lg.calls.Load())
}

// **A full batch does not wait out the interval**: releaseBatch released
// messages are removed at once, however long ack_commit_interval is.
func TestAFullBatchOfReleasedMessagesLeavesAtOnce(t *testing.T) {
	h, lg, _ := drainHarness(t)
	h.B.SetAckCommitInterval(time.Hour)
	offs := unowed(t, h, broker.ReleaseBatch, 0)
	eventually(t, "a full batch of released messages gone from the log", 5*time.Second, func() bool { return removedAll(t, lg, offs) })
}

// **A stopping broker does not wait for its releases**: Shutdown ends the
// wait and the removal happens before it returns, so the snapshot it writes
// holds nothing nobody owes and it is not held up by ack_commit_interval.
func TestAStoppingBrokerRemovesWhatItWasGathering(t *testing.T) {
	h, lg, _ := drainHarness(t)
	// Long enough that a Shutdown waiting it out fails the test, short
	// enough that the cleanup behind such a failure still ends.
	h.B.SetAckCommitInterval(30 * time.Second)
	offs := unowed(t, h, 10, 0)
	// The release is waiting out its interval, which a stop that came first
	// would have found with nothing to cut short.
	awaitBlockedIn(t, "(*bdrain).release.func1", "select")
	if held := heldOf(t, lg, offs); len(held) != len(offs) {
		t.Fatalf("%d of the ten released messages are still in the log, want all ten: nothing was "+
			"gathering for the stop to end", len(held))
	}
	stopped := make(chan struct{})
	go func() { h.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown had not returned after 5s: it waited out ack_commit_interval")
	}
	if !removedAll(t, lg, offs) {
		t.Error("Shutdown returned with messages nobody owes still in the log")
	}
}

// Invariant 18, RFC 0003 "Broadcast": a graceful stop is not an unclean one,
// so a broadcast delivery a session acknowledged before it is not sent again
// at the next start. The acknowledgement is forced to arrive after the
// drain's last flush and before it looks for more (SetDrainBeforeLookAgain),
// and the stop to close the connection in that window: the drain then stops
// at the closed connection with the acknowledgement taken and unwritten.
// Both providers keep a session's broadcast across a graceful stop - sqlite,
// and memory with a snapshot_dir.
func TestAnAcknowledgementTakenAsAGracefulStopClosesIsStored(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			dir := t.TempDir()
			start := func() *harness {
				if provider == "sqlite" {
					return startDurableSQLite(t, filepath.Join(dir, "sessions.db"))
				}
				return startDurable(t, dir)
			}
			h := start()
			// Away and owed the delivery too, so the log keeps it across the
			// stop, and a session whose acknowledgement was lost is sent it.
			w := dial(t, h, "watcher", false, false, 0, 3600, 0)
			w.Sub(t, "own/#", 1)
			before := h.Disconnects.Snapshot("watcher")
			w.Close()
			sessionGoneAfter(t, h, "watcher", before)
			s := dial(t, h, "sub", false, true, 10, 3600, 0)
			s.Sub(t, "own/#", 1)
			p := connect(t, h, "producer", true, false)
			var fired atomic.Bool
			stopped := make(chan struct{})
			problem := make(chan string, 1)
			restore := broker.SetDrainBeforeLookAgain(func(client string) {
				if client != "sub" {
					return
				}
				// The pass that sent the delivery: it is on the session's list.
				if owed, _ := h.B.OwedBroadcast("sub"); len(owed) == 0 || !fired.CompareAndSwap(false, true) {
					return
				}
				if !waitUntil(5*time.Second, func() bool { return s.Count() == 1 }) {
					problem <- fmt.Sprintf("the subscriber was sent %q, want the delivery", s.Payloads())
				}
				ackAll(s)
				// Taken: an acknowledged delivery leaves the session's list.
				if !waitUntil(5*time.Second, func() bool { owed, _ := h.B.OwedBroadcast("sub"); return len(owed) == 0 }) {
					owed, _ := h.B.OwedBroadcast("sub")
					problem <- fmt.Sprintf("the PUBACK was never taken: the session still owes %v", owed)
				}
				// The connection this drain serves, taken before the stop.
				cl, ok := h.Srv.Clients.Get("sub")
				go func() { h.Stop(); close(stopped) }()
				if !ok || !waitUntil(5*time.Second, cl.Closed) {
					problem <- fmt.Sprintf("the stop never closed the subscriber's connection (registered %v)", ok)
				}
			})
			defer restore()
			p.Pub(t, "own/x", "acknowledged")
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				t.Fatalf("the broker did not stop; the window fired %v, the subscriber was sent %q",
					fired.Load(), s.Payloads())
			}
			restore()
			select {
			case why := <-problem:
				t.Fatal(why)
			default:
			}
			if !fired.Load() {
				t.Fatal("the window after the drain's last flush was never opened, so this proves nothing")
			}

			h2 := start()
			// What the store kept of the session's window: nothing, the one
			// delivery having been acknowledged before the stop.
			kept, ok := brokertest.HarnessSessions.(interface {
				InFlight(client string) (uint16, []store.InFlight, error)
			})
			if !ok {
				t.Fatalf("the session store %T cannot be asked what it keeps in flight", brokertest.HarnessSessions)
			}
			if _, fs, err := kept.InFlight("sub"); err != nil || len(fs) != 0 {
				t.Errorf("the restarted broker's store keeps %+v (%v) in flight for the session: its "+
					"acknowledgement was not stored at the graceful stop", fs, err)
			}
			back := dial(t, h2, "sub", false, false, 0, 3600, 0)
			p2 := connect(t, h2, "producer", true, false)
			p2.Pub(t, "own/x", "after")
			eventually(t, "the publish after the restart", 5*time.Second, func() bool {
				return slices.Contains(back.Payloads(), "after")
			})
			if got := back.Payloads(); !slices.Equal(got, []string{"after"}) {
				t.Errorf("the session was sent %q after a graceful stop, want only what was published "+
					"since: a delivery it acknowledged before the stop was sent again", got)
			}
		})
	}
}

// keepsErrors is a test's T that keeps what the harness reports with Errorf
// rather than failing on it, so a test can read an error the harness's stop
// reports - Shutdown's - as its result.
type keepsErrors struct {
	testing.TB
	mu   sync.Mutex
	errs []string
}

func (k *keepsErrors) Errorf(format string, args ...any) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.errs = append(k.errs, fmt.Sprintf(format, args...))
}

func (k *keepsErrors) reported() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.errs)
}

// Invariant 18: a graceful stop whose store refuses the last write of a
// session's broadcast acknowledgements says so. Nothing is left to write
// them again - the drains have stopped, and a sqlite provider keeps no
// snapshot - so a Shutdown that answered nil told the operator the stop was
// clean while the next start sends those messages again. The store refuses
// every acknowledgement from the start, so the one taken is still unwritten
// when the broker stops.
func TestAGracefulStopWhoseAcknowledgementsTheStoreRefusesFails(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			kt := &keepsErrors{TB: t}
			var h *harness
			if provider == "sqlite" {
				h = startDurableSQLite(kt, filepath.Join(t.TempDir(), "sessions.db"))
			} else {
				h = startDurable(kt, t.TempDir())
			}
			st := new(refusesAcknowledgements)
			attachDrainThrough(t, h, func(s drainSessions) drainSessions {
				st.drainSessions = s
				return st
			})
			st.armed.Store(true)
			s := dial(t, h, "sub", false, false, 10, 3600, 0)
			s.Sub(t, "own/#", 1)
			p := connect(t, h, "producer", true, false)
			p.Pub(t, "own/x", "acknowledged")
			eventually(t, "the store refusing the delivery's acknowledgement", 5*time.Second, func() bool {
				return s.Count() == 1 && st.refused.Load() > 0
			})
			h.Stop()
			got := kt.reported()
			if len(got) != 1 || !strings.HasPrefix(got[0], "shutdown: ") ||
				!strings.Contains(got[0], "refused the acknowledgements of 1 sessions") {
				t.Fatalf("the stop reported %q, want Shutdown's error naming the one session whose "+
					"acknowledgements the store refused (the store refused %d writes)", got, st.refused.Load())
			}
		})
	}
}
