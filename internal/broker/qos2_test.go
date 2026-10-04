package broker_test

// Exactly-once publishing, driven packet by packet.
//
// **Every test here builds its own packets**, because no client library
// will do what these have to do: leave a handshake unfinished, send the
// same publish twice, or send a release for a message the broker never
// had. Paho completes the exchange for you, which is exactly the behaviour
// under test.
//
// The oracles come from RFC 0002 `broker.qos2`, RFC 0003, and the MQTT
// statements they cite. None of them is written from what the code does.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	mqttpackets "github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/store"
)

// RFC 0002 `broker.qos2`: the record is written when the release arrives,
// and not before.
//
// **This is the whole design in one test.** Storing on the first packet is
// what every version before this did, and it is what makes a restart
// mid-exchange leave the record kept while the publisher believes the
// publish failed.
func TestAnExactlyOncePublishIsStoredOnItsRelease(t *testing.T) {
	eachProvider(t, func(t *testing.T, h *harness) {
		w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
		defer w.Close()

		w.Publish(7, "events/orders/1", "order-1", false)
		if rc := w.Pubrec(7); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}

		// Nothing yet. The broker has taken ownership and written nothing -
		// and inChannel reads to an end marker published after this, so a
		// record stored at the PUBLISH is one it would count.
		if n := inChannel(t, h, "auditor-early", "order-1"); n != 0 {
			t.Errorf("the channel holds the record %d time(s) before the release: storing on the "+
				"first packet is what leaves a record kept when the exchange never finishes", n)
		}

		w.Pubrel(7)
		if rc := w.Pubcomp(7); rc != 0 {
			t.Fatalf("the completion carried 0x%02X, want success", rc)
		}
		// The PUBCOMP is sent once the record is stored (invariant 18), so
		// the channel is read straight after it.
		if n := inChannel(t, h, "auditor-late", "order-1"); n != 1 {
			t.Errorf("the channel holds the record %d time(s) after the release, want 1", n)
		}
	})
}

// An exchange the client never finishes stores nothing, and the message is
// dropped when its session ends.
//
// The publisher is not told, and cannot be: it has had its receipt, and
// MQTT gives a server no way to withdraw one. saguin_qos2_abandoned_total
// is where an operator sees it.
func TestAnUnfinishedExchangeStoresNothing(t *testing.T) {
	eachProvider(t, func(t *testing.T, h *harness) {
		w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)

		w.Publish(9, "events/orders/2", "order-2", false)
		if rc := w.Pubrec(9); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}
		w.Close() // the link goes, with no release and no session to come back to

		sessionGone(t, h, "sensor-1")
		if n := inChannel(t, h, "auditor", "order-2"); n != 0 {
			t.Errorf("the channel holds %d record(s) for an exchange the client never finished", n)
		}
	})
}

// **The defect this whole item is about.** A restart between the receipt
// and the completion used to leave the record stored while the publisher
// believed the publish had failed - so the application sent it again and
// the channel held it twice, with nothing on the wire saying so.
//
// **The exchange is now finished rather than abandoned.** The session comes
// back at the start and the publish waiting for its release comes back with
// it, so the client that reconnects is answered Session Present 1, re-sends
// what it never had answered, and the record is written once by the exchange
// that started before the restart. The publisher is told the truth about a
// message the broker took ownership of, which is what exactly-once is for -
// where before, the broker owned the message and told the publisher it had
// failed, and invariant 10 left the count to the broker because the
// application would send it again.
//
// It re-sends the PUBLISH before the PUBREL, which is the shape of a client
// that never saw the PUBREC: [MQTT-4.3.3-10] makes the repeat another receipt
// and no second delivery.
func TestARestartMidExchangeLeavesOneRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "qos2.db")
	h := startDurableSQLite(t, path)

	w, _ := qos2Dial(t, h.Addr, "sensor-1", false, 300)
	w.Publish(11, "events/orders/3", "order-3", false)
	if rc := w.Pubrec(11); rc != 0 {
		t.Fatalf("the receipt carried 0x%02X, want success", rc)
	}
	w.Close()
	sessionGone(t, h, "sensor-1")
	h.Crash()

	h2 := startDurableSQLite(t, path)
	again, ca := qos2Dial(t, h2.Addr, "sensor-1", false, 300)
	defer again.Close()
	if !ca.SessionPresent {
		t.Fatal("the restarted broker told the client its session was gone, so the exchange " +
			"it is holding for that session can never be finished")
	}

	// **The receipt is re-sent first**, which is what the substrate does for
	// any resumed session holding an exchange mid-flight, restored or not:
	// the client is told again what it may not have heard. Read here so the
	// rest of this reads the answers to what it sends, and read with its own
	// message because silence here is the failure this test is about - a
	// session that came back without the exchange it was in the middle of.
	cp, err := again.Read(3 * time.Second)
	if err != nil {
		t.Fatalf("the resumed session sent nothing (%v): its exchange did not come back with "+
			"it, so the client holds a publish this broker will refuse to finish", err)
	}
	rec, ok := cp.Content.(*packets.Pubrec)
	if !ok {
		t.Fatalf("the resumed session sent a %s, want the receipt it owes", cp.PacketType())
	}
	if rec.PacketID != 11 || rec.ReasonCode != 0 {
		t.Fatalf("the receipt re-sent on the resume is id %d carrying 0x%02X, want 11 and success",
			rec.PacketID, rec.ReasonCode)
	}

	// And the client re-sends what it never had answered. A publisher that
	// never saw the first receipt sends the PUBLISH again [MQTT-4.4.0-1],
	// and the repeat is another receipt rather than a second delivery
	// [MQTT-4.3.3-10].
	again.Publish(11, "events/orders/3", "order-3", true)
	if rc := again.Pubrec(11); rc != 0 {
		t.Fatalf("the receipt after the restart carried 0x%02X, want success", rc)
	}
	again.Pubrel(11)
	if rc := again.Pubcomp(11); rc != 0 {
		t.Fatalf("the completion after the restart carried 0x%02X, want success: the broker "+
			"holds the message and the session, so the release finishes the exchange", rc)
	}

	if n := inChannel(t, h2, "auditor", "order-3"); n != 1 {
		t.Errorf("the channel holds the record %d time(s) across a restart, want 1. Two is the "+
			"defect this store exists to remove: the record kept, the publisher told it "+
			"failed, and the application sending it again", n)
	}
}

// RFC 0002 `broker.qos2.max_inflight_per_client`, and invariant 13:
// enforcement happens before allocation, and the outcome at a bound is a
// reason code rather than making room.
//
// **The connection stays up**, which is the half worth testing. A publisher
// that pipelines a little too eagerly is inside a bound rather than
// misbehaving, and disconnecting it would turn that into a reconnect loop.
func TestAPublisherAtItsAllowanceIsRefusedAndStaysConnected(t *testing.T) {
	brokertest.QoS2Inflight = 2
	t.Cleanup(func() { brokertest.QoS2Inflight = 0 })
	eachProvider(t, func(t *testing.T, h *harness) {
		publisherAtItsAllowance(t, h)
	})
}

func publisherAtItsAllowance(t *testing.T, h *harness) {
	w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
	defer w.Close()

	for i := uint16(1); i <= 2; i++ {
		w.Publish(i, "events/orders/x", fmt.Sprintf("held-%d", i), false)
		if rc := w.Pubrec(i); rc != 0 {
			t.Fatalf("receipt %d carried 0x%02X, want success", i, rc)
		}
	}

	w.Publish(3, "events/orders/x", "over", false)
	if rc := w.Pubrec(3); rc != 0x97 {
		t.Fatalf("the publish past the allowance was answered 0x%02X, want 0x97 quota exceeded", rc)
	}
	// **Counted under its own name** (RFC 0005 saguin_publish_refused_total):
	// the allowance is split out of `quota exceeded`, a client to look at
	// where a full channel is storage to look at. No test read it.
	const series = `saguin_publish_refused_total{reason="inflight allowance exceeded"}`
	if got := metricValue(t, operationsAt(t, h), series); got != 1 {
		t.Errorf("%s is %v after one publish past the allowance, want 1", series, got)
	}

	// **Still connected, and the allowance comes back.** Releasing one of
	// its own is what lets the next through, which is the behaviour that
	// makes the refusal survivable rather than terminal.
	w.Pubrel(1)
	if rc := w.Pubcomp(1); rc != 0 {
		t.Fatalf("the completion carried 0x%02X, want success: the connection did not survive "+
			"the refusal", rc)
	}
	w.Publish(4, "events/orders/x", "after", false)
	if rc := w.Pubrec(4); rc != 0 {
		t.Errorf("after releasing one, the next publish was answered 0x%02X, want success: "+
			"the allowance is not coming back", rc)
	}

	if n := inChannel(t, h, "auditor", "over"); n != 0 {
		t.Errorf("the refused publish was stored %d time(s): a bound that stores and then "+
			"refuses has taken ownership of a message it said it would not", n)
	}
	// The one the bound must not have eaten. Making room by dropping a held
	// message discards one already claimed at its receipt.
	if n := inChannel(t, h, "auditor2", "held-1"); n != 1 {
		t.Errorf("the first held publish is in the channel %d time(s) after being released, "+
			"want 1: the bound made room by dropping it", n)
	}
}

// RFC 0002 `broker.qos2.expires_after`. A held message that nobody releases
// is dropped, and the release that arrives afterwards is answered
// `0x92 Packet Identifier not found` - which is the honest answer, because
// there is no message left to write.
func TestAHeldPublishExpiresAndItsReleaseIsAnsweredNotFound(t *testing.T) {
	brokertest.QoS2ExpiresAfter = time.Second
	t.Cleanup(func() { brokertest.QoS2ExpiresAfter = 0 })
	eachProvider(t, func(t *testing.T, h *harness) {
		heldPublishExpires(t, h)
	})
}

func heldPublishExpires(t *testing.T, h *harness) {
	w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
	defer w.Close()

	w.Publish(13, "events/orders/4", "order-4", false)
	if rc := w.Pubrec(13); rc != 0 {
		t.Fatalf("the receipt carried 0x%02X, want success", rc)
	}

	// Past the age, plus the tick the sweep runs on.
	time.Sleep(1500 * time.Millisecond)

	w.Pubrel(13)
	if rc := w.Pubcomp(13); rc != 0x92 {
		t.Errorf("the release of an expired publish was answered 0x%02X, want 0x92 packet "+
			"identifier not found", rc)
	}
	if n := inChannel(t, h, "auditor", "order-4"); n != 0 {
		t.Errorf("an expired publish was stored %d time(s) by its late release", n)
	}
}

// A release for an identifier this broker never held is answered
// `0x92`, and stores nothing. It is what a client gets after a restart, and
// what it gets if it invents one.
func TestAReleaseForNothingHeldIsAnsweredNotFound(t *testing.T) {
	eachProvider(t, func(t *testing.T, h *harness) {
		w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
		defer w.Close()

		w.Pubrel(99)
		if rc := w.Pubcomp(99); rc != 0x92 {
			t.Errorf("a release naming nothing held was answered 0x%02X, want 0x92", rc)
		}
	})
}

// Every channel type takes an exactly-once publish, and a broadcast topic
// does too: each exchange completes. What a broadcast's hold and release do
// is TestAQoS2BroadcastIsDeliveredAtItsReleaseAndOnce's.
func TestExactlyOnceReachesEveryDestination(t *testing.T) {
	eachProvider(t, func(t *testing.T, h *harness) {
		w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
		defer w.Close()

		for i, topic := range []string{
			"events/orders/5", // append
			"state/device/1",  // latest
			"jobs/work/1",     // queue
			"anything/else",   // broadcast, which no channel claims
		} {
			w.Exchange(uint16(20+i), topic, fmt.Sprintf("payload-%d", i))
		}
	})
}

// [MQTT-3.2.2-9] and section 3.2.2.3.4: the Maximum QoS property carries
// only 0 or 1, and *"If the Maximum QoS is absent, the Client uses a
// Maximum QoS of 2."* So a broker offering exactly-once says so by not
// sending the property, and one that sent a 2 would be a protocol error.
func TestABrokerOfferingExactlyOnceSendsNoMaximumQoS(t *testing.T) {
	h := start(t)
	_, ca := qos2Dial(t, h.Addr, "sensor-1", true, 0)
	if ca.Properties.MaximumQOS != nil {
		t.Errorf("the CONNACK carried Maximum QoS %d; a broker offering exactly-once sends "+
			"the property not at all, and the value 2 is a protocol error",
			*ca.Properties.MaximumQOS)
	}
}

// Invariant 7 says the visibility deadline starts at the transport
// acknowledgement, and names the `PUBACK`. Raising the ceiling to 2 must
// not move that clock, and it does not: saguin offers a queue record at
// QoS 1 whatever the worker asked for, so the acknowledgement is a `PUBACK`
// and the invariant is untouched.
//
// **QoS 2 on a queue offer would buy nothing**, which is why this is a
// decision rather than an omission. Invariant 6 says transport
// acknowledgement is never application acknowledgement: what resolves a
// queue record is the worker's reply, and a record redelivered after a
// visibility timeout is a duplicate offer by design. Exactly-once on the
// offer would cost a round trip per job and change nothing about how many
// times the job is done.
func TestAQueueOfferIsAtQoS1EvenForAWorkerAskingForTwo(t *testing.T) {
	h := start(t)
	p := connect(t, h, "producer", true, false)

	worker := connect(t, h, "worker", true, false)
	worker.Sub(t, "$saguin/queue/jobs", 2)
	p.Pub(t, "jobs/work/1", "job-1")

	r, ok := worker.Await(t, 3*time.Second)
	if !ok {
		t.Fatal("the worker was offered no job")
	}
	if r.QoS != 1 {
		t.Errorf("the job arrived at QoS %d, want 1: invariant 7 starts the visibility clock "+
			"at the PUBACK, and a QoS 2 offer is acknowledged with a PUBCOMP instead", r.QoS)
	}
}

// A consumer that asks for exactly-once on an `append` channel is now
// granted it, and its position still advances - one packet later, on the
// `PUBCOMP` rather than the `PUBACK`.
//
// **This is the half that would fail silently.** saguin does its own
// in-flight accounting for channel records, and it was written for the
// `PUBACK`. If nothing completed the QoS 2 handshake's accounting, the
// consumer would receive its records and its position would never move, so
// every reconnect would replay the channel from where it first started -
// at-least-once degraded to "always from the beginning", with no error
// anywhere.
func TestAnAppendConsumerAtQoS2AdvancesItsPosition(t *testing.T) {
	h := start(t)
	p := connect(t, h, "producer", true, false)
	for i := 1; i <= 3; i++ {
		p.Pub(t, fmt.Sprintf("events/orders/%d", i), fmt.Sprintf("order-%d", i))
	}
	settle(t, p)

	first := connect(t, h, "reader", false, false)
	sa := first.Sub(t, "events/#", 2)
	if sa.Reasons[0] != 2 {
		t.Fatalf("the subscription was granted QoS %d, want 2: this broker offers "+
			"exactly-once, so it may not grant less than was asked for", sa.Reasons[0])
	}
	for i := 1; i <= 3; i++ {
		r, ok := first.Await(t, 3*time.Second)
		if !ok {
			t.Fatalf("read %d of 3 records at QoS 2", i-1)
		}
		if r.QoS != 2 {
			t.Errorf("record %d arrived at QoS %d, want 2", i, r.QoS)
		}
	}
	time.Sleep(500 * time.Millisecond) // the tick that writes the position
	gone(t, first)

	// Back with the same session. Nothing is replayed, which is only true
	// if the position moved.
	again := connect(t, h, "reader", false, false)
	if extra, ok := again.Await(t, time.Second); ok {
		t.Errorf("the consumer was replayed %q after acknowledging every record at QoS 2: "+
			"its position never advanced, so every reconnect reads the channel again",
			extra.Payload)
	}
}

// [MQTT-3.8.4-8]: a delivery's QoS is the lower of the subscription's and
// the QoS the message was published at. A retained message is a publisher's
// message handed on later, so both halves are real - and saguin kept no
// record of the second until this, so every retained value went out at
// whatever the subscription asked for.
//
// **What that cost is worse than a wrong number in a header.** A value
// published at QoS 0 and delivered at 2 leaves the subscriber holding an
// exactly-once exchange for a message its publisher never asked to be
// tracked, and the exchange is one the broker started.
func TestARetainedMessageIsDeliveredAtTheQoSItWasPublishedAt(t *testing.T) {
	period := int64(0)
	brokertest.Retaining = &period
	t.Cleanup(func() { brokertest.Retaining = nil })
	h := start(t)

	p := connect(t, h, "producer", true, false)
	for qos := byte(0); qos <= 2; qos++ {
		p.PubRetainedAt(t, fmt.Sprintf("weather/oslo/%d", qos), fmt.Sprintf("v%d", qos), qos)
	}
	settle(t, p)

	// Subscribed at the highest, so nothing here is capped by the
	// subscription and every reading is the publisher's own.
	c := connect(t, h, "reader", true, false)
	c.Sub(t, "weather/oslo/#", 2)

	got := map[string]byte{}
	for range 3 {
		r, ok := c.Await(t, 3*time.Second)
		if !ok {
			break
		}
		got[r.Payload] = r.QoS
	}
	for qos := byte(0); qos <= 2; qos++ {
		want, name := qos, fmt.Sprintf("v%d", qos)
		if got[name] != want {
			t.Errorf("the value published at QoS %d came back at QoS %d. All three: %v",
				want, got[name], got)
		}
	}
}

// **A memory provider's bound holds unreleased exactly-once publishes too**
// (invariant 13). The QoS 2 store sat outside the channel registry, and the
// loop that hands each store its provider's quota walked only the registry:
// 64 unreleased publishes of 32KiB (2MiB) were all received on a provider
// bounded at 512KiB, and a channel on it went on accepting while they were
// held. Measured on bin/saguin before the fix; a sqlite provider already
// refused, its file being bounded by SQLite.
//
// Run unbounded as the control, so a refusal here is the quota's and not a
// per-client allowance or a harness limit.
func TestUnreleasedQoS2PublishesCountAgainstAMemoryProvider(t *testing.T) {
	const size, sent = 32 * 1024, 16 // 512KiB offered
	for _, bound := range []int64{0, 256 * 1024} {
		t.Run(fmt.Sprintf("max_bytes=%d", bound), func(t *testing.T) {
			brokertest.QoS2Inflight = 1000
			t.Cleanup(func() { brokertest.QoS2Inflight = 0 })
			h := startDurable(t, t.TempDir()) // names the memory provider `local`
			h.B.SetQuotas(map[string]*store.Quota{"local": store.NewQuota(bound, 0)})

			w, _ := qos2Dial(t, h.Addr, "qos2-holder", true, 300)
			payload := strings.Repeat("q", size)
			var held, refused int
			for id := uint16(1); id <= sent; id++ {
				w.Publish(id, "events/qos2/held", payload, false)
				switch rc := w.Pubrec(id); rc {
				case 0x00:
					held++
				case 0x97:
					refused++
				default:
					t.Fatalf("PUBREC for publish %d carried 0x%02X", id, rc)
				}
			}

			// A channel publish on the same provider, while those are held.
			pub := connect(t, h, "qos2-neighbour", true, false)
			ack, err := pub.C.Publish(context.Background(), &paho.Publish{
				Topic: "events/qos2/neighbour", QoS: 1, Payload: []byte(payload),
			})
			if ack == nil {
				t.Fatalf("no PUBACK for the channel publish: %v", err)
			}

			if bound == 0 {
				if refused != 0 || ack.ReasonCode != 0 {
					t.Fatalf("an unbounded provider refused %d QoS 2 publishes and answered the channel "+
						"0x%02X, so the bounded run below proves nothing", refused, ack.ReasonCode)
				}
				return
			}
			if refused == 0 {
				t.Fatalf("all %d unreleased publishes (%d bytes) were held on a provider bounded at %d",
					held, held*size, bound)
			}
			if int64(held*size) > bound {
				t.Errorf("%d bytes held past a bound of %d", held*size, bound)
			}
			if ack.ReasonCode != 0x97 {
				t.Errorf("a channel publish on the full provider was answered 0x%02X, want 0x97: the "+
					"unreleased publishes are not in the pool its channels share", ack.ReasonCode)
			}
		})
	}
}

// RFC 0003 "Exactly once", the subscriber's half across a restart: a
// delivery the client answered with a PUBREC leaves the broker owing the
// PUBREL, and a restart does not release it from that. The session comes
// back owing the acknowledgement rather than the message [MQTT-4.3.3-2], and
// the client that reconnects is sent the PUBREL it never had.
//
// **The message must not be sent again**, which is the failure this guards:
// a broker that came back owing the whole delivery would hand the subscriber
// a second copy of a message it had already taken, and exactly-once would be
// at-least-once across every restart.
func TestARestoredSessionIsOwedThePubrelAndNotTheMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "qos2.db")
	h := startDurableSQLite(t, path)

	w, _ := qos2Dial(t, h.Addr, "reader-1", false, 300)
	w.Subscribe("loose/exactly", 2)

	// Published at QoS 2, because a delivery is served at the lower of what
	// was published and what was granted [MQTT-3.8.4-8].
	pw, _ := qos2Dial(t, h.Addr, "producer-1", true, 300)
	pw.Exchange(21, "loose/exactly", "once")
	pw.Close()

	// The delivery arrives, and the subscriber takes it but never completes
	// the exchange.
	cp := w.Next(3 * time.Second)
	pub, ok := cp.Content.(*packets.Publish)
	if !ok {
		t.Fatalf("expected the delivery, got %s", cp.PacketType())
	}
	if pub.QoS != 2 {
		t.Fatalf("the delivery arrived at QoS %d, want 2: this test is about the exchange", pub.QoS)
	}
	id := pub.PacketID
	w.SendPubrec(id)
	if rel := w.Next(3 * time.Second); rel.PacketType() != "PUBREL" {
		t.Fatalf("expected the PUBREL, got %s", rel.PacketType())
	}
	w.Close()
	sessionGone(t, h, "reader-1")

	h.Crash()
	h2 := startDurableSQLite(t, path)

	again, ca := qos2Dial(t, h2.Addr, "reader-1", false, 300)
	defer again.Close()
	if !ca.SessionPresent {
		t.Fatal("the client that came back was told Session Present 0, so nothing below is " +
			"about a restored exchange")
	}
	next := again.Next(3 * time.Second)
	if next.PacketType() != "PUBREL" {
		t.Fatalf("the restored session sent a %s, want the PUBREL it owed: a subscriber that "+
			"answered the message with a PUBREC must not be sent the message again",
			next.PacketType())
	}
	if got := next.Content.(*packets.Pubrel).PacketID; got != id {
		t.Errorf("the PUBREL names id %d, want the %d the delivery carried", got, id)
	}
	again.SendPubcomp(id)

	// And the exchange is over: the session has nothing in flight.
	tables, ok := brokertest.HarnessSessions.(interface {
		InFlight(string) (uint16, []store.InFlight, error)
	})
	if !ok {
		t.Fatalf("the harness's session store is a %T, which keeps no in-flight table", brokertest.HarnessSessions)
	}
	var table []store.InFlight
	if !waitUntil(3*time.Second, func() bool {
		_, table, _ = tables.InFlight("reader-1")
		return len(table) == 0
	}) {
		t.Errorf("the session still has %+v in flight once the exchange is finished", table)
	}
}

// RFC 0004 "Memory, with a snapshot": a memory provider keeps its unfinished
// exactly-once publishes exactly as it keeps its sessions - as far as the
// file it writes on the way out, and no further. The pair travels together on
// purpose: a session restored without the exchange it was in the middle of is
// answered Session Present 1 and then refuses the release its client
// re-sends, which claims a session state the broker does not hold.
//
// So both endings are driven here. A graceful stop keeps both and the
// exchange finishes; a crash keeps neither, the client is told its session is
// gone, and MQTT has it discard the exchange and send again.
func TestAMemoryProviderKeepsAnExchangeExactlyAsFarAsItsSession(t *testing.T) {
	t.Run("a graceful stop keeps both, and the exchange finishes", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)

		w, _ := qos2Dial(t, h.Addr, "sensor-2", false, 300)
		w.Publish(12, "events/orders/4", "order-4", false)
		if rc := w.Pubrec(12); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}
		w.Close()
		sessionGone(t, h, "sensor-2")
		h.Stop()

		h2 := startDurable(t, dir)
		again, ca := qos2Dial(t, h2.Addr, "sensor-2", false, 300)
		defer again.Close()
		if !ca.SessionPresent {
			t.Fatal("the restarted broker told the client its session was gone")
		}
		cp, err := again.Read(3 * time.Second)
		if err != nil {
			t.Fatalf("the resumed session sent nothing (%v): the provider kept the session and "+
				"not the exchange it was in the middle of, so this client is holding a publish "+
				"the broker will refuse to finish", err)
		}
		if rec, ok := cp.Content.(*packets.Pubrec); !ok {
			t.Fatalf("the resumed session sent a %s, want the receipt it owes", cp.PacketType())
		} else if rec.PacketID != 12 || rec.ReasonCode != 0 {
			t.Fatalf("the receipt re-sent on the resume is id %d carrying 0x%02X, want 12 and success",
				rec.PacketID, rec.ReasonCode)
		}
		again.Pubrel(12)
		if rc := again.Pubcomp(12); rc != 0 {
			t.Fatalf("the completion carried 0x%02X, want success: the provider kept the "+
				"session and the message it was holding for it", rc)
		}
		if n := inChannel(t, h2, "auditor-mem-stop", "order-4"); n != 1 {
			t.Errorf("the channel holds the record %d time(s), want 1", n)
		}
	})

	t.Run("a crash keeps neither, and the client is told so", func(t *testing.T) {
		dir := t.TempDir()
		h := startDurable(t, dir)

		w, _ := qos2Dial(t, h.Addr, "sensor-3", false, 300)
		w.Publish(13, "events/orders/5", "order-5", false)
		if rc := w.Pubrec(13); rc != 0 {
			t.Fatalf("the receipt carried 0x%02X, want success", rc)
		}
		h.Crash()

		h2 := startDurable(t, dir)
		again, ca := qos2Dial(t, h2.Addr, "sensor-3", false, 300)
		defer again.Close()
		if ca.SessionPresent {
			t.Fatal("a memory provider reported a session it lost in a crash: the exchange " +
				"behind it is gone, and the client would be refused its release")
		}
		// [MQTT-3.2.2-5]: told its session is gone, the client starts again,
		// and the record lands once.
		again.Exchange(13, "events/orders/5", "order-5")
		if n := inChannel(t, h2, "auditor-mem-crash", "order-5"); n != 1 {
			t.Errorf("the channel holds the record %d time(s), want 1", n)
		}
	})
}

// RFC 0003 "Exactly once, and where the unfinished ones wait": an accepted
// exchange can wait for its channel to make room.
//
// The queue's own max_bytes fills between the PUBREC and the PUBREL. The
// release is refused with DISCONNECT 0x97 and no PUBCOMP, and the message
// stays held, offered to nobody. A worker resolving what the queue holds
// makes room, and the PUBREL the client sends again on its next connection
// completes the exchange: the job is offered once.
//
// **The defect this turns around**: the release took the message out of
// where it waited and then offered it to the channel, which refused it, and
// the PUBREL sent again was answered PUBCOMP 0x00 for a message in no
// channel.
func TestARefusedReleaseStaysHeldUntilItsChannelHasRoom(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			path := ""
			if provider == "sqlite" {
				path = filepath.Join(t.TempDir(), "qos2.db")
			}
			h := startBounded(t, path, map[string]int64{"jobs": 16 * 1024})
			payload := strings.Repeat("j", 1024)

			w, _ := qos2Dial(t, h.Addr, "stalled", false, 300)
			w.Publish(1, "jobs/work/held", "held-"+payload, false)
			if rc := w.Pubrec(1); rc != 0 {
				t.Fatalf("the receipt carried 0x%02X, want success: the queue had room", rc)
			}

			// Filled to its own bound after the receipt.
			p := connect(t, h, "filler", true, false)
			filled := 0
			for i := 0; ; i++ {
				if i == 100 {
					t.Fatal("100 jobs of 1KiB were taken by a queue bounded at 16KiB, so it was never full")
				}
				ack, err := p.C.Publish(context.Background(), &paho.Publish{
					Topic: fmt.Sprintf("jobs/work/fill-%d", i), QoS: 1, Payload: []byte(payload)})
				if ack == nil {
					t.Fatalf("no PUBACK for filler %d: %v", i, err)
				}
				if ack.ReasonCode == 0x97 {
					break
				}
				if ack.ReasonCode != 0 {
					t.Fatalf("filler %d was answered 0x%02X", i, ack.ReasonCode)
				}
				filled++
			}

			w.Pubrel(1)
			cp, err := w.Read(3 * time.Second)
			if err != nil {
				t.Fatalf("the refused release was answered with nothing (%v): want DISCONNECT 0x97, "+
					"which says why", err)
			}
			if d, ok := cp.Content.(*packets.Disconnect); !ok || d.ReasonCode != 0x97 {
				t.Fatalf("a release into a full queue was answered %s: want DISCONNECT 0x97, and never "+
					"a PUBCOMP", cp)
			}
			if cp, err := w.Read(time.Second); err == nil {
				t.Fatalf("after the DISCONNECT the connection sent %s, want it closed", cp.PacketType())
			}
			w.Close()
			if n := h.B.HeldBy("stalled"); n != 1 {
				t.Fatalf("the refused release left %d publish(es) held for the client, want the 1 it "+
					"was refused", n)
			}

			// A worker resolves every job, and is offered nothing the refused
			// release stored.
			worker := connect(t, h, "worker", true, false)
			worker.Sub(t, "$saguin/queue/jobs", 1)
			for i := 0; i < filled; i++ {
				r, ok := worker.Await(t, 3*time.Second)
				if !ok {
					t.Fatalf("the worker was offered %d of the %d fillers", i, filled)
				}
				if strings.HasPrefix(r.Payload, "held-") {
					t.Fatal("the held job was offered while its release was refused")
				}
				worker.Respond(t, r.RespTopic, "ack", r.CorrData)
			}
			if r, ok := worker.Await(t, 500*time.Millisecond); ok {
				t.Fatalf("the worker was offered %.12q after the fillers, before any release stored it",
					r.Payload)
			}

			sessionGone(t, h, "stalled")
			again, ca := qos2Dial(t, h.Addr, "stalled", false, 300)
			defer again.Close()
			if !ca.SessionPresent {
				t.Fatal("the session holding the refused exchange was not resumed")
			}
			if rc := again.Pubrec(1); rc != 0 {
				t.Fatalf("the resumed session's receipt carried 0x%02X, want success", rc)
			}
			again.Pubrel(1)
			if rc := again.Pubcomp(1); rc != 0 {
				t.Fatalf("the release sent again once the queue had room was answered 0x%02X, want "+
					"success", rc)
			}
			r, ok := worker.Await(t, 3*time.Second)
			if !ok || r.Payload != "held-"+payload {
				t.Fatalf("after the release the worker was offered %.12q (%v), want the held job", r.Payload, ok)
			}
			worker.Respond(t, r.RespTopic, "ack", r.CorrData)
			if r, ok := worker.Await(t, time.Second); ok {
				t.Fatalf("the worker was offered %.12q after the held job, which is offered once", r.Payload)
			}
		})
	}
}

// RFC 0003: the provider's room is taken at the PUBREC, so a provider that
// fills after it does not refuse the release. The events channel has no
// bound of its own here. Its provider is filled, by another channel and then
// by events, until it refuses an events publish of the same size, and every
// release still stores its held message, once.
//
// **Several held, so the releases run one after another at the ceiling.** A
// database frees room by the page: a release writes the record into the
// channel's pages and deletes the hold from the held rows' pages, and it
// runs in the room held back for the writes that relieve a full provider,
// as the binary holds back one record of max_message_size. One release
// fitting proves nothing about the tenth.
func TestAProviderThatFillsAfterTheHoldStillTakesItsRelease(t *testing.T) {
	const room, held = 64 * 1024, 10
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var h *harness
			switch provider {
			case "memory":
				h = startDurable(t, t.TempDir())
				h.B.SetQuotas(map[string]*store.Quota{"local": store.NewQuota(room, 0)})
			case "sqlite":
				brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+room, "16KiB")
				h = startDurableSQLite(t, filepath.Join(t.TempDir(), "qos2.db"))
			}
			payload := strings.Repeat("p", 1024)
			watch := connect(t, h, "watch", true, false)
			watch.Sub(t, "events/#", 1)

			w, _ := qos2Dial(t, h.Addr, "early", true, 0)
			defer w.Close()
			for id := uint16(1); id <= held; id++ {
				w.Publish(id, "events/orders/early", fmt.Sprintf("early-%d-%s", id, payload), false)
				if rc := w.Pubrec(id); rc != 0 {
					t.Fatalf("receipt %d carried 0x%02X, want success: the provider had room", id, rc)
				}
			}

			// Filled by the latest channel, and then by events itself until
			// events is refused: a database fills by the page, so a table can
			// have room left in its own after another's is refused.
			p := connect(t, h, "filler", true, false)
			for _, topic := range []string{"state/fill-%d", "events/fill/%d"} {
				for i := 0; ; i++ {
					if i == 1000 {
						t.Fatalf("1000 publishes of 1KiB to %s were taken by a provider bounded at %d",
							topic, room)
					}
					ack, err := p.C.Publish(context.Background(), &paho.Publish{
						Topic: fmt.Sprintf(topic, i), QoS: 1, Payload: []byte(payload)})
					if ack == nil {
						t.Fatalf("no PUBACK for filler %d: %v", i, err)
					}
					if ack.ReasonCode == 0x97 {
						break
					}
					if ack.ReasonCode != 0 {
						t.Fatalf("filler %d was answered 0x%02X", i, ack.ReasonCode)
					}
				}
			}

			for id := uint16(1); id <= held; id++ {
				w.Pubrel(id)
				if rc := w.Pubcomp(id); rc != 0 {
					t.Fatalf("release %d on a provider filled after its hold was answered 0x%02X, want "+
						"success: the hold already has its room", id, rc)
				}
			}
			// The watcher is sent the events fillers too; each released
			// message is among them, once.
			seen := map[string]int{}
			for {
				r, ok := watch.Await(t, 2*time.Second)
				if !ok {
					break
				}
				if strings.HasPrefix(r.Payload, "early-") {
					seen[r.Payload]++
				}
			}
			for id := 1; id <= held; id++ {
				if n := seen[fmt.Sprintf("early-%d-%s", id, payload)]; n != 1 {
					t.Errorf("the watcher was sent released message %d %d time(s), want 1", id, n)
				}
			}
		})
	}
}

// releasedThenLost stores a held publish and then answers as if the broker
// stopped before the PUBCOMP left: the swap committed, and the client heard
// nothing that completes the exchange.
type releasedThenLost struct {
	broker.HoldStore
	armed *atomic.Bool
}

func (s releasedThenLost) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	r, held, err := s.HoldStore.ReleaseHold(e)
	if err == nil && held && s.armed.CompareAndSwap(true, false) {
		return store.Record{}, true, errors.New("the broker stopped before the PUBCOMP")
	}
	return r, held, err
}

// RFC 0003 "What a restart costs", after the swap. A restart before it keeps
// the hold and the exchange finishes once (TestARestartMidExchangeLeavesOneRecord,
// TestAMemoryProviderKeepsAnExchangeExactlyAsFarAsItsSession). A restart
// after it - the record stored, its PUBCOMP never sent - keeps the record
// and not the hold: the PUBREL sent again is answered 0x92, which MQTT 5
// says is no error during recovery, and the channel holds the message once.
// Never twice, and never a PUBCOMP 0x00 from a table that no longer knows
// the exchange.
func TestARestartAfterTheReleaseKeepsTheRecordOnce(t *testing.T) {
	var armed atomic.Bool
	brokertest.WrapHolds = func(_ string, h broker.HoldStore) broker.HoldStore {
		return releasedThenLost{HoldStore: h, armed: &armed}
	}
	t.Cleanup(func() { brokertest.WrapHolds = nil })
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			dir, path := t.TempDir(), filepath.Join(t.TempDir(), "qos2.db")
			startIt := func() *harness {
				if provider == "memory" {
					return startDurable(t, dir)
				}
				return startDurableSQLite(t, path)
			}
			h := startIt()
			w, _ := qos2Dial(t, h.Addr, "cut-off", false, 300)
			w.Publish(1, "events/orders/cut", "cut-off", false)
			if rc := w.Pubrec(1); rc != 0 {
				t.Fatalf("the receipt carried 0x%02X, want success", rc)
			}
			armed.Store(true)
			w.Pubrel(1)
			for {
				cp, err := w.Read(time.Second)
				if err != nil {
					break
				}
				if _, ok := cp.Content.(*packets.Pubcomp); ok {
					t.Fatalf("the release was answered %s before the stop", cp)
				}
			}
			if armed.Load() {
				t.Fatal("the release never reached the store, so this proves nothing")
			}
			w.Close()
			sessionGone(t, h, "cut-off")
			// A memory provider keeps what its stores hold at a stop: the
			// record, and no hold. A database keeps what the commit left.
			if provider == "memory" {
				h.Stop()
			} else {
				h.Crash()
			}

			h2 := startIt()
			again, ca := qos2Dial(t, h2.Addr, "cut-off", false, 300)
			defer again.Close()
			if !ca.SessionPresent {
				t.Fatal("the session was not resumed, so its PUBREL below is not the one it owes")
			}
			again.Pubrel(1)
			cp := again.Next(3 * time.Second)
			comp, ok := cp.Content.(*packets.Pubcomp)
			if !ok || comp.PacketID != 1 {
				t.Fatalf("the resumed session was sent %s, want the PUBCOMP for 1: nothing is held, so "+
					"no receipt is owed", cp)
			}
			if comp.ReasonCode != 0x92 {
				t.Errorf("the PUBREL for a release stored before the restart was answered 0x%02X, "+
					"want 0x92: nothing knows the exchange now", comp.ReasonCode)
			}
			if n := inChannel(t, h2, "auditor", "cut-off"); n != 1 {
				t.Errorf("the channel holds the message %d time(s) across the restart, want 1", n)
			}
		})
	}
}

// RFC 0003 "What a restart costs": a channel removed from the configuration
// takes the exchanges held in it. Its PUBREL is answered 0x92, and an
// exchange in a channel still configured finishes as before.
//
// On a database the held row is deleted at that start and counted as
// abandoned: the file is opened for the other channels, and the row would
// take its room for good. A memory provider does not read the channel's
// snapshot at all, so the hold takes no room and is not counted then - and
// when the channel comes back, its snapshot is older than the shutdown
// before, which answered that release without it, so it comes back without
// its holds, counted. **Both providers end the same way**: the channel back
// holds nothing for a client already told 0x92, which may have sent the
// message again.
func TestAChannelRemovedAtARestartTakesItsHeldPublishes(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			dir, path := t.TempDir(), filepath.Join(t.TempDir(), "qos2.db")
			startIt := func(withLatest bool) *harness {
				brokertest.WithoutLatest = !withLatest
				defer func() { brokertest.WithoutLatest = false }()
				if provider == "memory" {
					return startDurable(t, dir)
				}
				return startDurableSQLite(t, path)
			}
			h := startIt(true)
			w, _ := qos2Dial(t, h.Addr, "orphan", false, 300)
			w.Publish(1, "state/orphan", "orphaned", false)
			if rc := w.Pubrec(1); rc != 0 {
				t.Fatalf("the receipt for the latest channel carried 0x%02X, want success", rc)
			}
			w.Publish(2, "events/orders/kept", "kept", false)
			if rc := w.Pubrec(2); rc != 0 {
				t.Fatalf("the receipt for the events channel carried 0x%02X, want success", rc)
			}
			w.Close()
			sessionGone(t, h, "orphan")
			h.Stop()

			h2 := startIt(false)
			wantAbandoned := uint64(0)
			if provider == "sqlite" {
				wantAbandoned = 1
			}
			if got := h2.B.QoS2Abandoned(); got != wantAbandoned {
				t.Errorf("the start without the latest channel counted %d abandoned, want %d", got, wantAbandoned)
			}
			if n := h2.B.HeldBy("orphan"); n != 1 {
				t.Errorf("the start without the latest channel holds %d for the client, want the 1 in "+
					"events", n)
			}
			again, ca := qos2Dial(t, h2.Addr, "orphan", false, 300)
			if !ca.SessionPresent {
				t.Fatal("the session was not resumed")
			}
			if rc := again.Pubrec(2); rc != 0 {
				t.Fatalf("the resumed receipt for events carried 0x%02X, want success", rc)
			}
			again.Pubrel(1)
			if rc := again.Pubcomp(1); rc != 0x92 {
				t.Errorf("the release for the removed channel was answered 0x%02X, want 0x92", rc)
			}
			again.Pubrel(2)
			if rc := again.Pubcomp(2); rc != 0 {
				t.Errorf("the release for the events channel was answered 0x%02X, want success", rc)
			}
			again.Close()
			sessionGone(t, h2, "orphan")
			h2.Stop()

			h3 := startIt(true)
			wantAbandoned = 0
			if provider == "memory" {
				wantAbandoned = 1
			}
			if got := h3.B.QoS2Abandoned(); got != wantAbandoned {
				t.Errorf("the start with the latest channel back counted %d abandoned, want %d", got, wantAbandoned)
			}
			if n := h3.B.HeldBy("orphan"); n != 0 {
				t.Errorf("the latest channel came back holding %d exchange(s) for a client told 0x92 "+
					"for them: a release now stores a message its publisher may have sent again", n)
			}
			back, ca := qos2Dial(t, h3.Addr, "orphan", false, 300)
			defer back.Close()
			if !ca.SessionPresent {
				t.Fatal("the session was not resumed")
			}
			if cp, err := back.Read(500 * time.Millisecond); err == nil {
				t.Errorf("the resumed session was sent %s, want nothing: it owes no receipt", cp)
			}
		})
	}
}

// RFC 0002 `broker.qos2.max_inflight_per_client` counts a client's held
// publishes across every channel, each held in its own channel's store: one
// in each of two channels uses an allowance of two, and a third, in a third
// channel, is refused 0x97 until one is released.
func TestTheAllowanceCountsHeldPublishesAcrossChannels(t *testing.T) {
	brokertest.QoS2Inflight = 2
	t.Cleanup(func() { brokertest.QoS2Inflight = 0 })
	eachProvider(t, func(t *testing.T, h *harness) {
		w, _ := qos2Dial(t, h.Addr, "spread", true, 0)
		defer w.Close()
		w.Publish(1, "events/orders/a", "a", false)
		if rc := w.Pubrec(1); rc != 0 {
			t.Fatalf("the first, in events, was answered 0x%02X", rc)
		}
		w.Publish(2, "jobs/work/b", "b", false)
		if rc := w.Pubrec(2); rc != 0 {
			t.Fatalf("the second, in jobs, was answered 0x%02X", rc)
		}
		w.Publish(3, "state/c", "c", false)
		if rc := w.Pubrec(3); rc != 0x97 {
			t.Fatalf("the third, in state, was answered 0x%02X, want 0x97: the allowance counts "+
				"each channel on its own", rc)
		}
		w.Pubrel(2)
		if rc := w.Pubcomp(2); rc != 0 {
			t.Fatalf("the release was answered 0x%02X", rc)
		}
		w.Publish(4, "news/d", "d", false)
		if rc := w.Pubrec(4); rc != 0 {
			t.Errorf("after one release, a broadcast was answered 0x%02X, want success", rc)
		}
		// A held broadcast counts as a channel's hold does.
		w.Publish(5, "state/e", "e", false)
		if rc := w.Pubrec(5); rc != 0x97 {
			t.Errorf("with one held in events and one broadcast, a third was answered 0x%02X, "+
				"want 0x97: a held broadcast is not counted", rc)
		}
	})
}

// broadcastProviders are the two session providers a broadcast is held on,
// each restartable on its own storage. A sqlite harness names its sessions'
// provider as its channels', since both are in its one file: the provider a
// start keeps the broadcast log's holds on (unreleasedBySession).
func broadcastProviders(t *testing.T) []restartable {
	brokertest.SessionsNamedLikeChannels = true
	t.Cleanup(func() { brokertest.SessionsNamedLikeChannels = false })
	return restartables(t)
}

// countOf counts what a client is sent with this payload until nothing more
// arrives for quiet.
func countOf(t *testing.T, c *client, payload string, quiet time.Duration) int {
	t.Helper()
	n := 0
	for {
		r, ok := c.Await(t, quiet)
		if !ok {
			return n
		}
		if r.Payload == payload {
			n++
		}
	}
}

// RFC 0003 "Exactly once, and where the unfinished ones wait": a QoS 2
// broadcast is held in the broadcast log's store at its PUBREC and delivered
// at its PUBREL, as a channel's publish is - a round trip later than a
// broadcast delivered at its PUBLISH, and once.
//
// A session that outlives its connection and one that does not are both
// subscribed: the first is served from the log, the second by the
// substrate. Neither is sent anything before the PUBREL, and each is sent the
// message once after it.
func TestAQoS2BroadcastIsDeliveredAtItsReleaseAndOnce(t *testing.T) {
	for _, p := range broadcastProviders(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			durable := connect(t, h, "durable", true, false)
			durable.Sub(t, "news/#", 2)
			live := dial(t, h, "live", true, false, 0, 0, 0)
			live.Sub(t, "news/#", 2)

			w, _ := qos2Dial(t, h.Addr, "publisher", true, 0)
			defer w.Close()
			w.Publish(1, "news/x", "held-news", false)
			if rc := w.Pubrec(1); rc != 0 {
				t.Fatalf("the receipt carried 0x%02X, want success", rc)
			}
			if n := h.B.HeldBy("publisher"); n != 1 {
				t.Fatalf("after the receipt the publisher holds %d exchange(s), want the broadcast held", n)
			}
			for _, c := range []*client{durable, live} {
				if r, ok := c.Await(t, 300*time.Millisecond); ok {
					t.Fatalf("%s was sent %q before the PUBREL", c.ID, r.Payload)
				}
			}

			w.Pubrel(1)
			if rc := w.Pubcomp(1); rc != 0 {
				t.Fatalf("the completion carried 0x%02X, want success", rc)
			}
			for _, c := range []*client{durable, live} {
				if n := countOf(t, c, "held-news", time.Second); n != 1 {
					t.Errorf("%s was sent the released broadcast %d time(s), want 1", c.ID, n)
				}
			}
			if n := h.B.HeldBy("publisher"); n != 0 {
				t.Errorf("after the release the publisher still holds %d exchange(s)", n)
			}
			// Counted at its release, so it leaves the log once the session
			// that outlives its connection has it: a release that skipped
			// the count would leave it there for good.
			lg := attachDrain(t, h)
			eventually(t, "the released broadcast left the log once delivered", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(lg.Next() - 1)
				return len(held) == 0
			})
		})
	}
}

// RFC 0002 "Every session's state": the broadcast log gives way to a
// broadcast's hold as to any write on its provider. The provider is full of
// QoS 1 broadcasts owed to a session that is away, and a QoS 2 broadcast is
// still held - the log's oldest going, counted against that session - where
// a log that kept everything would have refused it 0x97.
//
// Memory counts its room to the byte, so the hold there needs room the log
// gives up; a database fills by the page, and its held rows may find room in
// a page of their own, which is why only the memory arm is sure to exercise
// the give-way.
func TestAHeldBroadcastTakesItsRoomFromTheLogsOldest(t *testing.T) {
	const room = 64 * 1024
	// The away session's own bound above the provider's, so what it is owed
	// is let go only for the provider's room and the provider stays full.
	brokertest.SessionQueueBytes = 1 << 20
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := startDurable(t, t.TempDir())
	q := store.NewQuota(room, 0)
	h.B.SetQuotas(map[string]*store.Quota{"local": q})
	ops := operationsAt(t, h)
	reader := connect(t, h, "reader", false, false)
	reader.Sub(t, "news/#", 1)
	before := h.Disconnects.Snapshot("reader")
	reader.Close()
	sessionGoneAfter(t, h, "reader", before)

	payload := strings.Repeat("o", 1024)
	p := connect(t, h, "filler", true, false)
	// Until the provider has less room than one message: the hold below can
	// then be kept only by the log giving way.
	for i := 0; room-q.Bytes() >= int64(len(payload)); i++ {
		if i == 400 {
			t.Fatalf("400 broadcasts of 1KiB left %d bytes free on a %d-byte provider", room-q.Bytes(), room)
		}
		p.Pub(t, fmt.Sprintf("news/fill-%d", i), payload)
	}
	lost := scrapeGauges(t, ops)[storageFull]

	w, _ := qos2Dial(t, h.Addr, "publisher", true, 0)
	defer w.Close()
	w.Publish(1, "news/x", payload, false)
	if rc := w.Pubrec(1); rc != 0 {
		t.Fatalf("a QoS 2 broadcast on a provider full of the log was answered 0x%02X, want success: "+
			"the log did not give way to its hold", rc)
	}
	if n := h.B.HeldBy("publisher"); n != 1 {
		t.Errorf("the publisher holds %d exchange(s), want the broadcast held", n)
	}
	if got := scrapeGauges(t, ops)[storageFull]; got <= lost {
		t.Errorf("%s stayed at %v: the hold's room was not taken from the log's oldest", storageFull, got)
	}
}

// RFC 0003 "What a restart costs", for a broadcast: the broker stops after
// it has held a QoS 2 broadcast and before its PUBREC reaches the client, as
// a kill -9 lands. The client, never having seen the receipt, re-sends the
// PUBLISH flagged as a duplicate on its resumed session, then releases it.
// The subscriber is sent the message once.
//
// **The defect this closes, reproduced on the binary**: the
// broadcast was delivered at its PUBLISH and nothing kept the exchange, so
// the re-sent PUBLISH was a new message and the subscriber was sent it twice.
// The hold outlives the restart - on a database as the file, on a memory
// provider as far as its stop wrote - so the re-send is the repeat
// [MQTT-4.3.3-10] and the release delivers it once. The start must keep the
// broadcast log's holds, where it deletes a removed channel's: nothing is
// counted abandoned.
func TestARestartBeforeTheReceiptDeliversABroadcastOnce(t *testing.T) {
	for _, p := range broadcastProviders(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			reader := connect(t, h, "reader", false, false)
			reader.Sub(t, "news/#", 1)
			before := h.Disconnects.Snapshot("reader")
			reader.Close()
			sessionGoneAfter(t, h, "reader", before)

			w, _ := qos2Dial(t, h.Addr, "publisher", false, 300)
			w.Publish(5, "news/x", "once", false)
			// The receipt is never read: the broker stops with it unread, as
			// though it never reached the client.
			for deadline := time.Now().Add(5 * time.Second); h.B.HeldBy("publisher") != 1; time.Sleep(5 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the broadcast was never held, so the stop below is not mid-exchange")
				}
			}
			w.Close()
			sessionGone(t, h, "publisher")
			if p.name == "sqlite" {
				h.Crash()
			} else {
				h.Stop() // a memory provider keeps what its stop writes, and no more
			}

			h2 := p.start(t)
			if got := h2.B.QoS2Abandoned(); got != 0 {
				t.Errorf("the start counted %d exchange(s) abandoned: the held broadcast was dropped", got)
			}
			if n := h2.B.HeldBy("publisher"); n != 1 {
				t.Fatalf("after the restart the publisher holds %d exchange(s), want the broadcast", n)
			}
			again, ca := qos2Dial(t, h2.Addr, "publisher", false, 300)
			defer again.Close()
			if !ca.SessionPresent {
				t.Fatal("the publisher's session was not resumed")
			}
			if rc := again.Pubrec(5); rc != 0 {
				t.Fatalf("the resumed session's receipt carried 0x%02X, want success", rc)
			}
			again.Publish(5, "news/x", "once", true)
			if rc := again.Pubrec(5); rc != 0 {
				t.Fatalf("the re-sent PUBLISH was answered 0x%02X, want success", rc)
			}
			again.Pubrel(5)
			if rc := again.Pubcomp(5); rc != 0 {
				t.Fatalf("the release was answered 0x%02X, want success", rc)
			}

			back := connect(t, h2, "reader", false, false)
			if n := countOf(t, back, "once", 2*time.Second); n != 1 {
				t.Errorf("the subscriber was sent the broadcast %d time(s) across the restart, want 1", n)
			}
		})
	}
}

// RFC 0003 "What a restart costs", after the swap: the release wrote the
// broadcast into the log and the broker stopped before counting it for the
// sessions it reaches, or answering the PUBREL. The start counts every
// message at or after a session's cursor that its subscriptions match, so
// the subscriber is sent it once; the PUBREL sent again is answered 0x92,
// the exchange being unknown after a restart, and the message is not written
// twice.
func TestABroadcastReleasedBeforeARestartIsDeliveredOnce(t *testing.T) {
	var armed atomic.Bool
	brokertest.WrapHolds = func(where string, h broker.HoldStore) broker.HoldStore {
		if where != store.BroadcastLog {
			return h
		}
		return releasedThenLost{HoldStore: h, armed: &armed}
	}
	t.Cleanup(func() { brokertest.WrapHolds = nil })
	for _, p := range broadcastProviders(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			reader := connect(t, h, "reader", false, false)
			reader.Sub(t, "news/#", 1)
			before := h.Disconnects.Snapshot("reader")
			reader.Close()
			sessionGoneAfter(t, h, "reader", before)

			w, _ := qos2Dial(t, h.Addr, "publisher", false, 300)
			w.Publish(7, "news/x", "swapped", false)
			if rc := w.Pubrec(7); rc != 0 {
				t.Fatalf("the receipt carried 0x%02X, want success", rc)
			}
			armed.Store(true)
			w.Pubrel(7)
			for {
				cp, err := w.Read(time.Second)
				if err != nil {
					break
				}
				if _, ok := cp.Content.(*packets.Pubcomp); ok {
					t.Fatalf("the release was answered %s before the stop", cp)
				}
			}
			if armed.Load() {
				t.Fatal("the broadcast's release never reached the log, so this proves nothing")
			}
			w.Close()
			sessionGone(t, h, "publisher")
			if p.name == "sqlite" {
				h.Crash()
			} else {
				h.Stop()
			}

			h2 := p.start(t)
			back := connect(t, h2, "reader", false, false)
			if n := countOf(t, back, "swapped", 2*time.Second); n != 1 {
				t.Errorf("the subscriber was sent the broadcast %d time(s) after the restart, want 1: "+
					"it was in the log and counted by nobody", n)
			}
			again, ca := qos2Dial(t, h2.Addr, "publisher", false, 300)
			defer again.Close()
			if !ca.SessionPresent {
				t.Fatal("the publisher's session was not resumed")
			}
			again.Pubrel(7)
			if rc := again.Pubcomp(7); rc != 0x92 {
				t.Errorf("the PUBREL for a release written before the restart was answered 0x%02X, "+
					"want 0x92", rc)
			}
			if n := countOf(t, back, "swapped", time.Second); n != 0 {
				t.Errorf("the subscriber was sent the broadcast %d more time(s) after the PUBREL", n)
			}
		})
	}
}

// RFC 0003: a broadcast log that has nothing left to give refuses a QoS 2
// broadcast on its PUBREC with 0x97, before ownership is taken, and holds
// nothing. A QoS 1 broadcast is acknowledged there and lost to its sessions,
// counted; this publisher is still owed the answer, so it is refused
// instead.
func TestAFullBroadcastLogRefusesAQoS2BroadcastAtItsReceipt(t *testing.T) {
	const room = 64 * 1024
	brokertest.QoS2Inflight = 1000
	t.Cleanup(func() { brokertest.QoS2Inflight = 0 })
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var h *harness
			switch provider {
			case "memory":
				h = startDurable(t, t.TempDir())
				h.B.SetQuotas(map[string]*store.Quota{"local": store.NewQuota(room, 0)})
			case "sqlite":
				brokertest.SessionsNamedLikeChannels = true
				t.Cleanup(func() { brokertest.SessionsNamedLikeChannels = false })
				brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+room, "16KiB")
				h = startDurableSQLite(t, filepath.Join(t.TempDir(), "qos2.db"))
			}
			payload := strings.Repeat("f", 1024)
			p := connect(t, h, "filler", true, false)
			for i := 0; ; i++ {
				if i == 1000 {
					t.Fatalf("1000 publishes of 1KiB were taken by a provider bounded at %d", room)
				}
				ack, err := p.C.Publish(context.Background(), &paho.Publish{
					Topic: fmt.Sprintf("events/fill/%d", i), QoS: 1, Payload: []byte(payload)})
				if ack == nil {
					t.Fatalf("no PUBACK for filler %d: %v", i, err)
				}
				if ack.ReasonCode == 0x97 {
					break
				}
			}

			// Then held broadcasts until the provider takes no more: a
			// database fills by the page, so the held rows' own pages can
			// have room after the channel's are refused. The log holds no
			// message throughout, so it has nothing to give.
			w, _ := qos2Dial(t, h.Addr, "publisher", true, 0)
			defer w.Close()
			held, refused := 0, false
			for id := uint16(1); id <= 200 && !refused; id++ {
				w.Publish(id, "news/x", payload, false)
				switch rc := w.Pubrec(id); rc {
				case 0x00:
					held++
				case 0x97:
					refused = true
				default:
					t.Fatalf("broadcast %d was answered 0x%02X, want 0x00 or 0x97", id, rc)
				}
			}
			if !refused {
				t.Fatalf("%d QoS 2 broadcasts of 1KiB were held on a provider bounded at %d", held, room)
			}
			if n := h.B.HeldBy("publisher"); n != held {
				t.Errorf("the publisher holds %d exchange(s), want the %d answered 0x00: the one refused "+
					"0x97 was held anyway", n, held)
			}
		})
	}
}

// RFC 0003: a retained QoS 2 broadcast is retained at its release, with its
// message, and not at its PUBLISH - a subscriber arriving before the PUBREL
// is served no value that no delivery has carried, from an exchange that may
// never finish.
func TestARetainedQoS2BroadcastIsRetainedAtItsRelease(t *testing.T) {
	period := int64(0)
	brokertest.Retaining = &period
	t.Cleanup(func() { brokertest.Retaining = nil })
	h := start(t)

	w, _ := qos2Dial(t, h.Addr, "publisher", true, 0)
	defer w.Close()
	cp := packets.NewControlPacket(packets.PUBLISH)
	pub := cp.Content.(*packets.Publish)
	pub.Topic, pub.Payload, pub.PacketID, pub.QoS, pub.Retain = "news/state", []byte("retained-once"), 9, 2, true
	cp.FixedHeader.Flags |= 2<<1 | 1
	if _, err := cp.WriteTo(w.C); err != nil {
		t.Fatal(err)
	}
	if rc := w.Pubrec(9); rc != 0 {
		t.Fatalf("the receipt carried 0x%02X, want success", rc)
	}
	early := connect(t, h, "early", true, false)
	early.Sub(t, "news/#", 2)
	if r, ok := early.Await(t, 300*time.Millisecond); ok {
		t.Fatalf("a subscriber arriving before the PUBREL was served %q", r.Payload)
	}

	w.Pubrel(9)
	if rc := w.Pubcomp(9); rc != 0 {
		t.Fatalf("the completion carried 0x%02X, want success", rc)
	}
	late := connect(t, h, "late", true, false)
	late.Sub(t, "news/#", 2)
	if r, ok := late.Await(t, 3*time.Second); !ok || r.Payload != "retained-once" {
		t.Fatalf("a subscriber arriving after the release was served %q (%v), want the retained value",
			r.Payload, ok)
	}
}

// releaseRefusedOnce refuses one release while armed and leaves the hold, as
// a store whose write failed does.
type releaseRefusedOnce struct {
	broker.HoldStore
	armed *atomic.Bool
}

func (s releaseRefusedOnce) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	if s.armed.CompareAndSwap(true, false) {
		return store.Record{}, true, errors.New("disk I/O error")
	}
	return s.HoldStore.ReleaseHold(e)
}

// RFC 0003 "Exactly once": a retained QoS 2 broadcast is retained at its
// release, just before the swap - so a release refused after that write, and
// never sent again, leaves the value retained when the hold expires. There is
// no undo, since taking the value back would race a newer one. The exchange
// is counted as abandoned, and a warning says the retained value stays.
func TestARetainedBroadcastAbandonedAfterARefusedReleaseStaysRetained(t *testing.T) {
	period := int64(0)
	brokertest.Retaining = &period
	brokertest.QoS2ExpiresAfter = time.Second
	var armed atomic.Bool
	brokertest.WrapHolds = func(where string, h broker.HoldStore) broker.HoldStore {
		if where != store.BroadcastLog {
			return h
		}
		return releaseRefusedOnce{HoldStore: h, armed: &armed}
	}
	var mu sync.Mutex
	var warned []string
	brokertest.LogTo = func(l string) {
		if strings.Contains(l, "asked to be retained was abandoned") {
			mu.Lock()
			warned = append(warned, l)
			mu.Unlock()
		}
	}
	t.Cleanup(func() {
		brokertest.Retaining, brokertest.QoS2ExpiresAfter, brokertest.WrapHolds, brokertest.LogTo = nil, 0, nil, nil
	})
	h := start(t)

	w, _ := qos2Dial(t, h.Addr, "publisher", false, 300)
	cp := packets.NewControlPacket(packets.PUBLISH)
	pub := cp.Content.(*packets.Publish)
	pub.Topic, pub.Payload, pub.PacketID, pub.QoS, pub.Retain = "news/state", []byte("stays"), 9, 2, true
	cp.FixedHeader.Flags |= 2<<1 | 1
	if _, err := cp.WriteTo(w.C); err != nil {
		t.Fatal(err)
	}
	if rc := w.Pubrec(9); rc != 0 {
		t.Fatalf("the receipt carried 0x%02X, want success", rc)
	}
	armed.Store(true)
	w.Pubrel(9)
	if cp, err := w.Read(time.Second); err == nil {
		if d, ok := cp.Content.(*packets.Disconnect); !ok || d.ReasonCode != 0x83 {
			t.Fatalf("the refused release was answered %s, want DISCONNECT 0x83", cp)
		}
	}
	if armed.Load() {
		t.Fatal("the release never reached the log, so this proves nothing")
	}
	w.Close()

	before := h.B.QoS2Abandoned()
	for deadline := time.Now().Add(5 * time.Second); h.B.QoS2Abandoned() == before; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the hold was never counted abandoned after expires_after")
		}
	}
	mu.Lock()
	lines := append([]string(nil), warned...)
	mu.Unlock()
	if len(lines) != 1 || !strings.Contains(lines[0], "timed out") {
		t.Errorf("the abandoned retained broadcast was warned about in %q, want one line saying it "+
			"timed out", lines)
	}
	late := connect(t, h, "late", true, false)
	late.Sub(t, "news/#", 2)
	if r, ok := late.Await(t, 3*time.Second); !ok || r.Payload != "stays" {
		t.Errorf("after the hold expired a new subscriber was served %q (%v), want the value its "+
			"refused release had retained", r.Payload, ok)
	}
}

// **A QoS 2 PUBLISH repeated before its release is held once and answered as
// the first was** [MQTT-4.3.3-10], decided in the broker's own map of held
// exchanges. On one connection the engine answers a repeat from its own
// PUBREC entry and never asks the broker; the broker's map is what answers a
// repeat the engine has no entry for - a successor's re-send, where the
// takeover cloned the in-flight table before the old connection's hook had
// returned and its PUBREC entry was set. So the repeat is made here at the
// hook, as the engine would make it then. **With an allowance of one
// exchange**, so the repeat counted as a second held message is refused 0x97
// - the store alone keeps the first copy either way, so the record count
// cannot tell the two apart; what the broker's map decides is that a repeat
// takes no more of the client's allowance.
func TestAQoS2PublishRepeatedBeforeItsReleaseIsHeldOnce(t *testing.T) {
	brokertest.QoS2Inflight = 1
	t.Cleanup(func() { brokertest.QoS2Inflight = 0 })
	h := start(t)
	w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
	defer w.Close()
	w.Publish(12, "events/orders/4", "order-4", false)
	if rc := w.Pubrec(12); rc != 0 {
		t.Fatalf("the receipt carried 0x%02X, want success", rc)
	}
	cl, ok := h.Srv.Clients.Get("sensor-1")
	if !ok {
		t.Fatal("no client under sensor-1")
	}
	_, err := h.B.OnPublish(cl, mqttpackets.Packet{
		FixedHeader: mqttpackets.FixedHeader{Type: mqttpackets.Publish, Qos: 2, Dup: true},
		TopicName:   "events/orders/4", Payload: []byte("order-4"), PacketID: 12,
	})
	// The engine answers with the code a hook returns: 0x00 is the PUBREC
	// the first was answered with, and anything at or above 0x80 a refusal.
	var code mqttpackets.Code
	if err != nil && (!errors.As(err, &code) || code.Code != 0) {
		t.Errorf("the repeat was answered %v, want success as the first was: it is the same message", err)
	}
	w.Pubrel(12)
	if rc := w.Pubcomp(12); rc != 0 {
		t.Fatalf("the completion carried 0x%02X, want success", rc)
	}
	if n := inChannel(t, h, "auditor", "order-4"); n != 1 {
		t.Errorf("the channel holds the record %d time(s), want 1: a repeat before the release "+
			"was held as a second message", n)
	}
}

// **A released retained QoS 2 broadcast is kept with the User Properties it
// was published with.** Held at its PUBLISH and released at its PUBREL, its
// retained value is written from the stored record (retainHeld, publishOf),
// whose headers are what the publisher's User Properties became; a
// subscriber arriving afterwards is sent the retained value with each. On
// both session providers, since a broadcast is held in the session
// provider's broadcast log.
func TestAReleasedRetainedQoS2BroadcastKeepsItsUserProperties(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		releasedRetainedKeepsUser(t, brokertest.StartRetainingDurably(t, t.TempDir(), ""))
	})
	t.Run("sqlite", func(t *testing.T) {
		releasedRetainedKeepsUser(t, brokertest.StartRetainingDurably(t, "", filepath.Join(t.TempDir(), "saguin.db")))
	})
}

func releasedRetainedKeepsUser(t *testing.T, h *brokertest.Harness) {
	w, _ := qos2Dial(t, h.Addr, "sensor-1", true, 0)
	defer w.Close()

	cp := packets.NewControlPacket(packets.PUBLISH)
	p := cp.Content.(*packets.Publish)
	p.Topic, p.Payload, p.PacketID, p.QoS, p.Retain = "news/today", []byte("headline"), 13, 2, true
	p.Properties = &packets.Properties{User: []packets.User{{Key: "source", Value: "wire-7"}}}
	cp.FixedHeader.Flags |= 2<<1 | 0x01
	if _, err := cp.WriteTo(w.C); err != nil {
		t.Fatalf("write PUBLISH: %v", err)
	}
	if rc := w.Pubrec(13); rc != 0 {
		t.Fatalf("the receipt carried 0x%02X, want success", rc)
	}
	w.Pubrel(13)
	if rc := w.Pubcomp(13); rc != 0 {
		t.Fatalf("the completion carried 0x%02X, want success", rc)
	}
	var r brokertest.Received
	eventually(t, "the retained value", 5*time.Second, func() bool {
		sub := connect(t, h, "reader", true, false)
		defer func() { _ = sub.C.Disconnect(&paho.Disconnect{}) }()
		sub.Sub(t, "news/#", 1)
		got, ok := sub.Await(t, 500*time.Millisecond)
		r = got
		return ok && got.Retain
	})
	if r.Payload != "headline" || r.User["source"] != "wire-7" {
		t.Errorf("a later subscriber was sent the retained %q with User Properties %v, want source=wire-7",
			r.Payload, r.User)
	}
}
