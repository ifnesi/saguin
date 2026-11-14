package conformance

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Flows, CONNECT, step 5: "An old connection for the id gets
// DISCONNECT 0x8E on MQTT 5 and is closed. If its writer has not taken that
// DISCONNECT within 500 ms, its socket is cut (Closing a connection).
// Anything its reader still delivers is ignored." MQTT 5 3.1.4: "the Server
// sends a DISCONNECT packet to the existing Client with Reason Code of 0x8E
// (Session taken over) [...] and MUST close the Network Connection of the
// existing Client [MQTT-3.1.4-3]";
// 3.1.1 [MQTT-3.1.4-2]: "the Server MUST disconnect the existing Client", and
// DISCONNECT, "Reasons Sagüin sends": "These reasons are MQTT 5 only. An MQTT
// 3.1.1 connection just closes". design.md, Flows, Closing a connection: "The
// writer handles the DISCONNECT and orderly close". What the old connection
// sends after the takeover (a PINGREQ, a DISCONNECT ending its session)
// changes nothing: the new connection stays, and its session outlives it.
func TestTakeover(t *testing.T) {
	for name, version := range map[string]byte{"MQTT 5": harness.V5, "MQTT 3.1.1": harness.V311} {
		t.Run(name, func(t *testing.T) {
			keep := &harness.Connect{ClientID: clientID(t, "")}
			end := &harness.Disconnect{}
			if version == harness.V5 {
				keep.Props = support.Durable
				end.Props = support.SessionExpiry(0)
			}
			old, _ := harness.Connected(t, brokerAddr, version, keep)
			newer, _ := harness.Connected(t, brokerAddr, version, keep)
			if version == harness.V5 {
				if d := harness.Expect[*harness.Disconnect](t, old); d.Reason != 0x8E {
					t.Fatalf("the old connection got DISCONNECT 0x%02X, want 0x8E (Session taken over)", d.Reason)
				}
			}
			for _, p := range []harness.Packet{&harness.Pingreq{}, end} {
				// The broker may have ended its side already: what this
				// sends is meant to go nowhere.
				_ = old.Send(p)
			}
			_ = old.CloseWrite()
			harness.ExpectClosed(t, old)

			if err := newer.Send(&harness.Pingreq{}); err != nil {
				t.Fatal(err)
			}
			harness.Expect[*harness.Pingresp](t, newer)
			if err := newer.Close(); err != nil {
				t.Fatal(err)
			}
			if _, ack := harness.Connected(t, brokerAddr, version, keep); !ack.SessionPresent {
				t.Error("the session ended: the old connection's DISCONNECT changed it after the takeover")
			}
		})
	}
}

// takeoverWriteTimeout is the takeover tests' limits.write_timeout, long
// enough that a stalled writer's own deadline (writer_full) cannot end the
// old connection before the takeover's cut does; takeoverCut is the
// design's 500 ms, and takeoverBound, twice it, how late a takeover's
// CONNACK may come and the old connection end.
const (
	takeoverWriteTimeout = 10 * time.Second
	takeoverCut          = 500 * time.Millisecond
	takeoverBound        = 2 * takeoverCut
)

// design.md, Flows, CONNECT, step 5: "An old connection for the id gets
// DISCONNECT 0x8E on MQTT 5 and is closed. If its writer has not taken that
// DISCONNECT within 500 ms, its socket is cut (Closing a connection).
// Anything its reader still delivers is ignored. The takeover then follows
// its writer's reports and its store writes"; step 8: "Once the changes are
// stored, the broker sends CONNACK"; step 9: resent messages "with their
// packet identifiers and in their order. Resent messages have DUP set
// (MQTT-4.4.0-1) [...] New application deliveries wait behind recovered
// PUBLISHes"; Closing a connection: "The core never waits for a connection
// to close", "A write that has not finished limits.write_timeout after its
// first socket-write attempt closes the connection as writer_full", "The
// core cuts a connection only by closing its raw socket", and "The writer
// handles the DISCONNECT and orderly close"; Delivery: "A message is in
// flight from its hand-off to the writer", the window "the lower of the
// client's Receive Maximum and limits.max_inflight", "QoS 0 settles on
// successful writing", "When the connection ends, deliveries prepared and
// never handed to the writer are cancelled [...] a continuing session
// receives them later", "A connection's prepared and sent deliveries
// together are at most twice its window", and Order: "deliveries follow a
// channel's offset order on $broadcast, append and a queue's own topics
// [...] For a filter reaching several channels, order holds within each
// channel, not across them". MQTT 5 4.4: "When a Client reconnects with
// Clean Start set to 0 and a session is present, both the Client and
// Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0)
// [...] using their original Packet Identifiers [MQTT-4.4.0-1]"; 3.3.1.1:
// "The DUP flag MUST be set to 1 by the Client or Server when it attempts
// to re-deliver a PUBLISH packet [MQTT-3.3.1-1]", and "The DUP flag MUST be
// set to 0 for all QoS 0 messages [MQTT-3.3.1-2]"; 3.3.4: "The Server MUST
// NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH packets for
// which it has not received PUBACK, PUBCOMP, or PUBREC with a Reason Code
// of 128 or greater from the Client [MQTT-3.3.4-9]".
//
// A durable session's client stops reading, as a phone that changed
// networks, while a delivery larger than every buffer between the broker
// and it (stalledPayload) is being written: once that delivery's header has
// come, the broker's writer has begun a write it cannot finish, so it
// cannot take the takeover's DISCONNECT. A second connection with the
// client id and Clean Start 0 then takes the session over. Its CONNACK,
// Session Present 1, comes no sooner than takeoverCut after its CONNECT,
// since the takeover follows the old writer's report, which only the cut
// brings, and no later than takeoverBound; write_timeout is 10 seconds, so
// the CONNACK cannot wait for the old writer's own deadline. The old
// client's connection then ends within takeoverBound, no DISCONNECT on it
// (cutAfterTakeover). An old connection free to take its DISCONNECT, one
// that reads, takes DISCONNECT 0x8E before its end, and the CONNACK comes
// within takeoverBound (tookItsDisconnect). What the old writer did not
// write is owed on the new
// connection, nothing is sent twice but MQTT's DUP resends, the stored
// exchanges are resent first, in their sending order, and the new
// deliveries then come in each channel's offset order (recovered):
//   - QoS 1 ($broadcast, max_inflight 1): the new connection's first packet
//     is the stalled delivery resent, DUP 1, under its original identifier,
//     left unacknowledged. A third connection takes the session over, the
//     second's connection closed; the old writer's report settled nothing,
//     so the third's first packet is the same resend, and once
//     acknowledged, next follows, DUP 0.
//   - QoS 0, written: the old client, reading, takes a QoS 0 delivery
//     whole, then is taken over and takes its DISCONNECT 0x8E. A
//     publication made after the CONNACK, next, is the new connection's
//     first packet: the delivery written is not sent again.
//   - QoS 0, reading when taken over: the stalled client, dialled with
//     drainingBuffers, its delivery of drainedPayload, reads on at once as the
//     takeover's CONNECT goes, so its writer finishes the delivery within
//     500 ms: it receives the whole delivery, then DISCONNECT 0x8E, then
//     its connection ends; it is not cut. A connection that ends before the
//     whole delivery came, takeoverCut or more after the CONNECT went, had
//     a drain slower than the cut, which the broker rightly makes: the
//     test's premise is broken. next, published after the CONNACK, is the
//     new connection's first packet.
//   - QoS 0, never written ($broadcast, max_inflight 1): the new
//     connection's first packet is the whole stalled delivery, DUP 0, then
//     next, then last, published once next has come.
//
// Cases on channels a, b and c (append), max_inflight 4 unless said, a
// subscription at QoS 1 and the stalled delivery a/large published at QoS
// 0; each waits, in the database (storage.md, session_inflight), for the
// exchanges of the QoS 1 publications made after a/large:
//   - QoS 0 never written, then QoS 1: a/next follows a/large. With Receive
//     Maximum 1, the new connection receives a/next resent, then a/large,
//     whole, DUP 0, then last.
//   - two channels: a/next and b/other follow a/large. With Receive Maximum
//     1, the new connection receives a/next and b/other resent, then
//     a/large, each once, then last.
//   - smaller Receive Maximum ($broadcast, max_inflight 3): the old client
//     reads one and two, unacknowledged, and stalls on a third at QoS 1;
//     next is stored behind. With Receive Maximum 1, below the three owed,
//     each is resent in order, under its identifier, DUP 1, one at a time:
//     after each, a PINGREQ is answered before any other PUBLISH. Then next
//     and last.
//   - successive takeovers: three stalled clients in turn take the session
//     over, ch/large at QoS 0 published on its own channel to each, behind
//     which ch/1, ch/2 and ch/3 at QoS 1; then a last connection. It
//     receives each of the twelve once, it is resent no more than twice the
//     window, 8, and the database never holds more than 8 of the session's
//     exchanges.
//
// Cases raising the stalled delivery's QoS: the old client subscribes to
// a/large at QoS 0 and a/next/# at QoS 1, a/large is published at QoS 1, so
// it is sent at QoS 0, and a/next/0, 1 and 2 follow at QoS 1. Before the
// takeover, the stalled client subscribes to a/large again at QoS 1 ("A
// SUBSCRIBE repeating an existing filter replaces its options";
// "Subscription changes affect new deliveries"), stored in the database,
// so the a/large the old writer did not write is owed at QoS 1:
//   - window 4 to 1: with Receive Maximum 1, the new connection receives
//     each a/next resent, then a/large at QoS 1, whole, one at a time: a
//     PINGREQ is answered before any other PUBLISH while one is
//     unacknowledged. Then last.
//   - window 8 to 2, acknowledgements held back: the old client also read
//     b/0 and b/1, unacknowledged. With Receive Maximum 2, the new
//     connection's first two deliveries fill its window: a PINGREQ is then
//     answered before any other PUBLISH. It acknowledges them only once
//     /metrics counts the old socket closed, one at a time, each followed by
//     a PINGREQ answered; then the rest: b/0, b/1 and a/next/0, 1 and 2
//     resent, then a/large, each once.
//   - repeated takeovers, then fresh traffic: max_inflight 12, eight
//     a/replay records sent at QoS 0 to the stalled client, raised to QoS 1
//     before the takeover, and three a/tail at QoS 1 behind them. Sixteen
//     connections with Receive Maximum 2 in turn receive a/tail/0 and 1
//     resent, under their identifiers, unacknowledged, and are taken over,
//     each connection closed. The last then acknowledges: a/tail/2 resent
//     and the eight replay records at QoS 1 follow, each once, and
//     session_inflight is empty. Fresh traffic then flows at the full
//     window: b/fresh/0 and b/fresh/1 both arrive before either is
//     acknowledged, and session_inflight is empty again once they are.
func TestTakeoverDoesNotWaitForAStalledWriter(t *testing.T) {
	t.Run("QoS 1", func(t *testing.T) {
		t.Parallel()
		b := support.Launch(t, &harness.Options{WriteTimeout: takeoverWriteTimeout, MaxPacketSize: 2 * stalledPayload, MaxIncomingMemory: leastIncoming(2 * stalledPayload), MaxInflight: 1, Operations: true})
		topic := clientID(t, "/stalled")
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		old, first := stalledReader(t, b.Addr, harness.V5)
		harness.ConnectedOver(t, old, keep)
		support.Suback(t, old, nil, support.At(1, topic), 1)
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: bytes.Repeat([]byte{'s'}, stalledPayload)}})
		id, err := first()
		if err != nil {
			t.Fatalf("watchdog: the delivery's header on the stalled connection: %v", err)
		}
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("next")}})

		newer := tookOver(t, b.Addr, keep, true)
		cutAfterTakeover(t, old)
		stalledResent(t, newer, topic, id, "the takeover")
		third := tookOver(t, b.Addr, keep, false)
		tookItsDisconnect(t, newer)
		p := stalledResent(t, third, topic, id, "after the old writer's report")
		if err := third.Acknowledge(p); err != nil {
			t.Fatal(err)
		}
		nextLive(t, third, topic, "next", "after acknowledging the resend")
	})

	t.Run("QoS 0, written", func(t *testing.T) {
		t.Parallel()
		b := support.Launch(t, &harness.Options{WriteTimeout: takeoverWriteTimeout})
		topic := clientID(t, "/written")
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		old := support.Subscribed(t, b.Addr, harness.V5, keep, support.At(0, topic))
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		support.PublishAll(t, pub, []*harness.Publish{{Topic: topic, Payload: []byte("written")}})
		arrived(t, old, topic, 0, []byte("written"), "before the takeover")

		newer := tookOver(t, b.Addr, keep, false)
		tookItsDisconnect(t, old)
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("next")}})
		nextLive(t, newer, topic, "next", "after the takeover, the delivery written not again")
	})

	t.Run("QoS 0, reading when taken over", func(t *testing.T) {
		t.Parallel()
		size := drainedPayload(t)
		b := support.Launch(t, &harness.Options{WriteTimeout: takeoverWriteTimeout, MaxPacketSize: 2 * size, MaxIncomingMemory: leastIncoming(2 * size)})
		topic := clientID(t, "/reading")
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		old, first := stalledReaderOver(t, drainingBuffers, b.Addr, harness.V5)
		harness.ConnectedOver(t, old, keep)
		support.Suback(t, old, nil, support.At(0, topic), 0)
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		large := bytes.Repeat([]byte{'s'}, size)
		support.PublishAll(t, pub, []*harness.Publish{{Topic: topic, Payload: large}})
		if _, err := first(); err != nil {
			t.Fatalf("watchdog: the delivery's header on the old connection: %v", err)
		}

		// The takeover's CONNECT goes, and the old client reads on at
		// once: its writer finishes the delivery and takes the DISCONNECT.
		newer, sent := takingOver(t, b.Addr, keep)
		drainable(t, old)
		old.Timeout = takeoverBound
		p, err := old.Recv()
		if took := time.Since(sent).Round(time.Millisecond); err != nil {
			if took >= takeoverCut && !harness.IsTimeout(err) {
				t.Fatalf("test premise broken: the old connection ended %v after the takeover's CONNECT went, before its delivery came whole (%v): its drain did not finish within the takeover's %v cut", took, err, takeoverCut)
			}
			t.Fatalf("on the old connection, reading when taken over, %v after the takeover's CONNECT went: %v; want the whole delivery, then DISCONNECT 0x8E", took, err)
		}
		arrivedAs(t, old, p, topic, 0, large, "on the old connection, reading when taken over")
		tookItsDisconnect(t, old)
		tookOverSince(t, newer, sent, false)
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("next")}})
		nextLive(t, newer, topic, "next", "after the takeover, the delivery written not again")
	})

	t.Run("QoS 0, never written", func(t *testing.T) {
		t.Parallel()
		b := support.Launch(t, &harness.Options{WriteTimeout: takeoverWriteTimeout, MaxPacketSize: 2 * stalledPayload, MaxIncomingMemory: leastIncoming(2 * stalledPayload), MaxInflight: 1, Operations: true})
		topic := clientID(t, "/stalled")
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		old, first := stalledReader(t, b.Addr, harness.V5)
		harness.ConnectedOver(t, old, keep)
		support.Suback(t, old, nil, support.At(0, topic), 0)
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		large := bytes.Repeat([]byte{'s'}, stalledPayload)
		support.PublishAll(t, pub, []*harness.Publish{{Topic: topic, Payload: large}})
		if _, err := first(); err != nil {
			t.Fatalf("watchdog: the delivery's header on the stalled connection: %v", err)
		}
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("next")}})

		newer := tookOver(t, b.Addr, keep, true)
		cutAfterTakeover(t, old)
		arrived(t, newer, topic, 0, large, "the delivery the old writer did not write")
		nextLive(t, newer, topic, "next", "after it")
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("last")}})
		nextLive(t, newer, topic, "last", "after next")
	})

	t.Run("QoS 0 never written, then QoS 1", func(t *testing.T) {
		t.Parallel()
		b, keep, old, pub, large := stalledOnChannels(t, takeoverWriteTimeout, "a/#")
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "a/next", Payload: []byte("next")}})
		awaitExchanges(t, b, keep.ClientID, 1, "a/next's")

		ids := storedIDs(t, b, keep.ClientID)
		newer := tookOver(t, b.Addr, receiving(keep, 1), true)
		cutAfterTakeover(t, old)
		recovered(t, newer, map[string][]*harness.Publish{
			"a": {{QoS: 0, Topic: "a/large", Payload: large}, {QoS: 1, Topic: "a/next", Payload: []byte("next")}},
		}, ids, nil, nil)
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "a/next", Payload: []byte("last")}})
		nextLive(t, newer, "a/next", "last", "after a/next")
	})

	t.Run("two channels", func(t *testing.T) {
		t.Parallel()
		b, keep, old, pub, large := stalledOnChannels(t, takeoverWriteTimeout, "a/#", "b/#")
		support.PublishAll(t, pub, []*harness.Publish{
			{QoS: 1, Topic: "a/next", Payload: []byte("next")},
			{QoS: 1, Topic: "b/other", Payload: []byte("other")},
		})
		awaitExchanges(t, b, keep.ClientID, 2, "a/next's and b/other's")

		ids := storedIDs(t, b, keep.ClientID)
		newer := tookOver(t, b.Addr, receiving(keep, 1), true)
		cutAfterTakeover(t, old)
		recovered(t, newer, map[string][]*harness.Publish{
			"a": {{QoS: 0, Topic: "a/large", Payload: large}, {QoS: 1, Topic: "a/next", Payload: []byte("next")}},
			"b": {{QoS: 1, Topic: "b/other", Payload: []byte("other")}},
		}, ids, nil, nil)
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "a/next", Payload: []byte("last")}})
		nextLive(t, newer, "a/next", "last", "after the owed deliveries")
	})

	t.Run("smaller Receive Maximum", func(t *testing.T) {
		t.Parallel()
		b := support.Launch(t, &harness.Options{WriteTimeout: takeoverWriteTimeout, MaxPacketSize: 2 * stalledPayload, MaxIncomingMemory: leastIncoming(2 * stalledPayload), MaxInflight: 3})
		topic := clientID(t, "/stalled")
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		old, first := stalledReader(t, b.Addr, harness.V5)
		harness.ConnectedOver(t, old, keep)
		support.Suback(t, old, nil, support.At(1, topic), 1)

		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		owed := make([]*harness.Publish, 0, 3) // what the old connection was sent and did not acknowledge
		for _, payload := range []string{"one", "two"} {
			support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte(payload)}})
			owed = append(owed, harness.Expect[*harness.Publish](t, old))
		}
		large := bytes.Repeat([]byte{'s'}, stalledPayload)
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: large}})
		id, err := first()
		if err != nil {
			t.Fatalf("watchdog: the delivery's header on the stalled connection: %v", err)
		}
		owed = append(owed, &harness.Publish{QoS: 1, Topic: topic, ID: id, Payload: large})
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("next")}})

		newer := tookOver(t, b.Addr, receiving(keep, 1), true)
		cutAfterTakeover(t, old)
		for i, want := range owed {
			p := harness.Expect[*harness.Publish](t, newer)
			if p.Topic != want.Topic || p.QoS != 1 || !p.Dup || p.ID != want.ID || !bytes.Equal(p.Payload, want.Payload) {
				t.Fatalf("resend %d of %d, Receive Maximum 1: PUBLISH %q QoS %d, DUP %t, identifier %d, %d bytes; want %q QoS 1, DUP 1, identifier %d, %d bytes",
					i+1, len(owed), p.Topic, p.QoS, p.Dup, p.ID, len(p.Payload), want.Topic, want.ID, len(want.Payload))
			}
			if err := newer.Ping(); err != nil {
				t.Fatalf("resend %d of %d, unacknowledged, Receive Maximum 1: %v", i+1, len(owed), err)
			}
			if err := newer.Acknowledge(p); err != nil {
				t.Fatal(err)
			}
		}
		arrived(t, newer, topic, 1, []byte("next"), "after the resends")
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte("last")}})
		nextLive(t, newer, topic, "last", "after next")
	})

	t.Run("successive takeovers", func(t *testing.T) {
		t.Parallel()
		const window = 4
		b := support.Launch(t, channelOptions(takeoverWriteTimeout, window))
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		db := support.OpenDB(t, b.DB)
		most := 0 // the most exchanges of the session the database has shown
		stored := func() {
			most = max(most, len(storedInflight(t, db, keep.ClientID)))
		}
		large := bytes.Repeat([]byte{'s'}, stalledPayload)
		channels := []string{"a", "b", "c"}
		owed := map[string][]*harness.Publish{}
		var old *harness.Client
		for i, ch := range channels {
			stalled, first := stalledReader(t, b.Addr, harness.V5)
			if i == 0 {
				harness.ConnectedOver(t, stalled, keep)
				support.Suback(t, stalled, nil, support.At(1, "a/#", "b/#", "c/#"), 1, 1, 1)
			} else {
				tookOverOn(t, stalled, keep, true) // the one before, stalled
				cutAfterTakeover(t, old)
			}
			old = stalled
			owed[ch] = []*harness.Publish{
				{QoS: 0, Topic: ch + "/large", Payload: large},
				{QoS: 1, Topic: ch + "/1", Payload: []byte(ch + "1")},
				{QoS: 1, Topic: ch + "/2", Payload: []byte(ch + "2")},
				{QoS: 1, Topic: ch + "/3", Payload: []byte(ch + "3")},
			}
			support.PublishAll(t, pub, owed[ch][:1])
			if _, err := first(); err != nil {
				t.Fatalf("watchdog: a delivery's header on connection %d: %v", i+1, err)
			}
			support.PublishAll(t, pub, owed[ch][1:])
			stored()
		}

		stored()
		ids := storedIDs(t, b, keep.ClientID)
		last := tookOver(t, b.Addr, keep, true)
		cutAfterTakeover(t, old)
		stored()
		resent := recovered(t, last, owed, ids, nil, stored)
		support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "a/4", Payload: []byte("last")}})
		nextLive(t, last, "a/4", "last", "after the twelve deliveries")
		if most > 2*window || resent > 2*window {
			t.Errorf("the session held up to %d exchanges in the database, and the last connection was resent %d; want at most %d, twice the window %d", most, resent, 2*window, window)
		}
	})

	for _, c := range []struct {
		name          string
		before, after int  // the old connection's window, max_inflight, and the new one's Receive Maximum
		other         bool // channel b's two deliveries, unacknowledged, are owed too
	}{
		{"QoS raised, window 4 to 1", 4, 1, false},
		{"QoS raised, window 8 to 2, acknowledgements held back", 8, 2, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			b := support.Launch(t, channelOptions(takeoverWriteTimeout, c.before))
			keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
			old, first := stalledReader(t, b.Addr, harness.V5)
			harness.ConnectedOver(t, old, keep)
			support.Suback(t, old, nil, []harness.Subscription{{Filter: "b/#", QoS: 1}, {Filter: "a/large", QoS: 0}, {Filter: "a/next/#", QoS: 1}}, 1, 0, 1)

			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			var others []*harness.Publish // channel b's, read by the old client, unacknowledged
			if c.other {
				for _, topic := range []string{"b/0", "b/1"} {
					support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: []byte(topic)}})
					others = append(others, harness.Expect[*harness.Publish](t, old))
				}
			}
			large := bytes.Repeat([]byte{'s'}, stalledPayload)
			support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "a/large", Payload: large}})
			if _, err := first(); err != nil {
				t.Fatalf("watchdog: a/large's header on the stalled connection: %v", err)
			}
			nexts := []*harness.Publish{
				{QoS: 1, Topic: "a/next/0", Payload: []byte("n0")},
				{QoS: 1, Topic: "a/next/1", Payload: []byte("n1")},
				{QoS: 1, Topic: "a/next/2", Payload: []byte("n2")},
			}
			support.PublishAll(t, pub, nexts)
			awaitExchanges(t, b, keep.ClientID, len(others)+len(nexts), "the QoS 1 deliveries'")
			raisedOnStalled(t, b, old, keep.ClientID, "a/large")
			ids := storedIDs(t, b, keep.ClientID)

			newer := tookOver(t, b.Addr, receiving(keep, uint32(c.after)), true)
			cutAfterTakeover(t, old)
			owed := map[string][]*harness.Publish{"a": append([]*harness.Publish{{QoS: 1, Topic: "a/large", Payload: large}}, nexts...)}
			if !c.other {
				recovered(t, newer, owed, ids, nil, func() {
					if err := newer.Ping(); err != nil {
						t.Fatalf("a delivery unacknowledged, Receive Maximum 1: %v", err)
					}
				})
			} else {
				owed["b"] = others
				// The window filled, held: nothing more comes until an
				// acknowledgement, and the old socket is counted closed.
				var held []*harness.Publish
				for range c.after {
					held = append(held, harness.Expect[*harness.Publish](t, newer))
				}
				if err := newer.Ping(); err != nil {
					t.Fatalf("%d deliveries unacknowledged, Receive Maximum %d: %v", c.after, c.after, err)
				}
				support.AwaitSampled(t, b, "the old connection's close", map[string]float64{connectionsOpen: 2})
				for _, p := range held {
					if err := newer.Acknowledge(p); err != nil {
						t.Fatal(err)
					}
					pinged(t, newer)
				}
				recovered(t, newer, owed, ids, held, func() { pinged(t, newer) })
			}
			support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "a/next/3", Payload: []byte("last")}})
			nextLive(t, newer, "a/next/3", "last", "after the owed deliveries")
		})
	}

	t.Run("repeated takeovers, then fresh traffic", func(t *testing.T) {
		t.Parallel()
		const window, takeovers, backlog = 2, 16, 8
		b := support.Launch(t, channelOptions(takeoverWriteTimeout, backlog+4))
		keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
		old, first := stalledReader(t, b.Addr, harness.V5)
		harness.ConnectedOver(t, old, keep)
		support.Suback(t, old, nil, []harness.Subscription{{Filter: "a/replay/#", QoS: 0}, {Filter: "a/tail/#", QoS: 1}, {Filter: "b/#", QoS: 1}}, 0, 1, 1)

		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		replay := make([]*harness.Publish, backlog) // published at QoS 1, sent at QoS 0
		for i := range replay {
			replay[i] = &harness.Publish{QoS: 1, Topic: "a/replay/" + strconv.Itoa(i), Payload: []byte("r" + strconv.Itoa(i))}
		}
		replay[0].Payload = bytes.Repeat([]byte{'s'}, stalledPayload)
		support.PublishAll(t, pub, replay[:1])
		if _, err := first(); err != nil {
			t.Fatalf("watchdog: a/replay/0's header on the stalled connection: %v", err)
		}
		support.PublishAll(t, pub, replay[1:])
		tail := []*harness.Publish{
			{QoS: 1, Topic: "a/tail/0", Payload: []byte("t0")},
			{QoS: 1, Topic: "a/tail/1", Payload: []byte("t1")},
			{QoS: 1, Topic: "a/tail/2", Payload: []byte("t2")},
		}
		support.PublishAll(t, pub, tail)
		awaitExchanges(t, b, keep.ClientID, len(tail), "the tail's")
		// The tail handed to the stalled writer, behind the replay records.
		support.AwaitSampled(t, b, "the tail in flight", map[string]float64{"saguin_inflight{}": float64(len(tail))})
		// Every replay record the old writer did not write is owed at
		// QoS 1, a new exchange each.
		raisedOnStalled(t, b, old, keep.ClientID, "a/replay/#")

		ids := storedIDs(t, b, keep.ClientID)
		c := tookOver(t, b.Addr, receiving(keep, window), true)
		cutAfterTakeover(t, old)
		// Each connection receives the first two resends, filling its
		// window, and is taken over with them unacknowledged.
		var sent []*harness.Publish
		for n := range takeovers {
			sent = sent[:0]
			for _, want := range tail[:window] {
				p := harness.Expect[*harness.Publish](t, c)
				if p.Topic != want.Topic || !p.Dup || p.ID != ids[want.Topic] {
					t.Fatalf("connection %d: PUBLISH %q, DUP %t, identifier %d; want %q resent first, DUP 1, identifier %d", n+1, p.Topic, p.Dup, p.ID, want.Topic, ids[want.Topic])
				}
				sent = append(sent, p)
			}
			if n == takeovers-1 {
				break
			}
			previous := c
			c = tookOver(t, b.Addr, receiving(keep, window), false)
			tookItsDisconnect(t, previous)
		}

		// The recovery settles: the rest of the tail resent, then the
		// replay, every record the old writer did not write, at QoS 1.
		for _, p := range sent {
			if err := c.Acknowledge(p); err != nil {
				t.Fatal(err)
			}
		}
		recovered(t, c, map[string][]*harness.Publish{"a": append(slices.Clone(replay), tail...)}, ids, sent, nil)
		awaitExchanges(t, b, keep.ClientID, 0, "after the replay,")

		// Fresh traffic flows at the full window: both deliveries come
		// before either is acknowledged.
		fresh := []*harness.Publish{
			{QoS: 1, Topic: "b/fresh/0", Payload: []byte("f0")},
			{QoS: 1, Topic: "b/fresh/1", Payload: []byte("f1")},
		}
		support.PublishAll(t, pub, fresh)
		got := make([]*harness.Publish, 0, len(fresh))
		for _, want := range fresh {
			p := harness.Expect[*harness.Publish](t, c)
			if p.Topic != want.Topic || p.Dup || !bytes.Equal(p.Payload, want.Payload) {
				t.Fatalf("fresh traffic after %d takeovers, window %d, nothing acknowledged: PUBLISH %q, DUP %t; want %q, DUP 0", takeovers, window, p.Topic, p.Dup, want.Topic)
			}
			got = append(got, p)
		}
		for _, p := range got {
			if err := c.Acknowledge(p); err != nil {
				t.Fatal(err)
			}
		}
		awaitExchanges(t, b, keep.ClientID, 0, "after the fresh traffic,")
	})
}

// design.md, Flows, CONNECT, step 5: "An old connection for the id gets
// DISCONNECT 0x8E on MQTT 5 and is closed. If its writer has not taken that
// DISCONNECT within 500 ms, its socket is cut (Closing a connection)";
// Closing a connection: "When the core cuts a connection, the reason it
// selected is the closure's cause. The failed write or read the cut causes
// records no cause of its own"; DISCONNECT, Reasons Sagüin sends: "0x8E for
// session taken over [...] These reasons are MQTT 5 only. An MQTT 3.1.1
// connection just closes"; Operations, HTTP: "refused lists, under clients,
// connections refused at CONNECT or ended by the broker with a reason, with
// one row per client id", "The reason is the latest, named as in
// metrics.md". metrics.md, saguin_disconnects_total: "the reason Sagüin
// selected for a closure, including a closure without a sendable
// DISCONNECT (design.md, DISCONNECT): [...] session_taken_over"; Rules: "An
// MQTT 3.1.1 connection closed where MQTT 5 would get a code is counted
// under that code's name".
//
// On MQTT 5 and on MQTT 3.1.1, a durable session's client stops reading
// mid-way through a delivery larger than every buffer between it and the
// broker (stalledReader), so its writer cannot take the takeover's
// DISCONNECT, and a second connection takes the session over: the old
// connection is cut. Its closure is the broker's one: once
// saguin_disconnects_total counts one more closure, it is
// session_taken_over, and the refused history has the client id's row,
// session_taken_over, counted once.
func TestTakeoverCutCountsATakeover(t *testing.T) {
	for name, version := range map[string]byte{"MQTT 5": harness.V5, "MQTT 3.1.1": harness.V311} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := support.OpsLaunch(t, &harness.Options{WriteTimeout: takeoverWriteTimeout, MaxPacketSize: 2 * stalledPayload, MaxIncomingMemory: leastIncoming(2 * stalledPayload)})
			topic := clientID(t, "/stalled")
			keep := &harness.Connect{ClientID: clientID(t, "")} // MQTT 3.1.1: Clean Session 0
			if version == harness.V5 {
				keep.Props = support.Durable
			}
			old, first := stalledReader(t, b.Addr, version)
			harness.ConnectedOver(t, old, keep)
			support.Suback(t, old, nil, support.At(1, topic), 1)
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: topic, Payload: bytes.Repeat([]byte{'s'}, stalledPayload)}})
			if _, err := first(); err != nil {
				t.Fatalf("watchdog: the delivery's header on the stalled connection: %v", err)
			}

			before := baselineClosures(t, b)
			harness.Connected(t, b.Addr, version, keep)
			if got, want := closuresSince(t, b, before), map[string]float64{disconnectSeries("session_taken_over"): 1}; !maps.Equal(got, want) {
				t.Errorf("the old connection, cut by the takeover, is counted as %v; want %v, the reason the broker selected", got, want)
			}
			h := awaitRefused(t, b, "the taken-over client's row", func(h harness.History[harness.RefusedRow]) bool {
				_, ok := refusedOf(h.Rows, keep.ClientID)
				return ok
			})
			if r, _ := refusedOf(h.Rows, keep.ClientID); r.Reason != "session_taken_over" || r.Count != 1 {
				t.Errorf("the refused history's row of %q: %s, counted %d; want session_taken_over, counted 1", keep.ClientID, r.Reason, r.Count)
			}
		})
	}
}

// recovered reads c's deliveries after a takeover until every one of owed
// has come, and returns how many came resent. owed holds, per channel, its
// deliveries in offset order, each with the QoS and payload it is owed at.
// CONNECT, step 9: the stored in-flight messages are resent first, in
// their order, "New application deliveries wait behind recovered
// PUBLISHes"; so each delivery must come once, every resend, DUP 1 under
// the identifier ids holds for its topic (the session's stored exchanges
// before the takeover, storedIDs), before every new delivery, DUP 0, and
// within each of the two, each channel's in offset order (Delivery,
// Order); channels may interleave. first are deliveries already read and
// acknowledged; each one read here is acknowledged, after between, if not
// nil, runs.
func recovered(t *testing.T, c *harness.Client, owed map[string][]*harness.Publish, ids map[string]uint16, first []*harness.Publish, between func()) int {
	t.Helper()
	type place struct {
		channel string
		index   int
	}
	where := map[string]place{} // each owed delivery's channel and place in it
	for ch, ps := range owed {
		for i, p := range ps {
			where[p.Topic] = place{ch, i}
		}
	}
	type phase struct {
		resent  bool
		channel string
	}
	last := map[phase]int{} // each phase's last place on each channel, plus one
	seen := map[string]bool{}
	resent, fresh := 0, false
	for n := range len(where) {
		read := n >= len(first)
		var p *harness.Publish
		if read {
			p = harness.Expect[*harness.Publish](t, c)
		} else {
			p = first[n]
		}
		at, ok := where[p.Topic]
		if !ok || seen[p.Topic] {
			t.Fatalf("PUBLISH %q, DUP %t: owed %t, already come %t; want each owed delivery once", p.Topic, p.Dup, ok, seen[p.Topic])
		}
		seen[p.Topic] = true
		want := owed[at.channel][at.index]
		if p.QoS != want.QoS || !bytes.Equal(p.Payload, want.Payload) || (p.QoS == 0 && p.Dup) {
			t.Fatalf("PUBLISH %q QoS %d, DUP %t, %d bytes; want QoS %d, %d bytes, DUP 0 at QoS 0", p.Topic, p.QoS, p.Dup, len(p.Payload), want.QoS, len(want.Payload))
		}
		if p.Dup {
			if fresh {
				t.Fatalf("PUBLISH %q resent, DUP 1, after a new delivery; want every resend first (CONNECT, Resume)", p.Topic)
			}
			if id, ok := ids[p.Topic]; !ok || p.ID != id {
				t.Fatalf("PUBLISH %q resent under identifier %d; want its stored exchange's, %d (stored: %t)", p.Topic, p.ID, id, ok)
			}
			resent++
		} else {
			fresh = true
		}
		key := phase{p.Dup, at.channel}
		if at.index < last[key] {
			t.Fatalf("PUBLISH %q, DUP %t, after a later delivery of channel %s; want offset order", p.Topic, p.Dup, at.channel)
		}
		last[key] = at.index + 1
		if read {
			if between != nil {
				between()
			}
			if err := c.Acknowledge(p); err != nil {
				t.Fatal(err)
			}
		}
	}
	return resent
}

// storedIDs reads client's stored exchanges (storage.md, session_inflight)
// with their records' topics, and returns each topic's packet identifier.
func storedIDs(t *testing.T, b *harness.Broker, client string) map[string]uint16 {
	t.Helper()
	ids := map[string]uint16{}
	support.Query(t, support.OpenDB(t, b.DB), `SELECT r.topic, i.packet_id FROM session_inflight i JOIN sessions s ON s.id = i.session
		JOIN records r ON r.channel = i.channel AND r."offset" = i."offset" WHERE s.client = ? ORDER BY i.seq`,
		func(rows *sql.Rows) error {
			var topic string
			var id uint16
			err := rows.Scan(&topic, &id)
			ids[topic] = id
			return err
		}, client)
	return ids
}

// cutAfterTakeover reads, on c, a stalled reader's connection
// (stalledReader) just taken over, what it still gets, and fails the test
// unless its connection ends within takeoverBound with no DISCONNECT on it:
// packets the broker wrote before, then the end of stream or a reset, a
// packet the cut left short included (readUntilClosed). CONNECT, step 5:
// "If its writer has not taken that DISCONNECT within 500 ms, its socket is
// cut (Closing a connection)"; Closing a connection: "The core cuts a
// connection only by closing its raw socket". A writer stalled mid-write
// never takes the DISCONNECT, and once the socket is closed nothing more is
// written on it.
func cutAfterTakeover(t *testing.T, c *harness.Client) {
	t.Helper()
	drainable(t, c)
	c.Timeout = takeoverBound
	readUntilClosed(t, c, func(p harness.Packet) bool {
		_, disconnect := p.(*harness.Disconnect)
		return !disconnect
	}, "what the broker wrote before the takeover, then the cut, no DISCONNECT")
}

// tookItsDisconnect reads, on c, a connection just taken over whose writer
// was free to take its DISCONNECT, and fails the test unless DISCONNECT
// 0x8E (Session taken over) is its next packet, within takeoverBound, and
// once c ends its sending, the end of stream follows. CONNECT, step 5: "An
// old connection for the id gets DISCONNECT 0x8E on MQTT 5 and is closed.
// If its writer has not taken that DISCONNECT within 500 ms, its socket is
// cut"; a writer that takes it is not cut. Closing a connection: "The
// writer handles the DISCONNECT and orderly close".
func tookItsDisconnect(t *testing.T, c *harness.Client) {
	t.Helper()
	c.Timeout = takeoverBound
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8E {
		t.Fatalf("the connection taken over got DISCONNECT 0x%02X; want 0x8E (Session taken over)", d.Reason)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
}

// raisedOnStalled has c, a stalled client, subscribe again to filter at
// QoS 1, and waits until the database holds client's subscription to it at
// QoS 1: its SUBACK waits, unread, behind the stalled delivery.
func raisedOnStalled(t *testing.T, b *harness.Broker, c *harness.Client, client, filter string) {
	t.Helper()
	if err := c.Send(&harness.Subscribe{ID: c.NextID(), Subs: support.At(1, filter)}); err != nil {
		t.Fatal(err)
	}
	db := support.OpenDB(t, b.DB)
	qos := -1
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		support.Query(t, db, `SELECT s.qos FROM subscriptions s JOIN sessions ON sessions.id = s.session WHERE sessions.client = ? AND s.filter = ?`,
			func(rows *sql.Rows) error { return rows.Scan(&qos) }, client, filter)
		return qos == 1
	}) {
		t.Fatalf("watchdog: the subscription to %s is stored at QoS %d; want 1", filter, qos)
	}
}

// channelOptions is a broker with channels a, b and c (append, a/#, b/#
// and c/#), max_inflight window, write_timeout takeoverWriteTimeout and the
// operations listener.
func channelOptions(takeoverWriteTimeout time.Duration, window int) *harness.Options {
	return &harness.Options{WriteTimeout: takeoverWriteTimeout, MaxPacketSize: 2 * stalledPayload, MaxIncomingMemory: leastIncoming(2 * stalledPayload), MaxInflight: window, Operations: true,
		Channels: []harness.Channel{{Name: "a", Type: "append", Filter: "a/#"}, {Name: "b", Type: "append", Filter: "b/#"}, {Name: "c", Type: "append", Filter: "c/#"}}}
}

// stalledOnChannels launches a broker of channelOptions, max_inflight 4,
// connects a stalled reader (stalledReader) to it with keep, a durable
// session it returns, subscribed at QoS 1 to filters, and has pub, a
// publisher, publish large, stalledPayload bytes on a/large at QoS 0; it
// returns once that delivery's header has come on the stalled reader.
func stalledOnChannels(t *testing.T, takeoverWriteTimeout time.Duration, filters ...string) (b *harness.Broker, keep *harness.Connect, old, pub *harness.Client, large []byte) {
	t.Helper()
	b = support.Launch(t, channelOptions(takeoverWriteTimeout, 4))
	keep = &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
	old, first := stalledReader(t, b.Addr, harness.V5)
	harness.ConnectedOver(t, old, keep)
	granted := bytes.Repeat([]byte{1}, len(filters))
	support.Suback(t, old, nil, support.At(1, filters...), granted...)

	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	large = bytes.Repeat([]byte{'s'}, stalledPayload)
	support.PublishAll(t, pub, []*harness.Publish{{Topic: "a/large", Payload: large}})
	if _, err := first(); err != nil {
		t.Fatalf("watchdog: a/large's header on the stalled connection: %v", err)
	}
	return b, keep, old, pub, large
}

// awaitExchanges waits, under harness.DefaultTimeout as the watchdog, until
// client's durable session holds n stored exchanges (storage.md,
// session_inflight), which what names.
func awaitExchanges(t *testing.T, b *harness.Broker, client string, n int, what string) {
	t.Helper()
	db := support.OpenDB(t, b.DB)
	var got []inflightRow
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = storedInflight(t, db, client)
		return len(got) == n
	}) {
		t.Fatalf("watchdog: session_inflight holds %+v; want %s %d exchanges", got, what, n)
	}
}

// receiving is keep with Receive Maximum n: the replacement takes n
// deliveries at a time.
func receiving(keep *harness.Connect, n uint32) *harness.Connect {
	c := *keep
	c.Props = append(harness.Properties{{ID: harness.ReceiveMaximum, Int: n}}, keep.Props...)
	return &c
}

// arrived reads c's next packet and fails the test unless it is a
// delivery of topic at qos carrying payload, DUP 0 at QoS 0
// (MQTT-3.3.1-2), which it acknowledges; when names the moment for the
// message. A QoS 1 delivery's DUP is not checked: whether it had been
// handed to the old writer, and so is a resend, is the broker's timing.
func arrived(t *testing.T, c *harness.Client, topic string, qos byte, payload []byte, when string) {
	t.Helper()
	arrivedAs(t, c, harness.Expect[*harness.Publish](t, c), topic, qos, payload, when)
}

// arrivedAs is arrived for got, the packet c has just read.
func arrivedAs(t *testing.T, c *harness.Client, got harness.Packet, topic string, qos byte, payload []byte, when string) {
	t.Helper()
	p, ok := got.(*harness.Publish)
	if !ok {
		t.Fatalf("%s: %s; want PUBLISH %q", when, harness.Name(got.Type()), topic)
	}
	if p.Topic != topic || p.QoS != qos || (qos == 0 && p.Dup) || !bytes.Equal(p.Payload, payload) {
		t.Fatalf("%s: PUBLISH %q QoS %d, DUP %t, %d bytes; want %q QoS %d, %d bytes", when, p.Topic, p.QoS, p.Dup, len(p.Payload), topic, qos, len(payload))
	}
	if err := c.Acknowledge(p); err != nil {
		t.Fatal(err)
	}
}

// tookOver takes keep's durable session over on a new connection to addr
// (takingOver, tookOverSince), which it returns; stalled says whether the
// old connection's writer is stalled mid-write.
func tookOver(t *testing.T, addr string, keep *harness.Connect, stalled bool) *harness.Client {
	t.Helper()
	c, sent := takingOver(t, addr, keep)
	tookOverSince(t, c, sent, stalled)
	return c
}

// tookOverOn takes keep's durable session over on c (connectSent,
// tookOverSince).
func tookOverOn(t *testing.T, c *harness.Client, keep *harness.Connect, stalled bool) {
	t.Helper()
	tookOverSince(t, c, connectSent(t, c, keep), stalled)
}

// takingOver dials addr in MQTT 5 and sends keep on the new connection
// (connectSent), which it returns, closed when the test ends, with when
// the CONNECT went.
func takingOver(t *testing.T, addr string, keep *harness.Connect) (*harness.Client, time.Time) {
	t.Helper()
	c, err := harness.Dial(addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	return c, connectSent(t, c, keep)
}

// connectSent sends, on c, keep, a durable session's CONNECT with Clean
// Start 0, in c's version, and returns when it went, its CONNACK left to
// read.
func connectSent(t *testing.T, c *harness.Client, keep *harness.Connect) time.Time {
	t.Helper()
	p := *keep
	p.ProtocolName, p.Version = "MQTT", c.Version
	c.Advertise(&p)
	sent := time.Now()
	if err := c.Send(&p); err != nil {
		t.Fatal(err)
	}
	return sent
}

// tookOverSince reads, on c, the CONNACK of a CONNECT sent at sent taking a
// durable session over with Clean Start 0, and fails the test unless it is
// 0x00 with Session Present 1, come within takeoverBound of sent, and,
// where the old connection's writer is stalled mid-write, no sooner than
// takeoverCut: such a writer cannot take its DISCONNECT, so its socket is
// cut only then, and the takeover "follows its writer's reports" (CONNECT,
// step 5), which only the cut brings.
func tookOverSince(t *testing.T, c *harness.Client, sent time.Time, stalled bool) {
	t.Helper()
	c.Timeout = max(time.Until(sent.Add(takeoverBound)), time.Millisecond)
	p, err := c.Recv()
	took := time.Since(sent).Round(time.Millisecond)
	c.Timeout = 0
	if err != nil {
		t.Fatalf("the takeover: no CONNACK %v after its CONNECT: %v; want it within %v", took, err, takeoverBound)
	}
	ack, ok := p.(*harness.Connack)
	switch {
	case !ok:
		t.Fatalf("the takeover: %s before CONNACK", harness.Name(p.Type()))
	case ack.Reason != 0 || !ack.SessionPresent:
		t.Fatalf("the takeover: CONNACK 0x%02X, Session Present %t; want 0x00, Session Present 1", ack.Reason, ack.SessionPresent)
	case stalled && took < takeoverCut:
		t.Fatalf("the takeover: CONNACK %v after its CONNECT; want it no sooner than %v, when the old connection's stalled writer is cut", took, takeoverCut)
	}
}

// stalledResent reads c's next packet and fails the test unless it is the
// stalled QoS 1 delivery of topic resent, DUP 1, under identifier id, which
// it returns; when names the moment for the message.
func stalledResent(t *testing.T, c *harness.Client, topic string, id uint16, when string) *harness.Publish {
	t.Helper()
	p := harness.Expect[*harness.Publish](t, c)
	if p.Topic != topic || p.QoS != 1 || !p.Dup || p.ID != id || len(p.Payload) != stalledPayload {
		t.Fatalf("%s: PUBLISH %q QoS %d, DUP %t, identifier %d, %d bytes; want the stalled delivery resent: %q QoS 1, DUP 1, identifier %d, %d bytes",
			when, p.Topic, p.QoS, p.Dup, p.ID, len(p.Payload), topic, id, stalledPayload)
	}
	return p
}

// nextLive reads c's next packet and fails the test unless it is a first
// delivery of topic carrying payload, which it acknowledges; when names the
// moment for the message.
func nextLive(t *testing.T, c *harness.Client, topic, payload, when string) {
	t.Helper()
	p := harness.Expect[*harness.Publish](t, c)
	if p.Topic != topic || p.Dup || string(p.Payload) != payload {
		t.Fatalf("%s: PUBLISH %q, DUP %t, %d bytes; want %q, DUP 0, payload %q", when, p.Topic, p.Dup, len(p.Payload), topic, payload)
	}
	if err := c.Acknowledge(p); err != nil {
		t.Fatal(err)
	}
}

// stalledPayload is the payload of TestTakeoverDoesNotWaitForAStalledWriter's
// delivery: 16 MiB, four times the largest send buffer Linux gives a TCP
// socket by default (net.ipv4.tcp_wmem, 4 MiB), the stalled reader's own
// receive buffer the smallest the system allows.
const stalledPayload = 16 << 20

// drainedPayload is the size of TestTakeoverDoesNotWaitForAStalledWriter's
// delivery to a client that reads on when taken over: 1 MiB more than the
// largest send buffer this machine gives a TCP socket (sendBufferMost; 4 MiB
// by Linux's default) and that client's receive buffer (drainingBuffers)
// hold together, so the old writer is mid-write when the takeover comes;
// and no more, so that what the writer still has to write once the client
// reads on, within the takeover's 500 ms cut, is that 1 MiB and whatever
// part of the largest send buffer the broker's socket did not grow to.
func drainedPayload(t *testing.T) int {
	t.Helper()
	return sendBufferMost(t) + 2*drainingReceive + 1<<20
}

// drainingReceive is drainingBuffers' receive buffer, before Linux doubles
// it (socket(7)).
const drainingReceive = 64 << 10

// drainingBuffers dials as a client that takes nothing the broker writes
// until it reads on: its receive buffer, drainingReceive, set before it
// connects, holds whole loopback segments, so its window closes once it is
// full and, when it reads on, what was sent comes at loopback's speed, with
// none of the drops and retransmissions that make smallBuffers' reads crawl
// (drainable): measured here, 16 MiB drained in 56 to 66 ms this way,
// against 300 to 590 ms through smallBuffers.
var drainingBuffers = &net.Dialer{Timeout: harness.DefaultTimeout, Control: func(_, _ string, raw syscall.RawConn) error {
	var err error
	if cerr := raw.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, drainingReceive)
	}); cerr != nil {
		return cerr
	}
	return err
}}

// stalledReader dials addr in version, as a client that takes nothing the
// broker writes (smallBuffers) but the start of its first PUBLISH, and
// returns it with first, which waits for that PUBLISH's fixed header, Topic
// Name and packet identifier, under harness.DefaultTimeout as the watchdog,
// without taking them from the packets the client reads, and returns the
// identifier, 0 at QoS 0. The connection is closed when the test ends.
func stalledReader(t *testing.T, addr string, version byte) (c *harness.Client, first func() (uint16, error)) {
	t.Helper()
	return stalledReaderOver(t, smallBuffers, addr, version)
}

// stalledReaderOver is stalledReader, dialled by d, smallBuffers or
// drainingBuffers.
func stalledReaderOver(t *testing.T, d *net.Dialer, addr string, version byte) (c *harness.Client, first func() (uint16, error)) {
	t.Helper()
	raw, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn := &peekConn{Conn: raw, r: bufio.NewReaderSize(raw, 4<<10)}
	c = harness.NewClient(conn, version)
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	return c, func() (uint16, error) {
		if err := conn.SetReadDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
			return 0, err
		}
		return conn.publishID()
	}
}

// peekConn is a connection read through r, so a test can see that bytes
// have come without taking them.
type peekConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// CloseWrite ends the client's sending, as its TCP connection's does.
func (c *peekConn) CloseWrite() error { return c.Conn.(*net.TCPConn).CloseWrite() }

// publishID peeks at the PUBLISH that comes next on c: its fixed header,
// whose Remaining Length is a Variable Byte Integer (MQTT 5 1.5.5), its
// Topic Name, a two-byte length and its bytes (1.5.4), and, above QoS 0,
// its two-byte Packet Identifier (3.3.2.2), which it returns.
func (c *peekConn) publishID() (uint16, error) {
	n := 1 // the fixed header's bytes, its first and the Remaining Length's
	for {
		h, err := c.r.Peek(n + 1)
		if err != nil {
			return 0, err
		}
		n++
		if h[n-1]&0x80 == 0 {
			break
		}
	}
	h, err := c.r.Peek(n + 2)
	if err != nil {
		return 0, err
	}
	if kind := h[0] >> 4; kind != harness.PUBLISH {
		return 0, fmt.Errorf("the first packet is %s, want PUBLISH", harness.Name(kind))
	}
	if (h[0]>>1)&3 == 0 {
		return 0, nil
	}
	at := n + 2 + int(binary.BigEndian.Uint16(h[n:]))
	if h, err = c.r.Peek(at + 2); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(h[at:]), nil
}

// design.md, Flows, CONNECT: "A takeover of a session whose expiry is 0 ends
// it and starts a new one"; 5: "Its Will is published if it has no delay; a
// delayed Will stays with the session. If takeover ends the old session, its
// Will is due at that session end regardless of Will Delay. A delayed Will is
// cancelled only by reconnection to the still-existing session before its due
// time"; 6: "Clean start ends an existing session and publishes its delayed
// Will. Keeping the session resumes it and cancels the delayed Will"; 7-8:
// those changes stored, then the CONNACK. MQTT 5 3.1.4: "If the existing
// Client has a Will Message, that Will Message is published as described in
// section 3.1.2.5", and: "If the Will Delay Interval of the existing Network
// Connection is 0 and there is a Will Message, it will be sent because the
// Network Connection is closed. If the Session Expiry Interval of the
// existing Network Connection is 0, or the new Network Connection has Clean
// Start set to 1 then if the existing Network Connection has a Will Message
// it will be sent because the original Session is ended on the takeover";
// [MQTT-3.1.3-9]: a new Network Connection to the Session before the Will
// Delay Interval has passed, and "the Server MUST NOT send the Will Message".
// MQTT 3.1.1 [MQTT-3.1.4-2]: "the Server MUST disconnect the existing
// Client"; [MQTT-3.1.2-8]: its Will "MUST be published when the Network
// Connection is subsequently closed".
//
// A client connects with a Will on a topic a subscriber holds, and a
// second connection with its client id and no Will takes it over:
//   - a Will with no delay, the durable session kept (Clean Start 0), in
//     MQTT 5 and 3.1.1: the subscriber receives the Will;
//   - a Will of Will Delay Interval 1 second, the durable session kept:
//     Session Present 1, the database then holds no Will for the client,
//     and once a second has passed since the takeover, a message published
//     on the topic is the subscriber's next packet, the Will never sent;
//   - a Will of Will Delay Interval an hour, the takeover with Clean Start
//     1: the subscriber receives the Will;
//   - a Will of Will Delay Interval an hour, the old session of Session
//     Expiry 0, the takeover with Clean Start 0: the subscriber receives
//     the Will.
//
// Where the Will is sent, the database then holds it no more.
func TestTakeoverAndTheOldConnectionsWill(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	kept := harness.Connect{CleanStart: true, Props: support.Durable}
	cases := []struct {
		name     string
		version  byte
		old      harness.Connect // the first connection's CONNECT, but its client id and Will
		delay    uint32          // its Will's Will Delay Interval, in seconds
		takeover harness.Connect // the second's, but its client id
		sent     bool            // the Will is published
	}{
		{"no delay, the session kept", harness.V5, kept, 0, harness.Connect{Props: support.Durable}, true},
		{"no delay, the session kept, MQTT 3.1.1", harness.V311, harness.Connect{}, 0, harness.Connect{}, true},
		{"delayed, the session kept", harness.V5, kept, 1, harness.Connect{Props: support.Durable}, false},
		{"delayed, the takeover starting clean", harness.V5, kept, support.Hour, harness.Connect{CleanStart: true}, true},
		{"delayed, the old session ending with its connection", harness.V5, harness.Connect{CleanStart: true}, support.Hour, harness.Connect{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			topic := clientID(t, "/will")
			sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
			id := clientID(t, "")
			c.old.ClientID, c.takeover.ClientID = id, id
			c.old.Will = &harness.Will{Topic: topic, Payload: support.WillPayload}
			if c.delay != 0 {
				c.old.Will.Props = support.WillDelay(c.delay)
			}
			harness.Connected(t, b.Addr, c.version, &c.old)
			_, ack := harness.Connected(t, b.Addr, c.version, &c.takeover)
			took := time.Now() // the core took the takeover before its CONNACK came
			if c.sent {
				live(t, sub, topic, support.WillPayload)
				awaitWillGone(t, db, id, "its publication")
				return
			}
			if !ack.SessionPresent {
				t.Fatal("the takeover keeping the session got Session Present 0; want 1")
			}
			if _, held := support.StoredWill(t, db, id); held {
				t.Error("after the takeover's CONNACK, the database still holds the old connection's Will; want it cancelled")
			}
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			time.Sleep(time.Until(took.Add(time.Duration(c.delay)*time.Second + tick)))
			thenLive(t, pub, sub, topic)
		})
	}
}

// design.md, DISCONNECT: "The client may change its session expiry, except
// from 0, which is a protocol error. The change is stored before the
// connection closes"; "The session goes away until its expiry"; "When the
// expiry runs out, [...] the session deleted"; config.md, max_session_expiry:
// "caps session expiry, and is MQTT 3.1.1 Clean Session 0's". MQTT 5
// 3.1.2.11.2: "If it is set to 0, or is absent, the Session ends when the
// Network Connection is closed", and 3.14.2.2.2: "If the Session Expiry
// Interval is absent, the Session Expiry Interval in the CONNECT packet is
// used". A reconnect with Clean Start 0 tells, by Session Present, whether
// the session is still there (CONNECT: "a session exists for the client id").
func TestSessionExpiry(t *testing.T) {
	t.Parallel()
	const most = 4 * time.Second // max_session_expiry
	b := support.Launch(t, &harness.Options{MaxSessionExpiry: most})
	atMost := support.SessionExpiry(uint32(most / time.Second))
	type session struct {
		name    string
		version byte
		connect harness.Connect
		end     *harness.Disconnect // nil: the connection drops
		present bool                // after the wait
		wait    time.Duration       // from the end until the reconnect
	}
	sessions := []session{
		{"before its expiry", harness.V5, harness.Connect{Props: atMost}, nil, true, 0},
		{"after its expiry, capped by max_session_expiry", harness.V5, harness.Connect{Props: support.SessionExpiry(0xFFFFFFFF)}, nil, false, most},
		{"after a DISCONNECT lowering it", harness.V5, harness.Connect{Props: atMost},
			&harness.Disconnect{Props: support.SessionExpiry(1)}, false, time.Second},
		{"after a DISCONNECT setting 0", harness.V5, harness.Connect{Props: atMost},
			&harness.Disconnect{Props: support.SessionExpiry(0)}, false, 0},
		{"MQTT 3.1.1 Clean Session 0, before max_session_expiry", harness.V311, harness.Connect{}, nil, true, 0},
		{"MQTT 3.1.1 Clean Session 0, after max_session_expiry", harness.V311, harness.Connect{}, nil, false, most},
	}
	ended := make([]time.Time, len(sessions))
	for i := range sessions {
		s := &sessions[i]
		s.connect.ClientID = clientID(t, s.name)
		c, _ := harness.Connected(t, b.Addr, s.version, &s.connect)
		if s.end != nil {
			if err := c.Send(s.end); err != nil {
				t.Fatal(err)
			}
			harness.ExpectClosed(t, c)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		ended[i] = time.Now()
	}
	// Reconnect in order of the waits, each once its wait, and the time the
	// broker may take to see the end, have passed.
	for _, wait := range []time.Duration{0, time.Second, most} {
		for i, s := range sessions {
			if s.wait != wait {
				continue
			}
			if wait != 0 {
				time.Sleep(time.Until(ended[i].Add(wait + support.Late)))
			}
			_, ack := harness.Connected(t, b.Addr, s.version, &harness.Connect{ClientID: s.connect.ClientID})
			if ack.SessionPresent != s.present {
				t.Errorf("%s: Session Present %v, want %v", s.name, ack.SessionPresent, s.present)
			}
		}
	}
}

// design.md, Store reads: "Every other read used for a broker decision has
// a place in the core's store-request order. Earlier accepted writes are resolved
// before its view is fixed"; DISCONNECT, A lost connection: "Each time its
// timer comes, the core goes once through the sessions due, in expiry order.
// It then sets the timer for the next expiry still to come"; "When the expiry
// runs out, [...] the session deleted"; A clean DISCONNECT: the session's
// expiry "is stored before the connection closes". MQTT 5 3.1.2.11.2: the
// Session Expiry Interval is in seconds, and the state is kept after the
// connection closes while it is above 0 [MQTT-3.1.2-23].
//
// Two MQTT 5 clients hold sessions of Session Expiry 1 s. The first sends
// DISCONNECT and its connection closes: its going away is stored, its
// expiry a second away. The test then holds the database's write lock, as
// another program on the file may, and the second sends DISCONNECT: the
// store writer meets the lock, the second's going away accepted, not yet
// stored, when the first's expiry comes and the sweep's read takes its
// turn. Past that, the lock is released: the second's going away is
// stored, its connection closes, and its expiry has run out too. Each
// session is then ended by the sweep, its row gone from the database, the
// broker still running: the read, its view fixed after the second's going
// away was stored, found both due.
func TestSweepSeesADepartureAcceptedBeforeIt(t *testing.T) {
	t.Parallel()
	b := launchTracingLocks(t, &harness.Options{})
	keep := func(id string) *harness.Client {
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.SessionExpiry(1)})
		return c
	}
	first, second := keep("first"), keep("second")
	if err := first.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, first)
	gone := time.Now() // the first's expiry runs out no later than a second from here

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	if err := second.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	awaitLockMet(t, b.trace, before, "the second DISCONNECT")
	time.Sleep(time.Until(gone.Add(time.Second + support.Late)))
	lock.release(t)
	harness.ExpectClosed(t, second)

	db := support.OpenDB(t, b.DB)
	ended := func() bool { return sessionRows(t, db, "first")+sessionRows(t, db, "second") == 0 }
	if !testkit.Within(harness.DefaultTimeout, ended) {
		t.Errorf("the database still holds %d sessions rows of first and %d of second, their expiries past; want the sweep to have ended both",
			sessionRows(t, db, "first"), sessionRows(t, db, "second"))
	}
}

// design.md, Flows, Closing a connection, 3: "Only then does the writer
// discard what the client still sends, until the client's end of stream or
// the deadline, and close."; and below it: "A client that stops in time
// sees an orderly close". A connection the broker
// closes, by a refusal, a takeover or its DISCONNECT, while the client
// still sends: the client receives everything the broker sent (PINGRESPs
// to the PINGREQs read before the close, then the closing packet), goes on
// sending without a reset, stops, and reads an end of stream. The broker
// still holds unread PINGREQs when it closes: they go in the same write as
// what makes it close or, for a takeover, just before the new CONNECT. A
// close that discarded nothing would answer them with a reset, which the
// client's next write meets.
func TestOrderlyCloseWhileTheClientSends(t *testing.T) {
	more := bytes.Repeat(pingreq, 32<<10) // 64 KiB
	refused := func(version byte) func(t *testing.T, c *harness.Client) {
		return func(t *testing.T, c *harness.Client) {
			harness.SendBytes(t, c, append(harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: version}, version), more...))
		}
	}
	cases := []struct {
		name    string
		version byte
		close   func(t *testing.T, c *harness.Client) // makes the broker close c's connection while c sends
		want    harness.Packet                        // the closing packet
	}{
		{"a refusal", harness.V5, refused(harness.V5), &harness.Connack{Reason: 0x85}},
		{"a refusal, MQTT 3.1.1", harness.V311, refused(harness.V311), &harness.Connack{Reason: 2}},
		{"a takeover", harness.V5, func(t *testing.T, c *harness.Client) {
			id := clientID(t, "")
			if _, err := c.Connect(&harness.Connect{ClientID: id, CleanStart: true}); err != nil {
				t.Fatal(err)
			}
			harness.SendBytes(t, c, more)
			harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true})
		}, &harness.Disconnect{Reason: 0x8E}},
		{"its DISCONNECT", harness.V5, func(t *testing.T, c *harness.Client) {
			if _, err := c.Connect(&harness.Connect{ClientID: clientID(t, ""), CleanStart: true}); err != nil {
				t.Fatal(err)
			}
			harness.SendBytes(t, c, append([]byte{0xC0, 0x01, 0x00}, more...)) // first a PINGREQ with a body: malformed
		}, &harness.Disconnect{Reason: 0x81}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl, err := harness.Dial(brokerAddr, c.version)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cl.Close() }) // the test is over
			c.close(t, cl)
			for {
				got, err := cl.Recv()
				if err != nil {
					t.Fatalf("waiting for %s: %v", harness.Name(c.want.Type()), err)
				}
				if _, ok := got.(*harness.Pingresp); ok {
					continue
				}
				if !samePacket(got, c.want) {
					t.Fatalf("read %+v, want %+v", got, c.want)
				}
				break
			}
			if err := cl.SendRaw(more); err != nil {
				t.Fatalf("sending after the close began: %v, want the broker to discard it", err)
			}
			if err := cl.CloseWrite(); err != nil {
				t.Fatalf("ending what the client sends: %v", err)
			}
			harness.ExpectClosed(t, cl)
		})
	}
}

// samePacket reports whether got is want's type with want's reason code.
func samePacket(got, want harness.Packet) bool {
	switch w := want.(type) {
	case *harness.Connack:
		g, ok := got.(*harness.Connack)
		return ok && g.Reason == w.Reason
	case *harness.Disconnect:
		g, ok := got.(*harness.Disconnect)
		return ok && g.Reason == w.Reason
	}
	return false
}

// willFires connects, to b in version, a subscriber to a topic of the
// test's own, then a client of the test's own with connect, which it
// completes: its client id, and a Will of willPayload on that topic, at QoS
// 0, not retained and with no Will Delay Interval. lose then ends the
// client's connection, and the test fails unless the subscriber's next
// packet is the Will, a live delivery, and the database then no longer
// holds it (design.md, DISCONNECT: "A Will is published and removed in one
// transaction"; MQTT 5 [MQTT-3.1.2-10], 3.1.1 [MQTT-3.1.2-10]).
func willFires(t *testing.T, b *harness.Broker, version byte, connect *harness.Connect, lose func(t *testing.T, c *harness.Client)) {
	t.Helper()
	topic := clientID(t, "/will")
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
	connect.ClientID = clientID(t, "")
	connect.Will = &harness.Will{Topic: topic, Payload: support.WillPayload}
	c, _ := harness.Connected(t, b.Addr, version, connect)
	lose(t, c)
	live(t, sub, topic, support.WillPayload)
	awaitWillGone(t, support.OpenDB(t, b.DB), connect.ClientID, "its publication")
}

// design.md, DISCONNECT, A lost connection: "The Will goes after its Will
// Delay, or at once for MQTT 3.1.1"; A clean DISCONNECT: "0x00 discards the
// Will. The reason 0x04 keeps it [...] So does any other reason code, as a
// lost connection does"; "A keep-alive timeout closes the connection with
// 0x8D". MQTT 5 3.1.2.5: the Will "MUST be published after the Network
// Connection is subsequently closed and either the Will Delay Interval has
// elapsed or the Session ends" [MQTT-3.1.2-8], and among the situations in
// which it is published: "An I/O error or network failure detected by the
// Server", "The Client fails to communicate within the Keep Alive time", "The
// Client closes the Network Connection without first sending a DISCONNECT
// packet with a Reason Code 0x00", "The Server closes the Network Connection
// without first receiving a DISCONNECT packet with a Reason Code 0x00";
// 3.1.3.2.2: with no Will Delay Interval "there is no delay before the Will
// Message is published"; 3.1.0: "The Server MUST process a second CONNECT
// packet sent from a Client as a Protocol Error and close the Network
// Connection" [MQTT-3.1.0-2]. MQTT 3.1.1 [MQTT-3.1.2-8]: "The Will Message
// MUST be published when the Network Connection is subsequently closed unless
// the Will Message has been deleted by the Server on receipt of a DISCONNECT
// Packet", the situations including "The Server closes the Network Connection
// because of a protocol error".
//
// Clients of both versions, their sessions durable, so that no session
// end publishes the Will, each lose their connection: the client drops its
// socket; it stays silent past its keep-alive of 1 second; it sends a
// second CONNECT; and, in MQTT 5, it sends DISCONNECT 0x80, Unspecified
// error. Each time a subscriber then receives the Will, and the database
// no longer holds it.
func TestLostConnectionPublishesTheWill(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	ways := []struct {
		name      string
		only5     bool   // a way MQTT 3.1.1 does not have
		keepAlive uint16 // the client's; 0 for none
		lose      func(t *testing.T, c *harness.Client)
	}{
		{"the client drops its socket", false, 0, func(t *testing.T, c *harness.Client) {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"a keep-alive timeout", false, 1, func(*testing.T, *harness.Client) {}}, // silent: the broker closes it
		{"a protocol error, a second CONNECT", false, 0, func(t *testing.T, c *harness.Client) {
			if err := c.Send(&harness.Connect{ProtocolName: "MQTT", Version: c.Version, ClientID: "again", CleanStart: true}); err != nil {
				t.Fatal(err)
			}
		}},
		{"DISCONNECT 0x80", true, 0, func(t *testing.T, c *harness.Client) {
			support.Disconnected(t, c, &harness.Disconnect{Reason: 0x80})
		}},
	}
	for _, w := range ways {
		for _, version := range []byte{harness.V5, harness.V311} {
			if w.only5 && version != harness.V5 {
				continue
			}
			t.Run(w.name+", "+harness.VersionName(version), func(t *testing.T) {
				t.Parallel()
				// Durable: MQTT 5 by its expiry, MQTT 3.1.1 by Clean Session 0.
				connect := harness.Connect{KeepAlive: w.keepAlive}
				if version == harness.V5 {
					connect.CleanStart, connect.Props = true, support.Durable
				}
				willFires(t, b, version, &connect, w.lose)
			})
		}
	}
}

// design.md, DISCONNECT, A clean DISCONNECT: "0x04 keeps it for
// publication at the earlier of Will Delay ending and the session ending".
// MQTT 5 3.14.2.1, Table 3-10: 0x04, Disconnect with Will Message, "The
// Client wishes to disconnect but requires that the Server also publishes
// its Will Message"; 3.1.3.2.2: with no Will Delay Interval "there is no
// delay before the Will Message is published".
//
// MQTT 5 clients, of Session Expiry 0 and durable, with a Will and no Will
// Delay Interval, send DISCONNECT 0x04 and read the end of their
// connections: each time a subscriber then receives the Will, and the
// database no longer holds it.
func TestDisconnectWithWillMessagePublishesIt(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	sessions := []struct {
		name  string
		props harness.Properties
	}{{"Session Expiry 0", nil}, {"durable", support.Durable}}
	for _, s := range sessions {
		t.Run(s.name, func(t *testing.T) {
			willFires(t, b, harness.V5, &harness.Connect{CleanStart: true, Props: s.props}, func(t *testing.T, c *harness.Client) {
				support.Disconnected(t, c, &harness.Disconnect{Reason: 0x04})
			})
		})
	}
}

// awaitWillGone waits until q's database holds no Will for client, failing
// the test where it still holds one harness.DefaultTimeout after what ended
// the client's connection, which after names. A Will leaves the store when
// it is published, discarded or dropped: design.md, DISCONNECT, "A Will is
// published and removed in one transaction"; MQTT 5 [MQTT-3.1.2-10]: "The
// Will Message MUST be removed from the stored Session State in the Server
// once it has been published or the Server has received a DISCONNECT packet
// with a Reason Code of 0x00"; MQTT 3.1.1 [MQTT-3.1.2-10].
func awaitWillGone(t *testing.T, q support.RowQuerier, client, after string) {
	t.Helper()
	gone := func() bool {
		_, held := support.StoredWill(t, q, client)
		return !held
	}
	if !testkit.Within(harness.DefaultTimeout, gone) {
		t.Fatalf("%v after %s, the database still holds the Will of %q", harness.DefaultTimeout, after, client)
	}
}

// design.md, DISCONNECT, A clean DISCONNECT: "0x00 discards the Will";
// "An MQTT 3.1.1 DISCONNECT always discards the Will". MQTT 5
// [MQTT-3.14.4-3]: on receipt of DISCONNECT with Reason Code 0x00 the Server
// "MUST discard any Will Message associated with the current Connection
// without publishing it"; [MQTT-3.1.2-10]: it "MUST be removed from the
// stored Session State". MQTT 3.1.1 [MQTT-3.14.4-3] and [MQTT-3.1.2-10] the
// same of any DISCONNECT.
//
// Clients of both versions, their sessions ending with their connections
// or durable, connect with a Will on a topic a subscriber holds, which the
// database then holds (TestWillIsStoredBeforeTheConnack), and send
// DISCONNECT, Normal disconnection: once the Will has left the database,
// a message published on the topic is the subscriber's next packet, the
// Will never sent.
func TestNormalDisconnectDiscardsTheWill(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	cases := []struct {
		name    string
		version byte
		connect harness.Connect
	}{
		{"MQTT 5, Session Expiry 0", harness.V5, harness.Connect{CleanStart: true}},
		{"MQTT 5, durable", harness.V5, harness.Connect{CleanStart: true, Props: support.Durable}},
		{"MQTT 3.1.1, Clean Session 1", harness.V311, harness.Connect{CleanStart: true}},
		{"MQTT 3.1.1, Clean Session 0", harness.V311, harness.Connect{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			topic := clientID(t, "/will")
			sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
			c.connect.ClientID = clientID(t, "")
			c.connect.Will = &harness.Will{Topic: topic, Payload: support.WillPayload}
			cl, _ := harness.Connected(t, b.Addr, c.version, &c.connect)
			if _, ok := support.StoredWill(t, db, c.connect.ClientID); !ok {
				t.Fatal("after the CONNACK the database holds no Will for the client: its DISCONNECT has nothing to discard")
			}
			support.Away(t, cl)
			awaitWillGone(t, db, c.connect.ClientID, "its DISCONNECT")
			thenLive(t, pub, sub, topic)
		})
	}
}

// tick is the resolution of the times the store holds (storage.md: "Times
// are Unix milliseconds, UTC"): a due time read from the database may lie
// up to a tick from the instant the broker timed.
const tick = time.Millisecond

// willArrives reads c's next packet, failing the test unless it is the live
// delivery at QoS 0 of a Will of willPayload on topic (live), and returns
// when it was read.
func willArrives(t *testing.T, c *harness.Client, topic string) time.Time {
	t.Helper()
	live(t, c, topic, support.WillPayload)
	return time.Now()
}

// design.md, DISCONNECT, A lost connection: "The Will goes after its Will
// Delay, or at once for MQTT 3.1.1. It is read from the store when it goes";
// A clean DISCONNECT: "0x04 keeps it for publication at the earlier of Will
// Delay ending and the session ending"; "A Will is published and removed in
// one transaction". MQTT 5 3.1.3.2.2: "The Server delays publishing the
// Client's Will Message until the Will Delay Interval has passed or the
// Session ends, whichever happens first"; 3.1.2.5: the Will "MUST be
// published after the Network Connection is subsequently closed and either
// the Will Delay Interval has elapsed or the Session ends" [MQTT-3.1.2-8],
// and "MUST be removed from the stored Session State in the Server once it
// has been published" [MQTT-3.1.2-10].
//
// MQTT 5 clients of durable sessions, an hour's expiry, connect with a
// Will of Will Delay Interval 1 second, the smallest delay there is, on a
// topic a subscriber holds, and end their connections: one drops its
// socket, one sends DISCONNECT 0x04. The subscriber receives each Will no
// sooner than a second after its connection ended; the database then
// holds the Will no more, and still holds the session: the Will went by
// its delay, the session living on.
func TestDelayedWillGoesAfterItsDelay(t *testing.T) {
	const delay = time.Second
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	ways := []struct {
		name string
		end  func(t *testing.T, c *harness.Client)
	}{
		{"the client drops its socket", support.DropSocket},
		{"DISCONNECT 0x04", func(t *testing.T, c *harness.Client) {
			support.Disconnected(t, c, &harness.Disconnect{Reason: 0x04})
		}},
	}
	for _, w := range ways {
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()
			topic := clientID(t, "/will")
			sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
			id := clientID(t, "")
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable,
				Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: support.WillDelay(uint32(delay / time.Second))}})
			gone := time.Now() // before the broker can see the connection end
			w.end(t, c)
			if waited := willArrives(t, sub, topic).Sub(gone); waited < delay-tick {
				t.Errorf("the Will came %v after the connection ended; want it no sooner than its Will Delay Interval, %v", waited, delay)
			}
			awaitWillGone(t, db, id, "its publication")
			if n := sessionRows(t, db, id); n != 1 {
				t.Errorf("once the Will went, the database holds %d sessions rows for its client; want 1, the session living on", n)
			}
		})
	}
}

// design.md, Flows, CONNECT, 5: "A delayed Will is cancelled only by
// reconnection to the still-existing session before its due time, judged when
// the core takes the CONNECT. A CONNECT taken at or after the due time is too
// late"; 6: "Keeping the session resumes it and cancels the delayed Will"; 7:
// "The core stores step 6's changes [...] in one transaction"; 8: "Once the
// changes are stored, the broker sends CONNACK". MQTT 5 3.1.3.2.2: "If a new
// Network Connection to this Session is made before the Will Delay Interval
// has passed, the Server MUST NOT send the Will Message" [MQTT-3.1.3-9];
// 3.1.2.5: otherwise it "MUST be published after the Network Connection is
// subsequently closed and either the Will Delay Interval has elapsed or the
// Session ends" [MQTT-3.1.2-8].
//
// The broker runs under strace (launchTracingLocks). MQTT 5 clients of
// durable sessions connect with a Will of Will Delay Interval 1 second on
// a topic a subscriber holds, and drop their sockets:
//   - In time: once the database gives the Will its due time, the client
//     reconnects before it, with Clean Start 0 and no Will: Session
//     Present 1, and the database then holds no Will for it. Once the due
//     time has passed, a message published on the topic is the
//     subscriber's next packet, the Will never sent.
//   - Too late: the test holds the database's write lock, as another
//     program on the file may, so the going away waits at the store. Once
//     the store writer has met the lock, the due time can lie no later than
//     a second away; past it, the client reconnects with Clean Start 0 and
//     no Will, its CONNECT taken while the Will is still in the store,
//     unpublished. Once the lock is released, the reconnect gets Session
//     Present 1, the subscriber receives the Will, and the database then
//     holds it no more.
func TestReconnectInTimeCancelsTheDelayedWill(t *testing.T) {
	const delay = time.Second
	b := launchTracingLocks(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	// delayed connects a client of the test's own with a delayed Will on a
	// topic a new subscriber holds, and returns the client, its id, the
	// subscriber and the topic. The client's socket is the caller's to
	// drop.
	delayed := func(t *testing.T) (*harness.Client, string, *harness.Client, string) {
		topic := clientID(t, "/will")
		sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
		id := clientID(t, "")
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable,
			Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: support.WillDelay(uint32(delay / time.Second))}})
		return c, id, sub, topic
	}

	t.Run("in time", func(t *testing.T) {
		c, id, sub, topic := delayed(t)
		support.DropSocket(t, c)
		due := support.AwaitWillDue(t, db, id)
		if !time.Now().Before(due) {
			t.Fatalf("the Will's due time had passed before the reconnect could be sent: the test was too slow to check a reconnect in time")
		}
		resumed(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable})
		if _, held := support.StoredWill(t, db, id); held {
			t.Errorf("after the CONNACK of the reconnect in time, the database still holds the Will; want it cancelled")
		}
		time.Sleep(time.Until(due.Add(tick)))
		thenLive(t, pub, sub, topic)
	})

	t.Run("too late", func(t *testing.T) {
		c, id, sub, topic := delayed(t)
		lock := holdWriteLock(t, b.DB)
		before := len(traced(t, b.trace))
		support.DropSocket(t, c)
		awaitLockMet(t, b.trace, before, "the dropped connection")
		// The core took the going away before its write met the lock, so
		// the Will's due time lies no later than delay from now.
		time.Sleep(delay + tick)
		again, err := harness.Dial(b.Addr, harness.V5)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = again.Close() }) // the test is over
		if err := again.Send(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: id, Props: support.Durable}); err != nil {
			t.Fatal(err)
		}
		lock.release(t)
		if ack := harness.Expect[*harness.Connack](t, again); ack.Reason != 0 || !ack.SessionPresent {
			t.Errorf("the reconnect past the due time got CONNACK 0x%02X, Session Present %v; want 0x00, Session Present 1",
				ack.Reason, ack.SessionPresent)
		}
		live(t, sub, topic, support.WillPayload)
		awaitWillGone(t, db, id, "its publication")
	})
}

// design.md, DISCONNECT, A lost connection: the Will "If the session ends
// first, the Will goes early"; "When the expiry runs out, the delayed Will is
// published and the session deleted, in one transaction"; A clean DISCONNECT:
// "0x04 keeps it for publication at the earlier of Will Delay ending and the
// session ending"; "The client may change its session expiry"; R5: "Consumers
// read only committed records". MQTT 5 3.1.3.2.2: the Will waits "until the
// Will Delay Interval has passed or the Session ends, whichever happens
// first"; 3.1.2.11.2: with a Session Expiry Interval of 0 "the Session ends
// when the Network Connection is closed"; 3.1.2.5: "The Client can arrange
// for the Will Message to notify that Session Expiry has occurred by setting
// the Will Delay Interval to be longer than the Session Expiry Interval and
// sending DISCONNECT with Reason Code 0x04"; [MQTT-3.1.2-10]: the Will is
// removed once published.
//
// MQTT 5 clients connect with a Will of Will Delay Interval an hour,
// longer than their sessions, on a topic a subscriber holds: one of
// Session Expiry 1 second drops its socket; one of an hour's expiry sends
// DISCONNECT 0x04 lowering it to 1 second; one of Session Expiry 0 drops
// its socket. The subscriber receives each Will, the first two no sooner
// than a second after the connection ended. Once it has, the database
// holds neither the Will nor the session's row.
func TestSessionEndingFirstSendsTheWillEarly(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	sessions := []struct {
		name  string
		props harness.Properties // the CONNECT's
		end   func(t *testing.T, c *harness.Client)
		lasts time.Duration // the session's life past its connection
	}{
		{"Session Expiry 1 second, the socket dropped", support.SessionExpiry(1), support.DropSocket, time.Second},
		{"DISCONNECT 0x04 lowering the expiry to 1 second", support.Durable, func(t *testing.T, c *harness.Client) {
			support.Disconnected(t, c, &harness.Disconnect{Reason: 0x04, Props: support.SessionExpiry(1)})
		}, time.Second},
		{"Session Expiry 0, the socket dropped", nil, support.DropSocket, 0},
	}
	for _, s := range sessions {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			topic := clientID(t, "/will")
			sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
			id := clientID(t, "")
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: s.props,
				Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: support.WillDelay(support.Hour)}})
			gone := time.Now() // before the broker can see the connection end
			s.end(t, c)
			if waited := willArrives(t, sub, topic).Sub(gone); waited < s.lasts-tick {
				t.Errorf("the Will came %v after the connection ended; want it no sooner than the session's end, %v", waited, s.lasts)
			}
			if _, held := support.StoredWill(t, db, id); held {
				t.Error("once the Will was delivered, the database still holds it; want it removed with its publication")
			}
			if n := sessionRows(t, db, id); n != 0 {
				t.Errorf("once the Will was delivered, the database holds %d sessions rows for its client; want none, the session ended in the same transaction", n)
			}
		})
	}
}

// design.md, Flows, CONNECT, 6: "Clean start ends an existing session and
// publishes its delayed Will"; DISCONNECT, A lost connection: the Will "If
// the session ends first, the Will goes early". MQTT 5 3.1.2.4: with Clean
// Start 1, "the Client and Server MUST discard any existing Session and start
// a new Session" [MQTT-3.1.2-4]; 3.1.3.2.2: the Will waits "until the Will
// Delay Interval has passed or the Session ends, whichever happens first";
// [MQTT-3.1.2-8]; [MQTT-3.2.2-2]: with Clean Start 1, Session Present 0.
//
// An MQTT 5 client of a durable session connects with a Will of Will Delay
// Interval an hour on a topic a subscriber holds, and drops its socket.
// Once the database gives the Will its due time, the session away, the
// client connects again with Clean Start 1 and no Will: Session Present 0,
// the subscriber receives the Will, and the database then holds no Will
// for the client.
func TestCleanStartPublishesAnAwaySessionsDelayedWill(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	topic := clientID(t, "/will")
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
	id := clientID(t, "")
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable,
		Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: support.WillDelay(support.Hour)}})
	support.DropSocket(t, c)
	support.AwaitWillDue(t, db, id)
	if _, ack := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true}); ack.SessionPresent {
		t.Error("the CONNECT with Clean Start 1 got Session Present 1; want 0, the away session ended")
	}
	live(t, sub, topic, support.WillPayload)
	awaitWillGone(t, db, id, "the Clean Start")
}

// design.md, R6: "Everything is bounded"; Execution, Capacities:
// "Due-work and metadata pages hold 256 rows"; Resource bounds: "A Will [...]
// stay[s] in the store and [is] read when needed"; DISCONNECT, A lost
// connection: "For all away sessions together, the core keeps only the next
// session expiry and the next Will due time still to come. These are read
// from the store at start and at the end of each pass"; Restart: "a Will
// without a surviving session is due immediately"; storage.md, What is
// stored: a non-durable session stays in memory, "A Will is also stored,
// including a non-durable session's Will". MQTT 5 3.1.2.5: "In the case of a
// Server shutdown or failure, the Server MAY defer publication of Will
// Messages until a subsequent restart"; [MQTT-3.1.2-8].
//
// A subscriber's durable session holds the test's topics. 257 MQTT 5
// clients, one more than a page, of Session Expiry 0, connect with Wills
// on topics of their own and stay connected while the broker is killed:
// once it starts again on the same files, every one of those Wills is due
// at once, no session having outlived it. The subscriber resumes and
// receives each Will once, and the database then holds none.
func TestMoreWillsDueAtOnceThanAPageAllGo(t *testing.T) {
	const page = 256
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	sub := &harness.Connect{ClientID: "sub", CleanStart: true, Props: support.Durable}
	support.Subscribed(t, b.Addr, harness.V5, sub, support.At(0, "many/#"))
	// Every CONNECT is sent before any CONNACK is read: the broker stores
	// them together, the test waiting for none in turn.
	clients := make([]*harness.Client, page+1)
	for i := range clients {
		c, err := harness.Dial(b.Addr, harness.V5)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() }) // the test is over
		n := strconv.Itoa(i)
		if err := c.Send(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "many-" + n, CleanStart: true,
			Will: &harness.Will{Topic: "many/" + n, Payload: support.WillPayload}}); err != nil {
			t.Fatal(err)
		}
		clients[i] = c
	}
	for i, c := range clients {
		if ack := harness.Expect[*harness.Connack](t, c); ack.Reason != 0 {
			t.Fatalf("client %d: CONNACK 0x%02X, want 0x00", i, ack.Reason)
		}
	}
	support.Crash(t, b)

	again := support.Launch(t, &harness.Options{Dir: dir})
	c := resumed(t, again.Addr, harness.V5, sub)
	got := make(map[string]bool, len(clients))
	for range clients {
		p := harness.Expect[*harness.Publish](t, c)
		if got[p.Topic] || !strings.HasPrefix(p.Topic, "many/") || !bytes.Equal(p.Payload, support.WillPayload) {
			t.Fatalf("after %d Wills, PUBLISH %q of %q; want the Will of another client, each once", len(got), p.Topic, p.Payload)
		}
		got[p.Topic] = true
	}
	var held int
	if err := support.OpenDB(t, again.DB).QueryRowContext(context.Background(), `SELECT count(*) FROM wills`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Errorf("once every Will was delivered, the database holds %d Wills; want none", held)
	}
}
