package conformance

import (
	"fmt"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of outgoing QoS 2: a delivery to a subscriber at QoS 2 runs
// PUBLISH, PUBREC, PUBREL and PUBCOMP, a durable session storing each phase
// before the packet it allows goes, and a resumed session is resent its
// PUBLISHes and PUBRELs (design.md, Flows, Delivery; CONNECT, 9;
// storage.md, session_inflight). Publishers here complete their own QoS 2
// exchanges first (published), which qos2_test.go tests.

// published has c publish p, a QoS 2 publication, and complete its
// exchange, failing the test unless its PUBREC and its PUBCOMP are 0x00:
// design.md, Flows, PUBLISH, the table, QoS 2 "PUBREC, then PUBCOMP once in
// the channel".
func published(t *testing.T, c *harness.Client, p *harness.Publish) {
	t.Helper()
	support.Sent(t, c, p)
	pubrecOf(t, c, p, 0x00)
	pubrel(t, c, p.ID, 0x00)
}

// twice is a QoS 2 publication on topic of payload, as numbered's are at
// QoS 1.
func twice(topic, payload string) *harness.Publish {
	return &harness.Publish{QoS: 2, Topic: topic, Payload: []byte(payload)}
}

// pubrecFor sends c's PUBREC carrying reason for the broker's delivery
// under packet identifier id (MQTT 5 3.5; MQTT 3.1.1 3.5, which has no
// reason, so reason is 0 there).
func pubrecFor(t *testing.T, c *harness.Client, id uint16, reason byte) {
	t.Helper()
	if err := c.Send(&harness.Ack{Kind: harness.PUBREC, ID: id, Reason: reason}); err != nil {
		t.Fatalf("PUBREC %d: %v", id, err)
	}
}

// pubcomp sends c's PUBCOMP, 0x00, to the PUBREL of the broker's delivery
// under packet identifier id (MQTT 5 [MQTT-4.3.3-11]).
func pubcomp(t *testing.T, c *harness.Client, id uint16) {
	t.Helper()
	if err := c.Send(&harness.Ack{Kind: harness.PUBCOMP, ID: id}); err != nil {
		t.Fatalf("PUBCOMP %d: %v", id, err)
	}
}

// expectPubrel reads c's next packet and fails the test unless it is a
// PUBREL for packet identifier id carrying reason, which MQTT 3.1.1 reads as
// 0x00 (MQTT 5 [MQTT-4.3.3-4]: the PUBREL "MUST contain the same Packet
// Identifier as the original PUBLISH packet"). when says when.
func expectPubrel(t *testing.T, c *harness.Client, id uint16, reason byte, when string) {
	t.Helper()
	p, err := c.Recv()
	if err != nil {
		t.Fatalf("%s: %v; want PUBREL %d 0x%02X", when, err, id, reason)
	}
	if a, ok := p.(*harness.Ack); !ok || a.Kind != harness.PUBREL || a.ID != id || a.Reason != reason {
		t.Fatalf("%s: %s %+v; want PUBREL %d 0x%02X", when, harness.Name(p.Type()), p, id, reason)
	}
}

// released sends c's PUBREC 0x00 for the broker's delivery under packet
// identifier id and fails the test unless c's next packet is its PUBREL,
// 0x00: MQTT 5 4.3.3, the sender "MUST send a PUBREL packet when it receives
// a PUBREC packet from the receiver with a Reason Code value less than 0x80"
// [MQTT-4.3.3-4]; MQTT 3.1.1 4.3.3, Figure 4.3.
func released(t *testing.T, c *harness.Client, id uint16) {
	t.Helper()
	pubrecFor(t, c, id, 0x00)
	expectPubrel(t, c, id, 0x00, fmt.Sprintf("after PUBREC %d", id))
}

// design.md, Delivery, Positions: "an acknowledgement, PUBACK, PUBCOMP or a
// refusing PUBREC, settles its own delivery (MQTT-4.4.0-2)"; "The position
// advances only past the contiguous settled or skipped prefix of examined
// records"; "A QoS 2 delivery stores its PUBREC before its PUBREL goes to
// the writer"; One mechanism for every channel: "The window is the lower of
// the client's Receive Maximum and limits.max_inflight, or the cap alone for
// MQTT 3.1.1. A message is in flight from its hand-off to the writer";
// CONNECT, 9: "The broker reads in-flight messages and PUBRELs from the
// store and resends them". MQTT 5 4.3.3: the sender "MUST treat the PUBREL
// packet as "unacknowledged" until it has received the corresponding PUBCOMP
// packet from the receiver [MQTT-4.3.3-5]" and "MUST NOT re-send the PUBLISH
// once it has sent the corresponding PUBREL packet [MQTT-4.3.3-6]"; 4.9: the
// send quota is incremented "Each time a PUBACK or PUBCOMP packet is
// received". MQTT 3.1.1 4.3.3, Figure 4.3.
//
// On a broker with max_inflight 1, in MQTT 5 and in MQTT 3.1.1, a client
// holding a durable session subscribes at QoS 2, and a publisher stores two
// QoS 2 messages. The client receives the first at QoS 2, DUP 0, and sends
// PUBREC: its next packet is the PUBREL, 0x00, and nothing more comes for a
// while, the second message waiting behind the first, unsettled. The client
// sends DISCONNECT and resumes: the first's PUBREL comes again, and once the
// client sends PUBCOMP, its next PUBLISH is the second message, DUP 0, the
// first never sent as a new copy. It completes the second, sends DISCONNECT
// and resumes: a message published then is its next packet, nothing settled
// sent again.
func TestQoS2DeliveryIsSettledByItsPubcomp(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxInflight: 1})
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			prefix := "out2/" + clientID(t, "")
			connect := kept(version, clientID(t, "-sub"))
			sub := support.Subscribed(t, b.Addr, version, connect, support.At(2, prefix+"/#"))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			ps := []*harness.Publish{twice(prefix+"/x", "1"), twice(prefix+"/x", "2")}
			for _, p := range ps {
				published(t, pub, p)
			}
			first := support.DeliveredAt(t, sub, 2, ps[0].Topic, ps[0].Payload)
			released(t, sub, first.ID)
			nothingWithin(t, sub, quiet, "with its one delivery's PUBREC sent and its PUBCOMP withheld")

			support.Away(t, sub)
			sub = resumed(t, b.Addr, version, connect)
			if a, err := sub.AwaitAck(harness.PUBREL, first.ID); err != nil || a.Reason != 0 {
				t.Fatalf("after resuming with the first delivery past its PUBREC: %+v, %v; want its PUBREL again, 0x00", a, err)
			}
			pubcomp(t, sub, first.ID)
			second := support.DeliveredAt(t, sub, 2, ps[1].Topic, ps[1].Payload)
			released(t, sub, second.ID)
			pubcomp(t, sub, second.ID)

			support.Away(t, sub)
			sub = resumed(t, b.Addr, version, connect)
			thenLiveAt(t, pub, sub, 1, prefix+"/after")
		})
	}
}

// design.md, Flows, CONNECT, 9: "The broker reads in-flight messages and
// PUBRELs from the store and resends them with their packet identifiers and
// in their order. Resent messages have DUP set (MQTT-4.4.0-1)"; Delivery,
// Positions: "Before handing a QoS 1 or QoS 2 delivery to a durable
// session's writer, the core stores its packet identifier, its order (seq),
// its record and its phase"; Restart: "the broker restores durable sessions
// with their subscriptions, positions, in-flight windows". MQTT 5 4.3.3: the
// sender "MUST treat the PUBLISH packet as "unacknowledged" until it has
// received the corresponding PUBREC packet from the receiver
// [MQTT-4.3.3-3]"; [MQTT-4.4.0-1]: the Server "MUST resend any
// unacknowledged PUBLISH packets (where QoS > 0) [...] using their original
// Packet Identifiers"; [MQTT-3.3.1-1]: DUP 1 on a resend.
//
// A client holding a durable session subscribes at QoS 2 and receives a
// publisher's QoS 2 message, sending no PUBREC. It sends DISCONNECT, or the
// broker is killed and started again on the same files. The client resumes:
// the message comes again at QoS 2, DUP 1, under its packet identifier, and
// its PUBREC then gets the PUBREL; after its PUBCOMP, nothing more comes
// before the PINGRESP.
func TestQoS2PublishIsResentUntilItsPubrec(t *testing.T) {
	for _, tc := range []struct {
		name  string
		crash bool
	}{{"after a reconnect", false}, {"after a restart", true}} {
		t.Run(tc.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir()}
			b := support.Launch(t, o)
			connect := kept(harness.V5, "unreceived")
			sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "unreceived/#"))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "unreceived-pub", CleanStart: true})
			p := twice("unreceived/x", "resent")
			published(t, pub, p)
			first := support.DeliveredAt(t, sub, 2, p.Topic, p.Payload)
			if tc.crash {
				support.Crash(t, b)
				b = support.Launch(t, o)
			} else {
				support.Away(t, sub)
			}

			sub = resumed(t, b.Addr, harness.V5, connect)
			released(t, sub, redelivered(t, sub, first).ID)
			pubcomp(t, sub, first.ID)
			taken(t, sub, "the PUBCOMP of the resent delivery")
		})
	}
}

// design.md, Delivery, Positions: "A QoS 2 delivery stores its PUBREC before
// its PUBREL goes to the writer"; CONNECT, 9: "The broker reads in-flight
// messages and PUBRELs from the store and resends them with their packet
// identifiers"; Store, full mode: "A restart recovers committed state".
// storage.md, session_inflight: phase "2 PUBREC received and PUBREL
// owed". MQTT 5 4.3.3: the sender "MUST NOT re-send the PUBLISH once it has
// sent the corresponding PUBREL packet [MQTT-4.3.3-6]"; [MQTT-4.4.0-1]: the
// Server "MUST resend any unacknowledged [...] PUBREL packets using their
// original Packet Identifiers".
//
// A client holding a durable session subscribes at QoS 2, receives a
// publisher's QoS 2 message and sends PUBREC: once it has the PUBREL, the
// database holds the delivery's row, under its packet identifier, at QoS 2,
// phase 2. The client sends DISCONNECT, or the broker is killed and started
// again on the same files. The client resumes: its first packet is the
// PUBREL again, 0x00, and after its PUBCOMP nothing more comes before the
// PINGRESP, the PUBLISH never sent again.
func TestQoS2PubrelIsResentAfterItsPubrec(t *testing.T) {
	for _, tc := range []struct {
		name  string
		crash bool
	}{{"after a reconnect", false}, {"after a restart", true}} {
		t.Run(tc.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir()}
			b := support.Launch(t, o)
			const client = "pubrel-owed"
			connect := kept(harness.V5, client)
			sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "owed/#"))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "owed-pub", CleanStart: true})
			p := twice("owed/x", "released")
			published(t, pub, p)
			first := support.DeliveredAt(t, sub, 2, p.Topic, p.Payload)
			released(t, sub, first.ID)
			if rows := storedInflight(t, support.OpenDB(t, b.DB), client); len(rows) != 1 || rows[0].packetID != int64(first.ID) || rows[0].qos != 2 || rows[0].phase != 2 {
				t.Errorf("once the client has the PUBREL, session_inflight holds %+v; want one row, packet identifier %d, QoS 2, phase 2", rows, first.ID)
			}
			if tc.crash {
				support.Crash(t, b)
				b = support.Launch(t, o)
			} else {
				support.Away(t, sub)
			}

			sub = resumed(t, b.Addr, harness.V5, connect)
			expectPubrel(t, sub, first.ID, 0x00, "first after the CONNACK resuming an exchange past its PUBREC")
			pubcomp(t, sub, first.ID)
			taken(t, sub, "the PUBCOMP of the resumed exchange")
		})
	}
}

// design.md, Flows, CONNECT, 9: "The broker reads in-flight messages and
// PUBRELs from the store and resends them with their packet identifiers and
// in their order"; "The new connection's Receive Maximum governs resumed
// PUBLISHes as well as new ones. Outstanding PUBLISHes are offered in stored
// sending order, only while the new connection has quota"; "PUBREL and other
// control packets are not blocked by that quota". storage.md,
// session_inflight: "Messages are resent in seq order". MQTT 5 4.9: "The
// Client and Server MUST continue to process and respond to all other MQTT
// Control Packets even if the quota is zero [MQTT-4.9.0-3]"; [MQTT-4.6.0-1],
// which the Server applies in its turn: "re-send them in the order in which
// the original PUBLISH packets were sent".
//
// A client holding a durable session, with Receive Maximum 3, subscribes at
// QoS 2 and receives three QoS 2 messages; it sends PUBREC for the second
// alone and reads its PUBREL, then sends DISCONNECT. It resumes with Receive
// Maximum 1: the first comes again, DUP 1, then the second's PUBREL, though
// the first takes the new connection's quota, and nothing more for a while.
// The client completes the second and then the first; the third then comes
// again, DUP 1.
func TestResumeResendsPubrelsInStoredOrder(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	connect := &harness.Connect{ClientID: "ordered", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(3))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "ordered/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ordered-pub", CleanStart: true})
	ps := []*harness.Publish{twice("ordered/x", "1"), twice("ordered/x", "2"), twice("ordered/x", "3")}
	for _, p := range ps {
		published(t, pub, p)
	}
	firsts := make([]*harness.Publish, len(ps))
	for i, p := range ps {
		firsts[i] = support.DeliveredAt(t, sub, 2, p.Topic, p.Payload)
	}
	released(t, sub, firsts[1].ID)
	support.Away(t, sub)

	connect.Props = slices.Concat(support.Durable, receiveMaximum5(1))
	sub = resumed(t, b.Addr, harness.V5, connect)
	redelivered(t, sub, firsts[0])
	expectPubrel(t, sub, firsts[1].ID, 0x00, "after the first's resend, which takes the new connection's quota of 1")
	nothingWithin(t, sub, quiet, "with the new connection's Receive Maximum of 1 taken by the first's resend")
	pubcomp(t, sub, firsts[1].ID)
	released(t, sub, firsts[0].ID)
	pubcomp(t, sub, firsts[0].ID)
	redelivered(t, sub, firsts[2])
}

// design.md, Delivery, Positions: "an acknowledgement, PUBACK, PUBCOMP or a
// refusing PUBREC, settles its own delivery (MQTT-4.4.0-2)". MQTT 5 4.4: "If
// PUBACK or PUBREC is received containing a Reason Code of 0x80 or greater
// the corresponding PUBLISH packet is treated as acknowledged, and MUST NOT
// be retransmitted [MQTT-4.4.0-2]"; 4.3.3: the sender "MUST send a PUBREL
// packet when it receives a PUBREC packet from the receiver with a Reason
// Code value less than 0x80 [MQTT-4.3.3-4]"; 4.9: the send quota is
// incremented "Each time a PUBREC packet is received with a Return Code of
// 0x80 or greater"; 3.5.2.1, Table 3-5: 0x80, "Unspecified error"; 3.5.2.2:
// a PUBREC may carry a Reason String and User Properties.
//
// A client holding a durable session, with Receive Maximum 1, subscribes at
// QoS 2, and a publisher stores two QoS 2 messages. The client receives the
// first and refuses it, PUBREC 0x80 with a Reason String and two User
// Properties: its next packet is the second message,
// no PUBREL before it. It completes the second, sends DISCONNECT and
// resumes: a message published then is its next packet, the refused one not
// sent again.
func TestRefusingPubrecSettlesItsDelivery(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	connect := &harness.Connect{ClientID: "refuser", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(1))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "refused/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "refuser-pub", CleanStart: true})
	ps := []*harness.Publish{twice("refused/x", "refused"), twice("refused/x", "taken")}
	for _, p := range ps {
		published(t, pub, p)
	}
	first := support.DeliveredAt(t, sub, 2, ps[0].Topic, ps[0].Payload)
	refusal := &harness.Ack{Kind: harness.PUBREC, ID: first.ID, Reason: 0x80, Props: harness.Properties{
		{ID: harness.ReasonString, Str: "not now"}, harness.User("n", "v"), harness.User("n", "w"),
	}}
	if err := sub.Send(refusal); err != nil {
		t.Fatalf("PUBREC %d 0x80: %v", first.ID, err)
	}
	second := support.DeliveredAt(t, sub, 2, ps[1].Topic, ps[1].Payload)
	released(t, sub, second.ID)
	pubcomp(t, sub, second.ID)

	support.Away(t, sub)
	sub = resumed(t, b.Addr, harness.V5, connect)
	thenLiveAt(t, pub, sub, 1, "refused/after")
}

// design.md, Delivery, Positions: "Once a successful PUBREC is accepted for
// an outgoing QoS 2 exchange, every further PUBREC for it is answered with a
// successful PUBREL under the same identifier. While its phase save is at
// the store, these replies wait for its commit. They change no delivery
// state and need no store write of their own"; "an acknowledgement, PUBACK,
// PUBCOMP or a refusing PUBREC, settles its own delivery"; Flows, Replies:
// PUBREL is "sent only once the store confirms, where they confirm a
// change". MQTT 5 4.3.3: the sender "MUST send a PUBREL packet when it
// receives a PUBREC packet from the receiver with a Reason Code value less
// than 0x80 [MQTT-4.3.3-4]" and "MUST treat the PUBREL packet as
// "unacknowledged" until it has received the corresponding PUBCOMP packet
// [MQTT-4.3.3-5]"; MQTT 3.1.1 4.3.3, Figure 4.3.
//
// On a broker with max_inflight 1, in MQTT 5 and in MQTT 3.1.1, a client
// holding a durable session subscribes at QoS 2, and a publisher stores two
// QoS 2 messages. The client receives the first; the test holds the
// database's write lock while the client sends PUBREC, and once the store
// writer has met the lock, a second PUBREC for it, then a PINGREQ: the
// PINGRESP comes first, no PUBREL before it. Once the lock is released, the
// client reads two PUBRELs, each 0x00 under the delivery's identifier. Its
// one PUBCOMP then brings the second message; once the client completes
// that too, sends DISCONNECT and resumes, a message published then is its
// next packet, nothing of the first exchange sent again.
func TestRepeatedPubrecGetsPubrelOnceStored(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{MaxInflight: 1})
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			prefix := "again/" + clientID(t, "")
			connect := kept(version, clientID(t, "-sub"))
			sub := support.Subscribed(t, b.Addr, version, connect, support.At(2, prefix+"/#"))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			ps := []*harness.Publish{twice(prefix+"/x", "1"), twice(prefix+"/x", "2")}
			for _, p := range ps {
				published(t, pub, p)
			}
			first := support.DeliveredAt(t, sub, 2, ps[0].Topic, ps[0].Payload)

			lock := holdWriteLock(t, b.DB)
			before := len(traced(t, b.trace))
			pubrecFor(t, sub, first.ID, 0x00)
			awaitLockMet(t, b.trace, before, "the first PUBREC")
			pubrecFor(t, sub, first.ID, 0x00)
			if err := sub.Ping(); err != nil {
				t.Fatalf("a PINGREQ after two PUBRECs, the first's phase save held at the store: %v; want the PINGRESP first, both PUBRELs waiting for its commit", err)
			}
			lock.release(t)
			expectPubrel(t, sub, first.ID, 0x00, "once the first PUBREC's phase is stored")
			expectPubrel(t, sub, first.ID, 0x00, "for the repeated PUBREC")

			pubcomp(t, sub, first.ID)
			second := support.DeliveredAt(t, sub, 2, ps[1].Topic, ps[1].Payload)
			released(t, sub, second.ID)
			pubcomp(t, sub, second.ID)
			support.Away(t, sub)
			sub = resumed(t, b.Addr, version, connect)
			thenLiveAt(t, pub, sub, 1, prefix+"/after")
		})
	}
}

// design.md, Delivery, Positions: "A PUBREC naming no outgoing QoS 2
// exchange, a QoS 1 delivery's identifier included, gets PUBREL 0x92, a
// plain PUBREL on MQTT 3.1.1, and changes nothing"; Words: Packet
// identifier, "An acknowledgement settles only the exchange of its own
// direction and phase". MQTT 5 3.6.2.1, Table 3-6: 0x92, "Packet
// Identifier not found"; MQTT 3.1.1 3.6: a PUBREL carries the Packet
// Identifier alone.
//
// On a broker with max_inflight 1, in MQTT 5 and in MQTT 3.1.1, a client
// subscribes at QoS 1 and a publisher stores two QoS 1 messages. The client
// receives the first and sends a PUBREC for it, a delivery awaiting a
// PUBACK, then one for identifier 7, which no delivery holds: each gets
// PUBREL 0x92 (MQTT 3.1.1: a PUBREL), and nothing more comes for a while, the
// first still unsettled. Its PUBACK then brings the second.
//
// TestRepeatedPubrecGetsPubrelOnceStored covers a PUBREC repeating one for
// an outgoing QoS 2 exchange, which is answered 0x00.
func TestUnknownPubrecGetsPubrel92(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxInflight: 1})
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			unknown := byte(0x92)
			if version == harness.V311 {
				unknown = 0x00
			}
			topic := "unknown/" + clientID(t, "")
			sub := support.Subscribed(t, b.Addr, version, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, topic))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			ps := numbered(topic, 2)
			support.PublishAll(t, pub, ps)
			first := support.DeliveredAt(t, sub, 1, topic, ps[0].Payload)
			for _, id := range []uint16{first.ID, 7} {
				pubrecFor(t, sub, id, 0x00)
				expectPubrel(t, sub, id, unknown, fmt.Sprintf("after PUBREC %d, which names no outgoing QoS 2 exchange", id))
			}
			nothingWithin(t, sub, quiet, "after two PUBRECs naming no outgoing QoS 2 exchange, the first delivery's PUBACK withheld")
			acknowledge(t, sub, first.ID, 0)
			support.DeliveredAt(t, sub, 1, topic, ps[1].Payload)
		})
	}
}

// design.md, Flows, Replies: "A reply becomes ready when the core
// processes the event establishing its result. Ready replies enter the
// connection's reply queue in that event order"; "Acknowledgements of
// stored changes preserve their packet order"; "The reply queue holds
// acknowledgements, PINGRESP, SUBACK, UNSUBACK and the server's
// DISCONNECT"; Delivery, Positions: "every further PUBREC for it is
// answered with a successful PUBREL under the same identifier"; "A PUBREC
// naming no outgoing QoS 2 exchange [...] gets PUBREL 0x92 [...] and
// changes nothing".
//
// On a stallable broker, a client subscribed at QoS 2 receives a QoS 2
// message and sends its PUBREC, and the broker's PUBREL comes; the client
// sends no PUBCOMP. With the store stalled, the client publishes at QoS 1,
// whose PUBACK waits for its slow commit, then repeats its PUBREC, whose
// PUBREL keeps its place behind that PUBACK, then sends a PUBREC for
// identifier 7, which no delivery holds, then PINGREQ. PUBREL 7 0x92
// arrives before the PINGRESP, its result established first, and the
// PUBACK arrives too.
func TestPingrespKeepsItsPlaceBehindAReadyPubrel(t *testing.T) {
	s := launchStallable(t, &harness.Options{})
	topic := clientID(t, "/x")
	c := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true}, support.At(2, topic))
	pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	m := twice(topic, "m")
	published(t, pub, m)
	d := support.DeliveredAt(t, c, 2, topic, m.Payload)
	released(t, c, d.ID)

	p := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("p")}
	s.stall(t, true)
	support.Sent(t, c, p)
	pubrecFor(t, c, d.ID, 0x00)
	pubrecFor(t, c, 7, 0x00)
	if err := c.Send(&harness.Pingreq{}); err != nil {
		t.Fatalf("PINGREQ: %v", err)
	}
	var before []string
	unknown := false
	if _, err := c.Await(func(q harness.Packet) bool {
		if _, ok := q.(*harness.Pingresp); ok {
			return true
		}
		a, ok := q.(*harness.Ack)
		unknown = unknown || (ok && a.Kind == harness.PUBREL && a.ID == 7 && a.Reason == 0x92)
		before = append(before, fmt.Sprintf("%s %+v", harness.Name(q.Type()), q))
		return false
	}); err != nil {
		t.Fatalf("waiting for PINGRESP: %v", err)
	}
	if !unknown {
		t.Fatalf("PINGRESP came after %q; want PUBREL 7 0x92 before it, ready first", before)
	}
	support.Puback(t, c, p)
	s.stall(t, false)
}

// design.md, Words: Missed, "An exchange owed only its PUBREL needs no
// record. Its PUBREL is resent and nothing is counted (MQTT-4.4.0-1).";
// "Deliveries already settled or handed to a writer are excluded"; Words:
// Retention, a record past retention "is gone, whoever still needed it [...]
// a message sent and not yet acknowledged". metrics.md, saguin_missed_total:
// "records consumers were owed and never received". MQTT 5 [MQTT-4.4.0-1]:
// the Server "MUST resend any unacknowledged [...] PUBREL packets using their
// original Packet Identifiers".
//
// An append channel, events, keeps 150 bytes. A client holding a durable
// session, with Receive Maximum 1, subscribes at QoS 2 to events/#,
// receives a QoS 2 record r1, sends PUBREC, reads the PUBREL and leaves
// without a PUBCOMP. A publisher stores r2: retention deletes r1, and
// nothing is counted missed. The client resumes: r1's PUBREL comes again,
// and after its PUBCOMP the next PUBLISH is r2, a first delivery; once the
// client completes r2, nothing has been counted missed.
func TestPubrelIsResentAfterRetentionDeletedItsRecord(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: "outlived", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(1))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "events/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "events-pub", CleanStart: true})
	r1, r2 := support.Sized("events/r1", testkit.RecordBytes), support.Sized("events/r2", testkit.RecordBytes)
	r1.QoS, r2.QoS = 2, 2
	published(t, pub, r1)
	first := support.DeliveredAt(t, sub, 2, r1.Topic, r1.Payload)
	released(t, sub, first.ID)
	support.Away(t, sub)

	published(t, pub, r2)
	awaitHeld(t, db, "events", []string{r2.Topic}, "once r2 is stored")
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 0, "once retention has passed r1, its PUBREL owed")

	sub = resumed(t, b.Addr, harness.V5, connect)
	if a, err := sub.AwaitAck(harness.PUBREL, first.ID); err != nil || a.Reason != 0 {
		t.Fatalf("after resuming with r1 past its PUBREC and its record deleted: %+v, %v; want its PUBREL again, 0x00", a, err)
	}
	pubcomp(t, sub, first.ID)
	second := support.DeliveredAt(t, sub, 2, r2.Topic, r2.Payload)
	released(t, sub, second.ID)
	pubcomp(t, sub, second.ID)
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 0, "once the session has resumed and completed r1's exchange")
}

// design.md, Words: Packet identifier, "each connection has two independent
// spaces of packet identifiers, the client's and the broker's. An
// acknowledgement settles only the exchange of its own direction and
// phase"; Delivery, Sending: "Packet identifiers use the lowest free
// identifier"; Flows, PUBLISH: "A PUBREL for an unknown identifier gets
// PUBCOMP 0x92". MQTT 5 2.2.1: "Each time a Server sends a new PUBLISH (with
// QoS > 0) MQTT Control Packet it MUST assign it a non zero Packet
// Identifier that is currently unused [MQTT-2.2.1-4]"; "The Client and
// Server assign Packet Identifiers independently of each other".
//
// A client subscribes at QoS 2 to .../in and publishes at QoS 2 on .../out
// under packet identifier 1: PUBREC 0x00, its PUBREL withheld. A publisher's
// QoS 2 message on .../in reaches the client under the broker's identifier
// 1, the client's held 1 not taking it. The client's PUBREC 1 gets the
// delivery's PUBREL, 0x00; its PUBCOMP 1 completes the delivery; its own
// PUBREL 1 then gets PUBCOMP 0x00, its exchange still held. The next message
// on .../in comes under identifier 1 again, the delivery settled.
func TestIdentifierSpacesStayApart(t *testing.T) {
	prefix := "spaces/" + clientID(t, "")
	c := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true}, support.At(2, prefix+"/in"))
	own := &harness.Publish{QoS: 2, ID: 1, Topic: prefix + "/out", Payload: []byte("own")}
	support.Sent(t, c, own)
	pubrecOf(t, c, own, 0x00)
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{twice(prefix+"/in", "first"), twice(prefix+"/in", "second")}

	published(t, pub, ps[0])
	delivery := support.DeliveredAt(t, c, 2, ps[0].Topic, ps[0].Payload)
	if delivery.ID != own.ID {
		t.Fatalf("the delivery came under packet identifier %d; want %d, the lowest free of the broker's own identifiers", delivery.ID, own.ID)
	}
	released(t, c, delivery.ID)
	pubcomp(t, c, delivery.ID)
	pubrel(t, c, own.ID, 0x00)

	published(t, pub, ps[1])
	if next := support.DeliveredAt(t, c, 2, ps[1].Topic, ps[1].Payload); next.ID != delivery.ID {
		t.Errorf("the next delivery came under packet identifier %d; want %d, freed by the PUBCOMP", next.ID, delivery.ID)
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken. The 0x8B follows the PUBACK, PUBREC,
// PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken before the stop
// whose changes the stop stores (R4)"; Flows, Replies: "CONNACK (when it
// changes a session), PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK
// [...] are sent only once the store confirms, where they confirm a
// change"; Delivery, Positions: "A QoS 2 delivery stores its PUBREC before
// its PUBREL goes to the writer"; Execution, Decisions: "Each connection's
// packet order is preserved".
//
// A client holding a durable session subscribes at QoS 2 and receives a
// publisher's QoS 2 message. The test then holds the database's write lock
// while the client sends PUBREC, then a PINGREQ; the broker runs under
// strace. Once its store writer has met the lock, the PINGRESP, read first,
// says the core has taken the PUBREC. SIGTERM goes; once the listener
// refuses a connection, the stop has begun, and the lock is released. The
// client reads the PUBREL, 0x00, then DISCONNECT 0x8B and the end of stream,
// and the broker exits 0.
func TestStopSendsPubrelFirst(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{})
	sub := support.Subscribed(t, b.Addr, harness.V5, kept(harness.V5, "stopping-sub"), support.At(2, "stop/out/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "stopping-pub", CleanStart: true})
	p := twice("stop/out/x", "stopped")
	published(t, pub, p)
	delivery := support.DeliveredAt(t, sub, 2, p.Topic, p.Payload)

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	pubrecFor(t, sub, delivery.ID, 0x00)
	awaitLockMet(t, b.trace, before, "the PUBREC")
	if err := sub.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the PUBREC: %v; want the PINGRESP first, the PUBREL waiting for the store", err)
	}
	b.terminate(t)
	listenerClosed(t, b.Addr)
	lock.release(t)

	expectPubrel(t, sub, delivery.ID, 0x00, "after SIGTERM, before DISCONNECT 0x8B")
	shutDown(t, sub)
	_ = sub.Close() // the broker has ended its end
	if state := support.Exited(t, b.Broker); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}
