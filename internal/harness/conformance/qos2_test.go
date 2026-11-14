package conformance

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of incoming QoS 2: a client's QoS 2 PUBLISH held, stored and
// charged to its channel before PUBREC, entering the channel on PUBREL,
// PUBCOMP once it is in (design.md, Flows, PUBLISH; storage.md,
// held_publishes). Subscribers here take QoS 1 grants: the lower of the
// publication's QoS and the grant goes out (Flows, SUBSCRIBE, One copy).

// bothVersions are the two protocol versions a test runs in.
var bothVersions = []byte{harness.V5, harness.V311}

// kept is version's CONNECT for client's session that outlives its
// connection (design.md, Words: Session, "durable when its expiry is above
// zero"): MQTT 5's with a Session Expiry Interval of an hour, starting
// clean; MQTT 3.1.1's with Clean Session 0 (config.md, max_session_expiry
// "is MQTT 3.1.1 Clean Session 0's"). resumed reconnects it with Clean
// Start 0.
func kept(version byte, client string) *harness.Connect {
	if version == harness.V311 {
		return &harness.Connect{ClientID: client}
	}
	return &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
}

// pubrel sends c's PUBREL for packet identifier id and fails the test unless
// its PUBCOMP carries reason, which MQTT 3.1.1 reads as 0x00: design.md,
// Flows, PUBLISH, QoS 2 "PUBREC, then PUBCOMP once in the channel"; "A PUBREL
// for an unknown identifier gets PUBCOMP 0x92". MQTT 5 4.3.3: the receiver
// "MUST respond to a PUBREL packet by sending a PUBCOMP packet containing
// the same Packet Identifier as the PUBREL [MQTT-4.3.3-11]".
func pubrel(t *testing.T, c *harness.Client, id uint16, reason byte) {
	t.Helper()
	if err := c.Send(&harness.Ack{Kind: harness.PUBREL, ID: id}); err != nil {
		t.Fatalf("PUBREL %d: %v", id, err)
	}
	ack, err := c.AwaitAck(harness.PUBCOMP, id)
	if err != nil {
		t.Fatalf("PUBREL %d: %v", id, err)
	}
	if ack.Reason != reason {
		t.Fatalf("PUBREL %d: PUBCOMP 0x%02X, want 0x%02X", id, ack.Reason, reason)
	}
}

// heldCopies counts the rows of storage.md's held_publishes in q's database
// whose payload is payload: each test's messages have payloads of their own.
func heldCopies(t *testing.T, q support.RowQuerier, payload []byte) int {
	t.Helper()
	var n int
	if err := q.QueryRowContext(context.Background(), `SELECT count(*) FROM held_publishes WHERE payload = ?`, payload).Scan(&n); err != nil {
		t.Fatalf("reading the held_publishes rows of %q: %v", payload, err)
	}
	return n
}

// storedCopies returns the records of the database at path whose payload
// is payload.
func storedCopies(t *testing.T, path string, payload []byte) []record {
	t.Helper()
	return slices.DeleteFunc(records(t, path), func(r record) bool { return !bytes.Equal(r.payload, payload) })
}

// expectStoredOnce fails the test unless the database at path holds one
// record of payload, at QoS 2, and no held row of it: storage.md,
// held_publishes, "On PUBREL, the record takes its offset and retained
// flag, and the held row is deleted in the same transaction"; records, qos
// "the QoS it was published at". when says when.
func expectStoredOnce(t *testing.T, path string, payload []byte, when string) {
	t.Helper()
	rs := storedCopies(t, path, payload)
	if len(rs) != 1 || rs[0].qos != 2 {
		t.Errorf("%s, the database holds %d records of %q (%+v); want one, at QoS 2", when, len(rs), payload, rs)
	}
	if n := heldCopies(t, support.OpenDB(t, path), payload); n != 0 {
		t.Errorf("%s, the database holds %d held rows of %q; want none", when, n, payload)
	}
}

// design.md, Flows, PUBLISH, the table: "2 | The publication enters on
// PUBREL, from a held copy stored and charged to the channel before PUBREC.
// [...] | PUBREC, then PUBCOMP once in the channel"; Words: Offset, "The core
// assigns it when a publication enters the channel, in entry order";
// Delivery, Order: "deliveries follow a channel's offset order on
// $broadcast"; "A later QoS 0 message can overtake a publisher's earlier QoS
// 2 one"; SUBSCRIBE, One copy: "The delivery uses the lower of the
// publication's QoS and the highest contributing grant". MQTT 5 4.3.3: the
// receiver "MUST respond with a PUBREC containing the Packet Identifier from
// the incoming PUBLISH packet [MQTT-4.3.3-8]" and "MUST respond to a PUBREL
// packet by sending a PUBCOMP packet [MQTT-4.3.3-11]"; MQTT 3.1.1 4.3.3
// [MQTT-4.3.3-2].
//
// In MQTT 5 and in MQTT 3.1.1, a subscriber holds the test's q2/ topics at
// QoS 1. A publisher sends a QoS 2 PUBLISH on .../held and reads PUBREC 0x00;
// it then publishes at QoS 1 on .../after, PUBACK 0x00. The subscriber's next
// packet is .../after: the held message has not entered $broadcast, or it
// would have the lower offset and come first. The publisher's PUBREL gets
// PUBCOMP 0x00, and the subscriber's next packet is then .../held, once, at
// QoS 1: its PINGREQ's PINGRESP follows, no second copy before it.
func TestQoS2EntersOnPubrel(t *testing.T) {
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			prefix := "q2/" + clientID(t, "")
			sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, prefix+"/#"))
			pub, _ := harness.Connected(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			held := &harness.Publish{QoS: 2, Topic: prefix + "/held", Payload: []byte("held")}
			support.Sent(t, pub, held)
			pubrecOf(t, pub, held, 0x00)
			thenLiveAt(t, pub, sub, 1, prefix+"/after")
			pubrel(t, pub, held.ID, 0x00)
			liveAt(t, sub, 1, held.Topic, held.Payload)
			taken(t, sub, "the held message's delivery")
		})
	}
}

// design.md, Restart: "Restored: the broker restores durable sessions with
// their subscriptions, positions, in-flight windows, held incoming QoS 2
// exchanges and Wills"; Flows, PUBLISH, the table: QoS 2 "enters on PUBREL,
// from a held copy stored and charged to the channel before PUBREC";
// CONNECT, 9: "Held QoS 2 exchanges wait for their PUBREL"; Store, full mode:
// "Everything acknowledged survives a crash or a power cut". storage.md,
// held_publishes: "On PUBREL, the record takes its offset and retained flag,
// and the held row is deleted in the same transaction". MQTT 5 4.4: a Client
// reconnecting with Clean Start 0 "MUST resend any unacknowledged [...]
// PUBREL packets using their original Packet Identifiers [MQTT-4.4.0-1]".
//
// In MQTT 5 and in MQTT 3.1.1, on a broker of its own, a client holding a
// durable session sends a QoS 2 PUBLISH and reads PUBREC 0x00; the broker is
// killed. Started again on the same files, its database holds no record of
// the message. The client reconnects with Clean Start 0, Session Present 1,
// and sends the PUBREL: PUBCOMP 0x00. After a graceful stop the database
// holds the message's record once, at QoS 2, and no held row.
func TestHeldQoS2SurvivesARestart(t *testing.T) {
	t.Parallel()
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir()}
			b := support.Launch(t, o)
			connect := kept(version, "restarted")
			c, _ := harness.Connected(t, b.Addr, version, connect)
			p := &harness.Publish{QoS: 2, Topic: "q2/restarted", Payload: []byte("held")}
			support.Sent(t, c, p)
			pubrecOf(t, c, p, 0x00)
			support.Crash(t, b)

			again := support.Launch(t, o)
			if rs := storedCopies(t, again.DB, p.Payload); len(rs) != 0 {
				t.Errorf("after the restart, before the PUBREL, the database holds %d records of the held message; want none", len(rs))
			}
			c = resumed(t, again.Addr, version, connect)
			pubrel(t, c, p.ID, 0x00)
			stopped(t, again)
			expectStoredOnce(t, again.DB, p.Payload, "after the PUBREL sent past the restart")
		})
	}
}

// design.md, Flows, PUBLISH: "While a QoS 2 PUBLISH's identifier is held
// awaiting PUBREL, any later PUBLISH with that identifier gets PUBREC again.
// It is not stored twice, or charged twice against its channel's size,
// whatever its DUP flag (MQTT-4.3.3-10)";
// CONNECT, 9: "Held QoS 2 exchanges wait for their PUBREL". MQTT 5 4.3.3:
// "Until it has received the corresponding PUBREL packet, the receiver MUST
// acknowledge any subsequent PUBLISH packet with the same Packet Identifier
// by sending a PUBREC. It MUST NOT cause duplicate messages to be delivered
// to any onward recipients in this case [MQTT-4.3.3-10]"; 3.3.1.1: "If the
// DUP flag is set to 1, it indicates that this might be re-delivery of an
// earlier attempt to send the packet"; 2.2.1: a Packet Identifier is a Two
// Byte Integer, 65,535 the largest. MQTT 3.1.1 4.3.3 [MQTT-4.3.3-2].
//
// In MQTT 5 and in MQTT 3.1.1, on a broker of its own, a client holding a
// durable session sends a retained QoS 2 PUBLISH under packet identifier
// 65,535, PUBREC 0x00, then the same PUBLISH with DUP 1 and again with DUP
// 0: PUBREC 0x00 to each. It disconnects, reconnects with Clean Start 0,
// Session Present 1, and resends the PUBLISH with DUP 1: PUBREC 0x00. Its
// PUBREL gets PUBCOMP 0x00. After a graceful stop the database holds the
// message's record once.
func TestRepeatedQoS2PublishIsStoredOnce(t *testing.T) {
	t.Parallel()
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			b := support.Launch(t, &harness.Options{})
			connect := kept(version, "repeating")
			c, _ := harness.Connected(t, b.Addr, version, connect)
			p := &harness.Publish{QoS: 2, Retain: true, Topic: "q2/repeated", ID: 0xFFFF, Payload: []byte("once")}
			repeat := *p
			repeat.Dup = true
			for _, send := range []*harness.Publish{p, &repeat, p} {
				support.Sent(t, c, send)
				pubrecOf(t, c, send, 0x00)
			}
			support.Away(t, c)
			c = resumed(t, b.Addr, version, connect)
			support.Sent(t, c, &repeat)
			pubrecOf(t, c, &repeat, 0x00)
			pubrel(t, c, p.ID, 0x00)
			stopped(t, b)
			expectStoredOnce(t, b.DB, p.Payload, "after a PUBLISH repeated twice on its connection and once after a reconnect")
		})
	}
}

// design.md, Flows, PUBLISH: "A PUBREL for an unknown identifier gets
// PUBCOMP 0x92". MQTT 5 3.7.2.1, Table 3-7: 0x92, "Packet Identifier not
// found"; 4.3.3: the receiver "MUST respond to a PUBREL packet by sending a
// PUBCOMP packet containing the same Packet Identifier as the PUBREL
// [MQTT-4.3.3-11]"; "After it has sent a PUBCOMP, the receiver MUST treat
// any subsequent PUBLISH packet that contains that Packet Identifier as
// being a new Application Message [MQTT-4.3.3-12]". MQTT 3.1.1 3.7: PUBCOMP
// carries the Packet Identifier alone; 4.3.3 [MQTT-4.3.3-2].
//
// An MQTT 5 client sends a PUBREL for identifier 7, which none of its
// PUBLISHes used: PUBCOMP 7, 0x92. It completes a QoS 2 exchange under
// identifier 8, PUBCOMP 0x00, and sends PUBREL 8 again: PUBCOMP 8, 0x92. Its
// PINGREQ is then answered: the connection stays. An MQTT 3.1.1 client's
// PUBREL for identifier 7 gets PUBCOMP 7.
func TestUnknownPubrelGetsPubcomp92(t *testing.T) {
	t.Run(harness.VersionName(harness.V5), func(t *testing.T) {
		c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		pubrel(t, c, 7, 0x92)
		p := &harness.Publish{QoS: 2, ID: 8, Topic: "q2/" + clientID(t, ""), Payload: []byte("x")}
		support.Sent(t, c, p)
		pubrecOf(t, c, p, 0x00)
		pubrel(t, c, p.ID, 0x00)
		pubrel(t, c, p.ID, 0x92)
		taken(t, c, "two PUBRELs for identifiers no exchange holds")
	})
	t.Run(harness.VersionName(harness.V311), func(t *testing.T) {
		c, _ := harness.Connected(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		pubrel(t, c, 7, 0x00)
	})
}

// design.md, Flows, PUBLISH: "A QoS 2 refusal rides the PUBREC"; "At QoS 1 or
// 2 an MQTT 3.1.1 client has no refusal code and is disconnected"; "After a
// refusing PUBREC, a PUBLISH with that identifier is a new message
// (MQTT-4.3.3-9)"; the refusal table: "0x90 | a topic [...] under $share/".
// MQTT 5 4.3.3: "If it has sent a PUBREC with a Reason Code of 0x80 or
// greater, the receiver MUST treat any subsequent PUBLISH packet that
// contains that Packet Identifier as being a new Application Message
// [MQTT-4.3.3-9]".
//
// A subscriber holds the test's q2/ topics at QoS 1. An MQTT 5 client sends
// a QoS 2 PUBLISH on $share/g/x: PUBREC 0x90. Under the same identifier it
// sends a QoS 2 PUBLISH on .../new: PUBREC 0x00, and its PUBREL gets
// PUBCOMP 0x00, the subscriber receiving it: a new message, held and
// released. An MQTT 3.1.1 client's QoS 2 PUBLISH on $share/g/x closes its
// connection, nothing sent first.
func TestQoS2RefusalFreesItsIdentifier(t *testing.T) {
	t.Run(harness.VersionName(harness.V5), func(t *testing.T) {
		prefix := "q2/" + clientID(t, "")
		sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, prefix+"/#"))
		c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		refused := &harness.Publish{QoS: 2, Topic: "$share/g/x", Payload: []byte("refused")}
		support.Sent(t, c, refused)
		pubrecOf(t, c, refused, 0x90)
		fresh := &harness.Publish{QoS: 2, ID: refused.ID, Topic: prefix + "/new", Payload: []byte("new")}
		support.Sent(t, c, fresh)
		pubrecOf(t, c, fresh, 0x00)
		pubrel(t, c, fresh.ID, 0x00)
		liveAt(t, sub, 1, fresh.Topic, fresh.Payload)
	})
	t.Run(harness.VersionName(harness.V311), func(t *testing.T) {
		c, _ := harness.Connected(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		support.Sent(t, c, &harness.Publish{QoS: 2, Topic: "$share/g/x", Payload: []byte("refused")})
		harness.ExpectClosed(t, c)
	})
}

// heldLimit is the qos2.max_inflight_per_client of
// TestHeldQoS2TakesOneOfTheReceiveMaximum.
const heldLimit = 2

// overflows has c publish at QoS 1 under packet identifier id, which no
// exchange of its session holds, and fails the test unless it reads
// DISCONNECT 0x93 (Receive Maximum exceeded), then the end of stream. when
// says what c holds.
func overflows(t *testing.T, c *harness.Client, id uint16, when string) {
	t.Helper()
	support.Sent(t, c, &harness.Publish{QoS: 1, ID: id, Topic: "q2/over", Payload: []byte("over")})
	d, err := c.Recv()
	if err != nil {
		t.Fatalf("a QoS 1 PUBLISH %s: %v; want DISCONNECT 0x93", when, err)
	}
	if d, ok := d.(*harness.Disconnect); !ok || d.Reason != 0x93 {
		t.Fatalf("a QoS 1 PUBLISH %s: %s %+v; want DISCONNECT 0x93 (Receive Maximum exceeded)", when, harness.Name(d.Type()), d)
	}
	harness.ExpectClosed(t, c)
}

// design.md, Flows, PUBLISH: "A reader counts its client's unanswered QoS 1
// and 2 PUBLISHes against qos2.max_inflight_per_client, the value its
// CONNACK carries"; "A written PUBACK, PUBCOMP or refusing PUBREC frees one.
// A held QoS 2 identifier takes exactly one until its PUBCOMP, including one
// held from an earlier connection, which counts from the CONNACK. A PUBLISH
// repeating a held identifier takes none"; "If none is being sent, the
// connection ends with DISCONNECT 0x93"; DISCONNECT, Reasons Sagüin sends:
// "0x93 for Receive Maximum exceeded". MQTT 5 3.3.4: "If it receives more
// than Receive Maximum QoS 1 and QoS 2 PUBLISH packets where it has not sent
// a PUBACK or PUBCOMP in response, the Server uses a DISCONNECT packet with
// Reason Code 0x93 (Receive Maximum exceeded)".
//
// With qos2.max_inflight_per_client heldLimit (2), an MQTT 5 client holding a
// durable session holds two QoS 2 PUBLISHes, PUBREC 0x00 to each. A repeat of
// the first, DUP 1, gets PUBREC 0x00 and the connection stays, its PINGREQ
// answered; a QoS 1 PUBLISH then gets DISCONNECT 0x93. Reconnected with Clean
// Start 0, Session Present 1, its first packet, a QoS 1 PUBLISH, gets
// DISCONNECT 0x93 too: both identifiers count from the CONNACK. Reconnected
// again, its PUBREL of the first gets PUBCOMP 0x00, and a QoS 1 PUBLISH then
// gets PUBACK 0x00.
func TestHeldQoS2TakesOneOfTheReceiveMaximum(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxInflightPerClient: heldLimit})
	connect := kept(harness.V5, "limited")
	c, ack := harness.Connected(t, b.Addr, harness.V5, connect)
	if rm, ok := ack.Props.Get(harness.ReceiveMaximum); !ok || rm.Int != heldLimit {
		t.Fatalf("CONNACK's Receive Maximum %+v (present %v), want %d", rm, ok, heldLimit)
	}
	first := &harness.Publish{QoS: 2, ID: 1, Topic: "q2/limited/1", Payload: []byte("first")}
	second := &harness.Publish{QoS: 2, ID: 2, Topic: "q2/limited/2", Payload: []byte("second")}
	for _, p := range []*harness.Publish{first, second} {
		support.Sent(t, c, p)
		pubrecOf(t, c, p, 0x00)
	}
	repeat := *first
	repeat.Dup = true
	support.Sent(t, c, &repeat)
	pubrecOf(t, c, &repeat, 0x00)
	taken(t, c, "a PUBLISH repeating a held identifier with Receive Maximum held")
	overflows(t, c, 3, "with two identifiers held on its connection")

	c = resumed(t, b.Addr, harness.V5, connect)
	overflows(t, c, 3, "first after the CONNACK, two identifiers held from an earlier connection")

	c = resumed(t, b.Addr, harness.V5, connect)
	pubrel(t, c, first.ID, 0x00)
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, ID: 3, Topic: "q2/limited/3", Payload: []byte("third")})
}

// storage.md: "An incoming QoS 2 channel publication from any session is
// held in the store before PUBREC"; held_publishes: "A non-durable session's
// rows are deleted when it ends and at start". design.md, Flows, PUBLISH: "A
// PUBREL for an unknown identifier gets PUBCOMP 0x92"; CONNECT: "A takeover
// of a session whose expiry is 0 ends it and starts a new one"; Words:
// Session, durable "when its expiry is above zero". metrics.md:
// saguin_qos2_abandoned_total, "held QoS 2 messages discarded because their
// session ended".
//
// On a broker of its own with an operations listener, an MQTT 5 client whose
// session ends with its connection sends a QoS 2 PUBLISH: PUBREC 0x00, and
// the database then holds its held row. The client drops its connection:
// saguin_qos2_abandoned_total comes to 1, and the database holds no held
// row. Connected again under the same client id with Clean Start 0, Session
// Present 0, its PUBREL gets PUBCOMP 0x92; after a graceful stop the
// database holds no record of the message.
func TestEndedSessionAbandonsItsHeldQoS2(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "fleeting", CleanStart: true})
	p := &harness.Publish{QoS: 2, Topic: "q2/fleeting", Payload: []byte("abandoned")}
	support.Sent(t, c, p)
	pubrecOf(t, c, p, 0x00)
	if n := heldCopies(t, db, p.Payload); n != 1 {
		t.Errorf("after PUBREC 0x00, the database holds %d held rows of the message; want 1, stored before PUBREC", n)
	}
	support.DropSocket(t, c)
	awaitSeries(t, b, map[string]float64{testkit.SeriesKey("saguin_qos2_abandoned_total"): 1})
	if n := heldCopies(t, db, p.Payload); n != 0 {
		t.Errorf("once its session ended, the database holds %d held rows of the message; want none", n)
	}
	c, ack := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "fleeting"})
	if ack.SessionPresent {
		t.Fatal("CONNACK with Clean Start 0 has Session Present 1: the session that ended with its connection is still there")
	}
	pubrel(t, c, p.ID, 0x92)
	stopped(t, b)
	if rs := storedCopies(t, b.DB, p.Payload); len(rs) != 0 {
		t.Errorf("after the graceful stop, the database holds %d records of the abandoned message; want none", len(rs))
	}
}

// design.md, Flows, PUBLISH, the table: QoS 2 "The held copy, offset and
// retained flag move in one transaction. On a full disk, the move is lost
// and no PUBCOMP is sent. The held copy stays stored. The client's PUBREL,
// resent when it reconnects, completes the move"; Store, A full disk: "a
// write is answered "full" only when SQLite reports lack of space and
// rollback confirms that the transaction did not commit. The broker
// continues after that answer"; Flows, Replies: PUBCOMP "sent only once the
// store confirms". metrics.md: saguin_store_full, "1 while the disk is
// full: from a write SQLite could not save for room until the next saved
// one". MQTT 5 4.4 [MQTT-4.4.0-1]: PUBREL resent on reconnecting.
//
// On a filesystem of its own, an MQTT 5 client holding a durable session
// sends a QoS 2 PUBLISH, PUBREC 0x00; a ballast then takes every byte left.
// Its PUBREL meets the full disk: once saguin_store_full reads 1, the
// client's PINGREQ's PINGRESP is its next packet, no PUBCOMP before it, and
// the database still holds the held row and no record. Once the ballast is
// emptied, the client drops its connection, reconnects with Clean Start 0,
// Session Present 1, and resends the PUBREL: PUBCOMP 0x00. After a graceful
// stop the database holds the message's record once and no held row.
func TestQoS2PubrelMeetingAFullDisk(t *testing.T) {
	t.Parallel()
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.OpsLaunch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	db := support.OpenDB(t, b.DB)
	connect := kept(harness.V5, "full-release")
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	p := &harness.Publish{QoS: 2, Topic: "full/released", Payload: []byte("moved")}
	support.Sent(t, c, p)
	pubrecOf(t, c, p, 0x00)
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	if err := c.Send(&harness.Ack{Kind: harness.PUBREL, ID: p.ID}); err != nil {
		t.Fatal(err)
	}
	testkit.AwaitSample(t, b.Metrics(), "saguin_store_full", 1, harness.DefaultTimeout)
	taken(t, c, "the PUBREL whose move met the full disk")
	if n := heldCopies(t, db, p.Payload); n != 1 {
		t.Errorf("after the move refused for room, the database holds %d held rows of the message; want 1, the held copy kept", n)
	}
	if rs := storedCopies(t, b.DB, p.Payload); len(rs) != 0 {
		t.Errorf("after the move refused for room, the database holds %d records of the message; want none", len(rs))
	}

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	support.DropSocket(t, c)
	c = resumed(t, b.Addr, harness.V5, connect)
	pubrel(t, c, p.ID, 0x00)
	stopped(t, b)
	expectStoredOnce(t, b.DB, p.Payload, "after the PUBREL resent once there was room")
}

// retainedPayload is the size of each retained value of
// TestHeldQoS2IsChargedBeforePubrec: one fits retainedLimit, two do not.
const retainedPayload = 3000

// design.md, Flows, PUBLISH, the table: QoS 2 "enters on PUBREL, from a held
// copy stored and charged to the channel before PUBREC. The held copy,
// offset and retained flag move in one transaction"; "It is not stored
// twice, or charged twice against its channel's size"; the refusal table: "0x97 | over [...] the retained values'
// limit, a channel's max_bytes"; Channels: "A hard byte limit counts
// committed charges and reservations for accepted writes"; $broadcast,
// Retained: "each topic's charge against it is the largest of its committed
// current value and its accepted pending replacements, including held QoS 2
// replacements [...] A proposed replacement is admitted if that sum, with
// its proposed charge included, fits"; "A retained publish takes the flag";
// "RETAIN is 1 on retained values sent to a new subscription". storage.md:
// "A message's bytes are its topic's, payload's and properties' bytes".
//
// One broker has the channel capped (capped/#) with max_bytes 100, and
// retained.max_bytes retainedLimit (4096). Each case's MQTT 5 client:
//   - a channel's max_bytes: a QoS 2 PUBLISH on capped/t whose record is
//     60 bytes (sized) gets PUBREC 0x00; while it is held, a QoS 1 PUBLISH
//     of 48 bytes gets PUBACK 0x97. Its PUBREL gets PUBCOMP 0x00, and a QoS
//     1 PUBLISH of 40 bytes, the room left, then PUBACK 0x00: the move
//     charged nothing again. capped holds those two, 100 bytes.
//   - the retained values' limit: a retained QoS 2 PUBLISH on rq/a of
//     retainedPayload gets PUBREC 0x00; while it is held, a retained QoS 1
//     PUBLISH of the same size on rq/b gets PUBACK 0x97. Its PUBREL gets
//     PUBCOMP 0x00, and a new subscription to rq/# then receives rq/a alone,
//     its retained value, with RETAIN 1.
func TestHeldQoS2IsChargedBeforePubrec(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: capped(100), RetainedMaxBytes: retainedLimit})
	t.Run("a channel's max_bytes", func(t *testing.T) {
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		held := support.Sized("capped/t", 60)
		held.QoS = 2
		support.Sent(t, c, held)
		pubrecOf(t, c, held, 0x00)
		answered(t, c, support.Sized("capped/t", 48), 0x97)
		pubrel(t, c, held.ID, 0x00)
		answered(t, c, support.Sized("capped/t", 40), 0x00)
		if n, size := cappedRecords(t, b.DB); n != 2 || size != 100 {
			t.Errorf("capped holds %d records of %d bytes; want 2 of 100: the held one charged once", n, size)
		}
	})
	t.Run("the retained values' limit", func(t *testing.T) {
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		held := retain("rq/a", make([]byte, retainedPayload))
		held.QoS = 2
		support.Sent(t, c, held)
		pubrecOf(t, c, held, 0x00)
		answered(t, c, retain("rq/b", make([]byte, retainedPayload)), 0x97)
		pubrel(t, c, held.ID, 0x00)
		sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, "rq/#"))
		retainedPassAt(t, sub, 1, map[string][]byte{held.Topic: held.Payload})
		taken(t, sub, "the retained pass")
	})
}

// design.md, Flows, PUBLISH: "The reader records receipt time when the
// complete packet body has arrived, before decoding work or core handover
// delays. That time is carried with the packet. Message Expiry and
// publish-rate accounting use receipt time"; "A held QoS 2 publication
// preserves its original receipt-based expiry through PUBREL"; Delivery,
// Message Expiry: "On $broadcast, an expired message is skipped for that
// subscriber and counted". MQTT 5 4.3.3: the receiver "MUST continue the QoS
// 2 acknowledgement sequence even if it has applied message expiry
// [MQTT-4.3.3-13]".
//
// A subscriber holds the test's q2/ topics at QoS 1. An MQTT 5 client sends
// a QoS 2 PUBLISH with a Message Expiry Interval of 1 second, PUBREC 0x00,
// and waits until late (a second) past that second from its sending. Its
// PUBREL gets PUBCOMP 0x00. A publication of its at QoS 1 is then the
// subscriber's next packet: the held one, expired by its receipt, skipped.
func TestHeldQoS2KeepsItsReceiptExpiry(t *testing.T) {
	const lifetime = time.Second
	prefix := "q2/" + clientID(t, "")
	sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, prefix+"/#"))
	c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	p := &harness.Publish{QoS: 2, Topic: prefix + "/expiring", Props: expiry(uint32(lifetime / time.Second)), Payload: []byte("expired")}
	sentAt := support.Sent(t, c, p)
	pubrecOf(t, c, p, 0x00)
	time.Sleep(time.Until(sentAt.Add(lifetime + support.Late)))
	pubrel(t, c, p.ID, 0x00)
	thenLiveAt(t, c, sub, 1, prefix+"/after")
}

// design.md, Restart, Graceful stop: "MQTT listeners close"; "Every client
// gets 0x8B, except one whose DISCONNECT the core has taken. The 0x8B follows
// the PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken
// before the stop whose changes the stop stores (R4)"; Flows, Replies:
// "CONNACK (when it changes a session), PUBACK, PUBREC, PUBREL, PUBCOMP [...]
// are sent only once the store confirms"; "Acknowledgements of stored changes
// preserve their packet order". 0011a.md, 261022 144815: "a held publication
// the stop stores is answered with PUBREC before the connection's 0x8B".
//
// An MQTT 5 client holding a durable session holds a QoS 2 PUBLISH, PUBREC
// 0x00. The test then holds the database's write lock while the client
// sends at once a second QoS 2 PUBLISH and the first's PUBREL, then a
// PINGREQ; the broker runs under strace. Once its store writer has met the
// lock, the PINGRESP, read first, says the core has taken both packets
// (design.md, Execution, Decisions: "Each connection's packet order is
// preserved"). SIGTERM goes; once the listener refuses a connection, the
// stop has begun, and the lock is released. The client reads PUBREC 0x00 to
// the second, then PUBCOMP 0x00 to the first, then DISCONNECT 0x8B and the
// end of stream, and the broker exits 0.
func TestStopAnswersQoS2ExchangesFirst(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "stopping-q2"))
	first := &harness.Publish{QoS: 2, Topic: "stop/q2/first", Payload: []byte("first")}
	support.Sent(t, c, first)
	pubrecOf(t, c, first, 0x00)

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	second := &harness.Publish{QoS: 2, ID: c.NextID(), Topic: "stop/q2/second", Payload: []byte("second")}
	harness.SendBytes(t, c, slices.Concat(harness.Encoded(t, second, harness.V5), harness.Encoded(t, &harness.Ack{Kind: harness.PUBREL, ID: first.ID}, harness.V5)))
	awaitLockMet(t, b.trace, before, "the PUBLISH and the PUBREL")
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the PUBLISH and the PUBREL: %v; want the PINGRESP first, their replies waiting for the store", err)
	}
	b.terminate(t)
	listenerClosed(t, b.Addr)
	lock.release(t)

	for _, want := range []harness.Ack{{Kind: harness.PUBREC, ID: second.ID}, {Kind: harness.PUBCOMP, ID: first.ID}} {
		p, err := c.Recv()
		if err != nil {
			t.Fatalf("after SIGTERM: %v; want %s 0x00 to packet %d, in packet order, then DISCONNECT 0x8B", err, harness.Name(want.Kind), want.ID)
		}
		if a, ok := p.(*harness.Ack); !ok || a.Kind != want.Kind || a.ID != want.ID || a.Reason != 0 {
			t.Fatalf("after SIGTERM the client read %s %+v; want %s 0x00 to packet %d, in packet order, before DISCONNECT 0x8B",
				harness.Name(p.Type()), p, harness.Name(want.Kind), want.ID)
		}
	}
	shutDown(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b.Broker); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// design.md, Flows, CONNECT, the refusal table: "0x9B | a QoS 2 Will with
// qos2 denied | 5", and with no acl_file "everything is allowed" (Access and
// reload); DISCONNECT, A lost connection: "The Will goes after its Will
// Delay, or at once for MQTT 3.1.1"; Words: Record, "It holds an id, time,
// topic, QoS"; storage.md, records: qos, "the QoS it was published at". MQTT
// 5 3.1.2.6: "If the Will Flag is set to 1, the value of Will QoS can be 0
// (0x00), 1 (0x01), or 2 (0x02) [MQTT-3.1.2-12]".
//
// On a broker of its own with no acl_file, a subscriber holds q2will/# at QoS
// 1. In MQTT 5 and in MQTT 3.1.1, a client connects with a QoS 2 Will of no
// delay: CONNACK 0x00. It drops its connection, and the subscriber receives
// the Will at QoS 1. After a graceful stop, the database holds each Will's
// record at QoS 2.
func TestQoS2WillIsPublishedAtQoS2(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "q2will-sub", CleanStart: true}, support.At(1, "q2will/#"))
	wills := make([]*harness.Will, 0, len(bothVersions))
	for _, version := range bothVersions {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			w := &harness.Will{QoS: 2, Topic: "q2will/" + clientID(t, ""), Payload: []byte(clientID(t, ""))}
			c, _ := harness.Connected(t, b.Addr, version, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true, Will: w})
			support.DropSocket(t, c)
			acknowledge(t, sub, liveAt(t, sub, 1, w.Topic, w.Payload).ID, 0)
			wills = append(wills, w)
		})
	}
	stopped(t, b)
	for _, w := range wills {
		if rs := storedCopies(t, b.DB, w.Payload); len(rs) != 1 || rs[0].qos != 2 {
			t.Errorf("the database holds %d records of the Will on %q (%+v); want one, at QoS 2", len(rs), w.Topic, rs)
		}
	}
}

// metrics.md: saguin_qos2_held, "incoming QoS 2 messages of connected
// sessions waiting for their PUBREL: the identifiers the core holds against
// qos2.max_inflight_per_client"; Rules: "A gauge only the core knows is an
// atomic the core sets". design.md, Execution, Resource bounds: the core
// holds "held QoS 2 identifiers bounded by qos2.max_inflight_per_client";
// "An away session is in the store alone"; CONNECT, 9: "Held QoS 2 exchanges
// wait for their PUBREL".
//
// On a broker of its own with an operations listener, an MQTT 5 client
// holding a durable session holds two QoS 2 PUBLISHes: saguin_qos2_held
// reads 2. The PUBREL of one, PUBCOMP 0x00, brings it to 1. The client goes
// away (DISCONNECT): 0, its session no longer connected. Reconnected with
// Clean Start 0, Session Present 1: 1 again, the other identifier held.
func TestQoS2HeldCountsConnectedSessions(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	gauge := testkit.SeriesKey("saguin_qos2_held")
	connect := kept(harness.V5, "gauged")
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	ps := []*harness.Publish{
		{QoS: 2, Topic: "q2/gauged/1", Payload: []byte("1")},
		{QoS: 2, Topic: "q2/gauged/2", Payload: []byte("2")},
	}
	for _, p := range ps {
		support.Sent(t, c, p)
		pubrecOf(t, c, p, 0x00)
	}
	awaitSeries(t, b, map[string]float64{gauge: 2})
	pubrel(t, c, ps[0].ID, 0x00)
	awaitSeries(t, b, map[string]float64{gauge: 1})
	support.Away(t, c)
	awaitSeries(t, b, map[string]float64{gauge: 0})
	resumed(t, b.Addr, harness.V5, connect)
	awaitSeries(t, b, map[string]float64{gauge: 1})
}
