package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of durable delivery across a reconnect, a restart and a crash:
// what a kept session is owed when it comes back, and what its consumers'
// positions and in-flight windows keep in the store meanwhile (design.md,
// Flows, CONNECT, 9. Resume; Delivery, Positions and A resend; Restart).

// resumed connects p, which keeps its session (Clean Start 0), to addr in
// version, failing the test unless the CONNACK has Session Present 1: MQTT 5
// 3.2.2.1.1, with Clean Start 0 and Session State for the ClientID, "it
// MUST set Session Present to 1" [MQTT-3.2.2-3]; design.md, Flows,
// CONNECT: "Session Present is 1 when the CONNECT keeps its session [...] a
// session exists for the client id, either away".
func resumed(t *testing.T, addr string, version byte, p *harness.Connect) *harness.Client {
	t.Helper()
	p.CleanStart = false
	c, ack := harness.Connected(t, addr, version, p)
	if !ack.SessionPresent {
		t.Fatalf("CONNACK to %q with Clean Start 0 has Session Present 0: the durable session is gone", p.ClientID)
	}
	return c
}

// redelivered reads c's next packet and fails the test unless it is first's
// resend: a PUBLISH of first's topic and payload at first's QoS, DUP 1, under
// first's packet identifier (MQTT 5 [MQTT-3.3.1-1]: "The DUP flag MUST be
// set to 1 by the Client or Server when it attempts to re-deliver a PUBLISH
// packet"; [MQTT-4.4.0-1]: "resend any unacknowledged PUBLISH packets [...]
// using their original Packet Identifiers"; design.md, CONNECT, 9. Resume:
// "in-flight messages [...] resends them with their packet identifiers [...]
// Resent messages have DUP set"). It returns the resend.
func redelivered(t *testing.T, c *harness.Client, first *harness.Publish) *harness.Publish {
	t.Helper()
	p := harness.Expect[*harness.Publish](t, c)
	switch {
	case p.Topic != first.Topic || !bytes.Equal(p.Payload, first.Payload):
		t.Fatalf("PUBLISH %q of payload %.40q; want the resend of %q of %.40q", p.Topic, p.Payload, first.Topic, first.Payload)
	case p.QoS != first.QoS || !p.Dup:
		t.Fatalf("the resend of %q came at QoS %d, DUP %v; want QoS %d, DUP 1 [MQTT-3.3.1-1]", p.Payload, p.QoS, p.Dup, first.QoS)
	case p.ID != first.ID:
		t.Fatalf("the resend of %q came under packet identifier %d; want its original %d [MQTT-4.4.0-1]", p.Payload, p.ID, first.ID)
	}
	return p
}

// committed has c publish at QoS 1 on topic, which none of its
// subscriptions match, and waits for the PUBACK 0x00. The core takes a
// connection's packets in order (design.md, Execution, Decisions: "Each
// connection's packet order is preserved"), eligible writes join in their
// store-request order (Store, Writes) and a PUBACK waits for its commit
// (R4): once it arrives, every change c's earlier packets made is
// committed.
func committed(t *testing.T, c *harness.Client, topic string) {
	t.Helper()
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("barrier")})
}

// offsetsByPayload reads the records in the database at path and returns
// each one's offset by its payload; the tests give each record a payload of
// its own.
func offsetsByPayload(t *testing.T, path string) map[string]int64 {
	t.Helper()
	rs := records(t, path)
	offsets := make(map[string]int64, len(rs))
	for i := range rs {
		offsets[string(rs[i].payload)] = rs[i].offset
	}
	return offsets
}

// passing reads from db client's position on $broadcast, its one channel,
// and its session_inflight rows, failing the test unless each row's moves
// is 0 where the position is past its record and 1 where it is not: no
// seek is made in these tests (design.md, Delivery, Positions: "a durable
// session may also pass records represented by stored QoS 1 or QoS 2
// exchanges. Saving progress past an unsettled exchange marks it moves = 0
// in the same transaction"; storage.md, session_inflight, moves: "1 while
// the row protects or may move the current position; 0 after a seek or saved
// progress supersedes it"). when says when.
func passing(t *testing.T, db *sql.DB, client, when string) (int64, []inflightRow) {
	t.Helper()
	positions := storedSubscriptions(t, db, client).positions
	if len(positions) != 1 || positions[0].channel != 1 {
		t.Fatalf("%s, positions holds %+v; want one row, on $broadcast", when, positions)
	}
	position := positions[0].offset
	rows := storedInflight(t, db, client)
	for _, r := range rows {
		want := 1
		if position > r.offset {
			want = 0
		}
		if r.moves != want {
			t.Errorf("%s, the position is %d and the row of offset %d has moves %d; want %d", when, position, r.offset, r.moves, want)
		}
	}
	return position, rows
}

// design.md, Flows, CONNECT, 9. Resume: "The broker reads in-flight messages
// and PUBRELs from the store and resends them with their packet identifiers
// and in their order. Resent messages have DUP set (MQTT-4.4.0-1)"; "New
// application deliveries wait behind recovered PUBLISHes". Restart: "the
// broker restores durable sessions with their subscriptions, positions,
// in-flight windows [...] Each session is loaded only at its CONNECT".
// storage.md, session_inflight: "resent in seq order", seq "its place in the
// session's sending order". MQTT 5 [MQTT-4.4.0-1]: "When a Client reconnects
// with Clean Start set to 0 and a session is present, both the Client and
// Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0) [...]
// using their original Packet Identifiers"; [MQTT-4.6.0-1], which the server
// applies in its turn: "re-send them in the order in which the original
// PUBLISH packets were sent"; [MQTT-3.3.1-1] and [MQTT-3.3.1-3]: DUP 1 on a
// resend alone.
//
// A client holding a durable session subscribes at QoS 1. A publisher
// stores three messages; the client receives them and acknowledges the
// first alone, which the core takes; a fourth then goes out, under the
// identifier the first freed where the broker reuses it, so that the
// sending order differs from the identifiers' order. The client sends
// DISCONNECT, and while it is away two more messages are stored; in the
// second case the broker then stops and starts again on the same files.
// The client resumes the session: it receives the second, third and fourth
// again, DUP 1, under their identifiers, in the order first sent, then the
// two stored while it was away, DUP 0. The first, acknowledged, does not
// come back first.
func TestResumeResendsUnacknowledgedFirst(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart bool
	}{{"after a reconnect", false}, {"after a restart", true}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			b := support.Launch(t, &harness.Options{Dir: dir})
			connect := &harness.Connect{ClientID: "resumer", CleanStart: true, Props: support.Durable}
			sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "resume/#"))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "resume-pub", CleanStart: true})
			ps := numbered("resume/x", 6)
			support.PublishAll(t, pub, ps[:3])
			first := support.DeliveredAt(t, sub, 1, ps[0].Topic, ps[0].Payload)
			inflight := make([]*harness.Publish, 0, 3)
			for _, p := range ps[1:3] {
				inflight = append(inflight, support.DeliveredAt(t, sub, 1, p.Topic, p.Payload))
			}
			acknowledge(t, sub, first.ID, 0)
			taken(t, sub, "the PUBACK of the first delivery")
			support.Acknowledged(t, pub, ps[3])
			inflight = append(inflight, support.DeliveredAt(t, sub, 1, ps[3].Topic, ps[3].Payload))
			support.Away(t, sub)
			support.PublishAll(t, pub, ps[4:])
			if tc.restart {
				stopped(t, b)
				b = support.Launch(t, &harness.Options{Dir: dir})
			}

			sub = resumed(t, b.Addr, harness.V5, connect)
			for _, p := range inflight {
				acknowledge(t, sub, redelivered(t, sub, p).ID, 0)
			}
			for _, p := range ps[4:] {
				support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
			}
		})
	}
}

// design.md, Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records, except that a
// durable session may also pass records represented by stored QoS 1 or QoS
// 2 exchanges. Saving progress past an unsettled exchange marks it moves =
// 0 in the same transaction; its acknowledgement then settles only that
// exchange"; "QoS 0 settles on successful writing". CONNECT, 9. Resume:
// "The broker reads in-flight messages and PUBRELs from the store and
// resends them [...] Resent messages have DUP set". storage.md, positions: offset "the
// next offset for new delivery; unfinished exchanges may precede it".
// MQTT 5 4.3.1: a QoS 0 message "arrives at the receiver either once or
// not at all"; [MQTT-4.4.0-1]: the Server resends "any unacknowledged
// PUBLISH packets", and "MUST NOT resend messages at any other time";
// [MQTT-4.6.0-2]: a client acknowledges in the order it received, so
// what follows an unacknowledged QoS 1 delivery settles only at QoS 0.
//
// A client holding a durable session subscribes at QoS 1. A publisher
// stores a QoS 1 message, then two at QoS 0; the client receives the three
// and acknowledges none, the QoS 0 ones settling as they are written. It
// sends DISCONNECT and resumes: the first comes again, DUP 1, under its
// identifier, and next a QoS 0 message stored after the return, not the
// two settled. It sends DISCONNECT again and the broker stops: the
// database holds the session's position past every record it received,
// and the first's row alone, moves 0. After the start the client resumes:
// the first comes again, DUP 1; once it is acknowledged, a message stored
// after the restart comes, nothing settled before it.
func TestResumeSendsNothingSettledAgain(t *testing.T) {
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	const client = "settler"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "settle/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "settle-pub", CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "settle/a", Payload: []byte("unacknowledged")},
		{QoS: 0, Topic: "settle/b", Payload: []byte("settled 1")},
		{QoS: 0, Topic: "settle/b", Payload: []byte("settled 2")},
		{QoS: 0, Topic: "settle/c", Payload: []byte("after the return")},
		{QoS: 1, Topic: "settle/c", Payload: []byte("after the restart")},
	}
	support.PublishAll(t, pub, ps[:3])
	first := support.DeliveredAt(t, sub, 1, ps[0].Topic, ps[0].Payload)
	for _, p := range ps[1:3] {
		delivered(t, sub, p.Topic, p.Payload)
	}
	support.Away(t, sub)

	sub = resumed(t, b.Addr, harness.V5, connect)
	redelivered(t, sub, first)
	support.PublishAll(t, pub, ps[3:4])
	delivered(t, sub, ps[3].Topic, ps[3].Payload)
	support.Away(t, sub)
	stopped(t, b)
	offsets := offsetsByPayload(t, b.DB)
	position, rows := passing(t, support.OpenDB(t, b.DB), client, "after the stop")
	if received := offsets[string(ps[3].Payload)]; position <= received {
		t.Errorf("after the stop, the position is %d; want it past %d, every record the session received", position, received)
	}
	if len(rows) != 1 || rows[0].packetID != int64(first.ID) || rows[0].offset != offsets[string(ps[0].Payload)] {
		t.Errorf("after the stop, session_inflight holds %+v; want the unacknowledged delivery's row alone, packet identifier %d", rows, first.ID)
	}

	b = support.Launch(t, &harness.Options{Dir: dir})
	sub = resumed(t, b.Addr, harness.V5, connect)
	acknowledge(t, sub, redelivered(t, sub, first).ID, 0)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "settle-pub", CleanStart: true})
	support.Acknowledged(t, pub, ps[4])
	support.DeliveredAt(t, sub, 1, ps[4].Topic, ps[4].Payload)
}

// sessionID reads the id of client's durable session from db (storage.md,
// sessions).
func sessionID(t *testing.T, db *sql.DB, client string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRowContext(context.Background(), `SELECT id FROM sessions WHERE client = ?`, client).Scan(&id); err != nil {
		t.Fatalf("reading the session id of %q: %v", client, err)
	}
	return id
}

// sessionTables are the tables holding a durable session's rows, by its
// id (storage.md, Sessions).
var sessionTables = []string{"sessions", "subscriptions", "subscription_starts", "positions", "session_inflight"}

// rowsOfSession counts the rows the session of id holds in db, over every
// table of sessionTables.
func rowsOfSession(t *testing.T, db *sql.DB, id int64) int {
	t.Helper()
	total := 0
	for _, table := range sessionTables {
		column := "session"
		if table == "sessions" {
			column = "id"
		}
		var n int
		if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM `+table+` WHERE `+column+` = ?`, id).Scan(&n); err != nil {
			t.Fatalf("counting the rows of session %d in %s: %v", id, table, err)
		}
		total += n
	}
	return total
}

// design.md, Flows, CONNECT: "Session Present is 1 when the CONNECT keeps its
// session and a session exists for the client id [...] Keeping the session
// means Clean Start 0"; 6. "Keeping the session resumes it"; 9. Resume:
// "in-flight messages [...] resends them with their packet identifiers and in
// their order. Resent messages have DUP set (MQTT-4.4.0-1)"; 5. Take over.
// DISCONNECT: "A session that ends with its connection is ended in the store
// where it has rows". MQTT 5 3.1.2.4: "If a CONNECT packet is received with
// Clean Start set to 0 and there is a Session associated with the Client
// Identifier, the Server MUST resume communications with the Client based on
// state from the existing Session [MQTT-3.1.2-5]"; 3.1.2.11.2: "If the
// Session Expiry Interval is absent the value 0 is used. If it is set to 0,
// or is absent, the Session ends when the Network Connection is closed";
// [MQTT-4.4.0-1]; [MQTT-3.3.1-1].
//
// A client holding a durable session subscribes at QoS 1 and receives two
// messages, acknowledging neither. It comes back with Clean Start 0 and a
// Session Expiry Interval absent, or 0: after sending DISCONNECT, or by
// taking over its own connection. Session Present is 1, and it receives
// both messages again, DUP 1, under their identifiers, in their order.
// Once that connection ends with its DISCONNECT, the database holds no row
// of the session in any table.
func TestResumeWithSessionExpiryZero(t *testing.T) {
	for _, tc := range []struct {
		name     string
		takeover bool
		props    harness.Properties // the resuming CONNECT's
	}{
		{"reconnect, expiry absent", false, nil},
		{"reconnect, expiry 0", false, support.SessionExpiry(0)},
		{"takeover, expiry absent", true, nil},
		{"takeover, expiry 0", true, support.SessionExpiry(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := support.Launch(t, &harness.Options{})
			db := support.OpenDB(t, b.DB)
			const client = "ending"
			sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}, support.At(1, "ending/#"))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ending-pub", CleanStart: true})
			ps := numbered("ending/x", 2)
			support.PublishAll(t, pub, ps)
			inflight := make([]*harness.Publish, len(ps))
			for i, p := range ps {
				inflight[i] = support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
			}
			id := sessionID(t, db, client)
			if !tc.takeover {
				support.Away(t, sub)
			}

			c := resumed(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, Props: tc.props})
			for _, p := range inflight {
				redelivered(t, c, p)
			}
			support.Away(t, c)
			if !testkit.Within(harness.DefaultTimeout, func() bool { return rowsOfSession(t, db, id) == 0 }) {
				t.Errorf("once the connection with Session Expiry 0 ended, the database still holds %d rows of the session after %v; want none",
					rowsOfSession(t, db, id), harness.DefaultTimeout)
			}
		})
	}
}

// design.md, Choices MQTT leaves open: "QoS 0 messages for away sessions |
// delivered on return"; Words, Session: an away session keeps its
// "subscriptions, positions"; R5: "A consumer reads only stored records,
// from its own position; a slow consumer, or none, only falls behind";
// Delivery, Order: "a channel's offset order on $broadcast"; SUBSCRIBE, One
// copy: "The delivery uses the lower of the publication's QoS and the highest
// contributing grant". MQTT 5 4.1: the Server's Session State holds "QoS 1
// and QoS 2 messages pending transmission to the Client" and "Optionally, QoS
// 0 messages pending transmission to the Client".
//
// A client holding a durable session subscribes at QoS 1, then sends
// DISCONNECT. A publisher sends four messages back to back, alternating
// QoS 0 and 1 and two topics. The client resumes the session and receives
// all four, in the order sent, which is their offsets', each at its own
// QoS, DUP 0.
func TestAwaySessionReceivesWhatWasPublished(t *testing.T) {
	prefix := clientID(t, "")
	connect := &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: support.Durable}
	support.Away(t, support.Subscribed(t, brokerAddr, harness.V5, connect, support.At(1, prefix+"/#")))
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 0, Topic: prefix + "/a", Payload: []byte("1")},
		{QoS: 1, Topic: prefix + "/b", Payload: []byte("2")},
		{QoS: 0, Topic: prefix + "/b", Payload: []byte("3")},
		{QoS: 1, Topic: prefix + "/a", Payload: []byte("4")},
	}
	support.PublishAll(t, pub, ps)

	sub := resumed(t, brokerAddr, harness.V5, connect)
	for _, p := range ps {
		if got := support.DeliveredAt(t, sub, p.QoS, p.Topic, p.Payload); got.QoS == 1 {
			acknowledge(t, sub, got.ID, 0)
		}
	}
}

// design.md, Flows, CONNECT, 9. Resume: "The new connection's Receive
// Maximum governs resumed PUBLISHes as well as new ones. Outstanding
// PUBLISHes are offered in stored sending order, only while the new
// connection has quota. Unsent recovered entries remain in the store and
// consume no new-connection send quota. [...] New application deliveries
// wait behind recovered PUBLISHes." storage.md, session_inflight: rows "by
// packet identifier", "resent in seq order". MQTT 5 4.9: "If the send
// quota reaches zero, the Client or Server MUST NOT send any more PUBLISH
// packets with QoS > 0 [MQTT-4.9.0-2]"; the quota "is incremented by 1 [...]
// Each time a PUBACK or PUBCOMP packet is received".
//
// A client holding a durable session, with Receive Maximum 4, subscribes
// at QoS 1, receives four messages and acknowledges none; it sends
// DISCONNECT, and a fifth message is stored. It resumes with Receive
// Maximum 2: it receives the first two again, DUP 1, and nothing more for a
// while, the database still holding all four rows under their identifiers,
// in their order. Each PUBACK then brings the next of the four, DUP 1,
// under its identifier; only once the last of them has gone does the fifth
// come, DUP 0.
func TestResumeUnderASmallerReceiveMaximum(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: "shrinking", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(4))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "shrink/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "shrink-pub", CleanStart: true})
	ps := numbered("shrink/x", 5)
	support.PublishAll(t, pub, ps[:4])
	inflight := make([]*harness.Publish, 4)
	for i := range inflight {
		inflight[i] = support.DeliveredAt(t, sub, 1, ps[i].Topic, ps[i].Payload)
	}
	support.Away(t, sub)
	support.Acknowledged(t, pub, ps[4])

	connect.Props = slices.Concat(support.Durable, receiveMaximum5(2))
	sub = resumed(t, b.Addr, harness.V5, connect)
	redelivered(t, sub, inflight[0])
	redelivered(t, sub, inflight[1])
	nothingWithin(t, sub, quiet, "with the new connection's Receive Maximum of 2 taken by two resends")
	rows := storedInflight(t, db, "shrinking")
	ids := make([]uint16, len(rows))
	for i, r := range rows {
		ids[i] = uint16(r.packetID)
	}
	want := []uint16{inflight[0].ID, inflight[1].ID, inflight[2].ID, inflight[3].ID}
	if !slices.Equal(ids, want) {
		t.Errorf("with two of four recovered PUBLISHes resent, session_inflight holds packet identifiers %v in seq order; want %v, the unsent ones remaining", ids, want)
	}

	for i := range 2 {
		acknowledge(t, sub, inflight[i].ID, 0)
		redelivered(t, sub, inflight[i+2])
	}
	acknowledge(t, sub, inflight[2].ID, 0)
	support.DeliveredAt(t, sub, 1, ps[4].Topic, ps[4].Payload)
}

// messageExpiry returns p's Message Expiry Interval and whether it has one.
func messageExpiry(p *harness.Publish) (uint32, bool) {
	e, ok := p.Props.Get(harness.MessageExpiryInterval)
	return e.Int, ok
}

// design.md, Delivery, A resend: "A resend uses the Subscription
// Identifiers and delivery properties saved for its first transmission.
// Subscription changes affect new deliveries. The first-delivery RETAIN
// value is preserved. Topic Alias is established for the current
// connection; where that delivery carries Message Expiry, its remaining
// interval is recomputed from the record and never increased"; Message
// Expiry: "Expiry applies only to a message not yet sent. A resend still goes
// under the resend rule above (Delivery; MQTT-4.3.3-7)". SUBSCRIBE, One copy:
// "Subscription Identifiers appear in complete filter-byte order". MQTT 5
// [MQTT-4.3.3-7]: the sender "MUST NOT apply Message expiry if a PUBLISH
// packet has been sent"; [MQTT-3.3.2-6]: the interval sent is "the received
// value minus the time that the Application Message has been waiting in the
// Server"; 3.3.2.3.3: "If absent, the Application Message does not expire",
// so a resend without one would have its expiry increased; [MQTT-3.3.2-7]: "A
// receiver MUST NOT carry forward any Topic Alias mappings from one Network
// Connection to another"; [MQTT-3.3.1-13]: Retain As Published sets RETAIN
// "equal to the RETAIN flag in the received PUBLISH packet".
//
// A client holding a durable session, allowing Topic Aliases, subscribes
// at QoS 1 with Retain As Published and Subscription Identifier 4. A
// publisher stores a retained message expiring in 600 seconds and one
// expiring in 1 second; the client receives both and acknowledges neither.
// It then subscribes to the same filter again without Retain As Published,
// identifier 9, and to a second filter, identifier 7, both with Retain
// Handling 2, and sends
// DISCONNECT; a third message, retained, is stored. Once the second has
// expired, the client resumes: the first comes again with RETAIN 1 and
// identifier 4 alone, its Message Expiry Interval below the first
// transmission's and no more than 600 less the whole seconds since its
// publication; the second, expired, comes again too, its interval 0; the
// third comes with RETAIN 0 and identifiers 9 and 7, the filters' byte
// order. The harness client refuses a Topic Alias the new connection never
// set up.
func TestResendKeepsItsFirstTransmission(t *testing.T) {
	const lasting = 600
	prefix := clientID(t, "")
	aliases := harness.Properties{{ID: harness.TopicAliasMaximum, Int: 4}}
	connect := &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: slices.Concat(support.Durable, aliases)}
	sub, _ := harness.Connected(t, brokerAddr, harness.V5, connect)
	support.Suback(t, sub, identifier(4), []harness.Subscription{{Filter: prefix + "/#", QoS: 1, RetainAsPublished: true}}, 0x01)
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	kept := &harness.Publish{QoS: 1, Retain: true, Topic: prefix + "/kept", Props: expiry(lasting), Payload: []byte("kept")}
	brief := &harness.Publish{QoS: 1, Topic: prefix + "/brief", Props: expiry(1), Payload: []byte("brief")}
	publishedAt := time.Now()
	support.PublishAll(t, pub, []*harness.Publish{kept, brief})

	first := support.DeliveredAt(t, sub, 1, kept.Topic, kept.Payload)
	firstBrief := support.DeliveredAt(t, sub, 1, brief.Topic, brief.Payload)
	read := time.Now()
	firstExpiry, ok := messageExpiry(first)
	if !first.Retain || !ok {
		t.Fatalf("the first delivery has RETAIN %v and Message Expiry Interval %v; want RETAIN 1 (Retain As Published) and an interval, the resend's to compare with", first.Retain, ok)
	}
	// Retain Handling 2: no retained message is sent at these SUBSCRIBEs
	// ("the Server MUST NOT send the retained messages" [MQTT-3.3.1-11]).
	support.Suback(t, sub, identifier(9), []harness.Subscription{{Filter: prefix + "/#", QoS: 1, RetainHandling: 2}}, 0x01)
	support.Suback(t, sub, identifier(7), []harness.Subscription{{Filter: prefix + "/+", QoS: 1, RetainHandling: 2}}, 0x01)
	support.Away(t, sub)
	later := &harness.Publish{QoS: 1, Retain: true, Topic: prefix + "/later", Payload: []byte("later")}
	support.Acknowledged(t, pub, later)
	// The 1-second message expires while the client is away, and more
	// than a second passes between the first transmissions and the
	// resends: the behaviour under test.
	time.Sleep(time.Until(read.Add(1500 * time.Millisecond)))

	sub = resumed(t, brokerAddr, harness.V5, connect)
	again := redelivered(t, sub, first)
	if !again.Retain {
		t.Error("the resend of the retained message has RETAIN 0; want the first delivery's 1")
	}
	expectIDs(t, again, 4)
	elapsed := uint32(time.Since(publishedAt)/time.Second) + 1
	switch remaining, ok := messageExpiry(again); {
	case !ok:
		t.Errorf("the resend carries no Message Expiry Interval; the first carried %d", firstExpiry)
	case remaining >= firstExpiry || remaining < lasting-elapsed:
		t.Errorf("the resend's Message Expiry Interval is %d, the first's %d, at most %d seconds after publication; want below the first's, at least %d", remaining, firstExpiry, elapsed, lasting-elapsed)
	}
	againBrief := redelivered(t, sub, firstBrief)
	expectIDs(t, againBrief, 4)
	if remaining, ok := messageExpiry(againBrief); !ok || remaining != 0 {
		t.Errorf("the expired message's resend has Message Expiry Interval %d (present %v); want 0, its time run out and never increased", remaining, ok)
	}
	acknowledge(t, sub, again.ID, 0)
	acknowledge(t, sub, againBrief.ID, 0)
	fresh := support.DeliveredAt(t, sub, 1, later.Topic, later.Payload)
	if fresh.Retain {
		t.Error("the message stored after Retain As Published was dropped came with RETAIN 1; want 0, the new options")
	}
	expectIDs(t, fresh, 9, 7)
}

// design.md, Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records, except that a
// durable session may also pass records represented by stored QoS 1 or QoS
// 2 exchanges"; "Deleting a durable session's settled exchange row must
// commit with its position beyond that record, unless a seek supersedes that
// progress."; "A position is saved with the next
// commit, following the dirty-position rule (Store, A full disk). Recovery
// can replay unsaved progress. Before handing a QoS 1 or QoS 2 delivery to a
// durable session's writer, the core stores its packet identifier, its order
// (seq), its record and its phase". Store, full mode: "Everything
// acknowledged survives a crash or a power cut"; "A restart recovers
// committed state". storage.md, positions: offset "the next offset
// for new delivery; unfinished exchanges may precede it"; sessions: one left
// connected by a crash "is away from the start that follows". MQTT 5
// [MQTT-4.4.0-1].
//
// In full mode, a client holding a durable session, with Receive Maximum
// 3, subscribes at QoS 1; a publisher stores six messages. The client
// acknowledges the first three as they come and receives the next three,
// unacknowledged; its own publication's PUBACK then shows the core's
// changes committed. The database then holds its position past the three
// acknowledged, and session_inflight rows for the fourth to the sixth,
// under the identifiers they came with, each with moves 0 where the
// position has passed it. Two more messages are stored, and the broker is
// killed. Its database holds the same position and rows when it starts
// again, and the client, resuming, receives the fourth to the sixth again,
// DUP 1, then the seventh and the eighth, DUP 0: nothing acknowledged
// before the commit comes back ahead of them, and nothing stored is lost.
func TestCrashResendsNothingSavedAndLosesNothingOwed(t *testing.T) {
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	const client = "crashed"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(3))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "crash/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "crash-pub", CleanStart: true})
	ps := numbered("crash/x", 8)
	support.PublishAll(t, pub, ps[:6])
	for _, p := range ps[:3] {
		acknowledge(t, sub, support.DeliveredAt(t, sub, 1, p.Topic, p.Payload).ID, 0)
	}
	inflight := make([]*harness.Publish, 3)
	for i, p := range ps[3:6] {
		inflight[i] = support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
	}
	committed(t, sub, "crash-barrier")
	offsets := offsetsByPayload(t, b.DB)
	want := make([]inflightRow, len(inflight))
	for i, p := range inflight {
		want[i] = inflightRow{packetID: int64(p.ID), channel: 1, offset: offsets[string(p.Payload)], qos: 1, phase: 1}
	}
	position, rows := passing(t, support.OpenDB(t, b.DB), client, "once the client's PUBLISH after its PUBACKs was acknowledged")
	if position <= offsets[string(ps[2].Payload)] {
		t.Errorf("the position is %d; want it past %d, the third record, acknowledged", position, offsets[string(ps[2].Payload)])
	}
	got := slices.Clone(rows)
	for i := range got {
		got[i].seq, got[i].moves = 0, 0
	}
	if !slices.Equal(got, want) {
		t.Errorf("session_inflight holds %+v, seq and moves aside; want %+v", got, want)
	}
	support.PublishAll(t, pub, ps[6:])
	support.Crash(t, b)

	b = support.Launch(t, &harness.Options{Dir: dir})
	if again, againRows := passing(t, support.OpenDB(t, b.DB), client, "after the crash"); again != position || !slices.Equal(againRows, rows) {
		t.Errorf("after the crash, the position is %d and session_inflight holds %+v; want %d and %+v, as committed before it", again, againRows, position, rows)
	}
	sub = resumed(t, b.Addr, harness.V5, connect)
	for _, p := range inflight {
		acknowledge(t, sub, redelivered(t, sub, p).ID, 0)
	}
	for _, p := range ps[6:] {
		support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
	}
}

// design.md, Flows, SUBSCRIBE, The start point: "each subscription has a
// live-start boundary for every channel it reaches. The core captures that
// channel's tail when it accepts the subscription, and stores it with a
// durable session's subscription"; "Adding a filter does not move an
// existing consumer position"; One copy: "Only subscriptions eligible for
// the record contribute: topic, live-start boundary [...] checks apply".
// Restart: "the broker restores durable sessions with their subscriptions,
// positions, in-flight windows". storage.md, subscription_starts. From the
// maintainer's decision on audit finding 261013-007a: a record matching only
// a filter whose start is above it is not owed, before or after a restart.
//
// A client holding a durable session, with Receive Maximum 1, subscribes
// at QoS 1 to one topic; it receives a message there and holds it, so its
// position stays below what follows. A message is stored on a second
// topic, then the client subscribes to that topic, and another message is
// stored there. The client sends DISCONNECT, and the broker stops and
// starts again on the same files. The client resumes: it receives the held
// message again, DUP 1, and once it acknowledges it, the message stored
// after the second subscription: the one stored below that subscription's
// start, which no other filter matches, was never owed.
func TestRestartKeepsANewFiltersStart(t *testing.T) {
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	connect := &harness.Connect{ClientID: "starter", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(1))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "start/a"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "start-pub", CleanStart: true})
	held := &harness.Publish{QoS: 1, Topic: "start/a", Payload: []byte("held")}
	support.Acknowledged(t, pub, held)
	first := support.DeliveredAt(t, sub, 1, held.Topic, held.Payload)
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "start/b", Payload: []byte("below")})
	support.Suback(t, sub, nil, support.At(1, "start/b"), 0x01)
	above := &harness.Publish{QoS: 1, Topic: "start/b", Payload: []byte("above")}
	support.Acknowledged(t, pub, above)
	support.Away(t, sub)
	stopped(t, b)

	b = support.Launch(t, &harness.Options{Dir: dir})
	sub = resumed(t, b.Addr, harness.V5, connect)
	acknowledge(t, sub, redelivered(t, sub, first).ID, 0)
	support.DeliveredAt(t, sub, 1, above.Topic, above.Payload)
}

// design.md, Delivery, Positions: "an acknowledgement (PUBACK, or PUBCOMP)
// settles its own delivery. The position advances only past the
// contiguous settled or skipped prefix of examined records, except that a
// durable session may also pass records represented by stored QoS 1 or
// QoS 2 exchanges"; "A position is saved with the next commit [...];
// recovery can replay unsaved progress"; Preparation: "When the connection
// ends, deliveries prepared and never handed to the writer are cancelled
// [...] a continuing session receives them later", and "After a crash, a
// stored preparation is resent as recovered, with DUP 1, because it might
// have been sent"; Words: Consumer, "a consumer is a session's subscription
// to a channel. It has a position"; Restart: "the broker restores durable
// sessions with their subscriptions, positions, in-flight windows"; Store,
// full mode: "Everything acknowledged survives a crash". storage.md,
// positions: "A durable session's place in each channel it consumes", offset
// "the next offset for new delivery; unfinished exchanges may precede it".
// MQTT 5 [MQTT-4.4.0-1]; [MQTT-3.3.1-1].
//
// An append channel, history, holds history/#; a publisher stores six
// messages there. A client holding a durable session, with Receive
// Maximum 2, subscribes at QoS 1 to history/# and so starts at the floor:
// it acknowledges the first two as they come and holds the next two; its
// own publication's PUBACK then shows the core's changes committed. The
// database holds one position for the session, on history, past the second
// record. Then the client sends DISCONNECT, or the broker is killed and
// started again on the same files. The client resumes: the third and the
// fourth come again, DUP 1, under their packet identifiers, then the fifth
// and the sixth, at QoS 1 (DUP 0 after a DISCONNECT), each acknowledged in
// turn: nothing acknowledged comes back, and nothing stored is skipped.
func TestReaderCutOffMidHistoryContinues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		crash bool
	}{{"a disconnection", false}, {"a crash", true}} {
		t.Run(tc.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{{Name: "history", Type: "append", Filter: "history/#"}}}
			b := support.Launch(t, o)
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "history-pub", CleanStart: true})
			ps := numbered("history/x", 6)
			support.PublishAll(t, pub, ps)
			connect := &harness.Connect{ClientID: "historian", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(2))}
			sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "history/#"))
			for _, p := range ps[:2] {
				acknowledge(t, sub, support.DeliveredAt(t, sub, 1, p.Topic, p.Payload).ID, 0)
			}
			held := []*harness.Publish{support.DeliveredAt(t, sub, 1, ps[2].Topic, ps[2].Payload), support.DeliveredAt(t, sub, 1, ps[3].Topic, ps[3].Payload)}
			committed(t, sub, "history-barrier")
			channel := storedChannels(t, b.DB)["history"].id
			second := offsetsByPayload(t, b.DB)["2"]
			if got := storedSubscriptions(t, support.OpenDB(t, b.DB), "historian").positions; len(got) != 1 || got[0].channel != channel || got[0].offset <= second {
				t.Errorf("positions holds %+v; want one row, on history (channel %d), past %d, the second record, acknowledged", got, channel, second)
			}

			if tc.crash {
				support.Crash(t, b)
				b = support.Launch(t, o)
			} else {
				support.Away(t, sub)
			}
			sub = resumed(t, b.Addr, harness.V5, connect)
			for _, p := range held {
				acknowledge(t, sub, redelivered(t, sub, p).ID, 0)
			}
			for _, p := range ps[4:] {
				got := harness.Expect[*harness.Publish](t, sub)
				switch {
				case got.Topic != p.Topic || !bytes.Equal(got.Payload, p.Payload) || got.QoS != 1:
					t.Fatalf("PUBLISH %q of %q at QoS %d; want %q of %q at QoS 1, the next owed", got.Topic, got.Payload, got.QoS, p.Topic, p.Payload)
				case got.Dup && !tc.crash:
					t.Errorf("%q came with DUP 1 after a DISCONNECT; want 0, never handed to the writer before", got.Payload)
				}
				acknowledge(t, sub, got.ID, 0)
			}
		})
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "In-flight messages are
// finished. A consumer's position on a channel ends with the last
// subscription reaching it"; Delivery, Positions: "an acknowledgement
// (PUBACK, or PUBCOMP) settles its own delivery"; Sending: "Packet
// identifiers use the lowest free identifier". storage.md, session_inflight:
// "Messages in flight to a durable session". MQTT 5 3.10.4: the Server "MUST
// complete the delivery of any QoS 1 or QoS 2 messages which match the Topic
// Filters and it has started to send to the Client" [MQTT-3.10.4-3]; 4.3.2:
// "The Packet Identifier becomes available for reuse once the sender has
// received the PUBACK packet".
//
// A client holding a durable session, with Receive Maximum 1, subscribes
// at QoS 1, receives a message and unsubscribes before acknowledging it:
// UNSUBACK 0x00, and the database still holds the delivery's row. The
// client's PUBACK then finishes the exchange: once its own publication is
// acknowledged, the row is gone. The client subscribes again, and the next
// message comes under the same packet identifier, the one PUBACK freed.
func TestPubackAfterUnsubscribeFinishesTheExchange(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	const client = "unsubscriber"
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true,
		Props: slices.Concat(support.Durable, receiveMaximum5(1))}, support.At(1, "unsub/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "unsub-pub", CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "unsub/a", Payload: []byte("first")})
	first := support.DeliveredAt(t, sub, 1, "unsub/a", []byte("first"))
	unsuback(t, sub, nil, []string{"unsub/#"}, 0x00)
	if rows := storedInflight(t, db, client); len(rows) != 1 || rows[0].packetID != int64(first.ID) {
		t.Fatalf("after the UNSUBSCRIBE, session_inflight holds %+v; want one row, packet identifier %d, its exchange unfinished", rows, first.ID)
	}

	acknowledge(t, sub, first.ID, 0)
	committed(t, sub, "unsub-barrier")
	if rows := storedInflight(t, db, client); len(rows) != 0 {
		t.Errorf("after the PUBACK, session_inflight holds %+v; want none, the exchange finished", rows)
	}
	support.Suback(t, sub, nil, support.At(1, "unsub/#"), 0x01)
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "unsub/b", Payload: []byte("second")})
	if next := support.DeliveredAt(t, sub, 1, "unsub/b", []byte("second")); next.ID != first.ID {
		t.Errorf("the next delivery came under packet identifier %d; want %d, freed by the PUBACK after the UNSUBSCRIBE", next.ID, first.ID)
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A delivery prepared but not yet handed to the writer is cancelled and
// never sent when no remaining subscription is owed it, as when the
// connection ends. A durable session's stored preparation for it is removed
// in the UNSUBSCRIBE's own transaction", whose preparations are "deleted in the departure's
// transaction" (Delivery, Preparation); Preparation: "each consumer's live
// scan may select records ahead of the client's free window. It may keep at
// most one window of selected deliveries not yet handed to the writer. A
// selected QoS 1 or 2 delivery has its packet identifier and, for a durable
// session, its stored exchange"; "A connection's prepared and sent
// deliveries together are at most twice its window"; `$saguin` topics,
// Every request: "One descriptor within the connection's twice-window
// bound is reserved for direct answers", so a window of 3 holds three sent
// deliveries and two prepared; One mechanism: "A message is in flight from
// its hand-off to the writer"; Sending: "Packet identifiers use the lowest
// free identifier"; Flows, Replies: UNSUBACK "sent only once the store
// confirms". MQTT 5 [MQTT-3.10.4-2], [MQTT-3.10.4-3]; 4.9: the send quota "is
// incremented by 1 [...] Each time a PUBACK or PUBCOMP packet is received".
// TestUnsubscribeCancelsWhatIsPreparedForIt checks the wire alone, for a
// session that is not durable.
//
// A client holding a durable session, with Receive Maximum 3, subscribes
// at QoS 1 to detached/#; a publisher stores five messages there. The
// client receives the first three, on packet identifiers 1 to 3, and holds
// them, the database then holding the rows of all five, the last two
// prepared. The client unsubscribes: once the UNSUBACK arrives, the
// database holds the rows of the first three alone. It subscribes to
// newer/#, where two messages are stored: their rows follow, the first on
// packet identifier 4, the lowest free, the cancelled preparations' freed.
// Its PUBACK of 1 brings the first newer message, on 4, and its PUBACK of
// 2 the second: hand-off in selection order, and never the fourth or fifth
// message of detached/#.
func TestUnsubscribeDeletesItsStoredPreparations(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	const client = "detached"
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true,
		Props: slices.Concat(support.Durable, receiveMaximum5(3))}, support.At(1, "detached/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "detached-pub", CleanStart: true})
	ps := numbered("detached/x", 5)
	support.PublishAll(t, pub, ps)
	first := support.DeliveredAt(t, sub, 1, ps[0].Topic, ps[0].Payload)
	second := support.DeliveredAt(t, sub, 1, ps[1].Topic, ps[1].Payload)
	third := support.DeliveredAt(t, sub, 1, ps[2].Topic, ps[2].Payload)
	if first.ID != 1 || second.ID != 2 || third.ID != 3 {
		t.Fatalf("the first three deliveries came on packet identifiers %d, %d and %d; want 1, 2 and 3, the lowest free", first.ID, second.ID, third.ID)
	}
	offsets := offsetsByPayload(t, b.DB)
	firstRow := &inflightRow{packetID: 1, channel: 1, offset: offsets["1"], qos: 1, phase: 1, moves: 1}
	firstSeq := expectInflight(t, db, client, []int64{offsets["1"], offsets["2"], offsets["3"], offsets["4"], offsets["5"]}, firstRow, 0,
		"once the client has the first three deliveries")
	unsuback(t, sub, nil, []string{"detached/#"}, 0x00)
	if got := rowOffsets(storedInflight(t, db, client)); !slices.Equal(got, []int64{offsets["1"], offsets["2"], offsets["3"]}) {
		t.Errorf("once the UNSUBSCRIBE is acknowledged, session_inflight holds the records at offsets %v; want %v, the prepared two deleted with it",
			got, []int64{offsets["1"], offsets["2"], offsets["3"]})
	}

	support.Suback(t, sub, nil, support.At(1, "newer/#"), 0x01)
	newer := []*harness.Publish{{QoS: 1, Topic: "newer/x", Payload: []byte("n1")}, {QoS: 1, Topic: "newer/x", Payload: []byte("n2")}}
	support.PublishAll(t, pub, newer)
	offsets = offsetsByPayload(t, b.DB)
	expectInflight(t, db, client, []int64{offsets["1"], offsets["2"], offsets["3"], offsets["n1"], offsets["n2"]}, &inflightRow{packetID: 4,
		channel: 1, offset: offsets["n1"], qos: 1, phase: 1, moves: 1}, firstSeq, "once the newer messages are stored")
	acknowledge(t, sub, first.ID, 0)
	if got := support.DeliveredAt(t, sub, 1, newer[0].Topic, newer[0].Payload); got.ID != 4 {
		t.Errorf("the first newer message came on packet identifier %d; want 4, the lowest free at its preparation", got.ID)
	}
	acknowledge(t, sub, second.ID, 0)
	support.DeliveredAt(t, sub, 1, newer[1].Topic, newer[1].Payload)
}

// design.md, Delivery: "A consumer may prepare up to one send window of
// selected deliveries ahead of hand-off, storing QoS 1/2 exchanges and
// holding their identifiers"; "On orderly departure, or another change
// ending an exchange, the transaction deletes stored preparations known
// never to have been handed to a writer and saves the affected live
// position no further than their earliest record. Resume selects those
// records afresh with DUP 0 and the lowest free identifier. Exchanges
// handed off, or inherited as possibly sent, retain their original
// identifiers and resume with DUP 1"; Sending: "Packet identifiers use the
// lowest free identifier"; Preparation: "A connection's prepared and sent
// deliveries together are at most twice its window"; `$saguin` topics,
// Every request: "One descriptor within the connection's twice-window
// bound is reserved for direct answers", so a window of 3 holds three sent
// deliveries and two prepared. MQTT 5 [MQTT-4.4.0-1]: on reconnection the
// Server resends "any unacknowledged PUBLISH packets [...] using their
// original Packet Identifiers"; [MQTT-3.3.1-1]: DUP 1 only when it
// "attempts to re-deliver a PUBLISH packet"; [MQTT-4.3.2-2]: a new
// message goes with DUP 0.
//
// A client holding a durable session, with Receive Maximum 3, subscribes
// at QoS 1; a publisher stores five messages. The client receives the
// first three, on identifiers 1 to 3, the other two prepared behind them;
// its PUBACK of the second brings the fourth, on 4, and the database then
// holds the fifth's row, prepared on 5, never sent. The client sends
// DISCONNECT and resumes: the first, third and fourth come again, DUP 1,
// on 1, 3 and 4; its PUBACK of the fourth then brings the fifth, DUP 0, on
// 2, the lowest free, not the 5 it was prepared on.
func TestUnsentPreparationResumesAsAFirstDelivery(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	const client = "unsent"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(3))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "unsent/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "unsent-pub", CleanStart: true})
	ps := numbered("unsent/x", 5)
	support.PublishAll(t, pub, ps)
	offsets := offsetsByPayload(t, b.DB)
	sent := make([]*harness.Publish, 4)
	for i := range 3 {
		if sent[i] = support.DeliveredAt(t, sub, 1, ps[i].Topic, ps[i].Payload); sent[i].ID != uint16(i+1) {
			t.Fatalf("delivery %d: packet identifier %d; want %d, the lowest free", i+1, sent[i].ID, i+1)
		}
	}
	acknowledge(t, sub, sent[1].ID, 0)
	if sent[3] = support.DeliveredAt(t, sub, 1, ps[3].Topic, ps[3].Payload); sent[3].ID != 4 {
		t.Fatalf("the fourth delivery: packet identifier %d; want 4, held since its preparation", sent[3].ID)
	}
	expectInflight(t, db, client, []int64{offsets["1"], offsets["3"], offsets["4"], offsets["5"]}, &inflightRow{packetID: 5, channel: 1,
		offset: offsets["5"], qos: 1, phase: 1, moves: 1}, 0, "once the client has the fourth delivery")
	support.Away(t, sub)

	sub = resumed(t, b.Addr, harness.V5, connect)
	redelivered(t, sub, sent[0])
	redelivered(t, sub, sent[2])
	acknowledge(t, sub, redelivered(t, sub, sent[3]).ID, 0)
	if fifth := support.DeliveredAt(t, sub, 1, ps[4].Topic, ps[4].Payload); fifth.ID != 2 {
		t.Errorf("the fifth, prepared on 5 and never sent: packet identifier %d; want 2, the lowest free", fifth.ID)
	}
}
