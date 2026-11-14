package conformance

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of QoS 1 delivery: a delivery's packet identifier, the
// client's PUBACK settling it, the in-flight window, and a durable
// session's delivery stored before it is sent (design.md, Flows, Delivery).

// quiet is how long a test watches a connection to see that the broker
// sends nothing more on it: well past the time a delivery the broker has
// stored takes to arrive on loopback, so that one sent when it must not be
// arrives within it.
const quiet = 500 * time.Millisecond

// taken has c send a PINGREQ and fails the test unless the next packet is
// its PINGRESP: the core answers it only once it has taken every packet c
// sent before (design.md, Execution, Decisions: "Each connection's packet
// order is preserved"), and nothing else comes first. after names what c
// sent before.
func taken(t *testing.T, c *harness.Client, after string) {
	t.Helper()
	if err := c.Ping(); err != nil {
		t.Fatalf("after %s: %v", after, err)
	}
}

// takenKeeping is taken where a reply may come before the PINGRESP: it
// awaits the PINGRESP, keeping what comes before it for Recv.
func takenKeeping(t *testing.T, c *harness.Client, after string) {
	t.Helper()
	if err := c.Send(&harness.Pingreq{}); err != nil {
		t.Fatalf("after %s: PINGREQ: %v", after, err)
	}
	if _, err := c.Await(func(p harness.Packet) bool { _, ok := p.(*harness.Pingresp); return ok }); err != nil {
		t.Fatalf("after %s: waiting for PINGRESP: %v", after, err)
	}
}

// acknowledge sends c's PUBACK with reason to packet identifier id
// (MQTT 5 [MQTT-4.3.2-4], 3.4.2.1 Table 3-4).
func acknowledge(t *testing.T, c *harness.Client, id uint16, reason byte) {
	t.Helper()
	if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: id, Reason: reason}); err != nil {
		t.Fatalf("PUBACK %d 0x%02X: %v", id, reason, err)
	}
}

// receiveMaximum5 is the property section of an MQTT 5 CONNECT holding one
// property, a Receive Maximum of n.
func receiveMaximum5(n uint32) harness.Properties {
	return harness.Properties{{ID: harness.ReceiveMaximum, Int: n}}
}

// numbered returns n QoS 1 publications on topic, their payloads "1", "2"
// and so on.
func numbered(topic string, n int) []*harness.Publish {
	ps := make([]*harness.Publish, n)
	for i := range ps {
		ps[i] = &harness.Publish{QoS: 1, Topic: topic, Payload: fmt.Appendf(nil, "%d", i+1)}
	}
	return ps
}

// design.md, Delivery, Sending: "Packet identifiers use the lowest free
// identifier"; Positions: "an acknowledgement (PUBACK, or PUBCOMP) settles
// its own delivery"; Words, Packet identifier: "each connection has two
// independent spaces of packet identifiers, the client's and the broker's".
// MQTT 5 4.3.2: the sender "MUST assign an unused Packet Identifier each time
// it has a new Application Message to publish [MQTT-4.3.2-1]" and "MUST send
// a PUBLISH packet containing this Packet Identifier with QoS 1 and DUP flag
// set to 0 [MQTT-4.3.2-2]"; "The Packet Identifier becomes available for
// reuse once the sender has received the PUBACK packet"; 4.9: a PUBACK frees
// its place "regardless of whether the PUBACK [...] carried an error code";
// 3.4.2.1 and 3.4.2.2.1: the Reason Code and Property Length "can be
// omitted", so a PUBACK may carry them, and Table 3-4 and 3.4.2.2 give it a
// failure code, a Reason String and User Properties. MQTT 3.1.1 4.3.2:
// [MQTT-4.3.2-1], [MQTT-4.3.2-2], and "the Packet Identifier becomes
// available for reuse once the Sender has received the PUBACK Packet".
//
// Under each version, a client subscribed at QoS 1 receives a publisher's
// QoS 1 messages one by one, each at QoS 1, DUP 0. The first takes packet
// identifier 1 and, 1 still unacknowledged, the second 2. Then, for each
// form of PUBACK the version has, the client acknowledges 1 in that form,
// a PINGREQ showing the broker took it, and the next message takes 1
// again: the shortest form, then in MQTT 5 a Reason Code 0x00 alone, a
// Reason Code and an empty property section, and failure 0x83 with a
// Reason String and two User Properties. Last, the client acknowledges 2,
// and the next message takes 2: free, below the 3 the broker never used,
// while 1 is still held.
func TestQoS1DeliveryIsSettledByItsPuback(t *testing.T) {
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			topic := clientID(t, "/qos1")
			sub := support.Subscribed(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, topic))
			pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			sentCount := 0
			// next publishes the next message and fails the test unless
			// its delivery takes packet identifier id.
			next := func(id uint16) {
				t.Helper()
				sentCount++
				payload := fmt.Appendf(nil, "%d", sentCount)
				support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: topic, Payload: payload})
				if got := support.DeliveredAt(t, sub, 1, topic, payload); got.ID != id {
					t.Fatalf("message %d: packet identifier %d; want %d, the lowest free", sentCount, got.ID, id)
				}
			}
			next(1)
			next(2)

			forms := [][]byte{{0x40, 0x02, 0x00, 0x01}}
			if version == harness.V5 {
				failure := &harness.Ack{Kind: harness.PUBACK, ID: 1, Reason: 0x83, Props: harness.Properties{
					{ID: harness.ReasonString, Str: "not now"}, harness.User("n", "v"), harness.User("n", "w"),
				}}
				forms = append(forms, []byte{0x40, 0x03, 0x00, 0x01, 0x00}, []byte{0x40, 0x04, 0x00, 0x01, 0x00, 0x00},
					harness.Encoded(t, failure, harness.V5))
			}
			for _, form := range forms {
				harness.SendBytes(t, sub, form)
				taken(t, sub, fmt.Sprintf("the PUBACK % X", form))
				next(1)
			}
			acknowledge(t, sub, 2, 0)
			taken(t, sub, "the PUBACK of 2")
			next(2)
		})
	}
}

// windowInflight is the broker.limits.max_inflight of TestInflightWindow.
const windowInflight = 4

// design.md, Delivery: "A consumer may prepare up to one send window of
// selected deliveries ahead of hand-off, storing QoS 1/2 exchanges and
// holding their identifiers [...] Hand-off follows selection order and
// alone debits send quota"; Capacities: "The preparation allowance equals
// the send window: the lower of Receive Maximum and limits.max_inflight, or
// the latter alone for MQTT 3.1.1"; Sending: "Packet identifiers use the
// lowest free identifier"; Positions: "an acknowledgement (PUBACK, or
// PUBCOMP) settles its own delivery". MQTT 5 3.3.4: "The Server
// MUST NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH packets
// for which it has not received PUBACK, PUBCOMP, or PUBREC with a Reason
// Code of 128 or greater from the Client [MQTT-3.3.4-9]"; 4.9: "If the
// send quota reaches zero, the Client or Server MUST NOT send any more
// PUBLISH packets with QoS > 0 [MQTT-4.9.0-2]", and the quota "is
// incremented by 1 [...] Each time a PUBACK or PUBCOMP packet is
// received". Delivery, Preparation: "A connection's prepared and sent
// deliveries together are at most twice its window"; `$saguin` topics,
// Every request: "One descriptor within the connection's twice-window
// bound is reserved for direct answers", so ordinary deliveries have
// twice the window minus one, and two are prepared behind a full window
// only from a window of 3.
//
// With max_inflight windowInflight: an MQTT 5 client whose Receive Maximum
// is below it, one whose Receive Maximum is above it, and an MQTT 3.1.1
// client, each subscribed at QoS 1, its window being 3, 4 and 4. A
// publisher's window+2 QoS 1 messages are stored first. The client
// receives the window's worth, packet identifiers 1 up, and nothing more
// while it holds them all, the last two messages being prepared meanwhile
// on window+1 and window+2. Its PUBACK of 1 brings the next message, on
// window+1, and again nothing more; its PUBACKs of the rest bring the
// last, on window+2. With that one alone held, a new message is selected
// on 1, the lowest free.
func TestInflightWindow(t *testing.T) {
	t.Parallel()
	b := support.Launch(t, &harness.Options{MaxInflight: windowInflight})
	for _, tc := range []struct {
		name    string
		version byte
		props   harness.Properties // the client's CONNECT's
		window  int
	}{
		{"MQTT 5, Receive Maximum below max_inflight", harness.V5, receiveMaximum5(windowInflight - 1), windowInflight - 1},
		{"MQTT 5, Receive Maximum above max_inflight", harness.V5, receiveMaximum5(windowInflight + 2), windowInflight},
		{"MQTT 3.1.1, max_inflight alone", harness.V311, nil, windowInflight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := clientID(t, "/window")
			sub := support.Subscribed(t, b.Addr, tc.version, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: tc.props}, support.At(1, topic))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			ps := numbered(topic, tc.window+2)
			support.PublishAll(t, pub, ps)

			for i := range tc.window {
				if got := support.DeliveredAt(t, sub, 1, topic, ps[i].Payload); got.ID != uint16(i+1) {
					t.Fatalf("delivery %d: packet identifier %d; want %d", i+1, got.ID, i+1)
				}
			}
			nothingWithin(t, sub, quiet, fmt.Sprintf("with a window of %d full", tc.window))
			acknowledge(t, sub, 1, 0)
			prepared := uint16(tc.window + 1) // held since its selection, while 1 was in flight
			if got := support.DeliveredAt(t, sub, 1, topic, ps[tc.window].Payload); got.ID != prepared {
				t.Fatalf("the delivery the PUBACK of 1 made room for: packet identifier %d; want %d, held since its preparation", got.ID, prepared)
			}
			nothingWithin(t, sub, quiet, fmt.Sprintf("with a window of %d full again", tc.window))
			for id := uint16(2); id <= prepared; id++ {
				acknowledge(t, sub, id, 0)
			}
			if got := support.DeliveredAt(t, sub, 1, topic, ps[tc.window+1].Payload); got.ID != prepared+1 {
				t.Fatalf("the last delivery: packet identifier %d; want %d, held since its preparation", got.ID, prepared+1)
			}
			taken(t, sub, "its PUBACKs of 2 to the window's end")
			later := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("later")}
			support.Acknowledged(t, pub, later)
			if got := support.DeliveredAt(t, sub, 1, topic, later.Payload); got.ID != 1 {
				t.Fatalf("a delivery selected with only %d held: packet identifier %d; want 1, the lowest free", prepared+1, got.ID)
			}
		})
	}
}

// design.md, Delivery, One mechanism for every channel: "While connected, the
// broker starts no resend. Its replies to repeated packets follow their QoS
// flow."; Slow clients: "A slow client fills only its own
// writer"; R5: "Publishers do not wait for consumer progress. A publication
// is acknowledged after its channel stores it". MQTT 5 4.9: "The Client and Server MUST continue to process and
// respond to all other MQTT Control Packets even if the quota is zero
// [MQTT-4.9.0-3]"; [MQTT-4.3.2-2]: a new message goes with DUP 0.
//
// A client with Receive Maximum 1 and another client subscribe at QoS 1 to
// one topic. A publisher sends three QoS 1 messages, each acknowledged, the
// other client receiving and acknowledging each as it comes. The first
// client, which has not acknowledged the first message, receives nothing
// more for a while, the first message not sent again; its PINGREQ is
// answered; once it acknowledges, it receives the second message, DUP 0.
func TestWithheldPubackHoldsBackOnlyItsClient(t *testing.T) {
	topic := clientID(t, "/withheld")
	withholder := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-withholder"), CleanStart: true,
		Props: receiveMaximum5(1)}, support.At(1, topic))
	other := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true}, support.At(1, topic))
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	ps := numbered(topic, 3)
	for _, p := range ps {
		support.Acknowledged(t, pub, p)
		acknowledge(t, other, support.DeliveredAt(t, other, 1, topic, p.Payload).ID, 0)
	}
	first := support.DeliveredAt(t, withholder, 1, topic, ps[0].Payload)
	nothingWithin(t, withholder, quiet, "with its one delivery unacknowledged")
	taken(t, withholder, "its window filled")
	acknowledge(t, withholder, first.ID, 0)
	support.DeliveredAt(t, withholder, 1, topic, ps[1].Payload)
}

// inflightRow is a row of storage.md's session_inflight, the session's id
// left out. A row that names no queue attempt has attempt, attempt_number
// and attempt_began NULL, which attempted false stands for.
type inflightRow struct {
	packetID, seq, channel, offset int64
	qos, phase, moves, share       int
	attempted                      bool
	outRetain                      int
	outProperties                  string // its bytes, "" where NULL
}

// storedInflight reads the session_inflight rows of client's durable
// session from db, in seq order.
func storedInflight(t *testing.T, db *sql.DB, client string) []inflightRow {
	t.Helper()
	var rs []inflightRow
	support.Query(t, db, `SELECT i.packet_id, i.seq, i.channel, i."offset", i.qos, i.phase, i.moves, i.share,
		i.attempt IS NOT NULL OR i.attempt_number IS NOT NULL OR i.attempt_began IS NOT NULL, i.out_retain, i.out_properties
		FROM session_inflight i JOIN sessions s ON s.id = i.session WHERE s.client = ? ORDER BY i.seq`,
		func(rows *sql.Rows) error {
			var r inflightRow
			var props []byte
			err := rows.Scan(&r.packetID, &r.seq, &r.channel, &r.offset, &r.qos, &r.phase, &r.moves, &r.share,
				&r.attempted, &r.outRetain, &props)
			r.outProperties = string(props)
			rs = append(rs, r)
			return err
		}, client)
	return rs
}

// expectInflight waits, up to the harness client's wait, until client's
// session_inflight rows in db name the records at offsets, in seq order,
// then fails the test unless the row of want's record is want, seq aside,
// which must be at least 1 and above after's; it returns that row's seq.
// when says when.
func expectInflight(t *testing.T, db *sql.DB, client string, offsets []int64, want *inflightRow, after int64, when string) int64 {
	t.Helper()
	var got []inflightRow
	for deadline := time.Now().Add(harness.DefaultTimeout); ; time.Sleep(10 * time.Millisecond) {
		got = storedInflight(t, db, client)
		if slices.Equal(offsets, rowOffsets(got)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s, session_inflight holds %+v; want rows for the records at offsets %v, in that order", when, got, offsets)
		}
	}
	i := slices.IndexFunc(got, func(r inflightRow) bool { return r.offset == want.offset })
	if i < 0 {
		t.Fatalf("%s, session_inflight holds %+v; want a row for the record at offset %d", when, got, want.offset)
	}
	seq := got[i].seq
	want.seq = seq
	if got[i] != *want {
		t.Errorf("%s, session_inflight holds %+v; want %+v", when, got[i], *want)
	}
	if seq < 1 || seq <= after {
		t.Errorf("%s, seq %d; want it at least 1 and above the earlier delivery's %d (storage.md: \"its place in the session's sending order, increasing\")", when, seq, after)
	}
	return seq
}

// rowOffsets returns the record offsets of rs, in their order.
func rowOffsets(rs []inflightRow) []int64 {
	offsets := make([]int64, len(rs))
	for i, r := range rs {
		offsets[i] = r.offset
	}
	return offsets
}

// design.md, Delivery, Positions: "Before handing a QoS 1 or QoS 2
// delivery to a durable session's writer, the core stores its packet
// identifier, its order (seq), its record and its phase; a failed save
// leaves it unsent"; Delivery: "A consumer may prepare up to one send
// window of selected deliveries ahead of hand-off, storing QoS 1/2
// exchanges and holding their identifiers"; "an acknowledgement (PUBACK, or PUBCOMP) settles its
// own delivery"; A resend: "uses the Subscription Identifiers and delivery
// properties saved for its first transmission", "The first-delivery RETAIN
// value is preserved". storage.md, session_inflight: packet_id, seq "its
// place in the session's sending order, increasing", channel and offset
// "the record", qos "the QoS it went out at", phase "1 PUBLISH sent",
// moves "1 while its acknowledgement moves the session's position", share
// "0 for none", the attempt columns NULL but for a queue job, out_retain
// "the first delivery's RETAIN value", out_properties "the first
// delivery's delivery properties [...], excluding Topic Alias and Message
// Expiry"; "Both out_ columns are saved with the exchange before its first
// transmission"; storage.md: "The $broadcast channel is channel 1". Store, A
// full disk: "The broker continues after that answer"; "At the deadline, the
// core [...] From its own state, it retries connected consumers'
// dirty-position marks and their deliveries left unsent by a refused save",
// the deadline "one second after that answer"; design.md, Testing: the
// operator sees the broker through "the files on disk".
//
// Delivery, Preparation: "A connection's prepared and sent deliveries
// together are at most twice its window"; `$saguin` topics, Every request:
// "One descriptor within the connection's twice-window bound is reserved
// for direct answers", so a window of 2 leaves ordinary deliveries three
// descriptors: two sent and one prepared ahead.
//
// On a filesystem of its own, a client holding a durable session, with
// Receive Maximum 2 and Topic Alias Maximum 4, subscribes at QoS 1 with
// Retain As Published and Subscription Identifier 4. A publisher stores
// five QoS 1 messages, the first retained and expiring in 600 seconds.
// The client receives the first, with a Topic Alias and a Message Expiry
// Interval, and the second, and the database then holds their rows and the
// third's, prepared ahead: the first's with its packet identifier,
// $broadcast's record, QoS 1, phase 1, moves 1, no share or attempt,
// out_retain 1 and out_properties the Subscription Identifier alone. Its
// PUBACK brings the third, on the identifier its row holds, and the rows
// become the second's, the third's and the fourth's: the first's is gone,
// settled. A ballast then takes every byte left, and the client
// acknowledges the second: the fourth, saved, arrives; the client
// acknowledges the third and the fourth, and the fifth, which the store
// cannot save, is not sent, and the connection still answers PINGREQ.
// Once the ballast is emptied, the fifth arrives with no new publication,
// its row then the only one: saved before the client saw it.
func TestDurableDeliveryIsStoredBeforeItIsSent(t *testing.T) {
	t.Parallel()
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	db := support.OpenDB(t, b.DB)
	const client = "durable-sub"
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true,
		Props: slices.Concat(support.Durable, receiveMaximum5(2), harness.Properties{{ID: harness.TopicAliasMaximum, Int: 4}})})
	support.Suback(t, sub, identifier(4), []harness.Subscription{{Filter: "inflight/#", QoS: 1, RetainAsPublished: true}}, 0x01)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "durable-pub", CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Retain: true, Topic: "inflight/a", Props: harness.Properties{{ID: harness.MessageExpiryInterval, Int: 600}}, Payload: []byte("a")},
		{QoS: 1, Topic: "inflight/b", Payload: []byte("b")},
		{QoS: 1, Topic: "inflight/c", Payload: []byte("c")},
		{QoS: 1, Topic: "inflight/d", Payload: []byte("d")},
		{QoS: 1, Topic: "inflight/e", Payload: []byte("e")},
	}
	support.PublishAll(t, pub, ps)
	offsets := make(map[string]int64)
	for _, r := range records(t, b.DB) {
		offsets[r.topic] = r.offset
	}
	// offset is the record offset of ps[i].
	offset := func(i int) int64 { return offsets[ps[i].Topic] }
	saved := string(propertiesOf(t, identifier(4)))

	first := support.DeliveredAt(t, sub, 1, ps[0].Topic, ps[0].Payload)
	if _, ok := first.Props.Get(harness.TopicAlias); !ok {
		t.Fatal("the first delivery carries no Topic Alias; want one, so that out_properties is seen to leave it out")
	}
	if _, ok := first.Props.Get(harness.MessageExpiryInterval); !ok {
		t.Fatal("the first delivery carries no Message Expiry Interval; want one, so that out_properties is seen to leave it out")
	}
	second := support.DeliveredAt(t, sub, 1, ps[1].Topic, ps[1].Payload)
	seq := expectInflight(t, db, client, []int64{offset(0), offset(1), offset(2)}, &inflightRow{packetID: int64(first.ID), channel: 1, offset: offset(0),
		qos: 1, phase: 1, moves: 1, outRetain: 1, outProperties: saved}, 0, "once the client has the first two deliveries")

	acknowledge(t, sub, first.ID, 0)
	third := support.DeliveredAt(t, sub, 1, ps[2].Topic, ps[2].Payload)
	seq = expectInflight(t, db, client, []int64{offset(1), offset(2), offset(3)}, &inflightRow{packetID: int64(third.ID), channel: 1, offset: offset(2),
		qos: 1, phase: 1, moves: 1, outProperties: saved}, seq, "once the client has acknowledged the first and has the third")

	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	acknowledge(t, sub, second.ID, 0)
	fourth := support.DeliveredAt(t, sub, 1, ps[3].Topic, ps[3].Payload)
	acknowledge(t, sub, third.ID, 0)
	acknowledge(t, sub, fourth.ID, 0)
	nothingWithin(t, sub, quiet, "on a full disk, after the PUBACKs that made room for the fifth delivery")
	taken(t, sub, "the fifth delivery went unsaved")

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	fifth := support.DeliveredAt(t, sub, 1, ps[4].Topic, ps[4].Payload)
	expectInflight(t, db, client, []int64{offset(4)}, &inflightRow{packetID: int64(fifth.ID), channel: 1, offset: offset(4),
		qos: 1, phase: 1, moves: 1, outProperties: saved}, seq, "once the ballast is emptied and the client has the fifth")
}
