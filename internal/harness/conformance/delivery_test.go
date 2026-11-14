package conformance

import (
	"bytes"
	"cmp"
	endian "encoding/binary" // binary names the broker under test (main_test.go)
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of QoS 0 delivery: committed $broadcast records reach the
// connected subscribers whose filter matches, the delivery's QoS being the
// lower of the publication's and the subscription's grant (design.md,
// Flows, SUBSCRIBE, One copy: "The delivery uses the lower of the
// publication's QoS and the highest contributing grant"), here always 0.

// delivered reads c's next packet and fails the test unless it is a
// PUBLISH at QoS 0 of topic and payload, which it returns.
func delivered(t *testing.T, c *harness.Client, topic string, payload []byte) *harness.Publish {
	t.Helper()
	return support.DeliveredAt(t, c, 0, topic, payload)
}

// expiry is the property section of an MQTT 5 PUBLISH holding one
// property, a Message Expiry Interval of seconds (MQTT 5 3.3.2.3.3).
func expiry(seconds uint32) harness.Properties {
	return harness.Properties{{ID: harness.MessageExpiryInterval, Int: seconds}}
}

// sameProperties reports how got, a delivery's properties, differ from
// want, the publication's, or "" where they do not: the User Properties
// the same, in the same order (MQTT 5 [MQTT-3.3.2-17], [MQTT-3.3.2-18]),
// every other property the same value, in any order, and nothing more.
func sameProperties(got, want harness.Properties) string {
	split := func(ps harness.Properties) (user, other harness.Properties) {
		for _, p := range ps {
			if p.ID == harness.UserProperty {
				user = append(user, p)
			} else {
				other = append(other, p)
			}
		}
		slices.SortStableFunc(other, func(a, b harness.Property) int { return cmp.Compare(a.ID, b.ID) })
		return user, other
	}
	gotUser, gotOther := split(got)
	wantUser, wantOther := split(want)
	switch {
	case !reflect.DeepEqual(gotUser, wantUser):
		return fmt.Sprintf("User Properties %v; want %v, unaltered and in order", gotUser, wantUser)
	case !reflect.DeepEqual(gotOther, wantOther):
		return fmt.Sprintf("properties %v; want %v", gotOther, wantOther)
	}
	return ""
}

// design.md, Sagüin properties: "Deliveries to clients of messages on
// $broadcast carry none of them. They receive only MQTT's own per-delivery
// changes to Message Expiry, RETAIN and Subscription Identifiers. The
// publisher's own User Properties go unaltered (MQTT-3.3.2-17)", "Nothing a publisher sends is altered", "MQTT
// 3.1.1 clients receive none"; Channels, $broadcast: "QoS decides only when a
// message enters, and caps the QoS it is delivered at"; "RETAIN is 1 on
// retained values sent to a new subscription. Otherwise it is 0 unless Retain
// As Published". MQTT 5 [MQTT-3.3.2-17], [MQTT-3.3.2-18]: the User Properties
// unaltered and in order; [MQTT-3.3.1-12]: with Retain As Published 0, "the
// Server MUST set the RETAIN flag to 0 when forwarding an Application Message
// regardless of how the RETAIN flag was set"; MQTT 3.1.1 [MQTT-3.3.1-9]:
// RETAIN 0 "when a PUBLISH Packet is sent to a Client because it matches an
// established subscription". MQTT 5 3.3.3: "The Payload contains the
// Application Message that is being published".
//
// An MQTT 5 client subscribed at QoS 1 and an MQTT 3.1.1 client at QoS 0,
// then a publisher sends a retained message at QoS 0 carrying every
// property a publisher may set but Message Expiry and Topic Alias, User
// Properties repeated under one name among them. Each subscriber receives
// it at QoS 0, RETAIN 0, on its topic with its payload; the MQTT 5 one with
// exactly the publisher's properties, no saguin- property among them; the
// MQTT 3.1.1 one, whose PUBLISH has no property section, with the payload
// alone after the Topic Name.
func TestDeliveryCarriesTheMessageUnchanged(t *testing.T) {
	topic := clientID(t, "/room/1")
	v5 := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-5"), CleanStart: true}, support.At(1, topic))
	v311 := support.Subscribed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, "-311"), CleanStart: true}, support.At(0, topic))
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	props := harness.Properties{
		harness.User("unit", "celsius"),
		{ID: harness.PayloadFormatIndicator, Int: 1},
		{ID: harness.ContentType, Str: "text/plain"},
		harness.User("source", "probe-2"),
		{ID: harness.ResponseTopic, Str: "replies/room/1"},
		{ID: harness.CorrelationData, Data: []byte{0x00, 0xFF, 0x10}},
		harness.User("unit", "kelvin"),
	}
	payload := []byte("21.5")
	if err := pub.Send(&harness.Publish{Retain: true, Topic: topic, Props: props, Payload: payload}); err != nil {
		t.Fatal(err)
	}

	got := delivered(t, v5, topic, payload)
	if got.Retain {
		t.Error("MQTT 5: RETAIN 1 on a live delivery to a subscription without Retain As Published")
	}
	if diff := sameProperties(got.Props, props); diff != "" {
		t.Errorf("MQTT 5: the delivery carries %s", diff)
	}
	if got := delivered(t, v311, topic, payload); got.Retain {
		t.Error("MQTT 3.1.1: RETAIN 1 on a live delivery to an established subscription")
	}
}

// design.md, Flows, SUBSCRIBE, The start point: "The core captures that
// channel's tail when it accepts the subscription", the boundary separating
// "subsequent live records"; the subscription table, $broadcast: "the
// matching retained values, as Retain Handling allows, then the next
// message". Delivery, Order: "a channel's offset order on $broadcast"; R5: "A
// consumer reads only stored records, from its own position". Words: Offset,
// "an offset is a channel's counter. The core assigns it".
//
// A message is stored under the filter before the SUBSCRIBE, not retained.
// Then one publisher sends four messages back to back, alternating two
// topics and QoS 1 and 0. The subscriber receives exactly those four, in
// the order sent, which is their offsets' order: none from before its
// SUBSCRIBE, and no topic's messages ahead of another's.
func TestDeliveryStartsAfterTheSubscribe(t *testing.T) {
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: prefix + "/a", Payload: []byte("before")})
	sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, prefix+"/#"))

	ps := []*harness.Publish{
		{QoS: 1, Topic: prefix + "/a", Payload: []byte("1")},
		{QoS: 0, Topic: prefix + "/b", Payload: []byte("2")},
		{QoS: 0, Topic: prefix + "/a", Payload: []byte("3")},
		{QoS: 1, Topic: prefix + "/b", Payload: []byte("4")},
	}
	support.PublishAll(t, pub, ps)
	for _, p := range ps {
		delivered(t, sub, p.Topic, p.Payload)
	}
}

// fillerPayload is the payload of the messages that fill a subscriber's
// connection: with its PUBLISH's head it stays under config.md's default
// max_packet_size, 1 MiB.
const fillerPayload = 1<<20 - 256

// fillers is how many messages of fillerPayload more than fill one TCP
// connection: Linux's largest send buffer by default, net.ipv4.tcp_wmem's
// 4 MiB, on the broker's side, and the subscriber's receive buffer.
const fillers = 6

// design.md, Delivery, Slow clients: "A slow client fills only its own
// writer, and the core stops sending it deliveries". R5: "Publishers never
// wait for consumers [...] a slow consumer, or none, only falls behind".
// R2: "When work backs up, the clients slow, never the core." design.md,
// Testing: "A watchdog bounds the test and is reported separately; it does
// not establish a broker timeout"; "TCP-load tests use an overall watchdog
// and do not infer a broker write's start from a stalled client write".
// That a writer is held in its write is shown inside
// (internal/connection, TestStuckWriterHoldsBackOnlyItsConnection); here,
// what the wire shows.
//
// write_timeout is an hour, so no deadline of the broker's falls within
// the test; every wait is the harness client's, the watchdog. A
// subscriber that reads nothing and another subscribe to one filter. A
// publisher sends fillers messages of fillerPayload and then a small one,
// at QoS 1: each is acknowledged and read by the other subscriber while
// the first reads nothing, and a new client connects and is answered
// PINGRESP. Then the first subscriber reads, and receives every message,
// in order.
func TestSlowSubscriberHoldsBackOnlyItself(t *testing.T) {
	filter := "slow/#"
	b := support.Launch(t, &harness.Options{WriteTimeout: time.Hour})
	idle := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-idle"), CleanStart: true}, support.At(0, filter))
	other := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true}, support.At(0, filter))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	ps := make([]*harness.Publish, 0, fillers+1)
	for i := range fillers {
		ps = append(ps, &harness.Publish{QoS: 1, Topic: fmt.Sprintf("slow/big/%d", i), Payload: bytes.Repeat([]byte{byte('a' + i)}, fillerPayload)})
	}
	ps = append(ps, &harness.Publish{QoS: 1, Topic: "slow/small", Payload: []byte("x")})
	for _, p := range ps {
		support.Acknowledged(t, pub, p)
		delivered(t, other, p.Topic, p.Payload)
	}
	later, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-later"), CleanStart: true})
	if err := later.Ping(); err != nil {
		t.Fatalf("watchdog: a new client, while a subscriber reads nothing: %v", err)
	}

	for _, p := range ps {
		delivered(t, idle, p.Topic, p.Payload)
	}
}

// design.md, Store, Requests: "a request waiting for its predecessor holds
// back only its own owner's later requests"; Writes: "A write awaiting its
// owner's earlier request or another required predecessor stays pending;
// it does not hold back unrelated eligible writes."; Delivery, Slow
// clients: "A slow client fills only its own writer". Closing a
// connection: "A write that has not finished limits.write_timeout after
// its first socket-write attempt closes the connection"; "The writer
// writes the last packet, ends its sending". DISCONNECT, A lost
// connection: "The session goes away until its expiry, keeping its
// in-flight messages". R2: "When work backs up, the clients slow, never
// the core."
//
// write_timeout is an hour, so the stuck writer stays stuck for the whole
// test. A client holding a durable session subscribes at QoS 1 on a
// connection that takes nothing (stuckClient), and a publisher stores
// fillers messages of fillerPayload, each acknowledged: more than the
// connection's buffers hold, so the broker's side of its socket holds
// bytes the client will not take, and the broker's writer is left in a
// write it cannot finish. The client ends what it sends: its end of
// stream ends its reader, and its session's departure waits for that
// writer. Once the broker's side has that end of stream and the core has
// taken the reader's end, saguin_connections{protocol="5"} counting the
// publisher alone (metrics.md: "the sessions the core holds connected";
// 0020a.md, 261026 212930: no client can force that order, so the test
// waits for the operator's view of it), a new client connects with a
// durable session of its own: its CONNACK comes within
// the harness's wait, and the broker's side of the stuck connection still
// has its sending open and bytes it cannot send. The stuck client then
// reads until the broker's close, and receives more than the connection
// held when the CONNACK came: the writer was still in its write.
func TestStuckDepartureHoldsBackOnlyItself(t *testing.T) {
	b := support.Launch(t, &harness.Options{WriteTimeout: time.Hour, Operations: true})
	stuck, _ := stuckSubscriber(t, b, &harness.Connect{ClientID: clientID(t, "-stuck"), CleanStart: true, Props: support.Durable})
	brokerSide := func() tcpSocket { return brokerSideOf(t, stuck) }
	held := func() int64 { return heldBytes(t, stuck) }

	if err := stuck.CloseWrite(); err != nil {
		t.Fatalf("ending what the stuck client sends: %v", err)
	}
	if !testkit.Within(harness.DefaultTimeout, func() bool { return brokerSide().state == tcpCloseWait }) {
		t.Fatalf("watchdog: the broker's side has not had the stuck client's end of stream %v after it", harness.DefaultTimeout)
	}
	awaitSeries(t, b, map[string]float64{connected5: 1})
	other := touch(t, b, clientID(t, "-other"))
	p, err := other.Recv()
	if err != nil {
		t.Fatalf("a new client's CONNECT, while another's departure waits for its stuck writer: no CONNACK within %v: %v", harness.DefaultTimeout, err)
	}
	if ack, ok := p.(*harness.Connack); !ok || ack.Reason != 0 {
		t.Fatalf("a new client's CONNECT: %s %+v, want CONNACK 0x00", harness.Name(p.Type()), p)
	}
	side := brokerSide()
	if side.state != tcpCloseWait || side.sendQueue == 0 {
		t.Fatalf("when the CONNACK came, the broker's side of the stuck connection was in TCP state %d holding %d unsent bytes; want state %d (its sending still open) and bytes it cannot send",
			side.state, side.sendQueue, tcpCloseWait)
	}
	before := held()

	drainable(t, stuck)
	if err := stuck.Conn.SetReadDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
		t.Fatal(err)
	}
	read, err := io.Copy(io.Discard, stuck.Conn)
	switch {
	case harness.IsTimeout(err):
		t.Fatalf("watchdog: the stuck connection is still open %v after its client began reading", harness.DefaultTimeout)
	case err != nil && !errors.Is(err, syscall.ECONNRESET):
		t.Fatalf("reading the stuck connection: %v", err)
	}
	if read <= before {
		t.Errorf("the stuck client read %d bytes until the close, no more than the %d its connection held when the CONNACK came: its writer was no longer in a write", read, before)
	}
}

// design.md, Flows, CONNECT, step 5: "The same holds for an old connection
// that has already ended: if its departure still waits for its writer's
// delivery reports 500 ms after the CONNECT, its socket is cut. It keeps
// its own closure cause"; step 9: "The broker reads in-flight messages and
// PUBRELs from the store and resends them with their packet identifiers
// and in their order [...] New application deliveries wait behind
// recovered PUBLISHes"; DISCONNECT, A lost connection: "A keep-alive
// timeout closes the connection with 0x8D". metrics.md,
// saguin_disconnects_total: "the reason Sagüin selected for a closure,
// including a closure without a sendable DISCONNECT [...]:
// keep_alive_timeout, session_taken_over". Delivery, Order: deliveries
// "follow a channel's offset order on $broadcast".
//
// write_timeout is an hour, so the stuck writer's own deadline never comes
// within the test. A client holding a durable session, with a Keep Alive
// of 2 seconds, subscribes at QoS 1 on a connection that takes nothing,
// and fillers messages leave the broker's writer in a write it cannot
// finish (stuckSubscriber), while the core still holds the client
// connected. The client sends nothing more, so its keep-alive ends the
// connection; once the core has taken that end
// (saguin_connections{protocol="5"} counting the publisher alone, as in
// TestStuckDepartureHoldsBackOnlyItself), the same client id connects
// again with Clean Start 0, on a connection that reads: its CONNACK is
// 0x00, Session Present 1, no sooner than 500 ms after its CONNECT, when
// the old socket is cut, and within twice that (tookOverSince). The new
// connection then receives every message, in order, each once and
// acknowledged, a PINGREQ answered after them; and the old connection's
// closure is counted under keep_alive_timeout, not session_taken_over.
func TestReconnectCutsADepartureHeldByItsStuckWriter(t *testing.T) {
	b := support.Launch(t, &harness.Options{WriteTimeout: time.Hour, Operations: true})
	before := baselineClosures(t, b)
	keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
	first := *keep
	first.CleanStart, first.KeepAlive = true, 2
	_, owed := stuckSubscriber(t, b, &first)
	if got := support.Sampled(t, b, connected5); got[connected5] != 2 {
		t.Fatalf("once the writer is stuck, /metrics reads %v; want the stuck client and the publisher connected: its keep-alive ended it too soon", got)
	}

	awaitSeries(t, b, map[string]float64{connected5: 1})
	newer, sent := takingOver(t, b.Addr, keep)
	tookOverSince(t, newer, sent, true)
	for _, p := range owed {
		arrived(t, newer, p.Topic, 1, p.Payload, "on the new connection")
	}
	if err := newer.Send(&harness.Pingreq{}); err != nil {
		t.Fatal(err)
	}
	harness.Expect[*harness.Pingresp](t, newer)
	if got, want := closuresSince(t, b, before), map[string]float64{disconnectSeries("keep_alive_timeout"): 1}; !maps.Equal(got, want) {
		t.Errorf("the old connection, ended by its keep-alive and then cut, is counted as %v; want %v, its own closure cause", got, want)
	}
}

// design.md, Flows, CONNECT, step 5: "The same holds for an old connection
// that has already ended: if its departure still waits for its writer's
// delivery reports 500 ms after the CONNECT, its socket is cut"; DISCONNECT,
// A clean DISCONNECT: "The client may change its session expiry [...] The
// change is stored before the connection closes"; Delivery, Positions: "A
// QoS 2 delivery stores its PUBREC before its PUBREL goes to the writer".
// A departure that waits for the store, and no writer report, is not one
// the cut applies to.
//
// An MQTT 5 client holding a durable session subscribes at QoS 2, a
// publisher connected beside it. The test holds the database's write lock,
// as another program on the file may, so the store can save nothing; the
// broker runs under strace (launchTracingLocks):
//   - its change alone at the store: the client sends DISCONNECT with a
//     Session Expiry Interval of 60, and its store writer meets the lock;
//   - behind a QoS 2 phase save: the client receives a QoS 2 delivery,
//     every byte of it written, sends its PUBREC, whose phase save meets the
//     lock, then that DISCONNECT.
//
// Once the core has taken the DISCONNECT (saguin_connections{protocol="5"}
// counting the publisher alone), the same client id connects again with
// Clean Start 0. Twice 500 ms after that CONNECT, the old connection is
// still open, nothing read on it, and the stored expiry is the CONNECT's,
// 3600. Once the lock is released, the old connection closes in order and
// the new one gets CONNACK 0x00, Session Present 1. (The reconnect stores
// its own expiry, so the DISCONNECT's no longer shows in the row after it.)
func TestReconnectLeavesADisconnectAtTheStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		qos2 bool
	}{{"its change alone at the store", false}, {"behind a QoS 2 phase save", true}} {
		t.Run(tc.name, func(t *testing.T) {
			b := launchTracingLocks(t, &harness.Options{WriteTimeout: time.Hour, Operations: true})
			topic := clientID(t, "/phase")
			keep := &harness.Connect{ClientID: clientID(t, ""), Props: support.Durable}
			first := *keep
			first.CleanStart = true
			old := support.Subscribed(t, b.Addr, harness.V5, &first, support.At(2, topic))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			var delivery *harness.Publish
			if tc.qos2 {
				published(t, pub, twice(topic, "x"))
				delivery = support.DeliveredAt(t, old, 2, topic, []byte("x"))
			}

			lock := holdWriteLock(t, b.DB)
			before := len(traced(t, b.trace))
			if tc.qos2 {
				pubrecFor(t, old, delivery.ID, 0x00)
				awaitLockMet(t, b.trace, before, "the PUBREC")
			}
			if err := old.Send(&harness.Disconnect{Props: support.SessionExpiry(60)}); err != nil {
				t.Fatal(err)
			}
			if !tc.qos2 {
				awaitLockMet(t, b.trace, before, "the DISCONNECT")
			}
			awaitSeries(t, b.Broker, map[string]float64{connected5: 1})
			newer, _ := takingOver(t, b.Addr, keep)

			nothingWithin(t, old, takeoverBound, "the DISCONNECT's changed expiry at the store, after the same id's CONNECT")
			if got := storedExpiry(t, lock.conn, keep.ClientID); got != support.Durable[0].Int {
				t.Fatalf("with the write lock held, the stored expiry is %d; want the CONNECT's, %d", got, support.Durable[0].Int)
			}
			lock.release(t)
			harness.ExpectClosed(t, old)
			if ack := harness.Expect[*harness.Connack](t, newer); ack.Reason != 0 || !ack.SessionPresent {
				t.Errorf("the reconnect: CONNACK 0x%02X, Session Present %t; want 0x00, Session Present 1", ack.Reason, ack.SessionPresent)
			}
		})
	}
}

// stuckSubscriber connects p, an MQTT 5 CONNECT, to b on a connection that
// takes nothing (stuckClient), subscribed at QoS 1 to stuck/#, and has a
// publisher of its own store fillers messages of fillerPayload, stuck/0 up,
// at QoS 1, each acknowledged; it returns the client and those messages, in
// order. That is more than the connection's buffers hold, so the broker's
// side of its socket holds bytes the client will not take, and the
// broker's writer is left in a write it cannot finish: the test fails
// unless, within harness.DefaultTimeout, the broker's side holds unsent
// bytes, and the connection holds less than was owed. The publisher stays
// connected until the test ends.
func stuckSubscriber(t *testing.T, b *harness.Broker, p *harness.Connect) (*harness.Client, []*harness.Publish) {
	t.Helper()
	stuck := stuckClient(t, b.Addr, harness.V5)
	if _, err := stuck.Connect(p); err != nil {
		t.Fatal(err)
	}
	support.Suback(t, stuck, nil, support.At(1, "stuck/#"), 1)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	owed := make([]*harness.Publish, fillers)
	for i := range owed {
		owed[i] = &harness.Publish{QoS: 1, Topic: fmt.Sprintf("stuck/%d", i), Payload: bytes.Repeat([]byte{byte('a' + i)}, fillerPayload)}
		support.Acknowledged(t, pub, owed[i])
	}
	if !testkit.Within(harness.DefaultTimeout, func() bool { return brokerSideOf(t, stuck).sendQueue > 0 }) {
		t.Fatalf("watchdog: the broker's side of the stuck connection holds no unsent byte %v after the last PUBACK", harness.DefaultTimeout)
	}
	if held, all := heldBytes(t, stuck), int64(fillers*fillerPayload); held >= all {
		t.Fatalf("the stuck connection holds %d bytes, all of the %d owed: no write is left unfinished", held, all)
	}
	return stuck, owed
}

// brokerSideOf is the broker's side of c's TCP connection (tcpSocketOf).
func brokerSideOf(t *testing.T, c *harness.Client) tcpSocket {
	t.Helper()
	return tcpSocketOf(t, c.Conn.RemoteAddr(), c.Conn.LocalAddr())
}

// heldBytes is how many bytes c's TCP connection holds between the broker
// and c: those the broker's side has sent and c's has not acknowledged, and
// those c's side has received and c has not read.
func heldBytes(t *testing.T, c *harness.Client) int64 {
	t.Helper()
	return brokerSideOf(t, c).sendQueue + tcpSocketOf(t, c.Conn.LocalAddr(), c.Conn.RemoteAddr()).receiveQueue
}

// tcpCloseWait is the TCP state CLOSE_WAIT as /proc/net/tcp numbers it:
// the peer's end of stream has come, and this side's sending is still open.
const tcpCloseWait = 0x08

// tcpSocket is what the system shows of one TCP socket, as ss shows it:
// its state; its send queue, the bytes it has sent that its peer has not
// acknowledged, which on loopback are bytes its peer's full receive
// buffer will not take; and its receive queue, the bytes that have come
// that its owner has not read.
type tcpSocket struct {
	state, sendQueue, receiveQueue int64
}

// tcpSocketOf returns the IPv4 socket whose own address is local and whose
// peer's is remote, from /proc/net/tcp, failing the test where none is
// listed. An address there is its four bytes read as one number in the
// machine's byte order, and a port its number, both in hexadecimal; a line
// holds, after its number, the two addresses, the state, and the send and
// receive queues joined by a colon.
func tcpSocketOf(t *testing.T, local, remote net.Addr) tcpSocket {
	t.Helper()
	listed := func(a net.Addr) string {
		ap := a.(*net.TCPAddr).AddrPort()
		ip := ap.Addr().Unmap().As4()
		return fmt.Sprintf("%08X:%04X", endian.NativeEndian.Uint32(ip[:]), ap.Port())
	}
	table, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		t.Fatal(err)
	}
	from, to := listed(local), listed(remote)
	for line := range strings.Lines(string(table)) {
		f := strings.Fields(line)
		if len(f) < 5 || f[1] != from || f[2] != to {
			continue
		}
		send, receive, _ := strings.Cut(f[4], ":")
		var s tcpSocket
		for _, v := range []struct {
			field string
			n     *int64
		}{{f[3], &s.state}, {send, &s.sendQueue}, {receive, &s.receiveQueue}} {
			if *v.n, err = strconv.ParseInt(v.field, 16, 64); err != nil {
				t.Fatalf("/proc/net/tcp: %q: %v", line, err)
			}
		}
		return s
	}
	t.Fatalf("/proc/net/tcp lists no socket from %v to %v", local, remote)
	return tcpSocket{}
}

// topicAlias returns p's Topic Alias, 0 where it has none.
func topicAlias(p *harness.Publish) uint32 {
	a, _ := p.Props.Get(harness.TopicAlias)
	return a.Int
}

// design.md, Delivery, Sending: "Outgoing aliases use the peer's
// advertised limit. The lowest unused alias is assigned on first use; a
// full alias table leaves new topics unaliased." Execution, State
// ownership: "outgoing topic aliases" are the writer's. MQTT 5
// [MQTT-3.1.2-26]: "The Server MUST NOT send a Topic Alias in a PUBLISH
// packet to the Client greater than Topic Alias Maximum"; [MQTT-3.1.2-27]:
// "If Topic Alias Maximum is absent or zero, the Server MUST NOT send any
// Topic Aliases to the Client"; 3.3.2.3.4: a sender "sets a Topic Alias
// mapping by including a non-zero length Topic Name and a Topic Alias",
// and "If a Topic Alias mapping has been set at the receiver, a sender can
// send a PUBLISH packet that contains that Topic Alias and a zero length
// Topic Name".
//
// One publisher sends seven messages at QoS 1 on four topics, A, B, A, C,
// B, D, A, to subscribers whose CONNECT sent no Topic Alias Maximum, a
// Topic Alias Maximum of 0, and one of 2; the harness client refuses an
// alias over its maximum, any where it is 0, and an empty Topic Name whose
// alias maps nothing. Every subscriber receives the seven in order, each
// on its topic. Without a maximum, or with 0, none carries an alias. With
// 2, A takes alias 1 and B alias 2, each sent with its Topic Name the
// first time and by alias alone, its Topic Name empty, after that; C and D
// find the table full and go unaliased, with their Topic Names.
func TestOutgoingTopicAliases(t *testing.T) {
	prefix := clientID(t, "")
	topics := []string{prefix + "/A", prefix + "/B", prefix + "/A", prefix + "/C", prefix + "/B", prefix + "/D", prefix + "/A"}
	for _, tc := range []struct {
		name    string
		props   harness.Properties // the subscriber's CONNECT's
		aliases []uint32           // each delivery's Topic Alias, 0 for none
		byAlias []bool             // each delivery's Topic Name sent empty
	}{
		{"no Topic Alias Maximum", nil, make([]uint32, len(topics)), make([]bool, len(topics))},
		{"Topic Alias Maximum 0", harness.Properties{{ID: harness.TopicAliasMaximum, Int: 0}},
			make([]uint32, len(topics)), make([]bool, len(topics))},
		{"Topic Alias Maximum 2", harness.Properties{{ID: harness.TopicAliasMaximum, Int: 2}},
			[]uint32{1, 2, 1, 0, 2, 0, 1}, []bool{false, false, true, false, true, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: tc.props}, support.At(0, prefix+"/#"))
			pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			for i, topic := range topics {
				payload := []byte{byte('1' + i)}
				support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: topic, Payload: payload})
				got := delivered(t, sub, topic, payload)
				if alias := topicAlias(got); alias != tc.aliases[i] || got.ByAlias != tc.byAlias[i] {
					t.Errorf("delivery %d, of %q: Topic Alias %d, Topic Name sent empty %v; want %d and %v",
						i+1, topic, alias, got.ByAlias, tc.aliases[i], tc.byAlias[i])
				}
			}
		})
	}
}

// design.md, Delivery, Message Expiry: "Message Expiry is worked out when the
// message is written and sent as the time remaining. On $broadcast, an
// expired message is skipped for that subscriber and counted. It stays in the
// log for retention"; Flows, PUBLISH: "Message Expiry and publish-rate
// accounting use receipt time". MQTT 5 [MQTT-3.3.2-5]: "If the Message Expiry
// Interval has passed and the Server has not managed to start onward delivery
// to a matching subscriber, then it MUST delete the copy of the message for
// that subscriber"; [MQTT-3.3.2-6]: "The PUBLISH packet sent to a Client by
// the Server MUST contain a Message Expiry Interval set to the received value
// minus the time that the Application Message has been waiting in the
// Server".
//
// A subscriber that reads nothing for a while: fillers messages of
// fillerPayload hold its connection, so what follows them waits in the
// broker. A publisher sends them, then a message expiring in 1 second, one
// expiring in 60 and one with no expiry, each acknowledged; write_timeout
// is long enough that the waiting subscriber is not closed. Two seconds
// after the last PUBACK the subscriber reads: the fillers; then, the first
// expired before it could be written, the second, its Message Expiry
// Interval at most 58, the 60 less the two seconds at least it waited, and
// no less than 60 less the whole seconds from its sending to its reading;
// then the third, with no Message Expiry Interval.
func TestMessageExpiryIsTheTimeRemaining(t *testing.T) {
	const lasting = 60
	const wait = 2 * time.Second
	b := support.Launch(t, &harness.Options{WriteTimeout: 10 * wait})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, "expiry/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	big := bytes.Repeat([]byte("f"), fillerPayload)
	for range fillers {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "expiry/filler", Payload: big})
	}
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "expiry/short", Props: expiry(1), Payload: []byte("short")})
	sentAt := time.Now()
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "expiry/long", Props: expiry(lasting), Payload: []byte("long")})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "expiry/none", Payload: []byte("none")})
	time.Sleep(wait) // the messages wait in the broker: the behaviour under test

	for range fillers {
		delivered(t, sub, "expiry/filler", big)
	}
	long := delivered(t, sub, "expiry/long", []byte("long"))
	elapsed := uint32(math.Ceil(time.Since(sentAt).Seconds()))
	switch remaining, ok := long.Props.Get(harness.MessageExpiryInterval); {
	case !ok:
		t.Errorf("the message expiring in %d seconds came with no Message Expiry Interval", lasting)
	case remaining.Int > lasting-uint32(wait/time.Second) || remaining.Int < lasting-elapsed:
		t.Errorf("Message Expiry Interval %d after waiting at least %v and at most %d seconds; want %d to %d",
			remaining.Int, wait, elapsed, lasting-elapsed, lasting-uint32(wait/time.Second))
	}
	if none := delivered(t, sub, "expiry/none", []byte("none")); len(none.Props) != 0 {
		t.Errorf("the message with no expiry came with properties %v; want none", none.Props)
	}
}

// design.md, Delivery: "A message over the client's Maximum Packet Size is
// discarded for that subscriber as if sent, counted as missed and logged
// (MQTT-3.1.2-25)". MQTT 5 [MQTT-3.1.2-24]: "The Server MUST NOT send
// packets exceeding Maximum Packet Size to the Client"; [MQTT-3.1.2-25]:
// "Where a Packet is too large to send, the Server MUST discard it without
// sending it and then behave as if it had completed sending that
// Application Message". design.md, Testing: the operator sees the broker
// through "the log".
//
// A subscriber whose CONNECT sent a Maximum Packet Size of 256, which the
// harness client holds the broker to, and another with none subscribe to
// one filter. A publisher sends a small message, one whose PUBLISH is over
// 256 bytes, and another small one, each acknowledged. The first
// subscriber receives the two small ones, in order; the other all three.
// After the stop, the broker's log holds a line naming the first
// subscriber, at a level the default log_level, info, writes.
func TestMessageOverTheClientsMaximumPacketSizeIsSkipped(t *testing.T) {
	const limit = 256
	b := support.Launch(t, &harness.Options{})
	smallID := clientID(t, "-small")
	small := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: smallID, CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: limit}}}, support.At(0, "size/#"))
	other := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true}, support.At(0, "size/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	ps := []*harness.Publish{
		{QoS: 1, Topic: "size/first", Payload: []byte("first")},
		{QoS: 1, Topic: "size/big", Payload: bytes.Repeat([]byte("b"), limit)},
		{QoS: 1, Topic: "size/last", Payload: []byte("last")},
	}
	for _, p := range ps {
		support.Acknowledged(t, pub, p)
	}
	for _, p := range ps {
		delivered(t, other, p.Topic, p.Payload)
	}
	delivered(t, small, ps[0].Topic, ps[0].Payload)
	delivered(t, small, ps[2].Topic, ps[2].Payload)
	stopped(t, b)

	loggedNaming(t, b, smallID, "whose message was discarded as over its Maximum Packet Size")
}

// loggedNaming fails the test unless the log of b, stopped, holds a line
// naming name, a client id or an error, at a level the default log_level,
// info, writes (design.md, Delivery: a discarded delivery is "logged");
// why says what befell it.
func loggedNaming(t *testing.T, b *harness.Broker, name, why string) {
	t.Helper()
	log := b.Log()
	if !slices.ContainsFunc([]string{"INFO", "WARN", "ERROR"}, func(level string) bool {
		return len(testkit.LinesNaming(log, level, name)) > 0
	}) {
		t.Errorf("no log line names %q, %s; the log:\n%s", name, why, log)
	}
}

// leastIncoming is the least broker.limits.max_incoming_memory a
// max_packet_size of packet allows where no bridge has an in rule:
// config.md, max_incoming_memory, "It must hold the largest body one reader
// can ask for: max_packet_size, plus the largest S2 copy of it where
// max_packet_size is over 4 KiB", that copy being s2.MaxEncodedLen of it,
// the library storage.md, A stored payload, names.
func leastIncoming(packet int) int {
	if packet <= 4<<10 {
		return packet
	}
	return packet + s2.MaxEncodedLen(packet)
}

// tinyPacket is the broker.limits.max_packet_size of
// TestTinyBudgetsStillDeliver, whose max_incoming_memory is leastIncoming
// of it: its messages are over 4 KiB, so each body asks for room for its S2
// copy too (design.md, Message memory, Asking).
const tinyPacket = 8 << 10

// design.md, Message memory: "the incoming budget,
// limits.max_incoming_memory, holds PUBLISH bodies from the start of their
// reading until they are stored or refused"; "the outgoing budget,
// limits.max_outgoing_memory, holds deliveries queued to writers"; Two
// queues: "A request receives its whole reservation or waits"; "An outgoing
// delivery reserves its encoded packet size before loading its payload.
// [...] A delivery of a compressed payload also reserves the stored copy's
// size. That reservation is given back once the store reader has
// decompressed it."; Asking: "A body over 4 KiB also asks for room for the
// largest copy S2 can make of it [...] Once compressed, the body keeps only
// the room of the copy it stores, and the rest goes back"; Bytes back: "A
// delivery reservation ends when the writer reports completion or
// discard". R6: "Every resource the broker holds has a limit".
//
// With max_packet_size tinyPacket, max_incoming_memory at its least for it
// and max_outgoing_memory the largest one delivery reserves, a message of
// 8,190 bytes, its payload a compressible 8,178 (a QoS 1 PUBLISH of exactly
// tinyPacket), leaves no room for a second in either budget: its body asks
// for all of max_incoming_memory but 11 bytes, and each of its deliveries,
// the same 8,190 bytes and the stored copy, for more than half of
// max_outgoing_memory, so no other body or delivery fits beside one. Three
// subscribers read as messages come while a publisher sends 100 such
// messages at QoS 0 back to back, then one at QoS 1. The publisher gets its
// PUBACK, and each subscriber receives all 101, in order; every wait is
// bounded by the harness client's.
//
// The payloads are over 4 KiB, so they are stored compressed: only the S2
// room a body asks lets max_incoming_memory's least hold a single body.
func TestTinyBudgetsStillDeliver(t *testing.T) {
	const messages, subscribers = 101, 3
	// A fixed header of 3 (a Remaining Length of two bytes), the Topic
	// Name's 2+6, the Packet Identifier's 2 at QoS 1, an empty property
	// section's 1, and the payload.
	const payloadSize = tinyPacket - 3 - 8 - 2 - 1
	payload := func(i int) []byte {
		return fmt.Appendf(nil, "%03d%s", i, strings.Repeat(".", payloadSize-3))
	}
	if n := len(harness.Encoded(t, &harness.Publish{QoS: 1, ID: 1, Topic: "tiny/m", Payload: payload(0)}, harness.V5)); n != tinyPacket {
		t.Fatalf("the PUBLISH at QoS 1 is %d bytes, want max_packet_size, %d", n, tinyPacket)
	}
	// Each delivery at QoS 0 is the 8,190 bytes of the PUBLISH at QoS 0
	// with the stored copy beside them: the largest of them is the
	// outgoing budget, and two of the smallest must not fit in it.
	least, most := math.MaxInt, 0
	for m := range messages {
		delivery := tinyPacket - 2 + shrinks(t, payload(m))
		least, most = min(least, delivery), max(most, delivery)
	}
	if 2*least <= most {
		t.Fatalf("two deliveries of %d bytes fit in max_outgoing_memory's %d; the test needs room for one", least, most)
	}
	b := support.Launch(t, &harness.Options{MaxPacketSize: tinyPacket, MaxIncomingMemory: leastIncoming(tinyPacket), MaxOutgoingMemory: most})
	errs := make(chan error, subscribers)
	for i := range subscribers {
		sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, fmt.Sprint("-sub", i)), CleanStart: true}, support.At(0, "tiny/#"))
		go func() {
			for m := range messages {
				p, err := sub.Recv()
				if err != nil {
					errs <- fmt.Errorf("subscriber %d, waiting for message %d: %w", i+1, m+1, err)
					return
				}
				if got, ok := p.(*harness.Publish); !ok || got.QoS != 0 || !bytes.Equal(got.Payload, payload(m)) {
					errs <- fmt.Errorf("subscriber %d, message %d: read %s; want the PUBLISH at QoS 0 of %.10q", i+1, m+1, harness.Name(p.Type()), payload(m))
					return
				}
			}
			errs <- nil
		}()
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := make([]*harness.Publish, messages)
	for m := range messages {
		ps[m] = &harness.Publish{Topic: "tiny/m", Payload: payload(m)}
	}
	ps[messages-1].QoS = 1 // its PUBACK follows every QoS 0 message before it
	support.PublishAll(t, pub, ps)
	for range subscribers {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// identifiedSubscriber connects an MQTT 5 client of its own to addr with
// p, and subscribes it to each of filters at qos in a SUBSCRIBE of its own,
// the i-th carrying Subscription Identifier ids[i], failing the test
// unless each is granted qos. A record every filter matches reaches it
// once, carrying every identifier (design.md, Flows, SUBSCRIBE, One copy:
// "Subscription Identifiers appear in complete filter-byte order, once for
// each contributing subscription having one").
func identifiedSubscriber(t *testing.T, addr string, p *harness.Connect, qos byte, filters []string, ids []uint32) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, addr, harness.V5, p)
	for i, f := range filters {
		support.Suback(t, c, identifier(ids[i]), support.At(qos, f), qos)
	}
	return c
}

// identifiers is the property section of a delivery carrying each of ids
// as a Subscription Identifier (MQTT 5 3.3.2.3.8), for its size.
func identifiers(ids []uint32) harness.Properties {
	props := make(harness.Properties, len(ids))
	for i, id := range ids {
		props[i] = harness.Property{ID: harness.SubscriptionIdentifier, Int: id}
	}
	return props
}

// longID is the least Subscription Identifier a Variable Byte Integer
// writes in its largest form, 4 bytes (MQTT 5 1.5.5): with the property's
// identifier byte, each such Subscription Identifier adds 5 bytes to a
// delivery.
const longID = 1 << 21

// preparedPacket is the broker.limits.max_packet_size of
// TestPreparedDeliveryHoldsNoMemory.
const preparedPacket = 512

// design.md, Delivery, Preparation: "each consumer's live scan may select
// records ahead of the client's free window. It may keep at most one window
// of selected deliveries not yet handed to the writer"; "A selected delivery
// reserves message memory only when send capacity is free for it"; Message
// memory: "the outgoing budget, limits.max_outgoing_memory, holds
// deliveries queued to writers"; "An outgoing delivery reserves its encoded
// packet size before loading its payload"; Two queues: "A request receives
// its whole reservation or waits". R5: a publication "is acknowledged after
// its channel stores it".
//
// With max_packet_size preparedPacket and max_outgoing_memory the size of
// one delivery, a client S with Receive Maximum 1 and a client O subscribe
// at QoS 1 to one topic. Each message is a QoS 1 PUBLISH of exactly
// preparedPacket bytes on $broadcast, and its delivery to either client the
// same size: two deliveries do not fit together. A publisher sends a first
// message, acknowledged: S receives it and holds it unacknowledged, its
// window full, and O receives and acknowledges it. The publisher sends a
// second, acknowledged, which S's consumer may select but not read into
// memory: O receives it, its delivery given the outgoing budget while S's
// waits for S's window. S's PUBACK then brings the second to S.
func TestPreparedDeliveryHoldsNoMemory(t *testing.T) {
	const topic = "prepared/m"
	// A fixed header of 3 (a Remaining Length of two bytes), the Topic
	// Name's 2+10, the Packet Identifier's 2, an empty property section's
	// 1, and the payload.
	ps := make([]*harness.Publish, 2)
	for i := range ps {
		ps[i] = &harness.Publish{QoS: 1, ID: 1, Topic: topic, Payload: bytes.Repeat([]byte{byte('1' + i)}, preparedPacket-3-12-2-1)}
	}
	if n := len(harness.Encoded(t, ps[0], harness.V5)); n != preparedPacket {
		t.Fatalf("each PUBLISH is %d bytes, want max_packet_size, %d", n, preparedPacket)
	}
	b := support.Launch(t, &harness.Options{MaxPacketSize: preparedPacket, MaxOutgoingMemory: preparedPacket})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-s"), CleanStart: true,
		Props: receiveMaximum5(1)}, support.At(1, topic))
	other := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-o"), CleanStart: true}, support.At(1, topic))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	support.Acknowledged(t, pub, ps[0])
	first := support.DeliveredAt(t, sub, 1, topic, ps[0].Payload)
	acknowledge(t, other, support.DeliveredAt(t, other, 1, topic, ps[0].Payload).ID, 0)
	support.Acknowledged(t, pub, ps[1])
	support.DeliveredAt(t, other, 1, topic, ps[1].Payload)
	acknowledge(t, sub, first.ID, 0)
	support.DeliveredAt(t, sub, 1, topic, ps[1].Payload)
}

// design.md, Delivery, Preparation: "A selected delivery reserves message
// memory only when send capacity is free for it. It is handed to the writer
// only within the window. Where deliveries must be handed to a
// connection's writer in a given order, their message-memory requests
// enter the outgoing FIFO in that order. A later delivery never reserves
// send capacity ahead of a delivery it must follow"; Order: "Channels
// supplying one connection are visited round-robin by channel id, with one
// delivery per visit", order holding "within each channel, not across
// them". Message memory, Two queues: "When the head of a FIFO does not fit
// in its budget, it waits with every younger request of that FIFO behind
// it, and no younger request of that FIFO passes it. A request receives
// its whole reservation or waits"; "A delivery of a compressed payload also
// reserves the stored copy's size. That reservation is given back once the
// store reader has decompressed it"; Bytes back: "A delivery reservation
// ends when the writer reports completion or discard".
//
// The cycle this guards against: a later delivery holding the outgoing
// budget while the delivery it must follow waits for room in it. The later
// one waits for its predecessor's hand-off, which waits for the later one's
// memory.
//
// With max_outgoing_memory the largest reservation of the four deliveries
// below, so that one holds it alone, and the delivery admission gate
// disabled, S subscribes at QoS 1, Receive Maximum 4, to the append channels
// ca and cb. P publishes four small messages on ca, which S receives and
// holds unacknowledged: its window is full. P publishes a1 on ca, b1 on cb,
// a2 on ca and b2 on cb, each a compressible payload over 4 KiB, so each
// delivery reserves its packet and its stored copy; no two deliveries fit in
// the budget together, even once one's stored copy has gone back. S's
// consumers select them alternately, by channel, and for quiet nothing
// comes, S's window being full. S then acknowledges its four messages,
// freeing its whole window: S receives a1, b1, a2 and b2, a1 before a2 and
// b1 before b2, each a first delivery at QoS 1.
//
// Were a2 to ask for memory ahead of b1, which it must follow, a2 would
// hold the budget waiting for b1's hand-off, and b1 would wait for a2's
// room for good: nothing after a1. No client sees which delivery asks for
// memory when; what this test observes is that every delivery arrives.
func TestOrderedDeliveriesReserveInOrder(t *testing.T) {
	const window = 4
	records := make([]*harness.Publish, 4)
	for i, topic := range []string{"ca/a", "cb/b", "ca/a", "cb/b"} {
		// Half noise, half repetition: S2 makes it shorter, not tiny.
		p := fmt.Appendf(nil, "%s-%d:", topic, i)
		p = append(p, support.Noise(2500)...)
		p = append(p, strings.Repeat(".", 2500)...)
		records[i] = &harness.Publish{QoS: 1, Topic: topic, Payload: p}
	}
	// Each delivery's reservation, its packet and its stored copy, and its
	// packet alone, once the stored copy has gone back. A configured
	// channel adds its properties to the packet (design.md, Sagüin
	// properties), so each is larger than counted here: the budget is the
	// largest counted reservation, with room for those properties, and no
	// packet alone beside the smallest reservation fits in it.
	least, most, packetOnly := math.MaxInt, 0, math.MaxInt
	for _, r := range records {
		n := len(harness.Encoded(t, &harness.Publish{QoS: 1, ID: 1, Topic: r.Topic, Payload: r.Payload}, harness.V5))
		least, most, packetOnly = min(least, n+shrinks(t, r.Payload)), max(most, n+shrinks(t, r.Payload)), min(packetOnly, n)
	}
	const properties = 256 // saguin-id, -offset and -timestamp, with room to spare
	memory := most + properties
	if least+packetOnly <= memory {
		t.Fatalf("deliveries reserving at least %d and at most %d (%d without the stored copy), against max_outgoing_memory's %d; the test needs no two beside each other",
			least, most, packetOnly, memory)
	}
	b := support.Launch(t, &harness.Options{MaxOutgoingMemory: memory, DeliveryAdmissionDelay: harness.NoDeliveryAdmission,
		Channels: []harness.Channel{{Name: "ca", Type: "append", Filter: "ca/#"}, {Name: "cb", Type: "append", Filter: "cb/#"}}})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-s"), CleanStart: true,
		Props: receiveMaximum5(window)}, support.At(1, "ca/#", "cb/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-p"), CleanStart: true})
	held := make([]uint16, window)
	for i := range held {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "ca/f", Payload: fmt.Appendf(nil, "f%d", i+1)})
		held[i] = support.DeliveredAt(t, sub, 1, "ca/f", fmt.Appendf(nil, "f%d", i+1)).ID
	}
	for _, r := range records {
		support.Acknowledged(t, pub, r)
	}
	nothingWithin(t, sub, quiet, "with its window full of unacknowledged messages")
	wire := make([]byte, 0, len(held)*4)
	for _, id := range held {
		wire = append(wire, harness.Encoded(t, &harness.Ack{Kind: harness.PUBACK, ID: id}, harness.V5)...)
	}
	harness.SendBytes(t, sub, wire) // one write: the four acknowledgements free the window at once

	got := make([]*harness.Publish, 0, len(records))
	for len(got) < len(records) {
		p := harness.Expect[*harness.Publish](t, sub)
		if p.QoS != 1 || p.Dup {
			t.Fatalf("S read PUBLISH %q at QoS %d, DUP %v, after %d deliveries; want each of the 4 once, a first delivery at QoS 1", p.Topic, p.QoS, p.Dup, len(got))
		}
		got = append(got, p)
	}
	at := func(r *harness.Publish) int {
		return slices.IndexFunc(got, func(p *harness.Publish) bool { return p.Topic == r.Topic && bytes.Equal(p.Payload, r.Payload) })
	}
	for _, pair := range [][2]int{{0, 2}, {1, 3}} {
		first, second := at(records[pair[0]]), at(records[pair[1]])
		if first < 0 || second < 0 || first > second {
			t.Errorf("S received %q at %d and %q at %d of its 4 deliveries; want both, in channel order", records[pair[0]].Payload[:8], first, records[pair[1]].Payload[:8], second)
		}
	}
}

// overPacket and overMemory are the broker.limits.max_packet_size and
// max_outgoing_memory of TestDeliveryOverOutgoingMemoryIsSkipped: small, so
// a handful of Subscription Identifiers take a delivery past the budget.
const overPacket, overMemory = 32, 66

// design.md, Message memory, Two queues: "A delivery whose reservation (its
// encoded size and any stored copy) exceeds max_outgoing_memory is
// discarded for that subscriber, counted as missed and logged, as one over
// its Maximum Packet Size (Delivery)"; Delivery: such a message "is
// discarded for that subscriber as if sent". config.md,
// max_outgoing_memory: "A delivery larger than it is discarded for that
// subscriber and counted as missed". SUBSCRIBE, One copy: "Subscription
// Identifiers appear [...] once for each contributing subscription having
// one". design.md, Testing: the operator sees the broker through "the
// log".
//
// With max_packet_size overPacket and max_outgoing_memory overMemory, two
// MQTT 5 subscribers each subscribe at QoS 0 to seven filters of the topic
// m/x, each in a SUBSCRIBE carrying a Subscription Identifier: the first's
// each 4 bytes, the second's the same but one of 3 (MQTT 5 1.5.5). A
// publisher sends a QoS 0 PUBLISH of exactly overPacket bytes on m/x. Its
// delivery to the first, the seven identifiers added, is 67 bytes, a byte
// over max_outgoing_memory; to the second, 66, max_outgoing_memory exactly.
// The publisher then sends a small message at QoS 1, and a second publisher
// another, on topics four of the filters match: each is acknowledged. The
// second subscriber receives all three, the large one in 66 bytes; the
// first the two small ones alone, in order, nothing held behind the
// discarded one. After the stop, the log holds a line naming the first
// subscriber, at a level the default log_level, info, writes.
func TestDeliveryOverOutgoingMemoryIsSkipped(t *testing.T) {
	const memory = overMemory
	b := support.Launch(t, &harness.Options{MaxPacketSize: overPacket, MaxOutgoingMemory: memory})
	// Client ids short enough for a CONNECT within max_connect_size, the
	// smaller of 100KiB and max_packet_size (config.md).
	const overID, fitsID = "memory-over", "memory-fits"
	filters := []string{"m/x", "m/+", "+/x", "+/+", "m/#", "+/#", "#"}
	overIDs := []uint32{longID, longID + 1, longID + 2, longID + 3, longID + 4, longID + 5, longID + 6}
	fitsIDs := append(slices.Clone(overIDs[:6]), longID-1) // a Variable Byte Integer of 3 bytes
	over := identifiedSubscriber(t, b.Addr, &harness.Connect{ClientID: overID, CleanStart: true}, 0, filters, overIDs)
	fits := identifiedSubscriber(t, b.Addr, &harness.Connect{ClientID: fitsID, CleanStart: true}, 0, filters, fitsIDs)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "memory-pub", CleanStart: true})

	// A fixed header of 2, the Topic Name's 2+3, an empty property
	// section's 1, and the payload.
	big := &harness.Publish{Topic: "m/x", Payload: bytes.Repeat([]byte("m"), overPacket-2-5-1)}
	if n := len(harness.Encoded(t, big, harness.V5)); n != overPacket {
		t.Fatalf("the large PUBLISH is %d bytes, want max_packet_size, %d", n, overPacket)
	}
	for _, d := range []struct {
		ids  []uint32
		want int
	}{{overIDs, memory + 1}, {fitsIDs, memory}} {
		if n := len(harness.Encoded(t, &harness.Publish{Topic: big.Topic, Props: identifiers(d.ids), Payload: big.Payload}, harness.V5)); n != d.want {
			t.Fatalf("a delivery carrying %d Subscription Identifiers is %d bytes, want %d", len(d.ids), n, d.want)
		}
	}
	if err := pub.Send(big); err != nil {
		t.Fatal(err)
	}
	next := &harness.Publish{QoS: 1, Topic: "m/n", Payload: []byte("next")}
	support.Acknowledged(t, pub, next)
	later, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "memory-later", CleanStart: true})
	last := &harness.Publish{QoS: 1, Topic: "m/l", Payload: []byte("last")}
	support.Acknowledged(t, later, last)

	got := delivered(t, fits, big.Topic, big.Payload)
	if n := len(harness.Encoded(t, got, harness.V5)); n != memory {
		t.Errorf("the large message reached the second subscriber in %d bytes, with properties %v; want %d, max_outgoing_memory", n, got.Props, memory)
	}
	for _, p := range []*harness.Publish{next, last} {
		delivered(t, fits, p.Topic, p.Payload)
	}
	delivered(t, over, next.Topic, next.Payload)
	delivered(t, over, last.Topic, last.Payload)
	stopped(t, b)

	loggedNaming(t, b, overID, "whose delivery was discarded as over max_outgoing_memory")
}

// largestPacket is MQTT's largest packet: a fixed header of 5 bytes, the
// first and a four-byte Remaining Length, before the largest Remaining
// Length (MQTT 5 1.5.5, 2.1.4); config.md, max_packet_size, "at most
// 268,435,460, the largest MQTT packet".
const largestPacket = 1 + 4 + harness.MaxVarInt

// largestWait is the watchdog of TestDeliveryThatCannotBeEncodedIsSkipped's
// writes and reads: each moves or stores a packet of 256 MiB.
const largestWait = time.Minute

// design.md, Delivery, A message over the client's Maximum Packet Size: "A
// client's maximum is never above MQTT's largest packet. A delivery whose
// encoded packet cannot fit within that maximum is discarded the same way,
// counting Subscription Identifiers and Topic Alias. [...] Neither is a store
// error"; the same way being "discarded for that subscriber as if sent,
// counted as missed and logged (MQTT-3.1.2-25)". SUBSCRIBE, One copy:
// "Subscription Identifiers appear [...] once for each contributing
// subscription having one". R8: "A store error stops the broker".
//
// With max_packet_size largestPacket, and max_incoming_memory at its least
// for it, about twice that, and max_outgoing_memory the same, so no delivery
// here is over max_outgoing_memory (design.md, Message memory), a subscriber
// whose subscription carries Subscription Identifier 7 and another,
// subscribed to one topic only, listen. A publisher sends a QoS 0 PUBLISH
// of exactly largestPacket bytes on the first subscriber's filter: its
// delivery, the identifier's 2 bytes added (MQTT 5 3.3.2.3.8), cannot be
// encoded. The publisher then sends a small message at QoS 1, on the
// topic both listen to, and it is acknowledged. Both subscribers receive
// it, the first with its identifier, and nothing before it; a new client
// connects and is answered PINGRESP. The broker stops with exit status 0,
// and its log holds a line naming the first subscriber, at a level the
// default log_level, info, writes.
//
// The test holds a packet of 256 MiB in memory, and the broker stores it,
// its payload over 4 KiB being compressed (design.md, Store, Compression).
func TestDeliveryThatCannotBeEncodedIsSkipped(t *testing.T) {
	b := support.Launch(t, &harness.Options{MaxPacketSize: largestPacket,
		MaxIncomingMemory: leastIncoming(largestPacket), MaxOutgoingMemory: leastIncoming(largestPacket)})
	idsID := clientID(t, "-ids")
	ids, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: idsID, CleanStart: true})
	support.Suback(t, ids, identifier(7), support.At(0, "huge/#"), 0)
	other := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true}, support.At(0, "huge/next"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	// The PUBLISH at QoS 0, written in place: its fixed header, the Topic
	// Name, an empty property section, and the payload, zeros, to the end.
	huge := make([]byte, 0, largestPacket)
	huge = harness.AppendVarInt(append(huge, 0x30), harness.MaxVarInt)
	huge = harness.AppendString(huge, "huge/big")
	huge = append(huge, 0x00)
	huge = huge[:largestPacket]
	pub.Timeout = largestWait
	harness.SendBytes(t, pub, huge)

	next := &harness.Publish{QoS: 1, Topic: "huge/next", Payload: []byte("next")}
	support.Acknowledged(t, pub, next)
	got := delivered(t, ids, next.Topic, next.Payload)
	if id, ok := got.Props.Get(harness.SubscriptionIdentifier); !ok || id.Int != 7 {
		t.Errorf("the next message came with properties %v; want Subscription Identifier 7", got.Props)
	}
	delivered(t, other, next.Topic, next.Payload)
	later, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-later"), CleanStart: true})
	if err := later.Ping(); err != nil {
		t.Fatal(err)
	}
	stopped(t, b)

	loggedNaming(t, b, idsID, "whose delivery could not be encoded")
}

// design.md, Sagüin properties: "Deliveries to clients of messages on
// $broadcast carry none of them. They receive only MQTT's own per-delivery
// changes [...] The publisher's own User Properties go unaltered"; Flows, SUBSCRIBE, One copy: "The delivery
// uses the lower of the publication's QoS and the highest contributing
// grant"; Options: "No Local is honoured everywhere, against the publisher's
// client id stored on the record"; storage.md, wills: client, "the
// publisher's client id, captured when the Will was accepted". MQTT 5
// 3.1.2.5: "The Will Message consists of the Will Properties, Will Topic, and
// Will Payload fields in the CONNECT Payload"; 3.1.3.2: "The Will Properties
// field defines the Application Message properties to be sent with the Will
// Message when it is published"; [MQTT-3.1.3-10]: "The Server MUST maintain
// the order of User Properties when publishing the Will Message";
// [MQTT-3.1.2-14]: with Will Retain 0, "the Server MUST publish the Will
// Message as a non-retained message"; [MQTT-3.8.3-3]: with No Local,
// "Application Messages MUST NOT be forwarded to a connection with a ClientID
// equal to the ClientID of the publishing connection". MQTT 3.1.1
// 3.1.2.5-3.1.2.7: the Will Topic, QoS and Retain; [MQTT-3.1.2-16]: not
// retained.
//
// MQTT 5: a client with a durable session, subscribed with No Local to its
// own Will's topic, drops its socket. Its Will, at QoS 1 and carrying
// every property a Will may carry but Will Delay and Message Expiry, User
// Properties repeated under one name among them, reaches a subscriber at
// QoS 1 as given: topic, payload, RETAIN 0, exactly its properties. The
// client, resumed, receives a message published on the topic next, not
// its own Will. MQTT 3.1.1: a QoS 1 Will reaches an MQTT 3.1.1 subscriber
// at QoS 1, on its topic with its payload, RETAIN 0.
func TestWillArrivesAsGiven(t *testing.T) {
	t.Run("MQTT 5", func(t *testing.T) {
		topic := clientID(t, "/will")
		props := harness.Properties{
			{ID: harness.PayloadFormatIndicator, Int: 1},
			{ID: harness.ContentType, Str: "text/plain"},
			{ID: harness.ResponseTopic, Str: topic + "/replies"},
			{ID: harness.CorrelationData, Data: []byte{0, 1, 2}},
			harness.User("k", "2"), harness.User("a", "x"), harness.User("k", "1"),
		}
		sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, topic))
		pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		id := clientID(t, "")
		w, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable,
			Will: &harness.Will{QoS: 1, Topic: topic, Payload: support.WillPayload, Props: props}})
		support.Suback(t, w, nil, []harness.Subscription{{Filter: topic, QoS: 1, NoLocal: true}}, 1)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p := liveAt(t, sub, 1, topic, support.WillPayload)
		if diff := sameProperties(p.Props, props); diff != "" {
			t.Errorf("the Will came with %s", diff)
		}
		acknowledge(t, sub, p.ID, 0)
		again := resumed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable})
		thenLiveAt(t, pub, again, 1, topic)
	})
	t.Run("MQTT 3.1.1", func(t *testing.T) {
		topic := clientID(t, "/will")
		sub := support.Subscribed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, topic))
		w, _ := harness.Connected(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true,
			Will: &harness.Will{QoS: 1, Topic: topic, Payload: support.WillPayload}})
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		liveAt(t, sub, 1, topic, support.WillPayload)
	})
}

// design.md, Flows, PUBLISH: "A Will's Message Expiry starts when the Will
// is published"; Delivery, Message Expiry: "worked out when the message is
// written and sent as the time remaining. On $broadcast an expired message
// is skipped for that subscriber"; DISCONNECT, A lost connection: "a
// nonzero effective Keep Alive gives the reader 1.5 times that interval
// when it begins reading the next packet. [...] A keep-alive timeout
// closes the connection". MQTT 5 3.1.3.2.4: the Will's Message Expiry
// Interval "is the lifetime of the Will Message in seconds and is sent as
// the Publication Expiry Interval when the Server publishes the Will
// Message"; 3.3.2.3.3: "The PUBLISH packet sent to a Client by the Server
// MUST contain a Message Expiry Interval set to the received value minus
// the time that the Application Message has been waiting in the Server"
// [MQTT-3.3.2-6].
//
// A client with a keep-alive of 1 second connects with a Will whose
// Message Expiry Interval is 1 second, the smallest each can be, and stays
// silent: the broker closes its connection no sooner than 1.5 seconds
// after the CONNECT, past the Will's lifetime counted from then. A
// subscriber receives the Will, its Message Expiry Interval at most 1 and
// no less than 1 less the whole seconds from the earliest the Will could
// be published to its reading.
func TestWillMessageExpiryStartsAtItsPublication(t *testing.T) {
	const lifetime = 1  // the Will's Message Expiry Interval, in seconds
	const keepAlive = 1 // the client's, in seconds
	idle := keepAlive * 1500 * time.Millisecond
	topic := clientID(t, "/will")
	sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, topic))
	connecting := time.Now() // before the CONNECT, which starts the keep-alive's wait
	harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true, KeepAlive: keepAlive,
		Will: &harness.Will{Topic: topic, Payload: support.WillPayload, Props: expiry(lifetime)}})
	p := liveAt(t, sub, 0, topic, support.WillPayload)
	waited := int64(math.Ceil((time.Since(connecting) - idle).Seconds()))
	switch remaining, ok := p.Props.Get(harness.MessageExpiryInterval); {
	case !ok:
		t.Errorf("the Will came with no Message Expiry Interval; want what is left of its %d seconds", lifetime)
	case remaining.Int > lifetime || int64(remaining.Int) < lifetime-waited:
		t.Errorf("Message Expiry Interval %d, the Will published at most %d seconds before it was read; want %d to %d",
			remaining.Int, waited, lifetime-waited, lifetime)
	}
}

// design.md, Flows, SUBSCRIBE, the subscription table: "append, or a
// queue's own topics (a reader) | the floor, or the tail where the channel
// sets start: tail"; config.md, channels.<name>: start (append, queue),
// default floor, "floor or tail: where a reader with no position begins";
// design.md, Words: Retention, "The *floor* is the oldest record retention
// has not passed. The *tail* is the next offset"; Channel: "A subscription's
// filter reaches the channels its topics land in"; SUBSCRIBE, The start
// point: "The captured boundary separates the initial pass [...] from
// subsequent live records"; Delivery, Order: "a channel's offset order on
// $broadcast, append". MQTT 5 3.8.4: the delivery's QoS is "the minimum of
// the QoS of the originally published message and the Maximum QoS granted".
//
// Three append channels, nothing retained by them yet: early (early/#, its
// start left to the default), floored (floored/#, start: floor) and late
// (late/#, start: tail). A publisher stores three records in each, at QoS
// 1, 0 and 1, on two topics. Then, under each version, a client per
// channel subscribes at QoS 0 to its filter, and the publisher stores one
// more record in each channel. The readers of early and floored receive
// the channel's three records, then the fourth, in that order; the readers
// of late the fourth alone, then nothing before a fifth stored after it.
func TestNewReaderStartsAtTheFloorOrTheTail(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "early", Type: "append", Filter: "early/#"},
		{Name: "floored", Type: "append", Filter: "floored/#", Start: "floor"},
		{Name: "late", Type: "append", Filter: "late/#", Start: "tail"},
	}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "starts-pub", CleanStart: true})
	prefixes := []string{"early", "floored", "late"}
	stored := make(map[string][]*harness.Publish) // by prefix, in offset order
	for _, prefix := range prefixes {
		stored[prefix] = []*harness.Publish{
			{QoS: 1, Topic: prefix + "/a", Payload: []byte(prefix + " 1")},
			{QoS: 0, Topic: prefix + "/b", Payload: []byte(prefix + " 2")},
			{QoS: 1, Topic: prefix + "/a", Payload: []byte(prefix + " 3")},
		}
		support.PublishAll(t, pub, stored[prefix])
	}
	readers := make(map[string]*harness.Client) // by version's name and prefix
	for _, version := range []byte{harness.V5, harness.V311} {
		for _, prefix := range prefixes {
			id := fmt.Sprintf("%s-%d", prefix, version)
			readers[id] = support.Subscribed(t, b.Addr, version, &harness.Connect{ClientID: id, CleanStart: true}, support.At(0, prefix+"/#"))
		}
	}
	for _, prefix := range prefixes {
		live := &harness.Publish{QoS: 1, Topic: prefix + "/b", Payload: []byte(prefix + " live")}
		support.Acknowledged(t, pub, live)
		if prefix == "late" {
			stored[prefix] = nil // a reader at the tail is owed nothing stored before it
		}
		stored[prefix] = append(stored[prefix], live)
	}
	for _, version := range []byte{harness.V5, harness.V311} {
		for _, prefix := range prefixes {
			c := readers[fmt.Sprintf("%s-%d", prefix, version)]
			for _, p := range stored[prefix] {
				delivered(t, c, p.Topic, p.Payload)
			}
		}
	}
	after := &harness.Publish{QoS: 1, Topic: "late/a", Payload: []byte("late after")}
	support.Acknowledged(t, pub, after)
	for _, version := range []byte{harness.V5, harness.V311} {
		delivered(t, readers[fmt.Sprintf("late-%d", version)], after.Topic, after.Payload)
	}
}

// design.md, Words: Channel, "A subscription's filter reaches the channels
// its topics land in. Each topic belongs to its own channel"; Flows,
// SUBSCRIBE, One copy: "A filter reaching several channels receives each
// record under its own channel's rules"; Delivery, Order: "For a filter
// reaching several channels, order holds within each channel, not across
// them"; "Channels supplying one connection are visited round-robin by
// channel id, with one delivery per visit"; One mechanism for every channel:
// "a cursor or position and an in-flight window. The window is the lower of
// the client's Receive Maximum and limits.max_inflight"; Words: Consumer, "a
// share of the connection's in-flight window". MQTT 5 [MQTT-3.3.4-9]: "The
// Server MUST NOT send more than Receive Maximum QoS 1 and QoS 2 PUBLISH
// packets for which it has not received PUBACK".
//
// Three append channels, under rr/c, rr/a and rr/b, configured in that
// order. A publisher stores four records in each, all of rr/c's first,
// then rr/a's, then rr/b's. A client with Receive Maximum 2 subscribes at
// QoS 1 to rr/#, which reaches the three. It receives two deliveries and
// nothing more while it holds both; then, acknowledging the oldest it
// holds each time, one more per PUBACK, twelve in all, each record once.
// Each channel's records come in their offset order. Once each channel has
// supplied a delivery, every later delivery comes from the channel after
// the previous one's, in the cyclic order of the channels' ids
// (storage.md, channels), among the channels still owing records.
func TestFilterReachingSeveralChannels(t *testing.T) {
	const window, each = 2, 4
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "rr-c", Type: "append", Filter: "rr/c/#"},
		{Name: "rr-a", Type: "append", Filter: "rr/a/#"},
		{Name: "rr-b", Type: "append", Filter: "rr/b/#"},
	}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "rr-pub", CleanStart: true})
	owed := make(map[string][]string) // each channel's payloads, by its level under rr, in offset order
	for _, level := range []string{"c", "a", "b"} {
		for i := range each {
			p := &harness.Publish{QoS: 1, Topic: "rr/" + level + "/x", Payload: fmt.Appendf(nil, "%s%d", level, i+1)}
			support.Acknowledged(t, pub, p)
			owed[level] = append(owed[level], string(p.Payload))
		}
	}
	stored := storedChannels(t, b.DB)
	ids := map[string]int64{"a": stored["rr-a"].id, "b": stored["rr-b"].id, "c": stored["rr-c"].id}

	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "rr-sub", CleanStart: true, Props: receiveMaximum5(window)}, support.At(1, "rr/#"))
	var got []*harness.Publish
	// next reads the next delivery, a first one at QoS 1 under rr.
	next := func() {
		t.Helper()
		p := harness.Expect[*harness.Publish](t, sub)
		if p.QoS != 1 || p.Dup || !strings.HasPrefix(p.Topic, "rr/") {
			t.Fatalf("PUBLISH %q at QoS %d, DUP %v; want a first delivery at QoS 1 under rr", p.Topic, p.QoS, p.Dup)
		}
		got = append(got, p)
	}
	for range window {
		next()
	}
	nothingWithin(t, sub, quiet, fmt.Sprintf("with its window of %d full, across three channels", window))
	for i := 0; len(got) < 3*each; i++ {
		acknowledge(t, sub, got[i].ID, 0)
		next()
	}

	seen := make(map[string]bool)
	allSeen := -1 // the index from which every channel has supplied a delivery
	var previous string
	for i, p := range got {
		level := strings.Split(p.Topic, "/")[1]
		switch {
		case len(owed[level]) == 0:
			t.Fatalf("delivery %d, %q of %q: its channel owes nothing more", i+1, p.Payload, p.Topic)
		case string(p.Payload) != owed[level][0]:
			t.Fatalf("delivery %d is %q of %q; want %q, its channel's next in offset order", i+1, p.Payload, p.Topic, owed[level][0])
		}
		if allSeen >= 0 && level != nextOwing(ids, owed, previous) {
			t.Errorf("delivery %d comes from rr/%s after one from rr/%s; want rr/%s, the next channel by id still owing records (ids %v)",
				i+1, level, previous, nextOwing(ids, owed, previous), ids)
		}
		owed[level] = owed[level][1:]
		seen[level], previous = true, level
		if allSeen < 0 && len(seen) == len(ids) {
			allSeen = i
		}
	}
}

// nextOwing is the level of the channel after the one of previous, in the
// cyclic order of the channels' ids, among those whose owed list is not
// empty.
func nextOwing(ids map[string]int64, owed map[string][]string, previous string) string {
	levels := slices.SortedFunc(maps.Keys(ids), func(a, b string) int { return cmp.Compare(ids[a], ids[b]) })
	at := slices.Index(levels, previous)
	for i := 1; i <= len(levels); i++ {
		if level := levels[(at+i)%len(levels)]; len(owed[level]) > 0 {
			return level
		}
	}
	return ""
}

// splitSaguin splits ps into its saguin- User Properties, by name, counting
// repeats, and the rest, those the broker reads at sending (Message Expiry
// Interval, Subscription Identifier) left out.
func splitSaguin(ps harness.Properties) (own map[string][]string, others harness.Properties) {
	own = make(map[string][]string)
	for _, prop := range ps {
		switch {
		case prop.ID == harness.UserProperty && strings.HasPrefix(prop.Str, "saguin-"):
			own[prop.Str] = append(own[prop.Str], prop.Value)
		case prop.ID != harness.MessageExpiryInterval && prop.ID != harness.SubscriptionIdentifier:
			others = append(others, prop)
		}
	}
	return own, others
}

// expectSaguin fails the test unless p, a delivery of r to an MQTT 5
// client, carries the Sagüin properties design.md, Sagüin properties, gives
// it: "saguin-id, saguin-offset, saguin-timestamp | every delivery";
// "saguin-expires | where the publisher set an expiry"; "saguin-channel |
// where the consumer's filter reaches more than one channel", channel ""
// where it does not; each once, and no other saguin- property. Their
// values: "saguin-id is the publisher's own where it set one, kept as sent,
// else a UUIDv7 the broker makes; saguin-offset [...] decimal;
// saguin-timestamp, saguin-expires [...] Unix milliseconds in decimal;
// saguin-channel [...] channel names", read against the record's row
// (storage.md, records: id "its saguin-id", time "its timestamp", expires
// "when its Message Expiry runs out"). Besides them, p carries sent's
// properties, those the broker reads at sending aside (Message Expiry
// Interval, Subscription Identifier), "the publisher's own User Properties
// going unaltered (MQTT-3.3.2-17)"; "Nothing a publisher sends is altered".
// Each of extra, a saguin- User Property, is wanted once too, as a worker
// offer's saguin-attempt is ("saguin-attempt | queue jobs sent to workers").
func expectSaguin(t *testing.T, p *harness.Publish, r *record, channel string, sent harness.Properties, extra ...harness.Property) {
	t.Helper()
	want := map[string]string{"saguin-id": r.id, "saguin-offset": strconv.FormatInt(r.offset, 10),
		"saguin-timestamp": strconv.FormatInt(r.time, 10)}
	if r.expires != nil {
		want["saguin-expires"] = strconv.FormatInt(*r.expires, 10)
	}
	if channel != "" {
		want["saguin-channel"] = channel
	}
	for _, prop := range extra {
		want[prop.Str] = prop.Value
	}
	own, others := splitSaguin(p.Props)
	for name, value := range want {
		if values := own[name]; len(values) != 1 || values[0] != value {
			t.Errorf("the delivery of %q carries %s %q; want it once, %q", p.Payload, name, values, value)
		}
	}
	for name, values := range own {
		if _, ok := want[name]; !ok {
			t.Errorf("the delivery of %q carries %s %q; want no such property", p.Payload, name, values)
		}
	}
	_, publisher := splitSaguin(sent)
	if diff := sameProperties(others, publisher); diff != "" {
		t.Errorf("the delivery of %q, its saguin- properties aside, carries %s", p.Payload, diff)
	}
}

// design.md, Sagüin properties: "Sagüin adds these User Properties to
// deliveries to clients from configured channels. Deliveries to clients of
// messages on $broadcast carry none of them. [...] The publisher's own User Properties go unaltered
// (MQTT-3.3.2-17)"; the table, "saguin-id, saguin-offset, saguin-timestamp |
// every delivery", "saguin-expires | where the publisher set an expiry",
// "saguin-channel | where the consumer's filter reaches more than one
// channel"; "A publish carrying any saguin- property except saguin-id, which
// sets the record's id"; "MQTT 3.1.1 clients receive none"; values as
// expectSaguin reads them. Flows, SUBSCRIBE, Options: "RETAIN is 0 on channel
// deliveries; Retain As Published applies on $broadcast only"; Channels: "A
// retain flag on a configured channel topic is ignored (MQTT-3.3.1-5)";
// Delivery, Message Expiry: "worked out when the message is written and sent
// as the time remaining". MQTT 5 [MQTT-3.3.2-6]; [MQTT-3.3.1-13]: with Retain
// As Published 1, "the Server MUST set the RETAIN flag equal to the RETAIN
// flag in the received PUBLISH packet"; MQTT 3.1.1 [MQTT-3.3.1-9]: RETAIN 0
// on a message sent for an established subscription.
//
// Three append channels: props (props/#), multi-a (multi/a/#) and multi-b
// (multi/b/#). A publisher stores, retained, on props/own, a message with
// its own saguin-id, a Content Type and three User Properties, two under
// one name. Then an MQTT 5 client subscribes at QoS 1 to props/#, which
// reaches props alone; another, with Retain As Published, to multi/#,
// which reaches multi-a, multi-b and $broadcast; and an MQTT 3.1.1 client
// to both. The publisher then stores on props/expiring a message expiring
// in 600 seconds, and, retained, one on each of multi/a/1, multi/b/1 and
// multi/x. Each delivery comes at QoS 1, its payload as sent:
//   - props/own and props/expiring, in that order: the Sagüin properties
//     but saguin-channel, props/own's saguin-id its own, props/expiring's
//     a UUIDv7, with saguin-expires; RETAIN 0; props/expiring's Message
//     Expiry Interval at most 600 and no less than 600 less the whole
//     seconds from its sending to its reading;
//   - multi/a/1 and multi/b/1: the Sagüin properties, saguin-channel naming
//     its channel; RETAIN 0;
//   - multi/x: exactly the publisher's properties, none of them saguin-;
//     RETAIN 1;
//   - to MQTT 3.1.1, all five, RETAIN 0, with no property section.
func TestChannelDeliveriesCarrySaguinProperties(t *testing.T) {
	const lasting = 600
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "props", Type: "append", Filter: "props/#"},
		{Name: "multi-a", Type: "append", Filter: "multi/a/#"},
		{Name: "multi-b", Type: "append", Filter: "multi/b/#"},
	}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "props-pub", CleanStart: true})
	user := harness.Properties{harness.User("unit", "celsius"), harness.User("source", "probe-2"), harness.User("unit", "kelvin")}
	own := &harness.Publish{QoS: 1, Retain: true, Topic: "props/own", Payload: []byte("own"), Props: slices.Concat(user[:1],
		harness.Properties{harness.User("saguin-id", "order-17"), {ID: harness.ContentType, Str: "text/plain"}}, user[1:])}
	support.Acknowledged(t, pub, own)

	single := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "props-single", CleanStart: true}, support.At(1, "props/#"))
	multi := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "props-multi", CleanStart: true},
		[]harness.Subscription{{Filter: "multi/#", QoS: 1, RetainAsPublished: true}})
	legacy := support.Subscribed(t, b.Addr, harness.V311, &harness.Connect{ClientID: "props-legacy", CleanStart: true}, support.At(1, "props/#", "multi/#"))
	expiring := &harness.Publish{QoS: 1, Topic: "props/expiring", Payload: []byte("expiring"), Props: slices.Concat(expiry(lasting), user)}
	sentAt := support.Sent(t, pub, expiring)
	support.Puback(t, pub, expiring)
	spread := []*harness.Publish{
		{QoS: 1, Retain: true, Topic: "multi/a/1", Payload: []byte("a"), Props: user},
		{QoS: 1, Retain: true, Topic: "multi/b/1", Payload: []byte("b"), Props: user},
		{QoS: 1, Retain: true, Topic: "multi/x", Payload: []byte("x"), Props: user},
	}
	support.PublishAll(t, pub, spread)

	rows := make(map[string]*record) // by payload
	for _, r := range records(t, b.DB) {
		rows[string(r.payload)] = &r
	}
	got := support.DeliveredAt(t, single, 1, own.Topic, own.Payload)
	expectSaguin(t, got, rows["own"], "", own.Props)
	if id := got.Props.User("saguin-id"); len(id) != 1 || id[0] != "order-17" {
		t.Errorf("the delivery of own carries saguin-id %q; want the publisher's own, order-17, kept as sent", id)
	}
	got = support.DeliveredAt(t, single, 1, expiring.Topic, expiring.Payload)
	elapsed := uint32(math.Ceil(time.Since(sentAt).Seconds()))
	expectSaguin(t, got, rows["expiring"], "", expiring.Props)
	if !uuidv7.MatchString(rows["expiring"].id) {
		t.Errorf("the record of expiring has the id %q; want a UUIDv7, its publisher having set none", rows["expiring"].id)
	}
	if remaining, ok := got.Props.Get(harness.MessageExpiryInterval); !ok || remaining.Int > lasting || remaining.Int < lasting-elapsed {
		t.Errorf("the delivery of expiring carries Message Expiry Interval %d (present %v); want %d to %d", remaining.Int, ok, lasting-elapsed, lasting)
	}
	byTopic := make(map[string]*harness.Publish)
	for range spread {
		p := harness.Expect[*harness.Publish](t, multi)
		byTopic[p.Topic] = p
	}
	for _, p := range spread {
		d, ok := byTopic[p.Topic]
		switch {
		case !ok || d.QoS != 1 || !bytes.Equal(d.Payload, p.Payload):
			t.Errorf("multi/# received %+v for %q; want it once, at QoS 1, payload %q", d, p.Topic, p.Payload)
			continue
		case p.Topic == "multi/x":
			if !d.Retain {
				t.Error("multi/x, retained on $broadcast, came with RETAIN 0 to a subscription with Retain As Published")
			}
			if diff := sameProperties(d.Props, p.Props); diff != "" {
				t.Errorf("multi/x, on $broadcast, came with %s", diff)
			}
		case d.Retain:
			t.Errorf("%q, on a configured channel, came with RETAIN 1; want 0", p.Topic)
		default:
			expectSaguin(t, d, rows[string(p.Payload)], "multi-"+string(p.Payload), p.Props)
		}
	}
	payloads := make([]string, 0, 2+len(spread))
	for range 2 + len(spread) {
		p := harness.Expect[*harness.Publish](t, legacy)
		if p.QoS != 1 || p.Retain || len(p.Props) != 0 {
			t.Errorf("MQTT 3.1.1 received %q at QoS %d, RETAIN %v, properties %v; want QoS 1, RETAIN 0 and none", p.Topic, p.QoS, p.Retain, p.Props)
		}
		payloads = append(payloads, string(p.Payload))
	}
	slices.Sort(payloads)
	if !slices.Equal(payloads, []string{"a", "b", "expiring", "own", "x"}) {
		t.Errorf("MQTT 3.1.1 received the payloads %q; want each of the five once", payloads)
	}
}

// expiresOf reads, from the database at path, when the Message Expiry of the
// record of payload runs out, in Unix milliseconds, failing the test where
// it holds none (storage.md, records: expires "when its Message Expiry runs
// out, counted from its receipt"); each test's records have payloads of
// their own.
func expiresOf(t *testing.T, path string, payload []byte) int64 {
	t.Helper()
	rs := records(t, path)
	for i := range rs {
		if bytes.Equal(rs[i].payload, payload) && rs[i].expires != nil {
			return *rs[i].expires
		}
	}
	t.Fatalf("the record of %q holds no expiry; want when its Message Expiry runs out (0014a)", payload)
	return 0
}

// expectExpired fails the test unless got, a delivery to an MQTT 5 client of
// a configured channel's record whose Message Expiry ran out at expires, in
// Unix milliseconds, carries a Message Expiry Interval of 0 and
// saguin-expires, once, of expires (design.md, Delivery, Message Expiry: "On
// a configured channel it is still delivered, with its interval 0 and
// saguin-expires saying when it ran out"; Sagüin properties: "Unix
// milliseconds in decimal").
func expectExpired(t *testing.T, got *harness.Publish, expires int64) {
	t.Helper()
	if remaining, ok := got.Props.Get(harness.MessageExpiryInterval); !ok || remaining.Int != 0 {
		t.Errorf("the expired message %q came with Message Expiry Interval %d (present %v); want 0", got.Payload, remaining.Int, ok)
	}
	if at := got.Props.User("saguin-expires"); !slices.Equal(at, []string{strconv.FormatInt(expires, 10)}) {
		t.Errorf("the expired message %q came with saguin-expires %q; want once, %d, when it ran out", got.Payload, at, expires)
	}
}

// design.md, Delivery, Message Expiry: "On a configured channel it is still
// delivered, with its interval 0 and saguin-expires saying when it ran out (a
// departure from MQTT-3.3.2-5). Only retention removes a record"; Sagüin
// properties: "saguin-expires | where the publisher set an expiry", "Unix
// milliseconds in decimal"; storage.md, records: expires "when its Message
// Expiry runs out, counted from its receipt"; design.md, Sagüin properties:
// "MQTT 3.1.1 clients receive none". MQTT 5 3.3.2.3.3: the Message Expiry
// Interval is a Four Byte Integer, "sent as the time remaining".
//
// On an append channel, lapsed (lapsed/#), a publisher stores a message
// with a Message Expiry Interval of 1 second, the smallest there is, then
// one with none. Once the first's expiry, read from its row, has passed,
// an MQTT 5 and an MQTT 3.1.1 client subscribe at QoS 1 to lapsed/#: each
// receives both, in that order. To MQTT 5 the first carries a Message
// Expiry Interval of 0 and saguin-expires, the moment its row holds, and
// the second neither.
func TestExpiredChannelRecordIsStillDelivered(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "lapsed", Type: "append", Filter: "lapsed/#"}}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lapsed-pub", CleanStart: true})
	short := &harness.Publish{QoS: 1, Topic: "lapsed/short", Props: expiry(1), Payload: []byte("short")}
	lasting := &harness.Publish{QoS: 1, Topic: "lapsed/none", Payload: []byte("none")}
	support.PublishAll(t, pub, []*harness.Publish{short, lasting})
	expires := expiresOf(t, b.DB, short.Payload)
	time.Sleep(time.Until(time.UnixMilli(expires + 1))) // the expiry passing is the behaviour under test

	v5 := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lapsed-5", CleanStart: true}, support.At(1, "lapsed/#"))
	v311 := support.Subscribed(t, b.Addr, harness.V311, &harness.Connect{ClientID: "lapsed-311", CleanStart: true}, support.At(1, "lapsed/#"))
	expectExpired(t, support.DeliveredAt(t, v5, 1, short.Topic, short.Payload), expires)
	got := support.DeliveredAt(t, v5, 1, lasting.Topic, lasting.Payload)
	if _, ok := got.Props.Get(harness.MessageExpiryInterval); ok || got.Props.User("saguin-expires") != nil {
		t.Errorf("the message published with no expiry came with %v; want no Message Expiry Interval or saguin-expires", got.Props)
	}
	support.DeliveredAt(t, v311, 1, short.Topic, short.Payload)
	support.DeliveredAt(t, v311, 1, lasting.Topic, lasting.Payload)
}

// design.md, Delivery: "A message over the client's Maximum Packet Size is
// discarded for that subscriber as if sent, counted as missed and logged
// (MQTT-3.1.2-25)", the Sagüin properties being part of the delivery
// (Sagüin properties: "Sagüin adds these User Properties to deliveries to
// clients from configured channels. Deliveries to clients of messages on
// $broadcast carry none of them";
// "saguin-id, saguin-offset, saguin-timestamp | every delivery"; "The value
// of saguin-id [...] Otherwise the broker makes a UUIDv7" where the publisher
// set none). MQTT 5 [MQTT-3.1.2-24], [MQTT-3.1.2-25]. design.md, Testing: the
// operator sees the broker through "the log".
//
// An append channel, sized, holds sized/#; plain/# is $broadcast's. Two
// MQTT 5 clients send a Maximum Packet Size of 200, which the harness
// client holds the broker to: one subscribes at QoS 0 to sized/#, the
// other to plain/#. A payload of 150 bytes makes a QoS 0 PUBLISH under 200
// bytes on either topic, but over it once it carries the three Sagüin
// properties, their values as short as they can be (a UUIDv7's 36
// characters, a one-digit offset, a timestamp's 13 digits). A publisher
// stores that payload on sized/big and plain/big, then a one-byte payload
// on sized/next and plain/next. The first client receives sized/next
// alone; the second both of its own. After the stop, the broker's log
// holds a line naming the first client, at a level the default log_level,
// info, writes.
func TestSaguinPropertiesCanMakeADeliveryTooLarge(t *testing.T) {
	const limit = 200
	big := bytes.Repeat([]byte("b"), 150)
	bare := len(harness.Encoded(t, &harness.Publish{Topic: "sized/big", Payload: big}, harness.V5))
	dressed := len(harness.Encoded(t, &harness.Publish{Topic: "sized/big", Payload: big, Props: harness.Properties{
		harness.User("saguin-id", strings.Repeat("0", 36)), harness.User("saguin-offset", "1"),
		harness.User("saguin-timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10)),
	}}, harness.V5))
	if bare > limit || dressed <= limit {
		t.Fatalf("the PUBLISH is %d bytes bare and %d with the Sagüin properties; want the first within %d and the second over it", bare, dressed, limit)
	}
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "sized", Type: "append", Filter: "sized/#"}}})
	small := harness.Properties{{ID: harness.MaximumPacketSize, Int: limit}}
	sizedID := clientID(t, "-sized")
	sized := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: sizedID, CleanStart: true, Props: small}, support.At(0, "sized/#"))
	plain := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-plain"), CleanStart: true, Props: small}, support.At(0, "plain/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "sized/big", Payload: big},
		{QoS: 1, Topic: "plain/big", Payload: big},
		{QoS: 1, Topic: "sized/next", Payload: []byte("n")},
		{QoS: 1, Topic: "plain/next", Payload: []byte("n")},
	}
	support.PublishAll(t, pub, ps)
	delivered(t, sized, ps[2].Topic, ps[2].Payload)
	delivered(t, plain, ps[1].Topic, ps[1].Payload)
	delivered(t, plain, ps[3].Topic, ps[3].Payload)
	stopped(t, b)

	loggedNaming(t, b, sizedID, "whose delivery its Sagüin properties made too large")
}
