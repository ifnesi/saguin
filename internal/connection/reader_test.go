package connection_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Flows, CONNECT, 1: "If no CONNECT arrives within
// connect_timeout, the connection closes." A CONNECT in time stops the clock,
// however long its CONNACK takes: the core may wait on the store (CONNECT, 8:
// "Once the changes are stored, the broker sends CONNACK"), which no client
// can make slow.
func TestConnectTimeout(t *testing.T) {
	t.Parallel()
	cfg := newConfig(t)
	cfg.Broker.Limits.ConnectTimeout = connectTimeout
	b := start(t, cfg)
	c, conn := b.accept(harness.V5)
	b.sendConnect(c, conn, connectPacket(harness.V5))
	b.quiet(conn, 2*connectTimeout)
	b.send(conn, &packet.Connack{})
	harness.Expect[*harness.Connack](t, c)
	sendPacket(t, c, &harness.Pingreq{})
	b.await(conn, connection.Received)
	b.send(conn, connection.Pingresp{})
	harness.Expect[*harness.Pingresp](t, c)
}

// config.md, max_connect_size: "the largest CONNECT, read before
// authentication". design.md, Flows, Packet buffers: "A packet's size is
// checked before its body is read." design.md, CONNECT: "0x95 | a CONNECT
// over limits.max_connect_size, sent without the client's Maximum Packet
// Size, which is in the unread body | none: the connection closes" (no MQTT
// 3.1.1 code). MQTT 5 2.1.4: "The
// packet size is the total number of bytes in an MQTT Control Packet, this
// is equal to the length of the Fixed Header plus the Remaining Length."
// An oversize CONNECT is refused from its fixed header and protocol level,
// its body never sent; one of exactly the size is read.
func TestMaxConnectSize(t *testing.T) {
	t.Parallel()
	const size = 1024
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			t.Parallel()
			cfg := newConfig(t)
			cfg.Broker.Limits.MaxConnectSize = size
			b := start(t, cfg)

			// A fixed header announcing twice the size, then the Protocol Name
			// and level alone.
			c, conn := b.accept(version)
			head := harness.AppendVarInt([]byte{0x10}, 2*size)
			head = append(head, 0, 4, 'M', 'Q', 'T', 'T', version)
			harness.SendBytes(t, c, head)
			ev := b.await(conn, connection.Received)
			if ev.Reason != 0x95 {
				t.Fatalf("Reason 0x%02X, want 0x95", ev.Reason)
			}
			if _, ok := ev.Packet.(*packet.Connect); !ok {
				t.Fatalf("Received %T, want *packet.Connect", ev.Packet)
			}
			b.send(conn, &packet.Connack{ReasonCode: ev.Reason})
			if version == harness.V5 {
				if ack := harness.Expect[*harness.Connack](t, c); ack.Reason != 0x95 {
					t.Errorf("CONNACK 0x%02X, want 0x95", ack.Reason)
				}
			}
			harness.ExpectClosed(t, c)
			b.answerEnd(conn)
			testkit.AwaitSample(t, b.reg, refused("packet_too_large"), 1, backstop)

			// The boundary: exactly the size is read, one byte more is not.
			for _, tc := range []struct {
				size   int
				reason byte
			}{{size, 0}, {size + 1, 0x95}} {
				c, conn := b.accept(version)
				harness.SendBytes(t, c, connectOfSize(t, version, tc.size))
				if ev := b.await(conn, connection.Received); ev.Reason != tc.reason {
					t.Errorf("a CONNECT of %d bytes: Reason 0x%02X, want 0x%02X", tc.size, ev.Reason, tc.reason)
				}
			}
		})
	}
}

// design.md, CONNECT: "none | an unknown protocol level with the Protocol
// Name MQTT, or MQTT 3.1 (MQIsdp, level 3) | 1, sent in MQTT 3.1.1 form",
// and "Any other Protocol Name, a malformed CONNECT [...], or one that
// breaks the protocol closes the connection without a CONNACK (MQTT-3.1.2-1)".
// metrics.md: saguin_connections_refused_total's
// "unsupported_protocol_version (an unknown level included)" and
// "malformed_packet (another Protocol Name included) ... for a connection
// closed before its CONNACK". These hold for a CONNECT over
// max_connect_size too, read no further than its Protocol Name and level
// (design.md, Flows, Packet buffers: "A packet's size is checked before its
// body is read."). Each CONNECT
// announces more than max_connect_size and sends only the bytes shown; the
// client then ends its stream, so a reader reading past what the CONNECT
// announces, or past its Protocol Name and level, finds the end of stream
// and loses the connection instead of refusing it.
func TestOversizeConnectRefusals(t *testing.T) {
	t.Parallel()
	const size = 1024
	// oversize is a CONNECT's fixed header announcing twice size, then rest.
	oversize := func(rest ...byte) []byte {
		return append(harness.AppendVarInt([]byte{0x10}, 2*size), rest...)
	}
	for _, tc := range []struct {
		name   string
		limit  config.Size // max_connect_size
		wire   []byte
		reason byte // 0x84: a CONNACK 1 in MQTT 3.1.1 form; 0x81: closed without one
	}{
		// A body under two bytes holds no Protocol Name length.
		{"a body of one byte", 2, []byte{0x10, 0x01, 0x00}, 0x81},
		// A body too short for the Protocol Name and level it announces.
		{"a body short of its Protocol Name and level", 4, []byte{0x10, 0x05, 0x00, 0x04, 'M', 'Q', 'T'}, 0x81},
		{"a Protocol Name of 7 bytes", size, oversize(0, 7, 'M', 'Q', 'T', 'T', 'M', 'Q', 'T', 4), 0x81},
		{"another Protocol Name of 4 bytes", size, oversize(0, 4, 'M', 'Q', 'T', 'X', 5), 0x81},
		{"another Protocol Name of 6 bytes", size, oversize(0, 6, 'M', 'Q', 'I', 's', 'd', 'x', 3), 0x81},
		{"MQIsdp at level 4", size, oversize(0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 4), 0x81},
		{"MQTT 3.1 (MQIsdp, level 3)", size, oversize(0, 6, 'M', 'Q', 'I', 's', 'd', 'p', 3), 0x84},
		{"an unknown level under MQTT", size, oversize(0, 4, 'M', 'Q', 'T', 'T', 6), 0x84},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newConfig(t)
			cfg.Broker.Limits.MaxConnectSize = tc.limit
			b := start(t, cfg)
			c, conn := b.accept(harness.V311)
			harness.SendBytes(t, c, tc.wire)
			endStream(t, c)
			if tc.reason == 0x84 {
				ev := b.await(conn, connection.Received)
				if ev.Reason != 0x84 {
					t.Fatalf("Reason 0x%02X, want 0x84", ev.Reason)
				}
				b.send(conn, &packet.Connack{ReasonCode: ev.Reason})
				if ack := harness.Expect[*harness.Connack](t, c); ack.Reason != 1 {
					t.Fatalf("CONNACK code 0x%02X, want 1", ack.Reason)
				}
				harness.ExpectClosed(t, c)
				b.answerEnd(conn)
				testkit.AwaitSample(t, b.reg, refused(metrics.ReasonLabel(packet.CONNACK, 0x84)), 1, backstop)
				return
			}
			ev := b.await(conn, connection.Ended)
			if ev.Reason != tc.reason {
				t.Fatalf("Ended with 0x%02X, want 0x%02X", ev.Reason, tc.reason)
			}
			b.answer(&ev)
			harness.ExpectClosed(t, c)
			testkit.AwaitSample(t, b.reg, refused(metrics.ReasonLabel(packet.DISCONNECT, tc.reason)), 1, backstop)
		})
	}
}

// connectOfSize returns a CONNECT of exactly size bytes, fixed header
// included, its password filling it out (with a user name, which MQTT 3.1.1
// requires beside a password).
func connectOfSize(t *testing.T, version byte, size int) []byte {
	t.Helper()
	for n := 0; ; n++ {
		b := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: version, CleanStart: true, ClientID: "c",
			HasUsername: true, Username: "u", HasPassword: true, Password: bytes.Repeat([]byte{'p'}, n)}, version)
		switch {
		case len(b) == size:
			return b
		case len(b) > size:
			t.Fatalf("no CONNECT of exactly %d bytes", size)
		}
	}
}

// design.md, Flows, Replies: "For a packet needing a reply, a reader first
// takes a reply slot from its own writer. The slot is returned once the reply
// is written"; "A reader without a slot stops reading." With every slot held by
// an unanswered PINGREQ, the reader reads nothing, not even a DISCONNECT,
// until one reply is written.
func TestNoReadingWithoutAReplySlot(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))
	c, conn := b.connect(harness.V5, nil)
	ping := harness.Encoded(t, &harness.Pingreq{}, harness.V5)
	wire := bytes.Repeat(ping, connection.ReplySlots)
	wire = append(wire, harness.Encoded(t, &harness.Disconnect{}, harness.V5)...)
	harness.SendBytes(t, c, wire)
	for range connection.ReplySlots {
		if ev := b.await(conn, connection.Received); ev.Packet != (connection.Pingreq{}) {
			t.Fatalf("Received %s, want a PINGREQ", describe(&ev))
		}
	}
	b.quiet(conn, 200*time.Millisecond)

	// One reply written gives one slot back: the reader reads on.
	b.send(conn, connection.Pingresp{})
	harness.Expect[*harness.Pingresp](t, c)
	ev := b.await(conn, connection.Ended)
	if _, ok := ev.Packet.(*packet.Disconnect); !ok {
		t.Fatalf("Ended %s, want the client's DISCONNECT", describe(&ev))
	}
	b.answer(&ev)
	harness.ExpectClosed(t, c)
}

// design.md, DISCONNECT, A lost connection: "a nonzero effective Keep Alive
// gives the reader 1.5 times that interval when it begins reading the next
// packet. Waiting for a reply slot, message reservation or core handover
// does not consume keep-alive time"; "A keep-alive timeout closes the
// connection with 0x8D." A reader paused
// for want of a reply slot longer than 1.5 times the keep-alive is not
// closed; its clock starts again when it reads on.
func TestPausedReaderKeepsNoKeepAliveTime(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))
	c, conn := b.connect(harness.V5, &harness.Connect{CleanStart: true, ClientID: "c", KeepAlive: 1})
	harness.SendBytes(t, c, bytes.Repeat(harness.Encoded(t, &harness.Pingreq{}, harness.V5), connection.ReplySlots))
	for range connection.ReplySlots {
		b.await(conn, connection.Received)
	}
	b.quiet(conn, 2*time.Second)

	resumed := time.Now()
	b.send(conn, connection.Pingresp{})
	ev := b.await(conn, connection.Ended)
	if elapsed := time.Since(resumed); elapsed < 1500*time.Millisecond {
		t.Errorf("closed %v after the reader could read again, before 1.5 times the keep-alive", elapsed)
	}
	if ev.Reason != 0x8D {
		t.Fatalf("Ended with 0x%02X, want 0x8D", ev.Reason)
	}
	b.answer(&ev)
	harness.Expect[*harness.Pingresp](t, c)
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8D {
		t.Errorf("DISCONNECT 0x%02X, want 0x8D", d.Reason)
	}
	harness.ExpectClosed(t, c)
}

// design.md, Execution, Communication: "Once a buffer is sent, its sender
// never touches it again". 0005a, from 0003b: DecodeConnect "reads Binary
// Data in place (Will Payload, password, binary properties), sharing the
// packet's buffer ... So the reader reads each packet into its own buffer and
// never reuses one it has handed on. A test hands a packet to the core, reads
// the next one, and checks the first is unchanged." The next packet, a
// DISCONNECT the reader must decode, has a body of exactly the CONNECT's
// size, so a buffer reused for it, whether kept at the largest size seen or
// fixed, would be overwritten.
func TestHandedPacketIsNeverTouched(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))
	c, conn := b.accept(harness.V5)
	payload, password := bytes.Repeat([]byte{'w'}, 64), bytes.Repeat([]byte{'p'}, 64)
	connect := &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, CleanStart: true, ClientID: "c",
		Will:        &harness.Will{Topic: "status/c", Payload: payload},
		HasPassword: true, Password: password,
	}
	size := len(harness.Encoded(t, connect, harness.V5))
	ev := b.sendConnect(c, conn, connect)
	first := ev.Packet.(*packet.Connect)

	harness.SendBytes(t, c, disconnectOfSize(t, size))
	ev = b.await(conn, connection.Ended)
	if _, ok := ev.Packet.(*packet.Disconnect); !ok {
		t.Fatalf("Ended %s, want the client's DISCONNECT", describe(&ev))
	}
	if !bytes.Equal(first.Will.Payload, payload) || !bytes.Equal(first.Password, password) {
		t.Errorf("the CONNECT handed to the core changed once the next packet was read: Will Payload %q, password %q",
			first.Will.Payload, first.Password)
	}
	b.answer(&ev)
}

// disconnectOfSize returns an MQTT 5 DISCONNECT 0x00 of exactly size bytes,
// fixed header included, a Reason String of 'z' filling it out.
func disconnectOfSize(t *testing.T, size int) []byte {
	t.Helper()
	for n := 0; ; n++ {
		b := harness.Encoded(t, &harness.Disconnect{Props: harness.Properties{{ID: harness.ReasonString, Str: strings.Repeat("z", n)}}}, harness.V5)
		switch {
		case len(b) == size:
			return b
		case len(b) > size:
			t.Fatalf("no DISCONNECT of exactly %d bytes", size)
		}
	}
}

// metrics.md: saguin_disconnects_total's "connection_lost". A client that
// hangs up without a DISCONNECT ends its reader with nothing owed.
func TestLostConnection(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))
	c, conn := b.connect(harness.V5, nil)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	ev := b.await(conn, connection.Ended)
	if ev.Reason != 0 || ev.Packet != nil {
		t.Fatalf("Ended %s, want nothing owed", describe(&ev))
	}
	b.answer(&ev)
	testkit.AwaitSample(t, b.reg, disconnects("connection_lost"), 1, backstop)
	testkit.AwaitSample(t, b.reg, "saguin_connections_open", 0, backstop)
	expectSample(t, b.reg, disconnects("client"), 0)
}

// design.md, Message memory, Asking: "Before allocating or reading a
// nonempty PUBLISH body, its reader requests the body's Remaining Length
// from the core. Every such body uses this request. A reader has at most
// one request or granted body." On the seam, a connected MQTT 5 reader is
// handed a PUBLISH's fixed header, then its body:
//   - a tiny one at QoS 0, of 5 bytes;
//   - a large one at QoS 1, of 3000 bytes, behind a Remaining Length of two
//     bytes.
//
// The reader's first act after the fixed header is its ask for exactly the
// Remaining Length, with no read before it; nothing of the body is read
// until the bytes are given (the test waits until every goroutine of the
// bubble is blocked); once given, the body is read and handed over,
// carrying its size.
func TestReaderAsksBeforeReadingABody(t *testing.T) {
	for _, tc := range []struct {
		name string
		qos  byte
		body int
	}{
		{"a tiny body at QoS 0", 0, 5},
		{"a large body at QoS 1", 1, 3000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := startSeam(t)
				s.connect()
				s.readPublish(s.publishHeader(tc.qos, tc.body))
			})
		})
	}
}

// design.md, Flows, PUBLISH: "The reader records receipt time when the
// complete packet body has arrived, before decoding work or core handover
// delays. That time is carried with the packet." On the seam, a connected
// MQTT 5 reader is handed a QoS 1 PUBLISH's fixed header, asks for its
// body and is given it; the body comes in two reads, the clock moved
// before each, then moved again while the reader waits to hand the
// PUBLISH over. The PUBLISH the core takes carries the clock's reading
// when its last body byte came, in Unix milliseconds, the precision a
// record keeps (PublishMetadata.Received; storage.md, records: time and
// expires): not the header's, the first body byte's or the handover's.
func TestPublishCarriesItsReceiptTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		body := s.publishHeader(1, 64)
		s.awaitAsk(len(body))
		s.grant(len(body))
		s.clock.set(epoch.Add(time.Minute))
		s.feed(body[:10])
		arrived := epoch.Add(time.Minute + 3*time.Second)
		s.clock.set(arrived)
		s.feed(body[10:])
		synctest.Wait() // the reader waits to hand the PUBLISH over
		s.clock.set(arrived.Add(5 * time.Second))
		ev := s.event(connection.Received)
		if _, ok := ev.Packet.(*packet.Publish); !ok {
			t.Fatalf("the core got %s, want the PUBLISH", describe(&ev))
		}
		if ev.Publish.Received != arrived.UnixMilli() {
			t.Errorf("the PUBLISH carries receipt time %d, want %d (%v), when its complete body arrived",
				ev.Publish.Received, arrived.UnixMilli(), arrived)
		}
	})
}

// design.md, Testing: a test inside the code checks "the guards of [...]
// the busy path"; storage.md: "A message's properties are a BLOB holding its
// MQTT 5 properties in their wire form. This is a Property Length followed by
// the properties, as internal/packet writes them (MQTT 5 2.2.2)"; Delivery:
// "Topic Alias is established for the current connection"; MQTT 5 3.3.2.3.4:
// "Topic Alias mappings exist only within a Network Connection". On the seam,
// a connected MQTT 5 reader is handed PUBLISHes with a Topic Name and
// properties: without a Topic Alias; with one and a short User Property; and
// with one and a User Property long enough that the properties take 129 bytes
// with the alias and 126 without it, their Property Length one byte shorter.
// Each PUBLISH it hands the core carries StoredProperties: the properties but
// the Topic Alias, Property Length first, byte for byte, and in the body the
// reader read, not a copy.
func TestPublishCarriesItsStoredProperties(t *testing.T) {
	alias := harness.Property{ID: harness.TopicAlias, Int: 1}
	long := harness.User("step", strings.Repeat("v", 129-3-5-len("step"))) // a User Property's 5 bytes; the alias's 3
	for _, tc := range []struct {
		name       string
		sent, kept harness.Properties
	}{
		{"no Topic Alias", harness.Properties{{ID: harness.ContentType, Str: "text/plain"}, harness.User("k", "v")},
			harness.Properties{{ID: harness.ContentType, Str: "text/plain"}, harness.User("k", "v")}},
		{"a Topic Alias", harness.Properties{harness.User("k", "v"), alias, {ID: harness.ContentType, Str: "text/plain"}},
			harness.Properties{harness.User("k", "v"), {ID: harness.ContentType, Str: "text/plain"}}},
		{"a Topic Alias, the length a byte shorter without it", harness.Properties{alias, long}, harness.Properties{long}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				want, err := harness.AppendProperties(nil, tc.kept)
				if err != nil {
					t.Fatal(err)
				}
				s := startSeam(t)
				s.connect()
				ev := s.readPublish(s.publishOf(&harness.Publish{QoS: 1, ID: 1, Topic: "t", Props: tc.sent, Payload: []byte("payload")}))
				p, ok := ev.Packet.(*packet.Publish)
				if !ok {
					t.Fatalf("the core got %s, want the PUBLISH", describe(&ev))
				}
				if !bytes.Equal(p.StoredProperties, want) {
					t.Fatalf("StoredProperties % X, want % X: the properties but the Topic Alias, Property Length first", p.StoredProperties, want)
				}
				// The Topic Name is the body's: within its capacity lies the
				// rest of the body. A byte changed in StoredProperties shows
				// there only where they share it.
				body := p.Topic[:cap(p.Topic)]
				before := bytes.Clone(body)
				p.StoredProperties[0] ^= 0xFF
				shared := !bytes.Equal(body, before)
				p.StoredProperties[0] ^= 0xFF
				if !shared {
					t.Errorf("StoredProperties is a copy, want the body's own bytes")
				}
			})
		})
	}
}

// design.md, Closing a connection: "The writer writes the last packet,
// ends its sending and stops the reader. The reader stops reading and says
// so before it reports its end to the core"; Message memory: "Before
// allocating or reading a nonempty PUBLISH body, its reader requests the
// body's Remaining Length from the core"; design.md, Execution, Decisions:
// "Go's choice between ready cases" decides nothing. A reader waiting for
// its body's bytes when its writer stops it reads none of the body, even
// once given them. On the seam, a connected MQTT 5 reader asks for a body
// of 65; the core sends its final entry, Close{}, and the writer ends its
// sending and stops the reader; once every goroutine of the bubble is
// blocked, the bytes are given. The reader makes no socket call; the writer
// discards, then closes; the reader's Ended owes nothing.
func TestStopWinsOverBytesGiven(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		body := s.publishHeader(0, 65)
		s.awaitAsk(len(body))
		deadline := s.beginClose(connection.Close{})
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		s.stopped = true // waiting for its bytes, the reader is not in a read: it must call nothing more
		synctest.Wait()
		s.conn.MessageBytes() <- int64(len(body))
		// The writer discards only once the reader has said it stopped.
		s.discardFrom(deadline)
		s.expect(opRead).fail(io.EOF) // the client's end of stream
		s.closed(s.clock.Now())
		if ev := s.event(connection.Ended); ev.Reason != 0 || ev.Packet != nil {
			t.Fatalf("the reader ended with %s, want nothing owed and no PUBLISH", describe(&ev))
		}
		s.event(connection.Closed)
	})
}

// publishHeader hands the reader the fixed header of a PUBLISH at qos
// whose body is body bytes, and returns the body: a Topic Name of 3 bytes,
// at QoS 1 a Packet Identifier, and an empty property section.
func (s *seam) publishHeader(qos byte, body int) []byte {
	s.t.Helper()
	p := &harness.Publish{QoS: qos, Topic: "t"}
	head := 4
	if qos != 0 {
		p.ID, head = 1, head+2
	}
	p.Payload = make([]byte, body-head)
	return s.publishOf(p)
}

// publishOf hands the reader the fixed header of p, an MQTT 5 PUBLISH, and
// returns its body.
func (s *seam) publishOf(p *harness.Publish) []byte {
	s.t.Helper()
	header, body := split(harness.Encoded(s.t, p, harness.V5))
	if s.reading != nil {
		header = header[s.reading.give(header):]
		s.reading = nil
	}
	s.feed(header)
	return body
}

// awaitAsk answers the reader's deadlines until the core gets its ask for
// n bytes, failing on any other call or event.
func (s *seam) awaitAsk(n int) {
	s.t.Helper()
	for {
		select {
		case c := <-s.sock.calls:
			if c.who != reader || c.op != opReadDeadline {
				s.t.Fatalf("the %s called %v before the reader asked for the body's %d bytes", c.who, c, n)
			}
			c.ok()
		case ev := <-s.inbound:
			if ev.Kind != connection.BytesRequested || ev.Bytes != int64(n) {
				s.t.Fatalf("the core got %s, %d bytes; want an ask for the body's %d bytes", describe(&ev), ev.Bytes, n)
			}
			return
		case <-time.After(backstop):
			s.t.Fatalf("the reader neither asked nor called anything within %v of the fixed header", backstop)
		}
	}
}

// awaitBlocked answers the reader's deadlines until every goroutine of the
// bubble is blocked, failing on any other call, which the reader must not
// make at the moment when names.
func (s *seam) awaitBlocked(when string) {
	s.t.Helper()
	for {
		synctest.Wait()
		select {
		case c := <-s.sock.calls:
			if c.who != reader || c.op != opReadDeadline {
				s.t.Fatalf("the %s called %v %s", c.who, c, when)
			}
			c.ok()
		default:
			return
		}
	}
}

// grant gives the reader the n bytes it asked for, once every goroutine of
// the bubble is blocked, failing on any read it makes before them.
func (s *seam) grant(n int) {
	s.t.Helper()
	s.awaitBlocked("before the body's bytes were given")
	s.conn.MessageBytes() <- int64(n)
}

// readPublish plays the core and the client once the reader has read a
// PUBLISH's fixed header, as design.md, Message memory has it: ask, grant
// and read. The reader asks for exactly the body's bytes before reading
// any (awaitAsk), reads nothing until given them (grant), then reads the
// body (readBody); readPublish returns the PUBLISH it hands over.
func (s *seam) readPublish(body []byte) connection.Event {
	s.t.Helper()
	s.awaitAsk(len(body))
	s.grant(len(body))
	return s.readBody(body)
}

// readBody answers the reader's reads with body, and expects the PUBLISH
// handed over, carrying its body's size, which it returns; no event may
// come with any of the body unread.
func (s *seam) readBody(body []byte) connection.Event {
	s.t.Helper()
	for rest := body; len(rest) > 0; {
		select {
		case c := <-s.sock.calls:
			switch {
			case c.who != reader:
				s.t.Fatalf("the writer called %v while the reader read a PUBLISH", c)
			case c.op == opReadDeadline:
				c.ok()
			case c.op == opRead:
				rest = rest[c.give(rest):]
			default:
				s.t.Fatalf("the reader called %v, want a read of the body", c)
			}
		case ev := <-s.inbound:
			s.t.Fatalf("the core got %s, %d bytes, with %d of the %d-byte body unread",
				describe(&ev), ev.Bytes, len(rest), len(body))
		case <-time.After(backstop):
			s.t.Fatalf("the reader made no call within %v with %d of the body unread", backstop, len(rest))
		}
	}
	ev := s.event(connection.Received)
	if _, ok := ev.Packet.(*packet.Publish); !ok || ev.Bytes != int64(len(body)) {
		s.t.Fatalf("the core got %s, %d bytes; want the PUBLISH, carrying its body's %d", describe(&ev), ev.Bytes, len(body))
	}
	return ev
}

// Message memory, A stalled body: "While reading a PUBLISH body, a reader
// that receives no byte for `limits.write_timeout` closes its connection,
// without a DISCONNECT." metrics.md: saguin_disconnects_total counts
// "publish_stalled (no body byte for `limits.write_timeout` after the first
// body-read attempt or the latest body byte, Message memory, A stalled
// body)". What the client sees is the conformance test's
// (TestSilentPublisherCloses); the count is read here until /metrics is
// scraped from outside (0020a). On the seam, a connected MQTT 5 reader is
// handed a QoS 0 PUBLISH's fixed header and part of its body, in two reads
// with the clock moved between them, then nothing: its read waits under a
// deadline write_timeout after the last byte, and the socket ends it once
// the clock reaches that deadline. The reader's end owes no DISCONNECT; the
// core's Close is carried out with nothing written, and the close is
// counted once, under publish_stalled and no other reason.
func TestStalledPublishBodyIsCounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		body := s.publishHeader(0, 64)
		s.awaitAsk(len(body))
		s.grant(len(body))
		s.clock.set(epoch.Add(time.Minute))
		s.feed(body[:10])
		lastByte := epoch.Add(time.Minute + 3*time.Second)
		s.clock.set(lastByte)
		s.feed(body[10:20])

		// The rest of the body never comes: the reader's read waits under a
		// deadline write_timeout after the last byte, which the socket
		// passes.
		var deadline *sockCall
		for stalled := false; !stalled; {
			switch c := s.next(reader); c.op {
			case opReadDeadline:
				deadline = c
				c.ok()
			case opRead:
				if want := lastByte.Add(seamTimeout); deadline == nil || !deadline.t.Equal(want) {
					t.Fatalf("the reader waits for the body's rest under %v, want SetReadDeadline(%v): "+
						"no byte for write_timeout after its last", deadline, want)
				}
				s.clock.set(deadline.t)
				s.stopped = true
				c.fail(deadlinePassed)
				stalled = true
			default:
				t.Fatalf("the reader called %v, want a read of the body's rest", c)
			}
		}

		ev := s.event(connection.Ended)
		if ev.Reason != 0 || ev.Packet != nil {
			t.Fatalf("the reader ended with %s, want nothing owed: the stall closes without a DISCONNECT", describe(&ev))
		}
		s.queue(connection.Close{})
		for !s.shut {
			switch c := s.next(writer); c.op {
			case opWrite, opWriteDeadline:
				t.Fatalf("the writer called %v: the stall's close writes nothing", c)
			case opRead:
				c.fail(io.EOF) // the client's end of stream
			case opClose:
				s.shut = true
				c.ok()
			default:
				c.ok()
			}
		}
		s.event(connection.Closed)

		// Closed comes after the count: every series reads its final value.
		for line := range strings.SplitSeq(testkit.Scrape(t, s.reg), "\n") {
			series, value, _ := strings.Cut(line, " ")
			if !strings.HasPrefix(series, "saguin_disconnects_total{") || series == disconnects("publish_stalled") {
				continue
			}
			if value != "0" {
				t.Errorf("%s = %s, want 0: a stalled PUBLISH body is counted under publish_stalled alone", series, value)
			}
		}
		expectSample(t, s.reg, disconnects("publish_stalled"), 1)
	})
}
