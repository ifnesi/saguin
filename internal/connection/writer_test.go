package connection_test

import (
	"errors"
	"io"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, CONNECT, the refusal table's MQTT 3.1.1 column: 0x84 1, 0x85 2,
// 0x86 4, 0x87 5, 0x90 5, 0x9A 5, 0x9B 5, 0x95 "none: the connection
// closes", 0x83 3, 0x97 3. metrics.md: a reason label is "counted before an
// MQTT 3.1.1 client's code is mapped". MQTT 3.1.1 [MQTT-3.2.2-5]: "If a
// server sends a CONNACK packet containing a non-zero return code it MUST
// then close the Network Connection".
func TestMQTT311ConnackCodes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		reason byte
		code   int // -1: no CONNACK
	}{
		{0x84, 1}, {0x85, 2}, {0x86, 4}, {0x87, 5}, {0x90, 5}, {0x9A, 5}, {0x9B, 5},
		{0x95, -1}, {0x83, 3}, {0x97, 3},
	} {
		label := metrics.ReasonLabel(packet.CONNACK, tc.reason)
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			b := start(t, newConfig(t))
			c, conn := b.accept(harness.V311)
			b.sendConnect(c, conn, connectPacket(harness.V311))
			b.send(conn, &packet.Connack{ReasonCode: tc.reason})
			if tc.code >= 0 {
				if ack := harness.Expect[*harness.Connack](t, c); int(ack.Reason) != tc.code || ack.SessionPresent {
					t.Errorf("CONNACK %+v, want return code %d", ack, tc.code)
				}
			}
			harness.ExpectClosed(t, c)
			b.answerEnd(conn)
			testkit.AwaitSample(t, b.reg, refused(label), 1, backstop)
		})
	}
}

// MQTT 5 [MQTT-3.14.0-1]: "A Server MUST NOT send a DISCONNECT until after
// it has sent a CONNACK with Reason Code of less than 0x80." A DISCONNECT
// the core sends before any CONNACK only closes the connection: before
// CONNECT, and after a CONNECT not yet answered.
func TestNoDisconnectBeforeConnack(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))

	c, conn := b.accept(harness.V5)
	b.send(conn, &packet.Disconnect{ReasonCode: 0x8B})
	harness.ExpectClosed(t, c)
	b.await(conn, connection.Ended)

	c, conn = b.accept(harness.V5)
	b.sendConnect(c, conn, connectPacket(harness.V5))
	b.send(conn, &packet.Disconnect{ReasonCode: 0x8B})
	harness.ExpectClosed(t, c)
	b.await(conn, connection.Ended)
}

// MQTT 5, the peer's Maximum Packet Size: "The Server MUST NOT send packets
// exceeding Maximum Packet Size to the Client [MQTT-3.1.2-24]"; on CONNACK,
// "The Server MUST NOT send this property if it would increase the size of
// the CONNACK packet beyond the Maximum Packet Size specified by the Client"
// for the Reason String [MQTT-3.2.2-19] and User Property [MQTT-3.2.2-20];
// on DISCONNECT the same for both [MQTT-3.14.2-3], [MQTT-3.14.2-4]. A
// property that fits is sent.
func TestPeerMaximumPacketSize(t *testing.T) {
	t.Parallel()
	const limit = 64
	long := strings.Repeat("r", 2*limit)
	for _, tc := range []struct {
		name  string
		reply connection.Reply
		after bool // sent after a CONNACK 0x00, else in place of it
		kept  bool // the property fits and goes out
	}{
		{"CONNACK Reason String", &packet.Connack{Properties: props(packet.CONNACK, packet.Property{ID: packet.ReasonString, String: long})}, false, false},
		{"CONNACK User Property", &packet.Connack{Properties: props(packet.CONNACK,
			packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "u", Value: long}})}, false, false},
		{"CONNACK Reason String that fits", &packet.Connack{Properties: props(packet.CONNACK, packet.Property{ID: packet.ReasonString, String: "ok"})}, false, true},
		{"DISCONNECT Reason String", &packet.Disconnect{ReasonCode: 0x98, Properties: props(packet.DISCONNECT, packet.Property{ID: packet.ReasonString, String: long})}, true, false},
		{"DISCONNECT User Property", &packet.Disconnect{ReasonCode: 0x98, Properties: props(packet.DISCONNECT,
			packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "u", Value: long}})}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := start(t, newConfig(t))
			c, conn := b.accept(harness.V5)
			c.MaxPacketSize = limit // the client refuses a larger packet
			b.sendConnect(c, conn, &harness.Connect{CleanStart: true, ClientID: "c",
				Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: limit}}})
			var props harness.Properties
			if tc.after {
				b.send(conn, &packet.Connack{})
				harness.Expect[*harness.Connack](t, c)
				b.send(conn, tc.reply)
				d := harness.Expect[*harness.Disconnect](t, c)
				if d.Reason != 0x98 {
					t.Errorf("DISCONNECT 0x%02X, want 0x98", d.Reason)
				}
				props = d.Props
			} else {
				b.send(conn, tc.reply)
				ack := harness.Expect[*harness.Connack](t, c)
				if ack.Reason != 0 {
					t.Errorf("CONNACK 0x%02X, want 0x00", ack.Reason)
				}
				props = ack.Props
			}
			if got := len(props) == 1; got != tc.kept {
				t.Errorf("the packet went out with properties %+v; want the property sent: %v", props, tc.kept)
			}
		})
	}
}

// design.md, DISCONNECT: "0x80 for a packet it cannot build, which is never
// sent"; Flows, Logging: "Runtime log-worthy events reach the core through
// their existing event or failure reports". A CONNACK whose Assigned Client
// Identifier alone passes the client's Maximum Packet Size cannot be built:
// nothing is sent, the connection closes, and the writer's close report
// carries the error (connection.CannotBuild), for the core to log.
func TestCannotBuildIsReportedOnTheClose(t *testing.T) {
	t.Parallel()
	const limit = 64
	b := start(t, newConfig(t))
	c, conn := b.accept(harness.V5)
	b.sendConnect(c, conn, &harness.Connect{CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: limit}}})
	b.send(conn, &packet.Connack{Properties: props(packet.CONNACK,
		packet.Property{ID: packet.AssignedClientIdentifier, String: strings.Repeat("a", 2*limit)})})
	harness.ExpectClosed(t, c)
	b.answerEnd(conn)
	if err := b.cannotBuild(conn); err == nil {
		t.Errorf("the close report of a CONNACK the writer could not build carries no error")
	}
}

// design.md, Flows, Closing a connection: "A write that has not finished
// limits.write_timeout after its first socket-write attempt closes the
// connection as writer_full. Partial writes do not extend that deadline."
// design.md, Execution, State ownership: "the write timeout" is one of a
// connection's own deadlines; design.md, DISCONNECT: an MQTT 3.1.1
// connection just closes, "as does a connection closed for a full writer";
// metrics.md: saguin_disconnects_total's "writer_full". design.md, Testing:
// "A deadline test controls or observes the event starting that deadline. A
// watchdog bounds the test and is reported separately; it does not establish
// a broker timeout. Unrelated work need not finish before a legitimate
// deadline." The fake core queues a CONNACK far larger than the sockets'
// buffers to a client that reads nothing: the writer's first write of it
// follows the queueing, so the connection closes no sooner than write_timeout
// after it, with no DISCONNECT and the CONNACK cut short; every wait is
// bounded by backstop, the watchdog.
func TestFullWriterCloses(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))
	c, conn := b.accept(harness.V5)
	b.sendConnect(c, conn, connectPacket(harness.V5))

	value := strings.Repeat("v", 65535)
	values := make([]packet.Property, 512) // 32 MiB of User Properties
	for i := range values {
		values[i] = packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "u", Value: value}}
	}
	huge := &packet.Connack{Properties: props(packet.CONNACK, values...)}
	queued := time.Now()
	b.send(conn, huge)
	ev := b.await(conn, connection.Ended)
	if elapsed := time.Since(queued); elapsed < writeTimeout {
		t.Errorf("closed %v after the CONNACK was queued, before write_timeout's %v", elapsed, writeTimeout)
	}
	b.answer(&ev)

	if err := c.Conn.SetReadDeadline(time.Now().Add(backstop)); err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, c.Conn)
	if err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("reading what the writer sent: %v", err)
	}
	if n >= 32<<20 {
		t.Errorf("the client read %d bytes: the whole CONNACK went out", n)
	}
	testkit.AwaitSample(t, b.reg, disconnects("writer_full"), 1, backstop)
	expectSample(t, b.reg, disconnects("connection_lost"), 0)
}

// design.md, Flows, Closing a connection: "The core cuts a connection only by
// closing its raw socket. It then sends the writer its final entry at once,
// without waiting for the reader's end". A cut sends nothing; the reader
// ends, then the writer reports the close, the socket's count gone. The cut
// is not the writer's orderly close: the client may see an end of stream or a
// reset (audits 261012-002a: "the core's Cut, by their nature").
func TestCut(t *testing.T) {
	t.Parallel()
	b := start(t, newConfig(t))
	c, conn := b.connect(harness.V5, nil)
	conn.Cut()
	b.send(conn, connection.Close{})
	switch p, err := c.Recv(); {
	case err == nil:
		t.Fatalf("%s, want the connection closed", harness.Name(p.Type()))
	case !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET):
		t.Fatalf("reading after the cut: %v, want an end of stream or a reset", err)
	}
	b.await(conn, connection.Ended)
	b.awaitClosed(conn)
	expectSample(t, b.reg, "saguin_connections_open", 0)
}

// R2 and design.md, Flows, Replies: "The core never waits [...] For a packet
// needing a reply, a reader first takes a reply slot from its own writer. The
// slot is returned once the reply is written". connection.go: the replies
// queue holds a reply for each of the ReplySlots slots and the core's final
// entry. On the scripted socket the client sends a PINGREQ for every slot,
// and the writer is held in its write of the first PINGRESP: the core then
// sends every other PINGRESP and its final Close without waiting, the writer
// taking none meanwhile.
func TestRepliesQueueHoldsEverySlotAndTheFinal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		n := s.reading.give(pingreq)
		s.reading = nil
		s.feed(pingreq[n:])
		for i := range connection.ReplySlots {
			if i != 0 {
				s.feed(pingreq)
			}
			if ev := s.event(connection.Received); ev.Packet != (connection.Pingreq{}) {
				t.Fatalf("Received %s, want PINGREQ %d", describe(&ev), i+1)
			}
		}

		s.queue(connection.Pingresp{})
		s.expect(opWriteDeadline).ok()
		s.expect(opWrite) // held: the writer takes nothing more from the queue
		for range connection.ReplySlots - 1 {
			s.queue(connection.Pingresp{})
		}
		s.queue(connection.Close{})
	})
}
