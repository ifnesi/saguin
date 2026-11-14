package connection_test

import (
	"bytes"
	"errors"
	"net"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// longWriteTimeout is write_timeout where a test's client stops in time:
// an hour, so that the close's discard can end only at the client's end of
// stream, never at the deadline, however loaded the machine (design.md,
// Flows, Closing a connection: "A client that stops in time sees an orderly
// close").
const longWriteTimeout = time.Hour

// endStream ends c's stream: its half-close sends the broker an end of
// stream, and c can still read.
func endStream(t testing.TB, c *harness.Client) {
	t.Helper()
	if err := c.Conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("ending the client's stream: %v", err)
	}
}

// What these tests can see. TCP answers a close with bytes still unread in
// the socket with a reset, not an end of stream, and a reset discards what
// of the send queue is not yet on the wire (audits 261012-002a). Over a real
// link the CONNACK or DISCONNECT just written may still be in that queue,
// or in flight unacknowledged, and is then lost: the client sees a reset
// where it was owed the reason. Over loopback a write is on the peer's side
// at once, unless the peer's receive window is full; a half-close's end of
// stream then still beats the reset, so the end of stream alone does not
// show that the client's bytes were read before the close (audits
// 261012-005a). The tests arm what a real link does: a client whose receive
// window is as small as the kernel allows, which reads nothing until the
// broker's socket is closed (saguin_connections_open is 0), so the broker's
// last packets are still queued in its socket at the close. A reset there
// loses them; an orderly close delivers them, then the end of stream.
// Where nothing can be queued, the reset still reaches the client's socket
// after the end of stream: expectNoReset reads the socket's pending error,
// which a reset sets, so the client must find none.

// smallWindow is the client's receive buffer, set before it connects: the
// kernel raises it to its own floor, a window of a few KiB at most.
const smallWindow = 1024

// fillReply is the size of the replies a test queues behind smallWindow:
// far more than the window holds, and well within the broker's send buffer,
// so its write never waits.
const fillReply = 12 << 10

// dialSmallWindow opens a client connection to b's first listener with its
// receive buffer set to smallWindow before it connects, and returns it
// with the connection the core was told of.
func (b *broker) dialSmallWindow(version byte) (*harness.Client, *connection.Conn) {
	b.t.Helper()
	return b.dialWindow(version, smallWindow)
}

// dialWindow is dialSmallWindow with a receive buffer of rcvbuf bytes.
func (b *broker) dialWindow(version byte, rcvbuf int) (*harness.Client, *connection.Conn) {
	b.t.Helper()
	d := net.Dialer{Timeout: backstop, Control: func(_, _ string, rc syscall.RawConn) error {
		var err error
		ctrl := rc.Control(func(fd uintptr) {
			err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, rcvbuf)
		})
		return errors.Join(ctrl, err)
	}}
	raw, err := d.Dial("tcp", b.addr)
	if err != nil {
		b.t.Fatalf("dial %s: %v", b.addr, err)
	}
	// The kernel reports the buffer it set, its floor included: the window
	// is at most that, and the fill must not fit in it.
	if got := sockopt(b.t, raw, syscall.SO_RCVBUF); got >= fillReply {
		b.t.Fatalf("the client's receive buffer is %d bytes, at least the %d a test queues behind it: nothing would stay queued", got, fillReply)
	}
	c := harness.NewClient(raw, version)
	c.Timeout = backstop
	b.clients = append(b.clients, c)
	ev := b.await(nil, connection.Accepted)
	return c, ev.Conn
}

// bigUnread is the most input a test leaves unread behind the packet that
// ends a connection: sixteen times a 4 KiB buffer, and within what the
// broker's receive buffer and the client's send buffer hold, so the
// client's write never waits for the broker to read.
const bigUnread = 64 << 10

// sockopt returns the integer socket option opt of conn, a TCP connection.
func sockopt(t testing.TB, conn net.Conn, opt int) int {
	t.Helper()
	rc, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var serr error
	if err := rc.Control(func(fd uintptr) {
		v, serr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, opt)
	}); err != nil {
		t.Fatal(err)
	}
	if serr != nil {
		t.Fatalf("reading socket option %d: %v", opt, serr)
	}
	return v
}

// expectNoReset checks, once the client has read the end of stream, that
// its socket holds no error: a reset the broker sent after its end of
// stream sets one (ECONNRESET, or EPIPE once the end of stream was seen),
// which the end of stream read first does not show.
func expectNoReset(t testing.TB, c *harness.Client) {
	t.Helper()
	if errno := sockopt(t, c.Conn, syscall.SO_ERROR); errno != 0 {
		t.Errorf("the client's socket holds %v after the end of stream: the broker reset the connection", syscall.Errno(errno))
	}
}

// fillPings is how many PINGRESPs fillWindow queues: fillReply bytes.
const fillPings = fillReply / 2

// fillWindow has the broker answer fillPings PINGREQs that c sends and
// does not read the answers to: with c's small window, most of them stay
// queued in the broker's socket. The client reads them with readFill.
func (b *broker) fillWindow(c *harness.Client, conn *connection.Conn) {
	b.t.Helper()
	harness.SendBytes(b.t, c, bytes.Repeat(harness.Encoded(b.t, &harness.Pingreq{}, c.Version), fillPings))
	for range fillPings {
		b.await(conn, connection.Received)
		b.send(conn, connection.Pingresp{})
	}
}

// readFill reads the PINGRESPs fillWindow queued.
func readFill(t *testing.T, c *harness.Client) {
	t.Helper()
	for range fillPings {
		harness.Expect[*harness.Pingresp](t, c)
	}
}

// refusal returns the CONNACK refusing with reason: in MQTT 5 it carries a
// User Property of fillReply bytes, so that it cannot be all on the wire
// before the close; MQTT 3.1.1's has no properties.
func refusal(version, reason byte) *packet.Connack {
	ack := &packet.Connack{ReasonCode: reason}
	if version == harness.V5 {
		ack.Properties = props(packet.CONNACK, packet.Property{ID: packet.UserProperty,
			Pair: packet.StringPair{Name: "fill", Value: strings.Repeat("v", fillReply)}})
	}
	return ack
}

// design.md, Flows, Closing a connection: "The writer handles the DISCONNECT
// and orderly close"; "It keeps its first recorded cause. [...] When the
// close begins, it fixes one deadline limits.write_timeout later. Nothing
// extends that deadline. A refusal's close begins when the writer takes the
// refusing CONNACK." design.md, CONNECT, 2: "A failure gets a CONNACK with its reason,
// then the connection closes"; MQTT 5 [MQTT-3.2.2-7] and MQTT 3.1.1
// [MQTT-3.2.2-5]: after a refusing CONNACK the server "MUST then close the
// Network Connection". MQTT 5 [MQTT-3.14.4-2]: after DISCONNECT the sender
// "MUST close the Network Connection"; 4.13.1: on a Malformed Packet or
// Protocol Error the server SHOULD send DISCONNECT "before closing the
// Network Connection"; MQTT 3.1.1 [MQTT-4.8.0-1] closes. MQTT 5 3.1.4:
// "Clients are allowed to send further MQTT Control Packets immediately after
// sending a CONNECT packet", and [MQTT-3.1.4-6] forbids processing them, not
// reading them. Audits 261012-002a: the writer's close is a reset whenever
// the client's bytes were left unread. In each shape the finding names, the
// client's bytes are left unread behind the packet that ends the connection,
// and the broker's last packets are queued behind the client's small window
// (audits 261012-005a): before a refusal, a CONNACK of fillReply bytes (MQTT
// 5); after a CONNACK, fillPings PINGRESPs. The client reads only once the
// broker's socket is closed, and receives every packet, the refusal's CONNACK
// or the server's DISCONNECT last, then an end of stream, never a reset; the
// close is counted as before, never as a lost connection. Before its CONNACK
// the broker sends an MQTT 3.1.1 client nothing but a CONNACK of 4 bytes, so
// those refusals cannot queue anything behind the window: there the reset
// shows only as the error it leaves on the client's socket, which
// expectNoReset reads in every case. Two more endings close with the client's
// bytes unread (GPT Sol 6.1's review of 261012-005a): the client's own
// DISCONNECT with more behind it, which the core answers with Close
// (design.md, DISCONNECT, "a clean DISCONNECT"; MQTT 5 [MQTT-3.14.4-2] binds
// the client, and the server closes on its receipt), counted as client; and a
// CONNECT the reader passes and the core refuses (0x86, design.md, CONNECT,
// 2), its reader reading on behind it until the close stops it. In every case
// the client ends its stream behind its bytes and write_timeout is
// longWriteTimeout: design.md, Flows, Closing a connection, "A client that
// stops in time sees an orderly close".
func TestOrderlyCloseWithBytesUnread(t *testing.T) {
	t.Parallel()
	const limit = 1024 // max_connect_size and max_packet_size
	ping := func(t *testing.T, v byte) []byte { return harness.Encoded(t, &harness.Pingreq{}, v) }
	for _, tc := range []struct {
		name      string
		connected bool // a CONNACK 0x00 goes out first
		wire      func(t *testing.T, version byte) []byte
		reason    byte // the CONNECT's refusal, or the DISCONNECT the reader's end owes
		code311   int  // the MQTT 3.1.1 CONNACK's return code; -1: none
		count     string
		core      bool // the reader passes the CONNECT and the core refuses it with reason
	}{
		// A refused CONNECT with a PINGREQ pipelined in the same write.
		{"refused CONNECT with a packet behind it", false, func(t *testing.T, v byte) []byte {
			return append(harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: v, ClientID: ""}, v), ping(t, v)...)
		}, 0x85, 2, refused("client_identifier_not_valid"), false},
		// design.md, CONNECT: "0x95 | a CONNECT over limits.max_connect_size,
		// sent without the client's Maximum Packet Size, which is in the
		// unread body | none: the connection closes"; the reader refuses it from its
		// fixed header, its body unread.
		{"oversize CONNECT sent whole", false, func(t *testing.T, v byte) []byte {
			return connectOfSize(t, v, 4*limit)
		}, 0x95, -1, refused("packet_too_large"), false},
		// design.md, Flows, Packet buffers: "A packet's size is checked
		// before its body is read."
		{"oversize packet with its body", true, func(_ *testing.T, _ byte) []byte {
			wire := harness.AppendVarInt([]byte{0x30}, 2*limit)
			return append(wire, make([]byte, 2*limit)...)
		}, 0x95, -1, disconnects("packet_too_large"), false},
		// A PINGREQ with a body is malformed from its fixed header, the
		// body unread ([MQTT-2.1.3-1] and 3.12: no body).
		{"malformed packet with more behind it", true, func(t *testing.T, v byte) []byte {
			return append([]byte{0xC0, 0x01, 0x00}, ping(t, v)...)
		}, 0x81, -1, disconnects("malformed_packet"), false},
		// A second CONNECT is a Protocol Error ([MQTT-3.1.0-2]) from its
		// fixed header, its body unread.
		{"protocol error with more behind it", true, func(t *testing.T, v byte) []byte {
			return append(harness.Encoded(t, connectPacket(v), v), ping(t, v)...)
		}, 0x82, -1, disconnects("protocol_error"), false},
		// The client's DISCONNECT ends the reader, a PINGREQ unread behind
		// it; the core's answer is Close.
		{"client DISCONNECT with more behind it", true, func(t *testing.T, v byte) []byte {
			return append(harness.Encoded(t, &harness.Disconnect{}, v), ping(t, v)...)
		}, 0x00, -1, disconnects("client"), false},
		// A CONNECT every check of the reader passes, with more PINGREQs
		// behind it than the reader has reply slots for: the reader reads on
		// while the core refuses it, and some stay unread.
		{"core refusal with the reader reading on", false, func(t *testing.T, v byte) []byte {
			pings := bytes.Repeat(ping(t, v), connection.ReplySlots+1)
			return append(harness.Encoded(t, connectPacket(v), v), pings...)
		}, 0x86, 4, refused("bad_user_name_or_password"), true},
		// Unread input far beyond any small buffer a close could discard
		// and stop at (GPT Sol 6.1's second review of 261012-005a: a drain
		// of 4,096 bytes passed every case above): an oversize CONNECT of
		// bigUnread bytes, read only to its protocol level, and a packet
		// whose bigUnread-byte body is never read.
		{"oversize CONNECT of 64 KiB sent whole", false, func(_ *testing.T, v byte) []byte {
			wire := harness.AppendVarInt([]byte{0x10}, bigUnread)
			wire = append(wire, 0, 4, 'M', 'Q', 'T', 'T', v)
			return append(wire, make([]byte, bigUnread-7)...)
		}, 0x95, -1, refused("packet_too_large"), false},
		{"oversize packet with a 64 KiB body", true, func(_ *testing.T, _ byte) []byte {
			wire := harness.AppendVarInt([]byte{0x30}, bigUnread)
			return append(wire, make([]byte, bigUnread)...)
		}, 0x95, -1, disconnects("packet_too_large"), false},
	} {
		for _, version := range []byte{harness.V5, harness.V311} {
			t.Run(tc.name+", "+harness.VersionName(version), func(t *testing.T) {
				t.Parallel()
				cfg := newConfig(t)
				cfg.Broker.Limits.MaxConnectSize = limit
				cfg.Broker.Limits.MaxPacketSize = limit
				cfg.Broker.Limits.WriteTimeout = longWriteTimeout
				b := start(t, cfg)

				c, conn := b.dialSmallWindow(version)
				if tc.connected {
					if ev := b.sendConnect(c, conn, connectPacket(version)); ev.Reason != 0 {
						t.Fatalf("CONNECT refused with 0x%02X, want 0x00", ev.Reason)
					}
					b.send(conn, &packet.Connack{})
					harness.Expect[*harness.Connack](t, c)
					b.fillWindow(c, conn)
				}
				harness.SendBytes(t, c, tc.wire(t, version))
				endStream(t, c)
				var ev connection.Event
				switch {
				case tc.core:
					if ev := b.await(conn, connection.Received); ev.Reason != 0 {
						t.Fatalf("Reason 0x%02X, want 0x00: the core refuses", ev.Reason)
					}
					b.send(conn, refusal(version, tc.reason))
					ev = b.awaitEnd(conn)
				case !tc.connected:
					ev = b.await(conn, connection.Received)
					if ev.Reason != tc.reason {
						t.Fatalf("Reason 0x%02X, want 0x%02X", ev.Reason, tc.reason)
					}
					b.send(conn, refusal(version, ev.Reason))
					ev = b.await(conn, connection.Ended)
				default:
					ev = b.await(conn, connection.Ended)
					if ev.Reason != tc.reason {
						t.Fatalf("Ended with 0x%02X, want 0x%02X", ev.Reason, tc.reason)
					}
					if _, ok := ev.Packet.(*packet.Disconnect); tc.reason == 0 && !ok {
						t.Fatalf("Ended %s, want the client's DISCONNECT", describe(&ev))
					}
				}
				b.answer(&ev)
				testkit.AwaitSample(t, b.reg, "saguin_connections_open", 0, backstop)

				if tc.connected {
					readFill(t, c)
				}
				switch {
				case !tc.connected && version == harness.V5:
					ack := harness.Expect[*harness.Connack](t, c)
					if fill, _ := ack.Props.Get(harness.UserProperty); ack.Reason != tc.reason || len(fill.Value) != fillReply {
						t.Errorf("CONNACK 0x%02X with a User Property of %d bytes, want 0x%02X with %d", ack.Reason, len(fill.Value), tc.reason, fillReply)
					}
				case !tc.connected && tc.code311 >= 0:
					if ack := harness.Expect[*harness.Connack](t, c); int(ack.Reason) != tc.code311 {
						t.Errorf("CONNACK return code %d, want %d", ack.Reason, tc.code311)
					}
				case tc.connected && version == harness.V5 && tc.reason >= 0x80:
					if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != tc.reason {
						t.Errorf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, tc.reason)
					}
				}
				harness.ExpectClosed(t, c)
				expectNoReset(t, c)

				testkit.AwaitSample(t, b.reg, tc.count, 1, backstop)
				expectSample(t, b.reg, disconnects("connection_lost"), 0)
				expectSample(t, b.reg, disconnects("writer_full"), 0)
			})
		}
	}
}

// awaitEnd returns conn's Ended, passing over the PINGREQs its reader still
// hands on before it, and fails on any other event first. design.md, Flows,
// Closing a connection: "a connection's events arrive in one order:
// accepted, its packets, its end, its close". A close the core begins does
// not stop the reader at once: "the writer writes the last packet, ends its
// sending and stops the reader", so a PINGREQ read before that may reach
// the core after the core's final reply. After a CONNECT the core refuses,
// the core ignores them: MQTT 5 [MQTT-3.1.4-6], MQTT 3.1.1 [MQTT-3.1.4-5],
// "it MUST NOT process any data sent by the Client after the CONNECT".
// Nothing comes after the Ended but the close report, which take checks:
// "once it has reported its end, it passes nothing more to the core."
func (b *broker) awaitEnd(conn *connection.Conn) connection.Event {
	b.t.Helper()
	for {
		var ev connection.Event
		if i := slices.IndexFunc(b.backlog, func(e connection.Event) bool { return e.Conn == conn }); i >= 0 {
			ev = b.backlog[i]
			b.backlog = slices.Delete(b.backlog, i, i+1)
		} else {
			select {
			case ev = <-b.inbound:
				b.take(&ev)
			case <-time.After(backstop):
				b.t.Fatalf("no Ended within %v", backstop)
			}
			if ev.Conn != conn {
				b.backlog = append(b.backlog, ev)
				continue
			}
		}
		switch {
		case ev.Kind == connection.Ended:
			return ev
		case ev.Kind != connection.Received || ev.Packet != (connection.Pingreq{}):
			b.t.Fatalf("%s, want a PINGREQ or Ended", describe(&ev))
		}
	}
}

// keepSending writes p every 2 milliseconds, as a client sending steadily
// does, until a write fails because the broker closed the socket: its reset
// makes the next write fail with a reset or a broken pipe. Its pace is the
// client's, not a wait for an event.
func keepSending(t *testing.T, c *harness.Client, p []byte) {
	t.Helper()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(backstop)
	for {
		select {
		case <-tick.C:
		case <-deadline:
			t.Fatalf("the client went on sending for %v and the connection is still open", backstop)
		}
		err := c.SendRaw(p)
		switch {
		case err == nil:
		case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
			return
		default:
			t.Fatalf("sending: %v, want the broker's close: a reset or a broken pipe", err)
		}
	}
}

// design.md, Flows, Closing a connection: "It keeps its first recorded cause.
// [...] When the close begins, it fixes one deadline limits.write_timeout
// later. Nothing extends that deadline"; "at the deadline the socket is
// closed whatever is left". The server's DISCONNECT 0x98 (MQTT 3.1.1: the close
// alone) goes to a client that never stops sending, a PINGREQ every 2
// milliseconds, and never closes: no end of stream comes, so the close lasts
// until its deadline, and the client's sends succeed until the broker's
// socket is closed and resets the connection. That comes no earlier than
// write_timeout after the DISCONNECT was queued, as the close begins when the
// writer takes it. How much later it comes is the machine's load, which no
// test asserts. The scripted tests (close_seam_test.go) answer the deadline
// themselves; this one checks it on a real socket, under the real clock, and
// the reset TCP sends there.
func TestCloseLastsTheWriteTimeout(t *testing.T) {
	t.Parallel()
	const timeout = time.Second
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			t.Parallel()
			cfg := newConfig(t)
			cfg.Broker.Limits.WriteTimeout = timeout
			b := start(t, cfg)
			c, conn := b.connect(version, nil)
			ping := harness.Encoded(t, &harness.Pingreq{}, version)

			sent := time.Now()
			b.send(conn, &packet.Disconnect{ReasonCode: 0x98})
			keepSending(t, c, ping)
			if elapsed := time.Since(sent); elapsed < timeout {
				t.Errorf("the close ended %v after the DISCONNECT, before write_timeout's %v", elapsed, timeout)
			}
			// design.md, Flows, Closing a connection, 1: "The writer writes
			// the last packet, ends its sending and stops the reader":
			// PINGREQs it read before may come first.
			b.awaitEnd(conn)
			testkit.AwaitSample(t, b.reg, "saguin_connections_open", 0, backstop)
			testkit.AwaitSample(t, b.reg, disconnects("administrative_action"), 1, backstop)
			expectSample(t, b.reg, disconnects("connection_lost"), 0)
			expectSample(t, b.reg, disconnects("writer_full"), 0)
		})
	}
}
