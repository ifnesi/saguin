package connection_test

import (
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// These tests follow a connection on the scripted seam (close_seam_test.go)
// to its writer's close report, the last event of a connection (design.md,
// Execution, Communication: "As its last act, each writer reports on that
// channel that its socket is closed"). R6: "Every resource the broker holds
// has a limit, set by configuration or by the disk, which the core counts";
// design.md, Flows, CONNECT, Connection permits: "the core puts the permit
// back when the writer reports the close"; the core's stand-in here counts
// the one connection from its Accepted to that report.

// connReset and pipeBroken are what a real socket's Read and Write return
// once the peer has reset the connection.
var (
	connReset  = &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	pipeBroken = &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.EPIPE)}
)

// arrive answers the reader's held Read with data, then its next calls
// until it has read all of it.
func (s *seam) arrive(data []byte) {
	s.t.Helper()
	n := s.reading.give(data)
	s.reading = nil
	s.feed(data[n:])
}

// socketClosed reports whether the connection's raw socket is closed: by
// the core's Cut, or by the writer's Close the test has answered.
func (s *seam) socketClosed() bool { return s.shut || s.sock.wasCut() }

// finish plays the client and the core until the connection is quiet: every
// goroutine of the bubble waits, with no socket call or event for the test.
// The client ends its stream: a Read gets an end of stream, every other call
// succeeds; once the socket is closed, every call fails as on a closed
// socket. The core answers each packet (a CONNACK of the CONNECT's Reason, a
// PINGRESP, a QoS 1 PUBLISH's PUBACK, keeping the PUBLISH in s.publishes)
// and an Ended with its final entry, unless it has sent one.
//
// Each step waits until every goroutine is blocked, then answers the
// writer's call, else takes an event, else answers the reader's call: so an
// event the writer sends while the reader still waits in a socket call is
// taken before the reader goes on. finish fails the test unless the core
// takes exactly one close report, after the reader's Ended and with the
// socket closed, and nothing after it.
func (s *seam) finish() {
	s.t.Helper()
	if s.reading != nil {
		s.pending[reader] = s.reading
		s.reading = nil
	}
	for {
		synctest.Wait()
		s.collect()
		if c := s.pending[writer]; c != nil {
			delete(s.pending, writer)
			s.answer(c)
			continue
		}
		select {
		case ev := <-s.inbound:
			s.core(&ev)
			continue
		default:
		}
		if c := s.pending[reader]; c != nil {
			delete(s.pending, reader)
			if s.ended {
				s.t.Fatalf("the reader called %v after its end reached the core", c)
			}
			s.answer(c)
			continue
		}
		break
	}
	if !s.reported {
		s.t.Fatalf("the connection went quiet without its close report (R2: \"each writer also reports, as its last act, that its socket is closed\")")
	}
}

// collect takes every socket call waiting to be made: once all goroutines
// wait, at most one each, as a goroutine waits for its call's answer.
func (s *seam) collect() {
	s.t.Helper()
	for {
		select {
		case c := <-s.sock.calls:
			if s.pending[c.who] != nil {
				s.t.Fatalf("the %s called %v with its call %v unanswered", c.who, c, s.pending[c.who])
			}
			s.pending[c.who] = c
		default:
			return
		}
	}
}

// answer answers c as finish's client does.
func (s *seam) answer(c *sockCall) {
	switch {
	case s.socketClosed():
		c.fail(closedSocket)
	case c.op == opRead:
		c.fail(io.EOF)
	case c.op == opClose:
		s.shut = true
		c.ok()
	default:
		c.ok()
	}
}

// core takes ev as finish's core does.
func (s *seam) core(ev *connection.Event) {
	s.t.Helper()
	switch {
	case ev.Conn != s.conn:
		s.t.Fatalf("%s names another connection", describe(ev))
	case s.reported:
		s.t.Fatalf("%s after the connection's close report, its last event", describe(ev))
	case ev.Kind == connection.Closed:
		if !s.ended {
			s.t.Fatalf("the close report came before the reader's end (R2: \"The writer reports the close " +
				"only after the reader's end has reached the core\")")
		}
		if !s.socketClosed() {
			s.t.Fatalf("the close report came with the socket still open (R2: \"reports ... that its socket is closed\")")
		}
		// The core no longer counts the connection: neither does the gauge.
		if got := testkit.Sample(s.t, s.reg, "saguin_connections_open"); got != 0 {
			s.t.Errorf("saguin_connections_open = %v at the close report, want 0, the connections the core counts", got)
		}
		s.reported = true
	case s.ended:
		s.t.Fatalf("%s after the reader's end: only the close report follows it", describe(ev))
	case ev.Kind == connection.Ended:
		s.ended = true
		switch {
		case s.final:
		case ev.Reason >= 0x80:
			s.queue(&packet.Disconnect{ReasonCode: ev.Reason})
		default:
			s.queue(connection.Close{})
		}
	case ev.Kind != connection.Received:
		s.t.Fatalf("%s, want a packet, the reader's end or the close report", describe(ev))
	case ev.Packet == connection.Pingreq{}:
		s.queue(connection.Pingresp{})
	default:
		switch p := ev.Packet.(type) {
		case *packet.Publish:
			s.publishes = append(s.publishes, p)
			if p.QoS == 1 {
				s.queue(&connection.Ack{Type: packet.PUBACK, PacketID: p.PacketID})
			}
		case *packet.Connect:
			s.queue(&packet.Connack{ReasonCode: ev.Reason})
		default:
			s.t.Fatalf("Received %s, want a CONNECT, a PINGREQ or a PUBLISH", describe(ev))
		}
	}
}

// design.md, Execution, Communication, and Flows, Closing a connection:
// "As its last act, each writer reports on that channel that its socket is
// closed. [...] only after the reader's end has reached the core, so a
// connection's events arrive in one order: accepted, its packets, its end,
// its close"; design.md, Flows, CONNECT, Connection permits: "the core puts
// the permit back when the writer reports the close". Every way a connection
// ends gives the core exactly one close report, Closed, after the reader's
// Ended, with the raw socket closed and saguin_connections_open at 0; nothing
// comes after it.
func TestEveryCloseIsReportedOnceAfterTheEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(s *seam) // brings the connection to its end
	}{
		{"server DISCONNECT", func(s *seam) {
			s.connect()
			s.queue(&packet.Disconnect{ReasonCode: 0x98})
		}},
		{"refusing CONNACK", func(s *seam) {
			s.feed(harness.Encoded(s.t, connectPacket(harness.V5), harness.V5))
			s.event(connection.Received)
			s.queue(&packet.Connack{ReasonCode: 0x85}) // a client id refused
		}},
		{"client DISCONNECT", func(s *seam) {
			s.connect()
			s.arrive(harness.Encoded(s.t, &harness.Disconnect{}, harness.V5))
		}},
		{"client end of stream", func(s *seam) {
			s.connect()
			s.reading.fail(io.EOF)
			s.reading = nil
		}},
		{"read error", func(s *seam) {
			s.connect()
			s.reading.fail(connReset)
			s.reading = nil
		}},
		{"write error", func(s *seam) {
			s.connect()
			s.arrive(pingreq)
			s.event(connection.Received)
			s.queue(connection.Pingresp{})
			s.expect(opWriteDeadline).ok()
			s.expect(opWrite).fail(pipeBroken)
		}},
		{"connect timeout", func(s *seam) {
			s.hold()
			s.clock.set(epoch.Add(longConnectTimeout))
			s.reading.fail(deadlinePassed)
			s.reading = nil
		}},
		{"the close's deadline", func(s *seam) {
			// The client never answers: the reader's read stays under way
			// until the socket is closed at the deadline, so the writer has
			// closed its socket while the reader has not yet reported its
			// end; finish takes any close report before it answers that
			// read.
			s.connect()
			deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x98})
			s.lastPacket(deadline, s.clock.Now(), 0x98)
			s.expect(opCloseWrite).ok()
			s.interrupt().ok()
			synctest.Wait() // the writer waits for the reader, or holds out its next call
			s.clock.set(deadline)
		}},
		{"cut", func(s *seam) {
			s.connect()
			s.conn.Cut()
			s.queue(connection.Close{})
		}},
		{"shutdown", func(s *seam) {
			// design.md, Restart, Graceful stop: "every client gets
			// 0x8B ...; after limits.write_timeout from when the core accepts
			// the stop, the remaining sockets are cut". The
			// client sends nothing more and never ends its stream.
			s.connect()
			deadline := s.beginClose(&packet.Disconnect{ReasonCode: 0x8B})
			s.expect(opWriteDeadline).ok()
			s.expect(opWrite).ok()
			s.expect(opCloseWrite).ok()
			s.interrupt().ok()
			s.stopReader(deadlinePassed)
			s.discardFrom(deadline)
			r := s.expect(opRead)
			s.clock.set(deadline)
			s.conn.Cut()
			r.fail(closedSocket)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := startSeam(t)
				tc.end(s)
				s.finish()
			})
		})
	}
}

// design.md, Flows, Replies: "For a packet needing a reply, a reader first
// takes a reply slot from its own writer. [...] A reader without a slot stops
// reading"; Flows, Closing a connection: "The core cuts a connection only by
// closing its raw socket. It then sends the writer its final entry at once,
// without waiting for the reader's end". The client sends a PINGREQ for every
// reply slot and the core answers none: the reader waits for a slot, reading
// nothing. The core cuts the connection and sends Close at once: the reader
// reports its end, then the writer its close.
func TestCutWithEveryReplySlotSpentEndsTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		s.arrive(pingreq)
		for i := range connection.ReplySlots {
			if i != 0 {
				s.feed(pingreq)
			}
			if ev := s.event(connection.Received); ev.Packet != (connection.Pingreq{}) {
				t.Fatalf("Received %s, want PINGREQ %d", describe(&ev), i+1)
			}
		}
		synctest.Wait() // the reader waits for a slot
		select {
		case c := <-s.sock.calls:
			t.Fatalf("the %s called %v with every reply slot spent, want the reader waiting for a slot", c.who, c)
		default:
		}

		s.conn.Cut()
		s.queue(connection.Close{})
		s.finish()
	})
}

// A takeover of a connection whose reader waits for a reply slot ends that
// connection all the same. design.md, CONNECT, 5: "An old connection for the
// id gets DISCONNECT 0x8E on MQTT 5 and is closed"; MQTT 5 [MQTT-3.1.4-3]:
// "the Server sends a DISCONNECT packet to the existing Client with Reason
// Code of 0x8E (Session taken over) ... and MUST close the Network Connection
// of the existing Client". design.md, Flows, Closing a connection: the core
// sends a connection's final entry "at once, without waiting for the reader's
// end" (audit finding 261013-008a: a reader paused for a slot sends no Ended
// until its writer stops it), and "It keeps its first recorded cause. [...]
// When the close begins, it fixes one deadline limits.write_timeout later.
// Nothing extends that deadline".
//
// The real core decides (core.Handle, its time the fake clock's), and a
// store holding no session answers each of its reads at once. The old
// MQTT 5 client sends a PINGREQ for every reply slot and reads nothing: its
// writer's first PINGRESP write waits, so no slot comes back and the reader
// waits for one. A new connection's CONNECT for its client id is answered
// CONNACK 0x00 in the same decision that gives the old connection its
// final entry. The client then reads its PINGRESPs and never ends its
// stream: the writer writes DISCONNECT 0x8E, stops the reader, which
// reports its end, and closes the socket at the close's deadline,
// write_timeout after the takeover, when the close report reaches the
// core. The old connection's reader and writer have ended; once the new
// connection has gone too, saguin_connections_open is 0.
func TestTakeoverOfAReaderWaitingForASlotEndsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newConfig(t)
		s := startSeam(t)
		c := core.New(cfg, s.reg, &store.State{}, nil, nil, nil, logger.NewCredits(cfg.Broker.LogLevel, s.reg))
		others := map[*connection.Conn][]connection.Reply{} // what the core sends the connections with no socket here
		var carryOut func(acts []core.Action)
		carryOut = func(acts []core.Action) {
			for _, a := range acts {
				switch {
				case a.Read != nil: // a LoadSession, which finds no session, its view confirmed first
					carryOut(c.Answer(nil, store.Answer{Op: a.Read, View: true}, s.clock.Now()))
					carryOut(c.Answer(nil, store.Answer{Op: a.Read}, s.clock.Now()))
				case a.Write != nil:
					t.Fatalf("the core wrote %T, for sessions that end with their connections", a.Write)
				case a.Conn == s.conn:
					s.queue(a.Reply)
				default:
					others[a.Conn] = append(others[a.Conn], a.Reply)
				}
			}
		}
		s.decide = func(ev connection.Event) { carryOut(c.Handle(nil, ev, s.clock.Now())) }
		// connectOther hands the core a connection with no socket and its
		// CONNECT for "client", and returns its CONNACK's code.
		connectOther := func() (*connection.Conn, byte) {
			conn := new(connection.Conn)
			s.decide(connection.Event{Kind: connection.Accepted, Conn: conn,
				Packet: connection.Accept{Listener: "other", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 50000}}})
			s.decide(connection.Event{Kind: connection.Received, Conn: conn,
				Packet: decodedConnect(t, connectPacket(harness.V5))})
			if ack, ok := others[conn][0].(*packet.Connack); ok && len(others[conn]) == 1 {
				return conn, ack.ReasonCode
			}
			t.Fatalf("the CONNECT was answered with %v, want a CONNACK alone", others[conn])
			return nil, 0
		}

		s.decide(connection.Event{Kind: connection.Accepted, Conn: s.conn,
			Packet: connection.Accept{Listener: s.conn.Listener(), Remote: s.conn.RemoteAddr()}})
		s.connect() // connectPacket's client id, "client"
		s.arrive(pingreq)
		var unread *sockCall // the first PINGRESP's write: the client reads nothing
		for i := range connection.ReplySlots {
			if i != 0 {
				s.feed(pingreq)
			}
			s.decide(s.event(connection.Received))
			if i == 0 {
				s.expect(opWriteDeadline).ok()
				unread = s.expect(opWrite)
			}
		}
		synctest.Wait() // the reader waits for a slot
		select {
		case call := <-s.sock.calls:
			t.Fatalf("the %s called %v with every reply slot spent, want the reader waiting for a slot", call.who, call)
		default:
		}

		takeover := s.clock.Now()
		deadline := takeover.Add(seamTimeout)
		_, code := connectOther()
		if code != 0 || !s.final {
			t.Fatalf("the takeover: the new connection's CONNACK 0x%02X, the old one's final entry sent %v; want 0x00 and the final entry at once", code, s.final)
		}

		unread.ok()
		for range connection.ReplySlots - 1 {
			s.expect(opWriteDeadline).ok()
			s.expect(opWrite).ok()
		}
		s.hold() // the reader, its slots back, reads; the client sends nothing
		s.lastPacket(deadline, takeover, 0x8E)
		s.expect(opCloseWrite).ok()
		s.interrupt().ok()
		s.stopReader(deadlinePassed)
		s.discardFrom(deadline)
		r := s.expect(opRead)
		s.decide(s.event(connection.Ended))
		s.clock.set(deadline)
		r.fail(deadlinePassed)
		s.closed(deadline)
		s.decide(s.event(connection.Closed))
		synctest.Wait()
		if left := testkit.Started(s.runID, module); len(left) != 0 {
			t.Errorf("goroutines left after the old connection's close report: %v, want its reader and writer ended", left)
		}

		expectSample(t, s.reg, "saguin_connections_open", 0)
	})
}

// decodedConnect is c written and read back by the packet package
// (packet.DecodeConnect of its body), as a reader hands it to the core:
// carrying every field the decoder gives.
func decodedConnect(t *testing.T, c *harness.Connect) *packet.Connect {
	t.Helper()
	_, body := split(harness.Encoded(t, c, c.Version))
	p, err := packet.DecodeConnect(body)
	if err != nil {
		t.Fatalf("CONNECT %+v read back: %v", c, err)
	}
	return &p
}
