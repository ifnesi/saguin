package connection_test

import (
	"fmt"
	"os"
	"runtime"
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

// The API these tests fix:
//
//	const ReplySlots // each writer's reply slots (design.md, Execution,
//	Capacities: "Each connection has 64 reply slots and 64 answer
//	allowances. [...] These are fixed, not configuration")
//
//	type Kind uint8
//	const (
//		Accepted Kind = iota + 1 // the listener accepted Conn and started its reader and writer
//		Received                 // Conn's reader read and checked Packet
//		Ended                    // Conn's reader stopped: its last event
//		Closed                   // Conn's writer closed its socket: the connection's last event, after Ended
//	)
//	type Event struct {
//		Kind   Kind
//		Conn   *Conn
//		Packet any  // Accepted: Accept; Received: *packet.Connect or Pingreq{}; Ended: the client's *packet.Disconnect; Closed: CannotBuild or nil; else nil
//		Reason byte // Received CONNECT: 0x00, or the refusal its checks found; Ended: the DISCONNECT reason owed; else 0x00
//	}
//	type CannotBuild struct{ Err error } // a Closed event's Packet: the error of a reply the writer could not build; else nil
//	type Accept struct { // an Accepted event's Packet: what the core stores of the connection (storage.md, sessions)
//		Listener string   // the name of the listener that accepted it
//		Remote   net.Addr // the accepted socket's remote address, by reference: made text only where a session is saved
//	}
//	type Pingreq struct{}  // a PINGREQ, as an Event's Packet
//	type Pingresp struct{} // a PINGRESP, as a Reply
//	type Close struct{}    // a Reply: close the connection, writing nothing more
//	type Reply any         // a writer's replies-queue entry: *packet.Connack, *packet.Disconnect, Pingresp{} or Close{}
//
//	type Conn struct{ ... }                 // one connection, as the core holds it
//	func (c *Conn) Listener() string        // the name of the listener that accepted it
//	func (c *Conn) RemoteAddr() net.Addr    // the peer's address
//	func (c *Conn) Replies() chan<- Reply   // its writer's replies queue
//	func (c *Conn) Cut()                    // closes its raw socket, and nothing else; the core then sends the final entry at once
//
//	type Metrics struct{ ... }
//	func NewMetrics(reg *metrics.Registry) *Metrics
//
//	type Listener struct{ ... }
//	func Listen(cfg *config.Config, l *config.Listener, inbound chan<- Event, permits chan struct{}, m *Metrics) (*Listener, error)
//	func (l *Listener) Addr() net.Addr // the address it listens on
//	func (l *Listener) Run() error     // accepts until Close, then returns nil; run on the listener's own goroutine
//	func (l *Listener) Close() error   // stops accepting; connections already open go on
//
// NewMetrics registers, once per registry, the series of metrics.md,
// Connections and sessions, that connections move:
// saguin_connections_open, saguin_connections_accepted_total, and
// saguin_connections_refused_total and saguin_disconnects_total for every
// reason their rows list, all at 0 (metrics.md, Rules). Every listener of a
// broker shares the one Metrics and the one inbound channel, the core's; no
// listener, reader or writer holds the logger (R1's logger row: "lines come
// from the core, and from main before the core starts and after it ends").
//
// Listen binds l, a tcp listener of cfg, without accepting. Run accepts, as
// fast as limits.max_connect_rate allows, its bucket starting full (none: no
// limit); for each socket it sends Accepted on inbound, then starts the
// connection's reader and writer with two go statements of its own (the R1
// allowlist names connection.(*Listener).Run for the reader and writer
// rows), so the core meets a connection before any of its packets. An
// accept failing for any reason but the listener's own close ends Run,
// which returns its error (design.md, Restart: "a listener whose accept fails
// for any reason but its own close reports the error to main. The broker
// stops. Main logs the error once the core has ended").
// saguin_connections_open counts each socket served, from its permit to its
// close: the listener adds it before Accepted, the writer takes it away
// before its close report. Nothing decides with it: limits.max_connections is
// permits (design.md, Flows, CONNECT, Connection permits: "An MQTT listener
// accepts a connection, then takes a permit before serving it. [...] The core
// puts the permit back when the writer reports the close").
//
// The reader turns bytes into checked packets and hands them to the core on
// inbound, each read into a buffer of its own that it never touches again.
// Before reading a packet it holds a reply slot of its writer's, taken
// before the read and spent on a packet needing a reply (CONNECT, PINGREQ);
// with none free it reads nothing until one comes back, which the writer
// does once that reply is written. It applies connect_timeout before
// CONNECT and, after it, the keep-alive as a read deadline of 1.5 times the
// CONNECT's (an MQTT 5 client's capped by max_keepalive, a keep-alive of 0
// then counting as the cap). It answers nothing itself:
//
//   - a CONNECT is handed as Received, its Reason 0x00, or the refusal its
//     checks found: 0x84 for a version below min_protocol_version, an
//     unknown level or MQTT 3.1; 0x85 for a client id over
//     max_client_id_length, holding a control character, or empty with
//     Clean Start 0; 0x8C for an Authentication Method; 0x83 for a saguin-
//     User Property among the CONNECT's own properties; 0x95 for a CONNECT
//     over max_connect_size, refused from its fixed header and protocol
//     level, its body unread. Which reason a CONNECT failing several checks
//     gets is free (MQTT 5 4.13.2). After a refused CONNECT the reader reads
//     nothing more, and its next event is Ended.
//   - a PINGREQ is handed as Received with Pingreq{}.
//   - the client's DISCONNECT is handed as Ended, with the packet.
//   - a Malformed Packet, a Protocol Error, a packet over max_packet_size
//     and a keep-alive timeout end the reader: Ended, with the reason the
//     server's DISCONNECT owes (0x81, 0x82, 0x95, 0x8D). Before CONNECT the
//     same holds, no CONNACK going out (design.md, CONNECT): another
//     Protocol Name and a malformed CONNECT end it with 0x81, a CONNECT
//     breaking the protocol and a first packet other than CONNECT with
//     0x82. A lost socket and connect_timeout end it with 0x00.
//
// The writer turns the core's entries into bytes, each pointing to a packet,
// never a copy: a CONNACK, PINGRESP or DISCONNECT in the connection's
// version (a CONNACK refusing an unknown level in MQTT 3.1.1 form), and the
// orderly close, every write under limits.write_timeout. It keeps MQTT's
// rules that need to know Sagüin is the server: no DISCONNECT before a
// CONNACK below 0x80, none on MQTT 3.1.1, and the peer's Maximum Packet
// Size, dropping a Reason String or User Property that would pass it. A
// packet it cannot build is never sent and closes the connection, with
// DISCONNECT 0x80 on MQTT 5 after CONNACK: a packet over the peer's
// Maximum Packet Size even without those properties. It
// also closes the connection after a CONNACK of 0x80 or above, after a
// DISCONNECT, on Close, and when a write passes write_timeout. Once closed it writes nothing more, yet still takes
// entries, returning their slots, until the core's final entry, then ends.
// Its close follows R2, in the exact sequence export_test.go states: one
// deadline, write_timeout after the close begins, which nothing extends;
// the last packet, the end of its sending, the reader stopped; then what
// the client still sends discarded, until its end of stream or the
// deadline; then the socket closed. A client that ends its stream in time
// reads the last packet, then an end of stream, never a reset, even with
// bytes it sent left unread (audits 261012-002a); one still sending at the
// deadline is closed then, which may reset it. What the close reads it
// discards, keeping no buffer once the connection is gone; the close counts
// nothing of its own. Only a write past write_timeout, the deadline and the
// core's Cut may end in a reset.
//
// The core's side, which the tests' stand-in plays: it sends on Replies,
// never waiting, one reply for each slot spent (a CONNACK of the CONNECT's
// Reason, a PINGRESP), and at most one final entry: a *packet.Disconnect, or
// Close{}. On Ended it sends its final, unless it has: the DISCONNECT of
// the Reason where it is 0x80 or above, else Close{}. It cuts a connection
// with Cut, then sends Close{} at once, never waiting for Ended (design.md,
// Flows, Closing a connection: "The core cuts a connection only by closing
// its raw socket. It then sends the writer its final entry at once, without
// waiting for the reader's end"). Replies holds a reply for each of the
// ReplySlots slots and the final, so these sends always find room.
//
// The writer's last act is its close report, Closed, sent on inbound once
// its socket is closed and the reader's Ended has reached the core
// (design.md, Execution, Communication, and Flows, Closing a connection:
// "As its last act, each writer reports on that channel that its socket is
// closed. [...] only after the reader's end has reached the core, so a
// connection's events arrive in one order: accepted, its packets, its end,
// its close").
//
// Counted by the connections: the refusal CONNACKs written, or owed
// (0x95 to MQTT 3.1.1), under their MQTT 5 names; a connection closed
// before its CONNACK, as malformed_packet (another Protocol Name included)
// or protocol_error (a first packet other than CONNECT included), in
// saguin_connections_refused_total; connect_timeout; the
// DISCONNECT reasons sent, or owed by an MQTT 3.1.1 connection, after
// CONNACK; client for the client's DISCONNECT; connection_lost for a socket
// the peer lost; writer_full for a write past write_timeout; and
// unspecified_error for a packet the writer cannot build (0x80), whose
// error its close report carries, for the core to log (design.md, Flows,
// Logging: "Runtime log-worthy events reach the core through their existing
// event or failure reports").

// backstop bounds every wait in these tests; a correct connection is far
// faster.
const backstop = 10 * time.Second

// connectTimeout and writeTimeout are the tests' own limits, set below
// config.md's floors (connect_timeout is at least 1s), so that each test of
// a timeout takes a fraction of a second: writeTimeout in the resolved
// configuration, connectTimeout by the test of connect_timeout alone.
const (
	connectTimeout = 300 * time.Millisecond
	writeTimeout   = 300 * time.Millisecond
)

// longConnectTimeout is connect_timeout in every other test: connect_timeout
// runs from accept (design.md, CONNECT, 1), and a test's own work between
// its dial and its CONNECT, or a loaded machine, must never spend it (audits
// 261012-008a: an oversize CONNECT built after the dial, with load, did).
const longConnectTimeout = time.Hour

// module is the import path prefix of Sagüin's packages.
const module = "github.com/ifnesi/saguin/"

// baseYAML is the smallest valid configuration with one tcp listener on a
// loopback port the system picks. Its store file is never opened.
const baseYAML = `
broker:
  id: test
  storage:
    file_path: /var/lib/saguin/saguin.db
    default_retention_period: 1d
    default_retention_bytes: 1GiB
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
`

// newConfig returns baseYAML resolved, every default filled in, with
// longConnectTimeout and writeTimeout.
func newConfig(t testing.TB) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(baseYAML))
	if err != nil {
		t.Fatalf("the tests' configuration: %v", err)
	}
	cfg.Broker.Limits.ConnectTimeout = longConnectTimeout
	cfg.Broker.Limits.WriteTimeout = writeTimeout
	return cfg
}

// tcpListener returns a tcp listener named name on a loopback port the
// system picks.
func tcpListener(name string) *config.Listener {
	return &config.Listener{Name: name, Address: "127.0.0.1:0"}
}

// connState is what the core's stand-in knows of one connection.
type connState struct {
	ended  bool  // its Ended has been taken
	closed bool  // its close report has been taken
	final  bool  // the stand-in has sent its final entry
	err    error // the error of a reply it could not build, its close report's
}

// broker runs listeners of one configuration with the tests in the core's
// place: the inbound channel is the test's, and every reply is sent by the
// test through send.
type broker struct {
	t       *testing.T
	cfg     *config.Config
	reg     *metrics.Registry
	metrics *connection.Metrics
	inbound chan connection.Event
	addr    string // the first listener's address

	conns   map[*connection.Conn]*connState // every connection accepted
	backlog []connection.Event              // events taken while waiting for another
	clients []*harness.Client
	stops   []func() // each listener's Close and wait for its Run
}

// start runs cfg's first tcp listener; t's cleanup ends every connection
// the test leaves open and checks each one's reader ended.
func start(t *testing.T, cfg *config.Config) *broker {
	t.Helper()
	b := &broker{
		t:       t,
		cfg:     cfg,
		reg:     metrics.New(),
		inbound: make(chan connection.Event, 64),
		conns:   map[*connection.Conn]*connState{},
	}
	b.metrics = connection.NewMetrics(b.reg)
	t.Cleanup(b.stop)
	l := b.listen(&cfg.Broker.MQTT.Listen.TCP[0])
	b.addr = l.Addr().String()
	return b
}

// listen runs one more listener on b's inbound channel and metrics.
// It returns the listener.
func (b *broker) listen(lc *config.Listener) *connection.Listener {
	b.t.Helper()
	l, err := connection.Listen(b.cfg, lc, b.inbound, connection.NewPermits(b.cfg.Broker.Limits.MaxConnections), b.metrics)
	if err != nil {
		b.t.Fatalf("Listen(%s): %v", lc.Address, err)
	}
	b.run(l, lc.Name)
	return l
}

// run runs l, a listener on b's inbound channel and metrics named name,
// until the test ends: b's stop closes it and waits for its Run.
func (b *broker) run(l *connection.Listener, name string) {
	done := make(chan error, 1)
	go func() { done <- l.Run() }()
	b.stops = append(b.stops, func() {
		if err := l.Close(); err != nil {
			b.t.Errorf("closing listener %s: %v", name, err)
		}
		switch err, ok := testkit.RecvUntil(done, backstop, nil); {
		case !ok:
			b.t.Errorf("listener %s's Run did not return within %v of Close", name, backstop)
		case err != nil:
			b.t.Errorf("listener %s's Run returned %v after Close, want nil", name, err)
		}
	})
}

// stop closes the listeners and the clients, sends a Close to every
// connection given no final entry, and waits for every close report; the
// listeners closed, no accept is under way, and saguin_connections_open
// must then be 0, the connections the core counts.
func (b *broker) stop() {
	for _, stop := range b.stops {
		stop()
	}
	for _, c := range b.clients {
		_ = c.Close()
	}
	// Events kept for a wait that never came were recorded as they arrived.
	b.backlog = nil
	for {
		open := 0
		for conn, s := range b.conns {
			if !s.final {
				b.send(conn, connection.Close{})
			}
			if !s.closed {
				open++
			}
		}
		if open == 0 {
			break
		}
		select {
		case ev := <-b.inbound:
			b.take(&ev)
		case <-time.After(backstop):
			b.t.Errorf("%d connections sent no close report within %v of their final entry", open, backstop)
			return
		}
	}
	if got := testkit.Sample(b.t, b.reg, "saguin_connections_open"); got != 0 {
		b.t.Errorf("saguin_connections_open = %v with every close report taken, want 0", got)
	}
}

// take records what ev tells the stand-in: a connection is known from its
// Accepted until its close report, which follows its Ended, and nothing of
// it comes before or after (design.md, Flows, Closing a connection:
// "accepted, its packets, its end, its close").
func (b *broker) take(ev *connection.Event) {
	b.t.Helper()
	s, known := b.conns[ev.Conn]
	switch {
	case ev.Conn == nil:
		b.t.Fatalf("event %d names no connection", ev.Kind)
	case ev.Kind == connection.Accepted:
		if known {
			b.t.Fatalf("a second Accepted for one connection")
		}
		b.conns[ev.Conn] = &connState{}
	case !known:
		b.t.Fatalf("event %d for a connection the core was never told of: Accepted comes first", ev.Kind)
	case s.closed:
		b.t.Fatalf("event %d after the connection's close report", ev.Kind)
	case ev.Kind == connection.Closed:
		if !s.ended {
			b.t.Fatalf("a close report before the connection's Ended")
		}
		s.closed = true
		if cb, ok := ev.Packet.(connection.CannotBuild); ok {
			s.err = cb.Err
		}
	case s.ended:
		b.t.Fatalf("event %d after the connection's Ended: only its close report follows", ev.Kind)
	case ev.Kind == connection.Ended:
		s.ended = true
	}
}

// await returns conn's next event but a close report, keeping the other
// connections' events for later, and checks its kind. A nil conn takes any
// connection's. Close reports are recorded by take and passed over:
// awaitClosed waits for one.
func (b *broker) await(conn *connection.Conn, kind connection.Kind) connection.Event {
	b.t.Helper()
	for i, ev := range b.backlog {
		if conn == nil || ev.Conn == conn {
			b.backlog = append(b.backlog[:i], b.backlog[i+1:]...)
			return checkKind(b.t, &ev, kind)
		}
	}
	return awaitEvent(b.t, b.inbound, kind, func(ev connection.Event) bool {
		b.take(&ev)
		switch {
		case ev.Kind == connection.Closed:
			return false
		case conn == nil || ev.Conn == conn:
			return true
		}
		b.backlog = append(b.backlog, ev)
		return false
	})
}

// awaitClosed waits for conn's close report, keeping the other events for
// later.
func (b *broker) awaitClosed(conn *connection.Conn) {
	b.t.Helper()
	if b.conns[conn].closed {
		return
	}
	awaitEvent(b.t, b.inbound, connection.Closed, func(ev connection.Event) bool {
		b.take(&ev)
		if ev.Kind != connection.Closed {
			b.backlog = append(b.backlog, ev)
		}
		return b.conns[conn].closed
	})
}

// cannotBuild waits for conn's close report and returns the error of a
// reply its writer could not build, which the report carries, or nil.
func (b *broker) cannotBuild(conn *connection.Conn) error {
	b.t.Helper()
	b.awaitClosed(conn)
	return b.conns[conn].err
}

// awaitEvent takes events from inbound, handing each to keep, until keep
// keeps one, and returns that one, checked to be of kind. It fails the test
// where none is kept within backstop, on the bubble's clock in a synctest
// test.
func awaitEvent(t testing.TB, inbound <-chan connection.Event, kind connection.Kind, keep func(connection.Event) bool) connection.Event {
	t.Helper()
	ev, ok := testkit.RecvUntil(inbound, backstop, keep)
	if !ok {
		t.Fatalf("no event of kind %d within %v", kind, backstop)
	}
	return checkKind(t, &ev, kind)
}

// checkKind fails the test unless ev is of kind.
func checkKind(t testing.TB, ev *connection.Event, kind connection.Kind) connection.Event {
	t.Helper()
	if ev.Kind != kind {
		t.Fatalf("%s, want event kind %d", describe(ev), kind)
	}
	return *ev
}

// quiet fails the test if anything comes for conn within d, or for any
// connection where conn is nil. It is the one kind of wait that cannot end
// on an event: it checks that none comes.
func (b *broker) quiet(conn *connection.Conn, d time.Duration) {
	b.t.Helper()
	for _, ev := range b.backlog {
		if conn == nil || ev.Conn == conn {
			b.t.Fatalf("event %d (packet %T, reason 0x%02X), want none", ev.Kind, ev.Packet, ev.Reason)
		}
	}
	ev, came := testkit.RecvUntil(b.inbound, d, func(ev connection.Event) bool {
		b.take(&ev)
		if conn == nil || ev.Conn == conn {
			return true
		}
		if ev.Kind != connection.Closed {
			b.backlog = append(b.backlog, ev)
		}
		return false
	})
	if came {
		b.t.Fatalf("event %d (packet %T, reason 0x%02X) within %v, want none", ev.Kind, ev.Packet, ev.Reason, d)
	}
}

// send puts r on conn's replies queue as the core does, never waiting
// (R2: "The core never waits"): Replies holds a reply for every slot and
// the core's final entry, so a full queue is a broken contract.
func (b *broker) send(conn *connection.Conn, r connection.Reply) {
	b.t.Helper()
	if sendReply(b.t, conn, r) {
		if s := b.conns[conn]; s != nil {
			s.final = true
		}
	}
}

// sendReply puts r on conn's replies queue as the core does, never waiting,
// failing the test where the queue is full, and reports whether r is the
// core's final entry: a Close or a DISCONNECT.
func sendReply(t testing.TB, conn *connection.Conn, r connection.Reply) (final bool) {
	t.Helper()
	select {
	case conn.Replies() <- r:
	default:
		t.Fatalf("the replies queue is full: sending %T would make the core wait (R2)", r)
	}
	switch r.(type) {
	case connection.Close, *packet.Disconnect:
		return true
	}
	return false
}

// answer plays the core's side of an Ended: the DISCONNECT the reason owes,
// else Close.
func (b *broker) answer(ev *connection.Event) {
	b.t.Helper()
	if ev.Reason >= 0x80 {
		b.send(ev.Conn, &packet.Disconnect{ReasonCode: ev.Reason})
		return
	}
	b.send(ev.Conn, connection.Close{})
}

// answerEnd awaits conn's Ended and answers it (answer).
func (b *broker) answerEnd(conn *connection.Conn) {
	b.t.Helper()
	ev := b.await(conn, connection.Ended)
	b.answer(&ev)
}

// dial opens a client connection to b's first listener.
func (b *broker) dial(version byte) *harness.Client {
	b.t.Helper()
	return b.dialAt(b.addr, version)
}

// dialAt opens a client connection to addr, closed at the test's end.
func (b *broker) dialAt(addr string, version byte) *harness.Client {
	b.t.Helper()
	c, err := harness.Dial(addr, version)
	if err != nil {
		b.t.Fatalf("dial %s: %v", addr, err)
	}
	c.Timeout = backstop
	b.clients = append(b.clients, c)
	return c
}

// accept dials b's first listener and returns the client with the
// connection the core was told of.
func (b *broker) accept(version byte) (*harness.Client, *connection.Conn) {
	b.t.Helper()
	c := b.dial(version)
	ev := b.await(nil, connection.Accepted)
	return c, ev.Conn
}

// connectPacket returns a CONNECT every check passes.
func connectPacket(version byte) *harness.Connect {
	return &harness.Connect{ProtocolName: "MQTT", Version: version, CleanStart: true, ClientID: "client"}
}

// sendConnect sends p, filling in its protocol name and version where p
// leaves them empty, and returns the event the reader hands the core.
func (b *broker) sendConnect(c *harness.Client, conn *connection.Conn, p *harness.Connect) connection.Event {
	b.t.Helper()
	if p.ProtocolName == "" {
		p.ProtocolName = "MQTT"
	}
	if p.Version == 0 {
		p.Version = c.Version
	}
	if err := c.Send(p); err != nil {
		b.t.Fatalf("sending CONNECT: %v", err)
	}
	ev := b.await(conn, connection.Received)
	if _, ok := ev.Packet.(*packet.Connect); !ok {
		b.t.Fatalf("Received %T, want *packet.Connect", ev.Packet)
	}
	return ev
}

// connect opens a connection whose CONNECT, p or connectPacket's, is
// accepted with CONNACK 0x00, as the core accepts it.
func (b *broker) connect(version byte, p *harness.Connect) (*harness.Client, *connection.Conn) {
	b.t.Helper()
	c, conn := b.accept(version)
	if p == nil {
		p = connectPacket(version)
	}
	if ev := b.sendConnect(c, conn, p); ev.Reason != 0 {
		b.t.Fatalf("CONNECT refused with 0x%02X, want 0x00", ev.Reason)
	}
	b.send(conn, &packet.Connack{})
	if ack := harness.Expect[*harness.Connack](b.t, c); ack.Reason != 0 {
		b.t.Fatalf("CONNACK 0x%02X, want 0x00", ack.Reason)
	}
	return c, conn
}

// sendPacket encodes p in c's version and writes it.
func sendPacket(t testing.TB, c *harness.Client, p harness.Packet) {
	t.Helper()
	if err := c.Send(p); err != nil {
		t.Fatalf("sending %s: %v", harness.Name(p.Type()), err)
	}
}

// split returns a packet's fixed header and its body.
func split(wire []byte) (header, body []byte) {
	fixed := 2 // the type byte, then the Remaining Length's bytes
	for wire[fixed-1]&0x80 != 0 {
		fixed++
	}
	return wire[:fixed], wire[fixed:]
}

// expectSample checks one series' value now.
func expectSample(t testing.TB, reg *metrics.Registry, series string, want float64) {
	t.Helper()
	if got := testkit.Sample(t, reg, series); got != want {
		t.Errorf("%s = %v, want %v", series, got, want)
	}
}

// refused and disconnects name one series of each family.
func refused(reason string) string {
	return `saguin_connections_refused_total{reason="` + reason + `"}`
}

func disconnects(reason string) string {
	return `saguin_disconnects_total{reason="` + reason + `"}`
}

// describe renders a connection event for messages.
func describe(ev *connection.Event) string {
	return fmt.Sprintf("kind %d, packet %T, reason 0x%02X", ev.Kind, ev.Packet, ev.Reason)
}

// design.md, Testing: the loop "owns the one real timer"; "The busy path is
// anything run per packet, per message or per connection; it does no
// needless work". RealClock's Timer, reset
// to the zero time, the core having no deadline, is stopped: it does not
// fire, however long the wait; reset to a time again, the same timer fires
// once at it. On the bubble's clock (synctest), which time.Timer follows.
func TestRealClockTimerStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, reset := connection.RealClock{}.Timer(time.Now().Add(time.Hour))
		reset(time.Time{})
		time.Sleep(48 * time.Hour)
		synctest.Wait()
		select {
		case at := <-c:
			t.Fatalf("the timer reset to the zero time fired at %v, want it stopped", at)
		default:
		}
		at := time.Now().Add(time.Minute)
		reset(at)
		select {
		case got := <-c:
			if got.Before(at) {
				t.Errorf("the timer moved to %v fired at %v, before it", at, got)
			}
		case <-time.After(2 * time.Minute):
			t.Fatalf("the timer moved to %v did not fire", at)
		}
	})
}

// idleProbe names the environment variable that makes the test binary, run
// again by TestIdleConnectionsHoldNoBuffer, measure idle connections
// instead of running the tests.
const idleProbe = "SAGUIN_CONNECTION_IDLE_PROBE"

func TestMain(m *testing.M) {
	if os.Getenv(idleProbe) != "" {
		// Every allocation is recorded from here on, so the probe sees each
		// buffer a connection holds, whatever its size.
		runtime.MemProfileRate = 1
		os.Exit(probeIdle())
	}
	os.Exit(m.Run())
}

// props is a CONNACK's or DISCONNECT's property section for the values
// given, made by packet.NewProperties, as the core makes its replies'
// (0007a.md, 261013 135359: "the connection tests' props helper switches to
// it").
func props(typ packet.Type, values ...packet.Property) packet.Properties {
	section, err := packet.NewProperties(typ, values)
	if err != nil {
		panic(fmt.Sprintf("NewProperties(%d, %+v): %v", typ, values, err))
	}
	return section
}
