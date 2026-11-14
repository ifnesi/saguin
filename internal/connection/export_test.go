package connection

import (
	"crypto/tls"
	"net"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/packet"
)

// The seam the close's tests drive (close_seam_test.go), as the core's are
// driven with scripted events and fake times (design.md, Testing).
// The production code defines, unexported:
//
//	// socket is every operation a connection's reader and writer do on
//	// their socket; *net.TCPConn has them all.
//	type socket interface {
//		Read(p []byte) (int, error)
//		Write(p []byte) (int, error)
//		SetReadDeadline(t time.Time) error
//		SetWriteDeadline(t time.Time) error
//		CloseWrite() error
//		Close() error
//		RemoteAddr() net.Addr
//	}
//
//	// clock is where a connection's reader and writer read the time:
//	// time.Now and a timer in production.
//	type clock interface {
//		Now() time.Time
//		At(t time.Time) <-chan time.Time // receives once Now has reached t; at once if it has
//	}
//
//	// newListener is a Listener of the resolved configuration whose Run
//	// takes each socket from accept, until it returns an error, and starts
//	// that socket's reader and writer exactly as for a TCP socket, both
//	// reading the time from clk. Listen is newListener on its TCP socket's
//	// AcceptTCP and the real clock; Run keeps the two go statements (R1).
//	// Its readers count unanswered QoS 1 and 2 PUBLISHes against the
//	// configuration's qos2.max_inflight_per_client, fixed before each
//	// starts (design.md, PUBLISH), and its CONNECT deadline starts when
//	// Run takes the connection's service permit (CONNECT, 1).
//
// The reader and writer use only socket's operations on their socket, and
// read the time only from clk. The close, in R2's order and with the
// stated rule where two events meet (R3), is then one exact sequence of
// socket calls on the writer's side:
//
//  1. The close begins when the writer takes its final entry (a
//     *packet.Disconnect or Close{}) and fixes D = clk.Now() +
//     limits.write_timeout, which nothing after it changes.
//  2. The last packet, where there is one: SetWriteDeadline(D), then one
//     Write of the whole packet.
//  3. The end of its sending: CloseWrite().
//  4. The reader stopped: SetReadDeadline(t), t a time clk has reached and
//     not the zero time, so that a read under way returns at once.
//  5. Only once the reader has said it stopped, which it does before it
//     reports Ended to the core: SetReadDeadline(D), then Read until a
//     Read ends in an error (the client's end of stream, or D passing).
//  6. Close().
//
// Then, as its last act and only once the reader's Ended has reached the
// core, the writer sends Closed on the readers' inbound channel (design.md,
// Flows, Closing a connection: "The writer reports the close only after the
// reader's end has reached the core").
//
// Once clk has reached D, the writer's next socket call is Close, whatever
// step it is at: a Read returning bytes at D is not followed by another,
// and a reader's stop and D ready together close without the discard
// (design.md, Execution, Decisions: "Map iteration, an observed metric,
// queue emptiness and Go's choice between ready cases decide none of
// them"; design.md, Flows, Closing a connection: "Where the end and the
// deadline meet, the deadline wins").

// Socket is the connection's socket interface, for scripted sockets.
type Socket = socket

// NewListener is newListener, for the tests outside the package, of the
// resolved configuration cfg: its limits, its minimum protocol version and
// its qos2.max_inflight_per_client, with permits of its own for
// limits.max_connections.
func NewListener(name string, accept func() (Socket, error), clk Clock, cfg *config.Config,
	inbound chan<- Event, m *Metrics) *Listener {
	return newListener(name, accept, clk, cfg, inbound, NewPermits(cfg.Broker.Limits.MaxConnections), m)
}

// ReadProxyHeaders has l, not yet running, read a PROXY v2 header from every
// connection it serves before its CONNECT, as a unix listener with
// proxy_protocol does (design.md, Flows, Transport setup).
func ReadProxyHeaders(l *Listener) { l.proxyProtocol = true }

// ListenWebSocketOn is ListenWebSocket on lc, with permits of its own, its
// connections reading the time from clk, each TCP socket it accepts handed
// on through wrap: under TLS, the socket beneath the TLS layer.
func ListenWebSocketOn(cfg *config.Config, lc *config.Listener, inbound chan<- Event, m *Metrics,
	clk Clock, wrap func(*net.TCPConn) Socket) (*Listener, error) {
	l, err := ListenWebSocket(cfg, lc, inbound, NewPermits(cfg.Broker.Limits.MaxConnections), m)
	if err != nil {
		return nil, err
	}
	l.clock = clk
	accept := l.accept
	l.accept = func() (socket, error) {
		s, err := accept()
		switch s := s.(type) {
		case *tls.Conn:
			beneath := s.NetConn().(*tlsSocket)
			beneath.Conn = wrap(beneath.Conn.(*net.TCPConn)).(net.Conn)
		case *net.TCPConn:
			return wrap(s), err
		}
		return s, err
	}
	return l, nil
}

// PacketReader is the reader of one connection, already connected, for the
// busy path's guards (header_test.go): Next is its read of one packet, the
// work its loop does between two reply slots, alone. design.md, Testing: a
// test inside the code checks "the guards of [...] the busy path".
type PacketReader struct{ r *reader }

// NewPacketReader returns the reader of a connection on raw whose CONNECT
// in version has been accepted, under limits, counting into m.
func NewPacketReader(raw Socket, version packet.Version, limits *config.Limits, m *Metrics) *PacketReader {
	c := &Conn{raw: raw, clock: RealClock{}, stopping: make(chan struct{})}
	return &PacketReader{r: &reader{conn: c, limits: *limits, metrics: m, version: version, connected: true}}
}

// Next reads the next packet as the reader's loop does, and returns the
// event the reader would hand the core.
func (p *PacketReader) Next() (Event, error) { return p.r.next() }
