package connection_test

import (
	"crypto/tls"
	"net"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/testkit"
)

// refusedReasons and disconnectReasons are the label values metrics.md's
// rows list for saguin_connections_refused_total and
// saguin_disconnects_total. The refusal row ends "connect_timeout (no
// CONNECT in time); tls_handshake (a failed TLS handshake [...]); or
// proxy_header (a refused PROXY header [...])".
var (
	refusedReasons = []string{
		"unsupported_protocol_version", "client_identifier_not_valid", "bad_user_name_or_password",
		"not_authorized", "topic_name_invalid", "retain_not_supported", "qos_not_supported",
		"packet_too_large", "implementation_specific_error",
		"bad_authentication_method", "quota_exceeded", "malformed_packet", "protocol_error",
		"connect_timeout", "tls_handshake", "proxy_header",
	}
	disconnectReasons = []string{
		"unspecified_error", "malformed_packet", "protocol_error", "server_shutting_down",
		"keep_alive_timeout", "session_taken_over", "receive_maximum_exceeded",
		"topic_alias_invalid", "packet_too_large", "administrative_action",
		"retain_not_supported", "qos_not_supported", "shared_subscriptions_not_supported",
		"not_authorized", "topic_name_invalid", "quota_exceeded", "implementation_specific_error",
		"client", "connection_lost", "writer_full", "publish_stalled",
	}
)

// metrics.md, Rules: "Every series exists from start, for every value of its
// labels that applies [...] Counters start at 0 [...] Other gauges start at
// their current value." Connections and sessions lists
// saguin_connections_open, saguin_connections_accepted_total, and
// saguin_connections_refused_total and saguin_disconnects_total with their
// reasons.
func TestNewMetricsRegistersTheConnectionSeries(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	connection.NewMetrics(reg)
	series := make([]string, 0, 2+len(refusedReasons)+len(disconnectReasons))
	series = append(series, "saguin_connections_open", "saguin_connections_accepted_total")
	for _, r := range refusedReasons {
		series = append(series, refused(r))
	}
	for _, r := range disconnectReasons {
		series = append(series, disconnects(r))
	}
	for _, s := range series {
		expectSample(t, reg, s, 0)
	}
	// Each family has exactly the rows' reasons, no more.
	out := testkit.Scrape(t, reg)
	for _, family := range []struct {
		name    string
		reasons []string
	}{
		{"saguin_connections_refused_total{", refusedReasons},
		{"saguin_disconnects_total{", disconnectReasons},
	} {
		n := 0
		for line := range strings.SplitSeq(out, "\n") {
			if strings.HasPrefix(line, family.name) {
				n++
			}
		}
		if n != len(family.reasons) {
			t.Errorf("%s has %d series, want %d", strings.TrimSuffix(family.name, "{"), n, len(family.reasons))
		}
	}
}

// design.md, Flows, CONNECT, 1: "Connection startup follows R1, including
// its WebSocket exception. The listener tells the core about the
// connection." design.md, Execution, Communication: "All readers hand
// packets to the core on one bounded channel". Two listeners share one
// inbound channel; each accepted socket reaches the core as Accepted,
// naming its listener and peer, before any of its packets. The event itself
// carries both (connection.Accept), for the session the core stores
// (storage.md, sessions: listener, "the listener of its last connection",
// and remote, "its last connection's remote address").
func TestAcceptTellsTheCore(t *testing.T) {
	t.Parallel()
	cfg := newConfig(t)
	cfg.Broker.MQTT.Listen.TCP[0].Name = "first"
	b := start(t, cfg)
	second := b.listen(tcpListener("second"))

	for _, at := range []struct{ name, addr string }{{"first", b.addr}, {"second", second.Addr().String()}} {
		c := b.dialAt(at.addr, harness.V5)
		ev := b.await(nil, connection.Accepted)
		if got := ev.Conn.Listener(); got != at.name {
			t.Errorf("Listener() = %q, want %q", got, at.name)
		}
		if got, want := ev.Conn.RemoteAddr().String(), c.Conn.LocalAddr().String(); got != want {
			t.Errorf("RemoteAddr() = %s, want the client's %s", got, want)
		}
		if a, ok := ev.Packet.(connection.Accept); !ok || a.Listener != at.name || a.Remote == nil ||
			a.Remote.String() != c.Conn.LocalAddr().String() {
			t.Errorf("Accepted carries %#v, want Accept{%q, the client's %s}", ev.Packet, at.name, c.Conn.LocalAddr())
		}
		// Its CONNECT comes on the same channel, for the same connection.
		if ev := b.sendConnect(c, ev.Conn, connectPacket(harness.V5)); ev.Reason != 0 {
			t.Errorf("CONNECT refused with 0x%02X", ev.Reason)
		}
	}
}

// design.md, Flows, CONNECT, 1: "If no CONNECT arrives within
// connect_timeout, the connection closes. CONNECT's deadline starts when the
// connection receives its service permit. It includes transport setup";
// design.md, Flows, CONNECT, Connection permits: "An MQTT listener accepts a
// connection, then takes a permit before serving it"; metrics.md:
// saguin_connections_refused_total counts connect_timeout, "no CONNECT in
// time". The listener takes the connection's permit with the clock at T0 and
// offers the core its Accepted, which the core takes only at T0 plus twice
// connect_timeout; the reader starts only then. Its first read is under the
// deadline T0 plus connect_timeout, already past, not connect_timeout after
// the reader's start; the socket answers that read as a real one does once
// its deadline has passed, and the connection ends as connect_timeout: the
// reader's plain Ended, counted once. Only an inside test can hold the core's
// take of Accepted.
func TestConnectDeadlineStartsAtThePermit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newConfig(t)
		timeout := cfg.Broker.Limits.ConnectTimeout
		s := launchSeam(t, cfg)
		synctest.Wait() // the listener has taken the permit, at epoch, and offers Accepted
		s.clock.set(epoch.Add(2 * timeout))
		s.accepted()

		want := epoch.Add(timeout)
		var deadline time.Time // the reader's read deadline, as last set
		for {
			c := s.next(reader)
			if c.op == opReadDeadline {
				deadline = c.t
				c.ok()
				continue
			}
			if c.op != opRead {
				t.Fatalf("the reader called %v, want its first read", c)
			}
			if !deadline.Equal(want) {
				t.Fatalf("the reader's first read is under the deadline %v, want %v: connect_timeout from the "+
					"permit, taken at %v, not from the reader's start at %v", deadline, want, epoch, c.at)
			}
			s.stopped = true // the read ends the reader: it must call nothing more
			c.fail(deadlinePassed)
			break
		}
		if ev := s.event(connection.Ended); ev.Packet != nil || ev.Reason != 0 {
			t.Fatalf("the core got %s, want a plain Ended: no CONNECT in time sends no CONNACK", describe(&ev))
		}
		expectSample(t, s.reg, refused("connect_timeout"), 1)
	})
}

// design.md, Flows, Transport setup: "On an MQTT listener, the header has
// the earlier of the CONNECT deadline and five seconds from service
// beginning (CONNECT)"; "A v1 header, a missing header, one not sent in
// time [...] closes the connection and is logged. On an MQTT listener it is
// also counted as refused (proxy_header)"; CONNECT, 1: "CONNECT's deadline
// starts when the connection receives its service permit". metrics.md:
// saguin_connections_refused_total, proxy_header "a refused PROXY header"
// and connect_timeout "no CONNECT in time". Only an inside test can fix the
// time the permit is taken, and so the exact deadline.
//
// A listener reading PROXY headers takes a connection's permit with the
// clock at T0. Its reader reads the header under the earlier deadline,
// whatever the clock reads by then: T0 plus five seconds with
// connect_timeout 30s, T0 plus connect_timeout with 2s. The header's first
// 9 bytes come at T0; with the clock at T0 plus one second, the reader
// still reads the rest under that one deadline. With connect_timeout 30s
// the rest of the header comes, and the reader reads the CONNECT under T0
// plus 30s. With 2s the clock reaches T0 plus 2s, the socket answers the
// read as a real one does once its deadline has passed, and the connection
// ends refused, its failure handed to the core to log and counted once as
// proxy_header, connect_timeout not counted.
func TestProxyHeaderDeadlineIsTheEarlierFromThePermit(t *testing.T) {
	header := harness.ProxyHeader(harness.ProxyProxy, harness.ProxyTCP4,
		netip.MustParseAddrPort("192.0.2.10:51234"), netip.MustParseAddrPort("192.0.2.1:1883"))
	for _, tc := range []struct {
		name                   string
		connectTimeout, header time.Duration
	}{
		{"connect_timeout 30s", 30 * time.Second, 5 * time.Second},
		{"connect_timeout 2s", 2 * time.Second, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := newConfig(t)
				cfg.Broker.Limits.ConnectTimeout = tc.connectTimeout
				s := launchSeam(t, cfg, connection.ReadProxyHeaders)
				synctest.Wait() // the listener has taken the permit, at epoch, and offers Accepted
				s.accepted()

				var deadline time.Time // the reader's read deadline, as last set
				readUnder := func(want time.Time, what string) *sockCall {
					t.Helper()
					for {
						c := s.next(reader)
						switch c.op {
						case opReadDeadline:
							deadline = c.t
							c.ok()
							continue
						case opRead:
						default:
							t.Fatalf("the reader called %v, want a read of %s", c, what)
						}
						if !deadline.Equal(want) {
							t.Fatalf("the reader reads %s under the deadline %v at %v; want %v, fixed at the permit, taken at %v",
								what, deadline, c.at, want, epoch)
						}
						return c
					}
				}
				want := epoch.Add(tc.header)
				rest := header[readUnder(want, "the header").give(header[:9]):]
				s.clock.set(epoch.Add(time.Second))
				if tc.connectTimeout > tc.header {
					for len(rest) > 0 {
						rest = rest[readUnder(want, "the rest of the header").give(rest):]
					}
					readUnder(epoch.Add(tc.connectTimeout), "the CONNECT")
					return
				}
				c := readUnder(want, "the rest of the header")
				s.clock.set(want)
				s.stopped = true // the read ends the reader: it must call nothing more
				c.fail(deadlinePassed)
				if ev := s.event(connection.Ended); ev.Reason != 0 {
					t.Fatalf("the core got %s, want Ended with no reason: a refused header sends nothing", describe(&ev))
				} else if f, ok := ev.Packet.(connection.ReadFailure); !ok || f.Err == nil {
					t.Fatalf("the core got %s, want Ended carrying the header's failure, for the core to log", describe(&ev))
				}
				expectSample(t, s.reg, refused("proxy_header"), 1)
				expectSample(t, s.reg, refused("connect_timeout"), 0)
			})
		})
	}
}

// socketEvent is one thing done to a socket: a read or a write deadline
// set, to t, or a read that returned bytes.
type socketEvent struct {
	kind byte // 'r', 'w' or 'd': a read deadline, a write deadline, data read
	t    time.Time
}

// deadlineSocket is an accepted TCP socket that notes in events, in the
// order each goroutine does them, every deadline set on it and every read
// that returns bytes.
type deadlineSocket struct {
	*net.TCPConn
	events chan socketEvent
}

func (s *deadlineSocket) Read(p []byte) (int, error) {
	n, err := s.TCPConn.Read(p)
	if n > 0 {
		s.events <- socketEvent{kind: 'd'}
	}
	return n, err
}

func (s *deadlineSocket) SetDeadline(t time.Time) error {
	s.events <- socketEvent{'r', t}
	s.events <- socketEvent{'w', t}
	return s.TCPConn.SetDeadline(t)
}

func (s *deadlineSocket) SetReadDeadline(t time.Time) error {
	s.events <- socketEvent{'r', t}
	return s.TCPConn.SetReadDeadline(t)
}

func (s *deadlineSocket) SetWriteDeadline(t time.Time) error {
	s.events <- socketEvent{'w', t}
	return s.TCPConn.SetWriteDeadline(t)
}

// design.md, Flows, Transport setup: a ws listener's "TLS handshake, where
// it has one, and the upgrade both run within the CONNECT deadline
// (CONNECT). A connection still unupgraded at that deadline is counted as
// connect_timeout"; CONNECT, 1: "If no CONNECT arrives within
// connect_timeout, the connection closes. CONNECT's deadline starts when
// the connection receives its service permit. It includes transport setup".
// One budget, fixed at the permit, runs through TLS, the upgrade and the
// CONNECT. Only an inside test can fix the time the permit is taken, and so
// the exact cutoff; the wire's test is
// TestWebSocketSetupIsCutAtTheConnectDeadline.
//
// A ws listener, and one with tls, take a connection's permit with the
// clock at P, later than the real time, so no deadline it fixes passes
// while the test runs. Once the core is told of the connection, a read and
// a write deadline are set on its socket, each P plus connect_timeout. The
// clock moves to P plus one second, the client completes TLS where there is
// one and the upgrade, and the clock moves to P plus two seconds; the
// client sends its CONNECT, which reaches the core. Every deadline set on
// the socket until the CONNECT's last bytes are read is P plus
// connect_timeout, or a time already passed, which ends a read under way at
// once (net/http's own, before it hands the upgraded connection over); and
// those last bytes are read under the read deadline P plus connect_timeout.
// What is set once they are read is no longer setup.
func TestWebSocketSetupSharesTheConnectDeadline(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "ws"
		if secure {
			name = "wss"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := newConfig(t)
			cfg.Broker.Limits.MaxConnectRate = 0 // no rate bucket: the listener reads the clock only at the permit
			b := start(t, cfg)
			lc := &config.Listener{Name: name, Address: "127.0.0.1:0"}
			d := &harness.WebSocketDial{Subprotocols: []string{"mqtt"}}
			if secure {
				ca, err := testkit.NewAuthority("Sagüin test authority")
				if err != nil {
					t.Fatal(err)
				}
				server, err := ca.Issue(&testkit.Leaf{CommonName: "saguin", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
				if err != nil {
					t.Fatal(err)
				}
				dir := t.TempDir()
				lc.TLS = &config.TLS{CertFile: testkit.WriteFile(t, dir, "server.crt", string(server.CertPEM)),
					KeyFile: testkit.WriteFile(t, dir, "server.key", string(server.KeyPEM))}
				d.TLS = &tls.Config{RootCAs: ca.Pool(), MinVersion: tls.VersionTLS12}
			}
			permit := time.Now().Add(time.Hour)
			clk := &fakeClock{now: permit}
			events := make(chan socketEvent, 1024)
			l, err := connection.ListenWebSocketOn(cfg, lc, b.inbound, b.metrics, clk,
				func(c *net.TCPConn) connection.Socket { return &deadlineSocket{TCPConn: c, events: events} })
			if err != nil {
				t.Fatalf("ListenWebSocket: %v", err)
			}
			b.run(l, name)
			want := permit.Add(cfg.Broker.Limits.ConnectTimeout)

			// apply follows the socket's events so far, up to its last data
			// read, failing the test on any deadline but want or one
			// already passed. It keeps the deadlines in force and returns
			// the read deadline the last data read was under.
			var read, write time.Time
			apply := func(phase string) (lastRead time.Time) {
				t.Helper()
				var taken []socketEvent
				for len(events) > 0 {
					taken = append(taken, <-events)
				}
				for i, e := range slices.Backward(taken) {
					if e.kind == 'd' {
						taken = taken[:i+1]
						break
					}
				}
				for _, e := range taken {
					switch {
					case e.kind == 'd':
						lastRead = read
						continue
					case !e.t.Equal(want) && (e.t.IsZero() || !e.t.Before(time.Now())):
						t.Errorf("%s: a deadline of %v (%c) set; want %v, connect_timeout from the permit at %v", phase, e.t, e.kind, want, permit)
					}
					if e.kind == 'r' {
						read = e.t
					} else {
						write = e.t
					}
				}
				return lastRead
			}

			addr := l.Addr().String()
			raw, err := net.DialTimeout("tcp", addr, backstop)
			if err != nil {
				t.Fatal(err)
			}
			conn := b.await(nil, connection.Accepted).Conn
			apply("before the setup")
			if !read.Equal(want) || !write.Equal(want) {
				t.Fatalf("with the core told of the connection, its read deadline is %v and its write deadline %v; want both %v, before the setup begins", read, write, want)
			}
			clk.set(permit.Add(time.Second))
			c, err := harness.UpgradeWebSocket(raw, addr, harness.V5, d)
			if err != nil {
				t.Fatalf("the setup: %v", err)
			}
			c.Timeout = backstop
			b.clients = append(b.clients, c)
			clk.set(permit.Add(2 * time.Second))
			b.sendConnect(c, conn, connectPacket(harness.V5))
			if got := apply("through the setup and the CONNECT"); !got.Equal(want) {
				t.Errorf("the CONNECT's last bytes were read under the deadline %v; want %v, connect_timeout from the permit", got, want)
			}
		})
	}
}

// metrics.md: saguin_bytes_received_total and saguin_bytes_sent_total count
// "bytes on MQTT connections". One exchange, its every byte known: the
// client sends a CONNECT, a PINGREQ and a DISCONNECT, then ends its stream;
// the broker sends a CONNACK 0x00 with no properties and a PINGRESP. A
// PINGRESP is 2 bytes (MQTT 5 3.13, MQTT 3.1.1 3.13); the CONNACK is 4 in
// MQTT 3.1.1 (3.2: a fixed header of 2 and a variable header of 2) and 5 in
// MQTT 5 (3.2.2: the Connect Acknowledge Flags, the Reason Code and a
// Property Length of 0). Once the socket is closed, the counters hold
// exactly those sums.
func TestBytesCountTheExchange(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		version byte
		connack int
	}{{harness.V311, 4}, {harness.V5, 5}} {
		t.Run(harness.VersionName(tc.version), func(t *testing.T) {
			t.Parallel()
			b := start(t, newConfig(t))
			c, conn := b.connect(tc.version, nil)
			sendPacket(t, c, &harness.Pingreq{})
			b.await(conn, connection.Received)
			b.send(conn, connection.Pingresp{})
			harness.Expect[*harness.Pingresp](t, c)
			sendPacket(t, c, &harness.Disconnect{})
			endStream(t, c)
			b.answerEnd(conn)
			harness.ExpectClosed(t, c)
			b.awaitClosed(conn) // the socket is closed: every byte is counted

			received := len(harness.Encoded(t, connectPacket(tc.version), tc.version)) +
				len(harness.Encoded(t, &harness.Pingreq{}, tc.version)) +
				len(harness.Encoded(t, &harness.Disconnect{}, tc.version))
			expectSample(t, b.reg, "saguin_bytes_received_total", float64(received))
			expectSample(t, b.reg, "saguin_bytes_sent_total", float64(tc.connack+2))
		})
	}
}

// config.md, max_connect_rate: "connections each MQTT listener accepts per
// second [...]; none allowed": with none, the listener does not wait at
// all. The rate itself is the conformance tests' (TestMaxConnectRate).
func TestMaxConnectRateNone(t *testing.T) {
	t.Parallel()
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxConnectRate = 0
	b := start(t, cfg)
	for range 10 {
		b.dial(harness.V5)
	}
	for range 10 {
		b.await(nil, connection.Accepted)
	}
}
