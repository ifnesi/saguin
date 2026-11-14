package conformance

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// design.md, Flows, CONNECT, step 1: "If no CONNECT arrives within
// connect_timeout, the connection closes. CONNECT's deadline starts when the
// connection receives its service permit. It includes transport setup." Half
// of a CONNECT is no CONNECT. MQTT 5 3.1.4 and 3.1.1 3.1.4: "If the Server
// does not receive a CONNECT packet within a reasonable amount of time after
// the Network Connection is established, the Server SHOULD close" it; with no
// CONNACK there is nothing to send ([MQTT-3.2.0-1]). design.md, Testing: "A
// deadline test controls or observes the event starting that deadline. A
// watchdog bounds the test and is reported separately; it does not establish
// a broker timeout." The permit follows the accept, which follows the dial
// the client starts, so the close comes no sooner than connect_timeout after
// it; the Client's default wait is the watchdog.
func TestConnectTimeout(t *testing.T) {
	const timeout = time.Second
	b := support.Launch(t, &harness.Options{ConnectTimeout: timeout})
	half := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "c", CleanStart: true}, harness.V5)[:7]
	for name, sent := range map[string][]byte{"nothing": nil, "half a CONNECT": half} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dialled := time.Now()
			c, err := harness.Dial(b.Addr, harness.V5)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() }) // the test is over
			harness.SendBytes(t, c, sent)
			harness.ExpectClosed(t, c)
			if waited := time.Since(dialled); waited < timeout {
				t.Errorf("closed %v after the dial, before connect_timeout's %v", waited, timeout)
			}
		})
	}
}

// design.md, Flows, CONNECT, step 1: "If no CONNECT arrives within
// connect_timeout, the connection closes. CONNECT's deadline starts when the
// connection receives its service permit"; CONNECT, Connection permits: "An
// MQTT listener accepts a connection, then takes a permit before serving it.
// While no permit is left, the listener holds that connection unserved and
// accepts no more. More connections wait in the accept queue. The core puts
// the permit back when the writer reports the close". With max_connections 1
// and connect_timeout 1s, a first client holds the one permit; a second dials
// and sends nothing, held unserved, and gets nothing, neither a CONNACK nor a
// close, for twice connect_timeout. The first then disconnects, its permit
// going back with its close; only now does the second send its CONNECT, and
// it is connected, CONNACK 0x00: its connect_timeout ran from its permit, not
// from its accept. design.md, Testing: "A deadline test controls or observes
// the event starting that deadline"; the test causes the permit's return, and
// the Client's default wait is the watchdog.
func TestConnectDeadlineStartsAtThePermit(t *testing.T) {
	t.Parallel()
	const timeout = time.Second // config.md: "every other duration is at least 1s"
	b := support.Launch(t, &harness.Options{MaxConnections: 1, ConnectTimeout: timeout})
	first, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "first"), CleanStart: true})
	held, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatalf("a connection held unserved: %v", err)
	}
	t.Cleanup(func() { _ = held.Close() }) // the test is over
	nothingWithin(t, held, 2*timeout, "with max_connections 1 and one client connected, the held client unserved")

	if err := first.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, first)
	_ = first.Close() // its permit goes back with the writer's close report
	if err := held.Send(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, "held"), CleanStart: true}); err != nil {
		t.Fatal(err)
	}
	if ack := harness.Expect[*harness.Connack](t, held); ack.Reason != 0 {
		t.Errorf("the client held past twice connect_timeout, its CONNECT sent once served: CONNACK 0x%02X, want 0x00", ack.Reason)
	}
}

// design.md, DISCONNECT, a lost connection: "a nonzero effective Keep Alive
// gives the reader 1.5 times that interval when it begins reading the next
// packet"; "A keep-alive timeout closes the connection with 0x8D. The
// limits.max_keepalive caps what an MQTT 5 client may ask. A keep-alive of 0
// is never idle-closed unless capped"; "Reasons Sagüin sends": "These reasons
// are MQTT 5 only. An MQTT 3.1.1 connection just closes". MQTT 5 3.2.2.3.14:
// "If the Server sends a Server Keep Alive on the CONNACK packet, the Client
// MUST use this value instead of the Keep Alive value the Client sent on
// CONNECT [MQTT-3.2.2-21]"; [MQTT-3.1.2-22], 3.1.1 [MQTT-3.1.2-24]. A client
// sending PINGREQ within each keep-alive stays (5.0 3.1.2.10, 3.1.1
// 3.1.2.10). MQTT 3.1.1 has no Server Keep Alive, so max_keepalive leaves its
// keep-alive as asked: 0 never closes, 60 not within the test. With no
// max_keepalive (TestMain's broker), an MQTT 5 keep-alive of 0 never closes
// either.
//
// design.md, Testing: "A deadline test controls or observes the event
// starting that deadline. A watchdog bounds the test and is reported
// separately; it does not establish a broker timeout." The reader begins
// reading the packet after the CONNECT only once the client has sent it,
// so a close comes no sooner than 1.5 times the keep-alive after the
// CONNECT is sent; the Client's default wait is the watchdog. A connection
// never idle-closed reads nothing for twice the close a cap would make,
// counted from its CONNACK, and still answers a PINGREQ.
func TestKeepAlive(t *testing.T) {
	const keepAlive = time.Second // the cap, and the keep-alive asked where it is not over it
	b := support.Launch(t, &harness.Options{MaxKeepalive: keepAlive})
	idle := keepAlive + keepAlive/2
	cases := []struct {
		name    string
		version byte
		asked   uint16
		server  uint32 // the CONNACK's Server Keep Alive; 0 for none
		pinging bool   // sends a PINGREQ every half keep-alive
		closed  bool   // idle-closed after 1.5 times the keep-alive
		nocap   bool   // on TestMain's broker, with no max_keepalive
	}{
		{"MQTT 5, pinging in time", harness.V5, 1, 0, true, false, false},
		{"MQTT 3.1.1, pinging in time", harness.V311, 1, 0, true, false, false},
		{"MQTT 5, silent", harness.V5, 1, 0, false, true, false},
		{"MQTT 3.1.1, silent", harness.V311, 1, 0, false, true, false},
		{"MQTT 5, 60 capped", harness.V5, 60, 1, false, true, false},
		{"MQTT 5, 0 capped", harness.V5, 0, 1, false, true, false},
		{"MQTT 3.1.1, 0 never closed", harness.V311, 0, 0, false, false, false},
		{"MQTT 3.1.1, 60 not capped", harness.V311, 60, 0, false, false, false},
		{"MQTT 5, 0 with no max_keepalive", harness.V5, 0, 0, false, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			addr := b.Addr
			if c.nocap {
				addr = brokerAddr
			}
			start := time.Now() // before the CONNECT, which starts the wait
			cl, ack := harness.Connected(t, addr, c.version, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true, KeepAlive: c.asked})
			got, ok := ack.Props.Get(harness.ServerKeepAlive)
			if (c.server != 0) != ok || got.Int != c.server {
				t.Errorf("Server Keep Alive %v %d, want %d", ok, got.Int, c.server)
			}
			if c.pinging {
				for range 4 { // 2 seconds, past 1.5 times the keep-alive
					time.Sleep(keepAlive / 2)
					harness.SendBytes(t, cl, pingreq)
					harness.Expect[*harness.Pingresp](t, cl)
				}
				return
			}
			if !c.closed {
				nothingWithin(t, cl, 2*idle, "idle")
				harness.SendBytes(t, cl, pingreq)
				harness.Expect[*harness.Pingresp](t, cl)
				return
			}
			if c.version == harness.V5 {
				if d := harness.Expect[*harness.Disconnect](t, cl); d.Reason != 0x8D {
					t.Errorf("DISCONNECT 0x%02X, want 0x8D (Keep Alive timeout)", d.Reason)
				}
			}
			harness.ExpectClosed(t, cl)
			if waited := time.Since(start); waited < idle {
				t.Errorf("closed %v after the CONNECT, before 1.5 times the keep-alive, %v", waited, idle)
			}
		})
	}
}

// design.md, Flows, PUBLISH, Rate limits: "The wait holds every packet of
// that connection, acknowledgements and PINGREQ included. It does not
// consume keep-alive time (DISCONNECT)"; "A message larger than a second's
// bytes passes and leaves the byte bucket in debt"; DISCONNECT, a lost
// connection: "a nonzero effective Keep Alive gives the reader 1.5 times
// that interval when it begins reading the next packet. Waiting for a reply
// slot, message reservation, core handover or the publish-rate wait does
// not consume keep-alive time"; "A keep-alive timeout closes the connection
// with 0x8D".
//
// On a broker of its own with publish_bytes publishBytes, an MQTT 5 client
// with a keep-alive of 1 second writes, as soon as its CONNACK has come, a
// QoS 1 PUBLISH of 4 times publishBytes, then a PINGREQ (paced): its
// PUBACK is 0x00, and the PINGRESP, not DISCONNECT 0x8D, comes no sooner
// than three seconds after the write, the debt of at least three seconds'
// bytes paid first: twice the 1.5 seconds the keep-alive allows. The
// connection then answers another PINGREQ.
func TestRateWaitSparesKeepAlive(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishBytes: publishBytes})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true, KeepAlive: 1})
	took, reasons := paced(t, c, []*harness.Publish{{QoS: 1, Topic: "paced/keepalive", Payload: make([]byte, 4*publishBytes)}})
	if reasons[0] != 0 {
		t.Errorf("a PUBLISH of %d bytes, publish_bytes %d: PUBACK 0x%02X; want 0x00, it passes", 4*publishBytes, publishBytes, reasons[0])
	}
	if took < least(3*time.Second) {
		t.Errorf("the PINGRESP came %v after the write; want no sooner than %v, the PINGREQ read once the debt was paid", took, least(3*time.Second))
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("after a wait of %v, past 1.5 times the keep-alive: %v; want the connection to stay", took, err)
	}
}

// design.md, Flows, Closing a connection: "A write that has not finished
// limits.write_timeout after its first socket-write attempt closes the
// connection as writer_full. Partial writes do not extend that deadline."
// DISCONNECT, "Reasons Sagüin sends": "An MQTT 3.1.1 connection just closes,
// as does a connection closed for a full writer". design.md, Testing: "A
// deadline test controls or observes the event starting that deadline. A
// watchdog bounds the test and is reported separately [...]. TCP-load tests
// use an overall watchdog and do not infer a broker write's start from a
// stalled client write."
//
// A client that sends PINGREQs and reads nothing makes the broker write
// PINGRESPs it does not take (stuckClient), its segments the smallest TCP
// allows (fewSegments), so the broker's socket holds a few thousand of
// them before its write stalls, not half a million, and the stall comes
// within the watchdog however slowly the machine answers PINGREQs. The
// broker's first write of a PINGRESP follows the first PINGREQ, so the
// close comes no sooner than write_timeout after it. The broker's close
// finds PINGREQs it never read, so the client may see a reset; whatever it
// then reads holds PINGRESPs alone.
func TestFullWriterCloses(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	c := stuckClientOver(t, fewSegments, b.Addr, harness.V5)
	if _, err := c.Connect(&harness.Connect{ClientID: clientID(t, ""), CleanStart: true}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	pingUntilClosed(t, c, "the first PINGREQ")
	if waited := time.Since(started); waited < support.WriteTimeout {
		t.Errorf("closed %v after the first PINGREQ, before write_timeout's %v", waited, support.WriteTimeout)
	}
	drainable(t, c)
	readUntilClosed(t, c, func(p harness.Packet) bool {
		_, ok := p.(*harness.Pingresp)
		return ok
	}, "PINGRESPs alone")
}

// stuckClient dials addr in version, as a client that will take nothing
// the broker writes (smallBuffers). The connection is closed when the test
// ends.
func stuckClient(t *testing.T, addr string, version byte) *harness.Client {
	t.Helper()
	return stuckClientOver(t, smallBuffers, addr, version)
}

// stuckClientOver is stuckClient, dialled by d, smallBuffers or
// fewSegments.
func stuckClientOver(t *testing.T, d *net.Dialer, addr string, version byte) *harness.Client {
	t.Helper()
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := harness.NewClient(conn, version)
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	return c
}

// smallBuffers dials as a client that will take nothing the broker writes:
// its receive buffer is the smallest the system allows, set before it
// connects, so the window it advertises closes and stays closed while it
// reads nothing; its send buffer is small too, so its own writes stop soon
// after the broker stops reading.
var smallBuffers = &net.Dialer{Timeout: harness.DefaultTimeout, Control: func(_, _ string, raw syscall.RawConn) error {
	var err error
	if cerr := raw.Control(func(fd uintptr) {
		err = errors.Join(syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 1),
			syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096))
	}); cerr != nil {
		return cerr
	}
	return err
}}

// fewSegments dials as smallBuffers does, and also announces the smallest
// segment TCP allows, 88 bytes (TCP_MAXSEG, set before it connects, "also
// changes the MSS value announced to the other end", tcp(7)). Linux sizes
// the broker's send buffer for the connection from the segment it may
// send: measured here, a connection that took nothing held 6,550 bytes of
// two-byte writes before they blocked, and 987,268 with loopback's own
// segments.
var fewSegments = &net.Dialer{Timeout: harness.DefaultTimeout, Control: func(network, address string, raw syscall.RawConn) error {
	if err := smallBuffers.Control(network, address, raw); err != nil {
		return err
	}
	var err error
	if cerr := raw.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, 88)
	}); cerr != nil {
		return cerr
	}
	return err
}}

// drainable readies c, a client dialled with smallBuffers (stuckClient,
// stalledReader) or drainingBuffers and past its stuck phase, to read what
// the broker wrote: it raises c's receive buffer, SO_RCVBUF, to 4 MiB,
// which the system caps at net.core.rmem_max (socket(7)); what c's own
// reader holds stays first.
// A test calls it before c reads to drain. With smallBuffers' receive
// buffer, Linux's loopback TCP prunes and drops much of what arrives and
// sends it again on its retransmit timer, so a draining read crawls at the
// machine's timing: measured on one machine, 554 bytes every 208 ms while
// the broker's side held 986 KB unsent, past a 10-second watchdog, with
// 5,004 receive-queue prunes, the broker rightly blocked in its write.
func drainable(t *testing.T, c *harness.Client) {
	t.Helper()
	conn := c.Conn
	if p, ok := conn.(*peekConn); ok {
		conn = p.Conn
	}
	if err := conn.(*net.TCPConn).SetReadBuffer(4 << 20); err != nil {
		t.Fatal(err)
	}
}

// pingUntilClosed has c, reading nothing, send PINGREQs until a send
// fails, under one deadline for them all, the watchdog, and fails the test
// unless the broker closed the connection: the broker stops reading once
// its writer holds all of c's reply slots, so c's sends stop, and only the
// close ends them, with a reset or a broken pipe. since names when the
// broker's deadline began, for the watchdog's message.
func pingUntilClosed(t *testing.T, c *harness.Client, since string) {
	t.Helper()
	pings := bytes.Repeat(pingreq, 2<<10)
	if err := c.Conn.SetWriteDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
		t.Fatal(err)
	}
	var err error
	for err == nil {
		_, err = c.Conn.Write(pings)
	}
	switch {
	case harness.IsTimeout(err):
		t.Fatalf("watchdog: the connection still takes nothing %v after %s", harness.DefaultTimeout, since)
	case !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE):
		t.Fatalf("sending: %v, want the connection closed", err)
	}
}

// readUntilClosed reads what c still holds once the broker has closed its
// connection, until the end of stream, a reset, or a packet the close cut
// short (design.md, Flows, Closing a connection: "at the deadline the
// socket is closed whatever is left, which may reset it"), and fails the
// test at a packet allowed refuses, which want describes: no DISCONNECT
// comes from a close for a full writer (design.md, DISCONNECT).
func readUntilClosed(t *testing.T, c *harness.Client, allowed func(harness.Packet) bool, want string) {
	t.Helper()
	for {
		p, err := c.Recv()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
			return
		}
		if err != nil {
			t.Fatalf("reading what the broker sent: %v", err)
		}
		if !allowed(p) {
			t.Fatalf("read %s; want %s", harness.Name(p.Type()), want)
		}
	}
}

// config.md, max_connect_rate: "connections each MQTT listener accepts per
// second, in bursts of up to twice that; one over the rate waits in the
// accept queue". Connections dialled at once are all accepted and
// connected; the listener takes them in the order the queue holds them, so
// the k-th one's CONNACK, which follows its accept, comes no sooner than
// (k - burst) / rate seconds after the dials began.
func TestMaxConnectRate(t *testing.T) {
	const rate, burst, dialled = 10, 20, 30
	b := support.Launch(t, &harness.Options{MaxConnectRate: rate})
	clients := make([]*harness.Client, 0, dialled)
	started := time.Now()
	for range dialled {
		c, err := harness.Dial(b.Addr, harness.V5)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() }) // the test is over
		clients = append(clients, c)
	}
	for i, c := range clients {
		harness.SendBytes(t, c, harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, strconv.Itoa(i)), CleanStart: true}, harness.V5))
	}
	for i, c := range clients {
		if ack := harness.Expect[*harness.Connack](t, c); ack.Reason != 0 {
			t.Fatalf("connection %d: CONNACK 0x%02X, want 0x00", i+1, ack.Reason)
		}
		earliest := time.Duration(max(0, i+1-burst)) * time.Second / rate
		if at := time.Since(started); at < earliest {
			t.Errorf("connection %d connected %v after the dials, before %v: faster than max_connect_rate", i+1, at, earliest)
		}
	}
}
