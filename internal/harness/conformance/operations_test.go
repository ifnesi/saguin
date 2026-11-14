package conformance

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the operations HTTP service (design.md, Operations, HTTP and
// Who may call; config.md, broker.operations; metrics.md, Process). Each
// launches a broker of its own with one tcp operations listener on a
// loopback address (harness.Options.Operations) and calls it with no
// credential, as an operator on the same machine does.

// opsRequest is opsTry, failing the test where no answer comes.
func opsRequest(t *testing.T, b *harness.Broker, method, path, host string) harness.OpsAnswer {
	t.Helper()
	return opsCall(t, support.PlainOps(b), &harness.OpsCall{Method: method, Path: path, Host: host})
}

// opsCall sends c to target, failing the test where no answer comes.
func opsCall(t *testing.T, target harness.OpsTarget, c *harness.OpsCall) harness.OpsAnswer {
	t.Helper()
	a, err := target.Do(c)
	if err != nil {
		t.Fatalf("%s %s on %s: %v", c.Method, c.Path, target.Address, err)
	}
	return a
}

// design.md, Operations, HTTP: "/health returns 200 {"status":"ok"} once
// listeners and bridges have started, while the core answers within 5
// seconds and the last snapshot, if any yet, succeeded. Otherwise it
// returns 503 {"status":"unavailable"}, including while stopping"; "Every
// route takes GET, and /health also takes HEAD". Restart, Graceful stop:
// "/health answers 503 until exit". metrics.md, Process: saguin_ready, "1
// while listeners and bridges have started, the last snapshot, if any yet,
// succeeded and the broker is not stopping: /health's conditions but the
// core's answering".
//
// Once ready, GET and HEAD of /health answer 200 and saguin_ready is 1. An
// MQTT 5 client that never closes holds the graceful stop open: SIGTERM
// gives it DISCONNECT 0x8B, and its connection then waits for its end of
// stream until write_timeout (Closing a connection). Within that, GET and
// HEAD of /health answer 503 and saguin_ready is 0; the broker then exits 0.
func TestHealthFollowsReadinessAndTheStop(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		support.ExpectHealth(t, method, opsRequest(t, b, method, "/health", ""), http.StatusOK, "ok")
	}
	expectSample(t, b, "saguin_ready", 1, "once /health answers 200")

	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "holds-the-stop", CleanStart: true})
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8B {
		t.Fatalf("DISCONNECT 0x%02X, want 0x8B (Server shutting down)", d.Reason)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		support.ExpectHealth(t, method, opsRequest(t, b, method, "/health", ""), http.StatusServiceUnavailable, "unavailable")
	}
	expectSample(t, b, "saguin_ready", 0, "while stopping")
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// design.md, Operations, HTTP: "Every route takes GET, and /health also
// takes HEAD. The broker decides the route first. An unknown route gets
// 404, whatever the credential. For a known route, the broker checks the
// credential, then the method. Any other method gets 405. Credential checks
// follow Who may call."; "Errors are plain text". metrics.md, Process:
// saguin_operations_requests_total{route,code}, "HTTP requests: route is
// [...] or other; code is [...] 404, 405". Each refusal comes as
// text/plain; OPTIONS *, a request to the server as a whole, names no
// route, which is decided first: it gets 404 and adds one to route other,
// code 404.
func TestOperationsRefusesOtherMethodsAndRoutes(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{http.MethodPost, "/health", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/health", http.StatusMethodNotAllowed},
		{http.MethodHead, "/metrics", http.StatusMethodNotAllowed},
		{http.MethodPut, "/metrics", http.StatusMethodNotAllowed},
		{http.MethodOptions, "*", http.StatusNotFound},
		{http.MethodGet, "/nowhere", http.StatusNotFound},
		{http.MethodGet, "/v1/operations/nowhere", http.StatusNotFound},
	} {
		before := requestCounts(t, b)
		a := opsRequest(t, b, tc.method, tc.path, "")
		if a.Status != tc.status || a.MediaType != "text/plain" {
			t.Errorf("%s %s: %d, %q; want %d, text/plain", tc.method, tc.path, a.Status, a.MediaType, tc.status)
		}
		if tc.path != "*" {
			continue
		}
		other := testkit.SeriesKey("saguin_operations_requests_total", "route", "other", "code", "404")
		if got := requestCounts(t, b)[other] - before[other]; got != 1 {
			t.Errorf("%s %s: %s rose by %v; want 1", tc.method, tc.path, other, got)
		}
	}
}

// requestCounts reads b's saguin_operations_requests_total, each series by
// its testkit.SeriesKey; a series not yet counted is absent.
func requestCounts(t *testing.T, b *harness.Broker) map[string]float64 {
	t.Helper()
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	got := map[string]float64{}
	for _, s := range e.Samples {
		if s.Name == "saguin_operations_requests_total" {
			got[testkit.SeriesKey(s.Name, "route", s.Labels["route"], "code", s.Labels["code"])] = s.Value
		}
	}
	return got
}

// h2Preface is what an HTTP/2 client sends first over cleartext with prior
// knowledge: the connection preface "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
// followed by an empty SETTINGS frame, a 9-byte frame header of length 0,
// type 0x4, flags 0 and stream 0 (RFC 9113, 3.4 and 6.5).
var h2Preface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00")

// design.md, R1: "HTTP/2 is off"; 0020a.md, 261013 100403 (audit findings
// 261009-002a and 261013-011a): "over TLS, a client offering ALPN h2 and
// http/1.1 negotiates HTTP/1.1; over cleartext, the HTTP/2 preface (PRI *
// HTTP/2.0) gets an HTTP/1 error or a close, never a SETTINGS frame".
//
// A broker has three operations listeners: plain TCP, TLS and a unix
// socket. A client offering h2 and http/1.1 completes a handshake with the
// TLS one, which negotiates no h2. Each listener is then sent h2Preface,
// the TLS one on that connection; what comes back is an end of stream, a
// reset, or an answer beginning "HTTP/1." (refusesHTTP2).
func TestOperationsRefusesHTTP2(t *testing.T) {
	certs := newPKI(t)
	u := unixSocket(t, false)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{TLS: new(certs.listener(&harness.TLS{}))}, {Unix: &u}}})
	offer := certs.client(nil)
	offer.NextProtos = []string{"h2", "http/1.1"}
	secured := opsDial(t, harness.OpsTarget{Network: "tcp", Address: b.Ops[0], TLS: offer})
	if p := secured.(*tls.Conn).ConnectionState().NegotiatedProtocol; p == "h2" {
		t.Errorf("offered h2 and http/1.1, the TLS operations listener negotiated %q: HTTP/2 spoken", p)
	}
	for _, conn := range []net.Conn{opsDial(t, support.PlainOps(b)), secured, opsDial(t, harness.OpsTarget{Network: "unix", Address: b.Ops[1]})} {
		refusesHTTP2(t, conn)
	}
}

// refusesHTTP2 sends conn, a cleartext connection to an HTTP listener,
// h2Preface, and fails the test unless what comes back is an end of
// stream, a reset, or an answer beginning "HTTP/1.". A server speaking
// HTTP/2 answers with its own SETTINGS frame, a frame header whose fourth
// byte is the type 0x4.
func refusesHTTP2(t *testing.T, conn net.Conn) {
	t.Helper()
	if _, err := conn.Write(h2Preface); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
		t.Fatal(err)
	}
	// Nine bytes judge it: "HTTP/1.1 " or a frame header.
	got := make([]byte, 0, 9)
	var readErr error
	for len(got) < cap(got) && readErr == nil {
		var n int
		n, readErr = conn.Read(got[len(got):cap(got)])
		got = got[:len(got)+n]
	}
	closed := errors.Is(readErr, io.EOF) || errors.Is(readErr, syscall.ECONNRESET)
	switch {
	case len(got) >= 4 && got[3] == 0x4 && !strings.HasPrefix(string(got), "HTTP/"):
		t.Errorf("the HTTP/2 preface was answered % X: a SETTINGS frame, HTTP/2 spoken", got)
	case strings.HasPrefix(string(got), "HTTP/1."):
	case len(got) == 0 && closed:
	case harness.IsTimeout(readErr):
		t.Errorf("watchdog: %v after the HTTP/2 preface, %q came and the connection is still open; want an HTTP/1 error or a close", harness.DefaultTimeout, got)
	default:
		t.Errorf("the HTTP/2 preface was answered %q (%v); want an HTTP/1 error or a close", got, readErr)
	}
}

// opsDial dials target until the listener accepts and the connection is
// set up, its PROXY header sent and its TLS handshake done
// (harness.OpsTarget.Dial), failing the test where it has not within
// harness.DefaultTimeout of the broker's start (the watchdog is the test's):
// a dial the listener refuses makes no connection. The connection is closed
// when the test ends.
func opsDial(t *testing.T, target harness.OpsTarget) net.Conn {
	t.Helper()
	var conn net.Conn
	var err error
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		conn, err = target.Dial()
		return err == nil
	}) {
		t.Fatalf("watchdog: the operations listener %s does not accept: %v", target.Address, err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // a second close changes nothing
	return conn
}

// healthGET is a GET of /health, a route that needs no credential (design.md,
// Operations, Who may call), on a kept-alive HTTP/1.1 connection.
const healthGET = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"

// heldConnections is how many connections an operations listener holds:
// design.md, Operations, HTTP: "Each operations listener holds at most 64
// connections, counted at accept".
const heldConnections = 64

// unserved is how long a connection past heldConnections is watched for an
// answer that must not come. Every held connection was answered just
// before, well within the 5 seconds its next request's headers have, so
// none of them closes on its own while it is watched.
const unserved = time.Second

// design.md, Operations, HTTP: "Each operations listener holds at most 64
// connections, counted at accept. More connections wait in the accept
// queue"; Who may call: "/health needs no credential on any operations
// listener".
//
// A broker has three operations listeners: plain TCP, TLS, and a unix
// socket with proxy_protocol. On each in turn, 64 connections, dialled one
// after another, each set up (its handshake done, its PROXY header sent),
// and nothing else dialled to the listener, each send a GET of /health and
// get an answer: each is served. A 65th, dialled after them, makes the same
// request on a connection of its own and gets nothing for unserved; once
// one of the 64 closes, it is answered.
func TestOperationsHoldsAtMost64Connections(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	u := unixSocket(t, true)
	b := support.Launch(t, &harness.Options{Operations: true, OpsListeners: []harness.OpsListener{{TLS: new(certs.listener(&harness.TLS{}))}, {Unix: &u}}})
	served := func(conn net.Conn, r *bufio.Reader) error {
		if err := conn.SetDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
			return err
		}
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, resp.Body)
		return errors.Join(err, resp.Body.Close())
	}
	for _, c := range []struct {
		name   string
		target harness.OpsTarget
	}{
		{"plain", support.PlainOps(b)},
		{"TLS", harness.OpsTarget{Network: "tcp", Address: b.Ops[0], TLS: certs.client(nil)}},
		{"PROXY", harness.OpsTarget{Network: "unix", Address: b.Ops[1], Proxy: proxiedV4()}},
	} {
		target := c.target
		t.Run(c.name, func(t *testing.T) {
			held := make([]net.Conn, heldConnections)
			for i := range held {
				held[i] = opsDial(t, target)
				if _, err := io.WriteString(held[i], healthGET); err != nil {
					t.Fatal(err)
				}
				if err := served(held[i], bufio.NewReader(held[i])); err != nil {
					t.Fatalf("connection %d of %d: no answer: %v", i+1, heldConnections, err)
				}
			}
			answered := make(chan error, 1)
			go func() {
				_, err := target.Do(&harness.OpsCall{Method: http.MethodGet, Path: "/health"})
				answered <- err
			}()
			select {
			case err := <-answered:
				t.Fatalf("connection %d, with %d held, ended its request (%v); want nothing until one of them closes", heldConnections+1, heldConnections, err)
			case <-time.After(unserved):
			}
			if err := held[0].Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-answered; err != nil {
				t.Errorf("connection %d, once one of the %d closed: no answer: %v", heldConnections+1, heldConnections, err)
			}
		})
	}
}

// headerLimit is how long a request's headers have: design.md, Operations,
// HTTP: "A request's headers must arrive within 5 seconds".
const headerLimit = 5 * time.Second

// setupDelay is how long a connection waits after its dial before its TLS
// handshake begins or its PROXY header is sent, within the five seconds
// either has from the accept: a header limit counted from the accept would
// close the connection before setupDelay and headerLimit have passed since
// the dial.
const setupDelay = 3 * time.Second

// design.md, Operations, HTTP: "A request's headers must arrive within 5
// seconds. The whole request must arrive within 10 seconds"; Flows,
// Transport setup, TLS: "The HTTP request's own deadlines start once the
// handshake is done (Operations, HTTP)"; PROXY: "The HTTP request's own
// deadlines start once the header is in (Operations, HTTP)".
//
// A broker has three operations listeners: plain TCP, TLS, and a unix socket
// with proxy_protocol. On each, a connection sends the first lines of a
// GET's headers and never ends them: on plain TCP once dialled; on TLS after
// a handshake begun setupDelay after the dial; on the unix socket after a
// PROXY header sent setupDelay after the dial. It is closed, by an end of
// stream or a reset, no sooner than 5 seconds after it was dialled, its
// handshake began or its header was sent, and before 7: the header limit,
// counted from then, not the 10 seconds a whole request has. Whatever the
// broker writes before the close is read and ignored.
func TestOperationsClosesWhenHeadersAreLate(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	u := unixSocket(t, true)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{TLS: new(certs.listener(&harness.TLS{}))}, {Unix: &u}}})
	for _, c := range opsSetups(b, certs) {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			conn, from := c.open(t, 0)
			if _, err := io.WriteString(conn, "GET /health HTTP/1.1\r\nHost: localhost\r\n"); err != nil {
				t.Fatal(err)
			}
			closedBetween(t, conn, from, headerLimit, "its headers unfinished")
		})
	}
}

// opsSetup is how a connection reaches an operations listener's HTTP: on
// plain TCP once dialled; on TLS after a handshake begun setupDelay after
// the dial; on a unix socket with proxy_protocol after a PROXY header,
// naming a loopback client, sent setupDelay after the dial.
type opsSetup struct {
	name, network, addr string
	setUp               func(net.Conn) (net.Conn, error) // nil for none
}

// opsSetups are b's three operations listeners as opsSetup reaches them:
// its plain one, its first, with tls from certs, and its second, a unix
// socket with proxy_protocol.
func opsSetups(b *harness.Broker, certs *pki) []opsSetup {
	secure := certs.client(nil)
	secure.ServerName = "127.0.0.1"
	return []opsSetup{
		{"plain", "tcp", b.OpsAddr, nil},
		{"after the TLS handshake", "tcp", b.Ops[0], func(conn net.Conn) (net.Conn, error) {
			s := tls.Client(conn, secure)
			err := s.Handshake()
			return s, err
		}},
		{"after the PROXY header", "unix", b.Ops[1], func(conn net.Conn) (net.Conn, error) {
			_, err := conn.Write(harness.ProxyHeader(harness.ProxyProxy, harness.ProxyTCP4,
				netip.MustParseAddrPort("127.0.0.1:51234"), proxyV4))
			return conn, err
		}},
	}
}

// open dials s's listener and sets the connection up, returning it and
// when its HTTP's own deadlines start: the dial, or the end of the setup.
// A receive buffer above zero is the socket's SO_RCVBUF, set before it
// connects: a TCP client then takes no more than that before it reads,
// however its kernel would grow its buffer.
func (s opsSetup) open(t *testing.T, receiveBuffer int) (net.Conn, time.Time) {
	t.Helper()
	d := net.Dialer{Timeout: harness.DefaultTimeout}
	if receiveBuffer > 0 {
		d.Control = func(_, _ string, raw syscall.RawConn) error {
			var err error
			if cerr := raw.Control(func(fd uintptr) {
				err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, receiveBuffer)
			}); cerr != nil {
				return cerr
			}
			return err
		}
	}
	from := time.Now()
	conn, err := d.DialContext(t.Context(), s.network, s.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // a second close changes nothing
	if s.setUp != nil {
		time.Sleep(setupDelay) // the delay under test, within the setup's own five seconds
		if conn, err = s.setUp(conn); err != nil {
			t.Fatal(err)
		}
		from = time.Now()
	}
	return conn, from
}

// closedBetween reads conn to its end, failing the test unless it ends, by
// an end of stream, a reset, or over TLS an alert the client's TLS layer
// raises on what came last, no sooner than limit after from and before two
// seconds more; whatever the broker writes before is read and ignored. (A
// net/http server of Go's own, a body stalled under its ReadTimeout, ends
// a TLS stream with a record its client reads as "bad record MAC".)
func closedBetween(t *testing.T, conn net.Conn, from time.Time, limit time.Duration, what string) {
	t.Helper()
	judged := from.Add(limit + 2*time.Second)
	if err := conn.SetReadDeadline(judged); err != nil {
		t.Fatal(err)
	}
	_, err := io.Copy(io.Discard, conn)
	closed := time.Now()
	switch {
	case harness.IsTimeout(err):
		t.Errorf("the connection, %s, is still open %v after its setup; want it closed after %v", what, judged.Sub(from), limit)
	case err != nil && !errors.Is(err, syscall.ECONNRESET) && !tlsLocalError(err):
		t.Errorf("reading the connection, %s: %v; want an end of stream or a reset", what, err)
	case closed.Before(from.Add(limit)):
		t.Errorf("the connection, %s, closed %v after its setup, before %v", what, closed.Sub(from), limit)
	}
}

// tlsLocalError says whether err is an alert a TLS client's own layer
// raised on the records it read, as crypto/tls reports one: a *net.OpError
// whose Op is "local error".
func tlsLocalError(err error) bool {
	op, ok := errors.AsType[*net.OpError](err)
	return ok && op.Op == "local error"
}

// requestLimit is how long a whole request has, and its answer's writing:
// design.md, Operations, HTTP: "The whole request must arrive within 10
// seconds. Its answer must be written within 10 seconds".
const requestLimit = 10 * time.Second

// bigGrants is an acl_file whose user big holds 40,000 rules, so the acl
// route's answer for big, every grant, runs to megabytes: more than a
// socket's buffers hold for a client that reads none of it.
func bigGrants() string {
	var acl strings.Builder
	acl.WriteString("roles:\n  big:\n")
	for i := range 40000 {
		fmt.Fprintf(&acl, "    - topic: \"g/%d/#\"\n      allow: [read]\n", i)
	}
	acl.WriteString("users:\n  big: [big]\n")
	return acl.String()
}

// design.md, Operations, HTTP: "The whole request must arrive within 10
// seconds. Its answer must be written within 10 seconds"; Flows, Transport
// setup, TLS: "The HTTP request's own deadlines start once the handshake is
// done (Operations, HTTP)"; PROXY: "The HTTP request's own deadlines start
// once the header is in (Operations, HTTP)"; metrics.md, Process:
// saguin_operations_requests_total{route,code}, which counts a request once
// it has been answered.
//
// For each of the three setups (opsSetup), a broker whose acl_file gives
// big 40,000 grants has the plain, TLS and proxied operations listeners.
// Over that setup, two connections, with no credential: one sends a GET of
// /health whose headers promise a 100-byte body and sends 10 bytes of it,
// then nothing; it is closed, by an end of stream or a reset, no sooner
// than 10 seconds after its setup, and before 12. The other sends a GET of
// the acl route for big, its receive buffer 4 KiB, and reads none of the
// answer; the acl route's
// count of 200 answers rises by one, the answer given up, no sooner than
// 10 seconds after its setup, and before 12.
func TestOperationsRequestAndAnswerHaveTenSeconds(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	answered := testkit.SeriesKey("saguin_operations_requests_total", "route", "acl", "code", "200")
	for i := range 3 {
		u := unixSocket(t, true)
		o := aclOptions(t, bigGrants(), "big")
		o.OpsListeners = []harness.OpsListener{{TLS: new(certs.listener(&harness.TLS{}))}, {Unix: &u}}
		b := support.OpsLaunch(t, o)
		s := opsSetups(b, certs)[i]
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			t.Run("a stalled body", func(t *testing.T) {
				t.Parallel()
				conn, from := s.open(t, 0)
				if _, err := io.WriteString(conn, "GET /health HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n0123456789"); err != nil {
					t.Fatal(err)
				}
				closedBetween(t, conn, from, requestLimit, "its body unfinished")
			})
			t.Run("an answer never read", func(t *testing.T) {
				t.Parallel()
				before := requestCounts(t, b)[answered]
				conn, from := s.open(t, 4<<10)
				if _, err := io.WriteString(conn, "GET /v1/operations/acl?user=big HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
					t.Fatal(err)
				}
				var given time.Time // when a scrape first showed the answer counted: never before it was
				if !testkit.Within(time.Until(from.Add(requestLimit+2*time.Second)), func() bool {
					counted := requestCounts(t, b)[answered] == before+1
					given = time.Now()
					return counted
				}) {
					t.Fatalf("the answer, never read, is still being written %v after its setup; want it given up after %v", time.Since(from), requestLimit)
				}
				if given.Before(from.Add(requestLimit)) {
					t.Errorf("the answer, never read, was given up %v after its setup, before %v", given.Sub(from), requestLimit)
				}
			})
		})
	}
}

// design.md, Operations, Who may call: "A request with no credential
// reaches every route only on a loopback address or Unix socket, and only
// when its Host is an IP literal or localhost. Otherwise it gets 403"; HTTP:
// "Errors are plain text"; config.md, broker.operations, listen: "a
// non-loopback address [...] needs a password file or a required client
// certificate".
//
// A broker with the operators' file has three operations listeners: plain
// TCP on a loopback address, a unix socket, and TCP on a non-loopback
// address of this machine (nonLoopback). A request with no credential, to
// /metrics or the acl route, is answered 200 on the loopback address where
// its Host is the listener's address, any other IP literal, IPv4 or IPv6,
// with or without a port, or localhost, and on the unix socket where its
// Host is localhost or an IP literal; it gets 403, as text/plain, where its
// Host is a name, and on the non-loopback address whatever its Host.
func TestNoCredentialNeedsALocalCaller(t *testing.T) {
	u := unixSocket(t, false)
	b := support.OpsLaunch(t, &harness.Options{OpsPasswordFile: operatorsFile(t),
		OpsListeners: []harness.OpsListener{{Unix: &u}, {Host: nonLoopback(t)}}})
	plain, local, remote := support.PlainOps(b), harness.OpsTarget{Network: "unix", Address: b.Ops[0]}, harness.OpsTarget{Network: "tcp", Address: b.Ops[1]}
	acl := aclPath("anyone", "")
	cases := make([]opsCase, 0, 14)
	for _, r := range []struct {
		target     harness.OpsTarget
		path, host string
		status     int
	}{
		{plain, "/metrics", "", http.StatusOK},
		{plain, acl, "127.0.0.1", http.StatusOK},
		{plain, "/metrics", "192.0.2.7:8080", http.StatusOK},
		{plain, "/metrics", "[::1]:9", http.StatusOK},
		{plain, acl, "localhost", http.StatusOK},
		{plain, "/metrics", "localhost:9100", http.StatusOK},
		{plain, "/metrics", "example.com", http.StatusForbidden},
		{plain, acl, "localhost.example.com:9100", http.StatusForbidden},
		{plain, "/metrics", "127.0.0.1.example.com", http.StatusForbidden},
		{local, "/metrics", "", http.StatusOK},
		{local, acl, "127.0.0.1", http.StatusOK},
		{local, "/metrics", "example.com", http.StatusForbidden},
		{remote, "/metrics", "", http.StatusForbidden},
		{remote, acl, "localhost", http.StatusForbidden},
	} {
		cases = append(cases, opsCase{r.target, &harness.OpsCall{Method: http.MethodGet, Path: r.path, Host: r.host}, []int{r.status}})
	}
	expectOps(t, cases)
}

// versionOf returns the version binary's --version prints: its line's last
// word (design.md, Operations: "saguin --version | prints [...] the
// version").
func versionOf(t *testing.T, binary string) string {
	t.Helper()
	out, err := exec.Command(binary, "--version").Output()
	f := strings.Fields(string(out))
	if err != nil || len(f) == 0 {
		t.Fatalf("%s --version: %q, %v", binary, out, err)
	}
	return f[len(f)-1]
}

// goroutineRoles are saguin_goroutines' role label values, metrics.md,
// Process: "live goroutines per R1 row".
var goroutineRoles = []string{"main", "core", "store_writer", "store_reader", "logger", "listener", "reader", "writer", "dialer", "http_request"}

// metrics.md, Process: saguin_build_info{version,broker_id}, "always 1";
// saguin_uptime_seconds, "since start"; saguin_ready (above);
// saguin_goroutines{role}, "live goroutines per R1 row";
// saguin_operations_requests_total{route,code}, "HTTP requests: route is
// /health, /metrics, [...] or other; code is 200, 400, 401, 403, 404, 405
// or 503". design.md, R1's table: main 1, core 1, store writer 1, store
// reader read_connections (config.md: 2 by default), logger 1, listener "1
// per listener, MQTT and operations", reader and writer "1 per connection",
// dialer "1 per bridge connection attempt", HTTP request "1 per open
// operations connection".
//
// The broker has broker.id harness, one MQTT listener, one operations
// listener and no bridge. Once ready, its build information carries the
// version --version prints and its id; its uptime rises and is never more
// than the time since the test started it; it is ready. With one MQTT
// client connected, and this test's requests on one operations connection,
// the goroutines are main 1, core 1, store_writer 1, store_reader 2, logger
// 1, listener 2, reader 1, writer 1, dialer 0 and http_request 1. Requests
// of /health (GET and HEAD, 200; POST, 405), of /metrics (POST, 405; with
// no credential and a Host that is a name, 403) and of an unknown route
// (404, other) each add one to
// their route and code, and nothing else moves but /metrics' 200, the
// scrapes'.
func TestProcessSeries(t *testing.T) {
	before := time.Now()
	b := support.OpsLaunch(t, &harness.Options{})
	version := versionOf(t, support.BrokerBinary)
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	if got := e.Value(t, "saguin_build_info", "version", version, "broker_id", "harness"); got != 1 {
		t.Errorf("saguin_build_info = %v, want 1", got)
	}
	expectSample(t, b, "saguin_ready", 1, "once /health answers 200")
	uptime := func() float64 {
		u := testkit.Sample(t, b.Metrics(), "saguin_uptime_seconds")
		if since := time.Since(before).Seconds(); u < 0 || u > since {
			t.Fatalf("saguin_uptime_seconds = %v, %v after the test started the broker", u, since)
		}
		return u
	}
	first := uptime()
	if !testkit.Within(harness.DefaultTimeout, func() bool { return uptime() > first }) {
		t.Errorf("saguin_uptime_seconds is still %v %v later", first, harness.DefaultTimeout)
	}

	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "goroutines", CleanStart: true})
	want := map[string]float64{"main": 1, "core": 1, "store_writer": 1, "store_reader": 2, "logger": 1,
		"listener": 2, "reader": 1, "writer": 1, "dialer": 0, "http_request": 1}
	running := func() map[string]float64 {
		e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
		got := make(map[string]float64, len(goroutineRoles))
		for _, role := range goroutineRoles {
			got[role] = e.Value(t, "saguin_goroutines", "role", role)
		}
		return got
	}
	// The harness's check that the MQTT listener accepts was a connection
	// of its own, whose goroutines end once the broker has its close.
	if !testkit.Within(harness.DefaultTimeout, func() bool { return maps.Equal(running(), want) }) {
		t.Errorf("saguin_goroutines by role %v, want R1's %v", running(), want)
	}

	start := requestCounts(t, b)
	for _, r := range []struct{ method, path, host string }{
		{http.MethodGet, "/health", ""},
		{http.MethodHead, "/health", ""},
		{http.MethodPost, "/health", ""},
		{http.MethodGet, "/metrics", "example.com"},
		{http.MethodPost, "/metrics", ""},
		{http.MethodGet, "/nowhere", ""},
	} {
		opsRequest(t, b, r.method, r.path, r.host)
	}
	added := map[string]float64{
		testkit.SeriesKey("saguin_operations_requests_total", "route", "/health", "code", "200"):  2,
		testkit.SeriesKey("saguin_operations_requests_total", "route", "/health", "code", "405"):  1,
		testkit.SeriesKey("saguin_operations_requests_total", "route", "/metrics", "code", "403"): 1,
		testkit.SeriesKey("saguin_operations_requests_total", "route", "/metrics", "code", "405"): 1,
		testkit.SeriesKey("saguin_operations_requests_total", "route", "other", "code", "404"):    1,
	}
	scrapes := testkit.SeriesKey("saguin_operations_requests_total", "route", "/metrics", "code", "200")
	end := requestCounts(t, b)
	for _, key := range slices.Sorted(maps.Keys(added)) {
		if _, ok := end[key]; !ok {
			t.Errorf("the scrape has no series %s", key)
		}
	}
	for key, n := range end {
		if key != scrapes && n-start[key] != added[key] {
			t.Errorf("%s rose by %v, want %v", key, n-start[key], added[key])
		}
	}
}
