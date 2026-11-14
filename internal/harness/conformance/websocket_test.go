package conformance

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of MQTT over WebSocket (item 0013d). config.md, A listener:
// address and tls (tcp, ws); same_origin (ws), "a page's origin must equal
// the request's scheme, host and port"; allowed_origins (ws), "the origins
// allowed". design.md, Flows, Transport setup, its ws paragraph. MQTT 5
// chapter 6, "Using WebSocket as a network transport", MQTT-6.0.0-1 to -4.
// Every WebSocket is spoken by harness.DialWebSocket, written from RFC 6455
// with the standard library, never with the broker's WebSocket library.

// overMQTT is a client's default WebSocket, a new one for each to change:
// offering the mqtt subprotocol, which "The Client MUST include"
// [MQTT-6.0.0-3], each write one frame.
func overMQTT() *harness.WebSocketDial {
	return &harness.WebSocketDial{Subprotocols: []string{"mqtt"}}
}

// dialWS opens a WebSocket to addr as d says, its MQTT in version, failing
// the test where the upgrade fails. The connection is closed when the test
// ends.
func dialWS(t *testing.T, addr string, version byte, d *harness.WebSocketDial) *harness.Client {
	t.Helper()
	c, err := harness.DialWebSocket(addr, version, d)
	if err != nil {
		t.Fatalf("the WebSocket upgrade with %s (%+v): %v", addr, d, err)
	}
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	return c
}

// webSocket is the WebSocket c speaks over.
func webSocket(c *harness.Client) *harness.WebSocket {
	return c.Conn.(*harness.WebSocket)
}

// design.md, Flows, Transport setup: "A ws listener accepts any path. It
// answers the mqtt subprotocol when offered, and also accepts a client
// offering none, or only others." MQTT 5 chapter 6: "The WebSocket Subprotocol name
// selected and returned by the Server MUST be "mqtt"" [MQTT-6.0.0-4]; "The
// WebSocket URI used to connect the Client and Server has no impact on the
// MQTT protocol". RFC 6455 4.1 has a client fail an upgrade naming a
// subprotocol it did not offer (harness.DialWebSocket does), so one
// offering none, or only others, is answered with none.
//
// On a broker's one ws listener, clients upgrade on "/", "/mqtt", a deeper
// path with a query, and "/chat", offering mqtt, mqtt after another
// subprotocol, none, and only another: the first two are answered mqtt,
// the others with no subprotocol. Each connects, CONNACK 0x00, and is
// answered a PINGREQ. The MQTT 3.1.1 client on "/" subscribes at QoS 1;
// the one offering none publishes at QoS 1, PUBACK 0x00; the subscriber is
// sent the message.
func TestWebSocketListenerAcceptsAnyPath(t *testing.T) {
	b := support.Launch(t, &harness.Options{WS: []harness.WS{{}}})
	cases := []struct {
		path    string
		offered []string
		version byte
		want    string
	}{
		{"/", []string{"mqtt"}, harness.V311, "mqtt"},
		{"/mqtt", []string{"chat", "mqtt"}, harness.V5, "mqtt"},
		{"/any/deeper/path?query=1", nil, harness.V5, ""},
		{"/chat", []string{"chat"}, harness.V5, ""},
	}
	clients := make([]*harness.Client, len(cases))
	for i, c := range cases {
		clients[i] = dialWS(t, b.WS[0], c.version, &harness.WebSocketDial{Path: c.path, Subprotocols: c.offered})
		if got := webSocket(clients[i]).Response.Header.Get("Sec-WebSocket-Protocol"); got != c.want {
			t.Errorf("%s, offering %q: the subprotocol answered is %q; want %q", c.path, c.offered, got, c.want)
		}
		harness.ConnectedOver(t, clients[i], &harness.Connect{ClientID: clientID(t, c.path), CleanStart: true})
		if err := clients[i].Ping(); err != nil {
			t.Errorf("%s: %v", c.path, err)
		}
	}
	sub, pub := clients[0], clients[2]
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "ws/t", QoS: 1}}, 1)
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "ws/t", Payload: []byte("over WebSocket")})
	support.DeliveredAt(t, sub, 1, "ws/t", []byte("over WebSocket"))
}

// MQTT 5 chapter 6: "A single WebSocket data frame can contain multiple or
// partial MQTT Control Packets. The receiver MUST NOT assume that MQTT
// Control Packets are aligned on WebSocket frame boundaries"
// [MQTT-6.0.0-2]. RFC 6455 5.4: a message may be sent in fragments, a
// binary frame and continuation frames.
//
// A TCP client subscribes to ws/frames at QoS 1. Over the ws listener, one
// client sends every packet, its CONNECT and a QoS 1 PUBLISH, as a message
// of one-byte frames; another sends three QoS 1 PUBLISHes packed in one
// frame; a third cuts its CONNECT and two packed QoS 1 PUBLISHes into
// messages of three bytes, each ending inside a packet. Each is connected,
// CONNACK 0x00, and each PUBLISH acknowledged, PUBACK 0x00; the subscriber
// is sent the six messages, each client's in its order.
func TestWebSocketPacketsCrossFrameBoundaries(t *testing.T) {
	b := support.Launch(t, &harness.Options{WS: []harness.WS{{}}})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "ws/frames", QoS: 1}}, 1)
	publication := func(id uint16, payload string) *harness.Publish {
		return &harness.Publish{QoS: 1, ID: id, Topic: "ws/frames", Payload: []byte(payload)}
	}
	// packed sends ps, in one write, from c and waits for each PUBACK
	// 0x00.
	packed := func(c *harness.Client, ps ...*harness.Publish) {
		t.Helper()
		wire := make([][]byte, len(ps))
		for i, p := range ps {
			wire[i] = harness.Encoded(t, p, harness.V5)
		}
		harness.SendBytes(t, c, slices.Concat(wire...))
		for _, p := range ps {
			support.Puback(t, c, p)
		}
	}

	fragments := dialWS(t, b.WS[0], harness.V5, &harness.WebSocketDial{Subprotocols: []string{"mqtt"}, FrameSize: 1})
	harness.ConnectedOver(t, fragments, &harness.Connect{ClientID: clientID(t, "-fragments"), CleanStart: true})
	support.Acknowledged(t, fragments, publication(1, "in one-byte frames"))
	whole := dialWS(t, b.WS[0], harness.V5, overMQTT())
	harness.ConnectedOver(t, whole, &harness.Connect{ClientID: clientID(t, "-packed"), CleanStart: true})
	packed(whole, publication(1, "packed 1"), publication(2, "packed 2"), publication(3, "packed 3"))
	cut := dialWS(t, b.WS[0], harness.V5, &harness.WebSocketDial{Subprotocols: []string{"mqtt"}, MessageSize: 3})
	harness.ConnectedOver(t, cut, &harness.Connect{ClientID: clientID(t, "-cut"), CleanStart: true})
	packed(cut, publication(1, "cut 1"), publication(2, "cut 2"))

	for _, payload := range []string{"in one-byte frames", "packed 1", "packed 2", "packed 3", "cut 1", "cut 2"} {
		p := support.DeliveredAt(t, sub, 1, "ws/frames", []byte(payload))
		if err := sub.Acknowledge(p); err != nil {
			t.Fatal(err)
		}
	}
}

// design.md, Flows, Transport setup: a ws listener "closes a connection
// that sends a text frame, counted as connection_lost (MQTT-6.0.0-1)".
// MQTT 5 chapter 6: "MQTT Control Packets MUST be sent in WebSocket binary
// data frames. If any other type of data frame is received the recipient
// MUST close the Network Connection" [MQTT-6.0.0-1]. metrics.md,
// saguin_disconnects_total: "connection_lost".
//
// A client connected over the ws listener sends a PINGREQ in a text frame.
// It is sent no PINGRESP: the connection closes. connection_lost rises by
// one, and every other count of a refusal or a disconnect is as it was.
func TestWebSocketTextFrameClosesTheConnection(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{WS: []harness.WS{{}}})
	want := baselineClosures(t, b)
	c := dialWS(t, b.WS[0], harness.V5, overMQTT())
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	if err := webSocket(c).WriteFrame(true, harness.OpText, pingreq); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, c)
	want[lost]++
	var got map[string]float64
	if !testkit.Within(harness.DefaultTimeout, func() bool { got = closures(t, b); return maps.Equal(got, want) }) {
		t.Errorf("after %v, the counts of refusals and disconnects are %v; want %v: the text frame's close counted as connection_lost alone", harness.DefaultTimeout, got, want)
	}
}

// allowedOrigin is the origin the origin tests' listeners list in
// allowed_origins.
const allowedOrigin = "https://allowed.example:8443"

// design.md, Flows, Transport setup: "Without an Origin header, a
// WebSocket connection passes the origin check. With an Origin header and
// either check enabled, the origin must pass same_origin or appear in
// allowed_origins. The same_origin check compares scheme, case-insensitive
// host and effective port. Multiple, malformed or null origins fail. With
// same_origin: false and no allowed origins, origin checking is disabled.
// A refused origin gets HTTP 403 and is logged." config.md: same_origin
// (ws), default true, "a page's origin must equal the request's scheme,
// host and port". A ws listener without tls takes http requests, so a
// page of the same origin is http:// and the request's Host; a URL with
// no port has its scheme's, 80 for http (RFC 6454 4).
//
// Three ws listeners: the first with same_origin at its default and
// allowedOrigin listed; the second with same_origin false and the same
// list; the third with same_origin false and no list. Each upgrade either
// passes, its client connected, CONNACK 0x00, or is answered 403, and the
// log then gains a line.
func TestWebSocketOrigins(t *testing.T) {
	listed := []string{allowedOrigin}
	b := support.Launch(t, &harness.Options{WS: []harness.WS{
		{AllowedOrigins: listed},
		{SameOrigin: "false", AllowedOrigins: listed},
		{SameOrigin: "false"},
	}})
	_, port, err := net.SplitHostPort(b.WS[0])
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		listener int
		host     string   // the Host header; "" for the address dialled
		origins  []string // the Origin headers, each a line
		pass     bool
	}{
		{"no Origin, same_origin and a list", 0, "", nil, true},
		{"the same origin", 0, "", []string{"http://" + b.WS[0]}, true},
		{"the same origin, its host in another case", 0, "localhost:" + port, []string{"http://LocalHost:" + port}, true},
		{"the same origin, its port the scheme's", 0, "example.test:80", []string{"http://example.test"}, true},
		{"a listed origin", 0, "", []string{allowedOrigin}, true},
		{"another scheme", 0, "", []string{"https://" + b.WS[0]}, false},
		{"another host", 0, "", []string{"http://elsewhere.example:" + port}, false},
		{"another port", 0, "example.test:80", []string{"http://example.test:8080"}, false},
		{"null", 0, "", []string{"null"}, false},
		{"two Origin headers", 0, "", []string{"http://" + b.WS[0], "http://" + b.WS[0]}, false},
		{"a malformed origin", 0, "", []string{"http://[::1"}, false},
		{"no Origin, a list alone", 1, "", nil, true},
		{"a listed origin, a list alone", 1, "", []string{allowedOrigin}, true},
		{"the same origin, a list alone", 1, "", []string{"http://" + b.WS[1]}, false},
		{"another origin, checking off", 2, "", []string{"http://elsewhere.example"}, true},
		{"null, checking off", 2, "", []string{"null"}, true},
		{"two Origin headers, checking off", 2, "", []string{"http://elsewhere.example", "http://" + b.WS[2]}, true},
	}
	for _, c := range cases {
		d := &harness.WebSocketDial{Subprotocols: []string{"mqtt"}, Host: c.host}
		if c.origins != nil {
			d.Header = http.Header{"Origin": c.origins}
		}
		logged := len(b.Log())
		client, err := harness.DialWebSocket(b.WS[c.listener], harness.V5, d)
		if c.pass {
			if err != nil {
				t.Errorf("%s: %v; want the upgrade", c.name, err)
				continue
			}
			harness.ConnectedOver(t, client, &harness.Connect{ClientID: clientID(t, "-"+c.name), CleanStart: true})
			continue
		}
		var refused *harness.UpgradeError
		switch {
		case err == nil:
			_ = client.Close() // upgraded where it should not be
			t.Errorf("%s: upgraded; want 403", c.name)
			continue
		case !errors.As(err, &refused) || refused.StatusCode != http.StatusForbidden:
			t.Errorf("%s: %v; want the upgrade answered 403", c.name, err)
			continue
		}
		if !testkit.Within(harness.DefaultTimeout, func() bool { return len(b.Log()) > logged }) {
			t.Errorf("%s: refused, and nothing was logged within %v; want the refusal logged", c.name, harness.DefaultTimeout)
		}
	}
}

// design.md, Flows, Transport setup: "Any other request that is not a
// valid WebSocket upgrade gets the library's own HTTP error and is closed,
// neither logged nor counted"; a failed TLS handshake "is logged naming the
// TLS error, and is counted as refused (tls_handshake)", which marks the
// log. RFC 6455 4.1 and 4.2.1: an upgrade is a GET with Upgrade:
// websocket, Connection: Upgrade, a Sec-WebSocket-Key and
// Sec-WebSocket-Version 13.
//
// A broker has a TLS listener and a ws listener. Once every probe's socket
// has closed, the ws listener is sent, each on a connection of its own, a
// GET with no upgrade, a POST of an upgrade, an upgrade of version 8, and
// one with no key: each is answered an HTTP error, a status of 400 or
// more, and closed. Then a plaintext CONNECT to the TLS listener:
// tls_handshake rises by one, and every other count of a refusal or a
// disconnect is as it was; the first line logged since the requests names
// the TLS error.
func TestInvalidWebSocketUpgradeGetsAnHTTPError(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}, WS: []harness.WS{{}}})
	want := baselineClosures(t, b)
	logged := len(b.Log())

	const upgradeHeaders = "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Protocol: mqtt\r\n"
	const key = "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
	host := "Host: " + b.WS[0] + "\r\n"
	for _, c := range []struct{ name, request string }{
		{"a GET with no upgrade", "GET / HTTP/1.1\r\n" + host + "\r\n"},
		{"a POST of an upgrade", "POST / HTTP/1.1\r\n" + host + upgradeHeaders + key + "Sec-WebSocket-Version: 13\r\nContent-Length: 0\r\n\r\n"},
		{"an upgrade of version 8", "GET / HTTP/1.1\r\n" + host + upgradeHeaders + key + "Sec-WebSocket-Version: 8\r\n\r\n"},
		{"an upgrade with no key", "GET / HTTP/1.1\r\n" + host + upgradeHeaders + "Sec-WebSocket-Version: 13\r\n\r\n"},
	} {
		got := sentToClose(t, "tcp", b.WS[0], []byte(c.request))
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(got)), nil)
		if err != nil {
			t.Errorf("%s was answered %q before the close (%v); want an HTTP error", c.name, got, err)
			continue
		}
		_ = resp.Body.Close() // read from memory: nothing to release
		if resp.StatusCode < http.StatusBadRequest {
			t.Errorf("%s was answered %s; want an HTTP error", c.name, resp.Status)
		}
	}
	awaitSeries(t, b, map[string]float64{connectionsOpen: 0})

	connect := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true}, harness.V5)
	sentToClose(t, "tcp", b.Addr, connect)
	want[tlsRefused]++
	var got map[string]float64
	if !testkit.Within(harness.DefaultTimeout, func() bool { got = closures(t, b); return maps.Equal(got, want) }) {
		t.Errorf("after %v, the counts of refusals and disconnects are %v; want %v: the invalid upgrades counted nowhere, the plaintext CONNECT as tls_handshake", harness.DefaultTimeout, got, want)
	}
	marker := handshakeError(t, &certs.server.Certificate, connect)
	var since string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		since = b.Log()[logged:]
		return strings.Contains(since, marker)
	}) {
		t.Fatalf("watchdog: no log line names %q, the plaintext CONNECT's TLS error, within %v; the log since the invalid upgrades:\n%s", marker, harness.DefaultTimeout, since)
	}
	if first, _, _ := strings.Cut(since, "\n"); !strings.Contains(first, marker) {
		t.Errorf("lines were logged for invalid upgrades, before the plaintext CONNECT's; want none:\n%s", since)
	}
}

// config.md, A listener: tls (tcp, ws), "cert_file and key_file";
// design.md, Flows, Transport setup: "A listener with tls speaks only
// TLS". MQTT 5 chapter 6 and 4.2: MQTT over WebSocket, which runs over TLS
// as wss (RFC 6455 3).
//
// A broker's one ws listener has tls with the test's server certificate.
// An MQTT 3.1.1 and an MQTT 5 client, each trusting the test's authority
// alone, complete the TLS handshake, the broker presenting the
// certificate configured, and the upgrade, answered mqtt, and are
// connected, CONNACK 0x00. The 3.1.1 client subscribes at QoS 1; the MQTT
// 5 client publishes at QoS 1, PUBACK 0x00; the subscriber is sent the
// message.
func TestWebSocketOverTLS(t *testing.T) {
	certs := newPKI(t)
	block := certs.listener(&harness.TLS{})
	b := support.Launch(t, &harness.Options{WS: []harness.WS{{TLS: &block}}})
	d := overMQTT()
	d.TLS = certs.client(nil)
	clients := make([]*harness.Client, 2)
	for i, version := range []byte{harness.V311, harness.V5} {
		clients[i] = dialWS(t, b.WS[0], version, d)
		state := webSocket(clients[i]).Conn.(*tls.Conn).ConnectionState()
		if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, certs.server.Certificate.Certificate[0]) {
			t.Errorf("the broker presented %d certificates, not the one configured first", len(state.PeerCertificates))
		}
		if got := webSocket(clients[i]).Response.Header.Get("Sec-WebSocket-Protocol"); got != "mqtt" {
			t.Errorf("the subprotocol answered over TLS is %q; want mqtt", got)
		}
		harness.ConnectedOver(t, clients[i], &harness.Connect{ClientID: clientID(t, harness.VersionName(version)), CleanStart: true})
	}
	support.Suback(t, clients[0], nil, []harness.Subscription{{Filter: "wss/t", QoS: 1}}, 1)
	support.Acknowledged(t, clients[1], &harness.Publish{QoS: 1, Topic: "wss/t", Payload: []byte("over wss")})
	support.DeliveredAt(t, clients[0], 1, "wss/t", []byte("over wss"))
}

// design.md, Flows, Transport setup: a ws listener's "TLS handshake, where
// it has one, and the upgrade both run within the CONNECT deadline
// (CONNECT). A connection still unupgraded at that deadline is counted as
// connect_timeout"; CONNECT, step 1: "If no CONNECT arrives within
// connect_timeout, the connection closes. CONNECT's deadline starts when
// the connection receives its service permit. It includes transport setup";
// metrics.md: connect_timeout, "no CONNECT in time", and tls_handshake, "a
// failed TLS handshake". design.md, Testing: "A deadline test controls or
// observes the event starting that deadline"; the permit follows the dial,
// so the close comes no sooner than connect_timeout after it, and the
// watchdog is the read's.
//
// With connect_timeout 1s, a client sends a ws listener with tls half a
// ClientHello, and another sends a ws listener half the headers of an
// upgrade, then nothing. The broker closes each connection, no sooner than
// connect_timeout after its dial; connect_timeout rises by two and
// tls_handshake stays as it was.
func TestWebSocketSetupIsCutAtTheConnectDeadline(t *testing.T) {
	t.Parallel()
	const timeout = time.Second // config.md: "every other duration is at least 1s"
	certs := newPKI(t)
	block := certs.listener(&harness.TLS{})
	b := support.OpsLaunch(t, &harness.Options{ConnectTimeout: timeout, WS: []harness.WS{{TLS: &block}, {}}})
	before := baseline(t, b, tlsRefused, timedOut)
	for _, c := range []struct {
		name, addr string
		sent       []byte
	}{
		{"half a ClientHello to the wss listener", b.WS[0], halfAClientHello},
		{"half an upgrade's headers to the ws listener", b.WS[1], []byte("GET / HTTP/1.1\r\nHost: " + b.WS[1] + "\r\nUpgrade: websocket\r\n")},
	} {
		dialled := time.Now()
		sentToClose(t, "tcp", c.addr, c.sent) // whatever comes before the close, the close is what counts
		if waited := time.Since(dialled); waited < timeout {
			t.Errorf("%s: closed %v after the dial, before connect_timeout's %v", c.name, waited, timeout)
		}
	}
	awaitSeries(t, b, map[string]float64{timedOut: before[timedOut] + 2, tlsRefused: before[tlsRefused]})
}

// design.md, R1: "HTTP/2 is off". RFC
// 7301 3.2: the server selects one of the client's ALPN protocols, or
// none; without one, the client speaks the HTTP/1.1 it assumed; RFC 9113
// 3.2: h2 is HTTP/2 over TLS.
//
// The ws listener is sent the HTTP/2 preface: what comes back is an end of
// stream, a reset, or an answer beginning "HTTP/1." (refusesHTTP2). A
// client of the wss listener offering ALPN h2 then http/1.1 negotiates
// http/1.1 or nothing, never h2, and its upgrade then succeeds, its client
// connected, CONNACK 0x00.
func TestWebSocketListenersRefuseHTTP2(t *testing.T) {
	certs := newPKI(t)
	block := certs.listener(&harness.TLS{})
	b := support.Launch(t, &harness.Options{WS: []harness.WS{{}, {TLS: &block}}})
	conn, err := net.DialTimeout("tcp", b.WS[0], harness.DefaultTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // the test is over
	refusesHTTP2(t, conn)

	d := overMQTT()
	d.TLS = certs.client(nil)
	d.TLS.NextProtos = []string{"h2", "http/1.1"}
	c := dialWS(t, b.WS[1], harness.V5, d)
	if got := webSocket(c).Conn.(*tls.Conn).ConnectionState().NegotiatedProtocol; got != "http/1.1" && got != "" {
		t.Errorf("offered h2 and http/1.1, the wss listener negotiated %q; want http/1.1 or none", got)
	}
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
}

// design.md, Closing a connection: "The writer handles the DISCONNECT and
// orderly close"; "The writer writes the last packet, ends its sending and
// stops the reader"; "A client that stops in time sees an orderly close";
// "When the close begins, it fixes one deadline limits.write_timeout later";
// "At the deadline, the socket is closed whatever is left"; CONNECT, 5: "An
// old connection for the id gets DISCONNECT 0x8E on MQTT 5 and is closed";
// R1: "MQTT output and the orderly close stay with the writer"; Flows,
// Transport setup: "Closing a WebSocket connection ends the stream as on
// TCP, with no WebSocket Close frame".
//
// With write_timeout 5s, an MQTT 5 client on the ws listener is taken over
// by another. It reads DISCONNECT 0x8E, then the end of the stream, no
// Close frame (harness.ErrCloseFrame) and not a reset, sooner than
// write_timeout after the takeover's CONNECT was sent: the broker ended its
// sending, not the deadline its socket. The client then ends its own
// sending.
func TestWebSocketCloseIsOrderly(t *testing.T) {
	const timeout = 5 * time.Second
	b := support.Launch(t, &harness.Options{WriteTimeout: timeout, WS: []harness.WS{{}}})
	id := clientID(t, "")
	c := dialWS(t, b.WS[0], harness.V5, overMQTT())
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: id, CleanStart: true})
	taker := dialWS(t, b.WS[0], harness.V5, overMQTT())
	takeover := time.Now() // the CONNECT that takes over is sent after it
	harness.ConnectedOver(t, taker, &harness.Connect{ClientID: id, CleanStart: true})
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8E {
		t.Fatalf("the client taken over: DISCONNECT 0x%02X, want 0x8E", d.Reason)
	}
	harness.ExpectClosed(t, c)
	if took := time.Since(takeover); took >= timeout {
		t.Errorf("the end of the stream came %v after the takeover; want sooner than write_timeout's %v, the broker ending its sending", took, timeout)
	}
	if err := c.CloseWrite(); err != nil {
		t.Errorf("ending the client's sending: %v", err)
	}
}

// design.md, R1: "The WebSocket library, github.com/gorilla/websocket,
// answers a ping or a protocol error from that reader itself, each such
// write within the library's own one-second limit." RFC 6455 5.5.2: "Upon
// receipt of a Ping frame, an endpoint MUST send a Pong frame in
// response"; 5.5.3: "A Pong frame sent in response to a Ping frame must
// have identical "Application data" as found in the message body of the
// Ping frame being replied to". The reader answers the Ping before it reads
// on, so the Pong comes before anything the frames after it bring.
//
// A client connected over the ws listener sends a Ping frame, then a
// PINGREQ. By the PINGRESP, it has read one Pong, holding the Ping's
// data.
func TestWebSocketPingGetsAPong(t *testing.T) {
	b := support.Launch(t, &harness.Options{WS: []harness.WS{{}}})
	c := dialWS(t, b.WS[0], harness.V5, overMQTT())
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	data := []byte("are you there")
	if err := webSocket(c).WriteFrame(true, harness.OpPing, data); err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
	if got := webSocket(c).Pongs(); !slices.EqualFunc(got, [][]byte{data}, bytes.Equal) {
		t.Errorf("by the PINGRESP, the Pongs read hold %q; want one, %q", got, data)
	}
}

// design.md, Restart, Graceful stop: "Every client gets 0x8B, except one
// whose DISCONNECT the core has taken"; "Main waits until every reader,
// writer and dialer has ended, including WebSocket ones"; Access and
// reload: "SIGINT and SIGTERM stop gracefully"; Restart, A stop for a
// failure: "The exit status is 0 after a stop requested by SIGINT or
// SIGTERM"; Flows, Transport setup: "Closing a WebSocket connection ends
// the stream as on TCP, with no WebSocket Close frame".
//
// An MQTT 5 client is connected over the ws listener when the broker is
// sent SIGTERM. It reads DISCONNECT 0x8B, then the end of the stream, and
// closes its end; the broker exits 0 within the graceful stop's bound
// (exited).
func TestGracefulStopEndsWebSocketConnections(t *testing.T) {
	b := support.Launch(t, &harness.Options{WS: []harness.WS{{}}})
	c := dialWS(t, b.WS[0], harness.V5, overMQTT())
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	shutDown(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}
