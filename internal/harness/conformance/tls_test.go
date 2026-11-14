package conformance

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of MQTT over TLS (item 0013a). config.md, A listener, tls:
// "cert_file and key_file, written together; min_version, 1.2 (default) or
// 1.3; client_ca_file, which asks for client certificates;
// require_certificate, true by default with a CA and refused without one.
// [...] A certificate and key that do not load as a pair, or a client CA
// file that holds no certificate, stop the start, which names the listener,
// the file and the error". design.md, Flows, Transport setup: "A listener with tls
// speaks only TLS. The handshake runs within the CONNECT deadline. A failed
// handshake closes the connection, is logged naming the TLS error, and is
// counted as refused (tls_handshake). A handshake still unfinished at that
// deadline is counted as connect_timeout." Access and reload: "A verified
// client certificate is the whole proof. Its Common Name, or its first DNS
// name if there is no Common Name, is the user name. The password file is
// not consulted. When a verified certificate has neither name, or its name
// is not valid UTF-8 or holds a control character, the broker refuses the
// CONNECT with 0x86 and logs it"; "Otherwise the user name is the password user". MQTT 5
// 4.2 and MQTT 3.1.1 4.2 name TLS a transport for MQTT; 5.4.1: "Where TLS
// [RFC5246] is used, TLS Certificates sent from the Client can be used by
// the Server to authenticate the Client". Every certificate is made here
// with Go's own crypto (testkit.Authority), never with the broker's code.

// tlsRefused and timedOut are saguin_connections_refused_total's
// tls_handshake, "a failed TLS handshake", and connect_timeout, "no CONNECT
// in time" (metrics.md, Connections and sessions).
var (
	tlsRefused = refusedSeries("tls_handshake")
	timedOut   = refusedSeries("connect_timeout")
)

// pki is a test's certificates: an authority, the server certificate it
// issued for the loopback address every listener of the harness takes,
// and both written as the files a configuration names.
type pki struct {
	ca                        *testkit.Authority
	server                    *testkit.Issued
	dir                       string
	caFile, certFile, keyFile string
}

// newPKI makes a test's authority and server certificate, in files of a
// folder of the test's own.
func newPKI(t *testing.T) *pki {
	t.Helper()
	ca, err := testkit.NewAuthority("Sagüin test authority")
	if err != nil {
		t.Fatalf("making the authority: %v", err)
	}
	dir := t.TempDir()
	server := issue(t, ca, &testkit.Leaf{CommonName: "saguin", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
	return &pki{ca: ca, server: server, dir: dir,
		caFile:   testkit.WriteFile(t, dir, "ca.pem", string(ca.PEM)),
		certFile: testkit.WriteFile(t, dir, "server.crt", string(server.CertPEM)),
		keyFile:  testkit.WriteFile(t, dir, "server.key", string(server.KeyPEM))}
}

// issue has ca issue a certificate naming what leaf names, failing the
// test where it cannot.
func issue(t *testing.T, ca *testkit.Authority, leaf *testkit.Leaf) *testkit.Issued {
	t.Helper()
	issued, err := ca.Issue(leaf)
	if err != nil {
		t.Fatalf("issuing a certificate for %+v: %v", leaf, err)
	}
	return issued
}

// listener is a tls block serving p's server certificate, with the other
// keys with sets.
func (p *pki) listener(with *harness.TLS) harness.TLS {
	block := *with
	block.CertFile, block.KeyFile = p.certFile, p.keyFile
	return block
}

// requiring is a tls block serving p's server certificate and asking for
// client certificates p's authority issued, require_certificate left to
// its default, true with a CA.
func (p *pki) requiring() harness.TLS {
	return p.listener(&harness.TLS{ClientCAFile: p.caFile})
}

// client is the TLS configuration of a client trusting p's authority alone
// to verify the server, presenting cert where it is not nil. It presents cert
// whatever authorities the server names: with Certificates instead, Go's
// client sends nothing when no certificate matches the server's list
// (crypto/tls, Config.Certificates), so another authority's certificate would
// never reach the broker.
func (p *pki) client(cert *testkit.Issued) *tls.Config {
	tlsConfig := &tls.Config{RootCAs: p.ca.Pool()}
	if cert != nil {
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &cert.Certificate, nil
		}
	}
	return tlsConfig
}

// dialTLS dials addr over TLS with tlsConfig, its MQTT in version, failing
// the test where the handshake fails. The connection is closed when the test
// ends.
func dialTLS(t *testing.T, addr string, version byte, tlsConfig *tls.Config) *harness.Client {
	t.Helper()
	c, err := harness.DialTLS(nil, addr, version, tlsConfig)
	if err != nil {
		t.Fatalf("the TLS handshake with %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	return c
}

// baseline is series as /metrics reads them once b is quiescent: a baseline
// that no connection but a connected client's can still move.
func baseline(t *testing.T, b *harness.Broker, series ...string) map[string]float64 {
	t.Helper()
	values := quiescent(t, b)
	got := make(map[string]float64, len(series))
	for _, s := range series {
		v, ok := values[s]
		if !ok {
			t.Fatalf("the scrape has no series %s", s)
		}
		got[s] = v
	}
	return got
}

// baselineClosures is closures as /metrics reads them once b is quiescent,
// the before that closuresSince and every other comparison of closures
// start from.
func baselineClosures(t *testing.T, b *harness.Broker) map[string]float64 {
	t.Helper()
	return named(quiescent(t, b), closureMetrics...)
}

// quiescent waits, under harness.DefaultTimeout as the watchdog, until every
// socket b has accepted is a connected client's or has had its close
// counted, and returns every series of that scrape, each named as
// testkit.SeriesKey names it. Its first users are the probes
// harness.Broker.Listening sends, one per listener, each closed before
// Launch returns, before sending a byte or, on a unix listener with
// proxy_protocol, after a LOCAL header: their counts can come after
// Launch returns, so a baseline read too soon takes one in, or not, by
// the machine's speed. A probe is a quiet one where design.md, Transport
// setup, counts nothing for it: on a unix listener with proxy_protocol, a
// LOCAL command "closes the connection quietly, unserved: it is neither
// logged nor counted"; on a ws listener without tls, a request "that is
// not a valid WebSocket upgrade gets the library's own HTTP error and is
// closed, neither logged nor counted". On a tls listener, ws or not, a
// close "before the handshake finishes" is "counted as connection_lost
// [...], as on a plain listener".
//
// metrics.md, Connections and sessions: saguin_connections_accepted_total
// counts "sockets accepted, refused ones included";
// saguin_connections_open "sockets served, from their permit to their
// close"; saguin_connections{protocol} "connected clients [...]: the
// sessions the core holds connected"; a closure is counted once, under
// saguin_connections_refused_total or saguin_disconnects_total (closures).
// metrics.md, Rules:
// "A counter is an atomic changed by the goroutine that sees its event",
// so no order between a socket's close and its count is assumed. b is
// quiescent when, in one scrape:
//   - the sockets accepted are at least the probes;
//   - the sockets open are the connected clients: none is in its setup,
//     its CONNECT's check or its close;
//   - the sockets accepted are the closures counted, plus the sockets
//     open, plus the quiet probes: every accepted socket not open has had
//     its close counted.
func quiescent(t *testing.T, b *harness.Broker) map[string]float64 {
	t.Helper()
	cfg, err := config.Load(b.Config)
	if err != nil {
		t.Fatal(err)
	}
	listen := cfg.Broker.MQTT.Listen
	probes, quiet := len(listen.TCP)+len(listen.WS)+len(listen.Unix), 0
	for _, l := range listen.WS {
		if l.TLS == nil {
			quiet++
		}
	}
	for _, l := range listen.Unix {
		if l.ProxyProtocol {
			quiet++
		}
	}
	var values map[string]float64
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		values = testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics())).Values
		counted := 0.0
		for _, v := range named(values, closureMetrics...) {
			counted += v
		}
		open := values[connectionsOpen]
		return values[accepted] >= float64(probes) && open == values[connected5]+values[connected311] &&
			values[accepted] == counted+open+float64(quiet)
	}) {
		t.Fatalf("watchdog: after %v, /metrics reads %v accepted, %v open, %v and %v connected, closures %v; want the %d probes accepted, every open socket a connected client's, and every other accepted socket's close counted but the %d quiet probes'",
			harness.DefaultTimeout, values[accepted], values[connectionsOpen], values[connected5], values[connected311],
			named(values, closureMetrics...), probes, quiet)
	}
	return values
}

// handshakeRefused fails the test unless a client dialling addr over TLS
// with tlsConfig is refused at the handshake, getting no CONNACK: its
// handshake fails, or, in TLS 1.3, where the client's part of the
// handshake ends before the server has checked it, its CONNECT is answered
// by the end of the connection. what names the client.
func handshakeRefused(t *testing.T, addr string, tlsConfig *tls.Config, what string) {
	t.Helper()
	c, err := harness.DialTLS(nil, addr, harness.V5, tlsConfig)
	if err != nil {
		return
	}
	defer func() { _ = c.Close() }() // refused or not, the client is done
	if ack, err := c.Connect(&harness.Connect{ClientID: clientID(t, ""), CleanStart: true}); err == nil {
		t.Errorf("%s: CONNACK 0x%02X; want the TLS handshake refused", what, ack.Reason)
	}
}

// sentToClose dials addr on network ("tcp", plain, or "unix"), sends sent,
// and returns what it reads until the broker closes the connection, in
// order or with a reset, failing the test where it is still open after
// harness.DefaultTimeout, the watchdog. The connection is closed when the
// test ends.
func sentToClose(t *testing.T, network, addr string, sent []byte) []byte {
	t.Helper()
	conn, err := net.DialTimeout(network, addr, harness.DefaultTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // the test is over
	if _, err := conn.Write(sent); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(harness.DefaultTimeout)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(conn)
	if err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("watchdog: reading to the close: %v; want the connection closed within %v", err, harness.DefaultTimeout)
	}
	return got
}

// presented fails the test unless the certificate the server presented on
// conn, a TLS client's connection, is want's.
func presented(t *testing.T, conn net.Conn, want *testkit.Issued) {
	t.Helper()
	state := conn.(*tls.Conn).ConnectionState()
	if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, want.Certificate.Certificate[0]) {
		t.Errorf("the broker presented %d certificates, not the one configured", len(state.PeerCertificates))
	}
}

// design.md, Transport setup: "A listener with tls speaks only TLS";
// config.md, tls: "cert_file and key_file". MQTT 5 4.2 and 3.1.1 4.2:
// TLS is a transport for MQTT, which carries every packet unchanged.
//
// A broker's one listener has tls with the test's server certificate. An
// MQTT 3.1.1 and an MQTT 5 client, each trusting the test's authority
// alone, complete the handshake, the broker presenting the certificate
// configured, and are connected, CONNACK 0x00. The 3.1.1 client subscribes
// at QoS 1; the MQTT 5 client publishes at QoS 1, PUBACK 0x00; the
// subscriber is sent the message.
func TestTLSListenerServesMQTT(t *testing.T) {
	certs := newPKI(t)
	b := support.Launch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}})
	sub := dialTLS(t, b.Addr, harness.V311, certs.client(nil))
	presented(t, sub.Conn, certs.server)
	harness.ConnectedOver(t, sub, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "tls/t", QoS: 1}}, 1)
	pub := dialTLS(t, b.Addr, harness.V5, certs.client(nil))
	presented(t, pub.Conn, certs.server)
	harness.ConnectedOver(t, pub, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "tls/t", Payload: []byte("over TLS")})
	support.DeliveredAt(t, sub, 1, "tls/t", []byte("over TLS"))
}

// handshakeError is the error Go's own TLS server reports for a client
// whose first bytes are sent, serving cert: what a TLS listener's
// handshake meets, judged by crypto/tls, never by the broker's code.
func handshakeError(t *testing.T, cert *tls.Certificate, sent []byte) string {
	t.Helper()
	server, client := net.Pipe()
	go func() {
		_, _ = client.Write(sent) // what the handshake reads of it; the rest ends with the pipe
		_ = client.Close()
	}()
	err := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{*cert}}).Handshake()
	_ = server.Close() // the handshake is over
	if err == nil {
		t.Fatalf("Go's TLS server completed a handshake with % X", sent)
	}
	return err.Error()
}

// design.md, Transport setup: "A listener with tls speaks only TLS. [...] A
// failed handshake closes the connection, is logged naming the TLS error,
// and is counted as refused (tls_handshake)"; Logging: "A failed TLS
// handshake [...] and a refused client certificate [...] are each one WARN
// line"; metrics.md, saguin_connections_refused_total: "tls_handshake (a
// failed TLS handshake)". Item 0013a (261019 054601, from audit finding
// 261018-101a): "a TLS listener answers no plaintext MQTT".
//
// A client sends a plaintext MQTT 5 CONNECT to a TLS listener. It is sent
// no CONNACK, and the connection ends. tls_handshake rises by one
// from its baseline. Once the broker has stopped, one WARN line of its log
// names the error Go's TLS server reports for those bytes.
func TestPlaintextOnATLSListenerIsRefused(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}})
	before := baseline(t, b, tlsRefused)
	connect := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true}, harness.V5)
	got := sentToClose(t, "tcp", b.Addr, connect)
	if p, err := harness.ReadPacket(bytes.NewReader(got), harness.V5, 0); err == nil {
		if ack, ok := p.(*harness.Connack); ok {
			t.Errorf("the plaintext CONNECT was answered with CONNACK 0x%02X; want no CONNACK: the listener speaks only TLS", ack.Reason)
		}
	}
	awaitSeries(t, b, map[string]float64{tlsRefused: before[tlsRefused] + 1})
	want := handshakeError(t, &certs.server.Certificate, connect)
	stopped(t, b)
	log := b.Log()
	if lines := testkit.LinesNaming(log, "WARN", want); len(lines) != 1 {
		t.Errorf("WARN lines naming the TLS error of a plaintext CONNECT, %q: %q; want one; the log:\n%s", want, lines, log)
	}
}

// config.md, tls: "min_version, 1.2 (default) or 1.3"; design.md,
// Transport setup: a failed handshake "is counted as refused
// (tls_handshake)".
//
// Listener tcp1 has min_version 1.3, tcp2 leaves it to 1.2. On each, a
// client capped one version below the minimum (TLS 1.2 on tcp1, TLS 1.1 on
// tcp2) is refused at the handshake, and a client at the minimum is
// connected, CONNACK 0x00. tls_handshake rises by the two refused: the
// broker refused them, not the clients themselves.
func TestTLSMinVersionIsHonoured(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{TCPListeners: 2,
		ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{MinVersion: "1.3"}), certs.listener(&harness.TLS{})}})
	before := baseline(t, b, tlsRefused)
	for i, c := range []struct {
		minimum, below uint16
	}{
		{tls.VersionTLS13, tls.VersionTLS12},
		{tls.VersionTLS12, tls.VersionTLS11},
	} {
		capped := certs.client(nil)
		capped.MinVersion, capped.MaxVersion = c.below, c.below
		handshakeRefused(t, b.Addrs[i], capped, fmt.Sprintf("a client capped at %s on a listener whose minimum is %s",
			tls.VersionName(c.below), tls.VersionName(c.minimum)))
		at := certs.client(nil)
		at.MinVersion, at.MaxVersion = c.minimum, c.minimum
		harness.ConnectedOver(t, dialTLS(t, b.Addrs[i], harness.V5, at),
			&harness.Connect{ClientID: clientID(t, "-"+strconv.Itoa(i)), CleanStart: true})
	}
	awaitSeries(t, b, map[string]float64{tlsRefused: before[tlsRefused] + 2})
}

// config.md, tls: "client_ca_file, which asks for client certificates;
// require_certificate, true by default with a CA"; design.md, Transport
// setup: a failed handshake "is counted as refused (tls_handshake)". Item
// 0013a (261019 054601, from audit finding 261018-101a): "one requiring a
// client certificate admits no client without a valid one".
//
// A listener has a client CA, require_certificate left to its default. A
// client presenting no certificate, and one presenting a certificate
// another authority issued, are each refused at the handshake, never sent
// a CONNACK, and tls_handshake rises by two. A client presenting a
// certificate the listener's authority issued is connected, CONNACK 0x00.
func TestRequiredCertificateIsEnforcedAtTheHandshake(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.requiring()}})
	before := baseline(t, b, tlsRefused)
	stranger, err := testkit.NewAuthority("another authority")
	if err != nil {
		t.Fatal(err)
	}
	handshakeRefused(t, b.Addr, certs.client(nil), "a client presenting no certificate")
	handshakeRefused(t, b.Addr, certs.client(issue(t, stranger, &testkit.Leaf{CommonName: "stranger"})),
		"a client presenting another authority's certificate")
	harness.ConnectedOver(t, dialTLS(t, b.Addr, harness.V5, certs.client(issue(t, certs.ca, &testkit.Leaf{CommonName: "device-1"}))),
		&harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	awaitSeries(t, b, map[string]float64{tlsRefused: before[tlsRefused] + 2})
}

// config.md, tls: "client_ca_file, which asks for client certificates;
// require_certificate"; design.md, Access and reload: "Otherwise the user
// name is the password user"; CONNECT's refusal table: 0x86, "a wrong or
// missing credential".
//
// A listener asks for client certificates with require_certificate false;
// the broker has a password file holding alice. Clients presenting no
// certificate complete the handshake and are judged by the password file:
// alice with her password is connected, CONNACK 0x00; alice with another
// is refused 0x86.
func TestOptionalCertificateLeavesThePasswordFile(t *testing.T) {
	certs := newPKI(t)
	file := passwordFile(t, t.TempDir(), "passwd", "alice:"+sixHash("alice-pw", salt12))
	b := support.Launch(t, &harness.Options{Login: harness.Login{PasswordFile: file},
		ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{ClientCAFile: certs.caFile, RequireCertificate: "false"})}})
	harness.ConnectedOver(t, dialTLS(t, b.Addr, harness.V5, certs.client(nil)), credentials(clientID(t, "-right"), "alice", "alice-pw"))
	connackRefusesOver(t, dialTLS(t, b.Addr, harness.V5, certs.client(nil)), credentials(clientID(t, "-wrong"), "alice", "not-alice-pw"), 0x86)
}

// config.md, tls: "When require_certificate is false, a client may present
// no certificate, but a certificate it does present must verify, or the
// handshake fails"; design.md, Transport setup: a failed handshake "is
// counted as refused (tls_handshake)".
//
// A listener asks for client certificates with require_certificate false.
// A client presenting a certificate another authority issued is refused at
// the handshake, never sent a CONNACK, and tls_handshake rises by one. A
// client presenting a certificate the listener's authority issued is
// connected, CONNACK 0x00.
func TestOptionalCertificateThatFailsVerificationIsRefused(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{
		ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{ClientCAFile: certs.caFile, RequireCertificate: "false"})}})
	before := baseline(t, b, tlsRefused)
	stranger, err := testkit.NewAuthority("another authority")
	if err != nil {
		t.Fatal(err)
	}
	handshakeRefused(t, b.Addr, certs.client(issue(t, stranger, &testkit.Leaf{CommonName: "stranger"})),
		"a client presenting another authority's certificate")
	harness.ConnectedOver(t, dialTLS(t, b.Addr, harness.V5, certs.client(issue(t, certs.ca, &testkit.Leaf{CommonName: "device-1"}))),
		&harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	awaitSeries(t, b, map[string]float64{tlsRefused: before[tlsRefused] + 1})
}

// design.md, Access and reload: "A verified client certificate is the
// whole proof. Its Common Name, or its first DNS name if there is no
// Common Name, is the user name. The password file is not consulted";
// storage.md, sessions: user, "the proved user name".
//
// A listener requires client certificates; the broker has a password file
// holding alice. Each client connects a durable session and is connected,
// CONNACK 0x00, and its stored session's user is its certificate's name: a
// certificate with a Common Name and a DNS name, its CONNECT naming alice
// with a wrong password, gives the Common Name; one with two DNS names and
// no Common Name, its CONNECT carrying no user name, gives the first DNS
// name.
func TestVerifiedCertificateIsTheWholeProof(t *testing.T) {
	certs := newPKI(t)
	file := passwordFile(t, t.TempDir(), "passwd", "alice:"+sixHash("alice-pw", salt12))
	b := support.Launch(t, &harness.Options{Login: harness.Login{PasswordFile: file}, ListenerTLS: []harness.TLS{certs.requiring()}})
	db := support.OpenDB(t, b.DB)
	for _, c := range []struct {
		name    string
		leaf    testkit.Leaf
		connect func(client string) *harness.Connect
		user    string
	}{
		{"a Common Name, a wrong password sent", testkit.Leaf{CommonName: "device-7", DNSNames: []string{"dns-7.example"}},
			func(id string) *harness.Connect { return credentials(id, "alice", "not-alice-pw") }, "device-7"},
		{"DNS names alone, no user name sent", testkit.Leaf{DNSNames: []string{"first.example", "second.example"}},
			func(id string) *harness.Connect { return &harness.Connect{ClientID: id, CleanStart: true} }, "first.example"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := c.connect(clientID(t, ""))
			p.Props = support.Durable
			harness.ConnectedOver(t, dialTLS(t, b.Addr, harness.V5, certs.client(issue(t, certs.ca, &c.leaf))), p)
			var stored string
			if err := db.QueryRowContext(t.Context(), `SELECT user FROM sessions WHERE client = ?`, p.ClientID).Scan(&stored); err != nil {
				t.Fatalf("reading the session's user: %v", err)
			}
			if stored != c.user {
				t.Errorf("the session's user is %q; want %q, its certificate's name", stored, c.user)
			}
		})
	}
}

// design.md, Access and reload: "When a verified certificate has neither
// name, or its name is not valid UTF-8 or holds a control character, the
// broker refuses the CONNECT with 0x86 and logs it"; Logging: "A failed TLS
// handshake [...] and a refused client certificate [...] are each one WARN
// line"; CONNECT's refusal table: 0x86, MQTT 3.1.1 return code 4. A name
// not valid UTF-8 has no case here: no certificate can carry one to the
// broker, Go's certificate parser refusing a UTF8String that is not UTF-8
// and reading a T61String as Latin-1.
//
// A listener requires client certificates, with no password file. Clients
// whose certificates the listener's authority issued complete the
// handshake: one naming nothing, and one whose Common Name holds a control
// character. In MQTT 5 each is refused with 0x86, in MQTT 3.1.1 with
// return code 4, then closed. Once the broker has stopped, one WARN line of
// its log names each refused client id.
func TestCertificateWithoutAUsableNameIsRefused(t *testing.T) {
	certs := newPKI(t)
	b := support.Launch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.requiring()}})
	leaves := []testkit.Leaf{{}, {CommonName: "dev\x01ce"}}
	versions := []struct{ version, reason byte }{{harness.V5, 0x86}, {harness.V311, 4}}
	refused := make([]string, 0, len(leaves)*len(versions))
	for i, leaf := range leaves {
		for _, v := range versions {
			id := clientID(t, fmt.Sprintf("-%d-%d", i, v.version))
			refused = append(refused, id)
			c := dialTLS(t, b.Addr, v.version, certs.client(issue(t, certs.ca, &leaf)))
			connackRefusesOver(t, c, &harness.Connect{ClientID: id, CleanStart: true}, v.reason)
		}
	}
	stopped(t, b)
	log := b.Log()
	for _, id := range refused {
		if lines := testkit.LinesNaming(log, "WARN", id); len(lines) != 1 {
			t.Errorf("WARN lines naming %q, refused for its certificate's name: %q; want one; the log:\n%s", id, lines, log)
		}
	}
}

// halfAClientHello is the start of a TLS handshake whose rest never comes:
// a handshake record's header (RFC 8446 5.1: type handshake, 22; legacy
// version 0x0301; a length of 200) and the first byte of what it holds, a
// ClientHello (4: client_hello, 1).
var halfAClientHello = []byte{22, 0x03, 0x01, 0x00, 200, 1}

// design.md, Transport setup: "The handshake runs within the CONNECT
// deadline [...] A handshake still unfinished at that deadline is counted
// as connect_timeout"; Flows, CONNECT, step 1: "If no CONNECT arrives
// within connect_timeout, the connection closes. CONNECT's deadline starts
// when the connection receives its service permit. It includes transport
// setup"; metrics.md: connect_timeout, "no CONNECT in time", and
// tls_handshake, "a failed TLS handshake". design.md, Testing: "A deadline
// test controls or observes the event starting that deadline"; the permit
// follows the dial, so the close comes no sooner than connect_timeout after
// it, and the watchdog is the read's.
//
// With connect_timeout 1s, a client dials a TLS listener and sends half a
// ClientHello, then nothing. The broker closes the connection, no sooner
// than connect_timeout after the dial; connect_timeout rises by one and
// tls_handshake stays as it was.
func TestUnfinishedHandshakeIsAConnectTimeout(t *testing.T) {
	const timeout = time.Second // config.md: "every other duration is at least 1s"
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{ConnectTimeout: timeout, ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}})
	before := baseline(t, b, tlsRefused, timedOut)
	dialled := time.Now()
	sentToClose(t, "tcp", b.Addr, halfAClientHello) // whatever comes before the close, the close is what counts
	if waited := time.Since(dialled); waited < timeout {
		t.Errorf("closed %v after the dial, before connect_timeout's %v", waited, timeout)
	}
	awaitSeries(t, b, map[string]float64{timedOut: before[timedOut] + 1, tlsRefused: before[tlsRefused]})
}

// lost is saguin_disconnects_total's connection_lost (metrics.md,
// Connections and sessions).
var lost = disconnectSeries("connection_lost")

// design.md, Transport setup: "When the client closes the connection
// cleanly between TLS records, or resets it, before the handshake finishes,
// the connection is counted as connection_lost and not logged, as on a plain
// listener"; a failed handshake, by contrast, "is logged naming the TLS
// error, and is counted as refused (tls_handshake)".
//
// On a TLS listener, one client closes its connection before sending a
// byte, and another sends half a ClientHello and resets its connection.
// connection_lost rises by two and tls_handshake stays as it was. Once
// both sockets are closed, a plaintext CONNECT, whose failed handshake is
// logged naming its TLS error, marks the log: no line comes before that
// one since the two connections were made.
func TestClosedOrResetBeforeTheHandshakeIsALostConnection(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}})
	before := baseline(t, b, tlsRefused, lost)
	logged := len(b.Log())

	closed, err := net.DialTimeout("tcp", b.Addr, harness.DefaultTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	reset, err := net.DialTimeout("tcp", b.Addr, harness.DefaultTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reset.Write(halfAClientHello); err != nil {
		t.Fatal(err)
	}
	// With a linger of 0, closing sends a reset (socket(7), SO_LINGER).
	if err := reset.(*net.TCPConn).SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := reset.Close(); err != nil {
		t.Fatal(err)
	}
	awaitSeries(t, b, map[string]float64{lost: before[lost] + 2, tlsRefused: before[tlsRefused], connectionsOpen: 0})

	connect := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true}, harness.V5)
	sentToClose(t, "tcp", b.Addr, connect)
	marker := handshakeError(t, &certs.server.Certificate, connect)
	var since string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		since = b.Log()[logged:]
		return strings.Contains(since, marker)
	}) {
		t.Fatalf("watchdog: no log line names %q, the plaintext CONNECT's TLS error, within %v; the log since the two connections:\n%s", marker, harness.DefaultTimeout, since)
	}
	if first, _, _ := strings.Cut(since, "\n"); !strings.Contains(first, marker) {
		t.Errorf("lines were logged for a connection closed before a byte or reset, before the plaintext CONNECT's; want none:\n%s", since)
	}
}

// floodTopic and floodPayload make each delivery of the stuck client's
// flood a small packet: an MQTT 3.1.1 QoS 0 PUBLISH of 75 bytes, which
// TLS writes as a record of its own (design.md, Delivery, Sending: "A TLS
// or WebSocket connection takes them one at a time"). Linux fills a
// socket's buffer to the byte with writes this small, leaving no room for
// a close_notify after the last; behind a write of a few hundred bytes or
// more it can leave room (found on this machine, kernel 7.0, with Go's
// crypto/tls: a close_notify after writes of 200 bytes waited out Go's
// 5 seconds, after writes of 300 bytes it went at once).
const (
	floodTopic   = "stuck/t"
	floodPayload = 64
)

// floodBatch is the broker.storage.publish_commit_max_records the flood is
// stored with: config.md's most, limits.max_connections at its default, so
// the flood takes a few commits rather than one per 256 records.
const floodBatch = 10000

// floodCount is how many floodPayload messages hold more than every TCP
// buffer between the broker and a client that reads nothing: the most a
// socket's send buffer grows to on this machine (sendBufferMost), counted
// in payloads; each packet, longer than its payload by its headers and
// TLS's, holds more again than the client's receive buffer, the smallest
// the system allows.
func floodCount(t *testing.T) int {
	t.Helper()
	return sendBufferMost(t)/floodPayload + 1
}

// sendBufferMost is the most a TCP socket's send buffer grows to on this
// machine, in bytes: tcp_wmem's third field (tcp(7)).
func sendBufferMost(t *testing.T) int {
	t.Helper()
	const path = "/proc/sys/net/ipv4/tcp_wmem"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 3 {
		t.Fatalf("%s holds %q; want its minimum, default and maximum", path, data)
	}
	most, err := strconv.Atoi(fields[2])
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return most
}

// unread is how many bytes c's socket has received that c has not read
// (tcp(7): SIOCINQ, which Linux numbers as TIOCINQ, "the amount of queued
// unread data in the receive buffer"); c is a TLS client.
func unread(t *testing.T, c *harness.Client) int {
	t.Helper()
	raw, err := c.Conn.(*tls.Conn).NetConn().(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var n int32
	var ioErr error
	if err := raw.Control(func(fd uintptr) { ioErr = ioctl(fd, syscall.TIOCINQ, unsafe.Pointer(&n)) }); err != nil {
		t.Fatal(err)
	}
	if ioErr != nil {
		t.Fatalf("reading the socket's unread bytes: %v", ioErr)
	}
	return int(n)
}

// design.md, Closing a connection: "When the close begins, it fixes one
// deadline limits.write_timeout later. Nothing extends that deadline";
// "At the deadline, the socket is closed whatever is left"; "A close after
// a write past the write timeout has no time left. The socket is closed at
// once"; "A write that has not finished limits.write_timeout after its
// first socket-write attempt closes the connection as writer_full"; CONNECT,
// 5: an old connection taken over "is closed"; DISCONNECT, Reasons: "An
// MQTT 3.1.1 connection just closes". Item 0013a (261013 110314): "Go's TLS
// CloseWrite sets its own 5-second deadline for close_notify, replacing the
// close's one deadline [...] Ending a TLS connection keeps Sagüin's
// deadline; tested: a TLS close finishes by the close deadline".
// metrics.md: saguin_connections_open, "sockets served, from their permit
// to their close".
//
// With write_timeout 1s, an MQTT 3.1.1 client on a TLS listener, its
// buffers the smallest the system allows, subscribes at QoS 0 and reads
// nothing more. Another client publishes to it, in small packets, more
// than every TCP buffer between them holds, so the broker's writes to it
// stop for want of room, with none left for a close_notify either; then a
// QoS 1 publication whose PUBACK says the flood is stored. Once the stuck
// client's socket also holds bytes it has not read, the broker has begun
// writing to it. A third client then takes over its client id. The stuck
// connection's close is due by then: the write under way when the
// takeover comes ends within write_timeout, and the close's own deadline
// is write_timeout from its start. Its socket closes, connections_open
// falling to the other two, within twice write_timeout and a second for
// the takeover's own work and the scrape, sooner than Go's 5-second
// close_notify deadline would let it.
func TestTLSCloseKeepsItsOneDeadline(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})},
		PublishCommitMaxRecords: floodBatch})
	id := clientID(t, "")
	stuck, err := harness.DialTLS(smallBuffers, b.Addr, harness.V311, certs.client(nil))
	if err != nil {
		t.Fatalf("the TLS handshake with %s: %v", b.Addr, err)
	}
	harness.ConnectedOver(t, stuck, &harness.Connect{ClientID: id, CleanStart: true})
	support.Suback(t, stuck, nil, []harness.Subscription{{Filter: floodTopic, QoS: 0}}, 0)
	pub := dialTLS(t, b.Addr, harness.V5, certs.client(nil))
	harness.ConnectedOver(t, pub, &harness.Connect{ClientID: id + "-publisher", CleanStart: true})
	packet := harness.Encoded(t, &harness.Publish{Topic: floodTopic, Payload: bytes.Repeat([]byte{'x'}, floodPayload)}, harness.V5)
	harness.SendBytes(t, pub, bytes.Repeat(packet, floodCount(t)))
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "barrier/t", Payload: []byte("after the flood")})
	if !testkit.Within(harness.DefaultTimeout, func() bool { return unread(t, stuck) > 0 }) {
		t.Fatalf("watchdog: the stuck client's socket received nothing within %v of the flood's storing", harness.DefaultTimeout)
	}
	taker := dialTLS(t, b.Addr, harness.V311, certs.client(nil))
	takenOver := time.Now() // the CONNECT that takes over is sent after it
	harness.ConnectedOver(t, taker, &harness.Connect{ClientID: id, CleanStart: true})
	awaitSeries(t, b, map[string]float64{connectionsOpen: 2})
	if bound, took := 2*support.WriteTimeout+time.Second, time.Since(takenOver); took > bound {
		t.Errorf("the stuck TLS connection closed %v after its takeover; want within %v, its one close deadline (Go's close_notify waits 5s)", took, bound)
	}
}

// recording is a connection that keeps every byte read from it: the raw
// TLS records a client received.
type recording struct {
	net.Conn
	read []byte
}

func (r *recording) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	r.read = append(r.read, p[:n]...)
	return n, err
}

// alertRecord is the content type of a TLS alert record (RFC 5246 6.2.1,
// 7.2), which TLS 1.2 writes in the clear before each record.
const alertRecord = 21

// lastRecordType is the content type of the last whole TLS record in
// stream, a sequence of records each led by a type, a version and a
// length (RFC 5246 6.2.1); 0 for none.
func lastRecordType(stream []byte) byte {
	var last byte
	for len(stream) >= 5 {
		end := 5 + (int(stream[3])<<8 | int(stream[4])) // the header and its length, big-endian
		if end > len(stream) {
			break
		}
		last, stream = stream[0], stream[end:]
	}
	return last
}

// design.md, Closing a connection: "The writer handles the DISCONNECT and
// orderly close"; "The writer writes the last packet, ends its sending and
// stops the reader"; "A client that stops in time sees an orderly close";
// CONNECT, 5: "An old connection for the id gets DISCONNECT 0x8E on MQTT 5
// and is closed". Item 0013a (261012 120048): "the writer's orderly close
// half-closes the socket (CloseWrite). A TLS connection must be able to
// end its sending that way, keeping the same orderly close". TLS, which
// MQTT 4.2 names as RFC 5246, ends a sending with close_notify: "each party
// is required to send a close_notify alert before closing the write side
// of the connection" (RFC 5246 7.2.1).
//
// An MQTT 5 client speaking TLS 1.2, whose records' types travel in the
// clear, is taken over by another. It reads DISCONNECT 0x8E, then the end
// of the stream, not a reset, and the last record it received is an
// alert: the close_notify, since any other alert would have ended its
// read in an error.
func TestTLSCloseIsOrderly(t *testing.T) {
	certs := newPKI(t)
	b := support.Launch(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}})
	raw, err := net.DialTimeout("tcp", b.Addr, harness.DefaultTimeout)
	if err != nil {
		t.Fatal(err)
	}
	wire := &recording{Conn: raw}
	tlsConfig := certs.client(nil)
	tlsConfig.MaxVersion = tls.VersionTLS12
	tlsConfig.ServerName, _, err = net.SplitHostPort(b.Addr) // verified against the server's IP address
	if err != nil {
		t.Fatal(err)
	}
	conn := tls.Client(wire, tlsConfig)
	t.Cleanup(func() { _ = conn.Close() }) // the test is over
	ctx, cancel := context.WithTimeout(t.Context(), harness.DefaultTimeout)
	defer cancel()
	if err := conn.HandshakeContext(ctx); err != nil {
		t.Fatalf("the TLS 1.2 handshake: %v", err)
	}
	c := harness.NewClient(conn, harness.V5)
	id := clientID(t, "")
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: id, CleanStart: true})
	harness.ConnectedOver(t, dialTLS(t, b.Addr, harness.V5, certs.client(nil)), &harness.Connect{ClientID: id, CleanStart: true})
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8E {
		t.Fatalf("the client taken over: DISCONNECT 0x%02X, want 0x8E", d.Reason)
	}
	harness.ExpectClosed(t, c)
	if last := lastRecordType(wire.read); last != alertRecord {
		t.Errorf("the last TLS record before the end of the stream has content type %d; want %d, an alert: close_notify", last, alertRecord)
	}
}

// config.md, tls: "A certificate and key that do not load as a pair, or a
// client CA file that holds no certificate, stop the start, which names the
// listener, the file and the error"; design.md, Restart: "The exit status
// is [...] 1 after [...] a refused start"; Operations, --check-config: "It
// also opens every file the configuration names and checks it as startup
// does: [...] certificates, keys and client CAs. It reports every problem
// it finds, not only the first".
//
// Listener tcp2 has a tls block with one fault: the server's certificate
// with another certificate's key, or a client CA file holding a key and no
// certificate. --check-config exits 1 naming the same. The broker exits 1
// before it listens, and what it printed
// names tcp2 and the file: for the pair, both its files and the error Go's
// tls.LoadX509KeyPair gives for them; for the CA file, that file (an empty
// pool has no error of Go's to name).
func TestStartRefusesTLSFilesThatDoNotLoad(t *testing.T) {
	certs := newPKI(t)
	other := issue(t, certs.ca, &testkit.Leaf{CommonName: "other", Server: true})
	otherKey := testkit.WriteFile(t, certs.dir, "other.key", string(other.KeyPEM))
	_, pairErr := tls.LoadX509KeyPair(certs.certFile, otherKey)
	if pairErr == nil {
		t.Fatal("the server's certificate and another's key load as a pair")
	}
	noCertificate := testkit.WriteFile(t, certs.dir, "no-certificate.pem", string(other.KeyPEM))
	for _, c := range []struct {
		name  string
		tls   harness.TLS
		names []string
	}{
		{"a certificate and another's key", harness.TLS{CertFile: certs.certFile, KeyFile: otherKey},
			[]string{certs.certFile, otherKey, pairErr.Error()}},
		{"a client CA file holding no certificate", certs.listener(&harness.TLS{ClientCAFile: noCertificate}),
			[]string{noCertificate}},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir(), WriteTimeout: support.WriteTimeout,
				TCPListeners: 2, ListenerTLS: []harness.TLS{{}, c.tls}}
			file, err := harness.WriteConfig(o)
			if err != nil {
				t.Fatalf("writing the configuration: %v", err)
			}
			code, stdout, stderr := execute(t, support.BrokerBinary, "--check-config", file)
			if code != 1 {
				t.Errorf("--check-config: exit %d; want 1, the start it checks being refused", code)
			}
			for _, name := range append([]string{"tcp2"}, c.names...) {
				if !strings.Contains(stdout+stderr, name) {
					t.Errorf("--check-config does not name %q:\n%s%s", name, stdout, stderr)
				}
			}
			b, err := harness.Launch(support.BrokerBinary, o)
			if err == nil {
				_ = b.Signal(os.Kill) // a broker that started is the failure; it ends here
				_, _ = b.Wait(support.StopLimit)
				t.Fatalf("the broker started with %s on tcp2", c.name)
			}
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
				t.Errorf("the refused start: %v; want exit status 1", err)
			}
			for _, name := range append([]string{"tcp2"}, c.names...) {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("the refusal does not name %q:\n%v", name, err)
				}
			}
		})
	}
}
