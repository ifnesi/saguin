package conformance

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of bridge links over TLS and WebSocket (item 0019h). config.md,
// bridges.<name>: peer, "the peer's URL: tcp://, tls://, ws:// or wss://,
// [...] ws:// and wss:// may add a path (default /)"; tls (tls, wss),
// "ca_file, the authority for the peer's certificate (default: the system's
// roots); cert_file and key_file, Sagüin's certificate for the peer,
// written together; min_version, 1.2 (default) or 1.3". design.md,
// Bridges: "A bridge is a connection to a peer broker, opened by a dialer.
// On that connection Sagüin is an ordinary MQTT 5 client of the peer"; "One
// connect_timeout deadline covers each establishment attempt, from starting
// the dial through TLS, WebSocket upgrade and CONNACK". MQTT 5 4.2 names
// TLS and WebSocket as transports for MQTT, and chapter 6 says how MQTT
// travels over a WebSocket. The peer (harness.Peer, with
// harness.PeerOptions) serves TLS with Go's crypto/tls and the WebSocket's
// server end with the harness's own framing, never the broker's code; every
// certificate is made here (testkit.Authority).

// transportRules are the rules of a bridge whose transport is under test:
// an in rule taking in/# to from/in/$#, and an out rule taking out/# to
// to/$#, each at QoS 1.
var transportRules = []harness.BridgeRule{support.In("in/#", 1), support.Out("out/#", "to/$#", 1)}

// serving is the TLS configuration of a peer presenting p's server
// certificate, which p's authority issued for the loopback address every
// peer listens on.
func (p *pki) serving() *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{p.server.Certificate}}
}

// transportBridge is a bridge named name to peer with transportRules, and
// block as its tls block, none where nil.
func transportBridge(name string, peer *harness.Peer, block *harness.BridgeTLS) []harness.Bridge {
	bridge := support.BridgeTo(name, peer, transportRules...)
	bridge.TLS = block
	return []harness.Bridge{bridge}
}

// carriedBothWays brings up the link of b's bridge to peer (support.LinkUp),
// then fails the test unless the link carries the bridge's transportRules
// both ways (design.md, Bridges, In: "What arrives goes through the
// ordinary publish path as no user"; Out: "each rule is a durable consumer
// of what its filter matches locally"): a record the peer forwards on in/x
// at QoS 1 is acknowledged and reaches a local subscriber to from/# on
// from/in/x, and one a local client publishes on out/x at QoS 1 reaches the
// peer on to/x. It returns the link.
func carriedBothWays(t *testing.T, b *harness.Broker, peer *harness.Peer) *harness.PeerLink {
	t.Helper()
	l, _, _ := support.LinkUp(t, peer, support.Quiet)
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "transport-sub", CleanStart: true}, support.At(1, "from/#"))
	in := fromPeer(1, 1, "in/x", "from the peer")
	support.Bridged(t, l, in)
	support.DeliveredAt(t, sub, 1, "from/in/x", in.Payload)
	out := &harness.Publish{QoS: 1, ID: 1, Topic: "out/x", Payload: []byte("to the peer")}
	support.Acknowledged(t, localClient(t, b, "transport-pub"), out)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "to/x", 1, out.Payload)
	return l
}

// handshakeFailed takes the next link that reaches peer, failing the test
// unless its first arrival is its end, no CONNECT sent before it, then
// waits for a WARN line naming bridge (design.md, Bridges, Logging: "Every
// bridge line names its bridge"; "A failed dial is one WARN line when its
// reason differs from the previous attempt's").
func handshakeFailed(t *testing.T, b *harness.Broker, peer *harness.Peer, bridge string) {
	t.Helper()
	a, err := peer.Link(t).Next(harness.DefaultTimeout)
	switch {
	case err != nil:
		t.Fatalf("waiting for the bridge's dial to end: %v", err)
	case a.Packet != nil:
		t.Fatalf("the peer received %s; want the dial to fail, no CONNECT sent", a)
	}
	if !testkit.Within(harness.DefaultTimeout, func() bool { return len(testkit.LinesNaming(b.Log(), "WARN", bridge)) > 0 }) {
		t.Errorf("no WARN line names the bridge %s after its dial failed; want the failed dial logged", bridge)
	}
}

// connectedPresenting takes l's CONNECT, failing the test unless it comes
// first and the bridge presented want in the TLS handshake before it.
func connectedPresenting(t *testing.T, l *harness.PeerLink, want *testkit.Issued) {
	t.Helper()
	harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
	if got := l.TLS.PeerCertificates; len(got) == 0 || !bytes.Equal(got[0].Raw, want.Certificate.Certificate[0]) {
		names := make([]string, len(got))
		for i, c := range got {
			names[i] = c.Subject.CommonName
		}
		t.Errorf("the bridge presented the certificates of %q; want its cert_file's, of %q", names, want.Certificate.Leaf.Subject.CommonName)
	}
}

// TestBridgeOverTLS: config.md, bridges.<name>: peer, "tls://"; tls,
// "ca_file, the authority for the peer's certificate". MQTT 5 4.2: "TLS
// [RFC5246]" is a suitable transport. design.md, Bridges: "On that
// connection Sagüin is an ordinary MQTT 5 client of the peer".
//
// A peer serves TLS with a certificate its authority issued for the
// loopback address, and a bridge to it at tls:// names that authority as
// its ca_file. The bridge's CONNECT arrives over the completed handshake,
// and the link carries the bridge's rules both ways.
func TestBridgeOverTLS(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	peer := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: "tls", TLS: certs.serving()})
	b := support.Launch(t, &harness.Options{Bridges: transportBridge("over-tls", peer, &harness.BridgeTLS{CAFile: certs.caFile})})
	carriedBothWays(t, b, peer)
}

// TestBridgeOverWebSocket: config.md, bridges.<name>: peer, "ws:// and
// wss:// may add a path (default /)". MQTT 5 6.0: "MQTT Control Packets
// MUST be sent in WebSocket binary data frames" [MQTT-6.0.0-1]; "The Client
// MUST include mqtt in the list of WebSocket Sub Protocols it offers"
// [MQTT-6.0.0-3].
//
// Once at ws:// with the path /mqtt/edge, once at wss:// with no path, its
// ca_file the authority of the peer's certificate: the upgrade request the
// peer reads targets the configured path, / where none, and offers the
// subprotocol mqtt, which the peer selects [MQTT-6.0.0-4]; the link then
// carries the bridge's rules both ways, each packet in binary frames (the
// peer's end ends the link on any other).
func TestBridgeOverWebSocket(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	for _, c := range []struct {
		name   string
		peer   harness.PeerOptions
		block  *harness.BridgeTLS
		target string // the upgrade's request target
	}{
		{"ws on a configured path", harness.PeerOptions{Scheme: "ws", Path: "/mqtt/edge"}, nil, "/mqtt/edge"},
		{"wss on the default path", harness.PeerOptions{Scheme: "wss", TLS: certs.serving()}, &harness.BridgeTLS{CAFile: certs.caFile}, "/"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			peer := harness.ListenedPeerWith(t, c.peer)
			b := support.Launch(t, &harness.Options{Bridges: transportBridge("over-"+c.peer.Scheme, peer, c.block)})
			l := carriedBothWays(t, b, peer)
			if got := l.Upgrade.RequestURI; got != c.target {
				t.Errorf("the bridge's upgrade targets %q; want %q", got, c.target)
			}
			if offered := harness.Subprotocols(l.Upgrade.Header); !slices.Contains(offered, "mqtt") {
				t.Errorf("the bridge's upgrade offers the subprotocols %q; want mqtt among them [MQTT-6.0.0-3]", offered)
			}
		})
	}
}

// TestBridgeRefusesAPeerItCannotVerify: config.md, bridges.<name>, tls:
// "ca_file, the authority for the peer's certificate"; design.md, Bridges,
// Logging: "A failed dial is one WARN line when its reason differs from the
// previous attempt's". MQTT 5 5.4.5: "a Client can use TLS to verify that
// it is connected to the right Server" through "the Server's TLS
// certificate".
//
// A bridge at tls:// names an authority as its ca_file. Once its peer
// presents a certificate of that authority for another host,
// elsewhere.example and not the loopback address dialled; once one for the
// loopback address that another authority issued. Each time the bridge's
// handshake fails: the peer's link ends with no CONNECT sent, and a WARN
// line names the bridge.
func TestBridgeRefusesAPeerItCannotVerify(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	other, err := testkit.NewAuthority("Sagüin other test authority")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, bridge string
		cert         *testkit.Issued
	}{
		{"a certificate for another host", "wrong-host",
			issue(t, certs.ca, &testkit.Leaf{CommonName: "elsewhere.example", DNSNames: []string{"elsewhere.example"}, Server: true})},
		{"a certificate of another authority", "wrong-authority",
			issue(t, other, &testkit.Leaf{CommonName: "saguin", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			peer := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: "tls", TLS: &tls.Config{Certificates: []tls.Certificate{c.cert.Certificate}}})
			b := support.Launch(t, &harness.Options{Bridges: transportBridge(c.bridge, peer, &harness.BridgeTLS{CAFile: certs.caFile})})
			handshakeFailed(t, b, peer, c.bridge)
		})
	}
}

// TestBridgePresentsItsCertificate: config.md, bridges.<name>, tls:
// "cert_file and key_file, Sagüin's certificate for the peer, written
// together". The item's agreed code brief (item 0019h, 261105): "present
// the configured client certificate whenever the peer asks for one".
//
// A bridge at tls:// has a certificate a client authority issued as its
// cert_file and key_file. Its peer requires a client certificate: once
// naming that authority as the one it accepts, and verifying against it;
// once naming only another authority, and taking any certificate. Each time
// the bridge's CONNECT arrives over a handshake in which the bridge
// presented its certificate.
func TestBridgePresentsItsCertificate(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	clients, err := testkit.NewAuthority("Sagüin bridge client authority")
	if err != nil {
		t.Fatal(err)
	}
	other, err := testkit.NewAuthority("Sagüin other client authority")
	if err != nil {
		t.Fatal(err)
	}
	edge := issue(t, clients, &testkit.Leaf{CommonName: "edge"})
	block := &harness.BridgeTLS{CAFile: certs.caFile,
		CertFile: testkit.WriteFile(t, certs.dir, "edge.crt", string(edge.CertPEM)),
		KeyFile:  testkit.WriteFile(t, certs.dir, "edge.key", string(edge.KeyPEM))}
	for _, c := range []struct {
		name     string
		auth     tls.ClientAuthType
		accepted *testkit.Authority // the authority the peer names
	}{
		{"the peer names its issuer", tls.RequireAndVerifyClientCert, clients},
		{"the peer names another authority", tls.RequireAnyClientCert, other},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			config := certs.serving()
			config.ClientAuth, config.ClientCAs = c.auth, c.accepted.Pool()
			peer := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: "tls", TLS: config})
			support.Launch(t, &harness.Options{Bridges: transportBridge("presenting", peer, block)})
			connectedPresenting(t, peer.Link(t), edge)
		})
	}
}

// TestBridgeMinVersionRefusesAnOlderPeer: config.md, bridges.<name>, tls:
// "min_version, 1.2 (default) or 1.3".
//
// A peer serves TLS 1.2 at most, with a certificate the bridge's ca_file
// verifies, and the bridge's min_version is 1.3: the handshake fails, so
// the peer's link ends with no CONNECT sent, and a WARN line names the
// bridge.
func TestBridgeMinVersionRefusesAnOlderPeer(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	config := certs.serving()
	config.MaxVersion = tls.VersionTLS12
	peer := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: "tls", TLS: config})
	b := support.Launch(t, &harness.Options{Bridges: transportBridge("tls13-only", peer,
		&harness.BridgeTLS{CAFile: certs.caFile, MinVersion: "1.3"})})
	handshakeFailed(t, b, peer, "tls13-only")
}

// TestBridgeTakesReloadedTLSFiles: design.md, Access and reload: SIGUSR1
// "reloads every TLS certificate, key, client CA and bridge ca_file for new
// connections"; "Each TLS handshake takes the current certificate and
// client CA". Bridges: "The first redial after failed establishment or link
// loss is immediate". Kanban 0019h: "reloaded material used on the next
// attempt".
//
// A bridge at tls:// has a first certificate as its cert_file and key_file,
// and its ca_file names the first authority, whose certificate its peer
// presents; the peer asks for a client certificate. The first link presents
// the first certificate. The files then hold a second certificate and a
// second authority, and the broker gets SIGUSR1, its tls step applied. The
// peer then presents a certificate of the second authority alone, and drops
// the link: the next link's CONNECT arrives, so the bridge verified the peer
// against the second authority, over a handshake in which it presented the
// second certificate.
func TestBridgeTakesReloadedTLSFiles(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	clients, err := testkit.NewAuthority("Sagüin bridge client authority")
	if err != nil {
		t.Fatal(err)
	}
	ca2, err := testkit.NewAuthority("Sagüin second test authority")
	if err != nil {
		t.Fatal(err)
	}
	server2 := issue(t, ca2, &testkit.Leaf{CommonName: "saguin-2", IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
	first := issue(t, clients, &testkit.Leaf{CommonName: "edge-first"})
	second := issue(t, clients, &testkit.Leaf{CommonName: "edge-second"})
	block := &harness.BridgeTLS{CAFile: certs.caFile,
		CertFile: testkit.WriteFile(t, certs.dir, "edge.crt", string(first.CertPEM)),
		KeyFile:  testkit.WriteFile(t, certs.dir, "edge.key", string(first.KeyPEM))}
	// presented is the certificate the peer presents in each handshake from
	// now on: the first authority's until the test swaps in the second's.
	var presented atomic.Pointer[tls.Certificate]
	presented.Store(&certs.server.Certificate)
	config := &tls.Config{ClientAuth: tls.RequireAnyClientCert,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return presented.Load(), nil }}
	peer := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: "tls", TLS: config})
	b := support.OpsLaunch(t, &harness.Options{Bridges: transportBridge("reloaded", peer, block)})
	l := peer.Link(t)
	connectedPresenting(t, l, first)

	testkit.WriteFile(t, certs.dir, "edge.crt", string(second.CertPEM))
	testkit.WriteFile(t, certs.dir, "edge.key", string(second.KeyPEM))
	testkit.WriteFile(t, certs.dir, "ca.pem", string(ca2.PEM))
	reload(t, b)
	presented.Store(&server2.Certificate)
	_ = l.Close() // the peer drops the link
	connectedPresenting(t, peer.Link(t), second)
}

// upgradeAnswerBound is the bound on a peer's WebSocket upgrade answer
// (design.md, Bridges: "bounded as a listener's request headers are, at 1
// MiB").
const upgradeAnswerBound = 1 << 20

// paddedHeader is header lines of about n bytes in all, each line
// "X-Pad-NNNN: " and up to 8,000 letters with its line ending.
func paddedHeader(n int) http.Header {
	h := http.Header{}
	for i := 0; n > 0; i++ {
		name := fmt.Sprintf("X-Pad-%04d", i)
		value := strings.Repeat("a", max(1, min(8000, n-len(name)-4)))
		h.Set(name, value)
		n -= len(name) + 4 + len(value)
	}
	return h
}

// TestBridgeBoundsTheUpgradeAnswer: design.md, Bridges: "A peer's
// WebSocket upgrade answer is bounded as a listener's request headers are,
// at 1 MiB. A larger one fails the dial."
//
// At ws://, then at wss://, a broker has two bridges. One's peer answers the
// upgrade with header lines of 16 KiB under 1 MiB in all: its CONNECT
// arrives. The other's answers with lines of 16 KiB over 1 MiB: its dial
// fails, the peer's link ending with no CONNECT sent, and a WARN line names
// that bridge. The 16 KiB leave room for the answer's own lines, which the
// design does not say whether the bound counts.
func TestBridgeBoundsTheUpgradeAnswer(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	for _, c := range []struct {
		scheme string
		server *tls.Config
		block  *harness.BridgeTLS
	}{
		{"ws", nil, nil},
		{"wss", certs.serving(), &harness.BridgeTLS{CAFile: certs.caFile}},
	} {
		t.Run(c.scheme, func(t *testing.T) {
			t.Parallel()
			const room = 16 << 10
			under := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: c.scheme, TLS: c.server, UpgradeHeader: paddedHeader(upgradeAnswerBound - room)})
			over := harness.ListenedPeerWith(t, harness.PeerOptions{Scheme: c.scheme, TLS: c.server, UpgradeHeader: paddedHeader(upgradeAnswerBound + room)})
			bridges := []harness.Bridge{support.BridgeTo("under-"+c.scheme, under, support.Out("u/#", "u/$#", 1)),
				support.BridgeTo("over-"+c.scheme, over, support.Out("o/#", "o/$#", 1))}
			for i := range bridges {
				bridges[i].TLS = c.block
			}
			b := support.Launch(t, &harness.Options{Bridges: bridges})
			harness.Arrive[*harness.Connect](t, under.Link(t), harness.DefaultTimeout)
			handshakeFailed(t, b, over, "over-"+c.scheme)
		})
	}
}

// TestBridgeStalledSetupEndsAtConnectTimeout: design.md, Bridges: "One
// connect_timeout deadline covers each establishment attempt, from starting
// the dial through TLS, WebSocket upgrade and CONNACK."
//
// With connect_timeout 2 seconds, a peer accepts the bridge's connection
// and answers nothing: at tls://, it never answers the TLS handshake; at
// ws://, never the WebSocket upgrade; at wss://, it completes the handshake
// and never answers the upgrade. Each time the bridge ends the connection
// connect_timeout after its dial began: no earlier than connect_timeout
// after the peer accepted it, less the little the dial took, and within a
// second more.
func TestBridgeStalledSetupEndsAtConnectTimeout(t *testing.T) {
	t.Parallel()
	const connectTimeout = 2 * time.Second
	certs := newPKI(t)
	verified := &harness.BridgeTLS{CAFile: certs.caFile}
	for _, c := range []struct {
		name  string
		peer  harness.PeerOptions
		block *harness.BridgeTLS
	}{
		{"tls, the handshake unanswered", harness.PeerOptions{Scheme: "tls", Silent: true}, verified},
		{"ws, the upgrade unanswered", harness.PeerOptions{Scheme: "ws", Silent: true}, nil},
		{"wss, the upgrade unanswered after the handshake", harness.PeerOptions{Scheme: "wss", TLS: certs.serving(), Silent: true}, verified},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			peer := harness.ListenedPeerWith(t, c.peer)
			support.Launch(t, &harness.Options{ConnectTimeout: connectTimeout, Bridges: transportBridge("stalled", peer, c.block)})
			l := peer.Link(t)
			end, _ := support.Ending(t, l, connectTimeout+harness.DefaultTimeout)
			support.Between(t, "the stalled attempt, from the accept to its end", end.At.Sub(l.Accepted),
				connectTimeout-250*time.Millisecond, connectTimeout+time.Second)
		})
	}
}
