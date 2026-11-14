package conformance

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the secured operations listener (item 0020e): its transports,
// TLS, unix sockets and the PROXY header, and who may call it. design.md,
// Operations, HTTP and its Who may call; Flows, Transport setup; config.md,
// broker.operations. Each launches a broker of its own with the plain
// loopback operations listener of Options.Operations, which the test
// scrapes, beside the listeners under test (harness.OpsListener); every
// request is made by harness.OpsTarget, its credentials by Go's net/http
// (RFC 7617), its certificates by testkit.Authority and its PROXY headers
// by harness.ProxyHeader, never by the broker's code.

// opsCase is one request to an operations listener and the statuses it may
// get.
type opsCase struct {
	target harness.OpsTarget
	call   *harness.OpsCall
	status []int
}

// notAnOperator is the status of a certificate or PROXY name that names no
// entry of the operators' file: design.md, Who may call, "If an operators'
// password file is configured, a certificate or PROXY identity must name an
// entry in that file, or it gets 403".
var notAnOperator = []int{http.StatusForbidden}

// expectOps sends each case's call and fails the test unless its answer's
// status is one the case allows; an error, as text/plain (design.md,
// Operations, HTTP: "Errors are plain text"); and a 401, with a
// WWW-Authenticate header (Who may call: "A rejected credential gets 401
// with WWW-Authenticate").
func expectOps(t *testing.T, cases []opsCase) {
	t.Helper()
	for i := range cases {
		c := &cases[i]
		a := opsCall(t, c.target, c.call)
		what := fmt.Sprintf("%s %s on %s, Host %q, Basic user %q", c.call.Method, c.call.Path, c.target.Address, c.call.Host, c.call.User)
		switch {
		case !slices.Contains(c.status, a.Status):
			t.Errorf("%s: %d %q; want %v", what, a.Status, a.Body, c.status)
		case a.Status >= http.StatusBadRequest && a.MediaType != "text/plain":
			t.Errorf("%s: %d as %q; want an error in plain text", what, a.Status, a.MediaType)
		case a.Status == http.StatusUnauthorized && a.Challenge == "":
			t.Errorf("%s: 401 with no WWW-Authenticate header", what)
		}
	}
}

// get is a GET of path with no Host of its own, by user with password where
// user is set.
func get(path, user, password string) *harness.OpsCall {
	return &harness.OpsCall{Method: http.MethodGet, Path: path, User: user, Password: password}
}

// operatorsFile writes the operators' password file of the tests below in
// a folder of the test's own and returns its path (design.md, Who may
// call: "A line of the operators' password file may end in a third field
// containing route prefixes. These are separated by commas and matched at
// a / boundary (prometheus:$7$…:/metrics). Without that field, the user
// reaches every route"): root, with no third field; prom, scoped to
// /metrics; auditor, to /v1/operations; and pair, to /health and
// /v1/operations/acl; each one's password its name followed by -pw, among
// a comment and a blank line.
func operatorsFile(t *testing.T) string {
	t.Helper()
	return passwordFile(t, t.TempDir(), "operators", "# Sagüin's operators",
		"root:"+sixHash("root-pw", salt12), "",
		"prom:"+sixHash("prom-pw", salt12)+":/metrics",
		"auditor:"+sixHash("auditor-pw", salt12)+":/v1/operations",
		"pair:"+sixHash("pair-pw", salt12)+":/health,/v1/operations/acl")
}

// nonLoopback is an IPv4 address of an interface of this machine that is up
// and not a loopback one, failing the test where there is none: a caller
// dialling an operations listener there has that address, not a loopback
// one.
func nonLoopback(t *testing.T) string {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range interfaces {
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && n.IP.IsGlobalUnicast() {
				return n.IP.String()
			}
		}
	}
	t.Fatal("this machine has no interface up with an IPv4 address other than a loopback one, which the test needs to call from")
	return ""
}

// connectionCounts is every count a connection could move, on b's /metrics
// now: saguin_connections_accepted_total, saguin_connections_refused_total,
// saguin_disconnects_total, and saguin_operations_requests_total but its
// /metrics 200, the scrapes' own (metrics.md).
func connectionCounts(t *testing.T, b *harness.Broker) map[string]float64 {
	t.Helper()
	got := seriesOf(t, b, "saguin_connections_accepted_total", "saguin_connections_refused_total",
		"saguin_disconnects_total", "saguin_operations_requests_total")
	delete(got, testkit.SeriesKey("saguin_operations_requests_total", "route", "/metrics", "code", "200"))
	return got
}

// opsTLS is the operations listener at addr over TLS, trusting certs'
// authority, presenting cert where it is not nil.
func opsTLS(addr string, certs *pki, cert *testkit.Issued) harness.OpsTarget {
	return harness.OpsTarget{Network: "tcp", Address: addr, TLS: certs.client(cert)}
}

// opsProxied is the unix operations listener at path, each connection
// sending header first.
func opsProxied(path string, header []byte) harness.OpsTarget {
	return harness.OpsTarget{Network: "unix", Address: path, Proxy: header}
}

// design.md, Flows, Transport setup: "A listener with tls speaks only TLS";
// "On an operations listener, [...] A plain HTTP request there gets
// net/http's own answer, 400 "Client sent an HTTP request to an HTTPS
// server", and is closed. It reaches no route."; config.md,
// broker.operations, listen: "tcp and unix listeners with name, address or
// path, mode, proxy_protocol, tls as above". Audit finding 261030-001a
// (0020e.md, 261030 144735): "curl http://…/health must fail and curl
// --cacert ca.pem https://…/health answer 200": nothing is served in the
// clear.
//
// An operations listener has tls with the test's server certificate. A
// client trusting the test's authority alone gets GET /health over HTTPS
// answered 200 {"status":"ok"}. A plaintext GET of /health, and one of
// /metrics, each sent on a connection of its own, are answered 400 with
// that sentence, then the connection ends; neither answer holds what the
// route serves (no "status", no saguin_ series), and no count of a
// connection or a request moves.
func TestOperationsTLSListenerSpeaksOnlyTLS(t *testing.T) {
	certs := newPKI(t)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{TLS: new(certs.listener(&harness.TLS{}))}}})
	addr := b.Ops[0]
	support.ExpectHealth(t, http.MethodGet, opsCall(t, opsTLS(addr, certs, nil), get("/health", "", "")), http.StatusOK, "ok")
	before := connectionCounts(t, b)
	for _, route := range []string{"/health", "/metrics"} {
		got := sentToClose(t, "tcp", addr, []byte("GET "+route+" HTTP/1.1\r\nHost: "+addr+"\r\n\r\n"))
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(got)), nil)
		if err != nil {
			t.Errorf("a plaintext GET of %s: %q, %v; want net/http's 400, then the close", route, got, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body) // what came before the close
		_ = resp.Body.Close()            // read whole, from memory
		if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("Client sent an HTTP request to an HTTPS server")) ||
			bytes.Contains(body, []byte(`"status"`)) || bytes.Contains(body, []byte("saguin_")) {
			t.Errorf("a plaintext GET of %s was answered %d %q; want 400 \"Client sent an HTTP request to an HTTPS server\", no route reached", route, resp.StatusCode, body)
		}
	}
	if after := connectionCounts(t, b); !maps.Equal(after, before) {
		t.Errorf("after plaintext requests the counts are %v; want them as they were, %v: they reach no route", after, before)
	}
}

// handshakeLimit is how long an operations listener's TLS handshake has:
// design.md, Flows, Transport setup: "On an operations listener, the
// handshake has five seconds from the connection being accepted".
const handshakeLimit = 5 * time.Second

// design.md, Flows, Transport setup: "On an operations listener, the
// handshake has five seconds from the connection being accepted, and a
// failed one is logged only"; Logging: "A failed TLS handshake (Transport
// setup) and a refused client certificate (Access and reload) are each one
// WARN line". Testing: "A deadline test controls or observes the event
// starting that deadline": the accept follows the dial, so a close for
// lateness comes no sooner than its deadline after the dial.
//
// An operations listener has tls. A TLS client trusting no authority
// fails the handshake on the broker's certificate: the connection ends, no
// count of a connection or a request moves, and once the broker has
// stopped its log holds one WARN line, whatever it says. On another such
// listener, a client sending half a ClientHello, then nothing, is closed no
// sooner than five seconds after its dial, and before seven.
func TestOperationsTLSHandshakeHasFiveSeconds(t *testing.T) {
	t.Parallel()
	certs := newPKI(t)
	listener := harness.Options{OpsListeners: []harness.OpsListener{{TLS: new(certs.listener(&harness.TLS{}))}}}
	t.Run("a failed handshake", func(t *testing.T) {
		t.Parallel()
		b := support.OpsLaunch(t, &listener)
		addr := b.Ops[0]
		before := connectionCounts(t, b)
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: harness.DefaultTimeout}, "tcp", addr, &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12})
		if err == nil {
			_ = conn.Close()
			t.Fatal("a client trusting no authority completed the handshake")
		}
		if after := connectionCounts(t, b); !maps.Equal(after, before) {
			t.Errorf("after a failed handshake the counts are %v; want them as they were, %v: it is logged only", after, before)
		}
		stopped(t, b)
		log := b.Log()
		if lines := testkit.LogLines(log, "WARN"); len(lines) != 1 {
			t.Errorf("WARN lines: %q; want one, the failed handshake's; the log:\n%s", lines, log)
		}
	})
	t.Run("half a ClientHello", func(t *testing.T) {
		t.Parallel()
		b := support.OpsLaunch(t, &listener)
		dialled := time.Now()
		sentToClose(t, "tcp", b.Ops[0], halfAClientHello)
		if waited := time.Since(dialled); waited < handshakeLimit || waited > handshakeLimit+2*time.Second {
			t.Errorf("half a ClientHello was closed %v after the dial; want the handshake's %v from the accept", waited, handshakeLimit)
		}
	})
}

// design.md, Flows, Transport setup: "A unix listener, MQTT or operations,
// first takes a live exclusive lock on <path>.lock"; config.md,
// broker.operations, listen: "tcp and unix listeners with name, address or
// path, mode, proxy_protocol"; Who may call: "A request with no credential
// reaches every route only on a loopback address or Unix socket, and only
// when its Host is an IP literal or localhost".
//
// The broker's operations listener beside the plain one is a unix socket.
// Over it, with no credential and Host localhost, GET /health is answered
// 200 {"status":"ok"} and GET /metrics 200 in Prometheus text, holding
// saguin_ready.
func TestUnixOperationsListenerServes(t *testing.T) {
	u := unixSocket(t, false)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{Unix: &u}}})
	local := harness.OpsTarget{Network: "unix", Address: b.Ops[0]}
	support.ExpectHealth(t, http.MethodGet, opsCall(t, local, get("/health", "", "")), http.StatusOK, "ok")
	if a := opsCall(t, local, get("/metrics", "", "")); a.Status != http.StatusOK || a.MediaType != "text/plain" || !strings.Contains(a.Body, "saguin_ready") {
		t.Errorf("GET /metrics over the unix socket: %d, %q, %d bytes; want 200 and the Prometheus text of every series", a.Status, a.MediaType, len(a.Body))
	}
}

// design.md, Flows, Transport setup: "A unix listener, MQTT or operations,
// with proxy_protocol reads a PROXY protocol v2 header from every
// connection. It reads the client's address and, where the proxy verified
// a client certificate, its Common Name as the user name"; "A v1 header, a
// missing header, one not sent in time, another address family, transport
// or command, or a broken length closes the connection and is logged. On
// an MQTT listener it is also counted as refused (proxy_header)"; Who may
// call: "Without that file, a verified certificate or PROXY identity has
// every route"; a request with no credential and a Host that is a name
// "gets 403".
//
// A unix operations listener has proxy_protocol, with no operators' file.
// GET /metrics, its Host example.com, is answered 200 behind a header whose
// SSL record verifies device-9, and 403 behind one with no SSL record. A v1
// header followed by a GET of /health is sent nothing, its connection
// closed and a line logged; no count of a connection or a request moves.
func TestOperationsProxyNameIsTheOperator(t *testing.T) {
	u := unixSocket(t, true)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{Unix: &u}}})
	call := &harness.OpsCall{Method: http.MethodGet, Path: "/metrics", Host: "example.com"}
	expectOps(t, []opsCase{
		{opsProxied(b.Ops[0], proxiedV4(harness.ProxyVerifiedCN("device-9"))), call, []int{http.StatusOK}},
		{opsProxied(b.Ops[0], proxiedV4()), call, []int{http.StatusForbidden}},
	})
	before := connectionCounts(t, b)
	logged := len(b.Log())
	if got := sentToClose(t, "unix", b.Ops[0], append(slices.Clone(v1Header), healthGET...)); len(got) != 0 {
		t.Errorf("after a v1 header and a GET of /health, the broker sent %q; want nothing, the connection closed", got)
	}
	loggedSince(t, b, logged, "a v1 header on the operations listener")
	if after := connectionCounts(t, b); !maps.Equal(after, before) {
		t.Errorf("after a v1 header the counts are %v; want them as they were, %v: it is logged only", after, before)
	}
}

// design.md, Operations, HTTP: "The broker decides the route first. An
// unknown route gets 404, whatever the credential. For a known route, the
// broker checks the credential, then the method. Any other method gets 405.
// Credential checks follow Who may call."; "Errors are plain text"; Who may
// call: "A rejected credential gets 401 with WWW-Authenticate. A good
// credential outside its scope gets 403"; "The anonymous-local rule applies
// only when no identity credential was supplied".
//
// A broker's one operations listener, on a loopback address, has the
// operators' file (operatorsFile). Every call is HTTP Basic, its Host the
// listener's address, but two with no credential and Host example.com. An
// unknown route gets 404 with no credential, a wrong password, a wrong
// password and method POST, and prom's credential, whose scope it is not
// in. /metrics gets 401 for a wrong password, an unknown user, and a wrong
// password with POST; the acl route gets 403 for prom, with GET and POST;
// /metrics gets 403 for POST with no credential and Host example.com, and
// 405 for POST by prom; /metrics by prom and the acl route by root are
// answered 200.
func TestOperationsDecidesRouteThenCredentialThenMethod(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{OpsPasswordFile: operatorsFile(t)})
	plain, acl := support.PlainOps(b), aclPath("anyone", "")
	cases := make([]opsCase, 0, 13)
	for _, r := range []struct {
		method, path, user, password, host string
		status                             int
	}{
		{http.MethodGet, "/nowhere", "", "", "example.com", http.StatusNotFound},
		{http.MethodGet, "/v1/operations/nowhere", "root", "not-root-pw", "", http.StatusNotFound},
		{http.MethodPost, "/nowhere", "root", "not-root-pw", "", http.StatusNotFound},
		{http.MethodGet, "/nowhere", "prom", "prom-pw", "", http.StatusNotFound},
		{http.MethodGet, "/metrics", "root", "not-root-pw", "", http.StatusUnauthorized},
		{http.MethodGet, "/metrics", "nobody", "root-pw", "", http.StatusUnauthorized},
		{http.MethodPost, "/metrics", "root", "not-root-pw", "", http.StatusUnauthorized},
		{http.MethodGet, acl, "prom", "prom-pw", "", http.StatusForbidden},
		{http.MethodPost, acl, "prom", "prom-pw", "", http.StatusForbidden},
		{http.MethodPost, "/metrics", "", "", "example.com", http.StatusForbidden},
		{http.MethodPost, "/metrics", "prom", "prom-pw", "", http.StatusMethodNotAllowed},
		{http.MethodGet, "/metrics", "prom", "prom-pw", "", http.StatusOK},
		{http.MethodGet, acl, "root", "root-pw", "", http.StatusOK},
	} {
		cases = append(cases, opsCase{plain, &harness.OpsCall{Method: r.method, Path: r.path, Host: r.host, User: r.user, Password: r.password}, []int{r.status}})
	}
	expectOps(t, cases)
}

// design.md, Operations, Who may call: "/health needs no credential on any
// operations listener. Every other route needs one, as follows"; "A
// request with no credential reaches every route only on a loopback
// address or Unix socket, and only when its Host is an IP literal or
// localhost. Otherwise it gets 403"; config.md, broker.operations, listen:
// "a non-loopback address [...] needs a password file or a required client
// certificate".
//
// A broker with the operators' file has an operations listener on a
// non-loopback address of this machine (nonLoopback). Called from that
// address with no credential, GET and HEAD of /health are answered 200, and
// GET /metrics 403. On the loopback listener, with no credential and Host
// example.com, GET /health is answered 200 and GET /metrics 403.
func TestHealthNeedsNoCredential(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{OpsPasswordFile: operatorsFile(t), OpsListeners: []harness.OpsListener{{Host: nonLoopback(t)}}})
	remote := harness.OpsTarget{Network: "tcp", Address: b.Ops[0]}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		support.ExpectHealth(t, method, opsCall(t, remote, &harness.OpsCall{Method: method, Path: "/health"}), http.StatusOK, "ok")
	}
	support.ExpectHealth(t, http.MethodGet, opsRequest(t, b, http.MethodGet, "/health", "example.com"), http.StatusOK, "ok")
	expectOps(t, []opsCase{
		{remote, get("/metrics", "", ""), []int{http.StatusForbidden}},
		{support.PlainOps(b), &harness.OpsCall{Method: http.MethodGet, Path: "/metrics", Host: "example.com"}, []int{http.StatusForbidden}},
	})
}

// design.md, Operations, Who may call: "Identity sources are tried by
// precedence: verified client certificate, verified PROXY certificate
// identity, then HTTP Basic. The selected source is used alone. Failure
// does not fall back"; "a certificate or PROXY identity must name an entry
// in that file. It receives that entry's scope"; "The anonymous-local rule
// applies only when no identity credential was supplied". A certificate
// and a PROXY header never come together: tls is for tcp listeners,
// proxy_protocol for unix ones (config.md, A listener).
//
// A broker with the operators' file has two operations listeners: TLS,
// asking for client certificates without requiring them, and a unix socket
// with proxy_protocol. Every call carries root's good Basic credential, its
// Host the listener's own, which alone would reach every route. On TLS, a
// certificate naming prom gets the acl route 403 (prom's scope), and one
// naming stranger, in no entry, gets /metrics refused; with no certificate,
// Basic decides: the acl route is answered 200. Behind a PROXY header, a
// verified prom gets the acl route 403 and a verified stranger /metrics
// refused; a header with no SSL record leaves Basic to decide: 200.
func TestOperatorIdentityPrecedenceHasNoFallback(t *testing.T) {
	certs := newPKI(t)
	u := unixSocket(t, true)
	b := support.OpsLaunch(t, &harness.Options{OpsPasswordFile: operatorsFile(t), OpsListeners: []harness.OpsListener{
		{TLS: new(certs.listener(&harness.TLS{ClientCAFile: certs.caFile, RequireCertificate: "false"}))}, {Unix: &u}}})
	prom := issue(t, certs.ca, &testkit.Leaf{CommonName: "prom"})
	stranger := issue(t, certs.ca, &testkit.Leaf{CommonName: "stranger"})
	acl := aclPath("anyone", "")
	expectOps(t, []opsCase{
		{opsTLS(b.Ops[0], certs, prom), get(acl, "root", "root-pw"), []int{http.StatusForbidden}},
		{opsTLS(b.Ops[0], certs, stranger), get("/metrics", "root", "root-pw"), notAnOperator},
		{opsTLS(b.Ops[0], certs, nil), get(acl, "root", "root-pw"), []int{http.StatusOK}},
		{opsProxied(b.Ops[1], proxiedV4(harness.ProxyVerifiedCN("prom"))), get(acl, "root", "root-pw"), []int{http.StatusForbidden}},
		{opsProxied(b.Ops[1], proxiedV4(harness.ProxyVerifiedCN("stranger"))), get("/metrics", "root", "root-pw"), notAnOperator},
		{opsProxied(b.Ops[1], proxiedV4()), get(acl, "root", "root-pw"), []int{http.StatusOK}},
	})
}

// design.md, Operations, Who may call: "If an operators' password file is
// configured, a certificate or PROXY identity must name an entry in that
// file. It receives that entry's scope without checking its password.
// Without that file, a verified certificate or PROXY identity has every
// route"; a request with no credential and a Host that is a name "gets
// 403".
//
// Two brokers each have two operations listeners: TLS requiring a client
// certificate, and a unix socket with proxy_protocol. Every call is a GET
// with no Basic credential. With the operators' file, a certificate or a
// verified PROXY name prom, with Host example.com, gets /metrics answered
// 200 and the acl route 403, prom's scope; stranger, in no entry, gets
// /metrics refused, its Host the listener's own. Without the file, a
// certificate or a verified PROXY name anyone, with Host example.com, gets
// both answered 200.
func TestCertificateOrProxyNameMustBeAnOperator(t *testing.T) {
	acl := aclPath("anyone", "")
	for _, c := range []struct {
		name     string
		file     bool
		operator string // a name the certificate or header carries, with Host example.com
		acl      int    // the acl route's status for it; /metrics is answered 200
	}{
		{"with the operators' file", true, "prom", http.StatusForbidden},
		{"without it", false, "anyone", http.StatusOK},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			certs := newPKI(t)
			u := unixSocket(t, true)
			o := &harness.Options{OpsListeners: []harness.OpsListener{{TLS: new(certs.requiring())}, {Unix: &u}}}
			if c.file {
				o.OpsPasswordFile = operatorsFile(t)
			}
			b := support.OpsLaunch(t, o)
			// by is how a request carries name: a certificate, then a header.
			by := func(name string) []harness.OpsTarget {
				return []harness.OpsTarget{
					opsTLS(b.Ops[0], certs, issue(t, certs.ca, &testkit.Leaf{CommonName: name})),
					opsProxied(b.Ops[1], proxiedV4(harness.ProxyVerifiedCN(name))),
				}
			}
			cases := make([]opsCase, 0, 6)
			for _, target := range by(c.operator) {
				cases = append(cases,
					opsCase{target, &harness.OpsCall{Method: http.MethodGet, Path: "/metrics", Host: "example.com"}, []int{http.StatusOK}},
					opsCase{target, &harness.OpsCall{Method: http.MethodGet, Path: acl, Host: "example.com"}, []int{c.acl}})
			}
			if c.file {
				for _, target := range by("stranger") {
					cases = append(cases, opsCase{target, get("/metrics", "", ""), notAnOperator})
				}
			}
			expectOps(t, cases)
		})
	}
}

// design.md, Operations, Who may call: "A line of the operators' password
// file may end in a third field containing route prefixes. These are
// separated by commas and matched at a / boundary
// (prometheus:$7$…:/metrics). Without that field, the user reaches every
// route"; "A good credential outside its scope gets 403".
//
// A broker's operations listener has the operators' file (operatorsFile).
// By HTTP Basic: prom, scoped to /metrics, gets /metrics answered 200 and
// the acl route 403; auditor, scoped to /v1/operations, the acl route 200
// and /metrics 403; pair, scoped to /health and /v1/operations/acl, the acl
// route 200 and /metrics 403; root, with no third field, both 200.
func TestScopesMatchAtASlashBoundary(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{OpsPasswordFile: operatorsFile(t)})
	acl := aclPath("anyone", "")
	cases := make([]opsCase, 0, 8)
	for _, r := range []struct {
		user          string
		metrics, rest int
	}{
		{"prom", http.StatusOK, http.StatusForbidden},
		{"auditor", http.StatusForbidden, http.StatusOK},
		{"pair", http.StatusForbidden, http.StatusOK},
		{"root", http.StatusOK, http.StatusOK},
	} {
		cases = append(cases,
			opsCase{support.PlainOps(b), get("/metrics", r.user, r.user+"-pw"), []int{r.metrics}},
			opsCase{support.PlainOps(b), get(acl, r.user, r.user+"-pw"), []int{r.rest}})
	}
	expectOps(t, cases)
}

// design.md, Operations, Who may call: "Certificate names follow the
// certificate-name rule (Access and reload). A name that rule refuses gets
// 403"; Access and reload: "When a verified certificate has neither name,
// or its name is not valid UTF-8 or holds a control character, the broker
// refuses the CONNECT with 0x86 and logs it. A Common Name carried by a
// PROXY header is also the whole proof, and follows the same rule"; Who may
// call: "Without that file, a verified certificate or PROXY identity has
// every route"; "The anonymous-local rule applies only when no identity
// credential was supplied". A name not valid UTF-8 comes only behind a
// PROXY header: Go's certificate parser refuses a UTF8String that is not
// UTF-8 (TestCertificateWithoutAUsableNameIsRefused).
//
// A broker with no operators' file, so that a name the rule takes reaches
// every route, has two operations listeners: TLS requiring a client
// certificate, and a unix socket with proxy_protocol. GET /metrics, its
// Host the listener's own, gets 403 with a certificate naming nothing, and
// with one whose Common Name holds a control character; and behind a header
// whose SSL record verifies a Common Name holding a control character, and
// one not valid UTF-8.
func TestOperatorNameTheRuleRefusesGets403(t *testing.T) {
	certs := newPKI(t)
	u := unixSocket(t, true)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{TLS: new(certs.requiring())}, {Unix: &u}}})
	cases := make([]opsCase, 0, 4)
	for _, leaf := range []testkit.Leaf{{}, {CommonName: "dev\x01ce"}} {
		cases = append(cases, opsCase{opsTLS(b.Ops[0], certs, issue(t, certs.ca, &leaf)), get("/metrics", "", ""), []int{http.StatusForbidden}})
	}
	for _, name := range []string{"dev\x01ce", "dev\xffce"} {
		cases = append(cases, opsCase{opsProxied(b.Ops[1], proxiedV4(harness.ProxyVerifiedCN(name))), get("/metrics", "", ""), []int{http.StatusForbidden}})
	}
	expectOps(t, cases)
}

// design.md, Operations, Who may call: "A scope naming no route stops
// startup. A third field in a client password file also stops startup,
// naming the file and the line"; "matched at a / boundary"; config.md,
// broker.operations, password_file: "a different file from the clients'";
// Operations, --check-config: "It also opens every file the configuration
// names and checks it as startup does: the password files [...]. It
// reports every problem it finds"; Restart: "The exit status is [...] 1
// after [...] a refused start".
//
// Each case writes a configuration with one operations listener and one
// fault. --check-config exits 1 and so does the start: a client password
// file whose fourth line has a third field, named with that line; an
// operators' file whose scope names /nowhere; one whose scope is /metric,
// which reaches /metrics at no / boundary; one whose scope is
// /v1/operations/ac; and an operators' file that is broker.mqtt's. With the
// operators' file of the tests above, --check-config exits 0.
func TestStartRefusesABadScopeOrFile(t *testing.T) {
	hash := sixHash("x-pw", salt12)
	for _, c := range []struct {
		name    string
		clients []string // the client file's lines, none for no file
		scope   string   // the operators' file's one scope
		same    bool     // the operators' file is the clients'
		line    int      // the client file's line the refusal names, 0 for none
		code    int      // --check-config's exit status
	}{
		{"a third field in a client file", []string{"# clients", "alice:" + hash, "", "bob:" + hash + ":/metrics"}, "", false, 4, 1},
		{"a scope naming no route", nil, "/nowhere", false, 0, 1},
		{"a scope at no / boundary", nil, "/metric", false, 0, 1},
		{"a scope cut inside a route", nil, "/v1/operations/ac", false, 0, 1},
		{"the clients' file", []string{"alice:" + hash}, "", true, 0, 1},
		{"good scopes", nil, "", false, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			o := &harness.Options{Dir: dir, WriteTimeout: support.WriteTimeout, Operations: true}
			if c.clients != nil {
				o.Login.PasswordFile = passwordFile(t, dir, "clients", c.clients...)
			}
			switch {
			case c.same:
				o.OpsPasswordFile = o.Login.PasswordFile
			case c.scope != "":
				o.OpsPasswordFile = passwordFile(t, dir, "operators", "root:"+hash, "scoped:"+hash+":"+c.scope)
			case c.clients == nil:
				o.OpsPasswordFile = operatorsFile(t)
			}
			config, err := harness.WriteConfig(o)
			if err != nil {
				t.Fatalf("writing the configuration: %v", err)
			}
			code, stdout, stderr := execute(t, support.BrokerBinary, "--check-config", config)
			if code != c.code {
				t.Errorf("--check-config: exit %d; want %d\n%s%s", code, c.code, stdout, stderr)
			}
			if c.line != 0 && !namesFileAndLines(stdout+stderr, o.Login.PasswordFile, []int{c.line}) {
				t.Errorf("--check-config names no %s and its line %d:\n%s%s", o.Login.PasswordFile, c.line, stdout, stderr)
			}
			if c.code == 0 {
				return
			}
			if code, _, stderr := execute(t, support.BrokerBinary, "--config", config); code != 1 {
				t.Errorf("the start: exit %d; want it refused, exit 1\n%s", code, stderr)
			}
		})
	}
}

// design.md, Operations, Who may call: "Behind a PROXY header, the client's
// address in the header decides instead: only a loopback client counts. A
// header carrying no client address (an unspecified family) does not count
// as a loopback client. Otherwise it gets 403."
// The Host and credential stay the same; only the header's address changes.
func TestOperationsProxyAddressControlsAnonymousAccess(t *testing.T) {
	u := unixSocket(t, true)
	b := support.OpsLaunch(t, &harness.Options{OpsListeners: []harness.OpsListener{{Unix: &u}}})
	call := &harness.OpsCall{Method: http.MethodGet, Path: "/metrics", Host: "localhost"}
	for _, tc := range []struct {
		name, address string
		status        int
	}{
		{"loopback", "127.0.0.1:1234", http.StatusOK},
		{"remote", "192.0.2.1:1234", http.StatusForbidden},
		{"unspecified", "", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			family, source := byte(harness.ProxyUnspec), netip.AddrPort{}
			if tc.address != "" {
				family, source = harness.ProxyTCP4, netip.MustParseAddrPort(tc.address)
			}
			header := harness.ProxyHeader(harness.ProxyProxy, family, source, proxyV4)
			expectOps(t, []opsCase{{opsProxied(b.Ops[0], header), call, []int{tc.status}}})
		})
	}
}
