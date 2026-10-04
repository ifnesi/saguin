package broker_test

// RFC 0005 "The health endpoint" - the endpoint on a real socket, which is
// the half httptest cannot show: that the address is bound before the call
// returns, and that stopping it frees the port.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	enchex "encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/proxyproto"
)

// opsFiles is an *Operators holding byDoor, for a test that wants one
// ready-made: the field itself is unexported so a package outside broker
// - this one - goes through Set, the same way main does.
func opsFiles(byDoor map[string]*passwd.File) *broker.Operators {
	o := &broker.Operators{}
	o.Set(byDoor)
	return o
}

func TestHealthEndpointServesOverTCP(t *testing.T) {
	h := start(t)

	// Port 0, then read back what the kernel gave us.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve health: %v", err)
	}

	// Bound before ServeOperations returned: no retry loop here on purpose.
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatalf("get /health: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health answered %d, want 200", resp.StatusCode)
	}
	if string(body) != "{\"status\":\"ok\"}\n" {
		t.Errorf("body %q", body)
	}

	// A second broker on the same address fails at the bind, which is what
	// makes a port clash a startup error naming it rather than a surprise.
	if _, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil); err == nil {
		t.Error("binding the same address twice succeeded")
	}

	stop()

	// And the port is free again.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if l, err := net.Listen("tcp", addr); err == nil {
			_ = l.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("the health listener did not release its port")
}

// The socket half of the operations listener, which is what a reader on
// this box uses: no port is opened at all.
//
// Driven over the socket rather than inspected, because "it binds" and "it
// answers /health and /metrics on that socket" are different claims and
// only the second one is the feature.
func TestTheOperationsListenerAnswersOverAUnixSocket(t *testing.T) {
	h := start(t)
	path := filepath.Join(shortSocketDir(t), "operations.sock")

	stop, err := h.B.ServeOperations(config.OperationsListen{
		Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: path, Mode: "0660"}}},
	}, time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations on a socket: %v", err)
	}
	defer stop()

	// The file's permissions are the access control, so they are part of
	// what was asked for rather than a detail of how it was opened.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the socket was not created: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o660 {
		t.Errorf("the socket is %v, want 0660: its permissions are the whole of the "+
			"access control on a listener with no authentication", got)
	}

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
	}}
	for _, path := range []string{"/health", "/metrics"} {
		resp, err := client.Get("http://saguin" + path)
		if err != nil {
			t.Fatalf("get %s over the socket: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s over the socket answered %d, want 200:\n%s", path, resp.StatusCode, body)
		}
		if len(body) == 0 {
			t.Errorf("%s over the socket answered nothing", path)
		}
	}
}

// haproxyV2Header is a PROXY v2 header HAProxy really sent, naming client
// 127.0.0.1:55020 and Common Name lighthouse-99 (captured in
// internal/proxyproto's tests, where its provenance is recorded).
const haproxyV2Header = "" +
	"0d0a0d0a000d0a515549540a2111002e7f0000017f000001d6ec22b320001f07" +
	"00000000210007544c5376312e3322000d6c69676874686f7573652d3939"

// serveProxiedOperations opens the operations listener on a socket declared
// to be behind a PROXY v2 proxy, and returns its path.
func serveProxiedOperations(t *testing.T) string {
	t.Helper()
	h := start(t)
	path := filepath.Join(shortSocketDir(t), "operations.sock")
	stop, err := h.B.ServeOperations(config.OperationsListen{
		Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: path, Mode: "0660", ProxyProtocol: true}}},
	}, time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations behind a proxy: %v", err)
	}
	t.Cleanup(stop)
	return path
}

// proxiedHealth sends one /health request behind a good header and returns
// what came back within wait, or the error that ended the read.
func proxiedHealth(t *testing.T, path string, wait time.Duration) (string, error) {
	t.Helper()
	return proxiedGet(t, path, haproxyV2Header, "/health", wait)
}

// proxiedGet sends one GET for route behind the given header.
func proxiedGet(t *testing.T, path, header, route string, wait time.Duration) (string, error) {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	req := append(decodeHexE2E(t, header),
		"GET "+route+" HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"...)
	if _, err := c.Write(req); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(wait))
	b, err := io.ReadAll(c)
	return string(b), err
}

func decodeHexE2E(t *testing.T, s string) []byte {
	t.Helper()
	b, err := enchex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return b
}

// isTimeout reports whether err is a read that ran out of time, which on a
// socket the broker should have closed means it did not.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// RFC 0005 "The operations listener", behind a proxy.
//
// **Peers that send nothing do not stop anyone else being answered.** The
// header used to be read inside Accept, one connection at a time, so one
// local peer that connected and said nothing stopped every request behind
// it. A deadline on that read alone still lets a peer that opens a socket
// every few seconds keep the door shut, which is why this holds fifty open.
//
// The request is first made with nobody silent, so a request this test got
// wrong cannot pass as a door that was held.
func TestSilentPeersDoNotStopTheProxiedOperationsSocket(t *testing.T) {
	path := serveProxiedOperations(t)

	resp, err := proxiedHealth(t, path, 2*time.Second)
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("with nobody silent, /health behind a good header answered %q (%v)", resp, err)
	}

	for i := 0; i < 50; i++ {
		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("silent peer %d: %v", i, err)
		}
		defer c.Close()
	}
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	resp, err = proxiedHealth(t, path, time.Second)
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("with 50 silent peers open, /health was not answered within 1s: got %q (%v) "+
			"after %v", resp, err, time.Since(start))
	}
}

// RFC 0005 "A name with no entry in the password file reaches `/metrics`
// and nothing else", and a request carrying no name at all on a socket
// reaches everything.
//
// **The Common Name a proxy sent reaches the handler.** The same header
// with its TLVs removed names nobody and is the control: it proves the
// route answers on this socket, so the refusal is the name's doing.
func TestTheNameAProxySentDecidesWhatTheOperationsSocketAnswers(t *testing.T) {
	path := serveProxiedOperations(t)
	// haproxyV2Header's addresses with no TLVs: length 0x000c, no name.
	const nameless = "0d0a0d0a000d0a515549540a2111000c7f0000017f000001d6ec22b3"
	const route = "/v1/operations/sessions"

	resp, err := proxiedGet(t, path, nameless, route, 2*time.Second)
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("%s behind a header naming nobody answered %q (%v), want 200", route, resp, err)
	}
	resp, err = proxiedGet(t, path, haproxyV2Header, route, 2*time.Second)
	if !strings.HasPrefix(resp, "HTTP/1.1 403") {
		t.Fatalf("%s behind a header naming lighthouse-99, who has no entry, answered %q (%v), "+
			"want 403: the name did not reach the handler", route, resp, err)
	}
	resp, err = proxiedGet(t, path, haproxyV2Header, "/metrics", 2*time.Second)
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("/metrics behind a header naming lighthouse-99 answered %q (%v), want 200", resp, err)
	}
}

// nginx 1.31.4 headers with `ssl_verify_client optional_no_ca`, both naming
// device-7: one for a certificate from the authority nginx trusts, one from
// an authority it has never seen, which nginx forwards with verify result 21.
// Captured in internal/proxyproto's tests, where their provenance is
// recorded.
const (
	nginxTrustedHeader = "" +
		"0d0a0d0a000d0a515549540a211100607f0000017f000001c8a68fa320004a07" +
		"00000000210007544c5376312e332200086465766963652d37230016544c535f" +
		"4145535f3235365f47434d5f53484133383424000a5253412d53484132353625" +
		"0007525341323034380300047e24cfdd"
	nginxUntrustedHeader = "" +
		"0d0a0d0a000d0a515549540a211100607f0000017f000001c8aa8fa320004a07" +
		"00000015210007544c5376312e332200086465766963652d37230016544c535f" +
		"4145535f3235365f47434d5f53484133383424000a5253412d53484132353625" +
		"000752534132303438030004f1afb700"
)

// RFC 0002 "Behind a proxy": a name is believed only from a certificate the
// proxy verified.
//
// **A certificate the proxy could not verify names nobody on the operations
// socket.** The trusted header naming device-7, who has no entry, is refused
// the route, which proves a name reaches the handler; the nameless header is
// served it. The untrusted header must be answered as the nameless one.
func TestAnUnverifiedCertificateNamesNobodyOnTheOperationsSocket(t *testing.T) {
	path := serveProxiedOperations(t)
	const nameless = "0d0a0d0a000d0a515549540a2111000c7f0000017f000001d6ec22b3"
	const route = "/v1/operations/sessions"

	for _, tc := range []struct{ name, header, want string }{
		{"no name", nameless, "HTTP/1.1 200"},
		{"a verified device-7", nginxTrustedHeader, "HTTP/1.1 403"},
		{"an unverified device-7", nginxUntrustedHeader, "HTTP/1.1 200"},
	} {
		resp, err := proxiedGet(t, path, tc.header, route, 2*time.Second)
		if !strings.HasPrefix(resp, tc.want) {
			t.Errorf("%s behind %s answered %q (%v), want %s", route, tc.name, resp, err, tc.want)
		}
	}
}

// **The same on the MQTT socket, where the name is the client's user name.**
// With a password file and no anonymous clients, a verified device-7 is
// admitted with no password, which is the control proving the header reached
// authentication; a client with no name is refused. An unverified device-7
// must be refused as the nameless one is - before, it was admitted as
// device-7, measured on the binary behind nginx.
func TestAnUnverifiedCertificateAuthenticatesNobodyOnTheMQTTSocket(t *testing.T) {
	h := start(t)
	dir := t.TempDir()
	users := passwd.New(filepath.Join(dir, "clients.passwd"))
	if err := users.Set("someone-else", "hunter2"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := users.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := passwd.Load(filepath.Join(dir, "clients.passwd"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	h.B.SetCredentials(loaded, false)

	path := filepath.Join(shortSocketDir(t), "saguin.sock")
	if err := h.Srv.AddListener(proxyproto.NewUnixSock("proxied", path, 0o600)); err != nil {
		t.Fatalf("add the proxied listener: %v", err)
	}
	h.Srv.Listeners.Serve("proxied", h.Srv.EstablishConnection)

	// A v5 CONNECT for client id "probe", no user name and no password.
	const connect = "101200044d5154540502000000000570726f6265"
	const nameless = "0d0a0d0a000d0a515549540a2111000c7f0000017f000001d6ec22b3"
	reason := func(t *testing.T, header string) byte {
		t.Helper()
		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		if _, err := c.Write(decodeHexE2E(t, header+connect)); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		b := make([]byte, 4)
		if _, err := io.ReadFull(c, b); err != nil || b[0] != 0x20 {
			t.Fatalf("no CONNACK: % x (%v)", b, err)
		}
		return b[3]
	}

	if got := reason(t, nginxTrustedHeader); got != 0x00 {
		t.Fatalf("a verified device-7 was answered 0x%02X, want 0x00", got)
	}
	refused := reason(t, nameless)
	if refused == 0x00 {
		t.Fatal("a client with no name and no password was admitted, so this test cannot " +
			"tell a refused name from an ignored one")
	}
	if got := reason(t, nginxUntrustedHeader); got != refused {
		t.Errorf("an unverified device-7 was answered 0x%02X, want 0x%02X as a client with no "+
			"name is: a certificate nobody checked authenticated a device", got, refused)
	}
}

// A peer that never sends its header is closed, rather than held for as
// long as it likes.
func TestASilentPeerOnTheProxiedOperationsSocketIsClosed(t *testing.T) {
	path := serveProxiedOperations(t)
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
	b, err := c.Read(make([]byte, 64))
	if isTimeout(err) {
		t.Fatal("a peer that sent no PROXY header was still open after 8s")
	}
	if b != 0 {
		t.Fatalf("the broker sent %d bytes to a peer that sent no header", b)
	}
}

// **Every way a header can be wrong gets no answer at all.** A request
// served without a good header would be served as whoever the socket's
// peer is, while the operator believes a proxy vouched for it.
func TestTheProxiedOperationsSocketAnswersNoBadHeader(t *testing.T) {
	path := serveProxiedOperations(t)
	get := "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	local := decodeHexE2E(t, haproxyV2Header)
	local[12] = 0x20 // v2, command LOCAL
	for _, tc := range []struct {
		name string
		send []byte
	}{
		{"no header", []byte(get)},
		{"a v1 header", []byte("PROXY TCP4 10.0.0.1 10.0.0.2 5000 443\r\n" + get)},
		{"a LOCAL command", append(local, get...)},
		{"garbage", append([]byte("0d0a0d0a000d0a51"), get...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := net.Dial("unix", path)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.Close()
			if _, err := c.Write(tc.send); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
			b, err := io.ReadAll(c)
			if len(b) > 0 {
				t.Fatalf("%s was answered: %q", tc.name, b)
			}
			if isTimeout(err) {
				t.Fatalf("%s was neither answered nor closed within 8s", tc.name)
			}
		})
	}
}

// A socket left behind by a broker that was killed is not in use, and bind
// refuses it anyway - "address already in use" about a file nothing is
// listening on. Replacing it is what makes an unattended restart after a
// crash or a power failure work.
func TestAStaleOperationsSocketIsReplacedRatherThanRefused(t *testing.T) {
	h := start(t)
	path := filepath.Join(shortSocketDir(t), "operations.sock")

	// A leftover socket where the socket goes, exactly as a killed broker
	// leaves one: bound, and never removed.
	left, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("leaving a stale socket: %v", err)
	}
	left.SetUnlinkOnClose(false)
	_ = left.Close()

	stop, err := h.B.ServeOperations(config.OperationsListen{
		Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: path, Mode: "0660"}}},
	}, time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("a stale socket file stopped the listener: %v - a broker that was "+
			"killed would not start again until somebody deleted it by hand", err)
	}
	stop()
}

// **A file at the operations socket path that is not a socket is refused and
// kept.** A configuration naming the wrong path must not cost the file there.
func TestARegularFileAtTheOperationsSocketPathIsKept(t *testing.T) {
	h := start(t)
	path := filepath.Join(shortSocketDir(t), "operations.sock")
	if err := os.WriteFile(path, []byte("precious"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	stop, err := h.B.ServeOperations(config.OperationsListen{
		Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: path, Mode: "0660"}}},
	}, time.Minute, nil, nil)
	if err == nil {
		stop()
		t.Fatal("the operations listener started over a regular file at its socket path")
	}
	got, rerr := os.ReadFile(path)
	if rerr != nil || string(got) != "precious" {
		t.Fatalf("the regular file at the socket path was not kept: %q, %v", got, rerr)
	}
}

// **Half a listener is worse than none.** A broker that starts having
// opened one of the two its configuration asked for leaves its operator
// waiting for a scrape that never arrives, so the first is closed again and
// the startup fails naming the bind that did.
func TestABindThatFailsClosesTheOneThatWorked(t *testing.T) {
	h := start(t)

	// TCP is bound first, so the socket is the half made to fail: a
	// directory that does not exist, which is what a typo in the path or an
	// unmounted /run looks like.
	sock := filepath.Join(shortSocketDir(t), "nowhere", "operations.sock")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	if _, err := h.B.ServeOperations(config.OperationsListen{
		TCP:  []config.TCPDoor{{Name: "tcp", Address: config.Address{Address: addr}}},
		Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: sock, Mode: "0660"}}},
	}, time.Minute, nil, nil); err == nil {
		t.Fatal("a listener with an unopenable socket started anyway")
	}

	// The TCP half must have been given back, or a retry - or anything else
	// wanting that port - meets a listener nothing is serving.
	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the port from the half that worked was not closed: %v", err)
	}
	_ = again.Close()
}

// RFC 0005 "Authentication and TLS": `/metrics` behind a credential,
// `/health` never.
//
// **The asymmetry is the rule, not an oversight to tidy up.** /metrics
// carries channel names, message volumes and consumer positions - the
// shape of somebody's fleet - while /health answers the one question a
// supervisor asks before restarting a process, and a probe cannot be made
// to hold a credential.
func TestMetricsIsBehindACredentialAndHealthIsNot(t *testing.T) {
	h := start(t)

	// A real password file, written the way an operator writes one.
	path := filepath.Join(t.TempDir(), "operations.passwd")
	pf := passwd.New(path)
	if err := pf.Set("prometheus", "scrape me"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := pf.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	operators, err := passwd.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute,
		opsFiles(map[string]*passwd.File{"tcp": operators}), nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	get := func(t *testing.T, path, user, password string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, "http://"+addr+path, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if user != "" {
			req.SetBasicAuth(user, password)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		return resp
	}

	t.Run("no credential is refused, and says how to give one", func(t *testing.T) {
		resp := get(t, "/metrics", "", "")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("/metrics answered %d without a credential, want 401", resp.StatusCode)
		}
		// Without this header a client has no way to know what to send, and
		// every scraper and browser looks for it.
		if got := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(got, "Basic ") {
			t.Errorf("WWW-Authenticate is %q, want a Basic challenge", got)
		}
		body, _ := io.ReadAll(resp.Body)
		if strings.Contains(string(body), "saguin_") {
			t.Errorf("the refusal carried the catalogue anyway:\n%s", body)
		}
	})

	t.Run("the wrong password is refused", func(t *testing.T) {
		resp := get(t, "/metrics", "prometheus", "wrong")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("/metrics answered %d to a wrong password, want 401", resp.StatusCode)
		}
	})

	t.Run("a user the file does not have is refused", func(t *testing.T) {
		resp := get(t, "/metrics", "nobody", "scrape me")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("/metrics answered %d to an unknown user, want 401", resp.StatusCode)
		}
	})

	t.Run("the right credential gets the catalogue", func(t *testing.T) {
		resp := get(t, "/metrics", "prometheus", "scrape me")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/metrics answered %d to the right credential, want 200", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "saguin_") {
			t.Errorf("200, but not the catalogue:\n%s", body)
		}
	})

	t.Run("health is never authenticated", func(t *testing.T) {
		resp := get(t, "/health", "", "")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("/health answered %d with no credential: a supervisor deciding "+
				"whether to restart this process cannot hold one", resp.StatusCode)
		}
	})
}

// **Two doors, two sets of operators**, which is what a per-listener
// `password_file` is for: a monitoring system on the port whose credential
// rotates with the estate's secrets, and an agent on the socket that
// belongs to this machine. One file for both makes those rotate together,
// which is the thing an operator splitting them is trying to stop.
//
// **The claim under test is that the door decides**, so every case is run
// against *both* doors rather than each against the one it belongs to. A
// test that only sent the port's credential to the port would pass against
// a broker that ignored the split entirely and admitted everybody
// everywhere - which is the shape this replaced.
func TestEachOperationsDoorTakesItsOwnOperators(t *testing.T) {
	h := start(t)

	file := func(user, password string) *passwd.File {
		t.Helper()
		path := filepath.Join(t.TempDir(), user+".passwd")
		pf := passwd.New(path)
		if err := pf.Set(user, password); err != nil {
			t.Fatalf("set %s: %v", user, err)
		}
		if err := pf.Save(); err != nil {
			t.Fatalf("save %s: %v", user, err)
		}
		f, err := passwd.Load(path)
		if err != nil {
			t.Fatalf("load %s: %v", user, err)
		}
		return f
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	sock := filepath.Join(shortSocketDir(t), "operations.sock")

	stop, err := h.B.ServeOperations(config.OperationsListen{
		TCP:  []config.TCPDoor{{Name: "tcp", Address: config.Address{Address: addr}}},
		Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: sock, Mode: "0660"}}},
	}, time.Minute, opsFiles(map[string]*passwd.File{
		"tcp":  file("prometheus", "scrape me"),
		"unix": file("agent", "let me in"),
	}), nil)
	if err != nil {
		t.Fatalf("serve operations on both doors: %v", err)
	}
	defer stop()

	overSocket := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	get := func(t *testing.T, door, route, user, password string) int {
		t.Helper()
		client, host := http.DefaultClient, addr
		if door == "socket" {
			client, host = overSocket, "saguin"
		}
		req, err := http.NewRequest(http.MethodGet, "http://"+host+route, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if user != "" {
			req.SetBasicAuth(user, password)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("get %s at the %s: %v", route, door, err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	for _, tc := range []struct {
		door, user, password string
		want                 int
		why                  string
	}{
		{"port", "prometheus", "scrape me", http.StatusOK,
			"the port's own operator was refused at the port"},
		{"socket", "agent", "let me in", http.StatusOK,
			"the socket's own operator was refused at the socket"},
		{"port", "agent", "let me in", http.StatusUnauthorized,
			"the socket's operator was admitted at the port, so the doors share a file"},
		{"socket", "prometheus", "scrape me", http.StatusUnauthorized,
			"the port's operator was admitted at the socket, so the doors share a file"},
		{"port", "", "", http.StatusUnauthorized,
			"the port served /metrics with no credential at all"},
		{"socket", "", "", http.StatusUnauthorized,
			"the socket served /metrics with no credential at all"},
	} {
		name := fmt.Sprintf("%s at the %s", tc.user, tc.door)
		if tc.user == "" {
			name = "nobody at the " + tc.door
		}
		t.Run(name, func(t *testing.T) {
			if got := get(t, tc.door, "/metrics", tc.user, tc.password); got != tc.want {
				t.Errorf("/metrics answered %d, want %d: %s", got, tc.want, tc.why)
			}
		})
	}

	// /health is in front of the credential on every door, and stays there
	// when the doors stop agreeing about who the operators are: a
	// supervisor asking whether to restart a process cannot hold one.
	t.Run("health is open on both doors", func(t *testing.T) {
		for _, door := range []string{"port", "socket"} {
			if got := get(t, door, "/health", "", ""); got != http.StatusOK {
				t.Errorf("/health at the %s answered %d, want 200", door, got)
			}
		}
	})
}

// certNaming is a client certificate with this Common Name and these DNS
// names, and a pool holding the authority that signed it - clientCert's, for
// a certificate named by a subject alternative name as well or instead.
func certNaming(t *testing.T, cn string, dns ...string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "Names CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true, IsCA: true}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTmpl, &caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	tmpl := x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: cn}, DNSNames: dns,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("client certificate CN %q DNS %q: %v", cn, dns, err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// selfSigned writes a certificate and key a test can serve, and returns the
// pool that verifies it.
//
// Generated rather than committed: a certificate in the repository expires,
// and the day it does every test that uses it fails for a reason that has
// nothing to do with the code.
func selfSigned(t *testing.T, host string) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:         true,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{host},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	if err := os.WriteFile(keyFile,
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	pool = x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("the certificate this test just wrote does not parse")
	}
	return certFile, keyFile, pool
}

// RFC 0005 "Authentication and TLS": the operations listener serves TLS
// where the port crosses a network the operator does not own.
//
// **Driven through a real handshake**, because "the config was accepted"
// and "a client can verify this broker" are different claims, and Basic
// carries the credential in clear so only the second one protects it.
func TestTheOperationsListenerServesTLS(t *testing.T) {
	h := start(t)
	certFile, keyFile, pool := selfSigned(t, "localhost")

	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil,
		map[string]*tls.Config{"tcp": {Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}
	resp, err := client.Get("https://" + addr + "/health")
	if err != nil {
		t.Fatalf("https get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health over TLS answered %d", resp.StatusCode)
	}
	if resp.TLS == nil {
		t.Fatal("the response carries no TLS state, so this was not an encrypted connection")
	}
	if resp.TLS.Version < tls.VersionTLS12 {
		t.Errorf("negotiated TLS 0x%04X, below the 1.2 floor", resp.TLS.Version)
	}

	// A client that does not know this certificate must not get in. Without
	// this the test would pass against a broker serving anything at all.
	strict := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}}
	if _, err := strict.Get("https://" + addr + "/health"); err == nil {
		t.Error("a client with no root for this certificate connected anyway")
	}

	// And plain HTTP against a TLS listener does not get the endpoint.
	// net/http recognises the case and answers 400 in plain text rather
	// than dropping the connection, which is a better answer than a reset -
	// what matters is that it is not the endpoint.
	plain, err := http.Get("http://" + addr + "/health")
	if err != nil {
		return // the connection was dropped, which is also fine
	}
	defer plain.Body.Close()
	if plain.StatusCode == http.StatusOK {
		t.Error("the listener served /health over an unencrypted connection")
	}
	body, _ := io.ReadAll(plain.Body)
	if strings.Contains(string(body), "status") {
		t.Errorf("an unencrypted request got the endpoint's answer:\n%s", body)
	}
}

// RFC 0005 "/v1/operations/config".
//
// **What the broker is running, not what is on disk.** The route answers
// from the document the binary resolved at startup, so a file edited under
// a running broker does not change it - which is the same rule the
// configuration itself keeps, and the only answer worth giving to somebody
// asking why a broker is behaving oddly.
//
// The three things asserted here are the three a caller depends on: the
// whole document carries the resolved values rather than the written ones,
// `?section=` narrows it and repeats compose, and a name the vocabulary
// does not hold is refused rather than answered with an empty object.
func TestTheConfigRouteAnswersWithWhatTheBrokerResolved(t *testing.T) {
	h := start(t)

	doc := map[string]any{
		"broker": map[string]any{
			"id": "under-test",
			"storage": map[string]any{
				"default": "local",
				"providers": map[string]any{
					"local": map[string]any{"type": "memory"},
				},
			},
		},
		"channels": map[string]any{
			"events": map[string]any{
				// The resolved filter, which is the point: an operator who
				// wrote no filter cannot see `events/#` in their own file.
				"type": "append", "filter": "events/#", "storage": "local",
			},
		},
	}
	h.B.SetConfigDocument(doc)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	get := func(t *testing.T, query string) (int, map[string]any, string) {
		t.Helper()
		resp, err := http.Get("http://" + addr + "/v1/operations/config" + query)
		if err != nil {
			t.Fatalf("get %s: %v", query, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var out map[string]any
		_ = json.Unmarshal(body, &out)
		return resp.StatusCode, out, string(body)
	}

	t.Run("the whole document, with the resolved values in it", func(t *testing.T) {
		code, out, body := get(t, "")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		channels, _ := out["channels"].(map[string]any)
		events, _ := channels["events"].(map[string]any)
		if events["filter"] != "events/#" {
			t.Errorf("the channel's resolved filter is %v, want events/# - the route is "+
				"answering with what was written rather than what the broker serves", events["filter"])
		}
	})

	t.Run("a section narrows it, and repeats compose", func(t *testing.T) {
		code, out, body := get(t, "?section=providers&section=channels")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		if len(out) != 2 || out["providers"] == nil || out["channels"] == nil {
			t.Fatalf("two sections asked for and %d came back: %s", len(out), body)
		}
		if _, unwanted := out["broker"]; unwanted {
			t.Errorf("a filtered response still carries the whole document: %s", body)
		}
		// `providers` is nested at broker.storage.providers, so a route
		// that only knew top-level keys would answer this with nothing.
		provs, _ := out["providers"].(map[string]any)
		if _, ok := provs["local"]; !ok {
			t.Errorf("the providers section is %v, want the provider the broker holds", out["providers"])
		}
	})

	t.Run("a section that is configured but empty is present, not missing", func(t *testing.T) {
		code, out, body := get(t, "?section=bridges")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		if _, ok := out["bridges"]; !ok {
			t.Errorf("a broker with no bridges answered without the key at all, so a caller "+
				"cannot tell that from a name it spelled wrongly: %s", body)
		}
	})

	t.Run("an unknown section is refused, and does not echo what was sent", func(t *testing.T) {
		const injected = "<script>alert(1)</script>"
		code, _, body := get(t, "?section="+url.QueryEscape(injected))
		if code != http.StatusBadRequest {
			t.Fatalf("answered %d, want 400: %s", code, body)
		}
		if strings.Contains(body, "script") {
			t.Errorf("the refusal reflected the caller's own string back at it: %q", body)
		}
		if !strings.Contains(body, "providers") {
			t.Errorf("the refusal does not name the sections that exist, so a caller with a "+
				"typo learns nothing: %q", body)
		}
	})
}

// RFC 0005 "/v1/operations/config", and the wiring under it.
//
// **A broker nobody gave a configuration answers 500, not `{}`.** The
// document is set by four lines in cmd/saguin and by nothing else, so the
// failure this guards is the one where those lines go and every other test
// still passes: an empty object is a valid JSON response and reads, to an
// operator, as a broker that configures nothing. It is the same shape as
// the sqlite provider the binary hands the broker, which was guarded by
// nothing until it was mutated away and `go test` said ok.
func TestAConfigRouteWithNoDocumentSaysSoRatherThanAnsweringEmpty(t *testing.T) {
	h := start(t) // deliberately not given a document

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	resp, err := http.Get("http://" + addr + "/v1/operations/config")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("an unwired broker answered %d with %q, want 500 - an empty document and a "+
			"broker that was never given one are the same bytes to a caller",
			resp.StatusCode, body)
	}
}

// RFC 0005 "/v1/operations/acl".
//
// **The question is "why can device-7 not publish", and the answer has to
// carry why.** A grant list alone cannot: an empty one means "this file has
// no opinion about that name" and "this name is matched and granted
// nothing", which are different problems with different fixes. So the
// patterns come back beside the grants, always, and this asserts both
// halves rather than the presence of a body.
func TestTheACLRouteExplainsAUserAndSaysWhichPatternDecided(t *testing.T) {
	h := start(t)

	dir := t.TempDir()
	aclPath := filepath.Join(dir, "acl.yaml")
	// Three patterns on purpose: one name matched by two of them, so
	// `pattern_applied` has something to decide, and one matched by none,
	// so the empty-grants case is reachable. An entry with no roles is
	// refused at load, which is why "matched and granted nothing" is not a
	// state this file can be in.
	if err := os.WriteFile(aclPath, []byte(`roles:
  sensor:
    - channel: events
      allow: [write]
  auditor:
    - channel: events
      allow: [read]
users:
  "device-*":
    roles: [sensor]
    client_ids: "device-*"
  "device-special":
    roles: [auditor]
  "gateway-*":
    roles: [sensor]
`), 0o600); err != nil {
		t.Fatalf("write the acl: %v", err)
	}
	// A registry of its own rather than the harness's, so this test says
	// which channel the rule is about instead of depending on what the
	// harness happens to configure.
	reg, err := channel.NewRegistry([]*channel.Channel{
		{Name: "events", Type: channel.Append},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	acl, err := authz.Load(aclPath, reg, 0)
	if err != nil {
		t.Fatalf("load the acl: %v", err)
	}
	h.B.SetACL(acl, aclPath)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	get := func(t *testing.T, query string) (int, authz.Explanation, string) {
		t.Helper()
		resp, err := http.Get("http://" + addr + "/v1/operations/acl" + query)
		if err != nil {
			t.Fatalf("get %s: %v", query, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var out authz.Explanation
		_ = json.Unmarshal(body, &out)
		return resp.StatusCode, out, string(body)
	}

	t.Run("a matched name carries its grants and the pattern in force", func(t *testing.T) {
		code, out, body := get(t, "?user=device-7")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		if out.PatternApplied == nil || *out.PatternApplied != "device-*" {
			t.Fatalf("pattern_applied is %v, want device-*: %s", out.PatternApplied, body)
		}
		if len(out.Grants) != 1 || out.Grants[0].Subject == nil ||
			!strings.HasPrefix(*out.Grants[0].Subject, "events") {
			t.Errorf("grants is %+v, want the events channel: %s", out.Grants, body)
		}
	})

	t.Run("the shadowed pattern is shown beside the one in force", func(t *testing.T) {
		// `device-special` matches two entries and only the exact one
		// applies. A file taking something away by shadowing cannot be
		// refused at startup - it is the intended behaviour - so this route
		// is the only place it is visible.
		code, out, body := get(t, "?user=device-special")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		if out.PatternApplied == nil || *out.PatternApplied != "device-special" {
			t.Fatalf("pattern_applied is %v, want device-special - the more exact of the two "+
				"that match is the one in force: %s", out.PatternApplied, body)
		}
		if len(out.PatternsMatched) != 2 {
			t.Errorf("patterns_matched is %v, want both device-* and device-special - the one "+
				"that matched and did nothing is what an operator is looking for",
				out.PatternsMatched)
		}
		if len(out.Grants) != 1 || out.Grants[0].Role == nil || *out.Grants[0].Role != "auditor" {
			t.Errorf("grants is %+v, want the auditor role the exact entry gives, not the "+
				"shadowed one: %s", out.Grants, body)
		}
	})

	t.Run("a name the file has no opinion on says so", func(t *testing.T) {
		code, out, body := get(t, "?user=printer-3")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		if len(out.Grants) != 0 {
			t.Errorf("grants is %+v, want none: %s", out.Grants, body)
		}
		if len(out.PatternsMatched) != 0 {
			t.Errorf("patterns_matched is %v, want none - the empty grant list only means "+
				"something beside this field", out.PatternsMatched)
		}
		if len(out.PatternsInFile) != 3 {
			t.Errorf("patterns_in_file is %v, want all three: it is what a caller with a typo "+
				"reads to find out", out.PatternsInFile)
		}
	})

	t.Run("a client id the entry refuses is said before the grants", func(t *testing.T) {
		// `client_ids` refuses at CONNECT with 0x86, before a rule is read.
		// Grants for a pair that cannot connect answer the wrong question,
		// and the reader is somebody whose device is not working.
		code, out, body := get(t, "?user=device-7&client_id=printer-9")
		if code != http.StatusOK {
			t.Fatalf("answered %d: %s", code, body)
		}
		if out.ClientIDAllowed == nil || *out.ClientIDAllowed {
			t.Errorf("client_id_allowed is %v for a pair the entry refuses: the body lists "+
				"what it may publish and says nothing about it being unable to connect: %s",
				out.ClientIDAllowed, body)
		}
		code, out, body = get(t, "?user=device-7&client_id=device-7-boot-3")
		if code != http.StatusOK || out.ClientIDAllowed == nil || !*out.ClientIDAllowed {
			t.Errorf("client_id_allowed is %v for a pair the entry admits: %s",
				out.ClientIDAllowed, body)
		}
		// Absent where the question was not asked: answering true for a
		// pair nobody named is a claim about a connection this call knows
		// nothing about.
		if _, out, _ := get(t, "?user=device-7"); out.ClientIDAllowed != nil {
			t.Errorf("client_id_allowed is %v with no client id given, want null",
				*out.ClientIDAllowed)
		}
	})

	t.Run("no user is refused rather than answered", func(t *testing.T) {
		code, _, body := get(t, "")
		if code != http.StatusBadRequest {
			t.Fatalf("answered %d, want 400: %s", code, body)
		}
	})

	t.Run("an over-long name is refused rather than echoed", func(t *testing.T) {
		code, _, body := get(t, "?user="+url.QueryEscape(strings.Repeat("x", 4096)))
		if code != http.StatusBadRequest {
			t.Fatalf("answered %d, want 400: %s", code, body)
		}
		if strings.Contains(body, strings.Repeat("x", 64)) {
			t.Errorf("the refusal reflected the caller's own string back at it")
		}
	})
}

// RFC 0005 "/v1/operations/acl", the no-file case.
//
// **An empty grant list would be the reverse of the truth.** A broker with
// no `acl_file` lets every authenticated client do anything, and answering
// that with `"grants": []` says it is allowed nothing - which is the exact
// defect the flag's own JSON form was changed to avoid, so the route must
// not reintroduce it at the other door.
func TestTheACLRouteSaysEverythingIsAllowedWhereNoFileIsConfigured(t *testing.T) {
	h := start(t)
	h.B.SetACL(nil, "")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	resp, err := http.Get("http://" + addr + "/v1/operations/acl?user=anyone")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("answered %d: %s", resp.StatusCode, body)
	}
	var out authz.Unrestricted
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, body)
	}
	if !out.EverythingAllowed {
		t.Errorf("a broker with no acl_file answered %s, which reads as a client granted "+
			"nothing about one that may do anything", body)
	}
}

// RFC 0005 "/v1/operations/users".
//
// **Three things are asserted and only one of them is the list**: that a
// door allowing anonymous connections says so, that the operators are not
// in the body, and that no hash is. The list itself is the easy part; each
// of the other three is a way for this route to be quietly wrong.
func TestTheUsersRouteAnswersWhoMayConnectAndNotWhoMayRead(t *testing.T) {
	h := start(t)

	dir := t.TempDir()
	clientsPath := filepath.Join(dir, "clients.passwd")
	clients := passwd.New(clientsPath)
	for _, u := range []string{"gateway-1", "device-7"} {
		if err := clients.Set(u, "a password"); err != nil {
			t.Fatalf("set %s: %v", u, err)
		}
	}
	if err := clients.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	loadedClients, err := passwd.Load(clientsPath)
	if err != nil {
		t.Fatalf("load the clients: %v", err)
	}

	// A second file on one door only, which is the case the `listeners`
	// block exists for.
	localPath := filepath.Join(dir, "local.passwd")
	local := passwd.New(localPath)
	if err := local.Set("local-agent", "a password"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := local.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	loadedLocal, err := passwd.Load(localPath)
	if err != nil {
		t.Fatalf("load the local file: %v", err)
	}

	// And an operators file, which must not appear anywhere in the answer.
	opsPath := filepath.Join(dir, "operations.passwd")
	ops := passwd.New(opsPath)
	if err := ops.Set("prometheus", "scrape me"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := ops.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	operators, err := passwd.Load(opsPath)
	if err != nil {
		t.Fatalf("load the operators: %v", err)
	}

	h.B.SetCredentials(loadedClients, false)
	h.B.SetListenerCredentials("unix", loadedLocal, true)
	// Both setters write one entry, and the binary calls them in an order
	// this test must not depend on - certificates first there, credentials
	// first here, so a whole-struct assignment in either is caught.
	h.B.SetListenerCertificates("unix", config.CertNone)
	h.B.SetListenerCertificates("tcp", config.CertRequired)
	h.B.SetListenerCredentials("tcp", loadedClients, false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute,
		opsFiles(map[string]*passwd.File{"tcp": operators}), nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/v1/operations/users", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.SetBasicAuth("prometheus", "scrape me")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("answered %d: %s", resp.StatusCode, raw)
	}
	body := string(raw)

	var out struct {
		Users            []string `json:"users"`
		AnonymousAllowed bool     `json:"anonymous_allowed"`
		Listeners        map[string]struct {
			Users            []string `json:"users"`
			AnonymousAllowed bool     `json:"anonymous_allowed"`
			Certificate      string   `json:"certificate"`
		} `json:"listeners"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}

	t.Run("the client names, in a stable order", func(t *testing.T) {
		if len(out.Users) != 2 || out.Users[0] != "device-7" || out.Users[1] != "gateway-1" {
			t.Errorf("users is %v, want both names sorted - two scrapes of one broker that "+
				"answer in different orders make every diff noise", out.Users)
		}
	})

	t.Run("a door allowing anonymous says so", func(t *testing.T) {
		if out.AnonymousAllowed {
			t.Errorf("the broker-wide answer says anonymous is allowed and it is not")
		}
		unix, ok := out.Listeners["unix"]
		if !ok {
			t.Fatalf("the listener with a file of its own is not in %v", out.Listeners)
		}
		if !unix.AnonymousAllowed {
			t.Errorf("the unix door admits anonymous clients and the body does not say so: " +
				"a list of names without that flag says a broker is closed when it is open")
		}
		if len(unix.Users) != 1 || unix.Users[0] != "local-agent" {
			t.Errorf("the unix door's users are %v, want local-agent", unix.Users)
		}
	})

	t.Run("a door requiring a certificate says so", func(t *testing.T) {
		// The case the field exists for: none of the names listed can
		// connect through this door - they have passwords and the
		// handshake wants a certificate - and whoever the authority signed
		// can, with no password and no entry anywhere. A body carrying
		// only the names reads "closed to all but these" about a door
		// standing open to a CA.
		tcp, ok := out.Listeners["tcp"]
		if !ok {
			t.Fatalf("every configured door is listed, and tcp is not in %v", out.Listeners)
		}
		if tcp.Certificate != config.CertRequired {
			t.Errorf("the tcp door reports certificate %q, want %q - and it reports it "+
				"whichever order the two setters were called in, which is what the "+
				"second SetListenerCredentials above is here to catch",
				tcp.Certificate, config.CertRequired)
		}
		if unix := out.Listeners["unix"]; unix.Certificate != config.CertNone {
			t.Errorf("the unix door reports certificate %q, want %q: a socket carries no "+
				"TLS and its file permissions are what decide who reaches it",
				unix.Certificate, config.CertNone)
		}
	})

	t.Run("the operators are not in it", func(t *testing.T) {
		if strings.Contains(body, "prometheus") {
			t.Errorf("the operator's name is in the body of the route it guards: %s", body)
		}
	})

	t.Run("no hash is in it, whatever the shape becomes", func(t *testing.T) {
		// Read from the files rather than named here, so this keeps holding
		// when the body grows a field.
		// **Every field of a stored line, not the line.** Comparing whole
		// lines was the first version and a mutation walked through it: a
		// leak of just the digest - the last `$`-delimited field - is not
		// the line, so the sweep found nothing and passed. The password
		// file's own delimiters are what the fields are, so this stays
		// right as the format grows rather than depending on a list.
		checked := 0
		for _, path := range []string{clientsPath, localPath, opsPath} {
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			for _, field := range strings.FieldsFunc(string(content), func(r rune) bool {
				return r == ':' || r == '$' || r == '\n'
			}) {
				if len(field) < 16 {
					continue // a scheme or an iteration count, not a secret
				}
				checked++
				if strings.Contains(body, field) {
					t.Errorf("part of a password out of %s is in the body: %s", path, body)
				}
			}
		}
		if checked < 3 {
			t.Fatalf("the sweep compared %d secret-length fields across three password "+
				"files, which is too few to be checking anything", checked)
		}
	})
}

// **Who is connected, and who is holding a session with nothing behind it**
// (RFC 0005 "/v1/operations/sessions").
//
// The two counts this replaces cannot tell those apart: three hundred
// devices with two hundred connected is either a rota or a hundred that have
// stopped calling, and `saguin_connections` beside `saguin_sessions_offline`
// says the same number for both. So the case that matters here is a session
// that outlived its connection, and it is made rather than assumed.
func TestTheSessionsRouteNamesWhoIsConnectedAndWhoIsMerelyHeld(t *testing.T) {
	h := start(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	stop, err := h.B.ServeOperations(tcpOnly(addr), time.Minute, nil, nil)
	if err != nil {
		t.Fatalf("serve operations: %v", err)
	}
	defer stop()

	type session struct {
		ClientID      string `json:"client_id"`
		Connected     bool   `json:"connected"`
		User          string `json:"user"`
		Protocol      int    `json:"protocol"`
		Listener      string `json:"listener"`
		Keepalive     int    `json:"keepalive"`
		ExpiresAfter  uint32 `json:"expires_after"`
		Subscriptions int    `json:"subscriptions"`
	}
	type answer struct {
		Sessions  []session `json:"sessions"`
		Total     int       `json:"total"`
		Connected int       `json:"connected"`
		Offline   int       `json:"offline"`
		Returned  int       `json:"returned"`
	}
	get := func(t *testing.T) answer {
		t.Helper()
		resp, err := http.Get("http://" + addr + "/v1/operations/sessions")
		if err != nil {
			t.Fatalf("get sessions: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("answered %d: %s", resp.StatusCode, body)
		}
		var out answer
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("the body is not the shape RFC 0005 prints: %v\n%s", err, body)
		}
		return out
	}

	// One that stays, one that goes and leaves its session behind.
	//
	// **Named so that alphabetical order and held-first order disagree.** The
	// first pair were "leaving" and "staying", which sort into the order this
	// test wants by accident - the assertion below passed with the sort
	// removed entirely, which is a test proving nothing about the thing it
	// names.
	staying := dialWithUser(t, h, "aaa-connected", "operator")
	staying.Sub(t, "events/#", 1)
	leaving := connect(t, h, "zzz-held", false, false)
	leaving.Sub(t, "events/#", 1)
	leaving.Close()

	deadline := time.Now().Add(5 * time.Second)
	var got answer
	for time.Now().Before(deadline) {
		got = get(t)
		if got.Offline == 1 && got.Connected == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got.Connected != 1 || got.Offline != 1 {
		t.Fatalf("the broker reports %d connected and %d offline, want 1 and 1: %+v",
			got.Connected, got.Offline, got.Sessions)
	}
	if got.Total != 2 || got.Returned != 2 {
		t.Errorf("total %d and returned %d, want 2 and 2 - a caller shown some of the "+
			"fleet and not told so believes it has seen all of it", got.Total, got.Returned)
	}

	// **The held session is first.** The row cap cuts the end off, and the
	// rows nobody was looking for are the ones behaving.
	if got.Sessions[0].ClientID != "zzz-held" || got.Sessions[0].Connected {
		t.Errorf("the first row is %+v, want the held session: an operator reading a "+
			"capped list needs the anomalies at the top", got.Sessions[0])
	}

	byID := map[string]session{}
	for _, s := range got.Sessions {
		byID[s.ClientID] = s
	}
	held, ok := byID["zzz-held"]
	if !ok {
		t.Fatal("the session that outlived its connection is not in the answer at all")
	}
	if held.Connected {
		t.Error("a session with no connection behind it is reported as connected")
	}
	// **How long it has**, which is the question the row exists to answer:
	// a session nothing is coming back for is held until this expires.
	if held.ExpiresAfter == 0 {
		t.Error("the held session reports no expiry, so a page cannot say when it goes")
	}
	if held.Subscriptions == 0 {
		t.Error("the held session reports no subscriptions, and it made one")
	}

	live, ok := byID["aaa-connected"]
	if !ok {
		t.Fatal("the connected client is not in the answer")
	}
	if !live.Connected {
		t.Error("a connected client is reported as offline")
	}
	if live.User != "operator" {
		t.Errorf("the connected client's user is %q, want operator - the name a rule is "+
			"written about is the one an operator needs to see", live.User)
	}
	if live.Protocol != 5 || live.Listener == "" {
		t.Errorf("protocol %d on listener %q: a row that cannot say which door a client "+
			"came in by cannot answer why a rule applied to it", live.Protocol, live.Listener)
	}

	// **The broker's own in-process publisher is not a session**, and it is
	// in the same table as everybody else. Counting it made the metrics
	// scrape report a session already offline on a broker with two clients.
	// **Proved present before it is asserted absent.** A broker whose
	// substrate had no inline client would pass the check below by having
	// nothing to exclude, which is the shape of an assertion that stops
	// testing anything the day the thing it guards moves.
	if _, ok := h.Srv.Clients.Get("inline"); !ok {
		t.Fatal("the substrate has no inline client, so excluding it asserts nothing")
	}
	if _, ok := byID["inline"]; ok {
		t.Error("the broker's own in-process publisher is listed as a session, so a page " +
			"shows the operator a session that is not one and offers to hang it up")
	}

	t.Run("only GET", func(t *testing.T) {
		resp, err := http.Post("http://"+addr+"/v1/operations/sessions", "application/json", nil)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("a POST was answered %d, want 405 - this route reads and the verb "+
				"that acts is an MQTT publish", resp.StatusCode)
		}
	})
}

// A name with a NUL or a control character is nobody's, at either door and
// from every source (ValidName): a client certificate's Common Name, a PROXY
// header's, a CONNECT user name and a client id at the MQTT door, answered
// 0x86 - or 0x85 for the client id - and a proxy's name at the operations
// door, answered 403. mosquitto and EMQX refuse the same set. U+0000 in a
// user name or a client id is the engine's to refuse first, as a malformed
// packet [MQTT-1.5.4-2]. Each source is driven beside `device-7`, which is
// admitted, so a refusal is the name's and not the door's.
func TestANameWithAControlCharacterIsNobodysAtEitherDoor(t *testing.T) {
	names := []string{"dev\x00ice", "dev\r\nice", "dev\x1bice", "dev\x7fice", "dev\u0085ice"}
	const good = "device-7"

	// connect writes a CONNECT, MQTT 5, clean start, with the user name if
	// one is given, and reads the CONNACK's reason code.
	connect := func(t *testing.T, c net.Conn, prefix []byte, clientID, username string) byte {
		t.Helper()
		var payload []byte
		payload = append(payload, byte(len(clientID)>>8), byte(len(clientID)))
		payload = append(payload, clientID...)
		flags := byte(0x02)
		if username != "" {
			flags |= 0x80
			payload = append(payload, byte(len(username)>>8), byte(len(username)))
			payload = append(payload, username...)
		}
		body := append([]byte{0, 4, 'M', 'Q', 'T', 'T', 5, flags, 0, 0, 0}, payload...)
		if _, err := c.Write(append(prefix, append([]byte{0x10, byte(len(body))}, body...)...)); err != nil {
			t.Fatalf("write CONNECT: %v", err)
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 64)
		n, err := io.ReadAtLeast(c, buf, 4)
		if err != nil || buf[0] != 0x20 {
			t.Fatalf("no CONNACK for client id %q, user name %q: % x (%v)", clientID, username, buf[:n], err)
		}
		return buf[3]
	}

	t.Run("a client certificate's Common Name", func(t *testing.T) {
		certFile, keyFile, serverPool := selfSigned(t, "localhost")
		serverPair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatalf("server pair: %v", err)
		}
		for _, cn := range append([]string{good}, names...) {
			client, clientCAs := clientCert(t, cn)
			reg, err := channel.NewRegistry([]*channel.Channel{{Name: "events", Type: channel.Append}})
			if err != nil {
				t.Fatalf("registry: %v", err)
			}
			srv, b, err := broker.NewServer(reg, config.Limits{}.Resolve(), slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			b.SetCredentials(passwd.New(filepath.Join(t.TempDir(), "p")), false)
			l := listeners.NewTCP(listeners.Config{ID: "mtls", Address: "127.0.0.1:0", TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{serverPair}, MinVersion: tls.VersionTLS12,
				ClientCAs: clientCAs, ClientAuth: tls.RequireAndVerifyClientCert}})
			if err := srv.AddListener(l); err != nil {
				t.Fatalf("add listener: %v", err)
			}
			go func() { _ = srv.Serve() }()
			conn, err := tls.Dial("tcp", l.Address(), &tls.Config{RootCAs: serverPool, Certificates: []tls.Certificate{client}})
			if err != nil {
				t.Fatalf("CN %q: dial: %v", cn, err)
			}
			want := byte(0x86)
			if cn == good {
				want = 0
			}
			if got := connect(t, conn, nil, "cert-client", ""); got != want {
				t.Errorf("CN %q was answered 0x%02X, want 0x%02X", cn, got, want)
			}
			_ = conn.Close()
			_ = srv.Close()
			b.Stop()
		}
	})

	h := start(t)
	dir := shortSocketDir(t)

	t.Run("a PROXY header's name", func(t *testing.T) {
		sock := filepath.Join(dir, "mqtt.sock")
		if err := h.Srv.AddListener(proxyproto.NewUnixSock("unixp", sock, 0o600)); err != nil {
			t.Fatal(err)
		}
		h.Srv.Listeners.Serve("unixp", h.Srv.EstablishConnection)
		for i, cn := range append([]string{good}, names...) {
			c, err := net.Dial("unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			want := byte(0x86)
			if cn == good {
				want = 0
			}
			if got := connect(t, c, decodeHexE2E(t, proxyV2Naming(cn)), fmt.Sprintf("proxied-%d", i), ""); got != want {
				t.Errorf("a proxy's name %q was answered 0x%02X, want 0x%02X", cn, got, want)
			}
			_ = c.Close()
		}
	})

	t.Run("a CONNECT's user name and client id", func(t *testing.T) {
		for i, name := range append([]string{good}, names...) {
			wantUser, wantID := byte(0x86), byte(0x85)
			switch {
			case name == good:
				wantUser, wantID = 0, 0
			case strings.ContainsRune(name, 0):
				wantUser, wantID = 0x81, 0x85 // the engine's: a malformed user name, an invalid client id
			}
			c, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			if got := connect(t, c, nil, fmt.Sprintf("named-%d", i), name); got != wantUser {
				t.Errorf("user name %q was answered 0x%02X, want 0x%02X", name, got, wantUser)
			}
			_ = c.Close()
			c, err = net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			if got := connect(t, c, nil, name, ""); got != wantID {
				t.Errorf("client id %q was answered 0x%02X, want 0x%02X", name, got, wantID)
			}
			_ = c.Close()
		}
	})

	t.Run("the operations door, behind a proxy", func(t *testing.T) {
		ops := passwd.New(filepath.Join(t.TempDir(), "operations.passwd"))
		for _, cn := range append([]string{good}, names...) {
			if err := ops.Set(cn, "not-used"); err != nil {
				t.Fatalf("passwd refused %q: %v", cn, err)
			}
			ops.SetScopes(cn, []string{"/v1/operations"})
		}
		sock := filepath.Join(dir, "operations.sock")
		stop, err := h.B.ServeOperations(config.OperationsListen{
			Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: sock, Mode: "0660", ProxyProtocol: true}}},
		}, time.Minute, opsFiles(map[string]*passwd.File{"tcp": ops, "unix": ops}), nil)
		if err != nil {
			t.Fatalf("serve operations behind a proxy: %v", err)
		}
		t.Cleanup(stop)
		for _, cn := range append([]string{good}, names...) {
			resp, err := proxiedGet(t, sock, proxyV2Naming(cn), "/v1/operations/sessions", 2*time.Second)
			status := strings.SplitN(resp, "\r\n", 2)[0]
			want := " 403 "
			if cn == good {
				want = " 200 "
			}
			if !strings.Contains(status, want) {
				t.Errorf("an operator named %q through a proxy was answered %q (%v), want%s", cn, status, err, want)
			}
		}
	})
}

// RFC 0005 "Who Sagüin thinks you are" and RFC 0002's certificate names:
// **one certificate is one name, at either door**, taken exactly as the
// certificate states it. The operations listener trimmed a Common Name and
// the MQTT listener did not, so a CN of "ops " was the operator "ops" at one
// door and the client "ops " at the other - measured on the binary, where
// that certificate reached a route scoped to "ops".
//
// Each certificate goes through a real handshake that verifies it, and the
// name each door takes from the verified connection is compared with the
// other and with the name RFC 0002 gives it: the Common Name, else the first
// DNS name. The MQTT listener took the Common Name alone, so a certificate
// carrying only a subject alternative name was the operator
// "ops.example.net" at one door and nobody at the other. Then the same over
// a proxy: a PROXY v2
// header naming "ops " must not reach what "ops" is scoped to, and the
// header naming "ops" must, which is the control that the entry is reachable
// at all.
func TestOneCertificateIsOneNameAtBothDoors(t *testing.T) {
	certFile, keyFile, serverPool := selfSigned(t, "localhost")
	serverPair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("server pair: %v", err)
	}
	for _, tc := range []struct {
		cn   string
		dns  []string
		want string
	}{
		{cn: "ops", want: "ops"},
		{cn: "ops ", want: "ops "},
		{cn: " ops", want: " ops"},
		{cn: "   ", want: "   "},
		{cn: "o p s", want: "o p s"},
		{dns: []string{"ops.example.net", "other.example.net"}, want: "ops.example.net"},
		{cn: "ops", dns: []string{"ops.example.net"}, want: "ops"},
	} {
		cn := fmt.Sprintf("CN %q DNS %q", tc.cn, tc.dns)
		client, clientCAs := certNaming(t, tc.cn, tc.dns...)
		a, b := net.Pipe()
		server := tls.Server(a, &tls.Config{Certificates: []tls.Certificate{serverPair},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs})
		peer := tls.Client(b, &tls.Config{Certificates: []tls.Certificate{client},
			RootCAs: serverPool, ServerName: "localhost"})
		done := make(chan error, 1)
		go func() { done <- peer.Handshake() }()
		if err := server.Handshake(); err != nil {
			t.Fatalf("%s: server handshake: %v", cn, err)
		}
		if err := <-done; err != nil {
			t.Fatalf("%s: client handshake: %v", cn, err)
		}
		mqttName, ok := broker.VerifiedName(server)
		opsName := broker.CertificateName(server.ConnectionState().PeerCertificates[0])
		if !ok || mqttName != tc.want || opsName != tc.want {
			t.Errorf("%s is %q (named %t) at the MQTT door and %q at the operations door: one "+
				"certificate, want one name, %q, exactly as it states it", cn, mqttName, ok, opsName, tc.want)
		}
		// The pipe's own ends: a TLS close writes an alert that nothing on
		// an unbuffered pipe is left to read.
		_ = a.Close()
		_ = b.Close()
	}

	t.Run("through a proxy", func(t *testing.T) {
		h := start(t)
		ops := passwd.New(filepath.Join(t.TempDir(), "operations.passwd"))
		if err := ops.Set("ops", "not-used"); err != nil {
			t.Fatalf("set: %v", err)
		}
		ops.SetScopes("ops", []string{"/v1/operations"})
		path := filepath.Join(shortSocketDir(t), "operations.sock")
		stop, err := h.B.ServeOperations(config.OperationsListen{
			Unix: []config.UnixDoor{{Name: "unix", UnixSocket: config.UnixSocket{Path: path, Mode: "0660", ProxyProtocol: true}}},
		}, time.Minute, opsFiles(map[string]*passwd.File{"tcp": ops, "unix": ops}), nil)
		if err != nil {
			t.Fatalf("serve operations behind a proxy: %v", err)
		}
		t.Cleanup(stop)
		const route = "/v1/operations/sessions"
		for _, tc := range []struct{ cn, want string }{
			{"ops", "HTTP/1.1 200"},
			{"ops ", "HTTP/1.1 403"},
		} {
			resp, err := proxiedGet(t, path, proxyV2Naming(tc.cn), route, 2*time.Second)
			if !strings.HasPrefix(resp, tc.want) {
				t.Errorf("%s behind a proxy naming %q answered %.40q (%v), want %s", route, tc.cn,
					resp, err, tc.want)
			}
		}
	})
}

// proxyV2Naming is a PROXY v2 header, in hex, for a TCP connection from
// 127.0.0.1 carrying one SSL TLV: a client certificate the proxy verified
// (client flags 0x07, verify 0), with cn as its Common Name. No CRC TLV.
func proxyV2Naming(cn string) string {
	name := fmt.Sprintf("22%04x%x", len(cn), cn)
	ssl := "07" + "00000000" + name
	tlv := fmt.Sprintf("20%04x%s", len(ssl)/2, ssl)
	body := "7f0000017f000001d6ec22b3" + tlv
	return "0d0a0d0a000d0a515549540a" + "2111" + fmt.Sprintf("%04x", len(body)/2) + body
}

// routeJSON reads one operations route as an operator does and decodes it.
func routeJSON(t *testing.T, ops, path string, v any) {
	t.Helper()
	resp, err := http.Get("http://" + ops + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s answered %d: %v %s", path, resp.StatusCode, err, body)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("GET %s: %v\n%s", path, err, body)
	}
}

// RFC 0005 `GET /v1/operations/queues/<channel>`. The route was read only as an instrument - awaitLeased waits
// for a row to say "leased" - and nothing held it to what the RFC promises:
// `state` is waiting, delivering or leased; `holder` and `lease_until` appear
// only on a leased row; no payload appears at all; and `returned` stops at a
// hundred while `unresolved` is the total. A queue of 101 jobs, one handed to
// a worker that has not acknowledged it and one to a worker that has.
func TestTheQueueRouteSaysWhatEachJobIsDoing(t *testing.T) {
	h := start(t)
	ops := operationsAt(t, h)

	var empty struct {
		Channel    string            `json:"channel"`
		Unresolved int               `json:"unresolved"`
		Returned   int               `json:"returned"`
		Records    []json.RawMessage `json:"records"`
	}
	routeJSON(t, ops, "/v1/operations/queues/jobs", &empty)
	if empty.Channel != "jobs" || empty.Unresolved != 0 || empty.Returned != 0 || empty.Records == nil ||
		len(empty.Records) != 0 {
		t.Errorf("an empty queue answered %+v, want jobs, 0, 0 and an empty list", empty)
	}

	const payload = "a-payload-the-route-must-never-carry"
	p := connect(t, h, "producer", true, false)
	for i := range 101 {
		p.Pub(t, fmt.Sprintf("jobs/w%d", i), payload)
	}
	// One worker that never answers its PUBACK: its job stays delivering.
	silent := connect(t, h, "worker-silent", true, true)
	silent.Sub(t, "$saguin/queue/jobs", 1)
	if _, ok := silent.Await(t, 3*time.Second); !ok {
		t.Fatal("the silent worker was offered no job, so nothing below is about a delivering one")
	}
	// And one that does: its job is leased.
	busy := connect(t, h, "worker-busy", true, false)
	busy.Sub(t, "$saguin/queue/jobs", 1)
	job, ok := busy.Await(t, 3*time.Second)
	if !ok {
		t.Fatal("the second worker was offered no job")
	}
	awaitLeased(t, ops, "jobs", job)

	resp, err := http.Get("http://" + ops + "/v1/operations/queues/jobs")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if strings.Contains(string(raw), payload) {
		t.Errorf("the queue route carries a job's payload: %s", raw)
	}
	var got struct {
		Unresolved int `json:"unresolved"`
		Returned   int `json:"returned"`
		Records    []struct {
			Offset     uint64 `json:"offset"`
			State      string `json:"state"`
			Holder     string `json:"holder"`
			LeaseUntil string `json:"lease_until"`
		} `json:"records"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	if got.Unresolved != 101 || got.Returned != 100 || len(got.Records) != 100 {
		t.Errorf("101 jobs answered unresolved %d, returned %d, %d rows; want 101, 100, 100",
			got.Unresolved, got.Returned, len(got.Records))
	}
	states := map[string]int{}
	for _, r := range got.Records {
		states[r.State]++
		switch r.State {
		case "leased":
			if r.Holder != "worker-busy" || r.LeaseUntil == "" {
				t.Errorf("the leased row names holder %q and lease_until %q, want worker-busy and a time",
					r.Holder, r.LeaseUntil)
			}
		case "delivering", "waiting":
			if r.Holder != "" || r.LeaseUntil != "" {
				t.Errorf("a %s row carries holder %q and lease_until %q: both are present only when "+
					"a job is leased", r.State, r.Holder, r.LeaseUntil)
			}
		default:
			t.Errorf("offset %d is in state %q, which is not one RFC 0005 names", r.Offset, r.State)
		}
	}
	if states["leased"] != 1 || states["delivering"] != 1 || states["waiting"] != 98 {
		t.Errorf("the rows' states are %v, want 1 leased, 1 delivering and 98 waiting", states)
	}
}

// RFC 0005 `GET /v1/operations/consumers`: **`behind` is how far a consumer's position is from the channel's
// next offset**, which no test had a consumer behind to read. A durable
// consumer reads one record and goes; three more are published; the route
// must say it is three behind, and that its position and the channel's next
// offset differ by exactly that.
func TestTheConsumersRouteSaysHowFarBehindAConsumerIs(t *testing.T) {
	h := start(t)
	ops := operationsAt(t, h)
	c := dial(t, h, "reader", false, false, 0, 3600, 0)
	c.Sub(t, "events/#", 1)
	p := connect(t, h, "producer", true, false)
	p.Pub(t, "events/x", "first")
	if _, ok := c.Await(t, 3*time.Second); !ok {
		t.Fatal("the consumer was sent nothing, so it has no position to be behind from")
	}
	type row struct {
		Reader string `json:"reader"`
		Offset uint64 `json:"offset"`
		Behind uint64 `json:"behind"`
	}
	var out struct {
		Channels []struct {
			Channel    string `json:"channel"`
			NextOffset uint64 `json:"next_offset"`
			Positions  []row  `json:"positions"`
		} `json:"channels"`
	}
	find := func() (row, uint64, bool) {
		routeJSON(t, ops, "/v1/operations/consumers", &out)
		for _, ch := range out.Channels {
			if ch.Channel != "events" {
				continue
			}
			for _, r := range ch.Positions {
				if r.Reader == "mqtt:reader" {
					return r, ch.NextOffset, true
				}
			}
		}
		return row{}, 0, false
	}
	// The position is stored on the acknowledgement's own clock
	// (ack_commit_interval); wait for it to say the first record was read.
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if r, next, ok := find(); ok && r.Behind == 0 && r.Offset == next {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the consumer's position never caught up with the one record it read: %+v", out)
		}
	}
	c.Close()
	for i := range 3 {
		p.Pub(t, "events/x", fmt.Sprintf("later-%d", i))
	}
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		r, next, ok := find()
		if ok && r.Behind == 3 && next-r.Offset == 3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the route says %+v with next_offset %d (%v), want the consumer 3 behind", r, next, ok)
		}
	}
}

// RFC 0005 `saguin_channel_partitioned_consumers`: how many subscribers declared a partition slice on a channel,
// and on no other. No test read the series by name.
func TestThePartitionGaugeCountsTheSubscribersThatDeclaredASlice(t *testing.T) {
	h := start(t)
	ops := operationsAt(t, h)
	const events, state = `saguin_channel_partitioned_consumers{channel="events"}`,
		`saguin_channel_partitioned_consumers{channel="state"}`
	if got := metricValue(t, ops, events); got != 0 {
		t.Fatalf("%s is %v before anybody subscribed", events, got)
	}
	whole := connect(t, h, "whole", true, false)
	whole.Sub(t, "events/#", 1)
	for i, id := range []string{"slice-0", "slice-1"} {
		c := connect(t, h, id, true, false)
		if sa := c.SubSliced(t, "events/#", 2, i); len(sa.Reasons) != 1 || sa.Reasons[0] > 2 {
			t.Fatalf("%s's slice was refused: %+v", id, sa)
		}
	}
	if got := metricValue(t, ops, events); got != 2 {
		t.Errorf("%s is %v with two slices declared and one whole subscriber, want 2", events, got)
	}
	if got := metricValue(t, ops, state); got != 0 {
		t.Errorf("%s is %v on a channel nobody sliced", state, got)
	}
}

// RFC 0005's route table: **"a method a path does not take is 405"**, for
// every /v1 route. Only the sessions
// route's was tested. The routes are the RFC's own table, not the mux's.
func TestEveryOperationsRouteRefusesAMethodItDoesNotTake(t *testing.T) {
	h := start(t)
	ops := operationsAt(t, h)
	routes := []string{
		"/v1/operations/acl?user=x", "/v1/operations/config", "/v1/operations/consumers",
		"/v1/operations/queues/jobs", "/v1/operations/position-lost", "/v1/operations/refused",
		"/v1/operations/sessions", "/v1/operations/users",
	}
	for _, path := range routes {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			req, _ := http.NewRequest(method, "http://"+ops+path, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", method, path, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("%s %s answered %d, want 405", method, path, resp.StatusCode)
			}
		}
		if code := func() int {
			resp, err := http.Get("http://" + ops + path)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}(); code == http.StatusMethodNotAllowed || code == http.StatusNotFound {
			// Reaching the handler is the control, not answering 200: this
			// harness wires no configuration or acl_file, so those two answer
			// 500 from inside the handler, which is past the method check.
			t.Errorf("GET %s answered %d: the control, a method the route takes, did not reach it",
				path, code)
		}
	}
}
