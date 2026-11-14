package conformance

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of MQTT over unix sockets and of the PROXY protocol v2 header
// (item 0013b). config.md, A listener: path (unix), "the socket file, at
// most 107 bytes, unique, held by a <path>.lock file"; mode (unix), "0660",
// "the socket's permissions, in octal"; proxy_protocol (unix), "reads a
// PROXY protocol v2 header". design.md, Flows, Transport setup, its unix
// listener and its PROXY header. Every header is written by
// harness.ProxyHeader from the specification's bytes; every socket path is
// in a short folder of the test's own (testkit.ShortDir).

// proxyRefused is saguin_connections_refused_total's proxy_header, "a
// refused PROXY header" (metrics.md, Connections and sessions).
var proxyRefused = refusedSeries("proxy_header")

// The addresses of the headers' client and of the proxy's own end, in the
// documentation ranges (RFC 5737, RFC 3849).
var (
	clientV4 = netip.MustParseAddrPort("192.0.2.10:51234")
	proxyV4  = netip.MustParseAddrPort("192.0.2.1:1883")
	clientV6 = netip.MustParseAddrPort("[2001:db8::10]:51235")
	proxyV6  = netip.MustParseAddrPort("[2001:db8::1]:1883")
)

// proxiedV4 is the header of a proxy relaying clientV4's connection, with
// records.
func proxiedV4(records ...[]byte) []byte {
	return harness.ProxyHeader(harness.ProxyProxy, harness.ProxyTCP4, clientV4, proxyV4, records...)
}

// unixSocket is a unix listener at a path in a short folder of the test's
// own, reading a PROXY header where proxy is set.
func unixSocket(t *testing.T, proxy bool) harness.Unix {
	t.Helper()
	return harness.Unix{Path: filepath.Join(testkit.ShortDir(t), "mqtt.sock"), ProxyProtocol: proxy}
}

// withProxy is o with one unix listener more, proxy_protocol on, which it
// returns too.
func withProxy(t *testing.T, o *harness.Options) (*harness.Options, harness.Unix) {
	t.Helper()
	l := unixSocket(t, true)
	own := *o
	own.Unix = append(append([]harness.Unix(nil), o.Unix...), l)
	return &own, l
}

// dialUnix dials the unix socket at path, its MQTT in version, failing the
// test where it cannot. The connection is closed when the test ends.
func dialUnix(t *testing.T, path string, version byte) *harness.Client {
	t.Helper()
	c, err := harness.DialUnix(path, version)
	if err != nil {
		t.Fatalf("dialling %s: %v", path, err)
	}
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	return c
}

// proxiedOver dials the unix socket at path as a proxy does and sends
// header first, returning the client, its MQTT in MQTT 5, ready for its
// CONNECT.
func proxiedOver(t *testing.T, path string, header []byte) *harness.Client {
	t.Helper()
	c := dialUnix(t, path, harness.V5)
	harness.SendBytes(t, c, header)
	return c
}

// sessionRow is the user and remote of client's durable session in db
// (storage.md, sessions: user, "the proved user name", and remote, "its
// last connection's remote address").
func sessionRow(t *testing.T, db *sql.DB, client string) (user, remote string) {
	t.Helper()
	if err := db.QueryRowContext(t.Context(), `SELECT "user", remote FROM sessions WHERE client = ?`, client).Scan(&user, &remote); err != nil {
		t.Fatalf("reading the session of %q: %v", client, err)
	}
	return user, remote
}

// refusedStartNaming fails the test unless the start of b is refused, exit
// status 1 (design.md, Restart: "The exit status is [...] 1 after [...] a
// refused start"), what it printed naming each of names.
func refusedStartNaming(t *testing.T, b *harness.Broker, names ...string) {
	t.Helper()
	err := b.Listening()
	switch exit, ok := errors.AsType[*exec.ExitError](err); {
	case err == nil:
		t.Fatal("the broker started; want its start refused")
	case !ok || exit.ExitCode() != 1:
		t.Fatalf("the start: %v; want it refused, exit status 1", err)
	}
	for _, name := range names {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the refusal does not name %s:\n%v", name, err)
		}
	}
}

// modeCalls are the system calls that set a file's mode by its path, as
// strace names them: chmod, and fchmodat and fchmodat2 from the working
// directory.
var modeCalls = []string{"chmod", "fchmodat", "fchmodat2"}

// modedBeforeListening fails the test unless calls, a broker's bind, mode
// and listen calls as testkit.Traced returns them, show the socket at path
// bound, then given its mode by its path, then listening: the socket
// accepts no connection before it has its mode.
func modedBeforeListening(t *testing.T, calls []string, path string) {
	t.Helper()
	bind, mode, listen, fd := -1, -1, -1, ""
	for i, line := range calls {
		call := strings.TrimLeft(line, "0123456789 ") // past the thread's id
		name, args, _ := strings.Cut(call, "(")
		switch {
		case bind < 0 && name == "bind" && strings.Contains(args, `sun_path="`+path+`"`):
			bind = i
			fd, _, _ = strings.Cut(args, ",")
		case bind >= 0 && mode < 0 && slices.Contains(modeCalls, name) && strings.Contains(args, `"`+path+`"`):
			mode = i
		case bind >= 0 && listen < 0 && name == "listen" && strings.HasPrefix(args, fd+","):
			listen = i
		}
	}
	if bind < 0 || mode < bind || listen < mode {
		t.Errorf("the socket at %s: bound at call %d, its mode set at %d, listening at %d of the trace; want bound, then its mode, then listening:\n%s",
			path, bind, mode, listen, strings.Join(calls, "\n"))
	}
}

// config.md, A listener: path (unix), "the socket file"; mode (unix),
// "0660", "the socket's permissions, in octal: at most 0777, the owner's
// read and write bits set"; design.md, Flows, Transport setup: "The socket
// gets its mode before it listens".
//
// A broker has two unix listeners, unix1 with mode left to its default and
// unix2 with mode 0640, and runs under strace (testkit.TracedCalls), its
// bind, listen and mode-setting calls noted. Each path holds a socket with
// that mode, 0660 and 0640, and each socket was bound, then given its mode,
// then made to listen. An MQTT 3.1.1 client on unix1 subscribes at QoS 1;
// an MQTT 5 client on unix2 publishes at QoS 1, PUBACK 0x00; the subscriber
// is sent the message.
func TestUnixListenerServesMQTT(t *testing.T) {
	dir := testkit.ShortDir(t)
	unix := []harness.Unix{{Path: filepath.Join(dir, "a.sock")}, {Path: filepath.Join(dir, "b.sock"), Mode: "0640"}}
	trace := filepath.Join(dir, "strace.out")
	support.Launch(t, &harness.Options{Unix: unix, Under: testkit.TracedCalls(t, trace, append([]string{"bind", "listen"}, modeCalls...)...)})
	calls := testkit.Traced(t, trace)
	for i, want := range []fs.FileMode{0o660, 0o640} {
		info, err := os.Lstat(unix[i].Path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Type() != fs.ModeSocket || info.Mode().Perm() != want {
			t.Errorf("%s is %v; want a socket with mode %v", unix[i].Path, info.Mode(), want)
		}
		modedBeforeListening(t, calls, unix[i].Path)
	}
	sub := dialUnix(t, unix[0].Path, harness.V311)
	harness.ConnectedOver(t, sub, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "unix/t", QoS: 1}}, 1)
	pub := dialUnix(t, unix[1].Path, harness.V5)
	harness.ConnectedOver(t, pub, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "unix/t", Payload: []byte("over a unix socket")})
	support.DeliveredAt(t, sub, 1, "unix/t", []byte("over a unix socket"))
}

// design.md, Flows, Transport setup: "A unix listener, MQTT or operations,
// first takes a live exclusive lock on <path>.lock, as the storage lock is
// held (Store). If another process holds it, startup is refused, naming the
// lock"; Restart: "The exit status is [...] 1 after [...] a refused start".
//
// A broker serves a client over its unix listener. A second broker, on a
// database and a TCP address of its own but the same unix path, exits with
// status 1 naming <path>.lock. The first broker still answers its client's
// PINGREQ, and a new client dialling the path is connected, CONNACK 0x00:
// the socket is still the first broker's.
func TestASecondBrokerOnAUnixPathIsRefusedByTheLock(t *testing.T) {
	l := unixSocket(t, false)
	support.Launch(t, &harness.Options{Unix: []harness.Unix{l}})
	held := dialUnix(t, l.Path, harness.V5)
	harness.ConnectedOver(t, held, &harness.Connect{ClientID: clientID(t, "-held"), CleanStart: true})

	refusedStartNaming(t, support.Started(t, &harness.Options{Unix: []harness.Unix{l}}), l.Path+".lock")

	harness.SendBytes(t, held, pingreq)
	harness.Expect[*harness.Pingresp](t, held)
	harness.ConnectedOver(t, dialUnix(t, l.Path, harness.V5), &harness.Connect{ClientID: clientID(t, "-new"), CleanStart: true})
}

// design.md, Flows, Transport setup: "A file left at path is removed only
// if it is a socket. Any other kind of file refuses the start."; Restart:
// "The exit status is [...] 1 after [...] a refused start".
//
// A socket nobody listens on is left at a unix listener's path: the broker
// starts, and a client dialling the path is connected, CONNACK 0x00. A
// regular file is at the path: the start is refused, exit status 1, and
// the file is still there, regular, with what it held.
func TestALeftoverSocketIsReplacedAndAnyOtherFileRefusesTheStart(t *testing.T) {
	t.Run("a leftover socket", func(t *testing.T) {
		l := unixSocket(t, false)
		left, err := net.ListenUnix("unix", &net.UnixAddr{Name: l.Path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		left.SetUnlinkOnClose(false) // the socket file stays, as a crash leaves it
		if err := left.Close(); err != nil {
			t.Fatal(err)
		}
		support.Launch(t, &harness.Options{Unix: []harness.Unix{l}})
		harness.ConnectedOver(t, dialUnix(t, l.Path, harness.V5), &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	})
	t.Run("a regular file", func(t *testing.T) {
		l := unixSocket(t, false)
		const held = "not a socket"
		testkit.WriteFile(t, filepath.Dir(l.Path), filepath.Base(l.Path), held)
		refusedStartNaming(t, support.Started(t, &harness.Options{Unix: []harness.Unix{l}}))
		info, err := os.Lstat(l.Path)
		if err != nil {
			t.Fatalf("the regular file at the path: %v; want it left", err)
		}
		if data, err := os.ReadFile(l.Path); !info.Mode().IsRegular() || err != nil || string(data) != held {
			t.Errorf("the file at the path is %v holding %q (%v); want it untouched, regular, holding %q", info.Mode(), data, err, held)
		}
	})
}

// design.md, Flows, Transport setup: the socket "is removed at stop. A
// crash releases the lock, so the next start replaces the socket the crash
// left behind".
//
// A broker with a unix listener stops gracefully: no file is left at the
// path. The broker starts again on the same folder and is killed
// (SIGKILL), leaving its socket at the path. A third start on the same
// folder serves: a client dialling the path is connected, CONNACK 0x00.
func TestTheSocketIsRemovedAtStopAndReplacedAfterAKill(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Unix: []harness.Unix{unixSocket(t, false)}}
	path := o.Unix[0].Path
	stopped(t, support.Launch(t, o))
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("after the graceful stop, %s: %v; want the socket removed", path, err)
	}
	support.Crash(t, support.Launch(t, o))
	if info, err := os.Lstat(path); err != nil || info.Mode().Type() != fs.ModeSocket {
		t.Fatalf("the kill left %v (%v) at %s; want its socket, for the next start to replace", info, err, path)
	}
	support.Launch(t, o)
	harness.ConnectedOver(t, dialUnix(t, path, harness.V5), &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
}

// design.md, Closing a connection: "The writer writes the last packet, ends
// its sending and stops the reader"; "Only then does the writer discard
// what the client still sends, until the client's end of stream or the
// deadline, and close"; "A client that stops in time sees an orderly
// close"; CONNECT, 5: "An old connection for the id gets DISCONNECT 0x8E on
// MQTT 5 and is closed". A unix socket refuses a write
// once its peer has closed (unix(7), EPIPE), but not while the peer has only
// ended its own sending.
//
// With write_timeout 5s, an MQTT 5 client on a unix listener is taken over
// by another. It reads DISCONNECT 0x8E, then the end of the stream, not an
// error; it then still sends a PINGREQ, which the broker, its sending
// ended and its socket still open within the close's deadline, takes and
// discards. The client then ends its own sending.
func TestUnixCloseIsOrderly(t *testing.T) {
	l := unixSocket(t, false)
	support.Launch(t, &harness.Options{WriteTimeout: 5 * time.Second, Unix: []harness.Unix{l}})
	id := clientID(t, "")
	c := dialUnix(t, l.Path, harness.V5)
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: id, CleanStart: true})
	harness.ConnectedOver(t, dialUnix(t, l.Path, harness.V5), &harness.Connect{ClientID: id, CleanStart: true})
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8E {
		t.Fatalf("the client taken over: DISCONNECT 0x%02X, want 0x8E", d.Reason)
	}
	harness.ExpectClosed(t, c)
	if err := c.SendRaw(pingreq); err != nil {
		t.Errorf("a PINGREQ after the broker's end of stream: %v; want it sent, the broker's socket still open, reading until the client's end", err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Errorf("ending the client's sending: %v", err)
	}
}

// design.md, Flows, Transport setup: "A unix listener, MQTT or operations,
// with proxy_protocol reads a PROXY protocol v2 header from every
// connection. It reads the client's address"; "An IPv4 or IPv6 address is
// the client's address". storage.md, sessions: remote, "its last
// connection's remote address", which an operator reads from the database
// as from Operations, sessions ("remote"); written as a TCP client's is,
// host and port.
//
// A broker has a unix listener with proxy_protocol. Two clients connect
// durable sessions through it, CONNACK 0x00, behind a header naming an
// IPv4 and an IPv6 client: each session's stored remote is that address.
func TestProxyHeaderAddressIsTheClients(t *testing.T) {
	o, l := withProxy(t, &harness.Options{})
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	for _, c := range []struct {
		name          string
		family        byte
		client, proxy netip.AddrPort
	}{
		{"IPv4", harness.ProxyTCP4, clientV4, proxyV4},
		{"IPv6", harness.ProxyTCP6, clientV6, proxyV6},
	} {
		t.Run(c.name, func(t *testing.T) {
			id := clientID(t, "")
			harness.ConnectedOver(t, proxiedOver(t, l.Path, harness.ProxyHeader(harness.ProxyProxy, c.family, c.client, c.proxy)),
				&harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable})
			if _, remote := sessionRow(t, db, id); remote != c.client.String() {
				t.Errorf("the session's remote is %q; want %q, the header's client", remote, c.client)
			}
		})
	}
}

// design.md, Flows, Transport setup: a unix listener with proxy_protocol
// reads, "where the proxy verified a client certificate, its Common Name as
// the user name. Verification means that the SSL record marks a
// certificate as presented and verified"; "An SSL record that is
// truncated, or that does not mark a certificate as presented and
// verified, gives no Common Name, so the client's password or a refusal
// decides". Access and reload: "A Common Name carried by a PROXY header is
// also the whole proof"; "Otherwise the user name is the password user";
// CONNECT's table: 0x86, "a wrong or missing credential". storage.md,
// sessions: user, "the proved user name".
//
// A broker has a password file holding alice, an acl_file granting
// device-7 write on devices/#, and a unix listener with proxy_protocol. A
// client whose header's SSL record marks a certificate presented and
// verified, naming device-7, connects naming alice with a wrong password:
// CONNACK 0x00, its session's user device-7, and its QoS 1 publish to
// devices/7 is acknowledged 0x00. Behind each SSL record that names
// device-7 but proves nothing (verify not 0; no certificate presented; its
// Common Name cut short), a client naming alice with her password is
// connected as alice, and one with no user name is refused 0x86.
func TestProxyVerifiedCommonNameIsTheWholeProof(t *testing.T) {
	o, l := withProxy(t, aclOptions(t, "roles:\n  device:\n    - topic: \"devices/#\"\n      allow: [write]\nusers:\n  device-7: [device]\n", "alice"))
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)

	id := clientID(t, "-verified")
	p := credentials(id, "alice", "not-alice-pw")
	p.Props = support.Durable
	c := proxiedOver(t, l.Path, proxiedV4(harness.ProxyVerifiedCN("device-7")))
	harness.ConnectedOver(t, c, p)
	if user, _ := sessionRow(t, db, id); user != "device-7" {
		t.Errorf("the session's user is %q; want device-7, the verified Common Name", user)
	}
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "devices/7", Payload: []byte("device-7's right")})

	cn := harness.ProxyRecord(harness.ProxySubtypeCN, []byte("device-7"))
	presented := byte(harness.ProxyClientSSL | harness.ProxyClientCertConn)
	for _, u := range []struct {
		name string
		ssl  []byte
	}{
		{"not verified", harness.ProxySSL(presented, 1, cn)},
		{"no certificate presented", harness.ProxySSL(harness.ProxyClientSSL, 0, cn)},
		{"its Common Name cut short", harness.ProxySSL(presented, 0, cn[:len(cn)-2])},
	} {
		t.Run(u.name, func(t *testing.T) {
			id := clientID(t, "")
			p := aclLogin(id, "alice")
			p.Props = support.Durable
			harness.ConnectedOver(t, proxiedOver(t, l.Path, proxiedV4(u.ssl)), p)
			if user, _ := sessionRow(t, db, id); user != "alice" {
				t.Errorf("the session's user is %q; want alice, the password user", user)
			}
			connackRefusesOver(t, proxiedOver(t, l.Path, proxiedV4(u.ssl)), &harness.Connect{ClientID: id + "-none", CleanStart: true}, 0x86)
		})
	}
}

// design.md, Flows, Transport setup: "On an MQTT listener, an unspecified
// address family serves the client with the socket's own address": the
// address the unix socket gives the client's end, the name its client bound
// it to. storage.md,
// sessions: remote, "its last connection's remote address".
//
// A broker has a unix listener with proxy_protocol. A client binds its end
// of the socket to a path of its own, sends a header of the PROXY command
// with the unspecified family, and connects a durable session, CONNACK
// 0x00: the session's stored remote is the path the client bound.
func TestUnspecifiedProxyFamilyIsServedWithTheSocketsAddress(t *testing.T) {
	o, l := withProxy(t, &harness.Options{})
	b := support.Launch(t, o)
	own := filepath.Join(filepath.Dir(l.Path), "client.sock")
	conn, err := net.DialUnix("unix", &net.UnixAddr{Name: own, Net: "unix"}, &net.UnixAddr{Name: l.Path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	c := harness.NewClient(conn, harness.V5)
	harness.SendBytes(t, c, harness.ProxyHeader(harness.ProxyProxy, harness.ProxyUnspec, netip.AddrPort{}, netip.AddrPort{}))
	id := clientID(t, "")
	harness.ConnectedOver(t, c, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable})
	if _, remote := sessionRow(t, support.OpenDB(t, b.DB), id); remote != own {
		t.Errorf("the session's remote is %q; want %q, the address the socket gives the client", remote, own)
	}
}

// closures are every series of saguin_connections_refused_total and
// saguin_disconnects_total on b's /metrics now, each named as
// testkit.SeriesKey names it: every count a connection's end can move
// (metrics.md, Connections and sessions; Rules: "Every series exists from
// start, for every value of its labels that applies").
func closures(t *testing.T, b *harness.Broker) map[string]float64 {
	t.Helper()
	return seriesOf(t, b, closureMetrics...)
}

// closureMetrics are the metrics whose series closures reads.
var closureMetrics = []string{"saguin_connections_refused_total", "saguin_disconnects_total"}

// seriesOf is every series of the metrics names on b's /metrics now, each
// named as testkit.SeriesKey names it.
func seriesOf(t *testing.T, b *harness.Broker, names ...string) map[string]float64 {
	t.Helper()
	return named(testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics())).Values, names...)
}

// named is every series of values, a scrape's, of the metrics names.
func named(values map[string]float64, names ...string) map[string]float64 {
	got := map[string]float64{}
	for key, v := range values {
		if slices.ContainsFunc(names, func(name string) bool { return strings.HasPrefix(key, name+"{") }) {
			got[key] = v
		}
	}
	return got
}

// design.md, Flows, Transport setup: "A LOCAL command closes the
// connection quietly, unserved: it is neither logged nor counted"; a
// failed TLS handshake "is logged naming the TLS error, and is counted as
// refused (tls_handshake)", which marks the log.
//
// A broker has a TLS listener and a unix listener with proxy_protocol.
// Once every probe's socket has closed, a client sends the unix listener a
// LOCAL header and a CONNECT: it is sent nothing, and the connection ends.
// Then a plaintext CONNECT to the TLS listener: tls_handshake rises by one,
// and every other count of a refusal or a disconnect is as it was; the
// first line logged since the LOCAL header names the TLS error.
func TestLocalProxyHeaderClosesQuietly(t *testing.T) {
	certs := newPKI(t)
	o, l := withProxy(t, &harness.Options{ListenerTLS: []harness.TLS{certs.listener(&harness.TLS{})}})
	b := support.OpsLaunch(t, o)
	want := baselineClosures(t, b)
	logged := len(b.Log())

	connect := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true}, harness.V5)
	local := harness.ProxyHeader(harness.ProxyLocal, harness.ProxyUnspec, netip.AddrPort{}, netip.AddrPort{})
	if got := sentToClose(t, "unix", l.Path, slices.Concat(local, connect)); len(got) != 0 {
		t.Errorf("after a LOCAL header and a CONNECT, the broker sent % X; want nothing: the connection unserved", got)
	}
	awaitSeries(t, b, map[string]float64{connectionsOpen: 0})

	sentToClose(t, "tcp", b.Addr, connect)
	want[tlsRefused]++
	var got map[string]float64
	if !testkit.Within(harness.DefaultTimeout, func() bool { got = closures(t, b); return maps.Equal(got, want) }) {
		t.Errorf("after %v, the counts of refusals and disconnects are %v; want %v: the LOCAL header counted nowhere, the plaintext CONNECT as tls_handshake", harness.DefaultTimeout, got, want)
	}
	marker := handshakeError(t, &certs.server.Certificate, connect)
	var since string
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		since = b.Log()[logged:]
		return strings.Contains(since, marker)
	}) {
		t.Fatalf("watchdog: no log line names %q, the plaintext CONNECT's TLS error, within %v; the log since the LOCAL header:\n%s", marker, harness.DefaultTimeout, since)
	}
	if first, _, _ := strings.Cut(since, "\n"); !strings.Contains(first, marker) {
		t.Errorf("lines were logged for a LOCAL header, before the plaintext CONNECT's; want none:\n%s", since)
	}
}

// A broken header's parts, each sent in a row of TestBadProxyHeaderIsRefused.
var (
	// v1Header is a version 1 header, text (proxy-protocol.txt, 2.1).
	v1Header = []byte("PROXY TCP4 192.0.2.10 192.0.2.1 51234 1883\r\n")
	// halfASignature is a header begun, its first 8 bytes, whose rest never
	// comes.
	halfASignature = []byte(harness.ProxySignature[:8])
)

// loggedSince waits until b has logged a line after the first logged
// bytes of its log, failing the test where none comes within
// harness.DefaultTimeout, the watchdog; what names the event that should
// have been logged.
func loggedSince(t *testing.T, b *harness.Broker, logged int, what string) {
	t.Helper()
	if !testkit.Within(harness.DefaultTimeout, func() bool { return strings.Contains(b.Log()[logged:], "\n") }) {
		t.Errorf("%s: no line logged within %v", what, harness.DefaultTimeout)
	}
}

// lengthed is header with its length field, the 15th and 16th bytes,
// big-endian, set to n.
func lengthed(header []byte, n int) []byte {
	header[14], header[15] = byte(n>>8), byte(n)
	return header
}

// design.md, Flows, Transport setup: "A v1 header, a missing header, one
// not sent in time, another address family or command, or a broken length
// closes the connection and is logged. On an MQTT listener it is also
// counted as refused (proxy_header)"; "On an MQTT listener, the header has
// the earlier of the CONNECT deadline and five seconds from service
// beginning (CONNECT)"; CONNECT, 1: "CONNECT's deadline starts when the
// connection receives its service permit". metrics.md,
// saguin_connections_refused_total: "proxy_header (a refused PROXY
// header)". design.md, Testing: "A deadline test controls or observes the
// event starting that deadline"; the permit follows the dial, so a close
// for lateness comes no sooner than its deadline after the dial, and the
// watchdog is the read's, harness.DefaultTimeout.
//
// A broker with connect_timeout 30s has a unix listener with
// proxy_protocol. Each row's bytes, a CONNECT following any header, are
// sent to it on a connection of its own: a v1 header; a CONNECT with no
// header; the UNIX family; command 2; an IPv4 header whose length is 8,
// short of its 12 bytes of addresses; an IPv4 header holding a record whose
// value runs 2 bytes past the header's length; and the first 8 bytes of a
// signature, then nothing. The broker sends nothing, closes each
// connection, the last no sooner than five seconds after the dial (within
// the watchdog, short of the 30s), and logs a line for each;
// proxy_header rises by one for each. With connect_timeout 1s, the first
// 8 bytes of a signature are closed no sooner than a second after the
// dial, counted as proxy_header, connect_timeout as it was.
func TestBadProxyHeaderIsRefused(t *testing.T) {
	connect := harness.Encoded(t, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: clientID(t, ""), CleanStart: true}, harness.V5)
	recordPast := proxiedV4(harness.ProxyRecord(0xE0, []byte("abcd")))
	for _, deadline := range []struct {
		name           string
		connectTimeout time.Duration // broker.limits.connect_timeout
		header         time.Duration // the earlier of it and five seconds
		rows           []struct {
			name string
			sent []byte
		}
	}{
		{"connect_timeout 30s", 30 * time.Second, 5 * time.Second, []struct {
			name string
			sent []byte
		}{
			{"a v1 header", v1Header},
			{"no header", nil},
			{"the UNIX family", harness.ProxyHeader(harness.ProxyProxy, harness.ProxyUnixStream, netip.AddrPort{}, netip.AddrPort{})},
			{"command 2", harness.ProxyHeader(0x22, harness.ProxyTCP4, clientV4, proxyV4)},
			{"a length short of the addresses", lengthed(proxiedV4(), 8)},
			{"a record past the length", lengthed(recordPast, len(recordPast)-16-2)},
		}},
		{"connect_timeout 1s", time.Second, time.Second, nil},
	} {
		t.Run(deadline.name, func(t *testing.T) {
			t.Parallel()
			o, l := withProxy(t, &harness.Options{ConnectTimeout: deadline.connectTimeout})
			b := support.OpsLaunch(t, o)
			before := baseline(t, b, proxyRefused, timedOut)
			refused := func(name string, sent []byte) {
				t.Helper()
				logged := len(b.Log())
				if got := sentToClose(t, "unix", l.Path, sent); len(got) != 0 {
					t.Errorf("%s: the broker sent % X; want nothing, the connection closed", name, got)
				}
				before[proxyRefused]++
				awaitSeries(t, b, before)
				loggedSince(t, b, logged, name)
			}
			for _, row := range deadline.rows {
				refused(row.name, slices.Concat(row.sent, connect))
			}
			dialled := time.Now()
			refused("half a signature", halfASignature)
			if waited := time.Since(dialled); waited < deadline.header {
				t.Errorf("half a signature closed %v after the dial; want no sooner than %v, the earlier of connect_timeout and five seconds", waited, deadline.header)
			}
		})
	}
}

// design.md, Flows, Transport setup: "On an MQTT listener, the header has
// the earlier of the CONNECT deadline and five seconds from service
// beginning (CONNECT)"; "A v1 header, a missing header, one not sent in
// time [...] closes the connection"; "An IPv4 or IPv6 address over TCP is
// the client's address"; storage.md, sessions: remote, "its last
// connection's remote address". The header's deadline is one, fixed when
// service begins: its bytes arriving do not move it. Its exact time,
// counted from the permit, is the connection package's scripted test
// (TestProxyHeaderDeadlineIsTheEarlierFromThePermit); the wire sees that
// the header is read whole however it is written, and that bytes trickling
// in do not keep a connection unserved past it. design.md, Testing: "A
// deadline test controls or observes the event starting that deadline";
// the permit follows the dial, so the close comes no sooner than five
// seconds after it.
//
// A broker with connect_timeout 30s has a unix listener with
// proxy_protocol. A client's IPv4 header is written in three pieces, the
// first cut inside the signature and the second inside the addresses, a
// tenth of a second apart; the client then connects a durable session,
// CONNACK 0x00, its stored remote the header's client. Another header,
// padded with an unknown record to 64 bytes, is trickled one byte every
// 200 milliseconds, so that it would end 12.8 seconds after the dial: the
// broker closes the connection before its last byte, no sooner than five
// seconds after the dial, sending nothing.
func TestProxyHeaderDeadlineIsFixedWhateverItsWrites(t *testing.T) {
	o, l := withProxy(t, &harness.Options{ConnectTimeout: 30 * time.Second})
	b := support.Launch(t, o)
	t.Run("split", func(t *testing.T) {
		t.Parallel()
		id := clientID(t, "")
		header := proxiedV4()
		c := dialUnix(t, l.Path, harness.V5)
		for i, piece := range [][]byte{header[:7], header[7:20], header[20:]} {
			if i > 0 {
				time.Sleep(100 * time.Millisecond) // the pieces in writes of their own
			}
			harness.SendBytes(t, c, piece)
		}
		harness.ConnectedOver(t, c, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable})
		if _, remote := sessionRow(t, support.OpenDB(t, b.DB), id); remote != clientV4.String() {
			t.Errorf("the session's remote is %q; want %q, the split header's client", remote, clientV4)
		}
	})
	t.Run("trickled", func(t *testing.T) {
		t.Parallel()
		const pace = 200 * time.Millisecond
		header := proxiedV4(harness.ProxyRecord(0xE0, make([]byte, 64-16-12-3)))
		conn, err := net.DialTimeout("unix", l.Path, harness.DefaultTimeout)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() }) // the test is over
		dialled := time.Now()
		for i := range header {
			if _, err := conn.Write(header[i : i+1]); err != nil {
				closedAt(t, dialled, i, err)
				return
			}
			// Each byte waits its turn on a read, which ends early if the
			// broker closes the connection.
			if err := conn.SetReadDeadline(time.Now().Add(pace)); err != nil {
				t.Fatal(err)
			}
			got, err := conn.Read(make([]byte, 64))
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if got != 0 {
				t.Fatalf("the broker sent %d bytes to a trickled header; want nothing, the connection closed", got)
			}
			closedAt(t, dialled, i+1, err)
			return
		}
		t.Errorf("all %d bytes of the header were trickled in, %v after the dial, the connection still open; want it closed at the header's deadline, five seconds after service began",
			len(header), time.Since(dialled))
	})
}

// closedAt fails the test unless err, met after sent bytes of a trickled
// header, is the broker's close, no sooner than five seconds after dialled.
func closedAt(t *testing.T, dialled time.Time, sent int, err error) {
	t.Helper()
	waited := time.Since(dialled)
	switch {
	case !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE):
		t.Fatalf("after %d bytes of the header: %v; want the broker's close", sent, err)
	case waited < 5*time.Second:
		t.Errorf("the connection closed %v after the dial, %d bytes of the header sent; want no sooner than five seconds", waited, sent)
	}
}

// design.md, Flows, Transport setup: "Records of unknown types are
// skipped"; a verified Common Name is the user name and "An IPv4 or IPv6
// address is the client's address". The PROXY specification
// (proxy-protocol.txt, 2.2.7) reserves types 0xE0 to 0xEF for custom use and
// 0xF8 to 0xFF for future use.
//
// A broker has a unix listener with proxy_protocol. A client's IPv4 header
// carries, around an SSL record verifying device-9, a record of type 0xE0
// whose value is itself an SSL record verifying intruder, and one of type
// 0xF8. It connects a durable session, CONNACK 0x00: the session's user is
// device-9 and its remote the header's client.
func TestUnknownProxyRecordsAreSkipped(t *testing.T) {
	o, l := withProxy(t, &harness.Options{})
	b := support.Launch(t, o)
	id := clientID(t, "")
	header := proxiedV4(harness.ProxyRecord(0xE0, harness.ProxyVerifiedCN("intruder")),
		harness.ProxyVerifiedCN("device-9"), harness.ProxyRecord(0xF8, []byte("future")))
	harness.ConnectedOver(t, proxiedOver(t, l.Path, header), &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable})
	if user, remote := sessionRow(t, support.OpenDB(t, b.DB), id); user != "device-9" || remote != clientV4.String() {
		t.Errorf("the session's user is %q and remote %q; want device-9 and %q", user, remote, clientV4)
	}
}
