package main

import (
	"bytes"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The piece tests run the broker inside the test process, through run, the
// whole command apart from os.Exit:
//
//	run([]string{"--config", <file>}, stdout, stderr) int
//
// runs the broker on the goroutine that calls it, R1's main, until SIGINT
// or SIGTERM or a failure stops it, writes its log lines on stderr, and
// returns the exit code once the stop has ended. Each test runs in a run of
// the test binary of its own, as the broker takes the process's signals and
// the test reaches into its socket.

// module prefixes the functions of Sagüin's code, which start Sagüin's
// goroutines.
const module = "github.com/ifnesi/saguin/"

// inProcess names the environment variable set in the run of the test
// binary that holds the broker.
const inProcess = "SAGUIN_CMD_IN_PROCESS"

// writeTimeout is broker.limits.write_timeout of the broker a test runs.
const writeTimeout = time.Second

// startLimit bounds a start: the store's creation and the listener's.
const startLimit = 10 * time.Second

// stopLimit bounds a graceful stop: design.md, Restart, Graceful stop, its
// sockets cut after write_timeout and its logger writing until
// write_timeout, with 5 seconds for the rest.
const stopLimit = 2*writeTimeout + 5*time.Second

// inProcessLimit bounds the whole run of the test binary that holds the
// broker: a start, the test and a stop.
const inProcessLimit = startLimit + harness.DefaultTimeout + stopLimit

// alone runs the calling test again, alone, in a run of the test binary of
// its own, unless this is that run, and reports whether it is.
func alone(t *testing.T) bool {
	t.Helper()
	if os.Getenv(inProcess) != "" {
		return true
	}
	testkit.Rerun(t, "the run holding the broker", inProcessLimit, []string{inProcess + "=1"})
	return false
}

// inProcessBroker is a broker run started in this process.
type inProcessBroker struct {
	main   string        // the id of the goroutine running run: R1's main
	exit   chan int      // run's exit code, once it returns
	log    *bytes.Buffer // its stderr; read only once run has returned
	socket int           // its MQTT TCP listener's socket
	addr   string        // and that socket's address
	ws     []string      // its ws listeners' addresses, in configuration order
}

// startInProcess runs the broker, on the configuration harness.WriteConfig
// writes for o, the one harness.Launch runs, its folder the test's and its
// write timeout writeTimeout, on a goroutine of the test's, and waits for
// its MQTT TCP listener and each of its unix and ws listeners.
func startInProcess(t *testing.T, o *harness.Options) *inProcessBroker {
	t.Helper()
	own := *o // the caller's Options stay as they were
	own.Dir, own.WriteTimeout = t.TempDir(), writeTimeout
	path, err := harness.WriteConfig(&own)
	if err != nil {
		t.Fatalf("writing the configuration: %v", err)
	}
	resolved, err := config.Load(path)
	if err != nil {
		t.Fatalf("reading the configuration: %v", err)
	}
	listen := resolved.Broker.MQTT.Listen
	b := &inProcessBroker{exit: make(chan int, 1), log: new(bytes.Buffer), addr: listen.TCP[0].Address}
	for _, l := range listen.WS {
		b.ws = append(b.ws, l.Address)
	}
	ids := make(chan string, 1)
	go func() {
		ids <- testkit.Goid()
		b.exit <- run([]string{"--config", path}, io.Discard, b.log)
	}()
	b.main = <-ids
	found := testkit.Within(startLimit, func() bool {
		select {
		case code := <-b.exit:
			t.Fatalf("run returned %d before listening:\n%s", code, b.log)
		default:
		}
		var ok bool
		b.socket, ok = listening(t, inet4(b.addr))
		for _, addr := range b.ws {
			_, on := listening(t, inet4(addr))
			ok = ok && on
		}
		for _, l := range own.Unix {
			_, on := listening(t, unixAt(l.Path))
			ok = ok && on
		}
		return ok
	})
	if !found {
		t.Fatalf("the broker was not listening within %v", startLimit)
	}
	return b
}

// inet4 accepts a listening socket's address where it is addr, an IPv4
// address written host:port.
func inet4(addr string) func(syscall.Sockaddr) bool {
	return func(sa syscall.Sockaddr) bool {
		in4, ok := sa.(*syscall.SockaddrInet4)
		return ok && net.JoinHostPort(net.IP(in4.Addr[:]).String(), strconv.Itoa(in4.Port)) == addr
	}
}

// unixAt accepts a listening socket's address where it is the unix socket
// path.
func unixAt(path string) func(syscall.Sockaddr) bool {
	return func(sa syscall.Sockaddr) bool {
		u, ok := sa.(*syscall.SockaddrUnix)
		return ok && u.Name == path
	}
}

// listening returns a socket this process listens on whose address at
// accepts: a descriptor of /proc/self/fd that is a socket in the listening
// state (SO_ACCEPTCONN).
func listening(t *testing.T, at func(syscall.Sockaddr) bool) (fd int, ok bool) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		fd, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		// A descriptor that is no socket, or closed since the listing, has
		// no SO_ACCEPTCONN to read.
		if on, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN); err != nil || on != 1 {
			continue
		}
		if sa, err := syscall.Getsockname(fd); err == nil && at(sa) {
			return fd, true
		}
	}
	return 0, false
}

// returned waits for run's exit code, failing the test where run does not
// return within stopLimit.
func (b *inProcessBroker) returned(t *testing.T, after string) int {
	t.Helper()
	return testkit.Recv(t, b.exit, stopLimit, "run's return "+after)
}

// design.md, R1: "They also fail if the live count of Sagüin's goroutines
// differs from the table". With read_connections at its default, 2
// (config.md), three MQTT listeners, one TCP, one unix with
// proxy_protocol and one ws, no operations listener and no bridge, the
// table gives, besides main: core 1, store writer 1, store reader 2, logger
// 1, listener 1 per listener, 3, and a reader and a writer per connection
// ("Lives for: the connection"), a proxied one and a WebSocket one alike.
// R1: "A WebSocket connection accepted by a listener has as its reader the
// goroutine net/http starts for it. That reader starts the connection's writer once the upgrade
// succeeds", so a WebSocket connection's reader is the one goroutine
// net/http starts for a connection (testkit.ServedConnection). With two
// clients connected over TCP, one through a PROXY header over the unix
// socket and one over a WebSocket, that is 16, and after each way a
// connection ends (a client's DISCONNECT, a takeover, the keep-alive, a
// refusal, a malformed packet, a refused PROXY header), the ended
// connection's two are gone, the proxied and the WebSocket clients still
// connected; once the WebSocket client disconnects, its two are gone. A
// connection's goroutines end once the client, having read the broker's end
// of stream, closes its own (design.md, Flows, Closing a connection: the
// writer discards what the client sends "until the client's end of stream or
// the deadline"). Transport setup: "A v1 header [...] closes the connection".
func TestRunningGoroutinesMatchR1(t *testing.T) {
	if !alone(t) {
		return
	}
	proxied := harness.Unix{Path: filepath.Join(testkit.ShortDir(t), "mqtt.sock"), ProxyProtocol: true}
	b := startInProcess(t, &harness.Options{Unix: []harness.Unix{proxied}, WS: []harness.WS{{}}})
	const base = 1 + 1 + 2 + 1 + 3
	running := func() (int, map[string]int) {
		byCreator := testkit.Started(b.main, module)
		total := 0
		for _, n := range byCreator {
			total += n
		}
		return total, byCreator
	}
	// expect waits for the count of connections, webSockets of them over
	// a WebSocket.
	expect := func(when string, connections, webSockets int) {
		t.Helper()
		want := base + 2*connections
		// The last close is the client's own, just made: the broker's
		// goroutines end within the write timeout of it.
		if !testkit.Within(writeTimeout, func() bool {
			n, byCreator := running()
			return n == want && byCreator[testkit.ServedConnection] == webSockets
		}) {
			n, byCreator := running()
			t.Errorf("%s: Sagüin runs %d goroutines besides main, by the function starting them %v; R1's table gives %d, %d of them WebSocket readers (%s)",
				when, n, byCreator, want, webSockets, testkit.ServedConnection)
		}
	}
	harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "r1-v5", CleanStart: true})
	harness.Connected(t, b.addr, harness.V311, &harness.Connect{ClientID: "r1-v311", CleanStart: true})
	through, err := harness.DialUnix(proxied.Path, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	harness.SendBytes(t, through, harness.ProxyHeader(harness.ProxyProxy, harness.ProxyTCP4,
		netip.MustParseAddrPort("192.0.2.10:51234"), netip.MustParseAddrPort("192.0.2.1:1883")))
	harness.ConnectedOver(t, through, &harness.Connect{ClientID: "r1-proxied", CleanStart: true})
	ws, err := harness.DialWebSocket(b.ws[0], harness.V5, &harness.WebSocketDial{Subprotocols: []string{"mqtt"}})
	if err != nil {
		t.Fatal(err)
	}
	harness.ConnectedOver(t, ws, &harness.Connect{ClientID: "r1-websocket", CleanStart: true})
	expect("two clients connected over TCP, one through a PROXY header and one over a WebSocket", 4, 1)

	// ended reads the broker's DISCONNECT with reason, unless 0, then its
	// end of stream, and closes c.
	ended := func(c *harness.Client, reason byte) {
		t.Helper()
		if reason != 0 {
			if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != reason {
				t.Errorf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, reason)
			}
		}
		harness.ExpectClosed(t, c)
		_ = c.Close() // the broker has ended its side
	}
	c, _ := harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "r1-disconnect", CleanStart: true})
	if err := c.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	ended(c, 0)
	expect("after a client's DISCONNECT", 4, 1)

	old, _ := harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "r1-takeover", CleanStart: true})
	newer, _ := harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "r1-takeover", CleanStart: true})
	ended(old, 0x8E)
	expect("after a takeover", 5, 1)
	if err := newer.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	ended(newer, 0)

	c, _ = harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "r1-keep-alive", CleanStart: true, KeepAlive: 1})
	c.Timeout = 2 * time.Second // past 1.5 times the keep-alive
	ended(c, 0x8D)
	expect("after the keep-alive", 4, 1)

	c, err = harness.Dial(b.addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	if ack, err := c.Connect(&harness.Connect{ClientID: "r1-\x01", CleanStart: true}); err != nil || ack.Reason != 0x85 {
		t.Fatalf("connecting with a control character: %+v, %v; want CONNACK 0x85", ack, err)
	}
	ended(c, 0)
	expect("after a refusal", 4, 1)

	c, _ = harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "r1-malformed", CleanStart: true})
	if err := c.SendRaw([]byte{0xC0, 0x01, 0x00}); err != nil { // a PINGREQ with a body
		t.Fatal(err)
	}
	ended(c, 0x81)
	expect("after a malformed packet", 4, 1)

	c, err = harness.DialUnix(proxied.Path, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	harness.SendBytes(t, c, []byte("PROXY TCP4 192.0.2.10 192.0.2.1 51234 1883\r\n"))
	ended(c, 0)
	expect("after a refused PROXY header", 4, 1)

	if err := ws.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	ended(ws, 0)
	expect("after the WebSocket client's DISCONNECT", 3, 0)

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	b.returned(t, "after SIGTERM")
}

// design.md, Restart: "a listener whose accept fails for any reason but its
// own close reports the error to main. The broker stops. Main logs the error
// once the core has ended (Logging)"; "A stop for a failure, whether a store
// error or a failed accept, follows the graceful stop" ("every client gets
// 0x8B"); "The exit status is [...] It is 1 after a stop caused by a
// failure". Shutting the listening socket down for reading ends its
// listening: the accept waiting on it fails (EINVAL), and the listener did
// not close it. Flows, Logging, What the broker logs of its own life: "The
// start, once it is ready: one INFO line"; "The stop: one INFO line when a
// graceful stop begins, naming what asked for it, and one when it is done".
// The log holds one ERROR line, naming that error, no WARN line, and no
// line at any other level but those INFO lines, at most three.
func TestFailedAcceptStopsTheBroker(t *testing.T) {
	if !alone(t) {
		return
	}
	b := startInProcess(t, &harness.Options{})
	c, _ := harness.Connected(t, b.addr, harness.V5, &harness.Connect{ClientID: "connected", CleanStart: true})
	if err := syscall.Shutdown(b.socket, syscall.SHUT_RD); err != nil {
		t.Fatalf("shutting the listening socket down: %v", err)
	}
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8B {
		t.Errorf("DISCONNECT 0x%02X, want 0x8B (Server shutting down)", d.Reason)
	}
	harness.ExpectClosed(t, c)
	_ = c.Close() // the broker has ended its end
	if code := b.returned(t, "after the failed accept"); code != 1 {
		t.Errorf("run returned %d after the failed accept, want 1", code)
	}
	log := b.log.String()
	if lines := testkit.LogLines(log, "ERROR"); len(lines) != 1 || !strings.Contains(lines[0], syscall.EINVAL.Error()) {
		t.Errorf("ERROR lines: %q; want exactly one, naming the failed accept's error (%q):\n%s", lines, syscall.EINVAL.Error(), log)
	}
	if lines := testkit.LogLines(log, "WARN"); len(lines) != 0 {
		t.Errorf("WARN lines: %q; want none:\n%s", lines, log)
	}
	infos := testkit.LogLines(log, "INFO")
	if len(infos) > 3 {
		t.Errorf("INFO lines: %q; want at most the start's and the stop's two:\n%s", infos, log)
	}
	if n := len(strings.Split(strings.TrimSpace(log), "\n")); n != 1+len(infos) {
		t.Errorf("the log holds %d lines; want the ERROR line and the %d INFO lines alone:\n%s", n, len(infos), log)
	}
}

// design.md, Operations: "saguin --version | prints [...] the version: the
// version recorded in the binary, else the commit it was built from, else
// (devel)". The conformance test (TestVersionIsTheBuilds) runs the shipped
// binary, which make builds in the repository and so stamps with its
// commit; this one runs the command in the test binary, which go test
// builds with neither a version nor a commit recorded, as the oracle reads
// from the binary's own build information first.
func TestVersionOfAnUnstampedBuildIsDevel(t *testing.T) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("the test binary carries no build information to check it is unstamped")
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		t.Fatalf("the test binary records the version %q: nothing here tests the fallback", v)
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			t.Fatalf("the test binary records the commit %q: nothing here tests the fallback", s.Value)
		}
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"--version"}, &stdout, &stderr)
	fields := strings.Fields(stdout.String())
	if code != 0 || stderr.Len() != 0 || strings.Count(stdout.String(), "\n") != 1 || len(fields) == 0 || fields[len(fields)-1] != "(devel)" {
		t.Errorf("exit %d, stdout %q, stderr %q; want 0 and one line ending in (devel)", code, stdout.String(), stderr.String())
	}
}
