package proxyproto_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/hooks/auth"
	"github.com/ifnesi/saguin/internal/proxyproto"
)

// shortSocketDir is `t.TempDir()` for a Unix socket path specifically,
// which `t.TempDir()` itself is not safe for: it embeds the test's own
// name, and a name long enough - combined with macOS's default `TMPDIR`,
// already several directories deep under `/var/folders` - overruns the
// ~104-byte `sun_path` a Unix socket address is limited to. `/tmp` itself
// is short on every platform this runs on and is not subject to `TMPDIR`.
func shortSocketDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sgn")
	if err != nil {
		t.Fatalf("a short-enough temp dir for a socket: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// The listener end to end, on a real socket, with the bytes HAProxy really
// sent: what reaches the broker is a connection whose RemoteAddr is the
// client rather than the socket's peer, carrying the name the proxy
// verified - and whose next byte is the client's CONNECT.
func TestTheListenerHandsOnWhatTheProxySaid(t *testing.T) {
	path := filepath.Join(shortSocketDir(t), "s.sock")
	l := proxyproto.NewUnixSock("unix", path, 0o600)
	if err := l.Init(slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("init: %v", err)
	}

	got := make(chan net.Conn, 1)
	go l.Serve(func(id string, c net.Conn) error {
		got <- c
		return nil
	})
	t.Cleanup(func() { l.Close(func(string) {}) })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	raw, err := hex.DecodeString(haproxyHeader + firstMQTTPacket)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case c := <-got:
		if addr := c.RemoteAddr().String(); addr != "127.0.0.1:55020" {
			t.Errorf("the broker sees %q as the client, want 127.0.0.1:55020 - otherwise "+
				"every client behind the proxy is one local peer", addr)
		}
		if name := proxyproto.CommonNameOf(c); name != "lighthouse-99" {
			t.Errorf("the connection carries the name %q, want lighthouse-99", name)
		}
		// The CONNECT has to still be there: the listener reads the header
		// and stops, or the broker's first read finds a truncated packet.
		buf := make([]byte, len(firstMQTTPacket)/2)
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatalf("reading the CONNECT that followed the header: %v", err)
		}
		if hex.EncodeToString(buf) != firstMQTTPacket {
			t.Errorf("the first packet reached the broker as %q, want the CONNECT %q",
				hex.EncodeToString(buf), firstMQTTPacket)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the listener never handed the connection on")
	}
}

// **A connection with no header is closed rather than served.** This
// listener exists because a proxy is in front of it: serving one anyway
// would mean the address in every log line depended on whether the proxy
// was working, which is the kind of difference nobody notices until they
// are reading logs to find out who did something.
func TestTheListenerRefusesAConnectionWithNoHeader(t *testing.T) {
	path := filepath.Join(shortSocketDir(t), "s.sock")
	l := proxyproto.NewUnixSock("unix", path, 0o600)
	if err := l.Init(slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("init: %v", err)
	}
	// The header is read by the first read, so "served" is a read that
	// returned bytes - which is what the server would have acted on.
	served := make(chan struct{}, 1)
	go l.Serve(func(id string, c net.Conn) error {
		if n, _ := c.Read(make([]byte, 1)); n > 0 {
			served <- struct{}{}
		}
		return nil
	})
	t.Cleanup(func() { l.Close(func(string) {}) })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// An ordinary MQTT client, connecting straight to the socket.
	if _, err := conn.Write([]byte{0x10, 0x12, 0x00, 0x04, 'M', 'Q', 'T', 'T', 5}); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case <-served:
		t.Fatal("a connection with no PROXY header was served: its address would be the " +
			"socket's peer, reported as though a proxy had vouched for it")
	case <-time.After(500 * time.Millisecond):
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Error("the connection was left open after being refused")
	}
}

// The socket carries the mode it was given, which is the access control for
// a Unix listener: whoever can open the file can speak to the broker, and
// on this listener can also assert who its client is.
func TestTheSocketCarriesItsMode(t *testing.T) {
	path := filepath.Join(shortSocketDir(t), "s.sock")
	l := proxyproto.NewUnixSock("unix", path, 0o660)
	if err := l.Init(slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(func() { l.Close(func(string) {}) })

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o660 {
		t.Errorf("the socket is %04o, want 0660", got)
	}
}

// **The net.Listener half, which had no test at all.**
//
// `Listener` is what the operations endpoints use - they speak HTTP, so
// they need a net.Listener where the MQTT side needs mochi's interface.
// It was added, driven by hand against nginx, and shipped with nothing
// holding it to any of its three answers. Driving
// all three closed the gap; this is them, kept.
func TestTheHTTPListenerAnswersEveryWayAProxyCanBeWrong(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// serve accepts one connection through the wrapper and reports what
	// arrived, or that nothing did. Accept no longer reads, so "accepted"
	// is the first read after the header returning the byte sent behind it.
	serve := func(t *testing.T, send []byte) (name string, accepted bool) {
		send = append(append([]byte{}, send...), 'x')
		t.Helper()
		path := filepath.Join(shortSocketDir(t), "s.sock")
		raw, err := net.Listen("unix", path)
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		defer func() { _ = raw.Close() }()
		l := proxyproto.NewListener(raw, log)

		got := make(chan net.Conn, 1)
		go func() {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			if n, _ := c.Read(make([]byte, 1)); n == 1 {
				got <- c
			}
		}()

		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = c.Close() }()
		if _, err := c.Write(send); err != nil {
			t.Fatalf("write: %v", err)
		}
		select {
		case accepted := <-got:
			return proxyproto.CommonNameOf(accepted), true
		case <-time.After(time.Second):
			return "", false
		}
	}

	t.Run("a v2 header is accepted and carries the name", func(t *testing.T) {
		name, accepted := serve(t, decodeHex(t, haproxyHeader))
		if !accepted {
			t.Fatal("a good PROXY v2 header was not accepted")
		}
		if name == "" {
			t.Error("the connection carries no Common Name, so nothing downstream " +
				"can say who the proxy verified")
		}
	})

	// **Not a client that forgot.** On a listener declared to be behind a
	// proxy, bytes that are not a PROXY header are something speaking a
	// different protocol - serving them would mean serving whatever a local
	// caller sent while the operator believes a proxy is in front.
	t.Run("bytes that are not a header are dropped", func(t *testing.T) {
		if _, accepted := serve(t, []byte("GET /metrics HTTP/1.1\r\n\r\n")); accepted {
			t.Error("a plain HTTP request was accepted on a socket declared to be " +
				"behind a proxy, so it would be served with no name and no proxy")
		}
	})

	// v1 is the natural mistake - `proxy_protocol on` in nginx - and it
	// carries no TLVs, so it can never say who was verified.
	t.Run("a v1 header is dropped", func(t *testing.T) {
		if _, accepted := serve(t, []byte("PROXY TCP4 10.0.0.1 10.0.0.2 5000 443\r\n")); accepted {
			t.Error("a PROXY v1 header was accepted; it carries no name, so every " +
				"caller through it would be nobody")
		}
	})

	// A proxy's own health check describes no client. Closing it is right;
	// what matters is that it does not arrive as a nameless connection.
	t.Run("a LOCAL command is not handed on", func(t *testing.T) {
		local := decodeHex(t, haproxyHeader)
		local[12] = 0x20 // v2, command LOCAL
		if _, accepted := serve(t, local); accepted {
			t.Error("a LOCAL header was handed on as a connection; it describes no " +
				"client, so it would be served as nobody")
		}
	})
}

// serveMQTTBehindProxy runs a broker whose only listener is the MQTT socket
// declared to be behind a proxy, and returns the server and the path.
func serveMQTTBehindProxy(t *testing.T) (*mqtt.Server, string) {
	t.Helper()
	s := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := s.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("hook: %v", err)
	}
	path := filepath.Join(shortSocketDir(t), "s.sock")
	if err := s.AddListener(proxyproto.NewUnixSock("unix", path, 0o600)); err != nil {
		t.Fatalf("listener: %v", err)
	}
	if err := s.Serve(); err != nil {
		t.Fatalf("serve: %v", err)
	}
	return s, path
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// A peer that never sends its header is closed rather than held, on the
// MQTT socket as on the operations one.
func TestASilentPeerOnTheMQTTProxySocketIsClosed(t *testing.T) {
	s, path := serveMQTTBehindProxy(t)
	defer s.Close()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
	n, err := c.Read(make([]byte, 64))
	if isTimeout(err) {
		t.Fatal("a peer that sent no PROXY header was still open after 8s")
	}
	if n != 0 {
		t.Fatalf("the broker sent %d bytes to a peer that sent no header", n)
	}
}

// **Shutdown closes a peer still sending its header.** Close returning is
// not enough: the header used to be read outside everything shutdown
// reaches, so Close returned and left the socket open behind it.
func TestClosingTheBrokerClosesAPeerStillSendingItsHeader(t *testing.T) {
	s, path := serveMQTTBehindProxy(t)
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	time.Sleep(100 * time.Millisecond)

	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close is waiting on a peer that has not sent its header")
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); isTimeout(err) {
		t.Fatal("Close returned and the peer that never sent its header is still open")
	}
}

// **A CONNECT behind a bad header is never answered; behind a good one it
// is, as the client the proxy named.** The good case is the control: it
// proves the CONNECT used here is one the broker admits, so the refusals
// are about the header.
func TestTheMQTTProxySocketAdmitsOnlyAGoodHeader(t *testing.T) {
	s, path := serveMQTTBehindProxy(t)
	defer s.Close()
	connect := decodeHex(t, firstMQTTPacket)
	local := decodeHex(t, haproxyHeader)
	local[12] = 0x20 // v2, command LOCAL

	send := func(t *testing.T, b []byte) ([]byte, error) {
		t.Helper()
		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		if _, err := c.Write(b); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 64)
		n, err := c.Read(buf)
		return buf[:n], err
	}

	t.Run("a good header", func(t *testing.T) {
		b, err := send(t, append(decodeHex(t, haproxyHeader), connect...))
		if len(b) == 0 || b[0] != 0x20 {
			t.Fatalf("a CONNECT behind a good header got % x (%v), want a CONNACK", b, err)
		}
		// **Waited for, because the CONNACK does not say it has happened.**
		// The broker writes the CONNACK and only then registers the
		// connection, so that nothing can be written to a client before its
		// CONNACK [MQTT-3.2.0-1]. Read once, this found no client in 8 runs
		// of 100, and failed on a loaded CI runner.
		var cl *mqtt.Client
		var ok bool
		for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
			if cl, ok = s.Clients.Get("probe"); ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the client is not registered two seconds after its CONNACK")
			}
		}
		if cl.Net.Remote != "127.0.0.1:55020" {
			t.Errorf("the client is registered as %q, want the address the proxy named", cl.Net.Remote)
		}
	})
	for _, tc := range []struct {
		name string
		send []byte
	}{
		{"no header", connect},
		// **Exactly as long as a header's fixed part, then a good CONNECT.**
		// The other refusals mangle the CONNECT behind them as well, so a
		// reader that swallowed a bad header and carried on would still see
		// them refused - by the packet, not by the header. This is the one
		// such a reader would admit.
		{"sixteen bytes that are not a signature", append([]byte("0d0a0d0a000d0a51"), connect...)},
		{"a v1 header", append([]byte("PROXY TCP4 10.0.0.1 10.0.0.2 5000 443\r\n"), connect...)},
		{"a LOCAL command", append(local, connect...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := send(t, tc.send)
			if len(b) > 0 {
				t.Fatalf("%s was answered: % x", tc.name, b)
			}
			if isTimeout(err) {
				t.Fatalf("%s was neither answered nor closed within 2s", tc.name)
			}
		})
	}
}

// lockedBuffer is a log destination a test reads while the listener's
// goroutine may still be writing to it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// **A certificate the proxy could not verify is served with no name, and the
// operator is told why.** The connection is still served - it falls back to
// whatever else the door asks for, as a client with no certificate does -
// but a device that believes it holds a good certificate and is refused a
// password would otherwise leave nothing in the log to say which of the two
// was wrong. The trusted capture is the control: same proxy, same name, no
// warning.
func TestAnUnverifiedCertificateIsServedNamelessAndLogged(t *testing.T) {
	for _, tc := range []struct {
		name, header, wantName string
		warned                 bool
	}{
		{"trusted", nginxTrustedHeader, "device-7", false},
		{"untrusted", nginxUntrustedHeader, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs lockedBuffer
			path := filepath.Join(shortSocketDir(t), "s.sock")
			l := proxyproto.NewUnixSock("unix", path, 0o600)
			if err := l.Init(slog.New(slog.NewTextHandler(&logs, nil))); err != nil {
				t.Fatalf("init: %v", err)
			}
			got := make(chan net.Conn, 1)
			go l.Serve(func(id string, c net.Conn) error {
				got <- c
				return nil
			})
			t.Cleanup(func() { l.Close(func(string) {}) })

			conn, err := net.Dial("unix", path)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			if _, err := conn.Write(decodeHex(t, tc.header+firstMQTTPacket)); err != nil {
				t.Fatalf("write: %v", err)
			}

			var c net.Conn
			select {
			case c = <-got:
			case <-time.After(3 * time.Second):
				t.Fatal("the listener never handed the connection on")
			}
			// Served: the CONNECT behind the header is still readable.
			buf := make([]byte, len(firstMQTTPacket)/2)
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.ReadFull(c, buf); err != nil {
				t.Fatalf("the connection was not served: %v", err)
			}
			if name := proxyproto.CommonNameOf(c); name != tc.wantName {
				t.Errorf("the connection carries the name %q, want %q", name, tc.wantName)
			}

			out := logs.String()
			warned := strings.Contains(out, "could not verify")
			if warned != tc.warned {
				t.Errorf("warned about an unverified certificate: %v, want %v. The log:\n%s",
					warned, tc.warned, out)
			}
			if tc.warned && !strings.Contains(out, "verify_result=21") {
				t.Errorf("the warning does not carry the verify result nginx sent, which is "+
					"what says why:\n%s", out)
			}
		})
	}
}

// decodeHex is the hex helper the other file has, under a name this one
// can use without either moving.
func decodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return b
}
