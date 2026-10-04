package mqtt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// selfSigned is a server certificate for 127.0.0.1, made for one test.
func selfSigned(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

// wsServer is a server with a tcp door and a ws door - wss when tlsConfig is
// set - at max_connections limit and the given connect_timeout.
func wsServer(t *testing.T, limit int64, bound time.Duration, tlsConfig *tls.Config) (tcpAddr, wsAddr string) {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: bound})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	ws := listeners.NewWebsocket(listeners.Config{ID: "w", Address: "127.0.0.1:0", TLSConfig: tlsConfig})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.AddListener(ws))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	return tcp.Address(), ws.Address()
}

// dialWSS upgrades on a wss door, pausing before the TLS handshake and again
// before the upgrade request, so each phase before the CONNECT can be made
// to take time of its own.
func dialWSS(addr string, beforeTLS, beforeRequest time.Duration) (*websocket.Conn, error) {
	d := websocket.Dialer{
		Subprotocols: []string{"mqtt"},
		NetDialTLSContext: func(ctx context.Context, network, a string) (net.Conn, error) {
			raw, err := net.Dial(network, a)
			if err != nil {
				return nil, err
			}
			time.Sleep(beforeTLS)
			tc := tls.Client(raw, &tls.Config{InsecureSkipVerify: true})
			if err := tc.HandshakeContext(ctx); err != nil {
				_ = raw.Close()
				return nil, err
			}
			time.Sleep(beforeRequest)
			return tc, nil
		},
	}
	wc, _, err := d.Dial("wss://"+addr+"/", nil)
	return wc, err
}

// wsConnack sends a CONNECT over an upgraded connection and returns the
// CONNACK's reason code, or 0xFF when none came back.
func wsConnack(wc *websocket.Conn, id string) byte {
	if err := wc.WriteMessage(websocket.BinaryMessage, takeoverConnect(id)); err != nil {
		return 0xFF
	}
	_ = wc.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, b, err := wc.ReadMessage()
	if err != nil || len(b) < 4 || b[0] != 0x20 {
		return 0xFF
	}
	return b[3]
}

// RFC 0002 "How long a socket may wait to send CONNECT": on a ws door the
// bound runs once, from the moment the socket is accepted, over the TLS
// handshake, the HTTP request and the CONNECT together.
//
// **Each phase had a clock of its own**: net/http's for the handshake and
// again for the headers, its fixed 60 s for a body the request declared,
// and the server's from the upgrade. Pauses of 2.5 s in each were admitted
// 7.5 s after arrival at a bound of 3 s, and a GET declaring a body it never
// sent held its socket - and its max_connections slot - for 60 s.
func TestAWebsocketDoorHasOneBoundFromArrival(t *testing.T) {
	const bound = 2 * time.Second
	_, addr := wsServer(t, 10, bound, selfSigned(t))

	// The control: the three phases inside the bound between them.
	t.Run("three short pauses are admitted", func(t *testing.T) {
		wc, err := dialWSS(addr, 300*time.Millisecond, 300*time.Millisecond)
		require.NoError(t, err)
		defer wc.Close()
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, byte(0x00), wsConnack(wc, "short"),
			"a CONNECT sent 0.9s after arrival was refused at a bound of 2s")
	})

	// Each pause inside the bound, the three together past it.
	t.Run("three pauses each inside the bound are not", func(t *testing.T) {
		began := time.Now()
		wc, err := dialWSS(addr, 800*time.Millisecond, 800*time.Millisecond)
		require.NoError(t, err, "the upgrade, 1.6s after arrival, is inside the bound")
		defer wc.Close()
		time.Sleep(800 * time.Millisecond)
		code := wsConnack(wc, "slow")
		require.NotEqual(t, byte(0x00), code,
			"a CONNECT sent %s after arrival was admitted at a bound of %s: each phase was "+
				"timed on its own", time.Since(began).Round(100*time.Millisecond), bound)
	})

	t.Run("a request declaring a body it never sends", func(t *testing.T) {
		c, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
		require.NoError(t, err)
		defer c.Close()
		began := time.Now()
		_, err = c.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n"))
		require.NoError(t, err)
		_ = c.SetReadDeadline(time.Now().Add(20 * time.Second))
		_, _ = io.Copy(io.Discard, c) // until the broker closes it
		held := time.Since(began)
		require.Less(t, held, bound+time.Second,
			"the socket was held %s by a body it never sent, past the bound of %s", held, bound)
	})

	// The bound ends at the upgrade: from there the session's deadlines are
	// the server's, and one outliving the bound goes on answering.
	for _, secure := range []bool{false, true} {
		name := "ws"
		if secure {
			name = "wss"
		}
		t.Run("a "+name+" session outlives the bound", func(t *testing.T) {
			var tc *tls.Config
			if secure {
				tc = selfSigned(t)
			}
			_, addr := wsServer(t, 10, bound, tc)
			d := websocket.Dialer{Subprotocols: []string{"mqtt"},
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
			wc, _, err := d.Dial(name+"://"+addr+"/", nil)
			require.NoError(t, err)
			defer wc.Close()
			require.Equal(t, byte(0x00), wsConnack(wc, "long-"+name))
			time.Sleep(bound + 500*time.Millisecond)
			require.NoError(t, wc.WriteMessage(websocket.BinaryMessage, []byte{0xC0, 0x00})) // PINGREQ
			_ = wc.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, b, err := wc.ReadMessage()
			require.NoError(t, err, "a session %s old was ended by the bound on the time before its CONNECT",
				bound+500*time.Millisecond)
			require.Equal(t, []byte{0xD0, 0x00}, b, "a PINGREQ was not answered PINGRESP")
		})
	}
}

// socketRefusals records the sockets a listener refused at accept.
type socketRefusals struct {
	HookBase
	mu    sync.Mutex
	codes []packets.Code
	on    []string
}

func (h *socketRefusals) ID() string           { return "socket-refusals" }
func (h *socketRefusals) Provides(b byte) bool { return b == OnSocketRefused }
func (h *socketRefusals) count() int           { h.mu.Lock(); defer h.mu.Unlock(); return len(h.codes) }
func (h *socketRefusals) OnSocketRefused(listener string, code packets.Code) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.codes = append(h.codes, code)
	h.on = append(h.on, listener)
}

// RFC 0005 saguin_connections_refused_total, "the series behind a fleet in
// a reconnect loop". A ws socket that finds every max_connections slot taken
// is closed at accept with nothing written (RFC 0002), and **was counted
// nowhere**: before that close moved to accept, five ws knocks at the limit
// read 0x89 five times on the series, and after it none. The listener now tells the server, which tells the
// hooks, as Server busy.
func TestASocketRefusedAtAcceptIsReported(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 1
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: 10 * time.Second})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	rec := new(socketRefusals)
	require.NoError(t, s.AddHook(rec, nil))
	ws := listeners.NewWebsocket(listeners.Config{ID: "w", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(ws))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })

	holder, err := net.Dial("tcp", ws.Address()) // takes the one slot
	require.NoError(t, err)
	defer holder.Close()
	require.Eventually(t, func() bool { return s.slots.Load() == 1 }, 5*time.Second, time.Millisecond,
		"the holder never took the slot")
	for range 5 {
		c, err := net.Dial("tcp", ws.Address())
		require.NoError(t, err)
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = c.Read(make([]byte, 1)) // closed at accept
		_ = c.Close()
	}
	require.Eventually(t, func() bool { return rec.count() == 5 }, 2*time.Second, 10*time.Millisecond,
		"%d of 5 sockets refused at accept were reported", rec.count())
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i, code := range rec.codes {
		require.Equal(t, packets.ErrServerBusy.Code, code.Code, "refusal %d", i)
		require.Equal(t, "w", rec.on[i], "refusal %d", i)
	}
}
