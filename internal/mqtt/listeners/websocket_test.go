// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package listeners

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestNewWebsocket(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.Equal(t, "t1", l.id)
	require.Equal(t, testAddr, l.address)
}

func TestWebsocketID(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.Equal(t, "t1", l.ID())
}

func TestWebsocketAddress(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.Equal(t, testAddr, l.Address())
}

func TestWebsocketProtocol(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.Equal(t, "ws", l.Protocol())
}

func TestWebsocketProtocolTLS(t *testing.T) {
	l := NewWebsocket(tlsConfig)
	require.Equal(t, "wss", l.Protocol())
}

func TestWebsocketInit(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.Nil(t, l.listen)
	err := l.Init(logger)
	require.NoError(t, err)
	require.NotNil(t, l.listen)
	l.Close(MockCloser) // Init binds the socket; release it for the next test
}

func TestWebsocketServeAndClose(t *testing.T) {
	l := NewWebsocket(basicConfig)
	_ = l.Init(logger)

	o := make(chan bool)
	go func(o chan bool) {
		l.Serve(MockEstablisher)
		o <- true
	}(o)

	time.Sleep(time.Millisecond)

	var closed bool
	l.Close(func(id string) {
		closed = true
	})

	require.True(t, closed)
	<-o
}

func TestWebsocketServeTLSAndClose(t *testing.T) {
	l := NewWebsocket(tlsConfig)
	err := l.Init(logger)
	require.NoError(t, err)

	o := make(chan bool)
	go func(o chan bool) {
		l.Serve(MockEstablisher)
		o <- true
	}(o)

	time.Sleep(time.Millisecond)
	var closed bool
	l.Close(func(id string) {
		closed = true
	})
	require.Equal(t, true, closed)
	<-o
}

func TestWebsocketFailedToServe(t *testing.T) {
	config := tlsConfig
	config.Address = "wrong_addr"
	l := NewWebsocket(config)
	err := l.Init(logger)
	require.Error(t, err, "an unbindable address is refused at Init, where the caller can see it")

	o := make(chan bool)
	go func(o chan bool) {
		l.Serve(MockEstablisher)
		o <- true
	}(o)

	<-o
	var closed bool
	l.Close(func(id string) {
		closed = true
	})
	require.Equal(t, true, closed)
}

func TestWebsocketUpgrade(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.NoError(t, l.Init(logger))
	defer l.Close(MockCloser)

	e := make(chan bool)
	l.establish = func(id string, c net.Conn) error {
		e <- true
		return nil
	}

	s := httptest.NewServer(http.HandlerFunc(l.handler))
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	require.NoError(t, err)
	require.Equal(t, true, <-e)

	s.Close()
	_ = ws.Close()
}

// dialWithOrigin opens a WebSocket to the listener's handler, sending origin
// as the Origin header when it is not empty, and reports whether the upgrade
// was granted and whether the broker was handed the connection. SELF in
// origin is the test server's host and port.
func dialWithOrigin(t *testing.T, l *Websocket, origin string) (status int, established bool) {
	t.Helper()
	return dialAs(t, l, "", origin)
}

// dialAs is dialWithOrigin with the Host header set as well, which is what a
// browser sends for a page whose name was re-pointed at this broker: it
// connects to the broker's address and names the attacker's domain in both
// headers. An empty host sends the address dialled, as a client does.
func dialAs(t *testing.T, l *Websocket, host, origin string) (status int, established bool) {
	t.Helper()
	handed := make(chan struct{}, 1)
	l.establish = func(id string, c net.Conn) error {
		handed <- struct{}{}
		return nil
	}
	s := httptest.NewServer(http.HandlerFunc(l.handler))
	defer s.Close()
	self := strings.TrimPrefix(s.URL, "http://")
	h := http.Header{}
	if origin != "" {
		h.Set("Origin", strings.ReplaceAll(origin, "SELF", self))
	}
	if host != "" {
		h.Set("Host", strings.ReplaceAll(host, "SELF", self))
	}
	ws, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), h)
	if err == nil {
		defer ws.Close()
	}
	if resp == nil {
		t.Fatalf("no HTTP response to the upgrade: %v", err)
	}
	select {
	case <-handed:
		established = true
	case <-time.After(200 * time.Millisecond):
	}
	return resp.StatusCode, established
}

// RFC 0002 "Which web pages may connect", the two settings as NATS defines
// them: `same_origin` compares the page's scheme, host and port with the
// request's own, `allowed_origins` names sites, and when both are set a page
// must pass both. Every case is a policy and the status the broker answered.
//
// The rebound page is DNS rebinding: the browser dialled this broker's
// address, and names the attacker's domain in Host and Origin alike. It is
// its own site to `same_origin`, which is why only a list closes it.
func TestWebsocketOriginPolicy(t *testing.T) {
	const (
		upgraded = http.StatusSwitchingProtocols
		refused  = http.StatusForbidden
	)
	type dial struct{ host, origin string }
	var (
		noOrigin     = dial{"", ""}
		ownSite      = dial{"", "http://SELF"}
		ownSiteHTTPS = dial{"", "https://SELF"} // this listener is not TLS
		otherSite    = dial{"", "http://evil.example"}
		otherPort    = dial{"", "http://127.0.0.1:1"}
		rebound      = dial{"evil.example:8083", "http://evil.example:8083"}
		ownByName    = dial{"broker.example:8083", "http://broker.example:8083"}
		dashboard    = dial{"", "https://dashboard.example.com"}
		dashboardUp  = dial{"", "https://DASHBOARD.example.com"}
		dashboardAlt = dial{"", "http://dashboard.example.com"}
	)
	for _, tc := range []struct {
		name   string
		policy OriginPolicy
		want   map[dial]int
	}{
		{"the default: its own site only", DefaultOriginPolicy, map[dial]int{
			noOrigin: upgraded, ownSite: upgraded, ownSiteHTTPS: refused,
			otherSite: refused, otherPort: refused, rebound: upgraded, dashboard: refused,
		}},
		{"same_origin off with a list: the list only", OriginPolicy{
			SameOrigin: false, Allowed: []string{"https://dashboard.example.com"},
		}, map[dial]int{
			noOrigin: upgraded, dashboard: upgraded, dashboardUp: upgraded,
			dashboardAlt: refused, ownSite: refused, otherSite: refused, rebound: refused,
		}},
		// The broker reached by its own name, and only that name listed: the
		// rebound page is its own site too, and is refused for not being listed.
		{"both: its own site, and only as listed", OriginPolicy{
			SameOrigin: true, Allowed: []string{"http://broker.example:8083"},
		}, map[dial]int{
			noOrigin: upgraded, ownByName: upgraded, ownSite: refused, rebound: refused,
			otherSite: refused, dashboard: refused,
		}},
		{"both off: every site, as NATS does", OriginPolicy{SameOrigin: false}, map[dial]int{
			noOrigin: upgraded, ownSite: upgraded, otherSite: upgraded, rebound: upgraded,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := NewWebsocket(basicConfig)
			require.NoError(t, l.Init(logger))
			defer l.Close(MockCloser)
			if tc.policy.SameOrigin != DefaultOriginPolicy.SameOrigin || tc.policy.Allowed != nil {
				l.SetOriginPolicy(tc.policy)
			}
			for d, want := range tc.want {
				status, established := dialAs(t, l, d.host, d.origin)
				require.Equal(t, want, status, "Host %q Origin %q", d.host, d.origin)
				require.Equal(t, want == upgraded, established,
					"Host %q Origin %q: answered %d but reached the broker=%v", d.host, d.origin, status, established)
			}
		})
	}
}

// **The policy is replaced whole on a listener already serving**, which is
// what SIGUSR1 does to it: a site added is admitted, a site dropped is not,
// and turning same_origin off takes effect on the next upgrade.
func TestWebsocketOriginPolicyIsReplacedInPlace(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.NoError(t, l.Init(logger))
	defer l.Close(MockCloser)

	l.SetOriginPolicy(OriginPolicy{SameOrigin: false, Allowed: []string{"https://one.example"}})
	status, _ := dialWithOrigin(t, l, "https://one.example")
	require.Equal(t, http.StatusSwitchingProtocols, status, "a listed site")

	l.SetOriginPolicy(OriginPolicy{SameOrigin: false, Allowed: []string{"https://two.example"}})
	status, _ = dialWithOrigin(t, l, "https://two.example")
	require.Equal(t, http.StatusSwitchingProtocols, status, "a site added by a new list")
	status, _ = dialWithOrigin(t, l, "https://one.example")
	require.Equal(t, http.StatusForbidden, status, "a site the new list dropped")

	l.SetOriginPolicy(OriginPolicy{SameOrigin: true, Allowed: []string{"https://two.example"}})
	status, _ = dialWithOrigin(t, l, "https://two.example")
	require.Equal(t, http.StatusForbidden, status, "a listed site once same_origin is on again")
}

// **A refused upgrade leaves a line naming the page's origin**, as EMQX's
// does, so an operator whose dashboard stopped connecting can read why in the
// broker's own log rather than in a browser console they may not have. An
// admitted upgrade leaves none, and the origin is bounded: it is a string a
// page chose.
func TestWebsocketLogsARefusedOrigin(t *testing.T) {
	buf := new(syncBuffer)
	l := NewWebsocket(basicConfig)
	require.NoError(t, l.Init(slog.New(slog.NewTextHandler(buf, nil))))
	defer l.Close(MockCloser)

	status, _ := dialWithOrigin(t, l, "http://SELF")
	require.Equal(t, http.StatusSwitchingProtocols, status)
	require.NotContains(t, buf.String(), "origin", "an admitted upgrade was logged")

	status, _ = dialWithOrigin(t, l, "http://evil.example")
	require.Equal(t, http.StatusForbidden, status)
	require.Contains(t, buf.String(), "http://evil.example", "a refused origin was not logged")

	long := "http://" + strings.Repeat("a", 5000) + ".example"
	status, _ = dialWithOrigin(t, l, long)
	require.Equal(t, http.StatusForbidden, status)
	require.NotContains(t, buf.String(), long, "a 5kB origin reached the log whole")
	require.Contains(t, buf.String(), "truncated", "a long origin was not marked as cut")
}

// What an operator may write as an origin, and what it is compared as.
func TestNormalizeOrigin(t *testing.T) {
	for in, want := range map[string]string{
		"https://Dashboard.Example.com":     "https://dashboard.example.com",
		"https://dashboard.example.com:443": "https://dashboard.example.com",
		"http://dashboard.example.com:80":   "http://dashboard.example.com",
		"http://10.0.0.5:8080":              "http://10.0.0.5:8080",
		"https://[::1]:8443":                "https://[::1]:8443",
	} {
		got, err := NormalizeOrigin(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{
		"", "dashboard.example.com", "https://dashboard.example.com/",
		"https://dashboard.example.com/app", "https://dashboard.example.com?x=1",
		"https://user@dashboard.example.com", "ftp://dashboard.example.com",
		"*", "https://*.example.com", "https://",
	} {
		_, err := NormalizeOrigin(in)
		require.Error(t, err, "%q was accepted as an origin", in)
	}
}

func TestWebsocketConnectionReads(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.NoError(t, l.Init(nil))
	defer l.Close(MockCloser)

	recv := make(chan []byte)
	l.establish = func(id string, c net.Conn) error {
		var out []byte
		for {
			buf := make([]byte, 2048)
			n, err := c.Read(buf)
			require.NoError(t, err)
			out = append(out, buf[:n]...)
			if n < 2048 {
				break
			}
		}

		recv <- out
		return nil
	}

	s := httptest.NewServer(http.HandlerFunc(l.handler))
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	require.NoError(t, err)

	pkt := make([]byte, 3000) // make sure this is >2048
	for i := 0; i < len(pkt); i++ {
		pkt[i] = byte(i % 100)
	}

	err = ws.WriteMessage(websocket.BinaryMessage, pkt)
	require.NoError(t, err)

	got := <-recv
	require.Equal(t, 3000, len(got))
	require.Equal(t, pkt, got)

	s.Close()
	_ = ws.Close()
}

func TestWebsocketWriteDeadlineEndsAWriteToAClientThatNeverReads(t *testing.T) {
	l := NewWebsocket(basicConfig)
	require.NoError(t, l.Init(nil))
	defer l.Close(MockCloser)

	result := make(chan error, 1)
	l.establish = func(id string, c net.Conn) error {
		buf := make([]byte, 4096)
		for {
			if err := c.SetWriteDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
				result <- err
				return nil
			}
			if _, err := c.Write(buf); err != nil {
				result <- err
				return nil
			}
		}
	}

	s := httptest.NewServer(http.HandlerFunc(l.handler))
	defer s.Close()

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	require.NoError(t, err)
	defer ws.Close()
	// The client never reads, so the writes above fill the socket and stop.

	select {
	case err := <-result:
		var ne net.Error
		require.True(t, errors.As(err, &ne) && ne.Timeout(), "want a timeout, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("a write to a websocket client that never reads did not end: a deadline " +
			"set on this net.Conn has to reach the write, and gorilla overwrites one set " +
			"on the socket underneath it")
	}
}

// A ":0" address is bound at Init, so Address reports the port the kernel
// assigned and the port is held before Serve runs - nothing else in the
// process can take it in between, and a caller told the address can dial it.
func TestWebsocketAddressIsTheBoundPortAfterInit(t *testing.T) {
	l := NewWebsocket(Config{ID: "t1", Address: "127.0.0.1:0"})
	require.NoError(t, l.Init(logger))
	defer l.Close(MockCloser)

	addr := l.Address()
	require.NotEqual(t, "127.0.0.1:0", addr)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err, "the door must accept from the moment Init returns")
	_ = conn.Close()
	_, err = net.Listen("tcp", addr)
	require.Error(t, err, "the port must be held, not merely reserved")
}
