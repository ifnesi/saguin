// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package listeners

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/gorilla/websocket"
)

var (
	// ErrInvalidMessage indicates that a message payload was not valid.
	ErrInvalidMessage = errors.New("message type not binary")
)

// Websocket is a listener for establishing websocket connections.
type Websocket struct { // [MQTT-4.2.0-1]
	sync.RWMutex
	id        string                       // the internal id of the listener
	address   string                       // the network address to bind to
	config    Config                       // configuration values for the listener
	listen    *http.Server                 // a http server for serving websocket connections
	ln        net.Listener                 // the bound socket, held from Init so Address reports a real port
	log       *slog.Logger                 // server logger
	establish EstablishFn                  // the server's establish connection handler
	upgrader  *websocket.Upgrader          //  upgrade the incoming http/tcp connection to a websocket compliant connection.
	end       uint32                       // ensure the close methods are only called once
	policy    atomic.Pointer[OriginPolicy] // which pages may connect; replaced whole on SIGUSR1
	bound     func(string) string          // shortens a page's string for the log; set before Serve
	admit     Admission                    // the server's connection count; set before Init
}

// SetAdmission is called by the server before Init.
//
// **A socket on this door holds a max_connections slot from the moment it
// arrives**, as on every other door (RFC 0002 "How long a socket may wait
// to send CONNECT"). The server counts a connection when it is handed one,
// and this door hands it one only after the HTTP upgrade: before that the
// socket belonged to net/http, which held it up to its 60 s ReadTimeout
// and counted it against nothing. Fifty idle sockets here left a tcp
// CONNECT answered 0x00 at max_connections 2. So the slot is taken at
// accept, or waited for as any connection waits (Server.arrive), and the
// socket is closed with nothing written when it gets none - as mosquitto's
// own websockets and EMQX's listeners do; neither answers 0x89 or an HTTP
// 503 before the upgrade.
//
// **And one connect_timeout from arrival bounds everything before the
// CONNECT** - the TLS handshake, the HTTP request with any body it
// declares, and the CONNECT itself - as RFC 0002 says. Each used to have a
// clock of its own: net/http's for the handshake and again for the headers,
// its fixed 60 s for a declared body, and the server's from the upgrade.
// Three pauses of 2.5 s were admitted 7.5 s after arrival at a bound of 3 s,
// and a GET declaring a body it never sent held its slot for 60 s
// (admittedConn).
func (l *Websocket) SetAdmission(a Admission) { l.admit = a }

// OriginPolicy is which web pages may open the listener, with the meaning the
// NATS server gives its websocket `same_origin` and `allowed_origins` - a
// broker that has shipped the pair, where inventing a rule of our own would
// be one nobody has run.
//
//   - SameOrigin: the page's scheme, host and port must be the request's own -
//     https on a TLS listener, and the host and port the browser connected to.
//   - Allowed: the page's origin must be one of these. Empty checks nothing.
//
// When both are set a page must pass both, as in NATS. With neither, every
// page is admitted.
type OriginPolicy struct {
	SameOrigin bool
	Allowed    []string
}

// DefaultOriginPolicy is a listener's policy until SetOriginPolicy is called:
// its own site only.
//
// **NATS defaults same_origin to false, and saguin does not.** Off with no
// list admits a page from any site, which is a hole in this
// listener: any site the operator's browser visited could open it as that
// browser, with the client certificate the browser holds.
var DefaultOriginPolicy = OriginPolicy{SameOrigin: true}

// NewWebsocket initializes and returns a new Websocket listener, listening on an address.
func NewWebsocket(config Config) *Websocket {
	l := &Websocket{
		id:      config.ID,
		address: config.Address,
		config:  config,
		upgrader: &websocket.Upgrader{
			Subprotocols: []string{"mqtt"},
		},
	}
	l.upgrader.CheckOrigin = l.checkOrigin
	l.SetOriginPolicy(DefaultOriginPolicy)
	return l
}

// SetOriginPolicy replaces which pages may connect. It is safe on a listener
// already serving: an upgrade reads one policy or the other, never
// same_origin from one and the list from another.
//
// An entry NormalizeOrigin refuses is left out rather than kept, so a list
// that reaches here without validation can only admit fewer pages.
func (l *Websocket) SetOriginPolicy(p OriginPolicy) {
	list := make([]string, 0, len(p.Allowed))
	for _, o := range p.Allowed {
		if n, err := NormalizeOrigin(o); err == nil {
			list = append(list, n)
		}
	}
	l.policy.Store(&OriginPolicy{SameOrigin: p.SameOrigin, Allowed: list})
}

// SetLogBound sets how a string a page chose is shortened before it reaches
// the log: the broker's limits.Loggable, so this listener keeps no bound of
// its own. Call it before Serve.
func (l *Websocket) SetLogBound(bound func(string) string) {
	l.bound = bound
}

// Loggable is s as the log may carry it: through the bound SetLogBound was
// given, or cut at a kilobyte - the broker's default max_topic_length - for a
// listener constructed without one.
func (l *Websocket) Loggable(s string) string {
	if l.bound != nil {
		return l.bound(s)
	}
	const max = 1024
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("…(truncated from %d bytes)", len(s))
}

// checkOrigin decides whether an upgrade is allowed, from its Origin header.
//
// **Every browser sends Origin on a WebSocket upgrade, and a request without
// one is admitted**, as NATS, EMQX's default and mosquitto's built-in
// websockets all do: the header is how a browser names the page that is
// asking, and a client that is not a browser has no page. Paho for Go,
// autopaho and MQTT.js under Node send none. paho-mqtt for Python sends the
// broker's own scheme, host and port, which same_origin admits.
//
// A refusal is logged, as EMQX logs one, so an operator whose dashboard
// stopped connecting reads why in the broker's log.
func (l *Websocket) checkOrigin(r *http.Request) bool {
	values := r.Header["Origin"]
	if len(values) == 0 {
		return true
	}
	if l.originAdmitted(r, values[0]) {
		return true
	}
	if l.log != nil {
		l.log.Warn("ws: refused an upgrade from a page this listener does not admit; list its "+
			"origin in broker.mqtt.listen.ws.allowed_origins, with same_origin: false if it is "+
			"not this listener's own site", "origin", l.Loggable(values[0]))
	}
	return false
}

// originAdmitted applies the policy to one Origin.
func (l *Websocket) originAdmitted(r *http.Request, origin string) bool {
	p := l.policy.Load()
	if !p.SameOrigin && len(p.Allowed) == 0 {
		return true
	}
	n, err := NormalizeOrigin(origin)
	if err != nil {
		return false
	}
	if p.SameOrigin {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		// Both sides through one normaliser, so the default port and case are
		// settled the same way for the page and for the request.
		own, err := NormalizeOrigin(scheme + "://" + r.Host)
		if err != nil || n != own {
			return false
		}
	}
	if len(p.Allowed) > 0 && !slices.Contains(p.Allowed, n) {
		return false
	}
	return true
}

// NormalizeOrigin returns an origin in the form it is compared in - lower
// case, and without the scheme's default port, which is how a browser sends
// it - or an error saying why s is not one.
//
// An origin is a scheme, a host and a port, and nothing else: a browser never
// sends a path, so an entry with one could never match, and an operator who
// wrote one would be left with a refusal and no reason. There are no
// wildcards: each site that may connect is named.
func NormalizeOrigin(s string) (string, error) {
	const form = "write scheme://host or scheme://host:port, with http or https"
	u, err := url.Parse(strings.TrimSpace(s))
	switch {
	case err != nil:
		return "", fmt.Errorf("is not an origin: %v; %s", err, form)
	case u.Scheme != "http" && u.Scheme != "https":
		return "", fmt.Errorf("is not an origin: %s", form)
	case u.Opaque != "" || u.User != nil || u.Path != "" || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "":
		return "", fmt.Errorf("carries more than an origin: a browser sends no path, "+
			"query or user, so it could never match; %s", form)
	case u.Hostname() == "" || strings.HasSuffix(u.Host, ":"):
		return "", fmt.Errorf("names no host: %s", form)
	case strings.Contains(u.Host, "*"):
		return "", fmt.Errorf("has a wildcard, and there are none: name each site")
	}
	host := strings.ToLower(u.Host)
	if p := u.Port(); (u.Scheme == "https" && p == "443") || (u.Scheme == "http" && p == "80") {
		host = strings.TrimSuffix(host, ":"+p)
	}
	return u.Scheme + "://" + host, nil
}

// ID returns the id of the listener.
func (l *Websocket) ID() string {
	return l.id
}

// Address returns the address of the listener: the bound one once Init has
// run, so a ":0" configuration reports the port the kernel actually assigned.
func (l *Websocket) Address() string {
	if l.ln != nil {
		return l.ln.Addr().String()
	}
	return l.address
}

// Protocol returns the address of the listener.
func (l *Websocket) Protocol() string {
	if l.config.TLSConfig != nil {
		return "wss"
	}

	return "ws"
}

// Init initializes the listener.
func (l *Websocket) Init(log *slog.Logger) error {
	l.log = log

	mux := http.NewServeMux()
	mux.HandleFunc("/", l.handler)
	l.listen = &http.Server{
		Addr:         l.address,
		Handler:      mux,
		TLSConfig:    l.config.TLSConfig,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		// **HTTP/1.1 only.** A WebSocket here is an HTTP/1.1 upgrade, which
		// HTTP/2 cannot carry, and over HTTP/2 net/http reports no request
		// read (answering): a refusal there was written with the socket's
		// slot still held. An empty map, not nil, is what turns HTTP/2 off.
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
	if l.admit != nil {
		l.listen.ConnState = answering
		l.listen.ReadHeaderTimeout = l.admit.ConnectTimeout()
		// One upgrade per socket: a request that was not one ends its
		// socket, and its slot, rather than keeping both for keep-alive.
		l.listen.SetKeepAlivesEnabled(false)
	}

	// Bind here, as the TCP listener does, rather than inside Serve: the port
	// is then held from the moment Init returns, a ":0" address is reported
	// correctly by Address, and nothing can take the port between the two.
	var err error
	l.ln, err = net.Listen("tcp", l.address)
	if err != nil {
		return err
	}
	l.ln = RateLimit(l.ln, l.config.ConnectRate)
	if l.admit != nil {
		l.ln = Admit(l.ln, l.admit, l.id, false)
	}
	return nil
}

// answering follows net/http through a socket's one request (keep-alives
// are off): from the moment it has read the request until the handler takes
// the socket over for the upgrade, whatever it writes is the answer that
// refuses the socket (admittedConn.Write).
func answering(c net.Conn, st http.ConnState) {
	ac := admittedUnder(c)
	if ac == nil {
		return
	}
	switch st {
	case http.StateActive:
		ac.answering(true)
	case http.StateHijacked:
		ac.answering(false)
	}
}

// handler upgrades and handles an incoming websocket connection.
func (l *Websocket) handler(w http.ResponseWriter, r *http.Request) {
	c, err := l.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()

	ws := &wsConn{Conn: c.UnderlyingConn(), c: c}
	if ac := admittedUnder(c.UnderlyingConn()); ac != nil {
		ac.upgraded()
		ws.held, ws.arrived = ac, ac.Arrived()
	}
	err = l.establish(l.id, ws)
	if err != nil {
		l.log.Warn("connection ended with an error", "error", err)
	}
}

// Serve starts waiting for new Websocket connections, and calls the connection
// establishment callback for any received.
func (l *Websocket) Serve(establish EstablishFn) {
	var err error
	l.establish = establish

	if l.ln == nil {
		// Init failed (or was never called): there is no socket to serve.
		// Init already returned the bind error to the caller.
		// No "listener" key: the logger AddListener built already carries
		// it, and saying it twice is what #526 fixed.
		l.log.Error("failed to serve.", "error", "listener was not bound")
		return
	}
	if l.listen.TLSConfig != nil {
		err = l.listen.ServeTLS(l.ln, "", "")
	} else {
		err = l.listen.Serve(l.ln)
	}

	// After the listener has been shutdown, no need to print the http.ErrServerClosed error.
	if err != nil && atomic.LoadUint32(&l.end) == 0 {
		l.log.Error("failed to serve.", "error", err)
	}
}

// Close closes the listener and any client connections.
func (l *Websocket) Close(closeClients CloseFn) {
	l.Lock()
	defer l.Unlock()

	if atomic.CompareAndSwapUint32(&l.end, 0, 1) {
		// **Closed, not shut down: every socket not yet upgraded goes now.**
		// Each was admitted as it was accepted and holds a max_connections
		// slot that only its Close gives back. Shutdown waits for those it
		// counts idle, and counts one that has sent no request idle only
		// five seconds after it arrived, so it gave up at its own five and
		// left such a socket open, slot and all, until connect_timeout.
		// Nothing here is worth waiting for: an upgraded connection is
		// net/http's no more (closeClients closes it), and one upgraded
		// from here on is refused (Listeners.Establish).
		_ = l.listen.Close()
		if l.ln != nil {
			// Shutdown closes only the listeners Serve registered; a socket
			// bound at Init and never served is released here.
			_ = l.ln.Close()
		}
	}

	closeClients(l.id)
}

// wsConn is a websocket connection which satisfies the net.Conn interface.
type wsConn struct {
	net.Conn
	c *websocket.Conn

	// held is the socket that took its max_connections slot at accept
	// (SetAdmission), so the server does not take a second; nil for none.
	held *admittedConn
	// arrived is when that socket was accepted, zero where it took no slot.
	arrived time.Time

	// reader for the current message (can be nil)
	r io.Reader
}

// Arrived is when the socket under this connection was accepted, so the
// server times its CONNECT from there rather than from the upgrade; zero
// where the listener took no slot for it.
func (ws *wsConn) Arrived() time.Time { return ws.arrived }

// ConnectionState is the TLS state of the connection the WebSocket runs
// over, and the zero state for one that carries no TLS: what the broker
// reads a verified client certificate from, as it does on the tcp door.
func (ws *wsConn) ConnectionState() tls.ConnectionState {
	if tc, ok := ws.Conn.(*tls.Conn); ok {
		return tc.ConnectionState()
	}
	return tls.ConnectionState{}
}

// SetWriteDeadline sets the deadline on the websocket connection rather
// than on the socket underneath it.
//
// The embedded net.Conn's method would be promoted here and would appear
// to work, and does not: gorilla calls conn.SetWriteDeadline with its own
// stored deadline immediately before every frame it writes, so a deadline
// set on the socket is overwritten - with the zero value, meaning none -
// before the write it was meant to bound. A caller that sets a deadline on
// this net.Conn and expects the write to end has no way to tell.
func (ws *wsConn) SetWriteDeadline(t time.Time) error {
	return ws.c.SetWriteDeadline(t)
}

// Read reads the next span of bytes from the websocket connection and returns the number of bytes read.
func (ws *wsConn) Read(p []byte) (int, error) {
	if ws.r == nil {
		op, r, err := ws.c.NextReader()
		if err != nil {
			return 0, err
		}

		if op != websocket.BinaryMessage {
			err = ErrInvalidMessage
			return 0, err
		}

		ws.r = r
	}

	var n int
	for {
		// buffer is full, return what we've read so far
		if n == len(p) {
			return n, nil
		}

		br, err := ws.r.Read(p[n:])
		n += br
		if err != nil {
			// when ANY error occurs, we consider this the end of the current message (either because it really is, via
			// io.EOF, or because something bad happened, in which case we want to drop the remainder)
			ws.r = nil

			if errors.Is(err, io.EOF) {
				err = nil
			}
			return n, err
		}
	}
}

// Write writes bytes to the websocket connection.
func (ws *wsConn) Write(p []byte) (int, error) {
	err := ws.c.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

// Close signals the underlying websocket conn to close.
func (ws *wsConn) Close() error {
	return ws.Conn.Close()
}
