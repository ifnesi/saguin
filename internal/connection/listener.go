package connection

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/passwd"
)

// Listener owns a listening socket and its accept-rate bucket. Closing it
// stops acceptance and closes any unserved socket; the core ends served ones.
type Listener struct {
	socket          net.Listener
	stopping        chan struct{}
	workers         sync.WaitGroup
	permits         chan struct{}
	accept          func() (socket, error)
	clock           Clock
	name            string
	limits          config.Limits
	minimum         packet.Version
	receiveMaximum  uint32
	retainedMaximum int64
	routing         *config.Routing
	passwords       *passwd.File // immutable file selected for this listener
	live            *config.Reloads
	passwordPath    string
	acl             *acl.File
	requireUser     bool
	proxyProtocol   bool
	inbound         chan<- Event
	metrics         *Metrics
	server          *http.Server
	upgrader        *websocket.Upgrader
	rate            float64
	burst           float64
	tokens          float64
	last            time.Time
}

// Listen binds the resolved TCP address or Unix path without accepting clients.
// All MQTT listeners share the permit channel main created at startup.
func Listen(cfg *config.Config, l *config.Listener, inbound chan<- Event, permits chan struct{}, m *Metrics) (*Listener, error) {
	return listen(cfg, l, inbound, permits, m, false)
}

// ListenWebSocket binds a WebSocket listener, using the same admission and
// connection owners as TCP. net/http supplies its reader goroutines.
func ListenWebSocket(cfg *config.Config, l *config.Listener, inbound chan<- Event, permits chan struct{}, m *Metrics) (*Listener, error) {
	return listen(cfg, l, inbound, permits, m, true)
}

func listen(cfg *config.Config, l *config.Listener, inbound chan<- Event, permits chan struct{}, m *Metrics, ws bool) (*Listener, error) {
	passwords := cfg.Passwords[l.PasswordFile]
	if l.PasswordFile != "" && passwords == nil {
		return nil, fmt.Errorf("password file %s has not been loaded", l.PasswordFile)
	}
	secure, err := cfg.TLSConfig(l, false)
	if err != nil {
		return nil, err
	}
	var bound net.Listener
	var accept func() (socket, error)
	if l.Path != "" {
		unix, err := ListenUnix(l.Path, l.Mode)
		if err != nil {
			return nil, err
		}
		bound = unix
		accept = func() (socket, error) { return unix.AcceptUnix() }
	} else {
		addr, err := net.ResolveTCPAddr("tcp", l.Address)
		if err != nil {
			return nil, err
		}
		tcp, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		bound = tcp
		accept = func() (socket, error) { return tcp.AcceptTCP() }
		if secure != nil {
			accept = func() (socket, error) {
				raw, err := tcp.AcceptTCP()
				if err != nil {
					return nil, err
				}
				// Server only constructs the transport. The reader handshakes
				// after the service permit starts CONNECT's deadline (R1).
				return tls.Server(&tlsSocket{Conn: raw, closing: make(chan struct{})}, secure), nil
			}
		}
	}
	listener := newListener(l.Name, accept,
		RealClock{}, cfg, inbound, permits, m)
	listener.socket = bound
	listener.passwords = passwords
	listener.passwordPath = l.PasswordFile
	listener.proxyProtocol = l.ProxyProtocol
	// Without an identity source, anonymous access is the only valid default.
	// Configuration validation rejects an explicit false in that case.
	listener.requireUser = l.HasIdentity() && !l.AllowAnonymous
	if ws {
		if secure != nil {
			secure.NextProtos = []string{"http/1.1"}
		}
		if err := listener.serveWebSockets(l); err != nil {
			_ = bound.Close()
			return nil, err
		}
	}
	return listener, nil
}

// newListener shares the production accept loop with scripted transports.
// It fixes the listener's limits from the resolved configuration before Run.
// Run starts TCP and Unix workers; net/http starts WebSocket readers (R1).
func newListener(name string, accept func() (socket, error), clk Clock, cfg *config.Config,
	inbound chan<- Event, permits chan struct{}, m *Metrics) *Listener {
	return &Listener{
		stopping: make(chan struct{}), permits: permits,
		accept: accept, clock: clk, name: name,
		limits: cfg.Broker.Limits, minimum: cfg.Broker.MQTT.MinProtocolVersion,
		receiveMaximum:  uint32(cfg.Broker.QoS2.MaxInflightPerClient),
		retainedMaximum: int64(cfg.Broker.Retained.MaxBytes),
		routing:         config.NewRouting(cfg.Channels),
		acl:             cfg.ACL, live: cfg.Live,
		inbound: inbound, metrics: m,
	}
}

// Addr returns the bound address, including the chosen port for port zero.
func (l *Listener) Addr() net.Addr { return l.socket.Addr() }

// Close stops accepting and wakes a permit wait to close its unserved socket.
// Served sockets belong to their connections.
func (l *Listener) Close() error {
	close(l.stopping)
	err := l.socket.Close()
	// Serve also closes its listener on an accept failure. The requested
	// stop is already complete in that case; main reports the accept error.
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Run accepts on the listener's own goroutine. TCP and Unix start a reader
// and writer; net/http starts WebSocket readers, which start their writers
// after upgrading (R1). Accepted precedes each connection's reading.
// It returns nil after Close; any other accept error is returned so main can
// stop the broker and log it after the core ends (design.md, Restart).
func (l *Listener) Run() error {
	l.metrics.listeners.Add(1)
	defer l.metrics.listeners.Add(-1)
	l.rate = float64(l.limits.MaxConnectRate)
	l.burst = 2 * l.rate
	l.tokens = l.burst
	if l.rate != 0 {
		l.last = l.clock.Now()
	}
	if l.server != nil {
		err := l.server.Serve(websocketListener{l})
		return l.acceptError(err)
	}
	for {
		c, r, setupErr := l.acceptConnection()
		if c == nil {
			return l.acceptError(setupErr)
		}
		l.accepted(c)
		l.workers.Add(2)
		go r.serve(setupErr, &l.workers)
		go l.write(c)
	}
}

func (l *Listener) acceptError(err error) error {
	select {
	case <-l.stopping:
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
	default:
	}
	return err
}

// acceptConnection is shared by the socket loop and net/http's Accept.
// It holds at most one unserved socket and fixes every setup budget at service.
func (l *Listener) acceptConnection() (*Conn, *reader, error) {
	for {
		select {
		case <-l.stopping:
			return nil, nil, net.ErrClosed
		default:
		}
		if l.rate != 0 {
			now := l.clock.Now()
			l.tokens = math.Min(l.burst, l.tokens+now.Sub(l.last).Seconds()*l.rate)
			l.last = now
			if l.tokens < 1 {
				// Close interrupts both the rate wait and the permit wait.
				wait := time.Duration(math.Ceil((1 - l.tokens) / l.rate * float64(time.Second)))
				select {
				case <-l.stopping:
					return nil, nil, net.ErrClosed
				case <-l.clock.At(now.Add(wait)):
				}
				continue
			}
			l.tokens--
		}
		raw, err := l.accept()
		if err != nil {
			return nil, nil, err
		}
		l.metrics.accepted.Inc()
		// Hold at most one socket unserved while capacity is exhausted.
		// Idle listeners hold no permit; Close ends this wait.
		select {
		case <-l.stopping:
			_ = raw.Close()
			return nil, nil, net.ErrClosed
		case <-l.permits:
		}
		// CONNECT's budget includes transport setup and the Accepted handover.
		serving := l.clock.Now()
		connectDeadline := serving.Add(l.limits.ConnectTimeout)
		var proxyDeadline time.Time
		if l.proxyProtocol {
			proxyDeadline = serving.Add(5 * time.Second)
			if connectDeadline.Before(proxyDeadline) {
				proxyDeadline = connectDeadline
			}
		}
		// A stop wins if capacity and the stop were ready together.
		select {
		case <-l.stopping:
			l.permits <- struct{}{}
			_ = raw.Close()
			return nil, nil, net.ErrClosed
		default:
		}
		// Initialize the setup write budget before publishing the connection.
		// The writer later publishes its close cap through the TLS layer.
		var setupErr error
		if _, secure := raw.(*tls.Conn); secure || l.server != nil {
			setupErr = raw.SetWriteDeadline(connectDeadline)
		}
		l.metrics.open.Add(1)
		// Reader slots bound solicited replies. Resumed PUBRELs have no
		// reader slot, but hold preparation capacity through their report.
		c := newConn(raw, l.clock, &l.limits)
		c.permits, c.listener = l.permits, l.name
		r := &reader{conn: c, limits: l.limits, minimum: l.minimum, inbound: l.inbound, metrics: l.metrics,
			passwords: l.passwords, requireUser: l.requireUser, acl: l.acl,
			live: l.live, passwordPath: l.passwordPath, baseRate: l.limits.PublishRate, baseBytes: l.limits.PublishBytes,
			deadline: connectDeadline, proxyDeadline: proxyDeadline,
			receiveMaximum: l.receiveMaximum, retainedMaximum: l.retainedMaximum, routing: l.routing}
		return c, r, setupErr
	}
}

func (l *Listener) accepted(c *Conn) {
	l.inbound <- Event{Kind: Accepted, Conn: c, Packet: Accept{Listener: l.name, Remote: c.raw.RemoteAddr()}}
}

func (l *Listener) write(c *Conn) {
	l.writer(c).serve(&l.workers)
}

func (l *Listener) writer(c *Conn) *writer {
	return &writer{conn: c, inbound: l.inbound, timeout: l.limits.WriteTimeout, metrics: l.metrics}
}

// Wait waits for every reader and writer this listener started. Main calls
// it after Run has returned and the core has received their close reports.
func (l *Listener) Wait() { l.workers.Wait() }
