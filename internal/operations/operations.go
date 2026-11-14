// Package operations serves the broker's bounded HTTP operations interface.
package operations

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/operations/routes"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/passwd"
)

const (
	connectionLimit = 64
	headerTimeout   = 5 * time.Second
)

var codes = [...]int{200, 400, 401, 403, 404, 405, 503}

// Metrics is shared by operations listeners and registered before serving.
type Metrics struct {
	requests [len(routes.Paths) + 1][len(codes)]*metrics.Counter
	listener *metrics.Gauge
	http     *metrics.Gauge
}

// NewMetrics registers every documented route and status pair, even those
// whose handlers or authentication are supplied by later items.
func NewMetrics(reg *metrics.Registry) *Metrics {
	m := &Metrics{listener: reg.Goroutine("listener"), http: reg.Goroutine("http_request")}
	for i := range len(routes.Paths) + 1 {
		for j, code := range codes {
			m.requests[i][j] = reg.Counter("saguin_operations_requests_total", "route", routes.Name(i), "code", strconv.Itoa(code))
		}
	}
	return m
}

// Listener owns the socket on its R1 listener goroutine. net/http starts
// the connection goroutines; the handler has no broker state.
type Listener struct {
	socket                    *limitedListener
	server                    *http.Server
	workers                   sync.WaitGroup
	reg                       *metrics.Registry
	metrics                   *Metrics
	health                    chan<- *views.Request
	config                    *config.Config // immutable settings accepted at startup
	live                      *config.Reloads
	passwordPath              string
	acl                       *acl.File // immutable startup policy; no session state
	publishRate, publishBytes int64
	tls                       *tls.Config
	passwords                 *passwd.File
	reports                   chan<- connection.Event
	name                      string
	unix                      bool
}

// Listen binds the configured transport and loads its immutable TLS settings.
// TLS and PROXY setup run on net/http's connection goroutines, never accept.
func Listen(cfg *config.Config, listener *config.Listener, reg *metrics.Registry, m *Metrics, health chan<- *views.Request, reports chan<- connection.Event) (*Listener, error) {
	secure, err := cfg.TLSConfig(listener, true)
	if err != nil {
		return nil, err
	}
	var raw net.Listener
	if listener.Path != "" {
		raw, err = connection.ListenUnix(listener.Path, listener.Mode)
	} else {
		raw, err = net.Listen("tcp", listener.Address)
	}
	if err != nil {
		return nil, err
	}
	l := &Listener{socket: newLimitedListener(raw), reg: reg, metrics: m, health: health,
		config: cfg,
		tls:    secure, passwords: cfg.Passwords[listener.PasswordFile], reports: reports, name: listener.Name, unix: listener.Path != ""}
	l.socket.tls, l.socket.proxy = secure != nil, listener.ProxyProtocol
	if secure != nil {
		secure.NextProtos = []string{"http/1.1"}
	}
	l.acl, l.live, l.passwordPath = cfg.ACL, cfg.Live, listener.PasswordFile
	l.publishRate, l.publishBytes = int64(cfg.Broker.Limits.PublishRate), int64(cfg.Broker.Limits.PublishBytes)
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	l.server = &http.Server{
		Handler: l, Protocols: &protocols,
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            headerTimeout, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second,
		// net/http's diagnostic output cannot bypass the core's log admission.
		ErrorLog:    log.New(diagnosticWriter{listener: l}, "", 0),
		ConnState:   l.connState,
		ConnContext: connectionContext,
	}
	return l, nil
}

// Run serves until Close, or returns the first accept failure to main.
func (l *Listener) Run() error {
	l.metrics.listener.Add(1)
	defer l.metrics.listener.Add(-1)
	var socket net.Listener = l.socket
	if l.tls != nil {
		socket = tls.NewListener(socket, l.tls)
	}
	err := l.server.Serve(socket)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close ends acceptance and open sockets. Wait follows Run's completion,
// so no further connection can be added to the wait group.
func (l *Listener) Close() error { return l.server.Close() }

// Wait joins all HTTP connection owners before main ends the core.
func (l *Listener) Wait() { l.workers.Wait() }

// Addr returns the operations listener's bound address for the startup log.
func (l *Listener) Addr() net.Addr { return l.socket.Addr() }

func (l *Listener) connState(conn net.Conn, state http.ConnState) {
	if secure, ok := conn.(*tls.Conn); ok {
		conn = secure.NetConn()
	}
	limited := conn.(*limitedConn)
	proxy, _ := limited.Conn.(*proxyConn)
	switch state {
	case http.StateNew:
		l.workers.Add(1)
		l.metrics.http.Add(1)
	case http.StateActive:
		if proxy != nil {
			proxy.requestShift = 0 // readRequest has installed the body's deadline
		}
	case http.StateClosed:
		if proxy != nil && proxy.failure != nil {
			l.report(fmt.Errorf("PROXY header: %w", proxy.failure))
		}
		l.metrics.http.Add(-1)
		limited.permits <- struct{}{}
		l.workers.Done()
	}
}

type proxyContextKey struct{}

func connectionContext(ctx context.Context, conn net.Conn) context.Context {
	if limited, ok := conn.(*limitedConn); ok {
		if proxy, ok := limited.Conn.(*proxyConn); ok {
			return context.WithValue(ctx, proxyContextKey{}, proxy)
		}
	}
	return ctx
}

// diagnosticWriter hands net/http's diagnostic to the same bounded inbound
// channel as transport reports. Only the core admits runtime log lines.
type diagnosticWriter struct{ listener *Listener }

func (w diagnosticWriter) Write(p []byte) (int, error) {
	w.listener.report(errors.New(strings.TrimSpace(string(p))))
	return len(p), nil
}

func (l *Listener) report(err error) {
	l.reports <- connection.Event{Kind: connection.Ended, Packet: connection.ReadFailure{
		Err: fmt.Errorf("operations listener %s: %w", l.name, err),
	}}
}

func (l *Listener) count(route, code int) {
	for i, status := range codes {
		if status == code {
			l.metrics.requests[route][i].Inc()
			return
		}
	}
}

// ServeHTTP runs on net/http's connection goroutine. Body draining is
// synchronous under ReadTimeout; no helper goroutine or timer callback runs.
func (l *Listener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	access, passwords := l.acl, l.passwords
	if l.live != nil {
		snapshot := l.live.Access()
		access, passwords = snapshot.ACL, snapshot.Passwords[l.passwordPath]
	}
	route := routes.Index(r.URL.Path)
	code := http.StatusOK
	user := ""
	defer func() { l.count(route, code) }()
	if route == len(routes.Paths) {
		code = http.StatusNotFound
	} else if code, user = l.authorize(r, passwords); code != http.StatusOK {
		if code == http.StatusUnauthorized {
			l.report(fmt.Errorf("credential refused: route %q remote %q", r.URL.Path, r.RemoteAddr))
			w.Header().Set("WWW-Authenticate", `Basic realm="saguin"`)
		} else {
			l.report(fmt.Errorf("credential not allowed: user %q route %q remote %q", user, r.URL.Path, r.RemoteAddr))
		}
	} else if r.Method != http.MethodGet && (route != routes.Health || r.Method != http.MethodHead) {
		code = http.StatusMethodNotAllowed
	} else if _, err := io.Copy(io.Discard, r.Body); err != nil {
		code = http.StatusBadRequest
	}
	if code != http.StatusOK {
		http.Error(w, http.StatusText(code), code)
		return
	}
	if route == routes.Metrics {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = l.reg.WriteTo(w)
		return
	}
	if route == routes.ACL {
		query := r.URL.Query()
		user := query.Get("user")
		if user == "" || len(user) > 1024 {
			code = http.StatusBadRequest
			http.Error(w, "user is required and must be at most 1,024 bytes", code)
			return
		}
		var id *string
		if query.Has("client_id") {
			client := query.Get("client_id")
			id = &client
		}
		w.Header().Set("Content-Type", "application/json")
		if access == nil {
			_ = json.NewEncoder(w).Encode(acl.EverythingAllowed{EverythingAllowed: true})
		} else {
			_ = json.NewEncoder(w).Encode(access.Explain(user, id, l.publishRate, l.publishBytes))
		}
		return
	}
	if route >= routes.Config {
		body, status := l.stateView(r)
		code = status
		if code != http.StatusOK && code != http.StatusServiceUnavailable {
			http.Error(w, http.StatusText(code), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
		return
	}
	ready := l.healthy(r)
	status := "ok"
	if !ready {
		code, status = http.StatusServiceUnavailable, "unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, `{"status":"`+status+`"}`)
	}
}

func (l *Listener) healthy(r *http.Request) bool {
	return l.askCore(r, &views.Request{Route: routes.Health}).Code == http.StatusOK
}
