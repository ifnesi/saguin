package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ifnesi/saguin/internal/config"
)

// websocketStream only adapts binary messages to MQTT's byte stream. The
// reader owns message; the writer owns MQTT writes and write deadlines.
// Gorilla's own control replies run in its reading calls under its one-second
// limit. CloseWrite is inherited from the transport: no WebSocket Close frame.
type websocketStream struct {
	socket
	conn        *websocket.Conn
	message     io.Reader
	writeSocket *tlsSocket // shared transport deadline cap, also used without TLS
}

var errWebSocketText = errors.New("MQTT over WebSocket requires binary messages")

// newWebSocketStream gives listeners and bridges the same binary framing
// and transport close. A peer's Close ends input without a Close response;
// MQTT output and the orderly transport shutdown belong to the writer.
func newWebSocketStream(raw socket, ws *websocket.Conn, writeSocket *tlsSocket) *websocketStream {
	ws.SetCloseHandler(func(int, string) error { return nil })
	return &websocketStream{socket: raw, conn: ws, writeSocket: writeSocket}
}

// dialWebSocket upgrades the bridge's existing socket on its reader. TLS
// has already completed under the attempt's deadline. Supplying both dial
// callbacks keeps Gorilla on this socket, without another dial or handshake.
func dialWebSocket(raw socket, target string, deadline time.Time, established <-chan struct{}) (*websocketStream, error) {
	var writeSocket *tlsSocket
	transport := raw.(net.Conn)
	if secure, ok := raw.(*tls.Conn); ok {
		writeSocket = secure.NetConn().(*tlsSocket)
	} else {
		writeSocket = &tlsSocket{Conn: transport, closing: make(chan struct{}),
			establishment: deadline, established: established}
		transport = writeSocket
	}
	// Limit plaintext HTTP bytes above TLS too. Gorilla keeps its parser
	// and buffering, while its input cannot grow beyond the header bound.
	input := &io.LimitedReader{R: transport, N: http.DefaultMaxHeaderBytes}
	bounded := &tlsSocket{Conn: transport, readLimit: input}
	dial := func(context.Context, string, string) (net.Conn, error) { return bounded, nil }
	d := websocket.Dialer{Subprotocols: []string{"mqtt"}, NetDialContext: dial, NetDialTLSContext: dial}
	ws, response, err := d.Dial(target, nil)
	if response != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		if input.N == 0 {
			return nil, errors.New("WebSocket upgrade answer exceeds 1 MiB")
		}
		return nil, err
	}
	if ws.Subprotocol() != "mqtt" {
		return nil, errors.New("WebSocket peer did not select the mqtt subprotocol")
	}
	bounded.readLimit = nil // subsequent frames have MQTT's own bounds
	return newWebSocketStream(raw, ws, writeSocket), nil
}

func (s *websocketStream) Read(p []byte) (int, error) {
	for {
		if s.message == nil {
			kind, message, err := s.conn.NextReader()
			if err != nil {
				return 0, err
			}
			if kind != websocket.BinaryMessage {
				return 0, errWebSocketText
			}
			s.message = message
		}
		n, err := s.message.Read(p)
		if errors.Is(err, io.EOF) {
			s.message = nil
			if n != 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (s *websocketStream) Write(p []byte) (int, error) {
	if err := s.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *websocketStream) SetWriteDeadline(t time.Time) error {
	return s.conn.SetWriteDeadline(t)
}

// websocketHTTPConn carries connection-local setup through net/http. Reads
// are the embedded net.Conn's own method, including net/http's hang-up probe:
// no Sagüin reading code runs on that library helper goroutine. Its deadline
// cap survives net/http and Gorilla clearing their setup deadlines. Only the
// connection goroutine changes it, before it starts the writer.
type websocketHTTPConn struct {
	net.Conn
	reader   *reader
	deadline time.Time
	setupErr error
	hijacked bool
	upgraded bool
}

func (c *websocketHTTPConn) SetDeadline(t time.Time) error {
	return errors.Join(c.SetReadDeadline(t), c.SetWriteDeadline(t))
}

func (c *websocketHTTPConn) SetReadDeadline(t time.Time) error {
	return c.Conn.SetReadDeadline(c.capDeadline(t))
}

func (c *websocketHTTPConn) SetWriteDeadline(t time.Time) error {
	return c.Conn.SetWriteDeadline(c.capDeadline(t))
}

func (c *websocketHTTPConn) capDeadline(t time.Time) time.Time {
	if !c.deadline.IsZero() && (t.IsZero() || c.deadline.Before(t)) {
		return c.deadline
	}
	return t
}

// websocketListener leaves admission on the R1 listener goroutine. A TLS
// connection stays a concrete *tls.Conn, so net/http itself handshakes on its
// connection goroutine and supplies the request's verified TLS state.
type websocketListener struct{ *Listener }

func (l websocketListener) Accept() (net.Conn, error) {
	c, r, err := l.acceptConnection()
	if c == nil {
		// A failed accept must reach main, even when net.Error is temporary;
		// net/http must not retry it with a backoff of its own.
		return nil, fmt.Errorf("MQTT listener %s accept: %w", l.name, err)
	}
	h := &websocketHTTPConn{reader: r, deadline: r.deadline, setupErr: err}
	var served net.Conn
	if secure, ok := c.raw.(*tls.Conn); ok {
		s := secure.NetConn().(*tlsSocket)
		h.Conn = s.Conn
		s.Conn = h
		served = secure
	} else {
		h.Conn = c.raw.(net.Conn)
		served = h
	}
	if err := served.SetReadDeadline(r.deadline); h.setupErr == nil {
		h.setupErr = err
	}
	l.accepted(c)
	return served, nil
}

// Serve closes its listener on return. Main owns the stop notification and
// closes the listening socket too; neither action cuts a served connection.
func (l websocketListener) Close() error { return l.socket.Close() }

type websocketContextKey struct{}

func httpConnection(raw net.Conn) *websocketHTTPConn {
	if secure, ok := raw.(*tls.Conn); ok {
		return secure.NetConn().(*tlsSocket).Conn.(*websocketHTTPConn)
	}
	return raw.(*websocketHTTPConn)
}

func (l *Listener) serveWebSockets(cfg *config.Listener) error {
	policy, err := cfg.OriginPolicy()
	if err != nil {
		return err
	}
	l.upgrader = &websocket.Upgrader{
		Subprotocols: []string{"mqtt"},
		CheckOrigin: func(r *http.Request) bool {
			same, origins := policy.SameOrigin, policy.Allowed
			if l.live != nil {
				policy := l.live.Origins(l.name)
				same, origins = policy.SameOrigin, policy.Allowed
			}
			if acceptsOrigin(r, same, origins) {
				return true
			}
			h := r.Context().Value(websocketContextKey{}).(*websocketHTTPConn)
			h.reader.failure = fmt.Errorf("MQTT listener %s: WebSocket origin refused: %q", l.name, r.Header.Values("Origin"))
			return false
		},
	}
	l.server = &http.Server{
		Handler:                      l,
		TLSNextProto:                 map[string]func(*http.Server, *tls.Conn, http.Handler){},
		DisableGeneralOptionsHandler: true,
		ErrorLog:                     log.New(io.Discard, "", 0), // failures use the existing core handover
		ConnContext: func(ctx context.Context, raw net.Conn) context.Context {
			return context.WithValue(ctx, websocketContextKey{}, httpConnection(raw))
		},
		ConnState: l.webSocketState,
	}
	l.server.SetKeepAlivesEnabled(false)
	return nil
}

func acceptsOrigin(r *http.Request, same bool, allowed []*url.URL) bool {
	values := r.Header.Values("Origin")
	if len(values) == 0 || (!same && len(allowed) == 0) {
		return true
	}
	if len(values) != 1 {
		return false
	}
	origin, err := config.ParseOrigin(values[0])
	if err != nil {
		return false
	}
	if same {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		request, err := config.ParseOrigin(scheme + "://" + r.Host)
		if err == nil && equalOrigin(origin, request) {
			return true
		}
	}
	for _, permitted := range allowed {
		if equalOrigin(origin, permitted) {
			return true
		}
	}
	return false
}

func equalOrigin(a, b *url.URL) bool {
	return a.Scheme == b.Scheme && strings.EqualFold(a.Hostname(), b.Hostname()) && originPort(a) == originPort(b)
}

func originPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "https" {
		return "443"
	}
	return "80"
}

func (l *Listener) webSocketState(raw net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		l.workers.Add(1)
		l.metrics.readers.Add(1)
	case http.StateHijacked:
		httpConnection(raw).hijacked = true
	case http.StateClosed:
		l.finishWebSocket(httpConnection(raw))
	}
}

// finishWebSocket runs on the HTTP connection's reader. Before a successful
// upgrade there is no writer goroutine: the same owner reports its end and
// completes the ordinary close bookkeeping synchronously. An invalid upgrade
// has no MQTT closure cause; only deadline and TLS failures are counted.
func (l *Listener) finishWebSocket(h *websocketHTTPConn) {
	defer l.workers.Done()
	defer l.metrics.readers.Add(-1)
	if h.upgraded {
		return
	}
	r := h.reader
	switch {
	case !r.conn.clock.Now().Before(r.deadline):
		r.failed(os.ErrDeadlineExceeded)
	case h.setupErr != nil:
		r.failed(h.setupErr)
	default:
		if secure, ok := r.conn.raw.(*tls.Conn); ok && !secure.ConnectionState().HandshakeComplete {
			// net/http has already tried the handshake. TLS keeps its error;
			// reuse the reader's classification and logging, without another I/O.
			r.handshake(secure)
		} else {
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
		}
	}
	l.writer(r.conn).run()
}

// ServeHTTP is net/http's connection goroutine, the WebSocket's R1 reader.
// Only a successful upgrade starts a writer; MQTT reading stays here.
func (l *Listener) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	h := request.Context().Value(websocketContextKey{}).(*websocketHTTPConn)
	defer func() {
		if h.hijacked {
			l.finishWebSocket(h)
		}
	}()
	ws, err := l.upgrader.Upgrade(w, request, nil)
	if err != nil {
		return // Gorilla owns the HTTP response; origin failures were recorded by CheckOrigin
	}
	r := h.reader
	h.upgraded = true
	h.deadline = time.Time{} // HTTP no longer controls the stream's deadlines
	var writeSocket *tlsSocket
	if secure, ok := r.conn.raw.(*tls.Conn); ok {
		writeSocket = secure.NetConn().(*tlsSocket)
	} else {
		// Reuse the transport's deadline cap for plain WebSocket control
		// writes too. Hijacking has ended net/http's reads, and no writer
		// has started, so this layer is installed before either MQTT owner.
		writeSocket = &tlsSocket{Conn: h.Conn, closing: make(chan struct{})}
		h.Conn = writeSocket
	}
	r.conn.stream = newWebSocketStream(r.conn.raw, ws, writeSocket)
	if request.TLS != nil && len(request.TLS.VerifiedChains) != 0 {
		r.certificate = request.TLS.VerifiedChains[0][0]
	}
	err = r.conn.raw.SetReadDeadline(r.deadline)
	l.workers.Add(1)
	go l.write(r.conn)
	if err != nil {
		r.failed(err)
	} else {
		r.readPackets()
	}
}
