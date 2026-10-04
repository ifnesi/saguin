package proxyproto

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/listeners"
)

// HeaderTimeout is how long a connection has to deliver its PROXY header.
//
// Fixed rather than configured: the proxy writes the header the moment it
// connects, before any byte of its client's, so the only peer that takes
// longer is one that is not a proxy.
const HeaderTimeout = 5 * time.Second

// errLocal ends a connection whose header was a proxy's own health check.
var errLocal = errors.New("a PROXY LOCAL header describes no client")

// Conn is a connection a proxy described, and what it said about it.
//
// RemoteAddr is the **client's** address rather than the proxy's, so every
// log line, every connection count and every future decision that reads it
// sees who was actually on the other end. Without that a fleet behind one
// proxy is one local peer repeated.
//
// **The header is read on first use, not on accept**, as tls.Conn defers
// its handshake. Reading it inside Accept meant one peer that connected and
// said nothing stopped every connection behind it, and a deadline there
// alone only shortens each stop: a peer opening a socket every few seconds
// still keeps the door shut. Read here, it happens on the goroutine that
// serves this connection, so a silent peer holds nothing but itself, and a
// server's own shutdown reaches it like any other connection.
//
// A header that cannot be read, or that describes no client, closes the
// connection and fails every read and write on it: nothing is ever served
// on a connection a proxy did not describe.
type Conn struct {
	net.Conn
	log      *slog.Logger
	listener string // the socket it arrived on, for the refusal's log line

	once       sync.Once
	err        error
	remote     net.Addr
	commonName string

	mu           sync.Mutex
	readDeadline time.Time // what a caller last set, restored after the header
}

func newConn(c net.Conn, log *slog.Logger, listener string) *Conn {
	return &Conn{Conn: c, log: log, listener: listener}
}

// header reads the PROXY header once, and reports whether it described a
// client.
func (c *Conn) header() error {
	c.once.Do(func() {
		c.mu.Lock()
		caller := c.readDeadline
		c.mu.Unlock()
		deadline := time.Now().Add(HeaderTimeout)
		if !caller.IsZero() && caller.Before(deadline) {
			deadline = caller
		}
		_ = c.Conn.SetReadDeadline(deadline)
		h, err := Read(c.Conn)
		_ = c.Conn.SetReadDeadline(caller)

		switch {
		case err != nil:
			// The reason, never the bytes: a header is a peer's string.
			c.log.Warn("refusing a connection with no readable PROXY header",
				"socket", c.listener, "error", err)
			c.err = err
		case h.Local:
			// A proxy's own health check: it describes no client, so there
			// is nothing to serve and nothing wrong.
			c.err = errLocal
		default:
			c.remote = h.SourceAddr
			c.commonName = h.CommonName
			// Served, as a client with no certificate is, so the door's own
			// answer - a password, anonymous, or a refusal - decides. Said,
			// because a device sure of its certificate and refused a password
			// otherwise leaves nothing to say which of the two was wrong.
			if h.Unverified {
				c.log.Warn("the proxy could not verify this client's certificate, so the "+
					"name in it authenticates nothing", "socket", c.listener,
					"remote", h.SourceAddr, "verify_result", h.VerifyResult)
			}
			return
		}
		_ = c.Conn.Close()
	})
	return c.err
}

func (c *Conn) Read(b []byte) (int, error) {
	if err := c.header(); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

// Write reads the header first too, so that nothing is sent to a peer no
// proxy described - a server that speaks first would otherwise answer one.
func (c *Conn) Write(b []byte) (int, error) {
	if err := c.header(); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

// RemoteAddr is the client the proxy named. On a connection whose header
// failed it is the socket's own peer, and that connection is already closed.
func (c *Conn) RemoteAddr() net.Addr {
	if c.header() != nil || c.remote == nil {
		return c.Conn.RemoteAddr()
	}
	return c.remote
}

// CommonName is the certificate Common Name the proxy verified, or "" when
// it sent none. HAProxy sends it with `send-proxy-v2-ssl-cn`; nginx's
// stream module sends it with `proxy_protocol v2` behind
// `ssl_verify_client on`, which is the arrangement the README drove end to
// end against nginx 1.31.4. An earlier note here said nginx sent no TLVs at
// all, and believing it nearly cost a feature: the tested example in this
// repository disproves it.
func (c *Conn) CommonName() string {
	if c.header() != nil {
		return ""
	}
	return c.commonName
}

// Unwrap is the connection this one wraps, so a caller that tagged the
// original with something of its own - which listener it came from, say -
// can still find that tag once this type has wrapped it. Never the
// header's own business: nothing here waits on it or reads it.
func (c *Conn) Unwrap() net.Conn { return c.Conn }

// CommonNameOf is what a hook asks of any connection: the name a proxy
// verified, or "" for one that arrived directly.
func CommonNameOf(c net.Conn) string {
	if p, ok := c.(*Conn); ok {
		return p.CommonName()
	}
	return ""
}

// UnixSock is a Unix-socket listener that reads a PROXY v2 header from
// every connection before the broker sees a byte of MQTT.
//
// **A Unix socket and not TCP**, and that is the whole of the trust model:
// the header is the peer asserting who its client is, so anything that can
// connect can claim to be anybody. A socket's file permissions already
// decide who may speak to the broker, which is the allowlist a TCP listener
// would have to grow before this could be offered there at all.
type UnixSock struct {
	id          string
	path        string
	mode        os.FileMode
	log         *slog.Logger
	listen      net.Listener
	end         uint32
	closing     sync.Once
	connectRate int
	admit       listeners.Admission
}

// NewUnixSock returns a listener for path, expecting a PROXY v2 header.
func NewUnixSock(id, path string, mode os.FileMode) *UnixSock {
	return &UnixSock{id: id, path: path, mode: mode}
}

// SetConnectRate limits the connections accepted a second, as
// listeners.RateLimit does for the other MQTT listeners. Call it before Init.
func (l *UnixSock) SetConnectRate(perSecond int) {
	l.connectRate = perSecond
}

func (l *UnixSock) ID() string       { return l.id }
func (l *UnixSock) Address() string  { return l.path }
func (l *UnixSock) Protocol() string { return "unix" }

func (l *UnixSock) Init(log *slog.Logger) error {
	l.log = log
	// The lock, the leftover-socket rule and the permissions are
	// listeners.ListenUnix's, shared with every other Unix socket saguin opens.
	ln, err := listeners.ListenUnix(l.path, l.mode)
	if err != nil {
		return err
	}
	l.listen = listeners.RateLimit(ln, l.connectRate)
	if l.admit != nil {
		// Under the header, which is read on the connection's goroutine.
		l.listen = listeners.Admit(l.listen, l.admit, l.id, true)
	}
	return nil
}

// SetAdmission is called by the server before Init: every socket is
// admitted as it is accepted (listeners.Admit).
func (l *UnixSock) SetAdmission(a listeners.Admission) { l.admit = a }

// Serve hands every connection straight to establish, which is what
// registers it with the server's shutdown. The header is read by the first
// thing the server asks of the connection, on that connection's goroutine.
//
// **A connection whose header cannot be read is closed rather than served.**
// This listener exists because a proxy is in front of it: a connection
// arriving without a header is not a client saguin can describe, and
// serving it anyway would mean the address in every log line depended on
// whether the proxy was working.
func (l *UnixSock) Serve(establish listeners.EstablishFn) {
	for {
		if atomic.LoadUint32(&l.end) == 1 {
			return
		}
		conn, err := l.listen.Accept()
		if err != nil {
			return
		}
		if atomic.LoadUint32(&l.end) == 1 {
			_ = conn.Close()
			return
		}

		go func() {
			if err := establish(l.id, newConn(conn, l.log, l.path)); err != nil {
				l.log.Warn("connection ended with an error", "error", err)
			}
		}()
	}
}

// Listener wraps a net.Listener so that every connection carries what the
// proxy said about it, for a server that speaks net.Listener rather than
// mochi's interface - the operations endpoints, which are HTTP.
//
// **The same trust model as UnixSock above**, and for the same reason: the
// header is the peer asserting who its client is, so this belongs on a
// Unix socket whose file permissions already decide who may speak.
type Listener struct {
	net.Listener
	log *slog.Logger
}

// NewListener wraps ln so that Accept returns connections carrying the
// address and Common Name the proxy sent.
//
// **The logger is not optional.** A connection dropped here leaves nothing
// behind - the caller sees a reset and the operator sees an empty log -
// and the commonest reason to be dropped is `proxy_protocol on` where the
// broker wants v2, which is a one-word fix nobody can make without being
// told. It cost an hour of this feature's own testing.
func NewListener(ln net.Listener, log *slog.Logger) *Listener {
	return &Listener{Listener: ln, log: log}
}

// Accept returns the next connection without reading from it.
//
// net/http asks a connection its RemoteAddr on the goroutine that serves it,
// so that is where the header is read. **Nothing here may read, and nothing
// in ConnContext may either**, because both run in the loop that accepts
// every other connection.
func (l *Listener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newConn(conn, l.log, l.Listener.Addr().String()), nil
}

func (l *UnixSock) Close(closeClients listeners.CloseFn) {
	l.closing.Do(func() {
		atomic.StoreUint32(&l.end, 1)
		closeClients(l.id)
		// Closing the listener removes its own socket, under its lock.
		if l.listen != nil {
			_ = l.listen.Close()
		}
	})
}
