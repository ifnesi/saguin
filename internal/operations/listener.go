package operations

import (
	"fmt"
	"net"
	"time"
)

// limitedListener takes a permit before accept, leaving excess connections
// in the kernel's queue. Closing wakes a listener waiting for a permit.
type limitedListener struct {
	net.Listener
	permits chan struct{}
	stop    chan struct{}
	close   chan struct{}
	tls     bool
	proxy   bool
}

func newLimitedListener(raw net.Listener) *limitedListener {
	l := &limitedListener{Listener: raw, permits: make(chan struct{}, connectionLimit), stop: make(chan struct{}), close: make(chan struct{}, 1)}
	l.close <- struct{}{}
	for range connectionLimit {
		l.permits <- struct{}{}
	}
	return l
}

func (l *limitedListener) Accept() (net.Conn, error) {
	select {
	case <-l.stop:
		return nil, net.ErrClosed
	case <-l.permits:
	}
	raw, err := l.Listener.Accept()
	if err != nil {
		l.permits <- struct{}{}
		// Serve must report even a temporary accept failure instead of retrying.
		return nil, fmt.Errorf("operations accept: %w", err)
	}
	accepted := time.Now()
	c := &limitedConn{Conn: raw, permits: l.permits, close: make(chan struct{}, 1)}
	c.close <- struct{}{}
	if l.tls {
		c.setupDeadline = accepted.Add(5 * time.Second)
		c.tlsPending = make(chan struct{})
	}
	if l.proxy {
		c.Conn = &proxyConn{Conn: raw, pending: true, setupDeadline: accepted.Add(5 * time.Second)}
	}
	return c, nil
}

func (l *limitedListener) Close() error {
	select {
	case <-l.close:
		close(l.stop)
		return l.Listener.Close()
	default:
		return nil
	}
}

// net/http may close from either the connection or main's shutdown. The
// one-slot handover closes the socket only once. The connection owner keeps
// its permit until StateClosed has finished reporting the connection's failure.
type limitedConn struct {
	net.Conn
	permits chan<- struct{}
	close   chan struct{}
	// net/http clears its handshake deadline before reading HTTP. The
	// connection owner publishes that transition for a concurrent TLS close.
	setupDeadline time.Time
	tlsPending    chan struct{}
}

func (c *limitedConn) SetReadDeadline(t time.Time) error {
	if c.tlsPending != nil {
		select {
		case <-c.tlsPending:
		default:
			if t.IsZero() {
				close(c.tlsPending)
			} else {
				t = minDeadline(t, c.setupDeadline)
			}
		}
	}
	return c.Conn.SetReadDeadline(t)
}

func (c *limitedConn) SetWriteDeadline(t time.Time) error {
	if c.tlsPending != nil {
		select {
		case <-c.tlsPending:
		default:
			t = minDeadline(t, c.setupDeadline)
		}
	}
	return c.Conn.SetWriteDeadline(t)
}

func minDeadline(t, limit time.Time) time.Time {
	if t.IsZero() || limit.Before(t) {
		return limit
	}
	return t
}

func (c *limitedConn) Close() error {
	select {
	case <-c.close:
		return c.Conn.Close()
	default:
		return nil
	}
}
