package operations

import (
	"errors"
	"io"
	"math"
	"net"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
)

// proxyConn reads the header on net/http's first, synchronous connection
// read. That completes before net/http starts any request hang-up read.
// After setup, Read only forwards bytes; all header fields are immutable.
type proxyConn struct {
	net.Conn
	pending        bool
	setupDeadline  time.Time
	header         connection.ProxyHeader
	failure        error
	headerDeadline time.Time
	requestShift   time.Duration
}

func (c *proxyConn) Read(p []byte) (int, error) {
	if c.pending {
		c.pending = false
		if err := c.Conn.SetReadDeadline(c.setupDeadline); err != nil {
			c.failure = err
			return c.abort(err)
		}
		// The v2 length is uint16 and excludes its 16-byte fixed header.
		input := io.LimitedReader{R: c.Conn, N: math.MaxUint16 + 16}
		header, err := connection.ReadProxyHeader(&input)
		if err != nil {
			quiet := input.N == math.MaxUint16+16 && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET))
			if !quiet && !errors.Is(err, net.ErrClosed) {
				c.failure = err
			}
			return c.abort(err)
		}
		if header.Local {
			return c.abort(io.EOF)
		}
		c.header = header
		c.requestShift = time.Now().Add(headerTimeout).Sub(c.headerDeadline)
		// readRequest fixed its first deadlines before entering Read. Move
		// those by setup's duration, giving HTTP its own full budgets.
		if err := c.Conn.SetReadDeadline(c.headerDeadline.Add(c.requestShift)); err != nil {
			return c.abort(err)
		}
	}
	return c.Conn.Read(p)
}

// An invalid header ends the transport before HTTP sees any input or can
// answer it as a malformed HTTP request. The outer connection returns its
// permit when its owner finishes reporting the failure in StateClosed.
func (c *proxyConn) abort(err error) (int, error) {
	_ = c.Close()
	return 0, err
}

func (c *proxyConn) SetReadDeadline(t time.Time) error {
	if c.pending {
		c.headerDeadline = t
	} else if !t.IsZero() {
		t = t.Add(c.requestShift)
	}
	return c.Conn.SetReadDeadline(t)
}
