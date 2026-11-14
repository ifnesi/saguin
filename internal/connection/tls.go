package connection

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"
)

// tlsSocket shares transport bounds between TLS and WebSocket. An optional
// read limit bounds a bridge's HTTP upgrade before its MQTT stream begins.
// Until the reader publishes CONNACK, establishment caps every write deadline.
// The writer publishes closeDeadline once by closing closing; control writes
// and TLS's close_notify cannot extend that cutoff either.
type tlsSocket struct {
	net.Conn
	readLimit     *io.LimitedReader // owned by the reader; nil after the upgrade
	establishment time.Time         // immutable bridge attempt cutoff
	established   <-chan struct{}   // reader closes after a successful CONNACK
	closeDeadline time.Time
	closing       chan struct{}
}

func (s *tlsSocket) Read(p []byte) (int, error) {
	if s.readLimit != nil {
		return s.readLimit.Read(p)
	}
	return s.Conn.Read(p)
}

// SetDeadline must also pass through the write cap when a transport clears
// its setup deadlines, as Gorilla does after a successful upgrade.
func (s *tlsSocket) SetDeadline(t time.Time) error {
	return errors.Join(s.SetReadDeadline(t), s.SetWriteDeadline(t))
}

func (s *tlsSocket) SetWriteDeadline(t time.Time) error {
	if !s.establishment.IsZero() {
		select {
		case <-s.established:
		default:
			if t.IsZero() || s.establishment.Before(t) {
				t = s.establishment
			}
		}
	}
	select {
	case <-s.closing:
		if t.IsZero() || s.closeDeadline.Before(t) {
			t = s.closeDeadline
		}
	default:
	}
	if err := s.Conn.SetWriteDeadline(t); err != nil {
		return err
	}
	// Close may publish its cutoff while the transport's setter is in
	// progress. Reapply it before the caller can begin its write.
	select {
	case <-s.closing:
		if t.IsZero() || s.closeDeadline.Before(t) {
			return s.Conn.SetWriteDeadline(s.closeDeadline)
		}
	default:
	}
	return nil
}

// handshake runs on the reader, under the CONNECT or bridge CONNACK deadline.
// Handshake uses a background context, so TLS starts no cancellation worker.
func (r *reader) handshake(secure *tls.Conn) bool {
	// The listener or dialer initialized the handshake's write deadline
	// before handover; only the writer updates it from now on.
	if err := secure.Handshake(); err != nil {
		if r.conn.client {
			r.failed(err)
			return false
		}
		switch {
		case errors.Is(err, net.ErrClosed):
			// Closing our raw socket interrupts setup too. A local cut is
			// neither a peer loss nor a refused TLS handshake.
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
		case errors.Is(err, io.EOF), errors.Is(err, syscall.ECONNRESET),
			errors.Is(err, os.ErrDeadlineExceeded):
			r.failed(err)
		default:
			r.failure = fmt.Errorf("MQTT listener %s: TLS handshake: %w", r.conn.listener, err)
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{tlsHandshake: true})
		}
		return false
	}
	state := secure.ConnectionState()
	if !r.conn.client && len(state.VerifiedChains) != 0 {
		r.certificate = state.VerifiedChains[0][0]
	}
	return true
}
