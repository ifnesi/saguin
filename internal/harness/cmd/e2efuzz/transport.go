package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// stream is a client's connection: TCP, or TLS over TCP, each ending its
// sending with CloseWrite (a TLS one with close_notify).
type stream interface {
	net.Conn
	CloseWrite() error
}

// observedConn keeps a first-byte peek separate from decoding a packet.
// A quiet interval may expire before any byte arrives; once a byte arrives,
// the harness reads the complete packet under its ordinary watchdog. A
// timeout can therefore never discard a partially read MQTT packet. On a
// TLS connection, what it records is the MQTT inside.
type observedConn struct {
	stream
	reader *bufio.Reader
	input  *readAhead
	f      *fuzzer
	number int
}

type socketInput struct{ conn *observedConn }

func (in socketInput) Read(p []byte) (int, error) {
	c := in.conn
	if c.input != nil {
		return c.input.Read(p)
	}
	n, err := c.stream.Read(p)
	c.record("read", p[:n], err)
	return n, err
}

// readAhead holds incoming bytes in wire order until the driver's packet
// reader asks for them. Only this buffer is shared; decoding and every
// model check still belong to the caller of slot.read, at its usual step.
type readAhead struct {
	mu       sync.Mutex
	bytes    bytes.Buffer
	err      error
	deadline time.Time
	changed  chan struct{}
	done     chan struct{}
}

func (r *readAhead) wake() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *readAhead) Read(p []byte) (int, error) {
	for {
		r.mu.Lock()
		if r.bytes.Len() > 0 {
			n, err := r.bytes.Read(p)
			r.mu.Unlock()
			return n, err
		}
		err, deadline := r.err, r.deadline
		r.mu.Unlock()
		if err != nil {
			return 0, err
		}
		var timer *time.Timer
		var expired <-chan time.Time
		if !deadline.IsZero() {
			if !time.Now().Before(deadline) {
				return 0, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(time.Until(deadline))
			expired = timer.C
		}
		select {
		case <-r.changed:
		case <-expired:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// serve keeps a connected client's socket draining through every phase,
// including another client's blocked write or read. Connections deliberately
// held without a CONNECT keep their direct reader. Buffered bytes and the
// terminal error are returned in order, with the caller's receive deadline;
// that deadline never stops the socket reader behind unrelated work.
func (c *observedConn) serve() error {
	if c.input != nil {
		return nil
	}
	if err := c.stream.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	r := &readAhead{changed: make(chan struct{}, 1), done: make(chan struct{})}
	c.input = r
	go func() {
		defer close(r.done)
		buf := make([]byte, 32<<10)
		for {
			n, err := c.stream.Read(buf)
			c.record("read", buf[:n], err)
			r.mu.Lock()
			_, _ = r.bytes.Write(buf[:n]) // bytes.Buffer.Write never fails
			r.err = err
			r.mu.Unlock()
			r.wake()
			if err != nil {
				return
			}
		}
	}()
	return nil
}

func (c *observedConn) SetReadDeadline(deadline time.Time) error {
	if c.input == nil {
		return c.stream.SetReadDeadline(deadline)
	}
	r := c.input
	r.mu.Lock()
	r.deadline = deadline
	r.mu.Unlock()
	r.wake()
	return nil
}

func (c *observedConn) SetDeadline(deadline time.Time) error {
	if err := c.SetReadDeadline(deadline); err != nil {
		return err
	}
	return c.SetWriteDeadline(deadline)
}

func (c *observedConn) Close() error {
	err := c.stream.Close()
	if c.input != nil {
		<-c.input.done
	}
	return err
}

func (c *observedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (c *observedConn) Write(p []byte) (int, error) {
	n, err := c.stream.Write(p)
	c.record("sent", p[:n], err)
	return n, err
}

func (c *observedConn) record(direction string, p []byte, err error) {
	if c.f.trace != nil {
		c.f.trace.Printf("step %d conn %d %s %x error=%v", c.f.wireStep.Load(), c.number, direction, p, err)
	}
}

// ready waits for the first byte only, retaining it in reader for Recv.
func (c *observedConn) ready(deadline time.Time) (bool, error) {
	if err := c.SetReadDeadline(deadline); err != nil {
		return false, err
	}
	_, err := c.reader.Peek(1)
	if harness.IsTimeout(err) {
		return false, nil
	}
	return err == nil, err
}

// quietInterval defines a receive action's endpoint. Broker scheduling can
// change which packets precede it, but never the driver's subsequent draws.
const quietInterval = 2 * time.Millisecond

// drain reads deliveries until the broker is quiet for quietInterval. No
// clock bounds it: a retained pass "has no allowance of its own" and
// shares the in-flight window (design.md, Delivery, Preparation), so it
// lasts as long as the run's retained values take to cross that window,
// which grows with the run. A broker that stalls inside a packet still
// fails, each packet being read within -timeout, and so does one that never
// stops sending, past the deliveries it can owe (read).
func (s *slot) drain() error {
	c := s.conn.Conn.(*observedConn)
	for {
		ready, err := c.ready(time.Now().Add(quietInterval))
		if err != nil || !ready {
			return err
		}
		p, err := s.read()
		if err != nil {
			return err
		}
		if received(p) {
			continue
		}
		if settled, err := s.settle(p); err != nil {
			return err
		} else if !settled {
			return fmt.Errorf("%s: %s while draining deliveries", s.id, show(p))
		}
	}
}

func newWireLogger(w io.Writer) *log.Logger { return log.New(w, "", log.Lmicroseconds) }
