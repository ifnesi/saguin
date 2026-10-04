package listeners

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Admission is the server's side of a socket arriving: the
// max_connections slot it holds, and how long it may take to send its
// CONNECT. Every MQTT door is handed it (SetAdmission) and asks it as it
// accepts each socket, so that what the broker holds is decided before a
// socket is handed to anything (Admit).
type Admission interface {
	// Arrive admits a socket that arrived at arrived: it holds a
	// max_connections slot, or a place in the overflow budget (wait), or
	// neither, and is then closed at once with nothing read or written.
	Arrive(arrived time.Time) (held bool, wait SlotWait)
	// ReleaseSlot gives a slot back.
	ReleaseSlot()
	// ConnectTimeout is limits.connect_timeout, 0 for none.
	ConnectTimeout() time.Duration
	// SocketRefused says a socket on listener was closed for want of a
	// slot before it was a connection, so that it is counted where every
	// refused connection is.
	SocketRefused(listener string)
}

// SlotWait is a socket's place in the overflow budget: waiting for a slot,
// then, where it gets none, being refused.
type SlotWait interface {
	// Await waits for a slot, until the wait ends, and reports whether the
	// socket holds one.
	Await() bool
	// Until is when a socket that gets no slot is closed by.
	Until() time.Time
	// Done gives the place back as the socket closes.
	Done()
}

// Admitted is a socket a door admitted (AdmittedOf).
type Admitted interface {
	// AwaitSlot waits for the slot the socket queued for, once, and reports
	// whether it holds one.
	AwaitSlot() bool
	// ReleaseSlot gives the slot back, once.
	ReleaseSlot()
	// Until is when a socket that got no slot is closed by.
	Until() time.Time
	// Arrived is when the door accepted it.
	Arrived() time.Time
}

// Admit wraps ln so that every socket it accepts is admitted before it is
// handed over (Admission.Arrive), and one that gets neither a slot nor a
// place in the overflow budget is closed at once. answers says the door's
// sockets are read by the engine, which answers one refused 0x89 from its
// CONNECT; a ws door's are closed with nothing written instead.
func Admit(ln net.Listener, a Admission, id string, answers bool) net.Listener {
	return &admittedListener{Listener: ln, admit: a, id: id, answers: answers}
}

// admittedListener is a door's listener under Admit.
type admittedListener struct {
	net.Listener
	admit   Admission
	id      string
	answers bool
}

func (a *admittedListener) Accept() (net.Conn, error) {
	for {
		c, err := a.Listener.Accept()
		if err != nil {
			return nil, err
		}
		// Its arrival, before it waits for a slot: the wait counts against
		// connect_timeout.
		now := time.Now()
		held, wait := a.admit.Arrive(now)
		if !held && wait == nil {
			// Closed with nothing read or written, and **counted**: a fleet
			// at max_connections is the reconnect loop RFC 0005's
			// saguin_connections_refused_total is there to show.
			_ = c.Close()
			a.admit.SocketRefused(a.id)
			continue
		}
		// **A socket waiting for a slot waits on its own goroutine**, at
		// the server's first look at it or its first read (admitted), not
		// here: this loop accepts every socket on the door.
		ac := &admittedConn{Conn: c, door: a, arrived: now.UnixNano(), wait: wait}
		if !a.answers {
			// connect_timeout over everything a ws socket does before its
			// CONNECT (Websocket.SetAdmission).
			if d := a.admit.ConnectTimeout(); d > 0 {
				ac.until.Store(now.Add(d).UnixNano())
				_ = c.SetDeadline(now.Add(d))
			}
		}
		return ac, nil
	}
}

// admittedConn is a socket a door admitted: holding a slot, which it gives
// back once - as the server decides its end, or as it closes, whoever closes
// it - or a place in the overflow budget, which it gives back as it closes.
//
// **While its deadlines are bounded, every deadline set on it is held to
// that bound**: on a ws door one connect_timeout from its arrival until the
// upgrade, and on any door, once it has got no slot, the overflow bound
// (Until). net/http and gorilla set their own - for the TLS handshake, for
// the headers, for a body, and zero, meaning none, between them - and each
// is clamped to `until` rather than refused, so whichever of theirs is
// shorter still applies. The ws handler lifts the clamp once the upgrade is
// done (upgraded), before the server writes a byte: from there the
// connection's deadlines are the server's, and its CONNECT is read against
// the same arrival (wsConn.Arrived).
//
// **Kept small, because an away session keeps it**: the server's Client
// holds its connection for as long as the session is held, and what that
// costs is charged to the session (store.SessionSize). Its door's fields
// are reached through door, times are nanoseconds, and its flags are one
// word.
type admittedConn struct {
	net.Conn
	door    *admittedListener
	wait    SlotWait     // its place in the overflow budget; nil where it took a slot at accept
	arrived int64        // when the door accepted it, in Unix nanoseconds
	until   atomic.Int64 // the bound on its deadlines, in Unix nanoseconds; 0 for none

	admitting sync.Once     // settles the wait, once (admitted)
	flags     atomic.Uint32 // connHolds, connReleased, connSettled, connAnswering
}

// What an admittedConn's flags say.
const (
	connHolds     uint32 = 1 << iota // it holds a slot
	connReleased                     // its slot is given back, or it never had one to give
	connSettled                      // its place in the budget is given back
	connAnswering                    // net/http is answering a request it read (answering)
)

// set sets bit, and reports whether this call set it.
func (c *admittedConn) set(bit uint32) bool {
	for {
		f := c.flags.Load()
		if f&bit != 0 {
			return false
		}
		if c.flags.CompareAndSwap(f, f|bit) {
			return true
		}
	}
}

func (c *admittedConn) has(bit uint32) bool { return c.flags.Load()&bit != 0 }

// admitted waits for the slot the socket queued for, once. One that gets
// none is held to the overflow bound from then, and on a ws door counted
// as refused.
func (c *admittedConn) admitted() {
	c.admitting.Do(func() {
		if c.wait == nil || c.wait.Await() {
			c.set(connHolds)
			return
		}
		c.set(connReleased) // it holds none to give back
		u := c.wait.Until().UnixNano()
		if cur := c.until.Load(); cur != 0 && cur < u {
			u = cur
		}
		c.until.Store(u)
		_ = c.Conn.SetDeadline(time.Unix(0, u))
		if !c.door.answers {
			c.door.admit.SocketRefused(c.door.id)
		}
	})
}

// AwaitSlot waits for the slot the socket queued for, once, and reports
// whether it holds one.
func (c *admittedConn) AwaitSlot() bool {
	c.admitted()
	return c.has(connHolds)
}

// ReleaseSlot gives the slot back, once (release).
func (c *admittedConn) ReleaseSlot() { c.release() }

// Until is when a socket that got no slot is closed by.
func (c *admittedConn) Until() time.Time {
	if c.wait == nil {
		return time.Time{}
	}
	return c.wait.Until()
}

// Arrived is when the door accepted the socket.
func (c *admittedConn) Arrived() time.Time { return time.Unix(0, c.arrived) }

func (c *admittedConn) Close() error {
	c.admitting.Do(func() {
		// Closed before it was admitted: one held at accept still holds its
		// slot, to give back below; one waiting holds none.
		if c.wait == nil {
			c.set(connHolds)
		} else {
			c.set(connReleased)
		}
	})
	// Closed first: a place in the budget, or a slot no earlier ending gave
	// back, is another socket's only once this one is gone.
	err := c.Conn.Close()
	if c.wait != nil && c.set(connSettled) {
		c.wait.Done()
	}
	c.release()
	return err
}

// refused reports a socket that got no slot on a door whose sockets are not
// read to be answered: a ws socket, closed with nothing written.
func (c *admittedConn) refused() bool { return !c.has(connHolds) && !c.door.answers }

// Read reads once the socket is admitted. A ws socket that got no slot reads
// io.EOF, so net/http closes it with nothing written, as one refused at
// accept is closed; on a door that answers, it is read, for the CONNECT the
// engine answers 0x89, until its bound.
func (c *admittedConn) Read(p []byte) (int, error) {
	c.admitted()
	if c.refused() {
		return 0, io.EOF
	}
	return c.Conn.Read(p)
}

// Write gives the slot back before an answer net/http writes in place of
// the upgrade - a 400 for a request that is not one, a refused origin, a
// malformed request - since the socket ends with it (answering).
func (c *admittedConn) Write(p []byte) (int, error) {
	c.admitted()
	if c.refused() {
		return 0, io.EOF
	}
	if !c.door.answers && (c.has(connAnswering) || bytes.HasPrefix(p, plainRefusal)) {
		c.release()
	}
	return c.Conn.Write(p)
}

// answering marks or clears net/http answering a request (answering).
func (c *admittedConn) answering(on bool) {
	for {
		f := c.flags.Load()
		g := f &^ connAnswering
		if on {
			g |= connAnswering
		}
		if f == g || c.flags.CompareAndSwap(f, g) {
			return
		}
	}
}

// plainRefusal opens the answer net/http writes in plain text, under the
// TLS it never began, to an HTTP request sent to a wss door: written before
// any request is read, so answering never sees it.
var plainRefusal = []byte("HTTP/1.0 400 ")

// release gives the slot back, once, whether the close does it or the
// server does as it decides the connection's end.
func (c *admittedConn) release() {
	if c.set(connReleased) {
		c.door.admit.ReleaseSlot()
	}
}

// clamp is t held to the bound, while there is one.
func (c *admittedConn) clamp(t time.Time) time.Time {
	if u := c.until.Load(); u != 0 && (t.IsZero() || t.UnixNano() > u) {
		return time.Unix(0, u)
	}
	return t
}

func (c *admittedConn) SetDeadline(t time.Time) error { return c.Conn.SetDeadline(c.clamp(t)) }

func (c *admittedConn) SetReadDeadline(t time.Time) error {
	return c.Conn.SetReadDeadline(c.clamp(t))
}

func (c *admittedConn) SetWriteDeadline(t time.Time) error {
	return c.Conn.SetWriteDeadline(c.clamp(t))
}

// upgraded lifts the bound, and clears the write deadline the upgrade left
// at it: gorilla's reset to zero after writing its answer was clamped to
// the bound, and a session's writes would otherwise end there. The read
// deadline stays until the server sets the CONNECT's, which is the same
// moment.
func (c *admittedConn) upgraded() {
	c.until.Store(0)
	_ = c.Conn.SetWriteDeadline(time.Time{})
}

// admittedUnder is the admitted socket under c, through TLS, or nil.
func admittedUnder(c net.Conn) *admittedConn {
	if tc, ok := c.(*tls.Conn); ok {
		c = tc.NetConn()
	}
	ac, _ := c.(*admittedConn)
	return ac
}

// AdmittedOf is the socket a door admitted under c - through a ws upgrade,
// TLS, or a PROXY header - or nil for a connection no door admitted, such
// as one handed to the server directly.
func AdmittedOf(c net.Conn) Admitted {
	for c != nil {
		switch v := c.(type) {
		case *admittedConn:
			return v
		case *wsConn:
			if v.held == nil {
				return nil
			}
			return v.held
		case *tls.Conn:
			c = v.NetConn()
		case interface{ Unwrap() net.Conn }:
			c = v.Unwrap()
		default:
			return nil
		}
	}
	return nil
}
