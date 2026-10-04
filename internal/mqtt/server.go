// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co
// SPDX-FileContributor: Italo Nesi

// Package mqtt provides a high performance, fully compliant MQTT v5 broker server with v3.1.1 backward compatibility.
package mqtt

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/mqtt/system"

	"log/slog"
)

const (
	Version        = "2.7.9" // the mochi-mqtt release this engine was forked from.
	LocalListener  = "local"
	InlineClientId = "inline"
)

var (
	// Deprecated: Use NewDefaultServerCapabilities to avoid data race issue.
	DefaultServerCapabilities = NewDefaultServerCapabilities()

	ErrListenerIDExists       = errors.New("listener id already exists")                               // a listener with the same id already exists
	ErrConnectionClosed       = errors.New("connection not open")                                      // connection is closed
	ErrInlineClientNotEnabled = errors.New("please set Options.InlineClient=true to use this feature") // inline client is not enabled by default
	ErrOptionsUnreadable      = errors.New("unable to read options from bytes")
	// ErrSessionRestored is the stop cause of a client put back from a
	// keeper at a start: it never had a connection to lose, and a session
	// with no cause at all would read as one whose ending was not recorded.
	ErrSessionRestored = errors.New("session restored from its keeper, with no client connected")
)

// Capabilities indicates the capabilities and features provided by the server.
type Capabilities struct {
	MaximumClients               int64  // maximum number of connected clients
	MaximumMessageExpiryInterval int64  // maximum message expiry if message expiry is 0 or over
	MaximumClientWritesPending   int32  // maximum number of pending message writes for a client
	MaximumSessionExpiryInterval uint32 // maximum number of seconds to keep disconnected sessions
	MaximumPacketSize            uint32 // maximum packet size, no limit if 0
	maximumPacketID              uint32 // unexported, used for testing only
	ReceiveMaximum               uint16 // maximum number of concurrent qos messages per client
	TopicAliasMaximum            uint16 // maximum topic alias value
	SharedSubAvailable           byte   // support of shared subscriptions
	MinimumProtocolVersion       byte   // minimum supported mqtt version
	MaximumQos                   byte   // maximum qos value available to clients
	RetainAvailable              byte   // support of retain messages
	WildcardSubAvailable         byte   // support of wildcard subscriptions
	SubIDAvailable               byte   // support of subscription identifiers
}

// NewDefaultServerCapabilities defines the default features and capabilities provided by the server.
func NewDefaultServerCapabilities() *Capabilities {
	return &Capabilities{
		MaximumClients:               math.MaxInt64,  // maximum number of connected clients
		MaximumMessageExpiryInterval: 60 * 60 * 24,   // maximum message expiry if message expiry is 0 or over
		MaximumClientWritesPending:   1024 * 8,       // maximum number of pending message writes for a client
		MaximumSessionExpiryInterval: math.MaxUint32, // maximum number of seconds to keep disconnected sessions
		MaximumPacketSize:            0,              // no maximum packet size
		maximumPacketID:              math.MaxUint16,
		ReceiveMaximum:               1024,           // maximum number of concurrent qos messages per client
		TopicAliasMaximum:            math.MaxUint16, // maximum topic alias value
		SharedSubAvailable:           1,              // shared subscriptions are available
		MinimumProtocolVersion:       3,              // minimum supported mqtt version (3.0.0)
		MaximumQos:                   2,              // maximum qos value available to clients
		RetainAvailable:              1,              // retain messages is available
		WildcardSubAvailable:         1,              // wildcard subscriptions are available
		SubIDAvailable:               1,              // subscription identifiers are available
	}
}

// Options contains configurable options for the server.
type Options struct {
	// Capabilities defines the server features and behaviour. If you only wish to modify
	// several of these values, set them explicitly - e.g.
	// 	server.Options.Capabilities.MaximumClientWritesPending = 16 * 1024
	Capabilities *Capabilities

	// ClientNetWriteBufferSize specifies the size of the client *bufio.Writer write buffer.
	ClientNetWriteBufferSize int

	// ClientNetWriteTimeout bounds a single write to a client's connection.
	//
	// Zero, the default, does NOT mean unbounded: the bound is then the
	// client's keepalive expiry, which is what every release before this
	// option existed gave a write by arming both deadlines at once. Set this
	// for a tighter bound than 1.5x keepalive. A client connecting with
	// keepalive 0 has no bound either way.
	//
	// **It is not the slow client this protects.** WritePacket holds the
	// client's lock across the write, so a client that has stopped reading
	// its socket holds that lock for as long as it stays connected, and
	// every write to that client waits behind it: its own write loop, and
	// saguin's own writes of a channel's records and control replies.
	//
	// A publisher is no longer among them. The packet identifier for a QoS 1
	// delivery is taken without this lock, and the delivery is handed to the
	// client's write loop rather than written where it was published, so the
	// queue that sheds a slow subscriber is reached at every QoS.
	//
	// Set, a write that does not complete in time fails with a net.Error
	// reporting Timeout, the lock is released, and the connection is left
	// for the caller to close - a timed-out write has put part of a packet
	// on the wire and that stream cannot be continued. It is net.Error
	// rather than os.ErrDeadlineExceeded because the two agree for a plain
	// TCP write and do not for every listener: a websocket writes through
	// gorilla, whose buffered writev path yields "writev tcp ...: i/o
	// timeout", which is not that sentinel.
	ClientNetWriteTimeout time.Duration

	// ClientConnectTimeout bounds how long a new connection may take to send
	// its CONNECT. Zero, the default, leaves it unbounded.
	//
	// Before CONNECT there is no keepalive to arm a read deadline from, so a
	// peer that opens a socket and sends nothing held a goroutine and a
	// descriptor for ever - outside max_connections, because it is not yet a
	// client, and inside the wait Close makes. Once the CONNECT is read the
	// keepalive deadline replaces this one.
	ClientConnectTimeout time.Duration

	// ClientMaxConnectSize bounds the whole CONNECT packet, fixed header
	// included, in bytes. Zero, the default, leaves it bounded only by the
	// Maximum Packet Size.
	//
	// A CONNECT is read before its client has authenticated, and its body is
	// allocated at the size it declares - so without a bound of its own every
	// handshake may make the server hold up to the Maximum Packet Size for a
	// stranger. It is checked from the declared length before the body is
	// read, as mosquitto checks its own CONNECT limit and HiveMQ its packet
	// size, and answered 0x95 (Packet too large) - or closed, for a 3.1.1
	// client, which has no such code.
	ClientMaxConnectSize uint32

	// ClientSessionQueueBytes bounds what one session holds of deliveries it
	// has not had acknowledged - written, withheld for want of window, or
	// queued while its client is away - measured as the memory they take.
	// Past it the oldest delivery that can be taken back is dropped; zero is
	// no bound. Half of it at most is written (sessionWireBytes).
	ClientSessionQueueBytes int64

	// ClientNetReadBufferSize specifies the size of the client *bufio.Reader read buffer.
	ClientNetReadBufferSize int

	// Logger specifies a custom configured implementation of log/slog to override
	// the servers default logger configuration. If you wish to change the log level,
	// of the default logger, you can do so by setting:
	// server := mqtt.New(nil)
	// level := new(slog.LevelVar)
	// server.Slog = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	// 	Level: level,
	// }))
	// level.Set(slog.LevelDebug)
	Logger *slog.Logger

	// Enable Inline client to allow direct subscribing and publishing from the parent codebase,
	// with negligible performance difference (disabled by default to prevent confusion in statistics).
	InlineClient bool
}

// Server is an MQTT broker server. It should be created with server.New()
// in order to ensure all the internal fields are correctly populated.
type Server struct {
	Options      *Options             // configurable server options
	Listeners    *listeners.Listeners // listeners are network interfaces which listen for new connections
	Clients      *Clients             // clients known to the broker
	Topics       *TopicsIndex         // an index of topic filter subscriptions and retained messages
	Info         *system.Info         // values about the server commonly known as $SYS topics
	loop         *loop                // loop contains tickers for the system event loop
	done         chan bool            // indicate that the server is ending
	Log          *slog.Logger         // minimal no-alloc logger
	hooks        *Hooks               // hooks contains hooks for extra functionality such as authentication and authorization
	inlineClient *Client              // inlineClient is a special client used for inline subscriptions and inline Publish
	sessions     sessionLocks         // one session establishment at a time per client id

	// ClientWaiting, where set, is told a client id when a packet of that
	// client's that waits on stored state has been read, and what it returns
	// is called once the packet has been answered or refused (saguin): while
	// it runs, the store serves that id's writes as ones a client waits on.
	// It is called with the client's locks free, and must take none of them.
	ClientWaiting func(id string) (answered func())
	// slots is atomic.Int64 rather than int64 because the atomic types
	// carry align64. A raw int64 among pointer-width fields can land on a
	// 4-byte boundary on a 32-bit platform, and a 64-bit atomic there
	// panics - for this field, on the first connection. Reordering the
	// struct would fix it today and be silently undone by the next field
	// added above it.
	slots atomic.Int64 // connections holding a max_connections slot, from arrival until their end is decided (Client.beginEnd)

	// gate is the sockets that are not admitted (arrive): the budget they
	// take, those of them waiting for a slot, oldest first, and whether the
	// server is stopping.
	gate struct {
		mu       sync.Mutex
		waiters  []*slotWaiter
		overflow int
		stopping bool
	}
}

// busyPrefix bounds each field read from a CONNECT refused because the server
// is at its connection limit: its properties and its client id. A fixed part
// of the refusal rather than configuration - it only decides whether the
// refused client can be named in the log.
const busyPrefix = 256

// reserveSlot takes one of MaximumClients slots, or reports that none is
// free. Compared and taken in one step, so connections arriving together
// cannot all see the last free slot.
func (s *Server) reserveSlot() bool {
	for {
		n := s.slots.Load()
		if n >= s.Options.Capabilities.MaximumClients {
			return false
		}
		if s.slots.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// admitGrace is the longest a connection arriving with every
// max_connections slot taken waits for one to come back (arrive). A fixed
// part of admission rather than configuration: it only decides whether a
// client that hung up and connected again at once finds the slot its last
// connection gave back, which the broker sees a scheduling delay after the
// client does.
const admitGrace = 50 * time.Millisecond

// overflowBound is the longest a socket that gets no slot is held, from
// its arrival to its close, waiting included: long enough to read the
// opening bytes of a CONNECT sent with the connection and answer 0x89.
const overflowBound = 100 * time.Millisecond

// overflowMax is the most sockets that are not admitted the broker holds at
// once - waiting for a slot, or being read to be answered 0x89 - or
// max_connections where that is fewer (overflowBudget).
const overflowMax = 32

// slotWaiter is a socket's place in the overflow budget (arrive): waiting
// for a slot until deadline, then, where it gets none, read until until to
// be answered. Its fields are the gate's, under Server.gate.mu.
type slotWaiter struct {
	s        *Server
	granted  chan struct{} // closed when a slot is handed to it
	deadline time.Time     // its wait ends
	until    time.Time     // it is closed by, where it gets no slot
	handed   bool          // a slot was handed to it
	taken    bool          // it took the slot handed to it, or passed it on
	left     bool          // it has left the queue of waiters
	done     bool          // its place in the budget is given back
}

// overflowBudget is how many sockets that are not admitted may be held at
// once: max_connections or overflowMax, whichever is fewer, and never fewer
// than one, so that a server built with no slots still answers 0x89 (a
// configuration's max_connections is at least 1).
func (s *Server) overflowBudget() int {
	n := s.Options.Capabilities.MaximumClients
	if n > overflowMax {
		n = overflowMax
	}
	if n < 1 {
		n = 1
	}
	return int(n)
}

// arrive admits a socket that arrived at arrived: a max_connections slot, a
// place in the overflow budget to wait for one in, or neither, and then it
// is closed at once with nothing read or written.
//
// **A slot given back goes to the socket that has waited longest**
// (handOn), and one that arrives while any is waiting queues behind them
// rather than taking a slot first: the count never falls while a socket
// waits, so reserveSlot cannot succeed past one.
//
// **One budget bounds every socket that is not admitted**: those waiting
// and those being read to be answered 0x89 alike, each held no longer than
// overflowBound from its arrival. So the broker holds at most
// max_connections plus the budget, and a flood past that is closed as it
// arrives rather than held. Nothing is admitted once the server is stopping.
func (s *Server) arrive(arrived time.Time) (held bool, w *slotWaiter) {
	s.gate.mu.Lock()
	defer s.gate.mu.Unlock()
	if s.gate.stopping {
		return false, nil
	}
	s.pruneWaiters(time.Now())
	if len(s.gate.waiters) == 0 && s.reserveSlot() {
		return true, nil
	}
	if s.gate.overflow >= s.overflowBudget() {
		return false, nil
	}
	w = &slotWaiter{s: s, granted: make(chan struct{}),
		deadline: arrived.Add(admitGrace), until: arrived.Add(overflowBound)}
	if ct := s.Options.ClientConnectTimeout; ct > 0 {
		if end := arrived.Add(ct); end.Before(w.until) {
			w.until = end
		}
	}
	if w.until.Before(w.deadline) {
		w.deadline = w.until
	}
	s.gate.overflow++
	s.gate.waiters = append(s.gate.waiters, w)
	return false, w
}

// pruneWaiters drops from the front of the queue the waiters whose wait has
// ended or who have left it, under the gate's lock.
func (s *Server) pruneWaiters(now time.Time) {
	for len(s.gate.waiters) > 0 {
		w := s.gate.waiters[0]
		if !w.left && now.Before(w.deadline) {
			return
		}
		w.left = true
		s.gate.waiters = s.gate.waiters[1:]
	}
}

// Await waits for the slot w queued for and reports whether it holds one:
// until a slot is handed to it, its wait ends, or the server stops.
//
// **Whether it holds one is decided under the gate's lock**, by whether a
// slot was handed to it before its wait ended (handOn), not by which of its
// timer and the hand-off it saw first: a slot handed to a waiter whose wait
// had ended would be taken by a socket already being refused.
func (w *slotWaiter) Await() bool {
	if d := time.Until(w.deadline); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-w.granted:
		case <-t.C:
		case <-w.s.done:
		}
	}
	w.s.gate.mu.Lock()
	defer w.s.gate.mu.Unlock()
	if w.handed {
		w.taken = true
		return true
	}
	w.leave()
	return false
}

// Until is when a socket that gets no slot is closed by: overflowBound
// from its arrival, or connect_timeout where that is sooner.
func (w *slotWaiter) Until() time.Time { return w.until }

// Done gives w's place in the budget back as its socket closes, and a slot
// handed to it and not taken on to the next waiter.
func (w *slotWaiter) Done() {
	w.s.gate.mu.Lock()
	defer w.s.gate.mu.Unlock()
	if w.done {
		return
	}
	w.done = true
	if w.handed {
		// Its place was given back as the slot was handed to it.
		if !w.taken {
			w.taken = true
			w.s.handOn()
		}
		return
	}
	w.leave()
	w.s.gate.overflow--
}

// leave takes w out of the queue of waiters, under the gate's lock.
func (w *slotWaiter) leave() {
	if w.left {
		return
	}
	w.left = true
	w.s.gate.waiters = slices.DeleteFunc(w.s.gate.waiters, func(x *slotWaiter) bool { return x == w })
}

// Arrive is arrive to a door admitting the sockets it accepts
// (listeners.Admission).
func (s *Server) Arrive(arrived time.Time) (bool, listeners.SlotWait) {
	held, w := s.arrive(arrived)
	if w == nil {
		return held, nil
	}
	return false, w
}

// ReleaseSlot gives back a slot: to the socket that has waited longest for
// one, or to the count when none is waiting.
func (s *Server) ReleaseSlot() {
	s.gate.mu.Lock()
	defer s.gate.mu.Unlock()
	s.handOn()
}

// handOn is ReleaseSlot under the gate's lock. A waiter whose wait has
// ended, or who has left, is passed over; none is handed a slot once the
// server is stopping.
func (s *Server) handOn() {
	if !s.gate.stopping {
		s.pruneWaiters(time.Now())
		if len(s.gate.waiters) > 0 {
			w := s.gate.waiters[0]
			s.gate.waiters = s.gate.waiters[1:]
			w.left, w.handed = true, true
			s.gate.overflow-- // admitted: it holds a slot now, not a place
			close(w.granted)
			return
		}
	}
	s.slots.Add(-1)
}

// SocketRefused reports a socket a listener closed for want of a
// max_connections slot, before it was a connection, as Server busy.
func (s *Server) SocketRefused(listener string) {
	s.hooks.OnSocketRefused(listener, packets.ErrServerBusy)
}

// ConnectTimeout is how long a new connection may take to send its CONNECT,
// 0 for no bound (Options.ClientConnectTimeout).
func (s *Server) ConnectTimeout() time.Duration { return s.Options.ClientConnectTimeout }

// refuseBusy answers a connection that arrived with every slot taken.
//
// **It reads only what the answer needs**: the fixed header, the protocol
// version - which decides between MQTT 5's 0x89 and 3.1.1's 0x03 - and the
// client id when it fits within busyPrefix, so the refusal still names the
// device. The declared body is never allocated and no credential is checked,
// so a flood of connections over the limit costs the broker a goroutine and
// a few bytes each rather than a CONNECT body and a password hash.
//
// A connection that does not open with a CONNECT is closed with no answer,
// as any other would be.
//
// **Answered or closed by until**, the overflow bound from its arrival
// (arrive), whatever it waited for a slot first.
func (s *Server) refuseBusy(cl *Client, until time.Time) {
	r := cl.Net.bconn
	if r == nil {
		return
	}
	if cl.Net.Conn != nil {
		_ = cl.Net.Conn.SetReadDeadline(until)
	}

	b, err := r.ReadByte()
	if err != nil || b>>4 != packets.Connect {
		return
	}
	remaining, _, err := packets.DecodeLength(r)
	if err != nil {
		return
	}
	pk, ok := readConnectPrefix(cl, remaining)
	if !ok {
		return
	}

	// The hook is told the code the client was actually sent, which differs
	// by protocol version: a hook told one code while the client was sent
	// another cannot answer the question it exists for.
	code := packets.ErrServerBusy
	if pk.ProtocolVersion < 5 {
		code = packets.ErrServerUnavailable
	}
	_ = s.SendConnack(cl, code, false, nil)
	s.hooks.OnConnectRefused(cl, pk, code)
}

// refuseUnreadConnect answers a CONNECT refused while it was read, with the
// packet as far as it was read: malformed, a protocol error, or declaring
// more bytes than it may carry (readConnectionPacket).
//
// **MQTT 5 is answered with a CONNACK carrying the code**, which §4.13
// allows: 0x95 (Packet too large) as HiveMQ answers it, and a malformed one
// as mosquitto and EMQX answer it. A CONNECT refused before its protocol
// version was read, or below MQTT 5, is closed with nothing written, as
// mosquitto closes it: 3.1.1 defines no CONNACK code for either, and every
// CONNECT refusal a 3.1.1 client can reach must be one it defines. The hook
// is told the code either way, since that is why the connection went. An
// error that is no reason code - the connection closing, a deadline - is no
// refusal, and nothing is told.
func (s *Server) refuseUnreadConnect(cl *Client, pk packets.Packet, err error) {
	code, ok := readRefusal(err)
	if !ok {
		return
	}
	// The version the CONNACK is written in, as readConnectPrefix sets it:
	// the client is not parsed from a CONNECT that was not read.
	cl.Properties.ProtocolVersion = pk.ProtocolVersion
	// Told before the CONNACK is written, as validateConnect's refusals are.
	s.hooks.OnConnectRefused(cl, pk, code)
	if pk.ProtocolVersion >= 5 {
		_ = s.SendConnack(cl, code, false, nil)
	}
}

// readRefusal is the reason code a packet was refused with while it was read,
// and whether it was: a code of 0x80 or above, found with errors.As, since a
// decoder wraps the code it refuses with.
//
// **A hook's ErrRejectPacket is not one.** OnPacketRead returns it to drop a
// packet, and the hook contract never puts it on the wire: a hook that ends
// the connection for it has ended it and counted it, and saguin's release of
// a held exactly-once publish returns it when the store fails, to close the
// connection without a PUBCOMP so the client sends its PUBREL again.
func readRefusal(err error) (packets.Code, bool) {
	var code packets.Code
	if errors.Is(err, packets.ErrRejectPacket) || !errors.As(err, &code) ||
		code.Code < packets.ErrUnspecifiedError.Code {
		return code, false
	}
	return code, true
}

// readConnectPrefix reads the protocol version of a CONNECT whose fixed
// header has been read, and its client id when it fits within busyPrefix,
// and nothing past them. It returns a packet carrying only those, for a
// refusal and its hook; ok is false when the bytes are not a CONNECT this can
// answer.
//
// Every read is bounded by the length the CONNECT declared and by the
// connection's read deadline, and each field by busyPrefix, so what a refused
// connection can make the server read is a few hundred bytes.
func readConnectPrefix(cl *Client, remaining int) (pk packets.Packet, ok bool) {
	r := cl.Net.bconn
	// read takes n bytes of the CONNECT, never more than it declared.
	read := func(n int) []byte {
		if n < 0 || n > remaining {
			return nil
		}
		p := make([]byte, n)
		for got := 0; got < n; {
			k, err := r.Read(p[got:])
			if err != nil {
				return nil
			}
			got += k
		}
		remaining -= n
		return p
	}

	nameLen := read(2)
	if nameLen == nil {
		return pk, false
	}
	if n := int(nameLen[0])<<8 | int(nameLen[1]); n > 6 || read(n) == nil { // "MQTT", or 3.1's "MQIsdp"
		return pk, false
	}
	fields := read(4) // version, flags, keepalive
	if fields == nil {
		return pk, false
	}
	version := fields[0]
	cl.Properties.ProtocolVersion = version

	id := ""
	func() {
		if version >= 5 {
			props, bu, err := packets.DecodeLength(r)
			if err != nil {
				return
			}
			remaining -= bu
			if props > busyPrefix || read(props) == nil {
				return
			}
		}
		idLen := read(2)
		if idLen == nil {
			return
		}
		if n := int(idLen[0])<<8 | int(idLen[1]); n <= busyPrefix {
			if p := read(n); p != nil {
				id = string(p)
			}
		}
	}()
	cl.ID = id

	pk = packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: version,
		Connect: &packets.ConnectParams{ClientIdentifier: id}}
	return pk, true
}

// sessionLocks serializes establishing a session under one client id.
//
// Establishing a session looks the id up, takes over what it finds, adds the
// new client and sends its CONNACK - each step safe on its own and the
// sequence not - so two CONNECTs under one id arriving together could both
// find nothing and both stay connected as the same client, or one could be
// taken over before its CONNACK was written. [MQTT-3.1.4-3] [MQTT-3.2.0-1]
//
// **Per id rather than one lock for every connection**, because the sequence
// includes the takeover's DISCONNECT and the CONNACK, which are writes bounded
// only by the write deadline: one client that stopped reading would otherwise
// hold every other client's connect for that long.
//
// An id's entry is removed when nothing holds or waits for it, so the map is
// bounded by connections being established rather than by ids ever seen
// (invariant 13).
//
// **It is also the lock a hook takes to change what an id holds** (saguin:
// LockSession). A session ending outside it checked who owned the id, let go
// of the broker's own lock, and removed by id from its stores - so a
// connection that claimed the id in between, or claimed it and left, lost
// its stored positions, its exactly-once publishes and its record to a
// session that had already ended.
type sessionLocks struct {
	mu   sync.Mutex
	held map[string]*sessionLock
}

type sessionLock struct {
	holder  *lockWaiter   // who holds it, nil when nobody does; guarded by sessionLocks.mu
	queue   []*lockWaiter // waiting, in arrival order, guarded by sessionLocks.mu
	waiting int           // holders and waiters, parked ones included, guarded by sessionLocks.mu
	arrived uint64        // the last waiter's arrival number, guarded by sessionLocks.mu
	// after is what the holder queued to run once it lets go
	// (AfterSessionUnlock). Guarded by the lock itself: only its holder
	// appends, and the release takes the list before handing it on.
	after []func()
}

// lockWaiter is one caller holding or waiting for an id's lock.
type lockWaiter struct {
	granted chan bool // true: it holds the lock; false: superseded
	seq     uint64    // arrival order
	// connect is set for a connection establishing its session, and clean
	// for one asking for a clean start. passable is a CONNECT a newer one
	// may supersede. resumable is set by a CONNECT holding the lock: whether
	// the session was there to resume when it claimed the id, which is what
	// each CONNECT it passed over is answered.
	connect   bool
	clean     bool
	passable  bool
	resumable bool
	// Set for a waiter superseded: the Session Present it is answered, and
	// whether the CONNECT that would have taken the session from it - the
	// next one for the id - asked for a clean start, which decides its Will.
	sessionPresent bool
	successorClean bool
	// parked are the CONNECTs this one supersedes, answered once it has
	// claimed the id and given back to the queue if it does not.
	parked []*lockWaiter
}

// lock takes the lock for id and returns what releases it. It is not
// reentrant: a holder that takes it again waits for itself.
func (l *sessionLocks) lock(id string) (unlock func()) {
	release := l.acquire(id, &lockWaiter{})
	return func() { release(false) }
}

// lockConnect is lock for a connection establishing its session, w, whose
// release says whether it claimed the id. w.passable is false for a CONNECT
// that must run whatever follows it; otherwise the CONNECT may be
// superseded, and then it holds nothing and there is no release: nil, with
// w.sessionPresent and w.successorClean set.
//
// **Newest wins for a CONNECT that would only be taken over** (saguin). Each
// CONNECT waiting here was going to take the session over only to be taken
// over by the next, and a client re-connecting faster than a takeover
// completes - a slow store, a storm of reconnecting devices - queued them
// without bound: 2,616 CONNECTs whose clients had already gone, 1 GB, and
// 88s before the client's latest connection was answered, with every
// unrelated session waiting seconds behind them on the one store. A waiter
// that resumes, and has a newer CONNECT for the id behind it, is answered
// instead once that newer one has claimed the id - its
// CONNACK, then the takeover's DISCONNECT 0x8E, which is what it would have
// been sent anyway, sooner - and does no store work; its Will is decided as
// a taken-over connection's is, by whether the CONNECT after it asked for a
// clean start. Should the newer one not claim the id, the waiters it
// superseded go back to the queue in arrival order and run as they would
// have. A clean start, a CONNECT whose session ends with its connection, and
// every caller that is not a CONNECT, keep their place, since each changes
// what the next one finds.
func (l *sessionLocks) lockConnect(id string, w *lockWaiter) (unlock func(claimed bool)) {
	w.connect = true
	return l.acquire(id, w)
}

func (l *sessionLocks) acquire(id string, w *lockWaiter) (unlock func(claimed bool)) {
	l.mu.Lock()
	if l.held == nil {
		l.held = map[string]*sessionLock{}
	}
	e := l.held[id]
	if e == nil {
		e = &sessionLock{}
		l.held[id] = e
	}
	e.waiting++
	release := func(claimed bool) { l.release(id, e, claimed) }
	if e.holder == nil {
		e.holder = w
		l.mu.Unlock()
		return release
	}
	w.granted = make(chan bool, 1)
	e.arrived++
	w.seq = e.arrived
	e.queue = append(e.queue, w)
	l.mu.Unlock()
	if <-w.granted {
		return release
	}
	return nil
}

// release lets id's lock go. What the holder superseded is answered if it
// claimed the id and queued again first if it did not; the CONNECTs a newer
// one now makes pointless are parked on it; the first waiter left has the
// lock; and what the holder queued runs (afterUnlock).
func (l *sessionLocks) release(id string, e *sessionLock, claimed bool) {
	after := e.after
	e.after = nil
	l.mu.Lock()
	e.waiting--
	parked := e.holder.parked
	e.holder.parked = nil
	resumable := e.holder.resumable
	l.mu.Unlock()

	// **Answered what the holder found as it claimed the id, and answered
	// whatever that was.** Asked again of the session now, the holder owns it
	// by now, and one whose session ends with its connection read as no
	// session; and a CONNECT sent back to the queue for finding no session
	// then ran after the newer one and took the id from it. Each passed over
	// resumes and outlives its connection (passable), so it leaves the
	// session to the next as it found it - present or not.
	var gone, back []*lockWaiter
	for _, w := range parked {
		if claimed {
			w.sessionPresent = resumable
			gone = append(gone, w)
			continue
		}
		back = append(back, w)
	}

	l.mu.Lock()
	e.waiting -= len(gone)
	if len(back) > 0 {
		e.queue = append(e.queue, back...)
		slices.SortFunc(e.queue, func(a, b *lockWaiter) int { return cmp.Compare(a.seq, b.seq) })
	}
	// Each CONNECT that may be superseded and has a newer CONNECT behind it
	// is parked on the nearest newer one that runs, with what it had parked
	// on itself, and remembers whether the one right after it - which would
	// have taken the session from it - asked for a clean start.
	//
	// **A caller that is not a CONNECT and arrived after a parked one goes
	// after the one it is parked on**, so that it still finds the session as
	// the parked CONNECT would have left it: a delayed Will falling due behind
	// a resume was cancelled by that resume, and run ahead of it would be
	// published for a device that had come back inside its delay
	// [MQTT-3.1.3-9]. kept is built newest first, so what is between the
	// parked CONNECT and its successor is what follows the successor in it.
	var succ, taker *lockWaiter
	at := -1 // succ's index in kept
	kept := make([]*lockWaiter, 0, len(e.queue))
	for i := len(e.queue) - 1; i >= 0; i-- {
		w := e.queue[i]
		if succ != nil && w.passable {
			w.successorClean = taker.clean
			succ.parked = append(succ.parked, w)
			succ.parked = append(succ.parked, w.parked...)
			w.parked = nil
			taker = w
			if between := kept[at+1:]; len(between) > 0 {
				moved := append([]*lockWaiter(nil), between...)
				kept = append(append(kept[:at], moved...), succ)
				at = len(kept) - 1
			}
			continue
		}
		if w.connect {
			succ, taker, at = w, w, len(kept)
		}
		kept = append(kept, w)
	}
	slices.Reverse(kept)
	e.queue = kept
	e.holder = nil
	var next *lockWaiter
	if len(e.queue) > 0 {
		next = e.queue[0]
		e.queue = e.queue[1:]
		e.holder = next
	}
	if e.waiting == 0 {
		delete(l.held, id)
	}
	l.mu.Unlock()
	for _, w := range gone {
		w.granted <- false
	}
	if next != nil {
		next.granted <- true
	}
	for _, fn := range after {
		fn()
	}
}

// afterUnlock queues fn to run once the holder of id's lock lets it go.
//
// **Only that holder may call it.** The queue is guarded by the lock itself
// rather than by l.mu: the holder appends, and the release takes the queue
// before unlocking, so the next holder appends to a queue of its own. A
// caller not holding the lock would append while the holder's release is
// taking the queue, and what it queued - a Will - could be lost. With nothing
// holding the lock, now is after it, and fn runs at once.
func (l *sessionLocks) afterUnlock(id string, fn func()) {
	l.mu.Lock()
	e := l.held[id]
	l.mu.Unlock()
	if e == nil {
		fn()
		return
	}
	e.after = append(e.after, fn)
}

// waiters is how many hold or wait for id's lock.
func (l *sessionLocks) waiters(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.held[id]; e != nil {
		return e.waiting
	}
	return 0
}

// LockSession takes a client id's session lock, the one a connection holds
// from the hook that claims the id until it is registered, and returns what
// releases it (saguin).
//
// **A hook that changes what an id holds from outside that sequence takes
// it** - a session ending with its connection, a delayed Will falling due -
// so that no connection can claim the id between the hook deciding the id is
// still its own and the hook's last write. It is not reentrant, and a hook
// the engine calls while holding it (OnSessionEstablish, OnClientExpired,
// OnSessionRegistered) must not take it again.
func (s *Server) LockSession(id string) (unlock func()) {
	return s.sessions.lock(id)
}

// AfterSessionUnlock runs fn once the id's session lock is let go, on the
// goroutine that lets it go (saguin).
//
// **Only the holder of the id's lock may call it**: the queue is the lock's
// own, and a call from anywhere else races the holder's release and can lose
// what it queued (sessionLocks.afterUnlock). The broker's walk
// TestNothingPublishesUnderASessionLock checks every call site holds it.
//
// **What publishes waits for this.** A fan-out that reaches a connection
// being taken over waits for that connection's session lock (deliveryTarget),
// so a publish made while holding one id's lock can wait on another's - and
// two ids doing that at once, each subscribed to the other, wait for ever.
func (s *Server) AfterSessionUnlock(id string, fn func()) {
	s.sessions.afterUnlock(id, fn)
}

// SessionLockWaiters is how many connections and hooks hold or wait for a
// client id's session lock: what a test reads to know that a connection is
// waiting its turn rather than guessing from a clock.
func (s *Server) SessionLockWaiters(id string) int {
	return s.sessions.waiters(id)
}

// loop contains interval tickers for the system events loop.
type loop struct {
	clientExpiry   *time.Ticker // interval ticker for cleaning expired clients
	inflightExpiry *time.Ticker // interval ticker for cleaning up expired inflight messages
	retainedExpiry *time.Ticker // interval ticker for cleaning retained messages
}

// ops contains server values which can be propagated to other structs.
type ops struct {
	options *Options     // a pointer to the server options and capabilities, for referencing in clients
	info    *system.Info // pointers to server system info
	hooks   *Hooks       // pointer to the server hooks
	log     *slog.Logger // a structured logger for the client
}

// New returns a new instance of the broker. Optional parameters
// can be specified to override some default settings (see Options).
func New(opts *Options) *Server {
	if opts == nil {
		opts = new(Options)
	}

	opts.ensureDefaults()

	s := &Server{
		done:      make(chan bool),
		Clients:   NewClients(),
		Topics:    NewTopicsIndex(),
		Listeners: listeners.New(),
		loop: &loop{
			clientExpiry:   time.NewTicker(time.Second),
			inflightExpiry: time.NewTicker(time.Second),
			retainedExpiry: time.NewTicker(time.Second),
		},
		Options: opts,
		Info: &system.Info{
			Version: Version,
			Started: time.Now().Unix(),
		},
		Log: opts.Logger,
		hooks: &Hooks{
			Log: opts.Logger,
		},
	}

	if s.Options.InlineClient {
		s.inlineClient = s.NewClient(nil, LocalListener, InlineClientId, true)
		s.Clients.Add(s.inlineClient)
	}

	return s
}

// ensureDefaults ensures that the server starts with sane default values, if none are provided.
func (o *Options) ensureDefaults() {
	if o.Capabilities == nil {
		o.Capabilities = NewDefaultServerCapabilities()
	}

	o.Capabilities.maximumPacketID = math.MaxUint16 // spec maximum is 65535

	if o.ClientNetWriteBufferSize == 0 {
		o.ClientNetWriteBufferSize = 1024 * 2
	}

	// **512 bytes, not 2 KB** (saguin): every connection holds its read
	// buffer for as long as it is open, idle or not, so at 10,000
	// connections the difference was 15 MB. What it costs is a read call:
	// a packet of 506 to about 2,000 bytes takes two where it took one,
	// measured over loopback. A smaller packet, or a larger one, takes what
	// it took: bufio reads straight into the packet's own buffer whenever
	// what is left to read is at least the buffer's size
	// (TestAPacketLargerThanTheReadBufferIsReadWhole). NATS starts a
	// connection's read buffer at 512 bytes too.
	if o.ClientNetReadBufferSize == 0 {
		o.ClientNetReadBufferSize = 512
	}

	if o.Logger == nil {
		log := slog.New(slog.NewTextHandler(os.Stdout, nil))
		o.Logger = log
	}
}

// NewClient returns a new Client instance, populated with all the required values and
// references to be used with the server. If you are using this client to directly publish
// messages from the embedding application, set the inline flag to true to bypass ACL and
// topic validation checks.
func (s *Server) NewClient(c net.Conn, listener string, id string, inline bool) *Client {
	cl := newClient(c, &ops{ // [MQTT-3.1.2-6] implicit
		options: s.Options,
		info:    s.Info,
		hooks:   s.hooks,
		log:     s.Log,
	})

	cl.ID = id
	cl.Net.Listener = listener

	if inline { // inline clients bypass acl and some validity checks.
		cl.Net.Inline = true
		// By default, we don't want to restrict developer publishes,
		// but if you do, reset this after creating inline client.
		cl.State.Inflight.ResetReceiveQuota(math.MaxInt32)
	}

	return cl
}

// AddHook attaches a new Hook to the server. Ideally, this should be called
// before the server is started with s.Serve().
func (s *Server) AddHook(hook Hook, config any) error {
	nl := s.Log.With("hook", hook.ID())
	hook.SetOpts(nl, &HookOptions{
		Capabilities: s.Options.Capabilities,
	})

	s.Log.Info("added hook", "hook", hook.ID())
	return s.hooks.Add(hook, config)
}

// AddListener adds a new network listener to the server, for receiving incoming client connections.
func (s *Server) AddListener(l listeners.Listener) error {
	if _, ok := s.Listeners.Get(l.ID()); ok {
		return ErrListenerIDExists
	}

	// A door whose sockets arrive before they are handed over counts them
	// from their arrival (listeners.Websocket.SetAdmission).
	if a, ok := l.(interface{ SetAdmission(listeners.Admission) }); ok {
		a.SetAdmission(s)
	}
	nl := s.Log.With(slog.String("listener", l.ID()))
	err := l.Init(nl)
	if err != nil {
		return err
	}

	s.Listeners.Add(l)

	s.Log.Info("attached listener", "id", l.ID(), "protocol", l.Protocol(), "address", l.Address())
	return nil
}

// Serve starts the event loops responsible for establishing client connections
// on all attached listeners, and starting all hooks.
func (s *Server) Serve() error {
	s.Log.Info("saguin mqtt engine starting", "version", Version)
	defer s.Log.Info("saguin mqtt engine started")

	go s.eventLoop()                            // spin up event loop for server housekeeping and closing server.
	s.Listeners.ServeAll(s.EstablishConnection) // start listening on all listeners.
	s.hooks.OnStarted()

	return nil
}

// eventLoop loops forever, running various server housekeeping methods at different intervals.
func (s *Server) eventLoop() {
	s.Log.Debug("system event loop started")
	defer s.Log.Debug("system event loop halted")

	for {
		select {
		case <-s.done:
			return
		case <-s.loop.clientExpiry.C:
			s.clearExpiredClients(time.Now())
		case <-s.loop.retainedExpiry.C:
			s.clearExpiredRetainedMessages(time.Now().Unix())
		case <-s.loop.inflightExpiry.C:
			s.clearExpiredInflights(time.Now().Unix())
		}
	}
}

// EstablishConnection establishes a new client when a listener accepts a new
// connection, or when a caller embedding the server hands it one directly.
//
// Listeners.Establish is where the connection is registered with the
// waitgroup Close waits on, so that both of those callers are covered by it.
func (s *Server) EstablishConnection(listener string, c net.Conn) error {
	return s.Listeners.Establish(listener, c, func(listener string, c net.Conn) error {
		cl := s.NewClient(c, listener, "", false)
		// A shutdown reaches the socket through its client from here, and
		// no longer closes it under the client's writes (Listeners.CloseAll).
		s.Listeners.Govern(c, func() { s.closeGoverned(cl) })
		return s.attachClient(cl, listener)
	})
}

// attachClient validates an incoming client connection and if viable, attaches the client
// to the server, performs session housekeeping, and reads incoming packets.
//
// **The read loop blocks under this frame, so this frame stays small**
// (saguin): the CONNECT's handling is in establishClient and the ending in
// detachClient, each returned from or not yet called while the connection
// idles. An idle reader parks with every frame from the listener's goroutine
// down to the socket still live, and the GC halves a stack only while under
// a quarter of it is in use. With the CONNECT's packet in this frame (3 KB)
// and the read's in Client.Read's (1.7 KB), 5,960 bytes stayed live and every
// reader kept the 16 KB it grew to on the CONNECT; without them it is 1,432
// bytes and the reader goes back to 8 KB
// (TestAnIdleConnectionHoldsLittleStack).
func (s *Server) attachClient(cl *Client, listener string) error {
	var st attachState
	defer s.endAttach(cl, &st)
	cl.armWriter()

	if !s.admit(cl) {
		return packets.ErrServerBusy
	}

	proceed, err := s.establishClient(cl, listener, &st)
	if !proceed {
		cl.beginEnd() // ended here, whether or not anything was written
		return err
	}
	if err == nil {
		err = cl.Read(s.receivePacket, s.markWaiting)
	}
	return s.detachClient(cl, listener, err)
}

// admit bounds the connection's wait for its CONNECT and takes its
// max_connections slot, answering 0x89 where it gets none: everything
// attachClient does before the CONNECT is read, out of its frame
// (attachClient has why).
//
//go:noinline
func (s *Server) admit(cl *Client) bool {
	// **Its arrival is when its door accepted it**, recorded before it
	// waited for a slot, so the wait counts against connect_timeout rather
	// than starting it again; and for a websocket, before its TLS handshake
	// and HTTP upgrade, so connect_timeout is one bound over all of that
	// and the CONNECT together (RFC 0002), not a second one from the
	// upgrade.
	a := listeners.AdmittedOf(cl.Net.Conn)
	arrived := time.Now()
	if a != nil {
		arrived = a.Arrived()
	}
	if ws, ok := cl.Net.Conn.(interface{ Arrived() time.Time }); ok && !ws.Arrived().IsZero() {
		arrived = ws.Arrived()
	}
	// refreshDeadline replaces this once the CONNECT has been read, and
	// every path that returns before it reads nothing more.
	if d := s.Options.ClientConnectTimeout; d > 0 && cl.Net.Conn != nil {
		_ = cl.Net.Conn.SetReadDeadline(arrived.Add(d))
	}

	// **A connection holds a max_connections slot from the moment it
	// arrives**, as mosquitto, NATS and EMQX count it: before its CONNECT is
	// read and before it authenticates. Counted only once authenticated, the
	// limit bounded nothing that happens before that - every connection
	// still authenticating passed the check (50 of 50 against a limit of 5),
	// each could hold a CONNECT body of up to max_message_size (1,000 of
	// them held a gigabyte) and each ran a password hash. Held from accept,
	// all of that is bounded by the number an operator already sets.
	//
	// **Its door admitted it as it accepted it** (listeners.Admit): a slot,
	// or a place in the overflow budget to wait for one in (arrive), decided
	// before the socket was handed to anything. Either goes back as the
	// connection's end is decided (Client.beginEnd) or the socket closes.
	if a != nil {
		if a.AwaitSlot() {
			cl.holdSlot(a.ReleaseSlot)
			return true
		}
		s.refuseBusy(cl, a.Until())
		return false
	}

	// A connection handed to the server directly (EstablishConnection), past
	// any door, is admitted here the same way.
	held, w := s.arrive(arrived)
	if !held && w == nil {
		return false
	}
	if !held {
		if held = w.Await(); !held {
			s.refuseBusy(cl, w.Until())
			w.Done()
			return false
		}
	}
	cl.holdSlot(s.ReleaseSlot)
	return true
}

// attachState is what attachClient's one deferred call gives back: the count
// of connected clients, if it was counted.
type attachState struct {
	counted bool
}

// endAttach gives back what attachClient took: the connected count, then the
// connection, with its slot if no earlier ending gave that back (Stop).
func (s *Server) endAttach(cl *Client, st *attachState) {
	if st.counted {
		s.Info.ClientsConnected.Add(-1)
	}
	cl.Stop(nil)
}

// establishClient reads and answers the CONNECT and registers the
// connection: everything attachClient does before the read loop. It answers
// false where the connection ends without one, with the error to end on;
// true with a nil error to read, and true with an error where the connection
// was established and is to end as a failed read would.
//
//go:noinline
func (s *Server) establishClient(cl *Client, listener string, st *attachState) (bool, error) {
	pk, err := s.readConnectionPacket(cl)
	if err != nil {
		s.refuseUnreadConnect(cl, pk, err)
		return false, fmt.Errorf("read connection: %w", err)
	}

	cl.ParseConnect(listener, pk)

	// **From here to its answer, a client is waiting on this id** (saguin):
	// on the id's session lock, which a departing connection holds while its
	// store writes run, and on those writes. Marked before the lock is taken
	// (lockConnect), or the writes its holder waits on - queued behind a
	// storm's worth of other departed clients' - would be moved up only once
	// they had run. Cleared as the connection is established or refused,
	// after its CONNACK.
	if f := s.ClientWaiting; f != nil {
		defer f(cl.ID)()
	}

	code := s.validateConnect(cl, pk) // [MQTT-3.1.4-1] [MQTT-3.1.4-2]
	if code != packets.CodeSuccess {
		// Told before the CONNACK is written, so that a hook still sees the
		// refusal when the write itself fails.
		s.hooks.OnConnectRefused(cl, pk, code)

		// **A wrong protocol name is answered with nothing at all.**
		//
		// §3.1.2.1 gives a server two choices when the Protocol Name is not
		// "MQTT": disconnect, or carry on under some other specification.
		// What it forbids is the third thing, and [MQTT-3.1.2-1] says so:
		// "the Server MUST NOT continue to process the CONNECT packet in
		// line with this specification". A CONNACK is this specification,
		// so answering one is the option the rule rules out. The bytes on
		// that connection are not MQTT and the broker has no grounds to
		// believe the other end would understand a CONNACK anyway.
		//
		// Every other refusal here keeps its CONNACK, including an
		// unsupported protocol *version*: §3.1.2.2 explicitly allows 0x84
		// and then a close, because a client that got the name right is
		// speaking MQTT and can read the answer.
		if code == packets.ErrProtocolViolationProtocolName {
			return false, code // [MQTT-3.1.2-1]
		}

		if err := s.SendConnack(cl, code, false, nil); err != nil {
			return false, fmt.Errorf("invalid connection send ack: %w", err)
		}
		return false, code // [MQTT-3.2.2-7] [MQTT-3.1.4-6]
	}

	err = s.hooks.OnConnect(cl, pk)
	if err != nil {
		return false, err
	}

	cl.refreshDeadline(cl.State.Keepalive)
	if !s.hooks.OnConnectAuthenticate(cl, pk) { // [MQTT-3.1.4-2]
		err := s.SendConnack(cl, packets.ErrBadUsernameOrPassword, false, nil)
		if err != nil {
			return false, fmt.Errorf("invalid connection send ack: %w", err)
		}

		return false, packets.ErrBadUsernameOrPassword
	}

	s.Info.ClientsConnected.Add(1)
	st.counted = true

	// From the hook that claims the id to the CONNACK, one connection per
	// client id at a time: sessionLocks has why the CONNACK is inside. A
	// resume may be superseded while it waits (lockConnect).
	//
	// **Only a resume that outlives its connection may be passed over** for a
	// newer CONNECT, since only it leaves the session to the next as it found
	// it. One that ends with its connection ends the session when the next
	// takes it over, which changes what that one finds, as a clean start does.
	waiter := &lockWaiter{clean: pk.Connect.Clean, passable: !pk.Connect.Clean && !cl.EndsWithConnection()}
	release := s.sessions.lockConnect(cl.ID, waiter)
	if release == nil {
		return false, s.answerSuperseded(cl, pk, waiter.sessionPresent, waiter.successorClean)
	}

	// **A session whose expiry has passed is ended before this connection
	// claims its id** (saguin), rather than when the session is inherited.
	// Ended there, a hook's expiry work ran after OnSessionEstablish had
	// already recorded the new connection as the id's owner, so a hook that
	// asks whose session it is ending found somebody else's and ended
	// nothing: a new exactly-once publish acknowledged and the expired
	// session's unreleased one written in its place, and a client told
	// Session Present 0 served from the expired session's stored position.
	// The sweep would have ended it within the second; a client coming back
	// on the expiry's own schedule lands in that second. One moment for the
	// whole decision, so the session judged here is the one judged below.
	//
	// **This is where a session whose expiry has passed is not inherited**
	// [MQTT-4.1.0-2], answered when the question is asked rather than when
	// clearExpiredClients' one-second ticker next fires: a client asking for
	// a two-second expiry and coming back three seconds later found its
	// session still there whenever the tick had not landed in between - four
	// passes and two failures over six runs of one conformance test, before
	// this. inheritClientSession therefore never finds an expired session,
	// and holds no branch for one.
	now := time.Now()
	if existing, ok := s.Clients.Get(cl.ID); ok && existing != cl && s.sessionExpired(existing, now) {
		s.Log.Debug("a session expired as its client came back", "client", cl.ID)
		s.expireSession(existing)
	}

	// **A hook may refuse here, and this is the last moment one can**
	// (saguin): the CONNACK below carries what it returns, so state a hook
	// has to keep before a client is told it is connected - a Will - is
	// answered with a reason code rather than with an acceptance it cannot
	// honour.
	if err := s.hooks.OnSessionEstablish(cl, pk); err != nil {
		release(false)
		var code packets.Code
		if !errors.As(err, &code) {
			code = packets.ErrUnspecifiedError
		}
		s.hooks.OnConnectRefused(cl, pk, code)
		if err := s.SendConnack(cl, code, false, nil); err != nil {
			return false, fmt.Errorf("refuse connection packet: %w", err)
		}
		return false, code // [MQTT-3.2.2-7]
	}

	// **The CONNACK goes out before the connection can be found** (saguin).
	// Registered first, a connection could be found by its id and written to
	// before its CONNACK, which [MQTT-3.2.0-1] forbids and a client refuses:
	// saguin's delivery of a channel's records to a resuming consumer did
	// exactly that, measured in the link-churn soak as "received unexpected
	// packet 3". So whether a session is present is decided first, the
	// CONNACK is sent, and only then is the session taken over and the
	// connection registered - all under the id's session lock, so nothing
	// about the session moves in between.
	sessionPresent := s.sessionWillBePresent(pk, cl, now)
	// What each CONNECT passed over for this one is answered (release): in
	// arrival order each would have run just before this one and left the
	// session as it found it, so it would have found what this one finds.
	waiter.resumable = s.sessionResumable(cl, now)
	err = s.SendConnack(cl, code, sessionPresent, nil) // [MQTT-3.1.4-5] [MQTT-3.2.0-1] [MQTT-3.2.0-2] &[MQTT-3.14.0-1]
	var inherited uint64
	if err == nil {
		s.inheritClientSession(pk, cl, now)
		// What the resend below owes, read before the registration: once
		// registered, other senders reach this connection, and what they
		// register is theirs to write (ResendInherited).
		inherited = cl.State.Inflight.Registered()
		// And every PUBLISH held for the write loop until that resend is
		// written (holdForResend).
		cl.State.resuming.Store(sessionPresent)
		s.Clients.Add(cl) // [MQTT-4.1.0-1]
		// **The last moment the id is this connection's alone** (saguin):
		// registered, and still holding the lock no other connection can
		// claim the id without. A clean start ends the session it replaced
		// here - after the registration, so that a clean connect does not
		// wait on the ending's store writes between its CONNACK and being
		// registered, and before any other connection can take the id from
		// under the ending.
		s.hooks.OnSessionRegistered(cl, pk)
	}
	release(err == nil)
	if err != nil {
		// **Nothing this connection wrote may outlive it** (saguin). The
		// CONNACK never reached the client, so there is no session and never
		// was one: a hook that kept state at OnSessionEstablish - a session
		// record, the Will on it - is told here, with expire, or it keeps
		// state for a connection that did not happen. Nothing else would
		// tell it: this path returns before the disconnect below.
		s.hooks.OnDisconnect(cl, err, true)
		return false, fmt.Errorf("ack connection packet: %w", err)
	}

	// **A resend that fails ends the connection the way a read that fails
	// does** (saguin). It returned here, after the connection was
	// registered, so nothing below ran: no Will offered to OnWill, and
	// OnDisconnect never called - the broker's session store went on saying
	// the client was connected, and a later start discarded its Will as one
	// whose client had not gone. OnSessionEstablished still runs first, so
	// the hooks see the order a connection that dies straight after
	// establishing always showed them.
	if sessionPresent {
		if rerr := cl.ResendInherited(inherited); rerr != nil {
			err = fmt.Errorf("resend inflight: %w", rerr)
		}
		cl.endResend()
	}

	s.hooks.OnSessionEstablished(cl, pk)

	return true, err
}

// detachClient ends an established connection: everything attachClient
// does after the read loop, with the error it ended on.
//
//go:noinline
func (s *Server) detachClient(cl *Client, listener string, err error) error {
	cl.beginEnd() // ended as the read loop returned
	if errors.Is(err, ErrClientDisconnected) {
		err = nil
		cl.Properties.Will = Will{} // [MQTT-3.14.4-3] [MQTT-3.1.2-10]
	} else {
		// **A packet refused while it was read is answered with its reason
		// code**, as receivePacket answers one refused once it was read: a
		// malformed packet, a protocol error in one, or one over the Maximum
		// Packet Size this server's own CONNACK gave [MQTT-3.2.2-15]. §4.13
		// has a server send the DISCONNECT before it closes the connection,
		// and mosquitto and EMQX both send it. Without this the client was
		// hung up on with no reason, and the refusal counted nowhere.
		//
		// refuseConnection leaves the processPacket path out: receivePacket
		// ended a connection it refused, so a client still open here was
		// refused before the handler ran.
		owned := false
		if code, ok := readRefusal(err); ok {
			owned = s.refuse(cl, code)
		}
		s.sendLWT(cl)
		// **The read loop's end is claimed like any other** (Client.claimEnd):
		// a read that failed while another goroutine was writing the
		// DISCONNECT that ends the connection waits for it, rather than
		// closing the socket under it, and reports that end.
		if !owned && !cl.stop(err) {
			err = cl.endedBy(err)
		}
	}
	s.Log.Debug("client disconnected", "error", err, "client", cl.ID, "remote", cl.Net.Remote, "listener", listener)

	expire := cl.EndsWithConnection()
	s.hooks.OnDisconnect(cl, err, expire)

	// Under the id's session lock, and asked there (saguin): unsubscribing
	// reaches the hooks that keep a session's subscriptions by client id, and
	// a connection that took the id since is one this must not reach.
	if expire {
		unlock := s.sessions.lock(cl.ID)
		if !cl.IsTakenOver() {
			cl.ClearInflights()
			s.UnsubscribeClient(cl)
			// This client, not the id: a connection that took the id since stays.
			s.Clients.DeleteIf(cl.ID, cl) // [MQTT-4.1.0-2] ![MQTT-3.1.2-23]
		}
		unlock()
	}

	cl.releaseConnection()
	return err
}

// readConnectionPacket reads the first incoming header for a connection, and if
// acceptable, returns the valid connection packet.
func (s *Server) readConnectionPacket(cl *Client) (pk packets.Packet, err error) {
	r := cl.Net.bconn
	if r == nil {
		return pk, ErrConnectionClosed
	}
	fh := new(packets.FixedHeader)
	b, err := r.ReadByte()
	if err != nil {
		return pk, err
	}
	if err = fh.Decode(b); err != nil {
		return pk, err
	}
	if fh.Type != packets.Connect {
		return pk, packets.ErrProtocolViolationRequireFirstConnect // [MQTT-3.1.0-1]
	}
	var bu int
	fh.Remaining, bu, err = packets.DecodeLength(r)
	if err != nil {
		return pk, err
	}

	// **The whole packet, from its declared length, before a byte of the
	// body is read.** One byte of fixed header, the Remaining Length's own
	// bytes, and the Remaining Length. A CONNECT over either bound is
	// refused from its opening bytes - the one of its own, or the Maximum
	// Packet Size every packet is held to - so neither is ever allocated at
	// the size a stranger declared.
	total := uint64(1+bu) + uint64(fh.Remaining)
	max := uint64(s.Options.ClientMaxConnectSize)
	if mps := uint64(s.Options.Capabilities.MaximumPacketSize); mps > 0 && (max == 0 || mps < max) {
		max = mps
	}
	if max > 0 && total > max {
		// Its opening bytes, for refuseUnreadConnect, and not the rest.
		pk, _ = readConnectPrefix(cl, fh.Remaining)
		return pk, packets.ErrPacketTooLarge
	}

	pk, err = cl.ReadPacket(fh)
	if err != nil {
		return
	}

	return
}

// receivePacket processes an incoming packet for a client, and issues a disconnect to the client
// if an error has occurred (if mqtt v5).
func (s *Server) receivePacket(cl *Client, pk packets.Packet) error {
	err := s.processPacket(cl, pk)
	if err != nil {
		// errors.As, so a hook's wrapped code is answered as its bare one is.
		var code packets.Code
		refused := errors.As(err, &code)
		if refused && code.Code >= packets.ErrUnspecifiedError.Code {
			_ = s.refuseConnection(cl, code)
		}

		// A reason code is a decision, not a fault: the client has just
		// been told precisely what was wrong with its packet, and the
		// server did what the specification asks of it. Reporting that at
		// Warn tells an operator watching for problems that the server
		// failed when it worked -- and formatting the packet to say so
		// writes the client's own payload into the log, at a level that is
		// on by default, at whatever rate a client can reconnect and
		// repeat itself. One 100KiB publish carrying a topic alias its new
		// connection had never registered produced a single line of over
		// 300KB here.
		//
		// This is the same distinction #516 drew for a hook's reason code,
		// in the same tree, and the same treatment: debug, and the fields
		// that identify the case rather than the packet that carries it.
		// An error that is not a Code is a genuine fault and keeps both.
		if refused {
			s.Log.Debug("packet refused",
				"code", code.Code, "reason", code.Reason, "client", cl.ID,
				"listener", cl.Net.Listener, "type", pk.FixedHeader.Type)

			return err
		}

		s.Log.Warn("error processing packet", "error", err, "client", cl.ID, "listener", cl.Net.Listener, "pk", pk)

		return err
	}

	return nil
}

// markWaiting marks cl's id while pk, a packet whose answer waits on stored
// state, is handled, until what it returns is called (ClientWaiting).
//
// **A packet whose answer waits on stored state marks its client id
// until it is answered**, before it takes any lock: a SUBACK or UNSUBACK
// waits on the session's record, and with Retain Handling 2 on a position;
// a PUBACK or PUBREC on the record kept, a seek's on its position; a
// PUBCOMP on the exchange let go; an AUTH on the session it
// re-authenticates. A DISCONNECT, a PINGREQ and an acknowledgement answer
// nothing that waits on the store.
//
// **Marked as the packet is decoded, before any hook has it**
// (Client.readOne): an OnPacketRead hook may write what the answer waits
// on - saguin releases a held exactly-once publish there, before the
// PUBCOMP. Marked only once the hooks had run, that write was a departed
// client's when it forgot the hold of a deletion with nothing to delete,
// and queued behind every departed client's write.
func (s *Server) markWaiting(cl *Client, pk packets.Packet) (answered func()) {
	if f := s.ClientWaiting; f != nil && waitsOnStore(pk) {
		return f(cl.ID)
	}
	return func() {}
}

// waitsOnStore reports whether pk is a packet whose answer can wait on what
// the store writes for its client (markWaiting).
func waitsOnStore(pk packets.Packet) bool {
	switch pk.FixedHeader.Type {
	case packets.Subscribe, packets.Unsubscribe, packets.Pubrel, packets.Auth:
		return true
	case packets.Publish:
		return pk.FixedHeader.Qos > 0
	}
	return false
}

// validateConnect validates that a connect packet is compliant.
func (s *Server) validateConnect(cl *Client, pk packets.Packet) packets.Code {
	code := pk.ConnectValidate() // [MQTT-3.1.4-1] [MQTT-3.1.4-2]
	if code != packets.CodeSuccess {
		return code
	}

	if cl.Properties.ProtocolVersion < 5 && !pk.Connect.Clean && pk.Connect.ClientIdentifier == "" {
		return packets.ErrUnspecifiedError
	}

	if cl.Properties.ProtocolVersion < s.Options.Capabilities.MinimumProtocolVersion {
		return packets.ErrUnsupportedProtocolVersion // [MQTT-3.1.2-2]
	} else if cl.Properties.Will.Qos > s.Options.Capabilities.MaximumQos {
		return packets.ErrQosNotSupported // [MQTT-3.2.2-12]
	} else if cl.Properties.Will.Retain && s.Options.Capabilities.RetainAvailable == 0x00 {
		return packets.ErrRetainNotSupported // [MQTT-3.2.2-13]
	}

	return code
}

// sessionWillBePresent is what inheritClientSession will answer, decided
// before it moves anything so that the CONNACK can say it first. Both run
// under the id's session lock with the same now, so they cannot disagree.
func (s *Server) sessionWillBePresent(pk packets.Packet, cl *Client, now time.Time) bool {
	return !pk.Connect.Clean && s.sessionResumable(cl, now)
}

// sessionResumable reports whether a CONNECT for cl's id that asks to resume
// would find a session: one held, not expired, and not one that ends with the
// connection the takeover closes.
func (s *Server) sessionResumable(cl *Client, now time.Time) bool {
	existing, ok := s.Clients.Get(cl.ID)
	return ok && !s.sessionExpired(existing, now) && !existing.EndsWithConnection()
}

// answerSuperseded answers a CONNECT a newer one for its client id
// superseded while it waited (lockConnect), with what it would have been
// sent had it taken the session over and been taken over in turn: its
// CONNACK, then the takeover's DISCONNECT. It claimed nothing, so no other
// session hook is told of it; OnSessionSuperseded decides its Will, as a
// taken-over connection's is decided once it is closed, holding no lock.
func (s *Server) answerSuperseded(cl *Client, pk packets.Packet, present, successorClean bool) error {
	// Ended here, and given back as the DISCONNECT that tells it is written
	// (WritePacket), or for a 3.1.1 client by the close (Stop): the CONNACK
	// before it is an ordinary write.
	err := s.SendConnack(cl, packets.CodeSuccess, present, nil) // [MQTT-3.2.0-1]
	if err == nil {
		_ = s.DisconnectClient(cl, packets.ErrSessionTakenOver) // [MQTT-3.1.4-3]
	}
	s.hooks.OnSessionSuperseded(cl, pk, successorClean)
	atomic.StoreUint32(&cl.Properties.Will.Flag, 0)
	s.Log.Debug("a connection was superseded as it waited", "client", cl.ID, "remote", cl.Net.Remote)
	if err != nil {
		return fmt.Errorf("ack superseded connection packet: %w", err)
	}
	return packets.ErrSessionTakenOver
}

// inheritClientSession inherits the state of an existing client sharing the same
// connection ID. If clean is true, the state of any previously existing client
// session is abandoned.
func (s *Server) inheritClientSession(pk packets.Packet, cl *Client, now time.Time) bool {
	if existing, ok := s.Clients.Get(cl.ID); ok {
		// An expired session is not found here: establishClient ended it
		// before the id was claimed, with the same moment.

		_ = s.DisconnectClient(existing, packets.ErrSessionTakenOver) // [MQTT-3.1.4-3]
		// **A session that ends with its connection ends here** (saguin): the
		// takeover closes that connection, so what it held is not inherited,
		// whatever the new CONNECT's Clean Start says.
		if pk.Connect.Clean || existing.EndsWithConnection() { // [MQTT-3.1.2-4] [MQTT-3.1.4-4]
			s.UnsubscribeClient(existing)
			existing.EndTakenOver()
			return false // [MQTT-3.2.2-3]
		}

		// Nothing is recorded on the old connection once its table is copied:
		// deliveryTarget has why.
		existing.State.handover.Lock()
		existing.State.isTakenOver.Store(true)
		if existing.State.Inflight.Len() > 0 {
			cl.State.Inflight = existing.State.Inflight.Clone() // [MQTT-3.1.2-5]
			if cl.State.Inflight.maximumReceiveQuota == 0 && cl.ops.options.Capabilities.ReceiveMaximum != 0 {
				cl.State.Inflight.ResetReceiveQuota(int32(cl.ops.options.Capabilities.ReceiveMaximum)) // server receive max per client
				cl.State.Inflight.ResetSendQuota(int32(cl.Properties.Props.ReceiveMaximum))            // client receive max
			}
		}
		existing.State.handover.Unlock()

		for _, sub := range existing.State.Subscriptions.GetAll() {
			existed := !s.Topics.Subscribe(cl.ID, sub) // [MQTT-3.8.4-3]
			if !existed {
				s.Info.Subscriptions.Add(1)
			}
			cl.State.Subscriptions.Add(sub.Filter, sub)
		}

		// Clean the state of the existing client to prevent sequential take-overs
		// from increasing memory usage by inflights + subs * client-id.
		s.UnsubscribeClient(existing)
		existing.ClearInflights()

		s.Log.Debug("session taken over", "client", cl.ID, "old_remote", existing.Net.Remote, "new_remote", cl.Net.Remote)

		return true // [MQTT-3.2.2-3]
	}

	return false // [MQTT-3.2.2-2]
}

// expireSession ends a session whose expiry has passed, found as its client
// came back: what the sweep does for one nobody returns to, with the old
// connection marked taken over under its handover lock, so nothing more is
// recorded against it.
func (s *Server) expireSession(existing *Client) {
	s.hooks.OnClientExpired(existing)
	s.UnsubscribeClient(existing)
	existing.State.handover.Lock()
	existing.ClearInflights()
	existing.State.isTakenOver.Store(true)
	existing.State.handover.Unlock()
	s.Clients.DeleteIf(existing.ID, existing) // [MQTT-4.1.0-2]
}

// RestoredSession is a session a keeper held while the broker was stopped, as
// the server puts it back: a client the broker knows and nothing is connected
// to.
//
// It is the state a session is in between a disconnect and a resume, built
// from a store rather than left behind by a connection, so a client that comes
// back is resumed by the path that resumes one whose connection merely
// dropped - Session Present 1 and its subscriptions. What it is owed is the
// broadcast log's, which its drain serves when it returns.
type RestoredSession struct {
	ClientID string

	// ExpiryInterval is the Session Expiry Interval the session was granted,
	// after limits.max_session_expiry capped it, and DisconnectedAt is when
	// its client went away. Its expiry is measured from that moment rather
	// than from this start: a session does not get its interval back because
	// the broker was restarted.
	ExpiryInterval uint32
	DisconnectedAt time.Time

	Subscriptions []packets.Subscription

	// Receiving is the packet identifiers of the exactly-once publishes this
	// session sent and has not released: the broker answered each with a
	// PUBREC and owes the exchange. They are the client's own identifiers,
	// a different space from those the broker gives its deliveries
	// [MQTT-2.2.1], and they go back so that the PUBREL the client re-sends is
	// answered rather than refused - and so that the PUBLISH it may re-send
	// first is answered as the repeat it is, without the message being taken
	// in twice.
	Receiving []uint16
}

// RestoreSession puts one session back and returns the client holding it.
//
// **It runs before any listener opens**, where the sessions a keeper held are
// read, so nothing can be connecting to the id while this builds it. The
// client it registers has no connection at all - no socket, no read loop and
// no write loop - which is what an away client is; a delivery made to it is
// held for its session exactly as one made between a disconnect and a resume.
//
// **The protocol version is not kept and is not needed.** A session comes back
// as MQTT 5 so that its granted expiry is what judges it (sessionExpired reads
// the interval on the version 5 branch, and a 3.1.1 session was granted the
// cap). Nothing else a 3.1.1 session could be served differs: the subscription
// options that read the version - Retain As Published, No Local, a
// Subscription Identifier - are ones a 3.1.1 client cannot set, so a session
// made by one carries them off and is served the same either way. What the
// client is on the connection it comes back on is that connection's own.
func (s *Server) RestoreSession(r RestoredSession) *Client {
	cl := s.NewClient(nil, "", r.ClientID, false)
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Clean = false
	cl.Properties.Props.SessionExpiryInterval = r.ExpiryInterval
	cl.Properties.Props.SessionExpiryIntervalFlag = true

	// Closed before anything is put in it, so a delivery arriving while this
	// runs is held rather than written to a client with no socket. Stop
	// stamps the moment it was called; the session's own moment goes on
	// after, because that is what its expiry is measured from.
	cl.Stop(ErrSessionRestored)
	away := r.DisconnectedAt
	if away.IsZero() {
		// No moment was recorded for it, which is a crash rather than a
		// stop: a graceful shutdown ends every connection through the
		// ordinary disconnect path, so the store carries the moment the
		// broker stopped and the session's expiry runs from there - measured,
		// and the right answer, because that is when the client became
		// unreachable. What reaches here is a session nothing ended: the
		// process died, and the only moment anybody can honestly measure
		// from is this start.
		away = time.Now()
	}
	cl.State.disconnected.Store(away.UnixNano())

	for _, sub := range r.Subscriptions {
		if !s.Topics.Subscribe(cl.ID, sub) {
			continue // the same filter twice in one session's record
		}
		cl.State.Subscriptions.Add(sub.Filter, sub)
		s.Info.Subscriptions.Add(1)
	}

	for _, id := range r.Receiving {
		// The entry processPublish leaves between the PUBREC it wrote and
		// the PUBREL it is waiting for, under the client's own identifier.
		// Its receive quota is the resuming connection's own and is reset
		// there, so nothing of this connection's window is restored with it.
		ack := s.buildAck(id, packets.Pubrec, 0, packets.Properties{}, packets.CodeSuccess)
		cl.State.Inflight.Set(ack)
	}

	unlock := s.sessions.lock(cl.ID)
	s.Clients.Add(cl)
	unlock()
	return cl
}

// SendConnack returns a Connack packet to a client.
func (s *Server) SendConnack(cl *Client, reason packets.Code, present bool, properties *packets.Properties) error {
	if properties == nil {
		properties = &packets.Properties{
			ReceiveMaximum: s.Options.Capabilities.ReceiveMaximum,
		}
	}

	properties.ReceiveMaximum = s.Options.Capabilities.ReceiveMaximum // 3.2.2.3.3 Receive Maximum
	if s.Options.Capabilities.MaximumPacketSize > 0 {
		properties.MaximumPacketSize = s.Options.Capabilities.MaximumPacketSize
	}

	if cl.State.ServerKeepalive { // You can set this dynamically using the OnConnect hook.
		properties.ServerKeepAlive = cl.State.Keepalive // [MQTT-3.1.2-21]
		properties.ServerKeepAliveFlag = true
	}

	if reason.Code >= packets.ErrUnspecifiedError.Code {
		if cl.Properties.ProtocolVersion < 5 {
			if v3reason, ok := packets.V5CodesToV3[reason]; ok { // NB v3 3.2.2.3 Connack return codes
				reason = v3reason
			}
		}

		properties.ReasonString = reason.Reason
		ack := packets.Packet{
			FixedHeader: packets.FixedHeader{
				Type: packets.Connack,
			},
			SessionPresent: false,       // [MQTT-3.2.2-6]
			ReasonCode:     reason.Code, // [MQTT-3.2.2-8]
			Properties:     *properties,
		}
		return cl.WritePacket(ack)
	}

	if s.Options.Capabilities.MaximumQos < 2 {
		properties.MaximumQos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9]
		properties.MaximumQosFlag = true
	}

	if cl.Properties.Props.AssignedClientID != "" {
		properties.AssignedClientID = cl.Properties.Props.AssignedClientID // [MQTT-3.1.3-7] [MQTT-3.2.2-16]
	}

	if cl.Properties.Props.SessionExpiryInterval > s.Options.Capabilities.MaximumSessionExpiryInterval {
		properties.SessionExpiryInterval = s.Options.Capabilities.MaximumSessionExpiryInterval
		properties.SessionExpiryIntervalFlag = true
		cl.Properties.Props.SessionExpiryInterval = properties.SessionExpiryInterval
		cl.Properties.Props.SessionExpiryIntervalFlag = true
	}

	ack := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Connack,
		},
		SessionPresent: present,
		ReasonCode:     reason.Code, // [MQTT-3.2.2-8]
		Properties:     *properties,
	}
	return cl.WritePacket(ack)
}

// processPacket processes an inbound packet for a client. Since the method is
// typically called as a goroutine, errors are primarily for test checking purposes.
func (s *Server) processPacket(cl *Client, pk packets.Packet) error {
	var err error

	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = s.processConnect(cl, pk)
	case packets.Disconnect:
		err = s.processDisconnect(cl, pk)
	case packets.Pingreq:
		err = s.processPingreq(cl, pk)
	case packets.Publish:
		code := pk.PublishValidate(s.Options.Capabilities.TopicAliasMaximum)
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processPublish(cl, pk)
	case packets.Puback:
		err = s.processPuback(cl, pk)
	case packets.Pubrec:
		err = s.processPubrec(cl, pk)
	case packets.Pubrel:
		err = s.processPubrel(cl, pk)
	case packets.Pubcomp:
		err = s.processPubcomp(cl, pk)
	case packets.Subscribe:
		code := pk.SubscribeValidate()
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processSubscribe(cl, pk)
	case packets.Unsubscribe:
		code := pk.UnsubscribeValidate()
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processUnsubscribe(cl, pk)
	case packets.Auth:
		code := pk.AuthValidate()
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processAuth(cl, pk)
	default:
		return fmt.Errorf("no valid packet available; %v", pk.FixedHeader.Type)
	}

	if err != nil {
		return err
	}

	// **A withheld packet stays in flight once it is written.** It used to be
	// deleted here, which cost twice: its PUBACK found nothing and returned no
	// quota, so a client lost one slot of its Receive Maximum for every packet
	// sent this way and stopped being sent anything at about twice its window;
	// and a message on the wire and out of the session was not re-sent if the
	// link dropped before it was acknowledged. Claimed rather than read, so
	// saguin cannot return a withheld queue delivery that is being written.
	//
	// As many as there is room for rather than one: an acknowledgement returns
	// one slot of window but may free the bytes of several small deliveries
	// that were waiting for the session's share on the wire (sessionWireBytes).
	if cl.State.Inflight.Len() > 0 {
		cl.WakeWriter()
	}

	return nil
}

// PublishToSubscribers hands an accepted publish to the subscribers its topic
// matches, through the same selection OnSelectSubscribers makes for any other.
//
// It is for a record written after its PUBLISH was answered - an
// exactly-once publish stored at its PUBREL - whose shared subscribers would
// otherwise have been served at the PUBLISH, before there was a record.
func (s *Server) PublishToSubscribers(pk packets.Packet) {
	s.publishToSubscribers(pk)
}

// processConnect processes a Connect packet. The packet cannot be used to establish
// a new connection on an existing connection. See EstablishConnection instead.
func (s *Server) processConnect(cl *Client, _ packets.Packet) error {
	s.sendLWT(cl)
	return packets.ErrProtocolViolationSecondConnect // [MQTT-3.1.0-2]
}

// processPingreq processes a Pingreq packet.
// processPingreq answers a PINGREQ.
//
// **It takes no lock, and a PINGREQ waits only for what is ahead of it on
// its own connection**: the substrate reads a connection's packets one at a
// time, so it waits for the previous packet's handling - for a consumer, a
// PUBACK, which takes saguin's broker-wide lock - and its PINGRESP waits
// behind every PUBLISH already written to that connection, which no broker
// can reorder. With a Receive Maximum of 65,535 the second alone measured
// half a second under an ordinary drain.
//
// So a wedged lock delays a ping only where that connection has a PUBACK in
// handling at that moment. At the scale run's size that was many
// connections at once - 2,121 goroutines sat in OnQosComplete in its dump -
// so the wedge delayed pings statistically, while the queued PUBLISHes did
// the rest. A test pinging through a drain was tried as a wedge check and
// could not fail: it passed through four 700ms holds of the lock injected
// into OnQosComplete, pinging on a timer and pinging behind a PUBACK alike.
// The holds are guarded by lockhold_test.go instead.
func (s *Server) processPingreq(cl *Client, _ packets.Packet) error {
	return cl.WritePacket(packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Pingresp, // [MQTT-3.12.4-1]
		},
	})
}

// Publish publishes a publish packet into the broker as if it were sent from the specified client.
// This is a convenience function which wraps InjectPacket. As such, this method can publish packets
// to any topic (including $SYS) and bypass ACL checks. The qos byte is used for limiting the
// outbound qos (mqtt v5) rather than issuing to the broker (we assume qos 2 complete).
func (s *Server) Publish(topic string, payload []byte, retain bool, qos byte) error {
	if !s.Options.InlineClient {
		return ErrInlineClientNotEnabled
	}

	return s.InjectPacket(s.inlineClient, packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Qos:    qos,
			Retain: retain,
		},
		TopicName: topic,
		Payload:   payload,
		PacketID:  uint16(qos), // we never process the inbound qos, but we need a packet id for validity checks.
	})
}

// Subscribe adds an inline subscription for the specified topic filter and subscription identifier
// with the provided handler function.
func (s *Server) Subscribe(filter string, subscriptionId int, handler InlineSubFn) error {
	if !s.Options.InlineClient {
		return ErrInlineClientNotEnabled
	}

	if handler == nil {
		return packets.ErrInlineSubscriptionHandlerInvalid
	}

	if !IsValidFilter(filter, false) {
		return packets.ErrTopicFilterInvalid
	}

	subscription := packets.Subscription{
		Identifier: subscriptionId,
		Filter:     filter,
	}

	// Under the in-process client's session lock, as a connection's
	// SUBSCRIBE is, so every call of the subscription hooks holds one
	// (saguin).
	unlock := s.sessions.lock(s.inlineClient.ID)
	pk := s.hooks.OnSubscribe(s.inlineClient, packets.Packet{ // subscribe like a normal client.
		Origin:      s.inlineClient.ID,
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe},
		Filters:     packets.Subscriptions{subscription},
	})

	inlineSubscription := InlineSubscription{
		Subscription: subscription,
		Handler:      handler,
	}

	s.Topics.InlineSubscribe(inlineSubscription)
	s.hooks.OnSubscribed(s.inlineClient, pk, []byte{packets.CodeSuccess.Code})
	unlock()

	// Handling retained messages.
	for _, pkv := range s.Topics.Messages(filter) { // [MQTT-3.8.4-4]
		handler(s.inlineClient, inlineSubscription.Subscription, pkv)
	}
	return nil
}

// Unsubscribe removes an inline subscription for the specified subscription and topic filter.
// It allows you to unsubscribe a specific subscription from the internal subscription
// associated with the given topic filter.
func (s *Server) Unsubscribe(filter string, subscriptionId int) error {
	if !s.Options.InlineClient {
		return ErrInlineClientNotEnabled
	}

	if !IsValidFilter(filter, false) {
		return packets.ErrTopicFilterInvalid
	}

	unlock := s.sessions.lock(s.inlineClient.ID)
	pk := s.hooks.OnUnsubscribe(s.inlineClient, packets.Packet{
		Origin:      s.inlineClient.ID,
		FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe},
		Filters: packets.Subscriptions{
			{
				Identifier: subscriptionId,
				Filter:     filter,
			},
		},
	})

	s.Topics.InlineUnsubscribe(subscriptionId, filter)
	s.hooks.OnUnsubscribed(s.inlineClient, pk)
	unlock()
	return nil
}

// InjectPacket injects a packet into the broker as if it were sent from the specified client.
// Inline clients using this method can publish packets to any topic (including $SYS) and bypass ACL checks.
func (s *Server) InjectPacket(cl *Client, pk packets.Packet) error {
	pk.ProtocolVersion = cl.Properties.ProtocolVersion

	err := s.processPacket(cl, pk)
	if err != nil {
		return err
	}

	// **Not counted as a message received.** What is injected did not
	// arrive: a queue's offer to its worker, a Will, a record an inbound
	// bridge carried in. Counted here, saguin_publishes_received_total read
	// every job twice - once from its producer, once offered - so a queue
	// working perfectly looked like half of what was sent landing. A message
	// received is a PUBLISH read off a connection (readPacket), as
	// mosquitto's messages/received is; an injector whose publishes did
	// arrive over one of its own - the inbound bridge - counts them itself.

	return nil
}

// processPublish processes a Publish packet.
func (s *Server) processPublish(cl *Client, pk packets.Packet) error {
	if !cl.Net.Inline && !IsValidFilter(pk.TopicName, true) {
		// A $SYS topic is the only thing that reaches here: every other way
		// IsValidFilter can refuse a publish is a wildcard or an absent
		// topic name, and PublishValidate has already answered those with a
		// DISCONNECT before this runs.
		//
		// Refusing the publish is right - 4.7.2 says the server should stop
		// clients exchanging messages over $SYS - but a QoS 1 or 2 client
		// must still be answered [MQTT-4.3.2-4] [MQTT-4.3.3-8]. Returning
		// here without one leaves it waiting for an acknowledgement that
		// will never arrive, holding a slot of its send quota for as long as
		// the session lasts.
		//
		// This is what a publish an ACL denies already gets, a few lines
		// below, and for the same reason: the client may not write there.
		if pk.FixedHeader.Qos == 0 {
			return nil
		}

		if cl.Properties.ProtocolVersion != 5 {
			return s.refuseConnection(cl, packets.ErrNotAuthorized)
		}

		ackType := packets.Puback
		if pk.FixedHeader.Qos == 2 {
			ackType = packets.Pubrec
		}

		return cl.WritePacket(s.buildAck(pk.PacketID, ackType, 0, pk.Properties, packets.ErrNotAuthorized))
	}

	if atomic.LoadInt32(&cl.State.Inflight.receiveQuota) == 0 {
		return s.refuseConnection(cl, packets.ErrReceiveMaximum) // ~[MQTT-3.3.4-7] ~[MQTT-3.3.4-8]
	}

	if !cl.Net.Inline && !s.hooks.OnACLCheck(cl, pk.TopicName, true) {
		if pk.FixedHeader.Qos == 0 {
			return nil
		}

		if cl.Properties.ProtocolVersion != 5 {
			return s.refuseConnection(cl, packets.ErrNotAuthorized)
		}

		ackType := packets.Puback
		if pk.FixedHeader.Qos == 2 {
			ackType = packets.Pubrec
		}

		ack := s.buildAck(pk.PacketID, ackType, 0, pk.Properties, packets.ErrNotAuthorized)
		return cl.WritePacket(ack)
	}

	pk.Origin = cl.ID
	pk.Created = time.Now().Unix()

	if expiry := minimum(s.Options.Capabilities.MaximumMessageExpiryInterval,
		int64(pk.Properties.MessageExpiryInterval)); expiry > 0 {
		pk.Expiry = pk.Created + expiry
	}

	if !cl.Net.Inline {
		// A Pubrec entry is this client's own QoS 2 publish, mid-PUBREL
		// exchange, stored under the identifier the client chose - the one
		// kind of entry here that shares an identifier space with an inbound
		// PUBLISH. Anything else is a message the server sent, under an
		// identifier the server assigned, and the two spaces are independent
		// (MQTT-2.2.1). Deleting one of those discards a delivery the server
		// is still waiting to have acknowledged.
		if pki, ok := cl.State.Inflight.Get(pk.PacketID); ok && pki.FixedHeader.Type == packets.Pubrec {
			// **A repeat is answered the way the first one was, and the
			// message is not delivered twice.**
			//
			// This is a client re-sending a PUBLISH the server has already
			// taken ownership of and answered with a PUBREC, which is what
			// §4.3.3's exchange asks it to do when the PUBREC has not got
			// back. The server owns the message either way, so the second
			// PUBREC says the same thing as the first, and the publish
			// itself is dropped here rather than delivered again.
			//
			// **It used to answer 0x91, Packet Identifier in use, and that
			// is a failure report about a message the broker had
			// accepted.** [MQTT-4.4.0-2]: a reason code of 0x80 or above
			// means the exchange is over and the identifier is free, so a
			// publisher receiving it records the publish as failed and may
			// reuse the identifier for something else, while the broker
			// still holds the message and will deliver it. The publisher's
			// records and the broker's then disagree about a message that
			// was in fact delivered, which is the worst of the two ways to
			// be wrong.
			//
			// 0x91 is for an identifier in use by a *different* exchange.
			// It cannot be that here: the entry found is this client's own
			// QoS 2 publish under the identifier it chose, and §2.2.1
			// forbids reusing an identifier that is still in flight, so a
			// second PUBLISH under it is the same message or a protocol
			// violation the client has already committed.
			//
			// mosquitto 2.0.22 answers the repeat PUBREC 0x00 and delivers
			// one copy, which is the behaviour this now matches.
			ack := s.buildAck(pk.PacketID, packets.Pubrec, 0, pk.Properties, packets.CodeSuccess)
			return cl.WritePacket(ack) // [MQTT-4.3.3-10] [MQTT-4.4.0-2]
		}
	}

	if pk.Properties.TopicAliasFlag && pk.Properties.TopicAlias > 0 { // [MQTT-3.3.2-11]
		pk.TopicName = cl.State.TopicAliases.Inbound.Set(pk.Properties.TopicAlias, pk.TopicName)

		// Section 3.3.2.3.4 case 3a: an alias this connection never
		// registered, carrying no topic name of its own, is a Protocol
		// Error and the answer is a DISCONNECT.
		//
		// PublishValidate cannot catch it. It refuses an absent topic only
		// when there is no alias either, and it runs against the decoded
		// packet, which is the one place that does not know which aliases
		// this connection has registered.
		//
		// Reaching this is ordinary rather than hostile: [MQTT-3.3.2-7]
		// discards every mapping when the network connection goes, so any
		// client that reconnects and keeps using an alias lands here. It
		// has to be told. Falling through publishes to the empty string,
		// which reaches no subscriber and which [MQTT-4.7.3-1] says is not
		// a topic name at all - and a QoS 1 or 2 client is then sent an
		// acknowledgement saying the message was delivered.
		if pk.TopicName == "" {
			return s.refuseConnection(cl, packets.ErrProtocolViolationNoTopic)
		}
	}

	if pk.FixedHeader.Qos > s.Options.Capabilities.MaximumQos {
		pk.FixedHeader.Qos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9] Reduce qos based on server max qos capability
	}

	pkx, err := s.hooks.OnPublish(cl, pk)
	if err == nil {
		pk = pkx
	} else if errors.Is(err, packets.ErrRejectPacket) {
		return nil
	} else if errors.Is(err, packets.CodeSuccessIgnore) {
		pk.Ignore = true
	} else if refusal := (packets.Code{}); cl.Properties.ProtocolVersion == 5 && pk.FixedHeader.Qos > 0 && errors.As(err, &refusal) {
		// The code errors.As extracted, not a type assertion on err itself.
		// errors.As succeeds when a hook WRAPS a packets.Code - which
		// fmt.Errorf("...: %w", code) does, and hooks do - and err is then
		// the wrapper, so asserting on it panics and takes the broker down
		// on an ordinary publish refusal.
		//
		// A QoS 2 publish rejected here must be answered with a PUBREC,
		// not a PUBACK - same selection the ACL-deny branch above already
		// makes for the same reason.
		ackType := packets.Puback
		if pk.FixedHeader.Qos == 2 {
			ackType = packets.Pubrec
		}
		err = cl.WritePacket(s.buildAck(pk.PacketID, ackType, 0, pk.Properties, refusal))
		if err != nil {
			return err
		}
		return nil
	}

	if pk.FixedHeader.Retain { // [MQTT-3.3.1-5] ![MQTT-3.3.1-8]
		s.retainMessage(cl, pk)
	}

	// If it's inlineClient, it can't handle PUBREC and PUBREL.
	// When it publishes a package with a qos > 0, the server treats
	// the package as qos=0, and the client receives it as qos=1 or 2.
	if pk.FixedHeader.Qos == 0 || cl.Net.Inline {
		s.publishToSubscribers(pk)
		return nil
	}

	cl.State.Inflight.DecreaseReceiveQuota()
	ack := s.buildAck(pk.PacketID, packets.Puback, 0, pk.Properties, packets.CodeSuccess) // [MQTT-4.3.2-4] [MQTT-3.4.2-1]
	if pk.FixedHeader.Qos == 2 {
		ack = s.buildAck(pk.PacketID, packets.Pubrec, 0, pk.Properties, packets.CodeSuccess) // [MQTT-3.3.4-1] [MQTT-4.3.3-8]
	}

	// A PUBREC waits for a PUBREL, so it is held here under the client's own
	// identifier, which is the space the rest of that exchange uses. A PUBACK
	// waits for nothing: it is written below and the flow is over, and the
	// Set and Delete around that write cancelled out. What they did not
	// cancel out was the entry they overwrote - the identifier is the
	// client's and this map is keyed by the server's, so a colliding number
	// cost the server the delivery it had outstanding there.
	//
	// **The write below is still exposed to that collision and this release
	// does not close it.** Inflight.Set overwrites unconditionally, so a
	// client choosing an identifier the server already has outstanding
	// replaces that delivery: it is never acknowledged, never re-sent, and
	// its quota is never returned. Set reports false and the counter and the
	// hook are skipped, which is why nothing observes the loss.
	//
	// The root of it is that one map holds two identifier spaces MQTT-2.2.1
	// says are independent - the client's for inbound flows, the server's
	// for outbound. Separating them is a larger change than this release
	// should carry, and the behaviour is not a regression: it is what every
	// earlier version does. It is written here so the next reader does not
	// have to rediscover that the fix above covers only the read half.
	if pk.FixedHeader.Qos == 2 {
		if ok := cl.State.Inflight.Set(ack); ok {
			s.hooks.OnQosPublish(cl, ack, ack.Created, 0)
		}
	}

	// **Answered once every session it is owed has it kept, not before.** A
	// hook keeps what a session that outlives its connection is owed as the
	// publish is taken in or its subscribers are chosen - saguin's broadcast
	// log, at OnPublish and, for a channel record a shared group hands such
	// a member, at the selection inside the fan-out - so writing the
	// acknowledgement after the fan-out means a publisher told "accepted" is
	// told the truth (RFC 0003 "Broadcast"). What the publisher waits for is
	// storage - its own message's - which is what a channel publisher has
	// always waited for; what another client does after that, such as giving
	// up the oldest of what it holds, it does not wait for
	// (TestABroadcastPublisherDoesNotWaitForAnotherSessionsGiveUps).
	s.publishToSubscribers(pk)

	err = cl.WritePacket(ack)
	if err != nil {
		return err
	}

	if pk.FixedHeader.Qos == 1 {
		cl.State.Inflight.IncreaseReceiveQuota()
		s.hooks.OnQosComplete(cl, ack)
	}

	return nil
}

// retainMessage adds a message to a topic, or removes the topic's retained
// message when the payload is empty.
//
// It kept nothing beyond that even before the strip took the persistent
// stores out; the sentence about adding the message to a store to be
// reloaded described mochi's storage hooks, which this engine does not
// have and whose reload path went with them. Retention that outlives a
// restart is saguin's, in its retained store, and RFC 0004 specifies it.
func (s *Server) retainMessage(cl *Client, pk packets.Packet) {
	if s.Options.Capabilities.RetainAvailable == 0 || pk.Ignore {
		return
	}

	out := pk.Copy(false)
	r := s.Topics.RetainMessage(out)
	s.hooks.OnRetainMessage(cl, pk, r)
}

// publishToSubscribers publishes a publish packet to all subscribers with matching topic filters.
func (s *Server) publishToSubscribers(pk packets.Packet) {
	if pk.Ignore {
		return
	}

	if pk.Created == 0 {
		pk.Created = time.Now().Unix()
	}

	if pk.Expiry == 0 {
		if expiry := minimum(s.Options.Capabilities.MaximumMessageExpiryInterval,
			int64(pk.Properties.MessageExpiryInterval)); expiry > 0 {
			pk.Expiry = pk.Created + expiry
		}
	}

	subscribers := s.Topics.Subscribers(pk.TopicName)

	// Whether there were shared subscriptions is decided before the hook
	// runs, and the shared selection below is gated on that rather than on
	// what the hook left behind. A hook selecting for a shared group says
	// which subscriber it chose by filling SharedSelected, and is free to
	// empty Shared while doing it -- testing Shared afterwards would then
	// skip the merge and deliver the message to nobody.
	hadShared := len(subscribers.Shared) > 0

	subscribers = s.hooks.OnSelectSubscribers(subscribers, pk)
	if hadShared {
		if len(subscribers.SharedSelected) == 0 {
			subscribers.SelectShared()
		}
		subscribers.MergeSharedSelected()
	}

	for _, inlineSubscription := range subscribers.InlineSubscriptions {
		inlineSubscription.Handler(s.inlineClient, inlineSubscription.Subscription, pk)
	}

	// **One delivery at a time, each made as it is prepared** (publishToClient):
	// a target is read-locked against a takeover from being chosen to being
	// recorded on, and only for that. The fan-out was two passes - every
	// delivery prepared, holding every target's read lock, then every one
	// made - so that a session store was asked once for the whole publish in
	// between; nothing is asked there now. What the shape still cost was a
	// sort of every subscriber id per publish, to take those locks in one
	// order (27us at 1,000 subscribers, 813us at 10,000), and a takeover of any one target waiting for the whole
	// fan-out rather than for its own delivery. Holding one lock at a time
	// needs no order, since there is no second lock to wait on in a cycle.
	//
	// Nothing across subscribers was ever promised (RFC 0003 "Ordering":
	// broadcast is ordered per subscriber), and per subscriber nothing moves:
	// one publish is still fanned out on one goroutine, whole, before its
	// publisher's next is read.
	for id, subs := range subscribers.Subscriptions {
		cl, ok := s.Clients.Get(id)
		if !ok {
			continue
		}
		if _, err := s.publishToClient(cl, subs, pk); err != nil {
			s.Log.Debug("failed publishing packet", "error", err, "client", cl.ID, "packet", pk)
		}
	}
}

// preparedDelivery is one subscriber's delivery of a publish, with its packet
// identifier taken, before it is put in flight or written.
type preparedDelivery struct {
	cl       *Client
	out      *packets.Packet // the publish as this subscriber is sent it
	withheld bool            // waits for the client's window
}

// deliveryTarget read-locks the connection a delivery is about to be recorded
// on, against a takeover of its session, and returns it.
//
// **A delivery never lands on a connection whose session has moved on**
// (saguin). A takeover copies the old connection's in-flight table to the new
// one and then clears the old. A delivery that chose the old connection
// before the copy and recorded on it after was cleared with it, and nothing
// counted it - measured with 50 durable clients taking their sessions over
// every 200 ms under 200 QoS 1 publishes a second: 21 and 25 of 100,000
// deliveries lost before session stores existed, 92 to 160 with a memory
// session store and 356 to 463 with a sqlite one, where a delivery waits
// longer between choosing a connection and recording on it.
//
// The takeover holds this lock for writing. A delivery either records
// before the copy, or finds the connection taken over, waits for the
// takeover to finish - it holds the id's session lock until the new
// connection is registered - and follows the session to the connection
// that took it, when that one holds the same subscription. Otherwise there
// is nothing to follow: a clean start or an expiry ended the session it was
// for, and the delivery is not made.
//
// A delivery holds one of these locks at a time, so a takeover and a publish
// cannot wait on each other in a cycle.
func (s *Server) deliveryTarget(cl *Client, sub packets.Subscription) (*Client, bool) {
	for {
		cl.State.handover.RLock()
		if !cl.IsTakenOver() {
			return cl, true
		}
		cl.State.handover.RUnlock()
		unlock := s.sessions.lock(cl.ID)
		unlock()
		next, ok := s.Clients.Get(cl.ID)
		if !ok || next == cl {
			return nil, false
		}
		if _, ok := next.State.Subscriptions.Get(sub.Filter); !ok {
			return nil, false
		}
		cl = next
	}
}

func (s *Server) publishToClient(cl *Client, sub packets.Subscription, pk packets.Packet) (packets.Packet, error) {
	cl, ok := s.deliveryTarget(cl, sub)
	if !ok {
		return pk, nil
	}
	defer cl.State.handover.RUnlock()
	d, err := s.prepareDelivery(cl, sub, pk)
	if d.out == nil {
		return pk, err
	}
	return s.makeDelivery(d, pk)
}

// prepareDelivery builds one subscriber's copy of a publish and, at QoS 1 or
// 2, takes its packet identifier. A delivery with no packet is none to make,
// with the error that says why, or none for a No Local subscription.
func (s *Server) prepareDelivery(cl *Client, sub packets.Subscription, pk packets.Packet) (preparedDelivery, error) {
	if sub.NoLocal && pk.Origin == cl.ID {
		return preparedDelivery{}, nil // [MQTT-3.8.3-3]
	}

	// Every subscriber's delivery points at the one payload the publish
	// arrived with; see CopySharingPayload for why that is safe.
	out := pk.CopySharingPayload()
	if !s.hooks.OnACLCheck(cl, pk.TopicName, false) {
		return preparedDelivery{}, packets.ErrNotAuthorized
	}
	if !sub.FwdRetainedFlag && ((cl.Properties.ProtocolVersion == 5 && !sub.RetainAsPublished) || cl.Properties.ProtocolVersion < 5) { // ![MQTT-3.3.1-13] [v3 MQTT-3.3.1-9]
		out.FixedHeader.Retain = false // [MQTT-3.3.1-12]
	}

	if len(sub.Identifiers) > 0 { // [MQTT-3.3.4-3]
		out.Properties.SubscriptionIdentifier = []int{}
		for _, id := range sub.Identifiers {
			out.Properties.SubscriptionIdentifier = append(out.Properties.SubscriptionIdentifier, id) // [MQTT-3.3.4-4] ![MQTT-3.3.4-5]
		}
		sort.Ints(out.Properties.SubscriptionIdentifier)
	}

	if out.FixedHeader.Qos > sub.Qos {
		out.FixedHeader.Qos = sub.Qos
	}

	if out.FixedHeader.Qos > s.Options.Capabilities.MaximumQos {
		out.FixedHeader.Qos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9]
	}

	// **No topic alias is ever sent to a subscriber** (saguin). An alias is
	// state of one connection, and the packet written here outlives the write:
	// it is kept in flight and re-sent to a resumed session on a connection
	// that never registered the alias, and one withheld for want of window
	// took an alias that a later QoS 0 packet then used before the client had
	// seen the topic. Both measured on bin/saguin. mosquitto 2.0.22 sends no
	// outbound alias either, measured. A client's own aliases, inbound, are
	// untouched.
	out.Properties.TopicAlias = 0
	out.Properties.TopicAliasFlag = false

	d := preparedDelivery{cl: cl}
	if out.FixedHeader.Qos > 0 {
		if !s.sessionHasRoom(cl, out) {
			s.Info.InflightDropped.Add(1)
			s.Info.SessionQueueDropped.Add(1)
			s.Log.Debug("refused a delivery: the session holds limits.session_queue_bytes it has not acknowledged",
				"client", cl.ID, "session_queue_bytes", s.Options.ClientSessionQueueBytes)
			return preparedDelivery{}, packets.ErrQuotaExceeded
		}

		i, err := cl.NextPacketID() // [MQTT-4.3.2-1] [MQTT-4.3.3-1]
		if err != nil {
			s.Info.InflightDropped.Add(1)
			s.Info.PacketIDsExhausted.Add(1)
			if cl.Exhausted() {
				s.Log.Warn("packet ids exhausted", "error", err, "client", cl.ID, "listener", cl.Net.Listener)
			}
			return preparedDelivery{}, packets.ErrQuotaExceeded
		}

		out.PacketID = uint16(i) // [MQTT-2.2.1-4]
		sentQuota := atomic.LoadInt32(&cl.State.Inflight.sendQuota)
		d.withheld = sentQuota == 0 && atomic.LoadInt32(&cl.State.Inflight.maximumSendQuota) > 0 ||
			!cl.State.Inflight.MayWrite(inflightSize(out), s.Options.sessionWireBytes()) ||
			!cl.OutboundHasRoom(out)
	}
	d.out = &out
	return d, nil
}

// makeDelivery puts a prepared delivery in flight and writes it, or leaves it
// withheld for the client's window. pk is the publish as it arrived.
//
// **Held whole, on the wire and waiting**, whether or not its client is
// connected: the in-flight table is the one copy of the message the engine
// has, and a re-send reads nothing else.
func (s *Server) makeDelivery(d preparedDelivery, pk packets.Packet) (packets.Packet, error) {
	cl, out, withheld := d.cl, d.out, d.withheld
	if out.FixedHeader.Qos > 0 {
		if ok := cl.State.Inflight.Set(*out); ok { // [MQTT-4.3.2-3] [MQTT-4.3.3-3]
			s.hooks.OnQosPublish(cl, *out, out.Created, 0)
			// A withheld delivery takes its slot of window when it is claimed
			// for writing. It used to take one here as well, which cost nothing
			// while only an empty window withheld; withheld with window to
			// spare, each would cost two slots and return one.
			if !withheld {
				cl.State.Inflight.DecreaseSendQuota()
			}
		}
		s.boundSession(cl, out.PacketID)

		if withheld {
			out.Expiry = -1
			cl.State.Inflight.Set(*out)
			// **Room may have opened since it was prepared.** An
			// acknowledgement arriving in between found nothing withheld to
			// send, and without this the delivery would wait for the next one,
			// which never comes if that was the client's last.
			if !cl.Closed() {
				cl.WakeWriter()
			}
			return *out, nil
		}
	}

	if cl.Net.Conn == nil || cl.Closed() {
		return *out, packets.CodeDisconnect
	}

	// A QoS 1 or 2 copy is queued beside its entry, whose first send the
	// write loop claims against the expiry sweep (Inflight.ClaimQueued).
	if out.FixedHeader.Qos > 0 {
		out.FirstSend = true
		cl.State.Inflight.Queue(out)
	}
	if cl.enqueue(out) {
		return *out, nil
	}

	// **A QoS 1 or 2 delivery is withheld, never dropped.** It is in flight
	// and kept already; dropping it here, as Mochi did, lost a message its
	// publisher was told had been accepted - measured with a slow QoS 1
	// subscriber whose queue QoS 0 traffic filled: 1 to 71 of 20,000 lost, none
	// counted as given up. It gives back the slot of window it took, and is
	// written once no QoS 1 or 2 delivery is queued ahead of it. It reaches here
	// only when the queue filled after it was prepared, so its session store
	// row may say it was sent; resumed, it is sent flagged as a duplicate,
	// which a client must accept.
	if out.FixedHeader.Qos > 0 {
		cl.State.Inflight.Withhold(out.PacketID)
		cl.State.Inflight.IncreaseSendQuota()
		cl.WakeWriter()
		return *out, nil
	}
	// **A queue refused because its connection ended is not a full one.** The
	// connection was open at the check above and ended before the packet
	// reached its queue (releaseConnection); the delivery is answered as that
	// check answers one to a closed connection, and is not counted as dropped
	// for a subscriber that could not keep up.
	if cl.State.outbound.ended() {
		return *out, packets.CodeDisconnect
	}
	cl.ops.hooks.OnPublishDropped(cl, pk)
	return *out, packets.ErrPendingClientWritesExceeded
}

// sessionWireBytes is the share of ClientSessionQueueBytes a session may have
// written and not had acknowledged: half. Past it a delivery is withheld
// rather than written, and written as acknowledgements make room.
//
// **The share is what keeps the newest.** Nothing on the wire can be taken
// back (Inflight.DropOldest), so a client that stops reading, or reads and
// never acknowledges, used to fill its whole bound with the first deliveries
// it was sent, and everything after was lost. Measured on bin/saguin with
// 2,000 4 KiB messages and a 1 MiB bound: such a client recovered to the
// oldest 207 and at most the single newest; with half on the wire, to the
// oldest 103 and the newest 103, the same for a client that stops reading and
// one that never acknowledges. EMQX has the same shape, a small in-flight
// window beside a queue that drops its oldest; mosquitto bounds only the
// queue and, measured, held 8 MiB for a client that never acknowledged.
//
// A client that stops reading is not disconnected for holding its share: it
// holds no more than its bound, and the write deadline still hangs it up once
// its socket stops accepting what it is sent.
func (o *Options) sessionWireBytes() int64 {
	return o.ClientSessionQueueBytes / 2
}

// sessionHasRoom reports whether a session may take one more delivery within
// ClientSessionQueueBytes: what it holds, less everything boundSession could
// give up for it, plus the delivery.
//
// **This is the bound for what cannot be given up.** A delivery on the wire to
// a connected client stays until it is acknowledged, and so does one at QoS 2
// or one saguin wrote itself, withheld or not. With nothing else bounding
// them, a client that reads and never acknowledges held everything it was
// sent - up to Mochi's count of 8,192 entries, and past it once that went:
// measured with 4 KiB messages, the broker grew by 109 MiB with the count and
// by 719 MiB without it, for one session whose bound was 1 MiB. A QoS 1
// broadcast delivery reaches this only past the wire's share
// (sessionWireBytes), withheld and so droppable; the rest are refused here.
//
// A session holding nothing it cannot give up always has room, so a message as
// large as the bound is never refused for its own size.
func (s *Server) sessionHasRoom(cl *Client, out packets.Packet) bool {
	max := s.Options.ClientSessionQueueBytes
	if max <= 0 {
		return true
	}
	held, size := cl.State.Inflight.Bytes(), inflightSize(out)
	if held+size <= max {
		return true
	}
	kept := held - cl.State.Inflight.DroppableBytes(cl.Closed())
	return kept <= 0 || kept+size <= max
}

// boundSession gives up the oldest deliveries a session holds past
// ClientSessionQueueBytes, never the one just queued.
//
// **Oldest, and never the publisher.** A session that has stopped reading, or
// whose client is away, is the one that pays for it: what it loses is what it
// would have read first, so what it finds when it returns is the most recent
// of what was sent. The publisher was already answered and is not told.
//
// **Only what is safe to take back** (Inflight.DropOldest): a delivery on the
// wire to a connected client stays until it is acknowledged, and so does
// anything at QoS 2 or anything saguin wrote itself.
func (s *Server) boundSession(cl *Client, keep uint16) {
	max := s.Options.ClientSessionQueueBytes
	if max <= 0 || cl.State.Inflight.Bytes() <= max {
		return
	}
	dropped := cl.State.Inflight.DropOldest(max, keep, cl.Closed())
	if len(dropped) == 0 {
		return
	}
	s.Info.SessionQueueDropped.Add(int64(len(dropped)))
	// Nothing is keyed by what the bound gives up - it is none of saguin's own
	// (Inflight.droppable) - so the identifiers DropOldest reserved are free
	// at once.
	for _, m := range dropped {
		cl.State.Inflight.Unclaim(m.PacketID)
	}
	s.Log.Debug("dropped the oldest deliveries a session held: it reached limits.session_queue_bytes",
		"client", cl.ID, "dropped", len(dropped), "session_queue_bytes", max)
}

func (s *Server) publishRetainedToClient(cl *Client, sub packets.Subscription, existed bool) {
	if IsSharedFilter(sub.Filter) {
		return // 4.8.2 Non-normative - Shared Subscriptions - No Retained Messages are sent to the Session when it first subscribes.
	}

	if sub.RetainHandling == 1 && existed || sub.RetainHandling == 2 { // [MQTT-3.3.1-10] [MQTT-3.3.1-11]
		return
	}

	sub.FwdRetainedFlag = true
	for _, pkv := range s.Topics.Messages(sub.Filter) { // [MQTT-3.8.4-4]
		_, err := s.publishToClient(cl, sub, pkv)
		if err != nil {
			s.Log.Debug("failed to publish retained message", "error", err, "client", cl.ID, "listener", cl.Net.Listener, "packet", pkv)
			continue
		}
	}
}

// buildAck builds a standardised ack message for Puback, Pubrec, Pubrel, Pubcomp packets.
func (s *Server) buildAck(packetID uint16, pkt, qos byte, properties packets.Properties, reason packets.Code) packets.Packet {
	// **An acknowledgement carries what this broker has to say and nothing
	// the client sent.** The properties handed in are the request's, and
	// they are dropped rather than copied forward.
	//
	// [MQTT-3.1.2-29] is what puts them in a different category here than
	// on a PUBLISH: a client setting Request Problem Information to 0 must
	// be sent no Reason String and no User Property on any packet except
	// PUBLISH, CONNACK and DISCONNECT. So on an acknowledgement they are
	// problem information the server is volunteering, which a client may
	// decline; on a PUBLISH they are application data being forwarded,
	// which is why that one is exempt. Section 3.4.2.2.3 says it from the
	// other end: the property is the sender's, and the sender of a PUBACK
	// is this broker.
	//
	// Echoing therefore put application data in the field the protocol
	// reserves for the broker's own diagnostics, and it helped nobody: a
	// publisher already knows the labels it sent. It cost two of Eclipse
	// Paho's tests, whose client stops reading an acknowledgement carrying
	// anything extra, so its own messages were never completed.
	//
	// mochi made this a NoInheritedPropertiesOnAck option, off by default,
	// and saguin set it on every time. It is the protocol's rule rather
	// than a deployment's preference, so it is no longer a lever.
	//
	// **A refusal's Reason String survives**, and that is the half to check
	// before changing this: it is set below, after the inherited set is
	// cleared, so what the broker says about a refusal still reaches the
	// client. What goes is only what the client sent.
	properties = packets.Properties{}
	if reason.Code >= packets.ErrUnspecifiedError.Code {
		properties.ReasonString = reason.Reason
	}

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: pkt,
			Qos:  qos,
		},
		PacketID:   packetID,    // [MQTT-2.2.1-5]
		ReasonCode: reason.Code, // [MQTT-3.4.2-1]
		Properties: properties,
		Created:    time.Now().Unix(),
		Expiry:     time.Now().Unix() + s.Options.Capabilities.MaximumMessageExpiryInterval,
	}

	return pk
}

// ownsInflight read-locks cl against a takeover of its session and reports
// whether cl still owns its in-flight table. The caller unlocks, and does not
// hold the lock across a hook: a hook can deliver to this client, which
// read-locks the same mutex, and a takeover waiting between the two would
// deadlock them.
//
// **An acknowledgement on a connection whose session has moved on changes
// nothing** (saguin), which is RFC 0003 "Sessions" rule 1 and the guard
// deliveryTarget keeps for deliveries. A takeover with Clean Start 0 copies
// the in-flight table under this lock and clears the old one afterwards. A
// PUBACK read on the old connection and retired between the two was acted on
// there while the copy re-sent the packet on the new connection, and if the
// new connection ended before the old one's hooks ran, a queue job went back
// with the attempt its PUBACK proved uncounted - reproduced with holds at
// the three points, the operations route then saying attempts 0. Under this
// lock the old connection either retires first, so the copy does not carry
// the packet, or finds itself taken over and leaves the packet to the new
// connection's own acknowledgement.
//
// **What a client's answer retired is marked Acknowledged for the hooks** that
// are told of it (OnDeliveryDone, for a refusing PUBREC), because they run
// after the lock is let go and a takeover can land before them. A keeper that
// asked IsTakenOver there kept the entry of a delivery its client had
// answered - right for what a takeover's copy carries, wrong for what it did
// not - and a restart sent the message again: reproduced with a hold between
// the unlock and the hooks.
func ownsInflight(cl *Client) bool {
	cl.State.handover.RLock()
	if cl.IsTakenOver() {
		cl.State.handover.RUnlock()
		return false
	}
	return true
}

// WhileOwned runs f while cl still has its session, under the read lock a
// takeover copies cl's in-flight table under (inheritClientSession), and
// reports whether it ran (saguin). A takeover therefore happens wholly before
// f - which then does not run, since the table is copied already - or wholly
// after, and carries whatever f registered to the new connection. It is how
// something outside the engine registers or retires a delivery on a
// connection it wrote from another goroutine, as the engine's own paths do
// under ownsInflight. f must take no lock of its own that is ever held while
// a takeover runs: the takeover's clearing of a table calls DeliveryKeeper
// hooks under the write lock.
func (cl *Client) WhileOwned(f func()) bool {
	if !ownsInflight(cl) {
		return false
	}
	defer cl.State.handover.RUnlock()
	f()
	return true
}

// Own is WhileOwned for a section that does not fit one function: it reports
// whether cl still has its session and, where it does, holds it so - under
// the same read lock - until release is called (saguin). The same rule holds
// for what runs before release.
func (cl *Client) Own() (release func(), ok bool) {
	if !ownsInflight(cl) {
		return nil, false
	}
	return cl.State.handover.RUnlock, true
}

// processPuback processes a Puback packet, denoting completion of a QOS 1 packet sent from the server.
func (s *Server) processPuback(cl *Client, pk packets.Packet) error {
	if !ownsInflight(cl) {
		return nil
	}
	retired := cl.State.Inflight.Retire(pk.PacketID) // [MQTT-4.3.2-5]
	if retired {
		cl.State.Inflight.IncreaseSendQuota()
	}
	cl.State.handover.RUnlock()
	if !retired {
		return nil // omit, but would be packets.ErrPacketIdentifierNotFound
	}

	// **The identifier is freed after OnQosComplete, not before.** What that
	// hook clears is keyed by it - the broadcast log's entry on the wire, a
	// channel delivery's offset - so a delivery claiming it in between would
	// have the clearing land on its own (Inflight.Retire).
	s.hooks.OnQosComplete(cl, pk)
	cl.State.Inflight.Unclaim(pk.PacketID)
	return nil
}

// processPubrec processes a Pubrec packet, denoting receipt of a QOS 2 packet sent from the server.
func (s *Server) processPubrec(cl *Client, pk packets.Packet) error {
	if !ownsInflight(cl) {
		return nil
	}
	held, ok := cl.State.Inflight.Get(pk.PacketID)
	if !ok { // [MQTT-4.3.3-7] [MQTT-4.3.3-13]
		cl.State.handover.RUnlock()
		return cl.WritePacket(s.buildAck(pk.PacketID, packets.Pubrel, 1, pk.Properties, packets.ErrPacketIdentifierNotFound))
	}

	if pk.ReasonCode >= packets.ErrUnspecifiedError.Code || !pk.ReasonCodeValid() { // [MQTT-4.3.3-4]
		retired := cl.State.Inflight.Retire(pk.PacketID)
		cl.State.handover.RUnlock()
		if retired {
			held.Acknowledged = true
			s.hooks.OnDeliveryDone(cl, held)
			cl.State.Inflight.Unclaim(pk.PacketID)
		}
		cl.ops.hooks.OnQosDropped(cl, pk)
		return nil // as per MQTT5 Section 4.13.2 paragraph 2
	}
	cl.State.handover.RUnlock()

	ack := s.buildAck(pk.PacketID, packets.Pubrel, 1, pk.Properties, packets.CodeSuccess) // [MQTT-4.3.3-4] ![MQTT-4.3.3-6]
	// The PUBREL carries on in the in-flight table for the delivery it
	// replaces, so it keeps that delivery's origin - how a DeliveryKeeper
	// tells a delivery's PUBREL from anything else there. Never encoded.
	ack.Origin = held.Origin
	// A PUBREL tells the client the exchange has moved on, so it goes only
	// once a keeper has stored that. Where one could not, the entry stays the
	// PUBLISH and the connection ends (0x80, or a close below MQTT 5): the
	// PUBLISH may be sent again only when the session resumes
	// (MQTT-4.4.0-1), and the client's PUBREC to it asks again.
	if err := s.hooks.OnDeliveryReleased(cl, held); err != nil {
		return packets.ErrUnspecifiedError
	}
	// Asked again: the hook is a store write and runs without the lock, and a
	// takeover that copied the PUBLISH meanwhile has it re-sent on the new
	// connection, whose own PUBREC is answered there.
	if !ownsInflight(cl) {
		return nil
	}
	cl.State.Inflight.DecreaseReceiveQuota() // -1 RECV QUOTA
	cl.State.Inflight.Set(ack)               // [MQTT-4.3.3-5]
	cl.State.handover.RUnlock()
	return cl.WritePacket(ack)
}

// processPubrel processes a Pubrel packet, denoting completion of a QOS 2 packet sent from the client.
func (s *Server) processPubrel(cl *Client, pk packets.Packet) error {
	if _, ok := cl.State.Inflight.Get(pk.PacketID); !ok { // [MQTT-4.3.3-7] [MQTT-4.3.3-13]
		return cl.WritePacket(s.buildAck(pk.PacketID, packets.Pubcomp, 0, pk.Properties, packets.ErrPacketIdentifierNotFound))
	}

	if pk.ReasonCode >= packets.ErrUnspecifiedError.Code || !pk.ReasonCodeValid() { // [MQTT-4.3.3-9]
		cl.State.Inflight.Delete(pk.PacketID)
		cl.ops.hooks.OnQosDropped(cl, pk)
		return nil
	}

	ack := s.buildAck(pk.PacketID, packets.Pubcomp, 0, pk.Properties, packets.CodeSuccess) // [MQTT-4.3.3-11]
	cl.State.Inflight.Set(ack)

	err := cl.WritePacket(ack)
	if err != nil {
		return err
	}

	cl.State.Inflight.IncreaseReceiveQuota()             // +1 RECV QUOTA
	cl.State.Inflight.IncreaseSendQuota()                // +1 SENT QUOTA
	if ok := cl.State.Inflight.Delete(pk.PacketID); ok { // [MQTT-4.3.3-12]
		s.hooks.OnQosComplete(cl, pk)
	}

	return nil
}

// processPubcomp processes a Pubcomp packet, denoting completion of a QOS 2 packet sent from the server.
func (s *Server) processPubcomp(cl *Client, pk packets.Packet) error {
	if !ownsInflight(cl) {
		return nil
	}
	// Whether the PUBCOMP is a success or a failure, the QoS flow ends and
	// the entry goes. **The quotas come back only with an entry that held
	// them**, as processPuback's does: a QoS 2 delivery that expired in
	// flight gave its send slot back at the sweep, and the client's late
	// PUBREC was answered PUBREL 0x92 with no entry and no receive slot
	// taken. Restored unconditionally, the PUBCOMP to that PUBREL gave both
	// back a second time, and the broker could put one more delivery in
	// flight than the client's Receive Maximum allows [MQTT-3.3.4-9].
	retired := cl.State.Inflight.Retire(pk.PacketID)
	if retired {
		cl.State.Inflight.IncreaseReceiveQuota() // +1 RECV QUOTA
		cl.State.Inflight.IncreaseSendQuota()    // +1 SENT QUOTA
	}
	cl.State.handover.RUnlock()
	if retired {
		s.hooks.OnQosComplete(cl, pk) // and then the identifier, as processPuback
		cl.State.Inflight.Unclaim(pk.PacketID)
	}

	return nil
}

// processSubscribe processes a Subscribe packet.
func (s *Server) processSubscribe(cl *Client, pk packets.Packet) error {
	// **Handled holding the client id's session lock, up to the SUBACK**
	// (saguin). A SUBSCRIBE is read on its own connection and what it
	// changes is kept by client id - the topic index here, the session's
	// record in a hook - so one read before a takeover and handled after it
	// changed the new connection's: a filter the new session never asked
	// for went into the index under its id, and its record lost the
	// subscriptions it had made. Under the lock a connection is either not
	// yet taken over, and its subscription goes with the session it hands
	// over, or taken over for good, and nothing is changed. Not across the
	// SUBACK, a write bounded only by the write deadline.
	unlock := s.sessions.lock(cl.ID)
	if cl.IsTakenOver() {
		unlock()
		return packets.ErrSessionTakenOver
	}
	code := packets.CodeSuccess
	if pki, ok := cl.State.Inflight.Get(pk.PacketID); ok && pki.FixedHeader.Type == packets.Pubrec {
		code = packets.ErrPacketIdentifierInUse
	}

	// **The engine's own refusals are decided once, before the hook, and
	// handed to it** (saguin): an identifier in use, a filter that is not
	// one, No Local on a shared subscription, and the read rule. A hook that
	// stores the session (OnSubscribe) stores only what neither refused, and
	// the SUBACK answers with the codes it hands back - so no second check
	// can disagree with what was stored. Asked again after the hook, as it
	// was, the read rule could change in between (an acl_file reloaded) and
	// a filter the store kept was answered 0x87 (invariant 18).
	decided := make([]byte, len(pk.Filters))
	for i, sub := range pk.Filters {
		switch {
		case code != packets.CodeSuccess:
			decided[i] = code.Code // NB 3.9.3 Non-normative 0x91
		case !IsValidFilter(sub.Filter, false):
			decided[i] = packets.ErrTopicFilterInvalid.Code
		case sub.NoLocal && IsSharedFilter(sub.Filter):
			decided[i] = packets.ErrProtocolViolationInvalidSharedNoLocal.Code // [MQTT-3.8.3-4]
		case !s.hooks.OnACLCheck(cl, sub.Filter, false):
			// A refusal says why. mochi had an ObscureNotAuthorized mode
			// that answered 0x83 here instead, for deployments that would
			// rather not tell a client which filters exist; saguin never
			// set it, and a broker whose authorization is grant-only
			// (RFC 0001) leaks nothing by naming the reason.
			decided[i] = packets.ErrNotAuthorized.Code
		}
	}
	pk.ReasonCodes = decided
	pk = s.hooks.OnSubscribe(cl, pk)
	// A hook answers with its own codes, the engine's among them; one that
	// hands back none leaves the engine's.
	if len(pk.ReasonCodes) == len(pk.Filters) {
		decided = pk.ReasonCodes
	}

	filterExisted := make([]bool, len(pk.Filters))
	reasonCodes := make([]byte, len(pk.Filters))
	for i, sub := range pk.Filters {
		if code != packets.CodeSuccess {
			reasonCodes[i] = code.Code // NB 3.9.3 Non-normative 0x91
			continue
		} else if decided[i] >= packets.ErrUnspecifiedError.Code {
			// Refused by the engine above or by a hook, which says why.
			// [MQTT-3.9.3-1]
			reasonCodes[i] = decided[i]
		} else {
			isNew := s.Topics.Subscribe(cl.ID, sub) // [MQTT-3.8.4-3]
			if isNew {
				s.Info.Subscriptions.Add(1)
			}
			cl.State.Subscriptions.Add(sub.Filter, sub) // [MQTT-3.2.2-10]

			if sub.Qos > s.Options.Capabilities.MaximumQos {
				sub.Qos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9]
			}

			filterExisted[i] = !isNew
			reasonCodes[i] = sub.Qos // [MQTT-3.9.3-1] [MQTT-3.8.4-7]
		}
	}

	// **Every refusal is counted here, once, and before a 3.1.1 client's is
	// written as 0x80** (saguin_subscriptions_refused_total, RFC 0005). The
	// codes are all decided by now - the hooks', this function's own, and
	// 0x91 for an identifier in use - so one place sees them all.
	//
	// The downgrade is after the loop rather than inside it so that it
	// reaches every code: inside, the 0x91 branch skipped it and a 3.1.1
	// client was sent a code its protocol does not have.
	for i, c := range reasonCodes {
		if c >= packets.ErrUnspecifiedError.Code {
			s.Info.SubscribeRefused[c].Add(1)
		}
		if c > packets.CodeGrantedQos2.Code && cl.Properties.ProtocolVersion < 5 { // MQTT3
			reasonCodes[i] = packets.ErrUnspecifiedError.Code
		}
	}

	ack := packets.Packet{ // [MQTT-3.8.4-1] [MQTT-3.8.4-5]
		FixedHeader: packets.FixedHeader{
			Type: packets.Suback,
		},
		PacketID:    pk.PacketID, // [MQTT-2.2.1-6] [MQTT-3.8.4-2]
		ReasonCodes: reasonCodes, // [MQTT-3.8.4-6]
		Properties: packets.Properties{
			User: pk.Properties.User,
		},
	}

	if code.Code >= packets.ErrUnspecifiedError.Code {
		ack.Properties.ReasonString = code.Reason
	}

	s.hooks.OnSubscribed(cl, pk, reasonCodes)
	unlock()
	err := cl.WritePacket(ack)
	if err != nil {
		return err
	}

	for i, sub := range pk.Filters { // [MQTT-3.3.1-9]
		if reasonCodes[i] >= packets.ErrUnspecifiedError.Code {
			continue
		}

		s.publishRetainedToClient(cl, sub, filterExisted[i])
	}

	return nil
}

// processUnsubscribe processes an unsubscribe packet.
func (s *Server) processUnsubscribe(cl *Client, pk packets.Packet) error {
	code := packets.CodeSuccess
	if pki, ok := cl.State.Inflight.Get(pk.PacketID); ok && pki.FixedHeader.Type == packets.Pubrec {
		code = packets.ErrPacketIdentifierInUse
	}

	// Holding the client id's session lock, up to the UNSUBACK, for the
	// reason processSubscribe gives (saguin).
	unlock := s.sessions.lock(cl.ID)
	if cl.IsTakenOver() {
		unlock()
		return packets.ErrSessionTakenOver
	}
	// **An identifier in use refuses the packet before the hook sees it**
	// (saguin), and the hook is handed that refusal, so a hook that stores
	// the session (OnUnsubscribe) does not store an unsubscribe the engine
	// then refuses: answered 0x91 and still subscribed, a session whose
	// record had forgotten the filter lost it at the next restart.
	if code != packets.CodeSuccess {
		pk.ReasonCodes = make([]byte, len(pk.Filters))
		for i := range pk.ReasonCodes {
			pk.ReasonCodes[i] = code.Code
		}
	}
	pk = s.hooks.OnUnsubscribe(cl, pk)
	reasonCodes := make([]byte, len(pk.Filters))
	// **A filter a hook refused stays subscribed** (saguin): an OnUnsubscribe
	// hook that could not store the session without it says so with a
	// reason code, as processSubscribe lets one, and the filter is left
	// exactly as it was - the UNSUBACK saying it is gone would be a promise
	// a crash breaks (invariant 18). Only what was applied reaches
	// OnUnsubscribed.
	refusedByHook := len(pk.ReasonCodes) == len(pk.Filters)
	applied := pk
	applied.Filters = make(packets.Subscriptions, 0, len(pk.Filters))
	refused := false
	for i, sub := range pk.Filters { // [MQTT-3.10.4-6] [MQTT-3.11.3-1]
		if code != packets.CodeSuccess {
			reasonCodes[i] = code.Code // NB 3.11.3 Non-normative 0x91
			continue
		}
		if refusedByHook && pk.ReasonCodes[i] >= packets.ErrUnspecifiedError.Code {
			reasonCodes[i] = pk.ReasonCodes[i]
			refused = true
			continue
		}
		applied.Filters = append(applied.Filters, sub)

		if q := s.Topics.Unsubscribe(sub.Filter, cl.ID); q {
			s.Info.Subscriptions.Add(-1)
			reasonCodes[i] = packets.CodeSuccess.Code
		} else {
			reasonCodes[i] = packets.CodeNoSubscriptionExisted.Code
		}

		cl.State.Subscriptions.Delete(sub.Filter) // [MQTT-3.10.4-2] ~[MQTT-3.10.4-3]
	}

	ack := packets.Packet{ // [MQTT-3.10.4-4]
		FixedHeader: packets.FixedHeader{
			Type: packets.Unsuback,
		},
		PacketID:    pk.PacketID, // [MQTT-2.2.1-6]  [MQTT-3.10.4-5]
		ReasonCodes: reasonCodes, // [MQTT-3.11.3-2]
		Properties: packets.Properties{
			User: pk.Properties.User,
		},
	}

	if code.Code >= packets.ErrUnspecifiedError.Code {
		ack.Properties.ReasonString = code.Reason
	}

	s.hooks.OnUnsubscribed(cl, applied)
	unlock()
	// **3.1.1 has no code to refuse a filter with** - its UNSUBACK carries
	// none - so a refusal ends the connection rather than being answered
	// with an UNSUBACK that says every filter is gone.
	if refused && cl.Properties.ProtocolVersion < 5 {
		return packets.ErrImplementationSpecificError
	}
	return cl.WritePacket(ack)
}

// UnsubscribeClient unsubscribes a client from all of their subscriptions.
func (s *Server) UnsubscribeClient(cl *Client) {
	i := 0
	filterMap := cl.State.Subscriptions.GetAll()
	filters := make([]packets.Subscription, len(filterMap))
	for k := range filterMap {
		cl.State.Subscriptions.Delete(k)
	}

	if cl.IsTakenOver() {
		return
	}

	for k, v := range filterMap {
		if s.Topics.Unsubscribe(k, cl.ID) {
			s.Info.Subscriptions.Add(-1)
		}
		filters[i] = v
		i++
	}
	s.hooks.OnUnsubscribed(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe}, Filters: filters})
}

// processAuth accepts a well-formed AUTH packet and does nothing with it.
//
// AUTH belongs to MQTT 5's extended authentication exchange: a client names
// an Authentication Method in its CONNECT and the two sides trade AUTH
// packets before the CONNACK. saguin implements no such method, and refuses
// any CONNECT that names one with 0x8C before a session exists - so no
// client of this broker can be in that exchange, and an AUTH arriving here
// came from one that never entered it.
//
// **This is what the broker did before, kept deliberately rather than
// arrived at.** The engine used to hand the packet to an OnAuthPacket hook
// and return whatever came back; no hook ever provided it, so every AUTH
// was accepted and ignored. Removing the hook removed the indirection and
// not the behaviour. Whether a stray AUTH should instead be answered with a
// protocol error is a question about the protocol rather than about this
// removal, and it is not settled here.
func (s *Server) processAuth(cl *Client, pk packets.Packet) error {
	return nil
}

// processDisconnect processes a Disconnect packet.
func (s *Server) processDisconnect(cl *Client, pk packets.Packet) error {
	// Ended as it was read: the client has closed its side [MQTT-3.14.4-1]
	// and may connect again while this records it.
	cl.beginEnd()

	// **Under the id's session lock, and only from a connection that still
	// has the session** (saguin). A DISCONNECT changes two things a CONNECT
	// taking the session over decides by: its Session Expiry Interval, which
	// the takeover reads under that lock to decide whether the session ends
	// there (Client.EndsWithConnection), and, being clean, the Will, which
	// it withdraws where a connection taken over has its Will decided by the
	// takeover (OnWill). Written from the read loop without the lock, an
	// interval of 0 landing inside a CONNECT's claim had the broker resume
	// the session while the engine began a new one: the old session's
	// in-flight table left under the id, and sent to the new session after a
	// restart. A connection taken over is not read further, as a CONNECT
	// passed over is not: it ends as the closed connection it is, and its
	// Will is decided as a taken-over connection's (OnWill). mosquitto
	// handles every packet on one thread, so a DISCONNECT there is wholly
	// before a takeover or never read.
	unlock := s.sessions.lock(cl.ID)
	if cl.IsTakenOver() {
		unlock()
		return packets.ErrSessionTakenOver
	}
	if pk.Properties.SessionExpiryIntervalFlag &&
		pk.Properties.SessionExpiryInterval > 0 && cl.Properties.Props.SessionExpiryInterval == 0 {
		unlock()
		return packets.ErrProtocolViolationZeroNonZeroExpiry
	}
	// **Claimed before anything the DISCONNECT changes is stored, and held
	// through the store and the close** (Client.claimEnd): a shutdown or a
	// hang-up that won the end closed the connection while OnDisconnecting
	// was storing, so the client saw its close before its Will was withdrawn
	// and its expiry kept, and a crash then published the Will (invariant
	// 18). Claimed under the id's session lock, which a takeover holds while
	// it ends the connection, so no goroutine waiting for this end holds it.
	// An end another goroutine owns is not this DISCONNECT's: nothing is
	// stored for it, and the connection ends as that end decided.
	if !cl.claimEnd() {
		unlock()
		cl.awaitEnd()
		return ErrConnectionClosed
	}
	if pk.Properties.SessionExpiryIntervalFlag {
		// **Held to the cap a CONNECT is held to** (Capabilities.
		// MaximumSessionExpiryInterval). Taken as sent, a DISCONNECT
		// carrying 4,000,000,000 kept the session past
		// limits.max_session_expiry, which only the CONNECT enforced. EMQX clamps it the same way at both;
		// mosquitto has no maximum to clamp to. There is no answer to a
		// DISCONNECT to say so in, so the client is not told - as a server
		// may end a session early by its own policy.
		expiry := pk.Properties.SessionExpiryInterval
		if max := s.Options.Capabilities.MaximumSessionExpiryInterval; expiry > max {
			expiry = max
		}
		cl.Properties.Props.SessionExpiryInterval = expiry
		cl.Properties.Props.SessionExpiryIntervalFlag = true
	}
	// **Stored before the close, under the same lock** (saguin): the close
	// answering a clean DISCONNECT tells the client its Will is withdrawn and
	// its expiry is the one it just sent, and a hook storing that after the
	// close - from OnDisconnect, once the read loop has returned - left a
	// window in which a crash published the Will anyway and kept the session
	// for the CONNECT's expiry (invariant 18).
	s.hooks.OnDisconnecting(cl, pk)
	unlock()

	if pk.ReasonCode == packets.CodeDisconnectWillMessage.Code { // section 3.1.2.5, a non-normative comment
		cl.finishEnd(packets.CodeDisconnectWillMessage)
		return packets.CodeDisconnectWillMessage
	}

	cl.finishEnd(packets.CodeDisconnect) // [MQTT-3.14.4-2]

	return nil
}

// refuseConnection ends an established connection for a refusal the engine
// made (saguin), so the refusal is counted as well as answered:
// OnConnectionRefused is told, and then DisconnectClient ends it. Told first,
// as saguin counts its own refusals, so a client that has seen its
// connection close finds the refusal counted.
//
// **Once per connection, asked of the connection.** One whose end another
// goroutine owns (Client.claimEnd) is that goroutine's to end - a hook's own
// refusal, which counted itself, a takeover, a shutdown - and is neither
// answered nor counted again: this waits for its close.
func (s *Server) refuseConnection(cl *Client, code packets.Code) error {
	s.refuse(cl, code)
	return code
}

// refuse is refuseConnection, reporting whether this call owned the end.
func (s *Server) refuse(cl *Client, code packets.Code) bool {
	if !cl.claimEnd() {
		cl.awaitEnd()
		return false
	}
	s.hooks.OnConnectionRefused(cl, code)
	_ = s.disconnectOwned(cl, code)
	return true
}

// DisconnectClient sends a Disconnect packet to a client and then closes the
// client connection - where its end has no owner yet, and otherwise waits
// for the owner to close it (Client.claimEnd).
func (s *Server) DisconnectClient(cl *Client, code packets.Code) error {
	// **A shutdown and a takeover never wait on an unbounded write**
	// (invariant 16): where nothing bounds the connection's writes, the
	// owner's, or the one holding the client's lock, is cut so that it fails
	// and the owner finishes (Client.cutWrites).
	cuts := code == packets.ErrServerShuttingDown || code == packets.ErrSessionTakenOver
	if !cl.claimEnd() {
		if cuts {
			cl.cutWrites()
		}
		cl.awaitEnd()
		if code.Code >= packets.ErrUnspecifiedError.Code {
			return code
		}
		return nil
	}
	if cuts && cl.writeUnderWay() {
		cl.cutWrites()
	}
	return s.disconnectOwned(cl, code)
}

// closeGoverned ends, for a shutdown, a connection its listener holds that
// may not be registered yet, writing it nothing: no CONNACK has told it the
// session it would be ending (Listeners.Govern). Its end's owner, where it
// has one - the goroutine refusing its CONNECT - has its write cut where
// nothing bounds it, and is waited for.
func (s *Server) closeGoverned(cl *Client) {
	if !cl.claimEnd() {
		cl.cutWrites()
		cl.awaitEnd()
		return
	}
	cl.finishEnd(packets.ErrServerShuttingDown)
}

// disconnectOwned writes the DISCONNECT for the owner of the connection's end
// and closes the connection, whether or not the write succeeded.
func (s *Server) disconnectOwned(cl *Client, code packets.Code) error {
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Disconnect,
		},
		ReasonCode: code.Code, // [MQTT-3.14.2-1]
		Properties: packets.Properties{},
	}

	if code.Code >= packets.ErrUnspecifiedError.Code {
		out.Properties.ReasonString = code.Reason
	}

	// The server-to-client Disconnect packet was introduced in MQTT v5. In
	// v3.1.1 and v3.1 it flows client to server only, so a v3 client is
	// closed without one rather than being sent a packet type its own
	// specification does not define in that direction.
	//
	// We already have a code we are using to disconnect the client, so we are not
	// interested if the write packet fails due to a closed connection (as we are closing it).
	var err error
	told := cl.Properties.ProtocolVersion >= 5
	if told {
		err = cl.WritePacket(out)
	}

	// The connection is closed here whether or not the client was told, and
	// that is the specification's side rather than a choice.
	//
	// mochi had a PassiveClientDisconnect mode meaning "the client was sent
	// a DISCONNECT and will close the connection itself, so do not force
	// it", for a Paho client that objected to being hung up on. Nothing in
	// saguin set it, and it is a spec violation on its own terms: a server
	// that has decided to end a connection and then waits for the client to
	// agree is one a client can keep open by doing nothing. A v3 client is
	// sent no packet at all, so under that mode DisconnectClient did
	// nothing whatsoever.
	cl.finishEnd(code)
	if code.Code >= packets.ErrUnspecifiedError.Code {
		return code
	}

	return err
}

// Close attempts to gracefully shut down the server, all listeners, clients, and stores.
func (s *Server) Close() error {
	// Under the gate's lock, so that from here nothing is admitted and no
	// slot is handed to a waiter (arrive, handOn).
	s.gate.mu.Lock()
	s.gate.stopping = true
	s.gate.mu.Unlock()
	close(s.done)
	s.Log.Info("gracefully stopping server")
	s.Listeners.CloseAll(s.closeListenerClients)
	s.hooks.Stop()

	s.Log.Info("saguin mqtt engine stopped")
	return nil
}

// closeListenerClients closes all clients on the specified listener.
func (s *Server) closeListenerClients(listener string) {
	clients := s.Clients.GetByListener(listener)
	for _, cl := range clients {
		_ = s.DisconnectClient(cl, packets.ErrServerShuttingDown)
	}
}

// sendLWT issues an LWT message to a topic when a client disconnects.
func (s *Server) sendLWT(cl *Client) {
	if atomic.LoadUint32(&cl.Properties.Will.Flag) == 0 {
		return
	}

	modifiedLWT := s.hooks.OnWill(cl, cl.Properties.Will)

	// **A Will its hook has taken leaves nothing armed here** (saguin). A
	// hook that publishes or holds the Will itself hands it back with no
	// topic, and MQTT gives a Will no topicless form, so there is nothing
	// for the engine to publish or to wait for. Deciding the delay from the
	// connection's own Will instead queued the emptied packet on the delayed
	// path the engine then had, and a takeover's CONNACK cleared that path
	// only for entries already there: an old connection's late one survived,
	// and when it fell due it cleared the Will of the connection that now
	// held the id. That connection then died with no Will - 14 of 40 runs of
	// a clean start whose Will had no delay, after the old one's had passed.
	if modifiedLWT.TopicName == "" {
		atomic.StoreUint32(&cl.Properties.Will.Flag, 0) // [MQTT-3.1.2-10]
		return
	}

	// **The engine holds no Will for later** (saguin). A delay is the hook's
	// to wait out: saguin's OnWill keeps a delayed Will on the session record
	// and hands back none. The engine's own delayed path published past every
	// hook and, keyed by client id alone, cleared the Will of whichever
	// connection held the id when it fell due; nothing in saguin reached it,
	// and it was removed. A delayed Will no hook took is not published early,
	// which would announce a death its delay exists to wait out, and not
	// kept, since nothing here would ever fire it: it is dropped, and said so.
	// The Will itself is not logged: its topic and payload are the client's.
	if delay := modifiedLWT.WillDelayInterval; delay > 0 {
		s.Log.Error("a Will with a delay reached the engine, which holds none: it is not published",
			"client", cl.ID, "will_delay_interval", delay)
		atomic.StoreUint32(&cl.Properties.Will.Flag, 0)
		return
	}

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Retain: modifiedLWT.Retain, // [MQTT-3.1.2-14] [MQTT-3.1.2-15]
			Qos:    modifiedLWT.Qos,
		},
		TopicName: modifiedLWT.TopicName,
		Payload:   modifiedLWT.Payload,
		Properties: packets.Properties{
			User: modifiedLWT.User,
		},
		Origin:  cl.ID,
		Created: time.Now().Unix(),
	}

	if pk.FixedHeader.Retain {
		s.retainMessage(cl, pk)
	}

	s.publishToSubscribers(pk)                      // [MQTT-3.1.2-8]
	atomic.StoreUint32(&cl.Properties.Will.Flag, 0) // [MQTT-3.1.2-10]
}

// clearExpiredClients deletes all clients which have been disconnected for longer
// than their given expiry intervals.
//
// **The session goes whole: its subscriptions and its in-flight messages with
// the client.** Deleting the client alone left its subscriptions in the topic
// index, so a shared group went on selecting a member that no longer existed
// and the substrate, finding nobody under that id, dropped the message.
// Measured: a group of three with one member expired delivered 41 to 48 of 60
// publishes. Every other site that discards a session already did all three,
// and TestEverySiteThatDeletesAClientDiscardsItsWholeSession holds them to it.
//
// **Under the id's session lock**, so a CONNECT for the same id either
// finishes before the sweep looks again or waits until the session is gone
// and is told Session Present = 0. The check is repeated inside the lock
// because the table was read before it.
//
// **It collects the expired ones and copies nothing else.** The table used to
// be copied whole every second to find, usually, no expired client: at ten
// thousand idle sessions that garbage was what kept the process's memory
// twice what the sessions hold. The judging runs under the table's read
// lock, which is safe because sessionExpired reads one atomic and takes no
// lock; the deleting cannot, since it takes the session lock and the write
// lock, so it runs after the walk on the few collected.
func (s *Server) clearExpiredClients(dt time.Time) {
	var expired []*Client
	s.Clients.Each(func(client *Client) {
		if s.sessionExpired(client, dt) {
			expired = append(expired, client)
		}
	})
	for _, client := range expired {
		id := client.ID
		unlock := s.sessions.lock(id)
		// The client judged expired, not whatever holds the id by now: a
		// device reconnecting between the check and here stays connected.
		if current, ok := s.Clients.Get(id); ok && current == client && s.sessionExpired(client, dt) {
			s.hooks.OnClientExpired(client)
			s.UnsubscribeClient(client)
			client.ClearInflights()
			s.Clients.DeleteIf(id, client) // [MQTT-4.1.0-2]
		}
		unlock()
	}
}

// sessionExpired says whether a disconnected client's session has outlived
// its Session Expiry Interval at the moment dt.
//
// One function for two callers, which is the point: the sweep asks it on a
// ticker and inheritClientSession asks it when a client comes back, and a
// session that is gone for one has to be gone for the other. Two copies of
// this arithmetic would be two chances to disagree about whether a session
// exists, and the client would see whichever one it happened to reach.
//
// A connected client never expires: StopTime is zero until it disconnects.
//
// **The comparison includes the boundary, on real instants.** "When the
// Session Expiry Interval has passed" is passed at `disconnected + expire`,
// not a second later, and the strict `<` this replaced kept a two-second
// session alive into its third second.
//
// **Whole seconds made that inclusive boundary end sessions early**, which
// is worse than the lateness it was fixing: the moment was floored when it
// was stored and the clock was floored when it was read, so a client that
// disconnected at x.974 with a two-second interval was judged expired when
// the clock reached x+2 - 1.892 seconds of a two-second session, measured
// in a conformance run, and 1.001 in the worst case. A client reconnecting
// inside its own interval found its subscriptions and queued messages
// gone. Both ends are instants now, and the interval is added to the
// moment rather than compared second against second.
//
// **This is store.SessionExpired's rule, deliberately identical**: the
// running broker and the store decide the same session, one while it is
// held in memory and one across a restart, and a session that ends at
// 2.000 for one has to end at 2.000 for the other.
func (s *Server) sessionExpired(cl *Client, dt time.Time) bool {
	disconnected := cl.StopTime()
	if disconnected.IsZero() {
		return false
	}

	expire := s.Options.Capabilities.MaximumSessionExpiryInterval
	if cl.Properties.ProtocolVersion == 5 && cl.Properties.Props.SessionExpiryIntervalFlag {
		expire = cl.Properties.Props.SessionExpiryInterval
	}

	return !dt.Before(disconnected.Add(time.Duration(expire) * time.Second))
}

// retainedSweepJudged is a test seam: called between judging a retained value expired and deleting it.
var retainedSweepJudged func(topic string)

// clearExpiredRetainedMessages deletes retained messages from topics if they have expired.
//
// **It collects the expired filters and copies nothing else.** It used to
// copy every retained message, once a second, to find what is usually none:
// garbage proportional to the whole store, on an idle broker. The test runs
// under the store's read lock, where it is pure; the deletes run after it.
func (s *Server) clearExpiredRetainedMessages(now int64) {
	expiredAt := func(pk packets.Packet) bool {
		expired := pk.ProtocolVersion == 5 && pk.Expiry > 0 && pk.Expiry < now // [MQTT-3.3.2-5]

		// If the maximum message expiry interval is set (greater than 0), and the message
		// retention period exceeds the maximum expiry, the message will be forcibly removed.
		enforced := s.Options.Capabilities.MaximumMessageExpiryInterval > 0 &&
			now-pk.Created > s.Options.Capabilities.MaximumMessageExpiryInterval

		return expired || enforced
	}

	var gone []string
	s.Topics.Retained.Each(func(filter string, pk packets.Packet) {
		if expiredAt(pk) {
			gone = append(gone, filter)
		}
	})
	for _, filter := range gone {
		if retainedSweepJudged != nil {
			retainedSweepJudged(filter)
		}
		// The walk above is only a shortlist. A retained publish on this
		// topic may have replaced the value since, so the judgement is made
		// again on what is stored, under the lock that stores it, and only
		// then is anything removed.
		s.Topics.ExpireRetained(filter, expiredAt)
	}
}

// clearExpiredInflights deletes any inflight messages which have expired.
//
// **Only the clients with something expired are taken out of the table.**
// The walk asks each client's own in-flight table under the Clients read
// lock (Expired allocates only for what it finds), and the clearing, which
// reaches the hooks, runs after it. It used to copy the whole client table
// every second.
func (s *Server) clearExpiredInflights(now int64) {
	limit := s.Options.Capabilities.MaximumMessageExpiryInterval
	var due []*Client
	s.Clients.Each(func(client *Client) {
		if len(client.State.Inflight.Expired(now, limit)) > 0 {
			due = append(due, client)
		}
	})
	for _, client := range due {
		if deleted := client.ClearExpiredInflights(now, limit); len(deleted) > 0 {
			for _, id := range deleted {
				s.hooks.OnQosDropped(client, packets.Packet{PacketID: id})
			}
		}
	}
}

// minimum differs from built-in min, it returns minimum of the non-zero value a and b.
// If both a and b are zero value, it reutrns 0.
func minimum(a, b int64) (m int64) {
	if a != 0 {
		m = a
		if b != 0 && b < a {
			m = b
		}
		return
	}

	if b != 0 {
		m = b
	}
	return
}
