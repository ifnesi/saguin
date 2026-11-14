// Package connection runs MQTT listeners and each socket's reader and writer.
// Readers check packets; the core decides replies; writers own orderly close.
package connection

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
)

// ReplySlots bounds packet completions per connection, including direct
// answers. The replies queue also reserves one delivery window for resumed
// PUBRELs and one final entry (R2).
const ReplySlots = 64

// NewPermits returns the shared connection bound, initially holding every
// permit. Main creates it once and passes it to all MQTT listeners (R6).
func NewPermits(n int) chan struct{} {
	permits := make(chan struct{}, n)
	for range n {
		permits <- struct{}{}
	}
	return permits
}

// socket is the transport used by a connection's reader and writer.
// *net.TCPConn, *net.UnixConn and *tls.Conn implement it.
type socket interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	CloseWrite() error
	Close() error
	RemoteAddr() net.Addr
}

// Clock supplies connection deadlines and the core loop's movable timer.
// At receives once Now reaches its argument, including an already due time.
// Timer receives in the same way; reset moves that same timer and drops a
// fire not yet received. Resetting to the zero time stops it. The timer's
// owner alone receives and resets it.
type Clock interface {
	Now() time.Time
	At(time.Time) <-chan time.Time
	Timer(time.Time) (c <-chan time.Time, reset func(time.Time))
}

// RealClock supplies wall time to connections and the core loop.
type RealClock struct{}

// Now returns the current wall time.
func (RealClock) Now() time.Time { return time.Now() }

// At returns a channel receiving once the wall clock reaches t.
func (RealClock) At(t time.Time) <-chan time.Time { return time.After(time.Until(t)) }

// Timer makes one time.Timer. Stopping and draining before Reset also drops
// an unread fire with asynchronous timer channels.
func (RealClock) Timer(t time.Time) (c <-chan time.Time, reset func(time.Time)) {
	timer := time.NewTimer(time.Until(t))
	reset = func(t time.Time) {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !t.IsZero() {
			timer.Reset(time.Until(t))
		}
	}
	return timer.C, reset
}

// Kind identifies an event on the core's shared inbound channel.
type Kind uint8

// A connection's events arrive in order: accepted, packets, end, close.
const (
	Accepted Kind = iota + 1
	Received
	Ended
	Closed
	Refused
	BytesRequested
	Delivered      // Packet is a Delivery chain or Release, returned after writing or discard
	Dialed         // Packet is DialResult; Conn is non-nil once TCP connected
	ControlWritten // Packet is a ControlWrite after a SUBSCRIBE's last byte
	DialStarted    // Packet is a bridge name; the first such event makes it started
)

// Accept carries the listener and the socket's address without formatting it.
// Only saving a durable session needs the address as text (storage.md).
type Accept struct {
	Listener string
	Bridge   string
	Remote   net.Addr
}

// Event transfers a checked packet or a body-byte request to the core.
// Received carries a CONNECT, PUBLISH, Ack, Subscribe, Unsubscribe or Pingreq; Refused carries a
// PublishRefusal that keeps the connection; Ended carries a valid client
// DISCONNECT or a ReadFailure, otherwise nil. Reason is the CONNECT or PUBLISH refusal, or
// the server DISCONNECT owed by a reader error. BytesRequested asks for one
// complete PUBLISH body before allocation or reading (R6).
// Delivered returns a Delivery chain or Release pointer after a write or
// discard, with each packet's result. A Delivery chain contains at most the
// socket write's writePackets packets and transfers ownership as one event.
// Closed is the writer's last act, after the raw socket is closed and the
// reader's Ended has been sent on this channel.
// Its Packet is a CannotBuild for a reply the writer could not build, nil
// otherwise; the core logs the reported error.
// Closure identifies a refusal or broker closure counted by this event's
// sender. That handover also publishes its selection time (ClosureAt) and
// CONNECT's named user (ConnectUser).
// Client departures and lost connections leave it zero.
type Event struct {
	Conn    *Conn
	Packet  any
	Bytes   int64           // PUBLISH reservation requested, or kept by this outcome
	Publish PublishMetadata // PUBLISH receipt and decoded message fields
	Closure Closure         // counted reason, without reading a metric value
	Kind    Kind
	Reason  byte
	Quota   uint32 // incoming QoS 2 exchange generation; not a packet identifier
}

// CannotBuild carries a reply build error on a Closed event's Packet. Other
// events need no error field in their inbound-channel value (R6).
type CannotBuild struct{ Err error }

// ControlWrite records the packet's completed socket write. The core uses
// that observation for a bridge's unanswered-SUBSCRIBE deadline, even when
// store backpressure delays the report's intake.
type ControlWrite struct {
	Packet *packet.Subscribe
	At     time.Time
}

// ReadFailure carries a transport-setup or certificate-name error on Ended.
// It uses the existing handover so the core alone admits runtime log lines.
type ReadFailure struct{ Err error }

// PublishMetadata carries receipt time and the message properties the core
// needs without a second property traversal. HasID distinguishes an absent
// saguin-id from the publisher's own empty value.
type PublishMetadata struct {
	ID       string
	Received int64  // Unix milliseconds when the complete body arrived
	Expiry   uint32 // seconds, including zero when Expires is true
	Expires  bool
	HasID    bool
	Reserved bool // policy below retained/channel capacity, decided by the core
}

// RequestKind identifies the checked channel request sent to the core.
type RequestKind uint8

// Checked channel request routes.
const (
	SeekRequest RequestKind = iota
	QueueResponse
	KVGetRequest
	CatalogueRequest
	DisconnectRequest
)

// RoutedPublish carries a bridge topic after substitution alongside the decoded
// peer packet. The core uses the ordinary publish path for both representations.
type RoutedPublish struct {
	Publish packet.Publish
	Topic   string
}

// RequestPublish is a checked service publication with its route and answer
// properties. Its body follows the ordinary PUBLISH ownership path.
type RequestPublish struct {
	Kind RequestKind
	packet.Publish
	Channel       int
	ResponseTopic string
	Correlation   []byte
}

// PublishRefusal is a checked PUBLISH that the reader refused. Its body is
// discarded before handover; QoS and PacketID select the writer's answer.
type PublishRefusal struct {
	UnmatchedTopic string // bridge peer topic, for core-owned runtime logging
	Kind           RequestKind
	Request        bool             // recognized request refused by reader policy
	Counter        *metrics.Counter // identifier-bearing refusals count after the core excludes a held repeat
	QoS            byte
	PacketID       uint16
}

// Ack identifies the wire type of an outgoing acknowledgement.
// PUBACK and PUBCOMP, and a refusing PUBREC, return the inbound send quota
// only after the writer has written them (MQTT 5, 4.9).
type Ack struct {
	Quota uint32 // incoming QoS 2 generation, never an outgoing delivery identifier
	Type  packet.Type
	packet.Ack
}

func validClientAck(typ packet.Type, reason byte) bool {
	return reason != 0x10 || (typ != packet.PUBACK && typ != packet.PUBREC)
}

// Release transfers an outgoing QoS 2 PUBREL to the writer and returns its
// write outcome on Delivered. A resumed release has no reader reply slot;
// its core-owned delivery descriptor bounds it instead.
type Release struct {
	Ack
	Slot    uint32
	After   uint64 // preceding PUBLISH hand-offs that recovery must keep ahead of this PUBREL
	Resumed bool
	Result  DeliveryResult
}

// NoReply returns a reserved reply slot without writing a packet.
type NoReply struct{}

// releasesWindow identifies the last reply of an incoming QoS exchange.
// A successful PUBREC keeps QoS 2 in flight until PUBCOMP is written.
func (a *Ack) releasesWindow() bool {
	return a.Type == packet.PUBACK || a.Type == packet.PUBCOMP ||
		(a.Type == packet.PUBREC && a.ReasonCode >= 0x80)
}

// Pingreq asks the core for one Pingresp on the reserved reply slot.
type Pingreq struct{}

// Pingresp is the writer's reply to a PINGREQ.
type Pingresp struct{}

// Close is the core's final entry, closing without another MQTT packet.
type Close struct{}

// Reply is a small queue entry: *packet.Connack, *packet.Disconnect, *Ack,
// *Release, *packet.Suback, *packet.Unsuback, NoReply, Pingresp or Close.
// Packet pointers transfer ownership of their contents.
type Reply any

// Conn is the core's handle to a socket. Its channels are bounded; none of
// its mutable state is shared between the reader and writer (R1, R3).
type Conn struct {
	permits        chan<- struct{}
	raw            socket
	stream         socket // MQTT framing, published by the reader before the writer uses it
	clock          Clock
	listener       string
	client         bool        // Sagüin is the MQTT client on a bridge; immutable before handover
	proxyAddr      net.Addr    // optional PROXY source, published with CONNECT like user and access
	user           string      // reader sets once before handing CONNECT to the core; immutable thereafter
	connectUser    string      // CONNECT's named user, proved or not; published by the closure handover
	access         *acl.Client // compiled by the reader before CONNECT handover; immutable thereafter
	accessFile     *acl.File   // generation of access, transferred with the same CONNECT
	replies        chan Reply
	deliveries     chan *Delivery
	slots          chan replySlot
	ackWrites      chan ackWrite     // receive-quota writes and successful CONNACK completion
	messageBytes   chan int64        // core grants one requested body at a time (R6)
	peer           chan peerSettings // reader publishes bridge setup, then CONNECT/CONNACK settings
	ended          chan readEnd      // reader sends its outcome once, before Ended
	stopping       chan struct{}     // writer closes to request that the reader stop
	readStopped    chan struct{}     // reader closes once reads and deadline updates stop
	endReported    chan struct{}     // reader closes only after its Ended send completes
	finalTaken     chan struct{}     // writer closes once it takes its final entry
	coreCut        chan struct{}     // core closes before cutting the socket, publishing cutCause
	cutCause       Closure           // immutable after coreCut closes
	readClosureAt  time.Time         // reader-owned until Ended publishes it, immutable afterwards
	writeClosureAt time.Time         // writer-owned until Closed publishes it, immutable afterwards
	pongs          chan time.Time    // bridge reader reports PINGRESP receipt to its writer
	established    chan struct{}     // bridge reader closes on a successful CONNACK
}

// transport is used only by the reader and writer. The raw socket remains
// immutable for the core's Cut and for the writer's unframed close/drain.
func (c *Conn) transport() socket {
	if c.stream != nil {
		return c.stream
	}
	return c.raw
}

// replySlot returns capacity after a reply is written or discarded.
// ackWrites alone settles the incoming window, independently of this slot's
// later return; Receive Maximum is fixed from the listener's configuration.
type replySlot struct{}

// ackWrite transfers the writer's progress without sharing mutable state.
// Each reserved reply slot can produce at most two notifications. The
// reader drains these after taking returned slots, before reusing any slot.
// Thus a reader waiting for bytes, a slot or a write leaves at most
// 2*ReplySlots notifications outstanding, and the writer always has room
// for both ends of every write, including a failure before socket handover.
type ackWrite struct {
	state    ackWriteState
	typ      packet.Type
	quota    uint32
	packetID uint16      // the client's identifier; typ distinguishes an unknown PUBCOMP from PUBACK
	held     []uint16    // CONNECT's restored receive quota; core transfers it once
	denied   acl.Feature // successful CONNACK's negotiated feature denials
}

type ackWriteState uint8

const (
	ackWriteBegun ackWriteState = iota
	ackWriteSucceeded
	ackWriteFailed
	ackConnackWritten // seals restoration, including an empty set, before receive admission
)

type peerSettings struct {
	version   packet.Version
	maxSize   uint32 // zero: only MQTT's size bound applies
	aliases   uint16
	connected bool          // bridge reader has received a successful CONNACK
	keepAlive time.Duration // effective client interval, before halving
}

// newConn is the shared channel and queue construction for accepted clients
// and dialled links. Neither transport needs another pair of packet loops.
func newConn(raw socket, clock Clock, limits *config.Limits) *Conn {
	c := &Conn{
		raw: raw, clock: clock,
		replies: make(chan Reply, ReplySlots+limits.MaxInflight+1), slots: make(chan replySlot, ReplySlots),
		deliveries: make(chan *Delivery, limits.MaxInflight),
		ackWrites:  make(chan ackWrite, 2*ReplySlots+1), messageBytes: make(chan int64, 1),
		peer: make(chan peerSettings, 1), ended: make(chan readEnd, 1),
		stopping: make(chan struct{}), readStopped: make(chan struct{}), endReported: make(chan struct{}),
		finalTaken: make(chan struct{}), coreCut: make(chan struct{}),
	}
	for range ReplySlots {
		c.slots <- replySlot{}
	}
	return c
}

// serve keeps worker accounting shared by listeners and bridge attempts.
func (r *reader) serve(setupErr error, workers *sync.WaitGroup) {
	defer workers.Done()
	r.metrics.readers.Add(1)
	defer r.metrics.readers.Add(-1)
	if setupErr != nil {
		r.failed(setupErr)
		if r.conn.client {
			r.conn.peer <- peerSettings{version: packet.V5}
		}
	} else {
		r.run()
	}
}

func (w *writer) serve(workers *sync.WaitGroup) {
	defer workers.Done()
	w.metrics.writers.Add(1)
	defer w.metrics.writers.Add(-1)
	w.run()
}

type readEnd struct {
	client         bool
	lost           bool
	connectTimeout bool
	tlsHandshake   bool
	proxyHeader    bool
	publishStalled bool // the PUBLISH body's silence deadline expired
}

// Listener returns the name of the listener that accepted c.
func (c *Conn) Listener() string { return c.listener }

// ClientRole reports the immutable MQTT role chosen before this connection
// is published. Outbound bridge sockets use the client side of the protocol.
func (c *Conn) ClientRole() bool { return c.client }

// User returns the proved user name, empty without proof. The core reads it
// only after receiving CONNECT: that channel handover publishes the reader's
// one assignment, and no later packet can change it.
func (c *Conn) User() string { return c.user }

// ConnectUser returns the user name CONNECT named, proved or not, or empty
// without one. The closure event publishes the reader's immutable assignment.
func (c *Conn) ConnectUser() string { return c.connectUser }

// ClosureAt returns the selection time published by the named Ended or
// Closed handover. Socket cleanup never changes that observation's time.
func (c *Conn) ClosureAt(kind Kind) time.Time {
	if kind == Ended {
		return c.readClosureAt
	}
	return c.writeClosureAt
}

// Access returns the immutable grants published with CONNECT, nil without an
// acl_file. Like User, it is read by the core only after that handover.
func (c *Conn) Access() *acl.Client { return c.access }

// AccessFile identifies the immutable generation used to compile Access.
// The CONNECT handover publishes both; neither changes afterwards.
func (c *Conn) AccessFile() *acl.File { return c.accessFile }

// RemoteAddr returns the socket peer's address.
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// ProxyAddr returns the PROXY source, nil without a specified IP address.
// Like User, the core reads it only after the CONNECT handover publishes the
// reader's one assignment. Accepted carries the socket's address independently.
func (c *Conn) ProxyAddr() net.Addr { return c.proxyAddr }

// Replies returns the writer's queue. Each spent slot bounds one entry.
// Resumed PUBRELs and the final entry have their own reserved capacity (R2).
// Its several senders must never close this channel (R3).
func (c *Conn) Replies() chan<- Reply { return c.replies }

// FinalTaken observes the writer's immutable final-entry notification without
// requiring a handover on the core's inbound channel.
func (c *Conn) FinalTaken() bool {
	select {
	case <-c.finalTaken:
		return true
	default:
		return false
	}
}

// Deliveries returns the second writer queue, bounded by max_inflight.
// The core counts the negotiated window before sending, so it never waits.
func (c *Conn) Deliveries() chan<- *Delivery { return c.deliveries }

// MessageBytes returns the core's grant channel. A reader has at most one
// request outstanding, so its one buffered answer never makes the core wait.
func (c *Conn) MessageBytes() chan<- int64 { return c.messageBytes }

// Cut closes only the raw socket. The core must then send the writer its
// final entry at once, never waiting for the reader's Ended (R2).
func (c *Conn) Cut() {
	if secure, ok := c.raw.(*tls.Conn); ok {
		_ = secure.NetConn().Close() // bypass TLS shutdown and its locks (R2)
	} else {
		_ = c.raw.Close()
	}
}

// CutFor publishes the core's selected cause before closing the raw socket.
// The core calls it at most once. Readers and writers observe the immutable
// cause through the notification; the resulting I/O failures select no cause.
func (c *Conn) CutFor(cause Closure) {
	c.cutCause = cause
	close(c.coreCut)
	c.Cut()
}

func (c *Conn) cutByCore() bool {
	select {
	case <-c.coreCut:
		return true
	default:
		return false
	}
}

// Release returns the listener's permit at the writer's close report. The
// core calls it once; the permit taken before serving guarantees room (R6).
func (c *Conn) Release() {
	if c.permits != nil {
		c.permits <- struct{}{}
	}
}

// RestoreIncoming transfers CONNECT's held receive quota before its CONNACK.
// One extra notification fits beside every reserved reply's two write reports.
func (c *Conn) RestoreIncoming(ids []uint16) {
	c.ackWrites <- ackWrite{held: ids}
}

// ReceiveQuota transfers restored identifiers to the reader before CONNACK.
// It uses the existing receive-window handover, without a writer queue entry.
type ReceiveQuota struct{ Identifiers []uint16 }
