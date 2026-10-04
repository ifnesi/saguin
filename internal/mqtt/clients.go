// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"

	"github.com/ifnesi/saguin/internal/mqtt/mempool"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

const (
	defaultKeepalive             uint16 = 10 // the default connection keepalive value in seconds.
	defaultClientProtocolVersion byte   = 4  // the default mqtt protocol version of connecting clients (if somehow unspecified).
	minimumKeepalive             uint16 = 5  // the minimum recommended keepalive - values under with display a warning.
)

var (
	ErrMinimumKeepalive = errors.New("client keepalive is below minimum recommended value and may exhibit connection instability")
)

// ReadFn is the function signature for the function used for reading and processing new packets.
type ReadFn func(*Client, packets.Packet) error

// Clients contains a map of the clients known by the broker.
type Clients struct {
	internal map[string]*Client // clients known by the broker, keyed on client id.
	sync.RWMutex
}

// NewClients returns an instance of Clients.
func NewClients() *Clients {
	return &Clients{
		internal: make(map[string]*Client),
	}
}

// Add adds a new client to the clients map, keyed on client id.
func (cl *Clients) Add(val *Client) {
	cl.Lock()
	defer cl.Unlock()
	cl.internal[val.ID] = val
}

// GetAll returns all the clients.
func (cl *Clients) GetAll() map[string]*Client {
	cl.RLock()
	defer cl.RUnlock()
	m := map[string]*Client{}
	for k, v := range cl.internal {
		m[k] = v
	}
	return m
}

// Each calls fn for every client, without copying the table: GetAll copies
// it, and a sweep that runs every second and finds nothing to do paid a
// table-sized allocation per tick for it, which is garbage the collector
// then holds RSS for.
//
// **fn runs under the table's read lock**, so it must be quick and must not
// call back into Clients (a nested read lock behind a queued writer never
// returns), take a lock a Clients writer holds, do I/O, or delete. A caller
// that has work to do takes only the clients it needs out of fn and does the
// work after Each returns. Every CONNECT waits on the write lock meanwhile.
func (cl *Clients) Each(fn func(*Client)) {
	cl.RLock()
	defer cl.RUnlock()
	for _, c := range cl.internal {
		fn(c)
	}
}

// Get returns the value of a client if it exists.
func (cl *Clients) Get(id string) (*Client, bool) {
	cl.RLock()
	defer cl.RUnlock()
	val, ok := cl.internal[id]
	return val, ok
}

// Len returns the length of the clients map.
func (cl *Clients) Len() int {
	cl.RLock()
	defer cl.RUnlock()
	val := len(cl.internal)
	return val
}

// Delete removes a client from the internal map.
//
// Production code uses DeleteIf instead; TestNoProductionCodeDeletesAClientByIDAlone
// holds it to that.
func (cl *Clients) Delete(id string) {
	cl.Lock()
	defer cl.Unlock()
	delete(cl.internal, id)
}

// DeleteIf removes the client under id only while it is still val, and
// reports whether it did.
//
// **Every site that removes a client has examined one particular client** -
// the one it judged expired, disconnected or superseded - and between that
// and the delete a new connection may have taken the id. Deleting by id alone
// then removes the connection that arrived, leaving it connected and out of
// the registry. Compared and deleted under one lock, it is the client that
// was examined or nothing.
func (cl *Clients) DeleteIf(id string, val *Client) bool {
	cl.Lock()
	defer cl.Unlock()
	if cl.internal[id] != val {
		return false
	}
	delete(cl.internal, id)
	return true
}

// GetByListener returns clients matching a listener id.
//
// The capacity hint used to read cl.Len(), which takes
// RLock a second time while this function already holds it. sync.RWMutex
// documents that as prohibited - "if a goroutine holds a RWMutex for reading
// and another goroutine might call Lock, no goroutine should expect to be
// able to acquire a read lock until the initial read lock is released. In
// particular, this prohibits recursive read locking."
//
// A client connecting while a listener closes is exactly that interleaving:
// detachClient -> Clients.Delete -> Lock() queues a writer, RWMutex stops
// admitting new readers, and this nested RLock blocks forever behind the
// writer that is itself waiting on the RLock we still hold.
//
// The map is already guarded by the RLock held here, so read len directly.
func (cl *Clients) GetByListener(id string) []*Client {
	cl.RLock()
	defer cl.RUnlock()
	if hook := getByListenerLocked.Load(); hook != nil {
		(*hook)()
	}
	clients := make([]*Client, 0, len(cl.internal))
	for _, client := range cl.internal {
		if client.Net.Listener == id && !client.Closed() {
			clients = append(clients, client)
		}
	}
	return clients
}

// getByListenerLocked is a test seam, nil in production: it runs in
// GetByListener once the read lock is held, so a test can queue a writer
// there and see whether anything after it reads the lock again.
var getByListenerLocked atomic.Pointer[func()]

// Client contains information about a client known by the broker.
type Client struct {
	Properties   ClientProperties // client properties
	State        ClientState      // the operational state of the client.
	Net          ClientConnection // network connection state of the client
	ID           string           // the client id.
	ops          *ops             // ops provides a reference to server ops.
	sync.RWMutex                  // mutex
}

// countingReader counts every byte read from a connection's socket into n,
// beneath its bufio.Reader - saguin_bytes_received_total's "everything on the
// wire" (RFC 0005), counted where the bytes arrive, as mosquitto counts them
// at its socket read.
//
// **One place, rather than one per reader of the buffer.** Counted where
// packets were parsed, four reads counted nothing: the CONNECT's fixed header
// (read by readConnectionPacket itself, to refuse from the declared size),
// the header and discarded body of a packet over the size bound, a busy
// refusal's opening bytes, and a refused CONNECT's prefix. What the buffer
// reads ahead is counted when it is read, because it has been read off the
// wire; a connection that closes with bytes still buffered read them.
type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c countingReader) Read(p []byte) (int, error) {
	k, err := c.r.Read(p)
	c.n.Add(int64(k))
	return k, err
}

// ClientConnection contains the connection transport and metadata for the client.
type ClientConnection struct {
	Conn     net.Conn      // the net.Conn used to establish the connection
	bconn    *bufio.Reader // a buffered net.Conn for reading packets
	outbuf   *bytes.Buffer // a buffer for writing packets
	Remote   string        // the remote address of the client
	Listener string        // listener id of the client
	Inline   bool          // if true, the client is the built-in 'inline' embedded client
}

// ClientProperties contains the properties which define the client behaviour.
type ClientProperties struct {
	Props           packets.Properties
	Will            Will
	Username        []byte
	ProtocolVersion byte
	Clean           bool
}

// Will contains the last will and testament details for a client connection.
type Will struct {
	Payload           []byte                 // -
	User              []packets.UserProperty // -
	TopicName         string                 // -
	Flag              uint32                 // 0,1
	WillDelayInterval uint32                 // -
	Qos               byte                   // -
	Retain            bool                   // -
}

// ClientState tracks the state of the client.
type ClientState struct {
	TopicAliases    TopicAliases       // a map of topic aliases
	stopCause       atomic.Value       // reason for stopping
	Inflight        *Inflight          // a map of in-flight qos messages
	Subscriptions   *Subscriptions     // a map of the subscription filters a client maintains
	disconnected    atomic.Int64       // the moment the client disconnected, in unix nanoseconds, for calculating expiry
	outbound        outQueue           // queue for pending outbound packets
	endOnce         sync.Once          // only end once
	isTakenOver     atomic.Bool        // used to identify orphaned clients
	hangingUp       atomic.Bool        // the broker has begun ending this connection (BeginHangUp)
	handover        sync.RWMutex       // read while a delivery is recorded here, written while the session is taken over
	packetID        uint32             // the current highest packetID
	slot            slotFunc           // gives back the connection's max_connections slot, until it has (beginEnd)
	open            context.Context    // indicate that the client is open for packet exchange
	cancelOpen      context.CancelFunc // cancel function for open context
	outboundQty     int32              // number of messages currently in the outbound queue
	io              atomic.Int32       // where writes stand against the end (beginEnd)
	outboundBytes   atomic.Int64       // what the packets in the outbound queue take, as inflightSize counts them
	outboundQoS     atomic.Int32       // how many of them are QoS 1 or 2, queued and not yet written
	exhausted       atomic.Bool        // NextPacketID failed and has not succeeded since (Exhausted)
	wake            chan struct{}      // nudged when withheld deliveries wait; the write loop writes them
	writer          atomic.Int32       // writerUnarmed, writerArmed or writerStarted (armWriter)
	resuming        atomic.Bool        // a resumed session's resend is still to be written (holdForResend)
	writerDone      chan struct{}      // closed when the write loop has returned (releaseConnection)
	Keepalive       uint16             // the number of seconds the connection can wait
	ServerKeepalive bool               // keepalive was set by the server
}

// slotFunc gives back a connection's max_connections slot (Client.holdSlot).
//
// **ClientState's order keeps a Client at 760 bytes**, and the fields the
// slot and the end added sit where alignment left holes: slot after
// packetID, io after outboundQty, exhausted moved after outboundQoS. A
// Client over 760 bytes takes Go's next allocation size class, 896 with its
// 8-byte header, and every session held away kept 128 bytes more than it is
// charged (TestWhatAnAwaySessionCostsIsWhatItIsCharged).
type slotFunc = atomic.Pointer[func()]

// What a connection's writes may do as it ends (Client.beginEnd), and who
// finishes the end (Client.claimEnd). Every state from ioEndPending on is an
// end decided; the claimed ones have an owner.
const (
	ioOpen                int32 = iota // writes go out
	ioWriting                          // a write is under way, holding the client's lock
	ioEndPending                       // the end was decided during that write, which gives the slot back as it finishes
	ioClaimPending                     // the end was claimed during that write, which gives the slot back as it finishes
	ioEnded                            // ended, the slot given back: only a refusing CONNACK goes out
	ioEndedQuiet                       // ended, and nothing more goes out: that CONNACK has, or a write failed
	ioClaimed                          // ended and claimed: only its owner's DISCONNECT goes out
	ioClaimedQuiet                     // ended and claimed, and nothing more goes out
	ioConnackClaimed                   // claimed by the goroutine refusing the CONNECT: only its CONNACK goes out
	ioConnackClaimedQuiet              // claimed by it, and nothing more goes out
)

// newClient returns a new instance of Client. This is almost exclusively used by Server
// for creating new clients, but it lives here because it's not dependent.
func newClient(c net.Conn, o *ops) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	cl := &Client{
		State: ClientState{
			Inflight:      NewInflights(),
			Subscriptions: NewSubscriptions(),
			TopicAliases:  NewTopicAliases(o.options.Capabilities.TopicAliasMaximum),
			open:          ctx,
			cancelOpen:    cancel,
			Keepalive:     defaultKeepalive,
			outbound:      newOutQueue(int(o.options.Capabilities.MaximumClientWritesPending)),
			wake:          make(chan struct{}, 1),
			writerDone:    make(chan struct{}),
		},
		Properties: ClientProperties{
			ProtocolVersion: defaultClientProtocolVersion, // default protocol version
		},
		ops: o,
	}

	if c != nil {
		cl.Net = ClientConnection{
			Conn:   c,
			bconn:  bufio.NewReaderSize(countingReader{c, &o.info.BytesReceived}, o.options.ClientNetReadBufferSize),
			Remote: c.RemoteAddr().String(),
		}
	}

	return cl
}

// WriteLoop ranges over pending outbound messages and writes them to the client connection.
func (cl *Client) WriteLoop() {
	if done := cl.State.writerDone; done != nil {
		defer close(done)
	}
	if hook := writeLoopBegins.Load(); hook != nil {
		(*hook)(cl)
	}
	for {
		select {
		case <-cl.State.outbound.ready:
			// Everything queued, one packet at a time, with what the select
			// would have asked between two receives asked between two
			// packets: a withheld delivery to write, and the end.
			for {
				pk, more := cl.State.outbound.pop()
				if pk == nil {
					break
				}
				if !cl.writeQueued(pk) {
					return
				}
				if !more {
					// **Nothing is queued behind it, so nothing is left
					// parked**, whatever became of it. A write made while a
					// packet was queued waits in the connection's buffer for
					// that packet's write to carry it (WritePacket), and one
					// refused before a byte was written carries nothing - a
					// publisher's PUBACK, parked behind its own delivery
					// refused as too large, waited for whatever the client
					// was sent next, which with keepalive 0 was never, and
					// its re-send was delivered twice to everybody else. Asked here rather than where a
					// write is refused, so a refusal added later cannot leave
					// it behind.
					if !cl.flushParked() {
						return
					}
					break
				}
				select {
				case <-cl.State.wake:
					cl.drainWithheld()
				case <-cl.State.open.Done():
					return
				default:
				}
			}
		case <-cl.State.wake:
			cl.drainWithheld()
		case <-cl.State.open.Done():
			return
		}
	}
}

// writeQueued writes one packet taken from the queue, and reports false where
// the write ended the client and the loop must stop.
func (cl *Client) writeQueued(pk *packets.Packet) bool {
	var err error
	if pk.FirstSend {
		// A delivery's first send, claimed against the expiry sweep: a copy
		// whose entry the sweep retired is not written (Inflight.ClaimQueued).
		tk, how := cl.State.Inflight.ClaimQueued(pk, time.Now().Unix(), cl.maximumExpiry())
		switch how {
		case FirstSendNone:
			cl.dequeued(*pk)
			return true
		case FirstSendExpired:
			cl.dequeued(*pk)
			cl.retiredUnsent(tk, true)
			return true
		}
		err = cl.writeFirstSend(tk, false)
	} else {
		err = cl.writePacket(*pk)
	}
	cl.dequeued(*pk)

	// **A packet refused for its size is discarded whatever became of the
	// client meanwhile.** Nothing of it was written, so a close from
	// elsewhere between the refusal and here - a client hanging up on the
	// PUBACK that went out while this was being refused - does not make it a
	// failed write. Asked after the close, as it was, it went uncounted, 2 of
	// 18,000 under -race, and a QoS 1 or 2 delivery kept its entry and its
	// slot as if it had been sent
	// (TestADeliveryRefusedForItsSizeAsItsClientClosesIsDiscarded).
	if errors.Is(err, packets.ErrPacketTooLarge) {
		cl.discardTooLarge(*pk)
	}
	if err != nil {
		// TODO : Figure out what to do with error
		cl.ops.log.Debug("failed publishing packet", "error", err, "client", cl.ID, "packet", pk)

		// A write that failed on the connection - a timeout under
		// ClientNetWriteTimeout, a reset, a broken pipe - may have
		// put part of a packet on the wire, and WritePacket has
		// stopped the client for it: the next packet written would
		// land in the middle of the last one. Continuing would also
		// leave this loop taking the client lock for every remaining
		// queued packet, a failure at a time, while every other
		// goroutine that needs that lock - publishToClient taking a
		// packet identifier - waits.
		//
		// A packet refused before it was written leaves the client
		// open, and the loop goes on to the next one.
		if cl.Closed() {
			cl.Stop(err)
			return false
		}
	}
	return true
}

// flushParked writes what WritePacket left in the connection's buffer for a
// queued packet's write to carry, and reports false where that write failed
// and stopped the client. Bounded by the write deadline as every write is,
// armed and cleared inside the lock for the same reason (WritePacket).
func (cl *Client) flushParked() bool {
	err := func() (err error) {
		cl.Lock()
		defer cl.Unlock()
		if cl.Net.outbuf == nil {
			return nil
		}
		ok, writing := cl.beginWrite(false)
		if !ok {
			return nil
		}
		defer func() { cl.endWrite(writing, err) }()
		if d := cl.writeDeadline(); !d.IsZero() {
			if conn := cl.Net.Conn; conn != nil {
				if err := conn.SetWriteDeadline(d); err == nil {
					defer conn.SetWriteDeadline(time.Time{})
				}
			}
		}
		return cl.flushOutbuf()
	}()
	if err != nil {
		cl.Stop(err)
		return false
	}
	return true
}

// discardTooLarge answers a delivery refused for being larger than the
// client's Maximum Packet Size as MQTT has the server answer it: discarded
// without being sent, and the server behaving as if it had completed sending
// it [MQTT-3.1.2-25]. It is counted, because the client never had it.
//
// **At QoS 1 or 2 it gives its entry and its slot of window back**, as its
// acknowledgement would have. Kept, a client with Receive Maximum 2 sent two
// was never sent anything again, while every publisher was told its message
// had been accepted - which mosquitto does until the
// client reconnects, and EMQX until the session ends. Only while cl still has
// the session: one a takeover carried is the new connection's to answer for.
// The hooks run after, as processPuback's do, so the room is served.
func (cl *Client) discardTooLarge(pk packets.Packet) {
	cl.ops.info.DeliveriesTooLarge.Add(1)
	cl.ops.log.Debug("did not deliver a message larger than this client's Maximum Packet Size",
		"client", cl.ID, "packet", pk.PacketID, "qos", pk.FixedHeader.Qos,
		"maximum_packet_size", cl.Properties.Props.MaximumPacketSize)
	if pk.FixedHeader.Type != packets.Publish || pk.FixedHeader.Qos == 0 {
		return
	}
	retired := false
	if !cl.WhileOwned(func() {
		if retired = cl.State.Inflight.Retire(pk.PacketID); retired {
			cl.State.Inflight.IncreaseSendQuota()
		}
	}) || !retired {
		return
	}
	cl.ops.hooks.OnQosComplete(cl, pk)
	cl.State.Inflight.Unclaim(pk.PacketID) // after the hook, as processPuback
	cl.WakeWriter()
}

// OutboundHasRoom reports whether the queue for the client's socket has room
// for pk: a slot is free, and what it holds, as inflightSize counts it, stays
// within the session's share on the wire (sessionWireBytes). An empty queue
// takes a packet however large.
//
// **Bounded by bytes, not only by packets.** Mochi's queue was 8,192 packets
// of any size, and one subscriber that stopped reading held that many:
// measured with 10,000 QoS 0 messages of 32 KiB, 510 MiB.
//
// Asked and then added to by enqueue without a lock, so publishers delivering
// to one client at the same moment can each pass it and together exceed the
// share by one message each.
func (cl *Client) OutboundHasRoom(pk packets.Packet) bool {
	if cl.State.outbound.len() >= cl.State.outbound.max {
		return false
	}
	limit := cl.ops.options.sessionWireBytes()
	queued := cl.State.outboundBytes.Load()
	return limit <= 0 || queued <= 0 || queued+inflightSize(pk) <= limit
}

// enqueue puts a packet in the queue for the client's socket if it has room,
// and reports whether it did.
//
// **Counted before it is queued**, so from the moment the write loop can take
// it until the moment it has been written, it is counted: drainWithheld writes
// straight to the socket only when no QoS 1 or 2 delivery is counted, and one
// taken from the queue and not yet written would otherwise be overtaken.
func (cl *Client) enqueue(pk *packets.Packet) bool {
	if !cl.OutboundHasRoom(*pk) {
		return false
	}
	atomic.AddInt32(&cl.State.outboundQty, 1)
	cl.State.outboundBytes.Add(inflightSize(*pk))
	if pk.FixedHeader.Qos > 0 {
		cl.State.outboundQoS.Add(1)
	}
	if cl.State.outbound.push(pk) {
		cl.startWriter()
		return true
	}
	cl.uncount(*pk)
	return false
}

// uncount takes a packet out of the queue's counts.
func (cl *Client) uncount(pk packets.Packet) int32 {
	atomic.AddInt32(&cl.State.outboundQty, -1)
	cl.State.outboundBytes.Add(-inflightSize(pk))
	if pk.FixedHeader.Qos > 0 {
		return cl.State.outboundQoS.Add(-1)
	}
	return cl.State.outboundQoS.Load()
}

// dequeued counts a packet out of the queue once the write loop has written
// it, or failed to, and writes what was withheld behind it once no QoS 1 or 2
// delivery is left queued ahead of that.
func (cl *Client) dequeued(pk packets.Packet) {
	if cl.uncount(pk) == 0 && pk.FixedHeader.Qos > 0 && !cl.Closed() {
		cl.WakeWriter()
	}
}

// ParseConnect parses the connect parameters and properties for a client.
func (cl *Client) ParseConnect(lid string, pk packets.Packet) {
	cl.Net.Listener = lid

	cl.Properties.ProtocolVersion = pk.ProtocolVersion
	cl.Properties.Username = pk.Connect.Username
	cl.Properties.Clean = pk.Connect.Clean
	cl.Properties.Props = pk.Properties.Copy(false)
	// **Nothing of a CONNECT that nothing reads is kept for the life of the
	// connection.** Its User Properties and Authentication Data are for the
	// hooks that judge it, which are handed the packet itself; Copy keeps
	// them because a PUBLISH's must travel. Measured with 1,000 clients each
	// sending 60 KiB of User Properties, on a decoder that copies every field:
	// kept, they held 128 MiB of live heap; dropped here, 67 MiB, the same as
	// clients that sent none.
	cl.Properties.Props.User = nil
	cl.Properties.Props.AuthenticationData = nil

	if pk.Connect.Keepalive <= minimumKeepalive {
		cl.ops.log.Warn(
			ErrMinimumKeepalive.Error(),
			"client", cl.ID,
			"keepalive", pk.Connect.Keepalive,
			"recommended", minimumKeepalive,
		)
	}

	cl.State.Keepalive = pk.Connect.Keepalive                                              // [MQTT-3.2.2-22]
	cl.State.Inflight.ResetReceiveQuota(int32(cl.ops.options.Capabilities.ReceiveMaximum)) // server receive max per client
	cl.State.Inflight.ResetSendQuota(int32(cl.Properties.Props.ReceiveMaximum))            // client receive max
	cl.State.TopicAliases.Outbound = NewOutboundTopicAliases(cl.Properties.Props.TopicAliasMaximum)

	cl.ID = pk.Connect.ClientIdentifier
	if cl.ID == "" {
		cl.ID = xid.New().String() // [MQTT-3.1.3-6] [MQTT-3.1.3-7]
		cl.Properties.Props.AssignedClientID = cl.ID
	}

	if pk.Connect.WillFlag {
		cl.Properties.Will = Will{
			Qos:               pk.Connect.WillQos,
			Retain:            pk.Connect.WillRetain,
			Payload:           pk.Connect.WillPayload,
			TopicName:         pk.Connect.WillTopic,
			WillDelayInterval: pk.Connect.WillProperties.WillDelayInterval,
			User:              pk.Connect.WillProperties.User,
		}
		// **The delay the client asked for, uncapped.** Upstream capped it
		// here to the Session Expiry Interval the CONNECT states. saguin
		// holds every delayed Will itself and bounds the wait by the session
		// it was granted (Broker.willDelay), which is the tighter rule; the
		// cap here only hid what was asked. A CONNECT stating an expiry of 0
		// reached saguin with a delay of 0, so the line RFC 0003 promises -
		// the delay was not honoured, and why - was never written for it.
		if pk.Connect.WillFlag {
			cl.Properties.Will.Flag = 1 // atomic for checking
		}
	}
}

// refreshDeadline refreshes the read deadline for the net.Conn connection.
//
// The read deadline and not both: [MQTT-3.1.2-22] is a rule about packets
// the client sends, and SetDeadline would apply it to writes as well. That
// was harmless while nothing else set a write deadline, and became a way
// for a client to switch one off the moment ClientNetWriteTimeout existed:
// this runs at the top of every pass of the read loop, so any packet
// needing no reply - a QoS 0 PUBLISH, a PUBACK, an UNSUBSCRIBE - cleared
// the deadline a queued write had armed. With keepalive 0 it cleared it
// outright; with keepalive 60 it pushed it 90 seconds out, and the next
// packet pushed it again.
func (cl *Client) refreshDeadline(keepalive uint16) {
	var expiry time.Time // nil time can be used to disable deadline if keepalive = 0
	if keepalive > 0 {
		expiry = time.Now().Add(time.Duration(keepalive+(keepalive/2)) * time.Second) // [MQTT-3.1.2-22]
	}

	if cl.Net.Conn != nil {
		_ = cl.Net.Conn.SetReadDeadline(expiry) // [MQTT-3.1.2-22]
	}
}

// writeDeadline is when a single write to this client must have completed.
//
// ClientNetWriteTimeout when it is set, and otherwise the same keepalive
// expiry refreshDeadline uses for reads - because that is the bound every
// release before this one gave a write, by arming both deadlines at once.
// Zero means no bound, which happens only when neither is configured.
func (cl *Client) writeDeadline() time.Time {
	if d := cl.ops.options.ClientNetWriteTimeout; d > 0 {
		return time.Now().Add(d)
	}
	if k := cl.State.Keepalive; k > 0 {
		return time.Now().Add(time.Duration(k+(k/2)) * time.Second) // [MQTT-3.1.2-22]
	}
	return time.Time{}
}

// NextPacketID returns the next available (unused) packet id for the client.
// If no unused packet ids are available, an error is returned and the client
// should be disconnected.
func (cl *Client) NextPacketID() (i uint32, err error) {
	// **Not under the client's lock**, which WritePacket holds across the
	// write: a publisher choosing an identifier for a delivery to a client
	// that has stopped reading waited for that socket. Inflight.Claim is
	// atomic under the in-flight table's own lock, which is what makes the
	// identifier unique, so the client lock protected nothing here.
	//
	// **Reserved as it is chosen** (Inflight.Claim), because a delivery is
	// registered in flight only after its session store write: until then the
	// identifier it holds would otherwise look free to the next delivery.
	// A caller that then abandons the delivery releases it (Inflight.Unclaim).
	i, ok := cl.State.Inflight.Claim(atomic.LoadUint32(&cl.State.packetID),
		cl.ops.options.Capabilities.maximumPacketID)
	if !ok {
		return 0, packets.ErrQuotaExceeded
	}
	atomic.StoreUint32(&cl.State.packetID, i)
	cl.State.exhausted.Store(false)
	return i, nil
}

// Exhausted reports whether a NextPacketID that just failed is the first of
// its episode - the first since this client last got an identifier - so that
// whoever logs it says so once, as a full session queue is said once, rather
// than on every delivery tried while the client's identifiers are spent. A
// replay measured 79,504 such lines before this (the 2026-09-27 fleet rerun).
func (cl *Client) Exhausted() bool {
	return cl.State.exhausted.CompareAndSwap(false, true)
}

// ResendInflightMessages resends a resumed session's in-flight messages, as
// many as the client's Receive Maximum admits.
//
// **Within the window, and the rest withheld.** Every queued packet used to
// be written back to back, whatever the client's Receive Maximum - which MQTT
// 5 section 4.9 forbids [MQTT-3.3.4-9] - and on the CONNECT goroutine, one
// write deadline each, so a session that had queued eight thousand messages
// held its connect for eight thousand writes. What does not fit is marked
// withheld, and each acknowledgement sends the next (processPacket).
//
// An outbound PUBREL holds a slot until its PUBCOMP, so it is resent first and
// counted. Acknowledgements of the client's own publishes hold none.
func (cl *Client) ResendInflightMessages(force bool) error {
	if cl.State.Inflight.Len() == 0 {
		return nil
	}
	return cl.resendInflight(cl.State.Inflight.GetAll(false))
}

// ResendInherited is ResendInflightMessages for what the session held when
// this connection took it: the entries registered no later than inherited
// (Inflight.Registered, read before the connection was registered).
//
// **A delivery registered after the registration is a first delivery, and
// its sender writes it.** The resend runs after the connection is
// registered, so any sender can hand it a delivery first - a shared group
// draining for a member that came back beside it, a live fan-out, a channel
// pump - and a resend that read the whole table wrote that one again, with
// DUP and under the same packet identifier: one message delivered twice on
// one connection, which [MQTT-4.4.0-1] does not allow. Measured as 2 runs in
// 200 under load serving a group's job twice to one member (TestAGroupsBacklogIsServedOnceWhenItsMembersComeBackTogether).
// mosquitto and EMQX write a client's resend and its new deliveries from one
// thread or process, so the two cannot meet there.
func (cl *Client) ResendInherited(inherited uint64) error {
	if cl.State.Inflight.Len() == 0 {
		return nil
	}
	return cl.resendInflight(cl.State.Inflight.GetAllThrough(inherited))
}

func (cl *Client) resendInflight(all []packets.Packet) error {
	for _, tk := range all {
		if tk.FixedHeader.Type == packets.Publish {
			continue
		}

		cl.ops.hooks.OnQosPublish(cl, tk, tk.Created, 0)
		err := cl.WritePacket(tk)
		if err != nil {
			return err
		}

		switch tk.FixedHeader.Type {
		case packets.Puback, packets.Pubcomp:
			if ok := cl.State.Inflight.Delete(tk.PacketID); ok {
				cl.ops.hooks.OnQosComplete(cl, tk)
			}
		case packets.Pubrel:
			cl.State.Inflight.DecreaseSendQuota()
		}
	}

	share, waiting := cl.ops.options.sessionWireBytes(), false
	for _, tk := range all {
		if tk.FixedHeader.Type != packets.Publish {
			continue
		}
		if atomic.LoadInt32(&cl.State.Inflight.maximumSendQuota) > 0 &&
			atomic.LoadInt32(&cl.State.Inflight.sendQuota) <= 0 {
			// Held for room. One written before the connection went is a
			// re-send whenever it is written, so it goes as one
			// [MQTT-3.3.1-1]; one never written stays a first delivery.
			if tk.Expiry >= 0 {
				cl.State.Inflight.WithholdAgain(tk.PacketID)
			} else {
				cl.State.Inflight.Withhold(tk.PacketID)
			}
			continue
		}
		// Never written, and the session's share on the wire is full: it
		// waits, and so does every later one, which keeps them in order
		// [MQTT-4.6.0-1] when a smaller one behind it would have fitted.
		if tk.Expiry < 0 && (waiting || !cl.State.Inflight.WireHasRoom(inflightSize(tk), share)) {
			waiting = true
			continue
		}
		var err error
		if tk.Expiry < 0 {
			// Withheld before the disconnect, so never written: claimed, and
			// sent as a first delivery rather than a re-send - or retired,
			// expired unsent (Inflight.ClaimWithheld).
			claimed, how := cl.State.Inflight.ClaimWithheld(tk.PacketID, time.Now().Unix(), cl.maximumExpiry())
			switch how {
			case FirstSendNone:
				continue // taken back while this ran
			case FirstSendExpired:
				cl.retiredUnsent(claimed, false)
				continue
			}
			tk = claimed
			cl.ops.hooks.OnQosPublish(cl, tk, tk.Created, 0)
			err = cl.writeFirstSend(tk, false)
		} else {
			tk.FixedHeader.Dup = true // [MQTT-3.3.1-1] [MQTT-3.3.1-3]
			cl.State.Inflight.DecreaseSendQuota()
			cl.ops.hooks.OnQosPublish(cl, tk, tk.Created, 0)
			err = cl.writePacket(tk)
		}
		if err != nil {
			// One too large for this connection - kept by one that took
			// more - is discarded, and the rest are still owed their
			// re-send. Returned, the resume ended the connection, and the
			// next one met the same message.
			if !errors.Is(err, packets.ErrPacketTooLarge) {
				return err
			}
			cl.discardTooLarge(tk)
		}
	}

	return nil
}

// WakeWriter asks the write loop to write what is withheld, without writing it
// here. **Called after the state it is about has changed**, so the loop always
// re-reads a world in which the work exists: a signal that finds the buffer
// full is dropped because a wake is already pending, and that pending wake has
// not yet looked.
//
// **A delivery is written by the client's own write loop and by nothing
// else.** A publisher that wrote one itself waited on a stranger's socket,
// which is invariant 16's whole subject: measured with 30,000 QoS 1 publishes
// to one slow subscriber before this, a publisher took 10.62-10.73s against
// 1.11-1.13s with a subscriber that keeps up. The nudge is dropped if one is
// already pending, because the loop looks at everything withheld when it wakes.
func (cl *Client) WakeWriter() {
	select {
	case cl.State.wake <- struct{}{}:
	default:
	}
	cl.startWriter()
}

// The write loop's three states: a client no connection serves has none, one
// a connection serves may start one, and one has.
const (
	writerUnarmed int32 = iota
	writerArmed
	writerStarted
)

// armWriter lets the client's write loop start: at once where something
// already waits for it, otherwise on the first packet queued or the first
// wake (startWriter).
//
// **An idle connection has no write loop** (saguin). Started with the
// connection, every client held a goroutine parked on its queue, 2-3 KB of
// stack and runtime state, though a connection that is only subscribed may
// never be sent anything: the CONNACK and SUBACK are written by the reader
// (TestAnIdleClientHasNoWriteLoopUntilItIsSentSomething).
//
// **Only a connection arms it.** A client no connection serves - the
// inline client, a session restored from the store - never had a write loop,
// and a packet queued for one stays queued, as before.
//
// Asked after the arming, so a packet queued or a wake raised before it is
// not left waiting: whichever of this and the push comes second sees the
// other (TestAPacketQueuedAsTheWriterIsArmedIsWritten).
func (cl *Client) armWriter() {
	if !cl.State.writer.CompareAndSwap(writerUnarmed, writerArmed) {
		return
	}
	if cl.State.outbound.len() > 0 || len(cl.State.wake) > 0 {
		cl.startWriter()
	}
}

// startWriter starts the write loop, once, for an armed client whose
// connection is still open.
//
// **One write loop by construction**: this is the only place one is started
// (TestOnlyStartWriterStartsTheWriteLoop), and the swap lets exactly one
// caller through.
//
// **None once the connection has ended**, asked again after the swap: a Stop
// between the first look and the swap started a loop for a connection that
// was already over, which returned at once.
// What the second look leaves is a Stop after it, where the loop was decided
// on while the connection was open and stops on its end like any other.
// Taken and not started, the loop is recorded as returned (writerDone), so
// releaseConnection does not wait for one that never ran.
func (cl *Client) startWriter() {
	// Not while a resend is still to be written (holdForResend); endResend
	// starts it.
	if cl.State.resuming.Load() {
		return
	}
	if cl.State.writer.Load() != writerArmed || cl.State.open.Err() != nil {
		return
	}
	if hook := startWriterBeforeClaim.Load(); hook != nil {
		(*hook)(cl)
	}
	if cl.State.writer.CompareAndSwap(writerArmed, writerStarted) {
		if cl.State.open.Err() != nil {
			if cl.State.writerDone != nil {
				close(cl.State.writerDone)
			}
			return
		}
		go cl.WriteLoop()
	}
}

// startWriterBeforeClaim and writeLoopBegins are test seams, nil in
// production: the first runs in startWriter between its look at the
// connection and its claim of the loop, the second as a write loop begins.
var (
	startWriterBeforeClaim atomic.Pointer[func(*Client)]
	writeLoopBegins        atomic.Pointer[func(*Client)]
)

// drainWithheld writes withheld deliveries, earliest first, while the client's
// window and the session's share on the wire (sessionWireBytes) have room.
//
// **The write loop runs this and nothing else does.** Every other goroutine
// asks with WakeWriter, so there is one writer by construction rather than by
// a guard two goroutines race for: deliveries cannot be written out of order
// [MQTT-4.6.0-1], and a signal cannot be lost the way a note could when a
// second goroutine cleared it between its own set and its drain.
//
// **Only while no QoS 1 or 2 delivery is queued.** A withheld delivery is
// written straight to the socket, and one queued was made before it, so
// writing past the queue would put the later one first.
func (cl *Client) drainWithheld() {
	share := cl.ops.options.sessionWireBytes()
	for cl.State.outboundQoS.Load() == 0 && cl.State.Inflight.HasSendQuota() && cl.State.Inflight.MayClaim(share) {
		next, how := cl.State.Inflight.TakeImmediate(share, time.Now().Unix(), cl.maximumExpiry())
		switch how {
		case FirstSendNone:
			return
		case FirstSendExpired:
			cl.retiredUnsent(next, false)
			continue
		}
		if hook := firstSendClaimed.Load(); hook != nil {
			(*hook)(cl)
		}
		err := cl.writeFirstSend(next, true)
		if errors.Is(err, packets.ErrPacketTooLarge) {
			cl.discardTooLarge(next)
		}
	}
}

// EndTakenOver marks a connection taken over and clears its in-flight
// messages, for a takeover that ends its session rather than inheriting it
// (saguin): under the handover lock, so no delivery records on it after the
// clear, and marked, so a packet it read before its session was taken and
// handles after changes nothing the client id keeps (processSubscribe,
// processUnsubscribe). It belongs after the connection's UnsubscribeClient,
// which skips a connection already marked.
//
// **Marked before the clear, not after.** Each entry cleared reaches the
// hooks (OnDeliveryDone), and an entry cleared from a connection not yet
// marked reads there as an exchange its client finished: a QoS 1 delivery a
// shared group had lent a member was let go as acknowledged, where the
// ending of the member's session, which comes after this, gives it back to
// the group (MQTT 5 section 4.8.2).
func (cl *Client) EndTakenOver() {
	cl.State.handover.Lock()
	cl.State.isTakenOver.Store(true)
	cl.ClearInflights()
	cl.State.handover.Unlock()
}

// ClearInflights deletes all inflight messages for the client, e.g. for a disconnected user with a clean session.
func (cl *Client) ClearInflights() {
	for _, tk := range cl.State.Inflight.GetAll(false) {
		if ok := cl.State.Inflight.Retire(tk.PacketID); ok {
			cl.ops.hooks.OnDeliveryDone(cl, tk)
			cl.ops.hooks.OnQosDropped(cl, tk)
			cl.State.Inflight.Unclaim(tk.PacketID)
		}
	}
}

// writeFirstSend writes a delivery whose first send the caller has claimed
// (Inflight.TakeImmediate, ClaimQueued, ClaimWithheld), and ends the claim
// whatever becomes of the write, once. drained says the claim was
// TakeImmediate's.
//
// **Asked once more right before it is encoded**, since time passes between
// the claim and here: a delivery with nothing of its Message Expiry Interval
// left is not sent, and is retired by the writer that holds it
// [MQTT-3.3.2-5]. Sent, the encoder would have given it a second it did not
// have.
func (cl *Client) writeFirstSend(pk packets.Packet, drained bool) error {
	now := time.Now().Unix()
	if clock := firstSendClock.Load(); clock != nil {
		now = (*clock)()
	}
	if pk.ProtocolVersion == 5 && pk.Expiry > 0 && pk.Expiry-now <= 0 {
		if cl.State.Inflight.RetireUnsent(pk.PacketID, drained) {
			cl.retiredUnsent(pk, true)
		}
		return nil
	}
	err := cl.writePacket(pk)
	cl.State.Inflight.FirstSent(pk.PacketID, drained)
	return err
}

// retiredUnsent finishes the retirement of a delivery that expired before it
// was ever sent, for the one caller that retired it: its window slot back
// where it held one, the hooks told, and its identifier released.
func (cl *Client) retiredUnsent(tk packets.Packet, heldQuota bool) {
	cl.ops.hooks.OnDeliveryDone(cl, tk)
	cl.ops.hooks.OnDeliveryUnsent(cl, tk)
	cl.ops.hooks.OnQosDropped(cl, tk)
	cl.State.Inflight.Unclaim(tk.PacketID)
	if heldQuota {
		cl.State.Inflight.IncreaseSendQuota()
		cl.WakeWriter()
	}
}

// maximumExpiry is the longest the server holds a message
// (Capabilities.MaximumMessageExpiryInterval), for the expiry rule.
func (cl *Client) maximumExpiry() int64 {
	return cl.ops.options.Capabilities.MaximumMessageExpiryInterval
}

// firstSendClaimed is a test seam, nil in production: it runs in
// drainWithheld between claiming a withheld delivery's first send and
// writing it, the window the expiry sweep was let into.
var firstSendClaimed atomic.Pointer[func(*Client)]

// firstSendClock is a test seam, nil in production: the time writeFirstSend's
// last check reads, so a test can let a deadline pass between the claim and
// the encoding without waiting for it.
var firstSendClock atomic.Pointer[func() int64]

// ClearExpiredInflights deletes any inflight messages which have expired:
// only ones never sent (Inflight.mayExpire).
//
// **An expired delivery that took a slot gives its send quota back** - one
// queued for its first write takes it as it is queued - as an acknowledged
// one does (processPuback). Only an acknowledgement used to return it, so
// every expiry kept one slot for the life of the connection: at a Receive
// Maximum of one, the first expired delivery left every later one withheld
// until the client reconnected, however idle it was. Upstream mochi's sweep
// deletes without restoring it too. The writer is woken for what was withheld
// behind it, and OnQosDropped tells the broker the window has room, for its
// own channels' deliveries (Broker.wakeFreed).
func (cl *Client) ClearExpiredInflights(now, maximumExpiry int64) []uint16 {
	deleted := []uint16{}
	freedQuota := false
	for _, tk := range cl.State.Inflight.Expired(now, maximumExpiry) {
		if ok, heldQuota := cl.State.Inflight.RetireExpired(tk.PacketID, now, maximumExpiry); ok {
			if heldQuota {
				cl.State.Inflight.IncreaseSendQuota()
				freedQuota = true
			}
			cl.ops.hooks.OnDeliveryDone(cl, tk)
			cl.ops.hooks.OnDeliveryUnsent(cl, tk) // only one never sent expires (mayExpire)
			cl.ops.hooks.OnQosDropped(cl, tk)
			cl.State.Inflight.Unclaim(tk.PacketID)
			deleted = append(deleted, tk.PacketID)
		}
	}
	if freedQuota {
		cl.WakeWriter()
	}

	return deleted
}

// Read reads incoming packets from the connected client and transforms them into
// packets to be handled by the packetHandler.
//
// mark, where given, marks each packet whose answer waits on the store as
// it is decoded (Server.markWaiting). Passed in rather than kept on the
// Client, which an away session keeps: a field there took a Client past its
// allocation size class, 128 bytes more for every session.
func (cl *Client) Read(packetHandler ReadFn, mark ...markFn) error {
	var err error
	var marking markFn
	if len(mark) > 0 {
		marking = mark[0]
	}

	for {
		if cl.Closed() || cl.ending() {
			return cl.endedElsewhere()
		}

		cl.refreshDeadline(cl.State.Keepalive)
		fh := new(packets.FixedHeader)
		err = cl.ReadFixedHeader(fh)
		if err != nil {
			return err
		}

		err = cl.readOne(fh, packetHandler, marking)
		if err != nil {
			return err
		}
		// **The client's own DISCONNECT is an outcome of its own**, which
		// detachClient takes as the Will withdrawn [MQTT-3.14.4-3]:
		// processDisconnect answers nil only once it has ended the
		// connection, and every other end of the loop is an error.
		if fh.Type == packets.Disconnect {
			return ErrClientDisconnected
		}
	}
}

// ErrClientDisconnected is what the read loop returns when the client's own
// DISCONNECT ended the connection (Client.Read).
var ErrClientDisconnected = errors.New("client disconnected")

// endedElsewhere is what the read loop returns on finding, between packets,
// that another goroutine has decided the connection's end - a refusal, a
// hang-up, a takeover, a shutdown, a write that failed. It claims that end
// where nobody has, and otherwise waits for its owner to close the
// connection (claimEnd); detachClient names the owner's reason (endedBy).
//
// **Not nil.** Nil was a client's DISCONNECT, and detachClient withdrew the
// Will of a client the broker had ended [MQTT-3.1.2-8]: 94 of 200 consumers
// disconnected 0x95 lost theirs.
func (cl *Client) endedElsewhere() error {
	cl.Stop(nil)
	return ErrConnectionClosed
}

// endedBy is err, the error a read loop ended on, naming the reason another
// goroutine ended the connection for where that end was not the loop's own.
// The reason is named, not wrapped: it is not the loop's to answer
// (readRefusal).
func (cl *Client) endedBy(err error) error {
	if cause := cl.StopCause(); cause != nil && !errors.Is(err, cause) {
		return fmt.Errorf("%w (the connection was ended for %v)", err, cause)
	}
	return err
}

// markFn marks a packet whose answer waits on the store until what it
// returns is called (Server.markWaiting).
type markFn func(cl *Client, pk packets.Packet) (answered func())

// readOne reads the packet whose fixed header Read has read, and hands it to
// packetHandler.
//
// **The packet is in this frame, not Read's**, because Read's is live while
// the connection idles and this one is not: attachClient has why.
//
//go:noinline
func (cl *Client) readOne(fh *packets.FixedHeader, packetHandler ReadFn, mark markFn) error {
	pk, err := cl.decodePacket(fh)
	if err != nil {
		return err
	}
	// Marked before the hooks run, which may write what its answer waits on
	// (Server.markWaiting), and until it is answered.
	if mark != nil {
		defer mark(cl, pk)()
	}
	pk, err = cl.ops.hooks.OnPacketRead(cl, pk)
	if err != nil {
		return err
	}

	return packetHandler(cl, pk) // Process inbound packet.
}

// releaseConnection lets go of what only the connection needed, once the
// connection has ended and its read loop has returned (attachClient): the
// packets queued for its socket, and its read and write buffers.
//
// **A session that outlives its connection keeps its Client** for as long as
// it is away, up to limits.max_session_expiry, and everything the Client
// held stayed with it. Measured: a subscriber that stopped reading and went
// away kept half its session_queue_bytes queued for a socket that would never
// be written again - 516,018 bytes at the default - and every away session
// kept its 2 KiB read buffer. Nothing counted either (invariant 13).
//
// **Nothing a session is owed is let go here.** A QoS 1 or 2 packet in the
// queue is also in flight (makeDelivery records it before queueing it), and
// the session's next connection sends it again from there; a QoS 0 packet for
// a socket that has closed was never going to arrive. The queue refuses what
// is pushed after it is closed, so a delivery racing the end cannot put a
// packet back, and a write racing it finds the queue empty and never parks a
// packet in a new write buffer (WritePacket).
//
// **And not before the write loop has returned**, where one was started. It
// takes a packet off the queue before writing it and uncounts it after, so
// a loop still inside that write held a packet the queue no longer had and
// counted it: returned then, this left a closed connection's bytes counted
// on the away session (2 runs in 30 under
// -race). The wait is short: Stop has closed the connection, so the write
// fails at once, and the loop stops on the connection's end. Nothing is
// held while it waits.
func (cl *Client) releaseConnection() {
	for _, pk := range cl.State.outbound.close() {
		cl.uncount(*pk)
		if pk.FirstSend {
			cl.State.Inflight.Unqueue(pk) // sent again, as written, on the next connection
		}
	}
	if cl.State.writer.Load() == writerStarted && cl.State.writerDone != nil {
		<-cl.State.writerDone
	}
	cl.Lock()
	cl.Net.bconn = nil
	cl.Net.outbuf = nil
	cl.Unlock()
}

// holdSlot records how this connection gives back the max_connections slot
// it holds (beginEnd).
func (cl *Client) holdSlot(release func()) { cl.State.slot.Store(&release) }

// beginEnd marks the connection as ending, for good, and gives back its
// max_connections slot, once however many of its endings call it; a client
// that holds no slot gives back none.
//
// **Ending and giving the slot back are one step.** Once the slot can be
// another connection's, this one reads nothing more and writes nothing but
// the CONNACK or DISCONNECT that ends it (writePacket): a client DISCONNECT
// gives the slot back before its change is stored and the socket closed, and
// a delivery queued meanwhile was still written to a connection the broker
// had already counted as gone.
//
// **Called where the connection's end is decided, not where its socket
// closes** (RFC 0002 "How long a socket may wait to send CONNECT"): before
// the refusing CONNACK or the server's DISCONNECT is written (WritePacket),
// and as the client's own DISCONNECT or close is read. It went back as
// attachClient returned, after the CONNACK was on the wire: a client refused
// 0x86 read it, hung up and connected again before the refusing goroutine
// ran on, and was turned away 0x89 at max_connections 1 - 606 of 5,400
// trials with six brokers on two cores, mosquitto none. A client that hung
// up waited behind its session's teardown the same way.
//
// **A socket outlives its slot** by what comes between the decision and the
// close: writing the last packet, and what must precede the close - storing
// what a DISCONNECT changed (invariant 18), publishing the Will of a client
// that dropped.
//
// **It is a barrier, not a flag a writer looks at first.** A write under
// way when the end is decided holds the client's lock and may already be on
// the socket, so the slot goes back as that write finishes (endWrite), not
// before: the connection writes nothing once another can hold its slot. A
// writer that had looked at a flag and then waited for the lock wrote after
// the slot had gone. Nothing here waits, so ending a connection whose write
// is stuck behind a reader that has stopped never waits on it either.
//
// **Deciding the end is not owning it** (claimEnd): a connection decided
// here is closed by whoever claims it next.
func (cl *Client) beginEnd() {
	for {
		switch st := cl.State.io.Load(); st {
		case ioOpen:
			if cl.State.io.CompareAndSwap(ioOpen, ioEnded) {
				cl.releaseSlot()
				return
			}
		case ioWriting:
			if cl.State.io.CompareAndSwap(ioWriting, ioEndPending) {
				return
			}
		default:
			return
		}
	}
}

// claimEnd decides the connection's end where nothing has, and claims it: it
// reports true to one caller, ever, which then owns the end - it alone may
// write the DISCONNECT that names it, and it alone closes the connection
// (finishEnd). Every other caller waits for that close (awaitEnd) and closes
// nothing.
//
// **One owner, because two finished it independently.** Stop and
// DisconnectClient each closed the socket, so a read loop whose read failed
// or that found the end decided closed it under another goroutine's
// DISCONNECT: the client was told nothing, 67 of 100 0x95 DISCONNECTs with
// 20ms between deciding and writing, and every one where the client's read
// failed during the write. Claimed, the read loop's own end waits for the
// DISCONNECT to be written or to fail.
//
// **The owner needs nothing a waiter holds**: between its claim and its close
// it takes only the client's lock, for the DISCONNECT, and runs the hooks a
// refusal and a DISCONNECT run, and nothing waits for an end holding that
// lock (TestNothingWaitsForAnEndHoldingWhatItsOwnerNeeds). Every place that
// claims, and the close each owes, is TestEveryDecidedEndIsClosed's.
func (cl *Client) claimEnd() bool {
	for {
		switch st := cl.State.io.Load(); st {
		case ioOpen:
			if cl.State.io.CompareAndSwap(ioOpen, ioClaimed) {
				cl.releaseSlot()
				return true
			}
		case ioWriting, ioEndPending:
			if cl.State.io.CompareAndSwap(st, ioClaimPending) {
				return true
			}
		case ioEnded:
			if cl.State.io.CompareAndSwap(ioEnded, ioClaimed) {
				return true
			}
		case ioEndedQuiet:
			if cl.State.io.CompareAndSwap(ioEndedQuiet, ioClaimedQuiet) {
				return true
			}
		default:
			return false
		}
	}
}

// endClaimed reports whether the connection's end has an owner.
func (cl *Client) endClaimed() bool {
	switch cl.State.io.Load() {
	case ioClaimPending, ioClaimed, ioClaimedQuiet, ioConnackClaimed, ioConnackClaimedQuiet:
		return true
	}
	return false
}

// claimEndForConnack claims the end for the goroutine refusing the
// connection's CONNECT, which writes the refusing CONNACK and finishes the
// end when it returns (Server.endAttach, through Stop).
//
// **Claimed, because a shutdown reaches the connection too**: one that closed
// the socket of a connection being refused closed it under the CONNACK, and
// the client was told nothing (Listeners.CloseAll). The connection is not
// registered yet, so no other goroutine calls Stop on it: the Stop that
// follows on the refusing goroutine finishes the end, where another caller
// would wait for it (stop).
func (cl *Client) claimEndForConnack() bool {
	if cl.State.io.CompareAndSwap(ioOpen, ioConnackClaimed) {
		cl.releaseSlot()
		return true
	}
	return false
}

// connackOwnsEnd reports whether the end is the refusing goroutine's
// (claimEndForConnack).
func (cl *Client) connackOwnsEnd() bool {
	st := cl.State.io.Load()
	return st == ioConnackClaimed || st == ioConnackClaimedQuiet
}

// cutWrites makes every write to the connection fail at once, where nothing
// else bounds them: no limits.write_timeout and no keepalive. A shutdown or a
// takeover asks it of a connection whose end another goroutine owns, or whose
// lock a write holds, so that the write fails and the owner finishes
// (invariant 16); a bound in the waiter instead would close the socket under
// that write.
func (cl *Client) cutWrites() {
	if cl.Net.Conn != nil && cl.writeDeadline().IsZero() {
		_ = cl.Net.Conn.SetWriteDeadline(time.Now())
	}
}

// writeUnderWay reports whether a write holding the client's lock was under
// way as the end was claimed.
func (cl *Client) writeUnderWay() bool { return cl.State.io.Load() == ioClaimPending }

// finishEnd closes the connection for the owner of its end (claimEnd), with
// the reason it ended, and lets every goroutine waiting for the close go.
func (cl *Client) finishEnd(err error) {
	cl.State.endOnce.Do(func() {
		if cl.Net.Conn != nil {
			_ = cl.Net.Conn.Close() // omit close error
		}

		// Kept before the waiters are let go, which read it (endedBy).
		cl.recordCause(err)

		if cl.State.cancelOpen != nil {
			cl.State.cancelOpen()
		}

		cl.State.disconnected.Store(time.Now().UnixNano())
	})
}

// awaitEnd waits until the owner of the connection's end has closed it.
//
// **With no bound of its own.** The owner waits for the client's lock and
// writes its DISCONNECT, and each is bounded by the write deadline - its own
// and that of the write holding the lock - where one is armed:
// limits.write_timeout, or the keepalive where that is unset. With
// write_timeout none and a client that set no keepalive there is no bound,
// which is the operator's choice of writes without one, and this wait has
// none either. A bound here would close the socket under the owner's write,
// which is the defect claimEnd exists for.
func (cl *Client) awaitEnd() {
	if cl.State.open != nil {
		<-cl.State.open.Done()
	}
}

// errEnding is a write refused because the connection is ending
// (beginWrite), before a byte of it was written.
var errEnding = errors.New("connection ending")

// ending reports whether the connection's end has been decided.
func (cl *Client) ending() bool { return cl.State.io.Load() >= ioEndPending }

// releaseSlot gives back the slot, once (beginEnd).
func (cl *Client) releaseSlot() {
	if f := cl.State.slot.Swap(nil); f != nil {
		(*f)()
	}
}

// beginWrite is asked by every write holding the client's lock, before it
// writes a byte, and answers whether it may write and whether it is the
// write under way that the end may wait for (endWrite). Once the connection
// is ending only terminal may write, and only one: a refusing CONNACK on an
// end decided, or the DISCONNECT its owner writes on an end claimed.
func (cl *Client) beginWrite(terminal bool) (ok, writing bool) {
	for {
		switch st := cl.State.io.Load(); st {
		case ioOpen:
			if cl.State.io.CompareAndSwap(ioOpen, ioWriting) {
				return true, true
			}
		case ioEnded:
			return terminal && cl.State.io.CompareAndSwap(ioEnded, ioEndedQuiet), false
		case ioClaimed:
			return terminal && cl.State.io.CompareAndSwap(ioClaimed, ioClaimedQuiet), false
		case ioConnackClaimed:
			return terminal && cl.State.io.CompareAndSwap(ioConnackClaimed, ioConnackClaimedQuiet), false
		default:
			// Quiet, or ioWriting, ioEndPending or ioClaimPending, which
			// belong to a write holding the lock this caller now holds: not
			// reached.
			return false, false
		}
	}
}

// endWrite finishes what beginWrite began, still holding the client's
// lock, and gives the slot back where the end was decided meanwhile.
//
// **A write that failed ends the connection here, and quietly**: it may have
// put part of a packet on the wire, so the DISCONNECT an owner would write
// next would land inside it. Its reason is kept as the end's before anybody
// can see the end, so whoever claims it - this writer as it stops, or a read
// loop finding it ended - ends it for that reason.
func (cl *Client) endWrite(writing bool, failed error) {
	if !writing {
		return
	}
	cl.recordCause(failed)
	for {
		st := cl.State.io.Load()
		next := st
		switch st {
		case ioWriting:
			next = ioOpen
			if failed != nil {
				next = ioEndedQuiet
			}
		case ioEndPending:
			next = ioEnded
			if failed != nil {
				next = ioEndedQuiet
			}
		case ioClaimPending:
			next = ioClaimed
			if failed != nil {
				next = ioClaimedQuiet
			}
		default:
			return // not reached: only a write under way is here
		}
		if cl.State.io.CompareAndSwap(st, next) {
			if next != ioOpen {
				cl.releaseSlot()
			}
			return
		}
	}
}

// Stop ends the connection with err, where its end has no owner yet, and
// otherwise waits for the owner to close it (claimEnd).
//
// The slot goes back first, so it is free by the time the close is seen,
// on every path that ends here without deciding earlier.
func (cl *Client) Stop(err error) { cl.stop(err) }

// stop is Stop, reporting whether this call owned the end. An end the
// goroutine refusing the CONNECT claimed is finished by its own Stop, the
// only one that reaches an unregistered connection (claimEndForConnack).
func (cl *Client) stop(err error) bool {
	if cl.claimEnd() || cl.connackOwnsEnd() {
		cl.finishEnd(err)
		return true
	}
	cl.awaitEnd()
	return false
}

// StopCause returns the reason the client connection was stopped, if any.
func (cl *Client) StopCause() error {
	if v := cl.State.stopCause.Load(); v != nil {
		return v.(stopReason).err
	}
	return nil
}

// stopReason is what stopCause holds: one concrete type, as an atomic.Value
// requires, whatever the error.
type stopReason struct{ err error }

// recordCause keeps err as the reason the connection ended, where none is
// kept yet: the first reason stands.
func (cl *Client) recordCause(err error) {
	if err != nil {
		cl.State.stopCause.CompareAndSwap(nil, stopReason{err})
	}
}

// StopTime returns the moment the client disconnected, else the zero time.
//
// **To the nanosecond, because a lifetime is judged against it.** Held in
// whole seconds it was two truncations away from the answer: the moment
// rounded down when it was stored, the clock rounded down when it was
// asked, and a session whose interval had not passed was judged expired by
// as much as a second. See sessionExpired.
func (cl *Client) StopTime() time.Time {
	ns := cl.State.disconnected.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Closed returns true if client connection is closed.
func (cl *Client) Closed() bool {
	return cl.State.open == nil || cl.State.open.Err() != nil
}

func (cl *Client) IsTakenOver() bool {
	return cl.State.isTakenOver.Load()
}

// EndsWithConnection reports whether the client's session ends when its
// network connection closes: an MQTT 5 session with no Session Expiry
// Interval (section 3.1.2.11.2), or a 3.1.1 one with Clean Session set. A
// takeover closes that connection, so it ends such a session too (saguin).
func (cl *Client) EndsWithConnection() bool {
	if cl.Properties.ProtocolVersion == 5 {
		return cl.Properties.Props.SessionExpiryInterval == 0
	}
	return cl.Properties.Clean
}

// BeginHangUp marks the connection as one the broker has begun to end, and
// reports whether this call was the first to: a connection is hung up once,
// however many goroutines decide it should be. A flag on the connection
// rather than an entry anywhere else, so it goes when the connection does.
func (cl *Client) BeginHangUp() bool {
	return cl.State.hangingUp.CompareAndSwap(false, true)
}

// HangingUp reports whether the broker has begun to end the connection, which
// is then chosen for nothing more while it closes.
func (cl *Client) HangingUp() bool {
	return cl.State.hangingUp.Load()
}

// ReadFixedHeader reads in the values of the next packet's fixed header.
func (cl *Client) ReadFixedHeader(fh *packets.FixedHeader) error {
	if cl.Net.bconn == nil {
		return ErrConnectionClosed
	}

	b, err := cl.Net.bconn.ReadByte()
	if err != nil {
		return err
	}

	err = fh.Decode(b)
	if err != nil {
		return err
	}

	var bu int
	fh.Remaining, bu, err = packets.DecodeLength(cl.Net.bconn)
	if err != nil {
		return err
	}

	// **The whole packet**, as MQTT 5 §3.1.2.11.4 defines the size: the fixed
	// header byte, the bu bytes of the Remaining Length itself, and the
	// Remaining Length. Leaving bu out admitted a packet up to four bytes over
	// the maximum this broker advertised; mosquitto counts all three.
	if max := cl.ops.options.Capabilities.MaximumPacketSize; max > 0 && uint64(1+bu)+uint64(fh.Remaining) > uint64(max) {
		// **The body is read off the socket and thrown away before the
		// error goes up**, and that is what lets the client be told.
		//
		// DisconnectClient already answers this with 0x95, and it never
		// arrived: the client is still writing a packet the broker has
		// stopped reading, so closing the connection with unread bytes in
		// the receive queue sends a TCP reset, and the reset overtakes the
		// DISCONNECT. The client saw a dead socket and no reason.
		// mosquitto reads the packet out and then answers, which is the
		// behaviour this copies.
		//
		// **Bounded three ways**, because reading bytes for a client that
		// is about to be disconnected is work an attacker would choose:
		// the count is what the client itself declared and a Variable Byte
		// Integer caps that at 268,435,455; the bytes go to io.Discard so
		// the cost is constant memory whatever the number; and the read
		// deadline set before this call still applies, so a client that
		// declares a large packet and then dribbles it is cut off on
		// keepalive rather than held open.
		//
		// A read error here is discarded on purpose. The packet is refused
		// either way, and the reason the caller needs is the size rather
		// than whatever went wrong while emptying a connection that is
		// closing.
		_, _ = io.CopyN(io.Discard, cl.Net.bconn, int64(fh.Remaining))
		return packets.ErrPacketTooLarge // [MQTT-3.2.2-15]
	}

	return nil
}

// ReadPacket reads the remaining buffer into an MQTT packet.
func (cl *Client) ReadPacket(fh *packets.FixedHeader) (pk packets.Packet, err error) {
	pk, err = cl.decodePacket(fh)
	if err != nil {
		return pk, err
	}
	return cl.ops.hooks.OnPacketRead(cl, pk)
}

// decodePacket reads and decodes the packet whose fixed header has been
// read, and hands it to no hook (ReadPacket).
func (cl *Client) decodePacket(fh *packets.FixedHeader) (pk packets.Packet, err error) {

	pk.ProtocolVersion = cl.Properties.ProtocolVersion // inherit client protocol version for decoding
	pk.FixedHeader = *fh
	// **A read buffer is never recycled**, and payloads depend on it: a
	// publish's payload is a slice of what is read here, and it is kept by
	// reference - by every subscriber's delivery (CopySharingPayload), by the
	// session store's hold of it (the broker's deliveryRecord), which lasts
	// until a session that is away comes back, and by the retained store.
	// TestNothingWritesIntoAPayload catches a write into one; it cannot see a
	// pool. Reading into a pooled or reused buffer would change messages
	// those holders already have.
	p := make([]byte, pk.FixedHeader.Remaining)
	if _, err := io.ReadFull(cl.Net.bconn, p); err != nil {
		return pk, err
	}

	// **Decoded from p itself**, which is already this packet's own: it was
	// made above for this packet and nothing else holds it, so the next
	// packet cannot change it. The copy that used to be taken here was a
	// second allocation and memmove of every packet for nothing.
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectDecode(p)
	case packets.Disconnect:
		err = pk.DisconnectDecode(p)
	case packets.Connack:
		err = pk.ConnackDecode(p)
	case packets.Publish:
		err = pk.PublishDecode(p)
		if err == nil {
			cl.ops.info.MessagesReceived.Add(1)
		}
	case packets.Puback:
		err = pk.PubackDecode(p)
	case packets.Pubrec:
		err = pk.PubrecDecode(p)
	case packets.Pubrel:
		err = pk.PubrelDecode(p)
	case packets.Pubcomp:
		err = pk.PubcompDecode(p)
	case packets.Subscribe:
		err = pk.SubscribeDecode(p)
	case packets.Suback:
		err = pk.SubackDecode(p)
	case packets.Unsubscribe:
		err = pk.UnsubscribeDecode(p)
	case packets.Unsuback:
		err = pk.UnsubackDecode(p)
	case packets.Pingreq:
		err = pk.PingreqDecode(p)
	case packets.Pingresp:
		err = pk.PingrespDecode(p)
	case packets.Auth:
		err = pk.AuthDecode(p)
	default:
		err = fmt.Errorf("invalid packet type; %v", pk.FixedHeader.Type)
	}

	return pk, err
}

// WritePacket encodes and writes a packet to the client - except a PUBLISH
// while a resumed session's resend is still to be written, which waits for
// the write loop behind it (holdForResend).
func (cl *Client) WritePacket(pk packets.Packet) error {
	// **A packet that ends the connection gives its slot back before it is
	// written** (beginEnd): a CONNACK refusing it [MQTT-3.2.2-7], whoever
	// writes it - the substrate, or saguin's own to a 3.1.1 client - and the
	// server's DISCONNECT. The client may connect again the moment it reads
	// either.
	//
	// **A DISCONNECT is written only by the owner of the end** (claimEnd),
	// which closes the connection once it is written or has failed.
	if pk.FixedHeader.Type == packets.Disconnect {
		if !cl.endClaimed() {
			return ErrConnectionClosed
		}
		return cl.writePacketAs(pk, true)
	}
	if pk.FixedHeader.Type == packets.Connack && pk.ReasonCode != packets.CodeSuccess.Code {
		if !cl.claimEndForConnack() {
			return ErrConnectionClosed
		}
		return cl.writePacketAs(pk, true)
	}
	if pk.FixedHeader.Type == packets.Publish && cl.State.resuming.Load() {
		return cl.holdForResend(pk)
	}
	return cl.writePacket(pk)
}

// holdForResend leaves a delivery made while a resumed session's resend is
// still to be written for the write loop, which starts once the resend is
// on the wire (endResend): queued if the queue has room, and otherwise,
// being in flight already at QoS 1 or 2, withheld there, as makeDelivery
// leaves one it could not queue. A QoS 0 one with no room is dropped and
// counted as a full queue is.
//
// **The resend goes first** [MQTT-4.6.0-6]. It is written on the CONNECT
// goroutine once the connection is registered, and a sender that reached
// the connection in between - a shared group, a channel pump, a live
// fan-out - wrote straight to the socket or through the write loop, either
// of which can be first. A newer message from a publisher then reached the
// client ahead of an older one from the same publisher being sent again:
// reproduced every time with the delivery made in that window
// (TestAResumedSessionsResendGoesAheadOfWhatArrivesMeanwhile). mosquitto
// writes a client's resend and its new messages from one thread and one
// list, so the two cannot change places there.
func (cl *Client) holdForResend(pk packets.Packet) error {
	p := pk
	if cl.enqueue(&p) {
		return nil
	}
	if pk.FixedHeader.Qos > 0 {
		if _, ok := cl.State.Inflight.Get(pk.PacketID); ok {
			// Its sender took a slot of window as it wrote; the claim that
			// writes it takes one again (drainWithheld).
			cl.State.Inflight.Withhold(pk.PacketID)
			cl.State.Inflight.IncreaseSendQuota()
			cl.WakeWriter()
			return nil
		}
	}
	if cl.State.outbound.ended() {
		return packets.CodeDisconnect
	}
	cl.ops.hooks.OnPublishDropped(cl, pk)
	return packets.ErrPendingClientWritesExceeded
}

// endResend lets what arrived during a resumed session's resend be written,
// now that the resend is.
func (cl *Client) endResend() {
	cl.State.resuming.Store(false)
	if cl.State.outbound.len() > 0 || len(cl.State.wake) > 0 {
		cl.startWriter()
	}
}

func (cl *Client) writePacket(pk packets.Packet) error { return cl.writePacketAs(pk, false) }

// writePacketAs is writePacket, for the packet that ends the connection
// where terminal is set (beginWrite).
func (cl *Client) writePacketAs(pk packets.Packet, terminal bool) error {
	if cl.Closed() {
		return ErrConnectionClosed
	}

	if cl.Net.Conn == nil {
		return nil
	}

	if pk.Expiry > 0 {
		expiry := pk.Expiry - time.Now().Unix()
		if expiry < 1 {
			expiry = 1
		}
		pk.Properties.MessageExpiryInterval = uint32(expiry) // [MQTT-3.3.2-6]
	}

	pk.ProtocolVersion = cl.Properties.ProtocolVersion
	if pk.Mods.MaxSize == 0 { // NB we use this statement to embed client packet sizes in tests
		pk.Mods.MaxSize = cl.Properties.Props.MaximumPacketSize
	}

	if cl.Properties.Props.RequestProblemInfoFlag && cl.Properties.Props.RequestProblemInfo == 0x0 &&
		pk.FixedHeader.Type != packets.Publish {
		// [MQTT-3.1.2-29] the server must not send a Reason String or User
		// Properties on any packet other than PUBLISH, CONNACK or
		// DISCONNECT. PUBLISH is exempt: its User Properties are
		// application data forwarded from the publisher, not problem
		// information about a failure.
		pk.Mods.DisallowProblemInfo = true
	}

	// Response Information goes in a CONNACK only where the client asked
	// for it. mochi had an AlwaysReturnResponseInfo mode that sent it
	// regardless, described in its own source as useful for testing;
	// nothing in saguin set it, and [MQTT-3.1.2-28] is what this line is.
	if pk.FixedHeader.Type != packets.Connack || cl.Properties.Props.RequestResponseInfo == 0x1 {
		pk.Mods.AllowResponseInfo = true // [MQTT-3.1.2-28] we need to know which properties we can encode
	}

	pk = cl.ops.hooks.OnPacketEncode(cl, pk)

	var err error
	buf := encodeBufs.Get()
	defer encodeBufs.Put(buf)
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(buf)
	case packets.Connack:
		err = pk.ConnackEncode(buf)
	case packets.Publish:
		err = pk.PublishEncode(buf)
	case packets.Puback:
		err = pk.PubackEncode(buf)
	case packets.Pubrec:
		err = pk.PubrecEncode(buf)
	case packets.Pubrel:
		err = pk.PubrelEncode(buf)
	case packets.Pubcomp:
		err = pk.PubcompEncode(buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(buf)
	case packets.Suback:
		err = pk.SubackEncode(buf)
	case packets.Unsubscribe:
		err = pk.UnsubscribeEncode(buf)
	case packets.Unsuback:
		err = pk.UnsubackEncode(buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(buf)
	case packets.Pingresp:
		err = pk.PingrespEncode(buf)
	case packets.Disconnect:
		err = pk.DisconnectEncode(buf)
	case packets.Auth:
		err = pk.AuthEncode(buf)
	default:
		err = fmt.Errorf("%w: %v", packets.ErrNoValidPacketAvailable, pk.FixedHeader.Type)
	}
	if err != nil {
		return err
	}

	if pk.Mods.MaxSize > 0 && uint32(buf.Len()) > pk.Mods.MaxSize {
		return packets.ErrPacketTooLarge // [MQTT-3.1.2-24] [MQTT-3.1.2-25]
	}

	n, err := func() (_ int64, err error) {
		cl.Lock()
		defer cl.Unlock()
		// Asked holding the lock, where no end can be decided between the
		// answer and the write (beginEnd).
		ok, writing := cl.beginWrite(terminal)
		if !ok {
			return 0, errEnding
		}
		defer func() { cl.endWrite(writing, err) }()

		// Armed inside the lock and cleared before it is released, so that
		// the deadline belongs to this write and to no other. A connection
		// deadline is a property of the connection rather than of a write,
		// so arming it outside would let one goroutine clear the deadline
		// another is relying on.
		//
		// The lock is the reason this matters at all: every write below
		// happens while it is held, so a write that never completes holds
		// the client's lock for as long as the client stays connected, and
		// every write to this client waits behind it. It no longer holds up
		// a publisher: a packet identifier is taken without this lock
		// (NextPacketID), and a delivery is written by this client's own
		// write loop rather than by the goroutine that published it.
		// The connection is read once and held, so that the deadline is
		// cleared on the connection it was armed on rather than on
		// whatever cl.Net.Conn names by the time the write returns.
		// **Unset does not mean unbounded**, and this is the half that is
		// easy to get wrong. refreshDeadline used to arm SetDeadline, which
		// set the read AND write deadlines, so every release before this one
		// bounded a write to a stalled client at the keepalive expiry.
		// Arming only the read deadline there - which is what stops the read
		// loop clearing a write deadline a queued write is relying on - takes
		// that bound away, and a broker upgrading with this option unset
		// would be worse off than before: a write could block forever holding
		// the lock, which is the deadlock this option exists to prevent.
		//
		// So the option is an override, and its absence falls back to what
		// the keepalive already promised. A client with no keepalive gets no
		// bound, which is also what it got before.
		if d := cl.writeDeadline(); !d.IsZero() {
			if conn := cl.Net.Conn; conn != nil {
				if err := conn.SetWriteDeadline(d); err == nil {
					defer conn.SetWriteDeadline(time.Time{})
				}
			}
		}

		// **A DISCONNECT is never parked**, and carries out what was: it is
		// the last packet on the connection, which is closed behind it, so a
		// queued packet that would have carried it is never written.
		if cl.State.outbound.len() == 0 || pk.FixedHeader.Type == packets.Disconnect {
			if cl.Net.outbuf == nil {
				return buf.WriteTo(cl.Net.Conn)
			}

			// first write to buffer, then flush buffer
			n, _ := cl.Net.outbuf.Write(buf.Bytes()) // will always be successful
			err = cl.flushOutbuf()
			return int64(n), err
		}

		// there are more writes in the queue
		if cl.Net.outbuf == nil {
			if buf.Len() >= cl.ops.options.ClientNetWriteBufferSize {
				return buf.WriteTo(cl.Net.Conn)
			}
			cl.Net.outbuf = new(bytes.Buffer)
		}

		n, _ := cl.Net.outbuf.Write(buf.Bytes()) // will always be successful
		if cl.Net.outbuf.Len() < cl.ops.options.ClientNetWriteBufferSize {
			return int64(n), nil
		}

		err = cl.flushOutbuf()
		return int64(n), err
	}()
	if errors.Is(err, errEnding) {
		// Refused before a byte was written, on a connection already ending:
		// stopping it here would close it before what its end has still to
		// do, such as storing what its DISCONNECT changed (invariant 18).
		return ErrConnectionClosed
	}
	if err != nil {
		// **A write that fails on the connection ends the client, whatever
		// the reason.** It may have put part of a packet on the wire, and
		// the next packet written would land inside it - a peer parsing the
		// tail of one packet as the head of the next. Only a timeout used
		// to stop the client, and only from the write loop, so a reset or a
		// broken pipe on an acknowledgement or a delivery left it open and
		// written to.
		//
		// Everything returned before this point - a closed client, a packet
		// that could not be encoded, one larger than the peer accepts - is
		// refused before a byte is written, and leaves the stream intact.
		//
		// The packet that ends the connection is its owner's, or the
		// refusing CONNACK of a connection no other goroutine knows yet,
		// and whoever wrote it closes it (claimEnd).
		if !terminal {
			cl.Stop(err)
		}
		return err
	}

	cl.ops.info.BytesSent.Add(n)
	if pk.FixedHeader.Type == packets.Publish {
		cl.ops.info.MessagesSent.Add(1)
	}

	cl.ops.hooks.OnPacketSent(cl, pk, buf.Bytes())

	return err
}

// encodeBufs are the buffers writePacket encodes into, each back in the pool
// once the packet is written and OnPacketSent has returned.
//
// **Pooled because two were made for every message**, the delivery and
// the publisher's PUBACK, and each grew once past its first 64 bytes: three
// allocations and 1.3 µs of broker CPU a QoS 1 message at 10,000 msg/s.
// Nothing holds the bytes past the return: the connection's Write must not
// keep them (io.Writer), outbuf copies them, and OnPacketSent is handed
// them for the call only. One grown past 64 KiB is left to the collector
// rather than kept, so one large publish does not hold its size in the pool
// for as long as the pool lives.
var encodeBufs = mempool.NewBuffer(64 * 1024)

func (cl *Client) flushOutbuf() (err error) {
	if cl.Net.outbuf == nil {
		return
	}

	_, err = cl.Net.outbuf.WriteTo(cl.Net.Conn)
	if err == nil {
		cl.Net.outbuf = nil
	}
	return
}

// isTimeout reports whether an error is a network timeout, which is what a
// write that ran out of time under ClientNetWriteTimeout produces.
//
// net.Error's Timeout rather than errors.Is against os.ErrDeadlineExceeded:
// the two agree for a plain TCP write, and do not for every listener. A
// websocket connection writes through gorilla, whose buffered writev path
// yields "writev tcp …: i/o timeout" - a timeout by every definition that
// matters, and not that sentinel. Checking the sentinel silently covered
// one transport and not another.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
