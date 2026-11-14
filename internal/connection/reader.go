package connection

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/topic"
)

type reader struct {
	conn             *Conn
	limits           config.Limits
	minimum          packet.Version
	inbound          chan<- Event
	metrics          *Metrics
	deadline         time.Time
	proxyDeadline    time.Time
	version          packet.Version
	connected        bool          // a checked CONNECT, including a broker refusal
	problemInfo      bool          // CONNECT's Request Problem Information
	keepAlive        time.Duration // already multiplied by 1.5
	expiry           uint32
	header           headerInput
	available        int               // reply slots already taken from the writer
	identifiers      map[uint16]uint32 // held receive-quota generations, never publication state
	inflight         int               // QoS 1/2 PUBLISHes not yet settled by a written reply
	connackWritten   bool              // the writer has sent CONNECT's successful result
	ackPacket        uint16            // window-releasing acknowledgement currently being written
	quotaSequence    uint32            // new generations follow the restored identifiers (1..65535)
	ackWriting       bool              // a window-releasing write has begun but not ended
	receiveMaximum   uint32
	bridgeRules      []config.BridgeRule // immutable inbound rules; nil on accepted clients
	bridgeConnect    *packet.Connect     // sent only after this bridge's transport setup succeeds
	websocketURL     string              // bridge upgrade target; empty on other connections
	captures         topic.Captures      // reader-owned substitution scratch
	topicRoom        int64               // extra body grant for a bridge's name-building rules
	retainedMaximum  int64
	routing          *config.Routing // immutable channel filters, shared by this listener's readers
	passwords        *passwd.File    // immutable credentials, shared by this listener's readers
	live             *config.Reloads
	passwordPath     string
	accessCopy       *config.AccessCopy
	access           *acl.Client // reader-owned compiled grants; never changes Conn.access
	connectAccess    *acl.Client // immutable capabilities advertised at CONNECT
	negotiatedDenied acl.Feature // taken from the successful CONNACK, separate from current access
	clientID         string
	baseRate         int
	baseBytes        config.Size
	publishRate      int
	publishBytes     config.Size
	acl              *acl.File // immutable access file; nil costs no compilation
	requireUser      bool
	certificate      *x509.Certificate         // verified chain's leaf, never an unverified peer
	commonName       string                    // a complete, verified PROXY SSL record's name
	proxyVerified    bool                      // selects that identity even when its name is empty
	failure          error                     // transport or identity failure reported with Ended
	aliases          [TopicAliasMaximum][]byte // immutable names, owned by this connection
	rate             publishBuckets
	rateTimer        <-chan time.Time // created on the first rate wait, stopped between waits
	resetRateTimer   func(time.Time)
}

// headerInput supplies the codec's ByteReader without body prefetch (R6).
// Its two-byte scratch is part of the reader, avoiding an escaping adapter
// or scratch allocation per packet. No packet buffer is kept between reads.
type headerInput struct {
	reader  *reader
	scratch [2]byte // only the first byte and first Remaining Length byte
	pos     int
}

// A body-silence cutoff is separate from keep-alive and has no MQTT
// DISCONNECT reason. Preserve it through the ordinary reader-end path.
var errPublishSilence = errors.New("PUBLISH body silence timeout")

// readFull uses the socket directly, counting even an incomplete read. No
// io.Reader wrapper can move connection code onto another caller's goroutine.
func (r *reader) readFull(buf []byte) error {
	return r.readBytes(buf, 0)
}

// readBytes shares the socket/metric loop for all input. A nonzero silence
// duration belongs only to a PUBLISH body: arm before its first read, then
// renew after actual progress, never after a zero-byte read or while waiting
// for a grant. Keep-alive remains the deadline between packets.
func (r *reader) readBytes(buf []byte, silence time.Duration) error {
	read := 0
	progress := true
	for read < len(buf) {
		if r.stopRequested() {
			return net.ErrClosed
		}
		if silence != 0 && progress {
			if err := r.conn.raw.SetReadDeadline(r.conn.clock.Now().Add(silence)); err != nil {
				return err
			}
			// Do not read after a deadline update raced the writer's stop.
			if r.stopRequested() {
				return net.ErrClosed
			}
		}
		n, err := r.conn.transport().Read(buf[read:])
		read += n
		progress = n != 0
		r.metrics.receivedBytes.Add(uint64(n))
		// A complete read survives stop; the loop checks before reading more.
		// The socket's read deadline alone decides whether bytes are in time.
		if read == len(buf) {
			return nil
		}
		if err != nil {
			var timeout net.Error
			if silence != 0 && errors.As(err, &timeout) && timeout.Timeout() {
				return errPublishSilence
			}
			return err
		}
	}
	return nil
}

func (in *headerInput) ReadByte() (byte, error) {
	if in.pos < len(in.scratch) {
		b := in.scratch[in.pos]
		in.pos++
		return b, nil
	}
	err := in.reader.readFull(in.scratch[:1])
	return in.scratch[0], err
}

// run completes setup and reads CONNECT or a bridge's CONNACK under the
// establishment deadline. Later packet deadlines begin with their reading.
func (r *reader) run() {
	ready := r.setup()
	if r.conn.client {
		// Publish the completed stream before the writer touches it. This
		// first handover uses the existing connection-settings channel;
		// CONNACK supplies negotiated settings later on the same channel.
		r.conn.peer <- peerSettings{version: packet.V5}
	}
	if !ready {
		return
	}
	if r.conn.client {
		r.conn.replies <- r.bridgeConnect
		r.bridgeConnect = nil
	}
	r.readPackets()
}

// setup runs entirely on the reader. Its original deadline covers TLS,
// a bridge's WebSocket upgrade and the MQTT packet that follows them.
func (r *reader) setup() bool {
	if !r.proxyDeadline.IsZero() && !r.proxy(r.proxyDeadline) {
		return false
	}
	if err := r.conn.raw.SetReadDeadline(r.deadline); err != nil {
		r.failed(err)
		return false
	}
	if secure, ok := r.conn.raw.(*tls.Conn); ok && !r.handshake(secure) {
		return false
	}
	if r.websocketURL != "" {
		stream, err := dialWebSocket(r.conn.raw, r.websocketURL, r.deadline, r.conn.established)
		if err != nil {
			r.failed(err)
			return false
		}
		r.conn.stream = stream
		// Gorilla clears setup deadlines after a successful upgrade.
		// CONNACK still has only the remainder of this attempt's budget.
		if err := r.conn.raw.SetReadDeadline(r.deadline); err != nil {
			r.failed(err)
			return false
		}
	}
	return true
}

// readPackets is shared by socket readers and net/http's upgraded reader.
// Transport setup has finished; the original CONNECT deadline still applies.
func (r *reader) readPackets() {
	for {
		// Rate allowance and a reply slot precede even the next fixed header.
		// Both waits wake on the writer's stop before another read begins.
		if !r.waitPublish() || !r.takeSlot() {
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
			return
		}
		if r.connected {
			r.deadline = time.Time{}
			if !r.conn.client && r.keepAlive != 0 {
				r.deadline = r.conn.clock.Now().Add(r.keepAlive)
			}
			if err := r.conn.raw.SetReadDeadline(r.deadline); err != nil {
				r.available++
				r.failed(err)
				return
			}
		}
		// Check after updating the deadline: a racing update may overwrite
		// the writer's interrupt, but must then stop without another read.
		select {
		case <-r.conn.stopping:
			r.available++
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
			return
		default:
		}
		ev, err := r.next()
		if err != nil {
			r.available++ // this packet never spent its reservation
			r.failed(err)
			return
		}
		if ev.Kind == Ended {
			r.available++
			_, client := ev.Packet.(*packet.Disconnect)
			r.end(ev, readEnd{client: client})
			return
		}
		_, connect := ev.Packet.(*packet.Connect)
		connectRefused := connect && ev.Reason != 0
		// QoS 0 has no reply; refusing it still goes to the core to return
		// its body bytes.
		switch p := ev.Packet.(type) {
		case *PeerConnack, *packet.Suback:
			r.available++ // client-side replies owe the peer no answer
		case Pingresp:
			r.available++
			continue // already reported to this connection's keep-alive owner
		case *RoutedPublish:
			if p.Publish.QoS == 0 {
				r.available++
			}
		case *packet.Publish:
			if p.QoS == 0 {
				r.available++
			}
		case *RequestPublish:
			if p.QoS == 0 {
				r.available++
			}
		case PublishRefusal:
			if p.QoS == 0 {
				r.available++
			}
		case *Ack:
			if p.Type == packet.PUBCOMP {
				r.available++
			}
		case *packet.Ack:
			r.available++ // incoming acknowledgements consume no reply slot
		}
		if !r.hand(ev) {
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
			return
		}
		// A sent packet belongs to the core. Clearing this reference also
		// ensures that a blocked next read retains no previous packet (R6).
		ev = Event{}
		if connectRefused {
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
			return
		}
	}
}

// stopRequested observes the writer's request before any further reading.
func (r *reader) stopRequested() bool {
	select {
	case <-r.conn.stopping:
		return true
	default:
		return false
	}
}

// hand checks stop before starting a handover. A started handover sends
// exactly once and finishes before the reader reports its end; stop prevents
// the next handover (R2, R3).
func (r *reader) hand(ev Event) bool {
	if r.stopRequested() {
		return false
	}
	r.inbound <- ev
	return true
}

// next confines the packet's buffer to one read. The codecs may return
// Binary Data sharing it; that buffer is handed on and is never reused (R3).
func (r *reader) next() (Event, error) {
	// Two bytes is the shortest complete header. Never ask for more:
	// a complete body still needs its size check and the core's grant.
	in := &r.header
	in.reader, in.pos = r, 0
	if err := r.readFull(in.scratch[:]); err != nil {
		return Event{}, err
	}
	version, limit := r.version, r.limits.MaxPacketSize
	if !r.connected && !r.conn.client {
		version, limit = packet.V5, r.limits.MaxConnectSize
	}
	h, err := packet.ReadHeader(in, version, int(limit))
	if !r.connected && h.Type != 0 {
		first := packet.CONNECT
		if r.conn.client {
			first = packet.CONNACK
		}
		if h.Type != first {
			return Event{}, packet.ErrProtocol
		}
	}
	if !r.connected && h.Type == packet.CONNECT && errors.Is(err, packet.ErrTooLarge) {
		p, err := r.oversizeConnect(h.Length)
		if errors.Is(err, packet.ErrUnsupportedVersion) {
			return r.connect(&p, 0x84), nil
		}
		if err != nil {
			return Event{}, err
		}
		return r.connect(&p, 0x95), nil
	}
	if err != nil {
		return Event{}, err
	}
	if r.connected {
		if !r.allowed(h.Type) {
			return Event{}, packet.ErrProtocol
		}
	}
	if (h.Type == packet.PINGREQ || h.Type == packet.PINGRESP) && h.Length != 0 {
		return Event{}, packet.ErrMalformed // PINGREQ has no body to read
	}
	// Read the header before waiting for a bridge's restored identifiers so
	// PINGRESP reaches its keep-alive owner during replay. Every other packet
	// still waits for restoration, including publications at QoS 0.
	if r.conn.client && r.connected && !r.connackWritten && h.Type != packet.PINGRESP && !r.waitConnack() {
		return Event{}, net.ErrClosed
	}
	if h.Type == packet.PUBLISH {
		qos, err := packet.PublishQoS(h.Flags)
		if err != nil {
			return Event{}, err
		}
		if qos == 2 && !r.conn.client && !r.connackWritten && !r.waitConnack() {
			return Event{}, net.ErrClosed
		}
		if qos == 2 && r.negotiatedDenial(acl.QoS2) {
			// The fixed header suffices: do not request bytes or read a
			// body for a QoS this connection cannot support (MQTT-3.2.2-11).
			return Event{Kind: Ended, Conn: r.conn, Reason: 0x9B}, nil
		}
		if !r.drainSlots() {
			return Event{}, net.ErrClosed
		}
		if qos == 1 && r.connackWritten && len(r.identifiers) == 0 &&
			r.receiveMaximum != 0 && uint32(r.inflight) >= r.receiveMaximum {
			// Without a possible held repeat, overflow can be decided
			// before reading. Admission is checked again after the body.
			admitted, err := r.waitWindow(0)
			if err != nil {
				return Event{}, err
			}
			if !admitted {
				return Event{Kind: Ended, Conn: r.conn, Reason: 0x93}, nil
			}
		}
		if err := r.askBytes(h.Length); err != nil {
			return Event{}, err
		}
	}
	body := make([]byte, h.Length)
	var silence time.Duration
	if h.Type == packet.PUBLISH {
		silence = r.limits.WriteTimeout
	}
	if err := r.readBytes(body, silence); err != nil {
		return Event{}, err
	}
	switch h.Type {
	case packet.PUBACK, packet.PUBREC, packet.PUBREL, packet.PUBCOMP:
		p, err := packet.DecodeAck(body, h.Type, version)
		if err != nil {
			return Event{}, err
		}
		if !r.conn.client && !validClientAck(h.Type, p.ReasonCode) { // No matching subscribers is server-only.
			return Event{}, packet.ErrProtocol
		}
		if h.Type != packet.PUBACK {
			return Event{Kind: Received, Conn: r.conn, Packet: &Ack{Type: h.Type, Ack: p}}, nil
		}
		return Event{Kind: Received, Conn: r.conn, Packet: &p}, nil
	case packet.SUBSCRIBE:
		return r.subscribe(body)
	case packet.SUBACK:
		p, err := packet.DecodeSuback(body, version)
		return Event{Kind: Received, Conn: r.conn, Packet: &p}, err
	case packet.CONNACK:
		return r.connack(body)
	case packet.UNSUBSCRIBE:
		return r.unsubscribe(body)
	case packet.PUBLISH:
		return r.publish(body, h.Flags, r.conn.clock.Now())
	case packet.CONNECT:
		v, err := packet.DecodeConnectVersion(body)
		if errors.Is(err, packet.ErrUnsupportedVersion) {
			return r.connect(&packet.Connect{}, 0x84), nil
		}
		if err != nil {
			return Event{}, err
		}
		if v < r.minimum {
			return r.connect(&packet.Connect{Version: v}, 0x84), nil
		}
		p, err := packet.DecodeConnectBody(body[7:], v)
		if err != nil {
			return Event{}, err
		}
		return r.connect(&p, 0), nil
	case packet.PINGREQ:
		return Event{Kind: Received, Conn: r.conn, Packet: Pingreq{}}, nil
	case packet.PINGRESP:
		if err := packet.DecodePingresp(body, version); err != nil {
			return Event{}, err
		}
		select {
		case r.conn.pongs <- r.conn.clock.Now():
		case <-r.conn.stopping:
			return Event{}, net.ErrClosed
		}
		return Event{Kind: Received, Conn: r.conn, Packet: Pingresp{}}, nil
	case packet.DISCONNECT:
		p, err := packet.DecodeDisconnect(body, version)
		if err != nil {
			return Event{}, err
		}
		if (r.conn.client && !validServerDisconnect(&p)) || (!r.conn.client && !validClientDisconnect(&p, r.expiry)) {
			return Event{}, packet.ErrProtocol
		}
		return Event{Kind: Ended, Conn: r.conn, Packet: &p}, nil
	default:
		return Event{}, packet.ErrProtocol
	}
}

// validClientDisconnect applies Table 3-10's sender column and the prohibition
// on increasing an originally zero Session Expiry Interval (3.14.2.2.2).
func validClientDisconnect(p *packet.Disconnect, expiry uint32) bool {
	if p == nil {
		return false
	}
	switch p.ReasonCode {
	case 0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99:
	default:
		return false
	}
	for prop := range p.Properties.Views() {
		if prop.ID == packet.SessionExpiryInterval && expiry == 0 && prop.Int != 0 {
			return false
		}
	}
	return true
}

func (r *reader) failed(err error) {
	if r.conn.client {
		r.failure = err
	}
	ev := Event{Kind: Ended, Conn: r.conn}
	end := readEnd{}
	select {
	case <-r.conn.stopping:
		// The writer closed this socket; that close is not a lost peer.
	default:
		var timeout net.Error
		switch {
		case errors.Is(err, errPublishSilence):
			end.publishStalled = true // close without a DISCONNECT
		case errors.Is(err, packet.ErrTopicAlias):
			ev.Reason = 0x94
		case errors.Is(err, packet.ErrMalformed):
			ev.Reason = 0x81
		case errors.Is(err, packet.ErrProtocol):
			ev.Reason = 0x82
		case errors.Is(err, packet.ErrTooLarge):
			ev.Reason = 0x95
		case errors.As(err, &timeout) && timeout.Timeout():
			if r.connected && !r.conn.client {
				ev.Reason = 0x8D
			} else {
				end.connectTimeout = true
			}
		default:
			end.lost = true
		}
	}
	r.end(ev, end)
}

func (r *reader) end(ev Event, end readEnd) {
	// A checked DISCONNECT remains the client's end, even at stop. Otherwise
	// stop fixes the outcome before the acknowledgement below, after which
	// Ended is the reader's only remaining core notification. A certificate
	// refusal already handed to the core keeps its log even if the writer
	// has finished the refusing CONNACK and requested stop meanwhile.
	identityFailure := r.connected && r.failure != nil
	if !end.client && !identityFailure && (r.stopRequested() || r.conn.cutByCore()) {
		ev, end = Event{Kind: Ended, Conn: r.conn}, readEnd{}
	} else if ev.Packet == nil && r.failure != nil {
		ev.Packet = ReadFailure{Err: r.failure}
	}
	// Preserve the observation before handing the socket to the writer:
	// another connection may already be served while that writer cleans up.
	var refusal *metrics.Counter
	switch {
	case end.connectTimeout && !r.conn.client:
		refusal = r.metrics.connectTimeout
	case end.tlsHandshake:
		refusal = r.metrics.tlsHandshake
	case end.proxyHeader:
		refusal = r.metrics.proxyHeader
	case !r.connected && !r.conn.client:
		refusal = r.metrics.refused[ev.Reason]
	}
	if refusal != nil {
		ev.Closure = r.metrics.closureReasons[refusal]
	}
	if refusal != nil || end.publishStalled {
		r.conn.readClosureAt = r.conn.clock.Now()
	}
	r.conn.ended <- end
	// Hand the socket to the writer before waiting for the core. From here
	// the reader neither reads nor changes deadlines (R2, R3).
	close(r.conn.readStopped)
	if refusal != nil {
		refusal.Inc()
	}
	r.inbound <- ev // the reader's last event, also when the core is paused
	// Socket handover precedes Ended. Only this later signal lets the writer
	// report Closed, preserving their order on the shared inbound channel.
	close(r.conn.endReported)
}
