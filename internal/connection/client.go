package connection

import (
	"time"

	"github.com/ifnesi/saguin/internal/packet"
)

// PeerConnack transfers the negotiated client settings to the core without
// retaining the CONNACK's packet buffer. The reader visits its properties
// once and publishes the writer's connection-local settings separately.
type PeerConnack struct {
	ReasonCode        byte
	SessionPresent    bool
	MaximumPacketSize uint32
	ReceiveMaximum    uint16
	MaximumQoS        byte
	RetainAvailable   bool
	WildcardAvailable bool
	TopicAliasMaximum uint16
	SessionExpiry     uint32
}

func (r *reader) allowed(typ packet.Type) bool {
	switch typ {
	case packet.DISCONNECT, packet.PUBLISH, packet.PUBACK, packet.PUBREC, packet.PUBREL, packet.PUBCOMP:
		return true
	case packet.SUBACK, packet.PINGRESP:
		return r.conn.client
	case packet.PINGREQ, packet.SUBSCRIBE, packet.UNSUBSCRIBE:
		return !r.conn.client
	default:
		return false // CONNECT/CONNACK again, or AUTH without a method
	}
}

func (r *reader) connack(body []byte) (Event, error) {
	p, err := packet.DecodeConnack(body, packet.V5)
	if err != nil {
		return Event{}, err
	}
	ack := &PeerConnack{ReasonCode: p.ReasonCode, SessionPresent: p.SessionPresent,
		MaximumQoS: 2, ReceiveMaximum: 65535, RetainAvailable: true, WildcardAvailable: true, SessionExpiry: r.expiry}
	peer := peerSettings{version: packet.V5, connected: p.ReasonCode == 0, keepAlive: 30 * time.Second}
	for prop := range p.Properties.Views() {
		switch prop.ID {
		case packet.MaximumPacketSize:
			peer.maxSize, ack.MaximumPacketSize = prop.Int, prop.Int
		case packet.TopicAliasMaximum:
			peer.aliases, ack.TopicAliasMaximum = uint16(prop.Int), uint16(prop.Int)
		case packet.ServerKeepAlive:
			peer.keepAlive = time.Duration(prop.Int) * time.Second
		case packet.ReceiveMaximum:
			ack.ReceiveMaximum = uint16(prop.Int)
		case packet.MaximumQoS:
			ack.MaximumQoS = byte(prop.Int)
		case packet.WildcardSubscriptionAvailable:
			ack.WildcardAvailable = prop.Int != 0
		case packet.RetainAvailable:
			ack.RetainAvailable = prop.Int != 0
		case packet.SessionExpiryInterval:
			ack.SessionExpiry = prop.Int
		case packet.AuthenticationMethod, packet.AuthenticationData, packet.ResponseInformation, packet.AssignedClientIdentifier:
			return Event{}, packet.ErrProtocol // none was requested in CONNECT
		}
	}
	if p.ReasonCode == 0 {
		close(r.conn.established) // release transport write caps as CONNACK takes effect
		r.connected = true        // receive admission waits for the core's restored identifiers
		r.deadline = time.Time{}
		if err := r.conn.raw.SetReadDeadline(r.deadline); err != nil {
			return Event{}, err
		}
		r.conn.peer <- peer
	}
	return Event{Kind: Received, Conn: r.conn, Packet: ack}, nil
}

// validServerDisconnect applies Table 3-10 and MQTT-3.14.2-2. The codec has
// already checked the reason set; only Disconnect with Will is client-only.
func validServerDisconnect(p *packet.Disconnect) bool {
	if p == nil || p.ReasonCode == 0x04 {
		return false
	}
	for prop := range p.Properties.Views() {
		if prop.ID == packet.SessionExpiryInterval {
			return false
		}
	}
	return true
}

func (w *writer) updatePeer() {
	select {
	case peer := <-w.conn.peer:
		w.setPeer(peer)
	default:
	}
}

func (w *writer) setPeer(peer peerSettings) {
	w.peer = peer
	if w.conn.client && peer.connected {
		w.connack = true
		w.establishment = time.Time{}
		w.keepAlive = peer.keepAlive / 2
		if w.keepAlive != 0 {
			w.pingAt = w.lastControl.Add(w.keepAlive)
		}
	}
}

// Only complete outgoing control packets restart the idle interval. A
// PINGREQ's response deadline is independent and is never postponed by them.
func (w *writer) controlWritten() {
	if w.conn.client {
		w.lastControl = w.conn.clock.Now()
		if w.keepAlive != 0 {
			w.pingAt = w.lastControl.Add(w.keepAlive)
		}
	}
}

func (w *writer) clientDeadline() time.Time {
	if w.closed || !w.connack {
		return time.Time{}
	}
	if !w.pongAt.IsZero() {
		return w.pongAt
	}
	return w.pingAt
}

func (w *writer) pong(at time.Time) {
	if !w.pongAt.IsZero() && !at.Before(w.pingBegan) && at.Before(w.pongAt) {
		w.pongAt = time.Time{}
	}
}

func (w *writer) readPongs() {
	// There is one outstanding ping and one buffered notification. Bound
	// work even if a peer continually sends unsolicited PINGRESP packets.
	select {
	case at := <-w.conn.pongs:
		w.pong(at)
	default:
	}
}

// Response expiry applies even while replies or deliveries keep the writer
// busy. Check at packet boundaries; an in-progress write uses the same cap.
func (w *writer) checkPong() {
	if !w.closed && !w.closing && !w.pongAt.IsZero() && !w.conn.clock.Now().Before(w.pongAt) {
		w.deadline = time.Time{}
		w.close()
	}
}

func (w *writer) keepAliveDue() {
	at := w.clientDeadline()
	if at.IsZero() || w.conn.clock.Now().Before(at) {
		return
	}
	if !w.pongAt.IsZero() {
		// 0x8D belongs to a server. A client ends an unanswered ping by
		// closing, through the same reader-handover and close budget.
		w.deadline = time.Time{}
		w.close()
		return
	}
	w.deadline = time.Time{}
	w.pingBegan = w.conn.clock.Now()
	if w.send(Pingreq{}) {
		w.pongAt = w.pingAt // controlWritten fixed it at PINGREQ completion
	}
}
