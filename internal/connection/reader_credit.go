package connection

import (
	"net"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
)

// takeSlot collects completed replies before reserving the next packet's
// slot. Free slots stay reader-local; write notifications are drained after
// slot returns so no reused reservation leaves old notifications behind.
func (r *reader) takeSlot() bool {
	if r.available == 0 {
		select {
		case <-r.conn.stopping:
			return false
		case <-r.conn.slots:
			r.available++
		}
	}
	if !r.drainSlots() || r.stopRequested() {
		return false
	}
	r.available--
	return true
}

func (r *reader) drainSlots() bool {
	for {
		select {
		case <-r.conn.slots:
			r.available++
		default:
			return r.drainAckWrites()
		}
	}
}

// drainAckWrites observes every begun or completed acknowledgement before
// window admission. The writer sends completion before returning its slot,
// so draining after slot returns also bounds the notification backlog.
func (r *reader) drainAckWrites() bool {
	for {
		select {
		case state := <-r.conn.ackWrites:
			if !r.ackWrite(state) {
				return false
			}
		default:
			return true
		}
	}
}

func (r *reader) ackWrite(write ackWrite) bool {
	if write.held != nil {
		if r.identifiers == nil {
			r.identifiers = make(map[uint16]uint32, len(write.held))
		}
		for _, id := range write.held {
			r.identifiers[id] = uint32(id)
		}
		r.inflight += len(write.held)
		if r.conn.client {
			r.connackWritten = true // CONNACK was received before this ordered restoration
		}
		return true
	}
	if write.state == ackConnackWritten {
		r.connackWritten = true
		r.negotiatedDenied = write.denied
		return true
	}
	r.ackWriting = write.state == ackWriteBegun
	r.ackPacket = write.packetID
	if write.state == ackWriteSucceeded {
		if write.typ == packet.PUBACK {
			r.inflight--
		} else if generation := r.identifiers[write.packetID]; generation != 0 && generation == write.quota {
			delete(r.identifiers, write.packetID)
			r.inflight--
		}
	}
	return write.state != ackWriteFailed
}

// waitConnack takes the writer's successful CONNECT settings before a packet
// can use negotiated capabilities. Socket arrival alone cannot publish them.
func (r *reader) waitConnack() bool {
	for !r.connackWritten {
		select {
		case <-r.conn.stopping:
			return false
		case state := <-r.conn.ackWrites:
			if !r.ackWrite(state) {
				return false
			}
		}
	}
	return !r.stopRequested()
}

// waitIdentifier joins a terminal write that can already have reached the
// peer. Stale replies cannot release a later generation of this identifier.
func (r *reader) waitIdentifier(id uint16) error {
	for r.ackWriting && r.ackPacket == id {
		select {
		case <-r.conn.stopping:
			return net.ErrClosed
		case state := <-r.conn.ackWrites:
			if !r.ackWrite(state) {
				return net.ErrClosed
			}
		}
	}
	return nil
}

// admitPublish follows the successful CONNACK's ordered restoration-complete
// report, including when CONNECT restored no identifiers. No identifier-bearing
// PUBLISH can take quota before that hand-off. A repeat takes none at either QoS.
func (r *reader) admitPublish(id uint16, qos byte) (generation uint32, admitted bool, err error) {
	if qos == 0 {
		if !r.drainAckWrites() {
			return 0, false, net.ErrClosed
		}
		return 0, true, nil
	}
	admitted, err = r.waitWindow(id)
	if err != nil || !admitted {
		return 0, admitted, err
	}
	if generation = r.identifiers[id]; generation != 0 {
		return generation, true, nil
	}
	if qos == 2 {
		if r.identifiers == nil {
			r.identifiers = make(map[uint16]uint32)
		}
		r.quotaSequence++
		if r.quotaSequence <= uint32(^uint16(0)) {
			r.quotaSequence = uint32(^uint16(0)) + 1
		}
		generation = r.quotaSequence
		r.identifiers[id] = generation
	}
	r.inflight++
	return generation, true, nil
}

// waitWindow also supports the header-only optimization: id zero means the
// identifier is unread, so a held generation makes a repeat possible. The
// complete body must still pass admitPublish before it takes receive quota.
func (r *reader) waitWindow(id uint16) (bool, error) {
	for {
		if !r.drainAckWrites() {
			return false, net.ErrClosed
		}
		if r.stopRequested() {
			return false, net.ErrClosed
		}
		if !r.connackWritten {
			// Restoration is sent before CONNACK enters the writer's queue.
			// Its successful write seals that set, an empty set included.
			if !r.waitConnack() {
				return false, net.ErrClosed
			}
			continue
		}
		if id != 0 && r.ackWriting && r.ackPacket == id {
			if err := r.waitIdentifier(id); err != nil {
				return false, err
			}
			continue // drain restoration and other writes before deciding
		}
		if r.receiveMaximum == 0 || uint32(r.inflight) < r.receiveMaximum ||
			r.identifiers[id] != 0 || (id == 0 && len(r.identifiers) != 0) {
			return true, nil
		}
		if !r.ackWriting {
			return false, nil
		}
		select {
		case <-r.conn.stopping:
			return false, net.ErrClosed
		case state := <-r.conn.ackWrites:
			if !r.ackWrite(state) {
				return false, net.ErrClosed
			}
		}
	}
}

// askBytes requests exactly one body, before allocating or reading any of
// it. There are no unused grants between packets. The core owns the
// reservation through handover, refusal or the reader's end.
func (r *reader) askBytes(n int) error {
	if n == 0 {
		return nil
	}
	if !r.hand(Event{Kind: BytesRequested, Conn: r.conn, Bytes: payload.BodyMemory(n) + r.topicRoom}) {
		return net.ErrClosed
	}
	select {
	case <-r.conn.stopping:
		return net.ErrClosed
	case <-r.conn.messageBytes:
	}
	// A simultaneous stop wins, before another deadline change or read.
	if r.stopRequested() {
		return net.ErrClosed
	}
	// No read is active while waiting. The body sets its silence deadline
	// when reading starts, and the next packet starts fresh keep-alive time.
	return nil
}
