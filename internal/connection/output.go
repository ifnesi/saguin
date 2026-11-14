package connection

import (
	"net"
	"time"

	"github.com/ifnesi/saguin/internal/packet"
)

const (
	writeVectors = 32
	writeBytes   = 1 << 20
	writePackets = writeVectors / 2
)

// deliveryBatch holds references to reserved deliveries, never copied bodies.
// Every vector is bounded in both count and bytes, including a packet spanning
// several vectors. Completed packet boundaries determine individual results.
type deliveryBatch struct {
	vector     [writeVectors][]byte
	buffers    net.Buffers
	ends       [writePackets]deliveryEnd
	packets    int
	bytes      int64
	continuing bool // this vector starts with a packet already begun on the socket
}

type deliveryEnd struct {
	packet *Delivery
	end    int64 // end of this packet in the current vector, not a buffer boundary
}

// deliver collects only entries already waiting. Replies taken while collecting
// go before all unstarted bytes, including deliveries already in the vector.
func (w *writer) deliver(d *Delivery) {
	w.output.buffers = w.output.vector[:0]
	for {
		ready := w.waitingReplies()
		if p, ok := w.heldReply.(*Release); ok && d.Order > p.After {
			// A reply can arrive between the queue checks that collected
			// this delivery. Flush only its predecessors before taking it.
			w.flushDeliveries(false)
			ready = w.waitingReplies()
		}
		d.Result = DeliveryDiscarded
		var head []byte
		if ready && !w.closed {
			head = w.deliveryHead(d)
		}
		if head == nil {
			if !w.flushDeliveries(false) {
				d.Result = DeliveryDiscarded
			}
			w.deliveryOrder = d.Order
			w.inbound <- Event{Kind: Delivered, Conn: w.conn, Packet: d}
		} else if !w.appendDelivery(d, head) {
			w.deliveryOrder = d.Order
			w.inbound <- Event{Kind: Delivered, Conn: w.conn, Packet: d}
		}
		if !ready || w.closed {
			return
		}
		// At a packet boundary, a waiting reply wins before another delivery
		// is taken. Neither queue is awaited to fill the vector.
		if !w.waitingReplies() {
			w.flushDeliveries(false) // return collected ownership before the final entry
			return
		}
		if w.heldReply != nil || w.output.packets == writePackets || len(w.output.buffers) == writeVectors || w.output.bytes == writeBytes {
			w.flushDeliveries(false)
			return
		}
		select {
		case d = <-w.conn.deliveries:
		default:
			w.flushDeliveries(false)
			return
		}
	}
}

// appendDelivery slices a large packet across bounded vectors. Its first
// socket attempt fixes one deadline, which all remaining pieces keep. A full
// packet gets one completion boundary, only after its final byte is included.
func (w *writer) appendDelivery(d *Delivery, head []byte) bool {
	b := &w.output
	buffered := false
	for _, part := range [2][]byte{head, d.Payload} {
		for len(part) != 0 {
			if len(b.buffers) == writeVectors || b.bytes == writeBytes {
				if !w.flushDeliveries(buffered) {
					return false
				}
			}
			n := min(len(part), writeBytes-int(b.bytes))
			b.buffers = append(b.buffers, part[:n])
			b.bytes += int64(n)
			part = part[n:]
			buffered = true
		}
	}
	b.ends[b.packets] = deliveryEnd{packet: d, end: b.bytes}
	b.packets++
	if b.continuing {
		// Nothing else may share the deadline of this packet's first write.
		// flushDeliveries reports it even on failure, so ownership has returned.
		w.flushDeliveries(false)
	}
	return true
}

// flushDeliveries returns every collected packet exactly once, in order, in
// one report. A write error can still leave a completely written prefix; a
// packet cut short remains discarded. unfinished carries the deadline only
// to that packet's continuation, whose final vector is flushed before another
// packet joins.
func (w *writer) flushDeliveries(unfinished bool) bool {
	b := &w.output
	if len(b.buffers) == 0 {
		return !w.closed
	}
	vectors, packets := len(b.buffers), b.packets
	ok := !w.closed
	if ok && !b.continuing {
		// No delivery in this vector has begun transmission. Every waiting
		// reply goes first, then this vector starts its own write budget.
		ok = w.waitingReplies()
		if ok {
			w.deadline = time.Time{}
		}
	}
	var written int64
	if ok {
		written, ok = w.writeBuffers(&b.buffers, b.bytes, nil)
	}
	if packets != 0 && b.ends[0].end <= written {
		w.controlWritten()
	}
	clear(b.vector[:vectors]) // WriteTo consumes its slice; also clear an unwritten suffix
	b.buffers, b.bytes, b.packets = b.vector[:0], 0, 0
	b.continuing = ok && unfinished
	var first, last *Delivery
	for i := range packets {
		end := b.ends[i]
		b.ends[i] = deliveryEnd{}
		if end.end <= written {
			end.packet.Result = DeliveryWritten
		}
		w.deliveryOrder = end.packet.Order
		if last == nil {
			first = end.packet
		} else {
			last.Next = end.packet
		}
		last = end.packet
	}
	if first != nil {
		// The descriptors already belong to the writer. Linking them avoids
		// allocating a report buffer, which could not be reused after send.
		w.inbound <- Event{Kind: Delivered, Conn: w.conn, Packet: first}
	}
	return ok
}

// waitingReplies runs only at a packet boundary. The separate reply buffer
// leaves the delivery vector intact. A final entry stays at the reply head
// until collection returns its deliveries; entries then discards the queue
// before taking that final entry and beginning the close.
func (w *writer) waitingReplies() bool {
	for !w.closed || w.controlReport.Kind != 0 {
		if w.conn.client {
			w.readPongs()
			w.checkPong()
		}
		if w.controlReport.Kind != 0 {
			w.waitEntry()
			continue
		}
		if w.closed {
			return false
		}
		if reply := w.nextReply(); reply != nil {
			switch reply.(type) {
			case *packet.Disconnect, Close:
				w.heldReply = reply
				return false
			}
			if w.reply(reply) {
				return false
			}
		} else {
			return true
		}
	}
	return false
}
