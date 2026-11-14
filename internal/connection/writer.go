package connection

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
)

type writer struct {
	conn          *Conn
	inbound       chan<- Event
	timeout       time.Duration
	metrics       *Metrics
	err           error // a reply build failure, carried by the close report
	peer          peerSettings
	connack       bool          // a successful CONNACK was written, or received on a bridge
	establishment time.Time     // one attempt cutoff until the bridge's CONNACK
	keepAlive     time.Duration // half of the bridge's effective interval
	pingAt        time.Time     // half an interval after the last completed control packet
	pongAt        time.Time     // fixed when PINGREQ finishes writing
	pingBegan     time.Time     // excludes responses received before this PINGREQ began
	lastControl   time.Time     // includes CONNECT, before the effective interval is known
	controlReport Event         // one completed SUBSCRIBE, sent before taking another writer entry
	timer         <-chan time.Time
	resetTimer    func(time.Time)
	timerAt       time.Time
	closed        bool
	closing       bool              // writer alone publishes the TLS close cutoff once
	final         bool              // the core's final entry was consumed, including during vector collection
	cause         *metrics.Counter  // a writer failure or server DISCONNECT
	deadline      time.Time         // fixed when a final entry is taken, or a write starts
	aliases       map[string]uint16 // aliases assigned to earlier headers in stream order
	output        deliveryBatch     // bounded vector and packet-completion metadata; no bytes at rest
	replyVector   [1][]byte
	replyBuffers  net.Buffers
	heldReply     Reply  // head of the existing reply queue, waiting for recovery's delivery barrier
	deliveryOrder uint64 // last PUBLISH whose writer ownership has returned
}

func (w *writer) run() {
	if w.conn.client {
		// The reader completes bridge transport setup before publishing
		// its stream. It queues CONNECT only after successful setup.
		w.setPeer(<-w.conn.peer)
	}
	// Even after close, consume replies until the core's final entry. Queued
	// packets and their slots must not leave the core waiting (R2).
	w.entries()
	end := <-w.conn.ended
	w.selectCause(nil) // a cut may have occurred after the writer closed
	ev := Event{Kind: Closed, Conn: w.conn}
	switch {
	case w.conn.client:
		// The core counts bridge attempts and link outcomes separately.
	case w.cause != nil:
		w.cause.Inc()
		ev.Closure = w.metrics.closureReasons[w.cause]
	case end.client:
		w.metrics.client.Inc()
	case end.publishStalled:
		w.metrics.publishStalled.Inc()
		w.conn.writeClosureAt = w.conn.readClosureAt
		ev.Closure = w.metrics.closureReasons[w.metrics.publishStalled]
	case end.lost:
		w.metrics.lost.Inc()
	}
	// Every close path has returned from raw.Close and lowered the gauge.
	// Wait for the Ended send, not the earlier socket handover (R2, R3).
	<-w.conn.endReported
	if w.err != nil {
		ev.Packet = CannotBuild{Err: w.err}
	}
	w.inbound <- ev
}

func (w *writer) reply(reply Reply) bool {
	var release *Release
	switch p := reply.(type) {
	case NoReply:
		w.conn.slots <- replySlot{}
		return false
	case *Release:
		release = p
		p.Result = DeliveryDiscarded
		defer func() { w.inbound <- Event{Kind: Delivered, Conn: w.conn, Packet: p} }()
		if !p.Resumed {
			defer func() { w.conn.slots <- replySlot{} }()
		}
		reply = &p.Ack // ordinary acknowledgement encoding, without a second slot return
	}
	_, disconnect := reply.(*packet.Disconnect)
	_, closeOnly := reply.(Close)
	final := disconnect || closeOnly
	w.final = w.final || final
	if final {
		close(w.conn.finalTaken)
	}
	// Only these entries consume a packet's reserved reply slot. A final
	// entry has no reservation to return.
	switch reply.(type) {
	case *packet.Connack, Pingresp, *Ack, *packet.Suback, *packet.Unsuback:
		if release == nil {
			defer func() { w.conn.slots <- replySlot{} }()
		}
	}
	if w.closed {
		return final
	}
	w.deadline = time.Time{}
	ack, connack := reply.(*packet.Connack)
	if final || (connack && ack != nil && ack.ReasonCode != 0) {
		// Encoding and every subsequent close step use this same budget.
		// In particular, a refusal begins here, not after its write (R2).
		w.beginClose()
	}
	w.updatePeer()
	if closeOnly {
		w.close()
		return true
	}
	if disconnect && !w.connack {
		w.close() // MQTT-3.14.0-1: nothing before a successful CONNACK
		return true
	}
	if w.send(reply) {
		if release != nil {
			release.Result = DeliveryWritten
		}
		if connack && ack != nil && ack.ReasonCode == 0 {
			// Derive the handover from the exact properties just advertised,
			// even if access changed while CONNECT waited for storage.
			var denied acl.Feature
			for property := range ack.Properties.Views() {
				switch property.ID {
				case packet.MaximumQoS:
					if property.Int < 2 {
						denied |= acl.QoS2
					}
				case packet.RetainAvailable:
					if property.Int == 0 {
						denied |= acl.Retained
					}
				case packet.SharedSubscriptionAvailable:
					if property.Int == 0 {
						denied |= acl.Share
					}
				}
			}
			w.conn.ackWrites <- ackWrite{state: ackConnackWritten, denied: denied}
		}
		if p, ok := reply.(*packet.Subscribe); ok {
			w.controlReport = Event{Kind: ControlWritten, Conn: w.conn, Packet: ControlWrite{Packet: p, At: w.lastControl}}
		}
	}
	return final
}

// send confines encoded bytes to this reply. No encoded write buffer lives
// between replies, and the core's packet is never changed (R3, R6).
func (w *writer) send(reply Reply) bool {
	if ack, ok := reply.(*packet.Connack); ok && ack != nil &&
		w.peer.version == packet.V311 && ack.ReasonCode == 0x95 {
		w.selectCause(w.metrics.refused[0x95]) // intentionally has no MQTT 3.1.1 code
		w.close()
		return false
	}
	if d, ok := reply.(*packet.Disconnect); ok && w.peer.version == packet.V311 {
		w.selectCause(w.metrics.disconnected[d.ReasonCode])
		w.close() // a server DISCONNECT is MQTT 5 only; count the reason owed
		return false
	}
	buf, err := w.encode(reply)
	if err != nil {
		w.cannotBuild(reply, err)
		return false
	}
	// Fix the logical cause before the last write. Transport errors and the
	// close deadline cannot turn an intentional close into a lost connection.
	switch p := reply.(type) {
	case *packet.Connack:
		if p.ReasonCode != 0 {
			w.selectCause(w.metrics.refused[p.ReasonCode])
		}
	case *packet.Disconnect:
		w.selectCause(w.metrics.disconnected[p.ReasonCode])
	}
	ack, isAck := reply.(*Ack)
	if !isAck || !ack.releasesWindow() {
		ack = nil
	}
	if !w.write(buf, ack) {
		return false
	}
	w.controlWritten()
	switch p := reply.(type) {
	case *packet.Connack:
		if p.ReasonCode != 0 {
			w.close()
		} else {
			w.connack = true
		}
	case *packet.Disconnect:
		w.close()
	}
	return true
}

func (w *writer) write(buf []byte, ack *Ack) bool {
	// A reply can precede an already collected delivery vector. Its one
	// encoded buffer must not overwrite that vector's payload references.
	w.replyBuffers = w.replyVector[:]
	w.replyVector[0] = buf
	_, ok := w.writeBuffers(&w.replyBuffers, int64(len(buf)), ack)
	w.replyVector[0] = nil
	w.replyBuffers = nil
	return ok
}

// writeBuffers uses the same deadline, acknowledgement handover and failure
// path for a control packet and a delivery vector. net.Buffers uses writev on
// native sockets and the transport's Write otherwise, without copying bodies.
func (w *writer) writeBuffers(buffers *net.Buffers, size int64, ack *Ack) (n int64, ok bool) {
	if w.deadline.IsZero() {
		w.deadline = w.conn.clock.Now().Add(w.timeout)
		if !w.establishment.IsZero() && w.establishment.Before(w.deadline) {
			w.deadline = w.establishment
		}
	}
	n, err := w.writeOutput(buffers, size, ack)
	if n != 0 {
		w.metrics.sentBytes.Add(uint64(n))
	}
	// A completed write is governed by the socket's write deadline. For a
	// last packet, close checks the clock before its next socket operation.
	if err == nil {
		return n, true
	}
	var timeout net.Error
	if w.cause == nil {
		if !w.conn.clock.Now().Before(w.deadline) || (errors.As(err, &timeout) && timeout.Timeout()) {
			w.selectCause(w.metrics.writerFull)
		} else {
			w.selectCause(w.metrics.lost)
		}
	}
	w.close()
	return n, false
}

// writeOutput resolves connection deadlines at each socket operation. A
// keep-alive timeout can wake a write after the reader has received a timely
// PINGRESP. That cancels only the ping deadline. net.Buffers has consumed the
// written prefix, and its remaining suffix continues under the packet's
// unchanged write budget. This writer cannot start another ping meanwhile,
// so at most one deadline cancellation can occur during a write.
func (w *writer) writeOutput(buffers *net.Buffers, size int64, ack *Ack) (n int64, err error) {
	var notification ackWrite
	begun := false
	if ack != nil {
		notification.typ, notification.packetID = ack.Type, ack.PacketID
		if ack.Type != packet.PUBACK {
			notification.quota = ack.Quota
		}
	}
	for {
		at := w.deadline
		if !w.closing && !w.pongAt.IsZero() && w.pongAt.Before(at) {
			at = w.pongAt
		}
		err = os.ErrDeadlineExceeded
		if w.conn.clock.Now().Before(at) {
			err = w.conn.transport().SetWriteDeadline(at)
		}
		if err == nil {
			if !w.conn.clock.Now().Before(at) {
				err = os.ErrDeadlineExceeded
			} else {
				if !begun && ack != nil {
					notification.state = ackWriteBegun
					w.conn.ackWrites <- notification
				}
				begun = true
				var part int64
				part, err = buffers.WriteTo(w.conn.transport())
				n += part
				if err == nil && n != size {
					err = io.ErrShortWrite
				}
			}
		}
		if err == nil || at.Equal(w.deadline) {
			break
		}
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			break
		}
		w.readPongs()
		if !w.pongAt.IsZero() {
			break
		}
	}
	if begun && ack != nil {
		notification.state = ackWriteSucceeded
		if err != nil {
			notification.state = ackWriteFailed
		}
		// Completion precedes close: the reader can be waiting for this
		// outcome, and close must await its socket handover.
		w.conn.ackWrites <- notification
	}
	return n, err
}

func (w *writer) cannotBuild(reply Reply, err error) {
	w.beginClose()
	w.err = fmt.Errorf("cannot build MQTT reply %T: %w", reply, err)
	w.selectCause(w.metrics.disconnected[0x80])
	if w.connack && w.peer.version == packet.V5 {
		// A minimal DISCONNECT may itself exceed an exceptionally small peer
		// limit. In that case close without sending anything more.
		buf, _ := packet.AppendDisconnect(nil, packet.V5, &packet.Disconnect{ReasonCode: 0x80})
		if w.peer.maxSize == 0 || uint32(len(buf)) <= w.peer.maxSize {
			w.write(buf, nil)
		}
	}
	w.close()
}

// selectCause keeps the first closure reason and its observation time.
// Reporting and counting still follow cleanup, without adding a handover
// before socket work or letting core backpressure delay the close deadline.
func (w *writer) selectCause(cause *metrics.Counter) {
	if w.cause != nil {
		return
	}
	if w.conn.cutByCore() {
		cause = nil
		switch cut := w.conn.cutCause; cut.typ {
		case packet.CONNACK:
			cause = w.metrics.refused[cut.code]
		case packet.DISCONNECT:
			cause = w.metrics.disconnected[cut.code]
		}
	}
	w.cause = cause
	if reason := w.metrics.closureReasons[cause]; !w.conn.client && reason != (Closure{}) {
		w.conn.writeClosureAt = w.conn.clock.Now()
	}
}

// beginClose preserves an existing write budget on failure, or fixes the
// budget when a final entry is taken. The transport layer caps all later
// writes, including WebSocket control replies and TLS close_notify, at that
// cutoff. Applying it now also interrupts a control write already in progress,
// before the MQTT writer waits for Gorilla's write lock or TLS's output lock.
func (w *writer) beginClose() {
	if w.deadline.IsZero() {
		w.deadline = w.conn.clock.Now().Add(w.timeout)
	}
	if w.closing {
		return
	}
	w.closing = true
	var s *tlsSocket
	switch transport := w.conn.transport().(type) {
	case *tls.Conn:
		s = transport.NetConn().(*tlsSocket)
	case *websocketStream:
		s = transport.writeSocket
	}
	if s != nil {
		s.closeDeadline = w.deadline
		close(s.closing)
		if err := s.SetWriteDeadline(w.deadline); err != nil {
			w.conn.Cut()
		}
	}
}

func (w *writer) close() {
	if w.closed {
		return
	}
	w.closed = true
	w.beginClose()
	defer func() {
		w.conn.Cut()
		if !w.conn.client {
			w.metrics.open.Add(-1)
		}
	}()
	// Expiry bypasses the remaining graceful steps, including half-close.
	if !w.conn.clock.Now().Before(w.deadline) {
		close(w.conn.stopping)
		return
	}
	err := w.conn.raw.CloseWrite() // half-close TCP or Unix, close_notify on TLS
	close(w.conn.stopping)         // sending ended; request reader handover
	if err != nil || !w.conn.clock.Now().Before(w.deadline) {
		return
	}
	if w.conn.raw.SetReadDeadline(w.conn.clock.Now()) != nil {
		return
	}
	select {
	case <-w.conn.readStopped:
	case <-w.conn.clock.At(w.deadline):
	}
	// The wait only wakes us. Reading the clock decides even when both
	// channels were ready: the deadline wins over reader handover (R2, R3).
	if !w.conn.clock.Now().Before(w.deadline) {
		return
	}
	if w.conn.raw.SetReadDeadline(w.deadline) != nil {
		return
	}
	// Only this goroutine can read or change deadlines after handover. Keep
	// scratch local to the close, and check the deadline between every read:
	// bytes returned at the cutoff must not lead to another socket call.
	var scratch [4096]byte
	for w.conn.clock.Now().Before(w.deadline) {
		if _, err := w.conn.raw.Read(scratch[:]); err != nil {
			return
		}
	}
}

func (w *writer) encode(reply Reply) ([]byte, error) {
	var buf []byte
	var err error
	switch p := reply.(type) {
	case *packet.Connect:
		if !w.conn.client || w.connack {
			return nil, packet.ErrCannotEncode
		}
		buf, err = packet.AppendConnect(nil, p)
	case *packet.Subscribe:
		if !w.conn.client || !w.connack {
			return nil, packet.ErrCannotEncode
		}
		buf, err = packet.AppendSubscribe(nil, packet.V5, p)
	case Pingreq:
		if !w.conn.client || !w.connack {
			return nil, packet.ErrCannotEncode
		}
		buf, err = packet.AppendPingreq(nil, packet.V5)
	case *packet.Connack:
		buf, err = packet.AppendConnack(nil, w.peer.version, p)
	case Pingresp:
		buf, err = packet.AppendPingresp(nil, w.peer.version)
	case *packet.Disconnect:
		if (w.conn.client && !validClientDisconnect(p, 0xffffffff)) || (!w.conn.client && !validServerDisconnect(p)) {
			return nil, packet.ErrCannotEncode
		}
		buf, err = packet.AppendDisconnect(nil, w.peer.version, p)
	case *packet.Suback:
		buf, err = packet.AppendSuback(nil, w.peer.version, p)
	case *packet.Unsuback:
		buf, err = packet.AppendUnsuback(nil, w.peer.version, p)
	case *Ack:
		if p == nil {
			return nil, packet.ErrCannotEncode
		}
		if w.conn.client && !validClientAck(p.Type, p.ReasonCode) {
			return nil, packet.ErrCannotEncode
		}
		ack := p.Ack
		if w.peer.version == packet.V311 {
			ack.ReasonCode, ack.Properties = 0, nil
		}
		buf, err = packet.AppendAck(nil, p.Type, w.peer.version, &ack)
	}
	if err != nil || w.peer.maxSize == 0 || uint32(len(buf)) <= w.peer.maxSize {
		return buf, err
	}
	// Only diagnostics may be omitted to meet the peer's bound. Use a fresh
	// property section: the packet handed over by the core is never modified.
	switch p := reply.(type) {
	case *packet.Connack:
		trimmed := *p
		trimmed.Properties = p.Properties.Without(packet.ReasonString, packet.UserProperty)
		buf, err = packet.AppendConnack(buf[:0], w.peer.version, &trimmed)
	case *packet.Disconnect:
		trimmed := *p
		trimmed.Properties = p.Properties.Without(packet.ReasonString, packet.UserProperty)
		buf, err = packet.AppendDisconnect(buf[:0], w.peer.version, &trimmed)
	case *packet.Suback:
		trimmed := *p
		trimmed.Properties = nil // SUBACK carries diagnostics only
		buf, err = packet.AppendSuback(buf[:0], w.peer.version, &trimmed)
	case *packet.Unsuback:
		trimmed := *p
		trimmed.Properties = nil // UNSUBACK carries diagnostics only
		buf, err = packet.AppendUnsuback(buf[:0], w.peer.version, &trimmed)
	case *Ack:
		trimmed := p.Ack
		trimmed.Properties = p.Properties.Without(packet.ReasonString, packet.UserProperty)
		buf, err = packet.AppendAck(buf[:0], p.Type, w.peer.version, &trimmed)
	}
	if err == nil && uint32(len(buf)) > w.peer.maxSize {
		err = packet.ErrCannotEncode
	}
	return buf, err
}
