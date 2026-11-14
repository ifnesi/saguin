package core

import (
	"container/list"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// incomingPublish is the client's independent identifier space. A pending
// hold keeps only repeat reply slots; the original body belongs to its batch.
type incomingPublish struct {
	holdLane           *sessionLane  // pending hold must settle before a later CONNECT loads
	channel            *channelState // enough routing metadata to enter at PUBREL intake
	quota              uint32        // reader generation, independently restored on each connection
	pending, releasing bool
	request            bool // service requests have no persisted incoming exchange
	reason             byte // the original PUBREC result, also owed to classified repeats
	repeats            []*list.Element
}

// publishIdentity is a packet's fixed classification. Only a new QoS 2
// exchange owns incoming; a repeat names its original exchange independently
// of later identifier reuse. Execution and policy carry that decision through
// completion.
type publishIdentity struct {
	incoming, repeat *incomingPublish
}

// classifyPublish runs in packet order against CONNECT's resolved session.
// CONNECT-held packets pass here when granted, before any later intake.
func (c *Core) classifyPublish(s *connectionState, id uint16, qos byte, quota uint32) (identity publishIdentity, reason byte) {
	if s.phase == connecting || qos == 0 {
		return publishIdentity{}, 0
	}
	if h := s.incoming[id]; h != nil {
		return publishIdentity{repeat: h}, 0
	}
	if qos != 2 {
		return publishIdentity{}, 0
	}
	maximum := c.receiveMaximum
	if s.bridge != nil {
		maximum = uint32(s.bridge.options.Bridge.ReceiveMaximum)
	}
	if uint32(len(s.incoming)) >= maximum {
		return publishIdentity{}, 0x93
	}
	if s.incoming == nil {
		s.incoming = make(map[uint16]*incomingPublish)
	}
	h := &incomingPublish{pending: true, quota: quota, request: true}
	s.incoming[id] = h
	c.countIncoming(s, int64(len(s.incoming)))
	return publishIdentity{incoming: h}, 0
}

// restoreIncoming is shared by session CONNECT and bridge-link recovery.
// The store supplies only identifiers and routing; held bodies remain there.
func (c *Core) restoreIncoming(s *connectionState, held []store.HeldIdentifier) {
	s.incoming = make(map[uint16]*incomingPublish, len(held))
	for _, id := range held {
		s.incoming[id.PacketID] = &incomingPublish{channel: c.channel(id.Channel), quota: uint32(id.PacketID)}
	}
}

// hasHeldPublishes excludes transient request identifiers from session storage.
func (s *connectionState) hasHeldPublishes() bool {
	for _, h := range s.incoming {
		if !h.request {
			return true
		}
	}
	return false
}

func (c *Core) countIncoming(s *connectionState, n int64) {
	if s.bridge != nil {
		return // bridge receive quota is independent of the client-session gauge
	}
	c.incomingHeld += n - s.incomingHeld
	s.incomingHeld = n
	c.incomingGauge.Set(c.incomingHeld)
}

func (c *Core) repeatIncoming(acts []Action, s *connectionState, id uint16, h *incomingPublish, slot *list.Element) []Action {
	if slot == nil {
		slot = s.storedReplies.PushBack(nil)
	} else {
		c.completeAcknowledgement(s)
	}
	if h.pending {
		h.repeats = append(h.repeats, slot)
		s.pendingAcks++
		return acts
	}
	return c.completeStoredReply(acts, s, slot, &connection.Ack{Type: packet.PUBREC, PacketID: id, ReasonCode: h.reason}, h.reason == 0)
}

// finishIncoming applies only a committed hold or release. A full release
// drops the reply and keeps its stored source for the client's reconnect.
func (c *Core) finishIncoming(acts []Action, p *publication, reason byte, now time.Time) []Action {
	s, h := p.conn, p.incoming
	typ := packet.PUBREC
	if p.record.Offset != 0 {
		typ = packet.PUBCOMP
		h.releasing = false
		if reason == 0 {
			c.forgetIncoming(s, p.packetID)
		}
	} else {
		if lane := h.holdLane; lane != nil {
			lane.holds--
			h.holdLane = nil
			c.wakeRequests(&lane.waitRequests)
			c.advance(lane)
		}
		h.pending = false
		h.reason = reason
		if reason != 0 {
			c.forgetIncoming(s, p.packetID)
		}
	}
	if p.bridgeRecovery {
		if reason != 0 {
			// A refused move leaves its held source in the store. End this
			// link so it cannot deliver new records ahead of that source.
			return c.finish(acts, s, 0, now)
		}
		return c.replayBridgeHeld(acts, s)
	}
	var reply connection.Reply
	if reason == 0 || typ == packet.PUBREC {
		reply = &connection.Ack{Type: typ, PacketID: p.packetID, ReasonCode: reason, Quota: h.quota}
	}
	if reason != 0 && typ == packet.PUBREC && s.version == packet.V311 {
		reply = nil
		acts = c.finish(acts, s, reason, now)
	}
	return c.answerIncoming(acts, p, reply, reason == 0)
}

// answerIncoming settles the first PUBLISH and the repeats waiting for it
// at the same packet-ordered result, including a restored pipelined repeat.
func (c *Core) answerIncoming(acts []Action, p *publication, reply connection.Reply, ordered bool) []Action {
	s, h := p.conn, p.incoming
	c.completeAcknowledgement(s)
	acts = c.completeStoredReply(acts, s, p.pendingReply, reply, ordered)
	for _, slot := range h.repeats {
		c.completeAcknowledgement(s)
		acts = c.completeStoredReply(acts, s, slot, reply, ordered)
	}
	h.repeats = nil
	return acts
}

// forgetIncoming updates the connected gauge at the identifier's removal.
// A detached connection retains its exchanges without contributing to it.
func (c *Core) forgetIncoming(s *connectionState, id uint16) {
	delete(s.incoming, id)
	if s.incomingHeld != 0 {
		c.countIncoming(s, int64(len(s.incoming)))
	}
}

// pubrel reserves its reply in packet order, but judges the identifier only
// when the session lane has applied CONNECT and its preceding packets.
func (c *Core) pubrel(acts []Action, conn *connection.Conn, id uint16) []Action {
	s := c.connections[conn]
	if s.phase >= refused || s.end.seen {
		return acts
	}
	slot := s.storedReplies.PushBack(nil)
	s.pendingAcks++
	c.enqueueIncoming(s, id, slot, nil)
	return acts
}

// enqueueIncoming shares PUBREL's offset assignment and stored move with
// bridge session-loss recovery, which owns no wire reply slot.
func (c *Core) enqueueIncoming(s *connectionState, id uint16, slot *list.Element, recovery *incomingPublish) {
	r := &storeRequest{kind: releaseIncoming, conn: s, lane: c.connectionLane(s), replySlot: slot,
		op:          &store.ReadHeldPublish{PacketID: id},
		publication: &publication{conn: s, packetID: id, pendingReply: slot, bridgeRecovery: recovery != nil}}
	if recovery != nil {
		c.enterIncoming(r, recovery)
	} else if s.phase == connected {
		c.enterIncoming(r, s.incoming[id])
	}
	c.enqueue(r)
}

// startIncoming binds the release to CONNECT's resolved session, never its
// provisional identifier map or session id. A pending save keeps this lane
// until the source is stored; unrelated owners continue.
func (c *Core) startIncoming(acts []Action, r *storeRequest) ([]Action, bool) {
	s, op := r.conn, r.op.(*store.ReadHeldPublish)
	if s.end.seen || s.phase >= refused {
		c.abandonPublication(r)
		if r.publication.bridgeRecovery {
			return acts, true
		}
		c.completeAcknowledgement(s)
		return c.completeStoredReply(acts, s, r.replySlot, nil, false), true
	}
	h := r.publication.incoming
	if !r.publication.bridgeRecovery {
		h = s.incoming[op.PacketID]
	}
	if h == nil {
		c.abandonPublication(r)
		c.completeAcknowledgement(s)
		return c.completeStoredReply(acts, s, r.replySlot,
			&connection.Ack{Type: packet.PUBCOMP, PacketID: op.PacketID, ReasonCode: 0x92}, false), true
	}
	if h.request {
		if h.pending {
			return acts, false
		}
		c.forgetIncoming(s, op.PacketID)
		c.completeAcknowledgement(s)
		return c.completeStoredReply(acts, s, r.replySlot,
			&connection.Ack{Type: packet.PUBCOMP, PacketID: op.PacketID, Quota: h.quota}, true), true
	}
	if !c.enterIncoming(r, h) || h.pending {
		return acts, false
	}
	if blocked := c.blockedChannel(r); blocked != nil {
		c.parkRequest(r, &blocked.waitRequests)
		return acts, false
	}
	if !c.admitBatch(&r.publishes) {
		c.parkRequest(r, &c.batchWait)
		return acts, false
	}
	c.reserveChannel(r)
	r.incoming, op.Session, r.channel = h, s.session.ID, h.channel
	if s.bridge != nil {
		op.Bridge = s.bridge.id
	}
	r.kind = readIncoming
	return acts, false
}

// enterIncoming takes exactly one offset for a known PUBREL, including one
// whose preceding hold write is still pending. Its source remains in SQLite.
func (c *Core) enterIncoming(r *storeRequest, h *incomingPublish) bool {
	p := r.publication
	if p.incoming != nil {
		return true
	}
	if h == nil || h.releasing || h.request {
		return false
	}
	newChannel := r.channel == nil
	p.channel, p.incoming, r.channel = h.channel, h, h.channel
	// A CONNECT-held PUBREL did not have a channel when it was enqueued.
	// Cut the last collecting batch as soon as its owner is known, before
	// another publication can join that earlier member across this entry.
	if newChannel {
		c.registerChannel(r)
		r.channel.pending = r
	}
	p.record.Channel = h.channel.row.ID
	c.queueBatch(&r.publishes, h.channel)
	if p.record.Offset == 0 {
		p.enter()
	}
	h.releasing = true
	return true
}

// abandonPublication resolves an unused entry after its channel predecessors,
// for both queued bodies and incoming releases.
func (c *Core) abandonPublication(r *storeRequest) {
	c.returnReadPermit(r)
	c.unparkRequest(r)
	c.releaseRequestOrder(r)
	c.releaseBatch(&r.publishes)
	p := r.publication
	if p == nil {
		return
	}
	if p.incoming != nil {
		p.incoming.releasing = false
	}
	p.incoming = nil
	ch := r.channel
	if ch == nil || p.record.Offset == 0 {
		c.releaseChannel(r)
		if ch != nil && ch.pending == r {
			ch.pending = nil
		}
		r.kind, r.op, r.channel, r.lane = resolvePublication, nil, nil, nil
		return
	}
	if c.failure != nil {
		c.releaseChannel(r)
		if ch.pending == r {
			ch.pending = nil
		}
		r.channel = nil
		return
	}
	if c.requests[r.op] == nil {
		// Unsent work keeps its channel place through cancellation. No later
		// offset may resolve before this entry.
		r.kind, r.op, r.tail, r.lane = resolvePublication, nil, p.record.Offset+1, nil
		c.readyRequest(r)
		return
	}
	// A dispatched read has already resolved its channel predecessors. Its
	// cancellation precedes every pending channel entry, without touching
	// the store-owned operation or waiting for its stale result.
	marker := &storeRequest{kind: resolvePublication, channel: ch, tail: p.record.Offset + 1, order: r.order}
	c.replaceChannel(r, marker)
	r.channel = nil
	c.continueRequest(marker)
}

// loadedIncoming continues the original channel request as its move. Later
// publications already have their offsets, but cannot pass this request.
func (c *Core) loadedIncoming(acts []Action, r *storeRequest) []Action {
	// A superseded read returns only its store credit.
	if r.incoming == nil {
		return acts
	}
	if c.failure != nil {
		c.cancelIncoming(r.conn)
		return acts
	}
	p, op := r.publication, r.op.(*store.ReadHeldPublish)
	offset := p.record.Offset
	p.record = op.Record
	p.record.Offset = offset
	r.publishes.prepared, r.publishes.trackCharges = true, c.retained.maximum != 0
	r.publishes.append(r.channel, p)
	r.kind, r.op = savePublishes, &r.publishes
	c.continueRequest(r)
	return acts
}

// cancelIncoming supersedes a connection's decision read without waiting for
// its answer. Its already assigned offset is resolved in channel order; the
// stale answer owns only store credit and cannot move the held publication.
func (c *Core) cancelIncoming(s *connectionState) {
	lane := c.findConnectionLane(s)
	if lane == nil || lane.request == nil {
		return
	}
	r := lane.request
	if r.conn != s || (r.kind != readIncoming && r.kind != releaseIncoming) {
		return
	}
	c.abandonPublication(r)
	r.incoming = nil
	if !r.publication.bridgeRecovery {
		c.completeAcknowledgement(s)
		s.storedReplies.Remove(r.replySlot)
	}
	c.endLane(lane, r)
	r.lane = nil
}

// endedHeld applies aggregate reservations only after their deletion commits.
func (c *Core) endedHeld(op store.Write) {
	var held *store.HeldChanges
	switch w := op.(type) {
	case *store.EndSession:
		held = w.Held
		if held != nil {
			c.abandoned.Add(held.Count)
		}
	case *store.ReconcileBridges:
		held = w.Held
	case *Group:
		for _, member := range w.Writes {
			c.endedHeld(member)
		}
	}
	if held != nil {
		for channel, size := range held.Channels {
			c.channel(channel).row.HeldBytes -= size
		}
		c.retained.charged -= held.Retained
	}
}
