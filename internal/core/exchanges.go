package core

import (
	"container/heap"
	"errors"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// deliveryEnd distinguishes an acknowledgement from a known never-sent
// preparation. Cancellation deletes its row with safe progress or with the
// subscriptions that owed it.
type deliveryEnd struct {
	offset          int64
	recoveredOffset int64
	capacity        deliveryCapacity // returned descriptor's admission credit, held through settlement commit
	cancelled       bool
	shared          bool // a terminal shared exchange leaves evidence if its cursor has not passed
	retiredShare    bool // group membership ended; only the MQTT exchange remains
}

type deliveryCapacity uint8

const (
	noDeliveryCapacity deliveryCapacity = iota
	publishCapacity
	releaseCapacity
)

func (c *Core) returnDeliveryCapacity(s *connectionState, capacity deliveryCapacity) {
	switch capacity {
	case publishCapacity:
		s.freeSlots++
	case releaseCapacity:
		s.freeReleases++
	}
}

// endDelivery holds an identifier until its settlement commits. Both ordinary
// settlements and cancelled preparations use the same bounded owner map.
func (s *connectionState) endDelivery(d *outgoing, end deliveryEnd) {
	u := d.consumer
	if u.settled == nil {
		u.settled = make(map[uint16]deliveryEnd)
	}
	if s.settlements == nil {
		s.settlements = make(map[uint16]*consumerState)
	}
	id := d.exchange.PacketID
	if !d.exchange.NoMove {
		end.recoveredOffset = d.offset
	}
	u.settled[id], s.settlements[id] = end, u
	s.identifiers[id] = nil // held until deletion or marker conversion commits
}

// cancelPreparation uses core-owned exchange metadata even while a payload
// read still owns its buffer. Unsaved preparations leave pendingSaves at once;
// saved ones join their owner's transaction. Departure rewinds live progress;
// an UNSUBSCRIBE cancellation skips a record no remaining subscription owes.
func (c *Core) cancelPreparation(d *outgoing, now time.Time) {
	if d.saving || d.attempted || d.settled || c.failure != nil {
		return
	}
	d.settled = true
	c.removePendingSave(d)
	u, s := d.consumer, d.consumer.conn
	if d.initial && d.permissionHeld && !s.end.seen && s.phase == connected && u.ownsPosition() {
		c.startRetainedPass(s, d.pass, d.offset)
	}
	if u.sharedCursor != nil && u.share.queue == nil && u.share.active && (d.saved || d.sharedStored) {
		// The return and assignment deletion share the group's transaction.
		d.sharedRefused = true
		c.queueSharedEvidence(d)
		return
	}
	if !d.initial && (s.end.seen || s.phase != connected || d.permissionHeld) {
		u.scan = min(u.scan, d.offset)
	}
	if !d.saved {
		return // no stored row or progress to undo
	}
	offset := d.offset
	if d.initial {
		offset = 0 // a cut initial pass is not resumed and never moves live progress
	} else if s.end.seen || s.phase != connected || d.permissionHeld {
		u.position = min(u.position, offset)
		if u.ownsPosition() {
			u.rewind = earlier(u.rewind, offset)
		}
	}
	s.endDelivery(d, deliveryEnd{offset: offset, cancelled: true})
	u.dirty, u.seen = true, now.UnixMilli()
	c.queueConsumer(u)
}

// retiredDelivery releases a committed exchange's identifier and bounded
// settlement state after deletion or conversion to a scan marker.
func (c *Core) retiredDelivery(s *connectionState, u *consumerState, id uint16) {
	defer c.queueConsumers(s)
	defer c.queueConsumer(u)
	s.recoveryCount--
	end := u.settled[id]
	delete(u.recovered, end.recoveredOffset)
	delete(u.settled, id)
	delete(s.settlements, id)
	delete(s.identifiers, id)
	s.freeIdentifier = min(s.freeIdentifier, uint32(id))
	c.returnDeliveryCapacity(s, end.capacity)
}

// assignDelivery reserves the lowest free broker identifier, independently of
// the reader's incoming exchanges. The candidate only visits occupied ids;
// returning an identifier brings it back down immediately.
func (c *Core) assignDelivery(s *connectionState, d *outgoing) {
	if s.identifiers == nil {
		s.identifiers = make(map[uint16]*outgoing, len(s.window))
	}
	s.deliverySequence++
	d.exchange = store.SessionDelivery{PacketID: d.packet.PacketID, Seq: s.deliverySequence,
		Channel: d.consumer.channel.row.ID, Offset: d.offset, QoS: d.packet.QoS, Retain: d.packet.Retain, Initial: d.initial, NoMove: d.initial}
	if s.stored {
		d.saveAfter = d.consumer.preparationAfter(d)
		d.consumer.addPendingSave(d)
	}
	s.identifiers[d.exchange.PacketID] = d
	s.findFreeIdentifier()
}

func (s *connectionState) findFreeIdentifier() {
	for s.freeIdentifier <= 65535 {
		_, held := s.identifiers[uint16(s.freeIdentifier)]
		_, unread := s.bridgeIdentifiers[uint16(s.freeIdentifier)]
		if !held && !unread {
			break
		}
		s.freeIdentifier++
	}
}

func (c *Core) countInflight(delta int64) {
	c.inflight += delta
	c.inflightGauge.Set(c.inflight)
}

// puback settles only its outgoing QoS 1 exchange, for every valid reason.
// The reader may report it before the writer returns packet ownership. In
// that case the quota ends here, and the fixed slot waits for that report.
func (c *Core) puback(conn *connection.Conn, ack *packet.Ack, now time.Time) {
	s := c.connections[conn]
	if s.phase != connected || s.end.seen {
		return
	}
	d := s.identifiers[ack.PacketID]
	if d == nil || d.exchange.QoS != 1 || d.acknowledged ||
		(d.stage != deliveryWriting && d.stage != deliveryAcknowledgement) {
		return
	}
	if a := d.attempt; a != nil && !a.complete && a.ending == nil && ack.ReasonCode >= 0x80 {
		r := c.queueAttemptRequest(a, nil)
		c.finishQueueAttempt(r, a, false, queueReturned, now)
		c.enqueue(r)
	} else {
		c.spendAttempt(d, now)
	}
	c.acknowledgeDelivery(d, now)
}

// pubrec accepts an outgoing QoS 2 receipt once; repeats only answer its flow.
// A refusal settles the delivery. Success ends the PUBREC peer obligation,
// then waits for its phase commit and the PUBLISH write report.
func (c *Core) pubrec(acts []Action, conn *connection.Conn, ack *packet.Ack, now time.Time) []Action {
	s := c.connections[conn]
	if s.phase >= refused || s.end.seen {
		return acts
	}
	d := s.identifiers[ack.PacketID]
	if d != nil && d.exchange.QoS == 2 && d.receipt != receiptNone && !d.acknowledged {
		if d.receipt == receiptRefused {
			return c.reply(acts, conn, connection.NoReply{})
		}
		slot := s.storedReplies.PushBack(nil)
		if d.stage == deliveryReleasing || d.stage == deliveryCompletion {
			return c.completeStoredReply(acts, s, slot,
				&connection.Ack{Type: packet.PUBREL, PacketID: ack.PacketID}, true)
		}
		d.receiptRepeats = append(d.receiptRepeats, slot)
		s.pendingAcks++
		return acts
	}
	if d == nil || d.exchange.QoS != 2 || d.acknowledged ||
		(d.stage != deliveryWriting && d.stage != deliveryAcknowledgement) {
		return c.completeStoredReply(acts, s, s.storedReplies.PushBack(nil),
			&connection.Ack{Type: packet.PUBREL, PacketID: ack.PacketID, ReasonCode: 0x92}, false)
	}
	if ack.ReasonCode >= 0x80 {
		c.acknowledgeDelivery(d, now)
		return c.reply(acts, conn, connection.NoReply{})
	}
	c.endPeer(d)
	d.receipt = receiptAccepted
	d.receiptReply = s.storedReplies.PushBack(nil)
	s.pendingAcks++
	if s.stored || d.saved {
		d.saveAfter = 0 // the existing exchange already represents this obligation
		d.consumer.addPendingSave(d)
	}
	c.queueConsumer(d.consumer)
	if s.stored || d.saved {
		return acts
	}
	d.exchange.Released = true
	return c.readyRelease(acts, d)
}

// pubcomp settles only a QoS 2 exchange whose PUBREL was handed to this
// connection's writer. The report may still be in transit, as for PUBACK.
func (c *Core) pubcomp(conn *connection.Conn, id uint16, now time.Time) {
	s := c.connections[conn]
	if s.phase != connected || s.end.seen {
		return
	}
	d := s.identifiers[id]
	if d == nil || d.exchange.QoS != 2 || !d.exchange.Released || d.acknowledged ||
		(d.stage != deliveryReleasing && d.stage != deliveryCompletion) {
		return
	}
	c.acknowledgeDelivery(d, now)
}

// acknowledgeDelivery is the terminal transition shared by PUBACK, PUBCOMP
// and a refusing PUBREC. Quota belongs only to a PUBLISH sent on this
// connection; a recovered PUBREL cannot release another delivery's slot.
func (c *Core) acknowledgeDelivery(d *outgoing, now time.Time) {
	s := d.consumer.conn
	if c.admission.delay != 0 {
		c.endPeer(d)
	}
	d.releaseTopic()
	d.acknowledged = true
	if d.quota {
		d.quota = false
		s.inflight--
	}
	if d.counted {
		d.counted = false
		c.countInflight(-1)
	}
	if d.stage == deliveryAcknowledgement || d.stage == deliveryCompletion {
		c.settleDelivery(s, d, now)
	} else {
		c.recordSettlement(d, now)
		c.queueConsumers(s)
	}
}

// completeReceipt releases the accepted PUBREC and its repeats once their
// result is known. The writer owns their reader slots until PUBREL is sent.
func (c *Core) completeReceipt(acts []Action, d *outgoing, reply connection.Reply) []Action {
	s := d.consumer.conn
	if slot := d.receiptReply; slot != nil {
		d.receiptReply = nil
		c.completeAcknowledgement(s)
		acts = c.completeStoredReply(acts, s, slot, reply, true)
	}
	repeated := reply
	if _, release := reply.(*connection.Release); release && len(d.receiptRepeats) != 0 {
		// Only the original PUBREL returns delivery ownership and starts
		// the PUBCOMP obligation. Repeats share an immutable reply and
		// only return their reader slots.
		repeated = &connection.Ack{Type: packet.PUBREL, PacketID: d.exchange.PacketID}
	}
	for _, slot := range d.receiptRepeats {
		c.completeAcknowledgement(s)
		acts = c.completeStoredReply(acts, s, slot, repeated, true)
	}
	d.receiptRepeats = nil
	return acts
}

// readyRelease joins the phase commit and PUBLISH ownership return. Reply
// emission, including packet ordering, is the sole hand-off to the writer.
func (c *Core) readyRelease(acts []Action, d *outgoing) []Action {
	if !d.exchange.Released || d.pendingSave != nil || d.saving ||
		(d.stage != deliveryAcknowledgement && d.stage != deliveryLoaded) {
		return acts
	}
	s := d.consumer.conn
	if s.phase != connected || s.end.seen || c.failure != nil {
		return c.completeReceipt(acts, d, nil)
	}
	d.stage = deliveryReleaseReady
	d.release.Ack = connection.Ack{Type: packet.PUBREL, PacketID: d.exchange.PacketID}
	d.release.Slot = d.packet.Slot
	if d.release.Resumed {
		d.release.After = s.writerOrder
	}
	if !d.counted {
		d.counted = true
		c.countInflight(1)
	}
	if d.receiptReply != nil {
		return c.completeReceipt(acts, d, &d.release)
	}
	return c.reply(acts, s.conn, &d.release)
}

// released returns PUBREL ownership, using the same writer outcome channel
// as PUBLISH. Resumed releases hold bounded writer reply capacity until this
// report, independently of the connection's PUBLISH quota.
func (c *Core) released(acts []Action, conn *connection.Conn, p *connection.Release, now time.Time) []Action {
	s := c.connections[conn]
	d := &s.window[p.Slot]
	if &d.release != p || d.stage != deliveryReleasing {
		panic("core: unknown completed PUBREL")
	}
	c.returnWriterDelivery(s)
	if p.Result == connection.DeliveryDiscarded {
		// Finish still sees the writer's ownership. The report returns it
		// below, after departure has cancelled the other preparations.
		acts = c.finish(acts, s, 0, now)
	}
	// The writer no longer owns this PUBREL, even if settlement must wait
	// for shared evidence. Its later commit must be able to reclaim the slot.
	d.stage = deliveryCompletion
	u := d.consumer
	switch {
	case p.Result == connection.DeliveryDiscarded:
		c.releaseDelivery(s, d, now)
	case d.acknowledged:
		c.settleDelivery(s, d, now)
	case s.phase != connected || s.end.seen:
		c.releaseDelivery(s, d, now)
	default:
		c.requirePeer(d, now)
	}
	c.completeResume(s)
	if !u.indexed {
		c.queueConsumer(u)
	}
	c.queueConsumers(s)
	return acts
}

// settleDelivery holds only bounded bookkeeping until the settlement commits.
// Durable progress accompanies deletion; an inherited expiry-zero exchange
// ahead of the scan becomes a marker, returning credit to scan earlier records.
func (c *Core) settleDelivery(s *connectionState, d *outgoing, now time.Time) {
	if d.counted {
		d.counted = false
		c.countInflight(-1)
	}
	c.recordSettlement(d, now)
	c.releaseDelivery(s, d, now)
}

// recordSettlement ends the MQTT exchange at its terminal acknowledgement,
// even when the writer's ownership report is still in transit. Only the packet
// buffer waits for that report; settlement can join the next transaction.
func (c *Core) recordSettlement(d *outgoing, now time.Time) {
	if d.settled {
		return
	}
	d.settled = true
	// A discarded preparation needs no new exchange. Remove its unsubmitted
	// save before shared terminal evidence takes ownership of the descriptor.
	// An in-flight save keeps its nodes until its own answer returns them.
	if !d.saving {
		c.removePendingSave(d)
	}
	c.sharedSettled(d)
	u, s := d.consumer, d.consumer.conn
	saved, offset := d.saved, d.offset
	if d.initial || d.exchange.NoMove || !u.ownsPosition() {
		offset = 0 // detached exchanges and initial passes have no position to move
	}
	if c.failure == nil {
		if !d.initial && !d.exchange.NoMove {
			u.settledThrough = max(u.settledThrough, d.offset+1)
		}
		if d.saved {
			shared := d.exchange.Share != 0
			retired := shared && u.share != nil && !u.share.active
			s.endDelivery(d, deliveryEnd{offset: offset, shared: shared && !retired, retiredShare: retired})
		}
	}
	u.untrackDelivery(d)
	c.advanceConsumer(u, now)
	if saved && (!s.stored || offset < u.position) {
		u.dirty = true
	}
	c.queueConsumer(u)
}

func (u *consumerState) needsWrite() bool {
	// Departed members finish their existing exchanges independently of
	// recipient selection. Their atomic terminal evidence needs no scan lane.
	if u.share != nil && u.share.request != nil && (u.sharedMember == nil || !u.sharedMember.ending) {
		return false
	}
	if u.request != nil && ((!u.conn.stored && len(u.settled) != 0) || (!u.conn.end.seen && u.request.classification != store.RecordRequest)) {
		return false // session, subscription, cut and recovery work still owns this consumer
	}
	return (u.saving == nil && (u.pendingSaves.Len() != 0 || u.blockedSaveReady())) || (u.writing == nil && u.dirty)
}

// removePendingSave ends pending exchange membership on commit or cancellation.
// Existing list and heap indexes remove membership without searching the window.
func (c *Core) removePendingSave(d *outgoing) {
	removed := d.pendingSave != nil || d.boundary[saveDependencyBoundary] != 0
	if d.pendingSave != nil {
		d.consumer.pendingSaves.Remove(d.pendingSave)
		d.pendingSave = nil
	}
	if index := d.boundary[saveDependencyBoundary]; index != 0 {
		heap.Remove(&d.consumer.blockedSaves, index-1)
	}
	if removed {
		c.queueConsumer(d.consumer)
	}
}

// collectDeliveryChanges joins a session transition's transaction only after
// the connection's earlier writes and writer reports have returned. A takeover
// supersedes its reads; an ordinary departure also awaits their ownership.
// Departure and takeover save the same final progress and settled rows.
func (c *Core) collectDeliveryChanges(s *connectionState, now time.Time) {
	var initial [2]*consumerState
	for _, u := range s.deliveryOwners(initial[:0]) {
		if u.needsWrite() {
			c.collect(c.snapshotDeliveryChanges(u), now)
		}
	}
}

// saveDeliveryChanges snapshots scalars, never message bodies. The consumer
// owns at most one save and one progress write; a combined snapshot reserves
// both. Subscription changes await both owners, and
// departure awaits all delivery requests and writer reports; takeover awaits
// the writes and writer reports, superseding reads.
func (c *Core) saveDeliveryChanges(u *consumerState) {
	c.enqueue(c.snapshotDeliveryChanges(u))
}

// snapshotDeliveryChanges transfers one consumer's current scalars to a
// request. Ordinary saves and the final departure commit share this snapshot
// and its result handling; neither keeps payloads or intermediate progress.
// Only pending saves and the settled set supply changes. One save request owns
// the consumer's save nodes, independently of its payload read and a disjoint
// position/settlement request. Capturing the existing standard-library list's
// endpoints adds no copied membership.
func (c *Core) snapshotDeliveryChanges(u *consumerState) *storeRequest {
	s := u.conn
	op := &store.DeliveryChanges{Session: s.session.ID, Channel: u.channel.row.ID}
	if u.rule != nil {
		op.Markers = &store.DeliveryMarkers{Bridge: &store.BridgeDeliveryOwner{ID: s.bridge.id, Rule: u.rule.row.ID}}
	}
	if u.saving == nil {
		u.releaseBlockedSaves()
	}
	if n := u.pendingSaves.Len(); u.saving == nil && n != 0 {
		op.Save = make([]store.SessionDelivery, 0, n)
	}
	if u.writing == nil && len(u.settled) != 0 {
		op.Delete = make([]uint16, 0, len(u.settled))
		for id, end := range u.settled {
			if end.retiredShare {
				if op.Markers == nil {
					op.Markers = new(store.DeliveryMarkers)
				}
				op.Markers.Retired = append(op.Markers.Retired, id)
			}
			if end.shared {
				if op.Markers == nil {
					op.Markers = new(store.DeliveryMarkers)
				}
				op.Markers.Shared = append(op.Markers.Shared, id)
			}
			if end.cancelled || end.offset < u.position {
				op.Delete = append(op.Delete, id)
			} else if !s.stored {
				if op.Markers == nil {
					op.Markers = new(store.DeliveryMarkers)
				}
				op.Markers.IDs = append(op.Markers.IDs, id)
			}
		}
		slices.Sort(op.Delete)
		if op.Markers != nil {
			slices.Sort(op.Markers.IDs)
			slices.Sort(op.Markers.Shared)
			slices.Sort(op.Markers.Retired)
		}
	}
	if u.writing == nil && u.dirty && s.stored && u.ownsPosition() {
		op.Position, op.Seen = u.position, u.seen
		op.Rewind = u.rewind != 0
		if op.Rewind && len(u.live) != 0 {
			op.Restore = make([]uint16, 0, len(u.live))
			for id := range u.live {
				op.Restore = append(op.Restore, id)
			}
			slices.Sort(op.Restore)
		}
	}
	if u.rule == nil && u.writing == nil && u.ownsPosition() && ((u.dirty && u.markersBefore != 0) || op.Markers != nil) {
		if op.Markers == nil {
			op.Markers = new(store.DeliveryMarkers)
		}
		op.Markers.Position = u.position
	}

	r := &storeRequest{kind: saveDeliveryChanges, op: op, conn: s, consumer: u}
	if op.Markers != nil && len(op.Markers.IDs) != 0 {
		// A metadata page taken before this conversion must finish first.
		// Later pages consult the committed markers after this owner returns.
		c.joinConsumerRead(r, u)
	}
	if g := u.share; g != nil && g.queue == nil && c.shares[g.filter] == g && g.active && (u.sharedMember == nil || !u.sharedMember.ending) {
		r.share = g
		c.claimShared(g, r)
		if g.durable != 0 {
			last := g.last
			r.before = &store.SharedChange{ID: g.id, Last: &last,
				Cursors: []store.SubscriptionStart{{Channel: u.channel.row.ID, Start: u.sharedCursor.position}}}
		}
	}
	if op.Save != nil {
		r.firstSave, r.lastSave = u.pendingSaves.Front(), u.pendingSaves.Back()
		for e := r.firstSave; e != nil; e = e.Next() {
			d := e.Value.(*outgoing)
			op.Save = append(op.Save, d.exchange)
			d.saving = true
			if d.receipt == receiptAccepted {
				op.Save[len(op.Save)-1].Released = true
				if c.admission.delay != 0 {
					r.admissionAt = firstTime(r.admissionAt, c.eventAt)
				}
			}
		}
		u.saving = r
	}
	if op.Position != 0 || (op.Markers != nil && op.Markers.Bridge == nil) || len(op.Delete) != 0 {
		if c.admission.delay != 0 {
			r.admissionAt = firstTime(u.admission.pendingAt, c.eventAt)
			u.admission.pendingAt = time.Time{}
		}
		u.writing = r
	}
	s.deliveryRequests++
	s.deliveryWrites++
	return r
}

// savedDeliveryChanges returns immutable request ownership on every answer.
// A full refusal keeps only current window state and the latest dirty mark;
// the next full-store retry reconstructs a new write from those owners.
func (c *Core) savedDeliveryChanges(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	defer c.releaseSharedRequest(r)
	u, s := r.consumer, r.conn
	c.returnDeliveryWrite(s)
	if u.writing == r {
		u.writing = nil
	}
	if u.saving == r {
		u.saving = nil
	}
	op := r.op.(*store.DeliveryChanges)
	if err == nil && op.Position != 0 {
		u.persisted = op.Position
		if op.Rewind && u.rewind >= op.Position {
			u.rewind = 0
			u.live = nil
		}
	}
	if err == nil {
		for _, id := range op.Delete {
			c.retiredDelivery(s, u, id)
		}
		if markers := op.Markers; markers != nil {
			for _, id := range markers.IDs {
				u.markersBefore = max(u.markersBefore, u.settled[id].offset+1)
				c.retiredDelivery(s, u, id)
			}
			if markers.Position != 0 {
				u.persisted = markers.Position
				if markers.Position >= u.markersBefore {
					u.markersBefore = 0
				}
			}
		}
	}
	if op.Markers != nil && len(op.Markers.IDs) != 0 {
		c.finishConsumerRead(r)
	}
	for e := r.firstSave; e != nil; {
		next, last := e.Next(), e == r.lastSave
		d := e.Value.(*outgoing)
		d.saving = false
		if d.receipt == receiptAccepted && err != nil {
			// A refused phase change is lost. Reconnect recovers the
			// stored PUBLISH phase; no PUBREL or phase retry is invented.
			c.removePendingSave(d)
			d.receipt = receiptRefused
			acts = c.completeReceipt(acts, d, connection.NoReply{})
		}
		if err == nil {
			if !d.saved && !d.exchange.Reset {
				s.recoveryCount++
			}
			d.saved = true
			d.exchange.Reset = false
			c.sharedSaved(d)
			d.exchange.Released = d.receipt == receiptAccepted
			c.removePendingSave(d)
			if index := d.boundary[1]; index != 0 {
				heap.Remove(&u.boundaries[1], index-1)
			}
			d.exchange.Properties = nil
		} else if !d.saved && d.saveAfter > u.persisted {
			// A refused combined write did not establish the prerequisite.
			// Restore its dependency before any later progress-only write.
			c.removePendingSave(d)
			u.addPendingSave(d)
		}
		// Clearing saving returns a refused node to core ownership. A live
		// owner can retry; an ended owner cancels it even during a payload read.
		switch {
		case s.phase != connected || s.end.seen || c.failure != nil:
			acts = c.completeReceipt(acts, d, nil)
			if d.stage != deliveryWriting && d.stage != deliveryReleasing {
				c.releaseDelivery(s, d, now)
			}
		case d.accessRevoked:
			c.releaseDelivery(s, d, now)
			c.advanceConsumer(u, now)
		case err == nil && d.exchange.Released:
			acts = c.readyRelease(acts, d)
		case err == nil && d.removed:
			c.removePreparation(d)
		case err == nil:
			acts = c.offerLoadedDeliveries(acts, u)
		}
		if last {
			break
		}
		e = next
	}
	r.firstSave, r.lastSave = nil, nil
	if err == nil {
		// Newly committed exchanges can let an earlier PUBACK pass those
		// protected records without waiting for another client event. An ended
		// owner has released its boundaries; its final position is already fixed.
		if s.phase == connected && !s.end.seen {
			c.advanceConsumer(u, now)
		}
		u.dirty = u.ownsPosition() && (s.stored || u.markersBefore != 0) && (u.position != u.persisted || u.rewind != 0)
		// Another PUBACK may have arrived while this snapshot was at the
		// store, even without moving the already saved scan boundary.
		for _, end := range u.settled {
			if !s.stored || end.cancelled || end.offset < u.position {
				u.dirty = true
				break
			}
		}
		if op.Markers != nil {
			for _, g := range op.Markers.Groups {
				c.scheduleSharedCleanup(g)
			}
		}
	}
	if errors.Is(err, store.ErrFull) {
		if c.admission.delay != 0 && u.dirty {
			u.admission.pendingAt = firstTime(u.admission.pendingAt, r.admissionAt)
		}
		c.refuseDeliverySave(u)
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
	}
	if !u.indexed {
		c.queueConsumer(u)
	}
	c.queueConsumers(s)
	if s.bridge != nil && s.bridge.link == s && s.phase == connected && !s.end.seen && s.bridge.pending == nil && len(s.bridge.todo) != 0 {
		acts = c.subscribeBridge(acts, s.bridge, now)
	}
	return acts
}

// retryDeliveries visits only owners registered by a full refusal. The
// existing one-second retry deadline remains the store's explicit policy.
func (c *Core) retryDeliveries() {
	for e := c.deliveryRetries.Front(); e != nil; e = c.deliveryRetries.Front() {
		u := e.Value.(*consumerState)
		c.removeDeliveryRetry(u)
		u.refusedSave = false
		c.queueConsumer(u)
		if u.conn == nil {
			c.queueShared(u.share)
		}
	}
}
