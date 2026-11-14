package core

import (
	"container/list"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

type connectionPhase uint8

const (
	awaitingConnect connectionPhase = iota
	connecting
	connected
	refused
	closing // final entry sent; the reader's real Ended still applies
)

// connectionState keeps session scalars through the close report. An Ended
// during a CONNECT store request is applied after that request's answer.
type connectionState struct {
	bridge               *bridgeState
	bridgeTopics         map[string]*outgoing
	bridgeConsumers      map[int64]map[int64]*consumerState
	bridgeIdentifiers    map[uint16]struct{} // unread recovered exchanges also reserve packet identifiers
	workWait             list.List
	pendingShared        int                 // shared subscription decisions that can create group rows
	sharedEnd            *store.SharedEnding // bounded keys captured before cancellation
	accessUser           string
	access               *acl.Client
	incoming             map[uint16]*incomingPublish // client identifiers; bodies rest in SQLite
	incomingHeld         int64                       // identifiers counted while this session is connected
	held                 int64                       // connected subscriptions held by permission, included in Core.held
	conn                 *connection.Conn
	prev, next           *connectionState // live connections in R2's connection-key order
	order                uint64           // accept sequence; compared only within its listener
	accept               connection.Accept
	clientID             string
	version              packet.Version
	phase                connectionPhase
	attempt              *connectAttempt
	session              store.Session
	subscriptions        map[string]store.Subscription // connected session only; no payloads
	queueAttempts        map[string]*queueAttempt
	workers              []*sharedMember // queue memberships, with the same recipient machinery
	shared               []*sharedMember // complete shared-filter order, separate from ordinary positions
	stored               bool            // owns persistent session state
	spooled              bool            // inherited rows retained while this connection-lifetime session resumes
	willBytes            int64           // stored Will's bytes, reserved only when it fires
	willDelay            uint32
	replies              []connection.Reply // replies held behind a pending CONNACK, bounded by slots
	pendingAcks          int                // accepted changes whose acknowledgements await storage
	pendingSubscriptions int                // queued changes pause new scans until they finish
	end                  endMark
	closure              closureMoment // first core-selected closure, confirmed by the connection report
	cut                  bool          // the core has closed the raw socket
	takeoverCut          coreDeadline  // final-entry cutoff, cleared when it fires or the connection closes
	readerBytes          int64         // one granted body not yet handed over; Ended returns only this
	readerBatch          bool          // this grant owns one potential publication-batch slot
	readerRequest        memoryRequest
	peerDeadline         coreDeadline
	peerObligations      deadlineQueue  // bounded active exchanges, in peer deadline order
	peerWindow           []coreDeadline // allocated only with delivery admission enabled
	peerPausedAt         time.Time
	peerPaused           time.Duration
	service              *consumerState           // direct answers use an automatic exact-topic subscription
	answers              []*serviceAnswer         // answer metadata waiting for a delivery descriptor
	answerCount          int                      // requests and prepared answers not yet handed to the writer; at most ReplySlots
	serviceSlots         int                      // future writer capacity reserved by service answers
	writingPublishes     int                      // PUBLISH packets still owned by the writer, independent of QoS quota
	consumers            []*consumerState         // active positions, in channel id order
	scanning             *consumerState           // one channel visit at a time for this connection
	scanChannel          int64                    // last channel visited
	preparing            int                      // connection-wide selected deliveries awaiting hand-off
	resume               []*outgoing              // recovered exchanges in sending order
	resumeReleases       list.List                // recovered PUBRELs, independent of PUBLISH preparation and quota
	releasedThrough      int64                    // last control examined by recovery's bounded look-ahead
	recoveryCount        int                      // stored exchanges, including rows not yet read during recovery
	recoveryOwners       map[int64]*consumerState // detached recovery owners, bounded by inherited exchanges
	prepared             consumerQueue            // selected owners, including detached consumers, in selection order
	selectionOrder       uint64
	writerOrder          uint64               // PUBLISH hand-offs; recovered PUBRELs carry this writer barrier
	window               []outgoing           // fixed preparation/send descriptors; never moved while owned
	qos0Topics           []*outgoing          // newest pending QoS 0 slot per owner/topic, in owner/topic order
	free                 *outgoing            // unused slots, linked through next
	freeSlots            int                  // admission credits; cancellations and expiry-zero settlements await commit
	releaseFree          *outgoing            // recovered PUBREL slots, independent of the PUBLISH window
	freeReleases         int                  // control admission credits, also held through settlement deletion
	inflight             int                  // offered entries using this connection's window quota
	payloadSlots         int                  // future send slots held by memory requests and payload reads
	writingDeliveries    int                  // packets still owned by the connection writer, even after PUBACK
	deliveryRequests     int                  // outstanding consumer reads and writes, including undelivered results
	deliveryWrites       int                  // delivery bookkeeping writes awaiting their results
	identifiers          map[uint16]*outgoing // broker's independent packet identifier space
	freeIdentifier       uint32
	deliverySequence     int64
	resumeAfter          int64 // last recovered exchange admitted to preparation
	resumeThrough        int64 // CONNECT's fixed last recovered sequence
	resuming             *consumerState
	settlements          map[uint16]*consumerState // bounded, still-reserved identifiers awaiting deletion
	topicAliases         uint16                    // peer's outgoing alias bound; exact choices belong to its writer
	maximumPacket        uint32                    // peer's maximum, for proven direct-answer discards before preparation
	filters              []store.Subscription      // complete filter-byte order, rebuilt only on subscription changes
	subscriptionRevision uint64
	publishes            []*publication // pipelined bodies waiting for CONNECT and batch room
	storedReplies        list.List      // packet-ordered completion slots, bounded by reply slots
	replyQueued          bool
}

type endMark struct {
	left           int64
	expiry         uint32
	changed        bool
	reason         byte
	seen           bool
	discardWill    bool
	willDisconnect bool // DISCONNECT 0x04 selects its own publication counter
}

func (c *Core) accepted(conn *connection.Conn, accept connection.Accept) {
	if c.phase >= stopping {
		c.life.closed++
	}
	c.nextConnection++
	s := &connectionState{conn: conn, accept: accept, order: c.nextConnection,
		prepared: consumerQueue{prepared: true}}
	c.connections[conn] = s
	if c.admission.delay != 0 {
		s.peerDeadline.conn = s
		c.admission.peers.reserve(len(c.connections))
	}
	// A listener's Accepted events preserve its accept order. Insert after
	// its last live connection; the usual single-listener case appends.
	prev := c.lastConn
	for prev != nil && connectionBefore(s, prev) {
		prev = prev.prev
	}
	s.prev = prev
	if prev == nil {
		s.next, c.firstConn = c.firstConn, s
	} else {
		s.next, prev.next = prev.next, s
	}
	if s.next == nil {
		c.lastConn = s
	} else {
		s.next.prev = s
	}
}

func (c *Core) closed(conn *connection.Conn) {
	s := c.connections[conn]
	c.takeoverCuts.set(&s.takeoverCut, time.Time{})
	c.connectionWorkChanged(s)
	c.dropSharedConnection(s)
	c.unqueueConnect(s)
	if s.prev == nil {
		c.firstConn = s.next
	} else {
		s.prev.next = s.next
	}
	if s.next == nil {
		c.lastConn = s.prev
	} else {
		s.next.prev = s.prev
	}
	s.prev, s.next = nil, nil
	delete(c.connections, conn)
	conn.Release()
}

func (c *Core) ping(acts []Action, conn *connection.Conn) []Action {
	s := c.connections[conn]
	return c.completeStoredReply(acts, s, s.storedReplies.PushBack(nil), connection.Pingresp{}, false)
}

// reply preserves CONNACK as the first packet, including replies to packets
// handed over while CONNECT is still waiting for its store answer.
func (c *Core) reply(acts []Action, conn *connection.Conn, reply connection.Reply) []Action {
	s := c.connections[conn]
	if s.phase == connecting && !s.end.seen {
		s.replies = append(s.replies, reply)
	}
	if s.phase != connected {
		return acts
	}
	if release, ok := reply.(*connection.Release); ok {
		d := &s.window[release.Slot]
		d.stage = deliveryReleasing
		s.writingDeliveries++
		// Repeats waiting on recovery, or on an ordered release already
		// made ready, become eligible only at this tracked hand-off.
		acts = c.completeReceipt(acts, d, release)
	}
	return append(acts, Action{Conn: conn, Reply: reply})
}

// independentReply may pass an earlier result that is still unresolved at
// the end of this event. It cannot pass an earlier reply ready in this event.
type independentReply struct{ reply connection.Reply }

// completeStoredReply records readiness without emitting a partial event's
// replies. Slots contain only replies, never a request or its message body.
func (c *Core) completeStoredReply(acts []Action, s *connectionState, slot *list.Element, reply connection.Reply, ordered bool) []Action {
	live := c.failure == nil && c.connections[s.conn] == s && !s.end.seen && s.phase < refused
	if slot == nil {
		if reply == nil || !live {
			return acts
		}
		return c.reply(acts, s.conn, reply)
	}
	switch {
	case reply == nil || !live:
		s.storedReplies.Remove(slot)
	case ordered:
		slot.Value = reply
	default:
		slot.Value = independentReply{reply}
	}
	c.queueReplies(s)
	return acts
}

func (c *Core) queueReplies(s *connectionState) {
	if !s.replyQueued {
		s.replyQueued = true
		c.readyReplies = append(c.readyReplies, s)
	}
}

// flushStoredReplies sees all completions of this event, in packet order.
// Successful stored changes wait behind unresolved predecessors. Independent
// replies pass those predecessors, but never another ready earlier packet.
func (c *Core) flushStoredReplies(acts []Action, now time.Time) []Action {
	slices.SortFunc(c.readyReplies, compareConnections)
	at := 0
	for at < len(c.readyReplies) {
		s := c.readyReplies[at]
		at++
		s.replyQueued = false
		live := c.failure == nil && c.connections[s.conn] == s && !s.end.seen && s.phase < refused
		blocked := false
		readyBlocked := false
		waiting := false
		for slot := s.storedReplies.Front(); slot != nil; {
			next := slot.Next()
			var reply connection.Reply
			switch ready := slot.Value.(type) {
			case nil:
				blocked = true
			case independentReply:
				if !readyBlocked || !live {
					reply = ready.reply
				}
			case connection.Reply:
				released := !live || c.releaseStoredReply(s, now)
				if (!blocked && released) || !live {
					reply = ready
				} else {
					// Refusals may skip unresolved slots, but cannot pass
					// an earlier completed reply waiting for its own order.
					readyBlocked = true
					waiting = true
				}
			}
			if reply != nil {
				s.storedReplies.Remove(slot)
				if live {
					acts = c.reply(acts, s.conn, reply)
				}
			}
			slot = next
		}
		if s.bridge != nil && !waiting {
			s.bridge.releasingAcks = false
			c.bridgeAcks.set(&s.bridge.ackDeadline, time.Time{})
		}
	}
	clear(c.readyReplies)
	c.readyReplies = c.readyReplies[:0]
	return acts
}

// ended records the reader's outcome and reserves the departure's session
// lane. Delivery reports can still follow; collectDeparture waits for those
// outcomes before storing the latest progress with the departure. Only an
// expiry change holds the final entry. A superseded connection changes nothing.
func (c *Core) ended(acts []Action, ev connection.Event, now time.Time) []Action {
	s := c.connections[ev.Conn]
	// Ended is ordered after every packet. Cancel its outstanding request
	// and reclaim grants whose bodies never reached a store request (R6).
	c.unqueueBytes(s, now)
	c.returnBytes(s.readerBytes, now)
	s.readerBytes = 0
	c.releaseBodyBatch(s)
	c.discardPublishes(&s.publishes, now)
	c.cancelPackets(s, now)
	c.cancelIncoming(s)
	c.cancelChannelRequests(s, now)
	if s.end.seen {
		return acts
	}
	if s.phase == refused {
		return c.finish(acts, s, 0, now)
	}
	if ev.Reason >= 0x80 {
		c.selectClosure(s, packet.DISCONNECT, ev.Reason, now)
	}
	s.end = endMark{left: timestamp.CeilMilli(now), expiry: s.session.Expiry, reason: ev.Reason, seen: true}
	if s.bridge != nil {
		c.countIncoming(s, 0)
		acts = c.finish(acts, s, ev.Reason, now)
		c.enqueue(&storeRequest{kind: leaveBridge, conn: s, lane: &s.bridge.lane, op: &store.DeliveryChanges{Markers: &store.DeliveryMarkers{Bridge: &store.BridgeDeliveryOwner{ID: s.bridge.id}}}})
		return acts
	}
	c.queueConsumers(s)
	if s.attempt != nil {
		s.end.expiry = s.attempt.expiry
	}
	if d, ok := ev.Packet.(*packet.Disconnect); ok {
		s.end.discardWill = s.version == packet.V311 || d.ReasonCode == 0
		s.end.willDisconnect = d.ReasonCode == 0x04
		for prop := range d.Properties.Views() {
			if prop.ID == packet.SessionExpiryInterval {
				s.end.expiry = min(prop.Int, c.maximumExpiry())
				s.end.changed = s.end.expiry != s.session.Expiry
				if s.attempt != nil {
					s.end.changed = s.end.expiry != s.attempt.expiry
				}
			}
		}
	}
	c.unqueueConnect(s)
	if c.failure != nil {
		c.detach(s)
		return c.finish(acts, s, s.end.reason, now)
	}
	if s.attempt != nil {
		// A load has changed nothing; it can finish without the dead peer.
		// A save, however, must be followed by this mark if it commits.
		lane := c.lanes[s.clientID]
		if lane != nil && lane.request != nil && lane.request.kind == saveConnect && lane.request.conn == s {
			c.noteAway(s.end.left, s.end.expiry)
			return acts
		}
		return c.finish(acts, s, s.end.reason, now)
	}
	if c.sessions[s.clientID] != s && !s.stored && !s.spooled {
		return c.finish(acts, s, s.end.reason, now)
	}
	c.detach(s)
	if !s.stored && !s.spooled && s.willBytes == 0 && !s.hasHeldPublishes() && len(s.shared) == 0 && s.pendingShared == 0 {
		return c.leave(acts, nil, s, now)
	}
	return c.leave(acts, c.lane(s.clientID), s, now)
}

// cancelPackets drops this connection's undispatched packet decisions before
// departure can take its session lane. Otherwise a CONNECT save followed by
// an end could wait for replies queued behind that very departure.
func (c *Core) cancelPackets(s *connectionState, now time.Time) {
	lane := c.findConnectionLane(s)
	if lane == nil {
		return
	}
	lane.pending = slices.DeleteFunc(lane.pending, func(r *storeRequest) bool {
		if r.conn != s {
			return false
		}
		switch r.kind {
		case executeRequest:
			c.cancelChannelRequest(r, now)
		case releasePublishes:
			// discardPublishes already returned the bodies and their slots.
			c.abandonPublication(r)
		case changeSubscriptions, releaseIncoming:
			if r.kind == changeSubscriptions {
				c.completeSubscription(s)
			} else {
				c.abandonPublication(r)
			}
			if r.replySlot != nil {
				c.completeAcknowledgement(s)
				s.storedReplies.Remove(r.replySlot)
			}
		default:
			return false
		}
		if r.kind != resolvePublication {
			c.cancelPendingRequest(r)
		}
		return true
	})
}

// leave stores an Ended mark, after any CONNECT save it depended on. A
// session with no rows needs no deletion; every session still has an id.
func (c *Core) leave(acts []Action, lane *sessionLane, s *connectionState, now time.Time) []Action {
	if !s.stored && !s.spooled && s.willBytes == 0 && !s.hasHeldPublishes() && len(s.shared) == 0 && s.sharedEnd == nil && s.pendingShared == 0 {
		c.endedCounts[connectionEnd].Inc()
		return c.finish(acts, s, s.end.reason, now)
	}
	r := &storeRequest{kind: leaveSession, lane: lane, conn: s}
	fire := lane != nil && s.willBytes != 0 && !s.end.discardWill && (s.willDelay == 0 || s.end.expiry == 0) && c.phase < quiescing
	if s.end.expiry == 0 {
		c.captureSharedEnd(s)
		r.op = &store.EndSession{ID: s.session.ID, KeepWill: s.willBytes != 0 && !s.end.discardWill && !fire, Shared: s.sharedEnd}
	} else {
		r.op = &store.LeaveSession{ID: s.session.ID, Expiry: s.end.expiry, Left: s.end.left,
			DiscardWill: s.willBytes != 0 && (s.end.discardWill || fire)}
		c.noteAway(s.end.left, s.end.expiry)
	}
	if fire {
		r.willInfo = store.WillInfo{Session: s.session.ID, Bytes: s.willBytes, Delay: s.willDelay}
		r.willCause = willLost
		if s.end.willDisconnect {
			r.willCause = willDisconnect
		}
		if s.willDelay != 0 && s.end.expiry == 0 {
			r.willCause = willSessionEnd
		}
		if s.end.reason == 0x8E {
			r.willCause = willTakeover
		}
	} else if s.willBytes != 0 && !s.end.discardWill && c.phase < quiescing {
		due := expiryAt(s.end.left, s.willDelay)
		r.op.(*store.LeaveSession).WillDue = due
		c.willExpiry.note(due)
	}
	c.enqueue(r)
	if s.end.changed {
		// A changed expiry holds the final entry for this commit, but
		// unsent work must still stop and writer-owned packets must return.
		c.cancelDelivery(s, now)
	} else {
		acts = c.finish(acts, s, s.end.reason, now)
	}
	// The first waiting CONNECT may already be more than 500 ms old.
	// Enqueue armed its cut; enforce an elapsed deadline in this event.
	if !s.takeoverCut.at.IsZero() && !now.Before(s.takeoverCut.at) {
		acts = c.cutTakeovers(acts, now)
	}
	return acts
}

// collectDeparture decides only after every delivery's ownership has returned.
// Writer delivery reports precede Closed on inbound; reader Ended alone does
// not establish their outcomes. The reserved session lane holds a reconnect
// behind this decision without holding the core or an unrelated owner.
func (c *Core) collectDeparture(r *storeRequest, now time.Time) bool {
	s := r.conn
	if s.inflight != 0 || s.writingDeliveries != 0 || s.deliveryRequests != 0 || s.pendingAcks != 0 {
		c.parkRequest(r, &s.workWait)
		return false
	}
	if s.end.expiry != 0 || s.bridge != nil {
		c.collectDeliveryChanges(s, now)
	}
	if r.willInfo.Session != 0 {
		c.prepareWill(r)
	} else {
		if !c.prepareSharedEnding(r) {
			return false
		}
		c.collect(r, now)
	}
	return true
}

// finish fixes the one final entry, including at takeover, without waiting
// for Ended. The writer owns the DISCONNECT and orderly socket close (R2).
func (c *Core) finish(acts []Action, s *connectionState, reason byte, now time.Time) []Action {
	if s.phase == closing {
		return acts
	}
	c.captureSharedEnd(s)
	c.unqueueBytes(s, now)            // a final writer entry cancels any ungranted body request
	s.phase, s.replies = closing, nil // cancellation must not requeue prepared owners
	c.cancelDelivery(s, now)
	c.discardPublishes(&s.publishes, now)
	c.cancelPackets(s, now)
	c.cancelChannelRequests(s, now)
	c.cancelIncoming(s)
	var reply connection.Reply = connection.Close{}
	if s.bridge != nil && reason == 0x8B && s.bridge.up {
		// The stop cutoff's preceding cut shares this decision, even if
		// the writer closes before it can consume the final entry.
		s.bridge.closedForStop = true
		reply = &packet.Disconnect{} // MQTT client: normal disconnection
	} else if reason >= 0x80 {
		c.selectClosure(s, packet.DISCONNECT, reason, now)
		reply = &packet.Disconnect{ReasonCode: reason}
	}
	return append(acts, Action{Conn: s.conn, Reply: reply})
}

func (c *Core) detach(s *connectionState) {
	c.countIncoming(s, 0)
	c.setHeld(s, 0)
	if c.sessions[s.clientID] == s {
		delete(c.sessions, s.clientID)
		delete(c.sessionsByID, s.session.ID)
		c.countSession(s.version, -1)
	}
}

func (c *Core) countSession(version packet.Version, delta int64) {
	p := &c.protocols[version]
	p.connected += delta
	p.gauge.Set(p.connected)
}

// setHeld keeps the gauge in step with permission compilation and connection
// removal. A refused reconnect also releases subscriptions loaded for it.
func (c *Core) setHeld(s *connectionState, count int64) {
	if s.held == count {
		return
	}
	c.held += count - s.held
	s.held = count
	c.heldGauge.Set(c.held)
}

// recipientMemberships shares connection capacity and owner wakeups between
// shared subscriptions and queue workers, without building a combined slice.
func (s *connectionState) recipientMemberships(yield func(*sharedMember) bool) {
	for _, members := range [2][]*sharedMember{s.shared, s.workers} {
		for _, m := range members {
			if !yield(m) {
				return
			}
		}
	}
}
