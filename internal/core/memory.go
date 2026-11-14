package core

import (
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/store"
)

// memoryRequest is embedded in its bounded owner: a reader, a firing Will
// or a window entry. Each class uses the same FIFO links and byte accounting;
// waiting for publication capacity never holds a delivery behind its head.
type memoryRequest struct {
	bytes      int64
	conn       *connectionState
	delivery   *outgoing
	will       *storeRequest
	ticket     uint64 // original request order; zero when unqueued
	prev, next *memoryRequest
	parked     bool // reader body waiting for CONNECT, outside the FIFO
}

const memoryRecoveryDelay = time.Second

// memoryEpisode outlives individual full intervals. Its deadline starts only
// after ready grants leave no eligible reservation waiting for bytes.
type memoryEpisode struct {
	active    bool
	recoverAt time.Time
}

// takeBodyBytes transfers the reader's one outstanding grant to its body.
// The reader reports the reservation it keeps after compression. Return the
// rest, then carry that charge through refusal or storage independently of
// the reader's later grants and end.
func (c *Core) takeBodyBytes(ev connection.Event, now time.Time) int64 {
	s := c.connections[ev.Conn]
	if ev.Bytes < 0 {
		panic("core: negative message body size")
	}
	if ev.Bytes > s.readerBytes {
		panic("core: body exceeds reader's byte grant")
	}
	reserved := s.readerBytes
	s.readerBytes = 0
	c.releaseBodyBatch(s)
	c.returnBytes(reserved-ev.Bytes, now)
	return ev.Bytes
}

// requestBytes joins the incoming FIFO at the body's original order.
// A CONNECT-dependent body waits off the FIFO until takeover's writer reports
// are in and any prerequisite Will owns bytes and a slot. The Will inherits
// its dependent body's order within the incoming budget.
func (c *Core) requestBytes(ev connection.Event, now time.Time) {
	s := c.connections[ev.Conn]
	if s.end.seen || (s.phase != connecting && s.phase != connected) {
		return
	}
	if ev.Bytes <= 0 || ev.Bytes > int64(c.limits.MaxIncomingMemory) || s.readerRequest.bytes != 0 || s.readerBytes != 0 {
		panic("core: invalid reader byte request")
	}
	s.readerRequest.conn = s
	c.queueMemory(&s.readerRequest, ev.Bytes)
	if c.admission.delay != 0 {
		c.pausePeer(s, now)
	}
}

// allowConnectBodies restores a pending body at its original ticket order.
func (c *Core) allowConnectBodies(s *connectionState) {
	s.attempt.bodiesReady = true
	if r := &s.readerRequest; r.parked {
		r.parked = false
		c.insertMemory(r, c.memoryBefore(r.class(), r.ticket))
	}
	c.memoryReady = true
	c.connectionWorkChanged(s)
}

func (c *Core) queueMemory(r *memoryRequest, n int64) {
	c.memoryReady = true
	var before *memoryRequest
	if w := r.will; w != nil {
		if w.willKind == saveConnect && w.conn.readerRequest.bytes != 0 {
			before = &w.conn.readerRequest
		}
		// A firing already ahead of CONNECT in its session lane is that
		// CONNECT's prerequisite too. Body requests can arrive in another
		// order than CONNECTs, so use the oldest dependent body's place.
		for _, pending := range w.lane.pending {
			if pending.kind != connectSession {
				continue
			}
			body := &pending.conn.readerRequest
			if body.bytes != 0 && (before == nil || body.ticket < before.ticket) {
				before = body
			}
		}
	}
	r.bytes = n
	if before == nil {
		c.memoryTicket++
		r.ticket = c.memoryTicket
	} else {
		// Its prerequisite has the same age and precedes it within the
		// incoming FIFO; no younger eligible incoming request can pass it.
		r.ticket = before.ticket
		// The dependent body belongs to the reader list. Place this Will
		// among Wills at that same age; incoming selection merges the lists.
		before = c.memoryBefore(r.class(), r.ticket)
	}
	if s := r.conn; s != nil && s.phase == connecting && !s.attempt.bodiesReady {
		r.parked = true
		return
	}
	c.insertMemory(r, before)
}

// memoryBefore is needed only when restoring a parked ticket or inserting
// its prerequisite. Ordinary requests append and grants take the head.
func (c *Core) memoryBefore(class int, ticket uint64) *memoryRequest {
	var before *memoryRequest
	for r := c.memoryTail[class]; r != nil && r.ticket > ticket; r = r.prev {
		before = r
	}
	return before
}

func (c *Core) insertMemory(r, before *memoryRequest) {
	class := r.class()
	r.next = before
	if before == nil {
		r.prev = c.memoryTail[class]
		c.memoryTail[class] = r
	} else {
		r.prev = before.prev
		before.prev = r
	}
	if r.prev == nil {
		c.memoryHead[class] = r
	} else {
		r.prev.next = r
	}
}

func (r *memoryRequest) class() int {
	if r.delivery != nil {
		return 1
	}
	if r.will != nil {
		return 2
	}
	return 0
}

func (c *Core) unqueueBytes(s *connectionState, now time.Time) {
	c.unqueueMemory(&s.readerRequest, now)
}

func (c *Core) unqueueMemory(r *memoryRequest, now time.Time) {
	if r.bytes == 0 {
		return
	}
	class := r.class()
	if c.admission.delay != 0 && r.conn != nil && r.delivery == nil && r.will == nil {
		c.resumePeer(r.conn, now)
	}
	if !r.parked {
		if r.prev == nil {
			c.memoryHead[class] = r.next
		} else {
			r.prev.next = r.next
		}
		if r.next == nil {
			c.memoryTail[class] = r.prev
		} else {
			r.next.prev = r.prev
		}
	}
	r.parked = false
	r.bytes, r.ticket, r.prev, r.next = 0, 0, nil, nil
	c.memoryReady = true
}

// memoryNeedsPeer judges the oldest delivery independently of the gate.
// Only outgoing reservations held for peer input or writer completion can
// make its memory wait peer-dependent. Incoming requests have their own budget.
func (c *Core) memoryNeedsPeer() bool {
	out := c.memoryHead[1]
	if out == nil {
		return false
	}
	return out.bytes > int64(c.limits.MaxOutgoingMemory)-c.memoryPeer
}

// memoryCandidate checks a budget's oldest eligible request. The incoming
// reader and Will lists merge by original ticket; a prerequisite Will wins
// its dependent body's equal ticket. Closing the gate excludes the reader
// list without scanning it or changing its order. Publication capacity still
// applies to both lists, and a head waiting for bytes holds back its budget.
func (c *Core) memoryCandidate(class int) (*memoryRequest, bool) {
	r := c.memoryHead[class]
	if class == 0 {
		if !c.publicationRoom() {
			return nil, false
		}
		if !c.admission.closedAt.IsZero() {
			r = nil
		}
		if will := c.memoryHead[2]; will != nil && (r == nil || will.ticket <= r.ticket) {
			r = will
		}
	}
	if r == nil {
		return nil, false
	}
	if r.bytes <= c.memoryLimit(class)-c.memoryUsed[class] {
		return r, true
	}
	return nil, true
}

func (c *Core) memoryLimit(class int) int64 {
	if class == 0 {
		return int64(c.limits.MaxIncomingMemory)
	}
	return int64(c.limits.MaxOutgoingMemory)
}

// giveBytes never skips a class's eligible head for a smaller request. The one
// answer slot is empty until this grant, even if the reader has stopped;
// its later Ended returns any grant it did not use. No socket wait is on
// the core's path (R2), and no metric is read to decide admission (R3).
// Once a stop begins or the store fails, no more bytes are granted.
func (c *Core) giveBytes(acts []Action, now time.Time) []Action {
	// Only incoming grants depend on the gate. Outgoing reservations can
	// finish scheduling first; dispatch evaluates their owner changes once,
	// at the event boundary, instead of revisiting each intermediate stage.
	for _, class := range [...]int{1, 0} {
		for c.phase < quiescing {
			if class == 0 && c.admission.delay != 0 && (c.memoryHead[0] != nil || c.memoryHead[2] != nil) {
				c.evaluateAdmission(now)
			}
			r, waiting := c.memoryCandidate(class)
			// A connecting reader holds its publication until CONNECT succeeds;
			// granting its body needs no store answer and produces no reply.
			if r == nil {
				if waiting {
					if c.memoryFullAt[class].IsZero() {
						c.memoryFullAt[class] = now
					}
					c.noteMemoryWait(class, now)
				}
				break
			}
			// The head fits before it leaves the queue. Looking for room after
			// removing it but before charging its grant creates false intervals.
			c.endMemoryWait(class, now)
			n := r.bytes
			c.unqueueMemory(r, now)
			c.memoryUsed[class] += n
			c.memoryGauge[class].Set(c.memoryUsed[class])
			if d := r.delivery; d != nil {
				if c.admission.delay != 0 {
					c.memoryPeer += n - d.storedBytes
				}
				d.bytes, d.stage = n, deliveryReserved
				d.consumer.reserved++
				if d.consumer.reserved == 1 {
					c.queueConsumer(d.consumer)
				}
			} else if r.will != nil {
				w := r.will
				c.reserveBodyBatch()
				w.publishes.bytes = n
				w.kind = readWill
				if w.willKind == saveConnect {
					c.allowConnectBodies(w.conn)
				}
				c.readyRequest(w) // its session lane is already reserved
			} else {
				c.reserveBodyBatch()
				r.conn.readerBatch = true
				r.conn.readerBytes = n
				acts = append(acts, Action{Conn: r.conn.conn, Bytes: n})
			}
		}
	}
	return acts
}

// endMemoryWait ends this budget's interval once its current head fits, or
// no head remains. A gate or batch wait alone neither starts nor restarts it.
// Queue mutations are observed after grants have also charged their bytes.
func (c *Core) endMemoryWait(class int, now time.Time) {
	if c.memoryFullAt[class].IsZero() {
		return
	}
	if r, waiting := c.memoryCandidate(class); r == nil && waiting {
		return
	}
	c.memoryFull[class].Add(now.Sub(c.memoryFullAt[class]))
	c.memoryFullAt[class] = time.Time{}
}

// noteMemoryWait cancels pending recovery on every byte wait, but attempts
// the WARN only when the episode opens. Log admission never controls this state.
func (c *Core) noteMemoryWait(class int, now time.Time) {
	e := &c.memoryEpisodes[class]
	e.recoverAt = time.Time{}
	if e.active {
		return
	}
	e.active = true
	message := "incoming message memory full: publishers slowed"
	if class == 1 {
		message = "outgoing message memory full: deliveries wait"
	}
	c.queueLog(logger.Warn, message, now)
}

// updateMemoryEpisodes observes the settled event, after ready grants. Gate
// and batch waits do not prevent recovery; only an eligible byte wait does.
// Stopping cancels episodes without claiming that either budget recovered.
func (c *Core) updateMemoryEpisodes(now time.Time) {
	for class := range c.memoryEpisodes {
		e := &c.memoryEpisodes[class]
		if c.phase >= quiescing {
			*e = memoryEpisode{}
			continue
		}
		if !e.active {
			continue
		}
		if r, waiting := c.memoryCandidate(class); r == nil && waiting {
			e.recoverAt = time.Time{}
		} else if e.recoverAt.IsZero() {
			e.recoverAt = now.Add(memoryRecoveryDelay)
		}
	}
}

func (c *Core) memoryRecoveryTimer() time.Time {
	return firstTime(c.memoryEpisodes[0].recoverAt, c.memoryEpisodes[1].recoverAt)
}

// recoverMemory runs last in the timer order, after snapshot work and its
// ready grants. Recheck each due budget, incoming before outgoing. State
// advances before log admission, including when the INFO is dropped.
func (c *Core) recoverMemory(now time.Time) {
	for class := range c.memoryEpisodes {
		e := &c.memoryEpisodes[class]
		if e.recoverAt.IsZero() || now.Before(e.recoverAt) {
			continue
		}
		if r, waiting := c.memoryCandidate(class); r == nil && waiting {
			e.recoverAt = time.Time{}
			continue
		}
		*e = memoryEpisode{}
		message := "incoming message memory has room again"
		if class == 1 {
			message = "outgoing message memory has room again"
		}
		c.queueLog(logger.Info, message, now)
	}
}

// returnBytes settles exactly one charge, after storage, refusal, a stop's
// discarded PUBLISH or an unread grant's reader end. An accepted body's
// charge belongs to its store request, never to the reader after handover.
// Room ends a full interval at this event's time, even during a stop or failure.
func (c *Core) returnBytes(n int64, now time.Time) {
	c.returnMemory(0, n, now)
}

// returnMemory returns bytes to the class that reserved them. Both classes
// share queue wakeups and observations, but never capacity.
func (c *Core) returnMemory(class int, n int64, now time.Time) {
	if n == 0 {
		return
	}
	if n < 0 || n > c.memoryUsed[class] {
		panic("core: invalid returned message bytes")
	}
	c.memoryUsed[class] -= n
	c.memoryReady = true
	c.memoryGauge[class].Set(c.memoryUsed[class])
	c.endMemoryWait(class, now)
}

// refusedPublish uses the existing reply reservation. QoS 0 needs no reply.
// An identifier is judged by the core, after CONNECT, before applying
// reader policy. Never retain a refused body while deciding or replying.
func (c *Core) refusedPublish(acts []Action, ev connection.Event, now time.Time) []Action {
	c.returnBytes(c.takeBodyBytes(ev, now), now)
	if c.phase >= quiescing {
		return acts
	}
	p := ev.Packet.(connection.PublishRefusal)
	service := requestService(p.Kind)
	request := p.Request && service < serviceKinds
	if request && p.QoS == 0 {
		c.requestsTotal[service][requestDropped].Inc()
	}
	s := c.connections[ev.Conn]
	if s.phase >= refused || s.end.seen {
		return acts
	}
	if p.QoS == 0 {
		return acts
	}
	identity, reason := c.classifyPublish(s, p.PacketID, p.QoS, ev.Quota)
	if reason != 0 {
		return c.finish(acts, s, reason, now)
	}
	if identity.repeat != nil {
		return c.repeatIncoming(acts, s, p.PacketID, identity.repeat, nil)
	}
	return c.queuePublication(acts, &publication{record: store.Record{QoS: p.QoS}, conn: s,
		packetID: p.PacketID, quota: ev.Quota, refusal: ev.Reason, refused: p.Counter, request: request, requestKind: service, publishIdentity: identity}, now)
}
