package core

import (
	"container/list"

	"github.com/ifnesi/saguin/internal/store"
)

func (c *Core) unparkRequest(r *storeRequest) {
	q := r.wait
	if q == nil {
		return
	}
	q.Remove(r.waitElement)
	r.wait, r.waitElement = nil, nil
	delete(c.parked, r)
}

// A request has one membership: ready, running, owned by the store, or
// parked on its first unsatisfied dependency. Rechecking that dependency
// can move it to a different wait FIFO.
func (c *Core) parkRequest(r *storeRequest, q *list.List) {
	c.returnReadPermit(r)
	if q != &c.writeWait && q != &c.creditWait {
		// An owner dependency holds only this owner's successors. A read
		// can gate the store order only while awaiting writes or credit;
		// its prerequisite may itself need a later independent request.
		c.releaseRequestOrder(r)
	}
	if r.ready || r.wait != nil {
		panic("core: request already registered")
	}
	r.wait, r.waitElement = q, q.PushBack(r)
	if c.parked == nil {
		c.parked = make(map[*storeRequest]struct{})
	}
	c.parked[r] = struct{}{}
}

func (c *Core) readyRequest(r *storeRequest) {
	c.unparkRequest(r)
	if !r.ready {
		if c.readyAt == len(c.ready) {
			c.ready, c.readyAt = c.ready[:0], 0
		}
		r.ready = true
		c.ready = append(c.ready, r)
	}
}

func (c *Core) wakeRequests(q *list.List) {
	for q.Front() != nil {
		c.readyRequest(q.Front().Value.(*storeRequest))
	}
}

// releaseChannel captures the dependency before clearing ownership. All
// channel releases, including cancellation and synchronous refusal, use it.
func (c *Core) releaseChannel(r *storeRequest) {
	for i, ch := range [2]*channelState{r.channel, r.otherChannel} {
		if ch == nil {
			continue
		}
		if ch.request == r {
			ch.request = nil
		}
		if r.channelElements[i] != nil {
			ch.requestOrder.Remove(r.channelElements[i])
			r.channelElements[i] = nil
			c.wakeRequests(&ch.waitRequests)
		}
	}
}

// Channel order is independent of a request's current runnable membership.
// A lane continuation cannot overtake an older entry parked on this channel.
func (c *Core) registerChannel(r *storeRequest) {
	for i, ch := range [2]*channelState{r.channel, r.otherChannel} {
		if ch != nil && r.channelElements[i] == nil {
			r.channelElements[i] = ch.requestOrder.PushBack(r)
		}
	}
}

// A superseded store read retains only credit. Its core-owned channel place
// transfers to the offset-resolution marker before any owner identity clears.
func (c *Core) replaceChannel(r, next *storeRequest) {
	next.otherChannel, next.channelElements = r.otherChannel, r.channelElements
	for i, ch := range [2]*channelState{r.channel, r.otherChannel} {
		if ch == nil {
			continue
		}
		if next.channelElements[i] != nil {
			next.channelElements[i].Value = next
		}
		if ch.request == r {
			ch.request = next
		}
		if ch.pending == r {
			ch.pending = next
		}
	}
	r.channelElements = [2]*list.Element{}
}

func (c *Core) releaseLane(r *storeRequest) {
	if lane := r.lane; lane != nil && lane.request == r {
		lane.request = nil
		if lane.departing == r {
			lane.departing = nil
		}
		c.wakeRequests(&lane.waitRequests)
	}
}

// connectionWorkChanged belongs to the transitions returning delivery/read/
// reply ownership. It wakes only requests parked on this connection, including
// a takeover waiting for the old writer and subscription changes awaiting reads.
func (c *Core) connectionWorkChanged(s *connectionState) {
	if s != nil {
		c.wakeRequests(&s.workWait)
	}
}

func (c *Core) batchCapacityChanged() {
	c.memoryReady = true
	// Assign returned slots before newly ready requests can consume them.
	// These slots cover handed-over bodies as well as eligible PUBRELs.
	for e := c.batchWait.Front(); e != nil; {
		next := e.Next()
		r := e.Value.(*storeRequest)
		if c.admitBatch(&r.publishes) {
			c.readyRequest(r)
		}
		e = next
	}
}

func (c *Core) releaseRequestOrder(r *storeRequest) {
	if c.orderedRead == r {
		c.orderedRead = nil
		c.wakeRequests(&c.orderWait)
	}
}

func (c *Core) cancelPendingRequest(r *storeRequest) {
	if lane := r.lane; lane != nil && lane.departing == r {
		lane.departing = nil
	}
	c.returnReadPermit(r)
	c.unparkRequest(r)
	c.releaseRequestOrder(r)
	c.releaseChannel(r)
	c.releaseSharedRequest(r)
	r.cancelled = true
}

func (c *Core) completeAcknowledgement(s *connectionState) {
	s.pendingAcks--
	c.connectionWorkChanged(s)
}

func (c *Core) completeSubscription(s *connectionState) {
	s.pendingSubscriptions--
	c.queueConsumers(s)
}

func (c *Core) returnWriterDelivery(s *connectionState) {
	s.writingDeliveries--
	c.connectionWorkChanged(s)
}

func (c *Core) returnDeliveryWrite(s *connectionState) {
	s.deliveryRequests--
	s.deliveryWrites--
	c.connectionWorkChanged(s)
}

func (c *Core) refuseDeliverySave(u *consumerState) {
	u.refusedSave = true
	if u.retry == nil {
		u.retry = c.deliveryRetries.PushBack(u)
	}
}

func (c *Core) removeDeliveryRetry(u *consumerState) {
	if u.retry != nil {
		c.deliveryRetries.Remove(u.retry)
		u.retry = nil
	}
}

// Only metadata completions return page credit. Consumers parked here are
// the allocator's registered waiters, not a scan of connected consumers.
func (c *Core) returnMetadataCredit() {
	c.metadataReads--
	c.wakeRequests(&c.metadataWait)
	for c.deliveryDeferred.Len() != 0 {
		c.queueConsumer(c.deliveryDeferred.peek())
	}
}

// A clean takeover marks old shared memberships as ending before its I/O
// completes. Abandoning that CONNECT leaves a durable old session away;
// cancellation must release its speculative ending dependency as well.
func (c *Core) abortConnect(s *connectionState) {
	if a := s.attempt; a != nil && a.takeover != nil && a.takeover.session.Expiry != 0 {
		c.cancelSharedEnd(a.takeover)
	}
	s.attempt = nil
	c.connectionWorkChanged(s)
}

func (c *Core) cancelSharedEnd(s *connectionState) {
	for _, m := range s.shared {
		m.ending = false
	}
	if s.sharedEnd == nil {
		return
	}
	// Closed may already have removed the live memberships. The captured
	// ending keys still identify every affected group's dependency.
	for _, row := range s.sharedEnd.Groups {
		g := c.sharesByID[row.ID]
		if g == nil && row.ID == 0 {
			g = c.shares[row.Filter]
		}
		if g != nil {
			delete(g.endings, s.session.ID)
			c.queueShared(g)
			c.wakeRequests(&g.workWait)
		}
	}
}

func (c *Core) localWorkReady() bool {
	return c.readyAt != len(c.ready) || c.memoryReady || c.readyDeliveries() ||
		(c.retentionReady && c.phase < quiescing && c.Credit() > 0)
}

// endLane is the terminal transition. Cancellation must not leave advancing
// a successor to a caller; some cancellations happen during another ending.
func (c *Core) endLane(lane *sessionLane, r *storeRequest) {
	if lane == nil {
		return
	}
	if lane.request == r {
		lane.request = nil
		c.advance(lane)
	}
}

// Returned credit belongs to the oldest registered eligible read until it is
// dispatched or ceases to need it. Reads awaiting writes cannot reserve the
// writer's last slot. The event drains these registrations before returning.
func (c *Core) returnStoreCredit() {
	if c.failure != nil || c.readPermit != nil || c.Credit() == 0 {
		return
	}
	for e := c.creditWait.Front(); e != nil; e = e.Next() {
		r := e.Value.(*storeRequest)
		if r.classification == store.OrderedRequest && (c.outstanding != nil || len(c.sealed) != 0 || c.collecting != nil) {
			continue
		}
		c.readPermit = r
		c.readyRequest(r)
		return
	}
}

func (c *Core) returnReadPermit(r *storeRequest) {
	if c.readPermit == r {
		c.readPermit = nil
		c.returnStoreCredit()
	}
}

func (c *Core) freeStoreCredit() int {
	n := c.Credit()
	if c.readPermit != nil {
		n--
	}
	return n
}
