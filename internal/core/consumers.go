package core

import (
	"cmp"
	"container/heap"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// consumerQueue uses the standard indexed heap, with membership stored in each
// consumer. Ready queues order by wake event, then connection key. A
// connection's prepared owners instead order by their next selected delivery.
type consumerQueue struct {
	items      []*consumerState
	prepared   bool
	boundaries bool
}

func (q *consumerQueue) Len() int { return len(q.items) }

func (q *consumerQueue) Less(i, j int) bool {
	a, b := q.items[i], q.items[j]
	if q.boundaries {
		return a.boundaries[0].items[0].offset < b.boundaries[0].items[0].offset
	}
	if q.prepared {
		return a.offer.order < b.offer.order
	}
	return consumerBefore(a, b)
}

func (q *consumerQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	switch {
	case q.boundaries:
		q.items[i].sharedBoundIndex, q.items[j].sharedBoundIndex = i+1, j+1
	case q.prepared:
		q.items[i].preparedIndex, q.items[j].preparedIndex = i+1, j+1
	default:
		q.items[i].index, q.items[j].index = i, j
	}
}

func (q *consumerQueue) Push(value any) {
	u := value.(*consumerState)
	q.items = append(q.items, u)
	switch {
	case q.boundaries:
		u.sharedBoundIndex = len(q.items)
	case q.prepared:
		u.preparedIndex = len(q.items)
	default:
		u.queue, u.index = q, len(q.items)-1
	}
}

func (q *consumerQueue) Pop() any {
	i := len(q.items) - 1
	u := q.items[i]
	q.items[i] = nil
	q.items = q.items[:i]
	switch {
	case q.boundaries:
		u.sharedBoundIndex = 0
	case q.prepared:
		u.preparedIndex = 0
	default:
		u.queue, u.index = nil, -1
	}
	return u
}

func (q *consumerQueue) peek() *consumerState {
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

// reserveTurn keeps the head owner's reservations before the next owner's
// hand-off. A later payload must not hold memory needed to load that earlier
// delivery. The next owner in this heap is one of the root's two children.
func (q *consumerQueue) reserveTurn(d *outgoing) bool {
	return q.peek() == d.consumer &&
		(len(q.items) < 2 || d.order < q.items[1].offer.order) &&
		(len(q.items) < 3 || d.order < q.items[2].offer.order)
}

func (q *consumerQueue) remove(u *consumerState) { heap.Remove(q, u.index) }

// reserve provisions queue storage at registration, never during delivery.
// Retain the backing array as consumers leave and clear its vacated entries.
func (q *consumerQueue) reserve(n int) {
	if cap(q.items) < n {
		q.items = slices.Grow(q.items, n-len(q.items))
	}
}

func connectionBefore(a, b *connectionState) bool {
	return compareConnections(a, b) < 0
}

func compareConnections(a, b *connectionState) int {
	if order := cmp.Compare(a.accept.Bridge, b.accept.Bridge); order != 0 {
		return order
	}
	if order := cmp.Compare(a.accept.Listener, b.accept.Listener); order != 0 {
		return order
	}
	return cmp.Compare(a.order, b.order)
}

func consumerBefore(a, b *consumerState) bool {
	if a.ticket != b.ticket {
		return a.ticket < b.ticket
	}
	if a.conn != b.conn {
		if a.conn == nil {
			return true
		}
		if b.conn == nil {
			return false
		}
		return connectionBefore(a.conn, b.conn)
	}
	if a.conn == nil && a.share.id != b.share.id {
		return a.share.id < b.share.id
	}
	if a.channel.row.ID != b.channel.row.ID {
		return a.channel.row.ID < b.channel.row.ID
	}
	return a.registration < b.registration
}

// newConsumer registers reusable queues and read storage for one channel.
func (c *Core) newConsumer(s *connectionState, ch *channelState, position int64) *consumerState {
	c.nextConsumer++
	u := &consumerState{conn: s, channel: ch, index: -1, position: position, scan: position,
		persisted: position, available: ch.committedTail, registration: c.nextConsumer}
	u.read.conn, u.read.consumer = s, u
	n := len(c.connections)*(len(c.channels)+1) + c.bridgeConsumerLimit
	if c.admission.delay != 0 && s != nil {
		u.admission.deadline.owner, u.admission.deadline.conn = u, s
		u.admission.progress.owner, u.admission.progress.conn = u, s
		c.admission.ages.reserve(2 * n)
		c.admission.memory.reserve(n)
		if cap(c.admission.dirty) < n {
			c.admission.dirty = slices.Grow(c.admission.dirty, n-len(c.admission.dirty))
		}
	}
	c.deliveryReady.reserve(n)
	c.deliveryDeferred.reserve(n)
	c.deliveryLoads.reserve(n)
	c.deliveryChanges.reserve(n)
	if s != nil {
		s.prepared.reserve(s.prepared.Len() + 1)
	}
	return u
}

// consumer finds an active position without visiting unrelated filters.
func (s *connectionState) consumer(channel int64) *consumerState {
	i, found := slices.BinarySearchFunc(s.consumers, channel, func(u *consumerState, id int64) int { return cmp.Compare(u.channel.row.ID, id) })
	if found {
		return s.consumers[i]
	}
	return nil
}

func (s *connectionState) addConsumer(u *consumerState) {
	i, _ := slices.BinarySearchFunc(s.consumers, u.channel.row.ID, func(other *consumerState, id int64) int { return cmp.Compare(other.channel.row.ID, id) })
	s.consumers = slices.Insert(s.consumers, i, u)
}

// keepsPosition is the core's lifetime decision, shared by the transaction
// snapshot and the transition ending ordinary subscription reach.
func keepsPosition(s *connectionState, u *consumerState) bool {
	return s.session.Expiry != 0 && u.channel.row.ID != 1 &&
		(u.channel.row.Type == store.Append || u.channel.row.Type == store.Queue)
}

// subscriptionCheckpoint preserves examined progress but cannot pass a
// preparation that never reached the writer. Capture it before cancellation
// or acknowledgements can change the old scan's progress.
func subscriptionCheckpoint(u *consumerState, now time.Time) store.ChannelPosition {
	position := store.ChannelPosition{Channel: u.channel.row.ID, Offset: u.position}
	position.Seen.Int64, position.Seen.Valid = u.seen, u.seen != 0
	for d := u.last; d != nil; d = d.prev {
		if !d.attempted && !d.initial && !d.settled {
			position.Offset = min(position.Offset, d.offset)
		}
	}
	if position.Offset != u.position {
		position.Seen.Int64, position.Seen.Valid = now.UnixMilli(), true
	}
	return position
}

// endSubscriptionReach is the common transition for explicit removals and
// reach reconciliation. A durable log position remains without an active
// scan; existing exchanges retain only their MQTT obligations.
func (c *Core) endSubscriptionReach(s *connectionState, u *consumerState, now time.Time) {
	u.markersBefore = 0 // subscription removal committed its marker reclamation
	c.indexConsumer(u, false)
	if keepsPosition(s, u) {
		position := subscriptionCheckpoint(u, now)
		u.supersedePosition(position.Offset, position.Seen.Int64)
		u.filters = nil
	} else {
		s.consumers = slices.DeleteFunc(s.consumers, func(other *consumerState) bool { return other == u })
		u.pass, u.dirty = nil, false
	}
	c.queueConsumer(u)
}

// supersedePosition discards the old scan's progress and protections as one
// operation. A later filter cannot restore movement by these exchanges.
func (u *consumerState) supersedePosition(position, seen int64) {
	for d := u.last; d != nil; d = d.prev {
		d.exchange.NoMove = true
		u.untrackDelivery(d)
	}
	for id, end := range u.settled {
		end.offset, end.recoveredOffset = 0, 0
		u.settled[id] = end
	}
	u.position, u.scan, u.seen = position, position, seen
	u.settledThrough, u.rewind, u.pass = 0, 0, nil
	u.dirty = len(u.settled) != 0 || (u.conn.stored && u.position != u.persisted)
	u.admission.disqualify()
	clear(u.recovered)
	clear(u.live)
}

// observePositionFloor records one unknown loss observation per movement.
// CONNECT (including takeover) and reactivation use the same operation.
func (c *Core) observePositionFloor(u *consumerState, now time.Time) {
	if u.position >= u.channel.row.Floor {
		return
	}
	c.recordPositionLoss(positionLossKey{name: u.conn.clientID, channel: u.channel.row.Name},
		positionLoss{kind: "session", position: u.position, floor: u.channel.row.Floor}, now)
	u.position, u.scan, u.seen = u.channel.row.Floor, max(u.scan, u.channel.row.Floor), now.UnixMilli()
	u.dirty = (u.conn.stored || u.markersBefore != 0) && u.position != u.persisted
	c.queueConsumer(u)
}

func (c *Core) channel(id int64) *channelState {
	i, found := slices.BinarySearchFunc(c.channels, id, func(ch *channelState, id int64) int { return cmp.Compare(ch.row.ID, id) })
	if found {
		return c.channels[i]
	}
	return nil
}

// queueConsumers wakes positions sharing the connection's preparation and send
// capacity. Detached prepared owners and recovery keep their existing queues.
func (c *Core) queueConsumers(s *connectionState) {
	c.connectionWorkChanged(s)
	c.queueConsumer(s.service)
	for m := range s.recipientMemberships {
		for _, u := range m.owners {
			c.queueConsumer(u)
		}
		c.queueShared(m.group)
	}
	scan := s.nextScan()
	for _, u := range s.consumers {
		c.queueConsumerFor(u, scan)
	}
	// Indexed owners were visited above. Only detached preparations and
	// the separate recovery owner need another visit, once each.
	if u := s.resuming; u != nil && !u.indexed {
		c.queueConsumerFor(u, scan)
	}
	if u := s.prepared.peek(); u != nil && !u.indexed && u != s.resuming {
		c.queueConsumerFor(u, scan)
	}
}

// nextScan visits channels cyclically by id, selecting one delivery before
// continuing to the next. One outstanding visit fixes the order regardless
// of which store reader answers first. Empty channels do not hold up others.
func (s *connectionState) nextScan() *consumerState {
	if s.scanning != nil {
		return s.scanning
	}
	n := len(s.consumers)
	at, _ := slices.BinarySearchFunc(s.consumers, s.scanChannel+1, func(u *consumerState, id int64) int { return cmp.Compare(u.channel.row.ID, id) })
	for i := range n {
		u := s.consumers[(at+i)%n]
		if !u.indexed || u.held || u.refusedSave || (u.pass != nil && u.pass.before > u.channel.resolved) {
			continue
		}
		if u.pass != nil || u.scan < u.available {
			return u
		}
	}
	return nil
}

func (s *connectionState) subscriptionsBusy() bool {
	for m := range s.recipientMemberships {
		for _, u := range m.owners {
			if u.request != nil || u.writing != nil || u.saving != nil {
				return true
			}
		}
	}
	for _, u := range s.consumers {
		if u.request != nil || u.writing != nil || u.saving != nil {
			return true
		}
	}
	return false
}

// nextOffer maintains a separate membership for unsent owners. Subscription
// changes can detach an owner from the active consumer without ending its
// selected exchanges. Their original selection order still governs hand-off.
func (u *consumerState) nextOffer(d *outgoing) {
	d = nextPublish(d)
	u.offer = d
	if u.service {
		return
	}
	q := &u.conn.prepared
	switch {
	case d == nil && u.preparedIndex != 0:
		heap.Remove(q, u.preparedIndex-1)
	case d != nil && u.preparedIndex == 0:
		heap.Push(q, u)
	case d != nil:
		heap.Fix(q, u.preparedIndex-1)
	}
}

// queueConsumer reconciles eligibility without changing a waiting consumer's
// ticket. Window, memory and read completions wake their owners directly;
// committed records wake matching consumers through the subscription index.
func (c *Core) queueConsumer(u *consumerState) {
	if u != nil {
		if u.conn == nil {
			c.queueConsumerFor(u, nil)
			return
		}
		c.queueConsumerFor(u, u.conn.nextScan())
	}
}

func (c *Core) queueConsumerFor(u, scan *consumerState) {
	if u == nil {
		return
	}
	if c.admission.delay != 0 {
		c.touchAdmission(u)
	}
	if u.sharedCursor != nil {
		c.indexSharedWriter(u)
		c.indexSharedRefusal(u)
		c.indexSharedCapacity(u)
	} else if u.conn == nil {
		c.wakeSharedRefusals(u)
	}
	var queue *consumerQueue
	s := u.conn
	if s != nil && (s.end.seen || s.phase >= refused) {
		c.removeDeliveryRetry(u)
	}
	if s == nil {
		queue = c.sharedQueue(u)
	} else if c.failure == nil && (c.phase < quiescing || c.phase == stopping) && s.phase == connected && !s.end.seen &&
		c.connections[s.conn] == s && !u.refusedSave {
		switch {
		case u.removals.Len() != 0:
			queue = &c.deliveryLoads
		case u.needsWrite():
			queue = &c.deliveryChanges
		case c.phase >= quiescing:
		case s.resuming == u && c.canResumeRelease(s):
			queue = &c.deliveryLoads
		case u.service && len(s.answers) != 0 && c.preparationRoom(u) > 0:
			queue = &c.deliveryLoads
		case c.canOffer(u):
			queue = &c.deliveryLoads
		case c.canReservePayload(u):
			queue = &c.deliveryLoads
		case u.request != nil:
		case u.payloadReady():
			queue = &c.deliveryLoads
		case u.service:
		case u.share != nil:
		case !c.sharesReady(s):
		case s.resuming != nil && s.resuming != u:
		case s.resuming == u && len(s.resume) != 0 && s.publishPreparations() < c.deliveryWindow(s):
			queue = &c.deliveryLoads
		case s.pendingSubscriptions != 0:
		case s.resuming == u && (s.resumeAfter < s.resumeThrough || s.releasedThrough < s.resumeThrough):
			if c.resumeRead(u).Limit > 0 {
				queue = &c.deliveryReady
			}
		case s.resuming == u:
			// Finish recovered PUBLISH hand-offs before normal selection.
			// New preparations would otherwise keep recovery open while
			// waiting for that same recovery's ordering barrier to end.
		case u.pass == nil && c.preparationRoom(u) <= 0:
		case !c.consumerActive(s, u):
		case u.pass != nil && u.pass.before > u.channel.resolved:
			// The pass's first read waits for every publication below its
			// boundary to be stored or refused, holding no request or queue.
			if !slices.Contains(u.channel.passes, u) {
				u.channel.passes = append(u.channel.passes, u)
			}
		case scan != u:
		case u.pass != nil:
			// An empty or nonmatching pass can end even with a full window;
			// only finding an eligible value makes it wait for send capacity.
			if !u.pass.waiting || c.retainedRoom(u) > 0 {
				queue = &c.deliveryReady
			}
		case u.scan < u.available:
			queue = &c.deliveryReady
		}
	}
	if u.queue == queue {
		return
	}
	if u.queue == &c.deliveryReady || queue == &c.deliveryReady {
		c.deliveryReadyChanged = true
	}
	wasReady := u.queue != nil
	if u.queue != nil {
		u.queue.remove(u)
	}
	if queue != nil {
		if !wasReady {
			u.ticket = c.wakeTicket
		}
		heap.Push(queue, u)
	}
}

// deliveryOwners visits the active consumer and detached exchange owners in
// stable order. The fixed window and reserved identifiers bound this list;
// no owner is kept merely for a settled QoS 0 gap.
func (s *connectionState) deliveryOwners(owners []*consumerState) []*consumerState {
	owners = append(owners, s.consumers...)
	for m := range s.recipientMemberships {
		owners = append(owners, m.owners...)
	}
	if s.resuming != nil && !slices.Contains(owners, s.resuming) {
		owners = append(owners, s.resuming)
	}
	for i := range s.window {
		if u := s.window[i].consumer; u != nil && !slices.Contains(owners, u) {
			owners = append(owners, u)
		}
	}
	ids := make([]uint16, 0, len(s.settlements))
	for id := range s.settlements {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if u := s.settlements[id]; !slices.Contains(owners, u) {
			owners = append(owners, u)
		}
	}
	return owners
}

// filterPrefix selects the literal levels preceding the first wildcard. A
// topic visits only its own level prefixes, so unrelated filters need no
// matching work. The empty bucket includes leading wildcards and /+ filters.
func filterPrefix(filter string) string {
	if i := strings.IndexAny(filter, "+#"); i >= 0 {
		return filter[:max(0, i-1)]
	}
	return filter
}

// indexConsumer updates standard map buckets only at subscription changes.
// Each complete filter shares its matching check across all its consumers.
func (c *Core) indexConsumer(u *consumerState, add bool) {
	if u.indexed == add {
		return
	}
	u.indexed = add
	indexConsumerOwner(u)
	for i := range u.filters {
		sub := &u.filters[i]
		prefix := filterPrefix(sub.Filter)
		filters := u.channel.filters[prefix]
		if add {
			if filters == nil {
				filters = make(map[string]map[*consumerState]struct{})
				u.channel.filters[prefix] = filters
			}
			if filters[sub.Filter] == nil {
				filters[sub.Filter] = make(map[*consumerState]struct{})
			}
			filters[sub.Filter][u] = struct{}{}
		} else {
			delete(filters[sub.Filter], u)
			if len(filters[sub.Filter]) == 0 {
				delete(filters, sub.Filter)
			}
			if len(filters) == 0 {
				delete(u.channel.filters, prefix)
			}
		}
	}
}

// indexConsumerOwner keeps the channel index complete while an unsubscribed
// or recovered owner still has exchanges. Their final release removes it.
// Several generations can share a connection and channel; creation order
// breaks that tie without choosing actions by pointer or map order.
func indexConsumerOwner(u *consumerState) {
	add := u.indexed || u.last != nil
	if u.registered == add {
		return
	}
	u.registered = add
	consumers := u.channel.consumers
	i, found := slices.BinarySearchFunc(consumers, u, func(a, b *consumerState) int {
		if a.conn == nil || b.conn == nil {
			if a.conn != b.conn {
				if a.conn == nil {
					return -1
				}
				return 1
			}
			return cmp.Compare(a.share.id, b.share.id)
		}
		if order := compareConnections(a.conn, b.conn); order != 0 {
			return order
		}
		return cmp.Compare(a.registration, b.registration)
	})
	if add {
		u.channel.consumers = slices.Insert(consumers, i, u)
	} else if found {
		u.channel.consumers = slices.Delete(consumers, i, i+1)
	}
}

// wakeConsumers uses the committed record's core-held topic, without reading
// metadata or allocating. Map order chooses no action: ready work enters the
// existing heap under this event's ticket and its stable connection key.
func (c *Core) wakeConsumers(ch *channelState, name string, offset int64) {
	c.wakeMatching(ch.filters[""], name, offset)
	for i := range len(name) {
		if i > 0 && name[i] == '/' {
			c.wakeMatching(ch.filters[name[:i]], name, offset)
		}
	}
	c.wakeMatching(ch.filters[name], name, offset)
}

func (c *Core) wakeMatching(filters map[string]map[*consumerState]struct{}, name string, offset int64) {
	for filter, consumers := range filters {
		if !topic.Match(filter, name) {
			continue
		}
		for u := range consumers {
			u.available = max(u.available, offset+1)
			if u.conn == nil {
				if c.admission.delay != 0 {
					c.committedAdmission(u)
				}
				c.queueConsumer(u)
				continue
			}
			if c.admission.delay != 0 {
				c.committedAdmission(u)
			}
			c.queueConsumer(u)
		}
	}
}

// ownsPosition distinguishes rule/channel owners sharing a bridge connection.
// An ended link retains its own owners through their final bookkeeping save.
func (u *consumerState) ownsPosition() bool {
	if u.rule != nil {
		return !u.rule.removed
	}
	return u.conn.consumer(u.channel.row.ID) == u
}
