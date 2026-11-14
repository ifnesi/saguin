package core

import (
	"container/heap"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/store"
)

// admissionState is core-owned policy, independent of the values exported to
// scrapers. The age index contains only contributing owners, and the peer
// index contains only connections with a running acknowledgement deadline.
// Owner events update these indexes; evaluating the gate visits their roots.
type admissionState struct {
	delay, peerTimeout time.Duration
	closedAt           time.Time
	gauge              *metrics.Gauge
	seconds            *metrics.DurationCounter
	ages, peers        deadlineQueue
	memory             deadlineQueue // progress waiting for payload memory, in memoryTime
	memoryWait         brokerWait    // shared peer-dependent intervals in the memory FIFO
	dirty              []*consumerState
}

// brokerWait accumulates only broker-dependent time. Both progress boundaries
// share eligibility, but retain their separate ages when the follower grows or
// is promoted. A paused boundary retains its accumulated wait.
type brokerWait struct {
	elapsed time.Duration
	since   time.Time
}

func (w *brokerWait) age(now time.Time) time.Duration {
	if w.since.IsZero() {
		return w.elapsed
	}
	return w.elapsed + now.Sub(w.since)
}

func (w *brokerWait) run(running bool, now time.Time) {
	if running && w.since.IsZero() {
		w.since = now
	} else if !running && !w.since.IsZero() {
		w.elapsed += now.Sub(w.since)
		w.since = time.Time{}
	}
}

type admissionBoundary struct {
	before int64
	wait   brokerWait
}

type consumerAdmission struct {
	sharedEligible bool // contributes to the group's qualified recipient count
	assignable     bool // contributes to the common cursor's ready capacity
	recipients     int  // common cursor: qualified member/channel owners
	ready          int  // common cursor: qualified owners able to accept an assignment
	returnedKey    store.SharedRecord
	returned       brokerWait // returned work can lie below both publication boundaries
	qualified      bool
	overdue        int // overdue obligations of this consumer, not its connection
	dirty          bool
	boundary       [2]admissionBoundary
	pendingAt      time.Time    // ready progress not yet covered by a write
	deadline       coreDeadline // independent ready bookkeeping
	progress       coreDeadline
	progressQueue  *deadlineQueue
	memory         bool // boundary ages currently use the shared memory clock
}

// coreDeadline gives all indexes the same standard indexed-heap machinery.
// Membership is embedded in its bounded owner and is removed without searches.
// The age index orders effective start times; the peer index orders deadlines.
type coreDeadline struct {
	bridge   *bridgeState
	at       time.Time
	index    int // one-based; zero means absent
	owner    *consumerState
	conn     *connectionState
	delivery *outgoing     // only a connection's peer-obligation index
	attempt  *queueAttempt // a queue hold, independent of its MQTT descriptor
	overdue  bool
}

type deadlineQueue struct{ items []*coreDeadline }

func (q *deadlineQueue) Len() int { return len(q.items) }
func (q *deadlineQueue) Less(i, j int) bool {
	a, b := q.items[i], q.items[j]
	if !a.at.Equal(b.at) {
		return a.at.Before(b.at)
	}
	if a.bridge != nil && b.bridge != nil {
		return a.bridge.options.Bridge.Name < b.bridge.options.Bridge.Name
	}
	if a.attempt != nil && b.attempt != nil {
		if a.attempt.queue.channel.row.ID != b.attempt.queue.channel.row.ID {
			return a.attempt.queue.channel.row.ID < b.attempt.queue.channel.row.ID
		}
		return a.attempt.offset < b.attempt.offset
	}
	if a.delivery != nil && b.delivery != nil {
		return a.delivery.order < b.delivery.order
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
	return a.owner != nil && b.owner != nil && a.owner.channel.row.ID < b.owner.channel.row.ID
}
func (q *deadlineQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	q.items[i].index, q.items[j].index = i+1, j+1
}
func (q *deadlineQueue) Push(v any) {
	d := v.(*coreDeadline)
	q.items = append(q.items, d)
	d.index = len(q.items)
}
func (q *deadlineQueue) Pop() any {
	i := len(q.items) - 1
	d := q.items[i]
	q.items[i] = nil
	q.items = q.items[:i]
	d.index = 0
	return d
}
func (q *deadlineQueue) reserve(n int) {
	if cap(q.items) < n {
		q.items = slices.Grow(q.items, n-len(q.items))
	}
}
func (q *deadlineQueue) first() time.Time {
	if q.Len() == 0 {
		return time.Time{}
	}
	return q.items[0].at
}
func (q *deadlineQueue) set(d *coreDeadline, at time.Time) {
	if at.Equal(d.at) && (at.IsZero() || d.index != 0) {
		return
	}
	d.at = at
	switch {
	case at.IsZero() && d.index != 0:
		heap.Remove(q, d.index-1)
	case !at.IsZero() && d.index == 0:
		heap.Push(q, d)
	case d.index != 0:
		heap.Fix(q, d.index-1)
	}
}

func (c *Core) touchAdmission(u *consumerState) {
	if c.admission.delay == 0 || u.admission.dirty {
		return
	}
	u.admission.dirty = true
	c.admission.dirty = append(c.admission.dirty, u)
}

// committedAdmission is called in the existing matching-consumer wake, once
// per matching record. A commit supplies one fixed boundary, even if several
// of its records or filters wake the same owner.
func (c *Core) committedAdmission(u *consumerState) {
	if c.admission.delay == 0 {
		return
	}
	a := &u.admission
	if !a.qualified {
		return
	}
	addAdmissionBoundary(&a.boundary, u.channel.committedTail, 0, c.admissionProgressReady(u), a.clock(c, c.eventAt))
}

// handoffPosition includes skipped records, but stops before the first
// selected delivery not yet handed over. Sent, unacknowledged deliveries do
// not hold an admission boundary open.
func (u *consumerState) handoffPosition() int64 {
	if u.sharedCursor != nil {
		position := u.sharedCursor.sharedHandoffPosition()
		if u.offer != nil {
			position = min(position, u.offer.offset)
		}
		return position
	}
	position := u.scan
	if u.offer != nil && !u.offer.initial {
		position = min(position, u.offer.offset)
	}
	return position
}

func (c *Core) admissionProgressReady(u *consumerState) bool {
	if u.conn == nil {
		return u.admission.ready != 0 && u.share.scanTurn(u.channel.row.ID)
	}
	if u.offer != nil {
		// Loaded work must obey the scheduler's prepared order. A reserved
		// body waiting on the store may count through the same turn, but
		// its slot alone cannot establish progress behind another owner.
		return c.canOffer(u) || c.canReservePayload(u) ||
			(u.offer.payloadSlot && c.offerTurn(u))
	}
	if u.sharedCursor != nil {
		return u.share.scanTurn(u.channel.row.ID) && c.sharedCapacity(u) > 0
	}
	// A scan also needs a descriptor and an identifier. Their release may
	// require an acknowledgement or a writer report, even with send room.
	return c.payloadRoom(u.conn) && c.preparationRoom(u) > 0
}

// admissionProgress advances the same two serviced/following boundaries for
// ordinary offsets and shared selections. A clock change preserves their age.
func admissionProgress(boundaries *[2]admissionBoundary, position, available int64, running, changed bool, old, at time.Time) time.Time {
	for boundaries[0].before != 0 && position >= min(boundaries[0].before, available) {
		boundaries[0], boundaries[1] = boundaries[1], admissionBoundary{}
	}
	var progress time.Time
	for i := range boundaries {
		b := &boundaries[i]
		if b.before == 0 {
			continue
		}
		if changed {
			b.wait = brokerWait{elapsed: b.wait.age(old)}
		}
		b.wait.run(running, at)
		if i == 0 && running {
			progress = at.Add(-b.wait.age(at))
		}
	}
	return progress
}

func addAdmissionBoundary(boundaries *[2]admissionBoundary, before int64, age time.Duration, running bool, at time.Time) {
	if before <= boundaries[0].before || before <= boundaries[1].before {
		return
	}
	b := &boundaries[0]
	if b.before != 0 {
		b = &boundaries[1]
	}
	if b.before == 0 {
		b.wait.elapsed = age
		b.wait.run(running, at)
	}
	b.before = before
}

func (c *Core) refreshAdmission(u *consumerState, now time.Time) {
	if u.conn == nil {
		c.refreshSharedProgress(u, now)
		return
	}
	a, s := &u.admission, u.conn
	if u.sharedCursor != nil {
		u.available = u.sharedCursor.available
	}
	eligible := c.consumerActive(s, u) && u.pass == nil && s.resuming == nil && a.overdue == 0
	if !eligible {
		a.disqualify()
	} else if !a.qualified && u.handoffPosition() >= u.available && u.offer == nil {
		a.qualified = true
	}
	var oldest, progress time.Time
	if a.qualified {
		position, available := u.handoffPosition(), u.available
		if u.sharedCursor != nil {
			// Shared boundaries name this member's bounded selections, including
			// returns below the channel position. Promotion never resets age.
			position, available = int64(s.selectionOrder)+1, int64(s.selectionOrder)+1
			if u.offer != nil {
				position = int64(u.offer.order)
			}
		}
		running := c.admissionProgressReady(u)
		old := a.clock(c, now)
		memory := u.offer != nil && u.offer.stage == deliveryWaiting
		changed := memory != a.memory
		a.memory = memory
		progress = admissionProgress(&a.boundary, position, available, running, changed, old, a.clock(c, now))
		if u.dirty && u.writing == nil && a.pendingAt.IsZero() {
			a.pendingAt = now
		}
		if u.dirty && u.writing == nil {
			oldest = firstTime(oldest, a.pendingAt)
		}
		if u.writing != nil {
			oldest = firstTime(oldest, u.writing.admissionAt)
		}
		if u.saving != nil {
			oldest = firstTime(oldest, u.saving.admissionAt)
		}
	}
	if u.sharedCursor != nil {
		c.refreshSharedRecipient(u, eligible, now)
	}
	c.admission.ages.set(&a.deadline, oldest)
	queue := &c.admission.ages
	if a.memory {
		queue = &c.admission.memory
	}
	if a.progressQueue != nil && a.progressQueue != queue {
		a.progressQueue.set(&a.progress, time.Time{})
	}
	a.progressQueue = queue
	queue.set(&a.progress, progress)
}

// refreshSharedRecipient changes only this member and its common cursor's
// counts. Catch-up waiters wake once the last unsent assignment is handed off.
func (c *Core) refreshSharedRecipient(u *consumerState, eligible bool, now time.Time) {
	a, cursor, g := &u.admission, u.sharedCursor, u.share
	if a.sharedEligible != a.qualified {
		change := -1
		if a.qualified {
			change = 1
		}
		g.qualified += change
		cursor.admission.recipients += change
		a.sharedEligible = a.qualified
		c.touchAdmission(cursor)
	}
	ready := a.qualified && c.sharedCapacity(u) > 0
	if a.assignable != ready {
		change := -1
		if ready {
			change = 1
		}
		cursor.admission.ready += change
		a.assignable = ready
		c.touchAdmission(cursor)
	}
	waiting := eligible && !a.qualified && u.offer == nil
	switch {
	case waiting && u.sharedCatchup == nil:
		u.sharedCatchup = cursor.sharedCatchingUp.PushBack(u)
	case !waiting && u.sharedCatchup != nil:
		cursor.sharedCatchingUp.Remove(u.sharedCatchup)
		u.sharedCatchup = nil
	}
	c.refreshSharedAdmission(g, now)
}

func (c *Core) refreshSharedProgress(u *consumerState, now time.Time) {
	a, g := &u.admission, u.share
	qualified := g.active && a.recipients != 0
	if !qualified {
		a.disqualify()
	} else {
		a.qualified = true
	}
	running := qualified && a.ready != 0 && g.scanTurn(u.channel.row.ID)
	at := admissionProgress(&a.boundary, u.scan, u.available, running, false, now, now)
	key := g.returned
	if !qualified || key.Channel != u.channel.row.ID {
		key = store.SharedRecord{}
	}
	if key != a.returnedKey {
		a.returnedKey, a.returned = key, brokerWait{}
	}
	a.returned.run(running && key.Channel != 0, now)
	if running && key.Channel != 0 {
		at = firstTime(at, now.Add(-a.returned.age(now)))
	}
	c.admission.ages.set(&a.progress, at)
	if u.sharedHandoffPosition() >= u.available {
		for e := u.sharedCatchingUp.Front(); e != nil; {
			next := e.Next()
			owner := e.Value.(*consumerState)
			u.sharedCatchingUp.Remove(e)
			owner.sharedCatchup = nil
			c.touchAdmission(owner)
			e = next
		}
	}
}

// selectedSharedAdmission transfers the common readiness age to the member's
// serviced/following selection boundaries, before the returned head moves on.
func (c *Core) selectedSharedAdmission(cursor *consumerState, d *outgoing, returned bool, now time.Time) {
	if c.admission.delay == 0 || !d.consumer.admission.qualified {
		return
	}
	common := &cursor.admission
	var age time.Duration
	if returned {
		age = common.returned.age(now)
	} else {
		b := &common.boundary[0]
		if d.offset >= b.before && common.boundary[1].before != 0 {
			b = &common.boundary[1]
		}
		age = b.wait.age(now)
	}
	u := d.consumer
	addAdmissionBoundary(&u.admission.boundary, int64(d.order)+1, age, c.admissionProgressReady(u), u.admission.clock(c, now))
}

// disqualify retires the live progress boundaries. A later catch-up can
// qualify the owner again, without inheriting delay from its earlier scan.
func (a *consumerAdmission) disqualify() {
	a.qualified = false
	a.boundary = [2]admissionBoundary{}
}

// memoryTime excludes intervals in which the oldest waiting delivery needs
// peer input or writer completion to obtain memory. All queued payloads share
// this clock and index, so a resource transition never walks waiting consumers.
func (c *Core) memoryTime(now time.Time) time.Time {
	return now.Add(-c.admission.memoryWait.age(now))
}

func (a *consumerAdmission) clock(c *Core, now time.Time) time.Time {
	if a.memory {
		return c.memoryTime(now)
	}
	return now
}

func (c *Core) admissionOldest() time.Time {
	a := &c.admission
	oldest := a.ages.first()
	if a.memoryWait.since.IsZero() {
		if at := a.memory.first(); !at.IsZero() {
			oldest = firstTime(oldest, at.Add(a.memoryWait.elapsed))
		}
	}
	return oldest
}

func firstTime(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

// evaluateAdmission runs after event results, including every transaction
// member, and before any incoming grant. Deadline expiry only wakes affected
// connections; changes to ordinary owners arrive on their existing wake path.
func (c *Core) evaluateAdmission(now time.Time) {
	a := &c.admission
	if a.delay == 0 {
		return
	}
	if c.phase >= quiescing {
		c.openAdmission(now)
		return
	}
	if len(a.dirty) == 0 && a.ages.Len() == 0 && a.memory.Len() == 0 && a.peers.Len() == 0 {
		// With no owner work or peer deadline, delay is zero. There is no
		// progress clock whose eligibility needs the memory FIFO's state.
		c.openAdmission(now)
		return
	}
	a.memoryWait.run(c.memoryNeedsPeer(), now)
	c.expirePeers(now)
	for offset := 0; offset < len(a.dirty); {
		dirty := a.dirty[offset:]
		offset = len(a.dirty)
		for _, u := range dirty {
			u.admission.dirty = false
			c.refreshAdmission(u, now)
		}
	}
	clear(a.dirty)
	a.dirty = a.dirty[:0]
	oldest := c.admissionOldest()
	if oldest.IsZero() || now.Sub(oldest) < a.delay/2 {
		c.openAdmission(now)
	} else if a.closedAt.IsZero() && now.Sub(oldest) >= a.delay {
		a.closedAt = now
		a.gauge.Set(1)
	}
}

func (c *Core) openAdmission(now time.Time) {
	a := &c.admission
	if a.delay == 0 {
		return
	}
	if !a.closedAt.IsZero() {
		a.seconds.Add(now.Sub(a.closedAt))
		a.closedAt = time.Time{}
		c.memoryReady = true
		a.gauge.Set(0)
	}
}

func (c *Core) admissionTimer() time.Time {
	a := &c.admission
	if a.delay == 0 {
		return time.Time{}
	}
	at := a.peers.first()
	if a.closedAt.IsZero() {
		if oldest := c.admissionOldest(); !oldest.IsZero() {
			at = firstTime(at, oldest.Add(a.delay))
		}
	}
	return at
}
