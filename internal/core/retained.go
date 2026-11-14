package core

import (
	"container/heap"
	"errors"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

// retainedState holds aggregate charges and one age/size retention pass.
// after is its offset continuation, starting at the floor on every pass.
// Topic metadata belongs only to active publication batches.
type retainedState struct {
	maximum, period, bytes, values, reserved int64
	charged                                  int64 // committed topic maxima, including held QoS 2 replacements
	next, changed, at, after                 int64
	recordTail, recordNext                   int64 // latest pass boundary and accumulated future/publication deadlines
	channel, sizes                           int
	age                                      bool
	fired, writing, reading                  bool
	full, reservedProperty                   *metrics.Counter
}

// publishCharges borrows an immutable batch while loading its active topics'
// charges. The channel owns this request until admission and commit settle.
type publishCharges struct {
	batch *publishBatch
	after int
}

func (p *publishCharges) ReadConn(conn *store.Conn) error {
	end := min(p.after+store.MetadataPage, len(p.batch.charges))
	for p.after < end {
		if err := p.batch.charges[p.after].ReadConn(conn); err != nil {
			return err
		}
		p.after++
	}
	return nil
}

// admitPublishes follows packet policy order. Within a batch each topic is
// charged at its largest committed or accepted pending value; a clear cannot
// release the preceding charge before the batch's transaction commits.
func (c *Core) admitPublishes(acts []Action, b *publishBatch, now time.Time) []Action {
	ch := b.owner
	admitted := b.publications[:0]
	for _, p := range b.publications {
		if p.incoming != nil && p.record.Offset != 0 {
			admitted = append(admitted, p)
			continue // its channel and retained capacity were reserved at PUBLISH
		}
		var reason byte
		if p.dropped != nil {
			reason = 0x87
		}
		var increase int64
		var charge *store.RetainedCharge
		if reason == 0 && c.retained.maximum != 0 && p.record.PublishedRetained {
			charge = &b.charges[b.topics[p.record.Topic]]
			if p.record.Retained {
				increase = max(0, p.record.Bytes()-charge.Bytes)
			}
			if increase > c.retained.maximum-c.retained.charged-c.retained.reserved {
				reason = 0x97
				p.dropped = c.willsRetainedFull
				if p.will == nil {
					c.retained.full.Inc()
				}
			}
		}
		if reason == 0 && ch.maximum != 0 && p.record.Bytes() > ch.maximum-ch.row.Bytes-ch.row.HeldBytes-ch.reserved {
			reason = 0x97
			p.dropped = c.willsChannelFull
			if p.will == nil {
				c.channelFull.Inc()
			}
		}
		if reason == 0 && p.reserved {
			reason = 0x83
			c.retained.reservedProperty.Inc()
		}
		if reason != 0 {
			b.count--
			b.bytes -= p.bytes
			b.messageBytes -= p.record.Bytes()
			if p.incoming == nil || p.record.Offset != 0 {
				b.channel.Bytes -= p.record.Bytes()
			} else {
				b.holds--
			}
			acts = c.finishPublication(acts, p, reason, now)
			if p.will != nil {
				p.bytes, p.record.Payload, p.record.StoredPayload, p.record.Properties = 0, nil, nil, nil
				b.dropped = append(b.dropped, p)
			}
		} else {
			admitted = append(admitted, p)
			if ch.maximum != 0 {
				size := p.record.Bytes()
				ch.reserved += size
				b.channelReservation += size
			}
			if charge != nil {
				charge.Bytes += increase
				b.reservation += increase
				c.retained.reserved += increase
			}
		}
	}
	clear(b.publications[len(admitted):])
	b.publications = admitted
	b.prepared = true
	return acts
}

func retainedDeadline(r *store.Record, period int64) int64 {
	at := r.Expires.Int64
	if period != 0 {
		at = earlier(at, timestamp.AfterMilli(r.Time, period))
	}
	return at
}

// noteRetention preserves committed deadlines through paged walks and
// retained-value reads. Between store reads the timer only comes earlier.
func (c *Core) noteRetention(ch *channelState, at int64) {
	ch.retentionAt = earlier(ch.retentionAt, at)
	if c.retained.recordTail != 0 && c.channels[c.retained.channel] == ch {
		c.retained.recordNext = earlier(c.retained.recordNext, at)
	}
	c.retained.next = earlier(c.retained.next, at)
	if c.retained.at != 0 {
		c.retained.changed = earlier(c.retained.changed, at)
	}
}

// retainAge visits only due channels, in stored id order. Size changes join
// the same pass and use the same continuation.
func (c *Core) retainAge(now time.Time) {
	e := &c.retained
	if e.writing || e.reading || (e.at == 0 && !e.fired && e.sizes == 0) {
		return
	}
	if e.at == 0 {
		e.age, e.fired = e.fired || c.phase == starting, false
		e.at = now.UnixMilli()
		if e.at == 0 {
			return
		}
	}
	for e.channel < len(c.channels) {
		ch := c.channels[e.channel]
		age := ch.retentionAt != 0 && ch.retentionAt <= e.at && !ch.retentionRetry
		if !age && !ch.retainSize {
			e.channel++
			continue
		}
		e.age = e.age || age
		if ch.retainSize {
			ch.retainSize = false
			e.sizes--
		}
		if !e.age && ch.row.Type != store.Latest && (ch.retentionBytes == 0 || ch.row.Bytes-ch.queueHeldBytes() <= ch.retentionBytes) {
			// The scheduled size pass has nothing to remove. Its committed
			// charge is already known; no record read, consumer snapshot or
			// retention write is needed to establish that it fits.
			e.channel++
			continue
		}
		if e.after == 0 {
			e.after = ch.row.Floor - 1
		}
		if ch.row.Type == store.Latest && e.recordTail == 0 {
			e.recordTail = ch.committedTail
		}
		e.writing = true
		c.dueGroup(commitEvent)
		op := &store.Retention{Channel: ch.row.ID, At: e.at, Period: e.period, After: e.after,
			Tail: e.recordTail, LogPeriod: ch.period, TombstonePeriod: ch.tombstonePeriod, Latest: ch.row.Type == store.Latest, Limit: ch.retentionBytes, Expire: e.age}
		r := &storeRequest{kind: expireRetained, channel: ch, otherChannel: ch.deadLetter, op: op}
		if ch.deadLetter != nil {
			op.DeadLetters = &store.DeadLetters{Source: ch.row.Name}
			r.kind, r.op = readRetentionPage, &store.ReadRetention{Retention: op}
		}
		c.enqueue(r)
		return
	}
	if !e.age || e.values == 0 {
		// Size work never changes retained flags or their deadlines. Keep
		// the known deadline instead of reading the retained catalogue on
		// each commit. An age pass discovers its remaining deadlines anew.
		var next int64
		if !e.age {
			next = e.next
		}
		if e.fired {
			// An age deadline reached during this pass still needs service.
			next = earlier(next, e.at)
		}
		c.finishRetention(next, now)
		if e.fired {
			// A deadline may have passed while startup waited for the last
			// write. Dispatch its walk before the loop can report Ready.
			c.retainAge(now)
		}
		return
	}
	e.reading = true
	op := &store.RetentionDeadline{At: e.at, Period: e.period}
	if c.broadcast.retentionRetry {
		op.Refused = map[int64]bool{1: true}
	}
	c.enqueue(&storeRequest{kind: readRetentionDeadline, op: op})
}

// retentionRecords freezes the bounds of a record walk. Queue pages are read
// under the same reservation later used by their retention write.
func (c *Core) retentionRecords(op *store.Retention, ch *channelState) {
	op.TrackCharges = c.retained.maximum != 0
	op.Floor, op.Size = ch.row.Floor, ch.row.Bytes-ch.queueHeldBytes()
	op.Held = op.Held[:0]
	if q := ch.queue; q != nil && op.Job == nil {
		for offset, a := range q.attempts {
			if a.held {
				op.Held = append(op.Held, offset)
			}
		}
		slices.Sort(op.Held)
	}
	if !op.Latest && op.Job == nil {
		op.Tail = ch.committedTail
	}
}

func (c *Core) readRetentionPage(acts []Action, r *storeRequest, now time.Time) []Action {
	op := r.op.(*store.ReadRetention).Retention
	r.kind, r.op = expireRetained, op
	if op.Job != nil {
		r.kind = saveQueueChange
	}
	if c.failure != nil || (op.Job == nil && c.phase >= quiescing) {
		if op.Job != nil {
			acts = c.savedQueueChange(acts, r, c.failure, now)
			c.releaseChannel(r)
			if r.lane != nil {
				c.endLane(r.lane, r)
			}
			return acts
		}
		c.releaseRetentionPage(r)
		c.releaseChannel(r)
		c.retained.writing = false
		r.channel.retaining = false
		c.releaseSharedRequest(r)
		return acts
	}
	c.continueRequest(r)
	return acts
}

func (c *Core) releaseRetentionPage(r *storeRequest) {
	r.op.(*store.Retention).ReleasePage()
	c.returnMetadataCredit()
}

// enterDeadLetters applies the shared retention selection, then gives each
// encodable job its entry offset in the core, before any move is dispatched.
func (c *Core) enterDeadLetters(r *storeRequest, now time.Time) error {
	op := r.op.(*store.Retention)
	dest, moved := r.otherChannel, op.DeadLetters
	moved.At = now.UnixMilli()
	op.Select()
	for i := range moved.Moves {
		m := &moved.Moves[i]
		if err := m.Prepare(moved.Source, moved.At); err != nil {
			if errors.Is(err, packet.ErrCannotEncode) {
				continue // deletion and its too-large accounting still await commit
			}
			return err
		}
		m.Offset = dest.enter()
	}
	moved.Channel = store.AdvanceChannel{ID: dest.row.ID, Next: dest.row.Next}
	return nil
}

// retentionBoundary names the core's connected owners only when the write
// joins its ordered group. Later events cannot mutate this snapshot.
func (c *Core) retentionBoundary(r *storeRequest) {
	op, ch := r.op.(*store.Retention), r.channel
	c.retentionRecords(op, ch)
	var bridgeRules map[int64]struct{}
	for _, u := range ch.consumers {
		s := u.conn
		if s == nil {
			if !u.indexed || !u.share.active || u.share.queue != nil {
				continue
			}
			owner := store.RetentionConsumer{Share: u.share.id, Scan: u.scan,
				Filters: []store.Subscription{{Filter: u.share.filter, MatchFilter: u.share.part, Share: u.share.id, ReadAccess: acl.Full}}}
			for key, d := range u.share.assigned {
				if key.Channel != ch.row.ID {
					continue
				}
				if d.attempted || d.settled {
					owner.Excluded = append(owner.Excluded, key.Offset)
				} else {
					owner.Pending = append(owner.Pending, key.Offset)
				}
			}
			op.Consumers = append(op.Consumers, owner)
			r.retentionConsumers = append(r.retentionConsumers, retentionConsumer{owner: u, position: u.position})
			continue
		}
		if !u.indexed || s.phase != connected || s.end.seen {
			continue
		}
		if op.Named == nil {
			op.Named = make(map[int64]struct{}, len(ch.consumers))
			op.Consumers = slices.Grow(op.Consumers, len(ch.consumers))
			r.retentionConsumers = slices.Grow(r.retentionConsumers, len(ch.consumers))
		}
		op.Named[s.session.ID] = struct{}{}
		owner := store.RetentionConsumer{Session: s.session.ID, Client: s.clientID,
			Scan: u.scan, Filters: u.filters, Recovering: s.resuming != nil, MarkersBefore: u.markersBefore, Access: s.access}
		if u.rule != nil {
			owner.Rule = u.rule.row.ID
			if bridgeRules == nil {
				bridgeRules = make(map[int64]struct{})
			}
			bridgeRules[owner.Rule] = struct{}{}
		}
		if ch.row.ID != 1 {
			owner.Subject = ch.row.Name
		}
		for d := u.last; d != nil; d = d.prev {
			if d.initial {
				continue
			}
			if d.attempted || d.settled {
				owner.Excluded = append(owner.Excluded, d.offset)
			} else {
				owner.Pending = append(owner.Pending, d.offset)
			}
		}
		for _, end := range u.settled {
			owner.Excluded = append(owner.Excluded, end.offset)
		}
		op.Consumers = append(op.Consumers, owner)
		r.retentionConsumers = append(r.retentionConsumers, retentionConsumer{owner: u, bridge: s.bridge, position: u.position})
	}
	for _, b := range c.bridges {
		if !b.registered {
			continue
		}
		for i := range b.rules {
			rule := &b.rules[i]
			if rule.row == nil {
				continue
			}
			if _, named := bridgeRules[rule.row.ID]; named {
				continue
			}
			if !slices.ContainsFunc(rule.row.Cursors, func(p store.ChannelPosition) bool { return p.Channel == ch.row.ID }) {
				continue
			}
			owner := store.RetentionConsumer{Rule: rule.row.ID, Recovering: true}
			for _, filter := range rule.config.Filters {
				owner.Filters = append(owner.Filters, store.Subscription{Filter: filter, ReadAccess: acl.Full, Starts: []store.SubscriptionStart{{Channel: ch.row.ID, Start: 1}}})
			}
			op.Consumers = append(op.Consumers, owner)
			r.retentionConsumers = append(r.retentionConsumers, retentionConsumer{bridge: b})
		}
	}
	ch.retaining = true
}

func (c *Core) expiredRetained(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	c.retentionReady = true
	e, ch := &c.retained, r.channel
	e.writing, ch.retaining = false, false
	op := r.op.(*store.Retention)
	if moved := op.DeadLetters; moved != nil {
		c.resolvePublications(ch.deadLetter, moved.Channel.Next)
	}
	if err == nil {
		ch.retentionRetry = false
		if op.Latest {
			e.recordNext = earlier(e.recordNext, op.Next)
			ch.retentionAt = e.recordNext
		} else {
			ch.retentionAt, ch.retentionLogAt = op.Next, op.Next
		}
		acts = c.applyRetention(acts, r, now)
		if op.More {
			e.after = op.Last
			c.scheduleRetention(ch)
			return acts
		}
	} else if errors.Is(err, store.ErrFull) {
		// The failed body is discarded. The retry rediscovers both age and
		// size work from the store, without postponing an earlier retry.
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
		ch.retentionRetry = true
		c.advanceFloor(ch, ch.row.Floor, now)
	}
	e.channel++
	e.after, e.recordTail, e.recordNext = 0, 0, 0
	return acts
}

// advanceFloor cancels indexed readers' preparations below the new floor.
// Sparse deletions use invalidateRecord, including queue worker preparations.
// Reader/writer buffers keep their existing ownership rules.
func (c *Core) advanceFloor(ch *channelState, previous int64, now time.Time) {
	for _, u := range ch.consumers {
		s := u.conn
		if s == nil {
			if ch.row.Floor != previous {
				u.scan, u.position = max(u.scan, ch.row.Floor), max(u.position, ch.row.Floor)
				if u.share.queue == nil {
					u.share.invalidateReturned()
					u.share.dirty = true
				}
			}
			c.queueShared(u.share)
			continue
		}
		if s.phase != connected || s.end.seen {
			continue
		}
		if !u.indexed || ch.row.Floor == previous {
			// Detached exchanges finish without reviving a removed position.
			if c.canOffer(u) {
				c.queueConsumer(u)
			}
			continue
		}
		changed := false
		for d := u.last; d != nil; d = d.prev {
			if !d.initial && !d.attempted && d.offset < ch.row.Floor && !d.removed {
				c.removePreparation(d)
				changed = true
			}
		}
		if u.scan < ch.row.Floor {
			u.scan, changed = ch.row.Floor, true
		}
		if u.position < ch.row.Floor {
			u.position, u.seen, u.dirty = ch.row.Floor, now.UnixMilli(), s.stored || u.markersBefore != 0
			changed = true
		}
		switch {
		case changed:
			c.queueConsumer(u)
			c.queueConsumer(s.nextScan())
		case c.canOffer(u):
			// The write's retaining mark alone may have held this offer.
			c.queueConsumer(u)
		}
	}
}

func (c *Core) readRetentionDeadline(r *storeRequest, now time.Time) {
	next := r.op.(*store.RetentionDeadline).Next.Int64
	c.broadcast.retentionAt = earlier(c.broadcast.retentionAt, next)
	c.finishRetention(next, now)
}

func (c *Core) finishRetention(next int64, now time.Time) {
	defer func() {
		if c.retained.sizes != 0 || c.retained.fired {
			c.retentionReady = true
		}
	}()
	e := &c.retained
	e.next = earlier(next, e.changed)
	for _, ch := range c.channels {
		// A refused channel's overdue work belongs to the full-store
		// retry, rather than an immediate age-timer loop.
		if !ch.retentionRetry || ch.retentionAt > e.at {
			e.next = earlier(e.next, ch.retentionAt)
		}
	}
	e.at, e.changed, e.channel, e.reading, e.fired = 0, 0, 0, false, false
	if c.phase == starting && e.next != 0 && e.next <= now.UnixMilli() {
		e.next, e.fired = 0, true // finish overdue startup work before Ready
	}
}

// scheduleRetention coalesces size changes and continuation work into one
// bounded mark per channel. The ordinary dispatch path checks only the count.
func (c *Core) scheduleRetention(ch *channelState) {
	if !ch.retainSize {
		ch.retainSize = true
		c.retained.sizes++
		c.retentionReady = true
	}
}

// indexPreparation covers only payloads that can race a committed change.
// Metadata-only selections discover replacement through the existing missing
// payload path. Each entry is a reserved delivery within the fixed window.
func (d *outgoing) indexPreparation() {
	ch := d.consumer.channel
	if ch.row.Type != store.Latest && ch.row.Type != store.Queue {
		return
	}
	if ch.preparations == nil {
		ch.preparations = make(map[int64][]*outgoing)
	}
	entries := ch.preparations[d.offset]
	entries = append(entries, d)
	ch.preparations[d.offset], d.preparationIndex = entries, len(entries)
}

// unindexPreparation returns its slice slot by exchanging it with the last
// entry. No recipient scan or per-entry map allocation is needed.
func (d *outgoing) unindexPreparation() {
	if d.preparationIndex == 0 {
		return
	}
	ch := d.consumer.channel
	entries := ch.preparations[d.offset]
	i, last := d.preparationIndex-1, len(entries)-1
	entries[i] = entries[last]
	entries[i].preparationIndex = i + 1
	entries[last] = nil
	d.preparationIndex = 0
	if last == 0 {
		delete(ch.preparations, d.offset)
	} else {
		ch.preparations[d.offset] = entries[:last]
	}
}

// removePreparation marks work without settling it or issuing store requests.
// Floor changes, replacements and ownership returns share this path. The
// owner queues settle the ready removals in complete deterministic order.
func (c *Core) removePreparation(d *outgoing) {
	d.removed = true
	if !d.saving && d.stage != deliveryLoading && d.boundary[3] == 0 {
		u := d.consumer
		u.removals.kind = 3
		heap.Push(&u.removals, d)
		c.queueConsumer(u)
	}
}

// invalidateRecord marks exactly the payloads a committed change removed.
// The slice is an index, not an action order; owner scheduling determines it.
func (c *Core) invalidateRecord(ch *channelState, offset int64) {
	for _, d := range ch.preparations[offset] {
		c.removePreparation(d)
	}
}

// applyRetention applies the single committed deletion/move path, whether
// selected by retention or by an exhausted attempt. Losses, preparations,
// channel charges and dead-letter observations all share this boundary.
func (c *Core) applyRetention(acts []Action, r *storeRequest, now time.Time) []Action {
	e, ch := &c.retained, r.channel
	op := r.op.(*store.Retention)
	e.bytes -= op.Bytes
	e.charged -= op.ChargeReleased
	e.values -= int64(op.Count)
	ch.row.Bytes += op.Delta
	if c.phase == starting {
		ch.bytes.Set(ch.row.Bytes)
	}
	previousFloor := ch.row.Floor
	ch.row.Floor = op.Floor
	if ch.floor != nil {
		ch.floor.Set(op.Floor)
	}
	ch.removed.Add(uint64(op.Removed))
	ch.records.Add(-int64(op.Removed))
	ch.retentionMissed.Add(op.Missed)
	c.recordRetentionLosses(r, now)
	for _, offset := range op.Deleted {
		c.invalidateRecord(ch, offset)
	}
	if moved := op.DeadLetters; moved != nil {
		c.committedChannel(ch.deadLetter, &moved.Channel, int(moved.Expired+moved.Evicted+moved.Exhausted), 0)
		waiting := false
		for i := range moved.Moves {
			m := &moved.Moves[i]
			c.invalidateRecord(ch, m.Source)
			if q := ch.queue; q != nil {
				a := q.attempts[m.Source]
				if a == nil && m.Waiting {
					q.backoff--
					waiting = true
				} else if a == nil || !a.sent {
					q.ready--
				}
				if a != nil {
					c.endAttempt(a)
				}
			}
			if m.Offset == 0 {
				ch.missed.Inc()
				acts = c.logDeliveryDiscard(acts, nil, ch.row.Name, m.Source, now)
			} else {
				c.committedRecord(ch.deadLetter, m.Topic, m.Offset, moved.At)
			}
		}
		if ch.queue != nil {
			q := ch.queue
			if waiting {
				c.resetQueueRetry(q, moved.RetryAt)
			}
			q.updateJobs()
		}
		ch.deadLettered[0].Add(moved.Expired)
		ch.deadLettered[1].Add(moved.Evicted)
		ch.deadLettered[2].Add(moved.Exhausted)
	}
	c.advanceFloor(ch, previousFloor, now)
	return acts
}
