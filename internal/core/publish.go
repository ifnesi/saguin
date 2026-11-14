package core

import (
	"container/list"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

// channelState keeps one request and the last pending request for the channel
// (R2). Pending bodies remain in transit, charged to max_incoming_memory; neither
// a store request nor its batched bodies is changed between send and answer.
type channelState struct {
	preparations                            map[int64][]*outgoing // latest payloads loading or loaded but not handed off
	metadataWait                            consumerQueue
	requestOrder                            list.List
	waitRequests                            list.List
	row                                     store.Channel
	request, pending                        *storeRequest
	batches                                 int   // accumulating, sealed and outstanding publication batches
	maximum, reserved                       int64 // hard channel limit and accepted, uncommitted record charges
	stored                                  *metrics.Counter
	missed                                  *metrics.Counter
	period, tombstonePeriod, retentionBytes int64
	retentionAt, retentionLogAt             int64 // earliest applicable age deadline, and first unpassed log deadline
	retentionRetry, retainSize, retaining   bool
	floor, bytes                            *metrics.Gauge
	records, tail                           *metrics.Gauge
	removed, retentionMissed                *metrics.Counter
	startTail                               bool
	committedTail                           int64                                             // assigned offsets become readable only after commit
	resolved                                int64                                             // every offset below is stored or refused
	passes                                  []*consumerState                                  // initial passes waiting for resolved to reach their boundary
	filters                                 map[string]map[string]map[*consumerState]struct{} // literal prefix, complete filter, consumers
	consumers                               []*consumerState                                  // subscriptions and detached exchange owners, in connection-key order
	pages                                   []*storeRequest                                   // shared metadata reads, bounded by reader credit
	loading                                 *storeRequest                                     // payload batch still collecting before dispatch
	queue                                   *queueState
	deadLetter                              *channelState       // queue's broker-owned append destination
	deadLettered                            [3]*metrics.Counter // expired, evicted, exhausted; applied after commit
}

// publication retains the record and its reply or Will's original owner together.
// A reader's Ended returns only its unread grant, never this body's charge.
type publication struct {
	publishIdentity
	bridgeRecovery bool  // session-loss release has no peer packet or acknowledgement slot
	replacedOffset int64 // previous latest record, returned only by a committed replacement
	record         store.Record
	channel        *channelState
	conn           *connectionState
	quota          uint32 // reader generation carried through the terminal reply
	packetID       uint16
	request        bool // a recognized request refused before body admission
	requestKind    serviceKind
	refusal        byte             // reader policy, applied only after checking a QoS 2 identifier
	refused        *metrics.Counter // the reader's selected refusal series
	bytes          int64            // body's reservation, held until storage or discard
	hasID          bool             // the publisher supplied saguin-id, including an empty value
	reserved       bool             // a reserved property, checked after retained capacity
	batchCredit    bool             // a CONNECT-held body keeps its potential batch slot until admission
	pendingReply   *list.Element
	dropped        *metrics.Counter // Will admission refusal, counted only after its deletion commits
	will           *storeRequest    // a firing Will's reserved session lane and departure write
}

// publishBatch is an indivisible store write. Counts are maintained as
// bodies enter, without another pass over records or their reservations.
type publishBatch struct {
	capacity           batchCapacity // pending lists acquire a slot in store-request order
	floor              int64         // captured at admission; the writer never reads the channel owner
	replaced           int           // values updated in place, not retention losses
	removed            int           // cleared retained values deleted below the floor in this transaction
	publications       []*publication
	initial            [1]*publication // the usual single-record batch needs no separate allocation
	count              int
	bytes              int64 // body reservations of publications still in the batch
	messageBytes       int64 // topic, original payload and stored properties, independently of reservations
	owner              *channelState
	channel            store.AdvanceChannel
	channelReservation int64
	charges            []store.RetainedCharge // active topics only, never values at rest
	topics             map[string]int
	prepared           bool
	queue              bool // records and unfinished jobs enter together
	reservation        int64
	valueBytes         int64 // changes made by the writer, applied only after commit
	trackCharges       bool  // immutable configuration, copied before writer handover
	chargeDelta        int64 // retained topic maxima, including stored holds
	heldDelta          int64 // stored incoming reservations changed by this batch
	holds              int   // publications saved without entering the channel
	values             int64
	dropped            []*publication // refused Wills still remove their stored rows in this transaction
	wills              int            // departure writes accompanying channel publications
}

// append gives the publication its offset as it enters the channel, in
// entry order. A publication refused afterwards leaves its offset unused;
// the batch's channel row still stores the counter past it.
func (b *publishBatch) append(ch *channelState, p *publication) {
	holding := p.record.HeldPacket != 0 && p.record.Offset == 0 && p.incoming != nil
	if holding {
		b.holds++
	} else if p.record.Offset == 0 {
		p.enter()
	}
	if b.publications == nil {
		b.publications = b.initial[:0]
	}
	b.publications = append(b.publications, p)
	if len(b.publications) == 2 {
		clear(b.initial[:]) // append has moved membership to a larger backing array
	}
	b.count++
	if p.will != nil {
		b.wills += p.will.willChanges
	}
	b.bytes += p.bytes
	b.messageBytes += p.record.Bytes()
	b.owner = ch
	b.queue = ch.row.Type == store.Queue
	b.channel.ID = ch.row.ID
	if !holding {
		b.channel.Next = p.record.Offset + 1
		b.channel.Bytes += p.record.Bytes()
	}
}

// enter assigns the publication's offset when the core takes its entry
// packet. A held publication uses PUBREL, before its metadata read finishes.
func (p *publication) enter() {
	p.record.Offset = p.channel.enter()
}

// enter assigns an offset before dispatch, including broker-originated moves.
// A later refusal leaves the offset unused, as for any channel publication.
func (ch *channelState) enter() int64 {
	offset := ch.row.Next
	ch.row.Next++
	ch.tail.Set(ch.row.Next)
	return offset
}

// records yields the batch's surviving publications with the offsets they
// took on entering the channel.
func (b *publishBatch) records(yield func(*publication, int64) bool) {
	for _, p := range b.publications {
		if !yield(p, p.record.Offset) {
			return
		}
	}
}

// resolvePublications records that every publication below next is stored
// or refused, and wakes the initial passes whose boundary it has reached.
func (c *Core) resolvePublications(ch *channelState, next int64) {
	if next <= ch.resolved {
		return // a hold settles no channel entry
	}
	ch.resolved = next
	waiting := ch.passes[:0]
	for _, u := range ch.passes {
		switch {
		case u.pass == nil:
		case u.pass.before > next:
			waiting = append(waiting, u)
		default:
			c.queueConsumer(u)
		}
	}
	clear(ch.passes[len(waiting):])
	ch.passes = waiting
}

// Intake gates only the shared event channel. Publication backpressure stops
// new body grants, never PUBACKs, delivery completions or reader ends.
func (c *Core) Intake() bool {
	return c.Credit() > 0
}

// Batches counts unresolved batch slots, including each granted body's
// potential batch until it joins an actual batch or is refused or abandoned.
func (c *Core) Batches() int { return c.batchCount + c.bodyCount }

func (c *Core) publicationRoom() bool {
	if c.pendingBatches != 0 || c.Batches() >= publicationBatches {
		return false
	}
	// A body has no topic yet, so its potential slot must fit whichever
	// channel decoding selects. Four fixed buckets avoid a channel scan.
	for n := channelBatches; n > 0; n-- {
		if c.batchLevels[n] != 0 {
			return n+c.bodyCount < channelBatches
		}
	}
	return c.bodyCount < channelBatches
}

func (c *Core) reserveBodyBatch() {
	c.bodyCount++
}

func (c *Core) releaseBodyBatch(s *connectionState) {
	if s.readerBatch {
		s.readerBatch = false
		c.bodyCount--
		c.batchCapacityChanged()
	}
}

type batchCapacity uint8

const (
	batchUnqueued batchCapacity = iota
	batchPending
	batchAdmitted
)

// queueBatch registers pending publication work, including a held release.
// Completed bodies return their potential slots before joining this common
// admission. Later bodies cannot consume a slot needed by an earlier release.
func (c *Core) queueBatch(b *publishBatch, ch *channelState) {
	if b.capacity != batchUnqueued {
		return
	}
	b.owner, b.capacity = ch, batchPending
	c.pendingBatches++
}

// admitBatch runs in store-request order before later pending publications.
// A pending list carries message and reply reservations but is not a batch
// until it acquires capacity. The same slot covers a release's metadata read
// and its eventual move, without any further capacity acquisition.
func (c *Core) admitBatch(b *publishBatch) bool {
	if b.capacity == batchAdmitted {
		return true
	}
	ch := b.owner
	if c.Batches() >= publicationBatches || ch.batches+c.bodyCount >= channelBatches {
		return false
	}
	b.capacity = batchAdmitted
	c.pendingBatches--
	c.memoryReady = true
	c.batchLevels[ch.batches]--
	ch.batches++
	c.batchLevels[ch.batches]++
	c.batchCount++
	return true
}

func (c *Core) releaseBatch(b *publishBatch) {
	switch b.capacity {
	case batchPending:
		c.pendingBatches--
	case batchAdmitted:
		ch := b.owner
		c.batchLevels[ch.batches]--
		ch.batches--
		c.batchLevels[ch.batches]++
		c.batchCount--
	}
	b.capacity = batchUnqueued
	c.batchCapacityChanged()
}

func (c *Core) releasePublicationCredit(p *publication) {
	if p.batchCredit {
		p.batchCredit = false
		c.bodyCount--
		c.batchCapacityChanged()
	}
}

func (c *Core) batchRoom(b *publishBatch, p *publication) bool {
	return b.count < c.commitMaximum && p.record.Bytes() <= publicationBytes-b.messageBytes &&
		(p.will == nil || p.will.willChanges <= groupChanges-b.wills)
}

// Records counts messages for the core's group limit and commit metrics.
func (b *publishBatch) Records() int { return b.count }

// WriteTx keeps every row and the channel's next and bytes in one transaction.
func (b *publishBatch) WriteTx(tx *store.Tx) error {
	var publicationErr error
	replaced, err := tx.WriteRecords(func(yield func(*store.Record, bool) bool) {
		for i, p := range b.publications {
			holding := p.record.HeldPacket != 0 && p.record.Offset == 0
			charge := store.RetainedCharge{Topic: p.record.Topic}
			charged := b.trackCharges && p.record.PublishedRetained
			if charged {
				if err := charge.ReadTx(tx); err != nil {
					publicationErr = err
					return
				}
			}
			before := charge.Bytes
			if p.record.Retained && p.record.Channel != 1 && !holding {
				charge.Channel = p.record.Channel
				if err := charge.ReadTx(tx); err != nil {
					publicationErr = err
					return
				}
				b.channel.Bytes -= charge.Bytes
				p.replacedOffset = charge.Offset

			}
			if p.record.HeldPacket != 0 {
				delta := p.record.Bytes()
				if !holding {
					delta = -delta
				}
				b.heldDelta += delta
			}
			if p.record.PublishedRetained && !holding {
				offset, old, err := p.record.ClearRetained(tx, b.floor)
				if err != nil {
					publicationErr = err
					return
				}
				b.valueBytes -= old
				if old != 0 {
					b.values--
				}
				if offset != 0 && offset < b.floor {
					b.removed++
				} else {
					b.channel.Bytes += old
				}
				if p.record.Retained {
					size := p.record.Bytes()
					b.channel.Bytes -= size
					b.valueBytes += size
					b.values++
				}
			}
			// Flush before a Will's departure or the next retained replacement
			// changes rows, so each sees the preceding inserts in this batch.
			if !yield(&p.record, charged || p.will != nil || i+1 == len(b.publications) || b.publications[i+1].record.PublishedRetained) {
				return
			}
			if charged {
				if err := charge.ReadTx(tx); err != nil {
					publicationErr = err
					return
				}
				b.chargeDelta += charge.Bytes - before
			}
			if p.will != nil {
				if err := p.will.op.(store.Write).WriteTx(tx); err != nil {
					publicationErr = err
					return
				}
			}
		}
	}, b.queue)
	if err != nil {
		return err
	}
	if publicationErr != nil {
		return publicationErr
	}
	b.replaced = replaced
	for _, p := range b.dropped {
		if err := p.will.op.(store.Write).WriteTx(tx); err != nil {
			return err
		}
	}
	return b.channel.WriteTx(tx)
}

func (c *Core) publish(acts []Action, ev connection.Event, p *packet.Publish, now time.Time) []Action {
	charge := c.takeBodyBytes(ev, now)
	s := c.connections[ev.Conn]
	if s.phase >= refused || s.end.seen {
		// A connection already closed never creates a new record.
		c.returnBytes(charge, now)
		return acts
	}
	identity, reason := c.classifyPublish(s, p.PacketID, p.QoS, ev.Quota)
	if reason != 0 {
		c.returnBytes(charge, now)
		return c.finish(acts, s, reason, now)
	}
	if identity.repeat != nil {
		c.returnBytes(charge, now)
		return c.repeatIncoming(acts, s, p.PacketID, identity.repeat, nil)
	}
	if request, ok := ev.Packet.(*connection.RequestPublish); ok {
		return c.requestPublish(acts, ev, request, charge, identity, now)
	}
	var name string
	if routed, ok := ev.Packet.(*connection.RoutedPublish); ok {
		name = routed.Topic
	} else {
		name = string(p.Topic)
	}
	ch := c.route(name)
	r := store.Record{Channel: ch.row.ID, Time: now.UnixMilli(), Topic: name,
		QoS: p.QoS, Payload: p.Payload, StoredPayload: p.StoredPayload, PayloadSize: max(len(p.Payload), p.PayloadSize),
		Properties: p.StoredProperties, PublishedRetained: p.Retain && ch == &c.broadcast}
	r.ID = ev.Publish.ID
	if ev.Publish.Expires {
		r.Expires.Int64, r.Expires.Valid = expiryAt(ev.Publish.Received, ev.Publish.Expiry), true
	}
	r.Retained = ch.row.Type == store.Latest || (r.PublishedRetained && r.PayloadSize != 0)
	pub := &publication{record: r, channel: ch, conn: s, packetID: p.PacketID, quota: ev.Quota, bytes: charge,
		reserved: ev.Publish.Reserved, hasID: ev.Publish.HasID, publishIdentity: identity}
	if p.QoS == 2 {
		pub.record.HeldPacket, pub.record.HeldRetain = p.PacketID, p.Retain
	}
	return c.queuePublication(acts, pub, now)
}

// queuePublication reserves completion once, then orders a CONNECT-held
// publication with the session's other pipelined packets. Identifier-bearing
// packets also follow earlier subscription/request decisions, which may hold
// their identifier. Refusals keep only bounded reply metadata.
func (c *Core) queuePublication(acts []Action, p *publication, now time.Time) []Action {
	s := p.conn
	if p.record.QoS != 0 {
		p.pendingReply = s.storedReplies.PushBack(nil)
		s.pendingAcks++
	}
	if s.phase == connecting || len(s.publishes) != 0 || (p.record.QoS != 0 && s.pendingSubscriptions != 0) {
		if p.refusal == 0 && s.phase == connecting {
			c.reserveBodyBatch()
			p.batchCredit = true
		}
		s.publishes = append(s.publishes, p)
		r := &storeRequest{kind: releasePublishes, conn: s, lane: c.connectionLane(s), op: p, publication: p}
		if s.phase == connected {
			r.channel = p.channel
			if p.refusal == 0 {
				c.queueBatch(&r.publishes, p.channel)
			}
			if p.record.QoS != 2 && p.refusal == 0 {
				p.enter()
			}
		}
		c.enqueue(r)
		return acts
	}
	return c.startPublication(acts, p, nil, now)
}

// startPublication executes the fixed identifier decision. CONNECT grant and
// live intake share classification; a later packet cannot change this result.
func (c *Core) startPublication(acts []Action, p *publication, source *storeRequest, now time.Time) []Action {
	s := p.conn
	if h := p.repeat; h != nil {
		c.releasePublicationCredit(p)
		c.returnBytes(p.bytes, now)
		return c.repeatIncoming(acts, s, p.packetID, h, p.pendingReply)
	}
	if p.refusal != 0 {
		if p.request {
			c.requestsTotal[p.requestKind][requestRefused].Inc()
		}
		if p.refused != nil {
			p.refused.Inc()
		}
		return c.finishPublication(acts, p, p.refusal, now)
	}
	if p.record.QoS == 2 {
		// A queued identifier is connection-local until its hold write can
		// run. Only that write must precede a later session load.
		h := p.incoming
		h.channel, h.request = p.channel, false
		h.holdLane = c.connectionLane(s)
		h.holdLane.holds++
	}
	if !p.hasID {
		p.record.ID = newMessageID(time.UnixMilli(p.record.Time))
	}
	c.queuePublish(p, source)
	return acts
}

// route maps immutable configuration routing to the stored channel owner.
func (c *Core) route(name string) *channelState {
	if index := c.routing.Resolve(name); index >= 0 {
		return c.routed[index]
	}
	return &c.broadcast
}

// queuePublish extends the channel's last mutable batch. A next publication
// closes a batch before either bound would be exceeded; an oversized body
// starts a batch alone. Pending lists acquire capacity in store-request order.
func (c *Core) queuePublish(p *publication, source *storeRequest) {
	ch := p.channel
	if ch.row.ID == 0 {
		panic("core: PUBLISH without loaded $broadcast channel")
	}
	// An accepted batch can grow only at the collecting group's tail, keeping
	// member order. A pending batch keeps its channel position across unrelated
	// requests, but cannot grow across an ordered read or stop.
	r := ch.request
	// Adding a session ending requires a new joint owner check. An accepted
	// batch already holds its owners and cannot acquire another Will's groups.
	joining := p.will == nil && source == nil && ch.pending == nil && r != nil && r.kind == savePublishes && c.collecting != nil &&
		r.group == c.collecting && r.group.members[len(r.group.members)-1] == r && !p.reserved && p.dropped == nil && ch.maximum == 0 &&
		(c.retained.maximum == 0 || !p.record.PublishedRetained)
	changes := 0
	if p.will != nil {
		changes = p.will.willChanges
	}
	if joining && (!c.batchRoom(&r.publishes, p) || !c.groupRoom(1, p.record.Bytes(), changes)) {
		c.sealGroup(commitMaxRecords)
		joining = false
	}
	newBatch := false
	if !joining {
		if source != nil {
			r, newBatch = source, true
			if r.channel == nil {
				r.channel = ch
				c.registerChannel(r)
			}
		} else {
			r = ch.pending
			if r == nil || r.kind != savePublishes || r.group != nil || r.order <= c.publishBarrier || !c.batchRoom(&r.publishes, p) {
				r, newBatch = &storeRequest{conn: p.conn, channel: ch}, true
			}
		}
		if newBatch {
			r.kind = savePublishes
			r.publishes.prepared = true // ordinary publications need no admission pass
			r.op = &r.publishes
		}
	}
	c.releasePublicationCredit(p)
	if newBatch {
		c.queueBatch(&r.publishes, ch)
		if source == nil {
			c.enqueue(r)
		} else {
			// A queued body keeps its original channel position when it
			// becomes a batch, before later multi-owner requests.
			c.continueRequest(r)
		}
	}
	// A pipelined body may precede CONNECT's final assigned identifier.
	if p.conn != nil {
		if p.conn.bridge != nil {
			p.record.Bridge = p.conn.bridge.id
		} else if p.record.HeldSession == 0 {
			p.record.Publisher = p.conn.clientID
		}
		if p.incoming != nil && p.conn.bridge == nil {
			p.record.HeldSession = p.conn.session.ID
		}
	}
	b := &r.publishes
	b.trackCharges = c.retained.maximum != 0
	b.append(ch, p)
	if p.incoming == nil || p.record.Offset == 0 {
		if p.reserved || p.dropped != nil || ch.maximum != 0 {
			b.prepared = false
		}
		if c.retained.maximum != 0 && p.record.PublishedRetained {
			b.prepared = false
			if b.topics == nil {
				b.topics = make(map[string]int)
			}
			if _, ok := b.topics[p.record.Topic]; !ok {
				b.topics[p.record.Topic] = len(b.charges)
				b.charges = append(b.charges, store.RetainedCharge{Topic: p.record.Topic})
			}
		}
	}
	if joining {
		r.group.records++
		r.group.bytes += p.record.Bytes()
		r.group.changes += changes
		if p.will != nil {
			c.sessionWrites = r.group
		}
		c.limitGroup()
	}
}

// acceptPublication releases one CONNECT-held body at its place in the
// session lane. Repeats are judged against the resolved session and release
// their existing memory and batch reservations without another store write.
func (c *Core) acceptPublication(acts []Action, r *storeRequest, now time.Time) []Action {
	s, p := r.conn, r.op.(*publication)
	if s.end.seen || s.phase >= refused {
		return acts // ending the connection already discarded its queued bodies
	}
	s.publishes = slices.Delete(s.publishes, 0, 1)
	return c.startPublication(acts, p, r, now)
}

func (c *Core) discardPublishes(b *[]*publication, now time.Time) {
	for _, p := range *b {
		c.releasePublicationCredit(p)
		c.returnBytes(p.bytes, now)
		p.bytes, p.record.Payload, p.record.StoredPayload, p.record.Properties = 0, nil, nil, nil
		if p.will != nil {
			continue
		}
		if p.incoming != nil {
			p.incoming.pending = false
			if !p.bridgeRecovery {
				c.forgetIncoming(p.conn, p.packetID)
				c.answerIncoming(nil, p, nil, false)
			}
		} else if p.pendingReply != nil {
			p.conn.storedReplies.Remove(p.pendingReply)
			c.completeAcknowledgement(p.conn)
		}
		p.pendingReply = nil
	}
	clear(*b)
	*b = (*b)[:0]
}

// savedPublishes runs only after the store answers and returns every charge.
// An ended, taken-over or closed owner cannot receive an acknowledgement;
// its record is still stored and counted (R4).
func (c *Core) savedPublishes(acts []Action, b *publishBatch, err error, now time.Time) []Action {
	ch := b.owner
	c.releaseBatch(b)
	ch.reserved -= b.channelReservation
	if err == nil {
		c.committedChannel(ch, &b.channel, b.count-b.holds, b.removed+b.replaced)
		ch.stored.Add(uint64(b.count - b.holds))
		ch.row.HeldBytes += b.heldDelta
		if b.removed != 0 {
			ch.removed.Add(uint64(b.removed))
		}
		c.retained.charged += b.chargeDelta
		c.retained.bytes += b.valueBytes
		c.retained.values += b.values
	} else if errors.Is(err, store.ErrFull) {
		c.storeFull.Add(uint64(b.count))
	}
	c.resolvePublications(ch, b.channel.Next)
	c.retained.reserved -= b.reservation
	for p, offset := range b.records {
		if err == nil && p.record.Offset != 0 {
			if p.conn != nil && p.conn.bridge != nil {
				p.conn.bridge.received.Inc()
			}
			c.committedRecord(ch, p.record.Topic, offset, p.record.Time)
			if ch.row.Type == store.Latest {
				c.invalidateRecord(ch, p.replacedOffset)
				period := ch.period
				if p.record.PayloadSize == 0 {
					period = ch.tombstonePeriod
				}
				if period != 0 {
					c.noteRetention(ch, timestamp.AfterMilli(p.record.Time, period))
				}
			}
			if p.record.Retained && ch.row.ID == 1 {
				c.noteRetention(ch, retainedDeadline(&p.record, c.retained.period))
			}
		}
		var reason byte
		if err != nil {
			reason = 0x97
		}
		acts = c.finishPublication(acts, p, reason, now)
		if p.will != nil {
			acts = c.finishedWill(acts, p, err, false, now)
		}
	}
	for _, p := range b.dropped {
		acts = c.finishedWill(acts, p, err, true, now)
	}
	clear(b.publications)
	b.publications = b.publications[:0]
	clear(b.initial[:])
	clear(b.dropped)
	b.dropped = b.dropped[:0]
	return acts
}

// committedChannel applies publication accounting for client writes and queue
// moves alike. The same size scheduler services both kinds of append entry.
func (c *Core) committedChannel(ch *channelState, change *store.AdvanceChannel, added, removed int) {
	ch.committedTail = max(ch.committedTail, change.Next)
	ch.row.Bytes += change.Bytes
	ch.records.Add(int64(added - removed))
	if c.phase == starting {
		ch.bytes.Set(ch.row.Bytes)
	}
	if ch.retentionBytes != 0 && ch.row.Bytes > ch.retentionBytes {
		c.scheduleRetention(ch)
	}
}

// committedRecord wakes matching readers and establishes an append log's first
// age deadline. Later timestamps wait behind it, including after a clock change.
func (c *Core) committedRecord(ch *channelState, topic string, offset, at int64) {
	if ch.queue != nil {
		ch.queue.ready++
		ch.queue.updateJobs()
	}
	c.wakeConsumers(ch, topic, offset)
	if ch.row.Type != store.Latest && ch.period != 0 && ch.retentionLogAt == 0 {
		ch.retentionLogAt = timestamp.AfterMilli(at, ch.period)
		c.noteRetention(ch, ch.retentionLogAt)
	}
}

// finishPublication returns the body and settles its original reply slot for
// both admission and storage results. A stopped owner receives no reply.
func (c *Core) finishPublication(acts []Action, p *publication, reason byte, now time.Time) []Action {
	c.returnBytes(p.bytes, now)
	if p.will != nil {
		return acts
	}
	s := p.conn
	if p.incoming != nil {
		return c.finishIncoming(acts, p, reason, now)
	}
	if p.record.QoS != 0 {
		c.completeAcknowledgement(s)
	}
	if c.failure != nil || p.record.QoS == 0 || s.end.seen || s.phase != connected || c.connections[s.conn] != s {
		return c.completeStoredReply(acts, s, p.pendingReply, nil, false)
	}
	if reason != 0 && s.version == packet.V311 {
		return c.completeStoredReply(c.finish(acts, s, reason, now), s, p.pendingReply, nil, false)
	}
	typ := packet.PUBACK
	if p.record.QoS == 2 {
		typ = packet.PUBREC
	}
	return c.completeStoredReply(acts, s, p.pendingReply, &connection.Ack{Type: typ,
		PacketID: p.packetID, ReasonCode: reason, Quota: p.quota}, reason == 0)
}

// newMessageID encodes RFC 9562's UUIDv7 with the packet event's Unix
// milliseconds and 74 random bits. It never reads the clock or adds a
// dependency, and does not impose a uniqueness constraint on publisher IDs.
func newMessageID(at time.Time) string {
	var raw [16]byte
	if _, err := rand.Read(raw[6:]); err != nil {
		panic("core: generate message identifier: " + err.Error())
	}
	millis := uint64(at.UnixMilli())
	binary.BigEndian.PutUint32(raw[:4], uint32(millis>>16))
	binary.BigEndian.PutUint16(raw[4:6], uint16(millis))
	raw[6] = raw[6]&0x0f | 0x70
	raw[8] = raw[8]&0x3f | 0x80
	var text [36]byte
	hex.Encode(text[:8], raw[:4])
	text[8] = '-'
	hex.Encode(text[9:13], raw[4:6])
	text[13] = '-'
	hex.Encode(text[14:18], raw[6:8])
	text[18] = '-'
	hex.Encode(text[19:23], raw[8:10])
	text[23] = '-'
	hex.Encode(text[24:], raw[10:])
	return string(text[:])
}
