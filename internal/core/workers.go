package core

import (
	"cmp"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

// queueState keeps only connected assignments. Unassigned jobs and their
// attempt history stay in SQLite and are selected through bounded metadata pages.
// The common shared dispatcher supplies capacity, round-robin and admission.
type queueState struct {
	group                                    sharedGroup
	channel                                  *channelState
	responseTopic                            string
	visibility                               time.Duration
	retry                                    config.Retry
	retryDeadline                            coreDeadline
	attempts                                 map[int64]*queueAttempt
	ready, backoff, offered, held, heldBytes int64
	jobs                                     [4]*metrics.Gauge
	workers                                  *metrics.Gauge
	delivered, acknowledged, stale           *metrics.Counter
	failed                                   [3]*metrics.Counter
}

// queueAttempt survives MQTT settlement: PUBACK starts the application hold.
// It carries no payload and is bounded by one assignment per worker and queue.
type queueAttempt struct {
	queue                             *queueState
	member                            *sharedMember
	delivery                          *outgoing
	data                              store.DeliveryAttempt
	offset, bytes, failed, recordTime int64
	deadline                          coreDeadline
	ending                            *storeRequest
	heldUntil                         time.Time
	sent, spent, held, complete       bool
}

type queueFailure uint8

const (
	queueReturned queueFailure = iota
	queueTimedOut
	queueWorkerGone
)

func (c *Core) initQueue(ch *channelState, cfg *config.Channel, reg *metrics.Registry) {
	q := &queueState{channel: ch, visibility: cfg.VisibilityTimeout, retry: cfg.Retry, attempts: make(map[int64]*queueAttempt), ready: ch.row.Jobs - ch.row.RetryJobs, backoff: ch.row.RetryJobs}
	ch.queue = q
	q.group = sharedGroup{queue: q, filter: "$saguin/queue/" + ch.row.Name, part: "#", loaded: true, active: true, returnKnown: true, assigned: make(map[store.SharedRecord]*outgoing)}
	q.responseTopic = q.group.filter + "/response"
	for i, cause := range [...]string{"return", "timeout", "worker_gone"} {
		q.failed[i] = reg.Counter("saguin_queue_attempts_failed_total", "channel", ch.row.Name, "cause", cause)
	}
	for i, state := range [...]string{"ready", "backoff", "offered", "held"} {
		q.jobs[i] = reg.Gauge("saguin_queue_jobs", "channel", ch.row.Name, "state", state)
	}
	q.workers = reg.Gauge("saguin_queue_workers", "channel", ch.row.Name)
	q.delivered = reg.Counter("saguin_queue_delivered_total", "channel", ch.row.Name)
	q.acknowledged = reg.Counter("saguin_queue_acknowledged_total", "channel", ch.row.Name)
	q.stale = reg.Counter("saguin_queue_stale_responses_total", "channel", ch.row.Name)
	c.addSharedCursor(&q.group, ch, ch.row.Floor)
	c.indexConsumer(q.group.cursors[0], true)
	q.retryDeadline.owner = q.group.cursors[0]
	if ch.row.RetryAt.Valid {
		c.noteQueueRetry(q, ch.row.RetryAt.Int64)
	}
	q.updateJobs()
}

func (q *queueState) updateJobs() {
	q.jobs[0].Set(q.ready)
	q.jobs[1].Set(q.backoff)
	q.jobs[2].Set(q.offered)
	q.jobs[3].Set(q.held)
}

// syncWorkers changes membership only after the subscription change commits.
// A removed member's sent attempt remains owned by the asking connection.
func (c *Core) syncWorkers(s *connectionState) {
	for i, m := range slices.Backward(s.workers) {
		if _, ok := s.subscriptions[m.group.filter]; ok {
			continue
		}
		if !m.ending {
			c.removeSharedMember(m)
		}
		m.group.queue.workers.Set(int64(len(m.group.members)))
		if m.attempt == nil && m.owners[0].last == nil {
			s.workers = slices.Delete(s.workers, i, i+1)
		}
	}
	if s.end.seen || s.phase >= refused || c.connections[s.conn] != s {
		return
	}
	filters := make([]string, 0)
	for filter := range s.subscriptions {
		if _, ok := c.routing.WorkerChannel(filter); ok {
			filters = append(filters, filter)
		}
	}
	slices.Sort(filters)
	for _, filter := range filters {
		index, _ := c.routing.WorkerChannel(filter)
		q := c.routed[index].queue
		var m *sharedMember
		for _, member := range s.workers {
			if member.group == &q.group {
				m = member
				break
			}
		}
		if m == nil {
			m = c.addRecipient(s, &q.group)
			s.workers = append(s.workers, m)
			c.recipientOwner(m, q.group.cursors[0])
		} else if m.ending {
			c.joinRecipient(m)
		}
		m.sub = s.subscriptions[filter]
		u := m.owners[0]
		u.filters = []store.Subscription{m.sub}
		u.held = !s.supportsWorkerOffers() || !s.access.AllowsChannel(acl.Consume, q.channel.row.Name)
		c.initWindow(s)
		c.queueConsumer(u)
		q.workers.Set(int64(len(q.group.members)))
		c.queueShared(&q.group)
	}
}

// supportsWorkerOffers identifies connections that can answer a queue offer.
func (s *connectionState) supportsWorkerOffers() bool {
	return s.version == packet.V5
}

func workerGrant(m *sharedMember) subscriptionGrant {
	grant := subscriptionGrant{matched: !m.owners[0].held, qos: 1}
	if m.sub.Identifier != 0 {
		grant.ids = []uint32{m.sub.Identifier}
	}
	return grant
}

func (c *Core) assignAttempt(d *outgoing, row *store.RecordMetadata, now time.Time, id string) {
	u := d.consumer
	q := u.share.queue
	a := &queueAttempt{queue: q, member: u.sharedMember, delivery: d, data: store.DeliveryAttempt{ID: id, Number: row.Attempts + 1, Began: now.UnixMilli()}, offset: d.offset, failed: row.Failed, recordTime: row.Time, bytes: int64(len(row.Topic) + len(row.Properties) + row.PayloadSize)}
	a.deadline.attempt = a
	d.attempt = a
	a.member.attempt = a
	q.attempts[a.offset] = a
	if u.conn.queueAttempts == nil {
		u.conn.queueAttempts = make(map[string]*queueAttempt)
	}
	u.conn.queueAttempts[id] = a
	d.exchange.Attempt = &a.data
}

func (c *Core) handedAttempt(d *outgoing) {
	a := d.attempt
	if a.sent {
		return
	}
	a.sent = true
	a.queue.ready--
	a.queue.offered++
	a.queue.delivered.Inc()
	a.queue.updateJobs()
}

func (c *Core) spendAttempt(d *outgoing, now time.Time) {
	a := d.attempt
	if a == nil || a.complete || a.ending != nil || a.held {
		return
	}
	a.held = true
	a.heldUntil = now.Add(a.queue.visibility)
	c.queueHolds.set(&a.deadline, a.heldUntil)
	a.queue.offered--
	a.queue.held++
	a.queue.heldBytes += a.bytes
	a.queue.updateJobs()
	if a.spent {
		return
	}
	a.spent = true
	r := c.queueAttemptRequest(a, nil)
	r.op = &store.QueueChange{Channel: a.queue.channel.row.ID, Offset: a.offset, Began: a.data.Began, Spend: true}
	c.enqueue(r)
}

// releaseAttemptDelivery separates the exchange's descriptor lifetime from
// the hold. A cancelled preparation returns its job to the common scan.
func (c *Core) releaseAttemptDelivery(d *outgoing) {
	a := d.attempt
	if a == nil {
		return
	}
	a.delivery = nil
	d.attempt = nil
	if !d.attempted && a.ending == nil {
		c.endAttempt(a)
	}
}

func (c *Core) endAttempt(a *queueAttempt) {
	if a.complete {
		return
	}
	a.complete = true
	c.queueHolds.set(&a.deadline, time.Time{})
	q := a.queue
	delete(q.attempts, a.offset)
	delete(a.member.conn.queueAttempts, a.data.ID)
	if a.member.attempt == a {
		a.member.attempt = nil
	}
	if a.delivery != nil {
		// An application result ends the assignment even if its MQTT
		// exchange still awaits PUBACK. A later attempt owns its own entry.
		delete(q.group.assigned, a.delivery.sharedRecord())
		a.delivery.attempt = nil
		a.delivery = nil
	}
	if a.held {
		q.held--
		q.heldBytes -= a.bytes
	} else if a.sent {
		q.offered--
	}
	q.updateJobs()
	c.queueConsumers(a.member.conn)
	c.queueShared(&q.group)
}

func (c *Core) dropWorkers(s *connectionState) {
	for _, m := range s.workers {
		if !m.ending {
			c.removeSharedMember(m)
		}
		m.group.queue.workers.Set(int64(len(m.group.members)))
		c.queueShared(m.group)
	}
	s.workers = nil
}

// queueAttemptRequest uses the dispatcher lane as well as the channel lane.
// A decision page can never outlive the terminal change it would otherwise undo.
func (c *Core) queueAttemptRequest(a *queueAttempt, r *storeRequest) *storeRequest {
	if r == nil {
		r = new(storeRequest)
	}
	g := &a.queue.group
	g.references++
	r.kind, r.channel, r.queueAttempt, r.shares = saveQueueChange, a.queue.channel, a, []*sharedGroup{g}
	return r
}

// failWorkers runs at departure, before descriptors are cancelled. MQTT
// exchanges may survive, but their application attempts end at this event.
func (c *Core) failWorkers(s *connectionState, now time.Time) {
	attempts := make([]*queueAttempt, 0, len(s.queueAttempts))
	for _, a := range s.queueAttempts {
		if a.sent && a.ending == nil {
			attempts = append(attempts, a)
		}
	}
	slices.SortFunc(attempts, func(a, b *queueAttempt) int {
		if n := cmp.Compare(a.queue.channel.row.ID, b.queue.channel.row.ID); n != 0 {
			return n
		}
		return cmp.Compare(a.offset, b.offset)
	})
	for _, a := range attempts {
		if c.failure != nil {
			c.endAttempt(a)
			continue
		}
		r := c.queueAttemptRequest(a, nil)
		c.finishQueueAttempt(r, a, false, queueWorkerGone, now)
		c.enqueue(r)
	}
}

// retryTime uses milliseconds, the store's time representation. Arithmetic
// saturates only at its representable endpoint, never at a policy wait cap.
func retryTime(retry config.Retry, failures int64, now time.Time) int64 {
	at := now.UnixMilli()
	delay := timestamp.CeilMilli(time.Unix(0, 0).Add(retry.Delay))
	room := int64(math.MaxInt64)
	if at > 0 {
		room -= at
	}
	switch retry.Mode {
	case config.Linear:
		if delay > room/failures {
			return math.MaxInt64
		}
		delay *= failures
	case config.Exponential:
		shift := uint64(failures - 1)
		if shift >= 63 || delay > room>>shift {
			return math.MaxInt64
		}
		delay <<= shift
	}
	if delay > room {
		return math.MaxInt64
	}
	return at + delay
}

func (a *queueAttempt) moveRequired(now time.Time, spent int64) bool {
	q := a.queue
	return (q.channel.period != 0 && timestamp.AfterMilli(a.recordTime, q.channel.period) <= now.UnixMilli()) || spent >= int64(q.retry.MaxAttempts)
}

// finishQueueAttempt establishes the application result immediately. Its
// store request then preserves that result through any storage wait.
func (c *Core) finishQueueAttempt(r *storeRequest, a *queueAttempt, complete bool, cause queueFailure, now time.Time) {
	q := a.queue
	op := &store.QueueChange{Channel: q.channel.row.ID, Offset: a.offset, Began: a.data.Began,
		Spend: !a.spent && (complete || cause == queueReturned), Complete: complete, Fail: !complete}
	a.spent = a.spent || op.Spend
	a.ending = r
	c.queueHolds.set(&a.deadline, time.Time{})
	r.queueAttempt, r.kind, r.op = a, saveQueueChange, op
	if complete {
		return
	}
	q.failed[cause].Inc()
	op.NextTry = retryTime(q.retry, a.failed+1, now)
	spent := a.data.Number - 1
	if a.spent {
		spent++
	}
	if a.moveRequired(now, spent) {
		move := &store.Retention{Channel: q.channel.row.ID, At: now.UnixMilli(), LogPeriod: q.channel.period,
			After: a.offset - 1, Tail: a.offset + 1, Job: op, DeadLetters: &store.DeadLetters{Source: q.channel.row.Name}}
		r.kind, r.op, r.otherChannel = readRetentionPage, &store.ReadRetention{Retention: move}, q.channel.deadLetter
	}
}

// queueResponse accepts a classified packet at arrival. Its terminal write
// owns the queue and dispatcher, never the connection's session lane. Reply
// ownership remains connection-local and cannot gate the application result.
func (c *Core) queueResponse(acts []Action, r *storeRequest, now time.Time) []Action {
	q, s := r.request, r.conn
	answer := string(q.publish.Payload)
	c.returnBytes(q.bytes, now)
	q.bytes, q.publish = 0, nil
	if q.reserved {
		c.retained.reservedProperty.Inc()
		return c.finishChannelRequest(acts, r, 0x83, now)
	}
	a := s.queueAttempts[string(q.correlation)]
	if a == nil || a.queue.channel != r.channel || !a.sent || a.ending != nil || (answer != "ack" && answer != "return") {
		r.channel.queue.stale.Inc()
		acts = c.logFailure(acts, logger.Warn, fmt.Errorf("client %q: stale queue response on %s", s.clientID, r.channel.row.Name), now)
		return c.finishChannelRequest(acts, r, 0, now)
	}
	r = c.queueAttemptRequest(a, r)
	c.finishQueueAttempt(r, a, answer == "ack", queueReturned, now)
	c.enqueue(r)
	return acts
}

func (c *Core) savedQueueChange(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	a := r.queueAttempt
	op, plain := r.op.(*store.QueueChange)
	if !plain {
		move := r.op.(*store.Retention)
		op = move.Job
		c.releaseRetentionPage(r)
		r.channel.retaining = false
		if move.DeadLetters.Channel.Next != 0 {
			c.resolvePublications(r.otherChannel, move.DeadLetters.Channel.Next)
		}
		if err == nil {
			acts = c.applyRetention(acts, r, now)
		}
	}
	if a.ending == r && !a.complete {
		c.endAttempt(a)
		if plain && a.held && a.queue.channel.period != 0 {
			c.noteRetention(a.queue.channel, timestamp.AfterMilli(a.recordTime, a.queue.channel.period))
		}
		if err == nil {
			if op.Complete {
				a.queue.acknowledged.Inc()
			} else if plain {
				a.queue.backoff++
				c.noteQueueRetry(a.queue, op.NextTry)
			}
		} else if a.sent {
			a.queue.ready++
		}
		a.queue.updateJobs()
		c.scheduleRetention(a.queue.channel)
	}
	c.releaseSharedRequest(r)
	if r.request != nil {
		reason := byte(0)
		if err != nil {
			reason = 0x97
		}
		return c.finishChannelRequest(acts, r, reason, now)
	}
	return acts
}

func (c *Core) noteQueueRetry(q *queueState, at int64) {
	// The stored origin is floored to milliseconds. Use the existing
	// deadline rounding so the wait cannot finish in its discarded fraction.
	rounded := at
	if rounded != math.MaxInt64 {
		rounded = timestamp.AfterMilli(at, 0)
	}
	next := time.UnixMilli(rounded)
	if q.retryDeadline.at.IsZero() || next.Before(q.retryDeadline.at) {
		c.queueRetries.set(&q.retryDeadline, next)
	}
}

// resetQueueRetry replaces consumed waits with the indexed committed minimum.
// A full page's ready continuation owns discovery of its remaining minimum.
func (c *Core) resetQueueRetry(q *queueState, at sql.NullInt64) {
	c.queueRetries.set(&q.retryDeadline, time.Time{})
	if q.backoff != 0 && at.Valid {
		c.noteQueueRetry(q, at.Int64)
	}
}

func (c *Core) expireQueueHolds(now time.Time) {
	for c.queueHolds.Len() != 0 && !now.Before(c.queueHolds.items[0].at) {
		a := c.queueHolds.items[0].attempt
		r := c.queueAttemptRequest(a, nil)
		c.finishQueueAttempt(r, a, false, queueTimedOut, a.heldUntil)
		c.enqueue(r)
	}
}

func (c *Core) retryQueues(now time.Time) {
	for c.queueRetries.Len() != 0 && !now.Before(c.queueRetries.items[0].at) {
		deadline := c.queueRetries.items[0]
		q := deadline.owner.share.queue
		c.queueRetries.set(deadline, time.Time{})
		c.queueRetryPage(q, now.UnixMilli())
	}
}

// queueRetryPage schedules ready retry work through the ordinary owner FIFOs.
// Only the first page waits for its stored retry deadline.
func (c *Core) queueRetryPage(q *queueState, at int64) {
	g := &q.group
	g.references++
	op := &store.QueueRetries{Channel: q.channel.row.ID, At: at}
	c.enqueue(&storeRequest{kind: readQueueRetries, channel: q.channel, shares: []*sharedGroup{g}, op: &store.ReadQueueRetries{Retries: op}})
}

func (c *Core) readQueueRetries(r *storeRequest, now time.Time) {
	if c.failure != nil || c.phase >= quiescing {
		c.returnMetadataCredit()
		c.releaseChannel(r)
		c.releaseSharedRequest(r)
		return
	}
	op := r.op.(*store.ReadQueueRetries).Retries
	r.kind, r.op = saveQueueRetries, op
	if len(op.Offsets) == 0 {
		c.releaseChannel(r)
		c.savedQueueRetries(r, nil, now)
		return
	}
	c.continueRequest(r)
}

func (c *Core) savedQueueRetries(r *storeRequest, err error, now time.Time) {
	c.returnMetadataCredit()
	q, op := r.channel.queue, r.op.(*store.QueueRetries)
	if err == nil {
		q.backoff -= int64(len(op.Offsets))
		q.ready += int64(len(op.Offsets))
		cursor := q.group.cursors[0]
		for _, offset := range op.Offsets {
			cursor.scan = min(cursor.scan, offset)
		}
		cursor.position = min(cursor.position, cursor.scan)
		c.resetQueueRetry(q, op.Next)
		if len(op.Offsets) == store.MetadataPage {
			c.queueRetryPage(q, op.At)
		}
		q.updateJobs()
	} else {
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
		c.noteQueueRetry(q, now.Add(time.Second).UnixMilli())
	}
	c.releaseSharedRequest(r)
}

func (ch *channelState) queueHeldBytes() int64 {
	if ch.queue != nil {
		return ch.queue.heldBytes
	}
	return 0
}
