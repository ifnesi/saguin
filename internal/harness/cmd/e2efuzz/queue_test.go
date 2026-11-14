package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The fake broker's queue, written from design.md's Channels, queue
// ("Its records are stored like append records [...] A subscription to
// $saguin/queue/<channel> at QoS 1 makes a worker. Each job goes to one
// worker at a time"; "Retention never deletes an unfinished job. It moves
// the job to the append channel <channel>__dlq in one transaction"),
// Delivery, Queue jobs and Service publications, $saguin topics (the queue
// rows), SUBSCRIBE (the worker row, Options, Refusals) and storage.md,
// queue_jobs, not from the runner's model: a job is its record and a row of
// queue_jobs, stored together; an eligible worker, round-robin by client
// id, gets one attempt at a time, a QoS 1 offer under a new attempt id;
// PUBACK, response, hold, retry and retention move each job as the design
// says, with real timers unless a scene controls queue time.

// The faults of the queue the fake broker can be given, each breaking one
// rule the runner checks.
const (
	lostDeadLetter fault = "owed dead letter not delivered"
	// jobLost finishes a job at its worker's first PUBACK 0x00, no ack
	// having come.
	jobLost fault = "job finished without an ack"
	// deadAndKept moves a job to the dead-letter channel and keeps its row.
	deadAndKept fault = "dead letter whose job stays"
	// twoWorkers offers each held job to a second worker too, once.
	twoWorkers fault = "job held by two workers"
	// secondJob gives a worker a job while it holds another.
	secondJob fault = "second job to a worker"
	// reusedAttempt offers a job again under its last attempt's id.
	reusedAttempt fault = "attempt id reused"
	// offerExpiry sends each offer with its job's Message Expiry.
	offerExpiry fault = "offer with Message Expiry"
	// foreignAck lets an ack from any session complete the attempt it names.
	foreignAck fault = "foreign ack taken"
	// staleAck lets an ack naming an ended attempt finish its job.
	staleAck fault = "stale ack taken"
	// earlyExpired moves a job halfway through retention_period.
	earlyExpired fault = "job expired early"
	// noRetryWait offers a failed job again at once.
	noRetryWait fault = "no retry wait"
	// unspentSpent counts as spent an attempt whose worker went with
	// neither PUBACK nor answer.
	unspentSpent fault = "unspent attempt spent"
	// alwaysEvicted gives every move by retention the reason evicted.
	alwaysEvicted fault = "every move evicted"
	// freshResendID resends an offer on a session's new connection under
	// a new attempt id.
	freshResendID fault = "resent offer with a new attempt id"
	// offersAfterUnsubscribe keeps offering jobs to a worker that has
	// unsubscribed.
	offersAfterUnsubscribe fault = "offers after the UNSUBACK"
)

// withDeadLetters is cs and, after them, each queue's dead-letter channel
// as the broker derives it: an append channel named <queue>__dlq whose
// filter is the queue's under __dlq/, starting its readers at the floor
// (design.md, Channels, queue).
func withDeadLetters(cs []harness.Channel) []harness.Channel {
	for i := range cs {
		if c := &cs[i]; c.Type == "queue" {
			cs = append(cs, harness.Channel{Name: c.Name + "__dlq", Type: "append", Filter: "__dlq/" + c.Filter, Start: "floor"})
		}
	}
	return cs
}

// fakeQueueSchema is storage.md's queue_jobs, as far as the runner reads it.
const fakeQueueSchema = `CREATE TABLE queue_jobs (channel INTEGER, "offset" INTEGER, PRIMARY KEY (channel, "offset"))`

// fakeQueue is the queue of channel id: its settings, its jobs in offset
// order, finished ones included until retention deletes their records, and
// the client id it last offered a job to.
type fakeQueue struct {
	id, dlq int64
	c       *harness.Channel
	jobs    []*fakeJob
	last    string
}

// fakeJob is one job: its record, its attempts spent and failed, the
// moments its first and last spent attempt began, and its current attempt,
// nil for none; ready, it may be offered now; done, it is finished.
type fakeJob struct {
	rec                *fakeRecord
	spent, failed      int
	first, last        int64
	current            *fakeAttempt
	attempts           []*fakeAttempt
	ready, done, moved bool
	lastID             string // its last attempt's id, for reusedAttempt
	second             bool   // twoWorkers has offered it to a second worker
}

// fakeAttempt is one try at giving a job to a worker: its id, the session
// it went to, its offer, held once its PUBACK 0x00 came, and whether it was
// spent.
type fakeAttempt struct {
	id           string
	session      *fakeSession
	conn         *fakeConn
	offer        *delivery
	held, spent  bool
	began        int64
	j            *fakeJob
	q            *fakeQueue
	holdDeadline *time.Timer
}

// fakeQueueOf is the queue of channel id, made on first use, nil where id
// is no queue's.
func (b *fake) fakeQueueOf(id int64) *fakeQueue {
	if id < 2 || fakeConfigured[id-2].Type != "queue" {
		return nil
	}
	if b.queues == nil {
		b.queues = map[int64]*fakeQueue{}
	}
	q := b.queues[id]
	if q == nil {
		// Its settings as the runner's configuration holds them now, which
		// a scenario may set (scene).
		c := &withDeadLetters(channelOptions())[id-2]
		dlq := int64(slices.IndexFunc(fakeConfigured, func(o harness.Channel) bool { return o.Name == c.Name+"__dlq" }) + 2)
		q = &fakeQueue{id: id, dlq: dlq, c: c}
		b.queues[id] = q
		b.sweepLater()
	}
	return q
}

// fakeQueueNamed is the queue named, nil for none.
func (b *fake) fakeQueueNamed(name string) *fakeQueue {
	i := slices.IndexFunc(fakeConfigured, func(c harness.Channel) bool { return c.Name == name })
	if i < 0 {
		return nil
	}
	return b.fakeQueueOf(int64(i + 2))
}

// newJob makes r, a record of queue q stored in the same transaction as
// its row, a job ready to be offered, and offers it. It runs under the
// lock.
func (b *fake) newJob(q *fakeQueue, r *fakeRecord) {
	q.jobs = append(q.jobs, &fakeJob{rec: r, ready: true})
	b.seen.jobs++
	b.sweep(q)
	b.dispatch() // in the event storing it, never waiting for another
}

// fakeConsumes reports whether the runner's acl_file lets user consume the
// queue named; with ignoredACL, everyone may.
func (b *fake) fakeConsumes(user, queue string) bool {
	return b.fault == ignoredACL || slices.ContainsFunc(grantsOf(user), func(g grant) bool { return g.channel == queue && g.consume })
}

// workerCode is the SUBACK code of filter, under $saguin/queue/, asked at
// qos with No Local where noLocal, for s on c, a reserved saguin- property
// or a partition reserved: SUBSCRIBE, the table, "refused (0x83) / 1 / 1;
// MQTT 3.1.1: refused (0x80)"; Options, No Local "refused with 0x8F on a
// worker subscription"; Refusals, 0x87 for the acl_file, 0x8F "for a
// filter of the wrong shape, such as a narrower $saguin/queue filter", in
// "refusal order 0x87, 0x8F, 0x97, 0x83"; $saguin topics: "Any other
// filter under $saguin/ is refused with 0x87".
func (b *fake) workerCode(c *fakeConn, s *fakeSession, filter string, qos byte, noLocal, reserved bool) byte {
	name := strings.TrimPrefix(filter, "$saguin/queue/")
	q := b.fakeQueueNamed(name)
	switch {
	case c.version != harness.V5:
		return 0x80
	case strings.Contains(name, "/"):
		return 0x8F
	case q == nil || !b.fakeConsumes(s.user, name):
		return 0x87
	case noLocal:
		return 0x8F
	case s.subs[filter] == nil && len(s.subs) >= fakeSubscriptions:
		return 0x97
	case qos == 0 || reserved:
		return 0x83
	}
	return 1
}

// eligible reports whether s may be offered a job of q now: connected,
// subscribed as its worker, holding none of its jobs, allowed to consume,
// with room in its window (design.md, Delivery, Queue jobs: "Workers are
// selected round-robin among those connected, with room and allowed by the
// acl_file"; "each worker receives one job at a time per queue"). It runs
// under the lock.
func (b *fake) eligible(q *fakeQueue, s *fakeSession) bool {
	c := s.conn
	if c == nil || c.finished || !b.fakeConsumes(s.user, q.c.Name) {
		return false
	}
	if s.subs[workerTopic(q.c.Name)] == nil && (b.fault != offersAfterUnsubscribe || s.unsubscribed[q.c.Name] == nil) {
		return false
	}
	if b.fault != secondJob && slices.ContainsFunc(q.jobs, func(j *fakeJob) bool { return j.current != nil && j.current.session == s }) {
		return false
	}
	sent := 0
	for _, d := range s.inflight {
		if d.sent {
			sent++
		}
	}
	return sent+c.unwritten < c.window
}

// dispatch offers q's ready jobs, oldest first, each to the next eligible
// worker after the last selected, by complete client id, wrapping once
// (SUBSCRIBE, A shared group: "Selection is round-robin by complete client
// id, starting after the last selected id and wrapping once. Ineligible
// members are skipped"). A worker whose Maximum Packet Size the offer
// passes is passed over (Delivery: "A worker or $share member that cannot
// take it is passed over"). It runs under the lock.
func (b *fake) dispatch() {
	if b.dispatching {
		return
	}
	b.dispatching = true
	defer func() { b.dispatching = false }()
	for _, id := range slices.Sorted(maps.Keys(b.queues)) {
		q := b.queues[id]
		for _, j := range q.jobs {
			second := b.fault == twoWorkers && j.current != nil && j.current.held && !j.second
			if (!j.ready || j.done || j.current != nil) && !second {
				continue
			}
			ids := slices.Sorted(maps.Keys(b.sessions))
			start := max(slices.IndexFunc(ids, func(id string) bool { return id > q.last }), 0)
			for k := range ids {
				s := b.sessions[ids[(start+k)%len(ids)]]
				if (second && s == j.current.session) || !b.eligible(q, s) {
					continue
				}
				if b.offer(q, j, s) {
					q.last = s.id
					j.second = j.second || second
					break
				}
			}
		}
	}
}

// newAttemptID is a broker-made attempt id (design.md, Words: Attempt).
func newAttemptID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// offer hands j to s as a new attempt, a QoS 1 delivery of its record:
// Service publications, "a new broker-originated QoS 1 publication
// containing the job payload and attempt metadata, including
// saguin-expires where present. It does not inherit the job publication's
// Message Expiry [...] A worker offer carries the Subscription Identifier
// of the receiving worker's own $saguin/queue/<channel> subscription";
// Sagüin properties: "a queue job's Response Topic and Correlation Data
// are the queue's own", saguin-attempt "(from 1)". It reports whether the
// offer went. It runs under the lock.
func (b *fake) offer(q *fakeQueue, j *fakeJob, s *fakeSession) bool {
	id := newAttemptID()
	if b.fault == reusedAttempt && j.lastID != "" {
		id = j.lastID
	}
	r := j.rec
	props := slices.DeleteFunc(slices.Clone(r.props), func(p harness.Property) bool {
		return p.ID == harness.ResponseTopic || p.ID == harness.CorrelationData || (p.ID == harness.MessageExpiryInterval && b.fault != offerExpiry)
	})
	props = append(props, harness.Property{ID: harness.ResponseTopic, Str: workerTopic(q.c.Name) + "/response"},
		harness.Property{ID: harness.CorrelationData, Data: []byte(id)})
	props = append(slices.DeleteFunc(props, func(p harness.Property) bool { return p.ID == harness.UserProperty && p.Str == "saguin-id" }),
		(&delivery{rec: r}).saguin()...)
	props = append(props, harness.User("saguin-attempt", strconv.Itoa(j.spent+1)))
	sub := s.subs[workerTopic(q.c.Name)]
	if sub == nil && b.fault == offersAfterUnsubscribe {
		sub = s.unsubscribed[q.c.Name] // as if it still held it
	}
	if sub != nil && sub.id != 0 {
		props = append(props, harness.Property{ID: harness.SubscriptionIdentifier, Int: sub.id})
	}
	a := &fakeAttempt{id: id, session: s, conn: s.conn, j: j, q: q, began: b.queueNow().UnixMilli()}
	d := &delivery{rec: r, qos: 1, props: props, attempt: a}
	a.offer = d
	if !b.handOff(s.conn, s, d) {
		return false
	}
	if j.current == nil {
		j.current, j.ready, j.lastID = a, false, id
		j.attempts = append(j.attempts, a)
	}
	b.seen.offers++
	return true
}

// offerAcked takes the worker's PUBACK of d, an offer, reason its code:
// "The hold starts at the worker's PUBACK and lasts visibility_timeout"; "An
// attempt is spent at the worker's PUBACK or its response naming the
// attempt, whichever is first. A refusing PUBACK (reason 0x80 or above)
// spends the attempt and ends it as a failure, as return does". One whose
// attempt has ended changes nothing. It runs under the lock.
func (b *fake) offerAcked(d *delivery, reason byte) {
	a := d.attempt
	if a == nil || a.j.current != a || a.held {
		return
	}
	b.spend(a)
	if reason >= 0x80 {
		b.seen.refused++
		b.fail(a)
		return
	}
	a.held = true
	b.seen.held++
	if b.fault == jobLost {
		b.finish(a)
		return
	}
	a.holdDeadline = time.AfterFunc(a.q.c.VisibilityTimeout, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if a.j.current == a {
			b.seen.timedOut++
			b.fail(a)
			b.dispatch()
		}
	})
}

// spend counts a's attempt spent, once, and its beginning as its job's
// first or last spent attempt's (storage.md, queue_jobs: first_attempt,
// last_attempt).
func (b *fake) spend(a *fakeAttempt) {
	if a.spent {
		return
	}
	a.spent = true
	j := a.j
	j.spent++
	if j.first == 0 {
		j.first = a.began
	}
	j.last = a.began
}

// fail ends a, its job's current attempt, as a failure: "A failed attempt
// is offered again after a wait set by retry.mode [...] The wait starts at
// the attempt's end"; "When an attempt fails with retry.max_attempts spent,
// the job moves to the dead-letter channel as attempts_exhausted"; a held
// job past retention_period "moves to the dead-letter channel [...] when
// its hold ends", expired winning. It runs under the lock.
func (b *fake) fail(a *fakeAttempt) {
	j, q := a.j, a.q
	j.current = nil
	if a.holdDeadline != nil {
		a.holdDeadline.Stop()
	}
	j.failed++
	switch {
	case b.pastRetention(q, j):
		b.move(q, j, "expired")
	case j.spent >= q.c.RetryMaxAttempts:
		b.move(q, j, "attempts_exhausted")
	default:
		// "fixed (retry.delay), linear (retry.delay x n) or exponential
		// (retry.delay x 2^(n-1)). Here n is the number of failed attempts
		// so far."
		wait := q.c.RetryDelay
		switch q.c.RetryMode {
		case "linear":
			wait *= time.Duration(j.failed)
		case "exponential":
			wait <<= j.failed - 1
		}
		if b.fault == noRetryWait {
			j.ready = true // at once, in the event ending the attempt
			break
		}
		time.AfterFunc(wait, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if !j.done {
				j.ready = true
				b.dispatch()
			}
		})
	}
}

// finish ends j, acknowledged by a: "A finished job's row is deleted" (storage.md,
// queue_jobs); its record "stay[s] for readers". It runs under the lock.
func (b *fake) finish(a *fakeAttempt) {
	j := a.j
	j.current, j.done = nil, true
	if a.holdDeadline != nil {
		a.holdDeadline.Stop()
	}
	_, _ = b.db.Exec(`DELETE FROM queue_jobs WHERE channel = ? AND "offset" = ?`, a.q.id, j.rec.offset)
	b.seen.completed++
	b.sweep(a.q)
}

// workerGone ends the attempts c held as its worker goes: "The wait starts
// at the attempt's end, at [...] the worker's going"; "An attempt that ends
// with neither comes back unspent". It runs under the lock.
func (b *fake) workerGone(c *fakeConn) {
	for _, id := range slices.Sorted(maps.Keys(b.queues)) {
		for _, j := range slices.Clone(b.queues[id].jobs) { // a move takes a job out
			if a := j.current; a != nil && a.conn == c {
				if b.fault == unspentSpent {
					b.spend(a)
				}
				b.seen.workerGone++
				b.fail(a)
			}
		}
	}
}

// respond answers p, a response to the queue q on c, from s: "ack or
// return a job, with its attempt's Correlation Data. Needs consume (else
// 0x87). No answer: the PUBACK (0x00) confirms receipt only. A response
// naming an attempt that is not current or not this session's, or with
// another payload, changes nothing and is logged. Taken at QoS 0 too"; a
// QoS 2 one "runs on arrival", its identifier held until its PUBREL. It
// reports whether the connection goes on. It runs under the lock (owns).
func (b *fake) respond(c *fakeConn, s *fakeSession, q *fakeQueue, p *harness.Publish) bool {
	if !b.fakeConsumes(c.user, q.c.Name) {
		b.seen.unauthorized++
		return b.acknowledgeRequest(c, p, 0x87)
	}
	cd, _ := p.Props.Get(harness.CorrelationData)
	var a *fakeAttempt
	for _, j := range q.jobs {
		if b.fault == staleAck && !j.done {
			for _, old := range j.attempts {
				if old.id == string(cd.Data) && old.session == s {
					a = old
				}
			}
		}
		if j.current != nil && j.current.id == string(cd.Data) && (j.current.session == s || b.fault == foreignAck) {
			a = j.current
		}
	}
	switch payload := string(p.Payload); {
	case a == nil || (payload != "ack" && payload != "return"):
		b.seen.staleResponses++
	case payload == "ack":
		b.spend(a)
		if s.subs[workerTopic(q.c.Name)] == nil {
			b.seen.unsubscribedAck++
		}
		b.finish(a)
	default:
		b.spend(a)
		b.seen.returned++
		b.fail(a)
	}
	if !b.acknowledgeRequest(c, p, 0) {
		return false
	}
	b.dispatch()
	return true
}

// queueNow is the queue's clock, held still where a scene must observe a
// record before retention can move it. Intake, attempt and move timestamps
// use the same clock. It runs under the lock.
func (b *fake) queueNow() time.Time {
	if !b.queueTime.IsZero() {
		return b.queueTime
	}
	return time.Now()
}

// pastRetention reports whether j is past its queue's retention_period
// (design.md, Channels, queue: "expired when the job is past
// retention_period").
func (b *fake) pastRetention(q *fakeQueue, j *fakeJob) bool {
	period := q.c.RetentionPeriod
	if b.fault == earlyExpired {
		period /= 2
	}
	return b.queueNow().Sub(time.UnixMilli(j.rec.ms)) >= period
}

// move moves j to q's dead-letter channel as reason, in one transaction
// with its row: design.md, Channels, queue: "It moves the job to the
// append channel <channel>__dlq in one transaction. The topic gains the
// prefix __dlq/ [...] The moved job carries saguin-dlq-reason [...] A
// dead letter's age, for that channel's retention, counts from the move.
// Its Message Expiry stays as it was published"; Sagüin properties:
// saguin-dlq-reason, -channel, -offset, -attempts, -at, and "where an
// attempt was spent -first and -last". It runs under the lock.
func (b *fake) move(q *fakeQueue, j *fakeJob, reason string) {
	if b.fault == alwaysEvicted && reason == "expired" {
		reason = "evicted"
	}
	r := j.rec
	j.done, j.moved, j.current = true, true, nil
	q.jobs = slices.DeleteFunc(q.jobs, func(o *fakeJob) bool { return o == j })
	now := b.queueNow()
	b.offset++
	props := slices.Clone(r.props)
	props = append(props, harness.User("saguin-dlq-reason", reason), harness.User("saguin-dlq-channel", q.c.Name),
		harness.User("saguin-dlq-offset", strconv.FormatInt(r.offset, 10)), harness.User("saguin-dlq-attempts", strconv.Itoa(j.spent)),
		harness.User("saguin-dlq-at", strconv.FormatInt(now.UnixMilli(), 10)))
	if j.spent > 0 {
		props = append(props, harness.User("saguin-dlq-first", strconv.FormatInt(j.first, 10)), harness.User("saguin-dlq-last", strconv.FormatInt(j.last, 10)))
	}
	dead := &fakeRecord{channel: q.dlq, id: r.id, ms: now.UnixMilli(), offset: b.offset, topic: "__dlq/" + r.topic, qos: r.qos, publisher: r.publisher,
		payload: r.payload, props: props, received: r.received}
	tx, err := b.db.Begin()
	if err != nil {
		return // a lost write shows in the runner as a job neither finished nor dead
	}
	_, _ = tx.Exec(`DELETE FROM records WHERE channel = ? AND "offset" = ?`, q.id, r.offset)
	if b.fault != deadAndKept {
		_, _ = tx.Exec(`DELETE FROM queue_jobs WHERE channel = ? AND "offset" = ?`, q.id, r.offset)
	}
	_, _ = tx.Exec(`INSERT INTO records (channel, "offset", id, time, topic, qos, publisher, expires, properties, payload, payload_size, published_retained, retained)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 0, 0)`, q.dlq, dead.offset, dead.id, dead.ms, dead.topic, dead.qos, dead.publisher, nonNil(r.payload), len(r.payload))
	_ = tx.Commit()
	b.unrecord(q.id, r)
	b.seen.deadLettered[reason]++
	b.distribute(dead)
}

// nonNil is p, or an empty slice where p is nil, which binds as an empty
// BLOB, not NULL (storage.md: payload is NOT NULL).
func nonNil(p []byte) []byte {
	if p == nil {
		return []byte{}
	}
	return p
}

// unrecord drops r, a record of channel id that is moved or deleted, and
// the deliveries queued for it and not yet handed over, its charge with it.
// It runs under the lock.
func (b *fake) unrecord(id int64, r *fakeRecord) {
	b.records = slices.DeleteFunc(b.records, func(o *fakeRecord) bool { return o == r })
	_, charge := b.room(r.topic, r.payload, r.props)
	b.bytes[id] -= charge
	for _, s := range b.sessions {
		s.queue = slices.DeleteFunc(s.queue, func(d *delivery) bool { return d.rec == r })
	}
}

// sweep applies q's retention: its records past retention_period, and its
// oldest beyond retention_bytes, held jobs not counted ("Held jobs do not
// count toward retention_bytes"), are deleted where finished and moved
// where not ("Retention never deletes an unfinished job"; "A held job
// moves when its hold ends"): as expired past retention_period, else
// evicted. It runs under the lock.
func (b *fake) sweep(q *fakeQueue) {
	limit := int64(q.c.RetentionBytes)
	var size int64
	for _, j := range q.jobs {
		if j.current == nil || !j.current.held {
			_, charge := b.room(j.rec.topic, j.rec.payload, j.rec.props)
			size += charge
		}
	}
	for _, j := range slices.Clone(q.jobs) {
		held := j.current != nil && j.current.held
		past := b.pastRetention(q, j)
		_, charge := b.room(j.rec.topic, j.rec.payload, j.rec.props)
		switch {
		case held:
			continue
		case !past && (limit == 0 || size <= limit):
			continue
		case j.done:
			_, _ = b.db.Exec(`DELETE FROM records WHERE channel = ? AND "offset" = ?`, q.id, j.rec.offset)
			b.unrecord(q.id, j.rec)
			b.seen.retention++
		case past:
			b.move(q, j, "expired")
		default:
			b.move(q, j, "evicted")
		}
		size -= charge
		q.jobs = slices.DeleteFunc(q.jobs, func(o *fakeJob) bool { return o == j })
	}
}

// sweepLater sweeps every queue every 50 milliseconds while the fake
// listens, so age retention comes due without a job's own event.
func (b *fake) sweepLater() {
	time.AfterFunc(50*time.Millisecond, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.stopped {
			return
		}
		for _, id := range slices.Sorted(maps.Keys(b.queues)) {
			b.sweep(b.queues[id])
		}
		b.dispatch()
		b.sweepLater()
	})
}

// fakeResponseQueue is the queue topic responds to, if it is a response:
// $saguin/queue/<channel>/response of a queue.
func (b *fake) fakeResponseQueue(topic string) *fakeQueue {
	name, ok := strings.CutSuffix(strings.TrimPrefix(topic, "$saguin/queue/"), "/response")
	if !ok || !strings.HasPrefix(topic, "$saguin/queue/") {
		return nil
	}
	return b.fakeQueueNamed(name)
}

// sceneQueue is the queue's settings for a scene (scene_test.go): a
// minute's retention, a hold of half a minute and a retry wait of a
// millisecond, so that no timer ends a hold or moves a job while the scene
// goes on, unless the scene says otherwise.
var sceneQueue = queueConfig{retentionPeriod: time.Minute, visibility: 30 * time.Second, retryMode: "linear", retryDelay: time.Millisecond, maxAttempts: 2}

// worker has s subscribe to the queue's worker topic (subscribeWorker).
func (sc *scene) worker(s *slot) error {
	if err := sc.step(); err != nil {
		return err
	}
	return sc.f.subscribeWorker(s)
}

// job has s publish a job at QoS 1 on a topic of its own, with props, and
// reads its PUBACK; the fake offers it to a worker before (dispatch).
func (sc *scene) job(s *slot, props ...harness.Property) error {
	if err := sc.step(); err != nil {
		return err
	}
	sc.n++
	topic := queueName + "/" + strconv.Itoa(sc.n)
	o := &out{p: &harness.Publish{QoS: 1, Topic: topic, ID: s.nextID(), Props: props},
		m: &message{step: sc.f.step, publisher: s.clientID, topic: topic, qos: 1}}
	if err := sc.f.fill(o, 0, 1, 4, letters, s.conn.Version); err != nil {
		return err
	}
	if err := sc.f.placeAll([]*out{o}); err != nil {
		return err
	}
	sc.f.expect(o)
	if err := send(s, o); err != nil {
		return err
	}
	return collect(s)
}

// offers has s ping, reading past what the fake queued for it before the
// PINGRESP, and returns the attempts its connection has read.
func (sc *scene) offers(s *slot) ([]*jobAttempt, error) {
	if err := sc.step(); err != nil {
		return nil, err
	}
	return s.incoming.attempts, pingPast(s)
}

// awaitOffer reads s's connection until it has read n attempts, each
// packet waited for the runner's -timeout at most: the broker's retry
// timer offers the job again.
func (sc *scene) awaitOffer(s *slot, n int) error {
	if err := sc.step(); err != nil {
		return err
	}
	for len(s.incoming.attempts) < n {
		if _, err := s.read(); err != nil {
			return err
		}
	}
	return nil
}

// puback has s acknowledge every offer it holds with PUBACK 0x00 and
// answer none, then ping, past which the fake took them.
func (sc *scene) puback(s *slot) error { return sc.pubackWith(s, 0) }

// pubackWith is puback, the PUBACKs carrying reason.
func (sc *scene) pubackWith(s *slot, reason byte) error {
	if err := sc.step(); err != nil {
		return err
	}
	for _, a := range s.incoming.attempts {
		a.plan = plan{reason: reason}
	}
	if err := s.releaseAcks(); err != nil {
		return err
	}
	return pingPast(s)
}

// respond has s send payload at QoS 1 naming a's attempt, and reads its
// answer (sendResponses).
func (sc *scene) respond(s *slot, a *jobAttempt, payload string) error {
	if err := sc.step(); err != nil {
		return err
	}
	return s.sendResponses([]*response{s.newResponse(queueName, a, []byte(a.id), payload, 1)})
}

// TestQueueJobLostIsCaught: storage.md, queue_jobs: "This table holds a
// queue's unfinished jobs [...] A finished job's row is deleted"; design.md,
// Channels, queue: retention "moves the job"; $saguin topics: ack "a job,
// with its attempt's Correlation Data". A worker takes a job and
// acknowledges its offer with PUBACK 0x00, answering nothing; a broker
// finishing the job then, no ack having come, fails the record check that
// follows, the job neither unfinished nor dead-lettered.
func TestQueueJobLostIsCaught(t *testing.T) {
	caughtInScene(t, jobLost, sceneQueue, "no worker it went to answered ack", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		if err == nil {
			_, err = sc.offers(w)
		}
		if err == nil {
			err = sc.puback(w)
		}
		if err != nil {
			return err
		}
		return sc.check()
	})
}

// TestQueueDeadAndKeptIsCaught: storage.md, queue_jobs: "This table holds
// a queue's unfinished jobs. The record stays in records [...] When
// retention ends a job, the job moves to the dead-letter channel in one
// transaction with its row"; design.md, Delivery, Queue jobs: "When an
// attempt fails with retry.max_attempts spent, the job moves to the
// dead-letter channel". With one attempt allowed, a worker acknowledges its
// offer and answers return; a broker moving the job but keeping its row,
// its record gone, fails the record check that follows on the row left
// without its record.
func TestQueueDeadAndKeptIsCaught(t *testing.T) {
	q := sceneQueue
	q.maxAttempts = 1
	caughtInScene(t, deadAndKept, q, "with no record", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		var as []*jobAttempt
		if err == nil {
			as, err = sc.offers(w)
		}
		if err == nil {
			err = sc.puback(w)
		}
		if err == nil {
			err = sc.respond(w, as[0], "return")
		}
		if err == nil {
			err = sc.check()
		}
		if err != nil {
			return err
		}
		return sc.f.readDeadLetters()
	})
}

// TestQueueTwoWorkersIsCaught: design.md, Channels, queue: "Each job goes
// to one worker at a time"; Delivery, Queue jobs: "The hold starts at the
// worker's PUBACK and lasts visibility_timeout". Two workers subscribe and
// a job comes; the first, offered it, acknowledges it with PUBACK 0x00. A
// broker offering the held job to the second worker too fails the run when
// that offer is read, the first's hold running.
func TestQueueTwoWorkersIsCaught(t *testing.T) {
	caughtInScene(t, twoWorkers, sceneQueue, "each job goes to one worker at a time", func(sc *scene) error {
		a, err := sc.client(0, "ann", false, false)
		var b, p *slot
		if err == nil {
			b, err = sc.client(1, "ann", false, false)
		}
		if err == nil {
			err = sc.worker(a)
		}
		if err == nil {
			err = sc.worker(b)
		}
		if err == nil {
			p, err = sc.client(2, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		if err == nil {
			_, err = sc.offers(a)
		}
		if err == nil {
			err = sc.puback(a)
		}
		if err != nil {
			return err
		}
		_, err = sc.offers(b)
		return err
	})
}

// TestQueueSecondJobIsCaught: design.md, Delivery, Queue jobs: "each worker
// receives one job at a time per queue". One worker subscribes and two
// jobs come. A broker giving it the second while it holds the first fails
// the record check after, neither job having moved.
func TestQueueSecondJobIsCaught(t *testing.T) {
	caughtInScene(t, secondJob, sceneQueue, "each worker receives one job at a time per queue", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		for range 2 {
			if err == nil {
				err = sc.job(p)
			}
		}
		if err == nil {
			_, err = sc.offers(w)
		}
		if err != nil {
			return err
		}
		return sc.check()
	})
}

// TestQueueReusedAttemptIsCaught: design.md, Words: Attempt, "an attempt is
// one try at giving a queue job to a worker. It carries a broker-made id as
// its Correlation Data"; Delivery, Queue jobs: "A failed attempt is
// offered again after a wait set by retry.mode". A worker acknowledges its
// offer and answers return, and waits for the job's next offer. A broker
// offering it under its last attempt's id fails the run when it comes.
func TestQueueReusedAttemptIsCaught(t *testing.T) {
	caughtInScene(t, reusedAttempt, sceneQueue, "each attempt carries a broker-made id of its own", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		var as []*jobAttempt
		if err == nil {
			as, err = sc.offers(w)
		}
		if err == nil {
			err = sc.puback(w)
		}
		if err == nil {
			err = sc.respond(w, as[0], "return")
		}
		if err != nil {
			return err
		}
		return sc.awaitOffer(w, 2)
	})
}

// TestQueueOfferExpiryIsCaught: design.md, Delivery, Service publications:
// a worker offer "does not inherit the job publication's Message Expiry".
// A job published with a Message Expiry comes to a worker. A broker
// sending its offer with that Message Expiry fails the run when it is
// read.
func TestQueueOfferExpiryIsCaught(t *testing.T) {
	caughtInScene(t, offerExpiry, sceneQueue, "does not inherit the job publication's Message Expiry", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p, harness.Property{ID: harness.MessageExpiryInterval, Int: expiryFloor})
		}
		if err != nil {
			return err
		}
		_, err = sc.offers(w)
		return err
	})
}

// TestQueueForeignAckIsCaught: design.md, $saguin topics: "A response
// naming an attempt that is not current or not this session's [...]
// changes nothing". A worker holds a job, acknowledged with PUBACK 0x00;
// another client, allowed to consume, answers ack naming its attempt. A
// broker letting that ack complete the job fails the record check that
// follows, the job finished with no ack from the worker it went to.
func TestQueueForeignAckIsCaught(t *testing.T) {
	caughtInScene(t, foreignAck, sceneQueue, "no worker it went to answered ack", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		var as []*jobAttempt
		if err == nil {
			as, err = sc.offers(w)
		}
		if err == nil {
			err = sc.puback(w)
		}
		if err == nil {
			err = sc.respond(p, as[0], "ack")
		}
		if err != nil {
			return err
		}
		return sc.check()
	})
}

// TestQueueNoRetryWaitIsCaught: design.md, Delivery, Queue jobs: "A failed
// attempt is offered again after a wait set by retry.mode [...] The wait
// starts at the attempt's end". The queue waits ten seconds before a
// retry; a worker acknowledges its offer and answers return, then pings.
// A broker offering the job again at once fails the run when the offer
// comes; one keeping to the wait sends nothing before the PINGRESP.
func TestQueueNoRetryWaitIsCaught(t *testing.T) {
	q := sceneQueue
	q.retryDelay = 10 * time.Second
	caughtInScene(t, noRetryWait, q, "The wait starts at the attempt's end", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		var as []*jobAttempt
		if err == nil {
			as, err = sc.offers(w)
		}
		if err == nil {
			err = sc.puback(w)
		}
		if err == nil {
			err = sc.respond(w, as[0], "return")
		}
		if err != nil {
			return err
		}
		_, err = sc.offers(w)
		return err
	})
}

// TestQueueUnspentSpentIsCaught: design.md, Delivery, Queue jobs: "An
// attempt is spent at the worker's PUBACK or its response naming the
// attempt, whichever is first [...] An attempt that ends with neither comes
// back unspent". With one attempt allowed, a worker reads its offer and
// drops its connection with neither PUBACK nor answer. A broker counting
// that attempt spent moves the job as attempts_exhausted, and fails the
// run when the reader of the dead letters reads one carrying an attempt
// spent that no worker could have spent; one keeping to the rule moves
// nothing.
func TestQueueUnspentSpentIsCaught(t *testing.T) {
	q := sceneQueue
	q.maxAttempts = 1
	caughtInScene(t, unspentSpent, q, "only a PUBACK or a response naming an attempt spends it", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		if err == nil {
			_, err = sc.offers(w)
		}
		if err == nil {
			err = sc.drop(w)
		}
		if err == nil {
			err = sc.check()
		}
		if err != nil {
			return err
		}
		return sc.f.readDeadLetters()
	})
}

// TestQueueReasonIsCaught: design.md, Channels, queue: "Its value is
// expired when the job is past retention_period"; "When several
// dead-letter causes apply, expired wins, then attempts_exhausted, then
// evicted." Advancing the fake's queue clock first halfway through
// retention_period, then past it, exposes both an early move and a move
// wrongly labelled evicted.
func TestQueueReasonIsCaught(t *testing.T) {
	for _, c := range []struct {
		fault fault
		want  string
	}{{alwaysEvicted, "expired wins"}, {earlyExpired, "less than retention_period"}} {
		t.Run(string(c.fault), func(t *testing.T) { queueReasonScene(t, c.fault, c.want, true, 1) })
	}
}

// TestQueueUnobservedReasonIsCaught uses the same moves without reading
// the original record or delivering it. The observer's write-to-answer
// window must catch both faults, at QoS 1 and at QoS 0 followed by PINGREQ.
func TestQueueUnobservedReasonIsCaught(t *testing.T) {
	for _, c := range []struct {
		fault fault
		want  string
	}{{alwaysEvicted, "expired wins"}, {earlyExpired, "less than retention_period"}} {
		for _, qos := range []byte{0, 1} {
			t.Run(string(c.fault)+"/QoS"+strconv.Itoa(int(qos)), func(t *testing.T) { queueReasonScene(t, c.fault, c.want, false, qos) })
		}
	}
}

// TestQueueEvictionIsReached: design.md, Channels, queue: "Its value is
// [...] evicted when retention_bytes causes the move". Jobs published with
// no worker to take them fill the queue past its retention_bytes, and the
// fake's size pass, run as each is stored, evicts the oldest; the last
// reader of the dead letters finds one dead-lettered as evicted, and checks
// it as it checks every dead letter. A random run reaches this only when a
// burst lands on the queue before its jobs are taken, which load can
// prevent (TestRunPasses).
func TestQueueEvictionIsReached(t *testing.T) {
	err := playScene(t, "", sceneQueue, func(sc *scene) error {
		p, err := sc.client(0, "cy", false, false)
		if err != nil {
			return err
		}
		for range 200 { // far more jobs than retention_bytes holds
			sc.b.mu.Lock()
			evicted := sc.b.seen.deadLettered["evicted"]
			sc.b.mu.Unlock()
			if evicted != 0 {
				break
			}
			if err := sc.job(p); err != nil {
				return err
			}
		}
		if err := sc.check(); err != nil {
			return err
		}
		if err := sc.f.readDeadLetters(); err != nil {
			return err
		}
		if sc.f.queues.stats.reasons["evicted"] == 0 {
			return fmt.Errorf("the last reader found no job dead-lettered as evicted, of %d jobs published", sc.n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// queueReasonScene advances the fake's queue clock, observing the original
// record first only where observe is true. Otherwise only the write and
// the broker's answer bound intake.
// design.md, PUBLISH: "The record's timestamp is the core's intake time."
func queueReasonScene(t *testing.T, fault fault, want string, observe bool, qos byte) {
	t.Helper()
	q := sceneQueue
	q.retentionPeriod, q.visibility = time.Minute, time.Second
	caughtInScene(t, fault, q, want, func(sc *scene) error {
		p, err := sc.client(1, "cy", false, false)
		if err != nil {
			return err
		}
		o, err := sc.newOut(p, queueName+"/unobserved", qos, false)
		if err != nil {
			return err
		}
		if err := sc.sendOut(p, o); err != nil {
			return err
		}
		sc.b.mu.Lock()
		sc.b.queueTime = time.Now().Truncate(time.Millisecond)
		sc.b.mu.Unlock()
		if observe {
			if err := sc.check(); err != nil {
				return err
			}
		}
		for i := range 2 {
			if err := sc.step(); err != nil {
				return err
			}
			sc.b.mu.Lock()
			sc.b.queueTime = sc.b.queueTime.Add(q.retentionPeriod/2 + 2*queueMargin)
			sc.b.sweep(sc.b.fakeQueueNamed(queueName))
			sc.b.mu.Unlock()
			if observe || i == 1 {
				if !observe {
					for _, j := range sc.f.queues.jobs {
						if j.m.sentAt.Load() == 0 || j.m.acknowledgedAt.Load() < j.m.sentAt.Load() {
							t.Fatal("the publisher has no observed intake window")
						}
						if j.m.seen.Load() != nil || j.m.stored.Load() != nil {
							t.Fatal("the scene observed the original job before its move")
						}
					}
				}
				if err := sc.check(); err != nil {
					return err
				}
			}
		}
		return sc.f.readDeadLetters()
	})
}

// TestQueueFreshResendIDIsCaught: design.md, Delivery, A resend: "A resend
// uses the Subscription Identifiers and delivery properties saved for its
// first transmission"; storage.md, session_inflight: attempt, "so a resend
// is the same message"; CONNECT, 9: in-flight messages are resent "with
// their packet identifiers". A worker holding a durable session reads its
// offer, drops its connection without acknowledging it and reconnects
// keeping its session. A broker resending the offer under a new attempt id
// fails the run when the resend comes.
func TestQueueFreshResendIDIsCaught(t *testing.T) {
	caughtInScene(t, freshResendID, sceneQueue, "a resend is the same message", func(sc *scene) error {
		w, err := sc.client(0, "ann", true, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		if err == nil {
			_, err = sc.offers(w)
		}
		if err == nil {
			err = sc.drop(w)
		}
		if err != nil {
			return err
		}
		_, err = sc.client(0, "ann", true, true)
		return err
	})
}

// TestQueueOffersAfterUnsubscribeAreCaught: design.md, Delivery, Queue
// jobs: "A worker that unsubscribes keeps the job it holds until it
// answers, the hold ends or it goes. Its offers not yet sent are
// cancelled". A worker unsubscribes; a job comes, then, once the worker has
// answered whatever it was offered, a second. A broker that keeps offering
// it jobs fails the run when the second offer comes; one keeping to the
// rule offers it none.
func TestQueueOffersAfterUnsubscribeAreCaught(t *testing.T) {
	caughtInScene(t, offersAfterUnsubscribe, sceneQueue, "Its offers not yet sent are cancelled", func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		if err == nil {
			err = sc.step()
		}
		if err == nil {
			err = sc.f.unsubscribeFrom(w, workerTopic(queueName))
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		var as []*jobAttempt
		if err == nil {
			as, err = sc.offers(w)
		}
		if err == nil && len(as) > 0 {
			if err = sc.puback(w); err == nil {
				err = sc.respond(w, as[0], "ack")
			}
		}
		if err == nil {
			err = sc.job(p)
		}
		if err != nil {
			return err
		}
		_, err = sc.offers(w)
		return err
	})
}

// TestQueueHoldTimeoutPasses: design.md, Delivery, Queue jobs: "The hold
// starts at the worker's PUBACK and lasts visibility_timeout [...] A failed
// attempt is offered again after a wait set by retry.mode [...] at [...]
// its timeout". Two workers subscribe; the first takes a job, acknowledges
// it with PUBACK 0x00 and answers nothing; the second waits for the job's
// next offer, which follows the first's hold, a second, and its retry
// wait. The runner holds that offer to its rules (takeOffer) and finds
// nothing wrong: its model lets a hold run out, at the earliest a
// visibility_timeout after the PUBACK was sent.
func TestQueueHoldTimeoutPasses(t *testing.T) {
	q := sceneQueue
	q.visibility = time.Second
	err := playScene(t, "", q, func(sc *scene) error {
		a, err := sc.client(0, "ann", false, false)
		var b, p *slot
		if err == nil {
			b, err = sc.client(1, "ann", false, false)
		}
		if err == nil {
			err = sc.worker(a)
		}
		if err == nil {
			err = sc.worker(b)
		}
		if err == nil {
			p, err = sc.client(2, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		if err == nil {
			_, err = sc.offers(a)
		}
		if err == nil {
			err = sc.puback(a)
		}
		if err != nil {
			return err
		}
		return sc.awaitOffer(b, 1)
	})
	if err != nil {
		t.Error(err)
	}
}

// TestQueueRefusingPubackPasses: design.md, Delivery, Queue jobs: "A
// refusing PUBACK (reason 0x80 or above) spends the attempt and ends it as
// a failure, as return does"; "A failed attempt is offered again after a
// wait set by retry.mode". A worker refuses its offer with PUBACK 0x83 and
// waits for the job's next offer, which the runner holds to its rules and
// finds nothing wrong.
func TestQueueRefusingPubackPasses(t *testing.T) {
	err := playScene(t, "", sceneQueue, func(sc *scene) error {
		w, err := sc.client(0, "ann", false, false)
		if err == nil {
			err = sc.worker(w)
		}
		var p *slot
		if err == nil {
			p, err = sc.client(1, "cy", false, false)
		}
		if err == nil {
			err = sc.job(p)
		}
		if err == nil {
			_, err = sc.offers(w)
		}
		if err == nil {
			err = sc.pubackWith(w, 0x83)
		}
		if err != nil {
			return err
		}
		return sc.awaitOffer(w, 2)
	})
	if err != nil {
		t.Error(err)
	}
}

// TestQueueCountersAreChecked: metrics.md, Queues:
// saguin_queue_dead_lettered_total, "jobs moved to <channel>__dlq: expired,
// evicted or attempts_exhausted"; saguin_queue_attempts_failed_total,
// "return (a refusing PUBACK included), timeout [...] or worker_gone";
// saguin_queue_delivered_total, "attempts handed to workers";
// saguin_queue_acknowledged_total, "ack responses". The fake broker serves
// no /metrics, so the check a run against the broker makes is held here to
// scrapes made up around what a run saw: those a broker keeping to
// metrics.md could give pass, and one leaving a move uncounted, a return
// beyond those sent, fewer attempts than the workers read, or more acks
// than were sent, fails naming its series.
func TestQueueCountersAreChecked(t *testing.T) {
	st := &queueStats{reasons: map[string]int{"expired": 2, "evicted": 5, "attempts_exhausted": 1}, returns: 3, refusing: 1, puback0: 4, offers: 9, acks: 6, completed: 4}
	scrape := func(dead [3]float64, returned, timedOut, delivered, acked float64) testkit.Exposition {
		e := testkit.Exposition{Values: map[string]float64{}}
		for i, reason := range []string{"expired", "evicted", "attempts_exhausted"} {
			e.Values[testkit.SeriesKey(deadLetteredTotal, "channel", queueName, "reason", reason)] = dead[i]
		}
		e.Values[testkit.SeriesKey(attemptsFailedTotal, "channel", queueName, "cause", "return")] = returned
		e.Values[testkit.SeriesKey(attemptsFailedTotal, "channel", queueName, "cause", "timeout")] = timedOut
		e.Values[testkit.SeriesKey(deliveredTotal, "channel", queueName)] = delivered
		e.Values[testkit.SeriesKey(acknowledgedTotal, "channel", queueName)] = acked
		return e
	}
	before := scrape([3]float64{2, 4, 1}, 0, 0, 0, 0)
	if err := checkQueueCounters(before, scrape([3]float64{2, 5, 1}, 4, 4, 9, 5), queueName, st); err != nil {
		t.Errorf("counters a broker keeping to metrics.md could give: %v", err)
	}
	for _, c := range []struct {
		after testkit.Exposition
		want  string
	}{
		{scrape([3]float64{2, 4, 1}, 4, 4, 9, 5), deadLetteredTotal},
		{scrape([3]float64{2, 5, 1}, 5, 4, 9, 5), attemptsFailedTotal},
		{scrape([3]float64{2, 5, 1}, 4, 4, 8, 5), deliveredTotal},
		{scrape([3]float64{2, 5, 1}, 4, 4, 9, 7), acknowledgedTotal},
	} {
		if err := checkQueueCounters(before, c.after, queueName, st); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("counters breaking %s: %v; want an error naming it", c.want, err)
		}
	}
}

// TestQueueStaleAckIsCaught checks design.md, $saguin topics: "A response
// naming an attempt that is not current or not this session's, or with
// another payload, changes nothing and is logged." A worker answers an
// ended attempt after its hold expired or its connection ended. The fake
// accepting that stale ack must fail the runner's job-state check.
func TestQueueStaleAckIsCaught(t *testing.T) {
	for _, end := range []string{"hold expired", "worker reconnected", "later attempt"} {
		t.Run(end, func(t *testing.T) {
			q := sceneQueue
			q.visibility = 100 * time.Millisecond
			q.retryDelay = 10 * time.Second
			if end == "later attempt" {
				q.retryDelay = time.Millisecond
			}
			caughtInScene(t, staleAck, q, "no worker it went to answered ack naming a current attempt", func(sc *scene) error {
				w, p := sc.f.slots[0], sc.f.slots[1]
				var old *jobAttempt
				return do(
					func() error { return sc.connect(w, "ann", durable) },
					func() error { return sc.worker(w) },
					func() error { return sc.connect(p, "cy", nil) },
					func() error { return sc.job(p) },
					func() error { return sc.read(w) },
					func() error {
						old = w.incoming.attempts[0]
						if end == "worker reconnected" {
							return do(func() error { return sc.drop(w) }, func() error { return sc.connect(w, "ann", resumed) })
						}
						if err := sc.puback(w); err != nil {
							return err
						}
						if end == "later attempt" {
							return sc.awaitOffer(w, 2)
						}
						// PINGRESP above bounds the hold start; wait beyond that
						// bound, before the configured retry can offer again.
						time.Sleep(time.Until(old.pubackThrough.Add(q.visibility + 2*queueMargin)))
						return nil
					},
					func() error { return sc.respond(w, old, "ack") }, sc.check)
			})
		})
	}
}

// TestDeadLetterDiscoveryKeepsPosition: design.md, Delivery: "QoS 0
// settles on successful writing"; SUBSCRIBE: "A SUBSCRIBE repeating an
// existing filter replaces its options and keeps its position and
// live-start boundaries". Discovering a dead letter after a reconnect
// cannot owe its earlier QoS 0 delivery again.
func TestDeadLetterDiscoveryKeepsPosition(t *testing.T) {
	if err := playScene(t, "", sceneQueue, deadLetterDiscoveryScene(true)); err != nil {
		t.Fatal(err)
	}
}

// TestOwedDeadLetterIsCaught: design.md, Channels, queue: "Its readers
// start at the floor"; SUBSCRIBE: "A consumer has one position per
// channel, never one per filter". A connection continuously subscribed
// before a job is published must receive its dead letter. Discovering it
// still owes the delivery; dropping it must fail the normal checkpoint.
func TestOwedDeadLetterIsCaught(t *testing.T) {
	caughtInScene(t, lostDeadLetter, sceneQueue, "still owed at checkpoint", deadLetterDiscoveryScene(false))
}

func deadLetterDiscoveryScene(reconnect bool) func(*scene) error {
	return func(sc *scene) error {
		s, err := sc.client(0, "ann", true, false)
		if err != nil {
			return err
		}
		if err := sc.subscribe(s, harness.Subscription{Filter: "#", QoS: 0, RetainHandling: 2}); err != nil {
			return err
		}
		p, err := sc.client(1, "cy", false, false)
		if err != nil {
			return err
		}
		// Check the initial floor before the job exists: the later
		// obligation must come from the move, not an unchecked replay.
		if err := sc.checkpoint(); err != nil {
			return err
		}
		if err := sc.publish(p, queueName+"/discovery", false); err != nil {
			return err
		}
		sc.b.mu.Lock()
		q := sc.b.fakeQueueNamed(queueName)
		sc.b.move(q, q.jobs[0], "evicted")
		sc.b.mu.Unlock()
		if reconnect {
			if err := sc.read(s); err != nil {
				return err
			}
			if err := sc.drop(s); err != nil {
				return err
			}
			if _, err := sc.client(0, "ann", true, true); err != nil {
				return err
			}
			if err := sc.subscribe(s, harness.Subscription{Filter: "#", QoS: 0, RetainHandling: 2}); err != nil {
				return err
			}
		}
		if err := sc.check(); err != nil {
			return err
		}
		return sc.checkpoint()
	}
}
