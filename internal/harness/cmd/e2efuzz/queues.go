package main

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The queue (design.md, Channels, queue; Delivery, Queue jobs and Service
// publications; $saguin topics, the queue rows; Restart is not driven).
// The broker holds one queue, q, with age and byte retention, a hold
// shorter than its retention_period, a retry wait of milliseconds and a
// few attempts, and the dead-letter channel it derives, q__dlq. Clients
// publish jobs at every QoS (enqueue, and the PUBLISH draws that land on
// q), read the queue and its dead letters as append channels, and seek on
// the queue; MQTT 5 clients become workers (work, and the SUBSCRIBE draws
// of $saguin/queue/q and the filters refused beside it) and answer each
// offer they read as a plan drawn from its attempt says: a PUBACK 0x00, a
// refusing one, or none until they next release their acknowledgements;
// ack, return or another payload, before or after the PUBACK, at QoS 0, 1
// or 2, or nothing yet, the job held. Any client sends responses naming an
// attempt that is not its own or that it may hold no longer (respond),
// with and without consume. Connections end while holding jobs, and
// durable sessions come back to their offers.
//
// The model checks, without waiting on a clock, as far as the client can
// know:
//   - every offer: a QoS 1 delivery of a job of the run's, its payload as
//     published, to a session holding or having held a worker
//     subscription, with saguin-attempt, the attempt's id as its
//     Correlation Data (non-empty, never another attempt's), the queue's
//     Response Topic, no Message Expiry of its own, saguin-expires where
//     the publisher set an expiry, the Sagüin properties of its record, the
//     publisher's other properties, and the Subscription Identifier of the
//     worker's own subscription (takeOffer);
//   - a resend, DUP 1, on a later connection of the session, is the same
//     message: its attempt id, packet identifier and properties;
//   - each job goes to one worker at a time: a new attempt never comes
//     while an earlier attempt of the job may not have ended, nor after a
//     record check found the job finished; and one job at a time to each
//     worker, checked once a record check shows neither job moved
//     (suspect);
//   - a new attempt comes no earlier than retry.mode's wait after the
//     earliest moment the attempt before it could have ended, n counting
//     the attempts read before it (retryWait);
//   - after an UNSUBACK of the worker subscription, at most the one offer
//     handed to the writer before it may still come;
//   - every response gets PUBACK 0x00, or 0x87 without consume;
//   - in each record check's one read of the database, each job the run
//     knows stored is unfinished (its row in queue_jobs, its record in the
//     queue), dead-lettered once, or finished after an ack naming one of
//     its current attempts sent by the session that attempt went to; never two of
//     these, and never back once finished (checkQueues);
//   - every dead letter, in the database and wherever delivered: the job's
//     topic under __dlq/, its saguin-id and payload, saguin-dlq-reason
//     expired exactly where the move came retention_period after its
//     record's time, bounded by the publish/acknowledgement window when no
//     delivery or record check saw the job before its move;
//     saguin-dlq-attempts no more than the attempts its
//     workers could have spent and retry.max_attempts for
//     attempts_exhausted, -first and -last where one was spent; owed, as
//     an append channel's history, to the readers of q__dlq, who start at
//     the floor; and read whole at the run's end by a reader subscribing
//     then (readDeadLetters), whose reasons bracket the dead-letter
//     counters of a broker the run started (checkQueueCounters).
//
// Left out: round-robin by client id, which needs every worker's
// eligibility at each selection; whether an offer waits for an answered
// attempt's PUBACK; a dead letter's age for its channel's retention; the
// job-state gauges, which move with every timer; queue records owed to the
// queue's readers, whose retention moves or deletes them at any time; and a
// restart, which no run makes.

// The queue's settings (config.md, channels.<name>). Its retention moves
// jobs while the run goes on: a few seconds of age, and a few dozen small
// jobs' bytes, so the size pass every committed job schedules evicts the
// oldest unfinished jobs not held when a burst of publishers or a flood
// lands on it; a hold of a second, so holds run out in a run; retries
// waiting tens of milliseconds, linear, two attempts.
const (
	queueName           = "q"
	queueRetentionBytes = 1024
	// deadLetterSuffix ends a dead-letter channel's name, and
	// deadLetterPrefix begins its topics (design.md, Channels, queue: "It
	// moves the job to the append channel <channel>__dlq in one
	// transaction. The topic gains the prefix __dlq/, whatever the queue's
	// filter").
	deadLetterSuffix = "__dlq"
	deadLetterPrefix = "__dlq/"
	// deadLetterTag ends the tag of a job's dead letter, which carries the
	// job's payload and so its tag (fill).
	deadLetterTag = "!"
	// queueMargin is the slack every comparison of the client's clock with
	// one of the broker's deadlines grants, on the side that accepts more.
	queueMargin = 5 * time.Millisecond
)

// queueConfig is a queue's own settings.
type queueConfig struct {
	retentionPeriod, visibility, retryDelay time.Duration
	retryMode                               string
	maxAttempts                             int
}

// theQueue is q's settings: retention_period must be longer than
// visibility_timeout (config.md: "shorter than retention_period where one
// is set").
var theQueue = queueConfig{retentionPeriod: 3 * time.Second, visibility: time.Second, retryMode: "linear",
	retryDelay: 20 * time.Millisecond, maxAttempts: 2}

// retryWait is the wait before a job is offered again once n of its
// attempts have failed: design.md, Delivery, Queue jobs, "fixed
// (retry.delay), linear (retry.delay x n) or exponential (retry.delay x
// 2^(n-1)). Here n is the number of failed attempts so far".
func (q *queueConfig) retryWait(n int) time.Duration {
	switch q.retryMode {
	case "linear":
		return q.retryDelay * time.Duration(n)
	case "exponential":
		return q.retryDelay << max(n-1, 0)
	}
	return q.retryDelay
}

// workerTopic is the topic a worker of queue subscribes to, and
// responseTopic the one its responses go to (design.md, $saguin topics).
func workerTopic(queue string) string   { return "$saguin/queue/" + queue }
func responseTopic(queue string) string { return workerTopic(queue) + "/response" }

// queueLog is what the run has seen of the queue. Offers are read on the
// connections' readers, bursts' goroutines among them, so what more than
// one connection shares is kept under mu.
type queueLog struct {
	mu sync.Mutex
	// jobs are the jobs the run may have stored, by tag, written by the
	// driver alone; attempts every attempt read, by id.
	jobs     map[string]*job
	attempts map[string]*jobAttempt
	// ended is when the step that ended each connection holding an attempt
	// began, no later than the core took its end.
	ended map[*harness.Client]time.Time
	// suspects are offers of a second job to a worker that held one, each
	// a breach unless either job moved before (checkQueues).
	suspects []suspect
	// deadRead is the last offset of the dead-letter channel read, by its
	// channel id, and deadHeld how many dead letters it holds.
	deadRead map[int64]int64
	deadHeld map[int64]int
	stats    queueStats
}

// queueStats counts what the run did with the queue, for its last lines
// and the broker's counters.
type queueStats struct {
	jobs, offers, resent, workers, workerRefusals    int
	puback0, refusing, acks, returns, others, notOwn int
	completed, deadLetters, unsubscribedAt           int
	// reasons are the dead letters the run's last reader read, by reason.
	reasons map[string]int
}

// job is one job the run may have stored: its publication, its dead
// letter, the attempts of it read, and what the record checks found.
type job struct {
	m, dead  *message
	queue    string
	attempts []*jobAttempt
	// unfinishedAt is the serial of the last record check finding its row,
	// completedAt of the first finding neither row nor dead letter, deadAt
	// of the first finding its dead letter; 0 for none.
	unfinishedAt, completedAt, deadAt uint64
	foundStep                         int // first discovery of the dead letter, not its creation
	// acked: an ack naming an attempt not known ended was sent on its
	// original connection, with no later attempt already observed, so
	// the job may have finished; lateOffer: an offer of it was read after a
	// record check found it dead-lettered.
	acked, lateOffer bool
}

// jobAttempt is one offer of a job as its worker saw it (design.md, Words:
// Attempt, "one try at giving a queue job to a worker. It carries a
// broker-made id as its Correlation Data").
type jobAttempt struct {
	j        *job
	id       string
	session  *subscriptions
	conn     *harness.Client // the connection its first transmission came on
	packetID uint16
	readAt   time.Time
	props    harness.Properties // as first read, Topic Alias aside
	plan     plan
	// puback: its PUBACK was sent on conn, with pubackReason, pubackAt
	// before; spendable: that, or an ack or return naming it sent on conn
	// ("An attempt is spent at the worker's PUBACK or its response naming
	// the attempt, whichever is first").
	puback, spendable bool
	pubackReason      byte
	pubackAt          time.Time
	// pubackThrough is when a PINGRESP proved the broker had taken the
	// PUBACK, bounding the hold start from above.
	pubackThrough time.Time
	// ended is the first answer its client sent that ends it, "return",
	// "refused" or "ack", and endedAt the moment before; answered: the
	// response its plan holds back has gone.
	ended    string
	endedAt  time.Time
	answered bool
}

// plan is how a worker answers one offer, drawn from its attempt's id and
// the worker's client id, not from the generator: when an offer comes is
// the broker's, so the choice, like receipt's, is left out of the step
// log.
type plan struct {
	reason byte   // the PUBACK's reason: 0x00, or refusing
	first  string // a response sent before the PUBACK, "" for none
	// then is the response sent once the PUBACK 0x00 has gone; "" holds
	// the job until the worker's next work, which answers later, or, ""
	// too, never: the hold runs out.
	then, later string
	qos         byte // the responses' QoS
}

// refusingPubacks are the reason codes of 0x80 or above MQTT 5's Table 3-4
// gives a PUBACK (3.4.2.1).
var refusingPubacks = []byte{0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99}

// planFor draws a's plan, its responses at a QoS s's user may use.
func (s *slot) planFor(a *jobAttempt) plan {
	h := harness.TopicHash(s.id + " " + a.id)
	p := plan{then: "ack", later: "ack", qos: byte((h >> 4) % 3)}
	if denied(s.user, "qos2") {
		p.qos = min(p.qos, 1)
	}
	if (h>>12)%2 == 1 {
		p.later = "return"
	}
	switch h % 16 {
	case 4, 5:
		p.first, p.then = "ack", ""
	case 6, 7:
		p.then = "return"
	case 8, 9:
		p.reason, p.then = refusingPubacks[(h>>8)%uint64(len(refusingPubacks))], ""
	case 10, 11:
		p.then = ""
	case 12:
		p.then = "done" // another payload, which changes nothing
	case 13:
		p.first, p.then = "return", ""
	case 14, 15:
		p.then, p.later = "", ""
	}
	return p
}

// add keeps m, a publication the broker may store on a queue, as a job,
// once (a QoS 2 one is expected at its PUBREL), with the dead letter it
// may become: the job's record moved to the dead-letter channel, its topic
// under the prefix, its saguin-id and payload, kept among the publications
// so its deliveries are known by it.
func (f *fuzzer) addJob(m *message) {
	l := &f.queues
	if l.jobs[m.tag] != nil {
		return
	}
	j := &job{m: m, queue: m.channel}
	j.dead = &message{tag: m.tag + deadLetterTag, step: m.step, publisher: m.publisher, hidden: m.hidden,
		topic: deadLetterPrefix + m.topic, qos: m.qos, id: m.id, body: m.body, length: m.length, channel: m.channel + deadLetterSuffix,
		props: m.props, saguinIDs: m.saguinIDs, expiring: m.expiring, serial: m.serial, deadOf: j}
	m.job = j
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.jobs == nil {
		l.jobs, l.attempts, l.ended = map[string]*job{}, map[string]*jobAttempt{}, map[*harness.Client]time.Time{}
		l.deadRead, l.deadHeld = map[int64]int64{}, map[int64]int{}
	}
	l.jobs[m.tag] = j
	l.stats.jobs++
	f.publications[j.dead.tag] = j.dead
	f.channelRecords++
}

// outlastHolds: once the run's steps are done, a fresh MQTT 5 client, the
// queue's last worker, connects, subscribes as a worker and publishes a
// job, which goes to it or to another worker connected; every client
// releases its acknowledgements, and they wait past the queue's
// visibility_timeout before the last checkpoint, so the holds their workers
// never answered run out ("The hold starts at the worker's PUBACK and lasts
// visibility_timeout"), held jobs past retention_period move ("or when its
// hold ends"), and the offers that follow are read and checked; then the
// last worker reads what came and disconnects. Connections rarely live as
// long as a hold while the steps go on.
func (f *fuzzer) outlastHolds() error {
	p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "last-worker", CleanStart: true}, logins[0].user, logins[0].password)
	a, err := seal(p, noSession)
	if err != nil {
		return err
	}
	f.note("the queue's last worker: %s: want %s", describe(p), a.prediction)
	c, err := f.dial(harness.V5)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }() // checked closed by the broker, or the run has failed
	w := &slot{f: f, id: "last-worker", clientID: "last-worker", conn: c, subs: newSubscriptions(), user: p.Username, window: maxInflight}
	if err := c.SendRaw(a.wire); err != nil {
		return err
	}
	if err := accepted(w, a.prediction); err != nil {
		return err
	}
	if err := f.subscribeWorker(w); err != nil {
		return err
	}
	o, err := f.newPublish(w, 0, drawQueue)
	if err != nil {
		return err
	}
	o.p.QoS, o.m.qos, o.p.ID = 1, 1, w.nextID()
	if err := f.placeAll([]*out{o}); err != nil {
		return err
	}
	f.note("%s: %s", w.id, o)
	f.expect(o)
	if err := send(w, o); err != nil {
		return err
	}
	if err := collect(w); err != nil {
		return err
	}
	wait := theQueue.visibility + 4*theQueue.retryWait(theQueue.maxAttempts)
	f.note("the clients release their acknowledgements, their holds beginning, and wait %v, past the queue's visibility_timeout %v", wait, theQueue.visibility)
	for _, s := range append(slices.Clone(f.slots), w) {
		if s.conn == nil {
			continue
		}
		// The window its acknowledgements free takes an offer, which the
		// second release begins to hold.
		for range 2 {
			if err := s.releaseAcks(); err != nil {
				return err
			}
			if err := pingPast(s); err != nil {
				return err
			}
		}
		if err := s.releaseAcks(); err != nil {
			return err
		}
	}
	time.Sleep(wait)
	if err := pingPast(w); err != nil {
		return err
	}
	if err := w.releaseAcks(); err != nil {
		return err
	}
	f.note("%s: DISCONNECT: want the close", w.id)
	if err := c.Send(&harness.Disconnect{}); err != nil {
		return err
	}
	err = awaitClose(w)
	w.leaveAttempts()
	return err
}

// queueTopic draws a topic of the queue: q and up to two levels more.
func (f *fuzzer) queueTopic() string {
	more := f.rng.IntN(maxTopicLevels - 1)
	levels := make([]string, 1, 1+more)
	levels[0] = queueName
	for range more {
		levels = append(levels, []string{"a", "b", f.word(1, 3)}[f.rng.IntN(3)])
	}
	return strings.Join(levels, "/")
}

// enqueue: a connected client publishes one to four jobs, at QoS 0 or 1
// (QoS 2 ones come from exactlyOnce), and reads their answers, or ends its
// connection or is taken over first, as publish does.
func (f *fuzzer) enqueue() (bool, error) { return f.publishAs(drawQueue) }

// offerQueue is the queue p, a PUBLISH s's connection read, is an offer
// of, "" where it is none: a delivery of a queue's topic carrying
// saguin-attempt or the queue's Response Topic, which a reader's delivery
// of the record never does (design.md, Sagüin properties: saguin-attempt
// "queue jobs sent to workers"); MQTT 3.1.1 clients, which cannot be
// workers, receive none, a session that held a worker subscription
// resuming over MQTT 5 alone (newAttempt).
func (s *slot) offerQueue(p *harness.Publish) string {
	q := route(p.Topic)
	if s.conn.Version != harness.V5 || queueOf(q) == nil {
		return ""
	}
	if rt, _ := p.Props.Get(harness.ResponseTopic); len(p.Props.User("saguin-attempt")) == 0 && rt.Str != responseTopic(q) {
		return ""
	}
	return q
}

// worker reports whether subs's session held a worker subscription at any
// time.
func (subs *subscriptions) worker() bool {
	return subs != nil && slices.ContainsFunc(subs.history, func(r revision) bool {
		return slices.ContainsFunc(r.subs, func(sub *subscription) bool { return worker(sub.Filter) })
	})
}

// takeOffer checks p, an offer of queue read on s's connection, and keeps
// it (the opening's list).
func (s *slot) takeOffer(p *harness.Publish, queue string) error {
	f := s.f
	l := &f.queues
	tag, _ := s.deliveryTag(p)
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%s: offer %q (%s) of queue %s, DUP %v, id %d, properties %v: %s", s.id, p.Topic, tag, queue, p.Dup, p.ID, p.Props, fmt.Sprintf(format, args...))
	}
	if err := s.checkBytes(p); err != nil {
		return err
	}
	if err := s.noteDelivery(p); err != nil {
		return err
	}
	m := f.publications[tag]
	var j *job
	if m != nil {
		j = l.jobs[tag]
	}
	cd, _ := p.Props.Get(harness.CorrelationData)
	id := string(cd.Data)
	switch {
	case j == nil || m.topic != p.Topic:
		return bad("no job of the run's on that topic")
	case p.QoS != 1:
		return bad("a worker offer is a QoS 1 publication (design.md, Delivery, Service publications)")
	case p.Retain:
		return bad("RETAIN 1: RETAIN is 0 on channel deliveries (design.md, SUBSCRIBE, Options)")
	case id == "":
		return bad("no Correlation Data: an attempt carries a broker-made id as its Correlation Data (design.md, Words: Attempt)")
	}
	if !p.Dup && s.subs.current[workerTopic(queue)] == nil {
		if s.incoming.cancelBound <= 0 {
			return bad("a first transmission after the UNSUBACK of step %d, beyond the one offer its worker subscription may have had handed to the writer: \"Its offers not yet sent are cancelled\" (design.md, Delivery, Queue jobs)", s.incoming.unsubscribedWorker)
		}
		s.incoming.cancelBound--
	}
	if err := s.offerProperties(p, m, queue); err != nil {
		return bad("%v", err)
	}
	q := queueOf(queue)
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	s.incoming.offersRead++
	if most := len(l.jobs)*(q.maxAttempts+1) + 2*maxInflight; s.incoming.offersRead > most {
		return bad("%d offers on the connection, more than the %d the broker can owe it: it never stops sending", s.incoming.offersRead, most)
	}
	if s.incoming.offers == nil {
		s.incoming.offers = map[*harness.Publish]*jobAttempt{}
	}
	if p.Dup {
		l.stats.resent++
		a := s.subs.offerByID[p.ID]
		switch {
		case a == nil || a.j != j:
			if other := l.attempts[id]; other != nil && other.session != s.subs {
				return bad("a resend of another session's attempt, %s's (design.md, CONNECT, 9: in-flight messages are the session's own)", other.j.m.tag)
			}
		case a.id != id:
			return bad("a resend carrying Correlation Data %q, its first transmission %q: a resend is the same message (design.md, Delivery, A resend; storage.md, session_inflight: attempt \"so a resend is the same message\")", id, a.id)
		case !sameProps(a.props, p.Props):
			return bad("a resend carrying properties other than its first transmission's %v (design.md, Delivery: \"A resend uses the Subscription Identifiers and delivery properties saved for its first transmission\")", a.props)
		}
		// Its attempt ended with the connection it first came on: its
		// PUBACK spends nothing.
		s.incoming.offers[p] = nil
		return nil
	}
	if a := l.attempts[id]; a != nil {
		return bad("Correlation Data %q, the attempt of %s offered at %v: each attempt carries a broker-made id of its own (design.md, Words: Attempt)", id, a.j.m.tag, a.readAt.Format(time.StampMicro))
	}
	if slices.ContainsFunc(s.incoming.held, func(h *harness.Publish) bool { return h.ID == p.ID && h.QoS > 0 }) {
		return bad("packet identifier %d, whose delivery it has not acknowledged yet (MQTT-2.2.1-4)", p.ID)
	}
	// A job found finished was offered no more: the attempt its ack
	// completed was read before the ack went, and one before that ended
	// before it began, its worker gone or its job moved, either way never
	// read. A job found dead-lettered may have moved while an attempt the
	// worker had not read was current, an offered job not being held ("Held
	// jobs do not count toward retention_bytes"): that one may still come.
	switch {
	case j.completedAt != 0:
		return bad("offered again after the record check at serial %d found the job finished, no dead letter of it and no row in queue_jobs", j.completedAt)
	case j.deadAt != 0 && j.lateOffer:
		return bad("offered twice after the record check at serial %d found it dead-lettered, which only the attempt current at the move may be (design.md, Channels, queue: retention \"moves the job\")", j.deadAt)
	case j.deadAt != 0:
		j.lateOffer = true
	}
	for _, a := range j.attempts {
		if l.mayHaveEnded(a, now) {
			continue
		}
		how := "neither PUBACK nor answer"
		switch {
		case a.ended == "ack":
			how = "an ack, which completed the job as no hold or move had ended the attempt first"
		case a.puback:
			how = fmt.Sprintf("PUBACK 0x00 %v before, less than visibility_timeout %v", now.Sub(a.pubackAt).Round(time.Millisecond), q.visibility)
		}
		return bad("offered while its attempt read by %s at %v, its connection open, had %s: each job goes to one worker at a time (design.md, Channels, queue; Delivery, Queue jobs)",
			clientOf(f, a.session), a.readAt.Format(time.StampMicro), how)
	}
	if n := len(j.attempts); n > 0 {
		last := j.attempts[n-1]
		from := l.endLower(last)
		if wait := q.retryWait(n); now.Before(from.Add(wait - queueMargin)) {
			return bad("read %v after the earliest end of its attempt before, %d failed so far: want no earlier than retry.mode %s's wait of %v (design.md, Delivery, Queue jobs: \"The wait starts at the attempt's end\")",
				now.Sub(from).Round(time.Microsecond), n, q.retryMode, wait)
		}
	}
	a := &jobAttempt{j: j, id: id, session: s.subs, conn: s.conn, packetID: p.ID, readAt: now, props: withoutAlias(p.Props)}
	a.plan = s.planFor(a)
	for _, other := range s.incoming.attempts {
		if other.j != j && other.j.queue == queue && other.blocks(now) {
			l.suspects = append(l.suspects, suspect{held: other, offered: a, step: f.step})
		}
	}
	// A later offer proves any earlier ack failed to finish the job
	// (for example it raced a hold expiry). It cannot justify completion
	// of this attempt by a stale response later.
	j.acked = false
	j.attempts = append(j.attempts, a)
	l.attempts[id] = a
	l.stats.offers++
	s.incoming.attempts = append(s.incoming.attempts, a)
	s.incoming.offers[p] = a
	if s.subs.offerByID == nil {
		s.subs.offerByID = map[uint16]*jobAttempt{}
	}
	s.subs.offerByID[p.ID] = a
	return nil
}

// clientOf names the client whose session subs is, for messages.
func clientOf(f *fuzzer, subs *subscriptions) string {
	for _, s := range f.slots {
		if s.subs == subs || s.saved == subs {
			return s.id
		}
	}
	return "a session no client holds now"
}

// offerProperties checks p's properties, an offer of m on queue to s:
// design.md, Delivery, Service publications: "a worker offer is a new
// broker-originated QoS 1 publication containing the job payload and
// attempt metadata, including saguin-expires where present. It does not
// inherit the job publication's Message Expiry"; "A worker offer carries
// the Subscription Identifier of the receiving worker's own
// $saguin/queue/<channel> subscription, where it set one"; Sagüin
// properties: "Nothing a publisher sends is altered, except that a queue
// job's Response Topic and Correlation Data are the queue's own", and the
// table: saguin-id, saguin-offset and saguin-timestamp, saguin-expires,
// saguin-attempt "(from 1)", decimal; saguin-channel only "where the
// consumer's filter reaches more than one channel", which a worker's never
// does; $saguin topics: responses go to $saguin/queue/<channel>/response.
// A resend's are its first transmission's, which takeOffer compares.
func (s *slot) offerProperties(p *harness.Publish, m *message, queue string) error {
	got := map[string][]string{}
	var users, sent []string
	var ids []uint32
	for _, q := range p.Props {
		switch {
		case q.ID == harness.SubscriptionIdentifier:
			ids = append(ids, q.Int)
		case q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-"):
			got[q.Str] = append(got[q.Str], q.Value)
		case q.ID == harness.UserProperty:
			users = append(users, q.Str+"="+q.Value)
		}
	}
	for _, q := range m.props {
		if q.ID == harness.UserProperty && !strings.HasPrefix(q.Str, "saguin-") {
			sent = append(sent, q.Str+"="+q.Value)
		}
	}
	rt, _ := p.Props.Get(harness.ResponseTopic)
	mei, expiry := p.Props.Get(harness.MessageExpiryInterval)
	switch {
	case rt.Str != responseTopic(queue):
		return fmt.Errorf("its Response Topic %q; want the queue's own, %q (design.md, Sagüin properties; $saguin topics)", rt.Str, responseTopic(queue))
	case expiry:
		return fmt.Errorf("a Message Expiry Interval of %d: an offer \"does not inherit the job publication's Message Expiry\" (design.md, Delivery, Service publications)", mei.Int)
	case !slices.Equal(users, sent):
		return fmt.Errorf("its User Properties %q, the publisher's %q: \"The publisher's own User Properties go unaltered\" (design.md, Sagüin properties)", users, sent)
	}
	for _, id := range []byte{harness.PayloadFormatIndicator, harness.ContentType} {
		want, sentIt := m.props.Get(id)
		if have, ok := p.Props.Get(id); ok != sentIt || have.Int != want.Int || have.Str != want.Str {
			return fmt.Errorf("property 0x%02X %v present %v, the publisher's %v present %v: nothing a publisher sends is altered (design.md, Sagüin properties)", id, have, ok, want, sentIt)
		}
	}
	if v := got["saguin-attempt"]; len(v) != 1 || !positive(v[0]) {
		return fmt.Errorf("saguin-attempt %q; want it once, decimal, from 1 (design.md, Sagüin properties)", v)
	}
	switch v := got["saguin-id"]; {
	case len(v) == 0 || (len(v) != 1 && len(v) != m.saguinIDs):
		// Once, or as often as its publisher sent it: "The publisher's own
		// User Properties go unaltered" (as saguinProperties).
		return fmt.Errorf("saguin-id %q; want it once (design.md, Sagüin properties)", v)
	case m.id != "" && v[0] != m.id:
		return fmt.Errorf("saguin-id %q; want the publisher's first, %q (design.md, Sagüin properties)", v[0], m.id)
	case m.id == "" && !uuidV7.MatchString(v[0]):
		return fmt.Errorf("saguin-id %q; want a UUIDv7 (design.md, Sagüin properties)", v[0])
	}
	for _, name := range []string{"saguin-offset", "saguin-timestamp"} {
		if v := got[name]; len(v) != 1 || !decimal(v[0]) {
			return fmt.Errorf("%s %q; want it once, decimal (design.md, Sagüin properties)", name, v)
		}
	}
	switch v := got["saguin-expires"]; {
	case m.expiring && (len(v) != 1 || !decimal(v[0])):
		return fmt.Errorf("saguin-expires %q; want it once, decimal: its publisher set an expiry (design.md, Delivery, Service publications)", v)
	case !m.expiring && len(v) != 0:
		return fmt.Errorf("saguin-expires %q; want none: its publisher set no expiry", v)
	}
	for name := range got {
		if !slices.Contains([]string{"saguin-id", "saguin-offset", "saguin-timestamp", "saguin-expires", "saguin-attempt"}, name) {
			return fmt.Errorf("%s %q, which Sagüin does not add to an offer (design.md, Sagüin properties)", name, got[name])
		}
	}
	if !p.Dup {
		var want [][]uint32
		for _, r := range s.subs.history {
			for _, sub := range r.subs {
				if sub.Filter == workerTopic(queue) {
					w := []uint32{}
					if sub.id != 0 {
						w = append(w, sub.id)
					}
					want = append(want, w)
				}
			}
		}
		switch {
		case len(want) == 0:
			return errors.New("a first transmission to a session that never held a worker subscription: \"A subscription to $saguin/queue/<channel> at QoS 1 makes a worker\" (design.md, Channels, queue)")
		case !slices.ContainsFunc(want, func(w []uint32) bool { return slices.Equal(w, ids) || (len(w) == 0 && len(ids) == 0) }):
			return fmt.Errorf("its Subscription Identifiers %v; want those of its worker subscription, one of %v (design.md, Delivery, Service publications)", ids, want)
		}
	}
	return m.compare(got["saguin-id"][0], got["saguin-offset"][0], got["saguin-timestamp"][0], s.id)
}

// withoutAlias is ps without a Topic Alias, which a resend sets for its own
// connection (design.md, Delivery, A resend: "Topic Alias is established
// for the current connection").
func withoutAlias(ps harness.Properties) harness.Properties {
	return slices.DeleteFunc(slices.Clone(ps), func(q harness.Property) bool { return q.ID == harness.TopicAlias })
}

// sameProps reports whether first and again, an offer's first transmission
// and its resend, carry the same properties, Topic Alias aside, in any
// order.
func sameProps(first, again harness.Properties) bool {
	key := func(ps harness.Properties) []string {
		out := make([]string, 0, len(ps))
		for _, q := range withoutAlias(ps) {
			out = append(out, propString(q))
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(key(first), key(again))
}

// mayHaveEnded reports whether a may have ended by now: its worker went
// (design.md, Delivery, Queue jobs: "its timeout or the worker's going"),
// its client returned or refused it ("A refusing PUBACK (reason 0x80 or
// above) spends the attempt and ends it as a failure, as return does"), or
// its hold, which "starts at the worker's PUBACK and lasts
// visibility_timeout", may have run out, an ack coming later changing
// nothing. An ack its hold could not outlast completes the job, which ends
// with it (Channels, queue). No other end is the client's to know: a job
// moved by retention is finished.
func (l *queueLog) mayHaveEnded(a *jobAttempt, now time.Time) bool {
	_, gone := l.ended[a.conn]
	q := queueOf(a.j.queue)
	return gone || a.ended == "return" || a.ended == "refused" ||
		(a.puback && a.pubackReason < 0x80 && !now.Before(a.pubackAt.Add(q.visibility-queueMargin)))
}

// endLower is the earliest moment a, which may have ended (mayHaveEnded),
// could have: its worker's going, its client's return or refusal, or its
// hold's end, each no earlier than the client's act ("The wait starts at
// the attempt's end").
func (l *queueLog) endLower(a *jobAttempt) time.Time {
	var at time.Time
	earliest := func(t time.Time) {
		if at.IsZero() || t.Before(at) {
			at = t
		}
	}
	if t, gone := l.ended[a.conn]; gone {
		earliest(t)
	}
	if a.ended == "return" || a.ended == "refused" {
		earliest(a.endedAt)
	}
	if a.puback && a.pubackReason < 0x80 {
		earliest(a.pubackAt.Add(queueOf(a.j.queue).visibility))
	}
	return at
}

// blocks reports whether a, read on its worker's open connection, holds
// the worker so that no other job of its queue may go to it now, unless a's
// job moved meanwhile, which a later record check tells (suspect): no
// answer of its client has ended it, and its hold, if begun, cannot have
// run out ("each worker receives one job at a time per queue").
func (a *jobAttempt) blocks(now time.Time) bool {
	q := queueOf(a.j.queue)
	return a.ended == "" && (!a.puback || now.Before(a.pubackAt.Add(q.visibility-queueMargin)))
}

// suspect is an offer of a second job to a worker that held one, read at
// step: a breach unless either job moved before, which a record check
// finding neither dead-lettered rules out, a move being final.
type suspect struct {
	held, offered *jobAttempt
	step          int
}

// pubackOffer sends the PUBACK of p, an offer of a, as its plan says: one
// resent (a nil, or a's first transmission on an earlier connection) gets
// 0x00 (design.md, Restart: recovered exchanges "still finish with their
// original identifiers and attempt data").
func (s *slot) pubackOffer(p *harness.Publish, a *jobAttempt) error {
	delete(s.incoming.offers, p)
	mine := a != nil && a.conn == s.conn && !a.puback
	var reason byte
	if mine {
		reason = a.plan.reason
	}
	at := time.Now()
	if err := s.conn.Send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID, Reason: reason}); err != nil {
		return err
	}
	l := &s.f.queues
	l.mu.Lock()
	defer l.mu.Unlock()
	// MQTT 5 2.2.1: "The Packet Identifier becomes available for reuse
	// after the sender has processed the corresponding acknowledgement
	// packet [...] In the case of a QoS 1 PUBLISH, this is the corresponding
	// PUBACK". The broker's next offer under it (design.md, Delivery,
	// Sending: "Packet identifiers use the lowest free identifier") may be
	// handed to the writer and never read before the connection ends, so a
	// resend under it is judged as one whose first transmission went
	// unread, not against this offer.
	delete(s.subs.offerByID, p.ID)
	if !mine {
		return nil
	}
	a.puback, a.spendable, a.pubackReason, a.pubackAt = true, true, reason, at
	if reason < 0x80 {
		l.stats.puback0++
		return nil
	}
	l.stats.refusing++
	if a.ended == "" {
		a.ended, a.endedAt = "refused", at
	}
	return nil
}

// confirmPubacks records an upper bound on the hold start for PUBACKs
// sent before this PINGREQ. design.md, Decisions: "Each connection's packet
// order is preserved." Delivery: "The hold starts at the worker's PUBACK
// and lasts visibility_timeout".
func (s *slot) confirmPubacks(before, through time.Time) {
	l := &s.f.queues
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, a := range s.incoming.attempts {
		if a.conn == s.conn && a.puback && a.pubackReason < 0x80 &&
			a.pubackAt.Before(before) && a.pubackThrough.IsZero() {
			a.pubackThrough = through
		}
	}
}

// leaveAttempts records that s's connection, holding attempts, ends in
// this step: its worker goes.
func (s *slot) leaveAttempts() {
	if len(s.incoming.attempts) == 0 {
		return
	}
	l := &s.f.queues
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ended[s.conn] = s.f.began
}

// response is a response s's connection sends: its PUBLISH, the attempt
// it names (nil for none the run read), and the answer predicted: PUBACK
// or PUBREC reason, or the close of an MQTT 3.1.1 connection refused at
// QoS 1 or 2.
type response struct {
	p      *harness.Publish
	a      *jobAttempt
	reason byte
	ends   bool
}

// newResponse is s's response payload, naming id, at qos, to queue:
// design.md, $saguin topics, the response row: "ack or return a job, with
// its attempt's Correlation Data. Needs consume (else 0x87). No answer: the
// PUBACK (0x00) confirms receipt only [...] Taken at QoS 0 too"; "A request
// without a Response Topic is refused with 0x83, except for [...] a queue
// response"; Flows, PUBLISH: "At QoS 1 or 2 an MQTT 3.1.1 client has no
// refusal code and is disconnected". MQTT 3.1.1 carries no Correlation
// Data: its response names no attempt.
func (s *slot) newResponse(queue string, a *jobAttempt, id []byte, payload string, qos byte) *response {
	r := &response{p: &harness.Publish{QoS: qos, Topic: responseTopic(queue), Payload: []byte(payload)}, a: a}
	if s.conn.Version == harness.V5 {
		r.p.Props = harness.Properties{{ID: harness.CorrelationData, Data: id}}
	} else {
		r.a = nil
	}
	if !consumes(s.user, queue) {
		r.reason = 0x87
		r.ends = qos > 0 && s.conn.Version != harness.V5
	}
	return r
}

// firstResponseID is where the packet identifiers of a connection's
// responses begin (sendResponses).
const firstResponseID = 61000

// responseIDs gives each of rs at QoS 1 or 2 a packet identifier from
// firstResponseID up that none of s's pending PUBLISHes, its session's QoS
// 2 exchanges nor its unanswered PUBRELs holds. The responses a work step
// sends depend on when offers come, which is the broker's: drawn from the
// connection's own counter, they would shift every later identifier, and
// the step log with them. Every answer is read before sendResponses
// returns, so the next call may use the same ones.
func (s *slot) responseIDs(rs []*response) {
	id := uint16(firstResponseID)
	for _, r := range rs {
		if r.p.QoS == 0 {
			continue
		}
		for s.pending[id] != nil || s.held[id] != nil || s.unknown[id] {
			id++
		}
		r.p.ID = id
		id++
	}
}

// String is r, for the step log and messages.
func (r *response) String() string {
	want := "nothing"
	switch {
	case r.ends:
		want = "the close (MQTT 3.1.1 has no refusal code)"
	case r.p.QoS > 0:
		want = fmt.Sprintf("%s 0x%02X", harness.Name(harness.AnswerKind(r.p.QoS)), r.reason)
	}
	return fmt.Sprintf("response %q QoS %d to %q: want %s", r.p.Payload, r.p.QoS, r.p.Topic, want)
}

// sendResponses sends rs, in one write, on s's connection, records what
// each may do to the attempt it names (answer), and reads every answer:
// each PUBACK, or PUBREC then, unrefused, PUBREL and its PUBCOMP 0x00
// ("Every request taken at QoS 2 runs on arrival [...] The ordinary PUBREL
// and PUBCOMP follow"), then a PINGRESP, past which the QoS 0 ones were
// taken. A response that closes the connection ends the step there.
func (s *slot) sendResponses(rs []*response) error {
	if len(rs) == 0 {
		return nil
	}
	var wire []byte
	ends := false
	at := time.Now()
	s.responseIDs(rs)
	for _, r := range rs {
		b, err := harness.Encode(r.p, s.conn.Version)
		if err != nil {
			return err
		}
		wire = append(wire, b...)
		if r.reason == 0 {
			s.answer(r, at)
		}
		if r.ends {
			ends = true
			break
		}
	}
	if err := s.conn.SendRaw(wire); err != nil {
		return err
	}
	if ends {
		err := awaitClose(s)
		_ = s.conn.Close() // checked closed by the broker, or the step failed
		s.fire("the broker closed the connection for a response refused at QoS 1 or 2, MQTT 3.1.1 having no refusal code")
		s.end(false)
		return err
	}
	// owed are the responses whose PUBACK or PUBREC is to come, and
	// released those whose PUBCOMP is, by packet identifier.
	owed, released := map[uint16]*response{}, map[uint16]*response{}
	for _, r := range rs {
		if r.p.QoS > 0 {
			owed[r.p.ID] = r
		}
	}
	for len(owed)+len(released) > 0 {
		p, err := s.recv()
		if err != nil {
			return fmt.Errorf("%s: waiting for the answers to %d responses: %w", s.id, len(owed)+len(released), err)
		}
		ack, ok := p.(*harness.Ack)
		if !ok || (owed[ack.ID] == nil && released[ack.ID] == nil) {
			if settled, err := s.settle(p); err != nil || !settled {
				return cmp.Or(err, fmt.Errorf("%s: %s while waiting for the answers to %d responses", s.id, show(p), len(owed)+len(released)))
			}
			continue
		}
		if r := released[ack.ID]; r != nil {
			if ack.Kind != harness.PUBCOMP || ack.Reason != 0 {
				return fmt.Errorf("%s: %s after the PUBREL of the %s; want PUBCOMP %d 0x00 (design.md, $saguin topics: \"The ordinary PUBREL and PUBCOMP follow\")", s.id, show(p), r, ack.ID)
			}
			delete(released, ack.ID)
			continue
		}
		r := owed[ack.ID]
		switch {
		case ack.Kind != harness.AnswerKind(r.p.QoS):
			return fmt.Errorf("%s: %s while waiting for the answers to %d responses", s.id, show(p), len(owed)+len(released))
		case ack.Reason != r.reason:
			return fmt.Errorf("%s: %s 0x%02X to the %s (design.md, $saguin topics, the response row)", s.id, harness.Name(ack.Kind), ack.Reason, r)
		}
		delete(owed, ack.ID)
		if r.p.QoS == 2 && r.reason < 0x80 {
			if err := s.conn.Send(&harness.Ack{Kind: harness.PUBREL, ID: ack.ID}); err != nil {
				return err
			}
			released[ack.ID] = r
		}
	}
	return pingPast(s)
}

// answer records what r, a response s's connection sends with consume,
// may do to the attempt it names (design.md, $saguin topics: "A response
// naming an attempt that is not current or not this session's, or with
// another payload, changes nothing"): sent on the connection the attempt
// came on, an ack or return spends it, and the first of those, no refusal
// before, ends it. Only an ack whose attempt may still be current can
// explain a finished job. Timer ordering between observations remains
// uncertain until a later offer or a bound on the hold proves it ended.
func (s *slot) answer(r *response, at time.Time) {
	l := &s.f.queues
	l.mu.Lock()
	defer l.mu.Unlock()
	switch payload := string(r.p.Payload); payload {
	case "ack":
		l.stats.acks++
	case "return":
		l.stats.returns++
	default:
		l.stats.others++
	}
	a := r.a
	if a == nil || a.session != s.subs {
		return
	}
	payload := string(r.p.Payload)
	if payload != "ack" && payload != "return" {
		return
	}
	// design.md, $saguin topics: "A response naming an attempt that is
	// not current or not this session's, or with another payload, changes
	// nothing and is logged." A new connection, a later offer, an earlier
	// terminal answer or a hold known expired makes this response stale.
	_, gone := l.ended[a.conn]
	if a.conn != s.conn || gone || a.ended != "" ||
		a.j.attempts[len(a.j.attempts)-1] != a ||
		(!a.pubackThrough.IsZero() && at.After(a.pubackThrough.Add(queueOf(a.j.queue).visibility+queueMargin))) {
		return
	}
	a.spendable = true
	a.ended, a.endedAt = payload, at
	if payload == "ack" {
		a.j.acked = true
	}
}

// work: a connected MQTT 5 client that has not been a worker on its
// connection subscribes to the queue's worker topic; one that has works a
// few rounds, while the round before answered an offer.
func (f *fuzzer) work() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil && s.conn.Version == harness.V5 })
	if s == nil {
		return false, nil
	}
	if !s.incoming.worked {
		return true, f.subscribeWorker(s)
	}
	f.note("%s: works the offers it holds", s.id)
	for range workRounds {
		answered, err := s.workRound()
		if err != nil || s.conn == nil || answered == 0 {
			return true, err
		}
	}
	return true, nil
}

// workRounds is the most rounds one work step takes.
const workRounds = 4

// workRound drains s's connection, then answers the offers it read as
// each attempt's plan says: the responses due before the PUBACK, its
// acknowledgements released in the order their PUBLISHes came
// (MQTT-4.6.0-2), the responses due once the PUBACK 0x00 has gone, and
// those of the jobs it held since its last work (design.md, Delivery,
// Queue jobs: a worker "keeps the job it holds until it answers, the hold
// ends or it goes", unsubscribed or not). Every answer is read before it
// returns how many offers it answered.
func (s *slot) workRound() (int, error) {
	if err := s.drain(); err != nil {
		return 0, err
	}
	answered := len(s.incoming.offers)
	var before []*response
	for _, a := range s.incoming.attempts {
		if a.plan.first != "" && !a.puback && !a.answered {
			a.answered = true
			before = append(before, s.newResponse(a.j.queue, a, []byte(a.id), a.plan.first, a.plan.qos))
		}
	}
	if err := s.sendResponses(before); err != nil || s.conn == nil {
		return 0, err
	}
	if err := s.releaseAcks(); err != nil {
		return 0, err
	}
	var after []*response
	for _, a := range s.incoming.attempts {
		switch {
		case !a.puback || a.pubackReason >= 0x80 || a.answered:
		case a.plan.then != "":
			a.answered = true
			after = append(after, s.newResponse(a.j.queue, a, []byte(a.id), a.plan.then, a.plan.qos))
		default:
			// Held since an earlier work, or now: answered at the next.
			a.plan.then = a.plan.later
		}
	}
	return answered + len(after), s.sendResponses(after)
}

// subscribeWorker has s subscribe to the queue's worker topic at QoS 1,
// with a Subscription Identifier one time in two (design.md, SUBSCRIBE,
// the subscription table: "$saguin/queue/<channel> (a worker) | the next
// job | refused (0x83) / 1 / 1").
func (f *fuzzer) subscribeWorker(s *slot) error {
	p := &harness.Subscribe{ID: s.nextID(), Subs: []harness.Subscription{{Filter: workerTopic(queueName), QoS: 1, RetainHandling: 2}}}
	var sid uint32
	if f.rng.IntN(2) == 0 {
		sid = 1 + f.rng.Uint32N(harness.MaxVarInt)
		p.Props = harness.Properties{{ID: harness.SubscriptionIdentifier, Int: sid}}
	}
	wire, err := harness.Encode(p, s.conn.Version)
	if err != nil {
		return err
	}
	return f.subscribeWith(s, p, wire, sid, false, nil, false)
}

// workerFilter draws a filter of the worker topic's shape: mostly the
// queue's, now and then a narrower one or one of a channel that is no
// queue.
func (f *fuzzer) workerFilter() string {
	switch f.rng.IntN(6) {
	case 0:
		return workerTopic(queueName) + "/" + f.word(1, 2)
	case 1:
		return workerTopic("ca")
	}
	return workerTopic(queueName)
}

// workerCode is the SUBACK code of sub, a filter under $saguin/queue/, for
// s, its session holding held and shares, its SUBSCRIBE carrying an
// unknown saguin- property where reserved and a partition where part (bad
// where invalid), and either, another code the design allows as well, 0
// for none (design.md, SUBSCRIBE, the subscription table, a worker:
// "refused (0x83) / 1 / 1; MQTT 3.1.1: refused (0x80)"; Options: No Local
// "is refused with 0x8F on a worker subscription"; Refusals: 0x87 "where
// the acl_file refuses the client", 0x8F "for a filter of the wrong shape,
// such as a narrower $saguin/queue filter", 0x97 past max_subscriptions,
// "using refusal order 0x87, 0x8F, 0x97, 0x83"; Partitioning: "refused with
// 0x83 on [...] $saguin/queue"; Channels, queue: a worker needs consume).
// $saguin topics:
// "Any other filter under `$saguin/` is refused with 0x87, as `$SYS/` is
// ([Operations, HTTP](#http)). A filter under `$saguin/queue` that names no
// worker topic is the exception: it is refused with 0x8F
// ([SUBSCRIBE](#subscribe)). MQTT 3.1.1 gets 0x80 for both."
// The permission and wrong-shape refusals are both considered where the
// filter also names a queue the user may not consume.
func workerCode(s *slot, sub harness.Subscription, reserved bool, part *partition, bad bool) (code, either byte) {
	rest := strings.TrimPrefix(sub.Filter, "$saguin/queue/")
	name, _, narrower := strings.Cut(rest, "/")
	switch {
	case s.conn.Version != harness.V5:
		code = 0x80
	case queueOf(name) == nil || (narrower && !consumes(s.user, name)):
		code, either = 0x87, 0x8F
	case !consumes(s.user, name):
		code = 0x87
	case narrower || sub.NoLocal:
		code = 0x8F
	case s.subs.current[sub.Filter] == nil && len(s.subs.current)+len(s.subs.shares) >= maxSubscriptions:
		code = 0x97
	case sub.QoS == 0 || reserved || part != nil || bad:
		code = 0x83
	default:
		code = 1
	}
	if s.conn.Version != harness.V5 && either != 0 {
		either = 0
	}
	return code, either
}

// respond: a connected client sends a response naming an attempt that is
// not its own, one of its own that may have ended, or none the run read:
// "A response naming an attempt that is not current or not this
// session's, or with another payload, changes nothing and is logged"; its
// PUBACK, PUBREC or none is predicted (newResponse). Which attempt, the
// broker's timing having made those the run read, is left out of the step
// log. One of its own may still be current, and is recorded as any answer
// (answer).
func (f *fuzzer) respond() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	r := f.rng
	kind, payload, qos, pick := r.IntN(3), []string{"ack", "return", "done"}[r.IntN(3)], byte(r.IntN(3)), r.Uint64()
	if denied(s.user, "qos2") {
		qos = min(qos, 1)
	}
	l := &f.queues
	var from []*jobAttempt
	l.mu.Lock()
	for _, a := range l.attempts {
		if (kind == 0 && a.session != s.subs) || (kind == 1 && a.session == s.subs && l.mayHaveEnded(a, time.Now())) {
			from = append(from, a)
		}
	}
	l.stats.notOwn++
	l.mu.Unlock()
	slices.SortFunc(from, func(x, y *jobAttempt) int { return strings.Compare(x.id, y.id) })
	id := []byte("an attempt that never was " + strconv.Itoa(f.step))
	var a *jobAttempt
	if len(from) > 0 {
		a = from[pick%uint64(len(from))]
		id = []byte(a.id)
	}
	resp := s.newResponse(queueName, a, id, payload, qos)
	f.note("%s: %s, naming an attempt not its own, one it may hold no longer, or none", s.id, resp)
	return true, s.sendResponses([]*response{resp})
}

// checkQueues checks, in one read of the database, each queue's
// unfinished jobs and dead letters against the jobs the run sent
// (storage.md, queue_jobs: "This table holds a queue's unfinished jobs.
// The record stays in records. A finished job's row is deleted. When
// retention ends a job, the job moves to the dead-letter channel in one
// transaction with its row"; design.md, Channels, queue: "Retention never
// deletes an unfinished job. It moves the job to the append channel
// <channel>__dlq in one transaction. The topic gains the prefix __dlq/";
// Delivery, Queue jobs: "When an attempt fails with retry.max_attempts
// spent, the job moves to the dead-letter channel"; $saguin topics: a
// response that is "not current or not this session's [...] changes
// nothing"). Each job the run knows stored (acknowledged, or its record or
// row found) is in exactly one of three states: unfinished, its row there
// and its record with it; dead-lettered, once; or finished, which only an
// ack naming a current attempt, sent by the session it went to, can
// have done (R4: what was acknowledged is stored). A job never returns
// from finished or dead-lettered. A dead letter is the job's topic under
// the prefix, its saguin-id, its payload. Then each suspect is settled.
func (f *fuzzer) checkQueues(names map[int64]string) []error {
	l := &f.queues
	if len(l.jobs) == 0 {
		return nil
	}
	ids := map[string]int64{}
	for id, name := range names {
		ids[name] = id
	}
	f.serial++
	read := f.serial
	var errs []error
	for _, c := range channels {
		if c.queue == nil {
			continue
		}
		unfinished, dead, held, err := f.readQueue(ids[c.name], ids[c.name+deadLetterSuffix])
		if err != nil {
			return append(errs, err)
		}
		dlq := ids[c.name+deadLetterSuffix]
		if was := l.deadHeld[dlq]; held < was+len(dead) {
			errs = append(errs, fmt.Errorf("channel %s holds %d dead letters, fewer than the %d found before and the %d new: dead letters are kept for dlq_retention_period, a day here (config.md, channels.<name>)",
				c.name+deadLetterSuffix, held, was, len(dead)))
		}
		l.deadHeld[dlq] = held
		for i := range dead {
			errs = append(errs, f.deadLetterFound(c.name, &dead[i], read)...)
		}
		for tag := range unfinished {
			if j := l.jobs[tag]; j == nil || j.queue != c.name {
				errs = append(errs, fmt.Errorf("queue_jobs holds a job of %s whose record, %q, is no job the run sent (storage.md, queue_jobs)", c.name, tag))
			}
		}
		for _, tag := range slices.Sorted(maps.Keys(l.jobs)) {
			j := l.jobs[tag]
			if j.queue == c.name {
				errs = append(errs, j.state(unfinished[tag], read, &l.stats)...)
			}
		}
	}
	for _, sp := range l.suspects {
		if sp.held.j.deadAt == 0 && sp.offered.j.deadAt == 0 {
			errs = append(errs, fmt.Errorf("%s: at step %d, offered %s while it held %s, read at %v, which no answer, hold's end or going had ended, neither job moved since: each worker receives one job at a time per queue (design.md, Delivery, Queue jobs)",
				clientOf(f, sp.offered.session), sp.step, sp.offered.j.m.tag, sp.held.j.m.tag, sp.held.readAt.Format(time.StampMicro)))
		}
	}
	l.suspects = nil
	return errs
}

// readQueue reads, in one transaction, the payload tags of the queue id's
// unfinished jobs and the dead letters of its dead-letter channel dlq
// stored since the last read, with how many it holds. A row without its
// record fails the read: "The record stays in records".
func (f *fuzzer) readQueue(id, dlq int64) (unfinished map[string]bool, dead []record, held int, err error) {
	tx, err := f.db.Begin()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("reading the queue's jobs: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // read-only: nothing to keep
	rows, err := tx.Query(`SELECT j."offset", r.payload, r.payload_size FROM queue_jobs j LEFT JOIN records r ON r.channel = j.channel AND r."offset" = j."offset" WHERE j.channel = ?`, id)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("reading the queue's jobs: %w", err)
	}
	unfinished = map[string]bool{}
	for rows.Next() {
		var offset int64
		var stored []byte
		var size *int64
		if err := rows.Scan(&offset, &stored, &size); err != nil {
			_ = rows.Close()
			return nil, nil, 0, err
		}
		if size == nil {
			_ = rows.Close()
			return nil, nil, 0, fmt.Errorf("queue_jobs holds a job at offset %d with no record: \"The record stays in records\" (storage.md, queue_jobs)", offset)
		}
		payload, _, err := asReceived(stored, *size)
		if err != nil {
			_ = rows.Close()
			return nil, nil, 0, fmt.Errorf("the job at offset %d: %w", offset, err)
		}
		tag, _, _ := strings.Cut(string(payload), " ")
		unfinished[tag] = true
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	rows, err = tx.Query(`SELECT "offset", id, time, topic, qos, publisher, payload, payload_size FROM records WHERE channel = ? AND "offset" > ? ORDER BY "offset"`, dlq, f.queues.deadRead[dlq])
	if err != nil {
		return nil, nil, 0, fmt.Errorf("reading the dead letters: %w", err)
	}
	for rows.Next() {
		r := record{channel: dlq}
		var stored []byte
		var size int64
		if err := rows.Scan(&r.offset, &r.id, &r.time, &r.topic, &r.qos, &r.publisher, &stored, &size); err != nil {
			_ = rows.Close()
			return nil, nil, 0, err
		}
		if r.payload, _, err = asReceived(stored, size); err != nil {
			_ = rows.Close()
			return nil, nil, 0, fmt.Errorf("the dead letter at offset %d: %w", r.offset, err)
		}
		dead = append(dead, r)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	if err := tx.QueryRow(`SELECT COUNT(*) FROM records WHERE channel = ?`, dlq).Scan(&held); err != nil {
		return nil, nil, 0, err
	}
	if len(dead) > 0 {
		f.queues.deadRead[dlq] = dead[len(dead)-1].offset
	}
	return unfinished, dead, held, nil
}

// deadLetterFound checks r, a dead letter of queue the record check found,
// against the job its payload names, and keeps it as that job's dead
// letter's record. Discovery confirms existence; owed separately checks
// that the connection preceded the move, or made a fresh floor position.
func (f *fuzzer) deadLetterFound(queue string, r *record, read uint64) []error {
	l := &f.queues
	where := fmt.Sprintf("the dead letter at offset %d of %s", r.offset, queue+deadLetterSuffix)
	tag, _, _ := strings.Cut(string(r.payload), " ")
	j := l.jobs[tag]
	if j == nil || j.queue != queue {
		return []error{fmt.Errorf("%s, %q, payload %q: no job of the run's", where, r.topic, r.payload)}
	}
	var errs []error
	m := j.m
	if j.deadAt != 0 {
		errs = append(errs, fmt.Errorf("%s: a second dead letter of %s (step %d), dead-lettered at serial %d: a job moves once (design.md, Channels, queue)", where, tag, m.step, j.deadAt))
	}
	want := m.id
	if job := m.stored.Load(); job != nil {
		want = job.id
	}
	switch {
	case r.topic != deadLetterPrefix+m.topic:
		errs = append(errs, fmt.Errorf("%s: topic %q; want the job's, %q, under the prefix %s (design.md, Channels, queue)", where, r.topic, m.topic, deadLetterPrefix))
	case !bytes.Equal(r.payload, m.published()):
		errs = append(errs, fmt.Errorf("%s: payload %q, not the job's (design.md, Channels, queue: \"It moves the job\")", where, r.payload))
	case want != "" && r.id != want:
		errs = append(errs, fmt.Errorf("%s: id %q; want the job's saguin-id %q (design.md, Channels, queue: \"It moves the job\")", where, r.id, want))
	case want == "" && !uuidV7.MatchString(r.id):
		errs = append(errs, fmt.Errorf("%s: id %q; want the job's, a UUIDv7", where, r.id))
	}
	j.deadAt = read
	l.stats.deadLetters++
	dm := j.dead
	kept := *r
	kept.payload = nil
	dm.stored.Store(&kept)
	if seen := dm.seen.Load(); seen != nil {
		if err := r.compare(*seen, dm.tag, "a reader of the dead letters"); err != nil {
			errs = append(errs, err)
		}
	}
	dm.setFate(stored)
	j.foundStep = f.step
	dm.confirmed.CompareAndSwap(0, read)
	return errs
}

// state checks j against one record check's read, at serial read: its row
// there or not (unfinished), and its dead letter found or not (j.deadAt);
// st counts the jobs found finished.
func (j *job) state(unfinished bool, read uint64, st *queueStats) []error {
	m := j.m
	what := fmt.Sprintf("job %s on %q (step %d)", m.tag, m.topic, m.step)
	known := m.fateIs(stored) || m.stored.Load() != nil || j.unfinishedAt != 0 || j.deadAt != 0
	switch {
	case unfinished && j.deadAt != 0:
		return []error{fmt.Errorf("%s is unfinished, its row in queue_jobs, and dead-lettered at serial %d: the move takes the job \"in one transaction with its row\" (storage.md, queue_jobs)", what, j.deadAt)}
	case unfinished && j.completedAt != 0:
		return []error{fmt.Errorf("%s is unfinished again, its row back in queue_jobs after the record check at serial %d found it finished", what, j.completedAt)}
	case unfinished:
		j.unfinishedAt = read
	case j.deadAt != 0 && j.completedAt != 0 && j.completedAt < j.deadAt:
		return []error{fmt.Errorf("%s was dead-lettered at serial %d, after the record check at serial %d found it finished, no row and no dead letter: a job ends once (design.md, Channels, queue)", what, j.deadAt, j.completedAt)}
	case j.deadAt != 0 || !known || j.completedAt != 0:
	case !j.acked:
		return []error{fmt.Errorf("%s, stored as far as the run knows (acknowledged %v, its record found %v, unfinished at serial %d), has no row in queue_jobs and no dead letter, though no worker it went to answered ack naming a current attempt: each job ends acknowledged or dead-lettered (design.md, Channels, queue; storage.md, queue_jobs; R4)",
			what, m.fateIs(stored), m.stored.Load() != nil, j.unfinishedAt)}
	default:
		j.completedAt = read
		st.completed++
	}
	return nil
}

// spentAtMost is how many of j's attempts its workers could have spent:
// those whose PUBACK, or an ack or return naming them, was sent on the
// connection they came on. An attempt the run never read, its connection
// gone first, was spent by none ("An attempt that ends with neither comes
// back unspent").
func (j *job) spentAtMost() int {
	n := 0
	for _, a := range j.attempts {
		if a.spendable {
			n++
		}
	}
	return n
}

// checkDeadProps checks got, the saguin- properties a delivery of dm, the
// dead letter of a job, carries (design.md, Sagüin properties: "saguin-dlq-*
// | dead letters: saguin-dlq-reason, -channel, -offset, -attempts, -at, and
// where an attempt was spent -first and -last"; their values decimal, the
// times "Unix milliseconds in decimal", "saguin-dlq-at is when the job
// moved, and -first and -last are its first and last attempt"; Channels,
// queue: "Its value is expired when the job is past retention_period,
// evicted when retention_bytes causes the move, or attempts_exhausted
// [...] When several dead-letter causes apply, expired wins, then
// attempts_exhausted, then evicted"; Delivery, Queue jobs: "When an attempt
// fails with retry.max_attempts spent, the job moves [...] as
// attempts_exhausted"; "An attempt that ends with neither comes back
// unspent"). The job's intake time comes from its delivery's
// saguin-timestamp or its original stored record; otherwise the observer's
// publish/acknowledgement window bounds it, with queueMargin's allowance.
func (f *fuzzer) checkDeadProps(dm *message, got map[string][]string) error {
	j := dm.deadOf
	q := queueOf(j.queue)
	one := func(name string) (int64, error) {
		v := got[name]
		if len(v) != 1 || !decimal(v[0]) {
			return 0, fmt.Errorf("%s %q; want it once, decimal", name, v)
		}
		n, _ := strconv.ParseInt(v[0], 10, 64)
		return n, nil
	}
	reason := got["saguin-dlq-reason"]
	if len(reason) != 1 || !slices.Contains([]string{"expired", "evicted", "attempts_exhausted"}, reason[0]) {
		return fmt.Errorf("saguin-dlq-reason %q; want it once, expired, evicted or attempts_exhausted", reason)
	}
	if v := got["saguin-dlq-channel"]; !slices.Equal(v, []string{j.queue}) {
		return fmt.Errorf("saguin-dlq-channel %q; want once %q, the job's queue", v, j.queue)
	}
	offset, err := one("saguin-dlq-offset")
	if err != nil {
		return err
	}
	if r := j.m.stored.Load(); r != nil && r.offset != offset {
		return fmt.Errorf("saguin-dlq-offset %d; want the job's offset, %d", offset, r.offset)
	}
	spent, err := one("saguin-dlq-attempts")
	if err != nil {
		return err
	}
	at, err := one("saguin-dlq-at")
	if err != nil {
		return err
	}
	switch most := j.spentAtMost(); {
	case spent > int64(q.maxAttempts) || spent > int64(most):
		return fmt.Errorf("saguin-dlq-attempts %d, though its workers could have spent at most %d of its %d attempts read, and retry.max_attempts is %d: only a PUBACK or a response naming an attempt spends it", spent, most, len(j.attempts), q.maxAttempts)
	case reason[0] == "attempts_exhausted" && spent != int64(q.maxAttempts):
		return fmt.Errorf("saguin-dlq-reason attempts_exhausted with saguin-dlq-attempts %d; want retry.max_attempts, %d, spent", spent, q.maxAttempts)
	}
	var first, last int64
	switch _, has := got["saguin-dlq-first"]; {
	case spent > 0:
		if first, err = one("saguin-dlq-first"); err == nil {
			last, err = one("saguin-dlq-last")
		}
		if err != nil {
			return err
		}
		if first > last || last > at {
			return fmt.Errorf("saguin-dlq-first %d, -last %d, -at %d; want them in that order", first, last, at)
		}
	case has || len(got["saguin-dlq-last"]) > 0:
		return fmt.Errorf("saguin-dlq-first %q, -last %q, no attempt spent: they come \"where an attempt was spent\"", got["saguin-dlq-first"], got["saguin-dlq-last"])
	}
	// design.md, PUBLISH: "The record's timestamp is the core's intake
	// time." Sagüin properties puts saguin-timestamp on "every delivery".
	// A dead letter's own timestamp is the move, not the job's intake.
	// If neither a delivery nor a record read saw intake, the core took
	// the PUBLISH between its write and acknowledgement. Reuse the same
	// age checks with that interval, allowing queueMargin on the observer's
	// clock. No acknowledgement leaves the upper endpoint unknown.
	var earliest, latest int64
	seen, kept := j.m.seen.Load(), j.m.stored.Load()
	switch {
	case seen != nil:
		earliest, err = strconv.ParseInt(seen.timestamp, 10, 64)
		if err != nil {
			return fmt.Errorf("job's saguin-timestamp: %w", err)
		}
		latest = earliest
	case kept != nil:
		earliest, latest = kept.time, kept.time
	default:
		earliest = j.m.sentAt.Load()
		if earliest == 0 {
			return fmt.Errorf("job %s has no observed PUBLISH write", j.m.tag)
		}
		earliest -= queueMargin.Milliseconds()
		if at := j.m.acknowledgedAt.Load(); at != 0 {
			latest = at + queueMargin.Milliseconds()
		}
	}
	period := q.retentionPeriod.Milliseconds()
	switch {
	case spent > 0 && first < earliest:
		return fmt.Errorf("saguin-dlq-first %d, before its record's time, at least %d", first, earliest)
	case reason[0] == "expired" && at < earliest+period:
		return fmt.Errorf("saguin-dlq-reason expired, though it moved at %d, less than retention_period %v after its record's time, at least %d", at, q.retentionPeriod, earliest)
	case reason[0] != "expired" && latest != 0 && at > latest+period:
		return fmt.Errorf("saguin-dlq-reason %s, though it moved at %d, retention_period %v or more after its record's time, at most %d: \"expired wins\"", reason[0], at, q.retentionPeriod, latest)
	}
	return nil
}

// readDeadLetters: once the run's steps are done, a fresh MQTT 5 client
// subscribes at QoS 1 to every dead letter of the queue and must receive
// each one the record checks found, checked as dead letters are, the
// readers of a dead-letter channel starting at its floor (design.md,
// Channels, queue: "Its readers start at the floor"), and reads to quiet;
// it counts their reasons. With a broker of the run's, the counters are
// scraped before and after (checkQueueCounters).
func (f *fuzzer) readDeadLetters() error {
	l := &f.queues
	if f.dbPath == "" || l.stats.deadLetters == 0 {
		return nil
	}
	var before testkit.Exposition
	if f.scrape != nil {
		var err error
		if before, err = f.scrape(); err != nil {
			return err
		}
	}
	p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "dead-letters", CleanStart: true}, logins[0].user, logins[0].password)
	a, err := seal(p, noSession)
	if err != nil {
		return err
	}
	f.note("the last reader of the dead letters: %s, then SUBSCRIBE %q QoS 1: want each dead letter found", describe(p), deadLetterPrefix+queueName+"/#")
	c, err := f.dial(harness.V5)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }() // the run's last read; nothing follows
	if err := c.SendRaw(a.wire); err != nil {
		return err
	}
	reader := &slot{f: f, id: "dead-letters", conn: c, subs: newSubscriptions()}
	if err := accepted(reader, a.prediction); err != nil {
		return err
	}
	if err := c.Send(&harness.Subscribe{ID: 1, Subs: []harness.Subscription{{Filter: deadLetterPrefix + queueName + "/#", QoS: 1}}}); err != nil {
		return err
	}
	owed := map[string]bool{}
	for _, j := range l.jobs {
		if j.deadAt != 0 {
			owed[j.dead.tag] = true
		}
	}
	reasons := map[string]int{}
	quiet := false
	for len(owed) > 0 || !quiet {
		if len(owed) == 0 {
			ready, err := c.Conn.(*observedConn).ready(time.Now().Add(50 * time.Millisecond))
			if err != nil {
				return err
			}
			if quiet = !ready; quiet {
				break
			}
		}
		pk, err := c.Recv()
		if err != nil {
			return fmt.Errorf("the last reader of the dead letters, %d still owed, %v among them: %w", len(owed), slices.Sorted(maps.Keys(owed))[:min(len(owed), 4)], err)
		}
		switch pk := pk.(type) {
		case *harness.Suback:
			if !slices.Equal(pk.Reasons, []byte{1}) {
				return fmt.Errorf("the last reader of the dead letters: SUBACK % X; want 0x01", pk.Reasons)
			}
		case *harness.Publish:
			reason, err := f.lastDeadLetter(pk)
			if err != nil {
				return fmt.Errorf("the last reader of the dead letters: %w", err)
			}
			tag, _, _ := strings.Cut(string(pk.Payload), " ")
			delete(owed, tag+deadLetterTag)
			reasons[reason]++
			if pk.QoS > 0 {
				if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: pk.ID}); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("the last reader of the dead letters: %s", show(pk))
		}
	}
	l.stats.reasons = reasons
	if f.scrape == nil {
		return nil
	}
	after, err := f.scrape()
	if err != nil {
		return err
	}
	return checkQueueCounters(before, after, queueName, &l.stats)
}

// lastDeadLetter checks p, a delivery to the last reader, as a dead letter
// of a job of the run's, and returns its reason: its topic, payload,
// RETAIN 0, its Sagüin properties, its record's, and its saguin-dlq-*.
func (f *fuzzer) lastDeadLetter(p *harness.Publish) (string, error) {
	tag, _, _ := strings.Cut(string(p.Payload), " ")
	dm := f.publications[tag+deadLetterTag]
	if dm == nil || dm.topic != p.Topic {
		return "", fmt.Errorf("delivery %q (%s), no dead letter of a job of the run's", p.Topic, tag)
	}
	if !bytes.Equal(p.Payload, dm.deadOf.m.published()) || p.Retain {
		return "", fmt.Errorf("dead letter %q (%s): payload or RETAIN %v not the job's, RETAIN 0 (design.md, Channels, queue; SUBSCRIBE, Options)", p.Topic, tag, p.Retain)
	}
	got := map[string][]string{}
	for _, q := range p.Props {
		if q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-") {
			got[q.Str] = append(got[q.Str], q.Value)
		}
	}
	for _, name := range []string{"saguin-id", "saguin-offset", "saguin-timestamp"} {
		// saguin-id once, or as often as its publisher sent it ("The
		// publisher's own User Properties go unaltered").
		if v := got[name]; len(v) != 1 && (name != "saguin-id" || len(v) == 0 || len(v) != dm.saguinIDs) {
			return "", fmt.Errorf("dead letter %q (%s): %s %q; want it once (design.md, Sagüin properties)", p.Topic, tag, name, v)
		}
	}
	if err := dm.compare(got["saguin-id"][0], got["saguin-offset"][0], got["saguin-timestamp"][0], "the last reader"); err != nil {
		return "", err
	}
	if _, ok := p.Props.Get(harness.MessageExpiryInterval); ok != dm.expiring || (len(got["saguin-expires"]) == 1) != dm.expiring {
		return "", fmt.Errorf("dead letter %q (%s): Message Expiry present %v, saguin-expires %q, its publisher setting an expiry %v: \"Its Message Expiry stays as it was published\" (design.md, Channels, queue)", p.Topic, tag, ok, got["saguin-expires"], dm.expiring)
	}
	if err := f.checkDeadProps(dm, got); err != nil {
		return "", fmt.Errorf("dead letter %q (%s): %w (design.md, Sagüin properties; Channels, queue)", p.Topic, tag, err)
	}
	return got["saguin-dlq-reason"][0], nil
}

// The queue's counters (metrics.md, Queues).
const (
	deadLetteredTotal   = "saguin_queue_dead_lettered_total"
	attemptsFailedTotal = "saguin_queue_attempts_failed_total"
	deliveredTotal      = "saguin_queue_delivered_total"
	acknowledgedTotal   = "saguin_queue_acknowledged_total"
)

// checkQueueCounters checks queue's counters, scraped before and after the
// last reader read every dead letter, against what the run saw (metrics.md,
// Queues): saguin_queue_dead_lettered_total by reason, "jobs moved to
// <channel>__dlq", counts each move once, so for each reason it is at most
// the dead letters the reader read before and at least those after, every
// move counted being a dead letter there; attempts_failed_total, a refusing
// PUBACK counting as return, is at most the returns and refusals sent for
// that cause and the PUBACKs 0x00 sent for a timeout, each failing at most
// one attempt; delivered_total, "attempts handed to workers", is at least
// the attempts the workers read; acknowledged_total, "ack responses" that
// took effect, at least the jobs found finished, finished being acked, and
// at most the acks sent.
func checkQueueCounters(before, after testkit.Exposition, queue string, st *queueStats) error {
	value := func(e testkit.Exposition, name string, labels ...string) (float64, error) {
		v, ok := e.Values[testkit.SeriesKey(name, append([]string{"channel", queue}, labels...)...)]
		if !ok {
			return 0, fmt.Errorf("/metrics has no %s for channel %s %v (metrics.md, Rules: \"Every series exists from start\")", name, queue, labels)
		}
		return v, nil
	}
	var errs []error
	for _, reason := range []string{"expired", "evicted", "attempts_exhausted"} {
		lo, err1 := value(before, deadLetteredTotal, "reason", reason)
		hi, err2 := value(after, deadLetteredTotal, "reason", reason)
		if err := cmp.Or(err1, err2); err != nil {
			errs = append(errs, err)
			continue
		}
		if n := float64(st.reasons[reason]); n < lo || n > hi {
			errs = append(errs, fmt.Errorf("%s{reason=%q} read %v before the last reader and %v after; it read %v such dead letters: each move is counted once (metrics.md, Queues)", deadLetteredTotal, reason, lo, hi, n))
		}
	}
	for _, c := range []struct {
		cause string
		most  int
	}{{"return", st.returns + st.refusing}, {"timeout", st.puback0}} {
		v, err := value(after, attemptsFailedTotal, "cause", c.cause)
		switch {
		case err != nil:
			errs = append(errs, err)
		case v > float64(c.most):
			errs = append(errs, fmt.Errorf("%s{cause=%q} reads %v, more than the %d answers that could fail an attempt so (metrics.md, Queues: \"return (a refusing PUBACK included), timeout (a hold past visibility_timeout)\")", attemptsFailedTotal, c.cause, v, c.most))
		}
	}
	if v, err := value(after, deliveredTotal); err != nil {
		errs = append(errs, err)
	} else if v < float64(st.offers) {
		errs = append(errs, fmt.Errorf("%s reads %v, fewer than the %d attempts the workers read (metrics.md, Queues: \"attempts handed to workers\")", deliveredTotal, v, st.offers))
	}
	if v, err := value(after, acknowledgedTotal); err != nil {
		errs = append(errs, err)
	} else if v < float64(st.completed) || v > float64(st.acks) {
		errs = append(errs, fmt.Errorf("%s reads %v; want from the %d jobs found finished through the %d acks sent (metrics.md, Queues: \"ack responses\")", acknowledgedTotal, v, st.completed, st.acks))
	}
	return errors.Join(errs...)
}
