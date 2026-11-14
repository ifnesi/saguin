package conformance

import (
	"bytes"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of queue jobs' failed attempts, their retries and dead letters,
// from the wire (design.md, Delivery, Queue jobs; Channels, queue; Restart,
// Restored; $saguin topics, the response row; Sagüin properties;
// metrics.md, Queues; storage.md, queue_jobs). Each test runs a broker of
// its own holding a queue named and filtered as ordersQueue (queue_test.go),
// with the retry settings it needs, and its workers answer as those of
// worker_test.go do.

// retrying is ordersQueue with retry.mode mode, retry.delay delay and, where
// attempts is not 0, retry.max_attempts attempts (config.md, channels.<name>).
func retrying(mode string, delay time.Duration, attempts int) harness.Channel {
	c := ordersQueue
	c.RetryMode, c.RetryDelay, c.RetryMaxAttempts = mode, delay, attempts
	return c
}

// awaitJob waits until db holds ordersQueue's unfinished job at offset and
// ok reports true of it, and returns it; want says what ok looks for.
// harness.DefaultTimeout is the test's watchdog: a response's PUBACK
// "confirms receipt only" ($saguin topics), so an attempt's end is waited
// for in the database.
func awaitJob(t *testing.T, db *sql.DB, offset int64, want string, ok func(j job) bool) job {
	t.Helper()
	var found job
	awaitState(t, fmt.Sprintf("orders' job at offset %d with %s", offset, want),
		func() []job { return unfinished(t, db, ordersQueue.Name) },
		func(js []job) bool {
			i := slices.IndexFunc(js, func(j job) bool { return j.offset == offset })
			if i < 0 {
				return false
			}
			found = js[i]
			return ok(found)
		})
	return found
}

// expectNextTry fails the test unless j, a job whose attempt ended no
// earlier than ended and no later than seen, may be offered again wait
// after that end, in Unix milliseconds (design.md, Delivery, Queue jobs:
// "The wait starts at the attempt's end"; storage.md, queue_jobs: next_try,
// "when it may be offered again"). when says when.
func expectNextTry(t *testing.T, j *job, wait time.Duration, ended, seen time.Time, when string) {
	t.Helper()
	from, through := ended.Add(wait).UnixMilli(), seen.Add(wait).UnixMilli()
	if !j.nextTry.Valid || j.nextTry.Int64 < from || j.nextTry.Int64 > through {
		t.Fatalf("%s, the job's next try is %+v; want Unix milliseconds from %d through %d, %v after its attempt's end", when, j.nextTry, from, through, wait)
	}
}

// offeredAgain reads c's next packet and fails the test unless it is a new
// offer of p's job: a first delivery at QoS 1 of its topic and payload,
// whose Correlation Data is an attempt's other than earlier's (design.md,
// Words: Attempt, "an attempt is one try at giving a queue job to a worker.
// It carries a broker-made id as its Correlation Data"), read no earlier
// than notBefore. It returns the offer.
func offeredAgain(t *testing.T, c *harness.Client, p *harness.Publish, earlier []byte, notBefore time.Time) *harness.Publish {
	t.Helper()
	offer := support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
	if read := time.Now(); read.Before(notBefore) {
		t.Errorf("the offer of %s came again %v before its wait ended; want it after", p.Topic, notBefore.Sub(read))
	}
	if bytes.Equal(attemptOf(t, offer), earlier) {
		t.Errorf("the new offer of %s carries the Correlation Data %q of its earlier attempt; want a new attempt's", p.Topic, earlier)
	}
	return offer
}

// expectAttemptNumber fails the test unless offer carries saguin-attempt n
// once (design.md, Sagüin properties: "saguin-attempt | queue jobs sent to
// workers"; "saguin-attempt (from 1) [...] decimal").
func expectAttemptNumber(t *testing.T, offer *harness.Publish, n int) {
	t.Helper()
	if got := offer.Props.User("saguin-attempt"); !slices.Equal(got, []string{strconv.Itoa(n)}) {
		t.Errorf("the offer of %s carries saguin-attempt %q; want once %d", offer.Topic, got, n)
	}
}

// design.md, Delivery, Queue jobs: "A failed attempt is offered again after
// a wait set by retry.mode"; config.md, channels.<name>: "In fixed mode, the
// retry wait is retry.delay"; "The
// wait starts at the attempt's end, at its return"; "An attempt is spent at
// the worker's PUBACK or its response naming the attempt, whichever is
// first"; $saguin topics, the response row: "ack or return a job, with its
// attempt's Correlation Data"; Sagüin properties: "saguin-attempt (from
// 1)"; storage.md, queue_jobs: spent "attempts spent", failed "attempts
// failed", next_try "when it may be offered again".
//
// The queue orders retries after 500ms, fixed. A worker acknowledges the
// offer of orders/1 and answers return: the job is unfinished, one attempt
// spent and one failed, its next try 500ms after the return. The offer
// comes again no earlier, saguin-attempt 2, with a new attempt's
// Correlation Data; the worker completes it, and the job is finished.
func TestReturnedJobIsOfferedAgainAfterTheDelay(t *testing.T) {
	const delay = 500 * time.Millisecond
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", delay, 0)}})
	db := support.OpenDB(t, b.DB)
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	offset := channelRecords(t, b.DB, ordersQueue.Name)[0].offset

	first := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	acknowledge(t, w, first.ID, 0)
	returned := time.Now()
	respond(t, w, attemptOf(t, first), "return", 0x00)
	j := awaitJob(t, db, offset, "one attempt failed", func(j job) bool { return j.failed == 1 })
	seen := time.Now()
	if j.spent != 1 {
		t.Errorf("after its return, the job has %d attempts spent; want 1, spent at its PUBACK", j.spent)
	}
	expectNextTry(t, &j, delay, returned, seen, "after its return")

	again := offeredAgain(t, w, p, attemptOf(t, first), returned.Add(delay))
	expectAttemptNumber(t, again, 2)
	completed(t, w, again)
	awaitUnfinished(t, db, nil, "once the worker has answered ack for the job's second attempt")
}

// design.md, Delivery, Queue jobs: "A failed attempt is offered again after
// a wait set by retry.mode"; "The wait starts at the attempt's end, at its
// return [...] The wait has no maximum"; config.md, channels.<name>: "In
// fixed mode, the retry wait is retry.delay. In linear mode, it is
// retry.delay x n. In exponential mode, it is retry.delay x 2^(n-1). Here n
// is the number of failed attempts so far";
// retry.mode, "fixed, linear or exponential: how the wait before a job is
// offered again grows"; retry.delay, "the wait retry.mode starts from";
// storage.md, queue_jobs: failed, "the n of retry.mode".
//
// For each mode, the queue orders retries from 200ms. A worker acknowledges
// the offer of orders/1 and answers return, three times: after the nth
// return the job has n attempts failed, and its next try is, after that
// return, 200, 200 and 200ms (fixed), 200, 400 and 600ms (linear), or 200,
// 400 and 800ms (exponential); each offer comes again no earlier, with
// saguin-attempt n+1. The worker completes the fourth.
func TestRetryWaitGrowsByMode(t *testing.T) {
	const delay = 200 * time.Millisecond
	for _, tc := range []struct {
		mode  string
		waits []time.Duration
	}{
		{"fixed", []time.Duration{delay, delay, delay}},
		{"linear", []time.Duration{delay, 2 * delay, 3 * delay}},
		{"exponential", []time.Duration{delay, 2 * delay, 4 * delay}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying(tc.mode, delay, 0)}})
			db := support.OpenDB(t, b.DB)
			w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			p := support.Sized("orders/1", testkit.RecordBytes)
			support.Acknowledged(t, pub, p)
			offset := channelRecords(t, b.DB, ordersQueue.Name)[0].offset

			offer := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
			for i, wait := range tc.waits {
				n := int64(i + 1)
				acknowledge(t, w, offer.ID, 0)
				returned := time.Now()
				respond(t, w, attemptOf(t, offer), "return", 0x00)
				j := awaitJob(t, db, offset, strconv.FormatInt(n, 10)+" attempts failed", func(j job) bool { return j.failed == n })
				expectNextTry(t, &j, wait, returned, time.Now(), "after return "+strconv.FormatInt(n, 10))
				offer = offeredAgain(t, w, p, attemptOf(t, offer), returned.Add(wait))
				expectAttemptNumber(t, offer, i+2)
			}
			completed(t, w, offer)
			awaitUnfinished(t, db, nil, "once the worker has answered ack for the fourth attempt")
		})
	}
}

// design.md, Delivery, Queue jobs: "The hold starts at the worker's PUBACK
// and lasts visibility_timeout"; "The wait starts at the attempt's end, at
// [...] its timeout"; $saguin topics, the response row: "A response naming
// an attempt that is not current or not this session's, or with another
// payload, changes nothing and is logged"; Flows, Logging: a line at a
// level the default log_level writes (loggedNaming). metrics.md, Queues:
// saguin_queue_attempts_failed_total, "timeout (a hold past
// visibility_timeout)"; saguin_queue_stale_responses_total, "responses that
// changed nothing: naming an attempt not current"; saguin_queue_acknowledged_total.
//
// The queue orders holds jobs for a second and retries after a second,
// fixed. A worker acknowledges the offer of orders/1 and answers nothing:
// once the hold has run out, the job has one attempt spent and one failed,
// its next try a second after the hold's end, and one attempt is counted
// failed by timeout. The worker then answers ack for that attempt: PUBACK
// 0x00, one stale response and no ack counted, and the job is still
// unfinished, one attempt spent and one failed. The offer comes again no
// earlier than the hold and the wait together, saguin-attempt 2, with a new
// attempt's Correlation Data; the worker completes it, one ack counted. Once
// the broker has stopped, its log names the worker.
func TestTimedOutHoldEndsItsAttempt(t *testing.T) {
	t.Parallel()
	const visibility, delay = time.Second, time.Second
	q := retrying("fixed", delay, 0)
	q.VisibilityTimeout = visibility
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{q}})
	db := support.OpenDB(t, b.DB)
	workerID := clientID(t, "-worker")
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: workerID, CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	offset := channelRecords(t, b.DB, ordersQueue.Name)[0].offset

	first := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	held := time.Now()
	acknowledge(t, w, first.ID, 0)
	j := awaitJob(t, db, offset, "one attempt failed, its hold run out", func(j job) bool { return j.failed == 1 })
	expectNextTry(t, &j, delay, held.Add(visibility), time.Now(), "once its hold has run out")
	stale, acked := support.ChannelSeries("saguin_queue_stale_responses_total", ordersQueue.Name), support.ChannelSeries("saguin_queue_acknowledged_total", ordersQueue.Name)
	awaitSeries(t, b, map[string]float64{support.ChannelSeries("saguin_queue_attempts_failed_total", ordersQueue.Name, "cause", "timeout"): 1, stale: 0, acked: 0})

	respond(t, w, attemptOf(t, first), "ack", 0x00)
	awaitSeries(t, b, map[string]float64{stale: 1, acked: 0})
	if late := awaitJob(t, db, offset, "its attempts unchanged", func(job) bool { return true }); late.spent != 1 || late.failed != 1 {
		t.Errorf("after a late ack, the job has %d attempts spent and %d failed; want 1 and 1, unchanged", late.spent, late.failed)
	}

	again := offeredAgain(t, w, p, attemptOf(t, first), held.Add(visibility+delay))
	expectAttemptNumber(t, again, 2)
	completed(t, w, again)
	awaitSeries(t, b, map[string]float64{stale: 1, acked: 1})
	stopped(t, b)
	loggedNaming(t, b, workerID, "whose ack named an attempt whose hold had run out")
}

// design.md, Delivery, Queue jobs: "A refusing PUBACK (reason 0x80 or
// above) spends the attempt and ends it as a failure, as return does";
// "The wait starts at the attempt's end, at its return, a refusing PUBACK
// [...]"; storage.md, queue_jobs: spent, failed, first_attempt, next_try.
// MQTT 5 3.4.2.1, Table 3-4: 0x83 "Implementation specific error", "The
// PUBLISH is valid but the receiver is not willing to accept it".
//
// The queue orders retries after 500ms, fixed. A worker answers the offer
// of orders/1 with PUBACK 0x83: the job has one attempt spent, with its
// first attempt, and one failed, its next try 500ms after that PUBACK. The
// offer comes again no earlier, saguin-attempt 2, with a new attempt's
// Correlation Data; the worker completes it.
func TestRefusingPubackFailsTheAttempt(t *testing.T) {
	const delay = 500 * time.Millisecond
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", delay, 0)}})
	db := support.OpenDB(t, b.DB)
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	offset := channelRecords(t, b.DB, ordersQueue.Name)[0].offset

	first := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	refused := time.Now()
	acknowledge(t, w, first.ID, 0x83)
	j := awaitJob(t, db, offset, "one attempt failed", func(j job) bool { return j.failed == 1 })
	if j.spent != 1 || !j.firstAttempt.Valid {
		t.Errorf("after the refusing PUBACK, the job has %d attempts spent, first attempt %+v; want one spent, with its beginning", j.spent, j.firstAttempt)
	}
	expectNextTry(t, &j, delay, refused, time.Now(), "after the refusing PUBACK")

	again := offeredAgain(t, w, p, attemptOf(t, first), refused.Add(delay))
	expectAttemptNumber(t, again, 2)
	completed(t, w, again)
	awaitUnfinished(t, db, nil, "once the worker has answered ack for the job's second attempt")
}

// design.md, Delivery, Queue jobs: "The wait starts at the attempt's end,
// at its return, a refusing PUBACK, its timeout or the worker's going"; "An
// attempt is spent at the worker's PUBACK or its response naming the
// attempt, whichever is first [...] An attempt that ends with neither comes
// back unspent"; DISCONNECT, A clean DISCONNECT: "Queue jobs it held go
// back"; storage.md, queue_jobs: spent, failed, first_attempt "nullable:
// when its first spent attempt began", next_try.
//
// The queue orders retries after 500ms, fixed. A publisher stores orders/1.
// Worker -c, its session ending with its connection, receives the job's
// offer and disconnects with neither PUBACK nor answer. Worker -a receives
// the offer: the job has spent no attempt and has no first attempt. -a
// acknowledges it, and once its attempt is spent, disconnects: the job has
// one attempt failed more than before, its next try 500ms after -a's going.
// Worker -b, subscribing then, receives the offer no earlier, with a new
// attempt's Correlation Data, and completes it.
func TestWorkerGoingEndsItsAttempt(t *testing.T) {
	const delay = 500 * time.Millisecond
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", delay, 0)}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	offset := channelRecords(t, b.DB, ordersQueue.Name)[0].offset

	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-c"), CleanStart: true}, support.At(1, ordersWorker))
	support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
	support.Disconnected(t, c, &harness.Disconnect{})
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-a"), CleanStart: true}, support.At(1, ordersWorker))
	offer := support.DeliveredAt(t, a, 1, p.Topic, p.Payload)
	unspent := awaitJob(t, db, offset, "its record", func(job) bool { return true })
	if unspent.spent != 0 || unspent.firstAttempt.Valid {
		t.Errorf("once -c has gone with neither PUBACK nor answer, the job has %d attempts spent, first attempt %+v; want none spent, no first attempt", unspent.spent, unspent.firstAttempt)
	}

	acknowledge(t, a, offer.ID, 0)
	spent := awaitJob(t, db, offset, "one attempt spent", func(j job) bool { return j.spent == 1 })
	gone := time.Now()
	support.Disconnected(t, a, &harness.Disconnect{})
	j := awaitJob(t, db, offset, "one attempt failed more", func(j job) bool { return j.failed == spent.failed+1 })
	expectNextTry(t, &j, delay, gone, time.Now(), "once -a, holding the job, has gone")

	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, ordersWorker))
	completed(t, bb, offeredAgain(t, bb, p, attemptOf(t, offer), gone.Add(delay)))
	awaitUnfinished(t, db, nil, "once -b has answered ack")
}

// design.md, Delivery, Queue jobs: "When an attempt fails with
// retry.max_attempts spent, the job moves to the dead-letter channel as
// attempts_exhausted"; Channels, queue: "It moves the job to the append
// channel <channel>__dlq in one transaction. The topic gains the prefix
// __dlq/"; "The moved job carries saguin-dlq-reason. Its value is [...]
// attempts_exhausted"; Sagüin properties: "saguin-dlq-* | dead letters:
// saguin-dlq-reason, -channel, -offset, -attempts, -at, and where an
// attempt was spent -first and -last"; "-first and -last are its first and
// last attempt" (expectDeadLetter); storage.md, queue_jobs: first_attempt
// and last_attempt, "when its first [last] spent attempt began".
//
// The queue orders allows two attempts and retries after 200ms, fixed. A
// publisher stores orders/1 carrying a Content Type. A worker acknowledges
// its offer and answers return, twice: orders__dlq then holds
// __dlq/orders/1, and orders no unfinished job. A reader subscribing at
// QoS 1 to __dlq/orders/# receives it, with saguin-dlq-reason
// attempts_exhausted, saguin-dlq-attempts 2, saguin-dlq-first when the
// first offer could have begun, saguin-dlq-last when the second could have,
// and saguin-dlq-at no earlier than the second return.
func TestExhaustedJobMovesAsAttemptsExhausted(t *testing.T) {
	const delay = 200 * time.Millisecond
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", delay, 2)}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("order 1"), Props: harness.Properties{{ID: harness.ContentType, Str: "text/plain"}}}
	support.Acknowledged(t, pub, p)
	rs := channelRecords(t, b.DB, ordersQueue.Name)

	subscribing := time.Now()
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	first := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	firstRead := time.Now()
	acknowledge(t, w, first.ID, 0)
	returned := time.Now()
	respond(t, w, attemptOf(t, first), "return", 0x00)
	second := offeredAgain(t, w, p, attemptOf(t, first), returned.Add(delay))
	secondRead := time.Now()
	acknowledge(t, w, second.ID, 0)
	exhausted := time.Now()
	respond(t, w, attemptOf(t, second), "return", 0x00)
	awaitHeld(t, db, deadLetters, deadTopics(p.Topic), "once its second spent attempt has failed")
	moved := time.Now()
	if js := unfinished(t, db, ordersQueue.Name); len(js) != 0 {
		t.Errorf("once the job has moved, orders' unfinished jobs are %+v; want none", js)
	}

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload)
	expectDeadLetter(t, got, &rs[0], p.Props, "attempts_exhausted", exhausted, moved,
		&spentAttempts{n: 2, firstAfter: subscribing, firstBefore: firstRead, lastAfter: returned, lastBefore: secondRead})
}

// design.md, Channels, queue: "Retention never deletes an unfinished job";
// "A held job moves when its hold ends"; "Its value is expired when the job
// is past retention_period"; "When several dead-letter causes apply,
// expired wins"; Delivery, Queue jobs: "The hold starts at the worker's
// PUBACK and lasts visibility_timeout"; "A job past retention_period moves
// to the dead-letter channel when retention reaches it, with no attempt
// spent, or when its hold ends"; Sagüin properties (expectDeadLetter).
//
// The queue orders keeps jobs for 2 seconds and holds them for 1. A
// publisher stores orders/1. Half a second before it is past
// retention_period, a worker subscribes and acknowledges its offer, and
// answers nothing. orders__dlq then holds __dlq/orders/1, and orders no
// unfinished job. A reader subscribing at QoS 1 to __dlq/orders/# receives
// it with saguin-dlq-reason expired, saguin-dlq-attempts 1, saguin-dlq-first
// and saguin-dlq-last when the offer could have begun, and saguin-dlq-at no
// earlier than the end of the hold, past the job's retention_period.
func TestHeldJobPastRetentionMovesWhenItsHoldEnds(t *testing.T) {
	const period, visibility = 2 * time.Second, time.Second
	q := ordersQueue
	q.RetentionPeriod, q.VisibilityTimeout = period, visibility
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{q}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	rs := channelRecords(t, b.DB, ordersQueue.Name)
	due := time.UnixMilli(rs[0].time).Add(period)
	time.Sleep(time.Until(due.Add(-visibility / 2))) // the hold must span the job's retention deadline, the timing under test

	subscribing := time.Now()
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	offer := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	held := time.Now()
	acknowledge(t, w, offer.ID, 0)
	if !held.Before(due) {
		t.Fatalf("the worker acknowledged its offer %v after the job's retention deadline; want its hold begun before it", held.Sub(due))
	}
	awaitHeld(t, db, deadLetters, deadTopics(p.Topic), "once its hold, past retention_period, has ended")
	moved := time.Now()
	if js := unfinished(t, db, ordersQueue.Name); len(js) != 0 {
		t.Errorf("once the job has moved, orders' unfinished jobs are %+v; want none", js)
	}

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload)
	expectDeadLetter(t, got, &rs[0], nil, "expired", held.Add(visibility), moved,
		&spentAttempts{n: 1, firstAfter: subscribing, firstBefore: held, lastAfter: subscribing, lastBefore: held})
}

// design.md, Delivery, Queue jobs: "When an attempt fails with
// retry.max_attempts spent, the job moves to the dead-letter channel as
// attempts_exhausted"; "An attempt is spent at the worker's PUBACK or its
// response naming the attempt, whichever is first [...] An attempt that
// ends with neither comes back unspent"; storage.md, queue_jobs: spent,
// "retry.max_attempts counts these"; Sagüin properties: saguin-dlq-attempts
// and -first (expectDeadLetter).
//
// The queue orders allows one attempt and retries after 200ms, fixed. A
// publisher stores orders/1. Workers -a and -b, each in turn, receive its
// offer and disconnect with neither PUBACK nor answer. Worker -c receives
// the offer: the job is still unfinished, no attempt spent. -c
// acknowledges it and answers return: orders__dlq then holds
// __dlq/orders/1, which a reader subscribing at QoS 1 to __dlq/orders/#
// receives with saguin-dlq-reason attempts_exhausted, saguin-dlq-attempts 1,
// and saguin-dlq-first and saguin-dlq-last when -c's offer could have begun.
func TestUnspentEndingsDoNotCountTowardMaxAttempts(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", 200*time.Millisecond, 1)}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	rs := channelRecords(t, b.DB, ordersQueue.Name)

	for _, suffix := range []string{"-a", "-b"} {
		w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, suffix), CleanStart: true}, support.At(1, ordersWorker))
		support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
		support.Disconnected(t, w, &harness.Disconnect{})
	}
	subscribing := time.Now()
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-c"), CleanStart: true}, support.At(1, ordersWorker))
	offer := support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
	read := time.Now()
	if j := awaitJob(t, db, rs[0].offset, "its record", func(job) bool { return true }); j.spent != 0 {
		t.Errorf("after two workers went with neither PUBACK nor answer, the job has %d attempts spent; want none", j.spent)
	}
	acknowledge(t, c, offer.ID, 0)
	exhausted := time.Now()
	respond(t, c, attemptOf(t, offer), "return", 0x00)
	awaitHeld(t, db, deadLetters, deadTopics(p.Topic), "once its one spent attempt has failed")
	moved := time.Now()

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload)
	expectDeadLetter(t, got, &rs[0], nil, "attempts_exhausted", exhausted, moved,
		&spentAttempts{n: 1, firstAfter: subscribing, firstBefore: read, lastAfter: subscribing, lastBefore: read})
}

// design.md, Restart, Restored: "The broker also restores [...] queue
// attempt counts and next retry times [...] Queue holds are not restored";
// "Restart ends every pre-crash queue attempt as an application attempt.
// Stored spent and failed counts and next_try remain. A job with no future
// next_try is ready again. Recovered MQTT exchanges still finish with their
// original identifiers and attempt data. Their acknowledgements and
// responses cannot settle a new attempt or spend another attempt. A new
// offer gets a new attempt id"; $saguin topics, the response row: "A
// response naming an attempt that is not current [...] changes nothing and
// is logged"; Store: full mode, "A restart recovers committed state";
// config.md, channels.<name>: visibility_timeout "30s".
//
// The queue orders retries after 3 seconds, fixed, and holds jobs for
// config.md's 30 seconds. A publisher stores orders/a and orders/b. A
// worker holding a durable session acknowledges the offer of orders/a and
// answers return, then acknowledges the offer of orders/b, which it then
// holds; the broker is killed. Started again, the database holds both jobs
// as before the crash. The worker resumes its session and receives the
// offer of orders/b at once, saguin-attempt 2, with a new attempt's
// Correlation Data. It answers ack naming orders/b's pre-crash attempt:
// PUBACK 0x00, and orders/b's job is unfinished, one attempt spent. It
// completes the new offer; orders/a's offer comes no earlier than 3
// seconds after its return, saguin-attempt 2, and the worker completes it.
// Once the broker has stopped, its log names the worker.
func TestRestartRestoresAttemptsButNoHolds(t *testing.T) {
	t.Parallel()
	const delay = 3 * time.Second
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{retrying("fixed", delay, 0)}}
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	workerID := clientID(t, "-worker")
	connect := kept(harness.V5, workerID)
	w := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/a", testkit.RecordBytes), support.Sized("orders/b", testkit.RecordBytes)}
	support.PublishAll(t, pub, ps)
	rs := channelRecords(t, b.DB, ordersQueue.Name)

	first := support.DeliveredAt(t, w, 1, ps[0].Topic, ps[0].Payload)
	acknowledge(t, w, first.ID, 0)
	returned := time.Now()
	respond(t, w, attemptOf(t, first), "return", 0x00)
	awaitJob(t, db, rs[0].offset, "one attempt failed", func(j job) bool { return j.failed == 1 })
	second := support.DeliveredAt(t, w, 1, ps[1].Topic, ps[1].Payload)
	acknowledge(t, w, second.ID, 0)
	awaitJob(t, db, rs[1].offset, "one attempt spent", func(j job) bool { return j.spent == 1 })
	// Both offers acknowledged, no MQTT exchange is left to recover: the
	// worker's next packet after it resumes is a new offer, not a resend.
	awaitState(t, "with both offers acknowledged, no stored exchange before the crash",
		func() []inflightRow { return storedInflight(t, db, workerID) },
		func(rows []inflightRow) bool { return len(rows) == 0 })
	before := unfinished(t, db, ordersQueue.Name)
	support.Crash(t, b)

	b = support.Launch(t, o)
	db = support.OpenDB(t, b.DB)
	if after := unfinished(t, db, ordersQueue.Name); !slices.Equal(after, before) {
		t.Errorf("after the crash, orders' unfinished jobs are %+v; want %+v, their counts and next tries as stored", after, before)
	}
	w = resumed(t, b.Addr, harness.V5, connect)
	again := offeredAgain(t, w, ps[1], attemptOf(t, second), time.Time{})
	expectAttemptNumber(t, again, 2)
	respond(t, w, attemptOf(t, second), "ack", 0x00)
	if j := awaitJob(t, db, rs[1].offset, "its record", func(job) bool { return true }); j.spent != 1 {
		t.Errorf("after an ack naming its pre-crash attempt, orders/b's job has %d attempts spent; want 1, the new attempt not spent by it", j.spent)
	}
	completed(t, w, again)
	awaitUnfinished(t, db, []string{ps[0].Topic}, "once the worker has answered ack for orders/b's new attempt")

	later := offeredAgain(t, w, ps[0], attemptOf(t, first), returned.Add(delay))
	expectAttemptNumber(t, later, 2)
	completed(t, w, later)
	awaitUnfinished(t, db, nil, "once the worker has answered ack for orders/a's second attempt")
	stopped(t, b)
	loggedNaming(t, b, workerID, "whose ack named its pre-crash attempt")
}

// design.md, Delivery, A resend: "A resend uses the Subscription
// Identifiers and delivery properties saved for its first transmission";
// Sending: "Packet identifiers use the lowest free identifier"; CONNECT,
// 9. Resume: "The broker reads in-flight messages and PUBRELs from the
// store and resends them with their packet identifiers and in their order.
// Resent messages have DUP set (MQTT-4.4.0-1)"; Delivery, Queue jobs: "A
// failed attempt is offered again after a wait set by retry.mode"; "The
// wait starts at the attempt's end, at its return"; Restart, Restored:
// "Recovered MQTT exchanges still finish with their original identifiers
// and attempt data"; Words: Attempt, "It carries a broker-made id as its
// Correlation Data"; Sagüin properties: "saguin-attempt (from 1)";
// storage.md, session_inflight: attempt, "a queue job's attempt id, its
// Correlation Data (Words: Attempt), so a resend is the same message";
// attempt_number, "the job's saguin-attempt as sent". MQTT 5 2.2.1: "The
// Packet Identifier becomes available for reuse after the sender has
// processed the corresponding acknowledgement packet [...] In the case of
// a QoS 1 PUBLISH, this is the corresponding PUBACK".
//
// The queue orders retries after 20ms, fixed. A worker holding a durable
// session receives the offer of orders/1 under packet identifier 1,
// saguin-attempt 1, and acknowledges it; once its stored exchange is gone,
// identifier 1 free again, the worker answers return. The job's next offer
// comes under identifier 1 again, saguin-attempt 2, with a new attempt's
// Correlation Data. The worker goes away without acknowledging it and
// resumes its session: identifier 1 is resent, DUP 1, with that second
// offer's properties, its own first transmission's, not the first
// offer's, which shared its identifier.
func TestOfferUnderAReusedIdentifierIsResentAsItself(t *testing.T) {
	const delay = 20 * time.Millisecond
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", delay, 0)}})
	db := support.OpenDB(t, b.DB)
	workerID := clientID(t, "-worker")
	connect := kept(harness.V5, workerID)
	w := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	job := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, job)

	first := support.DeliveredAt(t, w, 1, job.Topic, job.Payload)
	expectAttemptNumber(t, first, 1)
	acknowledge(t, w, first.ID, 0)
	awaitState(t, "with the first offer acknowledged, no stored exchange",
		func() []inflightRow { return storedInflight(t, db, workerID) },
		func(rows []inflightRow) bool { return len(rows) == 0 })
	returned := time.Now()
	respond(t, w, attemptOf(t, first), "return", 0x00)
	second := offeredAgain(t, w, job, attemptOf(t, first), returned.Add(delay))
	expectAttemptNumber(t, second, 2)
	if second.ID != first.ID {
		t.Fatalf("the second offer came under packet identifier %d; want %d, the first offer's, freed by its PUBACK and the lowest free", second.ID, first.ID)
	}

	support.Away(t, w)
	w = resumed(t, b.Addr, harness.V5, connect)
	again := redelivered(t, w, second)
	if diff := sameProperties(again.Props, second.Props); diff != "" {
		t.Errorf("the resend under identifier %d carries %s, its own first transmission's", again.ID, diff)
	}
}

// metrics.md, Queues: saguin_queue_jobs{channel,state}, "unfinished jobs:
// ready (can be offered), backoff (waiting out its retry.mode wait),
// offered (sent, its PUBACK not yet in) or held (with a worker)";
// saguin_queue_attempts_failed_total{channel,cause}, "failed attempts:
// return (a refusing PUBACK included), timeout (a hold past
// visibility_timeout) or worker_gone";
// saguin_queue_dead_lettered_total{channel,reason}, "jobs moved to
// <channel>__dlq: [...] attempts_exhausted"; Rules: "Every series exists
// from start, for every value of its labels that applies [...] Counters
// start at 0". design.md, Delivery, Queue jobs: "The hold starts at the
// worker's PUBACK and lasts visibility_timeout"; "The wait starts at the
// attempt's end, at its return, a refusing PUBACK, its timeout or the
// worker's going"; "When an attempt fails with retry.max_attempts spent,
// the job moves to the dead-letter channel as attempts_exhausted".
//
// The queue orders allows three attempts, holds jobs for a second and
// retries after a second, fixed. Every series of these reads 0. A
// publisher stores a job: one ready. A worker subscribes and receives its
// offer: one offered. It acknowledges it: one held. It answers return: one
// in backoff, one failure by return. The offer comes again: one offered;
// it acknowledges it: one held. The hold runs out: one in backoff, one
// failure by timeout. The offer comes again, and the worker acknowledges
// it and disconnects: no job, one failure by worker_gone, one job dead
// lettered as attempts_exhausted.
func TestFailureSeriesFollowEachTransition(t *testing.T) {
	t.Parallel()
	q := retrying("fixed", time.Second, 3)
	q.VisibilityTimeout = time.Second
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{q}})
	type counts struct{ ready, backoff, offered, held, returned, timedOut, gone, exhausted float64 }
	await := func(c counts) {
		t.Helper()
		awaitSeries(t, b, map[string]float64{
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "ready"):                        c.ready,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "backoff"):                      c.backoff,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "offered"):                      c.offered,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "held"):                         c.held,
			support.ChannelSeries("saguin_queue_attempts_failed_total", ordersQueue.Name, "cause", "return"):      c.returned,
			support.ChannelSeries("saguin_queue_attempts_failed_total", ordersQueue.Name, "cause", "timeout"):     c.timedOut,
			support.ChannelSeries("saguin_queue_attempts_failed_total", ordersQueue.Name, "cause", "worker_gone"): c.gone,
			deadLetteredSeries(ordersQueue.Name, "attempts_exhausted"):                                            c.exhausted,
		})
	}
	await(counts{})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, p)
	await(counts{ready: 1})

	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	offer := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	await(counts{offered: 1})
	acknowledge(t, w, offer.ID, 0)
	await(counts{held: 1})
	respond(t, w, attemptOf(t, offer), "return", 0x00)
	await(counts{backoff: 1, returned: 1})

	offer = offeredAgain(t, w, p, attemptOf(t, offer), time.Time{})
	await(counts{offered: 1, returned: 1})
	acknowledge(t, w, offer.ID, 0)
	await(counts{held: 1, returned: 1})
	await(counts{backoff: 1, returned: 1, timedOut: 1})

	offer = offeredAgain(t, w, p, attemptOf(t, offer), time.Time{})
	acknowledge(t, w, offer.ID, 0)
	await(counts{held: 1, returned: 1, timedOut: 1})
	support.Disconnected(t, w, &harness.Disconnect{})
	await(counts{returned: 1, timedOut: 1, gone: 1, exhausted: 1})
}
