package conformance

import (
	"database/sql"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of queue channels' logs and dead letters: a queue publication
// stored as a record with its unfinished job, readers on the queue's own
// topics, and retention moving unfinished jobs to the dead-letter channel
// (design.md, Channels, queue; Words: Retention; Sagüin properties;
// $saguin topics; storage.md, records and queue_jobs). No worker takes a
// job in these tests (worker offers are item 0017c's), so every record a
// queue holds is an unfinished job.

// ordersQueue is the queue of these tests, holding orders/#, with config.md's
// default retention.
var ordersQueue = harness.Channel{Name: "orders", Type: "queue", Filter: "orders/#"}

// deadLetters is ordersQueue's dead-letter channel, and deadFilter its
// filter: design.md, Channels, queue, "It moves the job to the append
// channel <channel>__dlq"; "The dead-letter channel's filter is the queue's
// filter with the same prefix".
const (
	deadLetters = "orders__dlq"
	deadFilter  = "__dlq/orders/#"
)

// deadTopic is the topic of topic's dead letter: design.md, Channels, queue,
// "The topic gains the prefix __dlq/, whatever the queue's filter. Example:
// orders/a/42 becomes __dlq/orders/a/42".
func deadTopic(topic string) string { return "__dlq/" + topic }

// deadTopics is deadTopic of each of topics, in their order.
func deadTopics(topics ...string) []string {
	dead := make([]string, len(topics))
	for i, topic := range topics {
		dead[i] = deadTopic(topic)
	}
	return dead
}

// job is one row of storage.md's queue_jobs, as the operator reads it.
type job struct {
	offset                             int64
	topic                              string
	spent, failed                      int64
	firstAttempt, lastAttempt, nextTry sql.NullInt64
}

// unfinished reads from db the unfinished jobs of the channel named, in
// offset order (storage.md, queue_jobs: "This table holds a queue's
// unfinished jobs").
func unfinished(t *testing.T, db support.RowsQuerier, channel string) []job {
	t.Helper()
	var js []job
	support.Query(t, db, `SELECT j."offset", j.spent, j.failed, j.first_attempt, j.last_attempt, j.next_try, r.topic
		FROM queue_jobs j JOIN channels c ON c.id = j.channel
		LEFT JOIN records r ON r.channel = j.channel AND r."offset" = j."offset"
		WHERE c.name = ? ORDER BY j."offset"`, func(rows *sql.Rows) error {
		var j job
		err := rows.Scan(&j.offset, &j.spent, &j.failed, &j.firstAttempt, &j.lastAttempt, &j.nextTry, &j.topic)
		js = append(js, j)
		return err
	}, channel)
	return js
}

// jobsMatch reports how js, a queue's unfinished jobs, differ from new jobs
// of rs, the queue's records, or "" where they do not: one job at each
// record's offset, and no other, none spent or failed, no attempt begun,
// and each offered at once (storage.md, queue_jobs: "The record stays in
// records"; spent "attempts spent"; failed "attempts failed"; first_attempt
// and last_attempt "nullable: when its first [last] spent attempt began";
// next_try "NULL for at once").
func jobsMatch(js []job, rs []heldRecord) string {
	if len(js) != len(rs) {
		return fmt.Sprintf("%d unfinished jobs for %d records %q", len(js), len(rs), topicsOf(rs))
	}
	for i, j := range js {
		fresh := j.spent == 0 && j.failed == 0 && !j.firstAttempt.Valid && !j.lastAttempt.Valid && !j.nextTry.Valid
		if j.offset != rs[i].offset || !fresh {
			return fmt.Sprintf("the job %+v for %s at offset %d; want a new job at its record's offset", j, rs[i].topic, rs[i].offset)
		}
	}
	return ""
}

// expectJobs fails the test unless the queue named holds in db the records
// of topics, in offset order, each with its new unfinished job (jobsMatch).
// when says when.
func expectJobs(t *testing.T, db support.RowsQuerier, queue string, topics []string, when string) {
	t.Helper()
	rs := held(t, db, queue)
	if !slices.Equal(topicsOf(rs), topics) {
		t.Fatalf("%s, %s holds %q; want %q", when, queue, topicsOf(rs), topics)
	}
	if diff := jobsMatch(unfinished(t, db, queue), rs); diff != "" {
		t.Errorf("%s, %s holds %s", when, queue, diff)
	}
}

// snapshot runs read on one read transaction of db, every query of which
// sees the same committed state (SQLite's WAL: a reader's snapshot is the
// database as of its first read), and ends the transaction.
func snapshot(t *testing.T, db *sql.DB, read func(tx *sql.Tx)) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }() // it only read
	read(tx)
}

// spentAttempts is what a dead letter's job spent before it moved: n
// attempts, and where n is not 0, the moments its first and last spent
// attempt could have begun, from firstAfter through firstBefore and from
// lastAfter through lastBefore (storage.md, queue_jobs: first_attempt and
// last_attempt, "when its first [last] spent attempt began"). A nil
// *spentAttempts is a job with no attempt spent.
type spentAttempts struct {
	n                                              int64
	firstAfter, firstBefore, lastAfter, lastBefore time.Time
}

// expectDeadLetter fails the test unless got, the delivery to an MQTT 5
// reader of the dead letter of the queue ordersQueue's record job, sent
// with sent's properties, carries what design.md gives it: the job's
// saguin-id (Channels, queue: "It moves the job"); the publisher's
// properties unaltered (Sagüin properties: "Nothing a publisher sends is
// altered"); and the saguin-dlq-* properties of a job that spent spent,
// nil for none ("saguin-dlq-* | dead letters: saguin-dlq-reason, -channel,
// -offset, -attempts, -at, and where an attempt was spent -first and
// -last"), each once: saguin-dlq-reason reason ("expired when the job is past
// retention_period, evicted when retention_bytes causes the move, or
// attempts_exhausted"), saguin-dlq-channel the queue's name,
// saguin-dlq-offset the job's offset and saguin-dlq-attempts spent.n,
// decimal; saguin-dlq-at, "when the job moved", in Unix milliseconds from
// after through before; and, where spent.n is not 0, saguin-dlq-first and
// saguin-dlq-last, "its first and last attempt", in Unix milliseconds within
// spent's bounds ("The values of saguin-timestamp, saguin-expires,
// saguin-dlq-at, -first and -last are Unix milliseconds in decimal"). It
// returns the moment of saguin-dlq-at.
func expectDeadLetter(t *testing.T, got *harness.Publish, job *record, sent harness.Properties, reason string, after, before time.Time, spent *spentAttempts) time.Time {
	t.Helper()
	own, others := splitSaguin(got.Props)
	if ids := own["saguin-id"]; !slices.Equal(ids, []string{job.id}) {
		t.Errorf("the dead letter %q carries saguin-id %q; want once %q, the job's", got.Topic, ids, job.id)
	}
	_, publisher := splitSaguin(sent)
	if diff := sameProperties(others, publisher); diff != "" {
		t.Errorf("the dead letter %q, its saguin- properties aside, carries %s", got.Topic, diff)
	}
	if spent == nil {
		spent = &spentAttempts{}
	}
	want := map[string]string{"saguin-dlq-reason": reason, "saguin-dlq-channel": ordersQueue.Name,
		"saguin-dlq-offset": strconv.FormatInt(job.offset, 10), "saguin-dlq-attempts": strconv.FormatInt(spent.n, 10)}
	dlq := make(map[string][]string)
	for name, values := range own {
		if strings.HasPrefix(name, "saguin-dlq-") {
			dlq[name] = values
		}
	}
	// moment fails the test unless the dead letter carries name once, in
	// Unix milliseconds from from through through, and returns it.
	moment := func(name string, from, through time.Time, what string) time.Time {
		t.Helper()
		values := dlq[name]
		delete(dlq, name)
		if len(values) != 1 {
			t.Fatalf("the dead letter %q carries %s %q; want it once", got.Topic, name, values)
		}
		ms, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || ms < from.UnixMilli() || ms > through.UnixMilli() {
			t.Fatalf("the dead letter %q carries %s %q; want Unix milliseconds from %d through %d, %s", got.Topic, name, values[0], from.UnixMilli(), through.UnixMilli(), what)
		}
		return time.UnixMilli(ms)
	}
	at := moment("saguin-dlq-at", after, before, "when it could have moved")
	spentWhat := "no attempt having been spent"
	if spent.n != 0 {
		spentWhat = "with -first and -last, an attempt having been spent"
		moment("saguin-dlq-first", spent.firstAfter, spent.firstBefore, "when its first spent attempt could have begun")
		moment("saguin-dlq-last", spent.lastAfter, spent.lastBefore, "when its last spent attempt could have begun")
	}
	for name, value := range want {
		if !slices.Equal(dlq[name], []string{value}) {
			t.Errorf("the dead letter %q carries %s %q; want once %q", got.Topic, name, dlq[name], value)
		}
	}
	if names := slices.Sorted(maps.Keys(dlq)); !slices.Equal(names, slices.Sorted(maps.Keys(want))) {
		t.Errorf("the dead letter %q carries the saguin-dlq- properties %q besides those it was checked for; want %q, %s", got.Topic, names, slices.Sorted(maps.Keys(want)), spentWhat)
	}
	return at
}

// design.md, R4: "A reply confirming a change waits for that change's
// commit"; Store: full mode, "Everything acknowledged survives a crash or a
// power cut"; Channels, queue: "Its records are stored like append
// records"; storage.md, queue_jobs: "This table holds a queue's unfinished
// jobs. The record stays in records".
//
// A publisher stores orders/a/1 on the queue orders at QoS 1, and the
// broker is killed once the PUBACK has come. Started again on the same
// database, it holds orders/a/1 as a record of orders with its unfinished
// job: none spent or failed, offered at once.
func TestQueuePublicationKeepsItsJobAcrossACrash(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{ordersQueue}}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := support.Sized("orders/a/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, p)
	support.Crash(t, b)

	b = support.Launch(t, o)
	expectJobs(t, support.OpenDB(t, b.DB), ordersQueue.Name, []string{p.Topic}, "after the crash that followed its PUBACK")
}

// design.md, Flows, PUBLISH, the table: "2 | The publication enters on
// PUBREL, from a held copy stored and charged to the channel before PUBREC.
// The held copy, offset and retained flag move in one transaction [...] The
// client's PUBREL, resent when it reconnects, completes the move"; "While a
// QoS 2 PUBLISH's identifier is held awaiting PUBREL, any later PUBLISH with
// that identifier gets PUBREC again. It is not stored twice, or charged
// twice against its channel's size, whatever its DUP flag (MQTT-4.3.3-10)"; "A PUBREL for an unknown
// identifier gets PUBCOMP 0x92"; Channels, queue: "Its records are stored
// like append records"; storage.md, queue_jobs. MQTT 5 4.3.3: the receiver
// "MUST NOT cause duplicate messages to be delivered to any onward
// recipients" [MQTT-4.3.3-10].
//
// A client holding a durable session sends a QoS 2 PUBLISH on orders/a/2:
// after its PUBREC the database holds its held copy alone, no record or job
// on orders. The client sends it again, DUP 1, under the same identifier,
// and gets PUBREC again; the broker is then killed. Started again, the
// client resumes and sends PUBREL: PUBCOMP 0x00. A second PUBREL gets
// PUBCOMP 0x92. The database holds one record of the message, at QoS 2,
// no held copy, and one new job for it.
func TestQueueQoS2PublicationMakesOneJob(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{ordersQueue}}
	b := support.Launch(t, o)
	connect := kept(harness.V5, clientID(t, ""))
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	p := twice("orders/a/2", "exactly one job")
	support.Sent(t, c, p)
	pubrecOf(t, c, p, 0x00)
	db := support.OpenDB(t, b.DB)
	if n, rs, js := heldCopies(t, db, p.Payload), held(t, db, ordersQueue.Name), unfinished(t, db, ordersQueue.Name); n != 1 || len(rs) != 0 || len(js) != 0 {
		t.Fatalf("after its PUBREC the database holds %d held copies, the records %q and %d jobs on orders; want the held copy alone, the publication entering on PUBREL", n, topicsOf(rs), len(js))
	}
	p.Dup = true
	support.Sent(t, c, p)
	pubrecOf(t, c, p, 0x00)
	support.Crash(t, b)

	b = support.Launch(t, o)
	c = resumed(t, b.Addr, harness.V5, connect)
	pubrel(t, c, p.ID, 0x00)
	pubrel(t, c, p.ID, 0x92)
	expectStoredOnce(t, b.DB, p.Payload, "after the PUBREL")
	expectJobs(t, support.OpenDB(t, b.DB), ordersQueue.Name, []string{p.Topic}, "after the PUBREL")
}

// design.md, DISCONNECT, A lost connection: "The Will goes after its Will
// Delay"; "A Will is published and removed in one transaction"; Channels,
// queue: "Its records are stored like append records"; storage.md,
// queue_jobs: "This table holds a queue's unfinished jobs. The record stays
// in records".
//
// A client connects with a Will on orders/will/1, no Will Delay, which the
// database then holds, and drops its connection. Every snapshot of the
// database, read again and again until the Will is gone, holds either the
// Will and nothing on orders, or no Will and its one record on orders with
// its new job: never both, never neither.
func TestQueueWillMakesOneJobAsItLeaves(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	db := support.OpenDB(t, b.DB)
	client := clientID(t, "")
	will := &harness.Will{QoS: 1, Topic: "orders/will/1", Payload: support.WillPayload}
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Will: will})
	if _, ok := support.StoredWill(t, db, client); !ok {
		t.Fatal("after the CONNACK the database holds no Will for the client")
	}
	support.DropSocket(t, c)
	published := func() bool {
		var stored bool
		var rs []heldRecord
		var js []job
		snapshot(t, db, func(tx *sql.Tx) {
			_, stored = support.StoredWill(t, tx, client)
			rs, js = held(t, tx, ordersQueue.Name), unfinished(t, tx, ordersQueue.Name)
		})
		switch {
		case stored && (len(rs) != 0 || len(js) != 0):
			t.Fatalf("a snapshot holds the Will and, on orders, the records %q and %d jobs; want nothing on orders while the Will is stored", topicsOf(rs), len(js))
		case !stored && !slices.Equal(topicsOf(rs), []string{will.Topic}):
			t.Fatalf("a snapshot holds no Will and, on orders, the records %q; want %s, published in the write that removed the Will", topicsOf(rs), will.Topic)
		case !stored:
			if diff := jobsMatch(js, rs); diff != "" {
				t.Fatalf("a snapshot holds no Will and, on orders, %s", diff)
			}
		}
		return !stored
	}
	if !testkit.Within(harness.DefaultTimeout, published) {
		t.Fatalf("%v after its connection was lost, the database still holds the Will of %q", harness.DefaultTimeout, client)
	}
}

// design.md, Channels, queue: "A subscription to the channel's own topics
// makes a reader, as on append"; Flows, SUBSCRIBE, the table: "append, or a
// queue's own topics (a reader) | the floor"; $saguin topics, the seek row:
// "moves this durable session's position on an append channel [...] or a
// reader's on a queue, with seek on that channel [...] Answer: the new
// offset"; Delivery, Positions: "A seek changes the position at once";
// Order: "deliveries follow a channel's offset order on $broadcast, append
// and a queue's own topics".
//
// A publisher stores orders/a/1, orders/b/2 and orders/a/3 on the queue
// orders. A client holding a durable session subscribes at QoS 1 to
// orders/#: it receives the three, from the floor, in offset order, and
// acknowledges each. It seeks the queue to the second record's offset: the
// answer is that offset, and it receives orders/b/2 and orders/a/3 again,
// in order.
func TestQueueReaderReadsHistoryAsOnAppend(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{
		support.Sized("orders/a/1", testkit.RecordBytes), support.Sized("orders/b/2", testkit.RecordBytes), support.Sized("orders/a/3", testkit.RecordBytes),
	})
	c := support.Subscribed(t, b.Addr, harness.V5, kept(harness.V5, clientID(t, "")), support.At(1, "orders/#"))
	rs := channelRecords(t, b.DB, ordersQueue.Name)
	if len(rs) != 3 {
		t.Fatalf("orders holds %d records; want the three published", len(rs))
	}
	replayed(t, c, rs, rs[0].offset)
	taken(t, c, "the PUBACKs of the first deliveries")
	answersOffset(t, sought(t, c, ordersQueue.Name, strconv.FormatInt(rs[1].offset, 10), 0x00), rs[1].offset, "the second record of orders")
	replayed(t, c, rs, rs[1].offset)
}

// design.md, Delivery, Message Expiry: "On a configured channel it is still
// delivered, with its interval 0 and saguin-expires saying when it ran out
// (a departure from MQTT-3.3.2-5). Only retention removes a record. Message
// Expiry never ends a queue job"; Service publications: "The job follows
// the queue's lifetime rules (Channels)".
//
// A publisher stores on the queue orders, whose retention_period is the
// day the harness gives every log, a job with a Message Expiry Interval of
// 1 second. Once its expiry, read from its row, has passed, a new reader
// subscribing at QoS 1 to orders/# receives it with a Message Expiry
// Interval of 0 and saguin-expires, and the database still holds its
// record with its unfinished job.
func TestMessageExpiryNeverEndsAJob(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/a/5", Props: expiry(1), Payload: []byte("an expiring job")}
	support.Acknowledged(t, pub, p)
	expires := expiresOf(t, b.DB, p.Payload)
	time.Sleep(time.Until(time.UnixMilli(expires + 1))) // the expiry passing is the behaviour under test

	reader := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-reader"), CleanStart: true}, support.At(1, "orders/#"))
	expectExpired(t, support.DeliveredAt(t, reader, 1, p.Topic, p.Payload), expires)
	expectJobs(t, support.OpenDB(t, b.DB), ordersQueue.Name, []string{p.Topic}, "past its Message Expiry")
}

// design.md, Channels, queue: "Retention never deletes an unfinished job. It
// moves the job to the append channel <channel>__dlq in one transaction. The
// topic gains the prefix __dlq/, whatever the queue's filter. Example:
// orders/a/42 becomes __dlq/orders/a/42"; "The moved job carries
// saguin-dlq-reason. Its value is expired when the job is past
// retention_period"; "The dead-letter channel's filter is the queue's
// filter with the same prefix"; Sagüin properties (expectDeadLetter);
// Words: Retention, "retention deletes late, never early"; storage.md,
// queue_jobs: "When retention ends a job, the job moves to the dead-letter
// channel in one transaction with its row".
//
// The queue orders keeps jobs for 2 seconds. A publisher stores orders/a/42
// carrying a User Property and a Content Type. Once it is past
// retention_period, orders holds no record and no job, and orders__dlq
// holds __dlq/orders/a/42. A reader subscribing at QoS 1 to
// __dlq/orders/# receives it, with the job's payload, saguin-id and
// properties, saguin-dlq-reason expired, saguin-dlq-at no earlier than the
// job's time plus retention_period, and saguin-offset its offset on
// orders__dlq (Sagüin properties: "saguin-id, saguin-offset,
// saguin-timestamp | every delivery").
func TestExpiredJobMovesToTheDeadLetterChannel(t *testing.T) {
	t.Parallel()
	const period = 2 * time.Second
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionPeriod: period, VisibilityTimeout: time.Second},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/a/42", Payload: []byte("order 42"), Props: harness.Properties{
		{ID: harness.ContentType, Str: "text/plain"}, {ID: harness.UserProperty, Str: "customer", Value: "c-7"},
	}}
	support.Acknowledged(t, pub, p)
	rs := channelRecords(t, b.DB, ordersQueue.Name)
	if len(rs) != 1 {
		t.Fatalf("orders holds %d records; want the job published", len(rs))
	}
	awaitHeld(t, db, deadLetters, []string{deadTopic(p.Topic)}, "once the job is past retention_period")
	moved := time.Now()
	if left, js := held(t, db, ordersQueue.Name), unfinished(t, db, ordersQueue.Name); len(left) != 0 || len(js) != 0 {
		t.Errorf("after the move, orders holds the records %q and %d jobs; want neither, the job moved", topicsOf(left), len(js))
	}

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload)
	expectDeadLetter(t, got, &rs[0], p.Props, "expired", time.UnixMilli(rs[0].time).Add(period), moved, nil)
	if offset := strconv.FormatInt(held(t, db, deadLetters)[0].offset, 10); !slices.Equal(got.Props.User("saguin-offset"), []string{offset}) {
		t.Errorf("the dead letter carries saguin-offset %q; want once %s, its offset on orders__dlq", got.Props.User("saguin-offset"), offset)
	}
}

// design.md, Channels, queue: "Its value is [...] evicted when
// retention_bytes causes the move"; "Held jobs do not count toward
// retention_bytes"; Words: Retention, "retention deletes a channel's oldest
// records by age and size"; "Every committed change that affects size
// schedules a size pass for its channel"; config.md, channels.<name>,
// retention_bytes, "queue: dead-letters as evicted"; storage.md, A
// message's bytes.
//
// The queue orders keeps 250 bytes. A publisher stores orders/1 and
// orders/2, 100 bytes each: both stay. orders/3 makes 300 bytes: orders
// then holds orders/2 and orders/3, with their jobs, and orders__dlq
// __dlq/orders/1. A reader subscribing at QoS 1 to __dlq/orders/# receives
// it, saguin-dlq-reason evicted, saguin-dlq-at no earlier than orders/3's
// time.
func TestRetentionBytesEvictsTheOldestJobs(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes), support.Sized("orders/3", testkit.RecordBytes)}
	support.PublishAll(t, pub, ps[:2])
	first := channelRecords(t, b.DB, ordersQueue.Name)
	if !slices.Equal(topicsOf(held(t, db, ordersQueue.Name)), []string{"orders/1", "orders/2"}) {
		t.Fatalf("with 200 bytes stored, orders holds %q; want both jobs, within its retention_bytes", topicsOf(held(t, db, ordersQueue.Name)))
	}
	support.Acknowledged(t, pub, ps[2])
	awaitHeld(t, db, deadLetters, deadTopics("orders/1"), "once orders/3 has made 300 bytes")
	moved := time.Now()
	expectJobs(t, db, ordersQueue.Name, []string{"orders/2", "orders/3"}, "once orders/1 has moved")
	third := channelRecords(t, b.DB, ordersQueue.Name)[1]

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(ps[0].Topic), ps[0].Payload)
	expectDeadLetter(t, got, &first[0], nil, "evicted", time.UnixMilli(third.time), moved, nil)
}

// design.md, Channels, queue: "When several dead-letter causes apply,
// expired wins, then attempts_exhausted, then evicted"; Words: Retention,
// "Every committed change that affects size schedules a size pass for its
// channel"; "Storage delay or refusal follows the store's rules"; Store:
// "Each write claims the database as it starts (_txlock=immediate)".
//
// The queue orders keeps jobs for 3 seconds and 250 bytes. A publisher
// stores orders/1, and orders/2 a second and a half later, 100 bytes each.
// Before orders/1 is past retention_period, the test takes the database's
// write lock, and the publisher sends orders/3, which cannot commit; the
// lock is given back once orders/1 is past retention_period, and orders/3
// is acknowledged. Its 300 bytes and orders/1's age then both call for
// orders/1 to move: orders__dlq holds __dlq/orders/1, saguin-dlq-reason
// expired, and orders holds orders/2 and orders/3.
func TestExpiredWinsOverEvicted(t *testing.T) {
	t.Parallel()
	const period = 3 * time.Second
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionPeriod: period, VisibilityTimeout: time.Second, RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes), support.Sized("orders/3", testkit.RecordBytes)}
	support.Acknowledged(t, pub, ps[0])
	first := channelRecords(t, b.DB, ordersQueue.Name)
	due := time.UnixMilli(first[0].time).Add(period)
	time.Sleep(time.Until(due.Add(-period / 2))) // orders/2 must not be due with orders/1
	support.Acknowledged(t, pub, ps[1])

	time.Sleep(time.Until(due.Add(-700 * time.Millisecond)))
	lock := holdWriteLock(t, b.DB)
	support.Sent(t, pub, ps[2])
	time.Sleep(time.Until(due.Add(500 * time.Millisecond))) // the lock held well within the store's busy timeout
	lock.release(t)
	support.Puback(t, pub, ps[2])
	awaitHeld(t, db, deadLetters, deadTopics("orders/1"), "once orders/1 is past retention_period and orders/3 has made 300 bytes")
	moved := time.Now()
	expectJobs(t, db, ordersQueue.Name, []string{"orders/2", "orders/3"}, "once orders/1 has moved")

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(ps[0].Topic), ps[0].Payload)
	expectDeadLetter(t, got, &first[0], nil, "expired", due, moved, nil)
}

// placedOnce fails the test unless one snapshot of db holds each topic of
// acked, the topics of publications acknowledged on the queue orders,
// exactly once and each topic of unacked at most once, either on orders or
// as its dead letter on orders__dlq, with nothing else on either, and
// orders' unfinished jobs exactly its records' (design.md, Channels, queue:
// "It moves the job to the append channel <channel>__dlq in one
// transaction"; storage.md, queue_jobs: "When retention ends a job, the job
// moves to the dead-letter channel in one transaction with its row"). when
// says when.
func placedOnce(t *testing.T, db *sql.DB, acked, unacked []string, when string) {
	t.Helper()
	var queued, dead []heldRecord
	var js []job
	snapshot(t, db, func(tx *sql.Tx) {
		queued, dead, js = held(t, tx, ordersQueue.Name), held(t, tx, deadLetters), unfinished(t, tx, ordersQueue.Name)
	})
	if diff := jobsMatch(js, queued); diff != "" {
		t.Fatalf("%s, orders holds %s", when, diff)
	}
	places := make(map[string]int)
	for _, r := range queued {
		places[r.topic]++
	}
	for _, r := range dead {
		topic, ok := strings.CutPrefix(r.topic, "__dlq/")
		if !ok {
			t.Fatalf("%s, orders__dlq holds %q; want only dead letters of orders' topics", when, r.topic)
		}
		places[topic]++
	}
	for _, topic := range acked {
		if places[topic] != 1 {
			t.Fatalf("%s, %s, acknowledged, is held %d times on orders and orders__dlq (orders %q, orders__dlq %q); want once", when, topic, places[topic], topicsOf(queued), topicsOf(dead))
		}
		delete(places, topic)
	}
	for _, topic := range unacked {
		if places[topic] > 1 {
			t.Fatalf("%s, %s is held %d times on orders and orders__dlq; want at most once", when, topic, places[topic])
		}
		delete(places, topic)
	}
	if len(places) != 0 {
		t.Fatalf("%s, orders and orders__dlq hold %v; want only the publications' topics", when, places)
	}
}

// design.md, Channels, queue: "Retention never deletes an unfinished job.
// It moves the job to the append channel <channel>__dlq in one
// transaction"; Words: Retention, "Every committed change that affects size
// schedules a size pass for its channel"; Store: full mode, "Everything
// acknowledged survives a crash or a power cut. A restart recovers committed
// state".
//
// The queue orders keeps 150 bytes: each 100-byte job's commit moves the
// one before it. A publisher stores 40 jobs one by one, and after each
// acknowledgement a snapshot of the database holds every acknowledged job
// once, on orders with its job or on orders__dlq, never both, never
// neither (placedOnce). The publisher then sends 40 more without waiting,
// and the broker is killed. Started again, the database holds every
// acknowledged job once and every other job at most once.
func TestDeadLetterMoveIsAllOrNothing(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionBytes: testkit.RecordBytes + testkit.RecordBytes/2},
	}}
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	const stored, inFlight = 40, 40
	acked, unacked := make([]string, 0, stored), make([]string, 0, inFlight)
	for i := range stored {
		p := support.Sized(fmt.Sprintf("orders/%d", i), testkit.RecordBytes)
		support.Acknowledged(t, pub, p)
		acked = append(acked, p.Topic)
		placedOnce(t, db, acked, nil, "after the PUBACK of "+p.Topic)
	}
	for i := stored; i < stored+inFlight; i++ {
		p := support.Sized(fmt.Sprintf("orders/%d", i), testkit.RecordBytes)
		support.Sent(t, pub, p)
		unacked = append(unacked, p.Topic)
	}
	support.Crash(t, b)

	b = support.Launch(t, o)
	placedOnce(t, support.OpenDB(t, b.DB), acked, unacked, "after the crash")
}

// design.md, Channels, queue: "The dead-letter channel's filter is the
// queue's filter with the same prefix. Its readers start at the floor";
// "Only the broker writes to it, and a publish gets 0x87"; Flows, PUBLISH,
// the refusals: "0x87 | [...] a topic under $SYS/ or on a dead-letter
// channel"; config.md, channels.<name>.start, "where a reader with no
// position begins".
//
// The queue orders starts its own readers at the tail and keeps 150 bytes.
// A publisher stores orders/1, orders/2 and orders/3, 100 bytes each:
// orders__dlq holds __dlq/orders/1 and __dlq/orders/2. A reader then
// subscribing at QoS 1 to __dlq/orders/# receives both, in that order.
// A QoS 1 PUBLISH on __dlq/orders/x gets PUBACK 0x87.
func TestDeadLetterChannelReadersStartAtTheFloor(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, Start: "tail", RetentionBytes: testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes), support.Sized("orders/3", testkit.RecordBytes)}
	for _, p := range ps {
		support.Acknowledged(t, pub, p)
	}
	awaitHeld(t, db, deadLetters, deadTopics("orders/1", "orders/2"), "once orders/3 is stored")

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	for _, p := range ps[:2] {
		acknowledge(t, dead, support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload).ID, 0)
	}
	refused := &harness.Publish{QoS: 1, Topic: deadTopic("orders/x"), Payload: []byte("a client's dead letter")}
	support.Sent(t, pub, refused)
	support.PubackOf(t, pub, refused, 0x87)
}

// design.md, Channels, queue: "A dead letter's age, for that channel's
// retention, counts from the move. Its Message Expiry stays as it was
// published"; config.md, channels.<name>, dlq_retention_period, "the
// dead-letter channel's retention, due as Words: Retention says"; Sagüin
// properties: "saguin-expires | where the publisher set an expiry";
// Delivery, Message Expiry: "worked out when the message is written and
// sent as the time remaining". MQTT 5 [MQTT-3.3.2-6]: the interval sent is
// "the received value minus the time that the Application Message has been
// waiting in the Server".
//
// The queue orders keeps jobs for 2 seconds, its dead-letter channel dead
// letters for 4. A publisher stores orders/a/11 with a Message Expiry
// Interval of an hour. Once it has moved, a reader subscribing at QoS 1 to
// __dlq/orders/# receives it with saguin-expires its expiry as published,
// which its row on orders__dlq keeps, and a Message Expiry Interval counted
// down from its publication. Four seconds and a second after its
// publication the dead letter is still held; it is deleted no earlier than
// four seconds after its saguin-dlq-at.
func TestDeadLetterAgeCountsFromTheMove(t *testing.T) {
	t.Parallel()
	const period, deadPeriod = 2 * time.Second, 4 * time.Second
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionPeriod: period, VisibilityTimeout: time.Second, DLQRetentionPeriod: deadPeriod},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/a/11", Props: expiry(3600), Payload: []byte("an aging dead letter")}
	support.Acknowledged(t, pub, p)
	rs := channelRecords(t, b.DB, ordersQueue.Name)
	if len(rs) != 1 || rs[0].expires == nil {
		t.Fatalf("orders holds %+v; want the job published, with its expiry", rs)
	}
	expires, published := *rs[0].expires, time.UnixMilli(rs[0].time)
	awaitHeld(t, db, deadLetters, deadTopics(p.Topic), "once the job is past retention_period")
	if got := expiresOf(t, b.DB, p.Payload); got != expires {
		t.Errorf("the dead letter's row holds expires %d; want %d, its Message Expiry as published", got, expires)
	}

	before := time.Now()
	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	got := support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload)
	if at := got.Props.User("saguin-expires"); !slices.Equal(at, []string{strconv.FormatInt(expires, 10)}) {
		t.Errorf("the dead letter came with saguin-expires %q; want once %d, its expiry as published", at, expires)
	}
	limit := (expires - before.UnixMilli() + 999) / 1000 // seconds left when the reader subscribed, rounded up
	if remaining, ok := messageExpiry(got); !ok || remaining == 0 || int64(remaining) > limit {
		t.Errorf("the dead letter came with Message Expiry Interval %d (present %v); want 1 through %d, counted down from its publication", remaining, ok, limit)
	}
	moved := expectDeadLetter(t, got, &rs[0], p.Props, "expired", published.Add(period), time.Now(), nil)

	time.Sleep(time.Until(published.Add(deadPeriod + time.Second))) // past dlq_retention_period from the publication
	if left := topicsOf(held(t, db, deadLetters)); !slices.Equal(left, deadTopics(p.Topic)) {
		t.Fatalf("a second past dlq_retention_period from its publication, orders__dlq holds %q; want the dead letter, its age counted from the move", left)
	}
	awaitHeld(t, db, deadLetters, nil, "once dlq_retention_period has passed since the move")
	if gone := time.Now(); gone.Before(moved.Add(deadPeriod)) {
		t.Errorf("the dead letter was gone by %v after its move; want it kept for dlq_retention_period, %v", gone.Sub(moved), deadPeriod)
	}
}

// design.md, Channels, queue: "A dead-letter topic may exceed the topic
// limits by its prefix"; Flows, PUBLISH, the refusals: "0x90 | a topic over
// limits.max_topic_length or limits.max_topic_levels".
//
// With limits.max_topic_length 24 and limits.max_topic_levels 4, a
// publisher stores on the queue orders, keeping jobs for 2 seconds, a job on
// a topic of 24 bytes and 4 levels: PUBACK 0x00. Once it has moved,
// orders__dlq holds it on its topic with the prefix, 30 bytes and 5
// levels, and a reader subscribing at QoS 1 to __dlq/orders/# receives it
// there.
func TestDeadLetterTopicMayExceedTheTopicLimits(t *testing.T) {
	t.Parallel()
	b := support.Launch(t, &harness.Options{MaxTopicLength: 24, MaxTopicLevels: 4, Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionPeriod: 2 * time.Second, VisibilityTimeout: time.Second},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/aa/bb/" + strings.Repeat("c", 11), Payload: []byte("at the limits")}
	if len(p.Topic) != 24 || strings.Count(p.Topic, "/") != 3 {
		t.Fatalf("the job's topic %q has %d bytes and %d levels; want 24 and 4", p.Topic, len(p.Topic), strings.Count(p.Topic, "/")+1)
	}
	support.Acknowledged(t, pub, p)
	awaitHeld(t, db, deadLetters, deadTopics(p.Topic), "once the job is past retention_period")

	dead := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-dead"), CleanStart: true}, support.At(1, deadFilter))
	support.DeliveredAt(t, dead, 1, deadTopic(p.Topic), p.Payload)
}

// deadLetteredSeries is saguin_queue_dead_lettered_total of channel and
// reason (metrics.md, Queues: "jobs moved to <channel>__dlq: expired,
// evicted or attempts_exhausted").
func deadLetteredSeries(channel, reason string) string {
	return support.ChannelSeries("saguin_queue_dead_lettered_total", channel, "reason", reason)
}

// metrics.md, Queues: saguin_queue_dead_lettered_total{channel,reason},
// "jobs moved to <channel>__dlq"; Delivery: saguin_missed_total, "records
// consumers were owed and never received [...] retention (the channel's
// total from each deletion's answer, away sessions included)". design.md,
// Words: Missed, "missed records are those a consumer was owed but never
// received because retention removed them"; "Passing a record counts when
// retention makes it unavailable to that consumer"; "Away sessions use
// their stored state"; "The core applies these counts once"; Channels,
// queue: "Retention never deletes an unfinished job. It moves the job".
//
// The queue orders keeps 150 bytes. A reader holding a durable session
// subscribes at QoS 1 to orders/# and goes away. A publisher stores
// orders/1, orders/2 and orders/3, 100 bytes each: orders__dlq holds the
// dead letters of the first two. Two jobs have moved, evicted, and the
// away reader has missed two records on orders.
func TestDeadLetterMovesAreCountedOnce(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionBytes: testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	reader := clientID(t, "-reader")
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: reader, CleanStart: true, Props: support.Durable}, support.At(1, "orders/#")))
	support.AwaitAway(t, db, reader)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes), support.Sized("orders/3", testkit.RecordBytes)})
	awaitHeld(t, db, deadLetters, deadTopics("orders/1", "orders/2"), "once orders/3 is stored")
	committed(t, pub, "barrier")
	expectSample(t, b, deadLetteredSeries(ordersQueue.Name, "evicted"), 2, "once two jobs have moved")
	expectSample(t, b, testkit.MissedSeries(ordersQueue.Name), 2, "once two jobs the away reader was owed have moved")
}

// design.md, Words: Retention, "Startup processes overdue retention before
// serving"; Restart, Start: "Listeners and bridges start last"; Channels,
// queue: "Retention never deletes an unfinished job. It moves the job to
// the append channel <channel>__dlq".
//
// The queue orders keeps jobs for 3 seconds. A publisher stores orders/1
// and orders/2, and the broker stops at once: its database still holds
// both jobs on orders. Once their retention_period has passed since their
// PUBACKs, the broker starts again: as soon as it listens, orders holds no
// record and no job, and orders__dlq holds both dead letters.
func TestOverdueMovesFinishBeforeServing(t *testing.T) {
	t.Parallel()
	const period = 3 * time.Second
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionPeriod: period, VisibilityTimeout: time.Second},
	}}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes)})
	due := time.Now().Add(period) // their intake preceded their PUBACKs
	stopped(t, b)
	expectJobs(t, support.OpenDB(t, b.DB), ordersQueue.Name, []string{"orders/1", "orders/2"}, "after the stop, within their retention_period")
	time.Sleep(time.Until(due))

	b = support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	if left, js := held(t, db, ordersQueue.Name), unfinished(t, db, ordersQueue.Name); len(left) != 0 || len(js) != 0 {
		t.Errorf("once the broker listens, orders holds the records %q and %d jobs; want neither, the overdue jobs moved before serving", topicsOf(left), len(js))
	}
	if dead := topicsOf(held(t, db, deadLetters)); !slices.Equal(dead, deadTopics("orders/1", "orders/2")) {
		t.Errorf("once the broker listens, orders__dlq holds %q; want both dead letters, moved before serving", dead)
	}
}

// design.md, Store, A full disk: "a write is answered "full" only when
// SQLite reports lack of space and rollback confirms that the transaction
// did not commit. The broker continues after that answer"; "A housekeeping
// write refused for room sets a retry deadline one second after that
// answer [...] At the deadline, the core rediscovers [...] retention work
// from the store"; Words: Retention, "the store's refusal and retry rules
// apply"; Channels, queue: "It moves the job to the append channel
// <channel>__dlq in one transaction". metrics.md: saguin_store_full;
// saguin_queue_dead_lettered_total.
//
// On a filesystem of its own, the queue orders keeps jobs for 2 seconds. A
// publisher stores orders/1 and orders/2, and a ballast then takes every
// byte left. Once their retention_period has passed, a write is refused
// for room: the database still holds both jobs on orders and nothing on
// orders__dlq. With the ballast emptied and no client acting, the retry
// moves them: orders holds no record and no job, orders__dlq both dead
// letters; two jobs are counted moved, expired, and the disk is no longer
// full.
func TestFullDiskDefersDeadLetterMoves(t *testing.T) {
	t.Parallel()
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	const period = 2 * time.Second
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), Operations: true, Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionPeriod: period, VisibilityTimeout: time.Second},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes)})
	due := time.Now().Add(period)
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	time.Sleep(time.Until(due))
	testkit.AwaitSample(t, b.Metrics(), "saguin_store_full", 1, harness.DefaultTimeout)
	expectJobs(t, db, ordersQueue.Name, []string{"orders/1", "orders/2"}, "with the move refused for room")
	if dead := topicsOf(held(t, db, deadLetters)); len(dead) != 0 {
		t.Fatalf("with the move refused for room, orders__dlq holds %q; want nothing, the move rolled back", dead)
	}
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	awaitHeld(t, db, deadLetters, deadTopics("orders/1", "orders/2"), "once the ballast is emptied, with no client acting")
	if left, js := held(t, db, ordersQueue.Name), unfinished(t, db, ordersQueue.Name); len(left) != 0 || len(js) != 0 {
		t.Errorf("after the retried move, orders holds the records %q and %d jobs; want neither", topicsOf(left), len(js))
	}
	committed(t, pub, "barrier")
	expectSample(t, b, deadLetteredSeries(ordersQueue.Name, "expired"), 2, "once the retried move has committed")
	expectSample(t, b, "saguin_store_full", 0, "once the retried move has committed")
}
