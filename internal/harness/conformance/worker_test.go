package conformance

import (
	"bytes"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of queue workers, from the wire (design.md, Channels, queue;
// Words: Attempt; Flows, SUBSCRIBE, the worker row; Delivery, Queue jobs and
// Service publications; $saguin topics, the queue rows; metrics.md,
// Queues). Each test runs a broker of its own holding ordersQueue
// (queue_test.go), or a queue like it. No attempt here fails: a return, a
// hold's timeout and a worker's going, and the retries they lead to, are
// item 0017d's.

// ordersWorker is the topic a worker of ordersQueue subscribes to, and
// ordersResponse the topic its answers go to ($saguin topics:
// "$saguin/queue/<channel> | subscribe, at QoS 1: become a worker";
// "$saguin/queue/<channel>/response | ack or return a job, with its
// attempt's Correlation Data").
var (
	ordersWorker   = "$saguin/queue/" + ordersQueue.Name
	ordersResponse = ordersWorker + "/response"
)

// attemptOf returns the Correlation Data of offer, a worker offer: its
// attempt's id (design.md, Words: Attempt, "It carries a broker-made id as
// its Correlation Data"), failing the test where it carries none.
func attemptOf(t *testing.T, offer *harness.Publish) []byte {
	t.Helper()
	cd, ok := offer.Props.Get(harness.CorrelationData)
	if !ok || len(cd.Data) == 0 {
		t.Fatalf("the offer of %q carries no Correlation Data; want its attempt's id (Words: Attempt)", offer.Payload)
	}
	return cd.Data
}

// response is a QoS 1 response to ordersQueue carrying payload and, as its
// Correlation Data, attempt ($saguin topics: "ack or return a job, with its
// attempt's Correlation Data").
func response(attempt []byte, payload string) *harness.Publish {
	return &harness.Publish{QoS: 1, Topic: ordersResponse, Payload: []byte(payload),
		Props: harness.Properties{{ID: harness.CorrelationData, Data: attempt}}}
}

// respond sends from c the response of payload naming attempt, and fails
// the test unless its PUBACK carries reason ($saguin topics, the response
// row: "Needs consume (else 0x87). No answer: the PUBACK (0x00) confirms
// receipt only").
func respond(t *testing.T, c *harness.Client, attempt []byte, payload string, reason byte) {
	t.Helper()
	p := response(attempt, payload)
	support.Sent(t, c, p)
	support.PubackOf(t, c, p, reason)
}

// completed has c, the worker offer was sent to, acknowledge it and answer
// ack with its attempt's Correlation Data.
func completed(t *testing.T, c *harness.Client, offer *harness.Publish) {
	t.Helper()
	acknowledge(t, c, offer.ID, 0)
	respond(t, c, attemptOf(t, offer), "ack", 0x00)
}

// unfinishedTopics reads, in one snapshot of db, the topics of ordersQueue's
// unfinished jobs, in offset order, failing the test where a job has no
// record (storage.md, queue_jobs: "This table holds a queue's unfinished
// jobs. The record stays in records. A finished job's row is deleted").
func unfinishedTopics(t *testing.T, db *sql.DB) []string {
	t.Helper()
	js := unfinished(t, db, ordersQueue.Name)
	topics := make([]string, len(js))
	for i, j := range js {
		topics[i] = j.topic
	}
	return topics
}

// awaitUnfinished waits until ordersQueue's unfinished jobs are those of the
// records of topics, in offset order. harness.DefaultTimeout is the test's
// watchdog: a response's PUBACK "confirms receipt only" ($saguin topics),
// so the job's end is waited for in the database. when says when.
func awaitUnfinished(t *testing.T, db *sql.DB, topics []string, when string) {
	t.Helper()
	awaitState(t, when+", unfinished topics "+fmt.Sprint(topics),
		func() []string { return unfinishedTopics(t, db) },
		func(got []string) bool { return slices.Equal(got, topics) })
}

// design.md, Flows, SUBSCRIBE, the table: "$saguin/queue/<channel> (a
// worker) | the next job | refused (0x83) / 1 / 1; MQTT 3.1.1: refused
// (0x80)"; Options: "No Local is honoured everywhere [...] It is refused
// with 0x8F on a worker subscription"; Refusals: "It uses 0x8F for a filter
// of the wrong shape, such as a narrower $saguin/queue filter"; $saguin
// topics: "An MQTT 3.1.1 client cannot be a worker and is refused with 0x80
// if it tries"; Channels, queue: "A subscription to $saguin/queue/<channel>
// at QoS 1 makes a worker"; Delivery, Service publications: "a worker offer
// is a new broker-originated QoS 1 publication". MQTT 5 3.9.3, Table 3-8:
// 0x01 "Granted QoS 1"; MQTT 3.1.1 3.9.3: 0x80 "Failure".
//
// An MQTT 5 client subscribes to $saguin/queue/orders at QoS 0: SUBACK
// 0x83; at QoS 1: 0x01; at QoS 2: 0x01; at QoS 1 with No Local: 0x8F; and
// to $saguin/queue/orders/a at QoS 1: 0x8F. A publisher stores orders/1:
// the client receives it at QoS 1, a worker. An MQTT 3.1.1 client
// subscribing to $saguin/queue/orders at QoS 1 gets 0x80.
func TestWorkerSubscriptionIsGrantedAtQoS1(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	w, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true})
	support.Suback(t, w, nil, support.At(0, ordersWorker), 0x83)
	support.Suback(t, w, nil, support.At(1, ordersWorker), 0x01)
	support.Suback(t, w, nil, support.At(2, ordersWorker), 0x01)
	support.Suback(t, w, nil, []harness.Subscription{{Filter: ordersWorker, QoS: 1, NoLocal: true}}, 0x8F)
	support.Suback(t, w, nil, support.At(1, ordersWorker+"/a"), 0x8F)

	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, p)
	support.DeliveredAt(t, w, 1, p.Topic, p.Payload)

	legacy, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: clientID(t, "-legacy"), CleanStart: true})
	support.Suback(t, legacy, nil, support.At(1, ordersWorker), 0x80)
}

// workerACL lets work consume the queue orders, read read its topics and
// pub write them.
const workerACL = `roles:
  worker:
    - channel: orders
      allow: [consume]
  reader:
    - channel: orders
      allow: [read]
  publisher:
    - channel: orders
      allow: [write]
users:
  work: [worker]
  read: [reader]
  pub: [publisher]
`

// design.md, Channels, queue: "A subscription to $saguin/queue/<channel> at
// QoS 1 makes a worker. Each job goes to one worker at a time. A
// subscription to the channel's own topics makes a reader, as on append.
// The two need separate permissions, consume and read"; Access and reload:
// "A queue rule may list write, read and seek for its topics, and consume
// for a worker"; "The broker refuses a filter with 0x87 only when the user
// may read nothing the filter can match"; SUBSCRIBE, Refusals: "the broker
// refuses a filter with 0x87 where the acl_file refuses the client";
// $saguin topics, the response row: "Needs consume (else 0x87)".
//
// The worker, allowed consume alone, subscribes to $saguin/queue/orders at
// QoS 1: 0x01, and to orders/#: 0x87. The reader, allowed read alone,
// subscribes to orders/#: 0x01, and to $saguin/queue/orders: 0x87. A
// publisher stores orders/1: the worker receives its offer, the reader the
// record. The reader answers ack with the offer's Correlation Data: PUBACK
// 0x87, and the job is still unfinished. The worker acknowledges the offer
// and answers ack: PUBACK 0x00, and the job is finished.
func TestConsumeAndReadAreSeparatePermissions(t *testing.T) {
	o := aclOptions(t, workerACL, "work", "read", "pub")
	o.Channels = []harness.Channel{ordersQueue}
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	w, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-worker"), "work"))
	support.Suback(t, w, nil, support.At(1, ordersWorker), 0x01)
	support.Suback(t, w, nil, support.At(1, "orders/#"), 0x87)
	r, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-reader"), "read"))
	support.Suback(t, r, nil, support.At(1, "orders/#"), 0x01)
	support.Suback(t, r, nil, support.At(1, ordersWorker), 0x87)

	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	p := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, p)
	offer := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
	support.DeliveredAt(t, r, 1, p.Topic, p.Payload)

	respond(t, r, attemptOf(t, offer), "ack", 0x87)
	if got := unfinishedTopics(t, db); !slices.Equal(got, []string{p.Topic}) {
		t.Fatalf("after the reader's refused ack, orders' unfinished jobs are those of %q; want %s's, unchanged", got, p.Topic)
	}
	completed(t, w, offer)
	awaitUnfinished(t, db, nil, "once the worker, allowed consume alone, has answered ack")
}

// design.md, Delivery, Queue jobs: "each worker receives one job at a time
// per queue. Workers are selected round-robin among those connected, with
// room and allowed by the acl_file, as a shared group's members are";
// SUBSCRIBE, A shared group: "Selection is round-robin by complete client
// id, starting after the last selected id and wrapping once. Ineligible
// members are skipped. Only a selection moves the last-selected id. A new
// group or queue starts before the first id"; jobs "are offered oldest
// first" (the maintainer, 0017c's comment of 261102 084409).
//
// Three workers join out of client-id order: -c, then -a, then -b. A
// publisher stores five jobs, 1 to 5: -a receives 1, -b 2 and -c 3, and
// each acknowledges its offer. -b answers ack: 4 goes to -b, the next after
// -c holding nothing, -a passed over. -a answers ack: 5 goes to -a, -c
// passed over. Each worker's next packet is its own job, never a second
// one while it holds one.
func TestWorkersTakeOneJobAtATimeRoundRobin(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	ids := []string{clientID(t, "-a"), clientID(t, "-b"), clientID(t, "-c")}
	workers := make(map[string]*harness.Client, len(ids))
	for _, id := range []string{ids[2], ids[0], ids[1]} {
		workers[id] = support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true}, support.At(1, ordersWorker))
	}
	a, bb, c := workers[ids[0]], workers[ids[1]], workers[ids[2]]
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := numbered("orders/x", 5)
	support.PublishAll(t, pub, ps)

	offers := make([]*harness.Publish, 3)
	for i, w := range []*harness.Client{a, bb, c} {
		offers[i] = support.DeliveredAt(t, w, 1, "orders/x", ps[i].Payload)
		acknowledge(t, w, offers[i].ID, 0)
	}
	respond(t, bb, attemptOf(t, offers[1]), "ack", 0x00)
	support.DeliveredAt(t, bb, 1, "orders/x", ps[3].Payload)
	respond(t, a, attemptOf(t, offers[0]), "ack", 0x00)
	support.DeliveredAt(t, a, 1, "orders/x", ps[4].Payload)
}

// design.md, Delivery, Service publications: "a worker offer is a new
// broker-originated QoS 1 publication containing the job payload and
// attempt metadata, including saguin-expires where present. It does not
// inherit the job publication's Message Expiry"; Words: Attempt, "It
// carries a broker-made id as its Correlation Data";
// Sagüin properties: "Nothing a publisher sends is altered, except that a
// queue job's Response Topic and Correlation Data are the queue's own",
// the queue's own Response Topic being its response topic ($saguin topics:
// "$saguin/queue/<channel>/response | ack or return a job"); "saguin-attempt
// | queue jobs sent to workers"; "saguin-attempt (from 1) [...] decimal";
// the other saguin- properties as expectSaguin reads them, a worker's
// filter reaching its queue alone; SUBSCRIBE, Options: "RETAIN is 0 on
// channel deliveries".
//
// A publisher stores on orders a job with a Message Expiry Interval of an
// hour, a Content Type, a User Property, and a Response Topic and
// Correlation Data of its own; then a job with none of them. A worker
// receives the first: RETAIN 0, no Message Expiry Interval, the publisher's
// Content Type and User Property, Response Topic $saguin/queue/orders/response,
// Correlation Data an id of the broker's, saguin-attempt 1 and saguin-expires the job's
// expiry. It completes it and receives the second: no saguin-expires, and
// Correlation Data of its own.
func TestWorkerOfferCarriesItsAttempt(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "orders/a/4", Payload: []byte("order 4"), Props: harness.Properties{
			{ID: harness.MessageExpiryInterval, Int: 3600},
			{ID: harness.ContentType, Str: "text/plain"},
			{ID: harness.ResponseTopic, Str: "replies/publisher"},
			{ID: harness.CorrelationData, Data: []byte("the publisher's own")},
			harness.User("customer", "c-4"),
		}},
		{QoS: 1, Topic: "orders/a/5", Payload: []byte("order 5")},
	}
	support.PublishAll(t, pub, ps)
	rs := channelRecords(t, b.DB, ordersQueue.Name)
	if len(rs) != 2 || rs[0].expires == nil {
		t.Fatalf("orders holds %+v; want the two jobs published, the first with its expiry", rs)
	}

	attempts := make([][]byte, 0, len(ps))
	for i, p := range ps {
		offer := support.DeliveredAt(t, w, 1, p.Topic, p.Payload)
		if offer.Retain {
			t.Errorf("the offer of %q has RETAIN 1; want 0", p.Payload)
		}
		if remaining, ok := messageExpiry(offer); ok {
			t.Errorf("the offer of %q carries a Message Expiry Interval of %d; want none of its own", p.Payload, remaining)
		}
		attempt := attemptOf(t, offer)
		if len(attempt) == 0 || bytes.Equal(attempt, []byte("the publisher's own")) {
			t.Errorf("the offer of %q carries Correlation Data %q; want its attempt's broker-made id", p.Payload, attempt)
		}
		if slices.ContainsFunc(attempts, func(a []byte) bool { return bytes.Equal(a, attempt) }) {
			t.Errorf("the offer of %q carries Correlation Data %q, an earlier attempt's; want an id of its own", p.Payload, attempt)
		}
		attempts = append(attempts, attempt)
		queue := harness.Properties{{ID: harness.ResponseTopic, Str: ordersResponse}, {ID: harness.CorrelationData, Data: attempt}}
		for _, prop := range p.Props {
			if prop.ID != harness.ResponseTopic && prop.ID != harness.CorrelationData {
				queue = append(queue, prop)
			}
		}
		expectSaguin(t, offer, &rs[i], "", queue, harness.User("saguin-attempt", "1"))
		completed(t, w, offer)
	}
}

// design.md, Delivery, Service publications: "If the receiving worker set a
// Subscription Identifier on its own $saguin/queue/<channel> subscription,
// the offer carries that identifier"; "A worker offer
// [...] has an automatically assigned exact-topic subscription for that
// delivery [...] It does not change the client's explicit subscriptions".
// MQTT 5 3.3.2.3.8: the Subscription Identifiers of a PUBLISH are those of
// "the subscription(s) which caused the PUBLISH".
//
// Worker -a subscribes to $saguin/queue/orders with Subscription Identifier
// 7 and, a reader too, to orders/# with 9; worker -b subscribes to
// $saguin/queue/orders with none. A publisher stores two jobs on orders/x.
// -a receives the first job's offer, carrying 7 alone, and both records as
// a reader, each carrying 9 alone, in any order between the offer and the
// records; -b receives the second job's offer, carrying none.
func TestWorkerOfferCarriesItsOwnSubscriptionIdentifier(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	a := identifiedSubscriber(t, b.Addr, &harness.Connect{ClientID: clientID(t, "-a"), CleanStart: true}, 1, []string{ordersWorker, "orders/#"}, []uint32{7, 9})
	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := numbered("orders/x", 2)
	support.PublishAll(t, pub, ps)

	var offers, reads []*harness.Publish
	for range 3 {
		p := harness.Expect[*harness.Publish](t, a)
		if p.QoS != 1 || p.Topic != "orders/x" {
			t.Fatalf("-a received PUBLISH %q at QoS %d; want orders/x at QoS 1", p.Topic, p.QoS)
		}
		if len(p.Props.User("saguin-attempt")) != 0 {
			offers = append(offers, p)
		} else {
			reads = append(reads, p)
		}
	}
	if len(offers) != 1 || !bytes.Equal(offers[0].Payload, ps[0].Payload) {
		t.Fatalf("-a received %d offers; want one, the first job's", len(offers))
	}
	expectIDs(t, offers[0], 7)
	for _, p := range reads {
		expectIDs(t, p, 9)
	}
	expectIDs(t, support.DeliveredAt(t, bb, 1, "orders/x", ps[1].Payload))
}

// design.md, Channels, queue: "Its records are stored like append records,
// and acknowledged jobs stay for readers"; Delivery, Queue jobs: "An attempt
// is spent at the worker's PUBACK or its response naming the attempt,
// whichever is first"; "each worker receives one job at a time per queue";
// $saguin topics, the response row: "ack or return a job, with its
// attempt's Correlation Data"; storage.md, queue_jobs: spent "attempts
// spent", first_attempt and last_attempt "when its first [last] spent
// attempt began", "A finished job's row is deleted".
//
// A worker subscribes, and a publisher stores orders/1 and orders/2. The
// worker acknowledges orders/1's offer: its job has one attempt spent, none
// failed, its first and last attempt the same moment. The worker answers
// ack: orders/1's job is finished, and orders/2's offer comes. The worker
// answers ack before acknowledging it: orders/2's job is finished, then
// the worker acknowledges the offer. A reader subscribing at QoS 1 to
// orders/# receives both records, in offset order.
func TestAckCompletesAJobBeforeOrAfterThePuback(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	db := support.OpenDB(t, b.DB)
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes)}
	support.PublishAll(t, pub, ps)

	first := support.DeliveredAt(t, w, 1, ps[0].Topic, ps[0].Payload)
	acknowledge(t, w, first.ID, 0)
	js := awaitState(t, "after the PUBACK, two jobs with orders/1's attempt spent",
		func() []job { return unfinished(t, db, ordersQueue.Name) },
		func(js []job) bool { return len(js) == 2 && js[0].spent == 1 })
	if j := js[0]; j.failed != 0 || !j.firstAttempt.Valid || j.lastAttempt != j.firstAttempt {
		t.Errorf("after the PUBACK of orders/1's offer, its job is %+v; want none failed, its first and last attempt the one spent", j)
	}
	respond(t, w, attemptOf(t, first), "ack", 0x00)
	awaitUnfinished(t, db, []string{ps[1].Topic}, "once the worker has answered ack after its PUBACK")

	second := support.DeliveredAt(t, w, 1, ps[1].Topic, ps[1].Payload)
	respond(t, w, attemptOf(t, second), "ack", 0x00)
	awaitUnfinished(t, db, nil, "once the worker has answered ack before its PUBACK")
	acknowledge(t, w, second.ID, 0)

	reader := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-reader"), CleanStart: true}, support.At(1, "orders/#"))
	rs := channelRecords(t, b.DB, ordersQueue.Name)
	if len(rs) != 2 {
		t.Fatalf("orders holds %d records; want both acknowledged jobs', kept for readers", len(rs))
	}
	replayed(t, reader, rs, rs[0].offset)
}

// design.md, $saguin topics, the response row: "A response naming an
// attempt that is not current or not this session's, or with another
// payload, changes nothing and is logged"; Words: Attempt, "an attempt is
// one try at giving a queue job to a worker"; Flows, Logging: a line is
// written at a level the default log_level, info, writes (loggedNaming).
// metrics.md, Queues: saguin_queue_stale_responses_total, "responses that
// changed nothing: naming an attempt not current or not the session's, or
// with another payload"; saguin_queue_acknowledged_total, "ack responses".
//
// Workers -a and -b each acknowledge the offer of a job: orders/1 to -a,
// orders/2 to -b. -b answers ack with -a's attempt; -a answers done with
// it; -a answers ack naming an attempt that never was. Each gets PUBACK
// 0x00, three responses are counted stale, none acknowledged, and both jobs
// are still unfinished. -a answers ack: orders/1's job is finished, one ack
// counted. -a answers ack again, its attempt no longer current: four stale,
// orders/2's job still unfinished. Once the broker has stopped, its log
// names each of -a and -b.
func TestStaleForeignAndInvalidResponsesChangeNothing(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	db := support.OpenDB(t, b.DB)
	aID, bID := clientID(t, "-a"), clientID(t, "-b")
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: aID, CleanStart: true}, support.At(1, ordersWorker))
	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: bID, CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes)}
	support.PublishAll(t, pub, ps)
	first := support.DeliveredAt(t, a, 1, ps[0].Topic, ps[0].Payload)
	acknowledge(t, a, first.ID, 0)
	acknowledge(t, bb, support.DeliveredAt(t, bb, 1, ps[1].Topic, ps[1].Payload).ID, 0)

	stale, acked := support.ChannelSeries("saguin_queue_stale_responses_total", ordersQueue.Name), support.ChannelSeries("saguin_queue_acknowledged_total", ordersQueue.Name)
	attempt := attemptOf(t, first)
	respond(t, bb, attempt, "ack", 0x00)
	respond(t, a, attempt, "done", 0x00)
	respond(t, a, []byte("an attempt that never was"), "ack", 0x00)
	awaitSeries(t, b, map[string]float64{stale: 3, acked: 0})
	if got := unfinishedTopics(t, db); !slices.Equal(got, []string{ps[0].Topic, ps[1].Topic}) {
		t.Fatalf("after three stale responses, orders' unfinished jobs are those of %q; want both, unchanged", got)
	}

	respond(t, a, attempt, "ack", 0x00)
	awaitUnfinished(t, db, []string{ps[1].Topic}, "once -a has answered ack for its current attempt")
	respond(t, a, attempt, "ack", 0x00)
	awaitSeries(t, b, map[string]float64{stale: 4, acked: 1})
	if got := unfinishedTopics(t, db); !slices.Equal(got, []string{ps[1].Topic}) {
		t.Fatalf("after a repeated ack, orders' unfinished jobs are those of %q; want orders/2's alone, unchanged", got)
	}

	stopped(t, b)
	loggedNaming(t, b, bID, "whose response named an attempt not its session's")
	loggedNaming(t, b, aID, "whose responses named an attempt not current, or carried another payload")
}

// design.md, $saguin topics: "Every request taken at QoS 2 runs on arrival,
// as at QoS 1. If the request changes something, its PUBREC waits for the
// store to confirm that change (Replies). The ordinary PUBREL and PUBCOMP
// follow. Its identifier is held [...] like any other incoming QoS 2
// exchange's identifier"; the response row, "Taken at QoS 0 too"; Flows,
// PUBLISH: "While a QoS 2 PUBLISH's identifier is held awaiting PUBREL, any
// later PUBLISH with that identifier gets PUBREC again. It is not stored
// twice, or charged twice against its channel's size, whatever its DUP flag
// (MQTT-4.3.3-10)". metrics.md,
// Queues: saguin_queue_acknowledged_total, "ack responses";
// saguin_queue_stale_responses_total. MQTT 5 4.3.3: the receiver "MUST NOT
// cause duplicate messages to be delivered to any onward recipients"
// [MQTT-4.3.3-10].
//
// A worker acknowledges the offer of orders/1, then answers ack at QoS 2:
// PUBREC 0x00, and at once the database holds no unfinished job. It sends
// the same PUBLISH again, DUP 1, under the same identifier: PUBREC 0x00.
// PUBREL gets PUBCOMP 0x00. One ack is counted, and no stale response.
func TestQoS2ResponseTakesEffectBeforeItsPubrec(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	db := support.OpenDB(t, b.DB)
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	job := support.Sized("orders/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, job)
	offer := support.DeliveredAt(t, w, 1, job.Topic, job.Payload)
	acknowledge(t, w, offer.ID, 0)

	p := response(attemptOf(t, offer), "ack")
	p.QoS = 2
	support.Sent(t, w, p)
	pubrecOf(t, w, p, 0x00)
	if got := unfinishedTopics(t, db); len(got) != 0 {
		t.Fatalf("after the response's PUBREC, orders' unfinished jobs are those of %q; want none, its result stored before PUBREC", got)
	}
	p.Dup = true
	support.Sent(t, w, p)
	pubrecOf(t, w, p, 0x00)
	pubrel(t, w, p.ID, 0x00)
	awaitSeries(t, b, map[string]float64{
		support.ChannelSeries("saguin_queue_acknowledged_total", ordersQueue.Name):    1,
		support.ChannelSeries("saguin_queue_stale_responses_total", ordersQueue.Name): 0,
	})
}

// design.md, Channels, queue: "Held jobs do not count toward
// retention_bytes, so a queue never refuses a publish for being full";
// "Retention never deletes an unfinished job. It moves the job to the
// append channel <channel>__dlq"; "acknowledged jobs stay for readers";
// "A held job moves when its hold ends"; Delivery, Queue jobs: "The hold
// starts at the worker's PUBACK"; jobs "are offered oldest first" (the
// maintainer, 0017c's comment of 261102 084409); Words: Retention, "Every
// committed change that affects size schedules a size pass for its
// channel"; "A channel's records are deleted oldest first". metrics.md:
// saguin_channel_retention_removed_total, "records retention deleted";
// saguin_queue_dead_lettered_total. storage.md, A message's bytes.
//
// The queue orders keeps 250 bytes. Worker -a acknowledges the offer of
// orders/1, 150 bytes, which it then holds. A publisher stores orders/2 and
// orders/3, 100 bytes each: 200 bytes count, within 250, orders/1 held. A
// worker -b subscribes: its first offer is orders/2, which no move took.
// -a answers ack: orders/1 is a finished job's record, and its 150 bytes
// now count; retention deletes it, a record and no longer a job, and
// orders holds orders/2 and orders/3. One record is counted deleted, and no
// job moved.
func TestHeldJobsDoNotCountTowardRetentionBytes(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{
		{Name: ordersQueue.Name, Type: "queue", Filter: ordersQueue.Filter, RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-a"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	big := support.Sized("orders/1", testkit.RecordBytes+testkit.RecordBytes/2)
	support.Acknowledged(t, pub, big)
	holding := support.DeliveredAt(t, a, 1, big.Topic, big.Payload)
	acknowledge(t, a, holding.ID, 0)
	taken(t, a, "the PUBACK that starts orders/1's hold")

	ps := []*harness.Publish{support.Sized("orders/2", testkit.RecordBytes), support.Sized("orders/3", testkit.RecordBytes)}
	support.PublishAll(t, pub, ps)
	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, ordersWorker))
	support.DeliveredAt(t, bb, 1, ps[0].Topic, ps[0].Payload)

	respond(t, a, attemptOf(t, holding), "ack", 0x00)
	awaitHeld(t, db, ordersQueue.Name, []string{ps[0].Topic, ps[1].Topic}, "once orders/1's job is finished, its record counting")
	committed(t, pub, "barrier")
	expectSample(t, b, removedSeries(ordersQueue.Name), 1, "once orders/1's record is deleted")
	expectSample(t, b, deadLetteredSeries(ordersQueue.Name, "evicted"), 0, "once orders/1's record is deleted")
}

// metrics.md, Queues: saguin_queue_jobs{channel,state}, "unfinished jobs:
// ready (can be offered), backoff [...], offered (sent, its PUBACK not yet
// in) or held (with a worker)"; saguin_queue_workers, "connected workers";
// saguin_queue_delivered_total, "attempts handed to workers";
// saguin_queue_acknowledged_total, "ack responses"; Rules: "Every series
// exists from start, for every value of its labels that applies [...]
// queue series exist only for queues. Counters start at 0". design.md,
// Delivery, Queue jobs: "The hold starts at the worker's PUBACK and lasts
// visibility_timeout".
//
// On a broker with the queue orders, every series of orders reads 0. A
// publisher stores a job: one ready. A worker subscribes and receives its
// offer: one worker, one offered, none ready or held, one delivered. It
// acknowledges the offer: one held, none offered. It answers ack: none
// held, one acknowledged. It disconnects: no worker.
func TestWorkerSeriesFollowEachTransition(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	series := func(workers, ready, offered, holding, delivered, acknowledged float64) map[string]float64 {
		return map[string]float64{
			support.ChannelSeries("saguin_queue_workers", ordersQueue.Name):                  workers,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "ready"):   ready,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "backoff"): 0,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "offered"): offered,
			support.ChannelSeries("saguin_queue_jobs", ordersQueue.Name, "state", "held"):    holding,
			support.ChannelSeries("saguin_queue_delivered_total", ordersQueue.Name):          delivered,
			support.ChannelSeries("saguin_queue_acknowledged_total", ordersQueue.Name):       acknowledged,
		}
	}
	awaitSeries(t, b, series(0, 0, 0, 0, 0, 0))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	job := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, job)
	awaitSeries(t, b, series(0, 1, 0, 0, 0, 0))

	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	offer := support.DeliveredAt(t, w, 1, job.Topic, job.Payload)
	awaitSeries(t, b, series(1, 0, 1, 0, 1, 0))
	acknowledge(t, w, offer.ID, 0)
	awaitSeries(t, b, series(1, 0, 0, 1, 1, 0))
	respond(t, w, attemptOf(t, offer), "ack", 0x00)
	awaitSeries(t, b, series(1, 0, 0, 0, 1, 1))
	support.Disconnected(t, w, &harness.Disconnect{})
	awaitSeries(t, b, series(0, 0, 0, 0, 1, 1))
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A worker that unsubscribes
// keeps the job it holds until it answers, the hold ends or it goes. Its
// offers not yet sent are cancelled"; Delivery, Queue jobs: "The hold
// starts at the worker's PUBACK"; "Jobs are offered oldest first"; Flows,
// SUBSCRIBE, UNSUBSCRIBE: "A delivery prepared but not yet
// handed to the writer is cancelled and never sent when no remaining
// subscription is owed it"; Delivery, Positions: "Before handing a QoS 1 or
// QoS 2 delivery to a durable session's writer, the core stores its packet
// identifier"; A shared group: "Selection is round-robin by complete client
// id, starting after the last selected id".
//
// On a stallable broker, worker -a, holding a durable session, acknowledges
// the offer of orders/1 and unsubscribes: UNSUBACK 0x00. Worker -b
// subscribes, and a publisher stores orders/2: -b receives orders/2, not
// orders/1, which -a still holds. -a answers ack: orders/1's job is
// finished, and orders/2's alone is unfinished. -b completes orders/2.
// -a subscribes again. With the store stalled, the publisher stores
// orders/3, whose offer to -a, next after -b, waits for its exchange to be
// stored; -a unsubscribes at once. -b receives orders/3, and -a's next
// packet after its UNSUBACK is a PINGRESP: the offer it was prepared was
// never sent.
func TestUnsubscribedWorkerKeepsItsHeldJob(t *testing.T) {
	s := launchStallable(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	db := support.OpenDB(t, s.DB)
	a := support.Subscribed(t, s.Addr, harness.V5, kept(harness.V5, clientID(t, "-a")), support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{support.Sized("orders/1", testkit.RecordBytes), support.Sized("orders/2", testkit.RecordBytes), support.Sized("orders/3", testkit.RecordBytes)}
	support.Acknowledged(t, pub, ps[0])
	holding := support.DeliveredAt(t, a, 1, ps[0].Topic, ps[0].Payload)
	acknowledge(t, a, holding.ID, 0)
	unsuback(t, a, nil, []string{ordersWorker}, 0x00)

	bb := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, ordersWorker))
	support.Acknowledged(t, pub, ps[1])
	second := support.DeliveredAt(t, bb, 1, ps[1].Topic, ps[1].Payload)
	respond(t, a, attemptOf(t, holding), "ack", 0x00)
	awaitUnfinished(t, db, []string{ps[1].Topic}, "once -a, unsubscribed, has answered ack for the job it held")
	completed(t, bb, second)
	awaitUnfinished(t, db, nil, "once -b has answered ack")

	support.Suback(t, a, nil, support.At(1, ordersWorker), 0x01)
	s.stall(t, true)
	support.Acknowledged(t, pub, ps[2])
	unsuback(t, a, nil, []string{ordersWorker}, 0x00)
	support.DeliveredAt(t, bb, 1, ps[2].Topic, ps[2].Payload)
	s.stall(t, false)
	taken(t, a, "its UNSUBACK, with its offer of orders/3 cancelled")
}

// design.md, Flows, CONNECT, 9. Resume: "The broker reads in-flight
// messages and PUBRELs from the store and resends them with their packet
// identifiers and in their order. Resent messages have DUP set
// (MQTT-4.4.0-1)"; Delivery, A resend: "A resend uses the Subscription
// Identifiers and delivery properties saved for its first transmission";
// Service publications: "a worker offer is a new broker-originated QoS 1
// publication containing the job payload and attempt metadata"; storage.md,
// session_inflight: attempt, "a queue job's attempt id, its Correlation
// Data (Words: Attempt), so a resend is the same message"; attempt_number,
// "the job's saguin-attempt as sent". MQTT 5 [MQTT-4.4.0-1]: "the Client
// and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0)
// [...] using their original Packet Identifiers".
//
// A worker holding a durable session subscribes with Subscription
// Identifier 5, and receives the offer of orders/1; it disconnects without
// acknowledging it, and reconnects keeping its session. The offer is
// resent, DUP 1, under its packet identifier, with the first
// transmission's properties: its Correlation Data, saguin-attempt,
// Response Topic, Subscription Identifier and saguin- properties.
func TestDurableWorkerOfferIsResentAsTheSameMessage(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	connect := kept(harness.V5, clientID(t, "-worker"))
	w := identifiedSubscriber(t, b.Addr, connect, 1, []string{ordersWorker}, []uint32{5})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	job := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, job)
	first := support.DeliveredAt(t, w, 1, job.Topic, job.Payload)
	attemptOf(t, first)
	support.Away(t, w)

	w = resumed(t, b.Addr, harness.V5, connect)
	again := redelivered(t, w, first)
	if diff := sameProperties(again.Props, first.Props); diff != "" {
		t.Errorf("the resent offer carries %s, its first transmission's", diff)
	}
}

// design.md, Delivery, Admission qualification: "A shared group or queue
// dispatcher uses its common progress and its eligible recipients. A
// member's disconnection, permission hold or overdue obligation excludes
// that member, not other members"; Peer obligations: "An overdue obligation
// removes qualification at its deadline. It does not disconnect the peer";
// Queue jobs: "each worker receives one job at a time per queue";
// Execution, Capacities, Delivery admission: "The gate closes when the
// admission delay reaches delivery_admission_delay".
//
// On a stallable broker with delivery_peer_timeout 150ms, worker -a,
// holding a durable session, receives the offer of a job and acknowledges
// none. Worker -b, holding a durable session, joins. With the store
// stalled, a second job goes to -b, -a holding one already: the job's
// commit and then -b's stored exchange wait stallSync each, -a's
// acknowledgement long overdue. -b still qualifies, and the gate closes.
func TestOverdueWorkerAloneLosesQualification(t *testing.T) {
	s := launchStallable(t, &harness.Options{DeliveryPeerTimeout: 150 * time.Millisecond, Channels: []harness.Channel{ordersQueue}})
	a := support.Subscribed(t, s.Addr, harness.V5, kept(harness.V5, clientID(t, "-a")), support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	job := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, job)
	support.DeliveredAt(t, a, 1, job.Topic, job.Payload)

	bb := support.Subscribed(t, s.Addr, harness.V5, kept(harness.V5, clientID(t, "-b")), support.At(1, ordersWorker))
	s.stalledDelivery(t, pub, bb, "orders/2")
	if got := support.Sampled(t, s.Broker, gatedSecondsSeries)[gatedSecondsSeries]; got == 0 {
		t.Fatalf("%s reads 0 after -b's stalled offer; want the gate to have closed, -b qualifying beside the overdue -a", gatedSecondsSeries)
	}
}

// design.md, $saguin topics: "A worker session connected over MQTT 3.1.1
// gets no offers. Its subscription stays. Offers resume when it connects
// over MQTT 5"; Delivery, Queue jobs: "Workers are
// selected round-robin among those connected, with room and allowed by the
// acl_file, as a shared group's members are"; SUBSCRIBE, A shared group:
// "Ineligible members are skipped. Only a selection moves the last-selected
// id."
//
// Worker -a, holding a durable session, subscribes over MQTT 5 and goes
// away; it resumes over MQTT 3.1.1. Worker -b subscribes over MQTT 5. Two
// jobs are stored: -b receives the first and, once it has completed it, the
// second, -a being skipped while on MQTT 3.1.1. -a then resumes over MQTT 5,
// and the next job, round-robin after -b, is offered to -a.
func TestWorkerOnMQTT311GetsNoOffers(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	aID := clientID(t, "-a")
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, kept(harness.V5, aID), support.At(1, ordersWorker)))
	a := resumed(t, b.Addr, harness.V311, kept(harness.V311, aID))
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	jobs := []*harness.Publish{
		{QoS: 1, Topic: "orders/1", Payload: []byte("first")},
		{QoS: 1, Topic: "orders/2", Payload: []byte("second")},
	}
	support.PublishAll(t, pub, jobs)
	for _, job := range jobs {
		completed(t, w, support.DeliveredAt(t, w, 1, job.Topic, job.Payload))
	}

	support.Away(t, a)
	a = resumed(t, b.Addr, harness.V5, kept(harness.V5, aID))
	third := &harness.Publish{QoS: 1, Topic: "orders/3", Payload: []byte("third")}
	support.Acknowledged(t, pub, third)
	completed(t, a, support.DeliveredAt(t, a, 1, third.Topic, third.Payload))
}

// design.md, $saguin topics: "A worker session connected over MQTT 3.1.1
// gets no offers"; Delivery, Queue jobs: "The wait starts at
// the attempt's end, at its return, a refusing PUBACK, its timeout or the
// worker's going". CONNECT, 9. Resume: "The broker reads in-flight messages
// and PUBRELs from the store and resends them".
//
// Worker -a, holding a durable session, receives an offer over MQTT 5 and
// goes away without its PUBACK. It resumes over MQTT 3.1.1 and answers
// nothing. Worker -b, over MQTT 5, then receives the job, its attempt having
// ended with -a's going, and completes it. -a's session then stores no
// exchange: the offer it could not receive was settled as if sent, not
// resent and left waiting for a PUBACK.
func TestWorkerOnMQTT311GetsNoRecoveredOffer(t *testing.T) {
	t.Parallel()
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{ordersQueue}})
	db := support.OpenDB(t, b.DB)
	aID := clientID(t, "-a")
	a := support.Subscribed(t, b.Addr, harness.V5, kept(harness.V5, aID), support.At(1, ordersWorker))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	job := &harness.Publish{QoS: 1, Topic: "orders/1", Payload: []byte("a job")}
	support.Acknowledged(t, pub, job)
	support.DeliveredAt(t, a, 1, job.Topic, job.Payload)
	support.Away(t, a)

	a = resumed(t, b.Addr, harness.V311, kept(harness.V311, aID))
	taken(t, a, "resuming over MQTT 3.1.1 with an offer unacknowledged")
	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, ordersWorker))
	completed(t, w, support.DeliveredAt(t, w, 1, job.Topic, job.Payload))
	taken(t, a, "-b completing the job")

	awaitState(t, "with -a on MQTT 3.1.1 and the job completed by -b, no stored exchange",
		func() []inflightRow { return storedInflight(t, db, aID) },
		func(rows []inflightRow) bool { return len(rows) == 0 })
}
