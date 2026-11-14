package conformance

import (
	"bytes"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of latest channels: one value per topic, replaced in place at a
// new offset, a subscription's current values and then each change, empty
// payloads as tombstones, the retention of values and tombstones, and the
// values a changed filter leaves behind (design.md, Channels, latest;
// Words: Retention; Flows, SUBSCRIBE and Delivery; Restart, A changed
// configuration; storage.md, records).

// kvChannel is the latest channel of these tests, holding kv/#.
var kvChannel = harness.Channel{Name: "kv", Type: "latest", Filter: "kv/#"}

// currentPass is initialPass of a latest channel's current values: RETAIN
// 1 (design.md, Flows, SUBSCRIBE, Options: "a `latest` channel's initial
// pass to a client: its current values carry RETAIN 1, as values sent
// because of a new subscription do"), so a client tells the state it arrives
// to from a change, each of which comes with RETAIN 0.
func currentPass(t *testing.T, c *harness.Client, qos byte, values map[string][]byte) {
	t.Helper()
	initialPass(t, c, qos, true, values)
}

// value is a QoS 1 publication of payload on topic, an empty payload a
// deletion.
func value(topic, payload string) *harness.Publish {
	return &harness.Publish{QoS: 1, Topic: topic, Payload: []byte(payload)}
}

// channelRecords is the records of the channel named in the database at
// path, in offset order.
func channelRecords(t *testing.T, path, channel string) []record {
	t.Helper()
	return slices.DeleteFunc(records(t, path), func(r record) bool { return r.name != channel })
}

// design.md, Channels: "latest is a Kafka-like compacted topic. It holds
// one value per topic, replaced in place at a new offset, in one write";
// storage.md, records: retained "1 while it is its topic's current value:
// [...] on a latest channel, every record"; "A latest channel holds one
// record per topic. A new value updates the topic's record in place: its
// offset, its saguin-id, its time, QoS, publisher, bridge, expiry,
// properties, payload and payload size"; design.md, Store: "full mode
// [...] Everything acknowledged survives a crash or a power cut"; Words:
// Offset: "The core assigns it when a publication enters the channel, in
// entry order"; Flows, SUBSCRIBE, the subscription table: "latest | the
// current values, as Retain Handling allows, then each change".
//
// A publisher stores on kv kv/a "one", kv/b "b" and kv/a "two", at offsets
// 1 to 3, and the broker is killed. Started again, a new subscription to
// kv/# receives kv/a "two" and kv/b "b", RETAIN 0; the database holds two
// records on kv, kv/b at offset 2 and kv/a at offset 3, each flagged
// retained with its payload, the record of kv/a "one" gone.
func TestLatestValueIsReplacedAtANewOffset(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{kvChannel}}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{value("kv/a", "one"), value("kv/b", "b"), value("kv/a", "two")})
	support.Crash(t, b)

	b = support.Launch(t, o)
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-sub", CleanStart: true}, support.At(0, "kv/#"))
	currentPass(t, sub, 0, map[string][]byte{"kv/a": []byte("two"), "kv/b": []byte("b")})
	type kvRow struct {
		offset   int64
		topic    string
		payload  string
		retained int
	}
	rs := channelRecords(t, b.DB, "kv")
	got := make([]kvRow, 0, len(rs))
	for _, r := range rs {
		got = append(got, kvRow{r.offset, r.topic, string(r.payload), r.retained})
	}
	if want := []kvRow{{2, "kv/b", "b", 1}, {3, "kv/a", "two", 1}}; !slices.Equal(got, want) {
		t.Fatalf("after the restart kv holds %+v; want %+v: one record per topic, kv/a's replaced at its new offset", got, want)
	}
}

// design.md, Flows, SUBSCRIBE, the subscription table: "latest | the
// current values, as Retain Handling allows, then each change"; The start
// point: "When Retain Handling requests another initial pass, that pass
// takes a fresh boundary"; Options: "a `latest` channel's initial pass to a
// client: its current values carry RETAIN 1";
// Channels, latest: "An empty payload deletes the value"; Delivery, Order:
// "On latest, they follow offset order of changes". MQTT 5 3.3.1.3:
// Retain Handling 0, the Server "MUST send the retained messages matching
// the Topic Filter" [MQTT-3.3.1-9]; 1, "if the subscription did not already
// exist [...] MUST send all retained message matching the Topic Filter
// [...] and if the subscription did exist the Server MUST NOT send"
// [MQTT-3.3.1-10]; 2, "the Server MUST NOT send the retained messages"
// [MQTT-3.3.1-11]; [MQTT-3.8.4-4], with Retain Handling 0 "any existing
// retained messages matching the Topic Filter MUST be re-sent".
//
// A publisher stores kv/a, kv/b and kv/c. A client subscribes to kv/a
// with Retain Handling 0 and receives its value; the publisher replaces it,
// which arrives live. Subscribing to kv/a again with Retain Handling 0, the
// client receives the newer value; then to kv/a with Retain Handling 1,
// and to kv/b with Retain Handling 1: it receives kv/b's value alone; then
// to kv/c with Retain Handling 2. The publisher deletes kv/c and replaces
// kv/b: the client's next packets are the deletion, an empty payload, and
// kv/b's new value, live.
func TestLatestSubscriptionGetsTheCurrentValuesThenEachChange(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{kvChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{value("kv/a", "a1"), value("kv/b", "b1"), value("kv/c", "c1")})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-sub", CleanStart: true})

	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "kv/a", RetainHandling: 0}}, 0x00)
	currentPass(t, sub, 0, map[string][]byte{"kv/a": []byte("a1")})
	changed := value("kv/a", "a2")
	support.Acknowledged(t, pub, changed)
	liveAt(t, sub, 0, changed.Topic, changed.Payload)

	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "kv/a", RetainHandling: 0}}, 0x00)
	currentPass(t, sub, 0, map[string][]byte{"kv/a": []byte("a2")})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "kv/a", RetainHandling: 1}}, 0x00)
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "kv/b", RetainHandling: 1}}, 0x00)
	currentPass(t, sub, 0, map[string][]byte{"kv/b": []byte("b1")})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "kv/c", RetainHandling: 2}}, 0x00)

	deleted, replaced := value("kv/c", ""), value("kv/b", "b2")
	support.PublishAll(t, pub, []*harness.Publish{deleted, replaced})
	liveAt(t, sub, 0, deleted.Topic, deleted.Payload)
	liveAt(t, sub, 0, replaced.Topic, replaced.Payload)
}

// design.md, Flows, SUBSCRIBE, The start point: "The core takes that
// channel's next offset when it takes the SUBSCRIBE, in the connection's
// packet order. Every publication that entered before the SUBSCRIBE
// therefore falls below the boundary"; "The captured boundary separates the
// initial pass, which reads retained or current values below it, from
// subsequent live records. The initial pass's first read waits until every
// publication below the boundary is stored or refused"; the subscription
// table: "latest | the current values, as Retain Handling allows, then
// each change".
//
// The test holds the database's write lock, as another program on the file
// may. A publisher sends kv/value at QoS 1, whose write goes to the store
// and is held there, then pings: the PINGRESP says the core has taken it.
// A client with no session to save subscribes to kv/# and pings: the core
// has taken the SUBSCRIBE while the value is still being stored. Once the
// lock is released, the PUBACK is 0x00, the SUBACK grants QoS 0, and the
// subscriber receives the value in its initial pass, and then a newer
// value of the topic live, nothing between.
func TestLatestValueBeingStoredAtTheSubscribeIsInTheInitialPass(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{kvChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-sub", CleanStart: true})
	lock := holdWriteLock(t, b.DB)
	slow := value("kv/value", "being stored")
	support.Sent(t, pub, slow)
	taken(t, pub, "a PUBLISH, the store's write lock held")
	s := &harness.Subscribe{ID: sub.NextID(), Subs: support.At(0, "kv/#")}
	if err := sub.Send(s); err != nil {
		t.Fatal(err)
	}
	// The SUBACK, which confirms no stored change, may come before the
	// PINGRESP, and is kept.
	takenKeeping(t, sub, "a SUBSCRIBE, the store's write lock held")
	lock.release(t)

	support.Puback(t, pub, slow)
	p, err := sub.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Suback); return ok && a.ID == s.ID })
	if err != nil {
		t.Fatalf("waiting for the SUBACK: %v", err)
	}
	if reasons := p.(*harness.Suback).Reasons; !bytes.Equal(reasons, []byte{0x00}) {
		t.Fatalf("SUBACK % X, want 00", reasons)
	}
	currentPass(t, sub, 0, map[string][]byte{slow.Topic: slow.Payload})
	later := value("kv/value", "later")
	support.Acknowledged(t, pub, later)
	liveAt(t, sub, 0, later.Topic, later.Payload)
}

// design.md, Channels, latest: "An empty payload deletes the value and
// leaves a tombstone [...] Returning consumers receive surviving tombstones
// as empty payloads"; Flows,
// SUBSCRIBE, the subscription table: "latest | the current values, as
// Retain Handling allows, then each change"; DISCONNECT: "The session goes
// away until its expiry".
//
// A publisher stores kv/a, then kv/gone and its deletion. A client holding
// a durable session subscribes to kv/# at QoS 1: its initial pass is kv/a
// alone, the tombstone being no value, and a value then stored on kv/live
// arrives next, live. The client goes away; the publisher deletes kv/a.
// The client resumes: its next PUBLISH is kv/a's tombstone, an empty
// payload, and then a value stored after it.
func TestLatestTombstoneReachesOnlyAReturningConsumer(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{kvChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{value("kv/a", "a"), value("kv/gone", "x"), value("kv/gone", "")})
	connect := &harness.Connect{ClientID: "kv-returning", CleanStart: true, Props: support.Durable}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "kv/#"))
	currentPass(t, sub, 1, map[string][]byte{"kv/a": []byte("a")})
	fresh := value("kv/live", "live")
	support.Acknowledged(t, pub, fresh)
	acknowledge(t, sub, liveAt(t, sub, 1, fresh.Topic, fresh.Payload).ID, 0)
	support.Away(t, sub)

	deletion := value("kv/a", "")
	support.Acknowledged(t, pub, deletion)
	sub = resumed(t, b.Addr, harness.V5, connect)
	acknowledge(t, sub, liveAt(t, sub, 1, deletion.Topic, deletion.Payload).ID, 0)
	thenLiveAt(t, pub, sub, 1, "kv/after")
}

// design.md, Flows, Delivery, Order: "On latest, they follow offset order
// of changes and skip replaced values"; One mechanism for every channel:
// "The window is the lower of the client's Receive Maximum and
// limits.max_inflight [...] A message is in flight from its hand-off to the
// writer"; Channels, latest: a value is "replaced in place at a new
// offset". MQTT 5 [MQTT-4.9.0-2]: at a send quota of zero the Server "MUST
// NOT send any more PUBLISH packets with QoS > 0".
//
// A client with Receive Maximum 1 subscribes to kv/# at QoS 1 and holds
// kv/a unacknowledged. A publisher stores kv/b "1", kv/c "1" and kv/b "2",
// replacing kv/b while it waits to be sent. Once the client acknowledges
// kv/a, it receives kv/c "1", then kv/b "2", then a value stored after
// them: kv/b "1" never.
func TestLatestValueReplacedWhileWaitingIsSkipped(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{kvChannel}})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-sub", CleanStart: true, Props: receiveMaximum5(1)}, support.At(1, "kv/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	first := value("kv/a", "1")
	support.Acknowledged(t, pub, first)
	held := support.DeliveredAt(t, sub, 1, first.Topic, first.Payload)
	support.PublishAll(t, pub, []*harness.Publish{value("kv/b", "1"), value("kv/c", "1"), value("kv/b", "2")})

	acknowledge(t, sub, held.ID, 0)
	acknowledge(t, sub, support.DeliveredAt(t, sub, 1, "kv/c", []byte("1")).ID, 0)
	acknowledge(t, sub, support.DeliveredAt(t, sub, 1, "kv/b", []byte("2")).ID, 0)
	thenLiveAt(t, pub, sub, 1, "kv/after")
}

// design.md, Flows, Delivery, A message over the client's Maximum Packet
// Size: "On a latest channel, a recovered exchange whose record is gone is
// settled as if sent, and not counted, whatever removed it"; Words: Missed: "An exchange owed
// only its PUBREL needs no record. Its PUBREL is resent and nothing is
// counted (MQTT-4.4.0-1)"; CONNECT, 9. Resume: "The broker reads in-flight
// messages and PUBRELs from the store and resends them"; "New application
// deliveries wait behind recovered PUBLISHes"; Delivery, Positions:
// "Deleting a durable session's settled exchange row must commit with its
// position beyond that record". metrics.md, saguin_missed_total: "records
// consumers were owed and never received". MQTT 5 [MQTT-4.3.3-6]: the
// sender "MUST treat the PUBREL packet as "unacknowledged" until it has
// received the corresponding PUBCOMP".
//
// A client holding a durable session subscribes to kv/# at QoS 2. A
// publisher stores kv/a at QoS 1, which the client holds unacknowledged,
// and kv/b at QoS 2, which the client takes to its PUBREL and leaves
// without its PUBCOMP. The client goes away and the publisher replaces
// both. The client resumes: kv/b's PUBREL comes again, kv/a's first value
// does not, and kv/a's and kv/b's new values arrive as first deliveries, in
// that order. Once the client has completed every exchange, the session
// stores none, and nothing has been counted missed on kv.
func TestLatestResendOfAReplacedValueIsSettled(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{kvChannel}})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: "kv-resumer", CleanStart: true, Props: support.Durable}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "kv/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	support.Acknowledged(t, pub, value("kv/a", "1"))
	support.DeliveredAt(t, sub, 1, "kv/a", []byte("1"))
	published(t, pub, twice("kv/b", "1"))
	owed := support.DeliveredAt(t, sub, 2, "kv/b", []byte("1"))
	released(t, sub, owed.ID)
	support.Away(t, sub)

	newer := []*harness.Publish{value("kv/a", "2"), twice("kv/b", "2")}
	support.Acknowledged(t, pub, newer[0])
	published(t, pub, newer[1])
	sub = resumed(t, b.Addr, harness.V5, connect)
	var deliveries []*harness.Publish
	for resent := false; !resent || len(deliveries) < len(newer); {
		switch p := harness.Expect[harness.Packet](t, sub).(type) {
		case *harness.Ack:
			if resent || p.Kind != harness.PUBREL || p.ID != owed.ID || p.Reason != 0x00 {
				t.Fatalf("after resuming, %s %d 0x%02X; want kv/b's PUBREL %d, 0x00, once", harness.Name(p.Kind), p.ID, p.Reason, owed.ID)
			}
			resent = true
		case *harness.Publish:
			if len(deliveries) == len(newer) {
				t.Fatalf("after resuming, PUBLISH %q of %.40q, DUP %v; want only kv/b's PUBREL still to come", p.Topic, p.Payload, p.Dup)
			}
			want := newer[len(deliveries)]
			if p.Topic != want.Topic || !bytes.Equal(p.Payload, want.Payload) || p.QoS != want.QoS || p.Dup {
				t.Fatalf("after resuming, PUBLISH %q of %.40q at QoS %d, DUP %v; want %q of %.40q at QoS %d, DUP 0: kv/a's replaced value settled, not resent",
					p.Topic, p.Payload, p.QoS, p.Dup, want.Topic, want.Payload, want.QoS)
			}
			deliveries = append(deliveries, p)
		default:
			t.Fatalf("after resuming, %s; want kv/b's PUBREL and the new values", harness.Name(p.Type()))
		}
	}
	pubcomp(t, sub, owed.ID)
	acknowledge(t, sub, deliveries[0].ID, 0)
	released(t, sub, deliveries[1].ID)
	pubcomp(t, sub, deliveries[1].ID)

	var rows []inflightRow
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		rows = storedInflight(t, db, connect.ClientID)
		return len(rows) == 0
	}) {
		t.Fatalf("with every exchange completed, the session still stores %+v; want none, kv/a's first value settled as if sent", rows)
	}
	expectSample(t, b, testkit.MissedSeries("kv"), 0, "once kv/a's replaced value has been settled")
	expectSample(t, b, `saguin_missed_total{channel="kv",cause="too_large"}`, 0, "once kv/a's replaced value has been settled")
}

// design.md, Words: Retention: "A latest channel is the exception. There,
// retention deletes each record once it is due, wherever it sits.
// Nonempty current values expire under retention_period. Tombstones expire
// under tombstone_retention_period [...] A loss counts each deleted record a
// consumer was owed"; Missed: "missed records are those a consumer was owed
// but never received because retention removed them"; "Away sessions use
// their stored state"; config.md,
// channels.<name>: retention_period, "latest: retention of nonempty current
// values"; tombstone_retention_period, "how long a tombstone stays".
// metrics.md, saguin_missed_total: "away sessions included".
//
// kv keeps values for 4 seconds and tombstones for 1. A client holding a
// durable session subscribes to kv/# at QoS 1 and goes away. A publisher
// stores kv/old at offset 1, then kv/gone and its deletion, the tombstone
// at offset 3. The tombstone is deleted while kv/old, below it, stays;
// then kv/old is deleted. Two records the session was owed have been
// deleted, and two are counted missed: kv/gone's value, replaced, was not
// deleted by retention. The client resumes, and its next PUBLISH is a value
// stored after it.
func TestLatestRetentionDeletesEachRecordWhenDue(t *testing.T) {
	t.Parallel()
	kv := kvChannel
	kv.RetentionPeriod, kv.TombstoneRetentionPeriod = 4*time.Second, time.Second
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{kv}})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: "kv-away", CleanStart: true, Props: support.Durable}
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "kv/#")))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{value("kv/old", "v"), value("kv/gone", "x"), value("kv/gone", "")})

	awaitHeld(t, db, "kv", []string{"kv/old"}, "once kv/gone's tombstone has passed tombstone_retention_period")
	awaitHeld(t, db, "kv", nil, "once kv/old has passed retention_period")
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("kv"), 2, "once retention has deleted kv/old and kv/gone's tombstone, both owed")
	sub := resumed(t, b.Addr, harness.V5, connect)
	thenLiveAt(t, pub, sub, 1, "kv/after")
}

// design.md, Flows, PUBLISH, the table: QoS 2 "The publication enters on
// PUBREL, from a held copy stored and charged to the channel before
// PUBREC"; Delivery, Order: "On latest, they follow offset order of
// changes [...] A later QoS 0 message can overtake a publisher's earlier
// QoS 2 one"; Words: Offset: "The core assigns it when a publication enters
// the channel, in entry order"; Channels, latest: "It holds one value per
// topic, replaced in place at a new offset".
//
// A client subscribes to kv/# at QoS 0. A publisher sends kv/a "first" at
// QoS 2 and, after its PUBREC, kv/a "second" at QoS 0, then the first's
// PUBREL, which gets PUBCOMP 0x00. The subscriber receives "second", then
// "first": the QoS 2 value entered last. A new subscription then receives
// "first", kv/a's current value.
func TestLatestQoS2ValueEntersAtPubrel(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{kvChannel}})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-sub", CleanStart: true}, support.At(0, "kv/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	first := twice("kv/a", "first")
	support.Sent(t, pub, first)
	pubrecOf(t, pub, first, 0x00)
	second := &harness.Publish{QoS: 0, Topic: "kv/a", Payload: []byte("second")}
	if err := pub.Send(second); err != nil {
		t.Fatalf("PUBLISH %q at QoS 0: %v", second.Topic, err)
	}
	pubrel(t, pub, first.ID, 0x00)

	liveAt(t, sub, 0, second.Topic, second.Payload)
	liveAt(t, sub, 0, first.Topic, first.Payload)
	late := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-late", CleanStart: true}, support.At(0, "kv/#"))
	currentPass(t, late, 0, map[string][]byte{first.Topic: first.Payload})
}

// design.md, Flows, SUBSCRIBE, the subscription table:
// "$share/<group>/<filter> over any of the above but a worker's | new
// messages only (changes only on latest), no retained values"; A shared
// group: "A new group starts at the tail of every channel it reaches".
// MQTT 5 4.8.2: "No Retained Messages are sent to the Session when it first
// subscribes".
//
// A publisher stores kv/a. A client subscribes to $share/g/kv/# with Retain
// Handling 0. The publisher stores kv/b, then replaces kv/a: the member's
// next packets are those two changes, RETAIN 0, and never kv/a's earlier
// value.
func TestLatestSharedGroupGetsChangesOnly(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{kvChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-pub", CleanStart: true})
	support.Acknowledged(t, pub, value("kv/a", "before"))
	member := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kv-member", CleanStart: true},
		[]harness.Subscription{{Filter: shareOf("g", "kv/#"), RetainHandling: 0}})
	changes := []*harness.Publish{value("kv/b", "change"), value("kv/a", "after")}
	support.PublishAll(t, pub, changes)
	for _, p := range changes {
		liveAt(t, member, 0, p.Topic, p.Payload)
	}
}

// design.md, Restart, A changed configuration: "records stay in the
// channel where they landed. At start, the broker counts and logs latest
// values whose topics no longer match their channel's filter"; "A stored
// channel that is no longer configured, or is configured with another
// type, stops startup", so one configured with another filter starts.
// Logging: each line holds "the time [...], the level [...], the broker id
// and the message", "separated by single spaces".
//
// A latest channel, readings, holds readings/#. A publisher stores
// readings/a/1, readings/b/1, readings/b/2 and readings/c, and the broker
// stops. Started again with readings holding readings/a/# alone, the
// broker logs a line whose message names readings and counts 3: the values
// of readings/b/1, readings/b/2 and readings/c.
func TestStartupCountsLatestValuesOutsideAChangedFilter(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{{Name: "readings", Type: "latest", Filter: "readings/#"}}}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "readings-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{
		value("readings/a/1", "kept"), value("readings/b/1", "outside"), value("readings/b/2", "outside"), value("readings/c", "outside"),
	})
	stopped(t, b)

	o.Channels = []harness.Channel{{Name: "readings", Type: "latest", Filter: "readings/a/#"}}
	b = support.Launch(t, o)
	stopped(t, b)
	count := regexp.MustCompile(`\b3\b`)
	for _, level := range []string{"INFO", "WARN", "ERROR"} {
		for _, line := range testkit.LinesNaming(b.Log(), level, "readings") {
			// The message follows the time, the level and the broker id.
			if fields := strings.SplitN(line, " ", 4); len(fields) == 4 && strings.Contains(fields[3], "readings") && count.MatchString(fields[3]) {
				return
			}
		}
	}
	t.Fatalf("no log line's message names readings and counts its 3 values outside readings/a/#; the log:\n%s", b.Log())
}
