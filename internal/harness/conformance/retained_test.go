package conformance

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// The tests of retained values on $broadcast: each topic's current
// retained value, how it is replaced, cleared and ended, its own size
// limit, and the retained pass a new subscription receives (design.md,
// Channels, $broadcast, Retained; Flows, SUBSCRIBE, the subscription table
// and The start point).

// retainedPass is retainedPassAt QoS 0.
func retainedPass(t *testing.T, c *harness.Client, values map[string][]byte) map[string]*harness.Publish {
	t.Helper()
	return retainedPassAt(t, c, 0, values)
}

// retainedPassAt is initialPass of each topic's current retained value,
// with RETAIN 1 (design.md, Channels, $broadcast: "RETAIN is 1 on retained
// values sent to a new subscription"; MQTT 5 3.3.1.3: "These messages are
// sent with the RETAIN flag set to 1"; MQTT 3.1.1 [MQTT-3.3.1-8]).
func retainedPassAt(t *testing.T, c *harness.Client, qos byte, values map[string][]byte) map[string]*harness.Publish {
	t.Helper()
	return initialPass(t, c, qos, true, values)
}

// initialPass reads c's next len(values) packets and fails the test unless
// they are values, a payload by topic: each topic's current value once, in
// any order, at qos, DUP 0, with RETAIN retain. It acknowledges each value
// at QoS 1 once read, so a Receive Maximum of 1 lets the next come. It
// returns them by topic: the initial pass of retainedPassAt on $broadcast,
// and of currentPass on a latest channel.
func initialPass(t *testing.T, c *harness.Client, qos byte, retain bool, values map[string][]byte) map[string]*harness.Publish {
	t.Helper()
	got := make(map[string]*harness.Publish, len(values))
	for range values {
		p := harness.Expect[*harness.Publish](t, c)
		want, ok := values[p.Topic]
		switch {
		case !ok || got[p.Topic] != nil:
			t.Fatalf("PUBLISH %q of payload %.40q, RETAIN %v, after %d of the initial pass; want one of the %d current values not yet received",
				p.Topic, p.Payload, p.Retain, len(got), len(values)-len(got))
		case !bytes.Equal(p.Payload, want):
			t.Fatalf("the initial pass's value of %q has payload %.40q; want %.40q, its current value", p.Topic, p.Payload, want)
		case p.QoS != qos || p.Dup:
			t.Fatalf("the initial pass's value of %q came at QoS %d, DUP %v; want QoS %d, the lower of its own and the grant, DUP 0", p.Topic, p.QoS, p.Dup, qos)
		case p.Retain != retain:
			t.Fatalf("the initial pass's value of %q came with RETAIN %v; want %v", p.Topic, p.Retain, retain)
		}
		got[p.Topic] = p
		if qos == 1 {
			acknowledge(t, c, p.ID, 0)
		}
	}
	return got
}

// live is liveAt QoS 0.
func live(t *testing.T, c *harness.Client, topic string, payload []byte) {
	t.Helper()
	liveAt(t, c, 0, topic, payload)
}

// liveAt reads c's next packet and fails the test unless it is the first
// delivery at qos of topic and payload (deliveredAt) with RETAIN 0:
// design.md, Channels, $broadcast, RETAIN "Otherwise it is 0 unless Retain As
// Published"; MQTT 5 [MQTT-3.3.1-12]; MQTT 3.1.1 [MQTT-3.3.1-9]. It returns
// the delivery, unacknowledged.
func liveAt(t *testing.T, c *harness.Client, qos byte, topic string, payload []byte) *harness.Publish {
	t.Helper()
	p := support.DeliveredAt(t, c, qos, topic, payload)
	if p.Retain {
		t.Fatalf("the live delivery of %q came with RETAIN 1; want 0, its subscription established", topic)
	}
	return p
}

// thenLive is thenLiveAt QoS 0.
func thenLive(t *testing.T, pub, c *harness.Client, topic string) {
	t.Helper()
	thenLiveAt(t, pub, c, 0, topic)
}

// thenLiveAt has pub publish a message on topic, not retained, at QoS 1,
// and fails the test unless it is c's next packet, a live delivery at qos:
// a subscription starts with its retained values, "then the next message"
// (design.md, Flows, SUBSCRIBE, the subscription table), so whatever c has
// not read before it is no part of what it is owed.
func thenLiveAt(t *testing.T, pub, c *harness.Client, qos byte, topic string) {
	t.Helper()
	p := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("live")}
	support.Acknowledged(t, pub, p)
	liveAt(t, c, qos, p.Topic, p.Payload)
}

// retain returns a retained QoS 1 publication on topic of payload.
func retain(topic string, payload []byte) *harness.Publish {
	return &harness.Publish{QoS: 1, Retain: true, Topic: topic, Payload: payload}
}

// design.md, Flows, SUBSCRIBE, the subscription table: "$broadcast | the
// matching retained values, as Retain Handling allows, then the next
// message"; Channels, $broadcast: "RETAIN is 1 on retained values sent to a
// new subscription. Otherwise it is 0 unless Retain As Published". MQTT 3.1.1
// [MQTT-3.3.1-6]: "When a new subscription is established, the last retained
// message, if any, on each matching topic name MUST be sent to the
// subscriber"; [MQTT-3.3.1-8]: RETAIN 1 "if a message is sent as a result of
// a new subscription"; [MQTT-3.3.1-9]: RETAIN 0 "because it matches an
// established subscription". MQTT 5 [MQTT-3.3.1-9], under Retain Handling 0,
// the default.
//
// A publisher stores retained values on two topics a filter matches and
// one it does not. An MQTT 3.1.1 client then subscribes to the filter at
// QoS 0: it receives the two values, RETAIN 1, and then a retained message
// published after its SUBSCRIBE, RETAIN 0.
func TestNewSubscriptionGetsTheRetainedValuesFirst(t *testing.T) {
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{
		retain(prefix+"/a/1", []byte("one")),
		retain(prefix+"/a/2", []byte("two")),
		retain(prefix+"/b", []byte("elsewhere")),
	})
	sub := support.Subscribed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, prefix+"/a/#"))
	retainedPass(t, sub, map[string][]byte{prefix + "/a/1": []byte("one"), prefix + "/a/2": []byte("two")})

	later := retain(prefix+"/a/1", []byte("later"))
	support.Acknowledged(t, pub, later)
	live(t, sub, later.Topic, later.Payload)
}

// design.md, Channels, $broadcast, Retained: "A retained publish takes the
// flag; a plain publish leaves it; an empty retained publish clears it. A
// retained value ends only by losing its flag: to a newer retained
// publish, an empty one [...]". MQTT 5 [MQTT-3.3.1-5]: the Server "MUST
// replace any existing retained message for this topic"; [MQTT-3.3.1-6]:
// with a zero-byte payload "any retained message with the same topic name
// MUST be removed"; [MQTT-3.3.1-7]: "A retained message with a Payload
// containing zero bytes MUST NOT be stored as a retained message";
// [MQTT-3.3.1-8]: with RETAIN 0 it "MUST NOT remove or replace any
// existing retained message".
//
// A publisher stores two retained values on one topic; a retained value
// on a second, then a plain message there; a retained value on a third,
// then an empty retained message there. A client then subscribes to them
// all: it receives the first topic's newer value and the second's retained
// one, RETAIN 1, and then a message published after its SUBSCRIBE.
func TestRetainedValueIsReplacedKeptAndCleared(t *testing.T) {
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{
		retain(prefix+"/replaced", []byte("old")),
		retain(prefix+"/replaced", []byte("new")),
		retain(prefix+"/kept", []byte("kept")),
		{QoS: 1, Topic: prefix + "/kept", Payload: []byte("plain")},
		retain(prefix+"/cleared", []byte("cleared")),
		retain(prefix+"/cleared", nil),
	})
	sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, prefix+"/#"))
	retainedPass(t, sub, map[string][]byte{prefix + "/replaced": []byte("new"), prefix + "/kept": []byte("kept")})
	thenLive(t, pub, sub, prefix+"/live")
}

// design.md, Flows, SUBSCRIBE, the subscription table: "$broadcast | the
// matching retained values, as Retain Handling allows"; The start point:
// "A SUBSCRIBE repeating an existing filter replaces its options [...].
// An initial pass reads in pages; each page sends the values current when
// it is read, below the tail at the cut [...]. When Retain Handling
// requests another initial pass (MQTT-3.8.4-3), that pass takes a fresh
// Store-read cut and each channel's tail at that cut; it does not reuse
// the original pass cutoff". MQTT 5 3.3.1.3, "At the time of the
// Subscription": Retain Handling 0, the Server "MUST send the retained
// messages matching the Topic Filter" [MQTT-3.3.1-9]; 1, "if the
// subscription did not already exist, the Server MUST send all retained
// message matching the Topic Filter [...] and if the subscription did
// exist the Server MUST NOT send the retained messages" [MQTT-3.3.1-10];
// 2, "the Server MUST NOT send the retained messages" [MQTT-3.3.1-11].
// [MQTT-3.8.4-4]: "If the Retain Handling option is 0, any existing
// retained messages matching the Topic Filter MUST be re-sent";
// [MQTT-3.8.4-5]: a SUBSCRIBE of several Topic Filters is handled "as if
// it had received a sequence of multiple SUBSCRIBE packets, except that it
// combines their responses into a single SUBACK response".
//
// A publisher stores retained values on topics a, b, c and d. A client
// subscribes to a with Retain Handling 0 and receives its value; the
// publisher replaces it, which the client receives live. The client
// subscribes to a again with Retain Handling 0, and receives a's value
// again, the newer one; then to a again with Retain Handling 1, to b with
// Retain Handling 1: it receives b's value alone. One SUBSCRIBE then
// names c twice, first with Retain Handling 2, then 1, the second finding
// the subscription the first made; another names d twice, first with
// Retain Handling 0, then 2: each gets SUBACK 0x00 0x00, and the client
// receives d's value, once. Last it subscribes to a filter matching all
// four with Retain Handling 2, and receives a message published after
// these SUBSCRIBEs.
func TestRetainHandling(t *testing.T) {
	prefix := clientID(t, "")
	a, b, c, d := prefix+"/a", prefix+"/b", prefix+"/c", prefix+"/d"
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{retain(a, []byte("first")), retain(b, []byte("b")), retain(c, []byte("c")), retain(d, []byte("d"))})
	sub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true})

	support.Suback(t, sub, nil, []harness.Subscription{{Filter: a, RetainHandling: 0}}, 0x00)
	retainedPass(t, sub, map[string][]byte{a: []byte("first")})
	newer := retain(a, []byte("newer"))
	support.Acknowledged(t, pub, newer)
	live(t, sub, newer.Topic, newer.Payload)

	support.Suback(t, sub, nil, []harness.Subscription{{Filter: a, RetainHandling: 0}}, 0x00)
	retainedPass(t, sub, map[string][]byte{a: []byte("newer")})

	support.Suback(t, sub, nil, []harness.Subscription{{Filter: a, RetainHandling: 1}}, 0x00)
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: b, RetainHandling: 1}}, 0x00)
	retainedPass(t, sub, map[string][]byte{b: []byte("b")})

	support.Suback(t, sub, nil, []harness.Subscription{{Filter: c, RetainHandling: 2}, {Filter: c, RetainHandling: 1}}, 0x00, 0x00)
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: d, RetainHandling: 0}, {Filter: d, RetainHandling: 2}}, 0x00, 0x00)
	retainedPass(t, sub, map[string][]byte{d: []byte("d")})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: prefix + "/+", RetainHandling: 2}}, 0x00)
	thenLive(t, pub, sub, prefix+"/live")
}

// design.md, Channels, $broadcast, Retained: "A retained value ends only
// by losing its flag: to [...] its Message Expiry"; "Message Expiry
// follows Delivery"; Delivery, Message Expiry: "worked out when the
// message is written and sent as the time remaining. On $broadcast an
// expired message is skipped for that subscriber". Flows, PUBLISH:
// "Message Expiry [...] use receipt time". MQTT 5 [MQTT-3.3.2-5]: once
// "the Message Expiry Interval has passed and the Server has not managed
// to start onward delivery to a matching subscriber, then it MUST delete
// the copy of the message for that subscriber"; 3.3.1.3: "If the current
// retained message for a Topic expires, it is discarded"; [MQTT-3.3.2-6]:
// the interval sent is "the received value minus the time that the
// Application Message has been waiting in the Server".
//
// A publisher stores a retained value expiring in 600 seconds and one, on
// another topic, expiring in 1 second. A second after that one's PUBACK,
// when it has expired, a client subscribes to both topics: it receives the
// first, RETAIN 1, its Message Expiry Interval at most 599 and no less
// than 600 less the whole seconds since it was sent, and then a message
// published after its SUBSCRIBE.
func TestExpiredRetainedValueIsNotSent(t *testing.T) {
	const lasting = 600
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	long := &harness.Publish{QoS: 1, Retain: true, Topic: prefix + "/long", Props: expiry(lasting), Payload: []byte("long")}
	sentAt := support.Sent(t, pub, long)
	support.Puback(t, pub, long)
	short := &harness.Publish{QoS: 1, Retain: true, Topic: prefix + "/short", Props: expiry(1), Payload: []byte("short")}
	support.Sent(t, pub, short)
	// The short value expires a second after its receipt, which precedes
	// its PUBACK: the behaviour under test.
	time.Sleep(time.Until(support.Puback(t, pub, short).Add(time.Second)))

	sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(0, prefix+"/#"))
	got := retainedPass(t, sub, map[string][]byte{long.Topic: long.Payload})[long.Topic]
	elapsed := uint32(math.Ceil(time.Since(sentAt).Seconds()))
	switch remaining, ok := messageExpiry(got); {
	case !ok:
		t.Errorf("the retained value expiring in %d seconds came with no Message Expiry Interval", lasting)
	case remaining >= lasting || remaining < lasting-elapsed:
		t.Errorf("Message Expiry Interval %d after waiting at least 1 and at most %d seconds; want %d to %d",
			remaining, elapsed, lasting-elapsed, lasting-1)
	}
	thenLive(t, pub, sub, prefix+"/live")
}

// retainedPeriod is the retained.retention_period of
// TestRetainedRetentionPeriod.
const retainedPeriod = 2 * time.Second

// design.md, Channels, $broadcast, Retained: "A retained value ends only by
// losing its flag to [...] retained.retention_period, measured from the
// publish"; config.md, broker.retained: retained.retention_period, "a
// retained value ends this long after its publish, due as Words: Retention
// says"; Words, Retention: "Age retention becomes due at the earliest
// applicable record, retained-value or tombstone deadline"; "Startup
// processes overdue retention before serving"; Restart, Start: "listeners and
// bridges start last". MQTT 5 3.3.1.3: the Server "MAY choose to discard" a
// retained message, "If this happens there will be no retained message for
// that topic".
//
// With retained.retention_period retainedPeriod, a publisher stores a
// retained value. Past that period from its PUBACK, the broker given late
// to act, a second retained value is stored, and a client subscribes to
// both topics: it receives the second alone, RETAIN 1, and then a message
// published after its SUBSCRIBE. A third retained value is stored, the
// broker stops, and starts again on the same files once that value's
// period has passed. A fourth is stored, and a client subscribes to all
// four topics: it receives the fourth alone, and then a message published
// after its SUBSCRIBE.
func TestRetainedRetentionPeriod(t *testing.T) {
	t.Parallel()
	o := &harness.Options{Dir: t.TempDir(), RetainedRetentionPeriod: retainedPeriod}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	ended := retain("period/ended", []byte("ended"))
	support.Sent(t, pub, ended)
	time.Sleep(time.Until(support.Puback(t, pub, ended).Add(retainedPeriod + support.Late)))
	fresh := retain("period/fresh", []byte("fresh"))
	support.Acknowledged(t, pub, fresh)
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true}, support.At(0, "period/#"))
	retainedPass(t, sub, map[string][]byte{fresh.Topic: fresh.Payload})
	thenLive(t, pub, sub, "period/live")

	down := retain("period/down", []byte("down"))
	support.Sent(t, pub, down)
	downAcked := support.Puback(t, pub, down)
	stopped(t, b)
	// The third value falls due while the broker is stopped: the
	// behaviour under test.
	time.Sleep(time.Until(downAcked.Add(retainedPeriod)))

	b = support.Launch(t, o)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	after := retain("period/after", []byte("after"))
	support.Acknowledged(t, pub, after)
	sub = support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true}, support.At(0, "period/#"))
	retainedPass(t, sub, map[string][]byte{after.Topic: after.Payload})
	thenLive(t, pub, sub, "period/live")
}

// design.md, Channels, $broadcast, Retained: "A retained value ends only by
// losing its flag to [...] retained.retention_period, measured from the
// publish"; Words, Retention: "Age retention becomes due at the earliest
// applicable record, retained-value or tombstone deadline [...] Due work
// follows R2's timer order"; Store, Requests: "Each session, shared group and
// channel keeps at most one request of its own in flight, plus bounded
// pending work"; Execution, Decisions: "Time changes eligibility only at a
// deadline named here"; Flows, SUBSCRIBE, The start point: "The initial
// pass's first read waits until every publication below the boundary is
// stored or refused".
//
// With retained.retention_period retainedPeriod, a publisher stores a
// retained value. The test then holds the database's write lock, as
// another program on the file may, and the publisher sends a QoS 1 message
// on another topic and pings: the PINGRESP, read first, says the core has
// taken it, its batch held at the store. Past the value's period from its
// PUBACK, the expiry falls due behind that batch. A client with no session
// to save subscribes to the value's topic and pings, and the publisher
// sends two more QoS 1 messages, pending behind the held batch, and pings.
// Once the lock is released, the expiry takes its turn after the held
// batch, before the later ones; the subscription's pass reads after the
// held batch: the SUBACK grants QoS 0 and the subscriber receives no
// retained value, only a message published after it.
func TestRetainedValueEndsWhilePublicationsWait(t *testing.T) {
	t.Parallel()
	b := support.Launch(t, &harness.Options{RetainedRetentionPeriod: retainedPeriod})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true})
	ended := retain("waiting/ended", []byte("ended"))
	support.Sent(t, pub, ended)
	acked := support.Puback(t, pub, ended)

	lock := holdWriteLock(t, b.DB)
	held := &harness.Publish{QoS: 1, Topic: "elsewhere", Payload: []byte("held")}
	support.Sent(t, pub, held)
	taken(t, pub, "a QoS 1 PUBLISH, the store's write lock held")
	// The value's period ends while the batch is held: the behaviour under
	// test.
	time.Sleep(time.Until(acked.Add(retainedPeriod + support.Late)))
	s := &harness.Subscribe{ID: sub.NextID(), Subs: support.At(0, "waiting/#")}
	if err := sub.Send(s); err != nil {
		t.Fatal(err)
	}
	// The SUBACK, which confirms no stored change, may come before the
	// PINGRESP, and is kept.
	takenKeeping(t, sub, "a SUBSCRIBE, the store's write lock held")
	later := []*harness.Publish{
		{QoS: 1, Topic: "elsewhere", Payload: []byte("later-1")},
		{QoS: 1, Topic: "elsewhere", Payload: []byte("later-2")},
	}
	for _, p := range later {
		support.Sent(t, pub, p)
	}
	taken(t, pub, "two more QoS 1 PUBLISHes, the store's write lock held")
	lock.release(t)

	for _, p := range append([]*harness.Publish{held}, later...) {
		support.Puback(t, pub, p)
	}
	p, err := sub.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Suback); return ok && a.ID == s.ID })
	if err != nil {
		t.Fatalf("waiting for the SUBACK: %v", err)
	}
	if reasons := p.(*harness.Suback).Reasons; !bytes.Equal(reasons, []byte{0x00}) {
		t.Fatalf("SUBACK % X, want 00", reasons)
	}
	thenLive(t, pub, sub, "waiting/live")
}

// retainedLimit is the retained.max_bytes of TestRetainedMaxBytes: room
// for one of its larger values, whatever a value's charge adds to its
// payload for a short topic, but not for two.
const retainedLimit = 4096

// answered sends p from c at QoS 1 and fails the test unless its PUBACK
// carries reason.
func answered(t *testing.T, c *harness.Client, p *harness.Publish, reason byte) {
	t.Helper()
	support.Sent(t, c, p)
	support.PubackOf(t, c, p, reason)
}

// design.md, Channels, $broadcast, Retained: "Retained values have their own
// size limit. A value that does not fit is refused with 0x97. Where
// retained.max_bytes is set, each topic's charge against it is the largest of
// its committed current value and its accepted pending replacements [...] The
// retained limit counts the sum of those topic charges. A proposed
// replacement is admitted if that sum, with its proposed charge included,
// fits"; "A hard byte limit counts committed charges [...]. A pending
// deletion releases no charge before commit". Flows, PUBLISH: "0x97 | over
// [...] the retained values' limit"; config.md, broker.retained:
// retained.max_bytes, "the retained values' own limit". MQTT 5 3.4.2.1, Table
// 3-4: 0x97, Quota exceeded.
//
// With retained.max_bytes retainedLimit, an MQTT 5 publisher stores a
// retained value of 3,000 bytes on topic x, answered PUBACK 0x00; one of
// 2,000 on topic y, which would take the sum past the limit, gets PUBACK
// 0x97; a replacement of 3,500 on x, charged as the larger of it and x's
// value, 0x00. An empty retained message on x clears its value, 0x00, and
// then the 2,000 bytes on y fit, 0x00. A client subscribing to both
// topics receives y's value alone, and then a message published after its
// SUBSCRIBE.
func TestRetainedMaxBytes(t *testing.T) {
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: retainedLimit})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	sized := func(topic string, size int) *harness.Publish {
		return retain(topic, bytes.Repeat([]byte{'v'}, size))
	}
	answered(t, pub, sized("limit/x", 3000), 0x00)
	answered(t, pub, sized("limit/y", 2000), 0x97)
	answered(t, pub, sized("limit/x", 3500), 0x00)
	answered(t, pub, sized("limit/x", 0), 0x00)
	y := sized("limit/y", 2000)
	answered(t, pub, y, 0x00)

	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true}, support.At(0, "limit/#"))
	retainedPass(t, sub, map[string][]byte{y.Topic: y.Payload})
	thenLive(t, pub, sub, "limit/live")
}

// design.md, Channels, $broadcast, Retained: "A record keeps *published
// retained*, MQTT's RETAIN bit, which never changes. It also keeps *current
// retained value*, which applies to at most one record per topic. A retained
// publish takes the flag. A plain publish leaves it"; "RETAIN is 1 on
// retained values sent to a new subscription. Otherwise it is 0 unless Retain
// As Published"; Flows, SUBSCRIBE, One copy: "A live $broadcast delivery
// preserves RETAIN when any contributor requests Retain As Published".
// storage.md, records: published_retained, "MQTT's RETAIN bit as published,
// never changed"; retained, "1 while it is its topic's current retained
// value"; index records_retained, "at most one current retained value per
// topic". MQTT 5 [MQTT-3.3.1-13]: with Retain As Published, "the Server MUST
// set the RETAIN flag equal to the RETAIN flag in the received PUBLISH
// packet".
//
// A client with Receive Maximum 1 subscribes at QoS 1 with Retain As
// Published. A publisher stores a plain message, two retained values on
// one topic and a plain message on that topic. The client, holding the
// first unacknowledged until all four are stored, receives them one by one
// with their published RETAIN: 0, 1, 1, 0; the first retained value's
// RETAIN 1 though its value was replaced before it was sent. After the
// stop, the database holds the four records with published_retained 0, 1,
// 1 and 0, and retained 1 on the second retained value alone.
func TestRecordKeepsItsPublishedRetain(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true, Props: receiveMaximum5(1)})
	support.Suback(t, sub, nil, []harness.Subscription{{Filter: "kept/#", QoS: 1, RetainAsPublished: true}}, 0x01)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "kept/hold", Payload: []byte("hold")},
		retain("kept/a", []byte("first")),
		retain("kept/a", []byte("second")),
		{QoS: 1, Topic: "kept/a", Payload: []byte("plain")},
	}
	support.PublishAll(t, pub, ps)
	for i, p := range ps {
		got := support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
		if got.Retain != p.Retain {
			t.Errorf("%q, published with RETAIN %v: delivered with RETAIN %v under Retain As Published", p.Payload, p.Retain, got.Retain)
		}
		if i < len(ps)-1 {
			acknowledge(t, sub, got.ID, 0)
		}
	}
	stopped(t, b)

	rs := records(t, b.DB)
	if len(rs) != len(ps) {
		t.Fatalf("the database holds %d records, want the %d published", len(rs), len(ps))
	}
	for i, wantRetained := range []int{0, 0, 1, 0} {
		r, p := rs[i], ps[i]
		wantPublished := 0
		if p.Retain {
			wantPublished = 1
		}
		if r.topic != p.Topic || !bytes.Equal(r.payload, p.Payload) || r.publishedRetained != wantPublished || r.retained != wantRetained {
			t.Errorf("record %d: %q of %q, published_retained %d, retained %d; want %q of %q, published_retained %d, retained %d",
				r.offset, r.topic, r.payload, r.publishedRetained, r.retained, p.Topic, p.Payload, wantPublished, wantRetained)
		}
	}
}

// design.md, R7: "Every packet follows MQTT 3.1.1 and 5"; "The standards
// are kept in docs/mqtt, as 3.1.1 Plus Errata 01 and 5.0". MQTT 3.1.1
// 3.3.1.3: "It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent
// to a Client because it matches an established subscription regardless
// of how the flag was set in the message it received [MQTT-3.3.1-9]".
// Retain As Published is an MQTT 5 subscription option (MQTT 5 3.8.3.1);
// MQTT 3.1.1 has none, so a packet sent on an MQTT 3.1.1 connection keeps
// 3.1.1's rule whatever option the session's subscription was made with.
//
// A client holding a durable session subscribes over MQTT 5, at QoS 1,
// with Retain As Published, and goes away; it comes back to the same
// session over MQTT 3.1.1 with Clean Session 0. Only then does a publisher
// send a retained message on a topic its filter matches and that held no
// retained value, so nothing reaches it as a new subscription's retained
// value: the message reaches it live, through the established
// subscription, with RETAIN 0.
func TestMQTT311ResumeGetsRetainZeroDespiteRetainAsPublished(t *testing.T) {
	prefix := clientID(t, "")
	client := clientID(t, "-sub")
	support.Away(t, support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable},
		[]harness.Subscription{{Filter: prefix + "/#", QoS: 1, RetainAsPublished: true}}))
	sub := resumed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: client})

	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	p := retain(prefix+"/a", []byte("live"))
	support.Acknowledged(t, pub, p)
	liveAt(t, sub, 1, p.Topic, p.Payload)
}

// heldAsPublished connects a client of its own to addr over MQTT 5 with
// connect, subscribes it to filter at QoS 1 with Retain As Published, has
// pub store p, a retained QoS 1 message on a topic filter matches that
// holds no retained value yet, and reads its live delivery: QoS 1, DUP 0
// and RETAIN 1, MQTT 5 [MQTT-3.3.1-13]: "If the value of Retain As
// Published subscription option is set to 1, the Server MUST set the
// RETAIN flag equal to the RETAIN flag in the received PUBLISH packet";
// design.md, Flows, SUBSCRIBE, One copy: "A live $broadcast delivery
// preserves RETAIN when any contributor requests Retain As Published". It
// returns the client and the delivery, unacknowledged.
func heldAsPublished(t *testing.T, addr string, connect *harness.Connect, filter string, pub *harness.Client, p *harness.Publish) (*harness.Client, *harness.Publish) {
	t.Helper()
	sub := support.Subscribed(t, addr, harness.V5, connect, []harness.Subscription{{Filter: filter, QoS: 1, RetainAsPublished: true}})
	support.Acknowledged(t, pub, p)
	first := support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
	if !first.Retain {
		t.Fatalf("the live delivery of %q, published retained, came with RETAIN 0 under Retain As Published; want 1 [MQTT-3.3.1-13]", p.Topic)
	}
	return sub, first
}

// resentLive reads c's next packet, on an MQTT 3.1.1 connection, and
// fails the test unless it is the resend of first, a live delivery
// (redelivered), with RETAIN 0: design.md, Delivery, A resend: "The
// first-delivery RETAIN value is preserved, but an MQTT 3.1.1 connection
// receives RETAIN 1 only on a delivery of a subscription's initial pass
// (SUBSCRIBE, The start point; MQTT-3.3.1-8, MQTT-3.3.1-9)"; MQTT 3.1.1
// 3.3.1.3: "It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent
// to a Client because it matches an established subscription regardless
// of how the flag was set in the message it received [MQTT-3.3.1-9]".
func resentLive(t *testing.T, c *harness.Client, first *harness.Publish) {
	t.Helper()
	if p := redelivered(t, c, first); p.Retain {
		t.Fatalf("the MQTT 3.1.1 resend of the live delivery %q came with RETAIN 1; want 0, no part of an initial pass [MQTT-3.3.1-9]", p.Payload)
	}
}

// design.md, Delivery, A resend: "The first-delivery RETAIN value is
// preserved, but an MQTT 3.1.1 connection receives RETAIN 1 only on a
// delivery of a subscription's initial pass (SUBSCRIBE, The start point;
// MQTT-3.3.1-8, MQTT-3.3.1-9)"; Flows, CONNECT, 9. Resume: "in-flight
// messages [...] resends them with their packet identifiers [...] Resent
// messages have DUP set". MQTT 3.1.1 3.3.1.3: "It MUST set the RETAIN flag to
// 0 when a PUBLISH Packet is sent to a Client because it matches an
// established subscription regardless of how the flag was set in the message
// it received [MQTT-3.3.1-9]"; 4.4: "When a Client reconnects with
// CleanSession set to 0, both the Client and Server MUST re-send any
// unacknowledged PUBLISH Packets (where QoS > 0) and PUBREL Packets using
// their original Packet Identifiers [MQTT-4.4.0-1]".
//
// A client holding a durable session subscribes over MQTT 5, at QoS 1,
// with Retain As Published. A publisher then stores a retained message on
// a topic its filter matches and that held no retained value: the client
// receives it live with RETAIN 1, as MQTT 5 has it, and leaves it
// unacknowledged. It sends DISCONNECT and comes back to the same session
// over MQTT 3.1.1 with Clean Session 0: the message comes again, DUP 1,
// under its identifier, with RETAIN 0.
func TestMQTT311ResendGetsRetainZeroDespiteRetainAsPublished(t *testing.T) {
	prefix := clientID(t, "")
	connect := &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: support.Durable}
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	sub, first := heldAsPublished(t, brokerAddr, connect, prefix+"/#", pub, retain(prefix+"/a", []byte("live")))
	support.Away(t, sub)

	sub = resumed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: connect.ClientID})
	resentLive(t, sub, first)
}

// design.md, Delivery, A resend: "The first-delivery RETAIN value is
// preserved, but an MQTT 3.1.1 connection receives RETAIN 1 only on a
// delivery of a subscription's initial pass (SUBSCRIBE, The start point;
// MQTT-3.3.1-8, MQTT-3.3.1-9)". storage.md, session_inflight: out_retain,
// the first delivery's RETAIN value. Flows, CONNECT, 5. Take over, and 9.
// Resume: "in-flight messages [...] resends them with their packet
// identifiers [...] Resent messages have DUP set". MQTT 3.1.1 [MQTT-3.3.1-8]:
// RETAIN 1 "if a message is sent as a result of a new subscription being made
// by a Client"; [MQTT-3.3.1-9]: RETAIN 0 "when a PUBLISH Packet is sent to a
// Client because it matches an established subscription". MQTT 5
// [MQTT-3.3.1-13]: under Retain As Published, "the Server MUST set the RETAIN
// flag equal to the RETAIN flag in the received PUBLISH packet".
//
// Each case's first delivery goes over MQTT 3.1.1, at QoS 1, to a durable
// session, and is left unacknowledged; then an MQTT 5 connection with
// Clean Start 0 takes the session over, and the message comes again, DUP
// 1, under its identifier, with the first delivery's RETAIN:
//   - a retained value stored before the client subscribes over MQTT
//     3.1.1, Clean Session 0: its initial pass sends it with RETAIN 1, and
//     the resend has RETAIN 1;
//   - a subscription made over MQTT 5 with Retain As Published, the session
//     then resumed over MQTT 3.1.1, Clean Session 0: a message published
//     retained after it comes live with RETAIN 0, and the resend has RETAIN
//     0, though the MQTT 5 connection's Retain As Published would give 1.
func TestResendToMQTT5KeepsTheMQTT311Retain(t *testing.T) {
	t.Run("an initial pass's value, RETAIN 1", func(t *testing.T) {
		prefix := clientID(t, "")
		client := clientID(t, "-sub")
		pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		p := retain(prefix+"/a", []byte("kept"))
		support.Acknowledged(t, pub, p)
		sub := support.Subscribed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: client}, support.At(1, prefix+"/#"))
		first := support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
		if !first.Retain {
			t.Fatalf("the MQTT 3.1.1 initial pass sent %q with RETAIN 0; want 1 [MQTT-3.3.1-8]", p.Topic)
		}

		sub = resumed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: client, Props: support.Durable})
		if resend := redelivered(t, sub, first); !resend.Retain {
			t.Errorf("the MQTT 5 resend of the initial pass's %q came with RETAIN 0; want 1, the first delivery's", p.Topic)
		}
	})
	t.Run("a live delivery, RETAIN 0", func(t *testing.T) {
		prefix := clientID(t, "")
		client := clientID(t, "-sub")
		support.Away(t, support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable},
			[]harness.Subscription{{Filter: prefix + "/#", QoS: 1, RetainAsPublished: true}}))
		sub := resumed(t, brokerAddr, harness.V311, &harness.Connect{ClientID: client})
		pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		p := retain(prefix+"/a", []byte("live"))
		support.Acknowledged(t, pub, p)
		first := liveAt(t, sub, 1, p.Topic, p.Payload)

		sub = resumed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: client, Props: support.Durable})
		if resend := redelivered(t, sub, first); resend.Retain {
			t.Errorf("the MQTT 5 resend of the live %q came with RETAIN 1, recomputed under Retain As Published; want 0, the first delivery's", p.Topic)
		}
	})
}

// design.md, Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records, except that a
// durable session may also pass records represented by stored QoS 1 or QoS
// 2 exchanges. Saving progress past an unsettled exchange marks it moves =
// 0 in the same transaction; its acknowledgement then settles only that
// exchange"; "QoS 0 settles on successful writing". A resend: "The
// first-delivery RETAIN value is preserved, but an MQTT 3.1.1 connection
// receives RETAIN 1 only on a delivery of a subscription's initial pass
// (SUBSCRIBE, The start point; MQTT-3.3.1-8, MQTT-3.3.1-9)". storage.md,
// session_inflight: moves, "1 while the row protects or may move the current
// position; 0 after a seek or saved progress supersedes it";
// out_initial, "1 when the first delivery belonged to a subscription's
// initial pass (SUBSCRIBE, The start point): a value sent as a result of a
// new subscription; else 0". MQTT 3.1.1 [MQTT-3.3.1-9]: RETAIN 0 "when a
// PUBLISH Packet is sent to a Client because it matches an established
// subscription regardless of how the flag was set in the message it
// received". MQTT 5 [MQTT-4.6.0-2]: "The Client MUST send PUBACK packets in
// the order in which the corresponding PUBLISH packets were received (QoS
// 1 messages)", so what follows an unacknowledged QoS 1 delivery settles
// only at QoS 0.
//
// A client holding a durable session subscribes over MQTT 5, at QoS 1,
// with Retain As Published. A publisher stores a retained message, which
// the client receives live with RETAIN 1 and leaves unacknowledged, then a
// plain message at QoS 0, which settles as it is written. The client sends
// DISCONNECT and comes back to the same session over MQTT 3.1.1 with Clean
// Session 0. The database then holds the position past both records and
// the first's row alone, moves 0: its movement superseded, as an initial
// pass's never moves. The first comes again, DUP 1, under its identifier,
// with RETAIN 0: a live delivery still, not one of an initial pass.
func TestSupersededExchangeIsNotResentAsInitialPass(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	const client = "superseded"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "superseded-pub", CleanStart: true})
	sub, first := heldAsPublished(t, b.Addr, connect, "superseded/#", pub, retain("superseded/a", []byte("held")))
	settled := &harness.Publish{QoS: 0, Topic: "superseded/b", Payload: []byte("settled")}
	if err := pub.Send(settled); err != nil {
		t.Fatalf("PUBLISH %q at QoS 0: %v", settled.Topic, err)
	}
	delivered(t, sub, settled.Topic, settled.Payload)
	support.Away(t, sub)

	sub = resumed(t, b.Addr, harness.V311, &harness.Connect{ClientID: client})
	// The resumed CONNACK follows the session's load, a read whose view
	// holds every write the departure made (design.md, Store, Store reads).
	position, rows := passing(t, support.OpenDB(t, b.DB), client, "once resumed")
	offsets := offsetsByPayload(t, b.DB)
	if held := offsets[string(first.Payload)]; position <= offsets[string(settled.Payload)] || len(rows) != 1 || rows[0].offset != held {
		t.Fatalf("once resumed, the position is %d and session_inflight holds %+v; want the position past %d, the settled record, and the row of %d alone, its movement superseded",
			position, rows, offsets[string(settled.Payload)], held)
	}
	resentLive(t, sub, first)
}

// passValues is how many retained values make the passes held by
// heldPass: past the pages the core may have read while the pass holds
// its first value, so that the last, by offset and by topic, is read only
// once the pass moves on. design.md, Execution, Capacities: "metadata pages
// hold 256 rows"; Resource bounds: "at most one page per store reader";
// config.md, broker.storage: read_connections 2 by default.
const passValues = 600 // more than two pages of 256

// publishBurst is how many QoS 1 PUBLISHes a client sends before it waits
// for their PUBACKs: config.md, qos2.max_inflight_per_client, "the Receive
// Maximum sent in CONNACK", 20 by default.
const publishBurst = 20

// manyRetained has pub store passValues retained values at QoS 1, on
// topics under prefix whose offsets and bytes sort alike (prefix/old/000
// onwards), within the Receive Maximum, and returns them by topic.
func manyRetained(t *testing.T, pub *harness.Client, prefix string) map[string][]byte {
	t.Helper()
	values := make(map[string][]byte, passValues)
	ps := make([]*harness.Publish, passValues)
	for i := range ps {
		topic := fmt.Sprintf("%s/old/%03d", prefix, i)
		values[topic] = []byte("value of " + topic)
		ps[i] = retain(topic, values[topic])
	}
	for burst := range slices.Chunk(ps, publishBurst) {
		support.PublishAll(t, pub, burst)
	}
	return values
}

// heldPass connects to addr with connect, which asks for Receive Maximum
// 1, subscribes at QoS 1 to prefix/#, and reads the first value of its
// retained pass, one of values, at QoS 1 with RETAIN 1, which it removes
// from values and returns unacknowledged, with the client. The pass then
// sends nothing more until its PUBACK: design.md, Delivery, One mechanism:
// "The window is the lower of the client's Receive Maximum and
// limits.max_inflight"; MQTT 5 [MQTT-4.9.0-2]: at a send quota of zero the
// Server "MUST NOT send any more PUBLISH packets with QoS > 0".
func heldPass(t *testing.T, addr string, connect *harness.Connect, prefix string, values map[string][]byte) (*harness.Client, *harness.Publish) {
	t.Helper()
	c, _ := harness.Connected(t, addr, harness.V5, connect)
	support.Suback(t, c, nil, support.At(1, prefix+"/#"), 0x01)
	p := harness.Expect[*harness.Publish](t, c)
	if want, ok := values[p.Topic]; !ok || !bytes.Equal(p.Payload, want) || p.QoS != 1 || !p.Retain {
		t.Fatalf("PUBLISH %q of %.40q at QoS %d, RETAIN %v; want the first value of the retained pass, at QoS 1, RETAIN 1",
			p.Topic, p.Payload, p.QoS, p.Retain)
	}
	delete(values, p.Topic)
	return c, p
}

// design.md, Flows, SUBSCRIBE, The start point: "The core takes that
// channel's next offset when it takes the SUBSCRIBE, in the connection's
// packet order. Every publication that entered before the SUBSCRIBE therefore
// falls below the boundary"; "The initial pass's first read waits until every
// publication below the boundary is stored or refused; the wait holds nothing
// back, and nothing waits for it"; Channels, $broadcast, Retained: with
// retained.max_bytes set, "A proposed replacement is admitted if that sum,
// with its proposed charge included, fits"; the subscription table:
// "$broadcast | the matching retained values, as Retain Handling allows, then
// the next message"; Channels, $broadcast: "RETAIN is 1 on retained values
// sent to a new subscription. Otherwise it is 0". MQTT 5 3.3.1.3: retained
// messages are sent "At the time of the Subscription".
//
// With retained.max_bytes retainedLimit, the test holds the database's
// write lock, as another program on the file may. A publisher sends at QoS
// 1 a message on another topic, whose batch goes to the store and is held
// there, and a retained message, whose admission waits behind that batch,
// so its write is not yet accepted; then it pings: the PINGRESP,
// read first, says the core has taken both, their PUBACKs waiting for the
// store. A client with no session to save subscribes to a filter
// matching it and pings: the PINGRESP says the core has taken the
// SUBSCRIBE while the value is still being stored. Once the lock is
// released, the PUBACKs are 0x00, the SUBACK grants QoS 0, and the
// subscriber receives the value once, RETAIN 1, and then a message
// published after it, nothing between.
func TestValueBeingStoredAtTheSubscribeArrivesOnceAsRetained(t *testing.T) {
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: retainedLimit})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true})
	lock := holdWriteLock(t, b.DB)
	first := &harness.Publish{QoS: 1, Topic: "elsewhere", Payload: []byte("held")}
	value := retain("storing/value", []byte("being stored"))
	support.Sent(t, pub, first)
	support.Sent(t, pub, value)
	taken(t, pub, "two PUBLISHes, the store's write lock held")
	s := &harness.Subscribe{ID: sub.NextID(), Subs: support.At(0, "storing/#")}
	if err := sub.Send(s); err != nil {
		t.Fatal(err)
	}
	// The SUBACK, which confirms no stored change, may come before the
	// PINGRESP, and is kept.
	takenKeeping(t, sub, "a SUBSCRIBE, the store's write lock held")
	lock.release(t)

	support.Puback(t, pub, first)
	support.Puback(t, pub, value)
	p, err := sub.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Suback); return ok && a.ID == s.ID })
	if err != nil {
		t.Fatalf("waiting for the SUBACK: %v", err)
	}
	if reasons := p.(*harness.Suback).Reasons; !bytes.Equal(reasons, []byte{0x00}) {
		t.Fatalf("SUBACK % X, want 00", reasons)
	}
	retainedPass(t, sub, map[string][]byte{value.Topic: value.Payload})
	thenLive(t, pub, sub, "storing/live")
}

// design.md, Flows, SUBSCRIBE, The start point: "The core captures that
// channel's tail when it accepts the subscription"; "The captured boundary
// separates the initial pass, which reads retained or current values below
// it, from subsequent live records"; "An initial pass reads in pages; each
// page sends the values current when it is read, below the tail at the cut";
// the subscription table: "the matching retained values, as Retain Handling
// allows, then the next message". MQTT 5 3.3.1.3: retained messages are sent
// "At the time of the Subscription", and [MQTT-3.3.1-12], RETAIN 0 on the
// forwarded message.
//
// A publisher stores passValues retained values under a filter. A client
// with Receive Maximum 1 subscribes to it at QoS 1 and holds the first
// value of its pass unacknowledged while a new retained value is stored
// under the filter, on a topic of its own. The client then acknowledges
// each value as it comes: it receives every other value, RETAIN 1, and
// then the new value, once, RETAIN 0.
func TestValuePublishedJustAfterTheSubscribeArrivesOnce(t *testing.T) {
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	values := manyRetained(t, pub, prefix)
	sub, held := heldPass(t, brokerAddr, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: receiveMaximum5(1)}, prefix, values)

	after := retain(prefix+"/new", []byte("just after"))
	support.Acknowledged(t, pub, after)
	acknowledge(t, sub, held.ID, 0)
	retainedPassAt(t, sub, 1, values)
	liveAt(t, sub, 1, after.Topic, after.Payload)
}

// design.md, Flows, SUBSCRIBE, The start point: "An initial pass reads in
// pages; each page sends the values current when it is read, below the
// tail at the cut. A value whose flag ends during the pass is not sent:
// its replacement or clearing record reaches the subscriber live";
// Channels, $broadcast, Retained: "A retained value ends only by losing
// its flag: to a newer retained publish". MQTT 5 [MQTT-3.3.1-5]: a
// retained publish "MUST replace any existing retained message for this
// topic"; [MQTT-3.3.1-12], RETAIN 0 on the forwarded message.
//
// A publisher stores passValues retained values under a filter. A client
// with Receive Maximum 1 subscribes to it at QoS 1 and holds the first
// value of its pass unacknowledged, so the pass has not read the value
// last by offset and by topic, which is then replaced. The client then
// acknowledges each value as it comes: it receives every other value,
// RETAIN 1, then the replacement, once, RETAIN 0, and then a message
// published after it: the replaced value never.
func TestValueReplacedDuringThePassArrivesOnlyLive(t *testing.T) {
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	values := manyRetained(t, pub, prefix)
	sub, held := heldPass(t, brokerAddr, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: receiveMaximum5(1)}, prefix, values)
	replacement := retain(fmt.Sprintf("%s/old/%03d", prefix, passValues-1), []byte("replacement"))
	if held.Topic == replacement.Topic {
		t.Fatalf("the pass began with %q, last by offset and by topic; the test needs it not yet read", held.Topic)
	}

	support.Acknowledged(t, pub, replacement)
	delete(values, replacement.Topic)
	acknowledge(t, sub, held.ID, 0)
	retainedPassAt(t, sub, 1, values)
	acknowledge(t, sub, liveAt(t, sub, 1, replacement.Topic, replacement.Payload).ID, 0)
	thenLiveAt(t, pub, sub, 1, prefix+"/live")
}

// design.md, Flows, SUBSCRIBE, The start point: "A pass cut by a restart
// is not resumed"; the live-start boundary is stored "with a durable
// session's subscription"; Restart: "the broker restores durable sessions
// with their subscriptions, positions, in-flight windows"; Delivery,
// Positions: "Before handing a QoS 1 or QoS 2 delivery to a durable session's
// writer, the core stores its packet identifier"; Store, full mode:
// "Everything acknowledged survives a crash". MQTT 5 [MQTT-4.4.0-1]:
// unacknowledged PUBLISH packets are resent, "using their original Packet
// Identifiers".
//
// A publisher stores three retained values. A client holding a durable
// session, with Receive Maximum 1, subscribes to them at QoS 1 and holds
// the first value of its pass unacknowledged: the other two are not yet
// sent. The broker is killed and started again on the same files, and the
// client resumes its session: it receives the held value again, DUP 1,
// and, once it acknowledges it, a message published after it resumed.
func TestRetainedPassCutByARestartIsNotResumed(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir()}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	values := map[string][]byte{}
	for i := range 3 {
		p := retain(fmt.Sprintf("cut/%d", i), fmt.Appendf(nil, "value %d", i))
		support.Acknowledged(t, pub, p)
		values[p.Topic] = p.Payload
	}
	connect := &harness.Connect{ClientID: "sub", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(1))}
	_, held := heldPass(t, b.Addr, connect, "cut", values)
	support.Crash(t, b)

	b = support.Launch(t, o)
	sub := resumed(t, b.Addr, harness.V5, connect)
	acknowledge(t, sub, redelivered(t, sub, held).ID, 0)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	thenLiveAt(t, pub, sub, 1, "cut/live")
}

// replySlots is a connection's reply-slot capacity: design.md, Execution,
// Capacities, "Each connection has 64 reply slots and 64 answer
// allowances".
const replySlots = 64

// design.md, Flows, Replies: "For a packet needing a reply, a reader first
// takes a reply slot from its own writer. The slot is returned once the reply
// is written"; "A reply becomes ready when the core processes the event
// establishing its result"; Delivery, Positions: "an acknowledgement (PUBACK,
// or PUBCOMP) settles its own delivery". MQTT 5 [MQTT-4.9.0-3]: "The Client
// and Server MUST continue to process and respond to all other MQTT Control
// Packets even if the quota is zero"; [MQTT-3.8.4-1]: "When the Server
// receives a SUBSCRIBE packet from a Client, the Server MUST respond with a
// SUBACK packet".
//
// A publisher stores two retained values. A client with Receive Maximum 1
// subscribes to them at QoS 1 and holds the first value of its pass
// unacknowledged, its send quota zero. It then sends more SUBSCRIBEs than
// it has reply slots, each of a filter of its own matching nothing, and
// receives each one's SUBACK 0x00; then it acknowledges the held value,
// and receives the other.
func TestSubscribesAreAnsweredWhileAPassWaits(t *testing.T) {
	prefix := clientID(t, "")
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	values := map[string][]byte{prefix + "/a": []byte("a"), prefix + "/b": []byte("b")}
	support.PublishAll(t, pub, []*harness.Publish{retain(prefix+"/a", values[prefix+"/a"]), retain(prefix+"/b", values[prefix+"/b"])})
	sub, held := heldPass(t, brokerAddr, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: receiveMaximum5(1)}, prefix, values)

	ids := make([]uint16, replySlots+6)
	for i := range ids {
		ids[i] = sub.NextID()
		if err := sub.Send(&harness.Subscribe{ID: ids[i], Subs: support.At(0, fmt.Sprintf("%s-other/%d", prefix, i))}); err != nil {
			t.Fatalf("SUBSCRIBE %d: %v", i+1, err)
		}
	}
	for i, id := range ids {
		p, err := sub.Await(func(p harness.Packet) bool {
			a, ok := p.(*harness.Suback)
			return ok && a.ID == id
		})
		if err != nil {
			t.Fatalf("SUBSCRIBE %d of %d, sent while the pass waits for a PUBACK: waiting for its SUBACK: %v", i+1, len(ids), err)
		}
		if got := p.(*harness.Suback).Reasons; !bytes.Equal(got, []byte{0x00}) {
			t.Fatalf("SUBSCRIBE %d: SUBACK % X, want 00", i+1, got)
		}
	}
	acknowledge(t, sub, held.ID, 0)
	retainedPassAt(t, sub, 1, values)
}

// design.md, Channels, $broadcast, Retained: "Where retained.max_bytes is
// set, each topic's charge against it is the largest of its committed
// current value and its accepted pending replacements [...]. The retained
// limit counts the sum of those topic charges. A proposed replacement is
// admitted if that sum, with its proposed charge included, fits"; Flows,
// PUBLISH: "0x97 | over [...] the retained values' limit". R2: the core
// "sends a request and handles the answer as a later event"; Store, Store
// reads: "Every other read used for a broker decision has a place in the
// core's store-request order. Earlier accepted writes are resolved before its
// view is fixed"; Flows, Replies: SUBACK and PUBACK "sent only once the
// store confirms".
//
// With retained.max_bytes retainedLimit, the test holds the database's
// write lock, as another program on the file may. A client holding a
// durable session sends, at once, a SUBSCRIBE, whose change waits at the
// store; a retained message of 2,000 bytes, whose admission waits for that
// change; a retained message of 3,000 bytes on another topic; and a
// PINGREQ, whose PINGRESP, read first, says the core has taken them all.
// Once the lock is released, the SUBACK is 0x00, the first PUBACK 0x00,
// and the second 0x97, the two values not fitting together. A client
// subscribing to both topics receives the first value alone, and then a
// message published after its SUBSCRIBE.
func TestPublicationsWhileARetainedAdmissionWaits(t *testing.T) {
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: retainedLimit})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "durable", CleanStart: true, Props: support.Durable})
	lock := holdWriteLock(t, b.DB)
	s := &harness.Subscribe{ID: c.NextID(), Subs: []harness.Subscription{{Filter: "elsewhere", RetainHandling: 2}}}
	if err := c.Send(s); err != nil {
		t.Fatal(err)
	}
	fits := retain("admission/a", bytes.Repeat([]byte{'v'}, 2000))
	over := retain("admission/b", bytes.Repeat([]byte{'v'}, 3000))
	support.Sent(t, c, fits)
	support.Sent(t, c, over)
	taken(t, c, "a SUBSCRIBE and two retained PUBLISHes, the store's write lock held")
	lock.release(t)

	if _, err := c.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Suback); return ok && a.ID == s.ID }); err != nil {
		t.Fatalf("waiting for the SUBACK: %v", err)
	}
	for _, tc := range []struct {
		p      *harness.Publish
		reason byte
	}{{fits, 0x00}, {over, 0x97}} {
		ack, err := c.AwaitAck(harness.PUBACK, tc.p.ID)
		switch {
		case err != nil:
			t.Fatalf("PUBLISH %q of %d bytes: %v", tc.p.Topic, len(tc.p.Payload), err)
		case ack.Reason != tc.reason:
			t.Errorf("PUBLISH %q of %d bytes: PUBACK 0x%02X, want 0x%02X", tc.p.Topic, len(tc.p.Payload), ack.Reason, tc.reason)
		}
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true}, support.At(0, "admission/#"))
	retainedPass(t, sub, map[string][]byte{fits.Topic: fits.Payload})
	thenLive(t, pub, sub, "admission/live")
}

// design.md, Channels, $broadcast, Retained: "A retained publish takes the
// flag; [...] an empty retained publish clears it"; DISCONNECT, A lost
// connection: the Will goes "at once for MQTT 3.1.1", and with no Will Delay
// at once in MQTT 5. MQTT 5 [MQTT-3.1.2-15]: "If the Will Flag is set to 1
// and Will Retain is set to 1, the Server MUST publish the Will Message as a
// retained message"; MQTT 3.1.1 [MQTT-3.1.2-17]. MQTT 5 [MQTT-3.3.1-6] and
// 3.1.1 [MQTT-3.3.1-10]: a retained message of zero bytes removes any
// retained message on its topic.
//
// For each version, a client of a session ending with its connection
// connects with a retained Will and drops its socket; a watcher subscribed
// before receives the Will live, RETAIN 0, and a new subscription then
// receives it as the topic's retained value, RETAIN 1. Another client
// does the same with an empty retained Will: the watcher receives it, and
// a new subscription receives no retained value, only a message published
// after it.
func TestRetainedWillSetsAndClearsTheRetainedValue(t *testing.T) {
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			prefix := clientID(t, "")
			topic := prefix + "/will"
			pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			watch := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-watch"), CleanStart: true}, support.At(0, prefix+"/#"))
			// fire has the client suffix names publish a retained Will of
			// payload by dropping its socket, and waits until the watcher
			// receives it.
			fire := func(suffix string, payload []byte) {
				c, _ := harness.Connected(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, suffix), CleanStart: true,
					Will: &harness.Will{Retain: true, Topic: topic, Payload: payload}})
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				live(t, watch, topic, payload)
			}
			fire("-sets", support.WillPayload)
			first := support.Subscribed(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, "-first"), CleanStart: true}, support.At(0, prefix+"/#"))
			retainedPass(t, first, map[string][]byte{topic: support.WillPayload})
			fire("-clears", nil)
			second := support.Subscribed(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, "-second"), CleanStart: true}, support.At(0, prefix+"/#"))
			thenLive(t, pub, second, prefix+"/live")
		})
	}
}

// design.md, DISCONNECT, A lost connection: the Will "is dropped and
// counted if [...] its channel or the retained values' limit is full; on a
// full disk it stays in the store"; Channels, $broadcast, Retained: "The
// retained limit counts the sum of those topic charges. A proposed
// replacement is admitted if that sum, with its proposed charge included,
// fits"; Flows, CONNECT: only a retained Will "that alone exceeds
// retained.max_bytes" is refused; storage.md: "A message's bytes are its
// topic's, payload's and properties' bytes".
//
// With retained.max_bytes 100, a client connects with a retained Will of
// 49 bytes, which the database then holds; a publisher stores a retained
// value of 69 bytes on another topic, which a watcher receives. The client
// drops its socket: the Will, 118 bytes with that value, leaves the
// database, and is never sent: a new subscription receives the value alone
// as retained, and a message published next is the next packet of the
// watcher and of the new subscriber. Once the value is cleared, the same
// Will from another client fits, and the watcher receives it.
func TestRetainedWillOverTheLimitAtFiringIsDropped(t *testing.T) {
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: 100})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pub", CleanStart: true})
	watch := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "watch", CleanStart: true}, support.At(0, "drop/#"))
	will := &harness.Will{Retain: true, Topic: "drop/will", Payload: bytes.Repeat([]byte{'w'}, 40)}
	fill := retain("drop/fill", bytes.Repeat([]byte{'f'}, 60))

	dropped, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "dropped", CleanStart: true, Will: will})
	if _, ok := support.StoredWill(t, db, "dropped"); !ok {
		t.Fatal("after the CONNACK the database holds no Will for the client: there is nothing to fire")
	}
	answered(t, pub, fill, 0x00)
	live(t, watch, fill.Topic, fill.Payload)
	if err := dropped.Close(); err != nil {
		t.Fatal(err)
	}
	awaitWillGone(t, db, "dropped", "its socket dropped")
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sub", CleanStart: true}, support.At(0, "drop/#"))
	retainedPass(t, sub, map[string][]byte{fill.Topic: fill.Payload})
	next := &harness.Publish{QoS: 1, Topic: "drop/live", Payload: []byte("live")}
	support.Acknowledged(t, pub, next)
	for _, c := range []*harness.Client{watch, sub} {
		live(t, c, next.Topic, next.Payload)
	}

	cleared := retain(fill.Topic, nil)
	answered(t, pub, cleared, 0x00)
	live(t, watch, cleared.Topic, nil)
	fits, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "fits", CleanStart: true, Will: will})
	if err := fits.Close(); err != nil {
		t.Fatal(err)
	}
	live(t, watch, will.Topic, will.Payload)
}

// design.md, Channels: "A retain flag on a configured channel topic is
// ignored (MQTT-3.3.1-5)"; $broadcast: "Retained is a flag [...] A
// retained publish takes the flag"; "Retained values have their own size
// limit. A value that does not fit is refused with 0x97"; storage.md,
// records: published_retained and retained, "$broadcast only, 0 elsewhere";
// A stored payload: payload_size, "the payload's size as received"; Flows,
// PUBLISH: "1 | on PUBLISH | PUBACK once stored".
//
// With retained.max_bytes retainedLimit and an append channel, plain
// (plain/#), an MQTT 5 client publishes at QoS 1 with RETAIN 1: on
// plain/big, a payload larger than the retained limit, then an empty one,
// each answered 0x00, neither a retained value; an MQTT 3.1.1 client does
// so on plain/old, answered; and the MQTT 5 client on unclaimed/value,
// which no channel claims, answered 0x00. After the stop, plain holds the
// three as plain records, each with its payload_size as published, the
// empty one's 0, and published_retained and retained 0; $broadcast holds the last as its
// topic's retained value, both flags 1.
func TestRetainOnAConfiguredChannelIsIgnored(t *testing.T) {
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: retainedLimit, Channels: []harness.Channel{{Name: "plain", Type: "append", Filter: "plain/#"}}})
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "retain-5", CleanStart: true})
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "retain-3.1.1", CleanStart: true})
	answered(t, v5, retain("plain/big", bytes.Repeat([]byte{'v'}, retainedLimit+1)), 0x00)
	answered(t, v5, retain("plain/big", nil), 0x00)
	answered(t, v311, retain("plain/old", []byte("3.1.1")), 0x00)
	answered(t, v5, retain("unclaimed/value", []byte("kept")), 0x00)
	stopped(t, b)

	rs := records(t, b.DB)
	want := []struct {
		channel, topic string
		size, flags    int // payload_size, the payload's size as received; published_retained and retained
	}{
		{"$broadcast", "unclaimed/value", 4, 1},
		{"plain", "plain/big", retainedLimit + 1, 0},
		{"plain", "plain/big", 0, 0},
		{"plain", "plain/old", 5, 0},
	}
	if len(rs) != len(want) {
		t.Fatalf("the database holds %d records, want the %d published: %+v", len(rs), len(want), rs)
	}
	for i, w := range want {
		r := rs[i]
		if r.name != w.channel || r.topic != w.topic || r.payloadSize != w.size || r.publishedRetained != w.flags || r.retained != w.flags {
			t.Errorf("record %d: %q in %q, payload_size %d, published_retained %d, retained %d; want %q in %q, payload_size %d, both flags %d",
				i+1, r.topic, r.name, r.payloadSize, r.publishedRetained, r.retained, w.topic, w.channel, w.size, w.flags)
		}
	}
}
