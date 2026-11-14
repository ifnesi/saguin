package conformance

import (
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// The tests of one copy for overlapping subscriptions: which of a
// session's subscriptions contribute to a record's one delivery, and what
// each contributes to it (design.md, Flows, SUBSCRIBE, One copy and
// Options).

// subscriptionIDs returns the Subscription Identifiers p carries, in the
// order it carries them.
func subscriptionIDs(p *harness.Publish) []uint32 {
	var ids []uint32
	for _, prop := range p.Props {
		if prop.ID == harness.SubscriptionIdentifier {
			ids = append(ids, prop.Int)
		}
	}
	return ids
}

// expectIDs fails the test unless p carries the Subscription Identifiers
// want, in that order.
func expectIDs(t *testing.T, p *harness.Publish, want ...uint32) {
	t.Helper()
	if got := subscriptionIDs(p); !slices.Equal(got, want) {
		t.Errorf("the delivery of %q carries Subscription Identifiers %v; want %v", p.Topic, got, want)
	}
}

// design.md, Flows, SUBSCRIBE, One copy: "a record matching several of a
// session's non-shared subscriptions is sent once"; "The delivery uses the
// lower of the publication's QoS and the highest contributing grant".
// Channels, $broadcast: QoS "caps the QoS it is delivered at". MQTT 5
// 3.3.4: "the Server MUST deliver the message to the Client respecting the
// maximum QoS of all the matching subscriptions [MQTT-3.3.4-2]"; 3.8.4:
// "the QoS of the message delivered [is] the minimum of the QoS of the
// originally published message and the Maximum QoS granted".
//
// A client subscribes to three filters a topic matches, granted QoS 0, 1
// and 0, and of which the first alone matches a topic one level deeper. A
// publisher sends, in order, a QoS 1 message on the first topic, which
// arrives at QoS 1, the highest grant; a QoS 0 message there, which
// arrives at QoS 0, the publication's; and a QoS 1 message on the deeper
// topic, which arrives at QoS 0, the one grant that contributes.
func TestDeliveryQoSIsTheHighestContributingGrant(t *testing.T) {
	prefix := clientID(t, "")
	sub := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true},
		slices.Concat(support.At(0, prefix+"/#"), support.At(1, prefix+"/+"), support.At(0, prefix+"/a")))
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: prefix + "/a", Payload: []byte("1")},
		{QoS: 0, Topic: prefix + "/a", Payload: []byte("2")},
		{QoS: 1, Topic: prefix + "/a/b", Payload: []byte("3")},
	}
	support.PublishAll(t, pub, ps)
	acknowledge(t, sub, support.DeliveredAt(t, sub, 1, ps[0].Topic, ps[0].Payload).ID, 0)
	delivered(t, sub, ps[1].Topic, ps[1].Payload)
	delivered(t, sub, ps[2].Topic, ps[2].Payload)
}

// design.md, Flows, SUBSCRIBE, One copy: "a record matching several of a
// session's non-shared subscriptions is sent once"; "Subscription
// Identifiers appear in complete filter-byte order, once for each
// contributing subscription having one"; "Only subscriptions eligible for
// the record contribute: topic [...] checks apply". MQTT 5 3.3.4: "If the
// Client specified a Subscription Identifier for any of the overlapping
// subscriptions the Server MUST send those Subscription Identifiers in the
// message which is published as the result of the subscriptions
// [MQTT-3.3.4-3]. If the Server sends a single copy of the message it MUST
// include in the PUBLISH packet the Subscription Identifiers for all
// matching subscriptions which have a Subscription Identifiers
// [MQTT-3.3.4-4]"; "it used the same identifier for more than one of
// them. In this case the PUBLISH packet will carry multiple identical
// Subscription Identifiers". Which filters match a topic is MQTT 5 4.7's,
// its own examples: "#" takes "the parent and any number of child levels"
// (4.7.1.2); "+" "matches only one topic level", so "sport/+" "does not
// match “sport” but it does match “sport/”" and "“/finance” matches “+/+”
// and “/+”, but not “+”" (4.7.1.3); "The Server MUST NOT match Topic
// Filters starting with a wildcard character (# or +) with Topic Names
// beginning with a $ character [MQTT-4.7.2-1]"; "Topic Names and Topic
// Filters are case sensitive" (4.7.3).
//
// On a broker of its own, a client subscribes to fourteen filters at QoS
// 0, each in a SUBSCRIBE of its own, in an order other than their bytes':
// twelve with an identifier of their own, one with the identifier of
// another, one with none. A publisher sends a message on each of seven
// topics, in order; one of them, under $, no filter matches. The client
// receives one delivery of each other topic, in order, each carrying the
// identifiers of exactly the filters 4.7 makes it match, in their bytes'
// order: the next delivery being the next topic's shows that none came
// twice, and that the one under $ came not at all.
func TestOneCopyCarriesEveryContributingIdentifier(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	sub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "overlapping", CleanStart: true})
	for _, s := range []struct {
		filter string
		id     uint32 // 0 for none
	}{
		{"sport/tennis/player1/#", 1},
		{"sport/#", 2},
		{"#", 3},
		{"sport/tennis/+", 4},
		{"sport/+", 5},
		{"+/+", 6},
		{"/+", 7},
		{"+", 8},
		{"+/monitor/Clients", 9},
		{"$x/#", 10},
		{"$x/monitor/+", 11},
		{"sport/tennis/player1", 2},
		{"Sport/#", 12},
		{"sport/+/player1", 0},
	} {
		var props harness.Properties
		if s.id != 0 {
			props = identifier(s.id)
		}
		support.Suback(t, sub, props, support.At(0, s.filter), 0x00)
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})

	deliveries := []struct {
		topic string
		ids   []uint32 // nil where no filter matches
	}{
		// "#", "sport/#", "sport/+/player1" (none), "sport/tennis/+",
		// "sport/tennis/player1", "sport/tennis/player1/#".
		{"sport/tennis/player1", []uint32{3, 2, 4, 2, 1}},
		{"$y/monitor/Clients", nil},
		{"sport", []uint32{3, 8, 2}},             // "#", "+", "sport/#"
		{"sport/", []uint32{3, 6, 2, 5}},         // "#", "+/+", "sport/#", "sport/+"
		{"/finance", []uint32{3, 6, 7}},          // "#", "+/+", "/+"
		{"$x/monitor/Clients", []uint32{10, 11}}, // "$x/#", "$x/monitor/+"
		{"Sport/tennis", []uint32{3, 6, 12}},     // "#", "+/+", "Sport/#"
	}
	ps := make([]*harness.Publish, len(deliveries))
	for i, d := range deliveries {
		ps[i] = &harness.Publish{QoS: 1, Topic: d.topic, Payload: []byte(d.topic)}
	}
	support.PublishAll(t, pub, ps)
	for _, d := range deliveries {
		if d.ids != nil {
			expectIDs(t, delivered(t, sub, d.topic, []byte(d.topic)), d.ids...)
		}
	}
}

// design.md, Flows, SUBSCRIBE, Options: "No Local is honoured everywhere,
// against the publisher's client id stored on the record and never sent";
// One copy: "Only subscriptions eligible for the record contribute:
// topic, live-start boundary, No Local, partition and permission checks
// apply". MQTT 5 3.8.3.1: "If the value is 1, Application Messages MUST
// NOT be forwarded to a connection with a ClientID equal to the ClientID
// of the publishing connection [MQTT-3.8.3-3]".
//
// A client subscribes to a filter with No Local and Subscription
// Identifier 1, and to another without it, identifier 2. It publishes,
// each acknowledged, a message both filters match, then one only the No
// Local filter matches; another client then publishes one the No Local
// filter matches. The first client receives its own first message once,
// carrying identifier 2 alone; then, its second skipped, the other
// client's message, carrying 1.
func TestNoLocalLeavesOtherSubscriptionsDelivering(t *testing.T) {
	prefix := clientID(t, "")
	c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-local"), CleanStart: true})
	support.Suback(t, c, identifier(1), []harness.Subscription{{Filter: prefix + "/a/#", NoLocal: true}}, 0x00)
	support.Suback(t, c, identifier(2), support.At(0, prefix+"/+/x"), 0x00)
	other, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true})

	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: prefix + "/a/x", Payload: []byte("both")})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: prefix + "/a/y", Payload: []byte("own")})
	support.Acknowledged(t, other, &harness.Publish{QoS: 1, Topic: prefix + "/a/z", Payload: []byte("other's")})
	expectIDs(t, delivered(t, c, prefix+"/a/x", []byte("both")), 2)
	expectIDs(t, delivered(t, c, prefix+"/a/z", []byte("other's")), 1)
}

// design.md, Flows, SUBSCRIBE, One copy: "A live $broadcast delivery
// preserves RETAIN when any contributor requests Retain As Published";
// Options: "Retain As Published applies on $broadcast only"; Channels,
// $broadcast: "RETAIN is 1 on retained values sent to a new subscription.
// Otherwise it is 0 unless Retain As Published". MQTT 5 3.3.1.3: "If the
// value of Retain As Published subscription option is set to 0, the Server
// MUST set the RETAIN flag to 0 when forwarding an Application Message
// regardless of how the RETAIN flag was set in the received PUBLISH packet
// [MQTT-3.3.1-12]. If the value of Retain As Published subscription option is
// set to 1, the Server MUST set the RETAIN flag equal to the RETAIN flag in
// the received PUBLISH packet [MQTT-3.3.1-13]".
//
// A client subscribes to three filters, the middle one, in both their
// subscription and their bytes' order, alone with Retain As Published; a
// second client to one filter without it. A publisher sends a retained
// message all three of the first client's filters match, the same topic
// not retained, and a retained one on a topic only a filter without
// Retain As Published matches. The first client receives them with RETAIN
// 1, 0 and 0; the second, which the first topic alone reaches, with 0.
func TestRetainAsPublishedFromAnyContributor(t *testing.T) {
	prefix := clientID(t, "")
	rap, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-rap"), CleanStart: true})
	support.Suback(t, rap, nil, []harness.Subscription{
		{Filter: prefix + "/r/a"},
		{Filter: prefix + "/r/#", RetainAsPublished: true},
		{Filter: prefix + "/+/a"},
	}, 0x00, 0x00, 0x00)
	plain := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-plain"), CleanStart: true}, support.At(0, prefix+"/r/#"))
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})

	ps := []*harness.Publish{
		{QoS: 1, Retain: true, Topic: prefix + "/r/a", Payload: []byte("retained")},
		{QoS: 1, Topic: prefix + "/r/a", Payload: []byte("plain")},
		{QoS: 1, Retain: true, Topic: prefix + "/q/a", Payload: []byte("retained elsewhere")},
	}
	support.PublishAll(t, pub, ps)
	for i, want := range []bool{true, false, false} {
		if got := delivered(t, rap, ps[i].Topic, ps[i].Payload); got.Retain != want {
			t.Errorf("message %d, published with RETAIN %v: delivered with RETAIN %v; want %v", i+1, ps[i].Retain, got.Retain, want)
		}
	}
	for _, p := range ps[:2] {
		if got := delivered(t, plain, p.Topic, p.Payload); got.Retain {
			t.Errorf("%q, published with RETAIN %v, delivered with RETAIN 1 through a filter without Retain As Published", p.Payload, p.Retain)
		}
	}
}

// design.md, Flows, SUBSCRIBE, The start point: "each subscription has a
// live-start boundary for every channel it reaches. The core captures that
// channel's tail when it accepts the subscription"; "Adding a filter does
// not move an existing consumer position"; One copy: "Only subscriptions
// eligible for the record contribute: topic, live-start boundary [...]
// checks apply"; Delivery, A resend: "Subscription changes affect new
// deliveries". Delivery, One mechanism: the in-flight window holds back a
// client's deliveries.
//
// A client with Receive Maximum 1 subscribes at QoS 1 to a filter, with
// Subscription Identifier 1. A publisher stores two messages under it. The
// client receives the first and holds it, so its position stays below the
// second, then subscribes to the second's topic at QoS 0 with identifier
// 5, and the publisher stores a third message there. Once the client
// acknowledges the first, it receives the second, stored before the new
// subscription, carrying identifier 1 alone; once it acknowledges that,
// the third, carrying 1 and 5.
func TestRecordBelowTheStartIsNotOwedThroughTheNewSubscription(t *testing.T) {
	prefix := clientID(t, "")
	c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: receiveMaximum5(1)})
	support.Suback(t, c, identifier(1), support.At(1, prefix+"/#"), 0x01)
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: prefix + "/a", Payload: []byte("held")},
		{QoS: 1, Topic: prefix + "/b", Payload: []byte("before")},
		{QoS: 1, Topic: prefix + "/b", Payload: []byte("after")},
	}
	support.PublishAll(t, pub, ps[:2])

	held := support.DeliveredAt(t, c, 1, ps[0].Topic, ps[0].Payload)
	support.Suback(t, c, identifier(5), support.At(0, prefix+"/b"), 0x00)
	support.Acknowledged(t, pub, ps[2])
	acknowledge(t, c, held.ID, 0)
	before := support.DeliveredAt(t, c, 1, ps[1].Topic, ps[1].Payload)
	expectIDs(t, before, 1)
	acknowledge(t, c, before.ID, 0)
	expectIDs(t, support.DeliveredAt(t, c, 1, ps[2].Topic, ps[2].Payload), 1, 5)
}

// design.md, Flows, SUBSCRIBE, One copy: "a record matching several of a
// session's non-shared subscriptions is sent once"; "A filter reaching
// several channels receives each record under its own channel's rules.
// Only subscriptions eligible for the record contribute: topic, live-start
// boundary, No Local, partition and permission checks apply. The delivery
// uses the lower of the publication's QoS and the highest contributing
// grant. Subscription Identifiers appear in complete filter-byte order,
// once for each contributing subscription having one"; Partitioning:
// topic_hash, as harness.TopicHash computes it; Options: "No Local is
// honoured everywhere, against the publisher's client id stored on the record".
// MQTT 5 [MQTT-3.3.4-3], [MQTT-3.3.4-4]; [MQTT-3.8.3-3].
//
// An append channel, ov, holds ov/#. harness.TopicHash modulo 3 is 2 for
// ov/a and ov/a/b/c, 0 for ov/a/b. A client subscribes, each in a SUBSCRIBE
// of its own, all at QoS 1 but one: to ov/# and ov/+ together, identifier
// 1; ov/a at QoS 0, identifier 2; ov/a/# with saguin-filter topic_hash(3, 0),
// identifier 3; and ov/a/b with No Local, identifier 4. Then a publisher
// stores r1 on ov/a and r2 on ov/a/b, the client itself r3 on ov/a/b, and
// the publisher r4 on ov/a/b/c. The client receives, each once, at QoS 1,
// in that order: r1 carrying 1, 1, 2 (ov/#, ov/+, ov/a; ov/a/#'s partition
// leaving it out); r2 carrying 1, 3, 4 (ov/#, ov/a/#, ov/a/b); r3 carrying
// 1, 3 (ov/a/b leaving its own client's record out); r4 carrying 1.
func TestOneCopyFromTheEligibleSubscriptionsOfAChannel(t *testing.T) {
	for topic, want := range map[string]uint64{"ov/a": 2, "ov/a/b": 0, "ov/a/b/c": 2} {
		if got := harness.TopicHash(topic) % 3; got != want {
			t.Fatalf("topic_hash(%q) mod 3 is %d; the test's case needs %d", topic, got, want)
		}
	}
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "ov", Type: "append", Filter: "ov/#"}}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "overlap-pub", CleanStart: true})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "overlapper", CleanStart: true})
	support.Suback(t, c, identifier(1), support.At(1, "ov/#", "ov/+"), 0x01, 0x01)
	support.Suback(t, c, identifier(2), support.At(0, "ov/a"), 0x00)
	support.Suback(t, c, append(identifier(3), harness.User("saguin-filter", "topic_hash(3, 0)")), support.At(1, "ov/a/#"), 0x01)
	support.Suback(t, c, identifier(4), []harness.Subscription{{Filter: "ov/a/b", QoS: 1, NoLocal: true}}, 0x01)
	ps := []*harness.Publish{
		{QoS: 1, Topic: "ov/a", Payload: []byte("r1")},
		{QoS: 1, Topic: "ov/a/b", Payload: []byte("r2")},
		{QoS: 1, Topic: "ov/a/b", Payload: []byte("r3")},
		{QoS: 1, Topic: "ov/a/b/c", Payload: []byte("r4")},
	}
	support.PublishAll(t, pub, ps[:2])
	support.Acknowledged(t, c, ps[2])
	support.Acknowledged(t, pub, ps[3])

	for i, ids := range [][]uint32{{1, 1, 2}, {1, 3, 4}, {1, 3}, {1}} {
		expectIDs(t, support.DeliveredAt(t, c, 1, ps[i].Topic, ps[i].Payload), ids...)
	}
}
