package conformance

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of shared subscriptions, from the wire (design.md, Flows,
// SUBSCRIBE: the $share row, A shared group, One copy, Refusals; Delivery;
// MQTT 5 4.8.2). Each group is named by its group and filter, so a test's
// own topic keeps its groups apart from every other test's.

// shareOf is the shared subscription filter of group over filter (MQTT 5
// 4.8.2: "$share/{ShareName}/{filter}").
func shareOf(group, filter string) string { return "$share/" + group + "/" + filter }

// droppedSeries is saguin_shares_dropped_total of $broadcast, where every
// group of these tests over an unconfigured topic delivers (metrics.md,
// Delivery: "unfinished shared QoS 2 messages dropped when their member's
// session ended"; Rules: a channel is "a configured channel, its
// <channel>__dlq or $broadcast").
const droppedSeries = `saguin_shares_dropped_total{channel="$broadcast"}`

// design.md, Flows, SUBSCRIBE, A shared group: "Each message goes to one
// member with room"; "Selection is round-robin by complete client id,
// starting after the last selected id and wrapping once"; "A new group or
// queue starts before the first id". MQTT 5 4.8.2: "Each subsequent
// Application Message that matches the Shared Subscription is now sent to
// one and only one of the Sessions that are subscribed to the Shared
// Subscription".
//
// Three members, each with room for every message, join a group out of
// client-id order: -c, then -a, then -b. A publisher stores nine QoS 1
// messages: the first goes to -a, the second to -b, the third to -c, and so
// round again, each member receiving its own in order and no other's.
func TestShareSelectsMembersRoundRobinByClientID(t *testing.T) {
	topic := clientID(t, "/x")
	ids := []string{clientID(t, "-a"), clientID(t, "-b"), clientID(t, "-c")}
	members := make(map[string]*harness.Client, len(ids))
	for _, id := range []string{ids[2], ids[0], ids[1]} {
		members[id] = support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true}, support.At(1, shareOf("g", topic)))
	}
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := numbered(topic, 3*len(ids))
	support.PublishAll(t, pub, ps)
	for i, p := range ps {
		support.DeliveredAt(t, members[ids[i%len(ids)]], 1, topic, p.Payload)
	}
}

// design.md, Flows, SUBSCRIBE, the table: "$share/<group>/<filter> over any
// of the above but a worker's | new messages only (changes only on latest),
// no retained values"; A shared group: "A new group starts at the tail of
// every channel it reaches". MQTT 5 4.8.2: "No Retained Messages are sent to
// the Session when it first subscribes. It will be sent other matching
// messages as they are published".
//
// An append channel, hist, holds hist/#, starting at the floor, and a latest
// channel, last, holds last/#. A publisher stores hist/x, a value of
// last/x and a retained value of ret/x, on $broadcast. A client subscribes
// at QoS 1 to $share/g/+/x, a new group reaching all three channels. The
// publisher stores each topic again: the member's next three packets are
// those three, RETAIN 0, and nothing stored before its SUBSCRIBE.
func TestShareNewGroupStartsAtTheTail(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "hist", Type: "append", Filter: "hist/#"},
		{Name: "last", Type: "latest", Filter: "last/#"},
	}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "tail-pub", CleanStart: true})
	topics := []string{"hist/x", "last/x", "ret/x"}
	before := make([]*harness.Publish, len(topics))
	for i, topic := range topics {
		before[i] = &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("before"), Retain: topic == "ret/x"}
	}
	support.PublishAll(t, pub, before)
	member := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "tail-member", CleanStart: true}, support.At(1, shareOf("g", "+/x")))
	support.PublishAll(t, pub, numberedOn(topics, "after"))

	got := make(map[string]string, len(topics))
	for range topics {
		p := harness.Expect[*harness.Publish](t, member)
		if p.Retain {
			t.Errorf("the delivery of %q has RETAIN 1; want 0, no retained value", p.Topic)
		}
		got[p.Topic] = string(p.Payload)
	}
	for _, topic := range topics {
		if got[topic] != "after" {
			t.Fatalf("the member's first deliveries %q; want each of %q once, published after its SUBSCRIBE", got, topics)
		}
	}
}

// numberedOn is a QoS 1 publication of payload on each of topics, in order.
func numberedOn(topics []string, payload string) []*harness.Publish {
	ps := make([]*harness.Publish, len(topics))
	for i, topic := range topics {
		ps[i] = &harness.Publish{QoS: 1, Topic: topic, Payload: []byte(payload)}
	}
	return ps
}

// shareACL lets all write and read everything on $broadcast and part read
// perm/open/# alone.
const shareACL = `roles:
  everything:
    - topic: "#"
      allow: [write, read]
  open:
    - topic: "perm/open/#"
      allow: [read]
users:
  all: [everything]
  part: [open]
`

// design.md, Flows, SUBSCRIBE, A shared group: "Each message goes to one
// member with room"; "Ineligible members are skipped. Only a selection moves
// the last-selected id"; Delivery, Queue jobs: workers "are selected
// round-robin among those connected, with room and allowed by the acl_file,
// as a shared group's members are"; A message over the client's Maximum
// Packet Size: "A worker or $share member that cannot take it is passed
// over". One mechanism for every channel: "The window is the lower of the
// client's Receive Maximum and limits.max_inflight".
//
// Each case has two members, -a and -b, selected in that order:
//   - permission: -a may read perm/open/# alone. perm/secret/1 passes -a
//     over for -b, perm/open/2 goes to -a, perm/secret/3 to -b.
//   - room: -a has Receive Maximum 1. 1 goes to -a, unacknowledged; 2 to
//     -b; 3 passes -a over for -b. Once -a acknowledges 1, 4 goes to -a.
//   - Maximum Packet Size: -a takes packets of 128 bytes at most. A message
//     of 300 bytes passes -a over for -b; a small one then goes to -a.
func TestSharePassesOverIneligibleMembers(t *testing.T) {
	b := support.Launch(t, aclOptions(t, shareACL, "all", "part"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin("pass-pub", "all"))
	// pair subscribes -a, whose CONNECT is a's, and -b to filter at QoS 1.
	pair := func(t *testing.T, a *harness.Connect, filter string) (*harness.Client, *harness.Client) {
		t.Helper()
		ca := support.Subscribed(t, b.Addr, harness.V5, a, support.At(1, filter))
		cb := support.Subscribed(t, b.Addr, harness.V5, aclLogin(clientID(t, "-b"), "all"), support.At(1, filter))
		return ca, cb
	}
	t.Run("permission", func(t *testing.T) {
		a, bb := pair(t, aclLogin(clientID(t, "-a"), "part"), shareOf("g", "perm/#"))
		ps := []*harness.Publish{
			{QoS: 1, Topic: "perm/secret/1", Payload: []byte("1")},
			{QoS: 1, Topic: "perm/open/2", Payload: []byte("2")},
			{QoS: 1, Topic: "perm/secret/3", Payload: []byte("3")},
		}
		support.PublishAll(t, pub, ps)
		support.DeliveredAt(t, bb, 1, ps[0].Topic, ps[0].Payload)
		support.DeliveredAt(t, a, 1, ps[1].Topic, ps[1].Payload)
		support.DeliveredAt(t, bb, 1, ps[2].Topic, ps[2].Payload)
	})
	t.Run("room", func(t *testing.T) {
		connect := aclLogin(clientID(t, "-a"), "all")
		connect.Props = receiveMaximum5(1)
		a, bb := pair(t, connect, shareOf("g", "room/x"))
		ps := numbered("room/x", 4)
		support.PublishAll(t, pub, ps[:3])
		first := support.DeliveredAt(t, a, 1, "room/x", ps[0].Payload)
		support.DeliveredAt(t, bb, 1, "room/x", ps[1].Payload)
		support.DeliveredAt(t, bb, 1, "room/x", ps[2].Payload)
		acknowledge(t, a, first.ID, 0)
		taken(t, a, "the PUBACK of 1")
		support.Acknowledged(t, pub, ps[3])
		support.DeliveredAt(t, a, 1, "room/x", ps[3].Payload)
	})
	t.Run("Maximum Packet Size", func(t *testing.T) {
		connect := aclLogin(clientID(t, "-a"), "all")
		connect.Props = harness.Properties{{ID: harness.MaximumPacketSize, Int: 128}}
		a, bb := pair(t, connect, shareOf("g", "size/x"))
		large := &harness.Publish{QoS: 1, Topic: "size/x", Payload: testkit.Filling("size/x", 300)}
		small := &harness.Publish{QoS: 1, Topic: "size/x", Payload: []byte("small")}
		support.PublishAll(t, pub, []*harness.Publish{large, small})
		support.DeliveredAt(t, bb, 1, large.Topic, large.Payload)
		support.DeliveredAt(t, a, 1, small.Topic, small.Payload)
	})
}

// design.md, Flows, SUBSCRIBE, A shared group: "Its PUBACK or PUBCOMP
// settles the message. A negative acknowledgement discards it. QoS 0 settles
// on successful writing. An unfinished QoS 1 message goes to another member
// when its member's session ends"; "When the session ends, that message is
// dropped and counted". MQTT 5 4.8.2: "If a Client responds with a PUBACK or
// PUBREC containing a Reason Code of 0x80 or greater to a PUBLISH packet
// from the Server, the Server MUST discard the Application Message and not
// attempt to send it to any other Subscriber [MQTT-4.8.2-6]".
//
// A member -a, holding a durable session, alone in its group at QoS 2,
// receives five messages: 1 at QoS 1, acknowledged 0x00; 2 at QoS 1,
// acknowledged 0x80; 3 at QoS 2, completed with PUBCOMP; 4 at QoS 2,
// refused with PUBREC 0x80; 5 at QoS 0. A member -b joins, then -a ends
// its session. The next message, 6, is -b's first packet: none of the five
// went back to the group, and no message is counted dropped.
func TestShareAcknowledgementsSettle(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	topic := "settle/x"
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "settle-a", CleanStart: true, Props: support.Durable}, support.At(2, shareOf("g", topic)))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "settle-pub", CleanStart: true})

	acked := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("1")}
	support.Acknowledged(t, pub, acked)
	acknowledge(t, a, support.DeliveredAt(t, a, 1, topic, acked.Payload).ID, 0)
	refused := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("2")}
	support.Acknowledged(t, pub, refused)
	if err := a.Send(&harness.Ack{Kind: harness.PUBACK, ID: support.DeliveredAt(t, a, 1, topic, refused.Payload).ID, Reason: 0x80}); err != nil {
		t.Fatal(err)
	}
	completed := twice(topic, "3")
	published(t, pub, completed)
	id := support.DeliveredAt(t, a, 2, topic, completed.Payload).ID
	released(t, a, id)
	pubcomp(t, a, id)
	refusedQoS2 := twice(topic, "4")
	published(t, pub, refusedQoS2)
	pubrecFor(t, a, support.DeliveredAt(t, a, 2, topic, refusedQoS2.Payload).ID, 0x80)
	written := &harness.Publish{Topic: topic, Payload: []byte("5")}
	if err := pub.Send(written); err != nil {
		t.Fatal(err)
	}
	delivered(t, a, topic, written.Payload)
	taken(t, a, "its acknowledgements")

	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "settle-b", CleanStart: true}, support.At(2, shareOf("g", topic)))
	support.Disconnected(t, a, endSession)
	next := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("6")}
	support.Acknowledged(t, pub, next)
	support.DeliveredAt(t, bb, 1, topic, next.Payload)
	expectSample(t, b, droppedSeries, 0, "once -a's session ended with every message settled")
}

// design.md, Flows, SUBSCRIBE, A shared group: "An unfinished QoS 1 message
// goes to another member when its member's session ends"; "Returned QoS 1
// deliveries precede new records, ordered by channel id and offset".
// storage.md, Ids: "The $broadcast channel is channel 1". MQTT 5 4.8.2: "If
// the Client's Session terminates before the Client reconnects, the Server
// SHOULD send the Application Message to another Client that is subscribed
// to the same Shared Subscription".
//
// An append channel, two, holds two/#. A member -a, with a session ending
// with its connection, subscribes at QoS 1 to $share/g/+/x and receives,
// acknowledging none, two/x 1, one/x 2 (on $broadcast), two/x 3 and one/x
// 4. A member -b joins, and -a disconnects, ending its session. A publisher
// stores one/x 5: -b receives 2 and 4 ($broadcast, channel 1, by offset),
// then 1 and 3 (two), then 5.
func TestShareReturnsUnfinishedQoS1WhenItsSessionEnds(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "two", Type: "append", Filter: "two/#"}}})
	filter := shareOf("g", "+/x")
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "return-a", CleanStart: true}, support.At(1, filter))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "return-pub", CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "two/x", Payload: []byte("1")},
		{QoS: 1, Topic: "one/x", Payload: []byte("2")},
		{QoS: 1, Topic: "two/x", Payload: []byte("3")},
		{QoS: 1, Topic: "one/x", Payload: []byte("4")},
	}
	for _, p := range ps {
		support.Acknowledged(t, pub, p)
		support.DeliveredAt(t, a, 1, p.Topic, p.Payload)
	}
	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "return-b", CleanStart: true}, support.At(1, filter))
	support.Disconnected(t, a, &harness.Disconnect{})
	next := &harness.Publish{QoS: 1, Topic: "one/x", Payload: []byte("5")}
	support.Acknowledged(t, pub, next)
	for _, p := range []*harness.Publish{ps[1], ps[3], ps[0], ps[2], next} {
		support.DeliveredAt(t, bb, 1, p.Topic, p.Payload)
	}
}

// design.md, Flows, SUBSCRIBE, A shared group: "An unfinished QoS 2 message
// waits while that session is away. When the session ends, that message is
// dropped and counted (MQTT-4.8.2-4/-5)"; Flows, CONNECT, 6: "Clean start
// ends an existing session". metrics.md: saguin_shares_dropped_total. MQTT 5
// 4.8.2: "the Server MUST complete the delivery of the message to that
// Client when it reconnects [MQTT-4.8.2-4]. If the Client's Session
// terminates before the Client reconnects, the Server MUST NOT send the
// Application Message to any other subscribed Client [MQTT-4.8.2-5]".
//
// Members -a, holding a durable session, and -b subscribe at QoS 2. A QoS 2
// message 1 goes to -a, which sends no PUBREC and goes away. Message 2 goes
// to -b, 1 waiting, and nothing is counted dropped. -a resumes: 1 is resent
// to it, DUP 1. -a goes away again and connects with Clean Start 1, ending
// the session: 1 is counted dropped on $broadcast, and message 3 is -b's
// next packet.
func TestShareUnfinishedQoS2WaitsThenIsDropped(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	topic := "held/x"
	connect := &harness.Connect{ClientID: "held-a", CleanStart: true, Props: support.Durable}
	a := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, shareOf("g", topic)))
	bb := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "held-b", CleanStart: true}, support.At(2, shareOf("g", topic)))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "held-pub", CleanStart: true})

	unfinished := twice(topic, "1")
	published(t, pub, unfinished)
	first := support.DeliveredAt(t, a, 2, topic, unfinished.Payload)
	support.Away(t, a)
	second := twice(topic, "2")
	published(t, pub, second)
	id := support.DeliveredAt(t, bb, 2, topic, second.Payload).ID
	released(t, bb, id)
	pubcomp(t, bb, id)
	expectSample(t, b, droppedSeries, 0, "while -a's session is away")

	a = resumed(t, b.Addr, harness.V5, connect)
	redelivered(t, a, first)
	support.Away(t, a)
	connect.CleanStart = true
	harness.Connected(t, b.Addr, harness.V5, connect)
	awaitSeries(t, b, map[string]float64{droppedSeries: 1})
	third := twice(topic, "3")
	published(t, pub, third)
	support.DeliveredAt(t, bb, 2, topic, third.Payload)
}

// design.md, Flows, SUBSCRIBE, A shared group: a group's "cursors are
// stored if any member is durable"; "Joining an existing group preserves its
// cursors and returned deliveries"; "A durable shared group stores its
// last-selected id"; Restart: "The broker also restores $share cursors".
//
// Group g1: members -a and -b, holding durable sessions, receive 1, 2 and 3
// in turn, -a last. Group g2: -y, holding a durable session, joins and goes
// away; -x, whose session ends with its connection, joins, receives n1,
// unacknowledged, and leaves, handing n1 back; n2 is stored with no member
// connected. -a and -b go away and the broker restarts. Once -a and -b
// resume, message 4 goes to -b, after -a. A new member, -c, joins g2: it
// receives n1, then n2.
func TestShareDurableGroupSurvivesRestart(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir()}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-pub", CleanStart: true})
	one, two := "kept/one", "kept/two"
	connectA := &harness.Connect{ClientID: "kept-a", CleanStart: true, Props: support.Durable}
	connectB := &harness.Connect{ClientID: "kept-b", CleanStart: true, Props: support.Durable}
	a := support.Subscribed(t, b.Addr, harness.V5, connectA, support.At(1, shareOf("g1", one)))
	bb := support.Subscribed(t, b.Addr, harness.V5, connectB, support.At(1, shareOf("g1", one)))
	ms := numbered(one, 4)
	for i, m := range ms[:3] {
		support.Acknowledged(t, pub, m)
		member := []*harness.Client{a, bb}[i%2]
		acknowledge(t, member, support.DeliveredAt(t, member, 1, one, m.Payload).ID, 0)
	}

	y := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-y", CleanStart: true, Props: support.Durable}, support.At(1, shareOf("g2", two)))
	support.Away(t, y)
	x := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-x", CleanStart: true}, support.At(1, shareOf("g2", two)))
	returned := &harness.Publish{QoS: 1, Topic: two, Payload: []byte("n1")}
	support.Acknowledged(t, pub, returned)
	support.DeliveredAt(t, x, 1, two, returned.Payload)
	support.Disconnected(t, x, &harness.Disconnect{})
	owed := &harness.Publish{QoS: 1, Topic: two, Payload: []byte("n2")}
	support.Acknowledged(t, pub, owed)
	taken(t, a, "the messages")
	taken(t, bb, "the messages")
	support.Away(t, a)
	support.Away(t, bb)
	stopped(t, b)

	b = support.Launch(t, o)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-pub", CleanStart: true})
	resumed(t, b.Addr, harness.V5, connectA)
	bb = resumed(t, b.Addr, harness.V5, connectB)
	support.Acknowledged(t, pub, ms[3])
	support.DeliveredAt(t, bb, 1, one, ms[3].Payload)
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-c", CleanStart: true}, support.At(1, shareOf("g2", two)))
	support.DeliveredAt(t, c, 1, two, returned.Payload)
	support.DeliveredAt(t, c, 1, two, owed.Payload)
}

// design.md, Flows, SUBSCRIBE, A shared group: "When the last member
// unsubscribes or its session ends, the shared subscription's cursors and
// pending deliveries are removed"; "A new group starts at the tail of every
// channel it reaches". MQTT 5 4.8.2: "A Shared Subscription ends, and any
// undelivered messages associated with it are deleted, when there are no
// longer any Sessions subscribed to it".
//
// For each way of leaving, a member -a with Receive Maximum 1, alone in its
// group, receives 1 of three messages, unacknowledged, 2 and 3 waiting for
// room. -a unsubscribes, or disconnects ending its session. A member -b
// joins the group: the next message, 4, is its first packet.
func TestShareGroupEndsWithItsLastMember(t *testing.T) {
	for _, c := range []struct {
		name  string
		leave func(t *testing.T, a *harness.Client, filter string)
	}{
		{"unsubscribed", func(t *testing.T, a *harness.Client, filter string) { unsuback(t, a, nil, []string{filter}, 0x00) }},
		{"session ended", func(t *testing.T, a *harness.Client, _ string) { support.Disconnected(t, a, &harness.Disconnect{}) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			topic := clientID(t, "/x")
			filter := shareOf("g", topic)
			a := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-a"), CleanStart: true, Props: receiveMaximum5(1)},
				support.At(1, filter))
			pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
			ps := numbered(topic, 4)
			support.PublishAll(t, pub, ps[:3])
			support.DeliveredAt(t, a, 1, topic, ps[0].Payload)
			c.leave(t, a, filter)
			bb := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true}, support.At(1, filter))
			support.Acknowledged(t, pub, ps[3])
			support.DeliveredAt(t, bb, 1, topic, ps[3].Payload)
		})
	}
}

// design.md, Words: Retention, "A record past its channel's retention is
// gone, whoever still needed it: [...] or one handed back to a shared
// group"; "The session continues and the loss is counted"; Words: Missed.
// metrics.md: saguin_missed_total, "records consumers were owed and never
// received [...] retention".
//
// An append channel, events, keeps 250 bytes. -y, holding a durable
// session, joins $share/g/events/# and goes away; -x, whose session ends
// with its connection, joins, receives n1, a 100-byte record,
// unacknowledged, and leaves, handing n1 back to the group. n2 and n3 are
// stored: retention deletes n1, and the loss is counted once. A new member,
// -c, receives n2, then n3.
func TestRetentionPassesAReturnedShareMessage(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	filter := shareOf("g", "events/#")
	y := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "passed-y", CleanStart: true, Props: support.Durable}, support.At(1, filter))
	support.Away(t, y)
	x := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "passed-x", CleanStart: true}, support.At(1, filter))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "passed-pub", CleanStart: true})
	n1, n2, n3 := support.Sized("events/n1", testkit.RecordBytes), support.Sized("events/n2", testkit.RecordBytes), support.Sized("events/n3", testkit.RecordBytes)
	support.Acknowledged(t, pub, n1)
	support.DeliveredAt(t, x, 1, n1.Topic, n1.Payload)
	support.Disconnected(t, x, &harness.Disconnect{})
	support.PublishAll(t, pub, []*harness.Publish{n2, n3})
	awaitHeld(t, support.OpenDB(t, b.DB), "events", []string{n2.Topic, n3.Topic}, "once n3 is stored")
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 1, "once retention has passed n1, handed back to the group")
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "passed-c", CleanStart: true}, support.At(1, filter))
	support.DeliveredAt(t, c, 1, n2.Topic, n2.Payload)
	support.DeliveredAt(t, c, 1, n3.Topic, n3.Payload)
}

// design.md, Flows, SUBSCRIBE, the table: "$share/<group>/<filter> [...]
// MQTT 3.1.1: refused (0x80)"; Options: "On $share, it [No Local] is a
// protocol error that closes the connection with 0x82"; Refusals, per
// filter: "0x8F for a filter [...] over limits.max_topic_length or
// limits.max_topic_levels. A $share filter is measured whole, including
// $share/<group>/"; "A malformed filter is a malformed packet, with 0x81".
// MQTT 5 [MQTT-3.8.3-4]: "It is a Protocol Error to set the No Local bit to
// 1 on a Shared Subscription"; [MQTT-4.8.2-2]: the "/" after the ShareName
// "MUST be followed by a Topic Filter"; 4.7.3: "All Topic Names and Topic
// Filters MUST be at least one character long [MQTT-4.7.3-1]". The
// partition declaration on $share (0x83) is
// TestPartitionsSelectTopicsByTheirHash's; the acl_file denying share is
// TestACLConnackCapabilities's (Shared Subscription Available 0) and
// TestBreakingTheConnackDisconnects's (DISCONNECT 0x9E).
//
//   - MQTT 3.1.1: a SUBSCRIBE of $share/g/x and x gets 0x80, 0x01.
//   - No Local: an MQTT 5 SUBSCRIBE of $share/g/x with No Local gets
//     DISCONNECT 0x82, then the end of stream.
//   - measured whole: with max_topic_length 16 and max_topic_levels 4,
//     $share/g/abcdefg (16 bytes), $share/g/abcdefgh (17),
//     $share/g/a/b (4 levels) and $share/g/a/b/c (5) get 0x01, 0x8F, 0x01,
//     0x8F, each filter part within both limits alone.
//   - malformed: $share/g/, its filter part empty, gets DISCONNECT 0x81,
//     then the end of stream.
func TestShareRefusals(t *testing.T) {
	t.Run("MQTT 3.1.1", func(t *testing.T) {
		c, _ := harness.Connected(t, brokerAddr, harness.V311, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		support.Suback(t, c, nil, support.At(1, shareOf("g", "x"), "x"), 0x80, 0x01)
	})
	t.Run("measured whole", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{MaxTopicLength: 16, MaxTopicLevels: 4})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		support.Suback(t, c, nil, support.At(1, shareOf("g", "abcdefg"), shareOf("g", "abcdefgh"), shareOf("g", "a/b"), shareOf("g", "a/b/c")),
			0x01, 0x8F, 0x01, 0x8F)
	})
	for _, r := range []struct {
		name   string
		sub    harness.Subscription
		reason byte
	}{
		{"No Local", harness.Subscription{Filter: shareOf("g", "x"), QoS: 1, NoLocal: true}, 0x82},
		{"malformed", harness.Subscription{Filter: "$share/g/", QoS: 1}, 0x81},
	} {
		t.Run(r.name, func(t *testing.T) {
			c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			if err := c.Send(&harness.Subscribe{ID: c.NextID(), Subs: []harness.Subscription{r.sub}}); err != nil {
				t.Fatal(err)
			}
			if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != r.reason {
				t.Fatalf("DISCONNECT 0x%02X, want 0x%02X", d.Reason, r.reason)
			}
			harness.ExpectClosed(t, c)
		})
	}
}

// design.md, Flows, SUBSCRIBE, One copy: "a record matching several of a
// session's non-shared subscriptions is sent once. Each $share subscription
// is handled on its own"; "Subscription Identifiers appear in complete
// filter-byte order, once for each contributing subscription having one".
// MQTT 5 4.8.2: "If a Client has a Shared Subscription and a Non‑shared
// Subscription and a message matches both of them, the Client will receive
// a copy of the message by virtue of it having the Non‑shared Subscription.
// A second copy of the message will be delivered to one of the subscribers
// to the Shared Subscription".
//
// A client subscribes at QoS 1 to a topic with Subscription Identifier 1,
// and to $share/g/ that topic, alone in the group, with Subscription
// Identifier 2. A message on the topic comes twice, at QoS 1, DUP 0: one
// copy carrying identifier 1 alone, the other 2 alone.
func TestShareAndOrdinarySubscriptionsEachDeliver(t *testing.T) {
	topic := clientID(t, "/x")
	c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	support.Suback(t, c, identifier(1), support.At(1, topic), 0x01)
	support.Suback(t, c, identifier(2), support.At(1, shareOf("g", topic)), 0x01)
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	m := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("both")}
	support.Acknowledged(t, pub, m)
	got := make([][]uint32, 0, 2)
	for range 2 {
		got = append(got, subscriptionIDs(support.DeliveredAt(t, c, 1, topic, m.Payload)))
	}
	slices.SortFunc(got, slices.Compare)
	if !slices.EqualFunc(got, [][]uint32{{1}, {2}}, slices.Equal) {
		t.Fatalf("the two copies carry Subscription Identifiers %v; want [1] on one and [2] on the other", got)
	}
}

// design.md, Delivery, Admission qualification: "A shared group or queue
// dispatcher uses its common progress and its eligible recipients. A
// member's disconnection, permission hold or overdue obligation excludes
// that member, not other members. No qualifying recipient means no
// qualifying work"; Peer obligations: "An overdue obligation removes
// qualification at its deadline"; Execution, Capacities, Delivery admission:
// "The gate closes when the admission delay reaches
// delivery_admission_delay".
//
// On a stallable broker with delivery_peer_timeout 150ms, a member -a,
// holding a durable session, alone in its group, receives a message and
// acknowledges none. With the store stalled, a second message goes to -a,
// its work old past delivery_admission_delay: -a's acknowledgement overdue,
// the gate never closes. A member -b, holding a durable session, joins;
// with the store stalled, a third message goes to -b, after -a: -b still
// qualifies, and the gate closes.
func TestShareOverdueMemberAloneLosesQualification(t *testing.T) {
	s := launchStallable(t, &harness.Options{DeliveryPeerTimeout: 150 * time.Millisecond})
	filter := shareOf("g", "gated/#")
	a := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: "overdue-a", CleanStart: true, Props: support.Durable}, support.At(1, filter))
	pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: "overdue-pub", CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "gated/1", Payload: []byte("x")})
	support.DeliveredAt(t, a, 1, "gated/1", []byte("x"))
	s.stalledDelivery(t, pub, a, "gated/2")
	neverGated(t, s.Broker, "with -a alone in its group, its acknowledgement overdue")

	bb := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: "overdue-b", CleanStart: true, Props: support.Durable}, support.At(1, filter))
	s.stalledDelivery(t, pub, bb, "gated/3")
	if got := support.Sampled(t, s.Broker, gatedSecondsSeries)[gatedSecondsSeries]; got == 0 {
		t.Fatalf("%s reads 0 after -b's stalled delivery; want the gate to have closed, -b qualifying beside the overdue -a", gatedSecondsSeries)
	}
}

// design.md, Delivery, Order: "In a $share group, order holds per member";
// A shared group: "Each message goes to one member with room".
//
// Members -a, with Receive Maximum 1, and -b, with Receive Maximum 3, each
// acknowledge every delivery as it comes. A publisher stores 30 QoS 1
// messages on the group's topic, numbered in order, ten at a time: each one
// reaches one member, and each member receives its own in increasing order.
func TestShareOrderHoldsPerMember(t *testing.T) {
	topic := clientID(t, "/x")
	const total = 30
	// arrival is one delivery a member read and acknowledged, its payload
	// as a number, or the error that ended the member's reading.
	type arrival struct {
		member string
		n      int
		err    error
	}
	arrivals := make(chan arrival, 2*total+2)
	for _, m := range []struct {
		suffix string
		window uint32
	}{{"-a", 1}, {"-b", 3}} {
		c := support.Subscribed(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, m.suffix), CleanStart: true, Props: receiveMaximum5(m.window)},
			support.At(1, shareOf("g", topic)))
		go func() {
			for {
				got, err := c.Recv()
				p, ok := got.(*harness.Publish)
				switch {
				case err != nil:
				case !ok || p.QoS != 1 || p.Topic != topic:
					err = fmt.Errorf("%+v; want a PUBLISH of %q at QoS 1", got, topic)
				default:
					err = c.Send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID})
				}
				if err != nil {
					arrivals <- arrival{member: m.suffix, err: err}
					return // the test is over, or failed
				}
				n, err := strconv.Atoi(string(p.Payload))
				arrivals <- arrival{member: m.suffix, n: n, err: err}
			}
		}()
	}
	pub, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	// Ten at a time, within the publisher's Receive Maximum of 20 (config.md,
	// qos2.max_inflight_per_client).
	ps := numbered(topic, total)
	for i := 0; i < total; i += 10 {
		support.PublishAll(t, pub, ps[i:i+10])
	}
	last := map[string]int{}
	seen := make([]bool, total+1)
	for range total {
		a := <-arrivals
		switch {
		case a.err != nil:
			t.Fatalf("member %s: %v", a.member, a.err)
		case a.n < 1 || a.n > total || seen[a.n]:
			t.Fatalf("member %s received message %d; want each of 1 to %d once, to one member", a.member, a.n, total)
		case a.n < last[a.member]:
			t.Fatalf("member %s received message %d after %d; want its messages in order", a.member, a.n, last[a.member])
		}
		seen[a.n], last[a.member] = true, a.n
	}
}

// The shared group's stored evidence across a crash (design.md, Flows,
// Delivery, Positions): "A stored shared cursor passes an assignment only
// with stored evidence of its disposition, committed before the delivery is
// handed to the writer. A group's first durable membership captures its
// existing obligations before the SUBACK. Settlements, refusals and drops
// ahead of the cursor keep group-owned terminal evidence. A restart ends
// volatile members' assignments. QoS 1 returns to the group. QoS 2 is dropped
// and counted." storage.md, What is stored: "A shared group's bookkeeping of
// its members' assignments is stored, including a volatile member's
// assignments"; Store, full mode: "A restart recovers committed state".
// MQTT 5 4.8.2: a QoS 2 message whose session has terminated "MUST
// NOT [be sent] to any other subscribed Client [MQTT-4.8.2-5]"; 4.3.3, QoS
// 2: exactly once.

// crashedShareGroup builds the traced failure in a group of its own on
// topic, on a broker of the test's own with an operations listener, and
// returns the broker restarted on the same files after a crash, with -b
// resumed, and the publisher reconnected. -b, holding a durable session,
// joins $share/g/<topic> at QoS 2, then -a, whose session ends with its
// connection. n, published at qos, goes to -a, first by client id, which
// acknowledges nothing. n+1, published at QoS 2, goes to -b, which
// completes it: PUBREC, PUBREL, PUBCOMP, then a QoS 1 publish of its own
// whose PUBACK follows the commit of its settlement (resume_test.go,
// committed). The broker is killed (SIGKILL) and started again.
func crashedShareGroup(t *testing.T, name string, qos byte) (b *harness.Broker, bb, pub *harness.Client, n, n1 *harness.Publish) {
	t.Helper()
	o := &harness.Options{Dir: t.TempDir()}
	b = support.OpsLaunch(t, o)
	topic := name + "/x"
	filter := shareOf("g", topic)
	connectB := &harness.Connect{ClientID: name + "-b", CleanStart: true, Props: support.Durable}
	bb = support.Subscribed(t, b.Addr, harness.V5, connectB, support.At(2, filter))
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: name + "-a", CleanStart: true}, support.At(2, filter))
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: name + "-pub", CleanStart: true})

	n = &harness.Publish{QoS: qos, Topic: topic, Payload: []byte("n")}
	if qos == 2 {
		published(t, pub, n)
	} else {
		support.Acknowledged(t, pub, n)
	}
	support.DeliveredAt(t, a, qos, topic, n.Payload)
	n1 = twice(topic, "n+1")
	published(t, pub, n1)
	id := support.DeliveredAt(t, bb, 2, topic, n1.Payload).ID
	released(t, bb, id)
	pubcomp(t, bb, id)
	committed(t, bb, name+"/barrier")
	taken(t, a, "the delivery of n, unacknowledged")

	support.Crash(t, b)
	b = support.OpsLaunch(t, o)
	bb = resumed(t, b.Addr, harness.V5, connectB)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: name + "-pub", CleanStart: true})
	return b, bb, pub, n, n1
}

// The QoS 1 case (crashedShareGroup): after the restart, -a's session is
// gone and n, its QoS 1 assignment, returns to the group (A shared group:
// "Returned QoS 1 deliveries precede new records"). A publisher stores
// n+2: -b's first packet is n, at QoS 1, and its next is n+2, n+1, settled
// by -b, never sent again. Nothing is counted dropped.
func TestShareCrashReturnsAVolatileMembersQoS1(t *testing.T) {
	b, bb, pub, n, n1 := crashedShareGroup(t, "crash1", 1)
	next := &harness.Publish{QoS: 1, Topic: n.Topic, Payload: []byte("n+2")}
	support.Acknowledged(t, pub, next)
	p := harness.Expect[*harness.Publish](t, bb)
	if !bytes.Equal(p.Payload, n.Payload) || p.QoS != 1 {
		t.Fatalf("after the restart -b's first PUBLISH is %q at QoS %d; want %q at QoS 1, -a's QoS 1 assignment returned to the group", p.Payload, p.QoS, n.Payload)
	}
	p = harness.Expect[*harness.Publish](t, bb)
	if bytes.Equal(p.Payload, n1.Payload) {
		t.Fatalf("after the restart -b received %q again, which it had completed with PUBCOMP before the crash; want %q (exactly once, MQTT 5 4.3.3)", p.Payload, next.Payload)
	}
	if !bytes.Equal(p.Payload, next.Payload) {
		t.Fatalf("after the restart -b's next PUBLISH is %q; want %q", p.Payload, next.Payload)
	}
	expectSample(t, b, droppedSeries, 0, "once -a's QoS 1 assignment has returned to the group")
}

// The QoS 2 case (crashedShareGroup): after the restart, -a's session is
// gone and n, its QoS 2 assignment, is dropped and counted: it goes to no
// other member [MQTT-4.8.2-5], and saguin_shares_dropped_total on
// $broadcast reads 1. n+1, settled by -b, is never sent again: once a
// publisher stores n+2, it is -b's first packet.
func TestShareCrashDropsAVolatileMembersQoS2(t *testing.T) {
	b, bb, pub, n, n1 := crashedShareGroup(t, "crash2", 2)
	next := twice(n.Topic, "n+2")
	published(t, pub, next)
	p := harness.Expect[*harness.Publish](t, bb)
	switch string(p.Payload) {
	case string(n.Payload):
		t.Fatalf("after the restart -b received %q, -a's unfinished QoS 2 assignment; want it dropped, sent to no other member [MQTT-4.8.2-5]", p.Payload)
	case string(n1.Payload):
		t.Fatalf("after the restart -b received %q again, which it had completed with PUBCOMP before the crash; want %q (exactly once, MQTT 5 4.3.3)", p.Payload, next.Payload)
	case string(next.Payload):
	default:
		t.Fatalf("after the restart -b's first PUBLISH is %q; want %q", p.Payload, next.Payload)
	}
	awaitSeries(t, b, map[string]float64{droppedSeries: 1})
}

// The first durable membership (design.md, Delivery, Positions: "A group's
// first durable membership captures its existing obligations before the
// SUBACK."; "A restart ends volatile members' assignments. QoS 1 returns to
// the group. QoS 2 is dropped and counted."). A group whose only member, -a,
// ends its session with its connection stores nothing (A shared group: "its
// cursors are stored if any member is durable"); -a receives n at QoS 2 and
// sends no PUBREC. -b, holding a durable session, joins at QoS 2: the group
// is stored from its SUBACK, -a's assignment of n with it. The broker is
// killed at once and started again; -b resumes. n is dropped and counted,
// never sent to -b [MQTT-4.8.2-5]: once a publisher stores n+1, -b's first
// packet is n+1, and saguin_shares_dropped_total on $broadcast reads 1. A
// group stored without -a's assignment either sends n to -b, its cursor
// before n, or loses n uncounted, its cursor past it.
func TestShareFirstDurableMemberCapturesAssignments(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir()}
	b := support.OpsLaunch(t, o)
	topic := "capture/x"
	filter := shareOf("g", topic)
	a := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "capture-a", CleanStart: true}, support.At(2, filter))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "capture-pub", CleanStart: true})
	n := twice(topic, "n")
	published(t, pub, n)
	support.DeliveredAt(t, a, 2, topic, n.Payload)
	connectB := &harness.Connect{ClientID: "capture-b", CleanStart: true, Props: support.Durable}
	support.Subscribed(t, b.Addr, harness.V5, connectB, support.At(2, filter))

	support.Crash(t, b)
	b = support.OpsLaunch(t, o)
	bb := resumed(t, b.Addr, harness.V5, connectB)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "capture-pub", CleanStart: true})
	next := twice(topic, "n+1")
	published(t, pub, next)
	p := harness.Expect[*harness.Publish](t, bb)
	if !bytes.Equal(p.Payload, next.Payload) {
		t.Fatalf("after the restart -b's first PUBLISH is %q; want %q, -a's unfinished QoS 2 assignment of n dropped [MQTT-4.8.2-5]", p.Payload, next.Payload)
	}
	awaitSeries(t, b, map[string]float64{droppedSeries: 1})
}

// design.md, Store reads: "Before applying a result, the core checks
// whether its requesting owner and connection still exist and still own
// that request. An ended or superseded request has no further effects";
// Flows, Replies: "If the store confirms after that connection has ended
// or been taken over, what was stored stays and the reply is dropped";
// SUBSCRIBE, A shared group: "Joining an existing group preserves its
// cursors and returned deliveries"; "Each message goes to one member with
// room"; R8: "A store error stops the broker".
//
// On a stallable broker, two clients, -a and -b, each ending its session
// with its connection, and a publisher connect. With the store stalled, -a
// subscribes at QoS 1 to $share/g/b/#, a new group, and its PINGRESP shows
// the core has taken the SUBSCRIBE, whose write waits at the store; -b
// then subscribes to the same group, its PINGRESP likewise; -a's
// connection is lost, and saguin_connections{protocol="5"} counting -b and
// the publisher alone shows the core has taken its end, all before -a's
// write is answered. With the stall taken off, -b's SUBACK grants QoS 1,
// and a message the publisher stores on b/x goes to -b: the group -a's
// committed write stored stays, and -b's joining it is a store write the
// broker survives.
func TestShareJoinSurvivesTheFirstMembersEarlyEnd(t *testing.T) {
	s := launchStallable(t, &harness.Options{})
	filter := shareOf("g", "b/#")
	a, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-a"), CleanStart: true})
	bb, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-b"), CleanStart: true})
	pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	s.stall(t, true)
	join := &harness.Subscribe{ID: 1, Subs: support.At(1, filter)}
	for _, c := range []*harness.Client{a, bb} {
		if err := c.Send(join); err != nil {
			t.Fatalf("SUBSCRIBE %q: %v", filter, err)
		}
		if err := c.Ping(); err != nil {
			t.Fatalf("after SUBSCRIBE %q, with the store stalled: %v", filter, err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	awaitSeries(t, s.Broker, map[string]float64{connected5: 2})
	s.stall(t, false)
	p, err := bb.Await(func(p harness.Packet) bool { _, ok := p.(*harness.Suback); return ok })
	if err != nil {
		t.Fatalf("-b's SUBSCRIBE %q, made while -a's was at the store: waiting for its SUBACK: %v", filter, err)
	}
	if got := p.(*harness.Suback).Reasons; !bytes.Equal(got, []byte{1}) {
		t.Fatalf("-b's SUBACK % X; want 01, granted", got)
	}
	m := &harness.Publish{QoS: 1, Topic: "b/x", Payload: []byte("after")}
	support.PublishAll(t, pub, []*harness.Publish{m})
	support.DeliveredAt(t, bb, 1, m.Topic, m.Payload)
}
