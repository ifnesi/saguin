package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of records leaving for a bridge's peer (design.md, Bridges, Out;
// Delivery; Words: Missed; Restart, Restored; storage.md, bridge_rules,
// bridge_cursors and bridge_inflight; metrics.md, Bridges) whose waits are
// short. Each starts a broker of its own with a bridge to a test peer,
// harness.Peer, on loopback, whose CONNACK says what the peer allows: here
// the peer is the receiver. Local clients publish into channels; the peer
// records each PUBLISH the bridge sends it, with its receipt time, and
// answers each as the test scripts: PUBACK 0x00, a refusal, or nothing for
// a while. The test whose waits run to seconds, a Message Expiry's, is in
// the intervals package.

// hold is how long the peer holds an acknowledgement in the tests of what
// the bridge may have in flight: long beside the loopback and the broker's
// own turn, so a record the bridge sent where it should have waited would
// reach the peer within it.
const hold = 300 * time.Millisecond

// localClient connects an MQTT 5 client of its own, id, to b.
func localClient(t *testing.T, b *harness.Broker, id string) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true})
	return c
}

// publishedAt has c publish p and complete its exchange at its QoS: QoS 0
// sent, QoS 1 acknowledged with PUBACK 0x00, QoS 2 with PUBREC and PUBCOMP
// 0x00 (design.md, Flows, PUBLISH, the table).
func publishedAt(t *testing.T, c *harness.Client, p *harness.Publish) {
	t.Helper()
	switch p.QoS {
	case 0:
		if err := c.Send(p); err != nil {
			t.Fatalf("PUBLISH %q at QoS 0: %v", p.Topic, err)
		}
	case 1:
		support.Acknowledged(t, c, p)
	default:
		published(t, c, p)
	}
}

// eachOwn is a QoS 1 publication on each of topics, in order, each
// carrying its topic as its payload, so the peer and the database tell them
// apart.
func eachOwn(topics ...string) []*harness.Publish {
	ps := make([]*harness.Publish, len(topics))
	for i, topic := range topics {
		ps[i] = &harness.Publish{QoS: 1, Topic: topic, Payload: []byte(topic)}
	}
	return ps
}

// crossing is one record as the peer receives it: its topic there and its
// payload.
type crossing struct{ topic, payload string }

// byChannel groups ps, the PUBLISHes the peer received, by their topic's
// second level, which names the local channel each came from in the tests
// that use it (to/<channel>/...), each group in the order they came: the
// design orders deliveries within a channel, not across channels
// (design.md, Delivery, Order: "For a filter reaching several channels,
// order holds within each channel, not across them").
func byChannel(t *testing.T, ps []*harness.Publish) map[string][]crossing {
	t.Helper()
	got := map[string][]crossing{}
	for _, p := range ps {
		if p.QoS != 1 || p.Dup {
			t.Errorf("the peer received PUBLISH %q at QoS %d, DUP %v; want QoS 1, DUP 0", p.Topic, p.QoS, p.Dup)
		}
		levels := strings.SplitN(p.Topic, "/", 3)
		if len(levels) < 2 {
			t.Fatalf("the peer received PUBLISH %q; want a topic of the rule's template, to/$#", p.Topic)
		}
		got[levels[1]] = append(got[levels[1]], crossing{p.Topic, string(p.Payload)})
	}
	return got
}

// TestOutRuleStartsWhereANewSubscriptionWould: design.md, Bridges, Out:
// "each rule is a durable consumer of what its filter matches locally, with
// its own cursor."; "A new rule starts where a new subscription would on
// each channel, without a retained pass. That is the floor of an append
// channel or a queue, or its tail with start: tail, the current values of a
// latest channel, and the next message on $broadcast (SUBSCRIBE)."; storage.md,
// bridge_cursors: "This table holds an out rule's place in each channel its
// filter reaches. The rule is a durable consumer."; offset, "the next offset
// the rule is owed". MQTT 5 4.4: "Clients and Servers MUST NOT resend
// messages at any other time [MQTT-4.4.0-1]."
//
// A broker with no bridge holds the append channel ap (s/ap/#), the append
// channel tl (s/tl/#) starting new readers at the tail, and the latest
// channel kv (s/kv/#). A client stores s/ap/1 and s/ap/2; s/tl/1; s/kv/a
// twice, then s/kv/b; and, on $broadcast, s/b/1 retained and s/b/2. The
// broker stops and starts again with a bridge, starting, whose out rule
// takes s/# to to/$# at QoS 1. Its first link up, the peer receives, each
// answered PUBACK 0x00, ap's two records in order and kv's current values,
// to/kv/a's second value then to/kv/b: nothing of tl or $broadcast. Stored
// after, s/tl/2 and s/b/3 come next, each its channel's first, so none of
// the records before them on tl or $broadcast came. The peer drops the
// link; on the next, Session Present 1, the first PUBLISH is s/ap/3,
// stored then, DUP 0. Once the broker counts its PUBACK, the broker stops
// and starts again: on its link, the first PUBLISH is s/ap/4, stored then.
// Nothing acknowledged comes again.
func TestOutRuleStartsWhereANewSubscriptionWould(t *testing.T) {
	t.Parallel()
	o := &harness.Options{Dir: t.TempDir(), Operations: true, Channels: []harness.Channel{
		{Name: "ap", Type: "append", Filter: "s/ap/#"},
		{Name: "tl", Type: "append", Filter: "s/tl/#", Start: "tail"},
		{Name: "kv", Type: "latest", Filter: "s/kv/#"},
	}}
	b := support.Launch(t, o)
	support.PublishAll(t, localClient(t, b, "starting-before"), []*harness.Publish{
		{QoS: 1, Topic: "s/ap/1", Payload: []byte("ap 1")},
		{QoS: 1, Topic: "s/ap/2", Payload: []byte("ap 2")},
		{QoS: 1, Topic: "s/tl/1", Payload: []byte("tl 1")},
		{QoS: 1, Topic: "s/kv/a", Payload: []byte("kv a 1")},
		{QoS: 1, Topic: "s/kv/a", Payload: []byte("kv a 2")},
		{QoS: 1, Topic: "s/kv/b", Payload: []byte("kv b 1")},
		{QoS: 1, Retain: true, Topic: "s/b/1", Payload: []byte("b 1")},
		{QoS: 1, Topic: "s/b/2", Payload: []byte("b 2")},
	})
	stopped(t, b)

	peer := harness.ListenedPeer(t)
	o.Bridges = []harness.Bridge{support.BridgeTo("starting", peer, support.Out("s/#", "to/$#", 1))}
	b = support.Launch(t, o)
	l := support.Accepted(t, peer, support.Quiet)
	want := map[string][]crossing{
		"ap": {{"to/ap/1", "ap 1"}, {"to/ap/2", "ap 2"}},
		"kv": {{"to/kv/a", "kv a 2"}, {"to/kv/b", "kv b 1"}},
	}
	if got := byChannel(t, support.PeerTakes(t, l, 4)); !maps.EqualFunc(got, want, slices.Equal[[]crossing]) {
		t.Errorf("at the rule's first start, the peer received %v; want %v: ap from its floor, kv's current values, nothing of tl or $broadcast", got, want)
	}
	pub := localClient(t, b, "starting-after")
	support.PublishAll(t, pub, []*harness.Publish{
		{QoS: 1, Topic: "s/tl/2", Payload: []byte("tl 2")},
		{QoS: 1, Topic: "s/b/3", Payload: []byte("b 3")},
	})
	want = map[string][]crossing{"tl": {{"to/tl/2", "tl 2"}}, "b": {{"to/b/3", "b 3"}}}
	if got := byChannel(t, support.PeerTakes(t, l, 2)); !maps.EqualFunc(got, want, slices.Equal[[]crossing]) {
		t.Errorf("after the first start, the peer received %v; want %v: tl from its tail, $broadcast from the next message", got, want)
	}

	_ = l.Close() // the peer drops the link
	l = support.Accepted(t, peer, support.QuietResumed)
	again := &harness.Publish{QoS: 1, Topic: "s/ap/3", Payload: []byte("ap 3")}
	support.Acknowledged(t, pub, again)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "to/ap/3", 1, again.Payload)
	// Seven records acknowledged since this start: ap/3 is not one the
	// restart may send again until the broker has handled its PUBACK.
	support.AwaitAcknowledged(t, b, "starting", 7)

	stopped(t, b)
	b = support.Launch(t, o)
	l = support.Accepted(t, peer, support.QuietResumed)
	restarted := &harness.Publish{QoS: 1, Topic: "s/ap/4", Payload: []byte("ap 4")}
	support.Acknowledged(t, localClient(t, b, "starting-restarted"), restarted)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "to/ap/4", 1, restarted.Payload)
}

// TestOutRecordsCrossUnderTheirTemplateAtTheLowerQoS: design.md, Bridges:
// "In a topic template, $1, $2 and so on stand for the filter's + levels.
// Each must name a level the filter has and may occur anywhere in a level.
// The $# stands for the filter's # and must be the topic's whole last level.
// Where the filter's # matches no level, $# and the / before it are both
// left out."; "An outbound record crosses at the lower of its own QoS and
// the rule's, and never above the Maximum QoS the peer's CONNACK allows."
// MQTT 5 3.2.2.3.4: "If a Client receives a Maximum QoS from a Server, it
// MUST NOT send PUBLISH packets at a QoS level exceeding the Maximum QoS
// level specified [MQTT-3.2.2-11]."; config.md, bridges.<name>: "A topic
// template is required for in and out, and refused for both"; storage.md, bridge_rules: "For a
// both rule, the topic is ” and the topic is unchanged."
//
// A bridge, templated, has out rules taking a/+/+/# to x/$2-$1/$# at QoS 1
// and b/# to y/$# at QoS 0, and a both rule on c/# at QoS 1, whose inbound
// half the peer grants. One at a time, a client publishes, and the peer
// receives: a/k/m/n/o at QoS 1 as x/m-k/n/o at QoS 1; a/k/m at QoS 0 as
// x/m-k, at QoS 0, the # matching no level; a/k/m/z at QoS 2 as x/m-k/z at
// the rule's QoS 1; b/c at QoS 1 as y/c at its rule's QoS 0; c/d at QoS 1
// under its own topic, c/d, at QoS 1. The peer drops the link, and its next
// CONNACK has Maximum QoS 0: a/k/m/w, published at QoS 1, crosses as
// x/m-k/w at QoS 0.
func TestOutRecordsCrossUnderTheirTemplateAtTheLowerQoS(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	b := support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("templated", peer,
		support.Out("a/+/+/#", "x/$2-$1/$#", 1), support.Out("b/#", "y/$#", 0), harness.BridgeRule{Filter: "c/#", Direction: "both", QoS: 1})}})
	l, _, _ := support.LinkUp(t, peer, support.Quiet)
	pub := localClient(t, b, "templated-pub")
	for _, c := range []struct {
		p     *harness.Publish
		topic string
		qos   byte
	}{
		{&harness.Publish{QoS: 1, Topic: "a/k/m/n/o", Payload: []byte("deep")}, "x/m-k/n/o", 1},
		{&harness.Publish{QoS: 0, Topic: "a/k/m", Payload: []byte("no level for #")}, "x/m-k", 0},
		{&harness.Publish{QoS: 2, Topic: "a/k/m/z", Payload: []byte("above the rule's QoS")}, "x/m-k/z", 1},
		{&harness.Publish{QoS: 1, Topic: "b/c", Payload: []byte("a rule at QoS 0")}, "y/c", 0},
		{&harness.Publish{QoS: 1, Topic: "c/d", Payload: []byte("a both rule")}, "c/d", 1},
	} {
		publishedAt(t, pub, c.p)
		support.Crossed(t, support.PeerTakes(t, l, 1)[0], c.topic, c.qos, c.p.Payload)
	}

	_ = l.Close() // the peer drops the link
	l, _, _ = support.LinkUp(t, peer, support.QuietWith(true, harness.Property{ID: harness.MaximumQoS, Int: 0}))
	capped := &harness.Publish{QoS: 1, Topic: "a/k/m/w", Payload: []byte("above the peer's Maximum QoS")}
	support.Acknowledged(t, pub, capped)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "x/m-k/w", 0, capped.Payload)
}

// TestOutOneRecordPerTopicAndTheWindow: design.md, Bridges, Out: "One
// record per topic at the peer is in flight at a time. Across the whole
// connection, at most the lower of the peer's Receive Maximum and
// receive_maximum are in flight."; Delivery, One mechanism for every
// channel: "A message is in flight from its hand-off to the writer."
// config.md, bridges.<name>: receive_maximum, "the Receive Maximum sent to
// the peer, 1 to 65,535". MQTT 5 3.3.4: the Client "MUST NOT send more than
// Receive Maximum QoS 1 and QoS 2 PUBLISH packets for which it has not
// received PUBACK, PUBCOMP, or PUBREC with a Reason Code of 128 or greater
// from the Server [MQTT-3.3.4-7]."
//
// Per topic at the peer: a bridge's out rule takes u/+ to the one topic one
// at QoS 1. A client stores u/x, then u/y. The peer receives u/x's record
// on one and holds its PUBACK; u/y's comes after that PUBACK was sent.
//
// The window, once with the peer's CONNACK carrying Receive Maximum 2 and
// receive_maximum at its default, once with receive_maximum 2 and no
// Receive Maximum from the peer: an out rule takes w/# to w/$# at QoS 1,
// and a client stores w/1, w/2 and w/3. The peer receives two records and
// holds their PUBACKs; the third comes after the first PUBACK was sent.
func TestOutOneRecordPerTopicAndTheWindow(t *testing.T) {
	t.Parallel()
	t.Run("one per topic at the peer", func(t *testing.T) {
		t.Parallel()
		b, _, l := support.Outbound(t, &harness.Options{}, "per-topic", support.Quiet, support.Out("u/+", "one", 1))
		first := &harness.Publish{QoS: 1, Topic: "u/x", Payload: []byte("first")}
		second := &harness.Publish{QoS: 1, Topic: "u/y", Payload: []byte("second")}
		support.PublishAll(t, localClient(t, b, "per-topic-pub"), []*harness.Publish{first, second})
		got, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		support.Crossed(t, got, "one", 1, first.Payload)
		time.Sleep(hold) // the peer holding its PUBACK is the behaviour under test
		acked := support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: got.ID})
		next, a := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		support.Crossed(t, next, "one", 1, second.Payload)
		if !a.At.After(acked) {
			t.Errorf("u/y's record reached the peer %v before the PUBACK of u/x's, on the same topic there; want it after", acked.Sub(a.At))
		}
	})
	for _, c := range []struct {
		name string
		peer uint32 // the Receive Maximum of the peer's CONNACK, 0 for none
		own  int    // receive_maximum, 0 for config.md's default 20
	}{
		{"the peer's Receive Maximum", 2, 0},
		{"receive_maximum", 0, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			peer := harness.ListenedPeer(t)
			bridge := support.BridgeTo("windowed", peer, support.Out("w/#", "w/$#", 1))
			bridge.ReceiveMaximum = c.own
			b := support.Launch(t, &harness.Options{Bridges: []harness.Bridge{bridge}})
			ack := support.Quiet
			if c.peer != 0 {
				ack = support.QuietWith(false, harness.Property{ID: harness.ReceiveMaximum, Int: c.peer})
			}
			l := support.Accepted(t, peer, ack)
			support.PublishAll(t, localClient(t, b, "windowed-pub"), []*harness.Publish{
				{QoS: 1, Topic: "w/1", Payload: []byte("1")},
				{QoS: 1, Topic: "w/2", Payload: []byte("2")},
				{QoS: 1, Topic: "w/3", Payload: []byte("3")},
			})
			topics := make([]string, 0, 3)
			var first uint16
			for i := range 2 {
				p, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
				topics = append(topics, p.Topic)
				if i == 0 {
					first = p.ID
				}
			}
			time.Sleep(hold) // the peer holding its PUBACKs is the behaviour under test
			acked := support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: first})
			third, a := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
			if !a.At.After(acked) {
				t.Errorf("the third record reached the peer %v before the first PUBACK, two in flight; want it after", acked.Sub(a.At))
			}
			if topics = append(topics, third.Topic); !slices.Equal(slices.Sorted(slices.Values(topics)), []string{"w/1", "w/2", "w/3"}) {
				t.Errorf("the peer received %q; want w/1, w/2 and w/3 once each", topics)
			}
		})
	}
}

// bridgeInflight is one row of storage.md's bridge_inflight, as the
// operator reads it.
type bridgeInflight struct {
	packetID             uint16
	seq, channel, offset int64
	qos, phase           int
	rule                 int64
}

// inflightTo reads from db the bridge_inflight rows of the bridge named, in
// seq order (storage.md, bridge_inflight: "They are resent in seq order";
// bridges: "name | unique: the bridge's name").
func inflightTo(t *testing.T, db support.RowsQuerier, bridge string) []bridgeInflight {
	t.Helper()
	return support.Collect(t, db, `SELECT i.packet_id, i.seq, i.channel, i."offset", i.qos, i.phase, i.rule
		FROM bridge_inflight i JOIN bridges b ON b.id = i.bridge WHERE b.name = ? ORDER BY i.seq`, func(rows *sql.Rows, r *bridgeInflight) error {
		return rows.Scan(&r.packetID, &r.seq, &r.channel, &r.offset, &r.qos, &r.phase, &r.rule)
	}, bridge)
}

// ruleOf reads from db the id of the bridge named's rule r (storage.md,
// bridge_rules: "The tuple (bridge, filter, topic) is unique").
func ruleOf(t *testing.T, db *sql.DB, bridge string, r harness.BridgeRule) int64 {
	t.Helper()
	var id int64
	err := db.QueryRowContext(context.Background(), `SELECT r.id FROM bridge_rules r JOIN bridges b ON b.id = r.bridge
		WHERE b.name = ? AND r.filter = ? AND r.topic = ?`, bridge, r.Filter, r.Topic).Scan(&id)
	if err != nil {
		t.Fatalf("reading the id of %s's rule %s -> %s: %v", bridge, r.Filter, r.Topic, err)
	}
	return id
}

// cursorOf reads from db rule's place on $broadcast, 0 where it has none
// (storage.md, bridge_cursors: offset, "the next offset the rule is owed";
// Ids: "The $broadcast channel is channel 1").
func cursorOf(t *testing.T, db *sql.DB, rule int64) int64 {
	t.Helper()
	var offset int64
	err := db.QueryRowContext(context.Background(), `SELECT "offset" FROM bridge_cursors WHERE rule = ? AND channel = 1`, rule).Scan(&offset)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("reading rule %d's cursor on $broadcast: %v", rule, err)
	}
	return offset
}

// sentStored reads l's next n arrivals, each a QoS 1 PUBLISH the peer
// leaves unanswered, failing the test unless db holds each, as it arrives,
// in bridge_inflight for the bridge named: under its packet identifier, on
// $broadcast (channel 1) at the offset offsets gives its payload, QoS 1,
// phase 1, for rule, each seq above the one before. It returns them in the
// order they came.
func sentStored(t *testing.T, l *harness.PeerLink, db *sql.DB, bridge string, rule int64, offsets map[string]int64, n int) []*harness.Publish {
	t.Helper()
	sent := make([]*harness.Publish, n)
	var seq int64
	for i := range sent {
		p, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		if p.QoS != 1 || p.Dup {
			t.Fatalf("the peer received PUBLISH %q at QoS %d, DUP %v; want a first sending at QoS 1", p.Topic, p.QoS, p.Dup)
		}
		rows := inflightTo(t, db, bridge)
		j := slices.IndexFunc(rows, func(r bridgeInflight) bool { return r.packetID == p.ID })
		if j < 0 {
			t.Fatalf("PUBLISH %q under packet identifier %d reached the peer; bridge_inflight holds %+v, not it: a record is stored before it is sent", p.Topic, p.ID, rows)
		}
		want := bridgeInflight{packetID: p.ID, seq: rows[j].seq, channel: 1, offset: offsets[string(p.Payload)], qos: 1, phase: 1, rule: rule}
		if rows[j] != want || rows[j].seq <= seq {
			t.Errorf("PUBLISH %q is in bridge_inflight as %+v; want %+v, its seq above %d", p.Topic, rows[j], want, seq)
		}
		seq = rows[j].seq
		sent[i] = p
	}
	return sent
}

// resentAgain reads l's next arrivals, one for each of first, failing the
// test unless each is that PUBLISH sent again, in their order, under its
// own packet identifier and with DUP 1, and answers each with PUBACK 0x00.
func resentAgain(t *testing.T, l *harness.PeerLink, first ...*harness.Publish) {
	t.Helper()
	for _, f := range first {
		p, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		if p.ID != f.ID || !p.Dup || p.QoS != f.QoS || p.Topic != f.Topic || !bytes.Equal(p.Payload, f.Payload) {
			t.Errorf("the peer received PUBLISH %q of %q, QoS %d, packet identifier %d, DUP %v; want %q of %q sent again, QoS %d, packet identifier %d, DUP 1",
				p.Topic, p.Payload, p.QoS, p.ID, p.Dup, f.Topic, f.Payload, f.QoS, f.ID)
		}
		support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: p.ID})
	}
}

// TestOutQoS1InFlightIsStoredAndResent: storage.md, bridge_inflight: "This
// table holds records in flight to a bridge's peer, by packet identifier.
// They are resent in seq order."; phase, "1 PUBLISH sent or prepared";
// rule, "the id of the rule whose cursor its acknowledgement moves";
// bridge_cursors: offset, "the next offset the rule is owed"; design.md,
// Bridges, Out: "each rule is a durable consumer of what its filter matches
// locally, with its own cursor."; Delivery, Preparation: "A selected QoS 1
// or 2 delivery has its packet identifier and, for a durable session, its
// stored exchange."; Positions: "Deleting a durable session's settled
// exchange row must commit with its position beyond that record"; Restart,
// Restored: "The broker also restores $share cursors, retained flags, queue
// attempt counts and next retry times, bridges' in-flight windows and each
// channel's offset counter." MQTT 5 4.4: "When a Client reconnects with
// Clean Start set to 0 and a session is present, both the Client and Server
// MUST resend any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL
// packets using their original Packet Identifiers [MQTT-4.4.0-1]"; 3.3.1.1:
// "The DUP flag MUST be set to 1 by the Client or Server when it attempts to
// re-deliver a PUBLISH packet [MQTT-3.3.1-1]."
//
// A bridge, resending, has an out rule taking w/# to w/$# at QoS 1. A
// client stores w/1, w/2 and w/3. As each reaches the peer, unanswered, the
// database already holds it in bridge_inflight, under its packet
// identifier, its record's offset, QoS 1, phase 1 and the rule's id, in
// seq order. The peer acknowledges the second to come and drops the link.
// On the next, Session Present 1, the first and third come again, in that
// order, each with its packet identifier and DUP 1, each acknowledged, then
// w/4, stored then. Its PUBACK sent, bridge_inflight comes to hold none of
// the bridge's rows, and the rule's cursor on $broadcast w/4's offset plus
// one. The client stores w/5 and w/6, which reach the peer unanswered; the
// broker is killed. Started again on its files, on its link, Session
// Present 1, the two come again, in that order, with their packet
// identifiers and DUP 1.
func TestOutQoS1InFlightIsStoredAndResent(t *testing.T) {
	t.Parallel()
	const name = "resending"
	rule := support.Out("w/#", "w/$#", 1)
	peer := harness.ListenedPeer(t)
	o := &harness.Options{Dir: t.TempDir(), Bridges: []harness.Bridge{support.BridgeTo(name, peer, rule)},
		Channels: []harness.Channel{elsewhere}}
	b := support.Launch(t, o)
	db := support.OpenDB(t, b.DB)
	l := support.Accepted(t, peer, support.Quiet)
	pub := localClient(t, b, "resending-pub")
	support.PublishAll(t, pub, eachOwn("w/1", "w/2", "w/3"))
	id := ruleOf(t, db, name, rule)
	sent := sentStored(t, l, db, name, id, offsetsByPayload(t, b.DB), 3)
	support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: sent[1].ID})

	_ = l.Close() // the peer drops the link
	l = support.Accepted(t, peer, support.QuietResumed)
	resentAgain(t, l, sent[0], sent[2])
	last := eachOwn("w/4")[0]
	support.Acknowledged(t, pub, last)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "w/4", 1, last.Payload)
	next := offsetsByPayload(t, b.DB)[string(last.Payload)] + 1
	type progress struct {
		rows   int
		cursor int64
	}
	awaitState(t, fmt.Sprintf("none of %s's rows in flight and its rule's cursor at %d", name, next), func() progress {
		committed(t, pub, "elsewhere/barrier") // a commit for the position to go with
		return progress{len(inflightTo(t, db, name)), cursorOf(t, db, id)}
	}, func(p progress) bool { return p.rows == 0 && p.cursor == next })

	support.PublishAll(t, pub, eachOwn("w/5", "w/6"))
	sent = sentStored(t, l, db, name, id, offsetsByPayload(t, b.DB), 2)
	support.Crash(t, b)
	support.Launch(t, o)
	resentAgain(t, support.Accepted(t, peer, support.QuietResumed), sent...)
}

// TestOutSessionPresent0SendsUnacknowledgedAgainAsNew: design.md, Bridges,
// Out: "When the peer's CONNACK has Session Present 0, the bridge discards
// its in-flight state for the peer (MQTT-3.2.2-5). Its unacknowledged
// records are then sent again as new messages, from the cursor." MQTT 5
// 3.2.2.1.1: "If the Client does have Session State and receives Session
// Present set to 0 it MUST discard its Session State if it continues with
// the Network Connection [MQTT-3.2.2-5]."; 4.3.2: the sender "MUST send a
// PUBLISH packet containing this Packet Identifier with QoS 1 and DUP flag
// set to 0 [MQTT-4.3.2-2]."
//
// A bridge, forgotten, has an out rule taking w/# to w/$# at QoS 1. A
// client stores w/1, w/2 and w/3; the three reach the peer, which
// acknowledges w/2's alone and drops the link. Its next CONNACK has Session
// Present 0, and the client stores w/4: the peer receives w/1, w/3 and w/4,
// in that order, each DUP 0.
func TestOutSessionPresent0SendsUnacknowledgedAgainAsNew(t *testing.T) {
	t.Parallel()
	b, peer, l := support.Outbound(t, &harness.Options{}, "forgotten", support.Quiet, support.Out("w/#", "w/$#", 1))
	pub := localClient(t, b, "forgotten-pub")
	ps := eachOwn("w/1", "w/2", "w/3")
	support.PublishAll(t, pub, ps)
	for range ps {
		p, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		if p.Topic == "w/2" {
			support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: p.ID})
		}
	}

	_ = l.Close() // the peer drops the link
	l = support.Accepted(t, peer, support.Quiet)
	last := eachOwn("w/4")[0]
	support.Acknowledged(t, pub, last)
	got := support.PeerTakes(t, l, 3)
	for i, want := range []*harness.Publish{ps[0], ps[2], last} {
		support.Crossed(t, got[i], want.Topic, 1, want.Payload)
	}
}

// TestOutQoS2CrossesAtQoS2: design.md, Bridges: "An outbound record crosses
// at the lower of its own QoS and the rule's, and never above the Maximum
// QoS the peer's CONNACK allows."; storage.md, bridge_rules: "This table
// holds each rule that sends to the peer, either an out rule or the
// outbound half of a both rule". MQTT 5 3.2.2.3.4: "If the Maximum QoS is
// absent, the Client uses a Maximum QoS of 2."; 4.3.3: the sender "MUST
// send a PUBLISH packet containing this Packet Identifier with QoS 2 and
// DUP flag set to 0 [MQTT-4.3.3-2]"; it "MUST send a PUBREL packet when it
// receives a PUBREC packet from the receiver with a Reason Code value less
// than 0x80 [MQTT-4.3.3-4]".
//
// A bridge, exactly-once, has an out rule taking q/# to x/$# at QoS 2 and a
// both rule on c/# at QoS 2, whose inbound half the peer grants; the peer's
// CONNACK carries no Maximum QoS. One at a time, a client publishes at QoS
// 2, and the peer receives, each exchange complete before the next: q/1 as
// x/1 at QoS 2; c/1 under its own topic, c/1, at QoS 2.
func TestOutQoS2CrossesAtQoS2(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	b := support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("exactly-once", peer,
		support.Out("q/#", "x/$#", 2), harness.BridgeRule{Filter: "c/#", Direction: "both", QoS: 2})}})
	l, _, _ := support.LinkUp(t, peer, support.Quiet)
	pub := localClient(t, b, "exactly-once-pub")
	for _, c := range []struct {
		p     *harness.Publish
		topic string
	}{
		{&harness.Publish{QoS: 2, Topic: "q/1", Payload: []byte("an out rule")}, "x/1"},
		{&harness.Publish{QoS: 2, Topic: "c/1", Payload: []byte("a both rule's outbound half")}, "c/1"},
	} {
		published(t, pub, c.p)
		support.Crossed(t, support.PeerTakes(t, l, 1)[0], c.topic, 2, c.p.Payload)
	}
}

// exchange is an outgoing QoS 2 exchange as the peer saw it on a link: the
// bridge's PUBLISH, and released where the peer answered it with PUBREC
// 0x00 and received the bridge's PUBREL, whose PUBCOMP it withholds.
type exchange struct {
	p        *harness.Publish
	released bool
}

// inFlightQoS2 has c store a QoS 2 record on each of w/1, w/2 and w/3, each
// carrying its topic as its payload, which a bridge's out rule taking w/#
// to w/$# at QoS 2 sends to the peer on l, each a first sending at QoS 2.
// The peer answers the second PUBLISH to come with PUBREC 0x00 and receives
// the bridge's PUBREL under its packet identifier (MQTT-4.3.3-4), and
// leaves the other two unanswered. It returns the three exchanges in the
// order their PUBLISHes came.
func inFlightQoS2(t *testing.T, l *harness.PeerLink, c *harness.Client) []exchange {
	t.Helper()
	for _, topic := range []string{"w/1", "w/2", "w/3"} {
		published(t, c, &harness.Publish{QoS: 2, Topic: topic, Payload: []byte(topic)})
	}
	xs := make([]exchange, 3)
	for i := range xs {
		p, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		if p.QoS != 2 || p.Dup {
			t.Fatalf("the peer received PUBLISH %q at QoS %d, DUP %v; want a first sending at QoS 2", p.Topic, p.QoS, p.Dup)
		}
		xs[i].p = p
	}
	support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBREC, ID: xs[1].p.ID})
	support.PeerAck(t, l, harness.PUBREL, xs[1].p.ID, 0x00)
	xs[1].released = true
	return xs
}

// takenUpAgain reads l's next arrivals, one for each of xs, failing the test
// unless each is its exchange taken up again, in xs's order: PUBREL 0x00
// under its packet identifier where the peer had received it, else its
// PUBLISH again under that packet identifier with DUP 1. The peer then
// completes each: PUBCOMP 0x00 to a PUBREL; PUBREC 0x00 to a PUBLISH, and
// PUBCOMP 0x00 to the bridge's PUBREL that must follow.
func takenUpAgain(t *testing.T, l *harness.PeerLink, xs ...exchange) {
	t.Helper()
	for _, x := range xs {
		a, err := l.Next(harness.DefaultTimeout)
		if err != nil {
			t.Fatalf("awaiting the exchange of %q under packet identifier %d taken up again: %v", x.p.Topic, x.p.ID, err)
		}
		var again bool
		switch p := a.Packet.(type) {
		case *harness.Ack:
			again = x.released && p.Kind == harness.PUBREL && p.ID == x.p.ID && p.Reason == 0x00
		case *harness.Publish:
			again = !x.released && p.ID == x.p.ID && p.Dup && p.QoS == 2 && p.Topic == x.p.Topic && bytes.Equal(p.Payload, x.p.Payload)
		}
		switch {
		case again:
		case x.released:
			t.Fatalf("the peer received %s; want PUBREL 0x00 to %q under its packet identifier %d, sent again", a, x.p.Topic, x.p.ID)
		default:
			t.Fatalf("the peer received %s; want PUBLISH %q sent again at QoS 2 under its packet identifier %d with DUP 1", a, x.p.Topic, x.p.ID)
		}
	}
	for _, x := range xs {
		if !x.released {
			support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBREC, ID: x.p.ID})
			support.PeerAck(t, l, harness.PUBREL, x.p.ID, 0x00)
		}
		support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBCOMP, ID: x.p.ID})
	}
}

// TestOutQoS2ResumesInOrder: design.md, Bridges, Out: "each rule is a
// durable consumer of what its filter matches locally, with its own
// cursor."; Delivery, Positions: "Before handing a QoS 1 or QoS 2 delivery
// to a durable session's writer, the core stores its packet identifier, its
// order (seq), its record and its phase."; "A QoS 2 delivery stores its
// PUBREC before its PUBREL goes to the writer."; Restart, Restored: "The
// broker also restores [...] bridges' in-flight windows"; storage.md,
// bridge_inflight: "They are resent in seq order."; phase, "1 PUBLISH sent
// or prepared, 2 PUBREC received and PUBREL owed". MQTT 5 4.4: "When a
// Client reconnects with Clean Start set to 0 and a session is present,
// both the Client and Server MUST resend any unacknowledged PUBLISH packets
// (where QoS > 0) and PUBREL packets using their original Packet
// Identifiers [MQTT-4.4.0-1]"; 3.3.1.1: "The DUP flag MUST be set to 1 by
// the Client or Server when it attempts to re-deliver a PUBLISH packet
// [MQTT-3.3.1-1]."; 4.3.3: the sender "MUST NOT re-send the PUBLISH once it
// has sent the corresponding PUBREL packet [MQTT-4.3.3-6]."
//
// A bridge's out rule takes w/# to w/$# at QoS 2. A client stores w/1, w/2
// and w/3, and the peer receives the three; it answers the second with
// PUBREC and receives its PUBREL, and leaves the others unanswered. Then,
// once the peer drops the link, and once the broker is killed and started
// again on its files: on the next link, Session Present 1, the peer
// receives the first PUBLISH again, the second's PUBREL, then the third
// PUBLISH again, each under its packet identifier, each PUBLISH with DUP 1;
// and each exchange completes.
func TestOutQoS2ResumesInOrder(t *testing.T) {
	t.Parallel()
	rule := support.Out("w/#", "w/$#", 2)
	t.Run("over a new link", func(t *testing.T) {
		t.Parallel()
		b, peer, l := support.Outbound(t, &harness.Options{}, "resuming", support.Quiet, rule)
		xs := inFlightQoS2(t, l, localClient(t, b, "resuming-pub"))
		_ = l.Close() // the peer drops the link
		takenUpAgain(t, support.Accepted(t, peer, support.QuietResumed), xs...)
	})
	t.Run("across a restart", func(t *testing.T) {
		t.Parallel()
		peer := harness.ListenedPeer(t)
		o := &harness.Options{Dir: t.TempDir(), Bridges: []harness.Bridge{support.BridgeTo("restarting", peer, rule)}}
		b := support.Launch(t, o)
		xs := inFlightQoS2(t, support.Accepted(t, peer, support.Quiet), localClient(t, b, "restarting-pub"))
		support.Crash(t, b)
		support.Launch(t, o)
		takenUpAgain(t, support.Accepted(t, peer, support.QuietResumed), xs...)
	})
}

// TestOutSessionPresent0SendsQoS2AgainAsNew: design.md, Bridges, Out: "When
// the peer's CONNACK has Session Present 0, the bridge discards its
// in-flight state for the peer (MQTT-3.2.2-5). Its unacknowledged records
// are then sent again as new messages, from the cursor."; Delivery,
// Positions: "an acknowledgement, PUBACK, PUBCOMP or a refusing PUBREC,
// settles its own delivery (MQTT-4.4.0-2)."; storage.md, bridge_cursors:
// offset, "the next offset the rule is owed". MQTT 5 3.2.2.1.1: "If the
// Client does have Session State and receives Session Present set to 0 it
// MUST discard its Session State if it continues with the Network
// Connection [MQTT-3.2.2-5]."; 4.3.3: the sender "MUST send a PUBLISH
// packet containing this Packet Identifier with QoS 2 and DUP flag set to 0
// [MQTT-4.3.3-2]."
//
// A bridge's out rule takes w/# to w/$# at QoS 2. A client stores w/1, w/2
// and w/3, and the peer receives the three; it answers the second with
// PUBREC and receives its PUBREL, which it leaves without PUBCOMP, and
// leaves the others unanswered: none is settled. The peer drops the link,
// and its next CONNACK has Session Present 0: the peer receives the three
// again, in their order, each a new PUBLISH at QoS 2 with DUP 0, the second
// included, and each exchange completes.
func TestOutSessionPresent0SendsQoS2AgainAsNew(t *testing.T) {
	t.Parallel()
	b, peer, l := support.Outbound(t, &harness.Options{}, "forgotten-qos2", support.Quiet, support.Out("w/#", "w/$#", 2))
	xs := inFlightQoS2(t, l, localClient(t, b, "forgotten-qos2-pub"))
	_ = l.Close() // the peer drops the link
	got := support.PeerTakes(t, support.Accepted(t, peer, support.Quiet), len(xs))
	for i, x := range xs {
		support.Crossed(t, got[i], x.p.Topic, 2, x.p.Payload)
	}
}

// TestOutReplayInterrupted: design.md, Bridges: "A bridge uses MQTT 5,
// Clean Start 0 (1 while a replay is owed)"; "A CONNACK with Session Present
// 0 starts a replay of the bridge's state for the peer. Inbound QoS 2
// records awaiting PUBREL enter their channels (In). Unacknowledged outbound
// records are prepared again as new (Out). Before the replay starts, the
// bridge stores that a replay is owed. The replay's last step clears that
// mark in the same transaction. Until the mark is cleared, the bridge
// neither subscribes at the peer nor sends it a record. Every CONNECT it
// makes until then has Clean Start 1, so the peer answers Session Present 0
// again and the replay runs again. When the mark cannot be stored, the link
// ends. The bridge keeps the mark in memory alone until it is stored.";
// Bridges, Out: "Its unacknowledged records are then
// sent again as new messages, from the cursor."; Delivery, Positions: "an
// acknowledgement, PUBACK, PUBCOMP or a refusing PUBREC, settles its own
// delivery (MQTT-4.4.0-2)." MQTT 5 4.3.3: the sender "MUST send a PUBLISH
// packet containing this Packet Identifier with QoS 2 and DUP flag set to 0
// [MQTT-4.3.3-2]."
//
// On a filesystem of its own, a bridge, replaying-out, has an out rule
// taking w/# to w/$# at QoS 2. A client stores w/1, w/2 and w/3, and the
// peer receives the three; it answers the second with PUBREC and receives
// its PUBREL, and leaves the others unanswered. A ballast then takes every
// byte left, and the peer drops the link; it answers the next CONNECT with
// Session Present 0, and, the replay's mark finding no room, the bridge
// ends that link. The ballast emptied, the next CONNECT has Clean Start 1;
// answered with Session Present 0, the peer receives the three again, in
// their order, each a new PUBLISH at QoS 2 with DUP 0, and no PUBREL. Before
// the peer answers them, the rule's cursor on $broadcast is not past w/1's
// offset; once each exchange completes, it comes to w/3's offset plus one.
func TestOutReplayInterrupted(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	const name = "replaying-out"
	rule := support.Out("w/#", "w/$#", 2)
	b, peer, l := support.Outbound(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), Channels: []harness.Channel{elsewhere}},
		name, support.Quiet, rule)
	pub := localClient(t, b, "replaying-out-pub")
	xs := inFlightQoS2(t, l, pub)

	l, c, ballast := replayInterrupted(t, disk, peer, l)
	if !c.CleanStart {
		t.Errorf("the CONNECT after the replay's mark found no room has Clean Start 0; want 1 while a replay is owed")
	}
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	support.PeerSends(t, l, support.Quiet)
	got := make([]*harness.Publish, len(xs))
	for i, x := range xs {
		got[i], _ = harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
		support.Crossed(t, got[i], x.p.Topic, 2, x.p.Payload)
	}
	db := support.OpenDB(t, b.DB)
	id := ruleOf(t, db, name, rule)
	offsets := offsetsByPayload(t, b.DB)
	committed(t, pub, "elsewhere/barrier") // a commit any moved cursor would go with
	if cursor := cursorOf(t, db, id); cursor > offsets["w/1"] {
		t.Errorf("before the peer answered the three sent again, the rule's cursor on $broadcast is %d; want it not past w/1's offset %d", cursor, offsets["w/1"])
	}
	for _, p := range got {
		support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBREC, ID: p.ID})
		support.PeerAck(t, l, harness.PUBREL, p.ID, 0x00)
		support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBCOMP, ID: p.ID})
	}
	cursorComesTo(t, db, pub, id, offsets["w/3"]+1, "the three settled")
}

// elsewhere is an append channel no bridge rule's filter reaches, for a
// test's commit barriers (committed), so they leave $broadcast's offsets,
// and a rule's cursor there, as they were.
var elsewhere = harness.Channel{Name: "other", Type: "append", Filter: "elsewhere/#"}

// cursorComesTo waits until db holds rule's cursor on $broadcast at want, 0
// for no cursor, a commit following each look (committed, by c on
// elsewhere): a cursor is saved with the next commit (design.md, Delivery,
// Positions: "A position is saved with the next commit").
func cursorComesTo(t *testing.T, db *sql.DB, c *harness.Client, rule, want int64, when string) {
	t.Helper()
	awaitState(t, fmt.Sprintf("%s, rule %d's cursor on $broadcast at %d", when, rule, want), func() int64 {
		committed(t, c, "elsewhere/barrier")
		return cursorOf(t, db, rule)
	}, func(offset int64) bool { return offset == want })
}

// anyLineNaming returns the lines of log, at any level, that name each of
// names.
func anyLineNaming(log string, names ...string) []string {
	var lines []string
	for _, level := range []string{"DEBUG", "INFO", "WARN", "ERROR"} {
		for _, line := range testkit.LogLines(log, level) {
			if namesAll(line, names...) {
				lines = append(lines, line)
			}
		}
	}
	return lines
}

// TestRemovedRulesExchanges: design.md, Bridges, Out: "A removed rule's
// unfinished exchanges are finished with the peer when its CONNACK has
// Session Present 1 (MQTT-4.4.0-1), then deleted, moving no cursor. When it
// has Session Present 0, they are deleted unsent."; Restart, A changed
// configuration: "The cursors of a removed bridge or rule are deleted at
// start and logged."; Bridges, Logging: "Every bridge line names its
// bridge."; storage.md, bridge_rules: "A removed rule's cursors are deleted
// at start (Restart). Its row stays while an in-flight row names it.";
// bridge_inflight: rule, "the id of the rule whose cursor its
// acknowledgement moves". MQTT 5 4.4: "When a Client reconnects with Clean
// Start set to 0 and a session is present, both the Client and Server MUST
// resend any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL
// packets using their original Packet Identifiers [MQTT-4.4.0-1]".
//
// A bridge, pruned, has out rules taking w/# to w/$# and k/# to k/$#, each
// at QoS 2. A client stores w/0, which crosses and completes, and the w/#
// rule's cursor on $broadcast comes to w/0's offset plus one. The client
// stores w/1, w/2 and w/3; the peer answers the second with PUBREC and
// receives its PUBREL, and leaves the others unanswered. The broker stops
// and starts again, logging at debug, without the w/# rule: the rule's
// cursor is gone, and the log holds a line naming the bridge and w/#. Then:
//   - on a link with Session Present 1, the peer receives the first PUBLISH
//     again, the second's PUBREL, then the third PUBLISH again, each under
//     its packet identifier, each PUBLISH with DUP 1, and each exchange
//     completes;
//   - on a link with Session Present 0, the peer receives none of them.
//
// Either way, the next packet the peer receives is the PUBLISH of k/1,
// stored then, which completes; and the database comes to hold no
// in-flight row of the w/# rule, and still no cursor of it.
func TestRemovedRulesExchanges(t *testing.T) {
	t.Parallel()
	const name = "pruned"
	gone, kept := support.Out("w/#", "w/$#", 2), support.Out("k/#", "k/$#", 2)
	for _, c := range []struct {
		name    string
		resumed bool
	}{
		{"Session Present 1", true},
		{"Session Present 0", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			peer := harness.ListenedPeer(t)
			o := &harness.Options{Dir: t.TempDir(), Bridges: []harness.Bridge{support.BridgeTo(name, peer, gone, kept)}, Channels: []harness.Channel{elsewhere}}
			b := support.Launch(t, o)
			db := support.OpenDB(t, b.DB)
			l := support.Accepted(t, peer, support.Quiet)
			pub := localClient(t, b, "pruned-pub")
			first := &harness.Publish{QoS: 2, Topic: "w/0", Payload: []byte("w/0")}
			published(t, pub, first)
			support.Crossed(t, support.PeerTakes(t, l, 1)[0], "w/0", 2, first.Payload)
			rule := ruleOf(t, db, name, gone)
			cursorComesTo(t, db, pub, rule, offsetsByPayload(t, b.DB)[string(first.Payload)]+1, "w/0 settled")
			xs := inFlightQoS2(t, l, pub)
			stopped(t, b)

			o.Bridges, o.LogLevel = []harness.Bridge{support.BridgeTo(name, peer, kept)}, "debug"
			b = support.Launch(t, o)
			db = support.OpenDB(t, b.DB)
			pub = localClient(t, b, "pruned-pub")
			cursorComesTo(t, db, pub, rule, 0, "started without the rule")
			awaitState(t, fmt.Sprintf("a line naming %s and %s", name, gone.Filter), func() []string {
				return anyLineNaming(b.Log(), name, gone.Filter)
			}, func(lines []string) bool { return len(lines) > 0 })
			if c.resumed {
				l = support.Accepted(t, peer, support.QuietResumed)
				takenUpAgain(t, l, xs...)
			} else {
				l = support.Accepted(t, peer, support.Quiet)
			}
			next := &harness.Publish{QoS: 2, Topic: "k/1", Payload: []byte("k/1")}
			published(t, pub, next)
			support.Crossed(t, support.PeerTakes(t, l, 1)[0], "k/1", 2, next.Payload)
			awaitState(t, fmt.Sprintf("no in-flight row of rule %d", rule), func() []bridgeInflight {
				committed(t, pub, "elsewhere/barrier")
				return slices.DeleteFunc(inflightTo(t, db, name), func(r bridgeInflight) bool { return r.rule != rule })
			}, func(rows []bridgeInflight) bool { return len(rows) == 0 })
			if offset := cursorOf(t, db, rule); offset != 0 {
				t.Errorf("the removed rule %d has a cursor on $broadcast at %d; want none: finishing its exchanges moves no cursor", rule, offset)
			}
		})
	}
}

// saguinIDs is the values of p's saguin-id User Properties, in order.
func saguinIDs(p *harness.Publish) []string { return p.Props.User("saguin-id") }

// withoutSaguinID is ps without its saguin-id User Properties.
func withoutSaguinID(ps harness.Properties) harness.Properties {
	return slices.DeleteFunc(slices.Clone(ps), func(p harness.Property) bool { return p.ID == harness.UserProperty && p.Str == "saguin-id" })
}

// expectOnlySaguinID fails the test unless got, sent's record as the peer
// received it, carries one saguin-id, id, beside sent's own properties
// unaltered, User Properties in their order, and no other saguin- property.
func expectOnlySaguinID(t *testing.T, got, sent *harness.Publish, id string) {
	t.Helper()
	if ids := saguinIDs(got); !slices.Equal(ids, []string{id}) {
		t.Errorf("%q reached the peer with saguin-id %q; want once its record's id %q", got.Topic, ids, id)
	}
	for _, p := range got.Props {
		if p.ID == harness.UserProperty && strings.HasPrefix(p.Str, "saguin-") && p.Str != "saguin-id" {
			t.Errorf("%q reached the peer carrying %s=%q; want no saguin- property but saguin-id", got.Topic, p.Str, p.Value)
		}
	}
	if diff := sameProperties(withoutSaguinID(got.Props), withoutSaguinID(sent.Props)); diff != "" {
		t.Errorf("%q reached the peer with %s", got.Topic, diff)
	}
}

// TestOutOnlySaguinIDCrosses: design.md, Bridges: "A record leaves with no
// saguin- property except saguin-id. Every record a bridge sends carries
// saguin-id with its record's id, $broadcast records included. RETAIN
// crosses only on $broadcast records. To a peer whose CONNACK has Retain
// Available 0, a retained record crosses with RETAIN 0 and at QoS 0, so no
// resend of it can differ from its first send, and the first such record on
// each link is one WARN line."; "An outbound record crosses at the lower of
// its own QoS and the rule's"; "Every bridge line names its bridge.";
// Delivery, Positions: "QoS 0 settles on successful writing."; "A position
// is saved with the next commit"; storage.md, bridge_inflight: "This table
// holds records in flight to a bridge's peer, by packet identifier.";
// Sagüin properties: "The value of saguin-id is the publisher's own where it
// set one, kept as sent. Otherwise the broker makes a UUIDv7." MQTT 5
// 3.2.2.3.5: "A Client receiving Retain Available set to 0 from the Server
// MUST NOT send a PUBLISH packet with the RETAIN flag set to 1
// [MQTT-3.2.2-14]."; 2.2.1: "A PUBLISH packet MUST NOT contain a Packet
// Identifier if its QoS value is set to 0 [MQTT-2.2.1-2]."; 3.3.2.3.7: the
// Server "MUST maintain the order of User Properties when forwarding the
// Application Message [MQTT-3.3.2-18]."
//
// A broker holds the append channel ev (ev/#); its bridge, stripped, has out
// rules taking ev/# to x/ev/$# and bc/# to x/bc/$#, each at QoS 1. One at a
// time, a client stores: ev/1, retained, carrying a User Property, a
// Content Type and saguin-id mine-1; bc/1, retained, with the User Property
// and Content Type; bc/2. The peer receives ev/1 with RETAIN 0, bc/1 with
// RETAIN 1 and bc/2 with RETAIN 0, each at QoS 1, the rule's, carrying its
// publisher's properties unaltered and one saguin-id, its record's id,
// mine-1 for ev/1, and no other saguin- property: no saguin-offset,
// -timestamp or -channel. The peer drops the link; its next CONNACK has
// Retain Available 0. The client stores bc/3, retained, bc/6, not retained,
// and bc/4, retained, each at QoS 1. bc/3 and bc/4 reach the peer with
// RETAIN 0 at QoS 0, so with no packet identifier: as each arrives,
// bridge_inflight holds no row of the bridge's for its record, and the bc/#
// rule's cursor on $broadcast comes to pass its offset once the next commit
// follows its writing. bc/6 crosses at QoS 1 on that same link. The peer
// drops that link too; on the next, Retain Available 0 again, bc/5,
// retained, comes with RETAIN 0 at QoS 0. Once the broker has stopped, its
// log holds two WARN lines naming the bridge and Retain Available: one for
// each link, the first such record on it.
func TestOutOnlySaguinIDCrosses(t *testing.T) {
	t.Parallel()
	const name = "stripped"
	b, peer, l := support.Outbound(t, &harness.Options{Channels: []harness.Channel{{Name: "ev", Type: "append", Filter: "ev/#"}}}, name, support.Quiet,
		support.Out("ev/#", "x/ev/$#", 1), support.Out("bc/#", "x/bc/$#", 1))
	pub := localClient(t, b, "stripped-pub")
	own := harness.Properties{harness.User("k", "v"), {ID: harness.ContentType, Str: "text/plain"}}
	for _, c := range []struct {
		p      *harness.Publish
		topic  string
		retain bool
	}{
		{&harness.Publish{QoS: 1, Retain: true, Topic: "ev/1", Payload: []byte("on a channel"), Props: append(slices.Clone(own), harness.User("saguin-id", "mine-1"))}, "x/ev/1", false},
		{&harness.Publish{QoS: 1, Retain: true, Topic: "bc/1", Payload: []byte("retained on $broadcast"), Props: own}, "x/bc/1", true},
		{&harness.Publish{QoS: 1, Topic: "bc/2", Payload: []byte("plain on $broadcast")}, "x/bc/2", false},
	} {
		support.Acknowledged(t, pub, c.p)
		got := support.PeerTakes(t, l, 1)[0]
		support.Crossed(t, got, c.topic, 1, c.p.Payload)
		if got.Retain != c.retain {
			t.Errorf("%s reached the peer with RETAIN %v; want %v: RETAIN crosses only on $broadcast records", c.p.Topic, got.Retain, c.retain)
		}
		rs := records(t, b.DB)
		i := slices.IndexFunc(rs, func(r record) bool { return bytes.Equal(r.payload, c.p.Payload) })
		if i < 0 {
			t.Fatalf("the database holds no record of %s", c.p.Topic)
		}
		expectOnlySaguinID(t, got, c.p, rs[i].id)
	}

	db := support.OpenDB(t, b.DB)
	rule := ruleOf(t, db, name, support.Out("bc/#", "x/bc/$#", 1))
	unretained := support.QuietWith(true, harness.Property{ID: harness.RetainAvailable, Int: 0})
	for _, link := range [][]*harness.Publish{
		{
			{QoS: 1, Retain: true, Topic: "bc/3", Payload: []byte("retained bc/3")},
			{QoS: 1, Topic: "bc/6", Payload: []byte("plain bc/6")},
			{QoS: 1, Retain: true, Topic: "bc/4", Payload: []byte("retained bc/4")},
		},
		{{QoS: 1, Retain: true, Topic: "bc/5", Payload: []byte("retained bc/5")}},
	} {
		_ = l.Close() // the peer drops the link
		l = support.Accepted(t, peer, unretained)
		for _, p := range link {
			support.Acknowledged(t, pub, p)
			if !p.Retain {
				got := support.PeerTakes(t, l, 1)[0]
				support.Crossed(t, got, "x/"+p.Topic, 1, p.Payload)
				if got.Retain {
					t.Errorf("%s, not retained, reached a peer whose CONNACK has Retain Available 0 with RETAIN 1; want 0", p.Topic)
				}
				continue
			}
			got, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
			offset := offsetsByPayload(t, b.DB)[string(p.Payload)]
			for _, r := range inflightTo(t, db, name) {
				if r.channel == 1 && r.offset == offset {
					t.Errorf("%s, retained, is in bridge_inflight as %+v on a link with Retain Available 0; want no row: it crosses at QoS 0", p.Topic, r)
				}
			}
			if got.QoS == 1 {
				support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: got.ID})
			}
			support.Crossed(t, got, "x/"+p.Topic, 0, p.Payload)
			if got.Retain {
				t.Errorf("%s reached a peer whose CONNACK has Retain Available 0 with RETAIN 1; want 0", p.Topic)
			}
			awaitState(t, fmt.Sprintf("the bc/# rule's cursor on $broadcast past %s's offset %d", p.Topic, offset), func() int64 {
				committed(t, pub, "elsewhere/barrier") // a commit for the position to go with
				return cursorOf(t, db, rule)
			}, func(c int64) bool { return c > offset })
		}
	}
	stopped(t, b)
	var warned []string
	for _, line := range testkit.LinesNaming(b.Log(), "WARN", name) {
		if strings.Contains(strings.ToLower(line), "retain") {
			warned = append(warned, line)
		}
	}
	if len(warned) != 2 {
		t.Errorf("the log holds %d WARN lines naming %s and Retain Available, %q; want 2, one for each link with Retain Available 0", len(warned), name, warned)
	}
}

// TestBridgedRecordNeverLeaves: design.md, Bridges: "A record that arrived
// over a bridge never leaves over any bridge."; storage.md, records:
// "bridge | the id of the bridge it arrived over, 0 for none: such a record
// never leaves over a bridge". TestBridgedRecordKeepsItsBridge checks that
// such a record is stored with its bridge; this test checks it never leaves.
//
// A bridge named origin has an in rule taking a/# to from/a/$# at QoS 1 and
// an out rule taking from/# to back/$#; a bridge named other has an out rule
// taking from/# to there/$#, each at QoS 1. Origin's peer forwards a record
// on a/origin, PUBACK 0x00, and a local client then stores from/local. The
// first PUBLISH each peer receives is from/local's, on back/local and on
// there/local: the bridged record, stored first on $broadcast, crossed on
// neither.
func TestBridgedRecordNeverLeaves(t *testing.T) {
	t.Parallel()
	origin, other := harness.ListenedPeer(t), harness.ListenedPeer(t)
	b := support.Launch(t, &harness.Options{Bridges: []harness.Bridge{
		support.BridgeTo("origin", origin, support.In("a/#", 1), support.Out("from/#", "back/$#", 1)),
		support.BridgeTo("other", other, support.Out("from/#", "there/$#", 1)),
	}})
	in, _, _ := support.LinkUp(t, origin, support.Quiet)
	out := support.Accepted(t, other, support.Quiet)
	support.Bridged(t, in, fromPeer(1, 1, "a/origin", "from the peer"))
	local := &harness.Publish{QoS: 1, Topic: "from/local", Payload: []byte("from a client")}
	support.Acknowledged(t, localClient(t, b, "never-leaves"), local)
	support.Crossed(t, support.PeerTakes(t, in, 1)[0], "back/local", 1, local.Payload)
	support.Crossed(t, support.PeerTakes(t, out, 1)[0], "there/local", 1, local.Payload)
}

// TestOutQueueCrossesAsAReader: design.md, Bridges, Out: "On a queue, a rule
// reads the queue's own topics as a reader, never as a worker, so it spends
// no attempt and changes no job. A dead-letter channel crosses like any
// append channel."; "A record leaves with no saguin- property except
// saguin-id."; Delivery, Queue jobs: "When an attempt fails with
// retry.max_attempts spent, the job moves to the dead-letter channel as
// attempts_exhausted"; "A refusing PUBACK (reason 0x80 or above) spends the
// attempt and ends it as a failure"; Sagüin properties: saguin-attempt "(from
// 1)".
//
// The queue orders allows one attempt; a bridge, reader, has out rules
// taking orders/# to q/$# and __dlq/orders/# to dl/$#, each at QoS 1. A
// client stores orders/1 and orders/2: the peer receives q/1 and q/2, in
// order, and the queue still holds both jobs, none spent, failed or begun,
// each offered at once. A worker then receives orders/1's offer, its
// saguin-attempt 1, and refuses it with PUBACK 0x80: the job moves to
// orders__dlq, and the peer receives its dead letter on dl/1, at QoS 1,
// carrying the dead letter's saguin-id and no other saguin- property.
func TestOutQueueCrossesAsAReader(t *testing.T) {
	t.Parallel()
	b, _, l := support.Outbound(t, &harness.Options{Channels: []harness.Channel{retrying("fixed", 200*time.Millisecond, 1)}}, "reader", support.Quiet,
		support.Out("orders/#", "q/$#", 1), support.Out(deadFilter, "dl/$#", 1))
	db := support.OpenDB(t, b.DB)
	jobs := eachOwn("orders/1", "orders/2")
	support.PublishAll(t, localClient(t, b, clientID(t, "-pub")), jobs)
	got := support.PeerTakes(t, l, 2)
	support.Crossed(t, got[0], "q/1", 1, jobs[0].Payload)
	support.Crossed(t, got[1], "q/2", 1, jobs[1].Payload)
	expectJobs(t, db, ordersQueue.Name, []string{"orders/1", "orders/2"}, "once both crossed to the peer")

	w := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-worker"), CleanStart: true}, support.At(1, ordersWorker))
	offer := support.DeliveredAt(t, w, 1, "orders/1", jobs[0].Payload)
	expectAttemptNumber(t, offer, 1)
	acknowledge(t, w, offer.ID, 0x80)
	awaitHeld(t, db, deadLetters, deadTopics("orders/1"), "once its one attempt has failed")
	dead := support.PeerTakes(t, l, 1)[0]
	support.Crossed(t, dead, "dl/1", 1, jobs[0].Payload)
	rs := channelRecords(t, b.DB, deadLetters)
	expectOnlySaguinID(t, dead, jobs[0], rs[0].id)
}

// TestOutRuleMissesWhatRetentionPasses: design.md, Words: Missed, "missed
// records are those a consumer was owed but never received because
// retention removed them (retention)"; "In the retention write, the core
// names its connected consumers and bridge rules, with their filters,
// position, No Local id and partitions."; "Deliveries already settled or
// handed to a writer are excluded."; Words: Retention, "A channel's
// retention, $broadcast included, may pass a record a session is owed, even
// one in its in-flight window. The session continues and the loss is
// counted."; Channels, append: "A consumer behind the floor jumps to it.";
// Bridges, Out: "One record per topic at the peer is in flight at a time."
// metrics.md, saguin_missed_total{channel,cause}: "records consumers were
// owed and never received".
//
// An append channel, events, keeps 350 bytes; a bridge, lagging, has an out
// rule taking events/# to x/$# at QoS 1. A client stores events/a; it
// reaches the peer, which holds its PUBACK. The client stores four more on
// events/a, 100 bytes each, which wait behind it: retention leaves the last
// three, deleting the first, handed to the writer, and the second, owed and
// never sent. saguin_missed_total of events, retention, reads 1. The peer
// acknowledges the first: the next to come is the third.
func TestOutRuleMissesWhatRetentionPasses(t *testing.T) {
	t.Parallel()
	b, _, l := support.Outbound(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 3*testkit.RecordBytes + testkit.RecordBytes/2},
		elsewhere,
	}}, "lagging", support.Quiet, support.Out("events/#", "x/$#", 1))
	db := support.OpenDB(t, b.DB)
	pub := localClient(t, b, "lagging-pub")
	rs := make([]*harness.Publish, 5)
	for i := range rs {
		rs[i] = support.Sized("events/a", testkit.RecordBytes)
		rs[i].Payload[0] = byte('1' + i) // its own payload, of the same size
	}
	support.Acknowledged(t, pub, rs[0])
	first, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
	support.Crossed(t, first, "x/a", 1, rs[0].Payload)
	support.PublishAll(t, pub, rs[1:])
	awaitState(t, "events holding offsets 3 to 5", func() []int64 {
		rs := held(t, db, "events")
		offsets := make([]int64, len(rs))
		for i, r := range rs {
			offsets[i] = r.offset
		}
		return offsets
	}, func(offsets []int64) bool { return slices.Equal(offsets, []int64{3, 4, 5}) })
	committed(t, pub, "elsewhere/barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 1, "once retention has passed the second record, owed to the rule and never sent")

	support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: first.ID})
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "x/a", 1, rs[2].Payload)
}

// TestOutBridgeSeries: metrics.md, Bridges:
// saguin_bridge_rule_up{bridge,direction,filter,topic}, gauge, "in: 1 while
// the peer grants the subscription; out: 1 while sending";
// saguin_bridge_sent_total{bridge}, counter, "records the peer acknowledged,
// or sent at QoS 0"; saguin_bridge_unsent_total{bridge,cause}, counter,
// "refused (the peer refused it), expired, topic_invalid (its topic, after
// substitution, past a topic limit) or too_large (over the peer's Maximum
// Packet Size)"; Delivery: saguin_deliveries_total{qos}, counter, "PUBLISH
// packets sent to clients and outbound bridges, resends included";
// saguin_inflight, gauge, "QoS 1 and 2 deliveries to connected clients and
// bridges waiting for their acknowledgement: the windows the core holds";
// Rules: "Every series exists from start, for every value of its labels
// that applies."; "Counters start at 0."
//
// A bridge, metered-out, has out rules taking s/# to x/$# at QoS 1 and z/#
// to y/$# at QoS 0, and dials a peer that holds its first CONNECT
// unanswered: /metrics then has each series, of the type metrics.md gives,
// each rule's out series at 0, sent and unsent under each cause at 0,
// deliveries at each QoS 0 and nothing in flight. The peer accepts it: each
// out series reads 1. A client stores s/1, which reaches the peer, its
// PUBACK held: one delivery at QoS 1, one in flight. The peer acknowledges
// it: none in flight, one sent. The client stores z/1 at QoS 1, which
// crosses at QoS 0: one delivery at QoS 0, two sent. The client stores s/2,
// which reaches the peer unanswered: two deliveries at QoS 1. The peer
// drops the link: each out series reads 0. On the next link, Session
// Present 1, s/2 comes again with DUP 1; its PUBACK sent, deliveries at
// QoS 1 read three, the resend included, none is in flight and three sent.
func TestOutBridgeSeries(t *testing.T) {
	t.Parallel()
	const bridge = "metered-out"
	peer := harness.ListenedPeer(t)
	s, z := support.Out("s/#", "x/$#", 1), support.Out("z/#", "y/$#", 0)
	b := support.OpsLaunch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo(bridge, peer, s, z)}})
	sent := testkit.SeriesKey("saguin_bridge_sent_total", "bridge", bridge)
	unsent := func(cause string) string {
		return testkit.SeriesKey("saguin_bridge_unsent_total", "bridge", bridge, "cause", cause)
	}
	ruleUp := func(r harness.BridgeRule) string {
		return testkit.SeriesKey("saguin_bridge_rule_up", "bridge", bridge, "direction", "out", "filter", r.Filter, "topic", r.Topic)
	}
	deliveries := func(qos string) string { return testkit.SeriesKey("saguin_deliveries_total", "qos", qos) }
	inflight := testkit.SeriesKey("saguin_inflight")

	held := peer.Link(t)
	harness.Arrive[*harness.Connect](t, held, harness.DefaultTimeout)
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	for name, kind := range map[string]string{
		"saguin_bridge_sent_total": "counter", "saguin_bridge_unsent_total": "counter", "saguin_bridge_rule_up": "gauge",
		"saguin_deliveries_total": "counter", "saguin_inflight": "gauge",
	} {
		if e.Types[name] != kind {
			t.Errorf("%s is of type %q; want %s", name, e.Types[name], kind)
		}
	}
	support.AwaitSampled(t, b, "from start, the first CONNECT unanswered", map[string]float64{
		sent: 0, unsent("refused"): 0, unsent("expired"): 0, unsent("topic_invalid"): 0, unsent("too_large"): 0, ruleUp(s): 0, ruleUp(z): 0,
		deliveries("0"): 0, deliveries("1"): 0, deliveries("2"): 0, inflight: 0,
	})

	support.PeerSends(t, held, support.Quiet)
	support.AwaitSampled(t, b, "the link up", map[string]float64{ruleUp(s): 1, ruleUp(z): 1})
	pub := localClient(t, b, "metered-out-pub")
	acked := &harness.Publish{QoS: 1, Topic: "s/1", Payload: []byte("acknowledged")}
	support.Acknowledged(t, pub, acked)
	got, _ := harness.Arrive[*harness.Publish](t, held, harness.DefaultTimeout)
	support.Crossed(t, got, "x/1", 1, acked.Payload)
	support.AwaitSampled(t, b, "s/1 sent, its PUBACK held", map[string]float64{deliveries("1"): 1, inflight: 1, sent: 0})
	support.PeerSends(t, held, &harness.Ack{Kind: harness.PUBACK, ID: got.ID})
	support.AwaitSampled(t, b, "s/1 acknowledged", map[string]float64{inflight: 0, sent: 1})
	atZero := &harness.Publish{QoS: 1, Topic: "z/1", Payload: []byte("sent at QoS 0")}
	support.Acknowledged(t, pub, atZero)
	support.Crossed(t, support.PeerTakes(t, held, 1)[0], "y/1", 0, atZero.Payload)
	support.AwaitSampled(t, b, "one record acknowledged, one sent at QoS 0", map[string]float64{deliveries("0"): 1, deliveries("1"): 1, sent: 2})
	unanswered := &harness.Publish{QoS: 1, Topic: "s/2", Payload: []byte("resent")}
	support.Acknowledged(t, pub, unanswered)
	first, _ := harness.Arrive[*harness.Publish](t, held, harness.DefaultTimeout)
	support.Crossed(t, first, "x/2", 1, unanswered.Payload)
	support.AwaitSampled(t, b, "s/2 sent, unanswered", map[string]float64{deliveries("1"): 2, inflight: 1})

	_ = held.Close() // the peer drops the link
	support.AwaitSampled(t, b, "the link dropped", map[string]float64{ruleUp(s): 0, ruleUp(z): 0})
	l := support.Accepted(t, peer, support.QuietResumed)
	resentAgain(t, l, first)
	support.AwaitSampled(t, b, "s/2 sent again and acknowledged", map[string]float64{deliveries("1"): 3, inflight: 0, sent: 3})
}

// TestOutboundFiltersMayNotOverlapAtStart: config.md, bridges.<name>: "A
// bridge's outbound filters, those of its out rules and of its both rules,
// may not overlap, with {a,b} expanded. A configuration where they do is
// refused at load, naming both rules. Their topics may not overlap either. For this check each topic
// template is read as a filter: a level holding any $1, $2 and so on is
// read as +, $# as #, and a both rule's topic is its filter. A topic
// starting with $ is compared like any other. Such a configuration is
// refused at load, naming both rules."; design.md, Restart, Start:
// "startup checks the configuration"; "The exit status is [...] 1 after
// [...] a refused start". The same configurations under --check-config are
// TestConfigRefused's and TestConfigAccepted's cases (cmd/saguin).
//
// Each configuration has bridge b, and in two cases bridge c too, to a peer
// of the test's own:
//   - out s/# then out s/+, out a/b then both a/#, and out x/{a,b} then out
//     x/b, which overlap only with {a,b} expanded; filters apart whose
//     topics overlap: out s/a to one then out s/b to one, out a/+ to
//     x/$1/c then out b/# to x/b/$# (x/+/c and x/b/#), both t/# then out
//     s/+ to t/$1: the start exits 1, a line of what it printed naming the
//     later rule, bridges.b.topics[1], and the earlier, bridges.b.topics[0];
//   - out s/a, out s/b/+ and both t/#, which are apart; out s/# and both t/+
//     on b and the same on c; out s/a to one on b and on c; out a/# beside
//     in a/b; out s/+ to x/$1 beside out t/+ to y/$1; out a/+ to x/$1
//     beside in b/# to x/$#, whose topic x/# is not outbound: the broker
//     starts.
func TestOutboundFiltersMayNotOverlapAtStart(t *testing.T) {
	t.Parallel()
	both := func(filter string) harness.BridgeRule {
		return harness.BridgeRule{Filter: filter, Direction: "both", QoS: 1}
	}
	for _, c := range []struct {
		name    string
		rules   []harness.BridgeRule // bridge b's
		twice   bool                 // bridge c holds b's rules too
		refused bool
	}{
		{"out against out", []harness.BridgeRule{support.Out("s/#", "x/$#", 1), support.Out("s/+", "y/$1", 1)}, false, true},
		{"out against both", []harness.BridgeRule{support.Out("a/b", "x", 1), both("a/#")}, false, true},
		{"through {a,b}", []harness.BridgeRule{support.Out("x/{a,b}", "y", 1), support.Out("x/b", "z", 1)}, false, true},
		{"apart", []harness.BridgeRule{support.Out("s/a", "x", 1), support.Out("s/b/+", "y/$1", 1), both("t/#")}, false, false},
		{"on two bridges", []harness.BridgeRule{support.Out("s/#", "x/$#", 1), both("t/+")}, true, false},
		{"beside an in filter", []harness.BridgeRule{support.Out("a/#", "x/$#", 1), support.In("a/b", 1)}, false, false},
		{"one constant topic", []harness.BridgeRule{support.Out("s/a", "one", 1), support.Out("s/b", "one", 1)}, false, true},
		{"topics through $1 and $#", []harness.BridgeRule{support.Out("a/+", "x/$1/c", 1), support.Out("b/#", "x/b/$#", 1)}, false, true},
		{"a template over a both filter", []harness.BridgeRule{both("t/#"), support.Out("s/+", "t/$1", 1)}, false, true},
		{"templates apart", []harness.BridgeRule{support.Out("s/+", "x/$1", 1), support.Out("t/+", "y/$1", 1)}, false, false},
		{"one topic on two bridges", []harness.BridgeRule{support.Out("s/a", "one", 1)}, true, false},
		{"beside an in topic", []harness.BridgeRule{support.Out("a/+", "x/$1", 1),
			{Filter: "b/#", Topic: "x/$#", Direction: "in", QoS: 1}}, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			peer := harness.ListenedPeer(t)
			o := &harness.Options{Dir: t.TempDir(), Bridges: []harness.Bridge{support.BridgeTo("b", peer, c.rules...)}}
			if c.twice {
				o.Bridges = append(o.Bridges, support.BridgeTo("c", peer, c.rules...))
			}
			if !c.refused {
				support.Launch(t, o)
				return
			}
			config, err := harness.WriteConfig(o)
			if err != nil {
				t.Fatalf("writing the configuration: %v", err)
			}
			code, stdout, stderr := execute(t, support.BrokerBinary, "--config", config)
			if code != 1 {
				t.Errorf("--config: exit %d; want the start refused, exit 1", code)
			}
			if !namesAll(stdout+stderr, "bridges.b.topics[1]", "bridges.b.topics[0]") {
				t.Errorf("--config: no line of what it printed names both rules, bridges.b.topics[1] and bridges.b.topics[0]:\n%s%s", stdout, stderr)
			}
		})
	}
}
