package conformance

import (
	"bytes"
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// subscriptionLimit is the configuration of the tests that reach
// limits.max_subscriptions: two filters per session (config.md,
// broker.limits, max_subscriptions: "This bounds the filters per session").
var subscriptionLimit = harness.Options{MaxSubscriptions: 2}

// topicLimits is the configuration of the tests that reach the topic
// limits with short filters: limits.max_topic_length 8 bytes and
// limits.max_topic_levels 3.
var topicLimits = harness.Options{MaxTopicLength: 8, MaxTopicLevels: 3}

// identifier is the property section of an MQTT 5 SUBSCRIBE holding one
// Subscription Identifier (MQTT 5 3.8.2.1.2).
func identifier(id uint32) harness.Properties {
	return harness.Properties{{ID: harness.SubscriptionIdentifier, Int: id}}
}

// unsuback sends from c an UNSUBSCRIBE of filters carrying props, under the
// next packet identifier, and fails the test unless the UNSUBACK with that
// identifier carries want, which MQTT 3.1.1's has none of: "The UNSUBACK
// packet MUST have the same Packet Identifier as the UNSUBSCRIBE packet"
// (MQTT 5 [MQTT-3.10.4-4]; 3.1.1 [MQTT-3.10.4-4]); "The order of Reason
// Codes in the UNSUBACK packet MUST match the order of Topic Filters in the
// UNSUBSCRIBE packet" [MQTT-3.11.3-1]. It returns the UNSUBACK.
func unsuback(t *testing.T, c *harness.Client, props harness.Properties, filters []string, want ...byte) *harness.Unsuback {
	t.Helper()
	u := &harness.Unsubscribe{ID: c.NextID(), Props: props, Filters: filters}
	if err := c.Send(u); err != nil {
		t.Fatalf("UNSUBSCRIBE %.40q: %v", filters, err)
	}
	p, err := c.Await(func(p harness.Packet) bool {
		a, ok := p.(*harness.Unsuback)
		return ok && a.ID == u.ID
	})
	if err != nil {
		t.Fatalf("UNSUBSCRIBE %.40q: waiting for its UNSUBACK: %v", filters, err)
	}
	ack := p.(*harness.Unsuback)
	if !bytes.Equal(ack.Reasons, want) {
		t.Fatalf("UNSUBSCRIBE %.40q: UNSUBACK % X, want % X", filters, ack.Reasons, want)
	}
	return ack
}

// subscriptionRow, startRow and positionRow are rows of storage.md's
// subscriptions, subscription_starts and positions, the session's id left
// out, as the operator reads them. A position's last_seen is when it last
// moved, which delivery decides; it is not read.
type subscriptionRow struct {
	filter                                          string
	qos, noLocal, retainAsPublished, retainHandling int
	identifier                                      int
	partitions                                      string
}

type startRow struct {
	filter         string
	channel, start int64
}

type positionRow struct{ channel, offset int64 }

// sessionSubscriptions holds the three tables' rows of one session.
type sessionSubscriptions struct {
	subscriptions []subscriptionRow
	starts        []startRow
	positions     []positionRow
}

// storedSubscriptions reads the subscriptions, subscription_starts and
// positions rows of client's durable session from db, each in key order,
// failing the test unless the session has its sessions row (storage.md,
// sessions: "One row per durable session"), which the others name.
func storedSubscriptions(t *testing.T, db *sql.DB, client string) *sessionSubscriptions {
	t.Helper()
	if n := sessionRows(t, db, client); n != 1 {
		t.Fatalf("the database holds %d sessions rows for %q, want 1", n, client)
	}
	var s sessionSubscriptions
	support.Query(t, db, `SELECT s.filter, s.qos, s.no_local, s.retain_as_published, s.retain_handling, s.identifier, s.partitions
		FROM subscriptions s JOIN sessions ON sessions.id = s.session WHERE sessions.client = ? ORDER BY s.filter`,
		func(rows *sql.Rows) error {
			var r subscriptionRow
			err := rows.Scan(&r.filter, &r.qos, &r.noLocal, &r.retainAsPublished, &r.retainHandling, &r.identifier, &r.partitions)
			s.subscriptions = append(s.subscriptions, r)
			return err
		}, client)
	support.Query(t, db, `SELECT st.filter, st.channel, st.start
		FROM subscription_starts st JOIN sessions ON sessions.id = st.session WHERE sessions.client = ? ORDER BY st.filter, st.channel`,
		func(rows *sql.Rows) error {
			var r startRow
			err := rows.Scan(&r.filter, &r.channel, &r.start)
			s.starts = append(s.starts, r)
			return err
		}, client)
	support.Query(t, db, `SELECT p.channel, p."offset"
		FROM positions p JOIN sessions ON sessions.id = p.session WHERE sessions.client = ? ORDER BY p.channel`,
		func(rows *sql.Rows) error {
			var r positionRow
			err := rows.Scan(&r.channel, &r.offset)
			s.positions = append(s.positions, r)
			return err
		}, client)
	return &s
}

// expectStored fails the test unless db holds want for client's session,
// when says when.
func expectStored(t *testing.T, db *sql.DB, client string, want *sessionSubscriptions, when string) {
	t.Helper()
	got := storedSubscriptions(t, db, client)
	if !slices.Equal(got.subscriptions, want.subscriptions) {
		t.Errorf("%s, subscriptions holds %+v, want %+v", when, got.subscriptions, want.subscriptions)
	}
	if !slices.Equal(got.starts, want.starts) {
		t.Errorf("%s, subscription_starts holds %+v, want %+v", when, got.starts, want.starts)
	}
	if !slices.Equal(got.positions, want.positions) {
		t.Errorf("%s, positions holds %+v, want %+v", when, got.positions, want.positions)
	}
}

// passedOnlyStored returns client's saved position on $broadcast in db,
// failing the test unless it is settled, the end of the settled prefix, or
// past it with every record between held by a stored exchange (design.md,
// Delivery, Positions: "a durable session may also pass records
// represented by stored QoS 1 or QoS 2 exchanges"). when says when.
func passedOnlyStored(t *testing.T, db *sql.DB, client string, settled int64, when string) int64 {
	t.Helper()
	got := storedSubscriptions(t, db, client).positions
	if len(got) != 1 || got[0].channel != 1 {
		t.Fatalf("%s, positions holds %+v; want one row, on $broadcast", when, got)
	}
	position := got[0].offset
	if position < settled {
		t.Fatalf("%s, the position is %d; want it at least %d, the settled prefix's end", when, position, settled)
	}
	held := make(map[int64]bool)
	for _, r := range storedInflight(t, db, client) {
		if r.channel == 1 {
			held[r.offset] = true
		}
	}
	for offset := settled; offset < position; offset++ {
		if !held[offset] {
			t.Errorf("%s, the position %d passes record %d, which no stored exchange holds", when, position, offset)
		}
	}
	return position
}

// broadcastTail reads $broadcast's tail from db: storage.md, channels, next,
// "the tail: the offset the next record takes"; Ids: "The $broadcast channel
// is channel 1".
func broadcastTail(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var next int64
	if err := db.QueryRow(`SELECT next FROM channels WHERE id = 1`).Scan(&next); err != nil {
		t.Fatalf("reading $broadcast's tail: %v", err)
	}
	return next
}

// design.md, Flows, SUBSCRIBE, the subscription table: "$broadcast | [...]
// | 0 / 1 / 2", granted for QoS 0 / 1 / 2; Channels, $broadcast: "the
// reserved append channel for every topic no configured channel claims". MQTT
// 5 3.9.3, Table 3-8: 0x00, 0x01 and 0x02, "Granted QoS 0", "1" and "2"; MQTT
// 3.1.1 3.9.3: "0x00 - Success - Maximum QoS 0", "0x01", "0x02"; MQTT 5
// [MQTT-3.2.2-10]: a Server "MUST still accept SUBSCRIBE packets containing a
// Requested QoS of 0, 1 or 2". MQTT 5 3.8.2.1.2 and 3.8.2.1.3: a Subscription
// Identifier, and User Properties, whose "meaning [...] is not defined by
// this specification"; 3.8.3.1: the options.
//
// Under each version, a SUBSCRIBE of three filters, wildcards included, at
// QoS 0, 1 and 2 gets SUBACK 0x00 0x01 0x02. MQTT 5's carries a
// Subscription Identifier, a User Property outside the reserved saguin-
// prefix, and No Local, Retain As Published and each Retain Handling.
func TestSubackGrantsTheRequestedQoS(t *testing.T) {
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			c, _ := harness.Connected(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			subs := slices.Concat(support.At(0, "grants/#"), support.At(1, "grants/+/one"), support.At(2, "grants/two"))
			var props harness.Properties
			if version == harness.V5 {
				subs[1].NoLocal, subs[1].RetainAsPublished, subs[1].RetainHandling = true, true, 2
				subs[2].RetainHandling = 1
				props = append(identifier(5), harness.User("app", "x"))
			}
			support.Suback(t, c, props, subs, 0x00, 0x01, 0x02)
		})
	}
}

// design.md, Operations, HTTP, /metrics: "There is no $SYS tree. Sagüin
// publishes nothing under $SYS. A subscription to $SYS/… is refused with
// 0x87, or 0x80 for MQTT 3.1.1"; Flows, SUBSCRIBE: "Refusals, per filter".
// MQTT 5 3.9.3, Table 3-8: 0x87 "Not authorized"; MQTT 3.1.1 3.9.3: "0x80 -
// Failure"; MQTT 5 4.13.2: a Reason Code of 0x80 or greater on an
// acknowledgement has "no consequences for other Sessions or other Packets
// flowing on the same Session".
//
// Under each version, a SUBSCRIBE of $SYS/#, a filter outside $SYS and
// $SYS/+/load: the SUBACK refuses each $SYS filter, 0x87 (MQTT 3.1.1:
// 0x80), and grants the other; the connection then answers PINGREQ.
func TestSysFilterIsRefused(t *testing.T) {
	for _, version := range []byte{harness.V5, harness.V311} {
		t.Run(harness.VersionName(version), func(t *testing.T) {
			refused := byte(0x87)
			if version == harness.V311 {
				refused = 0x80
			}
			c, _ := harness.Connected(t, brokerAddr, version, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			support.Suback(t, c, nil, slices.Concat(support.At(1, "$SYS/#"), support.At(1, "not/sys"), support.At(0, "$SYS/+/load")), refused, 0x01, refused)
			if err := c.Ping(); err != nil {
				t.Fatalf("after the refusals: %v; want the connection to stay", err)
			}
		})
	}
}

// design.md, Flows, SUBSCRIBE, Refusals: "0x8F for a filter [...] over
// limits.max_topic_length or limits.max_topic_levels"; config.md,
// broker.limits: max_topic_length, "the longest topic or filter, in bytes",
// 1024 by default, and max_topic_levels, "levels in any topic or filter",
// 200 by default. MQTT 5 4.7.1.1: the separator "divides the Topic Name
// into multiple “topic levels”", and "Adjacent Topic level separators
// indicate a zero-length topic level", so a filter has one level more than
// it has separators, a wildcard filling a level as a name does (4.7.1.2,
// 4.7.1.3). MQTT 5 3.9.3, Table 3-8: 0x8F "Topic Filter invalid", "The
// Topic Filter is correctly formed but is not allowed for this Client".
//
// On a broker at config.md's limits, and on one at topicLimits, an MQTT 5
// SUBSCRIBE at QoS 1 of filters at each limit and one past it, counted in
// bytes, not characters, and in levels, empty and wildcard levels
// included: SUBACK grants each filter within the limits, 0x01, and refuses
// each one past either, 0x8F.
func TestFilterOverTheTopicLimitsIsRefused(t *testing.T) {
	type filter struct {
		filter string
		within bool
	}
	brokers := []struct {
		name    string
		addr    func(t *testing.T) string
		filters []filter
	}{
		{"config.md's limits", func(*testing.T) string { return brokerAddr }, []filter{
			{strings.Repeat("a", 1024), true},
			{strings.Repeat("a", 1025), false},
			{strings.Repeat("a/", 199) + "a", true},
			{strings.Repeat("a/", 200) + "a", false},
			{strings.Repeat("/", 199), true},
			{strings.Repeat("/", 200), false},
		}},
		{"8 bytes and 3 levels", func(t *testing.T) string { return support.Launch(t, &topicLimits).Addr }, []filter{
			{"abcdefgh", true},
			{"abcdefghi", false},
			{"éééé", true}, // eight bytes, four characters
			{"ééééa", false},
			{"a/b/c", true},
			{"a/b/c/d", false},
			{"//", true},
			{"///", false},
			{"+/+/#", true},
			{"+/+/+/#", false},
		}},
	}
	for _, b := range brokers {
		t.Run(b.name, func(t *testing.T) {
			c, _ := harness.Connected(t, b.addr(t), harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
			subs := make([]harness.Subscription, len(b.filters))
			want := make([]byte, len(b.filters))
			for i, f := range b.filters {
				subs[i] = harness.Subscription{Filter: f.filter, QoS: 1}
				want[i] = 0x8F
				if f.within {
					want[i] = 0x01
				}
			}
			support.Suback(t, c, nil, subs, want...)
		})
	}
}

// design.md, Flows, SUBSCRIBE, Refusals: "0x97 for a filter past
// limits.max_subscriptions"; "Filters are processed in packet order, using
// refusal order 0x87, 0x8F, 0x97, 0x83. Earlier accepted filters consume
// subscription capacity before later filters"; The start point: "A SUBSCRIBE
// repeating an existing filter replaces its options", as MQTT 5
// [MQTT-3.8.4-3] says it "MUST replace that existing Subscription", no new
// one; config.md, max_subscriptions: "This bounds the filters per session".
// MQTT 5 3.9.3, Table 3-8: 0x97 "Quota exceeded"; [MQTT-3.8.4-5]: a SUBSCRIBE
// of several filters is handled "as if it had received a sequence of multiple
// SUBSCRIBE packets".
//
// With max_subscriptions 2, an MQTT 5 client subscribes to $SYS/#, a, b
// and c: 0x87, 0x01, 0x01, 0x97, the refused $SYS filter taking no
// capacity and a and b, earlier in the packet, taking it before c. A
// SUBSCRIBE repeating b at QoS 2, then c: 0x02, 0x97, the repeat no new
// subscription. Once a is unsubscribed (0x00), c is granted: the limit
// counts the subscriptions held.
func TestMaxSubscriptions(t *testing.T) {
	b := support.Launch(t, &subscriptionLimit)
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "limited", CleanStart: true})
	support.Suback(t, c, nil, slices.Concat(support.At(0, "$SYS/#"), support.At(1, "limit/a", "limit/b", "limit/c")), 0x87, 0x01, 0x01, 0x97)
	support.Suback(t, c, nil, slices.Concat(support.At(2, "limit/b"), support.At(1, "limit/c")), 0x02, 0x97)
	unsuback(t, c, nil, []string{"limit/a"}, 0x00)
	support.Suback(t, c, nil, support.At(1, "limit/c"), 0x01)
}

// design.md, Flows, SUBSCRIBE, Refusals: "For an unknown saguin- property, it
// uses 0x83 on every otherwise eligible filter"; "using refusal order 0x87,
// 0x8F, 0x97, 0x83"; Sagüin properties: "The saguin- prefix is reserved on
// every topic". config.md, broker.limits: max_topic_length 1024 by default.
// MQTT 5 3.9.3, Table 3-8: 0x83 "Implementation specific error", "The
// SUBSCRIBE is valid but the Server does not accept it"; 3.11.3, Table 3-9:
// 0x11 "No subscription existed".
//
// An MQTT 5 SUBSCRIBE carrying the User Property saguin-x, of a filter,
// $SYS/#, a filter over max_topic_length and another filter: 0x83, 0x87,
// 0x8F, 0x83, each filter refused for its earlier reason where it has one.
// Neither filter refused 0x83 is held: an UNSUBSCRIBE of both gets 0x11
// for each.
func TestUnknownSaguinPropertyRefusesEveryEligibleFilter(t *testing.T) {
	c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
	subs := slices.Concat(support.At(1, "unknown/a", "$SYS/#", strings.Repeat("a", 1025)), support.At(2, "unknown/b"))
	support.Suback(t, c, harness.Properties{harness.User("saguin-x", "1")}, subs, 0x83, 0x87, 0x8F, 0x83)
	unsuback(t, c, nil, []string{"unknown/a", "unknown/b"}, 0x11, 0x11)
}

// partitioned is the property section of an MQTT 5 SUBSCRIBE holding a
// saguin-filter User Property for each of declarations, in order.
func partitioned(declarations ...string) harness.Properties {
	props := make(harness.Properties, len(declarations))
	for i, d := range declarations {
		props[i] = harness.User("saguin-filter", d)
	}
	return props
}

// design.md, Flows, SUBSCRIBE, Partitioning: "a saguin-filter User
// Property topic_hash(partitions, index) delivers only the topics where
// topic_hash(topic) mod partitions == index, keeping per-topic order";
// topic_hash being harness.TopicHash; "A declaration is
// topic_hash(count,index), with ASCII spaces or tabs allowed around the
// integers and comma. Integers are unsigned decimal without signs or leading
// zeroes, except zero itself. Count is 1 through 4,294,967,295. Index is 0
// through count minus one. Repeated declarations use one count and their
// indices form a set. Invalid syntax or inconsistent counts refuse each
// affected filter with 0x83. Partitioning is allowed on append, latest and a
// queue's own topics. It is refused with 0x83 on $broadcast, $share and
// $saguin/queue, for that filter alone". One copy: "Only subscriptions
// eligible for the record contribute: topic, live-start boundary, No Local,
// partition and permission checks apply". storage.md, subscriptions:
// partitions, "its saguin-filter partitioning ([SUBSCRIBE]), as declared:
// each declaration as the client sent it, in its order, one per line". MQTT 5
// 3.9.3, Table 3-8: 0x11 "No subscription existed".
//
// An append channel,
// parts, holds part/#, and a queue, jobs, jobs/#; a publisher stores a
// record on each of part/0 to part/15. For each declaration set, a client
// holding a durable session subscribes at QoS 0 to part/# with it, and the
// publisher stores a marker, a record on the first of part/end/0,
// part/end/1 and so on whose harness.TopicHash falls in the set (the largest
// count's index is chosen so that part/end/0's does): the client receives,
// in offset order, every record stored so far, earlier subtests' markers
// included, whose topic's harness.TopicHash modulo the count is one of the
// indices, the marker last. The database holds part/#'s partitions as its
// declarations, each as sent, in their order, one per line, the repeated
// one included. Then a client subscribes
// at QoS 1 with each invalid declaration set to part/a and part/#: SUBACK
// 0x83, 0x83, and an UNSUBSCRIBE of both gets 0x11, 0x11. With a valid
// declaration, part/# beside weather/# gets 0x01, 0x83; $share/g/part/#
// and $saguin/queue/jobs get 0x83.
func TestPartitionsSelectTopicsByTheirHash(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "parts", Type: "append", Filter: "part/#"},
		{Name: "jobs", Type: "queue", Filter: "jobs/#"},
	}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "parts-pub", CleanStart: true})
	var stored []*harness.Publish // every record of parts, in offset order
	for i := range 16 {
		stored = append(stored, &harness.Publish{QoS: 1, Topic: fmt.Sprintf("part/%d", i), Payload: fmt.Appendf(nil, "%d", i)})
	}
	support.PublishAll(t, pub, stored)

	largest := harness.TopicHash("part/end/0") % 4294967295
	for i, tc := range []struct {
		name         string
		declarations []string
		count        uint64
		indices      []uint64
	}{
		{"one declaration", []string{"topic_hash(4, 1)"}, 4, []uint64{1}},
		{"declarations forming a set", []string{"topic_hash(4,0)", "topic_hash( 4 ,\t2\t)", "topic_hash(4,0)"}, 4, []uint64{0, 2}},
		{"one partition", []string{"topic_hash(1,0)"}, 1, []uint64{0}},
		{"the largest count", []string{fmt.Sprintf("topic_hash(4294967295,%d)", largest)}, 4294967295, []uint64{largest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fmt.Sprintf("partition-%d", i)
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable})
			support.Suback(t, c, partitioned(tc.declarations...), support.At(0, "part/#"), 0x00)
			in := func(topic string) bool { return slices.Contains(tc.indices, harness.TopicHash(topic)%tc.count) }
			marker := &harness.Publish{QoS: 1, Payload: []byte("marker of " + tc.name)}
			for k := 0; marker.Topic == "" || !in(marker.Topic); k++ {
				marker.Topic = fmt.Sprintf("part/end/%d", k)
			}
			support.Acknowledged(t, pub, marker)
			stored = append(stored, marker)
			for _, p := range stored {
				if in(p.Topic) {
					delivered(t, c, p.Topic, p.Payload)
				}
			}
			declared := strings.Join(tc.declarations, "\n")
			if got := storedSubscriptions(t, support.OpenDB(t, b.DB), client).subscriptions; len(got) != 1 || got[0].partitions != declared {
				t.Errorf("subscriptions holds %+v; want part/#'s partitions %q, each declaration as sent, in order, one per line", got, declared)
			}
		})
	}

	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "partition-refused", CleanStart: true})
	for _, declarations := range [][]string{
		{"topic_hash(4,4)"}, {"topic_hash(0,0)"}, {"topic_hash(04,1)"}, {"topic_hash(+4,1)"}, {"topic_hash(4,-1)"},
		{"topic_hash(4294967296,0)"}, {"topic_hash(4)"}, {"topic_hash(4,1,2)"}, {"topic_hash(4 1)"}, {"hash(4,1)"},
		{"topic_hash(4,1"}, {""}, {"topic_hash(4,1)", "topic_hash(8,1)"},
	} {
		support.Suback(t, c, partitioned(declarations...), support.At(1, "part/a", "part/#"), 0x83, 0x83)
	}
	unsuback(t, c, nil, []string{"part/a", "part/#"}, 0x11, 0x11)
	valid := partitioned("topic_hash(4, 1)")
	support.Suback(t, c, valid, support.At(1, "part/#", "weather/#"), 0x01, 0x83)
	support.Suback(t, c, valid, support.At(1, "$share/g/part/#"), 0x83)
	support.Suback(t, c, valid, support.At(1, "$saguin/queue/jobs"), 0x83)
}

// design.md, Flows, SUBSCRIBE, Refusals: "MQTT 3.1.1 gets 0x80 for each";
// MQTT 3.1.1 3.9.3: the return codes are the granted QoS or "0x80 -
// Failure", the others "reserved and MUST NOT be used [MQTT-3.9.3-2]".
//
// On a broker at topicLimits with max_subscriptions 1, an MQTT 3.1.1
// SUBSCRIBE of a filter within every limit, one over max_topic_length and
// one over max_topic_levels (MQTT 5's 0x8F), and one past
// max_subscriptions (MQTT 5's 0x97): 0x01, 0x80, 0x80, 0x80; the
// connection then answers PINGREQ. The $SYS refusal is
// TestSysFilterIsRefused's, a full disk's TestFullDiskRefusesASubscribe's.
func TestMQTT311RefusalsAreFailures(t *testing.T) {
	o := topicLimits
	o.MaxSubscriptions = 1
	b := support.Launch(t, &o)
	c, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "legacy", CleanStart: true})
	support.Suback(t, c, nil, slices.Concat(support.At(1, "within", "abcdefghi"), support.At(0, "a/b/c/d"), support.At(2, "past")), 0x01, 0x80, 0x80, 0x80)
	if err := c.Ping(); err != nil {
		t.Fatalf("after the refusals: %v; want the connection to stay", err)
	}
}

// design.md, Flows, SUBSCRIBE, The start point: "each subscription has a
// live-start boundary for every channel it reaches. The core captures that
// channel's tail when it accepts the subscription, and stores it with a
// durable session's subscription"; "Adding a filter does not move an
// existing consumer position"; "A SUBSCRIBE repeating an existing filter
// replaces its options and keeps its position and live-start boundaries".
// Flows, Replies: SUBACK "sent only once the store confirms". Restart:
// "the broker restores durable sessions with their subscriptions, positions
// [...] Each session is loaded only at its CONNECT". storage.md:
// subscriptions, "a row per filter", with "the QoS granted", the options and
// "the Subscription Identifier; 0 for none", partitions empty "for none";
// subscription_starts.start, "the channel's tail when the core accepted the
// subscription"; positions.offset, "the next offset it is owed", the row made
// at the first subscription reaching the channel (0010a.md, 261024 070200).
// MQTT 5 3.8.2.1.2: "If there is a Subscription Identifier, it is stored with
// the subscription"; [MQTT-3.8.4-3]. Delivery, Positions: "The position
// advances only past the contiguous settled or skipped prefix of examined
// records, except that a durable session may also pass records represented by
// stored QoS 1 or QoS 2 exchanges".
//
// With max_subscriptions 2, another client publishes twice; then an MQTT 5
// client holding a durable session subscribes to kept/# at QoS 1 with
// Subscription Identifier 7. The database holds its subscription, its
// start and its position at $broadcast's tail. The client never
// acknowledges, so from then on its position is that tail or past it, every
// record it passes held by one of the session's stored exchanges. After
// another publication, a second filter, at QoS 0 with no identifier, adds
// its row and its own start at the new tail, the position not moved by it. After a third, a SUBSCRIBE
// repeating kept/# with QoS 2, No Local, Retain As Published, Retain
// Handling 2 and Subscription Identifier 9 changes that row's options
// alone. Across a reconnect with Clean Start 0, then across a restart, the
// session is present and the database holds the same rows; and a SUBSCRIBE
// of a new filter and kept/# again gets 0x97 and 0x02: both subscriptions
// are held, at the limit, and the repeat is still no new one.
func TestDurableSubscriptionIsKept(t *testing.T) {
	o := subscriptionLimit
	o.Dir = t.TempDir()
	b := support.Launch(t, &o)
	db := support.OpenDB(t, b.DB)
	publisher, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	publish := func() {
		support.Acknowledged(t, publisher, &harness.Publish{QoS: 1, Topic: "kept/a", Payload: []byte("x")})
	}
	publish()
	publish()

	connect := &harness.Connect{ClientID: "keeper", CleanStart: true, Props: support.Durable}
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	connect.CleanStart = false // every later CONNECT resumes the session
	first := broadcastTail(t, db)
	support.Suback(t, c, identifier(7), support.At(1, "kept/#"), 0x01)
	want := &sessionSubscriptions{
		subscriptions: []subscriptionRow{{filter: "kept/#", qos: 1, identifier: 7}},
		starts:        []startRow{{"kept/#", 1, first}},
	}
	// stored checks the session's rows in the database, its position on
	// $broadcast checked against first, the settled prefix.
	stored := func(db *sql.DB, when string) {
		t.Helper()
		want.positions = []positionRow{{1, passedOnlyStored(t, db, "keeper", first, when)}}
		expectStored(t, db, "keeper", want, when)
	}
	stored(db, "after the first SUBSCRIBE")

	publish()
	second := broadcastTail(t, db)
	support.Suback(t, c, nil, support.At(0, "other/+"), 0x00)
	want.subscriptions = append(want.subscriptions, subscriptionRow{filter: "other/+"})
	want.starts = append(want.starts, startRow{"other/+", 1, second})
	stored(db, "after a second filter on $broadcast")

	publish()
	repeat := []harness.Subscription{{Filter: "kept/#", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 2}}
	support.Suback(t, c, identifier(9), repeat, 0x02)
	want.subscriptions[0] = subscriptionRow{filter: "kept/#", qos: 2, noLocal: 1, retainAsPublished: 1, retainHandling: 2, identifier: 9}
	stored(db, "after a SUBSCRIBE repeating kept/# with other options")

	// held checks the two subscriptions are the session's on c, where it
	// has just resumed, and in the database.
	held := func(c *harness.Client, ack *harness.Connack, when string) {
		t.Helper()
		if !ack.SessionPresent {
			t.Fatalf("%s, CONNACK with Clean Start 0 has Session Present 0: the durable session is gone", when)
		}
		support.Suback(t, c, identifier(9), slices.Concat(support.At(0, "new/x"), repeat), 0x97, 0x02)
		stored(db, when)
	}
	_ = c.Close() // the session goes away with its connection
	c, ack := harness.Connected(t, b.Addr, harness.V5, connect)
	held(c, ack, "after a reconnect")

	stopped(t, b)
	_ = c.Close() // the broker is gone
	b = support.Launch(t, &o)
	db = support.OpenDB(t, b.DB)
	c, ack = harness.Connected(t, b.Addr, harness.V5, connect)
	held(c, ack, "after a restart")
}

// design.md, Flows, SUBSCRIBE, The start point: "Adding a filter does not
// move an existing consumer position. A consumer has one position per
// channel, never one per filter. On an append channel every filter reads
// from that position. A filter added later therefore receives what the
// position has not yet passed. For a consumer that has caught up, that is
// only what comes next"; "A SUBSCRIBE repeating an existing filter replaces its
// options and keeps its position"; the subscription table, append: "the
// floor"; Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records"; Order: "a
// channel's offset order on [...] append"; A resend: "Subscription changes
// affect new deliveries"; One copy: "Subscription Identifiers appear [...]
// once for each contributing subscription having one". storage.md:
// subscription_starts, "An append channel has no such boundary. Every filter
// on that channel reads from its consumer's position"; positions, "A durable
// session's place in each channel it consumes", primary key (session,
// channel). MQTT 5 3.8.2.1.2: "If there is a Subscription Identifier, it is
// stored with the subscription".
//
// An append channel, points, holds points/#; a publisher stores a1 and a2
// on points/a, b1 and b2 on points/b, and c1 on points/c. A client holding
// a durable session, with Receive Maximum 1, subscribes at QoS 1 to
// points/a with Subscription Identifier 1, creating its consumer of points
// at the floor, and receives a1, which it holds: its position stays at a1.
// The publisher stores x1 on points/x; the client subscribes to points/b,
// identifier 2, a filter added mid-history; the publisher stores x2; the
// client subscribes to points/a again, identifier 3, replacing it; the
// publisher stores b3 on points/b and a3 on points/a. Acknowledging each
// delivery in turn, the client receives, in offset order, a2, which
// points/a, replaced, still owes; b1, b2 and b3, carrying 2, the rest of
// points/b's history from the position on; then a3, carrying 3; a1 not
// again. Once its own publication is acknowledged, the database holds both
// subscriptions with their latest options, no start, and one position, on
// points, at a3's offset plus one, the tail: the consumer has caught up.
// The client then subscribes to points/c, identifier 4, and the publisher
// stores c2 there: the client receives c2 next, carrying 4, and never c1,
// which its position had passed.
func TestOneConsumerPositionPerChannel(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "points", Type: "append", Filter: "points/#"}}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "points-pub", CleanStart: true})
	store := func(topic, payload string) {
		t.Helper()
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "points/" + topic, Payload: []byte(payload)})
	}
	store("a", "a1")
	store("a", "a2")
	store("b", "b1")
	store("b", "b2")
	store("c", "c1")
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "pointer", CleanStart: true,
		Props: slices.Concat(support.Durable, receiveMaximum5(1))})
	support.Suback(t, c, identifier(1), support.At(1, "points/a"), 0x01)
	held := support.DeliveredAt(t, c, 1, "points/a", []byte("a1"))
	expectIDs(t, held, 1)
	store("x", "x1")
	support.Suback(t, c, identifier(2), support.At(1, "points/b"), 0x01)
	store("x", "x2")
	support.Suback(t, c, identifier(3), support.At(1, "points/a"), 0x01)
	store("b", "b3")
	store("a", "a3")

	// a2 may have been selected before the replacement, its identifier
	// then fixed (Delivery, Preparation): it is not read. points/b's
	// records are owed only through the filter added for them.
	for _, want := range []struct {
		topic, payload string
		id             uint32 // 0 for not read
	}{{"points/a", "a2", 0}, {"points/b", "b1", 2}, {"points/b", "b2", 2}, {"points/b", "b3", 2}, {"points/a", "a3", 3}} {
		acknowledge(t, c, held.ID, 0)
		held = support.DeliveredAt(t, c, 1, want.topic, []byte(want.payload))
		if want.id != 0 {
			expectIDs(t, held, want.id)
		}
	}
	acknowledge(t, c, held.ID, 0)
	committed(t, c, "pointer-barrier")
	offsets := offsetsByPayload(t, b.DB)
	points := storedChannels(t, b.DB)["points"].id
	expectStored(t, support.OpenDB(t, b.DB), "pointer", &sessionSubscriptions{
		subscriptions: []subscriptionRow{{filter: "points/a", qos: 1, identifier: 3}, {filter: "points/b", qos: 1, identifier: 2}},
		positions:     []positionRow{{points, offsets["a3"] + 1}},
	}, "once every delivery is acknowledged")

	support.Suback(t, c, identifier(4), support.At(1, "points/c"), 0x01)
	store("c", "c2")
	expectIDs(t, support.DeliveredAt(t, c, 1, "points/c", []byte("c2")), 4)
}

// design.md, Flows, PUBLISH: QoS 0 "Enters the channel | on PUBLISH";
// Execution, Decisions: "Each connection's packet order is preserved";
// Flows, SUBSCRIBE, The start point: "The core takes that channel's next
// offset when it takes the SUBSCRIBE, in the connection's packet order. Every
// publication that entered before the SUBSCRIBE therefore falls below the
// boundary"; "The captured boundary separates the initial pass, which reads
// retained or current values below it, from subsequent live records"; Words,
// Offset: "assigned by the core when a publication enters the channel
// ([PUBLISH]), in the order it enters". MQTT 5 3.8.4: a new subscription's
// messages are those published after it. MQTT 5 3.1.4: "Clients are allowed
// to send further MQTT Control Packets immediately after sending a CONNECT
// packet"; design.md, Message memory: "A granted body, unread or held behind
// its CONNECT".
//
// Each case holds the database's write lock, as another program on the
// file may, and then has a client send, in one write, a QoS 0 PUBLISH on
// own/topic and a SUBSCRIBE to own/topic. Once the lock is released, the
// SUBACK grants QoS 0, and the client's next delivery is a message another
// client publishes after it: never its own earlier one, nor anything stored
// before its SUBSCRIBE.
//
// Behind a batch at the store: another client publishes at QoS 1 and
// pings, the PINGRESP, read first, saying the core has taken the PUBLISH,
// whose batch of $broadcast is then at the store, held. The client, already
// connected, sends its PUBLISH and SUBSCRIBE, then pings: its PUBLISH
// entered $broadcast before its SUBSCRIBE was taken, behind the held batch.
//
// Behind its CONNECT: another client first stores three messages on
// own/topic, not retained. The client then sends its CONNECT, for a new
// durable session whose save the lock holds, the PUBLISH and SUBSCRIBE, and
// a QoS 0 PUBLISH of padding bytes on another topic, its own send buffer
// small: the write ends only once the broker has read most of the padding,
// which it reads only after the core has granted its body, and the core
// takes its reader's packets in their order, so it has taken the SUBSCRIBE
// before the CONNECT is saved. Once the lock is released, the CONNACK is
// 0x00.
func TestOwnEarlierPublicationIsNeverLive(t *testing.T) {
	// onlyLater waits for own's SUBACK to s, granting QoS 0, keeping what
	// comes before it, and has other publish after it: own's next delivery.
	onlyLater := func(t *testing.T, other, own *harness.Client, s *harness.Subscribe) {
		t.Helper()
		p, err := own.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Suback); return ok && a.ID == s.ID })
		if err != nil {
			t.Fatalf("waiting for the SUBACK: %v", err)
		}
		if reasons := p.(*harness.Suback).Reasons; !bytes.Equal(reasons, []byte{0x00}) {
			t.Fatalf("SUBACK % X, want 00", reasons)
		}
		thenLive(t, other, own, "own/topic")
	}
	earlier := &harness.Publish{Topic: "own/topic", Payload: []byte("earlier")}

	t.Run("behind a batch at the store", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{})
		other, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "other", CleanStart: true})
		own, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "own", CleanStart: true})
		lock := holdWriteLock(t, b.DB)
		support.Sent(t, other, &harness.Publish{QoS: 1, Topic: "elsewhere", Payload: []byte("held")})
		taken(t, other, "a QoS 1 PUBLISH, the store's write lock held")
		s := &harness.Subscribe{ID: own.NextID(), Subs: support.At(0, "own/topic")}
		harness.SendBytes(t, own, slices.Concat(harness.Encoded(t, earlier, harness.V5), harness.Encoded(t, s, harness.V5)))
		// The PINGRESP says the core has taken both packets; the SUBACK, which
		// confirms no stored change, may come before it, and is kept.
		takenKeeping(t, own, "a QoS 0 PUBLISH and a SUBSCRIBE in one write, the store's write lock held")
		lock.release(t)
		onlyLater(t, other, own, s)
	})

	t.Run("behind its CONNECT", func(t *testing.T) {
		const padding = 1 << 20 // many times what the two sockets buffer unread
		b := support.Launch(t, &harness.Options{MaxPacketSize: 2 * padding})
		other, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "other", CleanStart: true})
		for i := range 3 {
			support.Acknowledged(t, other, &harness.Publish{QoS: 1, Topic: "own/topic", Payload: []byte{'0' + byte(i)}})
		}
		lock := holdWriteLock(t, b.DB)
		own, err := harness.Dial(b.Addr, harness.V5)
		if err != nil {
			t.Fatalf("dialling %s: %v", b.Addr, err)
		}
		t.Cleanup(func() { _ = own.Close() }) // the test is over
		if err := own.Conn.(*net.TCPConn).SetWriteBuffer(16 << 10); err != nil {
			t.Fatal(err)
		}
		connect := &harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: "own", CleanStart: true, Props: support.Durable}
		s := &harness.Subscribe{ID: own.NextID(), Subs: support.At(0, "own/topic")}
		pad := &harness.Publish{Topic: "padding", Payload: make([]byte, padding)}
		harness.SendBytes(t, own, slices.Concat(harness.Encoded(t, connect, harness.V5), harness.Encoded(t, earlier, harness.V5),
			harness.Encoded(t, s, harness.V5), harness.Encoded(t, pad, harness.V5)))
		lock.release(t)
		if ack := harness.Expect[*harness.Connack](t, own); ack.Reason != 0 {
			t.Fatalf("CONNACK 0x%02X, want 0x00", ack.Reason)
		}
		onlyLater(t, other, own, s)
	})
}

// design.md, Flows, SUBSCRIBE, Refusals: "0x97 for a filter [...] that a full
// disk cannot hold"; "MQTT 3.1.1 gets 0x80 for each"; "Storage failure
// applies only to otherwise accepted changes". Store, A full disk: "Changes
// made by CONNECT, SUBSCRIBE or UNSUBSCRIBE are refused with that packet's
// code"; "The broker continues after that answer". MQTT 5 3.11.3, Table 3-9:
// 0x11 "No subscription existed".
//
// On a filesystem of its own, an MQTT 5 client and an MQTT 3.1.1 client
// hold durable sessions (MQTT 3.1.1: Clean Session 0); a ballast then takes
// every byte left. Each subscribes to a filter, $SYS/# and another filter:
// MQTT 5 gets 0x97, 0x87, 0x97, MQTT 3.1.1 0x80 for each, and both
// connections answer PINGREQ. Once the ballast is emptied, the MQTT 5
// client's UNSUBSCRIBE of the two filters gets 0x11 for each, as neither was
// held, and the same SUBSCRIBE gets 0x01, 0x87, 0x02.
func TestFullDiskRefusesASubscribe(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "full-5", CleanStart: true, Props: support.Durable})
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "full-3.1.1"})
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	subs := slices.Concat(support.At(1, "full/a", "$SYS/#"), support.At(2, "full/b"))
	support.Suback(t, v5, nil, subs, 0x97, 0x87, 0x97)
	support.Suback(t, v311, nil, subs, 0x80, 0x80, 0x80)
	for _, c := range []*harness.Client{v5, v311} {
		if err := c.Ping(); err != nil {
			t.Fatalf("after the SUBSCRIBE refused on a full disk: %v; want the connection to stay", err)
		}
	}

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	unsuback(t, v5, nil, []string{"full/a", "full/b"}, 0x11, 0x11)
	support.Suback(t, v5, nil, subs, 0x01, 0x87, 0x02)
}

// design.md, Flows, SUBSCRIBE, Refusals: "0x97 for a filter [...] that a full
// disk cannot hold"; "MQTT 3.1.1 gets 0x80 for each"; Store, A full disk:
// "Group refusal follows Writes. Publishes are refused with 0x97. Changes
// made by CONNECT, SUBSCRIBE or UNSUBSCRIBE are refused with that packet's
// code"; "A write request answered "full" is discarded. The core does not
// retain its body for retry."; Writes: "A full refusal rolls back the
// whole group". Words: Channel, "A subscription's filter reaches the channels
// its topics land in [...] The filter also reaches $broadcast where a
// matching topic lands in no configured channel"; the subscription table,
// append: "the floor". MQTT 5 3.11.3, Table 3-9: 0x11 "No subscription
// existed".
//
// On a filesystem of its own, two append channels, whole-a (fd/a/#) and
// whole-b (fd/b/#); a publisher stores a1 on fd/a/1 and b1 on fd/b/1. An
// MQTT 5 client and an MQTT 3.1.1 client hold durable sessions; a ballast
// then takes every byte left. Each subscribes to fd/# at QoS 1, which
// reaches both channels and $broadcast, and to fd/b/1 at QoS 0: MQTT 5
// gets 0x97, 0x97, MQTT 3.1.1 0x80, 0x80. Once the ballast is emptied, the
// database holds no subscription, start or position of the MQTT 5
// client's, and its UNSUBSCRIBE of both filters gets 0x11, 0x11. The same
// SUBSCRIBE then gets 0x01, 0x00 for each client, and each receives a1 and
// b1, at QoS 1, once each, from the floor.
func TestFullDiskRefusesASubscribeWhole(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), Channels: []harness.Channel{
		{Name: "whole-a", Type: "append", Filter: "fd/a/#"},
		{Name: "whole-b", Type: "append", Filter: "fd/b/#"},
	}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "whole-pub", CleanStart: true})
	stored := []*harness.Publish{{QoS: 1, Topic: "fd/a/1", Payload: []byte("a1")}, {QoS: 1, Topic: "fd/b/1", Payload: []byte("b1")}}
	support.PublishAll(t, pub, stored)
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "whole-5", CleanStart: true, Props: support.Durable})
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "whole-3.1.1"})
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	subs := slices.Concat(support.At(1, "fd/#"), support.At(0, "fd/b/1"))
	support.Suback(t, v5, nil, subs, 0x97, 0x97)
	support.Suback(t, v311, nil, subs, 0x80, 0x80)
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	expectStored(t, support.OpenDB(t, b.DB), "whole-5", &sessionSubscriptions{}, "after the SUBSCRIBE refused on a full disk")
	unsuback(t, v5, nil, support.FiltersOf(subs), 0x11, 0x11)
	for _, c := range []*harness.Client{v5, v311} {
		support.Suback(t, c, nil, subs, 0x01, 0x00)
		var got []string
		for range stored {
			p := harness.Expect[*harness.Publish](t, c)
			if p.QoS != 1 {
				t.Errorf("%q came at QoS %d; want 1, the highest contributing grant", p.Payload, p.QoS)
			}
			got = append(got, string(p.Payload))
		}
		slices.Sort(got)
		if !slices.Equal(got, []string{"a1", "b1"}) {
			t.Errorf("after the SUBSCRIBE taken, the client received %q; want a1 and b1, once each, from the floor", got)
		}
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A volatile session's
// positions, and positions on latest and $broadcast, still end with the
// last subscription reaching the channel". storage.md,
// subscriptions: "a SUBSCRIBE or an UNSUBSCRIBE writes only its own rows";
// subscription_starts: "written with its subscription at SUBSCRIBE. It is
// deleted with the subscription at UNSUBSCRIBE"; positions: "A durable
// session's place in each channel it consumes". MQTT 5 3.10.4: "If any filter
// matches exactly then its owning Subscription MUST be deleted
// [MQTT-3.10.4-1]"; 3.11.3, Table 3-9: 0x00 "Success", "The subscription is
// deleted", and 0x11 "No subscription existed"; [MQTT-3.10.4-5]: "Even where
// no Topic Subscriptions are deleted, the Server MUST respond with an
// UNSUBACK".
//
// An MQTT 5 client holding a durable session subscribes to two filters,
// both on $broadcast, then unsubscribes from the first and from a filter
// it never held: UNSUBACK 0x00, 0x11. The database holds the second
// filter's subscription and start rows alone, and the position, which the
// second still reaches. An UNSUBSCRIBE of the second and of the first again
// gets 0x00, 0x11, and leaves the session no subscription, start or
// position rows.
func TestUnsubscribe(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "leaver", CleanStart: true, Props: support.Durable})
	tail := broadcastTail(t, db)
	support.Suback(t, c, nil, slices.Concat(support.At(1, "one/#"), support.At(0, "two/+")), 0x01, 0x00)
	unsuback(t, c, nil, []string{"one/#", "never/held"}, 0x00, 0x11)
	expectStored(t, db, "leaver", &sessionSubscriptions{
		subscriptions: []subscriptionRow{{filter: "two/+"}},
		starts:        []startRow{{"two/+", 1, tail}},
		positions:     []positionRow{{1, tail}},
	}, "after one of two subscriptions on $broadcast ended")
	unsuback(t, c, nil, []string{"two/+", "one/#"}, 0x00, 0x11)
	expectStored(t, db, "leaver", &sessionSubscriptions{}, "after the last subscription on $broadcast ended")
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "When the last subscription
// reaching an append channel or a queue is removed, a durable session keeps
// its position there"; "The kept position is saved in the UNSUBSCRIBE's own
// transaction"; "The next subscription reaching the channel reads from the
// kept position"; "A volatile session's positions, and positions on latest
// and $broadcast, still end with the last subscription reaching the
// channel". Words: Channel, "A subscription's filter reaches the channels
// its topics land in. Each topic belongs to its own channel under that rule.
// The filter also reaches $broadcast where a matching topic lands in no
// configured channel"; Consumer, "a consumer is a session's subscription to
// a channel, or the position a durable session keeps on it"; SUBSCRIBE, The
// start point: "A consumer has one position per channel, never one per
// filter. On an append channel every filter reads from that position. A
// filter added later therefore receives what the position has not yet
// passed. For a consumer that has caught up, that is only what comes
// next"; Delivery, Positions: "The position advances only past the
// contiguous settled or skipped prefix of examined records". storage.md:
// positions, "A place on an append channel or a queue stays until the
// session ends, also while no subscription reaches the channel";
// subscription_starts, a boundary "on each $broadcast or latest channel it
// reaches [...] deleted with the subscription at UNSUBSCRIBE [...] An
// append channel has no such boundary".
//
// Two append channels, left (u/a/#) and right (u/b/#); a publisher stores
// a1 on u/a/x and b1 on u/b/x. A client holding a durable session
// subscribes at QoS 1 to u/a/# and u/+/x in one SUBSCRIBE, which reach
// left, and left, right and $broadcast, and receives a1 and b1, once each,
// acknowledging both; then to u/b/#. The database holds one start, u/+/x's
// on $broadcast, and positions on $broadcast, left and right. An
// UNSUBSCRIBE of u/+/x, the last subscription reaching $broadcast, leaves no
// start and the positions on left and right alone: $broadcast's ends. One
// of u/a/#, the last reaching left, leaves both: left's is kept, past a1,
// at left's tail. The client subscribes to u/a/# again, which reads from
// the kept position, so a1 does not come again; then to u/+/x, a filter
// added later on left and right, whose consumers have caught up: of a2 on
// u/a/x and b2 on u/b/x, stored next, it receives exactly those two, once
// each.
func TestPositionEndsWithTheLastSubscriptionReachingIt(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "left", Type: "append", Filter: "u/a/#"},
		{Name: "right", Type: "append", Filter: "u/b/#"},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "positions-pub", CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "u/a/x", Payload: []byte("a1")},
		{QoS: 1, Topic: "u/b/x", Payload: []byte("b1")},
		{QoS: 1, Topic: "u/a/x", Payload: []byte("a2")},
		{QoS: 1, Topic: "u/b/x", Payload: []byte("b2")},
	}
	support.PublishAll(t, pub, ps[:2])
	stored := storedChannels(t, b.DB)
	left, right := stored["left"].id, stored["right"].id
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "positioned", CleanStart: true, Props: support.Durable})
	tail := broadcastTail(t, db)
	// receive reads len(want) deliveries at QoS 1 and acknowledges each,
	// failing the test unless they are want's, once each, in any order:
	// channels supply them in turn.
	receive := func(want ...*harness.Publish) {
		t.Helper()
		got, wanted := make([]string, 0, len(want)), make([]string, 0, len(want))
		for _, p := range want {
			d := harness.Expect[*harness.Publish](t, c)
			acknowledge(t, c, d.ID, 0)
			got, wanted = append(got, d.Topic+" "+string(d.Payload)), append(wanted, p.Topic+" "+string(p.Payload))
		}
		slices.Sort(wanted)
		slices.Sort(got)
		if !slices.Equal(got, wanted) {
			t.Fatalf("the client received %q; want %q, once each", got, wanted)
		}
	}
	support.Suback(t, c, nil, support.At(1, "u/a/#", "u/+/x"), 0x01, 0x01)
	receive(ps[0], ps[1])
	support.Suback(t, c, nil, support.At(1, "u/b/#"), 0x01)

	// held checks the session's starts, by filter, and the channels of its
	// positions, whose offsets delivery moves.
	held := func(starts []startRow, channels []int64, when string) {
		t.Helper()
		slices.SortFunc(starts, func(a, b startRow) int {
			return cmp.Or(cmp.Compare(a.filter, b.filter), cmp.Compare(a.channel, b.channel))
		})
		slices.Sort(channels)
		got := storedSubscriptions(t, db, "positioned")
		positions := make([]int64, 0, len(got.positions))
		for _, p := range got.positions {
			positions = append(positions, p.channel)
		}
		if !slices.Equal(got.starts, starts) || !slices.Equal(positions, channels) {
			t.Errorf("%s, subscription_starts holds %+v and positions %+v; want %+v and positions on channels %v",
				when, got.starts, got.positions, starts, channels)
		}
	}
	held([]startRow{{"u/+/x", 1, tail}}, []int64{1, left, right}, "with u/a/#, u/+/x and u/b/# held")
	unsuback(t, c, nil, []string{"u/+/x"}, 0x00)
	held(nil, []int64{left, right}, "after u/+/x ended, the last subscription reaching $broadcast")
	unsuback(t, c, nil, []string{"u/a/#"}, 0x00)
	held(nil, []int64{left, right}, "after u/a/# ended too, the last subscription reaching left")
	want := tailOf(t, db, "left")
	if got, _ := storedPosition(t, db, "positioned", "left"); got != want {
		t.Errorf("after u/a/# ended, the kept position on left is %d; want %d, past a1, which the client acknowledged", got, want)
	}

	support.Suback(t, c, nil, support.At(1, "u/a/#"), 0x01)
	support.Suback(t, c, nil, support.At(1, "u/+/x"), 0x01)
	support.PublishAll(t, pub, ps[2:])
	receive(ps[2], ps[3])
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A volatile session's
// positions, and positions on `latest` and `$broadcast`, still end with the
// last subscription reaching the channel"; the subscription table:
// "`append`, or a queue's own topics (a reader) | the floor, or the tail
// where the channel sets `start: tail`" and "`latest` | the current values,
// as Retain Handling allows, then each change"; Channels, latest:
// "Returning consumers receive surviving tombstones as empty payloads".
// storage.md, positions: "A place on an `append` channel or a queue stays
// until the session ends", which a latest channel's place does not.
//
// On an append channel, vol (v/#), a publisher stores v1. A client holding
// a volatile session subscribes at QoS 1 to v/#, receives v1 and
// acknowledges it, and unsubscribes: its position ends, so its next
// SUBSCRIBE to v/# reads from the floor, v1 again, then a record stored
// after it. On the latest channel kv, the publisher stores kv/a and kv/b.
// A client holding a durable session subscribes at QoS 1 to kv/#: its
// initial pass is both, and the database holds its position on kv. Its
// UNSUBSCRIBE leaves the session no subscription, start or position rows.
// The publisher deletes kv/a and replaces kv/b; the client's next
// SUBSCRIBE to kv/# starts as a new subscription: its initial pass is kv/b's
// current value alone, RETAIN 1, never kv/a's tombstone, which only a
// position kept on kv would be owed, then a change stored after it.
func TestVolatileAndLatestPositionsEndWithTheLastSubscription(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "vol", Type: "append", Filter: "v/#"}, kvChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ends-pub", CleanStart: true})

	t.Run("a volatile session on an append channel", func(t *testing.T) {
		v1 := &harness.Publish{QoS: 1, Topic: "v/x", Payload: []byte("v1")}
		support.Acknowledged(t, pub, v1)
		c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "volatile-leaver", CleanStart: true}, support.At(1, "v/#"))
		acknowledge(t, c, support.DeliveredAt(t, c, 1, v1.Topic, v1.Payload).ID, 0)
		unsuback(t, c, nil, []string{"v/#"}, 0x00)
		support.Suback(t, c, nil, support.At(1, "v/#"), 0x01)
		acknowledge(t, c, support.DeliveredAt(t, c, 1, v1.Topic, v1.Payload).ID, 0)
		thenLiveAt(t, pub, c, 1, "v/after")
	})

	t.Run("a durable session on a latest channel", func(t *testing.T) {
		db := support.OpenDB(t, b.DB)
		kv := storedChannels(t, b.DB)[kvChannel.Name].id
		support.PublishAll(t, pub, []*harness.Publish{value("kv/a", "a"), value("kv/b", "b1")})
		c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "latest-leaver", CleanStart: true, Props: support.Durable}, support.At(1, "kv/#"))
		currentPass(t, c, 1, map[string][]byte{"kv/a": []byte("a"), "kv/b": []byte("b1")})
		if got := storedSubscriptions(t, db, "latest-leaver").positions; !slices.ContainsFunc(got, func(p positionRow) bool { return p.channel == kv }) {
			t.Fatalf("with kv/# held, positions holds %+v; want one on kv, channel %d", got, kv)
		}
		unsuback(t, c, nil, []string{"kv/#"}, 0x00)
		expectStored(t, db, "latest-leaver", &sessionSubscriptions{}, "after kv/# ended, the last subscription reaching kv")

		support.PublishAll(t, pub, []*harness.Publish{value("kv/a", ""), value("kv/b", "b2")})
		support.Suback(t, c, nil, support.At(1, "kv/#"), 0x01)
		currentPass(t, c, 1, map[string][]byte{"kv/b": []byte("b2")})
		thenLiveAt(t, pub, c, 1, "kv/after")
	})
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A delivery prepared but not yet handed to the writer is cancelled and
// never sent when no remaining subscription is owed it, as when the
// connection ends. A durable session's stored preparation for it is removed
// in the UNSUBSCRIBE's own transaction"; "In-flight messages are finished"; Delivery,
// Preparation: "each consumer's live scan may select records ahead of the
// client's free window"; Order: "a channel's offset order on $broadcast,
// append". MQTT 5 3.10.4: the Server "MUST stop adding any new messages which
// match the Topic Filters, for delivery to the Client" [MQTT-3.10.4-2], and
// "MUST complete the delivery of any QoS 1 or QoS 2 messages which match the
// Topic Filters and it has started to send to the Client" [MQTT-3.10.4-3];
// MQTT 3.1.1 3.10.4 the same.
//
// On an append channel (cx/#) and on $broadcast (bx/), a client with
// Receive Maximum 1 subscribes at QoS 1 to <p>/a/# and <p>/+/keep, and a
// publisher stores r1 on <p>/a/1, r2 on <p>/a/2, r3 on <p>/a/keep and r4
// on <p>/a/3. The client receives r1 and holds it, so the records behind
// it are at most prepared, never sent. It unsubscribes from <p>/a/#,
// UNSUBACK 0x00, then acknowledges r1. It receives r3, which <p>/+/keep
// still owes, then a fence the publisher stores on <p>/b/keep: never r2
// or r4, which only the ended filter was owed, and r3 once.
func TestUnsubscribeCancelsWhatIsPreparedForIt(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "cancelled", Type: "append", Filter: "cx/#"}}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "cancel-pub", CleanStart: true})
	for _, prefix := range []string{"cx", "bx"} {
		t.Run(map[string]string{"cx": "an append channel", "bx": "$broadcast"}[prefix], func(t *testing.T) {
			c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: prefix + "-cancelling", CleanStart: true,
				Props: receiveMaximum5(1)}, support.At(1, prefix+"/a/#", prefix+"/+/keep"))
			ps := []*harness.Publish{
				{QoS: 1, Topic: prefix + "/a/1", Payload: []byte("r1")},
				{QoS: 1, Topic: prefix + "/a/2", Payload: []byte("r2")},
				{QoS: 1, Topic: prefix + "/a/keep", Payload: []byte("r3")},
				{QoS: 1, Topic: prefix + "/a/3", Payload: []byte("r4")},
				{QoS: 1, Topic: prefix + "/b/keep", Payload: []byte("fence")},
			}
			support.PublishAll(t, pub, ps[:4])
			held := support.DeliveredAt(t, c, 1, ps[0].Topic, ps[0].Payload)
			unsuback(t, c, nil, []string{prefix + "/a/#"}, 0x00)
			acknowledge(t, c, held.ID, 0)
			acknowledge(t, c, support.DeliveredAt(t, c, 1, ps[2].Topic, ps[2].Payload).ID, 0)
			support.Acknowledged(t, pub, ps[4])
			support.DeliveredAt(t, c, 1, ps[4].Topic, ps[4].Payload)
		})
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "an unknown saguin- property
// does not itself refuse an UNSUBSCRIBE: it is reported on the UNSUBACK as
// saguin-unread unless the client set Request Problem Information to 0";
// Sagüin properties: "saguin-unread | an UNSUBACK, naming a saguin-
// property the UNSUBSCRIBE carried that Sagüin does not read"; storage.md:
// "Sagüin's own saguin- properties are User Properties". MQTT 5 3.1.2.11.7:
// "If the value of Request Problem Information is 0, the Server [...] MUST
// NOT send a Reason String or User Properties on any packet other than
// PUBLISH, CONNACK, or DISCONNECT [MQTT-3.1.2-29]".
//
// An MQTT 5 client subscribes to a filter, then unsubscribes from it with
// the User Property saguin-x: UNSUBACK 0x00, carrying saguin-unread naming
// saguin-x. A client that set Request Problem Information to 0 does the
// same: UNSUBACK 0x00, with neither a User Property nor a Reason String.
func TestUnsubackNamesAnUnreadProperty(t *testing.T) {
	unread := harness.Properties{harness.User("saguin-x", "1")}
	t.Run("reported", func(t *testing.T) {
		c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		support.Suback(t, c, nil, support.At(1, "unread/#"), 0x01)
		ack := unsuback(t, c, unread, []string{"unread/#"}, 0x00)
		if got := ack.Props.User("saguin-unread"); !slices.Equal(got, []string{"saguin-x"}) {
			t.Errorf("the UNSUBACK's saguin-unread User Properties are %q, want one naming saguin-x", got)
		}
	})
	t.Run("Request Problem Information 0", func(t *testing.T) {
		c, _ := harness.Connected(t, brokerAddr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true,
			Props: harness.Properties{{ID: harness.RequestProblemInformation, Int: 0}}})
		support.Suback(t, c, nil, support.At(1, "unread/#"), 0x01)
		ack := unsuback(t, c, unread, []string{"unread/#"}, 0x00)
		for _, p := range ack.Props {
			if p.ID == harness.UserProperty || p.ID == harness.ReasonString {
				t.Errorf("the UNSUBACK carries %+v; want no User Property or Reason String [MQTT-3.1.2-29]", p)
			}
		}
	})
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A full disk refuses it with 0x80
// on each filter. MQTT 3.1.1 has no code, so its connection is closed";
// Store, A full disk: "Changes made by CONNECT, SUBSCRIBE or UNSUBSCRIBE are
// refused with that packet's code". MQTT 5 3.11.3, Table 3-9: 0x80
// "Unspecified error", "The unsubscribe could not be completed".
//
// On a filesystem of its own, an MQTT 5 client holding a durable session
// subscribes to two filters, and an MQTT 3.1.1 client holding one (Clean
// Session 0) to a third; a ballast then takes every byte left. The MQTT 5
// client's UNSUBSCRIBE of its two filters gets 0x80 for each, and its
// connection answers PINGREQ; the MQTT 3.1.1 client's UNSUBSCRIBE closes
// its connection, nothing sent first. Once the ballast is emptied, the
// MQTT 5 client's UNSUBSCRIBE of its two filters gets 0x00 for each: both
// were still held.
func TestFullDiskRefusesAnUnsubscribe(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "full-5", CleanStart: true, Props: support.Durable})
	support.Suback(t, v5, nil, slices.Concat(support.At(1, "gone/a"), support.At(0, "gone/b")), 0x01, 0x00)
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "full-3.1.1"})
	support.Suback(t, v311, nil, support.At(1, "gone/c"), 0x01)
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	unsuback(t, v5, nil, []string{"gone/a", "gone/b"}, 0x80, 0x80)
	if err := v5.Ping(); err != nil {
		t.Fatalf("after the UNSUBSCRIBE refused on a full disk: %v; want the connection to stay", err)
	}
	if err := v311.Send(&harness.Unsubscribe{ID: v311.NextID(), Filters: []string{"gone/c"}}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, v311)

	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	unsuback(t, v5, nil, []string{"gone/a", "gone/b"}, 0x00, 0x00)
}

// design.md, Flows, Replies: SUBACK "where they confirm a change, [is] sent
// only once the store confirms. Such a reply goes only on the connection
// that asked: when the store confirms after that connection has ended or
// been taken over, what was stored stays and the reply is dropped"; R4:
// "A reply confirming a change waits for that change's commit"; Store
// reads: "An ended or superseded request has no further effects"; Restart:
// durable sessions' subscriptions "loaded only at its CONNECT". MQTT 5
// 3.2.2.1.1 [MQTT-3.2.2-3]: Session Present 1 for a session kept.
//
// The test holds the database's write lock, as another program on the file
// may, while an MQTT 5 client holding a durable session subscribes; the
// broker runs under strace, and once its store writer has met the lock the
// client closes its connection, the SUBSCRIBE's change still at the store.
// The client connects again with Clean Start 0; with the lock held nothing
// answers it for heldPast. Once the lock is released, the first packet is
// the CONNACK, Session Present 1, and a PINGREQ's PINGRESP comes next: the
// SUBACK of the ended connection is not sent on the new one. The database
// holds the subscription, and its UNSUBSCRIBE gets 0x00: the subscription
// was stored, and loaded at the CONNECT.
func TestLateSubackIsDropped(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{})
	connect := &harness.Connect{ClientID: "late", CleanStart: true, Props: support.Durable}
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	connect.CleanStart = false
	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	if err := c.Send(&harness.Subscribe{ID: c.NextID(), Subs: support.At(1, "late/#")}); err != nil {
		t.Fatal(err)
	}
	awaitLockMet(t, b.trace, before, "the SUBSCRIBE")
	_ = c.Close() // the connection ends, its SUBACK still awaiting the store

	again, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Close() }) // the test is over
	if err := again.Send(connect); err != nil {
		t.Fatal(err)
	}
	nothingWithin(t, again, heldPast, "with the SUBSCRIBE's change still at the store, after the CONNECT resuming the session")
	lock.release(t)

	if ack := harness.Expect[*harness.Connack](t, again); ack.Reason != 0 || !ack.SessionPresent {
		t.Fatalf("CONNACK 0x%02X, Session Present %v; want 0x00 with the session present", ack.Reason, ack.SessionPresent)
	}
	if err := again.Ping(); err != nil {
		t.Fatalf("after the CONNACK: %v; want the PINGRESP next, the ended connection's SUBACK dropped", err)
	}
	expectStored(t, support.OpenDB(t, b.DB), "late", &sessionSubscriptions{
		subscriptions: []subscriptionRow{{filter: "late/#", qos: 1}},
		starts:        []startRow{{"late/#", 1, 1}},
		positions:     []positionRow{{1, 1}},
	}, "once the store confirmed the ended connection's SUBSCRIBE")
	unsuback(t, again, nil, []string{"late/#"}, 0x00)
}

// design.md, Restart, Graceful stop: "MQTT listeners close"; "Every client
// gets 0x8B, except one whose DISCONNECT the core has taken. The 0x8B follows
// the PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK of packets taken
// before the stop whose changes the stop stores (R4)"; Flows, Replies:
// "Acknowledgements of stored changes preserve their packet order"; SUBACK
// and UNSUBACK "sent only once the store confirms". 0010a.md, 261022 144815:
// "a SUBSCRIBE (or UNSUBSCRIBE) whose durable change the stop stores is
// acknowledged before the connection's 0x8B".
//
// The test holds the database's write lock while an MQTT 5 client holding
// a durable session, subscribed to one filter, sends at once a SUBSCRIBE of
// another and an UNSUBSCRIBE of the first, then a PINGREQ; the broker runs
// under strace. Once its store writer has met the lock, the PINGRESP, read
// first, says the core has taken both packets (design.md, Execution,
// Decisions: "Each connection's packet order is preserved"), their replies
// waiting for the store. SIGTERM goes; once the listener refuses a
// connection, the stop has begun, and the lock is released. The client
// reads SUBACK 0x01, then UNSUBACK 0x00, then DISCONNECT 0x8B and the end
// of stream, and the broker exits 0.
func TestStopAcknowledgesSubscriptionsFirst(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "stopping", CleanStart: true, Props: support.Durable})
	support.Suback(t, c, nil, support.At(1, "stop/held"), 0x01)

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	sub := &harness.Subscribe{ID: c.NextID(), Subs: support.At(1, "stop/new")}
	unsub := &harness.Unsubscribe{ID: c.NextID(), Filters: []string{"stop/held"}}
	harness.SendBytes(t, c, slices.Concat(harness.Encoded(t, sub, harness.V5), harness.Encoded(t, unsub, harness.V5)))
	awaitLockMet(t, b.trace, before, "the SUBSCRIBE")
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the SUBSCRIBE and UNSUBSCRIBE: %v; want the PINGRESP first, their replies waiting for the store", err)
	}
	b.terminate(t)
	listenerClosed(t, b.Addr)
	lock.release(t)

	if p, err := c.Recv(); err != nil {
		t.Fatalf("after SIGTERM: %v; want SUBACK 0x01, UNSUBACK 0x00, then DISCONNECT 0x8B", err)
	} else if a, ok := p.(*harness.Suback); !ok || a.ID != sub.ID || !bytes.Equal(a.Reasons, []byte{0x01}) {
		t.Fatalf("after SIGTERM the client read %s %+v first; want SUBACK 0x01 to its SUBSCRIBE", harness.Name(p.Type()), p)
	}
	if p, err := c.Recv(); err != nil {
		t.Fatalf("after the SUBACK: %v; want UNSUBACK 0x00, then DISCONNECT 0x8B", err)
	} else if a, ok := p.(*harness.Unsuback); !ok || a.ID != unsub.ID || !bytes.Equal(a.Reasons, []byte{0x00}) {
		t.Fatalf("after the SUBACK the client read %s %+v; want UNSUBACK 0x00 to its UNSUBSCRIBE", harness.Name(p.Type()), p)
	}
	shutDown(t, c)
	_ = c.Close() // the broker has ended its end
	if state := support.Exited(t, b.Broker); !state.Success() {
		t.Errorf("the graceful stop ended %v, want exit status 0", state)
	}
}

// listenerClosed waits until the MQTT listener at addr refuses a
// connection, as it does once a graceful stop has begun (design.md,
// Restart, Graceful stop: "MQTT listeners close"), failing the test where
// it still accepts after harness.DefaultTimeout. A connection it accepts
// meanwhile is closed at once.
func listenerClosed(t *testing.T, addr string) {
	t.Helper()
	refused := func() bool {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close() // a probe, never used
			return false
		}
		return errors.Is(err, syscall.ECONNREFUSED)
	}
	if !testkit.Within(harness.DefaultTimeout, refused) {
		t.Fatalf("the listener at %s still accepts connections %v after SIGTERM", addr, harness.DefaultTimeout)
	}
}
