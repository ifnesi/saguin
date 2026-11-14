package conformance

import (
	"crypto/rand"
	"database/sql"
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

// The tests of records arriving over a bridge from its peer (design.md,
// Bridges, In; Flows, PUBLISH; storage.md, records and held_publishes;
// metrics.md, Bridges and Publishing) whose waits are short. Each starts a
// broker of its own with a bridge to a test peer, harness.Peer, on
// loopback, whose link is up with every inbound filter granted
// (support.Inbound): the peer plays the peer broker forwarding what its
// own clients published, at QoS 0, 1 and 2, and reads the bridge's
// PUBACK, PUBREC and PUBCOMP with their receipt times. Local clients
// subscribe to see what landed. The test whose waits run to seconds,
// ack_interval's, is in the intervals package.

// fromPeer is a PUBLISH the peer forwards to the bridge: on topic, at qos,
// under packet identifier id, which QoS 0 leaves out, carrying payload.
func fromPeer(qos byte, id uint16, topic, payload string) *harness.Publish {
	return &harness.Publish{QoS: qos, ID: id, Topic: topic, Payload: []byte(payload)}
}

// heldOf returns the packet identifiers of the held_publishes rows db holds
// for the bridge named, in order: storage.md,
// held_publishes, "session | the publishing session's id, durable or not; 0
// for a bridge", "bridge | the bridge's id; 0 for a session"; bridges,
// "name | unique: the bridge's name".
func heldOf(t *testing.T, db support.RowsQuerier, bridge string) []uint16 {
	t.Helper()
	var ids []uint16
	support.Query(t, db, `SELECT h.packet_id FROM held_publishes h JOIN bridges b ON b.id = h.bridge
		WHERE b.name = ? AND h.session = 0 ORDER BY h.packet_id`, func(rows *sql.Rows) error {
		var id uint16
		err := rows.Scan(&id)
		ids = append(ids, id)
		return err
	}, bridge)
	return ids
}

// TestBridgedRecordLandsOnItsRulesTopic: design.md, Bridges: "On that
// connection Sagüin is an ordinary MQTT 5 client of the peer. The receiving
// broker's own channels decide where a record lands."; config.md,
// bridges.<name>: "A topic template is required for in and out, and refused
// for both"; design.md, Bridges: "In a topic template, $1, $2 and so on stand for the filter's + levels. Each must name a level the
// filter has and may occur anywhere in a level. The $# stands for the
// filter's # and must be the topic's whole last level."; "Inbound, the
// rule's QoS takes effect through the subscription the bridge asks for, and a
// record is stored at the QoS it arrives with."; Flows, SUBSCRIBE, One copy:
// "The delivery uses the lower of the publication's QoS and the highest
// contributing grant". MQTT 5 4.3.2: the receiver "MUST respond with a
// PUBACK packet" [MQTT-4.3.2-4]; 4.3.3: it "MUST respond with a PUBREC"
// [MQTT-4.3.3-8], and to a PUBREL "by sending a PUBCOMP packet"
// [MQTT-4.3.3-11], whichever QoS its subscription was granted.
//
// A bridge has in rules on a/# at QoS 2 (topic from/a/$#), b/+ at QoS 1
// (from/b/$1) and m/+/+ at QoS 1 (mid/$2-$1), and a both rule on c at QoS
// 1. A local client subscribes to from/#, mid/# and c at QoS 2, so each
// delivery carries its record's QoS. The peer forwards, each exchange
// complete before the next, and the subscriber's next packet is each
// record, on its rule's topic, at the QoS it arrived with:
//   - a/x/y at QoS 2: from/a/x/y at QoS 2; a/x at QoS 1: from/a/x at QoS 1;
//     a/z at QoS 0: from/a/z at QoS 0;
//   - b/k at QoS 2, which only a peer breaking MQTT-3.8.4-8 sends on a rule
//     at QoS 1, answered PUBREC, then PUBCOMP to the peer's PUBREL:
//     from/b/k at QoS 2, the QoS it arrived with;
//   - m/p/q: mid/q-p, each + level where its $n stands inside the level;
//   - c, on the both rule: c, its own topic.
func TestBridgedRecordLandsOnItsRulesTopic(t *testing.T) {
	t.Parallel()
	b, _, l := support.Inbound(t, &harness.Options{}, "landing",
		support.In("a/#", 2), support.In("b/+", 1),
		harness.BridgeRule{Filter: "m/+/+", Topic: "mid/$2-$1", Direction: "in", QoS: 1},
		harness.BridgeRule{Filter: "c", Direction: "both", QoS: 1})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "landing-sub", CleanStart: true},
		support.At(2, "from/#", "mid/#", "c"))

	for _, c := range []struct {
		name  string
		p     *harness.Publish
		topic string
		qos   byte
	}{
		{"a QoS 2 record on a rule at QoS 2", fromPeer(2, 1, "a/x/y", "a2"), "from/a/x/y", 2},
		{"a QoS 1 record on a rule at QoS 2", fromPeer(1, 2, "a/x", "a1"), "from/a/x", 1},
		{"a QoS 0 record on a rule at QoS 2", fromPeer(0, 0, "a/z", "a0"), "from/a/z", 0},
		{"a QoS 2 record on a rule at QoS 1", fromPeer(2, 3, "b/k", "b2"), "from/b/k", 2},
		{"each $n inside its level", fromPeer(1, 4, "m/p/q", "m1"), "mid/q-p", 1},
		{"a both rule's own topic", fromPeer(1, 5, "c", "c1"), "c", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			support.Bridged(t, l, c.p)
			support.DeliveredAt(t, sub, c.qos, c.topic, c.p.Payload)
		})
	}
}

// TestBridgedTopicOverALimitAfterSubstitution: design.md, Bridges: "Topic
// limits are checked after substitution as well as at configuration load. An
// invalid inbound result is refused with 0x90, or dropped at QoS 0.";
// "Filters, with {a,b} expanded, and topics keep limits.max_topic_length and
// limits.max_topic_levels."; Flows, PUBLISH: "A QoS 2 refusal rides the
// PUBREC. At QoS 0 a refused publish is dropped and counted."
//
// With max_topic_length 24 and max_topic_levels 4, a bridge has an in rule
// on a/# at QoS 2 whose topic is from/a/$#, and a local client subscribes
// to from/# at QoS 2. In each case the peer's topic keeps both limits and
// its result breaks one: a/ and 19 letters, 21 bytes, gives 26; a/1/2/3,
// four levels, gives five. At QoS 1 it gets PUBACK 0x90, at QoS 2 PUBREC
// 0x90; at QoS 0 nothing answers it, and a record the peer then forwards at
// QoS 1 on a/ok is the subscriber's next packet: none of the three landed.
func TestBridgedTopicOverALimitAfterSubstitution(t *testing.T) {
	t.Parallel()
	b, _, l := support.Inbound(t, &harness.Options{MaxTopicLength: 24, MaxTopicLevels: 4}, "limited", support.In("a/#", 2))
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "limited-sub", CleanStart: true}, support.At(2, "from/#"))

	id := uint16(0)
	for _, c := range []struct{ name, topic string }{
		{"over max_topic_length", "a/" + strings.Repeat("x", 19)},
		{"over max_topic_levels", "a/1/2/3"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, qos := range []byte{1, 2} {
				id++
				support.PeerAnswered(t, l, fromPeer(qos, id, c.topic, "over"), 0x90)
			}
			support.PeerSends(t, l, fromPeer(0, 0, c.topic, "over"))
			id++
			next := fromPeer(1, id, "a/ok", c.name)
			support.Bridged(t, l, next)
			support.DeliveredAt(t, sub, 1, "from/a/ok", next.Payload)
		})
	}
}

// TestBridgedRecordMatchingNoRule: design.md, Bridges, In: "A record matching
// no inbound rule is refused as an invalid inbound result is, and logged.";
// Bridges: "An invalid inbound result is refused with 0x90, or dropped at
// QoS 0."; Bridges, Logging: "Every bridge line names its bridge."; "A record
// matching no inbound rule is one WARN line the first time on a link. Later
// records matching no inbound rule on that link are DEBUG lines."
//
// A bridge has an in rule on a/# at QoS 1 and an out rule on d/#, and a
// local client subscribes to # at QoS 2. The peer forwards records on z/1,
// which no rule's filter matches, and on d/1, which only the out rule's
// does. Each at QoS 1 gets PUBACK 0x90, at QoS 2 PUBREC 0x90; at QoS 0
// nothing answers it, and a record the peer then forwards at QoS 1 on a/ok
// is the subscriber's next packet. The broker logs at debug. After a
// graceful stop, the log holds one WARN line naming the bridge and z/1, the
// link's first such record, and DEBUG lines naming the bridge and d/1, the
// later ones.
func TestBridgedRecordMatchingNoRule(t *testing.T) {
	t.Parallel()
	b, _, l := support.Inbound(t, &harness.Options{LogLevel: "debug"}, "unmatched",
		support.In("a/#", 1), harness.BridgeRule{Filter: "d/#", Topic: "to/d/$#", Direction: "out", QoS: 1})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "unmatched-sub", CleanStart: true}, support.At(2, "#"))

	cases := []struct{ name, topic string }{
		{"a topic no rule's filter matches", "z/1"},
		{"a topic an out rule's filter alone matches", "d/1"},
	}
	id := uint16(0)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, qos := range []byte{1, 2} {
				id++
				support.PeerAnswered(t, l, fromPeer(qos, id, c.topic, "unmatched"), 0x90)
			}
			support.PeerSends(t, l, fromPeer(0, 0, c.topic, "unmatched"))
			id++
			next := fromPeer(1, id, "a/ok", c.name)
			support.Bridged(t, l, next)
			support.DeliveredAt(t, sub, 1, "from/a/ok", next.Payload)
		})
	}
	stopped(t, b)
	log := b.Log()
	if lines := testkit.LinesNaming(log, "WARN", cases[0].topic, cases[1].topic); len(lines) != 1 || !namesAll(lines[0], "unmatched", cases[0].topic) {
		t.Errorf("WARN lines naming an unmatched topic: %q; want one, naming the bridge and %s, the link's first", lines, cases[0].topic)
	}
	if lines := testkit.LinesNaming(log, "DEBUG", cases[1].topic); len(lines) == 0 || !namesAll(lines[0], "unmatched") {
		t.Errorf("DEBUG lines naming %s: %q; want the later unmatched records at DEBUG, naming the bridge", cases[1].topic, lines)
	}
}

// TestBridgedRecordsTakeTheOrdinaryPathAsNoUser: design.md, Bridges, In: "What
// arrives goes through the ordinary publish path as no user (PUBLISH)."; "The
// acl_file, publish_rate and publish_bytes never apply to a bridge, in or
// out."; Flows, PUBLISH, the refusal table: "0x87 | the acl_file refuses
// the topic, $saguin requests included; a topic under $SYS/ or on a
// dead-letter channel"; "0x83 | a saguin- property other than saguin-id";
// Sagüin properties: "The saguin-id property sets the record's id
// (PUBLISH)."; "The value of saguin-id is the publisher's own where it set
// one, kept as sent."; Flows, CONNECT: "A user matched by no users: pattern
// is accepted with no rights. Every publish and subscribe it makes is
// refused with 0x87, except a subscription to …/seek/reply, which seek
// grants."
func TestBridgedRecordsTakeTheOrdinaryPathAsNoUser(t *testing.T) {
	t.Parallel()
	// A broker reads an acl_file granting its one user read on elsewhere/#
	// alone, and has publish_rate 1 and publish_bytes 64. Its bridge's peer
	// forwards ten records of 256 bytes on a/n at QoS 1, back to back: each
	// gets PUBACK 0x00, in order, the last within two seconds of the first's
	// sending, where a publisher under those limits would have its next
	// packet read only once its byte bucket, in debt after the first, was
	// above zero again, three seconds on (design.md, PUBLISH, Rate limits).
	t.Run("neither the acl_file nor the publish limits", func(t *testing.T) {
		t.Parallel()
		o := aclOptions(t, "roles:\n  elsewhere:\n    - topic: \"elsewhere/#\"\n      allow: [read]\nusers:\n  someone: [elsewhere]\n", "someone")
		o.PublishRate, o.PublishBytes = 1, 64
		_, _, l := support.Inbound(t, o, "unlimited", support.In("a/#", 1))
		const n = 10
		var first time.Time
		for id := uint16(1); id <= n; id++ {
			at := support.PeerSends(t, l, &harness.Publish{QoS: 1, ID: id, Topic: "a/n", Payload: make([]byte, 256)})
			if id == 1 {
				first = at
			}
		}
		var last harness.Arrival
		for id := uint16(1); id <= n; id++ {
			last = support.PeerAck(t, l, harness.PUBACK, id, 0x00)
		}
		support.Between(t, "the ten PUBACKs, from the first PUBLISH's sending to the last PUBACK", last.At.Sub(first), 0, 2*time.Second)
	})

	// A broker has the queue orders, whose dead-letter channel orders__dlq
	// holds __dlq/orders/#, and the append channel ids on from/id/#. Its
	// bridge has in rules on s/+ (topic $1/s), dl/# (__dlq/orders/$#), a/#
	// (from/a/$#) and id/# (from/id/$#), each at QoS 1. The peer forwards
	// s/$SYS, which lands under $SYS/: PUBACK 0x87; dl/x, which lands on the
	// dead-letter channel: PUBACK 0x87; a/p carrying the User Property
	// saguin-x: PUBACK 0x83; and id/1 carrying saguin-id peer-chose-this:
	// PUBACK 0x00, and a local client subscribing to from/id/# then receives
	// it, from the floor, carrying saguin-id peer-chose-this alone.
	t.Run("the ordinary refusals, and saguin-id", func(t *testing.T) {
		t.Parallel()
		b, _, l := support.Inbound(t, &harness.Options{Channels: []harness.Channel{ordersQueue, {Name: "ids", Type: "append", Filter: "from/id/#"}}}, "ordinary",
			harness.BridgeRule{Filter: "s/+", Topic: "$1/s", Direction: "in", QoS: 1},
			harness.BridgeRule{Filter: "dl/#", Topic: "__dlq/orders/$#", Direction: "in", QoS: 1},
			support.In("a/#", 1), support.In("id/#", 1))
		support.PeerAnswered(t, l, fromPeer(1, 1, "s/$SYS", "system"), 0x87)
		support.PeerAnswered(t, l, fromPeer(1, 2, "dl/x", "dead"), 0x87)
		reserved := fromPeer(1, 3, "a/p", "reserved")
		reserved.Props = harness.Properties{harness.User("saguin-x", "1")}
		support.PeerAnswered(t, l, reserved, 0x83)
		named := fromPeer(1, 4, "id/1", "named")
		named.Props = harness.Properties{harness.User("saguin-id", "peer-chose-this")}
		support.Bridged(t, l, named)
		sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ordinary-sub", CleanStart: true}, support.At(1, "from/id/#"))
		got := support.DeliveredAt(t, sub, 1, "from/id/1", named.Payload)
		if ids := got.Props.User("saguin-id"); !slices.Equal(ids, []string{"peer-chose-this"}) {
			t.Errorf("the delivery carries saguin-id %q; want the peer's peer-chose-this alone, kept as sent", ids)
		}
	})
}

// TestFullDiskRefusesBridgedRecords: design.md, Store, A full disk:
// "Publishes are refused with 0x97."; "On a full disk or a full memory_dir,
// a write is answered "full" only when SQLite reports lack of space and
// rollback confirms that the transaction did not commit. The broker
// continues after that answer."; Bridges, In: "What arrives goes through
// the ordinary publish path as no user (PUBLISH)."; Flows, PUBLISH: "A QoS 2
// refusal rides the PUBREC. At QoS 0 a refused publish is dropped and
// counted."; Store, Writes: "A full refusal rolls back the whole group and
// answers every member "full", including members not executed."
//
// On a filesystem of its own, a broker's bridge has an in rule on a/# at
// QoS 2, and a local client subscribes to from/# at QoS 1; a ballast then
// takes every byte left. The peer's record at QoS 1 gets PUBACK 0x97, at
// QoS 2 PUBREC 0x97; one at QoS 0 is followed by one at QoS 1, PUBACK 0x97
// too, whose write follows the QoS 0 one's. Once the ballast is emptied, a
// record the peer forwards at QoS 1 gets PUBACK 0x00 on the same link, and
// is the subscriber's next packet: none of the refused landed.
func TestFullDiskRefusesBridgedRecords(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b, _, l := support.Inbound(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")}, "filled", support.In("a/#", 2))
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "filled-sub", CleanStart: true}, support.At(1, "from/#"))
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	support.PeerAnswered(t, l, fromPeer(1, 1, "a/1", "full"), 0x97)
	support.PeerAnswered(t, l, fromPeer(2, 2, "a/2", "full"), 0x97)
	support.PeerSends(t, l, fromPeer(0, 0, "a/0", "full"))
	support.PeerAnswered(t, l, fromPeer(1, 3, "a/3", "full"), 0x97)
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	after := fromPeer(1, 4, "a/after", "room again")
	support.Bridged(t, l, after)
	support.DeliveredAt(t, sub, 1, "from/a/after", after.Payload)
}

// TestBridgedQoS2: design.md, Flows, PUBLISH, the table: QoS 2 "The
// publication enters on PUBREL, from a held copy stored and charged to the
// channel before PUBREC. The held copy, offset and retained flag move in
// one transaction." | "PUBREC, then PUBCOMP once in the channel"; the
// refusal table: "0x97 | over [...] a channel's max_bytes"; Bridges, In:
// "What arrives goes through the ordinary publish path as no user
// (PUBLISH)."; "A rule at QoS 2 avoids it."; Restart: "Restored: the broker
// restores durable sessions with their subscriptions, positions, in-flight
// windows, held incoming QoS 2 exchanges and Wills with their due time.";
// Store, full mode: "Everything acknowledged survives a crash or a power
// cut."; storage.md, held_publishes: "They may come from any session or from
// a bridge's peer"; "session | the publishing session's id, durable or not; 0
// for a bridge"; "bridge | the bridge's id; 0 for a session"; "On PUBREL, the
// record takes its offset and retained flag, and the held row is deleted in
// the same transaction." MQTT 5 4.4: on reconnecting "the Server MUST resend
// any unacknowledged [...] PUBREL packets using their original Packet
// Identifiers [MQTT-4.4.0-1]"; 4.1: the Client's session state holds "QoS 2
// messages which have been received from the Server, but have not been
// completely acknowledged".
func TestBridgedQoS2(t *testing.T) {
	t.Parallel()
	// One broker has the channel capped (capped/#) with max_bytes 100. Its
	// bridge, held, has in rules on cap/# (topic capped/$#) and a/# (from/a/$#),
	// each at QoS 2, and a local client subscribes to from/# at QoS 2.
	t.Run("held and charged before PUBREC, entering on PUBREL, kept over a new link", func(t *testing.T) {
		t.Parallel()
		const name = "held"
		b, peer, l := support.Inbound(t, &harness.Options{Channels: capped(100)}, name,
			harness.BridgeRule{Filter: "cap/#", Topic: "capped/$#", Direction: "in", QoS: 2}, support.In("a/#", 2))
		sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "held-sub", CleanStart: true}, support.At(2, "from/#"))

		// The peer's QoS 2 record on cap/t, whose record is 60 bytes, gets
		// PUBREC 0x00, the database then holding its held row, the bridge's,
		// under its packet identifier. While it is held, one of 48 bytes at
		// QoS 1 gets PUBACK 0x97. Its PUBREL gets PUBCOMP 0x00, and one of 40
		// bytes, the room left, PUBACK 0x00: capped holds the two, 100 bytes.
		charged := &harness.Publish{QoS: 2, ID: 1, Topic: "cap/t", Payload: testkit.Filling("capped/t", 60)}
		support.PeerAnswered(t, l, charged, 0x00)
		if ids := heldOf(t, support.OpenDB(t, b.DB), name); !slices.Equal(ids, []uint16{charged.ID}) {
			t.Errorf("the database holds the bridge's held rows %v; want one, packet identifier %d", ids, charged.ID)
		}
		support.PeerAnswered(t, l, &harness.Publish{QoS: 1, ID: 2, Topic: "cap/t", Payload: testkit.Filling("capped/t", 48)}, 0x97)
		support.PeerReleases(t, l, charged.ID, 0x00)
		support.PeerAnswered(t, l, &harness.Publish{QoS: 1, ID: 3, Topic: "cap/t", Payload: testkit.Filling("capped/t", 40)}, 0x00)
		if n, size := cappedRecords(t, b.DB); n != 2 || size != 100 {
			t.Errorf("capped holds %d records of %d bytes; want 2 of 100: the held one charged once", n, size)
		}

		// The peer's QoS 2 record on a/held gets PUBREC 0x00; one it then
		// forwards at QoS 1 on a/after is the subscriber's next packet, the
		// held one not yet in. Its PUBREL gets PUBCOMP 0x00, and the held one
		// is the subscriber's next.
		held := fromPeer(2, 4, "a/held", "held")
		support.PeerAnswered(t, l, held, 0x00)
		after := fromPeer(1, 5, "a/after", "after")
		support.Bridged(t, l, after)
		support.DeliveredAt(t, sub, 1, "from/a/after", after.Payload)
		support.PeerReleases(t, l, held.ID, 0x00)
		support.DeliveredAt(t, sub, 2, "from/a/held", held.Payload)

		// The peer's QoS 2 record on a/dropped gets PUBREC 0x00, and the peer
		// drops the link. The bridge's next link, Session Present 1, up, the
		// peer resends the PUBREL: PUBCOMP 0x00, and the record is the
		// subscriber's next packet.
		dropped := fromPeer(2, 6, "a/dropped", "dropped")
		support.PeerAnswered(t, l, dropped, 0x00)
		_ = l.Close() // the peer drops the link
		again, _, _ := support.LinkUp(t, peer, support.QuietResumed)
		support.PeerReleases(t, again, dropped.ID, 0x00)
		support.DeliveredAt(t, sub, 2, "from/a/dropped", dropped.Payload)
	})

	// On a broker of its own, its bridge's peer forwards a QoS 2 record on
	// a/restarted, PUBREC 0x00, and the broker is killed. Started again on
	// the same files, its bridge's link up with Session Present 1, the peer
	// resends the PUBREL: PUBCOMP 0x00. After a graceful stop the database
	// holds the record once, at QoS 2, and no held row.
	t.Run("recovered across a restart", func(t *testing.T) {
		t.Parallel()
		peer := harness.ListenedPeer(t)
		o := &harness.Options{Dir: t.TempDir(), Bridges: []harness.Bridge{support.BridgeTo("restarted", peer, support.In("a/#", 2))}}
		b := support.Launch(t, o)
		l, _, _ := support.LinkUp(t, peer, support.Quiet)
		p := fromPeer(2, 9, "a/restarted", "held over a restart")
		support.PeerAnswered(t, l, p, 0x00)
		support.Crash(t, b)

		again := support.Launch(t, o)
		l, _, _ = support.LinkUp(t, peer, support.QuietResumed)
		support.PeerReleases(t, l, p.ID, 0x00)
		stopped(t, again)
		expectStoredOnce(t, again.DB, p.Payload, "after the PUBREL resent past the restart")
	})
}

// TestBridgedQoS2EntersOnSessionPresent0: design.md, Bridges, In: "When the
// peer's CONNACK has Session Present 0, every inbound QoS 2 record still
// awaiting its PUBREL enters its channel as a PUBREL would make it enter, in
// the order the records arrived, before anything the new link delivers.";
// Flows, PUBLISH, the table: QoS 2 "The publication enters on PUBREL, from
// a held copy stored and charged to the channel before PUBREC."; Words:
// Offset, "The core assigns it when a publication enters the channel, in
// entry order"; Delivery, Order: "deliveries follow a channel's offset
// order on $broadcast". MQTT 5 3.2.2.1.1: "If the Client does have Session
// State and receives Session Present set to 0 it MUST discard its Session
// State if it continues with the Network Connection [MQTT-3.2.2-5]."; 4.3.3:
// "After it has sent a PUBCOMP, the receiver MUST treat any subsequent
// PUBLISH packet that contains that Packet Identifier as being a new
// Application Message [MQTT-4.3.3-12]."
//
// A bridge, lost, has an in rule on a/# at QoS 2, and a local client
// subscribes to from/# at QoS 2. The peer forwards at QoS 2 a/first under
// packet identifier 7, then a/second under 3, each answered PUBREC 0x00,
// and drops the link without a PUBREL. The bridge's next CONNACK has
// Session Present 0; once its SUBSCRIBE is granted, the peer forwards
// a/new at QoS 2 under packet identifier 7 again, whose exchange completes.
// The subscriber's next packets are from/a/first, from/a/second and
// from/a/new, in that order, each at QoS 2: the held records in their
// arrival order, not their packet identifiers', then the new link's, a new
// message under a freed identifier.
func TestBridgedQoS2EntersOnSessionPresent0(t *testing.T) {
	t.Parallel()
	b, peer, l := support.Inbound(t, &harness.Options{}, "lost", support.In("a/#", 2))
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lost-sub", CleanStart: true}, support.At(2, "from/#"))
	first, second := fromPeer(2, 7, "a/first", "first to arrive"), fromPeer(2, 3, "a/second", "second to arrive")
	support.PeerAnswered(t, l, first, 0x00)
	support.PeerAnswered(t, l, second, 0x00)
	_ = l.Close() // the peer drops the link, its session lost

	l, _, _ = support.LinkUp(t, peer, support.Quiet)
	reused := fromPeer(2, first.ID, "a/new", "the new link's")
	support.Bridged(t, l, reused)
	for _, p := range []*harness.Publish{first, second, reused} {
		support.DeliveredAt(t, sub, 2, "from/"+p.Topic, p.Payload)
	}
}

// TestBridgedReplayInterrupted: design.md, Bridges: "A bridge uses MQTT 5,
// Clean Start 0 (1 while a replay is owed)"; "A CONNACK with Session Present
// 0 starts a replay of the bridge's state for the peer. Inbound QoS 2
// records awaiting PUBREL enter their channels (In). Unacknowledged outbound
// records are prepared again as new (Out). Before the replay starts, the
// bridge stores that a replay is owed. The replay's last step clears that
// mark in the same transaction. Until the mark is cleared, the bridge
// neither subscribes at the peer nor sends it a record. Every CONNECT it
// makes until then has Clean Start 1, so the peer answers Session Present 0
// again and the replay runs again. When the mark cannot be stored, the link
// ends. The bridge keeps the mark in memory alone until it is stored. A
// replay step refused for a full disk also ends the link. The held source
// stays stored, and the next link's replay takes it up again."; Bridges, In: "every inbound QoS 2 record still
// awaiting its PUBREL enters its channel as a PUBREL would make it enter,
// in the order the records arrived, before anything the new link
// delivers."; Restart, Restored: the broker restores "held incoming QoS 2
// exchanges"; storage.md, bridges: replay_owed, "1 from a CONNACK with
// Session Present 0 until its replay commits". MQTT 5 4.3.3: "After it has
// sent a PUBCOMP, the receiver MUST treat any subsequent PUBLISH packet that
// contains that Packet Identifier as being a new Application Message
// [MQTT-4.3.3-12]."
//
// On a filesystem of its own, a bridge, replaying-in, has an in rule on a/#
// at QoS 2. The peer forwards at QoS 2 a/first, of 256 KiB that do not
// compress, under packet identifier 7, then a/second under 3, each answered
// PUBREC 0x00. A ballast then takes every byte left, and the peer drops the
// link; it answers the next CONNECT with Session Present 0, and, the
// replay's mark finding no room, the bridge ends that link:
//   - the mark in memory: the next CONNECT has Clean Start 1. The ballast
//     emptied, that CONNECT left unanswered, the broker stops gracefully and
//     starts again on its files: its first CONNECT has Clean Start 1, the
//     mark kept until stored;
//   - a ballast again takes every byte but 64 KiB, room for the mark but not
//     for a/first's record; answered with Session Present 0, the database
//     comes to owe a replay with both held publishes still there; the broker
//     is killed;
//   - the mark stored: the ballast emptied, the broker starts again on its
//     files; its first CONNECT has Clean Start 1. A local client subscribes
//     to from/# at QoS 2. Answered with Session Present 0, the bridge's next
//     packet is its SUBSCRIBE, and when it comes the database owes no replay
//     and holds no held publish of the bridge's. Granted, the peer forwards
//     a/new at QoS 2 under packet identifier 7 again, whose exchange
//     completes. The subscriber's next packets are from/a/first,
//     from/a/second and from/a/new, in that order.
func TestBridgedReplayInterrupted(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	const name = "replaying-in"
	const room = 64 << 10 // the mark's write fits; moving a/first's record does not
	peer := harness.ListenedPeer(t)
	o := &harness.Options{Dir: t.TempDir(), FilePath: filepath.Join(disk, "saguin.db"),
		Bridges: []harness.Bridge{support.BridgeTo(name, peer, support.In("a/#", 2))}}
	b := support.Launch(t, o)
	l, _, _ := support.LinkUp(t, peer, support.Quiet)
	first := &harness.Publish{QoS: 2, ID: 7, Topic: "a/first", Payload: make([]byte, 256<<10)}
	_, _ = rand.Read(first.Payload) // never fails (crypto/rand)
	second := fromPeer(2, 3, "a/second", "second to arrive")
	support.PeerAnswered(t, l, first, 0x00)
	support.PeerAnswered(t, l, second, 0x00)

	_, c, ballast := replayInterrupted(t, disk, peer, l) // its link left unanswered
	if !c.CleanStart {
		t.Errorf("the CONNECT after the replay's mark found no room has Clean Start 0; want 1 while a replay is owed")
	}
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	stopped(t, b)
	b = support.Launch(t, o)
	l = peer.Link(t)
	if c, _ := harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout); !c.CleanStart {
		t.Errorf("the first CONNECT after a graceful stop, the mark kept in memory and room freed before it, has Clean Start 0; want 1: the mark is kept until it is stored")
	}

	ballast = testkit.FillDisk(t, filepath.Join(disk, "ballast"))
	full, err := ballast.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := ballast.Truncate(full.Size() - room); err != nil {
		t.Fatal(err)
	}
	support.PeerSends(t, l, support.Quiet)
	db := support.OpenDB(t, b.DB)
	type replay struct {
		owed bool
		held []uint16
	}
	awaitState(t, "a replay owed, both held publishes still there", func() replay {
		return replay{replayOwed(t, db, name), heldOf(t, db, name)}
	}, func(r replay) bool { return r.owed && len(r.held) == 2 })
	support.Crash(t, b)
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}

	b = support.Launch(t, o)
	l = peer.Link(t)
	if c, _ := harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout); !c.CleanStart {
		t.Errorf("the first CONNECT after the restart, a replay's mark stored, has Clean Start 0; want 1 while a replay is owed")
	}
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "replaying-in-sub", CleanStart: true}, support.At(2, "from/#"))
	support.PeerSends(t, l, support.Quiet)
	s, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	db = support.OpenDB(t, b.DB)
	if owed, held := replayOwed(t, db, name), heldOf(t, db, name); owed || len(held) != 0 {
		t.Errorf("when the SUBSCRIBE came, the database owed a replay %v and held the bridge's held publishes %v; want the replay committed first", owed, held)
	}
	support.PeerSends(t, l, support.Grants(s))
	reused := fromPeer(2, first.ID, "a/new", "the new link's")
	support.Bridged(t, l, reused)
	for _, p := range []*harness.Publish{first, second, reused} {
		support.DeliveredAt(t, sub, 2, "from/"+p.Topic, p.Payload)
	}
}

// TestBridgePingsWhileAReplayIsOwed: design.md, Bridges: "A CONNACK with
// Session Present 0 starts a replay of the bridge's state for the peer";
// "Before the replay starts, the bridge stores that a replay is owed. The
// replay's last step clears that mark in the same transaction. Until the
// mark is cleared, the bridge neither subscribes at the peer nor sends it a
// record. Every CONNECT it makes until then has Clean Start 1"; "A bridge
// advertises Keep Alive 30 seconds. A Server Keep Alive replaces it. For a
// nonzero effective interval, PINGREQ is due after half that interval
// without a completed outgoing control packet."; Bridges, In: "the bridge
// subscribes at the peer in one SUBSCRIBE carrying every inbound filter";
// storage.md, bridges: replay_owed, "1 from a CONNACK with Session Present
// 0 until its replay commits". MQTT 5 3.2.2.3.14: "If the Server sends a
// Server Keep Alive on the CONNACK packet, the Client MUST use this value
// instead of the Keep Alive value the Client sent on CONNECT
// [MQTT-3.2.2-21]."; 3.12.4: "The Server MUST send a PINGRESP packet in
// response to a PINGREQ packet [MQTT-3.12.4-1]."
//
// A bridge, pinging-in, has an in rule on a/# at QoS 2, its store's log
// read through a rig that makes each sync of it wait 3 seconds once
// stalled (launchSlowed), so the replay's mark, its move and its clearing
// each take one slowed commit and nothing is refused. The peer forwards
// a/first at QoS 2, answered PUBREC 0x00, then drops the link. The rig is
// stalled, and the peer answers the next CONNECT with Session Present 0
// and a Server Keep Alive of 2 seconds, answering every PINGREQ: the
// database comes to owe a replay; the link's next two packets are
// PINGREQs, no SUBSCRIBE or PUBLISH before them; the database still owes
// the replay after them. The stall taken off, the link's next packet
// other than a PINGREQ is the SUBSCRIBE, and when it comes the database
// owes no replay.
func TestBridgePingsWhileAReplayIsOwed(t *testing.T) {
	t.Parallel()
	const name, slowSync = "pinging-in", 3 * time.Second
	peer := harness.ListenedPeer(t)
	s := launchSlowed(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo(name, peer, support.In("a/#", 2))}}, "sync", slowSync)
	l, _, _ := support.LinkUp(t, peer, support.Quiet)
	support.PeerAnswered(t, l, fromPeer(2, 7, "a/first", "held"), 0x00)
	_ = l.Close() // the peer drops the link, its session lost

	l = peer.Link(t)
	harness.Arrive[*harness.Connect](t, l, harness.DefaultTimeout)
	l.AnswerPings(true)
	s.stall(t, true)
	support.PeerSends(t, l, &harness.Connack{Props: harness.Properties{{ID: harness.ServerKeepAlive, Int: 2}}})
	stalled := support.OpenDB(t, filepath.Join(s.stalled, "saguin.db"))
	awaitState(t, "a replay owed", func() bool { return replayOwed(t, stalled, name) }, func(owed bool) bool { return owed })
	for range 2 {
		harness.Arrive[*harness.Pingreq](t, l, harness.DefaultTimeout)
	}
	if !replayOwed(t, stalled, name) {
		t.Fatalf("after the two PINGREQs, the database owes no replay; want it still owed, its commits slowed")
	}

	s.stall(t, false)
	for {
		a, err := l.Next(harness.DefaultTimeout)
		if err != nil {
			t.Fatalf("waiting for the bridge's SUBSCRIBE once the stall is off: %v", err)
		}
		if _, ok := a.Packet.(*harness.Pingreq); ok {
			continue
		}
		if _, ok := a.Packet.(*harness.Subscribe); !ok {
			t.Fatalf("the peer received %s; want PINGREQs, then the SUBSCRIBE once the replay commits", a)
		}
		break
	}
	if replayOwed(t, support.OpenDB(t, s.DB), name) {
		t.Errorf("when the SUBSCRIBE came, the database owed a replay; want the replay committed first")
	}
}

// TestReplacedLinkGetsNoReply: design.md, Flows, Replies: "CONNACK (when it
// changes a session), PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK,
// $saguin answers and a bridge's acknowledgement to its peer are sent only
// once the store confirms, where they confirm a change. Such a reply goes
// only on the connection that asked. If the store confirms after that
// connection has ended or been taken over, what was stored stays and the
// reply is dropped. A sender that kept its session resends what was
// unacknowledged and is answered then (MQTT-4.4.0-1)."; Bridges: "The first
// redial after failed establishment or link loss is immediate."; Store,
// Writes: "With a configured interval, every kind of eligible write joins the
// collecting group. Its first publication fixes the core's collection
// deadline". MQTT 5 4.4: the Server "MUST resend any unacknowledged PUBLISH
// packets (where QoS > 0) [...] using their original Packet Identifiers
// [MQTT-4.4.0-1]".
//
// With a publish_commit_interval of 900 ms, a bridge has an in rule on
// a/# at QoS 1. The peer forwards a record on a/first under packet
// identifier 5 and drops the link at once, the record's write still
// collecting. On the bridge's next link, Session Present 1, the peer
// resends it, DUP 1, then forwards a/second under 6: the link's next
// arrivals are PUBACK 0x00 to 5, once, then PUBACK 0x00 to 6.
func TestReplacedLinkGetsNoReply(t *testing.T) {
	t.Parallel()
	_, peer, l := support.Inbound(t, &harness.Options{PublishCommitInterval: 900 * time.Millisecond}, "replaced", support.In("a/#", 1))
	first := fromPeer(1, 5, "a/first", "first")
	support.PeerSends(t, l, first)
	_ = l.Close() // the peer drops the link before the store confirms

	again, _, _ := support.LinkUp(t, peer, support.QuietResumed)
	resent := *first
	resent.Dup = true
	support.PeerSends(t, again, &resent)
	support.PeerSends(t, again, fromPeer(1, 6, "a/second", "second"))
	support.PeerAck(t, again, harness.PUBACK, 5, 0x00)
	support.PeerAck(t, again, harness.PUBACK, 6, 0x00)
}

// TestBridgedRecordKeepsItsBridge: design.md, Bridges: "A record that arrived
// over a bridge never leaves over any bridge."; Words: Record, "It holds an
// id, time, topic, QoS, MQTT properties, payload, offset and the publisher's
// client id."; storage.md, records: "publisher | the publisher's client id,
// for No Local; [the empty string] for one that arrived over a bridge; never sent"; "bridge |
// the id of the bridge it arrived over, 0 for none: such a record never
// leaves over a bridge"; bridges: "name | unique: the bridge's name".
//
// A bridge named origin has an in rule on a/# at QoS 1. Its peer forwards
// a record on a/origin, PUBACK 0x00, and a local client, local-origin,
// publishes one on local/origin at QoS 1, PUBACK 0x00. The database then
// holds the bridged record with an empty publisher and bridge origin, and the
// local one with publisher local-origin and no bridge. That such a record
// never leaves over a bridge is TestBridgedRecordNeverLeaves's to check.
func TestBridgedRecordKeepsItsBridge(t *testing.T) {
	t.Parallel()
	b, _, l := support.Inbound(t, &harness.Options{}, "origin", support.In("a/#", 1))
	bridged := fromPeer(1, 1, "a/origin", "from the peer")
	support.Bridged(t, l, bridged)
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "local-origin", CleanStart: true})
	local := &harness.Publish{QoS: 1, Topic: "local/origin", Payload: []byte("from a client")}
	support.Acknowledged(t, c, local)

	rs := records(t, b.DB)
	for _, want := range []struct{ topic, payload, publisher, bridge string }{
		{"from/a/origin", string(bridged.Payload), "", "origin"},
		{local.Topic, string(local.Payload), "local-origin", ""},
	} {
		i := slices.IndexFunc(rs, func(r record) bool { return string(r.payload) == want.payload })
		if i < 0 {
			t.Errorf("the database holds no record of %q; want one on %s", want.payload, want.topic)
			continue
		}
		if r := rs[i]; r.topic != want.topic || r.publisher != want.publisher || r.bridge != want.bridge {
			t.Errorf("the record of %q is on %q, publisher %q, bridge %q; want %q, publisher %q, bridge %q",
				want.payload, r.topic, r.publisher, r.bridge, want.topic, want.publisher, want.bridge)
		}
	}
}

// TestBridgedRetain: design.md, Bridges, In: "What arrives goes through the
// ordinary publish path as no user (PUBLISH)."; Bridges, In: the bridge asks
// "with No Local, Retain As Published 1 and Retain Handling 1"; Channels: "A
// retain flag on a configured channel topic is ignored (MQTT-3.3.1-5).";
// $broadcast, Retained: "A retained publish takes the flag."; "RETAIN is 1 on
// retained values sent to a new subscription."; Flows, SUBSCRIBE, Options:
// "RETAIN is 0 on channel deliveries"; "Retain As Published applies on
// `$broadcast` only."; the subscription table: an append channel starts with
// "the floor".
//
// A broker has the append channel kept on from/r/c/#, and its bridge an in
// rule on r/# at QoS 1 (topic from/r/$#). The peer forwards two records
// with RETAIN 1, each PUBACK 0x00: r/b/x, which lands on $broadcast, and
// r/c/x, which lands on kept. A new subscription to from/r/b/# receives
// from/r/b/x with RETAIN 1, its retained value; a new subscription to
// from/r/c/#, asking Retain As Published, receives from/r/c/x, from the
// floor, with RETAIN 0.
func TestBridgedRetain(t *testing.T) {
	t.Parallel()
	b, _, l := support.Inbound(t, &harness.Options{Channels: []harness.Channel{{Name: "kept", Type: "append", Filter: "from/r/c/#"}}},
		"retaining", support.In("r/#", 1))
	broadcast, channel := fromPeer(1, 1, "r/b/x", "broadcast"), fromPeer(1, 2, "r/c/x", "channel")
	for _, p := range []*harness.Publish{broadcast, channel} {
		p.Retain = true
		support.Bridged(t, l, p)
	}

	onBroadcast := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "retaining-broadcast", CleanStart: true}, support.At(1, "from/r/b/#"))
	if p := support.DeliveredAt(t, onBroadcast, 1, "from/r/b/x", broadcast.Payload); !p.Retain {
		t.Error("from/r/b/x came to a new subscription with RETAIN 0; want 1, the retained value of a retained publish on $broadcast")
	}
	onChannel := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "retaining-channel", CleanStart: true},
		[]harness.Subscription{{Filter: "from/r/c/#", QoS: 1, RetainAsPublished: true}})
	if p := support.DeliveredAt(t, onChannel, 1, "from/r/c/x", channel.Payload); p.Retain {
		t.Error("from/r/c/x came from the channel kept with RETAIN 1; want 0: the retain flag on a configured channel topic is ignored")
	}
}

// packetSize is the most a PUBLISH the peer forwards may be in
// TestBridgeMaximumPacketSize: limits.max_packet_size.
const packetSize = 1024

// sizedFromPeer is a QoS 1 PUBLISH the peer forwards on topic under id,
// its payload making the packet n bytes on the wire, fixed header included.
func sizedFromPeer(t *testing.T, id uint16, topic string, n int) *harness.Publish {
	t.Helper()
	p := &harness.Publish{QoS: 1, ID: id, Topic: topic}
	for size := len(harness.Encoded(t, p, harness.V5)); size != n; size = len(harness.Encoded(t, p, harness.V5)) {
		p.Payload = make([]byte, len(p.Payload)+n-size)
	}
	return p
}

// TestBridgeMaximumPacketSize: design.md, Bridges: "A bridge uses MQTT 5,
// Clean Start 0, its configured client id, Session Expiry and Receive
// Maximum. Its CONNECT also carries Maximum Packet Size
// limits.max_packet_size, so the peer skips a larger record instead of
// sending it (MQTT-3.1.2-25)."; "On that connection Sagüin is an ordinary
// MQTT 5 client of the peer."; Flows, Packet buffers: "A packet's size is
// checked before its body is read."; DISCONNECT, Reasons Sagüin sends:
// "0x95 for packet too large". MQTT 5 3.1.2.11.4: "The Server MUST NOT send
// packets exceeding Maximum Packet Size to the Client [MQTT-3.1.2-24]. If a
// Client receives a packet whose size exceeds this limit, this is a
// Protocol Error, the Client uses DISCONNECT with Reason Code 0x95 (Packet
// too large), as described in section 4.13."
//
// With max_packet_size packetSize (1,024 bytes), a bridge's CONNECT carries
// Maximum Packet Size 1,024. Its peer forwards a record of exactly 1,024
// bytes at QoS 1: PUBACK 0x00. It forwards one of 1,025 bytes: the bridge
// sends DISCONNECT 0x95 and ends the link.
func TestBridgeMaximumPacketSize(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	support.Launch(t, &harness.Options{MaxPacketSize: packetSize, Bridges: []harness.Bridge{support.BridgeTo("bounded-in", peer, support.In("a/#", 1))}})
	l, c, _ := support.LinkUp(t, peer, support.Quiet)
	if got, ok := c.Props.Get(harness.MaximumPacketSize); !ok || got.Int != packetSize {
		t.Errorf("the CONNECT's Maximum Packet Size is %d (present %v); want limits.max_packet_size, %d", got.Int, ok, packetSize)
	}
	support.Bridged(t, l, sizedFromPeer(t, 1, "a/at", packetSize))
	support.PeerSends(t, l, sizedFromPeer(t, 2, "a/over", packetSize+1))
	if _, d := support.Ending(t, l, harness.DefaultTimeout); d == nil || d.Reason != 0x95 {
		t.Errorf("the bridge ended the link after %+v; want DISCONNECT 0x95 (Packet too large)", d)
	}
}

// TestBridgeReceiveMaximumExceeded: design.md, Bridges: "A bridge uses MQTT
// 5, Clean Start 0, its configured client id, Session Expiry and Receive
// Maximum."; "On that connection Sagüin is an ordinary MQTT 5 client of the
// peer."; Flows, PUBLISH: "A written PUBACK, PUBCOMP or refusing PUBREC
// frees one. A held QoS 2 identifier takes exactly one until its PUBCOMP";
// "If none is being sent, the connection ends with DISCONNECT 0x93.";
// DISCONNECT, Reasons Sagüin sends: "0x93 for Receive Maximum exceeded";
// config.md, bridges.<name>: receive_maximum, "the Receive Maximum sent to
// the peer". MQTT 5 3.3.4: "If it receives more than Receive Maximum QoS 1
// and QoS 2 PUBLISH packets where it has not sent a PUBACK or PUBCOMP in
// response, the Client uses DISCONNECT with Reason Code 0x93 (Receive
// Maximum exceeded)".
//
// A bridge with receive_maximum 2 sends Receive Maximum 2 on its CONNECT.
// Its peer forwards two records at QoS 2, PUBREC 0x00 to each, both held,
// then one at QoS 1: the bridge sends DISCONNECT 0x93 and ends the link.
func TestBridgeReceiveMaximumExceeded(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	bridge := support.BridgeTo("eager-peer", peer, support.In("a/#", 2))
	bridge.ReceiveMaximum = 2
	support.Launch(t, &harness.Options{Bridges: []harness.Bridge{bridge}})
	l, c, _ := support.LinkUp(t, peer, support.Quiet)
	if got, ok := c.Props.Get(harness.ReceiveMaximum); !ok || got.Int != 2 {
		t.Errorf("the CONNECT's Receive Maximum is %d (present %v); want receive_maximum, 2", got.Int, ok)
	}
	support.PeerAnswered(t, l, fromPeer(2, 1, "a/1", "held"), 0x00)
	support.PeerAnswered(t, l, fromPeer(2, 2, "a/2", "held"), 0x00)
	support.PeerSends(t, l, fromPeer(1, 3, "a/3", "over"))
	if _, d := support.Ending(t, l, harness.DefaultTimeout); d == nil || d.Reason != 0x93 {
		t.Errorf("the bridge ended the link after %+v; want DISCONNECT 0x93 (Receive Maximum exceeded)", d)
	}
}

// TestBridgedSeries: metrics.md, Bridges: saguin_bridge_received_total{bridge},
// counter, "records from the peer that were stored"; Publishing:
// saguin_publishes_received_total{qos}, counter, "completely decoded PUBLISH
// packets from clients and inbound bridges, policy refusals included;
// packets rejected before complete decoding are excluded";
// saguin_publish_refused_total{cause}, counter, "refused publishes, QoS 0
// drops included (design.md, PUBLISH): not_authorized, topic_invalid,
// store_full, channel_full, retained_full, headers, reserved_property,
// no_response_topic"; Rules: "Every series exists from start, for every value
// of its labels that applies"; "Counters start at 0."; design.md, Bridges,
// In: "A record matching no inbound rule is refused as an invalid inbound
// result is, and logged."
//
// A bridge named metered-in has an in rule on a/# at QoS 2. With its link
// up and nothing forwarded, /metrics has each series, a counter, at 0. The
// peer forwards records on a/0 at QoS 0, a/1 at QoS 1 and a/2 at QoS 2,
// each stored, and on z/1 at QoS 1 and z/0 at QoS 0, which no rule
// matches, PUBACK 0x90 to the first. /metrics comes to read 3 records
// received over the bridge; 2 PUBLISHes received at QoS 0, 2 at QoS 1 and
// 1 at QoS 2; and 2 refused as topic_invalid.
func TestBridgedSeries(t *testing.T) {
	t.Parallel()
	const name = "metered-in"
	b, _, l := support.Inbound(t, &harness.Options{Operations: true}, name, support.In("a/#", 2))
	received := testkit.SeriesKey("saguin_bridge_received_total", "bridge", name)
	publishes := func(qos string) string { return testkit.SeriesKey("saguin_publishes_received_total", "qos", qos) }
	invalid := testkit.SeriesKey("saguin_publish_refused_total", "cause", "topic_invalid")
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	for _, series := range []string{"saguin_bridge_received_total", "saguin_publishes_received_total", "saguin_publish_refused_total"} {
		if e.Types[series] != "counter" {
			t.Errorf("%s is of type %q; want counter", series, e.Types[series])
		}
	}
	support.AwaitSampled(t, b, "from start, nothing forwarded", map[string]float64{
		received: 0, publishes("0"): 0, publishes("1"): 0, publishes("2"): 0, invalid: 0,
	})

	for _, p := range []*harness.Publish{fromPeer(0, 0, "a/0", "0"), fromPeer(1, 1, "a/1", "1"), fromPeer(2, 2, "a/2", "2")} {
		support.Bridged(t, l, p)
	}
	support.PeerAnswered(t, l, fromPeer(1, 3, "z/1", "unmatched"), 0x90)
	support.PeerSends(t, l, fromPeer(0, 0, "z/0", "unmatched"))
	support.AwaitSampled(t, b, "three stored, two matching no rule", map[string]float64{
		received: 3, publishes("0"): 2, publishes("1"): 2, publishes("2"): 1, invalid: 2,
	})
}

// TestIncomingMemoryBelowTheLargestBodyIsRefused: config.md,
// broker.limits, max_incoming_memory: "the incoming budget. It must hold
// the largest body one reader can ask for: max_packet_size, plus the
// largest S2 copy of it where max_packet_size is over 4 KiB, plus
// max_topic_length where a bridge has an in rule. A smaller value is
// refused, and the refusal names the value to set"; its default is 32MiB;
// max_packet_size: "at most 268,435,460, the largest MQTT packet".
// design.md, Restart, Start: "startup checks the configuration, including
// [...] size limits"; "The exit status is [...] 1 after [...] a refused
// start"; Operations: saguin --check-config "checks a configuration and
// exits. It also [...] checks it as startup does". The largest S2 copy is
// leastIncoming's; the value to set is taken as the least the key allows,
// in bytes.
//
// Each case is a configuration, with max_incoming_memory either at the
// least the case allows, whose --check-config exits 0, or below it, whose
// --check-config and start each exit 1, a line of what they printed naming
// broker.limits.max_incoming_memory and that least:
//   - max_packet_size 4 KiB, not over 4 KiB, so no S2 copy: the least is
//     4,096; 4,095 is refused;
//   - max_packet_size 64 KiB: the least is that and its largest S2 copy;
//     a byte below is refused;
//   - max_packet_size 64 KiB, max_topic_length 4,096 and a bridge with an
//     in rule: the least is the last case's and 4,096 more; a byte below is
//     refused;
//   - the same with the bridge's one rule out: a byte below the last case's
//     least is accepted, at least the second case's;
//   - max_packet_size 268,435,460 and max_incoming_memory left at its
//     default: refused, naming the least for that max_packet_size.
func TestIncomingMemoryBelowTheLargestBodyIsRefused(t *testing.T) {
	t.Parallel()
	const largest, topicRoom = 268435460, 4096
	packet := 64 << 10
	inRule, outRule := support.In("a/#", 1), support.Out("a/#", "to/$#", 1)
	for _, c := range []struct {
		name          string
		packet, topic int
		rule          *harness.BridgeRule
		memory, least int // memory 0 leaves max_incoming_memory at its default
	}{
		{name: "4 KiB at its least", packet: 4 << 10, memory: 4 << 10},
		{name: "4 KiB a byte below", packet: 4 << 10, memory: 4<<10 - 1, least: 4 << 10},
		{name: "64 KiB at its least", packet: packet, memory: leastIncoming(packet)},
		{name: "64 KiB a byte below", packet: packet, memory: leastIncoming(packet) - 1, least: leastIncoming(packet)},
		{name: "an in rule at its least", packet: packet, topic: topicRoom, rule: &inRule,
			memory: leastIncoming(packet) + topicRoom},
		{name: "an in rule a byte below", packet: packet, topic: topicRoom, rule: &inRule,
			memory: leastIncoming(packet) + topicRoom - 1, least: leastIncoming(packet) + topicRoom},
		{name: "an out rule alone", packet: packet, topic: topicRoom, rule: &outRule,
			memory: leastIncoming(packet) + topicRoom - 1},
		{name: "the largest packet under the default", packet: largest, least: leastIncoming(largest)},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			o := &harness.Options{Dir: t.TempDir(), MaxPacketSize: c.packet, MaxTopicLength: c.topic, MaxIncomingMemory: c.memory}
			if c.rule != nil {
				o.Bridges = []harness.Bridge{support.BridgeTo("room", harness.ListenedPeer(t), *c.rule)}
			}
			config, err := harness.WriteConfig(o)
			if err != nil {
				t.Fatalf("writing the configuration: %v", err)
			}
			if c.least == 0 {
				if code, stdout, stderr := execute(t, support.BrokerBinary, "--check-config", config); code != 0 {
					t.Errorf("--check-config: exit %d; want 0\n%s%s", code, stdout, stderr)
				}
				return
			}
			for _, flag := range []string{"--check-config", "--config"} {
				code, stdout, stderr := execute(t, support.BrokerBinary, flag, config)
				if code != 1 {
					t.Errorf("%s: exit %d; want the configuration refused, exit 1", flag, code)
				}
				if !namesAll(stdout+stderr, "broker.limits.max_incoming_memory", strconv.Itoa(c.least)) {
					t.Errorf("%s: no line of what it printed names both broker.limits.max_incoming_memory and the value to set, %d:\n%s%s", flag, c.least, stdout, stderr)
				}
			}
		})
	}
}

// bodyLength is the Remaining Length of the packet wire, the bytes after its
// fixed header (MQTT 5 2.1.4: "the number of bytes remaining within the
// current Control Packet, including data in the Variable Header and the
// Payload").
func bodyLength(wire []byte) int {
	i := 1
	for wire[i]&0x80 != 0 {
		i++
	}
	return len(wire) - i - 1
}

// lockedFor bounds a wait made with the database's write lock held, well
// within SQLite's own wait for it (holdWriteLock), or with a body half
// sent, within the write_timeout of twice it that the test sets.
const lockedFor = 2 * time.Second

// TestBridgedTopicNameReservation: design.md, Flows, Message memory, Asking:
// "Before allocating or reading a nonempty PUBLISH body, its reader requests
// the body's Remaining Length from the core."; "A body arriving over a
// bridge whose rule's template builds a new topic name also asks for
// limits.max_topic_length for that name. Once the name is built, the body
// keeps only the room it takes, and the rest goes back. Only the topic name
// is built anew; the rest of the body is used as it arrived."; Bytes back:
// "An incoming reservation follows its body through reading, handover and
// storage, and ends when the body is stored, refused or abandoned";
// Message memory: "the incoming budget, limits.max_incoming_memory, holds
// PUBLISH bodies from the start of their reading until they are stored or
// refused"; metrics.md, Store and memory:
// saguin_memory_in_transit_bytes{direction}, "outstanding reservations of
// each budget, incoming or outgoing (design.md, Message memory)", here
// incoming. Its bodies are not over 4 KiB, so none asks for an S2 copy.
//
// With max_topic_length 200, a bridge has an in rule on a/+ at QoS 1 whose
// template, built/$1/$1/$1/$1, builds a longer topic name, and a both rule
// on b/#, which keeps the topic. The peer writes the fixed header of a QoS 1
// PUBLISH on a/sensor-1 and half its body, the topic not yet whole: the
// reservation comes to its Remaining Length and max_topic_length more. With the database's write
// lock held, the peer writes the rest of it, then a QoS 1 PUBLISH on
// a/sensor-2 and one on b/kept: the reservation comes to each body's
// Remaining Length, with the length of the name built for each of the first
// two, built/sensor-1/sensor-1/sensor-1/sensor-1 and its sensor-2 twin, and
// nothing more for b/kept. The lock released, the three get PUBACK 0x00 in
// order, and the reservation goes back to what it read before.
func TestBridgedTopicNameReservation(t *testing.T) {
	t.Parallel()
	const topicRoom = 200
	b, _, l := support.Inbound(t, &harness.Options{Operations: true, MaxTopicLength: topicRoom, WriteTimeout: 2 * lockedFor}, "room",
		harness.BridgeRule{Filter: "a/+", Topic: "built/$1/$1/$1/$1", Direction: "in", QoS: 1},
		harness.BridgeRule{Filter: "b/#", Direction: "both", QoS: 1})
	before := support.Sampled(t, b, inTransitIncoming)[inTransitIncoming]
	built := func(level string) string { return "built/" + strings.Repeat(level+"/", 3) + level }
	records := []struct {
		p     *harness.Publish
		built string // the topic name the rule's template builds, "" where it keeps the topic
	}{
		{fromPeer(1, 1, "a/sensor-1", "first"), built("sensor-1")},
		{fromPeer(1, 2, "a/sensor-2", "second"), built("sensor-2")},
		{fromPeer(1, 3, "b/kept", "third"), ""},
	}

	// reads waits until the reservation reads want, within lockedFor: with
	// the write lock held, or a body half sent, whose silence ends its
	// connection after write_timeout.
	reads := func(want float64, while string) {
		t.Helper()
		var got float64
		if !testkit.Within(lockedFor, func() bool {
			got = support.Sampled(t, b, inTransitIncoming)[inTransitIncoming]
			return got == want
		}) {
			t.Fatalf("%s, %s reads %v after %v; want %v", while, inTransitIncoming, got, lockedFor, want)
		}
	}

	first := harness.Encoded(t, records[0].p, harness.V5)
	// The fixed header and half the body: the topic not yet whole.
	half := len(first) - bodyLength(first)/2
	support.PeerSendsBytes(t, l, first[:half])
	reads(before+float64(bodyLength(first)+topicRoom), "with the first body half read, its name not yet built")

	lock := holdWriteLock(t, b.DB)
	support.PeerSendsBytes(t, l, first[half:])
	want := before
	for i, r := range records {
		if i > 0 {
			support.PeerSends(t, l, r.p)
		}
		want += float64(bodyLength(harness.Encoded(t, r.p, harness.V5)) + len(r.built))
	}
	reads(want, "with the three held behind the write lock, each body and the name built for it")
	lock.release(t)
	for _, r := range records {
		support.PeerAck(t, l, harness.PUBACK, r.p.ID, 0x00)
	}
	reads(before, "with the three stored")
}
