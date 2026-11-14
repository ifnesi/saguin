package conformance

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the position a durable session keeps on an append channel
// or a queue once the last subscription reaching it is removed (design.md,
// Flows, SUBSCRIBE, UNSUBSCRIBE; Words: Consumer; storage.md, positions
// and session_inflight), from the wire and the database.

// keepingChannel is the channel the tests' sessions leave and come back to,
// of type typ: "append", or "queue", whose own topics a subscription reads
// as a reader (design.md, Channels: "A subscription to the channel's own
// topics makes a reader, as on append").
func keepingChannel(typ string) harness.Channel {
	return harness.Channel{Name: "kept", Type: typ, Filter: "kept/#"}
}

// keptRig launches a broker holding keepingChannel of typ in dir, a folder
// of the test's own where dir is "", and connects a publisher of its own,
// which it returns with the broker.
func keptRig(t *testing.T, typ, dir string) (*harness.Broker, *harness.Client) {
	t.Helper()
	b := support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{keepingChannel(typ)}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "kept-pub", CleanStart: true})
	return b, pub
}

// unsubscribed sends c's UNSUBSCRIBE of filters and waits for its UNSUBACK,
// 0x00 for each in MQTT 5, none in MQTT 3.1.1 (unsuback).
func unsubscribed(t *testing.T, c *harness.Client, filters ...string) {
	t.Helper()
	var want []byte
	if c.Version == harness.V5 {
		want = make([]byte, len(filters)) // 0x00, Success, for each
	}
	unsuback(t, c, nil, filters, want...)
}

// nothingResent resumes connect's durable session on b over MQTT 5,
// subscribes it to fence/#, on $broadcast, and fails the test unless the
// first PUBLISH it receives is a record pub stores there next, DUP 0:
// design.md, CONNECT, 9. Resume, "The broker reads in-flight messages and
// PUBRELs from the store and resends them"; "New application deliveries
// wait behind recovered PUBLISHes", so nothing the store still held for
// the session came before it. It returns the resumed client.
func nothingResent(t *testing.T, b *harness.Broker, pub *harness.Client, connect *harness.Connect) *harness.Client {
	t.Helper()
	c := resumed(t, b.Addr, harness.V5, connect)
	support.Suback(t, c, nil, support.At(1, "fence/#"), 0x01)
	fence := &harness.Publish{QoS: 1, Topic: "fence/x", Payload: []byte("fence")}
	support.Acknowledged(t, pub, fence)
	support.DeliveredAt(t, c, 1, fence.Topic, fence.Payload)
	return c
}

// stillAt fails the test unless client's stored position on kept is at,
// and its session_inflight holds no row: every exchange it had ended,
// none moving the kept position. when says when.
func stillAt(t *testing.T, b *harness.Broker, client string, at int64, when string) {
	t.Helper()
	db := support.OpenDB(t, b.DB)
	if got, _ := storedPosition(t, db, client, "kept"); got != at {
		t.Errorf("%s, the kept position is %d; want %d, where the UNSUBSCRIBE kept it (design.md, UNSUBSCRIBE: \"Their completion never moves the kept position\")", when, got, at)
	}
	if rows := storedInflight(t, db, client); len(rows) != 0 {
		t.Errorf("%s, session_inflight holds %+v; want none, every exchange ended", when, rows)
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "When the last subscription
// reaching an append channel or a queue is removed, a durable session keeps
// its position there"; "The next subscription reaching the channel reads
// from the kept position"; Restart: "the broker restores durable sessions
// with their subscriptions, positions"; Channels, queue: "A subscription to
// the channel's own topics makes a reader, as on append"; the subscription
// table, "append, or a queue's own topics (a reader) | the floor".
// storage.md, positions: "A place on an append channel or a queue stays
// until the session ends, also while no subscription reaches the channel".
// MQTT 5 [MQTT-3.1.2-5] and MQTT 3.1.1 [MQTT-3.1.2-4]: with Clean Start 0
// (Clean Session 0) the Server resumes from the existing Session.
//
// On an append channel and on a queue, over MQTT 5 and MQTT 3.1.1, a
// client holding a durable session subscribes at QoS 1 to kept/#, receives
// r1, stored before, and acknowledges it, then unsubscribes, the last
// subscription reaching the channel; a publisher stores r2. On the same
// connection; after a DISCONNECT and a resume; or after a DISCONNECT, a
// graceful stop and a start on the same files and a resume: the client
// subscribes to kept/# again, and its first delivery is r2, DUP 0, from the
// kept position: neither r1 again, from the floor, nor nothing, from the
// tail.
func TestKeptPositionContinuesAtTheNextSubscription(t *testing.T) {
	for _, typ := range []string{"append", "queue"} {
		for _, version := range bothVersions {
			for _, how := range []string{"same connection", "reconnected", "restarted"} {
				t.Run(fmt.Sprintf("%s/%s/%s", typ, harness.VersionName(version), how), func(t *testing.T) {
					dir := t.TempDir()
					b, pub := keptRig(t, typ, dir)
					r1 := &harness.Publish{QoS: 1, Topic: "kept/x", Payload: []byte("r1")}
					r2 := &harness.Publish{QoS: 1, Topic: "kept/x", Payload: []byte("r2")}
					support.Acknowledged(t, pub, r1)
					connect := kept(version, "keeper")
					c := support.Subscribed(t, b.Addr, version, connect, support.At(1, "kept/#"))
					acknowledge(t, c, support.DeliveredAt(t, c, 1, r1.Topic, r1.Payload).ID, 0)
					unsubscribed(t, c, "kept/#")
					support.Acknowledged(t, pub, r2)

					switch how {
					case "reconnected":
						support.Away(t, c)
						c = resumed(t, b.Addr, version, connect)
					case "restarted":
						support.Away(t, c)
						stopped(t, b)
						b = support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{keepingChannel(typ)}})
						c = resumed(t, b.Addr, version, connect)
					}
					support.Suback(t, c, nil, support.At(1, "kept/#"), 0x01)
					support.DeliveredAt(t, c, 1, r2.Topic, r2.Payload)
				})
			}
		}
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "If a delivery was prepared but
// never handed to the writer, the core first sets the position back to the
// earliest such delivery's offset, so no record is skipped"; "The kept
// position is saved in the UNSUBSCRIBE's own transaction, with the removal
// of the stored preparations"; "The next subscription reaching the channel
// reads from the kept position, so a record at or beyond it may be
// delivered again". Delivery, Preparation: "A connection's prepared and
// sent deliveries together are at most twice its window"; `$saguin`
// topics, Every request: "One descriptor within the connection's
// twice-window bound is reserved for direct answers", so a window of 3
// holds three sent deliveries and two prepared; Positions: "The position
// advances only past the contiguous settled or skipped prefix of examined
// records"; Order: deliveries follow "a channel's offset order on [...]
// append". MQTT 5 [MQTT-3.10.4-2]: the Server "MUST stop adding any new
// messages which match the Topic Filters, for delivery to the Client".
//
// A client holding a durable session, with Receive Maximum 3, subscribes
// at QoS 1 to kept/#, an append channel, and receives and acknowledges e1
// and e2. A publisher stores five records: the client receives the first
// three and holds them, the last two prepared behind them, the database
// holding all five rows. It unsubscribes: once the UNSUBACK arrives, the
// database holds the first three rows alone and a kept position at or
// after the first's offset, the settled e1 and e2 behind it, and at or
// before the fourth's, the earliest preparation. The client acknowledges
// the three and subscribes again; a publisher stores a fence. The client
// receives every record from the kept position on, in offset order, DUP 0,
// through the fence: the fourth and fifth among them, none skipped.
func TestKeptPositionSkipsNothingPrepared(t *testing.T) {
	b, pub := keptRig(t, "append", "")
	db := support.OpenDB(t, b.DB)
	const client = "preparer"
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true,
		Props: slices.Concat(support.Durable, receiveMaximum5(3))}, support.At(1, "kept/#"))
	early := []*harness.Publish{{QoS: 1, Topic: "kept/x", Payload: []byte("e1")}, {QoS: 1, Topic: "kept/x", Payload: []byte("e2")}}
	support.PublishAll(t, pub, early)
	for _, p := range early {
		acknowledge(t, c, support.DeliveredAt(t, c, 1, p.Topic, p.Payload).ID, 0)
	}
	ps := numbered("kept/x", 5)
	support.PublishAll(t, pub, ps)
	sent := make([]*harness.Publish, 3)
	for i := range sent {
		sent[i] = support.DeliveredAt(t, c, 1, ps[i].Topic, ps[i].Payload)
	}
	offsets := offsetsByPayload(t, b.DB)
	all := []int64{offsets["1"], offsets["2"], offsets["3"], offsets["4"], offsets["5"]}
	awaitState(t, fmt.Sprintf("session_inflight rows for the records at offsets %v, the last two prepared", all),
		func() []int64 { return rowOffsets(storedInflight(t, db, client)) }, func(got []int64) bool { return slices.Equal(got, all) })

	unsubscribed(t, c, "kept/#")
	if got := rowOffsets(storedInflight(t, db, client)); !slices.Equal(got, all[:3]) {
		t.Errorf("once the UNSUBACK arrived, session_inflight holds the records at offsets %v; want %v, the preparations removed with it", got, all[:3])
	}
	at, _ := storedPosition(t, db, client, "kept")
	if at < offsets["1"] || at > offsets["4"] {
		t.Fatalf("once the UNSUBACK arrived, the kept position is %d; want it from %d, past the settled e1 and e2, to %d, the earliest preparation never handed to the writer",
			at, offsets["1"], offsets["4"])
	}
	for _, d := range sent {
		acknowledge(t, c, d.ID, 0)
	}
	support.Suback(t, c, nil, support.At(1, "kept/#"), 0x01)
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "kept/x", Payload: []byte("fence")})
	for _, r := range channelRecords(t, b.DB, "kept") {
		if r.offset >= at {
			acknowledge(t, c, support.DeliveredAt(t, c, 1, r.topic, r.payload).ID, 0)
		}
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "Records the removed filters
// examined and skipped stay passed"; "The next subscription reaching the
// channel reads from the kept position"; Options: "No Local is honoured
// everywhere, against the publisher's client id stored on the record";
// Delivery, Positions: "The position advances only past the contiguous
// settled or skipped prefix of examined records"; Order: deliveries follow
// "a channel's offset order on [...] append". MQTT 5 3.8.3.1: with No Local
// 1, "Application Messages MUST NOT be forwarded to a connection with a
// ClientID equal to the ClientID of the publishing connection
// [MQTT-3.8.3-3]".
//
// A client holding a durable session subscribes at QoS 1 to kept/a, an
// append channel's, with No Local. A publisher stores b1 on kept/b, which
// the filter skips; the client stores own on kept/a, which No Local skips;
// the publisher stores a1 on kept/a, which the client receives first and
// acknowledges: its scan examined b1 and own before it. The client
// unsubscribes, then subscribes to kept/#, a wider filter, without No
// Local; the publisher stores a fence on kept/b. The client's next
// delivery is the fence: neither b1 nor own, passed under the removed
// filter, comes.
func TestKeptPositionKeepsSkippedRecordsPassed(t *testing.T) {
	b, pub := keptRig(t, "append", "")
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "examiner", CleanStart: true, Props: support.Durable},
		[]harness.Subscription{{Filter: "kept/a", QoS: 1, NoLocal: true}})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "kept/b", Payload: []byte("b1")})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "kept/a", Payload: []byte("own")})
	a1 := &harness.Publish{QoS: 1, Topic: "kept/a", Payload: []byte("a1")}
	support.Acknowledged(t, pub, a1)
	acknowledge(t, c, support.DeliveredAt(t, c, 1, a1.Topic, a1.Payload).ID, 0)
	unsubscribed(t, c, "kept/a")
	support.Suback(t, c, nil, support.At(1, "kept/#"), 0x01)
	fence := &harness.Publish{QoS: 1, Topic: "kept/b", Payload: []byte("fence")}
	support.Acknowledged(t, pub, fence)
	support.DeliveredAt(t, c, 1, fence.Topic, fence.Payload)
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "Exchanges already sent finish
// as before. A PUBACK, a PUBCOMP or a refusing PUBREC ends one"; "Their
// completion never moves the kept position, even after another
// subscription reaches the channel"; "The kept position is saved in the
// UNSUBSCRIBE's own transaction"; Delivery, Positions: "That removal marks
// the session's own stored exchanges on the channel moves = 0 [...] A later
// subscription does not set them back"; CONNECT, 9. Resume. storage.md,
// session_inflight, moves: "0 after a seek, saved progress or the removal
// of the last subscription reaching the channel supersedes it. A later
// subscription does not set it back to 1". MQTT 5 [MQTT-3.10.4-3]: the
// Server "MUST complete the delivery of any QoS 1 or QoS 2 messages which
// match the Topic Filters and it has started to send to the Client";
// [MQTT-4.4.0-1]: a reconnecting session resends "any unacknowledged
// PUBLISH packets".
//
// A client holding a durable session subscribes at QoS 1 to kept/#, an
// append channel, receives r1 and holds it, then unsubscribes, the last
// subscription reaching the channel: the UNSUBACK finds a kept position
// saved. Then it acknowledges r1, in the second case after subscribing to
// kept/# again, where r1 may come again, at or beyond the kept position,
// as a new delivery, which it does not acknowledge. Once its own
// publication is acknowledged, the database holds no row of r1's first
// exchange, and the kept position is where the UNSUBSCRIBE saved it. The
// client sends DISCONNECT and resumes: of r1's first exchange nothing is
// resent, its first PUBLISH under that packet identifier, DUP 1, never
// coming before a fence stored after the resume.
func TestPubackAfterTheLastUnsubscribeLeavesTheKeptPosition(t *testing.T) {
	for _, resubscribe := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsubscribed", true: "subscribed again"}[resubscribe], func(t *testing.T) {
			b, pub := keptRig(t, "append", "")
			db := support.OpenDB(t, b.DB)
			const client = "late-acker"
			connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
			c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "kept/#"))
			r1 := &harness.Publish{QoS: 1, Topic: "kept/x", Payload: []byte("r1")}
			support.Acknowledged(t, pub, r1)
			first := support.DeliveredAt(t, c, 1, r1.Topic, r1.Payload)
			unsubscribed(t, c, "kept/#")
			at, _ := storedPosition(t, db, client, "kept")

			if resubscribe {
				support.Suback(t, c, nil, support.At(1, "kept/#"), 0x01)
			}
			acknowledge(t, c, first.ID, 0)
			committed(t, c, "elsewhere/barrier")
			if rows := storedInflight(t, db, client); slices.ContainsFunc(rows, func(r inflightRow) bool { return r.packetID == int64(first.ID) }) {
				t.Errorf("after the PUBACK of r1's first exchange, session_inflight holds %+v; want no row of packet identifier %d", rows, first.ID)
			}
			if got, _ := storedPosition(t, db, client, "kept"); got != at {
				t.Errorf("after the PUBACK of r1's first exchange, the position is %d; want %d, where the UNSUBSCRIBE kept it", got, at)
			}
			// Subscribed again, r1 may come once more, a new delivery from the
			// kept position, which the client leaves unacknowledged.
			again := 0
			support.AwayAllowing(t, c, func(p *harness.Publish) bool {
				again++
				return resubscribe && again == 1 && !p.Dup && p.Topic == r1.Topic && bytes.Equal(p.Payload, r1.Payload)
			})

			c = resumed(t, b.Addr, harness.V5, connect)
			support.Suback(t, c, nil, support.At(1, "fence/#"), 0x01)
			fence := &harness.Publish{QoS: 1, Topic: "fence/x", Payload: []byte("fence")}
			support.Acknowledged(t, pub, fence)
			for {
				p := harness.Expect[*harness.Publish](t, c)
				if p.Topic == fence.Topic {
					break
				}
				if p.Dup && p.ID == first.ID {
					t.Fatalf("after the resume, PUBLISH %q DUP 1 under packet identifier %d: r1's first exchange, which its PUBACK ended, was resent", p.Payload, p.ID)
				}
			}
		})
	}
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "Exchanges already sent finish
// as before. A PUBACK [...] ends one"; "Their completion never moves the
// kept position"; Delivery, Positions: "That removal marks the session's
// own stored exchanges on the channel moves = 0"; "During reconnect
// recovery, an acknowledgement that settles a delivery frees its resend
// capacity at once"; CONNECT, 9. Resume: "Outstanding PUBLISHes are offered
// in stored sending order, only while the new connection has quota.
// Unsent recovered entries remain in the store and consume no
// new-connection send quota". storage.md, session_inflight, moves: "0
// after [...] the removal of the last subscription reaching the channel
// supersedes it". MQTT 5 [MQTT-4.4.0-1]; [MQTT-3.10.4-3]; 4.9: the send
// quota "is incremented by 1 [...] Each time a PUBACK or PUBCOMP packet is
// received".
//
// A client holding a durable session subscribes at QoS 1 to kept/a, an
// append channel's; a publisher stores 1, 2 and 3 on kept/a with a record
// on kept/b between each. The client receives the three, holds them, and
// sends DISCONNECT. It resumes with Receive Maximum 1: 1 comes again, DUP
// 1, and the two others wait in the store, not yet offered. The client
// unsubscribes from kept/a: once the UNSUBACK arrives, the database holds
// a kept position and the three exchanges' rows, each moves 0. The client
// acknowledges each resend in turn, and receives the next, DUP 1, under its
// identifier. Once its own publication is acknowledged, the database holds
// no row, and the kept position is where the UNSUBSCRIBE saved it. The
// client sends DISCONNECT and resumes: nothing is resent.
func TestUnsubscribeDuringRecoveryLeavesTheKeptPosition(t *testing.T) {
	b, pub := keptRig(t, "append", "")
	db := support.OpenDB(t, b.DB)
	const client = "recoverer"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
	c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "kept/a"))
	owed := make([]*harness.Publish, 0, 3)
	for i := range 3 {
		p := &harness.Publish{QoS: 1, Topic: "kept/a", Payload: fmt.Appendf(nil, "%d", i+1)}
		owed = append(owed, p)
		support.PublishAll(t, pub, []*harness.Publish{p, {QoS: 1, Topic: "kept/b", Payload: fmt.Appendf(nil, "gap %d", i+1)}})
	}
	sent := make([]*harness.Publish, len(owed))
	for i, p := range owed {
		sent[i] = support.DeliveredAt(t, c, 1, p.Topic, p.Payload)
	}
	support.Away(t, c)

	connect.Props = slices.Concat(support.Durable, receiveMaximum5(1))
	c = resumed(t, b.Addr, harness.V5, connect)
	resend := redelivered(t, c, sent[0])
	unsubscribed(t, c, "kept/a")
	at, _ := storedPosition(t, db, client, "kept")
	rows := storedInflight(t, db, client)
	if len(rows) != len(sent) || slices.ContainsFunc(rows, func(r inflightRow) bool { return r.moves != 0 }) {
		t.Errorf("once the UNSUBACK arrived, session_inflight holds %+v; want the %d exchanges' rows, each moves 0", rows, len(sent))
	}
	for _, d := range sent[1:] {
		acknowledge(t, c, resend.ID, 0)
		resend = redelivered(t, c, d)
	}
	acknowledge(t, c, resend.ID, 0)
	committed(t, c, "elsewhere/barrier")
	stillAt(t, b, client, at, "once every resend is acknowledged")
	support.Away(t, c)
	nothingResent(t, b, pub, connect)
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "A PUBACK, a PUBCOMP or a
// refusing PUBREC ends one. A successful PUBREC keeps it at phase 2 until
// its PUBCOMP. Their completion never moves the kept position"; Delivery,
// Positions: "an acknowledgement, PUBACK, PUBCOMP or a refusing PUBREC,
// settles its own delivery"; "A QoS 2 delivery stores its PUBREC before its
// PUBREL goes to the writer"; CONNECT, 9. Resume: "The broker reads
// in-flight messages and PUBRELs from the store and resends them".
// storage.md, session_inflight, phase: "2 PUBREC received and PUBREL
// owed". MQTT 5 4.3.3: the sender "MUST send a PUBREL packet when it
// receives a PUBREC packet from the receiver with a Reason Code value less
// than 0x80 [MQTT-4.3.3-4]", "MUST treat the PUBREL packet as
// "unacknowledged" until it has received the corresponding PUBCOMP packet
// [MQTT-4.3.3-5]" and "MUST NOT re-send the PUBLISH once it has sent the
// corresponding PUBREL packet [MQTT-4.3.3-6]"; 4.3.3, a PUBREC Reason
// Code of 0x80 or greater ends the exchange.
//
// A client holding a durable session subscribes at QoS 2 to kept/#, an
// append channel, and a publisher stores q1 and q2 at QoS 2. The client
// receives both and holds them, then unsubscribes: the UNSUBACK finds a
// kept position saved. Its PUBREC 0x00 to q1 gets PUBREL 0x00; its PUBREC
// 0x80 to q2 ends that exchange, no PUBREL coming. Once its own
// publication is acknowledged, the database holds q1's row alone, phase 2,
// and the kept position where the UNSUBSCRIBE saved it. The client sends
// DISCONNECT; the broker stops and starts again on the same files; the
// client resumes: its first packet is q1's PUBREL again, and once its
// PUBCOMP is taken, the database holds no row, the kept position still
// where it was.
func TestQoS2AcrossTheLastUnsubscribeLeavesTheKeptPosition(t *testing.T) {
	dir := t.TempDir()
	b, pub := keptRig(t, "append", dir)
	db := support.OpenDB(t, b.DB)
	const client = "twice-kept"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
	c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "kept/#"))
	q1, q2 := twice("kept/x", "q1"), twice("kept/x", "q2")
	published(t, pub, q1)
	published(t, pub, q2)
	d1 := support.DeliveredAt(t, c, 2, q1.Topic, q1.Payload)
	d2 := support.DeliveredAt(t, c, 2, q2.Topic, q2.Payload)
	unsubscribed(t, c, "kept/#")
	at, _ := storedPosition(t, db, client, "kept")

	released(t, c, d1.ID)
	pubrecFor(t, c, d2.ID, 0x80)
	committed(t, c, "elsewhere/barrier")
	if rows := storedInflight(t, db, client); len(rows) != 1 || rows[0].packetID != int64(d1.ID) || rows[0].phase != 2 {
		t.Errorf("after the PUBRECs, session_inflight holds %+v; want q1's row alone, packet identifier %d, phase 2: the refusing PUBREC ended q2's", rows, d1.ID)
	}
	if got, _ := storedPosition(t, db, client, "kept"); got != at {
		t.Errorf("after the PUBRECs, the kept position is %d; want %d, where the UNSUBSCRIBE kept it", got, at)
	}
	support.Away(t, c)
	stopped(t, b)

	b = support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{keepingChannel("append")}})
	c = resumed(t, b.Addr, harness.V5, connect)
	expectPubrel(t, c, d1.ID, 0x00, "the first packet after the restart")
	pubcomp(t, c, d1.ID)
	committed(t, c, "elsewhere/barrier")
	stillAt(t, b, client, at, "once q1's PUBCOMP is taken")
}

// design.md, Flows, SUBSCRIBE, UNSUBSCRIBE: "The session's end removes
// it"; "When the last subscription reaching an append channel or a queue is
// removed, a durable session keeps its position there"; DISCONNECT: "When
// the expiry runs out, the delayed Will is published and the session
// deleted in one transaction"; "The client may change its session expiry,
// except from 0"; "A session that ends with its connection is ended in the
// store where it has rows"; CONNECT, 6. Decide: "Clean start ends an
// existing session"; the subscription table, "append [...] | the floor".
// storage.md, positions: "A place on an append channel or a queue stays
// until the session ends". MQTT 5 3.1.2.11.2: "If the Session Expiry
// Interval is absent the value 0 is used. If it is set to 0, or is absent,
// the Session ends when the Network Connection is closed"; [MQTT-3.1.2-4]:
// with Clean Start 1 the Server "MUST discard any existing Session".
//
// A client holding a durable session subscribes at QoS 1 to kept/#, an
// append channel, receives r1 and r2 and acknowledges both, and
// unsubscribes: the UNSUBACK finds a kept position saved. Its session then
// ends: its expiry of a second runs out after its DISCONNECT; or, after
// its DISCONNECT, a CONNECT with Clean Start 1 ends it; or its DISCONNECT
// sets Session Expiry 0. The client connects with a session of its own and subscribes to
// kept/#: its first delivery is r1, from the floor, no kept position left.
func TestSessionEndRemovesTheKeptPosition(t *testing.T) {
	for _, how := range []string{"expiry", "Clean Start", "expiry-zero departure"} {
		t.Run(how, func(t *testing.T) {
			b, pub := keptRig(t, "append", "")
			db := support.OpenDB(t, b.DB)
			const client = "ender"
			ps := numbered("kept/x", 2)
			support.PublishAll(t, pub, ps)
			connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
			if how == "expiry" {
				connect.Props = support.SessionExpiry(1)
			}
			c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "kept/#"))
			for _, p := range ps {
				acknowledge(t, c, support.DeliveredAt(t, c, 1, p.Topic, p.Payload).ID, 0)
			}
			unsubscribed(t, c, "kept/#")
			storedPosition(t, db, client, "kept")

			if how == "expiry-zero departure" {
				support.Disconnected(t, c, &harness.Disconnect{Props: support.SessionExpiry(0)})
			} else {
				support.Away(t, c)
			}
			// The CONNECT that follows ends the session with Clean Start 1,
			// or finds it gone: Session Present 0.
			again := &harness.Connect{ClientID: client, CleanStart: how == "Clean Start", Props: support.Durable}
			if how != "Clean Start" && !testkit.Within(harness.DefaultTimeout, func() bool { return sessionRows(t, db, client) == 0 }) {
				t.Fatalf("%s: %v after its connection ended, the database still holds the session of %q", how, harness.DefaultTimeout, client)
			}
			c, ack := harness.Connected(t, b.Addr, harness.V5, again)
			if ack.SessionPresent {
				t.Fatalf("%s: the CONNECT that follows has Session Present 1; want 0, the session ended", how)
			}
			support.Suback(t, c, nil, support.At(1, "kept/#"), 0x01)
			support.DeliveredAt(t, c, 1, ps[0].Topic, ps[0].Payload)
		})
	}
}
