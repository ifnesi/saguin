package conformance

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of retention and of removing a channel (design.md, Words:
// Retention and Missed; Channels; Restart, A changed configuration;
// Operations, saguin --drop-channel). Each record they store holds
// testkit.RecordBytes, or a multiple of it, so the sizes retention counts are known:
// storage.md, A message's bytes, "A message's bytes are its topic's,
// payload's and properties' bytes, the payload counted at its size as
// received. Retention and the size limits count those bytes."; their
// PUBLISHes carry no property, so the records' properties are "NULL for
// none".

// sizedWill is a Will on topic whose record is n bytes, as sized is.
func sizedWill(topic string, n int) *harness.Will {
	return &harness.Will{Topic: topic, Payload: testkit.Filling(topic, n)}
}

// heldRecord is one record a channel holds, as the operator reads it.
type heldRecord struct {
	offset   int64
	topic    string
	retained int // storage.md, records: "1 while it is its topic's current value"
}

// held reads from db the records the channel named holds, in offset order.
func held(t *testing.T, db support.RowsQuerier, channel string) []heldRecord {
	t.Helper()
	var rs []heldRecord
	support.Query(t, db, `SELECT r."offset", r.topic, r.retained FROM records r JOIN channels c ON c.id = r.channel
		WHERE c.name = ? ORDER BY r."offset"`, func(rows *sql.Rows) error {
		var r heldRecord
		err := rows.Scan(&r.offset, &r.topic, &r.retained)
		rs = append(rs, r)
		return err
	}, channel)
	return rs
}

// topicsOf is the topics of rs, in their order.
func topicsOf(rs []heldRecord) []string {
	topics := make([]string, len(rs))
	for i, r := range rs {
		topics[i] = r.topic
	}
	return topics
}

// awaitHeld waits until the channel named holds exactly the records of
// topics, in offset order, and returns them: what retention leaves once it
// has run. harness.DefaultTimeout is the test's watchdog, not a deadline of
// the broker's. after says when.
func awaitHeld(t *testing.T, db *sql.DB, channel string, topics []string, after string) []heldRecord {
	t.Helper()
	return awaitState(t, after+", channel "+channel+" holds "+fmt.Sprint(topics),
		func() []heldRecord { return held(t, db, channel) },
		func(rs []heldRecord) bool { return slices.Equal(topicsOf(rs), topics) })
}

// awaitState polls read until matches holds, returning the last observation.
// The timeout is a test watchdog, never a broker deadline; want names the
// state and context expected, and a failure includes the last observation.
func awaitState[T any](t *testing.T, want string, read func() T, matches func(T) bool) T {
	t.Helper()
	var got T
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = read()
		return matches(got)
	}) {
		t.Fatalf("after %v, got %+v; want %s", harness.DefaultTimeout, got, want)
	}
	return got
}

// floorSeries is saguin_channel_floor_offset of channel (metrics.md).
func floorSeries(channel string) string {
	return fmt.Sprintf(`saguin_channel_floor_offset{channel=%q}`, channel)
}

// removedSeries is saguin_channel_retention_removed_total of channel
// (metrics.md).
func removedSeries(channel string) string {
	return fmt.Sprintf(`saguin_channel_retention_removed_total{channel=%q}`, channel)
}

// expectSample fails the test unless series reads want on b's /metrics.
// The tests read it after committed's PUBACK: the core keeps at most one
// write transaction outstanding and applies every member's result before
// taking another event (design.md, Store, Writes), so a deletion the
// database already shows has had its answer applied, and its counts with it
// ("The core applies these counts once", Words: Missed).
func expectSample(t *testing.T, b *harness.Broker, series string, want float64, when string) {
	t.Helper()
	if got := testkit.Sample(t, b.Metrics(), series); got != want {
		t.Errorf("%s, %s = %v; want %v", when, series, got, want)
	}
}

// design.md, Words: Retention, "retention deletes a channel's oldest
// records by age and size, as Kafka does"; "The floor is the oldest record
// retention has not passed"; "Age retention becomes due at the earliest
// applicable record [...] deadline"; config.md, channels.<name>,
// retention_period, "append: removes old records"; PUBLISH: "The record's
// timestamp is the core's intake time"; SUBSCRIBE: append "Starts with the
// floor". metrics.md: saguin_channel_floor_offset, "the floor";
// saguin_channel_retention_removed_total, "records retention deleted".
//
// An append channel, aged, keeps records for 3 seconds. A publisher stores
// aged/1 and aged/2, and, half a second before their retention_period has
// passed since they were sent, aged/3. Until that instant the database
// holds the first two, which cannot be due before it; then it holds aged/3
// alone. A new subscriber to aged/# at QoS 1 receives aged/3 first, the
// floor, then aged/4, published after its SUBACK. The floor is aged/3's
// offset and two records were removed.
func TestAgeRetentionDeletesOldRecords(t *testing.T) {
	t.Parallel()
	const period = 3 * time.Second
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "aged", Type: "append", Filter: "aged/#", RetentionPeriod: period},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "aged-pub", CleanStart: true})
	old := []*harness.Publish{support.Sized("aged/1", testkit.RecordBytes), support.Sized("aged/2", testkit.RecordBytes)}
	due := time.Now().Add(period) // their intake follows their sending
	support.PublishAll(t, pub, old)
	young := support.Sized("aged/3", testkit.RecordBytes)
	sent := false
	for {
		rs := held(t, db, "aged")
		if time.Now().After(due) {
			break // the read may have seen the records past their deadline
		}
		want := []string{"aged/1", "aged/2"}
		if sent {
			want = append(want, young.Topic)
		}
		if !slices.Equal(topicsOf(rs), want) {
			t.Fatalf("%v before the first records' retention_period has passed, aged holds %q; want %q", time.Until(due), topicsOf(rs), want)
		}
		if !sent && time.Until(due) < 500*time.Millisecond {
			support.Acknowledged(t, pub, young)
			sent = true
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sent {
		support.Acknowledged(t, pub, young) // later than meant: further from its own deadline
	}
	rs := awaitHeld(t, db, "aged", []string{young.Topic}, "past the first records' retention_period")

	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "aged-sub", CleanStart: true}, support.At(1, "aged/#"))
	acknowledge(t, sub, support.DeliveredAt(t, sub, 1, young.Topic, young.Payload).ID, 0)
	fresh := support.Sized("aged/4", testkit.RecordBytes)
	support.Acknowledged(t, pub, fresh)
	support.DeliveredAt(t, sub, 1, fresh.Topic, fresh.Payload)

	committed(t, pub, "barrier")
	expectSample(t, b, floorSeries("aged"), float64(rs[0].offset), "once the first two records are deleted")
	expectSample(t, b, removedSeries("aged"), 2, "once the first two records are deleted")
}

// design.md, Words: Retention, "retention deletes a channel's oldest
// records by age and size"; "Every committed change that affects size
// schedules a size pass for its channel"; config.md, channels.<name>,
// retention_bytes, "append: removes the oldest beyond it"; storage.md,
// channels: bytes, "the bytes of its records".
//
// An append channel, sized, keeps 250 bytes; its retention_period is the
// day the harness gives every log, so only size can remove a record. A
// publisher stores sized/1 and sized/2, 100 bytes each: the database holds
// both. sized/3 makes 300 bytes, and its commit alone brings the size pass
// that leaves sized/2 and sized/3; sized/4 and sized/5, published
// together, leave those two alone, and the channel's bytes are 200.
func TestSizeRetentionKeepsTheNewest(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "sized", Type: "append", Filter: "sized/#", RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "sized-pub", CleanStart: true})
	ps := make([]*harness.Publish, 5)
	for i := range ps {
		ps[i] = support.Sized(fmt.Sprintf("sized/%d", i+1), testkit.RecordBytes)
	}
	support.PublishAll(t, pub, ps[:2])
	if got := topicsOf(held(t, db, "sized")); !slices.Equal(got, []string{"sized/1", "sized/2"}) {
		t.Fatalf("with 200 bytes stored, sized holds %q; want both records, within its retention_bytes", got)
	}
	support.Acknowledged(t, pub, ps[2])
	awaitHeld(t, db, "sized", []string{"sized/2", "sized/3"}, "once sized/3 has made 300 bytes")
	support.PublishAll(t, pub, ps[3:])
	awaitHeld(t, db, "sized", []string{"sized/4", "sized/5"}, "once sized/4 and sized/5 have made 400 bytes")
	if got := storedChannels(t, b.DB)["sized"].bytes; got != 2*testkit.RecordBytes {
		t.Errorf("channels holds bytes %d for sized; want %d, its two records'", got, 2*testkit.RecordBytes)
	}
}

// design.md, Words: Retention, "Only a current retained value on
// $broadcast stays, until its flag ends by its own rules"; "A retained
// value kept below the floor is sent only as a retained value"; Channels,
// $broadcast: "Retention counts only unflagged records toward its size";
// "RETAIN is 1 on retained values sent to a new subscription"; config.md,
// broadcast.retention_bytes, "$broadcast's retention". storage.md,
// channels: bytes, "excluding on $broadcast those flagged retained".
// metrics.md: saguin_channel_floor_offset.
//
// $broadcast keeps 250 bytes. A publisher stores a retained value of 200
// bytes on kept/value, then plain/1, plain/2 and plain/3, 100 bytes each:
// counted with the value, the plain records would pass 250 one at a time
// and each be deleted after its own commit. Counted without it, plain/3
// passes 250, and the size pass passes the value, oldest, and deletes
// plain/1: the database then holds the value, still flagged, plain/2 and
// plain/3, and $broadcast's bytes are 200. A new subscriber to kept/#
// receives the value, RETAIN 1. $broadcast's floor is plain/2's offset,
// above the value's.
func TestBroadcastRetentionKeepsTheRetainedValue(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, BroadcastRetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2, Channels: []harness.Channel{
		{Name: "barriers", Type: "append", Filter: "barrier/#"}, // committed's PUBLISHes stay off $broadcast
	}})
	db := support.OpenDB(t, b.DB)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "broadcast-pub", CleanStart: true})
	value := support.Sized("kept/value", 2*testkit.RecordBytes)
	value.Retain = true
	support.Acknowledged(t, pub, value)
	for i := 1; i <= 3; i++ {
		support.Acknowledged(t, pub, support.Sized(fmt.Sprintf("plain/%d", i), testkit.RecordBytes))
	}
	rs := awaitHeld(t, db, "$broadcast", []string{value.Topic, "plain/2", "plain/3"}, "once plain/3 has made 300 unflagged bytes")
	if rs[0].retained != 1 {
		t.Errorf("the record of %s has retained %d; want 1, the topic's current retained value", value.Topic, rs[0].retained)
	}
	if got := storedChannels(t, b.DB)["$broadcast"].bytes; got != 2*testkit.RecordBytes {
		t.Errorf("channels holds bytes %d for $broadcast; want %d, its unflagged records'", got, 2*testkit.RecordBytes)
	}
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "broadcast-sub", CleanStart: true}, support.At(1, "kept/#"))
	retainedPassAt(t, sub, 1, map[string][]byte{value.Topic: value.Payload})

	committed(t, pub, "barrier/1")
	expectSample(t, b, floorSeries("$broadcast"), float64(rs[1].offset), "once plain/1 is deleted")
}

// design.md, Channels, append: "A consumer behind the floor jumps to it.
// Only the records it was owed are counted as missed"; Words: Missed, "A
// loss counts records owed under the consumer's filtering [...] An offset
// gap is not a count"; "Connected consumers use their progress held by the
// core"; "Deliveries already settled or handed to a writer are excluded";
// Words: Retention, a record past retention "is gone, whoever still needed
// it: a slow [...] consumer, a message sent and not yet acknowledged";
// Delivery, One mechanism: "A message is in flight from its hand-off to the
// writer". metrics.md, saguin_missed_total: "one record owed to two
// consumers contributes two".
//
// An append channel, events, keeps 350 bytes. Two clients, each with
// Receive Maximum 1, subscribe at QoS 1: narrow to events/a/#, wide to
// events/#. A publisher stores events/a/1; each receives it and holds it
// unacknowledged. The publisher then stores events/b/1, events/a/2,
// events/b/2, events/a/3 and events/b/3, 100 bytes each: retention leaves
// the last three. The first record was in flight to both; events/b/1 was
// owed to wide alone, events/a/2 to both: three consumer-record losses, where
// the offset gaps behind the floor would make four. Each client then
// acknowledges events/a/1: narrow next receives events/a/3, wide
// events/b/2, the first owed at the floor.
func TestLaggingConsumerJumpsToTheFloor(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: 3*testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	narrow := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "narrow", CleanStart: true, Props: receiveMaximum5(1)}, support.At(1, "events/a/#"))
	wide := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "wide", CleanStart: true, Props: receiveMaximum5(1)}, support.At(1, "events/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "events-pub", CleanStart: true})
	first := support.Sized("events/a/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, first)
	narrowHeld := support.DeliveredAt(t, narrow, 1, first.Topic, first.Payload)
	wideHeld := support.DeliveredAt(t, wide, 1, first.Topic, first.Payload)
	rest := make(map[string]*harness.Publish)
	for _, topic := range []string{"events/b/1", "events/a/2", "events/b/2", "events/a/3", "events/b/3"} {
		rest[topic] = support.Sized(topic, testkit.RecordBytes)
		support.Acknowledged(t, pub, rest[topic])
	}
	awaitHeld(t, db, "events", []string{"events/b/2", "events/a/3", "events/b/3"}, "once events/b/3 has made 600 bytes")

	acknowledge(t, narrow, narrowHeld.ID, 0)
	support.DeliveredAt(t, narrow, 1, "events/a/3", rest["events/a/3"].Payload)
	acknowledge(t, wide, wideHeld.ID, 0)
	support.DeliveredAt(t, wide, 1, "events/b/2", rest["events/b/2"].Payload)
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 3, "once retention has passed the two lagging consumers")
}

// design.md, Words: Missed, "A loss counts records owed under the
// consumer's filtering, start, seek, No Local and partition rules";
// "Retention determines losses before removing the information needed to
// count them"; "Away sessions use their stored state"; Words: Retention, a
// record past retention is gone, whoever still needed it, "a slow or away
// consumer"; config.md, channels.<name>.start, "tail: where a reader with no
// position begins"; SUBSCRIBE, Options: "No Local is honoured everywhere,
// against the publisher's client id stored on the record"; Partitioning;
// DISCONNECT: a Will is published when its connection is lost; storage.md,
// wills: client, "the publisher's client id". metrics.md,
// saguin_missed_total: "away sessions included".
//
// An append channel, events, starting new readers at the tail, keeps 150
// bytes: each 100-byte record's commit deletes the one before. A publisher
// stores p0, in events/a/# and the partition below. A client holding a
// durable session, its Will on events/a/will, subscribes at QoS 1 to
// events/a/# with No Local and one of two partitions, the Will topic's, and
// drops its connection: its Will is stored, and p0 deleted. While the
// session is away the publisher stores o1, in its filter and partition; o2,
// in its filter, the other partition; o3, outside its filter; o4, as o1;
// and a last record, outside its filter, which alone remains. Of the six
// records deleted, four are not owed, each for a reason of its own: p0 lies
// below the session's start, the Will is its own (No Local), o2 is in the
// other partition and o3 outside the filter. Two, o1 and o4, are counted
// missed.
func TestAwaySessionLossesComeFromItsStoredState(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", Start: "tail", RetentionBytes: testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	const client = "away-owed"
	will := sizedWill("events/a/will", testkit.RecordBytes)
	index := harness.TopicHash(will.Topic) % 2
	// inPartition is the next topic events/a/<k>, from k, whose hash falls
	// in the session's partition, or out of it.
	k := 0
	inPartition := func(in bool) string {
		for {
			topic := fmt.Sprintf("events/a/%d", k)
			k++
			if (harness.TopicHash(topic)%2 == index) == in {
				return topic
			}
		}
	}
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "events-pub", CleanStart: true})
	support.Acknowledged(t, pub, support.Sized(inPartition(true), testkit.RecordBytes))

	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable, Will: will})
	support.Suback(t, c, partitioned(fmt.Sprintf("topic_hash(2,%d)", index)), []harness.Subscription{{Filter: "events/a/#", QoS: 1, NoLocal: true}}, 0x01)
	support.DropSocket(t, c)
	support.AwaitAway(t, db, client)
	awaitHeld(t, db, "events", []string{will.Topic}, "once the session's Will is published")

	last := support.Sized("events/b/last", testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{
		support.Sized(inPartition(true), testkit.RecordBytes), support.Sized(inPartition(false), testkit.RecordBytes), support.Sized("events/b/1", testkit.RecordBytes),
		support.Sized(inPartition(true), testkit.RecordBytes), last,
	})
	awaitHeld(t, db, "events", []string{last.Topic}, "once the publisher's last record is stored")
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 2, "once retention has passed the away session's records")
}

// design.md, Words: Retention, a record past retention is gone, whoever
// still needed it, "a message sent and not yet acknowledged"; "A channel's
// retention [...] may pass a record a session is owed, even one in its
// in-flight window. The session continues and the loss is counted. This
// departs from MQTT 5 §4.1 and MQTT 3.1.1 §4.1, and from resending an
// unacknowledged message on reconnect (MQTT-4.4.0-1)"; Words: Missed,
// "Deliveries already settled or handed to a writer are excluded"; "A
// recovered exchange whose record retention has deleted is counted as
// missed (retention) when its session resumes, and settled as if sent";
// Delivery, A message over the client's Maximum Packet Size: "as is one
// whose record retention has deleted".
//
// An append channel, events, keeps 150 bytes. A client holding a durable
// session, with Receive Maximum 1, subscribes at QoS 1 to events/#,
// receives r1 and leaves with it unacknowledged. A publisher stores r2,
// then r3: retention deletes r1 and r2, and one record, r2, is counted
// missed, r1 having been handed to the writer. The client resumes: r1 is not
// resent; its next PUBLISH is r3, a first delivery, and the count is two.
func TestRecoveredExchangeOfADeletedRecordIsMissed(t *testing.T) {
	b := support.Launch(t, &harness.Options{Operations: true, Channels: []harness.Channel{
		{Name: "events", Type: "append", Filter: "events/#", RetentionBytes: testkit.RecordBytes + testkit.RecordBytes/2},
	}})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: "resumer", CleanStart: true, Props: slices.Concat(support.Durable, receiveMaximum5(1))}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "events/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "events-pub", CleanStart: true})
	r1 := support.Sized("events/r1", testkit.RecordBytes)
	support.Acknowledged(t, pub, r1)
	support.DeliveredAt(t, sub, 1, r1.Topic, r1.Payload)
	support.Away(t, sub)

	r3 := support.Sized("events/r3", testkit.RecordBytes)
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("events/r2", testkit.RecordBytes), r3})
	awaitHeld(t, db, "events", []string{r3.Topic}, "once r3 is stored")
	committed(t, pub, "barrier")
	expectSample(t, b, testkit.MissedSeries("events"), 1, "once retention has passed r1, in flight, and r2, owed")

	sub = resumed(t, b.Addr, harness.V5, connect)
	support.DeliveredAt(t, sub, 1, r3.Topic, r3.Payload)
	expectSample(t, b, testkit.MissedSeries("events"), 2, "once the session has resumed without r1")
}

// design.md, Words: Missed, "A recovered exchange whose record retention
// has deleted is counted as missed (retention) when its session resumes,
// and settled as if sent"; Delivery, Positions: "Deleting a durable
// session's settled exchange row must commit with its position beyond
// that record"; Flows, SUBSCRIBE, UNSUBSCRIBE: "In-flight messages are
// finished"; Execution, Ready work, 1: "Each change that allows work to
// continue schedules it immediately: applying a store answer, settling a
// delivery, returning capacity or ending a request".
//
// An append channel, aged, keeps records for a second. A client holding a
// durable session subscribes at QoS 1 to b/# and aged/#, receives aged/1
// unacknowledged, unsubscribes from aged/# and leaves: its exchange stays
// stored, owned by no subscription. Once retention has deleted aged/1, the
// client resumes and sends nothing more: the exchange is settled as if
// sent, and its row leaves the database with no further packet to carry
// it.
func TestRecoveredExchangeOwnedByNoSubscriptionIsSettled(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{
		{Name: "aged", Type: "append", Filter: "aged/#", RetentionPeriod: time.Second},
	}})
	db := support.OpenDB(t, b.DB)
	connect := &harness.Connect{ClientID: clientID(t, ""), CleanStart: true, Props: support.Durable}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "b/#", "aged/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	r := support.Sized("aged/1", testkit.RecordBytes)
	support.Acknowledged(t, pub, r)
	support.DeliveredAt(t, sub, 1, r.Topic, r.Payload)
	unsuback(t, sub, nil, []string{"aged/#"}, 0x00)
	support.Away(t, sub)
	if got := storedInflight(t, db, connect.ClientID); len(got) != 1 {
		t.Fatalf("the session left with %d exchanges stored; want 1, aged/1's", len(got))
	}
	awaitHeld(t, db, "aged", nil, "once aged's retention_period has passed")

	resumed(t, b.Addr, harness.V5, connect)
	var got []inflightRow
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = storedInflight(t, db, connect.ClientID)
		return len(got) == 0
	}) {
		t.Fatalf("after the resume the session still has %d exchanges stored; want none, aged/1's settled as if sent", len(got))
	}
}

// design.md, Words: Retention, "Startup processes overdue retention before
// serving"; Restart, Start: "Listeners and bridges start last"; SUBSCRIBE:
// append "Starts with the floor"; PUBLISH: "The record's timestamp is the
// core's intake time".
//
// An append channel, lapsing, keeps records for 3 seconds. A publisher
// stores lapsing/1 and lapsing/2, and the broker stops at once: its
// database still holds both. Once their retention_period has passed since
// their PUBACKs, the broker starts again: as soon as it listens, the
// database holds no record on lapsing, and the first client, subscribing
// to lapsing/# at QoS 1, receives as its first PUBLISH lapsing/3, published
// after its SUBACK.
func TestOverdueRetentionRunsBeforeServing(t *testing.T) {
	t.Parallel()
	const period = 3 * time.Second
	o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{
		{Name: "lapsing", Type: "append", Filter: "lapsing/#", RetentionPeriod: period},
	}}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lapsing-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("lapsing/1", testkit.RecordBytes), support.Sized("lapsing/2", testkit.RecordBytes)})
	due := time.Now().Add(period) // their intake preceded their PUBACKs
	stopped(t, b)
	if got := topicsOf(held(t, support.OpenDB(t, b.DB), "lapsing")); !slices.Equal(got, []string{"lapsing/1", "lapsing/2"}) {
		t.Fatalf("after the stop, lapsing holds %q; want both records, the stop within their retention_period", got)
	}
	time.Sleep(time.Until(due))

	b = support.Launch(t, o)
	if got := topicsOf(held(t, support.OpenDB(t, b.DB), "lapsing")); len(got) != 0 {
		t.Errorf("once the broker listens, lapsing holds %q; want nothing, the overdue records deleted before serving", got)
	}
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lapsing-first", CleanStart: true}, support.At(1, "lapsing/#"))
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lapsing-pub", CleanStart: true})
	fresh := support.Sized("lapsing/3", testkit.RecordBytes)
	support.Acknowledged(t, pub, fresh)
	support.DeliveredAt(t, sub, 1, fresh.Topic, fresh.Payload)
}

// design.md, Store, A full disk: "a write is answered "full" only when
// SQLite reports lack of space and rollback confirms that the transaction
// did not commit. The broker continues after that answer"; "A housekeeping
// write refused for room sets a retry deadline one second after that
// answer [...] At the deadline, the core rediscovers [...] retention work
// from the store"; Writes: "Rollback contributes nothing"; Words: Missed,
// "The core applies these counts once. Rollback contributes nothing".
// metrics.md: saguin_store_full, "1 while the disk is full: from a write
// SQLite could not save for room until the next saved one";
// saguin_channel_retention_removed_total; saguin_missed_total.
//
// On a filesystem of its own, an append channel, lapsing, keeps records for
// 2 seconds. A client holding a durable session subscribes at QoS 1 to
// lapsing/# and goes away; a publisher stores lapsing/1 and lapsing/2, and
// a ballast then takes every byte left. Once their retention_period has
// passed, a write is refused for room: the database still holds both. With
// the ballast emptied and no client acting, the retry deletes them; then
// two records have been removed and two counted missed, each once, and the
// disk is no longer full.
func TestFullDiskDefersRetention(t *testing.T) {
	t.Parallel()
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	const period = 2 * time.Second
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db"), Operations: true, Channels: []harness.Channel{
		{Name: "lapsing", Type: "append", Filter: "lapsing/#", RetentionPeriod: period},
	}})
	db := support.OpenDB(t, b.DB)
	const client = "lapsing-owed"
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}, support.At(1, "lapsing/#")))
	support.AwaitAway(t, db, client)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "lapsing-pub", CleanStart: true})
	support.PublishAll(t, pub, []*harness.Publish{support.Sized("lapsing/1", testkit.RecordBytes), support.Sized("lapsing/2", testkit.RecordBytes)})
	due := time.Now().Add(period)
	ballast := testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	time.Sleep(time.Until(due))
	testkit.AwaitSample(t, b.Metrics(), "saguin_store_full", 1, harness.DefaultTimeout)
	if got := topicsOf(held(t, db, "lapsing")); !slices.Equal(got, []string{"lapsing/1", "lapsing/2"}) {
		t.Fatalf("with the retention write refused for room, lapsing holds %q; want both records, the deletion rolled back", got)
	}
	if err := ballast.Truncate(0); err != nil {
		t.Fatal(err)
	}
	awaitHeld(t, db, "lapsing", nil, "once the ballast is emptied, with no client acting")
	committed(t, pub, "barrier")
	expectSample(t, b, removedSeries("lapsing"), 2, "once the retried deletion has committed")
	expectSample(t, b, testkit.MissedSeries("lapsing"), 2, "once the retried deletion has committed")
	expectSample(t, b, "saguin_store_full", 0, "once the retried deletion has committed")
}

// dropFlag is the command removing a channel: design.md, Operations,
// "saguin --drop-channel <config> <channel>".
const dropFlag = "--drop-channel"

// design.md, Operations: "saguin --drop-channel <config> <channel> | with
// the broker stopped, deletes a channel the configuration no longer has
// [...]: its records, the positions and start boundaries on it [...]; a
// subscription reaching a surviving channel is kept, losing only the
// dropped channel's state. The command determines surviving reach from the
// resolved configuration used for this invocation"; Store, The lock: "The
// drop-channel command takes the same lock"; "A broker that finds the lock
// held by a live process refuses to start, naming the lock"; Restart, A
// changed configuration: "A stored channel that is no longer configured
// [...] stops startup, naming it. Startup remains refused until the
// drop-channel command deletes it"; "records stay in the channel where they
// landed"; Words: Channel, "A subscription's filter reaches the channels its
// topics land in".
//
// A broker holds two append channels, kept (kept/#) and gone (kept/g/#).
// A publisher stores kept/1, in kept, and kept/g/1, in gone; a client
// holding a durable session subscribes at QoS 1 to kept/#, which reaches
// both, receives and acknowledges both, and goes away; the broker stops.
// On the configuration without gone the broker refuses to start, naming
// gone. With a broker running on the first configuration, the command on
// the second refuses, naming the lock, and changes nothing. With the broker
// stopped, the command exits 0: the database holds no channel gone, no
// record on it, kept/1 still in kept, and for the session its subscription to kept/# and
// its position on kept, none on gone. The broker then starts on the second
// configuration; the session resumes, and kept/g/2, now landing in kept,
// is its next PUBLISH.
func TestDropChannelRemovesAChannelNoLongerConfigured(t *testing.T) {
	dir := t.TempDir()
	kept := harness.Channel{Name: "kept", Type: "append", Filter: "kept/#"}
	gone := harness.Channel{Name: "gone", Type: "append", Filter: "kept/g/#"}
	both := &harness.Options{Dir: dir, Channels: []harness.Channel{kept, gone}}
	without := &harness.Options{Dir: dir, Channels: []harness.Channel{kept}}
	b := support.Launch(t, both)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "drop-pub", CleanStart: true})
	ps := []*harness.Publish{support.Sized("kept/1", testkit.RecordBytes), support.Sized("kept/g/1", testkit.RecordBytes)}
	support.PublishAll(t, pub, ps)
	const client = "drop-reader"
	connect := &harness.Connect{ClientID: client, CleanStart: true, Props: support.Durable}
	sub := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "kept/#"))
	for range ps {
		p := harness.Expect[*harness.Publish](t, sub)
		acknowledge(t, sub, p.ID, 0)
	}
	committed(t, sub, "barrier")
	support.Away(t, sub)
	stopped(t, b)
	channels := storedChannels(t, b.DB)
	keptID, goneID := channels["kept"].id, channels["gone"].id
	onChannel := func(ps []positionRow, id int64) bool {
		return slices.ContainsFunc(ps, func(p positionRow) bool { return p.channel == id })
	}
	if ps := storedSubscriptions(t, support.OpenDB(t, b.DB), client).positions; !onChannel(ps, keptID) || !onChannel(ps, goneID) {
		t.Fatalf("before the drop, positions holds %+v for %s; want one on kept (channel %d) and one on gone (channel %d)", ps, client, keptID, goneID)
	}

	err := support.Started(t, without).Listening()
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 || !strings.Contains(err.Error(), "gone") {
		t.Fatalf("the start without gone: %v; want it refused, exit status 1, naming gone", err)
	}

	running := support.Launch(t, both)
	config, err := harness.WriteConfig(&harness.Options{Dir: dir, Channels: without.Channels})
	if err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := execute(t, support.BrokerBinary, dropFlag, config, "gone")
	if code == 0 || !strings.Contains(stdout+stderr, running.DB+".lock") {
		t.Errorf("%s with the broker running exited %d:\n%s%s\nwant it refused, naming the lock %s.lock", dropFlag, code, stdout, stderr, running.DB)
	}
	stopped(t, running)
	db := support.OpenDB(t, b.DB)
	if got := topicsOf(held(t, db, "gone")); !slices.Equal(got, []string{"kept/g/1"}) {
		t.Fatalf("after the refused %s, gone holds %q; want its record, unchanged", dropFlag, got)
	}

	if code, stdout, stderr := execute(t, support.BrokerBinary, dropFlag, config, "gone"); code != 0 {
		t.Fatalf("%s with the broker stopped exited %d:\n%s%s\nwant 0", dropFlag, code, stdout, stderr)
	}
	if _, ok := storedChannels(t, b.DB)["gone"]; ok {
		t.Error("after the drop, channels still holds gone")
	}
	var onGone int
	if err := db.QueryRow(`SELECT count(*) FROM records WHERE channel = ?`, goneID).Scan(&onGone); err != nil {
		t.Fatal(err)
	}
	if got := topicsOf(held(t, db, "kept")); onGone != 0 || !slices.Equal(got, []string{"kept/1"}) {
		t.Errorf("after the drop, the database holds %d records on gone (channel %d), and kept holds %q; want none on gone, and kept/1 in kept", onGone, goneID, got)
	}
	after := storedSubscriptions(t, db, client)
	if len(after.subscriptions) != 1 || after.subscriptions[0].filter != "kept/#" {
		t.Errorf("after the drop, %s's subscriptions are %+v; want kept/#, which still reaches kept", client, after.subscriptions)
	}
	if !onChannel(after.positions, keptID) || onChannel(after.positions, goneID) {
		t.Errorf("after the drop, %s's positions are %+v; want its position on kept (channel %d), none on gone (channel %d)", client, after.positions, keptID, goneID)
	}

	b = support.Launch(t, without)
	sub = resumed(t, b.Addr, harness.V5, connect)
	pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "drop-pub", CleanStart: true})
	moved := support.Sized("kept/g/2", testkit.RecordBytes)
	support.Acknowledged(t, pub, moved)
	support.DeliveredAt(t, sub, 1, moved.Topic, moved.Payload)
}

// design.md, Operations, saguin --drop-channel: "Subscriptions are kept:
// each then reaches the channels its topics land in under the resolved
// configuration used for this invocation, $broadcast where no configured
// channel claims them"; "A channel a session's subscription newly reaches
// this way starts at that session's next CONNECT, as a subscription taken
// then would (SUBSCRIBE, The start point)"; SUBSCRIBE: $broadcast "Starts
// with | the matching retained values, as Retain Handling allows, then the
// next message"; The start point: "The core takes that channel's next
// offset when it takes the SUBSCRIBE [...] Every publication that entered
// before the SUBSCRIBE therefore falls below the boundary"; "The captured
// boundary separates the initial pass, which reads retained or current
// values below it, from subsequent live records"; Channels, $broadcast:
// "RETAIN is 1 on retained values sent to a new subscription". MQTT 5
// 3.3.1.3, Retain Handling: "0 = Send retained messages at the time of the
// subscribe"; "2 = Do not send retained messages at the time of the
// subscribe".
//
// A broker holds one append channel, gone, claiming dev/#. Two clients
// holding durable sessions subscribe at QoS 1 to dev/#, which reaches gone
// alone: sends with Retain Handling 0, skips with Retain Handling 2. Both
// go away and the broker stops; the command drops gone, and the broker
// starts without it, so dev/# now reaches $broadcast alone. A publisher
// stores a retained value on dev/value and a plain message on dev/plain,
// both now on $broadcast, before either session returns. sends resumes:
// it receives the retained value, RETAIN 1, and nothing of dev/plain, which
// entered before its CONNECT; skips resumes and receives neither. A message
// on dev/live, published after both have received what their CONNECT
// starts them with, is the next PUBLISH of each.
func TestDropChannelNewReachStartsAtTheNextConnect(t *testing.T) {
	dir := t.TempDir()
	gone := &harness.Options{Dir: dir, Channels: []harness.Channel{{Name: "gone", Type: "append", Filter: "dev/#"}}}
	without := &harness.Options{Dir: dir}
	b := support.Launch(t, gone)
	connects := map[byte]*harness.Connect{
		0: {ClientID: "sends", CleanStart: true, Props: support.Durable},
		2: {ClientID: "skips", CleanStart: true, Props: support.Durable},
	}
	for handling, connect := range connects {
		c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
		support.Suback(t, c, nil, []harness.Subscription{{Filter: "dev/#", QoS: 1, RetainHandling: handling}}, 0x01)
		support.Away(t, c)
	}
	stopped(t, b)
	config, err := harness.WriteConfig(without)
	if err != nil {
		t.Fatal(err)
	}
	if code, stdout, stderr := execute(t, support.BrokerBinary, dropFlag, config, "gone"); code != 0 {
		t.Fatalf("%s with the broker stopped exited %d:\n%s%s\nwant 0", dropFlag, code, stdout, stderr)
	}

	b = support.Launch(t, without)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "dev-pub", CleanStart: true})
	value := &harness.Publish{QoS: 1, Retain: true, Topic: "dev/value", Payload: []byte("retained after the restart")}
	support.PublishAll(t, pub, []*harness.Publish{value, {QoS: 1, Topic: "dev/plain", Payload: []byte("entered before the CONNECT")}})
	sends := resumed(t, b.Addr, harness.V5, connects[0])
	retainedPassAt(t, sends, 1, map[string][]byte{value.Topic: value.Payload})
	skips := resumed(t, b.Addr, harness.V5, connects[2])
	committed(t, skips, "barrier") // its CONNECT is taken: its boundary lies below what follows

	next := &harness.Publish{QoS: 1, Topic: "dev/live", Payload: []byte("published after both CONNECTs")}
	support.Acknowledged(t, pub, next)
	liveAt(t, sends, 1, next.Topic, next.Payload)
	liveAt(t, skips, 1, next.Topic, next.Payload)
}
