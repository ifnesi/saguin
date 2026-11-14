package conformance

import (
	"bytes"
	"database/sql"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// record is one row of storage.md's records, as the operator reads it from
// the database once the broker has exited.
type record struct {
	channel, offset   int64
	name              string // the channel's name, from storage.md's channels
	id, topic         string
	time              int64 // its timestamp, Unix milliseconds
	qos               int
	publisher         string
	bridge            string // the name of the bridge it arrived over, from storage.md's bridges; "" for none (bridge 0)
	payload           []byte // the stored payload, compressed where storage.md, A stored payload, says
	payloadSize       int    // payload_size: "the payload's size as received"
	properties        []byte // nil where the column is NULL: no properties
	publishedRetained int
	retained          int    // 1 while it is its topic's current value: on $broadcast its retained value, on a latest channel every record
	expires           *int64 // nil where the column is NULL: no Message Expiry
}

// records reads every row of storage.md's records in the database at path,
// by channel and offset, each with its channel's name and the name of the
// bridge it arrived over.
func records(t *testing.T, path string) []record {
	t.Helper()
	var rs []record
	support.Query(t, support.OpenDB(t, path), `SELECT r.channel, c.name, r."offset", r.id, r."time", r.topic, r.qos, r.publisher, coalesce(b.name, ''),
		r.payload, r.payload_size, r.properties, r.published_retained, r.retained, r.expires
		FROM records r JOIN channels c ON c.id = r.channel LEFT JOIN bridges b ON b.id = r.bridge ORDER BY r.channel, r."offset"`, func(rows *sql.Rows) error {
		var r record
		err := rows.Scan(&r.channel, &r.name, &r.offset, &r.id, &r.time, &r.topic, &r.qos, &r.publisher, &r.bridge, &r.payload, &r.payloadSize, &r.properties,
			&r.publishedRetained, &r.retained, &r.expires)
		rs = append(rs, r)
		return err
	})
	return rs
}

// stopped stops b gracefully and fails the test unless it exits 0
// (design.md, Restart, A stop for a failure: "The exit status is 0 after a
// stop requested by SIGINT or SIGTERM"), so its database can be read whole.
func stopped(t *testing.T, b *harness.Broker) {
	t.Helper()
	if state := stop(t, b); !state.Success() {
		t.Fatalf("the graceful stop ended %v, want exit status 0", state)
	}
}

// pubrecOf waits for c's PUBREC to p, a QoS 2 PUBLISH sent before, and
// fails the test unless it carries reason (design.md, Flows, PUBLISH: QoS 2
// "PUBREC, then PUBCOMP once in the channel"; "A QoS 2 refusal rides the
// PUBREC"); MQTT 3.1.1 reads its PUBREC as 0x00.
func pubrecOf(t *testing.T, c *harness.Client, p *harness.Publish, reason byte) {
	t.Helper()
	support.AnswerOf(t, c, harness.PUBREC, p, reason)
}

// propertiesOf is ps in the wire form storage.md stores ("a Property Length
// followed by the properties, as internal/packet writes them (MQTT 5
// 2.2.2)"), nil for none ("NULL for none").
func propertiesOf(t *testing.T, ps harness.Properties) []byte {
	t.Helper()
	if len(ps) == 0 {
		return nil
	}
	b, err := harness.AppendProperties(nil, ps)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// design.md, Flows, PUBLISH: "1 | on PUBLISH | PUBACK once stored";
// Channels, $broadcast: "the reserved append channel for every topic no
// configured channel claims", "Every message goes in, whatever its QoS", and
// a record keeps "*published retained*, MQTT's RETAIN bit, which never
// changes"; Words: Record, "id, time, topic, QoS, MQTT properties, payload,
// offset and the publisher's client id"; Offset, "an offset is a channel's
// counter. The core assigns it"; storage.md, Ids: "The $broadcast channel is
// channel 1"; storage.md, Logs, records: its columns; storage.md: "A
// message's properties are a BLOB holding its MQTT 5 properties in their wire
// form [...] NULL for none"; design.md, Flows, Replies: PUBACK "sent only
// once the store confirms".
//
// An MQTT 5 client publishes three messages at QoS 1, with no properties,
// with RETAIN and properties, and with other properties, each answered
// PUBACK 0x00. After the broker stops, the database holds them on channel
// 1 at offsets 1, 2 and 3 in publish order, each with its topic, payload,
// QoS 1, the publisher's client id, its properties as sent and its RETAIN
// bit as published retained.
func TestPublishIsStoredOnBroadcast(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	sent := []*harness.Publish{
		{QoS: 1, Topic: "sport/tennis/player1", Payload: []byte("a")},
		{QoS: 1, Retain: true, Topic: "a//b", Payload: []byte("b"),
			Props: harness.Properties{{ID: harness.ContentType, Str: "text/plain"}, harness.User("k", "v")}},
		{QoS: 1, Topic: "Accounts payable", Payload: []byte{0x00, 0xFF},
			Props: harness.Properties{{ID: harness.ResponseTopic, Str: "reply/to"}, {ID: harness.CorrelationData, Data: []byte{1, 2}}}},
	}
	for _, p := range sent {
		support.Acknowledged(t, c, p)
	}
	stopped(t, b)

	got := records(t, b.DB)
	if len(got) != len(sent) {
		t.Fatalf("the database holds %d records, want the %d published: %+v", len(got), len(sent), got)
	}
	for i, p := range sent {
		want := record{channel: 1, offset: int64(i + 1), topic: p.Topic, qos: 1, publisher: "publisher",
			payload: p.Payload, properties: propertiesOf(t, p.Props)}
		if p.Retain {
			want.publishedRetained = 1
		}
		r := got[i]
		if r.channel != want.channel || r.offset != want.offset || r.topic != want.topic || r.qos != want.qos ||
			r.publisher != want.publisher || !bytes.Equal(r.payload, want.payload) ||
			!bytes.Equal(r.properties, want.properties) || (r.properties == nil) != (want.properties == nil) ||
			r.publishedRetained != want.publishedRetained {
			t.Errorf("record %d: channel %d, offset %d, topic %q, QoS %d, publisher %q, payload % X, properties % X (NULL %v), published_retained %d;\n"+
				"want channel %d, offset %d, topic %q, QoS %d, publisher %q, payload % X, properties % X (NULL %v), published_retained %d",
				i+1, r.channel, r.offset, r.topic, r.qos, r.publisher, r.payload, r.properties, r.properties == nil, r.publishedRetained,
				want.channel, want.offset, want.topic, want.qos, want.publisher, want.payload, want.properties, want.properties == nil, want.publishedRetained)
		}
	}
}

// design.md, Flows, PUBLISH: "0 | on PUBLISH | never" (the publisher is
// never answered); Channels, $broadcast: "Every message goes in, whatever
// its QoS"; MQTT 5 3.3.4, Table 3-3: "QoS 0 | None".
//
// An MQTT 5 client publishes at QoS 0 and then pings: the PINGRESP is the
// next packet it reads, nothing answering the publish. The core takes the
// connection's packets in order (R2), so it took the publish before the
// PINGREQ; after the stop, which waits for "every store request it sent"
// (Restart, Graceful stop), the database holds the record on channel 1 at
// QoS 0.
func TestQoS0PublishIsStoredUnanswered(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "quiet", CleanStart: true})
	if _, err := c.Publish(&harness.Publish{Topic: "qos/0", Payload: []byte("zero")}); err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("after a QoS 0 PUBLISH: %v; want the PINGRESP next, the publish unanswered", err)
	}
	stopped(t, b)
	got := records(t, b.DB)
	if len(got) != 1 || got[0].channel != 1 || got[0].topic != "qos/0" || got[0].qos != 0 || !bytes.Equal(got[0].payload, []byte("zero")) {
		t.Errorf("the database holds %+v; want one record on channel 1, topic \"qos/0\", QoS 0, payload \"zero\"", got)
	}
}

// uuidv7 is a UUID of version 7 in RFC 9562's text form: 8-4-4-4-12 hex
// digits, the version digit 7 and the variant bits 10.
var uuidv7 = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// design.md, Sagüin properties: "The saguin-id property sets the record's
// id"; "A publish carrying saguin-id twice takes the first"; "The value of
// saguin-id is the publisher's own where it set one, kept as sent. Otherwise
// the broker makes a UUIDv7"; storage.md, records: id, "its saguin-id".
//
// An MQTT 5 client publishes at QoS 1 once with two saguin-id User
// Properties, then twice with none. After the stop, the first record's id
// is the first saguin-id as sent, and the other two are UUIDv7s, distinct.
func TestSaguinID(t *testing.T) {
	const own = "Sensor-42/0001 é"
	b := support.Launch(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "ids", CleanStart: true})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "ids", Payload: []byte("own"),
		Props: harness.Properties{harness.User("saguin-id", own), harness.User("saguin-id", "second")}})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "ids", Payload: []byte("made")})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "ids", Payload: []byte("made")})
	stopped(t, b)

	got := records(t, b.DB)
	if len(got) != 3 {
		t.Fatalf("the database holds %d records, want 3: %+v", len(got), got)
	}
	if got[0].id != own {
		t.Errorf("the record published with saguin-id %q, then %q, has id %q; want the first, as sent", own, "second", got[0].id)
	}
	for _, r := range got[1:] {
		if !uuidv7.MatchString(r.id) {
			t.Errorf("record %d, published with no saguin-id, has id %q; want a UUIDv7", r.offset, r.id)
		}
	}
	if got[1].id == got[2].id {
		t.Errorf("two records published with no saguin-id share the id %q; want each its own", got[1].id)
	}
}

// design.md, Words: Offset, "a channel's counter, assigned by the core,
// never reused once stored"; Restart: "Restored: [...] each channel's
// offset counter"; Store, full mode: "Everything acknowledged survives";
// "A restart recovers committed state".
//
// An MQTT 5 client publishes two messages at QoS 1 and the broker stops.
// Started again on the same files, it takes a third. After its stop, the
// database holds all three on channel 1, at offsets 1, 2 and 3 in publish
// order.
func TestRecordsSurviveARestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := support.Launch(t, &harness.Options{Dir: dir})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "restarts", CleanStart: true})
	for _, payload := range []string{"before-1", "before-2"} {
		support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "restart", Payload: []byte(payload)})
	}
	stopped(t, b)

	again := support.Launch(t, &harness.Options{Dir: dir})
	c, _ = harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: "restarts", CleanStart: true})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "restart", Payload: []byte("after")})
	stopped(t, again)

	got := records(t, again.DB)
	want := []string{"before-1", "before-2", "after"}
	if len(got) != len(want) {
		t.Fatalf("after the restart the database holds %d records, want %d: %+v", len(got), len(want), got)
	}
	for i, r := range got {
		if r.channel != 1 || r.offset != int64(i+1) || string(r.payload) != want[i] {
			t.Errorf("record %d: channel %d, offset %d, payload %q; want channel 1, offset %d, payload %q",
				i+1, r.channel, r.offset, r.payload, i+1, want[i])
		}
	}
}

// design.md, Words, Offset: "A publication refused after entering leaves
// its offset unused"; "The stored counter is raised with every saved record
// and every saved live-start boundary. Startup resumes from that counter";
// Flows, SUBSCRIBE, The start point: "The core takes that channel's next
// offset when it takes the SUBSCRIBE [...] The core stores the boundary with
// a durable session's subscription"; storage.md, channels, next: "the offset
// the next publication entering the channel takes at startup; raised, never
// lowered, with every saved record and live-start boundary". Store, full
// mode: "Everything acknowledged survives a crash".
//
// With retained.max_bytes retainedLimit, a client publishes a message at
// QoS 1, stored at offset 1, then a retained one larger than the limit,
// refused with PUBACK 0x97, which leaves offset 2 unused. A durable session
// then subscribes to their topics, its SUBACK confirming its start stored
// at 3, and the broker is killed, nothing published since. Restarted on
// the same file, the subscriber resumes its session and a new message is
// published: it is stored at offset 3, never at the refused 2 below the
// saved start, and the subscriber receives it.
func TestNumberingAfterACrashPassesASavedStart(t *testing.T) {
	o := &harness.Options{Dir: t.TempDir(), RetainedMaxBytes: retainedLimit}
	b := support.Launch(t, o)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "crash/a", Payload: []byte("stored")})
	answered(t, pub, retain("crash/b", bytes.Repeat([]byte{'v'}, retainedLimit+1)), 0x97)
	support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "saved", CleanStart: true, Props: support.Durable}, support.At(1, "crash/#"))
	support.Crash(t, b)

	again := support.Launch(t, o)
	sub, ack := harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: "saved", Props: support.Durable})
	if !ack.SessionPresent {
		t.Fatal("after the crash, CONNACK with Clean Start 0 has Session Present 0: the durable session is gone")
	}
	pub, _ = harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "crash/a", Payload: []byte("after")})
	support.DeliveredAt(t, sub, 1, "crash/a", []byte("after"))
	stopped(t, again)

	db := support.OpenDB(t, again.DB)
	if starts := storedSubscriptions(t, db, "saved").starts; !slices.Equal(starts, []startRow{{"crash/#", 1, 3}}) {
		t.Errorf("subscription_starts holds %+v; want crash/# on $broadcast (1) at 3, past the refused offset 2", starts)
	}
	stored := records(t, again.DB)
	offsets := make([]int64, 0, len(stored))
	for _, r := range stored {
		offsets = append(offsets, r.offset)
	}
	if !slices.Equal(offsets, []int64{1, 3}) {
		t.Errorf("records holds offsets %v; want 1 and 3: the refused 2 is never used, and the publication after the restart is not numbered below the saved start", offsets)
	}
}

// design.md, Flows, Replies: "PUBACK [...] sent only once the store
// confirms"; Flows, PUBLISH: "1 | on PUBLISH | PUBACK once stored".
//
// The test holds the database's write lock, as another program on the file
// may, while an MQTT 5 client publishes at QoS 1; the broker runs under
// strace, which notes its locks on the -shm file. Once its store writer
// has met the lock, no PUBACK comes for heldPast; once the lock is
// released, PUBACK 0x00 does.
func TestNoPubackBeforeTheSave(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "waits", CleanStart: true})

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	p := &harness.Publish{QoS: 1, Topic: "saved/first", Payload: []byte("x")}
	support.Sent(t, c, p)
	awaitLockMet(t, b.trace, before, "the PUBLISH")
	nothingWithin(t, c, heldPast, "with the write lock held after the PUBLISH")
	lock.release(t)
	support.Puback(t, c, p)
}

// nothingWithin fails the test unless c reads nothing for d and its
// connection stays open: what a test waits for is that nothing comes, so
// the wait's length is the behaviour it checks. what says when.
func nothingWithin(t *testing.T, c *harness.Client, d time.Duration, what string) {
	t.Helper()
	saved := c.Timeout
	c.Timeout = d
	p, err := c.Recv()
	c.Timeout = saved
	switch {
	case err == nil:
		t.Fatalf("%s, within %v the client read %s %+v; want nothing, its connection open", what, d, harness.Name(p.Type()), p)
	case !harness.IsTimeout(err):
		t.Fatalf("%s, within %v the client's read ended: %v; want nothing, its connection open", what, d, err)
	}
}

// commitInterval is the broker.storage.publish_commit_interval of the tests
// of the core's collection: long enough beside a commit's own time that an
// acknowledgement before it ends comes only from a group sent early.
const commitInterval = 500 * time.Millisecond

// unreachedRecords is a broker.storage.publish_commit_max_records no test
// beside commitInterval reaches, within config.md's range, 1 to
// limits.max_connections.
const unreachedRecords = 1000

// design.md, Store, Writes: "With a configured interval, every kind of
// eligible write joins the collecting group. Its first publication fixes
// the core's collection deadline (R1). The group is due at that deadline";
// design.md, Flows, Replies: PUBACK "sent only once the store confirms";
// config.md, publish_commit_interval: "the core's collection period".
//
// With publish_commit_interval commitInterval and a record limit not
// reached, an MQTT 5 client publishes at QoS 1, then again half the
// interval later. The first's PUBACK, 0x00, comes no sooner than the
// interval after the first was sent: the core took it after that, and its
// deadline runs from then. The second's comes with it, sooner than the
// interval after the second was sent: it joined the group the first
// publication's deadline sends, fixing no deadline of its own.
func TestPublishCommitInterval(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "grouped", CleanStart: true})
	first := &harness.Publish{QoS: 1, Topic: "grouped", Payload: []byte("first")}
	second := &harness.Publish{QoS: 1, Topic: "grouped", Payload: []byte("second")}
	firstSent := support.Sent(t, c, first)
	time.Sleep(commitInterval / 2)
	secondSent := support.Sent(t, c, second)
	if elapsed := support.Puback(t, c, first).Sub(firstSent); elapsed < commitInterval {
		t.Errorf("the first PUBACK %v after its PUBLISH; want none before publish_commit_interval, %v", elapsed, commitInterval)
	}
	if elapsed := support.Puback(t, c, second).Sub(secondSent); elapsed >= commitInterval {
		t.Errorf("the second PUBACK %v after its PUBLISH, sent %v into the first's interval; want it with the first's, "+
			"before publish_commit_interval, %v, had passed since its own", elapsed, commitInterval/2, commitInterval)
	}
}

// design.md, Store, Writes: "With a configured interval, every kind of
// eligible write joins the collecting group. Its first publication fixes
// the core's collection deadline (R1). The group is due at that deadline";
// design.md, Flows, Replies: PUBACK "sent only once the store confirms".
// Audit 261022-002a: a collection deadline that, once due, was never
// cleared left every later group waiting without one.
//
// With publish_commit_interval commitInterval and a record limit not
// reached, an MQTT 5 client publishes at QoS 1 and waits for its PUBACK,
// 0x00; then it publishes again. The second publication is the first of
// a new group, and fixes its deadline: its PUBACK, 0x00, comes no sooner
// than the interval after it was sent, and within late after that.
func TestEachGroupFixesItsOwnDeadline(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "regrouped", CleanStart: true})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "regrouped", Payload: []byte("first")})
	second := &harness.Publish{QoS: 1, Topic: "regrouped", Payload: []byte("second")}
	secondSent := support.Sent(t, c, second)
	c.Timeout = commitInterval + support.Late
	if elapsed := support.Puback(t, c, second).Sub(secondSent); elapsed < commitInterval {
		t.Errorf("the second PUBACK %v after its PUBLISH; want none before publish_commit_interval, %v, the deadline it fixed", elapsed, commitInterval)
	}
}

// design.md, Store, Writes: "The group is due at that deadline, or at the
// end of the event in which a write that is not a publication joins it";
// "A non-publication write adds no wait for the collection deadline";
// design.md, Flows, Replies: "CONNACK (when it changes a session), PUBACK
// [...] are sent only once the store confirms".
//
// With publish_commit_interval commitInterval and a record limit not
// reached, an MQTT 5 client publishes at QoS 1, then pings: the PINGRESP,
// read first, says the core has taken the PUBLISH (design.md, Execution,
// Decisions: "Each connection's packet order is preserved"), whose PUBACK
// waits in the collecting group. A second client then connects, asking for
// a durable session, whose save joins that group, and sends it at once. Its
// CONNACK, 0x00, comes sooner than the interval after it began connecting,
// and the waiting PUBACK, 0x00, sooner than the interval after the PUBLISH
// was sent.
func TestNonPublicationWriteAddsNoWait(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "waits", Payload: []byte("x")}
	published := support.Sent(t, c, p)
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the PUBLISH: %v; want the PINGRESP first, the PUBACK waiting for the store", err)
	}
	connecting := time.Now()
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "durable", CleanStart: true, Props: support.Durable})
	if elapsed := time.Since(connecting); elapsed >= commitInterval {
		t.Errorf("a durable session's CONNACK %v after it began connecting; want it before publish_commit_interval, %v: its save adds no wait", elapsed, commitInterval)
	}
	if elapsed := support.Puback(t, c, p).Sub(published); elapsed >= commitInterval {
		t.Errorf("the PUBACK %v after its PUBLISH; want it with the durable session's save, before publish_commit_interval, %v", elapsed, commitInterval)
	}
}

// design.md, Execution, Capacities: "A publication batch holds
// publish_commit_max_records records or 4 MiB of message bytes. - A write
// group holds one batch bound of publications"; Store, Writes: "Reaching a
// group's limit seals it at once"; config.md, broker.storage:
// publish_commit_max_records, "the record limit per publication batch";
// design.md, Flows, Replies: PUBACK "sent only once the store confirms".
//
// With publish_commit_interval commitInterval and publish_commit_max_records
// 2, an MQTT 5 client publishes twice at QoS 1, one PUBLISH after the
// other: the second takes the collecting group to its limit, one batch of
// two records, which sends it at once. Both PUBACKs, 0x00, come sooner
// than the interval after the first PUBLISH was sent.
func TestRecordLimitSendsTheGroupAtOnce(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: 2})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "limited", CleanStart: true})
	first := &harness.Publish{QoS: 1, Topic: "limited", Payload: []byte("first")}
	second := &harness.Publish{QoS: 1, Topic: "limited", Payload: []byte("second")}
	published := support.Sent(t, c, first)
	support.Sent(t, c, second)
	for i, p := range []*harness.Publish{first, second} {
		if elapsed := support.Puback(t, c, p).Sub(published); elapsed >= commitInterval {
			t.Errorf("PUBACK %d %v after the first PUBLISH; want both before publish_commit_interval, %v: the record limit sends the group at once",
				i+1, elapsed, commitInterval)
		}
	}
}

// batchBytes is the design's byte limit on a publication batch (design.md,
// Execution, Capacities: "A publication batch holds
// publish_commit_max_records records or 4 MiB of message bytes").
const batchBytes = 4 << 20

// design.md, Execution, Capacities: "A publication batch holds
// publish_commit_max_records records or 4 MiB of message bytes. - A write
// group holds one batch bound of publications"; Store, Writes: "A batch
// closes before either of its limits would be exceeded (Capacities); one
// otherwise valid publication larger than the byte limit forms a batch alone,
// fully charged. Reaching a group's limit seals it at once, before a write
// that would take it past"; "The limits hold with or without an interval";
// "Its first publication fixes the core's collection deadline"; Flows,
// Replies: PUBACK "sent only once the store confirms".
//
// With publish_commit_interval commitInterval, a record limit not reached
// and max_packet_size 6 MiB, an MQTT 5 client publishes at QoS 1 two
// messages of 2.5 MiB each, one PUBLISH after the other: the second would
// take the batch past 4 MiB, so the first's batch closes, and with it its
// group, at its limit, which goes at once: its PUBACK, 0x00, comes sooner
// than the interval after it was sent; the second starts a group of its
// own, whose deadline it fixes: its PUBACK comes no sooner than the
// interval after it was sent. Then it publishes one message over 4 MiB, and
// a small one after it: the large one forms a batch alone, its group at its
// limit and sent at once, its PUBACK sooner than the interval; the small
// one's, again, no sooner. Each size is past the limit, or within it,
// however a PUBLISH's message bytes are counted: whole body or payload.
func TestByteLimitClosesTheBatch(t *testing.T) {
	const half, over = batchBytes * 5 / 8, batchBytes + 1024
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords,
		MaxPacketSize: 6 << 20})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "bytes", CleanStart: true})
	c.Timeout = commitInterval + support.Late
	for _, pair := range []struct {
		what         string
		first, after int
	}{
		{"two of 2.5 MiB, together past 4 MiB", half, half},
		{"one over 4 MiB, then a small one", over, 1},
	} {
		first := &harness.Publish{QoS: 1, Topic: "bytes", Payload: make([]byte, pair.first)}
		after := &harness.Publish{QoS: 1, Topic: "bytes", Payload: make([]byte, pair.after)}
		firstSent := support.Sent(t, c, first)
		afterSent := support.Sent(t, c, after)
		if elapsed := support.Puback(t, c, first).Sub(firstSent); elapsed >= commitInterval {
			t.Errorf("%s: the first PUBACK %v after its PUBLISH; want it before publish_commit_interval, %v: "+
				"its batch closed at the byte limit, and its group with it", pair.what, elapsed, commitInterval)
		}
		if elapsed := support.Puback(t, c, after).Sub(afterSent); elapsed < commitInterval {
			t.Errorf("%s: the second PUBACK %v after its PUBLISH; want none before publish_commit_interval, %v: "+
				"it starts a batch and a group of its own", pair.what, elapsed, commitInterval)
		}
	}
}

// design.md, Store, Writes: "A write is never split. A channel's next
// publication starts another batch, which waits its turn. Replies for stored
// changes keep their packet order across batches"; config.md, broker.storage:
// publish_commit_max_records, "1 to limits.max_connections, with or without
// an interval"; design.md, Flows, Replies: "Acknowledgements of stored
// changes preserve their packet order"; Execution, Decisions: "Each
// connection's packet order is preserved".
//
// With publish_commit_max_records 1 and no interval, every publication a
// batch of its own, two MQTT 5 clients each send 20 PUBLISHes at QoS 1,
// the Receive Maximum the broker grants by default, each followed by one at
// QoS 0, without waiting; their batches pass the bound on those
// unresolved (design.md, Capacities: "4 per channel") many times over.
// Each client then reads its 20 PUBACKs, every one 0x00 and nothing else
// between them, in the order it sent the PUBLISHes.
func TestPubacksKeepPacketOrderAcrossBatches(t *testing.T) {
	const burst = 20
	b := support.Launch(t, &harness.Options{PublishCommitMaxRecords: 1})
	clients := make([]*harness.Client, 2)
	ids := make([][]uint16, len(clients))
	for i := range clients {
		clients[i], _ = harness.Connected(t, b.Addr, harness.V5,
			&harness.Connect{ClientID: "ordered" + strconv.Itoa(i), CleanStart: true})
	}
	for range burst {
		for i, c := range clients {
			p := &harness.Publish{QoS: 1, Topic: "ordered", Payload: []byte{byte(i)}}
			support.Sent(t, c, p)
			ids[i] = append(ids[i], p.ID)
			if err := c.Send(&harness.Publish{Topic: "ordered", Payload: []byte{byte(i)}}); err != nil {
				t.Fatalf("PUBLISH at QoS 0: %v", err)
			}
		}
	}
	for i, c := range clients {
		for n, id := range ids[i] {
			p, err := c.Recv()
			if err != nil {
				t.Fatalf("client %d, waiting for its reply %d, PUBACK %d: %v", i, n+1, id, err)
			}
			if ack, ok := p.(*harness.Ack); !ok || ack.Kind != harness.PUBACK || ack.ID != id || ack.Reason != 0 {
				t.Fatalf("client %d's reply %d is %s %+v; want PUBACK %d, 0x00: the PUBACKs in the order of the PUBLISHes",
					i, n+1, harness.Name(p.Type()), p, id)
			}
		}
	}
}

// design.md, Words, Offset: "assigned by the core when the record's write
// is accepted (Store, Writes), in the channel's order"; Store, Writes: "A
// write awaiting its owner's earlier request or another required
// predecessor stays pending"; "A write is accepted when it joins a group;
// until then it is pending"; "a channel's next publication starts another
// batch, which waits its turn". Channels, $broadcast, Retained: with
// retained.max_bytes set, a replacement is admitted against the topics'
// committed charges. Execution, Decisions: "Each connection's packet order
// is preserved". The database is read directly, as the operator reads it.
//
// On a fresh broker with publish_commit_interval commitInterval, a record
// limit not reached and retained.max_bytes set, an MQTT 5 client sends six
// PUBLISHes at QoS 1 back to back, two of them retained, well within the
// interval, then reads their PUBACKs, each 0x00, the first no sooner than
// the interval after the first was sent: every one was taken while the
// first group still collected, so the first two share it, and the first
// retained one and those after it wait, pending, for that group and for
// its admission read. Once the broker has stopped, $broadcast's records
// hold the six at offsets 1 to 6, in the order they were sent.
func TestOffsetsFollowPublishOrder(t *testing.T) {
	b := support.Launch(t, &harness.Options{PublishCommitInterval: commitInterval, PublishCommitMaxRecords: unreachedRecords, RetainedMaxBytes: 1 << 20})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "numbered", CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "numbered/a", Payload: []byte("1")},
		{QoS: 1, Topic: "numbered/a", Payload: []byte("2")},
		{QoS: 1, Retain: true, Topic: "numbered/b", Payload: []byte("3")},
		{QoS: 1, Topic: "numbered/a", Payload: []byte("4")},
		{QoS: 1, Retain: true, Topic: "numbered/c", Payload: []byte("5")},
		{QoS: 1, Topic: "numbered/a", Payload: []byte("6")},
	}
	first := support.Sent(t, c, ps[0])
	for _, p := range ps[1:] {
		support.Sent(t, c, p)
	}
	if took := time.Since(first); took >= commitInterval/2 {
		t.Fatalf("sending the six PUBLISHes took %v; want them well within publish_commit_interval, %v", took, commitInterval)
	}
	if acked := support.Puback(t, c, ps[0]).Sub(first); acked < commitInterval {
		t.Fatalf("the first PUBACK came %v after its PUBLISH; want none before publish_commit_interval, %v", acked, commitInterval)
	}
	for _, p := range ps[1:] {
		support.Puback(t, c, p)
	}
	stopped(t, b)

	rs := records(t, b.DB)
	if len(rs) != len(ps) {
		t.Fatalf("records holds %d rows; want the %d publications", len(rs), len(ps))
	}
	for i, r := range rs {
		if r.channel != 1 || r.offset != int64(i+1) || !bytes.Equal(r.payload, ps[i].Payload) {
			t.Errorf("row %d: channel %d, offset %d, payload %q; want $broadcast (1), offset %d, payload %q: offsets from the tail, in the order sent",
				i+1, r.channel, r.offset, r.payload, i+1, ps[i].Payload)
		}
	}
}

// design.md, Words, Offset: "assigned by the core when a publication
// enters the channel ([PUBLISH]), in the order it enters"; "A publication
// refused after entering leaves its offset unused: an offset gap is not a
// count of lost records"; Flows, PUBLISH: QoS 1 "Enters the channel | on
// PUBLISH"; Channels, $broadcast, Retained: "Retained values have their own
// size limit. A value that does not fit is refused with 0x97". The database
// is read directly, as the operator reads it.
//
// With retained.max_bytes retainedLimit, an MQTT 5 client publishes at QoS
// 1 a message, answered 0x00; a retained one larger than the limit,
// answered 0x97; and two more, answered 0x00. Once the broker has stopped,
// $broadcast's records hold the three it stored at offsets 1, 3 and 4, in
// the order they were sent: the refused one's offset, 2, is missing, and
// the later ones keep the numbers they took on entry.
func TestRefusedPublicationLeavesAGap(t *testing.T) {
	b := support.Launch(t, &harness.Options{RetainedMaxBytes: retainedLimit})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "gapped", CleanStart: true})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "gap/a", Payload: []byte("1")})
	answered(t, c, retain("gap/b", bytes.Repeat([]byte{'v'}, retainedLimit+1)), 0x97)
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "gap/a", Payload: []byte("3")})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "gap/a", Payload: []byte("4")})
	stopped(t, b)

	rs := records(t, b.DB)
	want := []int64{1, 3, 4}
	if len(rs) != len(want) {
		t.Fatalf("records holds %d rows: %+v; want the %d publications stored", len(rs), rs, len(want))
	}
	for i, r := range rs {
		if r.channel != 1 || r.offset != want[i] || string(r.payload) != strconv.FormatInt(want[i], 10) {
			t.Errorf("row %d: channel %d, offset %d, payload %q; want $broadcast (1), offset %d, payload %q: the refused offset 2 a gap, no survivor renumbered",
				i+1, r.channel, r.offset, r.payload, want[i], strconv.FormatInt(want[i], 10))
		}
	}
}

// design.md, Store, Writes: "A Store-read barrier or a stop also seals it at
// once"; Store reads: "Every other read used for a broker decision has a
// place in the core's store-request order. Earlier accepted writes are resolved before its
// view is fixed"; Flows, CONNECT, 4: "Load. Unless the id is connected, the
// core reads its session from the store", and 7: nothing is stored where
// nothing changed; design.md, Flows, Replies: PUBACK "sent only once the
// store confirms"; config.md, publish_commit_interval: "at most 1s".
//
// With publish_commit_interval at its most, 1 s, and a record limit not
// reached, an MQTT 5 client publishes at QoS 1, then pings: the PINGRESP,
// read first, says the core has taken the PUBLISH (design.md, Execution,
// Decisions: "Each connection's packet order is preserved"), whose PUBACK
// waits in the collecting group. A second client then connects under an id
// not connected, with Clean Start 1 and no session expiry: its session is
// read, and nothing is written for it. Its read seals the collecting group
// at once, and its view needs that group committed: its CONNACK is 0x00,
// and the PUBACK, 0x00, comes sooner than the interval after the PUBLISH
// was sent.
func TestReadBarrierSendsTheGroupAtOnce(t *testing.T) {
	const most = time.Second
	b := support.Launch(t, &harness.Options{PublishCommitInterval: most, PublishCommitMaxRecords: unreachedRecords})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "barrier", Payload: []byte("x")}
	published := support.Sent(t, c, p)
	if err := c.Ping(); err != nil {
		t.Fatalf("a PINGREQ after the PUBLISH: %v; want the PINGRESP first, the PUBACK waiting for the store", err)
	}
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "reader", CleanStart: true})
	if elapsed := support.Puback(t, c, p).Sub(published); elapsed >= most {
		t.Errorf("the PUBACK %v after its PUBLISH; want it at the other client's CONNECT, whose read seals the group, "+
			"before publish_commit_interval, %v", elapsed, most)
	}
}

// burstPayload is the payload of each message of TestLogShrinksBackAfterABurst's
// burst: within config.md's default max_packet_size (1MiB), and some 250 of
// SQLite's 4 KiB pages, so one commit of it carries the log from under
// SQLite's fold point, 1,000 pages, to past journal_size_limit's 4 MiB. Its
// bytes are noise, which compression cannot make shorter, so it is stored
// at that size (storage.md, A stored payload).
const burstPayload = 1_000_000

// burstLimit bounds the burst: publishes enough to cross the fold point
// several times over.
const burstLimit = 32

// walLimit is the log's size limit, journal_size_limit.
const walLimit = 4 << 20

// design.md, Store: "It runs in WAL mode [...] SQLite manages its own log:
// it folds it into the database by its own default rule, and Sagüin only
// sets journal_size_limit to 4 MiB at open, so the log shrinks back after
// a burst; Sagüin never folds it itself." SQLite's default rule folds the
// log after a commit that takes it past 1,000 pages (wal_autocheckpoint);
// journal_size_limit cuts its file back to 4 MiB when the next commit
// starts it again.
//
// On a new database, and on the same file reopened, an MQTT 5 client
// publishes messages of burstPayload random bytes at QoS 1, one at a time, until
// the -wal file beside the database holds more than 4 MiB once a PUBACK
// has come (the commit, and the fold after it, done). Then one small
// publish: once its PUBACK has come, the -wal file holds 4 MiB at most.
func TestLogShrinksBackAfterABurst(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, run := range []string{"a new database", "the database reopened"} {
		t.Run(run, func(t *testing.T) {
			b := support.Launch(t, &harness.Options{Dir: dir})
			wal := b.DB + "-wal"
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "burst", CleanStart: true})
			payload := support.Noise(burstPayload)
			support.Unshrunk(t, payload)
			burst := false
			for range burstLimit {
				support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "burst", Payload: payload})
				if burst = testkit.FileSize(t, wal) > walLimit; burst {
					break
				}
			}
			if !burst {
				t.Fatalf("after %d publishes of %d bytes, the log's file holds %d bytes, never past 4 MiB: no burst to shrink back from",
					burstLimit, burstPayload, testkit.FileSize(t, wal))
			}
			support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "burst", Payload: []byte("x")})
			if got := testkit.FileSize(t, wal); got > walLimit {
				t.Errorf("after the publish following the burst, the log's file holds %d bytes, past journal_size_limit's 4 MiB", got)
			}
			stopped(t, b)
		})
	}
}

// design.md, R8: "Recoverable full responses, fatal errors and recovery are
// defined in Store"; Store, A full disk: "a write is answered "full" only
// when SQLite reports lack of space and rollback confirms that the
// transaction did not commit. The broker continues after that answer"; "Group
// refusal follows Writes. Publishes are refused with 0x97"; Flows, PUBLISH:
// 0x97 for "a full disk", "A QoS 2 refusal rides the PUBREC. At QoS 0 a
// refused publish is dropped [...] At QoS 1 or 2 an MQTT 3.1.1 client has no
// refusal code and is disconnected"; the table: QoS 2 enters "from a held
// copy stored [...] before PUBREC"; MQTT 5 4.13.2: a PUBACK of 0x80 or above
// has "no consequences for other Sessions or other Packets flowing on the
// same Session".
//
// On a filesystem of its own, a broker holds an MQTT 5 and an MQTT 3.1.1
// client, connected; a ballast then takes every byte left. The MQTT 5
// client's QoS 1 PUBLISH gets PUBACK 0x97 and its PINGREQ is answered; its
// QoS 2 PUBLISH, whose held copy finds no room, gets PUBREC 0x97 and its
// PINGREQ is answered; its QoS 0 PUBLISH is answered by nothing, its
// PINGREQ's PINGRESP coming next; the MQTT 3.1.1 client's QoS 1 PUBLISH
// closes its connection.
func TestFullDiskRefusesPublishes(t *testing.T) {
	disk := os.Getenv(support.SmallDisk)
	if disk == "" {
		testkit.OnSmallDisk(t, support.SmallDisk, support.DiskSize, support.AloneLimit)
		return
	}
	b := support.Launch(t, &harness.Options{FilePath: filepath.Join(disk, "saguin.db")})
	v5, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "full-5", CleanStart: true})
	v311, _ := harness.Connected(t, b.Addr, harness.V311, &harness.Connect{ClientID: "full-3.1.1", CleanStart: true})
	testkit.FillDisk(t, filepath.Join(disk, "ballast"))

	ack, err := v5.Publish(&harness.Publish{QoS: 1, Topic: "full", Payload: []byte("1")})
	if err != nil {
		t.Fatalf("MQTT 5, QoS 1, on a full disk: %v", err)
	}
	if ack.Reason != 0x97 {
		t.Errorf("MQTT 5, QoS 1, on a full disk: PUBACK 0x%02X, want 0x97", ack.Reason)
	}
	if err := v5.Ping(); err != nil {
		t.Fatalf("after a QoS 1 PUBLISH refused for a full disk: %v", err)
	}
	held := &harness.Publish{QoS: 2, Topic: "full", Payload: []byte("2")}
	support.Sent(t, v5, held)
	pubrecOf(t, v5, held, 0x97)
	if err := v5.Ping(); err != nil {
		t.Fatalf("after a QoS 2 PUBLISH refused for a full disk: %v", err)
	}
	if _, err := v5.Publish(&harness.Publish{Topic: "full", Payload: []byte("0")}); err != nil {
		t.Fatal(err)
	}
	if err := v5.Ping(); err != nil {
		t.Fatalf("after a QoS 0 PUBLISH on a full disk: %v; want the PINGRESP next, the publish unanswered", err)
	}
	if err := v311.Send(&harness.Publish{QoS: 1, ID: v311.NextID(), Topic: "full", Payload: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, v311)
}

// routedChannels are append channels whose filters overlap, one with a
// {a,b} level written first and one with a {a,b} level inside:
// TestPublicationLandsInTheMostExactChannel publishes into them, and
// TestRouteAnswersAsTheBrokerRoutes asks saguin --route about them.
var routedChannels = []harness.Channel{
	{Name: "fleet", Type: "append", Filter: "fleet/#"},
	{Name: "devices", Type: "append", Filter: "fleet/+"},
	{Name: "temps", Type: "append", Filter: "fleet/+/temp"},
	{Name: "north", Type: "append", Filter: "fleet/north/#"},
	{Name: "northtemp", Type: "append", Filter: "fleet/north/temp"},
	{Name: "northroot", Type: "append", Filter: "fleet/north"},
	{Name: "sides", Type: "append", Filter: "fleet/{east,west}/#"},
	{Name: "books", Type: "append", Filter: "{orders,invoices}/#"},
}

// landings are topics and the channel of routedChannels each lands in,
// "$broadcast" where none claims it, by design.md, Words: Channel, "Where
// filters overlap, the topic goes to the filter that spells it out most
// exactly. Filters are compared level by level. The first level where they
// differ decides. A written level beats + and #, and + beats #. A filter
// ending there beats one going on with #. A {a,b} level is expanded first,
// with each alternative treated as a written level"; Channels, $broadcast,
// "the reserved append channel for every topic no configured channel claims";
// MQTT 5 4.7.1.2: "sport/#" also matches "sport", and 4.7.1.3: "sport/+" does
// not match "sport". why says what decides.
var landings = []struct{ topic, channel, why string }{
	{"fleet/x/y/z", "fleet", "fleet/# alone matches"},
	{"fleet", "fleet", "fleet/# matches its parent level; fleet/+ does not"},
	{"fleet/x", "devices", "+ beats # at the second level"},
	{"fleet/south/temp", "temps", "+ beats # at the second level"},
	{"fleet/north/speed", "north", "a written level beats + and #"},
	{"fleet/north/temp", "northtemp", "written beats + at the second level, then written beats # at the third"},
	{"fleet/north", "northroot", "a filter ending there beats one going on with #"},
	{"fleet/east/temp", "sides", "an {a,b} alternative is a written level, beating +"},
	{"fleet/west", "sides", "an {a,b} alternative is a written level, beating + and #"},
	{"orders", "books", "a first level written as {a,b}"},
	{"invoices/2026/7", "books", "a first level written as {a,b}"},
	{"Fleet/north", "$broadcast", "levels compare case-sensitively"},
	{"fleets/north", "$broadcast", "a level is matched whole"},
	{"weather/today", "$broadcast", "no channel claims it"},
}

// design.md, Words: Channel, "a named, typed log holding the topics its
// MQTT topic filter matches", and the rule landings follows; Channels,
// $broadcast, "the reserved append channel for every topic no configured
// channel claims"; Flows, PUBLISH: "0 | on PUBLISH | never" and "1 | on
// PUBLISH | PUBACK once stored"; Words: Offset, "a channel's counter [...] in
// entry order"; Flows, PUBLISH: "Message Expiry [...] use[s] receipt time";
// Delivery, Message Expiry: "On a configured channel it is still delivered
// [...] only retention removes a record", so the record keeps it; storage.md,
// records: channel "the channel's id", expires "when its Message Expiry runs
// out, counted from its receipt", published_retained and retained "0
// elsewhere" than $broadcast; channels: name "the channel's name [...] or
// $broadcast".
//
// With routedChannels configured, an MQTT 5 client publishes each topic of
// landings at QoS 1, its payload the topic, each answered PUBACK 0x00;
// then fleet/south/temp at QoS 0, answered by nothing, its PINGREQ's
// PINGRESP coming next; then fleet/north/temp at QoS 1 with a Message
// Expiry Interval of 600 seconds. After the stop, the database holds each
// in the channel landings names, by that channel's name, the QoS 0 one in
// temps at QoS 0, each channel's offsets rising in publish order, and the
// expiring one in northtemp expiring 600 seconds after it was received:
// no sooner than 600 seconds after it was sent, no later than 600 seconds
// after its PUBACK was read.
func TestPublicationLandsInTheMostExactChannel(t *testing.T) {
	const lasting = 600
	b := support.Launch(t, &harness.Options{Channels: routedChannels})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "router", CleanStart: true})
	order := make(map[string]int) // a payload's place in the publish order
	want := make(map[string]string)
	for i, l := range landings {
		support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: l.topic, Payload: []byte(l.topic)})
		order[l.topic], want[l.topic] = i, l.channel
	}
	if _, err := c.Publish(&harness.Publish{Topic: "fleet/south/temp", Payload: []byte("qos 0")}); err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("after a QoS 0 PUBLISH: %v; want the PINGRESP next, the publish unanswered", err)
	}
	order["qos 0"], want["qos 0"] = len(order), "temps"
	expiring := &harness.Publish{QoS: 1, Topic: "fleet/north/temp", Props: expiry(lasting), Payload: []byte("expiring")}
	from := support.Sent(t, c, expiring)
	to := support.Puback(t, c, expiring)
	order["expiring"], want["expiring"] = len(order), "northtemp"
	stopped(t, b)

	got := records(t, b.DB)
	if len(got) != len(want) {
		t.Fatalf("the database holds %d records, want the %d published: %+v", len(got), len(want), got)
	}
	last := make(map[string]int) // each channel's latest place in the publish order, read by offset
	for _, r := range got {
		payload := string(r.payload)
		channel, ok := want[payload]
		switch {
		case !ok:
			t.Errorf("a record of topic %q, payload %q, that was never published", r.topic, payload)
			continue
		case r.name != channel:
			t.Errorf("%q landed in %q; want %q", r.topic, r.name, channel)
		case r.publishedRetained != 0 || r.retained != 0:
			t.Errorf("%q is stored with published_retained %d, retained %d; want 0 and 0, published without RETAIN", r.topic, r.publishedRetained, r.retained)
		}
		if prev, seen := last[r.name]; seen && order[payload] < prev {
			t.Errorf("in %q, %q is at offset %d, after a record published later; want offsets in publish order", r.name, payload, r.offset)
		}
		last[r.name] = order[payload]
		switch payload {
		case "qos 0":
			if r.qos != 0 {
				t.Errorf("the QoS 0 publication is stored at QoS %d", r.qos)
			}
		case "expiring":
			low, high := from.UnixMilli()+lasting*1000, to.UnixMilli()+lasting*1000
			if r.expires == nil || *r.expires < low || *r.expires > high {
				t.Errorf("the publication expiring in %d seconds is stored with expires %v; want %d to %d, kept from its receipt", lasting, r.expires, low, high)
			}
		default:
			if r.qos != 1 || r.expires != nil {
				t.Errorf("%q is stored at QoS %d, expires %v; want QoS 1 and no expiry", r.topic, r.qos, r.expires)
			}
		}
	}
}

// storedChannel is one row of storage.md's channels, as the operator reads
// it from the database.
type storedChannel struct{ id, typ, bytes int64 }

// storedChannels reads every row of storage.md's channels in the database
// at path, by name.
func storedChannels(t *testing.T, path string) map[string]storedChannel {
	t.Helper()
	cs := make(map[string]storedChannel)
	support.Query(t, support.OpenDB(t, path), `SELECT name, id, type, bytes FROM channels`, func(rows *sql.Rows) error {
		var name string
		var c storedChannel
		err := rows.Scan(&name, &c.id, &c.typ, &c.bytes)
		cs[name] = c
		return err
	})
	return cs
}

// storage.md, channels: "One row per stored log, configured channels,
// dead-letter channels and $broadcast alike", type "1 append"; Ids: "A
// channel [...] ha[s] an integer id, which the other tables use in place
// of its name. The core assigns them [...] from 1, each the highest loaded
// at start plus one, so an id is never reused while anything names it";
// "The $broadcast channel is channel 1". design.md, Restart, A changed
// configuration: "records stay in the channel where they landed"; Restart:
// "Restored: [...] each channel's offset counter"; Words: Channel, the rule
// landings follows.
//
// On one database, three brokers run in turn, an MQTT 5 client publishing
// at QoS 1 on each, every publish answered PUBACK 0x00, its payload its
// topic:
//   - alpha (a/#) and beta (b/#): a/1 and b/1;
//   - beta, alpha and a new gamma (g/#), reordered: a/2, b/2 and g/1;
//   - beta, gamma and alpha, its filter now c/#: a/3 and c/1.
//
// After each stop, $broadcast is channel 1 and every configured channel
// has a row of type 1; alpha and beta, of ids other than 1, keep their
// first ids throughout; gamma's is the highest id the first database held
// plus one. After the last, the records stay where they landed: a/1 and a/2
// in alpha, b/1 and b/2 in beta, g/1 in gamma; a/3, which alpha's filter no
// longer matches, in $broadcast, and c/1 in alpha after its records,
// alpha's offsets rising across the restarts.
func TestChannelsKeepTheirIdentities(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	alpha := harness.Channel{Name: "alpha", Type: "append", Filter: "a/#"}
	beta := harness.Channel{Name: "beta", Type: "append", Filter: "b/#"}
	gamma := harness.Channel{Name: "gamma", Type: "append", Filter: "g/#"}
	moved := harness.Channel{Name: "alpha", Type: "append", Filter: "c/#"}
	runs := []struct {
		channels []harness.Channel
		topics   []string
	}{
		{[]harness.Channel{alpha, beta}, []string{"a/1", "b/1"}},
		{[]harness.Channel{beta, alpha, gamma}, []string{"a/2", "b/2", "g/1"}},
		{[]harness.Channel{beta, gamma, moved}, []string{"a/3", "c/1"}},
	}
	ids := make([]map[string]int64, 0, len(runs)) // after each run, each channel's id by name
	var b *harness.Broker
	for i, run := range runs {
		b = support.Launch(t, &harness.Options{Dir: dir, Channels: run.channels})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "identities", CleanStart: true})
		for _, topic := range run.topics {
			support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: topic, Payload: []byte(topic)})
		}
		stopped(t, b)
		stored := storedChannels(t, b.DB)
		byName := make(map[string]int64)
		for name, s := range stored {
			byName[name] = s.id
		}
		if byName["$broadcast"] != 1 {
			t.Errorf("run %d: $broadcast is channel %d; want 1", i+1, byName["$broadcast"])
		}
		for _, ch := range run.channels {
			if s, ok := stored[ch.Name]; !ok || s.typ != 1 || s.id == 1 {
				t.Errorf("run %d: channels holds %q as %+v (present %v); want a row of type 1 (append), its own id", i+1, ch.Name, s, ok)
			}
		}
		ids = append(ids, byName)
	}

	if ids[0]["alpha"] == ids[0]["beta"] {
		t.Errorf("alpha and beta share the id %d", ids[0]["alpha"])
	}
	highest := slices.Max(slices.Collect(maps.Values(ids[0])))
	for i, byName := range ids[1:] {
		for _, name := range []string{"alpha", "beta"} {
			if byName[name] != ids[0][name] {
				t.Errorf("run %d: %q is channel %d; want %d, its id before", i+2, name, byName[name], ids[0][name])
			}
		}
		if byName["gamma"] != highest+1 {
			t.Errorf("run %d: gamma, new in run 2, is channel %d; want %d, the highest loaded plus one", i+2, byName["gamma"], highest+1)
		}
	}

	want := map[string]string{"a/1": "alpha", "a/2": "alpha", "b/1": "beta", "b/2": "beta", "g/1": "gamma", "a/3": "$broadcast", "c/1": "alpha"}
	got := records(t, b.DB)
	if len(got) != len(want) {
		t.Fatalf("the database holds %d records, want the %d published: %+v", len(got), len(want), got)
	}
	var alphas []string // alpha's records, by offset
	for _, r := range got {
		if r.name != want[string(r.payload)] {
			t.Errorf("%q is in %q; want %q, where it landed", r.payload, r.name, want[string(r.payload)])
		}
		if r.name == "alpha" {
			alphas = append(alphas, string(r.payload))
		}
	}
	if !slices.Equal(alphas, []string{"a/1", "a/2", "c/1"}) {
		t.Errorf("alpha holds %q by offset; want a/1, a/2 and c/1, its counter restored at each start", alphas)
	}
}

// storage.md, channels: bytes, "the bytes of its records"; A message's bytes:
// "A message's bytes are its topic's, payload's and properties' bytes, the
// payload counted at its size as received. Retention and the size limits
// count those bytes.", properties "NULL for none". config.md,
// channels.<name>: max_bytes (append), "This is the channel's hard byte
// limit"; design.md, Flows, PUBLISH: "0x97 | over [...] a channel's
// max_bytes"; Store, full mode: "Everything acknowledged survives a
// crash"; "A restart recovers committed state".
//
// A channel, kept, holds kept/# with max_bytes 100. An MQTT 5 client
// publishes on kept/t (6 bytes) a payload of 54 at QoS 1, answered 0x00:
// 60 bytes. The broker then ends, by a graceful stop or a crash, and the
// database holds 60 as kept's bytes. Started again on the same file, a
// payload of 35 (41 bytes, 101 with the 60) is refused, PUBACK 0x97, and
// one of 34 (40 bytes, 100) is taken, 0x00; after the stop, kept's bytes
// are 100.
func TestChannelBytesSurviveARestart(t *testing.T) {
	t.Parallel()
	ends := []struct {
		name string
		end  func(*testing.T, *harness.Broker)
	}{{"a graceful stop", stopped}, {"a crash", support.Crash}}
	for _, e := range ends {
		t.Run(e.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir(), Channels: []harness.Channel{{Name: "kept", Type: "append", Filter: "kept/#", MaxBytes: 100}}}
			b := support.Launch(t, o)
			c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "bytes", CleanStart: true})
			support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "kept/t", Payload: make([]byte, 54)})
			e.end(t, b)
			if got := storedChannels(t, b.DB)["kept"].bytes; got != 60 {
				t.Errorf("after %s, kept's bytes are %d; want 60, its record's topic and payload", e.name, got)
			}

			again := support.Launch(t, o)
			c, _ = harness.Connected(t, again.Addr, harness.V5, &harness.Connect{ClientID: "bytes", CleanStart: true})
			answered(t, c, &harness.Publish{QoS: 1, Topic: "kept/t", Payload: make([]byte, 35)}, 0x97)
			answered(t, c, &harness.Publish{QoS: 1, Topic: "kept/t", Payload: make([]byte, 34)}, 0x00)
			stopped(t, again)
			if got := storedChannels(t, again.DB)["kept"].bytes; got != 100 {
				t.Errorf("kept's bytes are %d; want 100, its limit, reached exactly", got)
			}
		})
	}
}
