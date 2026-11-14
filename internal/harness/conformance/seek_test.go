package conformance

import (
	"bytes"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of $saguin requests, their answers and the seek, from the wire
// (design.md, $saguin topics; Flows, Replies and Delivery). Every broker
// here holds the append channel events (seekChannel), which the seeks name;
// a test's records each have a payload of their own.

// seekChannel is the append channel the seeks name.
var seekChannel = harness.Channel{Name: "events", Type: "append", Filter: "events/#"}

// answerTopic is the Response Topic of the seeks that set one.
const answerTopic = "answers/seek"

// seekTopic is the seek request's topic for channel, and replyTopic the
// topic its answer goes on where it sets no Response Topic (design.md,
// $saguin topics, the table).
func seekTopic(channel string) string  { return "$saguin/consumer/" + channel + "/seek" }
func replyTopic(channel string) string { return seekTopic(channel) + "/reply" }

// requestOf is a $saguin request on topic at qos carrying payload, with
// Response Topic reply and Correlation Data correlation where each is set
// (an empty, non-nil correlation is sent as empty Correlation Data).
func requestOf(topic string, qos byte, payload, reply string, correlation []byte) *harness.Publish {
	p := &harness.Publish{QoS: qos, Topic: topic, Payload: []byte(payload)}
	if reply != "" {
		p.Props = append(p.Props, harness.Property{ID: harness.ResponseTopic, Str: reply})
	}
	if correlation != nil {
		p.Props = append(p.Props, harness.Property{ID: harness.CorrelationData, Data: correlation})
	}
	return p
}

// seekOf is a seek of the channel named, as requestOf makes it.
func seekOf(channel string, qos byte, payload, reply string, correlation []byte) *harness.Publish {
	return requestOf(seekTopic(channel), qos, payload, reply, correlation)
}

// answerOn waits for c's PUBLISH on topic, keeping what comes before it for
// Recv, and fails the test unless it is QoS 0 carrying correlation as its
// Correlation Data, none where correlation is nil: design.md, $saguin
// topics, "Its answer is a QoS 0 PUBLISH queued as a delivery. The answer
// goes only to the asking connection, on its Response Topic with its
// Correlation Data". It returns the answer.
func answerOn(t *testing.T, c *harness.Client, topic string, correlation []byte) *harness.Publish {
	t.Helper()
	p, err := c.Await(func(p harness.Packet) bool { a, ok := p.(*harness.Publish); return ok && a.Topic == topic })
	if err != nil {
		t.Fatalf("waiting for the answer on %q: %v", topic, err)
	}
	a := p.(*harness.Publish)
	cd, has := a.Props.Get(harness.CorrelationData)
	switch {
	case a.QoS != 0:
		t.Fatalf("the answer on %q came at QoS %d; want a QoS 0 PUBLISH", topic, a.QoS)
	case has != (correlation != nil) || !bytes.Equal(cd.Data, correlation):
		t.Fatalf("the answer on %q (%q) carries Correlation Data %q (present %v); want %q, the request's", topic, a.Payload, cd.Data, has, correlation)
	}
	return a
}

// sought sends from c, an MQTT 5 client, a QoS 1 seek of the channel named
// carrying payload, with answerTopic as its Response Topic and Correlation
// Data of its own, and fails the test unless its PUBACK carries reason and
// its answer arrives: design.md, $saguin topics, "Answer: the new offset; or
// no-session [...], malformed, below-floor [...] or beyond-end [...], each
// also 0x83 on the PUBACK". It returns the answer, whatever came before it
// kept for Recv.
func sought(t *testing.T, c *harness.Client, channel, payload string, reason byte) *harness.Publish {
	t.Helper()
	correlation := []byte("seek " + payload)
	return asked(t, c, seekOf(channel, 1, payload, answerTopic, correlation), reason, answerTopic, correlation)
}

// asked sends p, a QoS 1 request, from c, an MQTT 5 client, and fails the
// test unless its PUBACK carries reason and its answer arrives on topic
// carrying correlation (answerOn). It returns the answer, whatever came
// before it kept for Recv.
func asked(t *testing.T, c *harness.Client, p *harness.Publish, reason byte, topic string, correlation []byte) *harness.Publish {
	t.Helper()
	support.Sent(t, c, p)
	support.PubackOf(t, c, p, reason)
	return answerOn(t, c, topic, correlation)
}

// answers fails the test unless a, a seek's answer, has payload want. what
// names the seek.
func answers(t *testing.T, a *harness.Publish, want, what string) {
	t.Helper()
	if string(a.Payload) != want {
		t.Fatalf("the seek of %s answered %q; want %q", what, a.Payload, want)
	}
}

// answersOffset fails the test unless a, a seek's answer, is offset, "the
// new offset" in decimal. what names the seek.
func answersOffset(t *testing.T, a *harness.Publish, offset int64, what string) {
	t.Helper()
	answers(t, a, strconv.FormatInt(offset, 10), what)
}

// answerProperty fails the test unless a, a seek's refusing answer, carries
// the User Property name once, holding offset in decimal (design.md,
// $saguin topics: below-floor "User Property saguin-floor: the floor",
// beyond-end "saguin-next: the next offset").
func answerProperty(t *testing.T, a *harness.Publish, name string, offset int64) {
	t.Helper()
	if got := a.Props.User(name); !slices.Equal(got, []string{strconv.FormatInt(offset, 10)}) {
		t.Fatalf("the answer %q carries %s %q; want it once, %d", a.Payload, name, got, offset)
	}
}

// eventRecords reads the records of seekChannel in the database at path,
// in offset order.
func eventRecords(t *testing.T, path string) []record {
	t.Helper()
	return channelRecords(t, path, seekChannel.Name)
}

// tailOf reads the tail of the channel named from db: storage.md, channels,
// next, "the offset the next publication entering the channel takes";
// design.md, Words: Retention, "The tail is the next offset".
func tailOf(t *testing.T, db *sql.DB, channel string) int64 {
	t.Helper()
	var next int64
	if err := db.QueryRow(`SELECT next FROM channels WHERE name = ?`, channel).Scan(&next); err != nil {
		t.Fatalf("reading the tail of %s: %v", channel, err)
	}
	return next
}

// atOrAfter is the offset a time seek of ms, Unix milliseconds, selects
// among rs, the surviving records in offset order, or tail: design.md,
// $saguin topics, "A time selects the lowest surviving offset whose
// timestamp is at or after it, or the tail".
func atOrAfter(rs []record, ms, tail int64) int64 {
	if i := slices.IndexFunc(rs, func(r record) bool { return r.time >= ms }); i >= 0 {
		return rs[i].offset
	}
	return tail
}

// replayed reads from c the QoS 1 deliveries of rs's records from offset
// on, in offset order, first deliveries each, and acknowledges each.
func replayed(t *testing.T, c *harness.Client, rs []record, from int64) {
	t.Helper()
	for i := range rs {
		if rs[i].offset >= from {
			acknowledge(t, c, support.DeliveredAt(t, c, 1, rs[i].topic, rs[i].payload).ID, 0)
		}
	}
}

// eventsAt returns n QoS 1 publications on events/x, their payloads prefix
// and a number from 1.
func eventsAt(prefix string, n int) []*harness.Publish {
	ps := numbered("events/x", n)
	for _, p := range ps {
		p.Payload = append([]byte(prefix), p.Payload...)
	}
	return ps
}

// seekRig launches a broker holding seekChannel, with o's other keys, and
// stores ps in it, at QoS 1, from a publisher of its own, which it returns.
func seekRig(t *testing.T, o *harness.Options, ps []*harness.Publish) (*harness.Broker, *harness.Client) {
	t.Helper()
	own := *o // the caller's Options stay as they were
	own.Channels = append(slices.Clone(own.Channels), seekChannel)
	b := support.Launch(t, &own)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "seek-pub", CleanStart: true})
	support.PublishAll(t, pub, ps)
	return b, pub
}

// design.md, $saguin topics, the seek row: "moves this durable session's
// position on an append channel [...]. Payload: 0 (the floor), -1 (the next
// offset), an offset, a duration ago (12h) or an RFC 3339 time [...]
// Answer: the new offset"; "A duration is subtracted from the time of the
// core event that accepts and executes the request. A time selects the
// lowest surviving offset whose timestamp is at or after it, or the tail";
// Flows, Delivery, Positions: "A seek changes the position at once".
// Words: Retention, "The floor is the oldest record retention has not
// passed. The tail is the next offset".
//
// A durable MQTT 5 session subscribes at QoS 1 to events/#, receives the
// four records stored there and acknowledges them. It then seeks with each
// payload form in turn: each answer is the offset the form names, read
// from the records' offsets and timestamps (a duration ago lies between
// the request's sending and its answer, so its offset lies between those
// the two instants select), and the session then receives the records from
// that offset on, again, in order. After -1, the tail, it receives the
// record stored next and nothing before it.
func TestSeekPayloadFormsMoveThePosition(t *testing.T) {
	ps := eventsAt("forms ", 4)
	b, pub := seekRig(t, &harness.Options{}, ps)
	c := support.Subscribed(t, b.Addr, harness.V5, kept(harness.V5, "forms"), support.At(1, "events/#"))
	rs := eventRecords(t, b.DB)
	replayed(t, c, rs, rs[0].offset)
	taken(t, c, "the PUBACKs of the first deliveries")
	tail := tailOf(t, support.OpenDB(t, b.DB), seekChannel.Name)
	second := rs[1]

	for _, form := range []struct {
		name    string
		payload func() (string, time.Time) // the payload, and when it was made
		want    func(sent, answered time.Time) (lowest, highest int64)
	}{
		{"0, the floor", fixedForm("0"), exactAnswer(rs[0].offset)},
		{"an offset", fixedForm(strconv.FormatInt(rs[2].offset, 10)), exactAnswer(rs[2].offset)},
		{"an RFC 3339 time", fixedForm(time.UnixMilli(second.time).UTC().Format(time.RFC3339Nano)), exactAnswer(atOrAfter(rs, second.time, tail))},
		{"a duration ago", func() (string, time.Time) {
			now := time.Now()
			return fmt.Sprintf("%dms", now.Sub(time.UnixMilli(second.time)).Milliseconds()), now
		}, func(sent, answered time.Time) (int64, int64) {
			// The core event lies between sent and answered: the duration,
			// a whole number of milliseconds no longer than sent less the
			// second record's time, reaches back to that time or later.
			ago := sent.Sub(time.UnixMilli(second.time)).Truncate(time.Millisecond)
			return atOrAfter(rs, second.time, tail), atOrAfter(rs, answered.Add(-ago).UnixMilli()+1, tail)
		}},
		{"-1, the tail", fixedForm("-1"), exactAnswer(tail)},
	} {
		payload, made := form.payload()
		a := sought(t, c, seekChannel.Name, payload, 0x00)
		lowest, highest := form.want(made, time.Now())
		got, err := strconv.ParseInt(string(a.Payload), 10, 64)
		if err != nil || got < lowest || got > highest {
			t.Fatalf("the seek of %s (%q) answered %q; want the new offset, %d through %d", form.name, payload, a.Payload, lowest, highest)
		}
		replayed(t, c, rs, got)
		taken(t, c, "the PUBACKs of the deliveries after the seek of "+form.name)
	}
	next := &harness.Publish{QoS: 1, Topic: "events/x", Payload: []byte("forms after the tail")}
	support.Acknowledged(t, pub, next)
	support.DeliveredAt(t, c, 1, next.Topic, next.Payload)
}

// fixedForm is a seek form's payload that does not depend on when it is
// made.
func fixedForm(payload string) func() (string, time.Time) {
	return func() (string, time.Time) { return payload, time.Now() }
}

// exactAnswer is a seek form's answer that does not depend on when it is
// made.
func exactAnswer(offset int64) func(time.Time, time.Time) (int64, int64) {
	return func(time.Time, time.Time) (int64, int64) { return offset, offset }
}

// design.md, $saguin topics: "Surrounding Unicode whitespace is trimmed. A
// decimal integer in the signed 64-bit range denotes 0 for the floor, -1
// for the tail, or a positive offset. Signs and leading zeroes are
// accepted. Values below -1 are malformed. A duration means ago, with or
// without a leading minus. Duration parsing accepts nonnegative durations
// of one or more decimal numbers, each with an optional fraction and a
// unit [...]. It also accepts decimal days [...]. A duration of either form
// is at most 106,751 days. The other time form is RFC 3339. Overflow or
// other syntax answers malformed"; the seek row: "any other form is
// malformed", "malformed [...] each also 0x83 on the PUBACK".
//
// A durable MQTT 5 session seeks on events, which holds three records. Each
// malformed payload answers malformed, PUBACK 0x83; each accepted one
// answers its offset, PUBACK 0x00: the third record's offset with
// surrounding whitespace, a plus sign or leading zeroes, and the floor for
// a duration with a leading minus reaching back before the first record
// (design.md: "With floor 1, a time before the first record starts at the
// floor").
func TestSeekMalformedPayload(t *testing.T) {
	b, _ := seekRig(t, &harness.Options{}, eventsAt("syntax ", 3))
	rs := eventRecords(t, b.DB)
	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "syntax"))
	for _, payload := range []string{
		"-2", "9223372036854775808", "-9223372036854775809", "12x", "1.5", "h", "0x10", "1 2", "",
		"abc", "2562025h", "106752d", "2026-02-30T00:00:00Z",
	} {
		answers(t, sought(t, c, seekChannel.Name, payload, 0x83), "malformed", fmt.Sprintf("%q", payload))
	}
	third := strconv.FormatInt(rs[2].offset, 10)
	for _, accepted := range []struct{ payload string }{{" \t" + third + " \n"}, {"+" + third}, {"000" + third}} {
		answersOffset(t, sought(t, c, seekChannel.Name, accepted.payload, 0x00), rs[2].offset, fmt.Sprintf("%q", accepted.payload))
	}
	answersOffset(t, sought(t, c, seekChannel.Name, "-1000h", 0x00), rs[0].offset, `"-1000h"`)
}

// retainedRig launches a broker whose events keeps two of the four records
// it stores, 100 bytes each, by size (retention_test.go, sized), and
// returns it with the records retention left, in offset order: design.md,
// Words: Retention, "retention deletes a channel's oldest records by age
// and size".
func retainedRig(t *testing.T, o *harness.Options) (*harness.Broker, []record) {
	t.Helper()
	own := *o // the caller's Options stay as they were
	own.Channels = []harness.Channel{{Name: seekChannel.Name, Type: "append", Filter: seekChannel.Filter, RetentionBytes: 2*testkit.RecordBytes + testkit.RecordBytes/2}}
	b := support.Launch(t, &own)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "retained-pub", CleanStart: true})
	for i := range 4 {
		support.Acknowledged(t, pub, support.Sized(fmt.Sprintf("events/%d", i+1), testkit.RecordBytes))
	}
	awaitHeld(t, support.OpenDB(t, b.DB), seekChannel.Name, []string{"events/3", "events/4"}, "once events/3 and events/4 have passed retention_bytes")
	return b, eventRecords(t, b.DB)
}

// design.md, $saguin topics: "Explicit offsets outside floor through tail
// are refused, never clamped"; the seek row, "below-floor (User Property
// saguin-floor: the floor) or beyond-end (saguin-next: the next offset),
// each also 0x83 on the PUBACK". Words: Retention, "The floor is the oldest
// record retention has not passed. The tail is the next offset".
//
// events has kept its two newest records of four. A durable MQTT 5
// session seeks the offset below the floor: below-floor, saguin-floor the
// oldest surviving record's offset, PUBACK 0x83; the offset past the tail:
// beyond-end, saguin-next the tail, PUBACK 0x83. The floor and the tail
// themselves are answered as asked.
func TestSeekOutsideTheRangeIsRefused(t *testing.T) {
	b, rs := retainedRig(t, &harness.Options{})
	floor, tail := rs[0].offset, tailOf(t, support.OpenDB(t, b.DB), seekChannel.Name)
	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "bounded"))

	below := sought(t, c, seekChannel.Name, strconv.FormatInt(floor-1, 10), 0x83)
	answers(t, below, "below-floor", "the offset below the floor")
	answerProperty(t, below, "saguin-floor", floor)
	beyond := sought(t, c, seekChannel.Name, strconv.FormatInt(tail+1, 10), 0x83)
	answers(t, beyond, "beyond-end", "the offset past the tail")
	answerProperty(t, beyond, "saguin-next", tail)
	answersOffset(t, sought(t, c, seekChannel.Name, strconv.FormatInt(floor, 10), 0x00), floor, "the floor")
	answersOffset(t, sought(t, c, seekChannel.Name, strconv.FormatInt(tail, 10), 0x00), tail, "the tail")
}

// design.md, $saguin topics: "A time selects the lowest surviving offset
// whose timestamp is at or after it, or the tail. If retention has
// advanced the floor, a time before the oldest surviving record answers
// below-floor"; the seek row, "below-floor (User Property saguin-floor: the
// floor) [...] also 0x83 on the PUBACK".
//
// events has kept its two newest records of four. A durable MQTT 5
// session seeks the instant a millisecond before the oldest surviving
// record, as an RFC 3339 time and as a duration of 1000 hours ago: each
// answers below-floor, saguin-floor that record's offset, PUBACK 0x83. A
// time a millisecond after the newest record answers the tail.
func TestSeekByTimeAfterRetention(t *testing.T) {
	b, rs := retainedRig(t, &harness.Options{})
	floor, tail := rs[0].offset, tailOf(t, support.OpenDB(t, b.DB), seekChannel.Name)
	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "timed"))
	for _, payload := range []string{time.UnixMilli(rs[0].time - 1).UTC().Format(time.RFC3339Nano), "1000h"} {
		a := sought(t, c, seekChannel.Name, payload, 0x83)
		answers(t, a, "below-floor", fmt.Sprintf("%q, before the oldest surviving record", payload))
		answerProperty(t, a, "saguin-floor", floor)
	}
	after := time.UnixMilli(rs[len(rs)-1].time + 1).UTC().Format(time.RFC3339Nano)
	answersOffset(t, sought(t, c, seekChannel.Name, after, 0x00), tail, fmt.Sprintf("%q, after the newest record", after))
}

// design.md, $saguin topics, the seek row: "Answer: [...] no-session (the
// session is not durable) [...] also 0x83 on the PUBACK"; "A seek requires
// a durable session, not an existing consumer". Words: Session, "It is
// durable when its expiry is above zero".
//
// An MQTT 5 client whose session ends with its connection (no Session
// Expiry Interval) seeks the floor of events: no-session, PUBACK 0x83.
func TestSeekWithoutDurableSession(t *testing.T) {
	b, _ := seekRig(t, &harness.Options{}, eventsAt("passing ", 1))
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "passing", CleanStart: true})
	answers(t, sought(t, c, seekChannel.Name, "0", 0x83), "no-session", "a session that is not durable")
}

// design.md, $saguin topics, the seek row: "with seek on that channel (else
// 0x87)"; Flows, PUBLISH: "0x87 | the acl_file refuses the topic, $saguin
// requests included"; Access and reload: "An append rule may list write,
// read and seek". 0016a.md, 261030 143507: "a publish under $saguin/ is
// checked against the verb the design's $saguin table names for it [...],
// not write".
//
// The acl_file gives writer write and read on events, and seeker read and
// seek, no write. Each holds a durable MQTT 5 session. writer stores a
// record on events, then seeks its floor at QoS 1: PUBACK 0x87. seeker's
// seek of the floor is answered with that record's offset, PUBACK 0x00.
func TestSeekNeedsTheSeekVerb(t *testing.T) {
	acl := `roles:
  writers:
    - channel: events
      allow: [write, read]
  seekers:
    - channel: events
      allow: [read, seek]
users:
  writer: [writers]
  seeker: [seekers]
`
	o := aclOptions(t, acl, "writer", "seeker")
	o.Channels = []harness.Channel{seekChannel}
	b := support.Launch(t, o)
	login := func(user string) *harness.Client {
		p := aclLogin(user, user)
		p.Props = support.Durable
		c, _ := harness.Connected(t, b.Addr, harness.V5, p)
		return c
	}
	w := login("writer")
	support.Acknowledged(t, w, &harness.Publish{QoS: 1, Topic: "events/x", Payload: []byte("verb")})
	refused := seekOf(seekChannel.Name, 1, "0", answerTopic, []byte("writer"))
	support.Sent(t, w, refused)
	support.PubackOf(t, w, refused, 0x87)
	answersOffset(t, sought(t, login("seeker"), seekChannel.Name, "0", 0x00), eventRecords(t, b.DB)[0].offset, "the floor, by a user holding seek but not write")
}

// design.md, $saguin topics, the seek row: "a session may seek before it
// reads the channel, a saved seek position controlling the first read of
// that channel, a position so made staying with the session until the
// session ends (UNSUBSCRIBE)"; A seek: "A seek requires a durable session,
// not an existing consumer"; Flows, SUBSCRIBE, UNSUBSCRIBE: "When the last
// subscription reaching an append channel or a queue is removed, a durable
// session keeps its position there"; "The next subscription reaching the
// channel reads from the kept position"; "Without a subscription, the
// position moves only by a seek, or to the floor (Words: Missed). The
// session's end removes it"; the subscription table: append "Starts with
// the floor".
//
// events holds four records. A durable MQTT 5 session with no subscription
// seeks the third record's offset, sends DISCONNECT and resumes: its first
// SUBSCRIBE to events/# receives the third record first, then the fourth.
// It unsubscribes, the last subscription reaching events, and subscribes
// again: its next delivery is a record stored then, from the kept position,
// not the first record again. It unsubscribes again and, no subscription
// reaching events, seeks the floor: its next SUBSCRIBE receives every
// record from the first, in order. Another durable session seeks the third
// record, and a CONNECT with Clean Start 1 ends it: the new session's
// SUBSCRIBE receives the first record first.
func TestSeekBeforeSubscribing(t *testing.T) {
	b, pub := seekRig(t, &harness.Options{}, eventsAt("early ", 4))
	rs := eventRecords(t, b.DB)
	third := strconv.FormatInt(rs[2].offset, 10)

	connect := kept(harness.V5, "early")
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	answersOffset(t, sought(t, c, seekChannel.Name, third, 0x00), rs[2].offset, "the third record, before any subscription")
	support.Away(t, c)
	c = resumed(t, b.Addr, harness.V5, connect)
	support.Suback(t, c, nil, support.At(1, "events/#"), 1)
	replayed(t, c, rs, rs[2].offset)
	taken(t, c, "the PUBACKs of the deliveries from the sought position")
	unsuback(t, c, nil, []string{"events/#"}, 0x00)
	support.Suback(t, c, nil, support.At(1, "events/#"), 1)
	after := &harness.Publish{QoS: 1, Topic: "events/x", Payload: []byte("early after")}
	support.Acknowledged(t, pub, after)
	acknowledge(t, c, support.DeliveredAt(t, c, 1, after.Topic, after.Payload).ID, 0)
	taken(t, c, "the PUBACK of the record stored after the second SUBSCRIBE")
	unsuback(t, c, nil, []string{"events/#"}, 0x00)
	answersOffset(t, sought(t, c, seekChannel.Name, "0", 0x00), rs[0].offset, "the floor, no subscription reaching events")
	support.Suback(t, c, nil, support.At(1, "events/#"), 1)
	replayed(t, c, eventRecords(t, b.DB), rs[0].offset)

	ending := kept(harness.V5, "ending")
	e, _ := harness.Connected(t, b.Addr, harness.V5, ending)
	answersOffset(t, sought(t, e, seekChannel.Name, third, 0x00), rs[2].offset, "the third record, before the session ends")
	support.Away(t, e)
	e = support.Subscribed(t, b.Addr, harness.V5, kept(harness.V5, "ending"), support.At(1, "events/#"))
	support.DeliveredAt(t, e, 1, rs[0].topic, rs[0].payload)
}

// design.md, Flows, Delivery, Positions: "A seek changes the position at
// once and disables position movement by the earlier exchanges it marks.
// Deliveries already in flight on that channel finish as MQTT requires
// (MQTT 5, 4.3). The seek's transaction marks them so a restart knows
// (session_inflight.moves)"; "Deleting a durable session's settled exchange
// row must commit with its position beyond that record, unless a seek
// supersedes that progress." storage.md, session_inflight, moves: "1 while
// the row protects or may move the current position; 0 after a seek or saved
// progress supersedes it". CONNECT, 9: "The broker reads
// in-flight messages and PUBRELs from the store and resends them [...]
// Resent messages have DUP set". MQTT 5 4.3.3: the sender "MUST send a
// PUBREL packet when it receives a PUBREC packet from the receiver with a
// Reason Code value less than 0x80" [MQTT-4.3.3-4]. 0016a.md, 261013
// 085322: "a restart after a seek neither skips the replay asked for nor
// replays twice".
//
// events holds a QoS 2 record and three QoS 1 ones. A durable MQTT 5
// session with Receive Maximum 2 subscribes at QoS 2 and receives the first
// two, acknowledging neither, and seeks the floor. Then, connected, its
// PUBREC to the first gets PUBREL 0x00 and it completes it and acknowledges
// the second; or, in the second case, it sends DISCONNECT, the broker stops
// and starts again, and it resumes, receiving the two again, DUP 1, which
// it completes. Either way it then receives the four records from the
// floor, each once and in order, DUP 0, and next a record stored after
// them: the acknowledgements of the exchanges the seek found in flight did
// not move the position.
func TestSeekWithExchangesInFlight(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart bool
	}{{"connected", false}, {"across a restart", true}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ps := eventsAt("flight ", 4)
			ps[0].QoS = 2
			b := support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{seekChannel}})
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "flight-pub", CleanStart: true})
			published(t, pub, ps[0])
			support.PublishAll(t, pub, ps[1:])
			connect := kept(harness.V5, "flight")
			connect.Props = slices.Concat(connect.Props, receiveMaximum5(2)) // durable's array stays as it is
			c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, "events/#"))
			first := support.DeliveredAt(t, c, 2, ps[0].Topic, ps[0].Payload)
			second := support.DeliveredAt(t, c, 1, ps[1].Topic, ps[1].Payload)
			rs := eventRecords(t, b.DB)
			answersOffset(t, sought(t, c, seekChannel.Name, "0", 0x00), rs[0].offset, "the floor, two exchanges in flight")

			if tc.restart {
				support.Away(t, c)
				stopped(t, b)
				b = support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{seekChannel}})
				c = resumed(t, b.Addr, harness.V5, connect)
				first, second = redelivered(t, c, first), redelivered(t, c, second)
			}
			released(t, c, first.ID)
			pubcomp(t, c, first.ID)
			acknowledge(t, c, second.ID, 0)

			// The second record's replay may come before the first's PUBREL:
			// the window has room for both.
			if err := c.Acknowledge(support.DeliveredAt(t, c, 2, rs[0].topic, rs[0].payload)); err != nil {
				t.Fatalf("completing the replay of the first record: %v", err)
			}
			replayed(t, c, rs[1:], rs[1].offset)
			pub, _ = harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "flight-pub", CleanStart: true})
			next := &harness.Publish{QoS: 1, Topic: "events/x", Payload: []byte("flight after the replay")}
			support.Acknowledged(t, pub, next)
			support.DeliveredAt(t, c, 1, next.Topic, next.Payload)
		})
	}
}

// design.md, $saguin topics, A seek: "Deliveries on that channel prepared
// but not yet handed to the writer are cancelled. Their stored preparations
// are removed in the seek's transaction. Reading resumes from the new
// position."; Flows, Delivery, Preparation: "each consumer's live scan may
// select records ahead of the client's free window [...] A selected QoS 1 or
// 2 delivery has its packet identifier and, for a durable session, its
// stored exchange"; "After a crash, a stored preparation is resent as
// recovered, with DUP 1"; Positions: "A seek changes the position at once".
// CONNECT, 9: "The broker reads in-flight messages and PUBRELs from the
// store and resends them".
//
// events holds four QoS 1 records. A durable MQTT 5 session with Receive
// Maximum 1 subscribes at QoS 1 and receives the first, acknowledging it
// not, the rest waiting behind it, and seeks the fourth record's offset:
// once answered, session_inflight holds no row of the second or third
// record. Then,
// connected, it acknowledges the first; or, in the second case, the broker
// is killed and started again on the same files and the session resumes,
// receiving the first again, DUP 1, which it acknowledges. Either way its
// next delivery is the fourth record, DUP 0 (DUP 1 too after the crash, as
// its own preparation may have been stored after the seek), and next a
// record stored after it: neither the second nor the third, which the
// window kept back, is sent.
func TestSeekCancelsPreparedDeliveries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		crash bool
	}{{"connected", false}, {"across a crash", true}} {
		t.Run(tc.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir()}
			b, _ := seekRig(t, o, eventsAt("prepared ", 4))
			rs := eventRecords(t, b.DB)
			const client = "prepared"
			connect := kept(harness.V5, client)
			connect.Props = slices.Concat(connect.Props, receiveMaximum5(1)) // durable's array stays as it is
			c := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(1, "events/#"))
			first := support.DeliveredAt(t, c, 1, rs[0].topic, rs[0].payload)
			answersOffset(t, sought(t, c, seekChannel.Name, strconv.FormatInt(rs[3].offset, 10), 0x00), rs[3].offset, "the fourth record, the first in flight")
			rows := storedInflight(t, support.OpenDB(t, b.DB), client)
			if slices.ContainsFunc(rows, func(r inflightRow) bool { return r.offset == rs[1].offset || r.offset == rs[2].offset }) {
				t.Errorf("once the seek is answered, session_inflight holds %+v; want no row of the second or third record (offsets %d, %d)", rows, rs[1].offset, rs[2].offset)
			}

			if tc.crash {
				support.Crash(t, b)
				b = support.Launch(t, &harness.Options{Dir: o.Dir, Channels: []harness.Channel{seekChannel}})
				c = resumed(t, b.Addr, harness.V5, connect)
				first = redelivered(t, c, first)
			}
			acknowledge(t, c, first.ID, 0)
			fourth := harness.Expect[*harness.Publish](t, c)
			if fourth.Topic != rs[3].topic || !bytes.Equal(fourth.Payload, rs[3].payload) || fourth.QoS != 1 || (fourth.Dup && !tc.crash) {
				t.Fatalf("after the first's PUBACK: PUBLISH %q of %.40q at QoS %d, DUP %v; want the fourth record, %q of %.40q at QoS 1, the sought position's",
					fourth.Topic, fourth.Payload, fourth.QoS, fourth.Dup, rs[3].topic, rs[3].payload)
			}
			acknowledge(t, c, fourth.ID, 0)
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "prepared-pub", CleanStart: true})
			thenLiveAt(t, pub, c, 1, "events/after")
		})
	}
}

// design.md, $saguin topics, A seek: "A seek first applies the ordinary
// PUBLISH admission policy checks in the order of the PUBLISH refusal table.
// Its own refusals then follow in this order: no-session, malformed, and
// below-floor or beyond-end."; the seek row, "with seek on that channel (else
// 0x87)", "no-session (the session is not durable), malformed, below-floor
// [...] or beyond-end [...], each also 0x83 on the PUBACK"; Flows, PUBLISH:
// "0x87 | the acl_file refuses the topic, $saguin requests included", "0x90
// | a topic over limits.max_topic_length".
//
// The broker holds events and longevents, whose seek topic alone is over
// max_topic_length (30); the acl_file gives writer write and read on both,
// and seeker read and seek. Each sends, from a session that is not durable,
// QoS 1 seeks with a Response Topic: writer's malformed seek of longevents
// gets PUBACK 0x87, with no answer; seeker's malformed seek of longevents
// gets PUBACK 0x90, with no answer; seeker's malformed seek of events, and
// its seek past the tail, each answer no-session, PUBACK 0x83.
func TestSeekRefusalOrder(t *testing.T) {
	acl := `roles:
  writers:
    - channel: events
      allow: [write, read]
    - channel: longevents
      allow: [write, read]
  seekers:
    - channel: events
      allow: [read, seek]
    - channel: longevents
      allow: [read, seek]
users:
  writer: [writers]
  seeker: [seekers]
`
	o := aclOptions(t, acl, "writer", "seeker")
	long := harness.Channel{Name: "longevents", Type: "append", Filter: "long/#"}
	o.Channels, o.MaxTopicLength = []harness.Channel{seekChannel, long}, len(seekTopic(long.Name))-2
	b := support.Launch(t, o)
	w, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin("writer", "writer"))
	s, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin("seeker", "seeker"))

	for _, ask := range []struct {
		c      *harness.Client
		reason byte
		what   string
	}{{w, 0x87, "writer's, without seek"}, {s, 0x90, "seeker's, over max_topic_length"}} {
		p := seekOf(seekChannel.Name, 1, "x", answerTopic, []byte(ask.what))
		p.Topic = seekTopic(long.Name)
		support.Sent(t, ask.c, p)
		support.PubackOf(t, ask.c, p, ask.reason)
		taken(t, ask.c, "the refused malformed seek of longevents, "+ask.what)
	}
	for _, payload := range []string{"x", "9223372036854775807"} {
		answers(t, sought(t, s, seekChannel.Name, payload, 0x83), "no-session", fmt.Sprintf("%q by a session that is not durable", payload))
	}
}

// design.md, $saguin topics: "Its answer is a QoS 0 PUBLISH queued as a
// delivery. The answer goes only to the asking connection, on its Response
// Topic with its Correlation Data"; the reply row, "subscribe: the seek's
// answer, when no Response Topic was set"; "An MQTT 3.1.1 client [...]
// cannot make a request needing a Response Topic, except for a seek, which
// is answered on its reply topic"; the seek row, "(MQTT 3.1.1: an ordinary
// PUBACK, the answer carrying the reason, and the connection stays)";
// Delivery, Service publications: "direct request answer has an
// automatically assigned exact-topic subscription for that delivery".
// Flows, PUBLISH: "A seek whose answer carries the reason is the
// exception". A seek: "An answer on …/seek/reply carries the request's
// Correlation Data, as one on its Response Topic does".
//
// A watcher subscribes at QoS 0 to answers/#. A durable MQTT 5 session
// seeks the floor of events with Response Topic answers/seek and
// Correlation Data: the answer comes to it, on that topic with that data;
// the watcher's next delivery is a message published after the answer on
// answers/marker. The session seeks again without a Response Topic or
// Correlation Data: the answer comes on $saguin/consumer/events/seek/reply,
// to which it never subscribed; and again with Correlation Data alone: the
// answer comes on the reply topic carrying that data. A durable MQTT 3.1.1
// session seeks the floor, then a malformed payload, at QoS 1: each gets an
// ordinary PUBACK and an answer on the reply topic, the floor then
// malformed, and the connection answers PINGREQ afterwards.
func TestSeekAnswerRouting(t *testing.T) {
	b, pub := seekRig(t, &harness.Options{}, eventsAt("routed ", 2))
	floor := strconv.FormatInt(eventRecords(t, b.DB)[0].offset, 10)
	watcher := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "watcher", CleanStart: true}, support.At(0, "answers/#"))

	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "asker"))
	answers(t, sought(t, c, seekChannel.Name, "0", 0x00), floor, "the floor, with a Response Topic")
	marker := &harness.Publish{QoS: 1, Topic: "answers/marker", Payload: []byte("routed marker")}
	support.Acknowledged(t, pub, marker)
	delivered(t, watcher, marker.Topic, marker.Payload)
	bare := seekOf(seekChannel.Name, 1, "0", "", nil)
	support.Sent(t, c, bare)
	support.Puback(t, c, bare)
	answers(t, answerOn(t, c, replyTopic(seekChannel.Name), nil), floor, "the floor, without a Response Topic")
	correlated := seekOf(seekChannel.Name, 1, "0", "", []byte("on the reply topic"))
	support.Sent(t, c, correlated)
	support.Puback(t, c, correlated)
	answers(t, answerOn(t, c, replyTopic(seekChannel.Name), []byte("on the reply topic")), floor, "the floor, with Correlation Data and no Response Topic")

	old, _ := harness.Connected(t, b.Addr, harness.V311, kept(harness.V311, "asker-311"))
	for _, ask := range []struct{ payload, want string }{{"0", floor}, {"x", "malformed"}} {
		p := seekOf(seekChannel.Name, 1, ask.payload, "", nil)
		support.Sent(t, old, p)
		support.Puback(t, old, p)
		answers(t, answerOn(t, old, replyTopic(seekChannel.Name), nil), ask.want, fmt.Sprintf("%q over MQTT 3.1.1", ask.payload))
	}
	taken(t, old, "the refused seek over MQTT 3.1.1")
}

// design.md, $saguin topics: "Taken at QoS 0 too, a refused one dropped"
// (the seek row); "Every request taken at QoS 2 runs on arrival, as at QoS
// 1. If the request changes something, its PUBREC waits for the store to
// confirm that change (Replies). [...] Nothing about the exchange is
// stored. After a reconnect, its PUBREL therefore gets PUBCOMP 0x92, as for
// any unknown identifier, although the request has already run". Store,
// full mode: "Everything acknowledged survives a crash".
//
// events holds three records. A durable MQTT 5 session, with no
// subscription, seeks at QoS 0: the floor, answered; a malformed payload,
// dropped; then the third record's offset, whose answer, with its own
// Correlation Data, is the next to come. It seeks the second record at QoS
// 2: the PUBREC 0x00 and the answer come before any PUBREL. The broker is
// killed and started again; the session resumes, and its PUBREL for that
// identifier gets PUBCOMP 0x92. Its first SUBSCRIBE to events/# receives
// the second record first.
func TestSeekAtQoS0AndQoS2(t *testing.T) {
	dir := t.TempDir()
	b, _ := seekRig(t, &harness.Options{Dir: dir}, eventsAt("qos ", 3))
	rs := eventRecords(t, b.DB)
	connect := kept(harness.V5, "qos")
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	for _, ask := range []struct {
		payload, correlation string
		want                 int64 // 0: dropped
	}{{"0", "first", rs[0].offset}, {"x", "dropped", 0}, {strconv.FormatInt(rs[2].offset, 10), "third", rs[2].offset}} {
		if err := c.Send(seekOf(seekChannel.Name, 0, ask.payload, answerTopic, []byte(ask.correlation))); err != nil {
			t.Fatalf("a QoS 0 seek of %q: %v", ask.payload, err)
		}
		if ask.want != 0 {
			answersOffset(t, answerOn(t, c, answerTopic, []byte(ask.correlation)), ask.want, fmt.Sprintf("%q at QoS 0", ask.payload))
		}
	}

	held := seekOf(seekChannel.Name, 2, strconv.FormatInt(rs[1].offset, 10), answerTopic, []byte("at QoS 2"))
	support.Sent(t, c, held)
	pubrecOf(t, c, held, 0x00)
	answersOffset(t, answerOn(t, c, answerTopic, []byte("at QoS 2")), rs[1].offset, "the second record at QoS 2, before its PUBREL")
	support.Crash(t, b)
	b = support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{seekChannel}})
	c = resumed(t, b.Addr, harness.V5, connect)
	pubrel(t, c, held.ID, 0x92)
	support.Suback(t, c, nil, support.At(1, "events/#"), 1)
	support.DeliveredAt(t, c, 1, rs[1].topic, rs[1].payload)
}

// design.md, $saguin topics: "A publish under $saguin/ naming no request is
// refused with 0x90 without consulting the acl_file."; the seek row, "moves
// this durable session's position on an append channel [...] Any other
// channel or name: 0x90"; the reply row, "subscribe". Flows, PUBLISH: "0x90
// | [...] under $saguin/ naming no request".
//
// The broker holds events, append, and kv, latest; its acl_file gives
// everyone write, read and seek on both, and outsider read on elsewhere/#
// alone. A durable MQTT 5 session of each sends, at QoS 1 with a Response
// Topic, a seek's payload to a channel that does not exist, to kv, to
// another name than seek under events, to events' reply topic and to
// events' consumer level: each gets PUBACK 0x90, outsider's as everyone's.
func TestSaguinPublishNamingNoRequest(t *testing.T) {
	acl := `roles:
  all:
    - channel: events
      allow: [write, read, seek]
    - channel: kv
      allow: [write, read]
  outside:
    - topic: elsewhere/#
      allow: [read]
users:
  everyone: [all]
  outsider: [outside]
`
	o := aclOptions(t, acl, "everyone", "outsider")
	o.Channels = []harness.Channel{seekChannel, {Name: "kv", Type: "latest", Filter: "kv/#"}}
	b := support.Launch(t, o)
	topics := []string{seekTopic("nosuch"), seekTopic("kv"), "$saguin/consumer/events/rewind", replyTopic("events"), "$saguin/consumer/events"}
	for _, user := range []string{"everyone", "outsider"} {
		p := aclLogin(user, user)
		p.Props = support.Durable
		c, _ := harness.Connected(t, b.Addr, harness.V5, p)
		ps := make([]*harness.Publish, len(topics))
		for i, topic := range topics {
			ps[i] = seekOf(seekChannel.Name, 1, "0", answerTopic, []byte(topic))
			ps[i].Topic = topic
		}
		if got := pubackReasons(t, c, ps); !bytes.Equal(got, bytes.Repeat([]byte{0x90}, len(topics))) {
			t.Errorf("%s's PUBLISHes to %q: PUBACKs % X; want 0x90 for each", user, topics, got)
		}
	}
}

// design.md, $saguin topics: "Only the topics the table marks subscribe can
// be subscribed to."
// "Any other filter under `$saguin/` is refused with 0x87, as `$SYS/` is
// ([Operations, HTTP](#http)). A filter under `$saguin/queue` that names no
// worker topic is the exception: it is refused with 0x8F
// ([SUBSCRIBE](#subscribe)). MQTT 3.1.1 gets 0x80 for both."
// The reply row, "subscribe: the seek's
// answer"; A seek: "A subscription to …/seek/reply is granted at the QoS
// asked, whatever the acl_file says. It only ever carries the subscriber's
// own answers." MQTT 5 3.9.3, Table 3-8: 0x00 "Granted QoS 0", 0x01
// "Granted QoS 1", 0x02 "Granted QoS 2", 0x87 "Not authorized"; MQTT 3.1.1
// 3.9.3: 0x80 "Failure".
//
// The broker's acl_file gives outsider read on elsewhere/# alone, nothing on
// events. In each version outsider sends one SUBSCRIBE at QoS 0 of events'
// reply topic, $saguin/#, events' seek topic, $saguin/kv/get and
// $saguin/catalogue/events: the reply topic is granted QoS 0, the others
// refused with 0x87, MQTT 3.1.1 0x80. It then subscribes to the reply topic
// at QoS 1, then at QoS 2: granted 1, then 2.
func TestSaguinFilters(t *testing.T) {
	acl := `roles:
  outside:
    - topic: elsewhere/#
      allow: [read]
users:
  outsider: [outside]
`
	o := aclOptions(t, acl, "outsider")
	o.Channels = []harness.Channel{seekChannel}
	b := support.Launch(t, o)
	subs := support.At(0, replyTopic("events"), "$saguin/#", seekTopic("events"), "$saguin/kv/get", "$saguin/catalogue/events")
	for _, version := range bothVersions {
		c, _ := harness.Connected(t, b.Addr, version, aclLogin("filters-"+harness.VersionName(version), "outsider"))
		refusal := byte(0x87)
		if version == harness.V311 {
			refusal = 0x80
		}
		support.Suback(t, c, nil, subs, 0x00, refusal, refusal, refusal, refusal)
		for _, qos := range []byte{1, 2} {
			support.Suback(t, c, nil, support.At(qos, replyTopic("events")), qos)
		}
	}
}

// design.md, $saguin topics: "A time selects the lowest surviving offset
// whose timestamp is at or after it, or the tail [...] With floor 1, a time
// before the first record starts at the floor"; the seek row, "Answer: the
// new offset". storage.md, channels: next is "the offset the next record
// takes", floor "the oldest offset retention has not passed", 1 in a new
// file.
//
// events has never held a record. A durable MQTT 5 session seeks it by an
// RFC 3339 time and by a duration of 1000 hours ago: each answers 1, the
// floor, PUBACK 0x00, not below-floor.
func TestSeekByTimeOnAnEmptyChannel(t *testing.T) {
	b, _ := seekRig(t, &harness.Options{}, nil)
	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "empty"))
	for _, payload := range []string{"2026-01-01T00:00:00Z", "1000h"} {
		answersOffset(t, sought(t, c, seekChannel.Name, payload, 0x00), 1, fmt.Sprintf("%q, on a channel that never held a record", payload))
	}
}

// design.md, Flows, Replies: "$saguin answers [...] are sent only once the
// store confirms, where they confirm a change. Such a reply goes only on the
// connection that asked. If the store confirms after that connection has
// ended or been taken over, what was stored stays and the reply is
// dropped"; $saguin topics, the seek row, "moves this durable session's
// position", "a saved seek position controlling the first read of that
// channel"; Closing a connection: "The core never waits for a connection to
// close". 0016a.md: agreed test 15, written so because a takeover follows
// the old connection's writes (Store, Requests).
//
// events holds three records; a watcher subscribes at QoS 0 to answers/#.
// The test holds the database's write lock while a durable MQTT 5 session
// with no subscription seeks the third record at QoS 1 with Response Topic
// answers/seek; once the store writer has met the lock, the client ends its
// sending and reads the end of its connection, no PUBACK or answer before
// it. The lock is released. The session resumes: its first SUBSCRIBE to
// events/# receives the third record first, nothing before the SUBACK, and
// the watcher has received nothing.
func TestSeekAnswerDroppedWithItsConnection(t *testing.T) {
	b := launchTracingLocks(t, &harness.Options{Channels: []harness.Channel{seekChannel}})
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "dropped-pub", CleanStart: true})
	support.PublishAll(t, pub, eventsAt("dropped ", 3))
	rs := eventRecords(t, b.DB)
	watcher := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "dropped-watcher", CleanStart: true}, support.At(0, "answers/#"))
	connect := kept(harness.V5, "dropped")
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)

	lock := holdWriteLock(t, b.DB)
	before := len(traced(t, b.trace))
	support.Sent(t, c, seekOf(seekChannel.Name, 1, strconv.FormatInt(rs[2].offset, 10), answerTopic, []byte("dropped")))
	awaitLockMet(t, b.trace, before, "the seek")
	if err := c.CloseWrite(); err != nil {
		t.Fatalf("ending the client's sending, its seek at the store: %v", err)
	}
	harness.ExpectClosed(t, c)
	lock.release(t)

	c = resumed(t, b.Addr, harness.V5, connect)
	support.Suback(t, c, nil, support.At(1, "events/#"), 1)
	support.DeliveredAt(t, c, 1, rs[2].topic, rs[2].payload)
	nothingWithin(t, watcher, quiet, "after the seek whose connection ended before its store confirmed")
}

// requestSeries is saguin_requests_total of request and result (metrics.md,
// Publishing).
func requestSeries(request, result string) string {
	return testkit.SeriesKey("saguin_requests_total", "request", request, "result", result)
}

// design.md, $saguin topics: "Any other answer exceeding it is discarded.
// [...] A lost or discarded answer never undoes a completed request. Each
// request receives one terminal result"; metrics.md, saguin_requests_total:
// "answer_too_large: an otherwise completed request whose answer exceeds
// Maximum Packet Size". MQTT 5 3.1.2.11.4: "The Server MUST NOT send packets
// exceeding Maximum Packet Size to the Client" [MQTT-3.1.2-24].
//
// events holds three records. A durable MQTT 5 session advertising a
// Maximum Packet Size of 200 bytes seeks the second record's offset with a
// Response Topic of over 300 bytes: PUBACK 0x00, and the answer, larger
// than 200 bytes, never reaches it (the client refuses anything larger).
// /metrics counts the seek as answer_too_large, not answered, and the
// session's first SUBSCRIBE to events/# receives the second record first.
func TestSeekAnswerOverMaximumPacketSize(t *testing.T) {
	b, _ := seekRig(t, &harness.Options{Operations: true}, eventsAt("large ", 3))
	rs := eventRecords(t, b.DB)
	connect := kept(harness.V5, "large")
	connect.Props = slices.Concat(connect.Props, harness.Properties{{ID: harness.MaximumPacketSize, Int: 200}})
	c, _ := harness.Connected(t, b.Addr, harness.V5, connect)
	p := seekOf(seekChannel.Name, 1, strconv.FormatInt(rs[1].offset, 10), "answers/"+strings.Repeat("a", 300), []byte("large"))
	support.Sent(t, c, p)
	support.Puback(t, c, p)
	awaitSeries(t, b, map[string]float64{requestSeries("seek", "answer_too_large"): 1, requestSeries("seek", "answered"): 0})
	support.Suback(t, c, nil, support.At(1, "events/#"), 1)
	support.DeliveredAt(t, c, 1, rs[1].topic, rs[1].payload)
}

// metrics.md, saguin_requests_total: "result is refused: a request refused
// on its PUBACK or PUBREC, including a refusal carried by its direct
// answer; dropped: a discarded QoS 0 request; answer_too_large [...];
// otherwise answered. Each request contributes one terminal result";
// design.md, $saguin topics: "Each request receives one terminal result",
// "Taken at QoS 0 too, a refused one dropped".
//
// Eight seeks of events: answered, a durable MQTT 5 session's floor at QoS
// 0, 1 and 2; refused, its malformed and past-the-tail payloads at QoS 1,
// a session that is not durable at QoS 1, and a durable MQTT 3.1.1
// session's malformed payload at QoS 1, its ordinary PUBACK's answer
// carrying the reason; dropped, the durable MQTT 5 session's malformed
// payload at QoS 0. /metrics then reads answered 3, refused 4, dropped 1
// and answer_too_large 0.
func TestRequestsCountOneResult(t *testing.T) {
	b, _ := seekRig(t, &harness.Options{Operations: true}, eventsAt("counted ", 1))
	c, _ := harness.Connected(t, b.Addr, harness.V5, kept(harness.V5, "counted"))
	sought(t, c, seekChannel.Name, "0", 0x00)
	sought(t, c, seekChannel.Name, "x", 0x83)
	sought(t, c, seekChannel.Name, "9223372036854775807", 0x83)
	for _, payload := range []string{"0", "x"} {
		if err := c.Send(seekOf(seekChannel.Name, 0, payload, answerTopic, []byte("QoS 0 "+payload))); err != nil {
			t.Fatalf("a QoS 0 seek of %q: %v", payload, err)
		}
	}
	answerOn(t, c, answerTopic, []byte("QoS 0 0"))
	held := seekOf(seekChannel.Name, 2, "0", answerTopic, []byte("QoS 2"))
	support.Sent(t, c, held)
	pubrecOf(t, c, held, 0x00)
	answerOn(t, c, answerTopic, []byte("QoS 2"))
	pubrel(t, c, held.ID, 0x00)

	passing, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "counted-passing", CleanStart: true})
	sought(t, passing, seekChannel.Name, "0", 0x83)
	old, _ := harness.Connected(t, b.Addr, harness.V311, kept(harness.V311, "counted-311"))
	p := seekOf(seekChannel.Name, 1, "x", "", nil)
	support.Sent(t, old, p)
	support.Puback(t, old, p)
	answerOn(t, old, replyTopic(seekChannel.Name), nil)

	awaitSeries(t, b, map[string]float64{
		requestSeries("seek", "answered"): 3, requestSeries("seek", "refused"): 4, requestSeries("seek", "dropped"): 1, requestSeries("seek", "answer_too_large"): 0,
	})
}
