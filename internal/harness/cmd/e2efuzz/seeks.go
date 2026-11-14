package main

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
)

// Seeks and the $saguin/ topics (design.md, $saguin topics). A client sends
// a seek, `$saguin/consumer/<channel>/seek`, at QoS 0, 1 or 2, in MQTT 5
// mostly with a Response Topic of the run's own (answerPrefix) and now and
// then Correlation Data; the model predicts its PUBACK or PUBREC and its
// answer, and the step waits for both: "Its answer is a QoS 0 PUBLISH
// queued as a delivery. The answer goes only to the asking connection, on
// its Response Topic with its Correlation Data", a seek without one on its
// reply topic, as every MQTT 3.1.1 seek is, with its Correlation Data too:
// "An answer on …/seek/reply carries the request's Correlation Data, as one
// on its Response Topic does". The refusals are drawn in the design's
// order (A seek): "A seek first applies the ordinary PUBLISH admission policy
// checks in the order of the PUBLISH refusal table. Its own refusals then
// follow in this order: no-session, malformed, and below-floor or
// beyond-end." Each payload form the design
// accepts and which reaches the floor (a time on a channel retention
// passes but by size or age, as a queue's, answering below-floor once
// it has), malformed payloads and an offset past the tail are drawn; so are a session that is not durable (no-session), a
// user without seek on the channel (0x87) and a seek topic over
// max_topic_length (0x90). A refusal at QoS 0 is dropped; an answer over the
// client's Maximum Packet Size is discarded, the seek standing. A PUBLISH on
// an answer's topic that no seek awaits fails the run.
//
// A seek the model predicts answered with an offset moves the session's
// position on the channel to the floor: what it has passed may come again,
// out of order, as a position made again does (appends.go, restart), and
// nothing is owed by it, which the run does not know the timing of; what
// was prepared and not yet handed to the writer is cancelled, never sent
// ("Deliveries on that channel prepared but not yet handed to the writer
// are cancelled. Their stored preparations are removed in the seek's
// transaction. Reading resumes from the new position."). Every
// form drawn reaches the floor, so no record the session is owed is
// skipped: "-1", a time after a record and an offset inside the channel
// would pass records the model owes. A seek with no position on the
// channel is kept for the next SUBSCRIBE reaching it, which then starts
// from the floor ("a saved seek position controlling the first read of
// that channel").
//
// SUBSCRIBE draws $saguin/ filters too, each refused with 0x87 (MQTT
// 3.1.1: 0x80), excluding the worker filters covered in queues.go:
// "Any other filter under `$saguin/` is refused with 0x87, as `$SYS/` is
// ([Operations, HTTP](#http)). A filter under `$saguin/queue` that names no
// worker topic is the exception: it is refused with 0x8F
// ([SUBSCRIBE](#subscribe)). MQTT 3.1.1 gets 0x80 for both."
// The reply topic, the one $saguin/
// filter granted ("A subscription to …/seek/reply is granted at the QoS
// asked, whatever the acl_file says"), is longer than max_topic_length on
// every channel, and is not drawn: the conformance tests cover it.

// answerPrefix begins the Response Topic of the run's seeks: no topic the
// run publishes begins with it, its letter an upper-case one (word), so a
// PUBLISH on it is an answer.
const answerPrefix = "A/"

// seekStats counts the run's seeks: those answered with an offset, those
// whose answer refused them, those refused on their PUBACK or PUBREC or
// dropped, and those whose answer the client's Maximum Packet Size
// discarded; filters counts the $saguin/ filters refused.
type seekStats struct {
	moved, answeredRefusal, refused, tooLarge, filters int
}

// seekForms are the payloads the run seeks with, by what the design makes
// of them on a channel retention never passes, holding a record: each
// floor form reaches the floor ("0 (the floor)", "Surrounding Unicode
// whitespace is trimmed", "Signs and leading zeroes are accepted"), and so
// does each time form, before every record or on a channel that never held
// one ("With floor 1, a time before the first record starts at the
// floor"; "A duration means ago, with or without a leading minus"); each
// malformed form answers malformed ("Values
// below -1 are malformed", "A duration of either form is at most 106,751
// days", "Overflow or other syntax answers malformed"); the largest offset
// is past the tail, beyond-end.
var (
	floorForms     = []string{"0", "+0", " 00\t", " 0\n"}
	timeForms      = []string{"1970-01-01T00:00:00Z", "1000000h", "-40000d"}
	malformedForms = []string{"-2", "9223372036854775808", "1.5", "12x", "", "abc", "106752d", "2026-02-30T00:00:00Z", "--1"}
	beyondForms    = []string{"9223372036854775807", "+09223372036854775807"}
)

// seekTopic is the seek of channel, and replyTopic the topic its answer
// goes on where no Response Topic was set (design.md, $saguin topics).
func seekTopic(channel string) string  { return "$saguin/consumer/" + channel + "/seek" }
func replyTopic(channel string) string { return seekTopic(channel) + "/reply" }

// seekable are the append channels whose seek topic keeps within
// max_topic_length, and tooLong those whose seek topic is over it, which the
// PUBLISH table refuses with 0x90. A seek on a latest channel is not drawn:
// its row names append channels and queues, its answer for any other
// channel 0x90, while the acl_file, which comes first, can grant no seek
// on a latest channel, and which of the two refuses it the design leaves
// open.
var seekable, tooLong = func() (in, over []string) {
	for _, c := range channels {
		switch {
		case c.latest:
		case len(seekTopic(c.name)) <= maxTopicLength:
			in = append(in, c.name)
		default:
			over = append(over, c.name)
		}
	}
	return in, over
}()

// saguinFilters are the $saguin/ filters SUBSCRIBE draws, none of them a
// topic marked subscribe, each within max_topic_length and
// max_topic_levels.
var saguinFilters = []string{"$saguin/#", "$saguin/kv/get", "$saguin/consumer/+/seek", "$saguin/catalogue/ca", seekTopic("ca")}

// asked is a request awaiting its answers (a seek, or one of requests.go):
// its packet identifier and QoS, the reason code its PUBACK or PUBREC
// carries, and the answer it gets, "" for none: on its topic, with its
// Correlation Data (nil for none), its payload an offset where offset is
// set, one check accepts where it is set, its length within lengths, else
// want, and, where property is set, that User Property holding an offset.
type asked struct {
	id          uint16
	qos         byte
	reason      byte
	topic       string
	correlation []byte
	offset      bool
	check       func(payload []byte) error
	lengths     [2]int
	want        string
	property    string
	got         bool
}

// expects reports whether a awaits an answer.
func (a *asked) expects() bool { return a.offset || a.check != nil || a.want != "" }

// seek: a connected client sends a seek and waits for its PUBACK or PUBREC,
// PUBREL and PUBCOMP, and its answer, as the model predicts them (design.md,
// $saguin topics): "Every request taken at QoS 2 runs on arrival, as at QoS
// 1 [...] The ordinary PUBREL and PUBCOMP follow"; "A refusal at QoS 2 is
// carried on PUBREC as it would be on PUBACK. An MQTT 3.1.1 client receives
// an ordinary PUBREC, with the answer carrying the reason"; Flows, PUBLISH:
// "0x87 | the acl_file refuses the topic, $saguin requests included",
// "0x90 | a topic over limits.max_topic_length", "At QoS 1 or 2 an MQTT
// 3.1.1 client has no refusal code and is disconnected", which the run
// leaves to the PUBLISH action: an MQTT 3.1.1 client is refused so only at
// QoS 0.
func (f *fuzzer) seek() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil && s.asked == nil && s.unanswered() == 0 })
	if s == nil {
		return false, nil
	}
	r := f.rng
	v5 := s.conn.Version == harness.V5
	channel := seekable[r.IntN(len(seekable))]
	p := &harness.Publish{QoS: byte(r.IntN(3)), Topic: seekTopic(channel)}
	if denied(s.user, "qos2") {
		p.QoS = min(p.QoS, 1) // its QoS 2 PUBLISH ends the connection (PUBLISH's action)
	}
	a := &asked{topic: replyTopic(channel)}
	if v5 && r.IntN(4) != 0 {
		a.topic = answerPrefix + strconv.Itoa(f.step)
		p.Props = append(p.Props, harness.Property{ID: harness.ResponseTopic, Str: a.topic})
	}
	if v5 && r.IntN(2) == 0 {
		a.correlation = []byte(f.word(0, 64))
		p.Props = append(p.Props, harness.Property{ID: harness.CorrelationData, Data: a.correlation})
	}
	answer := ""
	switch n := r.IntN(10); {
	case s.user == restricted:
		// Its rights hold no seek: design.md, $saguin topics, "with seek on
		// that channel (else 0x87)".
		a.reason = 0x87
	case n == 0:
		p.Topic = seekTopic(tooLong[r.IntN(len(tooLong))])
		a.reason = 0x90
	case !s.durable:
		answer = "no-session"
	case n < 4:
		p.Payload = []byte(malformedForms[r.IntN(len(malformedForms))])
		answer = "malformed"
	case n == 4:
		p.Payload = []byte(beyondForms[r.IntN(len(beyondForms))])
		answer = "beyond-end"
		if v5 {
			a.property = "saguin-next"
		}
	case n < 7 && f.retention[channel] == nil && queueOf(channel) == nil:
		p.Payload = []byte(timeForms[r.IntN(len(timeForms))])
		a.offset = true
	default:
		a.offset = true
	}
	if p.Payload == nil {
		p.Payload = []byte(floorForms[r.IntN(len(floorForms))])
	}
	// The request keeps within max_packet_size, its Correlation Data cut
	// short where it would not: long, its answer may pass the client's
	// Maximum Packet Size.
	for {
		wire, err := harness.Encode(p, s.conn.Version)
		if err != nil {
			return true, err
		}
		if len(wire) <= maxPacketSize {
			break
		}
		a.correlation = a.correlation[:len(a.correlation)-(len(wire)-maxPacketSize)]
		p.Props[len(p.Props)-1].Data = a.correlation
	}
	switch refusal := answer != ""; {
	case a.reason != 0:
		// Refused by the reader, before the request runs, and so never
		// answered.
		if !v5 {
			p.QoS = 0
		}
	case refusal && p.QoS == 0:
		// Dropped.
	case refusal:
		a.want = answer
		if v5 {
			a.reason = 0x83
		}
	}
	discarded, sure := s.answerFits(a)
	if !sure {
		return false, nil // its answer may or may not fit: another action
	}
	if p.QoS > 0 {
		p.ID = s.nextID()
	}
	a.id, a.qos = p.ID, p.QoS
	switch {
	case a.offset:
		s.sought(channel)
		f.seekStats.moved++
	case a.want != "":
		f.seekStats.answeredRefusal++
	default:
		f.seekStats.refused++
	}
	if discarded {
		f.seekStats.tooLarge++
		a.offset, a.want = false, ""
	}
	f.note("%s: SEEK %q QoS %d %q properties %v: want %s", s.id, p.Topic, p.QoS, p.Payload, p.Props, a)
	return true, s.ask(p, a)
}

// ask sends p, a request whose packet identifier and QoS a takes, on s's
// connection, and waits for what a awaits (awaitAnswers).
func (s *slot) ask(p *harness.Publish, a *asked) error {
	s.asked = a
	defer func() { s.asked = nil }()
	if err := s.conn.Send(p); err != nil {
		return err
	}
	if err := s.awaitAnswers(a); err != nil {
		return err
	}
	return s.drain()
}

// String describes what a awaits, for the step log.
func (a *asked) String() string {
	var b strings.Builder
	if a.qos > 0 {
		fmt.Fprintf(&b, "%s 0x%02X, ", harness.Name(harness.AnswerKind(a.qos)), a.reason)
	}
	switch {
	case a.offset:
		fmt.Fprintf(&b, "an offset on %q", a.topic)
	case a.check != nil:
		fmt.Fprintf(&b, "an answer of %d to %d bytes on %q", a.lengths[0], a.lengths[1], a.topic)
	case a.want != "":
		fmt.Fprintf(&b, "%q on %q", a.want, a.topic)
	default:
		b.WriteString("no answer")
	}
	return b.String()
}

// answerFits reports, for s's connection and a, whether its answer is
// surely over the client's Maximum Packet Size (discarded) or surely
// within it, or neither (sure false): design.md, $saguin topics, "An answer
// exceeding the client's Maximum Packet Size is discarded. The completed
// request is not undone". The answer is longest with a Topic Alias set
// beside its topic, its offsets of 19 digits; shortest with its topic sent
// by alias alone, its offsets of one.
func (s *slot) answerFits(a *asked) (discarded, sure bool) {
	limit := s.conn.MaxPacketSize
	if limit == 0 || !a.expects() {
		return false, true
	}
	size := func(topic int, alias bool, digits, length int) int {
		var props harness.Properties
		if a.correlation != nil {
			props = append(props, harness.Property{ID: harness.CorrelationData, Data: a.correlation})
		}
		if alias {
			props = append(props, harness.Property{ID: harness.TopicAlias, Int: 1})
		}
		if a.property != "" {
			props = append(props, harness.User(a.property, strings.Repeat("9", digits)))
		}
		payload := len(a.want)
		switch {
		case a.offset:
			payload = digits
		case a.check != nil:
			payload = length
		}
		n := 2 + topic + payload
		if s.conn.Version == harness.V5 {
			wire, _ := harness.AppendProperties(nil, props) // drawn valid
			n += len(wire)
		}
		return 1 + len(harness.AppendVarInt(nil, uint32(n))) + n
	}
	aliased := s.conn.TopicAliasMaximum > 0
	longest, shortest := size(len(a.topic), aliased, 19, a.lengths[1]), size(len(a.topic), false, 1, a.lengths[0])
	if aliased {
		shortest = size(0, true, 1, a.lengths[0])
	}
	switch {
	case longest <= limit:
		return false, true
	case shortest > limit:
		return true, true
	}
	return false, false
}

// awaitAnswers reads s's packets, checking deliveries, until a's PUBACK or
// PUBREC has come and its answer, where it awaits one (takeAnswer); then
// completes a QoS 2 exchange not refused with PUBREL, whose PUBCOMP is
// 0x00: the request's identifier is held until its PUBCOMP, "like any other
// incoming QoS 2 exchange's identifier" (design.md, $saguin topics).
//
// The answer is "a QoS 0 PUBLISH queued as a delivery", and "A connection's
// prepared and sent deliveries together are at most twice its window"
// (design.md, Delivery, Preparation): while the acknowledgements s's client
// withholds fill its window, the broker may hold a window of deliveries
// sent and another prepared, and the answer waits for the client. So the
// wait, while the answer is still owed and they fill the window, releases
// them all, the exchanges kept included (windowShut).
func (s *slot) awaitAnswers(a *asked) error {
	acked := a.qos == 0
	for !acked || (a.expects() && !a.got) {
		if a.expects() && !a.got && s.windowShut() {
			if err := s.releaseAcks(); err != nil {
				return err
			}
			if err := s.completePubrels(true); err != nil {
				return err
			}
		}
		p, err := s.read()
		if err != nil {
			return fmt.Errorf("%s: waiting for the request's answers (%s): %w", s.id, a, err)
		}
		if received(p) {
			continue // a delivery, its PUBREL, or the answer (read)
		}
		ack, ok := p.(*harness.Ack)
		if !ok || acked || ack.Kind != harness.AnswerKind(a.qos) || ack.ID != a.id {
			return fmt.Errorf("%s: %s while waiting for the request's answers (%s)", s.id, show(p), a)
		}
		if ack.Reason != a.reason {
			return fmt.Errorf("%s: %s 0x%02X to the request; want 0x%02X (design.md, $saguin topics)", s.id, harness.Name(ack.Kind), ack.Reason, a.reason)
		}
		acked = true
	}
	if a.qos != 2 || a.reason >= 0x80 {
		return nil
	}
	if err := s.conn.Send(&harness.Ack{Kind: harness.PUBREL, ID: a.id}); err != nil {
		return err
	}
	p, err := s.recv()
	if err != nil {
		return fmt.Errorf("%s: waiting for the request's PUBCOMP: %w", s.id, err)
	}
	if ack, ok := p.(*harness.Ack); !ok || ack.Kind != harness.PUBCOMP || ack.ID != a.id || ack.Reason != 0 {
		return fmt.Errorf("%s: %s after the request's PUBREL; want PUBCOMP %d 0x00 (design.md, $saguin topics: \"The ordinary PUBREL and PUBCOMP follow\")", s.id, show(p), a.id)
	}
	return nil
}

// windowShut reports whether the acknowledgements s's client withholds fill
// its in-flight window: the deliveries it has read and not acknowledged,
// and the PUBRELs whose PUBCOMP it has not sent, each holding its place in
// the window until the acknowledgement settling it ("A message is in flight
// from its hand-off to the writer"; Positions: "an acknowledgement, PUBACK,
// PUBCOMP or a refusing PUBREC, settles its own delivery"; design.md,
// Delivery). Below it, the broker hands what it prepared to the writer
// without the client, and so makes room for the answer.
func (s *slot) windowShut() bool {
	return len(s.incoming.held)+len(s.incoming.rels) >= s.window
}

// answerTopic reports whether topic is one an answer comes on: the run's
// Response Topics, or under $saguin/, where no record is.
func answerTopic(topic string) bool {
	return strings.HasPrefix(topic, answerPrefix) || strings.HasPrefix(topic, "$saguin/")
}

// takeAnswer checks p, a PUBLISH on an answer's topic, against the seek s
// awaits: design.md, $saguin topics, "Its answer is a QoS 0 PUBLISH queued
// as a delivery. The answer goes only to the asking connection, on its
// Response Topic with its Correlation Data"; the seek row, "Answer: the new
// offset; or no-session [...], malformed, below-floor (User Property
// saguin-floor: the floor) or beyond-end (saguin-next: the next offset)",
// "Taken at QoS 0 too, a refused one dropped"; "Each request receives one
// terminal result"; Sagüin properties: "MQTT 3.1.1 clients receive none of
// these properties".
func (s *slot) takeAnswer(p *harness.Publish) error {
	a := s.asked
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%s: PUBLISH %q QoS %d of %q, properties %v: %s (design.md, $saguin topics)", s.id, p.Topic, p.QoS, p.Payload, p.Props, fmt.Sprintf(format, args...))
	}
	if a == nil || !a.expects() || a.got {
		return bad("an answer no request awaits: a refused request at QoS 0 is dropped, an answer over Maximum Packet Size discarded, and each request answered once")
	}
	cd, has := p.Props.Get(harness.CorrelationData)
	switch {
	case p.Topic != a.topic:
		return bad("the answer goes on %q, its Response Topic or else a seek's reply topic", a.topic)
	case p.QoS != 0:
		return bad("an answer is a QoS 0 PUBLISH")
	case has != (a.correlation != nil) || !bytes.Equal(cd.Data, a.correlation):
		return bad("the answer carries Correlation Data %q, present %v; want the request's, %q", cd.Data, has, a.correlation)
	case a.check != nil:
		if err := a.check(p.Payload); err != nil {
			return bad("%v", err)
		}
	case a.offset && !positive(string(p.Payload)):
		return bad("want the new offset, in decimal")
	case !a.offset && string(p.Payload) != a.want:
		return bad("want %q", a.want)
	}
	if a.property != "" {
		if v := p.Props.User(a.property); len(v) != 1 || !positive(v[0]) {
			return bad("want %s once, an offset in decimal", a.property)
		}
	}
	a.got = true
	return nil
}

// positive reports whether v is a positive decimal offset.
func positive(v string) bool {
	n, err := strconv.ParseInt(v, 10, 64)
	return err == nil && n > 0
}

// sought moves s's session's position on channel to the floor, as a seek
// answered with an offset does (design.md, Flows, Delivery, Positions: "A
// seek changes the position at once"): the position lasting there ends,
// and one made at the seek, from the floor, takes its subscriptions,
// owing no history the run checks (epoch.checked); with none there, the
// seek is kept for the next SUBSCRIBE reaching the channel (place). What
// the connection has in flight on the channel, handed to its writer and
// not yet read, may come again beside the replay (restart); what it has
// prepared is cancelled, its record coming from the replay alone
// (design.md, $saguin topics: "Deliveries on that channel prepared but not
// yet handed to the writer are cancelled"). A position kept with no
// subscription stays kept where the seek moves it ("Without a
// subscription, the position moves only by a seek", UNSUBSCRIBE), what
// the connection reads of the channel from then on coming again from the
// next SUBSCRIBE reaching it (resume).
func (s *slot) sought(channel string) {
	s.f.serial++
	if e := s.subs.live(channel); e != nil {
		e.ended = s.f.serial
		next := &epoch{channel: channel, created: s.f.serial, step: s.f.step, conn: s.conn, filters: slices.Clone(e.filters), checked: true}
		if e.kept {
			next.kept, next.keptOn, next.keptFrom, next.keptAt = true, s.conn, len(s.incoming.reads[channel]), s.f.serial
		}
		s.subs.positions[channel] = append(s.subs.positions[channel], next)
	} else {
		if s.subs.sought == nil {
			s.subs.sought = map[string]bool{}
		}
		s.subs.sought[channel] = true
	}
	s.incoming.epochs++
	s.restart(channel, s.inFlightUnread())
}

// saguinFilter draws one of saguinFilters.
func (f *fuzzer) saguinFilter() string {
	return saguinFilters[f.rng.IntN(len(saguinFilters))]
}
