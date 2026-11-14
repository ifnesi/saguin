package main

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// fate is what the model knows of one message's record. Its zero value:
// sent, but its PUBACK (QoS 2: its PUBREL sent, its PUBCOMP) never read, or
// a QoS 0 message, which nothing answers (design.md, PUBLISH: "0 | on
// PUBLISH | never"), so stored or not.
// A message stored whose PUBACK the client never got, its connection ended
// or taken over first, is no error (design.md, Flows, Replies: "what was stored stays and the
// reply is dropped").
type fate uint8

// stored: acknowledged with PUBACK 0x00, or PUBCOMP 0x00 at QoS 2, so in
// the database from then on (design.md, Flows, Replies: "PUBACK, PUBREC,
// PUBREL, PUBCOMP [...] are sent only once the store confirms").
const stored fate = 1

// full: refused with 0x97, its channel full: never stored (refusedForRoom).
const full fate = 3

// takenWill: a Will whose end the client saw the core take, so published
// at once and owed as an acknowledged PUBLISH is (fireTaken); its record
// follows, unconfirmed.
const takenWill fate = 2

// message is one PUBLISH the run sent that the broker may store, and what
// its record must hold. A PUBLISH the model predicts refused is never one:
// a record of it is an error.
type message struct {
	// confirmed is an upper boundary on broker intake, established by the
	// publisher's PUBACK or a following PINGRESP. Zero leaves intake unordered
	// against later subscriptions on other connections. Burst readers share
	// this observation, so its publication uses the standard atomic type.
	// sentAt and acknowledgedAt bound intake on the observer's clock (Unix
	// milliseconds). The first write starts the window; PUBACK/PUBREC or a
	// later answered packet closes it. Atomic fields let other burst peers
	// check a delivery while the publisher observes its acknowledgement.
	sentAt, acknowledgedAt atomic.Int64
	confirmed              atomic.Uint64
	serial                 uint64
	tag                    string
	length                 int
	props                  harness.Properties
	ackStep                int
	step                   int    // the step that sent it
	publisher              string // the publisher's client id (storage.md, records); "" where hidden, and for a record the bridge's peer forwarded
	// bridge is the bridge it arrived over (bridges.go), "" for a client's
	// PUBLISH or a Will (storage.md, records: bridge, "the id of the bridge
	// it arrived over, 0 for none").
	bridge string
	// hidden counts the publisher's connection where it is MQTT 3.1.1 with
	// an empty client id, whose assigned identifier the run learns from
	// the records (slot.hidden); 0 otherwise.
	hidden int
	topic  string // as stored: the alias's topic where the PUBLISH named none
	qos    byte
	retain bool
	id     string // the saguin-id it set; "" for one the broker makes
	// body makes its payload's bytes after the tag (fill), from which the
	// model remakes the payload of every delivery of it (published);
	// payload is the payload itself, kept only for the record checks.
	body    body
	payload string
	// fate is what became of it (the fate type, through fateIs and
	// setFate): its publisher's goroutine sets it on the acknowledgement
	// while burst peers check a delivery against it (surelyIn), so it is
	// atomic, as confirmed is.
	fate atomic.Uint32
	will *will // the Will it is, nil for a PUBLISH
	// channel is the channel it lands in (route); bytes, on a channel with
	// a max_bytes or a retention_bytes, what it charges it (messageBytes);
	// deleted: retention deleted its record (retention.go); logged: it is
	// one of its latest channel topic's changes (latest.go).
	channel string
	bytes   int64
	deleted bool
	logged  bool
	// heldAt is the serial of the last record check that found it its
	// latest channel topic's record (latest.go, precedes).
	heldAt uint64
	// saguinIDs counts the saguin-id properties its publisher sent, and
	// expiring says it set a Message Expiry (Sagüin properties).
	saguinIDs int
	expiring  bool
	// seen is what a delivery carried of its record, and stored the record
	// once read (appends.go).
	seen   atomic.Pointer[seenProps]
	stored atomic.Pointer[record]
	// job is the job it is, on a queue, and deadOf the job whose dead
	// letter it is, nil for none (queues.go).
	job, deadOf *job
}

// fateIs reports whether m's fate is f.
func (m *message) fateIs(f fate) bool { return fate(m.fate.Load()) == f }

// setFate makes f m's fate. A writer sets ackStep first, so a reader that
// sees the fate sees the step with it.
func (m *message) setFate(f fate) { m.fate.Store(uint32(f)) }

// out is one PUBLISH drawn for a connection and the answer the model
// predicts for it.
type out struct {
	p *harness.Publish
	m *message // nil where the model predicts no record
	// refusals are the PUBACK (QoS 2: PUBREC) reason codes any of which
	// refuses it (design.md, PUBLISH's refusal table); none, it is accepted.
	refusals []byte
	// full: its channel may be full, so 0x00 or 0x97 (placeAll).
	// limited: its channel has a max_bytes, so what the model predicts of
	// it depends on the bytes the broker has committed, which the step log
	// leaves out: a seed's log is its draws alone (String).
	full, limited bool
	// ends: the broker closes the connection for it, after DISCONNECT
	// reason in MQTT 5; nothing after it on the connection is read.
	ends   bool
	reason byte
	// quiet: the CONNECT's Request Problem Information was 0.
	quiet bool
	// deletion: an empty payload on a latest channel's topic, which deletes
	// its value (latest.go).
	deletion bool
	// earliest is the earliest the reader can read it, its connection held
	// to pacedUser's publish limits (pacing.charge); zero for no bound.
	earliest time.Time
}

// draw is what a PUBLISH may be.
type draw uint8

const (
	drawAny      draw = iota // accepted, refused, or ending the connection
	drawAccepted             // accepted, at QoS 0 or 1
	drawQoS1                 // accepted, at QoS 1
	drawLarge                // accepted, at QoS 0 or 1, from half of max_packet_size up to its limit, sensor readings or random bytes
	drawCrowd                // as drawAccepted, on a channel with a max_bytes (MQTT 3.1.1: at QoS 0)
	drawQoS2                 // accepted, at QoS 2
	drawQueue                // accepted, at QoS 0 or 1, on the queue (queues.go)
)

// maxExchanges is the most QoS 2 exchanges the run lets one session hold: half
// of Receive Maximum, so the PUBLISHes a step sends beside them stay within
// it (MQTT 5 3.3.4: the Client "MUST NOT send more than Receive Maximum QoS
// 1 and QoS 2 PUBLISH packets for which it has not received PUBACK, PUBCOMP,
// or PUBREC with a Reason Code of 128 or greater" [MQTT-3.3.4-7]).
const maxExchanges = receiveMaximum / 2

// exchange is one incoming QoS 2 exchange of a session (design.md, Flows,
// PUBLISH: QoS 2 "enters on PUBREL, from a held copy stored and charged to
// the channel before PUBREC"), from its PUBREC 0x00 until its PUBCOMP, or
// sent on a connection that ended before its PUBREC came.
type exchange struct {
	o *out
	// answered: its PUBREC 0x00 came; otherwise its PUBLISH is resent when
	// the session resumes, which the core takes as a repeat or as new.
	answered bool
	// released: its PUBREL was sent, so its message may be stored; resent:
	// that PUBREL went on an earlier connection, whose move may have been
	// made, its PUBCOMP lost.
	released, resent bool
	// repeats counts the PUBRECs owed to its PUBLISH sent again on this
	// connection.
	repeats int
}

// mayHold reports whether s's session may hold n more QoS 2 exchanges: its
// own id's session, at most maxExchanges. A connection with an empty client id
// holds a session the model does not resume, so it sends none.
func (s *slot) mayHold(n int) bool {
	return !s.anon && len(s.held)+n <= maxExchanges
}

// carries reports whether a PUBLISH drawn as kind, at qos, may carry topic,
// drawn or named by a Topic Alias reused (publishProps): any topic for
// drawAny; otherwise one s's user may write (design.md, Access and reload),
// on a channel with a max_bytes for drawCrowd, on the queue for drawQueue
// and off it for drawAccepted and drawLarge (newPublish), and, at QoS 1 or
// 2 in MQTT 3.1.1, on no channel with a max_bytes.
func (s *slot) carries(kind draw, topic string, qos byte) bool {
	if kind == drawAny {
		return true
	}
	ch := route(topic)
	switch {
	case !may(s.user, topic, true):
		return false
	case kind == drawCrowd && limitOf(ch) == 0:
		return false
	case kind == drawQueue && queueOf(ch) == nil:
		return false
	case (kind == drawAccepted || kind == drawLarge) && queueOf(ch) != nil:
		return false
	}
	return s.conn.Version == harness.V5 || qos == 0 || limitOf(ch) == 0
}

// nextID is the next packet identifier of s's connection that none of its
// pending PUBLISHes, its session's QoS 2 exchanges or its unanswered
// PUBRELs holds: MQTT 5 2.2.1, a packet "MUST assign it a non-zero Packet
// Identifier that is currently unused [MQTT-2.2.1-3]". A new connection
// numbers from 1 again while its session's exchanges keep theirs.
func (s *slot) nextID() uint16 {
	for {
		if id := s.conn.NextID(); s.pending[id] == nil && s.held[id] == nil && !s.unknown[id] {
			return id
		}
	}
}

// newPublish draws s's n-th PUBLISH of the step: a topic, mostly valid, else
// in each way the PUBLISH table refuses one; in MQTT 5, topic aliases set
// and reused (MQTT 5 3.3.2.3.4), saguin-id, and other properties; and, now
// and then, one the broker ends the connection for.
func (f *fuzzer) newPublish(s *slot, n int, kind draw) (*out, error) {
	r := f.rng
	v5 := s.conn.Version == harness.V5
	p := &harness.Publish{QoS: byte(r.IntN(2)), Retain: r.IntN(8) == 0}
	switch kind {
	case drawQoS1:
		p.QoS = 1
	case drawQoS2:
		p.QoS = 2
	}
	if kind == drawCrowd && !v5 {
		p.QoS = 0
	}
	// A PUBLISH drawn to be accepted is on a topic its user may write
	// (design.md, Access and reload), and retained only where its user may
	// keep retained values (CONNECT, 8: Retain Available).
	for {
		p.Topic = f.pubTopic()
		switch {
		case kind == drawCrowd:
			p.Topic = f.limitedTopic()
		case kind == drawQueue || (kind == drawQoS2 && r.IntN(4) == 0):
			p.Topic = f.queueTopic()
		}
		if (kind == drawAccepted || kind == drawLarge) && queueOf(route(p.Topic)) != nil {
			// A burst's hundreds of jobs would leave the queue's retention
			// nothing but evictions: its jobs come one step at a time
			// (queues.go).
			p.Topic = "cc" + strings.TrimPrefix(p.Topic, queueName)
		}
		if !v5 && p.QoS > 0 && limitOf(route(p.Topic)) != 0 {
			// A refusal for room would close an MQTT 3.1.1 connection at QoS 1
			// or 2 ("has no refusal code and is disconnected"), which the model can
			// predict only where it is certain: such a PUBLISH goes to cc, which
			// no channel claims.
			p.Topic = "cc" + p.Topic[2:]
		}
		if s.carries(kind, p.Topic, p.QoS) {
			break
		}
	}
	if kind != drawAny && denied(s.user, "retained") {
		p.Retain = false
	}
	o := &out{p: p, quiet: s.quiet}
	shape := 0
	if kind == drawAny {
		shape = r.IntN(20)
	}
	// design.md, PUBLISH, the refusal table; the acl_file's 0x87, which
	// comes first, follows: no rule of the run's allows a topic beginning
	// with $ (may), so the topics under $share/ get it, but not those under
	// $saguin/ naming no request ($saguin topics: "A publish under $saguin/
	// naming no request is refused with 0x90 without consulting the
	// acl_file").
	switch shape {
	case 1:
		// 0x87: "a topic under $SYS/".
		p.Topic, o.refusals = "$SYS/"+f.topic(1+r.IntN(2)), []byte{0x87}
	case 2:
		// 0x90: "a topic over limits.max_topic_length".
		p.Topic, o.refusals = f.word(maxTopicLength+1, maxTopicLength+6), []byte{0x90}
	case 3:
		// 0x90: over "limits.max_topic_levels".
		p.Topic, o.refusals = strings.Repeat(f.word(1, 2)+"/", maxTopicLevels)+f.word(1, 2), []byte{0x90}
	case 4:
		// 0x90: "under $share/".
		p.Topic, o.refusals = "$share/"+f.topic(2), []byte{0x90}
	case 5:
		// 0x90: "under $saguin/ naming no request".
		p.Topic, o.refusals = "$saguin/x"+f.word(1, 3), []byte{0x90}
	case 6:
		// 0x83: "a saguin- property other than saguin-id" (MQTT 5 only:
		// MQTT 3.1.1 carries no properties).
		if v5 {
			p.Props, o.refusals = harness.Properties{harness.User("saguin-x"+f.word(1, 3), f.word(0, 3))}, []byte{0x83}
		}
	case 7:
		// DISCONNECT, Reasons Sagüin sends: "0x9B [...] for [...] QoS 2 [...]
		// its CONNACK withheld", the acl_file denying the user qos2; "An MQTT
		// 3.1.1 connection just closes". Otherwise a QoS 2 PUBLISH, whose
		// PUBREC settle reads, but in MQTT 3.1.1 on a channel that may be
		// full, as the topic's draw above has it.
		switch {
		case denied(s.user, "qos2"):
			p.QoS, o.ends, o.reason = 2, true, 0x9B
		case s.mayHold(n+1) && (v5 || limitOf(route(p.Topic)) == 0):
			p.QoS = 2
		}
	case 8:
		// MQTT 5 3.3.4: "If the receiver does not already have a mapping
		// for this Topic Alias [and] the packet has a zero length Topic Name
		// field it is a Protocol Error and the receiver uses DISCONNECT with
		// Reason Code of 0x82".
		if alias := uint16(1 + r.IntN(topicAliasMaximum)); v5 && s.aliases[alias] == "" {
			p.Topic, p.Props = "", harness.Properties{{ID: harness.TopicAlias, Int: uint32(alias)}}
			o.ends, o.reason = true, 0x82
		}
	}
	// design.md, PUBLISH: "0x87 | the acl_file refuses the topic", first in
	// the table's order ("Policy reasons are taken in the order of the
	// table"), a protocol error coming before every policy reason (Flows:
	// "Checks proceed through framing, protocol recognition and packet
	// validity, admission policy, then storage").
	if !o.ends && !strings.HasPrefix(p.Topic, "$saguin/") && !may(s.user, p.Topic, true) {
		o.refusals = []byte{0x87}
	}
	// DISCONNECT, Reasons Sagüin sends: "0x9A [...] for retained messages
	// its CONNACK withheld", on any topic (MQTT-3.2.2-14); "An MQTT 3.1.1
	// connection just closes". Where another protocol error ends the
	// connection, the PUBLISH is not retained, so the one reason is known.
	if p.Retain && denied(s.user, "retained") {
		if o.ends {
			p.Retain = false
		} else {
			o.refusals, o.ends, o.reason = nil, true, 0x9A
		}
	}
	if len(o.refusals) > 0 && p.QoS > 0 && !v5 {
		// design.md, PUBLISH: "At QoS 1 or 2 an MQTT 3.1.1 client has no
		// refusal code and is disconnected".
		o.ends = true
	}
	topic := p.Topic
	if len(o.refusals) == 0 && !o.ends {
		o.m = &message{step: f.step, publisher: s.clientID, hidden: s.hidden, topic: p.Topic, qos: p.QoS, retain: p.Retain}
		if v5 {
			f.publishProps(s, kind, o)
		}
		// A Topic Alias reused names its mapping's topic, which the PUBLISH
		// is taken as carrying (MQTT 5 3.3.2.3.4) and which a reload since
		// the mapping was set may forbid: "Every check after the swap uses
		// the new acl_file" (design.md, Access and reload), so the 0x87 above
		// is decided again on it. Only a PUBLISH drawn as any may meet it
		// (publishProps).
		if topic = o.m.topic; !may(s.user, topic, true) {
			o.m, o.refusals = nil, []byte{0x87}
		}
	}
	// A quarter of an MQTT 5 client's PUBLISHes on a latest channel's topic
	// are deletions (design.md, Channels: "An empty payload deletes the
	// value"), but a flood's, whose bodies are what it is for. An empty
	// payload carries no tag: its first saguin-id does, the record's id
	// (Sagüin properties: "The value of saguin-id is the publisher's own
	// where it set one, kept as sent"; "A publish carrying saguin-id twice
	// takes the first").
	if v5 && kind != drawLarge && isLatest(route(topic)) {
		o.deletion = r.IntN(4) == 0
	}
	if o.deletion && o.m != nil {
		o.m.id = payloadTag(f.step, n)
		p.Props = slices.Insert(p.Props, 0, harness.User("saguin-id", o.m.id))
	}
	if p.QoS > 0 {
		p.ID = s.nextID()
	}
	least, most, made := 0, 80, letters
	switch kind {
	case drawLarge:
		// At least half of max_packet_size; fill cuts it to the packet's
		// limit. A flood's bodies together outgrow max_incoming_memory.
		// Most are over 4 KiB, which the broker compresses where that makes
		// them shorter (design.md, Store, Compression): sensor readings,
		// which S2 makes shorter, or random bytes, which nothing does.
		least, most, made = maxPacketSize/2, maxPacketSize, [...]bodyKind{readings, noise}[f.rng.IntN(2)]
	case drawAccepted, drawQoS1, drawCrowd, drawQoS2, drawQueue:
		// A window's messages are small, as a busy publisher's are. Every
		// body asks for its bytes (design.md, Message memory).
		most = 8
	}
	return o, f.fill(o, n, least, most, made, s.conn.Version)
}

// publishProps draws an accepted MQTT 5 PUBLISH's properties: a Topic Alias
// set or reused (MQTT 5 3.3.2.3.4: the sender "sets a Topic Alias mapping by
// including a non-zero length Topic Name and a Topic Alias"; "a sender can
// send a PUBLISH packet that contains that Topic Alias and a zero length
// Topic Name. The receiver then treats the incoming PUBLISH as if it had
// contained the Topic Name of the Topic Alias"), then the others
// (messageProps). A PUBLISH reuses only a mapping whose topic is one its
// draw may carry (carries).
func (f *fuzzer) publishProps(s *slot, kind draw, o *out) {
	r, p := f.rng, o.p
	if r.IntN(3) == 0 {
		alias := uint16(1 + r.IntN(topicAliasMaximum))
		if topic := s.aliases[alias]; topic != "" && r.IntN(2) == 0 && s.carries(kind, topic, p.QoS) {
			p.Topic, o.m.topic = "", topic
		} else {
			if s.aliases == nil {
				s.aliases = map[uint16]string{}
			}
			s.aliases[alias] = p.Topic
		}
		p.Props = append(p.Props, harness.Property{ID: harness.TopicAlias, Int: uint32(alias)})
	}
	f.messageProps(o)
}

// messageProps draws the properties of an accepted MQTT 5 PUBLISH but a
// Topic Alias (publishProps): saguin-id, once or twice (design.md, Sagüin
// properties: "The saguin-id property sets the record's id"; "A publish
// carrying saguin-id twice takes the first"), User Properties and now and
// then another property a PUBLISH may carry.
func (f *fuzzer) messageProps(o *out) {
	r, p := f.rng, o.p
	if r.IntN(6) == 0 {
		o.m.id = f.word(1, 8)
		p.Props = append(p.Props, harness.User("saguin-id", o.m.id))
		if r.IntN(3) == 0 {
			p.Props = append(p.Props, harness.User("saguin-id", f.word(1, 8)))
		}
	}
	if r.IntN(3) == 0 {
		p.Props = append(p.Props, harness.User(f.word(1, 4), f.word(0, 4)))
	}
	switch r.IntN(8) {
	case 0:
		p.Props = append(p.Props, harness.Property{ID: harness.PayloadFormatIndicator, Int: 1})
	case 1:
		p.Props = append(p.Props, harness.Property{ID: harness.MessageExpiryInterval, Int: uint32(expiryFloor)})
	case 2:
		p.Props = append(p.Props, harness.Property{ID: harness.ResponseTopic, Str: f.topic(1 + r.IntN(2))})
	case 3:
		p.Props = append(p.Props, harness.Property{ID: harness.CorrelationData, Data: []byte(f.word(1, 4))})
	}
}

// payloadTag is "<step>.<n>", which no two PUBLISHes of the run share.
func payloadTag(step, n int) string {
	return strconv.Itoa(step) + "." + strconv.Itoa(n)
}

// bodyKind is what a payload holds after its tag.
type bodyKind uint8

const (
	letters  bodyKind = iota // lower-case letters, as most PUBLISHes carry
	readings                 // a device's sensor readings in JSON, which S2 makes shorter
	noise                    // random bytes, which no compression makes shorter
)

// body is how a payload's bytes after its tag are made: from its kind and
// a seed of its own, the first n bytes of one endless stream. The model
// remakes from it the payload of every delivery, which it checks byte for
// byte, keeping no body at rest (published); a payload fill cuts to fit its
// packet is the same stream, shorter.
type body struct {
	kind bodyKind
	seed uint64
}

// bytes is b's first n bytes.
func (b body) bytes(n int) []byte {
	r := rand.New(rand.NewPCG(b.seed, uint64(b.kind)))
	out := make([]byte, 0, n+96)
	for len(out) < n {
		switch b.kind {
		case letters:
			out = append(out, byte('a'+r.IntN(26)))
		case readings:
			out = fmt.Appendf(out, `{"device":"pump-%02d","ts":%d,"temperature":%.2f,"humidity":%.1f,"status":"ok"},`,
				r.IntN(16), 1760000000000+r.Int64N(1<<30), 18+4*r.Float64(), 40+20*r.Float64())
		case noise:
			out = binary.LittleEndian.AppendUint64(out, r.Uint64())
		}
	}
	return out[:n]
}

// published is m's payload as its PUBLISH carried it, or a Will's as its
// CONNECT did: its tag and a space, then its body (fill), cut to m.length.
func (m *message) published() []byte {
	if m.will != nil {
		return m.will.w.Payload
	}
	p := append(make([]byte, 0, max(m.length, len(m.tag)+1)), m.tag...)
	p = append(p, ' ')
	if rest := m.length - len(p); rest > 0 {
		p = append(p, m.body.bytes(rest)...)
	}
	return p[:m.length]
}

// fill gives o's PUBLISH its payload: its payloadTag and a space, then
// least to most bytes made as kind says, cut so the packet keeps
// limits.max_packet_size; a deletion's is empty, its tag its saguin-id
// (newPublish). Every body asks for its reservation before reading
// (design.md, Message memory).
func (f *fuzzer) fill(o *out, n, least, most int, kind bodyKind, version byte) error {
	tag := payloadTag(f.step, n)
	made := body{kind: kind, seed: f.rng.Uint64()}
	o.p.Payload = append([]byte(tag+" "), made.bytes(least+f.rng.IntN(most-least+1))...)
	if o.deletion {
		o.p.Payload = []byte{} // its draws taken all the same, so the step is the seed's
	}
	wire, err := harness.Encode(o.p, version)
	if err != nil {
		return fmt.Errorf("encoding a PUBLISH: %w", err)
	}
	if over := len(wire) - maxPacketSize; over > 0 {
		if over > len(o.p.Payload)-len(tag) {
			return fmt.Errorf("a PUBLISH of %d bytes with only its tag for payload, over max_packet_size %d", len(wire)-len(o.p.Payload)+len(tag), maxPacketSize)
		}
		o.p.Payload = o.p.Payload[:len(o.p.Payload)-over]
	}
	if o.m != nil {
		o.m.payload, o.m.body, o.m.tag, o.m.length = string(o.p.Payload), made, tag, len(o.p.Payload)
	}
	return nil
}

// String is o, for the step log.
func (o *out) String() string { return o.describe(!o.limited) }

// predicted is o with the answer the model predicts, observations
// included, for failures.
func (o *out) predicted() string { return o.describe(true) }

// describe is o, with the answer the model predicts where predicted, else
// for a PUBLISH on a channel with a max_bytes, what its room decides.
func (o *out) describe(predicted bool) string {
	p := o.p
	var b strings.Builder
	fmt.Fprintf(&b, "PUBLISH %q QoS %d retain %v", p.Topic, p.QoS, p.Retain)
	if p.QoS > 0 {
		fmt.Fprintf(&b, " id %d", p.ID)
	}
	for _, q := range p.Props {
		b.WriteString(", " + propString(q))
	}
	fmt.Fprintf(&b, ", %d bytes: want ", len(p.Payload))
	ack := harness.Name(harness.AnswerKind(p.QoS))
	switch {
	case !predicted && p.QoS > 0:
		fmt.Fprintf(&b, "%s as its channel's room decides (0x97 when full)", ack)
	case o.ends && len(o.refusals) > 0:
		b.WriteString("the close (MQTT 3.1.1 has no refusal code)")
	case o.ends:
		fmt.Fprintf(&b, "the close, after DISCONNECT 0x%02X in MQTT 5", o.reason)
	case p.QoS == 0:
		b.WriteString("nothing")
	case len(o.refusals) > 0:
		fmt.Fprintf(&b, "%s refusing with one of % X", ack, o.refusals)
	case o.full:
		fmt.Fprintf(&b, "%s 0x00, or 0x97 should its channel be full", ack)
	default:
		fmt.Fprintf(&b, "%s 0x00", ack)
	}
	return b.String()
}

// again is o's PUBLISH sent once more under its packet identifier, with
// dup: on its stored topic, with no Topic Alias, which may map another
// topic by now or, on a new connection, none (MQTT 5 3.3.2.3.4: mappings
// last as long as their connection [MQTT-3.3.2-7]). Its record's
// properties never held the alias (expect).
func (o *out) again(dup bool) *harness.Publish {
	p := *o.p
	p.Dup, p.Topic = dup, o.m.topic
	p.Props = slices.DeleteFunc(slices.Clone(p.Props), func(q harness.Property) bool { return q.ID == harness.TopicAlias })
	return &p
}

// expect keeps each message of batch the broker may store, for the record
// checks: a latest channel's change in its topic's log, checked by topic
// (latest.go), a queue's as a job (queues.go), any other by its payload,
// its tag (fill) naming it.
func (f *fuzzer) expect(batch ...*out) {
	for _, o := range batch {
		if o.m == nil {
			continue
		}
		f.serial++
		o.m.serial = f.serial
		if o.m.retain {
			f.retained++
		}
		o.m.props = slices.DeleteFunc(slices.Clone(o.p.Props), func(p harness.Property) bool { return p.ID == harness.TopicAlias })
		o.m.saguinIDs = len(o.p.Props.User("saguin-id"))
		_, o.m.expiring = o.p.Props.Get(harness.MessageExpiryInterval)
		f.track(o.m)
		f.publications[o.m.tag] = o.m
		if queueOf(o.m.channel) != nil {
			f.addJob(o.m)
		}
		switch {
		case isLatest(o.m.channel):
			f.latest.add(o.m)
			o.m.payload = ""
		case f.dbPath != "":
			f.messages[o.m.payload] = o.m
		default:
			o.m.payload = ""
		}
	}
}

// send writes batch, in one write, on s's connection, keeping each QoS 1
// PUBLISH that is answered with a PUBACK, and each QoS 2 one with a PUBREC,
// as pending.
func send(s *slot, batch ...*out) error {
	var wire []byte
	at := time.Now()
	for _, o := range batch {
		if o.m != nil {
			o.m.sentAt.CompareAndSwap(0, at.UnixMilli())
		}
		if s.paced() {
			if o.earliest = s.pace.charge(at, len(o.p.Payload)); o.earliest.After(at) {
				s.f.heldBack.Add(1)
			}
		}
		if o.m != nil && o.p.QoS == 0 {
			s.sent = append(s.sent, o.m)
		}
		b, err := harness.Encode(o.p, s.conn.Version)
		if err != nil {
			return fmt.Errorf("encoding %s: %w", o, err)
		}
		wire = append(wire, b...)
		if o.p.QoS > 0 && !o.ends {
			if s.pending == nil {
				s.pending = map[uint16]*out{}
			}
			s.pending[o.p.ID] = o
		}
	}
	return s.conn.SendRaw(wire)
}

// acknowledgeIntake bounds earlier QoS 0 intake by an answered packet.
// before is the acknowledged publication's serial, or zero for a control
// request sent after all pending QoS 0 input. A PUBACK for an earlier
// PUBLISH cannot bound a later QoS 0 publication in the same burst.
func (s *slot) acknowledgeIntake(before uint64, at int64) {
	for _, m := range s.sent {
		if before == 0 || m.serial < before {
			m.acknowledgedAt.CompareAndSwap(0, at)
		}
	}
}

// settle checks p when it answers what s's connection is owed, and reports
// whether it did. A PUBACK or PUBREC of one of pending: design.md, PUBLISH,
// "PUBACK once stored", QoS 2 "PUBREC, then PUBCOMP once in the channel", or
// a refusal's code as the refusal table gives it ("A QoS 2 refusal rides the
// PUBREC"); a PUBREC 0x00 makes the exchange held. A PUBREC for a PUBLISH
// repeating a held identifier, and a PUBCOMP (complete). MQTT 5 3.4.2.2.2:
// with Request Problem Information 0, no Reason String or User Property on
// it [MQTT-3.1.2-29].
func (s *slot) settle(p harness.Packet) (bool, error) {
	ack, ok := p.(*harness.Ack)
	if !ok {
		return false, nil
	}
	switch ack.Kind {
	case harness.PUBCOMP:
		return s.complete(ack)
	case harness.PUBREC:
		if x := s.held[ack.ID]; x != nil && x.repeats > 0 {
			// "While a QoS 2 PUBLISH's identifier is held awaiting PUBREL,
			// any later PUBLISH with that identifier gets PUBREC again"
			// (MQTT-4.3.3-10).
			x.repeats--
			if ack.Reason != 0 {
				return true, fmt.Errorf("PUBREC 0x%02X to a PUBLISH repeating held identifier %d, %s; want 0x00 (design.md, PUBLISH: \"any later PUBLISH with that identifier gets PUBREC again\")",
					ack.Reason, ack.ID, x.o)
			}
			return true, s.quietly(ack, x.o.String())
		}
	case harness.PUBACK:
	default:
		return false, nil
	}
	o := s.pending[ack.ID]
	if o == nil || harness.AnswerKind(o.p.QoS) != ack.Kind {
		return false, nil
	}
	delete(s.pending, ack.ID)
	now := time.Now()
	if err := earlyAnswer(ack, o, now); err != nil {
		return true, err
	}
	if o.m != nil {
		o.m.acknowledgedAt.CompareAndSwap(0, now.UnixMilli())
		if o.m.serial != 0 {
			s.acknowledgeIntake(o.m.serial, now.UnixMilli())
		}
	}
	switch {
	case ack.Reason == 0x97 && o.full:
		// design.md, PUBLISH: 0x97 for "a channel's max_bytes".
		s.f.refusedForRoom(o.m)
	case ack.Reason == 0 && len(o.refusals) == 0 && o.p.QoS == 2:
		if s.held == nil {
			s.held = map[uint16]*exchange{}
		}
		s.held[ack.ID] = &exchange{o: o, answered: true}
	case ack.Reason == 0 && len(o.refusals) == 0:
		o.m.ackStep = s.f.step
		o.m.setFate(stored)
		o.m.confirmed.CompareAndSwap(0, s.f.serial)
	case len(o.refusals) == 0:
		return true, fmt.Errorf("%s 0x%02X to %s", harness.Name(ack.Kind), ack.Reason, o.predicted())
	case !slices.Contains(o.refusals, ack.Reason):
		return true, fmt.Errorf("%s 0x%02X to %s", harness.Name(ack.Kind), ack.Reason, o.predicted())
	}
	return true, s.quietly(ack, o.String())
}

// complete checks a PUBCOMP, and reports whether it was one s's connection
// is owed: to a PUBREL for an identifier no exchange holds, 0x92 (MQTT
// 3.1.1: a PUBCOMP, which has no code), design.md, PUBLISH: "A PUBREL for an
// unknown identifier gets PUBCOMP 0x92"; to a held exchange's PUBREL, 0x00
// once its publication is in the channel ("PUBREC, then PUBCOMP once in the
// channel"), which makes its message stored, or 0x92 to a PUBREL resent on
// a new connection, whose earlier one's move was made.
func (s *slot) complete(ack *harness.Ack) (bool, error) {
	if s.unknown[ack.ID] {
		delete(s.unknown, ack.ID)
		want := byte(0x92)
		if s.conn.Version == harness.V311 {
			want = 0
		}
		if ack.Reason != want {
			return true, fmt.Errorf("PUBCOMP 0x%02X to a PUBREL for identifier %d, which no exchange holds; want 0x%02X (design.md, PUBLISH: \"A PUBREL for an unknown identifier gets PUBCOMP 0x92\")",
				ack.Reason, ack.ID, want)
		}
		return true, s.quietly(ack, fmt.Sprintf("a PUBREL for identifier %d", ack.ID))
	}
	x := s.held[ack.ID]
	if x == nil || !x.released {
		return false, nil
	}
	delete(s.held, ack.ID)
	if ack.Reason != 0 && (ack.Reason != 0x92 || !x.resent) {
		return true, fmt.Errorf("PUBCOMP 0x%02X to the PUBREL of %s; want 0x00, once in its channel (design.md, PUBLISH, the table)", ack.Reason, x.o)
	}
	m := x.o.m
	m.ackStep = s.f.step
	m.setFate(stored)
	m.confirmed.CompareAndSwap(0, s.f.serial)
	return true, s.quietly(ack, "the PUBREL of "+x.o.String())
}

// quietly checks ack, answering what, against the CONNECT's Request
// Problem Information 0: MQTT 5 3.1.2.11.7, the Server "MUST NOT send a
// Reason String or User Properties on any packet other than PUBLISH, CONNACK,
// or DISCONNECT" [MQTT-3.1.2-29].
func (s *slot) quietly(ack *harness.Ack, what string) error {
	if s.quiet && slices.ContainsFunc(ack.Props, func(q harness.Property) bool {
		return q.ID == harness.ReasonString || q.ID == harness.UserProperty
	}) {
		return fmt.Errorf("%s %v to %s, Request Problem Information 0: no Reason String or User Property (MQTT-3.1.2-29)", harness.Name(ack.Kind), ack.Props, what)
	}
	return nil
}

// unanswered counts the answers s's connection is still owed: a PUBACK or PUBREC
// for each pending PUBLISH, a PUBREC for each PUBLISH repeating a held
// identifier, and a PUBCOMP for each PUBREL sent.
func (s *slot) unanswered() int {
	n := len(s.pending) + len(s.unknown)
	for _, x := range s.held {
		n += x.repeats
		if x.released {
			n++
		}
	}
	return n
}

// carry keeps, as s's connection ends or is taken over, what its session's
// QoS 2 exchanges are still owed: a QoS 2 PUBLISH the model predicted taken
// whose PUBREC never came becomes an exchange, which the core may hold or
// not, its PUBLISH resent when the session resumes; a PUBREL whose PUBCOMP
// never came is resent then, its move made or not; and a repeat's PUBREC or
// a PUBCOMP to an unknown identifier is owed no more.
func (s *slot) carry() {
	for id, o := range s.pending {
		if o.p.QoS == 2 && o.m != nil {
			if s.held == nil {
				s.held = map[uint16]*exchange{}
			}
			s.held[id] = &exchange{o: o}
		}
	}
	for _, x := range s.held {
		x.repeats, x.resent = 0, x.resent || x.released
	}
	s.unknown = nil
}

// show is p, for messages: a DISCONNECT by its reason code.
func show(p harness.Packet) string {
	if d, ok := p.(*harness.Disconnect); ok {
		return fmt.Sprintf("DISCONNECT 0x%02X", d.Reason)
	}
	return fmt.Sprintf("%s %+v", harness.Name(p.Type()), p)
}

// answer checks deliveries and pending PUBACKs until another control
// packet arrives.
func answer(s *slot) (harness.Packet, error) {
	for {
		p, err := s.recv()
		if err != nil {
			return nil, err
		}
		if settled, err := s.settle(p); err != nil || !settled {
			return p, err
		}
	}
}

// awaitClose checks deliveries and pending PUBACKs before the orderly
// close, without requiring replies that closing may supersede.
func awaitClose(s *slot) error {
	for {
		p, err := s.recv()
		settled, serr := s.settle(p)
		if serr != nil {
			return serr
		}
		if !settled {
			if err := s.conn.Closed(p, err); err != nil {
				return err
			}
			s.closed()
			return nil
		}
	}
}

// collect checks every answer s's connection is owed (unanswered), allowing
// deliveries between the acknowledgements.
func collect(s *slot) error {
	for s.unanswered() > 0 {
		p, err := s.recv()
		if err != nil {
			return fmt.Errorf("waiting for %d acknowledgements: %w", s.unanswered(), err)
		}
		if settled, err := s.settle(p); err != nil || !settled {
			return cmp.Or(err, fmt.Errorf("%s while %d acknowledgements are owed", show(p), s.unanswered()))
		}
	}
	return s.drain()
}

// publish: a connected client sends one to four PUBLISHes in one write, and
// then mostly reads their answers, else drops its connection, sends
// DISCONNECT or is taken over before they come.
func (f *fuzzer) publish() (bool, error) { return f.publishAs(drawAny) }

// publishAs is publish, its PUBLISHes drawn as kind.
func (f *fuzzer) publishAs(kind draw) (bool, error) {
	// A client connected with an empty client id publishes too, under the
	// identifier assigned it (design.md, CONNECT, 8).
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	var batch []*out
	for n := range 1 + f.rng.IntN(4) {
		o, err := f.newPublish(s, n, kind)
		if err != nil {
			return true, err
		}
		batch = append(batch, o)
		if o.ends {
			break
		}
	}
	last := batch[len(batch)-1]
	then := f.rng.IntN(12)
	if err := f.placeAll(batch); err != nil {
		return true, err
	}
	for _, o := range batch {
		f.note("%s: %s", s.id, o)
	}
	f.expect(batch...)
	if err := send(s, batch...); err != nil {
		return true, err
	}
	switch {
	case last.ends:
		err := closedWith(s, last.reason)
		_ = s.conn.Close() // checked closed by the broker, or the step failed
		s.fire(fmt.Sprintf("the broker closed the connection for the PUBLISH %q QoS %d", last.p.Topic, last.p.QoS))
		s.end(false)
		return true, err
	case then == 0:
		f.note("%s: drops its connection before the answers", s.id)
		err := s.lose()
		if err == nil {
			s.fireTaken("its connection dropped")
		}
		s.end(false)
		return true, err
	case then == 1:
		f.note("%s: DISCONNECT 0x00 before the answers: want the close", s.id)
		return true, f.hangUp(s, &harness.Disconnect{})
	case then == 2 && !s.anon:
		// Only a named client's connection can be taken over: a second
		// CONNECT with an empty client id is given another identifier.
		if err := f.takeOver(s); err != nil || s.conn == nil {
			return true, err
		}
		// Taken over, the new connection has nothing pending; refused,
		// the old one goes on and its answers are still to come.
		return true, collect(s)
	}
	if err := collect(s); err != nil {
		return true, err
	}
	if slices.ContainsFunc(batch, func(o *out) bool { return o.p.QoS == 0 }) {
		// A QoS 0 PUBLISH has no answer, and a refused one is "dropped
		// and counted" (design.md, PUBLISH): the connection goes on.
		return true, pingPast(s)
	}
	return true, nil
}

// windowClients is the most clients one window action keeps busy at once.
const windowClients = 8

// windowMost is the most messages one client sends in a window action.
const windowMost = 10 * receiveMaximum

// window: up to eight connected clients at once each publish 20 to 200
// messages, keeping their QoS 1 ones at the CONNACK's Receive Maximum
// unanswered and sending the next the moment a PUBACK arrives; QoS 0 ones
// go as they come. MQTT 5 3.3.4: "The Client MUST NOT send more than
// Receive Maximum QoS 1 and QoS 2 PUBLISH packets for which it has not
// received PUBACK [...] [MQTT-3.3.4-7]. If it receives more than Receive
// Maximum [...] the Server uses a DISCONNECT packet with Reason Code 0x93":
// never one at exactly Receive Maximum. MQTT 3.1.1 has no Receive Maximum;
// the same window is within the broker's.
//
// Every PUBLISH is drawn first, in order, so the step is the seed's; then
// each client sends its own on a goroutine of the runner's, so the broker
// serves them together, as it serves many clients, and which PUBACK comes
// first is the broker's timing, never the run's sequence.
func (f *fuzzer) window() (bool, error) { return f.burst(drawAccepted) }

// flood is window with bodies from half of max_packet_size up to its limit,
// together more than max_incoming_memory: whenever the store writes slower
// than the readers read, the incoming budget is full, and readers wait for
// bytes, given them in the order asked once bytes come back (design.md,
// Message memory).
func (f *fuzzer) flood() (bool, error) { return f.burst(drawLarge) }

// crowd is window with every PUBLISH on a channel with a max_bytes, so
// many publishers reach its limit together: those it takes must keep
// within it, counting reservations as well as commits (design.md,
// Channels: "A hard byte limit counts committed charges and reservations
// for accepted writes").
func (f *fuzzer) crowd() (bool, error) { return f.burst(drawCrowd) }

// burst is window and flood, its PUBLISHes drawn as kind.
func (f *fuzzer) burst(kind draw) (bool, error) {
	var clients []*slot
	for range windowClients {
		s := f.pick(func(s *slot) bool { return s.conn != nil && !slices.Contains(clients, s) })
		if s == nil {
			break
		}
		clients = append(clients, s)
	}
	if len(clients) == 0 {
		return false, nil
	}
	batches := make([][]*out, len(clients))
	for i, s := range clients {
		batches[i] = make([]*out, receiveMaximum+f.rng.IntN(windowMost-receiveMaximum+1))
		for n := range batches[i] {
			var err error
			if batches[i][n], err = f.newPublish(s, i*windowMost+n, kind); err != nil {
				return true, err
			}
		}
	}
	// The clients' PUBLISHes reach the core in any order: all are counted
	// against a channel's max_bytes before any is predicted.
	if err := f.placeAll(slices.Concat(batches...)); err != nil {
		return true, err
	}
	for i, s := range clients {
		f.expect(batches[i]...)
		f.note("%s: %d PUBLISHes from %s on, QoS 1 ones kept at Receive Maximum %d unanswered: want PUBACK 0x00 to each, or 0x97 where a channel is full, never DISCONNECT 0x93",
			s.id, len(batches[i]), batches[i][0], receiveMaximum)
	}
	errs := make([]error, len(clients))
	var wg, publishing sync.WaitGroup
	publishing.Add(len(clients))
	for i, s := range clients {
		wg.Go(func() { errs[i] = pump(s, batches[i], &publishing) })
	}
	wg.Wait()
	return true, errors.Join(errs...)
}

// pump sends batch on s's connection, keeping its QoS 1 PUBLISHes, with
// its session's held QoS 2 exchanges (design.md, PUBLISH: "A held QoS 2
// identifier takes exactly one until its PUBCOMP"), at Receive Maximum
// unanswered, until every one is answered, then pings.
func pump(s *slot, batch []*out, publishing *sync.WaitGroup) error {
	finished := sync.OnceFunc(publishing.Done)
	defer finished() // a failed publisher also releases the other peers
	// The connection's byte reader keeps draining even while this pump
	// blocks writing or waits for another publisher. This goroutine alone
	// checks its packets and changes its connection's model observations.
	for sent := 0; ; {
		for ; sent < len(batch) && (batch[sent].p.QoS == 0 || len(s.pending)+len(s.held) < receiveMaximum); sent++ {
			if err := send(s, batch[sent]); err != nil {
				return fmt.Errorf("%s: %w", s.id, err)
			}
		}
		if len(s.pending) == 0 {
			break
		}
		p, err := s.recv()
		if err != nil {
			return fmt.Errorf("%s: waiting for a PUBACK with %d QoS 1 PUBLISHes unanswered: %w", s.id, len(s.pending), err)
		}
		settled, err := s.settle(p)
		switch d, ok := p.(*harness.Disconnect); {
		case err != nil:
			return fmt.Errorf("%s: %w", s.id, err)
		case ok:
			return fmt.Errorf("%s: DISCONNECT 0x%02X, the client never having had more than the CONNACK's Receive Maximum of %d QoS 1 PUBLISHes unanswered (%d now): MQTT-3.3.4-7 allows a client exactly that many, 0x93 is for more",
				s.id, d.Reason, receiveMaximum, len(s.pending))
		case !settled:
			return fmt.Errorf("%s: %s with %d QoS 1 PUBLISHes unanswered, want a PUBACK", s.id, show(p), len(s.pending))
		}
	}
	// A PINGRESP proves the broker took this peer's whole batch, including
	// QoS 0. Keep reading until every publisher has crossed that barrier:
	// another peer's later PUBLISHes can still deliver here. The second
	// PINGRESP and drain follow only after all batches were taken.
	if err := pingReply(s); err != nil {
		return fmt.Errorf("%s: %w", s.id, err)
	}
	finished()
	publishing.Wait()
	if err := pingPast(s); err != nil {
		return fmt.Errorf("%s: %w", s.id, err)
	}
	return nil
}

// overfill: a connected MQTT 5 client sends one to three QoS 1 PUBLISHes
// past the CONNACK's Receive Maximum in one write. MQTT 5 3.3.4: a server
// receiving "more than Receive Maximum QoS 1 and QoS 2 PUBLISH packets
// where it has not sent a PUBACK" sends DISCONNECT 0x93; one that has sent
// PUBACKs before the last arrives may take them all. Either way the client
// then ends its connection, so what follows does not hang on which came.
func (f *fuzzer) overfill() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil && s.conn.Version == harness.V5 })
	if s == nil {
		return false, nil
	}
	batch := make([]*out, receiveMaximum+1+f.rng.IntN(3))
	for i := range batch {
		var err error
		if batch[i], err = f.newPublish(s, i, drawQoS1); err != nil {
			return true, err
		}
	}
	if err := f.placeAll(batch); err != nil {
		return true, err
	}
	f.note("%s: %d QoS 1 PUBLISHes from %s on in one write, past Receive Maximum %d: want PUBACK 0x00 to each (0x97 where a channel is full), or DISCONNECT 0x93 then the close",
		s.id, len(batch), batch[0], receiveMaximum)
	f.expect(batch...)
	err := send(s, batch...)
	closed := false
	for err == nil && len(s.pending) > 0 {
		var p harness.Packet
		if p, err = s.recv(); err != nil {
			break
		}
		var settled bool
		if settled, err = s.settle(p); settled || err != nil {
			continue
		}
		if d, ok := p.(*harness.Disconnect); ok && d.Reason == 0x93 {
			err, closed = s.conn.AwaitClose(), true
			break
		}
		err = fmt.Errorf("%s with %d QoS 1 PUBLISHes unanswered, want a PUBACK or DISCONNECT 0x93", show(p), len(s.pending))
	}
	how := "the broker closed the connection with DISCONNECT 0x93"
	if err == nil && !closed {
		// Every PUBLISH answered: the client ends its connection.
		if err = s.lose(); err == nil {
			s.fireTaken("its connection dropped")
		}
	}
	_ = s.conn.Close() // checked closed by the broker, or the client ends it here
	s.fire(how)
	s.end(false)
	return true, err
}

// exactlyOnce: a connected client whose user may use QoS 2 (design.md,
// Access and reload: a features rule may deny qos2) and whose session is
// its id's sends one to three QoS 2 PUBLISHes in one write, its session
// holding at most maxExchanges, and reads their PUBRECs; then it settles the
// exchanges its session holds (releaseHeld).
func (f *fuzzer) exactlyOnce() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil && s.mayHold(0) && !denied(s.user, "qos2") })
	if s == nil {
		return false, nil
	}
	var batch []*out
	for n := range min(1+f.rng.IntN(3), maxExchanges-len(s.held)) {
		o, err := f.newPublish(s, n, drawQoS2)
		if err != nil {
			return true, err
		}
		batch = append(batch, o)
	}
	if len(batch) > 0 {
		if err := f.placeAll(batch); err != nil {
			return true, err
		}
		for _, o := range batch {
			f.note("%s: %s, held until its PUBREL", s.id, o)
		}
		// Not expected yet: a QoS 2 publication "enters on PUBREL"
		// (design.md, PUBLISH, the table), so a record of it before then is
		// an error (releaseHeld expects it).
		if err := send(s, batch...); err != nil {
			return true, err
		}
		if err := collect(s); err != nil {
			return true, err
		}
	}
	return true, f.releaseHeld(s)
}

// releaseHeld sends, in one write, for each exchange s's session holds, in
// identifier order so the step is the seed's, at random: its PUBLISH again,
// DUP 1 or 0, which gets PUBREC 0x00 again (design.md, PUBLISH: "While a
// QoS 2 PUBLISH's identifier is held awaiting PUBREL, any later PUBLISH
// with that identifier gets PUBREC again. It is not stored twice, or charged
// twice against its channel's size, whatever its DUP flag (MQTT-4.3.3-10)"); its PUBREL, which gets PUBCOMP
// 0x00 once the publication is in its channel; or nothing, the exchange
// left held. Now and then a PUBREL for an identifier no exchange holds
// follows, which gets PUBCOMP 0x92. It then reads every answer.
func (f *fuzzer) releaseHeld(s *slot) error {
	var wire []byte
	add := func(p harness.Packet) error {
		b, err := harness.Encode(p, s.conn.Version)
		wire = append(wire, b...)
		return err
	}
	for _, id := range slices.Sorted(maps.Keys(s.held)) {
		x := s.held[id]
		var p harness.Packet
		switch f.rng.IntN(4) {
		case 0:
			repeat := x.o.again(f.rng.IntN(2) == 0)
			x.repeats++
			f.note("%s: PUBLISH %q again under held identifier %d, DUP %v: want PUBREC 0x00, nothing stored twice", s.id, repeat.Topic, id, repeat.Dup)
			p = repeat
		case 1, 2:
			p = f.releasing(s, id, x)
		default:
			continue
		}
		if err := add(p); err != nil {
			return err
		}
	}
	if f.rng.IntN(6) == 0 {
		id := s.nextID()
		if s.unknown == nil {
			s.unknown = map[uint16]bool{}
		}
		s.unknown[id] = true
		f.note("%s: PUBREL %d, an identifier no exchange holds: want PUBCOMP 0x92 (MQTT 3.1.1: PUBCOMP)", s.id, id)
		if err := add(&harness.Ack{Kind: harness.PUBREL, ID: id}); err != nil {
			return err
		}
	}
	if len(wire) == 0 {
		return nil
	}
	if err := s.conn.SendRaw(wire); err != nil {
		return err
	}
	return collect(s)
}

// releasing marks x, the exchange s's session holds under id, released,
// and expects its publication, which the PUBREL it returns makes enter its
// channel (design.md, PUBLISH, the table: "The publication enters on
// PUBREL").
func (f *fuzzer) releasing(s *slot, id uint16, x *exchange) harness.Packet {
	x.released = true
	f.expect(x.o)
	f.note("%s: PUBREL %d: want PUBCOMP 0x00, %q entering its channel", s.id, id, x.o.m.tag)
	return &harness.Ack{Kind: harness.PUBREL, ID: id}
}

// resumeHeld resends, on s's new connection keeping its session, what its
// QoS 2 exchanges are still owed, in identifier order: each PUBLISH whose
// PUBREC never came, DUP 1, and each PUBREL whose PUBCOMP never came (MQTT 5
// 4.4: the Client "MUST resend any unacknowledged PUBLISH packets (where QoS
// > 0) and PUBREL packets using their original Packet Identifiers"
// [MQTT-4.4.0-1]; design.md, CONNECT, 9: "Held QoS 2 exchanges wait for
// their PUBREL"). It reads their answers: a PUBREC as to its first copy,
// the core holding it or taking it as new; a PUBCOMP (complete). Where the
// new connection's user is denied qos2, its CONNACK carries Maximum QoS 1,
// and the first PUBLISH resent, at QoS 2, ends the connection with 0x9B
// (MQTT 3.1.1: the close), whatever the core holds under its identifier:
// MQTT 5 3.2.2.3.4, "It is a Protocol Error if the Server receives a
// PUBLISH packet with a QoS greater than the Maximum QoS it specified. In
// this case use DISCONNECT with Reason Code 0x9B"; design.md, DISCONNECT,
// Reasons Sagüin sends: "0x9B [...] for [...] QoS 2 [...] its CONNACK
// withheld"; Flows: "Checks proceed through framing, protocol recognition
// and packet validity, admission policy, then storage", so a protocol error
// comes before the PUBREC a PUBLISH repeating a held identifier gets
// (PUBLISH). Nothing is sent after it, and the exchange stays held.
func (f *fuzzer) resumeHeld(s *slot) error {
	var wire []byte
	ends := false
	for _, id := range slices.Sorted(maps.Keys(s.held)) {
		x := s.held[id]
		var p harness.Packet
		switch {
		case x.released:
			f.note("%s: PUBREL %d again, its PUBCOMP lost with its connection: want PUBCOMP 0x00, or 0x92 where that PUBREL's move was made", s.id, id)
			p = &harness.Ack{Kind: harness.PUBREL, ID: id}
		case !x.answered:
			delete(s.held, id)
			x.o.p = x.o.again(true)
			if s.pending == nil {
				s.pending = map[uint16]*out{}
			}
			s.pending[id] = x.o
			ends = denied(s.user, "qos2")
			if ends {
				f.note("%s: %s again, its PUBREC lost with its connection: want DISCONNECT 0x9B (MQTT 3.1.1: the close), its user denied qos2", s.id, x.o)
			} else {
				f.note("%s: %s again, its PUBREC lost with its connection", s.id, x.o)
			}
			p = x.o.p
		default:
			continue
		}
		b, err := harness.Encode(p, s.conn.Version)
		if err != nil {
			return err
		}
		wire = append(wire, b...)
		if ends {
			break
		}
	}
	if len(wire) == 0 {
		return nil
	}
	if err := s.conn.SendRaw(wire); err != nil {
		return err
	}
	if !ends {
		return collect(s)
	}
	err := closedWith(s, 0x9B)
	_ = s.conn.Close() // checked closed by the broker, or the step failed
	s.fire("the broker closed the connection for a QoS 2 PUBLISH resent, its user denied qos2")
	s.end(false)
	return err
}
