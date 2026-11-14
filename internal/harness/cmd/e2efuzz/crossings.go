package main

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Records leaving for the bridge's peer (design.md, Bridges, Out): the
// run's clients publish on the topics of the bridge's out rules
// (crossBridge), and the peer the run serves (bridges.go) checks each
// PUBLISH the bridge sends it (crossedIn): the record it carries, known by
// its payload's tag, on its rule's template ("In a topic template, $1, $2
// and so on stand for the filter's + levels"), at the lower of its QoS and
// the rule's ("An outbound record crosses at the lower of its own QoS and
// the rule's"), with its publisher's properties and one saguin-id, its record's
// id, and no other saguin- property ("A record leaves with no saguin-
// property except saguin-id. Every record a bridge sends carries saguin-id
// with its record's id, $broadcast records included"), RETAIN only from
// $broadcast; to a peer whose CONNACK has Retain Available 0, a retained
// record first sent on that link with RETAIN 0 and at QoS 0, and one the
// peer's kept session still holds in flight sent again at its first
// sending's QoS with RETAIN 0 (MQTT-4.4.0-1, MQTT-3.2.2-14) ("RETAIN
// crosses only on $broadcast records. To a peer whose CONNACK has Retain
// Available 0, a retained record crosses with RETAIN 0 and at QoS 0, so no
// resend of it can differ from its first send, and the first such record on
// each link is one WARN line"); never a record that arrived over
// the bridge ("A record that arrived over a bridge never leaves over any
// bridge"), nor one whose topic breaks a limit once its template is applied
// or whose packet exceeds the peer's Maximum Packet Size ("An invalid
// outbound result is skipped [...] An outbound packet exceeding the peer's
// Maximum Packet Size is skipped in the same way"). Each rule's records of
// one channel and one topic at the peer come in the order they were stored,
// each once the one before it is settled ("One record per topic at the peer
// is in flight at a time"), and at most the peer's Receive Maximum unanswered
// ("at most the lower of the peer's Receive Maximum and receive_maximum are
// in flight"). The peer answers each at QoS 1 with PUBACK 0x00, a refusal
// ("A record the peer refuses is skipped"), or, until the next step that
// publishes such records or the next checkpoint, nothing; at QoS 2 with
// PUBREC 0x00, a refusal, or for a while nothing, and the bridge's PUBREL,
// which must follow a PUBREC 0x00 under its packet identifier
// (MQTT-4.3.3-4), with PUBCOMP 0x00 or for a while nothing. After the run
// drops the link, or stops the broker and starts it again (restart), the
// records the peer left unsettled come again first: where the CONNACK has
// Session Present 1, each exchange taken up under its own packet identifier,
// in the order the records were first sent ("They are resent in seq
// order", storage.md, bridge_inflight), a PUBLISH again with DUP 1, or
// PUBREL where the peer has received it (MQTT-4.4.0-1, MQTT-4.3.3-6); as new
// messages, DUP 0, where it has 0 ("Its unacknowledged records are then
// sent again as new messages, from the cursor"). At each checkpoint
// (settle), the peer answers what it held
// back, and every record an out rule owes it has reached it within the
// run's timeout: no loss, but a QoS 0 sending a dropped link may have taken
// with it, a retained record's to a peer with Retain Available 0 included.
// Left out: the counters (the conformance tests' TestOutBridgeSeries
// and TestOutSkipsAndCountsWhatItCannotSend), expiry (the run's Message
// Expiry is a day), a queue's records and the peer's Maximum QoS.

// outRules are the bridge's out rules, each a durable consumer of what its
// filter matches here. o1/+/# lands on $broadcast, at QoS 2, its template
// adding three bytes and a level, so a topic within the limits here may
// break one at the peer ("Topic limits are checked after substitution").
// ca/o1/#
// reaches the configured channels ca and cab (ca/+/b), at QoS 0. Its topics
// at the peer, near/ca/#, are apart from o1/+/#'s, far/+/x/# (config.md,
// bridges.<name>: "Their topics may not overlap either"). bridged/# takes
// what the in rules land on bridged/ (bridgeRules), which never leaves, and
// what the run's clients publish there, which does. No other action of the
// run publishes on their topics: o1 holds a digit, and bridged is longer
// than the levels the run's topics draw (fuzzer.topic, channelTopic).
var outRules = []harness.BridgeRule{
	{Filter: "o1/+/#", Topic: "far/$1/x/$#", Direction: "out", QoS: 2},
	{Filter: "ca/o1/#", Topic: "near/ca/$#", Direction: "out", QoS: 0},
	{Filter: "bridged/#", Topic: "back/$#", Direction: "out", QoS: 1},
}

// The peer's CONNACK caps what the bridge sends it (MQTT 5 3.2.2.3): a
// Receive Maximum below the bridge's receive_maximum, config.md's 20, so
// the window is the peer's; and a Maximum Packet Size the run's large
// crossings pass, its small ones well within it.
const (
	peerReceiveMaximum = 4
	peerPacketSize     = 1024
)

// crossed is a record an out rule takes to the peer: what the peer must
// receive of it, and what it has.
type crossed struct {
	m     *message
	rule  int    // its out rule, in outRules
	topic string // its topic at the peer, its rule's template applied (landed)
	qos   byte   // the lower of its own QoS and its rule's (want)
	// retain is its RETAIN at a peer allowing retained messages: its own
	// on $broadcast, 0 on a configured channel (placeAll); where it is set,
	// a peer whose CONNACK has Retain Available 0 takes it at QoS 0 (want).
	retain bool
	// skip is the cause under which the bridge must skip it, topic_invalid
	// or too_large; "" where it crosses.
	skip string
	// drops and link are the links the run had dropped or a restart ended
	// (bridging.interruptions), and the links up, when it was sent;
	// unretained, the dropped ones whose CONNACK had Retain Available 0
	// (bridging.unretainedDrops).
	drops, link, unretained int
	// settled: the peer answered it, or received it at QoS 0; id is the
	// packet identifier it last came under, on the link on, while that
	// link awaits the peer's answer.
	settled bool
	id      uint16
	on      *harness.PeerLink
	// sent orders its sending under id among the bridge's (bridging.sendings),
	// the order a kept session's exchanges are taken up in (resentInOrder).
	// At QoS 2, rec: the peer answered that PUBLISH with PUBREC 0x00; rel:
	// the bridge's PUBREL for it came, on the link relOn.
	sent     int
	rec, rel bool
	relOn    *harness.PeerLink
}

// lane is c's order: its rule's, on its channel, to its topic at the peer.
func (c *crossed) lane() string {
	return strconv.Itoa(c.rule) + " " + c.m.channel + " " + c.topic
}

// excused reports whether c is owed to the peer no more: the bridge must
// skip it, or it went at QoS 0 and a link the run dropped or a restart ended
// since it was sent may have taken it, a retained record on a link whose
// CONNACK had Retain Available 0 too.
func (b *bridging) excused(c *crossed) bool {
	return c.skip != "" || (c.qos == 0 && b.interruptions() > c.drops) || (c.retain && b.unretainedDrops > c.unretained)
}

// want is the QoS and RETAIN c must come with on a link whose CONNACK had
// Retain Available retainAvailable, carried where it comes again as the
// peer's kept session's in-flight record (MQTT-4.4.0-1). Its own and its
// rule's lower QoS, and its RETAIN where the peer allows retained messages,
// except that a retained record sent first to a peer whose CONNACK has
// Retain Available 0 comes with RETAIN 0 at QoS 0 (design.md, Bridges: "To
// a peer whose CONNACK has Retain Available 0, a retained record crosses
// with RETAIN 0 and at QoS 0"). Sent again on such a link, a record in
// flight since an earlier link keeps its packet identifier and QoS and
// comes with RETAIN 0 (MQTT-3.2.2-14: "A Client receiving Retain Available
// set to 0 from the Server MUST NOT send a PUBLISH packet with the RETAIN
// flag set to 1").
func (c *crossed) want(retainAvailable, carried bool) (qos byte, retain bool) {
	if c.retain && !retainAvailable && !carried {
		return 0, false
	}
	return c.qos, c.retain && retainAvailable
}

// landed is topic as rule's template makes it at the peer (design.md,
// Bridges: "In a topic template, $1, $2 and so on stand for the filter's +
// levels. Each must name a level the filter has and may occur anywhere in a
// level. The $# stands for the filter's # and must be the topic's whole
// last level. Where the filter's # matches no level, $# and the / before it
// are both left out").
func landed(rule harness.BridgeRule, topic string) string {
	levels := strings.Split(topic, "/")
	var plus []string
	rest := ""
	for i, level := range strings.Split(rule.Filter, "/") {
		switch level {
		case "+":
			plus = append(plus, levels[i])
		case "#":
			rest = strings.Join(levels[i:], "/")
		}
	}
	var out []string
	for level := range strings.SplitSeq(rule.Topic, "/") {
		if level == "$#" {
			if rest != "" {
				out = append(out, rest)
			}
			continue
		}
		for i := len(plus); i > 0; i-- {
			level = strings.ReplaceAll(level, "$"+strconv.Itoa(i), plus[i-1])
		}
		out = append(out, level)
	}
	return strings.Join(out, "/")
}

// own is ps without saguin-id and Message Expiry: the properties of a
// publisher's that cross unaltered (design.md, Sagüin properties: "Nothing
// a publisher sends is altered"), the Message Expiry the time remaining
// (Delivery, Message Expiry).
func own(ps harness.Properties) harness.Properties {
	return slices.DeleteFunc(slices.Clone(ps), func(p harness.Property) bool {
		return p.ID == harness.MessageExpiryInterval || (p.ID == harness.UserProperty && p.Str == "saguin-id")
	})
}

// userOrder is ps's User Properties, in their order.
func userOrder(ps harness.Properties) []string {
	var out []string
	for _, p := range ps {
		if p.ID == harness.UserProperty {
			out = append(out, propString(p))
		}
	}
	return out
}

// size is the length of c's PUBLISH as the bridge sends it, carrying props,
// its publisher's, with one saguin-id: the publisher's or a UUIDv7's 36
// characters. The run's crossings are well within peerPacketSize or well
// past it (newCrossing), so where the bridge puts its saguin-id, or a second
// one its publisher sent, changes nothing.
func (c *crossed) size(props harness.Properties) (int, error) {
	id := cmp.Or(c.m.id, strings.Repeat("0", 36))
	p := &harness.Publish{QoS: c.qos, Topic: c.topic, Props: append(slices.DeleteFunc(slices.Clone(props), func(p harness.Property) bool {
		return p.ID == harness.UserProperty && p.Str == "saguin-id"
	}), harness.User("saguin-id", id)), Payload: c.m.published()}
	if c.qos > 0 {
		p.ID = 1
	}
	wire, err := harness.Encode(p, harness.V5)
	return len(wire), err
}

// crossBridge: once a link is up, the peer first answers what it held back
// (release), then a connected client publishes one to four records on the
// out rules' topics (newCrossing), at QoS 0, 1 or 2, in one write, which
// the model expects as it does any PUBLISH, a QoS 2 one once its PUBREL is
// sent ("The publication enters on PUBREL", releasing), and the peer as it
// crosses (cross); the step then reads their answers, a ping bounding the
// intake of those at QoS 0, and releases its QoS 2 ones
// (releaseCrossings).
func (f *fuzzer) crossBridge() (bool, error) {
	b := f.bridge
	if b == nil {
		return false, nil
	}
	// The users but bo may write every topic (rights); di may keep no
	// retained value, which newCrossing leaves out.
	s := f.pick(func(s *slot) bool { return s.conn != nil && s.user != restricted })
	if s == nil {
		return false, nil
	}
	if _, err := b.up(nil, f.timeout); err != nil {
		return true, err
	}
	b.release()
	var batch []*out
	for n := range 1 + f.rng.IntN(4) {
		o, err := f.newCrossing(s, n)
		if err != nil {
			return true, err
		}
		batch = append(batch, o)
	}
	if err := f.placeAll(batch); err != nil {
		return true, err
	}
	f.expect(slices.DeleteFunc(slices.Clone(batch), func(o *out) bool { return o.p.QoS == 2 })...)
	if err := b.cross(batch); err != nil {
		return true, err
	}
	for _, o := range batch {
		f.note("%s: %s, crossing to the bridge's peer as %s", s.id, o, b.crossings[o.m.tag])
	}
	if err := send(s, batch...); err != nil {
		return true, err
	}
	if err := collect(s); err != nil {
		return true, err
	}
	if slices.ContainsFunc(batch, func(o *out) bool { return o.p.QoS == 0 }) {
		if err := pingPast(s); err != nil {
			return true, err
		}
	}
	return true, f.releaseCrossings(s, batch)
}

// releaseCrossings sends, in one write, the PUBREL of each QoS 2 PUBLISH of
// batch, s's, that got PUBREC 0x00, each crossing first joining its lane
// (entered), and reads their PUBCOMPs.
func (f *fuzzer) releaseCrossings(s *slot, batch []*out) error {
	var wire []byte
	for _, o := range batch {
		x := s.held[o.p.ID]
		if o.p.QoS != 2 || x == nil || x.o != o {
			continue
		}
		f.bridge.entered(o.m)
		b, err := harness.Encode(f.releasing(s, o.p.ID, x), s.conn.Version)
		if err != nil {
			return err
		}
		wire = append(wire, b...)
	}
	if len(wire) == 0 {
		return nil
	}
	if err := s.conn.SendRaw(wire); err != nil {
		return err
	}
	return collect(s)
}

// entered puts the crossing of m, a QoS 2 message whose PUBREL is about to
// be sent, in its lane, after every record of the lane before it: its
// publication enters its channel on that PUBREL (design.md, PUBLISH, the
// table).
func (b *bridging) entered(m *message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.crossings[m.tag]; c != nil {
		b.lanes[c.lane()] = append(b.lanes[c.lane()], c)
	}
}

// newCrossing draws s's n-th PUBLISH of the step, accepted, at QoS 0 or 1,
// or 2 where s's user may use QoS 2 and its session may hold the exchange
// (mayHold), on an out rule's topics: o1/+/#'s mostly of two or three
// levels, else four, one
// over max_topic_levels at the peer, or 25 to 27 bytes, over
// max_topic_length there; ca/o1/#'s on ca, or cab where its third level is
// b; bridged/#'s. Its payload is mostly small, else over peerPacketSize;
// in MQTT 5 it carries the properties any accepted PUBLISH may
// (messageProps), never a Topic Alias, which would map a topic of these
// for the slot's other PUBLISHes.
func (f *fuzzer) newCrossing(s *slot, n int) (*out, error) {
	r := f.rng
	var topic string
	switch r.IntN(len(outRules)) {
	case 0:
		levels := []string{"o1", [...]string{"a", "b", "ca"}[r.IntN(3)]}
		if r.IntN(6) == 0 {
			prefix := strings.Join(levels, "/") + "/"
			levels = append(levels, f.word(maxTopicLength-2-len(prefix), maxTopicLength-len(prefix)))
		} else {
			for range r.IntN(maxTopicLevels - 1) {
				levels = append(levels, f.word(1, 2))
			}
		}
		topic = strings.Join(levels, "/")
	case 1:
		levels := append(make([]string, 0, maxTopicLevels), "ca", "o1")
		for range r.IntN(3) {
			levels = append(levels, [...]string{"x", "b", "y"}[r.IntN(3)])
		}
		topic = strings.Join(levels, "/")
	default:
		levels := append(make([]string, 0, maxTopicLevels), "bridged")
		for range r.IntN(maxTopicLevels) {
			levels = append(levels, f.word(1, 3))
		}
		topic = strings.Join(levels, "/")
	}
	p := &harness.Publish{QoS: byte(r.IntN(3)), Retain: r.IntN(4) == 0 && !denied(s.user, "retained"), Topic: topic}
	if p.QoS == 2 && (denied(s.user, "qos2") || !s.mayHold(n+1) || !s.carries(drawAccepted, topic, 2)) {
		p.QoS = 1
	}
	o := &out{p: p, quiet: s.quiet}
	o.m = &message{step: f.step, publisher: s.clientID, hidden: s.hidden, topic: topic, qos: p.QoS, retain: p.Retain}
	if s.conn.Version == harness.V5 {
		f.messageProps(o)
	}
	if p.QoS > 0 {
		p.ID = s.nextID()
	}
	least, most := 0, 80
	if r.IntN(6) == 0 {
		least, most = peerPacketSize+100, peerPacketSize+500
	}
	return o, f.fill(o, n, least, most, letters, s.conn.Version)
}

// cross keeps each message of batch an out rule takes, before its PUBLISH
// is sent: what the peer must receive of it, or why the bridge must skip
// it. Each joins its lane, but a QoS 2 one, which joins it as it enters its
// channel (entered).
func (b *bridging) cross(batch []*out) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, o := range batch {
		m := o.m
		i := slices.IndexFunc(outRules, func(r harness.BridgeRule) bool { return m != nil && matchesFilter(r.Filter, m.topic) })
		if i < 0 {
			continue
		}
		rule := outRules[i]
		c := &crossed{m: m, rule: i, topic: landed(rule, m.topic), qos: min(m.qos, rule.QoS), retain: m.retain,
			drops: b.interruptions(), link: b.stats.links, unretained: b.unretainedDrops}
		size, err := c.size(o.p.Props)
		switch {
		case err != nil:
			return fmt.Errorf("bridge: encoding %s as it crosses: %w", m.tag, err)
		case len(c.topic) > maxTopicLength || strings.Count(c.topic, "/") >= maxTopicLevels:
			c.skip = "topic_invalid"
		case size > peerPacketSize:
			c.skip = "too_large"
		}
		b.crossings[m.tag] = c
		if m.qos < 2 {
			b.lanes[c.lane()] = append(b.lanes[c.lane()], c)
		}
		b.open = append(b.open, c)
		b.stats.crossings++
		if c.skip != "" {
			b.stats.skipped++
		}
	}
	return nil
}

// String is c, for the step log.
func (c *crossed) String() string {
	if c.skip != "" {
		return fmt.Sprintf("%q, skipped as %s", c.topic, c.skip)
	}
	return fmt.Sprintf("%q at QoS %d, retain %v", c.topic, c.qos, c.retain)
}

// relink follows a new link up whose CONNACK had Session Present present
// and Retain Available retain: with present, the records the peer left
// unsettled on the links before are still in flight from the bridge, to
// come again first under their own packet identifiers (MQTT-4.4.0-1), as
// may a PUBREL whose PUBCOMP the peer sent as a link ended (completed);
// without, the bridge discarded them, to send them again as new messages
// (design.md, Bridges: "When the peer's CONNACK has Session Present 0, the
// bridge discards its in-flight state for the peer (MQTT-3.2.2-5)").
func (b *bridging) relink(present, retain bool) {
	carried := map[uint16]*crossed{}
	if present {
		maps.Copy(carried, b.carried)
		maps.Copy(carried, b.inflight)
	} else {
		clear(b.completed)
	}
	for _, c := range b.inflight {
		c.on = nil
	}
	b.carried, b.inflight, b.withheld, b.lastResent = carried, map[uint16]*crossed{}, nil, 0
	b.sessionPresent, b.retainAvailable = present, retain
}

// crossedIn checks p, a PUBLISH the bridge sent on link l, the link up,
// against the record it carries, and answers it.
func (b *bridging) crossedIn(l *harness.PeerLink, p *harness.Publish) {
	tag, _, _ := strings.Cut(string(p.Payload), " ")
	c := b.crossings[tag]
	switch {
	case c == nil:
		b.fail("PUBLISH %q of %.40q reached the peer, the record of no client's PUBLISH an out rule takes: a record that arrived over a bridge never leaves over any bridge (design.md, Bridges)", p.Topic, p.Payload)
		return
	case c.skip != "":
		b.fail("PUBLISH %q of %s reached the peer; want it skipped as %s (design.md, Bridges)", p.Topic, c.m.tag, c.skip)
		return
	}
	carried := slices.Contains(slices.Collect(maps.Values(b.carried)), c)
	if err := c.check(p, b.retainAvailable, carried); err != nil {
		b.fail("%v", err)
	}
	b.inOrder(l, c, p)
	if p.QoS == 0 {
		if !c.settled {
			b.stats.crossed++
		}
		c.settled = true
		return
	}
	if !p.Dup {
		b.sendings++
		c.sent, c.rec, c.rel, c.relOn = b.sendings, false, false, nil
	}
	delete(b.completed, p.ID) // a PUBLISH under it: the bridge had the PUBCOMP (MQTT 5 4.3.3)
	b.inflight[p.ID], c.id, c.on = c, p.ID, l
	b.answerSoon(c, false)
}

// answerSoon has the peer answer c, in flight on its link, whose PUBLISH,
// or with release its PUBREL, has just come. A PUBLISH repeating one the
// peer answered with PUBREC 0x00 gets PUBREC 0x00 again (MQTT 5 4.3.3: the
// receiver "MUST acknowledge any subsequent PUBLISH packet with the same
// Packet Identifier by sending a PUBREC" [MQTT-4.3.3-10]). While the peer
// holds back its answers (holding), it answers about half the QoS 2
// PUBLISHes with PUBREC 0x00, so their PUBRELs come, and nothing else.
// Otherwise it answers with success where the model settles c or awaits
// every record (settling), and mostly so too, and else a PUBLISH with a
// refusal, or for now nothing (withheld).
func (b *bridging) answerSoon(c *crossed, release bool) {
	n := b.rng.IntN(10)
	switch {
	case !release && c.rec:
		b.answer(c, 0x00)
	case b.holding && (release || c.qos == 1 || n%2 != 0):
		b.withhold(c)
	case b.holding || c.settled || b.settling || n > 1:
		b.answer(c, 0x00)
	case n == 0 && !release:
		b.answer(c, [...]byte{0x80, 0x83, 0x87, 0x97, 0x99}[b.rng.IntN(5)])
	default:
		b.withhold(c)
	}
}

// withhold has the peer hold back its answer to c, in flight on its link,
// until it releases what it held back (release).
func (b *bridging) withhold(c *crossed) {
	b.withheld = append(b.withheld, c)
	b.stats.withheldOut++
}

// releasedIn checks p, a PUBREL the bridge sent on link l, the link up: it
// must name a QoS 2 record the peer answered with PUBREC 0x00 (MQTT 5 4.3.3:
// the sender "MUST send a PUBREL packet when it receives a PUBREC packet
// from the receiver with a Reason Code value less than 0x80. This PUBREL
// packet MUST contain the same Packet Identifier as the original PUBLISH
// packet [MQTT-4.3.3-4]"), in flight on l and not released on it before
// (design.md, Delivery: "While connected, the broker starts no resend"), or
// one the peer's kept session holds from an earlier link, taken up again in
// its order (MQTT-4.4.0-1; resentInOrder), its PUBCOMP still owed or sent
// as that link ended (completed). The peer answers it with PUBCOMP 0x00, or
// for now nothing.
func (b *bridging) releasedIn(l *harness.PeerLink, p *harness.Ack) {
	b.stats.releases++
	c, earlier := b.inflight[p.ID], false
	switch {
	case c != nil:
	case b.carried[p.ID] != nil:
		c, earlier = b.carried[p.ID], true
	case b.completed[p.ID] != nil && b.completed[p.ID].relOn != l:
		c, earlier = b.completed[p.ID], true
	}
	switch {
	case c == nil || !c.rec || (c.on != l && !earlier):
		b.fail("PUBREL %d on link %d, Session Present %v, naming no QoS 2 record in flight that the peer answered with PUBREC 0x00 (MQTT-4.3.3-4, MQTT-4.4.0-1; design.md, Bridges: \"When the peer's CONNACK has Session Present 0, the bridge discards its in-flight state for the peer\")",
			p.ID, b.stats.links, b.sessionPresent)
		return
	case p.Reason != 0x00:
		b.fail("PUBREL %d 0x%02X to %s, which the peer answered with PUBREC 0x00; want 0x00", p.ID, p.Reason, c.m.tag)
	case c.relOn == l:
		b.fail("PUBREL %d to %s came twice on one link: while connected, the broker starts no resend (design.md, Delivery, One mechanism for every channel)", p.ID, c.m.tag)
	case earlier && !b.sessionPresent:
		b.fail("PUBREL %d to %s, released on an earlier link, came on a link whose CONNACK had Session Present 0; want it sent again as a new message (design.md, Bridges)", p.ID, c.m.tag)
	}
	if earlier {
		b.resentInOrder(c)
		delete(b.carried, p.ID)
		delete(b.completed, p.ID)
		b.stats.resentReleases++
	}
	b.inflight[p.ID], c.on, c.rel, c.relOn = c, l, true, l
	b.answerSoon(c, true)
}

// resentInOrder checks that c, an exchange the peer's kept session holds,
// taken up again on the link up, comes after every one taken up on it
// before it: in the order they were first sent (storage.md,
// bridge_inflight: "They are resent in seq order"; design.md, CONNECT, 9:
// "resends them with their packet identifiers and in their order").
func (b *bridging) resentInOrder(c *crossed) {
	if c.sent < b.lastResent {
		b.fail("%s was taken up again after an exchange first sent after it: a kept session's exchanges come again in the order they were first sent (storage.md, bridge_inflight)", c.m.tag)
	}
	b.lastResent = c.sent
}

// check checks p against c, the record it carries, crossing to a peer
// whose CONNACK had Retain Available retainAvailable, carried where the
// peer's kept session holds it in flight (want).
func (c *crossed) check(p *harness.Publish, retainAvailable, carried bool) error {
	m := c.m
	var errs []error
	qos, retain := c.want(retainAvailable, carried)
	if p.Topic != c.topic || p.QoS != qos {
		errs = append(errs, fmt.Errorf("on %q at QoS %d, Retain Available %v; want %q, its rule's template applied, at QoS %d, the lower of its own and its rule's, 0 for a retained record's first sending to a peer with Retain Available 0 (design.md, Bridges)",
			p.Topic, p.QoS, retainAvailable, c.topic, qos))
	}
	if p.Retain != retain {
		errs = append(errs, fmt.Errorf("RETAIN %v; want %v: RETAIN crosses only on $broadcast records, and as 0 to a peer whose CONNACK has Retain Available 0 (design.md, Bridges)", p.Retain, retain))
	}
	if !bytes.Equal(p.Payload, m.published()) {
		errs = append(errs, fmt.Errorf("payload %.40q; want its publisher's, %.40q", p.Payload, m.published()))
	}
	ids := p.Props.User("saguin-id")
	switch stored := m.stored.Load(); {
	case len(ids) == 0 || (len(ids) != 1 && len(ids) != m.saguinIDs):
		errs = append(errs, fmt.Errorf("saguin-id %q, its publisher sending %d; want it once (design.md, Bridges: \"Every record a bridge sends carries saguin-id with its record's id\")", ids, m.saguinIDs))
	case m.id != "" && ids[0] != m.id:
		errs = append(errs, fmt.Errorf("saguin-id %q; want its record's, the publisher's first, %q (design.md, Sagüin properties)", ids[0], m.id))
	case m.id == "" && !uuidV7.MatchString(ids[0]):
		errs = append(errs, fmt.Errorf("saguin-id %q; want its record's, a UUIDv7 (design.md, Sagüin properties)", ids[0]))
	case stored != nil && ids[0] != stored.id:
		errs = append(errs, fmt.Errorf("saguin-id %q; want its record's, %q", ids[0], stored.id))
	}
	for _, q := range p.Props {
		if q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-") && q.Str != "saguin-id" {
			errs = append(errs, fmt.Errorf("%s %q: a record leaves with no saguin- property except saguin-id (design.md, Bridges)", q.Str, q.Value))
		}
	}
	if got, sent := own(p.Props), own(m.props); !sameProps(got, sent) || !slices.Equal(userOrder(got), userOrder(sent)) {
		errs = append(errs, fmt.Errorf("properties %v; want its publisher's %v unaltered, User Properties in their order (MQTT-3.3.2-17, MQTT-3.3.2-18)", got, sent))
	}
	got, ok := p.Props.Get(harness.MessageExpiryInterval)
	if sent, was := m.props.Get(harness.MessageExpiryInterval); ok != was || got.Int > sent.Int {
		errs = append(errs, fmt.Errorf("its Message Expiry %d (present %v); want the time remaining of its publisher's %d (present %v) (design.md, Delivery, Message Expiry)", got.Int, ok, sent.Int, was))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("bridge: %s crossed as PUBLISH %q: %w", m.tag, p.Topic, err)
	}
	return nil
}

// inOrder checks that c, arriving as p on link l, may come now: a resend
// only where the peer's session kept what was in flight, and then under its
// own packet identifier, in the order first sent (resentInOrder), and never
// once the bridge has sent its PUBREL; once on a link; its packet
// identifier in use by no other record in flight; within the peer's Receive
// Maximum; no other record of its topic at the peer in flight; and every
// record before it in its lane settled or excused.
func (b *bridging) inOrder(l *harness.PeerLink, c *crossed, p *harness.Publish) {
	resent := p.QoS > 0 && b.carried[p.ID] == c
	carried := slices.Contains(slices.Collect(maps.Values(b.carried)), c)
	if p.Dup && c.rel {
		b.fail("%s came again as a PUBLISH after the bridge sent its PUBREL (MQTT 5 4.3.3: the sender \"MUST NOT re-send the PUBLISH once it has sent the corresponding PUBREL packet\" [MQTT-4.3.3-6])", c.m.tag)
	}
	if p.Dup {
		b.resentInOrder(c)
	}
	switch {
	case c.on == l:
		b.fail("%s came twice on one link: while connected, the broker starts no resend (design.md, Delivery, One mechanism for every channel)", c.m.tag)
	case p.Dup && (!b.sessionPresent || c.link >= b.stats.links):
		b.fail("%s came with DUP 1, Session Present %v, sent on link %d of %d; want DUP 0: it was sent on no link before whose session the peer kept (MQTT-3.3.1-1; design.md, Bridges: \"sent again as new messages\")",
			c.m.tag, b.sessionPresent, c.link, b.stats.links)
	case carried && !resent:
		b.fail("%s came again under packet identifier %d, QoS %d; want its own, %d, the peer's session kept (MQTT-4.4.0-1)", c.m.tag, p.ID, p.QoS, c.id)
	case resent && !p.Dup:
		b.fail("%s came again under its packet identifier %d with DUP 0; want DUP 1 (MQTT-3.3.1-1)", c.m.tag, p.ID)
	}
	if resent {
		delete(b.carried, p.ID)
		b.stats.resent++
	} else if p.QoS > 0 {
		if other := cmp.Or(b.inflight[p.ID], b.carried[p.ID]); other != nil {
			b.fail("%s came under packet identifier %d, which %s, still in flight, holds (MQTT-2.2.1-3)", c.m.tag, p.ID, other.m.tag)
		}
		if n := len(b.inflight) + len(b.carried); n >= peerReceiveMaximum {
			b.fail("%s came with %d records unanswered; want at most the peer's Receive Maximum, %d, in flight (design.md, Bridges)", c.m.tag, n, peerReceiveMaximum)
		}
	}
	for _, r := range slices.Concat(slices.Collect(maps.Values(b.inflight)), slices.Collect(maps.Values(b.carried))) {
		if r != c && r.topic == c.topic {
			b.fail("%s came on %q while %s, on the same topic at the peer, is in flight: one record per topic at the peer is in flight at a time (design.md, Bridges)", c.m.tag, c.topic, r.m.tag)
		}
	}
	for _, e := range b.lanes[c.lane()] {
		if e == c {
			break
		}
		if !e.settled && !b.excused(e) {
			b.fail("%s came on %q before %s, stored before it on %s, was settled: each rule's records of a channel cross in its order, one per topic at the peer at a time (design.md, Bridges; Delivery, Order)", c.m.tag, c.topic, e.m.tag, c.m.channel)
			break
		}
	}
}

// answer has the peer answer c, in flight on its link: at QoS 1 with PUBACK
// reason; at QoS 2, before the bridge's PUBREL, with PUBREC reason, which
// leaves it awaiting that PUBREL where reason is a success; once the PUBREL
// has come, with PUBCOMP 0x00.
func (b *bridging) answer(c *crossed, reason byte) {
	l := c.on
	kind := harness.PUBACK
	switch {
	case c.qos == 2 && c.rel:
		kind = harness.PUBCOMP
		b.completed[c.id] = c
	case c.qos == 2:
		kind = harness.PUBREC
		if reason < 0x80 {
			c.rec = true
			// A link that has ended meanwhile loses the answer: the bridge
			// sends the PUBLISH again on its next.
			_, _ = l.Send(&harness.Ack{Kind: kind, ID: c.id, Reason: reason})
			return
		}
	}
	delete(b.inflight, c.id)
	c.on = nil
	if !c.settled {
		if reason == 0 {
			b.stats.crossed++
		} else {
			b.stats.refusedOut++
		}
	}
	c.settled = true
	// A link that has ended meanwhile loses the answer: the bridge sends
	// the record, or its PUBREL, again on its next.
	_, _ = l.Send(&harness.Ack{Kind: kind, ID: c.id, Reason: reason})
}

// release has the peer answer, with PUBACK 0x00, each record it held back
// on the link up.
func (b *bridging) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.withheld {
		if c.on != nil && c.on == b.link {
			b.answer(c, 0x00)
		}
	}
	b.withheld = nil
}

// settle has the peer answer what it held back, and hold back nothing, and
// waits, for at most limit, until every record the run's clients stored
// that an out rule owes the peer has reached it and been answered, or at
// QoS 0 received; a record the bridge must skip, and one sent at QoS 0 that
// a link the run dropped may have taken, are owed no more (excused).
func (b *bridging) settle(limit time.Duration) error {
	b.mu.Lock()
	b.settling = true
	b.mu.Unlock()
	b.release()
	defer func() {
		b.mu.Lock()
		b.settling = false
		b.mu.Unlock()
	}()
	// owed is the records owed and not yet answered, described for a
	// failure, the first mostErrors of them.
	var owed []string
	var failure error
	testkit.Within(limit, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.open = slices.DeleteFunc(b.open, func(c *crossed) bool { return c.settled || b.excused(c) })
		owed = owed[:0]
		for _, c := range b.open {
			// Only a record its client has had stored is owed: a QoS 1 one
			// acknowledged 0x00, a QoS 0 one whose intake a later answer
			// bounds.
			if c.m.fateIs(stored) || (c.m.qos == 0 && c.m.confirmed.Load() != 0) {
				owed = append(owed, fmt.Sprintf("%s (%s, awaiting the peer's answer %v)", c.m.tag, c, c.on != nil))
			}
		}
		failure = b.failure
		return len(owed) == 0 || failure != nil
	})
	switch {
	case failure != nil:
		return failure
	case len(owed) > 0:
		return fmt.Errorf("bridge: %d records an out rule owes the peer were not answered there within %v, the first %v (design.md, Bridges, Out: each rule is a durable consumer)",
			len(owed), limit, owed[:min(len(owed), mostErrors)])
	}
	return nil
}

// unfinishedCrossings is the run's last step, once its last checkpoint has
// settled every crossing: the peer holds back its answers (answerSoon), and
// a client publishes records the out rules take (crossBridge); the step
// waits, for at most the run's timeout, until the bridge can send the peer
// nothing more (stalled). The run's stop then leaves the bridge's
// exchanges unfinished, PUBLISHes awaiting PUBREC and PUBRELs awaiting
// PUBCOMP, which the restarted broker's bridge takes up (restart).
func (f *fuzzer) unfinishedCrossings() error {
	b := f.bridge
	if b == nil {
		return nil
	}
	// A step of its own, after the queue's last worker's (outlastHolds), and
	// the stop's publications at the next (stopPublications), so each
	// message keeps a tag of its own (payloadTag).
	f.step++
	defer func() { f.step++ }()
	if err := f.beginStep(); err != nil {
		return err
	}
	b.mu.Lock()
	b.holding = true
	b.mu.Unlock()
	f.note("bridge: the peer holds back its answers but PUBREC 0x00 to about half the QoS 2 PUBLISHes: the stop leaves exchanges unfinished")
	if _, err := f.crossBridge(); err != nil {
		return err
	}
	var failure error
	if !testkit.Within(f.timeout, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		failure = b.failure
		return failure != nil || b.stalled()
	}) {
		return fmt.Errorf("bridge: after %v, the bridge has not sent the peer every record owed that it may send while the peer holds back its answers (design.md, Bridges, Out)", f.timeout)
	}
	return failure
}

// stalled reports whether the bridge can send the peer nothing more while
// the peer holds back its answers: every record owed and not yet settled
// has reached it, its PUBREL too where the peer answered its PUBLISH with
// PUBREC 0x00, or waits behind another record in flight on its topic at the
// peer ("One record per topic at the peer is in flight at a time") or a
// window the peer's Receive Maximum fills.
func (b *bridging) stalled() bool {
	carried := slices.Collect(maps.Values(b.carried))
	busy := map[string]bool{}
	for _, c := range slices.Concat(slices.Collect(maps.Values(b.inflight)), carried) {
		busy[c.topic] = true
	}
	full := len(b.inflight)+len(b.carried) >= peerReceiveMaximum
	for _, c := range b.open {
		owed := c.m.fateIs(stored) || (c.m.qos == 0 && c.m.confirmed.Load() != 0)
		switch {
		case !owed || c.settled || b.excused(c):
		case c.on != nil:
			if c.rec && !c.rel {
				return false
			}
		case slices.Contains(carried, c) || (!full && !busy[c.topic]):
			return false
		}
	}
	return true
}
