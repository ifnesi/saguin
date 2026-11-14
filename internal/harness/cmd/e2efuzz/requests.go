package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
)

// The $saguin requests beside the seek (design.md, $saguin topics): kv/get,
// catalogue/<channel> and sessions/disconnect. An MQTT 5 client sends each
// at QoS 0, 1 or 2 with a Response Topic of the run's own (answerPrefix),
// with Correlation Data now and then; the model predicts its PUBACK or
// PUBREC and its answer, and the step waits for both, as for a seek
// (seeks.go, ask): "A request without a Response Topic is refused with 0x83,
// except for a seek or a queue response", which the conformance tests
// cover, is not drawn.
//
//   - A kv/get asks for a topic of kv, the latest channel, mostly one the
//     run has changed: "The request reads one topic's current value on a
//     latest channel. It needs read on that topic (else 0x87). The payload
//     is the topic. An empty answer means no value. The answer carries the
//     request's Correlation Data, or the topic where it set none or an
//     empty one. The broker refuses a topic on no latest channel, with a
//     wildcard, under $, or over max_topic_length or max_topic_levels with
//     0x90. It refuses an empty payload with 0x83. At QoS 0, the request is
//     dropped." The answer is a value that may have been current when it
//     was sent (latestLog.mayBeCurrentAt), byte for byte as published, or
//     empty where the topic may have held none. Flows: "Policy checks
//     select the first applicable reason in the stated order, independently
//     of property order." The row checks read first, including for an
//     invalid topic.
//   - A catalogue asks for a configured channel, but a dead-letter one, or
//     for one that does not exist: "The answer describes a channel:
//     {"name":…,"type":…,"filter":…,"verbs":[…]}. The filter is as
//     configured. The verbs are those of the asker's grants that name the
//     channel. Each operation is still checked against its own topic. A
//     queue adds "pin", its worker topic $saguin/queue/<channel>. A channel
//     that does not exist, or on which the asker holds no verb, gets an
//     empty answer and PUBACK 0x00. Correlation Data follows kv/get, with
//     the channel name standing for the topic. At QoS 0, the request is
//     dropped." The verbs are compared as a set: the design gives them no
//     order.
//   - A disconnect names another connected client, one connected nowhere,
//     the asker's own id or nothing: "An operator disconnects the client
//     whose id is the payload (0x98). Its session is kept. The request
//     needs sessions: disconnect (else 0x87). The answer is hung-up, or
//     no-such-client where no connection has that id [...]. The broker
//     refuses an empty payload or the asker's own id with 0x83. At QoS 0 it
//     still acts. A refusal at QoS 0 is dropped." Every user but restricted
//     holds sessions: disconnect (writeACL). The target's connection ends
//     as the broker's close of it does (badPacket): with DISCONNECT 0x98 in
//     MQTT 5, closed in MQTT 3.1.1 (DISCONNECT, Reasons Sagüin sends), its
//     Will published, its session kept for its next CONNECT, whose Session
//     Present the model predicts.

// requestStats counts the run's kv/gets, catalogue requests and
// disconnects, for its last lines: those answered, those answered empty,
// those refused or dropped without an answer, and the clients a disconnect
// hung up.
type requestStats struct {
	kvValues, kvRefused                   int
	catalogued, catalogueEmpty, catDrops  int
	hungUp, noSuchClient, disconnectFails int
}

// kvGetTopic, catalogueTopic and disconnectTopic are the requests' topics
// (design.md, $saguin topics, the table).
const (
	kvGetTopic      = "$saguin/kv/get"
	disconnectTopic = "$saguin/sessions/disconnect"
)

func catalogueTopic(channel string) string { return "$saguin/catalogue/" + channel }

// newRequest draws, for s's MQTT 5 connection, a request on topic carrying
// payload: its QoS, 2 only where s's user may use QoS 2, its Response Topic
// the run's own for this step, and its Correlation Data none, empty (where
// empty is true) or a word. It returns the request and what it awaits, its
// answer on that Response Topic carrying the Correlation Data sent, or
// fallback where the request set none or an empty one and fallback is set.
func (s *slot) newRequest(topic, payload string, empty bool, fallback string) (*harness.Publish, *asked) {
	f, r := s.f, s.f.rng
	p := &harness.Publish{QoS: byte(r.IntN(3)), Topic: topic, Payload: []byte(payload)}
	if denied(s.user, "qos2") {
		p.QoS = min(p.QoS, 1) // its QoS 2 PUBLISH ends the connection (PUBLISH's action)
	}
	a := &asked{topic: answerPrefix + strconv.Itoa(f.step)}
	p.Props = harness.Properties{{ID: harness.ResponseTopic, Str: a.topic}}
	switch n := r.IntN(3); {
	case n == 0:
		a.correlation = []byte(f.word(1, 32))
		p.Props = append(p.Props, harness.Property{ID: harness.CorrelationData, Data: a.correlation})
	case n == 1 && empty:
		p.Props = append(p.Props, harness.Property{ID: harness.CorrelationData, Data: []byte{}})
	}
	if a.correlation == nil && fallback != "" {
		a.correlation = []byte(fallback)
	}
	return p, a
}

// send sends p, the request a awaits the answers of, on s's connection, at
// the packet identifier it takes, and waits for them (ask).
func (s *slot) send(p *harness.Publish, a *asked, what string) error {
	if p.QoS > 0 {
		p.ID = s.nextID()
	}
	a.id, a.qos = p.ID, p.QoS
	s.f.note("%s: %s %q QoS %d %q properties %v: want %s", s.id, what, p.Topic, p.QoS, p.Payload, p.Props, a)
	return s.ask(p, a)
}

// refuse makes a, a request refused with reason, await that refusal and no
// answer: at QoS 0 nothing ("a refusal is dropped").
func (a *asked) refuse(reason, qos byte) {
	a.check, a.want = nil, ""
	if qos > 0 {
		a.reason = reason
	}
}

// requester draws a connected MQTT 5 client awaiting nothing, which may
// make a request: an MQTT 3.1.1 one "cannot make a request needing a
// Response Topic" (design.md, $saguin topics).
func (f *fuzzer) requester() *slot {
	return f.pick(func(s *slot) bool {
		return s.conn != nil && s.conn.Version == harness.V5 && s.asked == nil && s.unanswered() == 0
	})
}

// kvGet: a client asks for a topic of kv's current value.
func (f *fuzzer) kvGet() (bool, error) {
	s := f.requester()
	if s == nil {
		return false, nil
	}
	r := f.rng
	topics := slices.Sorted(maps.Keys(f.latest.topics))
	topic := "kv/" + []string{"z", "s", "b", "r", f.word(1, 3)}[r.IntN(5)]
	if len(topics) > 0 && r.IntN(3) != 0 {
		topic = topics[r.IntN(len(topics))]
	}
	switch n := r.IntN(12); {
	case s.user == restricted:
		// Its rights read kv's topics of two levels alone (rights).
	case n == 0:
		topic = f.topic(1 + r.IntN(2)) // on no latest channel
		if route(topic) == "kv" {
			topic = "c" + topic // kv's levels begin with kv
		}
	case n == 1:
		topic = "kv/+"
	case n == 2:
		topic = "kv/" + strings.Repeat("z", maxTopicLength)
	case n == 3:
		topic = ""
	}
	reason := kvGetReason(s.user, topic)
	p, a := s.newRequest(kvGetTopic, topic, true, topic)
	f.serial++
	at := f.serial // every change sent before the request has a serial below it
	switch {
	case reason != 0:
		a.refuse(reason, p.QoS)
	case p.QoS != 0: // at QoS 0, dropped
		a.check = func(payload []byte) error { return f.latest.answers(topic, at, payload) }
		for _, m := range f.latest.topics[topic] {
			a.lengths[1] = max(a.lengths[1], m.length)
		}
	}
	if discarded, sure := s.answerFits(a); !sure {
		return false, nil // its answer may or may not fit: another action
	} else if discarded {
		a.check = nil
	}
	if a.expects() {
		f.requestStats.kvValues++
	} else {
		f.requestStats.kvRefused++
	}
	return true, s.send(p, a, "KV/GET")
}

// kvGetReason follows design.md, $saguin topics: "It needs read on that
// topic (else 0x87)." precedes "The broker refuses a topic on no latest
// channel, with a wildcard, under $, or over max_topic_length or
// max_topic_levels with 0x90."
func kvGetReason(user, topic string) byte {
	switch {
	case !may(user, topic, false):
		return 0x87
	case topic == "":
		return 0x83
	case !isLatest(route(topic)) || strings.ContainsAny(topic, "+#") || strings.HasPrefix(topic, "$") ||
		len(topic) > maxTopicLength || strings.Count(topic, "/") >= maxTopicLevels:
		return 0x90
	}
	return 0
}

// answers reports why payload cannot be the answer to a kv/get of topic
// sent at serial at, nil where it can: a value of topic the run sent
// before it, byte for byte, that may have been current then; or empty,
// where topic may have held no value (mayBeEmptyAt).
func (l *latestLog) answers(topic string, at uint64, payload []byte) error {
	if len(payload) == 0 {
		if l.mayBeEmptyAt(topic, at) {
			return nil
		}
		return fmt.Errorf("an empty answer to the kv/get of %q, which surely held a value: \"an empty answer means no value\"", topic)
	}
	tag, _, _ := strings.Cut(string(payload), " ")
	i := slices.IndexFunc(l.topics[topic], func(m *message) bool { return m.tag == tag })
	switch {
	case i < 0:
		return fmt.Errorf("the kv/get of %q answered %.40q, no value the run sent there", topic, payload)
	case !l.mayBeCurrentAt(l.topics[topic][i], at):
		return fmt.Errorf("the kv/get of %q answered %s (step %d), which was not its current value when asked: it \"reads one topic's current value\"", topic, tag, l.topics[topic][i].step)
	case !bytes.Equal(payload, l.topics[topic][i].published()):
		return fmt.Errorf("the kv/get of %q answered %s altered: %d bytes, want the %d published", topic, tag, len(payload), l.topics[topic][i].length)
	}
	return nil
}

// mayBeEmptyAt reports whether topic may have held no value when a kv/get
// sent at serial at read it: no change of it surely entered by then, or a
// deletion sent before it may have been current then (Channels, latest:
// "An empty payload deletes the value").
func (l *latestLog) mayBeEmptyAt(topic string, at uint64) bool {
	changes := l.topics[topic]
	if !slices.ContainsFunc(changes, func(c *message) bool { return surelyIn(c) && enteredBy(c, at) }) {
		return true
	}
	return slices.ContainsFunc(changes, func(c *message) bool { return c.tombstone() && c.serial <= at && l.replacedBy(c, at) == nil })
}

// description is a catalogue answer (design.md, $saguin topics, the
// catalogue row).
type description struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Filter string   `json:"filter"`
	Verbs  []string `json:"verbs"`
	Pin    string   `json:"pin,omitempty"`
}

// catalogueOf is the catalogue of the channel named as user asks it, nil
// where its answer is empty: no such channel, or none of its verbs held.
// The verbs are what writeACL grants user anywhere within the channel,
// sorted: write, read and consume as its rules say, and seek on every
// channel but a latest one for every user but restricted.
func catalogueOf(user, name string) *description {
	i := slices.IndexFunc(channels, func(c channelConfig) bool { return c.name == name })
	if i < 0 {
		return nil
	}
	c := channels[i]
	held := map[string]bool{}
	for _, g := range grantsOf(user) {
		if g.channel == name {
			held["write"] = held["write"] || g.write
			held["read"] = held["read"] || g.read
			held["consume"] = held["consume"] || g.consume
		}
	}
	held["seek"] = len(held) > 0 && user != restricted && !c.latest
	d := &description{Name: name, Type: "append", Filter: c.filter}
	for _, verb := range slices.Sorted(maps.Keys(held)) {
		if held[verb] {
			d.Verbs = append(d.Verbs, verb)
		}
	}
	if len(d.Verbs) == 0 {
		return nil
	}
	switch {
	case c.latest:
		d.Type = "latest"
	case c.queue != nil:
		d.Type, d.Pin = "queue", "$saguin/queue/"+name
	}
	return d
}

// check reports why payload is not the answer that describes d, an
// empty one where d is nil.
func (d *description) check(payload []byte) error {
	if d == nil {
		if len(payload) != 0 {
			return fmt.Errorf("the catalogue answered %q; want an empty answer, for a channel that does not exist or holds none of the asker's verbs", payload)
		}
		return nil
	}
	keys := []string{"name", "type", "filter", "verbs"}
	if d.Pin != "" {
		keys = append(keys, "pin")
	}
	if _, err := harness.JSONObject(payload, keys...); err != nil {
		return fmt.Errorf("catalogue: %w", err)
	}
	var got description
	if err := json.Unmarshal(payload, &got); err != nil {
		return fmt.Errorf("the catalogue of %s answered %q: %w", d.Name, payload, err)
	}
	slices.Sort(got.Verbs)
	if got.Name != d.Name || got.Type != d.Type || got.Filter != d.Filter || !slices.Equal(got.Verbs, d.Verbs) || got.Pin != d.Pin {
		return fmt.Errorf("the catalogue of %s answered %s; want %+v", d.Name, payload, *d)
	}
	return nil
}

// catalogue: a client asks for a channel's description.
func (f *fuzzer) catalogue() (bool, error) {
	s := f.requester()
	if s == nil {
		return false, nil
	}
	names := []string{"nosuch"}
	for _, c := range channels {
		if c.deadLetter == "" {
			names = append(names, c.name)
		}
	}
	name := names[f.rng.IntN(len(names))]
	p, a := s.newRequest(catalogueTopic(name), "", true, name)
	d := catalogueOf(s.user, name)
	if p.QoS != 0 { // at QoS 0, dropped
		a.check = d.check
	}
	if d != nil {
		// The compact form the design writes, its whitespace and order
		// the broker's: at most twice as long, say.
		wire, err := json.Marshal(d)
		if err != nil {
			return true, err
		}
		a.lengths = [2]int{len(wire), 2 * len(wire)}
	}
	if discarded, sure := s.answerFits(a); !sure {
		return false, nil
	} else if discarded {
		a.check = nil
	}
	switch {
	case !a.expects():
		f.requestStats.catDrops++
	case d == nil:
		f.requestStats.catalogueEmpty++
	default:
		f.requestStats.catalogued++
	}
	return true, s.send(p, a, "CATALOGUE")
}

// hangUpOther: a client disconnects a client by its id, as an operator.
func (f *fuzzer) hangUpOther() (bool, error) {
	s := f.requester()
	if s == nil {
		return false, nil
	}
	r := f.rng
	target := f.pick(func(t *slot) bool { return t != s && t.conn != nil && !t.anon })
	id, want := "", ""
	away := false
	switch n := r.IntN(8); {
	case n < 4 && target != nil:
		id, want = target.clientID, "hung-up"
	case n < 6:
		id, want = "nobody-"+f.word(1, 3), "no-such-client"
		if t := f.pick(func(t *slot) bool { return t.conn == nil }); t != nil && n == 5 {
			// A client the run has closed, its session perhaps away; the
			// broker may not have taken that close yet, a dropped
			// connection's included (lose), and hang it up then.
			id, away = t.id, true
		}
	case n == 6:
		id = s.clientID
	}
	p, a := s.newRequest(disconnectTopic, id, false, "")
	acts := want == "hung-up"
	switch {
	case s.user == restricted:
		// Its rights hold no sessions: disconnect (writeACL).
		a.refuse(0x87, p.QoS)
		acts = false
	case want == "":
		a.refuse(0x83, p.QoS)
	case away:
		a.check = func(payload []byte) error {
			if answer := string(payload); answer != "no-such-client" && answer != "hung-up" {
				return fmt.Errorf("the disconnect of %s, closed by its client, answered %q; want no-such-client, or hung-up where the broker had not yet taken the close", id, payload)
			}
			return nil
		}
		a.lengths = [2]int{len("hung-up"), len("no-such-client")}
	default:
		a.want = want
	}
	if discarded, sure := s.answerFits(a); !sure {
		return false, nil
	} else if discarded {
		a.want, a.check = "", nil
	}
	switch {
	case acts:
		f.requestStats.hungUp++
	case a.expects():
		f.requestStats.noSuchClient++
	default:
		f.requestStats.disconnectFails++
	}
	if !acts {
		return true, s.send(p, a, "DISCONNECT REQUEST")
	}
	return true, s.disconnectOther(p, a, target)
}

// disconnectOther sends an allowed request to hang up target. design.md,
// $saguin topics: "An operator disconnects the client whose id is the
// payload (0x98). Its session is kept." Its Will can reach the requester
// before the answer does. Record that end before reading either, while
// still requiring the target's DISCONNECT and orderly close below.
func (s *slot) disconnectOther(p *harness.Publish, a *asked, target *slot) error {
	w := target.will
	target.fire("an operator's disconnect")
	if err := s.send(p, a, "DISCONNECT REQUEST"); err != nil {
		return err
	}
	// The broker cuts the target's connection, as for a malformed packet
	// (badPacket): design.md, DISCONNECT, Reasons Sagüin sends, "0x98 for
	// operator disconnect".
	err := closedWith(target, 0x98)
	_ = target.conn.Close() // checked closed by the broker, or the step failed
	target.end(false)
	if err != nil {
		return fmt.Errorf("%s: disconnected by %s: %w", target.id, s.id, err)
	}
	w.endObserved()
	return nil
}
