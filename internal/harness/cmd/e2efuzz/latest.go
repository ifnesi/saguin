package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/harness"
)

// Latest channels (design.md, Channels, latest: "It holds one value per
// topic, replaced in place at a new offset, in one write [...] An empty
// payload deletes the value and leaves a tombstone"; storage.md, records:
// "A latest channel holds one record per topic"). Clients replace values,
// delete them with an empty payload, subscribe with an initial pass and come
// back to a durable session; the model keeps each topic's changes in the
// order it sent them and checks what the current state obliges:
//
//   - the database holds one record per topic, flagged retained, the
//     topic's current value or tombstone, never one a newer change surely
//     replaced, and one wherever a change was acknowledged
//     (checkLatestRecords);
//   - a subscription gets "the current values, as Retain Handling allows,
//     then each change" (SUBSCRIBE, the subscription table): a pass value
//     is one that may have been current below its boundary ("Each page
//     sends the values current when it is read, below the boundary"),
//     never a tombstone (Channels, latest: "An empty payload deletes the
//     value"), and "On latest, they follow offset order of changes and
//     skip replaced values" (Delivery, Order), so a change is owed, at a
//     checkpoint, only while it is its topic's current one (latestOwed);
//   - "Returning consumers receive surviving tombstones as empty payloads"
//     (Channels), a change made while the session was away owed on its
//     return, never one replaced while it was away, nor a recovered
//     exchange whose value was replaced: it "is settled as if sent"
//     (Delivery; staleOnReturn).
//
// A value is known by its payload's tag, as every PUBLISH is; an empty
// payload carries none, so only MQTT 5 clients delete, each deletion's
// first saguin-id its tag, which its record and its MQTT 5 deliveries carry
// as sent (Sagüin properties). An MQTT 3.1.1 subscriber sees no
// properties: its tombstone is the earliest deletion of the topic it may
// be, in the model's own order (tombstoneOf).
//
// When a change entered its channel is the broker's: the model knows
// that c entered before m where c's intake was confirmed (by its PUBACK, a
// PINGRESP or the broker's close of its connection) before m was sent, or
// before a record check that found m its topic's record (precedes).

// latestLog is what the model knows of the latest channels' topics. It is
// written by the driver alone; bursts' goroutines read it.
type latestLog struct {
	// topics are each topic's changes, values and deletions, in the order
	// the run sent them, and last each topic's change sent last.
	topics map[string][]*message
	last   map[string]*message
	// values and deletions count the changes sent, passes the initial
	// passes started on a latest channel, due the current changes owed at
	// checkpoints, had or not, and awaited those a checkpoint waited for;
	// tombstones counts the empty payloads delivered, returned the changes
	// made while their session was away and delivered on its return,
	// copies the second copies passes sent.
	values, deletions, passes, due, awaited int
	tombstones, returned, copies            atomic.Int64
}

// latestPass is an initial pass a SUBSCRIBE started: its subscription, the
// connection it came on, the serial current when it was sent, and the
// topics whose value the pass has sent again, a second copy on the
// connection.
type latestPass struct {
	sub    *subscription
	conn   *harness.Client
	at     uint64
	copied map[string]bool
}

// latestSeen is the change of a topic a connection received last, live,
// and the serial current when it came.
type latestSeen struct {
	m  *message
	at uint64
}

// add keeps m, a change the broker may store on a latest channel, once,
// and as its topic's change sent last where it is: a QoS 2 PUBLISH is
// expected again at its PUBREL, with a later serial (releaseHeld).
func (l *latestLog) add(m *message) {
	if l.topics == nil {
		l.topics, l.last = map[string][]*message{}, map[string]*message{}
	}
	if last := l.last[m.topic]; last == nil || last.serial < m.serial {
		l.last[m.topic] = m
	}
	if m.logged {
		return
	}
	m.logged = true
	l.topics[m.topic] = append(l.topics[m.topic], m)
	if m.length == 0 {
		l.deletions++
	} else {
		l.values++
	}
}

// tombstone reports whether m is a deletion: an empty payload on a latest
// channel.
func (m *message) tombstone() bool {
	return m.length == 0 && isLatest(m.channel)
}

// surelyIn reports whether m surely entered its channel: acknowledged with
// PUBACK or PUBCOMP 0x00, or its record found.
func surelyIn(m *message) bool {
	return m.fateIs(stored) || m.stored.Load() != nil
}

// precedes reports whether c surely entered its latest channel before m:
// c's intake was confirmed before m was sent ("the core assigns [an
// offset] when a publication enters the channel, in entry order",
// design.md, Words: Offset), or before a record check that found m its
// topic's record.
func precedes(c, m *message) bool {
	conf := c.confirmed.Load()
	return conf != 0 && (conf < m.serial || conf < m.heldAt)
}

// enteredBy reports whether m's intake was confirmed by serial at.
func enteredBy(m *message, at uint64) bool {
	conf := m.confirmed.Load()
	return conf != 0 && conf <= at
}

// supersedes reports whether c surely replaced m: c surely entered, after
// m.
func supersedes(c, m *message) bool {
	return c != m && surelyIn(c) && precedes(m, c)
}

// current is topic's change the channel surely holds now: the one sent
// last, surely in, every other change of the topic surely entering before
// it; nil where the model cannot tell.
func (l *latestLog) current(topic string) *message {
	m := l.last[topic]
	if m == nil || !surelyIn(m) {
		return nil
	}
	for _, c := range l.topics[topic] {
		if c != m && !precedes(c, m) {
			return nil
		}
	}
	return m
}

// replacedBy is a change that surely replaced m and whose intake was
// confirmed by serial at, nil for none.
func (l *latestLog) replacedBy(m *message, at uint64) *message {
	for _, c := range l.topics[m.topic] {
		if supersedes(c, m) && enteredBy(c, at) {
			return c
		}
	}
	return nil
}

// mayBeCurrentAt reports whether m, a value, may have been its topic's
// current one when a SUBSCRIBE sent at serial at was taken: sent before it,
// and surely replaced by no change before it.
func (l *latestLog) mayBeCurrentAt(m *message, at uint64) bool {
	return !m.tombstone() && m.serial <= at && l.replacedBy(m, at) == nil
}

// startPass records the initial pass the SUBSCRIBE of sub, granted on s's
// connection with a pass and reaching a latest channel, starts: design.md,
// SUBSCRIBE, the subscription table, "latest | the current values, as
// Retain Handling allows"; MQTT 5 3.3.1.3, Retain Handling 0 sends them at
// every SUBSCRIBE [MQTT-3.3.1-9], 1 only where the subscription did not
// exist [MQTT-3.3.1-10]; MQTT 3.1.1 at every one [MQTT-3.3.1-6]. The pass
// ends with the connection ("A retained pass's prepared deliveries are
// dropped", Delivery, Preparation); what it handed to the writer may be
// resent on the session's next connection (CONNECT, 9), so the session
// keeps it.
func (s *slot) startPass(sub *subscription) {
	s.subs.latestPasses = append(s.subs.latestPasses, &latestPass{sub: sub, conn: s.conn, at: s.f.serial, copied: map[string]bool{}})
	s.f.latest.passes++
}

// passes are the passes of s's connection, or, resent, of any connection
// of its session ("A resend uses the Subscription Identifiers and delivery
// properties saved for its first transmission", Delivery, A resend).
func (s *slot) passes(resent bool) []*latestPass {
	if resent {
		return s.subs.latestPasses
	}
	i := slices.IndexFunc(s.subs.latestPasses, func(ps *latestPass) bool { return ps.conn == s.conn })
	if i < 0 {
		return nil
	}
	return s.subs.latestPasses[i:]
}

// passShapes are the eligible contributions to an initial pass. design.md,
// SUBSCRIBE, One copy: "Only subscriptions eligible for the record
// contribute"; "Subscription Identifiers appear in complete filter-byte
// order, once for each contributing subscription having one". Filters in
// one SUBSCRIBE start together; RH2 and an existing RH1 start no pass.
// Earlier SUBSCRIBEs may have finished before a later one arrives, or still
// be reading. Each possible starting group therefore contributes through
// the later groups begun by that revision, using its current options
// (Delivery: "Subscription changes affect new deliveries").
func (s *slot) passShapes(m *message, resent bool) []deliveryShape {
	if s.subs == nil || m.tombstone() {
		return nil
	}
	var shapes []deliveryShape
	var passes []*latestPass
	for _, ps := range s.passes(resent) {
		if matchesFilter(ps.sub.Filter, m.topic) && s.f.latest.mayBeCurrentAt(m, ps.at) {
			passes = append(passes, ps)
		}
	}
	for i, first := range passes {
		if i > 0 && passes[i-1].at == first.at && passes[i-1].conn == first.conn {
			continue
		}
		for _, r := range s.subs.history {
			if r.at <= first.at {
				continue
			}
			var eligible []*subscription
			for _, sub := range r.subs {
				if slices.ContainsFunc(passes[i:], func(ps *latestPass) bool {
					return ps.conn == first.conn && ps.at < r.at && ps.sub.Filter == sub.Filter &&
						ps.sub.start == sub.start
				}) {
					eligible = append(eligible, sub)
				}
			}
			shape := s.shapeOf(eligible, m, true, 0)
			// design.md, SUBSCRIBE, Options: "a `latest` channel's initial
			// pass to a client: its current values carry RETAIN 1".
			shape.retain = true
			shapes = append(shapes, shape)
		}
	}
	return shapes
}

// takePass reports whether a pass of s's connection may send m, which the
// connection has had already: "each page sends the values current when it
// is read" (SUBSCRIBE, The start point), so each pass at most one value of
// each topic, which it then takes.
func (s *slot) takePass(m *message) bool {
	for _, ps := range s.passes(false) {
		if !ps.copied[m.topic] && matchesFilter(ps.sub.Filter, m.topic) && s.f.latest.mayBeCurrentAt(m, ps.at) {
			ps.copied[m.topic] = true
			s.f.latest.copies.Add(1)
			return true
		}
	}
	return false
}

// tombstoneOf is the deletion p, an empty payload on a latest channel's
// topic delivered without the deletion's tag (MQTT 3.1.1, which carries no
// property), may deliver: the earliest of the topic's deletions the
// connection has not had, not surely older than the topic's change it had
// last nor than its publisher's last there (checkDelivery: "publisher's
// order"), that a subscription of its session takes at p's QoS; nil for
// none. sure reports whether d is the only deletion that fits: where
// several do, d is a guess, as the broker may have sent another, which
// entered its channel first (Delivery, Order: offset order is the
// broker's, not the order the run sent them in), so that neither d nor its
// publisher's progress is known (checkDelivery).
func (s *slot) tombstoneOf(p *harness.Publish) (d *message, sure bool) {
	if s.f == nil || s.subs == nil {
		return nil, false
	}
	last := s.incoming.lastOn[p.Topic].m
	for _, c := range s.f.latest.topics[p.Topic] {
		switch {
		case !c.tombstone(), s.incoming.seen[c.tag], last != nil && precedes(c, last), !p.Dup && c.serial < s.incoming.last[c.orderKey()]:
		case s.staleOnReturn(p, c) != nil:
		case !slices.ContainsFunc(s.shapes(c), func(shape deliveryShape) bool { return shape.allowed && (p.Dup || shape.qos == p.QoS) }):
		case d != nil:
			return d, false
		default:
			d = c
		}
	}
	return d, d != nil
}

// staleOnReturn checks m, a latest channel's change p delivers on s's
// connection, against the changes that replaced it before the session
// resumed there (the CONNECT, holders): "A recovered exchange whose latest
// value a newer one replaced is settled as if sent, and not counted"
// (design.md, Delivery), so a resend of one is an error; and a change that
// entered after the session went away and was replaced before it came back
// had its record replaced in place (Channels: "replaced in place at a new
// offset") with no connection to read it. Delivery, Order: "On latest,
// they follow offset order of changes and skip replaced values".
func (s *slot) staleOnReturn(p *harness.Publish, m *message) error {
	if s.subs == nil || len(s.subs.holders) == 0 {
		return nil
	}
	at := s.subs.holders[len(s.subs.holders)-1].at
	away := s.subs.departedAt != 0 && m.serial > s.subs.departedAt
	if !p.Dup && !away {
		return nil
	}
	c := s.f.latest.replacedBy(m, at)
	switch {
	case c == nil:
		return nil
	case p.Dup:
		return fmt.Errorf("%s: resend of %s of %q (step %d), which %s (step %d) replaced before the session resumed: a recovered exchange whose latest value a newer one replaced is settled as if sent (design.md, Delivery)",
			s.id, m.tag, m.topic, m.step, c.tag, c.step)
	}
	return fmt.Errorf("%s: delivery of %s of %q (step %d), which entered after its session went away at step %d and which %s (step %d) replaced before it came back: a returning consumer gets each changed topic's current value, not the history (design.md, Delivery, Order: \"skip replaced values\")",
		s.id, m.tag, m.topic, m.step, s.subs.departedStep, c.tag, c.step)
}

// latestOwed reports whether s owes m, a change of a latest channel, at a
// checkpoint: m is its topic's current change, acknowledged, which s's
// connection has not had (an MQTT 3.1.1 one's tombstone known only as the
// topic's, tombstoneOf), and either a pass of the connection reads it,
// through a subscription held unchanged since ("The captured boundary
// separates the initial pass, which reads retained or current values below
// it", The start point), or a subscription of the session has taken it
// throughout since before its acknowledgement (continuous), on this
// connection or, for a change acknowledged while the session was away, on
// its return ("Returning consumers receive surviving tombstones",
// Channels; Words: a session "holds subscriptions, positions, in-flight
// messages", an away one in the store). Each delivery fits the client's
// Maximum Packet Size (fitsAll).
func (s *slot) latestOwed(m *message) bool {
	if s.subs == nil || !m.fateIs(stored) || s.f.latest.current(m.topic) != m {
		return false
	}
	if s.hadDeletion(m) {
		return false
	}
	for _, ps := range s.passes(false) {
		if !m.tombstone() && enteredBy(m, ps.at) && s.subs.current[ps.sub.Filter] == ps.sub && s.shapeOf([]*subscription{ps.sub}, m, true, 0).allowed {
			return s.fitsAll(m)
		}
	}
	away := s.subs.departedStep != 0 && m.ackStep > s.subs.departedStep
	if m.ackStep < s.incoming.since && !away {
		return false
	}
	return s.continuous(m, m.ackStep) && s.fitsAll(m)
}

// hadDeletion reports whether m is a deletion that s's connection, MQTT
// 3.1.1, may have had, its tombstone known by its topic alone
// (tombstoneOf): one of m's topic came live after m was sent.
func (s *slot) hadDeletion(m *message) bool {
	seen := s.incoming.lastOn[m.topic]
	return m.tombstone() && s.conn.Version == harness.V311 && seen.m != nil && seen.m.tombstone() && seen.at >= m.serial
}

// noteLatest counts, for the run's last lines, p, a delivery of m from a
// latest channel on s's connection, and keeps m as the topic's change the
// connection had last where it came live, as a pass's may not (tombstoneOf).
func (s *slot) noteLatest(p *harness.Publish, m *message, live bool) {
	if m.tombstone() {
		s.f.latest.tombstones.Add(1)
	}
	if !p.Dup && s.subs.departedAt != 0 && m.serial > s.subs.departedAt && m.serial < s.subs.holders[len(s.subs.holders)-1].at {
		s.f.latest.returned.Add(1)
	}
	if live && !p.Dup {
		if s.incoming.lastOn == nil {
			s.incoming.lastOn = map[string]latestSeen{}
		}
		s.incoming.lastOn[m.topic] = latestSeen{m: m, at: s.f.serial}
	}
}

// checkLatestRecords checks the records of the latest channel id, named
// name, every one at each check (storage.md, records): each the run's
// change it names, its value by its payload's tag, its tombstone by its id
// (Sagüin properties: "The saguin-id property sets the record's id"), with
// what record.matches checks; "retained | 1 while it is its topic's
// current value: [...] on a latest channel, every record"; one per topic
// ("A latest channel holds one record per topic"), never a change a newer
// one surely replaced ("A new value updates the topic's record in
// place"); and a record wherever a change of the topic was acknowledged
// (R4), retention coming due in no run (channels.go).
func (f *fuzzer) checkLatestRecords(id int64, name string) []error {
	// Every intake confirmed before this read has a serial below this one
	// (precedes).
	f.serial++
	read := f.serial
	rows, err := f.db.Query(`SELECT "offset", id, time, topic, qos, publisher, expires, properties, payload, payload_size, published_retained, retained
		FROM records WHERE channel = ? ORDER BY "offset"`, id)
	if err != nil {
		return []error{fmt.Errorf("reading the records of %s: %w", f.dbPath, err)}
	}
	defer func() { _ = rows.Close() }() // read whole, or the check has failed
	var errs []error
	held := map[string]*message{}
	for rows.Next() {
		r := record{channel: id, channelName: name}
		var stored []byte
		var size, flag int64
		if err := rows.Scan(&r.offset, &r.id, &r.time, &r.topic, &r.qos, &r.publisher, &r.expires, &r.properties, &stored, &size, &r.retained, &flag); err != nil {
			return append(errs, fmt.Errorf("reading the records of %s: %w", f.dbPath, err))
		}
		where := fmt.Sprintf("the record at channel %d offset %d", id, r.offset)
		payload, compressed, err := asReceived(stored, size)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", where, err))
			continue
		}
		r.payload = payload
		tag := r.id
		if len(payload) > 0 {
			tag, _, _ = strings.Cut(string(payload), " ")
		}
		m := f.publications[tag]
		if m != nil && (!m.logged || m.topic != r.topic) {
			m = nil
		}
		if m != nil && m.hidden != 0 {
			if err := f.learn(m, r.publisher); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", where, err))
			}
		}
		if err := r.matches(f, m); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", where, err))
		}
		if flag != 1 {
			errs = append(errs, fmt.Errorf("%s, of %q: retained %d; on a latest channel every record is its topic's current value, retained 1 (storage.md, records)", where, r.topic, flag))
		}
		if m == nil {
			continue
		}
		if other := held[r.topic]; other != nil {
			errs = append(errs, fmt.Errorf("channel %s holds two records of %q, %s and %s: a latest channel holds one record per topic (storage.md, records)", name, r.topic, other.tag, m.tag))
		}
		held[r.topic] = m
		m.heldAt = read
		if m.stored.Load() != nil {
			continue
		}
		f.stored.count(size, compressed)
		f.onChannels++
		kept := r
		kept.payload, kept.properties = nil, nil // what compare reads is kept, no body
		m.stored.Store(&kept)
		if seen := m.seen.Load(); seen != nil {
			if err := r.compare(*seen, m.tag, "a subscriber"); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		errs = append(errs, fmt.Errorf("reading the records of %s: %w", f.dbPath, err))
	}
	for _, topic := range slices.Sorted(maps.Keys(f.latest.topics)) {
		changes := f.latest.topics[topic]
		if route(topic) != name {
			continue
		}
		if h := held[topic]; h != nil {
			if i := slices.IndexFunc(changes, func(c *message) bool { return supersedes(c, h) }); i >= 0 {
				errs = append(errs, fmt.Errorf("channel %s holds %s (step %d) of %q, which %s (step %d) replaced: a new value updates the topic's record in place (storage.md, records; design.md, Channels)",
					name, h.tag, h.step, topic, changes[i].tag, changes[i].step))
			}
			continue
		}
		if i := slices.IndexFunc(changes, surelyIn); i >= 0 {
			errs = append(errs, fmt.Errorf("channel %s holds no record of %q, though %s (step %d) was acknowledged with 0x00 (R4; storage.md, records: a latest channel holds one record per topic)",
				name, topic, changes[i].tag, changes[i].step))
		}
	}
	return errs
}
