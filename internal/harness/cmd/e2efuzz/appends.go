package main

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/harness"
)

// Delivery from the configured append channels (design.md, Flows,
// SUBSCRIBE and Delivery). A session's consumer has one position per
// channel, made by the first subscription reaching it, at the channel's
// floor or, where it sets start: tail, its tail, and, for a session that
// is not durable, ended with the last subscription reaching it: "A consumer
// has one position per channel, never one per filter. On an append channel
// every filter reads from that position. A filter added later therefore
// receives what the position has not yet passed" (The start point); "When
// the last subscription reaching an append channel or a queue is removed, a
// durable session keeps its position there"; "The next subscription
// reaching the channel reads from the kept position, so a record at or
// beyond it may be delivered again"; "A volatile session's positions [...]
// still end with the last subscription reaching the channel" (UNSUBSCRIBE).
// The model keeps each position's life, an epoch, and checks each delivery
// against them: a record
// is deliverable through any subscription the session held while an epoch
// lasted that matches it, its partition and No Local allowing, and not before
// the epoch's tail; in channel order (Delivery, Order), with the Sagüin
// properties (Sagüin properties); and owed (owed), a position made at the
// floor on the subscriber's own connection owing its channel's whole history.
// Where a position stood when a filter was added is the broker's timing, not
// the client's to know: such a filter's records are accepted, never owed.

// appendStats counts deliveries from configured channels, positions made
// (at the floor), floor histories owed at a checkpoint, and subscriptions
// holding a partition. Deliveries are counted on bursts' goroutines.
type appendStats struct {
	deliveries                     atomic.Int64
	positions, floors, partitioned int
	// resumed counts the positions a durable session kept with no
	// subscription and a SUBSCRIBE read again (resume).
	resumed int
	// orphans counts deliveries after an UNSUBACK matching no subscription
	// held, each handed to the writer before it (orphan).
	orphans atomic.Int64
}

// epoch is one consumer position of a session on an append channel.
type epoch struct {
	channel string
	// created is the serial at the SUBSCRIBE making it, ended the one at
	// the UNSUBSCRIBE ending it, 0 while it lasts.
	created, ended uint64
	tail           bool
	// step is the SUBSCRIBE's step, conn the connection it came on, and
	// filters those of that SUBSCRIBE reaching the channel, which take its
	// history; checked: a checkpoint has owed that history.
	step    int
	conn    *harness.Client
	filters []string
	checked bool
	// kept: no subscription reaches the channel, the UNSUBSCRIBE of the
	// last one having kept the durable session's position (unplace), until
	// a SUBSCRIBE reaches it again (place); keptOn is the connection that
	// sent that UNSUBSCRIBE, keptAt the serial then, and keptFrom the
	// place, in that connection's reads of the channel (incoming.reads),
	// of the first record it had not settled then: the kept position is no
	// later, so that record and every one it read after may come again
	// (repeatable). againOn, againFrom and againBefore are those of the
	// last SUBSCRIBE reading it again on its connection (resume), carried
	// to each connection the session resumes on after it (resumed), and
	// intervals counts the reading intervals the SUBSCRIBEs reading it
	// again began on that connection, or on the one it was carried from,
	// each of which may send a record once more.
	kept                bool
	keptOn, againOn     *harness.Client
	keptFrom, againFrom int
	keptAt, againBefore uint64
	intervals           int
}

// live is the epoch of s's session on channel that lasts, nil for none.
func (subs *subscriptions) live(channel string) *epoch {
	es := subs.positions[channel]
	if len(es) > 0 && es[len(es)-1].ended == 0 {
		return es[len(es)-1]
	}
	return nil
}

// place makes the positions the SUBSCRIBE of filters, answered want, makes:
// one on each append channel an accepted filter reaches where the session
// has none. A position made again restarts the channel's order and copies
// on this connection: its records may come again. A latest channel is read
// from each subscription's live-start boundary, as $broadcast is
// (latest.go).
func (s *slot) place(filters []harness.Subscription, want []byte) {
	made := map[string]*epoch{}
	for i, sub := range filters {
		if want[i] >= 0x80 {
			continue
		}
		for _, ch := range reach(sub.Filter) {
			if ch == broadcast || isLatest(ch) {
				continue
			}
			if e := made[ch]; e != nil {
				e.filters = append(e.filters, sub.Filter)
				continue
			}
			if e := s.subs.live(ch); e != nil {
				if e.kept {
					s.resume(e)
				}
				continue
			}
			// An earlier position's deliveries, resent or finishing, may
			// still come on this connection.
			again := len(s.subs.positions[ch]) > 0
			e := &epoch{channel: ch, created: s.f.serial, tail: startsAtTail(ch), step: s.f.step, conn: s.conn, filters: []string{sub.Filter}}
			if s.subs.sought[ch] {
				// A seek before any subscription made this position, at
				// the floor ("a saved seek position controlling the first
				// read of that channel", design.md, $saguin topics), and
				// the run owes nothing of it (seeks.go).
				e.tail, e.checked = false, true
				delete(s.subs.sought, ch)
			}
			s.subs.positions[ch] = append(s.subs.positions[ch], e)
			made[ch] = e
			s.incoming.epochs++
			s.f.appendStats.positions++
			if !e.tail {
				s.f.appendStats.floors++
			}
			if again {
				// What the ended position prepared was cancelled at the
				// UNSUBSCRIBE ending it; only what it handed to the writer
				// may still come (inFlightUnread).
				s.restart(ch, s.inFlightUnread())
			}
		}
	}
}

// restart forgets the order and copies seen of channel on s's connection,
// whose position was made again, letting grace second copies pass: what
// the position before still has in flight may yet arrive, its record
// coming again from the new one, and the channel's order is no longer
// checked there.
func (s *slot) restart(channel string, grace int) {
	s.unorder(channel, grace)
	for tag := range s.incoming.seen {
		if m := s.f.publications[tag]; m != nil && m.channel == channel {
			delete(s.incoming.seen, tag)
		}
	}
}

// unorder stops checking the order of channel on s's connection, letting
// grace second copies pass there (restart, resume).
func (s *slot) unorder(channel string, grace int) {
	if s.incoming.unordered == nil {
		s.incoming.unordered, s.incoming.grace = map[string]bool{}, map[string]int{}
	}
	s.incoming.unordered[channel] = true
	s.incoming.grace[channel] = max(s.incoming.grace[channel], grace)
}

// unplace ends the positions whose last subscription the UNSUBSCRIBE of
// filter removed, or, for a durable session, keeps them (epoch.kept): "When
// the last subscription reaching an append channel or a queue is removed, a
// durable session keeps its position there. If a delivery was prepared but
// never handed to the writer, the core first sets the position back to the
// earliest such delivery's offset"; "A volatile session's positions [...]
// still end with the last subscription reaching the channel"
// (UNSUBSCRIBE). The position stays before the first record its client
// had not settled ("The position advances only past the contiguous
// settled or skipped prefix of examined records", Delivery, Positions), so
// the records from that one on may come again; those before it, settled,
// may not. A position its connection has read again keeps no later than
// where that reading interval began (resume): "Their completion never moves
// the kept position, even after another subscription reaches the channel",
// so an exchange of an earlier interval settled since then has not moved
// it, and the model, which sees no offsets there, cannot tell which of the
// records from there on the interval passed.
func (s *slot) unplace(filter string) {
	for _, ch := range reach(filter) {
		e := s.subs.live(ch)
		if e == nil || e.kept || slices.ContainsFunc(slices.Collect(maps.Values(s.subs.current)), func(sub *subscription) bool {
			return slices.Contains(sub.reach, ch)
		}) {
			continue
		}
		if !s.durable {
			e.ended = s.f.serial
			continue
		}
		from := s.firstUnsettled(ch)
		if e.againOn == s.conn {
			from = min(from, e.againFrom)
		}
		e.kept, e.keptOn, e.keptFrom, e.keptAt = true, s.conn, from, s.f.serial
	}
}

// firstUnsettled is the place, in s's connection's reads of channel, of
// the first record its client has not settled: one it read and has not
// acknowledged, or whose QoS 2 exchange its PUBCOMP has not ended (design.md,
// Delivery, Positions: "an acknowledgement, PUBACK, PUBCOMP or a refusing
// PUBREC, settles its own delivery"); the end of those reads where it has
// settled every one, what it reads next then being handed to the writer
// before the UNSUBSCRIBE and as yet unsettled.
func (s *slot) firstUnsettled(channel string) int {
	// A dead letter is read under its job's tag and deadLetterTag
	// (checkDelivery); an exchange names its tag alone.
	unsettled := map[string]bool{}
	for _, p := range s.incoming.held {
		tag, _ := s.deliveryTag(p)
		unsettled[tag], unsettled[tag+deadLetterTag] = true, true
	}
	for _, x := range s.subs.outbound {
		if !x.done {
			unsettled[x.tag], unsettled[x.tag+deadLetterTag] = true, true
		}
	}
	reads := s.incoming.reads[channel]
	if i := slices.IndexFunc(reads, func(tag string) bool { return unsettled[tag] }); i >= 0 {
		return i
	}
	return len(reads)
}

// resume has e, a kept position, read again by a SUBSCRIBE reaching its
// channel: "The next subscription reaching the channel reads from the kept
// position, so a record at or beyond it may be delivered again"
// (UNSUBSCRIBE), which repeatable lets pass, once more for each reading
// interval begun on this connection. The replay goes back in the
// channel's offsets, so its order is no longer checked on this connection,
// and it may replay the channel (mostDeliveries).
func (s *slot) resume(e *epoch) {
	if e.againOn != s.conn {
		e.intervals = 0
	}
	e.againOn, e.againFrom, e.againBefore = s.conn, 0, e.keptAt
	e.intervals++
	if e.keptOn == s.conn {
		e.againFrom = e.keptFrom
	}
	s.unorder(e.channel, 0)
	s.incoming.epochs++
	s.f.appendStats.resumed++
	e.kept, e.keptOn = false, nil
}

// repeatable reports whether m, of which s's connection has had a copy,
// may come once more without DUP, from a kept position read again
// (resume) by a SUBSCRIBE of this connection, or of an earlier connection
// of the session that this one resumed (resumed), and takes that once for
// each reading interval begun there (epoch.intervals): a record published
// before the UNSUBSCRIBE that kept the position, its first copy read on
// this connection no earlier than the first record the client had not
// settled then, or later. A record its client had settled before that one
// the kept position passed ("The position advances only past the
// contiguous settled or skipped prefix of examined records", Delivery,
// Positions). On a later connection of the session than the one that kept
// it, every copy it reads counts: each is either a resend of an exchange
// the position had not passed or a first copy from the position. So it is
// on a connection that resumed the session after a reading interval
// began: the exchanges the UNSUBSCRIBE left in flight, which no longer
// hold the kept position back (Delivery, Positions: "That removal marks
// the session's own stored exchanges on the channel moves = 0"), are
// resent with DUP (CONNECT, 9), and what the interval had not yet handed
// to the writer, cancelled with the old connection, comes from the kept
// position anew after them (Delivery, Preparation: "A live scan's position
// stays before their records, so a continuing session receives them
// later"): the same record twice, its resend and its replay. A record
// handed to the writer before the UNSUBSCRIBE, read after it, may come
// twice so: its first copy and the replay's; read again by a later
// interval, its copy from each interval, one of them perhaps read only
// after the next began.
// The latest interval's bounds hold for the earlier ones' copies too: its
// kept position is no earlier than theirs, and its place in the reads no
// later (unplace).
func (s *slot) repeatable(m *message) bool {
	if s.subs == nil {
		return false
	}
	e := s.subs.live(m.channel)
	if e == nil || e.againOn != s.conn || m.serial > e.againBefore || s.incoming.repeats[m.tag] >= e.intervals ||
		slices.Index(s.incoming.reads[m.channel], m.tag) < e.againFrom {
		return false
	}
	if s.incoming.repeats == nil {
		s.incoming.repeats = map[string]int{}
	}
	s.incoming.repeats[m.tag]++
	return true
}

// channelShapes are the contributions a record of a configured channel may
// be delivered with: for each epoch it may be delivered under, not ending
// before the record entered nor starting at a tail it surely preceded,
// each revision of the session's subscriptions during it, from the one
// current when the record entered (its serial at the latest), the
// subscriptions' own live-start boundaries not applying.
func (s *slot) channelShapes(m *message) []deliveryShape {
	if s.subs == nil {
		return nil
	}
	var shapes []deliveryShape
	confirmed := m.confirmed.Load()
	for _, e := range s.subs.positions[m.channel] {
		if (e.tail && confirmed != 0 && confirmed <= e.created) || (e.ended != 0 && e.ended < m.serial) {
			continue
		}
		from := max(e.created, m.serial)
		history := s.subs.history
		for i, r := range history {
			if i+1 < len(history) && history[i+1].at <= from {
				continue // replaced before the record or the epoch
			}
			if e.ended != 0 && r.at > e.ended {
				break
			}
			shapes = append(shapes, s.shapeOf(r.subs, m, true, 0))
		}
	}
	return shapes
}

// checkChannel checks p, a delivery of m from a configured channel, as the
// design has it beyond the checks every delivery has: RETAIN 0 ("RETAIN is
// 0 on channel deliveries", SUBSCRIBE, Options) except on a latest
// channel's initial pass, whose RETAIN 1 its predicted shapes check ("a
// `latest` channel's initial pass to a client: its current values carry
// RETAIN 1"), the channel's offset order on this connection where it came
// live, and in MQTT 5 the Sagüin properties.
func (s *slot) checkChannel(p *harness.Publish, m *message, shapes []deliveryShape, live bool) error {
	s.f.appendStats.deliveries.Add(1)
	if s.incoming.reads == nil {
		s.incoming.reads = map[string][]string{}
	}
	s.incoming.reads[m.channel] = append(s.incoming.reads[m.channel], m.tag)
	if p.Retain && (live || !isLatest(m.channel)) {
		return fmt.Errorf("%s: delivery of %s from channel %s with RETAIN 1: RETAIN is 0 on channel deliveries (design.md, SUBSCRIBE, Options)", s.id, m.tag, m.channel)
	}
	if s.conn.Version != harness.V5 {
		return nil
	}
	if p.Dup && s.firstOverV311(m) && len(p.Props.User("saguin-offset")) == 0 {
		// "A resend uses the Subscription Identifiers and delivery
		// properties saved for its first transmission" (Delivery, A
		// resend), which may have gone over MQTT 3.1.1, whose clients
		// "receive none" (Sagüin properties): none were saved, and it
		// carries the record's own, as $broadcast's do. A dead letter's
		// record may hold its saguin-dlq-*, which the design does not place
		// on the record or on each delivery: they are checked where
		// present (queues.go).
		if m.deadOf != nil {
			return s.deadAsStored(p, m)
		}
		return s.noSaguin(p, m)
	}
	if err := s.saguinProperties(p, m, shapes); err != nil {
		return err
	}
	offset, _ := strconv.ParseInt(p.Props.User("saguin-offset")[0], 10, 64)
	if s.incoming.offsets == nil {
		s.incoming.offsets = map[string]int64{}
	}
	if !p.Dup && live && !s.incoming.unordered[m.channel] {
		if last := s.incoming.offsets[m.channel]; offset <= last {
			return fmt.Errorf("%s: delivery of %s at offset %d of channel %s after offset %d: a channel's records come in offset order (design.md, Delivery, Order)",
				s.id, m.tag, offset, m.channel, last)
		}
		s.incoming.offsets[m.channel] = offset
	}
	return nil
}

// saguinProperties checks p's saguin- User Properties, a delivery of m from
// a configured channel to MQTT 5 (design.md, Sagüin properties): saguin-id
// (the publisher's own, else a UUIDv7), saguin-offset and saguin-timestamp,
// each once, decimal, the record's; saguin-expires where the publisher set
// an expiry; saguin-channel, naming the channel, where the consumer's
// filter reaches more than one channel, which the model takes as each
// contributing filter, accepting either where they differ; a dead letter's
// saguin-dlq-* (queues.go, checkDeadProps); and no other.
// The Message Expiry Interval a record carries is worked out as the time
// remaining, 0 once expired (Delivery, Message Expiry).
func (s *slot) saguinProperties(p *harness.Publish, m *message, shapes []deliveryShape) error {
	got := map[string][]string{}
	for _, q := range p.Props {
		if q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-") {
			got[q.Str] = append(got[q.Str], q.Value)
		}
	}
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%s: delivery of %s from channel %s (QoS %d, DUP %v, properties %v): %s (design.md, Sagüin properties)",
			s.id, m.tag, m.channel, p.QoS, p.Dup, p.Props, fmt.Sprintf(format, args...))
	}
	switch ids := got["saguin-id"]; {
	case len(ids) == 0 || (len(ids) != 1 && len(ids) != m.saguinIDs):
		return bad("saguin-id %q, want it once", ids)
	case m.id != "" && ids[0] != m.id:
		return bad("saguin-id %q, want the publisher's first, %q", ids[0], m.id)
	case m.id == "" && !uuidV7.MatchString(ids[0]):
		return bad("saguin-id %q, want a UUIDv7", ids[0])
	}
	for _, name := range []string{"saguin-offset", "saguin-timestamp"} {
		if v := got[name]; len(v) != 1 || !decimal(v[0]) {
			return bad("%s %q, want it once, decimal", name, v)
		}
	}
	expiring := m.expiring
	switch v := got["saguin-expires"]; {
	case expiring && (len(v) != 1 || !decimal(v[0])):
		return bad("saguin-expires %q, want it once, decimal: its publisher set an expiry", v)
	case !expiring && len(v) != 0:
		return bad("saguin-expires %q, want none: its publisher set no expiry", v)
	}
	if mei, ok := p.Props.Get(harness.MessageExpiryInterval); expiring != ok {
		return bad("Message Expiry Interval %d present %v, want it as its publisher set it", mei.Int, ok)
	}
	multi, single := false, false
	for _, shape := range shapes {
		if shape.allowed {
			multi, single = multi || shape.multi, single || shape.single
		}
	}
	switch v := got["saguin-channel"]; {
	case len(v) > 1 || (len(v) == 1 && v[0] != m.channel):
		return bad("saguin-channel %q, want it once, %q", v, m.channel)
	case len(v) == 0 && !single:
		return bad("no saguin-channel, its filters reaching several channels")
	case len(v) == 1 && !multi:
		return bad("saguin-channel %q, its filters reaching one channel", v)
	}
	for name := range got {
		if !slices.Contains([]string{"saguin-id", "saguin-offset", "saguin-timestamp", "saguin-expires", "saguin-channel"}, name) &&
			(m.deadOf == nil || !strings.HasPrefix(name, "saguin-dlq-")) {
			return bad("%s %q, which Sagüin does not add", name, got[name])
		}
	}
	if m.deadOf != nil {
		if err := s.f.checkDeadProps(m, got); err != nil {
			return bad("%v", err)
		}
	}
	return m.compare(got["saguin-id"][0], got["saguin-offset"][0], got["saguin-timestamp"][0], s.id)
}

// noSaguin checks p, a delivery of m from $broadcast to MQTT 5, carries no
// Sagüin property but the publisher's own saguin-id: "Deliveries to clients
// of messages on $broadcast carry none of them. They receive only MQTT's own
// per-delivery changes [...]
// The publisher's own User Properties go unaltered" (Sagüin properties).
func (s *slot) noSaguin(p *harness.Publish, m *message) error {
	sent := m.saguinIDs
	got := 0
	for _, q := range p.Props {
		switch {
		case q.ID != harness.UserProperty || !strings.HasPrefix(q.Str, "saguin-"):
		case q.Str == "saguin-id":
			got++
		default:
			return fmt.Errorf("%s: delivery of %s from $broadcast carries %s %q: $broadcast messages carry no Sagüin property (design.md, Sagüin properties)", s.id, m.tag, q.Str, q.Value)
		}
	}
	if got > sent {
		return fmt.Errorf("%s: delivery of %s from $broadcast carries saguin-id %d times, its publisher %d (design.md, Sagüin properties)", s.id, m.tag, got, sent)
	}
	return nil
}

// deadAsStored checks p, the resend over MQTT 5 of m, a dead letter, whose
// first transmission went over MQTT 3.1.1 and so saved no delivery
// property: the publisher's saguin-id, at most as often as it sent it, and
// the saguin-dlq-* where present (checkDeadProps); no other Sagüin
// property.
func (s *slot) deadAsStored(p *harness.Publish, m *message) error {
	got := map[string][]string{}
	for _, q := range p.Props {
		if q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-") {
			got[q.Str] = append(got[q.Str], q.Value)
		}
	}
	dlq := 0
	for name := range got {
		switch {
		case name == "saguin-id":
		case strings.HasPrefix(name, "saguin-dlq-"):
			dlq++
		default:
			return fmt.Errorf("%s: resend of the dead letter %s, its first transmission over MQTT 3.1.1, carries %s %q: none was saved (design.md, Delivery, A resend; Sagüin properties)", s.id, m.tag, name, got[name])
		}
	}
	switch {
	case len(got["saguin-id"]) > m.saguinIDs:
		return fmt.Errorf("%s: resend of the dead letter %s carries saguin-id %d times, its publisher %d (design.md, Sagüin properties)", s.id, m.tag, len(got["saguin-id"]), m.saguinIDs)
	case dlq == 0:
		return nil
	}
	if err := s.f.checkDeadProps(m, got); err != nil {
		return fmt.Errorf("%s: resend of the dead letter %s: %w (design.md, Sagüin properties; Channels, queue)", s.id, m.tag, err)
	}
	return nil
}

// decimal reports whether v is a decimal integer.
func decimal(v string) bool {
	_, err := strconv.ParseInt(v, 10, 64)
	return err == nil
}

// replayOwed reports whether s owes m as part of a channel's history: a
// position s's own connection made at the floor, not yet checked, owes
// every stored record of its channel that one of the filters making it
// matches, held since and allowing it throughout, its partition and No
// Local included
// (design.md, SUBSCRIBE: "append [...] Starts with the floor"; Delivery,
// Positions).
func (s *slot) replayOwed(m *message) bool {
	if m.channel == "" || m.channel == broadcast || !m.fateIs(stored) || m.qos != 1 {
		return false
	}
	e := s.subs.live(m.channel)
	if e == nil || e.tail || e.checked || e.conn != s.conn {
		return false
	}
	for _, filter := range e.filters {
		if sub := s.subs.current[filter]; sub != nil && sub.since == e.step && s.allowedThroughout(filter, m, e) {
			return true
		}
	}
	return false
}

// allowedThroughout reports whether filter has been held, allowing m, in
// every revision of the session's subscriptions since e was made: a
// SUBSCRIBE repeating it "replaces its options", and a record its position
// passed while they excluded it, No Local or a partition, is not read
// again.
func (s *slot) allowedThroughout(filter string, m *message, e *epoch) bool {
	for _, r := range s.subs.history {
		if r.at <= e.created {
			continue
		}
		i := slices.IndexFunc(r.subs, func(sub *subscription) bool { return sub.Filter == filter })
		if i < 0 || !s.shapeOf(r.subs[i:i+1], m, true, 0).allowed {
			return false
		}
	}
	return true
}

// channelFilter draws a filter on the channels' levels: some reaching
// channels alone, some $broadcast alone, some both; three the latest
// channel alone.
func (f *fuzzer) channelFilter() string {
	return []string{"ca/#", "ca/+/b", "ca/s/#", "ca/s/+", "cb/z/#", "ca/z/+", "cb/#", "cc/#", "+/s/#", "ca/+/+", "ca/r/#", "kv/#", "kv/+", "kv/z/#"}[f.rng.IntN(14)]
}

// partitionFilter draws a filter for a SUBSCRIBE with a partition: one
// reaching channels alone, the latest channel among them, or $broadcast
// alone, never both, which the design leaves open.
func (f *fuzzer) partitionFilter() string {
	return []string{"ca/#", "ca/+/b", "ca/s/#", "cb/z/#", "ca/z/+", "ca/s/+", "cc/#", "cb/q", "kv/#"}[f.rng.IntN(9)]
}

// partitioned adds saguin-filter declarations to p's properties, one to
// three, and returns the partition they make, or bad where they make none
// valid (design.md, SUBSCRIBE, Partitioning: "Invalid syntax or
// inconsistent counts refuse each affected filter with 0x83").
func (f *fuzzer) partitioned(p *harness.Subscribe) (part *partition, bad bool) {
	r := f.rng
	if r.IntN(8) == 0 {
		decl := []string{"topic_hash(4,4)", "topic_hash(0,0)", "topic_hash(04,1)", "hash(4,1)", "topic_hash(4)"}[r.IntN(5)]
		if r.IntN(2) == 0 {
			p.Props = append(p.Props, harness.User("saguin-filter", "topic_hash(4,1)"))
			decl = "topic_hash(8,1)" // another count
		}
		p.Props = append(p.Props, harness.User("saguin-filter", decl))
		return nil, true
	}
	part = &partition{count: uint64(1 + r.IntN(4)), indices: map[uint64]bool{}}
	for range 1 + r.IntN(3) {
		i := r.Uint64N(part.count)
		part.indices[i] = true
		p.Props = append(p.Props, harness.User("saguin-filter", fmt.Sprintf("topic_hash(%d,%s%d)", part.count, []string{"", " ", "\t"}[r.IntN(3)], i)))
	}
	return part, false
}

// seenProps is what a delivery of a channel record carried of the record:
// its saguin-id, saguin-offset and saguin-timestamp, checked against the
// record itself when both are known (compare, record.compare).
type seenProps struct {
	id, offset, timestamp string
}

// compare checks a delivery's saguin-id, saguin-offset and saguin-timestamp
// against m's record where it has been read, else keeps them for it.
// Deliveries of one record on bursts' goroutines may race to keep them;
// any one serves.
func (m *message) compare(id, offset, timestamp, who string) error {
	if r := m.stored.Load(); r != nil {
		return r.compare(seenProps{id, offset, timestamp}, m.tag, who)
	}
	m.seen.Store(&seenProps{id, offset, timestamp})
	return nil
}

// compare checks a delivery's Sagüin properties, seen, against r, m's
// record: "saguin-id [...] sets the record's id"; "saguin-offset [...]
// decimal"; "saguin-timestamp [...] Unix milliseconds in decimal"
// (design.md, Sagüin properties; storage.md, records).
func (r *record) compare(seen seenProps, tag, who string) error {
	want := seenProps{r.id, strconv.FormatInt(r.offset, 10), strconv.FormatInt(r.time, 10)}
	if seen != want {
		return fmt.Errorf("%s: delivery of %s carried saguin-id %q, saguin-offset %s, saguin-timestamp %s; its record has %q, %s, %s (design.md, Sagüin properties)",
			who, tag, seen.id, seen.offset, seen.timestamp, want.id, want.offset, want.timestamp)
	}
	return nil
}

// firstOverV311 reports whether a connection of s's session before its own,
// from the one holding it when m was published, was MQTT 3.1.1, and so may
// have carried m's first transmission (couldSendFirst).
func (s *slot) firstOverV311(m *message) bool {
	holders := s.subs.holders
	if len(holders) > 0 {
		holders = holders[:len(holders)-1] // the last is s's own connection
	}
	first := 0
	for first+1 < len(holders) && holders[first+1].at < m.serial {
		first++
	}
	return first < len(holders) && slices.ContainsFunc(holders[first:], func(h holder) bool { return h.version == harness.V311 })
}

// resumed lets pass, on s's new connection, the second copies a position
// its session made again may give: the exchanges an ended position left
// in flight, at most a window, are resent with DUP (CONNECT, 9), and the
// position after it may deliver their records anew, being its own
// (restart). A kept position an earlier connection read again keeps that
// reading's bounds here, its intervals and the records it may send once
// more (repeatable), every read of this connection counting: its records
// the UNSUBSCRIBE left in flight are resent first, and those the reading
// had not handed to the writer come from the kept position anew.
func (s *slot) resumed() {
	for ch, es := range s.subs.positions {
		if len(es) > 1 {
			s.restart(ch, s.window) // the resent exchanges, at most a window
		}
		if e := s.subs.live(ch); e != nil && !e.kept && e.againOn != nil {
			e.againOn, e.againFrom = s.conn, 0
		}
	}
}

// inFlightUnread bounds the deliveries handed to s's writer and not yet
// read by its client: "A message is in flight from its hand-off to the
// writer" and the in-flight window is "the lower of the client's Receive
// Maximum and limits.max_inflight, or the cap alone for MQTT 3.1.1"
// (design.md, Delivery, One mechanism), the QoS 1 deliveries its client has
// read and not acknowledged holding their places in it.
func (s *slot) inFlightUnread() int {
	return max(0, s.window-len(s.incoming.held))
}

// orphan checks p, a first transmission read after an UNSUBACK, against the
// subscriptions s still holds (design.md, UNSUBSCRIBE: "A delivery
// prepared but not yet handed to the writer is cancelled and never sent
// when no remaining subscription is owed it"; [MQTT-3.10.4-2], -3), so one
// matching none of them was handed to the writer before the UNSUBSCRIBE,
// and at most inFlightUnread of those were left unread at the UNSUBACK.
// Option changes by a later SUBSCRIBE cancel nothing, so only the filters
// count here, not No Local or partitions.
func (s *slot) orphan(p *harness.Publish) error {
	if p.Dup || s.incoming.unsubscribed == 0 {
		return nil
	}
	for _, sub := range s.subs.current {
		if matchesFilter(sub.Filter, p.Topic) {
			return nil
		}
	}
	if s.incoming.orphans == 0 {
		return fmt.Errorf("%s: delivery %q (%q) after the UNSUBACK of step %d, matching no subscription it still holds, beyond the %d its in-flight window may have held handed to the writer: a delivery prepared for no remaining subscription is cancelled (MQTT-3.10.4-2, -3)",
			s.id, p.Topic, p.Payload[:min(len(p.Payload), 16)], s.incoming.unsubscribed, s.incoming.orphanBound)
	}
	s.incoming.orphans--
	s.f.appendStats.orphans.Add(1)
	return nil
}
