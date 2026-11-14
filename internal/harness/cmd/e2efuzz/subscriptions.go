package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// subscription keeps its original live boundary across option replacements.
// since is the SUBACK step of the uninterrupted subscription, for rule 4.
type subscription struct {
	harness.Subscription
	id    uint32
	start uint64
	since int
	// reach is the channels its filter reaches (channels.go); partition,
	// its saguin-filter declarations, nil for none.
	reach     []string
	partition *partition
	// reader is the user whose rights it delivers under (slot.user).
	reader string
}

// partition is a subscription's saguin-filter declarations (design.md,
// SUBSCRIBE, Partitioning): "delivers only the topics where
// topic_hash(topic) mod partitions == index"; "Repeated declarations use
// one count and their indices form a set".
type partition struct {
	count   uint64
	indices map[uint64]bool
}

// allows reports whether sub delivers topic: its reader may read it
// (design.md, Access and reload: "A record whose topic the user may not
// read is then skipped for that subscription. The record is not owed to
// it"), and its
// partition allows it.
func (sub *subscription) allows(topic string) bool {
	p := sub.partition
	return may(sub.reader, topic, false) && (p == nil || p.indices[harness.TopicHash(topic)%p.count])
}

type revision struct {
	at   uint64
	subs []*subscription // complete filter-byte order, as the design requires
}

// Revisions let an in-flight delivery finish with its original options after
// UNSUBSCRIBE or replacement. They belong to the session, not its connection.
type subscriptions struct {
	retainedPass bool // an MQTT 3.1.1 subscription may leave retained exchanges for resume
	current      map[string]*subscription
	history      []revision
	// holders are the session's connections, in order: each one's MQTT
	// version and the serial current when it took the session.
	holders []holder
	// positions are the session's consumer positions on the append
	// channels, by channel, oldest first (appends.go).
	positions map[string][]*epoch
	// sought are the channels a seek moved the session's position on
	// while it had none there, until a SUBSCRIBE reaches them (seeks.go).
	sought map[string]bool
	// retain is, by tag, the RETAIN value of a live exchange's first
	// transmission, where a copy has shown it: design.md, Delivery, A
	// resend: "The first-delivery RETAIN value is preserved, but an MQTT
	// 3.1.1 connection receives RETAIN 1 only on a delivery of a
	// subscription's initial pass". A first transmission shows it, and so
	// does a resend over MQTT 5; a resend over MQTT 3.1.1 carries 0 whatever
	// it is.
	retain map[string]bool
	// outbound are the session's QoS 2 deliveries as its client has seen
	// them, by the broker's packet identifier (deliveries.go): they last
	// as long as the session, across its connections (design.md, Restart:
	// "the broker restores durable sessions with their [...] in-flight
	// windows").
	outbound map[uint16]*outExchange
	// shares are the session's $share filters, and shareIDs every shared
	// subscription it was granted, by the Subscription Identifier of the
	// SUBSCRIBE granting it, kept after an UNSUBSCRIBE for the messages
	// still in flight (shares.go); ended: the session has ended, under
	// shareLog's lock.
	shares   map[string]*shareSub
	shareIDs map[uint32]*shareSub
	ended    bool
	// departedStep and departedAt are the step and the serial at which the
	// session's last connection ended, where its client saw the broker take
	// the end (slot.closed), the session away from then until its next
	// connection; 0 where it did not, or a takeover gave the session no time
	// away (latest.go).
	departedStep int
	departedAt   uint64
	// latestPasses are the initial passes its connections' SUBSCRIBEs
	// started on a latest channel (latest.go).
	latestPasses []*latestPass
	// offerByID are the worker offers its connections read and have not
	// acknowledged, by packet identifier, which a resend on a later one
	// repeats (queues.go, pubackOffer).
	offerByID map[uint16]*jobAttempt
}

// holder is one connection that held a session.
type holder struct {
	at      uint64
	version byte
}

func newSubscriptions() *subscriptions {
	return &subscriptions{current: map[string]*subscription{}, retain: map[string]bool{}, positions: map[string][]*epoch{},
		outbound: map[uint16]*outExchange{}, shares: map[string]*shareSub{}, shareIDs: map[uint32]*shareSub{}}
}

func (f *fuzzer) revise(s *slot) {
	f.serial++
	r := revision{at: f.serial}
	for _, filter := range slices.Sorted(maps.Keys(s.subs.current)) {
		r.subs = append(r.subs, s.subs.current[filter])
	}
	s.subs.history = append(s.subs.history, r)
}

// matchesFilter is MQTT's level matching, including # matching its parent
// and the exclusion of $ topics from a leading wildcard (MQTT 4.7).
// Syntax is checked separately with the shared testkit oracle.
func matchesFilter(filter, topic string) bool {
	if strings.HasPrefix(topic, "$") && (strings.HasPrefix(filter, "#") || strings.HasPrefix(filter, "+")) {
		return false
	}
	for {
		f, restF, moreF := strings.Cut(filter, "/")
		t, restT, moreT := strings.Cut(topic, "/")
		if f == "#" {
			return true
		}
		if f != "+" && f != t {
			return false
		}
		if !moreF {
			return !moreT
		}
		if !moreT {
			return restF == "#"
		}
		filter, topic = restF, restT
	}
}

// filter draws well-formed filters, including policy refusals, $saguin/
// filters among them (seeks.go) and the worker topic's shape (queues.go);
// $share filters go in SUBSCRIBEs of their own (shares.go). Broad
// overlapping filters exercise delivery even when publications choose
// previously unused topic names.
func (f *fuzzer) filter() string {
	if f.rng.IntN(3) == 0 {
		return f.channelFilter()
	}
	switch f.rng.IntN(14) {
	case 0:
		return "$SYS/#"
	case 12:
		return f.saguinFilter()
	case 13:
		return f.workerFilter()
	case 1:
		return f.word(maxTopicLength+1, maxTopicLength+4)
	case 2:
		return strings.Repeat("+/", maxTopicLevels) + "+"
	case 3, 4, 5:
		return "#"
	case 6, 7:
		return "+/#"
	case 8:
		return "+"
	case 9:
		return "+/+/+/#"
	default:
		return f.topic(1 + f.rng.IntN(maxTopicLevels))
	}
}

// subscriptionCode follows SUBSCRIBE, Refusals, in policy order, for a
// client of user: 0x87 "where the acl_file refuses the client", which it
// does "only when the user may read nothing the filter can match" (design.md,
// Access and reload; readsAny), a $SYS filter included, and a $saguin/
// filter, none of which the run draws is one of "the topics the table
// marks subscribe" ($saguin topics:
// "Any other filter under `$saguin/` is refused with 0x87, as `$SYS/` is
// ([Operations, HTTP](#http)). A filter under `$saguin/queue` that names no
// worker topic is the exception: it is refused with 0x8F
// ([SUBSCRIBE](#subscribe)). MQTT 3.1.1 gets 0x80 for both."). A saguin-filter
// partition refuses with 0x83 a filter that is invalid
// with it (bad) or that reaches $broadcast: "Partitioning is allowed on
// append, latest and a queue's own topics. It is refused with 0x83 on
// $broadcast, $share and $saguin/queue, for that filter alone". The
// session's $share filters, shares of them, count toward
// max_subscriptions too.
func subscriptionCode(user, filter string, held map[string]*subscription, shares int, reserved bool, part *partition, bad bool) byte {
	switch {
	case filter == "$SYS" || strings.HasPrefix(filter, "$SYS/") || strings.HasPrefix(filter, "$saguin/") || !readsAny(user, filter):
		return 0x87
	case len(filter) > maxTopicLength || strings.Count(filter, "/") >= maxTopicLevels:
		return 0x8F
	case held[filter] == nil && len(held)+shares >= maxSubscriptions:
		return 0x97
	case reserved, bad, part != nil && slices.Contains(reach(filter), broadcast):
		return 0x83
	}
	return 0
}

func (f *fuzzer) subscribe() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	p := &harness.Subscribe{ID: s.nextID()}
	var sid uint32
	reserved := false
	var part *partition
	bad := false
	if s.conn.Version == harness.V5 {
		if f.rng.IntN(5) == 0 {
			part, bad = f.partitioned(p)
		}
		if f.rng.IntN(4) != 0 {
			sid = 1 + f.rng.Uint32N(harness.MaxVarInt)
			p.Props = append(p.Props, harness.Property{ID: harness.SubscriptionIdentifier, Int: sid})
		}
		if f.rng.IntN(8) == 0 {
			p.Props = append(p.Props, harness.User("saguin-"+f.word(1, 3), ""))
			reserved = true
		}
	}
	for range 1 + f.rng.IntN(4) {
		filter := f.filter()
		if part != nil || bad {
			// Partitions are drawn on filters reaching channels alone, or
			// $broadcast alone, which refuses them.
			filter = f.partitionFilter()
		}
		sub := harness.Subscription{Filter: filter, QoS: byte(f.rng.IntN(3))}
		if s.conn.Version == harness.V5 {
			sub.NoLocal, sub.RetainAsPublished, sub.RetainHandling = f.rng.IntN(2) == 0, f.rng.IntN(2) == 0, 2
			if worker(filter) {
				// Mostly a worker's options, No Local now and then.
				sub.NoLocal = f.rng.IntN(6) == 0
			}
			// A filter reaching a latest channel and not $broadcast, whose
			// retained values the run does not predict, takes any Retain
			// Handling, its pass the latest channel's current values.
			if r := reach(filter); slices.ContainsFunc(r, isLatest) && !slices.Contains(r, broadcast) {
				sub.RetainHandling = byte(f.rng.IntN(3))
			}
		}
		if !testkit.TopicFilterOK(sub.Filter) {
			return true, fmt.Errorf("generated invalid filter %q", sub.Filter)
		}
		p.Subs = append(p.Subs, sub)
	}
	// The small broker packet limit also applies to SUBSCRIBE. Trim the
	// batch before predicting any changes; malformed-packet actions already
	// exercise packet-size refusals.
	var wire []byte
	for {
		var err error
		wire, err = harness.Encode(p, s.conn.Version)
		if err != nil {
			return true, err
		}
		if len(wire) <= maxPacketSize {
			break
		}
		if len(p.Subs) > 1 {
			p.Subs = p.Subs[:len(p.Subs)-1]
			continue
		}
		// A SUBSCRIBE holds a filter (MQTT-3.8.3-2): its properties go
		// first, a partition's and then the rest.
		if i := slices.IndexFunc(p.Props, func(q harness.Property) bool { return q.ID == harness.UserProperty && q.Str == "saguin-filter" }); i >= 0 {
			p.Props = slices.DeleteFunc(p.Props, func(q harness.Property) bool { return q.ID == harness.UserProperty && q.Str == "saguin-filter" })
			part, bad = nil, false
			continue
		}
		p.Props, reserved, sid = nil, false, 0
	}
	if err := f.subscribeWith(s, p, wire, sid, reserved, part, bad); err != nil || s.conn == nil {
		return true, err
	}
	return true, f.shareStep(s)
}

// worker reports whether filter is of the worker topic's shape, under
// $saguin/queue/ (queues.go).
func worker(filter string) bool { return strings.HasPrefix(filter, "$saguin/queue/") }

// subscribeWith sends p, a SUBSCRIBE of s's connection drawn with
// Subscription Identifier sid (0 for none), an unknown saguin- property
// where reserved and a partition where part (bad where invalid), its bytes
// wire, and checks its SUBACK against the refusal table, keeping what it
// grants.
func (f *fuzzer) subscribeWith(s *slot, p *harness.Subscribe, wire []byte, sid uint32, reserved bool, part *partition, bad bool) error {
	want := make([]byte, len(p.Subs))
	either := make([]byte, len(p.Subs))
	for i, sub := range p.Subs {
		if worker(sub.Filter) {
			code, alt := workerCode(s, sub, reserved, part, bad)
			if code == 1 {
				sub.QoS = 1
				s.hold(sub, sid, part)
				s.incoming.worked = true
				f.queues.stats.workers++
			} else {
				f.queues.stats.workerRefusals++
			}
			want[i], either[i] = code, alt
			continue
		}
		code := subscriptionCode(s.user, sub.Filter, s.subs.current, len(s.subs.shares), reserved, part, bad)
		if code == 0 {
			// design.md, Access and reload: features rules deny qos2; MQTT 5
			// [MQTT-3.8.4-7]: "The Server might grant a lower Maximum QoS
			// than the subscriber requested".
			if denied(s.user, "qos2") {
				sub.QoS = min(sub.QoS, 1)
			}
			if held, pass := s.hold(sub, sid, part); pass && slices.ContainsFunc(held.reach, isLatest) {
				s.startPass(held)
			}
			code = sub.QoS
		} else if s.conn.Version == harness.V311 {
			code = 0x80
		}
		if strings.HasPrefix(sub.Filter, "$saguin/") {
			f.seekStats.filters++
		}
		want[i] = code
	}
	// No publication is sent during this exchange, so recording the accepted
	// changes now also permits retained deliveries preceding the SUBACK.
	if s.conn.Version == harness.V311 {
		s.subs.retainedPass = true
	}
	s.place(p.Subs, want)
	f.revise(s)
	if slices.ContainsFunc(either, func(c byte) bool { return c != 0 }) {
		f.note("%s: SUBSCRIBE %v properties %v: want SUBACK % X, or where set % X", s.id, p.Subs, p.Props, want, either)
	} else {
		f.note("%s: SUBSCRIBE %v properties %v: want SUBACK % X", s.id, p.Subs, p.Props, want)
	}
	if err := s.conn.SendRaw(wire); err != nil {
		return err
	}
	ack, err := next[*harness.Suback](s)
	if err != nil {
		return err
	}
	if ack.ID != p.ID || len(ack.Reasons) != len(want) {
		return fmt.Errorf("SUBACK id %d codes % X, want id %d codes % X", ack.ID, ack.Reasons, p.ID, want)
	}
	for i, code := range ack.Reasons {
		if code != want[i] && (either[i] == 0 || code != either[i]) {
			return fmt.Errorf("SUBACK 0x%02X for %q, want 0x%02X", code, p.Subs[i].Filter, want[i])
		}
	}
	return s.drain()
}

// hold records sub, accepted with Subscription Identifier id (0 for none),
// in s's session, and returns it, and whether it starts a retained pass,
// which mostDeliveries counts. A SUBSCRIBE repeating a filter "replaces its
// options and keeps its position and live-start boundaries" (design.md,
// Flows, SUBSCRIBE, The start point), and its uninterrupted SUBACK step.
// Retain Handling 0 starts a pass, a repeated filter's included
// (MQTT-3.8.4-3), and 1 only where the filter is new (MQTT-3.3.1-10). An
// MQTT 3.1.1 subscription carries none and holds 0, its zero value, as
// MQTT 3.1.1 sends the retained values at every SUBSCRIBE (MQTT-3.3.1-6).
func (s *slot) hold(sub harness.Subscription, id uint32, part *partition) (held *subscription, pass bool) {
	old := s.subs.current[sub.Filter]
	pass = sub.RetainHandling == 0 || (sub.RetainHandling == 1 && old == nil)
	if pass {
		s.incoming.passes++
	}
	next := &subscription{Subscription: sub, id: id, start: s.f.serial, since: s.f.step, reach: reach(sub.Filter), partition: part, reader: s.user}
	if part != nil {
		s.f.appendStats.partitioned++
	}
	if old != nil {
		next.start, next.since = old.start, old.since
	}
	s.subs.current[sub.Filter] = next
	return next, pass
}

func (f *fuzzer) unsubscribe() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil && len(s.subs.current) > 0 })
	if s == nil {
		return false, nil
	}
	filters := slices.Sorted(maps.Keys(s.subs.current))
	return true, f.unsubscribeFrom(s, filters[f.rng.IntN(len(filters))])
}

// unsubscribeFrom has s's connection unsubscribe from filter, which its
// session holds, and checks the UNSUBACK.
func (f *fuzzer) unsubscribeFrom(s *slot, filter string) error {
	p := &harness.Unsubscribe{ID: s.nextID(), Filters: []string{filter}}
	if s.conn.Version == harness.V5 && f.rng.IntN(3) == 0 {
		p.Props = harness.Properties{harness.User("saguin-"+f.word(1, 3), "")}
	}
	f.note("%s: UNSUBSCRIBE %q", s.id, filter)
	if err := s.conn.Send(p); err != nil {
		return err
	}
	ack, err := next[*harness.Unsuback](s)
	if err != nil {
		return err
	}
	if ack.ID != p.ID || (s.conn.Version == harness.V5 && !slices.Equal(ack.Reasons, []byte{0})) {
		return fmt.Errorf("UNSUBACK id %d codes % X, want id %d success", ack.ID, ack.Reasons, p.ID)
	}
	delete(s.subs.current, filter)
	s.unplace(filter)
	f.revise(s)
	bound := s.inFlightUnread()
	s.incoming.unsubscribed, s.incoming.orphans, s.incoming.orphanBound = f.step, bound, bound
	if worker(filter) {
		// "Its offers not yet sent are cancelled" (design.md, Delivery,
		// Queue jobs): one handed to the writer before may still come, a
		// worker receiving "one job at a time per queue".
		s.incoming.cancelBound, s.incoming.unsubscribedWorker = 1, f.step
		f.queues.stats.unsubscribedAt++
	}
	return s.drain()
}
