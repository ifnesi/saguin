package main

import (
	"bufio"
	"bytes"
	"cmp"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The faults of subscription and delivery the fake broker can be given,
// each breaking one rule the runner checks (0029a.md, the agreed plan).
const (
	// strayDelivery also sends each message, at QoS 0, to every session
	// holding subscriptions none of whose filters matches its topic.
	strayDelivery fault = "stray delivery"
	// endedKept ends a durable session's position on an append channel or
	// a queue with the last subscription reaching it, as rc2 did before
	// 0048a, so the next subscription reads the channel from the floor.
	endedKept fault = "kept position ended"
	// secondCopy sends each QoS 1 delivery twice while connected, under two
	// packet identifiers, neither marked DUP.
	secondCopy fault = "second copy"
	// replayedTwice sends twice, under two packet identifiers, neither
	// marked DUP, a record a kept position read again while its first
	// delivery was unsettled.
	replayedTwice fault = "replayed twice"
	// reordered keeps a subscriber's live delivery back until the next one
	// from the same publisher, and sends that one first.
	reordered fault = "reordered"
	// lostDelivery never sends every fourth delivery of a QoS 1 message.
	lostDelivery fault = "lost delivery"
	// cappedGrant grants a subscription asking QoS 2 only QoS 1.
	cappedGrant fault = "capped grant"
	// ignoresNoLocal delivers a client's own publications through its No
	// Local subscriptions.
	ignoresNoLocal fault = "ignores No Local"
	// keepsRetain sends an MQTT 5 subscriber's live deliveries with the
	// RETAIN flag they were published with, whatever Retain As Published.
	keepsRetain fault = "keeps RETAIN"
	// wrongIdentifiers sends with each live delivery the Subscription
	// Identifiers of the session's subscriptions it did not match, and
	// none of those it did.
	wrongIdentifiers fault = "wrong Subscription Identifiers"
	// recomputesRetain sends a resend with the RETAIN a live delivery on
	// its current connection would carry, not its first transmission's.
	recomputesRetain fault = "recomputes RETAIN"
	// resentPublish resends a delivery past its PUBREC as a PUBLISH after a
	// reconnect, not as its PUBREL.
	resentPublish fault = "PUBLISH resent past its PUBREC"
	// sharedToAll sends each message through a $share filter to every
	// member of its group, not to one.
	sharedToAll fault = "shared message to every member"
)

// The limits the fake applies beyond model.go's constants: the runner's
// options where it sets them, else config.md's defaults, max_inflight 20
// and max_subscriptions 10000.
var (
	fakeInflight      = cmp.Or(options("", 1).MaxInflight, 20)
	fakeSubscriptions = cmp.Or(options("", 1).MaxSubscriptions, 10000)
)

// fakeRecord is a stored message, as the fake delivers it.
type fakeRecord struct {
	channel   int64 // its channel's id, $broadcast 1
	id        string
	ms        int64 // its time, Unix milliseconds
	offset    int64
	topic     string
	qos       byte
	retain    bool               // as published
	publisher string             // the publisher's client id, for No Local
	props     harness.Properties // as published, but its Topic Alias
	payload   []byte
	received  time.Time // when it arrived, for its Message Expiry
}

// fakeSub is one subscription of a session.
type fakeSub struct {
	filter  string
	qos     byte   // as granted
	noLocal bool   // No Local
	keep    bool   // Retain As Published
	id      uint32 // its Subscription Identifier, 0 for none
	// start is the offset last stored when it was accepted: records above
	// it are live for it (design.md, Flows, SUBSCRIBE, The start point).
	start int64
	// part is its saguin-filter partition, nil for none; reach the ids of
	// the channels its filter reaches (channels_test.go).
	part  *fakePartition
	reach []int64
	// shared is a $share subscription's filter part, "" for an ordinary
	// one, and shareReach the ids of the channels that part reaches; a
	// $share subscription makes no position, so its reach is empty.
	shared     string
	shareReach []int64
}

// delivery is one message on its way to a session: a live record, or a
// retained value of one subscription's pass; once worked out, what it
// carries, which a resend keeps.
type delivery struct {
	rec     *fakeRecord
	pass    []string // the filters starting this initial pass; nil for live records
	share   string   // the $share filter it goes through; "" for none
	away    bool     // queued while its session was away
	id      uint16   // its packet identifier, at QoS 1
	qos     byte
	retain  bool
	dup     bool
	again   bool     // queued by a kept position read again, its first delivery unsettled
	ids     []uint32 // its Subscription Identifiers
	filters []string // the subscriptions it went through
	multi   bool     // one of them reaches several channels (saguin-channel)
	// sent: at QoS 1 or 2, sent on the session's connection and awaiting
	// its PUBACK, PUBREC or PUBCOMP; released: at QoS 2, its PUBREC 0x00
	// taken, so its PUBREL goes in place of the PUBLISH, and its PUBCOMP
	// settles it.
	sent, released bool
	// attempt is the attempt a worker offer makes, nil for any other
	// delivery, and props an offer's properties, its resend's too
	// (queue_test.go).
	attempt *fakeAttempt
	props   harness.Properties
}

// seen counts the situations a run drove the fake through, which
// TestRunPasses requires.
type seen struct {
	waited       int // deliveries waited for PUBACKs the subscriber withheld, its window full
	overlapping  int // a live delivery went through two or more subscriptions
	tooLarge     int // a delivery was over its subscriber's Maximum Packet Size
	unsubscribed int // an UNSUBSCRIBE removed a filter a delivery awaiting PUBACK went through
	resumed      int // a reconnect kept a session holding subscriptions
	wills        int // a connection's end published its Will
	channelled   int // a record landed in a configured channel
	full         int // a PUBLISH was refused with 0x97 for its channel's max_bytes
	cancelled    int // a reconnect or takeover keeping its session cancelled a delayed Will
	positions    int // a subscription made a position on an append channel
	kept         int // a subscription read again a position its durable session kept with no subscription
	keptAgain    int // a record a kept position read again was queued anew, its first delivery still unsettled
	retention    int // retention deleted a record
	wrongLogins  int // a CONNECT was refused for a wrong or missing credential
	deniedWills  int // a CONNECT was refused for a Will the acl_file refuses
	unauthorized int // a PUBLISH or a filter was refused with 0x87 for the acl_file's rules
	unreadable   int // a record a subscription matched was skipped, its user not allowed to read it
	// released counts the QoS 2 publications a PUBREL brought into their
	// channel, releasedLater those of them held on an earlier connection,
	// repeated the PUBLISHes repeating a held identifier, and
	// unknownRelease the PUBRELs for an identifier none was held under.
	released, releasedLater, repeated, unknownRelease int
	// Of QoS 2 deliveries: receipted counts the PUBRECs 0x00 a PUBREL
	// answered, repeatedReceipt the PUBRECs repeating one so answered, and
	// strayReceipt the PUBRECs naming none. A PUBREL sent again on a later
	// connection is shown reached by TestPubrelResendIsCaught, as how often
	// it comes depends on timing.
	receipted, repeatedReceipt, strayReceipt int
	answered                                 int // a $saguin request's answer was sent (seeks_test.go)
	shared                                   int // a delivery went through a $share filter
	sharedAmong                              int // a record matched a $share group of two or more connected members
	// Of latest channels (latest_test.go): latestReplaced counts the values
	// replaced in place, latestDeleted the tombstones stored, latestPass
	// the current values passes sent, latestSkipped the replaced values
	// skipped, latestSettled the recovered exchanges of replaced values
	// settled as if sent, and latestReturned the changes made while a
	// session was away delivered on its return.
	latestReplaced, latestDeleted, latestPass, latestSkipped, latestSettled, latestReturned int
	// Of the queue (queue_test.go): jobs stored, offers made, holds begun,
	// jobs finished by an ack, attempts ended by a refusing PUBACK, a
	// return, a hold's end or a worker's going, responses that changed
	// nothing, offers resent, acks of a worker unsubscribed since, and
	// dead letters by reason.
	jobs, offers, held, completed, refused, returned, timedOut, workerGone int
	staleResponses, resentOffers, unsubscribedAck                          int
	deadLettered                                                           map[string]int
}

// topicMatches reports whether filter matches topic, MQTT 5 4.7 and 3.1.1
// 4.7: "#" "represents the parent and any number of child levels", "+"
// "matches only one topic level", and "The Server MUST NOT match Topic
// Filters starting with a wildcard character (# or +) with Topic Names
// beginning with a $ character [MQTT-4.7.2-1]".
func topicMatches(filter, topic string) bool {
	if strings.HasPrefix(topic, "$") && (strings.HasPrefix(filter, "+") || strings.HasPrefix(filter, "#")) {
		return false
	}
	fs, ts := strings.Split(filter, "/"), strings.Split(topic, "/")
	for i, f := range fs {
		switch {
		case f == "#":
			return true
		case i == len(ts) || (f != "+" && f != ts[i]):
			return false
		}
	}
	return len(fs) == len(ts)
}

// matchesAny reports whether any of s's filters matches topic.
func matchesAny(s *fakeSession, topic string) bool {
	for f := range s.subs {
		if topicMatches(f, topic) {
			return true
		}
	}
	return false
}

// saguinProperty reports whether q is a User Property under the reserved
// saguin- prefix.
func saguinProperty(q harness.Property) bool {
	return q.ID == harness.UserProperty && strings.HasPrefix(q.Str, "saguin-")
}

// subscribe answers p as design.md, Flows, SUBSCRIBE has the broker answer
// it, with no acl_file and no channel configured, so that every filter
// reaches $broadcast alone: "A malformed filter is a malformed packet, with
// 0x81", "Packet validity precedes per-filter policy"; each filter then
// refused or granted, "Filters are processed in packet order"; a granted one
// held with its options, its retained pass, as Retain Handling allows,
// following the SUBACK; a $saguin/ filter it refuses by code
// (filterRefusal). A $share filter (MQTT 5 4.8.2) with an empty filter part
// is malformed ([MQTT-4.8.2-2]); with No Local, a protocol error ("On
// $share, it is a protocol error that closes the connection with 0x82");
// from a user denied share, DISCONNECT 0x9E ("0x9E for [...] shared
// subscriptions its CONNACK withheld"); "MQTT 3.1.1: refused (0x80)";
// otherwise refused or granted as any filter, "measured whole", 0x83 for a
// partition, and held with no retained pass ("no retained values"). It
// runs under the lock (owns).
func (b *fake) subscribe(c *fakeConn, s *fakeSession, p *harness.Subscribe) bool {
	shares := false
	for _, o := range p.Subs {
		part, shared := sharePart(o.Filter)
		shares = shares || shared
		switch {
		case !testkit.TopicFilterOK(o.Filter) || (shared && part == ""):
			b.refuse(c, harness.ErrMalformed)
			return false
		case shared && o.NoLocal && c.version == harness.V5:
			b.refuse(c, harness.ErrProtocol)
			return false
		}
	}
	if shares && c.version == harness.V5 && b.fault != ignoredACL && denied(s.user, "share") {
		b.withheld(c, 0x9E)
		return false
	}
	reserved := slices.ContainsFunc(p.Props, func(q harness.Property) bool { return saguinProperty(q) && q.Str != "saguin-filter" })
	part, partBad := fakePartitionOf(p.Props)
	sid, _ := p.Props.Get(harness.SubscriptionIdentifier)
	ack := &harness.Suback{ID: p.ID, Reasons: make([]byte, len(p.Subs))}
	var passes []string
	for i, o := range p.Subs {
		if strings.HasPrefix(o.Filter, "$saguin/queue/") {
			// A worker's (queue_test.go): its offers are the queue's, no
			// retained pass nor position.
			code := b.workerCode(c, s, o.Filter, o.QoS, o.NoLocal, reserved || part != nil || partBad)
			if code == 1 {
				s.subs[o.Filter] = &fakeSub{filter: o.Filter, qos: 1, id: sid.Int}
			}
			ack.Reasons[i] = code
			continue
		}
		old := s.subs[o.Filter]
		code := b.filterRefusal(s, o.Filter, old != nil, reserved)
		// design.md, SUBSCRIBE, Partitioning: "Invalid syntax or
		// inconsistent counts refuse each affected filter with 0x83";
		// "refused with 0x83 on $broadcast, $share and $saguin/queue, for
		// that filter alone".
		filterPart, shared := sharePart(o.Filter)
		if code == 0 && (partBad || (part != nil && (shared || slices.Contains(b.fakeReach(o.Filter), 1)))) {
			code = 0x83
		}
		if shared && c.version != harness.V5 {
			code = 0x80
		}
		if code != 0 {
			if c.version != harness.V5 {
				code = 0x80 // "MQTT 3.1.1 gets 0x80 for each"
			}
			ack.Reasons[i] = code
			continue
		}
		// The table: $broadcast is "Granted for QoS 0 / 1 / 2" "0 / 1 / 2".
		grant := o.QoS
		// design.md, Access and reload: features rules deny qos2;
		// [MQTT-3.8.4-7]: "The Server might grant a lower Maximum QoS than
		// the subscriber requested".
		if b.fault == cappedGrant || (b.fault != ignoredACL && denied(s.user, "qos2")) {
			grant = min(grant, 1)
		}
		// "The core captures that channel's tail when it accepts the
		// subscription"; "A SUBSCRIBE repeating an existing filter replaces
		// its options and keeps its position and live-start boundaries".
		sub := &fakeSub{filter: o.Filter, qos: grant, noLocal: o.NoLocal, keep: o.RetainAsPublished, id: sid.Int, start: b.offset,
			part: part, reach: b.fakeReach(o.Filter)}
		if shared {
			sub.reach, sub.shared, sub.shareReach = nil, filterPart, b.fakeReach(filterPart)
		}
		if old != nil {
			sub.start = old.start
		}
		s.subs[o.Filter] = sub
		ack.Reasons[i] = grant
		// MQTT 5 3.3.1.3: Retain Handling 0 sends the retained values
		// [MQTT-3.3.1-9], 1 only "if the subscription did not already
		// exist" [MQTT-3.3.1-10], 2 none [MQTT-3.3.1-11]; MQTT 3.1.1 has
		// only the first.
		if !shared && (o.RetainHandling == 0 || (o.RetainHandling == 1 && old == nil)) {
			passes = append(passes, o.Filter)
		}
	}
	c.send(ack)
	b.position(s)
	// One copy: "a record matching several of a session's non-shared
	// subscriptions is sent once". A value an earlier pass still has
	// queued, unsent, is the same copy: this pass's filters join it, and
	// it goes out once, through every eligible subscription with its
	// current options ("A SUBSCRIBE repeating an existing filter replaces
	// its options").
	queued := map[*fakeRecord]*delivery{}
	for _, d := range s.queue {
		if len(d.pass) > 0 {
			queued[d.rec] = d
		}
	}
	for _, f := range passes {
		for _, r := range slices.Concat(b.retainedMatching(f), b.currentMatching(f)) {
			if !b.readable(s, r) {
				continue
			}
			d := queued[r]
			if d == nil {
				d = &delivery{rec: r}
				queued[r] = d
				s.queue = append(s.queue, d)
			}
			if !slices.Contains(d.pass, f) {
				d.pass = append(d.pass, f)
			}
		}
	}
	b.pump(s)
	return true
}

// filterRefusal is the code design.md, Flows, SUBSCRIBE, Refusals gives a
// filter of s, held telling whether s holds it already, in "refusal order
// 0x87, 0x8F, 0x97, 0x83", or 0: Operations, HTTP: "a subscription to
// $SYS/… is refused with 0x87"; "0x8F for a filter of the wrong shape [...]
// or one over limits.max_topic_length or limits.max_topic_levels"; "0x97 for
// a filter past limits.max_subscriptions", "Earlier accepted filters consume
// subscription capacity before later filters", a repeated filter taking none;
// and "For an unknown saguin- property, it uses 0x83 on every otherwise
// eligible filter", a saguin-filter included (Partitioning: "refused with
// 0x83 on $broadcast, $share and $saguin/queue, for that filter alone");
// $saguin topics:
// "Any other filter under `$saguin/` is refused with 0x87, as `$SYS/` is
// ([Operations, HTTP](#http)). A filter under `$saguin/queue` that names no
// worker topic is the exception: it is refused with 0x8F
// ([SUBSCRIBE](#subscribe)). MQTT 3.1.1 gets 0x80 for both."
// None drawn here names a worker or seek reply topic.
func (b *fake) filterRefusal(s *fakeSession, filter string, held, reserved bool) byte {
	read := filter
	if part, ok := sharePart(filter); ok {
		read = part // what a $share filter can match
	}
	levels := strings.Split(filter, "/")
	switch {
	case levels[0] == "$SYS" || levels[0] == "$saguin":
		return 0x87
	case !b.fakeReads(s.user, read):
		b.seen.unauthorized++
		return 0x87
	case len(filter) > maxTopicLength || len(levels) > maxTopicLevels:
		return 0x8F
	case !held && len(s.subs) >= fakeSubscriptions:
		return 0x97
	case reserved:
		return 0x83
	}
	return 0
}

// retainedMatching is the current retained values whose topics filter
// matches, in offset order.
func (b *fake) retainedMatching(filter string) []*fakeRecord {
	var rs []*fakeRecord
	for topic, r := range b.retained {
		if topicMatches(filter, topic) {
			rs = append(rs, r)
		}
	}
	slices.SortFunc(rs, func(x, y *fakeRecord) int { return cmp.Compare(x.offset, y.offset) })
	return rs
}

// unsubscribe answers p as design.md, Flows, SUBSCRIBE, UNSUBSCRIBE has the
// broker answer it: a malformed filter is a malformed packet; each filter
// held is removed, 0x00, any other 0x11 (MQTT 5 3.11.3: "No subscription
// existed"); an unknown saguin- property "is reported on the UNSUBACK as
// saguin-unread unless the client set Request Problem Information to 0".
// "In-flight messages are finished": those awaiting PUBACK stay, while a
// delivery not yet sent goes through the subscriptions left alone
// (Delivery, A resend: "Subscription changes affect new deliveries"). It
// runs under the lock (owns).
func (b *fake) unsubscribe(c *fakeConn, s *fakeSession, p *harness.Unsubscribe) bool {
	if slices.ContainsFunc(p.Filters, func(f string) bool { return !testkit.TopicFilterOK(f) }) {
		b.refuse(c, harness.ErrMalformed)
		return false
	}
	ack := &harness.Unsuback{ID: p.ID, Reasons: make([]byte, len(p.Filters))}
	for i, f := range p.Filters {
		if s.subs[f] == nil {
			ack.Reasons[i] = 0x11
			continue
		}
		if slices.ContainsFunc(s.inflight, func(d *delivery) bool { return d.sent && slices.Contains(d.filters, f) }) {
			b.seen.unsubscribed++
		}
		if name, ok := strings.CutPrefix(f, "$saguin/queue/"); ok {
			if s.unsubscribed == nil {
				s.unsubscribed = map[string]*fakeSub{}
			}
			s.unsubscribed[name] = s.subs[f] // for offersAfterUnsubscribe (queue_test.go)
		}
		delete(s.subs, f)
	}
	b.unposition(s)
	if !c.quiet {
		for _, q := range p.Props {
			if saguinProperty(q) {
				ack.Props = append(ack.Props, harness.User("saguin-unread", q.Str))
			}
		}
	}
	c.send(ack)
	return true
}

// away ends s's hold by its connection, the session kept: a retained
// pass's deliveries not yet sent are dropped, and those awaiting their
// acknowledgement go again on its next connection, with DUP, or past their
// PUBREC as their PUBREL (pump) (design.md, Delivery,
// Preparation: "deliveries prepared and never handed to the writer are
// cancelled"; "a live scan's position stays before their records, so a
// continuing session receives them later"; "A retained pass's prepared
// deliveries are dropped"; CONNECT, 9: "in-flight messages [...] resends them
// with their packet identifiers and in their order. Resent messages have DUP
// set"). It runs under the lock.
func (s *fakeSession) away() {
	s.conn = nil
	s.queue = slices.DeleteFunc(s.queue, func(d *delivery) bool { return len(d.pass) > 0 })
	for _, d := range s.inflight {
		d.sent, d.dup = false, true
	}
}

// settle takes the client's PUBACK, or with comp its PUBCOMP, for
// packetID: design.md, Delivery, Positions: "an acknowledgement, PUBACK,
// PUBCOMP or a refusing PUBREC, settles its own delivery"; Words: Packet
// identifier, "An acknowledgement settles only the exchange of its own
// direction and phase": a PUBACK a delivery at QoS 1, a PUBCOMP one past its
// PUBREC; Peer obligations: "Only the matching valid acknowledgement ends
// it", so one matching none changes nothing. It runs under the lock.
func (b *fake) settle(s *fakeSession, p *harness.Ack) {
	comp := p.Kind == harness.PUBCOMP
	if i := slices.IndexFunc(s.inflight, func(d *delivery) bool {
		return d.id == p.ID && ((comp && d.released) || (!comp && d.qos == 1))
	}); i >= 0 {
		d := s.inflight[i]
		s.inflight = slices.Delete(s.inflight, i, i+1)
		if d.attempt != nil {
			b.offerAcked(d, p.Reason) // a worker offer (queue_test.go)
		}
		b.pump(s)
	}
}

// receipt takes the client's PUBREC p on c: design.md, Delivery, Positions:
// a refusing PUBREC "settles its own delivery (MQTT-4.4.0-2). Once a
// successful PUBREC is accepted for an outgoing QoS 2 exchange, every
// further PUBREC for it is answered with a successful PUBREL under the
// same identifier. [...] They change no delivery state [...]. A PUBREC
// naming no outgoing QoS 2 exchange, a QoS 1 delivery's identifier
// included, gets PUBREL 0x92, a plain PUBREL on MQTT 3.1.1, and changes
// nothing"; any other gets the delivery's PUBREL, the delivery kept until
// its PUBCOMP (MQTT 5 [MQTT-4.3.3-4], [MQTT-4.3.3-5]). It runs under the
// lock.
func (b *fake) receipt(c *fakeConn, s *fakeSession, p *harness.Ack) {
	i := slices.IndexFunc(s.inflight, func(d *delivery) bool { return d.id == p.ID && d.qos == 2 && (d.sent || d.released) })
	switch {
	case i < 0:
		b.seen.strayReceipt++
		c.send(&harness.Ack{Kind: harness.PUBREL, ID: p.ID, Reason: 0x92}) // MQTT 3.1.1's has no code
	case s.inflight[i].released:
		b.seen.repeatedReceipt++
		c.send(&harness.Ack{Kind: harness.PUBREL, ID: p.ID})
	case p.Reason >= 0x80:
		s.inflight = slices.Delete(s.inflight, i, i+1)
		b.pump(s)
	default:
		b.seen.receipted++
		s.inflight[i].released = true
		c.send(&harness.Ack{Kind: harness.PUBREL, ID: p.ID})
	}
}

// written is c's writer having written a QoS 0 delivery, which settles it
// and frees its place in the window. It takes the lock.
func (b *fake) written(c *fakeConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c.unwritten--
	if s := b.sessions[c.session]; s != nil && s.conn == c {
		b.pump(s)
	}
}

// distribute takes a stored record to the sessions it may be owed to
// (design.md, Channels, $broadcast: "Every message goes in, whatever its
// QoS"; "Each session has one cursor"): each session, connected or away,
// with a filter matching its topic queues it; and a retained publish
// "takes the flag", an empty one clearing it. Whether it goes, at what QoS
// and with what, is worked out when it is sent (deliverLive). It runs
// under the lock.
func (b *fake) distribute(r *fakeRecord) {
	if r == nil {
		return
	}
	b.records = append(b.records, r)
	defer b.retain(r.channel)
	switch {
	case r.channel != 1:
	case r.retain && len(r.payload) == 0:
		delete(b.retained, r.topic)
	case r.retain:
		b.retained[r.topic] = r
	}
	for _, s := range b.sessions {
		if r.channel != 1 && !s.positions[r.channel] {
			continue
		}
		if matchesAny(s, r.topic) || (b.fault == strayDelivery && len(s.subs) > 0) {
			s.queue = append(s.queue, &delivery{rec: r, away: s.conn == nil})
			b.pump(s)
		}
	}
	b.share(r)
}

// share hands r to one member of each $share group whose filter part
// matches its topic, among the connected members allowed to read it,
// round-robin by client id (design.md, Flows, SUBSCRIBE, A shared group:
// "Each message goes to one member with room"; "Selection is round-robin
// by complete client id, starting after the last selected id and wrapping
// once"; Delivery, Queue jobs: "allowed by the acl_file, as a shared
// group's members are"), which sharedToAll breaks. A record no member is
// connected for goes to none: the runner owes nothing of a group, so the
// fake keeps no group backlog. It runs under the lock.
func (b *fake) share(r *fakeRecord) {
	groups := map[string][]string{}
	for _, id := range slices.Sorted(maps.Keys(b.sessions)) {
		s := b.sessions[id]
		for filter, sub := range s.subs {
			if s.conn != nil && sub.shared != "" && topicMatches(sub.shared, r.topic) && b.fakeMay(s.user, r.topic, false) {
				groups[filter] = append(groups[filter], id)
			}
		}
	}
	if b.lastShared == nil {
		b.lastShared = map[string]string{}
	}
	for _, filter := range slices.Sorted(maps.Keys(groups)) {
		members := groups[filter]
		if len(members) > 1 {
			b.seen.sharedAmong++
		}
		if b.fault != sharedToAll {
			i := slices.IndexFunc(members, func(id string) bool { return id > b.lastShared[filter] })
			members = members[max(i, 0) : max(i, 0)+1]
			b.lastShared[filter] = members[0]
		}
		for _, id := range members {
			s := b.sessions[id]
			s.queue = append(s.queue, &delivery{rec: r, share: filter})
			b.pump(s)
		}
	}
}

// pump hands s's waiting deliveries to its connection while the in-flight
// window has room (design.md, Delivery: "The window is the lower of the
// client's Receive Maximum and limits.max_inflight"; "A message is in flight
// from its hand-off to the writer"; "QoS 0 settles on successful writing"):
// first those to send again, in their order, then the rest. CONNECT, 9:
// "Outstanding PUBLISHes are offered in stored sending order, only while the
// new connection has quota"; "PUBREL and other control packets are not
// blocked by that quota"; "New application deliveries wait behind recovered
// PUBLISHes". It runs under the lock.
func (b *fake) pump(s *fakeSession) {
	c := s.conn
	if c == nil {
		return
	}
	// sent counts s's deliveries in flight on c.
	sent := func() int {
		n := 0
		for _, d := range s.inflight {
			if d.sent {
				n++
			}
		}
		return n
	}
	blocked := false
	for _, d := range slices.Clone(s.inflight) { // a resend too large leaves inflight
		switch {
		case c.finished:
			return
		case d.sent:
		case d.released && b.fault != resentPublish:
			d.sent = true
			c.send(&harness.Ack{Kind: harness.PUBREL, ID: d.id})
		case blocked || sent()+c.unwritten >= c.window:
			blocked = true
		default:
			b.resend(c, s, d)
		}
	}
	for !c.finished && !blocked && len(s.queue) > 0 && sent()+c.unwritten < c.window {
		d := s.queue[0]
		s.queue = s.queue[1:]
		b.deliver(c, s, d)
	}
	if sent() >= c.window && (blocked || len(s.queue) > 0) {
		b.seen.waited++
	}
	b.dispatch() // a worker with room may take a job (queue_test.go)
}

// resend sends d again on c, its session kept: CONNECT, 9, "with their
// packet identifiers and in their order", DUP set; Delivery: "A resend
// uses the Subscription Identifiers and delivery properties saved for its
// first transmission"; "A recovered exchange that cannot be encoded for
// the new connection is counted as missed and settled as if sent". It runs
// under the lock.
func (b *fake) resend(c *fakeConn, s *fakeSession, d *delivery) {
	if b.replaced(d.rec) && b.fault != resentReplaced {
		// "A recovered exchange whose latest value a newer one replaced is
		// settled as if sent, and not counted" (latest_test.go).
		b.seen.latestSettled++
		s.inflight = slices.DeleteFunc(s.inflight, func(e *delivery) bool { return e == d })
		return
	}
	if d.attempt != nil {
		b.seen.resentOffers++
		if b.fault == freshResendID {
			i := slices.IndexFunc(d.props, func(q harness.Property) bool { return q.ID == harness.CorrelationData })
			d.props = slices.Clone(d.props)
			d.props[i].Data = []byte(newAttemptID())
		}
	}
	if b.fault == recomputesRetain {
		d.retain = c.version == harness.V5 && d.rec.retain && slices.ContainsFunc(d.filters, func(f string) bool {
			return s.subs[f] != nil && s.subs[f].keep
		})
	}
	if b.transmit(c, d) {
		d.sent = true
		return
	}
	s.inflight = slices.DeleteFunc(s.inflight, func(e *delivery) bool { return e == d })
}

// deliver sends d, a queued delivery, on c: a retained value through its
// pass's subscription, or a live record. It runs under the lock.
func (b *fake) deliver(c *fakeConn, s *fakeSession, d *delivery) {
	if d.share != "" {
		b.deliverShared(c, s, d)
		return
	}
	if len(d.pass) > 0 {
		b.deliverRetained(c, s, d)
		return
	}
	if b.fault == reordered && b.swap(c, s, d) {
		return
	}
	b.deliverLive(c, s, d)
}

// deliverRetained sends d, a retained value of a subscription's pass, or
// a latest channel's current value: design.md, Channels, $broadcast:
// "RETAIN is 1 on retained values sent to a new subscription"; SUBSCRIBE,
// Options: "a `latest` channel's initial pass to a client: its current values
// carry RETAIN 1"; The start point: "A value
// whose flag ends during the pass is not sent"; Options: "No Local is
// honoured everywhere"; One copy: partition checks apply; Delivery,
// Message Expiry: on a configured channel an expired message "is still
// delivered"; at "the minimum of the QoS of the originally published
// message and the Maximum QoS granted" [MQTT-3.8.4-8], with the
// eligible subscriptions' identifiers. It runs under the lock.
func (b *fake) deliverRetained(c *fakeConn, s *fakeSession, d *delivery) {
	latest := fakeLatest(d.rec.channel)
	current := b.retained[d.rec.topic] == d.rec || (latest && b.latest[d.rec.topic] == d.rec)
	if !current || (!latest && d.rec.expired()) {
		return
	}
	// One copy, with one identifier per eligible filter, in filter-byte
	// order (design.md, SUBSCRIBE), including identical identifiers
	// [MQTT-3.3.4-3]. RH2 filters never entered d.pass.
	slices.Sort(d.pass)
	d.retain = true // a retained value's pass, or a latest channel's initial pass
	for _, filter := range d.pass {
		sub := s.subs[filter]
		if sub == nil || (sub.noLocal && d.rec.publisher == s.id) || !sub.part.allows(d.rec.topic) {
			continue
		}
		d.qos = max(d.qos, min(d.rec.qos, sub.qos))
		d.multi = d.multi || len(sub.reach) > 1
		d.filters = append(d.filters, filter)
		if sub.id != 0 {
			d.ids = append(d.ids, sub.id)
		}
	}
	if len(d.filters) == 0 {
		return
	}
	if b.fault == missingPassIdentifier && len(d.ids) > 1 {
		d.ids = d.ids[:len(d.ids)-1]
	}
	if b.handOff(c, s, d) && latest {
		b.seen.latestPass++
	}
}

// deliverShared sends d, a live record, through its $share subscription,
// on its own: design.md, Flows, SUBSCRIBE, One copy, "Each $share
// subscription is handled on its own"; at the lower of the publication's
// QoS and the member's grant [MQTT-4.8.2-3]; RETAIN as published where the
// subscription asks Retain As Published, else 0 [MQTT-3.3.1-12]; with its
// Subscription Identifier. A subscription gone since goes without it, and
// an expired $broadcast message is skipped. It runs under the lock.
func (b *fake) deliverShared(c *fakeConn, s *fakeSession, d *delivery) {
	sub := s.subs[d.share]
	if sub == nil || (d.rec.expired() && d.rec.channel == 1) || b.replaced(d.rec) {
		return
	}
	b.seen.shared++
	d.qos, d.retain = min(d.rec.qos, sub.qos), c.version == harness.V5 && sub.keep && d.rec.retain
	d.filters, d.multi = []string{sub.filter}, len(sub.shareReach) > 1
	if sub.id != 0 {
		d.ids = []uint32{sub.id}
	}
	b.handOff(c, s, d)
}

// deliverLive sends d, a live record, as design.md, Flows, SUBSCRIBE, One
// copy has it: "a record matching several of a session's non-shared
// subscriptions is sent once"; "Only subscriptions eligible for the record
// contribute: topic, live-start boundary, No Local [...]"; "The delivery
// uses the lower of the publication's QoS and the highest contributing
// grant"; "Subscription Identifiers appear in complete filter-byte order,
// once for each contributing subscription having one"; "A live $broadcast
// delivery preserves RETAIN when any contributor requests Retain As
// Published", RETAIN being 0 otherwise [MQTT-3.3.1-12], and on every live
// delivery in MQTT 3.1.1, which has no such option (3.1.1 [MQTT-3.3.1-9]).
// Delivery, Message Expiry: "On $broadcast an expired message is skipped
// for that subscriber". Each planted fault of delivery breaks one of these.
// It runs under the lock.
func (b *fake) deliverLive(c *fakeConn, s *fakeSession, d *delivery) {
	if b.fault == lostDeadLetter && strings.HasPrefix(d.rec.topic, deadLetterPrefix) {
		return
	}
	if d.rec.expired() && d.rec.channel == 1 {
		return // "On a configured channel it is still delivered"
	}
	// Delivery, Order: "On latest, they follow offset order of changes and
	// skip replaced values", which replayedHistory breaks; lostTombstone
	// sends no empty payload (latest_test.go).
	if (b.replaced(d.rec) && b.fault != replayedHistory) || (b.fault == lostTombstone && fakeLatest(d.rec.channel) && len(d.rec.payload) == 0) {
		b.seen.latestSkipped++
		return
	}
	subs := b.contributors(s, d.rec)
	if len(subs) == 0 {
		if b.fault == strayDelivery && !matchesAny(s, d.rec.topic) {
			b.handOff(c, s, d) // QoS 0, RETAIN 0, no identifier
		}
		return
	}
	if len(subs) > 1 {
		b.seen.overlapping++
	}
	if d.away && fakeLatest(d.rec.channel) {
		b.seen.latestReturned++
	}
	v5 := c.version == harness.V5
	for _, sub := range subs {
		d.multi = d.multi || len(sub.reach) > 1
		d.qos = max(d.qos, min(d.rec.qos, sub.qos))
		d.retain = d.retain || (v5 && sub.keep && d.rec.retain)
		d.filters = append(d.filters, sub.filter)
		if sub.id != 0 {
			d.ids = append(d.ids, sub.id)
		}
	}
	switch b.fault {
	case keepsRetain:
		d.retain = v5 && d.rec.retain
	case wrongIdentifiers:
		d.ids = nil
		for _, f := range slices.Sorted(maps.Keys(s.subs)) {
			if sub := s.subs[f]; sub.id != 0 && !slices.Contains(subs, sub) {
				d.ids = append(d.ids, sub.id)
			}
		}
	case lostDelivery:
		if d.rec.qos == 1 {
			b.lost++
			if b.lost%4 == 0 {
				return
			}
		}
	}
	if b.handOff(c, s, d) && d.qos == 1 && (b.fault == secondCopy || (b.fault == replayedTwice && d.again)) {
		b.handOff(c, s, &delivery{rec: d.rec, qos: d.qos, retain: d.retain, ids: d.ids, filters: d.filters})
	}
}

// contributors is s's subscriptions eligible for r, in complete filter-byte
// order: matching its topic, accepted before it was stored, and not No
// Local where s published it (design.md, Flows, SUBSCRIBE, Options: "No
// Local is honoured everywhere, against the publisher's client id stored on
// the record"), which ignoresNoLocal breaks.
func (b *fake) contributors(s *fakeSession, r *fakeRecord) []*fakeSub {
	var subs []*fakeSub
	for _, f := range slices.Sorted(maps.Keys(s.subs)) {
		sub := s.subs[f]
		// On an append channel every filter reads from the position; on
		// $broadcast and a latest channel each from its live-start boundary.
		if (sub.start < r.offset || (r.channel != 1 && !fakeLatest(r.channel))) && topicMatches(f, r.topic) && sub.part.allows(r.topic) &&
			(!sub.noLocal || r.publisher != s.id || b.fault == ignoresNoLocal) {
			subs = append(subs, sub)
		}
	}
	if len(subs) > 0 && !b.readable(s, r) {
		return nil
	}
	return subs
}

// readable reports whether s's user may read r's topic, counting the
// record skipped where not: design.md, Access and reload, "A record whose
// topic the user may not read is then skipped for that subscription. The
// record is not owed to it, the position moves past it". With unreadable, every record
// is. It runs under the lock.
func (b *fake) readable(s *fakeSession, r *fakeRecord) bool {
	if b.fault == unreadable || b.fakeMay(s.user, r.topic, false) {
		return true
	}
	b.seen.unreadable++
	return false
}

// swap, for reordered, keeps s's first live record it can deliver back and
// sends it after the next from the same publisher; it reports whether it
// took d. It runs under the lock.
func (b *fake) swap(c *fakeConn, s *fakeSession, d *delivery) bool {
	switch h := s.held; {
	case len(b.contributors(s, d.rec)) == 0:
		return false
	case h == nil:
		s.held = d
		return true
	case h.rec.publisher == d.rec.publisher:
		s.held = nil
		b.deliverLive(c, s, d)
		b.deliverLive(c, s, h)
		return true
	}
	return false
}

// handOff sends d on c for the first time, a QoS 1 or 2 one under the
// lowest free packet identifier (design.md, Delivery, Sending: "Packet
// identifiers use the lowest free identifier") and kept until its PUBACK
// or PUBCOMP, or a refusing PUBREC (receipt); it reports whether d went. It
// runs under the lock.
func (b *fake) handOff(c *fakeConn, s *fakeSession, d *delivery) bool {
	if d.qos > 0 {
		d.id = 1
		for slices.ContainsFunc(s.inflight, func(e *delivery) bool { return e.id == d.id }) {
			d.id++
		}
	}
	if !b.transmit(c, d) {
		return false
	}
	if d.qos > 0 {
		d.sent = true
		s.inflight = append(s.inflight, d)
	}
	return true
}

// transmit queues d for c's writer, unless it is over the client's Maximum
// Packet Size or cannot be encoded: design.md, Delivery, "A message over
// the client's Maximum Packet Size is discarded for that subscriber as if
// sent, counted as missed and logged (MQTT-3.1.2-25)"; "A delivery whose
// encoded packet cannot fit within that maximum is discarded the same way,
// counting Subscription Identifiers and Topic Alias". It reports whether d
// went. It runs under the lock.
func (b *fake) transmit(c *fakeConn, d *delivery) bool {
	wire, alias, err := c.encode(d)
	if err != nil || (c.maxPacket > 0 && len(wire) > c.maxPacket) {
		b.seen.tooLarge++
		return false
	}
	if alias != 0 {
		if c.sentAliases == nil {
			c.sentAliases = map[string]uint16{}
		}
		c.sentAliases[d.rec.topic] = alias
	}
	if d.qos == 0 {
		c.unwritten++
	}
	c.queue(wire, d.qos == 0)
	return true
}

// encode is d as a PUBLISH on c, and the Topic Alias it establishes, 0 for
// none. In MQTT 5 it carries the record's properties, its Subscription
// Identifiers and a Topic Alias: design.md, Delivery, Sending: "Outgoing
// aliases use the peer's advertised limit. The lowest unused alias is
// assigned on first use; a full alias table leaves new topics unaliased".
func (c *fakeConn) encode(d *delivery) (wire []byte, alias uint16, err error) {
	p := &harness.Publish{Dup: d.dup, QoS: d.qos, Retain: d.retain, Topic: d.rec.topic, ID: d.id, Payload: d.rec.payload}
	if c.version == harness.V5 {
		p.Props = d.rec.forwarded()
		if d.props != nil {
			p.Props = slices.Clone(d.props) // a worker offer's, worked out once
		} else if d.rec.channel != 1 {
			// design.md, Sagüin properties: saguin-id once, the
			// publisher's first where it set one.
			p.Props = append(slices.DeleteFunc(p.Props, func(q harness.Property) bool {
				return q.ID == harness.UserProperty && q.Str == "saguin-id"
			}), d.saguin()...)
		}
		for _, id := range d.ids {
			if d.props == nil {
				p.Props = append(p.Props, harness.Property{ID: harness.SubscriptionIdentifier, Int: id})
			}
		}
		switch a, ok := c.sentAliases[d.rec.topic]; {
		case ok:
			p.ByAlias = true
			p.Props = append(p.Props, harness.Property{ID: harness.TopicAlias, Int: uint32(a)})
		case len(c.sentAliases) < int(c.aliasMax):
			alias = uint16(len(c.sentAliases) + 1)
			p.Props = append(p.Props, harness.Property{ID: harness.TopicAlias, Int: uint32(alias)})
		}
	}
	wire, err = harness.Encode(p, c.version)
	return wire, alias, err
}

// forwarded is r's properties as a delivery carries them: its Message
// Expiry "worked out when the message is written and sent as the time
// remaining" (design.md, Delivery, Message Expiry), the rest unaltered
// (Sagüin properties: "Nothing a publisher sends is altered").
func (r *fakeRecord) forwarded() harness.Properties {
	ps := slices.Clone(r.props)
	for i, q := range ps {
		if q.ID == harness.MessageExpiryInterval {
			ps[i].Int -= min(q.Int, uint32(time.Since(r.received)/time.Second))
		}
	}
	return ps
}

// expired reports whether r's Message Expiry has run out.
func (r *fakeRecord) expired() bool {
	q, ok := r.props.Get(harness.MessageExpiryInterval)
	return ok && time.Since(r.received) >= time.Duration(q.Int)*time.Second
}

// TestStrayDeliveryIsCaught: rule 1 of 0029a.md, "nothing is delivered
// without a matching subscription", checked "on every delivery, rule 1 (it
// matches a subscription the session held when the message was published:
// design.md, Delivery; UNSUBSCRIBE "In-flight messages are finished")".
// design.md, Flows, SUBSCRIBE, One copy: "Only subscriptions eligible for
// the record contribute: topic, live-start boundary, No Local, partition
// and permission checks apply"; MQTT 5 3.3.2.1: "The Topic Name in a
// PUBLISH packet sent by a Server to a subscribing Client MUST match the
// Subscription’s Topic Filter according to the matching process defined in
// section 4.7 [MQTT-3.3.2-3]". A broker that also sends each message to
// every client holding subscriptions none of which matches its topic fails
// the run, naming the delivery as one "without a matching subscription at
// publication": a subscriber to x/# waits while t/a is published.
func TestStrayDeliveryIsCaught(t *testing.T) {
	caughtInScene(t, strayDelivery, sceneQueue, "without a matching subscription at publication", published(0, "ann", "x/#", "t/a", false))
}

// TestSecondCopyIsCaught: rule 2 of 0029a.md, "no QoS 1 duplicate while
// connected, unless it is marked as a resend", as planned: "rule 2 (no QoS
// 1 second copy while connected unless DUP; One copy: with $share out,
// always one delivery)". design.md, Delivery: "While connected, the broker
// starts no resend. Its replies to repeated packets follow their QoS flow.";
// Flows, SUBSCRIBE, One copy: "a record matching several of a
// session's non-shared subscriptions is sent once". MQTT 5 3.3.1.1: "The
// DUP flag MUST be set to 1 by the Client or Server when it attempts to
// re-deliver a PUBLISH packet [MQTT-3.3.1-1]". A broker that sends each
// QoS 1 delivery twice on one connection, under two packet identifiers and
// neither marked DUP, fails the run, naming the second copy "without DUP":
// a subscriber to t/# waits while t/a is published at QoS 1.
func TestSecondCopyIsCaught(t *testing.T) {
	caughtInScene(t, secondCopy, sceneQueue, "while connected without DUP", published(0, "ann", "t/#", "t/a", false))
}

// TestPublisherOrderIsCaught: rule 3 of 0029a.md, "each publisher's
// messages arrive in order", checked "on every delivery". design.md,
// Delivery, Order: "a channel's offset order on $broadcast"; MQTT 5 4.6: "it
// MUST send PUBLISH packets to consumers (for the same Topic and QoS) in the
// order that they were received from any given Client [MQTT-4.6.0-5]", every
// Topic "an Ordered Topic" by default [MQTT-4.6.0-6]. A broker that keeps a
// subscriber's delivery back until the next from the same publisher, and
// sends that one first, fails the run, naming the "publisher's order": a
// subscriber to t/# waits while one client publishes t/a, then t/b.
func TestPublisherOrderIsCaught(t *testing.T) {
	caughtInScene(t, reordered, sceneQueue, "publisher's order", orderScene)
}

// TestOwedMessageIsCaught: rule 4 of 0029a.md, "every acknowledged QoS 1
// message reaches every subscriber that stayed connected and subscribed
// throughout", "checked at the fuzzer's checkpoints (every 200 steps and
// at the end), after every connected subscriber releases its withheld
// acknowledgements, waiting under the watchdog"; "A message is owed to a
// subscriber only if its publisher's PUBACK came after that subscriber's
// SUBACK in the fuzzer's step order, and the subscriber stayed connected
// and subscribed from then to the checkpoint". design.md, Channels,
// $broadcast: "Every message goes in, whatever its QoS"; MQTT 5 4.5: "When
// a Server takes ownership of an incoming Application Message it MUST add
// it to the Session State for those Clients that have matching
// Subscriptions [MQTT-4.5.0-1]". A broker that never sends every fourth
// delivery of a QoS 1 message fails the run at a checkpoint, naming the
// message "owed": a subscriber to t/# waits while four messages are
// published at QoS 1, then a checkpoint owes it each.
func TestOwedMessageIsCaught(t *testing.T) {
	caughtInScene(t, lostDelivery, sceneQueue, "still owed at checkpoint", owedScene)
}

// TestWrongSubackIsCaught: 0029a.md, "the SUBACK code predicted from the
// design's table (SUBSCRIBE, Refusals) as the fuzzer does for CONNECT and
// PUBLISH", checked "on every SUBACK". design.md, Flows, SUBSCRIBE: the
// subscription table's $broadcast row, "Granted for QoS 0 / 1 / 2": "0 / 1
// / 2"; MQTT 5 3.8.4: "This Reason Code MUST either show the maximum QoS
// that was granted for that Subscription or indicate that the subscription
// failed [MQTT-3.8.4-7]". A broker granting a subscription asking QoS 2
// only QoS 1 fails the run, naming the "SUBACK" and its code "0x01". It
// stays a run of the random walk, the one fault test that also checks the
// command a failure prints replays the seed's steps through the failure
// (caught; design.md, Testing: "Each run's seed is recorded so a failure
// can be rerun with the same steps"): seed 1's first SUBACK asking QoS 2
// shows it, a draw no timing decides, at step 4.
func TestWrongSubackIsCaught(t *testing.T) {
	caught(t, cappedGrant, "SUBACK", "0x01")
}

// TestNoLocalIsCaught: 0029a.md, "No Local, a client never receives its
// own publication through a No Local subscription", checked "on every
// delivery". design.md, Flows, SUBSCRIBE, Options: "No Local is honoured
// everywhere, against the publisher's client id stored on the record and
// never sent"; MQTT 5 3.8.3.1: "If the value is 1, Application Messages
// MUST NOT be forwarded to a connection with a ClientID equal to the
// ClientID of the publishing connection [MQTT-3.8.3-3]". A broker that
// delivers a client's own publications through its No Local subscriptions
// fails the run, naming "No Local": a client subscribes to t/# with No
// Local and publishes t/a.
func TestNoLocalIsCaught(t *testing.T) {
	caughtInScene(t, ignoresNoLocal, sceneQueue, "No Local", noLocalScene)
}

// TestRetainAsPublishedIsCaught: 0029a.md, "Retain As Published, a live
// delivery carries RETAIN 0 unless the subscription keeps it", checked "on
// every delivery". design.md, Channels, $broadcast: "RETAIN is 1 on retained
// values sent to a new subscription. Otherwise it is 0 unless Retain As
// Published"; Flows, SUBSCRIBE, One copy: "A live $broadcast delivery
// preserves RETAIN when any contributor requests Retain As Published"; MQTT 5
// 3.3.1.3: "If the value of Retain As Published subscription option is set to
// 0, the Server MUST set the RETAIN flag to 0 when forwarding an Application
// Message regardless of how the RETAIN flag was set in the received PUBLISH
// packet [MQTT-3.3.1-12]". A broker that sends an MQTT 5 subscriber's live
// deliveries with RETAIN as published, whatever the option, fails the run,
// naming "Retain As Published".
func TestRetainAsPublishedIsCaught(t *testing.T) {
	caughtInScene(t, keepsRetain, sceneQueue, "Retain As Published", retainScene)
}

// TestSubscriptionIdentifiersAreCaught: 0029a.md, "Subscription
// Identifiers, a delivery carries the identifiers of the subscriptions it
// matched", checked "on every delivery". design.md, Flows, SUBSCRIBE, One
// copy: "Subscription Identifiers appear in complete filter-byte order,
// once for each contributing subscription having one"; MQTT 5 3.3.4: "If
// the Server sends a single copy of the message it MUST include in the
// PUBLISH packet the Subscription Identifiers for all matching
// subscriptions which have a Subscription Identifiers, their order is not
// significant [MQTT-3.3.4-4]". A broker that sends with each live delivery
// the identifiers of the subscriptions it did not match, and none of those
// it did, fails the run, naming the "Subscription Identifier": a client
// subscribes to t/# with identifier 7 and to x/# with 9, and t/a is
// published.
func TestSubscriptionIdentifiersAreCaught(t *testing.T) {
	caughtInScene(t, wrongIdentifiers, sceneQueue, "Subscription Identifier", identifiersScene)
}

// TestSharedToAllIsCaught: design.md, Flows, SUBSCRIBE, A shared group:
// "Each message goes to one member with room"; "An unfinished QoS 1 message
// goes to another member when its member's session ends". MQTT 5 4.8.2:
// "Each subsequent Application Message that matches the Shared
// Subscription is now sent to one and only one of the Sessions that are
// subscribed to the Shared Subscription". A broker that sends each message
// through a $share filter to every member of its group fails the run,
// naming the message "sent to a second member": two clients join the group
// g of t/#, and t/s is published.
func TestSharedToAllIsCaught(t *testing.T) {
	caughtInScene(t, sharedToAll, sceneQueue, "sent to a second member", sharedScene)
}

// TestAnAwayAssignedMemberKeepsItsGroupsBacklog: design.md, Flows,
// SUBSCRIBE, A shared group: "its cursors are stored if any member is
// durable"; "Joining an existing group preserves its cursors and returned
// deliveries"; "When the last member unsubscribes or its session ends, the
// shared subscription's cursors and pending deliveries are removed"; Words:
// a session "is away when it outlives its connection, until its expiry".
// An MQTT 5 client connecting with an empty client id and a Session Expiry
// joins a group, and its connection ends: no slot holds its session any
// more, as none reconnects to an assigned identifier, yet the session stays
// a member, so a publication the group took after its first SUBACK stays
// in the registry for the member that joins next (seed
// 17313423452207202444, step 1824: delivered then, it was failed as "no
// publication of the run's"). Where that session ends with its connection,
// the group goes with its last member, and the publication is released.
func TestAnAwayAssignedMemberKeepsItsGroupsBacklog(t *testing.T) {
	for _, c := range []struct {
		name    string
		durable bool
	}{
		{"a durable session, away", true},
		{"a session ending with its connection", false},
	} {
		f := newFuzzer("", "", 1, 1, time.Second, io.Discard)
		s := f.slots[0]
		s.subs, s.anon, s.durable = newSubscriptions(), true, c.durable
		f.serial = 10 // the group's first SUBACK
		s.holdShare(harness.Subscription{Filter: "$share/g/#", QoS: 1}, firstShareID)
		s.end(false)
		before := &message{serial: 5, tag: "5.0", topic: "o/a", channel: broadcast}
		owed := &message{serial: 15, tag: "15.0", topic: "o/b", channel: broadcast}
		f.publications[before.tag], f.publications[owed.tag] = before, owed
		f.serial = 30
		f.prunePublications()
		if f.publications[before.tag] != nil {
			t.Errorf("%s: a publication before the group's first SUBACK is kept; want it released (A new group starts at the tail)", c.name)
		}
		if kept := f.publications[owed.tag] != nil; kept != c.durable {
			t.Errorf("%s: the publication the group took after its first SUBACK kept %v, want %v", c.name, kept, c.durable)
		}
	}
}

// TestPubrelResendIsCaught: design.md, Delivery, Positions: "A QoS 2
// delivery stores its PUBREC before its PUBREL goes to the writer"; Flows,
// CONNECT, 9: "The broker reads in-flight messages and PUBRELs from the
// store and resends them"; MQTT 5 4.3.3: the sender "MUST NOT re-send the
// PUBLISH once it has sent the corresponding PUBREL packet [MQTT-4.3.3-6]".
// A broker that resends a delivery past its PUBREC as a PUBLISH after a
// reconnect, not as its PUBREL, fails the run, naming the PUBLISH under the
// identifier of a delivery "past its PUBREL": a subscriber holding a
// durable session answers a QoS 2 delivery with PUBREC 0x00, withholds the
// PUBCOMP of its PUBREL, drops its connection and comes back keeping its
// session.
func TestPubrelResendIsCaught(t *testing.T) {
	caughtInScene(t, resentPublish, sceneQueue, "past its PUBREL", pubrelResendScene)
}

// TestResendKeepsItsFirstRetain: audit finding 261026-004a. design.md,
// Delivery, A resend: "The first-delivery RETAIN value is preserved, but an
// MQTT 3.1.1 connection receives RETAIN 1 only on a delivery of a
// subscription's initial pass"; MQTT 5 3.3.1.1: "The DUP flag MUST be set to
// 1 by the Client or Server when it attempts to re-deliver a PUBLISH packet
// [MQTT-3.3.1-1]". A durable session subscribes over MQTT 5 with Retain As
// Published; an MQTT 3.1.1 connection takes it over and receives a retained
// publication live, RETAIN 0 (MQTT-3.3.1-9), leaving it unacknowledged; an
// MQTT 5 connection takes it over and receives the resend, DUP 1. A broker
// keeping the first copy's RETAIN 0 passes the run's checks; one giving the
// resend the RETAIN 1 a live delivery there would carry fails them, naming
// the resend's RETAIN and its first copy's.
func TestResendKeepsItsFirstRetain(t *testing.T) {
	for _, c := range []struct {
		fault  fault
		retain bool   // the resend's RETAIN
		want   string // what the failure names; "" for none
	}{
		{"", false, ""},
		{recomputesRetain, true, "first copy"},
	} {
		b := fakeBroker(t, c.fault)
		f := newFuzzer(b.addr, b.dbPath, 1, 1, 2*time.Second, io.Discard)
		t.Cleanup(f.closeAll)
		s := f.slots[0]
		connect := func(version byte, clean bool) error {
			p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: version, ClientID: s.id, CleanStart: clean, KeepAlive: keepAliveFloor}, logins[0].user, logins[0].password)
			if version == harness.V5 {
				p.Props = harness.Properties{{ID: harness.SessionExpiryInterval, Int: expiryFloor}}
			}
			a, err := seal(p, s.session)
			if err != nil {
				return err
			}
			return f.try(s, a, s.conn)
		}
		if err := connect(harness.V5, true); err != nil {
			t.Fatalf("%q: the MQTT 5 CONNECT: %v", c.fault, err)
		}
		sub := harness.Subscription{Filter: "t", QoS: 1, RetainAsPublished: true, RetainHandling: 2}
		s.hold(sub, 0, nil)
		f.revise(s)
		if err := s.conn.Send(&harness.Subscribe{ID: s.conn.NextID(), Subs: []harness.Subscription{sub}}); err != nil {
			t.Fatal(err)
		}
		if ack, err := next[*harness.Suback](s); err != nil || !slices.Equal(ack.Reasons, []byte{1}) {
			t.Fatalf("%q: SUBACK %v, %v; want QoS 1 granted", c.fault, ack, err)
		}
		if err := connect(harness.V311, false); err != nil {
			t.Fatalf("%q: the MQTT 3.1.1 takeover: %v", c.fault, err)
		}
		f.step++
		o := &out{p: &harness.Publish{QoS: 1, Retain: true, Topic: sub.Filter, ID: s.conn.NextID()}}
		o.m = &message{step: f.step, publisher: s.clientID, topic: o.p.Topic, qos: 1, retain: true}
		if err := f.fill(o, 0, 0, 0, letters, s.conn.Version); err != nil {
			t.Fatal(err)
		}
		f.expect(o)
		if err := send(s, o); err != nil {
			t.Fatal(err)
		}
		// The PINGRESP follows the delivery the PUBLISH made.
		if err := cmp.Or(collect(s), pingPast(s)); err != nil || !s.incoming.seen[o.m.tag] {
			t.Fatalf("%q: the live delivery over MQTT 3.1.1: seen %v, %v", c.fault, s.incoming.seen[o.m.tag], err)
		}
		err := connect(harness.V5, false)
		b.mu.Lock()
		resent := slices.ContainsFunc(b.sessions[s.id].inflight, func(d *delivery) bool { return d.dup && d.retain == c.retain })
		b.mu.Unlock()
		if !resent {
			t.Fatalf("%q: the fake sent no resend with DUP 1, RETAIN %v", c.fault, c.retain)
		}
		switch {
		case c.want == "" && (err != nil || !s.incoming.seen[o.m.tag]):
			t.Errorf("a resend over MQTT 5 keeping its first copy's RETAIN 0: seen %v, %v; want it accepted", s.incoming.seen[o.m.tag], err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "RETAIN true")):
			t.Errorf("%q: a resend over MQTT 5 changing RETAIN 0 to 1: %v; want it refused, naming %q and \"RETAIN true\"", c.fault, err, c.want)
		default:
			t.Logf("%q: %v", c.fault, err)
		}
	}
}

// TestDrainEndsOnQuietOrOnTooMany: audit finding 261026-005a. design.md,
// Delivery, Preparation: "A retained or initial pass has no allowance of
// its own: its selected, loading and sent deliveries share the one
// window"; Flows, SUBSCRIBE, The start point: "An initial pass reads in
// pages; each page sends the values current when it is read, below the
// tail at the cut"; Delivery: "While connected, the broker starts no resend.
// Its replies to repeated packets follow their QoS flow." An
// MQTT 3.1.1 subscription to # (no Retain Handling: every value is sent)
// takes as long as the run's retained values take to cross the window, so
// a drain ends on quiet, never on a clock: a queued pass is read whole even
// when the receiving driver pauses past -timeout. A prefix of an endless
// pass, repeating its values beyond what the broker can owe the connection,
// fails the run, named.
func TestDrainEndsOnQuietOrOnTooMany(t *testing.T) {
	const values = 2
	const timeout = 10 * time.Millisecond
	for _, c := range []struct {
		name string
		want string // what the failure names; "" for none
	}{
		{"long pass", ""},
		{"endless pass", "never stops sending"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := fakeBroker(t, "")
			f := newFuzzer(b.addr, "", 1, 2, harness.DefaultTimeout, io.Discard)
			t.Cleanup(f.closeAll)
			connect := func(s *slot, version byte) {
				t.Helper()
				p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: version, ClientID: s.id, CleanStart: true, KeepAlive: keepAliveFloor}, logins[0].user, logins[0].password)
				a, err := seal(p, s.session)
				if err == nil {
					err = f.try(s, a, s.conn)
				}
				if err != nil {
					t.Fatalf("the CONNECT of %s: %v", s.id, err)
				}
			}
			publisher, subscriber := f.slots[0], f.slots[1]
			connect(publisher, harness.V5)
			connect(subscriber, harness.V311)
			// The run's retained values, one per topic, each a publication the
			// run sent (expect) and the broker took before the PINGRESP.
			f.step++
			batch := make([]*out, values)
			for i := range batch {
				o := &out{p: &harness.Publish{Retain: true, Topic: "t/" + strconv.Itoa(i)}}
				o.m = &message{step: f.step, publisher: publisher.clientID, topic: o.p.Topic, retain: true}
				if err := f.fill(o, i, 0, 0, letters, harness.V5); err != nil {
					t.Fatal(err)
				}
				batch[i] = o
			}
			f.expect(batch...)
			if err := cmp.Or(send(publisher, batch...), pingPast(publisher)); err != nil {
				t.Fatalf("publishing the retained values: %v", err)
			}
			sub := harness.Subscription{Filter: "#"}
			subscriber.hold(sub, 0, nil)
			f.revise(subscriber)
			if err := subscriber.conn.Send(&harness.Subscribe{ID: subscriber.conn.NextID(), Subs: []harness.Subscription{sub}}); err != nil {
				t.Fatal(err)
			}
			if ack, err := next[*harness.Suback](subscriber); err != nil || !slices.Equal(ack.Reasons, []byte{0}) {
				t.Fatalf("SUBACK %v, %v; want QoS 0 granted", ack, err)
			}
			// Queue the complete pass before draining. The fake's writer need
			// not be scheduled again, even under load. Its socket stays open
			// and quiet after the queued bytes, rather than ending with EOF.
			var pass []byte
			for _, o := range batch {
				wire, err := harness.Encode(o.p, harness.V311)
				if err != nil {
					t.Fatal(err)
				}
				pass = append(pass, wire...)
			}
			conn := subscriber.conn.Conn.(*observedConn)
			queued := make([]byte, len(pass))
			if _, err := io.ReadFull(conn, queued); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(queued, pass) {
				t.Fatal("the retained pass differs from the published values")
			}
			pause := timeout
			if c.want != "" {
				// A finite prefix of an endless pass is enough: the bound
				// must refuse it before it can reach quiet.
				pass = bytes.Repeat(pass, subscriber.mostDeliveries()/values+1)
				pause = 0
			}
			conn.reader = bufio.NewReader(io.MultiReader(&pausedPass{Reader: bytes.NewReader(pass), pause: pause}, socketInput{conn}))
			subscriber.conn.Timeout = timeout
			start := time.Now()
			err := subscriber.drain()
			took := time.Since(start)
			switch {
			case c.want == "" && err != nil:
				t.Errorf("a pass of %d retained values, drained for %v with -timeout %v: %v; want it drained", values, took, timeout, err)
			case c.want == "" && (subscriber.incoming.delivered != values || took <= timeout):
				t.Errorf("drained %d of %d values in %v; want the whole pass after -timeout %v", subscriber.incoming.delivered, values, took, timeout)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Errorf("a pass sending its values again and again: %v after %v; want it refused, naming %q", err, took, c.want)
			default:
				t.Logf("a pass of %d retained values, %v with -timeout %v: %v", values, took, timeout, err)
			}
		})
	}
}

// pausedPass pauses the receiving driver once while the pass's bytes are
// already queued. The pause makes the drain outlast -timeout regardless of
// machine speed, without requiring a sender to meet the quiet interval.
type pausedPass struct {
	io.Reader
	pause time.Duration
}

func (p *pausedPass) Read(b []byte) (int, error) {
	time.Sleep(p.pause)
	p.pause = 0
	return p.Reader.Read(b)
}
