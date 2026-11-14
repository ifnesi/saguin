package main

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"

	"github.com/ifnesi/saguin/internal/harness"
)

// Shared subscriptions (design.md, Flows, SUBSCRIBE: the $share row, A
// shared group, One copy, Options, Refusals; MQTT 5 4.8.2). After a
// SUBSCRIBE, a client sometimes sends one of a $share filter alone, or
// unsubscribes one it holds; once granted one, another connected MQTT 5
// client with room for it often joins the same group at once, so that
// groups regularly have two or more connected members and "Each message
// goes to one member with room" and the round-robin selection are
// exercised in every run. These draws come from a generator of their
// own (fuzzer.shareRng), so the run's other draws stay as they were. Every
// answer is predicted: MQTT 3.1.1 gets 0x80 for each $share filter ("MQTT
// 3.1.1: refused (0x80)"); in MQTT 5, a user denied share gets DISCONNECT
// 0x9E ("0x9E for [...] shared subscriptions its CONNACK withheld", MQTT
// 5 3.2.2.3.13), No Local DISCONNECT 0x82 ("On $share, it is a protocol
// error that closes the connection with 0x82", MQTT-3.8.3-4), and each
// filter otherwise its code in refusal order: 0x87 where the user may read
// nothing its filter part matches, 0x8F where the whole filter, "including
// $share/<group>/", is over a topic limit, 0x97 past max_subscriptions,
// and 0x83 for a saguin-filter partition ("refused with 0x83 on [...]
// $share") or an unknown saguin- property; a granted one at the QoS asked.
//
// Each $share SUBSCRIBE, of one filter, carries a Subscription Identifier
// of its own, from a range ordinary SUBSCRIBEs reach almost never, so a
// delivery carrying one alone came through that shared subscription ("Each $share
// subscription is handled on its own", One copy), with its options then.
// Such a delivery is checked: its topic matches the filter part ("The
// $share and {ShareName} portions of the Topic Filter are not taken into
// account when matching", 4.8.2), its member's user may read it, it was
// published after the group's first member subscribed ("A new group
// starts at the tail"), its QoS the lower of the publication's and the
// member's grant [MQTT-4.8.2-3], RETAIN as for any live delivery, and the
// Sagüin properties as for any channel's. And each message goes to one
// member: a second member receives it only where it is QoS 1, the member
// before never acknowledged it as far as the run has confirmed, and that
// member's session has ended ("Each message goes to one member"; "An
// unfinished QoS 1 message goes to another member when its member's
// session ends"; "An unfinished QoS 2 message waits [...] When the session
// ends, that message is dropped", MQTT-4.8.2-5; "QoS 0 settles on
// successful writing"); its own member receives it again only as a resend
// (DUP 1), never while connected. Which member, and when, is the broker's:
// no shared delivery is owed at a checkpoint.

// shareIDs is how many Subscription Identifiers the run keeps for its
// $share SUBSCRIBEs, the top of MQTT's range, from firstShareID; an
// ordinary SUBSCRIBE draws from the whole range, so the session's own
// identifiers are skipped (holdShare).
const (
	shareIDs     = 1 << 16
	firstShareID = harness.MaxVarInt - shareIDs + 1
)

// shareGroups are the ShareNames the run draws, g three times as often as
// h: few, so groups gain members.
var shareGroups = []string{"g", "g", "g", "h"}

// shareSub is one shared subscription a session was granted, with the
// options of the SUBSCRIBE that granted it.
type shareSub struct {
	filter, part string // the whole filter, and its filter part
	qos          byte   // as granted
	keep         bool   // Retain As Published
	reader       string // the user whose rights it delivers under
	reach        []string
}

// sharePart returns the filter part of filter, and whether filter is a
// $share one (MQTT 5 4.8.2: "$share/{ShareName}/{filter}").
func sharePart(filter string) (part string, ok bool) {
	rest, ok := strings.CutPrefix(filter, "$share/")
	if !ok {
		return "", false
	}
	_, part, ok = strings.Cut(rest, "/")
	return part, ok
}

// shareLog is what the run has seen of the groups: read and written by
// the connections' readers, bursts' goroutines among them, under mu.
type shareLog struct {
	mu     sync.Mutex
	groups map[string]*shareGroup // by whole filter
	next   uint32                 // the Subscription Identifiers given so far
	// away are the durable sessions of empty client ids whose connections
	// have ended, holding $share filters: no slot holds them any more, as
	// none reconnects to an assigned identifier, yet each stays a member of
	// its groups until its expiry, beyond any run (expiryFloor), and so
	// keeps them and their cursors ("its cursors are stored if any member
	// is durable"; "When the last member unsubscribes or its session ends,
	// the shared subscription's cursors and pending deliveries are
	// removed", design.md, Flows, SUBSCRIBE, A shared group).
	away []*subscriptions
	// granted counts the $share filters granted, delivered the shared
	// deliveries, and returned those to a second member.
	granted, delivered, returned int
}

// shareGroup is one group as the run sees it: the serial at its first
// member's SUBACK, and each message delivered through it, by tag.
type shareGroup struct {
	first   uint64
	holders map[string]*holding
}

// holding is the member a shared message went to last.
type holding struct {
	subs    *subscriptions // the member's session
	conn    *harness.Client
	qos     byte
	settled bool // its PUBACK sent, and taken (confirmShares)
}

// shareKey names one shared delivery: its group and its message.
type shareKey struct{ filter, tag string }

// shared reports whether the session held a $share filter at any time.
func (subs *subscriptions) shared() bool {
	return subs != nil && len(subs.shareIDs) > 0
}

// ended records that subs's session has ended, so a member after it may
// receive what it left unfinished.
func (l *shareLog) ended(subs *subscriptions) {
	if subs == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	subs.ended = true
}

// wentAway records that subs, the durable session of an empty client id,
// has lost its connection: where it holds $share filters, its groups keep
// what they owe while it is away (shareLog.away), for whichever member
// joins them next ("Joining an existing group preserves its cursors and
// returned deliveries").
func (l *shareLog) wentAway(subs *subscriptions) {
	if subs == nil || len(subs.shares) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.away = append(l.away, subs)
}

// shareStep, after a SUBSCRIBE, has s send, one time in two, a $share
// SUBSCRIBE or, where it holds a $share filter, one time in three of
// those an UNSUBSCRIBE of one. Where s's $share filter is granted, two
// times in three another connected client joins its group (sharePartner).
func (f *fuzzer) shareStep(s *slot) error {
	r := f.shareRng
	if r.IntN(2) != 0 || s.conn == nil {
		return nil
	}
	if len(s.subs.shares) > 0 && r.IntN(3) == 0 {
		return f.unsubscribeShare(s)
	}
	filter, err := f.subscribeShares(s, "")
	if err != nil || filter == "" || r.IntN(3) == 0 {
		return err
	}
	if partner := f.sharePartner(s, filter); partner != nil {
		_, err = f.subscribeShares(partner, filter)
	}
	return err
}

// sharePartner draws, from the clients but s whose MQTT 5 connection's
// SUBSCRIBE of filter would be granted (shareCode) and that do not hold it
// yet, the one to join filter's group, nil where there is none: the
// group then has two connected members or more.
func (f *fuzzer) sharePartner(s *slot, filter string) *slot {
	var can []*slot
	for _, o := range f.slots {
		if o != s && o.conn != nil && o.conn.Version == harness.V5 && o.subs != nil && o.subs.shares[filter] == nil &&
			!denied(o.user, "share") && shareCode(o, filter, false) == 0 {
			can = append(can, o)
		}
	}
	if len(can) == 0 {
		return nil
	}
	return can[f.shareRng.IntN(len(can))]
}

// shareWord is a run of lower-case letters, lo to hi long, from r.
func shareWord(r *rand.Rand, lo, hi int) string {
	b := make([]byte, lo+r.IntN(hi-lo+1))
	for i := range b {
		b[i] = byte('a' + r.IntN(26))
	}
	return string(b)
}

// shareFilter draws a $share filter: mostly within the topic limits, its
// filter part on the channels' levels or $broadcast's; now and then one
// whose part alone is within them but the whole is not.
func shareFilter(r *rand.Rand) string {
	prefix := "$share/" + shareGroups[r.IntN(len(shareGroups))] + "/"
	var part string
	switch r.IntN(10) {
	case 0:
		// Within max_topic_length alone, over it with its prefix.
		part = shareWord(r, maxTopicLength-len(prefix)+1, maxTopicLength-len(prefix)+3)
	case 1:
		// Within max_topic_levels alone, over it with its prefix.
		part = strings.Repeat("+/", maxTopicLevels-2) + "#"
	case 2, 3, 4:
		part = "#"
	case 5, 6:
		part = []string{"ca/#", "ca/s", "cb/+", "ca/r", "cc/#", "ca/+", "kv/#"}[r.IntN(7)]
	default:
		part = shareWord(r, 1, 2)
		if r.IntN(2) == 0 {
			part = []string{"ca", "cb", "cc"}[r.IntN(3)] + "/" + part
		}
	}
	return prefix + part
}

// shareCode is the code design.md, Flows, SUBSCRIBE, Refusals gives
// filter, a $share filter of s's MQTT 5 SUBSCRIBE, in "refusal order 0x87,
// 0x8F, 0x97, 0x83", or 0: the acl_file refusing it "only when the user may
// read nothing the filter can match", its filter part; "A $share filter is
// measured whole, including $share/<group>/"; "0x97 for a filter past
// limits.max_subscriptions", a repeated filter taking none; and 0x83 for
// refused, a partition or an unknown saguin- property.
func shareCode(s *slot, filter string, refused bool) byte {
	part, _ := sharePart(filter)
	switch {
	case !readsAny(s.user, part):
		return 0x87
	case len(filter) > maxTopicLength || strings.Count(filter, "/") >= maxTopicLevels:
		return 0x8F
	case s.subs.shares[filter] == nil && len(s.subs.current)+len(s.subs.shares) >= maxSubscriptions:
		return 0x97
	case refused:
		return 0x83
	}
	return 0
}

// subscribeShares has s send a SUBSCRIBE of one $share filter and checks
// its answer (shares.go's opening); it returns the filter where it is
// granted, else "". The filter is drawn, or join where set, which then
// goes without the properties and No Local that refuse it.
func (f *fuzzer) subscribeShares(s *slot, join string) (string, error) {
	r := f.shareRng
	v5 := s.conn.Version == harness.V5
	p := &harness.Subscribe{ID: s.nextID()}
	var id uint32
	refused := false
	if v5 {
		id = f.shareID(s)
		p.Props = harness.Properties{{ID: harness.SubscriptionIdentifier, Int: id}}
		if join == "" && r.IntN(10) == 0 {
			p.Props, refused = append(p.Props, harness.User("saguin-filter", "topic_hash(2,0)")), true
		}
		if join == "" && r.IntN(10) == 0 {
			p.Props, refused = append(p.Props, harness.User("saguin-"+shareWord(r, 1, 3), "")), true
		}
	}
	// A user denied share is disconnected for it whatever its options, so
	// one reason is known.
	filter := join
	if filter == "" {
		filter = shareFilter(r)
		if joined := f.heldShares(s); len(joined) > 0 && r.IntN(3) != 0 {
			filter = joined[r.IntN(len(joined))] // joining a group of another client's
		}
	}
	sub := harness.Subscription{Filter: filter, QoS: byte(r.IntN(3))}
	if v5 {
		sub.NoLocal, sub.RetainAsPublished, sub.RetainHandling = join == "" && !denied(s.user, "share") && r.IntN(10) == 0, r.IntN(2) == 0, 2
	}
	p.Subs = []harness.Subscription{sub}
	wire, err := harness.Encode(p, s.conn.Version)
	switch {
	case err != nil:
		return "", err
	case len(wire) > maxPacketSize:
		return "", fmt.Errorf("generated a $share SUBSCRIBE of %d bytes, over max_packet_size %d", len(wire), maxPacketSize)
	}
	var reason byte
	switch {
	case v5 && denied(s.user, "share"):
		reason = 0x9E
	case sub.NoLocal:
		reason = 0x82
	}
	if reason != 0 {
		f.note("%s: SUBSCRIBE %v properties %v: want DISCONNECT 0x%02X, then the close", s.id, p.Subs, p.Props, reason)
		if err := s.conn.SendRaw(wire); err != nil {
			return "", err
		}
		err := closedWith(s, reason)
		_ = s.conn.Close() // checked closed by the broker, or the step failed
		s.fire(fmt.Sprintf("the broker closed the connection for its $share SUBSCRIBE, 0x%02X", reason))
		s.end(false)
		return "", err
	}
	want := []byte{0x80} // "MQTT 3.1.1: refused (0x80)"
	granted := ""
	if v5 {
		if want[0] = shareCode(s, sub.Filter, refused); want[0] == 0 {
			s.holdShare(sub, id)
			want[0], granted = sub.QoS, sub.Filter
		}
	}
	f.note("%s: SUBSCRIBE %v properties %v: want SUBACK % X", s.id, p.Subs, p.Props, want)
	if err := s.conn.SendRaw(wire); err != nil {
		return "", err
	}
	ack, err := next[*harness.Suback](s)
	if err != nil {
		return "", err
	}
	if ack.ID != p.ID || !slices.Equal(ack.Reasons, want) {
		return "", fmt.Errorf("SUBACK id %d codes % X to the $share SUBSCRIBE %v, want id %d codes % X (design.md, Flows, SUBSCRIBE: the $share row, Refusals)",
			ack.ID, ack.Reasons, p.Subs, p.ID, want)
	}
	return granted, s.drain()
}

// heldShares is the $share filters the other clients' connected sessions
// hold, in order.
func (f *fuzzer) heldShares(s *slot) []string {
	var held []string
	for _, o := range f.slots {
		if o != s && o.conn != nil && o.subs != nil {
			held = append(held, slices.Collect(maps.Keys(o.subs.shares))...)
		}
	}
	slices.Sort(held)
	return slices.Compact(held)
}

// shareID is the next Subscription Identifier of the run's range, none of
// s's session's own ordinary subscriptions holds.
func (f *fuzzer) shareID(s *slot) uint32 {
	for {
		f.shares.next++
		id := firstShareID + (f.shares.next-1)%shareIDs
		if !slices.ContainsFunc(s.subs.history, func(r revision) bool {
			return slices.ContainsFunc(r.subs, func(sub *subscription) bool { return sub.id == id })
		}) {
			return id
		}
	}
}

// holdShare records sub, granted to s's session under Subscription
// Identifier id: "A SUBSCRIBE repeating an existing filter replaces its
// options" (The start point), the group taking its first member's SUBACK
// where it has none yet.
func (s *slot) holdShare(sub harness.Subscription, id uint32) {
	part, _ := sharePart(sub.Filter)
	sh := &shareSub{filter: sub.Filter, part: part, qos: sub.QoS, keep: sub.RetainAsPublished, reader: s.user, reach: reach(part)}
	s.subs.shares[sub.Filter], s.subs.shareIDs[id] = sh, sh
	l := &s.f.shares
	l.mu.Lock()
	defer l.mu.Unlock()
	l.granted++
	if l.groups == nil {
		l.groups = map[string]*shareGroup{}
	}
	if l.groups[sub.Filter] == nil {
		l.groups[sub.Filter] = &shareGroup{first: s.f.serial, holders: map[string]*holding{}}
	}
}

// unsubscribeShare has s unsubscribe one of its $share filters: UNSUBACK
// 0x00 (MQTT 5 3.11.3), its in-flight messages finished ("In-flight
// messages are finished"), so it still identifies them.
func (f *fuzzer) unsubscribeShare(s *slot) error {
	filters := slices.Sorted(maps.Keys(s.subs.shares))
	filter := filters[f.shareRng.IntN(len(filters))]
	p := &harness.Unsubscribe{ID: s.nextID(), Filters: []string{filter}}
	f.note("%s: UNSUBSCRIBE %q", s.id, filter)
	if err := s.conn.Send(p); err != nil {
		return err
	}
	ack, err := next[*harness.Unsuback](s)
	if err != nil {
		return err
	}
	if ack.ID != p.ID || !slices.Equal(ack.Reasons, []byte{0}) {
		return fmt.Errorf("UNSUBACK id %d codes % X to the UNSUBSCRIBE of %q, want id %d success", ack.ID, ack.Reasons, filter, p.ID)
	}
	delete(s.subs.shares, filter)
	return s.drain()
}

// sharedThrough is the shared subscription p, a delivery to s, came
// through: the one whose Subscription Identifier p carries alone, nil for
// none.
func (s *slot) sharedThrough(p *harness.Publish) *shareSub {
	if s.subs == nil || len(s.subs.shareIDs) == 0 {
		return nil
	}
	var ids []uint32
	for _, q := range p.Props {
		if q.ID == harness.SubscriptionIdentifier {
			ids = append(ids, q.Int)
		}
	}
	if len(ids) != 1 {
		return nil
	}
	return s.subs.shareIDs[ids[0]]
}

// checkShared checks p, a delivery to s through sh (shares.go's opening).
func (s *slot) checkShared(p *harness.Publish, sh *shareSub) error {
	tag, _ := s.deliveryTag(p)
	m := s.f.publications[tag]
	if m != nil && isDeadLetter(route(p.Topic)) {
		// A dead letter, known by its job's tag (queues.go).
		tag += deadLetterTag
		m = s.f.publications[tag]
	}
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%s: delivery %q (%s) through %q: %s (design.md, Flows, SUBSCRIBE, A shared group)", s.id, p.Topic, tag, sh.filter, fmt.Sprintf(format, args...))
	}
	switch {
	case m == nil || m.topic != p.Topic:
		return bad("no publication of the run's on that topic")
	case !matchesFilter(sh.part, p.Topic):
		return bad("its topic does not match the filter part %q (MQTT 5 4.8.2)", sh.part)
	case !may(sh.reader, p.Topic, false):
		return bad("a topic its user %s may not read: the member is passed over", sh.reader)
	case p.QoS != min(m.qos, sh.qos):
		return bad("QoS %d; want the lower of its QoS %d and the member's grant %d (MQTT-4.8.2-3)", p.QoS, m.qos, sh.qos)
	}
	onChannel := m.channel != "" && m.channel != broadcast
	if retain := !onChannel && s.conn.Version == harness.V5 && sh.keep && m.retain; p.Retain != retain {
		return bad("RETAIN %v; want %v (Channels, $broadcast: \"RETAIN is 1 on retained values sent to a new subscription. Otherwise it is 0 unless Retain As Published\")", p.Retain, retain)
	}
	if s.conn.Version == harness.V5 {
		var err error
		if onChannel {
			err = s.saguinProperties(p, m, []deliveryShape{{allowed: true, multi: len(sh.reach) > 1, single: len(sh.reach) <= 1}})
		} else {
			err = s.noSaguin(p, m)
		}
		if err != nil {
			return err
		}
	}
	l := &s.f.shares
	l.mu.Lock()
	defer l.mu.Unlock()
	g := l.groups[sh.filter]
	// A delayed Will's serial is its connection's end, only the earliest
	// it can enter its channel: it is published "at the earlier of Will
	// Delay ending and the session ending" (design.md, DISCONNECT), or at a
	// clean start ending its session (CONNECT, 6), so a group whose first
	// member subscribed in between may receive it. So with a dead letter,
	// whose serial is its job's publication's, entering its channel at the
	// move (queues.go).
	if m.serial <= g.first && (m.will == nil || m.will.delay == 0) && m.deadOf == nil {
		return bad("published before the group's first member subscribed: \"A new group starts at the tail of every channel it reaches\"")
	}
	l.delivered++
	switch h := g.holders[tag]; {
	case h == nil:
	case h.subs == s.subs:
		if h.conn == s.conn && !p.Dup {
			return bad("a second copy to its member while connected, without DUP (design.md, Delivery: \"Nothing is re-sent while connected\")")
		}
	case h.qos != 1 || h.settled || !h.subs.ended:
		return bad("sent to a second member, the first's copy at QoS %d, its acknowledgement taken %v, its session ended %v: \"Each message goes to one member\"; only \"an unfinished QoS 1 message goes to another member when its member's session ends\" (MQTT-4.8.2-5, -6)",
			h.qos, h.settled, h.subs.ended)
	default:
		l.returned++
	}
	g.holders[tag] = &holding{subs: s.subs, conn: s.conn, qos: p.QoS}
	if p.QoS == 1 {
		if s.incoming.shared == nil {
			s.incoming.shared = map[*harness.Publish]shareKey{}
		}
		s.incoming.shared[p] = shareKey{sh.filter, tag}
	}
	return nil
}

// acknowledgedShare notes that s's client sent the PUBACK of p, a shared
// delivery or another, which settles the message once the broker has
// taken it (confirmShares).
func (s *slot) acknowledgedShare(p *harness.Publish) {
	if k, ok := s.incoming.shared[p]; ok {
		delete(s.incoming.shared, p)
		s.incoming.acked = append(s.incoming.acked, k)
	}
}

// confirmShares settles the shared messages s's client acknowledged
// before a PINGRESP it has just read: the core took those PUBACKs first
// (design.md, Execution, Decisions: "Each connection's packet order is
// preserved"), so no session end after can hand them to another member.
func (s *slot) confirmShares() {
	if len(s.incoming.acked) == 0 {
		return
	}
	l := &s.f.shares
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range s.incoming.acked {
		if h := l.groups[k.filter].holders[k.tag]; h != nil && h.subs == s.subs {
			h.settled = true
		}
	}
	s.incoming.acked = nil
}

// firstHeld is the first SUBACK serial of every group a session of sets,
// or an away session of an empty client id (away), holds, before which no
// publication may be released: joining a group "preserves its cursors and
// returned deliveries", so a member may receive what the group owed before
// it joined.
func (l *shareLog) firstHeld(sets []*subscriptions) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	oldest := uint64(math.MaxUint64)
	for _, set := range slices.Concat(sets, l.away) {
		for filter := range set.shares {
			oldest = min(oldest, l.groups[filter].first)
		}
	}
	return oldest
}

// forget drops the shared deliveries of publications the registry has
// released, which no delivery may name any more.
func (l *shareLog) forget(publications map[string]*message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, g := range l.groups {
		maps.DeleteFunc(g.holders, func(tag string, _ *holding) bool { return publications[tag] == nil })
	}
}
