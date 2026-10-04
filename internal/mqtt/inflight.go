// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// Inflight is a map of packets keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]*inflightEntry // internal contains the inflight packets
	receiveQuota        int32                     // remaining inbound qos quota for flow control
	sendQuota           int32                     // remaining outbound qos quota for flow control
	maximumReceiveQuota int32                     // maximum allowed receive quota
	maximumSendQuota    int32                     // maximum allowed send quota

	// bytes and messages are what the PUBLISH entries hold, kept as they are
	// set and removed rather than counted, so a bound can be asked on every
	// delivery and a scrape can sum every session. Guarded by the lock.
	bytes    int64
	messages int64

	// bounded is how many Bounded PUBLISH entries the table holds, and
	// tabled whether bytes holds InflightTableCost. Guarded by the lock.
	bounded int64
	tabled  bool

	// wire is the part of bytes written and not yet acknowledged, and takeBack
	// and takeBackOffline the parts DropOldest's rule could give up for a
	// connected client and for one that is away; withheld is how many entries
	// wait to be written. Kept the same way, so the bound's checks on every
	// delivery count nothing.
	wire, takeBack, takeBackOffline, withheld int64

	// claiming is how many entries TakeImmediate has claimed that are not yet
	// written (Claimed). A delivery made meanwhile waits behind them rather
	// than overtaking them (MayWrite).
	claiming int64

	// reserved holds the packet identifiers chosen for deliveries that are
	// not yet registered here, taken by Claim and released by Set or Unclaim,
	// and those of deliveries retired whose keyed state is still being
	// cleared (Retire). Guarded by the lock.
	reserved map[uint16]struct{}

	// seq numbers entries as they are registered, and order and waiting are
	// every entry and every withheld one in that order (inflightOrder).
	// Guarded by the lock.
	seq     uint64
	order   inflightOrder
	waiting inflightOrder
}

// inflightEntry is one delivery or acknowledgement in flight.
type inflightEntry struct {
	pk packets.Packet
	// seq is where it was registered among the session's entries, which is
	// its place: kept when the entry is updated, new when an identifier is
	// given to another delivery.
	seq uint64
	// queued says the entry has a slot in waiting (inflightOrder).
	queued bool
	// deadline is a withheld entry's own Expiry, which the withheld marker
	// (-1) takes the place of in pk until the entry is claimed (withhold).
	deadline int64
	// queuedCopy is the copy of a delivery waiting in its client's outbound
	// queue, never yet written, until its write claims it (ClaimQueued).
	queuedCopy *packets.Packet
	// writing says a writer has claimed the entry's first send: its window
	// slot is taken, and only that writer may retire it (firstSend).
	writing bool
}

// neverSent reports whether the entry has never been put on the wire, nor is
// being: withheld, or queued and not yet claimed. Called with the lock held.
func (e *inflightEntry) neverSent() bool {
	return !e.writing && (e.pk.Expiry < 0 || e.queuedCopy != nil)
}

// mayExpire is the expiry sweep's one rule (RFC 0005,
// saguin_deliveries_expired_total): **a delivery never sent expires, and one
// sent is never expired**, whatever its session. Once a QoS 2 PUBLISH has
// been sent its sender must not apply message expiry [MQTT-4.3.3-7]: expired
// on the wire, its identifier and window slot were given back while the
// client still held the exchange, and its PUBREC then named a delivery that
// was gone. A resumable session's is sent again when it resumes
// [MQTT-4.4.0-1]; one whose session ends with its connection goes when that
// session ends (Client.ClearInflights), which is the session ending and not
// an expiry. One being first sent is its writer's (firstSend). Called with
// the lock held.
func (e *inflightEntry) mayExpire() bool {
	return e.neverSent()
}

// withhold puts the withheld marker in place of an entry's Expiry, keeping a
// real deadline aside for the claim to put back and the expiry sweep to read
// (due).
//
// **The marker used to be written over the deadline**, so a withheld
// delivery never expired, and when the client's window opened it was written
// with the Message Expiry Interval its publisher gave it however long it had
// waited [MQTT-3.3.2-5] [MQTT-3.3.2-6]: measured, a message with a 1s interval
// withheld for 2.5s was sent, and one with 60s arrived saying 60s
// (TestAWithheldDeliveryKeepsItsMessageExpiry). Called with the lock held.
func (e *inflightEntry) withhold(prior int64) {
	if prior > 0 {
		e.deadline = prior
	}
	e.pk.Expiry = -1
	e.queuedCopy = nil // withheld instead of queued
}

// claim takes the withheld marker off, putting back its deadline, or zero,
// which marks a written packet with none. Called with the lock held.
func (e *inflightEntry) claim() {
	e.pk.Expiry = e.deadline
	e.deadline = 0
}

// due is the entry's packet with its own deadline in Expiry, withheld or not,
// for the expiry sweep. Called with the lock held.
func (e *inflightEntry) due() packets.Packet {
	pk := e.pk
	if pk.Expiry < 0 {
		pk.Expiry = e.deadline
	}
	return pk
}

// inflightOrder is entries in the order they were registered, oldest first.
//
// **The order of a session's deliveries is the order they were registered**,
// and neither of the things it used to be worked out from says it. A packet
// identifier wraps at 65,535 and starts again on a connection that takes a
// session over; a creation time is in whole seconds. So within one second a
// later delivery could hold a lower identifier, and ordered by the two it was
// written first [MQTT-4.6.0-1], re-sent first, and kept when the session's
// bound gave up the oldest - the newest was given up instead
// (TestADeliveryKeepsItsPlaceAcrossAnIdentifierWrap).
//
// **Slots are removed lazily.** An entry leaves the table under its lock in
// one step, and its slot stays until a walk from the front passes it: a slot
// is live while its identifier holds an entry of the same seq. So the oldest
// entry, and the oldest withheld one, are found from the front rather than by
// copying every entry to compare them - which, under a publisher's fan-out,
// was the session's whole table copied under its lock for every delivery it
// gave up and every one it wrote.
type inflightOrder struct {
	slots []inflightSlot
	head  int
}

type inflightSlot struct {
	id  uint16
	seq uint64
}

// push adds the newest slot.
func (o *inflightOrder) push(s inflightSlot) { o.slots = append(o.slots, s) }

// insert puts a slot back at its place, for an entry withheld again.
func (o *inflightOrder) insert(s inflightSlot) {
	live := o.slots[o.head:]
	at := sort.Search(len(live), func(k int) bool { return live[k].seq > s.seq })
	o.slots = append(o.slots, inflightSlot{})
	copy(o.slots[o.head+at+1:], o.slots[o.head+at:])
	o.slots[o.head+at] = s
}

// compact drops the slots before head and any keep says are dead, once they
// are most of what is held.
func (o *inflightOrder) compact(live int, keep func(inflightSlot) bool) {
	if len(o.slots)-o.head <= 2*live+64 && o.head <= len(o.slots)/2 {
		return
	}
	out := make([]inflightSlot, 0, live+16)
	for _, s := range o.slots[o.head:] {
		if keep(s) {
			out = append(out, s)
		}
	}
	o.slots, o.head = out, 0
}

// InflightEntryOverhead is the heap one in-flight PUBLISH takes beside the
// bytes it carries: the packet struct, its map slot and its copied fields.
// TestWhatAnInflightEntryCostsIsWhatItIsCharged holds it under what it is
// charged. Measured at 8,192 entries with 20, 256 and 4,096-byte payloads:
// 589, 821 and 4,661 bytes an entry, about 540 each beside the payload and
// topic, and up to 637 for a 20-byte payload at 100 entries. It was 960
// while a packet carried a CONNECT's 392 bytes of fields whatever it was
// (packets.ConnectParams).
const InflightEntryOverhead = 600

// InflightTableCost is what a table takes once it holds any PUBLISH, beside
// its entries: the map its first entry makes and the order's first slots.
// It is charged once, by whichever path counts the table's entries: here,
// to bytes, while the table holds one it counts and no Bounded one; and by
// the broadcast log, which counts the Bounded ones, while it has any on the
// wire (saguin's owed.tableLocked). It goes with the last entry, so what
// the bound may give up here (DroppableBytes) includes it only where every
// entry may go. TestWhatASmallInflightTableCostsIsWhatItIsCharged holds it:
// measured over 2,000 tables of a 20-byte payload, 750 bytes a table of
// one entry and 1,296 of two, charged 953 and 1,606; 936 for one where the
// empty table itself, made with its client, is counted as well.
const InflightTableCost = 300

// InflightSize is inflightSize, for a caller that registers its own
// deliveries (saguin's channels) and asks the same question the engine asks of
// its own before writing one (WireHasRoom).
func InflightSize(pk packets.Packet) int64 { return inflightSize(pk) }

// inflightSize is what one entry counts against a session's bound: the memory
// a PUBLISH takes, and nothing for an acknowledgement, which carries no data.
func inflightSize(pk packets.Packet) int64 {
	if pk.FixedHeader.Type != packets.Publish {
		return 0
	}
	n := InflightEntryOverhead + len(pk.Payload) + len(pk.TopicName) +
		len(pk.Properties.ContentType) + len(pk.Properties.ResponseTopic) +
		len(pk.Properties.CorrelationData)
	for _, u := range pk.Properties.User {
		n += len(u.Key) + len(u.Val)
	}
	return int64(n)
}

func (i *Inflight) account(e *inflightEntry, sign int64) {
	pk := &e.pk
	if pk.Expiry < 0 {
		i.withheld += sign // every entry TakeImmediate would look for, whatever its type
	}
	// **A Bounded delivery is counted once, by whoever wrote it.** Saguin's
	// broadcast log counts each message a session is owed against the
	// session's bound, on the wire or not; counted here as well, it would be
	// counted twice, and against a bound it is not held to. It still holds
	// its slot in the client's window: it is in this table, and the send
	// quota is not kept here.
	if pk.FixedHeader.Type != packets.Publish {
		return
	}
	defer i.charge()
	if pk.Bounded {
		i.bounded += sign
		return
	}
	size := sign * inflightSize(e.pk)
	i.bytes += size
	i.messages += sign
	if pk.Expiry >= 0 {
		i.wire += size
	}
	if droppable(pk, false) {
		i.takeBack += size
	}
	if droppable(pk, true) {
		i.takeBackOffline += size
	}
}

// charge keeps InflightTableCost in bytes while the table holds an entry it
// counts and no Bounded one: whoever counts a Bounded entry charges the
// table for it (the broadcast log, saguin's owed.tableLocked), and a table
// holding both is charged once, there. Called with the lock held.
func (i *Inflight) charge() {
	if want := i.messages > 0 && i.bounded == 0; want != i.tabled {
		if want {
			i.bytes += InflightTableCost
		} else {
			i.bytes -= InflightTableCost
		}
		i.tabled = want
	}
}

// live reports whether a slot still names the entry it was made for, and
// returns it. Called with the lock held.
func (i *Inflight) live(s inflightSlot) (*inflightEntry, bool) {
	e, ok := i.internal[s.id]
	return e, ok && e.seq == s.seq
}

// put registers m, in place where its identifier already holds an entry and as
// the newest otherwise, and reports whether it is new. Called with the lock
// held.
func (i *Inflight) put(m packets.Packet) (*inflightEntry, bool) {
	e, ok := i.internal[m.PacketID]
	if ok {
		i.account(e, -1)
		prior, held := e.pk.Expiry, e.deadline
		e.pk = m
		e.deadline = 0
		e.queuedCopy = nil
		if m.Expiry < 0 {
			// Withheld as it is updated (makeDelivery): the deadline it had,
			// or the one it was already withheld with.
			e.withhold(max(prior, held))
		}
	} else {
		i.seq++
		e = &inflightEntry{pk: m, seq: i.seq}
		i.register(m.PacketID, e)
		i.order.push(inflightSlot{m.PacketID, e.seq})
	}
	i.queue(e)
	i.account(e, 1)
	delete(i.reserved, m.PacketID) // registered here, so no longer only reserved
	if !ok {
		i.compact()
	}
	return e, !ok
}

// queue gives a withheld entry its slot in waiting, where it has none.
// Called with the lock held.
func (i *Inflight) queue(e *inflightEntry) {
	if e.pk.Expiry >= 0 || e.queued {
		return
	}
	e.queued = true
	if n := len(i.waiting.slots); n == i.waiting.head || i.waiting.slots[n-1].seq < e.seq {
		i.waiting.push(inflightSlot{e.pk.PacketID, e.seq})
		return
	}
	i.waiting.insert(inflightSlot{e.pk.PacketID, e.seq})
}

// remove takes an entry out of the table; its slots go when a walk passes
// them. Called with the lock held.
func (i *Inflight) remove(e *inflightEntry) {
	i.account(e, -1)
	delete(i.internal, e.pk.PacketID)
}

// compact keeps the orders from growing past what they index. Called with
// the lock held.
func (i *Inflight) compact() {
	i.order.compact(len(i.internal), func(s inflightSlot) bool {
		_, ok := i.live(s)
		return ok
	})
	i.waiting.compact(int(i.withheld), func(s inflightSlot) bool {
		e, ok := i.live(s)
		if ok && e.pk.Expiry >= 0 {
			e.queued = false
			return false
		}
		return ok
	})
}

// front is the oldest live entry of o that match accepts, dropping dead slots
// it passes at the front. Called with the lock held.
func (i *Inflight) front(o *inflightOrder, withheldOnly bool, match func(*inflightEntry) bool) (*inflightEntry, int) {
	for k := o.head; k < len(o.slots); k++ {
		e, ok := i.live(o.slots[k])
		if ok && withheldOnly && e.pk.Expiry >= 0 {
			// Written since it was queued. Its slot goes only from the front,
			// and the entry is told so only then: a slot left in the middle is
			// still its slot, and one queued again beside it would be two.
			if k == o.head {
				e.queued = false
				o.head++
			}
			continue
		}
		if !ok {
			if k == o.head {
				o.head++
			}
			continue
		}
		if match(e) {
			return e, k
		}
	}
	return nil, -1
}

// NewInflights returns a new instance of an Inflight packets map.
//
// Its two maps are made by the first entry and the first reservation
// (saguin): a session that is away, or subscribed at QoS 0, may never have
// either, and each was 48 bytes a session for nothing.
func NewInflights() *Inflight {
	return &Inflight{}
}

// register puts e under id, making the map for it on the first. The caller
// holds the lock, or has the only reference.
func (i *Inflight) register(id uint16, e *inflightEntry) {
	if i.internal == nil {
		i.internal = map[uint16]*inflightEntry{}
	}
	i.internal[id] = e
}

// reserve holds id back from Claim, making the map for it on the first.
// The caller holds the lock.
func (i *Inflight) reserve(id uint16) {
	if i.reserved == nil {
		i.reserved = map[uint16]struct{}{}
	}
	i.reserved[id] = struct{}{}
}

// Claim takes the first free packet identifier after start and reserves it,
// and reports whether it found one.
//
// **Chosen and reserved in one step, under this lock.** A delivery takes its
// identifier before its session store write and is registered here only after
// it, so an identifier chosen and not yet registered used to look free: with
// nearly all of them in use, a second delivery's search came back round to it
// and was given the same one [MQTT-2.2.1-4]. An acknowledgement for it would
// then complete one delivery and lose the other. The reservation is released
// by Set, where the delivery is registered, and by Unclaim where the caller
// abandons it.
func (i *Inflight) Claim(start, max uint32) (uint32, bool) {
	i.Lock()
	defer i.Unlock()

	// **A full table is known before any search.** Every identifier in flight
	// or reserved is a distinct key of one of these two maps, and a key
	// leaves reserved as it enters internal (put), so their sizes together,
	// less a key 0 that MQTT never assigns, are how many of 1 to 65,535 are
	// spoken for. Without it a client whose window was as large as the
	// identifier space had every delivery scan all 65,535 identifiers under
	// this lock before failing: 1.69 ms a failed claim, and at 400-500 a
	// second the lock the PUBACKs also take was held for most of every second
	// (the 2026-09-27 fleet rerun's replay A/B). Only at the MQTT maximum,
	// where it is exact; a smaller one is a test's, where the search is short.
	if max >= math.MaxUint16 {
		used := len(i.internal) + len(i.reserved)
		if _, ok := i.internal[0]; ok {
			used--
		}
		if _, ok := i.reserved[0]; ok {
			used--
		}
		if used >= math.MaxUint16 {
			return 0, false
		}
	}

	n, started, overflowed := start, start, false
	for {
		if overflowed && n == started {
			return 0, false
		}
		if n >= max {
			overflowed = true
			n = 0
			continue
		}
		n++
		id := uint16(n)
		if _, inFlight := i.internal[id]; inFlight {
			continue
		}
		if _, taken := i.reserved[id]; taken {
			continue
		}
		i.reserve(id)
		return n, true
	}
}

// Unclaim releases an identifier Claim reserved for a delivery that was not
// made, so that the slot does not stay spent for the life of the session - or
// one Retire, RetireExpired or DropOldest kept reserved, once everything
// keyed by it has been cleared.
func (i *Inflight) Unclaim(id uint16) {
	i.Lock()
	defer i.Unlock()
	delete(i.reserved, id)
}

// Set adds or updates an inflight packet by packet id.
func (i *Inflight) Set(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()
	_, isNew := i.put(m)
	return isNew
}

// Bytes is the memory the session's PUBLISH entries take, as inflightSize
// counts it.
func (i *Inflight) Bytes() int64 {
	i.RLock()
	defer i.RUnlock()
	return i.bytes
}

// Messages is how many PUBLISH entries the session holds.
func (i *Inflight) Messages() int64 {
	i.RLock()
	defer i.RUnlock()
	return i.messages
}

// DropOldest removes the oldest entries that are safe to take back until the
// session holds no more than max bytes, never the entry keep names, and
// returns what it removed. offline says the session's client is away.
//
// **Safe to take back is narrow, and each exclusion is a way to lose or
// corrupt a message instead of shedding one:**
//
//   - QoS 1 only. A QoS 2 PUBLISH the client has received holds its packet
//     identifier in the client's own state until the PUBREL, so reusing the
//     identifier for another message makes the client discard that one as a
//     duplicate.
//   - Withheld, or on a session whose client is away. A QoS 1 delivery on the
//     wire to a connected client may still be acknowledged, and an
//     acknowledgement for an identifier already reused completes the wrong
//     message.
//   - Published by a client. A delivery with no origin is one saguin wrote
//     from a channel, and one from the inline client is a queue's offer: each
//     is tracked by saguin under its packet identifier, and its position or
//     its lease would be left pointing at nothing.
func (i *Inflight) DropOldest(max int64, keep uint16, offline bool) []packets.Packet {
	i.Lock()
	defer i.Unlock()
	var dropped []packets.Packet
	// For a connected client only a withheld entry may go, so the oldest is
	// looked for among those; for one that is away, among all of them.
	from, withheldOnly, left := &i.waiting, true, &i.takeBack
	if offline {
		from, withheldOnly, left = &i.order, false, &i.takeBackOffline
	}
	for i.bytes > max && *left > 0 {
		e, _ := i.front(from, withheldOnly, func(e *inflightEntry) bool {
			return e.pk.PacketID != keep && droppable(&e.pk, offline)
		})
		if e == nil {
			break
		}
		i.remove(e)
		i.reserve(e.pk.PacketID) // as Retire: the caller Unclaims it
		dropped = append(dropped, e.pk)
	}
	return dropped
}

// DroppableBytes is what DropOldest could give up: the bytes of every entry
// its rule allows taking back.
func (i *Inflight) DroppableBytes(offline bool) int64 {
	i.RLock()
	defer i.RUnlock()
	d := i.takeBack
	if offline {
		d = i.takeBackOffline
	}
	if i.tabled && d == i.bytes-InflightTableCost {
		d = i.bytes // every entry, and the table goes with the last
	}
	return d
}

// WireHasRoom reports whether a delivery of size bytes may be written while
// what is written and unacknowledged stays within share. Always when nothing
// is, so a delivery larger than the share is still sent on its own; and
// always when share is zero, which is no bound.
func (i *Inflight) WireHasRoom(size, share int64) bool {
	i.RLock()
	defer i.RUnlock()
	return i.wireHasRoom(size, share)
}

// WireEmpty reports whether nothing this table counts against the wire is
// written and unacknowledged: the one state in which WireHasRoom answers yes
// whatever the size.
func (i *Inflight) WireEmpty() bool {
	i.RLock()
	defer i.RUnlock()
	return i.wire == 0
}

// MayWrite reports whether a new delivery of size bytes may be written now
// rather than withheld: nothing is withheld before it, which would be
// overtaken [MQTT-4.6.0-1], and WireHasRoom.
func (i *Inflight) MayWrite(size, share int64) bool {
	i.RLock()
	defer i.RUnlock()
	return i.withheld == 0 && i.claiming == 0 && i.wireHasRoom(size, share)
}

// MayClaim reports whether a withheld entry waits and the wire has room for
// at least a byte of it: what drainWithheld asks before it takes the lock and
// walks the table, which an acknowledgement does every time.
func (i *Inflight) MayClaim(share int64) bool {
	i.RLock()
	defer i.RUnlock()
	return i.withheld > 0 && i.wireHasRoom(1, share)
}

func (i *Inflight) wireHasRoom(size, share int64) bool {
	return share <= 0 || i.wire == 0 || i.wire+size <= share
}

// droppable is DropOldest's rule for one entry.
func droppable(m *packets.Packet, offline bool) bool {
	return m.FixedHeader.Type == packets.Publish && m.FixedHeader.Qos == 1 &&
		(m.Expiry < 0 || offline) &&
		m.Origin != "" && m.Origin != InlineClientId
}

// Get returns an inflight packet by packet id.
func (i *Inflight) Get(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	if e, ok := i.internal[id]; ok {
		return e.pk, true
	}

	return packets.Packet{}, false
}

// Len returns the size of the inflight messages map.
func (i *Inflight) Len() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal)
}

// Clone returns a new instance of Inflight with the same message data.
// This is used when transferring inflights from a taken-over session.
//
// **Reservations are not copied, and cannot be.** An identifier Retire or
// DropOldest reserved is freed by an Unclaim on the instance that reserved it,
// against that instance's own table, so a copy here would be held for ever.
// A reservation therefore protects identifiers within one session instance;
// across instances - this takeover, or a clean start's fresh table - what
// keeps a late removal off a new delivery under a reused identifier is that
// the removal names its message: the broadcast log removes an entry only under
// that identifier at that offset.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	defer i.RUnlock()
	c.seq = i.seq
	c.bytes, c.messages, c.bounded, c.tabled = i.bytes, i.messages, i.bounded, i.tabled
	c.wire, c.takeBack, c.takeBackOffline, c.withheld = i.wire, i.takeBack, i.takeBackOffline, i.withheld
	// In the order they were registered, and under the same numbers, so the
	// session keeps every delivery's place across the takeover.
	for _, sl := range i.order.slots[i.order.head:] {
		e, ok := i.live(sl)
		if !ok {
			continue
		}
		cp := &inflightEntry{pk: e.pk, seq: e.seq, deadline: e.deadline}
		c.register(sl.id, cp)
		c.order.push(sl)
		if e.queuedCopy != nil && !e.writing {
			c.unwritten(cp)
		}
		c.queue(cp)
	}
	return c
}

// unwritten withholds an entry whose queued copy was never written, keeping
// its deadline: the copy is the old connection's, so on this table the entry
// waits for its first send as a withheld one does, claimed and checked for
// expiry like any other (ClaimWithheld). Called with the lock held.
//
// **The queued copy was all that said it was unsent**, and it is not carried
// over: the entry looked written, so the successor sent it as a DUP re-send
// with no expiry check, and the sweep never expired it [MQTT-3.3.2-5].
func (i *Inflight) unwritten(e *inflightEntry) {
	i.account(e, -1)
	e.withhold(e.pk.Expiry)
	i.account(e, 1)
}

// GetAll returns all the inflight messages, or only the withheld ones, in
// the order they were registered.
//
// MQTT-4.6.0-1 requires that re-sent PUBLISH packets go out in the order the
// originals were sent. That order used to be worked out by sorting on Created
// and then the packet identifier, and neither is it: Created is in whole
// seconds, and an identifier wraps and starts again on a takeover
// (inflightOrder). The order is kept instead, so this is a copy under the
// read lock and nothing more - the sort it replaces was most of the cost,
// and NextPacketID waits on this lock for every delivery.
func (i *Inflight) GetAll(immediate bool) []packets.Packet {
	return i.getAll(immediate, math.MaxUint64)
}

// Registered is the number the newest entry was registered under, so that
// what is registered after it can be told apart (GetAllThrough).
func (i *Inflight) Registered() uint64 {
	i.RLock()
	defer i.RUnlock()
	return i.seq
}

// GetAllThrough is GetAll for the entries registered no later than seq
// (Registered), in the order they were registered. An entry given to
// another delivery since is a new registration, and is not among them.
func (i *Inflight) GetAllThrough(seq uint64) []packets.Packet {
	return i.getAll(false, seq)
}

func (i *Inflight) getAll(immediate bool, through uint64) []packets.Packet {
	i.RLock()
	defer i.RUnlock()
	m := make([]packets.Packet, 0, len(i.internal))
	for _, sl := range i.order.slots[i.order.head:] {
		if e, ok := i.live(sl); ok && e.seq <= through && (!immediate || e.pk.Expiry < 0) {
			m = append(m, e.pk)
		}
	}
	return m
}

// Expired returns the entries past their own Message Expiry Interval
// [MQTT-3.3.2-5] or held longer than maximumExpiry, oldest first.
//
// **It copies only what has expired.** The sweep that asks runs once a
// second for every client and used GetAll, which copied and sorted the whole
// table to find what is usually nothing - under the lock every delivery to
// that client waits on.
func (i *Inflight) Expired(now, maximumExpiry int64) []packets.Packet {
	i.RLock()
	defer i.RUnlock()
	var out []packets.Packet
	for _, sl := range i.order.slots[i.order.head:] {
		if e, ok := i.live(sl); ok && e.mayExpire() && expiredAt(e.due(), now, maximumExpiry) {
			out = append(out, e.due())
		}
	}
	return out
}

// A withheld packet is one registered in flight and not yet written, because
// the client's send quota was spent when it was ready. Its Expiry is -1, the
// substrate's marker for exactly that; a written packet carries zero or a
// real deadline.
//
// **Claiming one and writing it are two steps, and the claim is the one that
// has to be atomic.** saguin returns a withheld queue delivery to its queue
// when it has waited too long (returnUnsent), and a withheld packet written
// after that is a job in two workers' hands. So a writer takes the packet by
// turning its marker off under the lock, and saguin removes one only while
// the marker is still on: whichever gets there first has it, and the other
// finds nothing.

// Withhold marks a registered packet as not yet written.
func (i *Inflight) Withhold(id uint16) {
	i.Lock()
	defer i.Unlock()
	if e, ok := i.internal[id]; ok {
		i.account(e, -1)
		e.withhold(e.pk.Expiry)
		i.queue(e) // back at its own place, not at the end
		i.account(e, 1)
	}
}

// WithholdAgain withholds a delivery that has been written once, marking it
// DUP, so that the write that sends it later - drainWithheld, which writes a
// withheld delivery as it finds it - sends it as the re-send it is
// [MQTT-3.3.1-1]. It is for a resumed session's table that the client's window
// does not admit at once.
func (i *Inflight) WithholdAgain(id uint16) {
	i.Lock()
	defer i.Unlock()
	if e, ok := i.internal[id]; ok {
		i.account(e, -1)
		e.pk.FixedHeader.Dup = true
		e.withhold(e.pk.Expiry)
		i.queue(e) // back at its own place, not at the end
		i.account(e, 1)
	}
}

// TakeImmediate claims the earliest withheld packet for writing: it is in
// flight from here on, and a PUBACK for it returns its quota. The order is
// the one GetAll sorts by, so withheld packets go out in the order they were
// registered [MQTT-4.6.0-1]. It claims nothing when the earliest does not fit
// within share on the wire (WireHasRoom), rather than a later one that does.
func (i *Inflight) TakeImmediate(share, now, maximumExpiry int64) (packets.Packet, FirstSend) {
	i.Lock()
	defer i.Unlock()
	if hook := takeImmediateLocked.Load(); hook != nil {
		(*hook)()
	}
	if i.withheld == 0 {
		return packets.Packet{}, FirstSendNone // nothing waits, so nothing to look for
	}
	e, _ := i.front(&i.waiting, true, func(*inflightEntry) bool { return true })
	if e == nil || !i.wireHasRoom(inflightSize(e.pk), share) {
		return packets.Packet{}, FirstSendNone
	}
	// It is the front: everything before it was passed and dropped.
	i.waiting.head++
	e.queued = false
	if expiredAt(e.due(), now, maximumExpiry) {
		pk := e.due()
		i.remove(e)
		i.reserve(pk.PacketID)
		return pk, FirstSendExpired
	}
	i.account(e, -1)
	e.claim()
	i.account(e, 1)
	i.firstSend(e)
	i.claiming++
	return e.pk, FirstSendWrite
}

// FirstSend is what a claim on a delivery's first send found (firstSend).
type FirstSend int

const (
	FirstSendNone    FirstSend = iota // nothing to write: none waits, or its entry is not the one queued
	FirstSendWrite                    // the caller owns the first send, its window slot taken
	FirstSendExpired                  // expired unsent, and retired: the caller owns the retirement
)

// firstSend makes the caller the entry's writer and takes its slot of the
// client's window, in the one lock hold that the expiry sweep retires under.
// Called with the lock held.
//
// **One step, because two let the sweep in between.** The claim made the
// entry look written before its slot was taken: a sweep finding it expired
// then gave back a slot it never held, the writer took one, and the client
// was sent the expired message under an identifier no longer in flight - its
// window one slot smaller for the rest of the connection. The sweep skips a
// claimed entry (neverSent), so its writer alone decides what becomes of it.
func (i *Inflight) firstSend(e *inflightEntry) {
	e.writing = true
	e.queuedCopy = nil
	if atomic.LoadInt32(&i.sendQuota) > 0 {
		atomic.AddInt32(&i.sendQuota, -1)
	}
}

// Queue records cp as the queued copy of the delivery registered under its
// identifier, which has taken its slot of window (makeDelivery).
func (i *Inflight) Queue(cp *packets.Packet) {
	i.Lock()
	defer i.Unlock()
	if e, ok := i.internal[cp.PacketID]; ok {
		e.queuedCopy = cp
	}
}

// Unqueue forgets cp as a queued copy, for a queue closed with it unwritten:
// the entry is withheld, and first sent on the session's next connection
// (unwritten). Forgotten alone, it looked written there.
func (i *Inflight) Unqueue(cp *packets.Packet) {
	i.Lock()
	defer i.Unlock()
	if e, ok := i.internal[cp.PacketID]; ok && e.queuedCopy == cp && !e.writing {
		i.unwritten(e)
		i.queue(e)
	}
}

// ClaimQueued claims the first send of cp, a queued copy, for the write loop:
// FirstSendWrite with its entry exactly the one cp was queued for, and none
// where that entry is gone - retired, and perhaps its identifier another
// delivery's. Its window slot was taken as it was queued.
func (i *Inflight) ClaimQueued(cp *packets.Packet, now, maximumExpiry int64) (packets.Packet, FirstSend) {
	i.Lock()
	defer i.Unlock()
	e, ok := i.internal[cp.PacketID]
	if !ok || e.queuedCopy != cp {
		return packets.Packet{}, FirstSendNone
	}
	if expiredAt(e.due(), now, maximumExpiry) {
		pk := e.due()
		i.remove(e)
		i.reserve(pk.PacketID)
		return pk, FirstSendExpired
	}
	e.queuedCopy = nil
	e.writing = true
	return e.pk, FirstSendWrite
}

// ClaimWithheld claims one withheld delivery's first send by identifier, as
// TakeImmediate claims the earliest's.
func (i *Inflight) ClaimWithheld(id uint16, now, maximumExpiry int64) (packets.Packet, FirstSend) {
	i.Lock()
	defer i.Unlock()
	e, ok := i.internal[id]
	if !ok || e.pk.Expiry >= 0 {
		return packets.Packet{}, FirstSendNone
	}
	if expiredAt(e.due(), now, maximumExpiry) {
		pk := e.due()
		i.remove(e)
		i.reserve(id)
		return pk, FirstSendExpired
	}
	i.account(e, -1)
	e.claim() // its slot in waiting goes when a walk passes it
	i.account(e, 1)
	i.firstSend(e)
	return e.pk, FirstSendWrite
}

// FirstSent ends a claim on a first send once its write has been made or has
// failed: the entry is a written one from here. drained says TakeImmediate
// made the claim, which a delivery made meanwhile waited behind (Claimed).
func (i *Inflight) FirstSent(id uint16, drained bool) {
	i.Lock()
	defer i.Unlock()
	if e, ok := i.internal[id]; ok && e.writing {
		e.writing = false
	}
	if drained {
		i.claiming--
	}
}

// RetireUnsent retires a delivery whose first send its writer claimed and
// then found expired before encoding it (Client.writeFirstSend), and reports
// whether it did: its writer holds the claim, so nothing else retired it.
// The caller gives back its window slot and Unclaims the identifier.
func (i *Inflight) RetireUnsent(id uint16, drained bool) bool {
	i.Lock()
	defer i.Unlock()
	if drained {
		i.claiming--
	}
	e, ok := i.internal[id]
	if !ok || !e.writing {
		return false
	}
	i.remove(e)
	i.reserve(id)
	return true
}

// takeImmediateLocked is a test seam, nil in production: it runs in
// TakeImmediate once the lock is held, so a test can queue a writer there and
// see whether anything after it takes the lock again.
var takeImmediateLocked atomic.Pointer[func()]

// Claimed says an entry TakeImmediate claimed has been written, or its write
// has failed.
//
// **Withheld until then, as far as a new delivery is concerned.** Between the
// claim and the write nothing is withheld, and a delivery made in that moment
// went to the socket's queue and could be written first.
func (i *Inflight) Claimed() {
	i.Lock()
	defer i.Unlock()
	i.claiming--
}

// TakeWithheld claims one withheld packet by identifier, as TakeImmediate
// claims the earliest.
func (i *Inflight) TakeWithheld(id uint16) (packets.Packet, bool) {
	i.Lock()
	defer i.Unlock()
	e, ok := i.internal[id]
	if !ok || e.pk.Expiry >= 0 {
		return packets.Packet{}, false
	}
	i.account(e, -1)
	e.claim() // its slot in waiting goes when a walk passes it
	i.account(e, 1)
	return e.pk, true
}

// DeleteWithheld removes a packet only while it is still withheld, and
// reports whether it did. A packet already claimed for writing stays.
func (i *Inflight) DeleteWithheld(id uint16) bool {
	i.Lock()
	defer i.Unlock()
	e, ok := i.internal[id]
	if !ok || e.pk.Expiry >= 0 {
		return false
	}
	i.remove(e)
	return true
}

// expiredAt is Expired's rule for one entry: past its own Message Expiry
// Interval [MQTT-3.3.2-5], or held longer than a maximum expiry that is set.
//
// **Expired when nothing of its interval remains**: at the second it falls
// due as after it, since a Message Expiry Interval of 0 is not one MQTT lets a
// server send, and the encoder would round it up to a second it does not have.
func expiredAt(tk packets.Packet, now, maximumExpiry int64) bool {
	expired := tk.ProtocolVersion == 5 && tk.Expiry > 0 && tk.Expiry <= now
	enforced := maximumExpiry > 0 && now-tk.Created > maximumExpiry
	return expired || enforced
}

// RetireExpired retires an entry as Retire does, only if it is still
// expired and has never been sent (neverSent), and reports whether it did
// and whether the entry held a slot of the send quota: one queued took one,
// and one withheld never did. Both are read under the lock that
// deletes, since a withheld entry can be written between Expired and here.
// The caller Unclaims the identifier.
//
// **Asked again under the lock that deletes**, because Expired's answer is
// read under a lock this does not hold: between the two, the expired entry
// can be acknowledged and its packet identifier claimed by a new delivery,
// and deleting by identifier alone would then remove the new one - a live
// delivery gone from the table, never retried. A new entry under that
// identifier has not expired, so this leaves it.
func (i *Inflight) RetireExpired(id uint16, now, maximumExpiry int64) (retired, heldQuota bool) {
	i.Lock()
	defer i.Unlock()
	e, ok := i.internal[id]
	if !ok || !e.mayExpire() || !expiredAt(e.due(), now, maximumExpiry) {
		return false, false
	}
	heldQuota = e.pk.Expiry >= 0 // a withheld one never took a slot
	i.remove(e)
	i.reserve(id)
	return true, heldQuota
}

// Retire removes a delivery the server sent, and keeps its packet identifier
// reserved until the caller Unclaims it. It reports whether the entry existed;
// only a caller it answered true may Unclaim, since otherwise the reservation
// may be another delivery's.
//
// **The identifier outlives the entry until everything keyed by it is gone.**
// The broadcast log's record of a delivery on the wire, and the broker's
// record of which channel offset a delivery carries, are keyed by client and
// packet identifier, and both are cleared after the entry leaves this table -
// by OnQosComplete and OnDeliveryDone, and by the drain's own later writes.
// Freed here, the identifier could be claimed by a new delivery before they
// ran, and the clearing meant for the old one would land on the new one
// (TestAnIdentifierIsNotReusedWhileItsCompletionRuns). Reserved in the same
// lock hold that deletes, so there is no moment between the two at which it
// looks free.
//
// Delete is for an inbound QoS 2 exchange, whose identifier the client chose
// and which nothing of ours is keyed by.
func (i *Inflight) Retire(id uint16) bool {
	i.Lock()
	defer i.Unlock()
	e, ok := i.internal[id]
	if !ok {
		return false
	}
	i.remove(e)
	i.reserve(id)
	return true
}

// Delete removes an in-flight message from the map. Returns true if the message existed.
func (i *Inflight) Delete(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	e, ok := i.internal[id]
	if ok {
		i.remove(e)
	}

	return ok
}

// DecreaseReceiveQuota reduces the receive quota by 1.
func (i *Inflight) DecreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) > 0 {
		atomic.AddInt32(&i.receiveQuota, -1)
	}
}

// IncreaseReceiveQuota increases the receive quota by 1.
func (i *Inflight) IncreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) < atomic.LoadInt32(&i.maximumReceiveQuota) {
		atomic.AddInt32(&i.receiveQuota, 1)
	}
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	atomic.StoreInt32(&i.receiveQuota, n)
	atomic.StoreInt32(&i.maximumReceiveQuota, n)
}

// DecreaseSendQuota reduces the send quota by 1.
func (i *Inflight) DecreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) > 0 {
		atomic.AddInt32(&i.sendQuota, -1)
	}
}

// SendQuota is how many more packets may be put in flight to this client
// right now (saguin).
//
// Read-only, and it exists because the quota is a bound: a slot returned
// for a packet that never took one raises it above the client's Receive
// Maximum, which MQTT 5 section 4.9 forbids, and a bound nothing can read
// is a bound no test can hold anyone to.
func (i *Inflight) SendQuota() int32 { return atomic.LoadInt32(&i.sendQuota) }

// IncreaseSendQuota increases the send quota by 1.
func (i *Inflight) IncreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) < atomic.LoadInt32(&i.maximumSendQuota) {
		atomic.AddInt32(&i.sendQuota, 1)
	}
}

// HasSendQuota reports whether the client's window has room for one more
// delivery: always when it has none, as a client stating no Receive Maximum
// does, and then only the session's share on the wire withholds.
func (i *Inflight) HasSendQuota() bool {
	return atomic.LoadInt32(&i.maximumSendQuota) == 0 || atomic.LoadInt32(&i.sendQuota) > 0
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	atomic.StoreInt32(&i.sendQuota, n)
	atomic.StoreInt32(&i.maximumSendQuota, n)
}
