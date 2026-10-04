package store

import (
	"sort"
	"time"
)

// Exchange names one client's half-finished exactly-once publish.
//
// **Both halves are needed and neither substitutes for the other.** MQTT
// gives each session its own set of packet identifiers, assigned
// independently by each end (section 2.2.1), so identifier 1 from one
// client and identifier 1 from another are unrelated messages, and
// identifier 1 the server sent to a client is unrelated to identifier 1
// that client sent to the server. A store keyed on the number alone would
// hand one publisher another's message.
//
// Client is the session rather than the connection. A client that
// reconnects to a session saguin still holds re-sends its PUBLISH under the
// same identifier and must be answered with the message already held; a
// client that asks for a fresh session under the same name must not, which
// is why a clean start drops what that name held.
type Exchange struct {
	Client   string
	PacketID uint16
}

// HeldPublish is an exactly-once publish a channel's store is holding for
// its release: the exchange that sent it, its record without an offset, and
// when it was first held.
//
// **It is held in the store it will be kept in** (RFC 0003 "Exactly once,
// and where the unfinished ones wait"), so the release is one operation in
// one store: the record takes its offset and the hold goes together, and
// there is never a moment with both or with neither. Held anywhere else, the
// release is two writes in two stores, and a refusal or a crash between them
// loses a message whose PUBREC told the publisher it was taken.
type HeldPublish struct {
	Exchange Exchange
	Record   Record
	HeldAt   time.Time
}

// holds are the exactly-once publishes one memory store is holding. They
// live beside the store's records, under its lock, and nothing that reads
// the channel sees them: they have no offset, no reader is served one, no
// Trim takes one, and the channel's own size does not count them. The
// provider's bound does, from the moment each is held.
type holds struct {
	by    map[Exchange]HeldPublish
	bytes int64
}

// hold keeps a publish for its release, taking its room from the provider.
// A repeat of one already held is success and changes nothing, which is
// [MQTT-4.3.3-10] and keeps the clock the first copy started. The caller
// holds the store's lock.
func (h *holds) hold(e Exchange, r Record, now time.Time, q *Quota) error {
	if _, ok := h.by[e]; ok {
		return nil
	}
	size := RecordSize(r)
	if !q.Take(size) {
		return ErrProviderFull
	}
	if h.by == nil {
		h.by = map[Exchange]HeldPublish{}
	}
	r.Offset = 0
	h.by[e] = HeldPublish{Exchange: e, Record: r, HeldAt: now}
	h.bytes += size
	return nil
}

// peek is the publish held for an exchange, left where it is. The caller
// holds the store's lock.
func (h *holds) peek(e Exchange) (HeldPublish, bool) {
	p, ok := h.by[e]
	return p, ok
}

// take removes a hold whose bytes become the record's: the provider's count
// does not move, because the room the hold took is the room the record
// takes. The caller holds the store's lock and has already decided the
// record is stored.
func (h *holds) take(e Exchange) {
	if p, ok := h.by[e]; ok {
		h.bytes -= RecordSize(p.Record)
		delete(h.by, e)
	}
}

// drop removes a hold and gives its room back to the provider, and reports
// whether there was one. The caller holds the store's lock.
func (h *holds) drop(e Exchange, q *Quota) bool {
	p, ok := h.by[e]
	if !ok {
		return false
	}
	size := RecordSize(p.Record)
	h.bytes -= size
	q.Give(size)
	delete(h.by, e)
	return true
}

// list is every hold, by client and then packet identifier. The caller
// holds the store's lock.
func (h *holds) list() []HeldPublish {
	out := make([]HeldPublish, 0, len(h.by))
	for _, p := range h.by {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange.Client != out[j].Exchange.Client {
			return out[i].Exchange.Client < out[j].Exchange.Client
		}
		return out[i].Exchange.PacketID < out[j].Exchange.PacketID
	})
	return out
}

// restore puts back what a snapshot held. Their room is charged with the
// store's own when the provider's bound is given (SetQuota). The caller
// holds the store's lock.
func (h *holds) restore(held []HeldPublish) {
	for _, p := range held {
		if h.by == nil {
			h.by = map[Exchange]HeldPublish{}
		}
		p.Record.Offset = 0
		h.by[p.Exchange] = p
		h.bytes += RecordSize(p.Record)
	}
}

// Hold keeps an exactly-once publish in this channel until its release,
// without an offset: no reader sees it and no Trim takes it (HeldPublish).
// It is refused ErrFull where the channel is at its own max_bytes, as a
// publish would be, and ErrProviderFull where the provider has no room.
func (l *Log) Hold(e Exchange, r Record, now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.holds.peek(e); ok {
		return nil
	}
	if l.maxBytes > 0 && l.held+RecordSize(r) > l.maxBytes {
		return ErrFull
	}
	return l.holds.hold(e, r, now, l.quota)
}

// ReleaseHold stores a held publish as the channel's next record and forgets the
// hold, in one hold of the lock (HeldPublish). The provider's room is never
// what refuses it - the hold took that - but the channel's own max_bytes
// can: then it answers ErrFull, the hold stays, and no offset is taken. The
// bool is false where nothing is held for the exchange.
func (l *Log) ReleaseHold(e Exchange) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p, ok := l.holds.peek(e)
	if !ok {
		return Record{}, false, nil
	}
	r := p.Record
	size := RecordSize(r)
	if l.maxBytes > 0 && l.held+size > l.maxBytes {
		return Record{}, true, ErrFull
	}
	l.holds.take(e)
	r.Offset = l.next
	l.next++
	l.held += size
	l.records = append(l.records, r)
	return r, true, nil
}

// DropHold forgets a hold without storing it, giving its room back, and reports
// whether there was one.
func (l *Log) DropHold(e Exchange) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holds.drop(e, l.quota), nil
}

// Holds is every publish this channel is holding for its release.
func (l *Log) Holds() ([]HeldPublish, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holds.list(), nil
}

// RestoreHeld puts back the holds a snapshot kept, before the provider's
// bound is given.
func (l *Log) RestoreHeld(held []HeldPublish) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holds.restore(held)
}

// Hold keeps an exactly-once publish in this channel until its release; see
// Log.Hold. A latest channel has no size bound of its own, so only the
// provider can refuse it.
func (l *Latest) Hold(e Exchange, r Record, now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holds.hold(e, r, now, l.quota)
}

// ReleaseHold makes a held publish the topic's value and forgets the hold, in
// one hold of the lock; see Log.ReleaseHold. The value it replaces gives its
// room back.
//
// **A deletion of a topic with no live value stores nothing**, as the
// publish path stores nothing for one (a client repeating a deletion would
// otherwise fill the channel with them): the hold goes, its room comes back,
// and the record is answered without an offset for the caller to deliver as
// it delivers any deletion.
func (l *Latest) ReleaseHold(e Exchange) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p, ok := l.holds.peek(e)
	if !ok {
		return Record{}, false, nil
	}
	r := p.Record
	old, had := l.current[r.Topic]
	if IsDeletion(r) && (!had || IsDeletion(old)) {
		l.holds.drop(e, l.quota)
		return r, true, nil
	}
	l.holds.take(e)
	if had {
		l.quota.Give(RecordSize(old))
	}
	r.Offset = l.next
	l.next++
	l.current[r.Topic] = r
	return r, true, nil
}

// DropHold forgets a hold without storing it; see Log.DropHold.
func (l *Latest) DropHold(e Exchange) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holds.drop(e, l.quota), nil
}

// Holds is every publish this channel is holding for its release.
func (l *Latest) Holds() ([]HeldPublish, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holds.list(), nil
}

// RestoreHeld puts back the holds a snapshot kept; see Log.RestoreHeld.
func (l *Latest) RestoreHeld(held []HeldPublish) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holds.restore(held)
}

// Hold keeps an exactly-once publish in this queue until its release; see
// Log.Hold.
func (q *Queue) Hold(e Exchange, r Record, now time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.holds.peek(e); ok {
		return nil
	}
	if q.maxBytes > 0 && q.held+RecordSize(r) > q.maxBytes {
		return ErrFull
	}
	return q.holds.hold(e, r, now, q.quota)
}

// ReleaseHold makes a held publish available work and forgets the hold, in one
// hold of the lock; see Log.ReleaseHold. A queue at its own max_bytes answers
// ErrFull and keeps the hold, and has room again as its workers resolve
// what they hold.
func (q *Queue) ReleaseHold(e Exchange) (Record, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	p, ok := q.holds.peek(e)
	if !ok {
		return Record{}, false, nil
	}
	r := p.Record
	size := RecordSize(r)
	if q.maxBytes > 0 && q.held+size > q.maxBytes {
		return Record{}, true, ErrFull
	}
	q.holds.take(e)
	r.Offset = q.next
	q.next++
	q.held += size
	q.items[r.Offset] = &Item{Record: r, State: Available}
	q.order = append(q.order, r.Offset)
	return r, true, nil
}

// DropHold forgets a hold without storing it; see Log.DropHold.
func (q *Queue) DropHold(e Exchange) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.holds.drop(e, q.quota), nil
}

// Holds is every publish this queue is holding for its release.
func (q *Queue) Holds() ([]HeldPublish, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.holds.list(), nil
}

// RestoreHeld puts back the holds a snapshot kept; see Log.RestoreHeld.
func (q *Queue) RestoreHeld(held []HeldPublish) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.holds.restore(held)
}
