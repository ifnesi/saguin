package store

import (
	"sync"
	"time"
)

// Latest is a latest-value-per-topic store: one record per topic, replaced
// on every publish.
//
// Values still carry offsets from a monotonic counter, so a consumer sees a
// saguin-offset on a latest record as it does everywhere else and can tell
// which of two values it holds is newer. The counter is not a position - a
// latest channel has no history to resume from, and replacing a topic's
// value gives it a new offset rather than keeping the old one.
type Latest struct {
	mu      sync.Mutex
	current map[string]Record
	next    uint64

	// quota is the provider's bound across every channel it holds, or nil
	// when it has none. A latest channel has no bound of its own - RFC 0002
	// gives it none - but the values it holds are in the same memory as
	// everything else the provider owns, so they count against it.
	quota *Quota

	// holds are the exactly-once publishes waiting for their release
	// (HeldPublish).
	holds holds
}

// SetQuota gives the channel its provider's bound, shared with every other
// channel that provider holds. It charges what the channel already holds;
// see Log.SetQuota.
func (l *Latest) SetQuota(q *Quota) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.quota == q {
		return // already charged; see Log.SetQuota
	}
	l.quota = q
	held := l.holds.bytes
	for _, r := range l.current {
		held += RecordSize(r)
	}
	q.Charge(held)
}

func NewLatest() *Latest {
	return &Latest{current: map[string]Record{}, next: 1}
}

// Set stores a record as its topic's current value, replacing whatever was
// there, and returns it with its offset assigned.
//
// The channel has no size bound of its own: it is one value per topic, so
// what grows is the number of topics rather than a history, and the tool
// for a topic that has gone quiet is the retention period (RFC 0002).
//
// Its provider's bound still applies, because these values sit in the same
// memory as everything else that provider holds. A replacement is measured
// as a difference - the value going out is given back before the new one is
// taken - or a hundred topics updated for long enough would fill a provider
// while holding a hundred records. That is cheap here, because the value
// being replaced is already in the map. On a database it would be a lookup
// before every write, which is why a sqlite provider bounds its file
// instead.
func (l *Latest) Set(r Record) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var freed int64
	if old, ok := l.current[r.Topic]; ok {
		freed = RecordSize(old)
	}
	// **The difference, in one call to the quota**, so no other store on the
	// provider can take the room between the old value's bytes going back and
	// the new one's being taken, and a refusal leaves the provider counting
	// exactly what it holds (invariant 13).
	if size := RecordSize(r); size > freed {
		if !l.quota.Take(size - freed) {
			return Record{}, ErrProviderFull
		}
	} else {
		l.quota.Give(freed - size)
	}

	r.Offset = l.next
	l.next++
	l.current[r.Topic] = r
	return r, nil
}

// Next returns the offset the next value stored here will take.
//
// It exists on both stores rather than on one because the conformance
// script asks it of both: a copy taking offsets it was given has to keep
// this above every one it has seen, or a copy promoted to a primary reissues
// numbers its own records already hold (invariant 9).
func (l *Latest) Next() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// Delete removes a topic's current value and reports whether there was one.
func (l *Latest) Delete(topic string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	old, had := l.current[topic]
	if had {
		l.quota.Give(RecordSize(old))
	}
	delete(l.current, topic)
	return had, nil
}

// Trim deletes the current value of every topic whose last publish was
// before the given moment, and reports how many went and what they came to.
//
// **Expiry here deletes the current value, and that is the point rather
// than a defect** (RFC 0003). A subscriber arriving afterwards is told
// nothing about the topic, exactly as for one that never existed - because
// a stale reading is worse than none, and a consumer cannot tell the two
// apart if the old one is still being served. It follows that a device
// reporting less often than the period loses its state between reports, and
// a channel holding slow-moving state says so with its own period.
//
// There is no floor and nothing to advance. A latest channel has no history
// and no consumer positions into it: a subscriber is sent the whole of
// current state on every subscribe, so there is no coordinate that removal
// could invalidate.
//
// The bytes go back to the provider, which is the only counter a latest
// channel has - it keeps none of its own, because RFC 0002 gives it no size
// bound. That makes this the one channel type whose removal has to give
// room back to a counter it does not own.
// **Two clocks, because a value and a deletion are not the same thing to
// keep.** A value lives as long as it is the truth. A deletion only has to
// live long enough for everything that reads this channel to have seen it -
// on a channel keeping values for ever, keeping every deletion for ever
// too would make the topic count grow with every device ever
// decommissioned, and nothing would ever bring it down.
//
// Either clock is off at its zero value, so a channel with no periods
// removes nothing.
func (l *Latest) Trim(before, deletionsBefore time.Time) (removed int, freed int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if before.IsZero() && deletionsBefore.IsZero() {
		return 0, 0, nil
	}
	for topic, r := range l.current {
		if !PastRetention(r, before, deletionsBefore) {
			continue
		}
		size := RecordSize(r)
		l.quota.Give(size)
		freed += size
		removed++
		delete(l.current, topic)
	}
	return removed, freed, nil
}

// PastRetention reports whether a latest channel's retention removes r: a
// value stamped before `before`, a deletion before `deletionsBefore`. Either
// clock is off at its zero, and an unset timestamp is not old (see
// Log.Trim). **The one statement of the rule**, which Trim applies and a
// bridge's latest rule applies to the values waiting to cross, so that
// nothing crosses that the channel no longer holds; the sqlite store states
// the same rule in its query.
func PastRetention(r Record, before, deletionsBefore time.Time) bool {
	cutoff := before
	if IsDeletion(r) {
		cutoff = deletionsBefore
	}
	return !cutoff.IsZero() && !r.Timestamp.IsZero() && r.Timestamp.Before(cutoff)
}

// TrimExpired removes every value whose publisher's Message Expiry
// Interval has run out (MQTT-3.3.2-5). It reads the publisher's clock
// alone - a value with no expiry is not touched, however old - and it
// serves the retained store only: on a `latest` channel the client's
// expiry never deletes anything, because the operator's retention is the
// only clock that removes a record from a channel (invariant 2).
//
// A deletion is skipped even though the retained store never holds one,
// because this method is on the interface every latest store implements
// and a rule that is safe only where it happens to be called is not a
// rule.
func (l *Latest) TrimExpired(now time.Time) (removed int, freed int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for topic, r := range l.current {
		if IsDeletion(r) || !r.Expired(now) {
			continue
		}
		size := RecordSize(r)
		l.quota.Give(size)
		freed += size
		removed++
		delete(l.current, topic)
	}
	return removed, freed, nil
}

// Get returns the current value of one topic, and whether there is one.
//
// It is a map lookup rather than Match with an exact predicate, which is
// the difference between a point read and a scan: Match walks every topic
// in the channel, allocates a slice sized to the whole of it, and sorts
// the result, all to return at most one record.
func (l *Latest) Get(topic string) (Record, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.current[topic]
	return r, ok, nil
}

// Match returns the current value of every topic a filter reaches, oldest
// first, so a subscriber receives state in the order it was written.
//
// **A deletion is not current state and is not returned here.** It is held
// so that a copy of this channel can be told a topic is gone (RFC 0003), and
// to every ordinary reader a deleted topic is one there is nothing to say
// about - which is what this channel promised before deletions were stored
// at all, and what it still promises.
func (l *Latest) Match(match func(topic string) bool) ([]Record, error) {
	return l.match(false, match)
}

// MatchWithDeletions is Match, including the deletions Match leaves out.
//
// It is what a copy of this channel on another broker is served, and only
// that: a copy has to be told a topic is gone, and every other reader is
// asking what the state *is*. Two methods rather than one taking a flag,
// because `Match(true, …)` at a call site says nothing about which of the
// two things it is asking for.
func (l *Latest) MatchWithDeletions(match func(topic string) bool) ([]Record, error) {
	return l.match(true, match)
}

func (l *Latest) match(deletions bool, match func(topic string) bool) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]Record, 0, len(l.current))
	for topic, r := range l.current {
		if IsDeletion(r) && !deletions {
			continue
		}
		if match(topic) {
			out = append(out, r)
		}
	}
	sortByOffset(out)
	return out, nil
}

// Len returns how many topics have a current value.
func (l *Latest) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.current)
}

// sortByOffset orders records oldest first. Insertion sort: a latest
// channel holds one record per topic, and the slice is a subscriber's
// matching subset of that.
func sortByOffset(rs []Record) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j-1].Offset > rs[j].Offset; j-- {
			rs[j-1], rs[j] = rs[j], rs[j-1]
		}
	}
}
