package broker

import (
	"errors"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// Storage failures are counted where every store call passes rather than at
// the sites that log them, and that is the whole design of this file.
//
// **A list of call sites rots; a wrapper cannot.** The broker makes over a
// hundred calls that can fail on a store, and the next one - added next
// month, by somebody who copied the one above it - would be missed by any
// counter wired site by site, silently, in the metric an operator watches
// to find out whether their disk is failing. That is not hypothetical: the
// session store and the broadcast log were counted site by site until
// 56 calls were found that were not.
// Through here, a call that returns an error is counted whoever makes it.
//
// **What is not a storage failure**, and the rule is about what the value
// is rather than which caller produced it:
//
//   - store.ErrFull is a channel at its size bound. The write did not
//     happen and nothing is wrong with the storage - the producer is
//     answered 0x97 and counted by saguin_publish_refused_total.
//   - store.ErrBelowFloor is a read from an offset retention has removed.
//     That is invariant 1 working, and it has a counter of its own.
//   - store.ErrNoSession, store.ErrNoShareGroup, store.ErrWindowFull and
//     store.ErrPastNext are the session store's and the broadcast log's
//     answers about what they hold - no session under that id, no member
//     left, a table at its client's Receive Maximum, an offset not yet
//     written - which their callers read as answers (writeDisconnect takes
//     ErrNoSession as nothing to record).
//
// Each is an outcome saguin promises. Counting them here would make a
// broker doing exactly what it said it would do look like one with a
// failing disk.
func storageFailure(err error) bool {
	return err != nil &&
		!errors.Is(err, store.ErrFull) &&
		!errors.Is(err, store.ErrBelowFloor) &&
		!errors.Is(err, store.ErrNoSession) &&
		!errors.Is(err, store.ErrNoShareGroup) &&
		!errors.Is(err, store.ErrWindowFull) &&
		!errors.Is(err, store.ErrPastNext)
}

// countingLog counts the storage failures of one append channel, against
// the provider that channel names.
//
// The embedded interface carries every method that cannot fail; only the
// ones that return an error are named here.
//
// **The embedding does not make an added method a compile error**, which
// this comment claimed for a while and which is worth stating the other way
// round so nobody relies on it again. A method added to one of these
// interfaces is promoted from the embedded value and satisfies it, so the
// wrapper still compiles and the new call is simply never counted. That is
// not hypothetical: `Get` was added to LatestStore, the tree built, and its
// storage failures went uncounted until somebody looked. Anything added
// here has to be named here too, by hand, and the thing that says so is
// TestEveryStoreMethodThatCanFailIsCounted: it reads these interfaces and
// fails unless every method whose last result is an error is declared on
// the wrapper standing in front of it.
type countingLog struct {
	LogStore
	on func(error)
}

func (c countingLog) Append(r store.Record) (store.Record, error) {
	out, err := c.LogStore.Append(r)
	c.on(err)
	return out, err
}

func (c countingLog) ReadFrom(offset uint64) ([]store.Record, error) {
	out, err := c.LogStore.ReadFrom(offset)
	c.on(err)
	return out, err
}

func (c countingLog) ReadFromN(offset uint64, max int) ([]store.Record, error) {
	out, err := c.LogStore.ReadFromN(offset, max)
	c.on(err)
	return out, err
}

func (c countingLog) FirstAtOrAfter(t time.Time) (uint64, bool, error) {
	off, ok, err := c.LogStore.FirstAtOrAfter(t)
	c.on(err)
	return off, ok, err
}

func (c countingLog) Trim(before time.Time, maxBytes int64) (int, int64, error) {
	removed, freed, err := c.LogStore.Trim(before, maxBytes)
	c.on(err)
	return removed, freed, err
}

func (c countingLog) Position(reader string) (store.Position, bool, error) {
	p, ok, err := c.LogStore.Position(reader)
	c.on(err)
	return p, ok, err
}

func (c countingLog) SavePosition(p store.Position) error {
	err := c.LogStore.SavePosition(p)
	c.on(err)
	return err
}

// A failure here answers /v1/operations/consumers with a 500 rather than a
// wrong list, and it is a storage failure like any other - the route being
// read by a person rather than by a scraper changes who sees the error, not
// whether it happened.
func (c countingLog) ListPositions(limit int) ([]store.Position, int, error) {
	rows, total, err := c.LogStore.ListPositions(limit)
	c.on(err)
	return rows, total, err
}

func (c countingLog) DropPosition(reader string) (bool, error) {
	had, err := c.LogStore.DropPosition(reader)
	c.on(err)
	return had, err
}

// countingDropper counts the storage failures of a provider-level
// operation - today, forgetting one reader's positions everywhere.
//
// **Counted against the provider rather than against a channel**, which is
// the one difference from the wrappers above and follows from what the
// operation is: it clears rows across every channel the provider holds, so
// there is no single channel a failure belongs to. Attributing it to one
// would put a provider's failure on whichever channel a map happened to
// yield first, which is a metric that moves when nothing did.
type countingDropper struct {
	ReaderDropper
	on func(error)
}

func (c countingDropper) DropReader(reader string) (int, error) {
	n, err := c.ReaderDropper.DropReader(reader)
	c.on(err)
	return n, err
}

// countingLatest is the same for a latest channel.
type countingLatest struct {
	LatestStore
	on func(error)
}

func (c countingLatest) Set(r store.Record) (store.Record, error) {
	out, err := c.LatestStore.Set(r)
	c.on(err)
	return out, err
}

func (c countingLatest) Delete(topic string) (bool, error) {
	had, err := c.LatestStore.Delete(topic)
	c.on(err)
	return had, err
}

func (c countingLatest) Get(topic string) (store.Record, bool, error) {
	out, found, err := c.LatestStore.Get(topic)
	c.on(err)
	return out, found, err
}

func (c countingLatest) Match(match func(topic string) bool) ([]store.Record, error) {
	out, err := c.LatestStore.Match(match)
	c.on(err)
	return out, err
}

func (c countingLatest) MatchWithDeletions(match func(topic string) bool) ([]store.Record, error) {
	out, err := c.LatestStore.MatchWithDeletions(match)
	c.on(err)
	return out, err
}

func (c countingLatest) Trim(before, deletionsBefore time.Time) (int, int64, error) {
	removed, freed, err := c.LatestStore.Trim(before, deletionsBefore)
	c.on(err)
	return removed, freed, err
}

func (c countingLatest) TrimExpired(now time.Time) (int, int64, error) {
	removed, freed, err := c.LatestStore.TrimExpired(now)
	c.on(err)
	return removed, freed, err
}

// countingQueue is the same for a queue.
type countingQueue struct {
	QueueStore
	on func(error)
}

func (c countingQueue) Enqueue(r store.Record) (store.Record, error) {
	out, err := c.QueueStore.Enqueue(r)
	c.on(err)
	return out, err
}

func (c countingQueue) Offer(max int, now time.Time) ([]store.Offered, error) {
	out, err := c.QueueStore.Offer(max, now)
	c.on(err)
	return out, err
}

func (c countingQueue) Lease(h store.Held, now time.Time, visibility time.Duration) (store.Item, bool, error) {
	it, ok, err := c.QueueStore.Lease(h, now, visibility)
	c.on(err)
	return it, ok, err
}

func (c countingQueue) Resolve(h store.Held) (store.Item, bool, error) {
	it, ok, err := c.QueueStore.Resolve(h)
	c.on(err)
	return it, ok, err
}

func (c countingQueue) Release(h store.Held, now time.Time, answered bool, maxAttempts int, dl store.DeadLetter) (store.Outcome, bool, error) {
	out, ok, err := c.QueueStore.Release(h, now, answered, maxAttempts, dl)
	c.on(err)
	return out, ok, err
}

func (c countingQueue) ExpiredLeases(now time.Time) ([]store.Held, error) {
	out, err := c.QueueStore.ExpiredLeases(now)
	c.on(err)
	return out, err
}

// The same for the queue view: a read that fails is a storage failure, and
// the counter is what an operator has instead of noticing a 500 in a log.
func (c countingQueue) Unresolved(limit int) ([]store.Item, int, error) {
	rows, total, err := c.QueueStore.Unresolved(limit)
	c.on(err)
	return rows, total, err
}

func (c countingQueue) ExpireOlderThan(before time.Time, dl store.DeadLetter) ([]store.Outcome, error) {
	out, err := c.QueueStore.ExpireOlderThan(before, dl)
	c.on(err)
	return out, err
}

// countStorage returns the function a wrapper calls, which resolves the
// provider from the channel - the error does not carry one, and the
// configuration is the only thing that knows.
func (b *Broker) countStorage(chName string) func(error) {
	provider := ""
	if c := b.reg.Get(chName); c != nil {
		provider = c.Storage
	}
	return b.countStorageOn(provider)
}

// countStorageOn is the same against a provider named directly, for an
// operation that is the provider's rather than one channel's.
func (b *Broker) countStorageOn(provider string) func(error) {
	return func(err error) {
		if storageFailure(err) {
			b.counted.storageError(provider)
		}
	}
}

// The wrappers stand in front of a store, so a type assertion that asks
// what a store *can do* - write inside another's transaction, export itself
// for a snapshot - sees the wrapper and not the store, and answers no.
//
// Every place that asks goes through the unwrapping below - the
// dead-letter move, the snapshot export of the channels and of the
// retained store, the retained store's quota, and the metrics' topic count
// - or asks the store before it is wrapped (the session store's quota and
// snapshot, the broadcast log's type). The snapshot is the dangerous one:
// its `!ok` is the ordinary path for a sqlite channel, which is not
// snapshotted, so a wrapped memory store asked directly would be dropped
// from every snapshot in silence - invariant 14's failure, arrived at from
// a direction that has nothing to do with storage. The retained store and
// the session store are wrapped whichever provider they are on, so this is
// not a sqlite-only concern.

// unwrapped is the store a wrapper stands in front of.
//
// **The dead-letter move needs the store itself and not a wrapper**, and
// that is a property of the move rather than an inconvenience: a record
// leaves the queue and arrives in its dead-letter channel in one
// transaction or neither happens (invariant 5), so the queue asks the
// channel whether it can be written inside the queue's own transaction. A
// wrapper cannot answer that - it has no such method to promote - and the
// move is refused with the record left exactly where it was.
//
// Nothing is lost by counting: the append happens inside Queue.Release,
// whose error the queue's own wrapper counts already. A storage failure in
// the dead-letter write reaches this broker as a failed Release.
type unwrapped interface{ inner() LogStore }

// The exactly-once holds (Holds), on each of the three.

func (c countingLog) Hold(e store.Exchange, r store.Record, now time.Time) error {
	err := c.LogStore.Hold(e, r, now)
	c.on(err)
	return err
}

func (c countingLog) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	r, held, err := c.LogStore.ReleaseHold(e)
	c.on(err)
	return r, held, err
}

func (c countingLog) DropHold(e store.Exchange) (bool, error) {
	gone, err := c.LogStore.DropHold(e)
	c.on(err)
	return gone, err
}

func (c countingLog) Holds() ([]store.HeldPublish, error) {
	held, err := c.LogStore.Holds()
	c.on(err)
	return held, err
}

func (c countingLatest) Hold(e store.Exchange, r store.Record, now time.Time) error {
	err := c.LatestStore.Hold(e, r, now)
	c.on(err)
	return err
}

func (c countingLatest) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	r, held, err := c.LatestStore.ReleaseHold(e)
	c.on(err)
	return r, held, err
}

func (c countingLatest) DropHold(e store.Exchange) (bool, error) {
	gone, err := c.LatestStore.DropHold(e)
	c.on(err)
	return gone, err
}

func (c countingLatest) Holds() ([]store.HeldPublish, error) {
	held, err := c.LatestStore.Holds()
	c.on(err)
	return held, err
}

func (c countingQueue) Hold(e store.Exchange, r store.Record, now time.Time) error {
	err := c.QueueStore.Hold(e, r, now)
	c.on(err)
	return err
}

func (c countingQueue) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	r, held, err := c.QueueStore.ReleaseHold(e)
	c.on(err)
	return r, held, err
}

func (c countingQueue) DropHold(e store.Exchange) (bool, error) {
	gone, err := c.QueueStore.DropHold(e)
	c.on(err)
	return gone, err
}

func (c countingQueue) Holds() ([]store.HeldPublish, error) {
	held, err := c.QueueStore.Holds()
	c.on(err)
	return held, err
}

func (c countingLog) inner() LogStore { return c.LogStore }

func (c countingLatest) innerLatest() LatestStore { return c.LatestStore }
func (c countingQueue) innerQueue() QueueStore    { return c.QueueStore }

// storeBehind returns the store a wrapper stands in front of, for the
// callers that need what the store itself can do rather than what the
// interface says (listed above unwrapped).
//
// A log is followed through every layer, including a wrapper a test put
// beneath the counting one (WrapLog) that says what it wraps with Unwrap.
func storeBehind(v any) any {
	for {
		switch w := v.(type) {
		case unwrapped:
			v = w.inner()
		case interface{ Unwrap() LogStore }:
			v = w.Unwrap()
		case interface{ innerLatest() LatestStore }:
			return w.innerLatest()
		case interface{ innerQueue() QueueStore }:
			return w.innerQueue()
		default:
			return v
		}
	}
}

// **The stores behind every session, and the broadcast log they own, are
// counted the same way**. Only the channel
// stores SetStores attaches used to be wrapped, so 56 of the broker's 137
// error-returning store calls - the session store's, the broadcast log's
// and its sessions', the retained store's - were counted only where
// somebody had remembered to by hand, and a failed Disconnected or a
// broadcast hold that could not be written reached no series at all. Each
// is now wrapped where it is attached (SetSessions, useBroadcastLog,
// SetRetained), and the hand-written counts it replaces are gone, so
// nothing is counted twice. The type questions asked of these stores - a
// quota, a snapshot export, the broadcast log's own type - are asked of the
// store itself, before it is wrapped or through storeBehind.

// countingSessions counts the storage failures of the session store.
type countingSessions struct {
	SessionStore
	on func(error)
}

func (c countingSessions) Save(s store.Session) error {
	err := c.SessionStore.Save(s)
	c.on(err)
	return err
}

func (c countingSessions) SaveWithShareCursors(sess store.Session, cursor uint64, groups []string) error {
	err := c.SessionStore.SaveWithShareCursors(sess, cursor, groups)
	c.on(err)
	return err
}

func (c countingSessions) Get(client string) (store.Session, bool, error) {
	s, ok, err := c.SessionStore.Get(client)
	c.on(err)
	return s, ok, err
}

func (c countingSessions) Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error {
	err := c.SessionStore.Disconnected(client, at, dropWill, expiry)
	c.on(err)
	return err
}

func (c countingSessions) All() ([]store.Session, error) {
	s, err := c.SessionStore.All()
	c.on(err)
	return s, err
}

func (c countingSessions) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	d, err := c.SessionStore.Begin(client, held, next)
	c.on(err)
	return d, err
}

func (c countingSessions) Drop(client string, groups []string) (store.Dropped, error) {
	d, err := c.SessionStore.Drop(client, groups)
	c.on(err)
	return d, err
}

func (c countingSessions) DropExpired(now time.Time, keep func(store.Session) bool) ([]string, store.Dropped, error) {
	ids, d, err := c.SessionStore.DropExpired(now, keep)
	c.on(err)
	return ids, d, err
}

// countingBroadcastLog counts the storage failures of the broadcast log.
type countingBroadcastLog struct {
	broadcastLog
	on func(error)
}

func (c countingBroadcastLog) Append(r store.Record) (store.Record, error) {
	out, err := c.broadcastLog.Append(r)
	c.on(err)
	return out, err
}

func (c countingBroadcastLog) ReadAt(offsets ...uint64) ([]store.Record, error) {
	out, err := c.broadcastLog.ReadAt(offsets...)
	c.on(err)
	return out, err
}

func (c countingBroadcastLog) ReadFromN(offset uint64, max int) ([]store.Record, error) {
	out, err := c.broadcastLog.ReadFromN(offset, max)
	c.on(err)
	return out, err
}

func (c countingBroadcastLog) Remove(offsets ...uint64) (int, int64, error) {
	n, freed, err := c.broadcastLog.Remove(offsets...)
	c.on(err)
	return n, freed, err
}

func (c countingBroadcastLog) Position(reader string) (store.Position, bool, error) {
	p, ok, err := c.broadcastLog.Position(reader)
	c.on(err)
	return p, ok, err
}

// countingHolds counts the storage failures of the exactly-once holds kept
// in the broadcast log (holdsFor). A channel's are counted by its own
// wrapper, which declares the same four.
type countingHolds struct {
	HoldStore
	on func(error)
}

func (c countingHolds) Hold(e store.Exchange, r store.Record, now time.Time) error {
	err := c.HoldStore.Hold(e, r, now)
	c.on(err)
	return err
}

func (c countingHolds) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	r, held, err := c.HoldStore.ReleaseHold(e)
	c.on(err)
	return r, held, err
}

func (c countingHolds) DropHold(e store.Exchange) (bool, error) {
	had, err := c.HoldStore.DropHold(e)
	c.on(err)
	return had, err
}

func (c countingHolds) Holds() ([]store.HeldPublish, error) {
	h, err := c.HoldStore.Holds()
	c.on(err)
	return h, err
}

// countingBroadcastSessions counts the storage failures of the session
// store's in-flight tables and shared-group cursors, as the drain asks them.
type countingBroadcastSessions struct {
	broadcastSessions
	on func(error)
}

func (c countingBroadcastSessions) All() ([]store.Session, error) {
	s, err := c.broadcastSessions.All()
	c.on(err)
	return s, err
}

func (c countingBroadcastSessions) InFlight(client string) (uint16, []store.InFlight, error) {
	w, fs, err := c.broadcastSessions.InFlight(client)
	c.on(err)
	return w, fs, err
}

func (c countingBroadcastSessions) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	err := c.broadcastSessions.SetInFlightAll(client, window, fs)
	c.on(err)
	return err
}

func (c countingBroadcastSessions) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	n, err := c.broadcastSessions.Acknowledge(client, cursor, done)
	c.on(err)
	return n, err
}

func (c countingBroadcastSessions) CreateShareCursor(group string, cursor uint64) error {
	err := c.broadcastSessions.CreateShareCursor(group, cursor)
	c.on(err)
	return err
}

func (c countingBroadcastSessions) SetShareCursor(group string, cursor uint64, forget ...uint64) error {
	err := c.broadcastSessions.SetShareCursor(group, cursor, forget...)
	c.on(err)
	return err
}

func (c countingBroadcastSessions) HandOver(group string, cursor uint64, client string, window uint16,
	fs []store.InFlight) error {
	err := c.broadcastSessions.HandOver(group, cursor, client, window, fs)
	c.on(err)
	return err
}

func (c countingBroadcastSessions) Return(client, group string, fs []store.InFlight) error {
	err := c.broadcastSessions.Return(client, group, fs)
	c.on(err)
	return err
}

func (c countingBroadcastSessions) Lend(group string, cursor uint64, offsets []uint64) error {
	err := c.broadcastSessions.Lend(group, cursor, offsets)
	c.on(err)
	return err
}

func (c countingBroadcastSessions) EndShareCursorIfUnheld(group string) (bool, error) {
	ended, err := c.broadcastSessions.EndShareCursorIfUnheld(group)
	c.on(err)
	return ended, err
}

func (c countingBroadcastSessions) ShareCursors() (map[string]uint64, error) {
	m, err := c.broadcastSessions.ShareCursors()
	c.on(err)
	return m, err
}

func (c countingBroadcastSessions) ShareReturned() (map[string][]uint64, error) {
	m, err := c.broadcastSessions.ShareReturned()
	c.on(err)
	return m, err
}

// CountStorageFailure counts one failure that the provider's own goroutines
// met, where no store call carried it: the flusher's fsync
// (sqlite.DB.SetFlushInterval). Same counter, same rule as every call
// through the wrappers above.
func (b *Broker) CountStorageFailure(provider string, err error) {
	b.countStorageOn(provider)(err)
}
