package broker

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// The broadcast drain: how a session that outlives its connection is served
// the QoS 1 and 2 broadcast it is owed from its provider's broadcast log (RFC
// 0003 "Broadcast").
//
// **Who owes a message is recorded when the message arrives, not worked out
// again when it is read.** Each durable session keeps a list of the offsets
// it is owed, in memory, and each message a count of the lists holding it.
// The count is set before any list is given the offset, so no session can let
// a message go before every session owed it has it; and a message leaves the
// log when the last list lets it go - acknowledged, expired, or its session
// ended (RFC 0003 "Broadcast").
//
// Working it out at read time instead - applying a session's filters to
// everything after its cursor - disagrees with the count the moment a lagging
// session changes its subscriptions: the drain then lets go of a message
// another session still owes, which is lost, or never lets go of one, which
// is kept for ever. It would also have a session with a narrow filter read
// every other session's messages to find its own, which is why the append
// pump grew its pending list.

// owedEntryCost is what one entry on a session's owed list costs against its
// limits.session_queue_bytes beside its message: the heap the entry takes on
// the list. Counted, so the list's own memory is inside the bound that covers
// the session, however small its messages (RFC 0002; invariant 13).
//
// **Measured, not the size of an offset.** An owedEntry is 56 bytes, and the
// list is a slice that keeps spare capacity as it grows: 57-70 bytes an entry
// at 1,000 to 60,000 entries (TestWhatAnOwedEntryCostsIsWhatItIsCharged).
// It was 8, the offset alone, and a 1MiB bound then let 1,000 away sessions
// owed 128-byte messages hold 418MiB of heap: 0.4MiB each, where the bound
// said the 136 bytes a message counts was all of it.
const owedEntryCost = 80

// wireEntryCost is what an entry costs besides while it is on the wire, or
// picked by a batch to go there: the in-flight PUBLISH the substrate holds
// for it (mqtt.InflightEntryOverhead), which the substrate does not count
// for a delivery the log bounds (packets.Packet.Bounded), and the drain's own
// flight and stored entries for its packet identifier, 110-176 bytes
// measured.
//
// **This is where a connected session that does not read spent its memory.**
// Charged 8 bytes, such a session's half of the bound on the wire was
// thousands of small deliveries at about a kilobyte each: a 1MiB bound let
// 1,000 of them owed 128-byte messages hold 1,214MiB of heap.
const wireEntryCost = mqtt.InflightEntryOverhead + 192

// wireTableCost is what a session's deliveries on the wire cost once, while
// it has any (owed.tableLocked): the client's in-flight table's own map and
// order (mqtt.InflightTableCost), which the table leaves to whoever counts
// its Bounded entries, and the first groups of the drain's flying and stored
// maps, which the per-entry figure above spreads over thousands.
// TestWhatABroadcastOnlyInflightTableCostsIsWhatItIsCharged holds it:
// measured over 2,000 sessions of 20-byte messages, 1,400 bytes a session
// with one on the wire, charged 923 before this.
const wireTableCost = mqtt.InflightTableCost + 300

// broadcastLog is what the drain asks of a session provider's broadcast log.
type broadcastLog interface {
	Append(store.Record) (store.Record, error)
	ReadAt(offsets ...uint64) ([]store.Record, error)
	ReadFromN(offset uint64, max int) ([]store.Record, error)
	Remove(offsets ...uint64) (int, int64, error)
	Position(reader string) (store.Position, bool, error)
	Next() uint64
	Floor() uint64
}

// broadcastSessions is what the drain asks of the session store that owns
// the log: its sessions, and each one's in-flight table and cursor.
type broadcastSessions interface {
	All() ([]store.Session, error)
	InFlight(client string) (uint16, []store.InFlight, error)
	SetInFlightAll(client string, window uint16, fs []store.InFlight) error
	Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error)

	// A shared group's cursor and returned list (RFC 0003 "Broadcast").
	CreateShareCursor(group string, cursor uint64) error
	SetShareCursor(group string, cursor uint64, forget ...uint64) error
	HandOver(group string, cursor uint64, client string, window uint16, fs []store.InFlight) error
	Return(client, group string, fs []store.InFlight) error
	Lend(group string, cursor uint64, offsets []uint64) error
	EndShareCursorIfUnheld(group string) (bool, error)
	ShareCursors() (map[string]uint64, error)
	ShareReturned() (map[string][]uint64, error)
	EndedAtOpen() map[string]store.ShareGroupState
}

// bdrain is the provider's broadcast log with every durable session's owed
// list on it.
//
// **Locks: bdrain.mu, and each owed.mu, never one inside the other**, and
// neither held with b.mu or a consumer's cmu, or across a store call. A
// count takes bdrain.mu to set the message's count and then each owner's
// lock in turn; a session letting go takes its own lock to take the entry
// off, and bdrain.mu after it has let go of that one.
type bdrain struct {
	b   *Broker
	log broadcastLog
	st  broadcastSessions
	// holds is the log's exactly-once holds, counted (holdsFor); nil for a
	// log that keeps none.
	holds HoldStore

	// sessions is every durable session with an owed list, by client id. A
	// sync.Map, because an acknowledgement looks its session up on its
	// client's read loop and must not queue behind a count.
	sessions sync.Map
	// groups is every shared group with a cursor on the log, by its
	// `$share/` filter: what it is owed and has not handed out, on an owed
	// list of its own (bgroup.go).
	groups sync.Map
	// gate puts a group's creation between a publish's two readings of who
	// has a group, never inside either, and between a cursor's ending in
	// the store and its list's: createGroups takes it exclusive, from
	// deciding which groups are missing to putting their lists in; wanted
	// and takeRecipients shared, around reads of groups alone; and every
	// write that may end a cursor (endUnheld, endStoredSession,
	// beginSession) shared, from before the write until the lists are gone.
	// Shared, because what they exclude is a creation and not one another:
	// sessions end and begin as clients connect, and taking it exclusive
	// queued every one of those writes, and every publish, behind each
	// other. **A SUBSCRIBE or CONNECT takes it under its client id's session
	// lock, and nothing holding it takes that lock.** Under it the store is
	// written and b.mu, gmu and a list's mu are each taken and let go, and
	// none of them is held by anything that takes it.
	gate sync.RWMutex
	// lmu guards lends: each group delivery in flight to a member whose
	// session the store does not keep, by client id and packet identifier.
	// It is taken under no other lock and takes none.
	lmu   sync.Mutex
	lends map[string]map[uint16]lendRef

	mu sync.Mutex
	// owners is how many lists hold each message the log keeps for them.
	owners map[uint64]uint32
	// counted is how far the counts have run: every offset up to through
	// is on every list it belongs on. A cursor never passes an offset
	// still being counted, or a stop in that moment has the session resume
	// past a message it was about to be given.
	counted watermark
	// sinces is, for each session, where the log was when each of its
	// subscriptions was made, by filter (store.SessionSubscription.Since).
	sinces map[string]map[string]uint64

	// pending is each publish appended (published) and not yet counted
	// (chosen), by offset: the one live selection chooses its recipients
	// after the append, on the same goroutine.
	pending sync.Map

	// Releases, written off every caller's path: an acknowledgement is read
	// on its client's read loop and a count runs on its publisher's, and
	// neither waits for a store's commit. rkick ends a release's wait to
	// gather (release).
	rmu      sync.Mutex
	rpending []uint64
	rrunning bool
	rkick    chan struct{}
	// rfreed is the bytes releases have given back, for a give-up that
	// waited for one (giveUp).
	rfreed atomic.Int64

	// gmu keeps two give-ups from choosing the same messages (giveUp). It
	// is taken with no other lock held but the gate, which a group's
	// creation holds across its store write (createGroups), and held across
	// the log's calls.
	gmu sync.Mutex
	// gave is what the full log has given up since it last said so.
	gave gaveUp
	// provider is the name of the provider the log is on, which is the one
	// whose other writers it gives way to (withRoom).
	provider string
}

// afterGiveUpAsked is a test seam, nil in production: when set it runs after
// each time the drain is asked to say what a full log gave up (sayGaveUp),
// whether or not a line was due, so a test can wait for the moment a line
// too many would have been written rather than sleeping past it.
var afterGiveUpAsked atomic.Pointer[func(b *Broker, now time.Time)]

// giveUpReportEvery is how often a full log says what it gave up: one line
// with the totals since the last, however many give-ups there were between,
// so it is loud without writing a line for every message. A var so a test
// need not wait the whole of it.
var giveUpReportEvery = 10 * time.Second

// gaveUp is what the full log has given up since it last said so, and when
// that was.
type gaveUp struct {
	mu         sync.Mutex
	messages   uint64
	deliveries uint64
	sessions   map[string]struct{}
	// writers is what the room was given up for: the log itself, or another
	// writer on its provider (withRoom).
	writers map[string]struct{}
	said    time.Time
}

// owed is one durable session's owed list.
type owed struct {
	d      *bdrain
	client string

	// fmu is held across every store write of the session's - a flush, and
	// what record writes - store call included, so a flush waits for one
	// already writing (storeBeforeDisconnectCloses), and the session's ending
	// waits for a write in flight (end). It is taken before mu and under no
	// other lock.
	fmu sync.Mutex
	mu  sync.Mutex
	// list is every message the session is owed and has not let go of, in
	// offset order, whether sent or not.
	list []owedEntry
	// unsent is how many of list are waiting for the wire.
	unsent int
	// flying is what is on the wire, by packet identifier.
	flying map[uint16]flight
	// stored is the store's in-flight table as this session last wrote it,
	// identifier to offset: what the next batch's room is measured against,
	// since the store refuses a table larger than the window.
	stored map[uint16]uint64
	// acked is what has been acknowledged and not yet written, and dirty
	// that the cursor has moved since it last was. unclaims free the packet
	// identifiers of entries taken back without an acknowledgement, and run
	// once the write has cleared what is keyed by them.
	acked    []store.InFlight
	unclaims []func()
	dirty    bool
	// flushed is when a flush last took what was acknowledged (flushDue).
	flushed time.Time
	// bytes is what the list comes to against limits.session_queue_bytes:
	// each entry its message's size and owedEntryCost. wire is the part of
	// it on the wire or picked to go, which is never given up (dropLocked),
	// and is held to half the bound (batch). table says bytes holds the
	// client's in-flight table's own cost as well (tableLocked).
	bytes, wire int64
	table       bool

	pumping, repump bool
	// settling is a flush of an away session's cursor on its way (settle).
	settling bool
	gone     bool

	// A group's list (bdrain.groups, bgroup.go): whether it is one, where its
	// round-robin choice of member stands, and the returned offsets it let
	// go of otherwise than by handing them over, which its next write of its
	// cursor forgets.
	isGroup bool
	rr      uint64
	forget  []uint64
}

type owedEntry struct {
	offset uint64
	charge int64
	// sent is on the wire. picked is taken by the batch writing it, which has
	// let o.mu go to read and record it: nothing gives it up meanwhile
	// (dropLocked), so what is counted lost is never then written.
	sent, picked bool
	// group, on a member's list, is the shared group that handed it over,
	// and "" for one of the session's own. The member's cursor passes over
	// it: the group's cursor passed it at the hand-over (cursorLocked).
	group string
	// returned, on a group's list, is an entry behind the group's cursor
	// that the group is owed again: returned by a member's ending, or lent
	// to a clean member. The group's cursor passes over it.
	returned bool
	// lent, on a group's list, is the clean member it is in flight to, and
	// nil for one that is not.
	lent *lent
}

// waiting reports whether the entry is neither on the wire nor picked to go:
// what the bound may give up.
func (e owedEntry) waiting() bool { return !e.sent && !e.picked }

// cost is what the entry counts against the bound now: its charge, and
// wireEntryCost while it is on the wire or picked to go. The owed list's
// bytes are the sum of its entries' costs, and its wire the sum over those
// not waiting, so every change of state moves them by the difference.
func (e owedEntry) cost() int64 {
	if e.waiting() {
		return e.charge
	}
	return e.charge + wireEntryCost
}

// flight is one delivery on the wire and the connection it was written on,
// which is the only one whose acknowledgement it takes: a client id is not a
// connection, and a successor's identifiers restart where its predecessor's
// did (see pending.conn).
type flight struct {
	entry store.InFlight
	conn  *mqtt.Client
	// again is the packet a resume put in conn's in-flight table for this
	// entry and the drain has still to write (resumed), or nil.
	again *packets.Packet
}

// newBroadcastDrain serves the owed lists on lg. Everything counted from here
// is at or after lg's next offset.
func newBroadcastDrain(b *Broker, lg broadcastLog, st broadcastSessions) *bdrain {
	d := &bdrain{b: b, log: lg, st: st, owners: map[uint64]uint32{}, sinces: map[string]map[string]uint64{},
		rkick: make(chan struct{}, 1)}
	d.counted.through.Store(lg.Next() - 1)
	return d
}

// attach gives a durable session an owed list, or answers the one it has.
func (d *bdrain) attach(client string) *owed {
	o := &owed{d: d, client: client, flying: map[uint16]flight{}, stored: map[uint16]uint64{}}
	v, _ := d.sessions.LoadOrStore(client, o)
	return v.(*owed)
}

func (d *bdrain) session(client string) *owed {
	if v, ok := d.sessions.Load(client); ok {
		return v.(*owed)
	}
	return nil
}

// count puts a message the log has just kept on the list of every session
// and shared group it is owed to, and wakes those that can take it now.
// recipients are the durable sessions the live broadcast's own selection
// chose - their filters, No Local and partition slice, at QoS 1 or more - and
// groups the shared groups with a cursor it took out of that selection
// (takeRecipients), so the rule for who is owed a message is written once. A
// message none of them is still here to take is let go at once.
func (d *bdrain) count(rec store.Record, recipients, groups []string) {
	charge := store.RecordSize(rec) + owedEntryCost
	owners := d.listsOf(recipients)
	glists := d.groupsOf(groups)

	// **The count first**, before any list has the offset, so a session
	// taking it and letting go of it at once cannot bring it to zero while
	// another owner still has it to come.
	d.mu.Lock()
	d.counted.seen(rec.Offset)
	if n := len(owners) + len(glists); n > 0 {
		d.owners[rec.Offset] = uint32(n)
	}
	d.mu.Unlock()
	if len(owners)+len(glists) == 0 {
		d.release([]uint64{rec.Offset})
	}

	bound := d.b.limits.SessionQueueBytes
	for _, o := range owners {
		o.mu.Lock()
		if o.gone {
			o.mu.Unlock()
			d.letGo(rec.Offset)
			continue
		}
		if !o.roomLocked(bound, charge) {
			o.mu.Unlock()
			d.queueFull(o, 1)
			d.letGo(rec.Offset)
			continue
		}
		o.insertLocked(owedEntry{offset: rec.Offset, charge: charge})
		given := o.boundLocked(bound, rec.Offset)
		o.mu.Unlock()
		d.queueFull(o, len(given))
		d.letGo(given...)
		if len(given) > 0 {
			d.settle(o)
		}
	}

	// **A group's list is bounded as a session's is**: what one group's
	// backlog may cost is limits.session_queue_bytes, and at it the oldest
	// goes (RFC 0002 `broker.share`), counted as the group's.
	for _, g := range glists {
		g.mu.Lock()
		if g.gone {
			g.mu.Unlock()
			d.letGo(rec.Offset)
			continue
		}
		if !g.roomLocked(bound, charge) {
			g.mu.Unlock()
			d.droppedHanded(&d.b.counted.sharesDroppedFull, 1)
			d.letGo(rec.Offset)
			continue
		}
		g.insertLocked(owedEntry{offset: rec.Offset, charge: charge})
		given := g.boundLocked(bound, rec.Offset)
		g.mu.Unlock()
		d.b.counted.sharesHeld.Add(1)
		if len(given) > 0 {
			d.b.counted.sharesDroppedFull.Add(uint64(len(given)))
			d.letGo(given...)
		}
	}

	// And only then may a cursor pass it.
	d.mu.Lock()
	d.counted.done(rec.Offset)
	d.mu.Unlock()

	for _, o := range owners {
		d.wake(o, nil)
	}
	for _, g := range glists {
		d.wakeGroup(g)
	}
}

// listsOf is the owed list of each durable session among recipients, once
// each.
func (d *bdrain) listsOf(recipients []string) []*owed {
	var lists []*owed
	seen := make(map[string]bool, len(recipients))
	for _, id := range recipients {
		if seen[id] {
			continue
		}
		seen[id] = true
		if o := d.session(id); o != nil {
			lists = append(lists, o)
		}
	}
	return lists
}

// keep appends a message to the log and counts it for the sessions given, as
// count does, making room where the log's provider is at its max_bytes (RFC
// 0002's full-store table): giveUp lets the log's oldest messages go, each
// counted against every session that was owed it.
//
// **A full log costs the sessions behind it, never the publisher** (RFC 0003
// "Broadcast"). Where nothing in the log can go, the message is not kept and
// is counted against each session it was for, and keep still answers with no
// error, so the publish is acknowledged as any other. An error is a store
// that failed for some other reason, which is the caller's to answer.
func (d *bdrain) keep(rec store.Record, recipients, groups []string) (store.Record, bool, error) {
	got, kept, err := d.appendRoom(rec)
	switch {
	case err != nil:
		return store.Record{}, false, err
	case kept:
		d.count(got, recipients, groups)
		return got, true, nil
	}
	d.lostToFull(recipients, groups)
	return store.Record{}, false, nil
}

// appendRoom is keep's append: it answers the record the log kept, or false
// where the log is full and nothing in it could go.
func (d *bdrain) appendRoom(rec store.Record) (store.Record, bool, error) {
	for {
		got, err := d.log.Append(rec)
		if err == nil {
			return got, true, nil
		}
		if !errors.Is(err, store.ErrFull) {
			return store.Record{}, false, err
		}
		// Again after each give-up rather than once: a sqlite provider frees
		// room by the page, so the bytes one message comes to may not be room
		// for it yet. It ends when the log has nothing left it may give.
		if d.giveUp(store.RecordSize(rec), theLog) == 0 {
			return store.Record{}, false, nil
		}
	}
}

// withRoom runs write, a write to a store on provider, and where it finds
// the provider full and the broadcast log is on it, has the log give way
// (RFC 0002 "Every session's state"): giveUp lets the log's oldest messages
// go, at least need bytes of them, and write runs again, until it has room or
// the log has nothing left it may give. It answers what write last answered,
// which with nothing left to give is what it would have answered with no log.
// writer is what the warning says the room went to.
//
// **write must be all or nothing**: ErrFull has to mean it changed nothing,
// or running it again applies part of it twice. Every site wrapped is one
// whose store refuses before it writes.
//
// It is broker code around a whole store call, so no store's lock is held;
// and it is never reached under b.mu, a client's cmu, or the drain's mu or
// gmu (TestAGiveUpIsReachedUnderNoLockItCouldWaitOn). A list's fmu may be
// held - a session's record, and a group's hand-over to a member, write under
// it - and so may a connection's read lock, which the hand-over takes first:
// giveUp takes gmu and each list's mu, and never an fmu or a connection's
// lock. The engine's per-id session lock may be held, as it is for a
// CONNECT's writes: giveUp takes no session lock and fans nothing out, so
// nothing it waits for waits on one.
func (b *Broker) withRoom(provider string, need int64, writer string, write func() error) error {
	err := write()
	if !errors.Is(err, store.ErrProviderFull) {
		return err
	}
	d := b.broadcastDrain()
	if d == nil || d.provider == "" || provider != d.provider {
		return err
	}
	// Only the provider's own refusal: a store at its own bound - a channel's
	// max_bytes - is not helped by room in the log, and asking would give the
	// log up to no end.
	for errors.Is(err, store.ErrProviderFull) && d.giveUp(max(need, minGiveUp), writer) > 0 {
		err = write()
	}
	return err
}

// saveSession keeps a session's record, the broadcast log giving way where
// the provider they share is full (withRoom). Save replaces the whole record
// or nothing, so running it again cannot apply it twice.
func (b *Broker) saveSession(s SessionStore, sess store.Session) error {
	// A record owed to the store goes back before anything is built on the
	// one the store holds (settleRecord).
	if err := b.settleRecord(s, sess.Client); err != nil {
		return err
	}
	b.mu.Lock()
	provider := b.sessionsProvider
	b.mu.Unlock()
	return b.withRoom(provider, store.SessionSize(sess), "the session store", func() error { return s.Save(sess) })
}

// minGiveUp is the least withRoom asks the log for, where the write's own
// size is not known: about a page, which is what a sqlite provider frees by.
const minGiveUp = 4096

// theLog is the writer giveUp names when the room is for the log's own next
// message.
const theLog = "the broadcast log"

// lostToFull counts a message the full log could not keep against each
// session and group it was for.
func (d *bdrain) lostToFull(recipients, groups []string) {
	if n := len(d.groupsOf(groups)); n > 0 {
		d.droppedHanded(&d.b.counted.sharesDroppedStorageFull, n)
	}
	if lists := d.listsOf(recipients); len(lists) > 0 {
		d.b.counted.deliveriesStorageFull.Add(uint64(len(lists)))
		ids := make([]string, len(lists))
		for i, o := range lists {
			ids[i] = o.client
		}
		d.noteGaveUp(1, uint64(len(lists)), ids, theLog)
	}
}

// logNotKept is a publish's LogOffset where the log was full and nothing in
// it could go: the message was not kept, and each session the selection
// chooses for it loses it (lostToFull).
const logNotKept = ^uint64(0)

// published is keep for a publish, whose recipients are not known yet: it
// appends the message and answers where it went, or logNotKept, and the
// selection counts it for the sessions it chooses (chosen).
//
// **Appended before the recipients are chosen, and never left uncounted.**
// An offset nobody has counted holds every session's stored cursor behind
// it (counted), so each one published is counted by the selection the same
// publish runs next, on the same goroutine.
func (d *bdrain) published(rec store.Record) (uint64, error) {
	got, kept, err := d.appendRoom(rec)
	if err != nil {
		return 0, err
	}
	if !kept {
		return logNotKept, nil
	}
	d.pending.Store(got.Offset, got)
	return got.Offset, nil
}

// chosen counts a published message for the sessions and groups the live
// selection chose for it, or, where the log could not keep it, counts it lost
// to each.
func (d *bdrain) chosen(off uint64, recipients, groups []string) {
	if off == logNotKept {
		d.lostToFull(recipients, groups)
		return
	}
	if v, ok := d.pending.LoadAndDelete(off); ok {
		d.count(v.(store.Record), recipients, groups)
	}
}

// wanted reports whether a session this drain holds has a subscription that
// takes pk at QoS 1 or more, or a shared group with a cursor matches it, as
// the engine's topic index has them: a publish nobody here is owed is not
// written to the log at all.
//
// It walks the index without building a selection (AnySubscriber): the
// selection is built once, for the fan-out, and building it here as well
// was a second set of maps for every publish at QoS 1 or 2. Asking each
// matching subscription whether it is at QoS 1 or more is asking whether
// the client's highest is, which is what the merged selection held.
//
// **Under the gate**, so a group being created is not seen until its cursor
// is kept and from then on is (createGroups).
func (d *bdrain) wanted(pk packets.Packet) bool {
	if d.b.srv == nil || pk.FixedHeader.Qos == 0 {
		return false
	}
	d.gate.RLock()
	defer d.gate.RUnlock()
	return d.b.srv.Topics.AnySubscriber(pk.TopicName,
		func(id string, sub packets.Subscription) bool { return sub.Qos > 0 && d.session(id) != nil },
		func(filter string) bool { return d.group(filter) != nil })
}

// takeRecipients takes out of a publish's selection every subscriber this
// drain holds that it reaches at QoS 1 or more, and answers those it is owed
// to: they are served from the log by their drains, so the engine's fan-out
// writes nothing to them. One whose No Local subscription excludes its own
// publish is taken out and owed nothing, as the fan-out would skip it.
//
// **And every shared group with a cursor, at QoS 1 or 2**, answered as the
// groups it is owed to: the group's drain hands it to one member, in the
// order the log holds it (bgroup.go). One at QoS 0 is owed to nobody and
// stays in the selection, which serves it live; MQTT orders deliveries only
// within one topic and QoS (MQTT-4.6.0-6), so it may overtake the backlog.
// The groups are read under the gate, as wanted reads them.
func (d *bdrain) takeRecipients(subs *mqtt.Subscribers, pk packets.Packet) (ids, groups []string) {
	for id, sub := range subs.Subscriptions {
		if min(pk.FixedHeader.Qos, sub.Qos) == 0 || d.session(id) == nil {
			continue
		}
		delete(subs.Subscriptions, id)
		if sub.NoLocal && pk.Origin == id {
			continue
		}
		ids = append(ids, id)
	}
	if pk.FixedHeader.Qos == 0 {
		return ids, nil
	}
	d.gate.RLock()
	defer d.gate.RUnlock()
	for filter := range subs.Shared {
		if d.group(filter) == nil {
			continue
		}
		delete(subs.Shared, filter)
		groups = append(groups, filter)
	}
	return ids, groups
}

// keepRetained keeps in the log, owed to client alone (store.Record.OwedTo),
// each retained value of values that goes out at QoS 1 or 2, and answers the
// rest, which are sent as a retained value always was. The session's drain
// sends what it keeps, as any message it is owed: under a packet identifier,
// and again after a restart until it is acknowledged.
//
// A full log with nothing it can give up loses one for the session, counted
// storage_full (lostToFull); a store that fails otherwise leaves it with the
// rest, sent and kept nowhere, as before.
func (d *bdrain) keepRetained(client string, values []pendingValue) []pendingValue {
	rest := values[:0]
	for _, v := range values {
		if min(v.rec.QoS, v.qos) == 0 {
			rest = append(rest, v)
			continue
		}
		rec := v.rec
		rec.Offset, rec.ForGroups, rec.OwedTo = 0, false, client
		if _, _, err := d.keep(rec, []string{client}, nil); err != nil {
			d.b.log.Error("cannot keep a retained value for a session in the broadcast log; it is sent without it",
				"client", d.b.limits.Loggable(client), "topic", d.b.limits.Loggable(rec.Topic), "error", err)
			rest = append(rest, v)
		}
	}
	return rest
}

// giveUpBatch is how many of the log's oldest messages giveUp reads at a time.
const giveUpBatch = 64

// giveUp makes room in the log's provider by letting its oldest messages go
// before the sessions owed them have had them, and answers the bytes it
// freed: at least need where the log had that much it could give, and zero
// where it had nothing (RFC 0002's full-store table). It gives up the fewest
// that make room.
//
// **A message a session has on the wire stays**, and so does its place on
// every other session's list. It is in that session's in-flight table, sent
// again under its identifier on a resume, so it is the session's to finish;
// and taking it from the other lists alone would lose it for them and free
// nothing. Which messages are on the wire, or picked by a batch to go there,
// is asked of every list first, and only the rest are taken off them
// (dropLocked). One picked in between stays on that session's list: the
// others lose it, counted, and its room comes back when it is acknowledged.
//
// Each session is counted once for each entry taken off its list, under
// storage_full. Only messages at or below the watermark are looked at, so a
// message between its append and its count is never given up.
func (d *bdrain) giveUp(need int64, writer string) int64 {
	released := d.rfreed.Load()
	d.gmu.Lock()
	defer d.gmu.Unlock()
	// What a release removed while this waited for it is room made, and
	// counts before anything a session is owed is looked at.
	freed := d.rfreed.Load() - released
	if freed >= need {
		return freed
	}
	// **What releases are gathering goes first**: nobody owes it, and a
	// session owed a message in front of it would otherwise lose that one
	// to make room these were about to give back (release).
	if gathered := d.takeReleases(); len(gathered) > 0 {
		_, f, err := d.log.Remove(gathered...)
		if err != nil {
			d.b.log.Error("cannot remove broadcast messages nobody owes from the log",
				"messages", len(gathered), "error", err)
		}
		freed += f
	}
	next := d.log.Floor()
	for freed < need {
		through := d.counted.through.Load()
		next = max(next, d.log.Floor())
		if next > through {
			break
		}
		recs, err := d.log.ReadFromN(next, giveUpBatch)
		if err != nil {
			if errors.Is(err, store.ErrBelowFloor) {
				continue // the front moved on meanwhile: read from where it is
			}
			d.b.log.Error("cannot read the broadcast log to make room in it", "error", err)
			break
		}
		// The oldest, as many as come to what is still needed.
		var cands []uint64
		var size int64
		for _, r := range recs {
			if r.Offset > through || size >= need-freed {
				break
			}
			cands = append(cands, r.Offset)
			size += store.RecordSize(r)
		}
		if len(cands) == 0 {
			break
		}
		next = cands[len(cands)-1] + 1
		freed += d.giveUpOf(cands, writer)
	}
	return freed
}

// giveUpOf takes the messages at cands off every list that owes them, unless
// a session has one on the wire, and out of the log where no list still
// holds it, for writer. It answers the bytes the log freed.
func (d *bdrain) giveUpOf(cands []uint64, writer string) int64 {
	wire := map[uint64]bool{}
	onWire := func(_, v any) bool {
		o := v.(*owed)
		o.mu.Lock()
		for _, off := range cands {
			if o.onWireLocked(off) {
				wire[off] = true
			}
		}
		o.mu.Unlock()
		return true
	}
	d.sessions.Range(onWire)
	d.groups.Range(onWire)
	free := make([]uint64, 0, len(cands))
	for _, off := range cands {
		if !wire[off] {
			free = append(free, off)
		}
	}
	if len(free) == 0 {
		return 0
	}

	taken := map[uint64]uint32{}
	var lost, grouped uint64
	var losers []string
	// A group's losses are the group's (saguin_shares_dropped_total), a
	// session's its own (saguin_session_deliveries_dropped_total).
	drop := func(_, v any) bool {
		o := v.(*owed)
		o.mu.Lock()
		offs := o.dropLocked(free)
		o.mu.Unlock()
		for _, off := range offs {
			taken[off]++
		}
		if len(offs) > 0 {
			if o.isGroup {
				grouped += uint64(len(offs))
			} else {
				lost += uint64(len(offs))
			}
			losers = append(losers, o.client)
		}
		return true
	}
	d.sessions.Range(drop)
	d.groups.Range(drop)
	if lost > 0 {
		d.b.counted.deliveriesStorageFull.Add(lost)
	}
	if grouped > 0 {
		d.b.counted.sharesDroppedStorageFull.Add(grouped)
	}
	if lost+grouped > 0 {
		d.noteGaveUp(uint64(len(taken)), lost+grouped, losers, writer)
	}

	// Out of the log now, not on the release goroutine: the room is what the
	// caller is waiting for.
	var gone []uint64
	d.mu.Lock()
	for _, off := range free {
		n, owed := d.owners[off]
		if owed && taken[off] < n {
			d.owners[off] = n - taken[off]
			continue
		}
		delete(d.owners, off)
		gone = append(gone, off)
	}
	d.mu.Unlock()
	if len(gone) == 0 {
		return 0
	}
	_, freed, err := d.log.Remove(gone...)
	if err != nil {
		// Owed to nobody, and kept until the start that counts the log again
		// finds nobody owes it.
		d.b.log.Error("cannot remove broadcast messages given up to make room", "messages", len(gone),
			"error", err)
		return 0
	}
	return freed
}

// noteGaveUp adds what one give-up cost to what the log has still to say.
// The broker's tick says it (sayGaveUp), and not the give-up: that runs on a
// publisher's path, under whatever the caller holds.
func (d *bdrain) noteGaveUp(messages, deliveries uint64, sessions []string, writer string) {
	g := &d.gave
	g.mu.Lock()
	g.messages += messages
	g.deliveries += deliveries
	if g.sessions == nil {
		g.sessions, g.writers = map[string]struct{}{}, map[string]struct{}{}
	}
	g.writers[writer] = struct{}{}
	for _, id := range sessions {
		g.sessions[id] = struct{}{}
	}
	g.mu.Unlock()
}

// sayGaveUp warns of what the full log gave up since it last did, if it has
// given up anything and giveUpReportEvery has passed: how many messages,
// how many sessions lost them, and what to do about it (RFC 0002's
// full-store table). Messages sessions were owed and will never be sent is
// a loss an operator has to hear about, and the counter alone is heard only
// by whoever is watching it. Called from the broker's tick, holding nothing.
func (d *bdrain) sayGaveUp(now time.Time) {
	if f := afterGiveUpAsked.Load(); f != nil {
		defer (*f)(d.b, now)
	}
	g := &d.gave
	g.mu.Lock()
	if g.deliveries == 0 || now.Sub(g.said) < giveUpReportEvery {
		g.mu.Unlock()
		return
	}
	messageCount, deliveryCount, sessionCount := g.messages, g.deliveries, len(g.sessions)
	provider := d.provider
	roomFor := strings.Join(slices.Sorted(maps.Keys(g.writers)), ", ")
	g.messages, g.deliveries, g.sessions, g.writers, g.said = 0, 0, nil, nil, now
	g.mu.Unlock()

	d.b.log.Warn("the broadcast log is full: durable sessions lost messages they were owed",
		"messages", messageCount, "sessions", sessionCount, "deliveries", deliveryCount,
		"provider", provider, "to_make_room_for", roomFor,
		"remedy", "give broker.session.storage a provider of its own, or raise its max_bytes")
}

// end lets go of everything a session was owed, whichever way it ended, but
// what its shared groups had handed it, which it answers: the store's ending
// splits those (store.Dropped), and ended settles them from its answer. Until
// then they are let go of by nobody, so the log keeps them. The store's own
// ending takes its in-flight table and its cursor with its record.
func (d *bdrain) end(client string) []handed {
	d.mu.Lock()
	delete(d.sinces, client)
	d.mu.Unlock()
	v, ok := d.sessions.LoadAndDelete(client)
	if !ok {
		return nil
	}
	o := v.(*owed)
	// **After any store write of the session's already under way, and before
	// any other**: every one is made under fmu and only while the session
	// has not ended (flush, record). So one made before this lands before the
	// store's own ending of the session (endStoredSession), which takes it
	// out, and none is made after it - where a clean start's store writes the
	// successor's record, and a late write landed in the successor's table.
	o.fmu.Lock()
	o.mu.Lock()
	o.gone = true
	offs := make([]uint64, 0, len(o.list))
	charges := map[uint64]int64{}
	for _, e := range o.list {
		if e.group != "" {
			charges[e.offset] = e.charge
			continue
		}
		offs = append(offs, e.offset)
	}
	var hs []handed
	for _, f := range o.flying {
		if f.entry.Group != "" {
			hs = append(hs, handed{f: f.entry, charge: charges[f.entry.Offset]})
		}
	}
	unclaims := o.unclaims
	o.list, o.unsent, o.bytes, o.wire, o.table = nil, 0, 0, 0, false
	o.flying, o.stored, o.acked, o.unclaims = map[uint16]flight{}, map[uint16]uint64{}, nil, nil
	o.mu.Unlock()
	o.fmu.Unlock()
	// The store's ending takes the rows keyed by them.
	for _, f := range unclaims {
		f()
	}
	d.letGo(offs...)
	return hs
}

// since answers, for each filter a session holds, where the log was when the
// subscription was made, and records it now for one the session did not hold
// before - which is what makes it a new subscription (MQTT-3.8.4). A filter
// the session no longer holds is forgotten, so subscribing to it again is new
// again.
//
// **The next offset not yet counted, not the log's next.** A message stored
// before the subscription and counted after it may have been counted for it
// (bdrain.count), and must not be left behind at a start because its offset
// is below Since. So Since errs early: a start may count a message for a
// subscription made in the moment it was being published, and never misses
// one the running broker counted.
func (d *bdrain) since(client string, filters []string) map[string]uint64 {
	boundary := d.counted.through.Load() + 1
	d.mu.Lock()
	defer d.mu.Unlock()
	had := d.sinces[client]
	now := make(map[string]uint64, len(filters))
	for _, f := range filters {
		if v, ok := had[f]; ok {
			now[f] = v
			continue
		}
		now[f] = boundary
	}
	d.sinces[client] = now
	return now
}

// keepSinces answers what puts a client's record of where its filters were
// made back as it is now, for a write that changed it and was refused.
func (d *bdrain) keepSinces(client string) (restore func()) {
	d.mu.Lock()
	had, ok := d.sinces[client]
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if ok {
			d.sinces[client] = had
		} else {
			delete(d.sinces, client)
		}
	}
}

// acked takes an acknowledgement for a delivery this drain made - a PUBACK,
// or a QoS 2 exchange's PUBCOMP - or its exchange ending without one, and
// reports whether it was one. It runs on the client's read loop, so it
// changes memory and nothing else: the store is written by the session's
// drain, woken here.
//
// **The engine frees the packet identifier as the acknowledgement is taken**,
// once this returns (processPuback), before the drain has written it out of
// the store's table. That is safe because the table is cleared by identifier and offset
// together (Sessions.Acknowledge): a delivery made under the identifier
// meanwhile has its own offset, so its row is not the one cleared.
func (d *bdrain) acked(cl *mqtt.Client, id uint16) bool {
	o := d.session(cl.ID)
	if o == nil {
		return d.lentAcked(cl, id)
	}
	o.mu.Lock()
	f, ok := o.flying[id]
	if !ok || o.gone || !d.takes(f, cl) {
		o.mu.Unlock()
		return d.lentAcked(cl, id)
	}
	delete(o.flying, id)
	o.removeLocked(f.entry.Offset)
	o.acked = append(o.acked, f.entry)
	o.dirty = true
	o.mu.Unlock()
	d.letGo(f.entry.Offset)
	d.wake(o, cl)
	// Room in its window is room for its groups too.
	d.wakeGroupsOf(cl)
	return true
}

// received records a QoS 2 delivery's PUBREC, and reports whether it was one
// of this drain's: its exchange moves on, and what the session owes is the
// PUBREL (MQTT-4.3.3-4). The cursor does not move - the entry is still on the
// wire, and a start drops an entry behind its cursor - so the message stays
// owed until the PUBCOMP (acked).
//
// **Written before the PUBREL goes out, and waited for**, as the engine waits
// on OnDeliveryReleased before it sends it. The client answers the PUBREL with
// a PUBCOMP and forgets the identifier; an entry still saying Sent after a
// crash would then have the PUBLISH sent again under an identifier the client
// has finished with, which it takes as a new message - one QoS 2 message
// delivered twice. It runs on the client's read loop, which is the one wait
// here, and it is the one the engine already takes for every kept delivery.
//
// **A write that fails is answered, and the PUBREL is not sent.** The entry
// goes back to Sent, as the store still has it, and the engine ends the
// connection with the PUBLISH still in its table, to be sent again when the
// session resumes. A session the store no longer holds is not that case: it
// has nothing a restart could send again, so its PUBREL goes.
func (d *bdrain) received(cl *mqtt.Client, pk packets.Packet) (bool, error) {
	o := d.session(cl.ID)
	if o == nil {
		// A clean member's PUBREC for a delivery its group lent it: nothing
		// is written, its session ending with its connection.
		return d.lentReceived(cl, pk.PacketID), nil
	}
	o.mu.Lock()
	f, ok := o.flying[pk.PacketID]
	if !ok || o.gone || !d.takes(f, cl) || f.entry.QoS != 2 {
		o.mu.Unlock()
		return false, nil
	}
	was := f
	f.entry.State = store.MessageReleased
	f.conn = cl
	o.flying[pk.PacketID] = f
	// The table may hold more than the client's window now, where it resumed
	// declaring a smaller one: a state change adds no entry, so it is written
	// under whichever is larger.
	window := max(receiveMaximum(cl), uint16(len(o.stored)))
	o.mu.Unlock()
	err := d.record(o, window, []store.InFlight{f.entry})
	if err == nil || errors.Is(err, store.ErrNoSession) {
		return true, nil
	}
	d.b.log.Error("cannot record that a session's broadcast was received; its PUBREL is not sent",
		"client", d.b.limits.Loggable(o.client), "error", err)
	o.mu.Lock()
	if now, ok := o.flying[pk.PacketID]; ok && now == f {
		o.flying[pk.PacketID] = was
	}
	o.mu.Unlock()
	return true, err
}

// takes reports whether cl may settle a delivery on the wire: the connection
// it was written on, or the one its session has since resumed on.
//
// **A resume moves the exchange to the new connection.** After a link cut the
// engine hands the taken-over connection's in-flight table to the new one and
// re-sends it there under the same identifiers (MQTT-4.4.0-1), so what comes
// back is the new connection's acknowledgement of a delivery written on the
// old. The connection now registered for the client id is the one that may
// give it; any other is a connection whose session has moved on, whose
// packets change nothing (invariant 17).
func (d *bdrain) takes(f flight, cl *mqtt.Client) bool {
	return d.b.acksFor(f.conn, cl)
}

// resumed serves a session its client has just resumed (Clean Start 0): what
// was on the wire is sent again first, under the identifiers it went out with
// (MQTT-4.4.0-1), and then the rest of what the session is owed.
//
// After a link cut the engine has done the first half: it handed the
// taken-over connection's in-flight table to this one and sent it again
// (ResendInflightMessages). After a start there is no such table - what was
// on the wire is in the store's (rebuild), on no connection - so each entry
// goes into this connection's table here, under its own identifier: a
// PUBLISH with DUP set, or the PUBREL an exchange answered with a PUBREC is
// owed. The drain then writes them (writeAgain).
//
// **In the engine's table before anything else can send**, which is why it is
// done here, on the connection's own goroutine as it is established, and not
// on the drain's: an identifier in that table is one NextPacketID does not
// give out, and one given to another delivery first would put two messages on
// the wire under it.
//
// **Written by the drain, not left to the engine's withheld deliveries.** The
// engine reads a withheld PUBLISH with no payload as one whose payload the
// session store keeps, finds none, and leaves it unwritten for good - and an
// empty broadcast is one MQTT allows.
func (d *bdrain) resumed(cl *mqtt.Client) {
	o := d.session(cl.ID)
	if o == nil {
		return
	}
	o.mu.Lock()
	var due []store.InFlight
	for _, f := range o.flying {
		if f.conn == nil {
			due = append(due, f.entry)
		}
	}
	o.mu.Unlock()
	if len(due) > 0 {
		d.putBack(cl, o, due)
	}
	d.wake(o, cl)
	d.wakeGroupsOf(cl)
}

// putBack is resumed's second half, for the entries a start put back.
func (d *bdrain) putBack(cl *mqtt.Client, o *owed, due []store.InFlight) {
	var offs []uint64
	for _, f := range due {
		if f.State == store.MessageSent {
			offs = append(offs, f.Offset)
		}
	}
	recs, err := d.log.ReadAt(offs...)
	if err != nil {
		// Left on no connection, so the next resume tries again.
		d.b.log.Error("cannot read the broadcast log; this session's messages on the wire are not sent again",
			"client", d.b.limits.Loggable(o.client), "error", err)
		return
	}
	held := make(map[uint64]store.Record, len(recs))
	for _, r := range recs {
		held[r.Offset] = r
	}
	now := time.Now()
	subs := cl.State.Subscriptions.GetAll()
	legacy := legacyClient(cl)

	type put struct {
		f  store.InFlight
		pk packets.Packet
	}
	var (
		puts []put
		lost []store.InFlight
	)
	o.mu.Lock()
	for _, f := range due {
		cur, ok := o.flying[f.PacketID]
		if !ok || cur.conn != nil || cur.entry != f || o.gone {
			continue
		}
		var pk packets.Packet
		if f.State == store.MessageReleased {
			pk = packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: f.PacketID}
		} else {
			r, ok := held[f.Offset]
			if !ok {
				// Gone from the log since the start: nothing is left to send
				// under it.
				lost = append(lost, f)
				continue
			}
			opt := optionsOf(subs, f.Group, r.Topic)
			pk = broadcastPacket(r, f.QoS, opt, legacy, now)
			pk.PacketID = f.PacketID
			pk.FixedHeader.Dup = true // [MQTT-3.3.1-1]
		}
		puts = append(puts, put{f, pk})
	}
	for _, f := range lost {
		delete(o.flying, f.PacketID)
		o.removeLocked(f.Offset)
		o.acked = append(o.acked, f)
	}
	o.mu.Unlock()
	// A message the session had on the wire and the log gave up while the
	// broker was stopped is one it lost to the log's bound, as one given up
	// while it runs is (RFC 0002's full-store table).
	if len(lost) > 0 {
		d.b.counted.deliveriesStorageFull.Add(uint64(len(lost)))
	}
	d.letGo(offsetsOf(lost)...)

	// **Registered while cl still has the session** (WhileOwned), outside
	// o.mu, as batch's are: a connection taken over before its turn leaves
	// the rest on no connection, for the resume that took it.
	for _, p := range puts {
		pk := p.pk
		if !cl.WhileOwned(func() { cl.State.Inflight.Set(pk) }) {
			return
		}
		o.mu.Lock()
		if cur, ok := o.flying[p.f.PacketID]; ok && cur.conn == nil && cur.entry == p.f && !o.gone {
			cur.conn, cur.again = cl, &pk
			o.flying[p.f.PacketID] = cur
		}
		o.mu.Unlock()
	}
}

// writeAgain writes what a resume put back into cl's in-flight table, in the
// order it was first sent, as far as the client's send quota allows - so a
// client that resumed declaring a smaller Receive Maximum is sent what fits,
// and the rest as its acknowledgements free room, before anything new. It
// reports whether none is left to write.
func (d *bdrain) writeAgain(cl *mqtt.Client, o *owed) bool {
	o.mu.Lock()
	var due []flight
	for _, f := range o.flying {
		if f.again != nil && f.conn == cl {
			due = append(due, f)
		}
	}
	o.mu.Unlock()
	if len(due) == 0 {
		return true
	}
	slices.SortFunc(due, func(a, b flight) int {
		switch {
		case a.entry.Offset < b.entry.Offset:
			return -1
		case a.entry.Offset > b.entry.Offset:
			return 1
		}
		return 0
	})
	for _, f := range due {
		// The engine's rule, which keeps no quota for a client that stated
		// no Receive Maximum: read as a quota of none, such a session was
		// never sent again what it had on the wire, as no acknowledgement
		// was coming to call this back.
		if !cl.State.Inflight.HasSendQuota() {
			return false // the next acknowledgement calls this back
		}
		o.mu.Lock()
		cur, ok := o.flying[f.entry.PacketID]
		if !ok || cur.again == nil || cur.conn != cl {
			o.mu.Unlock()
			continue
		}
		pk := *cur.again
		cur.again = nil
		o.flying[f.entry.PacketID] = cur
		o.mu.Unlock()
		cl.State.Inflight.DecreaseSendQuota()
		if err := d.b.writeTo(cl, pk); err != nil {
			// The connection is going. What it held goes to the next one with
			// its in-flight table, which the engine sends again there.
			return false
		}
	}
	return true
}

func offsetsOf(fs []store.InFlight) []uint64 {
	out := make([]uint64, 0, len(fs))
	for _, f := range fs {
		out = append(out, f.Offset)
	}
	return out
}

// receiveMaximum is the client's Receive Maximum: how many QoS 1 and 2
// deliveries it takes unacknowledged.
func receiveMaximum(cl *mqtt.Client) uint16 {
	if rm := cl.Properties.Props.ReceiveMaximum; rm > 0 {
		return rm
	}
	return 65535 // section 3.1.2.11.3: a Receive Maximum not given is 65,535
}

// freed wakes a session's drain when something else's acknowledgement has
// freed room in the window it shares, and it has something waiting for it,
// and the drain of every shared group the client is a member of.
func (d *bdrain) freed(cl *mqtt.Client) {
	d.wakeGroupsOf(cl)
	o := d.session(cl.ID)
	if o == nil {
		return
	}
	o.mu.Lock()
	waiting := o.unsent > 0 && !o.gone
	o.mu.Unlock()
	if waiting {
		d.wake(o, cl)
	}
}

// wake starts a session's drain on a goroutine of its own, or asks the one
// running to look again: one drain per session, so its messages go out in
// the order its list holds them. A session whose client is not connected is
// not woken; what it is owed waits on its list.
func (d *bdrain) wake(o *owed, cl *mqtt.Client) {
	if cl == nil {
		if d.b.srv == nil {
			return
		}
		var ok bool
		if cl, ok = d.b.srv.Clients.Get(o.client); !ok {
			return
		}
	}
	if cl.Closed() {
		return
	}
	o.mu.Lock()
	if o.gone {
		o.mu.Unlock()
		return
	}
	if o.pumping {
		o.repump = true
		o.mu.Unlock()
		return
	}
	o.pumping = true
	o.mu.Unlock()
	d.b.drains.Add(1)
	go func() {
		defer d.b.drains.Done()
		d.run(cl, o)
	}()
}

// run writes what the session is owed until its window is full or nothing is
// waiting, writing its acknowledgements to the store first.
//
// **It serves the connection that has the session now.** A resume wakes a
// drain already running rather than starting its own (wake), so a loop that
// began on a connection since taken over, or gone, carries on with the one
// registered for the client id, and stops where there is none.
func (d *bdrain) run(cl *mqtt.Client, o *owed) {
	for {
		d.flush(o)
		// **Written every broker.session.ack_commit_interval while there is
		// more to send, not only once nothing is.** A drain that always has
		// more - a backlog, a window that never fills - stored none of its
		// session's acknowledgements until it went idle: 500 sqlite sessions
		// each held 5,659 acknowledged and unwritten on average, a crash would
		// have sent all of them again rather than what was acknowledged in the
		// last interval (invariant 18), and a stop took 41s to write them
		// (TestABusyDrainStoresItsAcknowledgementsAsItGoes).
		for d.batch(cl, o) {
			due := d.flushDue(o)
			if h := afterBusyFlushCheck.Load(); h != nil {
				(*h)(o.client)
			}
			if due {
				d.flush(o)
			}
		}
		d.flush(o)
		if h := drainBeforeLookAgain.Load(); h != nil {
			(*h)(o.client)
		}
		o.mu.Lock()
		if !o.repump || o.gone {
			o.pumping = false
			o.mu.Unlock()
			return
		}
		o.repump = false
		o.mu.Unlock()
		if cl.Closed() || cl.IsTakenOver() {
			now, ok := d.b.srv.Clients.Get(o.client)
			if !ok || now.Closed() {
				o.mu.Lock()
				o.pumping = false
				o.mu.Unlock()
				return
			}
			cl = now
		}
	}
}

// drainBeforeLookAgain is a test seam, nil in production: when set it runs in
// run after its last flush and before it looks for more, on the drain's own
// goroutine with no lock held - where an acknowledgement taken meanwhile is
// left to the next pass, which a closed connection never makes.
var drainBeforeLookAgain atomic.Pointer[func(client string)]

// flushAll writes every session's acknowledgements taken and not yet
// written, once a graceful stop has closed every client and its drains have
// returned. A drain stops at a closed connection without a last flush, and
// an acknowledgement taken after its flush wakes no drain on one, so a
// graceful stop sent those messages again at the next start (invariant 18).
// A group's drain ends every pass with a flush (runGroup), so it is not here.
//
// **A write the store refuses is the stop's error**: nothing is left to
// write it again, and a sqlite provider keeps no snapshot that would.
func (d *bdrain) flushAll() error {
	refused := 0
	d.sessions.Range(func(_, v any) bool {
		if !d.flush(v.(*owed)) {
			refused++
		}
		return true
	})
	if refused > 0 {
		return fmt.Errorf("the session store refused the acknowledgements of %d sessions at the stop; "+
			"they are sent again at the next start", refused)
	}
	return nil
}

// afterBusyFlushCheck is a test seam, nil in production: when set it runs in
// run's busy loop right after each flushDue check, on the drain's own
// goroutine, with no lock held. A test catches the drain busy here on every
// iteration the loop actually makes, rather than by racing a wall-clock poll
// against ack_commit_interval - which a loaded machine can step around
// without ever landing a sample inside a busy stretch.
var afterBusyFlushCheck atomic.Pointer[func(client string)]

// record keeps entries of a session's in-flight table, the broadcast log
// giving way where their provider is full (withRoom). **Under fmu, and only
// while the session has not ended**, as a flush writes: its ending waits for
// fmu (end), so this lands before the store's ending of the session, which
// takes it out, or is not made. An ended session is answered as the store
// answers one it no longer holds.
func (d *bdrain) record(o *owed, window uint16, fs []store.InFlight) error {
	o.fmu.Lock()
	defer o.fmu.Unlock()
	o.mu.Lock()
	gone := o.gone
	o.mu.Unlock()
	if gone {
		return store.ErrNoSession
	}
	return d.b.withRoom(d.provider, store.InFlightSize*int64(len(fs)), "the session store", func() error {
		return d.st.SetInFlightAll(o.client, window, fs)
	})
}

// flushDue reports whether a busy drain's session has gone
// broker.session.ack_commit_interval without its acknowledgements written.
func (d *bdrain) flushDue(o *owed) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return time.Since(o.flushed) >= d.b.AckCommitInterval()
}

// flush writes what the session's acknowledgements settled - the entries
// they cleared and where its cursor now is - in one store call
// (Sessions.Acknowledge).
//
// **Before the next batch is recorded once the window is full, and every
// broker.session.ack_commit_interval while it is not, on the drain's own
// goroutine**, for two reasons. The store refuses a table larger than the
// window, so what has been acknowledged has to leave it before more goes in -
// batch counts what is written and not yet cleared against the window, so a
// full one ends the loop and is flushed (run); and the drain is off
// every connection's read loop, so the commit this waits for holds up no
// client's acknowledgements. The one exception is a clean DISCONNECT, whose
// close waits for this (storeBeforeDisconnectCloses).
//
// **One flush at a time per session**, so a flush that returns has written
// everything acknowledged before it began, including what another flush had
// already taken to write.
func (d *bdrain) flush(o *owed) bool {
	o.fmu.Lock()
	defer o.fmu.Unlock()
	through := d.counted.through.Load()
	o.mu.Lock()
	if o.gone || (!o.dirty && len(o.acked) == 0) {
		o.mu.Unlock()
		return true
	}
	done, unclaims := o.acked, o.unclaims
	o.acked, o.unclaims, o.dirty = nil, nil, false
	o.flushed = time.Now()
	cursor := o.cursorLocked(through)
	o.mu.Unlock()

	_, err := d.st.Acknowledge(o.client, cursor, done)

	o.mu.Lock()
	if err != nil && !errors.Is(err, store.ErrNoSession) && !o.gone {
		// Kept to be written again with the next: a cursor left behind is
		// messages sent again after a restart, which is at-least-once.
		d.b.log.Error("cannot record a session's acknowledgements of its broadcast",
			"client", d.b.limits.Loggable(o.client), "error", err)
		o.acked = append(done, o.acked...)
		o.unclaims = append(unclaims, o.unclaims...)
		o.dirty = true
		o.mu.Unlock()
		return false
	}
	// Written, or the session has ended and its ending took the table and the
	// cursor: either way nothing is keyed by these identifiers any more.
	cleared := false
	for _, f := range done {
		if o.stored[f.PacketID] == f.Offset {
			delete(o.stored, f.PacketID)
			cleared = true
		}
	}
	o.mu.Unlock()
	for _, f := range unclaims {
		f()
	}
	// **Room in the store's table is room for its groups**, which a hand-over
	// is measured against (memberHasRoom): the acknowledgement that freed the
	// connection's slot woke them before this.
	if cleared && d.b.srv != nil {
		if cl, ok := d.b.srv.Clients.Get(o.client); ok {
			d.wakeGroupsOf(cl)
		}
	}
	return true
}

// settle writes a session's cursor where its bound moved it, when no drain of
// its own will: one runs only for a connected client (wake), so an away
// session's give-ups stayed in memory until it resumed, and a restart before
// then counted as owed again what it had given up - where the log still
// held it for another session - and gave it up again, counted twice. Off the
// caller's goroutine, since a count runs on its publisher's (invariant 16),
// one at a time per session, and again while there is more to write; a write
// the store refuses is left for the next give-up or resume, or the broker's
// retry a second later (retryAway), whichever comes first.
func (d *bdrain) settle(o *owed) {
	if d.b.srv != nil {
		if cl, ok := d.b.srv.Clients.Get(o.client); ok && !cl.Closed() {
			return // its drain writes it
		}
	}
	o.mu.Lock()
	if o.settling || o.gone {
		o.mu.Unlock()
		return
	}
	o.settling = true
	o.mu.Unlock()
	d.b.drains.Add(1)
	go func() {
		defer d.b.drains.Done()
		for {
			ok := d.flush(o)
			o.mu.Lock()
			if !ok || !o.dirty || o.gone {
				o.settling = false
				o.mu.Unlock()
				return
			}
			o.mu.Unlock()
		}
	}()
}

// retryAway writes again the acknowledgements the store refused for a session
// whose client is away, and answers how many it still could not and the last
// error. **Under the id's session lock, and only where no connection owns the
// id** (invariant 17): a connected session's own drain writes its
// acknowledgements. settle writes an away session's once, and a refused write
// stayed in memory until the client came back - so a clean DISCONNECT whose
// flush the store refused, and the broker crashing any time later, sent the
// session again what it had acknowledged (invariant 18).
func (d *bdrain) retryAway() (refused int, last error) {
	var due []*owed
	d.sessions.Range(func(_, v any) bool {
		o := v.(*owed)
		o.mu.Lock()
		if !o.gone && (o.dirty || len(o.acked) > 0) {
			due = append(due, o)
		}
		o.mu.Unlock()
		return true
	})
	owns := func(o *owed) bool {
		d.b.mu.Lock()
		defer d.b.mu.Unlock()
		_, owned := d.b.owner[o.client]
		return owned
	}
	for _, o := range due {
		// **Looked at before the lock as well as under it.** A connected
		// session is the common case and is skipped either way, but its id's
		// session lock is held across a CONNECT's store wait, and this runs on
		// the broker's loop: waiting for that lock only to find the id owned
		// stalls the loop's queue offers, sweeps and position flush behind a
		// reconnect storm. The look under the lock is the one that decides
		// (invariant 17): an owner that arrives between the two is seen there.
		if owns(o) {
			continue
		}
		unlock := d.b.lockSession(o.client)
		if !owns(o) && !d.flush(o) {
			refused++
			last = errors.New("the session store refused a session's acknowledgements")
		}
		unlock()
	}
	return refused, last
}

// broadcastOptions is what a session's subscriptions ask of one delivery,
// merged the way the substrate merges overlapping subscriptions: the highest
// QoS any grants, every identifier, and Retain As Published if any of them
// asks for it (RFC 0003 "Broadcast").
type broadcastOptions struct {
	qos byte
	ids []int
	rap bool
}

// optionsFor is what the client's own subscriptions ask of a delivery on
// topic now, and false where none of them reaches it any more. A shared
// subscription is not the session's own (a group's cursor serves it).
func optionsFor(subs map[string]packets.Subscription, topic string) (broadcastOptions, bool) {
	var o broadcastOptions
	matched := false
	for filter, s := range subs {
		if isShareFilter(filter) || !channel.Matches(filter, topic) {
			continue
		}
		if !matched || s.Qos > o.qos {
			o.qos = s.Qos
		}
		matched = true
		if s.Identifier > 0 {
			o.ids = append(o.ids, s.Identifier)
		}
		o.rap = o.rap || s.RetainAsPublished
	}
	sort.Ints(o.ids)
	return o, matched
}

// optionsOf is what a delivery sent again asks of its packet: for one a shared
// group handed the session, what the session's subscription to that group
// asks; for its own, what optionsFor finds. A group the session no longer
// holds asks nothing beyond the QoS the delivery went out at.
func optionsOf(subs map[string]packets.Subscription, group, topic string) broadcastOptions {
	if group == "" {
		opt, _ := optionsFor(subs, topic)
		return opt
	}
	sub, ok := subs[group]
	if !ok {
		return broadcastOptions{}
	}
	opt := broadcastOptions{qos: sub.Qos, rap: sub.RetainAsPublished}
	if sub.Identifier > 0 {
		opt.ids = []int{sub.Identifier}
	}
	return opt
}

// broadcastPacket is the delivery of one log message: the publish as it
// arrived (RFC 0003 "From publish to record"), at the QoS given, with the
// options the subscriptions asked for and the expiry left.
func broadcastPacket(r store.Record, qos byte, opt broadcastOptions, legacy bool, now time.Time) packets.Packet {
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos,
			// [MQTT-3.3.1-12]: a live delivery carries RETAIN only where the
			// subscription asked for it as published, which 3.1.1 cannot. A
			// retained value sent because of a SUBSCRIBE (OwedTo) carries it
			// always, on both versions [MQTT-3.3.1-8].
			Retain: r.OwedTo != "" || r.Retain && opt.rap && !legacy},
		TopicName: r.Topic,
		Payload:   r.Payload,
		// Counted against the session's bound by its owed list, not by the
		// in-flight table it waits in on the wire.
		Bounded: true,
	}
	for _, h := range r.Headers {
		pk.Properties.User = append(pk.Properties.User, packets.UserProperty{Key: h.Key, Val: h.Value})
	}
	applyProps(&pk, r, now, false)
	if len(opt.ids) > 0 {
		pk.Properties.SubscriptionIdentifier = slices.Clone(opt.ids)
	}
	return pk
}

// batch writes at most one batch of what the session is owed, and reports
// whether to come straight back for another: where it let messages go
// without sending them, or sent them at QoS 0, since neither brings an
// acknowledgement to call it back, and where it took as many as one batch
// may (pumpBatchCap), since the window may have room for more that no
// acknowledgement is needed to send - a client may take its whole Receive
// Maximum before it acknowledges any.
func (d *bdrain) batch(cl *mqtt.Client, o *owed) bool {
	window := receiveMaximum(cl)

	// What a resume put back on the wire goes out first (writeAgain), and
	// nothing new while any of it waits: a resumed session's table goes out in
	// the order it was first sent (MQTT-4.6.0-1). What the engine carried over
	// from the connection a link cut ended, and holds back for room, needs no
	// such rule here: it is in the engine's table, so the room measured below
	// (the Receive Maximum less that table) is gone until the engine has
	// written it.
	if !d.writeAgain(cl, o) {
		return false
	}
	o.mu.Lock()
	if o.gone {
		o.mu.Unlock()
		return false
	}
	room := min(d.b.window(cl), int(window)-len(o.stored), pumpBatchCap)
	// **No more than half the bound on the wire** (RFC 0002), earliest
	// first, and always one when nothing is: what waits can be given up, so a
	// client that stops acknowledging holds the oldest it was sent and the
	// newest it was not, rather than a bound of the oldest.
	half, wire := d.b.limits.SessionQueueBytes/2, o.wire
	var offs []uint64
	for _, e := range o.list {
		if len(offs) >= room {
			break
		}
		if e.sent {
			continue
		}
		c := e.charge + wireEntryCost
		if half > 0 && wire > 0 && wire+c > half {
			break
		}
		offs = append(offs, e.offset)
		wire += c
	}
	o.pickLocked(offs)
	o.mu.Unlock()
	// Whatever this batch picked and did not send or let go of - a read or a
	// record that failed, identifiers run out - goes back to waiting.
	defer func() {
		o.mu.Lock()
		o.unpickLocked(offs)
		o.mu.Unlock()
	}()
	if len(offs) == 0 {
		return false
	}

	recs, err := d.log.ReadAt(offs...)
	if err != nil {
		d.b.log.Error("cannot read the broadcast log; this session is not being served",
			"client", d.b.limits.Loggable(o.client), "error", err)
		return false
	}
	held := make(map[uint64]store.Record, len(recs))
	for _, r := range recs {
		held[r.Offset] = r
	}

	now := time.Now()
	subs := cl.State.Subscriptions.GetAll()
	legacy := legacyClient(cl)
	var (
		out     []bsend
		entries []store.InFlight
		letGo   []uint64
		expired int
	)
	for _, off := range offs {
		r, ok := held[off]
		if !ok {
			// The log no longer holds it: taken to make room (RFC 0002's
			// full-store table has what that costs the session).
			letGo = append(letGo, off)
			continue
		}
		if r.Expired(now) {
			// Read, not enforced by deleting it: it leaves the log only once
			// every session owed it has passed it (invariant 2).
			expired++
			letGo = append(letGo, off)
			continue
		}
		opt, ok := optionsFor(subs, r.Topic)
		if !ok {
			// No subscription reaches it any more. MQTT lets the server stop
			// delivering what an UNSUBSCRIBE removed (MQTT-3.10.4-2).
			letGo = append(letGo, off)
			continue
		}
		if !d.b.mayDeliver(cl, r.Topic) {
			// Asked per delivery, as the substrate asks for a live one, and
			// what it refuses is not delivered - there is no position to
			// stall at and resume from (invariant 16). Counted where the live
			// refusal is: nowhere but this line.
			d.b.log.Debug("did not deliver a broadcast the acl_file does not allow here",
				"client", d.b.limits.Loggable(o.client), "topic", d.b.limits.Loggable(r.Topic))
			letGo = append(letGo, off)
			continue
		}
		qos := min(r.QoS, opt.qos)
		pk := broadcastPacket(r, qos, opt, legacy, now)
		if qos > 0 {
			id, err := cl.NextPacketID()
			if err != nil {
				break // none left: an acknowledgement frees one and calls this back
			}
			pk.PacketID = uint16(id)
			entries = append(entries, store.InFlight{Offset: off, PacketID: pk.PacketID, QoS: qos, State: store.MessageSent})
		}
		out = append(out, bsend{pk, off})
	}

	// **Recorded before it is written**, so a delivery on the wire is always
	// one a resumed session can send again under the identifier it carried
	// (MQTT-4.4.0-1).
	if len(entries) > 0 {
		if err := d.record(o, window, entries); err != nil {
			for _, f := range entries {
				cl.State.Inflight.Unclaim(f.PacketID)
			}
			if !errors.Is(err, store.ErrNoSession) {
				d.b.log.Error("cannot record a session's broadcast on the wire; it is not sent",
					"client", d.b.limits.Loggable(o.client), "error", err)
			}
			d.drop(o, letGo, expired)
			return false
		}
	}

	o.mu.Lock()
	if o.gone {
		o.mu.Unlock()
		for _, f := range entries {
			cl.State.Inflight.Unclaim(f.PacketID)
		}
		return false
	}
	// **Nothing decided from a connection that no longer has the session.**
	// What this batch let go of - no subscription reaching a message, the
	// acl_file refusing it - it read from cl, and a takeover empties the old
	// connection's subscriptions as it moves them to the new one: it closes
	// the old connection first (inheritClientSession), so a batch that read
	// them emptied finds cl closed here. So nothing is let go or sent: what
	// it picked goes back to waiting (the deferred unpick) for the connection
	// that has the session now, its rows leave the table with the next write
	// - which comes before any batch can pick them again, as there is one
	// drain a session - and the identifiers, reserved on cl's own table, are
	// freed after it, as unsend's are.
	if cl.Closed() || cl.IsTakenOver() {
		for _, f := range entries {
			id := f.PacketID
			o.stored[id] = f.Offset
			o.acked = append(o.acked, f)
			o.unclaims = append(o.unclaims, func() { cl.State.Inflight.Unclaim(id) })
		}
		o.mu.Unlock()
		return false
	}
	for _, off := range letGo {
		o.removeLocked(off)
	}
	if len(letGo) > 0 {
		o.dirty = true
	}
	for _, s := range out {
		o.markSentLocked(s.off)
		if s.pk.FixedHeader.Qos > 0 {
			f := store.InFlight{Offset: s.off, PacketID: s.pk.PacketID, QoS: s.pk.FixedHeader.Qos, State: store.MessageSent}
			o.flying[s.pk.PacketID] = flight{entry: f, conn: cl}
			o.stored[s.pk.PacketID] = s.off
		}
	}
	o.mu.Unlock()
	d.counts(letGo, expired)

	sentAtZero := false
	for i, s := range out {
		registered := 0
		if s.pk.FixedHeader.Qos > 0 {
			// Registered before the write, because an acknowledgement can
			// arrive before WritePacket returns, and only while cl still has
			// the session (WhileOwned): one registered on a table a takeover
			// has already copied would be on no connection's, and one the copy
			// carries is the new connection's to send again.
			pk := s.pk
			if !cl.WhileOwned(func() {
				cl.State.Inflight.Set(pk)
				cl.State.Inflight.DecreaseSendQuota()
			}) {
				d.unsend(cl, o, out[i:], 0)
				return false
			}
			registered = 1
		}
		if err := d.b.writeTo(cl, s.pk); err != nil {
			if errors.Is(err, packets.ErrPacketTooLarge) {
				// Larger than the client said it can take: MQTT has the server
				// discard it, not send it (MQTT-3.1.2-25), and the link is
				// healthy, so the rest go on.
				d.discard(cl, o, s.pk, s.off)
				continue
			}
			// The connection is going. What was written stays in flight to
			// be sent again when the session resumes; the rest stay owed.
			d.unsend(cl, o, out[i:], registered)
			return false
		}
		if s.pk.FixedHeader.Qos == 0 {
			d.passed(o, s.off)
			sentAtZero = true
		}
	}
	if h := afterBatchWritten.Load(); h != nil {
		(*h)(o.client, len(out))
	}
	return len(letGo) > 0 || sentAtZero || len(offs) == pumpBatchCap
}

// afterBatchWritten is a test seam, nil in production: when set it runs at
// the end of a successful batch, on the drain's own goroutine, with no lock
// held, after this batch's writes reached the wire. In-process, a batch and
// its acknowledgement can round-trip faster than any wall-clock margin gives
// a test to observe the drain mid-backlog: a test paces sends here to spread
// a backlog across enough ack_commit_interval windows to be caught busy
// deterministically, rather than by luck of the machine's speed or load.
var afterBatchWritten atomic.Pointer[func(client string, sent int)]

// bsend is one delivery a batch is writing, and the offset it is for.
type bsend struct {
	pk  packets.Packet
	off uint64
}

// drop lets go of what a batch decided to let go of when the batch itself
// could not go on.
func (d *bdrain) drop(o *owed, letGo []uint64, expired int) {
	o.mu.Lock()
	if o.gone {
		// Its ending let go of everything on its list, these included.
		o.mu.Unlock()
		return
	}
	for _, off := range letGo {
		o.removeLocked(off)
	}
	if len(letGo) > 0 {
		o.dirty = true
	}
	o.mu.Unlock()
	d.counts(letGo, expired)
}

// counts lets go of messages a batch passed without sending, counting the
// expired among them.
func (d *bdrain) counts(letGo []uint64, expired int) {
	if expired > 0 {
		d.b.counted.expired.Add(uint64(expired))
	}
	d.letGo(letGo...)
}

// queued is what every session is owed from the log, and what that comes to
// against limits.session_queue_bytes: the log's part of
// saguin_session_queue_messages and saguin_session_queue_bytes (RFC 0005).
func (d *bdrain) queued() (messages, bytes int64) {
	d.sessions.Range(func(_, v any) bool {
		o := v.(*owed)
		o.mu.Lock()
		messages += int64(len(o.list))
		bytes += o.bytes
		o.mu.Unlock()
		return true
	})
	return messages, bytes
}

// queueFull counts what a session gave up, or was refused, at
// limits.session_queue_bytes (RFC 0002).
func (d *bdrain) queueFull(o *owed, n int) {
	if n == 0 {
		return
	}
	if d.b.srv != nil {
		d.b.srv.Info.SessionQueueDropped.Add(int64(n))
	}
	d.b.log.Debug("gave up broadcast messages a session was owed: it holds limits.session_queue_bytes",
		"client", d.b.limits.Loggable(o.client), "messages", n)
}

// unsend puts back what a batch could not write: out of the engine's window
// and back to waiting. Their entries leave the store's table with the next
// acknowledgements written, since nothing is on the wire under them, and each
// identifier stays reserved until they have: a delivery made under it before
// then would have its row cleared with the old one's. The first registered of
// rest are in cl's in-flight table; the rest never reached it.
//
// **Not one a takeover carried.** A takeover copies cl's table to the new
// connection, which sends it again under its identifier; that one is the new
// connection's to finish, so it stays on the wire, and its acknowledgement
// there settles it (takes). Put back as well, it went out twice. So each is
// retired only while cl still has the session (WhileOwned), outside o.mu,
// since a takeover's clearing of a table reaches o.mu under the lock
// WhileOwned takes.
func (d *bdrain) unsend(cl *mqtt.Client, o *owed, rest []bsend, registered int) {
	carried := map[uint16]bool{}
	for i, s := range rest[:min(registered, len(rest))] {
		if s.pk.FixedHeader.Qos == 0 {
			continue
		}
		id := rest[i].pk.PacketID
		if !cl.WhileOwned(func() {
			if cl.State.Inflight.Retire(id) {
				cl.State.Inflight.IncreaseSendQuota()
			}
		}) {
			carried[id] = true
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range rest {
		if s.pk.FixedHeader.Qos == 0 {
			// Not on the wire either: back to waiting, or it would hold the
			// cursor and a share of the wire for good.
			o.markUnsentLocked(s.off)
			continue
		}
		id := s.pk.PacketID
		if carried[id] {
			continue
		}
		o.unclaims = append(o.unclaims, func() { cl.State.Inflight.Unclaim(id) })
		if f, ok := o.flying[id]; ok {
			delete(o.flying, id)
			o.acked = append(o.acked, f.entry)
		}
		o.markUnsentLocked(s.off)
	}
}

// discard gives up a delivery MQTT forbids sending to this client, and lets
// it go.
func (d *bdrain) discard(cl *mqtt.Client, o *owed, pk packets.Packet, off uint64) {
	// Retired only while cl still has the session, as unsend's are: one a
	// takeover carried is the new connection's to answer for.
	id := pk.PacketID
	if pk.FixedHeader.Qos > 0 && !cl.WhileOwned(func() {
		if cl.State.Inflight.Retire(id) {
			cl.State.Inflight.IncreaseSendQuota()
		}
	}) {
		return
	}
	o.mu.Lock()
	if pk.FixedHeader.Qos > 0 {
		// Reserved until its entry has left the store's table, as unsend's.
		o.unclaims = append(o.unclaims, func() { cl.State.Inflight.Unclaim(id) })
		if f, ok := o.flying[id]; ok {
			delete(o.flying, id)
			o.acked = append(o.acked, f.entry)
		}
	}
	took := o.removeLocked(off)
	o.dirty = true
	o.mu.Unlock()
	if d.b.srv != nil {
		d.b.srv.Info.DeliveriesTooLarge.Add(1)
	}
	d.b.log.Debug("did not deliver a broadcast larger than this client's Maximum Packet Size",
		"client", d.b.limits.Loggable(o.client), "offset", off)
	if took {
		d.letGo(off)
	}
}

// passed takes a delivery that needs nothing more - written at QoS 0 - off
// its session's list, and lets go of it. **Only if the list still had it**:
// a session that ended meanwhile let go of its whole list, this one
// included, and a second letting go would take the message from the log
// while another session is still owed it.
func (d *bdrain) passed(o *owed, off uint64) {
	o.mu.Lock()
	took := o.removeLocked(off)
	o.dirty = true
	o.mu.Unlock()
	if took {
		d.letGo(off)
	}
}

// letGo is sessions letting go of messages, one each: a message leaves the
// log with the last.
func (d *bdrain) letGo(offsets ...uint64) {
	if len(offsets) == 0 {
		return
	}
	var gone []uint64
	d.mu.Lock()
	for _, off := range offsets {
		n, ok := d.owners[off]
		if !ok {
			continue
		}
		if n <= 1 {
			delete(d.owners, off)
			gone = append(gone, off)
			continue
		}
		d.owners[off] = n - 1
	}
	d.mu.Unlock()
	d.release(gone)
}

// release takes messages nobody owes out of the log, on a goroutine of its
// own: the caller may be a client's read loop or a publisher.
//
// **Gathered, one removal an ack_commit_interval** (saguin): a removal is a
// transaction, and a transaction's fixed cost - its commit, the counter row,
// a page of the log's file - is what a resumed session's drain was paying,
// about one for every two messages delivered. What was released waits up to
// broker.session.ack_commit_interval, or until releaseBatch are waiting, and
// goes in one Remove. RFC 0004 has a message leave once nobody owes it and
// promises nothing about how soon; nobody can come to owe one again, since
// an offset is counted once. Three things end the wait early: releaseBatch
// waiting, a give-up that needs the room (giveUp takes them itself), and a
// broker stopping (Shutdown kicks it before it waits for the drains, and a
// stopping broker does not wait). A crash in the wait loses nothing: the next
// start counts the log again and releases what nobody owes.
func (d *bdrain) release(offsets []uint64) {
	if len(offsets) == 0 {
		return
	}
	d.rmu.Lock()
	d.rpending = append(d.rpending, offsets...)
	start := !d.rrunning
	d.rrunning = true
	full := len(d.rpending) >= releaseBatch
	d.rmu.Unlock()
	if full {
		d.kickRelease()
	}
	if !start {
		return
	}
	d.b.drains.Add(1)
	go func() {
		defer d.b.drains.Done()
		for {
			d.rmu.Lock()
			if len(d.rpending) == 0 {
				d.rrunning = false
				d.rmu.Unlock()
				return
			}
			if wait := d.b.AckCommitInterval(); wait > 0 && len(d.rpending) < releaseBatch && !d.b.stopping.Load() {
				d.rmu.Unlock()
				t := time.NewTimer(wait)
				select {
				case <-t.C:
				case <-d.rkick:
				}
				t.Stop()
			} else {
				d.rmu.Unlock()
			}
			d.removeGathered()
		}
	}()
}

// removeGathered takes what releases have gathered and removes it, **under
// gmu, as a give-up takes and removes**: a give-up that needs room waits for
// a batch taken here, rather than finding it gone from the list and still
// in the log and giving up a message a session is owed in front of it.
func (d *bdrain) removeGathered() {
	d.gmu.Lock()
	defer d.gmu.Unlock()
	batch := d.takeReleases()
	if len(batch) == 0 {
		return // a give-up took them first
	}
	_, freed, err := d.log.Remove(batch...)
	if err != nil {
		// Kept, owed to nobody, until the start that counts the log
		// again finds nobody owes it.
		d.b.log.Error("cannot remove broadcast messages nobody owes from the log",
			"messages", len(batch), "error", err)
	}
	d.rfreed.Add(freed)
}

// releaseBatch is how many released messages end a release's wait to gather.
const releaseBatch = 500

// kickRelease ends a release's wait to gather, now or at its next.
func (d *bdrain) kickRelease() {
	select {
	case d.rkick <- struct{}{}:
	default:
	}
}

// takeReleases is what releases are gathering, taken from them.
func (d *bdrain) takeReleases() []uint64 {
	d.rmu.Lock()
	defer d.rmu.Unlock()
	batch := d.rpending
	d.rpending = nil
	return batch
}

// insertLocked puts an entry on the list in offset order: two counts can
// arrive in either order. The caller holds o.mu.
func (o *owed) insertLocked(e owedEntry) {
	i := sort.Search(len(o.list), func(i int) bool { return o.list[i].offset >= e.offset })
	if i < len(o.list) && o.list[i].offset == e.offset {
		return
	}
	o.list = slices.Insert(o.list, i, e)
	o.unsent++
	o.bytes += e.cost()
}

// onWireLocked reports whether the session has the message at off on the
// wire, or picked by a batch that is about to put it there. The caller holds
// o.mu.
func (o *owed) onWireLocked(off uint64) bool {
	i := o.findLocked(off)
	return i >= 0 && !o.list[i].waiting()
}

// removeLocked takes an entry off the list, and reports whether it was on
// it. The caller holds o.mu.
func (o *owed) removeLocked(off uint64) bool {
	i := o.findLocked(off)
	if i < 0 {
		return false
	}
	e := o.list[i]
	if !e.waiting() {
		o.wire -= e.cost()
	}
	if !e.sent {
		o.unsent--
	}
	o.bytes -= e.cost()
	o.deleteLocked(i)
	o.tableLocked()
	return true
}

// tableLocked charges the session's bytes wireTableCost while it has
// anything on the wire, and not otherwise. What it has on the wire is in the
// client's in-flight table, Bounded - counted here and not there - and a
// table holding anything takes its map and its order besides its entries;
// the table leaves that to whichever path counts its entries, so a session
// whose only deliveries in flight are the log's is charged it here or
// nowhere. Called by everything that changes wire. The caller holds o.mu.
func (o *owed) tableLocked() {
	if want := o.wire > 0; want != o.table {
		if want {
			o.bytes += wireTableCost
		} else {
			o.bytes -= wireTableCost
		}
		o.table = want
	}
}

// dropLocked takes the entries at offsets off the list, each that is on it
// and neither on the wire nor picked by a batch, and answers the offsets it
// took. The caller holds o.mu, and lets go of them (letGo) and counts them
// under its own cause once it has let o.mu go. The cursor moves past them
// with the session's next write. An entry on the wire stays until it is
// acknowledged (RFC 0002), and one picked is about to be.
func (o *owed) dropLocked(offsets []uint64) []uint64 {
	var taken []uint64
	for _, off := range offsets {
		i := o.findLocked(off)
		if i < 0 || !o.list[i].waiting() {
			continue
		}
		if o.list[i].returned {
			// Its returned row goes with the group's next write.
			o.forget = append(o.forget, off)
		}
		o.unsent--
		o.bytes -= o.list[i].cost()
		o.deleteLocked(i)
		taken = append(taken, off)
	}
	if len(taken) > 0 {
		o.dirty = true
	}
	return taken
}

// roomLocked reports whether the session may take a message of this charge
// within bound: what it holds, less everything boundLocked could give up for
// it, plus the message (RFC 0002: a session at its bound holding nothing it
// can give up is refused the next). One with nothing on the wire always has
// room, so a message as large as the bound is never refused for its size.
// The caller holds o.mu.
func (o *owed) roomLocked(bound, charge int64) bool {
	if bound <= 0 || o.bytes+charge <= bound {
		return true
	}
	return o.wire == 0 || o.wire+wireTableCost+charge <= bound
}

// boundLocked gives up the oldest entries the session has waiting, past
// bound, never the one at keep (RFC 0002: full means the oldest goes). It
// answers what it took, which the caller lets go of and counts. The caller
// holds o.mu.
func (o *owed) boundLocked(bound int64, keep uint64) []uint64 {
	if bound <= 0 || o.bytes <= bound {
		return nil
	}
	var offs []uint64
	over := o.bytes - bound
	for _, e := range o.list {
		if over <= 0 {
			break
		}
		if !e.waiting() || e.offset == keep {
			continue
		}
		offs = append(offs, e.offset)
		over -= e.charge
	}
	return o.dropLocked(offs)
}

// pickLocked marks what a batch is about to write, and unpickLocked puts back
// to waiting whatever of it is still picked. The caller holds o.mu.
func (o *owed) pickLocked(offs []uint64) {
	for _, off := range offs {
		if i := o.findLocked(off); i >= 0 && o.list[i].waiting() {
			o.list[i].picked = true
			o.wire += o.list[i].cost()
			o.bytes += wireEntryCost
		}
	}
	o.tableLocked()
}

func (o *owed) unpickLocked(offs []uint64) {
	for _, off := range offs {
		if i := o.findLocked(off); i >= 0 && o.list[i].picked {
			o.wire -= o.list[i].cost()
			o.bytes -= wireEntryCost
			o.list[i].picked = false
		}
	}
	o.tableLocked()
}

// findLocked is where off is on the list, or -1. The caller holds o.mu.
func (o *owed) findLocked(off uint64) int {
	i := sort.Search(len(o.list), func(i int) bool { return o.list[i].offset >= off })
	if i == len(o.list) || o.list[i].offset != off {
		return -1
	}
	return i
}

// deleteLocked takes the entry at i off the list, moving whichever side of it
// is shorter. The oldest waiting entry is what the bound gives up, one per
// message a full session is sent, and all that lies before it is what is on
// the wire, so that side is the short one. The caller holds o.mu.
func (o *owed) deleteLocked(i int) {
	if i < len(o.list)-1-i {
		copy(o.list[1:i+1], o.list[:i])
		o.list = o.list[1:]
		return
	}
	o.list = slices.Delete(o.list, i, i+1)
}

func (o *owed) markSentLocked(off uint64) {
	if i := o.findLocked(off); i >= 0 && !o.list[i].sent {
		if !o.list[i].picked {
			o.wire += o.list[i].charge + wireEntryCost
			o.bytes += wireEntryCost
		}
		o.list[i].sent, o.list[i].picked = true, false
		o.unsent--
	}
	o.tableLocked()
}

func (o *owed) markUnsentLocked(off uint64) {
	if i := o.findLocked(off); i >= 0 && o.list[i].sent {
		o.wire -= o.list[i].cost()
		o.bytes -= wireEntryCost
		o.list[i].sent = false
		o.unsent++
	}
	o.tableLocked()
}

// cursorLocked is where the session's cursor stands: the lowest offset it is
// owed and has not acknowledged, and never past an offset still being
// counted. The caller holds o.mu.
//
// **An entry another cursor governs is passed over**: on a member's list, one
// a group handed it (the group's cursor passed it at the hand-over), and on a
// group's list, one returned or lent (the group's returned list holds it).
// Counted, one at the front would put the cursor behind everything after it,
// and a restart would send all of that again.
func (o *owed) cursorLocked(through uint64) uint64 {
	return o.cursorExceptLocked(through, 0)
}

// cursorExceptLocked is cursorLocked with the entry at except left out: where
// the cursor stands once it has gone. The caller holds o.mu.
func (o *owed) cursorExceptLocked(through, except uint64) uint64 {
	c := through + 1
	for _, e := range o.list {
		if e.group != "" || e.returned || e.lent != nil || e.offset == except {
			continue
		}
		if e.offset < c {
			c = e.offset
		}
		break
	}
	return c
}

// repairCursors gives, at a start, each shared group a durable member holds
// and the store has no cursor for one, at the earliest Since of those
// members, and answers cursors with it added. A group's cursor is made with
// its first durable member's record (keepSubscriptions), so this finds one
// missing only where a broker before that left it so - one whose cursor the
// store refused while its SUBACK said success. **The earliest Since, so the group is owed
// at least what any member's own subscription was**: a message counted
// again is sent again, which at-least-once allows, and one left out is lost.
// A cursor the store refuses fails the start: a group served without one
// keeps nothing across the next crash.
func (d *bdrain) repairCursors(sessions []store.Session, cursors map[string]uint64) (map[string]uint64, error) {
	missing := map[string]uint64{}
	for _, sess := range sessions {
		if sess.ExpiryInterval == 0 {
			continue
		}
		for _, sub := range sess.Subscriptions {
			if !isShareFilter(sub.Filter) {
				continue
			}
			if _, has := cursors[sub.Filter]; has {
				continue
			}
			at := max(sub.Since, 1)
			if was, ok := missing[sub.Filter]; !ok || at < was {
				missing[sub.Filter] = at
			}
		}
	}
	for _, group := range slices.Sorted(maps.Keys(missing)) {
		offset := missing[group]
		if err := d.st.CreateShareCursor(group, offset); err != nil {
			return nil, fmt.Errorf("shared group %q has a durable member and no cursor, and cannot be given one: %w",
				group, err)
		}
		if cursors == nil {
			cursors = map[string]uint64{}
		}
		d.b.log.Warn("gave a shared group with a durable member and no cursor one at the start",
			"group", d.b.limits.Loggable(group), "cursor", offset)
		cursors[group] = offset
	}
	return cursors, nil
}

// rebuildBatch is how many messages a start reads from the log at a time.
const rebuildBatch = 1024

// rebuild puts back, at a start, who owes each message the log holds: every
// session's owed list and every message's count, which are kept in memory
// only. A session is owed:
//   - each entry of its in-flight table, still on the wire under the
//     identifier it went out with (MQTT-4.4.0-1); and
//   - every other message at or after its cursor that one of its
//     subscriptions matched when it was published: a subscription made at or
//     before the message's offset (Since), with No Local and a declared
//     partition slice asked as the live broadcast asks them, and a QoS of 1
//     or more granted.
//
// A copy of a channel's record (store.Record.ForGroups) is owed to the groups
// its topic matches and to no session's own subscription; a copy of a
// retained value (OwedTo) to its session alone, at or after its cursor.
//
// **At or after the cursor, and not only past its highest entry on the
// wire.** A message counted late can sit below one already sent, so a message
// between the cursor and the highest entry, and not in the table, may be one
// not yet sent rather than one acknowledged. Counting it again costs a message
// a session had acknowledged out of order being sent again after a restart,
// which is at-least-once; leaving it out would lose one it was never sent.
//
// A message nobody is owed leaves the log. An in-flight entry whose message
// the log no longer holds leaves the table with the session's next write.
//
// **A shared group with a cursor is owed as a session is** (bgroup.go): what
// was returned to it, and every message at or after its cursor its filter
// matches that none of its members holds in its table - those it handed
// over, which are each member's. An entry a group handed a member is put on
// the member's list whatever the member's own cursor, which does not govern
// it. A group the store ended as it was opened (EndedAtOpen) is owed nothing,
// and what it would have been owed is counted as dropped with no member left.
func (d *bdrain) rebuild() error {
	sessions, err := d.st.All()
	if err != nil {
		return fmt.Errorf("the sessions whose broadcast is to be counted: %w", err)
	}
	cursors, err := d.st.ShareCursors()
	if err != nil {
		return fmt.Errorf("the shared groups' cursors: %w", err)
	}
	returnedOffs, err := d.st.ShareReturned()
	if err != nil {
		return fmt.Errorf("the shared groups' returned lists: %w", err)
	}
	endedAtOpen := d.st.EndedAtOpen()
	if cursors, err = d.repairCursors(sessions, cursors); err != nil {
		return err
	}
	// Each group's filter in a trie of its own, under the group's name.
	gidx := mqtt.NewTopicsIndex()
	glists := map[string]*owed{}
	returned := map[string]map[uint64]bool{}
	for group := range cursors {
		g := d.newGroupList(group)
		d.groups.Store(group, g)
		glists[group] = g
		gidx.Subscribe(group, packets.Subscription{Filter: shareTopicFilter(group), Qos: 1})
		returned[group] = map[uint64]bool{}
		for _, off := range returnedOffs[group] {
			returned[group][off] = true
		}
	}
	endedIdx := mqtt.NewTopicsIndex()
	endedReturned := map[uint64]int{}
	for group, st := range endedAtOpen {
		if st.Cursor > 0 {
			endedIdx.Subscribe(group, packets.Subscription{Filter: shareTopicFilter(group), Qos: 1})
		}
		for _, off := range st.Returned {
			endedReturned[off]++
		}
	}
	handedBy := map[string]map[uint64]bool{}
	type restoring struct {
		o      *owed
		cursor uint64
		subs   []store.SessionSubscription
	}
	byClient := make(map[string]*restoring, len(sessions))
	onWire := map[uint64][]rebuiltEntry{}
	// **One trie over every subscription, each under a key of its own**, so
	// that a message is matched in the depth of its topic rather than
	// against every session in turn, and each subscription is judged on its
	// own Since, No Local and slice rather than merged with its session's
	// others.
	idx := mqtt.NewTopicsIndex()
	sinces := make(map[string]map[string]uint64, len(sessions))
	for _, sess := range sessions {
		r := &restoring{o: d.attach(sess.Client), cursor: 1, subs: sess.Subscriptions}
		byClient[sess.Client] = r
		pos, has, err := d.log.Position(store.MQTTReader(sess.Client))
		if err != nil {
			return fmt.Errorf("session %q's cursor: %w", sess.Client, err)
		}
		if has {
			r.cursor = pos.Offset
		}
		_, table, err := d.st.InFlight(sess.Client)
		if err != nil {
			return fmt.Errorf("session %q's in-flight table: %w", sess.Client, err)
		}
		for _, f := range table {
			onWire[f.Offset] = append(onWire[f.Offset], rebuiltEntry{client: sess.Client, f: f})
			if f.Group != "" {
				if handedBy[f.Group] == nil {
					handedBy[f.Group] = map[uint64]bool{}
				}
				handedBy[f.Group][f.Offset] = true
			}
		}
		sinces[sess.Client] = make(map[string]uint64, len(sess.Subscriptions))
		for i, sub := range sess.Subscriptions {
			sinces[sess.Client][sub.Filter] = sub.Since
			if isShareFilter(sub.Filter) || sub.QoS == 0 {
				continue
			}
			idx.Subscribe(rebuildKey(sess.Client, i), packets.Subscription{Filter: sub.Filter, Qos: sub.QoS})
		}
	}
	d.mu.Lock()
	d.sinces = sinces
	d.mu.Unlock()

	// **From the floor, not from the lowest cursor**, so a message every
	// session has passed, or one no session is left to be owed, is found
	// and leaves the log too.
	var nobody []uint64
	bound := d.b.limits.SessionQueueBytes
	gaveUp := map[*owed]int{}
	placed := map[rebuiltEntry]bool{}
	seen := map[uint64]bool{}
	endedCount := 0
	for from, next := d.log.Floor(), d.log.Next(); from < next; {
		recs, err := d.log.ReadFromN(from, rebuildBatch)
		if err != nil {
			return fmt.Errorf("the broadcast log from offset %d: %w", from, err)
		}
		if len(recs) == 0 {
			break
		}
		for _, rec := range recs {
			from = rec.Offset + 1
			charge := store.RecordSize(rec) + owedEntryCost
			owners := map[string]*store.InFlight{}
			for _, e := range onWire[rec.Offset] {
				f := e.f
				owners[e.client] = &f
				placed[e] = true
			}
			// **A copy of a channel's record is owed to groups alone**
			// (store.Record.ForGroups), never matched to a session's own
			// subscription: a durable subscriber to the channel has the
			// record from the channel. **A retained value's is owed to its
			// session alone** (OwedTo), at or after its cursor, and matched
			// to nothing.
			if r := byClient[rec.OwedTo]; rec.OwedTo != "" && r != nil && rec.Offset >= r.cursor {
				if _, done := owners[rec.OwedTo]; !done {
					owners[rec.OwedTo] = nil
				}
			}
			if !rec.ForGroups && rec.OwedTo == "" {
				var h uint64
				hashed := false
				idx.EachSubscriber(rec.Topic, func(key string) {
					client, i := splitRebuildKey(key)
					r := byClient[client]
					if _, done := owners[client]; done || r == nil || i >= len(r.subs) || rec.Offset < r.cursor {
						return
					}
					sub := r.subs[i]
					if rec.Offset < sub.Since || (sub.NoLocal && rec.Publisher == client) {
						return
					}
					if p := (partition{count: sub.PartitionCount, indices: sub.PartitionIndices}); p.declared() {
						if !hashed {
							h, hashed = channel.PartitionHash(rec.Topic), true
						}
						if !p.wants(h) {
							return
						}
					}
					owners[client] = nil
				})
			}
			// The groups owed it: returned to them, or at or after their
			// cursor, matched, and in no member's table.
			seen[rec.Offset] = true
			gowners := map[string]bool{}
			for group, r := range returned {
				if r[rec.Offset] {
					gowners[group] = true
				}
			}
			endedCount += endedReturned[rec.Offset]
			// A retained value's copy is its session's, and no group's.
			if rec.OwedTo == "" {
				gidx.EachSubscriber(rec.Topic, func(group string) {
					if _, done := gowners[group]; done || rec.Offset < cursors[group] || handedBy[group][rec.Offset] {
						return
					}
					gowners[group] = false
				})
				endedIdx.EachSubscriber(rec.Topic, func(group string) {
					if rec.Offset >= endedAtOpen[group].Cursor && !handedBy[group][rec.Offset] &&
						!slices.Contains(endedAtOpen[group].Returned, rec.Offset) {
						endedCount++
					}
				})
			}
			if len(owners)+len(gowners) == 0 {
				nobody = append(nobody, rec.Offset)
				continue
			}
			d.mu.Lock()
			d.owners[rec.Offset] = uint32(len(owners) + len(gowners))
			d.mu.Unlock()
			// **Each list is held to its bound as it is put back**, the
			// oldest giving way to the newest as a count gives way, rather
			// than once the whole log has been read (invariant 13: enforced
			// before allocation, and what a start puts back it also counts).
			// Bounded at the end, a session whose cursor one unacknowledged
			// delivery held at the log's start was owed, while the start
			// counted, every message its filter matched that anybody else
			// was still owed: measured, 117,760 entries on a list its bound
			// held to 291, and every such session multiplies it. What the
			// start keeps is the same either way - the newest that fit.
			for client, f := range owners {
				o := byClient[client].o
				o.mu.Lock()
				e := owedEntry{offset: rec.Offset, charge: charge}
				if f != nil {
					e.group = f.Group
				}
				o.insertLocked(e)
				if f != nil {
					o.markSentLocked(rec.Offset)
					o.flying[f.PacketID] = flight{entry: *f}
					o.stored[f.PacketID] = f.Offset
				}
				given := o.boundLocked(bound, rec.Offset)
				o.mu.Unlock()
				if len(given) > 0 {
					gaveUp[o] += len(given)
					d.letGo(given...)
				}
			}
			for group, back := range gowners {
				g := glists[group]
				g.mu.Lock()
				g.insertLocked(owedEntry{offset: rec.Offset, charge: charge, returned: back})
				given := g.boundLocked(bound, rec.Offset)
				g.mu.Unlock()
				if len(given) > 0 {
					d.b.counted.sharesDroppedFull.Add(uint64(len(given)))
					d.letGo(given...)
				}
			}
			// **Held again, as the start puts it back**: the counters start
			// at zero and the backlog does not, and held less drained less
			// dropped is what the groups hold (RFC 0005).
			d.b.counted.sharesHeld.Add(uint64(len(gowners)))
		}
	}
	if endedCount > 0 {
		d.droppedHanded(&d.b.counted.sharesDroppedNoMember, endedCount)
		d.b.log.Info("dropped the backlog of shared groups no session holds any more",
			"groups", len(endedAtOpen), "deliveries", endedCount)
	}
	// A returned offset the log no longer holds is forgotten with the
	// group's next write.
	for group, r := range returned {
		for off := range r {
			if !seen[off] {
				glists[group].forget = append(glists[group].forget, off)
			}
		}
	}

	// A session that gave up messages for its bound - one lowered across the
	// restart, or one owed more than it holds - is counted and settled once,
	// now the log is read, rather than once for each message it gave up.
	for o, n := range gaveUp {
		d.queueFull(o, n)
		d.settle(o)
	}

	// The table's entries for messages the log no longer holds: nothing is
	// owed under them, so they leave the table with the session's next write.
	for _, es := range onWire {
		for _, e := range es {
			if placed[e] {
				continue
			}
			o := byClient[e.client].o
			o.mu.Lock()
			o.stored[e.f.PacketID] = e.f.Offset
			o.acked = append(o.acked, e.f)
			o.mu.Unlock()
		}
	}
	d.release(nobody)
	return nil
}

// rebuiltEntry is one in-flight table entry a start found, and whose it is.
type rebuiltEntry struct {
	client string
	f      store.InFlight
}

// rebuildKey is the key one subscription is indexed under while a start
// counts the log: its session and its place among that session's
// subscriptions. A client id may hold any character but NUL is the one MQTT
// forbids in a UTF-8 string (MQTT-1.5.4-2), so it cannot be part of one.
func rebuildKey(client string, i int) string { return client + "\x00" + strconv.Itoa(i) }

func splitRebuildKey(key string) (string, int) {
	at := strings.LastIndexByte(key, 0)
	i, _ := strconv.Atoi(key[at+1:])
	return key[:at], i
}

// broadcastDrain is the drain, or nil where no session provider's log is
// attached.
func (b *Broker) broadcastDrain() *bdrain { return b.bcast.Load() }

// StartBroadcast attaches the broadcast log of the session provider s opened
// to a drain, counting again who owes what it holds (rebuild), so that a
// publish a session outliving its connection wants is kept once, in that
// log, and served to it from there (RFC 0003 "Broadcast"). s is the store the
// provider opened, not a wrapper around it. Called before any listener opens.
func (b *Broker) StartBroadcast(s SessionStore) error {
	switch p := s.(type) {
	case *store.Sessions:
		lg, err := p.Log()
		if err != nil {
			return err
		}
		return b.useBroadcastLog(lg, p)
	case *sqlite.Sessions:
		lg, err := p.Log()
		if err != nil {
			return err
		}
		return b.useBroadcastLog(lg, p)
	}
	return fmt.Errorf("saguin: a session store of type %T keeps no broadcast log", s)
}

// useBroadcastLog attaches the session provider's broadcast log and the store
// that owns it, and counts again who owes what the log holds (rebuild).
// Called before any listener opens. A store that cannot be read is an error,
// and the broker does not start on it: a drain started without its lists
// would owe every session nothing.
func (b *Broker) useBroadcastLog(lg broadcastLog, st broadcastSessions) error {
	b.mu.Lock()
	provider := b.sessionsProvider
	b.mu.Unlock()
	on := b.countStorageOn(provider)
	d := newBroadcastDrain(b, countingBroadcastLog{broadcastLog: lg, on: on},
		countingBroadcastSessions{broadcastSessions: st, on: on})
	d.provider = provider
	// The holds are asked of the log itself, and counted as its calls are.
	if h, ok := lg.(HoldStore); ok {
		d.holds = countingHolds{HoldStore: h, on: on}
	}
	if err := d.rebuild(); err != nil {
		return err
	}
	b.bcast.Store(d)
	return nil
}
