package store

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Session is one persistent MQTT session as a provider keeps it: who it
// belongs to, how long it may outlive its connection, and what it subscribed
// to. What it is owed is the broadcast log behind its cursor, and what it
// has on the wire its in-flight table (InFlight), held beside it.
//
// **A session rather than a client.** It is keyed by client id because MQTT
// keys a session that way (section 4.1), and a clean start or an expiry ends
// it whatever connection holds the id.
type Session struct {
	Client string

	// ExpiryInterval is the Session Expiry Interval the session was granted,
	// in seconds, after limits.max_session_expiry has capped what the client
	// asked for. It is what DropExpired judges a session against, so a
	// client asking for 136 years is judged against the cap rather than
	// against what it asked.
	ExpiryInterval uint32

	// DisconnectedAt is when the session's client went away, and the zero
	// time while it is connected. Expiry is measured from here.
	DisconnectedAt time.Time

	Subscriptions []SessionSubscription

	// Will is the message the broker publishes on this client's behalf if
	// its connection ends without a DISCONNECT, or nil where the client
	// armed none. It is the session's, not the connection's: a Will is
	// state MQTT keeps for a client (section 3.1.2.5), and an operator who
	// named a provider for session state named it for this too.
	Will *SessionWill
}

// SessionWill is a session's Last Will: what to publish, and when.
//
// **The delay is why it is kept rather than held.** A Will with a Will Delay
// Interval is not due when its client goes away - the interval exists so a
// device on a link that drops for four seconds is not announced dead - and a
// broker that restarts inside that wait would otherwise lose the
// announcement entirely, with the device gone and nobody ever told.
type SessionWill struct {
	Topic   string
	Payload []byte
	QoS     byte

	// Delay is the Will Delay Interval in seconds: how long after its
	// client goes away the Will is due.
	Delay uint32

	// DueAt is the moment it becomes due, set when its client went away,
	// and the zero time while that client is connected. Kept rather than
	// derived so that a broker restarting inside the wait publishes it when
	// the device has actually been gone that long, rather than starting the
	// interval again.
	DueAt time.Time

	// Props are the publish properties the Will carries, Retain among them.
	Props Props

	// User is the Will's User Properties, in the order the client sent
	// them. Props has no place for them because a record keeps them as its
	// Headers, and a Will published from here is delivered with them like
	// any other publish (RFC 0003 "Last Will").
	User []Header

	// Identity is the name the client that armed the Will authenticated as,
	// which the acl_file's rules are written about. A Will is published
	// after its client has gone, and it is judged against the rules as they
	// stand then, as that client: a reload that takes away the right to
	// write its topic takes the Will's with it.
	Identity string
}

// SessionSubscription is one filter a session holds, with every option MQTT
// carries on the SUBSCRIBE that made it and saguin's own partition
// declaration, since a restored session has to be served exactly what the
// original was.
type SessionSubscription struct {
	Filter            string `json:"filter"`
	QoS               byte   `json:"qos"`
	NoLocal           bool   `json:"no_local,omitempty"`
	RetainAsPublished bool   `json:"retain_as_published,omitempty"`
	RetainHandling    byte   `json:"retain_handling,omitempty"`
	Identifier        int    `json:"identifier,omitempty"`

	// PartitionCount and PartitionIndices are the slice the subscriber
	// declared (RFC 0003), a count of zero meaning none.
	PartitionCount   int   `json:"partition_count,omitempty"`
	PartitionIndices []int `json:"partition_indices,omitempty"`

	// Since is the broadcast log's offset when the subscription was made,
	// or 0 where the broker kept no log then. A message is owed through
	// this subscription only from here on: a subscription made later does
	// not reach back to a message already in the log, at a start as while
	// the broker runs (RFC 0003 "Broadcast").
	Since uint64 `json:"since,omitempty"`
}

// MessageState is how far a message owed to a session has got.
//
// The numbers are on disk, in a snapshot file and a database column, and
// must not change.
type MessageState uint8

const (
	// MessageQueued is a message the session is owed and has not been sent:
	// its client was away, or its window was full.
	MessageQueued MessageState = 1
	// MessageSent is a PUBLISH written to the client and not yet answered.
	// A resumed session is sent it again with DUP set (MQTT-4.4.0-1).
	MessageSent MessageState = 2
	// MessageReleased is an exactly-once delivery the client has answered
	// with a PUBREC, so what is owed is the PUBREL rather than the message
	// (MQTT-4.3.3-2).
	MessageReleased MessageState = 3
)

// InFlight is one broadcast message on the wire to a durable session: where
// it is in the broadcast log, the packet identifier it went out under, the
// QoS it went out at, and how far its exchange has got.
//
// **The packet identifier is the reason the table is kept.** A plain MQTT
// subscriber has no offset to spot a re-send by, so the identifier is its
// only duplicate check, and MQTT requires a resumed session to re-send an
// unacknowledged message under the one it first carried (MQTT-4.4.0-1) - at
// QoS 2, across a restart too. A cursor alone says where the session is, not
// what it was told.
type InFlight struct {
	Offset   uint64
	PacketID uint16
	QoS      byte
	// State is MessageSent, a PUBLISH not yet answered, or MessageReleased,
	// an exactly-once delivery answered with a PUBREC so that the PUBREL is
	// what is owed. Nothing waiting for room is on the wire, so
	// MessageQueued is never here.
	State MessageState
	// Group is the shared group that handed this message to the session,
	// by its `$share/` filter, or empty for one the session's own
	// subscriptions are owed (RFC 0003 "Broadcast").
	//
	// **It is what keeps the entry across a start.** A group lags its
	// members, so what it hands one is usually behind that member's own
	// cursor, and the start lets go of an entry behind its session's cursor
	// as one already acknowledged. A group's entry is behind the group's
	// cursor instead, which passed it in the same write that kept it
	// (HandOver), so it is kept for as long as its session is.
	Group string
}

// InFlightSize is what one in-flight entry counts against its provider's
// bound beside its group's name (InFlightBytes): its offset, identifier, QoS
// and state. One figure for both stores, for the reason RecordSize is one.
const InFlightSize = 8 + 2 + 1 + 1

// InFlightBytes is what an in-flight entry counts against its provider's
// bound: InFlightSize and the name of the group that handed it out, which
// the entry carries.
func InFlightBytes(f InFlight) int64 { return InFlightSize + int64(len(f.Group)) }

// ShareCursorSize is what a shared group's cursor counts against its
// provider's bound: the group's name and the offset.
func ShareCursorSize(group string) int64 { return int64(len(group)) + 8 }

// ShareReturnedSize is what one delivery returned to a shared group counts
// against its provider's bound: the group's name and the offset.
func ShareReturnedSize(group string) int64 { return int64(len(group)) + 8 }

// ShareGroupState is a shared group's cursor on the broadcast log and the
// deliveries returned to it: the group is owed the returned offsets, ahead
// of everything after its cursor its filter matches.
type ShareGroupState struct {
	Cursor   uint64
	Returned []uint64
}

// ErrNoShareGroup reports a shared group with no cursor: no kept session
// holds its filter, so nothing is owed to it and nothing can be handed out,
// lent or returned through it (RFC 0003 "Broadcast").
var ErrNoShareGroup = errors.New("saguin: no member of this shared group has a session that outlives its connection")

// ShareMessage is one delivery a shared group is owed while nobody takes
// it: the log's record, at the QoS it was published at, and Seq its offset
// in the log, which is the group's order.
type ShareMessage struct {
	Seq    uint64
	QoS    byte
	Record Record
}

// Dropped is what a session's ending did, beside ending it: the shared
// groups whose cursors ended with it, and what became of each delivery a
// group had handed it and it had not finished (RFC 0003 "Sessions").
type Dropped struct {
	// Groups is the groups whose cursors ended with the session (Drop).
	Groups []string
	// Returned is each QoS 1 delivery it had not acknowledged, by group, now
	// returned to that group to go to another member: MQTT 5 section 4.8.2
	// says it should.
	Returned map[string][]uint64
	// Lost is each QoS 2 delivery it had not answered with a PUBREC, which
	// MQTT-4.8.2-5 forbids sending to another member. One it had answered
	// is not here: it has the message, and only the PUBREL goes with it.
	Lost []InFlight
	// Orphaned is each delivery whose group has no cursor any more - this
	// ending ended it, or an earlier one did - and so nobody to return it to.
	Orphaned []InFlight
	// Unreturned is each QoS 1 delivery that would have been returned, where
	// the provider had no room to keep its returned row even in its reserve
	// and the session ended without it. A memory provider never has one: a
	// returned row costs less than the in-flight entry it replaces.
	Unreturned []InFlight
}

// add puts another ending's into d, as DropExpired reports several.
func (d *Dropped) add(o Dropped) {
	d.Groups = append(d.Groups, o.Groups...)
	for g, offs := range o.Returned {
		if d.Returned == nil {
			d.Returned = map[string][]uint64{}
		}
		d.Returned[g] = append(d.Returned[g], offs...)
	}
	d.Lost = append(d.Lost, o.Lost...)
	d.Orphaned = append(d.Orphaned, o.Orphaned...)
	d.Unreturned = append(d.Unreturned, o.Unreturned...)
}

// AddDropped is add for the sqlite store, which reports endings the same way.
func (d *Dropped) AddDropped(o Dropped) { d.add(o) }

// ShareGroupPrefix begins every shared group's filter, which is the name a
// group's cursor and the entries it hands out are kept under.
const ShareGroupPrefix = "$share/"

// ValidShareGroup refuses a name that is not a shared group's filter. Both
// stores ask it of a group's cursor and of an entry naming its group.
func ValidShareGroup(group string) error {
	if !strings.HasPrefix(group, ShareGroupPrefix) {
		return fmt.Errorf("%q is not a shared group's filter", group)
	}
	return nil
}

// ShareHolders is how many kept sessions hold each shared group's filter:
// the members a group's cursor is kept for. Both stores keep one beside
// their sessions, so a session's ending knows at once which groups it was
// the last member of.
type ShareHolders map[string]int

// Add counts the groups a session holds.
func (h ShareHolders) Add(sess Session) {
	for _, sub := range sess.Subscriptions {
		if strings.HasPrefix(sub.Filter, ShareGroupPrefix) {
			h[sub.Filter]++
		}
	}
}

// Remove stops counting the groups a session held.
func (h ShareHolders) Remove(sess Session) {
	for _, sub := range sess.Subscriptions {
		if !strings.HasPrefix(sub.Filter, ShareGroupPrefix) {
			continue
		}
		if h[sub.Filter]--; h[sub.Filter] <= 0 {
			delete(h, sub.Filter)
		}
	}
}

// Ending is the groups whose cursors end with sess: those it held that no
// other kept session holds. held is the groups the ending session held,
// beside those its record holds - a clean start writes the new session's
// record, holding nothing, before the session it replaces ends - and sess is
// the record as it is kept now.
func (h ShareHolders) Ending(sess Session, held []string) []string {
	mine := map[string]bool{}
	for _, sub := range sess.Subscriptions {
		if strings.HasPrefix(sub.Filter, ShareGroupPrefix) {
			mine[sub.Filter] = true
		}
	}
	var ending []string
	seen := map[string]bool{}
	for _, g := range append(slices.Sorted(maps.Keys(mine)), held...) {
		if seen[g] || !strings.HasPrefix(g, ShareGroupPrefix) {
			continue
		}
		seen[g] = true
		others := h[g]
		if mine[g] {
			others--
		}
		if others <= 0 {
			ending = append(ending, g)
		}
	}
	return ending
}

// ErrWindowFull refuses an in-flight table larger than the window it was
// written under: the client's Receive Maximum, which MQTT forbids the server
// to exceed with unacknowledged QoS 1 and 2 publishes. A table past it is a
// sender that broke that rule, or a file that cannot be true.
var ErrWindowFull = errors.New("saguin: the in-flight table would be larger than the client's Receive Maximum")

// ValidInFlight refuses an entry that cannot be on the wire: no identifier,
// a QoS that has no acknowledgement, a state that is not an exchange in
// progress, or a PUBREL owed on a delivery that was not exactly-once. Both
// stores ask it before they keep an entry, and both ask it of what they
// read back.
func ValidInFlight(f InFlight) error {
	switch {
	case f.PacketID == 0:
		return fmt.Errorf("in-flight offset %d has packet identifier 0", f.Offset)
	case f.Offset == 0:
		return fmt.Errorf("in-flight packet %d is at offset 0, before the log's first", f.PacketID)
	case f.QoS != 1 && f.QoS != 2:
		return fmt.Errorf("in-flight packet %d is at QoS %d; only QoS 1 and 2 are acknowledged", f.PacketID, f.QoS)
	case f.State != MessageSent && f.State != MessageReleased:
		return fmt.Errorf("in-flight packet %d is in state %d, not one a message on the wire can be in", f.PacketID, f.State)
	case f.State == MessageReleased && f.QoS != 2:
		return fmt.Errorf("in-flight packet %d owes a PUBREL at QoS %d", f.PacketID, f.QoS)
	case f.Group != "":
		if err := ValidShareGroup(f.Group); err != nil {
			return fmt.Errorf("in-flight packet %d: %w", f.PacketID, err)
		}
	}
	return nil
}

// ErrNoSession reports a message held for a client with no session in the
// store. A message is refused rather than kept on its own, because a message
// with no session is one nothing will ever deliver or remove.
var ErrNoSession = errors.New("saguin: no persistent session is held for this client")

// SessionSize is what a session counts against its provider's bound, beside
// what its messages count (RecordSize). One function for both stores, for
// the reason RecordSize is one.
//
// **It counts memory: what the broker holds for the session while its client
// is away, not only the strings it stores** (invariant 13). The broker keeps
// every away session's client, its subscriptions in two indexes, its
// in-flight table and its place in the broadcast drain until it expires, and
// charging the strings alone let a provider's max_bytes admit 115 to 290
// times its figure in heap. So each session is
// charged SessionEntryCost, and each filter FilterEntryCost and
// FilterLevelCost for every level after its first, since the topic index
// holds a node per level. A level another session's filter already made is
// charged again: the charge is a bound, and a node shared today is one the
// other session may let go of tomorrow.
//
// **A shared filter is charged SharedFilterCost besides**: its node holds a
// map of groups and a map of members, the broadcast drain a list for its
// group and the store the group's cursor, none of which an ordinary filter
// has. Its `$share/group/` prefix is charged as two levels already.
//
// The Will's payload is counted once, and it is held once: measured with a
// 16 KiB Will, an away session cost 16,400 bytes more than one without.
func SessionSize(s Session) int64 {
	n := SessionEntryCost + int64(len(s.Client))
	for _, sub := range s.Subscriptions {
		n += FilterEntryCost + FilterLevelCost*int64(strings.Count(sub.Filter, "/")) +
			int64(len(sub.Filter)+8*len(sub.PartitionIndices))
		if first, _, ok := strings.Cut(sub.Filter, "/"); ok && strings.EqualFold(first, "$share") {
			n += SharedFilterCost
		}
	}
	if w := s.Will; w != nil {
		n += int64(len(w.Topic) + len(w.Payload) + len(w.Props.ContentType) +
			len(w.Props.ResponseTopic) + len(w.Props.CorrelationData) + len(w.Identity))
		for _, h := range w.User {
			n += int64(len(h.Key) + len(h.Value))
		}
	}
	return n
}

// What SessionSize charges a session, a filter, and each level of a filter
// after its first, in bytes of heap. Measured on the running broker by
// TestWhatAnAwaySessionCostsIsWhatItIsCharged (internal/broker), which holds
// the heap an away session takes under what it is charged. Measured once a
// session's one filter was held without a map and the maps nearly nobody
// fills were made on first use (mqtt.Subscriptions), per away session:
// 2,959 to 2,978 with no filter; 3,728 to 3,735 with one filter of one
// level; 9,425 to 9,428 with ten; 555 to 562 more for each further level of
// a filter; 61,909 with a hundred, 589 a filter past the first. With shared
// filters, each in a group of its own: 5,289 with one, 8,037 with two and
// 24,920 with ten, 2,200 to 2,750 a filter. Before either, they were 3,127 to 3,225; 5,391 to 5,403; 17,527 to 17,746; and 605 to
// 650.
const (
	SessionEntryCost = 3500
	FilterEntryCost  = 800
	FilterLevelCost  = 600
	SharedFilterCost = 400
)

// Sessions holds persistent sessions and their in-flight tables, in memory,
// bounded by the provider's quota and kept across a graceful shutdown
// by the snapshot directory's sessions file.
type Sessions struct {
	mu    sync.Mutex
	held  map[string]*sessionEntry
	bytes int64
	quota *Quota

	// log is the provider's broadcast log (BroadcastLog), and each session's
	// cursor is its reader position on it, under MQTTReader(client). It is
	// the session store's because it is session state in the same provider:
	// a cursor is worth nothing without the log it points into, so the two
	// are written and read together (Dir.SaveSessions), and a session's
	// ending takes its cursor with it (drop).
	log *Log

	// groups is each shared group's cursor on the log, by its `$share/`
	// filter: what the group is owed is the log after it (RFC 0003
	// "Broadcast"). Kept here rather than as a reader position on the log,
	// because the log's positions are its sessions' and go with them.
	groups map[string]uint64
	// returned is, for each group, the deliveries returned to it by a
	// member's session ending (Drop): offsets behind its cursor that it is
	// owed again, and that its next hand-over of them takes out.
	returned map[string]map[uint64]struct{}
	// holders counts the kept sessions holding each group's filter; a
	// group's cursor ends with the last of them (drop).
	holders ShareHolders
	// endedAtOpen is the groups the file held that no kept session holds,
	// which RestoreSessions ended (EndedAtOpen).
	endedAtOpen map[string]ShareGroupState
}

type sessionEntry struct {
	session Session

	// window and inflight are the session's in-flight table on the
	// broadcast log (InFlight), kept beside the record rather than on
	// Session so that a Save built from the broker's view of a session
	// cannot erase them.
	window   uint16
	inflight map[uint16]InFlight
}

func NewSessions() *Sessions {
	return &Sessions{held: map[string]*sessionEntry{}, log: NewBroadcastLog(),
		groups: map[string]uint64{}, returned: map[string]map[uint64]struct{}{}, holders: ShareHolders{}}
}

// Log is the provider's broadcast log, whose reader positions are the
// sessions' cursors. The error is always nil; it is there because a sqlite
// provider's can fail.
func (s *Sessions) Log() (*Log, error) { return s.log, nil }

// SetQuota gives the store its provider's bound, shared with everything else
// that provider holds, and charges what is already there; see Log.SetQuota.
func (s *Sessions) SetQuota(q *Quota) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.quota == q {
		return
	}
	s.quota = q
	q.Charge(s.bytes)
	// The log's messages are held in the same provider, so they count
	// against the same bound.
	s.log.SetQuota(q)
}

// Save keeps a session, replacing what was kept under its client id. Its
// messages are untouched. ErrFull leaves the previous session as it was.
func (s *Sessions) Save(sess Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.save(sess)
}

// save is Save, for a caller holding s.mu.
func (s *Sessions) save(sess Session) error { return s.saveCharging(sess, 0) }

// saveCharging is save, taking extra from the quota in the same step as the
// record's own growth, so that ErrProviderFull for the two together leaves
// both as they were. The caller counts extra in s.bytes once it has applied
// what it is for.
func (s *Sessions) saveCharging(sess Session, extra int64) error {
	sess = copySession(sess)
	size := SessionSize(sess)
	e, ok := s.held[sess.Client]
	var old int64
	if ok {
		old = SessionSize(e.session)
	}
	if need := size - old + extra; need > 0 && !s.quota.Take(need) {
		return ErrProviderFull
	} else if need < 0 {
		s.quota.Give(-need)
	}
	s.bytes += size - old
	s.holders.Add(sess)
	if ok {
		s.holders.Remove(e.session)
		e.session = sess
		return nil
	}
	s.held[sess.Client] = &sessionEntry{session: sess}
	return nil
}

// Disconnected records when a session's client went away, the expiry
// interval in force as it went - a DISCONNECT may have changed the one its
// CONNECT was granted - and, with dropWill, that its Will is withdrawn, and
// gives back what the Will took. ErrNoSession where the client has no
// session, and then nothing changes.
//
// **One step, with nothing read first by the caller.** Recording a disconnect
// was a Get and then a Save, and a Get that failed returned before anything
// was written: the record went on saying its client was connected with its
// Will armed, and a Will withdrawn by a clean DISCONNECT was published when
// the session ended (MQTT-3.1.2-10).
func (s *Sessions) Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.held[client]
	if !ok {
		return ErrNoSession
	}
	e.session.DisconnectedAt = at
	e.session.ExpiryInterval = expiry
	if dropWill && e.session.Will != nil {
		old := SessionSize(e.session)
		e.session.Will = nil
		freed := old - SessionSize(e.session)
		s.quota.Give(freed)
		s.bytes -= freed
	}
	return nil
}

// Get returns the session kept for a client id.
func (s *Sessions) Get(client string) (Session, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.held[client]
	if !ok {
		return Session{}, false, nil
	}
	return copySession(e.session), true, nil
}

// All returns every session, ordered by client id.
func (s *Sessions) All() ([]Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Session, 0, len(s.held))
	for _, e := range s.held {
		out = append(out, copySession(e.session))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Client < out[j].Client })
	return out, nil
}

// Drop ends a session: it and every message it is owed go together, and the
// count is how many messages went. Dropping a client with no session is not
// an error.
//
// **The cursor of every shared group it was the last member of goes with
// it**, in the same step, and those groups are named: a group's cursor is
// kept for the sessions that hold its filter (RFC 0003 "Sessions"), and one
// left with none would hold what it is owed in the log for ever, since no
// member is left to take it. held is the groups the ending session held,
// which its record may no longer say: a clean start writes the new
// session's record before the one it replaces ends (ShareHolders.Ending).
//
// **What a group had handed it and it had not finished is split as MQTT 5
// section 4.8.2 splits it**, in the same step (Dropped): a QoS 1 delivery is
// returned to its group, a QoS 2 one awaiting its PUBREC is lost, and one
// whose group has no cursor any more is orphaned.
func (s *Sessions) Drop(client string, held []string) (Dropped, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drop(client, held, nil), nil
}

// DropExpired ends every session whose client has been away for longer than
// its expiry interval, and names them, and the shared groups whose cursors
// went with them (Drop), as its record holds them: nothing writes over an
// expired session's record. A connected session is never expired, and one
// keep answers true for, where keep is not nil, is left for the caller.
func (s *Sessions) DropExpired(now time.Time, keep func(Session) bool) ([]string, Dropped, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var gone []string
	for client, e := range s.held {
		if SessionExpired(e.session, now) && (keep == nil || !keep(e.session)) {
			gone = append(gone, client)
		}
	}
	sort.Strings(gone)
	var all Dropped
	for _, client := range gone {
		all.add(s.drop(client, nil, nil))
	}
	return gone, all, nil
}

// SessionExpired reports whether a session's client has been away for its
// whole expiry interval. One function for both stores, so that a session
// cannot outlive a restart on one provider and not the other.
func SessionExpired(sess Session, now time.Time) bool {
	if sess.DisconnectedAt.IsZero() {
		return false
	}
	return !now.Before(sess.DisconnectedAt.Add(time.Duration(sess.ExpiryInterval) * time.Second))
}

// SetInFlight keeps one entry of a session's in-flight table - a message sent,
// or one whose exchange has moved on - under its packet identifier, and
// records the window it was written under, the client's Receive Maximum.
// ErrNoSession where the client has no session, ErrWindowFull where the table
// would outgrow that window, ErrPastNext for an entry at or past the broadcast
// log's next offset, and an invalid entry is refused; in every case the table
// stays as it was. Memory charges the provider for a new entry and is refused
// ErrFull at its bound.
func (s *Sessions) SetInFlight(client string, window uint16, f InFlight) error {
	return s.SetInFlightAll(client, window, []InFlight{f})
}

// SetInFlightAll keeps several entries of a session's in-flight table in one
// step, each as SetInFlight keeps one, and all of them or none: a refusal of
// any entry, of the window, or of the provider's bound leaves the table as it
// was. It is how a batch of deliveries is recorded before it is written, and
// one identifier given twice in one call is refused as a message the batch
// would have put on the wire under two. An empty call changes nothing, the
// window included.
func (s *Sessions) SetInFlightAll(client string, window uint16, fs []InFlight) error {
	if len(fs) == 0 {
		return nil
	}
	if err := ValidInFlightAll(fs); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setInFlightLocked(client, window, fs, 0)
}

// setInFlightLocked is SetInFlightAll's body, with s.mu held and fs already
// valid. extra is what the caller writes in the same step and is charged
// with it, so one refusal of the bound refuses both. It changes nothing
// unless it answers nil.
func (s *Sessions) setInFlightLocked(client string, window uint16, fs []InFlight, extra int64) error {
	e, ok := s.held[client]
	if !ok {
		return ErrNoSession
	}
	next := s.log.Next()
	fresh := 0
	cost := extra
	for _, f := range fs {
		if f.Offset >= next {
			return pastNext(f, next)
		}
		old, had := e.inflight[f.PacketID]
		if !had {
			fresh++
		} else {
			cost -= InFlightBytes(old)
		}
		cost += InFlightBytes(f)
	}
	if len(e.inflight)+fresh > int(window) {
		return ErrWindowFull
	}
	switch {
	case cost > 0:
		if !s.quota.Take(cost) {
			return ErrProviderFull
		}
	case cost < 0:
		s.quota.Give(-cost)
	}
	s.bytes += cost
	if e.inflight == nil {
		e.inflight = map[uint16]InFlight{}
	}
	for _, f := range fs {
		e.inflight[f.PacketID] = f
	}
	e.window = window
	return nil
}

// pastNext is the refusal of an in-flight entry at or past the log's next
// offset, which names a message the log has never had (ErrPastNext).
func pastNext(f InFlight, next uint64) error {
	return fmt.Errorf("in-flight packet %d is at offset %d, and the broadcast log's next is %d: %w",
		f.PacketID, f.Offset, next, ErrPastNext)
}

// PastNext is pastNext for the sqlite store, which refuses the same entries.
func PastNext(f InFlight, next uint64) error { return pastNext(f, next) }

// ValidInFlightAll asks ValidInFlight of every entry of one write, and
// refuses an identifier given twice in it. Both stores ask it before they
// write anything.
func ValidInFlightAll(fs []InFlight) error {
	seen := make(map[uint16]struct{}, len(fs))
	for _, f := range fs {
		if err := ValidInFlight(f); err != nil {
			return err
		}
		if _, twice := seen[f.PacketID]; twice {
			return fmt.Errorf("in-flight packet %d is given twice in one write", f.PacketID)
		}
		seen[f.PacketID] = struct{}{}
	}
	return nil
}

// Acknowledge records what a session's acknowledgements settled, in one step:
// each entry given leaves its in-flight table where the table still holds it
// under that identifier at that offset, and the session's cursor is saved at
// cursor. It reports how many entries went. ErrNoSession where the client has
// no session, and ErrPastNext for a cursor past the log's next offset; either
// way nothing is written.
//
// **The identifier and the offset together name an entry.** An identifier is
// free again once its acknowledgement arrives, and a delivery made under it
// afterwards may be recorded before this runs; a removal by identifier alone
// would take that newer entry, and a message on the wire would then have no
// entry to be sent again from.
//
// **The table and the cursor move together**, so no start finds one moved and
// not the other. The start already drops an entry its cursor has passed; this
// is what keeps the other half - an entry gone, the cursor not moved over it -
// from ever being on disk, where the message would be sent again as a new one.
func (s *Sessions) Acknowledge(client string, cursor uint64, done []InFlight) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.held[client]
	if !ok {
		return 0, ErrNoSession
	}
	// The cursor first, because it is the half that can be refused: a cursor
	// past next leaves the table as it was.
	if err := s.log.SavePosition(Position{Reader: MQTTReader(client), Offset: cursor, LastSeen: time.Now()}); err != nil {
		return 0, err
	}
	n := 0
	var freed int64
	for _, f := range done {
		if held, had := e.inflight[f.PacketID]; had && held.Offset == f.Offset {
			delete(e.inflight, f.PacketID)
			freed += InFlightBytes(held)
			n++
		}
	}
	s.bytes -= freed
	s.quota.Give(freed)
	return n, nil
}

// ClearInFlight takes one entry out of a session's in-flight table, which is
// what an acknowledgement does, and reports whether it was there.
func (s *Sessions) ClearInFlight(client string, packetID uint16) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.held[client]
	if !ok {
		return false, nil
	}
	held, had := e.inflight[packetID]
	if !had {
		return false, nil
	}
	delete(e.inflight, packetID)
	s.bytes -= InFlightBytes(held)
	s.quota.Give(InFlightBytes(held))
	return true, nil
}

// HandOver keeps entries a shared group hands one of its members in that
// session's in-flight table, as SetInFlightAll keeps them, and moves the
// group's cursor to cursor, in one step: both or neither. Every entry names
// group. It answers as SetInFlightAll does, and as SetShareCursor does for
// the cursor - ErrNoShareGroup where the group has none, so a hand-over
// finishing after the group ended cannot bring its cursor back - and a
// refusal of either leaves the table, the cursor and the count as they were.
//
// **One step, because either half alone is a message the group gives twice
// or never.** With the entry kept and the cursor not moved, a start owes the
// group the message again and it goes to a second member while the first
// still has it on the wire; with the cursor moved and the entry not kept, a
// start has nothing to send it again from, and nobody has it.
func (s *Sessions) HandOver(group string, cursor uint64, client string, window uint16, fs []InFlight) error {
	if err := validHandOver(group, fs); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.shareCursorFits(cursor); err != nil {
		return err
	}
	if _, had := s.groups[group]; !had {
		return ErrNoShareGroup
	}
	// A returned delivery handed over again leaves the returned list in
	// the same step, or a start would return it to the group a second time.
	var again []uint64
	for _, f := range fs {
		if _, ok := s.returned[group][f.Offset]; ok {
			again = append(again, f.Offset)
		}
	}
	freed := ShareReturnedSize(group) * int64(len(again))
	if err := s.setInFlightLocked(client, window, fs, -freed); err != nil {
		return err
	}
	s.forgetLocked(group, again)
	s.groups[group] = cursor
	return nil
}

// forgetLocked takes offsets off a group's returned list without counting
// them: the caller has. Called with s.mu held.
func (s *Sessions) forgetLocked(group string, offsets []uint64) {
	r := s.returned[group]
	for _, off := range offsets {
		delete(r, off)
	}
	if len(r) == 0 {
		delete(s.returned, group)
	}
}

// Return undoes a hand-over whose delivery never reached its member: the
// entries leave the member's in-flight table, each named by its identifier
// and offset, and go on its group's returned list, in one step. Every entry
// names group. **ErrNoShareGroup where the group has no cursor**, and
// nothing is written: with nobody to return them to, the caller lets them go
// as the member's own. ErrNoSession where the client has no session.
func (s *Sessions) Return(client, group string, fs []InFlight) error {
	if err := validHandOver(group, fs); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.held[client]
	if !ok {
		return ErrNoSession
	}
	if _, had := s.groups[group]; !had {
		return ErrNoShareGroup
	}
	var freed int64
	for _, f := range fs {
		if held, had := e.inflight[f.PacketID]; had && held.Offset == f.Offset {
			delete(e.inflight, f.PacketID)
			freed += InFlightBytes(held)
		}
	}
	// The returned rows are written in the room the entries give back,
	// which is more, so a return is never refused for room.
	freed -= s.returnLocked(group, offsetsOf(fs))
	s.bytes -= freed
	s.quota.Give(freed)
	return nil
}

// Lend is a hand-over to a member whose session the store does not keep: the
// offsets go on the group's returned list and its cursor moves to cursor, in
// one step. A restart ends every such session, and the returned list then
// gives the group back what it was in flight to them, as a session's ending
// does (RFC 0003 "Broadcast"); an acknowledgement takes it out
// (SetShareCursor's forget). ErrNoShareGroup where the group has no cursor.
func (s *Sessions) Lend(group string, cursor uint64, offsets []uint64) error {
	if err := ValidShareGroup(group); err != nil {
		return err
	}
	for _, off := range offsets {
		if off == 0 {
			return fmt.Errorf("a shared group lends offset 0, before the log's first")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.shareCursorFits(cursor); err != nil {
		return err
	}
	if _, had := s.groups[group]; !had {
		return ErrNoShareGroup
	}
	for _, off := range offsets {
		if off >= s.log.Next() {
			return fmt.Errorf("a shared group lends offset %d, and the broadcast log's next is %d: %w",
				off, s.log.Next(), ErrPastNext)
		}
	}
	var cost int64
	for _, off := range offsets {
		if _, had := s.returned[group][off]; !had {
			cost += ShareReturnedSize(group)
		}
	}
	if !s.quota.Take(cost) {
		return ErrProviderFull
	}
	s.bytes += s.returnLocked(group, offsets)
	s.groups[group] = cursor
	return nil
}

// returnLocked puts offsets on a group's returned list and answers what the
// new ones cost, which the caller charges the provider. Called with s.mu
// held.
func (s *Sessions) returnLocked(group string, offsets []uint64) int64 {
	if s.returned[group] == nil {
		s.returned[group] = map[uint64]struct{}{}
	}
	var cost int64
	for _, off := range offsets {
		if _, had := s.returned[group][off]; !had {
			s.returned[group][off] = struct{}{}
			cost += ShareReturnedSize(group)
		}
	}
	if len(s.returned[group]) == 0 {
		delete(s.returned, group)
	}
	return cost
}

// EndShareCursorIfUnheld ends a shared group's cursor and its returned list
// where no kept session holds the group's filter, and reports whether it
// did: the question and the ending in one step, so a member subscribing
// meanwhile is never left without the cursor it holds.
func (s *Sessions) EndShareCursorIfUnheld(group string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, had := s.groups[group]; !had || s.holders[group] > 0 {
		return false, nil
	}
	s.endGroupLocked(group)
	return true, nil
}

func offsetsOf(fs []InFlight) []uint64 {
	out := make([]uint64, len(fs))
	for i, f := range fs {
		out[i] = f.Offset
	}
	return out
}

// validHandOver refuses a hand-over no group could make: none at all,
// entries that cannot be on the wire, or ones that name another group or
// none.
func validHandOver(group string, fs []InFlight) error {
	if err := ValidShareGroup(group); err != nil {
		return err
	}
	if len(fs) == 0 {
		return fmt.Errorf("a hand-over from %q hands over nothing", group)
	}
	if err := ValidInFlightAll(fs); err != nil {
		return err
	}
	for _, f := range fs {
		if f.Group != group {
			return fmt.Errorf("in-flight packet %d names group %q in a hand-over from %q", f.PacketID, f.Group, group)
		}
	}
	return nil
}

// ValidHandOver is validHandOver for the sqlite store, which refuses the same
// hand-overs.
func ValidHandOver(group string, fs []InFlight) error { return validHandOver(group, fs) }

// shareCursorFits refuses a group's cursor that cannot point into the log: 0,
// before its first offset, or past its next, a message it has never had
// (ErrPastNext). A cursor at next is a group owed nothing yet. Called with
// s.mu held.
func (s *Sessions) shareCursorFits(cursor uint64) error {
	if cursor == 0 {
		return fmt.Errorf("a shared group's cursor at offset 0, before the log's first")
	}
	if next := s.log.Next(); cursor > next {
		return fmt.Errorf("a shared group's cursor at %d, and the broadcast log's next is %d: %w",
			cursor, next, ErrPastNext)
	}
	return nil
}

// CreateShareCursor gives a shared group a cursor on the broadcast log at
// cursor, charged to the provider and refused ErrProviderFull at its bound. A
// group that has one keeps it, unmoved. **Refused ErrNoShareGroup where no
// kept session holds the group's filter**, so no cursor is ever made for a
// group with no member to take what it is owed (Drop). A cursor at 0 or past
// the log's next offset is refused, and a name that is not a shared group's
// filter.
func (s *Sessions) CreateShareCursor(group string, cursor uint64) error {
	if err := ValidShareGroup(group); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.shareCursorFits(cursor); err != nil {
		return err
	}
	if s.holders[group] <= 0 {
		return ErrNoShareGroup
	}
	if _, had := s.groups[group]; had {
		return nil
	}
	cost := ShareCursorSize(group)
	if !s.quota.Take(cost) {
		return ErrProviderFull
	}
	s.bytes += cost
	s.groups[group] = cursor
	return nil
}

// SaveWithShareCursors keeps a session as Save does and gives each of groups
// a cursor at cursor where it has none, as CreateShareCursor does, in one
// step: both, or neither. A durable member's SUBSCRIBE to a group with no
// cursor is this one write, so a cursor the store refuses refuses the record
// too and the member is not left holding a filter its group keeps nothing
// for. **The quota is asked once, for the record's growth and the cursors
// together, before either is changed.** Refused ErrNoShareGroup for a group
// whose filter sess does not hold.
func (s *Sessions) SaveWithShareCursors(sess Session, cursor uint64, groups []string) error {
	for _, g := range groups {
		if err := ValidShareGroup(g); err != nil {
			return err
		}
		if !slices.ContainsFunc(sess.Subscriptions, func(sub SessionSubscription) bool { return sub.Filter == g }) {
			return ErrNoShareGroup
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(groups) > 0 {
		if err := s.shareCursorFits(cursor); err != nil {
			return err
		}
	}
	var made []string
	var cost int64
	for _, g := range groups {
		if _, had := s.groups[g]; had || slices.Contains(made, g) {
			continue
		}
		made = append(made, g)
		cost += ShareCursorSize(g)
	}
	if err := s.saveCharging(sess, cost); err != nil {
		return err
	}
	s.bytes += cost
	for _, g := range made {
		s.groups[g] = cursor
	}
	return nil
}

// SetShareCursor moves a shared group's cursor to cursor, and takes forget off
// its returned list - deliveries it let go of otherwise than by handing them
// over - in the same step. Neither charges anything, so the bound never
// refuses a group letting go of what it passed. **ErrNoShareGroup where the
// group has none**: it only moves one, so a move finishing after the group
// ended cannot bring its cursor back. A cursor at 0 or past the log's next
// offset is refused.
func (s *Sessions) SetShareCursor(group string, cursor uint64, forget ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.shareCursorFits(cursor); err != nil {
		return err
	}
	if _, had := s.groups[group]; !had {
		return ErrNoShareGroup
	}
	var gone []uint64
	for _, off := range forget {
		if _, ok := s.returned[group][off]; ok {
			gone = append(gone, off)
		}
	}
	freed := ShareReturnedSize(group) * int64(len(gone))
	s.forgetLocked(group, gone)
	s.bytes -= freed
	s.quota.Give(freed)
	s.groups[group] = cursor
	return nil
}

// DropShareCursor ends a shared group's cursor, giving its room back, and
// reports whether the group had one. What the group had handed its members
// stays in their in-flight tables: each is its member's to finish.
func (s *Sessions) DropShareCursor(group string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, had := s.groups[group]; !had {
		return false, nil
	}
	s.endGroupLocked(group)
	return true, nil
}

// endGroupLocked takes a group's cursor and its returned list out, giving
// their room back. Called with s.mu held.
func (s *Sessions) endGroupLocked(group string) {
	freed := ShareCursorSize(group) + ShareReturnedSize(group)*int64(len(s.returned[group]))
	delete(s.groups, group)
	delete(s.returned, group)
	s.bytes -= freed
	s.quota.Give(freed)
}

// ShareCursors is every shared group's cursor, by the group's filter.
func (s *Sessions) ShareCursors() (map[string]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.groups), nil
}

// ShareReturned is every shared group's returned list, by the group's
// filter, each in offset order.
func (s *Sessions) ShareReturned() (map[string][]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.returnedLocked(), nil
}

func (s *Sessions) returnedLocked() map[string][]uint64 {
	out := make(map[string][]uint64, len(s.returned))
	for g, r := range s.returned {
		out[g] = slices.Sorted(maps.Keys(r))
	}
	return out
}

// EndedAtOpen is the groups the store ended as it was opened, by the group's
// filter, because no kept session holds the group's filter: what each group
// was owed - its returned list, and the log after its cursor - is for the
// start to count as dropped (RFC 0003 "Sessions").
func (s *Sessions) EndedAtOpen() map[string]ShareGroupState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.endedAtOpen)
}

// InFlight is a session's in-flight table, ordered by offset, and the window
// it was written under. A session with none, or no session, answers an empty
// table.
func (s *Sessions) InFlight(client string) (uint16, []InFlight, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.held[client]
	if !ok {
		return 0, nil, nil
	}
	return e.window, sortedInFlight(e.inflight), nil
}

func sortedInFlight(m map[uint16]InFlight) []InFlight {
	if len(m) == 0 {
		return nil
	}
	out := make([]InFlight, 0, len(m))
	for _, f := range m {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	return out
}

// Len is how many sessions are kept.
func (s *Sessions) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.held)
}

// Bytes is what the sessions come to.
func (s *Sessions) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// Begin ends whatever is kept under client - its record, its in-flight
// table, its cursor, and the cursors of the groups it was the last
// member of, split as Drop splits them - and keeps next in its place, as one
// step; next nil keeps nothing.
//
// **A session that begins new begins with nothing an ended one left.** A
// clean start Dropped the session it replaced and then saved its own record,
// and a Drop the store refused left the ended session's in-flight table and
// cursor under the id for the new one to inherit: its window full of
// entries it never had, and after a restart those sent to it again.
//
// **Room is taken as a replacement's.** With nothing kept under the id, next
// is an ordinary Save and ErrProviderFull where it does not fit. Replacing a
// session, the ending gives back what the old one held and next is charged
// without the check, as a write that relieves the provider is: a bound never
// refuses the operation that relieves it.
func (s *Sessions) Begin(client string, held []string, next *Session) (Dropped, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if next != nil && next.Client != client {
		return Dropped{}, fmt.Errorf("saguin: a session for %q begun under %q", next.Client, client)
	}
	if _, ok := s.held[client]; !ok {
		if next == nil {
			return Dropped{}, nil
		}
		return Dropped{}, s.save(*next)
	}
	return s.drop(client, held, next), nil
}

// drop removes one session and its in-flight table and gives their room back, and
// the cursors of the shared groups it was the last member of, which it names
// (Drop has held), and keeps next in its place where it is not nil (Begin).
// Called with s.mu held.
func (s *Sessions) drop(client string, held []string, next *Session) Dropped {
	e, ok := s.held[client]
	if !ok {
		return Dropped{}
	}
	var d Dropped
	ending := s.holders.Ending(e.session, held)
	if next != nil {
		// A group the next session still holds does not end with this one.
		ending = slices.DeleteFunc(ending, func(g string) bool {
			return slices.ContainsFunc(next.Subscriptions, func(sub SessionSubscription) bool { return sub.Filter == g })
		})
	}
	for _, group := range ending {
		if _, had := s.groups[group]; had {
			s.endGroupLocked(group)
			d.Groups = append(d.Groups, group)
		}
	}
	s.holders.Remove(e.session)
	size := SessionSize(e.session)
	var returned int64
	for _, f := range sortedInFlight(e.inflight) {
		size += InFlightBytes(f)
		if f.Group == "" {
			continue
		}
		_, kept := s.groups[f.Group]
		switch {
		case f.QoS == 2 && f.State == MessageReleased:
			// Answered with a PUBREC: the member has the message.
		case !kept:
			d.Orphaned = append(d.Orphaned, f)
		case f.QoS == 1:
			if s.returned[f.Group] == nil {
				s.returned[f.Group] = map[uint64]struct{}{}
			}
			if _, had := s.returned[f.Group][f.Offset]; !had {
				s.returned[f.Group][f.Offset] = struct{}{}
				returned += ShareReturnedSize(f.Group)
			}
			if d.Returned == nil {
				d.Returned = map[string][]uint64{}
			}
			d.Returned[f.Group] = append(d.Returned[f.Group], f.Offset)
		default:
			d.Lost = append(d.Lost, f)
		}
	}
	// The returned rows are written in the room the in-flight ones give back,
	// which is more, so an ending is never refused for room.
	size -= returned
	delete(s.held, client)
	s.bytes -= size
	s.quota.Give(size)
	// The session's cursor goes with it, whichever way it ends: a cursor
	// outliving its session is a row for every client id that ever
	// subscribed, and nothing else would take it out (invariant 13).
	_, _ = s.log.DropPosition(MQTTReader(client))
	if next != nil {
		n := copySession(*next)
		size := SessionSize(n)
		s.quota.Charge(size)
		s.bytes += size
		s.holders.Add(n)
		s.held[client] = &sessionEntry{session: n}
	}
	return d
}

func copySession(sess Session) Session {
	subs := make([]SessionSubscription, len(sess.Subscriptions))
	for i, sub := range sess.Subscriptions {
		sub.PartitionIndices = append([]int(nil), sub.PartitionIndices...)
		subs[i] = sub
	}
	sess.Subscriptions = subs
	// The Will as well, and both of its byte slices: a caller that kept the
	// packet it built one from would otherwise be holding the store's copy.
	if sess.Will != nil {
		w := *sess.Will
		w.Payload = append([]byte(nil), w.Payload...)
		w.Props.CorrelationData = append([]byte(nil), w.Props.CorrelationData...)
		w.User = append([]Header(nil), w.User...)
		sess.Will = &w
	}
	return sess
}

// SessionState is one session and its in-flight table, as a snapshot file
// carries it.
type SessionState struct {
	Session Session

	// Window and InFlight are the session's in-flight table on the
	// broadcast log, ordered by offset, and the Receive Maximum it was
	// written under.
	Window   uint16
	InFlight []InFlight
}

// SessionsSnapshot is what a memory provider's sessions survive a graceful
// shutdown as. It is the whole of their durability, as a channel's snapshot
// is of the channel's (invariant 14).
//
// **The broadcast log travels with the sessions**, in its own file
// (Dir.SaveSessions): the sessions' cursors are reader positions on it, and
// either restored without the other is cursors pointing at nothing or a log
// owed to nobody.
type SessionsSnapshot struct {
	Writer    string
	WrittenAt time.Time
	Sessions  []SessionState
	// Groups is every shared group's cursor on the log, by the group's
	// filter (Sessions.SetShareCursor), and Returned each one's returned
	// list (Sessions.Drop).
	Groups   map[string]uint64
	Returned map[string][]uint64
	Log      *BroadcastSnapshot
}

// Snapshot copies out the sessions and their broadcast log together, which
// is what a shutdown writes (Dir.SaveSessions).
//
// **The in-flight tables and the groups' cursors are copied under one
// lock**, because a hand-over changes one of each in one step (HandOver): a
// file holding the entry without the cursor that passed it, or the cursor
// without the entry, is a message a start gives twice or never.
//
// **The log is not copied under that lock**, so the copy is whole only on a
// broker nothing is writing to: called by Broker.SaveSnapshots, which only
// Broker.Shutdown calls, once no client, sweep, bridge, drain or Will timer
// can run. A caller while the broker runs could save a session owed a
// message the saved log does not hold, or a log without a session's
// position past it.
func (s *Sessions) Snapshot() *SessionsSnapshot {
	next, floor, records := s.log.Export()
	holds, _ := s.log.Holds()
	s.mu.Lock()
	sessions, groups, returned := s.exportLocked(), maps.Clone(s.groups), s.returnedLocked()
	s.mu.Unlock()
	return &SessionsSnapshot{
		Sessions: sessions,
		Groups:   groups,
		Returned: returned,
		Log: &BroadcastSnapshot{Next: next, Floor: floor, Records: records, Positions: s.log.Positions(),
			Holds: holds},
	}
}

// Export copies out every session, ordered by client id.
func (s *Sessions) Export() []SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exportLocked()
}

// exportLocked is Export with s.mu held.
func (s *Sessions) exportLocked() []SessionState {
	out := make([]SessionState, 0, len(s.held))
	for _, e := range s.held {
		st := SessionState{Session: copySession(e.session), Window: e.window, InFlight: sortedInFlight(e.inflight)}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Session.Client < out[j].Session.Client })
	return out
}

// RestoreSessions builds a store from what a snapshot held: the sessions,
// and their broadcast log with every session's cursor on it.
//
// **A cursor whose session is not held is dropped here.** The two files are
// written one after the other, so a stop that crashes between them leaves a
// log holding cursors for sessions the older sessions file never had. Nothing
// else would take one out: a cursor goes with its session (drop), and this
// one has none (invariant 13). Only an MQTT session's cursor is judged so;
// a reader of another kind is not a session's to lose.
func RestoreSessions(snap *SessionsSnapshot) *Sessions {
	s := NewSessions()
	for _, st := range snap.Sessions {
		sess := copySession(st.Session)
		e := &sessionEntry{session: sess}
		s.bytes += SessionSize(sess)
		s.holders.Add(sess)
		e.window = st.Window
		for _, f := range st.InFlight {
			if e.inflight == nil {
				e.inflight = map[uint16]InFlight{}
			}
			e.inflight[f.PacketID] = f
			s.bytes += InFlightBytes(f)
		}
		s.held[sess.Client] = e
	}
	// **A cursor no kept session holds is ended here**, and named
	// (EndedAtOpen). A stop between a clean start writing its record and the
	// session it replaced ending leaves one, and so does a last member
	// unsubscribing; nothing else would take it out, and it would keep what
	// the group is owed in the log with no member to take it (RFC 0003
	// "Sessions": dropped at the start where no member is left).
	// A returned list whose group has no cursor is ended with it, for the
	// same reason.
	for group, cursor := range snap.Groups {
		if s.holders[group] <= 0 {
			s.endAtOpen(group, cursor, snap.Returned[group])
			continue
		}
		s.groups[group] = cursor
		s.bytes += ShareCursorSize(group)
	}
	for group, offs := range snap.Returned {
		if _, kept := s.groups[group]; !kept {
			if _, said := s.endedAtOpen[group]; !said {
				s.endAtOpen(group, 0, offs)
			}
			continue
		}
		r := make(map[uint64]struct{}, len(offs))
		for _, off := range offs {
			r[off] = struct{}{}
		}
		if len(r) > 0 {
			s.returned[group] = r
			s.bytes += ShareReturnedSize(group) * int64(len(r))
		}
	}
	if lg := snap.Log; lg != nil {
		s.log = RestoreBroadcastLog(lg.Next, lg.Floor, lg.Records, lg.Positions)
		s.log.RestoreHeld(lg.Holds)
		for _, p := range lg.Positions {
			client, mqtt := strings.CutPrefix(p.Reader, ReaderPrefixMQTT)
			e, held := s.held[client]
			if mqtt && !held {
				_, _ = s.log.DropPosition(p.Reader)
				continue
			}
			// **An in-flight entry behind its session's cursor was
			// acknowledged before the stop that wrote these files.** The
			// cursor and the table are written apart, so a crash between an
			// acknowledgement moving the cursor and the entry going leaves
			// one; re-sending it would be a message delivered twice. A
			// group's entry is behind the group's cursor, not its session's
			// (InFlight.Group).
			if mqtt {
				for id, f := range e.inflight {
					if f.Group == "" && f.Offset < p.Offset {
						delete(e.inflight, id)
						s.bytes -= InFlightBytes(f)
					}
				}
			}
		}
	}
	return s
}

// endAtOpen records a group RestoreSessions ended, with the cursor it had (0
// for none) and its returned list (EndedAtOpen).
func (s *Sessions) endAtOpen(group string, cursor uint64, returned []uint64) {
	if s.endedAtOpen == nil {
		s.endedAtOpen = map[string]ShareGroupState{}
	}
	s.endedAtOpen[group] = ShareGroupState{Cursor: cursor, Returned: slices.Clone(returned)}
}

// The smallest a session and its parts can be in a snapshot file, so that a
// count read out of a damaged file cannot ask for more entries than the bytes
// left could hold.
const (
	minSessionSize       = 4 + 4 + 8 + 4 + 4
	minSubscriptionSize  = 4 + 1 + 1 + 1 + 4 + 4 + 4 + 8
	minInFlightSize      = 8 + 2 + 1 + 1 + 4
	minShareCursorSize   = 4 + 8
	minShareReturnedSize = 4 + 4
)

// Encode writes the sessions file and its trailing checksum.
func (s *SessionsSnapshot) Encode(w io.Writer) error {
	sum := crc32.New(crcTable)
	e := &encoder{w: io.MultiWriter(w, sum)}
	writeHeader(e, kindSessions, s.Writer, s.WrittenAt)

	e.u32(uint32(len(s.Sessions)))
	for _, st := range s.Sessions {
		e.str(st.Session.Client)
		e.u32(st.Session.ExpiryInterval)
		e.time(st.Session.DisconnectedAt)
		e.u32(uint32(len(st.Session.Subscriptions)))
		for _, sub := range st.Session.Subscriptions {
			e.str(sub.Filter)
			e.u8(sub.QoS)
			// Bit 2 held a request for deletions, which nothing read; files
			// written then carry it set, so it is never given another meaning.
			e.u8(b2u8(sub.NoLocal) | b2u8(sub.RetainAsPublished)<<1)
			e.u8(sub.RetainHandling)
			e.u32(uint32(sub.Identifier))
			e.u32(uint32(sub.PartitionCount))
			e.u32(uint32(len(sub.PartitionIndices)))
			for _, idx := range sub.PartitionIndices {
				e.u32(uint32(idx))
			}
			e.u64(sub.Since)
		}
		// The Will. A session that armed none writes the flag byte and
		// nothing else.
		if w := st.Session.Will; w == nil {
			e.u8(0)
		} else {
			e.u8(1)
			e.str(w.Topic)
			e.blob(w.Payload)
			e.u8(w.QoS)
			e.u32(w.Delay)
			e.time(w.DueAt)
			e.str(w.Props.ContentType)
			e.str(w.Props.ResponseTopic)
			e.blob(w.Props.CorrelationData)
			e.props(w.Props)
			e.u32(w.Props.MessageExpiry)
			e.u8(b2u8(w.Props.Retain))
			e.u32(uint32(len(w.User)))
			for _, h := range w.User {
				e.str(h.Key)
				e.str(h.Value)
			}
			e.str(w.Identity)
		}
		// The in-flight table, after the Will.
		e.write([]byte{byte(st.Window), byte(st.Window >> 8)})
		e.u32(uint32(len(st.InFlight)))
		for _, f := range st.InFlight {
			e.u64(f.Offset)
			e.write([]byte{byte(f.PacketID), byte(f.PacketID >> 8)})
			e.u8(f.QoS)
			e.u8(uint8(f.State))
			e.str(f.Group)
		}
	}
	// Every shared group's cursor, after the sessions, in the groups' order
	// so that one state writes one file.
	e.u32(uint32(len(s.Groups)))
	for _, group := range slices.Sorted(maps.Keys(s.Groups)) {
		e.str(group)
		e.u64(s.Groups[group])
	}
	// And every group's returned list, after the cursors.
	e.u32(uint32(len(s.Returned)))
	for _, group := range slices.Sorted(maps.Keys(s.Returned)) {
		e.str(group)
		e.u32(uint32(len(s.Returned[group])))
		for _, off := range s.Returned[group] {
			e.u64(off)
		}
	}
	return seal(e, w, sum)
}

// DecodeSessions reads a sessions file. A file that is damaged, truncated,
// from a format this broker does not read, or that parses into something that
// cannot be true is an error: an empty start would be indistinguishable from
// a fresh install and destroy the evidence (invariant 14).
func DecodeSessions(b []byte) (*SessionsSnapshot, error) {
	h, body, err := readHeader(b, kindSessions)
	if err != nil {
		return nil, err
	}
	s := &SessionsSnapshot{Writer: h.writer, WrittenAt: h.writtenAt}
	d := &decoder{b: body}

	n := d.count(minSessionSize)
	for range n {
		var st SessionState
		st.Session.Client = d.str()
		st.Session.ExpiryInterval = d.u32()
		st.Session.DisconnectedAt = d.time()
		if subs := d.count(minSubscriptionSize); subs > 0 {
			st.Session.Subscriptions = make([]SessionSubscription, 0, subs)
			for range subs {
				var sub SessionSubscription
				sub.Filter = d.str()
				sub.QoS = d.u8()
				flags := d.u8()
				sub.NoLocal, sub.RetainAsPublished = flags&1 != 0, flags&2 != 0
				sub.RetainHandling = d.u8()
				sub.Identifier = int(d.u32())
				sub.PartitionCount = int(d.u32())
				if idx := d.count(4); idx > 0 {
					sub.PartitionIndices = make([]int, 0, idx)
					for range idx {
						sub.PartitionIndices = append(sub.PartitionIndices, int(d.u32()))
					}
				}
				sub.Since = d.u64()
				st.Session.Subscriptions = append(st.Session.Subscriptions, sub)
			}
		}
		if d.u8() == 1 {
			var w SessionWill
			w.Topic = d.str()
			w.Payload = d.blob()
			w.QoS = d.u8()
			w.Delay = d.u32()
			w.DueAt = d.time()
			w.Props.ContentType = d.str()
			w.Props.ResponseTopic = d.str()
			w.Props.CorrelationData = d.blob()
			pf := d.props()
			w.Props.PayloadFormatFlag, w.Props.PayloadFormat = pf.PayloadFormatFlag, pf.PayloadFormat
			w.Props.ContentTypeEmpty, w.Props.ResponseTopicEmpty, w.Props.CorrelationDataEmpty =
				pf.ContentTypeEmpty, pf.ResponseTopicEmpty, pf.CorrelationDataEmpty
			w.Props.MessageExpiry = d.u32()
			w.Props.Retain = d.u8() == 1
			if n := d.count(minHeaderSize); n > 0 {
				w.User = make([]Header, 0, n)
				for range n {
					k := d.str()
					w.User = append(w.User, Header{Key: k, Value: d.str()})
				}
			}
			w.Identity = d.str()
			st.Session.Will = &w
		}
		if w := d.bytes(2); len(w) == 2 {
			st.Window = uint16(w[0]) | uint16(w[1])<<8
		}
		if n := d.count(minInFlightSize); n > 0 {
			st.InFlight = make([]InFlight, 0, n)
			for range n {
				var f InFlight
				f.Offset = d.u64()
				if id := d.bytes(2); len(id) == 2 {
					f.PacketID = uint16(id[0]) | uint16(id[1])<<8
				}
				f.QoS = d.u8()
				f.State = MessageState(d.u8())
				f.Group = d.str()
				st.InFlight = append(st.InFlight, f)
			}
		}
		s.Sessions = append(s.Sessions, st)
	}
	if n := d.count(minShareCursorSize); n > 0 {
		s.Groups = make(map[string]uint64, n)
		for range n {
			group := d.str()
			cursor := d.u64()
			if _, twice := s.Groups[group]; twice && d.err == nil {
				d.err = fmt.Errorf("shared group %q has two cursors", group)
			}
			s.Groups[group] = cursor
		}
	}
	if n := d.count(minShareReturnedSize); n > 0 {
		s.Returned = make(map[string][]uint64, n)
		for range n {
			group := d.str()
			if _, twice := s.Returned[group]; twice && d.err == nil {
				d.err = fmt.Errorf("shared group %q has two returned lists", group)
			}
			var offs []uint64
			for range d.count(8) {
				offs = append(offs, d.u64())
			}
			s.Returned[group] = offs
		}
	}
	if d.err != nil {
		return nil, fmt.Errorf("%s: %w", h.describe(), d.err)
	}
	if d.i != len(body) {
		return nil, fmt.Errorf("%s: %d bytes remain after the last session", h.describe(), len(body)-d.i)
	}
	if err := validateSessions(s.Sessions); err != nil {
		return nil, fmt.Errorf("%s: %w", h.describe(), err)
	}
	for group, cursor := range s.Groups {
		if err := ValidShareGroup(group); err != nil {
			return nil, fmt.Errorf("%s: a cursor: %w", h.describe(), err)
		}
		if cursor == 0 {
			return nil, fmt.Errorf("%s: shared group %q's cursor is at offset 0, before the log's first", h.describe(), group)
		}
	}
	for group, offs := range s.Returned {
		if err := ValidShareGroup(group); err != nil {
			return nil, fmt.Errorf("%s: a returned list: %w", h.describe(), err)
		}
		seen := map[uint64]bool{}
		for _, off := range offs {
			if off == 0 || seen[off] {
				return nil, fmt.Errorf("%s: shared group %q's returned list holds offset %d at 0 or twice",
					h.describe(), group, off)
			}
			seen[off] = true
		}
	}
	return s, nil
}

// validateSessions rejects a file that parsed but cannot be true.
func validateSessions(states []SessionState) error {
	clients := map[string]bool{}
	for _, st := range states {
		c := st.Session.Client
		if c == "" {
			return fmt.Errorf("a session names no client")
		}
		if clients[c] {
			return fmt.Errorf("client %q has two sessions", c)
		}
		clients[c] = true
		for _, sub := range st.Session.Subscriptions {
			if sub.QoS > 2 || sub.RetainHandling > 2 {
				return fmt.Errorf("client %q: subscription %q has QoS %d and retain handling %d",
					c, sub.Filter, sub.QoS, sub.RetainHandling)
			}
		}
		if len(st.InFlight) > int(st.Window) {
			return fmt.Errorf("client %q: %d messages in flight under a window of %d: %w",
				c, len(st.InFlight), st.Window, ErrWindowFull)
		}
		flying := map[uint16]bool{}
		for _, f := range st.InFlight {
			if err := ValidInFlight(f); err != nil {
				return fmt.Errorf("client %q: %w", c, err)
			}
			if flying[f.PacketID] {
				return fmt.Errorf("client %q: packet identifier %d is in flight twice", c, f.PacketID)
			}
			flying[f.PacketID] = true
		}
	}
	return nil
}
