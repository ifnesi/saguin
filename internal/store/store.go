// Package store holds saguin's records in memory. It knows nothing about
// MQTT: it stores records and channel state, and the broker layer owns
// packets, sessions, and subscriptions. RFC 0004 was written from this
// and from the sqlite store beside it, rather than before either.
package store

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// A reader name says who holds a position and under what scheme, so that
// two different kinds of reader cannot collide over one name. There are two
// schemes: an MQTT session, named by its client id, and a bridge's outbound
// rule, which holds a position with no MQTT session at all.
//
// The prefix is here rather than in the broker because both a stored
// snapshot and a database keep these names, and one rule has to govern
// what is written into either.
const readerMQTT = "mqtt:"

// MQTTReader is the reader name for a durable consumer that is an MQTT
// session.
func MQTTReader(clientID string) string { return readerMQTT + clientID }

// readerBridge is the scheme for a position held by a bridge's outbound
// rule, which has no MQTT session at all.
const readerBridge = "bridge:"

// BridgeReader is the reader name an outbound bridge rule holds its
// position under.
//
// **The two schemes cannot collide, and that is the whole reason there are
// schemes.** A client is free to call itself `bridge:head-office` - MQTT
// puts almost no rule on a client id - but its position is stored under
// `mqtt:bridge:head-office`, because every client's goes through MQTTReader
// above. Without the prefix, such a client holding a durable subscription
// would write the bridge's own row: the link's position advanced or rewound
// under it, and records skipped with nothing to say so.
//
// The name given is the bridge's and one outbound rule's, so each rule
// holds its own position in each channel it reads (RFC 0002 "Bridges").
// One per bridge let the rule the peer takes carry the shared position past
// records another rule had not sent, and a restart then skipped them.
func BridgeReader(bridge string) string { return readerBridge + bridge }

// ReaderPrefixMQTT is the scheme every reader that is an MQTT session
// carries, exported so that a store expressing the rule below in SQL builds
// it from the same constant the rule itself uses.
const ReaderPrefixMQTT = readerMQTT

// ReaderPrefixBridge is the scheme every reader that is an outbound bridge
// rule carries, exported beside ReaderPrefixMQTT and for the same reason:
// a caller checking which kind of reader a name belongs to must build the
// question from the constant the names themselves are built from, rather
// than from a second copy of the string that can drift from this one.
const ReaderPrefixBridge = readerBridge

// PositionExpires reports whether a reader's stored position is subject to
// session expiry.
//
// **Only a reader that has a session, and the failure it prevents is total.**
// A position goes when `last_seen + expires_in` is past. A reader with no
// MQTT session has no expiry interval to give, so the interval is zero - and
// zero reads as "expired the moment it was written". Measured: a bridge
// position saved at offset 42, closed, and reopened was not found in either
// store, so the bridge would copy its channel from the beginning on every
// restart.
//
// Zero cannot simply be read as "never" instead, and that is why this asks
// about the reader rather than about the number: MQTT's own Session Expiry
// Interval of 0 means the session ends with the connection, so a client that
// asked for zero is a client whose position must go. The two zeroes mean
// opposite things and only the scheme tells them apart.
//
// What keeps it from being an unbounded table (invariant 13) is that a
// reader with no session is one the operator wrote in the configuration
// file: a bridge exists because a line names it, and it stops existing when
// that line goes. It is not a name a client chooses.
func PositionExpires(reader string) bool {
	return strings.HasPrefix(reader, readerMQTT)
}

// CommitStats is how a storage provider's publish transactions were closed,
// and how many records they carried.
//
// **It exists because group commit is otherwise unobservable.** A provider
// collecting publishes into shared transactions and one committing them one
// at a time hold identical records, identical offsets and identical counter
// rows; the only difference is how long a publisher waited. So an operator
// who sets a record count above the traffic loses several times the write
// rate and nothing anywhere says why - which is what these numbers are for.
//
// Records divided by the four commit counts is the average batch size, and
// it read against MaxRecords is the whole diagnosis where an interval is
// set: a batch size near one under a ceiling of hundreds means every
// transaction is waiting out the interval and collecting almost nothing.
type CommitStats struct {
	// ClosedByRecords is transactions that filled and committed at once.
	ClosedByRecords int64
	// ClosedByInterval is transactions that did not fill and waited.
	ClosedByInterval int64
	// ClosedByCommit is transactions that collected what arrived while the
	// one before them committed, on a provider collecting without waiting.
	ClosedByCommit int64
	// Unbatched is transactions carrying a single publish because the
	// provider was not asked to collect any.
	Unbatched int64
	// Records is how many records all of those carried.
	Records int64
	// MaxRecords is what the operator set, the fixed ceiling where
	// publishes are collected without waiting, or zero where they are not
	// collected - so a dashboard can graph the batch size against its
	// ceiling without being told the configuration separately.
	MaxRecords int
}

// ErrFull reports a write refused because the channel is at its size
// bound. It is distinct from any other failure because the broker owes the
// producer a different answer: 0x97 (Quota exceeded) says "the channel is
// full, try later", where 0x83 says "storage did not work". A queue at its
// bound recovers as its workers resolve records, so the producer that gets
// this can retry and will eventually be let in.
var ErrFull = errors.New("saguin: the channel is at its size bound")

// ErrProviderFull is ErrFull where the provider refused, at the max_bytes it
// holds every store on it to, rather than the store's own bound. errors.Is
// finds ErrFull in it, so every answer ErrFull gets, it gets. It is told
// apart only so the broadcast log gives way to a write its provider refused
// and not to one a store's own bound refused, which room in the log cannot
// help (RFC 0002 "Every session's state").
var ErrProviderFull error = providerFull{}

type providerFull struct{}

func (providerFull) Error() string        { return "saguin: the storage provider is at its max_bytes" }
func (providerFull) Is(target error) bool { return target == ErrFull }

// RecordSize is what a record counts against a size bound: the bytes the
// application published, and not what a store adds around them.
//
// It is one function for both stores on purpose. Two implementations
// counting differently would make one bound mean two amounts, and the
// conformance test that drives both through the same script would have
// nothing to compare.
//
// It counts what an operator can reason about - "my payloads are a
// kilobyte, so 100MiB holds about a hundred thousand of them" - which
// means the file on disk is somewhat larger than the bound. A channel
// bound is a bound on what was published; a provider bound is a bound on
// the file. They are different questions and RFC 0004 says so.
func RecordSize(r Record) int64 {
	n := int64(len(r.Payload) + len(r.Topic) + len(r.MessageID))
	for _, h := range r.Headers {
		n += int64(len(h.Key) + len(h.Value))
	}
	return n
}

// ErrBelowFloor reports a read from an offset retention has already
// removed. It is returned rather than serving the oldest survivor,
// because serving it is a silent data skip that reports success (invariant 1).
var ErrBelowFloor = errors.New("saguin: offset is below the retention floor")

// IsDeletion reports whether a stored value is a deletion rather than a
// value - a record with no payload, on a `latest` channel.
//
// **The empty payload is the marker rather than a flag beside it**, and that
// is deliberate: it is already what a deletion looks like on the wire, so a
// stored one and a published one are the same thing, and nothing can be a
// deletion in the store and a value on the way out. A flag would be a second
// source of truth for one fact.
//
// It follows that a value with a genuinely empty payload cannot be stored on
// a latest channel, which was already true: publishing one has always been
// how a client deletes.
func IsDeletion(r Record) bool { return len(r.Payload) == 0 }

// Header is one MQTT 5 User Property on a record: a name and a value, in
// the position the publisher put it.
//
// **An ordered list rather than a map, and the difference is data.** MQTT 5
// allows the same name more than once - it is the standard way to carry a
// list - and requires a server to forward the properties unaltered and in
// order. A map can express neither, and saguin lost both silently: three
// properties named `tag` arrived as one, and the survivors came back in Go
// map order, so the same record delivered twice gave a consumer two
// different lists. Measured against this broker before the change, and
// against an ordinary broadcast topic after it, where the packet is passed
// through untouched and all three `tag`s arrive in order - which is what
// made the loss a channel-only defect rather than a broker-wide one.
type Header struct {
	Key   string
	Value string
}

// Props are the MQTT 5 publish properties that are not User Properties.
//
// They cross the link the way the stamped three do, so a copy holds what its
// source held: a consumer that fails over to the copy reads the same records,
// described the same way. A record published before saguin kept them carries
// none, which is the truth about it rather than a gap.
type Props struct {
	ContentType       string
	ResponseTopic     string
	CorrelationData   []byte
	PayloadFormat     byte
	PayloadFormatFlag bool
	MessageExpiry     uint32
	Retain            bool
	// ContentTypeEmpty, ResponseTopicEmpty and CorrelationDataEmpty say the
	// property was sent with no bytes in it: present, and empty. See
	// Record.
	ContentTypeEmpty     bool
	ResponseTopicEmpty   bool
	CorrelationDataEmpty bool
	// QoS is the quality of service the publisher used. See Record.QoS.
	QoS byte
}

// Record is one durable entry in a channel.
//
// The five fields below Headers are the MQTT 5 publish properties that are
// not User Properties. A record kept none of them: they reached the broker,
// reached a subscriber of a broadcast topic, and were dropped the moment a
// channel claimed the topic - so the same client library saw one thing on
// `weather/oslo` and another on `events/thing` against the same server,
// which is the kind of difference that makes a broker hard to adopt.
//
// **Absent is not zero, for two of them.** MQTT distinguishes "no Payload
// Format Indicator" from "one saying 0, unspecified bytes", and a Message
// Expiry Interval of 0 is not the same as none. PayloadFormatFlag carries
// the first distinction the way the substrate does; MessageExpiry uses 0
// for absent, because an interval of zero and no interval at all are the
// same statement about a record that is already stored.
type Record struct {
	Offset    uint64
	MessageID string
	Topic     string
	Payload   []byte
	Headers   []Header
	Timestamp time.Time

	ContentType       string
	ResponseTopic     string
	CorrelationData   []byte
	PayloadFormat     byte
	PayloadFormatFlag bool
	MessageExpiry     uint32

	// **Present is not non-empty, for three more.** A UTF-8 string and Binary
	// Data may be zero bytes long, and [MQTT-3.3.2-15], [MQTT-3.3.2-16] and
	// [MQTT-3.3.2-20] have the server send the Response Topic, Correlation
	// Data and Content Type unaltered, so one sent empty is kept and
	// delivered empty, and not dropped for having nothing in it. True only
	// when the property was there and had no bytes; a property with bytes
	// needs no flag.
	ContentTypeEmpty     bool
	ResponseTopicEmpty   bool
	CorrelationDataEmpty bool

	// Retain is the flag the publisher set, kept so that a subscriber
	// asking for Retain As Published can be told what it was
	// (MQTT-3.3.1-13). It is not what the delivery carries: an ordinary
	// subscriber sees a live delivery with the flag down, and a `latest`
	// channel's catch-up carries it up for everybody, because that one is
	// saguin saying "this is current state" rather than repeating a
	// publisher.
	Retain bool

	// QoS is the quality of service the publisher used, kept for the same
	// reason Retain is: a delivery has to be able to say what the original
	// publish was.
	//
	// **It is what a retained message is delivered at**, capped by the
	// subscription: [MQTT-3.8.4-8] makes a delivery's QoS the lower of the
	// two, and a retained message is a *publisher's* message handed on
	// later rather than a statement of saguin's own. Without this the
	// broker had nothing to take the minimum of, so every retained value
	// went out at whatever the subscription asked for - measured against
	// Eclipse Paho's suite, three values published at QoS 0, 1 and 2 came
	// back as 2, 2, 2, and the client was left holding an exactly-once
	// exchange for a message that had been published at QoS 0.
	//
	// A `latest` channel's catch-up is the deliberate exception and takes
	// the subscription's QoS, for the reason the Retain flag above is
	// forced up on one: that delivery is saguin saying "this is current
	// state" rather than repeating a publisher (RFC 0003).
	QoS byte

	// Bridge names the bridge this record arrived on, and is empty for one
	// this broker originated. It is what stops a bridge loop: **a record
	// that arrived over a bridge is never forwarded out over a bridge**, so
	// an `out` rule skips every record carrying a name here (RFC 0002
	// "Bridges").
	//
	// **It is set by the broker that received the record and never crosses
	// the wire**, which is the whole reason the rule holds. A mark that
	// travelled would have to survive every hop - and a broker relaying over
	// MQTT 3.1.1 drops User Properties, which mosquitto's own bridge does by
	// default (`bridge_protocol_version` is `mqttv311`) - so a scheme
	// depending on one is a loop guard that fails exactly where the topology
	// is most tangled. Set locally, the protocol on the far side cannot
	// matter.
	//
	// **A field rather than a header**, because record() strips every
	// reserved `saguin-` user property a publisher sends, with no exemption
	// for the inline client. A header would need one, which is the shape the
	// replica machinery had and this is not that.
	//
	// **Cleared when a record is derived**, which today means
	// dead-lettering: a dead letter is this broker stating a new fact about
	// work that failed rather than relaying somebody's record, and a mark
	// carried onto it would stop a `__dlq` shipping anywhere - silently
	// swallowing the records an operator most needs out.
	//
	// It is stored, because an `out` rule drains a channel from a position
	// rather than watching records go past: a mark that did not survive a
	// restart would let everything already in the channel loop on the next
	// start.
	Bridge string

	// Publisher is the client id of the connection that published this
	// record, and exists for one comparison: No Local [MQTT-3.8.3-3] says a
	// record must not be forwarded to a connection whose client id equals
	// the publishing one's.
	//
	// **It is stored because every channel delivery is a read from the
	// store.** Broadcast is forwarded as it passes, so the substrate answers
	// the question from the packet in hand; a channel hands a subscriber
	// records it may have read minutes or a restart later, and by then the
	// publishing connection is a fact only the record can carry. Without it
	// a No Local subscriber on a channel was simply sent its own records -
	// silently, since nothing in MQTT reports a flag a server ignored.
	//
	// **It never leaves the broker.** It is not stamped onto a delivery, in
	// this space or any other: who published a record is not something
	// saguin tells its subscribers, and a reserved header carrying it would
	// tell every one of them about every record. The one thing it is allowed
	// to do is be compared against the client id of the connection a
	// delivery is about to go to.
	Publisher string

	// ForGroups is set on a broadcast log message that is a copy of a
	// channel's record, kept for the shared groups over that channel whose
	// members' sessions outlive their connections (RFC 0003 "Broadcast").
	// **Owed to those groups alone**, never to a session's own subscription
	// its topic matches: a durable subscriber to the channel has the record
	// from the channel already, and a start that matched the copy would send
	// it a second time.
	//
	// It never leaves the broker, as Publisher does not.
	ForGroups bool

	// OwedTo is set on a broadcast log message that is a copy of a retained
	// value delivered at QoS 1 or 2, at SUBSCRIBE, to a session that outlives
	// its connection: the one session it is owed to, so a restart sends it
	// again under its identifier until it is acknowledged (MQTT-4.4.0-1).
	// **Owed to that session alone**, never to a subscription or a group its
	// topic matches: the value was sent because of that session's SUBSCRIBE,
	// and nobody else's.
	//
	// It never leaves the broker, as Publisher does not.
	OwedTo string
}

// Props reads a record's publish properties out as a set.
func (r Record) Props() Props {
	return Props{
		ContentType: r.ContentType, ResponseTopic: r.ResponseTopic,
		CorrelationData: r.CorrelationData,
		PayloadFormat:   r.PayloadFormat, PayloadFormatFlag: r.PayloadFormatFlag,
		MessageExpiry: r.MessageExpiry, Retain: r.Retain, QoS: r.QoS,
		ContentTypeEmpty: r.ContentTypeEmpty, ResponseTopicEmpty: r.ResponseTopicEmpty,
		CorrelationDataEmpty: r.CorrelationDataEmpty,
	}
}

// SetProps writes them onto a record.
func (r *Record) SetProps(p Props) {
	r.ContentType, r.ResponseTopic, r.CorrelationData = p.ContentType, p.ResponseTopic, p.CorrelationData
	r.PayloadFormat, r.PayloadFormatFlag = p.PayloadFormat, p.PayloadFormatFlag
	r.MessageExpiry = p.MessageExpiry
	r.Retain = p.Retain
	r.QoS = p.QoS
	r.ContentTypeEmpty, r.ResponseTopicEmpty, r.CorrelationDataEmpty =
		p.ContentTypeEmpty, p.ResponseTopicEmpty, p.CorrelationDataEmpty
}

// HasProps reports whether a record carries anything a store keeps beside
// its payload, headers, identity and timestamp - so a store can write
// nothing where there is nothing, which is every record published before
// any of this existed and most after.
//
// **`Bridge` is in here although it is not a publish property**, and the
// alternative was worse. Both stores ask this one question to decide
// whether to write their side-channel at all, so a mark left out of it
// would be dropped by whichever store somebody forgot - silently, and only
// on records that carry no publish properties, which is most of them. One
// question keeps the two in step by construction.
func (r Record) HasProps() bool {
	return r.ContentType != "" || r.ResponseTopic != "" ||
		len(r.CorrelationData) > 0 || r.PayloadFormatFlag || r.MessageExpiry != 0 ||
		r.ContentTypeEmpty || r.ResponseTopicEmpty || r.CorrelationDataEmpty ||
		r.Retain || r.QoS != 0 || r.Bridge != "" || r.Publisher != "" || r.ForGroups || r.OwedTo != ""
}

// RemainingExpiry is the Message Expiry Interval to send with a delivery at
// now, and whether to send one at all.
//
// **MQTT requires the value to be decremented by the time the message has
// been waiting**, so a record replayed three days later must not repeat the
// hour its publisher wrote. Once it has run out, the interval is simply not
// sent: what saguin does *not* do is delete the record, because retention on
// a channel is the operator's (invariant 2) and a publisher quietly removing
// records from another consumer's replay is the silent gap invariant 1
// exists to prevent. RFC 0003 states that deviation rather than leaving it
// to be discovered.
func (r Record) RemainingExpiry(now time.Time) (uint32, bool) {
	if r.MessageExpiry == 0 || r.Timestamp.IsZero() {
		return 0, false
	}
	gone := now.Sub(r.Timestamp)
	if gone < 0 {
		gone = 0
	}
	left := int64(r.MessageExpiry) - int64(gone/time.Second)
	if left <= 0 {
		return 0, false
	}
	return uint32(left), true
}

// Expired reports whether the publisher's Message Expiry Interval has run
// out - false for a record whose publisher set none, which never expires
// on this clock however old it is.
//
// What that means depends on where the record is, and the split is the
// item's whole design: on a broadcast topic the retained value is
// discarded at expiry (MQTT-3.3.2-5), while on a channel the record stays
// - retention is the operator's (invariant 2) - and only the countdown a
// delivery reports reaches zero.
func (r Record) Expired(now time.Time) bool {
	if r.MessageExpiry == 0 || r.Timestamp.IsZero() {
		return false
	}
	_, ok := r.RemainingExpiry(now)
	return !ok
}

// Header returns the last value carried under a name, and whether there was
// one. Last rather than first because that is what the map this replaced
// did, so nothing that reads a single value changes its answer.
//
// It is for saguin's own `saguin-` properties, which are written once by
// the broker. A publisher's may repeat, and code that cares about all of
// them walks Headers rather than calling this.
func (r Record) Header(key string) (string, bool) {
	for i := len(r.Headers) - 1; i >= 0; i-- {
		if r.Headers[i].Key == key {
			return r.Headers[i].Value, true
		}
	}
	return "", false
}

// BroadcastLog is the name a session provider keeps its broadcast log under:
// every QoS 1 and 2 broadcast owed to a session that outlives its connection,
// written once however many sessions are owed it (RFC 0003 "Broadcast").
//
// **It is kept in an append channel's store under a reserved name, not in a
// store of its own.** What it needs - an offset never reused, a floor stored
// rather than derived, a trim that moves the floor in the same operation - is
// what a Log already is, and per-session copies of the message were a second
// implementation of exactly that. A sqlite provider keeps it in the append
// channels' tables, as it keeps the retained store in the latest channels'; a
// memory provider writes it to its own file beside the sessions
// (Dir.SaveBroadcast). It is not a channel, and nothing can subscribe to it.
//
// It holds a `/`, which is what makes it safe: a channel name is one topic
// level and may not contain one, so no configuration can name a channel that
// collides with it. The prefix is the one RFC 0002 reserves for topics saguin
// defines.
const BroadcastLog = "$saguin/broadcast"

// Log is an append-only channel: records ordered by an offset that is
// monotonic and never reused, even after removal or restart (invariant 9).
type Log struct {
	mu      sync.Mutex
	records []Record
	// start is how many records at the front have been removed and cleared.
	// Retention removes only from the front, so advancing an index costs
	// what went rather than what stayed; compact below is what eventually
	// reclaims the prefix. Nothing reads records directly - live() is the
	// one place the prefix is skipped.
	start int
	next  uint64
	// floor is stored, never recomputed as MIN(offset) over survivors -
	// a recomputed floor reads as "nothing was ever removed" on an emptied
	// channel, losing it exactly when it matters most (invariant 1).
	floor uint64

	// held is what the surviving records count against maxBytes, and
	// maxBytes is the bound or zero for none. held is maintained on every
	// write and every removal rather than recomputed, because recomputing
	// means walking the channel on a path that must not walk anything.
	//
	// Every removal decrements it in the same operation (Trim, Remove), or
	// the channel refuses writes against room it is no longer using.
	held     int64
	maxBytes int64

	// quota is the provider's bound across every channel it holds, or nil
	// when the provider has none. A channel can be inside its own bound and
	// still be refused because the provider is full, which is what sharing
	// one pool of memory means.
	quota *Quota

	// positions is where each durable consumer of this channel had got to,
	// by reader name. It lives with the records rather than with the broker
	// because it is stored where they are: a position and the records it
	// points into belong to one storage provider, and a position kept
	// somewhere else can outlive the channel it names.
	positions map[string]Position

	// gapped says this log may lose a message from the middle, which only
	// the broadcast log may: a message leaves it once nobody owes it (RFC
	// 0003 "Broadcast"). A channel's records leave only from the front,
	// because a consumer reading across a gap in the middle reports success
	// over what it never received (invariant 1).
	gapped bool

	// holes is every message Remove took out from behind the front: its slot
	// is cleared, so the payload stops being held the moment it stops being
	// counted, and keeps only its offset, so the slice stays in order for a
	// binary search. Every read skips it. A hole never sits at the front -
	// the front moves past it the moment it would - and compact drops them
	// all. Always empty on a channel.
	holes map[uint64]struct{}

	// holds are the exactly-once publishes waiting for their release, kept
	// here so the release is one operation (HeldPublish).
	holds holds
}

func NewLog() *Log {
	return &Log{next: 1, floor: 1, positions: map[string]Position{}}
}

// NewBroadcastLog is the log a session provider keeps its broadcast in
// (BroadcastLog): a Log that may also Remove.
func NewBroadcastLog() *Log {
	l := NewLog()
	l.gapped = true
	return l
}

// ErrNotTheBroadcastLog refuses Remove on a channel's log. A channel's
// records leave only from the front, by retention, because a gap in the
// middle is read straight across by a consumer that then reports success
// over records it never received (invariant 1).
var ErrNotTheBroadcastLog = errors.New("saguin: only the broadcast log lets a message go from the middle; a channel's records leave only from the front")

// ErrPastNext refuses a write that names an offset the log has not reached: a
// position past its next offset, or an in-flight entry at or past it. Each
// names a message the log never had. A reader stored past next steps over
// every record written below its position afterwards, and is never told
// (invariant 1); and a start refuses both - an entry on either provider, a
// position in a memory snapshot - so a store that kept one would be a broker
// that does not come back up. Refused as it is written instead, it is the
// caller's error, and the store stays one a start accepts.
var ErrPastNext = errors.New("saguin: past the log's next offset")

// hole reports whether Remove has taken out the message at this offset.
// Callers hold l.mu.
func (l *Log) hole(offset uint64) bool {
	if len(l.holes) == 0 {
		return false
	}
	_, gone := l.holes[offset]
	return gone
}

// Position returns where a reader had got to, and whether it has one here
// at all. A reader with no stored position starts at the beginning.
func (l *Log) Position(reader string) (Position, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p, ok := l.positions[reader]
	return p, ok, nil
}

// SavePosition stores where a reader has got to.
//
// The offset is the lowest one it has not acknowledged, never the highest
// it has been sent: acknowledgements may complete out of order, and storing
// the highest steps over a record still in flight, which the consumer then
// never receives because it resumes past the gap and reports success.
// Working that out is the caller's job; storing it is this one's.
//
// **A position past next is refused**, ErrPastNext, and one at next is a
// reader that has read everything. The first would step over whatever is
// written below it next, and a start refuses it (a snapshot's validate), so
// it is refused here, where it is a caller's error.
func (l *Log) SavePosition(p Position) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p.Offset > l.next {
		return fmt.Errorf("%q at %d, and the log's next is %d: %w", p.Reader, p.Offset, l.next, ErrPastNext)
	}
	l.positions[p.Reader] = p
	return nil
}

// DropPosition forgets a reader, and reports whether it had a position.
//
// A position outlives its session for nobody. Dropping it when the session
// is discarded is what keeps one mistyped client id from leaving a row
// behind for ever (invariant 13).
func (l *Log) DropPosition(reader string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, had := l.positions[reader]
	delete(l.positions, reader)
	return had, nil
}

// Positions returns every stored position, ordered by reader name so that
// one state writes one snapshot file.
func (l *Log) Positions() []Position {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Position, 0, len(l.positions))
	for _, p := range l.positions {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Reader < out[j].Reader })
	return out
}

// ListPositions is every stored position, furthest-behind first, capped at
// limit - and how many there are in total.
//
// **Ordered by offset rather than by name, so the cap keeps the ones worth
// looking at.** The question this answers is "which of my three hundred
// devices is behind"; a cap over an alphabetical list would return three
// hundred readers named a-something and none of the stragglers.
//
// The total is returned beside the rows because a caller shown fifty of
// three hundred and not told so believes it has seen the fleet.
func (l *Log) ListPositions(limit int) ([]Position, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Position, 0, len(l.positions))
	for _, p := range l.positions {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Offset != out[j].Offset {
			return out[i].Offset < out[j].Offset
		}
		return out[i].Reader < out[j].Reader // stable across two calls
	})
	total := len(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, total, nil
}

// SetMaxBytes bounds what the channel holds, or removes the bound at zero.
// Called once before any listener opens.
func (l *Log) SetMaxBytes(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maxBytes = n
}

// SetQuota gives the channel its provider's bound, shared with every other
// channel that provider holds.
//
// It charges what the channel already holds, because a store restored from
// a snapshot arrives full of records that are in the provider's memory
// whether or not anything counted them. Called once, before any listener
// opens, or the same records are charged twice.
//
// **Charged, never asked for.** The records are in memory already, so a
// bound they do not fit under cannot refuse them - it can only fail to count
// them. Asked for with Take, a store restored over its bound was refused and
// then held uncounted for the life of the process, and every removal from it
// gave back room the provider had never taken, uncounting every other store
// the provider holds. Charged, the provider stands over its bound and refuses
// publishes until retention or consumers free room, as a sqlite provider
// keeps the pages it has and refuses more (RFC 0004 "Enforcing a size
// bound").
func (l *Log) SetQuota(q *Quota) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.quota == q {
		// Already charged. Bounds are applied wherever a store is attached,
		// so this runs again for every channel each time any of them gets a
		// new store, and charging twice would leave a provider believing it
		// held double what it does.
		return
	}
	l.quota = q
	q.Charge(l.held + l.holds.bytes)
}

// Bytes is what the surviving records come to.
func (l *Log) Bytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

// Append assigns the next offset and stores the record, or refuses it
// because the channel is full.
//
// The bound is checked before the offset is assigned, so a refused publish
// consumes no offset. One that did would leave a hole in the channel that a
// consumer reads straight past, which is the same failure a renumbering
// makes and is not worth risking for a record nobody kept.
func (l *Log) Append(r Record) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	size := RecordSize(r)
	if l.maxBytes > 0 && l.held+size > l.maxBytes {
		return Record{}, ErrFull
	}
	// The provider's bound after the channel's, and reserved rather than
	// merely checked, so that two channels cannot both be told yes for the
	// last of the room.
	if !l.quota.Take(size) {
		return Record{}, ErrProviderFull
	}
	r.Offset = l.next
	l.next++
	l.held += size
	l.records = append(l.records, r)
	return r, nil
}

// deadLetterAppender is a log a queue can dead-letter into without being
// refused for want of capacity. It exists for exactly one caller, and it is
// unexported so that only a store in this package can satisfy it - the same
// bound the sqlite store puts on its own version, and for the same reason: a
// queue and its dead-letter channel share a provider by construction.
type deadLetterAppender interface {
	appendForDeadLetter(Record) Record
}

// appendForDeadLetter stores a record leaving a queue, and cannot refuse it.
//
// No bound is checked here - neither the channel's nor the provider's. This
// is the dead-letter move, which is not a publish, and a bound never refuses
// the operation that would relieve it (RFC 0003). Refusing leaves the queue
// holding work it can neither deliver nor be rid of.
//
// Going through Append instead is what this store used to do, and it fails
// in a way nothing reports: the move asks the provider for room *before* the
// record it removes gives any back, so a queue that had filled its own
// provider could never dead-letter at all, however much bigger the record it
// was removing than the one it writes.
//
// The bytes are still charged, to the channel and to the provider, so no
// counter drifts. The provider may then stand above its bound by what the
// move cost - one record per queue - which is the trade RFC 0003 asks for.
func (l *Log) appendForDeadLetter(r Record) Record {
	l.mu.Lock()
	defer l.mu.Unlock()

	size := RecordSize(r)
	r.Offset = l.next
	l.next++
	l.held += size
	l.quota.Charge(size)
	l.records = append(l.records, r)
	return r
}

// Trim removes the oldest records to bring the channel under maxBytes and
// to drop everything published before `before`, and reports how many went
// and what they came to. Either rule is off at its zero value.
//
// **The floor advances in the same operation as the removal**, which is the
// whole of invariant 1: a moment in which records are gone and the floor
// still covers them is a moment in which a consumer reading across it is
// served the oldest survivor and reports success over what it never got.
// Nothing here can fail partway, because the map, the counters and the
// floor all move under one lock.
//
// The bytes go back to the channel and to its provider, in that same
// operation. A removal that forgets leaves the provider believing it is
// fuller than it is, for ever, and nothing recomputes it.
//
// Age is measured against the record's own timestamp, which is broker
// receipt time. A record with no timestamp is never too old: an unset time
// read at face value is a date in 1754, which would remove the whole
// channel on the first tick.
func (l *Log) Trim(before time.Time, maxBytes int64) (removed int, freed int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	live := l.live()
	slots := 0
	for slots < len(live) {
		r := live[slots]
		// A message Remove already took out frees nothing more and is not
		// counted twice: the trim walks past its slot. Only the broadcast log
		// has any.
		if l.hole(r.Offset) {
			delete(l.holes, r.Offset)
			live[slots] = Record{}
			slots++
			continue
		}
		tooOld := !before.IsZero() && !r.Timestamp.IsZero() && r.Timestamp.Before(before)
		tooMany := maxBytes > 0 && l.held > maxBytes
		if !tooOld && !tooMany {
			break
		}
		size := RecordSize(r)
		l.held -= size
		l.quota.Give(size)
		freed += size
		// Cleared as it goes, so the payload is released here rather than at
		// whatever later moment the slice is compacted. A memory provider's
		// bound is a number about memory, so a record counted out of it has
		// to stop being reachable at the same moment.
		live[slots] = Record{}
		slots++
		removed++
	}
	if slots == 0 {
		return 0, 0, nil
	}
	l.start += slots
	l.floorToFront()
	l.compact()
	return removed, freed, nil
}

// Remove takes out the messages at the offsets given, wherever they are in
// the log, and reports how many were there and what they came to. An offset
// the log does not hold - never written, trimmed, or removed already - is
// passed over and not counted, so a message released twice changes nothing
// the second time.
//
// **next never moves, and the floor moves only when the lowest message still
// held goes**: to the next one still held, or to next when the log is left
// empty (RFC 0004). A message taken from the middle leaves a gap that no read
// serves and no offset refills (invariant 9). The bytes go back to the log
// and its provider in the same operation, as a trim's do.
//
// Only the broadcast log may, which is ErrNotTheBroadcastLog.
func (l *Log) Remove(offsets ...uint64) (removed int, freed int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.gapped {
		return 0, 0, ErrNotTheBroadcastLog
	}

	live := l.live()
	for _, off := range offsets {
		i := sort.Search(len(live), func(i int) bool { return live[i].Offset >= off })
		if i == len(live) || live[i].Offset != off || l.hole(off) {
			continue
		}
		size := RecordSize(live[i])
		l.held -= size
		l.quota.Give(size)
		freed += size
		removed++
		live[i] = Record{Offset: off}
		if l.holes == nil {
			l.holes = map[uint64]struct{}{}
		}
		l.holes[off] = struct{}{}
	}
	if removed == 0 {
		return 0, 0, nil
	}

	// The front moves past every hole it now reaches, so none sits there and
	// the floor is always a message still held.
	moved := false
	for l.start < len(l.records) && l.hole(l.records[l.start].Offset) {
		delete(l.holes, l.records[l.start].Offset)
		l.records[l.start] = Record{}
		l.start++
		moved = true
	}
	if moved {
		l.floorToFront()
	}
	l.compact()
	return removed, freed, nil
}

// floorToFront sets the floor to the lowest offset still held, or to next
// when nothing is, once a removal has moved the front. It runs in the same
// operation as the removal, which is the whole of invariant 1: a moment in
// which records are gone and the floor still covers them is a moment in which
// a consumer reading across it is served the oldest survivor and reports
// success over what it never got.
//
// On a channel, which has no gaps, the lowest offset held is the one after
// the last removed. On the broadcast log it can be further on, past messages
// that left on their own. Either way it is set from what the removal
// reached, never recomputed from the records at a start (invariant 1's third
// way back into the bug): an emptied log takes next, not "nothing removed".
//
// Callers hold l.mu.
func (l *Log) floorToFront() {
	if l.start < len(l.records) {
		l.floor = l.records[l.start].Offset
		return
	}
	l.floor = l.next
}

// live is the records that are still there: everything before start has
// been removed and cleared. Every read goes through this rather than
// touching the slice, so there is one place the dead prefix is skipped. The
// broadcast log's holes are inside it, and every read skips them too.
func (l *Log) live() []Record { return l.records[l.start:] }

// compact moves the survivors to the front, and only once the dead prefix
// and the holes together have grown to half the slice.
//
// Compacting on every trim is what the first version of this did, and it
// costs what SURVIVES rather than what went: measured at 15ms on a channel
// of 800,000 records, under the channel's own lock, for removing a single
// record. On a sweep running every second that is a stall a publisher
// feels. Amortised over half the channel it is proportional to what was
// removed instead.
//
// The holes are why the broadcast log does not delete a message out of the
// slice when it lets one go from the middle: that costs what survives behind
// it, on every release - measured at 0.78ms at 100,000 messages and 7.6ms at
// a million - in exactly the case a slow session holding the front makes
// common.
//
// Callers hold l.mu.
func (l *Log) compact() {
	dead := l.start + len(l.holes)
	if dead == 0 || dead*2 < len(l.records) {
		return
	}
	kept := l.records[l.start:]

	// The holes go in the same pass, into a fresh array sized for what is
	// actually held.
	if len(l.holes) > 0 {
		out := make([]Record, 0, 2*(len(kept)-len(l.holes))+16)
		for _, r := range kept {
			if !l.hole(r.Offset) {
				out = append(out, r)
			}
		}
		l.records, l.start, l.holes = out, 0, nil
		return
	}

	// A fresh array when the old one has grown far larger than what is left
	// in it. Copying in place keeps the capacity, so a channel that was once
	// long would hold that array for as long as it existed - the records are
	// cleared, but a hundred thousand empty structs are still memory a
	// memory provider is not counting.
	if cap(l.records) > 4*len(kept) && cap(l.records) > 1024 {
		l.records = append(make([]Record, 0, 2*len(kept)+16), kept...)
		l.start = 0
		return
	}
	n := copy(l.records, kept)
	clear(l.records[n:])
	l.records = l.records[:n]
	l.start = 0
}

// ReadFrom returns every record at or after offset. A read below the
// floor is refused (invariant 1).
func (l *Log) ReadFrom(offset uint64) ([]Record, error) {
	return l.ReadFromN(offset, 0)
}

// FirstAtOrAfter returns the offset of the earliest record the broker
// received at or after t, and whether there is one at all.
//
// **At or after, never an exact match**, because the timestamp is the
// broker's own receipt clock: many records can share a millisecond, and
// nothing guarantees the moment asked for is one any record carries.
//
// **A scan rather than a binary search**, and that is deliberate. Records
// are in offset order, and their timestamps are *almost* in that order too
// - but the clock is the wall clock, so a step backwards from NTP leaves a
// short run where they are not. A binary search over nearly-sorted data
// can land past the true first match, which would move a consumer's
// position forward over records it asked for and report success: the one
// failure a position must never produce. The scan stops at the first match,
// costs nothing on a channel whose records are all newer than t, and a seek
// is something an operator does rather than something on the publish path.
func (l *Log) FirstAtOrAfter(t time.Time) (uint64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// live() rather than records: retention removes from the front by
	// advancing an index, so the slice still holds what it removed, and
	// scanning it directly would answer with an offset below the floor.
	// Nothing seeks the broadcast log by time - only channels are asked, and
	// they have no holes - and a hole could not be the answer if something
	// did: its cleared time is before every moment but the zero one, and the
	// zero one is answered by the front, which is never a hole.
	for _, r := range l.live() {
		if !r.Timestamp.Before(t) {
			return r.Offset, true, nil
		}
	}
	return 0, false, nil
}

// ReadFromN returns at most max records at or after offset, or every such
// record when max is zero. A read below the floor is refused
// (invariant 1).
//
// The bound exists because a consumer is fed as far as its in-flight
// window allows and then re-read when a PUBACK frees a slot. Without it,
// draining a backlog of n records would copy the whole remainder n times.
func (l *Log) ReadFromN(offset uint64, max int) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < l.floor {
		return nil, ErrBelowFloor
	}

	// Bisected rather than walked from the front. Records are appended in
	// ascending offset order and never reordered, so the first one at or
	// after offset is found in log time. Walking cost time proportional to
	// everything before it, and pump calls this on every PUBACK - so a
	// consumer draining a backlog of n records a window at a time did work
	// proportional to n squared, which is invisible on a short channel and
	// is the whole of the delivery path on a long one.
	// live(), though the search would survive the whole slice: a removed
	// entry is cleared, so its offset is zero and it sorts ahead of every
	// real record, and the floor check above has already refused anything
	// inside the prefix. Both of those are true today and neither is
	// something the next reader should have to work out - this reads the
	// records that are there. A hole keeps its offset, so the slice stays in
	// order for the search, and is skipped as it is walked.
	live := l.live()
	i := sort.Search(len(live), func(i int) bool { return live[i].Offset >= offset })

	size := len(live) - i
	if max > 0 && max < size {
		size = max
	}
	out := make([]Record, 0, size)
	for _, r := range live[i:] {
		if l.hole(r.Offset) {
			continue
		}
		out = append(out, r)
		if max > 0 && len(out) == max {
			break
		}
	}
	return out, nil
}

// ReadAt returns the records held at the offsets given, in offset order and
// each once, however the offsets were given. An offset the log does not hold -
// never written, trimmed, or removed - is left out, so what went is told by
// what is missing.
//
// **Absence is the answer here, not a refusal**, which is why a channel's
// consumer never reads this way: it reads in order from its position, and a
// read below the floor is refused rather than answered (invariant 1). The
// broadcast log's drain reads the offsets its session is owed, which may be a
// few among many other sessions' messages, and has to learn which of them the
// log no longer holds.
func (l *Log) ReadAt(offsets ...uint64) ([]Record, error) {
	want := slices.Clone(offsets)
	slices.Sort(want)
	want = slices.Compact(want)

	l.mu.Lock()
	defer l.mu.Unlock()
	live := l.live()
	out := make([]Record, 0, len(want))
	for _, off := range want {
		i := sort.Search(len(live), func(i int) bool { return live[i].Offset >= off })
		if i == len(live) || live[i].Offset != off || l.hole(off) {
			continue
		}
		out = append(out, live[i])
	}
	return out, nil
}

// Next returns the offset the next appended record will take.
func (l *Log) Next() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// Floor returns the oldest offset still readable.
func (l *Log) Floor() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.floor
}

// Len returns how many records survive.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.live()) - len(l.holes)
}

// State is where a queue record is in its lifecycle (RFC 0003 "States").
type State int

const (
	// Available: nobody holds it.
	Available State = iota
	// Delivering: sent, occupying a worker's in-flight window, no
	// visibility deadline running.
	Delivering
	// Leased: the worker acknowledged receipt at the transport layer and
	// the visibility deadline is running.
	Leased
)

// Item is one unit of queue work.
type Item struct {
	Record
	State      State
	Attempts   int
	Epoch      uint64 // advances on every resolution; the fencing token (invariant 3)
	DeliveryID string
	Holder     string // client id of the current holder
	LeaseUntil time.Time
	FirstSeen  time.Time
	LastSeen   time.Time
}

// BackoffKind is how the gap before a returned record is offered again
// grows with the attempts already spent.
type BackoffKind int

const (
	// BackoffNone offers a returned record at once, which is right for work
	// that failed on its own and wrong for work failing because something
	// downstream is down. It is the default, so no deployment changes.
	BackoffNone BackoffKind = iota
	BackoffLinear
	BackoffExponential
)

// Backoff is how long a record waits after an attempt before it may be
// offered again.
//
// **It is asked only of a record that is available**, which is what makes
// it and the visibility timeout two halves of one fence rather than two
// clocks racing. A record a worker holds is not a candidate for delivery,
// so nothing here applies to it; the visibility timeout is what ends that
// state. Once the record is back in the queue the timeout is over, and
// this is what decides when it goes out again.
//
// The consequence worth stating, because it is the whole of RFC 0003's
// answer for a worker that says nothing: the wait runs from when the
// worker last had the record, so a record taken back by the visibility
// timeout has already served it. The two mechanisms compose rather than
// add, and neither needs to know about the other.
type Backoff struct {
	Kind BackoffKind
	Base time.Duration
}

// Wait is the gap after `attempts` have been spent, zero when there is no
// backoff or nothing has been attempted.
//
// The factor is clamped (MaxBackoffShift, MaxBackoffFactor), and the product
// is held at the longest Duration where it would pass it rather than let it
// wrap: time.Duration is int64 nanoseconds, so a nine-second base shifted 30
// is past it, and wrapped it came out negative - a record that waits no time
// at all. A wait that long is never reached - the attempt before it waits
// over a century - and saturating is what the sqlite store's query does by
// itself, since SQLite turns an integer product that overflows into a real
// one, so the two stores agree about every record at every attempt.
func (b Backoff) Wait(attempts int) time.Duration {
	if b.Kind == BackoffNone || b.Base <= 0 || attempts <= 0 {
		return 0
	}
	factor := int64(min(attempts, MaxBackoffFactor))
	if b.Kind != BackoffLinear {
		factor = int64(1) << min(attempts-1, MaxBackoffShift)
	}
	if int64(b.Base) > math.MaxInt64/factor {
		return math.MaxInt64
	}
	return b.Base * time.Duration(factor)
}

// holds reports whether this record is still waiting.
//
// A record nothing has ever delivered has no last-seen moment and no
// attempts, and is never held back - the wait is between attempts, not
// before the first one.
func (b Backoff) holds(lastSeen time.Time, attempts int, now time.Time) bool {
	if lastSeen.IsZero() {
		return false
	}
	w := b.Wait(attempts)
	return w > 0 && now.Sub(lastSeen) < w
}

// firstOf keeps the earlier of a moment already recorded and now, so that
// FirstSeen means the first attempt however many times it is stamped.
func firstOf(first, now time.Time) time.Time {
	if first.IsZero() {
		return now
	}
	return first
}

const (
	// MaxBackoffShift and MaxBackoffFactor bound the arithmetic rather than
	// the policy. `max_attempts` is what bounds the policy and the
	// configuration puts no ceiling on it, so an operator who writes a
	// thousand attempts must not thereby produce a shift that wraps.
	// They are exported because the sqlite store must clamp by exactly the
	// same numbers inside its query: two clamps that disagree are two
	// answers to "is this record ready", and the store would hand out work
	// the memory store was still holding back.
	MaxBackoffShift  = 30
	MaxBackoffFactor = 1 << 20
)

// Queue holds work for one queue channel.
type Queue struct {
	mu    sync.Mutex
	items map[uint64]*Item
	order []uint64
	next  uint64

	held     int64
	maxBytes int64
	quota    *Quota
	backoff  Backoff

	// holds are the exactly-once publishes waiting for their release
	// (HeldPublish).
	holds holds
}

// SetBackoff gives the queue its retry policy. Called once before any
// listener opens, like SetMaxBytes.
func (q *Queue) SetBackoff(b Backoff) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.backoff = b
}

// SetQuota gives the queue its provider's bound, shared with every other
// channel that provider holds. It charges what the queue already holds;
// see Log.SetQuota.
func (q *Queue) SetQuota(quota *Quota) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.quota == quota {
		return // already charged; see Log.SetQuota
	}
	q.quota = quota
	quota.Charge(q.held + q.holds.bytes)
}

// SetMaxBytes bounds the unresolved work the queue holds, or removes the
// bound at zero. Called once before any listener opens.
//
// A queue takes this and never retention: deleting unacknowledged work is
// eviction of unresolved work and is never permitted (invariant 2). So the
// bound is flow control rather than a limit - resolution is what frees the
// space, and a queue at its bound accepts work again as its workers catch
// up, with nobody acting.
func (q *Queue) SetMaxBytes(n int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxBytes = n
}

// Bytes is what the unresolved records come to.
func (q *Queue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.held
}

func NewQueue() *Queue {
	return &Queue{items: map[uint64]*Item{}, next: 1}
}

// Enqueue stores a record as available work. The error is always nil; see
// Log.Append.
func (q *Queue) Enqueue(r Record) (Record, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	size := RecordSize(r)
	if q.maxBytes > 0 && q.held+size > q.maxBytes {
		return Record{}, ErrFull
	}
	if !q.quota.Take(size) {
		return Record{}, ErrProviderFull
	}
	r.Offset = q.next
	q.next++
	q.held += size
	q.items[r.Offset] = &Item{Record: r, State: Available}
	q.order = append(q.order, r.Offset)
	return r, nil
}

// Every transition below runs under the queue's own lock, so a state
// change and its epoch advance are never observed apart. Each is a named
// operation rather than a caller-supplied closure over the items map,
// because a store that keeps its records in a database has no map to hand
// out - and because the transitions that must happen together, notably the
// dead-letter move, cannot be assembled from outside (invariant 5).

// Held identifies one delivery: which record, and the epoch that was
// current when it was handed out.
type Held struct {
	Offset uint64
	Epoch  uint64
	Holder string
}

// Offered is one record the queue has just handed out.
type Offered struct {
	Record
	DeliveryID string
	Epoch      uint64
	// Attempt is the number this delivery becomes if the worker
	// acknowledges receipt. The counter itself advances at the PUBACK,
	// never here (invariant 7).
	Attempt int
}

// Offer marks up to max available records as being delivered and returns
// what to send, in offset order.
//
// The bound is how much room the live workers actually have. Without it
// every available record is marked as being delivered on every pass, and
// each one the workers cannot take has to be handed back - a Delivery ID
// minted and an epoch spent per record per tick, over a whole channel. It
// is also what keeps a store that keeps its records on a disk from reading
// the whole channel each time.
//
// Each delivery gets a fresh Delivery ID: a redelivery of the same record
// carries a different one, which is what makes a superseded resolution
// detectable (invariant 3). It is minted here rather than by the caller so
// that no record can be marked as delivered without one.
func (q *Queue) Offer(max int, now time.Time) ([]Offered, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var out []Offered
	for _, off := range q.order {
		if len(out) >= max {
			break
		}
		it, ok := q.items[off]
		if !ok || it.State != Available {
			continue
		}
		// Skipped, never waited on. Stopping at the oldest record that is
		// still backing off would let one job nobody can process stall the
		// whole queue for the length of its own gap, which is the failure
		// the queue exists to avoid rather than a slower version of working.
		//
		// And it is asked here rather than by the caller filtering what
		// comes back: past this line the record is Delivering, and handing
		// one back is a second state change on a record that was never
		// delivered - a Delivery ID minted and an epoch spent for nothing.
		if q.backoff.holds(it.LastSeen, it.Attempts, now) {
			continue
		}
		it.State = Delivering
		it.DeliveryID = NewDeliveryID()
		out = append(out, Offered{
			Record:     it.Record,
			DeliveryID: it.DeliveryID,
			Epoch:      it.Epoch,
			Attempt:    it.Attempts + 1,
		})
	}
	return out, nil
}

// Lease starts a record's visibility deadline and counts the attempt, and
// reports the record as it now stands.
//
// It applies at the PUBACK and never at the send: while the record is
// being delivered it occupies a slot in the worker's in-flight window, and
// a deadline running at the same time is the overlap that leaks one slot
// per timeout (invariant 7). A delivery that never reaches a PUBACK burns
// no attempt, because the worker never showed it had the record.
//
// It applies only while this delivery is the current one (invariant 3).
func (q *Queue) Lease(h Held, now time.Time, visibility time.Duration) (Item, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.items[h.Offset]
	if !ok || it.Epoch != h.Epoch || it.State != Delivering {
		return Item{}, false, nil
	}
	it.State = Leased
	it.Attempts++
	it.Holder = h.Holder
	it.LeaseUntil = now.Add(visibility)
	it.LastSeen = now
	if it.FirstSeen.IsZero() {
		it.FirstSeen = now
	}
	return *it, true, nil
}

// Resolve removes an acknowledged record and reports what it was.
//
// It applies only while this delivery is the current one: a late
// acknowledgement from a worker whose lease has already been taken away
// resolves nothing, or it would delete work another worker is holding
// (invariant 3).
func (q *Queue) Resolve(h Held) (Item, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.items[h.Offset]
	if !ok || it.Epoch != h.Epoch {
		return Item{}, false, nil
	}
	// The worker answered before its acknowledgement was processed, so Lease
	// has not counted this attempt. The record is leaving the queue either
	// way and nothing durable carries the number - the only reader of it is
	// the broker's `acked` line - but zero there says "never delivered" of a
	// record a worker just did, and an operator counting attempts from those
	// lines under-counts by one for every worker built the way RFC 0003
	// recommends. It is the same rule as Release's, at the third of the
	// three sites that need it.
	out := *it
	if out.State == Delivering {
		out.Attempts++
	}
	q.remove(h.Offset)
	return out, true, nil
}

// ExpiredLeases returns every delivery whose visibility deadline has
// passed. A deadline only ever runs while a record is Leased - that is,
// after the worker's PUBACK released its in-flight slot (invariant 7).
func (q *Queue) ExpiredLeases(now time.Time) ([]Held, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var out []Held
	for off, it := range q.items {
		if it.State != Leased || it.LeaseUntil.IsZero() || it.LeaseUntil.After(now) {
			continue
		}
		out = append(out, Held{Offset: off, Epoch: it.Epoch, Holder: it.Holder})
	}
	return out, nil
}

// Appender is the one thing a queue needs of its dead-letter channel. It
// is an interface so that a queue can only ever dead-letter into a log
// kept the same way it is: both belong to one storage provider, and the
// move between them is one operation (invariant 5).
type Appender interface {
	Append(Record) (Record, error)
}

// DeadLetter is where a queue sends a record whose attempts are spent: the
// log that receives it, and how to build the record that lands there.
//
// It is passed to Release rather than held by the queue because the two
// are separate objects only in this store.
type DeadLetter struct {
	Log    Appender
	Record func(Item) Record
}

// Outcome is what became of a released record.
type Outcome struct {
	// Item is the record as it stood after the release: its attempt count
	// includes this delivery, whether or not it was dead-lettered.
	Item Item

	// DeadLettered says the record left the queue. Stored is then the
	// record as it landed in the dead-letter channel, carrying the offset
	// it was given there.
	DeadLettered bool
	Stored       Record
}

// ExpireOlderThan dead-letters every waiting job published before the given
// moment, and reports what went.
//
// This is the half of `job_expires_after` a worker cannot enforce. The
// broker checks a job's age as it hands it over, which is where "without
// further delivery" is kept - but a queue whose workers have all gone away
// is never asked for anything, so nothing there would ever expire. Since
// expiry exists for work nobody is processing, that is the case it most has
// to reach.
//
// **Only jobs that are waiting.** A record out with a worker is left alone:
// removing one is removing a record in flight to a consumer, which is the
// first thing a sweep must not do. It expires on its next offer instead, or
// when the worker hands it back.
//
// **The operator's clock and no other.** A publisher's own Message Expiry
// Interval ends no job: taking work out of a queue is the operator's
// decision and `job_expires_after` is where they make it, so a queue that
// sets none is never swept at all.
//
// The move stays here rather than in the broker because it must be atomic -
// a record leaves the queue and appears in the dead-letter channel in one
// operation or neither happens (invariant 5). Each is its own move, so a
// failure part-way leaves the ones already done and reports the rest.
func (q *Queue) ExpireOlderThan(before time.Time, dl DeadLetter) ([]Outcome, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if before.IsZero() {
		return nil, nil
	}

	target, ok := dl.Log.(deadLetterAppender)
	if !ok {
		return nil, fmt.Errorf(
			"saguin: the dead-letter channel is a %T, which this queue cannot write "+
				"without being refused for want of capacity", dl.Log)
	}

	// The order slice is walked rather than the map, so what goes is decided
	// oldest first and does not depend on map iteration order.
	var out []Outcome
	for _, off := range append([]uint64(nil), q.order...) {
		it, ok := q.items[off]
		if !ok || it.State != Available {
			continue
		}
		// An unset timestamp is never too old; see Log.Trim.
		if it.Timestamp.IsZero() || !it.Timestamp.Before(before) {
			continue
		}
		gone := *it
		gone.Epoch++
		stored := target.appendForDeadLetter(dl.Record(gone))
		q.remove(off)
		// Item is the record as it was and Stored is where it landed, kept
		// apart because Item embeds a Record: folding one into the other
		// would lose the offset it had in the queue, which is half of what
		// links a dead-lettered record back to its origin (invariant 8).
		out = append(out, Outcome{Item: gone, DeadLettered: true, Stored: stored})
	}
	return out, nil
}

// Release returns a held record to the queue, or moves it to the
// dead-letter channel when its attempts are spent, and advances the epoch
// so the Delivery ID just used resolves nothing from here on (invariant 3).
//
// answered says the worker replied about this delivery. That is evidence
// it received the record, and it may arrive before the PUBACK does - a
// client library sends the acknowledgement when its receive handler
// returns, so a worker that answers from inside the handler answers first.
// Without counting the attempt here, a record such a worker keeps handing
// back is redelivered for ever and never reaches the dead-letter channel.
//
// The removal and the arrival happen together, under one lock, so a record
// can never be in neither place - which is the outcome dead-lettering
// exists to prevent (invariant 5).
//
// The new state is worked out on a copy and applied only once the move has
// succeeded. A move refused for want of capacity leaves the record exactly
// as it was, delivery state and attempt count included, so that an attempt
// is not spent on a failure that was the broker's (RFC 0003). Nothing in
// this store can fail, but the rule belongs with the operation rather than
// with whichever store is under it.
//
// It applies only while this delivery is the current one (invariant 3).
func (q *Queue) Release(h Held, now time.Time, answered bool, maxAttempts int, dl DeadLetter) (Outcome, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.items[h.Offset]
	if !ok || it.Epoch != h.Epoch {
		return Outcome{}, false, nil
	}

	next := *it
	next.Epoch++
	next.Holder = ""
	next.DeliveryID = ""
	next.LeaseUntil = time.Time{}

	// The worker answered without its acknowledgement having been processed
	// first, so Lease has not counted the attempt yet.
	//
	// **The two timestamps follow the same rule as the count, and did not.**
	// RFC 0003 says the attempt increments when the worker is known to have
	// received the record - at the PUBACK, or when it answers, whichever
	// comes first - and only the counter obeyed it. FirstSeen and LastSeen
	// were stamped in Lease alone, so a worker that answers from inside its
	// receive handler, which is what Paho's ordering makes the ordinary
	// shape and what examples/support/worker does, left both zero. A record
	// dead-lettered that way reached its channel with no saguin-dlq-first
	// and no saguin-dlq-last: the one question anybody asks of a
	// dead-lettered record is when it started failing and when it last did,
	// and for the recommended worker shape there was no answer.
	if answered && it.State == Delivering {
		next.Attempts++
		next.FirstSeen = firstOf(next.FirstSeen, now)
	}

	// **LastSeen is when the worker last had the record, and it was when the
	// worker was last given one.** Lease stamps it at the PUBACK and the
	// branch above stamped it only for a worker that answered before its
	// PUBACK was processed - so for the ordinary shape, which acknowledges
	// receipt, works, and then answers, it went on reading as the moment the
	// job was handed out.
	//
	// It cost nothing while it was only a header. It is load-bearing now:
	// Backoff measures the gap from here, so a worker that takes twenty-five
	// seconds to fail against a downstream that is down would have had a
	// two-second gap measured from twenty-five seconds ago - expired before
	// it began, and the backoff would have shipped doing nothing in the one
	// case it exists for.
	//
	// **Only an answer moves it, and that is what makes an expired lease
	// need no rule of its own.** A record the visibility timeout takes back
	// keeps the moment it was handed out, which is already a whole timeout
	// ago, so its gap is served and it goes out on the next tick. The two
	// mechanisms compose; see Backoff.
	if answered {
		next.LastSeen = now
		next.FirstSeen = firstOf(next.FirstSeen, now)
	}

	if next.Attempts < maxAttempts {
		next.State = Available
		*it = next
		return Outcome{Item: next}, true, nil
	}

	// Not Append: the move must not be refused for want of capacity, so it
	// takes the path that cannot refuse (RFC 0003).
	target, ok := dl.Log.(deadLetterAppender)
	if !ok {
		return Outcome{}, false, fmt.Errorf(
			"saguin: the dead-letter channel is a %T, which this queue cannot write "+
				"without being refused for want of capacity", dl.Log)
	}

	// The append happens while the queue's lock is held, which is the whole
	// point: no reader can see the record gone from here and not yet there.
	// It takes the log's lock inside this one, and nothing anywhere takes
	// them in the other order, so there is no cycle to deadlock on.
	stored := target.appendForDeadLetter(dl.Record(next))
	q.remove(h.Offset)
	return Outcome{Item: next, DeadLettered: true, Stored: stored}, true, nil
}

// remove drops an item and gives back what it held. Every path that takes
// a record out of the queue goes through here - resolution and the
// dead-letter move - so the counter cannot be left behind by one of them.
// Callers hold q.mu.
func (q *Queue) remove(offset uint64) {
	if it, ok := q.items[offset]; ok {
		size := RecordSize(it.Record)
		q.held -= size
		q.quota.Give(size)
	}
	delete(q.items, offset)
	for i, o := range q.order {
		if o == offset {
			q.order = append(q.order[:i], q.order[i+1:]...)
			break
		}
	}
}

// NewDeliveryID mints an identifier for one delivery attempt. RFC 0003
// makes it opaque bytes a client echoes and never parses, at most 32 of
// them; this is 16 random bytes as hex, which is exactly 32.
//
// Exported because every store mints them and the shape is the rule rather
// than each store's business. It is minted inside the store that hands the
// record out, so that no record can be marked as delivered without one.
func NewDeliveryID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// Depth reports how many items remain, and how many are out with a worker.
// It is what saguin_queue_depth and saguin_queue_inflight are (RFC 0005).
//
// The total is the map's length, which is the depth by construction and
// cannot drift from it. The walk is for the in-flight half only, and it
// walks unresolved work rather than a history - the distinction RFC 0004
// draws where the sqlite queue sums its bytes at open: a history must not
// be scanned, a working set may be, and `max_bytes` is what keeps this one
// small.
func (q *Queue) Depth() (total, inflight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, it := range q.items {
		if it.State != Available {
			inflight++
		}
	}
	return len(q.items), inflight
}

// Unresolved is the work this queue is holding, oldest first, capped at
// limit - and how many there are in total.
//
// **It leases nothing**, which is the whole reason it exists. The only
// other way to see a queue's contents is to consume them, and a viewer that
// consumed would take work from the workers it was sent to diagnose.
//
// **The payload is not returned.** Reading records is MQTT's job, and a
// store method that handed one back would leave nothing between an
// operator's curiosity and a body of somebody's data crossing an HTTP
// listener. Stripped here rather than in the handler so that a second
// caller cannot reintroduce it by forgetting.
func (q *Queue) Unresolved(limit int) ([]Item, int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Item, 0, len(q.items))
	for _, off := range q.order {
		it, ok := q.items[off]
		if !ok {
			continue // resolved, and order keeps the gap
		}
		copy := *it
		copy.Payload = nil
		out = append(out, copy)
	}
	total := len(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, total, nil
}

// Quota is how much a memory provider may hold across every channel it
// owns, and how much it is holding.
//
// A sqlite provider needs nothing like this: SQLite bounds its own file
// with max_page_count and refuses the write itself. A memory provider has
// no engine underneath it, so the counting is saguin's - and it has to
// span channels, because what is being protected is one pool of RAM rather
// than one channel's growth.
//
// That coupling is the bound working rather than a defect in it. One
// channel filling the provider does stop the others, because they share
// the memory the number is about, and an operator who needs isolation
// configures two providers. Per-channel bounds cannot protect the box: any
// number of channels, each inside its own bound, can still exceed what the
// machine has.
//
// A store with no quota is unbounded by the provider, which is what a
// configuration that sets no max_bytes means.
type Quota struct {
	mu   sync.Mutex
	held int64
	max  int64

	// reserve is held back from publishes and spent only by the operations
	// that relieve the provider, so a bound never refuses the one thing that
	// would let it recover (RFC 0003). Carved out of max rather than added
	// to it: the provider never holds more than the operator wrote, except
	// transiently by what one relief write costs.
	//
	// The sqlite provider does the same thing with two page ceilings. Same
	// rule, coarser measurement - that store counts a file in pages and this
	// one counts published bytes exactly.
	reserve int64
}

// NewQuota bounds a provider at max bytes, of which reserve is held back for
// the operations that relieve it. A max of zero is no bound at all.
func NewQuota(max, reserve int64) *Quota { return &Quota{max: max, reserve: reserve} }

// Take reserves n bytes for a publish, or reports that they would not fit.
// The check and the reservation are one operation: two channels asking at
// once must not both be told yes for the last of the room.
//
// A publish stops at max less the reserve. What is held back is not room the
// operator loses - it is room only the operations that free the provider may
// use.
func (q *Quota) Take(n int64) bool {
	if q == nil {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.max > 0 && q.held+n > q.max-q.reserve {
		return false
	}
	q.held += n
	return true
}

// Charge adds n without checking the bound, and is how an operation that
// relieves the provider takes room it must not be refused: a bound never
// refuses the operation that would relieve it (RFC 0003) - the dead-letter
// move, and a session begun new taking its replacement's room. It is also
// how a store attached to its provider is charged for what it already holds
// (Log.SetQuota), since bytes already in memory cannot be refused.
//
// The provider can therefore stand above its bound: by one dead-lettered
// record per queue, and by whatever a restore brought back over a bound an
// operator lowered. That is the trade RFC 0003 asks for, and the alternative
// is worse in the way this design refuses everywhere: a queue holding a
// record it can neither deliver nor be rid of, with nothing saying why.
func (q *Quota) Charge(n int64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.held += n
}

// Give returns n bytes to the provider, which is what every removal owes
// it. A removal that forgets leaves the provider believing it is fuller
// than it is, for ever, and nothing recomputes it.
func (q *Quota) Give(n int64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.held -= n
	if q.held < 0 {
		// Not reachable from a correct caller, and clamped rather than
		// trusted: a negative total would let the provider hold more than
		// its bound for ever after, silently.
		q.held = 0
	}
}

// Effective reports what the bound came to: the bytes a publish may take the
// provider to, and the bytes the reserve raises that to. Said once at
// startup, so an operator who wrote 16MiB and sees publishes refused at
// something less has been told why rather than left to work it out.
func (q *Quota) Effective() (publish, full int64) {
	if q == nil || q.max == 0 {
		return 0, 0
	}
	return q.max - q.reserve, q.max
}

// MaxBytes is the bound this provider is held to, or zero when it is
// unbounded - the same "zero for no bound" RFC 0005 states for
// saguin_provider_max_bytes.
//
// It exists so that a memory provider and a sqlite one can be asked the
// same two questions by the metrics collector, which is what stopped a
// bounded provider reporting that it had no bound.
func (q *Quota) MaxBytes() int64 {
	_, full := q.Effective()
	return full
}

// OverBound reports whether the provider holds more than a publish may take
// it to, which a store restored over a lowered bound leaves it doing: every
// publish into it is refused until room frees. Said once at startup, as the
// sqlite provider says the same of its file.
func (q *Quota) OverBound() bool {
	publish, _ := q.Effective()
	return publish > 0 && q.Bytes() > publish
}

// Bytes is what the provider is holding across all of its channels.
func (q *Quota) Bytes() int64 {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.held
}

// LowestPosition is the lowest offset any durable consumer of this channel
// has stored, or zero when none has.
//
// The memory store walks its positions rather than keeping a field, and
// that is not the exception it looks like: the map is already in memory and
// bounded by the number of consumers, so this is the same walk the sqlite
// store spends a query to avoid crossing a driver. What neither does is
// read a row per consumer *per scrape* from a table, which is what RFC 0005
// refuses.
// **The count rides the same walk**, which is the whole reason it is
// published: a second method would be a second pass over the same map for a
// number this one has already seen. RFC 0005's rule is that a metric is a
// number the broker already holds or it does not ship, and this is the
// cheapest reading of that - one traversal, two answers.
func (l *Log) LowestPosition() (lowest uint64, consumers int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.positions {
		if lowest == 0 || p.Offset < lowest {
			lowest = p.Offset
		}
	}
	return lowest, len(l.positions)
}

// SQLiteEmptyBytes is what a sqlite provider's database takes before it holds
// a single record: its schema, at the 4KiB page size saguin creates it with.
// Configuration validation cannot open the file - `--check-config` touches no
// storage - so the figure is here, where both it and the sqlite package can
// see it, and a test in that package fails if a schema change moves it.
const SQLiteEmptyBytes = 22 * 4096

// SQLiteReadConnections is how many read-only connections a sqlite provider
// opens beside its one writer when read_connections says nothing, and
// SQLiteMaxReadConnections the most it accepts. Here for the reason
// SQLiteEmptyBytes is: configuration validation and the sqlite package both
// read them. What set them is at sqlite.DB.ReadConnections.
const (
	SQLiteReadConnections    = 2
	SQLiteMaxReadConnections = 8
)
