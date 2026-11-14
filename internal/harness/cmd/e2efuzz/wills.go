package main

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The Wills the run's CONNECTs carry (design.md, Flows, CONNECT, 7 and
// DISCONNECT; MQTT 5 3.1.2.5 to 3.1.2.7, 3.1.3.2 and 3.14). What is built
// is a Will with no Will Delay, published at once when its connection ends
// any way but a normal DISCONNECT, as an ordinary record of its client:
// "The Will goes after its Will Delay, or at once for MQTT 3.1.1. It is read
// from the store when it goes"; "A Will is published and removed in one
// transaction"; "0x00 discards the Will. The reason 0x04 keeps it for
// publication [...] So does any other reason code, as a lost connection
// does"; "An MQTT 3.1.1 DISCONNECT always discards the Will". A refused
// CONNECT stores nothing, its Will included (CONNECT, 2 and 7).
//
// A Will Delay of a second or two (MQTT 5 3.1.2.11.3, 3.1.3.2.2) holds a
// Will back while its session outlives its connection: "The Will goes after
// its Will Delay [...] A reconnect in time cancels it. If the session ends
// first, the Will goes early" (DISCONNECT); a session of expiry 0, or a
// DISCONNECT setting it to 0, ends with its connection, so its Will goes at
// once. A reconnect is in time when the core takes its CONNECT before the due
// time (CONNECT, 5): keeping the session cancels the Will, a clean start
// publishes it at once (CONNECT, 6). A takeover publishes the old
// connection's Will at once unless it has a delay and the session is kept,
// which cancels it (CONNECT, 5 and 6). The client knows the due time only
// within a window, from the start of the step that ended the connection to
// its reading the broker's close, plus the delay: a reconnect it is not sure
// fell on one side of the due time may have cancelled the Will or not, and is
// checked for neither (reconnectWill). Session expiry and the restart are out
// of a run's reach. The run sets no retained.max_bytes, so neither a retained
// Will refused at CONNECT for it (0x97) nor one dropped at firing for it
// arises.
//
// Each Will's topic begins with its tag, "w" and the CONNECT's step, which
// no other topic of the run holds, so its record and its deliveries are
// known by their topic whatever the payload, an empty one included.

// willState is where a Will is in its life, as the model knows it.
type willState uint8

const (
	willSent      willState = iota // its CONNECT sent, not yet answered
	willRefused                    // its CONNECT refused: never stored, never published
	willHeld                       // its connection open
	willDiscarded                  // its connection ended by a normal DISCONNECT
	willDeferred                   // its connection open at SIGTERM: published, or kept for the restart
	willFired                      // published, its connection having ended otherwise, or kept for the restart if the stop came first
	willWaiting                    // its connection ended otherwise, waiting out its Will Delay
	willCancelled                  // cancelled by a reconnect in time, or a takeover keeping its session
)

// willStats counts what the run did with Wills, for its last lines.
type willStats struct {
	delayed, cancelled, uncertain, late, early, takeovers int
}

// willAccess bounds a time in which a Will's check may use one set of
// rights. A reload's swap lies between its request and its observation, so
// consecutive intervals overlap there. A zero until leaves the interval
// open. These are check times, not storage-completion times.
type willAccess struct {
	allowed     bool
	from, until time.Time
}

// willDecision is the publication decision at one observation boundary.
// Delivery readers compute it without changing the model shared by a burst;
// syncWill applies it together with the record and its accounting.
type willDecision struct {
	dropped, denied, mayDrop, uncertain bool
}

// will is one Will a CONNECT of the run carried.
type will struct {
	tag    string // its topic's first level
	step   int    // the CONNECT's step
	client string // the slot's client id, for messages
	user   string // the user its CONNECT logged in as, whose rights it is published under
	w      *harness.Will
	state  willState
	// how says what ended its connection, or why its CONNECT was refused,
	// for messages; at is that step.
	how string
	at  int
	// firedAt is when the step that ended its connection began: its
	// record's time, the core's intake time, is no earlier (design.md,
	// PUBLISH: "A Will's Message Expiry starts when the Will is
	// published").
	firedAt time.Time
	// found is where its record was found, "" until then.
	found string
	// keptAfterStop is the final wills-table observation: even an immediate
	// Will can remain for restart if the stop reached the core first.
	keptAfterStop bool
	// dropped is terminal, for capacity or, with denied, access. denied
	// without dropped means no allowed check is possible under the rights
	// seen so far, but a future reload may allow it. mayDrop means a possible
	// check met denied rights or a channel that might be full. Exactly one
	// of m and unsent holds the original record after release: m while a
	// publication is possible, unsent otherwise. syncWill changes these
	// together with the registries, obligations and accounting.
	dropped, mayDrop, denied bool
	unsent                   *message
	access                   []willAccess
	// settledAt bounds the check and publication once the CONNECT's stored
	// changes are confirmed, by its CONNACK or the expected close when that
	// reply cannot fit (CONNECT, 5–8). An ordinary close proves no settlement.
	settledAt time.Time
	// delivered: a subscriber has received it, so it was published. Bursts
	// read on goroutines of their own.
	delivered atomic.Bool
	// delay is its Will Delay Interval, 0 for none; m its record's
	// expectations while publication is possible after its connection ends.
	delay time.Duration
	m     *message
	// A delayed Will goes no earlier than notBefore, the start of the step
	// that ended its connection plus its delay; owedAfter, where the client
	// read the close, is that plus its delay, past which it is due (zero:
	// no bound, or not owed); checked: a checkpoint has settled every
	// subscriber's obligation for its established publication.
	notBefore, owedAfter time.Time
	checked              bool
	// uncertain: a reconnect near its due time may have cancelled it.
	uncertain bool
}

// willTag is the tag of a Will its CONNECT sends at step.
func willTag(step int) string { return "w" + strconv.Itoa(step) }

// willTagOf is the tag topic begins with, "" where it holds none. The
// run's other topics are letters alone.
func willTagOf(topic string) string {
	for level := range strings.SplitSeq(topic, "/") {
		if len(level) >= 2 && level[0] == 'w' && strings.Trim(level[1:], "0123456789") == "" {
			return level
		}
	}
	return ""
}

// recordKey is what a record or a delivery is known by: a Will's tag where
// its topic holds one, else its payload's tag (payloadTag) or whole payload.
func recordKey(topic string, payload []byte) string {
	if tag := willTagOf(topic); tag != "" {
		return tag
	}
	return string(payload)
}

// willProps draws a Will's MQTT 5 properties, each valid (MQTT 5
// 3.1.3.2); a payload is always text, so its format indicator holds. A
// Will Delay Interval is 0 where present: one above 0 is not built.
func (f *fuzzer) willProps() harness.Properties {
	r := f.rng
	var ps harness.Properties
	for _, p := range []harness.Property{
		{ID: harness.WillDelayInterval, Int: r.Uint32N(3)},
		{ID: harness.PayloadFormatIndicator, Int: r.Uint32N(2)},
		{ID: harness.MessageExpiryInterval, Int: r.Uint32N(3601)},
		{ID: harness.ContentType, Str: "text/plain"},
		{ID: harness.ResponseTopic, Str: f.topic(2)},
		{ID: harness.CorrelationData, Data: []byte(f.word(1, 4))},
		harness.User(f.word(1, 3), f.word(0, 3)),
	} {
		if r.IntN(4) == 0 || (p.ID == harness.WillDelayInterval && r.IntN(4) != 0) {
			ps = append(ps, p)
		}
	}
	return ps
}

// newWill registers the Will a's CONNECT carries for s, nil where it has
// none or its topic holds no tag (an invalid topic, refused).
func (f *fuzzer) newWill(s *slot, a *attempt) *will {
	w := a.connect.Will
	if w == nil {
		return nil
	}
	tag := willTagOf(w.Topic)
	if tag == "" {
		return nil
	}
	n := &will{tag: tag, step: f.step, client: s.id, user: a.connect.Username, w: w}
	if d, ok := w.Props.Get(harness.WillDelayInterval); ok {
		n.delay = time.Duration(d.Int) * time.Second
	}
	if f.wills == nil {
		f.wills = map[string]*will{}
	}
	f.wills[tag] = n
	return n
}

// refuse records that w's CONNECT was refused.
func (w *will) refuse(why string) {
	if w != nil {
		w.state, w.how, w.at = willRefused, why, w.step
	}
}

// discard records that s's connection ended with a normal DISCONNECT,
// which discards its Will.
func (s *slot) discardWill(how string) {
	if w := s.will; w != nil {
		w.state, w.how, w.at = willDiscarded, how, s.f.step
		s.will = nil
	}
}

// fire records that s's connection ended any way but a normal DISCONNECT,
// how, so its Will, if any, is published at once, an ordinary record of
// s's client: its topic, QoS, payload and RETAIN, its properties but the
// Will Delay Interval (MQTT 5 3.1.3.2: the Will Properties "to be sent
// with the Will Message"; the delay is the server's alone), and the
// publisher's client id, against which No Local is honoured (design.md,
// SUBSCRIBE, Options). Its intake follows the end, which no answer
// confirms, so it is ordered after every step before this one and against
// none after (message.confirmed). It must come before s.end.
func (s *slot) fire(how string) {
	s.release(how, willFired, false, false)
}

// fireTaken is fire for an end the client has seen the core take: it ended
// its sending, or sent a DISCONNECT, and read the broker's close, which the
// writer makes only once the core has taken the reader's end (design.md,
// Flows, Closing a connection: the core cuts a connection by sending the
// writer its final entry). The broker serving, the Will is then published
// at once (DISCONNECT), so from this step it is treated as an acknowledged
// QoS 1 PUBLISH of its client is: owed to every subscriber whose matching
// subscription was in place before (owed), and delivered by the next
// checkpoint. A close the broker makes gives no such order: its writer
// ends its sending before the reader reports its end (Closing a
// connection, 1 and 2). Even an observed end does not timestamp publication:
// the Will's read may still be pending when the connection closes.
func (s *slot) fireTaken(how string) {
	s.release(how, willFired, true, false)
}

// delayed reports whether s's Will, its connection ending now any way but
// a normal DISCONNECT, waits out its Will Delay: it has one and the
// session outlives the connection.
func (s *slot) delayed() bool {
	return s.will != nil && s.will.delay > 0 && s.durable
}

// endObserved bounds a delayed Will once the connection's end is seen.
// design.md, DISCONNECT: "The Will goes after its Will Delay, or at once
// for MQTT 3.1.1." Its existing notBefore bounds the end from below; this
// observation bounds it from above, including an operator's hang-up.
func (w *will) endObserved() {
	if w != nil && w.state == willWaiting {
		w.owedAfter = time.Now().Add(w.delay)
	}
}

// release is fire, the Will taking state, which a graceful stop makes
// willDeferred, taken as fireTaken says, where s holds one.
// A delayed Will (delayed) waits, s.waiting, unless atOnce: its session
// ends now.
func (s *slot) release(how string, state willState, taken, atOnce bool) {
	w := s.will
	if w == nil {
		return
	}
	f := s.f
	waits := state == willFired && !atOnce && s.delayed()
	s.will = nil
	w.state, w.how, w.at, w.firedAt = state, how, f.step, f.began
	if waits {
		w.state, w.notBefore = willWaiting, f.began.Add(w.delay)
		if !s.anon {
			// An empty client id's session, its assigned identifier's, is
			// never reconnected to: its Will goes after its delay.
			s.waiting = w
		}
		if taken {
			w.endObserved()
		}
		f.willStats.delayed++
	}
	m := &message{step: w.step, publisher: s.clientID, hidden: s.hidden, topic: w.w.Topic, qos: w.w.QoS,
		retain: w.w.Retain, payload: string(w.w.Payload), will: w, tag: w.tag, length: len(w.w.Payload),
		props: slices.DeleteFunc(slices.Clone(w.w.Props), func(p harness.Property) bool { return p.ID == harness.WillDelayInterval })}
	_, m.expiring = m.props.Get(harness.MessageExpiryInterval)
	m.channel = route(m.topic)
	w.unsent = m
	w.access = []willAccess{{allowed: may(w.user, m.topic, true), from: f.began}}
	if taken {
		// Its intake follows, unconfirmed: confirmed stays 0.
		m.ackStep = f.step
		if atOnce {
			// A takeover's CONNACK follows its Will transaction. An ordinary
			// connection's close only confirms that the core took its end.
			w.settledAt = time.Now()
		}
	}
	f.syncWill(w, false, f.willTime(time.Now()))
}

// observed reports publication evidence, which no later policy can undo.
func (w *will) observed() bool { return w.found != "" || w.delivered.Load() }

// record is the original record of a released Will, whether a publication
// is currently possible or a later reload may still allow its check.
func (w *will) record() *message {
	if w.m != nil {
		return w.m
	}
	return w.unsent
}

// possibleChecks reports whether a check could allow publication, including
// a future check under today's rights, and whether a check could already
// have denied it. Denial entirely before the earliest due time contributes
// neither uncertainty nor an obligation's removal.
func (w *will) possibleChecks(now time.Time) (allowed, denied bool) {
	earliest := w.notBefore
	if earliest.IsZero() {
		earliest = w.firedAt
	}
	for _, access := range w.access {
		from, until := access.from, access.until
		if from.Before(earliest) {
			from = earliest
		}
		if !w.settledAt.IsZero() && (until.IsZero() || w.settledAt.Before(until)) {
			until = w.settledAt
		}
		if !access.allowed && (until.IsZero() || now.Before(until)) {
			until = now
		}
		if until.IsZero() || !until.Before(from) {
			allowed = allowed || access.allowed
			denied = denied || !access.allowed
		}
	}
	return allowed, denied
}

// decision reconciles timing, access and publication evidence. An observed
// publication and a certain drop are terminal. Before a delayed check can
// happen, denied rights alone do not explain a missing stored Will.
func (w *will) decision(at time.Time, limited, uncertain bool) willDecision {
	switch {
	case w.observed() || w.state == willCancelled:
		return willDecision{}
	case w.dropped:
		return willDecision{dropped: true, denied: w.denied}
	}
	allowed, denied := w.possibleChecks(at)
	dropped := !allowed && !w.settledAt.IsZero()
	return willDecision{dropped: dropped, denied: !allowed,
		mayDrop: !dropped && (denied || limited), uncertain: uncertain}
}

// willTime caps observations at the broker's stop. Inspecting its files
// later cannot make another check possible before that stop.
func (f *fuzzer) willTime(at time.Time) time.Time {
	if f.stopped {
		return f.willAt
	}
	return at
}

// observeWills refreshes time-dependent decisions on the driver, at record
// checks and checkpoints. A held or refused CONNECT has no released record
// to reconcile. It returns the Wills newly owed, so a checkpoint can check
// earlier subscribers too. Burst readers use decision without mutating it.
func (f *fuzzer) observeWills(at time.Time) (owed []*message) {
	f.willAt = f.willTime(at)
	for _, w := range f.wills {
		if w.record() != nil && f.syncWill(w, w.uncertain, f.willAt) {
			owed = append(owed, w.m)
		}
	}
	return owed
}

// syncWill reconciles a Will's whole publication decision. The first call
// prepares its original record and checks the committed capacity preceding
// release. Later calls reuse that record: a database read then could already
// contain the Will itself. Publication evidence and certain drops are
// terminal; only a still-possible check can change with a reload. uncertain
// carries the reconnect's timing decision: it may have cancelled the Will.
// The result reports a newly established delivery obligation.
func (f *fuzzer) syncWill(w *will, uncertain bool, at time.Time) bool {
	m := w.record()
	wasOwed := m.fateIs(takenWill)
	if m.serial == 0 {
		f.serial++
		m.serial = f.serial
		if m.channel != broadcast {
			m.retain = false // "A retain flag on a configured channel topic is ignored"
		}
		if lim := f.limits[m.channel]; lim != nil {
			m.bytes = messageBytes(m.topic, m.length, m.props)
			if lower, ok := f.lowerAt[m.channel]; ok && lower+m.bytes > lim.max {
				w.dropped = true
			}
		}
	}
	d := w.decision(at, f.limits[m.channel] != nil, uncertain)
	w.dropped, w.denied, w.mayDrop, w.uncertain = d.dropped, d.denied, d.mayDrop, d.uncertain
	publishes := !w.dropped && !w.denied && w.state != willCancelled
	if publishes {
		f.publishWill(w, m)
	} else {
		f.unpublishWill(w)
	}
	if publishes && m.ackStep > 0 && !w.mayDrop && !w.uncertain {
		m.setFate(takenWill)
	} else {
		m.setFate(0)
	}
	return !wasOwed && m.fateIs(takenWill)
}

// publishWill registers m once as w's possible publication. Its inverse
// unpublishWill releases the same accounting, retaining the original record
// for a check a reload may still allow.
func (f *fuzzer) publishWill(w *will, m *message) {
	if w.m != nil {
		return
	}
	if m.retain {
		f.retained++
	}
	if m.channel != broadcast {
		f.channelRecords++
	}
	if lim := f.limits[m.channel]; lim != nil {
		lim.upper.Add(m.bytes)
	}
	w.m, w.unsent = m, nil
	f.publications[w.tag] = m
	if f.dbPath != "" {
		f.messages[w.tag] = m
	} else {
		m.payload = ""
	}
}

// unpublishWill removes w's possible publication and its charges together,
// keeping the original record in unsent. An observed Will never comes here.
func (f *fuzzer) unpublishWill(w *will) {
	m := w.m
	if m == nil {
		return
	}
	if m.retain {
		f.retained--
	}
	if m.channel != broadcast {
		f.channelRecords--
	}
	if lim := f.limits[m.channel]; lim != nil {
		lim.upper.Add(-m.bytes)
	}
	w.m, w.unsent = nil, m
	delete(f.publications, w.tag)
	delete(f.messages, w.tag)
}

// beginStep starts a step: its time, and, where a slot holds a Will on a
// channel with a max_bytes, the bytes committed on each such channel, which
// only grow while no retention comes due: a Will whose connection ends in
// the step is admitted after this read, and refused for room for certain
// where these bytes leave it none (release; design.md, DISCONNECT: a Will
// "is dropped and counted if [...] its channel [...] is full"; Channels:
// "A hard byte limit counts committed charges and reservations for
// accepted writes").
func (f *fuzzer) beginStep() error {
	f.wireStep.Store(int64(f.step))
	f.began, f.lowerAt = time.Now(), nil
	if !f.stopped {
		f.willAt = f.began
	}
	if f.dbPath == "" || !slices.ContainsFunc(f.slots, func(s *slot) bool { return s.will != nil && f.limits[route(s.will.w.Topic)] != nil }) {
		return nil
	}
	f.lowerAt = make(map[string]int64, len(f.limits))
	for name := range f.limits {
		lower, err := f.lowerBytes(name)
		if err != nil {
			return err
		}
		f.lowerAt[name] = lower
	}
	return nil
}

// lateReconnect: a client whose session holds a delayed Will it saw the
// core take waits past the latest due time, then connects: a CONNECT
// keeping the session is then too late to cancel it (design.md, CONNECT,
// 5: "A CONNECT taken at or after the due time is too late"), and the Will
// goes all the same. The pick reads only the driver's state; the wait, up to
// the Will Delay, is the clock's.
func (f *fuzzer) lateReconnect() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn == nil && s.waiting != nil && !s.waiting.owedAfter.IsZero() })
	if s == nil {
		return false, nil
	}
	wait := max(0, time.Until(s.waiting.owedAfter)) + 20*time.Millisecond
	a, err := f.newAttempt(s, false, false)
	if err != nil {
		return true, err
	}
	f.note("%s: waits past the due time of %s, then %s: want %s", s.id, s.waiting.describe(), describe(a.connect), a.prediction)
	time.Sleep(wait)
	return true, f.try(s, a, nil)
}

// takeOverWill settles the Will of s's connection, which an accepted CONNECT
// keeping its session (keep) or not takes over (design.md, CONNECT, 5 and
// 6): a delayed Will whose session the CONNECT keeps is cancelled; any
// other is published at once, the takeover ending the session or the Will
// having no delay. acked confirms its stored changes, through its CONNACK
// or the expected close when that reply cannot fit.
// It runs before s holds the new connection.
func (f *fuzzer) takeOverWill(s *slot, keep, acked bool) {
	w := s.will
	if w == nil {
		return
	}
	f.willStats.takeovers++
	if keep && s.delayed() {
		s.will = nil
		w.state, w.how, w.at = willCancelled, "the takeover of its connection by a CONNECT keeping its session", f.step
		f.willStats.cancelled++
		return
	}
	s.release("its connection taken over (DISCONNECT 0x8E)", willFired, acked, true)
}

// reconnectWill settles s's waiting Will at an accepted CONNECT for its
// client id, sent at sent, its answer read at answered (design.md, CONNECT,
// 5 and 6). A clean start ends the session and publishes the Will at once.
// Keeping the session cancels it if the core took the CONNECT before its
// due time: certainly where the answer came before the earliest due time,
// certainly not where the CONNECT was sent after the latest; between the
// two the client cannot know, and the Will may go or not. acked confirms
// settlement, including an accepted CONNECT whose CONNACK could not fit.
func (f *fuzzer) reconnectWill(s *slot, clean, acked bool, sent, answered time.Time) {
	w := s.waiting
	if w == nil {
		return
	}
	s.waiting = nil
	if acked {
		w.settledAt = answered
	}
	uncertain := false
	switch {
	case clean:
		f.willStats.early++
		w.state, w.how, w.at = willFired, w.how+", then a clean start ending its session", f.step
		// It may already have gone at its due time, or go at this CONNECT.
		// Confirmed settlement bounds its publication or drop before a later
		// reload. An earlier possible denial remains possible.
		if sent.Before(w.notBefore) {
			w.notBefore = sent
		}
		w.owedAfter = time.Time{}
		if acked {
			w.record().ackStep = f.step
		}
	case w.observed() || w.dropped:
		// A completed publication or drop leaves nothing to cancel.
	case acked && answered.Before(w.notBefore):
		f.willStats.cancelled++
		w.state, w.how, w.at = willCancelled, "a CONNECT keeping its session, taken before its due time", f.step
	case !w.owedAfter.IsZero() && !sent.Before(w.owedAfter):
		f.willStats.late++ // too late: published at its due time
	default:
		f.willStats.uncertain++
		uncertain = true
	}
	f.syncWill(w, uncertain, f.willTime(answered))
}

// describe is w, for messages.
func (w *will) describe() string {
	return fmt.Sprintf("the Will %q (QoS %d, retain %v, payload %q) of %s's CONNECT at step %d",
		w.w.Topic, w.w.QoS, w.w.Retain, w.w.Payload, w.client, w.step)
}

// unexpected says why the model expects no record of w, a record of which
// has been read.
func (w *will) unexpected() string {
	switch w.state {
	case willSent:
		return "its CONNECT not yet answered"
	case willRefused:
		return fmt.Sprintf("its CONNECT refused (%s), and a refused CONNECT stores nothing (design.md, CONNECT, 2 and 7)", w.how)
	case willHeld:
		return "its connection still open: a Will is published only once its connection has ended (design.md, DISCONNECT)"
	case willCancelled:
		return fmt.Sprintf("cancelled at step %d by %s (design.md, CONNECT, 5 and 6)", w.at, w.how)
	case willDiscarded:
		return fmt.Sprintf("its connection ended at step %d by %s, which discards the Will (design.md, DISCONNECT: \"A clean DISCONNECT: 0x00 discards the Will\"; MQTT 3.1.1 \"always\"; MQTT-3.14.4-3)", w.at, w.how)
	}
	if w.denied {
		return fmt.Sprintf("its topic no longer one %s may write when it went, after its connection ended at step %d (%s): a Will the acl_file no longer allows is dropped (design.md, DISCONNECT)",
			w.user, w.at, w.how)
	}
	if w.dropped {
		return fmt.Sprintf("its channel %s full when its connection ended at step %d (%s), and a Will whose channel is full is dropped (design.md, DISCONNECT)", route(w.w.Topic), w.at, w.how)
	}
	return fmt.Sprintf("published when its connection ended at step %d (%s), and found already at %s: a Will is published and removed in one transaction (design.md, DISCONNECT)", w.at, w.how, w.found)
}

// checkWillRecord checks r, the record of m's Will, beyond what every record
// is checked for: its payload, its properties as the Will gave them without
// the Will Delay Interval (storage.md: "its MQTT 5 properties in their wire
// form [...] NULL for none"), and its Message Expiry counted from its
// publication (design.md, PUBLISH: "A Will's Message Expiry starts when the
// Will is published"), which follows the end of its connection.
func (r *record) checkWillRecord(m *message) []error {
	w := m.will
	var errs []error
	if string(r.payload) != m.payload {
		errs = append(errs, fmt.Errorf("payload %q, want %q", r.payload, m.payload))
	}
	// From the Will itself: pruning releases a message's properties, and a
	// delayed Will's record comes after checkpoints.
	props := slices.DeleteFunc(slices.Clone(w.w.Props), func(p harness.Property) bool { return p.ID == harness.WillDelayInterval })
	var want []byte
	if len(props) > 0 {
		var err error
		if want, err = harness.AppendProperties(nil, props); err != nil {
			return append(errs, err)
		}
	}
	if !bytes.Equal(r.properties, want) || (r.properties == nil) != (want == nil) {
		errs = append(errs, fmt.Errorf("properties % X (NULL %v), want % X (NULL %v): the Will's, without its Will Delay Interval",
			r.properties, r.properties == nil, want, want == nil))
	}
	if !w.notBefore.IsZero() && r.time < w.notBefore.UnixMilli() {
		errs = append(errs, fmt.Errorf("time %d, before its Will Delay of %v from the end of its connection at step %d (%d): a delayed Will goes after it (design.md, DISCONNECT)",
			r.time, w.delay, w.at, w.notBefore.UnixMilli()))
	}
	if r.time < w.firedAt.UnixMilli() {
		errs = append(errs, fmt.Errorf("time %d, before step %d began (%d), when its connection ended: a Will is published when its connection ends",
			r.time, w.at, w.firedAt.UnixMilli()))
	}
	if !w.settledAt.IsZero() && r.time > w.settledAt.UnixMilli() {
		errs = append(errs, fmt.Errorf("time %d of %s, after its publication or drop was settled at %d: a clean CONNECT settles its Will before CONNACK (design.md, CONNECT, 6–8)",
			r.time, w.describe(), w.settledAt.UnixMilli()))
	}
	mei, ok := props.Get(harness.MessageExpiryInterval)
	switch {
	case ok && (!r.expires.Valid || r.expires.Int64 != r.time+int64(mei.Int)*1000):
		errs = append(errs, fmt.Errorf("expires %v, want its time %d plus its Message Expiry Interval of %d s: a Will's Message Expiry starts when the Will is published (design.md, PUBLISH)",
			nullable(r.expires.Int64, r.expires.Valid), r.time, mei.Int))
	case !ok && r.expires.Valid:
		errs = append(errs, fmt.Errorf("expires %d, want NULL: the Will has no Message Expiry Interval", r.expires.Int64))
	}
	return errs
}

// nullable is v, or NULL where it is not valid, for messages.
func nullable(v int64, valid bool) string {
	if !valid {
		return "NULL"
	}
	return strconv.FormatInt(v, 10)
}

// checkWillsAfterStop checks every Will of the run against the database
// the broker left at its graceful stop. One whose connection ended other
// than by a normal DISCONNECT, before the stop or by it, is either
// published, its record found, or still held in the wills table, never
// neither and never both: "A Will is published and removed in one
// transaction" (design.md, DISCONNECT), and one the core has not published
// by the stop stays for the restart (Restart: "Wills with their due time";
// "A Will whose client was connected at the stop is due its Will Delay
// Interval after the start"). One a normal DISCONNECT discarded, or a refused
// CONNECT carried, is neither (its record is caught as unexpected). The wills
// table holds no Will the run did not send.
func (f *fuzzer) checkWillsAfterStop() []error {
	f.observeWills(time.Now())
	rows, err := f.db.Query(`SELECT topic FROM wills`)
	if err != nil {
		return []error{fmt.Errorf("reading the wills of %s: %w", f.dbPath, err)}
	}
	defer func() { _ = rows.Close() }() // read whole, or the check has failed
	held := map[string]bool{}
	var errs []error
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic); err != nil {
			return []error{fmt.Errorf("reading the wills of %s: %w", f.dbPath, err)}
		}
		if w := f.wills[willTagOf(topic)]; w == nil || w.w.Topic != topic {
			errs = append(errs, fmt.Errorf("the wills table holds a Will on %q, which no CONNECT of the run sent", topic))
		}
		held[topic] = true
	}
	if err := rows.Err(); err != nil {
		return []error{fmt.Errorf("reading the wills of %s: %w", f.dbPath, err)}
	}
	for _, w := range slices.SortedFunc(maps.Values(f.wills), func(a, b *will) int { return cmp.Compare(a.step, b.step) }) {
		kept := held[w.w.Topic]
		w.keptAfterStop = kept
		switch {
		case (w.state == willFired || w.state == willDeferred || w.state == willWaiting) && kept && w.found != "":
			errs = append(errs, fmt.Errorf("%s, whose connection ended at step %d (%s), is both published (at %s) and still held in the wills table after the broker stopped: a Will is published and removed in one transaction (design.md, DISCONNECT)",
				w.describe(), w.at, w.how, w.found))
		case (w.state == willFired || w.state == willDeferred || w.state == willWaiting) && !kept && w.found == "" && !w.dropped && !w.mayDrop && !w.uncertain:
			errs = append(errs, fmt.Errorf("%s, whose connection ended at step %d (%s), is neither published nor held in the wills table after the broker stopped: it goes at once, or stays for the restart (design.md, DISCONNECT, Restart)",
				w.describe(), w.at, w.how))
		case w.delivered.Load() && w.found == "":
			errs = append(errs, fmt.Errorf("%s, whose connection ended at step %d (%s), was delivered, so published, but its record is not in the database after the broker stopped (R4)",
				w.describe(), w.at, w.how))
		case (w.state == willDiscarded || w.state == willRefused || w.state == willCancelled) && kept:
			errs = append(errs, fmt.Errorf("%s is still held in the wills table after the broker stopped, %s", w.describe(), w.unexpected()))
		}
	}
	return errs
}

// willCounts counts observed publications and Wills known due, and how
// many of those it found stored, for the run's last line. notDue counts
// pending Wills not yet known due, including those kept for restart or
// whose cancellation remains uncertain. A delayed Will is known due from
// the checkpoint's time, or its completed obligations (checked), unless a
// reconnect may have cancelled it. Do not use the summary's time: a delay
// passing after the stop does not publish a Will kept for restart (design.md,
// DISCONNECT; Restart, Graceful stop: "Expired sessions and due Wills are handled at
// the next start"). Possible full-channel drops are not claimed as
// publications: DISCONNECT says "The Will is dropped and counted" where
// "its channel or the retained values' limit is full".
func (f *fuzzer) willCounts() (published, found, notDue, possibleDrops int) {
	for _, w := range f.wills {
		if w.observed() {
			published++
			if w.found != "" {
				found++
			}
			continue
		}
		if w.dropped || (w.state != willFired && w.state != willDeferred && w.state != willWaiting) {
			continue
		}
		checkedDue := w.checked || (!w.owedAfter.IsZero() && !f.checkAt.Before(w.owedAfter))
		due := !w.keptAfterStop && (w.state == willFired || (w.state == willWaiting && checkedDue && !w.uncertain))
		if !due {
			notDue++
			continue
		}
		if w.mayDrop {
			possibleDrops++
			continue
		}
		published++
	}
	return published, found, notDue, possibleDrops
}

// witness: a client holding a QoS 1 Will drops its connection while
// another connected client holds a subscription matching the Will's topic
// since an earlier step, and each subscriber the Will is owed to (owed,
// fireTaken) must receive it, its acknowledgements released as at a
// checkpoint, before the next step: the Will "goes at once" (design.md,
// DISCONNECT). The pick reads only the driver's own state.
func (f *fuzzer) witness() (bool, error) {
	s := f.pick(func(s *slot) bool {
		return s.conn != nil && s.will != nil && s.will.w.QoS == 1 && !s.delayed() && route(s.will.w.Topic) == broadcast && slices.ContainsFunc(f.slots, func(r *slot) bool {
			return r != s && r.conn != nil && r.clientID != s.clientID && slices.ContainsFunc(slices.Collect(maps.Values(r.subs.current)),
				func(sub *subscription) bool {
					return sub.since < f.step && matchesFilter(sub.Filter, s.will.w.Topic) && sub.allows(s.will.w.Topic)
				})
		})
	})
	if s == nil {
		return false, nil
	}
	w := s.will
	f.note("%s: drops its connection, holding %s: want it delivered at once to each subscriber it is owed to", s.id, w.describe())
	err := s.lose()
	if err == nil {
		s.fireTaken("its connection dropped")
	}
	s.end(false)
	m := f.publications[w.tag]
	for _, r := range f.slots {
		if err != nil || m == nil || r.conn == nil || !r.owed(m) {
			continue
		}
		r.incoming.auto = true
		if err = r.releaseAcks(); err == nil {
			err = r.awaitOwed([]*message{m}, "after its client's close")
		}
		r.incoming.auto = false
	}
	return true, err
}

// wentLate is how many delayed Wills the run found published after their
// delay, for its last lines.
func (f *fuzzer) wentLate() int {
	n := 0
	for _, w := range f.wills {
		if w.state == willWaiting && w.found != "" {
			n++
		}
	}
	return n
}
