package bridge

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/store"
)

// The outbound half of a bridge: what this broker sends to the peer.
//
// **An `out` rule is a durable consumer and nothing more exotic.** It reads
// the channels its filter matches, from a stored position, and publishes
// what it reads - so everything already true of a consumer is true of it.
// Its filter reaches every `append` and `latest` channel it matches and no
// queue (invariant 11); it resumes where it left off; and when retention
// passes the position it is left behind it, counted, exactly as an MQTT
// consumer is.
//
// Three sources, and the difference between them is what the store keeps:
//
//   - `append`: a history with a stored position. An outage is survived for
//     as long as retention holds, and a restart resumes.
//   - `latest`: a value per topic with a position in memory only. A restart
//     re-sends the whole of current state, which is the honest answer for a
//     store that keeps no history to resume through.
//   - broadcast: nothing is stored, so there is no position and an outage
//     loses what it missed. Records arrive live from the fan-out.
//
// **What is never forwarded**: a record that arrived over a bridge. That is
// the loop guard, and it is a blanket rather than per-bridge because any
// mark permitting a second hop cannot detect a cycle - three brokers in a
// ring each see a name that is not their own and forward for ever.

// outWindow is how many publishes an `out` rule keeps in flight.
//
// **The smaller of what the peer will take and what this broker was told to
// send**, and the second half is why the bridge's own `receive_maximum` is
// read here at all. A peer that omits Receive Maximum means 65,535 by the
// specification, so a bound taken from the peer alone is no bound: a link
// drop would then cost a re-send of everything in flight, which is a number
// the operator never wrote down (invariant 13).
func outWindow(peer uint16, configured int) int {
	n := configured
	if peer > 0 && int(peer) < n {
		n = int(peer)
	}
	if n < 1 {
		n = 1
	}
	return n
}

// storeMinOffset keeps the lowest offset ever stored. Two topics can be
// refused in the same pass and the earliest gap is the one that governs:
// everything behind it is coming again anyway, so a later refusal must not
// raise the bar and let records between the two be abandoned.
func storeMinOffset(v *atomic.Uint64, off uint64) {
	for {
		cur := v.Load()
		if cur != 0 && cur <= off {
			return
		}
		if v.CompareAndSwap(cur, off) {
			return
		}
	}
}

// outcome is what the peer's PUBACK said about one record.
type outcome int

const (
	// unattempted: no goroutine ever called publish for this record - it
	// returned first, because a refusal ahead of it or a cancelled context
	// stopped the pass. **It is the zero value deliberately.** sent was,
	// and a plan left untouched therefore read as "the peer took it": the
	// position walk advanced through records that were never sent, and
	// SavePosition made the skip permanent. A zero value that means
	// "nothing happened" cannot be mistaken for success.
	unattempted outcome = iota
	// sent: the peer took it, and the position may advance through it.
	sent
	// contingent: refused for a reason that can stop being true - a full
	// queue at the far end, a broker under pressure. Retried where it
	// stands, and the position does not move.
	contingent
	// permanent: refused for a reason that never stops being true. Skipped
	// and counted, because holding the link behind it would stop every
	// record after it for ever.
	permanent
	// expired: the publisher's Message Expiry Interval ran out while the
	// record was in the channel, so nothing is sent. It advances the
	// position like a permanent refusal and is not counted like one: the
	// publisher said when it stopped being worth delivering, and a peer
	// that never saw it is the outcome they asked for.
	expired
)

// readOutcome reads a PUBACK's reason code.
//
// **The list is of what may be retried, not of what may not**, which is the
// same rule `transient` applies to the inbound direction and for the same
// reason: a code nobody classified must default to "give up and say so",
// never to "hold the link for ever". A peer answering `0x99` payload format
// invalid answers it every time, so retrying it is a link that never moves
// again.
func readOutcome(code byte) outcome {
	switch code {
	case 0x00, 0x10: // success, and "no matching subscribers" which is still taken
		return sent
	case 0x97: // quota exceeded: the far end is full, and a full store empties
		return contingent
	case 0x80: // unspecified error, which a peer may use for anything
		return contingent
	case 0x87:
		// **Not authorized is retried, and it is the one code where that is
		// not obvious.** A peer's `acl_file` is a file somebody edits: the
		// grant this bridge needs is missing for as long as the mistake
		// stands and present the minute it is fixed, which is the same
		// shape as a full store rather than a malformed packet. Skipping
		// instead would advance the position past every record published
		// during a misconfiguration - permanently, for a condition that
		// heals by itself. The streak line is what an operator sees while
		// it stands.
		return contingent
	default:
		return permanent
	}
}

// outRule is one `out` rule, running: one goroutine per channel it reads,
// plus the broadcast feed where its filter reaches topics nothing claims.
type outRule struct {
	b    *Bridge
	rule config.Rule
	// waiting counts this rule's records waiting past unackedAfter for a
	// PUBACK, so the line about a wait is written when the first starts and
	// taken back when the last ends - per rule, because one rule's peer
	// holding a record says nothing about another's.
	waiting atomic.Int32
}

// runOut starts every outbound rule and returns when they have all stopped.
//
// **Started once the link is up rather than at construction**, because a
// rule with nothing to publish to would otherwise read a channel and hold
// every record it read in memory waiting for a connection - the channel is
// the buffer, and reading ahead of the link turns it into two buffers.
func (b *Bridge) runOut(ctx context.Context, cm publisher) {
	var wg sync.WaitGroup
	for _, r := range b.cfg.Topics {
		if !r.Out {
			continue
		}
		o := &outRule{b: b, rule: r}
		for _, src := range b.rd.Sources(r.Filter) {
			wg.Add(1)
			go func() { defer wg.Done(); o.drain(ctx, cm, src) }()
		}
		wg.Add(1)
		go func() { defer wg.Done(); o.live(ctx, cm) }()
	}
	wg.Wait()
}

// publisher is the half of autopaho's connection manager an out rule uses,
// narrowed so a test can stand in for it without a broker at the far end.
type publisher interface {
	Publish(ctx context.Context, p *paho.Publish) (*paho.PublishResponse, error)
}

// drain carries one channel to the peer, from where this bridge had reached.
func (o *outRule) drain(ctx context.Context, cm publisher, src channel.Source) {
	if src.Latest {
		o.drainLatest(ctx, cm, src)
		return
	}
	o.drainAppend(ctx, cm, src)
}

// drainAppend reads an append channel from its stored position and
// publishes what it finds, advancing the position as the peer acknowledges.
func (o *outRule) drainAppend(ctx context.Context, cm publisher, src channel.Source) {
	reader := o.readerName()
	wake, stop := o.b.rd.Watch(src.Name)
	defer stop()

	at, ok, err := o.b.rd.Position(src.Name, reader)
	if err != nil {
		o.b.log.Error("an outbound rule cannot read its own position, so it will not start",
			"channel", src.Name, "error", err)
		return
	}
	// fresh is what makes a first read below the floor a beginning rather
	// than a loss - see carry.
	fresh := !ok
	if !ok {
		// **No stored position: the channel's own answer, not a third
		// rule.** `start: tail` means a reader with no position begins at
		// the head, and an outbound rule is a reader like any other - a
		// bridge that always replayed from the floor would ship a week of
		// history to the peer the first time somebody added a rule.
		at = 1
		if src.StartAtTail {
			at = o.b.rd.Head(src.Name)
		}
	}

	// **A refused record needs a clock of its own, because the wake is not
	// one.** Watch fires when a record lands, so a rule that stopped on a
	// refusal the peer will recover from - a full channel at the far end,
	// answered `0x97` - would wait for the next local publish to try again.
	// On a channel that has gone quiet, which is exactly the channel an
	// outage is noticed on last, that is a stall with no end: the records
	// are safe and the position is honest, and nothing moves until somebody
	// publishes or restarts the broker. So a refusal retries on the same
	// capped backoff the inbound direction uses, and the wake stays as the
	// fast path for ordinary work.
	wait := o.b.firstRetry
	for {
		moved, next, stuck := o.carry(ctx, cm, src.Name, reader, at, fresh)
		at = next
		if moved {
			fresh = false
		}
		if ctx.Err() != nil {
			return
		}
		if stuck {
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter(wait)):
			}
			if wait < mostRetry {
				wait *= 2
			}
			continue
		}
		wait = o.b.firstRetry
		if moved {
			continue // there may be more behind it
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
	}
}

// refusalStored, where a test sets it, is called once a pass has recorded a
// refused record's offset (carry): the moment from which a goroutine still
// publishing must find the gap. A variable only so a test can wait for that
// moment rather than guess at it.
var refusalStored func()

// carry publishes one batch of records and reports whether anything moved,
// the position to read from next, and whether it stopped on a refusal that
// may stop being true.
//
// **The position advances only through the contiguous acknowledged
// prefix**: every record up to the first that did not settle. Publishes
// are pipelined across topics, one in flight per topic and at most
// `outWindow` in all, so acknowledgements arrive out of order and the
// prefix is computed after the batch rather than read off where a loop
// stopped - see the pass below.
//
// fresh says this rule has never stored a position, which changes what
// being below the floor means - see the branch that reads it.
func (o *outRule) carry(ctx context.Context, cm publisher, name, reader string,
	at uint64, fresh bool) (bool, uint64, bool) {
	window := outWindow(o.b.peerReceiveMaximum(), o.b.cfg.ReceiveMaximum)
	recs, floor, err := o.b.rd.ReadFrom(name, at, window)
	if errors.Is(err, store.ErrBelowFloor) {
		// **A rule that has never stored a position has lost nothing.** It
		// starts at 1 because that is where a channel starts, and on a
		// channel retention has already trimmed that read is below the
		// floor on the rule's very first pass - which is not a loss but a
		// beginning. Reported as one, it warned that records "were never
		// sent and never will be" and moved saguin_channel_positions_lost,
		// which is the counter an operator watches for invariant 1: a new
		// bridge rule would have raised the alarm for the broker's whole
		// trimmed history, every time anybody added one.
		if fresh {
			o.b.log.Info("an outbound rule starts at its channel's floor, the history "+
				"before it having been trimmed already",
				"channel", name, "floor", floor)
			if err := o.b.rd.SavePosition(name, reader, floor); err != nil {
				o.b.log.Error("cannot store an outbound rule's position", "channel", name, "error", err)
			}
			return true, floor, false
		}
		// **Retention has passed where this rule had reached.** The records
		// between are gone and no link can fetch them back, so it resumes at
		// the floor and says how much went - the same treatment an MQTT
		// consumer gets, against the same counter, because it is the same
		// loss and an operator watching for it should not have to know which
		// kind of reader it was (invariant 1).
		o.b.log.Warn("retention passed an outbound rule's position, so records were never "+
			"sent to the peer and never will be",
			"channel", name, "position", at, "floor", floor, "records_lost", floor-at)
		o.b.rd.PositionLost(name, reader, at, floor)
		if err := o.b.rd.SavePosition(name, reader, floor); err != nil {
			o.b.log.Error("cannot store an outbound rule's position", "channel", name, "error", err)
		}
		return true, floor, false
	}
	if err != nil {
		o.b.log.Error("an outbound rule cannot read its channel", "channel", name, "error", err)
		return false, at, false
	}
	if len(recs) == 0 {
		return false, at, false
	}

	// **The batch stops at the first record the peer did not take**, and
	// this is what it buys: the position cannot advance past that record
	// anyway, so everything published after it would be published again on
	// the next pass and taken again - up to a batch of systematic
	// duplicates per retry, for as long as the refusal stands. At-least-once
	// permits them and an operator reading their channel should not have to
	// explain them.
	next := at
	stuck := false

	// **What each record is for, decided before anything is written.** The
	// decision is cheap and touches the rule rather than the link, so it
	// runs once, in order, and leaves the publishing below with nothing to
	// work out.
	type plan struct {
		rec   store.Record
		topic string
		keep  bool
		out   outcome
	}
	plans := make([]plan, len(recs))
	byTopic := map[string][]int{}
	for i, rec := range recs {
		topic, keep := o.forward(rec)
		plans[i] = plan{rec: rec, topic: topic, keep: keep}
		if keep {
			byTopic[topic] = append(byTopic[topic], i)
		}
	}

	// **One publish in flight per topic, several topics at once.** A
	// sequential rule costs one round trip per record, which on a 20ms link
	// is about 47 records a second however fast either broker is - the
	// ceiling RFC 0002 measures. Pipelining removes it, but not at the price
	// of order: a bridged record is given a *local* offset where it lands,
	// so at the far end currency is arrival order, and two writes to one
	// topic arriving out of order would leave a `latest` channel there
	// holding the older one for good.
	//
	// So the serialisation is per topic, which is the only order anything
	// promises, and the pipelining is across topics, which is where a
	// channel's traffic actually spreads. A single-topic stream stays at one
	// record per round trip, and that is the honest residual rather than a
	// shortfall.
	//
	// **Bounded by the window either way**, which is
	// `min(peer Receive Maximum, receive_maximum)`: the semaphore is what
	// spends it, where before it only sized the batch read above.
	sem := make(chan struct{}, window)
	var wg sync.WaitGroup
	// **The first refusal stops the whole pass from issuing more.** A gap
	// can only be found after the records beyond it are already in flight,
	// so some will have gone - but nothing new is added behind a gap that
	// is already known, which is what keeps the duplication bounded by the
	// window rather than by the batch.
	// **The refusal is positional, not temporal.** As a flag it stopped
	// every goroutine that had not published *yet*, including ones holding
	// records ordered before the gap - which the comment below always said
	// it did not do ("stops anything further being issued"). Those records
	// then went unattempted and, before the zero value above changed, were
	// carried as sent. Holding the lowest refused offset instead, a record
	// is abandoned only when it really is behind the gap.
	var refused atomic.Uint64 // lowest refused offset, 0 meaning none
	for _, idxs := range byTopic {
		wg.Add(1)
		go func(idxs []int) {
			defer wg.Done()
			for _, i := range idxs {
				if ctx.Err() != nil {
					return
				}
				if gap := refused.Load(); gap != 0 && plans[i].rec.Offset > gap {
					return
				}
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				out := o.publish(ctx, cm, plans[i].topic, plans[i].rec)
				<-sem
				plans[i].out = out
				if out == contingent {
					// **The rest of this topic stops here, and so does
					// everything else.** The position cannot pass this
					// record, so what follows it is coming again anyway -
					// and on this topic, sending it now would put it on the
					// wire ahead of the record it follows.
					storeMinOffset(&refused, plans[i].rec.Offset)
					if refusalStored != nil {
						refusalStored()
					}
					return
				}
				if out == sent {
					o.b.forwarded.Add(1)
				}
			}
		}(idxs)
	}
	wg.Wait()

	// **The position advances through the contiguous acknowledged prefix and
	// stops at the first record that did not settle.** Acknowledgements now
	// genuinely arrive out of order - the window spans topics while the
	// position is one number per channel - so a walk that took the highest
	// settled offset, or that skipped a gap, would carry the position past a
	// record the peer never took: everything behind it skipped silently and
	// for ever, which is invariant 1's failure reached by arithmetic.
	//
	// Records beyond the gap that were published anyway are sent again on
	// the next pass. That is at-least-once keeping its promise, and it is
	// the price of spending the window before knowing where the gap is.
	// **stuck is answered over the whole pass, not where the walk stops.**
	// The walk now halts at the first unattempted record, which can sit
	// before the refused one - and deciding stuck from the halting record
	// would then leave it false, so the retry clock never starts and the
	// pass that could not move waits for a wake that may never come. That
	// trades a silent skip for a silent stall. The two questions are
	// separate: "how far may the position go" and "is there a refusal to
	// retry".
	for i := range plans {
		if plans[i].keep && plans[i].out == contingent {
			stuck = true
			break
		}
	}
	for i := range plans {
		// A skipped record is carried like a settled one: a record this rule
		// will never forward must not stop the ones behind it. sent,
		// permanent and expired all leave the peer's side settled.
		// unattempted and contingent do not, and both must stop the walk -
		// the first because nothing was sent, the second because the peer
		// said no.
		if plans[i].keep && plans[i].out != sent &&
			plans[i].out != permanent && plans[i].out != expired {
			break
		}
		next = plans[i].rec.Offset + 1
	}

	if next == at {
		return false, at, stuck
	}
	if err := o.b.rd.SavePosition(name, reader, next); err != nil {
		o.b.log.Error("cannot store an outbound rule's position, so records already sent "+
			"would be sent again after a restart", "channel", name, "error", err)
	}
	return true, next, stuck
}

// drainLatest forwards a latest channel's values as they are published, the
// newest waiting value per topic, for as long as the rule runs.
//
// **The live fan-out and never a stored pass** (RFC 0003). It used to read
// the channel's whole current state when it started, and then wait for a
// wake that nothing on a latest channel ever sent: so a peer was handed the
// state as it stood when the bridge came up, re-sent at every start, and
// never another change - to a Sagüin and to any other broker alike, with
// nothing logged and nothing counted. A stored pass is what RFC 0003
// forbids a bridge, for two reasons written there: it re-ships the whole of
// current state at every start, and a value the bridge itself brought in is
// still in the store to be handed back.
//
// **What it carries is what is published while it runs**, and that includes
// while the link is down: the rule runs beside the link rather than inside a
// connection (Run), so it is still watching when the link goes, values keep
// arriving here, and the newest for each topic crosses when the link comes
// back. What a peer is not sent is what was published before the rule
// started - the same answer broadcast gets.
//
// **Newest wins per topic while waiting**, as a subscriber's own pending
// list does: a value replaced before it crossed is counted (superseded) and
// not sent, and the one that replaced it is. One value per topic, and none
// the channel has expired, so what waits is bounded by the topics the channel
// holds rather than by how fast they change: about three times them at most -
// pending runs to twice before prune passes, and a batch taken out of it can
// still be waiting on the link beside it. A deletion the channel stored is a value like any other and
// crosses, which is how a peer is told a topic is gone.
//
// **And no value the channel's retention has removed.** A latest channel's
// period deletes a topic that goes quiet (RFC 0003), and that deletion never
// reached this rule: every topic published while the link was down waited,
// and crossed when it came back - measured, 50,000 topics published once
// each during an outage were all held, 426 bytes each, and all sent. What
// crossed then included values the channel had already expired, and RFC 0003
// calls a stale reading worse than none. So a waiting value is held to the
// channel's own two clocks (store.PastRetention): past them it is let go
// while it waits, and not sent if its turn comes after. Not counted, as a
// record whose Message Expiry ran out is not (outcome expired): the channel
// removed it, and a peer that never saw it is what the channel's period says.
func (o *outRule) drainLatest(ctx context.Context, cm publisher, src channel.Source) {
	var mu sync.Mutex
	pending := map[string]store.Record{}
	wake := make(chan struct{}, 1)
	// gone reports whether the channel's retention has removed rec by now.
	gone := func(rec store.Record, now time.Time) bool {
		return store.PastRetention(rec, retentionCutoff(now, src.RetentionPeriod),
			retentionCutoff(now, src.DeletionRetentionPeriod))
	}
	// prune lets go of every waiting value the channel no longer holds. It
	// runs when pending has doubled since it last ran - a scan of pending,
	// O(len), on whichever publisher's goroutine the watcher calls keep on -
	// so each keep pays a constant share of it amortised, and no publisher
	// waits on the link (invariant 16). Pending stays within twice what the
	// channel holds, or pruneFloor, below which it is not worth a pass; the
	// batch in flight beside it is the rest of the three times above. The
	// caller holds mu.
	nextPrune := pruneFloor
	prune := func(now time.Time) {
		for topic, rec := range pending {
			if gone(rec, now) {
				delete(pending, topic)
			}
		}
		nextPrune = max(2*len(pending), pruneFloor)
	}
	// keep puts a value in pending unless a newer one for its topic is
	// already there, and counts the one it replaces. The caller holds mu.
	// Equal offsets are the same value, and supersede nothing.
	keep := func(rec store.Record) {
		if p, ok := pending[rec.Topic]; ok {
			if p.Offset == rec.Offset {
				return
			}
			o.b.superseded.Add(1)
			if p.Offset > rec.Offset {
				return
			}
		}
		pending[rec.Topic] = rec
		if len(pending) >= nextPrune {
			prune(time.Now())
		}
	}
	stop := o.b.rd.WatchLatest(src.Name, func(rec store.Record) {
		mu.Lock()
		keep(rec)
		mu.Unlock()
		select {
		case wake <- struct{}{}:
		default: // one pending wakeup is as good as two
		}
	})
	defer stop()

	wait := o.b.firstRetry
	for {
		mu.Lock()
		batch := make([]store.Record, 0, len(pending))
		for _, rec := range pending {
			batch = append(batch, rec)
		}
		pending = map[string]store.Record{}
		nextPrune = pruneFloor
		mu.Unlock()
		// In the order they were stored, which across topics is the order
		// they were published in; within a topic there is one.
		sort.Slice(batch, func(i, j int) bool { return batch[i].Offset < batch[j].Offset })

		stuck := false
		for i, rec := range batch {
			if ctx.Err() != nil {
				return
			}
			// Asked at its turn rather than when the batch was built: a
			// batch waits behind a stuck link too.
			if gone(rec, time.Now()) {
				continue
			}
			topic, keepIt := o.forward(rec)
			if !keepIt {
				continue
			}
			switch o.publish(ctx, cm, topic, rec) {
			case sent:
				o.b.forwarded.Add(1)
			case contingent:
				// Put back, with everything not yet tried, unless a newer
				// value for its topic has arrived meanwhile - and retried from
				// a clock rather than from the next change, for the reason
				// drainAppend states.
				mu.Lock()
				for _, r := range batch[i:] {
					keep(r)
				}
				mu.Unlock()
				stuck = true
			}
			if stuck {
				break
			}
		}
		if stuck {
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter(wait)):
			}
			if wait < mostRetry {
				wait *= 2
			}
			continue
		}
		wait = o.b.firstRetry
		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
	}
}

// pruneFloor is the number of waiting values below which a latest rule does
// not look for ones its channel has expired: a pass over that many costs less
// than the memory it could give back is worth.
const pruneFloor = 1024

// retentionCutoff is the moment before which a period of the given seconds
// has removed a record, or the zero time where the period is 0, for none - the
// broker's sweep reads a channel's periods the same way.
func retentionCutoff(now time.Time, periodSeconds int64) time.Time {
	if periodSeconds <= 0 {
		return time.Time{}
	}
	return now.Add(-time.Duration(periodSeconds) * time.Second)
}

// live forwards records published to topics no channel claims.
//
// **The live fan-out and nothing else.** Broadcast stores one thing - the
// retained value - and an outbound rule is never handed it: a retained pass
// is served on every subscribe, so a rule taking one would re-ship the whole
// of the retained set at each reconnect (RFC 0003).
func (o *outRule) live(ctx context.Context, cm publisher) {
	in := make(chan store.Record, 256)
	stop := o.b.rd.WatchBroadcast(func(rec store.Record) {
		select {
		case in <- rec:
		default:
			// **Dropped rather than blocking the publish path.** A broadcast
			// record has no store to fall back on, so the alternative to
			// dropping it here is holding up every publisher on this broker
			// behind one slow link. It is counted where an operator looks.
			o.b.liveDropped.Add(1)
		}
	})
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-in:
			topic, keep := o.forward(rec)
			if !keep {
				continue
			}
			switch o.publish(ctx, cm, topic, rec) {
			case sent:
				o.b.forwarded.Add(1)
			case contingent:
				// **On broadcast a refusal that would be retried anywhere
				// else is a loss, so it is counted as one.** There is no
				// store to come back to and no position to leave standing:
				// the record passed, the peer said not now, and there is no
				// later. The streak line says the peer is refusing; this is
				// what says how much went while it was.
				o.b.broadcastRefused.Add(1)
			}
		}
	}
}

// forward decides whether this rule sends a record, and under what topic.
func (o *outRule) forward(rec store.Record) (string, bool) {
	// **A record that arrived over a bridge is never forwarded out over a
	// bridge.** Any mark permitting a second hop cannot detect a cycle -
	// three brokers in a ring would each see a name that is not their own -
	// so this is a blanket rather than a comparison against this bridge's
	// own name.
	if rec.Bridge != "" {
		o.b.loopsSkipped.Add(1)
		return "", false
	}
	topic, ok := o.rule.Match(rec.Topic)
	if !ok {
		// This rule does not cover the record. An outbound rule reads the
		// channels its filter reaches, and a channel holds more topics than
		// one filter - so this is the ordinary case rather than a loss, and
		// it is silent for the same reason the inbound direction's
		// "no rule covers it" is not: there, nothing else would have taken
		// the record; here, another rule may and the drain walks them all.
		return "", false
	}
	if topic == "" {
		// **A rule covered it and could build no topic to publish at the
		// peer, and the check that says so is the same one the inbound
		// direction uses** - it lives in Match, which both directions call,
		// rather than in either path. That is the decision rather than an
		// accident of where it was written: a `$saguin/…` topic built out of
		// what a *local* publisher sent would forge this broker's own
		// control topics at the far end, and at a foreign peer it would
		// write into a namespace saguin does not own and cannot clean up.
		// Refused here, so the answer does not depend on whether the peer
		// happens to be a saguin with a door to refuse it.
		//
		// **Counted and said out loud**, because an outbound drop is a
		// record that is on this broker and will never be on the other one,
		// with nothing on the far end to notice it missing. Match answers
		// the same way for two reasons and they are two different drops in
		// RFC 0002, so the reason is worked out here rather than reported as
		// whichever was written first.
		o.b.unmappable.Add(1)
		if o.rule.Reserved(rec.Topic) {
			o.b.log.Error("not sending a record to the peer: the rule would build a topic "+
				"in the reserved `$` space, where saguin keeps its own control topics and "+
				"a bridge may not forge them",
				"topic", o.b.lim.Loggable(rec.Topic), "offset", rec.Offset,
				"filter", o.rule.Filter, "template", o.rule.Topic)
		} else {
			o.b.log.Error("not sending a record to the peer: the rule that covers it "+
				"produced no topic",
				"topic", o.b.lim.Loggable(rec.Topic), "offset", rec.Offset,
				"filter", o.rule.Filter, "template", o.rule.Topic)
		}
		return "", false
	}
	return topic, true
}

// publish sends one record to the peer and reads what came back.
//
// **QoS 1, and the position moves on the PUBACK.** It is the only level
// that matches at-least-once: at QoS 0 there is nothing to advance a
// position against, so a link that dropped mid-window would lose records
// with nothing noticing, and QoS 2 costs a second round trip for a
// guarantee saguin does not promise.
//
// **The retain flag crosses as the publisher set it**, so a value retained
// here becomes state at the far end and a subscriber arriving there later
// is served it. That is Retain As Published semantics on the link, and it
// is what an operator migrating a mosquitto bridge expects: mosquitto
// preserves the flag, and a bridge that quietly dropped it left a late
// subscriber at the peer finding nothing, with nothing saying so.
//
// **What does not cross is the retained *store* pass.** A bridge is never
// handed one: an outbound rule ships records as they are written, not the
// peer's or this broker's retained set at each reconnect (RFC 0003). That
// rule is unchanged, and it is the one that keeps a link restart from
// re-shipping everything.
//
// **A peer that refuses retained messages gets the value with the flag
// down.** See Bridge.peerNoRetain: paho refuses such a publish
// client-side, so carrying the flag regardless would fail the write, hold
// the rule's position at that record, and retry it on every reconnect for
// ever. The value crosses live instead and the retained promise does not,
// which RFC 0002 states as the degradation it is.
func (o *outRule) publish(ctx context.Context, cm publisher, topic string, rec store.Record) outcome {
	props := &paho.PublishProperties{
		ContentType:     rec.ContentType,
		ResponseTopic:   rec.ResponseTopic,
		CorrelationData: rec.CorrelationData,
	}
	if rec.CorrelationDataEmpty && props.CorrelationData == nil {
		props.CorrelationData = []byte{} // present, and empty
	}
	if rec.PayloadFormatFlag {
		v := rec.PayloadFormat
		props.PayloadFormat = &v
	}
	// **What is left of the publisher's expiry, not what they wrote.** MQTT
	// requires the interval to be decremented by the time the message has
	// been waiting, and a record leaving a channel has been waiting since it
	// was stored - so shipping the original would restart the clock at every
	// hop. A record whose interval has run out is not sent at all: it is
	// still here at its offset under this channel's retention, which is the
	// same split RFC 0003 draws for a retained value against a stored one.
	if rec.MessageExpiry > 0 {
		left, ok := rec.RemainingExpiry(time.Now())
		if !ok {
			return expired
		}
		props.MessageExpiry = &left
	}
	for _, h := range rec.Headers {
		props.User = append(props.User, paho.UserProperty{Key: h.Key, Value: h.Value})
	}
	// **The record's identity crosses with it** (invariant 8). A receiving
	// saguin lifts `saguin-id` into the record it stores, so a record that
	// crossed keeps the Message ID it was given here and takes a local
	// offset; without it the far end mints a fresh one and the same record
	// on two brokers has two identities, which is what makes a duplicate
	// after a link drop impossible to recognise. Appended after the
	// publisher's own headers because the reserved prefix is stripped on
	// ingest, so nothing a publisher sent can occupy the name.
	props.User = append(props.User, paho.UserProperty{Key: "saguin-id", Value: rec.MessageID})

	retain := rec.Retain
	if retain && o.b.peerNoRetain.Load() {
		retain = false
		// Once per link. A source whose records are mostly retained would
		// otherwise write this per record, which is the shape `refusing`
		// already exists to avoid.
		if o.b.retainCleared.CompareAndSwap(false, true) {
			o.b.log.Warn("the peer does not accept retained messages, so values " +
				"cross with the retain flag down: subscribers there receive them " +
				"live and a late subscriber finds nothing")
		}
	}

	// **Under the rule's context and the peer's session both** (publishWait):
	// the wait for a PUBACK crosses a reconnect, and ends when the bridge
	// stops or the peer turns out to have kept no session.
	session := o.b.session.Load()
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(session.ctx, cancel)()
	// **A peer that holds a record unacknowledged is waited on, and said.**
	// A link that answers PINGREQ is up, so a PUBACK that does not come is
	// the peer's to send; the rule waits for it and sends nothing behind it
	// on this topic, as mosquitto's bridge does, rather than sending it again
	// as a new message. Once per wait, so an operator is not left with a
	// rule that has gone quiet and says nothing.
	fired := make(chan struct{})
	timer := time.AfterFunc(o.b.unackedAfter, func() {
		defer close(fired)
		if o.waiting.Add(1) == 1 {
			o.b.log.Warn("the peer has not acknowledged a record on a link that is up, so "+
				"this rule is waiting for it and sends nothing behind it on that topic",
				"rule", o.rule.Filter, "topic", o.b.lim.Loggable(topic), "offset", rec.Offset,
				"waited", o.b.unackedAfter)
		}
	})
	ack, err := cm.Publish(pctx, &paho.Publish{
		Topic: topic, QoS: 1, Retain: retain, Payload: rec.Payload, Properties: props,
	})
	// A wait that was announced ends here: counted down once its line has been
	// written, and taken back when it was this rule's last and the peer
	// answered. One that ends in an error is left to the line that error
	// writes below.
	if !timer.Stop() {
		<-fired
		if o.waiting.Add(-1) == 0 && err == nil {
			o.b.log.Warn("the peer acknowledged the record it was holding, so this rule is moving again",
				"rule", o.rule.Filter, "topic", o.b.lim.Loggable(topic))
		}
	}
	if err != nil {
		// The link, rather than the peer's opinion of the record. Left
		// unacknowledged so the position does not move and the record is
		// offered again on the next connection.
		why := fmt.Sprintf("the link failed: %v", err)
		if session.ctx.Err() != nil && ctx.Err() == nil {
			why = "the peer kept no session for this bridge, so what was in flight is sent again"
		}
		o.b.retryStreak(topic, why)
		return contingent
	}
	if ack == nil {
		o.b.retryStreak(topic, "the peer answered nothing")
		return contingent
	}

	switch readOutcome(ack.ReasonCode) {
	case sent:
		o.b.retryCleared()
		return sent
	case contingent:
		o.b.retryStreak(topic, fmt.Sprintf("the peer answered 0x%02X", ack.ReasonCode))
		return contingent
	default:
		// **Skipped, counted and named.** The record stays where it is,
		// under this channel's retention, so an operator reading this line
		// can go and find it with any client - which is more than the
		// inbound direction's drop can say. Holding the link instead would
		// turn one record the peer will never take into total
		// non-delivery, on a box nobody attends for weeks.
		o.b.retryCleared()
		o.b.log.Error("skipping a record the peer will never take, so it is not on the "+
			"far end and never will be; it is still here at its offset, under this "+
			"channel's retention",
			"topic", o.b.lim.Loggable(topic), "offset", rec.Offset,
			"message_id", o.b.lim.Loggable(rec.MessageID),
			"reason", ack.ReasonCode)
		o.b.peerRefused.Add(1)
		return permanent
	}
}

// retryStreak writes one line when a run of refusals starts and nothing
// until it clears.
//
// **A streak rather than a line each**, which is the discipline the link
// already follows for a connection that will not come up: a peer answering
// `0x97` to every publish is a bridge that is connected and shipping
// nothing, and one line per record would bury the fact in the evidence for
// it.
func (b *Bridge) retryStreak(topic, why string) {
	if b.refusing.Swap(true) {
		return
	}
	b.log.Warn("the peer is refusing this bridge's records, so nothing is moving out; "+
		"the channel is holding them and the position has not advanced",
		"topic", b.lim.Loggable(topic), "reason", why)
}

// retryCleared says so once, when records start moving again.
func (b *Bridge) retryCleared() {
	if b.refusing.Swap(false) {
		// **Warn, because it retracts one** - the rule at "bridge link up
		// again": a level is a severity filter, not a subject one, so a
		// reader at `warn` who was told the peer is refusing is owed the
		// line that says it stopped.
		//
		// **"Answering" rather than "taking"**, because this is also reached
		// when the streak ends in a record the peer refused for good: the
		// link is working and the queue behind it is moving again, which is
		// what the line is for, but the record that ended the streak may
		// have been skipped rather than taken and its own line says so.
		b.log.Warn("the peer is answering this bridge's records again, so they are moving")
	}
}

// readerName is what this rule stores its positions under: the bridge's
// name and the rule's own filter and topic, quoted so that no two rules
// share one however their topics are spelled.
//
// **One position per rule, never per bridge.** Each rule drains a channel on
// a goroutine of its own and carries past what is not its record, so two
// rules saving one position let the rule the peer takes carry it past a
// record the other had not sent: after a restart both resumed beyond it and
// it was never sent, with nothing counted (invariant 1). Per rule, a peer refusing one rule's records
// holds that rule's position and nobody else's, and retention passing it is
// counted once, against that rule.
func (o *outRule) readerName() string {
	return store.BridgeReader(o.b.cfg.Name + " " + strconv.Quote(o.rule.Filter) + " " + strconv.Quote(o.rule.Topic))
}

// peerReceiveMaximum is how many publishes the peer said it would handle at
// once, or zero where it said nothing - which the specification reads as
// 65,535 and outWindow refuses to take as a bound.
func (b *Bridge) peerReceiveMaximum() uint16 { return uint16(b.peerRxMax.Load()) }

// Forwarded, LoopsSkipped and the unsent counts are what the metric catalogue
// asks of an outbound rule (RFC 0005).
func (b *Bridge) Forwarded() uint64        { return b.forwarded.Load() }
func (b *Bridge) LoopsSkipped() uint64     { return b.loopsSkipped.Load() }
func (b *Bridge) PeerRefused() uint64      { return b.peerRefused.Load() }
func (b *Bridge) BroadcastRefused() uint64 { return b.broadcastRefused.Load() }
func (b *Bridge) Unmappable() uint64       { return b.unmappable.Load() }
func (b *Bridge) LiveDropped() uint64      { return b.liveDropped.Load() }

// UnstoredQueueFull is the QoS 0 records from the peer dropped because the
// bridge's queue was full (enqueue).
func (b *Bridge) UnstoredQueueFull() uint64 { return b.unstoredQueueFull.Load() }

// UnstoredNoRule, UnstoredUnmappable and UnstoredNeverAccepted are the
// records from the peer dropped with a warning (route, and handle's Bounded
// refusal): the rest of saguin_bridge_unstored_total.
func (b *Bridge) UnstoredNoRule() uint64        { return b.unstoredNoRule.Load() }
func (b *Bridge) UnstoredUnmappable() uint64    { return b.unstoredUnmappable.Load() }
func (b *Bridge) UnstoredNeverAccepted() uint64 { return b.unstoredNeverAccepted.Load() }
func (b *Bridge) Superseded() uint64            { return b.superseded.Load() }
