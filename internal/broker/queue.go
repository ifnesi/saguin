package broker

import (
	"context"
	"encoding/hex"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
)

// Run drives what the broker needs a clock for: handing available work to
// waiting workers, noticing that a lease has expired, storing the consumer
// positions that have moved every broker.session.ack_commit_interval, and
// sweeping what retention has to remove.
//
// Correctness does not depend on either interval. A shorter tick makes
// redelivery prompter and retention tighter; it does not make anything true
// that was false.
//
// Four tickers and one goroutine. What must not happen is a ticker per
// channel - fifteen channels must not mean fifteen goroutines on an edge
// box - and four clocks in one select loop is still one goroutine.
//
// They are separate because the two retention rules want different
// intervals for different reasons. **Size** decides how far a channel may
// stand above its retention_bytes before anything trims it, so it wants a
// short fixed interval and gets one; it has no deadline to derive from
// anyway. **Age** only decides how far past its deadline a record may
// survive, so a tenth of the shortest period configured is exactly right,
// and a channel retaining for thirty days should not be read every second
// for nothing.
//
// Correctness depends on none of the three. A shorter tick makes redelivery
// prompter and retention tighter; it does not make anything true that was
// false.
func (b *Broker) Run(ctx context.Context) {
	// Closed on the way out, so that a shutdown can wait for this loop
	// rather than merely ask it to stop. Cancelling the context returns
	// immediately and says nothing about whether a sweep is still inside
	// a store; Shutdown is what turns that into an order.
	b.running.Store(true)
	defer close(b.stopped)

	t := time.NewTicker(queuePeriod)
	defer t.Stop()

	size := time.NewTicker(sizeSweepPeriod)
	defer size.Stop()

	// Consumer positions on broker.session.ack_commit_interval, the interval
	// a broadcast session's acknowledgements are stored on, and not on the
	// queue's own tick: a connected session's progress is stored on one
	// clock whichever kind of channel it reads.
	positions := time.NewTicker(b.AckCommitInterval())
	defer positions.Stop()

	age := b.ageSweepPeriod()
	byAge := time.NewTicker(age)
	defer byAge.Stop()

	// What the store refused and a client was told is stored, written again
	// until it lands (retryOwedWrites).
	owed := time.NewTicker(owedRetryPeriod)
	defer owed.Stop()

	b.log.Info("retention sweeps started", "by_size", sizeSweepPeriod, "by_age", age)

	// Once before either ticker fires. A derived age interval can be long -
	// a channel retaining for a year sweeps every five weeks - and a broker
	// restarted more often than that would otherwise never sweep at all. It
	// is also what an operator expects of a broker that was down while its
	// records aged.
	b.sweepBySize()
	b.sweepByAge(time.Now())

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.expireLeases()
			// Before the offer rather than after it, so a record the wire
			// never carried can go out again on the same tick.
			b.returnUnsent()
			b.deliver()
			// A departed client's publish budget goes when it has refilled,
			// which is when keeping it stops saying anything a fresh one
			// would not. Held until then, a reconnect resumes the budget
			// instead of being handed a new one - see forgetAllowance.
			b.sweepAllowances(time.Now())
			// And a shared group's rotation, once nothing has used it.
			b.sweepShareCursors(time.Now())
			// And the latest topics' heads that have gone quiet.
			b.sweepLatestHeads(time.Now())
			// And what a group has held past broker.share.expires_after.
			b.sweepShareBacklogs(time.Now())
			// An exactly-once publish nobody released, past
			// `broker.qos2.expires_after`. It rides this ticker rather than
			// one of its own because the answer only has to be right to
			// within a tick: a message held a fifth of a second longer than
			// the operator wrote costs nothing, and a ticker per feature is
			// a goroutine per feature.
			b.sweepHeldPublishes(time.Now())
			// And what a full broadcast log gave up, once in a while rather
			// than once per message.
			if d := b.broadcastDrain(); d != nil {
				d.sayGaveUp(time.Now())
			}
			if f := afterQueueTick.Load(); f != nil {
				(*f)(b)
			}
		case <-b.offerNow:
			// A worker was freed by an answer or a departure, so its queue is
			// offered now. The same goroutine as the tick, so the two passes
			// never run at once.
			b.mu.Lock()
			due := b.offerDue
			b.offerDue = map[string]bool{}
			b.mu.Unlock()
			b.deliverTo(due)
		case <-positions.C:
			// Written here rather than on the publish path, which is what
			// keeps a store write off it. One interval is therefore how far a
			// stored position may lag behind the consumer, and lagging means
			// a record arrives twice rather than not at all. Reset each time,
			// so an interval set after this loop began is the one it keeps.
			b.flushPositions()
			positions.Reset(b.AckCommitInterval())
			if f := afterPositionsFlush.Load(); f != nil {
				(*f)(b)
			}
		case <-owed.C:
			b.retryOwedWrites()
			if f := afterOwedRetry.Load(); f != nil {
				(*f)(b)
			}
		case <-size.C:
			b.sweepBySize()
			if f := afterSizeSweep.Load(); f != nil {
				(*f)(b)
			}
		case <-byAge.C:
			b.sweepByAge(time.Now())
		}
	}
}

// afterSizeSweep is a test seam, nil in production: when set it runs on Run's
// goroutine each time a size sweep has finished, so a test can wait for a
// sweep to have come round rather than sleeping through its period.
var afterSizeSweep atomic.Pointer[func(b *Broker)]

// afterOwedRetry and afterPositionsFlush are the same seam for the owed-write
// retry and the position flush: a pass that writes nothing calls no store, so
// no wrapper a test installs can see it come round.
var afterOwedRetry, afterPositionsFlush atomic.Pointer[func(b *Broker)]

// afterQueueTick is a test seam, nil in production: when set it runs on Run's
// goroutine each time a queuePeriod tick's work has finished, so a test about
// what the offer loop does over some ticks counts them rather than sleeping.
var afterQueueTick atomic.Pointer[func(b *Broker)]

const (
	// queuePeriod is what the queue's own work runs on: offering available
	// records, noticing an expired lease.
	queuePeriod = 200 * time.Millisecond

	// owedRetryPeriod is how often a write the store refused, of something a
	// client was already told is stored, is tried again.
	owedRetryPeriod = time.Second

	// sizeSweepPeriod is how long a channel may stand above its
	// retention_bytes before anything trims it, so it is short and fixed.
	//
	// There is nothing to derive it from - a size bound carries no deadline
	// - and nothing to gain by making it longer, at any size. A sweep that
	// finds nothing to remove is answered from the channel's byte count
	// rather than from its records: `held` is the number a publish is
	// refused against, so a channel under its bound is settled without
	// asking storage anything. BenchmarkRetentionSweepBySize measures one
	// pass over ten thousand channels at 1.3-1.5ms on sqlite and 1.5ms in
	// memory, which is the same figure twice because neither one reads.
	//
	// **It was not always, and the earlier reasoning is why this says so.**
	// Asking each channel's records cost 164-174ms a pass at that size, so a
	// broker holding ten thousand channels with no client connected at all
	// spent a fifth of a core, every second, for the life of the process,
	// establishing that nothing had changed. The per-channel figures quoted
	// here before were 50us and 68ns, measured at one size; the conclusion
	// drawn from them - "frequency is free here" - was reasoned from fifteen
	// channels and did not survive ten thousand. It survives now, and for a
	// better reason than a smaller number: the size sweep stopped scaling
	// with the channel count in the way that mattered.
	//
	// **The age sweep is the one still holding a cost**, and it is not a
	// defect to be fixed the same way: a record becomes too old with nothing
	// written to its channel, so there is no counter to consult and it has
	// to look. 162ms a pass at ten thousand channels on sqlite, unchanged.
	// What decides whether that matters is its period, which is derived from
	// the shortest retention any channel configures rather than fixed here -
	// an hour puts the passes six minutes apart and the cost disappears, and
	// ten seconds puts them on this same one-second floor, where it becomes
	// the figure the size sweep used to be. RFC 0004 carries both tables.
	//
	// It is not a knob: an operator has no way to reason about how far above
	// their number they would like to sit, and the honest answer is "as
	// little as it costs".
	sizeSweepPeriod = time.Second

	// ageSweepFloor stops a very short retention period from making the
	// broker sweep continuously. A one-second period is already the smallest
	// a duration can express.
	ageSweepFloor = time.Second
)

// ageSweepPeriod is a tenth of the shortest retention period or message
// TTL any channel configured, and at least the floor above.
//
// A tenth, so that nothing outlives its period by much more than a tenth of
// it. It is derived rather than exposed because that is the only thing it
// decides, and an operator who set it coarser than their own retention
// would have set it wrongly.
//
// There is no ceiling, unlike the size sweep: a channel retaining for a
// month is served perfectly well by a sweep every three days, and reading
// every channel every thirty seconds to find nothing would be work for
// nobody. What covers the long end instead is the sweep Run does before any
// ticker fires, so a broker that restarts more often than it sweeps still
// sweeps.
//
// A configuration with no deadline at all still gets a ticker, at the
// floor. It costs a read per channel that finds nothing, and it means a
// period added later needs no special case.
func (b *Broker) ageSweepPeriod() time.Duration {
	shortest := time.Duration(0)
	for _, c := range b.reg.All() {
		// Every age rule that lands on this sweep counts, and each is here
		// because leaving one out makes the sweep coarser than what it
		// promises - a record outliving its period by much more than the
		// tenth above. job_expires_after is an age rule on the same clock.
		// deletion_retention_period is the one a latest channel keeps its
		// deletions on, and it was missed when it was added: a channel
		// keeping values for ever and deletions for an hour contributed
		// nothing, so on a broker whose other channel retained for a month
		// the deletions were swept every three days.
		for _, secs := range []int64{c.RetentionPeriod, c.JobExpiresAfter, c.DeletionRetentionPeriod} {
			if secs <= 0 {
				continue
			}
			d := time.Duration(secs) * time.Second
			if shortest == 0 || d < shortest {
				shortest = d
			}
		}
	}
	// The retained store is not a channel and the loop above cannot see it,
	// but its period is an age rule on the same clock and lands on the same
	// sweep.
	if b.retainedPeriod > 0 {
		if d := time.Duration(b.retainedPeriod) * time.Second; shortest == 0 || d < shortest {
			shortest = d
		}
	}
	if shortest == 0 {
		return ageSweepFloor
	}
	return max(shortest/10, ageSweepFloor)
}

// sweepBySize brings every channel back under its retention_bytes.
//
// It passes no deadline, so age is not evaluated here at all: the two rules
// run on their own clocks because they answer different questions, and one
// pass doing both would tie the tighter interval size needs to the looser
// one age wants.
//
// A latest channel has no size rule - it holds one value per topic rather
// than a history - so nothing here touches one.
func (b *Broker) sweepBySize() {
	for name, lg := range b.logStores() {
		c := b.reg.Get(name)
		if c == nil || c.RetentionBytes == 0 {
			continue
		}
		b.trimLog(name, lg, time.Time{}, c.RetentionBytes)
	}
}

// sweepByAge removes everything published before each channel's deadline.
func (b *Broker) sweepByAge(now time.Time) {
	for name, lg := range b.logStores() {
		c := b.reg.Get(name)
		if c == nil || c.RetentionPeriod == 0 {
			continue
		}
		b.trimLog(name, lg, deadline(now, c.RetentionPeriod), 0)
	}

	// A queue takes no retention, but its jobs take an expiry, and this is
	// the only thing that reaches one whose workers have all gone away -
	// deliver() is never asked for work nobody can receive, so the check it
	// makes on hand-over never runs there.
	//
	// **Only where the operator set a deadline.** A publisher's own Message
	// Expiry Interval ends no job, so a queue with no `job_expires_after`
	// has no clock to sweep on and is skipped entirely, which is also what
	// keeps a scan no index can serve off it.
	for name, q := range b.queueStores() {
		c := b.reg.Get(name)
		if c == nil || c.JobExpiresAfter == 0 || c.DLQ == nil {
			continue
		}
		b.mu.Lock()
		// **Unwrapped, the way releaseWith does it, and for its reason.**
		// A SQLite queue asks the dead-letter log whether it can be written
		// inside the queue's own transaction - the move is one transaction
		// or it is neither half (invariant 5) - and it asks by type
		// assertion. A metrics wrapper cannot answer that question, so the
		// bare store has to be the one handed over.
		//
		// The other path that builds a store.DeadLetter unwraps; this one
		// did not, so on SQLite every sweep failed, logged an ERROR and
		// counted a storage error - on a healthy disk, once per tick, from
		// the first sweep before anything was published. A job with no
		// worker was then held until one connected, which is the one case
		// this loop exists for.
		dlq, _ := storeBehind(b.logs[c.DLQ.Name]).(LogStore)
		b.mu.Unlock()
		if dlq == nil {
			continue
		}

		gone, err := q.ExpireOlderThan(deadline(now, c.JobExpiresAfter), store.DeadLetter{
			Log:    dlq,
			Record: b.deadLettered(c, "expired"),
		})
		// Reported after the records that did move, because the move is one
		// transaction per record: a failure part-way leaves the ones already
		// done, and those still have to reach their consumers.
		//
		// **Counted here as well as in releaseWith, because a record leaves
		// a queue by two paths and only one of them is a delivery.** That
		// one counts where a release applied; this one has no delivery to
		// release - the job aged out with no worker attached, which is the
		// case this loop exists for. Counting only there left both numbers
		// at zero while work was being dead-lettered: an operator watching
		// saguin_queue_expired_total saw nothing happening, and
		// saguin_queue_dead_lettered_total - which RFC 0005 makes the way to
		// learn a dead-letter channel is filling - agreed with it.
		cc := b.counted.forChannel(name)
		for _, out := range gone {
			if cc != nil {
				cc.deadLettered.Add(1)
				cc.expired.Add(1)
			}
			b.log.Warn("dead-lettered", "from", c.Name, "offset", out.Item.Offset,
				"to", c.DLQ.Name, "dlq_offset", out.Stored.Offset,
				"reason", "expired", "attempts", out.Item.Attempts)
			b.pumpAll(c.DLQ, out.Stored)
		}
		if err != nil {
			b.log.Error("cannot expire waiting jobs", "channel", name, "error", err)
		}
	}

	for name, lt := range b.latestStores() {
		c := b.reg.Get(name)
		if c == nil || (c.RetentionPeriod == 0 && c.DeletionRetentionPeriod == 0) {
			continue
		}
		// Two clocks. A deletion is held only so that a copy of this channel
		// can be told a topic is gone, so it expires on its own period -
		// otherwise a channel keeping values for ever would keep a row for
		// every device ever decommissioned, and nothing would bring the
		// topic count down again (RFC 0003).
		removed, freed, err := lt.Trim(
			deadline(now, c.RetentionPeriod), deadline(now, c.DeletionRetentionPeriod))
		switch {
		case err != nil:
			b.log.Error("cannot expire latest values", "channel", name, "error", err)
		case removed > 0:
			// Said at Info because this one is not obvious from outside: a
			// topic that expired stops existing, and a subscriber arriving
			// afterwards is told nothing about it. The log line is the only
			// place an operator can tell that from a device that went quiet.
			b.log.Info("expired latest values", "channel", name, "topics", removed,
				"freed_bytes", freed)
		}
	}

	// The retained store expires on the same rule and the same clock. It is
	// not a channel, so the loop above cannot reach it, and its period
	// defaults to none - a figure that could be defaulted to would lose the
	// state of a device reporting less often than it.
	b.mu.Lock()
	rt, period := b.retained, b.retainedPeriod
	b.mu.Unlock()
	if rt != nil && period > 0 {
		// The retained store holds no deletions: a zero-length retained
		// publish removes the value there, which is MQTT's own rule and is
		// not saguin's to change. So its second clock is never set.
		removed, freed, err := rt.Trim(deadline(now, period), time.Time{})
		switch {
		case err != nil:
			b.log.Error("cannot expire retained messages", "store", RetainedStoreName, "error", err)
		case removed > 0:
			b.log.Info("expired retained messages", "store", RetainedStoreName,
				"topics", removed, "freed_bytes", freed)
		}
	}
	if rt != nil {
		// The publisher's own clock, beside the operator's: a retained
		// value dies at the earlier of its Message Expiry Interval and the
		// period above (MQTT-3.3.2-5), and this one runs whether or not an
		// operator configured a period at all. Deleted silently, as
		// retention deletes - the log line is the operator's, and nothing
		// is broadcast, because an expiry is not a deletion anybody
		// published.
		//
		// The tick this rides is derived from the operator's deadlines,
		// which cannot see a per-record expiry - so a value can outlive
		// its expiry until the next tick. What holds the promise in that
		// window is deliverRetained, which skips an expired value rather
		// than serving it; this sweep is where the bytes come back.
		removed, freed, err := rt.TrimExpired(now)
		switch {
		case err != nil:
			b.log.Error("cannot expire retained messages", "store", RetainedStoreName, "error", err)
		case removed > 0:
			b.log.Info("expired retained messages", "store", RetainedStoreName,
				"topics", removed, "freed_bytes", freed, "clock", "message_expiry")
		}
	}
}

// trimLog is one channel's removal, and what to say about it.
//
// A store that fails is logged and left for the next tick rather than
// stopping the sweep: stopping would leave every channel after it unswept
// for as long as the failure lasted, which on a full provider is exactly
// when retention is what would relieve them. Nothing was removed and
// nothing moved, so the next tick tries the same work again.
func (b *Broker) trimLog(name string, lg LogStore, before time.Time, maxBytes int64) {
	removed, freed, err := lg.Trim(before, maxBytes)
	switch {
	case err != nil:
		b.log.Error("cannot sweep a channel", "channel", name, "error", err)
	case removed > 0:
		// Both sweeps come through here, so the counter covers retention by
		// age and by size without either having to remember it. Size-based
		// trimming is the one that gets missed, being the one that does not
		// feel like deleting data (invariant 1).
		if cc := b.counted.forChannel(name); cc != nil {
			cc.retentionRemoved.Add(uint64(removed))
		}
		b.log.Info("swept", "channel", name, "removed", removed, "freed_bytes", freed,
			"floor", lg.Floor())
	}
}

// The two store maps, copied under the lock so that no removal is done
// holding it. A queue has neither map and is never swept: removing
// unacknowledged work by age or size is eviction of unresolved work, which
// is never permitted (invariant 2). QueueStore has no Trim at all, so that
// is the compiler's rule rather than one anybody has to remember.
func (b *Broker) logStores() map[string]LogStore {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]LogStore, len(b.logs))
	for k, v := range b.logs {
		out[k] = v
	}
	return out
}

func (b *Broker) queueStores() map[string]QueueStore {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]QueueStore, len(b.queues))
	for k, v := range b.queues {
		out[k] = v
	}
	return out
}

func (b *Broker) latestStores() map[string]LatestStore {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]LatestStore, len(b.latest))
	for k, v := range b.latest {
		out[k] = v
	}
	return out
}

// deadline turns a retention period into the moment before which a record
// is too old, or the zero time when the channel keeps everything.
func deadline(now time.Time, periodSeconds int64) time.Time {
	if periodSeconds <= 0 {
		return time.Time{}
	}
	return now.Add(-time.Duration(periodSeconds) * time.Second)
}

// expireLeases returns records whose visibility deadline has passed. The
// deadline only ever runs while a record is Leased - that is, after the
// worker's PUBACK released its in-flight slot (invariant 7).
func (b *Broker) expireLeases() {
	now := time.Now()

	b.mu.Lock()
	queues := make(map[string]QueueStore, len(b.queues))
	for k, v := range b.queues {
		queues[k] = v
	}
	b.mu.Unlock()

	for name, q := range queues {
		expired, err := q.ExpiredLeases(now)
		if err != nil {
			// Nothing was changed. Every deadline that has passed is still
			// past on the next tick, so this is a delay in reclaiming work
			// rather than work lost.
			b.log.Error("cannot look for expired leases", "channel", name, "error", err)
			continue
		}
		for _, h := range expired {
			b.releaseDelivery(&delivery{
				channel: name, offset: h.Offset, epoch: h.Epoch, holder: h.Holder,
			}, "visibility timeout", false)
		}
	}
}

// returnUnsent gives back every record the server registered as being
// delivered and then never put on the wire.
//
// It exists because a Delivering record has no visibility deadline by
// design: the deadline starts at the PUBACK, so that it can never run while
// the worker still holds the packet identifier (invariant 7). That rests on
// the PUBACK eventually arriving, and for one record it may not. Past a
// worker's Receive Maximum the server registers the next record in its
// in-flight set, tells saguin a worker has it, and then declines to write
// it until the worker acknowledges something - which a worker that has
// stopped acknowledging never does, so the record sits in Delivering with no
// deadline, no attempt spent, and no worker, until the session ends.
//
// **It is not a deadline on Delivering, and that is the point.** A clock
// there cannot tell a worker that never received the record from one that
// received it and is doing the work - with Paho, and with most libraries,
// the PUBACK is sent when the handler returns, so "no PUBACK yet" is the
// ordinary state of a job in progress. Any clock short enough to rescue the
// first takes the job off the second and has it done twice, which is the
// degradation invariant 7 documents itself as buying. This asks the
// question the clock was standing in for instead, and answers it exactly:
// the record comes back within a tick, and a worker mid-job is never
// touched.
//
// Nothing here spends an attempt. The worker never showed it had the
// record - it never had it - so this is the send that did not happen rather
// than a delivery that failed (RFC 0003).
//
// A record returned here cannot also reach the worker afterwards. The packet
// leaves the worker's in-flight set in the same step that finds it withheld,
// and the substrate claims a withheld packet before it writes one, so the two
// cannot both have it (Inflight.DeleteWithheld).
func (b *Broker) returnUnsent() {
	// Collected under the lock and released after it, because releasing
	// takes the lock itself and writes to a store.
	b.mu.Lock()
	var unsent []*delivery
	for _, d := range b.deliveries {
		if d.holder == "" {
			continue // not yet handed to a worker at all
		}
		if _, waiting := b.byPacket[packetKey(d.holder, d.packetID)]; !waiting {
			// The PUBACK arrived, so the record is Leased and its visibility
			// deadline is running. That is expireLeases's business.
			continue
		}
		cl, ok := b.srv.Clients.Get(d.holder)
		if !ok || cl.Closed() {
			continue // the session is ending; OnDisconnect returns what it held
		}
		// **Removed from the worker's in-flight set in the same step that
		// finds it withheld**, so it cannot be written after it is returned.
		// A withheld packet is sent when the worker's window frees, and one
		// sent after this would put the job in two workers' hands; the
		// substrate claims a packet before writing it, and whichever of the
		// two gets there first has it (Inflight.DeleteWithheld).
		if !cl.State.Inflight.DeleteWithheld(d.packetID) {
			continue
		}
		unsent = append(unsent, d)
	}
	b.mu.Unlock()

	stuck := map[string]int{}
	for _, d := range unsent {
		stuck[d.holder]++
		b.releaseDelivery(d, "never sent", false)
	}

	// One line per worker rather than per record, and at Warn: a worker whose
	// window stayed full for a whole tick with work registered to it is one
	// that has stopped acknowledging, and nothing else says so - which is
	// what made a starved worker indistinguishable from an empty queue.
	for clientID, n := range stuck {
		b.log.Warn("returned records this worker's connection never carried; "+
			"its in-flight window stayed full",
			"worker", clientID, "returned", n)
	}
}

// deliver offers available work to the queue's workers. saguin picks which
// worker receives it (OnSelectSubscribers) from its own index of who is
// consuming the queue, and learns the packet identifier it went out under
// from OnQosPublish.
func (b *Broker) deliver() {
	b.deliverTo(nil)
}

// deliverTo is deliver for the named queues only, or for all of them when
// only is nil.
func (b *Broker) deliverTo(only map[string]bool) {
	b.mu.Lock()
	queues := make(map[string]QueueStore, len(b.queues))
	for k, v := range b.queues {
		queues[k] = v
	}
	b.mu.Unlock()

	for name, q := range queues {
		if only != nil && !only[name] {
			continue
		}
		c := b.reg.Get(name)
		if c == nil {
			continue
		}
		// Never more than the live workers can hold. A record offered beyond
		// that is registered in a worker's in-flight set, reported to saguin
		// as delivered, and then withheld by the server because the worker's
		// send quota is spent - and nothing resends it, so it is held by a
		// worker that never received it, with no deadline to rescue it
		// (invariant 7). Nobody is subscribed is the same question with the
		// answer zero.
		room := b.roomFor(c)
		if room == 0 {
			continue
		}

		// One moment for the whole pass: what the backoff measures against,
		// and what the expiry check below compares to. Two calls would let a
		// record be ready for one and not the other.
		now := time.Now()
		offered, err := q.Offer(room, now)
		if err != nil {
			// Nothing was handed out, so nothing is held by a worker that
			// will never hear about it. The next tick tries again.
			b.log.Error("cannot offer work", "channel", name, "error", err)
			continue
		}
		for _, o := range offered {
			d := &delivery{channel: name, offset: o.Offset, epoch: o.Epoch}
			b.mu.Lock()
			b.deliveries[o.DeliveryID] = d
			b.mu.Unlock()

			// A job past its expiry is dead-lettered "without further
			// delivery" (RFC 0003), so the check is here rather than after
			// the hand-over: a worker must never be given work the broker
			// has already decided is too old to do.
			//
			// It is out of time rather than out of attempts, so it goes
			// whatever the attempt count says - a job that expires before it
			// is ever delivered is dead-lettered on its first offer.
			if expired(c, o.Record, now) {
				b.releaseWith(d, "expired", false, "expired")
				continue
			}
			// Not counted here: a nil error does not mean a worker has it.
			// The selection runs inside this call and can give the record
			// back with no error - nobody live with room, or a job larger
			// than the chosen worker takes - so the count is taken where a
			// worker is known to hold it, in OnQosPublish.
			if err := b.offer(c, o.Record, o.DeliveryID, o.Attempt); err != nil {
				b.log.Warn("delivery failed", "channel", name, "offset", o.Offset, "error", err)
				b.releaseDelivery(d, "delivery failed", false)
				continue
			}
		}
	}
}

// roomFor is how many deliveries this queue's live workers can take right
// now: one for each that holds no job from it and has room in its in-flight
// window. Zero when nobody is subscribed, which is also the answer to "should
// anything be offered at all".
//
// It is a bound and not a reservation. The window is shared with whatever
// else saguin is delivering to the same session - a worker may read an
// append channel too - so it can close between here and the handover. What
// catches that is the same check applied to the one worker chosen, in
// OnSelectSubscribers.
func (b *Broker) roomFor(c *channel.Channel) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	room := 0
	for id := range b.members[c.Name] {
		if cl, ok := b.srv.Clients.Get(id); ok && !cl.Closed() &&
			!b.holds(id, c.Name) && b.window(cl) > 0 {
			room++
		}
	}
	return room
}

// offer injects the delivery carrying the two MQTT 5 properties a worker
// needs to answer: where to reply, and which attempt it is answering for.
func (b *Broker) offer(c *channel.Channel, r store.Record, deliveryID string, attempt int) error {
	raw, err := hex.DecodeString(deliveryID)
	if err != nil {
		return err
	}
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   r.Topic,
		Payload:     r.Payload,
		PacketID:    1, // replaced per client by the server
		Properties: packets.Properties{
			ResponseTopic:   c.ResponseTopic(),
			CorrelationData: raw,
			// **No `saguin-channel`.** A worker's pin names one queue -
			// that is the only subscription form a queue admits (invariant
			// 4) - so a queue offer is never ambiguous about which channel
			// it came from, and paying 26 bytes a job to say what the
			// worker already named would be paying it for nothing.
			User: append(userProps(r, ""),
				packets.UserProperty{Key: "saguin-attempt", Val: strconv.Itoa(attempt)}),
		},
	}
	// The publisher's own Response Topic and Correlation Data are held back
	// here and nowhere else: this delivery's pair is what a worker answers
	// on and the Delivery ID it answers with. The record keeps the
	// publisher's, and the dead-letter channel delivers them.
	applyProps(&pk, r, time.Now(), true)

	// Debug: a line per job is gigabytes a day on a busy queue, and
	// saguin_queue_delivered_total counts them (TestNothingPerRecordLogsAtInfo).
	b.log.Debug("offering", "channel", c.Name, "offset", r.Offset, "delivery", deliveryID[:8])
	return b.srv.InjectPacket(b.inline, pk)
}

// sweepHeldPublishes drops the exactly-once publishes that have waited
// longer than `broker.qos2.expires_after` for a PUBREL that is not coming.
//
// **What it is for is the publisher that went away without its session
// ending.** A session that ends takes its held messages with it, and that
// is the ordinary path - but `limits.max_session_expiry` lets a session
// last thirty days by default, and a message held for thirty days on the
// chance of a PUBREL is a provider's bytes spent on nothing. A connected
// publisher releases within a round trip, so nothing in ordinary running is
// ever old enough to be seen here.
//
// **The publisher is not told**, and cannot be: it has had its PUBREC and
// MQTT gives a server no way to withdraw one. What it gets is `0x92 Packet
// Identifier not found` if it does send the PUBREL, which is the honest
// answer. saguin_qos2_abandoned_total is where an operator sees this
// happening, and it is the only place.
func (b *Broker) sweepHeldPublishes(now time.Time) {
	b.mu.Lock()
	after := b.pendingExpires
	b.mu.Unlock()
	if after <= 0 {
		return
	}
	before := now.Add(-after)
	b.heldMu.Lock()
	var due []store.Exchange
	for e, h := range b.held {
		if h.at.Before(before) {
			due = append(due, e)
		}
	}
	b.heldMu.Unlock()
	if len(due) == 0 {
		return
	}

	// **One exchange at a time, under its client id's session lock**, as a
	// release runs: an expiry and a release of the same exchange never
	// interleave, so a PUBREL racing the expiry either releases it first or
	// finds it gone with the substrate's entry, and is answered 0x92 - never
	// completed for a message nobody has.
	n := 0
	for _, e := range due {
		unlock := b.lockSession(e.Client)
		b.heldMu.Lock()
		h, still := b.held[e]
		b.heldMu.Unlock()
		if still && h.at.Before(before) {
			dropped := true
			if st := b.holdsFor(h.channel); st != nil {
				if _, err := st.DropHold(e); err != nil {
					b.log.Error("cannot drop an unfinished exactly-once publish that timed out",
						"client", b.limits.Loggable(e.Client), "channel", h.channel, "error", err)
					dropped = false
				}
			}
			if dropped {
				b.forgetHeld(e)
				b.retainedStands(e, h, "timed out")
				// **The substrate's side goes with saguin's.** It answers a
				// PUBREL from its own table, which still holds the PUBREC
				// this exchange started with, so without this a client whose
				// message had just been swept was told the exchange
				// completed for a record nobody has.
				b.forgetExchange(e.Client, e.PacketID)
				n++
			}
		}
		unlock()
	}
	if n == 0 {
		return
	}
	b.counted.qos2Abandoned.Add(uint64(n))
	b.log.Info("dropped unfinished exactly-once publishes that timed out",
		"publishes", n, "expires_after", after)
}
