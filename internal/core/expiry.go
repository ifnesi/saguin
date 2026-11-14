package core

import (
	"errors"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// sessionExpiry serves session and Will deadlines, holding one bounded batch
// and the current pass's cutoff and
// continuation point. changed keeps the earliest deadline noted since the
// pass began, until the pass ends.
// Between reads next may only come earlier.
type sessionExpiry struct {
	next       int64
	retry      bool
	firedRetry bool
	changed    int64
	at         int64
	after      store.DueSession
	reading    bool
	fired      bool
	batch      []store.DueSession
	ending     int
}

// Timer returns the earliest core deadline, or zero when none is needed.
func (c *Core) Timer() time.Time {
	if c.phase == stopping {
		return firstTime(c.stopAt, c.takeoverCuts.first())
	}
	if c.phase >= quiescing {
		return time.Time{}
	}
	at := firstTime(firstTime(firstTime(firstTime(c.commitAt, c.syncAt), c.snapshot.at), c.admissionTimer()), firstTime(c.queueHolds.first(), c.queueRetries.first()))
	at = firstTime(at, c.bridgeTimer())
	at = firstTime(at, c.takeoverCuts.first())
	at = firstTime(at, c.memoryRecoveryTimer())
	if next := earlier(earlier(earlier(earlier(c.expiry.next, c.willExpiry.next), c.retained.next), c.retryAt), c.metricAt); next != 0 {
		expires := time.UnixMilli(next)
		if at.IsZero() || expires.Before(at) {
			at = expires
		}
	}
	return at
}

// Fire handles the stop cutoff, session expiry, full-store retry and commit
// cutoff in R2's order. Fires while a pass is unfinished coalesce; the pass
// continues from its last entry regardless of storage refusal (Store).
func (c *Core) Fire(acts []Action, now time.Time) []Action {
	c.wakeTicket++
	if c.admission.delay != 0 {
		c.eventAt = now
	}
	if c.phase == stopping {
		next := c.takeoverCuts.first()
		if !c.stopCutDue(now) && (next.IsZero() || now.Before(next)) {
			return acts
		}
		if c.stopCutDue(now) {
			c.stopAt = time.Time{}
			acts = c.cutConnections(acts, now)
		}
		return c.dispatch(c.cutTakeovers(acts, now), now)
	}
	if c.phase >= quiescing {
		return acts
	}
	takeoverAt := c.takeoverCuts.first()
	acts = c.cutTakeovers(acts, now)
	if c.syncEvery != 0 && c.syncAt.IsZero() && !c.syncPending {
		c.syncAt = now.Add(c.syncEvery)
	}
	if c.snapshot.every != 0 && c.snapshot.at.IsZero() && !c.snapshot.pending {
		c.snapshot.at = now.Add(c.snapshot.every)
	}
	// Fire announces the earliest timer. With no deadline it starts (or
	// requests) an expiry pass. Expiry and retry share one pass; another
	// timer alone must not start one or consume a future expiry deadline.
	retrying := false
	next := earlier(earlier(c.expiry.next, c.willExpiry.next), c.retryAt)
	// Preserve the expiry pass's existing Fire contract: without a competing
	// cutoff, a fire requests a pass even while its saved deadline is future.
	// An earlier cutoff identifies another timer, including memory recovery.
	// A metric deadline competes only when due: the initial fire must still
	// discover overdue sessions before any expiry deadline is known.
	cutoff := firstTime(firstTime(c.commitAt, c.admissionTimer()), takeoverAt)
	// Startup must discover overdue sessions even when its first restored
	// deadline belongs to a queue. Afterwards queue timers compete normally.
	if c.phase != starting {
		cutoff = firstTime(cutoff, firstTime(c.queueHolds.first(), c.queueRetries.first()))
		cutoff = firstTime(cutoff, c.bridgeTimer())
	}
	if !c.syncAt.IsZero() && !now.Before(c.syncAt) {
		cutoff = firstTime(cutoff, c.syncAt)
	}
	if c.metricAt != 0 && c.metricAt <= now.UnixMilli() {
		cutoff = firstTime(cutoff, time.UnixMilli(c.metricAt))
	}
	if !c.snapshot.at.IsZero() && !now.Before(c.snapshot.at) {
		cutoff = firstTime(cutoff, c.snapshot.at)
	}
	if recovery := c.memoryRecoveryTimer(); !recovery.IsZero() && !now.Before(recovery) {
		cutoff = firstTime(cutoff, recovery)
	}
	expiryFirst := cutoff.IsZero() || (next != 0 && (next <= now.UnixMilli() || !cutoff.Before(time.UnixMilli(next))))
	if expiryFirst {
		c.expiry.next, c.expiry.fired = 0, true
		if c.retryAt != 0 && c.retryAt <= now.UnixMilli() {
			c.retryAt = 0
			c.expiry.firedRetry, c.willExpiry.firedRetry = true, true
			if c.willCount != 0 {
				c.willExpiry.next, c.willExpiry.fired = 0, true
			}
			retrying = true
			for _, ch := range c.channels {
				if ch.retentionRetry {
					ch.retentionRetry = false
					c.scheduleRetention(ch)
				}
			}
		}
	}
	if c.willExpiry.next != 0 && c.willExpiry.next <= now.UnixMilli() {
		c.willExpiry.next, c.willExpiry.fired = 0, true
	}
	if c.retained.next != 0 && c.retained.next <= now.UnixMilli() {
		c.retained.next, c.retained.fired = 0, true
		c.retentionReady = true
	}
	// Reserve due session, Will and retention requests before retrying
	// delivery bookkeeping. Their views still follow accepted predecessors.
	c.sweep(now)
	c.expireQueueHolds(now)
	c.retainAge(now)
	if retrying {
		c.retryDeliveries()
	}
	c.retryQueues(now)
	acts = c.fireBridges(acts, now)
	acts = c.giveBytes(acts, now)
	// The expiry read's barrier takes its turn before an equal commit
	// cutoff. Dispatch the group only after both timers have acted.
	acts = c.dispatchRequests(acts, now)
	if !c.commitAt.IsZero() && !now.Before(c.commitAt) {
		c.dueGroup(commitInterval)
	}
	if !c.syncAt.IsZero() && !now.Before(c.syncAt) {
		if c.syncDirty {
			c.syncAt, c.syncPending = time.Time{}, true
			c.enqueue(&storeRequest{kind: syncStore, op: &store.Sync{}})
		} else {
			c.syncAt = now.Add(c.syncEvery)
		}
	}
	if c.metricAt != 0 && c.metricAt <= now.UnixMilli() && !c.metricsReading {
		acts = c.refreshMetrics(acts, now)
	}
	if !c.snapshot.at.IsZero() && !now.Before(c.snapshot.at) {
		if c.snapshot.dirty {
			c.startSnapshot(now, false)
		} else {
			c.snapshot.at = now.Add(c.snapshot.every)
		}
	}
	acts = c.dispatch(acts, now)
	c.recoverMemory(now)
	return c.flushLogs(acts)
}

// takeoverCutNeeded covers a takeover's pending final entry and an ordinary
// departure's pending writer reports. Store work alone never needs this cut.
func (s *connectionState) takeoverCutNeeded() bool {
	if s.end.reason != 0x8E && s.writingDeliveries == 0 {
		return false
	}
	return !s.conn.FinalTaken()
}

// armTakeoverCut fixes one cutoff from the takeover's final entry or the
// departure's first waiting CONNECT. Later CONNECTs cannot postpone it.
func (c *Core) armTakeoverCut(s *connectionState, at time.Time) {
	if s.cut || s.takeoverCut.index != 0 || c.connections[s.conn] != s || !s.takeoverCutNeeded() {
		return
	}
	s.takeoverCut.conn = s
	c.takeoverCuts.set(&s.takeoverCut, at.Add(500*time.Millisecond))
}

// armDeparture runs once when a departure joins the lane. Pending requests
// already hold earlier CONNECTs in intake order; CONNECT intake never scans.
func (c *Core) armDeparture(lane *sessionLane) {
	s := lane.departing.conn
	if s.cut || s.takeoverCut.index != 0 || !s.takeoverCutNeeded() {
		return
	}
	for _, r := range lane.pending {
		if r.kind == connectSession {
			c.armTakeoverCut(s, r.conn.attempt.at)
			return
		}
	}
}

// cutTakeovers visits only due owners, in connection-key order on equal
// deadlines. The connection's state decides whether it still needs the cut,
// independently of its departure request's place in the lane.
func (c *Core) cutTakeovers(acts []Action, now time.Time) []Action {
	for c.takeoverCuts.Len() != 0 {
		d := c.takeoverCuts.items[0]
		if now.Before(d.at) {
			break
		}
		s := d.conn
		c.takeoverCuts.set(d, time.Time{})
		if s.takeoverCutNeeded() {
			acts = c.cut(acts, s, 0, now) // keep the connection's first closure cause
			acts = c.finish(acts, s, s.end.reason, now)
		}
	}
	return acts
}

// expiryAt adds an interval in seconds to a Unix millisecond timestamp.
// Session callers exclude NeverExpires; Message Expiry is always finite.
func expiryAt(left int64, seconds uint32) int64 {
	return left + int64(seconds)*1000
}

func earlier(a, b int64) int64 {
	if a == 0 || (b != 0 && b < a) {
		return b
	}
	return a
}

// noteAway keeps a finite away session's deadline.
func (c *Core) noteAway(left int64, expiry uint32) {
	if left == 0 || expiry == 0 || expiry == store.NeverExpires {
		return
	}
	c.noteExpiry(expiryAt(left, expiry))
}

// noteExpiry brings the timer forward and preserves the deadline through
// an unfinished pass, whose read may not include it in Next.
func (c *Core) noteExpiry(at int64) {
	c.expiry.note(at)
}

// note preserves deadlines arriving after a pass fixed its store view.
func (s *sessionExpiry) note(at int64) {
	s.next = earlier(s.next, at)
	if s.at != 0 {
		s.changed = earlier(s.changed, at)
	}
}

func (s *sessionExpiry) due(d *store.DueSessions) {
	s.reading = false
	s.next = earlier(d.Next, s.changed)
	if len(d.Sessions) == store.DueBatch && d.Next != 0 && d.Next <= d.At {
		s.after = d.Sessions[len(d.Sessions)-1]
		s.next = s.changed
	} else {
		s.after = store.DueSession{}
	}
	s.batch = d.Sessions
}

// sweep uses the same pass for session endings and Will deadlines. Session
// endings precede Will-only work, and a pass holds at most one metadata page.
func (c *Core) sweep(now time.Time) {
	c.sweepDue(&c.expiry, false, now)
	if c.expiry.at == 0 && !c.expiry.reading && c.expiry.ending == 0 {
		c.sweepDue(&c.willExpiry, true, now)
	}
}

func (c *Core) sweepDue(s *sessionExpiry, wills bool, now time.Time) {
	for len(s.batch) != 0 {
		d := s.batch[0]
		s.batch[0] = store.DueSession{}
		s.batch = s.batch[1:]
		if d.Shared {
			if g := c.shares[d.Client]; g != nil {
				continue
			}
			c.scheduleSharedCleanup(store.SharedGroup{ID: d.ID, Filter: d.Client})
			return
		}
		if d.HeldOnly && c.sessionsByID[d.ID] != nil {
			continue // a backing row without a sessions row can still have a live owner
		}
		if lane := c.lanes[d.Client]; lane != nil {
			if wills {
				lane.willDue = earlier(lane.willDue, d.Expiry)
			} else {
				lane.expiry = earlier(lane.expiry, d.Expiry)
				lane.heldDue = lane.heldDue || d.HeldOnly
			}
			continue
		}
		if current := c.sessions[d.Client]; current != nil && ((!wills && !d.HeldOnly) || current.session.ID == d.ID) {
			continue
		}
		s.ending++
		r := &storeRequest{kind: endExpired, lane: c.lane(d.Client), op: &store.EndSession{ID: d.ID}, willInfo: d.Will}
		if d.HeldOnly {
			r.kind = endHeld
			r.op.(*store.EndSession).KeepWill = true
		}
		cause := willSessionEnd
		if d.Will.Due.Valid && d.Will.Due.Int64 < d.Expiry {
			cause = willLost
		}
		r.willCause = c.recoveredCause(d.Will, cause, s.retry)
		if wills {
			r.kind, r.op = endWill, &store.SaveWill{Session: d.ID}
			r.willCause = c.recoveredCause(d.Will, willLost, s.retry)
		}
		c.enqueue(r)
		return
	}
	if s.reading || s.ending != 0 {
		return
	}
	if s.after.ID == 0 {
		s.at, s.changed = 0, 0
		if !s.fired {
			return
		}
		s.fired, s.at = false, now.UnixMilli()
		s.retry, s.firedRetry = s.firedRetry, false
	}
	s.reading = true
	d := store.DueSessions{At: s.at, After: s.after, Retry: s.retry}
	r := &storeRequest{kind: dueSessions, op: &d}
	if wills {
		r.kind, r.op = dueWills, &store.DueWills{DueSessions: d, Retry: s.retry}
	}
	c.enqueue(r)
}

// expired releases the ending on every answer. A full refusal schedules
// rediscovery from the store, retaining neither the write nor the lane.
func (c *Core) expired(err error, now time.Time, session bool) {
	c.expiry.ending--
	if err == nil && session {
		c.endedCounts[expired].Inc()
		if c.phase == starting {
			c.life.ended++
		}
	} else if errors.Is(err, store.ErrFull) {
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
	}
}

// readChannelMetrics ends the refresh after all its pages used one read view.
func (c *Core) readChannelMetrics(now time.Time) {
	c.metricsReading, c.metricAt = false, now.UnixMilli()+c.metricEvery
}

// refreshMetrics places one refresh in the existing Store-read order. Its
// core-owned contribution is captured only when that request can dispatch.
func (c *Core) refreshMetrics(acts []Action, now time.Time) []Action {
	c.metricAt, c.metricsReading = 0, true
	c.enqueue(&storeRequest{kind: readChannelMetrics, op: &store.MetricRefresh{
		Channels: c.metricChannels, Gauges: c.metricGauges, Measure: c.metricMeasure,
	}})
	return c.dispatchRequests(acts, now)
}
