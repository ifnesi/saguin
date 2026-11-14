package core

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
)

// corePhase orders startup and the graceful stop. Starting and running take
// all events; quiescing waits for main to close the listeners after a failure.
// Stopping takes reader ends, close reports and the socket cutoff, ignoring
// other packets. Draining closes store-reading view admission and settles
// admitted work; stopped awaits main's final stop. Both still answer live
// operations requests and admit their failure reports.
type corePhase uint8

const (
	starting corePhase = iota
	running
	quiescing
	stopping
	draining
	stopped
)

// Ready is closed by the core after its startup expiry pass has settled.
// Main binds listeners only after this notification.
func (c *Core) Ready() <-chan struct{} { return c.initialized }

// Drained is closed after every accepted connection has reported its close
// and every read and write sent to the store has been answered.
func (c *Core) Drained() <-chan struct{} { return c.drained }

// Failures reports the first store failure to main, without waiting (R8).
func (c *Core) Failures() <-chan error { return c.failures }

// Stop requests the graceful phase, after main has closed and awaited the
// listeners. Main calls it exactly once; Run continues taking connection
// events and store answers until every connection and request has ended.
// Main then awaits the workers and closes Run's final stop channel.
func (c *Core) Stop() { c.StopFor("requested") }

// StopFor publishes main's stop cause with the existing shutdown notification.
func (c *Core) StopFor(cause string) {
	c.life.stopCause = cause
	close(c.shutdown)
}

func (c *Core) fail(acts []Action, err error, now time.Time) []Action {
	if c.failure != nil {
		return acts
	}
	if c.snapshot.every != 0 {
		err = fmt.Errorf("RAM copy %s retained: %w", c.snapshot.source, err)
	}
	c.failure = err
	c.readPermit = nil
	c.openAdmission(now)
	acts = c.logFailure(acts, logger.Error, err, now)
	c.failures <- err
	// Sent batches retain their charges until their answers; unsent bodies
	// can no longer enter the failed store and are discarded now.
	for r := range c.parked {
		c.readyRequest(r)
	}
	c.orderedRead = nil
	for _, r := range c.ready[c.readyAt:] {
		c.discardWrite(r, now)
		switch r.kind {
		case readOperations:
			c.answerOperation(r, err)
		case readDeliveryPayload:
			acts = c.readDeliveryPayload(acts, r, now)
		case readDeliveryMetadata:
			acts = c.readDeliveryMetadata(acts, r, now)
		case readResumedDeliveries:
			acts = c.readResumedDeliveries(acts, r, now)
		}
	}
	if c.collecting != nil {
		c.discardGroup(c.collecting, now)
		c.collecting = nil
	}
	for _, g := range c.sealed {
		c.discardGroup(g, now)
	}
	clear(c.sealed)
	c.sealed = c.sealed[:0]
	c.commitAt = time.Time{}
	for _, ch := range c.channels {
		ch.pending = nil // every pending channel request was already in ready
	}
	// A consumer's write may still be waiting for its session lane. It was
	// never dispatched, so its immutable snapshot can be returned now too.
	for _, lane := range c.lanes {
		if lane.request != nil && lane.request.kind == fireWill {
			c.discardWrite(lane.request, now)
		}
		for _, r := range lane.pending {
			c.discardWrite(r, now)
		}
	}
	for s := c.firstConn; s != nil; s = s.next {
		c.discardPublishes(&s.publishes, now)
		c.cancelDelivery(s, now)
	}
	c.ready = nil // unsent requests cannot be started on the failed store
	c.readyAt = 0
	if c.phase < quiescing {
		c.phase = quiescing
	} else if c.phase == stopping {
		if c.stopCutDue(now) {
			acts = c.cutConnections(acts, now)
		}
		acts = c.closeConnections(acts, now)
	}
	c.updateReady()
	return acts
}

// beginStop is the core's decision on the stop, which Run takes once main
// has called Stop. It appends the stop's actions at now in execution order;
// subsequent events follow the graceful stop's acceptance rules.
func (c *Core) beginStop(acts []Action, now time.Time) []Action {
	if c.failure != nil {
		c.life.stopCause = "store failure: " + c.failure.Error()
	}
	c.queueLog(logger.Info, "stopping: "+c.life.stopCause, now)
	c.life.closed = len(c.connections)
	c.wakeTicket++
	if c.admission.delay != 0 {
		c.eventAt = now
	}
	c.openAdmission(now)
	c.phase = stopping
	c.stopBridges()
	c.updateReady()
	// Answers awaiting preparation need no new delivery after the stop.
	// Complete their accepted requests before dispatch emits final entries;
	// their replies must not wait for a peer to free preparation capacity.
	for s := c.firstConn; s != nil; s = s.next {
		acts = c.admitServiceAnswers(acts, s)
	}
	for r := c.memoryHead[2]; r != nil; {
		next := r.next
		c.unqueueMemory(r, now)
		acts = c.deferWill(acts, r.will, now)
		r = next
	}
	c.publishBarrier = c.nextRequest
	c.sealGroup(commitStop)
	c.stopAt = now.Add(c.limits.WriteTimeout)
	return c.dispatch(acts, now)
}

// stopCutDue keeps the stop's cutoff fixed at its beginning. Once Fire
// spends it, later store answers and queued accepts still require a cut.
func (c *Core) stopCutDue(now time.Time) bool {
	if c.phase != stopping {
		return false
	}
	return c.stopAt.IsZero() || !now.Before(c.stopAt)
}

// disconnectPending protects an expiry change until its departure write
// answers, including a CONNECT save that must finish before that write.
// A failed store cannot confirm the change and no longer holds the socket.
func (c *Core) disconnectPending(s *connectionState) bool {
	if !s.end.changed || c.failure != nil {
		return false
	}
	lane := c.lanes[s.clientID]
	if lane == nil {
		return false
	}
	r := lane.request
	if r != nil && r.conn == s && (r.kind == saveConnect || r.kind == leaveSession || r.kind == fireWill || r.kind == readWill) {
		return true
	}
	for _, pending := range lane.pending {
		if pending.conn == s && (pending.kind == leaveSession || pending.kind == fireWill) {
			return true
		}
	}
	return false
}

// cut closes a raw socket at most once and never after its close report.
// The caller follows it with the writer's final entry if still needed (R2).
func (c *Core) cut(acts []Action, s *connectionState, reason byte, now time.Time) []Action {
	c.takeoverCuts.set(&s.takeoverCut, time.Time{})
	if s.cut || c.connections[s.conn] != s {
		return acts
	}
	if reason >= 0x80 {
		c.selectClosure(s, packet.DISCONNECT, reason, now)
	}
	s.cut = true
	return append(acts, Action{Conn: s.conn, Cut: true, Cause: s.closure.cause})
}

// cutConnections enforces the stop's cutoff independently of queued replies.
// A held DISCONNECT is cut by its store answer once that protection ends.
func (c *Core) cutConnections(acts []Action, now time.Time) []Action {
	for s := c.firstConn; s != nil; s = s.next {
		if c.disconnectPending(s) {
			continue
		}
		reason := byte(0x8B)
		if s.end.seen {
			reason = s.end.reason
		}
		acts = c.cut(acts, s, reason, now)
		acts = c.finish(acts, s, reason, now)
	}
	return acts
}

// closeConnections follows this event's acknowledgements with final entries
// in connection-key order. Accepted changes hold a connected publisher's
// entry until their replies are ready; only a client's own DISCONNECT change
// also holds its socket past the stop cutoff. Failure releases both holds.
func (c *Core) closeConnections(acts []Action, now time.Time) []Action {
	for s := c.firstConn; s != nil; s = s.next {
		switch {
		case s.bridge != nil && !s.bridge.up && s.phase != closing:
			// An in-progress establishment is abandoned at once, in the
			// same connection-key order as every other final entry.
			acts = c.finish(c.cut(acts, s, 0, now), s, 0, now)
		case s.end.seen:
			if !c.disconnectPending(s) {
				acts = c.finish(acts, s, s.end.reason, now)
			}
		case c.failure != nil || s.phase != connected || s.pendingAcks == 0:
			acts = c.finish(acts, s, 0x8B, now)
		}
	}
	return acts
}
