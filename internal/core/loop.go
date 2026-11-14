package core

import (
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/store"
)

// InboundCapacity bounds the readers' shared handoff (R2). PUBLISH bodies
// remain charged to their reservations while queued here (R6).
const InboundCapacity = 64

// Run owns no goroutine of its own: the caller runs it on the core's. A
// blocking receive wakes the ordered checks, retaining the event it took,
// just as the store writer does. Stop, main reloads, store answers, logger results, timer,
// then readers decide every boundary; store credit gates the shared channel.
// Publication capacity gates only new body grants (R2, R3).
func (c *Core) Run(inbound chan connection.Event, answers <-chan store.Answer,
	results <-chan logger.Result, stop <-chan struct{}, clock connection.Clock) {
	c.goroutine.Add(1)
	defer c.goroutine.Add(-1)
	c.bridgeInbound, c.bridgeClock = inbound, clock
	acts := c.Fire(nil, clock.Now())
	var at time.Time
	var ticks <-chan time.Time
	var timer <-chan time.Time
	var reset func(time.Time)
	var event connection.Event
	var answer store.Answer
	var result logger.Result
	var logged bool
	defer func() {
		// A wakeup may have taken a result just before main's final stop.
		// Settle that held credit before handing admission back to main.
		if logged {
			c.Logged(result)
		}
	}()
	var reloadEvent Reload
	var reloadReady, reloadApplied bool
	var fired bool
	var finishing bool
	shutdown := c.shutdown
	listening := c.listening
	var operation *views.Request
	var operationReady bool
	for {
		if c.updateReady() && c.life.start != nil {
			c.logStarted(clock.Now())
		}
		acts = c.flushLogs(acts)
		c.carryOut(acts)
		if reloadApplied {
			c.reloaded <- struct{}{}
			reloadApplied = false
		}
		clear(acts)
		acts = acts[:0]
		if c.phase == starting && c.storeIdle() {
			// Once overdue startup work has committed, read the published
			// gauges before opening listeners. A loaded startup timestamp
			// enables the metric timer and this initial observation together.
			if c.metricAt != 0 && !c.startupMetrics {
				c.startupMetrics = true
				acts = c.refreshMetrics(acts, clock.Now())
				continue
			}
			c.phase = running
			close(c.initialized)
		}
		if c.phase == draining && c.storeIdle() {
			c.phase = stopped
			close(c.drained)
		}
		c.updateReady()
		select {
		case <-shutdown:
			shutdown = nil
			acts = c.beginStop(acts, clock.Now())
			continue
		default:
		}
		if next := c.Timer(); !next.Equal(at) {
			at, ticks = next, nil
			fired = false // a previously ready timer no longer applies
			if reset != nil {
				reset(at)
			} else if !at.IsZero() {
				timer, reset = clock.Timer(at)
			}
			if !at.IsZero() {
				ticks = timer
			}
		}
		select {
		case <-stop:
			if c.phase < draining {
				return
			}
			// Main has joined operations too. Their reports may still be
			// queued or held in event; admit those before returning the log
			// credits to main. Nothing can submit another report now.
			stop = nil
			finishing = true
		default:
		}
		if !reloadReady {
			select {
			case reloadEvent = <-c.reloads:
				reloadReady = true
			default:
			}
		}
		if reloadReady {
			acts = c.reload(acts, reloadEvent, clock.Now())
			reloadApplied = reloadEvent.Notice == nil
			reloadEvent, reloadReady = Reload{}, false
			continue
		}
		if answer.Op == nil {
			select {
			case answer = <-answers:
			default:
			}
		}
		if answer.Op != nil {
			acts = c.Answer(acts, answer, clock.Now())
			answer = store.Answer{}
			continue
		}
		if !logged {
			select {
			case result = <-results:
				logged = true
			default:
			}
		}
		if logged {
			c.Logged(result)
			logged = false
			continue
		}
		if !fired {
			select {
			case <-ticks:
				fired = true
			default:
			}
		}
		if fired {
			ticks, fired = nil, false
			acts = c.Fire(acts, clock.Now())
			continue
		}
		select {
		case <-listening:
			listening = nil
			c.listenersStarted = true
			acts = c.startBridges(acts)
			c.updateReady()
			continue
		default:
		}
		var readers <-chan connection.Event
		if c.phase >= draining || c.Intake() {
			readers = inbound
			if event.Kind == 0 && (finishing || (c.phase == stopping && len(c.connections) == 0 && c.dialing == 0)) {
				select {
				case event = <-readers:
				default:
				}
			}
			if event.Kind != 0 {
				// Process the one dequeued reader event before taking another.
				// Any body it carries retains its reservation through handover.
				acts = c.Handle(acts, event, clock.Now())
				event = connection.Event{}
				continue
			}
			// The final drain closed store-reading view admission and
			// settled admitted work before Drained. Finish taking operations
			// reports before returning store ownership to main.
			if finishing && c.storeIdle() {
				return
			}
			// Main has awaited the listeners before Stop. With their queued
			// accepts taken and every writer's close reported, MQTT intake
			// is complete. Operations may still send failure reports while
			// draining; store answers retain their priority.
			if c.phase == stopping && len(c.connections) == 0 && c.dialing == 0 {
				c.phase = draining
				continue
			}
		}
		if operationReady {
			acts = c.operation(acts, operation, clock.Now())
			operation, operationReady = nil, false
			continue
		}
		select {
		case <-stop:
		case <-shutdown:
			// The ordered check above begins the stop before another event.
		case <-listening:
			listening = nil
			c.listenersStarted = true
			acts = c.startBridges(acts)
		case operation = <-c.health:
			operationReady = true
		case reloadEvent = <-c.reloads:
			reloadReady = true
		case answer = <-answers:
		case result = <-results:
			logged = true
		case <-ticks:
			fired = true
		case event = <-readers:
		}
	}
}

// storeIdle includes core-owned work that has not yet spent store credit.
func (c *Core) storeIdle() bool {
	return c.readPermit == nil && c.Credit() == store.Credit && c.readyAt == len(c.ready) && len(c.parked) == 0 && c.collecting == nil && len(c.sealed) == 0
}

func (c *Core) carryOut(acts []Action) {
	for _, a := range acts {
		switch {
		case a.Write != nil:
			c.writes <- a.Write
		case a.Read != nil:
			c.reads <- a.Read
		case a.Line != (logger.Line{}):
			c.lines <- a.Line
		case a.Cut:
			a.Conn.CutFor(a.Cause)
		case a.Bytes != 0:
			a.Conn.MessageBytes() <- a.Bytes
		default:
			switch reply := a.Reply.(type) {
			case *bridgeState:
				c.startDialer(reply)
			case connection.ReceiveQuota:
				a.Conn.RestoreIncoming(reply.Identifiers)
			case *connection.Delivery:
				a.Conn.Deliveries() <- reply
			default:
				a.Conn.Replies() <- a.Reply
			}
		}
	}
}
