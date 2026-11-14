package core

import "github.com/ifnesi/saguin/internal/operations/views"

// Listening publishes main's completion of listener and bridge startup.
// Main calls it once, after all startup steps succeed.
func (c *Core) Listening(info ...StartInfo) {
	if len(info) != 0 {
		c.life.start = &info[0]
	}
	close(c.listening)
}

// Health is the one bounded Health / actions handover. Each request carries
// its own one-slot answer channel. A nil entry announces caller abandonment;
// it can coalesce with an already queued request, which also checks departures.
func (c *Core) Health() chan<- *views.Request { return c.health }

// updateReady uses precisely the flags from which the core answers health.
func (c *Core) updateReady() bool {
	ready := c.listenersStarted && c.bridgeStarts == len(c.bridges) && c.phase == running && !c.snapshot.unhealthy
	if ready != c.wasReady {
		value := int64(0)
		if ready {
			value = 1
		}
		c.readyGauge.Set(value)
		c.wasReady = ready
	}
	return ready
}
