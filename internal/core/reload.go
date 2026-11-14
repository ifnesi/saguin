package core

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/logger"
)

// Reload is main's completed file read, never a request for core I/O. A nil
// Err publishes that part; a refusal carries the unchanged loader diagnostic.
type Reload struct {
	Notice  *logger.Line // a main signal report; no reload step or acknowledgement
	Part    string
	Origins *config.LogAndOrigins
	Access  *config.AccessCopy
	TLS     *config.TLSCopy
	Err     error
}

// Reloads and Reloaded form a bounded handover with main. Only one step is
// outstanding, and the acknowledgement has room even if main starts stopping.
func (c *Core) Reloads() chan<- Reload { return c.reloads }

// Reloaded acknowledges that the preceding result is in use.
func (c *Core) Reloaded() <-chan struct{} { return c.reloaded }

func (c *Core) reload(acts []Action, event Reload, now time.Time) []Action {
	if event.Notice != nil {
		c.queueLog(event.Notice.Level, event.Notice.Msg, event.Notice.At)
		return c.flushLogs(acts)
	}
	if c.phase >= quiescing {
		return acts
	}
	c.wakeTicket++
	c.eventAt = now
	// This event is the single publication boundary for all readers. Main
	// only constructs copies; no shared policy changes before this point.
	if c.reloadCopies != nil {
		switch event.Part {
		case "log_and_origins":
			if event.Err == nil {
				c.reloadCopies.SwapLogAndOrigins(event.Origins)
			}
		case "access":
			if event.Err == nil {
				c.reloadCopies.SwapAccess(event.Access)
			}
		case "tls":
			if event.TLS != nil {
				c.reloadCopies.SwapTLS(event.TLS)
			}
		}
	}
	if event.Err == nil {
		switch event.Part {
		case "log_and_origins":
			c.logCredits.SetLevel(event.Origins.Level)
		case "access":
			c.access = event.Access.ACL
			for s := c.firstConn; s != nil; s = s.next {
				if s.bridge != nil || (s.phase != connected && s.phase != connecting) {
					continue
				}
				c.compileAccess(s, s.accessUser, s.clientID)
				for name := range s.subscriptions {
					sub := s.subscriptions[name]
					c.refreshSubscriptionAccess(s, &sub)
					s.subscriptions[name] = sub
				}
				if len(s.subscriptions) != 0 {
					c.syncConsumer(s, now)
					c.syncShared(s, now)
				}
				c.recheckPreparations(s, now)
			}
		}
	}
	result, level, message := "applied", logger.Info, fmt.Sprintf("reload %s applied", event.Part)
	if event.Err != nil {
		result, level, message = "refused", logger.Error, fmt.Sprintf("reload %s refused: %v", event.Part, event.Err)
	}
	c.reloadCounts[event.Part][result].Inc()
	if c.logCredits.Admit(level) {
		acts = append(acts, Action{Line: logger.Line{At: now, Level: level, Msg: message}})
	}
	if event.Part == "tls" && event.TLS != nil {
		c.logCertificates(event.TLS.Certificates(), now)
	}
	return c.dispatch(acts, now)
}

// recheckPreparations visits the bounded window only when access changes.
// Existing cancellation owns rewinds, stored deletions and returned capacity,
// including when a save or payload read still owns the preparation.
func (c *Core) recheckPreparations(s *connectionState, now time.Time) {
	for i := range s.window {
		d := &s.window[i]
		if d.consumer == nil || d.consumer.service || d.attempted || d.settled || d.accessRevoked {
			continue
		}
		owed, held := d.owedAfter(nil)
		if owed {
			continue
		}
		u := d.consumer
		// A hold also cancels later unsent selections of this owner: they
		// cannot pass the held record, or be selected twice after its rewind.
		for pending := d; pending != nil; {
			next := pending.next
			if !pending.attempted && !pending.settled {
				pending.accessRevoked, pending.permissionHeld = true, held
				c.releaseDelivery(s, pending, now)
			}
			if !held {
				break
			}
			pending = next
		}
		c.advanceConsumer(u, now)
	}
}

func (c *Core) compileAccess(s *connectionState, user, id string) {
	s.access, s.accessUser = nil, user
	if c.access != nil {
		s.access = c.access.Compile(c.access.Select(user), user, id)
	}
}
