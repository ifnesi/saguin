package core

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
)

// StartInfo is main's completed startup description, published by Listening.
// The core adds its recovery counts and the time readiness was established.
type StartInfo struct {
	Message string
	Began   time.Time
}

type lifeLog struct {
	start     *StartInfo
	restored  uint64
	ended     uint64
	closed    int
	stopCause string // published by StopFor, read only after shutdown is observed
}

// queueLog admits a state change where the owner changes it. The admitted
// actions hold ordinary log credits and leave with this event's other actions.
// This also lets byte returns record recovery without threading actions
// through every reservation owner. No text waits for a future credit.
func (c *Core) queueLog(level logger.Level, message string, now time.Time) {
	if c.logCredits.Admit(level) {
		c.pendingLogs = append(c.pendingLogs, Action{Line: logger.Line{At: now, Level: level, Msg: message}})
	}
}

func (c *Core) flushLogs(acts []Action) []Action {
	acts = append(acts, c.pendingLogs...)
	clear(c.pendingLogs)
	c.pendingLogs = c.pendingLogs[:0]
	return acts
}

func (c *Core) logStarted(now time.Time) {
	info := c.life.start
	if c.logCredits.Admit(logger.Info) {
		c.pendingLogs = append(c.pendingLogs, Action{Line: logger.Line{At: now, Level: logger.Info,
			Msg: fmt.Sprintf("%s sessions_restored=%d sessions_ended=%d startup_took=%s",
				info.Message, c.life.restored, c.life.ended, now.Sub(info.Began))}})
	}
	c.life.start = nil
	if c.reloadCopies != nil {
		c.logCertificates(c.reloadCopies.TLSCertificates(), now)
	}
}

func (c *Core) logCertificates(certificates []config.CertificateExpiry, now time.Time) {
	for _, cert := range certificates {
		if cert.Expires.After(now.Add(30 * 24 * time.Hour)) {
			continue
		}
		level, state := logger.Warn, "expires soon"
		if !cert.Expires.After(now) {
			level, state = logger.Error, "expired"
		}
		if c.logCredits.Admit(level) {
			c.pendingLogs = append(c.pendingLogs, Action{Line: logger.Line{At: now, Level: level,
				Msg: fmt.Sprintf("%s TLS certificate %q %s: expiry=%s", cert.Owner, cert.File, state, cert.Expires.UTC().Format(time.RFC3339))}})
		}
	}
}

func (c *Core) logDiskRoom(full bool, now time.Time) {
	if c.diskFull == full {
		return
	}
	c.diskFull = full
	level, message := logger.Info, "store has room again"
	if full {
		level, message = logger.Warn, "store full: write refused for lack of room"
	}
	c.queueLog(level, message, now)
}

// logReadEnd combines a credential refusal with any certificate-name failure
// on the existing end report. They describe the same refusal and get one line.
func (c *Core) logReadEnd(acts []Action, ev connection.Event, now time.Time) []Action {
	report, reported := ev.Packet.(connection.ReadFailure)
	if ev.Conn != nil {
		s := c.connections[ev.Conn]
		if s.closure.cause == connection.MQTTClosure(packet.CONNACK, 0x86) {
			if c.logCredits.Admit(logger.Warn) {
				user := ev.Conn.User()
				if user == "" {
					user = ev.Conn.ConnectUser()
				}
				message := fmt.Sprintf("MQTT listener %s: credential refused (0x86): client %q user %q remote %v",
					s.accept.Listener, s.clientID, user, s.accept.Remote)
				if reported {
					message += ": " + report.Err.Error()
				}
				acts = append(acts, Action{Line: logger.Line{At: s.closure.at, Level: logger.Warn, Msg: message}})
			}
			return acts
		}
	}
	if reported {
		acts = c.logFailure(acts, logger.Warn, report.Err, now)
	}
	return acts
}

// StopResult returns the completed stop's observations. Main calls it only
// after Run has returned ownership, before it stops the store and logger.
func (c *Core) StopResult() (closed int, snapshotTaken bool) {
	return c.life.closed, c.snapshot.finalTaken
}
