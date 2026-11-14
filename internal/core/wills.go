package core

import (
	"errors"
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

type willCause uint8

const (
	willLost willCause = iota
	willDisconnect
	willTakeover
	willSessionEnd
	willRestart
	willCauses
)

// recoveredCause gives a recovered Will priority over the current event.
// A full-store retry has discarded the original cause, as metrics.md states.
func (c *Core) recoveredCause(w store.WillInfo, cause willCause, retry bool) willCause {
	if retry {
		return willLost
	}
	if c.started != 0 && ((w.Left != 0 && w.Left <= c.started) ||
		(w.Left == 0 && w.Due.Valid && w.Due.Int64 <= c.started)) {
		return willRestart
	}
	return cause
}

// prepareWill keeps the original completion and write while the existing
// message-memory FIFO reserves the stored body's bytes. CONNECT may also
// carry a preceding session ending; both remain in the publication transaction.
func (c *Core) prepareWill(r *storeRequest) {
	write := r.op.(store.Write)
	r.willChanges = 1
	if r.before != nil {
		r.willChanges++
		write = &Group{Writes: []store.Write{r.before, write}}
	}
	r.willKind, r.kind = r.kind, fireWill
	r.before, r.op = write, &store.LoadWill{Session: r.willInfo.Session, Expected: r.willInfo}
	c.classifyRequest(r)
	r.memory.will = r
	c.queueMemory(&r.memory, r.willInfo.Bytes)
}

// deferWill leaves an unfetched Will in the store when stopping forbids new
// message reservations. A departure proceeds; recovery owns the firing.
func (c *Core) deferWill(acts []Action, r *storeRequest, now time.Time) []Action {
	if r.willKind == leaveSession {
		r.kind, r.op, r.before = leaveSession, r.before, nil
		r.willInfo = store.WillInfo{}
		switch op := r.op.(type) {
		case *store.EndSession:
			op.KeepWill = true
		case *store.LeaveSession:
			op.DiscardWill, op.WillDue = false, 0
		}
		c.readyRequest(r)
		return acts
	}
	// No write was accepted for this reservation. A stop abandons the
	// pending CONNECT or housekeeping decision, leaving its stored Will.
	c.cancelPendingRequest(r)
	switch r.willKind {
	case saveConnect:
		if a := r.conn.attempt; a != nil {
			if a.takeover != nil && (a.takeover.stored || a.takeover.willBytes != 0 || a.takeover.hasHeldPublishes()) {
				acts = c.leave(acts, r.lane, a.takeover, now)
			}
			c.abortConnect(r.conn)
		}
	case endExpired:
		c.expiry.ending--
	case endWill:
		c.willExpiry.ending--
	}
	c.endLane(r.lane, r)
	return acts
}

// loadedWill transfers a reserved read straight into the channel's ordinary
// publication batches. Its session lane remains occupied until the batch's
// transaction has published (or dropped) the Will and stored its departure.
func (c *Core) loadedWill(acts []Action, r *storeRequest, now time.Time) []Action {
	if c.failure != nil {
		c.discardWrite(r, now)
		return acts
	}
	op := r.op.(*store.LoadWill)
	if !op.Found {
		// A reconnect can supersede a page after its view was fixed and
		// finish before this lane was reserved. It must not fire a newer
		// Will or delete the resumed session using that page's decision.
		c.bodyCount--
		c.batchCapacityChanged()
		c.returnBytes(r.publishes.bytes, now)
		r.publishes.bytes = 0
		switch r.willKind {
		case endExpired:
			c.expiry.ending--
		case endWill:
			c.willExpiry.ending--
		default:
			r.kind, r.op, r.before = r.willKind, r.before, nil
			r.willInfo = store.WillInfo{}
			c.readyRequest(r)
			return acts
		}
		c.cancelPendingRequest(r)
		c.endLane(r.lane, r)
		return acts
	}
	w := &op.Will
	ch := c.route(w.Topic)
	record := store.Record{Channel: ch.row.ID, Time: now.UnixMilli(), Topic: w.Topic,
		QoS: w.QoS, Publisher: w.Client, Payload: w.Payload, StoredPayload: w.StoredPayload, PayloadSize: max(len(w.Payload), w.PayloadSize), Properties: w.Properties,
		PublishedRetained: w.Retain && ch == &c.broadcast, Retained: ch.row.Type == store.Latest || (w.Retain && ch == &c.broadcast && (len(w.Payload) != 0 || w.PayloadSize != 0))}
	if w.StoredPayload != nil {
		// The read reserved both copies. Storage needs only the existing S2
		// block; all later policy and retention decisions use PayloadSize.
		record.Payload, w.Payload = nil, nil
		r.publishes.bytes -= int64(record.PayloadSize)
		c.returnBytes(int64(record.PayloadSize), now)
	}
	hasID := false
	hasDelay := false
	props, err := packet.ReadStoredProperties(w.Properties, func(p packet.PropertyView) error {
		switch p.ID {
		case packet.MessageExpiryInterval:
			record.Expires.Int64, record.Expires.Valid = expiryAt(record.Time, p.Int), true
		case packet.WillDelayInterval:
			hasDelay = true
		case packet.UserProperty:
			if !hasID && string(p.Name) == "saguin-id" {
				record.ID, hasID = string(p.Value), true
			}
		}
		return nil
	})
	if err != nil {
		acts = c.fail(acts, fmt.Errorf("read Will properties: %w", err), now)
		c.discardWrite(r, now)
		return acts
	}
	if !hasID {
		record.ID = newMessageID(now)
	}
	if hasDelay {
		record.Properties, err = props.Without(packet.WillDelayInterval).Stored(nil)
		if err != nil {
			acts = c.fail(acts, fmt.Errorf("encode Will properties: %w", err), now)
			c.discardWrite(r, now)
			return acts
		}
	}
	p := &publication{record: record, channel: ch, will: r, bytes: r.publishes.bytes, batchCredit: true}
	if c.access != nil {
		access := c.access.Compile(c.access.Select(w.User), w.User, w.Client)
		channel := ""
		if ch != &c.broadcast {
			channel = ch.row.Name
		}
		if access.Denied(acl.Will) || !access.Allows(acl.Write, channel, w.Topic) ||
			(w.Retain && access.Denied(acl.Retained)) || (w.QoS == 2 && access.Denied(acl.QoS2)) {
			p.dropped = c.willsNotAuthorized
		}
	}
	r.publishes.bytes = 0 // the publication now owns the memory and batch credits
	r.kind, r.op, r.before = r.willKind, r.before, nil
	c.queuePublish(p, nil)
	return acts
}

// finishedWill applies only the transaction's result, then frees the session
// lane. A full refusal leaves the stored Will for later rediscovery; no body
// or failed write is retained in the core.
func (c *Core) finishedWill(acts []Action, p *publication, err error, dropped bool, now time.Time) []Action {
	r := p.will
	if err == nil {
		if r.kind != saveConnect {
			c.willCount--
		}
		if r.kind == leaveSession {
			r.conn.willBytes = 0
		}
		if dropped {
			p.dropped.Inc()
		} else {
			c.willsPublished[r.willCause].Inc()
		}
	} else if errors.Is(err, store.ErrFull) {
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
	}
	return c.applyAnswer(acts, r, err, now)
}
