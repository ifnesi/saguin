package core

import (
	"time"

	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
)

// subscribeBridge fills the next packet in configuration order. Only one
// SUBSCRIBE is unanswered at a time, independently of the publication window.
// Both kinds reserve identifiers in the connection's one client-side space.
func (c *Core) subscribeBridge(acts []Action, b *bridgeState, now time.Time) []Action {
	if len(b.todo) == 0 {
		if len(b.retry) != 0 {
			b.refusals = min(b.refusals+1, 5)
			c.bridgeRetries.set(&b.retryDeadline, now.Add(bridgeWait(b.refusals)))
		} else {
			b.refusals = 0
			if b.refusalActive && b.granted == len(b.filters) {
				b.refusalActive = false
				acts = c.bridgeLog(acts, b, logger.Error, "every subscription granted again", now)
			}
		}
		return acts
	}
	list := b.subscriptions[:0]
	size := 0
	for _, index := range b.todo {
		payloadSize := size + 3 + len(b.filters[index].filter)
		encoded, err := packet.SubscribeSize(packet.V5, nil, payloadSize)
		if err != nil || (b.peer.MaximumPacketSize != 0 && uint32(encoded) > b.peer.MaximumPacketSize) {
			break // each filter was already proved to fit alone
		}
		size = payloadSize
		f := &b.filters[index]
		list = append(list, packet.Subscription{Filter: f.filter, QoS: min(b.rules[f.rule].config.QoS, b.peer.MaximumQoS),
			NoLocal: true, RetainAsPublished: true, RetainHandling: 1})
	}
	n := len(list)
	subscriptions, err := packet.NewSubscriptions(packet.V5, list)
	if err != nil {
		return c.finish(acts, b.link, 0x80, now)
	}
	s := b.link
	if s.identifiers == nil {
		s.identifiers = make(map[uint16]*outgoing)
	}
	s.findFreeIdentifier()
	if s.freeIdentifier > 65535 {
		return acts // a committed settlement wakes this pending subscription
	}
	id := uint16(s.freeIdentifier)
	s.identifiers[id] = nil
	s.findFreeIdentifier()
	b.pending = &packet.Subscribe{PacketID: id, Subscriptions: subscriptions}
	b.pendingID = id
	b.asked, b.todo = b.todo[:n], b.todo[n:]
	return append(acts, Action{Conn: s.conn, Reply: b.pending})
}

func (c *Core) bridgeSuback(acts []Action, b *bridgeState, ack *packet.Suback, now time.Time) []Action {
	if b.pending == nil || ack.PacketID != b.pendingID || len(ack.ReasonCodes) != len(b.asked) {
		return c.finish(acts, b.link, 0x82, now)
	}
	for i, code := range ack.ReasonCodes {
		index := b.asked[i]
		f := &b.filters[index]
		r := &b.rules[f.rule]
		if code < 0x80 && code > min(r.config.QoS, b.peer.MaximumQoS) {
			return c.finish(acts, b.link, 0x82, now)
		}
		if code >= 0x80 {
			b.retry = append(b.retry, index)
			level := logger.Error
			if f.refused {
				level = logger.Debug
			}
			f.refused, b.refusalActive = true, true
			acts = c.bridgeLog(acts, b, level, "subscription refused: "+string(f.filter)+": "+packet.ReasonName(packet.SUBACK, code), now)
		} else if !f.granted {
			f.granted = true
			f.refused = false
			b.granted++
			r.granted++
			if r.granted == r.total {
				r.in.Set(1)
			}
		}
	}
	c.bridgeTimeouts.set(&b.subscriptionDeadline, time.Time{})
	b.lastSuback = now
	delete(b.link.identifiers, b.pendingID)
	b.link.freeIdentifier = min(b.link.freeIdentifier, uint32(b.pendingID))
	b.pending, b.asked = nil, nil
	return c.subscribeBridge(acts, b, now)
}

func (c *Core) bridgeTimer() time.Time {
	return firstTime(c.bridgeAcks.first(), firstTime(c.bridgeTimeouts.first(), c.bridgeRetries.first()))
}

// fireBridges shares the core's single timer. Subscription expiry runs before
// redials at an equal deadline, and each index breaks ties by bridge name.
func (c *Core) fireBridges(acts []Action, now time.Time) []Action {
	for c.bridgeAcks.Len() != 0 && !now.Before(c.bridgeAcks.first()) {
		b := c.bridgeAcks.items[0].bridge
		c.bridgeAcks.set(&b.ackDeadline, time.Time{})
		b.releasingAcks = true
		c.queueReplies(b.link)
	}
	for c.bridgeTimeouts.Len() != 0 && !now.Before(c.bridgeTimeouts.first()) {
		b := c.bridgeTimeouts.items[0].bridge
		b.unanswered++
		acts = c.bridgeDown(acts, b, "SUBSCRIBE unanswered for 30s", now)
		acts = c.finish(c.cut(acts, b.link, 0, now), b.link, 0, now)
	}
	for c.bridgeRetries.Len() != 0 && !now.Before(c.bridgeRetries.first()) {
		b := c.bridgeRetries.items[0].bridge
		c.bridgeRetries.set(&b.retryDeadline, time.Time{})
		if b.link == nil {
			acts = c.dialBridge(acts, b)
		} else if b.up && b.link.phase == connected {
			b.todo, b.retry = b.retry, nil
			acts = c.subscribeBridge(acts, b, now)
		}
	}
	return acts
}
