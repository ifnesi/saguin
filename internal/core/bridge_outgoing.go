package core

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// loadedBridgeOutbound attaches each durable rule/channel cursor to the common
// delivery scheduler. A link's fixed read follows its predecessor's departure.
func (c *Core) loadedBridgeOutbound(acts []Action, r *storeRequest, now time.Time) []Action {
	s, b := r.conn, r.conn.bridge
	if s.end.seen || s.phase != connected || b.link != s {
		return acts
	}
	op := r.op.(*store.LoadBridgeOutbound)
	s.bridgeConsumers = make(map[int64]map[int64]*consumerState, len(b.rules))
	s.resumeThrough, s.deliverySequence, s.recoveryCount = op.Sequence, op.Sequence, op.Count
	rules := make(map[int64]*bridgeRuleState, len(b.rules))
	for i := range b.rules {
		if row := b.rules[i].row; row != nil {
			rules[row.ID] = &b.rules[i]
		}
	}
	for _, position := range op.Cursors {
		rule := rules[position.Rule]
		if rule == nil {
			continue
		}
		ch := c.channel(position.Channel)
		if ch == nil {
			continue
		}
		u := c.newConsumer(s, ch, max(position.Offset, ch.row.Floor))
		u.rule, u.persisted = rule, position.Offset
		if s.bridgeConsumers[position.Rule] == nil {
			s.bridgeConsumers[position.Rule] = make(map[int64]*consumerState)
		}
		s.bridgeConsumers[position.Rule][position.Channel] = u
		if position.Seen.Valid {
			u.seen = position.Seen.Int64
		}
		if u.position != position.Offset {
			u.dirty, u.seen = true, now.UnixMilli()
		}
		starts := []store.SubscriptionStart{{Channel: ch.row.ID, Start: position.Offset}}
		for _, filter := range rule.config.Filters {
			u.filters = append(u.filters, store.Subscription{Filter: filter, QoS: rule.config.QoS, ReadAccess: acl.Full, Starts: starts})
		}
		s.addConsumer(u)
		c.indexConsumer(u, true)
	}
	c.beginResume(s)
	c.initWindow(s)
	s.bridgeIdentifiers = op.Identifiers
	s.findFreeIdentifier()
	for i := range b.rules {
		if b.rules[i].out != nil {
			b.rules[i].out.Set(1)
		}
	}
	c.queueConsumers(s)
	return c.subscribeBridge(acts, b, now)
}

// bridgeTopic substitutes the compiled template once per selected record.
func (c *Core) bridgeTopic(u *consumerState, name []byte) (result string, matched, valid bool) {
	result, matched = u.rule.config.TopicName(string(name), &u.rule.captures)
	return result, matched, matched && topic.CheckName(result, topic.Limits{MaxLength: c.limits.MaxTopicLength, MaxLevels: c.limits.MaxTopicLevels}) == nil
}

// deliveryQoS applies peer capabilities. A new retained message suppressed
// to RETAIN 0 uses QoS 0, leaving no exchange whose resend could change it.
// An existing exchange keeps its QoS unless the peer's Maximum QoS is lower.
func (b *bridgeState) deliveryQoS(qos byte, retainedNew bool) byte {
	if retainedNew && !b.peer.RetainAvailable {
		return 0
	}
	return min(qos, b.peer.MaximumQoS)
}

func (c *Core) bridgeUnsent(acts []Action, u *consumerState, offset int64, cause string, now time.Time) []Action {
	b := u.conn.bridge
	b.unsent[cause].Inc()
	return c.bridgeLog(acts, b, logger.Warn, fmt.Sprintf("%s: %s offset %d", cause, u.channel.row.Name, offset), now)
}

// bridgeOutcome counts only the terminal acknowledgement of a valid outgoing
// exchange. Successful PUBREC keeps both the exchange and its topic occupied.
func (c *Core) bridgeOutcome(acts []Action, s *connectionState, typ packet.Type, ack *packet.Ack, now time.Time) []Action {
	d := s.identifiers[ack.PacketID]
	if s.phase != connected || s.end.seen || d == nil || d.acknowledged {
		return acts
	}
	publish := d.stage == deliveryWriting || d.stage == deliveryAcknowledgement
	terminal := (typ == packet.PUBACK && d.exchange.QoS == 1 && publish) ||
		(typ == packet.PUBREC && d.exchange.QoS == 2 && d.receipt == receiptNone && ack.ReasonCode >= 0x80 && publish) ||
		(typ == packet.PUBCOMP && d.exchange.QoS == 2 && d.exchange.Released && (d.stage == deliveryReleasing || d.stage == deliveryCompletion))
	if !terminal {
		return acts
	}
	if typ != packet.PUBCOMP && ack.ReasonCode >= 0x80 {
		return c.bridgeUnsent(acts, d.consumer, d.offset, "refused", now)
	}
	s.bridge.sent.Inc()
	return acts
}

// topicReady shares one destination-topic obligation across every rule on
// the link. Its lifetime includes payload loading and the writer hand-off.
func (d *outgoing) topicReady() bool {
	if d.consumer.rule == nil {
		return true
	}
	held := d.consumer.conn.bridgeTopics[d.peerTopic]
	return held == nil || held == d
}

func (d *outgoing) releaseTopic() {
	if d.topicHeld {
		delete(d.consumer.conn.bridgeTopics, d.peerTopic)
		d.topicHeld = false
	}
}

// warnBridgeRetain admits only the first suppressed retain flag on a link.
func (c *Core) warnBridgeRetain(acts []Action, b *bridgeState, now time.Time) []Action {
	if b.retainWarned {
		return acts
	}
	b.retainWarned = true
	return c.bridgeLog(acts, b, logger.Warn, "Retain Available 0: sending RETAIN 0", now)
}
