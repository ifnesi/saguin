package core

import (
	"fmt"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// restoreDelivery installs only metadata and reserves the original packet
// identifier. Both durable recovery and a transition to an in-memory session
// use these entries; offered entries take the ordinary reservation/read path.
func (c *Core) restoreDelivery(s *connectionState, row *store.RecoveredDelivery) error {
	var u *consumerState
	if s.bridge != nil {
		u = s.bridgeConsumers[row.Rule][row.Channel]
		if u == nil {
			// Removed rules retain only the exchanges required to finish MQTT.
			// Compile their stored template through the configuration's parser.
			rule, err := config.RestoreBridgeRule(row.RuleFilter, row.RuleTopic)
			if err != nil {
				return fmt.Errorf("store: bridge rule %d: %w", row.Rule, err)
			}
			r := &bridgeRuleState{removed: true, config: rule,
				row: &store.BridgeRule{ID: row.Rule, Bridge: s.bridge.id}}
			r.captures.Plus = make([]topic.Capture, rule.CaptureCount())
			u = c.newConsumer(s, c.channel(row.Channel), 1)
			u.rule = r
			if s.bridgeConsumers[row.Rule] == nil {
				s.bridgeConsumers[row.Rule] = make(map[int64]*consumerState)
			}
			s.bridgeConsumers[row.Rule][row.Channel] = u
		}
	} else {
		u = c.recoveryOwner(s, max(1, row.Channel), row.Share, row.Attempt != nil)
	}
	if u == nil {
		return fmt.Errorf("store: bridge exchange names unknown rule %d channel %d", row.Rule, row.Channel)
	}
	m := &row.Record
	p := connection.Delivery{Channel: u.channel.row.ID, Topic: m.Topic, Properties: m.Properties, Additions: row.Properties,
		Worker: row.Attempt != nil, QoS: row.QoS, Retain: row.Retain, Dup: true, PacketID: row.PacketID}
	var skip, peerTopic string
	reset := false
	if u.rule != nil {
		name, _, valid := c.bridgeTopic(u, m.Topic)
		peerTopic, p.Bridge = name, true
		if u.rule.config.Direction != config.Both {
			p.Topic = []byte(name)
		}
		if !valid && !row.Missing {
			skip = "topic_invalid"
		}
		if err := p.SetBridgeProperties(m.ID); err != nil {
			return err
		}
		fresh := !s.bridge.peer.SessionPresent
		qos := row.QoS
		if fresh {
			// Session loss replaced every surviving exchange with a stored
			// PUBLISH preparation. Apply today's grant as for a new selection.
			p.Dup = false
			qos = min(m.QoS, u.rule.config.QoS)
		}
		p.QoS = s.bridge.deliveryQoS(qos, fresh && p.Retain)
		p.Retain = p.Retain && s.bridge.peer.RetainAvailable
		reset = p.QoS != row.QoS && !row.Released
		if reset {
			p.Dup = false
		}
		if p.QoS == 0 {
			p.PacketID = 0
		}
	}
	if m.Expires.Valid && row.Attempt == nil {
		p.Expires = m.Expires.Int64
	}
	var size int
	if !row.Released {
		var encodable bool
		var err error
		size, encodable, err = deliverySize(s, &p, m.PayloadSize)
		if err != nil {
			return err
		}
		size += m.StoredPayloadSize
		if row.Missing || skip != "" || !encodable || int64(size) > int64(c.limits.MaxOutgoingMemory) {
			size = 0
		}
	}
	d := c.takeDelivery(u, &p, row.Offset, int64(size), row.Initial, row.Released)
	d.storedBytes = int64(m.StoredPayloadSize)
	d.skipCause = skip
	if u.rule != nil {
		d.peerTopic = peerTopic
	}
	d.removed = row.Missing
	d.attempted = true // an inherited row may have reached the previous writer
	d.exchange, d.saved = row.SessionDelivery, true
	if reset {
		d.exchange.Released, d.exchange.QoS = false, p.QoS
		// QoS 0 has no replacement exchange. Keep the original stored row
		// until the writer succeeds or a counted skip settles the obligation.
		if p.QoS != 0 {
			d.exchange.Reset = true
			d.attempted, d.saved = false, false
			u.addPendingSave(d)
		}
	}
	if row.Released {
		d.receipt = receiptAccepted
		if u.rule != nil && !row.Missing {
			if s.bridgeTopics == nil {
				s.bridgeTopics = make(map[string]*outgoing, c.deliveryWindow(s))
			}
			s.bridgeTopics[d.peerTopic], d.topicHeld = d, true
		}
	}
	d.release.Resumed = row.Released
	d.exchange.Properties = nil // writer-bound properties belong to the packet
	u.trackDelivery(d)
	if row.Released {
		// Control packets never occupy the PUBLISH preparation window.
		d.stage = deliveryLoaded
		d.resumedRelease = s.resumeReleases.PushBack(d)
	} else {
		s.resume = append(s.resume, d)
	}
	if s.identifiers == nil {
		s.identifiers = make(map[uint16]*outgoing, len(s.window))
	}
	s.identifiers[row.PacketID] = d
	delete(s.bridgeIdentifiers, row.PacketID)
	if !row.NoMove || row.Attempt != nil {
		u.rememberRecovered(row.Offset)
	}
	if !u.ownsPosition() {
		// The last UNSUBSCRIBE ended this position. Its exchanges finish
		// without reviving a consumer or depending on a saved position.
		u.scan = max(u.scan, row.Offset+1)
	}
	return nil
}

// rememberRecovered prevents unsaved pre-crash scan progress from selecting
// an exchange's record again as a new application delivery.
func (u *consumerState) rememberRecovered(offset int64) {
	if u.recovered == nil {
		u.recovered = make(map[int64]struct{})
	}
	u.recovered[offset] = struct{}{}
}

// beginResume gives the existing recovery read one owner while recovered
// exchanges themselves retain their own channel's progress and payload path.
func (c *Core) beginResume(s *connectionState) {
	if s.resumeThrough == 0 {
		return
	}
	s.recoveryOwners = make(map[int64]*consumerState)
	for _, u := range s.consumers {
		s.recoveryOwners[u.channel.row.ID] = u
	}
	if len(s.consumers) != 0 {
		s.resuming = s.consumers[0]
	} else {
		s.resuming = c.newConsumer(s, &c.broadcast, 1)
	}
}

func (c *Core) recoveryOwner(s *connectionState, channel, share int64, worker bool) *consumerState {
	if worker {
		for _, m := range s.workers {
			if m.group.queue.channel.row.ID == channel {
				return m.owners[0]
			}
		}
		return c.newConsumer(s, c.channel(channel), 1)
	}
	if share != 0 {
		for _, m := range s.shared {
			if m.group.id == share {
				if u := m.owner(channel); u != nil {
					return u
				}
			}
		}
		// A removed membership finishes MQTT without acquiring an ordinary
		// subscription's admission or peer-obligation owner.
		return c.newConsumer(s, c.channel(channel), 1)
	}
	if u := s.recoveryOwners[channel]; u != nil {
		return u
	}
	u := s.resuming
	if u.channel.row.ID != channel {
		u = c.newConsumer(s, c.channel(channel), 1)
	}
	s.recoveryOwners[channel] = u
	return u
}

func (c *Core) offerResumedDeliveries(acts []Action, u *consumerState, now time.Time) []Action {
	s := u.conn
	for len(s.resume) != 0 && s.publishPreparations() < c.deliveryWindow(s) {
		d := s.resume[0]
		s.resume = slices.Delete(s.resume, 0, 1)
		s.resumeAfter = d.exchange.Seq
		c.prepareDelivery(d)
		owner := d.consumer
		receivable := d.exchange.Attempt == nil || s.supportsWorkerOffers()
		if !receivable || d.size == 0 {
			if receivable && !d.removed {
				if d.skipCause != "" {
					acts = c.bridgeUnsent(acts, owner, d.offset, d.skipCause, now)
				} else {
					acts = c.missedDelivery(acts, owner, d.offset, now)
				}
			} else if receivable && owner.channel.row.Type != store.Latest {
				owner.channel.retentionMissed.Inc()
			}
			c.settleDelivery(s, d, now)
		}
		c.queueConsumer(owner)
	}
	c.completeResume(s)
	return acts
}

func (c *Core) completeResume(s *connectionState) {
	if s.resuming != nil && s.resumeAfter >= s.resumeThrough && s.releasedThrough >= s.resumeThrough &&
		len(s.resume) == 0 && s.resumeReleases.Len() == 0 && s.publishPreparations() == 0 {
		s.resuming = nil
		s.recoveryOwners = nil
		c.queueConsumers(s)
	}
}

// canResumeRelease preserves stored order against sendable PUBLISHes. Only
// PUBLISHes held by the peer's quota may be passed; a payload already admitted
// to that quota still goes first, even while its read is outstanding.
func (c *Core) canResumeRelease(s *connectionState) bool {
	first := s.resumeReleases.Front()
	if first == nil {
		return false
	}
	d := first.Value.(*outgoing)
	if s.resumeAfter < d.exchange.Seq && s.inflight < c.deliveryWindow(s) {
		// A look-ahead may have found this control while quota was full.
		// If quota opens before hand-off, first examine the preceding rows
		// still in the store: their PUBLISHes are sendable again.
		return false
	}
	if u := s.prepared.peek(); u != nil && u.offer.exchange.Seq < d.exchange.Seq {
		return s.inflight >= c.deliveryWindow(s) && s.payloadSlots == 0
	}
	// A transferred non-durable window can still have earlier metadata
	// waiting to enter preparation. It obeys the same quota exception.
	return len(s.resume) == 0 || s.resume[0].exchange.Seq > d.exchange.Seq ||
		(s.inflight >= c.deliveryWindow(s) && s.payloadSlots == 0)
}

// resumeTurn prevents a later recovered PUBLISH passing an earlier control
// packet already read, including when their payloads have different owners.
func (s *connectionState) resumeTurn(d *outgoing) bool {
	if s.resuming != nil && d.exchange.Seq > s.releasedThrough {
		return false // a control still in the store may precede this PUBLISH
	}
	first := s.resumeReleases.Front()
	return first == nil || d.exchange.Seq < first.Value.(*outgoing).exchange.Seq
}

func (c *Core) offerResumedReleases(acts []Action, s *connectionState) []Action {
	for c.canResumeRelease(s) {
		first := s.resumeReleases.Front()
		d := s.resumeReleases.Remove(first).(*outgoing)
		d.resumedRelease = nil
		acts = c.readyRelease(acts, d)
	}
	c.completeResume(s)
	return acts
}

// resumeRead uses independent phase cursors and admission allowances. Either
// phase can continue when the other's capacity is spent. Mixed pages fit both
// pools; the store's control watermark preserves sequence order at hand-off.
func (c *Core) resumeRead(u *consumerState) store.ReadSessionDeliveries {
	s := u.conn
	publishes, releases := c.preparationRoom(u), s.freeReleases
	if s.resumeAfter >= s.resumeThrough {
		publishes = 0
	}
	if s.releasedThrough >= s.resumeThrough {
		releases = 0
	}
	r := store.ReadSessionDeliveries{Session: s.session.ID, After: s.resumeAfter,
		Through: s.resumeThrough, ReleasedThrough: s.releasedThrough,
		Limit: min(publishes, releases, store.MetadataPage)}
	if s.bridge != nil {
		r.Bridge = s.bridge.id
	}
	switch {
	case publishes <= 0:
		r.ReleasesOnly = true
		r.Limit = max(0, min(releases, store.MetadataPage))
	case releases <= 0:
		r.PublishesOnly = true
		r.Limit = min(publishes, store.MetadataPage)
	}
	return r
}

// recoveryCapacity reserves the common descriptor allowances before a read.
// A mixed page may contain either phase, so it reserves its bound in both.
// The answer returns the reservation and immediately transfers its used part
// through takeDelivery, without another core event admitting intervening work.
func (s *connectionState) recoveryCapacity(r *store.ReadSessionDeliveries, direction int) {
	if !r.ReleasesOnly {
		s.freeSlots += direction * r.Limit
	}
	if !r.PublishesOnly {
		s.freeReleases += direction * r.Limit
	}
}

// readResumedDeliveries consumes one bounded metadata page in its answer
// event. Unsent exchanges use preparation capacity, not send quota; subsequent
// pages stay in the store until preparation has room. Payloads take exactly the
// same reservation and read path as a first transmission.
func (c *Core) readResumedDeliveries(acts []Action, r *storeRequest, now time.Time) []Action {
	c.returnMetadataCredit()
	u, s := r.consumer, r.conn
	s.recoveryCapacity(&u.recovery, 1)
	c.finishConsumerRequest(u)
	rows := u.recovery.Rows
	u.recovery.Rows = nil
	if c.failure != nil || c.phase >= quiescing || s.phase != connected || s.end.seen || c.connections[s.conn] != s {
		return acts
	}
	for i := range rows {
		if s.bridge != nil && rows[i].Retain && !s.bridge.peer.RetainAvailable {
			acts = c.warnBridgeRetain(acts, s.bridge, now)
		}
		if err := c.restoreDelivery(s, &rows[i]); err != nil {
			return c.fail(acts, err, now)
		}
	}
	s.findFreeIdentifier()
	acts = c.offerResumedDeliveries(acts, u, now)
	after := u.recovery.Through
	if len(rows) == u.recovery.Limit {
		after = rows[len(rows)-1].Seq
	}
	if !u.recovery.PublishesOnly {
		s.releasedThrough = max(s.releasedThrough, after)
	}
	s.releasedThrough = max(s.releasedThrough, u.recovery.ReleasedThrough)
	if !u.recovery.ReleasesOnly {
		s.resumeAfter = max(s.resumeAfter, after)
	}
	c.completeResume(s)
	c.queueConsumer(u)
	c.queueConsumers(s)
	return acts
}
