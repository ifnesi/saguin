package core

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// subscriptionBoundary captures a reached channel at packet intake. A body
// or release held behind CONNECT supplies its offset when that predecessor enters.
type subscriptionBoundary struct {
	start store.SubscriptionStart
	after *publication
}

// subscriptionAdmission collects the starts, positions and initial-pass ranges
// admitted together. SUBSCRIBE and CONNECT's newly reached channels use the
// same rules; their caller supplies the boundary captured at packet intake.
type subscriptionAdmission struct {
	op       *store.SaveSubscriptions
	channels map[int64]int
	retained map[retainedKey]retainedFilter
}

func (c *Core) admitSubscription(a *subscriptionAdmission, sub *store.Subscription, start store.SubscriptionStart, fresh bool) {
	if group, _, _ := topic.SplitShared(sub.Filter); group != "" {
		return
	}
	ch := c.channel(start.Channel)
	if (start.Channel == 1 || ch.row.Type == store.Latest) && (sub.RetainHandling == 0 || (sub.RetainHandling == 1 && fresh)) {
		if a.retained == nil {
			a.retained = make(map[retainedKey]retainedFilter)
		}
		a.retained[retainedKey{start.Channel, sub.Filter}] = retainedFilter{channel: start.Channel, Subscription: *sub, from: 1, before: start.Start}
	}
	if !fresh {
		return
	}
	if start.Channel == 1 || ch.row.Type == store.Latest {
		// Existing options may still belong to an outstanding selection.
		sub.Starts = append(slices.Clone(sub.Starts), start)
	}
	if a.channels[start.Channel] == 0 {
		if ch.row.Type != store.Latest && start.Channel != 1 && !ch.startTail {
			start.Start = ch.row.Floor
		}
		a.op.Positions = append(a.op.Positions, start)
	}
	a.channels[start.Channel]++
}

func (c *Core) applySubscriptionPositions(s *connectionState, positions []store.SubscriptionStart) {
	for _, position := range positions {
		if s.consumer(position.Channel) == nil {
			s.addConsumer(c.newConsumer(s, c.channel(position.Channel), position.Start))
		}
	}
}

// subscriptions reserves the packet's place in its session lane. Its reply
// slot bounds pending metadata, including packets pipelined behind CONNECT.
// The live-start boundary is the channel's next offset at intake, so every
// publication that entered before the SUBSCRIBE falls below it; behind a
// CONNECT-held publication, it is taken once that publication has entered.
func (c *Core) subscriptions(conn *connection.Conn, p any) {
	s := c.connections[conn]
	if s.end.seen || s.phase >= refused {
		return
	}
	s.pendingAcks++
	s.pendingSubscriptions++
	s.subscriptionRevision++ // an earlier metadata page can no longer select deliveries
	c.queueConsumers(s)
	r := &storeRequest{kind: changeSubscriptions, lane: c.lane(s.clientID), conn: s, op: p}
	if sub, ok := p.(*connection.Subscribe); ok {
		r.boundaries = make([][]subscriptionBoundary, len(sub.Filters))
		for i, filter := range sub.Filters {
			if _, service := c.routing.SeekChannel(filter.Filter, true); service || strings.HasPrefix(filter.Filter, "$saguin/queue") {
				continue
			}
			_, part, _ := topic.SplitShared(filter.Filter)
			for _, index := range c.routing.Reaches(part) {
				ch := &c.broadcast
				if index >= 0 {
					ch = c.routed[index]
				}
				r.boundaries[i] = append(r.boundaries[i], subscriptionBoundary{
					start: store.SubscriptionStart{Channel: ch.row.ID, Start: ch.row.Next}})
			}
			slices.SortFunc(r.boundaries[i], func(a, b subscriptionBoundary) int {
				return cmp.Compare(a.start.Channel, b.start.Channel)
			})
		}
	}
	r.replySlot = s.storedReplies.PushBack(nil)
	c.enqueue(r)
}

// enterPendingPackets binds CONNECT-held packets in packet order before later
// events can enter their channels. One predecessor per channel supplies the
// existing deferred boundary for each SUBSCRIBE in the same prefix. Once
// connected, publication offsets are known at intake and need no such pass.
func (c *Core) enterPendingPackets(acts []Action, s *connectionState, now time.Time) []Action {
	var held map[int64]*publication
	lane := c.lanes[s.clientID]
	if lane == nil {
		return acts
	}
	visit := func(r *storeRequest) {
		if r.conn != s {
			return
		}
		if r.kind == changeSubscriptions {
			for _, starts := range r.boundaries {
				for i := range starts {
					if p := held[starts[i].start.Channel]; p != nil {
						starts[i].after = p
					}
				}
			}
			return
		}
		p := r.publication
		var reason byte
		switch r.kind {
		case executeRequest:
			q := r.request
			q.publishIdentity, reason = c.classifyPublish(s, q.id, q.qos, q.quota)
			if reason != 0 {
				acts = c.finish(acts, s, reason, now)
				return
			}
			r.channel = r.request.channel
		case releaseIncoming:
			c.enterIncoming(r, s.incoming[p.packetID])
		case releasePublishes:
			// CONNECT no longer holds this complete body. It joins the
			// same ordered capacity admission as the prefix's releases.
			c.releasePublicationCredit(p)
			p.publishIdentity, reason = c.classifyPublish(s, p.packetID, p.record.QoS, p.quota)
			if reason != 0 {
				acts = c.finish(acts, s, reason, now)
				return
			}
			if p.repeat != nil {
				return // a restored repeat keeps its original exchange
			}
			r.channel = p.channel
			if p.refusal == 0 {
				c.queueBatch(&r.publishes, p.channel)
			}
			if p.record.QoS != 2 && p.refusal == 0 && p.record.Offset == 0 {
				p.enter()
			}
		default:
			return
		}
		if r.channel != nil {
			r.channel.pending = r
			c.registerChannel(r)
			c.readyRequest(r)
		}
		if p == nil || s.pendingSubscriptions == 0 || p.channel == nil || (p.record.QoS == 2 && r.kind == releasePublishes) {
			return
		}
		if r.kind == releaseIncoming && p.incoming == nil {
			return // an unknown or repeated PUBREL has no publication entry
		}
		if held == nil {
			held = make(map[int64]*publication)
		}
		held[p.channel.row.ID] = p
	}
	if lane.request != nil {
		visit(lane.request)
	}
	for i := 0; i < len(lane.pending); {
		r := lane.pending[i]
		if r.kind == leaveSession && r.conn != s {
			// This CONNECT has committed the earlier session's disposition.
			// Its queued departure is superseded. An expiry-change departure
			// still needs the final writer entry it held for that decision.
			lane.pending = slices.Delete(lane.pending, i, i+1)
			c.cancelPendingRequest(r)
			c.cancelSharedEnd(r.conn)
			acts = c.finish(acts, r.conn, r.conn.end.reason, now)
			continue
		}
		visit(r)
		i++
	}
	return acts
}

// startSubscriptions decides from the preceding session request's result.
// Only accepted changes need the store; a refusal or absent filter can be
// answered at once. Non-durable sessions use the same decision and result
// path, without writing their subscriptions to disk.
func (c *Core) startSubscriptions(acts []Action, r *storeRequest, now time.Time) []Action {
	var changed bool
	for _, starts := range r.boundaries {
		for i := range starts {
			if p := starts[i].after; p != nil {
				starts[i].start.Start = max(starts[i].start.Start, p.record.Offset+1)
			}
		}
	}
	switch p := r.op.(type) {
	case *connection.Subscribe:
		op, ack, pass := c.subscriptionChanges(r.conn, p, r.boundaries)
		r.op, r.reply, changed = op, ack, len(op.Updates) != 0
		r.pass = pass
		op.Volatile = !r.conn.stored
	case *connection.Unsubscribe:
		op, ack, cancellations := subscriptionRemovals(r.conn, p, now)
		r.op, r.reply, changed = op, ack, len(op.Filters) != 0
		r.cancellations = cancellations
		op.Volatile = !r.conn.stored
		for _, filter := range op.Filters {
			g := c.shares[filter]
			if g == nil {
				continue
			}
			remaining := g.durable
			if r.conn.stored {
				remaining--
			}
			change := &store.SharedChange{ID: g.id, End: remaining == 0 && len(g.members) == 1, Volatile: remaining == 0}
			for _, d := range cancellations {
				if d.consumer.share == g {
					change.Return = append(change.Return, store.SharedRecord{Channel: d.consumer.channel.row.ID, Offset: d.offset})
				}
			}
			op.Shared = append(op.Shared, change)
		}
	}
	r.kind = saveSubscriptions
	sharedWrite := false
	switch op := r.op.(type) {
	case *store.SaveSubscriptions:
		sharedWrite = len(op.Shared) != 0
	case *store.RemoveSubscriptions:
		sharedWrite = len(op.Shared) != 0 || len(op.Cancel) != 0
		if r.conn.spooled {
			for _, channel := range op.Channels {
				if u := r.conn.consumer(channel); u != nil && u.markersBefore != 0 {
					sharedWrite = true
					break
				}
			}
		}
	}
	r.orderedReply = changed && (r.conn.stored || sharedWrite)
	if !changed || (!r.conn.stored && !sharedWrite) {
		c.releaseLane(r)
		return c.savedSubscriptions(acts, r, nil, now)
	}
	// Reserve all existing positions with the session until this atomic
	// subscription change commits. None can begin a scan under old options.
	for _, u := range r.conn.consumers {
		c.joinConsumerRead(r, u)
		c.queueConsumer(u)
	}
	c.continueRequest(r)
	return acts
}

// subscriptionChanges works in packet order. The temporary index holds only
// this packet's accepted filters, so duplicates replace options without
// spending capacity twice or capturing a second live-start boundary.
func (c *Core) subscriptionChanges(s *connectionState, p *connection.Subscribe, boundaries [][]subscriptionBoundary) (op *store.SaveSubscriptions, ack *packet.Suback, pass *retainedPass) {
	op = &store.SaveSubscriptions{Session: s.session.ID}
	ack = &packet.Suback{PacketID: p.PacketID, ReasonCodes: make([]byte, len(p.Filters))}
	var updates map[string]int
	admission := subscriptionAdmission{op: op}
	count := len(s.subscriptions)
	for i, filter := range p.Filters {
		group, _, _ := topic.SplitShared(filter.Filter)
		before, held := s.subscriptions[filter.Filter]
		index, repeated := updates[filter.Filter]
		if p.AccessFile != c.access {
			switch {
			case held && before.ReadAccessFile == c.access:
				filter.ReadAccess = before.ReadAccess
			case repeated:
				filter.ReadAccess = op.Updates[index].ReadAccess
			default:
				filter.ReadAccess = connection.SubscriptionCoverage(s.access, c.routing, filter.Filter)
			}
		}
		_, service := c.routing.SeekChannel(filter.Filter, true)
		_, worker := c.routing.WorkerChannel(filter.Filter)
		queueFilter := filter.Filter == "$saguin/queue" || strings.HasPrefix(filter.Filter, "$saguin/queue/")
		switch {
		case service:
			filter.ReadAccess = acl.Full
		case worker:
			filter.QoS = min(filter.QoS, 1)
		case queueFilter:
			filter.ReadAccess = acl.Full
		}
		if !service && filter.QoS == 2 && s.access.Denied(acl.QoS2) {
			filter.QoS = 1
		}
		var reason byte
		switch {
		case strings.HasPrefix(filter.Filter, "$SYS/") || (strings.HasPrefix(filter.Filter, "$saguin/") && !service && !worker && !queueFilter) || filter.ReadAccess == acl.None:
			reason = 0x87
		case filter.OverLimit || (queueFilter && (!worker || filter.NoLocal)):
			reason = 0x8F
		case s.version == packet.V311 && (strings.HasPrefix(filter.Filter, "$share/") || strings.HasPrefix(filter.Filter, "$saguin/queue/")):
			// Shared members and queue workers require MQTT 5. Count the
			// MQTT 3.1.1-only refusal under unspecified_error (metrics.md).
			reason = 0x80
		case !held && !repeated && count >= c.limits.MaxSubscriptions:
			reason = 0x97
		case (worker && filter.QoS == 0) || p.Unknown || p.InvalidPartitions || !partitionsAllowed(p, filter.Filter, boundaries[i]):
			reason = 0x83
		}
		if reason != 0 {
			ack.ReasonCodes[i] = reason
			c.subscriptionRefused[reason].Inc()
			continue
		}
		ack.ReasonCodes[i] = filter.QoS
		if repeated {
			before = op.Updates[index].Subscription
		}
		sub := store.Subscription{Filter: filter.Filter, QoS: filter.QoS, ReadAccess: filter.ReadAccess, ReadAccessFile: c.access,
			NoLocal: filter.NoLocal, RetainAsPublished: filter.RetainAsPublished,
			RetainHandling: filter.RetainHandling, Identifier: p.Identifier, Starts: before.Starts, Channels: before.Channels,
			Partitions: p.Partitions, Partition: p.Partition}
		// Retained eligibility belongs to this occurrence. A later duplicate
		// sees the subscription just accepted, even though only its final
		// options need storing. It cannot undo an earlier requested pass.
		fresh := !held && !repeated
		if fresh {
			sub.Channels = make([]int64, 0, len(boundaries[i]))
			if admission.channels == nil {
				admission.channels = subscriptionChannels(s.subscriptions)
				for _, u := range s.consumers {
					admission.channels[u.channel.row.ID] = max(1, admission.channels[u.channel.row.ID])
				}
			}
		}
		for _, boundary := range boundaries[i] {
			if fresh {
				sub.Channels = append(sub.Channels, boundary.start.Channel)
			}
			c.admitSubscription(&admission, &sub, boundary.start, fresh)
		}
		if group != "" {
			if change := c.sharedSubscriptionChange(s, &sub, boundaries[i], fresh); change != nil {
				if !slices.ContainsFunc(op.Shared, func(other *store.SharedChange) bool { return other.ID == change.ID }) {
					op.Shared = append(op.Shared, change)
				}
			}
		}
		if repeated {
			op.Updates[index].Subscription = sub
			continue
		}
		if fresh {
			count++
		}
		if updates == nil {
			updates = make(map[string]int)
		}
		updates[filter.Filter] = len(op.Updates)
		op.Updates = append(op.Updates, store.SubscriptionUpdate{Subscription: sub, New: !held})
	}
	return op, ack, newRetainedPass(admission.retained)
}

func partitionsAllowed(p *connection.Subscribe, filter string, boundaries []subscriptionBoundary) bool {
	if p.Partition.Count == 0 {
		return true
	}
	if strings.HasPrefix(filter, "$share/") || strings.HasPrefix(filter, "$saguin/queue/") {
		return false
	}
	for _, boundary := range boundaries {
		if boundary.start.Channel == 1 {
			return false
		}
	}
	return true
}

// refreshSubscriptionAccess reuses coverage until its immutable ACL generation
// changes. Restored subscriptions start with the unrestricted nil generation.
func (c *Core) refreshSubscriptionAccess(s *connectionState, sub *store.Subscription) {
	if sub.ReadAccessFile != c.access {
		sub.ReadAccess = connection.SubscriptionCoverage(s.access, c.routing, sub.Filter)
		sub.ReadAccessFile = c.access
	}
}

// subscriptionChannels counts the held filters reaching each channel. A
// packet derives this only when it changes reach, then adjusts the counts in
// filter order so the core selects first and last subscriptions exactly once.
func subscriptionChannels(subscriptions map[string]store.Subscription) map[int64]int {
	channels := make(map[int64]int)
	for filter := range subscriptions {
		sub := subscriptions[filter]
		if group, _, _ := topic.SplitShared(sub.Filter); group != "" {
			continue
		}
		for _, channel := range sub.Channels {
			channels[channel]++
		}
	}
	return channels
}

func subscriptionRemovals(s *connectionState, p *connection.Unsubscribe, now time.Time) (op *store.RemoveSubscriptions, ack *packet.Unsuback, cancellations []*outgoing) {
	op = &store.RemoveSubscriptions{Session: s.session.ID}
	ack = &packet.Unsuback{PacketID: p.PacketID, Properties: p.Unread, ReasonCodes: make([]byte, len(p.Filters))}
	var removed map[string]bool
	var channels map[int64]int
	for i, filter := range p.Filters {
		sub, held := s.subscriptions[filter]
		if !held || removed[filter] {
			ack.ReasonCodes[i] = 0x11
			continue
		}
		if removed == nil {
			removed = make(map[string]bool)
			channels = subscriptionChannels(s.subscriptions)
		}
		removed[filter] = true
		op.Filters = append(op.Filters, filter)
		if sub.Share != 0 {
			continue
		}
		for _, channel := range sub.Channels {
			channels[channel]--
			if channels[channel] == 0 {
				if u := s.consumer(channel); u != nil && keepsPosition(s, u) {
					op.Kept = append(op.Kept, subscriptionCheckpoint(u, now))
				} else {
					op.Channels = append(op.Channels, channel)
				}
			}
		}
	}
	if len(removed) != 0 {
		for i := range s.window {
			d := &s.window[i]
			if d.consumer == nil || d.consumer.service || d.attempted || d.settled {
				continue
			}
			if owed, _ := d.owedAfter(removed); owed {
				continue
			}
			// The subscription transaction owns this preparation until its
			// answer, including if the connection ends in the meantime.
			d.saving = true
			cancellations = append(cancellations, d)
			if d.saved {
				op.Cancel = append(op.Cancel, d.exchange.PacketID)
			}
		}
	}
	return op, ack, cancellations
}

// owedAfter reviews the original contributing options, without rebuilding
// delivery properties or letting a later subscription acquire an old preparation.
func (d *outgoing) owedAfter(removed map[string]bool) (owed, held bool) {
	match := store.SubscriptionMatch{Topic: d.packet.Topic, Offset: d.offset, Channel: d.consumer.channel.row.ID, Type: d.consumer.channel.row.Type, Local: d.local}
	n := len(d.filters)
	if d.pass != nil {
		n = len(d.pass.filters)
	}
	for i := range n {
		var sub *store.Subscription
		if d.pass != nil {
			f := &d.pass.filters[i]
			if d.offset < f.from || d.offset >= f.before {
				continue
			}
			sub = &f.Subscription
		} else {
			sub = &d.filters[i]
		}
		current, exists := d.consumer.conn.subscriptions[sub.Filter]
		if !exists || removed[sub.Filter] || (d.exchange.Attempt == nil && !match.Eligible(sub, d.initial)) {
			continue
		}
		switch current.ReadAccess {
		case acl.None:
			held = true
		case acl.Full:
			return true, false
		case acl.Partial:
			channel := ""
			if d.consumer.channel.row.ID != 1 {
				channel = d.consumer.channel.row.Name
			}
			if d.consumer.conn.access.Allows(acl.Read, channel, string(d.packet.Topic)) {
				return true, false
			}
		}
	}
	return false, held
}

// savedSubscriptions applies committed changes to the original owner before
// dropping an ended or superseded connection's acknowledgement (R4).
func (c *Core) savedSubscriptions(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	defer c.releaseSharedRequest(r)
	s := r.conn
	c.completeSubscription(s)
	for _, u := range c.finishConsumerRead(r) {
		c.queueConsumer(u)
	}
	if op, ok := r.op.(*store.RemoveSubscriptions); ok && err == nil {
		// The transaction's checkpoint wins over acknowledgements taken
		// while it was outstanding. Reset before cancelling preparations.
		for _, position := range op.Kept {
			if u := s.consumer(position.Channel); u != nil {
				u.position, u.persisted, u.seen = position.Offset, position.Offset, position.Seen.Int64
				c.endSubscriptionReach(s, u, now)
			}
		}
	}
	c.finishCancellations(r, err, now)
	c.completeAcknowledgement(s)
	if err == nil {
		switch op := r.op.(type) {
		case *store.SaveSubscriptions:
			if len(op.Updates) != 0 && s.subscriptions == nil {
				s.subscriptions = make(map[string]store.Subscription, len(op.Updates))
			}
			for i := range op.Updates {
				update := &op.Updates[i]
				if update.Share != 0 {
					g := c.sharesByID[update.Share]
					g.stored = true
					if update.New && s.stored {
						g.durable++
					}
				}
				sub := update.Subscription
				// A reload may have refreshed this filter while its old save
				// was outstanding. Keep that result with the saved options.
				if current, held := s.subscriptions[update.Filter]; held && current.ReadAccessFile == c.access {
					sub.ReadAccess, sub.ReadAccessFile = current.ReadAccess, current.ReadAccessFile
				}
				c.refreshSubscriptionAccess(s, &sub)
				s.subscriptions[update.Filter] = sub
			}
			c.applySubscriptionPositions(s, op.Positions)
		case *store.RemoveSubscriptions:
			for _, filter := range op.Filters {
				delete(s.subscriptions, filter)
			}
			for _, channel := range op.Channels {
				if u := s.consumer(channel); u != nil {
					c.endSubscriptionReach(s, u, now)
				}
			}
		}
		c.syncShared(s, now)
	}
	if c.failure != nil {
		return c.completeStoredReply(acts, s, r.replySlot, nil, false)
	}
	if err != nil {
		switch ack := r.reply.(type) {
		case *packet.Suback:
			for i, reason := range ack.ReasonCodes {
				if reason < 0x80 {
					ack.ReasonCodes[i] = 0x97
					c.subscriptionRefused[0x97].Inc()
				}
			}
		case *packet.Unsuback:
			if s.version == packet.V311 {
				return c.completeStoredReply(c.finish(acts, s, 0, now), s, r.replySlot, nil, false)
			}
			for i := range ack.ReasonCodes {
				ack.ReasonCodes[i] = 0x80
			}
		}
	}
	if s.end.seen || s.phase != connected || c.connections[s.conn] != s {
		return c.completeStoredReply(acts, s, r.replySlot, nil, false)
	}
	if err == nil {
		c.syncConsumer(s, now)
		if r.pass != nil && c.phase < quiescing {
			c.startRetainedPass(s, r.pass, 0)
		}
	}
	return c.completeStoredReply(acts, s, r.replySlot, r.reply, r.orderedReply && err == nil)
}

// finishCancellations returns transaction-owned preparations after either a
// subscription removal or a seek. Failed writes leave live preparations intact.
func (c *Core) finishCancellations(r *storeRequest, err error, now time.Time) {
	s := r.conn
	for _, d := range r.cancellations {
		d.saving = false
		u := d.consumer
		switch {
		case err == nil || s.end.seen || s.phase != connected || c.failure != nil:
			id, saved := d.exchange.PacketID, d.saved
			if err == nil {
				// This transaction already removed the preparation. Retire
				// it once, including when its payload read still owns bytes;
				// release must not create a second deletion or dirty position.
				d.settled, d.removed = true, true
				c.removePendingSave(d)
				if u.sharedCursor != nil && u.share.queue == nil && u.share.active {
					d.sharedReturned, d.sharedStored = true, false
					u.share.invalidateReturned()
				}
				if saved {
					s.endDelivery(d, deliveryEnd{offset: d.offset, cancelled: true})
				}
			}
			c.releaseDelivery(s, d, now)
			if err == nil && saved {
				c.retiredDelivery(s, u, id)
			}
			c.advanceConsumer(u, now)
		case d.accessRevoked:
			c.releaseDelivery(s, d, now)
			c.advanceConsumer(u, now)
		default:
			c.queueConsumer(u)
		}
	}
	r.cancellations = nil
}
