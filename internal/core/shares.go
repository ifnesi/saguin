package core

import (
	"cmp"
	"container/heap"
	"container/list"
	"errors"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// sharedGroup owns common progress, independently of its members' ordinary
// positions. Only connected members live here. Durable membership is an
// aggregate; returned work is paged from the store, never kept as a backlog.
type sharedGroup struct {
	queue                         *queueState // a queue uses this dispatcher without stored shared-group rows
	workWait                      list.List
	references                    int
	qualified                     int       // qualifying member/channel owners, maintained by their admission transitions
	bookkeepingAt                 time.Time // in the group's qualified time base
	bookkeepingPause              brokerWait
	bookkeeping                   coreDeadline
	endings                       map[int64]bool // only session endings pending in the core
	id                            int64
	channel                       int64 // last channel visited, independently of member round-robin
	filter, part, last            string
	durable                       int64
	loaded, stored, active, dirty bool
	request                       *storeRequest
	cleanup                       *storeRequest    // at most one scheduled ending per group owner
	members                       []*sharedMember  // complete client-id order
	cursors                       []*consumerState // channel-id order
	returned                      store.SharedRecord
	returnAfter                   store.SharedRecord
	returnRevision                uint64
	retiring                      store.SharedRecord // at most the missing returned head
	returnKnown                   bool
	assigned                      map[store.SharedRecord]*outgoing // bounded by members' windows
	visit                         uint64                           // bounded metadata visit shared by its recipients
	lastSelection                 *outgoing                        // latest round-robin selection, never a scheduling dependency
	writers                       list.List                        // owners whose exchange writes can wait on this group
	pending                       []*outgoing                      // assignment and terminal evidence retain bounded descriptors through commit
}

type sharedMember struct {
	attempt *queueAttempt
	visit   uint64 // most recent common-page selection, for channel fairness
	ending  bool
	group   *sharedGroup
	conn    *connectionState
	sub     store.Subscription
	owners  []*consumerState
}

type sharedEnding struct {
	waitRequests list.List
	write        *store.EndSession
	load         *store.LoadEndingShares
	loaded       bool
}

func (c *Core) captureSharedEnd(s *connectionState) {
	if s.sharedEnd != nil {
		return
	}
	op := &store.SharedEnding{}
	for _, m := range s.shared {
		op.Groups = append(op.Groups, store.SharedGroup{ID: m.group.id, Filter: m.group.filter})
		if s.session.Expiry == 0 || (s.end.seen && s.end.expiry == 0) {
			m.ending = true
			if m.group.endings == nil {
				m.group.endings = make(map[int64]bool)
			}
			m.group.endings[s.session.ID] = true
		}
	}
	if s.pendingShared != 0 {
		if lane := c.lanes[s.clientID]; lane != nil {
			add := func(r *storeRequest) {
				if r != nil && r.conn == s {
					for _, g := range r.shares {
						if g.loaded {
							op.Groups = append(op.Groups, store.SharedGroup{ID: g.id, Filter: g.filter})
						}
					}
				}
			}
			add(lane.request)
			for _, r := range lane.pending {
				add(r)
			}
		}
	}
	for i := range s.window {
		d := &s.window[i]
		if d.consumer == nil || d.consumer.share == nil || d.exchange.Share == 0 || d.settled {
			continue
		}
		g := d.consumer.share
		if !g.active && d.attempted {
			continue // only MQTT remains after the group's membership ends
		}
		op.Groups = append(op.Groups, store.SharedGroup{ID: g.id, Filter: g.filter})
		if !d.attempted {
			if d.saved {
				op.Cancel = append(op.Cancel, d.exchange.PacketID)
			}
			op.Return = append(op.Return, store.SharedReturn{Share: g.id, SharedRecord: d.sharedRecord()})
			continue
		}
		if d.saved || d.sharedStored {
			continue
		}
		switch d.exchange.QoS {
		case 1:
			op.Return = append(op.Return, store.SharedReturn{Share: g.id, SharedRecord: d.sharedRecord()})
		case 2:
			if op.Dropped == nil {
				op.Dropped = make(map[int64]uint64)
			}
			op.Dropped[d.consumer.channel.row.ID]++
		}
	}
	if len(op.Groups) != 0 {
		s.sharedEnd = op
	}
}

func (c *Core) markSharedEnd(s *connectionState) {
	c.captureSharedEnd(s)
	for _, m := range s.shared {
		if !m.ending {
			m.ending = true
			if m.group.endings == nil {
				m.group.endings = make(map[int64]bool)
			}
			m.group.endings[s.session.ID] = true
		}
	}
}

func endingSession(op any) *store.EndSession {
	switch w := op.(type) {
	case *store.EndSession:
		return w
	case *Group:
		for _, member := range w.Writes {
			if end := endingSession(member); end != nil {
				return end
			}
		}
	}
	return nil
}

// sessionEndings visits the session transitions in this indivisible request.
// A Will's original request keeps its completion, while its publication batch
// acquires all execution owners for the transaction.
func (r *storeRequest) sessionEndings(yield func(*storeRequest) bool) {
	if r.kind != savePublishes {
		yield(r)
		return
	}
	if r.publishes.wills == 0 {
		return
	}
	for _, publications := range [2][]*publication{r.publishes.publications, r.publishes.dropped} {
		for _, p := range publications {
			if p.will != nil && !yield(p.will) {
				return
			}
		}
	}
}

// prepareSharedEnding discovers affected group keys before acquiring their
// request ownership. All owners are reserved together, including every ending
// carried by one publication batch.
func (c *Core) prepareSharedEnding(r *storeRequest) bool {
	if c.nextShare == 0 {
		return true
	}
	for owner := range r.sessionEndings {
		if !c.loadSharedEnding(owner, r) {
			return false
		}
	}
	if r.kind != savePublishes && r.sharedEnd == nil {
		return true
	}
	if !c.reserveSharedRequest(r) {
		return false
	}
	for owner := range r.sessionEndings {
		if owner.sharedEnd != nil {
			c.snapshotSharedEnding(owner)
		}
	}
	return true
}

func (c *Core) loadSharedEnding(r, request *storeRequest) bool {
	end := endingSession(r.before)
	if end == nil {
		end = endingSession(r.op)
	}
	if end == nil {
		return true
	}
	if r.sharedEnd == nil {
		if end.Shared == nil {
			if s := r.conn; s != nil && s.attempt != nil && s.attempt.takeover != nil && s.attempt.takeover.session.ID == end.ID {
				end.Shared = s.attempt.takeover.sharedEnd
			}
			if end.Shared == nil {
				end.Shared = &store.SharedEnding{}
			}
		}
		state := &sharedEnding{write: end, load: &store.LoadEndingShares{Session: end.ID}}
		r.sharedEnd = state
		c.enqueue(&storeRequest{kind: loadSharedEnding, op: state.load, sharedEnd: state})
		c.parkRequest(request, &state.waitRequests)
		return false
	}
	state := r.sharedEnd
	if !state.loaded {
		c.parkRequest(request, &state.waitRequests)
		return false
	}
	// Captured volatile memberships can precede their group's load. Resolve
	// all provisional keys before allocating references or reserving any group.
	for i := range end.Shared.Groups {
		row := &end.Shared.Groups[i]
		if g := c.shares[row.Filter]; g != nil && row.ID == 0 {
			if !g.loaded {
				c.parkRequest(request, &g.workWait)
				return false
			}
			row.ID = g.id
		}
	}
	slices.SortFunc(end.Shared.Groups, func(a, b store.SharedGroup) int { return cmp.Compare(a.ID, b.ID) })
	end.Shared.Groups = slices.CompactFunc(end.Shared.Groups, func(a, b store.SharedGroup) bool { return a.ID == b.ID })
	for _, row := range end.Shared.Groups {
		// Even an away group's key needs an owner through this ending.
		// Several endings in a batch share one reference and reservation.
		if slices.ContainsFunc(request.shares, func(g *sharedGroup) bool { return g.filter == row.Filter }) {
			continue
		}
		g := c.loadShared(row.Filter)
		g.references++
		request.shares = append(request.shares, g)
	}
	return true
}

func (c *Core) snapshotSharedEnding(r *storeRequest) {
	end := r.sharedEnd.write
	end.Shared.Live = make(map[int64]bool)
	end.Shared.Progress = nil
	for _, row := range end.Shared.Groups {
		g := c.shares[row.Filter]
		if end.KeepHeld {
			if _, continuing := r.conn.subscriptions[g.filter]; continuing {
				end.Shared.Live[g.id] = true
			}
		}
		progress := &store.SharedChange{ID: g.id}
		for _, u := range g.cursors {
			progress.Cursors = append(progress.Cursors, store.SubscriptionStart{Channel: u.channel.row.ID, Start: u.position})
		}
		end.Shared.Progress = append(end.Shared.Progress, progress)
		for _, m := range g.members {
			if m.conn.session.ID != end.ID && m.conn.phase == connected && !m.conn.end.seen {
				end.Shared.Live[g.id] = true
				break
			}
		}
	}
}

func (c *Core) loadedSharedEnding(r *storeRequest) {
	state := r.sharedEnd
	state.write.Shared.Groups = append(state.write.Shared.Groups, state.load.Groups...)
	state.load.Groups = nil
	state.loaded = true
	c.wakeRequests(&state.waitRequests)
}

func (c *Core) completedSharedEnding(r *storeRequest, err error, now time.Time) {
	if r.sharedEnd == nil || r.kind == loadSharedEnding {
		return
	}
	end := r.sharedEnd.write
	if errors.Is(err, store.ErrFull) {
		// The ordinary orphan sweep also discovers volatile assignments.
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
	}
	if err == nil {
		for channel, count := range end.Shared.Dropped {
			c.sharesDropped[channel].Add(count)
		}
	}
	if err == nil && end.KeepHeld {
		// The commit continues these memberships with their loaded cursors.
		// Install them before returning the transaction's group references.
		c.syncShared(r.conn, now)
	}
	for _, row := range end.Shared.Groups {
		g := c.sharesByID[row.ID]
		if g == nil {
			continue
		}
		if err == nil {
			g.durable = end.Shared.Members[g.id]
			g.dirty = false
			g.invalidateReturned()
		}
		for _, m := range slices.Backward(g.members) {
			if !end.KeepHeld && m.conn.session.ID == end.ID {
				c.removeSharedMember(m)
			}
		}
		delete(g.endings, end.ID)
		g.active = g.durable != 0 || len(g.members) != 0
		if !g.active && errors.Is(err, store.ErrFull) {
			// Refusal did not reclaim evidence. Started exchanges and their
			// bounded pending dispositions still need this bookkeeping owner.
			g.active = len(g.assigned) != 0 || len(g.pending) != 0
			if !g.active {
				c.forgetShared(g)
			}
		} else if !g.active {
			c.endShared(g, now)
		}
	}
	c.releaseSharedRequest(r)
}

func (c *Core) endShared(g *sharedGroup, now time.Time) {
	// Membership ends, while started exchanges keep their shared provenance
	// through MQTT completion. Their stored terminal evidence cannot be returned.
	for _, d := range g.assigned {
		d.sharedReturned = false
		d.sharedStored = false
	}
	c.forgetShared(g)
	// Terminal acknowledgements may have arrived behind the deletion's
	// group owner. Its commit reclaimed their evidence too; retire those
	// bounded descriptors instead of queuing a write for the deleted group.
	pending := len(g.pending)
	for i := range pending {
		d := g.pending[i]
		g.pending[i] = nil
		d.sharedSave = false
		if d.sharedRefused && d.saved {
			// The ending retired this never-written session exchange too.
			// Retire it through its session owner instead of returning work
			// to a group whose final membership has already ended.
			d.saving, d.settled, d.sharedRefused = false, false, false
			c.settleDelivery(d.consumer.conn, d, now)
		} else {
			c.committedSharedEvidence(nil, d, store.SharedTerminal, now)
		}
	}
	g.pending = slices.Delete(g.pending, 0, pending)
}

// scheduleSharedCleanup gives the ending the same group lane as membership
// changes. A settlement can name an away group with no current core owner.
// Loading it fixes its current membership before the ending reserves it.
func (c *Core) scheduleSharedCleanup(row store.SharedGroup) {
	g := c.shares[row.Filter]
	if g != nil && g.loaded && (g.id != row.ID || g.durable != 0 || len(g.members) != 0) {
		return
	}
	g = c.loadShared(row.Filter)
	if g.cleanup != nil {
		return
	}
	g.references++
	r := &storeRequest{kind: endOrphanShared, op: &store.SharedChange{ID: row.ID, End: true},
		shares: []*sharedGroup{g}}
	g.cleanup = r
	c.expiry.ending++
	c.enqueue(r)
}

func (c *Core) completedSharedCleanup(r *storeRequest, err error, now time.Time) {
	g := r.shares[0]
	if err == nil {
		c.endShared(g, now)
	}
	// Refusal leaves any bounded assignments and dispositions with their
	// owners. Returning the lane forgets the group only when none remain.
	c.releaseSharedRequest(r)
	c.expired(err, now, false)
}

func (c *Core) forgetShared(g *sharedGroup) {
	c.wakeRequests(&g.workWait)
	c.admission.ages.set(&g.bookkeeping, time.Time{})
	for _, u := range g.cursors {
		c.removeDeliveryRetry(u)
		c.admission.ages.set(&u.admission.progress, time.Time{})
		c.indexConsumer(u, false)
		if u.queue != nil {
			u.queue.remove(u)
		}
	}
	if c.shares[g.filter] == g {
		delete(c.shares, g.filter)
	}
	delete(c.sharesByID, g.id)
	g.active = false
}

// claimShared removes every other channel visit when one request acquires
// the group. Returning ownership wakes the same registered visits.
func (c *Core) claimShared(g *sharedGroup, r *storeRequest) {
	g.request = r
	c.wakeSharedWriters(g)
	c.queueShared(g)
}

// roundRobin visits the sorted recipients once, starting after the complete
// last-selected id. It supplies the selection rule shared with queue workers.
func roundRobin[T any, K cmp.Ordered](members []T, last K, key func(T) K, eligible func(T) bool) (selected T, found bool) {
	start, _ := slices.BinarySearchFunc(members, last, func(m T, id K) int { return cmp.Compare(key(m), id) })
	if start < len(members) && key(members[start]) == last {
		start++
	}
	for i := range members {
		m := members[(start+i)%len(members)]
		if eligible(m) {
			return m, true
		}
	}
	return selected, false
}

func (c *Core) loadShared(filter string) *sharedGroup {
	g := c.shares[filter]
	if g != nil {
		return g
	}
	_, part, _ := topic.SplitShared(filter)
	g = &sharedGroup{filter: filter, part: part, assigned: make(map[store.SharedRecord]*outgoing)}
	c.shares[filter] = g
	op := &store.LoadSharedGroup{Filter: filter}
	r := &storeRequest{kind: loadSharedGroup, op: op, share: g}
	g.request = r
	c.enqueue(r)
	return g
}

func (c *Core) loadedShared(r *storeRequest) {
	g, op := r.share, r.op.(*store.LoadSharedGroup)
	g.loaded, g.stored = true, op.Found
	g.id, g.last, g.durable = op.ID, op.Last, op.Members
	if !op.Found {
		c.nextShare++
		g.id = c.nextShare
	}
	c.sharesByID[g.id] = g
	for _, cursor := range op.Cursors {
		if ch := c.channel(cursor.Channel); ch != nil {
			c.addSharedCursor(g, ch, cursor.Start)
		}
	}
	for _, member := range g.members {
		c.compileSharedMember(member)
		c.queueConsumers(member.conn)
	}
	c.finishSharedRequest(g, r)
}

func (c *Core) addSharedCursor(g *sharedGroup, ch *channelState, start int64) *consumerState {
	u := c.newConsumer(nil, ch, max(start, ch.row.Floor))
	u.share = g
	u.filters = []store.Subscription{{Filter: g.part, QoS: 2, ReadAccess: acl.Full}}
	u.sharedBounds.boundaries = true
	u.sharedHandoffs.kind = 2
	u.sharedHeadSeen = u.scan
	u.admission.progress.owner = u
	if c.admission.delay != 0 {
		c.admission.ages.reserve(c.admission.ages.Len() + 1)
	}
	g.cursors = append(g.cursors, u)
	slices.SortFunc(g.cursors, func(a, b *consumerState) int { return cmp.Compare(a.channel.row.ID, b.channel.row.ID) })
	return u
}

func (g *sharedGroup) cursor(channel int64) *consumerState {
	i, ok := slices.BinarySearchFunc(g.cursors, channel, func(u *consumerState, id int64) int { return cmp.Compare(u.channel.row.ID, id) })
	if ok {
		return g.cursors[i]
	}
	return nil
}

func (m *sharedMember) owner(channel int64) *consumerState {
	i, ok := slices.BinarySearchFunc(m.owners, channel, func(u *consumerState, id int64) int { return cmp.Compare(u.channel.row.ID, id) })
	if ok {
		return m.owners[i]
	}
	return nil
}

// reserveSharedGroups reserves every affected group together, after earlier
// reads and writes return. Subscription changes and seek cancellations share
// this ownership; a waiting packet keeps its session lane.
func (c *Core) reserveSharedGroups(r *storeRequest) bool {
	if r.shares == nil {
		var filters []string
		if r.request != nil && r.request.kind == seekService {
			for i := range r.conn.window {
				d := &r.conn.window[i]
				if d.consumer != nil && d.consumer.channel == r.channel && d.consumer.share != nil &&
					d.consumer.share.active && d.consumer.share.queue == nil && !d.attempted && !d.settled {
					filters = append(filters, d.consumer.share.filter)
				}
			}
		}
		switch p := r.op.(type) {
		case *connection.Subscribe:
			for _, sub := range p.Filters {
				if group, _, _ := topic.SplitShared(sub.Filter); group != "" {
					filters = append(filters, sub.Filter)
				}
			}
		case *connection.Unsubscribe:
			for _, filter := range p.Filters {
				if group, _, _ := topic.SplitShared(filter); group != "" {
					filters = append(filters, filter)
				}
			}
		}
		r.shares = make([]*sharedGroup, 0, len(filters))
		for _, filter := range filters {
			g := c.loadShared(filter)
			if slices.Contains(r.shares, g) {
				continue
			}
			g.references++
			r.shares = append(r.shares, g)
		}
		if len(r.shares) != 0 && r.request == nil {
			r.conn.pendingShared++
		}
	}
	return c.reserveSharedRequest(r)
}

func (c *Core) reserveSharedRequest(r *storeRequest) bool {
	for i, g := range r.shares {
		if g.queue == nil && c.shares[g.filter] != g {
			if g.cleanup == r {
				g.cleanup = nil
			}
			g.references--
			g = c.loadShared(g.filter)
			g.references++
			r.shares[i] = g
		}
		if !g.loaded || (g.request != nil && g.request != r) || len(g.pending) != 0 {
			c.parkRequest(r, &g.workWait)
			return false
		}
	}
	for _, g := range r.shares {
		c.claimShared(g, r)
	}
	return true
}

func (c *Core) releaseSharedRequest(r *storeRequest) {
	if len(r.shares) != 0 && (r.kind == changeSubscriptions || r.kind == saveSubscriptions) {
		r.conn.pendingShared--
	}
	for _, g := range r.shares {
		if g.cleanup == r {
			g.cleanup = nil
		}
		g.references--
		c.finishSharedRequest(g, r)
	}
	r.shares = nil
	if g := r.share; g != nil {
		c.finishSharedRequest(g, r)
	}
}

// finishSharedRequest returns the group lane and wakes both its common scan
// and its member work. Returned work is ready independently of new traffic;
// preparations held behind a group read need the same wake as after a write.
func (c *Core) finishSharedRequest(g *sharedGroup, r *storeRequest) {
	if g.request == r {
		g.request = nil
		c.wakeRequests(&g.workWait)
		c.wakeSharedWriters(g)
		c.queueShared(g)
	}
}

// sharedSubscriptionChange joins group creation and the first durable cursors
// to SUBSCRIBE's transaction. Existing groups keep their common progress.
func (c *Core) sharedSubscriptionChange(s *connectionState, sub *store.Subscription, boundaries []subscriptionBoundary, fresh bool) *store.SharedChange {
	g := c.shares[sub.Filter]
	sub.Share, sub.MatchFilter = g.id, g.part
	change := &store.SharedChange{ID: g.id, Filter: g.filter, New: !g.stored}
	if s.stored && fresh && g.durable == 0 {
		// Capture live volatile obligations in the membership transaction,
		// before its SUBACK establishes the group's first durable owner.
		if g.last != "" {
			last := g.last
			change.Last = &last
		}
		for _, d := range g.assigned {
			if !d.saved && !d.settled && d.exchange.QoS != 0 {
				change.Evidence = append(change.Evidence, sharedEvidence(d))
			}
		}
		slices.SortFunc(change.Evidence, func(a, b store.SharedEvidence) int {
			if order := cmp.Compare(a.Channel, b.Channel); order != 0 {
				return order
			}
			return cmp.Compare(a.Offset, b.Offset)
		})
	}
	for _, boundary := range boundaries {
		u := g.cursor(boundary.start.Channel)
		if u == nil {
			u = c.addSharedCursor(g, c.channel(boundary.start.Channel), boundary.start.Start)
		}
		if s.stored && ((fresh && g.durable == 0) || !g.stored) {
			change.Cursors = append(change.Cursors, store.SubscriptionStart{Channel: u.channel.row.ID, Start: u.position})
		}
	}
	if !change.New && change.Last == nil && len(change.Cursors) == 0 && len(change.Evidence) == 0 {
		return nil
	}
	return change
}

func (c *Core) syncShared(s *connectionState, now time.Time) {
	c.syncWorkers(s)
	for i, m := range slices.Backward(s.shared) {
		if _, ok := s.subscriptions[m.group.filter]; ok {
			continue
		}
		g := m.group
		c.removeSharedMember(m)
		s.shared = slices.Delete(s.shared, i, i+1)
		if s.stored {
			g.durable--
		}
		g.active = g.durable != 0 || len(g.members) != 0
		if !g.active {
			c.endShared(g, now)
		}
	}
	// Committed removals still apply after departure, but cannot restore
	// recipient eligibility to an ended or superseded connection.
	if s.end.seen || s.phase >= refused || c.connections[s.conn] != s {
		return
	}
	filters := make([]string, 0, len(s.subscriptions))
	for filter := range s.subscriptions {
		if group, _, _ := topic.SplitShared(filter); group != "" {
			filters = append(filters, filter)
		}
	}
	slices.Sort(filters)
	for _, filter := range filters {
		g := c.loadShared(filter)
		var m *sharedMember
		for _, member := range s.shared {
			if member.group == g {
				m = member
				break
			}
		}
		if m == nil {
			m = c.addRecipient(s, g)
			s.shared = append(s.shared, m)
		}
		m.sub = s.subscriptions[filter]
		if g.loaded {
			c.compileSharedMember(m)
		}
	}
}

// removeSharedMember retires recipient eligibility with membership, including
// owners with no exchange left to produce another wake. Started deliveries
// keep their MQTT bookkeeping, but no departed owner can hold admission shut.
func (c *Core) removeSharedMember(m *sharedMember) {
	g := m.group
	m.ending = true
	g.members = slices.DeleteFunc(g.members, func(other *sharedMember) bool { return other == m })
	for _, u := range m.owners {
		u.held = true
		if c.admission.delay != 0 {
			c.refreshAdmission(u, c.eventAt)
		}
		c.queueConsumer(u)
	}
}

func (c *Core) compileSharedMember(m *sharedMember) {
	g, s := m.group, m.conn
	m.sub.Share, m.sub.MatchFilter = g.id, g.part
	c.refreshSubscriptionAccess(s, &m.sub)
	if m.sub.Channels == nil {
		for _, index := range c.routing.Reaches(g.part) {
			ch := &c.broadcast
			if index >= 0 {
				ch = c.routed[index]
			}
			m.sub.Channels = append(m.sub.Channels, ch.row.ID)
		}
		slices.Sort(m.sub.Channels)
	}
	s.subscriptions[g.filter] = m.sub
	for _, channel := range m.sub.Channels {
		cursor := g.cursor(channel)
		if cursor == nil {
			cursor = c.addSharedCursor(g, c.channel(channel), c.channel(channel).row.Next)
		}
		u := c.recipientOwner(m, cursor)
		u.filters = []store.Subscription{m.sub}
		u.sharedBlocked = 0
		u.held = m.sub.ReadAccess == acl.None
		c.initWindow(s)
		g.active = true
		c.indexConsumer(cursor, true)
		c.queueConsumer(u)
	}
	c.queueShared(g)
}

// addRecipient installs membership in the common client-id selection order.
func (c *Core) addRecipient(s *connectionState, g *sharedGroup) *sharedMember {
	m := &sharedMember{conn: s, group: g}
	c.joinRecipient(m)
	return m
}

func (c *Core) joinRecipient(m *sharedMember) {
	g := m.group
	m.ending = false
	g.members = append(g.members, m)
	slices.SortFunc(g.members, func(a, b *sharedMember) int { return cmp.Compare(a.conn.clientID, b.conn.clientID) })
}

// recipientOwner gives each member the same delivery, capacity and admission
// owner for a channel, for shared subscriptions and worker offers alike.
func (c *Core) recipientOwner(m *sharedMember, cursor *consumerState) *consumerState {
	u := m.owner(cursor.channel.row.ID)
	if u != nil {
		return u
	}
	u = c.newConsumer(m.conn, cursor.channel, cursor.scan)
	u.share, u.sharedMember, u.sharedCursor = m.group, m, cursor
	cursor.sharedBounds.reserve(len(m.group.members) + cursor.sharedBounds.Len() + 1)
	m.owners = append(m.owners, u)
	slices.SortFunc(m.owners, func(a, b *consumerState) int { return cmp.Compare(a.channel.row.ID, b.channel.row.ID) })
	return u
}

func (c *Core) sharesReady(s *connectionState) bool {
	for _, m := range s.shared {
		if !m.group.loaded {
			return false
		}
	}
	return true
}

// sharedCapacity is the common scheduler/admission decision. Occupied
// descriptors and future send capacity can both depend on peer progress.
func (c *Core) sharedCapacity(u *consumerState) int {
	s := u.conn
	if (u.share.queue != nil && (u.sharedMember.attempt != nil || len(u.recovered) != 0)) || u.held || u.sharedMember.ending || u.sharedBlocked == u.sharedHead() || s.phase != connected || s.end.seen ||
		s.pendingSubscriptions != 0 || s.resuming != nil || !c.payloadRoom(s) {
		return 0
	}
	room := max(0, c.retainedRoom(u))
	if u.share.queue != nil {
		room = min(room, 1)
	}
	return room
}

func (c *Core) sharedRoom(m *sharedMember, channel int64) bool {
	u := m.owner(channel)
	return u != nil && c.sharedCapacity(u) > 0
}

// sharedVisitRoom bounds useful selection by both capacity and channel visits.
// A member reaching other channels takes one delivery before their next visit.
func sharedVisitLimited(m *sharedMember) bool {
	return len(m.owners) > 1 || len(m.conn.shared) > 1 || len(m.conn.consumers) != 0
}

func (c *Core) sharedVisitRoom(m *sharedMember, u *consumerState) int {
	room := c.sharedCapacity(u)
	if sharedVisitLimited(m) {
		room = min(room, 1)
	}
	return room
}

// Capacity belongs to member transitions, even with admission disabled. Page
// budgets and readiness therefore need no additional pass over membership.
func (c *Core) indexSharedCapacity(u *consumerState) {
	budget := c.sharedVisitRoom(u.sharedMember, u)
	if budget != u.sharedBudget {
		u.sharedCursor.sharedAvailable += budget - u.sharedBudget
		u.sharedBudget = budget
		c.queueShared(u.share)
	}
}

func (c *Core) indexSharedWriter(u *consumerState) {
	pending := !u.sharedMember.ending && (u.dirty || u.pendingSaves.Len() != 0)
	switch {
	case pending && u.sharedWrite == nil:
		u.sharedWrite = u.share.writers.PushBack(u)
	case !pending && u.sharedWrite != nil:
		u.share.writers.Remove(u.sharedWrite)
		u.sharedWrite = nil
	default:
		return
	}
	c.queueShared(u.share)
}

// Refusals remain indexed by their offset even while the scan is ahead. A
// returned head can restore that dependency; only the old and new heads wake.
func (c *Core) indexSharedRefusal(u *consumerState) {
	at := u.sharedBlocked
	if u.sharedMember.ending {
		at = 0
	}
	if at == u.sharedRefusalAt {
		return
	}
	cursor := u.sharedCursor
	if u.sharedRefusal != nil {
		q := cursor.sharedRefusals[u.sharedRefusalAt]
		q.Remove(u.sharedRefusal)
		if q.Len() == 0 {
			delete(cursor.sharedRefusals, u.sharedRefusalAt)
		}
		u.sharedRefusal = nil
	}
	u.sharedRefusalAt = at
	if at != 0 {
		if cursor.sharedRefusals == nil {
			cursor.sharedRefusals = make(map[int64]*list.List)
		}
		q := cursor.sharedRefusals[at]
		if q == nil {
			q = new(list.List)
			cursor.sharedRefusals[at] = q
		}
		u.sharedRefusal = q.PushBack(u)
	}
}

func (c *Core) wakeSharedRefusals(cursor *consumerState) {
	head := cursor.scan
	if returned := cursor.share.returned; returned.Channel == cursor.channel.row.ID {
		head = returned.Offset
	}
	if cursor.sharedHeadSeen == head {
		return
	}
	previous := cursor.sharedHeadSeen
	cursor.sharedHeadSeen = head
	for _, offset := range [2]int64{previous, head} {
		if q := cursor.sharedRefusals[offset]; q != nil {
			for e := q.Front(); e != nil; {
				next := e.Next()
				c.queueConsumer(e.Value.(*consumerState))
				e = next
			}
		}
	}
}

func (c *Core) wakeSharedWriters(g *sharedGroup) {
	for e := g.writers.Front(); e != nil; {
		next := e.Next()
		c.queueConsumer(e.Value.(*consumerState))
		e = next
	}
}

func (u *consumerState) sharedHandoffPosition() int64 {
	position := u.scan
	if u.sharedHandoffs.Len() != 0 {
		position = min(position, u.sharedHandoffs.items[0].offset)
	}
	return position
}

func (u *consumerState) sharedHead() int64 {
	if returned := u.share.returned; returned.Channel == u.channel.row.ID {
		return returned.Offset
	}
	return u.sharedCursor.scan
}

// refreshSharedAdmission indexes one bounded bookkeeping owner per group.
// Member transitions maintain qualification without scanning the membership.
func (c *Core) refreshSharedAdmission(g *sharedGroup, now time.Time) {
	if c.admission.delay == 0 {
		return
	}
	qualified := g.active && g.qualified != 0
	g.bookkeepingPause.run(!qualified, now)
	clock := now.Add(-g.bookkeepingPause.age(now))
	pending := len(g.pending) != 0 || g.dirty || g.retiring.Channel != 0
	if pending && g.bookkeepingAt.IsZero() {
		g.bookkeepingAt = clock
	}
	if !pending {
		g.bookkeepingAt = time.Time{}
	}
	var at time.Time
	if qualified {
		at = g.bookkeepingAt
		if g.request != nil && g.request.kind == saveSharedGroup {
			at = firstTime(at, g.request.admissionAt)
		}
		if !at.IsZero() {
			at = now.Add(-clock.Sub(at))
		}
	}
	c.admission.ages.set(&g.bookkeeping, at)
}

func (c *Core) queueShared(g *sharedGroup) {
	c.refreshSharedAdmission(g, c.eventAt)
	if !g.loaded || (g.queue == nil && c.shares[g.filter] != g) {
		return
	}
	if g.queue == nil && len(g.members) == 0 && len(g.assigned) == 0 && g.references == 0 && len(g.endings) == 0 && g.request == nil && len(g.pending) == 0 && g.retiring.Channel == 0 && !g.dirty {
		c.forgetShared(g)
		return
	}
	for _, u := range g.cursors {
		c.queueConsumer(u)
	}
}

func (c *Core) dropSharedConnection(s *connectionState) {
	c.dropWorkers(s)
	for _, m := range s.shared {
		g := m.group
		c.removeSharedMember(m)
		if g.durable == 0 && !m.ending {
			g.active = len(g.members) != 0
		}
		c.queueShared(g)
	}
}

// scanTurn is shared by assignment and admission. Free member capacity cannot
// advance another channel past a returned record.
func (g *sharedGroup) scanTurn(channel int64) bool {
	return !g.returnKnown || g.returned.Channel == 0 || g.returned.Channel == channel
}

func (c *Core) sharedQueue(u *consumerState) *consumerQueue {
	g := u.share
	if c.failure != nil || g.request != nil || u.refusedSave {
		return nil
	}
	if len(g.endings) != 0 && len(g.pending) == 0 {
		return nil
	}
	if len(g.pending) != 0 || g.retiring.Channel != 0 || (g.active && g.dirty && c.phase < quiescing) {
		return &c.deliveryChanges
	}
	// Member bookkeeping precedes later group reads. Releasing a read wakes
	// both owners in one event, whose common-scan tie would otherwise let
	// another read claim the group before the member can save its exchange.
	if g.writers.Len() != 0 {
		return nil
	}
	if c.phase >= quiescing || !g.active || !g.scanTurn(u.channel.row.ID) {
		return nil
	}
	if !g.returnKnown {
		return &c.deliveryReady
	}
	if g.returned.Channel == 0 && u.scan >= u.available {
		return nil
	}
	if c.nextSharedScan(g) == u {
		return &c.deliveryReady
	}
	return nil
}

// nextSharedScan uses the same round-robin rule as members and queue workers.
// One useful visit per member keeps channels fair without rereading a wide page.
func (c *Core) nextSharedScan(g *sharedGroup) *consumerState {
	u, _ := roundRobin(g.cursors, g.channel, func(u *consumerState) int64 { return u.channel.row.ID }, func(u *consumerState) bool {
		return !u.refusedSave && g.scanTurn(u.channel.row.ID) && u.sharedAvailable > 0 &&
			(g.returned.Channel != 0 || u.scan < u.available)
	})
	return u
}

func (c *Core) scheduleShared(u *consumerState) bool {
	g := u.share
	if g.queue != nil {
		return c.readMetadataPage(u)
	}
	if g.dirty || len(g.pending) != 0 || g.retiring.Channel != 0 {
		defer c.queueShared(g)
		op := &store.SharedChange{ID: g.id}
		if g.retiring.Channel != 0 {
			op.Settle = []store.SharedRecord{g.retiring}
		}
		if g.durable != 0 {
			last := g.last
			op.Last = &last
		}
		for _, cursor := range g.cursors {
			progress := store.SubscriptionStart{Channel: cursor.channel.row.ID, Start: cursor.position}
			if g.durable != 0 {
				op.Cursors = append(op.Cursors, progress)
			} else if cursor.sharedTerminalBefore != 0 {
				op.Prune = append(op.Prune, progress)
			}
		}
		g.dirty = false
		if op.Last != nil || len(op.Prune) != 0 || len(op.Settle) != 0 || len(g.pending) != 0 {
			r := &storeRequest{kind: saveSharedGroup, op: op, share: g, admissionAt: g.bookkeepingAt}
			g.bookkeepingAt = time.Time{}
			r.sharedSaves = len(g.pending)
			for _, d := range g.pending {
				op.Evidence = append(op.Evidence, sharedEvidence(d))
			}
			c.claimShared(g, r)
			c.enqueue(r)
		}
		return true
	}
	if !g.returnKnown {
		if c.metadataReads >= c.metadataLimit {
			return false
		}
		op := &store.ReadSharedReturned{Share: g.id, After: g.returnAfter, Limit: 1}
		r := &storeRequest{kind: readSharedReturned, op: op, share: g, sharedRevision: g.returnRevision}
		c.claimShared(g, r)
		c.metadataReads++
		c.enqueue(r)
		return true
	}
	return c.readMetadataPage(u)
}

// invalidateReturned prevents an older store view from restoring retired work.
func (g *sharedGroup) invalidateReturned() {
	g.returnRevision++
	g.returnAfter, g.returned, g.returnKnown = store.SharedRecord{}, store.SharedRecord{}, false
}

func (c *Core) readSharedReturned(r *storeRequest) {
	c.returnMetadataCredit()
	g, op := r.share, r.op.(*store.ReadSharedReturned)
	if r.sharedRevision != g.returnRevision {
		c.finishSharedRequest(g, r)
		return
	}
	g.returnKnown = true
	g.returned = store.SharedRecord{}
	if len(op.Rows) != 0 {
		g.returned = op.Rows[0]
		if g.assigned[g.returned] != nil {
			g.returnAfter, g.returnKnown = g.returned, false
		}
	}
	c.finishSharedRequest(g, r)
}

func (c *Core) advanceShared(g *sharedGroup, cursor *consumerState) {
	defer c.queueShared(g)
	position := cursor.scan
	if owner := cursor.sharedBounds.peek(); owner != nil {
		position = min(position, owner.boundaries[0].items[0].offset)
	}
	if position > cursor.position {
		cursor.position = position
		g.dirty = g.queue == nil && (g.dirty || g.durable != 0 || cursor.sharedTerminalBefore != 0)
	}
}

// updateSharedBound indexes one unsaved-prefix minimum per member. Advancing
// common progress never scans windows or retains settled gaps in memory.
func (u *consumerState) updateSharedBound() {
	q := &u.sharedCursor.sharedBounds
	switch {
	case u.boundaries[0].Len() == 0 && u.sharedBoundIndex != 0:
		heap.Remove(q, u.sharedBoundIndex-1)
	case u.boundaries[0].Len() != 0 && u.sharedBoundIndex == 0:
		heap.Push(q, u)
	case u.sharedBoundIndex != 0:
		heap.Fix(q, u.sharedBoundIndex-1)
	}
}

// selectShared selects a recipient before reserving its ordinary descriptor.
// Size and permission refusals pass that member without losing group work.
func (c *Core) selectShared(acts []Action, cursor *consumerState, op *store.ReadRecordMetadata, rows []store.RecordMetadata, now time.Time) []Action {
	g := cursor.share
	g.channel = cursor.channel.row.ID
	g.visit++
	returned := g.returned.Channel != 0
	found := false
	exhausted := true
	for i := range rows {
		m := &rows[i]
		if returned && m.Offset != g.returned.Offset {
			continue
		}
		found = true
		if g.queue != nil && g.queue.attempts[m.Offset] != nil {
			cursor.scan = max(cursor.scan, m.Offset+1)
			continue
		}
		if !returned && (m.Offset < cursor.scan || m.Offset >= op.Before) {
			continue
		}
		if !returned && (m.Settled || g.assigned[store.SharedRecord{Channel: cursor.channel.row.ID, Offset: m.Offset}] != nil) {
			if m.Settled {
				cursor.sharedTerminalBefore = max(cursor.sharedTerminalBefore, m.Offset+1)
			}
			cursor.scan = m.Offset + 1
			continue
		}
		if g.queue == nil && !topic.Match(g.part, string(m.Topic)) {
			if !returned {
				cursor.scan = m.Offset + 1
			}
			continue
		}
		if !returned {
			cursor.scan = m.Offset // offset gaps contain no work to assign
		}
		var p connection.Delivery
		var reservation int64
		var selectionErr error
		var attempt string
		member, ok := roundRobin(g.members, g.last, func(m *sharedMember) string { return m.conn.clientID }, func(member *sharedMember) bool {
			if (member.visit == g.visit && sharedVisitLimited(member)) || !c.sharedRoom(member, cursor.channel.row.ID) {
				return false
			}
			u := member.owner(cursor.channel.row.ID)
			grant := c.deliveryGrant(u, m, nil)
			if g.queue != nil {
				if attempt == "" {
					attempt = newMessageID(now)
				}
				grant.attempt = attempt
			}
			if !grant.matched {
				u.sharedBlocked = m.Offset
				c.queueConsumer(u)
				return false
			}
			var encodable bool
			p, reservation, encodable, selectionErr = c.deliveryPacket(u, m, grant)
			if selectionErr == nil && encodable && member.conn.maximumPacket != 0 {
				var minimum int
				minimum, selectionErr = minimumDeliverySize(member.conn, &p, m.PayloadSize)
				encodable = selectionErr == nil && uint64(minimum) <= uint64(member.conn.maximumPacket)
			}
			if selectionErr == nil && !encodable {
				u.sharedBlocked = m.Offset
				c.queueConsumer(u)
			}
			return selectionErr == nil && encodable
		})
		if selectionErr != nil {
			return c.fail(acts, selectionErr, now)
		}
		if !ok {
			exhausted = false
			break
		}
		u, s := member.owner(cursor.channel.row.ID), member.conn
		d := c.selectDelivery(u, &p, m.Offset, reservation, false)
		d.sharedPrevious = g.last
		g.lastSelection = d
		d.storedBytes = int64(m.StoredPayloadSize)
		d.sharedReturned, d.filters = returned, u.filters
		member.visit = g.visit
		heap.Push(&cursor.sharedHandoffs, d)
		c.selectedSharedAdmission(cursor, d, returned, now)
		if p.QoS != 0 {
			c.assignDelivery(s, d)
			d.exchange.Share, d.exchange.NoMove = g.id, true
			if g.queue != nil {
				c.assignAttempt(d, m, now, attempt)
			}
			d.exchange.Returned = returned
			if s.stored {
				d.exchange.Properties = p.Additions
				if d.exchange.Properties == nil {
					d.exchange.Properties = packet.SubscriptionProperties(p.SubscriptionIDs)
				}
			}
		}
		g.assigned[store.SharedRecord{Channel: cursor.channel.row.ID, Offset: m.Offset}] = d
		if !returned {
			cursor.scan = m.Offset + 1
			heap.Push(&u.boundaries[0], d)
			u.updateSharedBound()
		}
		if g.queue == nil && !s.stored && p.QoS != 0 {
			c.queueSharedEvidence(d)
		}
		g.last, g.dirty = s.clientID, g.queue == nil
		if returned {
			g.returnAfter, g.returnKnown, g.returned = g.returned, false, store.SharedRecord{}
		}
		c.queueConsumer(u)
		if returned {
			break // the next returned key has its own ordered discovery
		}
	}
	if returned && !found && (len(rows) < op.Limit || rows[len(rows)-1].Offset >= g.returned.Offset) {
		g.retiring = g.returned
	} else if exhausted && len(rows) == 0 && !returned {
		cursor.scan = min(op.Before, cursor.read.tail)
	}
	c.advanceShared(g, cursor)
	return acts
}

func (c *Core) sharedSaved(d *outgoing) {
	u := d.consumer
	if u.sharedCursor == nil {
		return
	}
	if index := d.boundary[0]; index != 0 {
		heap.Remove(&u.boundaries[0], index-1)
		u.updateSharedBound()
	}
	d.exchange.Returned = false
	c.advanceShared(u.share, u.sharedCursor)
}

func (c *Core) sharedHandedOff(d *outgoing) {
	u := d.consumer
	if u.sharedCursor == nil {
		return
	}
	if index := d.boundary[2]; index != 0 {
		heap.Remove(&u.sharedCursor.sharedHandoffs, index-1)
	}
	c.queueConsumer(u)
	c.queueConsumer(u.sharedCursor)
}

func (c *Core) releaseShared(d *outgoing) {
	u := d.consumer
	if u.sharedCursor == nil {
		return
	}
	cursor, g := u.sharedCursor, u.share
	if index := d.boundary[2]; index != 0 {
		heap.Remove(&cursor.sharedHandoffs, index-1)
	}
	if g.lastSelection == d {
		g.lastSelection = nil
	}
	if index := d.boundary[0]; index != 0 {
		heap.Remove(&u.boundaries[0], index-1)
		u.updateSharedBound()
	}
	if !d.attempted {
		if d.sharedReturned {
			// A returned QoS 0 preparation still owns its stored key. Once
			// cancelled, discovery must offer that key again too.
			g.invalidateReturned()
		} else {
			offset := max(d.offset, cursor.channel.row.Floor)
			cursor.scan, cursor.position = min(cursor.scan, offset), min(cursor.position, offset)
			g.dirty = g.queue == nil
		}
	}
	if g.queue != nil {
		c.releaseAttemptDelivery(d)
	}
	key := d.sharedRecord()
	if g.assigned[key] == d {
		delete(g.assigned, key)
	}
	c.advanceShared(g, cursor)
}

func (c *Core) sharedSettled(d *outgoing) {
	u := d.consumer
	if u.sharedCursor == nil {
		return
	}
	if index := d.boundary[0]; index != 0 {
		heap.Remove(&u.boundaries[0], index-1)
		u.updateSharedBound()
	}
	if u.share.queue != nil {
		return
	}
	if !d.saved && u.share.active {
		c.queueSharedEvidence(d)
	} else {
		if u.share.active && d.offset >= u.sharedCursor.persisted {
			u.sharedCursor.sharedTerminalBefore = max(u.sharedCursor.sharedTerminalBefore, d.offset+1)
		}
		delete(u.share.assigned, store.SharedRecord{Channel: u.channel.row.ID, Offset: d.offset})
	}
	c.advanceShared(u.share, u.sharedCursor)
}

func (d *outgoing) sharedRecord() store.SharedRecord {
	return store.SharedRecord{Channel: d.consumer.channel.row.ID, Offset: d.offset}
}

func sharedEvidence(d *outgoing) store.SharedEvidence {
	row := store.SharedEvidence{SharedRecord: d.sharedRecord(), State: store.SharedTerminal}
	if d.sharedRefused {
		row.State = store.SharedReturned
		if d.saved {
			row.Session, row.PacketID = d.consumer.conn.session.ID, d.exchange.PacketID
		}
	} else if !d.settled {
		row.State, row.Session, row.QoS = store.SharedAssigned, d.consumer.conn.session.ID, d.exchange.QoS
	}
	return row
}

// refuseShared returns a writer's size refusal without settling group work.
// The descriptor keeps its identifier and admission credit until the return
// and any durable assignment deletion commit together.
func (c *Core) refuseShared(d *outgoing, now time.Time) {
	u, s := d.consumer, d.consumer.conn
	g := u.share
	if q := g.queue; q != nil {
		u.sharedBlocked = d.offset
		if g.lastSelection == d {
			g.last = d.sharedPrevious
		}
		if a := d.attempt; a != nil {
			c.endAttempt(a)
			q.ready++
			q.updateJobs()
		}
		d.attempted = false
		c.settleDelivery(s, d, now)
		return
	}
	d.sharedRefused, d.settled = true, true
	u.sharedBlocked = d.offset
	if g.lastSelection == d {
		g.last = d.sharedPrevious
	}
	if d.quota {
		d.quota = false
		s.inflight--
	}
	if d.counted {
		d.counted = false
		c.countInflight(-1)
	}
	c.queueSharedEvidence(d)
	c.queueConsumer(u)
	c.queueConsumers(s)
}

// queueSharedEvidence retains the existing descriptor and identifier until
// its group disposition commits. Refusals keep these bounded owners, never a
// failed write body; the ordinary bookkeeping deadline reconstructs the write.
func (c *Core) queueSharedEvidence(d *outgoing) {
	u := d.consumer
	if !d.sharedSave {
		g := u.share
		g.pending = append(g.pending, d)
		d.sharedSave = true
	}
	d.saving = true
	if d.settled && !d.sharedRefused {
		u.sharedCursor.sharedTerminalBefore = max(u.sharedCursor.sharedTerminalBefore, d.offset+1)
	}
	c.queueShared(u.share)
}

func (c *Core) committedSharedEvidence(acts []Action, d *outgoing, state byte, now time.Time) []Action {
	g, s := d.consumer.share, d.consumer.conn
	d.saving = false
	if state == store.SharedAssigned {
		d.sharedStored = true
		c.sharedSaved(d)
		if d.settled {
			c.queueSharedEvidence(d)
			return acts
		}
		if s.phase == connected && !s.end.seen && c.failure == nil {
			if d.removed {
				c.removePreparation(d)
				return acts
			}
			return c.offerLoadedDeliveries(acts, d.consumer)
		}
	} else {
		d.sharedStored = false
		if state == store.SharedReturned {
			d.sharedReturned = true // the committed return owns replay; never rewind behind it
			if d.saved {
				d.saved = false
				s.recoveryCount--
			}
			g.invalidateReturned()
		}
		delete(g.assigned, store.SharedRecord{Channel: d.consumer.channel.row.ID, Offset: d.offset})
	}
	if d.stage != deliveryWriting && d.stage != deliveryLoading && d.stage != deliveryReleasing {
		c.releaseDelivery(s, d, now)
	}
	c.queueConsumers(s)
	return acts
}

func (c *Core) savedShared(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	g, op := r.share, r.op.(*store.SharedChange)
	if errors.Is(err, store.ErrFull) {
		g.bookkeepingAt = firstTime(g.bookkeepingAt, r.admissionAt)
		g.dirty = true
		for _, u := range g.cursors {
			c.refuseDeliverySave(u)
		}
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
	} else {
		for i := range r.sharedSaves {
			d := g.pending[i]
			g.pending[i] = nil
			d.sharedSave = false
			if err == nil {
				acts = c.committedSharedEvidence(acts, d, op.Evidence[i].State, now)
			} else {
				d.saving = false
				if d.stage != deliveryWriting && d.stage != deliveryLoading && d.stage != deliveryReleasing {
					c.releaseDelivery(d.consumer.conn, d, now)
				}
			}
		}
		g.pending = slices.Delete(g.pending, 0, r.sharedSaves)
		if err == nil {
			if len(op.Settle) != 0 {
				g.retiring = store.SharedRecord{}
				g.invalidateReturned()
			}
			for _, positions := range [2][]store.SubscriptionStart{op.Cursors, op.Prune} {
				for _, progress := range positions {
					if u := g.cursor(progress.Channel); u != nil {
						u.persisted = progress.Start
						if u.sharedTerminalBefore <= progress.Start {
							u.sharedTerminalBefore = 0
						}
					}
				}
			}
			if g.stored && g.durable == 0 && len(g.members) == 0 {
				c.scheduleSharedCleanup(store.SharedGroup{ID: g.id, Filter: g.filter})
			}
		}
	}
	r.sharedSaves = 0
	c.releaseSharedRequest(r)
	return acts
}
