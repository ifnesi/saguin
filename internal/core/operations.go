package core

import (
	"cmp"
	"net/http"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/operations/routes"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/store"
)

func (c *Core) operation(acts []Action, request *views.Request, now time.Time) []Action {
	c.wakeTicket++
	if c.admission.delay != 0 {
		c.eventAt = now
	}
	c.dropAbandonedOperations()
	c.requestOperation(request)
	return c.dispatch(acts, now)
}

func (c *Core) requestOperation(request *views.Request) {
	if request == nil || request.Abandoned() {
		return
	}
	// These observations need only live state. Store-reader pressure and
	// shutdown do not prevent the core from answering on this handover.
	switch request.Route {
	case routes.Health:
		code := http.StatusServiceUnavailable
		if c.updateReady() {
			code = http.StatusOK
		}
		request.Reply <- views.Result{Code: code}
		return
	case routes.Bridges:
		request.Reply <- views.Result{Code: http.StatusOK, Body: c.bridgesView()}
		return
	case routes.Refused:
		request.Reply <- views.Result{Code: http.StatusOK, Body: c.refusedView()}
		return
	case routes.PositionLost:
		request.Reply <- views.Result{Code: http.StatusOK, Body: c.positionLostView()}
		return
	}
	// Resolve a queue route before admission: a missing channel is a route
	// error even when every observation credit is still held by a reader.
	var queue *channelState
	if request.Route == routes.Queues {
		for _, ch := range c.channels {
			if ch.row.Name == request.Channel && ch.queue != nil {
				queue = ch
				break
			}
		}
		if queue == nil {
			request.Reply <- views.Result{Code: http.StatusNotFound}
			return
		}
	}
	// Only store-reading views need observation credit and a healthy store.
	// The final drain closes their admission, so polling cannot replenish
	// the finite set already admitted. Credit stays held through the reader's
	// final answer; HTTP timeouts cannot reuse it early.
	if c.phase >= draining || c.failure != nil || c.operationRequests.Len() == InboundCapacity {
		request.Reply <- views.Result{Code: http.StatusServiceUnavailable, Body: map[string]string{"status": "unavailable"}}
		return
	}
	r := &storeRequest{kind: readOperations, operation: request}
	observation := store.NewObservation(request.Done)
	switch request.Route {
	case routes.Sessions:
		r.op = &store.ReadSessionsView{Observation: observation}
	case routes.Consumers:
		r.op = &store.ReadConsumersView{Observation: observation}
	case routes.Queues:
		r.op = &store.ReadQueueView{Observation: observation, Channel: queue.row.ID}
	}
	r.operationElement = c.operationRequests.PushBack(r)
	c.enqueue(r)
}

// dropAbandonedOperations handles cancellation notices on the HTTP handover,
// not on the packet path. Notices coalesce: every HTTP event checks the bounded
// set, so a full handover already guarantees that departures will be observed.
// A queued request is removed immediately. A dispatched request still belongs
// to its reader and retains both credits until the final answer returns it.
func (c *Core) dropAbandonedOperations() {
	for e := c.operationRequests.Front(); e != nil; {
		next := e.Next()
		r := e.Value.(*storeRequest)
		if !r.cancelled && r.operation.Abandoned() {
			if _, sent := c.requests[r.op]; sent {
				r.cancelled = true
			} else {
				c.cancelPendingRequest(r)
				c.completeOperation(r)
			}
		}
		e = next
	}
}

func (c *Core) completeOperation(r *storeRequest) {
	if r.operationElement != nil {
		c.operationRequests.Remove(r.operationElement)
		r.operationElement = nil
	}
}

// captureOperation follows the reader's snapshot confirmation and supplies
// the authoritative live rows and client identities. Selection precedes row
// construction: only a bounded answer needs display strings or partitions.
// The reader contributes away sessions outside this capture, without gating
// writes while either side prepares the answer.
func (c *Core) captureOperation(r *storeRequest) {
	switch op := r.op.(type) {
	case *store.ReadSessionsView:
		op.Live = make(map[string]struct{}, len(c.sessions))
		clients := make([]string, 0, min(len(c.sessions), views.Limit))
		op.View.Connected = len(c.sessions)
		for client := range c.sessions {
			op.Live[client] = struct{}{}
			clients = views.Include(clients, client, strings.Compare)
		}
		op.View.Sessions = make([]*views.Session, 0, len(clients))
		for _, client := range clients {
			s := c.sessions[client]
			row := store.SessionView(&s.session, len(s.subscriptions), true)
			row.Listener = s.accept.Listener
			if s.accept.Remote != nil {
				row.Remote = s.accept.Remote.String()
			}
			filters := s.filters
			for j := range filters {
				sub := &filters[j]
				if sub.Partition.Count != 0 {
					row.Partitions = append(row.Partitions, sub.PartitionView())
				}
			}
			op.View.Sessions = append(op.View.Sessions, row)
		}
	case *store.ReadConsumersView:
		c.captureConsumers(op)
	case *store.ReadQueueView:
		op.View.Records = []views.Job{}
		q := c.channel(op.Channel).queue
		offsets := make([]int64, 0, min(len(q.attempts), views.Limit))
		for offset, attempt := range q.attempts {
			if !attempt.sent || attempt.complete {
				continue
			}
			offsets = views.Include(offsets, offset, cmp.Compare[int64])
		}
		// An attempt among the store's oldest 100 jobs must also be among
		// the oldest 100 active attempts. No other holder can be displayed.
		op.Live = make(map[int64]views.Job, len(offsets))
		for _, offset := range offsets {
			attempt := q.attempts[offset]
			holder := views.Text(attempt.member.conn.clientID)
			row := views.Job{State: "offered", Holder: &holder}
			if attempt.held {
				row.State, row.HeldUntil = "held", views.Time(attempt.heldUntil)
			}
			op.Live[offset] = row
		}
	}
}

func (c *Core) captureConsumers(op *store.ReadConsumersView) {
	op.Live = make(map[string]struct{}, len(c.sessions))
	op.ActiveRules = make(map[int64]struct{})
	selected := make([]*channelState, 0, min(len(c.channels), views.Limit))
	for _, ch := range c.channels {
		if ch.row.Type != store.Latest {
			op.View.Total++
			selected = views.Include(selected, ch, func(a, b *channelState) int {
				return strings.Compare(a.row.Name, b.row.Name)
			})
		}
	}
	op.View.Returned = len(selected)
	op.View.Channels = make([]views.Channel, len(selected))
	// Retain owner keys only during this capture. Nothing mutable is handed
	// to the reader, and at most 100 positions per selected channel are built.
	channels := make(map[int64]int, len(selected))
	positions := make([][]*consumerState, len(selected))
	for i, ch := range selected {
		channels[ch.row.ID] = i
		op.View.Channels[i] = views.Channel{ID: ch.row.ID, Channel: ch.row.Name, NextOffset: ch.row.Next}
		positions[i] = make([]*consumerState, 0, views.Limit)
	}
	for client, s := range c.sessions {
		op.Live[client] = struct{}{}
		for _, u := range s.consumers {
			if i, ok := channels[u.channel.row.ID]; ok {
				op.View.Channels[i].Consumers++
				positions[i] = views.Include(positions[i], u, compareOperationConsumers)
			}
		}
	}
	for _, b := range c.bridges {
		if b.link == nil {
			continue
		}
		for _, u := range b.link.consumers {
			if u.rule == nil {
				continue
			}
			op.ActiveRules[u.rule.row.ID] = struct{}{}
			if i, ok := channels[u.channel.row.ID]; ok {
				op.View.Channels[i].Consumers++
				positions[i] = views.Include(positions[i], u, compareOperationConsumers)
			}
		}
	}
	for i := range op.View.Channels {
		ch := &op.View.Channels[i]
		ch.Total, ch.Returned = ch.Consumers, len(positions[i])
		ch.Positions = make([]views.Position, 0, len(positions[i]))
		for _, u := range positions[i] {
			key := operationReaderKey(u)
			p := views.Position{Offset: u.position,
				Behind: ch.NextOffset - u.position, LastSeen: views.Millis(u.seen)}
			if key.Bridge {
				p.Reader, p.Rule = "bridge:"+key.Name, &views.Rule{Filter: key.Filter, Topic: key.Topic}
			} else {
				p.Reader = "mqtt:" + key.Name
			}
			ch.Positions = append(ch.Positions, p)
		}
	}
}

func operationReaderKey(u *consumerState) views.ReaderKey {
	if u.rule != nil {
		return views.ReaderKey{Name: u.conn.bridge.options.Bridge.Name, Bridge: true,
			Filter: u.rule.config.Filter, Topic: u.rule.config.Topic}
	}
	return views.ReaderKey{Name: u.conn.clientID}
}

func compareOperationConsumers(a, b *consumerState) int {
	// A channel has one captured next offset, so ascending offsets select
	// the same positions as descending lag without building display rows.
	if order := cmp.Compare(a.position, b.position); order != 0 {
		return order
	}
	return views.CompareReaders(operationReaderKey(a), operationReaderKey(b))
}

func (c *Core) answerOperation(r *storeRequest, err error) {
	c.completeOperation(r)
	if r.operation.Abandoned() {
		return
	}
	result := views.Result{Code: http.StatusOK}
	switch op := r.op.(type) {
	case *store.ReadSessionsView:
		result.Body = op.View
	case *store.ReadConsumersView:
		result.Body = op.View
	case *store.ReadQueueView:
		result.Body = op.View
	}
	if err != nil {
		result.Code, result.Body = http.StatusServiceUnavailable, map[string]string{"status": "unavailable"}
	}
	r.operation.Reply <- result
}

func (c *Core) bridgesView() views.Bridges {
	selected := make([]*bridgeState, 0, min(len(c.bridges), views.Limit))
	for _, b := range c.bridges {
		selected = views.Include(selected, b, func(a, b *bridgeState) int {
			if a.up != b.up {
				if a.up {
					return 1
				}
				return -1
			}
			if order := cmp.Compare(len(b.filters)-b.granted, len(a.filters)-a.granted); order != 0 {
				return order
			}
			return strings.Compare(a.options.Bridge.Name, b.options.Bridge.Name)
		})
	}
	result := views.Bridges{Total: len(c.bridges), Returned: len(selected), Bridges: make([]views.Bridge, 0, len(selected))}
	for _, b := range selected {
		row := views.Bridge{Name: b.options.Bridge.Name, Peer: b.options.Bridge.Peer, Connected: b.up,
			Since: views.Time(b.since), SubscriptionsGranted: b.granted, SubscriptionsPending: len(b.filters) - b.granted,
			UnansweredSubscribes: b.unanswered, LastSuback: views.Time(b.lastSuback), Sent: b.sent.Value(), Received: b.received.Value()}
		result.Bridges = append(result.Bridges, row)
	}
	return result
}
