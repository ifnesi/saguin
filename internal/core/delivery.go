package core

import (
	"cmp"
	"container/heap"
	"container/list"
	"container/ring"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/topic"
)

// consumerState keeps a scan boundary and only unsettled records. Skipped or
// settled gaps need no entries. A consumer has one read, one exchange save and
// one progress/settlement write, overlapping only for independent work. A
// combined write reserves both write places. Pages end in their answer event.
type consumerState struct {
	rule                 *bridgeRuleState // independent outbound rule cursor on this channel
	retry                *list.Element
	sharedHandoffs       deliveryBoundary // selected assignments not yet handed to a writer
	sharedCatchingUp     list.List        // only members waiting to qualify at this cursor
	sharedCatchup        *list.Element
	sharedWrite          *list.Element        // membership in the group's pending-writer wake list
	sharedRefusals       map[int64]*list.List // common cursor: members indexed by their last refused offset
	sharedRefusal        *list.Element
	sharedRefusalAt      int64
	sharedHeadSeen       int64
	sharedBudget         int           // member visit capacity, maintained by its own transitions
	sharedAvailable      int           // common cursor: sum of its members' visit capacities
	sharedBounds         consumerQueue // one unsaved-prefix minimum per member, on a group cursor
	sharedBoundIndex     int
	share                *sharedGroup   // a nil connection denotes the group's common scan
	sharedMember         *sharedMember  // membership lifetime, retained by its started exchanges
	sharedCursor         *consumerState // member delivery owner uses this group's channel cursor
	sharedBlocked        int64          // current group record this member cannot receive
	sharedTerminalBefore int64          // upper bound of group terminal evidence awaiting progress reclamation
	service              bool           // direct service publications never scan a channel
	admission            consumerAdmission
	conn                 *connectionState
	channel              *channelState
	queue                *consumerQueue
	index                int
	preparedIndex        int  // one-based index in the connection's selected-owner heap
	indexed              bool // membership in the channel's subscription/consumer index
	registered           bool // channel ownership survives detached outstanding exchanges
	registration         uint64
	ticket               uint64
	position, scan       int64
	available            int64 // committed boundary through the latest matching wake
	request              *storeRequest
	writing              *storeRequest            // progress and settlements; may also own exchange saves
	saving               *storeRequest            // exchange saves, independent of an earlier progress-only write
	read                 storeRequest             // reused only after its final answer
	metadata             store.ReadRecordMetadata // Rows is released in the answer event
	recovery             store.ReadSessionDeliveries
	last                 *outgoing
	removals             deliveryBoundary    // invalidated preparations ready for deterministic settlement
	boundaries           [2]deliveryBoundary // all live exchanges, then those not yet stored
	reserve              *outgoing           // first prepared entry not yet admitted to payload memory
	load                 *outgoing           // first entry not yet included in a payload read
	offer                *outgoing           // first selected entry not yet handed to the writer
	reserved             int                 // granted entries ready for the next bounded payload read
	pendingSaves         list.List           // new exchanges and PUBREC phases, including store-owned changes
	blockedSaves         deliveryBoundary    // preparations waiting for earlier cursor progress
	qos0Pending          *ring.Ring          // unsettled QoS 0 obligations; nodes belong to fixed delivery slots
	persisted            int64               // last successfully saved position
	seen                 int64               // time of the latest core-held position movement
	retentionMissed      uint64
	dirty                bool
	refusedSave          bool                   // wait for the design's full-store retry deadline
	settled              map[uint16]deliveryEnd // acknowledgements and cancelled preparations awaiting deletion
	rewind               int64                  // earliest cancelled live preparation, until its deletion commits
	live                 map[uint16]struct{}    // surviving live exchanges released at departure; bounded by the window
	markersBefore        int64                  // upper bound of committed inherited scan markers
	settledThrough       int64                  // furthest settlement; bounded by scan when advancing
	recovered            map[int64]struct{}     // recovered records already represented by exchanges
	filters              []store.Subscription   // contributing filters in complete filter-byte order
	held                 bool                   // every filter is wholly denied, or an owed record is permission-held
	pass                 *retainedPass
}

type deliveryStage uint8

const (
	deliveryPrepared deliveryStage = iota // metadata and identifier only; no message memory
	deliveryWaiting
	deliveryReserved
	deliveryLoading
	deliveryLoaded // payload returned; first transmission still awaits exchange commit
	deliveryWriting
	deliveryAcknowledgement
	deliveryReleaseReady // committed PUBREC, waiting for packet-ordered reply emission
	deliveryReleasing    // PUBREL owned by the writer
	deliveryCompletion   // waiting for PUBCOMP
	deliveryRecovered    // core-owned recovery state, not yet using send quota or message memory
)

type receiptState uint8

const (
	receiptNone receiptState = iota
	receiptAccepted
	receiptRefused // phase save was lost; only reconnect can recover the stored phase
)

// outgoing has no payload at rest. The store owns a loading body's buffer;
// the writer owns a handed-off packet. Its queue links and reservation remain
// core-owned, independently of those transfers. Live preparation and sending
// have separate allowances, backed by twice the send window. Recovered PUBRELs
// have a separate fixed allowance, independent of PUBLISH quota.
type outgoing struct {
	skipCause        string
	peerTopic        string
	topicHeld        bool
	attempt          *queueAttempt
	sharedSave       bool
	sharedReturned   bool
	sharedRefused    bool           // writer could not encode this member's assignment; return it to the group
	sharedPrevious   string         // round-robin predecessor until the writer accepts this selection
	sharedStored     bool           // compact group assignment, independently of a session exchange
	answer           *serviceAnswer // body loader and terminal request accounting
	packet           connection.Delivery
	payload          store.ReadRecordPayload // store-owned from read dispatch until its final answer
	payloadBody      []byte                  // read result; never points into the core-owned packet
	missing          bool                    // applied read result, owned by the core
	consumer         *consumerState
	prev, next       *outgoing
	offset           int64
	saveAfter        int64  // cursor boundary required before or with this preparation's commit
	order            uint64 // connection-wide original selection order, including QoS 0
	bytes            int64
	size             int64 // packet and stored-copy reservation; zero when recovery must discard
	storedBytes      int64 // temporary S2 source, returned with the payload read
	memory           memoryRequest
	stage            deliveryStage
	exchange         store.SessionDelivery // exchange scalars, no payload; phase changes only on commit
	pendingSave      *list.Element         // membership in the consumer's pending exchange changes
	qos0Pending      ring.Ring             // reusable node in its consumer's unsettled QoS 0 obligations
	qos0Topic        []byte                // immutable topic view, independent of the writer-owned packet
	receiptReply     *list.Element         // accepted PUBREC's packet-ordered reply
	receiptRepeats   []*list.Element       // repeated PUBRECs waiting for the same phase commit
	resumedRelease   *list.Element         // recovered control packet awaiting ordered hand-off
	release          connection.Release    // writer-owned only at deliveryReleasing
	saved            bool
	saving           bool                 // store owns this exchange's save or deletion
	counted          bool                 // contributes to the core's in-flight gauge
	preparing        bool                 // owns preparation capacity until hand-off or cancellation
	payloadSlot      bool                 // holds future send capacity while reserving or loading a payload
	quota            bool                 // PUBLISH send quota, released by its terminal acknowledgement
	acknowledged     bool                 // terminal acknowledgement may precede the writer's report
	receipt          receiptState         // accepted PUBREC, or the terminal refusal of its phase save
	attempted        bool                 // handed off here, or recovered as sent/possibly sent
	accessRevoked    bool                 // never hand off; cancel when store/read ownership returns
	permissionHeld   bool                 // cancellation preserves the position for a wholly denied filter
	removed          bool                 // deletion or replacement invalidated this pending hand-off
	settled          bool                 // settlement or cancellation recorded while a reader or writer still owned the packet
	initial          bool                 // retained-pass delivery never moves the live position
	preparationIndex int                  // one-based position in the channel offset's payload index
	boundary         [5]int               // position, unsaved, group hand-off, removal and save dependency indexes
	filters          []store.Subscription // immutable selection options, shared with the consumer
	pass             *retainedPass        // initial selection's immutable filter ranges
	local            bool                 // the record's publisher is this client
}

// payloadBatch hands the store an immutable list of reserved window entries,
// just as a publication batch hands it its members. Results remain separate
// from the core-owned packets until the final answer returns ownership.
type payloadBatch struct {
	store.ReadRecordPayloads
	members []*outgoing
}

// syncConsumer applies subscription changes without moving held positions
// or broadcast live-start boundaries. Selected deliveries keep their first-delivery
// properties; reads not yet applied use the new subscription revision.
func (c *Core) syncConsumer(s *connectionState, now time.Time) (reach *store.SaveSubscriptions) {
	// CONNECT reuses this subscription compilation before its save, so kept
	// filters can gain positions and live boundaries under changed routing.
	// Ordinary subscription changes already saved those rows themselves.
	var admission subscriptionAdmission
	s.subscriptionRevision++
	for _, u := range s.consumers {
		c.indexConsumer(u, false)
	}
	var heldSubscriptions int64
	oldFilters := s.filters
	s.filters = slices.Grow(s.filters[:0], len(s.subscriptions))
	for filter := range s.subscriptions {
		sub := s.subscriptions[filter]
		if group, _, _ := topic.SplitShared(filter); group != "" {
			continue
		}
		c.refreshSubscriptionAccess(s, &sub)
		if sub.ReadAccess == acl.None {
			heldSubscriptions++
		}
		if _, service := c.routing.SeekChannel(sub.Filter, true); service || strings.HasPrefix(sub.Filter, "$saguin/queue") {
			sub.Channels = []int64{}
		}
		if sub.Channels == nil {
			for _, index := range c.routing.Reaches(sub.Filter) {
				ch := &c.broadcast
				if index >= 0 {
					ch = c.routed[index]
				}
				sub.Channels = append(sub.Channels, ch.row.ID)
			}
			slices.Sort(sub.Channels)
		}
		if s.phase == connecting {
			before := len(sub.Starts)
			for _, id := range sub.Channels {
				ch := c.channel(id)
				if ch == nil {
					continue
				}
				fresh := s.consumer(id) == nil
				if id == 1 || ch.row.Type == store.Latest {
					fresh = !slices.ContainsFunc(sub.Starts, func(start store.SubscriptionStart) bool { return start.Channel == id })
				}
				if fresh {
					if admission.op == nil {
						admission.op = &store.SaveSubscriptions{Session: s.session.ID, ReachOnly: true}
						admission.channels = make(map[int64]int, len(s.consumers))
						for _, u := range s.consumers {
							admission.channels[u.channel.row.ID] = 1
						}
					}
					c.admitSubscription(&admission, &sub, store.SubscriptionStart{Channel: id, Start: s.attempt.starts[id]}, true)
				}
			}
			if len(sub.Starts) != before {
				update := sub
				update.Starts = sub.Starts[before:]
				admission.op.Updates = append(admission.op.Updates, store.SubscriptionUpdate{Subscription: update})
			}
		}
		s.subscriptions[filter] = sub
		s.filters = append(s.filters, sub)
	}
	if len(s.filters) < len(oldFilters) {
		clear(oldFilters[len(s.filters):])
	}
	slices.SortFunc(s.filters, func(a, b store.Subscription) int { return cmp.Compare(a.Filter, b.Filter) })
	if admission.op != nil {
		c.applySubscriptionPositions(s, admission.op.Positions)
	}
	reached := make(map[int64][]store.Subscription)
	for i := range s.filters {
		sub := &s.filters[i]
		for _, id := range sub.Channels {
			if ch := c.channel(id); ch != nil {
				reached[id] = append(reached[id], *sub)
			}
		}
	}
	for _, ch := range c.channels {
		filters := reached[ch.row.ID]
		u := s.consumer(ch.row.ID)
		if len(filters) == 0 {
			if u != nil && len(u.filters) != 0 {
				c.endSubscriptionReach(s, u, now)
			}
			continue
		}
		if u == nil {
			position := ch.row.Floor
			if ch.row.ID == 1 || ch.row.Type == store.Latest || ch.startTail {
				position = ch.row.Next
			}
			u = c.newConsumer(s, ch, position)
			s.addConsumer(u)
		}
		// CONNECT observes every position after its save succeeds. SUBSCRIBE
		// observes an existing dormant position when it becomes reachable.
		if s.phase == connected && len(u.filters) == 0 {
			c.observePositionFloor(u, now)
		}
		u.available = max(u.available, ch.committedTail)
		u.filters = filters
		u.held = true
		for i := range filters {
			if filters[i].ReadAccess != acl.None {
				u.held = false
				break
			}
		}
		u.reconcileRetained()
		c.initWindow(s)
		c.indexConsumer(u, true)
	}
	if admission.op != nil && (len(admission.op.Updates) != 0 || len(admission.op.Positions) != 0) {
		reach = admission.op
		slices.SortFunc(reach.Updates, func(a, b store.SubscriptionUpdate) int { return cmp.Compare(a.Filter, b.Filter) })
		slices.SortFunc(reach.Positions, func(a, b store.SubscriptionStart) int { return cmp.Compare(a.Channel, b.Channel) })
		s.attempt.pass = newRetainedPass(admission.retained)
	}
	c.setHeld(s, heldSubscriptions)
	c.queueConsumers(s)
	return reach
}

func (c *Core) initWindow(s *connectionState) {
	if s.window != nil {
		return
	}
	n := 2 * c.deliveryWindow(s)
	if s.resuming != nil {
		s.freeReleases = c.deliveryWindow(s)
	}
	s.window = make([]outgoing, n+s.freeReleases)
	if s.stored {
		s.qos0Topics = make([]*outgoing, 0, n)
	}
	s.freeSlots = n
	if c.admission.delay != 0 {
		s.peerObligations.reserve(len(s.window))
		s.peerWindow = make([]coreDeadline, len(s.window))
	}
	for i := len(s.window) - 1; i >= 0; i-- {
		d := &s.window[i]
		d.packet.Slot = uint32(i)
		if i >= n {
			d.next, s.releaseFree = s.releaseFree, d
		} else {
			d.next, s.free = s.free, d
		}
	}
	s.freeIdentifier = 1
	s.findFreeIdentifier()
}

func (c *Core) deliveryWindow(s *connectionState) int {
	n := c.limits.MaxInflight
	if s.bridge != nil {
		n = s.bridge.options.Bridge.ReceiveMaximum
	}
	if maximum := int(s.session.ReceiveMaximum); s.version == packet.V5 && maximum != 0 {
		n = min(n, maximum)
	}
	return n
}

// preparationRoom bounds selection independently of send quota. One common
// descriptor is reserved for direct answers, including while a recovery read
// owns ordinary credits. No answer borrows another preparation's allowance.
func (c *Core) preparationRoom(u *consumerState) int {
	s := u.conn
	var answer *outgoing
	if s.service != nil {
		answer = s.service.last
	}
	if u.service {
		if answer != nil {
			return 0 // one active answer, serviced by the same event-ordered queues
		}
		return min(1, s.freeSlots)
	}
	room := s.freeSlots
	if answer == nil && s.bridge == nil {
		room-- // keep the answer's descriptor inside the existing twice-window bound
	}
	return min(c.deliveryWindow(s)-s.publishPreparations(), room, 65535-len(s.identifiers))
}

// publishPreparations excludes the one direct answer from ordinary look-ahead
// and recovery. Its descriptor still counts against the common fixed window.
func (s *connectionState) publishPreparations() int {
	n := s.preparing
	if s.service != nil && s.service.last != nil && s.service.last.preparing {
		n--
	}
	return n
}

// retainedRoom shares one window among selected, loading, saving and sent
// entries, including earlier deliveries whose consumer is now detached.
// Only live scans have a separate preparation allowance.
func (c *Core) retainedRoom(u *consumerState) int {
	s := u.conn
	return min(c.preparationRoom(u), c.deliveryWindow(s)-(2*c.deliveryWindow(s)-s.freeSlots))
}

func (u *consumerState) offerReady() bool {
	d := u.offer
	return d != nil && d.stage == deliveryLoaded && !d.saving && (!d.exchange.Reset || d.saved) &&
		(!u.conn.stored || d.exchange.QoS == 0 || d.saved)
}

func (c *Core) canOffer(u *consumerState) bool {
	return u.offerReady() && !u.offer.accessRevoked && c.offerTurn(u) &&
		(u.service || (u.conn.pendingSubscriptions == 0 && !u.channel.retaining))
}

// offerTurn checks the order and peer-dependent capacity shared by hand-off
// and admission. Owning a payload slot does not let an owner pass an earlier
// preparation that still needs send capacity.
func (c *Core) offerTurn(u *consumerState) bool {
	return u.service || (u.conn.prepared.peek() == u && u.conn.resumeTurn(u.offer) &&
		(u.offer.missing || u.conn.inflight < c.deliveryWindow(u.conn)))
}

func (c *Core) payloadRoom(s *connectionState) bool {
	return s.inflight+s.payloadSlots < c.deliveryWindow(s) && c.serviceRoom(s)
}

func (c *Core) canReservePayload(u *consumerState) bool {
	if u.service {
		if u.reserve == nil || u.reserve.stage != deliveryPrepared || !c.serviceRoom(u.conn) {
			return false
		}
		// Share send capacity in selection order with an ordinary owner
		// that can advance. Peer-blocked preparations cannot hold an
		// answer, and a continuing answer stream cannot starve older work.
		ordinary := u.conn.prepared.peek()
		return ordinary == nil || !c.canReservePayload(ordinary) || u.reserve.order < ordinary.reserve.order
	}
	return u.conn.pendingSubscriptions == 0 && u.reserve != nil && !u.reserve.accessRevoked && u.reserve.stage == deliveryPrepared &&
		u.reserve.topicReady() && u.conn.prepared.reserveTurn(u.reserve) && u.conn.resumeTurn(u.reserve) && c.payloadRoom(u.conn)
}

// payloadReady excludes a revoked reservation awaiting its exchange save.
// That save's answer completes cancellation; no payload read need begin.
func (u *consumerState) payloadReady() bool {
	return u.reserved != 0 && u.load != nil && !u.load.accessRevoked
}

func (c *Core) consumerActive(s *connectionState, consumer *consumerState) bool {
	if s == nil {
		return c.failure == nil && c.phase < quiescing && consumer.share.active
	}
	// Subscription changes maintain indexed membership on the active owner.
	// Detached and recovered exchange owners have no such membership, so
	// checking it needs neither a connection lookup nor a channel search.
	return c.failure == nil && c.phase < quiescing && s.phase == connected && !s.end.seen &&
		(consumer.indexed || consumer.share != nil) && !consumer.held &&
		(consumer.sharedCursor == nil || consumer.sharedBlocked != consumer.sharedHead())
}

// scheduleDeliveries services event-ordered FIFO queues, with connection-key
// ties within an event. Loads need no metadata-page credit. Exhausted page
// credit revisits sharing only when the ready membership changes.
func (c *Core) scheduleDeliveries(acts []Action, now time.Time) []Action {
	if c.failure != nil {
		return acts
	}
	defer func() { c.deliveryReadyChanged = false }()
	for {
		u := c.deliveryLoads.peek()
		if ready := c.deliveryChanges.peek(); ready != nil && (u == nil || consumerBefore(ready, u)) {
			u = ready
		}
		if c.phase < quiescing && (c.metadataReads < c.metadataLimit || c.deliveryReadyChanged) {
			if ready := c.deliveryReady.peek(); ready != nil && (u == nil || consumerBefore(ready, u)) {
				u = ready
			}
		}
		if u == nil {
			return acts
		}
		heap.Pop(u.queue)
		if u.conn == nil {
			if !c.scheduleShared(u) {
				if u.queue == nil {
					heap.Push(&c.deliveryDeferred, u)
				}
			} else {
				c.queueConsumer(u)
			}
			continue
		}
		s := u.conn
		switch {
		case u.removals.Len() != 0:
			d := heap.Pop(&u.removals).(*outgoing)
			c.settleDelivery(s, d, now)
			c.completeResume(s)
		case u.needsWrite():
			c.saveDeliveryChanges(u)
		case c.phase >= quiescing:
		case s.resuming == u && c.canResumeRelease(s):
			acts = c.offerResumedReleases(acts, s)
			c.queueConsumers(s)
		case u.service && len(s.answers) != 0 && c.preparationRoom(u) > 0:
			acts = c.admitServiceAnswers(acts, s)
		case c.canOffer(u):
			acts = c.offerLoadedDeliveries(acts, u)
			c.queueConsumers(s)
		case c.canReservePayload(u):
			c.reservePayloads(u)
			acts = c.giveBytes(acts, now)
		case u.payloadReady():
			if u.service {
				acts = c.loadServiceAnswers(acts, u, now)
			} else {
				c.loadDeliveries(u)
			}
		case len(s.resume) != 0 && s.resuming == u:
			acts = c.offerResumedDeliveries(acts, u, now)
			acts = c.giveBytes(acts, now)
			c.queueConsumer(u)
			c.queueConsumers(s)
		case s.resumeAfter < s.resumeThrough || s.releasedThrough < s.resumeThrough:
			if c.metadataReads >= c.metadataLimit {
				heap.Push(&c.deliveryDeferred, u)
				continue
			}
			u.recovery = c.resumeRead(u)
			s.recoveryCapacity(&u.recovery, -1)
			c.readConsumer(u, readResumedDeliveries, &u.recovery)
			c.metadataReads++
		default:
			if !c.readMetadataPage(u) {
				continue
			}
		}
		c.queueConsumer(u)
	}
}

// readConsumer reuses the owner's request only after the previous final answer
// has returned ownership. View confirmation alone leaves request occupied.
func (c *Core) readConsumer(u *consumerState, kind requestKind, op store.Read) {
	r := &u.read
	r.kind, r.op, r.revision = kind, op, u.conn.subscriptionRevision
	u.request = r
	u.conn.deliveryRequests++
	c.enqueue(r)
}

// loadDeliveries joins a granted prefix to its channel's collecting read.
// No payload is loaded for an entry still waiting in the memory FIFO. The
// batch membership survives subscription changes and earlier writer reports.
func (c *Core) loadDeliveries(u *consumerState) {
	r := u.channel.loading
	for d := u.load; d != nil; d = u.load {
		if d.stage != deliveryReserved || d.accessRevoked {
			break
		}
		if r == nil {
			r = c.newPayloadRead(u)
		}
		// Whole FIFO grants make the reserved entries a contiguous prefix.
		// The cursor avoids revisiting packets already owned by the writer.
		d.payload = store.ReadRecordPayload{Channel: u.channel.row.ID, Offset: d.offset, Into: &d.payloadBody}
		if !r.payloads.Add(&d.payload) {
			if u.request == nil {
				// This consumer has not joined the full earlier batch yet.
				r = c.newPayloadRead(u)
				continue
			}
			break
		}
		if u.request == nil {
			c.joinConsumerRead(r, u)
		}
		u.load = nextPublish(d.next)
		d.stage = deliveryLoading
		d.indexPreparation()
		r.payloads.members = append(r.payloads.members, d)
		u.reserved--
	}
}

// subscriptionGrant combines the eligible filters for one copy of a record.
type subscriptionGrant struct {
	topicInvalid                   bool
	topic                          string
	attempt                        string
	matched, retain, channel, held bool
	qos                            byte
	ids                            []uint32
}

// deliveryGrant selects only eligible subscriptions. Filter order fixes the
// Subscription Identifiers; all contributors supply QoS and RETAIN options.
func (c *Core) deliveryGrant(u *consumerState, m *store.RecordMetadata, pass *retainedPass) subscriptionGrant {
	if u.rule != nil {
		if m.Bridge != 0 {
			return subscriptionGrant{}
		}
		name, matched, valid := c.bridgeTopic(u, m.Topic)
		if !matched {
			return subscriptionGrant{}
		}
		retain := u.channel.row.ID == 1 && m.PublishedRetained
		return subscriptionGrant{matched: true, topic: name,
			topicInvalid: !valid,
			qos:          u.conn.bridge.deliveryQoS(min(m.QoS, u.rule.config.QoS), retain),
			retain:       retain}
	}
	if u.share != nil && u.share.queue != nil {
		return workerGrant(u.sharedMember)
	}
	var matched, retain, channel, held bool
	var ids []uint32
	var grant byte
	match := store.SubscriptionMatch{Topic: m.Topic, Offset: m.Offset, Channel: u.channel.row.ID, Type: u.channel.row.Type, Local: m.Publisher == u.conn.clientID}
	n := len(u.filters)
	if pass != nil {
		n = len(pass.filters)
	}
	for i := range n {
		var sub *store.Subscription
		eligible := true
		if pass != nil {
			filter := &pass.filters[i]
			sub = &filter.Subscription
			eligible = filter.from <= m.Offset && m.Offset < filter.before
		} else {
			sub = &u.filters[i]
		}
		if !eligible || !match.Eligible(sub, pass != nil) {
			continue
		}
		if sub.ReadAccess == acl.None {
			held = true
			continue
		}
		if sub.ReadAccess == acl.Partial {
			name := ""
			if u.channel.row.ID != 1 {
				name = u.channel.row.Name
			}
			if !u.conn.access.Allows(acl.Read, name, string(m.Topic)) {
				continue
			}
		}
		matched, grant = true, max(grant, sub.QoS)
		channel = channel || len(sub.Channels) > 1
		retain = retain || (u.channel.row.ID == 1 && (pass != nil || (m.PublishedRetained && sub.RetainAsPublished)))
		// design.md, SUBSCRIBE, Options: a latest channel's initial pass to a
		// client carries RETAIN 1; a bridge rule's deliveries keep RETAIN 0.
		retain = retain || (pass != nil && u.channel.row.Type == store.Latest && u.rule == nil)
		if sub.Identifier != 0 {
			ids = append(ids, sub.Identifier)
		}
	}
	return subscriptionGrant{matched: matched, qos: min(grant, m.QoS), retain: retain, ids: ids, channel: channel, held: held}
}

func (c *Core) selectMetadata(acts []Action, u *consumerState, op *store.ReadRecordMetadata, rows []store.RecordMetadata, now time.Time) []Action {
	s := u.conn
	pass := u.pass
	if pass != nil {
		pass.waiting = false
	}
	through := min(op.Before, u.read.tail)
	exhausted := true
	selected := 0
	for i := range rows {
		m := &rows[i]
		from := u.scan
		if pass != nil {
			from = pass.scan
		}
		if m.Offset < from {
			continue
		}
		if m.Offset >= through {
			break
		}
		if pass == nil && m.Settled {
			u.scan = m.Offset + 1
			continue
		}
		if (pass == nil && c.preparationRoom(u) <= 0) || (len(s.consumers) > 1 && selected != 0) {
			exhausted = false
			break // retain neither the page nor an unreserved payload for this share
		}
		if pass != nil {
			pass.scan = m.Offset + 1
		} else {
			u.scan = m.Offset + 1
		}
		if pass == nil && m.Offset < u.channel.row.Floor {
			continue // retention alone accounts for a passed record
		}
		if _, represented := u.recovered[m.Offset]; represented && pass == nil {
			continue
		}
		grant := c.deliveryGrant(u, m, pass)
		if !grant.matched && grant.held {
			if pass != nil {
				pass.scan = m.Offset
			} else {
				u.scan = m.Offset
			}
			u.held, exhausted = true, false
			break
		}
		if !grant.matched {
			continue
		}
		if pass != nil && c.retainedRoom(u) <= 0 {
			pass.scan, pass.waiting = m.Offset, true
			exhausted = false
			break // retry current retained eligibility when send capacity returns
		}
		if u.rule != nil {
			if grant.topicInvalid {
				acts = c.bridgeUnsent(acts, u, m.Offset, "topic_invalid", now)
				continue
			}
			if grant.retain && !s.bridge.peer.RetainAvailable {
				grant.retain = false
				acts = c.warnBridgeRetain(acts, s.bridge, now)
			}
		}
		selected++
		p, reservation, encodable, err := c.deliveryPacket(u, m, grant)
		if err != nil {
			return c.fail(acts, err, now)
		}
		if !encodable {
			acts = c.missedDelivery(acts, u, m.Offset, now)
			continue // an impossible reservation never enters the FIFO
		}
		d := c.selectDelivery(u, &p, m.Offset, reservation, pass != nil)
		d.peerTopic = grant.topic
		d.storedBytes = int64(m.StoredPayloadSize)
		d.filters, d.pass, d.local = u.filters, pass, m.Publisher == s.clientID
		if grant.qos != 0 {
			c.assignDelivery(s, d)
			if s.stored {
				d.exchange.Properties = p.Additions
				if d.exchange.Properties == nil {
					d.exchange.Properties = packet.SubscriptionProperties(grant.ids)
				}
			}
		}
		u.trackDelivery(d)
	}
	if exhausted && (len(rows) < op.Limit || (len(rows) != 0 && rows[len(rows)-1].Offset >= through)) {
		if pass != nil {
			pass.scan = through
			if through == pass.before {
				u.pass = nil
			}
		} else {
			u.scan = through // include offset gaps, which are not records owed
		}
	}
	if selected != 0 || (u.pass == nil && u.scan >= u.available) {
		s.scanChannel = u.channel.row.ID
	}
	c.advanceConsumer(u, now)
	return acts
}

// deliveryPacket compiles delivery properties and the conservative memory
// reservation. Only the writer knows the encoding's actual Topic Alias size.
func (c *Core) deliveryPacket(u *consumerState, m *store.RecordMetadata, grant subscriptionGrant) (p connection.Delivery, reservation int64, encodable bool, err error) {
	s := u.conn
	p = connection.Delivery{Channel: u.channel.row.ID, Topic: m.Topic, Properties: m.Properties, Worker: u.share != nil && u.share.queue != nil,
		SubscriptionIDs: grant.ids, QoS: grant.qos, Retain: grant.retain, Bridge: u.rule != nil}
	if u.rule != nil && u.rule.config.Direction != config.Both {
		p.Topic = []byte(grant.topic)
	}
	if p.QoS != 0 {
		p.PacketID = uint16(s.freeIdentifier)
	}
	if m.Expires.Valid {
		p.Expires = m.Expires.Int64
	}
	if u.rule != nil {
		if err := p.SetBridgeProperties(m.ID); err != nil {
			return p, 0, false, err
		}
	} else if s.version == packet.V5 && u.channel.row.ID != 1 {
		name := ""
		if grant.channel {
			name = u.channel.row.Name
		}
		var worker *packet.WorkerProperties
		if p.Worker {
			worker = &packet.WorkerProperties{Attempt: grant.attempt, Number: m.Attempts + 1, ResponseTopic: u.share.queue.responseTopic}
		}
		if err := p.SetChannelProperties(m.ID, m.Offset, m.Time, name, worker); err != nil {
			return p, 0, false, err
		}
	}
	var size int
	size, encodable, err = deliverySize(s, &p, m.PayloadSize)
	reservation = int64(size) + int64(m.StoredPayloadSize)
	return p, reservation, encodable && reservation <= int64(c.limits.MaxOutgoingMemory), err
}

// deliverySize counts the full topic and an alias property, bounding every
// writer choice without copying properties or allocating a packet header.
// Structural damage is an error; lengths the protocol cannot encode are an
// ordinary recipient discard, handled before any payload reservation.
func deliverySize(s *connectionState, p *connection.Delivery, payloadSize int) (size int, encodable bool, err error) {
	var alias uint16
	if s.topicAliases != 0 {
		alias = 1
	}
	publication, err := p.Publish(p.Topic, alias)
	if err != nil {
		return 0, false, err
	}
	size, err = packet.PublishSize(s.version, &publication, payloadSize)
	return size, err == nil, nil // packet fields and stored structure have already been checked
}

// minimumDeliverySize rules out recipients that cannot fit even the smallest
// legal alias encoding. It never discards a record: the writer still decides
// actual encoding, including whether this topic already has an alias.
func minimumDeliverySize(s *connectionState, p *connection.Delivery, payloadSize int) (int, error) {
	publication, err := p.Publish(p.Topic, 0)
	if err != nil {
		return 0, err
	}
	size, layoutErr := packet.PublishSize(s.version, &publication, payloadSize)
	if s.topicAliases != 0 {
		publication.Topic, publication.TopicAlias = nil, 1
		aliased, err := packet.PublishSize(s.version, &publication, payloadSize)
		if err == nil && (layoutErr != nil || aliased < size) {
			size, layoutErr = aliased, nil
		}
	}
	return size, layoutErr
}

// selectDelivery gives a selected record one preparation slot, without payload memory.
// New sends and recovered exchanges share all subsequent ownership transfers.
func (c *Core) selectDelivery(u *consumerState, p *connection.Delivery, offset, size int64, initial bool) *outgoing {
	d := c.takeDelivery(u, p, offset, size, initial, false)
	c.prepareDelivery(d)
	return d
}

// takeDelivery installs state in a fixed descriptor, including recovered
// exchanges not yet offered. Retention and group ownership can use these
// same core-owned entries after their former durable rows have been removed.
func (c *Core) takeDelivery(u *consumerState, p *connection.Delivery, offset, size int64, initial, resumedRelease bool) *outgoing {
	s := u.conn
	// MQTT 3.1.1 sets RETAIN only for an initial pass, including resends.
	if s.version == packet.V311 && !initial {
		p.Retain = false
	}
	d := s.free
	switch {
	case resumedRelease:
		d = s.releaseFree
		s.releaseFree = d.next
		s.freeReleases--
	default:
		s.free = d.next
		s.freeSlots--
	}
	s.selectionOrder++
	p.Slot = d.packet.Slot
	*d = outgoing{packet: *p, consumer: u, offset: offset, order: s.selectionOrder,
		size: size, prev: u.last, stage: deliveryRecovered, initial: initial}
	if u.last != nil {
		u.last.next = d
	}
	u.last = d
	if !u.registered && !u.service {
		indexConsumerOwner(u)
	}
	return d
}

// prepareDelivery starts bounded exchange preparation, without message memory.
// Recovered metadata alone uses neither preparation nor message memory.
func (c *Core) prepareDelivery(d *outgoing) {
	u := d.consumer
	d.stage = deliveryPrepared
	d.preparing = true
	u.conn.preparing++
	if u.reserve == nil {
		u.reserve = d
	}
	if u.load == nil {
		u.load = d
	}
	if u.offer == nil {
		u.nextOffer(d)
	}
}

// nextPublish advances preparation cursors past exchanges that only owe a
// control packet. Their position guards remain linked until settlement, but
// neither payload loading nor the PUBLISH offer order can wait on them.
func nextPublish(d *outgoing) *outgoing {
	for d != nil && d.exchange.Released {
		d = d.next
	}
	return d
}

// reservePayloads admits only sendable entries to the memory FIFO. Requests,
// grants and reads hold future send capacity until hand-off transfers it to
// send quota. Metadata-only preparation cannot hold up a publisher's memory.
func (c *Core) reservePayloads(u *consumerState) {
	s := u.conn
	for c.canReservePayload(u) {
		d := u.reserve
		u.reserve = nextPublish(d.next)
		d.stage, d.payloadSlot = deliveryWaiting, true
		if u.rule != nil {
			if s.bridgeTopics == nil {
				s.bridgeTopics = make(map[string]*outgoing, c.deliveryWindow(s))
			}
			s.bridgeTopics[d.peerTopic], d.topicHeld = d, true
		}
		if u.service {
			s.serviceSlots++
		} else {
			s.payloadSlots++
		}
		d.memory.conn, d.memory.delivery = s, d
		c.queueMemory(&d.memory, d.size)
	}
	// Only this prepared head can reserve or offer. Reservation changes no
	// other owner's scheduling eligibility. Exhausting payload capacity does
	// pause admission clocks of other channels whose scans have no offer yet.
	if c.admission.delay != 0 && !c.payloadRoom(s) {
		for _, owner := range s.consumers {
			c.touchAdmission(owner)
		}
	}
}

func (c *Core) advanceConsumer(u *consumerState, now time.Time) {
	defer c.queueConsumer(u)
	if u.sharedCursor != nil {
		u.scan = u.sharedCursor.scan
		return
	}
	// Recovery has already examined these records. Consume their contiguous
	// prefix without waiting for a metadata read: a recovered PUBACK can be
	// followed immediately by departure, which must save its settlement.
	// Stop at an unknown record so unsaved progress before a crash can replay.
	for {
		if _, represented := u.recovered[u.scan]; !represented {
			break
		}
		u.scan++
	}
	position, storedPrefix := u.scan, u.scan
	if q := &u.boundaries[0]; q.Len() != 0 {
		position = min(position, q.items[0].offset)
	}
	if !u.conn.stored {
		storedPrefix = position
	} else if q := &u.boundaries[1]; q.Len() != 0 {
		storedPrefix = min(storedPrefix, q.items[0].offset)
	}
	// Ordinary progress follows the contiguous settled prefix. A settled
	// durable exchange may require passing earlier stored exchanges so its
	// deletion can commit with a position beyond its own record. No metadata
	// scan alone moves progress past an unacknowledged delivery.
	position = max(u.position, position, min(u.settledThrough, storedPrefix))
	if position != u.position {
		u.position, u.seen = position, now.UnixMilli()
		u.dirty = (u.conn.stored || u.markersBefore != 0) && u.ownsPosition()
	}
}

func (c *Core) readDeliveryPayload(acts []Action, r *storeRequest, now time.Time) []Action {
	owners := c.finishConsumerRead(r)
	for _, d := range r.payloads.members {
		d.applyPayload()
		u, s := d.consumer, d.consumer.conn
		if d.answer == nil {
			// A channel record has a fixed payload size. A current-value
			// answer can instead use its old source allowance for a larger
			// decoded replacement; settle that whole grant after sizing it.
			c.returnMemory(1, d.storedBytes, now)
			d.bytes -= d.storedBytes
			d.storedBytes = 0
		}
		active := c.failure == nil && c.phase < quiescing && s.phase == connected &&
			!s.end.seen && c.connections[s.conn] == s
		d.stage = deliveryLoaded
		if active && d.answer != nil && d.answer.body != nil {
			var retry bool
			acts, retry = c.loadedServiceValue(acts, d, now)
			if retry {
				continue
			}
			active = c.failure == nil
		}
		switch {
		case active && d.accessRevoked:
			c.releaseDelivery(s, d, now)
			c.advanceConsumer(u, now)
		case active && d.removed:
			c.removePreparation(d)
		case active:
			acts = c.offerLoadedDeliveries(acts, u)
		case !d.saving:
			c.releaseDelivery(s, d, now)
		}
	}
	clear(r.payloads.members)
	r.payloads = payloadBatch{members: r.payloads.members[:0]}
	if r.consumer.channel.loading == r {
		r.consumer.channel.loading = nil
	}
	for _, u := range owners {
		c.completeResume(u.conn)
		c.queueConsumer(u)
		c.queueConsumers(u.conn)
	}
	return acts
}

// applyPayload runs only once the body loader has returned ownership. The
// core and writer see the result through the packet, never through a read
// operation that can still belong to a store reader.
func (d *outgoing) applyPayload() {
	d.packet.Payload, d.missing = d.payloadBody, d.payload.Missing
	d.payloadBody, d.payload = nil, store.ReadRecordPayload{}
}

// offerLoadedDeliveries joins independent payload and exchange completions in
// selection order. A ready QoS 0 packet also waits for an earlier saved exchange.
// The cursor visits each offered entry once, without scanning the live window.
func (c *Core) offerLoadedDeliveries(acts []Action, u *consumerState) []Action {
	s := u.conn
	for c.canOffer(u) {
		d := u.offer
		if d.removed || d.missing {
			if !d.removed && d.attempted && u.channel.row.Type != store.Latest {
				u.channel.retentionMissed.Inc()
			}
			c.removePreparation(d)
			break // the retention boundary already counted any loss
		}
		u.nextOffer(d.next)
		d.preparing, d.quota = false, !u.service
		d.attempted = true
		d.unindexPreparation()
		c.sharedHandedOff(d)
		if d.attempt != nil {
			c.handedAttempt(d)
		}
		s.preparing--
		d.payloadSlot = false
		if u.service {
			s.serviceSlots--
		} else {
			s.payloadSlots--
			s.inflight++
		}
		s.writingPublishes++
		s.writerOrder++
		d.packet.Order = s.writerOrder
		d.stage = deliveryWriting
		u.conn.writingDeliveries++
		if d.exchange.QoS != 0 {
			d.counted = true
			c.countInflight(1)
		}
		acts = append(acts, Action{Conn: u.conn.conn, Reply: &d.packet})
		if d.answer != nil {
			c.finishServiceAnswer(d.answer)
		}
	}
	c.completeResume(s)
	c.queueConsumer(s.prepared.peek())
	return acts
}

func (c *Core) delivered(acts []Action, s *connectionState, p *connection.Delivery, now time.Time) []Action {
	d := &s.window[p.Slot]
	if &d.packet != p || d.consumer == nil || d.stage != deliveryWriting {
		panic("core: unknown completed delivery")
	}
	c.returnWriterDelivery(s)
	s.writingPublishes--
	u := d.consumer
	reassign := u.sharedCursor != nil && u.share.active && !p.Dup
	if p.Err != nil {
		acts = c.fail(acts, p.Err, now)
	}
	switch p.Result {
	case connection.DeliveryWritten:
		c.deliveries[p.QoS].Inc()
		if u.rule != nil && p.QoS == 0 {
			s.bridge.sent.Inc()
		}
	case connection.DeliveryExpired:
		if u.rule != nil {
			acts = c.bridgeUnsent(acts, u, d.offset, "expired", now)
		} else {
			c.deliveriesExpired.Inc()
		}
	case connection.DeliveryTooLarge:
		if d.answer != nil {
			d.answer.count(true)
			acts = c.logDeliveryDiscard(acts, s, string(p.Topic), 0, now)
		} else if !reassign {
			acts = c.missedDelivery(acts, u, d.offset, now)
		}
	case connection.DeliveryDiscarded:
		// A transport failure can report before Ended. Cancel unsent work
		// instead of refilling the dying writer; release rewinds unsaved work.
		acts = c.finish(acts, s, 0, now)
	}
	// Message memory ends at the writer's report, independently of PUBACK.
	c.returnDeliveryBytes(d, now)
	d.bytes = 0
	result := p.Result
	d.packet = connection.Delivery{Slot: p.Slot}
	d.stage = deliveryAcknowledgement // PUBLISH ownership returned; its PUBREC save may still be outstanding
	if g := u.share; g != nil && g.lastSelection == d && result != connection.DeliveryTooLarge {
		g.lastSelection = nil
		c.queueShared(g)
	}
	switch {
	case result == connection.DeliveryTooLarge && reassign:
		c.refuseShared(d, now)
	case result == connection.DeliveryWritten && d.exchange.QoS != 0 && !d.acknowledged:
		if !s.end.seen && s.phase == connected {
			d.stage = deliveryAcknowledgement
			if d.receipt == receiptAccepted {
				acts = c.readyRelease(acts, d)
			} else if d.receipt == receiptNone && c.admission.delay != 0 {
				c.requirePeer(d, now)
			}
			c.queueConsumers(s)
		} else {
			c.releaseDelivery(s, d, now)
		}
	case result == connection.DeliveryDiscarded:
		c.releaseDelivery(s, d, now)
	default:
		c.settleDelivery(s, d, now)
	}
	if !u.indexed {
		c.queueConsumer(u)
	}
	return acts
}

// returnDeliveryBytes distinguishes payload reservations from incoming work:
// only a writer report or cancellation can release an outgoing reservation.
func (c *Core) returnDeliveryBytes(d *outgoing, now time.Time) {
	if d.bytes == 0 {
		return
	}
	if c.admission.delay != 0 {
		c.memoryPeer -= d.bytes - d.storedBytes
	}
	c.returnMemory(1, d.bytes, now)
}

// missedDelivery is the common terminal accounting for a core memory-size
// discard and a writer's actual Maximum Packet Size discard.
func (c *Core) missedDelivery(acts []Action, u *consumerState, offset int64, now time.Time) []Action {
	u.channel.missed.Inc()
	if u.rule != nil {
		return c.bridgeUnsent(acts, u, offset, "too_large", now)
	}
	return c.logDeliveryDiscard(acts, u.conn, u.channel.row.Name, offset, now)
}

// logDeliveryDiscard also covers direct answers, which have no channel record
// or missed-record metric. Offset zero identifies such a service publication;
// a nil connection identifies a queue job discarded before a dead-letter move.
func (c *Core) logDeliveryDiscard(acts []Action, s *connectionState, name string, offset int64, now time.Time) []Action {
	if c.logCredits.Admit(logger.Warn) {
		var msg string
		switch {
		case s == nil:
			msg = fmt.Sprintf("queue %q discarded offset %d: too_large", name, offset)
		case offset == 0:
			msg = fmt.Sprintf("client %q missed answer on %q: too_large", s.clientID, name)
		default:
			msg = fmt.Sprintf("client %q missed %s offset %d: too_large", s.clientID, name, offset)
		}
		acts = append(acts, Action{Line: logger.Line{At: now, Level: logger.Warn,
			Msg: msg}})
	}
	return acts
}

// releaseDelivery cancels preparation bookkeeping independently of a payload
// read's buffer ownership. Once ownership returns, it releases reservations
// and the fixed slot. Only settleDelivery advances the position.
func (c *Core) releaseDelivery(s *connectionState, d *outgoing, now time.Time) {
	// Return quota and descriptor capacity before revisiting their owners.
	// A pending save can keep the descriptor after writer ownership returns.
	defer c.queueConsumers(s)
	c.completeReceipt(nil, d, nil)
	if c.admission.delay != 0 {
		c.endPeer(d)
	}
	c.cancelPreparation(d, now)
	d.releaseTopic()
	d.unindexPreparation()
	if index := d.boundary[3]; index != 0 {
		heap.Remove(&d.consumer.removals, index-1)
	}
	if d.answer != nil {
		c.finishServiceAnswer(d.answer)
	}
	if d.stage == deliveryLoading || d.saving {
		return
	}
	c.unqueueMemory(&d.memory, now)
	c.returnDeliveryBytes(d, now)
	d.bytes = 0
	if d.stage == deliveryReserved {
		d.consumer.reserved--
	}
	u := d.consumer
	if d.exchange.Attempt != nil {
		delete(u.recovered, d.offset)
	}
	if d.resumedRelease != nil {
		s.resumeReleases.Remove(d.resumedRelease)
		d.resumedRelease = nil
	}
	c.releaseShared(d)
	if u.ownsPosition() && d.saved && d.attempted && !d.initial && !d.exchange.NoMove && !d.settled && c.failure == nil {
		// A later departure rewind can cross this row after a progress save
		// marked it moves=0. Preserve its live origin before clearing the slot.
		if u.live == nil {
			u.live = make(map[uint16]struct{})
		}
		u.live[d.exchange.PacketID] = struct{}{}
	}
	if !d.saved && !d.settled && !d.initial {
		// Remove an unsaved position guard only after restoring the scan.
		// A later settlement/save answer must not pass this undelivered row.
		u.scan = min(u.scan, d.offset)
	}
	u.untrackDelivery(d)
	if u.reserve == d {
		u.reserve = nextPublish(d.next)
	}
	if u.load == d {
		u.load = nextPublish(d.next)
	}
	if u.offer == d {
		u.nextOffer(d.next)
	}
	if d.prev != nil {
		d.prev.next = d.next
	}
	if d.next == nil {
		u.last = d.prev
	} else {
		d.next.prev = d.prev
	}
	if u.last == nil && !u.indexed && !u.service {
		indexConsumerOwner(u)
	}
	if d.exchange.PacketID != 0 && s.identifiers[d.exchange.PacketID] == d {
		delete(s.identifiers, d.exchange.PacketID)
		s.freeIdentifier = min(s.freeIdentifier, uint32(d.exchange.PacketID))
	}
	if d.counted {
		c.countInflight(-1)
	}
	c.removePendingSave(d)
	if d.preparing {
		s.preparing--
	}
	if d.quota {
		s.inflight--
	}
	if d.payloadSlot {
		if u.service {
			s.serviceSlots--
		} else {
			s.payloadSlots--
		}
	}
	// Ownership has returned from the store or writer. Clear every message
	// reference before linking this fixed slot back onto the free list.
	if d.answer != nil {
		d.answer.count(false)
		c.finishServiceAnswer(d.answer)
	}
	slot, resumed := d.packet.Slot, d.release.Resumed
	capacity := publishCapacity
	if resumed {
		capacity = releaseCapacity
	}
	if end, held := u.settled[d.exchange.PacketID]; held && d.saved && (!s.stored || end.cancelled) {
		// An expiry-zero scan needs committed marker protection, and a
		// cancellation needs its deletion, before reusing the allowance.
		// Durable acknowledgements retain only their identifier and offset.
		end.capacity = capacity
		u.settled[d.exchange.PacketID] = end
		capacity = noDeliveryCapacity
	}
	*d = outgoing{packet: connection.Delivery{Slot: slot}}
	switch {
	case resumed:
		d.next, s.releaseFree = s.releaseFree, d
	default:
		d.next, s.free = s.free, d
	}
	c.returnDeliveryCapacity(s, capacity)
	if !u.indexed {
		c.queueConsumer(u)
	}
}

// cancelDelivery visits the fixed slots once, including consumers detached
// by UNSUBSCRIBE. Store and writer ownership ends at their later reports.
func (c *Core) cancelDelivery(s *connectionState, now time.Time) {
	c.failWorkers(s, now)
	c.admitServiceAnswers(nil, s)
	for _, u := range s.consumers {
		c.indexConsumer(u, false)
		if c.admission.delay != 0 {
			c.touchAdmission(u)
		}
		u.pass = nil
		if u.queue != nil {
			u.queue.remove(u)
		}
	}
	for i := range s.window {
		d := &s.window[i]
		if c.admission.delay != 0 && d.consumer != nil {
			c.endPeer(d)
		}
		if d.counted {
			d.counted = false
			c.countInflight(-1)
		}
		u := d.consumer
		if u == nil {
			continue
		}
		if q := u.queue; q != nil {
			q.remove(u)
		}
		if d.stage != deliveryWriting && d.stage != deliveryReleasing && !d.saving {
			c.releaseDelivery(s, d, now)
		}
	}
	if u := s.resuming; u != nil && u.queue != nil {
		u.queue.remove(u)
	}
}

// readyDeliveries inspects registered heads only. Deferred metadata owners
// remain parked until page credit or a new sharing cohort changes eligibility.
func (c *Core) readyDeliveries() bool {
	return c.deliveryLoads.Len() != 0 || c.deliveryChanges.Len() != 0 ||
		(c.phase < quiescing && c.deliveryReady.Len() != 0 && (c.metadataReads < c.metadataLimit || c.deliveryReadyChanged))
}
