package core

import (
	"container/list"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/timestamp"
)

type requestKind uint8

const (
	releaseIncoming    requestKind = iota
	resolvePublication             // an abandoned entry, resolved after its channel predecessors
	readIncoming
	loadSession
	loadBridgeHeld
	loadBridgeOutbound
	saveBridgeReplay
	leaveBridge
	loadConnectWill
	saveConnect
	leaveSession
	dueSessions
	dueWills
	endWill
	endExpired
	endHeld // non-durable holds discovered by the session sweep
	savePublishes
	connectSession // a CONNECT waiting for this session's preceding request
	changeSubscriptions
	saveSubscriptions
	readDeliveryMetadata
	readDeliveryPayload
	saveDeliveryChanges
	readResumedDeliveries
	readPublishCharges
	expireRetained
	readRetentionPage
	readRetentionDeadline
	readChannelMetrics
	releasePublishes // one CONNECT-held body at its place in the session lane
	fireWill         // a firing waiting for message memory
	readWill
	registerChannels
	executeRequest
	readSeek
	readKVSize
	saveSeek
	loadSharedGroup
	readSharedReturned
	saveSharedGroup
	loadSharedEnding
	endOrphanShared
	countUnmatchedValues
	saveQueueChange
	readQueueRetries
	saveQueueRetries
	syncStore
	foldSnapshot
	copySnapshot
	readOperations
)

// sessionLane serializes requests for a client id, including CONNECTs that
// wait for an earlier request. It is discarded as soon as its work ends;
// connected state belongs to connections, away state to the store (R6).
type sessionLane struct {
	bridge       *bridgeState // bridges have an independent owner space from client ids
	holds        int          // incoming holds not yet reflected in a session load
	waitRequests list.List
	client       string
	departing    *storeRequest // departure indexed at intake, even behind an earlier request
	request      *storeRequest
	pending      []*storeRequest // bounded by connection permits and reply slots
	expiry       int64           // earliest session deadline skipped by a sweep
	heldDue      bool            // orphan cleanup must be rediscovered, rather than ordinary expiry alone
	willDue      int64
}

type storeRequest struct {
	operation            *views.Request
	operationElement     *list.Element // the view credit, retained through its final store answer
	order                uint64
	channelElements      [2]*list.Element
	cancelled, discarded bool
	ready                bool
	classification       store.RequestClass // core-owned stage classification, captured before dispatch
	wait                 *list.List
	waitElement          *list.Element
	sharedEnd            *sharedEnding
	share                *sharedGroup
	shares               []*sharedGroup // group owners reserved together by this request
	queueAttempt         *queueAttempt
	service              *serviceRequest
	request              *channelRequest
	incoming             *incomingPublish
	publication          *publication // a queued entry keeps its offset through session and channel decisions
	admissionAt          time.Time    // oldest ready bookkeeping confirmed by this request
	kind                 requestKind
	op                   any         // the original store.Read or store.Write, never changed in flight
	before               store.Write // CONNECT's preceding ending, or departure held through a Will read
	lane                 *sessionLane
	channel              *channelState
	otherChannel         *channelState // a queue move reserves its append destination together
	conn                 *connectionState
	publishes            publishBatch
	group                *writeGroup
	predecessor          *writeGroup              // a fixed accepted-write dependency, never a later group
	transaction          writeGroup               // used only when this request starts a group
	viewPending          bool                     // a dispatched decision read still gates later writes
	boundaries           [][]subscriptionBoundary // per-filter channel tails captured at SUBSCRIBE intake
	tail                 int64                    // metadata read's committed upper boundary
	reply                connection.Reply
	consumer             *consumerState
	payloads             payloadBatch
	firstSave, lastSave  *list.Element                  // existing pendingSaves nodes covered by this write
	sharedSaves          int                            // pending shared-evidence prefix covered by this write
	dispositions         store.ReadMetadataDispositions // the page cohort, core-owned until metadata returns
	metadata             store.ReadRecordMetadata       // core-owned query bounds; Rows is always nil
	metadataCheck        int                            // one-based recipient check within the common channel page
	sharedRevision       uint64                         // group read view, invalidated by retention or returns
	revision             uint64                         // subscription revision at a consumer read's admission
	pass                 *retainedPass
	consumers            []*consumerState    // one read slot per participant, released together
	retentionConsumers   []retentionConsumer // loss observations captured at the retention boundary
	admitted             uint64              // retained pages can share only their admission event's order
	replySlot            *list.Element       // packet-order place for a stored-change reply
	orderedReply         bool
	cancellations        []*outgoing // preparations owned by this UNSUBSCRIBE until its answer
	willInfo             store.WillInfo
	willKind             requestKind // completion to apply after publishing the Will
	willChanges          int         // writes stored with the publication
	willCause            willCause
	memory               memoryRequest // a firing Will uses the existing incoming reservation FIFO
}

// Credit is the free room shared by store reads and writes (R2).
func (c *Core) Credit() int {
	credit := store.Credit - len(c.requests)
	if c.outstanding != nil {
		credit--
	}
	return credit
}

func (c *Core) lane(client string) *sessionLane {
	lane := c.lanes[client]
	if lane == nil {
		lane = &sessionLane{client: client}
		c.lanes[client] = lane
	}
	return lane
}

// connectionLane gives session packets and bridge packets the same request
// ordering, without confusing a local client id with a bridge's client id.
func (c *Core) connectionLane(s *connectionState) *sessionLane {
	if s.bridge != nil {
		return &s.bridge.lane
	}
	return c.lane(s.clientID)
}

func (c *Core) findConnectionLane(s *connectionState) *sessionLane {
	if s.bridge != nil {
		return &s.bridge.lane
	}
	return c.lanes[s.clientID]
}

// enqueue reserves both owners together. Packets whose channel still awaits
// CONNECT keep their session order until that decision can bind the channel.
func (c *Core) enqueue(r *storeRequest) {
	c.nextRequest++
	r.order = c.nextRequest
	c.classifyRequest(r)
	c.registerChannel(r)
	if r.lane != nil {
		if r.lane.bridge == nil {
			c.lanes[r.lane.client] = r.lane
		}
		switch r.kind {
		case connectSession:
			if departure := r.lane.departing; departure != nil {
				c.armTakeoverCut(departure.conn, r.conn.attempt.at)
			}
		case leaveSession:
			r.lane.departing = r
			c.armDeparture(r.lane)
		}
		if r.lane.request != nil {
			r.lane.pending = append(r.lane.pending, r)
			if r.channel == nil {
				return
			}
		} else {
			r.lane.request = r
		}
	}
	if r.channel != nil {
		r.channel.pending = r
	}
	c.readyRequest(r)
}

// continueRequest resumes another stage of the same operation before later
// pending work. Lane successors enter the ordinary ready FIFO instead.
func (c *Core) continueRequest(r *storeRequest) {
	c.classifyRequest(r)
	c.unparkRequest(r)
	if !r.ready {
		r.ready = true
		if c.readyAt != 0 {
			c.readyAt--
			c.ready[c.readyAt] = r
		} else {
			c.ready = slices.Insert(c.ready, 0, r)
		}
	}
}

// A read boundary survives parking and continuation; runnable queue position
// cannot identify it. Record reads, observations and writes leave pending
// batches open. Later stages retain their original store-request order.
func (c *Core) classifyRequest(r *storeRequest) {
	r.classification = store.Classify(r.op)
	if _, read := r.op.(store.Read); read && r.classification == store.OrderedRequest {
		c.publishBarrier = max(c.publishBarrier, r.order)
	}
}

// advance makes the next request eligible only after all earlier work for
// its client id. Pending packets retain their original connection and slot.
func (c *Core) advance(lane *sessionLane) {
	if lane.request == nil && len(lane.pending) != 0 {
		r := lane.pending[0]
		lane.pending[0] = nil
		lane.pending = lane.pending[1:]
		lane.request = r
		c.readyRequest(r)
		c.wakeRequests(&lane.waitRequests)
	}
	if lane.request == nil && len(lane.pending) == 0 && lane.holds == 0 {
		if lane.bridge == nil {
			delete(c.lanes, lane.client)
		}
		if lane.willDue != 0 {
			c.willExpiry.note(lane.willDue)
		}
		if lane.expiry != 0 {
			c.expiry.firedRetry = c.expiry.firedRetry || lane.heldDue
			c.noteExpiry(lane.expiry)
		}
	}
}

// blockedChannel checks every channel before taking any exclusive ownership.
// List positions remain independent of the request's current dependency.
func (c *Core) blockedChannel(r *storeRequest) *channelState {
	c.registerChannel(r)
	for _, ch := range [2]*channelState{r.channel, r.otherChannel} {
		if ch != nil && (ch.requestOrder.Front().Value != r || (ch.request != nil && ch.request != r)) {
			if !c.groupRoom(r.publishes.count, r.publishes.messageBytes, 0) {
				c.sealGroup(commitMaxRecords)
			}
			return ch
		}
	}
	return nil
}

// reserveChannel follows the joint eligibility check in this dispatch iteration.
func (c *Core) reserveChannel(r *storeRequest) {
	for _, ch := range [2]*channelState{r.channel, r.otherChannel} {
		if ch != nil {
			ch.request = r
			if ch.pending == r {
				ch.pending = nil
			}
		}
	}
}

// startConnect takes the old connection over once, then waits asynchronously
// for its last writes and writer reports before deciding from its current
// state. Its reads are superseded and their results cannot affect the takeover.
func (c *Core) startConnect(acts []Action, lane *sessionLane, s *connectionState, now time.Time) ([]Action, bool) {
	old := s.attempt.takeover
	if old == nil {
		old = c.sessions[lane.client]
		if old != nil {
			s.attempt.takeover = old
			old.end = endMark{left: timestamp.CeilMilli(now), expiry: old.session.Expiry, reason: 0x8E, seen: true}
			if s.attempt.clean || old.session.Expiry == 0 {
				c.markSharedEnd(old)
			}
			c.detach(old)
			acts = c.finish(acts, old, 0x8E, now)
			c.armTakeoverCut(old, now)
			if old.session.Expiry == 0 && old.willBytes == 0 && !old.hasHeldPublishes() {
				// Takeover ends a connection-lifetime session even when
				// the replacement CONNECT is later refused.
				acts = c.leave(acts, nil, old, now)
			}
		}
	}
	// Stop the old owner and cancel its queued channel predecessors before
	// waiting for holds. A later hold can sit behind one of those requests.
	if lane.holds != 0 {
		return acts, false
	}
	if old != nil && (old.inflight != 0 || old.writingDeliveries != 0 || old.deliveryWrites != 0 || old.pendingAcks != 0) {
		return acts, false
	}
	// Only now can a pipelined body avoid holding incoming bytes and a
	// batch slot while the old connection's writer reports are pending.
	// No stored Wills means the load cannot need a prerequisite reservation;
	// an assigned identifier never inherits another session's Will.
	if c.willCount == 0 || s.attempt.assigned {
		c.allowConnectBodies(s)
	}
	if old != nil {
		exists := old.session.Expiry != 0
		if exists && !s.attempt.clean {
			s.incoming = old.incoming
			for id, h := range s.incoming {
				if h.request {
					delete(s.incoming, id)
				}
			}
			s.deliverySequence = old.deliverySequence
			s.recoveryCount = old.recoveryCount
			if s.recoveryCount != 0 {
				s.resumeThrough = old.deliverySequence
			}
			for _, u := range old.consumers {
				next := c.newConsumer(s, u.channel, u.position)
				next.persisted, next.seen = u.persisted, u.seen
				next.dirty = u.dirty && s.attempt.expiry != 0
				s.addConsumer(next)
			}
			c.beginResume(s)
		}
		if old.willBytes != 0 {
			c.enqueue(&storeRequest{kind: loadConnectWill, op: &store.LoadWillInfo{Session: old.session.ID}, conn: s, lane: lane})
			return acts, true
		}
		return c.decide(acts, lane, s, &old.session, old.subscriptions, exists, now), true
	}
	c.enqueue(&storeRequest{kind: loadSession, op: &store.LoadSession{Client: lane.client}, conn: s, lane: lane})
	return acts, true
}

// Answer releases a read's view gate at confirmation, retaining its credit
// until the result. Results return credit and settle the original request.
// A failed read or a non-full write error is fatal, as for the store (R8).
func (c *Core) Answer(acts []Action, a store.Answer, now time.Time) []Action {
	c.wakeTicket++
	if c.admission.delay != 0 {
		c.eventAt = now
	}
	if c.stopCutDue(now) {
		acts = c.cutConnections(acts, now)
	}
	if g := c.outstanding; g != nil && a.Op == g.write() {
		c.outstanding = nil
		// Standalone syncs and snapshots cannot establish that a refused
		// transaction has room again. Only transaction results change it.
		if g.operation == nil {
			full := errors.Is(a.Err, store.ErrFull)
			if full || a.Err == nil {
				c.logDiskRoom(full, now)
			}
		}
		if a.Err == nil && g.operation == nil {
			c.syncDirty = c.syncEvery != 0
			c.snapshot.dirty = c.snapshot.every != 0
			c.commits[g.cause].Inc()
			c.committed.Add(uint64(g.records))
		} else if a.Err != nil && !errors.Is(a.Err, store.ErrFull) && !g.members[0].snapshot() {
			acts = c.fail(acts, a.Err, now)
		}
		for _, r := range g.members {
			acts = c.applyAnswer(acts, r, a.Err, now)
		}
		clear(g.members)
		g.members = g.members[:0]
		clear(g.Writes)
		g.Writes = g.Writes[:0]
		clear(g.initial[:])
		g.applied = true
		c.wakeRequests(&g.waitRequests)
		c.wakeRequests(&c.writeWait)
		c.returnStoreCredit()
		if c.sessionWrites == g {
			c.sessionWrites = nil
		}
	} else {
		r := c.requests[a.Op]
		if r == nil {
			panic("core: answer to an unknown store request")
		}
		if r.viewPending {
			r.viewPending = false
			c.unconfirmed--
		}
		if a.View {
			if r.kind == readOperations {
				// The store snapshot precedes the live capture. A reconnect
				// is therefore either still away there or held by this core.
				if a.Err == nil && !r.operation.Abandoned() {
					c.captureOperation(r)
				}
				r.op.(store.ObservationRead).Capture()
			}
			if r.kind == readKVSize {
				// This decision's view is fixed. Later channel writes need
				// not await its result or the answer's memory reservation.
				c.releaseChannel(r)
			}
			if r.kind == readRetentionDeadline {
				// Every earlier accepted commit is in this fixed view. Only
				// subsequent commits need the pass's changed-deadline guard.
				c.retained.changed = 0
			}
			return c.dispatch(acts, now)
		}
		delete(c.requests, a.Op)
		c.returnStoreCredit()
		if a.Err != nil && !errors.Is(a.Err, store.ErrFull) && !r.snapshot() {
			acts = c.fail(acts, a.Err, now)
		}
		acts = c.applyAnswer(acts, r, a.Err, now)
	}
	return c.dispatch(acts, now)
}

// applyAnswer settles one owner's result. Group answers call it for every
// member before dispatching any work made eligible by those results.
func (c *Core) applyAnswer(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	if r.kind == readOperations {
		c.answerOperation(r, err)
		return acts
	}
	if r.kind != readWill && r.kind != loadSharedEnding {
		c.completedSharedEnding(r, err, now)
	}
	if err == nil {
		if w, ok := r.op.(store.Write); ok {
			c.endedHeld(w)
		}
		if r.before != nil {
			c.endedHeld(r.before)
		}
	}
	if r.kind == readWill {
		return c.loadedWill(acts, r, now)
	}
	if r.kind == readIncoming {
		return c.loadedIncoming(acts, r)
	}
	if r.kind == readSeek {
		if r.request == nil {
			return acts
		}
		acts = c.readSeek(acts, r, now)
		if r.kind == readSeek {
			c.releaseChannel(r)
			c.releaseLane(r)
			c.advance(r.lane)
		}
		return acts
	}
	if r.kind == readKVSize {
		if r.request == nil {
			return acts
		}
		acts = c.readKVSize(acts, r, now)
		c.releaseChannel(r)
		c.releaseLane(r)
		c.advance(r.lane)
		return acts
	}
	if r.kind == readPublishCharges {
		if c.failure != nil {
			c.releaseSharedRequest(r)
			c.releaseBatch(&r.publishes)
			c.discardPublishes(&r.publishes.publications, now)
			c.releaseChannel(r)
			return acts
		}
		if op := r.op.(*publishCharges); op.after == len(r.publishes.charges) {
			acts = c.admitPublishes(acts, &r.publishes, now)
			r.kind, r.op = savePublishes, &r.publishes
		}
		// Resume the channel's pending work after applying its admission read.
		c.continueRequest(r)
		return acts
	}
	if r.kind == readRetentionPage {
		return c.readRetentionPage(acts, r, now)
	}
	if r.kind == readQueueRetries {
		c.readQueueRetries(r, now)
		return acts
	}
	if r.channel != nil {
		c.releaseChannel(r)
	}
	switch r.kind {
	case foldSnapshot:
		if err == nil && c.failure == nil {
			c.snapshot.dirty = false
			r.kind, r.op = copySnapshot, &store.SnapshotCopy{Source: c.snapshot.source, Destination: c.snapshot.destination}
			c.continueRequest(r)
			return acts
		}
		if err == nil {
			err = c.failure // a failed store cannot begin the copy
		}
		if c.orderedRead == r {
			c.orderedRead = nil
			c.wakeRequests(&c.orderWait)
		}
		return c.endSnapshot(acts, err, now)
	case copySnapshot:
		if warning := r.op.(*store.SnapshotCopy).GroupWarning; warning != nil {
			acts = c.logFailure(acts, logger.Warn, warning, now)
		}
		return c.endSnapshot(acts, err, now)
	case syncStore:
		c.syncPending = false
		c.syncDirty = false
		c.syncAt = now.Add(c.syncEvery)
	case saveQueueChange:
		acts = c.savedQueueChange(acts, r, err, now)
	case saveQueueRetries:
		c.savedQueueRetries(r, err, now)
	case registerChannels:
		if _, ok := r.op.(*store.ReconcileBridges); ok && err != nil {
			acts = c.fail(acts, err, now)
		}
		if op, ok := r.op.(*store.ReconcileBridges); ok && err == nil {
			for _, message := range op.Notices {
				acts = c.logFailure(acts, logger.Info, errors.New(message), now)
			}
			// The buffer holds a full event's log credits. Any excess is
			// dropped at this same event, with no intervening results.
			c.logCredits.AdmitBatch(logger.Info, op.Omitted)
		}
		if _, ok := r.op.(*store.AddBridges); ok && err == nil {
			for _, b := range c.bridges {
				b.registered = true
			}
		}
	case countUnmatchedValues:
		if op := r.op.(*store.CountUnmatchedValues); err == nil && op.Count != 0 {
			acts = c.logFailure(acts, logger.Warn, fmt.Errorf("latest channel %s: %d current values no longer match its filter", r.channel.row.Name, op.Count), now)
		}
	case loadSharedGroup:
		c.loadedShared(r)
	case readSharedReturned:
		c.readSharedReturned(r)
	case saveSharedGroup:
		acts = c.savedShared(acts, r, err, now)
	case loadSharedEnding:
		c.loadedSharedEnding(r)
	case expireRetained:
		if r.channel.deadLetter != nil {
			c.releaseRetentionPage(r)
		}
		if c.failure == nil {
			acts = c.expiredRetained(acts, r, err, now)
		}
	case readRetentionDeadline:
		c.readRetentionDeadline(r, now)
	case readChannelMetrics:
		c.readChannelMetrics(now)
	case readDeliveryMetadata:
		acts = c.readDeliveryMetadata(acts, r, now)
	case readResumedDeliveries:
		acts = c.readResumedDeliveries(acts, r, now)
	case readDeliveryPayload:
		acts = c.readDeliveryPayload(acts, r, now)
	case saveDeliveryChanges:
		acts = c.savedDeliveryChanges(acts, r, err, now)
	}
	if r.kind == savePublishes {
		// The store returned ownership even on a fatal answer. Settle
		// these bodies before skipping any further decisions.
		acts = c.savedPublishes(acts, &r.publishes, err, now)
		c.releaseSharedRequest(r)
	}
	if r.kind == saveSubscriptions {
		acts = c.savedSubscriptions(acts, r, err, now)
	}
	if r.kind == saveSeek {
		acts = c.savedSeek(acts, r, err, now)
	}
	if r.kind == endOrphanShared {
		c.completedSharedCleanup(r, err, now)
	}
	if c.failure != nil {
		return acts // reclaim all outstanding credits without starting more work
	}
	if r.lane != nil {
		c.releaseLane(r)
	}
	switch r.kind {
	case loadBridgeOutbound:
		acts = c.loadedBridgeOutbound(acts, r, now)
	case loadBridgeHeld:
		acts = c.loadedBridgeHeld(acts, r)
	case saveBridgeReplay:
		acts = c.savedBridgeReplay(acts, r, err, now)
	case loadSession:
		if err != nil {
			panic(err) // reads cannot answer full
		}
		acts = c.loaded(acts, r, now)
	case loadConnectWill:
		acts = c.loadedConnectWill(acts, r, now)
	case saveConnect:
		acts = c.savedConnect(acts, r, err, now)
	case leaveSession:
		acts = c.left(acts, r, err, now)
	case dueSessions:
		if err != nil {
			panic(err)
		}
		c.expiry.due(r.op.(*store.DueSessions))
	case dueWills:
		c.willExpiry.due(&r.op.(*store.DueWills).DueSessions)
	case endWill:
		c.willExpiry.ending--
		if errors.Is(err, store.ErrFull) {
			c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
		}
	case endExpired, endHeld:
		c.expired(err, now, r.kind == endExpired)
	}
	if r.lane != nil {
		c.advance(r.lane)
	}
	return acts
}

func (c *Core) loaded(acts []Action, r *storeRequest, now time.Time) []Action {
	s, load := r.conn, r.op.(*store.LoadSession)
	if s.end.seen || s.phase == closing {
		c.abortConnect(s)
		return acts
	}
	if s.attempt.assigned && load.Found {
		// Keep the old name reserved while choosing its replacement.
		s.clientID = c.assignClientID(s.attempt.at)
		next := c.lane(s.clientID)
		// Pipelined packets belong to the finally assigned session too.
		// Move their existing order before the replacement load can finish.
		r.lane.pending = slices.DeleteFunc(r.lane.pending, func(packet *storeRequest) bool {
			if packet.conn != s {
				return false
			}
			packet.lane = next
			next.pending = append(next.pending, packet)
			return true
		})
		acts, _ = c.startConnect(acts, next, s, now) // an assigned id has no connected owner
		return acts
	}
	s.attempt.oldWill = load.Will
	if load.Found && !s.attempt.clean && len(load.Held) != 0 {
		c.restoreIncoming(s, load.Held)
	}
	if load.Found && !s.attempt.clean {
		s.deliverySequence, s.resumeThrough = load.Sequence, load.Sequence
		s.recoveryCount = load.Deliveries
		if load.Position != 0 {
			u := c.newConsumer(s, &c.broadcast, load.Position)
			u.seen = load.Seen.Int64
			s.addConsumer(u)
		}
		for _, position := range load.Positions {
			u := c.newConsumer(s, c.channel(position.Channel), position.Offset)
			u.seen = position.Seen.Int64
			s.addConsumer(u)
		}
		c.beginResume(s)
	}
	return c.decide(acts, r.lane, s, &load.Session, load.Subscriptions, load.Found, now)
}

func (c *Core) loadedConnectWill(acts []Action, r *storeRequest, now time.Time) []Action {
	s := r.conn
	a := s.attempt
	old := a.takeover
	if s.end.seen || s.phase == closing {
		c.abortConnect(s)
		return c.leave(acts, r.lane, old, now)
	}
	a.oldWill = r.op.(*store.LoadWillInfo).Will
	return c.decide(acts, r.lane, s, &old.session, old.subscriptions, old.session.Expiry != 0, now)
}

func (c *Core) savedConnect(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	s := r.conn
	if err == nil {
		s.stored = s.session.Expiry != 0
		if s.attempt.oldWill.Session != 0 {
			c.willCount--
		}
		if s.attempt.will != nil {
			c.willCount++
		}
		return c.grant(acts, s, now)
	}
	a := s.attempt
	// A refused takeover leaves the previous durable session away with
	// its old expiry. This is separate from the rolled-back CONNECT.
	if a.takeover != nil && (a.takeover.stored || a.takeover.willBytes != 0 || a.takeover.hasHeldPublishes()) {
		old := a.takeover
		acts = c.leave(acts, r.lane, old, now)
	}
	if c.stopCutDue(now) {
		acts = c.cut(acts, s, 0x8B, now)
	}
	return c.refuse(acts, s, 0x97, 0, now)
}

func (c *Core) left(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	s := r.conn
	if errors.Is(err, store.ErrFull) && s.end.expiry == 0 && (len(s.incoming) != 0 || s.spooled) {
		c.retryAt = earlier(c.retryAt, now.Add(time.Second).UnixMilli())
	}
	if err == nil {
		s.session.Expiry, s.session.Left = s.end.expiry, s.end.left
		if s.willBytes != 0 && s.end.discardWill {
			c.willCount--
			s.willBytes = 0
		}
		if s.end.expiry == 0 {
			s.stored = false
			c.endedCounts[connectionEnd].Inc()
		}
	}
	if c.stopCutDue(now) {
		acts = c.cut(acts, s, s.end.reason, now)
	}
	if c.phase == stopping {
		return acts // this event's final entries follow all member results
	}
	return c.finish(acts, s, s.end.reason, now)
}

// dispatch first applies this event's request order, then dispatches at most
// one write group, after all eligible members have joined.
func (c *Core) dispatch(acts []Action, now time.Time) []Action {
	for {
		acts = c.dispatchRequests(acts, now)
		if c.collecting != nil || len(c.sealed) != 0 {
			// Refused bridge marks survive their link. Give them the next
			// ordinary commit opportunity, including already sealed work.
			c.saveBridgeReplayMarks()
		}
		acts = c.flushStoredReplies(acts, now)
		if c.phase == stopping {
			acts = c.closeConnections(acts, now)
		}
		if c.failure != nil || !c.localWorkReady() {
			break
		}
	}
	acts = c.finalSnapshot(acts, now)
	acts = c.dispatchGroup(acts)
	c.noteStoreCredit(now)
	c.endMemoryWait(0, now)
	c.endMemoryWait(1, now)
	c.updateMemoryEpisodes(now)
	return c.flushLogs(acts)
}

// dispatchRequests collects eligible writes up to each decision read. A
// barrier seals accepted writes immediately and waits for their results.
// Requests keep their admission order across dependency waits. Only owners
// whose dependency changed return to ready work. Unrelated owners continue.
func (c *Core) dispatchRequests(acts []Action, now time.Time) []Action {
	if c.failure != nil {
		return acts
	}
	reads := c.readActions[:0]
	var previous *storeRequest
	for {
		// A read superseded or completed without I/O returns its temporary
		// permit before considering the next registered request.
		if previous != nil {
			c.returnReadPermit(previous)
			previous = nil
		}
		if c.readyAt == len(c.ready) {
			if c.memoryReady {
				c.memoryReady = false
				acts = c.giveBytes(acts, now)
			}
			if c.readyDeliveries() {
				acts = c.scheduleDeliveries(acts, now)
			}
			if c.phase < quiescing && c.Credit() > 0 {
				c.sweep(now)
				if c.retentionReady {
					c.retentionReady = false
					c.retainAge(now)
				}
			}
			if c.admission.delay != 0 {
				c.evaluateAdmission(now)
			}
			if !c.localWorkReady() {
				break
			}
			if c.readyAt == len(c.ready) {
				continue
			}
		}
		r := c.ready[c.readyAt]
		c.ready[c.readyAt] = nil
		c.readyAt++
		r.ready = false
		previous = r
		if r.cancelled {
			continue
		}
		if r.kind == readOperations && r.operation.Abandoned() {
			c.cancelPendingRequest(r)
			c.completeOperation(r)
			continue
		}
		if (r.kind == executeRequest || r.kind == readSeek || r.kind == readKVSize) && r.request == nil {
			continue
		}
		if r.kind == readDeliveryMetadata && !c.metadataCurrent(r) {
			// A queued subscription change supersedes an undispatched scan.
			// Return its page credit without issuing work that cannot be used.
			for _, u := range c.finishMetadataRead(r) {
				c.queueConsumer(u)
			}
			continue
		}
		if r.kind == releasePublishes {
			p := r.publication
			if r.conn.end.seen || r.conn.phase >= refused ||
				p.repeat != nil {
				c.releaseBatch(&r.publishes) // ended packets and repeats need no new batch
			}
		}
		// A handed-over body transfers its existing potential batch slot
		// before owner waits; otherwise pendingBatches stops the next grant.
		if r.publishes.capacity != batchUnqueued && !c.admitBatch(&r.publishes) {
			c.parkRequest(r, &c.batchWait)
			continue
		}
		// Order positions are not exclusive claims. Check both owners first.
		if r.lane != nil && r.lane.request != r {
			c.parkRequest(r, &r.lane.waitRequests)
			continue
		}
		if r.kind == loadBridgeHeld && r.lane.holds != 0 {
			c.parkRequest(r, &r.lane.waitRequests)
			continue
		}
		if blocked := c.blockedChannel(r); blocked != nil {
			c.parkRequest(r, &blocked.waitRequests)
			continue
		}
		decision := r.classification == store.OrderedRequest
		if c.orderedRead != nil && c.orderedRead != r && (r.order > c.orderedRead.order || c.orderedRead.kind == foldSnapshot) && decision {
			c.parkRequest(r, &c.orderWait)
			continue
		}
		switch r.kind {
		case releaseIncoming, releasePublishes, changeSubscriptions, executeRequest:
			if r.conn.phase == connecting && !r.conn.end.seen {
				// An assigned-id collision can move CONNECT to another
				// lane. Its packets still await the final session decision.
				c.parkRequest(r, &r.conn.workWait)
				continue
			}
		}
		if (r.kind == saveQueueChange || r.kind == readQueueRetries || r.kind == saveQueueRetries || (r.kind == readRetentionPage && r.queueAttempt != nil)) && !c.reserveSharedRequest(r) {
			continue
		}
		if r.kind != executeRequest && r.kind != changeSubscriptions && r.kind != releaseIncoming && r.kind != savePublishes {
			c.reserveChannel(r)
		}
		if r.kind == releaseIncoming {
			lane := r.lane
			var finished bool
			acts, finished = c.startIncoming(acts, r)
			if finished {
				c.endLane(lane, r)
				if r.kind == resolvePublication {
					c.continueRequest(r)
				}
				continue
			}
			if r.kind == releaseIncoming {
				if r.wait == nil {
					c.parkRequest(r, &r.conn.workWait)
				}
				continue
			}
		}
		if r.kind == releasePublishes {
			lane := r.lane
			lane.request, r.lane = nil, nil
			acts = c.acceptPublication(acts, r, now)
			if r.kind == releasePublishes {
				c.releaseBatch(&r.publishes)
				if p := r.op.(*publication); p.record.Offset != 0 {
					r.kind, r.op, r.tail = resolvePublication, nil, p.record.Offset+1
					c.continueRequest(r)
				} else if r.channel != nil {
					c.releaseChannel(r)
				}
			}
			c.advance(lane)
			continue
		}
		if r.kind == executeRequest {
			if r.request.kind == seekService && !r.conn.end.seen && r.conn.phase == connected && r.conn.subscriptionsBusy() {
				c.parkRequest(r, &r.conn.workWait)
				continue
			}
			if !c.reserveSharedGroups(r) {
				continue
			}
			var finished bool
			c.reserveChannel(r)
			acts, finished = c.startRequest(acts, r, now)
			if finished {
				if r.channel != nil {
					c.releaseChannel(r)
					if r.channel.pending == r {
						r.channel.pending = nil
					}
				}
				c.endLane(r.lane, r)
			}
			continue
		}
		if r.kind == connectSession || r.kind == changeSubscriptions {
			if r.kind == changeSubscriptions && r.conn.subscriptionsBusy() &&
				!r.conn.end.seen && r.conn.phase == connected {
				c.parkRequest(r, &r.conn.workWait)
				continue
			}
			if r.kind == changeSubscriptions && !c.reserveSharedGroups(r) {
				continue
			}
			if r.kind == connectSession || r.conn.end.seen || r.conn.phase >= refused {
				c.releaseLane(r)
			}
			switch {
			case r.conn.end.seen || r.conn.phase >= refused:
				if r.kind == changeSubscriptions {
					c.releaseSharedRequest(r)
					c.completeAcknowledgement(r.conn)
					c.completeSubscription(r.conn)
				} else {
					if a := r.conn.attempt; a != nil && a.takeover != nil &&
						(a.takeover.stored || a.takeover.willBytes != 0 || a.takeover.hasHeldPublishes()) {
						acts = c.leave(acts, r.lane, a.takeover, now)
					}
					c.abortConnect(r.conn)
				}
			case r.kind == connectSession:
				var ready bool
				acts, ready = c.startConnect(acts, r.lane, r.conn, now)
				if !ready {
					r.lane.request = r
					if old := r.conn.attempt.takeover; old != nil {
						c.parkRequest(r, &old.workWait)
					} else {
						c.parkRequest(r, &r.lane.waitRequests)
					}
					continue
				}
			default:
				acts = c.startSubscriptions(acts, r, now)
			}
			c.advance(r.lane)
			continue
		}
		if r.kind == resolvePublication {
			if r.channel != nil {
				c.releaseChannel(r)
				c.resolvePublications(r.channel, r.tail)
			}
			continue
		}
		if _, ok := r.op.(store.Write); ok {
			if r.kind == endOrphanShared {
				if !c.reserveSharedRequest(r) {
					continue
				}
				g := r.shares[0]
				if g.id != r.op.(*store.SharedChange).ID || g.durable != 0 || len(g.members) != 0 {
					c.releaseSharedRequest(r)
					c.expired(nil, now, false)
					continue
				}
			}
			if r.kind == leaveSession || r.kind == leaveBridge {
				c.collectDeparture(r, now)
				continue
			}
			if r.kind == saveConnect {
				if a := r.conn.attempt; a.present && a.takeover != nil {
					c.collectDeliveryChanges(a.takeover, now)
				}
			}
			if r.willInfo.Session != 0 {
				c.prepareWill(r)
				continue
			}
			// Charge reads are prerequisites of publication admission. They
			// must finish before the write acquires its shared groups.
			if r.kind == savePublishes && !r.publishes.prepared && len(r.publishes.charges) != 0 {
				r.kind, r.op = readPublishCharges, &publishCharges{batch: &r.publishes}
				c.continueRequest(r)
				continue
			}
			if !c.prepareSharedEnding(r) {
				continue
			}
			if r.kind == savePublishes {
				c.reserveChannel(r)
				if !r.publishes.prepared {
					acts = c.admitPublishes(acts, &r.publishes, now)
				}
				if r.publishes.count == 0 && len(r.publishes.dropped) == 0 {
					c.releaseBatch(&r.publishes)
					c.releaseChannel(r)
					c.resolvePublications(r.channel, r.publishes.channel.Next)
					continue
				}
			}
			_, deleting := r.op.(*store.Retention)
			if deleting {
				// The core-held snapshot depends on applying earlier session
				// changes, not merely executing their SQL before the deletion.
				// Seal its predecessor so it can finish. Independent eligible
				// writes may pass this pending request; if accepted, they too
				// precede the eventual deletion's ordered boundary.
				if (r.predecessor == nil || r.predecessor.applied) && c.sessionWrites != nil {
					r.predecessor = c.sessionWrites
					c.sealGroup(commitEvent)
				}
				if r.predecessor != nil && !r.predecessor.applied {
					c.parkRequest(r, &r.predecessor.waitRequests)
					continue
				}
			}
			switch r.kind {
			case expireRetained, saveQueueChange:
				if _, moving := r.op.(*store.Retention); !moving {
					break
				}
				c.retentionBoundary(r)
				if r.channel.deadLetter != nil {
					if err := c.enterDeadLetters(r, now); err != nil {
						acts = c.fail(acts, err, now)
						c.discardWrite(r, now)
						continue
					}
				}
			case savePublishes:
				r.publishes.floor = r.channel.row.Floor
			}
			c.collect(r, now)
			continue
		}
		if decision {
			c.sealGroup(commitBarrier)
		}
		if decision && (c.outstanding != nil || len(c.sealed) != 0) {
			c.orderedRead = r
			c.parkRequest(r, &c.writeWait)
			continue
		}
		if c.readPermit != r && c.freeStoreCredit() == 0 {
			if decision {
				c.orderedRead = r
			}
			c.parkRequest(r, &c.creditWait)
			continue
		}
		if c.readPermit == r {
			c.readPermit = nil
		}
		if c.orderedRead == r {
			c.orderedRead = nil
			c.wakeRequests(&c.orderWait)
		}
		if r.kind == readRetentionPage || r.kind == readQueueRetries {
			if c.metadataReads == c.metadataLimit {
				c.parkRequest(r, &c.metadataWait)
				continue
			}
			c.metadataReads++
			if r.kind == readRetentionPage {
				op := r.op.(*store.ReadRetention).Retention
				op.Floor = r.channel.row.Floor
				if op.Job == nil {
					op.Tail = r.channel.committedTail
				}
				r.channel.retaining = true
			}
		}
		if decision {
			r.viewPending = true
			c.unconfirmed++
		}
		if r.kind == readChannelMetrics {
			// Earlier accepted writes have been applied and later ordered
			// changes cannot pass this boundary. Freeze the core's source
			// before handing the read to a store reader.
			op := r.op.(*store.MetricRefresh)
			for _, s := range c.sessions {
				if !s.stored {
					op.NonDurable += int64(len(s.subscriptions))
				}
			}
		}
		c.requests[r.op] = r
		c.returnStoreCredit()
		if r.kind == readDeliveryPayload && r.consumer.channel.loading == r {
			r.consumer.channel.loading = nil // a dispatched batch's membership is immutable
		}
		a := Action{}
		if r.conn != nil {
			a.Conn = r.conn.conn
		}
		a.Read = r.op.(store.Read)
		reads = append(reads, a)
	}
	acts = c.flushStoredReplies(acts, now)
	acts = append(acts, reads...)
	clear(reads)
	c.readActions = reads[:0]
	if c.admission.delay != 0 {
		c.evaluateAdmission(now)
	}
	return acts
}

// noteStoreCredit measures only completed intervals, at the core event that
// exhausts or returns the shared credit. Scrapes never influence intake.
func (c *Core) noteStoreCredit(now time.Time) {
	if c.Credit() == 0 {
		if c.noCreditAt.IsZero() {
			c.noCreditAt = now
		}
	} else if !c.noCreditAt.IsZero() {
		c.noCredit.Add(now.Sub(c.noCreditAt))
		c.noCreditAt = time.Time{}
	}
}
