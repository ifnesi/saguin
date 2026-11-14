package core

import (
	"crypto/rand"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// connectAttempt retains scalars and the Will being transferred to storage,
// never the CONNECT. A takeover retains the old owner until
// its replacement is saved, so a refusal can leave the old session away.
type connectAttempt struct {
	at              time.Time
	user            string
	expiry          uint32
	keepAlive       uint16
	receiveMaximum  uint16
	clean           bool
	assigned        bool
	capped          bool
	keepAliveCapped bool
	present         bool
	cause           endCause
	ends            bool
	takeover        *connectionState
	will            *store.Will
	oldWill         store.WillInfo
	fireCause       willCause
	fireOldWill     bool
	bodiesReady     bool // takeover's writer reports are in; any prerequisite Will has bytes and a slot
	cancelOldWill   bool
	starts          map[int64]int64 // channel tails at a continuing CONNECT's intake
	pass            *retainedPass
}

// connect applies Check before Order and takeover. Every queued CONNECT has
// a live connection to bound it; replies for pipelined packets wait with it.
func (c *Core) connect(acts []Action, conn *connection.Conn, p *packet.Connect, reason byte, now time.Time) []Action {
	s := c.connections[conn]
	if s.phase != awaitingConnect {
		return acts
	}
	if remote := conn.ProxyAddr(); remote != nil {
		s.accept.Remote = remote // CONNECT publishes the reader's PROXY address
	}
	// Keep the checked identity even when admission refuses this CONNECT.
	// The closure report arrives after its writer has selected the reason.
	s.clientID, s.version, s.accessUser = p.ClientID, p.Version, conn.User()
	if reason != 0 {
		var returnCode311 byte
		if reason == 0x97 {
			returnCode311 = 5 // retained Will admission; a storage refusal maps to 3
		}
		return c.refuse(acts, s, reason, returnCode311, now)
	}
	user := conn.User()
	if user == "" && conn.Access() == nil {
		user = p.UserName
	}
	if conn.AccessFile() == c.access {
		s.access, s.accessUser = conn.Access(), user
	} else {
		c.compileAccess(s, user, p.ClientID)
	}
	a := &connectAttempt{at: now, user: conn.User(), clean: p.CleanStart, assigned: p.ClientID == "", receiveMaximum: p.ReceiveMaximum}
	if !a.clean {
		// Kept filters are not known until the session load completes. Capture
		// their possible new channels now, before later publications enter.
		a.starts = make(map[int64]int64)
		for _, ch := range c.channels {
			if ch.row.ID == 1 || ch.row.Type == store.Latest || ch.startTail {
				a.starts[ch.row.ID] = ch.row.Next
			}
		}
	}
	a.keepAlive = connection.KeepAlive(p.Version, p.KeepAlive, c.limits.MaxKeepalive)
	a.keepAliveCapped = a.keepAlive != p.KeepAlive
	a.expiry = min(p.SessionExpiryInterval, c.maximumExpiry())
	a.capped = a.expiry != p.SessionExpiryInterval
	if p.Version == packet.V311 && !p.CleanStart {
		a.expiry = c.maximumExpiry()
	}
	if s.access.Denied(acl.Persistent) {
		a.expiry = 0
		a.capped = p.SessionExpiryInterval != 0
	}
	s.clientID, s.version, s.phase, s.attempt = p.ClientID, p.Version, connecting, a
	if p.WillFlag {
		w := &p.Will
		a.will = &store.Will{Topic: w.Topic, QoS: w.QoS, Retain: w.Retain,
			Payload: w.Payload, StoredPayload: w.StoredPayload, PayloadSize: w.PayloadSize, Properties: w.StoredProperties}
		for prop := range w.Properties.Views() {
			if prop.ID == packet.WillDelayInterval {
				a.will.Delay = prop.Int
			}
		}
	}
	s.topicAliases = p.TopicAliasMaximum
	s.maximumPacket = p.MaximumPacketSize
	if a.assigned {
		s.clientID = c.assignClientID(now)
	}
	lane := c.lane(s.clientID)
	if r := lane.request; r != nil {
		switch r.kind {
		case readSeek, readKVSize, executeRequest, readIncoming, releaseIncoming:
			if r.conn.phase != connected {
				break
			}
			c.discardPublishes(&r.conn.publishes, now)
			c.cancelPackets(r.conn, now)
			if r.kind == readSeek || r.kind == readKVSize || r.kind == executeRequest {
				c.cancelChannelRequests(r.conn, now)
			} else {
				c.cancelIncoming(r.conn)
			}
			if len(lane.pending) != 0 {
				c.advance(lane)
			}
		}
	}
	c.enqueue(&storeRequest{kind: connectSession, lane: lane, conn: s})
	return acts
}

// unqueueConnect drops an ended or closed waiter without waiting for the
// lane's store answer. Delete keeps the live CONNECTs in order and clears
// the vacated pointer; clearing the attempt avoids another search at Closed.
func (c *Core) unqueueConnect(s *connectionState) {
	if s.attempt == nil {
		return
	}
	lane := c.lanes[s.clientID]
	if lane == nil {
		return
	}
	if r := lane.request; r != nil && r.conn == s {
		// A CONNECT parked before takeover or I/O owns no changed session
		// state. Its ended peer must not outlive its connection permit.
		if r.kind == connectSession && s.attempt.takeover == nil {
			c.cancelPendingRequest(r)
			c.abortConnect(s)
			c.endLane(lane, r)
		}
		return
	}
	if i := slices.IndexFunc(lane.pending, func(r *storeRequest) bool {
		return r.kind == connectSession && r.conn == s
	}); i >= 0 {
		c.cancelPendingRequest(lane.pending[i])
		lane.pending = slices.Delete(lane.pending, i, i+1)
		c.abortConnect(s)
	}
}

// decide connects from either a loaded row or the connected owner's state.
// Deletion and replacement share a group, so a failed transaction preserves
// the old session and never counts an ending that was not saved.
func (c *Core) decide(acts []Action, lane *sessionLane, s *connectionState, before *store.Session,
	subscriptions map[string]store.Subscription, exists bool, now time.Time) []Action {
	a := s.attempt
	durable := exists && before.Expiry != 0
	if exists && before.Left > 0 && before.Expiry != store.NeverExpires && expiryAt(before.Left, before.Expiry) <= a.at.UnixMilli() {
		a.ends, a.cause, exists = true, expired, false
	} else if exists && a.clean {
		a.ends, a.cause = true, cleanStart
	}
	a.present = exists && !a.clean
	if w := a.oldWill; w.Session != 0 {
		// The intake time, never the load or commit time, decides whether a
		// continuing session cancels its delayed Will.
		a.cancelOldWill = a.present && w.Delay != 0 && (!w.Due.Valid || a.at.UnixMilli() < w.Due.Int64)
		a.fireOldWill = !a.cancelOldWill
		a.fireCause = willSessionEnd
		if w.Due.Valid && w.Due.Int64 <= a.at.UnixMilli() {
			a.fireCause = willLost
		}
		if a.takeover != nil {
			a.fireCause = willTakeover
		}
		a.fireCause = c.recoveredCause(w, a.fireCause, false)
	}
	if a.present {
		s.subscriptions = subscriptions
	} else {
		s.incoming = nil // a loaded exchange belongs only to a continuing session
		s.consumers = nil
		s.resuming = nil
		s.recoveryOwners = nil
		s.deliverySequence, s.resumeThrough = 0, 0
	}
	id := before.ID
	if !a.present {
		c.nextSession++
		id = c.nextSession
	}
	s.session = store.Session{ID: id, Client: s.clientID, User: a.user, Protocol: uint8(s.version), Expiry: a.expiry,
		ReceiveMaximum: a.receiveMaximum, KeepAlive: a.keepAlive, CleanStart: a.clean}
	if a.will != nil {
		a.will.Client = s.clientID
		a.will.User = a.user
		s.willBytes = int64(len(a.will.Topic) + max(len(a.will.Payload), a.will.PayloadSize) + len(a.will.Properties) + len(a.will.StoredPayload))
		s.willDelay = a.will.Delay
	}
	var previous int64
	switch {
	case durable:
		previous = before.ID
	case a.oldWill.Session != 0:
		previous = a.oldWill.Session
	case a.takeover != nil && a.takeover.hasHeldPublishes():
		previous = a.takeover.session.ID
	}
	return c.storeConnect(acts, lane, s, previous, now)
}

// storeConnect builds CONNECT's one transaction only after its required
// reads have been applied. A continuing connection-lifetime session keeps its
// inherited exchanges in the store for bounded paging, under the same id.
func (c *Core) storeConnect(acts []Action, lane *sessionLane, s *connectionState, previous int64, now time.Time) []Action {
	a := s.attempt
	if !a.fireOldWill {
		c.allowConnectBodies(s)
	}
	var reach *store.SaveSubscriptions
	if a.present {
		reach = c.syncConsumer(s, now)
	}
	var op, beforeSave store.Write
	if previous != 0 && (!a.present || a.expiry == 0) {
		op = &store.EndSession{ID: previous, KeepHeld: a.present,
			KeepDeliveries: a.present && s.resumeThrough != 0}
	}
	if a.expiry != 0 {
		s.session.Listener, s.session.Remote = s.accept.Listener, s.accept.Remote.String()
		save := &store.SaveSession{Session: s.session, New: previous == 0 || !a.present, Will: a.will, Reach: reach}
		beforeSave, op = op, save
	} else if a.will != nil {
		beforeSave, op = op, &store.SaveWill{Session: s.session.ID, Will: a.will}
	}
	if op == nil {
		return c.grant(acts, s, now)
	}
	r := &storeRequest{kind: saveConnect, lane: lane, conn: s, op: op, before: beforeSave}
	if a.fireOldWill {
		r.willInfo, r.willCause = a.oldWill, a.fireCause
	}
	c.enqueue(r)
	return acts
}

func (c *Core) grant(acts []Action, s *connectionState, now time.Time) []Action {
	a := s.attempt
	if a.ends {
		c.endedCounts[a.cause].Inc()
	}
	if a.cancelOldWill {
		c.willsCancelled.Inc()
	}
	// A successful CONNECT may be answered after its peer has already left.
	// Its preserved rows still need that connection's ordinary departure.
	s.spooled = a.present && a.expiry == 0 && s.resumeThrough != 0
	if s.end.seen {
		s.attempt = nil
		return c.leave(acts, c.lane(s.clientID), s, now)
	}
	if s.phase == closing {
		// The save won the race with the reader's end, but the writer has
		// its final entry. Keep the saved scalars for that end, without a
		// CONNACK or a connected session to resume.
		s.attempt = nil
		return acts
	}
	s.phase = connected
	c.connectionWorkChanged(s)
	c.syncShared(s, now)
	if a.present && a.expiry != 0 && a.takeover != nil {
		for _, u := range s.consumers {
			if before := a.takeover.consumer(u.channel.row.ID); before != nil {
				u.persisted = before.persisted
			}
			u.dirty = u.position != u.persisted
		}
	}
	if a.present {
		for _, u := range s.consumers {
			c.observePositionFloor(u, now)
		}
	}
	if !a.present {
		c.syncConsumer(s, now)
	}
	if a.pass != nil && c.phase < quiescing {
		c.startRetainedPass(s, a.pass, 0)
	}
	c.queueConsumers(s)
	if s.resuming != nil {
		c.initWindow(s)
		c.queueConsumer(s.resuming)
	}
	c.sessions[s.clientID] = s
	c.sessionsByID[s.session.ID] = s
	c.countSession(s.version, 1)
	if len(s.incoming) != 0 {
		ids := make([]uint16, 0, len(s.incoming))
		for id, h := range s.incoming {
			h.quota = uint32(id)
			ids = append(ids, id)
		}
		slices.Sort(ids)
		c.countIncoming(s, int64(len(ids)))
		acts = append(acts, Action{Conn: s.conn, Reply: connection.ReceiveQuota{Identifiers: ids}})
	}
	if a.capped {
		c.capped.Inc()
	}
	ack := &packet.Connack{SessionPresent: a.present, Properties: c.connackProperties(s)}
	acts = append(acts, Action{Conn: s.conn, Reply: ack})
	for _, reply := range s.replies {
		acts = append(acts, Action{Conn: s.conn, Reply: reply})
	}
	// The restored quota and CONNACK precede decisions on pipelined packets.
	// Bind that prefix in arrival order before connected intake can continue.
	acts = c.enterPendingPackets(acts, s, now)
	s.attempt, s.replies = nil, nil
	return acts
}

func (c *Core) refuse(acts []Action, s *connectionState, reason, returnCode311 byte, now time.Time) []Action {
	c.abortConnect(s)
	if s.phase == closing {
		s.attempt = nil
		return acts
	}
	c.unqueueBytes(s, now) // a failed CONNECT can never admit its pipelined body
	c.discardPublishes(&s.publishes, now)
	c.cancelDelivery(s, now) // a refused CONNECT also releases any recovered window state
	c.setHeld(s, 0)
	s.phase, s.attempt, s.replies = refused, nil, nil
	if s.end.seen {
		return c.finish(acts, s, s.end.reason, now)
	}
	c.selectClosure(s, packet.CONNACK, reason, now)
	return append(acts, Action{Conn: s.conn, Reply: &packet.Connack{ReasonCode: reason, ReturnCode311: returnCode311}})
}

func (c *Core) maximumExpiry() uint32 {
	return uint32(c.limits.MaxSessionExpiry / time.Second)
}

// assignClientID uses the event's UTC time and six uniformly random letters
// or digits. Connected clients and pending loads reserve their names; Load
// checks the store before the identifier is acknowledged (MQTT-3.2.2-16).
func (c *Core) assignClientID(at time.Time) string {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	const ceiling = byte(256 - 256%len(alphabet))
	for {
		var suffix [6]byte
		for n := 0; n < len(suffix); {
			if _, err := rand.Read(suffix[n:]); err != nil {
				panic("core: generate assigned client identifier: " + err.Error())
			}
			// Reject the incomplete group of 62, then pack accepted bytes
			// in place. Read again only for characters still missing.
			for _, b := range suffix[n:] {
				if b < ceiling {
					suffix[n] = alphabet[b%byte(len(alphabet))]
					n++
				}
			}
		}
		id := at.UTC().Format("060102150405") + string(suffix[:])
		if c.sessions[id] == nil && c.lanes[id] == nil {
			return id
		}
	}
}

// connackProperties reports selected limits and the QoS the reader accepts.
// MQTT 5 omits Maximum QoS when QoS 2 is supported. The reader's compiled
// feature denials narrow the capabilities advertised on this connection.
func (c *Core) connackProperties(s *connectionState) packet.Properties {
	if s.version != packet.V5 {
		return nil
	}
	a := s.attempt
	values := [9]packet.Property{
		{ID: packet.ReceiveMaximum, Int: c.receiveMaximum},
		{ID: packet.MaximumPacketSize, Int: uint32(c.limits.MaxPacketSize)},
		{ID: packet.TopicAliasMaximum, Int: connection.TopicAliasMaximum},
	}
	props := values[:3]
	if access := s.access; access != nil {
		if access.Denied(acl.QoS2) {
			props = append(props, packet.Property{ID: packet.MaximumQoS, Int: 1})
		}
		if access.Denied(acl.Retained) {
			props = append(props, packet.Property{ID: packet.RetainAvailable})
		}
		if access.Denied(acl.Share) {
			props = append(props, packet.Property{ID: packet.SharedSubscriptionAvailable})
		}
	}
	if a.keepAliveCapped {
		props = append(props, packet.Property{ID: packet.ServerKeepAlive, Int: uint32(a.keepAlive)})
	}
	if a.capped {
		props = append(props, packet.Property{ID: packet.SessionExpiryInterval, Int: a.expiry})
	}
	if a.assigned {
		props = append(props, packet.Property{ID: packet.AssignedClientIdentifier, String: s.clientID})
	}
	properties, err := packet.NewProperties(packet.CONNACK, props)
	if err != nil {
		// Configuration bounds every scalar, and assigned ids are ASCII.
		panic("core: invalid CONNACK properties: " + err.Error())
	}
	return properties
}
