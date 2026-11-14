package core

import (
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/store"
)

// loadBridge opens both directions only after any lost-session replay has
// committed. Both reads follow earlier links' bookkeeping in the bridge lane.
func (c *Core) loadBridge(s *connectionState) {
	b := s.bridge
	c.enqueue(&storeRequest{kind: loadBridgeOutbound, conn: s, lane: &b.lane, op: &store.LoadBridgeOutbound{Bridge: b.id}})
	c.enqueue(&storeRequest{kind: loadBridgeHeld, conn: s, lane: &b.lane, op: &store.LoadBridgeHeld{Bridge: b.id}})
}

// queueBridgeReplayMark retains one dirty mark per bridge until storage
// confirms it. Neither ending a link nor beginning a stop removes its owner.
func (c *Core) queueBridgeReplayMark(b *bridgeState) {
	if b.replayMark == nil && !b.replayMarkSaving {
		b.replayMark = c.bridgeReplayMarks.PushBack(b)
	}
}

// saveBridgeReplayMarks snapshots only registered dirty marks. Initial
// CONNACKs, ordinary commits and the stop drain all use the same write path.
// A refused request is discarded; its answer restores the bridge's dirty mark.
func (c *Core) saveBridgeReplayMarks() {
	if c.failure != nil {
		return
	}
	for e := c.bridgeReplayMarks.Front(); e != nil; e = c.bridgeReplayMarks.Front() {
		b := e.Value.(*bridgeState)
		c.bridgeReplayMarks.Remove(e)
		b.replayMark, b.replayMarkSaving = nil, true
		c.enqueue(&storeRequest{kind: saveBridgeReplay, lane: &b.lane,
			op: &store.BridgeReplay{Bridge: b.id, Owed: true}})
	}
}

// savedBridgeReplay is the common durable gate for both directions. Mark
// persistence belongs to the bridge even after the requesting link has ended;
// that link's ordered held read can start replay only after this commit.
func (c *Core) savedBridgeReplay(acts []Action, r *storeRequest, err error, now time.Time) []Action {
	b := r.lane.bridge
	mark := r.op.(*store.BridgeReplay).Owed
	s := r.conn
	if mark {
		b.replayMarkSaving = false
		if err != nil {
			c.queueBridgeReplayMark(b)
		}
		s = b.link
	}
	if err != nil {
		if s != nil {
			acts = c.finish(acts, s, 0, now)
		}
		return acts
	}
	if mark || s.end.seen || s.phase != connected || b.link != s || c.phase >= quiescing {
		return acts
	}
	b.replayOwed = false
	c.loadBridge(s)
	return acts
}

// loadedBridgeHeld publishes ordinary restoration through the reader's quota
// channel. During replay, the bridge owns its ordered moves instead; nothing
// on the new link except PINGRESP can pass the reader's restoration gate
// before completion.
func (c *Core) loadedBridgeHeld(acts []Action, r *storeRequest) []Action {
	s := r.conn
	b := s.bridge
	if s.end.seen || s.phase != connected || b.link != s || c.phase >= quiescing {
		return acts
	}
	held := r.op.(*store.LoadBridgeHeld).Held
	if b.replayOwed {
		b.replayHeld = held
		return c.replayBridgeHeld(acts, s)
	}
	c.restoreIncoming(s, held)
	c.countIncoming(s, int64(len(held)))
	ids := make([]uint16, len(held)) // non-nil also seals an empty restoration
	for i, id := range held {
		ids[i] = id.PacketID
	}
	return append(acts, Action{Conn: s.conn, Reply: connection.ReceiveQuota{Identifiers: ids}})
}

// replayBridgeHeld shares PUBREL's channel entry and stored move, one record
// at a time in arrival order. The last step resets outbound preparations and
// clears the durable mark together; only its answer can open either direction.
func (c *Core) replayBridgeHeld(acts []Action, s *connectionState) []Action {
	b := s.bridge
	if s.end.seen || s.phase != connected || b.link != s || c.phase >= quiescing {
		return acts
	}
	if len(b.replayHeld) != 0 {
		id := b.replayHeld[0]
		b.replayHeld = b.replayHeld[1:]
		h := &incomingPublish{channel: c.channel(id.Channel)}
		c.enqueueIncoming(s, id.PacketID, nil, h)
	} else {
		b.replayHeld = nil
		c.enqueue(&storeRequest{kind: saveBridgeReplay, conn: s, lane: &b.lane, op: &store.BridgeReplay{Bridge: b.id}})
	}
	return acts
}

// releaseStoredReply uses the existing bounded, ordered completion slots.
// The first store-confirmed reply sets the batch deadline; later results
// never extend it. Refusals without a change use independent replies instead.
func (c *Core) releaseStoredReply(s *connectionState, now time.Time) bool {
	b := s.bridge
	if b == nil || b.releasingAcks || c.phase >= quiescing || b.options.Bridge.AckInterval == 0 {
		return true
	}
	if b.ackDeadline.at.IsZero() {
		c.bridgeAcks.set(&b.ackDeadline, now.Add(b.options.Bridge.AckInterval))
	}
	return false
}
