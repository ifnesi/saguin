package core

import (
	"container/list"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

const (
	publicationBytes   = 4 << 20 // topic, original payload and stored property bytes
	channelBatches     = 4
	publicationBatches = 8
	groupChanges       = 512
)

// Group is one transaction, with its writes executed in order. Members are
// never groups; CONNECT's ending and replacement are consecutive members.
type Group struct {
	Writes []store.Write
}

// WriteTx runs every member, combining independent delivery changes before
// the next dependent member. The store owns commit and rollback of the group.
func (g *Group) WriteTx(tx *store.Tx) error {
	for remaining := g.Writes; len(remaining) != 0; {
		w := remaining[0]
		if _, delivery := w.(*store.DeliveryChanges); delivery {
			n, err := tx.WriteDeliveryChanges(remaining)
			if err != nil {
				return err
			}
			remaining = remaining[n:]
			continue
		}
		if err := w.WriteTx(tx); err != nil {
			return err
		}
		remaining = remaining[1:]
	}
	return nil
}

// Records returns the number of message records carried by the transaction.
func (g *Group) Records() int {
	n := 0
	for _, w := range g.Writes {
		n += w.Records()
	}
	return n
}

type commitCause uint8

const (
	collecting commitCause = iota // no event has made the group due yet
	commitEvent
	commitInterval
	commitMaxRecords
	commitBarrier
	commitStop
	commitCauses
)

// writeGroup keeps the core's accounting beside the transaction. Requests
// retain their existing owners and batches; the slice lets an answer apply
// each result in member order.
type writeGroup struct {
	waitRequests list.List
	Group
	initial        [2]store.Write // the usual batch, or CONNECT's ending and save
	members        []*storeRequest
	initialMembers [1]*storeRequest // the usual single-request group needs no separate allocation
	records        int
	bytes          int64 // original message bytes, independently of memory reservations
	changes        int   // non-publication writes, including a request's before write
	cause          commitCause
	operation      store.Write // a sync or checkpoint uses the same ordered write slot alone
	applied        bool        // every member's result has been applied by the core
}

func (c *Core) dueGroup(cause commitCause) {
	if g := c.collecting; g != nil {
		if g.cause == collecting {
			g.cause = cause
		}
		c.commitAt = time.Time{}
	}
}

// sealGroup freezes both membership and the channel batch, even if the
// previous transaction still owns the write slot.
func (c *Core) sealGroup(cause commitCause) {
	if c.collecting == nil {
		return
	}
	c.dueGroup(cause)
	g := c.collecting
	c.collecting = nil
	c.sealed = append(c.sealed, g)
}

func (c *Core) collect(r *storeRequest, now time.Time) {
	if r.kind == syncStore || r.kind == foldSnapshot {
		c.sealGroup(commitEvent)
	}
	w := r.op.(store.Write)
	n := w.Records()
	changes := r.publishes.wills
	if n == 0 && changes == 0 {
		changes++
	}
	if r.before != nil {
		changes++
	}
	if !c.groupRoom(n, r.publishes.messageBytes, changes) {
		c.sealGroup(commitMaxRecords)
	}
	if c.collecting == nil {
		// The first request supplies the group and its initial member space.
		c.collecting = &r.transaction
		c.collecting.Writes = c.collecting.initial[:0]
		c.collecting.members = c.collecting.initialMembers[:0]
	}
	g := c.collecting
	g.members = append(g.members, r)
	if len(g.members) == 2 {
		clear(g.initialMembers[:])
	}
	r.group = g
	if r.lane != nil || r.publishes.wills != 0 {
		// A Will publication carries its session transition in this same
		// transaction, even though the batch itself owns a channel lane.
		c.sessionWrites = g
	}
	if r.before != nil {
		g.Writes = append(g.Writes, r.before)
	}
	g.Writes = append(g.Writes, w)
	g.records += n
	g.bytes += r.publishes.messageBytes
	g.changes += changes
	switch {
	case c.phase >= stopping, c.commitEvery == 0, r.kind != savePublishes:
		c.dueGroup(commitEvent)
	case g.cause == collecting && c.commitAt.IsZero():
		c.commitAt = now.Add(c.commitEvery)
	}
	c.limitGroup()
	if r.kind == syncStore || r.kind == foldSnapshot {
		g.operation = w
		if r.kind == foldSnapshot {
			// Keep the fold and its copy's view one ordered boundary. All
			// other decision work waits until this continuation can open.
			c.orderedRead = r
		}
		c.sealGroup(commitEvent)
	}
}

// groupRoom tests the next indivisible write against all group bounds. An
// empty group admits one otherwise valid publication over the byte bound.
func (c *Core) groupRoom(records int, bytes int64, changes int) bool {
	g := c.collecting
	return g == nil || (records <= c.commitMaximum-g.records &&
		bytes <= publicationBytes-g.bytes && changes <= groupChanges-g.changes)
}

// limitGroup also runs when a publication extends the collecting channel
// batch. Its count is maintained on append, without another publication pass.
func (c *Core) limitGroup() {
	g := c.collecting
	if g.records >= c.commitMaximum || g.bytes >= publicationBytes || g.changes >= groupChanges {
		c.sealGroup(commitMaxRecords)
	}
}

func (c *Core) dispatchGroup(acts []Action) []Action {
	if c.outstanding != nil || c.freeStoreCredit() == 0 || c.unconfirmed != 0 {
		return acts
	}
	if len(c.sealed) == 0 && c.collecting != nil && c.collecting.cause != collecting {
		c.sealGroup(c.collecting.cause)
	}
	if len(c.sealed) == 0 {
		return acts
	}
	g := c.sealed[0]
	c.sealed = slices.Delete(c.sealed, 0, 1)
	c.outstanding = g
	return append(acts, Action{Write: g.write()})
}

// write selects the operation carried by this place in the single write lane.
func (g *writeGroup) write() store.Write {
	if g.operation != nil {
		return g.operation
	}
	return &g.Group
}

// discardGroup releases unsent work after a fatal store failure. Outstanding
// members retain ownership and charges until their transaction answers.
func (c *Core) discardGroup(g *writeGroup, now time.Time) {
	for _, r := range g.members {
		c.discardWrite(r, now)
	}
	clear(g.members)
	g.members = g.members[:0]
	clear(g.Writes)
	g.Writes = g.Writes[:0]
	clear(g.initial[:])
}

// discardWrite returns ownership of work that can no longer reach the store.
// Dispatched members still belong to the store until their real answer.
func (c *Core) discardWrite(r *storeRequest, now time.Time) {
	if r.discarded {
		return
	}
	r.discarded = true
	c.unparkRequest(r)
	switch r.kind {
	case expireRetained:
		if r.channel.deadLetter != nil {
			c.releaseRetentionPage(r)
		}
	case executeRequest, readSeek:
		c.cancelChannelRequest(r, now)
	case saveQueueChange:
		c.savedQueueChange(nil, r, c.failure, now)
	case saveQueueRetries:
		c.savedQueueRetries(r, c.failure, now)
	case saveSeek:
		c.savedSeek(nil, r, c.failure, now)
	case releasePublishes, releaseIncoming, readIncoming:
		c.releaseBatch(&r.publishes)
	case savePublishes:
		c.releaseSharedRequest(r)
		c.retained.reserved -= r.publishes.reservation
		r.channel.reserved -= r.publishes.channelReservation
		c.releaseBatch(&r.publishes)
		c.discardPublishes(&r.publishes.publications, now)
	case readPublishCharges:
		c.releaseSharedRequest(r)
		c.releaseBatch(&r.publishes)
		c.discardPublishes(&r.publishes.publications, now)
	case fireWill:
		c.unqueueMemory(&r.memory, now)
	case readWill:
		c.bodyCount--
		c.batchCapacityChanged()
		c.returnBytes(r.publishes.bytes, now)
		r.publishes.bytes = 0
		r.op.(*store.LoadWill).Will = store.Will{}
	case saveDeliveryChanges:
		c.savedDeliveryChanges(nil, r, c.failure, now)
	case endOrphanShared:
		c.completedSharedCleanup(r, c.failure, now)
	case saveSubscriptions:
		c.savedSubscriptions(nil, r, c.failure, now)
	}
}
