package core

import (
	"container/heap"
	"slices"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// joinConsumerRead reserves each participant's one read slot. The leader's
// reusable request remains owned until the common answer, even if its own
// subscription or connection has meanwhile ended.
func (c *Core) joinConsumerRead(r *storeRequest, u *consumerState) {
	if u.conn != nil {
		u.read.revision = u.conn.subscriptionRevision
	}
	u.request = r
	if u.conn != nil {
		u.conn.deliveryRequests++
	} else {
		c.claimShared(u.share, r)
	}
	r.consumers = append(r.consumers, u)
}

func metadataBounds(u *consumerState) (from, before int64, retained bool) {
	if u.conn == nil && u.share.returned.Channel != 0 {
		return u.share.returned.Offset, u.share.returned.Offset + 1, false
	}
	if u.pass != nil {
		return u.pass.scan, u.pass.before, true
	}
	return u.scan, u.available, false
}

// metadataVisitLimit bounds the page by useful selections. Skipped rows still
// advance its scan; matching rows fill available windows within channel fairness.
func (c *Core) metadataVisitLimit(u *consumerState) int {
	if u.conn == nil {
		if u.share.returned.Channel != 0 {
			return 1
		}
		return min(store.MetadataPage, max(1, u.sharedAvailable))
	}
	if len(u.conn.consumers) > 1 {
		return 1
	}
	room := c.preparationRoom(u)
	if u.pass != nil {
		room = c.retainedRoom(u)
	}
	return min(store.MetadataPage, max(1, room))
}

// readMetadataPage joins a channel page whose range covers this scan. Ordinary
// scans may join an outstanding read; retained scans share only the same
// admission event, before dispatch, so every participant keeps its barrier.
func (c *Core) readMetadataPage(u *consumerState) bool {
	from, before, retained := metadataBounds(u)
	limit := c.metadataVisitLimit(u)
	for _, r := range u.channel.pages {
		op := &r.metadata
		if op.Retained != retained || op.Jobs != (u.conn == nil && u.share.queue != nil) {
			continue
		}
		if r.admitted == c.wakeTicket {
			// This event's page has not dispatched. Its range serves the
			// whole ready cohort, rather than whichever cursor woke first.
			op.From, op.Before = min(op.From, from), max(op.Before, before)
			op.Limit = max(op.Limit, limit)
			r.consumer.metadata = *op // this event's page is still core-owned
		} else if retained || from < op.From || from >= op.Before {
			// Wait for the channel's existing page instead of rescanning its
			// rows concurrently for a consumer whose cursor does not fit.
			heap.Push(&u.channel.metadataWait, u)
			return false
		}
		c.joinMetadataCheck(r, u, retained)
		c.joinConsumerRead(r, u)
		u.read.tail = before
		if u.conn != nil {
			u.conn.scanning = u
		}
		return true
	}
	if c.metadataReads == c.metadataLimit {
		heap.Push(&c.deliveryDeferred, u)
		return false
	}
	u.metadata = store.ReadRecordMetadata{Channel: u.channel.row.ID, From: from,
		Before: before, Retained: retained, Jobs: u.conn == nil && u.share.queue != nil, Limit: limit}
	r := &u.read
	r.metadata = u.metadata
	r.kind, r.op, r.admitted = readDeliveryMetadata, &u.metadata, c.wakeTicket
	c.joinMetadataCheck(r, u, retained)
	c.joinConsumerRead(r, u)
	u.read.tail = before
	u.channel.pages = append(u.channel.pages, r)
	c.metadataReads++
	c.enqueue(r)
	if u.conn != nil {
		u.conn.scanning = u
	}
	return true
}

// joinMetadataCheck keeps recipient dispositions out of the shared metadata.
// The core owns this cohort until the page returns, independently of the
// immutable metadata operation already running on a reader.
func (c *Core) joinMetadataCheck(r *storeRequest, u *consumerState, retained bool) {
	u.read.metadataCheck = 0
	if retained {
		return
	}
	var check store.MetadataDisposition
	switch {
	case u.conn == nil && u.share.queue != nil:
		return
	case u.conn == nil:
		check.Share = u.share.id
		u.read.sharedRevision = u.share.returnRevision
	case u.markersBefore != 0:
		check.Session = u.conn.session.ID
	default:
		return
	}
	for i := range r.dispositions.Checks {
		if r.dispositions.Checks[i].Session == check.Session && r.dispositions.Checks[i].Share == check.Share {
			u.read.metadataCheck = i + 1
			return
		}
	}
	r.dispositions.Checks = append(r.dispositions.Checks, check)
	u.read.metadataCheck = len(r.dispositions.Checks)
}

func (c *Core) newPayloadRead(u *consumerState) *storeRequest {
	r := &u.read
	r.kind, r.op = readDeliveryPayload, &r.payloads
	u.channel.loading = r
	c.enqueue(r)
	return r
}

func (c *Core) metadataCurrent(r *storeRequest) bool {
	for _, u := range r.consumers {
		if c.consumerActive(u.conn, u) && ((u.conn == nil && u.read.sharedRevision == u.share.returnRevision) ||
			(u.conn != nil && u.read.revision == u.conn.subscriptionRevision)) {
			return true
		}
	}
	return false
}

// finishMetadataRead releases the single page credit and all participant
// read slots. A superseded undispatched page follows this same path, without
// selecting anything or establishing a read view.
func (c *Core) finishMetadataRead(r *storeRequest) []*consumerState {
	c.releaseRequestOrder(r)
	switch op := r.op.(type) {
	case *store.ReadRecordMetadata:
		op.Rows = nil
	case *store.ReadMetadataDispositions:
		op.Page.Rows = nil
	}
	r.dispositions = store.ReadMetadataDispositions{}
	ch := r.consumer.channel
	ch.pages = slices.DeleteFunc(ch.pages, func(page *storeRequest) bool { return page == r })
	c.returnMetadataCredit()
	for ch.metadataWait.Len() != 0 {
		c.queueConsumer(ch.metadataWait.peek())
	}
	return c.finishConsumerRead(r)
}

func (c *Core) finishConsumerRead(r *storeRequest) []*consumerState {
	owners := r.consumers
	r.consumers = nil
	for _, u := range owners {
		c.finishConsumerRequest(u)
	}
	return owners
}

// finishConsumerRequest returns the consumer's request lane and wakes work
// held behind it, including pre-stop bookkeeping when a read is discarded.
func (c *Core) finishConsumerRequest(u *consumerState) {
	r := u.request
	u.request = nil
	if u.conn == nil {
		c.finishSharedRequest(u.share, r)
		return
	}
	if u.conn.scanning == u {
		u.conn.scanning = nil
	}
	u.conn.deliveryRequests--
	c.connectionWorkChanged(u.conn)
	c.queueConsumer(u)
}

func (c *Core) readDeliveryMetadata(acts []Action, r *storeRequest, now time.Time) []Action {
	var op *store.ReadRecordMetadata
	switch read := r.op.(type) {
	case *store.ReadRecordMetadata:
		op = read
		if len(op.Rows) != 0 && len(r.dispositions.Checks) != 0 && c.metadataCurrent(r) {
			// The page has answered: freeze its covered cohort and check only
			// their dispositions. No second metadata page or credit is needed.
			ch := r.consumer.channel
			ch.pages = slices.DeleteFunc(ch.pages, func(page *storeRequest) bool { return page == r })
			r.dispositions.Page = op
			r.op = &r.dispositions
			c.enqueue(r)
			return acts
		}
	case *store.ReadMetadataDispositions:
		op = read.Page
	}
	rows, checks := op.Rows, r.dispositions.Checks
	owners := c.finishMetadataRead(r) // no page waits for a full delivery window
	for _, u := range owners {
		if c.consumerActive(u.conn, u) && (u.conn == nil || u.read.revision == u.conn.subscriptionRevision) {
			var settled []int64
			if u.read.metadataCheck != 0 {
				settled = checks[u.read.metadataCheck-1].Settled
			}
			for i := range rows {
				_, rows[i].Settled = slices.BinarySearch(settled, rows[i].Offset)
			}
			if u.conn == nil {
				if u.read.sharedRevision != u.share.returnRevision || !u.share.scanTurn(u.channel.row.ID) {
					c.queueConsumer(u)
					continue
				}
				acts = c.selectShared(acts, u, op, rows, now)
			} else {
				acts = c.selectMetadata(acts, u, op, rows, now)
			}
		}
		c.queueConsumer(u)
		if u.conn != nil {
			c.queueConsumers(u.conn)
		}
	}
	return acts
}
