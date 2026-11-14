package core

import (
	"bytes"
	"cmp"
	"container/heap"
	"slices"
)

const saveDependencyBoundary = 4

// deliveryBoundary uses container/heap's indexed removal to maintain the
// earliest outstanding offset or required cursor boundary. Recovery follows
// sending sequence, so a list's first entry need not be the earliest record. Membership
// changes on selection, successful storage, removal requests and release,
// never on a scan for the next position. Every membership is a bounded window entry.
type deliveryBoundary struct {
	items []*outgoing
	kind  int
}

func (q *deliveryBoundary) Len() int { return len(q.items) }
func (q *deliveryBoundary) Less(i, j int) bool {
	a, b := q.items[i], q.items[j]
	first, second := a.offset, b.offset
	if q.kind == saveDependencyBoundary {
		first, second = a.saveAfter, b.saveAfter
	}
	if first != second {
		return first < second
	}
	return a.order < b.order
}

func (q *deliveryBoundary) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	q.items[i].boundary[q.kind], q.items[j].boundary[q.kind] = i+1, j+1
}

func (q *deliveryBoundary) Push(value any) {
	d := value.(*outgoing)
	q.items = append(q.items, d)
	d.boundary[q.kind] = len(q.items)
}

func (q *deliveryBoundary) Pop() any {
	i := len(q.items) - 1
	d := q.items[i]
	q.items[i] = nil
	q.items = q.items[:i]
	d.boundary[q.kind] = 0
	return d
}

func (u *consumerState) trackDelivery(d *outgoing) {
	if d.initial || d.exchange.NoMove {
		return
	}
	heap.Push(&u.boundaries[0], d)
	if u.conn.stored && !d.saved {
		u.boundaries[1].kind = 1
		heap.Push(&u.boundaries[1], d)
		if d.exchange.QoS == 0 && u.sharedCursor == nil && !u.service {
			d.qos0Topic, d.qos0Pending.Value = d.packet.Topic, d
			s := u.conn
			i, found := s.qos0TopicIndex(u, d.qos0Topic)
			switch {
			case u.qos0Pending == nil:
				u.qos0Pending = &d.qos0Pending
			case found:
				s.qos0Topics[i].qos0Pending.Link(&d.qos0Pending)
			default:
				u.qos0Pending.Prev().Link(&d.qos0Pending)
			}
			if found {
				s.qos0Topics[i] = d
			} else {
				s.qos0Topics = slices.Insert(s.qos0Topics, i, d)
			}
		}
	}
}

func (u *consumerState) untrackDelivery(d *outgoing) {
	if d.qos0Pending.Value != nil {
		s := u.conn
		i, _ := s.qos0TopicIndex(u, d.qos0Topic)
		if s.qos0Topics[i] == d {
			previous := d.qos0Pending.Prev().Value.(*outgoing)
			if previous != d && bytes.Equal(previous.qos0Topic, d.qos0Topic) {
				s.qos0Topics[i] = previous
			} else {
				s.qos0Topics = slices.Delete(s.qos0Topics, i, i+1)
			}
		}
		if u.qos0Pending == &d.qos0Pending {
			u.qos0Pending = d.qos0Pending.Next()
			if u.qos0Pending == &d.qos0Pending {
				u.qos0Pending = nil
			}
		}
		d.qos0Pending.Prev().Unlink(1)
		d.qos0Pending.Value, d.qos0Topic = nil, nil
	}
	for i, index := range d.boundary[:2] {
		if index != 0 {
			heap.Remove(&u.boundaries[i], index-1)
		}
	}
}

// qos0TopicIndex borrows the slot's immutable topic view for binary search.
// Each owner's fresh QoS 0 entries arrive in offset order and stay adjacent in
// its ring, so one indexed tail per topic also identifies the greatest offset.
// The index's backing array is reserved with the fixed connection window.
func (s *connectionState) qos0TopicIndex(u *consumerState, name []byte) (int, bool) {
	return slices.BinarySearchFunc(s.qos0Topics, name, func(d *outgoing, name []byte) int {
		if order := cmp.Compare(d.consumer.registration, u.registration); order != 0 {
			return order
		}
		return bytes.Compare(d.qos0Topic, name)
	})
}

// preparationAfter captures both settled progress and an earlier same-topic
// QoS 0 obligation without visiting unrelated slots. Writing or skipping releases
// the indexed slot; progress must still commit before or with the preparation.
func (u *consumerState) preparationAfter(d *outgoing) int64 {
	if d.initial || u.sharedCursor != nil || u.service {
		return 0
	}
	after := u.position
	if i, found := u.conn.qos0TopicIndex(u, d.packet.Topic); found {
		after = max(after, u.conn.qos0Topics[i].offset+1)
	}
	return after
}

// savesThrough includes uncommitted progress only when the next snapshot can
// save it together with the preparation. An independent progress write may
// still fail, so its proposed boundary cannot release a dependent save.
func (u *consumerState) savesThrough() int64 {
	if u.writing == nil {
		return max(u.persisted, u.position)
	}
	return u.persisted
}

func (u *consumerState) blockedSaveReady() bool {
	return u.blockedSaves.Len() != 0 && u.blockedSaves.items[0].saveAfter <= u.savesThrough()
}

// addPendingSave leaves unrelated preparations on their ordinary FIFO. Only
// entries needing cursor progress use the shared indexed heap, within the window.
func (u *consumerState) addPendingSave(d *outgoing) {
	if d.saveAfter > u.persisted {
		u.blockedSaves.kind = saveDependencyBoundary
		heap.Push(&u.blockedSaves, d)
		return
	}
	d.pendingSave = u.pendingSaves.PushBack(d)
}

// releaseBlockedSaves is called only while taking a save snapshot, so progress
// used here is either committed already or included in that same transaction.
func (u *consumerState) releaseBlockedSaves() {
	through := u.savesThrough()
	for u.blockedSaves.Len() != 0 && u.blockedSaves.items[0].saveAfter <= through {
		d := heap.Pop(&u.blockedSaves).(*outgoing)
		d.pendingSave = u.pendingSaves.PushBack(d)
	}
}
