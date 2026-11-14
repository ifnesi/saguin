package core

import (
	"cmp"
	"container/heap"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// history indexes retained rows by their complete keys and keeps replacement
// candidates in a standard heap. Repeated observations update the existing
// row in place; only answers sort rows and construct display values.
type history[K comparable, V any] struct {
	rows    map[K]*historyRow[K, V]
	victims historyHeap[K, V]
	beyond  uint64
}

type historyRow[K comparable, V any] struct {
	key   K
	value V
	count uint64
	seen  time.Time
	index int
}

type historyHeap[K comparable, V any] struct {
	items   []*historyRow[K, V]
	compare func(a, b *historyRow[K, V]) int
}

func (h *historyHeap[K, V]) Len() int { return len(h.items) }
func (h *historyHeap[K, V]) Less(i, j int) bool {
	return h.compare(h.items[i], h.items[j]) < 0
}
func (h *historyHeap[K, V]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index, h.items[j].index = i, j
}
func (h *historyHeap[K, V]) Push(value any) {
	r := value.(*historyRow[K, V])
	r.index = len(h.items)
	h.items = append(h.items, r)
}
func (h *historyHeap[K, V]) Pop() any {
	i := len(h.items) - 1
	r := h.items[i]
	h.items[i], h.items = nil, h.items[:i]
	return r
}

func (h *history[K, V]) add(key K, value V, now time.Time, compare func(a, b *historyRow[K, V]) int) {
	if h.rows == nil {
		h.rows = make(map[K]*historyRow[K, V], views.HistoryLimit)
		h.victims = historyHeap[K, V]{items: make([]*historyRow[K, V], 0, views.HistoryLimit), compare: compare}
	}
	r := &historyRow[K, V]{key: key, value: value, count: 1, seen: now}
	h.rows[key] = r
	heap.Push(&h.victims, r)
}

func (h *history[K, V]) evict() {
	r := heap.Pop(&h.victims).(*historyRow[K, V])
	delete(h.rows, r.key)
	h.beyond += r.count
}

func (h *history[K, V]) counts() views.HistoryCounts {
	n := len(h.rows)
	return views.HistoryCounts{Total: n, Returned: n, Tracked: n, Beyond: h.beyond}
}

func (h *history[K, V]) sorted(compare func(a, b *historyRow[K, V]) int) []*historyRow[K, V] {
	rows := slices.Clone(h.victims.items)
	slices.SortFunc(rows, compare)
	return rows
}

type refusal struct {
	user, reason string
	listener     string
	connection   uint64
	decision     uint64
	source       connection.Kind
}

// closureMoment belongs to the connection, not the bounded history. The
// final report confirms the counted outcome, without moving this decision
// past later ones while the writer finishes its socket cleanup.
type closureMoment struct {
	at     time.Time
	reason string
	order  uint64
	cause  connection.Closure
}

func (c *Core) selectClosure(s *connectionState, typ packet.Type, code byte, now time.Time) {
	if s.closure.reason == "" {
		s.closure = closureMoment{at: now, reason: metrics.ReasonLabel(typ, code), order: c.wakeTicket,
			cause: connection.MQTTClosure(typ, code)}
	}
}

// compareRefusalSources uses R2's stable connection key for simultaneous
// observations. A reader's end precedes its writer's close on one connection;
// delayed report arrival never breaks a tie between different observations.
func compareRefusalSources(a, b refusal) int {
	if a.decision != 0 && b.decision != 0 {
		if order := cmp.Compare(a.decision, b.decision); order != 0 {
			return order
		}
	}
	return cmp.Or(strings.Compare(a.listener, b.listener), cmp.Compare(a.connection, b.connection), cmp.Compare(a.source, b.source))
}

func compareRefusalVictims(a, b *historyRow[string, refusal]) int {
	if (a.count == 1) != (b.count == 1) {
		if a.count == 1 {
			return -1
		}
		return 1
	}
	return cmp.Or(a.seen.Compare(b.seen), strings.Compare(a.key, b.key))
}

// recordClosure consumes the sender's counted event, never a metric value.
// A writer that could not build a refusing CONNACK reports its replacement
// closure here, so the unsent refusal cannot become a second observation.
// Core-selected reasons use their decision moment. Connection-local failures
// use the moment their owner selected them, before its last write or cleanup.
// Delayed reports still add their counts but cannot replace newer fields.
func (c *Core) recordClosure(ev connection.Event) {
	reason := ev.Closure.Label()
	if reason == "" {
		return
	}
	s := c.connections[ev.Conn]
	h := &c.refusals
	at := ev.Conn.ClosureAt(ev.Kind)
	value := refusal{user: views.Text(ev.Conn.ConnectUser()), reason: reason, listener: s.accept.Listener, connection: s.order, source: ev.Kind}
	key := s.clientID
	if len(key) > c.limits.MaxClientIDLength {
		key = views.Text(key)
	}
	if s.closure.reason == reason {
		at, value.decision = s.closure.at, s.closure.order
	}
	if r := h.rows[key]; r != nil {
		if order := at.Compare(r.seen); order > 0 || (order == 0 && compareRefusalSources(value, r.value) > 0) {
			value.user = strings.Clone(value.user)
			r.value, r.seen = value, at
		}
		r.count++
		heap.Fix(&h.victims, r.index)
		return
	}
	if len(h.rows) == views.HistoryLimit {
		if h.victims.items[0].count != 1 {
			h.beyond++
			return
		}
		h.evict()
	}
	// A sliced prefix must not retain the peer's complete CONNECT string.
	if len(key) != len(s.clientID) {
		key = strings.Clone(key)
	}
	value.user = strings.Clone(value.user)
	h.add(key, value, at, compareRefusalVictims)
}

func (c *Core) refusedView() views.Refused {
	h := &c.refusals
	rows := h.sorted(func(a, b *historyRow[string, refusal]) int {
		return cmp.Or(cmp.Compare(b.count, a.count), strings.Compare(a.key, b.key))
	})
	result := views.Refused{HistoryCounts: h.counts(), Clients: make([]views.RefusedClient, 0, len(rows))}
	for _, r := range rows {
		result.Clients = append(result.Clients, views.RefusedClient{ClientID: views.Text(r.key),
			User: views.Text(r.value.user), Reason: r.value.reason, Count: r.count, LastSeen: views.Time(r.seen)})
	}
	return result
}

type positionLossKey struct {
	name, channel string
	bridge        bool
}

func compareLossKeys(a, b positionLossKey) int {
	return cmp.Or(views.CompareReaders(views.ReaderKey{Name: a.name, Bridge: a.bridge},
		views.ReaderKey{Name: b.name, Bridge: b.bridge}), strings.Compare(a.channel, b.channel))
}

type positionLoss struct {
	kind            string
	missed          uint64
	position, floor int64
	known           bool
}

func compareLossVictims(a, b *historyRow[positionLossKey, positionLoss]) int {
	if a.value.known != b.value.known {
		if !a.value.known {
			return -1
		}
		return 1
	}
	if a.value.known {
		if order := cmp.Compare(a.value.missed, b.value.missed); order != 0 {
			return order
		}
	}
	return cmp.Or(a.seen.Compare(b.seen), compareLossKeys(a.key, b.key))
}

func (c *Core) recordPositionLoss(key positionLossKey, value positionLoss, now time.Time) {
	h := &c.positionLosses
	if r := h.rows[key]; r != nil {
		value.missed += r.value.missed
		value.known = value.known && r.value.known
		r.value, r.seen = value, now
		r.count++
		heap.Fix(&h.victims, r.index)
		return
	}
	if len(h.rows) == views.HistoryLimit {
		victim := h.victims.items[0]
		if !value.known || (victim.value.known && value.missed <= victim.value.missed) {
			h.beyond++
			return
		}
		h.evict()
	}
	h.add(key, value, now, compareLossVictims)
}

func (c *Core) positionLostView() views.PositionLost {
	h := &c.positionLosses
	rows := h.sorted(func(a, b *historyRow[positionLossKey, positionLoss]) int {
		if a.value.known != b.value.known {
			if a.value.known {
				return -1
			}
			return 1
		}
		order := cmp.Compare(b.count, a.count)
		if a.value.known {
			order = cmp.Compare(b.value.missed, a.value.missed)
		}
		return cmp.Or(order, compareLossKeys(a.key, b.key))
	})
	result := views.PositionLost{HistoryCounts: h.counts(), Readers: make([]views.LostReader, 0, len(rows))}
	for _, r := range rows {
		reader := "mqtt:" + views.Text(r.key.name)
		if r.key.bridge {
			reader = "bridge:" + r.key.name
		}
		row := views.LostReader{Reader: reader, Channel: r.key.channel, Kind: r.value.kind, Count: r.count,
			LastPosition: r.value.position, LastFloor: r.value.floor, LastSeen: views.Time(r.seen)}
		if r.value.known {
			missed := r.value.missed
			row.RecordsMissed = &missed
		}
		result.Readers = append(result.Readers, row)
	}
	return result
}

// retentionConsumer preserves the position at the write boundary, before
// later acknowledgements or subscription changes can move the live owner.
// A disconnected bridge's exact stored scan comes back in the store answer.
type retentionConsumer struct {
	owner    *consumerState
	bridge   *bridgeState
	position int64
}

// recordRetentionLosses records only the successful deletion's exact counts.
// Bridge-local scratch adds all of a bridge's rules before recording its one
// observation. The touched list visits only bridges represented in this answer
// and needs no allocation or scan of the history on repeated losses.
func (c *Core) recordRetentionLosses(r *storeRequest, now time.Time) {
	op := r.op.(*store.Retention)
	bridges := c.retentionBridges[:0]
	for i, captured := range r.retentionConsumers {
		missed := op.Consumers[i].Missed
		if u := captured.owner; u != nil {
			u.retentionMissed += missed
		}
		if missed == 0 {
			continue
		}
		value := positionLoss{kind: "consumer", known: true, missed: missed,
			position: captured.position, floor: op.Floor}
		if b := captured.bridge; b != nil {
			value.kind = "bridge"
			if captured.owner == nil {
				value.position = op.Consumers[i].Scan
			}
			if b.retentionLoss.missed == 0 {
				b.retentionLoss = value
				bridges = append(bridges, b)
			} else {
				b.retentionLoss.missed += missed
				b.retentionLoss.position = min(b.retentionLoss.position, value.position)
			}
		} else if u := captured.owner; u != nil && u.conn != nil {
			c.recordPositionLoss(positionLossKey{name: u.conn.clientID, channel: r.channel.row.Name}, value, now)
		}
	}
	for _, b := range bridges {
		c.recordPositionLoss(positionLossKey{name: b.options.Bridge.Name, channel: r.channel.row.Name, bridge: true}, b.retentionLoss, now)
		b.retentionLoss = positionLoss{}
	}
	clear(bridges)
	c.retentionBridges = bridges[:0]
}
