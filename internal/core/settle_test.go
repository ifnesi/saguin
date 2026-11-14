package core_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// BenchmarkSettlement measures the work of one acknowledgement against the
// size of the in-flight window it settles in (design.md, Testing: "The busy
// path is anything run per packet, per message or per connection. It does no
// needless work. It makes no second pass over the same data"; Delivery,
// Positions: "an acknowledgement (PUBACK, or PUBCOMP) settles its own
// delivery", "The core keeps a scan position and the bounded set of unsettled
// deliveries"). One PUBACK's work is its own delivery's, its save and its
// row's deletion included: ns/ack stays flat as the window grows. It reports,
// and never decides: elapsed time is no pass or fail.
//
// The core alone (deliveryBench): an MQTT 3.1.1 client, whose window is
// limits.max_inflight alone (Delivery: "the cap alone for MQTT 3.1.1"),
// either with Clean Session 1 or durable, with Clean Session 0, whose
// QoS 1 deliveries are saved before they are sent and whose
// acknowledgements are saved (Delivery, Positions), subscribes at QoS 1
// and is handed one QoS 1 delivery for each of a window's worth of QoS 1
// publications, each reported written. Each op then acknowledges them all
// in order, the time per PUBACK reported as ns/ack.
func BenchmarkSettlement(b *testing.B) {
	for _, durable := range []bool{false, true} {
		for _, window := range []int{512, 32_768} {
			b.Run(fmt.Sprintf("durable=%v/window=%d", durable, window), func(b *testing.B) {
				var acks int
				for range b.N {
					b.StopTimer()
					d, sub := filledWindow(b, window, window, durable)
					b.StartTimer()
					for _, id := range d.sent {
						d.drain(d.c.Handle(nil, connection.Event{Kind: connection.Received, Conn: sub, Packet: &packet.Ack{PacketID: id}}, d.now))
					}
					acks += len(d.sent)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(acks), "ns/ack")
			})
		}
	}
}

// filledWindow returns a core whose limits.max_inflight is capacity and
// whose subscriber, durable or not, has been handed filled QoS 1
// deliveries, of offsets 1 to filled, each reported written and none
// acknowledged, and that subscriber's connection.
func filledWindow(tb testing.TB, capacity, filled int, durable bool) (*deliveryBench, *connection.Conn) {
	tb.Helper()
	cfg := newConfig(tb)
	cfg.Broker.Limits.MaxInflight = capacity
	d := newDeliveryBench(tb, cfg, "settle/topic", "0123456789abcdef")
	d.keepSent = true
	connect := connectOf(packet.V311, "subscriber")
	if durable {
		connect = keeping(packet.V311, "subscriber", 0)
	}
	sub := d.connect(0, connect)
	d.subscribe(sub, string(d.topic), 1)
	d.publisher = d.connect(1, connectOf(packet.V5, "publisher"))

	body, acts := d.bodyOf(), make([]core.Action, 0, 8)
	for range filled {
		d.publishNext(body, acts)
	}
	if len(d.sent) != filled {
		tb.Fatalf("%d QoS 1 deliveries handed to the subscriber's writer; want %d", len(d.sent), filled)
	}
	return d, sub
}

// publishNext has the publisher send the next QoS 1 message of d's topic
// and payload, whose body is body bytes.
func (d *deliveryBench) publishNext(body int64, acts []core.Action) {
	id := uint16(len(d.records)%65_535 + 1)
	d.publish(&packet.Publish{QoS: 1, PacketID: id, Topic: d.topic, Payload: d.payload}, body, acts)
}

// bodyOf is the body of d's publications: the Topic Name, the Packet
// Identifier, an empty property section and the payload.
func (d *deliveryBench) bodyOf() int64 { return int64(2 + len(d.topic) + 2 + 1 + len(d.payload)) }

// windows are the sizes of TestDurableWindowWrites: a few, a page's worth
// beyond the metadata page, and MQTT's whole Packet Identifier space, less
// one for the delivery a test adds.
var windows = []int{8, 512, 65_534}

// design.md, Delivery, Positions: "an acknowledgement (PUBACK, or PUBCOMP)
// settles its own delivery. The position advances only past the
// contiguous settled or skipped prefix of examined records [...] Before
// handing a QoS 1 or QoS 2 delivery to a durable session's writer, the
// core stores its packet identifier, its order (seq), its record and its
// phase; a failed save leaves it unsent". Store, A full disk: "A housekeeping write refused for room sets a retry
// deadline one second after that answer [...] At the deadline, the core
// [...] retries connected consumers' dirty-position marks from its own
// state. Failed write bodies are not retained". Testing: the busy path
// "does no needless work. It makes no second pass over the same data". What
// the core writes per acknowledgement or delivery is that event's own change,
// whatever the window's size; the cost of finding it is BenchmarkSettlement's
// to show.
//
// The core alone (deliveryBench), an MQTT 3.1.1 subscriber with Clean
// Session 0, a durable session, subscribed at QoS 1, with a window of N
// QoS 1 exchanges, each saved and written, for each N of windows:
//   - acknowledged: its first exchange's PUBACK leads to one
//     delivery-changes operation, which saves nothing, deletes exactly
//     that identifier and moves the position to 2, past offset 1 alone;
//   - grown: one more publication, room in the window for it, leads to
//     one operation, which saves exactly that exchange, of offset N+1, and
//     deletes nothing;
//   - refused: with the disk full, three more publications, room for
//     them, lead to operations refused "full", and none of the three is
//     handed out; once there is room, at the retry deadline one second on,
//     the operation that saves succeeds saving exactly those three, of
//     offsets N+1 to N+3, once each, and they are handed out.
func TestDurableWindowWrites(t *testing.T) {
	for _, n := range windows {
		t.Run(fmt.Sprintf("acknowledged/N=%d", n), func(t *testing.T) {
			d, sub := filledWindow(t, n, n, true)
			d.keepChanges = true
			d.drain(d.c.Handle(nil, connection.Event{Kind: connection.Received, Conn: sub, Packet: &packet.Ack{PacketID: d.sent[0]}}, d.now))
			if len(d.changes) != 1 {
				t.Fatalf("the PUBACK led to %d delivery-changes operations; want 1: %+v", len(d.changes), brief(d.changes))
			}
			if op := d.changes[0]; len(op.Save) != 0 || !slices.Equal(op.Delete, []uint16{d.sent[0]}) || op.Position != 2 {
				t.Errorf("the PUBACK of identifier %d, offset 1, led to %+v; want no save, that identifier deleted, position 2", d.sent[0], brief(d.changes))
			}
		})
		t.Run(fmt.Sprintf("grown/N=%d", n), func(t *testing.T) {
			d, _ := filledWindow(t, n+1, n, true)
			d.keepChanges = true
			d.publishNext(d.bodyOf(), nil)
			if len(d.changes) != 1 {
				t.Fatalf("one more delivery led to %d delivery-changes operations; want 1: %+v", len(d.changes), brief(d.changes))
			}
			if op := d.changes[0]; len(op.Save) != 1 || op.Save[0].Offset != int64(n+1) || len(op.Delete) != 0 {
				t.Errorf("one more delivery, of offset %d, led to %+v; want that one exchange saved alone", n+1, brief(d.changes))
			}
		})
	}
	for _, n := range windows[:2] {
		t.Run(fmt.Sprintf("refused/N=%d", n), func(t *testing.T) {
			const more = 3
			d, _ := filledWindow(t, n+more, n, true)
			d.keepChanges, d.refuseChanges = true, true
			for range more {
				d.publishNext(d.bodyOf(), nil)
			}
			if len(d.sent) != n {
				t.Fatalf("with their saves refused, %d more exchanges were handed out; want none", len(d.sent)-n)
			}
			d.changes, d.refuseChanges = nil, false
			d.now = d.now.Add(time.Second)
			d.drain(d.c.Fire(nil, d.now))
			var saved []int64
			for _, op := range d.changes {
				for _, row := range op.Save {
					saved = append(saved, row.Offset)
				}
			}
			slices.Sort(saved)
			if want := []int64{int64(n + 1), int64(n + 2), int64(n + 3)}; !slices.Equal(saved, want) {
				t.Errorf("the retry saved the exchanges of offsets %v, in %+v; want %v, the unsaved ones, once each", saved, brief(d.changes), want)
			}
			if len(d.sent) != n+more {
				t.Errorf("after the retry %d exchanges were handed out; want %d", len(d.sent), n+more)
			}
		})
	}
}

// brief describes ops for a failure, each save by its offset.
func brief(ops []store.DeliveryChanges) []string {
	out := make([]string, len(ops))
	for i, op := range ops {
		offsets := make([]int64, len(op.Save))
		for j, row := range op.Save {
			offsets[j] = row.Offset
		}
		out[i] = fmt.Sprintf("{saves offsets %v, deletes %v, position %d}", offsets, op.Delete, op.Position)
	}
	return out
}
