package core_test

import (
	"fmt"
	"net"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// deliveryBench drives the core alone through its public surface (Handle
// and Answer), as BenchmarkPublishEvents does: no network and no SQLite
// (design.md, Testing: "An inside test therefore drives it with scripted
// events and fake times"). It plays the store, answering every write saved
// and every read from the records the core has stored, and every writer,
// reporting each delivery the core hands it as written (design.md, Flows:
// Delivery).
type deliveryBench struct {
	tb      testing.TB
	c       *core.Core
	reg     *metrics.Registry
	now     time.Time
	topic   []byte
	payload []byte
	// records holds $broadcast's stored records' metadata, offset k at
	// index k-1: the one publisher writes one record per QoS 1 PUBLISH.
	records    []store.RecordMetadata
	rows       []store.RecordMetadata // one metadata page, answered in place
	queue      []core.Action
	deliveries int
	pubacks    int
	publisher  *connection.Conn
	// sent, where keepSent is set, holds the Packet Identifier of every
	// QoS 1 delivery handed to a writer, in order.
	keepSent bool
	sent     []uint16
	// changes, where keepChanges is set, holds every delivery-changes
	// operation the core writes, in order, a copy of each; where
	// refuseChanges is set, a transaction holding one and no record is
	// answered "full" (store.ErrFull), its rows unchanged.
	keepChanges, refuseChanges bool
	changes                    []store.DeliveryChanges
	// refuseRecords answers "full" (store.ErrFull) every transaction
	// holding a record.
	refuseRecords bool
	// hold keeps every delivery handed to a writer in held, unreported;
	// without it each is reported written at once.
	hold bool
	held []heldDelivery
}

// heldDelivery is a delivery a writer holds, not yet reported.
type heldDelivery struct {
	conn *connection.Conn
	p    *connection.Delivery
}

// newDeliveryBench returns a deliveryBench of a new core for cfg, started
// on a store holding $broadcast, its publications of topic and payload.
func newDeliveryBench(tb testing.TB, cfg *config.Config, topic, payload string) *deliveryBench {
	tb.Helper()
	reg := metrics.New()
	c := core.New(cfg, reg, &broadcast, nil, nil, nil, logger.NewCredits(cfg.Broker.LogLevel, reg))
	d := &deliveryBench{tb: tb, c: c, reg: reg, now: epoch, topic: []byte(topic), payload: []byte(payload),
		rows: make([]store.RecordMetadata, 0, store.MetadataPage)}
	d.drain(c.Fire(nil, d.now))
	return d
}

// write answers w, a transaction the core sends, as refuseRecords,
// keepChanges and refuseChanges say.
func (d *deliveryBench) write(w store.Write) store.Answer {
	if d.refuseRecords && w.Records() > 0 {
		return store.Answer{Op: w, Err: store.ErrFull}
	}
	members := []store.Write{w}
	if g, ok := w.(*core.Group); ok {
		members = g.Writes
	}
	changes := false
	for _, m := range members {
		if op, ok := m.(*store.DeliveryChanges); ok {
			changes = true
			if d.keepChanges {
				c := *op
				c.Save, c.Delete = slices.Clone(op.Save), slices.Clone(op.Delete)
				d.changes = append(d.changes, c)
			}
		}
	}
	if changes && d.refuseChanges && w.Records() == 0 {
		return store.Answer{Op: w, Err: store.ErrFull}
	}
	return store.Answer{Op: w}
}

// connect has the i-th connection accepted and connected with p.
func (d *deliveryBench) connect(i int, p *packet.Connect) *connection.Conn {
	conn := new(connection.Conn)
	accept := connection.Accept{Listener: "bench", Remote: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1024 + i%60_000}}
	d.drain(d.c.Handle(nil, connection.Event{Kind: connection.Accepted, Conn: conn, Packet: accept}, d.now))
	d.drain(d.c.Handle(nil, connection.Event{Kind: connection.Received, Conn: conn, Packet: p}, d.now))
	return conn
}

// subscribe has conn subscribe to filter at qos, with packet identifier 1,
// and carries out everything that leads to.
func (d *deliveryBench) subscribe(conn *connection.Conn, filter string, qos byte) {
	sub := &connection.Subscribe{PacketID: 1, Filters: []connection.Subscription{{Filter: filter, QoS: qos}}}
	d.drain(d.c.Handle(nil, connection.Event{Kind: connection.Received, Conn: conn, Packet: sub}, d.now))
}

// publish has the publisher send p, a QoS 1 PUBLISH of d's topic and
// payload whose body is body bytes: its ask for the bytes, then the
// PUBLISH, each with everything it leads to, acts the slice the core
// appends to. The store will hold its record once the core's write is
// answered.
func (d *deliveryBench) publish(p *packet.Publish, body int64, acts []core.Action) {
	d.now = d.now.Add(time.Microsecond)
	d.drain(d.c.Handle(acts[:0], connection.Event{Kind: connection.BytesRequested, Conn: d.publisher, Bytes: body}, d.now))
	d.now = d.now.Add(time.Microsecond)
	d.records = append(d.records, store.RecordMetadata{Offset: int64(len(d.records) + 1), Topic: d.topic,
		QoS: 1, Publisher: "publisher", PayloadSize: len(d.payload)})
	d.drain(d.c.Handle(acts[:0], connection.Event{Kind: connection.Received, Conn: d.publisher, Packet: p, Bytes: body}, d.now))
}

// drain carries out acts and everything they lead to, in order, until the
// core asks for nothing more.
func (d *deliveryBench) drain(acts []core.Action) {
	d.queue = append(d.queue[:0], acts...)
	for i := 0; i < len(d.queue); i++ {
		if i == 1<<24 {
			d.tb.Fatalf("the core still acts after %d actions", i)
		}
		a := d.queue[i]
		switch {
		case a.Write != nil:
			d.queue = d.c.Answer(d.queue, d.write(a.Write), d.now)
		case a.Read != nil:
			d.queue = d.c.Answer(d.queue, store.Answer{Op: a.Read, View: true}, d.now)
			if m, ok := a.Read.(*store.ReadRecordMetadata); ok {
				d.page(m)
			}
			d.queue = d.c.Answer(d.queue, store.Answer{Op: a.Read}, d.now)
		case a.Reply != nil:
			if p, ok := a.Reply.(*connection.Delivery); ok {
				// The store would have read the payload into the delivery;
				// the writer writes it and reports it written, unless it
				// holds it.
				p.Payload = d.payload
				d.deliveries++
				if d.keepSent && p.QoS > 0 {
					d.sent = append(d.sent, p.PacketID)
				}
				if d.hold {
					d.held = append(d.held, heldDelivery{a.Conn, p})
					continue
				}
				p.Result = connection.DeliveryWritten
				d.queue = d.c.Handle(d.queue, connection.Event{Kind: connection.Delivered, Conn: a.Conn, Packet: p}, d.now)
			} else if a.Conn == d.publisher {
				d.pubacks++ // after its CONNACK, the publisher's replies are its PUBACKs
			}
		}
	}
}

// report has the writer holding held delivery i report it with result.
func (d *deliveryBench) report(i int, result connection.DeliveryResult) {
	h := d.held[i]
	h.p.Result = result
	d.drain(d.c.Handle(nil, connection.Event{Kind: connection.Delivered, Conn: h.conn, Packet: h.p}, d.now))
}

// page answers a metadata read from the stored records: offsets From to
// Before, at most Limit rows (design.md, Flows: Delivery).
func (d *deliveryBench) page(m *store.ReadRecordMetadata) {
	if m.Channel != 1 {
		d.tb.Fatalf("a metadata read of channel %d; only $broadcast, channel 1, holds records", m.Channel)
	}
	d.rows = d.rows[:0]
	for off := max(m.From, 1); off < m.Before && len(d.rows) < m.Limit && off <= int64(len(d.records)); off++ {
		d.rows = append(d.rows, d.records[off-1])
	}
	m.Rows = d.rows
}

// BenchmarkDeliveryScheduling measures the core's delivery scheduling under
// load (design.md, Flows: Message memory, Delivery; Execution, Decisions).
// C MQTT 5 clients are connected; every one subscribes at QoS 0, one in
// ten to the publisher's topic and the rest to a filter matching nothing,
// so every published record wakes all C consumers and is delivered to C/10.
// One more client publishes QoS 1 messages into $broadcast, one at a time:
// each published message is its body's ask for bytes, its PUBLISH, and
// everything those lead to until the core is idle again: the record's
// write, the C consumers' metadata reads, the matching ones' payload reads,
// their deliveries handed to writers and each reported back written.
//
// ns/op, B/op and allocs/op are per published message; ns/delivery,
// B/delivery and allocs/delivery divide the same totals by the deliveries.
func BenchmarkDeliveryScheduling(b *testing.B) {
	for _, clients := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("C=%d", clients), func(b *testing.B) { benchDeliveries(b, clients) })
	}
}

func benchDeliveries(b *testing.B, clients int) {
	cfg := newConfig(b)
	cfg.Broker.Limits.MaxConnections = clients + 1
	cfg.Broker.Limits.MaxConnectRate = clients + 1
	d := newDeliveryBench(b, cfg, "bench/topic", "0123456789abcdef")

	matching := 0
	for i := range clients {
		conn := d.connect(i, connectOf(packet.V5, fmt.Sprintf("sub-%d", i)))
		filter := "nothing/matches"
		if i%10 == 0 {
			filter = string(d.topic)
			matching++
		}
		d.subscribe(conn, filter, 0)
	}
	d.publisher = d.connect(clients, connectOf(packet.V5, "publisher"))

	// The body: the Topic Name, the Packet Identifier, an empty property
	// section and the payload.
	body := int64(2 + len(d.topic) + 2 + 1 + len(d.payload))
	publishes := make([]packet.Publish, b.N)
	for i := range publishes {
		publishes[i] = packet.Publish{QoS: 1, PacketID: uint16(i%65_535 + 1), Topic: d.topic, Payload: d.payload}
	}
	d.records = make([]store.RecordMetadata, 0, b.N)
	d.queue = make([]core.Action, 0, 4*clients)
	acts := make([]core.Action, 0, 8)
	runtime.GC()
	var before, after runtime.MemStats
	b.ReportAllocs()
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	for i := range publishes {
		d.publish(&publishes[i], body, acts)
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)

	if d.pubacks != b.N {
		b.Fatalf("%d PUBACKs for %d QoS 1 PUBLISHes", d.pubacks, b.N)
	}
	if want := b.N * matching; d.deliveries != want {
		b.Fatalf("%d deliveries written; want %d messages to %d matching subscribers, %d", d.deliveries, b.N, matching, want)
	}
	n := float64(d.deliveries)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/n, "ns/delivery")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/n, "allocs/delivery")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/n, "B/delivery")
}
