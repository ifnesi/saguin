package connection_test

import (
	"bytes"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
)

// The deliveries seam these tests drive, beside the replies queue
// (connection_test.go's API):
//
//	type Delivery struct {
//		Topic      []byte // the record's topic
//		Properties []byte // the record's stored properties, Property Length first, nil for none (storage.md, records)
//		Payload    []byte // the record's payload, as the store read it
//		...               // what else a delivery carries: its QoS and RETAIN (zero: 0), when its Message Expiry runs out (zero: never)
//	}
//	func (c *Conn) Deliveries() chan<- *Delivery // its writer's deliveries queue, bounded by the in-flight window
//	const Delivered Kind // the writer's report of deliveries written or discarded, on the readers' channel
//
// The core sends a *Delivery on Deliveries, never waiting: it counts the
// in-flight window, whose room the queue always has (design.md, Flows,
// Replies: "The deliveries queue is bounded by the in-flight window"). The
// writer writes it as a PUBLISH in the connection's version, the stored
// properties and payload as they are, and then reports it to the core with
// an Event of Kind Delivered (design.md, Message memory, Bytes back: "A
// delivery reservation ends when the writer reports completion or
// discard"). The event's Packet is the first *Delivery of a socket write,
// the others linked behind it through Next, in the order written, each with
// its own Result.

// pingresp is a PINGRESP's bytes (MQTT 5 3.13).
var pingresp = []byte{0xD0, 0x00}

// deliver puts d on conn's deliveries queue, as the core does, failing the
// test where the queue has no room: the core never waits (R2).
func deliver(t testing.TB, conn *connection.Conn, d *connection.Delivery) {
	t.Helper()
	select {
	case conn.Deliveries() <- d:
	default:
		t.Fatalf("the deliveries queue is full: sending %q would make the core wait (R2)", d.Topic)
	}
}

// writing answers the writer's write deadlines until it writes, and
// returns that Write, unanswered: the writer waits in it.
func (s *seam) writing() *sockCall {
	s.t.Helper()
	for {
		switch c := s.next(writer); c.op {
		case opWriteDeadline:
			c.ok()
		case opWrite:
			return c
		default:
			s.t.Fatalf("the writer called %v, want a write", c)
		}
	}
}

// delivered answers the writer's calls until it has written a whole
// packet, which must be the PUBLISH of want in MQTT 5, then takes its
// report of d, and returns the bytes of each Write, in order, as the
// writer passed them.
func (s *seam) delivered(d *connection.Delivery, want *harness.Publish) [][]byte {
	s.t.Helper()
	return s.deliveredFrom(nil, d, want)
}

// deliveredFrom is delivered where the writer's first Write of the
// packet, held, is held: it is answered first.
func (s *seam) deliveredFrom(held *sockCall, d *connection.Delivery, want *harness.Publish) [][]byte {
	s.t.Helper()
	writes, _ := s.deliveredAll(held, []*connection.Delivery{d}, []*harness.Publish{want})
	return writes
}

// deliveredAll answers the writer's calls until it has written the whole
// packets of ds, in order, each the PUBLISH of its want in MQTT 5, in as
// many Writes as the writer makes (design.md, Delivery, Sending: the writer
// hands a packet's pieces to the connection "together with the deliveries
// already waiting behind them. They go in one gathered write within the
// capacity bound, without waiting for more"; "A packet that does not fit in
// a write's remaining room continues in the next"). It takes the writer's
// Delivered reports on this connection, whether they come before or after
// its next Write: walked through Next, they hold ds in order, each once,
// each reported only once its packet is written and with the result
// DeliveryWritten ("Only a packet written whole counts as written").
// held, if not nil, is the writer's first Write, held, answered first. It
// returns the bytes of each Write, in order, as the writer passed them, and
// the number of reports.
func (s *seam) deliveredAll(held *sockCall, ds []*connection.Delivery, wants []*harness.Publish) (writes [][]byte, reports int) {
	s.t.Helper()
	packets := make([][]byte, len(wants))
	ends := make([]int, len(wants)) // where each packet's bytes end in wire
	end := 0
	for i, want := range wants {
		packets[i] = harness.Encoded(s.t, want, harness.V5)
		end += len(packets[i])
		ends[i] = end
	}
	wire := bytes.Join(packets, nil)
	reported, n := 0, 0 // deliveries reported; bytes written
	report := func(ev connection.Event) {
		checkKind(s.t, &ev, connection.Delivered)
		p, ok := ev.Packet.(*connection.Delivery)
		if ev.Conn != s.conn || !ok || p == nil {
			s.t.Fatalf("the writer reported %s for connection %p; want deliveries of its own connection, %p", describe(&ev), ev.Conn, s.conn)
		}
		reports++
		for ; p != nil; p = p.Next {
			switch {
			case reported == len(ds):
				s.t.Fatalf("report %d goes on to %q, after all %d deliveries; want nothing more", reports, p.Topic, len(ds))
			case p != ds[reported]:
				s.t.Fatalf("report %d holds %q where %q is due; want the deliveries reported in the order written", reports, p.Topic, ds[reported].Topic)
			case ends[reported] > n:
				s.t.Fatalf("report %d holds %q with %d of its packet's bytes unwritten; want it reported once written whole", reports, p.Topic, ends[reported]-n)
			case p.Result != connection.DeliveryWritten:
				s.t.Fatalf("report %d holds %q with result %v; want it written", reports, p.Topic, p.Result)
			}
			reported++
		}
	}
	for n < len(wire) {
		c := held
		if c == nil {
			c = s.writing()
		}
		held = nil
		writes = append(writes, c.p)
		n += len(c.p)
		c.ok()
		// The writer may report a written packet before its next Write.
		synctest.Wait()
		for reported < len(ds) && ends[reported] <= n {
			select {
			case ev := <-s.inbound:
				report(ev)
				continue
			default:
			}
			break
		}
	}
	if got := bytes.Join(writes, nil); !bytes.Equal(got, wire) {
		s.t.Fatalf("the writer wrote % .64X for %d deliveries from %q; want % .64X", got, len(ds), ds[0].Topic, wire)
	}
	for reported < len(ds) {
		report(s.take())
	}
	return writes, reports
}

// design.md, Flows, Replies: "Each writer has exactly two queues, replies and
// deliveries. The reply queue holds acknowledgements, PINGRESP, SUBACK,
// UNSUBACK and the server's DISCONNECT. The writer sends a waiting reply
// first". Execution, Decisions: "Map iteration, an observed metric, queue
// emptiness and Go's choice between ready cases decide none of them". No
// client can force a reply and a delivery to wait together; the scripted
// socket holds the writer in a write while both are queued.
//
// The client sends a PINGREQ for each of rounds and one more. The core
// queues a PINGRESP, whose write the writer is held in; then, in each
// round, it queues a delivery and then a PINGRESP, and lets the held write
// finish: the writer's next write is that PINGRESP, held in turn, every
// delivery so far still waiting. A choice between ready queues left to Go
// would pass all rounds once in 2^rounds runs. Last, the writer writes the
// deliveries, in the order queued, in as many writes as it makes, and
// reports each, in that order.
func TestWriterSendsAWaitingReplyFirst(t *testing.T) {
	const rounds = 16
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		s.connect()
		n := s.reading.give(pingreq)
		s.reading = nil
		s.feed(pingreq[n:])
		for i := range rounds + 1 {
			if i != 0 {
				s.feed(pingreq)
			}
			if ev := s.event(connection.Received); ev.Packet != (connection.Pingreq{}) {
				t.Fatalf("Received %s, want PINGREQ %d", describe(&ev), i+1)
			}
		}

		s.queue(connection.Pingresp{})
		held := s.writing()
		deliveries := make([]*connection.Delivery, rounds)
		for i := range rounds {
			deliveries[i] = &connection.Delivery{Topic: fmt.Appendf(nil, "t/%d", i), Payload: []byte{byte(i)}}
			deliver(t, s.conn, deliveries[i])
			s.queue(connection.Pingresp{})
			held.ok()
			held = s.writing()
			if !bytes.Equal(held.p, pingresp) {
				t.Fatalf("round %d: with a PINGRESP and %d deliveries waiting, the writer wrote % .16X; want the PINGRESP first",
					i+1, i+1, held.p)
			}
		}
		held.ok()
		wants := make([]*harness.Publish, rounds)
		for i, d := range deliveries {
			wants[i] = &harness.Publish{Topic: string(d.Topic), Payload: []byte{byte(i)}}
		}
		s.deliveredAll(nil, deliveries, wants)
	})
}

// design.md, Delivery, Sending: "the writer encodes a header apart from its
// payload. It never copies the payload. It hands the two to the connection as
// separate pieces, together with the deliveries already waiting behind them.
// They go in one gathered write within the capacity bound, without waiting
// for more (Capacities). A plain socket passes the pieces to the system
// together. A TLS or WebSocket connection takes them one at a time"; "Stored
// properties are structurally decoded. They are not revalidated against
// publish policy for every subscriber". The seam's scripted socket is not a
// plain socket: like a TLS or WebSocket connection, it takes the pieces one
// at a time, so it sees each as a Write of its own. Testing: "The busy path
// is anything run per packet, per message or per connection. It does no
// needless work. It makes no second pass over the same data and avoids every
// copy or allocation it can". Flows, Packet buffers: "Between packets, a
// connection holds no read or write buffer".
//
// Under a configuration whose limits.max_topic_length is 8, the core
// queues a delivery whose stored record has a longer topic, as one stored
// before a restart under a larger limit has, and properties publish policy
// refuses, a saguin- User Property (design.md, Sagüin properties: "a
// publish carrying any saguin- property except saguin-id [...] is refused
// with 0x83"), and a payload of 64 KiB. The writer writes exactly the
// PUBLISH of that topic, those properties and that payload, in two
// Writes: the packet's head, then the payload itself, the very bytes the
// delivery holds, never a copy.
func TestDeliveryWritesTheStoredPayloadAsItIs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newConfig(t)
		cfg.Broker.Limits.MaxTopicLength = 8
		s := launchSeam(t, cfg)
		s.accepted()
		s.connect()

		props := harness.Properties{harness.User("saguin-unread", "kept"), {ID: harness.ContentType, Str: "text/plain"}}
		section, err := harness.AppendProperties(nil, props)
		if err != nil {
			t.Fatal(err)
		}
		d := &connection.Delivery{Topic: []byte("stored/under/a/larger/limit"), Properties: section, Payload: bytes.Repeat([]byte("p"), 64<<10)}
		deliver(t, s.conn, d)
		writes := s.delivered(d, &harness.Publish{Topic: string(d.Topic), Props: props, Payload: d.Payload})
		if len(writes) != 2 {
			t.Fatalf("the delivery took %d Writes; want 2, its head and its payload", len(writes))
		}
		if p := writes[1]; len(p) != len(d.Payload) || &p[0] != &d.Payload[0] {
			t.Errorf("the second Write is %d bytes at %p; want the delivery's own payload, %d bytes at %p, not a copy",
				len(p), &p[0], len(d.Payload), &d.Payload[0])
		}
	})
}

// design.md, Delivery, Slow clients: "A slow client fills only its own
// writer, and the core stops sending it deliveries"; Flows, Replies: "The
// deliveries queue is bounded by the in-flight window", the writer "sends
// a waiting reply first"; R2: "The core never waits". Testing: a test
// inside the code checks "orderings no client can force": that a writer is
// in a write that does not finish is one, which the scripted socket makes
// by holding the Write it is called with.
//
// Two connections, each on a scripted socket, with limits.max_inflight
// window. On the first, the core queues a delivery and the writer enters
// its Write, which is held. While it is held, the core hands that
// connection the rest of its window, window-1 deliveries, and a PINGRESP
// for a PINGREQ the client sends, never waiting: the deliveries queue
// holds exactly the window. Meanwhile the second connection's writer
// writes a delivery whole and reports it. Once the held Write finishes,
// the first writer completes its delivery, writes the PINGRESP, and then
// the other deliveries in order, reporting each.
func TestStuckWriterHoldsBackOnlyItsConnection(t *testing.T) {
	const window = 4
	synctest.Test(t, func(t *testing.T) {
		launch := func() *seam {
			cfg := newConfig(t)
			cfg.Broker.Limits.MaxInflight = window
			s := launchSeam(t, cfg)
			s.accepted()
			s.connect()
			return s
		}
		stuck, other := launch(), launch()
		if got := cap(stuck.conn.Deliveries()); got != window {
			t.Fatalf("the deliveries queue holds %d; want the in-flight window, %d", got, window)
		}
		deliveries := make([]*connection.Delivery, window)
		for i := range deliveries {
			deliveries[i] = &connection.Delivery{Topic: fmt.Appendf(nil, "stuck/%d", i), Payload: []byte{byte(i)}}
		}
		deliver(t, stuck.conn, deliveries[0])
		held := stuck.writing()

		for _, d := range deliveries[1:] {
			deliver(t, stuck.conn, d)
		}
		n := stuck.reading.give(pingreq)
		stuck.reading = nil
		stuck.feed(pingreq[n:])
		if ev := stuck.event(connection.Received); ev.Packet != (connection.Pingreq{}) {
			t.Fatalf("Received %s, want the PINGREQ", describe(&ev))
		}
		stuck.queue(connection.Pingresp{})

		d := &connection.Delivery{Topic: []byte("other/0"), Payload: []byte("o")}
		deliver(t, other.conn, d)
		other.delivered(d, &harness.Publish{Topic: string(d.Topic), Payload: d.Payload})

		stuck.deliveredFrom(held, deliveries[0], &harness.Publish{Topic: string(deliveries[0].Topic), Payload: deliveries[0].Payload})
		if c := stuck.writing(); !bytes.Equal(c.p, pingresp) {
			t.Fatalf("after the held delivery the writer wrote % .16X; want the waiting PINGRESP", c.p)
		} else {
			c.ok()
		}
		wants := make([]*harness.Publish, 0, window-1)
		for _, d := range deliveries[1:] {
			wants = append(wants, &harness.Publish{Topic: string(d.Topic), Payload: d.Payload})
		}
		stuck.deliveredAll(nil, deliveries[1:], wants)
	})
}

// design.md, Flows, Replies: "Each writer has exactly two queues, replies and
// deliveries. The reply queue holds acknowledgements, PINGRESP, SUBACK,
// UNSUBACK and the server's DISCONNECT. The writer sends a waiting reply
// first"; Delivery, Sending: the writer hands a delivery's pieces to the
// connection "together with the deliveries already waiting behind them.
// They go in one gathered write within the capacity bound, without waiting
// for more". A reply that
// arrives while the writer is working through its deliveries queue goes
// before every delivery not yet begun. That the writer is in a write while
// both queues hold entries is an ordering no client can force; the scripted
// socket holds the Write it is called with.
//
// While the writer is held in the CONNACK's Write, the core queues four
// deliveries: the first, then one whose Message Expiry ran out before it is
// written (design.md, Delivery, Message Expiry), which the writer reports
// without writing, then two more; all four are waiting when the CONNACK's
// Write returns, whatever the scheduling. The writer begins the first and
// is held in its Write; the client sends a
// PINGREQ and the core queues its PINGRESP. Once the first is written and
// the writer has reported the first two, its next Write is the PINGRESP,
// before either delivery still waiting; last, it writes those two, in the
// order queued.
func TestReplyArrivingAmidDeliveriesGoesFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		connack := s.connectHeld()

		first := &connection.Delivery{Topic: []byte("t/first"), Payload: []byte{1}}
		expired := &connection.Delivery{Topic: []byte("t/expired"), Payload: []byte{2}, Expires: s.clock.Now().UnixMilli()}
		waiting := []*connection.Delivery{
			{Topic: []byte("t/third"), Payload: []byte{3}},
			{Topic: []byte("t/fourth"), Payload: []byte{4}},
		}
		for _, d := range append([]*connection.Delivery{first, expired}, waiting...) {
			deliver(t, s.conn, d)
		}
		connack.ok()
		s.hold()
		held := s.writing()
		s.arrive(pingreq)
		if ev := s.event(connection.Received); ev.Packet != (connection.Pingreq{}) {
			t.Fatalf("Received %s, want the PINGREQ", describe(&ev))
		}
		s.queue(connection.Pingresp{})
		s.deliveredFrom(held, first, &harness.Publish{Topic: string(first.Topic), Payload: first.Payload})
		if ev := s.event(connection.Delivered); ev.Packet != expired {
			t.Fatalf("the writer reported %s; want the report of %q, expired", describe(&ev), expired.Topic)
		}

		reply := s.writing()
		if !bytes.Equal(reply.p, pingresp) {
			t.Fatalf("with a PINGRESP and two deliveries waiting, the writer wrote % .32X; want the PINGRESP first", reply.p)
		}
		reply.ok()
		wants := make([]*harness.Publish, len(waiting))
		for i, d := range waiting {
			wants[i] = &harness.Publish{Topic: string(d.Topic), Payload: d.Payload}
		}
		s.deliveredAll(nil, waiting, wants)
	})
}

// design.md, Delivery, Sending: the writer hands a delivery's pieces to the
// connection "together with the deliveries already waiting behind them.
// They go in one gathered write within the capacity bound, without waiting
// for more"; Message memory, Bytes back: "A delivery reservation ends when
// the writer reports completion or discard". Testing: "The busy path is
// anything run per packet, per message or per connection. It does no
// needless work. It makes no second pass over the same data and avoids every
// copy or allocation it can": a gathered write is reported to the core once,
// not once per packet (0047b). That every delivery is waiting when the
// writer begins is an ordering no client can force; the scripted socket
// holds the Write it is called with.
//
// While the writer is held in the CONNACK's Write, the core queues four
// deliveries. Once the CONNACK's Write returns, the writer gathers all four
// into one write and reports them to the core in one Delivered event: the
// first delivery, the other three behind it through Next, in the order
// queued, each written.
func TestGatheredWriteIsReportedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSeam(t)
		connack := s.connectHeld()
		deliveries := make([]*connection.Delivery, 4)
		wants := make([]*harness.Publish, len(deliveries))
		for i := range deliveries {
			deliveries[i] = &connection.Delivery{Topic: fmt.Appendf(nil, "gathered/%d", i), Payload: []byte{byte(i)}}
			deliver(t, s.conn, deliveries[i])
			wants[i] = &harness.Publish{Topic: string(deliveries[i].Topic), Payload: deliveries[i].Payload}
		}
		connack.ok()
		if _, reports := s.deliveredAll(nil, deliveries, wants); reports != 1 {
			t.Errorf("the writer reported one gathered write of %d deliveries in %d Delivered events; want one", len(deliveries), reports)
		}
	})
}
