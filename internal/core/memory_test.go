package core_test

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Message memory, cited below by its paragraphs:
//
// "Message bytes in transit are bounded by two budgets, which the core
// counts (R6): the incoming budget, limits.max_incoming_memory, holds
// PUBLISH bodies from the start of their reading until they are stored or
// refused, and Wills being published; the outgoing budget,
// limits.max_outgoing_memory, holds deliveries queued to writers, the
// answers to requests included. A reservation is taken from one budget and
// given back to that same budget. Neither budget lends to the other.
// Admission and reservation accounting use one implementation for both
// budgets."
//
// Asking: "Before allocating or reading a nonempty PUBLISH body, its reader
// requests the body's Remaining Length from the core. Every such body uses
// this request. A reader has at most one request or granted body."
//
// Two queues: "Each budget has its own FIFO of requests, bounded by one
// request per reader and one per delivery-window entry. A request receives
// its whole reservation or waits. When the head of a FIFO does not fit in
// its budget, it waits with every younger request of that FIFO behind it,
// and no younger request of that FIFO passes it. A head that waits holds
// back only its own FIFO: a delivery never waits for room in the incoming
// budget, and a body never waits for room in the outgoing one"; "Once a
// stop begins or the store fails, the core grants no more."
//
// Bytes back: "An incoming reservation follows its body through reading,
// handover and storage, and ends when the body is stored, refused or
// abandoned, at the reader's end if unread."
//
// The script hands the core every PUBLISH body as a reader does, only once
// its reader was given its bytes (script.body).

// maxIncoming and maxPacket are the limits.max_incoming_memory and
// limits.max_packet_size of memoryScript's core.
const maxIncoming, maxPacket = 16384, 4096

// memoryScript is a script of a core whose limits.max_incoming_memory is
// maxIncoming, limits.max_packet_size maxPacket and limits.max_connections
// connections, started on a store holding $broadcast, into which every
// publish here goes.
func memoryScript(t *testing.T, connections int) *script {
	t.Helper()
	return memoryScriptOf(t, connections, maxPacket)
}

// memoryScriptOf is memoryScript with limits.max_packet_size packetSize: two
// bodies of twice maxPacket fill max_incoming_memory with two publication
// slots (design.md, Message memory: "A granted body, unread or held behind
// its CONNECT, reserves one potential batch slot"), leaving slots free
// where a test is of bytes alone.
func memoryScriptOf(t *testing.T, connections int, packetSize config.Size) *script {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxIncomingMemory = maxIncoming
	cfg.Broker.Limits.MaxPacketSize = packetSize
	cfg.Broker.Limits.MaxConnections = connections
	return newScriptOn(t, cfg, &broadcast)
}

// inTransit is reg's saguin_memory_in_transit_bytes of direction, incoming
// or outgoing: "outstanding reservations of each budget" (metrics.md, Store
// and memory).
func inTransit(tb testing.TB, reg *metrics.Registry, direction string) int64 {
	tb.Helper()
	return int64(testkit.Sample(tb, reg, testkit.SeriesKey("saguin_memory_in_transit_bytes", "direction", direction)))
}

// expectInTransit checks the incoming budget's
// saguin_memory_in_transit_bytes.
func (s *script) expectInTransit(want int64, when string) {
	s.t.Helper()
	if got := inTransit(s.t, s.reg, "incoming"); got != want {
		s.t.Errorf("%s: saguin_memory_in_transit_bytes{direction=\"incoming\"} = %d, want %d", when, got, want)
	}
}

// ask hands the core conn's ask for a body of n bytes, and returns the
// bytes the decision gives conn.
func (s *script) ask(conn *connection.Conn, n int64) int64 {
	s.t.Helper()
	return given(s.step(&connection.Event{Kind: connection.BytesRequested, Conn: conn, Bytes: n}), conn)
}

// given returns the bytes acts give conn.
func given(acts []core.Action, conn *connection.Conn) int64 {
	var n int64
	for _, a := range acts {
		if a.Conn == conn {
			n += a.Bytes
		}
	}
	return n
}

// granted connects an MQTT 5 reader for each id, each asking for n bytes
// and given them at once, and returns the readers.
func (s *script) granted(n int64, ids ...string) []*connection.Conn {
	s.t.Helper()
	readers := make([]*connection.Conn, 0, len(ids))
	for _, id := range ids {
		r := s.connected(connectOf(packet.V5, id))
		if got := s.ask(r, n); got != n {
			s.t.Fatalf("reader %s asked for %d bytes with %d in transit and was given %d", id, n, inTransit(s.t, s.reg, "incoming")-got, got)
		}
		readers = append(readers, r)
	}
	return readers
}

// publish is a reader's PUBLISH as Message memory has it, ask, grant and
// read: conn asks for the body's n bytes, is given them, and hands the
// core an MQTT 5 PUBLISH into $broadcast at QoS 1 with packet identifier
// id; publish returns the decision on the PUBLISH.
func (s *script) publish(conn *connection.Conn, id uint16, n int64) []core.Action {
	s.t.Helper()
	return s.publishAt(conn, 1, id, n)
}

// publishAt is publish at qos, id being 0 at QoS 0.
func (s *script) publishAt(conn *connection.Conn, qos byte, id uint16, n int64) []core.Action {
	s.t.Helper()
	if got := s.ask(conn, n); got != n {
		s.t.Fatalf("a reader asked for a body of %d bytes with %d in transit and was given %d", n, inTransit(s.t, s.reg, "incoming")-got, got)
	}
	return s.handOver(conn, qos, id, n)
}

// handOver hands the core conn's body of the n bytes it was given: an
// MQTT 5 PUBLISH into $broadcast at qos with packet identifier id (0 at
// QoS 0), Received carrying the body's size, as the reader hands it over;
// and returns the decision.
func (s *script) handOver(conn *connection.Conn, qos byte, id uint16, n int64) []core.Action {
	s.t.Helper()
	return s.handOverWith(conn, qos, id, n, connection.PublishMetadata{})
}

// handOverWith is handOver, the PUBLISH carrying meta, the metadata its
// reader collected.
func (s *script) handOverWith(conn *connection.Conn, qos byte, id uint16, n int64, meta connection.PublishMetadata) []core.Action {
	s.t.Helper()
	const topic = "memory"
	head := int64(2 + len(topic) + 1) // the Topic Name and an empty property section
	if qos != 0 {
		head += 2 // the Packet Identifier
	}
	if n < head {
		s.t.Fatalf("a PUBLISH body of %d bytes: at QoS %d it takes at least %d", n, qos, head)
	}
	p := &packet.Publish{QoS: qos, PacketID: id, Topic: []byte(topic), Payload: make([]byte, n-head)}
	return s.step(&connection.Event{Kind: connection.Received, Conn: conn, Packet: p, Bytes: n, Publish: meta})
}

// refuse hands the core conn's refusal of the body of the n bytes it was
// given, an MQTT 5 PUBLISH at qos with packet identifier id, for a topic
// under $SYS/ (design.md, Flows, PUBLISH: "0x87 | [...] a topic under
// $SYS/").
func (s *script) refuse(conn *connection.Conn, qos byte, id uint16, n int64) []core.Action {
	s.t.Helper()
	return s.step(&connection.Event{Kind: connection.Refused, Conn: conn, Reason: 0x87, Bytes: n,
		Packet: connection.PublishRefusal{QoS: qos, PacketID: id}})
}

// Two queues: "A request receives its whole reservation or waits"; Bytes
// back: a reservation "ends when the body is [...] abandoned, at the
// reader's end if unread".
//
// With max_incoming_memory 16384 and max_packet_size 4096, reader r asks
// for 1 byte and is given it; readers a, b and c ask for 4096 each and are
// given them, 12289 reserved in all; d asks for 4096, one byte more than
// the 4095 left, and is given nothing; r ends with its byte unread, and d
// is given its 4096, the reservations then holding all of
// max_incoming_memory.
func TestRequestsGrantedFromAllOfTheIncomingBudget(t *testing.T) {
	const memory, body = maxIncoming, maxPacket
	s := memoryScript(t, 8)
	r := s.granted(1, "r")[0]
	s.granted(body, "a", "b", "c")
	d := s.connected(connectOf(packet.V5, "d"))
	if got := s.ask(d, body); got != 0 {
		t.Fatalf("a reader asking for %d bytes with %d of max_incoming_memory's %d reserved was given %d: a request receives its whole reservation or waits",
			body, inTransit(s.t, s.reg, "incoming")-got, memory, got)
	}
	if got := given(s.end(r, nil, 0), d); got != body {
		t.Fatalf("a reader's end returned its 1 byte unread, and the reader waiting for %d bytes was given %d", body, got)
	}
	s.expectInTransit(memory, "every byte of max_incoming_memory reserved")
}

// Two queues: "Once a stop begins or the store fails, the core grants no
// more"; R8: "A store error stops the broker". Audit 261019-009a: no test
// failed the store with an ask outstanding.
//
// With max_incoming_memory 16384 and max_packet_size 4096, readers a, b, c and d
// ask for 4000 bytes each and are given them, leaving 384; e asks for 500
// and waits, in the middle of its PUBLISH. a's body of 4000 is handed
// over, and the store answers its write with an error other than full.
// Then b ends with its 4000 unread, which leaves room for the 500; c hands
// over its body of 4000; the stop begins; and only then does e end. No
// decision from the failure on gives e bytes.
func TestNoBytesGivenAfterAStoreFailure(t *testing.T) {
	const memory = maxIncoming
	s := memoryScript(t, 8)
	readers := s.granted(4000, "a", "b", "c", "d")
	waiting := s.connected(connectOf(packet.V5, "e"))
	if got := s.ask(waiting, 500); got != 0 {
		t.Fatalf("a reader asking for 500 bytes with 16000 of max_incoming_memory's %d reserved was given %d", memory, got)
	}
	s.handOver(readers[0], 1, 1, 4000)
	failure := errors.New("disk I/O error")
	noBytes := func(acts []core.Action, after string) {
		t.Helper()
		if got := given(acts, waiting); got != 0 {
			t.Fatalf("%s, the store having failed, the reader waiting for 500 bytes was given %d", after, got)
		}
	}
	noBytes(s.reply(only[store.Write](s), failure), "the store's failure")
	select {
	case err := <-s.core.Failures():
		if !errors.Is(err, failure) {
			t.Fatalf("the core reported %v, want the store's error, %v", err, failure)
		}
	default:
		t.Fatalf("a store error other than full, and no failure reported (R8: it stops the broker)")
	}
	noBytes(s.end(readers[1], nil, 0), "a reader's end returning its 4000 bytes unread")
	if held := inTransit(s.t, s.reg, "incoming"); held+500 > memory {
		t.Fatalf("%d bytes in transit after a reader's end: max_incoming_memory's %d has no room for the 500 asked, so nothing here tests the rule", held, memory)
	}
	noBytes(s.handOver(readers[2], 1, 1, 4000), "another reader's body of the 4000 bytes given, handed over")
	noBytes(s.stop(), "the stop")
	noBytes(s.end(waiting, nil, 0), "the waiting reader's end")
}

// Two queues: "Once a stop begins or the store fails, the core grants no
// more"; design.md, Restart, Graceful stop: "every client gets 0x8B", and
// of what readers hand the core "any other packet it takes is neither
// stored nor answered"; Closing a connection: the writer "stops the
// reader", which until then reads on.
//
// With max_incoming_memory 16384 and max_packet_size 4096, readers a, b, c
// and d ask for 4000 bytes each and are given them, leaving 384; e asks for
// 500 and waits. The stop begins, with no store failure. b, which its writer
// has not yet stopped, hands over its body of 4000, neither stored nor
// answered, and asks for 200 for its next; a ends with its 4000 unread,
// which leaves room for both asks; then the asking readers end. No
// decision from the stop on gives either of them bytes.
func TestNoBytesGivenOnceAStopBegins(t *testing.T) {
	const memory = maxIncoming
	s := memoryScript(t, 8)
	readers := s.granted(4000, "a", "b", "c", "d")
	queued, late := s.connected(connectOf(packet.V5, "e")), readers[1]
	if got := s.ask(queued, 500); got != 0 {
		t.Fatalf("a reader asking for 500 bytes with 16000 of max_incoming_memory's %d reserved was given %d", memory, got)
	}
	noBytes := func(acts []core.Action, after string) {
		t.Helper()
		for _, r := range []*connection.Conn{queued, late} {
			if got := given(acts, r); got != 0 {
				t.Fatalf("%s, the stop having begun, a reader asking was given %d bytes", after, got)
			}
		}
	}
	noBytes(s.stop(), "the stop")
	noBytes(s.handOver(late, 1, 1, 4000), "a reader's body of the 4000 bytes given, handed over after the stop")
	if got := s.ask(late, 200); got != 0 {
		t.Fatalf("a reader's ask for 200 bytes after the stop began was given %d", got)
	}
	noBytes(s.end(readers[0], nil, 0), "a reader's end returning its 4000 bytes unread")
	if held := inTransit(s.t, s.reg, "incoming"); held+500+200 > memory {
		t.Fatalf("%d bytes in transit after a reader's end: max_incoming_memory's %d has no room for the 500 and 200 asked, so nothing here tests the rule", held, memory)
	}
	noBytes(s.end(late, nil, 0), "the late reader's end")
	noBytes(s.end(queued, nil, 0), "the queued reader's end")
}

// design.md, Message memory, Two queues: "A request receives its whole
// reservation or waits. When the head of a FIFO does not fit in its budget,
// it waits with every younger request of that FIFO behind it, and no
// younger request of that FIFO passes it"; Bytes back: a reservation ends
// "at the reader's end if unread", an ask the reader's end cancels with it.
//
// With max_incoming_memory 16384, max_packet_size 8192 and max_connections 16, two
// readers ask for 8048 bytes each and are given them, leaving 288, and
// publication slots for every grant below (design.md, Message memory:
// "batches and reserved slots together obey the batch bound").
// Then,
// in order, w1 asks for 1000, w2 to w5 for 100 each, and w6 for 1000: none
// is given, the 100s fitting but asked after w1's 1000. w3 ends, from the
// queue's middle, then w4, next to where w3 was, then w6, its tail:
// nothing is given. w7 asks for 100, now at the tail. w1 ends, the head:
// w2 then w5 are given their 100, in that order, and w7 nothing, 88 being
// left. The first of the two ends, its 8048 unread: w7 is given its 100.
// w2 ends, its 100 unread, and w8, asking for 50 with no one waiting, is
// given them at once.
func TestAskedBytesGivenInTheOrderAsked(t *testing.T) {
	s := memoryScriptOf(t, 16, 2*maxPacket)
	full := s.granted(8048, "f1", "f2")
	w := map[string]*connection.Conn{}
	for _, id := range []string{"w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8"} {
		w[id] = s.connected(connectOf(packet.V5, id))
	}
	// grants returns who acts gives bytes to, and how many, in order.
	grants := func(acts []core.Action) []string {
		var got []string
		for _, a := range acts {
			for id, conn := range w {
				if a.Bytes != 0 && a.Conn == conn {
					got = append(got, fmt.Sprintf("%s %d", id, a.Bytes))
				}
			}
		}
		return got
	}
	expect := func(acts []core.Action, when string, want ...string) {
		t.Helper()
		if got := grants(acts); !slices.Equal(got, want) {
			t.Fatalf("%s: given %v, want %v", when, got, want)
		}
	}
	for _, ask := range []struct {
		id string
		n  int64
	}{{"w1", 1000}, {"w2", 100}, {"w3", 100}, {"w4", 100}, {"w5", 100}, {"w6", 1000}} {
		expect(s.step(&connection.Event{Kind: connection.BytesRequested, Conn: w[ask.id], Bytes: ask.n}),
			fmt.Sprintf("%s asking for %d with 288 left, behind w1's 1000", ask.id, ask.n))
	}
	expect(s.end(w["w3"], nil, 0), "w3 ending, from the queue's middle")
	expect(s.end(w["w4"], nil, 0), "w4 ending, next to where w3 was")
	expect(s.end(w["w6"], nil, 0), "w6 ending, the queue's tail")
	expect(s.step(&connection.Event{Kind: connection.BytesRequested, Conn: w["w7"], Bytes: 100}), "w7 asking for 100 behind w5")
	expect(s.end(w["w1"], nil, 0), "w1 ending, the queue's head", "w2 100", "w5 100")
	expect(s.end(full[0], nil, 0), "a reader's end returning its 8048 unread", "w7 100")
	expect(s.end(w["w2"], nil, 0), "w2 ending, its 100 unread, no one waiting")
	expect(s.step(&connection.Event{Kind: connection.BytesRequested, Conn: w["w8"], Bytes: 50}), "w8 asking for 50, no one waiting", "w8 50")
}

// metrics.md: saguin_memory_in_transit_bytes{direction}, "outstanding
// reservations of each budget, incoming or outgoing (design.md, Message
// memory)", here incoming; Bytes back: "An incoming reservation
// follows its body through reading, handover and storage, and ends when
// the body is stored, refused or abandoned, at the reader's end if
// unread."
//
// For each case, beside a bystander holding a grant of 1000, a reader asks
// for 500 and is given them, the count 1500; where the case hands the body
// over to be stored, the count stays 1500 until the store answers, the
// reservation following its body. Once the case ends the 500, the count is
// 1000, so a reservation ended twice shows; the reader's end ends nothing
// more; the bystander's end brings the count to 0.
//   - Stored, at QoS 1, answered PUBACK 0x00 (R4), and at QoS 0.
//   - Refused (design.md, PUBLISH: "0x87 | [...] a topic under $SYS/"), at
//     QoS 1, answered PUBACK 0x87, and at QoS 0, answered nothing.
//   - An MQTT 3.1.1 refusal at QoS 1, which ends the connection: "At QoS 1 or
//     2 an MQTT 3.1.1 client has no refusal code and is disconnected".
//   - The store answering the body's write full: "publishes are refused
//     with 0x97" (Store), answered PUBACK 0x97.
//   - The store failing the body's write (R8).
//   - A body unsent at the failure: the reader's first body at the store,
//     its second, given 500 more, waiting behind it; the store fails the
//     first, and both end.
//   - A takeover: the connection taken over hands its body over after its
//     DISCONNECT 0x8E, and "anything its reader still delivers is ignored"
//     (CONNECT, 5).
//   - A stop: the body handed over once the stop has begun, when "any
//     other packet it takes is neither stored nor answered" (Restart,
//     Graceful stop).
//   - The reader's end with its grant unread.
func TestInTransitIsTheOutstandingReservations(t *testing.T) {
	const asked, bystander = 500, 1000
	failure := errors.New("disk I/O error")
	// stored hands conn's body over to be stored, and checks its
	// reservation follows it.
	stored := func(s *script, conn *connection.Conn, qos byte, id uint16) {
		s.t.Helper()
		s.handOver(conn, qos, id, asked)
		s.expectInTransit(bystander+asked, "the body handed over, at the store")
	}
	for _, tc := range []struct {
		name    string
		version packet.Version
		release func(s *script, conn *connection.Conn)
	}{
		{"stored at QoS 1", packet.V5, func(s *script, conn *connection.Conn) {
			stored(s, conn, 1, 1)
			if got := repliesTo(s.settle(), conn); len(got) != 1 || !isPuback(got[0], 1, 0) {
				s.t.Fatalf("a body stored, answered %v; want PUBACK 0x00", showAll(got))
			}
		}},
		{"stored at QoS 0", packet.V5, func(s *script, conn *connection.Conn) {
			stored(s, conn, 0, 0)
			s.settle()
		}},
		{"refused at QoS 1", packet.V5, func(s *script, conn *connection.Conn) {
			if got := repliesTo(s.refuse(conn, 1, 1, asked), conn); len(got) != 1 || !isPuback(got[0], 1, 0x87) {
				s.t.Fatalf("a body refused, answered %v; want PUBACK 0x87", showAll(got))
			}
		}},
		{"refused at QoS 0", packet.V5, func(s *script, conn *connection.Conn) {
			if got := repliesTo(s.refuse(conn, 0, 0, asked), conn); len(got) != 0 {
				s.t.Fatalf("a QoS 0 body refused, answered %v; want nothing", showAll(got))
			}
		}},
		{"an MQTT 3.1.1 refusal ending the connection", packet.V311, func(s *script, conn *connection.Conn) {
			s.step(&connection.Event{Kind: connection.Ended, Conn: conn, Bytes: asked,
				Packet: connection.PublishRefusal{QoS: 1, PacketID: 1}})
		}},
		{"a full batch answer", packet.V5, func(s *script, conn *connection.Conn) {
			stored(s, conn, 1, 1)
			if got := repliesTo(s.reply(only[store.Write](s), store.ErrFull), conn); len(got) != 1 || !isPuback(got[0], 1, 0x97) {
				s.t.Fatalf("a body the store answered full, answered %v; want PUBACK 0x97", showAll(got))
			}
		}},
		{"a fatal batch answer", packet.V5, func(s *script, conn *connection.Conn) {
			stored(s, conn, 1, 1)
			s.reply(only[store.Write](s), failure)
		}},
		{"a batch unsent at the failure", packet.V5, func(s *script, conn *connection.Conn) {
			stored(s, conn, 1, 1)
			first := only[store.Write](s)
			s.publish(conn, 2, asked)
			s.expectInTransit(bystander+2*asked, "two bodies handed over, the second behind the first's write")
			s.reply(first, failure)
		}},
		{"a takeover", packet.V5, func(s *script, conn *connection.Conn) {
			s.connected(connectOf(packet.V5, "x"))
			s.handOver(conn, 1, 1, asked)
		}},
		{"a stop", packet.V5, func(s *script, conn *connection.Conn) {
			s.stop()
			s.handOver(conn, 1, 1, asked)
		}},
		{"the reader's end, its grant unread", packet.V5, func(s *script, conn *connection.Conn) {
			s.end(conn, nil, 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := memoryScript(t, 4)
			keep := s.granted(bystander, "keep")[0]
			conn := s.connected(connectOf(tc.version, "x"))
			if got := s.ask(conn, asked); got != asked {
				t.Fatalf("a reader asked for %d and was given %d", asked, got)
			}
			s.expectInTransit(bystander+asked, "the two grants")
			tc.release(s, conn)
			s.expectInTransit(bystander, "the reader's reservation ended")
			if !s.conns[conn].ended {
				s.end(conn, nil, 0)
			}
			s.expectInTransit(bystander, "the reader's end, its reservation already ended")
			s.end(keep, nil, 0)
			s.expectInTransit(0, "the bystander's end")
		})
	}
}

// metrics.md: saguin_memory_full_seconds_total{direction}, "each budget's
// full time (design.md, Message memory), by the events' times. Includes
// completed intervals only; an ongoing interval is added when it ends";
// Message memory, Full, and an overload episode: "A budget is full from the
// event at which its eligible FIFO head does not fit to the first event at
// which that head fits or no head remains"; Bytes back: a reservation "ends
// when the body is stored, refused or abandoned", the store's failure and a
// stop's start abandoning bodies (Restart: "A stop for a failure [...]
// follows the graceful stop").
//
// With max_incoming_memory 16384 and max_packet_size 8192, and a second between
// events, the budget is used up at the event at which a reader's ask for
// 500 finds no room, 16000 being reserved in two bodies, publication slots
// left free (design.md, Message memory: "A granted body, unread or held
// behind its CONNECT, reserves one potential batch slot"), and the counter runs
// from it. Then:
//   - the store fails: one reader's body of 8000 is handed over, and the
//     store answers its write with an error, which ends its 8000: room,
//     two seconds after the ask;
//   - a stop begins: one of the 8000 is a body held behind its reader's
//     CONNECT, at the store, which the stop's start discards: room, one
//     second after the ask.
//
// A later event, a second later, adds nothing: the counter holds the time
// from the ask to the event that freed the bytes.
func TestMemoryFullTimeEndsWhenBytesReturn(t *testing.T) {
	full := testkit.SeriesKey("saguin_memory_full_seconds_total", "direction", "incoming")
	for _, tc := range []struct {
		name string
		want time.Duration
	}{
		{"the store fails", 2 * time.Second},
		{"a stop begins", time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := memoryScriptOf(t, 8, 2*maxPacket)
			stop := tc.name == "a stop begins"
			ids := []string{"g0", "g1"}
			if stop {
				ids = ids[:1]
			}
			givers := s.granted(8000, ids...)
			waiting := s.connected(connectOf(packet.V5, "w"))
			if stop {
				// Last, as connected settles every store request.
				held := s.accept()
				s.receive(held, keeping(packet.V5, "held", 60), 0)
				s.answer(only[*store.LoadSession](s))
				s.publish(held, 1, 8000)
			}
			if got := s.ask(waiting, 500); got != 0 {
				t.Fatalf("a reader asking for 500 bytes with 16000 reserved was given %d", got)
			}
			start := s.now
			if stop {
				s.stop()
			} else {
				s.handOver(givers[0], 1, 1, 8000)
				s.reply(only[store.Write](s), errors.New("disk I/O error"))
			}
			s.expectInTransit(8000, "the 8000 ended, one grant of 8000 left")
			if got, want := s.now.Sub(start), tc.want; got != want {
				t.Fatalf("the bytes returned %v after the ask, want %v: the script's times are off", got, want)
			}
			s.end(givers[len(givers)-1], nil, 0)
			if got := time.Duration(testkit.Sample(t, s.reg, full) * float64(time.Second)); got != tc.want {
				t.Errorf("%s = %v, want %v: from the ask that found no room to the event that returned the bytes, not to the event after it",
					full, got, tc.want)
			}
		})
	}
}

// Two queues: "Each budget has its own FIFO of requests [...] A request
// receives its whole reservation or waits"; Bytes back: a reservation
// "ends when the body is stored, refused or abandoned, at the reader's end
// if unread"; R2: "The core never waits. It never
// blocks on [...] the store"; design.md, Flows, Replies: "CONNACK (when it
// changes a session), PUBACK [...] are sent only once the store confirms";
// design.md, CONNECT, 8: "Once the changes are stored, the broker sends
// CONNACK"; "The Server MUST send a CONNACK with a 0x00 (Success) Reason Code
// before sending any Packet other than AUTH [MQTT-3.2.0-1]"; after a refused
// CONNECT the server "MUST NOT process any data sent by the Client after the
// CONNECT packet" [MQTT-3.1.4-6]. Audits 261019-003a: a reader whose CONNECT
// was at the store was given nothing until its CONNACK, and every ask behind
// it waited on that store round trip.
//
// With max_incoming_memory 16384, max_packet_size 4096 and max_connections 4, a
// client "b" connected, and "c" connected then taken over, so closing: c's
// ask is never given. Reader "a" sends a CONNECT keeping a session of
// expiry 60, whose save is outstanding: a asks for 1000 and is given them,
// and b, asking for 500 behind it, is given its 500 too; a hands over its
// body of 1000, a QoS 1 PUBLISH, which gets no answer and goes to no store
// request while the CONNECT is unsaved; a asks for 800 and is given them.
// Then:
//   - the save confirmed: CONNACK 0x00, then the PUBLISH's PUBACK 0x00
//     once stored, its 1000 bytes back;
//   - the save refused for a full disk: CONNACK 0x97, no PUBACK, the
//     held PUBLISH discarded and its 1000 back; the 800 granted stay
//     charged until a hands its body over, which is discarded and returns
//     them, once: a's ask then is never given, and holds back no ask
//     behind it, b's next given at once; a's end returns nothing more;
//   - the save refused, then a's end, with the 800 unread: they come back
//     then, once.
func TestAskWhileTheConnectIsAtTheStore(t *testing.T) {
	for _, tc := range []struct {
		name  string
		saved bool // the CONNECT's save is confirmed
		hands bool // refused, a hands its granted body over before its end
	}{
		{name: "saved", saved: true},
		{name: "refused, the granted body handed over", hands: true},
		{name: "refused, the reader's end"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := memoryScript(t, 4)
			b := s.connected(connectOf(packet.V5, "b"))
			c := s.connected(connectOf(packet.V5, "c"))
			s.connected(connectOf(packet.V5, "c")) // takes c over: c's final entry, DISCONNECT 0x8E
			if got := s.ask(c, 100); got != 0 {
				t.Fatalf("a connection taken over, closing, asked for 100 bytes and was given %d", got)
			}
			s.end(c, nil, 0)
			s.close(c)

			a := s.accept()
			s.receive(a, keeping(packet.V5, "a", 60), 0)
			s.answer(only[*store.LoadSession](s))
			save := only[store.Write](s)
			if got := s.ask(a, 1000); got != 1000 {
				t.Fatalf("a reader whose CONNECT is at the store asked first for 1000 bytes, none in transit, and was given %d: "+
					"requests enter the FIFO order (Message memory), never waiting on the store (R2)", got)
			}
			if got := s.ask(b, 500); got != 500 {
				t.Fatalf("a reader asking for 500 bytes behind one whose CONNECT is at the store was given %d", got)
			}
			if got := repliesTo(s.handOver(a, 1, 1, 1000), a); len(got) != 0 {
				t.Fatalf("with the CONNECT at the store, a's PUBLISH answered %v, want nothing before the CONNACK [MQTT-3.2.0-1]", showAll(got))
			}
			if len(s.outstanding) != 1 {
				t.Fatalf("with the CONNECT unsaved, %d store requests outstanding, want its save alone: the PUBLISH waits for the CONNACK", len(s.outstanding))
			}
			if got := s.ask(a, 800); got != 800 {
				t.Fatalf("a reader whose CONNECT is at the store asked for 800 bytes and was given %d", got)
			}
			s.expectInTransit(1000+500+800, "a's body and grant and b's grant")

			if tc.saved {
				acts := s.answer(save)
				if got := repliesTo(acts, a); len(got) != 1 || connackIn(acts, a) == nil || connackIn(acts, a).ReasonCode != 0 {
					t.Fatalf("the CONNECT saved, a got %v, want CONNACK 0x00 alone: its PUBLISH is not yet stored", showAll(got))
				}
				if got := repliesTo(s.settle(), a); len(got) != 1 || !isPuback(got[0], 1, 0) {
					t.Fatalf("after the CONNACK, a got %v; want PUBACK 0x00 to its PUBLISH once stored", showAll(got))
				}
				s.expectInTransit(500+800, "a's PUBLISH stored")
				return
			}
			acts := s.reply(save, store.ErrFull)
			if got := repliesTo(acts, a); len(got) != 1 || connackIn(acts, a) == nil || connackIn(acts, a).ReasonCode != 0x97 {
				t.Fatalf("the CONNECT's save refused for a full disk, a got %v, want CONNACK 0x97 alone", showAll(got))
			}
			s.expectInTransit(500+800, "a's held PUBLISH discarded, its 800 granted still charged: a's reader may be reading them")
			if tc.hands {
				if got := repliesTo(s.handOver(a, 1, 2, 800), a); len(got) != 0 {
					t.Fatalf("after the refused CONNECT, a's PUBLISH answered %v, want nothing [MQTT-3.1.4-6]", showAll(got))
				}
				s.expectInTransit(500, "the granted body handed over after the refusal, discarded")
				if got := s.ask(a, 300); got != 0 {
					t.Fatalf("a reader whose CONNECT was refused asked for 300 bytes and was given %d", got)
				}
				s.handOver(b, 0, 0, 500)
				s.settle()
				if got := s.ask(b, 200); got != 200 {
					t.Fatalf("a reader asking for 200 bytes behind a refused connection's ask was given %d: that ask is never given, and holds back none", got)
				}
				s.end(a, nil, 0)
				s.expectInTransit(200, "a's end, its grant already returned")
				return
			}
			s.end(a, nil, 0)
			s.expectInTransit(500, "a's end, its 800 unread")
		})
	}
}

// isPuback reports whether r is a PUBACK of reason to packet identifier id.
func isPuback(r connection.Reply, id uint16, reason byte) bool {
	a, ok := r.(*connection.Ack)
	return ok && a.Type == packet.PUBACK && a.PacketID == id && a.ReasonCode == reason
}

// design.md, Message memory, Bytes back: "A delivery reservation ends when
// the writer reports completion or discard"; Store, A full disk: "publishes
// are refused with 0x97" (the PUBACK is TestInTransitIsTheOutstandingReservations'
// full batch answer), the refused transaction never committed, so nothing
// is delivered from it.
//
// a and b subscribe at QoS 0, and their writers hold what they are handed.
// The store refuses the first publication, full: neither is handed it, and
// nothing is reserved. The second is committed and handed to both. A new
// connection takes a over: the reservations stay as they were until a's
// writer reports its delivery discarded, which leaves b's; the stop
// begins, b's stays; b's writer reports its delivery written: none is left.
func TestDeliveryReservationsEndWithTheirReports(t *testing.T) {
	d := newDeliveryBench(t, newConfig(t), "reports/topic", "body")
	a := d.connect(0, connectOf(packet.V5, "a"))
	d.subscribe(a, "reports/topic", 0)
	b := d.connect(1, connectOf(packet.V5, "b"))
	d.subscribe(b, "reports/topic", 0)
	d.publisher = d.connect(2, connectOf(packet.V5, "publisher"))
	d.hold = true

	d.refuseRecords = true
	d.publishNext(d.bodyOf(), nil)
	d.records = d.records[:0] // the refused publication's record was never stored
	if len(d.held) != 0 {
		t.Fatalf("a publication refused full was handed to %d writers; want none", len(d.held))
	}
	if got := inTransit(d.tb, d.reg, "outgoing"); got != 0 {
		t.Fatalf("a publication refused full, %d bytes are reserved; want none", got)
	}

	d.refuseRecords = false
	d.publishNext(d.bodyOf(), nil)
	if len(d.held) != 2 || d.held[0].conn != a || d.held[1].conn != b {
		t.Fatalf("the publication was handed to %d writers; want a's, then b's", len(d.held))
	}
	handed := inTransit(d.tb, d.reg, "outgoing")
	if handed == 0 {
		t.Fatalf("two deliveries held by their writers, no bytes are reserved")
	}
	d.connect(3, connectOf(packet.V5, "a"))
	if got := inTransit(d.tb, d.reg, "outgoing"); got != handed {
		t.Fatalf("a taken over, its writer not yet reporting, %d bytes are reserved; want %d, as before", got, handed)
	}
	d.report(0, connection.DeliveryDiscarded)
	left := inTransit(d.tb, d.reg, "outgoing")
	if left == 0 || left >= handed {
		t.Fatalf("a's writer reported its delivery discarded: %d bytes reserved, of %d; want b's still reserved, a's released", left, handed)
	}
	d.drain(d.c.BeginStop(nil, d.now))
	if got := inTransit(d.tb, d.reg, "outgoing"); got != left {
		t.Fatalf("the stop begun, b's writer not yet reporting, %d bytes are reserved; want %d, as before", got, left)
	}
	d.report(1, connection.DeliveryWritten)
	if got := inTransit(d.tb, d.reg, "outgoing"); got != 0 {
		t.Fatalf("every writer reported, %d bytes are still reserved; want none", got)
	}
}

// design.md, Message memory, Two queues: "An outgoing delivery reserves
// its encoded packet size before loading its payload"; "A delivery of a
// compressed payload also reserves the stored copy's size. That
// reservation is given back once the store reader has decompressed it";
// "Separate deliveries are charged separately even when they share
// underlying bytes"; Bytes back: "A delivery reservation ends when the
// writer reports completion or discard". Execution, Ready work: "Ending,
// cancelling, refusing or replacing work clears its waits and temporary
// reservations", a cancelled delivery's reservation, handed to no writer,
// ending only once its payload read is answered. Store, Compression: "A
// store reader decompresses it once per read, whatever the number of
// recipients"; Testing: the busy path "avoids every copy or allocation it
// can".
//
// On a real store, with the writers stuck, reporting nothing until the
// test reports for them: A and D subscribe to shared/# over MQTT 5 at
// QoS 1, B over MQTT 5 at QoS 0 and C over MQTT 3.1.1 at QoS 1. P publishes
// shared/x at QoS 1, a payload of 64 KiB: half noise, which its reader
// compresses to about half, or noise, stored as received. The record's
// payload is loaded for the four deliveries by one read, whose answer the
// store holds back from the core: meanwhile each delivery holds its own
// encoded size, which its version and QoS decide, and the stored copy's
// size, if any. D's connection ends while that read is outstanding: the
// outgoing budget still holds what it held while loading, D's reservation
// included, its stored copy not yet decompressed and no writer having
// reported it. The answer then reaches the core: A, B and C are each
// handed the record's payload, one body for the three, decompressed once
// or handed on, and D nothing; each of the three holds its own encoded size
// alone, every stored copy's reservation given back, D's whole reservation
// too.
// A's writer reports its delivery written; B's connection ends and its
// writer reports its delivery discarded; C's writer reports its delivery
// written: each report gives back exactly that delivery's encoded size,
// B's connection's end alone giving back nothing, and none is left.
func TestSharedBodiesAreChargedPerDelivery(t *testing.T) {
	const topic, size = "shared/x", 64 << 10
	for _, tc := range []struct {
		name       string
		payload    func(n int) []byte
		compressed bool
	}{
		{"stored compressed", halfNoise, true},
		{"stored as received", noise, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLiveScript(t, "")
			pub := s.connect("p", 0, 10)
			a := s.connect("a", 0, 10)
			b := s.connect("b", 0, 10)
			c, _ := s.connectWith(connectOf(packet.V311, "c"))
			d := s.connect("d", 0, 10)
			s.subscribe(a, "shared/#")
			s.subscribe(c, "shared/#")
			s.subscribe(d, "shared/#")
			s.step(connection.Event{Kind: connection.Received, Conn: b, Packet: &connection.Subscribe{PacketID: 1,
				Filters: []connection.Subscription{{Filter: "shared/#", QoS: 0}}}})
			if !slices.ContainsFunc(s.replies[b], func(r connection.Reply) bool {
				ack, ok := r.(*packet.Suback)
				return ok && ack.PacketID == 1 && slices.Equal(ack.ReasonCodes, []byte{0})
			}) {
				t.Fatalf("B's SUBSCRIBE of shared/# at QoS 0 was answered %v; want SUBACK 0x00", showAll(s.replies[b]))
			}
			version := map[*connection.Conn]packet.Version{a: packet.V5, b: packet.V5, c: packet.V311, d: packet.V5}
			qos := map[*connection.Conn]byte{a: 1, b: 0, c: 1, d: 1}
			// encoded is the packet size of conn's delivery of shared/x: in its
			// version, at its QoS, a Packet Identifier above QoS 0, no property, no
			// Topic Alias (none of these clients allows one).
			encoded := func(conn *connection.Conn) int64 {
				t.Helper()
				p := &packet.Publish{QoS: qos[conn], Topic: []byte(topic)}
				if p.QoS != 0 {
					p.PacketID = 1
				}
				n, err := packet.PublishSize(version[conn], p, size)
				if err != nil {
					t.Fatalf("sizing a delivery of %s: %v", topic, err)
				}
				return int64(n)
			}

			body := tc.payload(size)
			p, n := stored(topic, 1, body)
			if (p.StoredPayload != nil) != tc.compressed {
				t.Fatalf("rig: %s's payload is not %s", topic, tc.name)
			}
			copied := int64(len(p.StoredPayload))
			var loads []store.Answer // the payload reads' answers, held back from the core
			s.answer = func(ans store.Answer) []core.Action {
				if r, ok := ans.Op.(store.Read); ok && !ans.View && len(core.PayloadsOf(r)) != 0 {
					loads = append(loads, ans)
					return nil
				}
				return s.core.Answer(nil, ans, s.now)
			}
			s.stuck = true
			s.hand(pub, n, p)
			s.serve()
			if len(loads) != 1 || len(core.PayloadsOf(loads[0].Op.(store.Read))) != 4 {
				t.Fatalf("rig: %s's payload was loaded by %d reads; the test needs one, for its four deliveries", topic, len(loads))
			}
			var loading int64
			for _, conn := range []*connection.Conn{a, b, c, d} {
				loading += encoded(conn) + copied
			}
			if got := inTransit(t, s.reg, "outgoing"); got != loading {
				t.Errorf("four deliveries of %s loading, %d bytes are in transit in the outgoing budget; want %d, each its encoded size and the stored copy's %d",
					topic, got, loading, copied)
			}

			s.leave(d)
			if got := inTransit(t, s.reg, "outgoing"); got != loading {
				t.Errorf("D's connection ended while the payload read was outstanding: %d bytes are in transit in the outgoing budget; want %d, as while loading",
					got, loading)
			}
			s.answer = nil
			s.now = s.now.Add(eventGap)
			s.take(s.core.Answer(nil, loads[0], s.now))
			s.serve()
			delivered := map[*connection.Conn]*connection.Delivery{}
			for _, h := range s.writing {
				if dl, ok := h.Reply.(*connection.Delivery); ok && string(dl.Topic) == topic {
					if delivered[h.Conn] != nil {
						t.Fatalf("a client was handed %s twice", topic)
					}
					delivered[h.Conn] = dl
				}
			}
			if delivered[d] != nil {
				t.Errorf("D, its connection ended while the payload read was outstanding, was handed %s", topic)
			}
			for _, conn := range []*connection.Conn{a, b, c} {
				if dl := delivered[conn]; dl == nil || !bytes.Equal(dl.Payload, body) {
					t.Fatalf("the payload read answered, a subscriber was not handed %s's payload", topic)
				}
			}
			if &delivered[a].Payload[0] != &delivered[b].Payload[0] || &delivered[b].Payload[0] != &delivered[c].Payload[0] {
				t.Errorf("A, B and C were handed %s's payload in more than one body; want the one the read handed on", topic)
			}
			held := encoded(a) + encoded(b) + encoded(c)
			if got := inTransit(t, s.reg, "outgoing"); got != held {
				t.Errorf("A, B and C handed %s, D gone, %d bytes are in transit in the outgoing budget; want %d, each delivery's encoded size alone",
					topic, got, held)
			}

			// report has conn's writer report its delivery of shared/x with
			// result, and checks the report gives back that delivery's encoded size.
			report := func(name string, conn *connection.Conn, result connection.DeliveryResult) {
				t.Helper()
				i := slices.IndexFunc(s.writing, func(h handed) bool { return h.Reply == delivered[conn] })
				if i < 0 {
					t.Fatalf("%s's delivery of %s is not held by its writer", name, topic)
				}
				before := inTransit(t, s.reg, "outgoing")
				s.report(i, result)
				s.serve()
				if back := before - inTransit(t, s.reg, "outgoing"); back != encoded(conn) {
					t.Errorf("%s's writer reported its delivery of %s: %d bytes given back; want %d, its encoded size", name, topic, back, encoded(conn))
				}
			}
			report("A", a, connection.DeliveryWritten)
			before := inTransit(t, s.reg, "outgoing")
			s.step(connection.Event{Kind: connection.Ended, Conn: b, Packet: &packet.Disconnect{}})
			if got := inTransit(t, s.reg, "outgoing"); got != before {
				t.Errorf("B's connection ended, its writer not yet reporting: %d bytes in transit; want %d, as before", got, before)
			}
			report("B", b, connection.DeliveryDiscarded)
			s.step(connection.Event{Kind: connection.Closed, Conn: b})
			report("C", c, connection.DeliveryWritten)
			if got := inTransit(t, s.reg, "outgoing"); got != 0 {
				t.Errorf("every writer reported, %d bytes are in transit in the outgoing budget; want none", got)
			}
		})
	}
}

// design.md, Message memory: "A reservation is taken from one budget and
// given back to that same budget. Neither budget lends to the other"; Two
// queues: "Each budget has its own FIFO of requests, bounded by one request
// per reader and one per delivery-window entry. A request receives its
// whole reservation or waits. When the head of a FIFO does not fit in its
// budget, it waits with every younger request of that FIFO behind it, and
// no younger request of that FIFO passes it. A head that waits holds back
// only its own FIFO: a delivery never waits for room in the incoming
// budget, and a body never waits for room in the outgoing one"; Bytes back:
// "A delivery reservation ends when the writer reports completion or
// discard", and an incoming one "at the reader's end if unread". Which
// request the core takes first among connections is an ordering no client
// can force (design.md, Testing).
//
// With max_incoming_memory 4096, max_packet_size 4096, max_outgoing_memory
// 2100 and the delivery admission gate disabled, A and B subscribe to big,
// C to small, at QoS 0, and every writer is stuck, reporting nothing. P
// publishes a big message, a payload of 2000: one of A's and B's
// deliveries holds the outgoing budget, the other, too large beside it,
// waits at the head of the outgoing FIFO. P publishes a small message: C's
// delivery, which would fit beside the one held, waits behind that head,
// younger. Reader R1 asks for 3000 and is given them, the outgoing head
// waiting; R2 asks for 2000, too large beside them, and waits; R3 asks for
// 100, which would fit, and waits behind R2. The writers report what they
// hold, then carry on: the waiting big delivery, then C's, are handed to
// their writers, in that order, R2 and R3 still waiting. R1 ends with its
// 3000 unread: R2 is given its 2000, then R3 its 100.
func TestEachBudgetQueuesOnItsOwn(t *testing.T) {
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxIncomingMemory, cfg.Broker.Limits.MaxPacketSize, cfg.Broker.Limits.MaxOutgoingMemory = 4096, 4096, 2100
	cfg.Broker.DeliveryAdmissionDelay = 0
	s := newPositionScriptOf(t, cfg, 0)
	p, a, b, c := new(connection.Conn), new(connection.Conn), new(connection.Conn), new(connection.Conn)
	readers := []*connection.Conn{new(connection.Conn), new(connection.Conn), new(connection.Conn)}
	for i, conn := range append([]*connection.Conn{p, a, b, c}, readers...) {
		s.connect(conn, &packet.Connect{Version: packet.V5, ClientID: fmt.Sprint("queues-", i)})
	}
	s.subscribe(a, 1, "big")
	s.subscribe(b, 1, "big")
	s.subscribe(c, 1, "small")
	s.stuck = true
	s.publish(p, &packet.Publish{Topic: []byte("big"), Payload: make([]byte, 2000)})
	if handed := len(s.deliveries); handed != 1 || s.deliveries[0].conn == c {
		t.Fatalf("the big message was handed to %d writers; want one of A's and B's, the other's delivery too large beside it", handed)
	}
	waiting := b
	if s.deliveries[0].conn == b {
		waiting = a
	}
	s.publish(p, &packet.Publish{Topic: []byte("small"), Payload: make([]byte, 10)})
	if s.receivedTopic(c, "small") {
		t.Fatal("C's small delivery was handed to its writer while an older delivery waited at the head of the outgoing FIFO; want it behind that head")
	}
	ask := func(r *connection.Conn, n int64) int64 {
		t.Helper()
		before := given(s.acts, r)
		s.event(connection.Event{Kind: connection.BytesRequested, Conn: r, Bytes: n})
		return given(s.acts, r) - before
	}
	if got := ask(readers[0], 3000); got != 3000 {
		t.Fatalf("R1 asked for 3000 with the incoming budget empty and was given %d, the outgoing FIFO's head waiting: a body never waits for room in the outgoing budget", got)
	}
	if got := ask(readers[1], 2000); got != 0 {
		t.Fatalf("R2 asked for 2000 with 1096 left and was given %d; want it waiting", got)
	}
	if got := ask(readers[2], 100); got != 0 {
		t.Fatalf("R3 asked for 100 behind R2's waiting 2000 and was given %d; no younger request passes the head", got)
	}

	from := len(s.acts)
	s.stuck = false
	s.writeAll()
	s.serve()
	if !s.handedBefore(waiting, "big", c, "small") {
		t.Fatalf("the writers reporting, the waiting big delivery handed over %v and C's small one %v; want both, the big first: a delivery never waits for room in the incoming budget",
			s.receivedTopic(waiting, "big"), s.receivedTopic(c, "small"))
	}
	if got := given(s.acts[from:], readers[1]) + given(s.acts[from:], readers[2]); got != 0 {
		t.Fatalf("the outgoing budget's room given back, R2 and R3 were given %d bytes; want none, the incoming budget holding R1's 3000", got)
	}

	from = len(s.acts)
	s.event(connection.Event{Kind: connection.Ended, Conn: readers[0]})
	var order []string
	for _, act := range s.acts[from:] {
		for i, r := range readers[1:] {
			if act.Conn == r && act.Bytes != 0 {
				order = append(order, fmt.Sprintf("R%d %d", i+2, act.Bytes))
			}
		}
	}
	if want := []string{"R2 2000", "R3 100"}; !slices.Equal(order, want) {
		t.Fatalf("R1's end returning its 3000 unread, the readers were given %v; want %v, in the order they asked", order, want)
	}
}

// willOn connects client to s, MQTT 5, Clean Start 1, with a QoS 1 Will on
// topic carrying payload and no Will Delay, and returns its connection.
func willOn(s *liveScript, client, topic string, payload []byte) *connection.Conn {
	s.t.Helper()
	conn, _ := s.connectWith(decodedConnect(&packet.Connect{Version: packet.V5, CleanStart: true, KeepAlive: 60, ClientID: client,
		WillFlag: true, Will: packet.Will{QoS: 1, Topic: topic, Payload: payload}}))
	return conn
}

// lost hands the core conn's end, its connection lost with no DISCONNECT,
// and serves what it leads to (design.md, DISCONNECT, A lost connection:
// "The Will goes after its Will Delay").
func lost(s *liveScript, conn *connection.Conn) {
	s.t.Helper()
	s.step(connection.Event{Kind: connection.Ended, Conn: conn})
	s.step(connection.Event{Kind: connection.Closed, Conn: conn})
}

// queuedWill is the topic of the Wills TestEligibleWillsPassAWithheldBody
// publishes.
const queuedWill = "wills/w"

// willStored reports whether s's database holds a record of queuedWill.
func willStored(s *liveScript) bool {
	s.t.Helper()
	return s.count(`SELECT count(*) FROM records WHERE topic = '`+queuedWill+`'`) != 0
}

// asks has conn's reader ask for a body of n bytes, serves what it leads
// to, and returns the bytes conn was given since.
func asks(s *liveScript, conn *connection.Conn, n int64) int64 {
	s.t.Helper()
	before := s.granted[conn]
	s.step(connection.Event{Kind: connection.BytesRequested, Conn: conn, Bytes: n})
	return s.granted[conn] - before
}

// design.md, Message memory, Two queues: "An incoming FIFO head is
// ineligible while its body grant is withheld (Communication). An
// ineligible incoming request does not stand in the way: the oldest
// incoming request whose body grant is not withheld is the incoming head";
// "A request receives its whole reservation or waits. When the head of a
// FIFO does not fit in its budget, it waits with every younger request of
// that FIFO behind it, and no younger request of that FIFO passes it"; "A
// granted body, unread or held behind its CONNECT, reserves one potential
// batch slot. Batches and reserved slots together obey the batch bound. A
// CONNECT that must publish a Will gets that Will's reservation, from the
// incoming budget, and its batch slot before the core grants any body
// pipelined behind that CONNECT"; the incoming budget "holds PUBLISH bodies
// from the start of their reading until they are stored or refused, and
// Wills being published". Communication: "The delivery admission gate
// applies to every client and inbound bridge. While closed, it withholds
// new publication body grants". Capacities: "Accepted, unresolved
// publication batches are limited to 4 per channel and 8 in all".
// DISCONNECT, A lost connection: "The Will goes after its Will Delay";
// CONNECT, 5: an old connection taken over "is closed [...] Its Will is
// published if it has no delay". Which incoming request the core serves
// first is an ordering no client can force (Testing).
//
// The cases, each on a live script, a real store behind the core:
//   - a Will behind a withheld body: S subscribes to gate/#, and the
//     store's answer to S's read of the record P then publishes there is
//     held back, so the gate closes once delivery_admission_delay has
//     passed. R asks for a body: withheld. W, with a Will of no delay,
//     loses its connection: its Will is published, the withheld body
//     standing not in its way. R2 asks for a body: withheld. The held
//     answer comes, and the gate opens: R is given its body, then R2, the
//     older first.
//   - a body behind its CONNECT's Will: with max_incoming_memory 5,000, W
//     holds a Will of 3,000 bytes. A second connection for W's client id
//     takes W over, and its reader asks for a body of 3,000 behind that
//     CONNECT; W's connection ends, and the store holds back the group that
//     publishes W's Will: the body is given nothing while the Will holds its
//     reservation. The group committed, the body is given.
//   - the batch bound: with publish_commit_max_records 1, four QoS 1
//     publications of P's are unresolved, the store holding the first
//     group, the bound of 4 on $broadcast reached. W loses its connection:
//     its Will on $broadcast takes no batch. The held group committed, the
//     Will is published.
func TestEligibleWillsPassAWithheldBody(t *testing.T) {
	t.Run("a Will behind a withheld body", func(t *testing.T) {
		s := newLiveScript(t, "")
		sub := s.connect("s", 0, 10)
		s.subscribe(sub, "gate/#")
		pub := s.connect("p", 0, 10)
		r, r2 := s.connect("r", 0, 10), s.connect("r2", 0, 10)
		w := willOn(s, "w", queuedWill, []byte("w"))
		var held []store.Answer
		s.answer = func(a store.Answer) []core.Action {
			if m, ok := a.Op.(*store.ReadRecordMetadata); ok && !m.Retained && len(m.Rows) != 0 {
				held = append(held, a) // S's read of gate/1, unfinished broker work
				return nil
			}
			return s.core.Answer(nil, a, s.now)
		}
		s.publish(pub, 1, "gate/1")
		s.serve()
		if len(held) == 0 {
			t.Fatal("rig: S's read of gate/1 was not held")
		}
		s.advance(s.now.Add(s.cfg.Broker.DeliveryAdmissionDelay + eventGap))
		if got := testkit.Sample(t, s.reg, gatedSeries); got != 1 {
			t.Fatalf("rig: S's read held past delivery_admission_delay, %s = %v; want the gate closed, 1", gatedSeries, got)
		}
		var order []string
		s.nudge = func(acts []core.Action) {
			for _, a := range acts {
				if a.Bytes != 0 && (a.Conn == r || a.Conn == r2) {
					order = append(order, map[*connection.Conn]string{r: "R", r2: "R2"}[a.Conn])
				}
			}
		}
		if got := asks(s, r, 100); got != 0 {
			t.Fatalf("rig: with the gate closed, R's body was given %d bytes; want it withheld", got)
		}
		lost(s, w)
		if !willStored(s) {
			t.Error("W's Will, behind R's withheld body, was not published: an ineligible incoming request does not stand in the way")
		}
		if got := asks(s, r2, 100); got != 0 {
			t.Fatalf("rig: with the gate closed, R2's body was given %d bytes; want it withheld", got)
		}
		s.answer = nil
		for _, a := range held {
			s.take(s.core.Answer(nil, a, s.now))
		}
		s.serve()
		s.advance(s.now.Add(s.cfg.Broker.DeliveryAdmissionDelay))
		if want := []string{"R", "R2"}; !slices.Equal(order, want) {
			t.Errorf("the gate opened, the withheld bodies were given in the order %v; want %v, the older keeping its place", order, want)
		}
	})
	t.Run("a body behind its CONNECT's Will", func(t *testing.T) {
		s := newLiveScript(t, "", func(cfg *config.Config) { cfg.Broker.Limits.MaxIncomingMemory = 5000 })
		old := willOn(s, "w", queuedWill, make([]byte, 3000))
		// The takeover changes no stored session: the one group it leads
		// to publishes W's Will.
		s.hold = func(*core.Group) bool { return true }
		taker := new(connection.Conn)
		s.event(connection.Event{Kind: connection.Accepted, Conn: taker,
			Packet: connection.Accept{Listener: "mqtt", Remote: &net.TCPAddr{IP: net.IPv4(10, 0, 3, 1), Port: 42000}}})
		s.event(connection.Event{Kind: connection.Received, Conn: taker, Packet: &packet.Connect{Version: packet.V5, CleanStart: true, ClientID: "w"}})
		s.event(connection.Event{Kind: connection.BytesRequested, Conn: taker, Bytes: 3000})
		s.serve()
		lost(s, old) // its reader's end and its writer's close, after DISCONNECT 0x8E
		if len(s.held) != 1 || willStored(s) {
			t.Fatalf("rig: the takeover led to %d held groups, W's Will stored %v; want one, publishing the Will", len(s.held), willStored(s))
		}
		if got := s.granted[taker]; got != 0 {
			t.Fatalf("the body pipelined behind the CONNECT that publishes W's Will was given %d bytes while the Will held its reservation; want none beside it", got)
		}
		s.hold = nil
		s.release(s.held[0])
		if !willStored(s) {
			t.Fatal("rig: the held group committed, W's Will is not stored")
		}
		if got := s.granted[taker]; got != 3000 {
			t.Errorf("W's Will stored, the body behind the CONNECT was given %d bytes; want its 3000", got)
		}
	})
	t.Run("the batch bound", func(t *testing.T) {
		s := newLiveScript(t, "", func(cfg *config.Config) { cfg.Broker.Storage.PublishCommitMaxRecords = 1 })
		pub := s.connect("p", 0, 10)
		w := willOn(s, "w", queuedWill, []byte("w"))
		s.hold = func(g *core.Group) bool {
			return slices.ContainsFunc(g.Writes, func(w store.Write) bool { return len(core.RecordsOf(w)) != 0 })
		}
		for id := range uint16(4) {
			s.publish(pub, id+1, fmt.Sprintf("bound/%d", id))
		}
		s.serve()
		if len(s.held) != 1 || s.core.Batches() != 4 {
			t.Fatalf("rig: P's four publications left %d groups held and %d batches unresolved; want one held, four unresolved, the bound", len(s.held), s.core.Batches())
		}
		lost(s, w)
		if s.core.Batches() != 4 || willStored(s) {
			t.Fatalf("W's Will, the batch bound reached, took a batch: %d unresolved; want four, the Will waiting for a batch slot", s.core.Batches())
		}
		s.hold = nil
		s.release(s.held[0])
		if !willStored(s) {
			t.Error("a batch committed, W's Will was not published")
		}
	})
}
