package broker_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/store"
)

// A session's bound on what it is owed from the broadcast log, RFC 0002 "How
// much a session may hold: limits.session_queue_bytes": each message counts
// as its size and its entry on the session's list (costOf), and while it is
// on the wire the in-flight PUBLISH held for it besides; past the
// bound the oldest the session holds that is not on the wire goes, its cursor
// moves past it, and each is counted as session_queue_full; no more than half
// the bound is on the wire; and a session at its bound holding nothing it can
// give up is refused the next.

// Invariant 13: the bound holds a session's memory to what it says only if
// what an entry is charged is at least what it takes. An entry on an owed
// list is charged OwedEntryCost beside its message, and one on the wire the
// drain's flight and stored entries for its packet identifier within
// WireEntryCost besides the substrate's in-flight PUBLISH. Measured on the
// heap at list lengths from a thousand to sixty thousand, the least of three
// tries each, since a collection cannot be made to count only this.
func TestWhatAnOwedEntryCostsIsWhatItIsCharged(t *testing.T) {
	flightCharge := float64(broker.WireEntryCost - mqtt.InflightEntryOverhead)
	for _, n := range []int{1000, 7700, 20000, 60000} {
		entry, flight := math.Inf(1), math.Inf(1)
		for range 3 {
			e, f := broker.OwedHeap(n)
			entry, flight = min(entry, e), min(flight, f)
		}
		t.Logf("%d entries: %.1f bytes each on the list, charged %d; %.1f each on the wire, charged %.0f",
			n, entry, broker.OwedEntryCost, flight, flightCharge)
		if entry > broker.OwedEntryCost {
			t.Errorf("at %d entries an owed entry took %.1f bytes and is charged %d: a bound of small "+
				"messages holds more memory than it says", n, entry, broker.OwedEntryCost)
		}
		if flight > flightCharge {
			t.Errorf("at %d entries a wire entry's flight took %.1f bytes and is charged %.0f", n, flight, flightCharge)
		}
		if entry <= 0 || flight <= 0 {
			t.Fatalf("at %d entries the heap moved %.1f and %.1f bytes an entry, so this measured nothing", n, entry, flight)
		}
	}
}

// Invariant 13, for a session whose only deliveries in flight are the
// broadcast log's: those are Bounded, counted on the session's list and not
// by the client's in-flight table, and the table's own cost - its map and
// its order, made by its first entry - is charged by the list while it has
// anything on the wire (owed.tableLocked), or by nothing. One and two
// deliveries a session, over 2,000 sessions, the least of three tries.
func TestWhatABroadcastOnlyInflightTableCostsIsWhatItIsCharged(t *testing.T) {
	for _, n := range []int{1, 2, 4} {
		took, charged := math.Inf(1), 0.0
		for range 3 {
			h, c := broker.BroadcastTableHeap(2000, n)
			took, charged = min(took, h), c
		}
		t.Logf("%d on the wire: %.0f bytes a session, charged %.0f", n, took, charged)
		if took <= 0 {
			t.Fatalf("the heap moved %.0f bytes a session, so this measured nothing", took)
		}
		if took > charged {
			t.Errorf("%d broadcast deliveries on the wire took %.0f bytes a session and are charged %.0f: "+
				"a session's bound holds more memory than it says", n, took, charged)
		}
	}
}

// Invariant 13: how many sessions are held is bounded by the provider's
// max_bytes, and "what limits.session_queue_bytes counts is memory". A
// session held while its client is away takes heap for as long as it is
// held - its client, its filters in two indexes, its place in the broadcast
// drain - and what it is charged (store.SessionSize) has to cover that, or a
// provider's max_bytes admits many times its figure. Before this, a session
// with one filter took 5,400 bytes and was charged 18; with ten, 17,600 and
// 90.
//
// Each shape is measured as n sessions connected once and gone, the heap
// they left against what they are charged, the least of three runs. What the
// harness keeps of each connection is counted in the heap, which makes the
// check stricter rather than looser.
//
// A shared filter is its own shape, each in a group of its own: its node
// holds a map of groups and one of members, the drain a list for the group
// and the store its cursor, none of which an ordinary filter has. A hundred
// filters is where the charge per filter meets the marginal cost of one
// rather than the first one's.
func TestWhatAnAwaySessionCostsIsWhatItIsCharged(t *testing.T) {
	type shape struct {
		filters, levels, will int
		shared                bool
		sessions              int
	}
	for _, sh := range []shape{{0, 0, 0, false, 1000}, {1, 1, 0, false, 1000}, {1, 3, 0, false, 1000},
		{10, 1, 0, false, 1000}, {10, 3, 0, false, 1000}, {100, 1, 0, false, 200}, {1, 32, 0, false, 1000},
		{1, 1, 16384, false, 1000}, {1, 1, 0, true, 1000}, {2, 1, 0, true, 1000}, {10, 1, 0, true, 1000}} {
		t.Run(fmt.Sprintf("filters=%d,levels=%d,will=%d,shared=%v", sh.filters, sh.levels, sh.will, sh.shared), func(t *testing.T) {
			took, charged := math.Inf(1), 0.0
			for range 3 {
				h, c := awayHeap(t, sh.sessions, sh.filters, sh.levels, sh.will, sh.shared)
				took, charged = min(took, h), c
			}
			t.Logf("%.0f bytes of heap each, charged %.0f", took, charged)
			if took <= 0 {
				t.Fatalf("the heap moved %.0f bytes a session, so this measured nothing", took)
			}
			if took > charged {
				t.Errorf("an away session took %.0f bytes and is charged %.0f: a provider's max_bytes "+
					"holds more memory than it says", took, charged)
			}
		})
	}
}

// awayHeap connects n sessions that outlive their connections, each with
// the given number of filters of the given depth - every one distinct, down
// to its first level, and where shared each in a group of its own - and a
// Will of the given size, ends each connection, and returns the heap each
// left behind and what each is charged.
func awayHeap(t *testing.T, n, filters, levels, will int, shared bool) (heap, charged float64) {
	t.Helper()
	h := brokertest.Start(t)
	measure := func() int64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int64(m.HeapAlloc)
	}
	before := measure()
	var sum int64
	for i := range n {
		id := fmt.Sprintf("away-%05d", i)
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatal(err)
		}
		c := &rawClient{t: t, conn: conn}
		cp := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
			Connect:    &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: id, Keepalive: 60},
			Properties: packets.Properties{SessionExpiryInterval: 3600, SessionExpiryIntervalFlag: true}}
		sess := store.Session{Client: id, ExpiryInterval: 3600}
		if will > 0 {
			cp.Connect.WillFlag = true
			cp.Connect.WillTopic = "wills/" + id
			cp.Connect.WillPayload = bytes.Repeat([]byte("w"), will)
			cp.Properties.WillDelayInterval = 3600
			sess.Will = &store.SessionWill{Topic: cp.Connect.WillTopic, Payload: cp.Connect.WillPayload}
		}
		c.write(cp)
		if pk := c.read(); pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
			t.Fatalf("connect %s: %+v", id, pk)
		}
		for f := range filters {
			filter := fmt.Sprintf("s%05d-%d", i, f) + strings.Repeat("/x", levels-1)
			if shared {
				filter = fmt.Sprintf("$share/g%05d-%d/", i, f) + filter
			}
			c.subscribeRaw(filter, uint16(f+1))
			sess.Subscriptions = append(sess.Subscriptions, store.SessionSubscription{Filter: filter, QoS: 1})
		}
		_ = conn.Close()
		sum += store.SessionSize(sess)
	}
	deadline := time.Now().Add(20 * time.Second)
	for h.Srv.Info.ClientsConnected.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d connections still open 20s after they were closed", h.Srv.Info.ClientsConnected.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	away := 0
	for _, cl := range h.Srv.Clients.GetAll() {
		if cl.Closed() && !cl.Net.Inline {
			away++
		}
	}
	if away != n {
		t.Fatalf("%d of %d sessions are held away, so this measures something else", away, n)
	}
	grew := measure() - before
	return float64(grew) / float64(n), float64(sum) / float64(n)
}

// capAt gives the next harness a limits.session_queue_bytes of bound.
func capAt(t *testing.T, bound int64) {
	t.Helper()
	brokertest.SessionQueueBytes = bound
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
}

// sized is the i'th of a run of messages that each cost the same.
func sized(i int, payload string) store.Record {
	return store.Record{MessageID: fmt.Sprintf("m%05d", i), Topic: "news/a", Payload: []byte(payload), QoS: 1}
}

// costOf is what a message counts against a session's bound.
func costOf(r store.Record) int64 { return store.RecordSize(r) + broker.OwedEntryCost }

// queueFull is how many messages sessions have given up, or been refused, at
// their bound: saguin_session_deliveries_dropped_total{cause="session_queue_full"}.
func queueFull(h *brokertest.Harness) int64 { return h.Srv.Info.SessionQueueDropped.Load() }

func offsets(rs []store.Record) []uint64 {
	var out []uint64
	for _, r := range rs {
		out = append(out, r.Offset)
	}
	return out
}

// A session past its bound gives up its oldest, each counted once, and holds
// the newest that fit; a message only it was owed leaves the log. Another
// session on the same log is owed all of its own, and none of its messages
// leaves. On both providers.
func TestASessionPastItsBoundGivesUpItsOldestAndNoOtherSessionDoes(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	capAt(t, 10*c+c/2)
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			h.B.AttachBroadcast("full")
			h.B.AttachBroadcast("light")
			before := queueFull(h)
			var ms []store.Record
			for i := range 25 {
				to := []string{"full"}
				if i%5 == 0 {
					to = append(to, "light")
				}
				ms = append(ms, owe(t, h, lg, m(i), to...))
			}

			if got, bytes := owedOf(h, "full"); !slices.Equal(got, offsets(ms[15:])) || bytes != 10*c {
				t.Fatalf("full owes %v (%d bytes), want the newest ten %v (%d)", got, bytes, offsets(ms[15:]), 10*c)
			}
			if got := queueFull(h) - before; got != 15 {
				t.Errorf("%d counted as session_queue_full, want the 15 full gave up", got)
			}
			var light []uint64
			for i := 0; i < 25; i += 5 {
				light = append(light, ms[i].Offset)
			}
			if got, _ := owedOf(h, "light"); !slices.Equal(got, light) {
				t.Errorf("light owes %v, want all five of its own %v", got, light)
			}
			eventually(t, "what only full was owed and gave up gone from the log, and light's kept", 5*time.Second,
				func() bool {
					for i, r := range ms[:15] {
						held, _ := lg.ReadAt(r.Offset)
						if (len(held) == 1) != (i%5 == 0) {
							return false
						}
					}
					return true
				})
		})
	}
}

// The bound counts each entry's 8 bytes, so a stream of messages with no
// payload stops at it like any other: the list holds what the bound admits
// and no more (invariant 13), and the rest is counted. A message larger than
// the bound is taken all the same once nothing older is left to give up: it
// is never refused for its size alone.
func TestAStreamOfTinyMessagesStopsAtTheBound(t *testing.T) {
	tiny := func(i int) store.Record { return store.Record{MessageID: fmt.Sprintf("%06d", i), Topic: "n/t", QoS: 1} }
	c := costOf(tiny(0))
	const bound = 1000
	capAt(t, bound)
	h, lg, _ := drainHarness(t)
	h.B.AttachBroadcast("dev")
	before := queueFull(h)
	const n = 5000
	for i := range n {
		owe(t, h, lg, tiny(i), "dev")
	}
	keep := bound / c
	owed, bytes := owedOf(h, "dev")
	if int64(len(owed)) != keep || bytes != keep*c {
		t.Fatalf("after %d empty messages the session owes %d (%d bytes), want the %d the bound of %d admits",
			n, len(owed), bytes, keep, bound)
	}
	if got := queueFull(h) - before; got != n-keep {
		t.Errorf("%d counted as session_queue_full, want %d", got, n-keep)
	}
	eventually(t, "the log holding only what the session is owed", 5*time.Second, func() bool {
		return lg.Len() == int(keep)
	})

	large := owe(t, h, lg, store.Record{MessageID: "large", Topic: "n/t", Payload: make([]byte, 2*bound), QoS: 1}, "dev")
	if got, _ := owedOf(h, "dev"); !slices.Equal(got, []uint64{large.Offset}) {
		t.Fatalf("after a message twice the bound the session owes %v, want only it (%d)", got, large.Offset)
	}
	if got := queueFull(h) - before; got != n {
		t.Errorf("%d counted as session_queue_full, want %d: every earlier message given up for it", got, n)
	}
}

// A session at its bound holding nothing it can give up - all of it on the
// wire - is refused the next message, counted; one that fits beside what is
// on the wire is taken. And no more than half the bound is on the wire: the
// one that fits waits until the first is acknowledged, except that one
// message is always written when nothing is on the wire, however large.
func TestASessionHoldingOnlyWhatIsOnTheWireIsRefusedTheNext(t *testing.T) {
	bigRec, refusedRec, fitsRec := sized(0, strings.Repeat("b", 700)), sized(1, strings.Repeat("r", 400)),
		sized(2, strings.Repeat("f", 100))
	// The first on the wire, which costs the in-flight PUBLISH held for it
	// too, and room beside it for the small one and not the middling one:
	// more than half the bound is on the wire.
	onWire := costOf(bigRec) + broker.WireEntryCost + broker.WireTableCost
	capAt(t, onWire+costOf(fitsRec)+(costOf(refusedRec)-costOf(fitsRec))/2)
	h, lg, _ := drainHarness(t)
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	before := queueFull(h)

	big := owe(t, h, lg, bigRec, "raw")
	first := c.read()
	if first.FixedHeader.Type != packets.Publish || string(first.Payload) != string(big.Payload) {
		t.Fatalf("the first message, more than half the bound, was not written: %+v", sentOf(first))
	}

	refused := owe(t, h, lg, refusedRec, "raw")
	if got := queueFull(h) - before; got != 1 {
		t.Fatalf("%d counted as session_queue_full, want the one refused", got)
	}
	if got, _ := owedOf(h, "raw"); !slices.Equal(got, []uint64{big.Offset}) {
		t.Fatalf("the session owes %v, want only what is on the wire (%d)", got, big.Offset)
	}
	eventually(t, "the refused message gone from the log", 5*time.Second, func() bool {
		held, _ := lg.ReadAt(refused.Offset)
		return len(held) == 0
	})

	fits := owe(t, h, lg, fitsRec, "raw")
	if got, _ := owedOf(h, "raw"); !slices.Equal(got, []uint64{big.Offset, fits.Offset}) {
		t.Fatalf("the session owes %v, want what is on the wire and the one that fits", got)
	}
	if got := queueFull(h) - before; got != 1 {
		t.Errorf("%d counted as session_queue_full, want still the one", got)
	}
	if !c.quiet(300 * time.Millisecond) {
		t.Fatal("the connection ended")
	}
	c.ack(first)
	if next := c.read(); string(next.Payload) != string(fits.Payload) {
		t.Fatalf("after the acknowledgement the broker sent %+v, want the one that fit", sentOf(next))
	}
}

// A client that reads and never acknowledges holds half its bound on the
// wire, and no more; what waits is the newest that fit in the other half, and
// the rest is given up, counted. Once it acknowledges it is sent that newest,
// in order. On both providers.
func TestAClientThatNeverAcknowledgesHoldsHalfItsBoundAndIsSentTheNewest(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	// Half the bound is ten on the wire, each its message and the in-flight
	// PUBLISH held for it; the other half is as many waiting as fit.
	// And the table held for what is on the wire, once (WireTableCost).
	w := c + broker.WireEntryCost
	capAt(t, 20*w+broker.WireTableCost)
	waiting := int(10 * w / c)
	sent := 10 + waiting + 30
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			raw, _ := connectRaw(t, h, "raw", 100)
			raw.subscribeRaw("news/#", 1)
			h.B.AttachBroadcast("raw")
			before := queueFull(h)

			var ms []store.Record
			for i := range 10 {
				ms = append(ms, owe(t, h, lg, m(i), "raw"))
			}
			var wire []packets.Packet
			for range 10 {
				wire = append(wire, raw.read())
			}
			for i := 10; i < sent; i++ {
				ms = append(ms, owe(t, h, lg, m(i), "raw"))
			}
			if !raw.quiet(300 * time.Millisecond) {
				t.Fatal("the connection ended")
			}
			newest := ms[sent-waiting:]
			want := append(offsets(ms[:10]), offsets(newest)...)
			if got, bytes := owedOf(h, "raw"); !slices.Equal(got, want) || bytes != 10*w+broker.WireTableCost+int64(waiting)*c {
				t.Fatalf("the session owes %d (%d bytes), want the ten on the wire and the newest %d (%d)",
					len(got), bytes, waiting, 10*w+broker.WireTableCost+int64(waiting)*c)
			}
			if got := queueFull(h) - before; got != 30 {
				t.Errorf("%d counted as session_queue_full, want 30", got)
			}

			for _, pk := range wire {
				raw.ack(pk)
			}
			for _, r := range newest {
				pk := raw.read()
				if string(pk.Payload) != string(r.Payload) {
					t.Fatalf("after it acknowledged, the broker sent %q, want %q", pk.Payload, r.Payload)
				}
				raw.ack(pk)
			}
			eventually(t, "everything acknowledged", 5*time.Second, func() bool {
				got, _ := owedOf(h, "raw")
				return len(got) == 0
			})
			if got := queueFull(h) - before; got != 30 {
				t.Errorf("%d counted as session_queue_full, want still 30", got)
			}
		})
	}
}

// A session owed a backlog while it was away is written, when it comes back,
// no more than half its bound in its first batch - each delivery its message
// and the in-flight PUBLISH held for it - whatever room its window has.
func TestAResumedSessionIsWrittenNoMoreThanHalfItsBound(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	w := costOf(m(0)) + broker.WireEntryCost
	capAt(t, 20*w)
	h, lg, _ := drainHarness(t)
	c, _ := connectRaw(t, h, "raw", 100)
	c.subscribeRaw("news/#", 1)
	_ = c.conn.Close()
	sessionGone(t, h, "raw")
	for i := range 30 {
		owe(t, h, lg, m(i), "raw")
	}
	back, _ := connectRaw(t, h, "raw", 100)
	got := len(back.publishesUntilQuiet(300 * time.Millisecond))
	if got != 10 {
		t.Fatalf("a session back to thirty owed, with a window of a hundred, was written %d before it "+
			"acknowledged anything, want 10: half its bound", got)
	}
}

// A start counts what a session has on the wire as the bound counts it on the
// wire, its message and the in-flight PUBLISH held for it, so the bound after
// a restart is the bound before it. On both providers.
func TestAStartCountsWhatIsOnTheWireAsTheBoundDoes(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	w := costOf(m(0)) + broker.WireEntryCost
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			dev := durable(t, h, "dev", true, 10, "news/#")
			for i := range 3 {
				owe(t, h, lg, m(i), "dev")
			}
			awaitCount(t, dev, 3, 5*time.Second)
			eventually(t, "three on the wire", 5*time.Second, func() bool {
				_, bytes := owedOf(h, "dev")
				return bytes == 3*w+broker.WireTableCost
			})
			dev.Close()
			sessionGone(t, h, "dev")
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			if got, bytes := owedOf(h, "dev"); len(got) != 3 || bytes != 3*w+broker.WireTableCost {
				t.Fatalf("after the start the session owes %v (%d bytes), want the three on the wire (%d)",
					got, bytes, 3*w+broker.WireTableCost)
			}
		})
	}
}

// A write that fails part way through a batch - a client that stopped
// reading, hung up at limits.write_timeout - puts what it had not written
// back to waiting, and gives back the wire those held: the list's totals are
// its entries' costs as they stand, and the session still owes every one.
func TestAWriteThatFailsMidBatchGivesBackTheWireOfWhatItDidNotWrite(t *testing.T) {
	brokertest.WriteDeadline = 200 * time.Millisecond
	t.Cleanup(func() { brokertest.WriteDeadline = 0 })
	capAt(t, 256<<20)
	h, lg, _ := drainHarness(t)
	deaf, _ := connectRaw(t, h, "deaf", 1000)
	deaf.subscribeRaw("news/#", 1)
	h.B.AttachBroadcast("deaf")
	// Far more than the sockets between them buffer, so a write blocks and
	// times out with most of the batch still to write.
	const n = 120
	big := strings.Repeat("x", 256<<10)
	var ms []store.Record
	for i := range n {
		ms = append(ms, owe(t, h, lg, sized(i, big), "deaf"))
	}
	sessionGone(t, h, "deaf")
	eventually(t, "the batch done", 5*time.Second, func() bool {
		return len(h.B.PickedBroadcast("deaf")) == 0
	})
	bytes, wire, entryBytes, entryWire := h.B.OwedTotals("deaf")
	if bytes != entryBytes || wire != entryWire {
		t.Fatalf("after the failed write the list holds %d bytes, %d on the wire, and its entries come to %d "+
			"and %d: the wire of what was not written was not given back", bytes, wire, entryBytes, entryWire)
	}
	if got, _ := owedOf(h, "deaf"); len(got) != n {
		t.Fatalf("the session owes %d, want all %d", len(got), n)
	}
	if entryWire == 0 || entryWire >= entryBytes {
		t.Fatalf("%d of %d bytes on the wire: the write failed before anything or after everything, so "+
			"nothing was put back and this proves nothing", entryWire, entryBytes)
	}
}

// Invariant 13: enforcement happens before allocation, and "a restart is not
// a way around any of them: what a start puts back it also counts". So a
// list a start puts back is held to its bound while the start reads the log,
// not only once it has. Before this the bound was applied at the end: a
// session whose cursor one unacknowledged delivery held at the log's first
// offset was owed, while the start counted, every message its filter matched
// that other sessions still owed - measured, 117,760 entries on a list its
// bound held to 291. A shared group's list is put back the same way.
func TestAStartHoldsEachListToItsBoundWhileItCounts(t *testing.T) {
	const bound = 64 << 10
	for _, group := range []bool{false, true} {
		name := "session"
		if group {
			name = "group"
		}
		t.Run(name, func(t *testing.T) {
			s := store.NewSessions()
			lg, err := s.Log()
			if err != nil {
				t.Fatal(err)
			}
			list := "A"
			if group {
				list = "$share/g/t/#"
				if err := s.Save(store.Session{Client: "G", ExpiryInterval: 3600, DisconnectedAt: time.Now(),
					Subscriptions: []store.SessionSubscription{{Filter: list, QoS: 1, Since: 1}}}); err != nil {
					t.Fatal(err)
				}
				if err := s.CreateShareCursor(list, 1); err != nil {
					t.Fatal(err)
				}
			} else if err := s.Save(store.Session{Client: "A", ExpiryInterval: 3600, DisconnectedAt: time.Now(),
				Subscriptions: []store.SessionSubscription{{Filter: "t/#", QoS: 1, Since: 1}}}); err != nil {
				t.Fatal(err)
			}
			// 100 other sessions, each owed its own topic up to its bound: what
			// a running broker leaves in the log for them.
			const k = 100
			for i := range k {
				if err := s.Save(store.Session{Client: fmt.Sprintf("B%03d", i), ExpiryInterval: 3600,
					DisconnectedAt: time.Now(), Subscriptions: []store.SessionSubscription{
						{Filter: fmt.Sprintf("t/%03d", i), QoS: 1, Since: 1}}}); err != nil {
					t.Fatal(err)
				}
			}
			payload := make([]byte, 128)
			charge := costOf(store.Record{Topic: "t/000", Payload: payload, QoS: 1, MessageID: "m0000000",
				Timestamp: time.Now()})
			each := int(bound / charge)
			n := 0
			for range each {
				for i := range k {
					r, err := lg.Append(store.Record{Topic: fmt.Sprintf("t/%03d", i), Payload: payload, QoS: 1,
						MessageID: fmt.Sprintf("m%07d", n), Timestamp: time.Now()})
					if err != nil {
						t.Fatal(err)
					}
					if n == 0 && !group {
						// A was sent the first and never acknowledged it.
						if err := s.SetInFlightAll("A", 10, []store.InFlight{{Offset: r.Offset, PacketID: 1, QoS: 1,
							State: store.MessageSent}}); err != nil {
							t.Fatal(err)
						}
					}
					n++
				}
			}

			peak, reads, after, err := broker.StartPeak(lg, s, bound, list, group)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%d messages in the log, %d reads: %s's list held at most %d bytes while the start counted, "+
				"%d after; bound %d", n, reads, list, peak, after, bound)
			if reads < 10 {
				t.Fatalf("the start read the log in %d batches, so nothing was sampled while it counted", reads)
			}
			if after <= 0 {
				t.Fatalf("%s's list is empty after the start, so it was never put back and this proves nothing", list)
			}
			if peak > bound {
				t.Errorf("a start put %d bytes on %s's list before holding it to a bound of %d: %.0fx, "+
					"allocated before it was enforced", peak, list, int64(bound), float64(peak)/float64(bound))
			}
			if after > bound {
				t.Errorf("%s's list holds %d bytes after the start, over its bound of %d", list, after, int64(bound))
			}
		})
	}
}

// A start counts again what each session is owed, and a session owed more
// than its bound - one lowered across the restart - gives up its oldest then,
// counted, and keeps the newest. On both providers.
func TestAStartUnderASmallerBoundGivesUpTheOldest(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			away := durable(t, h, "away", true, 10, "news/#")
			away.Close()
			sessionGone(t, h, "away")
			var ms []store.Record
			for i := range 20 {
				ms = append(ms, owe(t, h, lg, m(i), "away"))
			}
			if got, _ := owedOf(h, "away"); len(got) != 20 {
				t.Fatalf("before the stop the session owes %d, want 20", len(got))
			}
			h.Stop()

			capAt(t, 10*c+c/2)
			h = p.start(t)
			lg = attachDrain(t, h)
			if got, bytes := owedOf(h, "away"); !slices.Equal(got, offsets(ms[10:])) || bytes != 10*c {
				t.Fatalf("after the start the session owes %v (%d bytes), want the newest ten %v (%d)",
					got, bytes, offsets(ms[10:]), 10*c)
			}
			if got := queueFull(h); got != 10 {
				t.Errorf("%d counted as session_queue_full at the start, want 10", got)
			}
			eventually(t, "the ten given up gone from the log", 5*time.Second, func() bool {
				held, _ := lg.ReadAt(offsets(ms[:10])...)
				return len(held) == 0
			})
			// Back, and acknowledging nothing: its cursor is past what it
			// gave up all the same.
			connectRx(t, h, "away", false, true, 10)
			eventually(t, "the cursor past what was given up", 5*time.Second, func() bool {
				pos, ok, _ := lg.Position(store.MQTTReader("away"))
				return ok && pos.Offset == ms[10].Offset
			})
		})
	}
}

// gatesWrites is a session store that, once armed, holds the drain's next
// record of what a batch is putting on the wire until it is let go, and
// refuses those records while failing is set.
type gatesWrites struct {
	drainSessions
	armed   atomic.Bool
	failing atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatesWrites() *gatesWrites {
	return &gatesWrites{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *gatesWrites) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	if len(fs) > 0 && fs[0].State == store.MessageSent {
		if s.armed.CompareAndSwap(true, false) {
			close(s.entered)
			<-s.release
		}
		if s.failing.Load() {
			return errors.New("the store is refusing writes")
		}
	}
	return s.drainSessions.SetInFlightAll(client, window, fs)
}

func (s *gatesWrites) let() { s.once.Do(func() { close(s.release) }) }

func (s *gatesWrites) await(t *testing.T) {
	t.Helper()
	select {
	case <-s.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no batch recorded anything, so this proves nothing")
	}
}

// A batch picks what it writes and lets go of the session's lock to read and
// record it. A count past the bound meanwhile gives up the oldest the session
// has waiting, which is not what the batch picked: nothing counted lost is
// then written. And the message the batch picked, which another session is
// owed too, is let go of once, so it stays in the log for that session.
func TestABoundRacingABatchGivesUpNothingItThenSends(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	// Room for the one the batch picks, which costs its place on the wire
	// too, and two waiting: the fourth gives up one.
	capAt(t, 3*c+broker.WireEntryCost+broker.WireTableCost+c/2)
	h := start(t)
	st := newGatesWrites()
	t.Cleanup(st.let)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	raw := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")
	h.B.AttachBroadcast("other")
	before := queueFull(h)

	st.armed.Store(true)
	m0 := owe(t, h, lg, m(0), "raw", "other")
	st.await(t)
	m1 := owe(t, h, lg, m(1), "raw")
	m2 := owe(t, h, lg, m(2), "raw")
	m3 := owe(t, h, lg, m(3), "raw")
	if got := queueFull(h) - before; got != 1 {
		t.Fatalf("%d counted as session_queue_full, want 1", got)
	}
	if got, _ := owedOf(h, "raw"); !slices.Equal(got, offsets([]store.Record{m0, m2, m3})) {
		t.Fatalf("while the batch held the first the session owes %v, want the first, the third and the fourth: "+
			"the oldest waiting (%d) given up, not the one picked (%d)", got, m1.Offset, m0.Offset)
	}
	st.let()

	for _, r := range []store.Record{m0, m2, m3} {
		pk := raw.read()
		if string(pk.Payload) != string(r.Payload) {
			t.Fatalf("the broker sent %q, want %q; %q was given up", pk.Payload, r.Payload, m1.Payload)
		}
		raw.ack(pk)
	}
	if !raw.quiet(300 * time.Millisecond) {
		t.Fatal("the connection ended")
	}
	eventually(t, "raw's acknowledgements written", 5*time.Second, func() bool {
		got, _ := owedOf(h, "raw")
		return len(got) == 0
	})
	if got := h.B.BroadcastOwners(m0.Offset); got != 1 {
		t.Errorf("the first message has %d owners, want 1: other is still owed it", got)
	}
	if held, _ := lg.ReadAt(m0.Offset); len(held) != 1 {
		t.Error("the first message left the log while other is still owed it")
	}
}

// A batch that cannot record what it picked puts it back to waiting, and
// gives back its share of the wire, so the next batch sends it.
func TestABatchThatCannotRecordWhatItPickedPutsItBack(t *testing.T) {
	r := sized(0, "refused a record")
	capAt(t, 2*costOf(r))
	h := start(t)
	st := newGatesWrites()
	t.Cleanup(st.let)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")

	st.armed.Store(true)
	st.failing.Store(true)
	r = owe(t, h, lg, r, "raw")
	st.await(t)
	if got := h.B.PickedBroadcast("raw"); !slices.Equal(got, []uint64{r.Offset}) {
		t.Fatalf("while its record was out the session had %v picked, want %d", got, r.Offset)
	}
	st.let()
	eventually(t, "the refused message back to waiting", 5*time.Second, func() bool {
		return len(h.B.PickedBroadcast("raw")) == 0
	})
	if got, bytes := owedOf(h, "raw"); !slices.Equal(got, []uint64{r.Offset}) || bytes != costOf(r) {
		t.Fatalf("the session owes %v (%d bytes), want still %d, waiting (%d): what it held for the wire "+
			"is given back with the wire", got, bytes, r.Offset, costOf(r))
	}
	st.failing.Store(false)
	h.B.WakeBroadcast("raw")
	if pk := c.read(); string(pk.Payload) != string(r.Payload) {
		t.Fatalf("once the store took records again the broker sent %+v, want the message put back", sentOf(pk))
	}
}

// A delivery at QoS 0 that a failed write left unwritten goes back to
// waiting, like one at QoS 1, and is sent when the session resumes. A session
// subscribed at QoS 0 is sent a message at QoS 0 though it was counted at a
// higher QoS.
func TestAQoS0DeliveryAFailedWriteLeftIsSentOnResume(t *testing.T) {
	zero := store.Record{MessageID: "zero", Topic: "news/zero", Payload: []byte("at QoS 0"), QoS: 1}
	one := store.Record{MessageID: "one", Topic: "news/one", Payload: []byte("at QoS 1"), QoS: 1}
	// Half of it holds both on the wire, and nothing more: a share of the
	// wire the failed writes did not give back would hold them both back.
	capAt(t, 2*(costOf(zero)+costOf(one)+2*broker.WireEntryCost))
	h := start(t)
	st := newGatesWrites()
	t.Cleanup(st.let)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	raw, _ := connectRaw(t, h, "raw", 10)
	raw.subscribeRaw("news/one", 1)
	raw.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: 2,
		Filters: packets.Subscriptions{{Filter: "news/zero", Qos: 0}}})
	if pk := raw.read(); pk.FixedHeader.Type != packets.Suback || len(pk.ReasonCodes) != 1 || pk.ReasonCodes[0] != 0 {
		t.Fatalf("subscribe at QoS 0: %+v", pk)
	}
	h.B.AttachBroadcast("raw")
	_ = raw.conn.Close()
	sessionGone(t, h, "raw")

	zero = owe(t, h, lg, zero, "raw")
	one = owe(t, h, lg, one, "raw")
	st.armed.Store(true)
	back, present := connectRaw(t, h, "raw", 10)
	if !present {
		t.Fatal("the session did not resume")
	}
	st.await(t)
	// Gone while the batch that picked both was recording: its writes fail.
	gone := h.Disconnects.Snapshot("raw")
	_ = back.conn.Close()
	brokertest.SessionGoneAfter(t, h, "raw", gone)
	st.let()
	eventually(t, "the batch done with both", 5*time.Second, func() bool {
		return len(h.B.PickedBroadcast("raw")) == 0
	})
	if got, bytes := owedOf(h, "raw"); len(got) != 2 || bytes != costOf(zero)+costOf(one) {
		t.Fatalf("after the failed writes the session owes %v (%d bytes), want both waiting (%d): what "+
			"they held for the wire is given back with it", got, bytes, costOf(zero)+costOf(one))
	}

	again, present := connectRaw(t, h, "raw", 10)
	if !present {
		t.Fatal("the session did not resume a second time")
	}
	for _, r := range []store.Record{zero, one} {
		pk := again.read()
		if string(pk.Payload) != string(r.Payload) {
			t.Fatalf("after the resume the broker sent %q, want %q", pk.Payload, r.Payload)
		}
		if pk.FixedHeader.Qos == 1 {
			again.ack(pk)
		}
	}
}

// What a batch picks and lets go of without sending - expired, here - gives
// back its share of the wire: a message more than half the bound is still
// written next, as one always is when nothing is on the wire.
func TestWhatABatchLetsGoOfGivesBackItsShareOfTheWire(t *testing.T) {
	const bound = 1000
	capAt(t, bound)
	h, lg, _ := drainHarness(t)
	c := dialRaw(t, h, "raw", "news/#")
	h.B.AttachBroadcast("raw")

	expired := sized(0, "expired")
	expired.MessageExpiry, expired.Timestamp = 1, time.Now().Add(-2*time.Second)
	gone := owe(t, h, lg, expired, "raw")
	eventually(t, "the expired message let go", 5*time.Second, func() bool {
		got, _ := owedOf(h, "raw")
		return !slices.Contains(got, gone.Offset)
	})
	big := owe(t, h, lg, sized(1, strings.Repeat("b", 700)), "raw")
	if pk := c.read(); string(pk.Payload) != string(big.Payload) {
		t.Fatalf("the broker sent %+v, want the message more than half the bound", sentOf(pk))
	}
}

// queueGauges scrapes saguin_session_queue_messages and
// saguin_session_queue_bytes.
func queueGauges(t *testing.T, ops string) (messages, bytes float64) {
	t.Helper()
	return metricValue(t, ops, "saguin_session_queue_messages"), metricValue(t, ops, "saguin_session_queue_bytes")
}

// awaitGauges waits for the queue gauges to read messages and bytes, and fails
// with what they last read. Waited for, because the operations listener
// answers a scrape inside its minimum interval with the one before.
func awaitGauges(t *testing.T, ops, what string, messages, bytes float64) {
	t.Helper()
	var m, b float64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if m, b = queueGauges(t, ops); m == messages && b == bytes {
			return
		}
	}
	t.Fatalf("%s: the gauges read %v messages and %v bytes, want %v and %v", what, m, b, messages, bytes)
}

// awaitSeries waits for a counter to read want, and fails with what it last
// read.
func awaitSeries(t *testing.T, ops, series string, want float64) {
	t.Helper()
	var got float64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if got = metricValue(t, ops, series); got == want {
			return
		}
	}
	t.Fatalf("%s read %v, want %v", series, got, want)
}

// RFC 0005 saguin_session_queue_messages and saguin_session_queue_bytes: what
// every session holds that its client has not acknowledged, each delivery
// counted once, as the bound that holds it counts it. A message a session is
// owed from the broadcast log counts its size and its entry on the list,
// connected or away, and while it is on the wire the in-flight PUBLISH held
// for it besides (WireEntryCost); a delivery in a client's in-flight table
// counts its size and InflightEntryOverhead beside it, and a client's table
// that holds any it counts InflightTableCost once; a session with anything
// of the log's on the wire is charged WireTableCost once. On both providers.
func TestTheQueueGaugesCountEachDeliveryOnceAsItsBoundCountsIt(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := float64(costOf(m(0)))
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			ops := operationsAt(t, h)
			awaitGauges(t, ops, "with nothing owed", 0, 0)

			// Owed while away: on the log's list alone.
			h.B.AttachBroadcast("away")
			for i := range 5 {
				owe(t, h, lg, m(i), "away")
			}
			awaitGauges(t, ops, "five owed to an away session", 5, 5*c)

			// Owed while connected: two on the wire, in the client's in-flight
			// table as well as on the list, and three waiting.
			raw, _ := connectRaw(t, h, "raw", 2)
			raw.subscribeRaw("news/#", 1)
			h.B.AttachBroadcast("raw")
			for i := 5; i < 10; i++ {
				owe(t, h, lg, m(i), "raw")
			}
			wire := []packets.Packet{raw.read(), raw.read()}
			awaitGauges(t, ops, "ten owed, two of them on the wire", 10, 10*c+2*broker.WireEntryCost+broker.WireTableCost)
			for _, pk := range wire {
				raw.ack(pk)
			}
			// And two of the three waiting take their place on the wire.
			awaitGauges(t, ops, "the two acknowledged", 8, 8*c+2*broker.WireEntryCost+broker.WireTableCost)

			// A delivery held in a client's in-flight table rather than owed
			// from the log: to a session that ends with its connection.
			held := brokertest.Dial(t, h, "held", true, true, 10, 0, 0)
			held.Sub(t, "loose/x", 1)
			connect(t, h, "producer", true, false).Pub(t, "loose/x", "p")
			awaitCount(t, held, 1, 5*time.Second)
			awaitGauges(t, ops, "and a delivery in a client's in-flight table", 9,
				8*c+2*broker.WireEntryCost+broker.WireTableCost+float64(mqtt.InflightTableCost+mqtt.InflightEntryOverhead+len("p")+len("loose/x")))
		})
	}
}

// RFC 0005 saguin_session_deliveries_dropped_total{cause="session_queue_full"}
// counts each message a session gives up at its bound, or is refused there,
// from the broadcast log; and the gauges hold what the bound holds.
func TestWhatASessionGivesUpAtItsBoundIsCountedAtTheEndpoint(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	capAt(t, 10*c+c/2)
	h, lg, _ := drainHarness(t)
	ops := operationsAt(t, h)
	const series = `saguin_session_deliveries_dropped_total{cause="session_queue_full"}`
	awaitSeries(t, ops, series, 0)
	h.B.AttachBroadcast("full")
	for i := range 25 {
		owe(t, h, lg, m(i), "full")
	}
	awaitSeries(t, ops, series, 15)
	awaitGauges(t, ops, "what the bound holds", 10, float64(10*c))
}

// RFC 0002 "How much a session may hold": what a session gives up at its
// bound is counted once. An away session's give-ups move its cursor in the
// store too, not only in memory, so a restart does not count again, and give
// up again, what it had already given up - which the log still holds, because
// another session is owed it. On both providers.
func TestAnAwaySessionsGiveUpsAreCountedOnceAcrossARestart(t *testing.T) {
	m := func(i int, topic string) store.Record {
		return store.Record{MessageID: fmt.Sprintf("m%05d", i), Topic: topic,
			Payload: []byte(fmt.Sprintf("payload-%03d", i)), QoS: 1}
	}
	c := costOf(m(0, "news/a/x"))
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			capAt(t, 10*c+c/2)
			h := p.start(t)
			lg := attachDrain(t, h)
			away(t, h, "full", "news/#")
			away(t, h, "keeper", "news/b/#")
			var ms []store.Record
			for i := range 5 {
				ms = append(ms, owe(t, h, lg, m(i, "news/b/x"), "full", "keeper"))
			}
			for i := 5; i < 20; i++ {
				ms = append(ms, owe(t, h, lg, m(i, "news/a/x"), "full"))
			}
			if got := queueFull(h); got != 10 {
				t.Fatalf("%d counted as session_queue_full, want the 10 full gave up", got)
			}
			if held, _ := lg.ReadAt(offsets(ms[:5])...); len(held) != 5 {
				t.Fatalf("the log holds %d of the five keeper is owed, so a restart has nothing to count again", len(held))
			}
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			if got := queueFull(h); got != 0 {
				t.Errorf("the restart counted %d more as session_queue_full: full's give-ups were counted twice", got)
			}
			if owed, _ := owedOf(h, "full"); !slices.Equal(owed, offsets(ms[10:])) {
				t.Errorf("after the restart full owes %v, want the newest ten %v", owed, offsets(ms[10:]))
			}
		})
	}
	// And what a start gives up, under a bound lowered while it was
	// stopped, is counted at that start and no other.
	for _, p := range restartables(t) {
		t.Run(p.name+"/given up at a start", func(t *testing.T) {
			h := p.start(t)
			lg := attachDrain(t, h)
			away(t, h, "full", "news/#")
			away(t, h, "keeper", "news/b/#")
			for i := range 5 {
				owe(t, h, lg, m(i, "news/b/x"), "full", "keeper")
			}
			for i := 5; i < 20; i++ {
				owe(t, h, lg, m(i, "news/a/x"), "full")
			}
			h.Stop()

			capAt(t, 10*c+c/2)
			h = p.start(t)
			attachDrain(t, h)
			if got := queueFull(h); got != 10 {
				t.Fatalf("the start under a lower bound counted %d, want the 10 full gave up", got)
			}
			eventually(t, "full's cursor written past what it gave up", 5*time.Second, func() bool {
				pos, ok, _ := attachDrain(t, h).Position(store.MQTTReader("full"))
				return ok && pos.Offset > 5
			})
			h.Stop()

			h = p.start(t)
			attachDrain(t, h)
			if got := queueFull(h); got != 0 {
				t.Errorf("the next start counted %d more: what the last start gave up was counted twice", got)
			}
		})
	}
}

// refusesAcknowledgementsCounting is a session store that refuses every write
// of a session's cursor and acknowledgements, counting each.
type refusesAcknowledgementsCounting struct {
	drainSessions
	refused atomic.Int32
}

func (s *refusesAcknowledgementsCounting) Acknowledge(string, uint64, []store.InFlight) (int, error) {
	s.refused.Add(1)
	return 0, errors.New("disk I/O error")
}

// A cursor write the store refuses, for a session that is away, is left for
// the next give-up or resume rather than asked again at once, over and over.
func TestACursorWriteTheStoreRefusesIsNotAskedInALoop(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	capAt(t, 2*c+c/2)
	kt := &keepsErrors{TB: t}
	h := start(kt)
	st := new(refusesAcknowledgementsCounting)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	away(t, h, "full", "news/#")
	for i := range 3 {
		owe(t, h, lg, m(i), "full")
	}
	eventually(t, "the cursor write refused", 5*time.Second, func() bool { return st.refused.Load() > 0 })
	// **Counted against what may ask, not against a clock.** Nothing gives up
	// again, so what may ask again is the owed-write retry, once a pass: a
	// loop asks again at once, whatever the number of passes.
	retries, restore := broker.CountOwedRetries(h.B)
	defer restore()
	first := st.refused.Load()
	awaitPasses(t, "the owed-write retry", retries, 1)
	if n, passes := int64(st.refused.Load()-first), retries(); n > passes {
		t.Errorf("the store was asked %d more times for one give-up's cursor over %d retry passes: "+
			"it is being asked in a loop", n, passes)
	}
	h.Stop()
	const want = "shutdown: the session store refused the acknowledgements of 1 sessions at the stop; " +
		"they are sent again at the next start"
	if got := kt.reported(); len(got) != 1 || got[0] != want {
		t.Errorf("the stop reported %q, want exactly %q", got, want)
	}
}

// holdsCursorWrites is a session store that, once armed, holds its next write
// of a session's cursor, with or without acknowledgements, until let go.
type holdsCursorWrites struct {
	drainSessions
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *holdsCursorWrites) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	if s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return s.drainSessions.Acknowledge(client, cursor, done)
}

func (s *holdsCursorWrites) let() { s.once.Do(func() { close(s.release) }) }

// An away session that gives more up while its cursor is being written has
// that written after it, not left in memory: the cursor ends past everything
// it gave up.
func TestGiveUpsWhileACursorIsWrittenAreWrittenAfterIt(t *testing.T) {
	m := func(i int) store.Record { return sized(i, fmt.Sprintf("payload-%03d", i)) }
	c := costOf(m(0))
	capAt(t, 2*c+c/2)
	h := start(t)
	st := &holdsCursorWrites{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(st.let)
	lg := attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		st.drainSessions = s
		return st
	})
	away(t, h, "full", "news/#")

	st.armed.Store(true)
	var ms []store.Record
	for i := range 3 {
		ms = append(ms, owe(t, h, lg, m(i), "full"))
	}
	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first give-up's cursor was never written, so this proves nothing")
	}
	for i := 3; i < 6; i++ {
		ms = append(ms, owe(t, h, lg, m(i), "full"))
	}
	st.let()
	eventually(t, "the cursor written past all it gave up", 5*time.Second, func() bool {
		pos, ok, _ := lg.Position(store.MQTTReader("full"))
		return ok && pos.Offset == ms[4].Offset
	})
}

// RFC 0002 "How much a session may hold": the bound counts "every QoS 1 and
// QoS 2 delivery on the wire and unacknowledged", a channel's own included,
// and no more than half of it is written and unacknowledged. A consumer of an
// append or a latest channel that reads and never acknowledges, with the
// Receive Maximum MQTT defaults to, held Receive Maximum x a record's size:
// 16 MiB at a 1 MiB bound. It holds half its bound
// now, one record past it at most - and nothing is given up for it: a
// channel's record waits where it is, and once the consumer acknowledges,
// every one arrives.
func TestAChannelConsumerThatNeverAcknowledgesHoldsHalfItsBound(t *testing.T) {
	for _, tc := range []struct{ name, filter, topic string }{
		{"append", "events/#", "events/%03d"},
		{"latest", "state/#", "state/%03d"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const bound, n = 1 << 20, 64
			brokertest.SessionQueueBytes = bound
			t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
			h := brokertest.Start(t)

			conn, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			c := &rawClient{t: t, conn: conn}
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
				Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "hoarder", Keepalive: 60, Clean: true}})
			if pk := c.read(); pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
				t.Fatalf("connect: %+v", pk)
			}
			c.subscribeRaw(tc.filter, 1)

			pub := brokertest.Connect(t, h, "producer", true, false)
			payload := strings.Repeat("x", 64<<10)
			for i := range n {
				pub.Pub(t, fmt.Sprintf(tc.topic, i), payload)
			}

			first := c.publishesUntilQuiet(time.Second)
			cl, ok := h.Srv.Clients.Get("hoarder")
			if !ok {
				t.Fatal("the consumer is not connected")
			}
			held := cl.State.Inflight.Bytes()
			one := int64(mqtt.InflightEntryOverhead + len(payload) + 64)
			t.Logf("%d of %d records of 64KiB written before any acknowledgement; the in-flight table holds %d KiB; bound %d KiB",
				len(first), n, held>>10, bound>>10)
			if len(first) == 0 {
				t.Fatalf("no record was written before any acknowledgement, so this proves nothing")
			}
			if held > bound/2+one {
				t.Errorf("a consumer that never acknowledges holds %d KiB unacknowledged, past half its %d KiB bound",
					held>>10, bound>>10)
			}

			// Nothing given up: acknowledged, every record arrives, once.
			seen := map[string]int{}
			batch := first
			for len(batch) > 0 {
				for _, pk := range batch {
					seen[pk.TopicName]++
					c.ack(pk)
				}
				batch = c.publishesUntilQuiet(time.Second)
			}
			if len(seen) != n {
				t.Errorf("once acknowledged, %d of %d records arrived: a channel's record was given up", len(seen), n)
			}
			for topic, k := range seen {
				if k != 1 {
					t.Errorf("%s arrived %d times", topic, k)
				}
			}
		})
	}
}

// Invariant 13: a retained value served on SUBSCRIBE waits on the client's
// pending list while its window is full, and Retain Handling 0 has every
// SUBSCRIBE sent the retained values. The list kept every SUBSCRIBE's copy:
// 200 SUBSCRIBEs of one filter left 20,099 waiting for a store of 100 topics. It keeps one value per topic and subscription
// now, and every topic still arrives once the client acknowledges.
func TestRepeatedSubscribesKeepOneRetainedValueATopicWaiting(t *testing.T) {
	h := brokertest.StartRetaining(t, 0)
	pub := brokertest.Connect(t, h, "producer", true, false)
	const values = 100
	for i := range values {
		pub.PubRetained(t, fmt.Sprintf("r/%03d", i), "v")
	}

	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &rawClient{t: t, conn: conn}
	// A session that ends with its connection, so nothing goes to the
	// broadcast log, and a window of one.
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
		Connect:    &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "holder", Keepalive: 60, Clean: true},
		Properties: packets.Properties{ReceiveMaximum: 1}})
	if pk := c.read(); pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
		t.Fatalf("connect: %+v", pk)
	}
	c.subscribeRaw("r/#", 1)
	first := c.read()
	if first.FixedHeader.Type != packets.Publish || first.FixedHeader.Qos != 1 {
		t.Fatalf("the first retained value came as %+v", first)
	}
	if n := broker.PendingValues(h.B, "holder", broker.RetainedStoreName); n != values-1 {
		t.Fatalf("after one SUBSCRIBE %d values wait, want %d with one on the wire, so this proves nothing", n, values-1)
	}
	for i := range 200 {
		c.subscribeRaw("r/#", uint16(i+2))
	}
	// A SUBSCRIBE's retained values are queued after its SUBACK is written,
	// and the connection's next packet is read only once that returns, so
	// the SUBACK to one more is the proof the 200th has queued its values.
	c.subscribeRaw("nothing-retained/#", 202)
	if n := broker.PendingValues(h.B, "holder", broker.RetainedStoreName); n > values {
		t.Errorf("after 200 more SUBSCRIBEs %d values wait for a retained store of %d topics", n, values)
	}

	// Each acknowledged as it arrives: a window of one sends the next only
	// then.
	seen := map[string]bool{first.TopicName: true}
	c.ack(first)
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(time.Second))
		head, body, err := brokertest.ReadRawPacket(c.conn)
		if err != nil {
			break
		}
		pk := packets.Packet{ProtocolVersion: 5}
		if err := pk.FixedHeader.Decode(head); err != nil {
			t.Fatalf("fixed header: %v", err)
		}
		if pk.FixedHeader.Type != packets.Publish {
			continue
		}
		if err := pk.PublishDecode(body); err != nil {
			t.Fatalf("decode a PUBLISH: %v", err)
		}
		seen[pk.TopicName] = true
		c.ack(pk)
	}
	if len(seen) != values {
		t.Errorf("once acknowledged, %d of %d retained topics arrived", len(seen), values)
	}
}

// RFC 0002 "How much a session may hold": a channel's record that finds its
// session's share of the wire full "waits where it is, and is written as
// acknowledgements make room". A consumer that acknowledges everything it is
// sent therefore receives everything, however little half its bound holds.
// The first version of that rule did not: a `latest` value refused for want
// of wire went back to the top of its drain, which asks the window alone, and
// was taken and refused again for ever. A SUBSCRIBE's snapshot drains on the
// connection's own read loop, so the acknowledgements that would have made
// room were never read, and a replay of 1.28 million records stopped at 479
// with a core busy. So: values stored before the SUBSCRIBE, more published
// while the consumer reads, the consumer acknowledging each round only after
// it has read it, and every topic must arrive, once.
func TestAConsumerWaitingForItsWireIsFedAsItAcknowledges(t *testing.T) {
	for _, tc := range []struct {
		name, filter, topic string
		retained            bool
	}{
		{"append", "events/#", "events/%03d", false},
		{"latest", "state/#", "state/%03d", false},
		{"retained", "r/#", "r/%03d", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const bound, stored, live = 1 << 20, 48, 16
			brokertest.SessionQueueBytes = bound
			t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
			var h *brokertest.Harness
			if tc.retained {
				h = brokertest.StartRetaining(t, 0)
			} else {
				h = brokertest.Start(t)
			}
			pub := brokertest.Connect(t, h, "producer", true, false)
			payload := strings.Repeat("x", 64<<10)
			send := func(i int) {
				if tc.retained {
					pub.PubRetained(t, fmt.Sprintf(tc.topic, i), payload)
				} else {
					pub.Pub(t, fmt.Sprintf(tc.topic, i), payload)
				}
			}
			for i := range stored {
				send(i)
			}

			conn, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			c := &rawClient{t: t, conn: conn}
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
				Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "replayer", Keepalive: 60, Clean: true}})
			if pk := c.read(); pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
				t.Fatalf("connect: %+v", pk)
			}
			// From the floor (brokertest.StartTail is false), so the stored
			// records are a backlog, as a replay's are.
			c.subscribeRaw(tc.filter, 1)

			seen := map[string]int{}
			rounds, published, first := 0, 0, 0
			for idle := 0; idle < 3; rounds++ {
				batch := c.publishesUntilQuiet(200 * time.Millisecond)
				if rounds == 0 {
					first = len(batch)
				}
				for _, pk := range batch {
					seen[pk.TopicName]++
					c.ack(pk)
				}
				if published < live {
					for range 4 {
						send(stored + published)
						published++
					}
					continue
				}
				if len(batch) == 0 {
					idle++
				} else {
					idle = 0
				}
			}
			t.Logf("%d of %d topics arrived in %d rounds, %d in the first; half the bound holds %d records of 64KiB; %d given up",
				len(seen), stored+live, rounds, first, (bound/2)/(64<<10), queueFull(h))
			if first == 0 || first >= stored {
				t.Fatalf("the first round brought %d records, so the wire never stopped the drain and this proves nothing", first)
			}
			// Every value stored before the SUBSCRIBE arrives: a channel's
			// record is never given up, and a retained value is served from
			// the store. A live message on a broadcast topic is the one thing
			// the bound may give up, the oldest not on the wire, and each is
			// counted (session_queue_full); a channel's never is.
			given := queueFull(h)
			var missing []string
			for i := range stored {
				if seen[fmt.Sprintf(tc.topic, i)] == 0 {
					missing = append(missing, fmt.Sprintf(tc.topic, i))
				}
			}
			if len(missing) > 0 {
				t.Errorf("a consumer acknowledging everything it was sent never received %d of the %d values "+
					"stored before its SUBSCRIBE, %v: its drain stopped waiting for room its own "+
					"acknowledgements would have made", len(missing), stored, missing)
			}
			if !tc.retained && given != 0 {
				t.Errorf("%d of a channel's records were given up at the bound", given)
			}
			if int64(len(seen))+given != stored+live {
				t.Errorf("%d topics arrived and %d were given up at the bound, of %d", len(seen), given, stored+live)
			}
			for topic, k := range seen {
				if k != 1 {
					t.Errorf("%s arrived %d times", topic, k)
				}
			}
		})
	}
}

// RFC 0002 "How much a session may hold", and what it costs to wait: a cursor
// stopped because its share of the wire is full is woken by every record
// appended to its channel, and the store is not read on such a wake, because
// nothing read could be written. The acknowledgement that makes room is what
// reads again. Counted at the pump's one read seam: before, every append
// read a batch and threw it away at its first record.
func TestAConsumerStoppedOnItsWireReadsNothingUntilItAcknowledges(t *testing.T) {
	const bound, backlog, appended = 1 << 20, 16, 100
	brokertest.SessionQueueBytes = bound
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := brokertest.Start(t)

	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := &rawClient{t: t, conn: conn}
	c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
		Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "stopped", Keepalive: 60, Clean: true}})
	if pk := c.read(); pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
		t.Fatalf("connect: %+v", pk)
	}
	c.subscribeRaw("events/#", 1)

	pub := brokertest.Connect(t, h, "producer", true, false)
	payload := strings.Repeat("x", 64<<10)
	for i := range backlog {
		pub.Pub(t, fmt.Sprintf("events/%03d", i), payload)
	}
	got := c.publishesUntilQuiet(500 * time.Millisecond)
	if len(got) == 0 || len(got) >= backlog {
		t.Fatalf("%d of %d records written before any acknowledgement, so the wire never stopped the cursor", len(got), backlog)
	}

	var reads atomic.Int64
	defer broker.SetPumpBetweenReadAndCommit(func(id string) {
		if id == "stopped" {
			reads.Add(1)
		}
	})()
	for i := range appended {
		pub.Pub(t, fmt.Sprintf("events/%03d", backlog+i), payload)
	}
	if more := c.publishesUntilQuiet(500 * time.Millisecond); len(more) != 0 {
		t.Fatalf("%d more records were written to a consumer that acknowledged nothing", len(more))
	}
	whileStopped := reads.Load()

	// And the acknowledgements read again, which is what proves the count
	// above could see a read: every record arrives.
	seen := map[string]bool{}
	for batch := got; len(batch) > 0; batch = c.publishesUntilQuiet(500 * time.Millisecond) {
		for _, pk := range batch {
			seen[pk.TopicName] = true
			c.ack(pk)
		}
	}
	t.Logf("%d store reads while stopped across %d appends; %d once it acknowledged, and %d of %d records arrived",
		whileStopped, appended, reads.Load()-whileStopped, len(seen), backlog+appended)
	if reads.Load() == whileStopped {
		t.Fatalf("no store read counted once the consumer acknowledged, so the instrument counts nothing")
	}
	if whileStopped > 2 {
		t.Errorf("a consumer stopped on its wire had the store read %d times across %d appends: "+
			"each wake read a batch it could not write", whileStopped, appended)
	}
	if len(seen) != backlog+appended {
		t.Errorf("once acknowledged, %d of %d records arrived", len(seen), backlog+appended)
	}
}

// A delivery sent and unacknowledged is not expired, so it keeps its room in
// the window until it is acknowledged; one that expires never sent takes no
// room, and what waits behind it is sent once the room is made (RFC 0005
// `saguin_deliveries_expired_total`; MQTT-4.3.3-7; RFC 0002 "How much a
// session may hold": a channel's record "is written as acknowledgements make
// room"; RFC 0003's latest value is sent when the window allows).
//
// **The oracle changed with the RFC.** This test used to hold that the sent
// delivery, to a session that ends with its connection, expires on the wire
// and makes room for what waits - the RFC's rule then, and wrong: expired
// there, a QoS 2 delivery's identifier and slot went back while the client
// still held the exchange. Now the sent one keeps the window past its
// deadline, and a broadcast message behind it that expires unsent is never
// sent. A client with a window of one holds a broadcast message that expires
// in a second, unacknowledged; a second broadcast message that expires in a
// second waits behind it, and one more delivery on each row behind that.
// Once the first is acknowledged, the last, and only the last, arrives.
func TestADeliveryThatExpiresInFlightMakesRoomForWhatWaits(t *testing.T) {
	for _, row := range []struct{ name, filter, topic string }{
		{"latest", "state/#", "state/x"},
		{"append", "events/#", "events/x"},
		{"broadcast", "c/#", "c/x"},
	} {
		t.Run(row.name, func(t *testing.T) {
			h := brokertest.Start(t)
			conn, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			c := &rawClient{t: t, conn: conn}
			c.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
				Connect:    &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "slow", Keepalive: 60, Clean: true},
				Properties: packets.Properties{ReceiveMaximum: 1}})
			if pk := c.read(); pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
				t.Fatalf("connect: %+v", pk)
			}
			c.subscribeRaw("b/#", 1)
			c.subscribeRaw(row.filter, 2)

			pconn, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pconn.Close() })
			p := &rawClient{t: t, conn: pconn}
			p.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Connect}, ProtocolVersion: 5,
				Connect: &packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: "pub", Keepalive: 60, Clean: true}})
			if pk := p.read(); pk.FixedHeader.Type != packets.Connack {
				t.Fatalf("connect the publisher: %+v", pk)
			}
			// The PUBACK is read here rather than by rawClient.read, which
			// reads what a subscriber is sent.
			pub := func(topic string, id uint16, expiry uint32) {
				p.write(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: id,
					TopicName: topic, Payload: []byte(fmt.Sprintf("v%d", id)),
					Properties: packets.Properties{MessageExpiryInterval: expiry}})
				_ = pconn.SetReadDeadline(time.Now().Add(3 * time.Second))
				first, body, err := brokertest.ReadRawPacket(pconn)
				if err != nil || first>>4 != packets.Puback {
					t.Fatalf("publish %s: header %#x, %v", topic, first, err)
				}
				if len(body) > 2 && body[2] >= 0x80 {
					t.Fatalf("publish %s refused: %#x", topic, body[2])
				}
			}

			pub("b/x", 1, 1)
			sent := c.publishesUntilQuiet(300 * time.Millisecond)
			if len(sent) != 1 || sent[0].TopicName != "b/x" {
				t.Fatalf("want the expiring message in the one slot, got %d", len(sent))
			}
			pub("b/y", 2, 1)
			pub(row.topic, 3, 0)
			if got := c.publishesUntilQuiet(300 * time.Millisecond); len(got) != 0 {
				t.Fatalf("%s went out past a full window, so nothing waited", got[0].TopicName)
			}
			cl, ok := h.Srv.Clients.Get("slow")
			if !ok {
				t.Fatal("the subscriber is not connected")
			}
			// Both messages expire at one second and the sweep runs each
			// second; four is room for both.
			got := c.publishesUntilQuiet(4 * time.Second)
			_, onWire := cl.State.Inflight.Get(sent[0].PacketID)
			t.Logf("after the expiry: %d in flight, the sent one among them %v, send quota %d, sent %d",
				cl.State.Inflight.Len(), onWire, cl.State.Inflight.SendQuota(), len(got))
			if len(got) != 0 {
				t.Fatalf("%s was sent past a window the unacknowledged delivery still holds", got[0].TopicName)
			}
			if !onWire {
				t.Fatalf("the delivery on the wire was expired, giving back room the client still held (MQTT-4.3.3-7)")
			}
			c.ack(sent[0])
			got = c.publishesUntilQuiet(time.Second)
			if len(got) != 1 || got[0].TopicName != row.topic {
				names := []string{}
				for _, pk := range got {
					names = append(names, pk.TopicName)
				}
				t.Fatalf("acknowledged, the client was sent %v, want only %s: the message that expired "+
					"unsent was sent, or what waited behind it was not", names, row.topic)
			}
			// And the room is whole again: acknowledged, the window and the
			// quota are back where a fresh connection starts.
			c.ack(got[0])
			if !waitUntil(3*time.Second, func() bool {
				return cl.State.Inflight.Len() == 0 && cl.State.Inflight.SendQuota() == 1
			}) {
				t.Errorf("acknowledged, the client has %d in flight and send quota %d, want 0 and 1",
					cl.State.Inflight.Len(), cl.State.Inflight.SendQuota())
			}
		})
	}
}
