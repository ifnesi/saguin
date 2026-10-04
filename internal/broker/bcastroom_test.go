package broker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/store"
)

// The broadcast log at its provider's bound. Nothing publishes into the log
// until the switch, so these tests keep each message through the drain as the
// switch will (Broker.KeepBroadcast). Oracles are RFC 0002's full-store table
// and RFC 0003 "Broadcast": a full log costs the sessions behind it, never
// the publisher.

// storageFull is the counter a session's loss to a full log is counted in.
const storageFull = `saguin_session_deliveries_dropped_total{cause="storage_full"}`

// boundedDrain is a broker whose session provider is bounded at about room
// bytes, with its broadcast log on the drain, on the provider named. free is
// the room a memory provider has left, which it counts exactly; nil on
// sqlite, which counts pages.
func boundedDrain(t *testing.T, provider string, room int64) (h *brokertest.Harness, lg drainLog, free func() int64) {
	t.Helper()
	switch provider {
	case "memory":
		h, lg, s := drainHarness(t)
		q := store.NewQuota(room, 0)
		s.SetQuota(q)
		return h, lg, func() int64 { return room - q.Bytes() }
	case "sqlite":
		brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+room, "16KiB")
		h := startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
		return h, attachDrain(t, h), nil
	}
	t.Fatalf("no provider %q", provider)
	return nil, nil, nil
}

// sessionCharge is what a session of the given client id and filters counts
// against its provider (store.SessionSize), for a bound sized to hold it.
func sessionCharge(id string, filters ...string) int64 {
	sess := store.Session{Client: id}
	for _, f := range filters {
		sess.Subscriptions = append(sess.Subscriptions, store.SessionSubscription{Filter: f})
	}
	return store.SessionSize(sess)
}

// away is a durable session subscribed to filter whose client has gone: what
// it is owed waits on its list.
func away(t *testing.T, h *brokertest.Harness, id, filter string) {
	t.Helper()
	c := durable(t, h, id, true, 10, filter)
	before := h.Disconnects.Snapshot(id)
	c.Close()
	sessionGoneAfter(t, h, id, before)
}

// filler is a QoS 1 message on news/<topic> whose payload is size bytes.
func filler(topic string, i, size int) store.Record {
	r := news(topic, "")
	r.Payload = bytes.Repeat([]byte{byte('a' + i%26)}, size)
	r.MessageID = store.NewDeliveryID()
	r.Timestamp = time.Now()
	return r
}

// heldOf is which of offs the log still holds.
func heldOf(t *testing.T, lg drainLog, offs []uint64) map[uint64]bool {
	t.Helper()
	recs, err := lg.ReadAt(offs...)
	if err != nil {
		t.Fatalf("read the log: %v", err)
	}
	held := map[uint64]bool{}
	for _, r := range recs {
		held[r.Offset] = true
	}
	return held
}

// RFC 0002's full-store table, RFC 0003 "Broadcast"
//
// **A full log costs the sessions behind it, never the publisher.** Two
// sessions are away: a on news/#, owed every message, and b on news/a, owed
// half of them. Messages are kept until the provider has been full for a
// while. Every one is kept - the log's oldest go to make room - and each
// session is counted once for each message it was owed and lost: its owed
// list is exactly what it was owed less what went, and storage_full moves by
// the two sessions' losses together. What went is the oldest. On a memory
// provider it is also the fewest: the room left is less than one message,
// which one give-up too many would have left. A sqlite provider frees room
// by the page, so there it may cost more. And what goes has gone when keep
// returns: each message a has lost is already out of the log, since the room
// is what the publisher was waiting for.
func TestAFullLogCostsTheSessionsBehindItNeverThePublisher(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			h, lg, free := boundedDrain(t, provider, 256*1024)
			ops := operationsAt(t, h)
			away(t, h, "a", "news/#")
			away(t, h, "b", "news/a")

			var offs, toB []uint64
			var size int64
			for i := 0; i < 160; i++ {
				topic, to := "b", []string{"a"}
				if i%2 == 0 {
					topic, to = "a", []string{"a", "b"}
				}
				rec := filler(topic, i, 4096)
				size = store.RecordSize(rec)
				got, kept, err := h.B.KeepBroadcast(rec, to...)
				if err != nil || !kept {
					t.Fatalf("message %d: kept %v (%v), want kept: the log had older messages to give up", i, kept, err)
				}
				offs = append(offs, got.Offset)
				if topic == "a" {
					toB = append(toB, got.Offset)
				}
				// a is owed every message: one off its list was given up, and
				// has left the log by now.
				owedA, _ := owedOf(h, "a")
				held := heldOf(t, lg, offs)
				for _, off := range offs {
					if !slices.Contains(owedA, off) && held[off] {
						t.Fatalf("offset %d was given up and is still in the log once keep returned", off)
					}
				}
			}

			held := heldOf(t, lg, offs)
			var wantA, wantB []uint64
			var lostA, lostB int
			newestLost, oldestHeld := uint64(0), ^uint64(0)
			for _, off := range offs {
				if held[off] {
					wantA = append(wantA, off)
					if slices.Contains(toB, off) {
						wantB = append(wantB, off)
					}
					oldestHeld = min(oldestHeld, off)
					continue
				}
				lostA++
				if slices.Contains(toB, off) {
					lostB++
				}
				newestLost = max(newestLost, off)
			}
			if lostA == 0 || lostB == 0 {
				t.Fatalf("a lost %d and b %d: both had to lose something for this to prove anything", lostA, lostB)
			}
			if newestLost > oldestHeld {
				t.Errorf("offset %d was given up and %d kept: the oldest go first", newestLost, oldestHeld)
			}
			if got, _ := owedOf(h, "a"); !slices.Equal(got, wantA) {
				t.Errorf("a is owed %v, want %v: what it was owed less what went", got, wantA)
			}
			if got, _ := owedOf(h, "b"); !slices.Equal(got, wantB) {
				t.Errorf("b is owed %v, want %v: what it was owed less what went", got, wantB)
			}
			if got := scrapeGauges(t, ops)[storageFull]; got != float64(lostA+lostB) {
				t.Errorf("%s is %v, want %d: a lost %d and b %d", storageFull, got, lostA+lostB, lostA, lostB)
			}
			if free != nil && free() >= size {
				t.Errorf("the provider has %d bytes free after the last message, room for another of "+
					"%d: more was given up than it needed", free(), size)
			}
			t.Logf("%s: %d kept, %d held; a lost %d, b lost %d", provider, len(offs), len(wantA), lostA, lostB)
		})
	}
}

// RFC 0002's full-store table, RFC 0003 "Broadcast"
//
// **A message a session has on the wire is not given up, for anybody.** c is
// connected and has m on the wire, unacknowledged; d is away and owes m too.
// The provider fills with messages owed to d alone. m is the oldest, and it
// stays: in the log, on d's list, and on the wire to c. What goes is the
// oldest after it, and only d is counted for them.
func TestAMessageOnTheWireIsNotGivenUp(t *testing.T) {
	h, lg, _ := boundedDrain(t, "memory", 128*1024)
	ops := operationsAt(t, h)
	c := durable(t, h, "c", true, 10, "news/#")
	away(t, h, "d", "news/#")

	m, kept, err := h.B.KeepBroadcast(filler("a", 0, 4096), "c", "d")
	if err != nil || !kept {
		t.Fatalf("m: kept %v (%v)", kept, err)
	}
	awaitCount(t, c, 1, 5*time.Second)

	var fill []uint64
	for i := 1; i < 80; i++ {
		got, kept, err := h.B.KeepBroadcast(filler("b", i, 4096), "d")
		if err != nil || !kept {
			t.Fatalf("filler %d: kept %v (%v)", i, kept, err)
		}
		fill = append(fill, got.Offset)
	}
	held := heldOf(t, lg, append([]uint64{m.Offset}, fill...))
	lost := 0
	want := []uint64{m.Offset}
	for _, off := range fill {
		if held[off] {
			want = append(want, off)
		} else {
			lost++
		}
	}
	if lost == 0 {
		t.Fatal("nothing was given up, so this proves nothing")
	}
	if !held[m.Offset] {
		t.Fatal("m left the log while c had it on the wire")
	}
	if got, _ := owedOf(h, "d"); !slices.Equal(got, want) {
		t.Errorf("d is owed %v, want %v: m, and what of the rest was not given up", got, want)
	}
	if got := scrapeGauges(t, ops)[storageFull]; got != float64(lost) {
		t.Errorf("%s is %v, want %d: d's losses, and nothing of m", storageFull, got, lost)
	}
	if got := h.B.BroadcastOwners(m.Offset); got != 2 {
		t.Errorf("m has %d owners, want 2: c on the wire and d", got)
	}
}

// RFC 0002's full-store table
//
// **Where nothing in the log can go, the new message is not kept, and is
// counted for each session it was for - and the publisher is still answered
// as for any other.** The only message in the log is on the wire to c, and
// the provider has no room for another: keeping one more for c and d gives
// up nothing, keeps nothing, answers no error, and counts two.
func TestAMessageTheFullLogCannotMakeRoomForIsCountedForEachSession(t *testing.T) {
	h, lg, _ := boundedDrain(t, "memory", 16*1024+sessionCharge("c", "news/#")+sessionCharge("d", "news/#"))
	ops := operationsAt(t, h)
	c := durable(t, h, "c", true, 10, "news/#")
	away(t, h, "d", "news/#")

	m, kept, err := h.B.KeepBroadcast(filler("a", 0, 8192), "c", "d")
	if err != nil || !kept {
		t.Fatalf("m: kept %v (%v)", kept, err)
	}
	awaitCount(t, c, 1, 5*time.Second)

	next := lg.Next()
	_, kept, err = h.B.KeepBroadcast(filler("a", 1, 8192), "c", "d", "c")
	if err != nil {
		t.Fatalf("a message the log had no room for answered %v: the publisher is not the one who pays", err)
	}
	if kept {
		t.Fatal("the message was kept: the provider had room, so this proves nothing")
	}
	if lg.Next() != next {
		t.Errorf("the log's next offset moved from %d to %d for a message it did not keep", next, lg.Next())
	}
	if held := heldOf(t, lg, []uint64{m.Offset}); !held[m.Offset] {
		t.Error("m, on the wire to c, was given up")
	}
	if got := scrapeGauges(t, ops)[storageFull]; got != 2 {
		t.Errorf("%s is %v, want 2: one for each session the message was for", storageFull, got)
	}
	if got, _ := owedOf(h, "d"); !slices.Equal(got, []uint64{m.Offset}) {
		t.Errorf("d is owed %v, want only m", got)
	}
}

// RFC 0002's full-store table
//
// **What a full log gives up is said as well as counted: loudly, and not
// once per message.** A WARN names how many messages went, how many sessions
// lost them and how many deliveries that was, and the remedy, and it comes
// at most once an interval with the totals since the last. Two away sessions
// lose messages over about three intervals of give-ups: the lines' deliveries
// add up to the counter, no two lines come closer than the interval, there
// is more than one, and there are far fewer lines than give-ups.
func TestAFullLogSaysWhatItGaveUpOnceAnInterval(t *testing.T) {
	const every = 400 * time.Millisecond
	t.Cleanup(broker.SetGiveUpReportEvery(every))
	var mu sync.Mutex
	var lines []string
	brokertest.LogTo = func(l string) {
		if strings.Contains(l, "the broadcast log is full") {
			mu.Lock()
			lines = append(lines, l)
			mu.Unlock()
		}
	}
	t.Cleanup(func() { brokertest.LogTo = nil })
	h, _, _ := boundedDrain(t, "memory", 64*1024)
	ops := operationsAt(t, h)
	away(t, h, "a", "news/#")
	away(t, h, "b", "news/a")

	gaveUp := 0
	for i := 0; i < 60; i++ {
		topic, to := "b", []string{"a"}
		if i%2 == 0 {
			topic, to = "a", []string{"a", "b"}
		}
		before := scrapeGauges(t, ops)[storageFull]
		if _, kept, err := h.B.KeepBroadcast(filler(topic, i, 4096), to...); err != nil || !kept {
			t.Fatalf("message %d: kept %v (%v)", i, kept, err)
		}
		if scrapeGauges(t, ops)[storageFull] > before {
			gaveUp++
		}
		time.Sleep(20 * time.Millisecond)
	}
	lost := scrapeGauges(t, ops)[storageFull]
	if gaveUp < 10 {
		t.Fatalf("%d give-ups, too few for a burst: this proves nothing", gaveUp)
	}

	field := regexp.MustCompile(`(time|messages|sessions|deliveries)=(\S+)`)
	var said float64
	eventually(t, "the WARN lines to add up to the counter", 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		said = 0
		for _, l := range lines {
			for _, m := range field.FindAllStringSubmatch(l, -1) {
				if m[1] == "deliveries" {
					v, _ := strconv.ParseFloat(m[2], 64)
					said += v
				}
			}
		}
		return said == lost
	})
	// **Until the drain has been asked once more past the interval**, which
	// is when a line too many would be written: the last line was written
	// before now, so a call this late is past it by the interval.
	asked := make(chan struct{})
	var once sync.Once
	due := time.Now().Add(every)
	restore := broker.SetAfterGiveUpAsked(func(b *broker.Broker, now time.Time) {
		if b == h.B && !now.Before(due) {
			once.Do(func() { close(asked) })
		}
	})
	select {
	case <-asked:
	case <-time.After(every + 5*time.Second):
		t.Fatal("the drain was not asked to say what it gave up past the interval")
	}
	restore()
	mu.Lock()
	defer mu.Unlock()
	var last time.Time
	for _, l := range lines {
		if !strings.Contains(l, "level=WARN") || !strings.Contains(l, "broker.session.storage a provider of its own") {
			t.Errorf("the line is not a WARN naming the remedy: %s", l)
		}
		var at time.Time
		for _, m := range field.FindAllStringSubmatch(l, -1) {
			switch m[1] {
			case "time":
				at, _ = time.Parse(time.RFC3339Nano, m[2])
			case "messages", "sessions":
				if v, _ := strconv.Atoi(m[2]); v < 1 {
					t.Errorf("%s=%s on a line that reports a loss: %s", m[1], m[2], l)
				}
			}
		}
		if !last.IsZero() && at.Sub(last) < every-50*time.Millisecond {
			t.Errorf("two lines %v apart, closer than the interval of %v", at.Sub(last), every)
		}
		last = at
	}
	if len(lines) < 2 || len(lines) > gaveUp/4 {
		t.Errorf("%d lines for %d give-ups over about three intervals: want more than one, and one an "+
			"interval rather than one a give-up", len(lines), gaveUp)
	}
	t.Logf("%d give-ups, %v deliveries lost, %d lines", gaveUp, lost, len(lines))
}

// sharedProvider is a broker whose sessions, broadcast log and append channel
// events are on one memory provider, "local", bounded at room bytes - the
// arrangement a configuration gets when broker.session.storage is left to
// default to the channels' provider. It answers the channel's log too.
//
// On sqlite the harness names its session provider apart from its channels'
// though both are in one file, so the broker cannot see them as one
// provider there; the store-level refusal that tells the two bounds apart is
// held on both (TestAProviderRefusalIsToldApartFromAStoresOwnBound).
func sharedProvider(t *testing.T, room int64) (*brokertest.Harness, drainLog, *store.Log) {
	t.Helper()
	h := startDurable(t, t.TempDir())
	lg := attachDrain(t, h)
	q := store.NewQuota(room, 0)
	sessions, ok := brokertest.HarnessSessions.(*store.Sessions)
	if !ok {
		t.Fatalf("the harness's session store is a %T, not a memory store", brokertest.HarnessSessions)
	}
	sessions.SetQuota(q)
	l := broker.LogOf(h.B, "events")
	events, ok := l.(*store.Log)
	if !ok {
		t.Fatalf("the events channel's log is a %T, not a memory log", l)
	}
	events.SetQuota(q)
	return h, lg, events
}

// pubCode publishes at QoS 1 and answers the PUBACK's reason code.
func pubCode(t *testing.T, c *brokertest.Client, topic string, payload []byte) byte {
	t.Helper()
	pr, err := c.C.Publish(context.Background(), &paho.Publish{Topic: topic, QoS: 1, Payload: payload})
	if pr != nil {
		return pr.ReasonCode
	}
	t.Fatalf("publish %s: %v", topic, err)
	return 0
}

// fillLog keeps messages owed to a until the log has given up its own oldest
// to make room: the provider is full, and full of the log.
func fillLog(t *testing.T, h *brokertest.Harness, ops string) {
	t.Helper()
	for i := 0; scrapeGauges(t, ops)[storageFull] == 0; i++ {
		if i > 400 {
			t.Fatal("400 messages never filled the provider, so this proves nothing")
		}
		if _, kept, err := h.B.KeepBroadcast(filler("a", i, 4096), "a"); err != nil || !kept {
			t.Fatalf("message %d: kept %v (%v)", i, kept, err)
		}
	}
}

// warned collects the full log's warning lines, from a broker started after it.
func warned(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	brokertest.LogTo = func(l string) {
		if strings.Contains(l, "the broadcast log is full") {
			mu.Lock()
			lines = append(lines, l)
			mu.Unlock()
		}
	}
	t.Cleanup(func() { brokertest.LogTo = nil })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(lines)
	}
}

// RFC 0002 "Every session's state"
//
// **The broadcast log gives way to every other writer on its provider.** The
// provider is full of the log, owed to a session that is away, and an
// append channel on the same provider is published to: each publish is
// acknowledged 0x00 and kept, the log's oldest messages go to make the room,
// a is counted for each, and the warning names the channel it made room for.
// On memory and on sqlite, where the channel and the sessions share the one
// database and its page ceiling.
func TestTheBroadcastLogGivesWayToAChannelOnItsProvider(t *testing.T) {
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			t.Cleanup(broker.SetGiveUpReportEvery(200 * time.Millisecond))
			said := warned(t)
			var h *brokertest.Harness
			switch provider {
			case "memory":
				h, _, _ = sharedProvider(t, 128*1024)
			case "sqlite":
				brokertest.SessionsNamedLikeChannels = true
				brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+256*1024, "16KiB")
				t.Cleanup(func() { brokertest.SessionsNamedLikeChannels = false })
				h = startDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
				attachDrain(t, h)
			}
			events := broker.LogOf(h.B, "events")
			ops := operationsAt(t, h)
			away(t, h, "a", "news/#")
			fillLog(t, h, ops)

			lostBefore := scrapeGauges(t, ops)[storageFull]
			owedBefore, _ := owedOf(h, "a")
			p := connect(t, h, "producer", true, false)
			for i := range 3 {
				if code := pubCode(t, p, "events/x", bytes.Repeat([]byte{'e'}, 4096)); code != 0 {
					t.Fatalf("channel publish %d on a provider the log filled was answered 0x%02X, want "+
						"0x00: the log gives way to it", i, code)
				}
			}
			if got, err := events.ReadFrom(events.Floor()); err != nil || len(got) != 3 {
				t.Errorf("the channel holds %d records (%v), want the 3 acknowledged", len(got), err)
			}
			owedAfter, _ := owedOf(h, "a")
			lost := len(owedBefore) - len(owedAfter)
			if lost < 3 || !slices.Equal(owedAfter, owedBefore[lost:]) {
				t.Errorf("a is owed %v after, %v before: the oldest, at least three, should have gone",
					owedAfter, owedBefore)
			}
			eventually(t, "the loss counted", 3*time.Second, func() bool {
				return scrapeGauges(t, ops)[storageFull] == lostBefore+float64(lost)
			})
			eventually(t, "the warning naming the channel", 3*time.Second, func() bool {
				for _, l := range said() {
					if strings.Contains(l, "channel events") {
						return true
					}
				}
				return false
			})
			t.Logf("%s: the channel's 3 records cost a %d messages", provider, lost)
		})
	}
}

// RFC 0002 "Every session's state"
//
// **The log gives way only to its provider's refusal, and only where it has
// something to give.** A channel at its own max_bytes is refused 0x97 though
// the log holds messages: the log's room is not the channel's, so nothing is
// given up for it. And a provider full with nothing in the log refuses as a
// provider with no log does.
func TestTheBroadcastLogGivesWayOnlyWhereItCanMakeRoom(t *testing.T) {
	t.Run("a channel at its own bound", func(t *testing.T) {
		brokertest.Bound = map[string]int64{"events": 8 * 1024}
		t.Cleanup(func() { brokertest.Bound = nil })
		h, _, _ := sharedProvider(t, 512*1024)
		ops := operationsAt(t, h)
		away(t, h, "a", "news/#")
		for i := range 20 {
			if _, kept, err := h.B.KeepBroadcast(filler("a", i, 4096), "a"); err != nil || !kept {
				t.Fatalf("message %d: kept %v (%v)", i, kept, err)
			}
		}
		owedBefore, _ := owedOf(h, "a")
		p := connect(t, h, "producer", true, false)
		refused := false
		for i := range 5 {
			if code := pubCode(t, p, "events/x", bytes.Repeat([]byte{'e'}, 3000)); code != 0 {
				if code != 0x97 {
					t.Fatalf("publish %d answered 0x%02X, want 0x97", i, code)
				}
				refused = true
				break
			}
		}
		if !refused {
			t.Fatal("the channel took five records past its 8KiB bound, so this proves nothing")
		}
		if got, _ := owedOf(h, "a"); !slices.Equal(got, owedBefore) {
			t.Errorf("a is owed %v, was %v: the log gave up messages for a channel at its own bound", got, owedBefore)
		}
		if got := scrapeGauges(t, ops)[storageFull]; got != 0 {
			t.Errorf("%s is %v, want 0", storageFull, got)
		}
	})
	t.Run("a full provider with nothing in the log", func(t *testing.T) {
		h, _, _ := sharedProvider(t, 32*1024)
		ops := operationsAt(t, h)
		p := connect(t, h, "producer", true, false)
		refused := false
		for i := range 20 {
			if code := pubCode(t, p, "events/x", bytes.Repeat([]byte{'e'}, 4096)); code != 0 {
				if code != 0x97 {
					t.Fatalf("publish %d answered 0x%02X, want 0x97", i, code)
				}
				refused = true
				break
			}
		}
		if !refused {
			t.Fatal("20 records of 4KiB fitted a 32KiB provider, so this proves nothing")
		}
		if got := scrapeGauges(t, ops)[storageFull]; got != 0 {
			t.Errorf("%s is %v, want 0: the log had nothing to give", storageFull, got)
		}
	})
}

// RFC 0002 "Every session's state"
//
// **A subscription is kept though the log filled the provider**, the log
// giving way to the session store as to any other writer: a new client's
// CONNECT and SUBSCRIBE are written, and its filter granted, where a
// provider without the log's room to give would answer 0x97.
func TestTheBroadcastLogGivesWayToASubscription(t *testing.T) {
	t.Cleanup(broker.SetGiveUpReportEvery(200 * time.Millisecond))
	said := warned(t)
	h, _, _ := sharedProvider(t, 128*1024)
	ops := operationsAt(t, h)
	away(t, h, "a", "news/#")
	fillLog(t, h, ops)
	lostBefore := scrapeGauges(t, ops)[storageFull]

	c := connectRx(t, h, "b", false, false, 10)
	var filters []paho.SubscribeOptions
	// Filters charged about 12KiB (store.SessionSize): more than the room a
	// fill leaves, which is less than one of its 4KiB messages.
	for i := range 3 {
		filters = append(filters, paho.SubscribeOptions{Topic: fmt.Sprintf("news/b/%d/%s", i, strings.Repeat("f", 300)), QoS: 1})
	}
	sa, err := c.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: filters})
	if sa == nil {
		t.Fatalf("subscribe: %v", err)
	}
	for i, r := range sa.Reasons {
		if r != 1 {
			t.Errorf("filter %d was answered 0x%02X, want granted at QoS 1: the log gives way to the session store", i, r)
		}
	}
	eventually(t, "the log's loss to the subscription counted", 3*time.Second, func() bool {
		return scrapeGauges(t, ops)[storageFull] > lostBefore
	})
	eventually(t, "the warning naming the session store", 3*time.Second, func() bool {
		for _, l := range said() {
			if strings.Contains(l, "the session store") {
				return true
			}
		}
		return false
	})
}

// RFC 0002 "Every session's state"
//
// **The log gives way on its own provider and nowhere else**, and runs the
// write again only once it has made room. A write refused by another
// provider that is full gives up nothing and is answered as it was; one
// refused by the log's own provider has the log's oldest given up and is run
// again, and answers what that second run answered.
func TestTheBroadcastLogGivesWayOnlyOnItsOwnProvider(t *testing.T) {
	h, _, _ := sharedProvider(t, 512*1024)
	ops := operationsAt(t, h)
	away(t, h, "a", "news/#")
	for i := range 10 {
		if _, kept, err := h.B.KeepBroadcast(filler("a", i, 4096), "a"); err != nil || !kept {
			t.Fatalf("message %d: kept %v (%v)", i, kept, err)
		}
	}
	owed, _ := owedOf(h, "a")

	runs := 0
	err := h.B.WithRoom("elsewhere", 4096, "a store elsewhere", func() error {
		runs++
		return store.ErrProviderFull
	})
	if !errors.Is(err, store.ErrProviderFull) || runs != 1 {
		t.Errorf("a write another provider refused answered %v after %d runs, want ErrProviderFull after one", err, runs)
	}
	if got, _ := owedOf(h, "a"); !slices.Equal(got, owed) {
		t.Errorf("a is owed %v, was %v: the log gave up messages for another provider's write", got, owed)
	}

	runs = 0
	err = h.B.WithRoom("local", 4096, "a store here", func() error {
		runs++
		if runs == 1 {
			return store.ErrProviderFull
		}
		return nil
	})
	if err != nil || runs != 2 {
		t.Errorf("a write its own provider refused answered %v after %d runs, want nil after two", err, runs)
	}
	if got, _ := owedOf(h, "a"); len(got) != len(owed)-1 || !slices.Equal(got, owed[1:]) {
		t.Errorf("a is owed %v, was %v: the oldest message, and only it, should have gone", got, owed)
	}
	eventually(t, "the loss counted", 3*time.Second, func() bool {
		return scrapeGauges(t, ops)[storageFull] == 1
	})
}

// **A give-up takes what releases are gathering before anything a session
// is owed**: nobody owes those, so a session owed the oldest message must
// not lose it for room they were about to give back (bdrain.giveUp).
func TestAGiveUpTakesWhatNobodyOwesFirst(t *testing.T) {
	h, lg, free := boundedDrain(t, "memory", 256*1024)
	h.B.SetAckCommitInterval(time.Hour)
	ops := operationsAt(t, h)
	away(t, h, "a", "news/a")
	first, kept, err := h.B.KeepBroadcast(filler("a", 0, 4096), "a")
	if err != nil || !kept {
		t.Fatalf("the first message: kept %v (%v)", kept, err)
	}
	var nobody []uint64
	for i := 1; free() >= store.RecordSize(filler("nobody", i, 4096)); i++ {
		got, kept, err := h.B.KeepBroadcast(filler("nobody", i, 4096))
		if err != nil || !kept {
			t.Fatalf("message %d: kept %v (%v)", i, kept, err)
		}
		nobody = append(nobody, got.Offset)
	}
	if held := heldOf(t, lg, nobody); len(held) == 0 {
		t.Fatal("the messages nobody owes left at once, so the log was never full of them")
	}
	last, kept, err := h.B.KeepBroadcast(filler("a", 999, 4096), "a")
	if err != nil || !kept {
		t.Fatalf("the message that needed room: kept %v (%v)", kept, err)
	}
	if got, _ := owedOf(h, "a"); !slices.Equal(got, []uint64{first.Offset, last.Offset}) {
		t.Errorf("a is owed %v, want %v: it lost a message to room nobody owed", got, []uint64{first.Offset, last.Offset})
	}
	if got := scrapeGauges(t, ops)[storageFull]; got != 0 {
		t.Errorf("%s is %v, want 0", storageFull, got)
	}
	t.Logf("%d messages nobody owed filled the log before the give-up", len(nobody))
}

// **A give-up waits for a release already removing**: a batch the release
// has taken is gone from its list and still in the log, and a give-up that
// went ahead would find it owed to nobody only after giving up a message a
// session is owed in front of it (bdrain.removeGathered). The release's
// removal is held open while a full log needs room.
func TestAGiveUpWaitsForAReleaseAlreadyRemoving(t *testing.T) {
	h, _, s := drainHarness(t)
	const room = 256 * 1024
	q := store.NewQuota(room, 0)
	s.SetQuota(q)
	free := func() int64 { return room - q.Bytes() }
	lg := &holdsRemoves{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(lg.let)
	attachDrainWith(t, h, func(l drainReads) drainReads { lg.drainReads = l; return lg },
		func(s drainSessions) drainSessions { return s })
	h.B.SetAckCommitInterval(time.Second)
	ops := operationsAt(t, h)
	away(t, h, "a", "news/a")
	first, kept, err := h.B.KeepBroadcast(filler("a", 0, 4096), "a")
	if err != nil || !kept {
		t.Fatalf("the first message: kept %v (%v)", kept, err)
	}
	lg.armed.Store(true)
	nobody := 0
	for i := 1; free() >= store.RecordSize(filler("nobody", i, 4096)); i++ {
		if _, kept, err := h.B.KeepBroadcast(filler("nobody", i, 4096)); err != nil || !kept {
			t.Fatalf("message %d: kept %v (%v)", i, kept, err)
		}
		nobody++
	}
	select {
	case <-lg.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the release never began removing, so there was nothing for the give-up to wait for")
	}
	done := make(chan struct{})
	var last store.Record
	go func() {
		defer close(done)
		var kept bool
		last, kept, err = h.B.KeepBroadcast(filler("a", 999, 4096), "a")
		if err == nil && !kept {
			err = errors.New("not kept")
		}
	}()
	// The give-up is queued behind the removal, which is the order this is
	// about; let go before then, the removal finished first and the give-up
	// had nothing to wait for.
	awaitBlockedIn(t, "(*bdrain).giveUp", "sync.Mutex.Lock")
	lg.let()
	<-done
	if err != nil {
		t.Fatalf("the message that needed room: %v", err)
	}
	if got, _ := owedOf(h, "a"); !slices.Equal(got, []uint64{first.Offset, last.Offset}) {
		t.Errorf("a is owed %v, want %v: it lost a message to room a release was giving back",
			got, []uint64{first.Offset, last.Offset})
	}
	if got := scrapeGauges(t, ops)[storageFull]; got != 0 {
		t.Errorf("%s is %v, want 0", storageFull, got)
	}
	t.Logf("%d messages nobody owed were being removed when the log needed room", nobody)
}
