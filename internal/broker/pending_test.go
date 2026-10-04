package broker

import (
	"io"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	mqtt "github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/store"
)

// The rule that lets a consumer step over records unread, against the
// slow way of knowing what it may step over: every record, every filter.
//
// A channel is filled with records on random topics, announced to pumpAll
// in a random order - out of order is the case the watermark exists for -
// while a consumer's filters change at random moments. At every step, for
// every position the consumer could be at, whatever skipDeclinedLocked
// says it may step over must be records none of its filters match, and all
// of them announced. Stepping over a matching record is a record never
// sent, reported as success (invariant 1); stepping over an unannounced one
// is the same, a moment later.
func TestSteppingOverDeclinedRecordsNeverStepsOverAMatch(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q is not a number", v)
		}
		seed = n
	}
	t.Logf("seed %d; replay with SAGUIN_RANDOM_SEED=%d", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	topics := []string{"events/a", "events/a/x", "events/b", "events/b/y", "events/c"}
	filterSets := [][]string{
		{"events/a/#"}, {"events/b"}, {"events/a/#", "events/c"}, {"events/+/y"}, {"events/#"},
	}

	var skipped, positions int
	steps := map[string]int{}
	for round := range 200 {
		b := testBroker(t)
		c := b.reg.Get("events")
		lg := b.logs["events"]
		// Records already there when the broker starts count as announced.
		for range rng.Intn(5) {
			if _, err := lg.Append(store.Record{Topic: topics[rng.Intn(len(topics))], Timestamp: time.Now()}); err != nil {
				t.Fatal(err)
			}
		}
		b.attachLog("events", lg)

		filters := filterSets[rng.Intn(len(filterSets))]
		b.subs["reader"] = nil
		for _, f := range filters {
			b.subs["reader"] = append(b.subs["reader"], subscription{filter: f, channel: c})
		}
		b.reindex("reader")
		cur := b.cursor("reader", "events", false)

		// A record is announced in three steps (see announce), and each is
		// taken on its own, in any order across records, with the filters
		// changing between them - so a list reset lands between steps of an
		// announcement that did, or did not, count this consumer as a
		// candidate, and two announcements reach its list out of order.
		type flight struct {
			r     store.Record
			a     *announcing
			phase int // 0 stored, 1 begun, 2 noted; done ones are removed
		}
		var stored []store.Record
		var inFlight []*flight
		for range 120 {
			switch rng.Intn(20) {
			case 0, 1, 2, 3, 4, 5: // a record is stored
				r, err := lg.Append(store.Record{Topic: topics[rng.Intn(len(topics))], Timestamp: time.Now()})
				if err != nil {
					t.Fatal(err)
				}
				stored = append(stored, r)
				inFlight = append(inFlight, &flight{r: r})
			case 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18: // one announcement, any of them, takes its next step
				if len(inFlight) == 0 {
					continue
				}
				i := rng.Intn(len(inFlight))
				f := inFlight[i]
				switch f.phase {
				case 0:
					_, f.a = b.announceBegin(c, f.r)
					// A record no consumer matched is announced in that one
					// step: there is no list for it to go on, so the
					// watermark moves there and nothing else is owed.
					if f.a.w == nil {
						inFlight = append(inFlight[:i], inFlight[i+1:]...)
						steps["done in one step"]++
						continue
					}
				case 1:
					f.a.note()
				case 2:
					b.announceDone(f.a)
					inFlight = append(inFlight[:i], inFlight[i+1:]...)
					steps["done"]++
				}
				f.phase++
			case 19: // the consumer's filters change
				filters = filterSets[rng.Intn(len(filterSets))]
				b.subs["reader"] = nil
				for _, f := range filters {
					b.subs["reader"] = append(b.subs["reader"], subscription{filter: f, channel: c})
				}
				b.reindex("reader")
			}

			isAnnounced := func(off uint64) bool {
				for _, f := range inFlight {
					if f.r.Offset == off {
						return false
					}
				}
				return true
			}
			for from := uint64(1); from <= lg.Next(); from++ {
				to := b.skipDeclinedLocked(cur, "events", from)
				positions++
				for off := from; off < to; off++ {
					skipped++
					if !isAnnounced(off) {
						t.Fatalf("round %d: from %d stepped to %d over offset %d, which is stored "+
							"and not announced", round, from, to, off)
					}
					recs, err := lg.ReadFromN(off, 1)
					if err != nil || len(recs) != 1 {
						t.Fatalf("round %d: reading offset %d: %v", round, off, err)
					}
					for _, f := range filters {
						if channel.Matches(f, recs[0].Topic) {
							t.Fatalf("round %d: from %d stepped to %d over offset %d (%s), which %s "+
								"matches", round, from, to, off, recs[0].Topic, f)
						}
					}
				}
			}
		}
	}
	t.Logf("%d positions asked, %d records stepped over, %d announcements completed step by step",
		positions, skipped, steps["done"])
	if steps["done"] == 0 {
		t.Fatal("no announcement ever completed, so nothing was announced in steps")
	}
	if skipped == 0 {
		t.Fatal("nothing was ever stepped over, so every check above was of an empty range")
	}
}

// An acknowledgement that arrives while its consumer's channel is being
// drained tells that drain to read again, rather than being lost.
//
// appendFreedC decides in one hold what a freed slot is owed on the append
// channels, and for one whose drain already holds the claim it must set
// repump, as
// claimPump did when this was a separate hold. Dropped, the drain finishes
// on the window it planned with, the slot the acknowledgement freed is never
// used, and a consumer with records behind it waits for a wake-up that may
// not come.
//
// **The end-to-end suites passed with it dropped** when an acknowledgement's
// drain ran on that client's own read loop, since no second acknowledgement
// from it could be read until the drain was done. That drain now runs on a
// goroutine of its own, so a second acknowledgement arriving mid-drain is
// the ordinary case and this is what serves it; it is pinned here, where the
// decision is made, either way.
func TestAnAcknowledgementDuringADrainAsksTheDrainToReadAgain(t *testing.T) {
	b := testBroker(t)
	c := b.reg.Get("events")
	b.subs["reader"] = []subscription{{filter: "events/#", channel: c}}
	b.reindex("reader")
	cl := &mqtt.Client{ID: "reader"}
	cl.Properties.Props.SessionExpiryInterval = 3600

	cur := b.cursor("reader", "events", false)
	cur.pumping = true // a drain holds the claim

	con := b.lookupConsumer("reader")
	con.cmu.Lock()
	drains := b.appendFreedC(cl, con)
	con.cmu.Unlock()

	if !cur.repump {
		t.Fatal("an acknowledgement during a drain did not ask it to read again: the slot it " +
			"freed goes unused until some other wake-up comes")
	}
	if len(drains) != 0 {
		t.Fatalf("an acknowledgement claimed a channel another drain already holds (%d claims): "+
			"two drains would write one consumer's records out of order", len(drains))
	}
}

// A subscriber's pending latest values, against the rules the drain and the
// resume rely on, over values arriving in any order: one value per topic,
// always the newest that topic has been given, and the list in offset order.
//
// **Newest, never the reverse**, because the subscriber is owed the current
// value (RFC 0003) and an older value put over a newer one would be sent as
// current. **Offset order**, because it is the order the drain writes in, and
// a resumed session is served from below the lowest value still waiting when
// its connection went (forgetConsumer) - which is only the lowest undelivered
// value if nothing below the highest written is waiting out of order. Values
// arrive out of order for real: a subscription's state is merged into a list
// live values reach first, and two publishers' values reach one subscriber
// from two goroutines.
func TestPendingLatestValuesKeepTheNewestPerTopicInOffsetOrder(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q: %v", v, err)
		}
		seed = n
	}
	t.Logf("seed %d (SAGUIN_RANDOM_SEED to repeat)", seed)
	rng := rand.New(rand.NewSource(seed))

	checked, refused := 0, 0
	for run := 0; run < 200; run++ {
		const ch = "state"
		con := &consumer{}
		newest := map[string]uint64{}
		offsets := rng.Perm(60)
		for _, o := range offsets {
			topic := "t" + strconv.Itoa(rng.Intn(8))
			off := uint64(o + 1)
			ok, sup := con.enqueueLatest(ch, pendingValue{rec: store.Record{Topic: topic, Offset: off}})
			if want := off >= newest[topic]; ok != want {
				t.Fatalf("seed %d: offset %d for %s (newest %d) was queued=%v, want %v",
					seed, off, topic, newest[topic], ok, want)
			}
			// Superseded exactly when the topic already had a different value
			// waiting: the older of the two will never be sent, whichever it is
			// (saguin_latest_superseded_total).
			if want := newest[topic] != 0 && newest[topic] != off; sup != want {
				t.Fatalf("seed %d: offset %d for %s (newest %d) reported superseded=%v, want %v",
					seed, off, topic, newest[topic], sup, want)
			}
			if !ok {
				refused++
			} else {
				newest[topic] = off
			}

			queued := con.latest[ch]
			seen := map[string]bool{}
			for i, v := range queued {
				if seen[v.rec.Topic] {
					t.Fatalf("seed %d: %s is waiting twice: %v", seed, v.rec.Topic, queued)
				}
				seen[v.rec.Topic] = true
				if v.rec.Offset != newest[v.rec.Topic] {
					t.Fatalf("seed %d: %s waits at offset %d, but %d is the newest it was given",
						seed, v.rec.Topic, v.rec.Offset, newest[v.rec.Topic])
				}
				if i > 0 && queued[i-1].rec.Offset > v.rec.Offset {
					t.Fatalf("seed %d: the list is out of offset order at %d: %v", seed, i, queued)
				}
			}
			if len(seen) != len(newest) {
				t.Fatalf("seed %d: %d topics were given a value and %d are waiting", seed,
					len(newest), len(seen))
			}
			checked++
		}
	}
	// A run that never offered an older value after a newer one tested only
	// the easy half.
	if refused == 0 {
		t.Fatalf("seed %d: no older value was ever offered after a newer one, so the rule that "+
			"matters was never exercised", seed)
	}
	t.Logf("%d states checked, %d older values refused", checked, refused)
}

// A topic's head is seeded from the store when it has none, which is the
// state a sweep leaves - so a publish that outlived its topic's sweep, holding
// a value the store has since replaced, is still seen to be overtaken rather
// than taken for the newest because nothing in memory remembers otherwise.
func TestALatestHeadForgottenBySweepIsSeededFromTheStore(t *testing.T) {
	b := latestHeadBroker(t)
	lt := store.NewLatest()
	older, err := lt.Set(store.Record{Topic: "state/x", Payload: []byte("v1")})
	if err != nil {
		t.Fatal(err)
	}
	newer, err := lt.Set(store.Record{Topic: "state/x", Payload: []byte("v2")})
	if err != nil {
		t.Fatal(err)
	}
	if older.Offset >= newer.Offset {
		t.Fatalf("offsets %d and %d do not order, so this proves nothing", older.Offset, newer.Offset)
	}
	// Nothing in memory about state/x: the older value's hand-off arrives
	// after a sweep, and the store already holds the newer.
	if _, current := b.advanceHead("state", lt, older); current {
		t.Fatalf("offset %d was taken for the newest with %d in the store: a sweep forgot the "+
			"head and nothing read the store back", older.Offset, newer.Offset)
	}
	if _, current := b.advanceHead("state", lt, newer); !current {
		t.Fatalf("the store's own current value, %d, was refused", newer.Offset)
	}
}

// A head that has not moved for latestHeadIdle is swept and one that has is
// kept, and the sweep runs at most once a minute however often it is asked.
func TestIdleLatestHeadsAreSweptAndActiveOnesKept(t *testing.T) {
	b := latestHeadBroker(t)
	lt := store.NewLatest()
	quiet, _ := lt.Set(store.Record{Topic: "state/quiet", Payload: []byte("q")})
	busy, _ := lt.Set(store.Record{Topic: "state/busy", Payload: []byte("b")})
	b.advanceHead("state", lt, quiet)
	b.advanceHead("state", lt, busy)
	m, _ := b.latestHeads.Load("state")
	heads := m.(*sync.Map)
	q, _ := heads.Load("state/quiet")
	q.(*latestHead).touched.Store(time.Now().Add(-latestHeadIdle - time.Minute).UnixNano())

	count := func() int {
		n := 0
		heads.Range(func(_, _ any) bool { n++; return true })
		return n
	}
	if count() != 2 {
		t.Fatalf("%d heads before the sweep, want 2", count())
	}
	now := time.Now()
	b.sweepLatestHeads(now)
	if _, ok := heads.Load("state/quiet"); ok {
		t.Error("a head idle past latestHeadIdle was kept")
	}
	if _, ok := heads.Load("state/busy"); !ok {
		t.Error("a head touched just now was swept")
	}
	// Asked again within the minute, it does nothing - even with the busy one
	// now idle too.
	bh, _ := heads.Load("state/busy")
	bh.(*latestHead).touched.Store(now.Add(-latestHeadIdle - time.Minute).UnixNano())
	b.sweepLatestHeads(now.Add(30 * time.Second))
	if _, ok := heads.Load("state/busy"); !ok {
		t.Error("the sweep ran twice within a minute")
	}
	b.sweepLatestHeads(now.Add(61 * time.Second))
	if _, ok := heads.Load("state/busy"); ok {
		t.Error("a minute on, the idle head was not swept")
	}
}

// latestHeadBroker is a broker with one latest channel, "state", for the head
// tests above.
func latestHeadBroker(t *testing.T) *Broker {
	t.Helper()
	reg, err := channel.NewRegistry([]*channel.Channel{{Name: "state", Type: channel.Latest, Storage: "mem"}})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}
