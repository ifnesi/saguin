package sqlite

// The two queue implementations, asked the same questions and required to
// give the same answers.
//
// It is a table over both stores rather than two test files, because the
// value is entirely in the comparison: a queue on SQLite that behaves
// almost like the memory one is the failure to look for, and almost is
// invisible when each is tested alone. RFC 0004 was written from these
// two, so a difference either has to be a decision or a defect.
//
// Two things are deliberately not compared. Epoch values: both stores mint
// a fencing token that never repeats, one per record and one per channel,
// and no caller may read the number - so the tests use whatever Offer
// returned. And ordering within ExpiredLeases, which neither promises.

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// queueStore is what the broker asks of a queue, which is what these tests
// hold both implementations to.
type queueStore interface {
	Enqueue(store.Record) (store.Record, error)
	SetBackoff(store.Backoff)
	Depth() (total, inflight int)
	ExpireOlderThan(before time.Time, dl store.DeadLetter) ([]store.Outcome, error)
	Offer(max int, now time.Time) ([]store.Offered, error)
	Lease(h store.Held, now time.Time, visibility time.Duration) (store.Item, bool, error)
	Resolve(h store.Held) (store.Item, bool, error)
	Release(h store.Held, now time.Time, answered bool, maxAttempts int, dl store.DeadLetter) (store.Outcome, bool, error)
	ExpiredLeases(now time.Time) ([]store.Held, error)
}

type queueUnderTest struct {
	q   queueStore
	dlq store.Appender
	// dlqRecords reads back what reached the dead-letter channel.
	dlqRecords func() []store.Record
	// refuse returns a dead-letter channel that cannot accept the next
	// record, in whatever way this implementation can actually fail. A stub
	// that simply returns an error would take the sqlite queue down the path
	// where it refuses a log it cannot write in one transaction with itself,
	// which is a different branch and not the one invariant 5 is about.
	refuse func(t *testing.T) store.Appender
}

// bothQueues runs fn against each implementation in turn.
func bothQueues(t *testing.T, fn func(t *testing.T, u queueUnderTest)) {
	t.Helper()

	t.Run("memory", func(t *testing.T) {
		dlq := store.NewLog()
		fn(t, queueUnderTest{
			q:   store.NewQueue(),
			dlq: dlq,
			dlqRecords: func() []store.Record {
				out, err := dlq.ReadFrom(1)
				if err != nil {
					t.Fatalf("reading the dead-letter channel: %v", err)
				}
				return out
			},
			// Nothing can make a memory log refuse, so this one is a stub.
			// The memory queue appends through the same interface either way.
			refuse: func(*testing.T) store.Appender { return refusingLog{} },
		})
	})

	t.Run("sqlite", func(t *testing.T) {
		db, err := Open(filepath.Join(t.TempDir(), "saguin.db"), "test")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		q, err := db.Queue("jobs")
		if err != nil {
			t.Fatalf("queue: %v", err)
		}
		dlq, err := db.Log("jobs__dlq")
		if err != nil {
			t.Fatalf("dead-letter channel: %v", err)
		}
		fn(t, queueUnderTest{
			q:   q,
			dlq: dlq,
			dlqRecords: func() []store.Record {
				out, err := dlq.ReadFrom(1)
				if err != nil {
					t.Fatalf("reading the dead-letter channel: %v", err)
				}
				return out
			},
			// The real log, with the offset it is about to use already
			// taken. The append then fails inside the queue's own
			// transaction, which is the failure invariant 5 is about, rather
			// than before it.
			refuse: func(t *testing.T) store.Appender {
				_, err := db.db.Exec(
					`INSERT INTO records (channel, "offset", message_id, topic, ts)
					 VALUES ('jobs__dlq', ?, 'squatter', 'jobs__dlq/x', 0)`, int64(dlq.Next()))
				if err != nil {
					t.Fatalf("seeding a conflicting record: %v", err)
				}
				return dlq
			},
		})
	})
}

// **`job_expires_after` is the only clock that ends a job**, and a
// publisher's Message Expiry Interval is not one. Both stores through one
// script, because a clock one of them read and the other did not would be a
// queue that empties on sqlite and fills in memory out of one
// configuration.
//
// The publisher's TTL reaches the worker as `saguin-expires` instead and
// the application decides. Taking work out of an operator's queue is the
// operator's call, and they make it with `job_expires_after`.
func TestExpireOlderThanReadsTheOperatorsClockOnly(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	job := func(id string, age time.Duration, expiry uint32) store.Record {
		return store.Record{MessageID: id, Topic: "jobs/" + id, Payload: []byte(id),
			Timestamp: now.Add(-age), MessageExpiry: expiry}
	}

	for name, tc := range map[string]struct {
		before time.Time // the operator's deadline, zero for none
		want   []string
	}{
		// No operator deadline: nothing goes, however hard a publisher
		// asked for it.
		"no operator deadline takes nothing": {time.Time{}, nil},
		// An hour ago takes both old jobs, whatever their publishers said.
		"the operator's deadline takes what is old": {
			now.Add(-time.Hour), []string{"spent", "ancient"}},
	} {
		t.Run(name, func(t *testing.T) {
			bothQueues(t, func(t *testing.T, u queueUnderTest) {
				for _, r := range []store.Record{
					job("spent", 2*time.Hour, 3600),      // publisher's TTL run out
					job("running", 30*time.Minute, 3600), // still inside it
					job("ancient", 2*time.Hour, 0),       // old, nobody set one
					job("fresh", time.Minute, 0),
				} {
					if _, err := u.q.Enqueue(r); err != nil {
						t.Fatalf("enqueue %s: %v", r.MessageID, err)
					}
				}

				out, err := u.q.ExpireOlderThan(tc.before, deadLetter(u))
				if err != nil {
					t.Fatalf("expire: %v", err)
				}
				got := map[string]bool{}
				for _, o := range out {
					got[o.Item.MessageID] = true
				}
				if len(got) != len(tc.want) {
					t.Fatalf("expired %v, want exactly %v", got, tc.want)
				}
				for _, id := range tc.want {
					if !got[id] {
						t.Errorf("%s was not expired, want it gone: %v", id, got)
					}
				}
			})
		})
	}
}

// deadLetter is the policy the broker supplies, reduced to what these tests
// need: the record keeps its identity and gains a reason.
func deadLetter(u queueUnderTest) store.DeadLetter {
	return store.DeadLetter{
		Log: u.dlq,
		Record: func(it store.Item) store.Record {
			return store.Record{
				MessageID: it.MessageID,
				Topic:     "jobs__dlq/" + it.Topic,
				Payload:   it.Payload,
				Headers:   []store.Header{{Key: "saguin-dlq-attempts", Value: fmt.Sprint(it.Attempts)}},
				Timestamp: it.Timestamp,
			}
		},
	}
}

func enqueue(t *testing.T, u queueUnderTest, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := u.q.Enqueue(store.Record{
			MessageID: fmt.Sprintf("id-%d", i),
			Topic:     fmt.Sprintf("jobs/task/%d", i),
			Payload:   []byte(fmt.Sprintf("job-%d", i)),
			Timestamp: time.Unix(1700000000, 0).UTC(),
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
}

// RFC 0003 "`queue` - Delivery", invariant 7
//
// Offer hands out available work in offset order, no more than it is asked
// for, and never the same record twice while it is still out.
func TestQueueOffersInOrderAndOnlyOnce(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 5)

		first, err := u.q.Offer(2, time.Now())
		if err != nil {
			t.Fatalf("offer: %v", err)
		}
		if len(first) != 2 {
			t.Fatalf("offered %d records with room for 2", len(first))
		}
		if first[0].Offset != 1 || first[1].Offset != 2 {
			t.Fatalf("offered offsets %d and %d, want 1 and 2", first[0].Offset, first[1].Offset)
		}
		if first[0].Attempt != 1 {
			t.Fatalf("first attempt is %d, want 1", first[0].Attempt)
		}
		if first[0].DeliveryID == "" || first[0].DeliveryID == first[1].DeliveryID {
			t.Fatalf("delivery ids %q and %q: each attempt needs its own (invariant 3)",
				first[0].DeliveryID, first[1].DeliveryID)
		}
		if got := len(first[0].DeliveryID); got > 32 {
			t.Fatalf("delivery id is %d characters; RFC 0003 allows at most 32", got)
		}
		if string(first[0].Payload) != "job-1" || first[0].MessageID != "id-1" {
			t.Fatalf("record 1 came back as %q/%q", first[0].MessageID, first[0].Payload)
		}

		// The two that are out are not offered again; the next three are.
		next, err := u.q.Offer(10, time.Now())
		if err != nil {
			t.Fatalf("offer: %v", err)
		}
		if len(next) != 3 {
			t.Fatalf("offered %d records, want the 3 that are not already out", len(next))
		}
		if next[0].Offset != 3 {
			t.Fatalf("resumed at offset %d, want 3", next[0].Offset)
		}
	})
}

// RFC 0003 "`queue` - Attempts, timeout, and redelivery" - invariant 7
//
// The attempt is counted at the lease and never at the offer, and the
// deadline starts there too. A record offered and never leased burns
// nothing.
func TestQueueCountsTheAttemptAtTheLease(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 1)
		now := time.Unix(1700000000, 0).UTC()

		offered, err := u.q.Offer(1, time.Now())
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer: %v (%d records)", err, len(offered))
		}
		held := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker-1"}

		// Nothing has a deadline yet: the record is being delivered, not
		// leased, and a deadline running now would overlap the worker's
		// in-flight window (invariant 7).
		expired, err := u.q.ExpiredLeases(now.Add(time.Hour))
		if err != nil {
			t.Fatalf("expired: %v", err)
		}
		if len(expired) != 0 {
			t.Fatalf("%d deliveries had a deadline before any PUBACK", len(expired))
		}

		it, ok, err := u.q.Lease(held, now, time.Minute)
		if err != nil || !ok {
			t.Fatalf("lease: ok=%v err=%v", ok, err)
		}
		if it.Attempts != 1 {
			t.Fatalf("attempts %d after the first lease, want 1", it.Attempts)
		}
		if it.FirstSeen.IsZero() || it.LastSeen.IsZero() {
			t.Fatal("a leased record has no first or last delivery time; a dead-lettered " +
				"record could not say when it was tried")
		}

		// Leasing the same delivery twice must not count twice.
		if _, ok, _ := u.q.Lease(held, now, time.Minute); ok {
			t.Fatal("the same delivery was leased twice, so one attempt counted as two")
		}

		// Before the deadline, nothing. After it, exactly this one.
		if e, _ := u.q.ExpiredLeases(now.Add(30 * time.Second)); len(e) != 0 {
			t.Fatalf("%d leases expired before their deadline", len(e))
		}
		e, err := u.q.ExpiredLeases(now.Add(2 * time.Minute))
		if err != nil {
			t.Fatalf("expired: %v", err)
		}
		if len(e) != 1 || e[0].Offset != 1 || e[0].Holder != "worker-1" {
			t.Fatalf("expired leases %+v, want offset 1 held by worker-1", e)
		}
	})
}

// Invariant 3
//
// A superseded delivery resolves nothing. Every operation naming a delivery
// checks it, not only the acknowledgement.
func TestQueueSupersededDeliveryChangesNothing(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 1)
		now := time.Unix(1700000000, 0).UTC()

		offered, _ := u.q.Offer(1, time.Now())
		stale := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker-1"}
		if _, ok, err := u.q.Lease(stale, now, time.Minute); err != nil || !ok {
			t.Fatalf("lease: ok=%v err=%v", ok, err)
		}

		// The lease is given back and the record handed to somebody else.
		if _, ok, err := u.q.Release(stale, time.Now(), false, 5, deadLetter(u)); err != nil || !ok {
			t.Fatalf("release: ok=%v err=%v", ok, err)
		}
		again, err := u.q.Offer(1, time.Now())
		if err != nil || len(again) != 1 {
			t.Fatalf("re-offer: %v (%d records)", err, len(again))
		}
		fresh := store.Held{Offset: again[0].Offset, Epoch: again[0].Epoch, Holder: "worker-2"}
		if again[0].DeliveryID == offered[0].DeliveryID {
			t.Fatal("the redelivery reused the Delivery ID; a stale ack would resolve it")
		}
		if again[0].Attempt != 2 {
			t.Fatalf("redelivery is attempt %d, want 2", again[0].Attempt)
		}

		// Now the first worker answers, late. Every one of these must do
		// nothing at all.
		if _, ok, _ := u.q.Resolve(stale); ok {
			t.Fatal("a superseded acknowledgement resolved the record, deleting work " +
				"another worker is holding (invariant 3)")
		}
		if _, ok, _ := u.q.Lease(stale, now, time.Minute); ok {
			t.Fatal("a superseded delivery took a lease")
		}
		if _, ok, _ := u.q.Release(stale, time.Now(), true, 5, deadLetter(u)); ok {
			t.Fatal("a superseded delivery returned the record another worker holds")
		}

		// And the current one still works.
		if _, ok, err := u.q.Resolve(fresh); err != nil || !ok {
			t.Fatalf("the current delivery could not resolve: ok=%v err=%v", ok, err)
		}
		if left, _ := u.q.Offer(10, time.Now()); len(left) != 0 {
			t.Fatalf("%d records left after the only one was acknowledged", len(left))
		}
	})
}

// RFC 0003 "`queue` - Attempts, timeout, and redelivery"
//
// A worker that answers before its acknowledgement is processed still burns
// an attempt, or a job it keeps handing back is redelivered for ever.
func TestQueueAnswerBeforeTheLeaseStillBurnsAnAttempt(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 1)

		offered, _ := u.q.Offer(1, time.Now())
		h := store.Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}

		out, ok, err := u.q.Release(h, time.Now(), true, 5, deadLetter(u))
		if err != nil || !ok {
			t.Fatalf("release: ok=%v err=%v", ok, err)
		}
		if out.Item.Attempts != 1 {
			t.Fatalf("attempts %d after a return that arrived before the lease, want 1 - "+
				"uncounted, this job bounces for ever", out.Item.Attempts)
		}

		again, _ := u.q.Offer(1, time.Now())
		if len(again) != 1 || again[0].Attempt != 2 {
			t.Fatalf("re-offered as attempt %d, want 2", again[0].Attempt)
		}
	})
}

// RFC 0003 "`queue` - Attempts, timeout, and redelivery"
//
// An acknowledgement that arrives before the `PUBACK` counts its attempt,
// and is not counted twice when the `PUBACK` came first.
//
// The rule is that the attempt increments when the worker is known to have
// received the record - at the `PUBACK`, or when it answers, whichever
// comes first. `Lease` is the first site and `Release` the second; this is
// the third, the path an `ack` takes straight out of Delivering, which is
// what a client library that acknowledges when its handler returns
// produces and what RFC 0003 recommends.
//
// **It belongs here rather than beside one store.** The fix landed in both
// and only the memory half had a test, so reverting the SQLite increment
// alone stayed green across every package - and SQLite is the store this
// project recommends for anything that matters. Only the broker's acked
// line reads the value, which is why nothing else would catch the drift.
func TestQueueAnswerFromDeliveringCountsItsAttemptOnce(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 2)

		// No Lease call: answered straight out of Delivering.
		offered, _ := u.q.Offer(1, time.Now())
		h := store.Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
		it, ok, err := u.q.Resolve(h)
		if err != nil || !ok {
			t.Fatalf("resolve: ok=%v err=%v", ok, err)
		}
		if it.Attempts != 1 {
			t.Fatalf("attempts %d after an acknowledgement from Delivering, want 1: "+
				"the broker reports this on its acked line, and zero there says the "+
				"record was never delivered when a worker had just done it", it.Attempts)
		}

		// And the PUBACK first, then the answer, is still one attempt.
		offered, _ = u.q.Offer(1, time.Now())
		h = store.Held{Offset: 2, Epoch: offered[0].Epoch, Holder: "worker-1"}
		if _, ok, err := u.q.Lease(h, time.Now(), time.Minute); err != nil || !ok {
			t.Fatalf("lease: ok=%v err=%v", ok, err)
		}
		it, ok, err = u.q.Resolve(h)
		if err != nil || !ok {
			t.Fatalf("resolve: ok=%v err=%v", ok, err)
		}
		if it.Attempts != 1 {
			t.Fatalf("attempts %d after a PUBACK and then an acknowledgement, want 1: "+
				"it is counted once, at whichever came first", it.Attempts)
		}
	})
}

// RFC 0003 "`queue` - Dead-lettering" - invariants 5 and 8
//
// Attempts spent moves the record out of the queue and into the dead-letter
// channel, keeping its Message ID, and it stops being work.
func TestQueueDeadLettersWhenAttemptsAreSpent(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		const maxAttempts = 3
		enqueue(t, u, 1)
		now := time.Unix(1700000000, 0).UTC()

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			offered, err := u.q.Offer(1, time.Now())
			if err != nil {
				t.Fatalf("offer: %v", err)
			}
			if len(offered) != 1 {
				t.Fatalf("attempt %d: the record was not offered again", attempt)
			}
			if offered[0].Attempt != attempt {
				t.Fatalf("offered as attempt %d, want %d", offered[0].Attempt, attempt)
			}
			h := store.Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
			if _, ok, err := u.q.Lease(h, now, time.Minute); err != nil || !ok {
				t.Fatalf("attempt %d lease: ok=%v err=%v", attempt, ok, err)
			}
			out, ok, err := u.q.Release(h, time.Now(), false, maxAttempts, deadLetter(u))
			if err != nil || !ok {
				t.Fatalf("attempt %d release: ok=%v err=%v", attempt, ok, err)
			}
			if attempt < maxAttempts {
				if out.DeadLettered {
					t.Fatalf("dead-lettered on attempt %d of %d", attempt, maxAttempts)
				}
				continue
			}
			if !out.DeadLettered {
				t.Fatalf("attempts are spent at %d and the record is still work", maxAttempts)
			}
			if out.Item.Attempts != maxAttempts {
				t.Fatalf("dead-lettered with %d attempts, want %d", out.Item.Attempts, maxAttempts)
			}
		}

		// Out of the queue...
		if left, _ := u.q.Offer(10, time.Now()); len(left) != 0 {
			t.Fatalf("%d records still in the queue after dead-lettering", len(left))
		}
		// ...and into the dead-letter channel, whole, with its identity.
		dead := u.dlqRecords()
		if len(dead) != 1 {
			t.Fatalf("%d records in the dead-letter channel, want 1 (invariant 5)", len(dead))
		}
		if dead[0].MessageID != "id-1" {
			t.Fatalf("dead-lettered as %q, want id-1 - a restart made it a new message "+
				"and a consumer deduplicating on identity processes it twice (invariant 8)",
				dead[0].MessageID)
		}
		if string(dead[0].Payload) != "job-1" {
			t.Fatalf("dead-lettered payload %q", dead[0].Payload)
		}
		if v, _ := dead[0].Header("saguin-dlq-attempts"); v != fmt.Sprint(maxAttempts) {
			t.Fatalf("dead-letter metadata says %q attempts, want %d", v, maxAttempts)
		}
		if dead[0].Offset != 1 {
			t.Fatalf("dead-lettered at offset %d, want 1", dead[0].Offset)
		}
	})
}

// RFC 0003 "`queue` - Dead-lettering" - invariant 5
//
// A move refused for want of capacity leaves the record exactly as it was,
// delivery state and attempt count included, so the attempt is not spent on
// a failure that was the broker's - and the record is in one place, not
// none and not both.
func TestQueueRefusedDeadLetterMoveChangesNothing(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 1)
		now := time.Unix(1700000000, 0).UTC()

		offered, _ := u.q.Offer(1, time.Now())
		h := store.Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
		if _, ok, err := u.q.Lease(h, now, time.Minute); err != nil || !ok {
			t.Fatalf("lease: ok=%v err=%v", ok, err)
		}

		// Its attempts are spent, so this release would dead-letter it - into
		// a log that refuses.
		dl := deadLetter(u)
		dl.Log = u.refuse(t)
		// Whatever refuse had to put in the way is in the channel already, so
		// what matters is that the move adds nothing to it.
		seeded := len(u.dlqRecords())
		if _, ok, err := u.q.Release(h, time.Now(), false, 1, dl); err == nil || ok {
			t.Fatalf("a refused dead-letter move reported ok=%v err=%v; it must fail", ok, err)
		}

		// Still work, still held by the same delivery, with the same attempt
		// count - so the next attempt is a real one.
		if left, _ := u.q.Offer(10, time.Now()); len(left) != 0 {
			t.Fatalf("the record was offered again while still held: %d", len(left))
		}
		if dead := u.dlqRecords(); len(dead) != seeded {
			t.Fatalf("the dead-letter channel went from %d records to %d on a refused "+
				"move; the record is now in both places (invariant 5)", seeded, len(dead))
		}

		// The same delivery is still current, which is what says its state
		// survived the refusal.
		out, ok, err := u.q.Release(h, time.Now(), false, 5, deadLetter(u))
		if err != nil || !ok {
			t.Fatalf("the delivery did not survive the refused move: ok=%v err=%v", ok, err)
		}
		if out.Item.Attempts != 1 {
			t.Fatalf("attempts %d after a refused move, want 1 - the attempt was spent "+
				"on a failure that was the broker's", out.Item.Attempts)
		}

		// And the record itself is still there to be done. This is the half
		// that says the move was one operation: a queue that removed it and
		// then failed to store it satisfies everything above and has lost the
		// job - which is the outcome dead-lettering exists to prevent.
		again, err := u.q.Offer(10, time.Now())
		if err != nil {
			t.Fatalf("offer after a refused move: %v", err)
		}
		if len(again) != 1 {
			t.Fatalf("%d records available after a refused dead-letter move, want 1; "+
				"the record left the queue without arriving anywhere (invariant 5)", len(again))
		}
		if again[0].MessageID != "id-1" || string(again[0].Payload) != "job-1" {
			t.Fatalf("the record came back as %q/%q, want id-1/job-1",
				again[0].MessageID, again[0].Payload)
		}
		if again[0].Attempt != 2 {
			t.Fatalf("re-offered as attempt %d, want 2", again[0].Attempt)
		}
	})
}

// refusingLog is a dead-letter channel that cannot accept anything, which
// is how a move is refused for want of capacity.
type refusingLog struct{}

func (refusingLog) Append(store.Record) (store.Record, error) {
	return store.Record{}, fmt.Errorf("no capacity")
}

// RFC 0002 `retry.backoff` - RFC 0003 "`queue` - Attempts, timeout, and
// redelivery"
//
// **The whole of the feature, as one timeline, because the two answers it
// gives are only interesting beside each other.** A returned job waits; a
// job the visibility timeout took back does not, and neither is a rule about
// what kind of failure it was. There is one rule - the gap runs from when
// the worker last had the record - and the second answer falls out of it,
// because a lease that expired has already spent a whole visibility timeout.
//
// Linear at a 2s base, so the gap after one attempt is 2s and after two is
// 4s, and a visibility timeout of 30s which is far longer than either. Every
// moment is computed from a fixed origin rather than from time.Now(), so
// nothing here depends on how fast the machine is.
func TestABackedOffJobWaitsAfterAReturnAndNotAfterATimeout(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		u.q.SetBackoff(store.Backoff{Kind: store.BackoffLinear, Base: 2 * time.Second})
		enqueue(t, u, 1)

		t0 := time.Unix(1700000000, 0).UTC()
		at := func(seconds int) time.Time { return t0.Add(time.Duration(seconds) * time.Second) }
		const visibility = 30 * time.Second

		// t=0 handed out, and the worker acknowledges receipt.
		offered, err := u.q.Offer(1, t0)
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer at t=0: %v (%d records)", err, len(offered))
		}
		h := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker"}
		if _, ok, err := u.q.Lease(h, t0, visibility); err != nil || !ok {
			t.Fatalf("lease at t=0: %v (ok=%v)", err, ok)
		}

		// t=10 the worker reports failure, having worked for ten seconds.
		// This is the shape the defect was in: the record's last-seen moment
		// has to move to t=10, because a gap measured from t=0 expired at
		// t=2 and the job would go straight back out.
		out, ok, err := u.q.Release(h, at(10), true, 5, deadLetter(u))
		if err != nil || !ok {
			t.Fatalf("return at t=10: %v (ok=%v)", err, ok)
		}
		if out.DeadLettered {
			t.Fatal("the first return dead-lettered the job")
		}

		// t=11 is inside the 2s gap.
		if got, err := u.q.Offer(1, at(11)); err != nil || len(got) != 0 {
			t.Fatalf("offered %d records at t=11, one second after a return with a 2s gap: "+
				"the wait is being measured from when the job was handed out rather than "+
				"from when it failed (err=%v)", len(got), err)
		}

		// t=12 it is out again. This is the answer to "when does the worker
		// get it back after returning it": two seconds later, not at once.
		second, err := u.q.Offer(1, at(12))
		if err != nil || len(second) != 1 {
			t.Fatalf("offered %d records at t=12, when the 2s gap is up (err=%v)", len(second), err)
		}
		if second[0].Attempt != 2 {
			t.Fatalf("the second delivery says attempt %d, want 2", second[0].Attempt)
		}

		// t=12 the worker acknowledges receipt of attempt 2, and then says
		// nothing at all.
		h2 := store.Held{Offset: second[0].Offset, Epoch: second[0].Epoch, Holder: "worker"}
		if _, ok, err := u.q.Lease(h2, at(12), visibility); err != nil || !ok {
			t.Fatalf("lease at t=12: %v (ok=%v)", err, ok)
		}

		// While the worker holds it, nothing offers it to anybody - the
		// visibility timeout governs here and the gap is not consulted.
		if got, err := u.q.Offer(1, at(20)); err != nil || len(got) != 0 {
			t.Fatalf("offered %d records at t=20, while a worker held the job under a "+
				"30s lease (err=%v)", len(got), err)
		}

		// t=42 the lease expires and the broker takes the job back.
		expired, err := u.q.ExpiredLeases(at(42))
		if err != nil || len(expired) != 1 {
			t.Fatalf("expired leases at t=42: %v (%d)", err, len(expired))
		}
		if _, ok, err := u.q.Release(expired[0], at(42), false, 5, deadLetter(u)); err != nil || !ok {
			t.Fatalf("reclaim at t=42: %v (ok=%v)", err, ok)
		}

		// And it goes out at once. The 4s gap for a second attempt started
		// at t=12 when the worker received it, so it was served at t=16 -
		// twenty-six seconds before the timeout even noticed. This is the
		// answer to "what about a worker that never replies": immediately,
		// and by arithmetic rather than by an exemption written anywhere.
		third, err := u.q.Offer(1, at(42))
		if err != nil || len(third) != 1 {
			t.Fatalf("offered %d records at t=42, immediately after a visibility timeout "+
				"took the job back: a reclaimed job has already served its gap (err=%v)",
				len(third), err)
		}
		if third[0].Attempt != 3 {
			t.Fatalf("the third delivery says attempt %d, want 3", third[0].Attempt)
		}
	})
}

// RFC 0002 `retry.backoff`
//
// **A job that is waiting is stepped over, not queued behind.** The failure
// this prevents is the one that makes a backoff worse than none: a single
// job nobody can process holds up every job behind it for the length of its
// own gap, and a queue with work in it goes quiet.
//
// **Three waiting jobs against room for two, which is the only shape that
// catches the sqlite store.** That store reads a window of rows - `max` plus
// however many are out with a worker, since those are held in memory and
// cannot be excluded by the table - so a gap applied to the rows that came
// back rather than inside the query is invisible whenever the window is
// wider than the queue. Written first with three jobs and room for ten, this
// test passed against exactly that defect: all three rows came back, two
// survived the filter, and the assertion was satisfied. It takes more
// waiting records at the front than the caller asked for to make the window
// close over them.
func TestABackingOffJobDoesNotHoldUpTheOnesBehindIt(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		u.q.SetBackoff(store.Backoff{Kind: store.BackoffExponential, Base: 10 * time.Second})
		enqueue(t, u, 5)

		t0 := time.Unix(1700000000, 0).UTC()

		// The first three fail, and each starts a ten-second gap.
		offered, err := u.q.Offer(3, t0)
		if err != nil || len(offered) != 3 {
			t.Fatalf("offer: %v (%d records)", err, len(offered))
		}
		for _, o := range offered {
			h := store.Held{Offset: o.Offset, Epoch: o.Epoch, Holder: "worker"}
			if _, ok, err := u.q.Lease(h, t0, 30*time.Second); err != nil || !ok {
				t.Fatalf("lease of offset %d: %v (ok=%v)", o.Offset, err, ok)
			}
			if _, ok, err := u.q.Release(h, t0, true, 5, deadLetter(u)); err != nil || !ok {
				t.Fatalf("return of offset %d: %v (ok=%v)", o.Offset, err, ok)
			}
		}

		// A second later, with room for two: the three at the front are
		// still waiting and the two behind them are handed out. A store that
		// asks the gap after reading its window returns nothing here, having
		// spent the whole window on records it then discarded.
		got, err := u.q.Offer(2, t0.Add(time.Second))
		if err != nil {
			t.Fatalf("offer: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("offered %d records with room for 2 while offsets 1-3 were backing off, "+
				"want the 2 behind them: work is in the queue and nothing is being handed out",
				len(got))
		}
		if got[0].Offset != 4 || got[1].Offset != 5 {
			t.Fatalf("offered offsets %d and %d, want 4 and 5", got[0].Offset, got[1].Offset)
		}
	})
}

// RFC 0002 `retry.backoff: none` is the default, and a queue that says
// nothing behaves exactly as it did before any of this existed: a returned
// job is available at once.
//
// It is a test rather than an assumption because the default is what every
// deployment gets, and a backoff that switched itself on would be a change
// to every queue in the field.
func TestWithoutBackoffAReturnedJobIsAvailableAtOnce(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		enqueue(t, u, 1)
		t0 := time.Unix(1700000000, 0).UTC()

		offered, err := u.q.Offer(1, t0)
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer: %v (%d records)", err, len(offered))
		}
		h := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker"}
		if _, ok, err := u.q.Lease(h, t0, 30*time.Second); err != nil || !ok {
			t.Fatalf("lease: %v (ok=%v)", err, ok)
		}
		if _, ok, err := u.q.Release(h, t0, true, 5, deadLetter(u)); err != nil || !ok {
			t.Fatalf("return: %v (ok=%v)", err, ok)
		}

		// The same moment, not a later one.
		again, err := u.q.Offer(1, t0)
		if err != nil || len(again) != 1 {
			t.Fatalf("offered %d records at the moment of the return with no backoff "+
				"configured (err=%v)", len(again), err)
		}
	})
}

// RFC 0002 `retry.backoff` - the gap widens, which is the whole point of
// there being two shapes rather than one interval.
//
// Asked of the store rather than of the arithmetic: store.Backoff.Wait is
// one function and both stores must agree with it, and the sqlite store
// computes the same thing again inside its query. That second copy is what
// this catches - the two clamps and the two expressions have to give one
// answer, or a job is ready in one store and not in the other.
//
// The offer that proves the gap is up is the next attempt's delivery rather
// than a peek before it. A queue has no way to look without taking, so a
// verifying offer would consume the record and the attempt after it would
// have nothing to take.
func TestTheGapWidensWithEachAttempt(t *testing.T) {
	for _, tc := range []struct {
		kind store.BackoffKind
		name string
		gaps []int // the gap in seconds after 1, 2 and 3 attempts
	}{
		{store.BackoffLinear, "linear", []int{2, 4, 6}},
		{store.BackoffExponential, "exponential", []int{2, 4, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bothQueues(t, func(t *testing.T, u queueUnderTest) {
				u.q.SetBackoff(store.Backoff{Kind: tc.kind, Base: 2 * time.Second})
				enqueue(t, u, 1)

				now := time.Unix(1700000000, 0).UTC()
				for attempt, gap := range tc.gaps {
					// The delivery. On every pass but the first, this is also
					// the proof that the previous gap had elapsed: `now` was
					// advanced to exactly the moment it was up.
					offered, err := u.q.Offer(1, now)
					if err != nil || len(offered) != 1 {
						t.Fatalf("attempt %d was not delivered at the moment its gap was up "+
							"(%d records, err=%v)", attempt+1, len(offered), err)
					}
					h := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker"}
					if _, ok, err := u.q.Lease(h, now, time.Hour); err != nil || !ok {
						t.Fatalf("attempt %d: lease: %v (ok=%v)", attempt+1, err, ok)
					}
					// max_attempts is high so that the job keeps coming back
					// rather than being dead-lettered partway through.
					if _, ok, err := u.q.Release(h, now, true, 99, deadLetter(u)); err != nil || !ok {
						t.Fatalf("attempt %d: return: %v (ok=%v)", attempt+1, err, ok)
					}

					// One second short of the gap it is still waiting.
					short := now.Add(time.Duration(gap)*time.Second - time.Second)
					if got, err := u.q.Offer(1, short); err != nil || len(got) != 0 {
						t.Fatalf("after attempt %d the job was offered %ds after it failed, "+
							"want a gap of %ds (%d records, err=%v)",
							attempt+1, gap-1, gap, len(got), err)
					}
					now = now.Add(time.Duration(gap) * time.Second)
				}

				// And the last gap, which the loop set up and nothing has
				// checked: without this the final entry of gaps is never read.
				if got, err := u.q.Offer(1, now); err != nil || len(got) != 1 {
					t.Fatalf("the job was not delivered when the last gap of %ds was up "+
						"(%d records, err=%v)", tc.gaps[len(tc.gaps)-1], len(got), err)
				}
			})
		})
	}
}

// RFC 0005 "What a metric is allowed to cost"
//
// **Both stores answer the same depth, and both answer it from a field.**
// The sqlite one used to run a count(*), which is what that document
// refuses: a scraper doing it every fifteen seconds across every channel is
// a load source wearing a monitoring tool's clothes. Replacing a query with
// a counter is the change that drifts silently - a removal path that
// forgets to decrement reads as a queue that never empties - so this drives
// a record through every state and asks both implementations at each step.
func TestBothStoresAgreeOnQueueDepth(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		now := time.Unix(1700000000, 0).UTC()
		check := func(when string, total, inflight int) {
			t.Helper()
			gotTotal, gotIn := u.q.Depth()
			if gotTotal != total || gotIn != inflight {
				t.Errorf("%s: depth is %d total and %d in flight, want %d and %d",
					when, gotTotal, gotIn, total, inflight)
			}
		}

		check("on an empty queue", 0, 0)
		enqueue(t, u, 3)
		check("after three were published", 3, 0)

		offered, err := u.q.Offer(2, now)
		if err != nil || len(offered) != 2 {
			t.Fatalf("offer: %v (%d)", err, len(offered))
		}
		// Delivering counts as out with a worker: the record is on its way
		// and is available to nobody else.
		check("with two handed out", 3, 2)

		h := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker"}
		if _, ok, err := u.q.Lease(h, now, time.Hour); err != nil || !ok {
			t.Fatalf("lease: %v (ok=%v)", err, ok)
		}
		check("with one leased and one delivering", 3, 2)

		if _, ok, err := u.q.Resolve(h); err != nil || !ok {
			t.Fatalf("resolve: %v (ok=%v)", err, ok)
		}
		check("after one was acknowledged", 2, 1)

		// A return puts the record back in the pool: still held, no longer
		// out with anybody.
		h2 := store.Held{Offset: offered[1].Offset, Epoch: offered[1].Epoch, Holder: "worker"}
		if _, ok, err := u.q.Release(h2, now, true, 5, deadLetter(u)); err != nil || !ok {
			t.Fatalf("return: %v (ok=%v)", err, ok)
		}
		check("after one was returned", 2, 0)

		// And out of the queue entirely when its attempts are spent, which
		// is the removal path a counter is most likely to miss.
		offered, err = u.q.Offer(1, now)
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer: %v (%d)", err, len(offered))
		}
		h3 := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker"}
		out, ok, err := u.q.Release(h3, now, true, 1, deadLetter(u))
		if err != nil || !ok {
			t.Fatalf("dead-letter: %v (ok=%v)", err, ok)
		}
		if !out.DeadLettered {
			t.Fatal("the record was not dead-lettered at its attempt limit")
		}
		check("after one was dead-lettered", 1, 0)

		// And the other removal path: work that aged out unresolved, which
		// is the only thing that reaches a queue whose workers have all gone
		// away. It was missed the first time this test was written - the
		// counter could be left un-decremented there and every step above
		// still passed, so a queue that expired its whole backlog would have
		// gone on reporting it as depth for ever.
		outcomes, err := u.q.ExpireOlderThan(now.Add(time.Hour), deadLetter(u))
		if err != nil {
			t.Fatalf("expire: %v", err)
		}
		if len(outcomes) != 1 {
			t.Fatalf("expired %d records, want the 1 that was left", len(outcomes))
		}
		check("after the last one expired", 0, 0)
	})
}

// RFC 0004 "What a store is asked for": two implementations of one contract
// agree. A record whose next wait is past the longest time.Duration - an
// exponential base of nine seconds at its 31st attempt, an hour at the same,
// a linear base of three hours at its 2^20th - is held back by both stores,
// never offered at once.
//
// **The memory store wrapped there**: Wait(31) at a nine-second base came
// out as -2,439,741 hours and the record was offered a second after its
// attempt, where the sqlite store - whose SQLite turns the overflowing
// product into a real - went on holding it back. The attempts are restored
// rather than spent, since spending them would take the clock past what a
// Unix nanosecond can hold.
func TestBothStoresHoldBackARecordWhoseWaitIsPastTheLongestDuration(t *testing.T) {
	t0 := time.Unix(1700000000, 0).UTC()
	for _, tc := range []struct {
		name     string
		backoff  store.Backoff
		attempts int
	}{
		{"exponential 9s, attempt 31", store.Backoff{Kind: store.BackoffExponential, Base: 9 * time.Second}, 31},
		{"exponential 1h, attempt 31", store.Backoff{Kind: store.BackoffExponential, Base: time.Hour}, 31},
		{"linear 3h, attempt 2^20", store.Backoff{Kind: store.BackoffLinear, Base: 3 * time.Hour}, store.MaxBackoffFactor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := tc.backoff.Wait(tc.attempts); w != math.MaxInt64 {
				t.Errorf("Wait(%d) is %v, want the longest Duration: the product is past it", tc.attempts, w)
			}
			// fresh is each store holding the record with its attempts spent,
			// its last one at t0.
			fresh := func(t *testing.T) map[string]queueStore {
				mem := store.RestoreQueue(2, []store.Item{{Record: store.Record{Offset: 1, MessageID: "m",
					Topic: "jobs/x", Payload: []byte("job"), Timestamp: t0}, Attempts: tc.attempts, LastSeen: t0}})
				db, err := Open(filepath.Join(t.TempDir(), "saguin.db"), "test")
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				t.Cleanup(func() { _ = db.Close() })
				lite, err := db.Queue("jobs")
				if err != nil {
					t.Fatalf("queue: %v", err)
				}
				if _, err := lite.Enqueue(store.Record{MessageID: "m", Topic: "jobs/x", Payload: []byte("job"), Timestamp: t0}); err != nil {
					t.Fatalf("enqueue: %v", err)
				}
				if res, err := db.db.Exec(`UPDATE queue_items SET attempts = ?, last_seen = ? WHERE channel = 'jobs'`,
					tc.attempts, t0.UnixNano()); err != nil {
					t.Fatalf("restore the attempts: %v", err)
				} else if n, _ := res.RowsAffected(); n != 1 {
					t.Fatalf("restored the attempts on %d rows, want 1", n)
				}
				return map[string]queueStore{"memory": mem, "sqlite": lite}
			}

			for name, q := range fresh(t) {
				q.SetBackoff(tc.backoff)
				got, err := q.Offer(1, t0.Add(time.Second))
				if err != nil {
					t.Fatalf("%s: offer: %v", name, err)
				}
				if len(got) != 0 {
					t.Errorf("%s offered the record a second after attempt %d, want it held back", name, tc.attempts)
				}
			}
			// And with no backoff it is offered, so what held it back was
			// the wait and nothing else.
			for name, q := range fresh(t) {
				if got, err := q.Offer(1, t0.Add(time.Second)); err != nil || len(got) != 1 {
					t.Errorf("%s offered %d records with no backoff (%v), want the 1, so this proves nothing", name, len(got), err)
				}
			}
		})
	}
}
