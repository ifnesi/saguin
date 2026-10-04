package store

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// foreignLog is a dead-letter channel this store cannot write to.
//
// It used to stand in for a destination that was full, and that case no
// longer exists here: a memory queue's move is never refused for want of
// capacity, because a bound never refuses the operation that would relieve
// it (RFC 0003). What is left is a destination of the wrong kind, which is
// a configuration that cannot arise - a queue and its dead-letter channel
// share a provider - and is refused rather than trusted.
//
// The rule under test is the same either way and is the one that matters:
// a move that does not happen leaves the record exactly as it was.
type foreignLog struct{}

func (foreignLog) Append(Record) (Record, error) {
	return Record{}, errors.New("not this store's log")
}

// A dead-letter move that fails leaves the record exactly as it was -
// delivery state, epoch and attempt count included - so that an attempt is
// not spent on a failure that was the broker's (invariant 5, RFC 0003).
//
// Getting this wrong loses the record in the obvious direction, and in the
// subtle one it burns an attempt per failure until a record nobody ever
// refused is dead-lettered for having been retried by the disk.
func TestARefusedDeadLetterMoveChangesNothing(t *testing.T) {
	q := NewQueue()
	if _, err := q.Enqueue(Record{Topic: "jobs/a"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Through a real delivery first, so the record has state worth losing.
	if _, err := q.Offer(1, time.Now()); err != nil {
		t.Fatalf("offer: %v", err)
	}
	held := Held{Offset: 1, Epoch: 0, Holder: "worker-1"}
	if _, ok, err := q.Lease(held, time.Now(), time.Hour); err != nil || !ok {
		t.Fatalf("lease: ok=%v err=%v", ok, err)
	}

	_, before := q.Export()

	// maxAttempts of 1 against an attempt already counted, so this release
	// would dead-letter if the destination would take it.
	out, ok, err := q.Release(held, time.Now(), false, 1, DeadLetter{
		Log:    foreignLog{},
		Record: func(Item) Record { return Record{Topic: "jobs__dlq/a"} },
	})
	if err == nil {
		t.Fatalf("a move into a channel this queue cannot write reported success: %+v", out)
	}
	if ok {
		t.Error("a move that failed reported that it had been applied")
	}

	_, after := q.Export()
	if len(after) != 1 {
		t.Fatalf("the queue holds %d records after a failed move, want 1", len(after))
	}
	a, b := before[0], after[0]
	if a.State != b.State || a.Epoch != b.Epoch || a.Attempts != b.Attempts ||
		a.Holder != b.Holder || a.DeliveryID != b.DeliveryID || !a.LeaseUntil.Equal(b.LeaseUntil) {
		t.Errorf("the record changed under a failed move:\n before %+v\n after  %+v", a, b)
	}

	// The delivery naming it is still the current one, so the release can
	// simply be tried again. An epoch that had advanced would have made the
	// record unreachable by the only caller that knows about it.
	if _, ok, err := q.Release(held, time.Now(), false, 1, DeadLetter{
		Log:    NewLog(),
		Record: func(Item) Record { return Record{Topic: "jobs__dlq/a"} },
	}); err != nil || !ok {
		t.Fatalf("retrying the move after a failure: ok=%v err=%v", ok, err)
	}
	if total, _ := q.Depth(); total != 0 {
		t.Errorf("the queue holds %d records after the retry succeeded, want 0", total)
	}
}

// The other half of the same rule, and the half only this store can be
// asked for: a *provider* at its bound does not refuse the dead-letter move
// either. A memory provider's bound is saguin's own counter, so the rule
// reaches it. A sqlite provider's is SQLite's, and RFC 0004 says where the
// rule stops.
//
// The provider is packed to within a few bytes rather than merely filled,
// because the first version of this left a few hundred bytes of slack and
// the move fitted into them - a test that passed against the very defect it
// was written for.
func TestAFullProviderDoesNotRefuseTheDeadLetterMove(t *testing.T) {
	const max = 64 << 10
	quota := NewQuota(max, 0)

	q := NewQueue()
	q.SetQuota(quota)
	dlq := NewLog()
	dlq.SetQuota(quota)
	other := NewLog()
	other.SetQuota(quota)

	if _, err := q.Enqueue(Record{MessageID: "j", Topic: "jobs/a",
		Payload: []byte("0123456789")}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// What the move will write: the same record under the dead-letter
	// channel's name, so it costs slightly more than the one it removes.
	dead := Record{MessageID: "j", Topic: "jobs__dlq/jobs/a", Payload: []byte("0123456789")}

	// Another channel on the same provider takes everything else, one byte
	// of payload at a time at the end so almost nothing is left over.
	for {
		if _, err := other.Append(Record{MessageID: "f", Topic: "events/x",
			Payload: []byte{0}}); err != nil {
			if !errors.Is(err, ErrFull) {
				t.Fatalf("filling the provider: %v", err)
			}
			break
		}
	}
	// There is genuinely no room for what the move is about to write, shown
	// rather than assumed: an ordinary publish of the same shape is refused
	// here and now. Reasoning about the slack instead is what let the first
	// version of this pass - it left a few hundred bytes and the move fitted.
	if _, err := dlq.Append(dead); !errors.Is(err, ErrFull) {
		t.Fatalf("the provider had room for the record the move writes, "+
			"so this proves nothing: %v", err)
	}

	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer gave %d records, %v", len(offered), err)
	}
	held := Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
	if _, ok, err := q.Lease(held, time.Now(), time.Minute); err != nil || !ok {
		t.Fatalf("lease: ok=%v err=%v", ok, err)
	}

	out, ok, err := q.Release(held, time.Now(), false, 1, DeadLetter{
		Log:    dlq,
		Record: func(Item) Record { return dead },
	})
	if err != nil {
		t.Fatalf("a full provider refused the dead-letter move: %v", err)
	}
	if !ok || !out.DeadLettered {
		t.Fatalf("the record was not dead-lettered: ok=%v out=%+v", ok, out)
	}
	if got, err := dlq.ReadFrom(1); err != nil || len(got) != 1 {
		t.Fatalf("the dead-letter channel holds %d records, %v", len(got), err)
	}
}

// What the reserve buys here, which is not what it buys on sqlite.
//
// The move already cannot be refused - it charges the provider rather than
// asking it. Without a reserve that means the provider stands above the
// operator's number for as long as the dead-lettered record is there. The
// reserve is room held back from publishes, so the overshoot lands inside
// the number the operator wrote instead of outside it, and both providers
// then mean the same thing by max_bytes.
func TestTheReserveKeepsADeadLetterMoveInsideTheBound(t *testing.T) {
	const max, reserve = 64 << 10, 4 << 10
	quota := NewQuota(max, reserve)

	q := NewQueue()
	q.SetQuota(quota)
	dlq := NewLog()
	dlq.SetQuota(quota)
	other := NewLog()
	other.SetQuota(quota)

	job := Record{MessageID: "j", Topic: "jobs/a", Payload: make([]byte, 1000)}
	if _, err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	for {
		if _, err := other.Append(Record{MessageID: "f", Topic: "events/x",
			Payload: []byte{0}}); err != nil {
			if !errors.Is(err, ErrFull) {
				t.Fatalf("filling the provider: %v", err)
			}
			break
		}
	}

	// Publishes have stopped short of max_bytes by the reserve, which is the
	// room the move is about to need.
	if held := quota.Bytes(); held > max-reserve {
		t.Fatalf("publishes reached %d, past the %d they stop at", held, max-reserve)
	}

	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer gave %d records, %v", len(offered), err)
	}
	held := Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
	if _, ok, err := q.Lease(held, time.Now(), time.Minute); err != nil || !ok {
		t.Fatalf("lease: ok=%v err=%v", ok, err)
	}

	dead := Record{MessageID: "j", Topic: "jobs__dlq/jobs/a", Payload: make([]byte, 1000)}
	if _, ok, err := q.Release(held, time.Now(), false, 1, DeadLetter{
		Log:    dlq,
		Record: func(Item) Record { return dead },
	}); err != nil || !ok {
		t.Fatalf("the dead-letter move: ok=%v err=%v", ok, err)
	}

	if got := quota.Bytes(); got > max {
		t.Errorf("the provider holds %d, past the max_bytes of %d the operator wrote", got, max)
	}
}

// RFC 0003 "`queue` - States" - invariant 7
//
// Offer hands out no more than it is asked for. The caller's bound is how
// much room the live workers have, and a record offered past it is marked as
// being delivered to a worker that cannot take it - held by nobody, with no
// visibility deadline running, and nothing left to notice.
//
// The bound is part of the contract rather than an optimisation, so a second
// store answering this question differently would be wrong in the same way.
func TestOfferHandsOutNoMoreThanTheRoomThereIs(t *testing.T) {
	q := NewQueue()
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue(Record{Topic: "jobs/a"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	out, err := q.Offer(2, time.Now())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("offered %d records with room for 2; the rest are being delivered "+
			"to a worker with no window to receive them", len(out))
	}
	// In offset order, and the first two.
	if out[0].Offset != 1 || out[1].Offset != 2 {
		t.Fatalf("offered offsets %d and %d, want 1 and 2", out[0].Offset, out[1].Offset)
	}

	// The rest are untouched and still available.
	rest, err := q.Offer(10, time.Now())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if len(rest) != 3 {
		t.Fatalf("%d records left available, want 3", len(rest))
	}
}

// RFC 0002: a provider's max_bytes is a bound across every channel it
// holds, because what it protects is one pool of memory rather than one
// channel's growth.
//
// The coupling is the bound working and not a defect in it: one channel
// filling the provider does stop the others, and an operator who needs
// isolation configures two providers. Per-channel bounds cannot protect
// the box - any number of channels, each inside its own, still add up to
// more than the machine has.
func TestAProviderBoundSpansItsChannels(t *testing.T) {
	rec := Record{MessageID: "m", Topic: "events/x", Payload: []byte("0123456789")}
	each := RecordSize(rec)

	quota := NewQuota(each*2, 0)
	busy, quiet := NewLog(), NewLog()
	busy.SetQuota(quota)
	quiet.SetQuota(quota)

	// The busy channel takes the room. Neither has a bound of its own.
	for i := 1; i <= 2; i++ {
		if _, err := busy.Append(rec); err != nil {
			t.Fatalf("record %d was refused: %v", i, err)
		}
	}
	if quota.Bytes() != each*2 {
		t.Errorf("the provider holds %d, want %d", quota.Bytes(), each*2)
	}

	// And the quiet one is refused, having published nothing.
	if _, err := quiet.Append(rec); !errors.Is(err, ErrFull) {
		t.Fatalf("a channel that had published nothing was accepted into a full provider: %v", err)
	}
}

// A queue and a latest channel on the same provider, so that every kind of
// removal is asked to give the room back. A provider that only ever counts
// upwards fills once and stays full.
func TestEveryRemovalGivesRoomBackToTheProvider(t *testing.T) {
	rec := Record{MessageID: "m", Topic: "jobs/x", Payload: []byte("0123456789")}
	each := RecordSize(rec)

	quota := NewQuota(each*3, 0)
	q, lt := NewQueue(), NewLatest()
	q.SetQuota(quota)
	lt.SetQuota(quota)

	if _, err := q.Enqueue(rec); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	value := Record{MessageID: "m", Topic: "state/a", Payload: []byte("0123456789")}
	if _, err := lt.Set(value); err != nil {
		t.Fatalf("set: %v", err)
	}
	before := quota.Bytes()

	// Replacing a latest value must not count twice: the value going out
	// is given back before the new one is taken.
	for range 5 {
		if _, err := lt.Set(value); err != nil {
			t.Fatalf("replacing a value was refused: %v", err)
		}
	}
	if quota.Bytes() != before {
		t.Errorf("five replacements moved the provider from %d to %d", before, quota.Bytes())
	}

	// Resolution frees a queue record.
	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer gave %d, %v", len(offered), err)
	}
	if _, ok, err := q.Resolve(Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch}); err != nil || !ok {
		t.Fatalf("resolve reported %v %v", ok, err)
	}
	// And deleting a latest value frees it.
	if had, err := lt.Delete("state/a"); err != nil || !had {
		t.Fatalf("delete reported %v %v", had, err)
	}
	if quota.Bytes() != 0 {
		t.Errorf("the provider still holds %d after everything was removed", quota.Bytes())
	}
}

// Retention is the newest removal path and the same rule reaches it: a
// channel's own counter coming back is not enough, because the provider's
// is a second counter that only this store keeps and nothing recomputes.
//
// It is a separate test from the trim's own because the trim is driven
// through the conformance script against both stores, and only this one has
// a provider counter to forget.
func TestTrimmingGivesRoomBackToTheProviderToo(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	rec := Record{MessageID: "m", Topic: "events/x", Payload: []byte("0123456789"), Timestamp: now}
	each := RecordSize(rec)

	quota := NewQuota(each*4, 0)
	lg, lt := NewLog(), NewLatest()
	lg.SetQuota(quota)
	lt.SetQuota(quota)

	for range 3 {
		if _, err := lg.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	value := Record{MessageID: "m", Topic: "state/a", Payload: []byte("0123456789"), Timestamp: now}
	if _, err := lt.Set(value); err != nil {
		t.Fatalf("set: %v", err)
	}
	if quota.Bytes() != each*3+RecordSize(value) {
		t.Fatalf("the provider holds %d before any removal", quota.Bytes())
	}

	// Age removes the whole log, and the whole latest channel with it.
	if removed, _, err := lg.Trim(now.Add(time.Hour), 0); err != nil || removed != 3 {
		t.Fatalf("trimming the log removed %d, %v", removed, err)
	}
	if removed, _, err := lt.Trim(now.Add(time.Hour), time.Time{}); err != nil || removed != 1 {
		t.Fatalf("trimming the latest channel removed %d, %v", removed, err)
	}

	if quota.Bytes() != 0 {
		t.Errorf("the provider still holds %d after retention removed everything on it - "+
			"a provider that only counts upwards fills once and stays full", quota.Bytes())
	}
	// The room is real, not just a number: the provider takes work again.
	if _, err := lg.Append(rec); err != nil {
		t.Errorf("the provider refused a record against room retention freed: %v", err)
	}
}

// What a sweep costs on a long channel, which is what decides how often the
// broker can afford to run one.
//
// The first version of Trim compacted on every call, which costs what
// SURVIVES rather than what went: 15ms on a channel of 800,000 records,
// under the channel's own lock, for removing one record. A sweep every
// second would have been a stall a publisher feels. This should be flat in
// the channel's length instead.
func BenchmarkTrimOnALongChannel(b *testing.B) {
	now := time.Unix(1770000000, 0).UTC()
	for _, held := range []int{10000, 100000, 800000} {
		b.Run(fmt.Sprintf("%d-records", held), func(b *testing.B) {
			rec := Record{MessageID: "m", Topic: "events/x",
				Payload: make([]byte, 128), Timestamp: now}
			size := RecordSize(rec)
			fill := func() *Log {
				lg := NewLog()
				for range held {
					if _, err := lg.Append(rec); err != nil {
						b.Fatal(err)
					}
				}
				return lg
			}

			// The channel is refilled rather than drained. What is being
			// measured is one record removed from a channel of `held`, and
			// b.N comes from how long that takes - which since this became
			// flat is a few hundred nanoseconds, so b.N is always far larger
			// than the channel is long. The refill is the whole cost of the
			// setup and is paid once per `held` calls, outside the timer.
			//
			// It happens with one record still in hand rather than none,
			// because a target of zero bytes is how Trim is told there is no
			// size limit at all: it would then remove nothing, and the count
			// below would fail on a channel that is simply empty.
			lg, left := fill(), held
			b.ResetTimer()
			for range b.N {
				if left < 2 {
					b.StopTimer()
					lg, left = fill(), held
					b.StartTimer()
				}
				// One record removed per call, which is the worst shape for a
				// compaction proportional to what stays.
				removed, _, err := lg.Trim(time.Time{}, int64(left-1)*size)
				if err != nil || removed != 1 {
					b.Fatalf("removed %d, %v", removed, err)
				}
				left--
			}
		})
	}
}

// The start index is a dead prefix inside the slice, so everything that
// reads the records has to skip it. Two of those are easy to miss and both
// are silent: a read served from the prefix would hand back records the
// channel has said are gone, and a snapshot written from it would bring
// them back at the next start - below the floor stored beside them, which
// is a channel whose own numbers disagree with its contents.
//
// The trim here is large enough to trigger a compaction as well, so the
// binary search runs against a slice that has moved underneath it.
func TestReadsAndSnapshotsSkipWhatRetentionRemoved(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	lg := NewLog()
	for i := range 20 {
		if _, err := lg.Append(Record{MessageID: "m", Topic: "events/x",
			Payload: []byte("0123456789"), Timestamp: now.Add(time.Duration(i) * time.Minute)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Two trims on purpose. The first removes a quarter, which is under the
	// half that makes compaction run - so the dead prefix is still sitting
	// inside the slice while everything below reads it. A single large trim
	// compacts, which sets the index back to zero and hides exactly the
	// defect this is written for.
	if removed, _, err := lg.Trim(now.Add(5*time.Minute), 0); err != nil || removed != 5 {
		t.Fatalf("the first trim removed %d, %v", removed, err)
	}
	if lg.start == 0 {
		t.Fatal("the first trim compacted, so this cannot show a read past the prefix")
	}
	if got, err := lg.ReadFrom(1); !errors.Is(err, ErrBelowFloor) {
		t.Fatalf("a read from the dead prefix gave %d records, %v", len(got), err)
	}
	if lg.Len() != 15 {
		t.Errorf("the channel reports %d records with a dead prefix, want 15", lg.Len())
	}
	if _, _, records := lg.Export(); len(records) != 15 {
		t.Fatalf("a snapshot taken with a dead prefix holds %d records, want 15", len(records))
	}

	// And now past the threshold, so the rest runs against a slice that has
	// been compacted underneath the binary search.
	removed, _, err := lg.Trim(now.Add(15*time.Minute), 0)
	if err != nil || removed != 10 {
		t.Fatalf("trim removed %d, %v", removed, err)
	}
	if lg.Len() != 5 {
		t.Errorf("the channel reports %d records, want 5", lg.Len())
	}

	// Reading from the floor gives the survivors and nothing before them.
	got, err := lg.ReadFrom(16)
	if err != nil || len(got) != 5 {
		t.Fatalf("read %d records from the floor, %v", len(got), err)
	}
	for i, r := range got {
		if want := uint64(16 + i); r.Offset != want {
			t.Fatalf("record %d has offset %d, want %d", i, r.Offset, want)
		}
	}
	// And a read from inside the removed prefix is refused rather than
	// served out of it.
	if _, err := lg.ReadFrom(3); !errors.Is(err, ErrBelowFloor) {
		t.Errorf("a read from a removed offset returned %v, want ErrBelowFloor", err)
	}

	// The snapshot carries the survivors only, and comes back the same.
	next, floor, records := lg.Export()
	if len(records) != 5 || next != 21 || floor != 16 {
		t.Fatalf("exported %d records with next %d floor %d, want 5, 21, 16",
			len(records), next, floor)
	}
	back := RestoreLog(next, floor, records, nil)
	if back.Len() != 5 || back.Floor() != 16 || back.Next() != 21 {
		t.Errorf("restored %d records with floor %d next %d", back.Len(), back.Floor(), back.Next())
	}
	if back.Bytes() != lg.Bytes() {
		t.Errorf("the restored channel holds %d bytes, want %d", back.Bytes(), lg.Bytes())
	}
	if _, err := back.ReadFrom(3); !errors.Is(err, ErrBelowFloor) {
		t.Errorf("after a restore, a read from a removed offset returned %v", err)
	}
}

// What a point read on a `latest` channel costs, by the two routes open to
// it: a lookup, and Match with an exact predicate.
//
// Match is what exists - it is how a new subscription is served "the
// current value of every topic the filter reaches" - and reusing it for a
// single key is the cheaper change to make. This measures what that costs
// before the choice is made rather than after, because a scan and a lookup
// read the same on the page and diverge with the size of the channel.
func BenchmarkLatestPointRead(b *testing.B) {
	for _, topics := range []int{100, 1000, 10000} {
		lt := NewLatest()
		payload := make([]byte, 256)
		for i := range topics {
			if _, err := lt.Set(Record{
				MessageID: "m",
				Topic:     fmt.Sprintf("state/device/%d/level", i),
				Payload:   payload,
			}); err != nil {
				b.Fatalf("set: %v", err)
			}
		}
		// The last topic stored, so neither route is helped by reading the
		// one it happens to reach first.
		key := fmt.Sprintf("state/device/%d/level", topics-1)

		b.Run(fmt.Sprintf("get/%d", topics), func(b *testing.B) {
			for b.Loop() {
				if _, ok, err := lt.Get(key); err != nil || !ok {
					b.Fatalf("get: ok=%v err=%v", ok, err)
				}
			}
		})
		b.Run(fmt.Sprintf("match/%d", topics), func(b *testing.B) {
			for b.Loop() {
				got, err := lt.Match(func(t string) bool { return t == key })
				if err != nil || len(got) != 1 {
					b.Fatalf("match: %d records, err=%v", len(got), err)
				}
			}
		})
	}
}

// The memory store's half of TestLatestReadsOneTopicOnItsOwn in the sqlite
// package. Both providers answer a point read the same way or the channel
// type means two different things depending on where it is stored.
func TestLatestReadsOneTopicOnItsOwn(t *testing.T) {
	lt := NewLatest()
	for _, r := range []Record{
		{Topic: "state/a", Payload: []byte("on")},
		{Topic: "state/b", Payload: []byte("off")},
	} {
		if _, err := lt.Set(r); err != nil {
			t.Fatalf("set %s: %v", r.Topic, err)
		}
	}

	got, ok, err := lt.Get("state/a")
	if err != nil || !ok {
		t.Fatalf("reading a topic that has a value: ok=%v err=%v", ok, err)
	}
	if string(got.Payload) != "on" || got.Topic != "state/a" {
		t.Errorf("read %q from %q, want on from state/a", got.Payload, got.Topic)
	}

	if _, err := lt.Set(Record{Topic: "state/a", Payload: []byte("on again")}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if got, _, _ := lt.Get("state/a"); string(got.Payload) != "on again" {
		t.Errorf("after a replacement the read gave %q, want on again", got.Payload)
	}

	if _, ok, err := lt.Get("state/never"); err != nil || ok {
		t.Errorf("a topic with no value answered ok=%v err=%v, want false and no error", ok, err)
	}
	if _, err := lt.Delete("state/b"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, err := lt.Get("state/b"); err != nil || ok {
		t.Errorf("a deleted topic answered ok=%v err=%v, want false and no error", ok, err)
	}
}

// **The queue view leases nothing and carries no payloads**, which are the
// two things that make it safe to point at a live queue.
//
// The only other way to see a queue's contents is to consume them, and a
// viewer that consumed would take work from the workers it was sent to
// diagnose. And reading records is MQTT's job: a payload leaving over an
// HTTP listener is somebody's data crossing a door it was never meant to.
func TestUnresolvedLeasesNothingAndCarriesNoPayload(t *testing.T) {
	q := NewQueue()
	for i := 1; i <= 3; i++ {
		if _, err := q.Enqueue(Record{
			Topic:   fmt.Sprintf("jobs/work/%d", i),
			Payload: []byte("the customer's data"),
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	rows, total, err := q.Unresolved(10)
	if err != nil {
		t.Fatalf("unresolved: %v", err)
	}
	if total != 3 || len(rows) != 3 {
		t.Fatalf("read %d of %d, want 3 of 3", len(rows), total)
	}
	for _, it := range rows {
		if it.Payload != nil {
			t.Errorf("offset %d came back carrying %q: reading records is MQTT's job, "+
				"and a payload leaving here crosses a door it was never meant to",
				it.Offset, it.Payload)
		}
		if it.State != Available {
			t.Errorf("offset %d reads as %v after a look: nothing was consumed, so "+
				"nothing may be held", it.Offset, it.State)
		}
	}

	// **Nothing was taken.** A view that leased would leave the queue with
	// work out to a holder nobody sent it to, which is the failure that
	// makes consuming the wrong way to look.
	if depth, inflight := q.Depth(); depth != 3 || inflight != 0 {
		t.Errorf("after a look the queue holds %d with %d in flight, want 3 and 0",
			depth, inflight)
	}

	// And the cap keeps the oldest, which is where a stuck job is.
	capped, total, err := q.Unresolved(2)
	if err != nil {
		t.Fatalf("unresolved: %v", err)
	}
	if len(capped) != 2 || total != 3 {
		t.Fatalf("a cap of 2 read %d of %d, want 2 of 3 - the total is what stops a "+
			"caller believing it has seen the whole queue", len(capped), total)
	}
	if capped[0].Offset != 1 {
		t.Errorf("the cap kept offset %d first, want 1: worst-first ordering is what "+
			"makes a cap safe, because the rows it drops are the ones nobody wanted",
			capped[0].Offset)
	}
}

// **A client cannot write a bridge's position, however it names itself.**
//
// MQTT puts almost no rule on a client id, so a client is free to connect
// as `bridge:head-office`. If positions were keyed by that string, such a
// client holding a durable subscription would write the row the bridge's
// outbound rule reads: the link's position advanced or rewound underneath
// it, and records skipped on the next drain with nothing anywhere saying
// so. The scheme is what makes that unreachable, and this is the test that
// says the two spaces do not meet.
func TestABridgesPositionIsNotAKeyAClientCanSpell(t *testing.T) {
	// The adversarial case first: a client that names itself exactly what a
	// bridge is called.
	const bridge = "head-office"
	if got, want := MQTTReader("bridge:"+bridge), "mqtt:bridge:head-office"; got != want {
		t.Errorf("a client calling itself %q is stored as %q, want %q",
			"bridge:"+bridge, got, want)
	}
	if MQTTReader("bridge:"+bridge) == BridgeReader(bridge) {
		t.Fatal("a client can spell a bridge's own position key, so it can advance or " +
			"rewind the link's position and skip records with nothing saying so")
	}

	// And the general rule, over the client ids most likely to reach for it.
	for _, id := range []string{
		"head-office", "bridge:head-office", "mqtt:head-office",
		"bridge:", "", ":", "bridge:mqtt:head-office",
	} {
		if MQTTReader(id) == BridgeReader(bridge) {
			t.Errorf("client id %q produces a bridge's reader name", id)
		}
	}

	// The other half, or the rule above would be satisfied by two schemes
	// that never produce anything at all.
	if BridgeReader(bridge) != "bridge:head-office" {
		t.Errorf("a bridge's reader name is %q, want bridge:head-office", BridgeReader(bridge))
	}
	// And a bridge's position outlives a session's, because it has none:
	// zero expiry on a row that expires would delete it the moment it was
	// written.
	if PositionExpires(BridgeReader(bridge)) {
		t.Error("a bridge's position is subject to session expiry, so it would be removed " +
			"the moment it was stored and the link would restart from the floor")
	}
	if !PositionExpires(MQTTReader("device-7")) {
		t.Error("an MQTT session's position does not expire, so every client id ever seen " +
			"keeps a row for ever")
	}
}

// RFC 0004 "Removing a record": the broadcast log lets a message go from the
// middle without shuffling. The slot is cleared where it stands and every read
// skips it until the holes and the dead prefix reach half the slice, when one
// compaction drops them all - so a release costs the same wherever the message
// stands, which is the case a slow session holding the front makes common.
//
// What this holds the memory store to, beyond what the both-providers script
// checks: the reads are right while the holes are still in the slice, the
// compaction really happens, and the provider gets every byte back.
func TestTheBroadcastLogLetsGoFromTheMiddleWithoutShuffling(t *testing.T) {
	l := NewBroadcastLog()
	q := NewQuota(0, 0)
	l.SetQuota(q)
	t0 := time.Unix(1700000000, 0)
	var total int64
	for i := range 1000 {
		r, err := l.Append(Record{MessageID: fmt.Sprint("m-", i+1), Topic: "state/a",
			Payload: []byte(fmt.Sprint("value ", i+1)), Timestamp: t0.Add(time.Duration(i) * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		total += RecordSize(r)
	}
	size := func(offset uint64) int64 {
		return RecordSize(Record{MessageID: fmt.Sprint("m-", offset), Topic: "state/a",
			Payload: []byte(fmt.Sprint("value ", offset))})
	}

	// Offset 1 is held by a slow session; every odd one behind it is let go.
	var gone []uint64
	for o := uint64(3); o <= 999; o += 2 {
		gone = append(gone, o)
	}
	n, freed, err := l.Remove(gone...)
	if err != nil || n != len(gone) {
		t.Fatalf("remove %d messages: %d (%v)", len(gone), n, err)
	}
	var want int64
	for _, o := range gone {
		want += size(o)
	}
	if freed != want || l.Bytes() != total-want || q.Bytes() != total-want {
		t.Fatalf("freed %d, the log holds %d and the provider %d; want %d, %d and %d",
			freed, l.Bytes(), q.Bytes(), want, total-want, total-want)
	}

	// 499 holes in a slice of 1000 is under half, so they are still there -
	// the reads below are the holes being skipped, not a compaction's work.
	if len(l.records) != 1000 || len(l.holes) != 499 {
		t.Fatalf("the slice holds %d with %d holes, want 1000 and 499: the case under test is not the one reached",
			len(l.records), len(l.holes))
	}
	check := func(when string, wantOffsets []uint64) {
		t.Helper()
		got, err := l.ReadFromN(l.Floor(), 0)
		if err != nil {
			t.Fatalf("%s: read: %v", when, err)
		}
		if len(got) != len(wantOffsets) || l.Len() != len(wantOffsets) {
			t.Fatalf("%s: read %d, Len %d, want %d", when, len(got), l.Len(), len(wantOffsets))
		}
		for i, r := range got {
			if r.Offset != wantOffsets[i] || r.MessageID != fmt.Sprint("m-", wantOffsets[i]) {
				t.Fatalf("%s: read %d is offset %d %q, want offset %d", when, i, r.Offset, r.MessageID, wantOffsets[i])
			}
		}
		_, _, exported := l.Export()
		if len(exported) != len(wantOffsets) {
			t.Errorf("%s: a snapshot would hold %d, want %d", when, len(exported), len(wantOffsets))
		}
	}
	held := []uint64{1, 2}
	for o := uint64(4); o <= 1000; o += 2 {
		held = append(held, o)
	}
	check("with the holes in place", held)
	// A seek by time to a message that has gone lands on the next one held.
	if off, ok, err := l.FirstAtOrAfter(t0.Add(2 * time.Second)); err != nil || !ok || off != 4 {
		t.Errorf("a seek to the moment of offset 3 found %d, %v (%v), want 4", off, ok, err)
	}
	if b, _ := l.ReadFromN(3, 1); len(b) != 1 || b[0].Offset != 4 {
		t.Errorf("a read from 3 returned %+v, want offset 4 first", b)
	}

	// The 500th hole reaches half, and one compaction drops them all.
	if n, _, err := l.Remove(2); err != nil || n != 1 {
		t.Fatalf("remove 2: %d (%v)", n, err)
	}
	if len(l.records) != 500 || len(l.holes) != 0 || l.start != 0 {
		t.Fatalf("after the 500th hole the slice holds %d with %d holes from %d, want 500, 0 and 0",
			len(l.records), len(l.holes), l.start)
	}
	check("compacted", append([]uint64{1}, held[2:]...))

	// The slow session lets go of the front, and the floor passes every gap
	// to the lowest message still held.
	if n, _, err := l.Remove(1); err != nil || n != 1 {
		t.Fatalf("remove 1: %d (%v)", n, err)
	}
	if l.Floor() != 4 || l.Next() != 1001 {
		t.Errorf("floor %d, next %d, want 4 and 1001", l.Floor(), l.Next())
	}
	check("the front let go", held[2:])
	if l.Bytes() != q.Bytes() {
		t.Errorf("the log holds %d and the provider counts %d", l.Bytes(), q.Bytes())
	}
}

// The broadcast log is held in the session provider, so its messages count
// against that provider's bound beside the sessions themselves - including a
// log restored from its file, which arrives full of messages that are in
// memory whether or not anything counted them.
func TestTheSessionsLogCountsAgainstTheirProvider(t *testing.T) {
	s := NewSessions()
	q := NewQuota(0, 0)
	s.SetQuota(q)
	sess := Session{Client: "c", ExpiryInterval: 3600}
	if err := s.Save(sess); err != nil {
		t.Fatal(err)
	}
	lg, _ := s.Log()
	r, err := lg.Append(Record{MessageID: "m-1", Topic: "state/a", Payload: []byte("on"), QoS: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := SessionSize(sess) + RecordSize(r)
	if q.Bytes() != want {
		t.Errorf("the provider counts %d, want the session and the log's message, %d", q.Bytes(), want)
	}

	back := RestoreSessions(s.Snapshot())
	q2 := NewQuota(0, 0)
	back.SetQuota(q2)
	if q2.Bytes() != want {
		t.Errorf("after a restore the provider counts %d, want %d", q2.Bytes(), want)
	}
}

// A session's in-flight table is memory the provider holds, so each entry is
// charged against its bound as it is kept - refused ErrFull when there is no
// room, leaving the table as it was - and given back as it goes. A state
// change keeps an entry rather than adding one, and costs nothing more.
func TestAnInFlightEntryIsChargedToItsProvider(t *testing.T) {
	sess := Session{Client: "c", ExpiryInterval: 3600}
	logged := inTheLog(t, nil, 2)
	q := NewQuota(SessionSize(sess)+logged+InFlightSize, 0)
	s := NewSessions()
	s.SetQuota(q)
	if err := s.Save(sess); err != nil {
		t.Fatal(err)
	}
	inTheLog(t, s, 2)
	first := InFlight{Offset: 1, PacketID: 1, QoS: 2, State: MessageSent}
	if err := s.SetInFlight("c", 5, first); err != nil {
		t.Fatalf("the first entry, with room for it: %v", err)
	}
	if err := s.SetInFlight("c", 5, InFlight{Offset: 2, PacketID: 2, QoS: 1, State: MessageSent}); !errors.Is(err, ErrFull) {
		t.Errorf("a second entry with no room was answered %v, want ErrFull", err)
	}
	first.State = MessageReleased
	if err := s.SetInFlight("c", 5, first); err != nil {
		t.Errorf("a state change at the bound was refused: %v", err)
	}
	if _, got, _ := s.InFlight("c"); len(got) != 1 || got[0] != first {
		t.Errorf("the table holds %+v, want only %+v", got, first)
	}
	if ok, err := s.ClearInFlight("c", 1); !ok || err != nil {
		t.Fatalf("clear: %v, %v", ok, err)
	}
	if q.Bytes() != SessionSize(sess)+logged || s.Bytes() != SessionSize(sess) {
		t.Errorf("after the entry went the provider counts %d and the store %d, want %d and %d",
			q.Bytes(), s.Bytes(), SessionSize(sess)+logged, SessionSize(sess))
	}
}

// inTheLog appends n messages to a session store's broadcast log, so that
// in-flight entries at offsets 1 to n name messages it holds - one past its
// next is refused (ErrPastNext) - and answers what they count against the
// provider. With no store it only answers.
func inTheLog(t *testing.T, s *Sessions, n int) int64 {
	t.Helper()
	var bytes int64
	for i := range n {
		r := Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 2}
		bytes += RecordSize(r)
		if s == nil {
			continue
		}
		lg, _ := s.Log()
		if _, err := lg.Append(r); err != nil {
			t.Fatalf("append message %d to the log: %v", i, err)
		}
	}
	return bytes
}

// A batch of in-flight entries is charged to the provider whole or not at
// all: a batch with no room for every entry is refused ErrFull and charges
// nothing, and one that fits charges each new entry once. An acknowledgement
// gives back exactly what it clears.
func TestAnInFlightBatchIsChargedWholeOrNotAtAll(t *testing.T) {
	sess := Session{Client: "c", ExpiryInterval: 3600}
	logged := inTheLog(t, nil, 3)
	q := NewQuota(SessionSize(sess)+logged+2*InFlightSize, 0)
	s := NewSessions()
	s.SetQuota(q)
	if err := s.Save(sess); err != nil {
		t.Fatal(err)
	}
	inTheLog(t, s, 3)
	base := SessionSize(sess) + logged
	three := []InFlight{
		{Offset: 1, PacketID: 1, QoS: 1, State: MessageSent},
		{Offset: 2, PacketID: 2, QoS: 1, State: MessageSent},
		{Offset: 3, PacketID: 3, QoS: 2, State: MessageSent},
	}
	if err := s.SetInFlightAll("c", 5, three); !errors.Is(err, ErrFull) {
		t.Fatalf("three entries with room for two were answered %v, want ErrFull", err)
	}
	if _, got, _ := s.InFlight("c"); len(got) != 0 || q.Bytes() != base || s.Bytes() != SessionSize(sess) {
		t.Fatalf("after the refusal the table holds %+v and the provider counts %d, the store %d; want nothing, %d and %d",
			got, q.Bytes(), s.Bytes(), base, SessionSize(sess))
	}
	if err := s.SetInFlightAll("c", 5, three[:2]); err != nil {
		t.Fatalf("two entries with room for two: %v", err)
	}
	if q.Bytes() != base+2*InFlightSize || s.Bytes() != SessionSize(sess)+2*InFlightSize {
		t.Errorf("two entries: the provider counts %d and the store %d, want %d and %d",
			q.Bytes(), s.Bytes(), base+2*InFlightSize, SessionSize(sess)+2*InFlightSize)
	}
	if n, err := s.Acknowledge("c", 3, three[:2]); err != nil || n != 2 {
		t.Fatalf("acknowledge both: %d, %v", n, err)
	}
	if q.Bytes() != base || s.Bytes() != SessionSize(sess) {
		t.Errorf("after both were acknowledged the provider counts %d and the store %d, want %d and %d",
			q.Bytes(), s.Bytes(), base, SessionSize(sess))
	}
}

// Invariant 13: a provider counts exactly what its stores hold, however their
// writes interleave. A latest store replacing values on a provider another
// store is filling and emptying settles the difference with the quota in one
// step, so a refused replacement leaves the count as it was; afterwards the
// provider's count is what the two stores hold, to the byte.
//
// **The contention is built, not hoped for.** Every topic holds a value and
// the log has filled the provider before the setters start, and the log
// trims nothing until a replacement has been refused, so at least one
// refused replacement happens on every run. Left to the scheduler, one P
// ran each goroutine through without the other and refused nothing.
func TestAProviderCountsExactlyWhatConcurrentLatestSetsLeave(t *testing.T) {
	q := NewQuota(64*1024, 0)
	lt := NewLatest()
	lt.SetQuota(q)
	lg := NewLog()
	lg.SetQuota(q)

	for i := range 8 {
		if _, err := lt.Set(Record{Topic: fmt.Sprintf("t/%d", i), Payload: make([]byte, 100)}); err != nil {
			t.Fatalf("the first value of t/%d: %v", i, err)
		}
	}
	for {
		if _, err := lg.Append(Record{Topic: "x", Payload: make([]byte, 1500)}); err != nil {
			break
		}
	}

	var wg, setters sync.WaitGroup
	var refused atomic.Int64
	firstRefusal, settersDone := make(chan struct{}), make(chan struct{})
	var once sync.Once
	for g := range 4 {
		wg.Add(1)
		setters.Add(1)
		go func() {
			defer wg.Done()
			defer setters.Done()
			for i := range 3000 {
				size := 100 + ((i+g)%7)*1200
				if _, err := lt.Set(Record{Topic: fmt.Sprintf("t/%d", i%8), Payload: make([]byte, size)}); err != nil {
					if !errors.Is(err, ErrProviderFull) {
						t.Errorf("a refused replacement answered %v, want ErrProviderFull", err)
						return
					}
					refused.Add(1)
					once.Do(func() { close(firstRefusal) })
				}
			}
		}()
	}
	go func() {
		setters.Wait()
		close(settersDone)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 6000 {
			if _, err := lg.Append(Record{Topic: "x", Payload: make([]byte, 1500)}); err != nil {
				// Nothing is trimmed until a replacement has been refused
				// - or until there are no setters left to refuse.
				select {
				case <-firstRefusal:
				case <-settersDone:
				}
				if _, _, err := lg.Trim(time.Time{}, 16*1024); err != nil {
					t.Errorf("trim: %v", err)
					return
				}
			}
		}
	}()
	wg.Wait()

	lt.mu.Lock()
	var latest int64
	for _, r := range lt.current {
		latest += RecordSize(r)
	}
	lt.mu.Unlock()
	if want := lg.Bytes() + latest; q.Bytes() != want {
		t.Errorf("the provider counts %d bytes and its stores hold %d: a replacement's room went astray",
			q.Bytes(), want)
	}
	if refused.Load() == 0 {
		t.Fatal("no replacement was refused, so the provider was never contended and this proves nothing")
	}
	t.Logf("%d replacements refused at the bound", refused.Load())
}

// RFC 0004 "Enforcing a size bound": a memory provider is charged for
// whatever a store already holds when it is attached, and a bound below
// that is not an error - the provider stands over it and refuses the next
// publish until room frees, as a sqlite provider keeps the pages it has and
// refuses more. Each kind of store, restored over its provider's bound.
//
// **Charged whole, never asked for.** Asked for, the attach was refused and
// the store held uncounted for the life of the process: the provider read 0
// of 306 bytes and took the next publish.
func TestAStoreRestoredOverItsProvidersBoundIsChargedWhole(t *testing.T) {
	records := func(n, size int) []Record {
		out := make([]Record, 0, n)
		for i := range n {
			out = append(out, Record{Offset: uint64(i + 1), MessageID: "m", Topic: fmt.Sprintf("t/%d", i),
				Payload: make([]byte, size)})
		}
		return out
	}
	one := Record{MessageID: "m", Topic: "t/new", Payload: make([]byte, 10)}
	for _, tc := range []struct {
		name string
		// restore builds a store holding more than the bound, attaches q,
		// and answers what the store holds and a write asking for room.
		restore func(q *Quota) (held int64, write func() error)
	}{
		{"append", func(q *Quota) (int64, func() error) {
			l := RestoreLog(4, 1, records(3, 100), nil)
			l.SetQuota(q)
			return l.Bytes(), func() error { _, err := l.Append(one); return err }
		}},
		{"latest", func(q *Quota) (int64, func() error) {
			l := RestoreLatest(4, records(3, 100))
			l.SetQuota(q)
			var held int64
			for _, r := range records(3, 100) {
				held += RecordSize(r)
			}
			return held, func() error { _, err := l.Set(one); return err }
		}},
		{"queue", func(q *Quota) (int64, func() error) {
			var items []Item
			for _, r := range records(3, 100) {
				items = append(items, Item{Record: r})
			}
			qu := RestoreQueue(4, items)
			qu.SetQuota(q)
			return qu.Bytes(), func() error { _, err := qu.Enqueue(one); return err }
		}},
		{"sessions", func(q *Quota) (int64, func() error) {
			s := RestoreSessions(&SessionsSnapshot{
				Sessions: []SessionState{{Session: Session{Client: "c", ExpiryInterval: 60,
					Will: &SessionWill{Topic: "w", Payload: make([]byte, 300)}}}},
				Log: &BroadcastSnapshot{Next: 1, Floor: 1},
			})
			s.SetQuota(q)
			return s.Bytes(), func() error { return s.Save(Session{Client: "d", ExpiryInterval: 60}) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := NewQuota(250, 50) // publishes stop at 200
			held, write := tc.restore(q)
			if held <= 250 {
				t.Fatalf("the restored store holds %d bytes, which is not over the bound of 250, so this proves nothing", held)
			}
			if q.Bytes() != held {
				t.Errorf("the provider counts %d of the %d bytes the restored store holds", q.Bytes(), held)
			}
			if !q.OverBound() {
				t.Errorf("the provider holding %d of max 250 does not say it is over its bound", q.Bytes())
			}
			if err := write(); !errors.Is(err, ErrProviderFull) {
				t.Errorf("a write into a provider holding %d of max 250 was answered %v, want ErrProviderFull", q.Bytes(), err)
			}
		})
	}
}

// The same bound at both ends, with no operator change: a stop at which the
// provider stood in its reserve - a dead-letter move, a session begun new -
// leaves stores that do not all fit under max less the reserve at the next
// start. Every one of them is charged, whichever is attached last.
func TestAProviderThatStoppedInItsReserveIsChargedWholeAtTheStart(t *testing.T) {
	logOf := func(size int) *Log {
		return RestoreLog(3, 1, []Record{
			{Offset: 1, MessageID: "m", Topic: "t", Payload: make([]byte, size)},
			{Offset: 2, MessageID: "m", Topic: "t", Payload: make([]byte, size)},
		}, nil)
	}
	q := NewQuota(250, 50)
	a, b := logOf(73), logOf(48)
	a.SetQuota(q)
	b.SetQuota(q)
	if a.Bytes()+b.Bytes() <= 200 {
		t.Fatalf("the two logs hold %d bytes, inside what a publish may take the provider to, so this proves nothing",
			a.Bytes()+b.Bytes())
	}
	if q.Bytes() != a.Bytes()+b.Bytes() {
		t.Errorf("the provider counts %d of the %d bytes its two logs hold", q.Bytes(), a.Bytes()+b.Bytes())
	}
}

// A store restored over its provider's bound gives back, as it is trimmed,
// exactly what it was charged, so the provider goes on counting every other
// store. Uncharged, its trim gave back room the provider had never taken:
// the provider read 0 while another log held 104, and took 200 more bytes on
// top of them.
func TestAStoreRestoredOverTheBoundLeavesTheOthersCountedAsItIsTrimmed(t *testing.T) {
	records := func(n, size int) []Record {
		out := make([]Record, 0, n)
		for i := range n {
			out = append(out, Record{Offset: uint64(i + 1), MessageID: "m", Topic: "t", Payload: make([]byte, size)})
		}
		return out
	}
	q := NewQuota(250, 50)
	counted := RestoreLog(3, 1, records(2, 50), nil)
	counted.SetQuota(q)
	big := RestoreLog(4, 1, records(3, 100), nil)
	big.SetQuota(q)
	if removed, _, err := big.Trim(time.Time{}, 1); err != nil || removed != 3 {
		t.Fatalf("the trim removed %d records (%v), want 3, so this proves nothing", removed, err)
	}
	if q.Bytes() != counted.Bytes() {
		t.Errorf("after the over-bound log was trimmed away the provider counts %d, and the other log holds %d",
			q.Bytes(), counted.Bytes())
	}
	accepted := 0
	for range 4 {
		if _, err := counted.Append(Record{MessageID: "m", Topic: "t", Payload: make([]byte, 48)}); err == nil {
			accepted++
		}
	}
	if q.Bytes() > 200 {
		t.Errorf("%d of 4 publishes were accepted and the provider counts %d, past the 200 a publish may take it to",
			accepted, q.Bytes())
	}
}

// RFC 0003 "Exactly once, and where the unfinished ones wait", on the two
// channel kinds a hold is not pre-checked for as Log.Hold is: a repeated
// PUBLISH of an exchange already held is success and changes nothing
// [MQTT-4.3.3-10], a PUBREL for nothing held stores nothing, and a hold
// dropped gives its room back once. And a queue at its own max_bytes refuses
// the hold, and the release, which keeps the hold for the PUBREL sent again.
func TestAHoldOnALatestOrQueueChannelAnswersAsALogsDoes(t *testing.T) {
	now := time.Unix(1700000000, 0)
	e, other := Exchange{Client: "c", PacketID: 1}, Exchange{Client: "c", PacketID: 2}
	first := Record{MessageID: "m1", Topic: "t/a", Payload: []byte("first")}
	second := Record{MessageID: "m2", Topic: "t/a", Payload: []byte("a second, longer payload")}
	type held interface {
		Hold(Exchange, Record, time.Time) error
		ReleaseHold(Exchange) (Record, bool, error)
		DropHold(Exchange) (bool, error)
		Holds() ([]HeldPublish, error)
	}
	for name, make_ := range map[string]func(q *Quota) held{
		"latest": func(q *Quota) held { l := NewLatest(); l.SetQuota(q); return l },
		"queue":  func(q *Quota) held { u := NewQueue(); u.SetQuota(q); return u },
	} {
		t.Run(name, func(t *testing.T) {
			q := NewQuota(0, 0)
			s := make_(q)
			if err := s.Hold(e, first, now); err != nil {
				t.Fatalf("hold: %v", err)
			}
			charged := q.Bytes()
			if err := s.Hold(e, second, now.Add(time.Second)); err != nil {
				t.Errorf("a repeated PUBLISH of a held exchange was answered %v, want success", err)
			}
			if hs, _ := s.Holds(); len(hs) != 1 || hs[0].Record.MessageID != "m1" || !hs[0].HeldAt.Equal(now) {
				t.Errorf("after a repeat the store holds %+v, want the first copy alone, on its first clock", hs)
			}
			if q.Bytes() != charged {
				t.Errorf("a repeat moved the provider from %d to %d bytes", charged, q.Bytes())
			}
			if r, found, err := s.ReleaseHold(other); found || err != nil || r.Offset != 0 {
				t.Errorf("a release with nothing held answered %+v, %t, %v; want nothing found and nothing stored", r, found, err)
			}
			if dropped, err := s.DropHold(e); !dropped || err != nil {
				t.Errorf("dropping the hold answered %t, %v; want it dropped", dropped, err)
			}
			if q.Bytes() != 0 {
				t.Errorf("the provider counts %d bytes after the only hold was dropped", q.Bytes())
			}
			if dropped, err := s.DropHold(e); dropped || err != nil {
				t.Errorf("dropping it again answered %t, %v; want nothing to drop", dropped, err)
			}
		})
	}

	t.Run("queue at its own max_bytes", func(t *testing.T) {
		u := NewQueue()
		u.SetMaxBytes(RecordSize(first) - 1)
		if err := u.Hold(e, first, now); !errors.Is(err, ErrFull) {
			t.Errorf("a hold past the queue's max_bytes was answered %v, want ErrFull", err)
		}
		u = NewQueue()
		u.SetMaxBytes(RecordSize(first) + RecordSize(second) - 1)
		if err := u.Hold(e, first, now); err != nil {
			t.Fatalf("a hold with room: %v", err)
		}
		if _, err := u.Enqueue(second); err != nil {
			t.Fatalf("filling the queue: %v", err)
		}
		if r, found, err := u.ReleaseHold(e); !found || !errors.Is(err, ErrFull) || r.Offset != 0 {
			t.Errorf("a release into a full queue answered %+v, %t, %v; want the hold found and ErrFull", r, found, err)
		}
		if hs, _ := u.Holds(); len(hs) != 1 {
			t.Errorf("after a refused release the queue holds %d, want the hold kept for the PUBREL sent again", len(hs))
		}
	})
}

// RFC 0002's 0x97 row for a queue: a publish the memory provider has no room
// for is refused, and takes no offset.
func TestAQueuePublishTheProviderHasNoRoomForIsRefused(t *testing.T) {
	u := NewQueue()
	q := NewQuota(100, 0)
	u.SetQuota(q)
	next := u.next
	if _, err := u.Enqueue(Record{MessageID: "m", Topic: "jobs/x", Payload: make([]byte, 200)}); !errors.Is(err, ErrProviderFull) {
		t.Errorf("a publish past the provider's bound was answered %v, want ErrProviderFull", err)
	}
	if total, _ := u.Depth(); total != 0 || u.next != next || q.Bytes() != 0 {
		t.Errorf("a refused publish left %d jobs, next %d (was %d), and %d bytes counted", total, u.next, next, q.Bytes())
	}
}

// RFC 0003 "Broadcast": a shared group's cursor, and a lend to a member the
// store keeps no session for, each take room from the provider, and at its
// bound each is refused ErrProviderFull and leaves what the store held as it
// was - no cursor made, the cursor unmoved, nothing on the returned list.
func TestAGroupsCursorAndALendAreRefusedAtTheProvidersBound(t *testing.T) {
	const group = "$share/g/jobs"
	member := Session{Client: "m", ExpiryInterval: 60, Subscriptions: []SessionSubscription{{Filter: group, QoS: 1}}}
	setup := func(t *testing.T, cursor bool) (*Sessions, *Quota) {
		t.Helper()
		s := NewSessions()
		q := NewQuota(1<<20, 0)
		s.SetQuota(q)
		if err := s.Save(member); err != nil {
			t.Fatalf("save: %v", err)
		}
		lg, _ := s.Log()
		for i := range 3 {
			if _, err := lg.Append(Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if cursor {
			if err := s.CreateShareCursor(group, 1); err != nil {
				t.Fatalf("the group's cursor: %v", err)
			}
		}
		publish, _ := q.Effective()
		q.Charge(publish - q.Bytes()) // the provider's other stores fill it
		return s, q
	}

	t.Run("a group's cursor", func(t *testing.T) {
		s, q := setup(t, false)
		before := q.Bytes()
		if err := s.CreateShareCursor(group, 1); !errors.Is(err, ErrProviderFull) {
			t.Errorf("a cursor at the provider's bound was answered %v, want ErrProviderFull", err)
		}
		if g := s.Snapshot().Groups; len(g) != 0 || q.Bytes() != before {
			t.Errorf("a refused cursor left groups %v and the provider at %d (was %d)", g, q.Bytes(), before)
		}
	})
	t.Run("a lend", func(t *testing.T) {
		s, q := setup(t, true)
		before := q.Bytes()
		if err := s.Lend(group, 3, []uint64{1, 2}); !errors.Is(err, ErrProviderFull) {
			t.Errorf("a lend at the provider's bound was answered %v, want ErrProviderFull", err)
		}
		snap := s.Snapshot()
		if snap.Groups[group] != 1 || len(snap.Returned[group]) != 0 || q.Bytes() != before {
			t.Errorf("a refused lend left the cursor at %d, returned %v, and the provider at %d (was %d)",
				snap.Groups[group], snap.Returned[group], q.Bytes(), before)
		}
	})
}
