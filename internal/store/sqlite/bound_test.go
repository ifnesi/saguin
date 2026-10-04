package sqlite

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// bounded is what both stores are asked of, so the two can be driven
// through one script and compared. A bound one store enforces and the other
// does not is the divergence nobody sees, because neither looks wrong alone.
type bounded interface {
	SetMaxBytes(int64)
	Bytes() int64
}

func sized(payload string) store.Record {
	return store.Record{MessageID: "m", Topic: "events/x", Payload: []byte(payload),
		Timestamp: time.Unix(1770000000, 0)}
}

// RFC 0003 "At a size bound", invariant 13.
//
// A bound that is not the same number in both stores makes one
// configuration mean two amounts, so what a record costs is one function
// and both stores are held to it here.
func TestAChannelRefusesWhatWouldTakeItPastItsBound(t *testing.T) {
	rec := sized("0123456789")
	each := store.RecordSize(rec)
	if each != int64(len(rec.Payload)+len("events/x")+len("m")) {
		t.Fatalf("a record of this shape costs %d, which is not what it is made of", each)
	}

	db := open(t, tempPath(t))
	sq, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	mem := store.NewLog()

	for name, lg := range map[string]interface {
		logStore
		bounded
	}{"sqlite": sq, "memory": mem} {
		// Room for two and not a third.
		lg.SetMaxBytes(each*2 + each/2)

		for i := 1; i <= 2; i++ {
			got, err := lg.Append(rec)
			if err != nil {
				t.Fatalf("%s: append %d was refused: %v", name, i, err)
			}
			if got.Offset != uint64(i) {
				t.Errorf("%s: append %d took offset %d", name, i, got.Offset)
			}
		}
		if lg.Bytes() != each*2 {
			t.Errorf("%s: holds %d bytes after two records of %d", name, lg.Bytes(), each)
		}

		if _, err := lg.Append(rec); !errors.Is(err, store.ErrFull) {
			t.Fatalf("%s: the third record was accepted past the bound, got %v", name, err)
		}

		// A refusal is not a failure of storage, and it must not spend an
		// offset: a channel with a hole in it is one a consumer reads
		// straight past.
		if lg.Next() != 3 {
			t.Errorf("%s: a refused publish moved next to %d, want 3", name, lg.Next())
		}
		if lg.Bytes() != each*2 {
			t.Errorf("%s: a refused publish changed what the channel holds, now %d", name, lg.Bytes())
		}

		// Removing the bound lets it through, so the refusal was the bound
		// and not something else that had gone wrong.
		lg.SetMaxBytes(0)
		if got, err := lg.Append(rec); err != nil || got.Offset != 3 {
			t.Errorf("%s: unbounded, the record took %+v %v", name, got, err)
		}
	}
}

// A queue's bound is flow control rather than a limit: resolution is what
// frees the space, so it accepts work again with nobody acting. If it did
// not, a queue that filled once would be full for ever.
func TestAQueueAtItsBoundRecoversAsWorkIsResolved(t *testing.T) {
	rec := sized("0123456789")
	each := store.RecordSize(rec)

	db := open(t, tempPath(t))
	sq, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	mem := store.NewQueue()

	for name, q := range map[string]interface {
		queueStore
		bounded
	}{"sqlite": sq, "memory": mem} {
		q.SetMaxBytes(each * 2)

		for i := 1; i <= 2; i++ {
			if _, err := q.Enqueue(rec); err != nil {
				t.Fatalf("%s: job %d was refused: %v", name, i, err)
			}
		}
		if _, err := q.Enqueue(rec); !errors.Is(err, store.ErrFull) {
			t.Fatalf("%s: a third job was accepted past the bound, got %v", name, err)
		}

		// A worker takes one and acknowledges it.
		offered, err := q.Offer(1, time.Now())
		if err != nil || len(offered) != 1 {
			t.Fatalf("%s: offer gave %d records, %v", name, len(offered), err)
		}
		held := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch}
		if _, ok, err := q.Resolve(held); err != nil || !ok {
			t.Fatalf("%s: resolve reported %v %v", name, ok, err)
		}

		if q.Bytes() != each {
			t.Errorf("%s: holds %d after resolving one of two, want %d", name, q.Bytes(), each)
		}
		if _, err := q.Enqueue(rec); err != nil {
			t.Errorf("%s: the queue did not accept work after one was resolved: %v", name, err)
		}
	}
}

// RFC 0003 "At a size bound": a bound never refuses the operation that
// would relieve it, and the move into the dead-letter channel is named as
// one of those.
//
// Both stores are held to it here because only one of them had it. The
// sqlite store skips the bound in the append the move uses; the memory
// store went through the ordinary publish path and refused. Nothing caught
// that for the length of the repository, because the conformance tests
// drove a dead-letter, and drove a bound, and never drove both at once -
// which is how two implementations of one contract come apart.
//
// What the failure costs: a queue holding a record it can neither deliver
// nor be rid of, retrying for ever, with the bound that caused it invisible
// from the outside.
func TestTheDeadLetterMoveIsNotRefusedByABound(t *testing.T) {
	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		bound, ok := u.dlq.(bounded)
		if !ok {
			t.Fatalf("the dead-letter channel is a %T, which takes no bound", u.dlq)
		}
		// Room for nothing at all: every publish here would be refused.
		bound.SetMaxBytes(1)

		enqueue(t, u, 1)
		now := time.Unix(1700000000, 0).UTC()

		offered, err := u.q.Offer(1, time.Now())
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer gave %d records, %v", len(offered), err)
		}
		h := store.Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
		if _, ok, err := u.q.Lease(h, now, time.Minute); err != nil || !ok {
			t.Fatalf("lease: ok=%v err=%v", ok, err)
		}

		// Its attempts are spent, so this release dead-letters it.
		out, ok, err := u.q.Release(h, time.Now(), false, 1, deadLetter(u))
		if err != nil {
			t.Fatalf("the dead-letter move was refused by a bound: %v", err)
		}
		if !ok || !out.DeadLettered {
			t.Fatalf("the record was not dead-lettered: ok=%v out=%+v", ok, out)
		}
		if dead := u.dlqRecords(); len(dead) != 1 {
			t.Errorf("the dead-letter channel holds %d records, want 1", len(dead))
		}
	})
}

// The same rule against a *provider's* ceiling, which is the half that
// needed a reserve: SQLite's bound is the engine's, and it refuses any write
// that needs a new page - the dead-letter move included.
//
// Measured before this was built: at a full provider the move is refused for
// any record above about 1.5KiB, and the attempt count above about 4KiB.
// Neither is a publish, and RFC 0003 says neither may be refused, so the
// provider holds back a reserve that only they may spend.
func TestAFullSqliteProviderDoesNotRefuseTheDeadLetterMove(t *testing.T) {
	// 32KiB of reserve, which is eight pages: room for the move and for the
	// attempt count that has to happen first.
	db, err := OpenBounded(tempPath(t), "test", 256<<10, 32<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	q, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	dlq, err := db.Log("jobs__dlq")
	if err != nil {
		t.Fatalf("dead-letter channel: %v", err)
	}
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	// A record in the queue, then another channel takes the provider to the
	// ceiling publishes stop at.
	job := store.Record{MessageID: "j", Topic: "jobs/x", Payload: make([]byte, 3000),
		Timestamp: time.Unix(1770000000, 0)}
	if _, err := q.Enqueue(job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	filler := store.Record{MessageID: "m", Topic: "events/x", Payload: make([]byte, 4096),
		Timestamp: time.Unix(1770000000, 0)}
	var stored int
	for stored < 500 {
		if _, err := lg.Append(filler); err != nil {
			if !errors.Is(err, store.ErrFull) {
				t.Fatalf("filling: %v", err)
			}
			break
		}
		stored++
	}
	if stored == 0 || stored == 500 {
		t.Fatalf("the provider took %d records: it was not brought to its ceiling", stored)
	}

	// The reserve is not a way around the bound: publishes stay refused.
	if _, err := lg.Append(filler); !errors.Is(err, store.ErrFull) {
		t.Fatalf("a publish was accepted at the ceiling: %v", err)
	}

	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer gave %d records, %v", len(offered), err)
	}
	h := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker-1"}

	// The attempt count first: without it the record never reaches its limit,
	// so it never becomes eligible for the move at all.
	if _, ok, err := q.Lease(h, time.Now(), time.Minute); err != nil || !ok {
		t.Fatalf("the attempt count was refused at a full provider: ok=%v err=%v", ok, err)
	}

	out, ok, err := q.Release(h, time.Now(), false, 1, store.DeadLetter{
		Log: dlq,
		Record: func(it store.Item) store.Record {
			r := it.Record
			r.Topic = "jobs__dlq/x"
			r.Headers = []store.Header{{Key: "saguin-dlq-channel", Value: "jobs"}, {Key: "saguin-dlq-attempts", Value: "1"}, {Key: "saguin-dlq-reason", Value: "attempts_exhausted"}}
			return r
		},
	})
	if err != nil {
		t.Fatalf("a full provider refused the dead-letter move: %v", err)
	}
	if !ok || !out.DeadLettered {
		t.Fatalf("the record was not dead-lettered: ok=%v out=%+v", ok, out)
	}

	// And the reserve closed again behind it, so the next publish is refused
	// rather than finding the door open.
	if _, err := lg.Append(filler); !errors.Is(err, store.ErrFull) {
		t.Errorf("a publish was accepted after the reserve was used: %v", err)
	}
}

// trimmer is retention's half of what a log is asked for, so the two
// implementations can be driven through one script and compared.
type trimmer interface {
	logStore
	bounded
	Trim(before time.Time, maxBytes int64) (int, int64, error)
}

var _ trimmer = (*Log)(nil)
var _ trimmer = (*store.Log)(nil)

// bothLogs runs fn against each implementation of an append channel.
func bothLogs(t *testing.T, fn func(t *testing.T, lg trimmer)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) { fn(t, store.NewLog()) })
	t.Run("sqlite", func(t *testing.T) {
		lg, err := open(t, tempPath(t)).Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}
		fn(t, lg)
	})
}

// at returns a record stamped at a given moment, because retention by age
// is the one thing here that turns on a record's own clock.
func at(when time.Time, payload string) store.Record {
	return store.Record{MessageID: "m", Topic: "events/x", Payload: []byte(payload), Timestamp: when}
}

// Invariant 1, which is what the whole of retention rests on: a channel
// distinguishes "you are caught up" from "your position was deleted".
//
// The floor advances in the same operation as the removal. Both halves are
// checked from the outside here - the records are gone AND a read from
// where they were is refused rather than served the oldest survivor, which
// is the failure that reports success.
//
// Nothing has ever exercised this: no floor has ever moved, because nothing
// removed an append record until now.
func TestTrimRaisesTheFloorOverWhatItRemoved(t *testing.T) {
	bothLogs(t, func(t *testing.T, lg trimmer) {
		now := time.Unix(1770000000, 0).UTC()
		for i := range 5 {
			if _, err := lg.Append(at(now.Add(time.Duration(i)*time.Minute), "0123456789")); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		if lg.Floor() != 1 {
			t.Fatalf("the channel starts at floor %d", lg.Floor())
		}

		// Everything published before the third record.
		removed, freed, err := lg.Trim(now.Add(2*time.Minute), 0)
		if err != nil {
			t.Fatalf("trim: %v", err)
		}
		if removed != 2 || freed != 2*store.RecordSize(at(now, "0123456789")) {
			t.Errorf("removed %d records for %d bytes, want 2", removed, freed)
		}

		if lg.Floor() != 3 {
			t.Errorf("the floor is %d after removing offsets 1 and 2, want 3", lg.Floor())
		}
		// The point of the floor: a consumer that was at offset 1 is told its
		// position is gone rather than handed offset 3 and left to report
		// success over the two it never received.
		if _, err := lg.ReadFromN(1, 0); !errors.Is(err, store.ErrBelowFloor) {
			t.Errorf("a read from a removed offset returned %v, want ErrBelowFloor", err)
		}
		got, err := lg.ReadFromN(3, 0)
		if err != nil || len(got) != 3 || got[0].Offset != 3 {
			t.Errorf("reading from the new floor gave %d records, %v", len(got), err)
		}

		// next never moves. Recomputing it from what survives would restart a
		// swept channel at 1 and hand out offsets a stored position already
		// used (invariant 9).
		if lg.Next() != 6 {
			t.Errorf("trimming moved next to %d, want 6", lg.Next())
		}
	})
}

// The quiet one, and the reason this is the case to watch: the bytes come
// back in the same operation. On sqlite the counter is a column, so a
// missed decrement is permanent - the channel would refuse publishes
// against room it is no longer using, and nothing would say why.
func TestTrimGivesTheBytesBack(t *testing.T) {
	bothLogs(t, func(t *testing.T, lg trimmer) {
		now := time.Unix(1770000000, 0).UTC()
		rec := at(now, "0123456789")
		each := store.RecordSize(rec)

		for range 4 {
			if _, err := lg.Append(rec); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if lg.Bytes() != each*4 {
			t.Fatalf("the channel holds %d, want %d", lg.Bytes(), each*4)
		}

		// Down to two records' worth.
		removed, freed, err := lg.Trim(time.Time{}, each*2)
		if err != nil {
			t.Fatalf("trim: %v", err)
		}
		if removed != 2 || freed != each*2 {
			t.Errorf("removed %d records for %d bytes, want 2 and %d", removed, freed, each*2)
		}
		if lg.Bytes() != each*2 {
			t.Errorf("the channel holds %d after the trim, want %d", lg.Bytes(), each*2)
		}

		// And the room is really usable, which a counter that only went one
		// way would not show: the bound is the same one that was full.
		lg.SetMaxBytes(each * 3)
		if _, err := lg.Append(rec); err != nil {
			t.Errorf("the channel refused a record against room the trim freed: %v", err)
		}
	})
}

// A sweep with nothing to do writes nothing and moves nothing. It runs on a
// ticker over every channel, so the common case is the empty one.
func TestTrimWithNothingToDoChangesNothing(t *testing.T) {
	bothLogs(t, func(t *testing.T, lg trimmer) {
		now := time.Unix(1770000000, 0).UTC()
		for range 3 {
			if _, err := lg.Append(at(now, "0123456789")); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		before, floor, next := lg.Bytes(), lg.Floor(), lg.Next()

		for _, tc := range []struct {
			name     string
			before   time.Time
			maxBytes int64
		}{
			{"no rule at all", time.Time{}, 0},
			{"a deadline nothing has reached", now.Add(-time.Hour), 0},
			{"a bound nothing exceeds", time.Time{}, before * 2},
		} {
			removed, freed, err := lg.Trim(tc.before, tc.maxBytes)
			if err != nil || removed != 0 || freed != 0 {
				t.Errorf("%s: removed %d for %d, %v", tc.name, removed, freed, err)
			}
		}
		if lg.Bytes() != before || lg.Floor() != floor || lg.Next() != next {
			t.Errorf("a sweep with nothing to do moved bytes %d->%d floor %d->%d next %d->%d",
				before, lg.Bytes(), floor, lg.Floor(), next, lg.Next())
		}
	})
}

// A record with no timestamp is never too old. Taken at face value an unset
// time is a date in 1754, so a channel of them would be emptied by the
// first sweep that ran - silently, because retention deletes behind a
// producer and never says so.
func TestTrimKeepsRecordsWithNoTimestamp(t *testing.T) {
	bothLogs(t, func(t *testing.T, lg trimmer) {
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x",
			Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("append: %v", err)
		}
		removed, _, err := lg.Trim(time.Now(), 0)
		if err != nil {
			t.Fatalf("trim: %v", err)
		}
		if removed != 0 {
			t.Errorf("a record with no timestamp was removed as too old")
		}
	})
}

// Emptying a channel entirely leaves next where it was and the floor equal
// to it, so the channel reads as "everything up to here is gone" rather
// than as a fresh one. Deriving either from what survives is what makes a
// stored position point at unrelated records (invariants 1 and 9).
func TestTrimmingEverythingLeavesTheCountersMeaningful(t *testing.T) {
	bothLogs(t, func(t *testing.T, lg trimmer) {
		now := time.Unix(1770000000, 0).UTC()
		for range 3 {
			if _, err := lg.Append(at(now, "0123456789")); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		removed, _, err := lg.Trim(now.Add(time.Hour), 0)
		if err != nil {
			t.Fatalf("trim: %v", err)
		}
		if removed != 3 {
			t.Fatalf("removed %d of 3", removed)
		}
		if lg.Next() != 4 || lg.Floor() != 4 {
			t.Errorf("an emptied channel has next %d floor %d, want 4 and 4", lg.Next(), lg.Floor())
		}
		if lg.Bytes() != 0 {
			t.Errorf("an emptied channel holds %d bytes", lg.Bytes())
		}
		// A consumer that had a position anywhere in it is told so.
		if _, err := lg.ReadFromN(2, 0); !errors.Is(err, store.ErrBelowFloor) {
			t.Errorf("a read into an emptied channel returned %v, want ErrBelowFloor", err)
		}
		// And the next record takes the offset after the last one, never 1.
		got, err := lg.Append(at(now, "x"))
		if err != nil || got.Offset != 4 {
			t.Errorf("the record after an emptied channel took offset %d, want 4: %v", got.Offset, err)
		}
	})
}

// The floor and the size are columns, so they have to come back. A broker
// that restarted believing an untrimmed floor would serve records it has
// told a consumer are gone, and one believing an untrimmed size would
// refuse publishes against room it is not using.
func TestATrimmedChannelComesBackTrimmed(t *testing.T) {
	path := tempPath(t)
	now := time.Unix(1770000000, 0).UTC()
	rec := at(now, "0123456789")

	db := open(t, path)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	for range 4 {
		if _, err := lg.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, _, err := lg.Trim(time.Time{}, store.RecordSize(rec)); err != nil {
		t.Fatalf("trim: %v", err)
	}
	floor, held, next := lg.Floor(), lg.Bytes(), lg.Next()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again, err := again(t, path).Log("events")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if again.Floor() != floor || again.Bytes() != held || again.Next() != next {
		t.Errorf("the channel came back with floor %d bytes %d next %d, want %d %d %d",
			again.Floor(), again.Bytes(), again.Next(), floor, held, next)
	}
	// And it still refuses the reads it refused before the restart.
	if _, err := again.ReadFromN(1, 0); !errors.Is(err, store.ErrBelowFloor) {
		t.Errorf("after a restart, a read from a removed offset returned %v", err)
	}
}

// Expiry on a latest channel deletes the current value, and that is the
// point rather than a defect (RFC 0003): a subscriber arriving afterwards
// is told nothing about the topic, exactly as for one that never existed,
// because a stale reading is worse than none.
//
// There is no floor here and nothing to advance - a latest channel has no
// history, so nothing holds a position into it.
func TestTrimExpiresALatestValue(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()

	for name, lt := range map[string]interface {
		latestStore
		Trim(before, deletionsBefore time.Time) (int, int64, error)
	}{"memory": store.NewLatest(), "sqlite": mustLatest(t)} {
		fresh := store.Record{MessageID: "m", Topic: "state/fresh",
			Payload: []byte("0123456789"), Timestamp: now}
		stale := store.Record{MessageID: "m", Topic: "state/stale",
			Payload: []byte("0123456789"), Timestamp: now.Add(-time.Hour)}
		for _, r := range []store.Record{fresh, stale} {
			if _, err := lt.Set(r); err != nil {
				t.Fatalf("%s: set: %v", name, err)
			}
		}

		removed, freed, err := lt.Trim(now.Add(-time.Minute), time.Time{})
		if err != nil {
			t.Fatalf("%s: trim: %v", name, err)
		}
		if removed != 1 || freed != store.RecordSize(stale) {
			t.Errorf("%s: removed %d for %d bytes, want 1 and %d",
				name, removed, freed, store.RecordSize(stale))
		}

		got, err := lt.Match(func(string) bool { return true })
		if err != nil {
			t.Fatalf("%s: match: %v", name, err)
		}
		if len(got) != 1 || got[0].Topic != "state/fresh" {
			t.Errorf("%s: the channel holds %+v, want only state/fresh", name, got)
		}
	}
}

// MQTT-3.3.2-5: a retained message whose publisher's Message Expiry
// Interval has passed is discarded. TrimExpired is the deletion half of
// that rule, and it reads the publisher's clock alone: a value with no
// expiry is kept for ever here, however old, because the operator's
// retention_period has a clock of its own (Trim) and the two must not be
// conflated. It serves the retained store only - on a `latest` channel the
// client's expiry never deletes anything.
func TestTrimExpiredRemovesOnlyValuesThePublisherExpired(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()

	for name, lt := range map[string]interface {
		latestStore
		TrimExpired(now time.Time) (int, int64, error)
	}{"memory": store.NewLatest(), "sqlite": mustLatest(t)} {
		expired := store.Record{MessageID: "m", Topic: "state/expired",
			Payload: []byte("0123456789"), Timestamp: now.Add(-2 * time.Hour),
			MessageExpiry: 3600}
		running := store.Record{MessageID: "m", Topic: "state/running",
			Payload: []byte("0123456789"), Timestamp: now.Add(-30 * time.Minute),
			MessageExpiry: 3600}
		forever := store.Record{MessageID: "m", Topic: "state/forever",
			Payload: []byte("0123456789"), Timestamp: now.Add(-100 * time.Hour)}
		for _, r := range []store.Record{expired, running, forever} {
			if _, err := lt.Set(r); err != nil {
				t.Fatalf("%s: set %s: %v", name, r.Topic, err)
			}
		}

		removed, freed, err := lt.TrimExpired(now)
		if err != nil {
			t.Fatalf("%s: trim expired: %v", name, err)
		}
		if removed != 1 || freed != store.RecordSize(expired) {
			t.Errorf("%s: removed %d for %d bytes, want 1 and %d",
				name, removed, freed, store.RecordSize(expired))
		}

		got, err := lt.Match(func(string) bool { return true })
		if err != nil {
			t.Fatalf("%s: match: %v", name, err)
		}
		held := map[string]bool{}
		for _, r := range got {
			held[r.Topic] = true
		}
		if held["state/expired"] || !held["state/running"] || !held["state/forever"] {
			t.Errorf("%s: the store holds %v, want state/running and state/forever only", name, held)
		}
	}
}

// The same rule as the log's: a value with no timestamp is not stale. On a
// latest channel this one bites hardest, because there is no floor to
// report the loss - the topic simply stops existing.
func TestTrimKeepsALatestValueWithNoTimestamp(t *testing.T) {
	for name, lt := range map[string]interface {
		latestStore
		Trim(before, deletionsBefore time.Time) (int, int64, error)
	}{"memory": store.NewLatest(), "sqlite": mustLatest(t)} {
		if _, err := lt.Set(store.Record{MessageID: "m", Topic: "state/a",
			Payload: []byte("0123456789")}); err != nil {
			t.Fatalf("%s: set: %v", name, err)
		}
		removed, _, err := lt.Trim(time.Now(), time.Time{})
		if err != nil {
			t.Fatalf("%s: trim: %v", name, err)
		}
		if removed != 0 {
			t.Errorf("%s: a value with no timestamp was expired", name)
		}
	}
}

func mustLatest(t *testing.T) *Latest {
	t.Helper()
	lt, err := open(t, tempPath(t)).Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	return lt
}

// again reopens a database the test already closed.
func again(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path, "0.1.0-test")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// RFC 0004: what a record costs is one function for both stores, because
// "two implementations counting differently would make one bound mean two
// amounts".
//
// A queue does not store its counter - it sums the rows when it opens - so
// that sum has to agree with what the live path counted. It did not: the
// scan added length(headers), which is the JSON text in the database, where
// RecordSize counts the keys and values themselves. A queue holding records
// with headers therefore held 54 bytes while running and 67 after a
// restart, and refused publishes at a different point on either side of one
// with nothing saying why.
//
// Headers are what makes this visible, so the record here carries some.
func TestAQueuesSizeIsTheSameAfterAReopen(t *testing.T) {
	path := tempPath(t)
	rec := store.Record{
		MessageID: "m", Topic: "jobs/x", Payload: []byte("0123456789"),
		Headers:   []store.Header{{Key: "saguin-dlq-reason", Value: "attempts_exhausted"}, {Key: "k", Value: "v"}},
		Timestamp: time.Unix(1770000000, 0),
	}

	db := open(t, path)
	q, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if _, err := q.Enqueue(rec); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	live := q.Bytes()
	if live != store.RecordSize(rec) {
		t.Errorf("the live path counted %d, and a record costs %d", live, store.RecordSize(rec))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := open(t, path)
	q2, err := again.Queue("jobs")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := q2.Bytes(); got != live {
		t.Errorf("the same queue holds %d while running and %d after reopening, so one "+
			"max_bytes means two amounts", live, got)
	}
}

// The counter is a column for append channels, so it has to come back. A
// broker that restarted believing an emptied counter would accept writes
// past the bound until something recomputed it - and nothing does. A queue
// sums its own at open and a latest channel has no bound, so this is the
// only one that has to be stored.
func TestAChannelsSizeSurvivesReopening(t *testing.T) {
	path := tempPath(t)
	rec := sized("0123456789")
	each := store.RecordSize(rec)

	db := open(t, path)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	for range 3 {
		if _, err := lg.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again := open(t, path)
	lg2, err := again.Log("events")
	if err != nil {
		t.Fatalf("reopen log: %v", err)
	}
	if lg2.Bytes() != each*3 {
		t.Errorf("the channel came back holding %d bytes, want %d", lg2.Bytes(), each*3)
	}
}

// RFC 0002: a provider's max_bytes bounds the file, and SQLite enforces it
// rather than saguin. What saguin owes is telling a full provider apart
// from a broken one - a database at its ceiling answers the same refusal a
// channel at its bound does, so a producer is told to try later and an
// operator is not sent to look at a disk that is fine.
func TestAProviderAtItsCeilingRefusesLikeAFullChannel(t *testing.T) {
	path := tempPath(t)
	// Small enough to reach in a test, and above what the empty schema takes
	// (store.SQLiteEmptyBytes), so a schema that grows does not leave it
	// opening over its own bound with room for nothing.
	db, err := OpenBounded(path, "test", store.SQLiteEmptyBytes+64<<10, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: make([]byte, 4096),
		Timestamp: time.Unix(1770000000, 0)}

	var stored int
	for range 200 {
		if _, err := lg.Append(rec); err != nil {
			if !errors.Is(err, store.ErrFull) {
				t.Fatalf("after %d records the refusal was %v, want store.ErrFull - "+
					"a full provider reported as a broken one sends an operator to the disk", stored, err)
			}
			break
		}
		stored++
	}
	if stored == 0 {
		t.Fatal("the provider refused the first record: the ceiling was set below anything usable")
	}
	if stored == 200 {
		t.Fatal("200 records of 4KiB fitted in a provider bounded 64KiB above its empty schema: the ceiling was not applied")
	}

	// Readable at the ceiling, which is what makes it a bound rather than a
	// broken database.
	got, err := lg.ReadFrom(1)
	if err != nil {
		t.Fatalf("a bounded database stopped being readable: %v", err)
	}
	if len(got) != stored {
		t.Errorf("read %d records back, want the %d that were accepted", len(got), stored)
	}
}

// A ceiling that rounds down to no pages would be read by SQLite as "tell
// me the current value", so a provider given a few hundred bytes would come
// up unbounded - the silent shape of failure this package is written
// against.
func TestAProviderCeilingBelowOnePageIsRefused(t *testing.T) {
	_, err := OpenBounded(tempPath(t), "test", 100, 0)
	if err == nil {
		t.Fatal("a max_bytes of 100 was accepted")
	}
	if !strings.Contains(err.Error(), "less than one") {
		t.Errorf("the refusal does not say why: %v", err)
	}
}

// RFC 0003 "`queue` - Dead-lettering": a job older than its expiry is
// dead-lettered with reason `expired`.
//
// This is the half no worker can enforce. The broker checks a job's age as
// it hands it over, but a queue whose workers have all gone away is never
// asked for anything - and expiry exists for work nobody is processing, so
// that is the case it most has to reach.
//
// Both stores, because a queue that expired work on one and not the other
// would mean one configuration and two behaviours.
func TestExpiringWaitingJobs(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	job := func(topic string, when time.Time) store.Record {
		return store.Record{MessageID: "m", Topic: topic,
			Payload: []byte("0123456789"), Timestamp: when}
	}

	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		q, ok := u.q.(interface {
			ExpireOlderThan(time.Time, store.DeadLetter) ([]store.Outcome, error)
		})
		if !ok {
			t.Fatalf("a %T cannot expire waiting jobs", u.q)
		}

		stale := job("jobs/old", now.Add(-2*time.Hour))
		fresh := job("jobs/new", now.Add(-time.Minute))
		// No timestamp at all: never too old, or a queue of these would be
		// dead-lettered whole on the first sweep.
		undated := store.Record{MessageID: "m", Topic: "jobs/undated", Payload: []byte("x")}
		for _, r := range []store.Record{stale, fresh, undated} {
			if _, err := u.q.Enqueue(r); err != nil {
				t.Fatalf("enqueue %s: %v", r.Topic, err)
			}
		}

		gone, err := q.ExpireOlderThan(now.Add(-time.Hour), deadLetter(u))
		if err != nil {
			t.Fatalf("expiring: %v", err)
		}
		if len(gone) != 1 {
			t.Fatalf("expired %d jobs, want 1 (only jobs/old is past the hour)", len(gone))
		}
		// Item is the record as it was in the queue; Stored is where it
		// landed. Both, because a dead-lettered record has to be traceable
		// to its origin (invariant 8).
		if gone[0].Item.Topic != "jobs/old" {
			t.Errorf("expired %q, want jobs/old", gone[0].Item.Topic)
		}
		if gone[0].Stored.Offset == 0 {
			t.Error("the expired job reports no offset in the dead-letter channel")
		}

		// It is in the dead-letter channel, and it left the queue.
		if dead := u.dlqRecords(); len(dead) != 1 {
			t.Errorf("the dead-letter channel holds %d records, want 1", len(dead))
		}
		offered, err := u.q.Offer(10, time.Now())
		if err != nil {
			t.Fatalf("offer: %v", err)
		}
		if len(offered) != 2 {
			t.Errorf("the queue holds %d jobs after the expiry, want 2", len(offered))
		}
		for _, o := range offered {
			if o.Topic == "jobs/old" {
				t.Error("the expired job is still being offered to workers")
			}
		}
	})
}

// The first thing a sweep must not do: remove a record in flight to a
// consumer. A job out with a worker is left alone however old it is - it
// expires on its next offer instead, or when the worker hands it back.
//
// Taking it out from under a worker would leave that worker holding a
// delivery for a record that no longer exists, and its answer would arrive
// for a job that is gone.
func TestExpiringLeavesAJobThatIsWithAWorker(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	old := store.Record{MessageID: "m", Topic: "jobs/x",
		Payload: []byte("0123456789"), Timestamp: now.Add(-2 * time.Hour)}

	bothQueues(t, func(t *testing.T, u queueUnderTest) {
		q, ok := u.q.(interface {
			ExpireOlderThan(time.Time, store.DeadLetter) ([]store.Outcome, error)
		})
		if !ok {
			t.Fatalf("a %T cannot expire waiting jobs", u.q)
		}
		if _, err := u.q.Enqueue(old); err != nil {
			t.Fatalf("enqueue: %v", err)
		}

		// Handed to a worker, which now holds it.
		offered, err := u.q.Offer(1, time.Now())
		if err != nil || len(offered) != 1 {
			t.Fatalf("offer gave %d, %v", len(offered), err)
		}
		h := store.Held{Offset: offered[0].Offset, Epoch: offered[0].Epoch, Holder: "worker-1"}
		if _, ok, err := u.q.Lease(h, now, time.Minute); err != nil || !ok {
			t.Fatalf("lease: ok=%v err=%v", ok, err)
		}

		gone, err := q.ExpireOlderThan(now, deadLetter(u))
		if err != nil {
			t.Fatalf("expiring: %v", err)
		}
		if len(gone) != 0 {
			t.Errorf("expired %d jobs that were with a worker, want 0", len(gone))
		}
		if dead := u.dlqRecords(); len(dead) != 0 {
			t.Errorf("a job in flight to a worker was dead-lettered out from under it")
		}

		// And the worker's acknowledgement still lands, which it could not
		// if the record had gone.
		if _, ok, err := u.q.Resolve(h); err != nil || !ok {
			t.Errorf("the worker could not acknowledge its job: ok=%v err=%v", ok, err)
		}
	})
}

// RFC 0005 "The one alert this exists for"
//
// **Both stores report the same worst-placed consumer**, which is the
// number the alert compares against the retention floor: a floor above it
// is data a consumer had not reached and retention has already removed.
//
// It is asked of both because they keep it two different ways - the memory
// store walks a map it already holds, the sqlite one keeps a field so that
// a scrape never reads a row per consumer - and two ways of answering one
// question is exactly where they drift.
func TestBothStoresAgreeOnTheLowestConsumerPosition(t *testing.T) {
	bothLogs(t, func(t *testing.T, lg trimmer) {
		type positioned interface {
			SavePosition(store.Position) error
			DropPosition(string) (bool, error)
			LowestPosition() (uint64, int)
		}
		p, ok := lg.(positioned)
		if !ok {
			t.Fatalf("%T keeps no positions", lg)
		}

		// Nobody has a position: no minimum, and not a minimum of zero.
		// Offsets start at one, so zero says "none" and cannot be mistaken
		// for a consumer stuck at the beginning.
		if got, _ := p.LowestPosition(); got != 0 {
			t.Errorf("a channel with no consumers reports %d, want 0 meaning none", got)
		}

		// Records up to the highest position stored below, since a position
		// past the log's next is refused as it is written (store.ErrPastNext).
		for range 90 {
			if _, err := lg.Append(at(time.Now(), "x")); err != nil {
				t.Fatalf("append: %v", err)
			}
		}

		save := func(reader string, at uint64) {
			t.Helper()
			if err := p.SavePosition(store.Position{
				Reader: store.MQTTReader(reader), Offset: at,
				LastSeen: time.Now(), ExpiresIn: time.Hour,
			}); err != nil {
				t.Fatalf("save %s: %v", reader, err)
			}
		}

		save("fast", 90)
		if got, _ := p.LowestPosition(); got != 90 {
			t.Errorf("one consumer at 90 reports %d", got)
		}
		save("slow", 12)
		if got, _ := p.LowestPosition(); got != 12 {
			t.Errorf("with a consumer at 12 behind one at 90, reports %d, want 12", got)
		}
		save("middling", 40)
		if got, _ := p.LowestPosition(); got != 12 {
			t.Errorf("a consumer added above the lowest changed it to %d", got)
		}

		// **The half this test used to stop one line short of, and the half
		// that was broken.** Everything above only ever moves the minimum
		// down, which an implementation that keeps a field and lowers it on
		// the way past satisfies exactly - so the sqlite store passed this
		// while reporting the first offset any consumer ever stored, for
		// ever after. The alert RFC 0005 says the whole catalogue exists for
		// fired on a broker where every consumer had caught up.
		//
		// Two ways the minimum rises, and a store has to answer both:
		// the consumer holding it moves on, and the consumer holding it goes
		// away.
		save("slow", 75)
		if got, _ := p.LowestPosition(); got != 40 {
			t.Errorf("the slowest consumer moved from 12 to 75 and the minimum is %d, "+
				"want 40 - the next-slowest. A minimum that only falls reports a lag "+
				"nobody has, and against the retention floor that is the one alert "+
				"firing on a healthy broker", got)
		}

		if _, err := p.DropPosition(store.MQTTReader("middling")); err != nil {
			t.Fatalf("drop middling: %v", err)
		}
		if got, _ := p.LowestPosition(); got != 75 {
			t.Errorf("the consumer at 40 was dropped and the minimum is %d, want 75", got)
		}

		// And every consumer gone is none again, not a minimum of zero
		// left over from the last one.
		for _, reader := range []string{"fast", "slow"} {
			if _, err := p.DropPosition(store.MQTTReader(reader)); err != nil {
				t.Fatalf("drop %s: %v", reader, err)
			}
		}
		if got, _ := p.LowestPosition(); got != 0 {
			t.Errorf("every consumer dropped and the minimum is %d, want 0 meaning none", got)
		}
	})
}

// RFC 0004 "The reserve".
//
// **A provider that is full can still run retention, which is what makes a
// full provider recoverable rather than stuck.** RFC 0004 says retention
// does not use the reserve - it frees room rather than relieving the
// provider on saguin's behalf - and that it may therefore be refused when
// there is none to spare, running again on the next tick. Read on its own
// that is a livelock waiting to happen: a provider at its ceiling that
// cannot delete anything never comes back, and the sentence says nothing
// about what stops it.
//
// This is what stops it. A delete frees pages rather than needing them, so
// retention at the ceiling is not the operation the reserve exists for and
// does not need it. Driven rather than reasoned about, because the
// reasoning is a claim about SQLite's page allocation and the only thing
// worth having is what the database does.
//
// Age rather than size on purpose: the size half is answered from the
// channel's byte count without reading anything, so a size sweep at the
// ceiling would prove nothing about a transaction under a full provider.
func TestRetentionRunsOnAFullProviderAndGivesTheRoomBack(t *testing.T) {
	path := tempPath(t)
	// A reserve big enough to be real, inside a ceiling small enough to
	// reach: the publish ceiling is what fills, and the reserve stays shut.
	db, err := OpenBounded(path, "test", 256<<10, 16<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	old := time.Unix(1700000000, 0)
	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: make([]byte, 4096),
		Timestamp: old}

	stored := 0
	for range 500 {
		if _, err := lg.Append(rec); err != nil {
			if !errors.Is(err, store.ErrFull) {
				t.Fatalf("after %d records the refusal was %v, want store.ErrFull", stored, err)
			}
			break
		}
		stored++
	}
	if stored == 0 || stored == 500 {
		t.Fatalf("the provider stored %d records: it was never filled, so nothing below "+
			"is being asked of a full provider", stored)
	}
	// The next publish is refused, which is the state this test is about.
	if _, err := lg.Append(rec); !errors.Is(err, store.ErrFull) {
		t.Fatalf("the provider took another record after %d: it is not full, and the "+
			"retention below would be running against room it did not have to find",
			stored)
	}

	// Retention by age, on a provider that is refusing publishes.
	removed, freed, err := lg.Trim(old.Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("retention on a full provider failed: %v - RFC 0004 says it may be "+
			"refused for want of room and run again next tick, and a provider whose "+
			"retention can never run is one that never recovers", err)
	}
	if removed != stored || freed <= 0 {
		t.Fatalf("retention removed %d of %d records and %d bytes: it ran and did "+
			"nothing, which recovers no provider", removed, stored, freed)
	}

	// And the room came back: the provider takes records again.
	if _, err := lg.Append(rec); err != nil {
		t.Fatalf("a provider that had just given back %d bytes still refuses a record: "+
			"%v - the pages retention freed are not reachable by a publish, and a full "+
			"provider is then full for good", freed, err)
	}
}

// RFC 0002 "Every session's state": the broadcast log gives way to a write its
// provider refused, and not to one a store's own bound refused, which room in
// the log cannot help.
//
// **So the two refusals are told apart, on both providers**, while both stay
// ErrFull to everything that asks only that. A channel at its own max_bytes
// answers ErrFull and not ErrProviderFull - the memory store's check, and the
// sqlite store's reserve before its transaction. Another channel filling the
// provider is refused ErrProviderFull - the memory quota, and SQLITE_FULL at
// the page ceiling.
func TestAProviderRefusalIsToldApartFromAStoresOwnBound(t *testing.T) {
	type channel interface {
		Append(store.Record) (store.Record, error)
		bounded
	}
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var own, filler channel
			switch provider {
			case "memory":
				q := store.NewQuota(256<<10, 0)
				a, b := store.NewLog(), store.NewLog()
				a.SetQuota(q)
				b.SetQuota(q)
				own, filler = a, b
			case "sqlite":
				db, err := OpenBounded(tempPath(t), "test", 256<<10, 0)
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				t.Cleanup(func() { _ = db.Close() })
				a, err := db.Log("own")
				if err != nil {
					t.Fatalf("log: %v", err)
				}
				b, err := db.Log("filler")
				if err != nil {
					t.Fatalf("log: %v", err)
				}
				own, filler = a, b
			}
			rec := store.Record{MessageID: "m", Topic: "x/y", Payload: make([]byte, 4096),
				Timestamp: time.Unix(1770000000, 0)}

			own.SetMaxBytes(12 << 10)
			var err error
			for range 10 {
				if _, err = own.Append(rec); err != nil {
					break
				}
			}
			if !errors.Is(err, store.ErrFull) {
				t.Fatalf("a channel at its own bound answered %v, want ErrFull", err)
			}
			if errors.Is(err, store.ErrProviderFull) {
				t.Errorf("a channel at its own bound answered ErrProviderFull: the log would give way to it " +
					"and it would still be refused")
			}

			stored := 0
			for range 100 {
				if _, err = filler.Append(rec); err != nil {
					break
				}
				stored++
			}
			if stored == 0 || stored == 100 {
				t.Fatalf("the filler stored %d records (last answer %v), so the provider was never brought to "+
					"its bound", stored, err)
			}
			if !errors.Is(err, store.ErrProviderFull) {
				t.Errorf("the provider at its bound answered %v, want ErrProviderFull", err)
			}
			if !errors.Is(err, store.ErrFull) {
				t.Errorf("the provider at its bound answered %v, which is not ErrFull to what asks only that", err)
			}
		})
	}
}

// **The expiry sweep's dead-letter move takes the reserve too**. ExpireOlderThan moved each expired job in an
// ordinary transaction while Release moved the same kind of job in the
// reserve, so at a full provider the sweep moved two jobs and was refused
// on the third, for ever: the queue it exists for has no worker, so nothing
// else frees room. A queue is filled with expired jobs to the ceiling
// publishes stop at, and the sweep must move them without being refused.
func TestAFullSqliteProviderDoesNotRefuseTheExpiryMove(t *testing.T) {
	const page = 4096
	db, err := OpenBounded(tempPath(t), "test", 64*page, 4*page)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	q, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	dlq, err := db.Log("jobs__dlq")
	if err != nil {
		t.Fatalf("dead-letter channel: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	var n int
	for ; n < 10000; n++ {
		_, err := q.Enqueue(store.Record{MessageID: fmt.Sprint("m", n), Topic: "jobs/x",
			Payload: make([]byte, 1500), Timestamp: old})
		if errors.Is(err, store.ErrFull) {
			break
		}
		if err != nil {
			t.Fatalf("enqueue %d: %v", n, err)
		}
	}
	if n == 0 || n == 10000 {
		t.Fatalf("the queue took %d jobs: the provider was not brought to its ceiling", n)
	}
	out, err := q.ExpireOlderThan(time.Now(), store.DeadLetter{Log: dlq, Record: func(it store.Item) store.Record {
		r := it.Record
		r.Topic = "jobs__dlq/x"
		r.Headers = []store.Header{{Key: "saguin-dlq-reason", Value: "expired"}}
		return r
	}})
	if err != nil {
		t.Fatalf("the expiry sweep was refused after moving %d of %d expired jobs at a full provider: %v",
			len(out), n, err)
	}
	if len(out) != n || dlq.Next() != uint64(n)+1 {
		t.Errorf("the sweep moved %d of %d expired jobs, and the dead-letter channel's next is %d, want %d",
			len(out), n, dlq.Next(), n+1)
	}
}

// **Recording a disconnect takes the reserve**. The write starts the clock that ends the session and may
// withdraw a Will, and the row grows by the time it now carries; in an
// ordinary transaction a full provider refused it for 717 of 887 sessions.
// Sessions are saved to the ceiling, and every one's disconnect must be
// recorded.
func TestAFullSqliteProviderRecordsEveryDisconnect(t *testing.T) {
	const page = 4096
	db, err := OpenBounded(tempPath(t), "test", 48*page, 4*page)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := db.Sessions()
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	var n int
	for ; n < 100000; n++ {
		err := s.Save(store.Session{Client: fmt.Sprintf("c%05d", n), ExpiryInterval: 3600,
			Subscriptions: []store.SessionSubscription{{Filter: strings.Repeat("f", 40)}}})
		if errors.Is(err, store.ErrFull) {
			break
		}
		if err != nil {
			t.Fatalf("save %d: %v", n, err)
		}
	}
	if n == 0 || n == 100000 {
		t.Fatalf("the provider took %d sessions: it was not brought to its ceiling", n)
	}
	var refused int
	var first error
	for i := range n {
		if err := s.Disconnected(fmt.Sprintf("c%05d", i), time.Now(), false, 3600); err != nil {
			refused++
			if first == nil {
				first = err
			}
		}
	}
	if refused > 0 {
		t.Errorf("recording a disconnect was refused for %d of %d sessions at a full provider: %v",
			refused, n, first)
	}
}
