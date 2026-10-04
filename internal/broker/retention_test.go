package broker

// Unit tests for the retention sweep. The sweep is driven directly rather
// than through the ticker: what the ticker does is obvious, and waiting for
// it would put a second of sleep in every one of these.

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// surviving counts what a channel still holds. LogStore has no Len - a
// count costs a scan in a store that keeps its records on a disk, and
// nothing on the delivery path asks for one - so this reads from the floor,
// which is what a consumer starting fresh would do.
func surviving(t *testing.T, lg LogStore) int {
	t.Helper()
	got, err := lg.ReadFrom(lg.Floor())
	if err != nil {
		t.Fatalf("reading from the floor: %v", err)
	}
	return len(got)
}

func retentionBroker(t *testing.T, chans ...*channel.Channel) *Broker {
	t.Helper()
	reg, err := channel.NewRegistry(chans)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return New(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// The age interval is derived rather than exposed, because the only thing
// it decides is how far past its deadline a record may survive - and an
// operator who set it coarser than their own retention would have set it
// wrongly.
//
// The size interval is not derived at all: a size bound carries no deadline
// to derive from, and what it decides is how far above their number a
// channel may sit, where the honest answer is "as little as it costs".
func TestTheAgeSweepIntervalIsDerivedFromTheShortestDeadline(t *testing.T) {
	for name, tc := range map[string]struct {
		chans []*channel.Channel
		want  time.Duration
	}{
		"a tenth of the only deadline": {
			[]*channel.Channel{{Name: "events", Type: channel.Append, RetentionPeriod: 200}},
			20 * time.Second,
		},
		"the shortest of several": {
			[]*channel.Channel{
				{Name: "slow", Type: channel.Append, RetentionPeriod: 86400},
				{Name: "fast", Type: channel.Append, RetentionPeriod: 100},
			},
			10 * time.Second,
		},
		// job_expires_after rides the same clock and lands on this sweep when it
		// is enforced, so it counts as a deadline now.
		"a queue's job_expires_after counts": {
			[]*channel.Channel{
				{Name: "events", Type: channel.Append, RetentionPeriod: 86400},
				{Name: "jobs", Type: channel.Queue, JobExpiresAfter: 200, VisibilityTimeout: 30, MaxAttempts: 3},
			},
			20 * time.Second,
		},
		// **A deletion's own period is a deadline too**, and it was missed:
		// a latest channel keeping its values for ever but its deletions for
		// an hour contributed nothing here, so on a broker whose other
		// channel retains for a month the deletions were swept every three
		// days - outliving their period by seventy times, where a tenth is
		// what this promises.
		"a latest channel's deletion_retention_period counts": {
			[]*channel.Channel{
				{Name: "events", Type: channel.Append, RetentionPeriod: 30 * 86400},
				{Name: "state", Type: channel.Latest, DeletionRetentionPeriod: 200},
			},
			20 * time.Second,
		},
		// No ceiling: a channel retaining for a month is served by a sweep
		// every three days, and reading every channel every thirty seconds
		// to find nothing would be work for nobody. What covers the long end
		// is the sweep Run does before any ticker fires.
		"a long period gets a long interval": {
			[]*channel.Channel{{Name: "events", Type: channel.Append, RetentionPeriod: 30 * 86400}},
			3 * 24 * time.Hour,
		},
		"clamped at the floor": {
			[]*channel.Channel{{Name: "events", Type: channel.Append, RetentionPeriod: 1}},
			ageSweepFloor,
		},
		// Size-only and nothing at all both still get a ticker, so a period
		// added later needs no special case.
		"size only": {
			[]*channel.Channel{{Name: "events", Type: channel.Append, RetentionBytes: 1 << 20}},
			ageSweepFloor,
		},
		"nothing configured at all": {
			[]*channel.Channel{{Name: "events", Type: channel.Append}},
			ageSweepFloor,
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := retentionBroker(t, tc.chans...)
			if got := b.ageSweepPeriod(); got != tc.want {
				t.Errorf("age sweep interval %s, want %s", got, tc.want)
			}
		})
	}
}

// Age and size both remove, and the floor moves over what went. The store
// is what enforces each rule; this is the broker asking for the right one.
func TestSweepRemovesByAgeAndBySize(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	rec := func(when time.Time) store.Record {
		return store.Record{MessageID: "m", Topic: "events/x",
			Payload: []byte("0123456789"), Timestamp: when}
	}
	each := store.RecordSize(rec(now))

	t.Run("by age", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "events", Type: channel.Append, RetentionPeriod: 3600,
		})
		// Two records older than the hour, two inside it.
		for _, age := range []time.Duration{-3 * time.Hour, -2 * time.Hour, -time.Minute, 0} {
			if _, err := b.logs["events"].Append(rec(now.Add(age))); err != nil {
				t.Fatalf("append: %v", err)
			}
		}

		b.sweepByAge(now)

		if got := b.logs["events"].Floor(); got != 3 {
			t.Errorf("the floor is %d after two records aged out, want 3", got)
		}
		got, err := b.logs["events"].ReadFrom(3)
		if err != nil || len(got) != 2 {
			t.Errorf("the channel holds %d records after the sweep, %v", len(got), err)
		}
	})

	t.Run("by size", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "events", Type: channel.Append, RetentionBytes: each * 2,
		})
		for range 5 {
			if _, err := b.logs["events"].Append(rec(now)); err != nil {
				t.Fatalf("append: %v", err)
			}
		}

		b.sweepBySize()

		got, err := b.logs["events"].ReadFrom(b.logs["events"].Floor())
		if err != nil || len(got) != 2 {
			t.Errorf("the channel holds %d records after the sweep, want 2: %v", len(got), err)
		}
		if floor := b.logs["events"].Floor(); floor != 4 {
			t.Errorf("the floor is %d, want 4", floor)
		}
	})

	// A latest channel takes the period and expires the value of a topic
	// that has gone quiet. There is no floor here - nothing holds a position
	// into a channel with no history - so the topic simply stops existing.
	t.Run("a latest value expires", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "state", Type: channel.Latest, RetentionPeriod: 3600,
		})
		for topic, when := range map[string]time.Time{
			"state/quiet": now.Add(-2 * time.Hour),
			"state/live":  now.Add(-time.Minute),
		} {
			if _, err := b.latest["state"].Set(store.Record{MessageID: "m", Topic: topic,
				Payload: []byte("0123456789"), Timestamp: when}); err != nil {
				t.Fatalf("set %s: %v", topic, err)
			}
		}

		b.sweepByAge(now)

		got, err := b.latest["state"].Match(func(string) bool { return true })
		if err != nil {
			t.Fatalf("match: %v", err)
		}
		if len(got) != 1 || got[0].Topic != "state/live" {
			t.Errorf("the channel holds %+v, want only state/live", got)
		}
	})
}

// The two sweeps run on their own clocks, so each must apply only its own
// rule. If either evaluated both, the tighter interval size needs would be
// tied to the looser one age wants - which is the whole reason they are
// separate - and a channel would be trimmed by a rule whose ticker had not
// fired.
func TestEachSweepAppliesOnlyItsOwnRule(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	rec := func(when time.Time) store.Record {
		return store.Record{MessageID: "m", Topic: "events/x",
			Payload: []byte("0123456789"), Timestamp: when}
	}
	each := store.RecordSize(rec(now))

	// A channel with both rules, and records that are over on both counts:
	// four of them, all older than the period, against a bound of two.
	build := func(t *testing.T) *Broker {
		t.Helper()
		b := retentionBroker(t, &channel.Channel{
			Name: "events", Type: channel.Append,
			RetentionPeriod: 3600, RetentionBytes: each * 2,
		})
		for range 4 {
			if _, err := b.logs["events"].Append(rec(now.Add(-2 * time.Hour))); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		return b
	}

	t.Run("the size sweep ignores age", func(t *testing.T) {
		b := build(t)
		b.sweepBySize()
		// Two left, because the bound says two - not zero, which is what
		// applying the age rule as well would have left.
		if got := surviving(t, b.logs["events"]); got != 2 {
			t.Errorf("the size sweep left %d records, want 2", got)
		}
	})

	t.Run("the age sweep ignores size", func(t *testing.T) {
		b := build(t)
		b.sweepByAge(now)
		// None left, because everything is older than the period. A size
		// rule applied here would stop at two.
		if got := surviving(t, b.logs["events"]); got != 0 {
			t.Errorf("the age sweep left %d records, want 0", got)
		}
	})

	// And a channel with only one of the rules is untouched by the other's
	// sweep, which is what makes running them at different rates safe.
	t.Run("a size-only channel is not touched by the age sweep", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "events", Type: channel.Append, RetentionBytes: each * 2,
		})
		for range 4 {
			if _, err := b.logs["events"].Append(rec(now.Add(-100 * time.Hour))); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		b.sweepByAge(now)
		if got := surviving(t, b.logs["events"]); got != 4 {
			t.Errorf("the age sweep removed %d records from a channel with no period", 4-got)
		}
	})

	t.Run("an age-only channel is not touched by the size sweep", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "events", Type: channel.Append, RetentionPeriod: 3600,
		})
		for range 4 {
			if _, err := b.logs["events"].Append(rec(now.Add(-2 * time.Hour))); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		b.sweepBySize()
		if got := surviving(t, b.logs["events"]); got != 4 {
			t.Errorf("the size sweep removed %d records from a channel with no byte bound", 4-got)
		}
	})
}

// A queue is never swept. Removing unacknowledged work by age or size is
// eviction of unresolved work, which is never permitted (invariant 2).
//
// The configuration already refuses the keys that would ask for it, so this
// is the second line: a queue reaches the sweep with no policy, and the
// sweep does not go looking for one.
func TestSweepLeavesAQueueAlone(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	b := retentionBroker(t,
		&channel.Channel{Name: "jobs", Type: channel.Queue,
			VisibilityTimeout: 30, MaxAttempts: 3,
			// What a configuration cannot express, set by hand, so that this
			// tests the sweep rather than the validation that precedes it.
			RetentionPeriod: 1, RetentionBytes: 1,
		})

	old := store.Record{MessageID: "m", Topic: "jobs/x", Payload: []byte("0123456789"),
		Timestamp: now.Add(-24 * time.Hour)}
	if _, err := b.queues["jobs"].Enqueue(old); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	b.sweepByAge(now)
	b.sweepBySize()

	offered, err := b.queues["jobs"].Offer(10, time.Now())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if len(offered) != 1 {
		t.Errorf("the sweep removed unacknowledged work: %d records remain, want 1", len(offered))
	}
}

// A queue's dead-letter channel is an ordinary append channel and IS swept,
// through the two dlq_retention_* keys the queue carries for it. Its
// records are resolved work, not unresolved: nothing is waiting on them.
func TestSweepReachesADeadLetterChannel(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	b := retentionBroker(t, &channel.Channel{
		Name: "jobs", Type: channel.Queue, VisibilityTimeout: 30, MaxAttempts: 3,
		DLQRetentionPeriod: 3600,
	})

	dlq := b.reg.Get("jobs").DLQ
	if dlq.RetentionPeriod != 3600 {
		t.Fatalf("the derived channel has period %d, want 3600", dlq.RetentionPeriod)
	}
	if _, err := b.logs[dlq.Name].Append(store.Record{MessageID: "m", Topic: dlq.Name + "/x",
		Payload: []byte("0123456789"), Timestamp: now.Add(-2 * time.Hour)}); err != nil {
		t.Fatalf("append: %v", err)
	}

	b.sweepByAge(now)

	if floor := b.logs[dlq.Name].Floor(); floor != 2 {
		t.Errorf("the dead-letter channel's floor is %d after its record aged out, want 2", floor)
	}
}

// A channel that keeps everything is not touched, and neither is one whose
// records are all inside its period. The sweep runs over every channel on
// every tick, so doing nothing is the common case and it must cost nothing
// and change nothing.
func TestSweepLeavesAChannelWithNoPolicyAlone(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	b := retentionBroker(t,
		&channel.Channel{Name: "audit", Type: channel.Append},
		&channel.Channel{Name: "events", Type: channel.Append, RetentionPeriod: 86400},
	)
	for _, name := range []string{"audit", "events"} {
		if _, err := b.logs[name].Append(store.Record{MessageID: "m", Topic: name + "/x",
			Payload: []byte("0123456789"), Timestamp: now.Add(-time.Hour)}); err != nil {
			t.Fatalf("append to %s: %v", name, err)
		}
	}

	b.sweepByAge(now)
	b.sweepBySize()

	for _, name := range []string{"audit", "events"} {
		if floor := b.logs[name].Floor(); floor != 1 {
			t.Errorf("%s: the floor moved to %d with nothing to remove", name, floor)
		}
		got, err := b.logs[name].ReadFrom(1)
		if err != nil || len(got) != 1 {
			t.Errorf("%s: holds %d records, %v", name, len(got), err)
		}
	}
}

// The first of the three things the sweep must not do: remove a record in
// flight to a consumer.
//
// This is the one that had been reasoned about rather than run. A consumer
// draining a channel holds a position and is fed a window at a time, so the
// question is what a sweep between two windows does to it - and the answer
// has to be that what it has already been sent still arrives, and what it
// has not is either still there or reported gone, never skipped.
func TestSweepDoesNotSkipRecordsUnderAConsumer(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	b := retentionBroker(t, &channel.Channel{
		Name: "events", Type: channel.Append, RetentionPeriod: 3600,
	})
	lg := b.logs["events"]

	// Ten records, the first four already older than the period.
	for i := range 10 {
		age := -2 * time.Hour
		if i >= 4 {
			age = -time.Minute
		}
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x",
			Payload: []byte("0123456789"), Timestamp: now.Add(age)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// A consumer has drained the first six and stored its position there.
	first, err := lg.ReadFromN(1, 6)
	if err != nil || len(first) != 6 {
		t.Fatalf("the first window gave %d records, %v", len(first), err)
	}
	if err := lg.SavePosition(store.Position{
		Reader: store.MQTTReader("c1"), Offset: 7, LastSeen: now, ExpiresIn: time.Hour}); err != nil {
		t.Fatalf("save position: %v", err)
	}

	b.sweepByAge(now)

	// The four that aged out are gone and the floor says so.
	if floor := lg.Floor(); floor != 5 {
		t.Fatalf("the floor is %d after four records aged out, want 5", floor)
	}

	// The consumer is at 7, which is above the floor, so it carries on and
	// receives every record it had not yet been sent - none skipped, none
	// repeated.
	rest, err := lg.ReadFrom(7)
	if err != nil {
		t.Fatalf("the consumer could not carry on: %v", err)
	}
	if len(rest) != 4 {
		t.Errorf("the consumer was served %d records after the sweep, want 4", len(rest))
	}
	for i, r := range rest {
		if want := uint64(7 + i); r.Offset != want {
			t.Errorf("record %d has offset %d, want %d", i, r.Offset, want)
		}
	}
}

// And the other half of the same rule: a consumer whose position the sweep
// HAS passed is told, rather than served the oldest survivor and left to
// report success over what it never received (invariant 1).
//
// This path has never been reachable - no floor had ever moved - and
// retention is what makes it fire.
func TestASweepPastAConsumerIsReported(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	b := retentionBroker(t, &channel.Channel{
		Name: "events", Type: channel.Append, RetentionPeriod: 3600,
	})
	lg := b.logs["events"]

	for range 5 {
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x",
			Payload: []byte("0123456789"), Timestamp: now.Add(-2 * time.Hour)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// A consumer that had got as far as the second record and went away.
	if err := lg.SavePosition(store.Position{
		Reader: store.MQTTReader("c1"), Offset: 2, LastSeen: now, ExpiresIn: time.Hour}); err != nil {
		t.Fatalf("save position: %v", err)
	}

	b.sweepByAge(now)

	if floor := lg.Floor(); floor != 6 {
		t.Fatalf("the floor is %d after the whole channel aged out, want 6", floor)
	}
	// Its stored position still says 2, and a read from there is refused.
	// That refusal is the whole of invariant 1: the alternative is being
	// handed record 6 and concluding that 2 through 5 were processed.
	if _, err := lg.ReadFrom(2); err == nil {
		t.Error("a read from a swept position was served rather than refused")
	}
}

// RFC 0003: a `latest` channel keeps a deletion on its own clock, and the
// sweep is where that clock is read.
//
// **Nothing drove this until now.** The two clocks were tested where they
// are implemented - both stores, one script - and the wiring between a
// channel's `deletion_retention_period` and the sweep that reads it was
// not, which is the half a defect hides in: it would look correct for a day
// and then keep every deletion for ever, or expire values on the deletion
// clock and lose live state.
func TestTheAgeSweepReadsBothOfALatestChannelsClocks(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	value := func(topic string, at time.Time) store.Record {
		return store.Record{MessageID: "m-" + topic, Topic: topic,
			Payload: []byte("here"), Timestamp: at}
	}
	deletion := func(topic string, at time.Time) store.Record {
		return store.Record{MessageID: "d-" + topic, Topic: topic, Timestamp: at}
	}
	held := func(t *testing.T, lt LatestStore, topic string) bool {
		t.Helper()
		_, ok, err := lt.Get(topic)
		if err != nil {
			t.Fatalf("get %s: %v", topic, err)
		}
		return ok
	}

	// Values kept for a day, deletions for an hour: the shape the two
	// clocks exist for, and the one where sharing a clock would be wrong in
	// both directions at once.
	t.Run("each expires on its own", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "state", Type: channel.Latest,
			RetentionPeriod: 86400, DeletionRetentionPeriod: 3600,
		})
		lt := b.latest["state"]
		for _, r := range []store.Record{
			value("state/fresh", now.Add(-30*time.Minute)),
			// Older than the deletion period, younger than the value one:
			// under one clock this would still be here, and a copy would
			// never be told the topic is gone.
			value("state/old-value", now.Add(-2*time.Hour)),
			deletion("state/gone", now.Add(-2*time.Hour)),
			deletion("state/just-gone", now.Add(-30*time.Minute)),
		} {
			if _, err := lt.Set(r); err != nil {
				t.Fatalf("set %s: %v", r.Topic, err)
			}
		}

		b.sweepByAge(now)

		for topic, want := range map[string]bool{
			"state/fresh":     true,
			"state/old-value": true,  // a value the deletion clock must not touch
			"state/gone":      false, // a deletion past its own period
			"state/just-gone": true,  // a deletion still inside it
		} {
			if got := held(t, lt, topic); got != want {
				t.Errorf("%s held=%v after the sweep, want %v: the two clocks are not "+
					"being read separately", topic, got, want)
			}
		}
	})

	// A channel that keeps its values for ever still expires its deletions,
	// which is the case the second clock exists for: otherwise a copy holds
	// a row for every device ever decommissioned and nothing brings the
	// topic count down.
	t.Run("values for ever, deletions on a clock", func(t *testing.T) {
		b := retentionBroker(t, &channel.Channel{
			Name: "state", Type: channel.Latest, DeletionRetentionPeriod: 3600,
		})
		lt := b.latest["state"]
		if _, err := lt.Set(value("state/ancient", now.Add(-100*time.Hour))); err != nil {
			t.Fatalf("set: %v", err)
		}
		if _, err := lt.Set(deletion("state/gone", now.Add(-2*time.Hour))); err != nil {
			t.Fatalf("set: %v", err)
		}

		b.sweepByAge(now)

		if !held(t, lt, "state/ancient") {
			t.Error("a value was expired on a channel with no retention_period")
		}
		if held(t, lt, "state/gone") {
			t.Error("a deletion outlived its period on a channel keeping values for ever, " +
				"which is the case the second clock exists for")
		}
	})
}

// On a broadcast topic the retained value dies at the earlier of two
// clocks: the publisher's Message Expiry Interval (MQTT-3.3.2-5) and the
// operator's retention_period. The sweep is where both are read, and each
// deletes on its own - an operator who configured no period still loses
// nothing a publisher promised would expire, and a publisher who set no
// expiry still loses nothing before the operator's period.
func TestTheRetainedStoreDiesAtTheEarlierOfTwoClocks(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	value := func(topic string, age time.Duration, expiry uint32) store.Record {
		return store.Record{MessageID: "m-" + topic, Topic: topic,
			Payload: []byte("here"), Timestamp: now.Add(-age), MessageExpiry: expiry}
	}
	held := func(t *testing.T, lt LatestStore, topic string) bool {
		t.Helper()
		_, ok, err := lt.Get(topic)
		if err != nil {
			t.Fatalf("get %s: %v", topic, err)
		}
		return ok
	}

	t.Run("the publisher's expiry deletes with no operator period", func(t *testing.T) {
		b := retentionBroker(t)
		rt := store.NewLatest()
		b.SetRetained("local", rt, 0)
		for _, r := range []store.Record{
			value("house/expired", 2*time.Hour, 3600),
			value("house/running", 30*time.Minute, 3600),
			// No expiry and very old: with no operator period this is kept
			// for ever, which is what "the publisher's countdown, never the
			// operator's period" costs when neither clock was set.
			value("house/forever", 100*time.Hour, 0),
		} {
			if _, err := rt.Set(r); err != nil {
				t.Fatalf("set %s: %v", r.Topic, err)
			}
		}

		b.sweepByAge(now)

		for topic, want := range map[string]bool{
			"house/expired": false,
			"house/running": true,
			"house/forever": true,
		} {
			if got := held(t, rt, topic); got != want {
				t.Errorf("%s held=%v after the sweep, want %v", topic, got, want)
			}
		}
	})

	t.Run("the operator's period deletes first", func(t *testing.T) {
		b := retentionBroker(t)
		rt := store.NewLatest()
		b.SetRetained("local", rt, 3600)
		if _, err := rt.Set(value("house/lamp", 2*time.Hour, 86400)); err != nil {
			t.Fatalf("set: %v", err)
		}

		b.sweepByAge(now)

		if held(t, rt, "house/lamp") {
			t.Error("a day of Message Expiry kept a value past the operator's hour; " +
				"the earlier clock deletes")
		}
	})

	t.Run("the publisher's expiry deletes first", func(t *testing.T) {
		b := retentionBroker(t)
		rt := store.NewLatest()
		b.SetRetained("local", rt, 86400)
		if _, err := rt.Set(value("house/lamp", 2*time.Hour, 3600)); err != nil {
			t.Fatalf("set: %v", err)
		}

		b.sweepByAge(now)

		if held(t, rt, "house/lamp") {
			t.Error("the operator's day kept a value whose publisher promised an hour; " +
				"the earlier clock deletes")
		}
	})
}

// sweepBenchRecords is what each channel holds while the sweeps below run.
//
// **A handful, because the sweep is flat in what a channel holds**: the
// read stops at the first record that has to stay, so a channel with a
// million records and one with eight cost the same when nothing is over the
// bound. What the benchmark varies is the number of channels, which is the
// term nothing had measured.
const sweepBenchRecords = 8

// sweepBenchBroker is n append channels holding records that are all well
// inside their bounds, on the named provider.
//
// **Nothing to remove is the case worth measuring**, not a sweep that
// trims. The size sweep runs every second for the life of the broker and
// almost always finds nothing; a sweep that removes something is bounded by
// what it removes, which is the operator's own retention policy rather than
// a cost the broker imposes on itself.
func sweepBenchBroker(tb testing.TB, provider string, n int) *Broker {
	tb.Helper()

	chans := make([]*channel.Channel, 0, n)
	for i := range n {
		chans = append(chans, &channel.Channel{
			Name: fmt.Sprintf("c%06d", i), Type: channel.Append, Storage: "local",
			// Far above and far beyond what the records below reach, so
			// every pass finds nothing and the figure is the walk itself.
			RetentionBytes: 1 << 30, RetentionPeriod: 86_400,
		})
	}
	br := benchBroker(tb, chans, "")

	if provider == "sqlite" {
		db, err := sqlite.Open(filepath.Join(tb.TempDir(), "saguin.db"), "sweep-bench")
		if err != nil {
			tb.Fatalf("open: %v", err)
		}
		tb.Cleanup(func() { _ = db.Close() })
		logs := make(map[string]LogStore, n)
		for _, c := range chans {
			lg, err := db.Log(c.Name)
			if err != nil {
				tb.Fatalf("log %s: %v", c.Name, err)
			}
			logs[c.Name] = lg
		}
		br.SetStores(Stores{Logs: logs})
	}

	payload := []byte(strings.Repeat("x", 128))
	for _, c := range chans {
		lg := br.logs[c.Name]
		for j := range sweepBenchRecords {
			if _, err := lg.Append(store.Record{
				MessageID: "m", Topic: c.Name + "/x",
				Payload: payload, Offset: uint64(j),
			}); err != nil {
				tb.Fatalf("append: %v", err)
			}
		}
	}
	return br
}

// sweepBenchSizes is how many channels each sweep is measured at, and it is
// the same list for both providers.
//
// **Ten thousand is affordable on sqlite, which was checked rather than
// assumed.** Building that arm opens ten thousand logs in one database and
// takes about three seconds of wall clock and 230MB before the timer
// starts. That is inside what `make bench` can carry at -benchtime 100ms,
// which matters: a benchmark people skip is a benchmark that says nothing.
var sweepBenchSizes = []int{10, 100, 1_000, 10_000}

// BenchmarkRetentionSweepBySize is the cost of one pass of the sweep that
// runs every second, against the number of channels.
//
// **This is the measurement sizeSweepPeriod rests on.** That constant is a
// second rather than something longer because a sweep finding nothing was
// measured cheap per channel, and fifteen channels a second is nothing at
// all. The per-channel figure says nothing about what ten thousand of them
// cost on a one-second ticker, and ten thousand channels is a size this
// broker's configuration is built for - so the number that matters is this
// one against the period, not the one per channel.
func BenchmarkRetentionSweepBySize(b *testing.B) {
	for _, provider := range []string{"memory", "sqlite"} {
		b.Run(provider, func(b *testing.B) {
			for _, n := range sweepBenchSizes {
				b.Run(fmt.Sprintf("%d-channels", n), func(b *testing.B) {
					br := sweepBenchBroker(b, provider, n)
					b.ResetTimer()
					for range b.N {
						br.sweepBySize()
					}
				})
			}
		})
	}
}

// BenchmarkRetentionSweepByAge is the same walk on the other clock. Its
// period is derived from the shortest retention any channel configured, so
// it is usually far longer than the size sweep's second - but it is the
// same walk over the same channels, and a deployment whose shortest
// retention is ten seconds runs it every one.
func BenchmarkRetentionSweepByAge(b *testing.B) {
	now := time.Now()
	for _, provider := range []string{"memory", "sqlite"} {
		b.Run(provider, func(b *testing.B) {
			for _, n := range sweepBenchSizes {
				b.Run(fmt.Sprintf("%d-channels", n), func(b *testing.B) {
					br := sweepBenchBroker(b, provider, n)
					b.ResetTimer()
					for range b.N {
						br.sweepByAge(now)
					}
				})
			}
		})
	}
}
