package sqlite

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// An exactly-once publish held in the store of the channel it is for
// (store.HeldPublish). The oracles are RFC 0003 "Exactly once, and where the
// unfinished ones wait" and RFC 0004's log rules: a hold has no offset and
// nothing that reads the channel sees it; its release is one operation that
// gives it the channel's next offset and forgets it, with never both and
// never neither; the provider's bound counts it from the moment it is held,
// and never refuses its release; the channel's own bound can, and then the
// hold stays. Run against both providers.

// holder is what the hold asks of a channel's store, on either provider.
type holder interface {
	Hold(store.Exchange, store.Record, time.Time) error
	ReleaseHold(store.Exchange) (store.Record, bool, error)
	DropHold(store.Exchange) (bool, error)
	Holds() ([]store.HeldPublish, error)
}

// heldLog is an append channel that can hold.
type heldLog interface {
	holder
	Append(store.Record) (store.Record, error)
	ReadFrom(uint64) ([]store.Record, error)
	Next() uint64
	Floor() uint64
	Bytes() int64
	SetMaxBytes(int64)
	Trim(time.Time, int64) (int, int64, error)
}

var (
	_ heldLog = (*Log)(nil)
	_ heldLog = (*store.Log)(nil)
	_ holder  = (*Latest)(nil)
	_ holder  = (*store.Latest)(nil)
	_ holder  = (*Queue)(nil)
	_ holder  = (*store.Queue)(nil)
)

// bothHeldLogs runs fn against an append channel on each provider, with the
// provider bounded at room bytes where room is not zero.
func bothHeldLogs(t *testing.T, room int64, fn func(t *testing.T, lg heldLog)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		lg := store.NewLog()
		if room > 0 {
			lg.SetQuota(store.NewQuota(room, 0))
		}
		fn(t, lg)
	})
	t.Run("sqlite", func(t *testing.T) {
		db, err := OpenBounded(tempPath(t), "test", room, 0)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		lg, err := db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}
		fn(t, lg)
	})
}

func heldRecord(payload string) store.Record {
	return store.Record{MessageID: "m-" + payload, Topic: "events/x", Payload: []byte(payload),
		Timestamp: time.Unix(1770000000, 0)}
}

var ex = store.Exchange{Client: "pub", PacketID: 7}

// **Nothing that reads the channel sees a hold, and its release is the
// channel's next record.** Held: no record, the next offset where it was,
// the channel's size unchanged, and a retention sweep leaves it alone. A
// repeat of the PUBLISH changes nothing. Released: the record at the next
// offset, readable, the hold gone. Released again: nothing held.
func TestAHeldPublishIsNotReadUntilItsRelease(t *testing.T) {
	bothHeldLogs(t, 0, func(t *testing.T, lg heldLog) {
		r := heldRecord("held")
		now := time.Unix(1770000000, 0)
		// An offset a caller left on the record is not the hold's: it has
		// none until its release.
		stamped := r
		stamped.Offset = 99
		if err := lg.Hold(ex, stamped, now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		if err := lg.Hold(ex, heldRecord("a second copy"), now.Add(time.Second)); err != nil {
			t.Fatalf("a repeat of the PUBLISH: %v", err)
		}
		if got, _ := lg.ReadFrom(lg.Floor()); len(got) != 0 {
			t.Errorf("a reader was served %d records while the publish was only held", len(got))
		}
		if lg.Next() != 1 || lg.Bytes() != 0 {
			t.Errorf("held: next %d and %d bytes, want 1 and 0: a hold takes no offset and no channel room",
				lg.Next(), lg.Bytes())
		}
		if removed, _, err := lg.Trim(now.Add(time.Hour), 0); err != nil || removed != 0 {
			t.Errorf("a retention sweep removed %d (%v) with only a hold in the channel", removed, err)
		}
		held, err := lg.Holds()
		if err != nil || len(held) != 1 || held[0].Exchange != ex || string(held[0].Record.Payload) != "held" ||
			!held[0].HeldAt.Equal(now) || held[0].Record.Offset != 0 {
			t.Fatalf("holds are %+v (%v), want the first copy of the one exchange, with no offset", held, err)
		}

		got, ok, err := lg.ReleaseHold(ex)
		if err != nil || !ok || got.Offset != 1 {
			t.Fatalf("release: %+v, %v, %v, want the record at offset 1", got, ok, err)
		}
		recs, err := lg.ReadFrom(lg.Floor())
		if err != nil || len(recs) != 1 || recs[0].Offset != 1 || string(recs[0].Payload) != "held" {
			t.Fatalf("after the release the channel holds %+v (%v), want the one record", recs, err)
		}
		if lg.Next() != 2 || lg.Bytes() != store.RecordSize(r) {
			t.Errorf("released: next %d and %d bytes, want 2 and %d", lg.Next(), lg.Bytes(), store.RecordSize(r))
		}
		if held, _ := lg.Holds(); len(held) != 0 {
			t.Errorf("the hold is still there after its release: %+v", held)
		}
		if _, ok, err := lg.ReleaseHold(ex); ok || err != nil {
			t.Errorf("a second release answered %v, %v, want nothing held", ok, err)
		}
	})
}

// **The channel's own bound refuses a release, and the hold stays.** A hold
// is taken while the channel has room, another publish fills it, and the
// release is refused ErrFull - held, no offset taken, the hold still there.
// Once retention makes room, the same release stores it. A hold asked for
// while the channel is already full is refused as a publish would be.
func TestTheChannelsOwnBoundRefusesAReleaseAndKeepsTheHold(t *testing.T) {
	bothHeldLogs(t, 0, func(t *testing.T, lg heldLog) {
		filler := heldRecord(string(bytes.Repeat([]byte("f"), 3000)))
		// Room for two fillers, or one and the held publish, and not for
		// all three.
		lg.SetMaxBytes(2*store.RecordSize(filler) + store.RecordSize(heldRecord("held")) - 1)
		if _, err := lg.Append(filler); err != nil {
			t.Fatalf("append: %v", err)
		}
		now := time.Unix(1770000000, 0)
		if err := lg.Hold(ex, heldRecord("held"), now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		if _, err := lg.Append(filler); err != nil {
			t.Fatalf("the second filler: %v", err)
		}
		next := lg.Next()

		_, ok, err := lg.ReleaseHold(ex)
		if !errors.Is(err, store.ErrFull) || errors.Is(err, store.ErrProviderFull) || !ok {
			t.Fatalf("the release into a full channel answered %v (held %v), want ErrFull and held", err, ok)
		}
		if lg.Next() != next {
			t.Errorf("the refused release moved next from %d to %d", next, lg.Next())
		}
		if held, _ := lg.Holds(); len(held) != 1 {
			t.Fatalf("the refused release took the hold: %+v", held)
		}
		other := store.Exchange{Client: "pub", PacketID: 8}
		if err := lg.Hold(other, heldRecord("late"), now); !errors.Is(err, store.ErrFull) {
			t.Errorf("a hold into a full channel answered %v, want ErrFull", err)
		}

		if removed, _, err := lg.Trim(time.Time{}, store.RecordSize(filler)); err != nil || removed != 1 {
			t.Fatalf("trim made room of %d records (%v)", removed, err)
		}
		got, ok, err := lg.ReleaseHold(ex)
		if err != nil || !ok || got.Offset != next {
			t.Fatalf("the release once there was room: %+v, %v, %v, want offset %d", got, ok, err, next)
		}
	})
}

// **A hold takes the provider's room when it is held, and its release takes
// no more.** On memory the provider counts it to the byte: the hold's size
// while held, the same after its release, nothing after a hold is dropped.
// On sqlite the page ceiling refuses a hold with store.ErrProviderFull, and a
// release still goes through a provider another channel has filled since.
func TestAHoldTakesTheProvidersRoomAndItsReleaseTakesNoMore(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		q := store.NewQuota(64<<10, 0)
		lg := store.NewLog()
		lg.SetQuota(q)
		r := heldRecord("counted")
		now := time.Unix(1770000000, 0)
		if err := lg.Hold(ex, r, now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		if q.Bytes() != store.RecordSize(r) {
			t.Errorf("held: the provider counts %d, want %d", q.Bytes(), store.RecordSize(r))
		}
		if _, _, err := lg.ReleaseHold(ex); err != nil {
			t.Fatalf("release: %v", err)
		}
		if q.Bytes() != store.RecordSize(r) || lg.Bytes() != store.RecordSize(r) {
			t.Errorf("released: the provider counts %d and the channel %d, want %d each", q.Bytes(), lg.Bytes(),
				store.RecordSize(r))
		}
		other := store.Exchange{Client: "pub", PacketID: 8}
		if err := lg.Hold(other, r, now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		if gone, err := lg.DropHold(other); !gone || err != nil {
			t.Fatalf("drop: %v, %v", gone, err)
		}
		if q.Bytes() != store.RecordSize(r) {
			t.Errorf("after a dropped hold the provider counts %d, want %d", q.Bytes(), store.RecordSize(r))
		}
		big := heldRecord(string(bytes.Repeat([]byte("b"), 70<<10)))
		if err := lg.Hold(store.Exchange{Client: "pub", PacketID: 9}, big, now); !errors.Is(err, store.ErrProviderFull) {
			t.Errorf("a hold past the provider's bound answered %v, want ErrProviderFull", err)
		}
	})
	t.Run("sqlite", func(t *testing.T) {
		// A reserve, as every bounded provider has, and small records, so
		// the pages are full of rows: the release's insert then needs a page
		// the publish ceiling has no room for, which only the reserve gives.
		db, err := OpenBounded(tempPath(t), "test", store.SQLiteEmptyBytes+128<<10, 32<<10)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		lg, err := db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}
		now := time.Unix(1770000000, 0)
		held := 0
		for i := 1; i <= 40; i++ {
			e := store.Exchange{Client: "pub", PacketID: uint16(i)}
			if err := lg.Hold(e, heldRecord(fmt.Sprintf("held-%03d-%s", i, bytes.Repeat([]byte("h"), 150))), now); err != nil {
				t.Fatalf("hold %d: %v", i, err)
			}
			held++
		}
		chunk := heldRecord(string(bytes.Repeat([]byte("f"), 200)))
		filled := 0
		for range 5000 {
			if _, err = lg.Append(chunk); err != nil {
				break
			}
			filled++
		}
		if !errors.Is(err, store.ErrProviderFull) || filled == 0 {
			t.Fatalf("filling stopped after %d with %v, want the page ceiling", filled, err)
		}
		if err := lg.Hold(store.Exchange{Client: "late", PacketID: 1}, chunk, now); !errors.Is(err, store.ErrProviderFull) {
			t.Errorf("a hold at the page ceiling answered %v, want ErrProviderFull", err)
		}
		for i := 1; i <= held; i++ {
			e := store.Exchange{Client: "pub", PacketID: uint16(i)}
			if _, ok, err := lg.ReleaseHold(e); err != nil || !ok {
				t.Fatalf("release %d of %d into the full provider answered %v, %v: the hold's room is the "+
					"record's, and the page ceiling never refuses the swap", i, held, ok, err)
			}
		}
	})
}

// **A hold outlives a restart as its channel does, and is released after
// it.** sqlite: the file is closed and opened again. memory: the channel is
// written to a snapshot and read back, which keeps the hold with its
// exchange and the moment it was held.
func TestAHoldOutlivesARestart(t *testing.T) {
	now := time.Unix(1770000000, 0)
	t.Run("sqlite", func(t *testing.T) {
		path := tempPath(t)
		db, err := Open(path, "0.1.0-test")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		lg, _ := db.Log("events")
		if err := lg.Hold(ex, heldRecord("kept"), now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		db = open(t, path)
		lg, err = db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}
		held, err := lg.Holds()
		if err != nil || len(held) != 1 || held[0].Exchange != ex || !held[0].HeldAt.Equal(now) {
			t.Fatalf("after the restart the holds are %+v (%v)", held, err)
		}
		if got, ok, err := lg.ReleaseHold(ex); err != nil || !ok || string(got.Payload) != "kept" || got.Offset != 1 {
			t.Fatalf("release after the restart: %+v, %v, %v", got, ok, err)
		}
	})
	t.Run("memory", func(t *testing.T) {
		lg := store.NewLog()
		if err := lg.Hold(ex, heldRecord("kept"), now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		next, floor, records := lg.Export()
		holds, _ := lg.Holds()
		var buf bytes.Buffer
		snap := &store.Snapshot{Writer: "test", WrittenAt: now, Channels: []store.ChannelState{{
			Name: "events", Kind: store.KindAppend, Next: next, Floor: floor, Records: records, Holds: holds}}}
		if err := snap.Encode(&buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		back, err := store.DecodeSnapshot(buf.Bytes())
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		c := back.Channels[0]
		restored := store.RestoreLog(c.Next, c.Floor, c.Records, c.Positions)
		restored.RestoreHeld(c.Holds)
		q := store.NewQuota(1<<20, 0)
		restored.SetQuota(q)
		if q.Bytes() != store.RecordSize(heldRecord("kept")) {
			t.Errorf("the restored hold is charged %d to the provider, want %d", q.Bytes(),
				store.RecordSize(heldRecord("kept")))
		}
		held, _ := restored.Holds()
		if len(held) != 1 || held[0].Exchange != ex || !held[0].HeldAt.Equal(now) {
			t.Fatalf("after the snapshot the holds are %+v", held)
		}
		if got, ok, err := restored.ReleaseHold(ex); err != nil || !ok || string(got.Payload) != "kept" || got.Offset != 1 {
			t.Fatalf("release after the snapshot: %+v, %v, %v", got, ok, err)
		}
	})
}

// **One identifier from two clients is two holds.** MQTT gives each session
// its own packet identifiers (section 2.2.1), so a hold is keyed by client as
// well as identifier: holding, releasing or dropping one client's never
// touches the other's.
func TestOneIdentifierFromTwoClientsIsTwoHolds(t *testing.T) {
	bothHeldLogs(t, 0, func(t *testing.T, lg heldLog) {
		now := time.Unix(1770000000, 0)
		a, b := store.Exchange{Client: "a", PacketID: 7}, store.Exchange{Client: "b", PacketID: 7}
		if err := lg.Hold(a, heldRecord("from-a"), now); err != nil {
			t.Fatalf("hold a: %v", err)
		}
		if err := lg.Hold(b, heldRecord("from-b"), now); err != nil {
			t.Fatalf("hold b: %v", err)
		}
		if held, err := lg.Holds(); err != nil || len(held) != 2 {
			t.Fatalf("two clients' identifier 7 is %d hold(s) (%v), want 2", len(held), err)
		}
		got, ok, err := lg.ReleaseHold(a)
		if err != nil || !ok || string(got.Payload) != "from-a" {
			t.Fatalf("releasing a's 7 gave %q (%v, %v), want a's message", got.Payload, ok, err)
		}
		if gone, err := lg.DropHold(a); err != nil || gone {
			t.Errorf("a's 7 was still there to drop after its release (%v, %v)", gone, err)
		}
		held, err := lg.Holds()
		if err != nil || len(held) != 1 || held[0].Exchange != b || string(held[0].Record.Payload) != "from-b" {
			t.Fatalf("after a's release the holds are %+v (%v), want b's 7 alone", held, err)
		}
	})
}

// **A hold for a channel the configuration no longer keeps here is deleted
// at the start, and counted**: nothing can release it, and it would take the
// provider's room for good (RFC 0003 "Exactly once, and where the unfinished
// ones wait"). What a kept channel holds stays, and a channel named but
// holding nothing is no error. Asked again, there is nothing left to delete.
func TestAHoldForAChannelNoLongerConfiguredIsDroppedAtTheStart(t *testing.T) {
	now := time.Unix(1770000000, 0)
	path := tempPath(t)
	db := open(t, path)
	for _, h := range []struct {
		channel string
		e       store.Exchange
	}{
		{"events", ex},
		{"gone", store.Exchange{Client: "pub", PacketID: 1}},
		{"gone", store.Exchange{Client: "other", PacketID: 1}},
		{"moved", store.Exchange{Client: "pub", PacketID: 2}},
	} {
		lg, err := db.Log(h.channel)
		if err != nil {
			t.Fatalf("log %s: %v", h.channel, err)
		}
		if err := lg.Hold(h.e, heldRecord(h.channel), now); err != nil {
			t.Fatalf("hold in %s: %v", h.channel, err)
		}
	}
	n, err := db.DropHeldExcept([]string{"events", "configured-but-empty"})
	if err != nil || n != 3 {
		t.Fatalf("DropHeldExcept deleted %d (%v), want the 3 held for the two channels gone", n, err)
	}
	for channel, want := range map[string]int{"events": 1, "gone": 0, "moved": 0} {
		lg, _ := db.Log(channel)
		if held, err := lg.Holds(); err != nil || len(held) != want {
			t.Errorf("channel %s holds %d (%v), want %d", channel, len(held), err, want)
		}
	}
	if n, err := db.DropHeldExcept([]string{"events"}); err != nil || n != 0 {
		t.Errorf("asked again it deleted %d (%v), want 0", n, err)
	}
	if n, err := db.DropHeldExcept(nil); err != nil || n != 1 {
		t.Errorf("with no channel kept it deleted %d (%v), want the 1 left", n, err)
	}
}

// A snapshot whose hold carries an offset, or names no exchange, or holds one
// exchange twice, is refused: each is a state no running broker writes, and
// a hold with an offset would be a record readers could not see.
func TestASnapshotWithAnImpossibleHoldIsRefused(t *testing.T) {
	now := time.Unix(1770000000, 0)
	for _, tc := range []struct {
		name  string
		holds []store.HeldPublish
	}{
		{"an offset", []store.HeldPublish{{Exchange: ex, Record: store.Record{Offset: 3, MessageID: "m", Topic: "t"}, HeldAt: now}}},
		{"no client", []store.HeldPublish{{Exchange: store.Exchange{PacketID: 1}, Record: heldRecord("x"), HeldAt: now}}},
		{"twice", []store.HeldPublish{{Exchange: ex, Record: heldRecord("x"), HeldAt: now}, {Exchange: ex, Record: heldRecord("y"), HeldAt: now}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			snap := &store.Snapshot{Writer: "test", WrittenAt: now, Channels: []store.ChannelState{{
				Name: "events", Kind: store.KindAppend, Next: 1, Floor: 1, Holds: tc.holds}}}
			if err := snap.Encode(&buf); err != nil {
				t.Fatalf("encode: %v", err)
			}
			if _, err := store.DecodeSnapshot(buf.Bytes()); err == nil {
				t.Fatal("the snapshot was accepted")
			}
		})
	}
}

// **The swap is one transaction: never both, never neither.** Two ways it
// can fail inside, and each leaves the hold and no record, with the next
// offset where it was: the record's row already taken (the insert fails),
// and the hold gone meanwhile (a session ending), which stores nothing and
// answers nothing held.
func TestASwapThatFailsLeavesTheHoldAndNoRecord(t *testing.T) {
	now := time.Unix(1770000000, 0)
	t.Run("the insert fails", func(t *testing.T) {
		db := open(t, tempPath(t))
		lg, _ := db.Log("events")
		if err := lg.Hold(ex, heldRecord("held"), now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		// A row already at the offset the release will take, as a store
		// whose counter fell behind would have.
		if _, err := db.db.Exec(`INSERT INTO records (channel, "offset", message_id, topic, payload, ts)
			VALUES ('events', 1, 'squatter', 'events/x', X'00', 0)`); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, ok, err := lg.ReleaseHold(ex); err == nil || !ok {
			t.Fatalf("the release over a taken row answered %v, %v, want a failure with the exchange held", ok, err)
		}
		if lg.Next() != 1 {
			t.Errorf("the failed swap left next at %d, want 1", lg.Next())
		}
		if held, _ := lg.Holds(); len(held) != 1 {
			t.Errorf("the failed swap took the hold: %+v", held)
		}
		var n int
		_ = db.db.QueryRow(`SELECT count(*) FROM records WHERE channel = 'events' AND message_id = 'm-held'`).Scan(&n)
		if n != 0 {
			t.Errorf("the failed swap stored the record %d times", n)
		}
	})
	t.Run("the hold went meanwhile", func(t *testing.T) {
		db := open(t, tempPath(t))
		lg, _ := db.Log("events")
		if err := lg.Hold(ex, heldRecord("held"), now); err != nil {
			t.Fatalf("hold: %v", err)
		}
		r, _, _ := db.heldOne("events", ex)
		p, _, err := lg.publishOf(r)
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		if gone, err := lg.DropHold(ex); !gone || err != nil {
			t.Fatalf("drop: %v, %v", gone, err)
		}
		if err := db.swap(p, "events", ex); !errors.Is(err, errNotHeld) {
			t.Fatalf("a swap of a hold that went answered %v, want errNotHeld", err)
		}
		if lg.Next() != 1 {
			t.Errorf("the swap left next at %d, want 1", lg.Next())
		}
		if recs, _ := lg.ReadFrom(1); len(recs) != 0 {
			t.Errorf("the swap stored %d records for an exchange nobody held", len(recs))
		}
	})
}

// A latest channel and a queue hold and release as an append channel does:
// the release is the topic's value, replacing the one before it, and a
// deletion of a topic with no live value stores nothing; a queue's release
// is work a worker is offered.
func TestALatestChannelAndAQueueHoldAndRelease(t *testing.T) {
	now := time.Unix(1770000000, 0)
	type latest interface {
		holder
		Set(store.Record) (store.Record, error)
		Get(string) (store.Record, bool, error)
	}
	type queue interface {
		holder
		Offer(int, time.Time) ([]store.Offered, error)
	}
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			var lt latest
			var qu queue
			if provider == "memory" {
				lt, qu = store.NewLatest(), store.NewQueue()
			} else {
				db := open(t, tempPath(t))
				l, err := db.Latest("state")
				if err != nil {
					t.Fatalf("latest: %v", err)
				}
				q, err := db.Queue("jobs")
				if err != nil {
					t.Fatalf("queue: %v", err)
				}
				lt, qu = l, q
			}

			if _, err := lt.Set(store.Record{MessageID: "v1", Topic: "s/a", Payload: []byte("one"), Timestamp: now}); err != nil {
				t.Fatalf("set: %v", err)
			}
			if err := lt.Hold(ex, store.Record{MessageID: "v2", Topic: "s/a", Payload: []byte("two"), Timestamp: now}, now); err != nil {
				t.Fatalf("hold: %v", err)
			}
			if cur, _, _ := lt.Get("s/a"); string(cur.Payload) != "one" {
				t.Errorf("the topic's value is %q while the new one is only held", cur.Payload)
			}
			if got, ok, err := lt.ReleaseHold(ex); err != nil || !ok || got.Offset == 0 {
				t.Fatalf("release: %+v, %v, %v", got, ok, err)
			}
			if cur, _, _ := lt.Get("s/a"); string(cur.Payload) != "two" {
				t.Errorf("after the release the topic's value is %q, want two", cur.Payload)
			}
			del := store.Exchange{Client: "pub", PacketID: 8}
			if err := lt.Hold(del, store.Record{MessageID: "d", Topic: "s/none", Timestamp: now}, now); err != nil {
				t.Fatalf("hold a deletion: %v", err)
			}
			if got, ok, err := lt.ReleaseHold(del); err != nil || !ok || got.Offset != 0 {
				t.Errorf("releasing a deletion of a topic with no value answered %+v, %v, %v, want no offset", got, ok, err)
			}
			if _, found, _ := lt.Get("s/none"); found {
				t.Error("a deletion of a topic with no value was stored")
			}
			if held, _ := lt.Holds(); len(held) != 0 {
				t.Errorf("holds left behind: %+v", held)
			}

			if err := qu.Hold(ex, store.Record{MessageID: "j", Topic: "jobs/x", Payload: []byte("job"), Timestamp: now}, now); err != nil {
				t.Fatalf("hold a job: %v", err)
			}
			if offered, _ := qu.Offer(10, now); len(offered) != 0 {
				t.Errorf("a worker was offered %d jobs while the only one was held", len(offered))
			}
			if _, ok, err := qu.ReleaseHold(ex); err != nil || !ok {
				t.Fatalf("release the job: %v, %v", ok, err)
			}
			offered, err := qu.Offer(10, now)
			if err != nil || len(offered) != 1 || string(offered[0].Record.Payload) != "job" {
				t.Fatalf("after the release a worker was offered %+v (%v)", offered, err)
			}
		})
	}
}
