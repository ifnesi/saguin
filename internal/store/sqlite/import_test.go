package sqlite

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// A channel that retention has trimmed is the case the whole import is
// about. Its records start at 5, its floor is 5, and its next is 8 - none
// of which can be worked out from the three records that survive.
//
// An import that assigned offsets the ordinary way would give 1, 2, 3 and a
// next of 4, and every one of those numbers would be wrong in the way
// invariant 9 names: a stored consumer position would point at a record it
// never saw, and the channel would hand out 5, 6 and 7 a second time.
func trimmed() store.ChannelState {
	at := time.Unix(1770000000, 0)
	return store.ChannelState{
		Name: "events", Kind: store.KindAppend, Next: 8, Floor: 5,
		Records: []store.Record{
			{Offset: 5, MessageID: "m5", Topic: "events/a", Payload: []byte("five"), Timestamp: at},
			{Offset: 6, MessageID: "m6", Topic: "events/b", Payload: []byte("six"),
				Headers: []store.Header{{Key: "k", Value: "v"}}, Timestamp: at},
			{Offset: 7, MessageID: "m7", Topic: "events/c", Payload: []byte("seven"), Timestamp: at},
		},
		Positions: []store.Position{
			{Reader: "mqtt:live", Offset: 6, LastSeen: at, ExpiresIn: time.Hour},
		},
	}
}

func TestImportKeepsTheOffsetsItWasGiven(t *testing.T) {
	db := open(t, tempPath(t))
	if err := db.Import([]store.ChannelState{trimmed()}); err != nil {
		t.Fatalf("import: %v", err)
	}

	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if got := lg.Next(); got != 8 {
		t.Errorf("next = %d, want 8: the counter came from the records rather than from what was stored", got)
	}
	if got := lg.Floor(); got != 5 {
		t.Errorf("floor = %d, want 5: an emptied channel would report itself caught up", got)
	}

	// Below the floor is refused, not served from the oldest survivor.
	if _, err := lg.ReadFrom(4); err != store.ErrBelowFloor {
		t.Errorf("reading from 4 gave %v, want %v", err, store.ErrBelowFloor)
	}

	got, err := lg.ReadFrom(5)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := trimmed().Records
	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].Offset != want[i].Offset || got[i].MessageID != want[i].MessageID ||
			got[i].Topic != want[i].Topic || string(got[i].Payload) != string(want[i].Payload) {
			t.Errorf("record %d came back as %+v, want %+v", i, got[i], want[i])
		}
		if !got[i].Timestamp.Equal(want[i].Timestamp) {
			t.Errorf("record %d timestamp %v, want %v", i, got[i].Timestamp, want[i].Timestamp)
		}
	}
	if v, ok := got[1].Header("k"); !ok || v != "v" {
		t.Errorf("headers came back as %v, want k=v", got[1].Headers)
	}

	// The next record appended carries on rather than colliding.
	r, err := lg.Append(store.Record{MessageID: "m8", Topic: "events/d"})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if r.Offset != 8 {
		t.Errorf("the record after the import took offset %d, want 8", r.Offset)
	}
}

// A position that does not survive the move is a consumer that silently
// starts again from the beginning, which is the failure carrying them
// across exists to prevent.
func TestImportCarriesConsumerPositions(t *testing.T) {
	db := open(t, tempPath(t))
	if err := db.Import([]store.ChannelState{trimmed()}); err != nil {
		t.Fatalf("import: %v", err)
	}
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	p, ok, err := lg.Position("mqtt:live")
	if err != nil {
		t.Fatalf("position: %v", err)
	}
	if !ok {
		t.Fatal("the consumer has no stored position: it would resume from the floor and replay")
	}
	if p.Offset != 6 {
		t.Errorf("position = %d, want 6", p.Offset)
	}
	if p.ExpiresIn != time.Hour {
		t.Errorf("expiry = %v, want 1h: a position with no expiry left is dropped at the next open", p.ExpiresIn)
	}
}

// RFC 0004 "A position lives with the records it points into"
//
// **Import stores a reader name exactly as it was handed one.** Every name in
// a file this broker reads carries its scheme, and prefixing one again would
// take a bridge's `bridge:head-office/events` to
// `mqtt:bridge:head-office/events` - a name the bridge never looks up, so it
// would copy its channel from the beginning.
func TestImportStoresTheReaderNameItWasGiven(t *testing.T) {
	db := open(t, tempPath(t))
	cs := trimmed()
	cs.Positions = []store.Position{
		{Reader: store.MQTTReader("sensor"), Offset: 7,
			LastSeen: time.Unix(1770000000, 0), ExpiresIn: time.Hour},
		// No session behind it, so no expiry interval to give.
		{Reader: "bridge:head-office/events", Offset: 8,
			LastSeen: time.Unix(1770000000, 0)},
	}
	if err := db.Import([]store.ChannelState{cs}); err != nil {
		t.Fatalf("import: %v", err)
	}
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	for _, reader := range []string{store.MQTTReader("sensor"), "bridge:head-office/events"} {
		if _, ok, _ := lg.Position(reader); !ok {
			t.Errorf("no position under %q after the import: that reader starts "+
				"again from the beginning of its channel", reader)
		}
	}
	if _, ok, _ := lg.Position(store.MQTTReader("bridge:head-office/events")); ok {
		t.Error("the bridge's position was stored under a second scheme, so the " +
			"bridge looks up its own name and finds nothing")
	}
}

// A queue and its dead-letter channel are in one snapshot file so that the
// move between them cannot be recorded by half (invariant 5). They arrive
// in one transaction for the same reason.
func TestImportCarriesAQueueWithItsDeadLetterChannel(t *testing.T) {
	at := time.Unix(1770000000, 0)
	db := open(t, tempPath(t))

	err := db.Import([]store.ChannelState{
		{
			Name: "jobs", Kind: store.KindQueue, Next: 4, Floor: 1,
			Items: []store.Item{
				{Record: store.Record{Offset: 2, MessageID: "j2", Topic: "jobs/x", Timestamp: at},
					Attempts: 2, FirstSeen: at, LastSeen: at},
				{Record: store.Record{Offset: 3, MessageID: "j3", Topic: "jobs/y", Timestamp: at}},
			},
		},
		{
			Name: "jobs__dlq", Kind: store.KindAppend, Next: 2, Floor: 1,
			Records: []store.Record{{Offset: 1, MessageID: "j1", Topic: "jobs/w", Timestamp: at}},
		},
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	q, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	total, inflight := q.Depth()
	if total != 2 || inflight != 0 {
		t.Errorf("depth = %d total, %d in flight; want 2 and 0 - no record is in flight after a restart", total, inflight)
	}
	if got := q.Next(); got != 4 {
		t.Errorf("next = %d, want 4", got)
	}

	// The attempt count is what survives, so a record that has spent its
	// attempts is dead-lettered rather than starting over.
	offered, err := q.Offer(10, time.Now())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if len(offered) != 2 {
		t.Fatalf("offered %d records, want 2", len(offered))
	}
	byOffset := map[uint64]store.Offered{}
	for _, o := range offered {
		byOffset[o.Offset] = o
	}
	if got := byOffset[2].Attempt; got != 3 {
		t.Errorf("the record with two attempts is offered as attempt %d, want 3", got)
	}
	if got := byOffset[3].Attempt; got != 1 {
		t.Errorf("the untried record is offered as attempt %d, want 1", got)
	}
	if byOffset[2].DeliveryID == "" || byOffset[2].DeliveryID == byOffset[3].DeliveryID {
		t.Error("each delivery needs a fresh Delivery ID")
	}

	dlq, err := db.Log("jobs__dlq")
	if err != nil {
		t.Fatalf("dlq: %v", err)
	}
	if n, err := dlq.Len(); err != nil || n != 1 {
		t.Errorf("the dead-letter channel holds %d records (%v), want 1", n, err)
	}
}

func TestImportKeepsALatestChannelKeyedByTopic(t *testing.T) {
	at := time.Unix(1770000000, 0)
	db := open(t, tempPath(t))

	err := db.Import([]store.ChannelState{{
		Name: "state", Kind: store.KindLatest, Next: 99, Floor: 1,
		Records: []store.Record{
			{Offset: 91, MessageID: "s1", Topic: "state/a", Payload: []byte("A"), Timestamp: at},
			{Offset: 97, MessageID: "s2", Topic: "state/b", Payload: []byte("B"), Timestamp: at},
		},
	}})
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if got := lt.Next(); got != 99 {
		t.Errorf("next = %d, want 99", got)
	}
	got, err := lt.Match(func(string) bool { return true })
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(got) != 2 || got[0].Offset != 91 || got[1].Offset != 97 {
		t.Fatalf("current state came back as %+v, want offsets 91 and 97 in that order", got)
	}
	if string(got[0].Payload) != "A" || got[0].Topic != "state/a" {
		t.Errorf("the value for state/a came back as %+v", got[0])
	}
}

// The rule that stops two histories ending up under one name.
func TestImportRefusesAChannelAlreadyThere(t *testing.T) {
	db := open(t, tempPath(t))
	if err := db.Import([]store.ChannelState{trimmed()}); err != nil {
		t.Fatalf("first import: %v", err)
	}

	err := db.Import([]store.ChannelState{trimmed()})
	if err == nil {
		t.Fatal("a second import of the same channel was accepted")
	}
	if !strings.Contains(err.Error(), "events") {
		t.Errorf("the error does not name the channel: %v", err)
	}

	// And it changed nothing: one transaction, so the refusal took the whole
	// import with it.
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if n, err := lg.Len(); err != nil || n != 3 {
		t.Errorf("the channel holds %d records (%v), want the original 3", n, err)
	}
}

// Nothing is imported into a database that is being served. A running
// broker always has a store open on every channel it holds, so this is what
// stops the one write that does not assign an offset from reaching a live
// database.
func TestImportRefusesADatabaseWithChannelsOpen(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatalf("log: %v", err)
	}

	err := db.Import([]store.ChannelState{trimmed()})
	if err == nil {
		t.Fatal("an import into a database with an open channel was accepted")
	}
	if !strings.Contains(err.Error(), "served rather than migrated") {
		t.Errorf("the error does not say why: %v", err)
	}
}

// One transaction for everything handed over in a call, so a snapshot that
// cannot be imported leaves nothing of itself behind. The migration passes
// one file at a time and carries on past a failure, and a queue's file
// holds its dead-letter channel too - so without this, a refusal partway
// through such a file would land the queue and not the dead-letter channel,
// which is the half-recorded move invariant 5 forbids.
func TestAFailedImportLeavesNothing(t *testing.T) {
	db := open(t, tempPath(t))

	good := trimmed()
	bad := store.ChannelState{Name: "state", Kind: store.KindLatest, Next: 2, Floor: 1,
		Positions: []store.Position{{Reader: "mqtt:x", Offset: 1}}}

	if err := db.Import([]store.ChannelState{good, bad}); err == nil {
		t.Fatal("a latest channel carrying a consumer position was accepted")
	}

	var channels int
	if err := db.db.QueryRow(`SELECT count(*) FROM channels`).Scan(&channels); err != nil {
		t.Fatalf("count: %v", err)
	}
	if channels != 0 {
		t.Errorf("%d channel row(s) survived a failed import, want 0", channels)
	}
	var records int
	if err := db.db.QueryRow(`SELECT count(*) FROM records`).Scan(&records); err != nil {
		t.Fatalf("count: %v", err)
	}
	if records != 0 {
		t.Errorf("%d record(s) survived a failed import, want 0", records)
	}
}

// RFC 0004 "A position lives with the records it points into"
//
// **A reader with no MQTT session keeps its place across a restart, in both
// stores.** This was a defect once, and it was measured rather than
// reasoned: a position saved at an offset, closed, and reopened was not
// found in either store, so an outbound bridge would copy its whole channel
// again on every restart of the broker.
//
// Two separate causes, one per store and both fixed here, which is why this
// asks the same question of each rather than trusting one to speak for the
// other. The snapshot renamed it - every name went through a migration that
// knew one scheme - and both stores expired it, because a position goes when
// `last_seen + expires_in` is past and a reader with no session has no
// interval to give. Zero read as "expired the moment it was written".
func TestAReaderWithNoSessionKeepsItsPlaceAcrossARestart(t *testing.T) {
	const bridge = "bridge:head-office/events"
	// Long enough ago that any session-shaped rule would have swept it.
	saved := time.Now().Add(-30 * 24 * time.Hour)

	t.Run("memory", func(t *testing.T) {
		lg := store.NewLog()
		for range 2 {
			if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x"}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		// ExpiresIn is deliberately absent: a bridge has no session, so
		// there is no interval for it to state.
		if err := lg.SavePosition(store.Position{Reader: bridge, Offset: 2, LastSeen: saved}); err != nil {
			t.Fatalf("save: %v", err)
		}

		next, floor, records := lg.Export()
		s := &store.Snapshot{Writer: "0.1.0-test", WrittenAt: time.Now(),
			Channels: []store.ChannelState{{
				Name: "events", Kind: store.KindAppend, Next: next, Floor: floor,
				Records: records, Positions: lg.Positions(),
			}}}
		var buf bytes.Buffer
		if err := s.Encode(&buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		back, err := store.DecodeSnapshot(buf.Bytes())
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		// The sweep a starting broker runs over what it loaded.
		back.DropExpiredPositions(time.Now())

		c := back.Channels[0]
		restored := store.RestoreLog(c.Next, c.Floor, c.Records, c.Positions)
		p, ok, err := restored.Position(bridge)
		if err != nil || !ok {
			t.Fatalf("the bridge has no position after a snapshot round trip "+
				"(ok=%v err=%v): it copies its channel from the beginning", ok, err)
		}
		if p.Offset != 2 {
			t.Errorf("the bridge resumed at %d, want 2", p.Offset)
		}
	})

	t.Run("sqlite", func(t *testing.T) {
		path := tempPath(t)
		db := open(t, path)
		lg, err := db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}
		for range 2 {
			if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x"}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := lg.SavePosition(store.Position{Reader: bridge, Offset: 2, LastSeen: saved}); err != nil {
			t.Fatalf("save: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		// Reopening is what runs the sweep, so this is the restart rather
		// than an imitation of one.
		again := open(t, path)
		lg2, err := again.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}
		p, ok, err := lg2.Position(bridge)
		if err != nil || !ok {
			t.Fatalf("the bridge has no position after the database was reopened "+
				"(ok=%v err=%v): it copies its channel from the beginning", ok, err)
		}
		if p.Offset != 2 {
			t.Errorf("the bridge resumed at %d, want 2", p.Offset)
		}
	})
}
