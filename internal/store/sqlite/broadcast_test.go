package sqlite

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// broadcastLog is what both providers' broadcast logs are asked for here.
type broadcastLog interface {
	Append(store.Record) (store.Record, error)
	ReadFromN(offset uint64, max int) ([]store.Record, error)
	Trim(before time.Time, maxBytes int64) (int, int64, error)
	Remove(offsets ...uint64) (int, int64, error)
	ReadAt(offsets ...uint64) ([]store.Record, error)
	Next() uint64
	Floor() uint64
	Bytes() int64
}

var _ broadcastLog = (*Log)(nil)
var _ broadcastLog = (*store.Log)(nil)

// A provider's broadcast log, and a way to stop the provider and start it
// again the way a broker does: a memory provider through its file, a sqlite
// one by closing the database and opening it anew.
type broadcastProvider struct {
	name    string
	log     broadcastLog
	restart func(t *testing.T)
}

func memoryBroadcast(t *testing.T) *broadcastProvider {
	d := store.NewDir(t.TempDir(), "0.1.0-test")
	p := &broadcastProvider{name: "memory", log: store.NewBroadcastLog()}
	p.restart = func(t *testing.T) {
		t.Helper()
		next, floor, records := p.log.(*store.Log).Export()
		if err := d.SaveBroadcast(&store.BroadcastSnapshot{Next: next, Floor: floor, Records: records}); err != nil {
			t.Fatalf("memory: save the broadcast log: %v", err)
		}
		snap, err := d.LoadBroadcast()
		if err != nil {
			t.Fatalf("memory: load the broadcast log: %v", err)
		}
		// No file reads as no log, which is what a first start is. Here it
		// would be a restart that never went through the file.
		if snap == nil {
			t.Fatal("memory: the broadcast log's file was not there to load")
		}
		p.log = store.RestoreBroadcastLog(snap.Next, snap.Floor, snap.Records, snap.Positions)
	}
	return p
}

func sqliteBroadcast(t *testing.T) *broadcastProvider {
	path := tempPath(t)
	db := open(t, path)
	lg, err := db.Broadcast()
	if err != nil {
		t.Fatalf("sqlite: broadcast log: %v", err)
	}
	p := &broadcastProvider{name: "sqlite", log: lg}
	p.restart = func(t *testing.T) {
		t.Helper()
		if err := db.Close(); err != nil {
			t.Fatalf("sqlite: close: %v", err)
		}
		db = open(t, path)
		if p.log, err = db.Broadcast(); err != nil {
			t.Fatalf("sqlite: broadcast log after a restart: %v", err)
		}
	}
	return p
}

// state checks the log's two counters and its bytes, what it holds read from
// the floor, and that a read below the floor is refused rather than served.
func (p *broadcastProvider) state(t *testing.T, when string, next, floor uint64, bytes int64, held []store.Record) {
	t.Helper()
	if p.log.Next() != next || p.log.Floor() != floor || p.log.Bytes() != bytes {
		t.Errorf("%s: next %d, floor %d, %d bytes; want %d, %d and %d",
			when, p.log.Next(), p.log.Floor(), p.log.Bytes(), next, floor, bytes)
	}
	got, err := p.log.ReadFromN(floor, 0)
	if err != nil {
		t.Fatalf("%s: read from the floor: %v", when, err)
	}
	assertSameRecords(t, when, got, held)
	if floor > 1 {
		if _, err := p.log.ReadFromN(floor-1, 0); !errors.Is(err, store.ErrBelowFloor) {
			t.Errorf("%s: a read below the floor returned %v, want ErrBelowFloor", when, err)
		}
	}
}

// RFC 0004's rules for the broadcast log, on both providers: `next` and
// `floor` are stored and never derived, and an offset is never reused after a
// trim or a restart. The log restarts between every step, because a restart is
// where a derived counter shows itself: MAX(offset)+1 over an emptied log is 1,
// and MIN(offset) over it reads as nothing ever removed (invariants 1 and 9).
func TestTheBroadcastLogKeepsItsOffsetsAcrossTrimsAndRestarts(t *testing.T) {
	for _, p := range []*broadcastProvider{memoryBroadcast(t), sqliteBroadcast(t)} {
		t.Run(p.name, func(t *testing.T) {
			state := func(when string, next, floor uint64, bytes int64, held []store.Record) {
				t.Helper()
				p.state(t, when, next, floor, bytes, held)
			}

			state("a new log", 1, 1, 0, nil)

			var held []store.Record
			var total int64
			for _, r := range awkward() {
				got, err := p.log.Append(r)
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				held = append(held, got)
				total += store.RecordSize(r)
			}
			n := uint64(len(held))
			for i, r := range held {
				if r.Offset != uint64(i+1) {
					t.Fatalf("record %d took offset %d, want %d", i, r.Offset, i+1)
				}
			}
			p.restart(t)
			state("appended and restarted", n+1, 1, total, held)

			// The bound that frees exactly the first two: the floor is the
			// offset after the last one removed, in the same operation.
			first2 := store.RecordSize(held[0]) + store.RecordSize(held[1])
			removed, freed, err := p.log.Trim(time.Time{}, total-first2)
			if err != nil || removed != 2 || freed != first2 {
				t.Fatalf("trim to %d bytes: removed %d freeing %d (%v), want 2 freeing %d",
					total-first2, removed, freed, err, first2)
			}
			state("two trimmed", n+1, 3, total-first2, held[2:])
			p.restart(t)
			state("two trimmed and restarted", n+1, 3, total-first2, held[2:])

			// A bound of one byte, which no message fits under, empties it.
			if removed, _, err := p.log.Trim(time.Time{}, 1); err != nil || removed != int(n)-2 {
				t.Fatalf("trim everything: removed %d (%v), want %d", removed, err, n-2)
			}
			state("emptied", n+1, n+1, 0, nil)
			p.restart(t)
			state("emptied and restarted", n+1, n+1, 0, nil)

			// Nothing survived to derive either counter from, so the next
			// message takes the offset after the last one ever written.
			one := store.Record{MessageID: "m-after", Topic: "events/after", Payload: []byte("after"), QoS: 1}
			got, err := p.log.Append(one)
			if err != nil {
				t.Fatalf("append after emptying: %v", err)
			}
			if got.Offset != n+1 {
				t.Fatalf("the first message after emptying took offset %d, want %d", got.Offset, n+1)
			}
			p.restart(t)
			state("one more and restarted", n+2, n+1, store.RecordSize(one), []store.Record{got})
		})
	}
}

// A message leaves the broadcast log on its own once nobody owes it, wherever
// it stands (RFC 0003 "Broadcast"), so the log has gaps. RFC 0004's rules for
// them, on both providers: a message removed from the middle survives neither
// a read nor a restart, and next and floor are unchanged by it; the floor is
// the lowest offset still held, and no offset is reused.
func TestAMessageLeavesTheBroadcastLogOnItsOwn(t *testing.T) {
	for _, p := range []*broadcastProvider{memoryBroadcast(t), sqliteBroadcast(t)} {
		t.Run(p.name, func(t *testing.T) {
			var held []store.Record
			var total int64
			for _, r := range awkward() {
				got, err := p.log.Append(r)
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				held = append(held, got)
				total += store.RecordSize(r)
			}
			if len(held) != 6 {
				t.Fatalf("the script is written for 6 messages and awkward() holds %d", len(held))
			}
			size := func(offset uint64) int64 { return store.RecordSize(held[offset-1]) }
			// keep is the messages at these offsets, as appended.
			keep := func(offsets ...uint64) []store.Record {
				var out []store.Record
				for _, o := range offsets {
					out = append(out, held[o-1])
				}
				return out
			}
			remove := func(want int, offsets ...uint64) {
				t.Helper()
				var freed int64
				for _, o := range offsets {
					if want > 0 && o >= 1 && o <= uint64(len(held)) {
						freed += size(o)
					}
				}
				n, got, err := p.log.Remove(offsets...)
				if err != nil || n != want || (want > 0 && got != freed) {
					t.Fatalf("remove %v: %d freeing %d (%v), want %d freeing %d", offsets, n, got, err, want, freed)
				}
			}
			p.restart(t)

			remove(1, 3)
			state := func(when string, next, floor uint64, bytes int64, held []store.Record) {
				t.Helper()
				p.state(t, when, next, floor, bytes, held)
			}
			state("3 removed from the middle", 7, 1, total-size(3), keep(1, 2, 4, 5, 6))
			p.restart(t)
			state("3 removed and restarted", 7, 1, total-size(3), keep(1, 2, 4, 5, 6))

			// Gone already, never written, and below every offset: a release
			// that finds nothing changes nothing.
			remove(0, 3, 99, 0)
			state("nothing there to remove", 7, 1, total-size(3), keep(1, 2, 4, 5, 6))

			remove(1, 1)
			state("the lowest removed", 7, 2, total-size(3)-size(1), keep(2, 4, 5, 6))
			// The next lowest goes, and the floor passes the gap at 3 to the
			// lowest message still held.
			remove(1, 2)
			state("the floor past a gap", 7, 4, size(4)+size(5)+size(6), keep(4, 5, 6))
			p.restart(t)
			state("the floor past a gap, restarted", 7, 4, size(4)+size(5)+size(6), keep(4, 5, 6))

			// A trim to make room walks past a message that left on its own,
			// counting it neither as removed nor as freed again.
			remove(1, 5)
			removed, freed, err := p.log.Trim(time.Time{}, size(6))
			if err != nil || removed != 1 || freed != size(4) {
				t.Fatalf("trim across a gap: removed %d freeing %d (%v), want 1 freeing %d", removed, freed, err, size(4))
			}
			state("trimmed across a gap", 7, 6, size(6), keep(6))
			p.restart(t)
			state("trimmed across a gap, restarted", 7, 6, size(6), keep(6))

			remove(1, 6)
			state("emptied by removal", 7, 7, 0, nil)
			p.restart(t)
			state("emptied by removal, restarted", 7, 7, 0, nil)

			// Offsets carry on from where they were, and one call takes out
			// several, in any order.
			var more []store.Record
			for i := range 3 {
				got, err := p.log.Append(store.Record{MessageID: fmt.Sprint("m-late-", i), Topic: "events/late",
					Payload: []byte("late"), QoS: 1})
				if err != nil {
					t.Fatalf("append after emptying: %v", err)
				}
				more = append(more, got)
			}
			if more[0].Offset != 7 {
				t.Fatalf("the first message after emptying took offset %d, want 7", more[0].Offset)
			}
			if n, _, err := p.log.Remove(9, 7); err != nil || n != 2 {
				t.Fatalf("remove 9 and 7 in one call: %d (%v), want 2", n, err)
			}
			state("two removed in one call", 10, 8, store.RecordSize(more[1]), more[1:2])
			p.restart(t)
			state("two removed in one call, restarted", 10, 8, store.RecordSize(more[1]), more[1:2])
		})
	}
}

// A session's drain reads the offsets it is owed rather than everything after
// its cursor, so the log answers a read by offset. On both providers, and
// across a restart: the records come back whole, in offset order and each
// once, however the offsets were asked for; an offset the log does not hold -
// removed from the middle, trimmed from the front, never written - is left
// out rather than refused or answered with a neighbour.
func TestTheBroadcastLogIsReadAtTheOffsetsAsked(t *testing.T) {
	for _, p := range []*broadcastProvider{memoryBroadcast(t), sqliteBroadcast(t)} {
		t.Run(p.name, func(t *testing.T) {
			var held []store.Record
			for _, r := range awkward() {
				got, err := p.log.Append(r)
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				held = append(held, got)
			}
			if len(held) != 6 {
				t.Fatalf("the script is written for 6 messages and awkward() holds %d", len(held))
			}
			keep := func(offsets ...uint64) []store.Record {
				var out []store.Record
				for _, o := range offsets {
					out = append(out, held[o-1])
				}
				return out
			}
			read := func(when string, want []store.Record, offsets ...uint64) {
				t.Helper()
				got, err := p.log.ReadAt(offsets...)
				if err != nil {
					t.Fatalf("%s: read %v: %v", when, offsets, err)
				}
				assertSameRecords(t, when, got, want)
			}

			read("all six, backwards", keep(1, 2, 3, 4, 5, 6), 6, 5, 4, 3, 2, 1)
			read("nothing asked", nil)

			// 3 leaves from the middle, and 1 is trimmed from the front.
			if n, _, err := p.log.Remove(3); err != nil || n != 1 {
				t.Fatalf("remove 3: %d (%v)", n, err)
			}
			if n, _, err := p.log.Trim(time.Time{}, p.log.Bytes()-store.RecordSize(held[0])); err != nil || n != 1 {
				t.Fatalf("trim 1: %d (%v)", n, err)
			}
			if p.log.Floor() != 2 {
				t.Fatalf("the floor is %d after the trim, want 2", p.log.Floor())
			}
			asked := []uint64{6, 2, 99, 3, 1, 0, 4, 2, 6}
			read("removed, trimmed, never written and repeated", keep(2, 4, 6), asked...)
			p.restart(t)
			read("the same, restarted", keep(2, 4, 6), asked...)
			// One offset asked more times than one query names on sqlite, so
			// the repeats fall in more than one query and are still one record.
			many := slices.Repeat([]uint64{4}, 2*readAtChunk+1)
			read("one offset asked in more than two queries' worth", keep(4), many...)

			// More offsets than one query names on sqlite, so the answer is
			// assembled from several and still comes back as one, in order.
			var all []uint64
			want := keep(2, 4, 5, 6)
			for i := range 1200 {
				got, err := p.log.Append(store.Record{MessageID: fmt.Sprint("m-many-", i), Topic: "events/many",
					Payload: []byte{byte(i)}, QoS: 1})
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				want = append(want, got)
			}
			for o := p.log.Next() - 1; o >= 1; o-- {
				all = append(all, o)
			}
			if len(all) <= 2*readAtChunk {
				t.Fatalf("%d offsets asked, which is not more than two queries' worth (%d each)", len(all), readAtChunk)
			}
			read("every offset ever written, backwards", want, all...)
		})
	}
}

// Only the broadcast log lets a message go from the middle. A channel's
// records leave only from the front: a gap in the middle is one a consumer
// reads straight across, reporting success over what it never received
// (invariant 1). So a channel's store refuses, and keeps everything it holds.
func TestOnlyTheBroadcastLogLetsAMessageGoFromTheMiddle(t *testing.T) {
	db := open(t, tempPath(t))
	sq, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	channels := map[string]broadcastLog{
		"a sqlite channel":           sq,
		"a memory channel":           store.NewLog(),
		"a memory channel, restored": store.RestoreLog(1, 1, nil, nil),
	}
	for name, lg := range channels {
		for i := range 3 {
			if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/a", Payload: []byte("x")}); err != nil {
				t.Fatalf("%s: append: %v", name, err)
			}
		}
		bytes := lg.Bytes()
		n, freed, err := lg.Remove(2)
		if !errors.Is(err, store.ErrNotTheBroadcastLog) || n != 0 || freed != 0 {
			t.Errorf("%s: remove 2 returned %d, %d, %v; want ErrNotTheBroadcastLog and nothing removed", name, n, freed, err)
		}
		got, err := lg.ReadFromN(1, 0)
		if err != nil || len(got) != 3 || lg.Floor() != 1 || lg.Next() != 4 || lg.Bytes() != bytes {
			t.Errorf("%s: after the refusal, %d records (%v), floor %d, next %d, %d bytes; want 3, 1, 4 and %d",
				name, len(got), err, lg.Floor(), lg.Next(), lg.Bytes(), bytes)
		}
	}
}

// The broadcast log is one store per provider, as a channel is: asking twice
// gives back the store built the first time, so it has one writer and one
// copy of its counters. It is kept under its reserved name in the channels
// table, as an append channel's counters are.
func TestTheBroadcastLogIsOneStoreUnderItsReservedName(t *testing.T) {
	db := open(t, tempPath(t))
	first, err := db.Broadcast()
	if err != nil {
		t.Fatalf("broadcast log: %v", err)
	}
	second, err := db.Broadcast()
	if err != nil {
		t.Fatalf("broadcast log again: %v", err)
	}
	if first != second {
		t.Error("asking twice built two stores, which would be two writers with two copies of one counter")
	}
	var kind int64
	if err := db.db.QueryRow(`SELECT kind FROM channels WHERE name = ?`, store.BroadcastLog).Scan(&kind); err != nil {
		t.Fatalf("the log's counter row: %v", err)
	}
	if store.Kind(kind) != store.KindAppend {
		t.Errorf("the log's counter row is kind %s, want append", store.Kind(kind))
	}
}

// RFC 0004: the broadcast log is session state, so the migration leaves it
// behind with the sessions. Export is the one reader that walks every row of
// the channels table, so it is the one that could carry it as a channel -
// which a memory provider would then never read, since its log is its own
// file.
func TestExportLeavesTheBroadcastLogBehind(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Broadcast()
	if err != nil {
		t.Fatalf("broadcast log: %v", err)
	}
	if _, err := lg.Append(store.Record{MessageID: "m-1", Topic: "state/a", Payload: []byte("on")}); err != nil {
		t.Fatalf("append to the log: %v", err)
	}

	out, err := db.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("a database holding only the broadcast log exported %d channel(s): %+v", len(out), out)
	}

	events, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if _, err := events.Append(store.Record{MessageID: "m-2", Topic: "events/a", Payload: []byte("x")}); err != nil {
		t.Fatalf("append to the channel: %v", err)
	}
	// Both rows are there to walk, so leaving one out is the query's doing
	// and not an empty table's.
	var rows int
	if err := db.db.QueryRow(`SELECT count(*) FROM channels`).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("the channels table holds %d rows (%v), want the log's and the channel's", rows, err)
	}
	out, err = db.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(out) != 1 || out[0].Name != "events" || len(out[0].Records) != 1 {
		t.Errorf("exported %+v, want the one channel with its one record", out)
	}
}

// cursorLog is what the tests below ask of a session store's broadcast log:
// the position API a durable consumer's cursor already uses.
type cursorLog interface {
	Append(store.Record) (store.Record, error)
	Next() uint64
	Position(reader string) (store.Position, bool, error)
	SavePosition(store.Position) error
}

// A session store with the broadcast log it owns, and a way to stop and
// start it the way a broker does: a memory provider through its two files,
// a sqlite one by closing the database and opening it again.
type sessionsWithLog struct {
	name     string
	sessions interface {
		Save(store.Session) error
		Get(string) (store.Session, bool, error)
		Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error
		Begin(client string, held []string, next *store.Session) (store.Dropped, error)
		Drop(string, []string) (store.Dropped, error)
		DropExpired(time.Time, func(store.Session) bool) ([]string, store.Dropped, error)
		SetInFlight(client string, window uint16, f store.InFlight) error
		SetInFlightAll(client string, window uint16, fs []store.InFlight) error
		ClearInFlight(client string, packetID uint16) (bool, error)
		Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error)
		InFlight(client string) (uint16, []store.InFlight, error)
		HandOver(group string, cursor uint64, client string, window uint16, fs []store.InFlight) error
		CreateShareCursor(group string, cursor uint64) error
		SetShareCursor(group string, cursor uint64, forget ...uint64) error
		DropShareCursor(group string) (bool, error)
		ShareCursors() (map[string]uint64, error)
		EndedAtOpen() map[string]store.ShareGroupState
		ShareReturned() (map[string][]uint64, error)
		Return(client, group string, fs []store.InFlight) error
		Lend(group string, cursor uint64, offsets []uint64) error
		EndShareCursorIfUnheld(group string) (bool, error)
		Bytes() int64
	}
	log     func(t *testing.T) cursorLog
	restart func(t *testing.T)
}

func memorySessionsWithLog(t *testing.T) *sessionsWithLog {
	d := store.NewDir(t.TempDir(), "0.1.0-test")
	s := store.NewSessions()
	p := &sessionsWithLog{name: "memory", sessions: s}
	p.log = func(t *testing.T) cursorLog {
		lg, err := s.Log()
		if err != nil {
			t.Fatalf("memory: the sessions' log: %v", err)
		}
		return lg
	}
	p.restart = func(t *testing.T) {
		t.Helper()
		if err := d.SaveSessions(s.Snapshot()); err != nil {
			t.Fatalf("memory: save the sessions and their log: %v", err)
		}
		snap, err := d.LoadSessions()
		if err != nil || snap == nil || snap.Log == nil {
			t.Fatalf("memory: load the sessions and their log: %+v, %v", snap, err)
		}
		s = store.RestoreSessions(snap)
		p.sessions = s
	}
	return p
}

func sqliteSessionsWithLog(t *testing.T) *sessionsWithLog {
	path := tempPath(t)
	db := open(t, path)
	s := sessionsOn(t, db)
	p := &sessionsWithLog{name: "sqlite", sessions: s}
	p.log = func(t *testing.T) cursorLog {
		lg, err := s.Log()
		if err != nil {
			t.Fatalf("sqlite: the sessions' log: %v", err)
		}
		return lg
	}
	p.restart = func(t *testing.T) {
		t.Helper()
		if err := db.Close(); err != nil {
			t.Fatalf("sqlite: close: %v", err)
		}
		db = open(t, path)
		s = sessionsOn(t, db)
		p.sessions = s
	}
	return p
}

// RFC 0004: a session's cursor is its reader position on the broadcast log,
// kept as a durable consumer's is, and it lives exactly as long as the
// session record. On both providers:
//   - it survives a restart even when it last moved longer ago than the
//     session's expiry interval, which is what a session connected at a
//     crash looks like - the positions' own expiry sweep does not judge it;
//   - a session's ending takes it, whichever way the session ends;
//   - a cursor whose session is not held is dropped at the start, since
//     nothing else would ever take it out (invariant 13), and a reader of
//     another kind is not a session's to lose.
func TestASessionsCursorLivesAndEndsWithItsSession(t *testing.T) {
	long := time.Unix(1700000000, 0)
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			expiring := aSession("expiring")
			expiring.ExpiryInterval = 1
			expiring.DisconnectedAt = long
			for _, s := range []store.Session{aSession("stale"), aSession("fresh"), expiring} {
				if err := p.sessions.Save(s); err != nil {
					t.Fatalf("save %s: %v", s.Client, err)
				}
			}
			lg := p.log(t)
			for i := range 3 {
				if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			cursors := []store.Position{
				// Last moved long ago, with an interval that has long run out.
				{Reader: store.MQTTReader("stale"), Offset: 2, LastSeen: long, ExpiresIn: time.Second},
				{Reader: store.MQTTReader("fresh"), Offset: 3, LastSeen: time.Now(), ExpiresIn: time.Hour},
				{Reader: store.MQTTReader("expiring"), Offset: 1, LastSeen: long, ExpiresIn: time.Second},
				{Reader: store.MQTTReader("ghost"), Offset: 2, LastSeen: time.Now(), ExpiresIn: time.Hour},
				{Reader: store.BridgeReader("head-office"), Offset: 1},
			}
			for _, c := range cursors {
				if err := lg.SavePosition(c); err != nil {
					t.Fatalf("save %s's cursor: %v", c.Reader, err)
				}
			}
			// The ghost's cursor is really there before the restart, so its
			// absence after is the start's doing.
			if _, ok, err := lg.Position(store.MQTTReader("ghost")); !ok || err != nil {
				t.Fatalf("the ghost's cursor was not kept before the restart: %v, %v", ok, err)
			}

			at := func(when, client string, want uint64) {
				t.Helper()
				reader := store.MQTTReader(client)
				if client == "head-office" {
					reader = store.BridgeReader(client)
				}
				got, ok, err := p.log(t).Position(reader)
				if err != nil {
					t.Fatalf("%s: %s's cursor: %v", when, client, err)
				}
				switch {
				case want == 0 && ok:
					t.Errorf("%s: %s still has a cursor at %d", when, client, got.Offset)
				case want != 0 && (!ok || got.Offset != want):
					t.Errorf("%s: %s's cursor is at %d (held %v), want %d", when, client, got.Offset, ok, want)
				}
			}

			p.restart(t)
			at("restarted", "stale", 2)
			at("restarted", "fresh", 3)
			at("restarted", "expiring", 1)
			at("restarted", "ghost", 0)
			at("restarted", "head-office", 1)
			if next := p.log(t).Next(); next != 4 {
				t.Errorf("restarted: the log's next is %d, want 4", next)
			}

			// A session saved again - new subscriptions, a reconnect - keeps
			// its place.
			again := aSession("stale")
			again.Subscriptions = again.Subscriptions[:1]
			if err := p.sessions.Save(again); err != nil {
				t.Fatalf("save again: %v", err)
			}
			at("saved again", "stale", 2)

			if _, err := p.sessions.Drop("stale", nil); err != nil {
				t.Fatalf("drop: %v", err)
			}
			at("dropped", "stale", 0)
			gone, _, err := p.sessions.DropExpired(time.Now(), nil)
			if err != nil || len(gone) != 1 || gone[0] != "expiring" {
				t.Fatalf("expired %v (%v), want only expiring", gone, err)
			}
			at("expired", "expiring", 0)

			p.restart(t)
			at("restarted again", "stale", 0)
			at("restarted again", "expiring", 0)
			at("restarted again", "fresh", 3)
			at("restarted again", "head-office", 1)
		})
	}
}

// The start drops a cursor with no session on the broadcast log alone. A
// channel's positions are judged by their own expiry sweep, and the session
// they belong to may be kept by another provider entirely, so this file's
// sessions table says nothing about them.
func TestOnlyTheBroadcastLogsCursorsGoWithoutASession(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	events, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	lg, err := db.Broadcast()
	if err != nil {
		t.Fatalf("broadcast log: %v", err)
	}
	ghost := store.Position{Reader: store.MQTTReader("ghost"), Offset: 1, LastSeen: time.Now(), ExpiresIn: time.Hour}
	for _, l := range []cursorLog{events, lg} {
		if err := l.SavePosition(ghost); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = open(t, path)
	if events, err = db.Log("events"); err != nil {
		t.Fatalf("log: %v", err)
	}
	if lg, err = db.Broadcast(); err != nil {
		t.Fatalf("broadcast log: %v", err)
	}
	if _, ok, err := events.Position(ghost.Reader); !ok || err != nil {
		t.Errorf("a channel's position with no session here was dropped: %v, %v", ok, err)
	}
	if _, ok, err := lg.Position(ghost.Reader); ok || err != nil {
		t.Errorf("the broadcast log kept a cursor with no session: %v, %v", ok, err)
	}
}

// RFC 0004: a durable session keeps its in-flight table on the broadcast log
// - each message on the wire under the packet identifier a re-send must carry
// (MQTT-4.4.0-1) - with the window it was written under, and the table lives
// exactly as long as the session. On both providers, restarting between
// steps:
//   - it survives a restart and a session saved again;
//   - a table larger than its window is refused, and so is an entry that
//     cannot be on the wire, each leaving the table as it was;
//   - an entry its session's cursor has passed was acknowledged before the
//     stop, and is gone after the start;
//   - a session's ending takes it;
//   - each entry is counted against the provider as it is kept and as it goes.
func TestASessionsInFlightTableLivesAndEndsWithIt(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save: %v", err)
			}
			lg := p.log(t)
			for i := range 20 {
				if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 2}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if err := lg.SavePosition(store.Position{Reader: store.MQTTReader("c"), Offset: 10}); err != nil {
				t.Fatalf("cursor: %v", err)
			}

			table := func(when string, window uint16, want ...store.InFlight) {
				t.Helper()
				gotWindow, got, err := p.sessions.InFlight("c")
				if err != nil {
					t.Fatalf("%s: read the table: %v", when, err)
				}
				if gotWindow != window || len(got) != len(want) {
					t.Fatalf("%s: window %d holding %+v, want %d holding %+v", when, gotWindow, got, window, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("%s: entry %d is %+v, want %+v", when, i, got[i], want[i])
					}
				}
			}
			a := store.InFlight{Offset: 10, PacketID: 1, QoS: 1, State: store.MessageSent}
			b := store.InFlight{Offset: 11, PacketID: 2, QoS: 2, State: store.MessageSent}
			c := store.InFlight{Offset: 12, PacketID: 3, QoS: 2, State: store.MessageReleased}

			before := p.sessions.Bytes()
			for _, f := range []store.InFlight{a, b, c} {
				if err := p.sessions.SetInFlight("c", 3, f); err != nil {
					t.Fatalf("set %+v: %v", f, err)
				}
			}
			if got := p.sessions.Bytes() - before; got != 3*store.InFlightSize {
				t.Errorf("three entries counted %d bytes, want %d", got, 3*store.InFlightSize)
			}
			// PUBREC on the second: its exchange moves on, and the table
			// holds no more than it did, so the full window takes it.
			b.State = store.MessageReleased
			if err := p.sessions.SetInFlight("c", 3, b); err != nil {
				t.Fatalf("move 2 on to its PUBREL: %v", err)
			}
			if got := p.sessions.Bytes() - before; got != 3*store.InFlightSize {
				t.Errorf("a state change counted %d bytes in all, want %d", got, 3*store.InFlightSize)
			}

			// Every case but the first has room in its window, so a refusal
			// there is the entry's own and not the window's.
			for _, bad := range []struct {
				why    string
				client string
				window uint16
				f      store.InFlight
				is     error
			}{
				{"a fourth in a window of three", "c", 3, store.InFlight{Offset: 13, PacketID: 4, QoS: 1, State: store.MessageSent}, store.ErrWindowFull},
				{"no session", "nobody", 5, store.InFlight{Offset: 13, PacketID: 4, QoS: 1, State: store.MessageSent}, store.ErrNoSession},
				{"packet identifier 0", "c", 5, store.InFlight{Offset: 13, PacketID: 0, QoS: 1, State: store.MessageSent}, nil},
				{"offset 0", "c", 5, store.InFlight{Offset: 0, PacketID: 4, QoS: 1, State: store.MessageSent}, nil},
				{"QoS 0", "c", 5, store.InFlight{Offset: 13, PacketID: 4, QoS: 0, State: store.MessageSent}, nil},
				{"QoS 3", "c", 5, store.InFlight{Offset: 13, PacketID: 4, QoS: 3, State: store.MessageSent}, nil},
				{"waiting, which is not on the wire", "c", 5, store.InFlight{Offset: 13, PacketID: 4, QoS: 1, State: store.MessageQueued}, nil},
				{"a PUBREL owed at QoS 1", "c", 5, store.InFlight{Offset: 13, PacketID: 4, QoS: 1, State: store.MessageReleased}, nil},
			} {
				err := p.sessions.SetInFlight(bad.client, bad.window, bad.f)
				if err == nil || (bad.is != nil && !errors.Is(err, bad.is)) {
					t.Errorf("%s: set returned %v, want a refusal%s", bad.why, err, map[bool]string{true: " that is " + fmt.Sprint(bad.is)}[bad.is != nil])
				}
			}
			table("refusals", 3, a, b, c)

			p.restart(t)
			table("restarted", 3, a, b, c)

			// Saved again - a reconnect, new subscriptions - the table stays.
			again := aSession("c")
			again.Subscriptions = again.Subscriptions[:1]
			if err := p.sessions.Save(again); err != nil {
				t.Fatalf("save again: %v", err)
			}
			table("saved again", 3, a, b, c)
			// The session itself now counts less, with fewer subscriptions;
			// what the table counts is measured from here.
			before = p.sessions.Bytes() - 3*store.InFlightSize

			if ok, err := p.sessions.ClearInFlight("c", 1); !ok || err != nil {
				t.Fatalf("clear 1: %v, %v", ok, err)
			}
			if ok, err := p.sessions.ClearInFlight("c", 1); ok || err != nil {
				t.Errorf("clearing 1 twice answered %v, %v", ok, err)
			}
			if ok, err := p.sessions.ClearInFlight("nobody", 2); ok || err != nil {
				t.Errorf("clearing for no session answered %v, %v", ok, err)
			}
			table("1 acknowledged", 3, b, c)

			// The cursor moved past 11 and the stop came before its entry
			// went: that one was acknowledged, and the start lets it go.
			// 12 is at the cursor, still unacknowledged, and stays.
			if err := p.log(t).SavePosition(store.Position{Reader: store.MQTTReader("c"), Offset: 12}); err != nil {
				t.Fatalf("cursor: %v", err)
			}
			p.restart(t)
			table("the cursor past 11, restarted", 3, c)
			if got := p.sessions.Bytes() - before; got != store.InFlightSize {
				t.Errorf("after the start the table counts %d bytes, want %d", got, store.InFlightSize)
			}

			if _, err := p.sessions.Drop("c", nil); err != nil {
				t.Fatalf("drop: %v", err)
			}
			table("dropped", 0)
			p.restart(t)
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save anew: %v", err)
			}
			table("a new session under the same id", 0)
		})
	}
}

// RFC 0004: what cannot be true of an in-flight table at a start is refused,
// naming what was found, rather than started on - on both providers. An entry
// at or past the log's next offset names a message the log never had, and a
// table larger than the window it was written under is one no sender could
// have written within MQTT's Receive Maximum. On sqlite, rows with no
// broadcast log in the file are refused too; memory's equivalent is a format
// 10 sessions file with no saguin.broadcast (see
// TestTheSessionsAndTheirLogAreWrittenAndReadAsOnePair).
func TestAnInFlightTableThatCannotBeTrueIsRefusedAtTheStart(t *testing.T) {
	flying := store.InFlight{Offset: 99, PacketID: 1, QoS: 1, State: store.MessageSent}

	t.Run("memory, past the log's next", func(t *testing.T) {
		// Written as a file directly: the store refuses to hold the entry
		// (TestAnInFlightPastTheLogsNextIsRefusedAsItIsWritten), so a file
		// holding it is what a start can meet and nothing it could write.
		d := store.NewDir(t.TempDir(), "0.1.0-test")
		snap := &store.SessionsSnapshot{
			Sessions: []store.SessionState{{Session: aSession("c"), Window: 1, InFlight: []store.InFlight{flying}}},
			Log:      &store.BroadcastSnapshot{Next: 1, Floor: 1},
		}
		if err := d.SaveSessions(snap); err != nil {
			t.Fatal(err)
		}
		if _, err := d.LoadSessions(); err == nil || !strings.Contains(err.Error(), "the broadcast log's next is 1") {
			t.Errorf("loaded, or refused for another reason: %v", err)
		}
	})
	t.Run("memory, larger than its window", func(t *testing.T) {
		var buf bytes.Buffer
		snap := &store.SessionsSnapshot{Sessions: []store.SessionState{{
			Session: aSession("c"), Window: 1, InFlight: []store.InFlight{
				{Offset: 1, PacketID: 1, QoS: 1, State: store.MessageSent},
				{Offset: 2, PacketID: 2, QoS: 1, State: store.MessageSent},
			}}}}
		if err := snap.Encode(&buf); err != nil {
			t.Fatal(err)
		}
		if _, err := store.DecodeSessions(buf.Bytes()); !errors.Is(err, store.ErrWindowFull) {
			t.Errorf("decoded, or refused for another reason: %v", err)
		}
	})
	for _, c := range []struct {
		why  string
		snap store.SessionsSnapshot
		want string
	}{
		{"a group's cursor at 0", store.SessionsSnapshot{Groups: map[string]uint64{"$share/g/jobs": 0}},
			"cursor is at offset 0"},
		{"a cursor for no group", store.SessionsSnapshot{Groups: map[string]uint64{"jobs": 3}},
			`"jobs" is not a shared group's filter`},
		{"a returned offset at 0", store.SessionsSnapshot{Returned: map[string][]uint64{"$share/g/jobs": {0}}},
			"holds offset 0 at 0 or twice"},
		{"a returned offset twice", store.SessionsSnapshot{Returned: map[string][]uint64{"$share/g/jobs": {2, 2}}},
			"holds offset 2 at 0 or twice"},
		{"a returned list for no group", store.SessionsSnapshot{Returned: map[string][]uint64{"jobs": {2}}},
			`"jobs" is not a shared group's filter`},
		{"an entry naming no group's filter", store.SessionsSnapshot{Sessions: []store.SessionState{{
			Session: aSession("c"), Window: 1, InFlight: []store.InFlight{
				{Offset: 1, PacketID: 1, QoS: 1, State: store.MessageSent, Group: "jobs"}}}}},
			`"jobs" is not a shared group's filter`},
	} {
		t.Run("memory, "+c.why, func(t *testing.T) {
			var buf bytes.Buffer
			if err := c.snap.Encode(&buf); err != nil {
				t.Fatal(err)
			}
			if _, err := store.DecodeSessions(buf.Bytes()); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("decoded, or refused for another reason: %v; want %q", err, c.want)
			}
		})
	}

	sqliteCase := func(t *testing.T, want string, is error, spoil func(t *testing.T, db *DB)) {
		t.Helper()
		path := tempPath(t)
		db := open(t, path)
		s := sessionsOn(t, db)
		if err := s.Save(aSession("c")); err != nil {
			t.Fatal(err)
		}
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
		for _, f := range []store.InFlight{{Offset: 1, PacketID: 1, QoS: 1, State: store.MessageSent},
			{Offset: 2, PacketID: 2, QoS: 1, State: store.MessageSent}} {
			if err := s.SetInFlight("c", 2, f); err != nil {
				t.Fatal(err)
			}
		}
		spoil(t, db)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = open(t, path).Sessions()
		if err == nil || !strings.Contains(err.Error(), want) || (is != nil && !errors.Is(err, is)) {
			t.Errorf("the store opened, or was refused for another reason: %v; want %q", err, want)
		}
	}
	t.Run("sqlite, past the log's next", func(t *testing.T) {
		sqliteCase(t, "the broadcast log's next is 4", nil, func(t *testing.T, db *DB) {
			if _, err := db.db.Exec(`UPDATE session_inflight SET "offset" = 99 WHERE packet_id = 2`); err != nil {
				t.Fatal(err)
			}
		})
	})
	t.Run("sqlite, larger than its window", func(t *testing.T) {
		sqliteCase(t, "under a window of 1", store.ErrWindowFull, func(t *testing.T, db *DB) {
			if _, err := db.db.Exec(`UPDATE sessions SET receive_maximum = 1`); err != nil {
				t.Fatal(err)
			}
		})
	})
	// The file's CHECKs: a row naming no group's filter, and a group's
	// cursor at 0 or under no group's name, cannot be written at all.
	for _, spoil := range []string{
		`UPDATE session_inflight SET share_group = 'jobs' WHERE packet_id = 2`,
		`INSERT INTO share_groups (share_group, "cursor") VALUES ('$share/g/jobs', 0)`,
		`INSERT INTO share_groups (share_group, "cursor") VALUES ('jobs', 3)`,
		`INSERT INTO share_returned (share_group, "offset") VALUES ('$share/g/jobs', 0)`,
		`INSERT INTO share_returned (share_group, "offset") VALUES ('jobs', 3)`,
	} {
		t.Run("sqlite, "+spoil, func(t *testing.T) {
			db := open(t, tempPath(t))
			s := sessionsOn(t, db)
			if err := s.Save(aSession("c")); err != nil {
				t.Fatal(err)
			}
			lg, err := s.Log()
			if err != nil {
				t.Fatal(err)
			}
			for i := range 3 {
				if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x")}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.SetInFlight("c", 2, store.InFlight{Offset: 2, PacketID: 2, QoS: 1, State: store.MessageSent}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.db.Exec(spoil); err == nil || !strings.Contains(err.Error(), "CHECK") {
				t.Errorf("the file took it, or refused it for another reason: %v", err)
			}
		})
	}
	t.Run("sqlite, with no log", func(t *testing.T) {
		sqliteCase(t, "a broadcast log the file does not hold", nil, func(t *testing.T, db *DB) {
			if _, err := db.db.Exec(`DELETE FROM channels WHERE name = ?`, store.BroadcastLog); err != nil {
				t.Fatal(err)
			}
		})
	})
}

// RFC 0004: a drain records a batch of deliveries in a session's in-flight
// table before it writes them, and the batch is kept whole or not at all. On
// both providers:
//   - a batch goes in with its window, and is counted as each entry is new;
//   - a refusal - the window, any one entry, an identifier given twice, no
//     session - leaves the table and the count as they were;
//   - a batch may move an entry on and add another, counting only the new one;
//   - an empty batch changes nothing, its window included;
//   - what went in survives a restart.
func TestAnInFlightBatchIsKeptWholeOrNotAtAll(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save: %v", err)
			}
			lg := p.log(t)
			for i := range 20 {
				if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 2}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			table := func(when string, window uint16, want ...store.InFlight) {
				t.Helper()
				gotWindow, got, err := p.sessions.InFlight("c")
				if err != nil {
					t.Fatalf("%s: read the table: %v", when, err)
				}
				if gotWindow != window || len(got) != len(want) {
					t.Fatalf("%s: window %d holding %+v, want %d holding %+v", when, gotWindow, got, window, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("%s: entry %d is %+v, want %+v", when, i, got[i], want[i])
					}
				}
			}
			a := store.InFlight{Offset: 10, PacketID: 1, QoS: 1, State: store.MessageSent}
			b := store.InFlight{Offset: 11, PacketID: 2, QoS: 2, State: store.MessageSent}
			c := store.InFlight{Offset: 12, PacketID: 3, QoS: 2, State: store.MessageReleased}
			d := store.InFlight{Offset: 13, PacketID: 4, QoS: 1, State: store.MessageSent}

			before := p.sessions.Bytes()
			if err := p.sessions.SetInFlightAll("c", 4, []store.InFlight{a, b, c}); err != nil {
				t.Fatalf("a batch of three: %v", err)
			}
			table("a batch of three", 4, a, b, c)
			if got := p.sessions.Bytes() - before; got != 3*store.InFlightSize {
				t.Errorf("three entries counted %d bytes, want %d", got, 3*store.InFlightSize)
			}

			// Every case but the first has room in its window, so a refusal
			// there is the batch's own and not the window's - and the window
			// the table was written under is still four after each.
			for _, bad := range []struct {
				why    string
				client string
				window uint16
				fs     []store.InFlight
				is     error
			}{
				{"two more in a window of four", "c", 4, []store.InFlight{d, {Offset: 14, PacketID: 5, QoS: 1, State: store.MessageSent}}, store.ErrWindowFull},
				{"one entry of two cannot be on the wire", "c", 6, []store.InFlight{d, {Offset: 14, PacketID: 0, QoS: 1, State: store.MessageSent}}, nil},
				{"one identifier given twice", "c", 6, []store.InFlight{d, {Offset: 14, PacketID: 4, QoS: 1, State: store.MessageSent}}, nil},
				{"no session", "nobody", 6, []store.InFlight{d}, store.ErrNoSession},
			} {
				err := p.sessions.SetInFlightAll(bad.client, bad.window, bad.fs)
				if err == nil || (bad.is != nil && !errors.Is(err, bad.is)) {
					t.Errorf("%s: the batch was answered %v, want a refusal%s", bad.why, err, map[bool]string{true: " that is " + fmt.Sprint(bad.is)}[bad.is != nil])
				}
				table(bad.why, 4, a, b, c)
				if got := p.sessions.Bytes() - before; got != 3*store.InFlightSize {
					t.Errorf("%s: the table counts %d bytes after the refusal, want %d", bad.why, got, 3*store.InFlightSize)
				}
			}

			// PUBREC on 2 and a new delivery under 4, in one batch: the full
			// window of four takes it, and only 4 is new.
			b.State = store.MessageReleased
			if err := p.sessions.SetInFlightAll("c", 4, []store.InFlight{b, d}); err != nil {
				t.Fatalf("move 2 on and add 4: %v", err)
			}
			table("2 moved on and 4 added", 4, a, b, c, d)
			if got := p.sessions.Bytes() - before; got != 4*store.InFlightSize {
				t.Errorf("four entries counted %d bytes, want %d", got, 4*store.InFlightSize)
			}

			if err := p.sessions.SetInFlightAll("c", 1, nil); err != nil {
				t.Errorf("an empty batch was answered %v", err)
			}
			table("an empty batch under a window of one", 4, a, b, c, d)

			p.restart(t)
			table("restarted", 4, a, b, c, d)
		})
	}
}

// RFC 0004: an acknowledgement clears its in-flight entries and moves the
// session's cursor together, and names each entry by its identifier and its
// offset. On both providers:
//   - what is cleared goes, the cursor is where it was put, and the count
//     falls by exactly what went;
//   - an acknowledgement for an identifier since given to another message
//     leaves that message's entry alone, and one repeated changes nothing;
//   - a client with no session is refused, and no cursor is made for it;
//   - the table and the cursor survive a restart as the last one left them.
func TestAnAcknowledgementClearsItsEntriesAndMovesTheCursor(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 20 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 2}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			state := func(when string, cursor uint64, want ...store.InFlight) {
				t.Helper()
				_, got, err := p.sessions.InFlight("c")
				if err != nil {
					t.Fatalf("%s: read the table: %v", when, err)
				}
				if len(got) != len(want) {
					t.Fatalf("%s: the table holds %+v, want %+v", when, got, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("%s: entry %d is %+v, want %+v", when, i, got[i], want[i])
					}
				}
				pos, has, err := p.log(t).Position(store.MQTTReader("c"))
				if err != nil || !has || pos.Offset != cursor {
					t.Errorf("%s: the cursor is %+v (held %v, %v), want %d", when, pos, has, err, cursor)
				}
			}
			ack := func(when string, cursor uint64, want int, done ...store.InFlight) {
				t.Helper()
				n, err := p.sessions.Acknowledge("c", cursor, done)
				if err != nil || n != want {
					t.Fatalf("%s: acknowledged %d (%v), want %d", when, n, err, want)
				}
			}
			a := store.InFlight{Offset: 10, PacketID: 1, QoS: 1, State: store.MessageSent}
			b := store.InFlight{Offset: 11, PacketID: 2, QoS: 1, State: store.MessageSent}
			c := store.InFlight{Offset: 12, PacketID: 3, QoS: 2, State: store.MessageReleased}
			if err := p.sessions.SetInFlightAll("c", 5, []store.InFlight{a, b, c}); err != nil {
				t.Fatalf("set: %v", err)
			}
			before := p.sessions.Bytes()

			ack("PUBACK on 10", 11, 1, a)
			state("10 acknowledged", 11, b, c)
			if got := before - p.sessions.Bytes(); got != store.InFlightSize {
				t.Errorf("one entry cleared gave back %d bytes, want %d", got, store.InFlightSize)
			}

			// Identifier 1 is free again and goes out with 13. The first
			// acknowledgement arriving again, or a stale copy of it, names 10.
			a13 := store.InFlight{Offset: 13, PacketID: 1, QoS: 1, State: store.MessageSent}
			if err := p.sessions.SetInFlightAll("c", 5, []store.InFlight{a13}); err != nil {
				t.Fatalf("set 13 under identifier 1: %v", err)
			}
			ack("the acknowledgement of 10 again", 11, 0, a)
			state("identifier 1 reused for 13", 11, b, c, a13)

			if n, err := p.sessions.Acknowledge("nobody", 5, []store.InFlight{b}); !errors.Is(err, store.ErrNoSession) || n != 0 {
				t.Errorf("an acknowledgement for no session was answered %d, %v; want ErrNoSession", n, err)
			}
			if pos, has, err := p.log(t).Position(store.MQTTReader("nobody")); has || err != nil {
				t.Errorf("a refused acknowledgement left a cursor %+v (%v)", pos, err)
			}
			state("after the refusal", 11, b, c, a13)

			p.restart(t)
			state("restarted", 11, b, c, a13)

			// PUBACK on 11 and PUBCOMP on 12, together.
			ack("11 and 12", 13, 2, b, c)
			state("11 and 12 acknowledged", 13, a13)
			p.restart(t)
			state("11 and 12 acknowledged, restarted", 13, a13)
		})
	}
}

// On sqlite the two promises above are one transaction's: a write that fails
// part of the way leaves nothing of itself behind. A trigger refuses the last
// statement of each, and the table, the cursor and the count are what they
// were before it.
func TestAFailedInFlightWriteLeavesNothingOfItself(t *testing.T) {
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	if err := s.Save(aSession("c")); err != nil {
		t.Fatal(err)
	}
	lg, err := s.Log()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	a := store.InFlight{Offset: 10, PacketID: 1, QoS: 1, State: store.MessageSent}
	if err := s.SetInFlightAll("c", 5, []store.InFlight{a}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Acknowledge("c", 10, nil); err != nil || n != 0 {
		t.Fatalf("place the cursor: %d, %v", n, err)
	}
	unchanged := func(when string) {
		t.Helper()
		_, got, err := s.InFlight("c")
		if err != nil || len(got) != 1 || got[0] != a {
			t.Errorf("%s: the table holds %+v (%v), want only %+v", when, got, err, a)
		}
		if pos, _, err := lg.Position(store.MQTTReader("c")); err != nil || pos.Offset != 10 {
			t.Errorf("%s: the cursor is at %d (%v), want 10", when, pos.Offset, err)
		}
	}
	bytes := s.Bytes()

	// The second entry's insert is refused, after the first's has run.
	if _, err := db.db.Exec(`CREATE TRIGGER refuse_second BEFORE INSERT ON session_inflight
		WHEN NEW.packet_id = 3 BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`); err != nil {
		t.Fatal(err)
	}
	err = s.SetInFlightAll("c", 5, []store.InFlight{
		{Offset: 11, PacketID: 2, QoS: 1, State: store.MessageSent},
		{Offset: 12, PacketID: 3, QoS: 1, State: store.MessageSent},
	})
	if err == nil || !strings.Contains(err.Error(), "refused by the test") {
		t.Fatalf("the batch was answered %v, want the trigger's refusal", err)
	}
	unchanged("a batch refused at its second entry")

	// The cursor's write is refused, after the entry's deletion has run.
	for _, stmt := range []string{
		`CREATE TRIGGER refuse_cursor_insert BEFORE INSERT ON positions
			WHEN NEW.reader = 'mqtt:c' BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`,
		`CREATE TRIGGER refuse_cursor_update BEFORE UPDATE ON positions
			WHEN NEW.reader = 'mqtt:c' BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`,
	} {
		if _, err := db.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.Acknowledge("c", 11, []store.InFlight{a})
	if err == nil || !strings.Contains(err.Error(), "refused by the test") || n != 0 {
		t.Fatalf("the acknowledgement was answered %d, %v; want the trigger's refusal", n, err)
	}
	unchanged("an acknowledgement refused at its cursor")
	if s.Bytes() != bytes {
		t.Errorf("the store counts %d bytes after two refused writes, want %d", s.Bytes(), bytes)
	}
}

// RFC 0004 refuses an in-flight entry at or past the broadcast log's next
// offset at a start, so the store refuses it as it is written: a store that
// kept one would be a broker that does not start. On both providers, through
// the one-entry call and the batch: an entry at next, one past it, and a
// batch holding one of them beside an entry the log does hold are each
// ErrPastNext and leave the table as it was; the entry below next is kept;
// and what was kept is a file the start accepts.
func TestAnInFlightPastTheLogsNextIsRefusedAsItIsWritten(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 3 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if next := p.log(t).Next(); next != 4 {
				t.Fatalf("the log's next is %d after three messages, want 4", next)
			}
			last := store.InFlight{Offset: 3, PacketID: 1, QoS: 1, State: store.MessageSent}
			if err := p.sessions.SetInFlight("c", 5, last); err != nil {
				t.Fatalf("the last message the log holds: %v", err)
			}
			table := func(when string) {
				t.Helper()
				_, got, err := p.sessions.InFlight("c")
				if err != nil || len(got) != 1 || got[0] != last {
					t.Errorf("%s: the table holds %+v (%v), want only %+v", when, got, err, last)
				}
			}
			at := store.InFlight{Offset: 4, PacketID: 2, QoS: 1, State: store.MessageSent}
			past := store.InFlight{Offset: 99, PacketID: 2, QoS: 1, State: store.MessageSent}
			held := store.InFlight{Offset: 2, PacketID: 3, QoS: 1, State: store.MessageSent}
			for _, c := range []struct {
				why string
				set func() error
			}{
				{"one entry at next", func() error { return p.sessions.SetInFlight("c", 5, at) }},
				{"one entry past next", func() error { return p.sessions.SetInFlight("c", 5, past) }},
				{"a batch with one at next", func() error { return p.sessions.SetInFlightAll("c", 5, []store.InFlight{held, at}) }},
				{"a batch with one past next", func() error { return p.sessions.SetInFlightAll("c", 5, []store.InFlight{past, held}) }},
			} {
				if err := c.set(); !errors.Is(err, store.ErrPastNext) {
					t.Errorf("%s: answered %v, want ErrPastNext", c.why, err)
				}
				table(c.why)
			}
			p.restart(t)
			table("restarted")
		})
	}
}

// A stored position past its log's next offset is refused at a start (a
// snapshot's validate, and the in-flight rule beside it), so SavePosition
// refuses it as it is written, for every log: a channel's and the broadcast
// log, on both providers. A position at next is a reader that has read
// everything and is kept; one past it is ErrPastNext and leaves the position
// that was there.
func TestAPositionPastTheLogsNextIsRefusedAsItIsWritten(t *testing.T) {
	type positionLog interface {
		Append(store.Record) (store.Record, error)
		Next() uint64
		Position(reader string) (store.Position, bool, error)
		SavePosition(store.Position) error
	}
	db := open(t, tempPath(t))
	sqChannel, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	sqBroadcast, err := db.Broadcast()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		log  positionLog
	}{
		{"a memory channel", store.NewLog()},
		{"the memory broadcast log", store.NewBroadcastLog()},
		{"a sqlite channel", sqChannel},
		{"the sqlite broadcast log", sqBroadcast},
	} {
		t.Run(c.name, func(t *testing.T) {
			for i := range 3 {
				if _, err := c.log.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/a", Payload: []byte("x")}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			reader := store.MQTTReader("c")
			next := c.log.Next()
			if err := c.log.SavePosition(store.Position{Reader: reader, Offset: next}); err != nil {
				t.Fatalf("a position at next (%d): %v", next, err)
			}
			if err := c.log.SavePosition(store.Position{Reader: reader, Offset: next + 1}); !errors.Is(err, store.ErrPastNext) {
				t.Errorf("a position past next was answered %v, want ErrPastNext", err)
			}
			if pos, has, err := c.log.Position(reader); err != nil || !has || pos.Offset != next {
				t.Errorf("after the refusal the position is %+v (held %v, %v), want %d", pos, has, err, next)
			}
		})
	}
}

// An acknowledgement clears everything it names however many that is, on
// both providers. A session acknowledges whatever arrived in
// broker.session.ack_commit_interval in one call, which for a fast one is
// thousands of entries, and sqlite clears them in statements that each take
// at most so many parameters: 17,003 entries is more than one statement could
// carry, two parameters each against SQLite's 32,766, and leaves a last
// statement of three padded out to the rest.
func TestAnAcknowledgementOfThousandsClearsEveryEntry(t *testing.T) {
	const n = 17003
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save: %v", err)
			}
			done := make([]store.InFlight, n)
			for i := range done {
				r, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a",
					Payload: []byte("x"), QoS: 1})
				if err != nil {
					t.Fatalf("append: %v", err)
				}
				done[i] = store.InFlight{Offset: r.Offset, PacketID: uint16(i + 1), QoS: 1, State: store.MessageSent}
			}
			if err := p.sessions.SetInFlightAll("c", 65535, done); err != nil {
				t.Fatalf("set: %v", err)
			}
			if _, table, err := p.sessions.InFlight("c"); err != nil || len(table) != n {
				t.Fatalf("the table holds %d entries (%v), want %d, so this proves nothing", len(table), err, n)
			}
			before := p.sessions.Bytes()

			got, err := p.sessions.Acknowledge("c", done[n-1].Offset+1, done)
			if err != nil || got != n {
				t.Fatalf("acknowledged %d of %d: %v", got, n, err)
			}
			if _, table, err := p.sessions.InFlight("c"); err != nil || len(table) != 0 {
				t.Errorf("after the acknowledgement the table still holds %d entries (%v)", len(table), err)
			}
			if freed := before - p.sessions.Bytes(); freed != n*store.InFlightSize {
				t.Errorf("clearing %d entries gave back %d bytes, want %d", n, freed, n*store.InFlightSize)
			}
		})
	}
}

// A session that begins new begins with nothing an ended one left, on both
// providers: Begin ends the record, the in-flight table and the cursor kept
// under the id and keeps the new record in their place, in one step. For an id
// with nothing kept it is a save, and with no record given it keeps nothing.
// The bytes it counts are what a restart counts from what is stored.
func TestABeginEndsWhatWasKeptAndKeepsTheNext(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			fresh := store.Session{Client: "fresh", ExpiryInterval: 60}
			if _, err := p.sessions.Begin("fresh", nil, &fresh); err != nil {
				t.Fatalf("begin a session for an id with nothing kept: %v", err)
			}
			if got, ok, _ := p.sessions.Get("fresh"); !ok || got.ExpiryInterval != 60 {
				t.Fatalf("an id with nothing kept holds %+v (%v), want the session begun", got, ok)
			}

			// What an ended session left under "c": its record, with a Will,
			// three entries on the wire and its cursor.
			old := aSession("c")
			old.Will = &store.SessionWill{Topic: "wills/c", Payload: []byte("gone"), QoS: 1}
			if err := p.sessions.Save(old); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 5 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a",
					Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			var rows []store.InFlight
			for i := range 3 {
				rows = append(rows, store.InFlight{Offset: uint64(2 + i), PacketID: uint16(1 + i), QoS: 1,
					State: store.MessageSent})
			}
			if err := p.sessions.SetInFlightAll("c", 10, rows); err != nil {
				t.Fatalf("set: %v", err)
			}
			if _, err := p.sessions.Acknowledge("c", 2, nil); err != nil {
				t.Fatalf("a cursor: %v", err)
			}
			nothingOfTheOld := func(when string) {
				t.Helper()
				window, table, _ := p.sessions.InFlight("c")
				if len(table) != 0 {
					t.Errorf("%s: the new session's in-flight table holds %+v, the ended one's", when, table)
				}
				if window != 0 {
					t.Errorf("%s: the new session's table was written under a window of %d, the ended one's", when, window)
				}
				if pos, ok, _ := p.log(t).Position(store.MQTTReader("c")); ok {
					t.Errorf("%s: the new session has a cursor at %d, the ended one's", when, pos.Offset)
				}
			}

			next := store.Session{Client: "c", ExpiryInterval: 300}
			if _, err := p.sessions.Begin("c", nil, &next); err != nil {
				t.Fatalf("begin over what an ended session left: %v", err)
			}
			nothingOfTheOld("begun")
			counted := p.sessions.Bytes()
			p.restart(t)
			nothingOfTheOld("after a restart")
			got, ok, _ := p.sessions.Get("c")
			if !ok || got.ExpiryInterval != 300 || got.Will != nil || len(got.Subscriptions) != 0 {
				t.Errorf("after a restart the id holds %+v (%v), want the session begun and nothing of the "+
					"ended one's record", got, ok)
			}
			if p.sessions.Bytes() != counted {
				t.Errorf("Begin counted %d bytes and a restart counts %d from what is stored",
					counted, p.sessions.Bytes())
			}

			if _, err := p.sessions.Begin("c", nil, nil); err != nil {
				t.Fatalf("begin keeping nothing: %v", err)
			}
			if _, ok, _ := p.sessions.Get("c"); ok {
				t.Error("a Begin that keeps nothing left the record")
			}
			other := store.Session{Client: "other"}
			if _, err := p.sessions.Begin("c", nil, &other); err == nil {
				t.Error("a session for one client id was begun under another")
			}
		})
	}
}

// A session begun for an id with nothing kept is a new session's save, and is
// refused at the publish ceiling as a save is; one that replaces an ended
// session gives back more than it writes, and is kept there. Otherwise every
// fresh CONNECT at the bound would spend the room the endings rely on.
func TestABeginTakesTheReserveOnlyToReplace(t *testing.T) {
	will := func(client string) store.Session {
		return store.Session{Client: client, ExpiryInterval: 300, Will: &store.SessionWill{
			Topic: "wills/" + client, Payload: bytes.Repeat([]byte("w"), 200), QoS: 1}}
	}
	type sessions interface {
		Save(store.Session) error
		Begin(client string, held []string, next *store.Session) (store.Dropped, error)
	}
	for _, tc := range []struct {
		name string
		open func(t *testing.T) sessions
	}{
		{"memory", func(t *testing.T) sessions {
			s := store.NewSessions()
			s.SetQuota(store.NewQuota(64<<10, 16<<10))
			return s
		}},
		{"sqlite", func(t *testing.T) sessions {
			db, err := OpenBounded(tempPath(t), "test", store.SQLiteEmptyBytes+128<<10, 32<<10)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			return sessionsOn(t, db)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.open(t)
			if err := s.Save(will("ended")); err != nil {
				t.Fatalf("save the session to be replaced: %v", err)
			}
			var err error
			filled := 0
			for i := range 5000 {
				if err = s.Save(will(fmt.Sprintf("filler-%04d", i))); err != nil {
					break
				}
				filled++
			}
			if !errors.Is(err, store.ErrProviderFull) || filled == 0 {
				t.Fatalf("filling stopped after %d with %v, want the publish ceiling", filled, err)
			}
			fresh := will("fresh")
			if _, err := s.Begin("fresh", nil, &fresh); !errors.Is(err, store.ErrProviderFull) {
				t.Errorf("a session begun at the ceiling for an id with nothing kept answered %v, want "+
					"ErrProviderFull: it took the reserve", err)
			}
			next := will("ended")
			if _, err := s.Begin("ended", nil, &next); err != nil {
				t.Errorf("a session begun at the ceiling in place of an ended one answered %v, want it kept", err)
			}
		})
	}
}

// A disconnect is recorded in one write, on both providers: the moment the
// client went away, and - only when asked - the Will withdrawn, with what it
// took given back. A client with no session is told so and nothing is made
// for it, and what was written survives a restart.
func TestADisconnectIsRecordedInOneWrite(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			sess := aSession("c")
			sess.Will = &store.SessionWill{Topic: "wills/c", Payload: []byte("gone"), QoS: 1}
			if err := p.sessions.Save(sess); err != nil {
				t.Fatalf("save: %v", err)
			}
			withWill := p.sessions.Bytes()
			first, second := time.Unix(1_700_000_000, 0), time.Unix(1_700_000_100, 0)

			if err := p.sessions.Disconnected("c", first, false, 60); err != nil {
				t.Fatalf("a disconnect keeping the Will: %v", err)
			}
			got, _, _ := p.sessions.Get("c")
			if !got.DisconnectedAt.Equal(first) || got.Will == nil || p.sessions.Bytes() != withWill {
				t.Errorf("keeping the Will: disconnected %v, Will %v, bytes %d; want %v, the Will, %d",
					got.DisconnectedAt, got.Will, p.sessions.Bytes(), first, withWill)
			}
			// **The expiry in force as the client went is what is kept**: a
			// DISCONNECT may change the one its CONNECT was granted.
			if got.ExpiryInterval != 60 {
				t.Errorf("the disconnect's expiry was kept as %d, want 60", got.ExpiryInterval)
			}

			if err := p.sessions.Disconnected("c", second, true, 7200); err != nil {
				t.Fatalf("a disconnect withdrawing the Will: %v", err)
			}
			p.restart(t)
			got, _, _ = p.sessions.Get("c")
			sess.Will = nil
			if got.ExpiryInterval != 7200 {
				t.Errorf("after a restart the session's expiry is %d, want 7200, the second disconnect's",
					got.ExpiryInterval)
			}
			if !got.DisconnectedAt.Equal(second) || got.Will != nil || len(got.Subscriptions) != len(sess.Subscriptions) {
				t.Errorf("withdrawing the Will, after a restart: disconnected %v, Will %v, %d subscriptions; "+
					"want %v, none, %d", got.DisconnectedAt, got.Will, len(got.Subscriptions), second,
					len(sess.Subscriptions))
			}
			if want := withWill - int64(len("wills/c")+len("gone")); p.sessions.Bytes() != want {
				t.Errorf("the withdrawn Will left %d bytes counted, want %d", p.sessions.Bytes(), want)
			}

			if err := p.sessions.Disconnected("nobody", first, true, 3600); !errors.Is(err, store.ErrNoSession) {
				t.Errorf("a disconnect for no session answered %v, want ErrNoSession", err)
			}
			if _, ok, _ := p.sessions.Get("nobody"); ok {
				t.Error("a disconnect for no session made one")
			}
		})
	}
}

// Each statement Acknowledge pads a chunk out to names exactly the pairs its
// size says, and a chunk takes the smallest that holds it: an acknowledgement
// of one entry binds three parameters, not the 1,001 of a full chunk, which
// SQLite parses and binds whatever is real.
func TestAnAcknowledgementBindsOnlyTheStatementItsChunkNeeds(t *testing.T) {
	for _, st := range []struct {
		text  string
		pairs int
	}{
		{acknowledgeStatement1, 1}, {acknowledgeStatement10, 10},
		{acknowledgeStatement100, 100}, {acknowledgeStatement, acknowledgeChunk},
	} {
		if n := strings.Count(st.text, "(?, ?)"); n != st.pairs {
			t.Errorf("the %d-pair statement names %d pairs", st.pairs, n)
		}
		if n := strings.Count(st.text, "?"); n != 1+2*st.pairs {
			t.Errorf("the %d-pair statement binds %d parameters, want %d", st.pairs, n, 1+2*st.pairs)
		}
	}
	for _, c := range []struct{ entries, slots int }{
		{1, 1}, {2, 10}, {10, 10}, {11, 100}, {100, 100}, {101, 500}, {500, 500},
	} {
		if got := acknowledgeSlots(c.entries); got != c.slots {
			t.Errorf("a chunk of %d entries takes the %d-pair statement, want %d", c.entries, got, c.slots)
		}
	}
}

// Every entry an acknowledgement names is cleared exactly once, whatever
// size of statement its chunk takes and however far it is padded, and an
// entry it does not name stays: at each size's edges, and across chunks.
func TestAnAcknowledgementClearsWhatItNamesAtEverySize(t *testing.T) {
	p := sqliteSessionsWithLog(t)
	if err := p.sessions.Save(aSession("c")); err != nil {
		t.Fatalf("save: %v", err)
	}
	for i := range 1100 {
		if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 1}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	for _, n := range []int{1, 2, 9, 10, 11, 99, 100, 101, 499, 500, 501, 1001} {
		fs := make([]store.InFlight, n+1)
		for i := range fs {
			fs[i] = store.InFlight{Offset: uint64(i + 1), PacketID: uint16(i + 1), QoS: 1, State: store.MessageSent}
		}
		if err := p.sessions.SetInFlightAll("c", 65535, fs); err != nil {
			t.Fatalf("%d entries: set: %v", n, err)
		}
		kept := fs[n]
		got, err := p.sessions.Acknowledge("c", 1, fs[:n])
		if err != nil || got != n {
			t.Errorf("%d entries named: cleared %d (%v), want %d", n, got, err, n)
		}
		_, left, err := p.sessions.InFlight("c")
		if err != nil || len(left) != 1 || left[0] != kept {
			t.Errorf("%d entries named: the table holds %d entries (%v), want only the one not named, %+v",
				n, len(left), err, kept)
		}
		if _, err := p.sessions.Acknowledge("c", 1, []store.InFlight{kept}); err != nil {
			t.Fatalf("%d entries: clear the one left: %v", n, err)
		}
	}
}

// An acknowledgement's cursor is a stored position, so one past the
// broadcast log's next offset is refused the same way, on both providers:
// ErrPastNext, with the entries it named still in the table and the cursor
// where it was. A cursor at next is kept, and the start accepts what was.
func TestACursorPastTheLogsNextIsRefusedAsItIsAcknowledged(t *testing.T) {
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aSession("c")); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 3 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "state/a", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			a := store.InFlight{Offset: 2, PacketID: 1, QoS: 1, State: store.MessageSent}
			b := store.InFlight{Offset: 3, PacketID: 2, QoS: 1, State: store.MessageSent}
			if err := p.sessions.SetInFlightAll("c", 5, []store.InFlight{a, b}); err != nil {
				t.Fatalf("set: %v", err)
			}
			if n, err := p.sessions.Acknowledge("c", 2, nil); err != nil || n != 0 {
				t.Fatalf("place the cursor: %d, %v", n, err)
			}
			state := func(when string, cursor uint64, want ...store.InFlight) {
				t.Helper()
				_, got, err := p.sessions.InFlight("c")
				if err != nil || len(got) != len(want) {
					t.Fatalf("%s: the table holds %+v (%v), want %+v", when, got, err, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("%s: entry %d is %+v, want %+v", when, i, got[i], want[i])
					}
				}
				if pos, has, err := p.log(t).Position(store.MQTTReader("c")); err != nil || !has || pos.Offset != cursor {
					t.Errorf("%s: the cursor is %+v (held %v, %v), want %d", when, pos, has, err, cursor)
				}
			}
			bytes := p.sessions.Bytes()
			if n, err := p.sessions.Acknowledge("c", 5, []store.InFlight{a}); !errors.Is(err, store.ErrPastNext) || n != 0 {
				t.Errorf("a cursor past next (4) was answered %d, %v; want ErrPastNext", n, err)
			}
			state("after the refusal", 2, a, b)
			if p.sessions.Bytes() != bytes {
				t.Errorf("the refusal moved the count from %d to %d", bytes, p.sessions.Bytes())
			}
			if n, err := p.sessions.Acknowledge("c", 4, []store.InFlight{a, b}); err != nil || n != 2 {
				t.Fatalf("both acknowledged, the cursor at next: %d, %v", n, err)
			}
			state("the cursor at next", 4)
			p.restart(t)
			state("restarted", 4)
		})
	}
}

// aMember is a session holding a shared group's filter, beside one of its
// own, with nothing else set.
func aMember(client string, groups ...string) store.Session {
	sess := store.Session{Client: client, ExpiryInterval: 3600,
		Subscriptions: []store.SessionSubscription{{Filter: "own/#", QoS: 1}}}
	for _, g := range groups {
		sess.Subscriptions = append(sess.Subscriptions, store.SessionSubscription{Filter: g, QoS: 2})
	}
	return sess
}

// sameTable fails unless the session's in-flight table is want, in offset
// order.
func sameTable(t *testing.T, p *sessionsWithLog, when, client string, want ...store.InFlight) {
	t.Helper()
	_, got, err := p.sessions.InFlight(client)
	if err != nil {
		t.Fatalf("%s: read %s's table: %v", when, client, err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("%s: %s's table holds %+v, want %+v", when, client, got, want)
	}
}

// sameCursors fails unless the groups' cursors are want.
func sameCursors(t *testing.T, p *sessionsWithLog, when string, want map[string]uint64) {
	t.Helper()
	got, err := p.sessions.ShareCursors()
	if err != nil {
		t.Fatalf("%s: read the groups' cursors: %v", when, err)
	}
	if len(got) != len(want) {
		t.Errorf("%s: the groups' cursors are %v, want %v", when, got, want)
		return
	}
	for g, c := range want {
		if got[g] != c {
			t.Errorf("%s: the groups' cursors are %v, want %v", when, got, want)
			return
		}
	}
}

// RFC 0004, as amended for shared groups: when the store is opened, an
// in-flight entry behind its session's cursor goes, unless a group handed it
// over - that one is behind the group's cursor, and stays while its session
// does. A group lags its members, so what it hands one is usually behind
// that member's own cursor. On both providers, across a restart: the
// member's own entry behind its cursor goes, the group's entry further
// behind stays, the group's cursor is where the hand-over put it, and the
// count falls by exactly the entry that went.
func TestAnEntryAGroupHandedOverOutlivesItsSessionsCursor(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aMember("m", group)); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 20 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 2}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			own := store.InFlight{Offset: 10, PacketID: 1, QoS: 1, State: store.MessageSent}
			handed := store.InFlight{Offset: 4, PacketID: 2, QoS: 2, State: store.MessageReleased, Group: group}
			if err := p.sessions.SetInFlightAll("m", 5, []store.InFlight{own}); err != nil {
				t.Fatalf("the member's own entry: %v", err)
			}
			if err := p.sessions.CreateShareCursor(group, 1); err != nil {
				t.Fatalf("the group's cursor: %v", err)
			}
			if err := p.sessions.HandOver(group, 5, "m", 5, []store.InFlight{handed}); err != nil {
				t.Fatalf("the group's hand-over: %v", err)
			}
			// The member's cursor moves past both, and the stop comes before
			// its own entry leaves the table: the state a start is written for.
			if _, err := p.sessions.Acknowledge("m", 15, nil); err != nil {
				t.Fatalf("move the member's cursor: %v", err)
			}
			sameTable(t, p, "before the restart", "m", handed, own)
			before := p.sessions.Bytes()

			p.restart(t)
			sameTable(t, p, "restarted", "m", handed)
			sameCursors(t, p, "restarted", map[string]uint64{group: 5})
			if got := before - p.sessions.Bytes(); got != store.InFlightBytes(own) {
				t.Errorf("the start gave back %d bytes, want the own entry's %d", got, store.InFlightBytes(own))
			}
		})
	}
}

// A hand-over keeps the member's entries and moves the group's cursor as one
// write (RFC 0004: the cursor passes the entry in the same transaction that
// writes it), so nothing is refused by half. On both providers:
//   - every refusal - a group with no cursor, the window, no session, an
//     entry that names another group or none, one past the log's next, a
//     cursor past it or at 0, a name that is not a group's, nothing handed
//     over - leaves the table, the cursors and the count as they were;
//   - one that is kept counts the entry with its group's name, and moving the
//     cursor counts nothing;
//   - what was kept survives a restart.
func TestAHandOverKeepsTheEntryAndTheGroupsCursorTogether(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aMember("m", group)); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 20 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 2}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			own := store.InFlight{Offset: 15, PacketID: 1, QoS: 1, State: store.MessageSent}
			if err := p.sessions.SetInFlightAll("m", 2, []store.InFlight{own}); err != nil {
				t.Fatalf("the member's own entry: %v", err)
			}
			entry := func(off uint64, id uint16) store.InFlight {
				return store.InFlight{Offset: off, PacketID: id, QoS: 1, State: store.MessageSent, Group: group}
			}
			// Before the group has a cursor: one ended, or never made.
			before := p.sessions.Bytes()
			if err := p.sessions.HandOver(group, 7, "m", 5, []store.InFlight{entry(6, 2)}); !errors.Is(err, store.ErrNoShareGroup) {
				t.Errorf("a hand-over from a group with no cursor was answered %v, want ErrNoShareGroup", err)
			}
			sameTable(t, p, "a group with no cursor", "m", own)
			sameCursors(t, p, "a group with no cursor", nil)
			if got := p.sessions.Bytes(); got != before {
				t.Errorf("a group with no cursor: the store counts %d bytes after the refusal, want %d", got, before)
			}
			if err := p.sessions.CreateShareCursor(group, 1); err != nil {
				t.Fatalf("the group's cursor: %v", err)
			}
			before = p.sessions.Bytes()
			for _, bad := range []struct {
				why    string
				group  string
				cursor uint64
				client string
				window uint16
				fs     []store.InFlight
				is     error
			}{
				{"two more in a window of two", group, 8, "m", 2, []store.InFlight{entry(6, 2), entry(7, 3)}, store.ErrWindowFull},
				{"no session", group, 7, "nobody", 5, []store.InFlight{entry(6, 2)}, store.ErrNoSession},
				{"an entry naming another group", group, 7, "m", 5, []store.InFlight{{Offset: 6, PacketID: 2, QoS: 1, State: store.MessageSent, Group: "$share/other/jobs"}}, nil},
				{"an entry naming no group", group, 7, "m", 5, []store.InFlight{{Offset: 6, PacketID: 2, QoS: 1, State: store.MessageSent}}, nil},
				{"an entry past the log's next", group, 7, "m", 5, []store.InFlight{entry(21, 2)}, store.ErrPastNext},
				{"a cursor past the log's next", group, 22, "m", 5, []store.InFlight{entry(6, 2)}, store.ErrPastNext},
				{"a cursor at 0", group, 0, "m", 5, []store.InFlight{entry(6, 2)}, nil},
				{"a name that is not a group's", "jobs", 7, "m", 5, []store.InFlight{{Offset: 6, PacketID: 2, QoS: 1, State: store.MessageSent, Group: "jobs"}}, nil},
				{"nothing handed over", group, 7, "m", 5, nil, nil},
			} {
				err := p.sessions.HandOver(bad.group, bad.cursor, bad.client, bad.window, bad.fs)
				if err == nil || (bad.is != nil && !errors.Is(err, bad.is)) {
					t.Errorf("%s: the hand-over was answered %v, want a refusal%s", bad.why, err, map[bool]string{true: " that is " + fmt.Sprint(bad.is)}[bad.is != nil])
				}
				sameTable(t, p, bad.why, "m", own)
				sameCursors(t, p, bad.why, map[string]uint64{group: 1})
				if got := p.sessions.Bytes(); got != before {
					t.Errorf("%s: the store counts %d bytes after the refusal, want %d", bad.why, got, before)
				}
			}

			first := entry(6, 2)
			if err := p.sessions.HandOver(group, 7, "m", 3, []store.InFlight{first}); err != nil {
				t.Fatalf("the first hand-over: %v", err)
			}
			sameTable(t, p, "handed 6", "m", first, own)
			sameCursors(t, p, "handed 6", map[string]uint64{group: 7})
			want := before + store.InFlightBytes(first)
			if got := p.sessions.Bytes(); got != want {
				t.Errorf("the first hand-over counts %d bytes in all, want %d", got, want)
			}
			if store.InFlightBytes(first) != store.InFlightSize+int64(len(group)) {
				t.Errorf("an entry naming %q counts %d bytes, want its fields and the name", group, store.InFlightBytes(first))
			}

			second := entry(8, 3)
			if err := p.sessions.HandOver(group, 9, "m", 3, []store.InFlight{second}); err != nil {
				t.Fatalf("the second hand-over: %v", err)
			}
			want += store.InFlightBytes(second)
			if got := p.sessions.Bytes(); got != want {
				t.Errorf("a hand-over moving the cursor counts %d bytes in all, want %d", got, want)
			}

			p.restart(t)
			sameTable(t, p, "restarted", "m", first, second, own)
			sameCursors(t, p, "restarted", map[string]uint64{group: 9})
			if got := p.sessions.Bytes(); got != want {
				t.Errorf("restarted, the store counts %d bytes, want %d", got, want)
			}
		})
	}
}

// On sqlite a hand-over is one transaction: a trigger refusing the cursor's
// write leaves no entry, and one refusing the entry leaves the cursor where
// it was. The count is unchanged by both.
func TestAFailedHandOverLeavesNothingOfItself(t *testing.T) {
	const group = "$share/g/jobs"
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	if err := s.Save(aMember("m", group)); err != nil {
		t.Fatal(err)
	}
	lg, err := s.Log()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	first := store.InFlight{Offset: 3, PacketID: 1, QoS: 1, State: store.MessageSent, Group: group}
	if err := s.CreateShareCursor(group, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.HandOver(group, 4, "m", 5, []store.InFlight{first}); err != nil {
		t.Fatal(err)
	}
	bytes := s.Bytes()
	unchanged := func(when string) {
		t.Helper()
		_, got, err := s.InFlight("m")
		if err != nil || !slices.Equal(got, []store.InFlight{first}) {
			t.Errorf("%s: the table holds %+v (%v), want only %+v", when, got, err, first)
		}
		cursors, err := s.ShareCursors()
		if err != nil || len(cursors) != 1 || cursors[group] != 4 {
			t.Errorf("%s: the groups' cursors are %v (%v), want %s at 4", when, cursors, err, group)
		}
		if s.Bytes() != bytes {
			t.Errorf("%s: the store counts %d bytes, want %d", when, s.Bytes(), bytes)
		}
	}
	next := store.InFlight{Offset: 5, PacketID: 2, QoS: 1, State: store.MessageSent, Group: group}

	if _, err := db.db.Exec(`CREATE TRIGGER refuse_cursor BEFORE UPDATE ON share_groups
		BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.HandOver(group, 6, "m", 5, []store.InFlight{next}); err == nil || !strings.Contains(err.Error(), "refused by the test") {
		t.Fatalf("the hand-over was answered %v, want the trigger's refusal", err)
	}
	unchanged("a hand-over refused at its cursor")

	if _, err := db.db.Exec(`DROP TRIGGER refuse_cursor`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`CREATE TRIGGER refuse_entry BEFORE INSERT ON session_inflight
		BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.HandOver(group, 6, "m", 5, []store.InFlight{next}); err == nil || !strings.Contains(err.Error(), "refused by the test") {
		t.Fatalf("the hand-over was answered %v, want the trigger's refusal", err)
	}
	unchanged("a hand-over refused at its entry")
}

// RFC 0004: a group's cursor is kept while a session holds the group's
// filter, and goes in the same transaction as the last of them - a cursor
// left with no member would keep what the group is owed in the log with
// nobody to take it. On both providers:
//   - a session ending while another still holds the filter leaves the
//     cursor, and one holding no group's filter ends none;
//   - a session that no longer holds the filter is no member, so the one
//     that still does is the last;
//   - the last member's ending names the groups whose cursors went, and the
//     count falls by exactly their rows;
//   - a clean start writes the new session's record, holding nothing, before
//     the one it replaces ends, so the ending is told the groups it held: the
//     cursor goes where no other kept session holds the filter, and stays
//     where one away still does;
//   - an expiry ends the cursors of the groups whose last member expired and
//     leaves those a connected session still holds;
//   - what went stays gone across a restart.
func TestAGroupsCursorEndsWithTheLastSessionHoldingItsFilter(t *testing.T) {
	const g1, g2, g3, g4 = "$share/one/jobs", "$share/two/jobs", "$share/three/jobs", "$share/four/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			for _, sess := range []store.Session{aMember("a", g1), aMember("b", g1, g2), aMember("c")} {
				if err := p.sessions.Save(sess); err != nil {
					t.Fatalf("save %s: %v", sess.Client, err)
				}
			}
			if _, err := p.log(t).Append(store.Record{MessageID: "m-0", Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
				t.Fatalf("append: %v", err)
			}
			for _, g := range []string{g1, g2} {
				if err := p.sessions.CreateShareCursor(g, 1); err != nil {
					t.Fatalf("%s's cursor: %v", g, err)
				}
			}
			endsHolding := func(when, client string, held []string, want ...string) {
				t.Helper()
				dropped, err := p.sessions.Drop(client, held)
				ended := dropped.Groups
				if err != nil {
					t.Fatalf("%s: drop %s: %v", when, client, err)
				}
				slices.Sort(ended)
				if !slices.Equal(ended, want) {
					t.Errorf("%s: dropping %s ended the cursors of %q, want %q", when, client, ended, want)
				}
			}
			ends := func(when, client string, want ...string) {
				t.Helper()
				endsHolding(when, client, nil, want...)
			}

			ends("a session holding no group", "c")
			sameCursors(t, p, "c ended", map[string]uint64{g1: 1, g2: 1})

			// a unsubscribes from g1: b is now its last member.
			if err := p.sessions.Save(aMember("a")); err != nil {
				t.Fatalf("save a again: %v", err)
			}
			sameCursors(t, p, "a saved without g1", map[string]uint64{g1: 1, g2: 1})
			before := p.sessions.Bytes()
			ends("the last member of both", "b", g1, g2)
			sameCursors(t, p, "b ended", nil)
			member := store.SessionSize(aMember("b", g1, g2))
			if got, want := before-p.sessions.Bytes(), member+store.ShareCursorSize(g1)+store.ShareCursorSize(g2); got != want {
				t.Errorf("b's ending gave back %d bytes, want %d: its record and two cursors", got, want)
			}

			// Clean starts: each member's record is written over with one
			// holding nothing, and then the session it replaced ends. d was
			// g5's only member; e shares g6 with f, which is away and kept.
			const g5, g6 = "$share/five/jobs", "$share/six/jobs"
			for _, sess := range []store.Session{aMember("d", g5), aMember("e", g6), aMember("f", g6)} {
				if err := p.sessions.Save(sess); err != nil {
					t.Fatalf("save %s: %v", sess.Client, err)
				}
			}
			for _, g := range []string{g5, g6} {
				if err := p.sessions.CreateShareCursor(g, 1); err != nil {
					t.Fatalf("%s's cursor: %v", g, err)
				}
			}
			for _, id := range []string{"d", "e"} {
				if err := p.sessions.Save(aMember(id)); err != nil {
					t.Fatalf("the clean start's record for %s: %v", id, err)
				}
			}
			sameCursors(t, p, "the clean starts' records written", map[string]uint64{g5: 1, g6: 1})
			endsHolding("the clean start of g5's only member", "d", []string{g5}, g5)
			endsHolding("the clean start of one of g6's two members", "e", []string{g6})
			sameCursors(t, p, "d and e ended", map[string]uint64{g6: 1})
			if _, err := p.sessions.Drop("f", nil); err != nil {
				t.Fatalf("drop f: %v", err)
			}
			sameCursors(t, p, "f, g6's last member, ended", nil)

			// An expiry: x and z are away past their interval, y is connected.
			away := time.Now().Add(-2 * time.Hour)
			x, z := aMember("x", g3), aMember("z", g4)
			x.DisconnectedAt, z.DisconnectedAt = away, away
			for _, sess := range []store.Session{x, aMember("y", g3), z} {
				if err := p.sessions.Save(sess); err != nil {
					t.Fatalf("save %s: %v", sess.Client, err)
				}
			}
			for _, g := range []string{g3, g4} {
				if err := p.sessions.CreateShareCursor(g, 2); err != nil {
					t.Fatalf("%s's cursor: %v", g, err)
				}
			}
			gone, dropped, err := p.sessions.DropExpired(time.Now(), nil)
			ended := dropped.Groups
			if err != nil {
				t.Fatalf("expire: %v", err)
			}
			slices.Sort(gone)
			if !slices.Equal(gone, []string{"x", "z"}) || !slices.Equal(ended, []string{g4}) {
				t.Errorf("the expiry ended sessions %q and cursors %q, want [x z] and [%s]", gone, ended, g4)
			}
			sameCursors(t, p, "x and z expired", map[string]uint64{g3: 2})

			p.restart(t)
			sameCursors(t, p, "restarted", map[string]uint64{g3: 2})
		})
	}
}

// RFC 0003 "Sessions": a group's backlog is dropped at the start where no
// member is left. A cursor no kept session holds - left by a stop between a
// clean start writing its record and the session it replaced ending, or by a
// last member unsubscribing - is ended as the store is opened, on both
// providers, and named so the start can count what it was owed; a cursor a
// session still holds is kept. Before the restart, making a cursor again for
// the orphaned group is refused on both, as for any group no session holds.
func TestACursorNoSessionHoldsEndsAtTheStart(t *testing.T) {
	const g1, g2 = "$share/one/jobs", "$share/two/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			if err := p.sessions.Save(aMember("m", g1, g2)); err != nil {
				t.Fatalf("save: %v", err)
			}
			for i := range 5 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			for _, g := range []string{g1, g2} {
				if err := p.sessions.CreateShareCursor(g, 3); err != nil {
					t.Fatalf("%s's cursor: %v", g, err)
				}
			}
			// The clean start's record, or an UNSUBSCRIBE: m holds g2 alone,
			// and the stop comes before anything ends g1.
			if err := p.sessions.Save(aMember("m", g2)); err != nil {
				t.Fatalf("save m without g1: %v", err)
			}
			if err := p.sessions.CreateShareCursor(g1, 4); !errors.Is(err, store.ErrNoShareGroup) {
				t.Errorf("making a cursor for g1, which no session holds, was answered %v, want ErrNoShareGroup", err)
			}
			before := p.sessions.Bytes()

			p.restart(t)
			sameCursors(t, p, "restarted", map[string]uint64{g2: 3})
			if got := p.sessions.EndedAtOpen(); len(got) != 1 || got[g1].Cursor != 3 || len(got[g1].Returned) != 0 {
				t.Errorf("the start names the cursors it ended as %v, want %s at 3", got, g1)
			}
			if got, want := before-p.sessions.Bytes(), store.ShareCursorSize(g1); got != want {
				t.Errorf("the start gave back %d bytes, want g1's cursor's %d", got, want)
			}
		})
	}
}

// A group's cursor alone, outside a hand-over (RFC 0004: kept while a
// session holds the group's filter). On both providers:
//   - making one is refused ErrNoShareGroup while no kept session holds the
//     filter, and at 0, past the log's next or under a name that is not a
//     group's, each leaving the count;
//   - one made is counted, and making it again leaves it where it was;
//   - moving one counts nothing, and moving one the group has not got is
//     ErrNoShareGroup and makes none, before it was made and after it ended;
//   - ending it gives its row back and reports it once.
func TestAGroupsCursorIsMadeMovedAndEnded(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			for i := range 5 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			noCursor := func(when string) {
				t.Helper()
				if err := p.sessions.SetShareCursor(group, 2); !errors.Is(err, store.ErrNoShareGroup) {
					t.Errorf("%s: moving a cursor the group has not got was answered %v, want ErrNoShareGroup", when, err)
				}
				sameCursors(t, p, when, nil)
			}
			before := p.sessions.Bytes()
			if err := p.sessions.CreateShareCursor(group, 2); !errors.Is(err, store.ErrNoShareGroup) {
				t.Errorf("a cursor for a group no session holds was answered %v, want ErrNoShareGroup", err)
			}
			noCursor("before it was made")
			if err := p.sessions.Save(aMember("m", group)); err != nil {
				t.Fatalf("save: %v", err)
			}
			before = p.sessions.Bytes()
			for _, bad := range []struct {
				why    string
				group  string
				cursor uint64
				is     error
			}{
				{"at 0", group, 0, nil},
				{"past the log's next", group, 7, store.ErrPastNext},
				{"not a group's name", "jobs", 2, nil},
			} {
				if err := p.sessions.CreateShareCursor(bad.group, bad.cursor); err == nil || (bad.is != nil && !errors.Is(err, bad.is)) {
					t.Errorf("a cursor %s was answered %v, want a refusal", bad.why, err)
				}
			}
			sameCursors(t, p, "refused", nil)
			if got := p.sessions.Bytes(); got != before {
				t.Errorf("refusals left the count at %d, want %d", got, before)
			}

			if err := p.sessions.CreateShareCursor(group, 6); err != nil {
				t.Fatalf("a cursor at the log's next: %v", err)
			}
			if got, want := p.sessions.Bytes()-before, store.ShareCursorSize(group); got != want {
				t.Errorf("a new cursor counts %d bytes, want %d", got, want)
			}
			if err := p.sessions.CreateShareCursor(group, 2); err != nil {
				t.Fatalf("making it again: %v", err)
			}
			sameCursors(t, p, "made again", map[string]uint64{group: 6})
			if err := p.sessions.SetShareCursor(group, 3); err != nil {
				t.Fatalf("move the cursor: %v", err)
			}
			if err := p.sessions.SetShareCursor(group, 7); !errors.Is(err, store.ErrPastNext) {
				t.Errorf("a move past the log's next was answered %v, want ErrPastNext", err)
			}
			if got, want := p.sessions.Bytes()-before, store.ShareCursorSize(group); got != want {
				t.Errorf("a moved cursor counts %d bytes, want %d", got, want)
			}
			p.restart(t)
			sameCursors(t, p, "restarted", map[string]uint64{group: 3})
			if got, want := p.sessions.Bytes()-before, store.ShareCursorSize(group); got != want {
				t.Errorf("restarted, the cursor counts %d bytes, want %d", got, want)
			}

			if had, err := p.sessions.DropShareCursor(group); !had || err != nil {
				t.Fatalf("end the cursor: %v, %v", had, err)
			}
			if had, err := p.sessions.DropShareCursor(group); had || err != nil {
				t.Errorf("ending it twice answered %v, %v", had, err)
			}
			noCursor("after it ended")
			if got := p.sessions.Bytes(); got != before {
				t.Errorf("the ended cursor left the count at %d, want %d", got, before)
			}
		})
	}
}

// On a memory provider the file is the whole of what survives, so a
// snapshot taken while groups hand out messages holds each hand-over whole
// or not at all: a group's cursor one past the last entry it handed over, or
// no cursor and no entry. A file with the entry and not the cursor has the
// start give the message to a second member; one with the cursor and not the
// entry has nobody hold it. Snapshots race eight groups handing over 5,000
// messages each, so a hand-over is waiting on the store's lock whenever a
// snapshot lets it go, and the test counts the snapshots taken mid-way, since
// only they can see a half.
func TestAMemorySnapshotHoldsAHandOverWholeOrNotAtAll(t *testing.T) {
	const groups, each = 8, 5000
	s := store.NewSessions()
	for g := range groups {
		if err := s.Save(aMember(fmt.Sprint("m", g), fmt.Sprintf("$share/g%d/jobs", g))); err != nil {
			t.Fatal(err)
		}
	}
	lg, err := s.Log()
	if err != nil {
		t.Fatal(err)
	}
	all := make([]uint64, 0, groups*each)
	for i := range groups * each {
		r, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, r.Offset)
	}
	// Out of the log again, which leaves its next where it was: a snapshot
	// copies the log too, and a short copy is more snapshots.
	if _, _, err := lg.Remove(all...); err != nil {
		t.Fatal(err)
	}
	for g := range groups {
		if err := s.CreateShareCursor(fmt.Sprintf("$share/g%d/jobs", g), 1); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, groups)
	for g := range groups {
		go func() {
			group, member := fmt.Sprintf("$share/g%d/jobs", g), fmt.Sprint("m", g)
			for i := uint64(1); i <= each; i++ {
				off := uint64(g)*each + i
				// Sixteen identifiers, each taken again as it comes round, so
				// a member holds sixteen entries and a snapshot is quick: the
				// more snapshots, the more moments a half could be seen in.
				f := store.InFlight{Offset: off, PacketID: uint16(i%16 + 1), QoS: 1, State: store.MessageSent, Group: group}
				if err := s.HandOver(group, off+1, member, 65535, []store.InFlight{f}); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	var midway, halves int
	for finished := 0; finished < groups; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("hand-over: %v", err)
			}
			finished++
		default:
		}
		snap := s.Snapshot()
		last := map[string]uint64{}
		for _, st := range snap.Sessions {
			for _, f := range st.InFlight {
				last[f.Group] = max(last[f.Group], f.Offset)
			}
		}
		for g := range groups {
			group := fmt.Sprintf("$share/g%d/jobs", g)
			cursor, has := snap.Groups[group]
			switch {
			case !has && last[group] == 0:
			case has && cursor == last[group]+1:
				if last[group] < uint64(g+1)*each {
					midway++
				}
			default:
				halves++
				if halves <= 3 {
					t.Errorf("a snapshot holds %s's entries through %d with its cursor at %d (held %v)",
						group, last[group], cursor, has)
				}
			}
		}
	}
	if halves > 0 {
		t.Errorf("%d snapshots held a hand-over by half", halves)
	}
	if midway == 0 {
		t.Fatal("no snapshot was taken while the hand-overs ran, so none could have seen a half")
	}
	t.Logf("%d group states read mid-way through %d hand-overs", midway, groups*each)
}

// RFC 0003 "Broadcast": a member's session ending splits what its groups had
// handed it and it had not finished, as MQTT 5 section 4.8.2 splits it, in
// the ending's own step. On both providers, one ending holding one of each:
//   - a QoS 1 delivery is returned to its group, which keeps it across a
//     restart (the SHOULD of 4.8.2);
//   - a QoS 2 one awaiting its PUBREC is lost: no other member may be sent it
//     (MQTT-4.8.2-5);
//   - a QoS 2 one it had answered with a PUBREC is neither: it has it;
//   - one whose group ended with this session, or ended before it, is
//     orphaned, with nobody to return it to;
//   - its own entry is none of these;
//   - the count falls by the session, its table and the group that ended
//     with it, and rises by the returned row.
func TestAMembersEndingSplitsWhatItsGroupsHandedIt(t *testing.T) {
	const g1, g2, g3, g4 = "$share/one/jobs", "$share/two/jobs", "$share/three/jobs", "$share/four/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			for _, sess := range []store.Session{aMember("m", g1, g2, g3, g4), aMember("n", g1, g2), aMember("x", g4)} {
				if err := p.sessions.Save(sess); err != nil {
					t.Fatalf("save %s: %v", sess.Client, err)
				}
			}
			for i := range 20 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 2}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			for _, g := range []string{g1, g2, g3, g4} {
				if err := p.sessions.CreateShareCursor(g, 1); err != nil {
					t.Fatalf("%s's cursor: %v", g, err)
				}
			}
			qos1 := store.InFlight{Offset: 2, PacketID: 1, QoS: 1, State: store.MessageSent, Group: g1}
			qos2 := store.InFlight{Offset: 3, PacketID: 2, QoS: 2, State: store.MessageSent, Group: g1}
			pubrecd := store.InFlight{Offset: 4, PacketID: 3, QoS: 2, State: store.MessageReleased, Group: g2}
			endsWith := store.InFlight{Offset: 5, PacketID: 4, QoS: 1, State: store.MessageSent, Group: g3}
			endedBefore := store.InFlight{Offset: 7, PacketID: 6, QoS: 1, State: store.MessageSent, Group: g4}
			own := store.InFlight{Offset: 6, PacketID: 5, QoS: 1, State: store.MessageSent}
			for _, f := range []store.InFlight{qos1, qos2, pubrecd, endsWith, endedBefore} {
				if err := p.sessions.HandOver(f.Group, 8, "m", 10, []store.InFlight{f}); err != nil {
					t.Fatalf("hand %+v to m: %v", f, err)
				}
			}
			if err := p.sessions.SetInFlightAll("m", 10, []store.InFlight{own}); err != nil {
				t.Fatalf("m's own entry: %v", err)
			}
			// g4 ends before m does: m unsubscribes, and x, its last
			// member, ends. m's entry from it stays in m's table.
			record := aMember("m", g1, g2, g3)
			if err := p.sessions.Save(record); err != nil {
				t.Fatalf("save m without g4: %v", err)
			}
			if d, err := p.sessions.Drop("x", nil); err != nil || !slices.Equal(d.Groups, []string{g4}) {
				t.Fatalf("x's ending ended %q (%v), want [%s]", d.Groups, err, g4)
			}
			before := p.sessions.Bytes()

			d, err := p.sessions.Drop("m", nil)
			if err != nil {
				t.Fatalf("drop m: %v", err)
			}
			if !slices.Equal(d.Groups, []string{g3}) {
				t.Errorf("m's ending ended the cursors of %q, want [%s]", d.Groups, g3)
			}
			if len(d.Returned) != 1 || !slices.Equal(d.Returned[g1], []uint64{2}) {
				t.Errorf("m's ending returned %v, want %s: [2]", d.Returned, g1)
			}
			if !slices.Equal(d.Lost, []store.InFlight{qos2}) {
				t.Errorf("m's ending lost %+v, want only %+v", d.Lost, qos2)
			}
			if !slices.Equal(d.Orphaned, []store.InFlight{endsWith, endedBefore}) {
				t.Errorf("m's ending orphaned %+v, want %+v and %+v", d.Orphaned, endsWith, endedBefore)
			}
			sameCursors(t, p, "m ended", map[string]uint64{g1: 8, g2: 8})
			want := before - store.SessionSize(record) - store.ShareCursorSize(g3) + store.ShareReturnedSize(g1)
			for _, f := range []store.InFlight{qos1, qos2, pubrecd, endsWith, endedBefore, own} {
				want -= store.InFlightBytes(f)
			}
			if got := p.sessions.Bytes(); got != want {
				t.Errorf("after m's ending the store counts %d bytes, want %d", got, want)
			}
			sameReturned(t, p, "m ended", map[string][]uint64{g1: {2}})

			p.restart(t)
			sameReturned(t, p, "restarted", map[string][]uint64{g1: {2}})
			sameCursors(t, p, "restarted", map[string]uint64{g1: 8, g2: 8})
			if got := p.sessions.Bytes(); got != want {
				t.Errorf("restarted, the store counts %d bytes, want %d", got, want)
			}
		})
	}
}

// sameReturned fails unless the groups' returned lists are want.
func sameReturned(t *testing.T, p *sessionsWithLog, when string, want map[string][]uint64) {
	t.Helper()
	got, err := p.sessions.ShareReturned()
	if err != nil {
		t.Fatalf("%s: read the returned lists: %v", when, err)
	}
	if len(got) != len(want) {
		t.Errorf("%s: the returned lists are %v, want %v", when, got, want)
		return
	}
	for g, offs := range want {
		if !slices.Equal(got[g], offs) {
			t.Errorf("%s: the returned lists are %v, want %v", when, got, want)
			return
		}
	}
}

// A returned delivery leaves its group's returned list exactly once, and with
// its room: handed over again, in the hand-over's own step; let go of
// otherwise, by the group's next move of its cursor; and with the group, when
// it ends. On both providers, each surviving a restart.
func TestAReturnedDeliveryLeavesItsListOnce(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			for _, id := range []string{"a", "b", "n"} {
				if err := p.sessions.Save(aMember(id, group)); err != nil {
					t.Fatalf("save %s: %v", id, err)
				}
			}
			for i := range 20 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if err := p.sessions.CreateShareCursor(group, 1); err != nil {
				t.Fatalf("the cursor: %v", err)
			}
			hand := func(id string, off uint64, pid uint16) store.InFlight {
				t.Helper()
				f := store.InFlight{Offset: off, PacketID: pid, QoS: 1, State: store.MessageSent, Group: group}
				if err := p.sessions.HandOver(group, 10, id, 10, []store.InFlight{f}); err != nil {
					t.Fatalf("hand %d to %s: %v", off, id, err)
				}
				return f
			}
			hand("a", 2, 1)
			hand("a", 3, 2)
			hand("b", 4, 1)
			for _, id := range []string{"a", "b"} {
				if _, err := p.sessions.Drop(id, nil); err != nil {
					t.Fatalf("drop %s: %v", id, err)
				}
			}
			sameReturned(t, p, "a and b ended", map[string][]uint64{group: {2, 3, 4}})
			before := p.sessions.Bytes()

			again := hand("n", 2, 1)
			sameReturned(t, p, "2 handed over again", map[string][]uint64{group: {3, 4}})
			if got, want := p.sessions.Bytes()-before, store.InFlightBytes(again)-store.ShareReturnedSize(group); got != want {
				t.Errorf("handing a returned delivery over again changed the count by %d, want %d", got, want)
			}
			before = p.sessions.Bytes()
			if err := p.sessions.SetShareCursor(group, 11, 3, 99); err != nil {
				t.Fatalf("move the cursor, forgetting 3: %v", err)
			}
			sameReturned(t, p, "3 forgotten", map[string][]uint64{group: {4}})
			if got, want := before-p.sessions.Bytes(), store.ShareReturnedSize(group); got != want {
				t.Errorf("forgetting one gave back %d bytes, want %d", got, want)
			}
			p.restart(t)
			sameReturned(t, p, "restarted", map[string][]uint64{group: {4}})
			sameCursors(t, p, "restarted", map[string]uint64{group: 11})

			before = p.sessions.Bytes()
			if had, err := p.sessions.DropShareCursor(group); !had || err != nil {
				t.Fatalf("end the group: %v, %v", had, err)
			}
			sameReturned(t, p, "the group ended", nil)
			if got, want := before-p.sessions.Bytes(), store.ShareCursorSize(group)+store.ShareReturnedSize(group); got != want {
				t.Errorf("the group's end gave back %d bytes, want its cursor and its one returned row, %d", got, want)
			}
			p.restart(t)
			sameReturned(t, p, "the group ended, restarted", nil)
		})
	}
}

// What the start ends of a group no session holds includes what was returned
// to it (EndedAtOpen), so the start can count it with the rest of the
// group's backlog. On both providers.
func TestAGroupEndedAtTheStartNamesWhatWasReturnedToIt(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			for _, id := range []string{"a", "n"} {
				if err := p.sessions.Save(aMember(id, group)); err != nil {
					t.Fatalf("save %s: %v", id, err)
				}
			}
			for i := range 5 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if err := p.sessions.CreateShareCursor(group, 1); err != nil {
				t.Fatalf("the cursor: %v", err)
			}
			f := store.InFlight{Offset: 2, PacketID: 1, QoS: 1, State: store.MessageSent, Group: group}
			if err := p.sessions.HandOver(group, 4, "a", 5, []store.InFlight{f}); err != nil {
				t.Fatalf("hand-over: %v", err)
			}
			if _, err := p.sessions.Drop("a", nil); err != nil {
				t.Fatalf("drop a: %v", err)
			}
			// n, the last member, unsubscribes, and the stop comes first.
			if err := p.sessions.Save(aMember("n")); err != nil {
				t.Fatalf("save n without the group: %v", err)
			}
			p.restart(t)
			got := p.sessions.EndedAtOpen()
			if len(got) != 1 || got[group].Cursor != 4 || !slices.Equal(got[group].Returned, []uint64{2}) {
				t.Errorf("the start names %+v, want %s at 4 with [2] returned", got, group)
			}
			sameReturned(t, p, "restarted", nil)
			sameCursors(t, p, "restarted", nil)
		})
	}
}

// A member's ending is never refused for room, on a sqlite file at its bound
// (RFC 0002: a bound never refuses the operation that relieves it). Its rows
// share their pages with another member's, so deleting them frees no page,
// and its returned rows need pages of their own:
//   - where the provider's reserve holds them, the ending writes them there,
//     and every QoS 1 delivery is returned;
//   - where it cannot - a reserve of four pages here, as a long group name
//     and a large Receive Maximum can outgrow any - the session ends anyway,
//     and those deliveries are unreturned, lost to the provider's room.
func TestAMembersEndingIsNeverRefusedForRoom(t *testing.T) {
	group := "$share/" + strings.Repeat("g", 240) + "/jobs"
	for _, c := range []struct {
		name    string
		reserve int64
		handed  int
	}{
		{"the reserve holds its returned rows", 256 << 10, 400},
		{"the reserve cannot hold them", 16 << 10, 2000},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, err := OpenBounded(tempPath(t), "test", 4<<20, c.reserve)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			s := sessionsOn(t, db)
			for _, id := range []string{"m", "n"} {
				if err := s.Save(aMember(id, group)); err != nil {
					t.Fatal(err)
				}
			}
			lg, err := s.Log()
			if err != nil {
				t.Fatal(err)
			}
			for i := range c.handed + 1 {
				if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("r-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.CreateShareCursor(group, 1); err != nil {
				t.Fatal(err)
			}
			// Five at a time to each in turn, so m's rows and n's share pages.
			var mine []uint64
			for from := 0; from < c.handed; from += 5 {
				to := []string{"m", "n"}[(from/5)%2]
				var fs []store.InFlight
				for i := from; i < from+5; i++ {
					fs = append(fs, store.InFlight{Offset: uint64(i + 1), PacketID: uint16(i + 1), QoS: 1,
						State: store.MessageSent, Group: group})
					if to == "m" {
						mine = append(mine, uint64(i+1))
					}
				}
				if err := s.HandOver(group, uint64(from+6), to, 65535, fs); err != nil {
					t.Fatalf("hand %d-%d to %s: %v", from+1, from+5, to, err)
				}
			}
			filled := 0
			for ; ; filled++ {
				_, err := lg.Append(store.Record{MessageID: fmt.Sprint("f-", filled), Topic: "fill", Payload: make([]byte, 4000), QoS: 1})
				if errors.Is(err, store.ErrFull) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if filled == 0 {
				t.Fatal("nothing filled the file, so this is not an ending at its bound")
			}

			d, err := s.Drop("m", nil)
			if err != nil {
				t.Fatalf("an ending at the bound was refused: %v", err)
			}
			if _, had, _ := s.Get("m"); had {
				t.Fatal("the ending was answered, and m's session is still kept")
			}
			returned, err := s.ShareReturned()
			if err != nil {
				t.Fatal(err)
			}
			unreturned := offsetsOf(d.Unreturned)
			if c.reserve == 256<<10 {
				if !slices.Equal(d.Returned[group], mine) || len(unreturned) != 0 || !slices.Equal(returned[group], mine) {
					t.Errorf("returned %d and kept %d, and %d unreturned; want all %d returned",
						len(d.Returned[group]), len(returned[group]), len(unreturned), len(mine))
				}
			} else if len(d.Returned) != 0 || !slices.Equal(unreturned, mine) || len(returned) != 0 {
				t.Errorf("returned %d and kept %d, and %d unreturned; want all %d unreturned",
					len(d.Returned[group]), len(returned[group]), len(unreturned), len(mine))
			}
			t.Logf("%d of m's deliveries at a bound %d appends in: %d returned, %d unreturned",
				len(mine), filled, len(d.Returned[group]), len(unreturned))
		})
	}
}

func offsetsOf(fs []store.InFlight) []uint64 {
	out := make([]uint64, len(fs))
	for i, f := range fs {
		out[i] = f.Offset
	}
	return out
}

// The switch's three writes to a group's returned list, on both providers,
// each surviving a restart:
//   - Return: a delivery handed over and never written leaves the member's
//     table for its group's returned list in one step, charged as the row it
//     becomes; with no cursor left to return it to, nothing is written;
//   - Lend: a hand-over to a clean member writes its returned row and moves
//     the cursor together, so a restart - which ends that session - gives it
//     back, and a forget takes it out once it is acknowledged;
//   - EndShareCursorIfUnheld: an UNSUBSCRIBE by the last holder ends the
//     group with its returned rows, and one another session holds is kept.
//
// On memory, the provider's quota agrees with the store's count after each,
// since a memory provider's bound is that quota.
func TestTheSwitchsWritesToAReturnedList(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			agree := func(string) {}
			if ms, ok := p.sessions.(*store.Sessions); ok {
				q := store.NewQuota(1<<30, 0)
				ms.SetQuota(q)
				agree = func(when string) {
					t.Helper()
					lg, _ := ms.Log()
					if q.Bytes() != ms.Bytes()+lg.Bytes() {
						t.Errorf("%s: the quota holds %d bytes, and the sessions and their log count %d",
							when, q.Bytes(), ms.Bytes()+lg.Bytes())
					}
				}
			}
			for _, id := range []string{"m", "n"} {
				if err := p.sessions.Save(aMember(id, group)); err != nil {
					t.Fatalf("save %s: %v", id, err)
				}
			}
			for i := range 10 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if err := p.sessions.Lend(group, 3, []uint64{2}); !errors.Is(err, store.ErrNoShareGroup) {
				t.Errorf("a lend from a group with no cursor was answered %v, want ErrNoShareGroup", err)
			}
			if err := p.sessions.CreateShareCursor(group, 1); err != nil {
				t.Fatalf("the cursor: %v", err)
			}
			f := store.InFlight{Offset: 1, PacketID: 1, QoS: 1, State: store.MessageSent, Group: group}
			if err := p.sessions.HandOver(group, 2, "m", 5, []store.InFlight{f}); err != nil {
				t.Fatalf("hand-over: %v", err)
			}

			before := p.sessions.Bytes()
			if err := p.sessions.Return("m", group, []store.InFlight{f}); err != nil {
				t.Fatalf("return: %v", err)
			}
			sameTable(t, p, "returned", "m")
			sameReturned(t, p, "returned", map[string][]uint64{group: {1}})
			agree("returned")
			if got, want := before-p.sessions.Bytes(), store.InFlightBytes(f)-store.ShareReturnedSize(group); got != want {
				t.Errorf("a return gave back %d bytes, want %d", got, want)
			}

			before = p.sessions.Bytes()
			if err := p.sessions.Lend(group, 5, []uint64{2, 3, 4}); err != nil {
				t.Fatalf("lend: %v", err)
			}
			if err := p.sessions.Lend(group, 5, []uint64{11}); !errors.Is(err, store.ErrPastNext) {
				t.Errorf("a lend past the log's next was answered %v, want ErrPastNext", err)
			}
			sameReturned(t, p, "lent", map[string][]uint64{group: {1, 2, 3, 4}})
			agree("lent")
			sameCursors(t, p, "lent", map[string]uint64{group: 5})
			if got, want := p.sessions.Bytes()-before, 3*store.ShareReturnedSize(group); got != want {
				t.Errorf("lending three counted %d bytes, want %d", got, want)
			}
			if err := p.sessions.SetShareCursor(group, 5, 3); err != nil {
				t.Fatalf("forget 3, acknowledged: %v", err)
			}
			agree("3 forgotten")
			p.restart(t)
			sameReturned(t, p, "restarted", map[string][]uint64{group: {1, 2, 4}})
			sameCursors(t, p, "restarted", map[string]uint64{group: 5})

			if ended, err := p.sessions.EndShareCursorIfUnheld(group); ended || err != nil {
				t.Errorf("a group two sessions hold was ended: %v, %v", ended, err)
			}
			for _, id := range []string{"m", "n"} {
				if err := p.sessions.Save(aMember(id)); err != nil {
					t.Fatalf("unsubscribe %s: %v", id, err)
				}
			}
			before = p.sessions.Bytes()
			if ended, err := p.sessions.EndShareCursorIfUnheld(group); !ended || err != nil {
				t.Fatalf("the last holder's unsubscribe did not end the group: %v, %v", ended, err)
			}
			sameCursors(t, p, "ended", nil)
			sameReturned(t, p, "ended", nil)
			if got, want := before-p.sessions.Bytes(), store.ShareCursorSize(group)+3*store.ShareReturnedSize(group); got != want {
				t.Errorf("the group's end gave back %d bytes, want %d", got, want)
			}
			if err := p.sessions.Return("m", group, []store.InFlight{f}); !errors.Is(err, store.ErrNoShareGroup) {
				t.Errorf("a return to an ended group was answered %v, want ErrNoShareGroup", err)
			}
		})
	}
}

// Remove's four statements each name as many offsets as they say and bind
// one parameter more, and a chunk takes the smallest that holds it.
func TestARemoveBindsOnlyTheStatementItsChunkNeeds(t *testing.T) {
	for _, st := range []struct {
		text  string
		slots int
	}{
		{removeStatement1, 1}, {removeStatement10, 10}, {removeStatement100, 100}, {removeStatement, removeChunk},
	} {
		if n := strings.Count(st.text, "?"); n != 1+st.slots {
			t.Errorf("the %d-offset statement binds %d parameters, want %d", st.slots, n, 1+st.slots)
		}
	}
	for _, c := range []struct{ offsets, slots int }{
		{1, 1}, {2, 10}, {10, 10}, {11, 100}, {100, 100}, {101, 500}, {500, 500},
	} {
		if got := removeSlots(c.offsets); got != c.slots {
			t.Errorf("a chunk of %d offsets takes the %d-offset statement, want %d", c.offsets, got, c.slots)
		}
	}
}

// varied is the i'th of a run of records that differ in size, so a count of
// bytes freed that is wrong for any one of them is wrong in total.
func varied(i int) store.Record {
	r := store.Record{MessageID: fmt.Sprint("m-", i), Topic: fmt.Sprint("events/", i%7),
		Payload: bytes.Repeat([]byte{byte(i)}, i%37), QoS: 1}
	if i%3 == 0 {
		r.Headers = []store.Header{{Key: "k", Value: strings.Repeat("v", i%11)}}
	}
	return r
}

// **Remove takes out exactly the offsets it is given**, at every size of
// statement and across chunks: every other offset of a run, so a delete by
// range would take the ones between; a repeat and an offset already gone
// placed at the edges of the 500-offset chunks; each counted once, and the
// bytes freed exactly theirs. The ones not given stay, and the counters are
// what the file says after a restart.
func TestARemoveTakesExactlyTheOffsetsItIsGiven(t *testing.T) {
	p := sqliteBroadcast(t)
	sizes := map[uint64]int64{}
	const total = 4600
	for i := range total {
		r := varied(i)
		got, err := p.log.Append(r)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		sizes[got.Offset] = store.RecordSize(r)
	}
	bytesHeld := p.log.Bytes()
	next := uint64(2) // offset 1 stays throughout, so the floor never moves
	for _, n := range []int{1, 2, 10, 11, 100, 101, 500, 501, 1001} {
		var named, between []uint64
		var freed int64
		for range n {
			named = append(named, next)
			between = append(between, next+1)
			freed += sizes[next]
			next += 2
		}
		if next >= total {
			t.Fatalf("the run is too short for %d more", n)
		}
		asked := append([]uint64(nil), named...)
		if n > 1 {
			// A repeat and an offset already gone, at a chunk's edge where
			// there is one, otherwise at the end.
			at := min(removeChunk-1, len(asked)-1)
			asked = slices.Insert(asked, at, named[at], 2)
		}
		removed, got, err := p.log.Remove(asked...)
		if err != nil || removed != n || got != freed {
			t.Fatalf("%d offsets: removed %d freeing %d (%v), want %d freeing %d", n, removed, got, err, n, freed)
		}
		bytesHeld -= freed
		if recs, err := p.log.ReadAt(named...); err != nil || len(recs) != 0 {
			t.Errorf("%d offsets: %d of the ones given are still held (%v)", n, len(recs), err)
		}
		if recs, err := p.log.ReadAt(between...); err != nil || len(recs) != n {
			t.Errorf("%d offsets: %d of the %d between them are held (%v), want every one", n, len(recs), n, err)
		}
		if p.log.Bytes() != bytesHeld || p.log.Floor() != 1 {
			t.Errorf("%d offsets: %d bytes and floor %d, want %d and 1", n, p.log.Bytes(), p.log.Floor(), bytesHeld)
		}
	}
	p.restart(t)
	if p.log.Bytes() != bytesHeld || p.log.Floor() != 1 || p.log.Next() != total+1 {
		t.Errorf("after a restart: %d bytes, floor %d, next %d; want %d, 1, %d",
			p.log.Bytes(), p.log.Floor(), p.log.Next(), bytesHeld, total+1)
	}
}

// **A Remove that fails leaves the log as it was**, file and memory, when
// it fails after a chunk has been deleted and when it fails at the counter
// row after every chunk: one transaction, and the floor and the bytes held
// move only once it has committed.
func TestAFailedRemoveLeavesTheLogAsItWas(t *testing.T) {
	for _, c := range []struct{ name, trigger string }{
		{"in its second chunk", `CREATE TRIGGER refuse BEFORE DELETE ON records
			WHEN OLD."offset" = 700 BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`},
		{"at the counter row", `CREATE TRIGGER refuse BEFORE UPDATE ON channels
			WHEN NEW.name = '` + store.BroadcastLog + `' BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := tempPath(t)
			db := open(t, path)
			lg, err := db.Broadcast()
			if err != nil {
				t.Fatal(err)
			}
			const total = 1200
			for i := range total {
				if _, err := lg.Append(varied(i)); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			all := make([]uint64, total)
			for i := range all {
				all[i] = uint64(i + 1)
			}
			floor, next, held := lg.Floor(), lg.Next(), lg.Bytes()
			if _, err := db.db.Exec(c.trigger); err != nil {
				t.Fatal(err)
			}
			// From the floor, so a Remove that got as far as the counters
			// would have moved the floor too.
			removed, freed, err := lg.Remove(all[:1000]...)
			if err == nil || !strings.Contains(err.Error(), "refused by the test") || removed != 0 || freed != 0 {
				t.Fatalf("the remove was answered %d freeing %d (%v), want the trigger's refusal", removed, freed, err)
			}
			unchanged := func(when string, lg *Log) {
				t.Helper()
				if recs, err := lg.ReadAt(all...); err != nil || len(recs) != total {
					t.Errorf("%s: %d of the %d records held (%v)", when, len(recs), total, err)
				}
				if lg.Floor() != floor || lg.Next() != next || lg.Bytes() != held {
					t.Errorf("%s: floor %d, next %d, %d bytes; want %d, %d, %d",
						when, lg.Floor(), lg.Next(), lg.Bytes(), floor, next, held)
				}
			}
			unchanged("refused", lg)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = open(t, path)
			t.Cleanup(func() { _ = db.Close() })
			lg, err = db.Broadcast()
			if err != nil {
				t.Fatal(err)
			}
			unchanged("refused, restarted", lg)
		})
	}
}

// combinedSaver is the one write a durable member's SUBSCRIBE to a group with
// no cursor is: its record and the group's cursor, both or neither.
type combinedSaver interface {
	SaveWithShareCursors(sess store.Session, cursor uint64, groups []string) error
}

// RFC 0003 "Broadcast", RFC 0002 SUBACK `0x97`: a group's cursor is made with
// the record of its first durable member, in one write. On both providers:
//   - kept, the record holds the filter and the group has its cursor, each
//     counted;
//   - a group that has a cursor keeps it where it was;
//   - a group the record does not hold is refused ErrNoShareGroup, and
//     nothing is kept.
//
// And refused inside the write, nothing of it is kept: on memory the quota,
// asked once for the record and the cursor, refuses the two where the
// record alone fits; on sqlite a cursor insert the database refuses takes the
// record's upsert back with it, and the count of who holds the group too.
func TestASessionAndItsGroupsCursorAreKeptTogetherOrNotAtAll(t *testing.T) {
	const group = "$share/g/jobs"
	for _, p := range []*sessionsWithLog{memorySessionsWithLog(t), sqliteSessionsWithLog(t)} {
		t.Run(p.name, func(t *testing.T) {
			for i := range 4 {
				if _, err := p.log(t).Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			s := p.sessions.(combinedSaver)
			before := p.sessions.Bytes()
			if err := s.SaveWithShareCursors(aMember("other"), 2, []string{group}); !errors.Is(err, store.ErrNoShareGroup) {
				t.Errorf("a cursor for a group the record does not hold was answered %v, want ErrNoShareGroup", err)
			}
			if _, ok, _ := p.sessions.Get("other"); ok {
				t.Error("the refused write kept the record")
			}
			sameCursors(t, p, "refused for a group not held", nil)

			m := aMember("m", group)
			if err := s.SaveWithShareCursors(m, 3, []string{group}); err != nil {
				t.Fatalf("keep the member and its group's cursor: %v", err)
			}
			if got, ok, err := p.sessions.Get("m"); err != nil || !ok || len(got.Subscriptions) != 2 {
				t.Errorf("the member's record is %+v (%v, %v), want it holding the group", got, ok, err)
			}
			sameCursors(t, p, "kept", map[string]uint64{group: 3})
			if got, want := p.sessions.Bytes()-before, store.SessionSize(m)+store.ShareCursorSize(group); got != want {
				t.Errorf("the write counts %d bytes, want %d", got, want)
			}
			if err := s.SaveWithShareCursors(aMember("n", group), 5, []string{group}); err != nil {
				t.Fatalf("a second member: %v", err)
			}
			sameCursors(t, p, "a second member's write", map[string]uint64{group: 3})
		})
	}

	t.Run("memory refused for room", func(t *testing.T) {
		s := store.NewSessions()
		lg, _ := s.Log()
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
			t.Fatalf("append: %v", err)
		}
		probe := store.NewQuota(0, 0)
		s.SetQuota(probe)
		m := aMember("m", group)
		room := probe.Bytes() + store.SessionSize(m) + store.ShareCursorSize(group) - 1
		q := store.NewQuota(room, 0)
		s.SetQuota(q)
		held, bytes := q.Bytes(), s.Bytes()
		err := s.SaveWithShareCursors(m, 1, []string{group})
		if !errors.Is(err, store.ErrProviderFull) {
			t.Fatalf("a record and a cursor one byte past the bound were answered %v, want ErrProviderFull", err)
		}
		if _, ok, _ := s.Get("m"); ok {
			t.Error("the refused write kept the record")
		}
		if c, _ := s.ShareCursors(); len(c) != 0 {
			t.Errorf("the refused write kept cursors %v", c)
		}
		if q.Bytes() != held || s.Bytes() != bytes {
			t.Errorf("the refused write moved the quota to %d (from %d) and the store to %d (from %d)",
				q.Bytes(), held, s.Bytes(), bytes)
		}
		// The record alone fits: the refusal was of the two together.
		if err := s.Save(m); err != nil {
			t.Fatalf("the record alone: %v", err)
		}
	})

	t.Run("sqlite refused inside the transaction", func(t *testing.T) {
		db := open(t, tempPath(t))
		s := sessionsOn(t, db)
		lg, err := s.Log()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if _, err := db.db.Exec(`CREATE TRIGGER refuse_group BEFORE INSERT ON share_groups
			BEGIN SELECT RAISE(ABORT, 'refused by the test'); END`); err != nil {
			t.Fatal(err)
		}
		bytes := s.Bytes()
		err = s.SaveWithShareCursors(aMember("m", group), 1, []string{group})
		if err == nil || !strings.Contains(err.Error(), "refused by the test") {
			t.Fatalf("the write was answered %v, want the trigger's refusal", err)
		}
		if _, ok, _ := s.Get("m"); ok {
			t.Error("the record's upsert outlived the refused cursor insert")
		}
		var rows int
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&rows); err != nil || rows != 0 {
			t.Errorf("the file holds %d session rows (%v), want none", rows, err)
		}
		if c, _ := s.ShareCursors(); len(c) != 0 {
			t.Errorf("the refused write kept cursors %v", c)
		}
		if s.Bytes() != bytes {
			t.Errorf("the refused write moved the count to %d, from %d", s.Bytes(), bytes)
		}
		if _, err := db.db.Exec(`DROP TRIGGER refuse_group`); err != nil {
			t.Fatal(err)
		}
		// Nobody holds the group: the refused write's count went back too.
		if err := s.CreateShareCursor(group, 1); !errors.Is(err, store.ErrNoShareGroup) {
			t.Errorf("after the refused write a cursor was answered %v, want ErrNoShareGroup", err)
		}
	})
}
