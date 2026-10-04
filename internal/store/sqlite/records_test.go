package sqlite

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// logStore is what both implementations of an append channel offer. It is
// declared here so the tests below can drive the memory one and the SQLite
// one through the same script: two implementations of one contract are
// only worth having if they answer the same questions the same way, and
// nothing else in the build compares them.
type logStore interface {
	Append(store.Record) (store.Record, error)
	SetMaxBytes(int64)
	ReadFromN(offset uint64, max int) ([]store.Record, error)
	Next() uint64
	Floor() uint64
	FirstAtOrAfter(t time.Time) (uint64, bool, error)
	Position(reader string) (store.Position, bool, error)
	SavePosition(store.Position) error
	DropPosition(reader string) (bool, error)
}

var _ logStore = (*Log)(nil)
var _ logStore = (*store.Log)(nil)
var _ store.Appender = (*Log)(nil)

// latestStore is the same idea for a latest channel. Len is not on it:
// counting costs a scan in a store that keeps its records on a disk, so
// the two return different things and nothing in the broker asks either.
type latestStore interface {
	Set(store.Record) (store.Record, error)
	Delete(topic string) (bool, error)
	Get(topic string) (store.Record, bool, error)
	Next() uint64
	Trim(before, deletionsBefore time.Time) (int, int64, error)
	Match(match func(topic string) bool) ([]store.Record, error)
	MatchWithDeletions(match func(topic string) bool) ([]store.Record, error)
}

var _ latestStore = (*Latest)(nil)
var _ latestStore = (*store.Latest)(nil)

// records with the awkward values a store gets wrong: an empty payload, a
// payload holding the bytes that terminate a string elsewhere, several
// headers, no headers at all, and a timestamp nobody set.
func awkward() []store.Record {
	t0 := time.Unix(1700000000, 123456789)
	return []store.Record{
		{MessageID: "m-1", Topic: "events/a", Payload: []byte{0, 1, 0xff, '\n'}, Timestamp: t0},
		{MessageID: "m-2", Topic: "events/b", Payload: nil,
			Headers: []store.Header{{Key: "saguin-id", Value: "m-2"}, {Key: "z", Value: ""}, {Key: "a", Value: "1"}}},
		{MessageID: "m-3", Topic: "events/c", Payload: []byte("plain")},
		// **A record that arrived over a bridge, carrying nothing else.**
		// Both stores decide whether to write their side-channel from one
		// question (HasProps), so a record whose only entry there is the
		// bridge mark is the case that catches a store asking the old
		// question - and it is the common case, since most records carry no
		// publish properties at all. Lost here, an `out` rule forwards the
		// record back where it came from.
		{MessageID: "m-6", Topic: "events/f", Payload: []byte("from a link"),
			Bridge: "head-office"},
		// **A repeated name, and one whose order is not its sorted order.**
		// MQTT 5 allows both and a record has to carry both; the encoding
		// this replaced was a JSON object, which could express neither -
		// `tag` would have come back once, and `b` before `a`. The
		// comparison above is reflect.DeepEqual on the slice, so a store
		// that reordered or collapsed these fails here rather than at a
		// consumer months later.
		// **Every publish property at once, on a record every read path
		// returns**, with the three saguin keeps beside them: the QoS, the
		// retain flag and the publisher. A store that adds a column and
		// forgets one SELECT loses them on that path only, silently - so the
		// fixture carries them and the comparison below checks them, which
		// makes a missed statement a failing test rather than a consumer's
		// problem months later.
		{MessageID: "m-5", Topic: "events/e", Payload: []byte("props"),
			ContentType:     "application/json",
			ResponseTopic:   "replies/here",
			CorrelationData: []byte{0x59, 0x0d, 0x4f, 0xc2, 0x70, 0xa6},
			PayloadFormat:   1, PayloadFormatFlag: true,
			MessageExpiry: 3600,
			QoS:           2, Retain: true, Publisher: "device-7"},
		{MessageID: "m-4", Topic: "events/d", Payload: []byte("repeats"),
			Headers: []store.Header{
				{Key: "tag", Value: "first"},
				{Key: "b", Value: "2"},
				{Key: "tag", Value: "second"},
				{Key: "a", Value: "1"},
				{Key: "tag", Value: "third"},
			}},
	}
}

func assertSameRecords(t *testing.T, what string, got, want []store.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d records, want %d", what, len(got), len(want))
	}
	for i := range want {
		for _, d := range recordDiff(got[i], want[i]) {
			t.Errorf("%s record %d: %s", what, i, d)
		}
	}
}

// recordDiff names every field of two records that differs.
//
// **The whole Record, never a chosen few of its fields.** A helper that picks
// what to compare passes over the field a store drops: this one used to check
// thirteen of sixteen, so a memory snapshot lost the QoS a record was published
// at while both providers were reported to agree. Walking the struct means a
// field added tomorrow is compared the day it is added.
//
// A time is compared as an instant and an empty slice equals a nil one,
// because neither store promises which of the two it hands back.
func recordDiff(got, want store.Record) []string {
	var out []string
	g, w := reflect.ValueOf(got), reflect.ValueOf(want)
	for i := range w.NumField() {
		gf, wf := g.Field(i), w.Field(i)
		var same bool
		switch {
		case wf.Type() == reflect.TypeFor[time.Time]():
			same = gf.Interface().(time.Time).Equal(wf.Interface().(time.Time))
		case wf.Kind() == reflect.Slice && gf.Len() == 0 && wf.Len() == 0:
			same = true
		default:
			same = reflect.DeepEqual(gf.Interface(), wf.Interface())
		}
		if !same {
			out = append(out, fmt.Sprintf("%s is %#v, want %#v", w.Type().Field(i).Name, gf.Interface(), wf.Interface()))
		}
	}
	return out
}

// The two stores are one contract. Driven through the same script they
// must answer identically - the offsets they assign, the records they
// return, the counters they report, and the refusal below the floor.
//
// A divergence here is the failure that costs an afternoon: a deployment
// that behaves one way on memory and another way on SQLite, with nothing
// in either store looking wrong on its own.
func TestSQLiteAndMemoryAgree(t *testing.T) {
	db := open(t, tempPath(t))
	sq, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	mem := store.NewLog()

	stores := map[string]logStore{"sqlite": sq, "memory": mem}
	results := map[string][]store.Record{}

	for name, s := range stores {
		if s.Next() != 1 || s.Floor() != 1 {
			t.Fatalf("%s starts at next=%d floor=%d, want 1 and 1", name, s.Next(), s.Floor())
		}

		var stored []store.Record
		for _, r := range awkward() {
			got, err := s.Append(r)
			if err != nil {
				t.Fatalf("%s append: %v", name, err)
			}
			stored = append(stored, got)
		}
		// 1, 2, 3 … however many awkward() holds. Written out as three
		// fixed numbers this broke the moment a record was added to the
		// fixture, which says nothing about the store.
		for i, r := range stored {
			if want := uint64(i + 1); r.Offset != want {
				t.Errorf("%s assigned offset %d to record %d, want %d", name, r.Offset, i, want)
			}
		}
		if want := uint64(len(stored) + 1); s.Next() != want {
			t.Errorf("%s reports next=%d after %d appends, want %d",
				name, s.Next(), len(stored), want)
		}

		all, err := s.ReadFromN(1, 0)
		if err != nil {
			t.Fatalf("%s read: %v", name, err)
		}
		assertSameRecords(t, name, all, stored)

		// The bound exists so that draining a backlog does not copy the
		// remainder once per record.
		some, err := s.ReadFromN(2, 1)
		if err != nil {
			t.Fatalf("%s bounded read: %v", name, err)
		}
		if len(some) != 1 || some[0].Offset != 2 {
			t.Errorf("%s read from 2 limit 1 = %+v, want offset 2 alone", name, some)
		}

		results[name] = all
	}

	assertSameRecords(t, "sqlite against memory", results["sqlite"], results["memory"])
}

// filled is a Record with every field set to something other than its zero
// value, found by walking the struct rather than listed.
//
// **A field it cannot fill fails the test** rather than being left at zero,
// because a field left at zero is one a store can drop with nothing noticing -
// which is how a memory snapshot came to lose the QoS a record was published
// at while every comparison passed.
func filled(t *testing.T) store.Record {
	t.Helper()
	var r store.Record
	v := reflect.ValueOf(&r).Elem()
	for i := range v.NumField() {
		f, name := v.Field(i), v.Type().Field(i).Name
		switch {
		case f.Type() == reflect.TypeFor[time.Time]():
			f.Set(reflect.ValueOf(time.Unix(1700000000, 123456789)))
		case f.Type() == reflect.TypeFor[[]store.Header]():
			f.Set(reflect.ValueOf([]store.Header{{Key: "k-" + name, Value: "v-" + name}}))
		case f.Kind() == reflect.String:
			f.SetString("s-" + name)
		case f.Kind() == reflect.Slice && f.Type().Elem().Kind() == reflect.Uint8:
			f.SetBytes([]byte{0, 0xff, byte(i)})
		case f.Kind() == reflect.Bool:
			f.SetBool(true)
		case f.CanUint():
			// 1 is a valid QoS, a valid payload format and the first offset
			// either store assigns, so one value serves every unsigned field.
			f.SetUint(1)
		default:
			t.Fatalf("Record.%s is a %s, which this sweep cannot fill: teach it one, "+
				"so that both providers are checked for keeping it", name, f.Type())
		}
		if f.IsZero() {
			t.Fatalf("Record.%s is still its zero value after filling", name)
		}
	}
	return r
}

// **Every field of a record survives both providers, on every path that keeps
// one, whatever is added to Record later.** A memory provider encodes a record
// in one function for every kind of file it writes; a sqlite provider writes
// one JSON object for most fields and a table's own columns for the rest, in
// each table that holds records. A field any of those forgets is lost on that
// path only. The memory one forgot the QoS: a retained message published at
// QoS 1 came back from a snapshot at 0.
//
// The record is filled by walking the struct, so a field added tomorrow is in
// it tomorrow, and a kind of field the filler does not know fails rather than
// being skipped. **A path is excused a field only where its table has no place
// for it**, and the reason is what the table is: a publish waiting for its
// release, a group's backlog and a session's message are held under their own
// keys rather than under an offset in a channel (RFC 0004 "The schema").
func TestEveryRecordFieldSurvivesBothProviders(t *testing.T) {
	full := filled(t)
	fields := reflect.TypeFor[store.Record]().NumField()
	if fields == 0 {
		t.Fatal("store.Record has no fields to check")
	}
	now := time.Unix(1700000000, 0)

	db := open(t, tempPath(t))
	sessions, err := db.Sessions()
	if err != nil {
		t.Fatalf("sessions: %v", err)
	}
	// migrated carries one channel through Import into a database of its own,
	// since Import refuses one a channel store is open on, and back out
	// through Export.
	migrated := func(cs store.ChannelState) (store.ChannelState, error) {
		to := open(t, tempPath(t))
		if err := to.Import([]store.ChannelState{cs}); err != nil {
			return store.ChannelState{}, err
		}
		out, err := to.Export()
		if err != nil {
			return store.ChannelState{}, err
		}
		if len(out) != 1 {
			return store.ChannelState{}, fmt.Errorf("exported %d channels, want 1", len(out))
		}
		return out[0], nil
	}
	const unreleased = "a publish waiting for its PUBREL is given an offset only when it is written"

	paths := []struct {
		name   string
		exempt string // the field this path's table has no place for
		why    string
		run    func(r store.Record) (store.Record, error)
	}{
		{name: "a memory channel snapshot", run: func(r store.Record) (store.Record, error) {
			var buf bytes.Buffer
			in := &store.Snapshot{Channels: []store.ChannelState{
				{Name: "events", Kind: store.KindAppend, Next: 2, Floor: 1, Records: []store.Record{r}}}}
			if err := in.Encode(&buf); err != nil {
				return store.Record{}, err
			}
			out, err := store.DecodeSnapshot(buf.Bytes())
			if err != nil {
				return store.Record{}, err
			}
			return out.Channels[0].Records[0], nil
		}},
		// What a session is owed is the broadcast log, which a memory
		// provider's sessions carry across a stop in a file of its own.
		{name: "a memory broadcast log file", run: func(r store.Record) (store.Record, error) {
			var buf bytes.Buffer
			in := &store.BroadcastSnapshot{Next: 2, Floor: 1, Records: []store.Record{r}}
			if err := in.Encode(&buf); err != nil {
				return store.Record{}, err
			}
			out, err := store.DecodeBroadcast(buf.Bytes())
			if err != nil {
				return store.Record{}, err
			}
			if len(out.Records) != 1 {
				return store.Record{}, fmt.Errorf("the file holds %d records, want 1", len(out.Records))
			}
			return out.Records[0], nil
		}},
		{name: "a memory channel's hold", exempt: "Offset", why: unreleased,
			run: func(r store.Record) (store.Record, error) {
				r.Offset = 0
				var buf bytes.Buffer
				in := &store.Snapshot{Channels: []store.ChannelState{{Name: "events", Kind: store.KindAppend,
					Next: 1, Floor: 1, Holds: []store.HeldPublish{
						{Exchange: store.Exchange{Client: "c", PacketID: 1}, Record: r, HeldAt: now}}}}}
				if err := in.Encode(&buf); err != nil {
					return store.Record{}, err
				}
				out, err := store.DecodeSnapshot(buf.Bytes())
				if err != nil {
					return store.Record{}, err
				}
				return out.Channels[0].Holds[0].Record, nil
			}},
		{name: "a sqlite append channel", run: func(r store.Record) (store.Record, error) {
			lg, err := db.Log("events")
			if err != nil {
				return store.Record{}, err
			}
			if _, err := lg.Append(r); err != nil {
				return store.Record{}, err
			}
			read, err := lg.ReadFromN(1, 1)
			if err == nil && len(read) != 1 {
				err = fmt.Errorf("read %d records, want 1", len(read))
			}
			if err != nil {
				return store.Record{}, err
			}
			return read[0], nil
		}},
		// The retained store's path on a sqlite provider, and the one broker
		// reader of a record's QoS today.
		{name: "a sqlite latest channel", run: func(r store.Record) (store.Record, error) {
			lt, err := db.Latest("state")
			if err != nil {
				return store.Record{}, err
			}
			if _, err := lt.Set(r); err != nil {
				return store.Record{}, err
			}
			v, ok, err := lt.Get(r.Topic)
			if err == nil && !ok {
				err = fmt.Errorf("no value for %q", r.Topic)
			}
			return v, err
		}},
		{name: "a sqlite queue", run: func(r store.Record) (store.Record, error) {
			q, err := db.Queue("jobs")
			if err != nil {
				return store.Record{}, err
			}
			if _, err := q.Enqueue(r); err != nil {
				return store.Record{}, err
			}
			offered, err := q.Offer(1, now)
			if err == nil && len(offered) != 1 {
				err = fmt.Errorf("offered %d jobs, want 1", len(offered))
			}
			if err != nil {
				return store.Record{}, err
			}
			return offered[0].Record, nil
		}},
		{name: "a sqlite hold, released into its channel", exempt: "Offset", why: unreleased,
			run: func(r store.Record) (store.Record, error) {
				lg, err := db.Log("held")
				if err != nil {
					return store.Record{}, err
				}
				e := store.Exchange{Client: "c", PacketID: 1}
				if err := lg.Hold(e, r, now); err != nil {
					return store.Record{}, err
				}
				back, ok, err := lg.ReleaseHold(e)
				if err == nil && !ok {
					err = fmt.Errorf("nothing held for %+v", e)
				}
				if err != nil {
					return store.Record{}, err
				}
				read, err := lg.ReadFromN(back.Offset, 1)
				if err == nil && len(read) != 1 {
					err = fmt.Errorf("read %d records at the released offset, want 1", len(read))
				}
				if err != nil {
					return store.Record{}, err
				}
				return read[0], nil
			}},
		// And a sqlite provider's, in the session store's database.
		{name: "a sqlite broadcast log", run: func(r store.Record) (store.Record, error) {
			lg, err := sessions.Log()
			if err != nil {
				return store.Record{}, err
			}
			got, err := lg.Append(r)
			if err != nil {
				return store.Record{}, err
			}
			read, err := lg.ReadAt(got.Offset)
			if err == nil && len(read) != 1 {
				err = fmt.Errorf("read %d records at offset %d, want 1", len(read), got.Offset)
			}
			if err != nil {
				return store.Record{}, err
			}
			return read[0], nil
		}},
		{name: "a migrated append channel", run: func(r store.Record) (store.Record, error) {
			cs, err := migrated(store.ChannelState{Name: "events", Kind: store.KindAppend, Next: 2, Floor: 1,
				Records: []store.Record{r}})
			if err != nil {
				return store.Record{}, err
			}
			return cs.Records[0], nil
		}},
		{name: "a migrated latest channel", run: func(r store.Record) (store.Record, error) {
			cs, err := migrated(store.ChannelState{Name: "state", Kind: store.KindLatest, Next: 2, Floor: 1,
				Records: []store.Record{r}})
			if err != nil {
				return store.Record{}, err
			}
			return cs.Records[0], nil
		}},
		{name: "a migrated queue", run: func(r store.Record) (store.Record, error) {
			cs, err := migrated(store.ChannelState{Name: "jobs", Kind: store.KindQueue, Next: 2, Floor: 1,
				Items: []store.Item{{Record: r, Attempts: 1}}})
			if err != nil {
				return store.Record{}, err
			}
			return cs.Items[0].Record, nil
		}},
	}

	exempted := 0
	for _, p := range paths {
		want := full
		// Each path's own message: a sqlite session store keeps a message
		// with an identity once, so two paths sharing one would read back
		// whichever wrote it first.
		want.MessageID = "s-MessageID/" + p.name
		got, err := p.run(want)
		if err != nil {
			t.Errorf("through %s: %v", p.name, err)
			continue
		}
		if p.name == "a sqlite session message kept inline" {
			want.MessageID = ""
		}
		if p.exempt != "" {
			f := reflect.ValueOf(&want).Elem().FieldByName(p.exempt)
			if !f.IsValid() {
				t.Fatalf("%s is excused Record.%s, which does not exist", p.name, p.exempt)
			}
			f.SetZero()
			reflect.ValueOf(&got).Elem().FieldByName(p.exempt).SetZero()
			exempted++
		}
		for _, d := range recordDiff(got, want) {
			t.Errorf("through %s: %s", p.name, d)
		}
	}
	t.Logf("checked all %d fields of store.Record through %d paths, %d of them excused one field",
		fields, len(paths), exempted)
}

// The point of the whole provider: records are there after the process
// went away, without anybody having written a snapshot.
func TestRecordsSurviveClosingTheDatabase(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	var stored []store.Record
	for _, r := range awkward() {
		got, err := lg.Append(r)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		stored = append(stored, got)
	}
	if err := lg.SavePosition(store.Position{
		Reader: store.MQTTReader("reader"), Offset: 2,
		LastSeen: time.Now(), ExpiresIn: 24 * time.Hour,
	}); err != nil {
		t.Fatalf("save position: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = open(t, path)
	lg, err = db.Log("events")
	if err != nil {
		t.Fatalf("log after restart: %v", err)
	}

	back, err := lg.ReadFromN(1, 0)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	assertSameRecords(t, "after a restart", back, stored)

	// Invariant 9: the counter comes back where it was, so the next record
	// cannot take an offset a stored position already points at.
	// Counted from what was written rather than written down: these were
	// three fixed numbers, and adding a record to awkward() moved all of
	// them at once - a test that fails because its fixture grew is a test
	// nobody trusts the next time it goes red.
	written := uint64(len(stored))
	if lg.Next() != written+1 {
		t.Errorf("after a restart next=%d, want %d", lg.Next(), written+1)
	}
	if n, err := lg.Len(); err != nil || n != int(written) {
		t.Errorf("after a restart the channel holds %d records (err %v), want %d", n, err, written)
	}
	next, err := lg.Append(store.Record{Topic: "events/d"})
	if err != nil {
		t.Fatalf("append after restart: %v", err)
	}
	if next.Offset != written+1 {
		t.Errorf("the first record after a restart took offset %d, want %d",
			next.Offset, written+1)
	}

	p, ok, err := lg.Position(store.MQTTReader("reader"))
	if err != nil || !ok {
		t.Fatalf("the position did not survive: ok=%v err=%v", ok, err)
	}
	if p.Offset != 2 {
		t.Errorf("the consumer resumed at %d, want 2", p.Offset)
	}
}

// Invariant 1: a read from below the retention floor is refused, never
// served from the oldest survivor. Both stores must refuse it the same way,
// because the broker tells that error apart from a storage failure and
// does different things with each.
func TestAReadBelowTheFloorIsRefused(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if _, err := db.db.Exec(`UPDATE channels SET next = 40, floor = 40 WHERE name = 'events'`); err != nil {
		t.Fatalf("seeding a trimmed channel: %v", err)
	}
	// Reloaded, because the copy in memory is what the read consults.
	if err := lg.load(); err != nil {
		t.Fatalf("reload: %v", err)
	}

	if _, err := lg.ReadFromN(39, 0); err != store.ErrBelowFloor {
		t.Errorf("a read below the floor returned %v, want ErrBelowFloor", err)
	}
	// At the floor is not below it, and a caught-up consumer asking from
	// the floor of an empty channel gets nothing rather than an error.
	if _, err := lg.ReadFromN(40, 0); err != nil {
		t.Errorf("a read at the floor returned %v, want no error", err)
	}
}

// A position belongs to a session, and goes when the session does.
func TestDroppingAPosition(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	reader := store.MQTTReader("reader")

	if had, err := lg.DropPosition(reader); err != nil || had {
		t.Errorf("dropping a position nobody held: had=%v err=%v", had, err)
	}
	// Records up to the positions below, since a position past the log's
	// next is refused as it is written (store.ErrPastNext).
	for i := range 9 {
		if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/a", Payload: []byte("x")}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := lg.SavePosition(store.Position{
		Reader: reader, Offset: 5, LastSeen: time.Now(), ExpiresIn: time.Hour,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Saving again replaces rather than adding: one row per reader and
	// channel, whatever happens.
	if err := lg.SavePosition(store.Position{
		Reader: reader, Offset: 9, LastSeen: time.Now(), ExpiresIn: time.Hour,
	}); err != nil {
		t.Fatalf("save again: %v", err)
	}
	p, ok, err := lg.Position(reader)
	if err != nil || !ok || p.Offset != 9 {
		t.Fatalf("position = %+v ok=%v err=%v, want offset 9", p, ok, err)
	}

	if had, err := lg.DropPosition(reader); err != nil || !had {
		t.Errorf("dropping a stored position: had=%v err=%v", had, err)
	}
	if _, ok, err := lg.Position(reader); err != nil || ok {
		t.Errorf("the position outlived its drop: ok=%v err=%v", ok, err)
	}
}

// A latest channel holds one record per topic. Replacing gives the value a
// new offset, so a consumer can tell which of two it holds is newer, and a
// zero-length payload is a delete rather than an empty value.
func TestLatestKeepsOneValuePerTopic(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}

	first, err := lt.Set(store.Record{Topic: "state/a", Payload: []byte("on")})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := lt.Set(store.Record{Topic: "state/b", Payload: []byte("off")}); err != nil {
		t.Fatalf("set: %v", err)
	}
	again, err := lt.Set(store.Record{Topic: "state/a", Payload: []byte("on again")})
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if again.Offset <= first.Offset {
		t.Errorf("a replaced value took offset %d, not past the %d it replaced", again.Offset, first.Offset)
	}
	if n := lt.Len(); n != 2 {
		t.Errorf("the channel holds %d topics after three publishes to two, want 2", n)
	}

	// Oldest first, so a subscriber receives state in the order it was
	// written: b was set before a was replaced.
	all, err := lt.Match(func(string) bool { return true })
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(all) != 2 || all[0].Topic != "state/b" || all[1].Topic != "state/a" {
		t.Fatalf("match returned %+v, want b then the replaced a", all)
	}
	if string(all[1].Payload) != "on again" {
		t.Errorf("state/a holds %q, want the replacement", all[1].Payload)
	}

	one, err := lt.Match(func(topic string) bool { return topic == "state/b" })
	if err != nil {
		t.Fatalf("filtered match: %v", err)
	}
	if len(one) != 1 || one[0].Topic != "state/b" {
		t.Errorf("a filter reaching one topic returned %+v", one)
	}

	if had, err := lt.Delete("state/b"); err != nil || !had {
		t.Errorf("deleting a topic that had a value: had=%v err=%v", had, err)
	}
	if had, err := lt.Delete("state/b"); err != nil || had {
		t.Errorf("deleting it twice: had=%v err=%v", had, err)
	}
	if n := lt.Len(); n != 1 {
		t.Errorf("the channel holds %d topics after a delete, want 1", n)
	}

	// And all of it after a restart, which is the first of the three things
	// RFC 0003 says latest adds over MQTT retained messages.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = open(t, path)
	lt, err = db.Latest("state")
	if err != nil {
		t.Fatalf("latest after restart: %v", err)
	}
	back, err := lt.Match(func(string) bool { return true })
	if err != nil {
		t.Fatalf("match after restart: %v", err)
	}
	if len(back) != 1 || back[0].Topic != "state/a" || string(back[0].Payload) != "on again" {
		t.Errorf("after a restart the channel holds %+v, want state/a on again", back)
	}
	// The counter came back where it was, so a value published after the
	// restart is still ordered after one published before it - which is the
	// whole use a consumer has for a latest channel's offsets.
	after, err := lt.Set(store.Record{Topic: "state/c", Payload: []byte("new")})
	if err != nil {
		t.Fatalf("set after restart: %v", err)
	}
	if after.Offset <= again.Offset {
		t.Errorf("a value published after the restart took offset %d, not past the %d already used",
			after.Offset, again.Offset)
	}
}

// The same contract for a latest channel: publish, replace, delete, and
// read back current state. Both stores are driven through one script and
// their answers compared, because a latest channel is where the two are
// most likely to drift - one is a map and the other is a table, and only
// this says they behave as the same thing.
func TestSQLiteAndMemoryAgreeOnLatest(t *testing.T) {
	db := open(t, tempPath(t))
	sq, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}

	results := map[string][]store.Record{}
	for name, s := range map[string]latestStore{"sqlite": sq, "memory": store.NewLatest()} {
		first, err := s.Set(store.Record{MessageID: "a-1", Topic: "state/a", Payload: []byte("on")})
		if err != nil {
			t.Fatalf("%s set: %v", name, err)
		}
		if _, err := s.Set(store.Record{MessageID: "b-1", Topic: "state/b", Payload: []byte("off")}); err != nil {
			t.Fatalf("%s set: %v", name, err)
		}
		if _, err := s.Set(store.Record{MessageID: "c-1", Topic: "state/c", Payload: []byte("gone")}); err != nil {
			t.Fatalf("%s set: %v", name, err)
		}

		// Replacing gives the value a new offset, which is the only use a
		// consumer has for a latest channel's offsets.
		again, err := s.Set(store.Record{MessageID: "a-2", Topic: "state/a", Payload: []byte("on again")})
		if err != nil {
			t.Fatalf("%s replace: %v", name, err)
		}
		if again.Offset <= first.Offset {
			t.Errorf("%s: a replacement took offset %d, not past the %d it replaced", name, again.Offset, first.Offset)
		}

		had, err := s.Delete("state/c")
		if err != nil || !had {
			t.Errorf("%s deleting a topic that had a value: had=%v err=%v", name, had, err)
		}
		if had, err := s.Delete("state/c"); err != nil || had {
			t.Errorf("%s deleting it twice: had=%v err=%v", name, had, err)
		}

		all, err := s.Match(func(string) bool { return true })
		if err != nil {
			t.Fatalf("%s match: %v", name, err)
		}
		// Oldest first: b was set before a was replaced, so a comes second
		// even though it was written first.
		if len(all) != 2 || all[0].Topic != "state/b" || all[1].Topic != "state/a" {
			t.Fatalf("%s match returned %+v, want b then the replaced a", name, all)
		}
		if string(all[1].Payload) != "on again" || all[1].MessageID != "a-2" {
			t.Errorf("%s: state/a is %+v, want the replacement", name, all[1])
		}

		one, err := s.Match(func(topic string) bool { return topic == "state/b" })
		if err != nil {
			t.Fatalf("%s filtered match: %v", name, err)
		}
		if len(one) != 1 || one[0].Topic != "state/b" {
			t.Errorf("%s: a filter reaching one topic returned %+v", name, one)
		}

		results[name] = all
	}

	assertSameRecords(t, "sqlite against memory", results["sqlite"], results["memory"])

	// A latest channel is a map, so replacing a topic's value replaces its
	// row. Four publishes to three topics and one delete leave two rows -
	// not five with the current one somewhere among them, which is what a
	// table keyed by offset instead of by topic would hold.
	var rows int
	if err := db.db.QueryRow(
		`SELECT count(*) FROM latest_values WHERE channel = 'state'`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 2 {
		t.Errorf("the table holds %d rows for two live topics, want 2", rows)
	}
	// And nothing landed in the append table.
	if err := db.db.QueryRow(`SELECT count(*) FROM records`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 0 {
		t.Errorf("a latest channel wrote %d rows into the append table", rows)
	}
}

// One channel, one store. Each caches that channel's next offset and
// prepares its own statements, so two of them would be two writers with
// two copies of one counter - both assigning the same offset until one
// stopped being able to publish at all. Asking twice gives back the same
// one, and asking for the wrong kind is refused rather than silently
// building a second.
func TestOneStorePerChannel(t *testing.T) {
	db := open(t, tempPath(t))

	first, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	again, err := db.Log("events")
	if err != nil {
		t.Fatalf("log twice: %v", err)
	}
	if first != again {
		t.Error("asking twice for one append channel built two stores")
	}

	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if lt2, err := db.Latest("state"); err != nil || lt2 != lt {
		t.Errorf("asking twice for one latest channel gave %p and %p (err %v)", lt, lt2, err)
	}

	if _, err := db.Latest("events"); err == nil {
		t.Error("an append channel was handed out as a latest one")
	}
	if _, err := db.Log("state"); err == nil {
		t.Error("a latest channel was handed out as an append one")
	}

	// And the counter is still one counter: two references, one sequence.
	a, err := first.Append(store.Record{Topic: "events/a"})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	b, err := again.Append(store.Record{Topic: "events/b"})
	if err != nil {
		t.Fatalf("append through the second reference: %v", err)
	}
	if a.Offset != 1 || b.Offset != 2 {
		t.Errorf("offsets %d and %d through two references, want 1 and 2", a.Offset, b.Offset)
	}
}

// Seeking by time is one contract across both stores, and the answer is
// "at or after" rather than an exact match - the timestamp is the broker's
// receipt clock, so many records share a millisecond and nothing
// guarantees the moment asked for is one any record carries.
//
// The awkward row is the one that matters: a clock that stepped backwards
// leaves a record whose timestamp is older than its predecessor's. A
// binary search over that data can land past the true first match, which
// moves a consumer forward over records it asked for and reports success.
// Both stores answer by looking at every candidate for exactly that
// reason, and this is what says they agree.
func TestSQLiteAndMemoryAgreeOnSeekingByTime(t *testing.T) {
	db := open(t, tempPath(t))
	sq, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	mem := store.NewLog()

	base := time.Unix(1770000000, 0).UTC()
	// Offsets 1..5. Offset 2 carries a timestamp far ahead of the three
	// that follow it: the clock stepped backwards after it was written.
	// That shape is chosen because a binary search answers several of the
	// rows below wrongly on it - for 25s it lands on offset 5 and for 45s
	// it finds nothing, where the true answer is offset 2 both times.
	times := []time.Time{
		base,
		base.Add(50 * time.Second), // written before the clock stepped back
		base.Add(10 * time.Second),
		base.Add(20 * time.Second),
		base.Add(30 * time.Second),
	}

	for name, s := range map[string]logStore{"sqlite": sq, "memory": mem} {
		for i, ts := range times {
			if _, err := s.Append(store.Record{
				MessageID: "m", Topic: "events/x", Payload: []byte{byte(i)}, Timestamp: ts,
			}); err != nil {
				t.Fatalf("%s append: %v", name, err)
			}
		}
	}

	// The answer is the earliest OFFSET carrying a time at or after the one
	// asked for, which is not the same as the record closest to it. That is
	// the safe direction: a consumer placed there receives everything in
	// the window it asked for, and possibly a few older records as well.
	// Placing it later would skip records inside the window and report
	// success, which is the one failure a position must never produce.
	for _, tc := range []struct {
		name  string
		at    time.Time
		want  uint64
		found bool
	}{
		{"before everything", base.Add(-time.Hour), 1, true},
		{"exactly the first", base, 1, true},
		{"just after the first", base.Add(5 * time.Second), 2, true},
		{"inside the run the clock disturbed", base.Add(25 * time.Second), 2, true},
		{"past every record but the disturbed one", base.Add(45 * time.Second), 2, true},
		{"after everything", base.Add(time.Hour), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for name, s := range map[string]logStore{"sqlite": sq, "memory": mem} {
				got, found, err := s.FirstAtOrAfter(tc.at)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if found != tc.found {
					t.Fatalf("%s reported found=%v, want %v", name, found, tc.found)
				}
				if found && got != tc.want {
					t.Errorf("%s answered offset %d, want %d", name, got, tc.want)
				}
			}
		})
	}
}

// A point read answers with the current value, and says when there is none.
//
// The distinction is the whole reason the verb exists: a caller that
// subscribes to ask cannot tell an absent key from a slow one, and this is
// what replaces that guess with an answer. So absence is a result here and
// never an error, and the two are asserted separately.
func TestLatestReadsOneTopicOnItsOwn(t *testing.T) {
	db := open(t, tempPath(t))
	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	for _, r := range []store.Record{
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

	// The current value, not the first one: a latest channel keeps one per
	// topic and a read that returned a superseded value would be worse than
	// no read at all.
	if _, err := lt.Set(store.Record{Topic: "state/a", Payload: []byte("on again")}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if got, _, _ := lt.Get("state/a"); string(got.Payload) != "on again" {
		t.Errorf("after a replacement the read gave %q, want on again", got.Payload)
	}

	// A topic nobody has published to, and one whose value was deleted, are
	// the same answer - which is what a zero-length payload already means on
	// this channel type, so absence needs no new vocabulary.
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

// RFC 0003: a deletion on a latest channel is stored - a value with no
// payload and an offset of its own - and **nothing that reads this channel
// can tell**.
//
// It is held for one reader: a copy of this channel on another broker,
// which is handed current state when it subscribes and cannot learn from a
// set of current values that a topic is gone. Every other reader sees what
// it always saw.
//
// The two clocks are the other half. A value lives as long as it is the
// truth; a deletion only until everything reading this channel has seen it.
// Under one clock a channel keeping values for ever would keep a row for
// every device ever decommissioned, and nothing would bring the topic count
// down again.
func TestBothStoresHoldADeletionWithoutShowingIt(t *testing.T) {
	db := open(t, tempPath(t))
	sq, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	mem := store.NewLatest()

	old := time.Unix(1770000000, 0).UTC()
	recent := old.Add(48 * time.Hour)
	all := func(string) bool { return true }

	for name, s := range map[string]latestStore{"sqlite": sq, "memory": mem} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Set(store.Record{
				MessageID: "m-1", Topic: "state/a", Payload: []byte("here"), Timestamp: recent,
			}); err != nil {
				t.Fatalf("set: %v", err)
			}
			// The deletion: an ordinary Set with no payload.
			if _, err := s.Set(store.Record{
				MessageID: "m-2", Topic: "state/gone", Payload: nil, Timestamp: recent,
			}); err != nil {
				t.Fatalf("store a deletion: %v", err)
			}

			// **What a subscriber is sent.** Match is the state dump, and a
			// deleted topic is not part of current state - which is what
			// this channel promised before deletions were stored at all.
			state, err := s.Match(all)
			if err != nil {
				t.Fatalf("match: %v", err)
			}
			if len(state) != 1 || state[0].Topic != "state/a" {
				t.Fatalf("a subscriber would be sent %d records %v, want only state/a - a "+
					"deleted topic is not current state", len(state), state)
			}

			// And the one reader that is shown them: a copy of this channel
			// on another broker, which has to be told a topic is gone.
			withDeletions, err := s.MatchWithDeletions(all)
			if err != nil {
				t.Fatalf("match with deletions: %v", err)
			}
			if len(withDeletions) != 2 {
				t.Fatalf("a copy would be sent %d records %v, want both the value and the "+
					"deletion", len(withDeletions), withDeletions)
			}

			// **What a point read answers.** The deletion is found, and its
			// payload is empty - which is the same answer as a topic that
			// was never set, exactly as RFC 0003 already promised.
			got, found, err := s.Get("state/gone")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !found {
				t.Fatal("the deletion is not stored, so a copy of this channel could never " +
					"be told the topic is gone")
			}
			if len(got.Payload) != 0 {
				t.Errorf("the stored deletion carries %q, want no payload", got.Payload)
			}
			if got.Offset == 0 {
				t.Error("the deletion has no offset: it is the one thing on this channel a " +
					"consumer could not place against the value it replaced")
			}

			// **Two clocks.** The deletion is a day old against a deletion
			// period of an hour; the value is the same age against a value
			// period that has not come round. Only the deletion goes.
			removed, _, err := s.Trim(old, recent.Add(time.Hour))
			if err != nil {
				t.Fatalf("trim: %v", err)
			}
			if removed != 1 {
				t.Fatalf("trim removed %d, want 1: the deletion expires on its own clock and "+
					"the value on its own", removed)
			}
			if _, found, err := s.Get("state/gone"); err != nil || found {
				t.Errorf("the deletion is still here after its period: %v %v", found, err)
			}
			if _, found, err := s.Get("state/a"); err != nil || !found {
				t.Errorf("the value went with the deletion, so the two share a clock: %v %v",
					found, err)
			}
		})
	}
}

// **Replacing a value replaces the whole of it**, by either route, and the
// two providers hold the same record afterwards.
//
// They did not. This store's upsert wrote the payload, the headers, the
// offset and the timestamp on a replacement and left `props` at whatever
// the first value carried, so a topic held its old content type, response
// topic, correlation data, message expiry and payload format for ever
// after. The memory store replaces the record whole and always did, so the
// two providers answered differently about the same channel - and the one
// that mattered is content type, which is what tells a consumer how to read
// the bytes it was handed.
//
// Written as an agreement between the providers rather than as a list of
// columns, because the defect was a column nobody listed. A field added to
// a record and forgotten in the SQL fails here without anybody remembering
// to come back and add it to a check.
func TestBothProvidersHoldTheSameRecordAfterAReplacement(t *testing.T) {
	first := store.Record{
		Topic: "state/d1", MessageID: "m1", Payload: []byte("one"),
		Headers:   []store.Header{{Key: "schema", Value: "v1"}},
		Timestamp: time.Unix(1000, 0).UTC(),

		ContentType: "application/json", ResponseTopic: "reply/one",
		CorrelationData: []byte("c1"), PayloadFormat: 1, PayloadFormatFlag: true,
		MessageExpiry: 60,
	}
	// Every field differs, so a column left behind shows up whichever one
	// it is.
	second := store.Record{
		Topic: "state/d1", MessageID: "m2", Payload: []byte("two"),
		Headers:   []store.Header{{Key: "schema", Value: "v2"}},
		Timestamp: time.Unix(2000, 0).UTC(),

		ContentType: "application/x-protobuf", ResponseTopic: "reply/two",
		CorrelationData: []byte("c2"), PayloadFormat: 0, PayloadFormatFlag: true,
		MessageExpiry: 120,
	}

	for _, path := range []struct {
		name  string
		write func(t *testing.T, s latestStore, r store.Record, nth int)
	}{
		{"Set", func(t *testing.T, s latestStore, r store.Record, _ int) {
			t.Helper()
			if _, err := s.Set(r); err != nil {
				t.Fatalf("set: %v", err)
			}
		}},
	} {
		t.Run(path.name, func(t *testing.T) {
			mem := store.NewLatest()
			mem.SetQuota(store.NewQuota(0, 0))
			lt, err := open(t, tempPath(t)).Latest("state")
			if err != nil {
				t.Fatalf("latest: %v", err)
			}

			for nth, r := range []store.Record{first, second} {
				path.write(t, mem, r, nth)
				path.write(t, lt, r, nth)
			}

			fromMem, had, err := mem.Get("state/d1")
			if err != nil || !had {
				t.Fatalf("memory get: had=%v err=%v", had, err)
			}
			fromDB, had, err := lt.Get("state/d1")
			if err != nil || !had {
				t.Fatalf("sqlite get: had=%v err=%v", had, err)
			}

			// The counter this shape owes: both stores really took the
			// second record, so an agreement below is about the
			// replacement rather than about two stores that ignored it.
			if string(fromMem.Payload) != "two" || string(fromDB.Payload) != "two" {
				t.Fatalf("neither store holds the second value: memory %q, sqlite %q - "+
					"nothing below is about a replacement",
					fromMem.Payload, fromDB.Payload)
			}

			for _, f := range []struct {
				what     string
				mem, db  any
				wantFrom any
			}{
				{"content type", fromMem.ContentType, fromDB.ContentType, second.ContentType},
				{"response topic", fromMem.ResponseTopic, fromDB.ResponseTopic, second.ResponseTopic},
				{"correlation data", string(fromMem.CorrelationData), string(fromDB.CorrelationData), string(second.CorrelationData)},
				{"payload format", fromMem.PayloadFormat, fromDB.PayloadFormat, second.PayloadFormat},
				{"message expiry", fromMem.MessageExpiry, fromDB.MessageExpiry, second.MessageExpiry},
				{"message id", fromMem.MessageID, fromDB.MessageID, second.MessageID},
			} {
				if f.mem != f.db {
					t.Errorf("the two providers disagree about %s after a replacement "+
						"through %s: memory has %v, sqlite has %v. One channel, two "+
						"answers - and a consumer cannot tell which provider it is "+
						"reading", f.what, path.name, f.mem, f.db)
				}
				if f.db != f.wantFrom {
					t.Errorf("sqlite kept the %s of the value that was replaced, "+
						"through %s: has %v, want %v from the record that replaced it",
						f.what, path.name, f.db, f.wantFrom)
				}
			}
		})
	}
}

// **The count is kept in memory, so a restart has to find it again.**
// Nothing writes it to the database - a number written beside the rows is
// a number that can be written without them - so opening the channel
// counts the rows once and starts from that.
func TestALatestChannelFindsItsCountAgainAfterARestart(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	for _, topic := range []string{"state/a", "state/b", "state/c"} {
		if _, err := lt.Set(store.Record{Topic: topic, Payload: []byte("v")}); err != nil {
			t.Fatalf("set %s: %v", topic, err)
		}
	}
	// Replacing one must not move it, which is the whole reason the count
	// is kept rather than derived from the offsets.
	if _, err := lt.Set(store.Record{Topic: "state/a", Payload: []byte("again")}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if n := lt.Len(); n != 3 {
		t.Fatalf("before the restart the channel reports %d topics, want 3", n)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	again, err := open(t, path).Latest("state")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if n := again.Len(); n != 3 {
		t.Errorf("after the restart the channel reports %d topics, want 3: the count "+
			"is seeded at open and this one did not find its rows", n)
	}
	// And it carries on from there rather than starting again.
	if _, err := again.Set(store.Record{Topic: "state/d", Payload: []byte("v")}); err != nil {
		t.Fatalf("set after restart: %v", err)
	}
	if n := again.Len(); n != 4 {
		t.Errorf("the channel reports %d topics after one more arrived, want 4", n)
	}
}

// **A batch is where a count kept on the write path goes wrong**, because
// several publishes share one transaction and one of them inserts the row
// the others then find. Every writer here sends to the same topic that
// does not exist yet: exactly one insert happens, so the channel holds one
// topic however the batch was cut.
func TestOnePublishInABatchCanAddATopicAndTheRestCannot(t *testing.T) {
	db := open(t, tempPath(t))
	db.CommitGroup(20*time.Millisecond, 64)
	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}

	const writers = 48
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := lt.Set(store.Record{
				Topic: "state/one", Payload: []byte(fmt.Sprint(i))}); err != nil {
				t.Errorf("set: %v", err)
			}
		}()
	}
	wg.Wait()

	rows, err := lt.countRows()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	// The counter this shape owes: one topic is the answer whether or not
	// anything batched, so prove the offsets moved as far as the writers.
	if got := lt.Next(); got != writers+1 {
		t.Fatalf("the channel assigned offsets up to %d for %d publishes: they did "+
			"not all land, so one topic below proves nothing", got, writers)
	}
	if rows != 1 {
		t.Fatalf("the table holds %d rows for one topic", rows)
	}
	if n := lt.Len(); n != 1 {
		t.Errorf("the channel reports %d topics after %d publishes to one of them, "+
			"want 1: a batch counted an insert more than once", n, writers)
	}
}

// **A sweep that meets a row it cannot read leaves the write connection
// free**. Each of the four sweeps
// reads its rows on the provider's one write connection, and on a publish
// properties column it could not decode it returned with the rows still
// open: the connection was held for ever, and every later write - a
// publish, an acknowledgement, Close itself - waited for it. Such a row is
// an edited file or a damaged page, so it is rare; what it cost was the
// whole broker. Each arm plants the row, runs the sweep, sees the row
// reported, and then asks for a write that needs the connection. The
// control is an unreadable headers column, whose branch always closed.
func TestASweepThatMeetsAnUnreadableRowLeavesTheWriteConnectionFree(t *testing.T) {
	old := time.Now().Add(-2 * time.Hour)
	rec := func(expiry uint32) store.Record {
		return store.Record{MessageID: "m1", Topic: "t", Payload: []byte("x"), Timestamp: old, MessageExpiry: expiry}
	}
	for _, arm := range []struct {
		name string
		run  func(t *testing.T, db *DB) error
	}{
		{"Log.Trim, publish properties unreadable", func(t *testing.T, db *DB) error {
			lg := mustLog(t, db, "events")
			if _, err := lg.Append(rec(0)); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, `UPDATE records SET props = '{' WHERE channel = 'events'`)
			_, _, err := lg.Trim(time.Now(), 0)
			return err
		}},
		{"Latest.Trim, publish properties unreadable", func(t *testing.T, db *DB) error {
			lt, err := db.Latest("state")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := lt.Set(rec(0)); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, `UPDATE latest_values SET props = '{' WHERE channel = 'state'`)
			_, _, err = lt.Trim(time.Now(), time.Time{})
			return err
		}},
		{"Latest.TrimExpired, publish properties SQLite reads and Go does not", func(t *testing.T, db *DB) error {
			lt, err := db.Latest("retained")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := lt.Set(rec(1)); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, `UPDATE latest_values SET props = '{"me":1,"ct":7}' WHERE channel = 'retained'`)
			_, _, err = lt.TrimExpired(time.Now())
			return err
		}},
		{"Queue.ExpireOlderThan, publish properties unreadable", func(t *testing.T, db *DB) error {
			q, err := db.Queue("jobs")
			if err != nil {
				t.Fatal(err)
			}
			dlq := mustLog(t, db, "jobs__dlq")
			if _, err := q.Enqueue(rec(0)); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, `UPDATE queue_items SET props = '{' WHERE channel = 'jobs'`)
			_, err = q.ExpireOlderThan(time.Now(), store.DeadLetter{Log: dlq,
				Record: func(it store.Item) store.Record { return it.Record }})
			return err
		}},
		{"control: Log.Trim, headers unreadable", func(t *testing.T, db *DB) error {
			lg := mustLog(t, db, "events")
			if _, err := lg.Append(rec(0)); err != nil {
				t.Fatal(err)
			}
			mustExec(t, db, `UPDATE records SET headers = '{' WHERE channel = 'events'`)
			_, _, err := lg.Trim(time.Now(), 0)
			return err
		}},
	} {
		t.Run(arm.name, func(t *testing.T) {
			db, err := OpenBoundedWithReads(tempPath(t), "0.1.0-test", 0, 0, 0)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			// Abandon rather than Close: with the connection held, Close
			// would wait for it too, and the failure below would be a hang.
			t.Cleanup(func() { _ = db.Abandon() })
			if err := arm.run(t, db); err == nil || !strings.Contains(err.Error(), "not readable") {
				t.Fatalf("the sweep did not report the unreadable row (%v), so nothing below is about one", err)
			}
			done := make(chan error, 1)
			go func() { _, err := db.Log("after"); done <- err }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the write after the sweep failed: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("a write after the sweep waited 3s for the provider's one write connection: " +
					"the sweep returned with its rows open")
			}
		})
	}
}

// **Every result set is closed on every return**, by syntax. A *sql.Rows holds its connection until it
// is closed, the write pool is one connection, and database/sql has no
// finalizer for rows, so a result left open on an early return holds the
// provider for ever. Every result of a Query in the package must be closed
// by a defer in the function that opened it, or be handed to a function
// that defers the close of the rows it takes - found here by what its body
// does, not by a list of names. Hand-closing on every branch is not
// accepted: it was right at four sites and one edit away from wrong, and
// four sweeps were already wrong.
func TestEveryResultSetIsClosedOnEveryReturn(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package: %v", err)
	}
	var files []*ast.File
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		files = append(files, f)
	}
	// defersClose reports whether body defers name.Close(), directly or
	// inside a deferred function literal.
	defersClose := func(body *ast.BlockStmt, name string) bool {
		found := false
		ast.Inspect(body, func(n ast.Node) bool {
			d, ok := n.(*ast.DeferStmt)
			if !ok {
				return true
			}
			ast.Inspect(d.Call, func(k ast.Node) bool {
				c, ok := k.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Close" {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == name {
						found = true
					}
				}
				return true
			})
			return true
		})
		return found
	}
	// The functions that take a *sql.Rows and defer its close.
	closers := map[string]bool{}
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			for _, p := range fd.Type.Params.List {
				star, ok := p.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				if sel, ok := star.X.(*ast.SelectorExpr); !ok || sel.Sel.Name != "Rows" {
					continue
				}
				for _, n := range p.Names {
					if defersClose(fd.Body, n.Name) {
						closers[fd.Name.Name] = true
					}
				}
			}
		}
	}
	var results, deferred, handed int
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			var body *ast.BlockStmt
			switch fn := n.(type) {
			case *ast.FuncDecl:
				body = fn.Body
			case *ast.FuncLit:
				body = fn.Body
			default:
				return true
			}
			if body == nil {
				return true
			}
			// The results opened in this body, not in a function literal
			// inside it, which is walked as its own.
			ast.Inspect(body, func(m ast.Node) bool {
				if lit, ok := m.(*ast.FuncLit); ok && lit.Body != body {
					return false
				}
				as, ok := m.(*ast.AssignStmt)
				if !ok || len(as.Rhs) != 1 || len(as.Lhs) == 0 {
					return true
				}
				call, ok := as.Rhs[0].(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Query" && sel.Sel.Name != "query" && sel.Sel.Name != "QueryContext") {
					return true
				}
				id, ok := as.Lhs[0].(*ast.Ident)
				if !ok {
					t.Errorf("%s: a result set assigned to something this sweep cannot follow",
						fset.Position(as.Pos()))
					return true
				}
				results++
				if defersClose(body, id.Name) {
					deferred++
					return true
				}
				passed := false
				ast.Inspect(body, func(k ast.Node) bool {
					c, ok := k.(*ast.CallExpr)
					if !ok {
						return true
					}
					var callee string
					switch fn := c.Fun.(type) {
					case *ast.Ident:
						callee = fn.Name
					case *ast.SelectorExpr:
						callee = fn.Sel.Name
					}
					if !closers[callee] {
						return true
					}
					for _, a := range c.Args {
						if ai, ok := a.(*ast.Ident); ok && ai.Name == id.Name {
							passed = true
						}
					}
					return true
				})
				if passed {
					handed++
					return true
				}
				t.Errorf("%s: %s is a result set nothing defers the close of: an early return leaves "+
					"it open, and it holds the provider's connection for ever", fset.Position(as.Pos()), id.Name)
				return true
			})
			return true
		})
	}
	t.Logf("%d result sets: %d closed by a defer, %d handed to %v", results, deferred, handed, closers)
	if results < 20 || len(closers) == 0 {
		t.Fatalf("only %d result sets and %d closing functions found: the sweep is not reading the "+
			"package it thinks it is", results, len(closers))
	}
}

// mustLog is db's log for name, or the test stops.
func mustLog(t *testing.T, db *DB, name string) *Log {
	t.Helper()
	lg, err := db.Log(name)
	if err != nil {
		t.Fatalf("log %s: %v", name, err)
	}
	return lg
}

// mustExec runs a statement on the write connection behind the package's
// back, which is how a test plants a row no writer here would write.
func mustExec(t *testing.T, db *DB, stmt string) {
	t.Helper()
	if _, err := db.db.Exec(stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}
