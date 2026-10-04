package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// encoded is a snapshot of all three channel kinds, with the awkward
// values a format gets wrong: an empty payload, a payload holding the
// bytes that terminate a string in other formats, several headers, and a
// record whose timestamp was never set.
func sample() *Snapshot {
	t0 := time.Unix(1700000000, 123456789)
	return &Snapshot{
		Writer:    "0.1.0-test",
		WrittenAt: t0,
		Channels: []ChannelState{
			{
				Name: "events", Kind: KindAppend, Next: 13, Floor: 9,
				Records: []Record{
					{Offset: 9, MessageID: "m-9", Topic: "events/a", Payload: []byte{0, 1, 0xff, '\n'}, Timestamp: t0},
					{Offset: 10, MessageID: "m-10", Topic: "events/b", Payload: nil,
						Headers: []Header{{Key: "saguin-id", Value: "m-10"}, {Key: "z", Value: ""}, {Key: "a", Value: "1"}}},
					{Offset: 11, MessageID: "m-11", Topic: "events/c", Payload: []byte("plain")},
					// Arrived over a bridge and carrying nothing else, which
					// is the record a store decides to write no side-channel
					// for. Lost here, an out rule forwards it back where it
					// came from after the next restart.
					{Offset: 12, MessageID: "m-12", Topic: "events/d", Payload: []byte("from a link"),
						Bridge: "head-office"},
				},
				Positions: []Position{
					{Reader: "reader-1", Offset: 10, LastSeen: t0, ExpiresIn: time.Hour},
				},
			},
			{
				Name: "device-state", Kind: KindLatest, Next: 6, Floor: 1,
				Records: []Record{
					{Offset: 3, MessageID: "m-3", Topic: "device-state/x", Payload: []byte("on")},
					// A value as the retained store keeps one: the QoS it was
					// published at is what it is delivered at, so a snapshot
					// that drops it downgrades the delivery after a restart.
					{Offset: 4, MessageID: "m-4", Topic: "device-state/y", Payload: []byte("off"),
						QoS: 1, Retain: true, Publisher: "device-7"},
					// Properties sent present and empty: MQTT-3.3.2-15, -16, -20.
					{Offset: 5, MessageID: "m-5", Topic: "device-state/z", Payload: []byte("x"),
						ContentTypeEmpty: true, ResponseTopicEmpty: true, CorrelationDataEmpty: true},
				},
			},
			{
				Name: "jobs", Kind: KindQueue, Next: 3, Floor: 1,
				Items: []Item{
					{Record: Record{Offset: 1, MessageID: "j-1", Topic: "jobs/one", Payload: []byte("work")},
						Attempts: 2, FirstSeen: t0, LastSeen: t0.Add(time.Minute)},
					{Record: Record{Offset: 2, MessageID: "j-2", Topic: "jobs/two"}},
				},
			},
			{Name: "jobs__dlq", Kind: KindAppend, Next: 1, Floor: 1},
		},
	}
}

func encode(t *testing.T, s *Snapshot) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

func TestSnapshotRoundTrip(t *testing.T) {
	in := sample()
	got, err := DecodeSnapshot(encode(t, in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Writer != in.Writer {
		t.Errorf("writer = %q, want %q", got.Writer, in.Writer)
	}
	if !got.WrittenAt.Equal(in.WrittenAt) {
		t.Errorf("written at = %v, want %v", got.WrittenAt, in.WrittenAt)
	}
	if len(got.Channels) != len(in.Channels) {
		t.Fatalf("got %d channels, want %d", len(got.Channels), len(in.Channels))
	}

	for i := range in.Channels {
		w, g := in.Channels[i], got.Channels[i]
		if w.Name != g.Name || w.Kind != g.Kind || w.Next != g.Next || w.Floor != g.Floor {
			t.Errorf("channel %d = %+v, want %+v", i, g, w)
		}
		if len(w.Records) != len(g.Records) {
			t.Fatalf("channel %q: got %d records, want %d", w.Name, len(g.Records), len(w.Records))
		}
		for j := range w.Records {
			assertRecord(t, w.Name, g.Records[j], w.Records[j])
		}
		if len(w.Items) != len(g.Items) {
			t.Fatalf("channel %q: got %d items, want %d", w.Name, len(g.Items), len(w.Items))
		}
		for j := range w.Items {
			assertRecord(t, w.Name, g.Items[j].Record, w.Items[j].Record)
			if g.Items[j].Attempts != w.Items[j].Attempts {
				t.Errorf("channel %q item %d: attempts = %d, want %d", w.Name, j, g.Items[j].Attempts, w.Items[j].Attempts)
			}
			if !g.Items[j].FirstSeen.Equal(w.Items[j].FirstSeen) || !g.Items[j].LastSeen.Equal(w.Items[j].LastSeen) {
				t.Errorf("channel %q item %d: first/last seen not preserved", w.Name, j)
			}
		}
		if len(w.Positions) != len(g.Positions) {
			t.Fatalf("channel %q: got %d positions, want %d", w.Name, len(g.Positions), len(w.Positions))
		}
		for j := range w.Positions {
			if g.Positions[j].Reader != w.Positions[j].Reader ||
				g.Positions[j].Offset != w.Positions[j].Offset ||
				!g.Positions[j].LastSeen.Equal(w.Positions[j].LastSeen) ||
				g.Positions[j].ExpiresIn != w.Positions[j].ExpiresIn {
				t.Errorf("channel %q position %d = %+v, want %+v", w.Name, j, g.Positions[j], w.Positions[j])
			}
		}
	}
}

func assertRecord(t *testing.T, ch string, got, want Record) {
	t.Helper()
	for _, d := range recordDiff(got, want) {
		t.Errorf("channel %q offset %d: %s", ch, want.Offset, d)
	}
}

// recordDiff names every field of two records that differs.
//
// **The whole Record, never a chosen few of its fields.** A helper that picks
// what to compare passes over the field a format drops: this one used to check
// seven of sixteen, and the QoS a record was published at went missing from every snapshot
// with the round trip passing. Walking the struct means a field added tomorrow
// is compared the day it is added. Headers are compared as a sequence, the
// publisher's order and repeats included, because DeepEqual walks the slice.
//
// A time is compared as an instant and an empty slice equals a nil one. The
// sqlite package's tests walk records the same way; a test helper in one
// package cannot be reached from the other.
func recordDiff(got, want Record) []string {
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

// One state must encode to one sequence of bytes. Header maps are the
// place this goes wrong: with map order in the file, two snapshots of an
// unchanged channel differ and nobody can tell a file that changed from
// one that was merely rewritten.
func TestSnapshotEncodingIsDeterministic(t *testing.T) {
	first := encode(t, sample())
	for i := range 20 {
		if !bytes.Equal(first, encode(t, sample())) {
			t.Fatalf("the same state encoded to different bytes on attempt %d", i)
		}
	}
}

// Invariant 15: after a restart no record is in flight.
//
// RestoreQueue is handed the delivery state directly rather than through
// the decoder, because the decoder never reads those fields out of a file
// in the first place - going through it would test nothing. RestoreQueue
// is the enforcement point, and it holds for any caller.
func TestQueueItemsLoadAvailable(t *testing.T) {
	s := sample()
	s.Channels[2].Items[0].State = Leased
	s.Channels[2].Items[0].Epoch = 7
	s.Channels[2].Items[0].DeliveryID = "deadbeef"
	s.Channels[2].Items[0].Holder = "worker-1"
	s.Channels[2].Items[0].LeaseUntil = time.Now().Add(time.Hour)

	q := RestoreQueue(s.Channels[2].Next, s.Channels[2].Items)

	total, inflight := q.Depth()
	if total != 2 || inflight != 0 {
		t.Fatalf("depth = %d total, %d in flight; want 2 and 0", total, inflight)
	}
	_, items := q.Export()
	it := items[0]
	if it.State != Available {
		t.Errorf("state = %v, want Available", it.State)
	}
	if it.Epoch != 0 || it.DeliveryID != "" || it.Holder != "" || !it.LeaseUntil.IsZero() {
		t.Errorf("a delivery survived the restore: %+v", it)
	}
	// Attempts and the history a dead-lettered record carries do survive.
	if it.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", it.Attempts)
	}
	if it.FirstSeen.IsZero() || it.LastSeen.IsZero() {
		t.Errorf("first/last seen were lost: %+v", it)
	}
}

// Every prefix of a good file must be refused, and none may panic. This
// is the crash-mid-write case: the temp file exists, it stopped
// somewhere, and it must never load as a shorter but plausible snapshot.
func TestTruncatedSnapshotIsRefused(t *testing.T) {
	good := encode(t, sample())
	for i := range good {
		s, err := DecodeSnapshot(good[:i])
		if err == nil {
			t.Fatalf("a file truncated to %d of %d bytes decoded into %d channels", i, len(good), len(s.Channels))
		}
	}
}

// Bytes past the last channel are refused. They mean the file says one
// thing and holds another - a writer that ran twice into the same file,
// or a section this broker did not know to read. The checksum cannot
// catch it, because a writer that produced the extra bytes checksummed
// them too, so the trailing bytes here are resealed to test the check
// that does catch it rather than the one that does not.
func TestTrailingBytesAreRefused(t *testing.T) {
	good := encode(t, sample())
	long := append(append([]byte(nil), good[:len(good)-trailerSize]...), 0xAA, 0, 0, 0, 0)
	reseal(long)

	if _, err := DecodeSnapshot(long); err == nil {
		t.Fatal("a file with a byte past the last channel decoded")
	}
}

func TestDamagedSnapshotIsRefused(t *testing.T) {
	good := encode(t, sample())
	for i := range good {
		damaged := append([]byte(nil), good...)
		damaged[i] ^= 0x40
		if _, err := DecodeSnapshot(damaged); err == nil {
			t.Fatalf("a file with byte %d of %d flipped decoded without complaint", i, len(good))
		}
	}
}

// RFC 0004 "The file format": the broker writes and reads one format, and a
// file of any other is refused by name - the format it holds, the build that
// wrote it, and the format this broker reads - rather than read as something.
// Every kind of file the broker writes, stamped with every other format up to
// one past its own; the sweep counts what it refused, so an empty list of
// kinds or formats cannot pass.
func TestEveryOtherFormatIsRefusedByName(t *testing.T) {
	const writer = "0.1.0-test"
	rec := Record{Offset: 1, MessageID: "m-1", Topic: "state/a", Payload: []byte("on")}
	encoded := func(enc func(*bytes.Buffer) error) []byte {
		t.Helper()
		var buf bytes.Buffer
		if err := enc(&buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		return buf.Bytes()
	}
	kinds := []struct {
		name   string
		file   []byte
		decode func([]byte) error
	}{
		{"channels", encode(t, sample()),
			func(b []byte) error { _, err := DecodeSnapshot(b); return err }},
		{"sessions", encoded(func(w *bytes.Buffer) error {
			return (&SessionsSnapshot{Writer: writer, Sessions: []SessionState{{Session: Session{Client: "c"}}}}).Encode(w)
		}), func(b []byte) error { _, err := DecodeSessions(b); return err }},
		{"broadcast log", encoded(func(w *bytes.Buffer) error {
			return (&BroadcastSnapshot{Writer: writer, Next: 2, Floor: 1, Records: []Record{rec}}).Encode(w)
		}), func(b []byte) error { _, err := DecodeBroadcast(b); return err }},
	}

	refused := 0
	for _, k := range kinds {
		// The control: the file as written reads, so each refusal below is
		// the format's doing.
		if err := k.decode(k.file); err != nil {
			t.Fatalf("%s: the file as written was refused: %v", k.name, err)
		}
		for v := uint32(1); v <= formatVersion+1; v++ {
			if v == formatVersion {
				continue
			}
			b := append([]byte(nil), k.file...)
			binary.LittleEndian.PutUint32(b[8:12], v)
			reseal(b)
			err := k.decode(b)
			if err == nil {
				t.Errorf("%s: a format %d file was read", k.name, v)
				continue
			}
			refused++
			// Built from the constants rather than written out, so raising
			// the format does not fail this for anything about the error.
			for _, want := range []string{fmt.Sprintf("format %d,", v), writer, fmt.Sprintf("reads format %d", formatVersion)} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s at format %d: %q does not say %q", k.name, v, err, want)
				}
			}
		}
	}
	if want := len(kinds) * int(formatVersion); refused != want || refused == 0 {
		t.Errorf("refused %d files, want %d: %d kinds at %d other formats", refused, want, len(kinds), formatVersion)
	}
	t.Logf("refused %d files: %d kinds at every other format from 1 to %d", refused, len(kinds), formatVersion+1)
}

// RFC 0004: a snapshot directory from a format this one would misread is
// refused by name rather than loaded - 16, whose saguin.shares file held a
// shared group's backlog, 17 and 18, whose sessions file held the messages a
// session was owed, and 19, whose records end before the session a copy is
// owed to. The file the broker does read, the sessions file, is what says so.
func TestASnapshotHoldingWhatThisFormatNoLongerReadsIsRefused(t *testing.T) {
	var buf bytes.Buffer
	if err := (&SessionsSnapshot{Writer: "0.1.0-test", Sessions: []SessionState{{Session: Session{Client: "c"}}}}).Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, v := range []uint32{16, 17, 18, 19} {
		b := append([]byte(nil), buf.Bytes()...)
		binary.LittleEndian.PutUint32(b[8:12], v)
		reseal(b)
		_, err := DecodeSessions(b)
		if err == nil {
			t.Errorf("a format %d sessions file was read, and what it could hold would be lost unsaid", v)
			continue
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("format %d,", v)) {
			t.Errorf("the refusal does not name format %d: %v", v, err)
		}
	}
}

func TestNotASnapshot(t *testing.T) {
	for _, b := range [][]byte{
		nil,
		[]byte("short"),
		bytes.Repeat([]byte("no"), 200),
	} {
		if _, err := DecodeSnapshot(b); err == nil {
			t.Errorf("%q decoded as a snapshot", b)
		}
	}
}

// A count is checked against the bytes that remain before the memory for
// it is taken. Without that, this test is an out-of-memory kill rather
// than a failure.
func TestImpossibleCountIsRefusedBeforeAllocating(t *testing.T) {
	b := encode(t, sample())
	binary.LittleEndian.PutUint32(b[headerSize:headerSize+4], 0xFFFFFFFF)
	reseal(b)

	if _, err := DecodeSnapshot(b); err == nil {
		t.Fatal("a channel count of four billion was accepted")
	}
}

// A file can be undamaged and still untrue - a writer with a bug, or a
// hand-edited file. Every rule here is one a later read depends on.
func TestImpossibleStateIsRefused(t *testing.T) {
	for name, break_ := range map[string]func(s *Snapshot){
		"floor past next":    func(s *Snapshot) { s.Channels[0].Floor = 99 },
		"next of zero":       func(s *Snapshot) { s.Channels[0].Next = 0; s.Channels[0].Records = nil },
		"floor of zero":      func(s *Snapshot) { s.Channels[0].Floor = 0 },
		"offset below floor": func(s *Snapshot) { s.Channels[0].Records[0].Offset = 1 },
		"offset past next":   func(s *Snapshot) { s.Channels[0].Records[2].Offset = 50 },
		"offsets not sorted": func(s *Snapshot) { s.Channels[0].Records[0].Offset = 11; s.Channels[0].Records[2].Offset = 9 },
		"unknown kind":       func(s *Snapshot) { s.Channels[0].Kind = 9 },
		"no name":            func(s *Snapshot) { s.Channels[0].Name = "" },
		"queue with records": func(s *Snapshot) { s.Channels[2].Records = []Record{{Offset: 1}} },
		"append with items":  func(s *Snapshot) { s.Channels[0].Items = []Item{{Record: Record{Offset: 10}}} },
		"position past next": func(s *Snapshot) { s.Channels[0].Positions[0].Offset = 99 },
	} {
		s := sample()
		break_(s)
		if _, err := DecodeSnapshot(encode(t, s)); err == nil {
			t.Errorf("%s: decoded without complaint", name)
		}
	}
}

func TestDropExpiredPositions(t *testing.T) {
	now := time.Unix(1700000000, 0)
	s := &Snapshot{Channels: []ChannelState{{
		Name: "events", Kind: KindAppend, Next: 10, Floor: 1,
		Positions: []Position{
			{Reader: MQTTReader("gone"), Offset: 3, LastSeen: now.Add(-2 * time.Hour), ExpiresIn: time.Hour},
			{Reader: MQTTReader("here"), Offset: 4, LastSeen: now.Add(-2 * time.Hour), ExpiresIn: 3 * time.Hour},
			{Reader: MQTTReader("clean-session"), Offset: 5, LastSeen: now.Add(-time.Second)},
			// MQTT's "never expires" is 0xFFFFFFFF seconds, which is 136 years.
			{Reader: MQTTReader("forever"), Offset: 6, LastSeen: now.Add(-2 * time.Hour), ExpiresIn: 0xFFFFFFFF * time.Second},
			// A reader with no MQTT session behind it, whose interval is
			// therefore zero - which is what "clean-session" above asked for
			// deliberately and this one cannot ask for at all. The two are
			// the same number and opposite intentions, and only the scheme
			// tells them apart.
			{Reader: "bridge:head-office/events", Offset: 7, LastSeen: now.Add(-2 * time.Hour)},
		},
	}}}

	if dropped := s.DropExpiredPositions(now); dropped != 2 {
		t.Fatalf("dropped %d positions, want 2", dropped)
	}
	kept := map[string]bool{}
	for _, p := range s.Channels[0].Positions {
		kept[p.Reader] = true
	}
	if !kept[MQTTReader("here")] || !kept[MQTTReader("forever")] {
		t.Errorf("kept %v, want the two sessions that had not expired", kept)
	}
	// The one that matters: a reader with no session is not swept. It was,
	// and its zero interval read as "expired before it was written" - so a
	// bridge saved at an offset was gone the next time the file was opened,
	// and would copy its whole channel again.
	if !kept["bridge:head-office/events"] {
		t.Errorf("a reader with no MQTT session was expired by a session rule: "+
			"its interval is zero because it has no session to expire, not because "+
			"it expired immediately (kept %v)", kept)
	}
	if len(kept) != 3 {
		t.Errorf("kept %v, want three", kept)
	}
}

// The stores must survive a round trip through the format, and the two
// counters that cannot be recomputed must come back as they were.
func TestStoresRoundTripThroughASnapshot(t *testing.T) {
	lg := NewLog()
	for range 5 {
		lg.Append(Record{Topic: "events/x", Payload: []byte("p")})
	}
	// A channel retention has emptied: every record gone, the floor and
	// the next offset well past 1. Recomputing either from the survivors
	// reads as a fresh channel, which is invariant 1 and invariant 9 both.
	// Reached into directly because retention does not exist yet.
	lg.records, lg.floor = nil, 6

	lt := NewLatest()
	lt.Set(Record{Topic: "device-state/a", Payload: []byte("1")})
	lt.Set(Record{Topic: "device-state/b", Payload: []byte("2")})
	lt.Set(Record{Topic: "device-state/a", Payload: []byte("3")})

	q := NewQueue()
	q.Enqueue(Record{Topic: "jobs/a"})
	q.Enqueue(Record{Topic: "jobs/b"})

	lnext, lfloor, lrecs := lg.Export()
	tnext, trecs := lt.Export()
	qnext, qitems := q.Export()

	s := &Snapshot{Writer: "0.1.0-test", WrittenAt: time.Now(), Channels: []ChannelState{
		{Name: "events", Kind: KindAppend, Next: lnext, Floor: lfloor, Records: lrecs},
		{Name: "device-state", Kind: KindLatest, Next: tnext, Floor: 1, Records: trecs},
		{Name: "jobs", Kind: KindQueue, Next: qnext, Floor: 1, Items: qitems},
	}}

	got, err := DecodeSnapshot(encode(t, s))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	lg2 := RestoreLog(got.Channels[0].Next, got.Channels[0].Floor, got.Channels[0].Records, got.Channels[0].Positions)
	if lg2.Next() != 6 || lg2.Floor() != 6 || lg2.Len() != 0 {
		t.Errorf("restored log: next %d, floor %d, %d records; want 6, 6 and 0",
			lg2.Next(), lg2.Floor(), lg2.Len())
	}
	// The next record must not reuse an offset a consumer already holds.
	if r, _ := lg2.Append(Record{Topic: "events/y"}); r.Offset != 6 {
		t.Errorf("the first record after a restore took offset %d, want 6", r.Offset)
	}
	if _, err := lg2.ReadFrom(5); err != ErrBelowFloor {
		t.Errorf("a read below the restored floor returned %v, want ErrBelowFloor", err)
	}

	lt2 := RestoreLatest(got.Channels[1].Next, got.Channels[1].Records)
	if lt2.Len() != 2 {
		t.Errorf("restored latest holds %d topics, want 2", lt2.Len())
	}
	vals, _ := lt2.Match(func(string) bool { return true })
	if len(vals) != 2 || string(vals[1].Payload) != "3" || vals[1].Topic != "device-state/a" {
		t.Errorf("restored latest = %+v; want b then the replaced value of a", vals)
	}
	if r, _ := lt2.Set(Record{Topic: "device-state/c"}); r.Offset != 4 {
		t.Errorf("the first value after a restore took offset %d, want 4", r.Offset)
	}

	q2 := RestoreQueue(got.Channels[2].Next, got.Channels[2].Items)
	if total, inflight := q2.Depth(); total != 2 || inflight != 0 {
		t.Errorf("restored queue: %d items, %d in flight; want 2 and 0", total, inflight)
	}
	if r, _ := q2.Enqueue(Record{Topic: "jobs/c"}); r.Offset != 3 {
		t.Errorf("the first job after a restore took offset %d, want 3", r.Offset)
	}
}

// reseal recomputes the trailing checksum after a test has changed the
// body, so that the test exercises the check it means to and not the
// checksum.
func reseal(b []byte) {
	binary.LittleEndian.PutUint32(b[len(b)-trailerSize:], crc32.Checksum(b[:len(b)-trailerSize], crcTable))
}

// **RFC 0004 states the snapshot format version, and it has gone stale
// twice.** The number is bumped in this file and the document is edited
// separately, so nothing connects them - a reader with an older file is
// then told the wrong version is current.
//
// Read out of the prose rather than asserted as a constant, because the
// prose is what a reader gets. It fails if the sentence is missing
// entirely, so rewording the paragraph out of existence is a failure rather
// than a silent pass.
func TestRFC0004StatesTheSnapshotFormatVersion(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "docs", "rfcs", "0004-storage.md"))
	if err != nil {
		t.Fatalf("read RFC 0004: %v", err)
	}
	m := regexp.MustCompile(`the broker writes (\d+)`).FindSubmatch(md)
	if m == nil {
		t.Fatalf("RFC 0004 no longer says which format version the broker writes; " +
			"it is the one place a reader learns it, so say it or move this check")
	}
	said, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatalf("RFC 0004's format version is not a number: %q", m[1])
	}
	if said != formatVersion {
		t.Errorf("RFC 0004 says the broker writes snapshot format %d and it writes %d - "+
			"a reader with an older file is told the wrong version is current",
			said, formatVersion)
	}
}

// A subscription's flags byte kept a request for deletions in bit 2, which
// nothing read, and sessions files written then carry it set. Such a file
// still reads, and the bit changes nothing: No Local and Retain As Published
// are bits 0 and 1 as they always were, whichever of them is set beside it.
func TestASessionsFileWithTheRetiredSubscriptionBitSetStillReads(t *testing.T) {
	for _, tc := range []struct {
		name                string
		noLocal, rap        bool
		written, withBitTwo byte
	}{
		{"Retain As Published", false, true, 0x02, 0x06},
		{"both", true, true, 0x03, 0x07},
		{"neither", false, false, 0x00, 0x04},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := &SessionsSnapshot{Writer: "test", WrittenAt: time.Unix(1_700_000_000, 0), Sessions: []SessionState{{
				Session: Session{Client: "c", ExpiryInterval: 60, Subscriptions: []SessionSubscription{
					{Filter: "flagged/#", QoS: 1, NoLocal: tc.noLocal, RetainAsPublished: tc.rap},
				}},
			}}}
			var buf bytes.Buffer
			if err := snap.Encode(&buf); err != nil {
				t.Fatalf("encode: %v", err)
			}
			b := buf.Bytes()
			// The filter, its QoS, then its flags byte.
			at := bytes.Index(b, []byte("flagged/#"))
			if at < 0 {
				t.Fatal("the subscription's filter is not in the file, so this proves nothing")
			}
			flags := at + len("flagged/#") + 1
			if b[flags] != tc.written {
				t.Fatalf("the flags byte is %#x, want %#x: this is not the byte the bit lives in", b[flags], tc.written)
			}
			b[flags] = tc.withBitTwo
			body := b[:len(b)-trailerSize]
			binary.LittleEndian.PutUint32(b[len(b)-trailerSize:], crc32.Checksum(body, crcTable))

			got, err := DecodeSessions(b)
			if err != nil {
				t.Fatalf("a sessions file with the retired bit set does not read: %v", err)
			}
			sub := got.Sessions[0].Session.Subscriptions[0]
			if sub.NoLocal != tc.noLocal || sub.RetainAsPublished != tc.rap {
				t.Errorf("read back No Local %v and Retain As Published %v, want %v and %v",
					sub.NoLocal, sub.RetainAsPublished, tc.noLocal, tc.rap)
			}
		})
	}
}

// RFC 0004 "The file format": a sessions file that parsed but cannot be
// true is refused at the start rather than restored - each of the states
// the decoder refuses, one at a time, from a file that decodes without them.
// The two a map cannot hold twice, and trailing bytes, are made in the bytes
// and the checksum sealed again, so each is refused for itself and not as a
// damaged file.
func TestAnImpossibleSessionsFileIsRefused(t *testing.T) {
	sample := func() *SessionsSnapshot {
		return &SessionsSnapshot{
			Sessions: []SessionState{{
				Session: Session{Client: "a", ExpiryInterval: 60,
					Subscriptions: []SessionSubscription{{Filter: "t/#", QoS: 1}}},
				Window:   5,
				InFlight: []InFlight{{Offset: 1, PacketID: 1, QoS: 1, State: MessageSent}},
			}},
			Groups:   map[string]uint64{"$share/grpA/t": 1, "$share/grpB/t": 1},
			Returned: map[string][]uint64{"$share/retA/t": {1}, "$share/retB/t": {2}},
		}
	}
	encodeSessions := func(t *testing.T, s *SessionsSnapshot) []byte {
		t.Helper()
		var buf bytes.Buffer
		if err := s.Encode(&buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		return buf.Bytes()
	}
	// reseal puts back a checksum over what a test changed in the bytes.
	reseal := func(b []byte) []byte {
		binary.LittleEndian.PutUint32(b[len(b)-trailerSize:], crc32.Checksum(b[:len(b)-trailerSize], crcTable))
		return b
	}
	rename := func(from, to string) func(t *testing.T) []byte {
		return func(t *testing.T) []byte {
			b := encodeSessions(t, sample())
			if n := bytes.Count(b, []byte(from)); n != 1 {
				t.Fatalf("%q is in the file %d times, want once", from, n)
			}
			return reseal(bytes.Replace(b, []byte(from), []byte(to), 1))
		}
	}
	changed := func(change func(s *SessionsSnapshot)) func(t *testing.T) []byte {
		return func(t *testing.T) []byte {
			s := sample()
			change(s)
			return encodeSessions(t, s)
		}
	}

	if _, err := DecodeSessions(encodeSessions(t, sample())); err != nil {
		t.Fatalf("the sample itself is refused (%v), so what follows proves nothing", err)
	}
	for _, tc := range []struct {
		name, want string
		file       func(t *testing.T) []byte
	}{
		{"two cursors for a group", "has two cursors", rename("grpB", "grpA")},
		{"two returned lists for a group", "has two returned lists", rename("retB", "retA")},
		{"bytes after the last session", "remain after the last session", func(t *testing.T) []byte {
			b := encodeSessions(t, sample())
			body, trailer := b[:len(b)-trailerSize], b[len(b)-trailerSize:]
			return reseal(append(append(append([]byte(nil), body...), 0), trailer...))
		}},
		{"a session naming no client", "names no client", changed(func(s *SessionsSnapshot) {
			s.Sessions[0].Session.Client = ""
		})},
		{"two sessions for one client", "has two sessions", changed(func(s *SessionsSnapshot) {
			s.Sessions = append(s.Sessions, s.Sessions[0])
		})},
		{"a subscription at QoS 3", "has QoS 3", changed(func(s *SessionsSnapshot) {
			s.Sessions[0].Session.Subscriptions[0].QoS = 3
		})},
		{"retain handling 3", "retain handling 3", changed(func(s *SessionsSnapshot) {
			s.Sessions[0].Session.Subscriptions[0].RetainHandling = 3
		})},
		{"a packet identifier in flight twice", "in flight twice", changed(func(s *SessionsSnapshot) {
			s.Sessions[0].InFlight = append(s.Sessions[0].InFlight, InFlight{Offset: 2, PacketID: 1, QoS: 1, State: MessageSent})
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSessions(tc.file(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("decoded with %v, want a refusal saying %q", err, tc.want)
			}
		})
	}
}
