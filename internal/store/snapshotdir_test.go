package store

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func channels(names ...string) []*Snapshot {
	out := make([]*Snapshot, 0, len(names))
	for i, n := range names {
		out = append(out, &Snapshot{Channels: []ChannelState{{
			Name: n, Kind: KindAppend, Next: uint64(i) + 2, Floor: 1,
			Records: []Record{{Offset: uint64(i) + 1, Topic: n + "/x", Payload: []byte(n)}},
		}}})
	}
	return out
}

func saveTo(t *testing.T, d *Dir, at time.Time, snaps []*Snapshot) {
	t.Helper()
	if err := d.save(at, snaps); err != nil {
		t.Fatalf("save: %v", err)
	}
}

func TestDirRoundTrip(t *testing.T) {
	d := NewDir(t.TempDir(), "0.1.0-test")
	now := time.Unix(1700000000, 0)

	// A queue and its dead-letter channel share one file, named after the
	// queue, so the move between them cannot be recorded by half.
	snaps := channels("events", "device-state")
	snaps = append(snaps, &Snapshot{Channels: []ChannelState{
		{Name: "jobs", Kind: KindQueue, Next: 2, Floor: 1,
			Items: []Item{{Record: Record{Offset: 1, Topic: "jobs/a"}, Attempts: 3}}},
		{Name: "jobs__dlq", Kind: KindAppend, Next: 1, Floor: 1},
	}})
	saveTo(t, d, now, snaps)

	got, err := d.Load([]string{"events", "device-state", "jobs"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Warnings) != 0 {
		t.Errorf("a directory written in one go warned: %v", got.Warnings)
	}
	if len(got.Snapshots) != 3 {
		t.Fatalf("loaded %d snapshots, want 3", len(got.Snapshots))
	}
	for ch, s := range got.Snapshots {
		if !s.WrittenAt.Equal(now) {
			t.Errorf("channel %q stamped %v, want %v", ch, s.WrittenAt, now)
		}
		if s.Writer != "0.1.0-test" {
			t.Errorf("channel %q written by %q, want the directory's version", ch, s.Writer)
		}
	}
	jobs := got.Snapshots["jobs"]
	if len(jobs.Channels) != 2 || jobs.Channels[1].Name != "jobs__dlq" {
		t.Errorf("the queue's file holds %d channels, want the queue and its DLQ", len(jobs.Channels))
	}
	if jobs.Channels[0].Items[0].Attempts != 3 {
		t.Error("the queue item's attempt count did not survive")
	}
}

// A channel name is not a file name: "a.b" is a legal channel and ".." is
// a directory.
//
// **Four of the names below are ones the configuration refuses**: "..", a
// slash, a space, a percent. No operator can configure them, and they are
// tested anyway because this encoder is a defence rather than a formality.
// It must not depend on validName having run first - the same directory is
// read by the migration commands and by a broker whose name rule has
// changed under it - so tightening or loosening that rule cannot quietly
// make a file name unsafe. The two rules meet only at the length check
// further down, which configuration validation calls.
//
// The legal names are the other three. "a.b" is what makes the length
// check reachable at all, and "jobs__dlq" is a derived name the operator
// never writes.
func TestChannelNamesBecomeSafeFileNames(t *testing.T) {
	for channel, want := range map[string]string{
		"events":    "events.snapshot",
		"a.b":       "a%2Eb.snapshot",
		"jobs__dlq": "jobs__dlq.snapshot",

		// Refused by validName, encoded here regardless.
		"..":     "%2E%2E.snapshot",
		"site/a": "site%2Fa.snapshot",
		"100%":   "100%25.snapshot",
		"a b":    "a%20b.snapshot",
	} {
		got, err := SnapshotFileName(channel)
		if err != nil {
			t.Errorf("%q: %v", channel, err)
			continue
		}
		if got != want {
			t.Errorf("%q became %q, want %q", channel, got, want)
		}
		if strings.ContainsAny(strings.TrimSuffix(got, snapshotExt), "/.") {
			t.Errorf("%q became %q, which is not a safe file name", channel, got)
		}
	}

	// The encoding trebles a dot, so a name that fits the 128-byte channel
	// limit need not fit a file name - a hundred dots is a legal channel
	// name and a 309-character file. It must be refused where an operator
	// can act on it, not at the shutdown that loses the channel - hence an
	// error rather than a truncation.
	if _, err := SnapshotFileName(strings.Repeat(".", 100)); err == nil {
		t.Error("a channel name that cannot be stored was accepted")
	}

	d := NewDir(t.TempDir(), "test")
	saveTo(t, d, time.Unix(1700000000, 0), channels("a.b"))
	got, err := d.Load([]string{"a.b"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := got.Snapshots["a.b"]; !ok {
		t.Error("a channel whose name needed encoding did not come back")
	}
}

// The replace is what makes a crash mid-write survivable: the file that
// loads is either the old one or the new one, never a mixture.
func TestSaveReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	t0 := time.Unix(1700000000, 0)

	saveTo(t, d, t0, channels("events"))
	second := channels("events")
	second[0].Channels[0].Records[0].Payload = []byte("second")
	saveTo(t, d, t0.Add(time.Hour), second)

	got, err := d.Load([]string{"events"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if p := string(got.Snapshots["events"].Channels[0].Records[0].Payload); p != "second" {
		t.Errorf("payload = %q, want the second save", p)
	}
	if len(got.Warnings) != 0 {
		t.Errorf("warned: %v", got.Warnings)
	}

	// A temporary file left behind by a write that never finished is not
	// the snapshot and must never be loaded as one.
	tmp := filepath.Join(dir, "events"+snapshotExt+tempExt)
	if err := os.WriteFile(tmp, []byte("half a file"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = d.Load([]string{"events"})
	if err != nil {
		t.Fatalf("a leftover temporary file broke the load: %v", err)
	}
	if p := string(got.Snapshots["events"].Channels[0].Records[0].Payload); p != "second" {
		t.Errorf("payload = %q, want the second save still", p)
	}

	// And the next save overwrites it rather than appending to it.
	saveTo(t, d, t0.Add(2*time.Hour), channels("events"))
	if _, err := d.Load([]string{"events"}); err != nil {
		t.Fatalf("load after a save over a leftover temporary file: %v", err)
	}
}

// Invariant 14: a snapshot failing its integrity check is a startup
// error naming what was wrong, never an empty start.
func TestDamagedChannelFileRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	saveTo(t, d, time.Unix(1700000000, 0), channels("events", "device-state"))

	path := filepath.Join(dir, "events"+snapshotExt)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, damaged := range [][]byte{
		good[:len(good)/2],        // the write stopped partway
		append([]byte(nil), 1, 2), // not a snapshot at all
		flip(good, len(good)/3),   // a bad sector
	} {
		if err := os.WriteFile(path, damaged, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := d.Load([]string{"events", "device-state"})
		if err == nil {
			t.Fatalf("a damaged snapshot loaded into %d channels", len(got.Snapshots))
		}
		if !strings.Contains(err.Error(), path) {
			t.Errorf("the error does not name the file: %v", err)
		}
	}
}

func flip(b []byte, at int) []byte {
	out := append([]byte(nil), b...)
	out[at] ^= 0x40
	return out
}

// The case one file per channel trades away, and the manifest is what
// buys it back: a crash partway through a shutdown, leaving some channels
// from this run and some from the last.
func TestMixedAgesWarn(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	t0 := time.Unix(1700000000, 0)

	saveTo(t, d, t0, channels("events", "device-state", "jobs"))
	// A second shutdown that got the manifest and one channel out before
	// the power went.
	saveTo(t, d, t0.Add(time.Hour), channels("events"))

	got, err := d.Load([]string{"events", "device-state", "jobs"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Snapshots) != 3 {
		t.Fatalf("loaded %d snapshots, want 3 - the stale ones still load", len(got.Snapshots))
	}
	joined := strings.Join(got.Warnings, "\n")
	for _, want := range []string{`"device-state"`, `"jobs"`} {
		if !strings.Contains(joined, want) {
			t.Errorf("no warning names %s:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, `"events"`) {
		t.Errorf("the channel that did get written was warned about:\n%s", joined)
	}
	if len(got.Warnings) != 2 {
		t.Errorf("got %d warnings, want one per stale channel:\n%s", len(got.Warnings), joined)
	}
}

// The crash the manifest catches that comparing the channel files against
// each other cannot: nothing at all was written after it.
func TestNothingWrittenAfterTheManifestWarns(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	t0 := time.Unix(1700000000, 0)

	saveTo(t, d, t0, channels("events", "device-state"))
	// The next shutdown reached the manifest and stopped.
	m := &manifest{writer: "test", writtenAt: t0.Add(time.Hour),
		files: []string{"events" + snapshotExt, "device-state" + snapshotExt}}
	if err := d.replace(manifestName, m.encode); err != nil {
		t.Fatal(err)
	}

	got, err := d.Load([]string{"events", "device-state"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Warnings) != 2 {
		t.Fatalf("got %d warnings, want one per channel: %v", len(got.Warnings), got.Warnings)
	}
}

// A channel the manifest was writing whose file is not there at all: no
// timestamp can catch this one, because there is no file to stamp.
func TestMissingChannelFileWarns(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	saveTo(t, d, time.Unix(1700000000, 0), channels("events", "device-state"))

	if err := os.Remove(filepath.Join(dir, "device-state"+snapshotExt)); err != nil {
		t.Fatal(err)
	}
	got, err := d.Load([]string{"events", "device-state"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "device-state") {
		t.Fatalf("warnings = %v, want one naming device-state", got.Warnings)
	}
}

// A channel removed from the configuration is a file nobody opens. That
// is the whole reason for one file per channel, so it must stay quiet.
func TestRemovedChannelIsSilent(t *testing.T) {
	d := NewDir(t.TempDir(), "test")
	saveTo(t, d, time.Unix(1700000000, 0), channels("events", "retired"))

	got, err := d.Load([]string{"events"})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Warnings) != 0 {
		t.Errorf("dropping a channel from the configuration warned: %v", got.Warnings)
	}
	if len(got.Snapshots) != 1 {
		t.Errorf("loaded %d snapshots, want only the configured one", len(got.Snapshots))
	}
}

// The manifest holds no records, so none of its failures may stop a
// broker that has perfectly good channel files sitting beside it.
func TestManifestFailuresAreSurvivable(t *testing.T) {
	t.Run("fresh install says nothing", func(t *testing.T) {
		got, err := NewDir(t.TempDir(), "test").Load([]string{"events"})
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(got.Warnings) != 0 || len(got.Snapshots) != 0 {
			t.Errorf("an empty directory produced %v and %d snapshots", got.Warnings, len(got.Snapshots))
		}
	})

	for name, damage := range map[string]func(path string){
		"missing":   func(path string) { os.Remove(path) },
		"truncated": func(path string) { os.Truncate(path, 20) },
		"empty":     func(path string) { os.WriteFile(path, nil, 0o600) },
		"garbage":   func(path string) { os.WriteFile(path, []byte(strings.Repeat("x", 200)), 0o600) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			d := NewDir(dir, "test")
			saveTo(t, d, time.Unix(1700000000, 0), channels("events"))
			damage(filepath.Join(dir, manifestName))

			got, err := d.Load([]string{"events"})
			if err != nil {
				t.Fatalf("a %s manifest stopped the broker: %v", name, err)
			}
			if len(got.Snapshots) != 1 {
				t.Fatalf("a %s manifest lost the channel files: %d loaded", name, len(got.Snapshots))
			}
			if len(got.Warnings) != 1 {
				t.Errorf("a %s manifest produced %d warnings, want one: %v", name, len(got.Warnings), got.Warnings)
			}
		})
	}
}

// A manifest read as channels, or channels read as a manifest, would
// restore a broker to nothing. The kind byte is what stops it.
func TestKindsAreNotInterchangeable(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	saveTo(t, d, time.Unix(1700000000, 0), channels("events"))

	m, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeSnapshot(m); err == nil {
		t.Error("a manifest decoded as a channel snapshot")
	}

	if err := os.Rename(filepath.Join(dir, "events"+snapshotExt), filepath.Join(dir, manifestName)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.loadManifest(); err == nil {
		t.Error("a channel snapshot decoded as a manifest")
	}
}

// Every file lands, whatever order the writers finish in.
func TestParallelWritesAllLand(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")

	names := make([]string, 0, 32)
	for i := range 32 {
		names = append(names, "channel-"+string(rune('a'+i%26))+string(rune('0'+i/26)))
	}
	slices.Sort(names)
	names = slices.Compact(names)

	saveTo(t, d, time.Unix(1700000000, 0), channels(names...))

	got, err := d.Load(names)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Snapshots) != len(names) {
		t.Fatalf("loaded %d of %d channels", len(got.Snapshots), len(names))
	}
	if len(got.Warnings) != 0 {
		t.Errorf("warned: %v", got.Warnings)
	}
	for _, n := range names {
		s := got.Snapshots[n]
		if p := string(s.Channels[0].Records[0].Payload); p != n {
			t.Errorf("channel %q holds %q: a parallel write crossed its files", n, p)
		}
	}
}

func TestSaveReportsEveryFailure(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	saveTo(t, d, time.Unix(1700000000, 0), channels("a", "b", "c"))

	// Two of the three cannot be replaced. A save that stopped at the
	// first would leave the operator repairing one problem at a time.
	for _, n := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(dir, n+snapshotExt+tempExt), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	err := d.save(time.Unix(1700003600, 0), channels("a", "b", "c"))
	if err == nil {
		t.Fatal("a save that could not write two of three files reported success")
	}
	for _, n := range []string{"a", "b"} {
		if !strings.Contains(err.Error(), n+snapshotExt) {
			t.Errorf("the error does not name %s: %v", n, err)
		}
	}

	// The channels that could not be written still load, from the
	// snapshot that was already there.
	got, loadErr := d.Load([]string{"a", "b", "c"})
	if loadErr != nil {
		t.Fatalf("load after a failed save: %v", loadErr)
	}
	if len(got.Snapshots) != 3 {
		t.Errorf("a failed save cost %d channels their last good snapshot", 3-len(got.Snapshots))
	}
}

// LoadAll is how a directory is read with no configuration to say what is
// in it, which is what the migration has. It must find every channel by
// itself - a directory that reported only some of its channels would
// convert some of them, and the rule that a migration writes only into
// something empty means nobody could retry the rest.
func TestLoadAllFindsEveryChannelWithoutBeingTold(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")

	snaps := channels("events", "a.b", "audit")
	// A queue and its dead-letter channel share one file, and both have to
	// come back through it.
	snaps = append(snaps, &Snapshot{Channels: []ChannelState{
		{Name: "jobs", Kind: KindQueue, Next: 2, Floor: 1,
			Items: []Item{{Record: Record{Offset: 1, Topic: "jobs/x"}, Attempts: 1}}},
		{Name: "jobs__dlq", Kind: KindAppend, Next: 1, Floor: 1},
	}})
	saveTo(t, d, time.Now(), snaps)

	// The broadcast log's file sits beside the channels on a provider that
	// keeps sessions, and it is written in a channel section. It is still
	// not a channel: the migration commands read this directory through
	// LoadAll, and a log taken for one would arrive without the cursors that
	// give it meaning.
	if err := d.SaveBroadcast(&BroadcastSnapshot{Next: 2, Floor: 1,
		Records: []Record{{Offset: 1, MessageID: "m-1", Topic: "state/a", Payload: []byte("on")}}}); err != nil {
		t.Fatalf("save the broadcast log: %v", err)
	}

	// Things in the directory that are not channels. The lock is left by
	// every broker that has ever run here, and a temporary file by any whose
	// write did not finish.
	for _, name := range []string{"saguin.lock", "events.snapshot.tmp", "notes.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("not a channel"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	got, err := d.LoadAll()
	if err != nil {
		t.Fatalf("load all: %v", err)
	}

	var names []string
	for _, s := range got {
		for _, c := range s.Channels {
			names = append(names, c.Name)
		}
	}
	slices.Sort(names)
	want := []string{"a.b", "audit", "events", "jobs", "jobs__dlq"}
	if !slices.Equal(names, want) {
		t.Errorf("found %v, want %v", names, want)
	}

	// The queue's two channels stay in one snapshot, or the migration could
	// commit half the dead-letter move.
	for _, s := range got {
		if len(s.Channels) == 2 && s.Channels[0].Name == "jobs" {
			if s.Channels[1].Name != "jobs__dlq" {
				t.Errorf("the queue's file holds %q beside it, want jobs__dlq", s.Channels[1].Name)
			}
			return
		}
	}
	t.Error("the queue and its dead-letter channel did not come back in one snapshot")
}

// broadcastFile encodes one section as a broadcast log file, whatever it
// holds, so a test can hand the decoder what a correct writer never would.
func broadcastFile(t *testing.T, c ChannelState) []byte {
	t.Helper()
	var buf bytes.Buffer
	sum := crc32.New(crcTable)
	e := &encoder{w: io.MultiWriter(&buf, sum)}
	writeHeader(e, kindBroadcast, "test", time.Unix(1700000000, 0))
	c.encode(e)
	if err := seal(e, &buf, sum); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

// RFC 0004: the broadcast log is the file saguin.broadcast, and one that is
// damaged, or is not the log, is a startup error naming the file - never an
// empty log. An empty log starts its offsets again at 1 under every cursor a
// session holds, which is invariant 9's failure reached through invariant
// 14's.
func TestABroadcastFileThatIsNotTheLogIsRefused(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	path := filepath.Join(dir, broadcastName)

	if got, err := d.LoadBroadcast(); got != nil || err != nil {
		t.Fatalf("with no file, loaded %+v and %v; want no log and no error", got, err)
	}

	log := ChannelState{Name: BroadcastLog, Kind: KindAppend, Next: 3, Floor: 2,
		Records: []Record{{Offset: 2, MessageID: "m-2", Topic: "state/a", Payload: []byte("on")}}}
	good := broadcastFile(t, log)

	// The control: the same writer's good file loads, so each refusal below
	// is about what was changed and not about the helper.
	if err := os.WriteFile(path, good, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := d.LoadBroadcast()
	if err != nil {
		t.Fatalf("a good file was refused: %v", err)
	}
	if got.Next != 3 || got.Floor != 2 || len(got.Records) != 1 {
		t.Fatalf("a good file loaded next %d floor %d with %d records, want 3, 2 and 1",
			got.Next, got.Floor, len(got.Records))
	}

	var sessions bytes.Buffer
	if err := (&SessionsSnapshot{}).Encode(&sessions); err != nil {
		t.Fatal(err)
	}
	named, latest, items, cursor, floor := log, log, log, log, log
	named.Name = "events"
	latest.Kind = KindLatest
	items.Items = []Item{{Record: Record{Offset: 2, Topic: "jobs/a"}}}
	// A session's cursor is a reader position on the log, and one past the
	// next offset points at a message the log never had.
	cursor.Positions = []Position{{Reader: MQTTReader("c"), Offset: 4}}
	floor.Floor = 4

	for _, c := range []struct {
		why  string
		file []byte
		says string
	}{
		{"a write that stopped partway", good[:len(good)/2], "damaged"},
		{"a bad sector", flip(good, len(good)/3), "damaged"},
		{"a sessions file under the log's name", sessions.Bytes(), "not kind 6"},
		{"a channel's section", broadcastFile(t, named), "not the broadcast log"},
		{"a latest section under the log's name", broadcastFile(t, latest), "not the broadcast log"},
		{"queue items", broadcastFile(t, items), "queue items"},
		{"a cursor past the next offset", broadcastFile(t, cursor), "holds position 4, past the next offset 3"},
		{"a floor past the next offset", broadcastFile(t, floor), "past the next offset"},
	} {
		if err := os.WriteFile(path, c.file, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := d.LoadBroadcast()
		if err == nil {
			t.Errorf("%s: loaded as a log at next %d, floor %d", c.why, got.Next, got.Floor)
			continue
		}
		if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s: refused with %q, which should name %s and say %q", c.why, err, path, c.says)
		}
	}
}

// RFC 0004: the sessions and their broadcast log are written and read as one
// pair, because the sessions' cursors are positions on the log. The log is
// written first and made durable, so a write that fails there leaves the last
// pair exactly as it was; a snapshot with no log is refused rather than
// written beside whatever log the directory already holds.
func TestTheSessionsAndTheirLogAreWrittenAndReadAsOnePair(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	sessionsFile, logFile := filepath.Join(dir, sessionsName), filepath.Join(dir, broadcastName)

	s := NewSessions()
	if err := s.Save(Session{Client: "c", ExpiryInterval: 3600}); err != nil {
		t.Fatal(err)
	}
	lg, _ := s.Log()
	if _, err := lg.Append(Record{MessageID: "m-1", Topic: "state/a", Payload: []byte("on"), QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := lg.SavePosition(Position{Reader: MQTTReader("c"), Offset: 1}); err != nil {
		t.Fatal(err)
	}

	if err := d.SaveSessions(&SessionsSnapshot{Sessions: s.Export()}); err == nil {
		t.Error("sessions with no log were written")
	}
	for _, f := range []string{sessionsFile, logFile} {
		if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a refused save left %s behind: %v", f, err)
		}
	}

	if err := d.SaveSessions(s.Snapshot()); err != nil {
		t.Fatalf("save: %v", err)
	}
	before, err := os.ReadFile(sessionsFile)
	if err != nil {
		t.Fatal(err)
	}

	// The log's file cannot be replaced - a directory stands where it goes -
	// so the save stops there, and the sessions file is the last one.
	if err := s.Save(Session{Client: "d", ExpiryInterval: 3600}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(logFile); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(logFile, "in-the-way"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := d.SaveSessions(s.Snapshot()); err == nil {
		t.Fatal("a save whose log could not be written reported success")
	}
	after, err := os.ReadFile(sessionsFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("the sessions file was replaced although its log was not")
	}
	if err := os.RemoveAll(logFile); err != nil {
		t.Fatal(err)
	}

	// The log is always written first, so a sessions file alone has lost its
	// log, and the start refuses it rather than restarting the log's offsets
	// under its in-flight tables.
	if _, err := d.LoadSessions(); err == nil || !strings.Contains(err.Error(), logFile) {
		t.Errorf("a sessions file with no log loaded, or was refused without naming the log: %v", err)
	}

	// A log with no sessions file is a first stop that crashed between the
	// two: it reads, and its cursors are ones no session holds.
	if err := d.SaveSessions(s.Snapshot()); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := os.Remove(sessionsFile); err != nil {
		t.Fatal(err)
	}
	snap, err := d.LoadSessions()
	if err != nil || snap == nil || snap.Log == nil || len(snap.Sessions) != 0 || len(snap.Log.Positions) != 1 {
		t.Fatalf("a log alone loaded %+v, %v; want its one cursor and no sessions", snap, err)
	}
	back := RestoreSessions(snap)
	blg, _ := back.Log()
	if _, ok, _ := blg.Position(MQTTReader("c")); ok || blg.Next() != 2 {
		t.Errorf("a log alone restored next %d, cursor held %v; want 2 and no cursor", blg.Next(), ok)
	}

	// A damaged log stops the start, naming its file, even beside a good
	// sessions file.
	if err := d.SaveSessions(s.Snapshot()); err != nil {
		t.Fatalf("save: %v", err)
	}
	good, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logFile, flip(good, len(good)/2), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := d.LoadSessions(); err == nil || !strings.Contains(err.Error(), logFile) {
		t.Errorf("a damaged log beside good sessions loaded, or was refused without its name: %v", err)
	}
}

// Half a directory converted is worse than none of it, and the operator can
// act on a file that is named.
func TestLoadAllRefusesADamagedFile(t *testing.T) {
	dir := t.TempDir()
	d := NewDir(dir, "test")
	saveTo(t, d, time.Now(), channels("events", "audit"))

	path := filepath.Join(dir, "events.snapshot")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := os.WriteFile(path, flip(b, len(b)/2), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := d.LoadAll()
	if err == nil {
		t.Fatalf("a damaged file was accepted, returning %d snapshots", len(got))
	}
	if got != nil {
		t.Errorf("snapshots were returned beside the error: %d", len(got))
	}
	if !strings.Contains(err.Error(), "events.snapshot") {
		t.Errorf("the error does not name the file: %v", err)
	}
}

// A directory that is not there at all is not an error: it is what an
// operator naming the wrong path meets, and the caller says so better than
// this can.
func TestLoadAllOnADirectoryThatIsNotThere(t *testing.T) {
	d := NewDir(filepath.Join(t.TempDir(), "nothing"), "test")
	got, err := d.LoadAll()
	if err != nil {
		t.Fatalf("load all: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("found %d snapshots in a directory that does not exist", len(got))
	}
}

// The durability of a snapshot rests on three orderings, and until this
// existed all three rested on reading.
//
// **Remove the `f.Sync()` before the rename, remove the directory sync after
// it, or write the manifest last instead of first, and every other test in
// this repository still passes.** The bytes are in the page cache and a
// process that exits normally sees them all, so nothing short of a power cut
// tells the versions apart. The argument for each is in a comment at the
// line it defends, and a comment is not a check: two of the three are one
// line each, and a reader tidying up has no way to find out they matter.
//
// So this is the half that is reachable without staging a power cut. It does
// not prove the data survives one - nothing here can - it proves the broker
// asks the kernel for what the comments say it asks for, in the order they
// say. That is exactly the part a later change breaks silently.
//
// The instrument is `strace`, reading the syscalls themselves rather than
// the Go calls that make them. Anything closer to the code would be
// asserting that `os.File.Sync` was called, which is a different claim and a
// weaker one: what the comments promise is about what reaches the kernel.
func TestTheSnapshotWriteSyncsInTheOrderItsCommentsClaim(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("the syscall ordering behind snapshot durability is UNCHECKED on %s: "+
			"this reads a strace, which is Linux only", runtime.GOOS)
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("the syscall ordering behind snapshot durability is UNCHECKED: " +
			"strace is not installed. Everything else about snapshots is still tested; " +
			"what is not is that the fsyncs happen in the order snapshotdir.go's comments claim")
	}

	dir := t.TempDir()
	trace := filepath.Join(t.TempDir(), "trace.txt")
	cmd := exec.Command("strace",
		"-f", // the writes run on several threads
		"-y", // print the path beside every file descriptor
		"-e", "trace=openat,fsync,fdatasync,rename,renameat,renameat2,close",
		"-o", trace,
		os.Args[0], "-test.run", "^"+traceHelper+"$", "-test.v")
	cmd.Env = append(os.Environ(), traceDirEnv+"="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// A machine that cannot trace at all - a container with ptrace
		// restricted - is told apart from a helper that genuinely failed,
		// because they mean opposite things about the code under test.
		if strings.Contains(string(out), "ptrace") || strings.Contains(string(out), "Operation not permitted") {
			t.Skipf("the syscall ordering behind snapshot durability is UNCHECKED: "+
				"strace cannot trace on this machine: %s", out)
		}
		t.Fatalf("writing a snapshot under strace: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("PASS")) {
		t.Fatalf("the traced helper did not pass:\n%s", out)
	}

	events := parseTrace(t, trace, dir)
	if len(events) == 0 {
		// The counter every source-level check in this repository owes:
		// a trace nothing matched would satisfy every assertion below by
		// vacuum, which is a green tick over a rule that never ran.
		t.Fatalf("the trace holds no syscall on %s: this checked nothing", dir)
	}

	manifest := filepath.Join(dir, manifestName)
	channelFiles := []string{
		filepath.Join(dir, "events"+snapshotExt),
		filepath.Join(dir, "state"+snapshotExt),
	}

	// "The manifest goes first, and is made durable before any channel is
	// written." Written last, a crash before it leaves channel files stamped
	// later than the manifest - a state nothing can interpret.
	manifestRenamed := indexOf(events, "rename", manifest)
	if manifestRenamed < 0 {
		t.Fatalf("the manifest was never renamed into place\n%s", format(events))
	}
	firstChannelOpened := len(events)
	for _, f := range channelFiles {
		if i := indexOf(events, "open", f+tempExt); i >= 0 && i < firstChannelOpened {
			firstChannelOpened = i
		}
	}
	if firstChannelOpened < manifestRenamed {
		t.Errorf("a channel file was opened before the manifest was renamed into place: "+
			"a crash here leaves channels stamped later than the manifest, which nothing can read\n%s",
			format(events))
	}

	// The directory sync that makes the manifest's own rename durable, before
	// any channel is touched.
	if i := indexOf(events, "fsync", dir); i < 0 || i > firstChannelOpened || i < manifestRenamed {
		t.Errorf("the snapshot directory was not synced between the manifest's rename and "+
			"the first channel file: the manifest is written, synced, and its directory "+
			"entry need not survive\n%s", format(events))
	}

	// "The temporary file sits in the same directory because a rename is
	// atomic only within one filesystem." A rename across filesystems is not
	// atomic and Go's os.Rename would refuse it outright, so what this
	// guards is the quieter version: a temporary directory somewhere that
	// happens to be the same filesystem today and is a tmpfs tomorrow.
	for _, e := range events {
		if e.op != "rename" {
			continue
		}
		if filepath.Dir(e.path) != dir {
			t.Errorf("a snapshot was renamed into %s rather than the snapshot directory: "+
				"a rename is atomic only within one filesystem\n%s",
				filepath.Dir(e.path), format(events))
		}
	}
	for _, e := range events {
		if e.op == "open" && strings.HasSuffix(e.path, tempExt) && filepath.Dir(e.path) != dir {
			t.Errorf("a temporary file was written to %s rather than beside the file it "+
				"replaces: the rename that puts it in place is atomic only within one "+
				"filesystem\n%s", filepath.Dir(e.path), format(events))
		}
	}

	// "Synced before the rename, never after." Renaming over the last
	// known-good snapshot while the new one's bytes are still only in the
	// page cache is how a power cut destroys both at once.
	for _, f := range channelFiles {
		synced := indexOf(events, "fsync", f+tempExt)
		renamed := indexOf(events, "rename", f)
		if synced < 0 {
			t.Errorf("%s was never synced before being put in place\n%s",
				filepath.Base(f), format(events))
			continue
		}
		if renamed < 0 {
			t.Errorf("%s was never renamed into place\n%s", filepath.Base(f), format(events))
			continue
		}
		if synced > renamed {
			t.Errorf("%s was renamed over the last known-good snapshot before its own bytes "+
				"were synced: a power cut in that window destroys both\n%s",
				filepath.Base(f), format(events))
		}
	}

	// "Without it a file's contents survive a power cut but the directory
	// entry naming them need not, which leaves the new snapshot written,
	// synced, and invisible."
	lastRename := -1
	for i, e := range events {
		if e.op == "rename" {
			lastRename = i
		}
	}
	lastDirSync := -1
	for i, e := range events {
		if e.op == "fsync" && e.path == dir {
			lastDirSync = i
		}
	}
	if lastDirSync < lastRename {
		t.Errorf("no directory sync followed the last rename: the snapshots are written, "+
			"synced, and their directory entries need not survive a power cut\n%s",
			format(events))
	}
}

// traceHelper writes one snapshot directory and nothing else, so that the
// trace holds the syscalls of a save and no others. It is an ordinary test
// that skips, rather than a separate program, because a second binary is a
// second thing to keep building.
const (
	traceHelper = "TestWriteASnapshotToBeTraced"
	traceDirEnv = "SAGUIN_TRACE_SNAPSHOT_DIR"
)

func TestWriteASnapshotToBeTraced(t *testing.T) {
	dir := os.Getenv(traceDirEnv)
	if dir == "" {
		t.Skip("the traced half of TestTheSnapshotWriteSyncsInTheOrderItsCommentsClaim; " +
			"it runs that test rather than this one")
	}
	d := NewDir(dir, "0.1.0-test")
	if err := d.save(time.Unix(1700000000, 0), channels("events", "state")); err != nil {
		t.Fatalf("save: %v", err)
	}
}

// event is one syscall the trace holds, reduced to what these assertions
// ask: which operation, and on which path. A rename is recorded under the
// name it produced, since that is what the ordering is about.
type event struct {
	op   string // open, fsync, rename, close
	path string
}

var (
	// strace -y writes the path beside a descriptor: fsync(4</tmp/x/a.tmp>).
	traceFsync = regexp.MustCompile(`\b(fsync|fdatasync)\(\d+<([^>]+)>`)
	traceClose = regexp.MustCompile(`\bclose\(\d+<([^>]+)>`)
	// openat's path is its one quoted argument; the descriptor before it is
	// annotated by -y and holds no quotes.
	traceOpen = regexp.MustCompile(`\bopenat\([^,]+, "([^"]+)"`)
	// rename's two paths are both quoted, and the second is the one that
	// ends up naming the data.
	traceRename = regexp.MustCompile(`\brename(?:at2?)?\((?:[^,]+, )?"([^"]+)", (?:[^,]+, )?"([^"]+)"`)
)

// parseTrace reads the syscalls that touched one directory, in the order
// strace recorded them.
//
// A call interrupted by another thread is printed as an `<unfinished ...>`
// line and a `<... resumed>` one. Every pattern above matches only the half
// that carries the path, which is the half printed when the call was made -
// so an event's position is where the call started, and two calls on one
// thread can never be recorded out of order.
func parseTrace(t *testing.T, path, dir string) []event {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the trace: %v", err)
	}
	var out []event
	for _, line := range strings.Split(string(b), "\n") {
		// By the directory *or* by the names saguin gives these files. Only
		// by the directory and a snapshot written somewhere else is invisible
		// rather than caught - which is what happened: the check below that a
		// temporary file sits beside the file it replaces could not fail,
		// because moving it elsewhere removed it from the trace this reads.
		// An instrument that drops the evidence of the defect it is looking
		// for reports on work it did not do.
		if !strings.Contains(line, dir) &&
			!strings.Contains(line, snapshotExt) &&
			!strings.Contains(line, manifestName) {
			continue
		}
		switch {
		case traceRename.MatchString(line):
			m := traceRename.FindStringSubmatch(line)
			out = append(out, event{"rename", m[2]})
		case traceFsync.MatchString(line):
			out = append(out, event{"fsync", traceFsync.FindStringSubmatch(line)[2]})
		case traceOpen.MatchString(line):
			out = append(out, event{"open", traceOpen.FindStringSubmatch(line)[1]})
		case traceClose.MatchString(line):
			out = append(out, event{"close", traceClose.FindStringSubmatch(line)[1]})
		}
	}
	return out
}

func indexOf(events []event, op, path string) int {
	for i, e := range events {
		if e.op == op && e.path == path {
			return i
		}
	}
	return -1
}

// format prints the trace as this test read it, so that a failure says what
// the broker actually did rather than only what it should have done.
func format(events []event) string {
	var b strings.Builder
	b.WriteString("the syscalls on the snapshot directory, in order:\n")
	for i, e := range events {
		fmt.Fprintf(&b, "  %2d  %-7s %s\n", i, e.op, filepath.Base(e.path))
	}
	return b.String()
}
