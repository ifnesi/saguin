package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// run calls the migration with its output captured, because what it prints
// is most of what it is: every refusal has to tell an operator which rule
// they met and what to do instead.
func run(t *testing.T, args []string, withConfig, withCheck bool) (int, string) {
	t.Helper()

	stdout, stderr := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout, os.Stderr = w, w
	said := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		said <- string(b)
	}()

	code := migrateToSQLite(args, withConfig, withCheck)

	_ = w.Close()
	os.Stdout, os.Stderr = stdout, stderr
	return code, <-said
}

// snapshots writes a directory the way a broker's shutdown would, with a
// channel retention has trimmed: its records start at 5 and its next is 8,
// neither of which can be recovered from the records that survive.
func snapshots(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "snaps")
	at := time.Unix(1770000000, 0)

	err := store.NewDir(dir, "test").Save([]*store.Snapshot{
		{Channels: []store.ChannelState{{
			Name: "events", Kind: store.KindAppend, Next: 8, Floor: 5,
			Records: []store.Record{
				{Offset: 5, MessageID: "m5", Topic: "events/a", Payload: []byte("five"), Timestamp: at},
				{Offset: 6, MessageID: "m6", Topic: "events/b", Payload: []byte("six"), Timestamp: at},
				{Offset: 7, MessageID: "m7", Topic: "events/c", Payload: []byte("seven"), Timestamp: at},
			},
			Positions: []store.Position{
				{Reader: "mqtt:live", Offset: 6, LastSeen: time.Now(), ExpiresIn: time.Hour},
			},
		}}},
		{Channels: []store.ChannelState{
			{Name: "jobs", Kind: store.KindQueue, Next: 3, Floor: 1,
				Items: []store.Item{{Record: store.Record{Offset: 2, MessageID: "j2",
					Topic: "jobs/x", Timestamp: at}, Attempts: 2}}},
			{Name: "jobs__dlq", Kind: store.KindAppend, Next: 2, Floor: 1,
				Records: []store.Record{{Offset: 1, MessageID: "j1", Topic: "jobs/w", Timestamp: at}}},
		}},
	})
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	return dir
}

// The numbers on screen are the point of the command: an operator has to be
// able to see that the offsets came over rather than starting again.
func TestMigrateCarriesTheChannelsAndSaysWhat(t *testing.T) {
	from := snapshots(t)
	to := filepath.Join(t.TempDir(), "saguin.db")

	code, said := run(t, []string{from, to}, false, false)
	if code != 0 {
		t.Fatalf("exit %d, want 0. It said:\n%s", code, said)
	}
	for _, want := range []string{
		"events", "offsets 5..7", "next 8", "floor 5", "1 position(s)",
		"jobs", "1 unresolved record(s)", "jobs__dlq", "are unchanged",
	} {
		if !strings.Contains(said, want) {
			t.Errorf("the report does not mention %q. It said:\n%s", want, said)
		}
	}

	db, err := sqlite.Open(to, "test")
	if err != nil {
		t.Fatalf("open the migrated database: %v", err)
	}
	defer func() { _ = db.Close() }()

	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if lg.Next() != 8 || lg.Floor() != 5 {
		t.Errorf("next %d floor %d, want 8 and 5: the channel was renumbered", lg.Next(), lg.Floor())
	}
	if _, ok, _ := lg.Position("mqtt:live"); !ok {
		t.Error("the consumer's position did not come across: it would replay from the floor")
	}

	// The snapshots stay exactly as they were. Removing them is the
	// operator's business, and a command that deleted them would take the
	// only copy while the new one is still unproven.
	if _, err := os.Stat(filepath.Join(from, "events.snapshot")); err != nil {
		t.Errorf("the source snapshot is gone: %v", err)
	}
}

func TestMigrateRefusesTheFlagsItCannotHonour(t *testing.T) {
	from, to := snapshots(t), filepath.Join(t.TempDir(), "x.db")

	for _, c := range []struct {
		name                  string
		withConfig, withCheck bool
		want                  string
	}{
		{"--config", true, false, "consults no configuration"},
		{"--check-config", false, true, "cannot be given together"},
	} {
		code, said := run(t, []string{from, to}, c.withConfig, c.withCheck)
		if code == 0 {
			t.Errorf("%s alongside a migration was accepted", c.name)
		}
		if !strings.Contains(said, c.want) {
			t.Errorf("%s: the refusal does not say why. It said:\n%s", c.name, said)
		}
		if _, err := os.Stat(to); err == nil {
			t.Errorf("%s: a database was created by a refused migration", c.name)
		}
	}
}

func TestMigrateNeedsExactlyTwoPaths(t *testing.T) {
	from := snapshots(t)
	for _, args := range [][]string{{}, {from}, {from, "a", "b"}} {
		code, said := run(t, args, false, false)
		if code == 0 {
			t.Errorf("%d path(s) was accepted", len(args))
		}
		if !strings.Contains(said, "takes two paths") {
			t.Errorf("%d path(s): the refusal does not say what is wanted. It said:\n%s", len(args), said)
		}
	}
}

// Anything at the destination stops it, not merely a database saguin
// recognises. The symlink is the case that decides between Lstat and Stat:
// a dangling one does not exist as far as Stat is concerned, and following
// it would create the database at whatever it points at - outside the path
// the operator named.
func TestMigrateRefusesAnythingAtTheDestination(t *testing.T) {
	from := snapshots(t)

	for _, c := range []struct {
		name string
		make func(path string) error
	}{
		{"a file", func(p string) error { return os.WriteFile(p, []byte("hello"), 0o600) }},
		{"a directory", func(p string) error { return os.Mkdir(p, 0o700) }},
		{"a dangling symlink", func(p string) error { return os.Symlink(filepath.Join(p, "nowhere"), p) }},
	} {
		to := filepath.Join(t.TempDir(), "saguin.db")
		if err := c.make(to); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		code, said := run(t, []string{from, to}, false, false)
		if code == 0 {
			t.Errorf("%s at the destination was accepted", c.name)
		}
		if !strings.Contains(said, "only ever writes a database that does not") {
			t.Errorf("%s: the refusal does not say why. It said:\n%s", c.name, said)
		}
	}
}

func TestMigrateRefusesASourceThatIsNotADirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(file, []byte("broker:\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, said := run(t, []string{file, filepath.Join(t.TempDir(), "x.db")}, false, false)
	if code == 0 {
		t.Fatal("a file was accepted as a snapshot directory")
	}
	if !strings.Contains(said, "is not a directory") {
		t.Errorf("the refusal does not say why. It said:\n%s", said)
	}
}

// Nothing to migrate creates nothing. An empty database left behind would
// count as "already exists" and lock the operator out of that path for
// ever, which is the rule refusing them their own retry.
func TestMigrateFromAnEmptyDirectoryWritesNothing(t *testing.T) {
	from := t.TempDir()
	to := filepath.Join(t.TempDir(), "saguin.db")

	code, said := run(t, []string{from, to}, false, false)
	if code != 0 {
		t.Errorf("exit %d, want 0: an empty directory is not a failure. It said:\n%s", code, said)
	}
	if !strings.Contains(said, "No database was written") {
		t.Errorf("it does not say nothing happened. It said:\n%s", said)
	}
	if _, err := os.Stat(to); err == nil {
		t.Error("a database was created for a directory holding no snapshots")
	}
}

// A directory a broker is serving is being rewritten at its next shutdown,
// so reading it out from underneath is reading a state that is about to
// change. The lock is the same one the broker takes.
func TestMigrateRefusesASourceABrokerIsHolding(t *testing.T) {
	from := snapshots(t)
	to := filepath.Join(t.TempDir(), "saguin.db")

	held, err := sqlite.Hold(filepath.Join(from, snapshotLockName), from)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	defer func() { _ = held.Release() }()

	code, said := run(t, []string{from, to}, false, false)
	if code == 0 {
		t.Fatal("a snapshot directory a broker holds was migrated")
	}
	if !strings.Contains(said, "Stop saguin before migrating") {
		t.Errorf("the refusal does not say what to do. It said:\n%s", said)
	}
	if _, err := os.Stat(to); err == nil {
		t.Error("a database was created by a refused migration")
	}
}

// The destination's twin (RFC 0004's rules table: both migrations take the
// provider's lock). A snapshot directory a broker holds is one its next
// shutdown rewrites: migrated into, it reported three records done and held
// none once that broker had stopped. So it is refused, with nothing written
// there, and the same migration goes through once the broker has let go -
// the lock file a stopped broker leaves behind is not state.
func TestMigrateRefusesADestinationABrokerIsHolding(t *testing.T) {
	from := snapshots(t)
	db := filepath.Join(t.TempDir(), "saguin.db")
	if code, said := run(t, []string{from, db}, false, false); code != 0 {
		t.Fatalf("building the source database: exit %d:\n%s", code, said)
	}
	to := t.TempDir()
	held, err := sqlite.Hold(filepath.Join(to, snapshotLockName), to)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}

	code, said := runBack(t, []string{db, to}, false, false)
	if code == 0 {
		_ = held.Release()
		t.Fatalf("a snapshot directory a broker holds was migrated into:\n%s", said)
	}
	if !strings.Contains(said, "Stop saguin before migrating") {
		t.Errorf("the refusal does not say what to do. It said:\n%s", said)
	}
	if err := emptyOfSnapshots(to); err != nil {
		t.Errorf("a refused migration wrote into the directory: %v", err)
	}

	if err := held.Release(); err != nil {
		t.Fatal(err)
	}
	if code, said := runBack(t, []string{db, to}, false, false); code != 0 {
		t.Fatalf("once the broker let the directory go, the migration was still refused: exit %d:\n%s", code, said)
	}
	if err := emptyOfSnapshots(to); err == nil {
		t.Error("the migration said it was done and the directory holds no snapshots")
	}
}

// The strongest thing that can be asked of two converters is that together
// they change nothing. A round trip catches an asymmetry in either
// direction, which testing each alone cannot: both could agree on a wrong
// answer only by having the same defect twice.
func TestMigrateRoundTripsWithoutChangingAChannel(t *testing.T) {
	from := snapshots(t)
	db := filepath.Join(t.TempDir(), "saguin.db")
	back := filepath.Join(t.TempDir(), "back")

	if code, said := run(t, []string{from, db}, false, false); code != 0 {
		t.Fatalf("to sqlite: exit %d:\n%s", code, said)
	}
	code, said := runBack(t, []string{db, back}, false, false)
	if code != 0 {
		t.Fatalf("back to snapshots: exit %d:\n%s", code, said)
	}

	before := channelsIn(t, from)
	after := channelsIn(t, back)
	if len(before) != len(after) {
		t.Fatalf("started with %d channels and came back with %d", len(before), len(after))
	}
	for name, was := range before {
		is, ok := after[name]
		if !ok {
			t.Errorf("channel %q did not come back", name)
			continue
		}
		if was.Kind != is.Kind || was.Next != is.Next || was.Floor != is.Floor {
			t.Errorf("channel %q came back as %s next=%d floor=%d, was %s next=%d floor=%d",
				name, is.Kind, is.Next, is.Floor, was.Kind, was.Next, was.Floor)
		}
		if len(was.Records) != len(is.Records) || len(was.Items) != len(is.Items) {
			t.Errorf("channel %q came back with %d records and %d items, was %d and %d",
				name, len(is.Records), len(is.Items), len(was.Records), len(was.Items))
			continue
		}
		for i := range was.Records {
			w, g := was.Records[i], is.Records[i]
			if w.Offset != g.Offset || w.MessageID != g.MessageID || w.Topic != g.Topic ||
				string(w.Payload) != string(g.Payload) || !w.Timestamp.Equal(g.Timestamp) {
				t.Errorf("channel %q record %d came back as %+v, was %+v", name, i, g, w)
			}
		}
		for i := range was.Items {
			w, g := was.Items[i], is.Items[i]
			if w.Offset != g.Offset || w.MessageID != g.MessageID || w.Attempts != g.Attempts {
				t.Errorf("channel %q item %d came back as offset %d attempts %d, was %d and %d",
					name, i, g.Offset, g.Attempts, w.Offset, w.Attempts)
			}
		}
		if len(was.Positions) != len(is.Positions) {
			t.Errorf("channel %q came back with %d positions, was %d",
				name, len(is.Positions), len(was.Positions))
			continue
		}
		for i := range was.Positions {
			w, g := was.Positions[i], is.Positions[i]
			if w.Reader != g.Reader || w.Offset != g.Offset || w.ExpiresIn != g.ExpiresIn {
				t.Errorf("channel %q position %d came back as %+v, was %+v", name, i, g, w)
			}
		}
	}

	// And the queue is still in one file with its dead-letter channel, or a
	// record leaving one for the other could be recorded by half.
	for _, s := range loadAll(t, back) {
		if s.Channels[0].Name != "jobs" {
			continue
		}
		if len(s.Channels) != 2 || s.Channels[1].Name != "jobs__dlq" {
			t.Errorf("the queue's file holds %d channel(s), want jobs and jobs__dlq", len(s.Channels))
		}
		return
	}
	t.Error("no file is named by the queue")
}

// The source of a conversion is the operator's remaining copy until the
// destination has proved itself. Opening it the way a broker does would
// drop the positions whose sessions expired, which is a write.
func TestMigrateToSnapshotsChangesNothingInTheDatabase(t *testing.T) {
	db := filepath.Join(t.TempDir(), "saguin.db")
	if code, said := run(t, []string{snapshots(t), db}, false, false); code != 0 {
		t.Fatalf("to sqlite: exit %d:\n%s", code, said)
	}

	// A position whose session ran out while nothing was running. A broker
	// opening this database deletes it; this must not.
	open, err := sqlite.Open(db, "test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	lg, err := open.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	err = lg.SavePosition(store.Position{
		Reader: "mqtt:gone", Offset: 5,
		LastSeen: time.Now().Add(-2 * time.Hour), ExpiresIn: time.Minute,
	})
	if err != nil {
		t.Fatalf("save position: %v", err)
	}
	if err := open.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if code, said := runBack(t, []string{db, filepath.Join(t.TempDir(), "out")}, false, false); code != 0 {
		t.Fatalf("back to snapshots: exit %d:\n%s", code, said)
	}

	// OpenForExport, so nothing is recovered and the row is still there.
	again, err := sqlite.OpenForExport(db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = again.Close() }()
	channels, err := again.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	for _, c := range channels {
		if c.Name != "events" {
			continue
		}
		for _, p := range c.Positions {
			if p.Reader == "mqtt:gone" {
				return
			}
		}
	}
	t.Error("the expired position was deleted from the source: reading a database changed it")
}

func TestMigrateToSnapshotsRefusesADirectoryHoldingSnapshots(t *testing.T) {
	db := filepath.Join(t.TempDir(), "saguin.db")
	if code, said := run(t, []string{snapshots(t), db}, false, false); code != 0 {
		t.Fatalf("to sqlite: exit %d:\n%s", code, said)
	}

	to := t.TempDir()
	// A lock file left by a broker that has stopped must not count: the
	// kernel drops the lock when a process ends but the file stays, so this
	// would otherwise refuse every directory a broker had ever used.
	if err := os.WriteFile(filepath.Join(to, snapshotLockName), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, said := runBack(t, []string{db, to}, false, false); code != 0 {
		t.Fatalf("a directory holding only a lock file was refused: exit %d:\n%s", code, said)
	}

	// And now it holds snapshots, so a second conversion into it is refused.
	code, said := runBack(t, []string{db, to}, false, false)
	if code == 0 {
		t.Fatal("a directory that already holds snapshots was written into a second time")
	}
	if !strings.Contains(said, "already holds snapshots") {
		t.Errorf("the refusal does not say why. It said:\n%s", said)
	}
}

func TestMigrateToSnapshotsRefusesADatabaseThatIsNotThere(t *testing.T) {
	code, said := runBack(t,
		[]string{filepath.Join(t.TempDir(), "nothing.db"), filepath.Join(t.TempDir(), "out")}, false, false)
	if code == 0 {
		t.Fatal("a database that does not exist was accepted")
	}
	if !strings.Contains(said, "no such file") {
		t.Errorf("the refusal does not say why. It said:\n%s", said)
	}
}

func TestMigrateToSnapshotsRefusesTheFlagsItCannotHonour(t *testing.T) {
	db := filepath.Join(t.TempDir(), "saguin.db")
	if code, said := run(t, []string{snapshots(t), db}, false, false); code != 0 {
		t.Fatalf("to sqlite: exit %d:\n%s", code, said)
	}
	to := filepath.Join(t.TempDir(), "out")

	if code, said := runBack(t, []string{db, to}, true, false); code == 0 {
		t.Errorf("--config alongside a migration was accepted:\n%s", said)
	}
	if code, said := runBack(t, []string{db, to}, false, true); code == 0 {
		t.Errorf("--check-config alongside a migration was accepted:\n%s", said)
	}
	if code, said := runBack(t, []string{db}, false, false); code == 0 {
		t.Errorf("one path was accepted:\n%s", said)
	}
	if _, err := os.Stat(to); err == nil {
		t.Error("a refused migration created the directory anyway")
	}
}

// runBack is run, for the other direction.
func runBack(t *testing.T, args []string, withConfig, withCheck bool) (int, string) {
	t.Helper()

	stdout, stderr := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout, os.Stderr = w, w
	said := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		said <- string(b)
	}()

	code := migrateToSnapshots(args, withConfig, withCheck)

	_ = w.Close()
	os.Stdout, os.Stderr = stdout, stderr
	return code, <-said
}

func loadAll(t *testing.T, dir string) []*store.Snapshot {
	t.Helper()
	snaps, err := store.NewDir(dir, "test").LoadAll()
	if err != nil {
		t.Fatalf("load %s: %v", dir, err)
	}
	return snaps
}

func channelsIn(t *testing.T, dir string) map[string]store.ChannelState {
	t.Helper()
	out := map[string]store.ChannelState{}
	for _, s := range loadAll(t, dir) {
		for _, c := range s.Channels {
			out[c.Name] = c
		}
	}
	return out
}
