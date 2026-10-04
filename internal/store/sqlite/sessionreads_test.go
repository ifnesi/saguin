package sqlite

import (
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// sessionsWithReads is a session store on a provider opened with
// read_connections n, holding one session with one returned delivery.
func sessionsWithReads(t *testing.T, n int) (*DB, *Sessions) {
	t.Helper()
	db, err := OpenBoundedWithReads(tempPath(t), "0.1.0-test", 0, 0, n)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := sessionsOn(t, db)
	if err := s.Save(aSession("reader")); err != nil {
		t.Fatalf("save: %v", err)
	}
	lg, err := s.Log()
	if err != nil {
		t.Fatalf("the log: %v", err)
	}
	rec, err := lg.Append(store.Record{MessageID: "m", Topic: "fleet/a", Payload: []byte("p")})
	if err != nil {
		t.Fatalf("append to the log: %v", err)
	}
	off := rec.Offset
	if err := s.CreateShareCursor("$share/g/fleet/+", off); err != nil {
		t.Fatalf("create the group's cursor: %v", err)
	}
	if err := s.Lend("$share/g/fleet/+", off, []uint64{off}); err != nil {
		t.Fatalf("return a delivery to the group: %v", err)
	}
	return db, s
}

// sessionReads runs the three reads the pool serves and says what each found.
func sessionReads(s *Sessions) (got store.Session, had bool, all []store.Session, returned map[string][]uint64, err error) {
	if got, had, err = s.Get("reader"); err != nil {
		return
	}
	if all, err = s.All(); err != nil {
		return
	}
	returned, err = s.ShareReturned()
	return
}

// **A session's reads are served beside the writer** (Sessions.Get, All,
// ShareReturned). A SUBSCRIBE reads its session's record before it writes it,
// and every one of those reads queued on the one write connection behind
// every other client's write - measured, a third of a sqlite provider's cost
// of ten thousand clients subscribing. With the writer held inside a
// transaction, each read still answers, and answers what was committed.
func TestSessionReadsAreServedBesideTheWriter(t *testing.T) {
	db, s := sessionsWithReads(t, store.SQLiteReadConnections)
	if db.reads == nil {
		t.Fatal("the provider opened no read pool, so nothing below is asked of one")
	}
	hold, err := db.db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := hold.Exec(`UPDATE meta SET value = value WHERE key = 'writer'`); err != nil {
		t.Fatalf("take the write lock: %v", err)
	}
	defer func() { _ = hold.Rollback() }()
	type answer struct {
		got      store.Session
		had      bool
		all      []store.Session
		returned map[string][]uint64
		err      error
	}
	done := make(chan answer, 1)
	go func() {
		var a answer
		a.got, a.had, a.all, a.returned, a.err = sessionReads(s)
		done <- a
	}()
	select {
	case a := <-done:
		if a.err != nil || !a.had || len(a.all) != 1 || len(a.returned["$share/g/fleet/+"]) != 1 {
			t.Fatalf("beside a held writer: had %v, %d sessions, returned %v, err %v; want the session and "+
				"its group's one returned delivery", a.had, len(a.all), a.returned, a.err)
		}
		sameSession(t, a.got, aSession("reader"))
	case <-time.After(5 * time.Second):
		t.Fatal("a session read waited five seconds behind a writer holding the write connection: " +
			"it is not on the read pool")
	}
}

// **Without a pool each read runs on the write connection**, as it did before
// the pool served them: read_connections 0, which a small box chooses, and a
// database opened for export, which opens none. The same answers either way.
func TestSessionReadsWithoutAPoolRunOnTheWriter(t *testing.T) {
	db, s := sessionsWithReads(t, 0)
	if db.reads != nil {
		t.Fatal("read_connections 0 opened a read pool")
	}
	got, had, all, returned, err := sessionReads(s)
	if err != nil || !had || len(all) != 1 || !slices.Equal(returned["$share/g/fleet/+"], []uint64{1}) {
		t.Fatalf("read_connections 0: had %v, %d sessions, returned %v, err %v", had, len(all), returned, err)
	}
	sameSession(t, got, aSession("reader"))

	path := db.path
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	exp, err := OpenForExport(path)
	if err != nil {
		t.Fatalf("open for export: %v", err)
	}
	t.Cleanup(func() { _ = exp.Close() })
	if exp.reads != nil {
		t.Fatal("a database opened for export opened a read pool")
	}
	es, err := exp.Sessions()
	if err != nil {
		t.Fatalf("sessions of the export: %v", err)
	}
	got, had, all, returned, err = sessionReads(es)
	if err != nil || !had || len(all) != 1 || !slices.Equal(returned["$share/g/fleet/+"], []uint64{1}) {
		t.Fatalf("opened for export: had %v, %d sessions, returned %v, err %v", had, len(all), returned, err)
	}
	sameSession(t, got, aSession("reader"))
}

// **A read on the pool sees the caller's own write** once that write has
// returned: each is a fresh read transaction begun after the commit. Saved,
// read, changed, read, dropped, read - a hundred times, each read after each
// write returned.
func TestASessionReadSeesTheWriteBeforeIt(t *testing.T) {
	_, s := sessionsWithReads(t, store.SQLiteReadConnections)
	for i := range 100 {
		sess := aSession("reader")
		sess.ExpiryInterval = uint32(1000 + i)
		if err := s.Save(sess); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		got, had, err := s.Get("reader")
		if err != nil || !had || got.ExpiryInterval != sess.ExpiryInterval {
			t.Fatalf("round %d: read expiry %d (had %v, err %v) after saving %d", i, got.ExpiryInterval, had, err,
				sess.ExpiryInterval)
		}
	}
	if _, err := s.Drop("reader", nil); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, had, err := s.Get("reader"); err != nil || had {
		t.Fatalf("after the drop returned, the read found the session (had %v, err %v)", had, err)
	}
	if all, err := s.All(); err != nil || len(all) != 0 {
		t.Fatalf("after the drop returned, every session read %d (err %v), want 0", len(all), err)
	}
}
