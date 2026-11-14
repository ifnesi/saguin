package store_test

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The API these tests add to the store's (store_test.go, full_test.go):
//
//	type Session struct { // a durable session's sessions row (storage.md), column for column
//		ID             int64
//		Client         string
//		User           string // "" for none
//		Protocol       uint8  // 4 for MQTT 3.1.1, 5 for MQTT 5
//		Expiry         uint32 // the session expiry granted, in seconds
//		Left           int64  // when its connection ended, Unix milliseconds; 0 while connected
//		ReceiveMaximum uint16
//		Listener       string
//		Remote         string
//		KeepAlive      uint16
//		CleanStart     bool
//	}
//	type LoadSession struct { // *LoadSession is a Read
//		Client  string  // the client id asked for
//		Session Session // its sessions row, when Found
//		Found   bool    // false where the client id has none
//	}
//	type SaveSession struct { // *SaveSession is a Write of no record
//		Session Session
//		New     bool // inserts Session's row; false updates every column of the row of Session.ID
//	}
//	type LeaveSession struct { // *LeaveSession is a Write of no record: session ID goes away
//		ID     int64
//		Expiry uint32 // its session expiry from now on, in seconds
//		Left   int64  // when its connection ended, Unix milliseconds
//	}
//	type EndSession struct{ ID int64 } // *EndSession is a Write of no record: session ID, and every row naming it, go
//	const DueBatch = 256 // the most sessions one DueSessions returns (R6), set by measurement
//	type DueSession struct {
//		ID     int64
//		Client string
//		Expiry int64 // when its expiry runs out, Unix milliseconds
//	}
//	type DueSessions struct { // *DueSessions is a Read
//		At       int64        // Unix milliseconds
//		After    DueSession   // where a pass goes on: the last session it handled, by Expiry, then ID; zero for the start
//		Sessions []DueSession // the durable sessions whose expiry has run out by At, strictly after After, in expiry, then id, order, at most DueBatch
//		Next     int64        // the expiry, Unix milliseconds, of the earliest away session after After not in Sessions, due or not; 0 for none
//		// A session granted 4,294,967,295 seconds, which never runs out, is in neither (storage.md, sessions).
//	}
//	type State struct {
//		Channels       []Channel
//		HighestSession int64 // the highest session id any table names; 0 for none
//	}
//
// The core decides and assigns session ids (storage.md, Ids); these requests
// store and read what it decided (R1). A durable session's row is inserted
// at its first CONNECT and updated at each later one (design.md, CONNECT,
// 7), read at a CONNECT for an id not connected (CONNECT, 4), written when
// its connection ends (DISCONNECT) and deleted, with every row naming the
// session, when the session ends. An away session's expiry is its left time
// plus its expiry in seconds (DISCONNECT: "The session goes away until its
// expiry"); a session connected has none running. It is due at a time when
// its expiry has run out by then, the instant itself included
// (DISCONNECT: "When the expiry runs out, the delayed Will is published and
// the session deleted"): at that instant, it has. The core keeps "only the
// next session expiry [...] still to come, read from the store at start
// and at the end of each pass" (DISCONNECT), and ends the sessions due, at
// start and whenever that expiry comes, through DueSessions and EndSession:
// it never holds an unbounded list. One read per timer fire: its Next is
// the expiry the core keeps, the timer re-armed at once where it is
// already past, and the first read at start gives the earliest expiry
// (0007c, decided with the maintainer: one fact, one place). Each due
// session comes with its client id, so the core can order its ending
// against a CONNECT for that client (R2: "Each session [...] has at most
// one store request outstanding").
//
// Start reads the clock once (design.md, Testing: only "the core
// never reads the clock"), which these tests drive with testing/synctest's
// fake clock. Start registers saguin_sessions_restored_total on the registry
// it is given.

// sessionTables are the tables whose rows name a session (storage.md,
// Sessions): its own row, then the rows it owns.
var sessionTables = []string{
	"sessions", "subscriptions", "subscription_starts", "wills", "positions", "session_inflight", "held_publishes",
}

// durableSession is the durable session id, of client id "c<id>", granted
// expiry seconds, whose connection ended at left, 0 while connected.
func durableSession(id int64, expiry uint32, left int64) store.Session {
	return store.Session{
		ID: id, Client: "c" + strconv.FormatInt(id, 10), Protocol: 5, Expiry: expiry, Left: left,
		ReceiveMaximum: 20, Listener: "mqtt", Remote: "10.0.0.1:50000", KeepAlive: 60,
	}
}

// sessionRow is s as its sessions row, by column name (storage.md).
func sessionRow(s *store.Session) testkit.Row {
	var cleanStart int64
	if s.CleanStart {
		cleanStart = 1
	}
	return testkit.Row{
		"id": s.ID, "client": s.Client, "user": s.User, "protocol": int64(s.Protocol), "expiry": int64(s.Expiry),
		"left": s.Left, "receive_maximum": int64(s.ReceiveMaximum), "listener": s.Listener, "remote": s.Remote,
		"keepalive": int64(s.KeepAlive), "clean_start": cleanStart,
	}
}

// tableRow is a row of one table.
type tableRow struct {
	table string
	row   testkit.Row
}

// sessionRows returns the sessions rows of sessions.
func sessionRows(sessions ...store.Session) []tableRow {
	rows := make([]tableRow, len(sessions))
	for i := range sessions {
		rows[i] = tableRow{"sessions", sessionRow(&sessions[i])}
	}
	return rows
}

// ownedRow is a row of table naming session id: storage.md's first valid
// row of that table (testkit.Tables).
func ownedRow(t *testing.T, table string, id int64) tableRow {
	t.Helper()
	return tableRow{table, testkit.With(testkit.TableNamed(t, table).Valid[0], testkit.Row{"session": id})}
}

// owned returns a row naming session id in each table a session owns rows
// of: its subscriptions, their start boundaries, its Will, positions,
// in-flight messages and held QoS 2 messages.
func owned(t *testing.T, id int64) []tableRow {
	t.Helper()
	rows := make([]tableRow, 0, len(sessionTables)-1)
	for _, table := range sessionTables[1:] {
		rows = append(rows, ownedRow(t, table, id))
	}
	return rows
}

// putRows is a test write inserting rows, each into its table, as later
// items' writes store a session's rows: a write of no record.
type putRows struct{ rows []tableRow }

func (*putRows) Records() int { return 0 }

func (p *putRows) WriteTx(tx *store.Tx) error {
	for _, r := range p.rows {
		if err := testkit.InsertRow(tx, r.table, r.row); err != nil {
			return err
		}
	}
	return nil
}

// seed stores rows through the writer.
func seed(t *testing.T, r *running, rows ...tableRow) {
	t.Helper()
	if err := write(t, r, &putRows{rows: rows}); err != nil {
		t.Fatalf("storing the test's rows: %v", err)
	}
}

// rowsQuerier is what *sql.DB and a reader's store.Conn share for reading
// rows.
type rowsQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// readSessions reads every sessions row through q, by id, each by column
// name.
func readSessions(q rowsQuerier) ([]testkit.Row, error) {
	rows, err := q.QueryContext(context.Background(), `SELECT * FROM sessions ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var got []testkit.Row
	values := make([]any, len(names))
	dests := make([]any, len(names))
	for i := range values {
		dests[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(dests...); err != nil {
			return nil, err
		}
		r := make(testkit.Row, len(names))
		for i, name := range names {
			r[name] = values[i]
		}
		got = append(got, r)
	}
	return got, rows.Err()
}

// sessionsRead is a test read of every sessions row.
type sessionsRead struct{ rows []testkit.Row }

func (s *sessionsRead) ReadConn(conn *store.Conn) (err error) {
	s.rows, err = readSessions(conn)
	return err
}

// compareSessions checks that got, sessions rows by id, are want's.
func compareSessions(t *testing.T, got []testkit.Row, want ...store.Session) {
	t.Helper()
	wantRows := make([]testkit.Row, len(want))
	for i := range want {
		wantRows[i] = sessionRow(&want[i])
	}
	if len(got) != len(wantRows) {
		t.Errorf("sessions rows:\n got %v\nwant %v", got, wantRows)
		return
	}
	for i := range got {
		if !maps.Equal(got[i], wantRows[i]) {
			t.Errorf("a sessions row:\n got %v\nwant %v", got[i], wantRows[i])
		}
	}
}

// checkSessions reads every sessions row through the readers, which must
// be want's, by id.
func checkSessions(t *testing.T, r *running, want ...store.Session) {
	t.Helper()
	s := &sessionsRead{}
	if err := read(t, r, s); err != nil {
		t.Fatalf("reading the sessions rows: %v", err)
	}
	compareSessions(t, s.rows, want...)
}

// naming is a test read counting the rows naming session id in each of
// sessionTables.
type naming struct {
	id     int64
	counts map[string]int
}

func (n *naming) ReadConn(conn *store.Conn) error {
	n.counts = make(map[string]int, len(sessionTables))
	for _, table := range sessionTables {
		column := "session"
		if table == "sessions" {
			column = "id"
		}
		var c int
		if err := conn.QueryRowContext(context.Background(), `SELECT count(*) FROM "`+table+`" WHERE "`+column+`" = ?`, n.id).Scan(&c); err != nil {
			return err
		}
		n.counts[table] = c
	}
	return nil
}

// checkNaming checks, through the readers, that each table of
// sessionTables holds want rows naming session id.
func checkNaming(t *testing.T, r *running, id int64, want int) {
	t.Helper()
	n := &naming{id: id}
	if err := read(t, r, n); err != nil {
		t.Fatalf("counting the rows naming session %d: %v", id, err)
	}
	for _, table := range sessionTables {
		if n.counts[table] != want {
			t.Errorf("%s: %d rows name session %d, want %d", table, n.counts[table], id, want)
		}
	}
}

// storage.md: "A row is never written by INSERT OR REPLACE. A reused key is a
// failed write and a store error (R8)"; sessions: id is the primary key,
// client unique. A new session's row reusing a stored session's id, or its
// client id, is answered with a store error, not "full", reported there once:
// Stop returns nil (design.md, Store, Writes: "A store error is reported
// once, in the answers that carry it; the store's stop reports only an error
// no answer carried", decided from item 0026d); and the stored row is as it
// was.
func TestSessionInsertNeverReplaces(t *testing.T) {
	stored := durableSession(3, 60, 0)
	reusedID, reusedClient := stored, stored
	reusedID.Client, reusedID.Expiry = "c9", 120
	reusedClient.ID, reusedClient.Expiry = 9, 120
	for _, tc := range []struct {
		name  string
		reuse store.Session
	}{
		{"a reused id", reusedID},
		{"a reused client id", reusedClient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newConfig(t, 2)
			r := start(t, cfg)
			if err := write(t, r, &store.SaveSession{Session: stored, New: true}); err != nil {
				t.Fatal(err)
			}
			err := write(t, r, &store.SaveSession{Session: tc.reuse, New: true})
			if err == nil || errors.Is(err, store.ErrFull) {
				t.Fatalf("answered %v, want a store error", err)
			}
			if stopErr := r.Stop(); stopErr != nil {
				t.Errorf("Stop: got %v, want nil: the store error %v was reported in its answer", stopErr, err)
			}
			got, err := readSessions(openFile(t, cfg.Broker.Storage.FilePath))
			if err != nil {
				t.Fatal(err)
			}
			compareSessions(t, got, stored)
		})
	}
}

// 0007c: "delete a session with every row it owns, in one transaction".
// storage.md, Sessions: a session's rows are its sessions row, where
// durable, its subscriptions, their start boundaries, its Will, its
// positions, its in-flight messages and its held QoS 2 messages;
// subscription_starts: "deleted with the subscription at UNSUBSCRIBE or when
// the session ends"; "A Will is also stored, including a non-durable
// session's Will. Its lifetime follows DISCONNECT and Restart";
// held_publishes: "A non-durable session's rows are deleted when it ends".
//
// Ending a durable session deletes its row and every row naming it; ending
// a non-durable session, which has no sessions row, deletes its Will and
// its held QoS 2 message. Another session's rows all stay.
func TestEndSession(t *testing.T) {
	r := start(t, newConfig(t, 2))
	ending, kept := durableSession(3, 60, 0), durableSession(5, 60, 0)
	rows := sessionRows(ending, kept)
	rows = append(rows, owned(t, ending.ID)...)
	rows = append(rows, owned(t, kept.ID)...)
	rows = append(rows, ownedRow(t, "wills", 8), ownedRow(t, "held_publishes", 8))
	seed(t, r, rows...)

	for _, id := range []int64{ending.ID, 8} {
		if err := write(t, r, &store.EndSession{ID: id}); err != nil {
			t.Fatalf("ending session %d: %v", id, err)
		}
		checkNaming(t, r, id, 0)
	}
	checkNaming(t, r, kept.ID, 1)
	checkSessions(t, r, kept)
	stop(t, r)
}

// store_test.go's API: Records() is "the records (design.md, Words: Record)
// it stores, 0 for none"; Words: "a record is one stored message". A
// session's rows hold no record, so a session write never counts toward
// publish_commit_max_records (design.md, Store, Writes).
func TestSessionWritesStoreNoRecord(t *testing.T) {
	for _, op := range []store.Write{&store.SaveSession{New: true}, &store.SaveSession{}, &store.LeaveSession{}, &store.EndSession{}} {
		if n := op.Records(); n != 0 {
			t.Errorf("%T counts %d records, want 0", op, n)
		}
	}
}

// storage.md, Ids: a session's id is the core's, "from 1 and assignment
// resumes at the highest of that kind loaded at start plus one. An id is
// therefore never reused while anything names it"; "Every session has an id,
// durable or not. Only a durable session has a row in sessions"; "A Will is
// also stored, including a non-durable session's Will. Its lifetime follows
// DISCONNECT and Restart"; held_publishes holds QoS 2 messages "from any
// session". State's HighestSession is the highest session id a table names at
// the start: a sessions row, a non-durable session's Will or held QoS 2
// message, or a subscription's start boundary (subscription_starts); 0 with
// none.
func TestStartHighestSession(t *testing.T) {
	durable := sessionRows(durableSession(2, 60, 0), durableSession(6, 60, 0))
	for _, tc := range []struct {
		name string
		rows []tableRow
		want int64
	}{
		{"no session", nil, 0},
		{"durable sessions", durable, 6},
		{"a non-durable session's Will", append(slices.Clone(durable), ownedRow(t, "wills", 9)), 9},
		{"a non-durable session's held QoS 2 message", append(slices.Clone(durable), ownedRow(t, "held_publishes", 11)), 11},
		{"a subscription's start boundary", append(slices.Clone(durable), ownedRow(t, "subscription_starts", 13)), 13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newConfig(t, 2)
			r := start(t, cfg)
			seed(t, r, tc.rows...)
			stop(t, r)
			r = start(t, cfg)
			if r.state.HighestSession != tc.want {
				t.Errorf("HighestSession: got %d, want %d", r.state.HighestSession, tc.want)
			}
			stop(t, r)
		})
	}
}

// dueOf returns sessions as DueSessions returns them.
func dueOf(sessions ...store.Session) []store.DueSession {
	due := make([]store.DueSession, len(sessions))
	for i := range sessions {
		due[i] = store.DueSession{ID: sessions[i].ID, Client: sessions[i].Client, Expiry: testkit.ExpiryAt(sessions[i].Left, sessions[i].Expiry)}
	}
	return due
}

// pass goes once through the sessions due at at, as the core does: each
// read after the last session the one before returned, until a read
// returns the last one due; with ending, it ends each read's sessions
// before the next read. It returns every session the reads returned, in
// order, and the last read's Next, failing the test at once on a session
// returned twice.
func pass(t *testing.T, r *running, at int64, ending bool) (due []store.DueSession, next int64) {
	t.Helper()
	d := &store.DueSessions{At: at}
	seen := map[int64]bool{}
	for {
		if err := read(t, r, d); err != nil {
			t.Fatalf("reading the sessions due at %d after %+v: %v", at, d.After, err)
		}
		if len(d.Sessions) > store.DueBatch {
			t.Fatalf("a read returned %d sessions, want at most %d", len(d.Sessions), store.DueBatch)
		}
		for _, s := range d.Sessions {
			if seen[s.ID] {
				t.Fatalf("session %d returned again, by the read after %+v", s.ID, d.After)
			}
			seen[s.ID] = true
		}
		due = append(due, d.Sessions...)
		if ending {
			endAll(t, r, d.Sessions)
		}
		if len(d.Sessions) < store.DueBatch || d.Next == 0 || d.Next > at {
			return due, d.Next
		}
		d = &store.DueSessions{At: at, After: d.Sessions[len(d.Sessions)-1]}
	}
}

// endDue reads, through the readers, the sessions due at at, which must be
// want, in order, with next as Next, and ends each, as the core does.
func endDue(t *testing.T, r *running, at, next int64, want ...store.Session) {
	t.Helper()
	d := &store.DueSessions{At: at}
	if err := read(t, r, d); err != nil {
		t.Fatalf("reading the sessions due at %d: %v", at, err)
	}
	if !slices.Equal(d.Sessions, dueOf(want...)) {
		t.Fatalf("due at %d: got %v, want %v", at, d.Sessions, dueOf(want...))
	}
	if d.Next != next {
		t.Errorf("due at %d: Next %d, want %d", at, d.Next, next)
	}
	endAll(t, r, d.Sessions)
}

// endAll ends the sessions of due, as the core does: an EndSession each, in
// one group (design.md, Store, Writes: "Publications, session saves and
// endings [...] may share a group"), none for no session ("Empty groups are
// not sent").
func endAll(t *testing.T, r *running, due []store.DueSession) {
	t.Helper()
	if len(due) == 0 {
		return
	}
	ends := make([]store.Write, len(due))
	for i, d := range due {
		ends[i] = &store.EndSession{ID: d.ID}
	}
	if err := write(t, r, &core.Group{Writes: ends}); err != nil {
		t.Fatalf("ending %d due sessions: %v", len(due), err)
	}
}

// design.md, R6: "Everything is bounded"; DISCONNECT: "Each time its timer
// comes, the core goes once through the sessions due, in expiry order. It
// then sets the timer for the next expiry still to come" (audit finding
// 261016-003a: every read started again from the earliest). One DueSessions
// returns at most DueBatch sessions, and goes on strictly after the last
// session the pass handled, ids ordering equal expiries. Here 2*DueBatch+3
// sessions are due, their expiries the reverse of their ids but for four at
// the first batch's boundary, which share one expiry, beside one not due. A
// pass returns every due session exactly once, in expiry, then id, order,
// with no repeat and no gap, whether its sessions are left in the store or
// ended between reads; its last Next is the session not due.
func TestDueSessionsInBatches(t *testing.T) {
	r := start(t, newConfig(t, 2))
	const a = 1_760_000_000_000
	n := 2*store.DueBatch + 3
	sessions := make([]store.Session, n)
	for i := range sessions {
		id := int64(i + 1)
		sessions[i] = durableSession(id, 60, a-60_000-id) // the higher the id, the earlier its expiry
	}
	slices.Reverse(sessions) // in expiry order
	for i := store.DueBatch - 2; i <= store.DueBatch+1; i++ {
		sessions[i].Left = sessions[store.DueBatch-2].Left
	}
	slices.SortFunc(sessions, func(x, y store.Session) int {
		return cmp.Or(cmp.Compare(testkit.ExpiryAt(x.Left, x.Expiry), testkit.ExpiryAt(y.Left, y.Expiry)), cmp.Compare(x.ID, y.ID))
	})
	notDue := durableSession(int64(n+1), 60, a) // a+60_000
	seed(t, r, sessionRows(append(slices.Clone(sessions), notDue)...)...)
	for _, ending := range []bool{false, true} {
		due, next := pass(t, r, a, ending)
		if !slices.Equal(due, dueOf(sessions...)) {
			t.Errorf("a pass, ending %v, returned %d sessions, want each of the %d due once, in order: %v", ending, len(due), n, due)
		}
		if want := testkit.ExpiryAt(notDue.Left, notDue.Expiry); next != want {
			t.Errorf("a pass, ending %v: Next %d, want %d", ending, next, want)
		}
	}
	checkSessions(t, r, notDue)
	stop(t, r)
}

// storage.md, sessions: "An expiry of 4,294,967,295 never runs out, so it is
// excluded from the index and from every due-expiry query"; design.md,
// DISCONNECT: "A granted Session Expiry of 4,294,967,295 means never expires.
// It has no expiry deadline." Beside an away session that is due, one granted
// that expiry has been away since the same time: a DueSessions read at the
// latest time there is returns the first alone, with Next 0, the second
// neither due nor next at any time.
func TestNeverExpiringSessionIsNeverDue(t *testing.T) {
	r := start(t, newConfig(t, 2))
	const a = 1_760_000_000_000
	never, due := durableSession(1, math.MaxUint32, a), durableSession(2, 60, a)
	seed(t, r, sessionRows(never, due)...)
	endDue(t, r, math.MaxInt64, 0, due)
	stop(t, r)
}

// storage.md, wills: the primary key is session, "the session's id"; "The
// index wills_due is on (due) WHERE due IS NOT NULL. It orders the Wills of
// gone clients by the instant they are due, so the Will pass reads only
// those (DISCONNECT). The Wills of connected clients are left out of it.
// Only the full-store retry reads them, to find a Will whose publication a
// full disk refused". design.md, Testing: "A test inside the code checks
// only what neither can see"; Capacities: "Due-work and metadata pages hold
// 256 rows".
//
// On a file the store made, SQLite explains the two reads of the Wills as
// the store prepares them (StatementSQL): the Will pass's, DueWills,
// searches the wills table through wills_due, and the full-store retry's,
// RetryWills, seeks it by its primary key, neither sorting on its own (no
// temporary B-tree): each page is read where it starts, in its order.
func TestWillReadsSeekTheirKeys(t *testing.T) {
	r := start(t, newConfig(t, 1))
	stop(t, r)
	db := openFile(t, r.cfg.Broker.Storage.FilePath)
	for _, tc := range []struct {
		name, wills string // the statement, and how its plan reads the wills table
	}{
		{"DueWills", "SEARCH w USING INDEX wills_due ("},
		{"RetryWills", "SEARCH w USING INTEGER PRIMARY KEY ("},
	} {
		query, ok := store.StatementSQL(tc.name)
		if !ok {
			t.Fatalf("the store prepares no statement %s", tc.name)
		}
		args := make([]any, strings.Count(query, "?"))
		for i := range args {
			args[i] = 1
		}
		plan := testkit.QueryPlan(t, db, query, args...)
		if !slices.ContainsFunc(plan, func(d string) bool { return strings.HasPrefix(d, tc.wills) }) ||
			slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, "TEMP B-TREE") }) {
			t.Errorf("%s's plan, %q: want the wills table read as %q, with no sort of its own", tc.name, plan, tc.wills+"...")
		}
	}
}

// restartAt stops r, moves the bubble's fake clock on to at, and starts a
// store on r's file again, its start reading the clock at at.
func restartAt(t *testing.T, r *running, at time.Time) *running {
	t.Helper()
	stop(t, r)
	time.Sleep(time.Until(at))
	if now := time.Now(); !now.Equal(at) {
		t.Fatalf("the fake clock is at %v, want %v", now, at)
	}
	return start(t, r.cfg)
}

// checkSessionState checks what r's start loaded for the sessions: the
// highest session id. What it counted, saguin_sessions_restored_total, is
// checked from /metrics (conformance, TestSessionSeries).
func checkSessionState(t *testing.T, r *running, highest int64) {
	t.Helper()
	if got := r.state.HighestSession; got != highest {
		t.Errorf("HighestSession: got %d, want %d", got, highest)
	}
}

// design.md, Restart: "Sessions that expired while the broker was down end
// at start, each in one transaction"; "then opens the store. The core loads
// channels [...] the next session expiry and Will due time still to come, but
// no session. It ends expired sessions". DISCONNECT: "The session goes away
// until its expiry"; "When the expiry runs out, the delayed Will is published
// and the session deleted, in one transaction". storage.md, sessions: "A
// session is away while its client is gone. A session left connected by a
// crash is away from the start that follows"; left "when its connection
// ended; 0 while connected". metrics.md: saguin_sessions_restored_total
// "durable sessions kept across the start, after those expired ended"; Rules:
// "Every series exists from start".
//
// The core ends the sessions that expired while the broker was down, as the
// design's Start says, so that it can publish a delayed Will in the
// ending's transaction (0007b): Start leaves their rows, and the core,
// given the start's time, ends them through DueSessions and EndSession. A
// session left connected by a crash (left 0) is away from that start:
// Start stores the start's time as its left time, so a later start counts
// its expiry from there, not from that later start.
// saguin_sessions_restored_total counts the sessions not due at the start,
// and the core's first read gives, as its Next, the earliest of their
// expiries. Sessions expiring 1 ms each side of the start, and one exactly
// at it, which is due, pin the expiry to the millisecond.
func TestStartSessionState(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := start(t, newConfig(t, 2))
		checkSessionState(t, r, 0)

		at := time.Now().Add(24 * time.Hour)
		a := at.UnixMilli()
		long := durableSession(2, 10_800, a-7_200_000)  // expires at a+3_600_000: the earliest left kept
		soon := durableSession(3, 60, a-60_000+1)       // a+1: the earliest kept
		connected := durableSession(4, 30, 0)           // the shortest expiry: a+30_000
		gone := durableSession(5, 3_600, a-3_600_000-1) // a-1
		longGone := durableSession(6, 60, a-120_000)    // a-60_000
		now := durableSession(7, 60, a-60_000)          // a: due
		seed(t, r, sessionRows(long, soon, connected, gone, longGone, now)...)

		r = restartAt(t, r, at)
		checkSessionState(t, r, 7)
		connected.Left = a
		checkSessions(t, r, long, soon, connected, gone, longGone, now)
		endDue(t, r, a, a+1, longGone, gone, now)
		checkSessions(t, r, long, soon, connected)

		later := at.Add(30*time.Second + time.Millisecond)
		r = restartAt(t, r, later)
		checkSessionState(t, r, 4)
		endDue(t, r, later.UnixMilli(), a+3_600_000, soon, connected)
		checkSessions(t, r, long)
		stop(t, r)
	})
}
