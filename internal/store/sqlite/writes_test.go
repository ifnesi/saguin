package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// queuedWrites is how many client writes are waiting for the transaction
// running now to end.
func queuedWrites(db *DB) int {
	db.forming.Lock()
	defer db.forming.Unlock()
	n := 0
	for _, g := range db.waiting {
		n += len(g.writes)
	}
	return n
}

// behindAHeldCommit runs each of writes on its own goroutine while a publish
// is held inside its transaction (holdACommit), waits until every one of
// them is queued behind it, releases it, and answers what each was told, in
// the order given. So which transaction each write lands in is decided rather
// than raced: all of them in the one after the publish's.
func behindAHeldCommit(t *testing.T, db *DB, writes ...func() error) []error {
	t.Helper()
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	release := holdACommit(t, db, lg)
	errs := make([]error, len(writes))
	var wg sync.WaitGroup
	for i, w := range writes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w()
		}()
		// Queued one at a time, so the group holds them in this order.
		until(t, fmt.Sprintf("write %d to queue", i), func() bool { return queuedWrites(db) == i+1 })
	}
	release()
	wg.Wait()
	return errs
}

// sameCountsAsTheFile holds a session store's in-memory counts to what its
// file holds: a store built again from the same file counts what the rows
// say, so any member whose counts moved without its rows - or whose rows
// stayed without its counts - shows here.
func sameCountsAsTheFile(t *testing.T, db *DB, s *Sessions) {
	t.Helper()
	fresh := &Sessions{db: db, holders: store.ShareHolders{}}
	all, err := fresh.all()
	if err != nil {
		t.Fatalf("read every session: %v", err)
	}
	for _, sess := range all {
		fresh.sessions++
		fresh.bytes += store.SessionSize(sess)
		fresh.holders.Add(sess)
	}
	inflight, err := fresh.checkInFlight()
	if err != nil {
		t.Fatalf("read the in-flight tables: %v", err)
	}
	fresh.bytes += inflight
	var cursors int64
	if err := db.queryRow(nil, `SELECT coalesce((SELECT sum(length(CAST(share_group AS BLOB)) + 8) FROM share_groups), 0)
		+ coalesce((SELECT sum(length(CAST(share_group AS BLOB)) + 8) FROM share_returned), 0)`).Scan(&cursors); err != nil {
		t.Fatalf("read the groups: %v", err)
	}
	fresh.bytes += cursors
	if s.Len() != fresh.sessions || s.Bytes() != fresh.bytes {
		t.Fatalf("the store counts %d sessions and %d bytes, and its file holds %d and %d", s.Len(), s.Bytes(),
			fresh.sessions, fresh.bytes)
	}
	if len(s.holders) != len(fresh.holders) {
		t.Fatalf("the store counts holders %v, and its file holds %v", s.holders, fresh.holders)
	}
	for g, n := range fresh.holders {
		if s.holders[g] != n {
			t.Fatalf("the store counts holders %v, and its file holds %v", s.holders, fresh.holders)
		}
	}
}

// RFC 0004 "Group commit": a client's writes arriving while a transaction
// commits share the next, as publishes do, SessionGroupMax to a transaction.
// Sessions saved behind a held publish are the publish's transaction and one
// for each group of them, not one each, and every one of them is stored.
func TestClientWritesArrivingDuringACommitShareTheNext(t *testing.T) {
	db := open(t, tempPath(t))
	db.budget = time.Hour // counts transactions: none split for time
	s := sessionsOn(t, db)
	if _, err := db.Log("events"); err != nil { // the held publish's channel, made before counting
		t.Fatal(err)
	}
	before := db.committed.Load()
	const n = 2*SessionGroupMax + 3
	var writes []func() error
	for i := range n {
		writes = append(writes, func() error { return s.Save(aSession(fmt.Sprint("c-", i))) })
	}
	for i, err := range behindAHeldCommit(t, db, writes...) {
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	// The held publish's, and one for each group of SessionGroupMax.
	if got, want := db.committed.Load()-before, int64(1+(n+SessionGroupMax-1)/SessionGroupMax); got != want {
		t.Fatalf("a held publish and %d saves behind it took %d transactions, want %d", n, got, want)
	}
	if s.Len() != n {
		t.Fatalf("the store counts %d sessions, want %d", s.Len(), n)
	}
	sameCountsAsTheFile(t, db, s)
}

// fullProvider is a session store on a provider whose publish ceiling is
// reached and then given back a little, so a session's record fits and a
// session carrying a large Will does not.
func fullProvider(t *testing.T) (*DB, *Sessions) {
	t.Helper()
	db, err := OpenBounded(tempPath(t), "test", 2<<20, 256<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := sessionsOn(t, db)
	if _, err := db.Log("events"); err != nil { // the held publish's channel, made before counting
		t.Fatal(err)
	}
	lg, err := db.Log("fill")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	filled := 0
	for ; ; filled++ {
		_, err := lg.Append(store.Record{MessageID: fmt.Sprint("f-", filled), Topic: "fill", Payload: make([]byte, 4000)})
		if errors.Is(err, store.ErrFull) {
			break
		}
		if err != nil {
			t.Fatalf("fill: %v", err)
		}
	}
	if filled == 0 {
		t.Fatal("nothing filled the file")
	}
	if _, _, err := lg.Trim(time.Time{}, lg.Bytes()-64<<10); err != nil {
		t.Fatalf("give back some room: %v", err)
	}
	return db, s
}

// withWill is a session whose Will's payload is n bytes.
func withWill(client string, n int) store.Session {
	sess := aSession(client)
	sess.Will = &store.SessionWill{Topic: "gone/" + client, Payload: make([]byte, n), QoS: 1}
	return sess
}

// **A member refused for room is refused alone** (writes.go "No
// savepoints"). A write that fills the provider ends the transaction every
// write beside it shares - SQLITE_FULL does not stay inside a savepoint here -
// so the others are stored in a transaction without it, and it is answered
// alone, as it would have been with a transaction of its own. What each is
// told is what the file holds, and the counts are the file's.
func TestAWriteRefusedForRoomIsRefusedAlone(t *testing.T) {
	db, s := fullProvider(t)
	if err := s.Save(withWill("too-big", 1<<20)); !errors.Is(err, store.ErrProviderFull) {
		t.Fatalf("a session larger than the room left, alone: %v, want ErrProviderFull - the fixture "+
			"does not make one refused", err)
	}
	errs := behindAHeldCommit(t, db,
		func() error { return s.Save(aSession("before")) },
		func() error { return s.Save(withWill("too-big", 1<<20)) },
		func() error { return s.Save(aSession("after")) },
	)
	if errs[0] != nil || errs[2] != nil || !errors.Is(errs[1], store.ErrProviderFull) {
		t.Fatalf("told %v; want the two that fit stored and the large one ErrProviderFull", errs)
	}
	for _, id := range []string{"before", "after"} {
		if _, had, err := s.Get(id); err != nil || !had {
			t.Fatalf("%s was told it was stored, and the file holds it: %v, %v", id, had, err)
		}
	}
	if _, had, _ := s.Get("too-big"); had {
		t.Fatal("the refused session is in the file")
	}
	sameCountsAsTheFile(t, db, s)
}

// **Two shared transactions at most, then each alone** (runWrites). Two
// writes that fill the provider, among four that fit: the first shared
// transaction ends at the first of them, the second at the second, and every
// write left runs alone - four commits for the four that fit, and none for
// the two refused. Re-forming a group after every failure would commit the
// four together and cost a transaction per failure, which in a reconnect storm
// into a full provider is the square of the storm.
func TestAWriteGroupTriesTwiceTogetherThenEachAlone(t *testing.T) {
	db, s := fullProvider(t)
	db.budget = time.Hour // counts transactions: none split for time
	before := db.committed.Load()
	errs := behindAHeldCommit(t, db,
		func() error { return s.Save(aSession("a")) },
		func() error { return s.Save(withWill("big-1", 1<<20)) },
		func() error { return s.Save(aSession("b")) },
		func() error { return s.Save(withWill("big-2", 1<<20)) },
		func() error { return s.Save(aSession("c")) },
		func() error { return s.Save(aSession("d")) },
	)
	for i, err := range errs {
		refused := i == 1 || i == 3
		if refused != errors.Is(err, store.ErrProviderFull) || !refused && err != nil {
			t.Fatalf("write %d was told %v", i, err)
		}
	}
	// The held publish's commit, and one for each write that fits.
	if got := db.committed.Load() - before; got != 1+4 {
		t.Fatalf("%d transactions committed, want 5: the held publish's and one for each of the four "+
			"writes that fit, run alone after two shared attempts", got)
	}
	sameCountsAsTheFile(t, db, s)
}

// **A statement sent after the transaction ended does not reach SQLite**
// (DB.note). SQLITE_FULL ends the whole transaction, and a statement the same
// connection runs afterwards runs outside any transaction and commits by
// itself. A write that ignored the refusal and wrote on would have that row
// stored while every write beside it was told nothing was.
func TestAStatementAfterTheTransactionEndedIsNeverRun(t *testing.T) {
	db, s := fullProvider(t)
	big, will, err := s.encodeSession(withWill("big", 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	var ignored, then error
	careless := &write{s: s, run: func(tx *sql.Tx) error {
		_, ignored = db.exec(tx, upsertSession, "big", int64(3600), int64(0), big, will)
		_, then = db.exec(tx, upsertSession, "leaked", int64(3600), int64(0), "[]", nil)
		return nil // both errors ignored
	}}
	errs := behindAHeldCommit(t, db,
		func() error { return s.Save(aSession("beside")) },
		func() error { return db.joinWrite(careless) },
	)
	if !errors.Is(ignored, store.ErrProviderFull) && !full(ignored) {
		t.Fatalf("the careless write's first statement was answered %v: the fixture does not refuse it", ignored)
	}
	if then == nil {
		t.Fatal("the statement after the refusal was run and succeeded")
	}
	if errs[0] != nil || errs[1] == nil {
		t.Fatalf("told %v; want the one beside stored and the careless one refused", errs)
	}
	if _, had, err := s.Get("leaked"); err != nil || had {
		t.Fatalf("the row written after the transaction ended is in the file (%v, %v): it ran on its own", had, err)
	}
	if _, had, _ := s.Get("beside"); !had {
		t.Fatal("the write beside the careless one is not in the file")
	}
}

// **A commit that fails is every member's, and moves no count**. Nothing is
// acknowledged, the counts are what the file holds, and the queue goes on.
func TestAFailedCommitTellsEveryWriteAndMovesNothing(t *testing.T) {
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	if err := s.Save(aSession("kept")); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	db.failCommit = func() error {
		var err error
		once.Do(func() { err = errors.New("the disk refused the commit") })
		return err
	}
	errs := behindAHeldCommit(t, db,
		func() error { return s.Save(aSession("x")) },
		func() error { _, err := s.Drop("kept", nil); return err },
		func() error { return s.Save(aSession("y")) },
	)
	for i, err := range errs {
		if err == nil {
			t.Fatalf("write %d was told it was stored by a transaction whose commit failed", i)
		}
	}
	sameCountsAsTheFile(t, db, s)
	if s.Len() != 1 {
		t.Fatalf("the store counts %d sessions after a failed commit, want the 1 kept before it", s.Len())
	}
	if err := s.Save(aSession("after")); err != nil {
		t.Fatalf("the write after a failed commit: %v", err)
	}
}

// **An ending refused for room in a shared transaction ends anyway, alone**,
// without returning its deliveries, as it did with a transaction of its own
// (Sessions.drop, write.again) - and the holders and counts are the file's.
func TestAnEndingRefusedForRoomAmongOthersEndsAlone(t *testing.T) {
	group := "$share/" + strings.Repeat("g", 240) + "/jobs"
	db, err := OpenBounded(tempPath(t), "test", 4<<20, 16<<10)
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
	const handed = 2000
	for i := range handed + 1 {
		if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("r-", i), Topic: "jobs", Payload: []byte("x"), QoS: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreateShareCursor(group, 1); err != nil {
		t.Fatal(err)
	}
	var mine []uint64
	for from := 0; from < handed; from += 5 {
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
	for filled := 0; ; filled++ {
		_, err := lg.Append(store.Record{MessageID: fmt.Sprint("f-", filled), Topic: "fill", Payload: make([]byte, 4000), QoS: 1})
		if errors.Is(err, store.ErrFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	var dropped store.Dropped
	errs := behindAHeldCommit(t, db,
		func() error { return s.Disconnected("n", time.Now(), false, 3600) },
		func() error { var err error; dropped, err = s.Drop("m", nil); return err },
		func() error { return s.Disconnected("n", time.Now(), true, 3600) },
	)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("write %d at the bound: %v", i, err)
		}
	}
	if _, had, _ := s.Get("m"); had {
		t.Fatal("the ending was answered, and m's session is still kept")
	}
	if len(dropped.Returned) != 0 || len(dropped.Unreturned) != len(mine) {
		t.Fatalf("returned %d groups and %d unreturned; want all %d of m's unreturned", len(dropped.Returned),
			len(dropped.Unreturned), len(mine))
	}
	if returned, err := s.ShareReturned(); err != nil || len(returned) != 0 {
		t.Fatalf("the file holds returned rows %v (%v) from an ending that returned none", returned, err)
	}
	sameCountsAsTheFile(t, db, s)
}

// **Invariant 18: a write returns only once its transaction has committed.**
// Held behind a publish's commit, a Save has not returned; released, it has,
// and the file holds it. A caller told before the commit would be a client
// told its subscription was kept by a broker that loses it to a crash.
func TestAWriteReturnsOnlyOnceItsTransactionCommitted(t *testing.T) {
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	release := holdACommit(t, db, lg)
	// Two, so both the group's leader and a member that only waits for it
	// are held to the rule.
	done := make(chan error, 2)
	for i, id := range []string{"held-1", "held-2"} {
		go func() { done <- s.Save(aSession(id)) }()
		until(t, "the save to queue", func() bool { return queuedWrites(db) == i+1 })
	}
	select {
	case err := <-done:
		t.Fatalf("a save returned (%v) while the transaction it waits for had not begun", err)
	case <-time.After(200 * time.Millisecond):
	}
	for _, id := range []string{"held-1", "held-2"} {
		if _, had, _ := s.Get(id); had {
			t.Fatalf("the file holds %s before its transaction ran", id)
		}
	}
	release()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	for _, id := range []string{"held-1", "held-2"} {
		if _, had, err := s.Get(id); err != nil || !had {
			t.Fatalf("the save of %s returned, and the file does not hold it: %v, %v", id, had, err)
		}
	}
}

// **sql.ErrNoRows is an answer, not a failure** (DB.note). A write that reads
// a row that is not there - an ending of a session not kept, a disconnect
// with no session, an entry already cleared - is answered for itself, and the
// writes beside it in the same transaction are stored with it.
func TestAReadFindingNothingDoesNotEndTheTransaction(t *testing.T) {
	db := open(t, tempPath(t))
	db.budget = time.Hour // counts transactions: none split for time
	s := sessionsOn(t, db)
	if _, err := db.Log("events"); err != nil { // the held publish's channel, made before counting
		t.Fatal(err)
	}
	before := db.committed.Load()
	errs := behindAHeldCommit(t, db,
		func() error { return s.Save(aSession("a")) },
		func() error { return s.Disconnected("nobody", time.Now(), true, 60) },
		func() error { _, err := s.ClearInFlight("a", 7); return err },
		func() error { _, err := s.Drop("nobody", nil); return err },
		func() error { return s.Save(aSession("b")) },
	)
	if errs[0] != nil || !errors.Is(errs[1], store.ErrNoSession) || errs[2] != nil || errs[3] != nil || errs[4] != nil {
		t.Fatalf("told %v", errs)
	}
	// The held publish's; the four writes that answer a client, together;
	// and ClearInFlight, which answers nobody, in a group of its own (handOn).
	if got := db.committed.Load() - before; got != 3 {
		t.Fatalf("%d transactions committed, want 3: a read that found nothing ended a shared one", got)
	}
	sameCountsAsTheFile(t, db, s)
}

// **An exactly-once publish held, released or dropped, and a consumer's
// position saved or dropped, are a client's writes too**, collected with the
// others: holds and positions behind a held publish are a transaction for the
// holds and one for the positions, not one each, and every one is stored.
func TestHoldsAndPositionsShareTheNextTransaction(t *testing.T) {
	db := open(t, tempPath(t))
	db.budget = time.Hour // counts transactions: none split for time
	lg, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	before := db.committed.Load()
	var writes []func() error
	for i := range SessionGroupMax / 2 {
		writes = append(writes, func() error {
			return lg.Hold(store.Exchange{Client: fmt.Sprint("c-", i), PacketID: 1},
				store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/x", Payload: []byte("p")}, time.Now())
		})
		writes = append(writes, func() error {
			return lg.SavePosition(store.Position{Reader: fmt.Sprint("r-", i), Offset: 1, LastSeen: time.Now()})
		})
	}
	for i, err := range behindAHeldCommit(t, db, writes...) {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// The held publish's, the holds' - a publisher's PUBREC waits on each -
	// and the positions', which answer nobody (handOn).
	if got := db.committed.Load() - before; got != 3 {
		t.Fatalf("a held publish and %d holds and positions behind it took %d transactions, want 3",
			len(writes), got)
	}
	holds, err := lg.Holds()
	if err != nil || len(holds) != SessionGroupMax/2 {
		t.Fatalf("the file holds %d held publishes (%v), want %d", len(holds), err, SessionGroupMax/2)
	}
	for i := range SessionGroupMax / 2 {
		if _, had, err := lg.Position(fmt.Sprint("r-", i)); err != nil || !had {
			t.Fatalf("position r-%d: had %v, %v", i, had, err)
		}
	}
}

// **Publishes and client writes take turns** (DB.handOn). A publish arriving
// behind two groups of client writes - a full one and one more - is stored
// between them, not after both: strictly oldest first, a publisher arriving
// during a reconnect storm waited behind every write group queued before it,
// and a PUBACK's median went from 0.19ms to 8ms with 10,000 sessions
// reconnecting.
func TestAPublishWaitsBehindOneGroupOfClientWritesAtMost(t *testing.T) {
	db := open(t, tempPath(t))
	db.budget = time.Hour // counts transactions: none split for time
	s := sessionsOn(t, db)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	release := holdACommit(t, db, lg)
	var wg sync.WaitGroup
	errs := make(chan error, SessionGroupMax+3)
	for i := range SessionGroupMax {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Save(aSession(fmt.Sprint("first-", i))) }()
	}
	until(t, "a full group of writes to queue", func() bool { return queuedWrites(db) == SessionGroupMax })
	// One more, in a second group; it counts the records stored before it.
	var seen int
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- db.joinWrite(&write{run: func(tx *sql.Tx) error {
			return db.queryRow(tx, `SELECT count(*) FROM records WHERE channel = ?`, "events").Scan(&seen)
		}})
	}()
	until(t, "the second group to queue", func() bool { return queuedWrites(db) == SessionGroupMax+1 })
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := lg.Append(store.Record{MessageID: "behind", Topic: "events/x", Payload: []byte("p")})
		errs <- err
	}()
	until(t, "the publish to queue", func() bool { pubs, _ := queuedBehind(db); return pubs == 1 })
	release()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if seen != 2 {
		t.Fatalf("the second group of writes ran with %d records stored, want 2 - the held publish and the one "+
			"that arrived after it: the publish waited behind both groups", seen)
	}
}

// **A check that fails with anything but a refusal ends the transaction**
// (attempt). A later step of a multi-row read can fail - SQLITE_IOERR,
// SQLITE_NOMEM - after SQLite has already rolled the whole transaction back,
// and the statement helpers see only a query's first step (note). Taken for a
// refusal, the next member's write would run outside any transaction and
// commit by itself while the COMMIT failed for everyone: a write told it
// failed, and stored. Here a check rolls the transaction back as SQLite
// would, and answers an error that is not one of the refusals.
func TestACheckThatFailsEndsTheTransaction(t *testing.T) {
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	var tries int
	failing := &write{
		// In the same transaction as the saves either side: a record
		// written with no packet of its client's waiting is a departed
		// client's (Sessions.Save).
		departed: true,
		check: func(tx *sql.Tx) error {
			tries++
			if _, err := tx.Exec(`ROLLBACK`); err != nil { // as SQLite does, unseen
				return err
			}
			return errors.New("a later step of the read failed")
		},
		run: func(*sql.Tx) error { return nil },
	}
	errs := behindAHeldCommit(t, db,
		func() error { return s.Save(aSession("before")) },
		func() error { return db.joinWrite(failing) },
		func() error { return s.Save(aSession("after")) },
	)
	if errs[0] != nil || errs[1] == nil || errs[2] != nil {
		t.Fatalf("told %v; want the two saves stored and the failing check's write refused", errs)
	}
	for _, id := range []string{"before", "after"} {
		if _, had, err := s.Get(id); err != nil || !had {
			t.Fatalf("%s was told it was stored, and the file does not hold it: %v, %v", id, had, err)
		}
	}
	if tries < 2 {
		t.Fatalf("the failing check ran %d times, want it tried again alone", tries)
	}
	sameCountsAsTheFile(t, db, s)
}

// **A group of client writes gives way to a publish after the member in
// progress** (attempt, yield). A publish that arrives while the first member
// of a full group runs is stored before the second member runs - not after the
// whole group, which with groups of 64 put a reconnect storm's PUBACK median at
// 3.3ms - and every write is still stored.
func TestAWriteGroupGivesWayToAPublishAfterOneMember(t *testing.T) {
	db := open(t, tempPath(t))
	db.budget = time.Hour // counts transactions: none split for time
	s := sessionsOn(t, db)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	published := make(chan error, 1)
	var storedBySecond int
	calls := 0
	db.afterMember = func(*write) {
		calls++
		switch calls {
		case 1: // the first member has run: a publish arrives
			go func() {
				_, err := lg.Append(store.Record{MessageID: "arrives", Topic: "events/x", Payload: []byte("p")})
				published <- err
			}()
			until(t, "the publish to queue", func() bool { pubs, _ := queuedBehind(db); return pubs == 1 })
		case 2: // the second has run: what is stored, read beside the writer
			if err := db.reads.QueryRow(`SELECT count(*) FROM records WHERE channel = ?`, "events").Scan(&storedBySecond); err != nil {
				t.Errorf("count the records: %v", err)
			}
		}
	}
	t.Cleanup(func() { db.afterMember = nil })
	var writes []func() error
	for i := range SessionGroupMax {
		writes = append(writes, func() error { return s.Save(aSession(fmt.Sprint("w-", i))) })
	}
	for i, err := range behindAHeldCommit(t, db, writes...) {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if storedBySecond != 2 {
		t.Fatalf("the second member of the group ran with %d records stored, want 2 - the held publish and "+
			"the one that arrived during the first member: the group did not give way", storedBySecond)
	}
	if s.Len() != SessionGroupMax {
		t.Fatalf("the store counts %d sessions, want %d", s.Len(), SessionGroupMax)
	}
	sameCountsAsTheFile(t, db, s)
}

// **A group of client writes that has run for writeGroupBudget commits what
// it has and queues the rest again** (attempt), with no publish waiting: the
// first member here takes longer than the budget, so the group is two
// transactions, and every write is stored.
func TestAWriteGroupPastItsBudgetCommitsWhatItHas(t *testing.T) {
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	// **Past the budget once, and only once.** The first member sleeps past
	// it, which a sleep does deterministically - it never returns early. The
	// rest is a group of its own, and runs with the budget out of reach:
	// counted at three members of 20-40us against 5ms, a busy machine
	// descheduling the leader for a few milliseconds cut it a second time,
	// and the count read four for a group that did exactly what it should.
	// The leader sets it, under the writer every leader holds.
	calls := 0
	db.afterMember = func(*write) {
		switch calls++; calls {
		case 1:
			time.Sleep(writeGroupBudget + time.Millisecond)
		case 2:
			db.budget = time.Hour
		}
	}
	t.Cleanup(func() { db.afterMember = nil })
	before := db.committed.Load()
	var writes []func() error
	for i := range 4 {
		writes = append(writes, func() error { return s.Save(aSession(fmt.Sprint("w-", i))) })
	}
	for i, err := range behindAHeldCommit(t, db, writes...) {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if got := db.committed.Load() - before; got != 3 {
		t.Fatalf("%d transactions, want 3: the held publish's, the first member's once it ran past the "+
			"budget, and the rest's", got)
	}
	sameCountsAsTheFile(t, db, s)
}

// **A write waits for another writer's lock rather than being refused at
// once** (writerPragmas). A write group's members read before they write,
// inside the transaction, and a deferred transaction that has read is
// answered SQLITE_BUSY the moment it tries to write while another connection
// holds the file's write lock - without the busy timeout. Begun IMMEDIATE, it
// waits at BEGIN, and is stored once the lock is let go.
func TestAWriteWaitsForAnotherWritersLock(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	s := sessionsOn(t, db)
	other, err := sql.Open(driverName, "file:"+path+pragmas)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	conn, err := other.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("take the file's write lock: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Save(aSession("waits")) }()
	// **At the lock before anything is asked of it.** The save's group has
	// begun once its leader is committing, and a transaction begun IMMEDIATE
	// is inside BEGIN until the lock is let go: a writer that began - or a
	// save that returned - while the other connection holds the lock did not
	// wait. Watched for 300ms from there, rather than from before the save
	// was known to have started, which on a busy machine could be before it
	// had asked for anything.
	until(t, "the save to begin", func() bool {
		db.forming.Lock()
		defer db.forming.Unlock()
		return db.committing
	})
	for watch := time.Now().Add(300 * time.Millisecond); time.Now().Before(watch); time.Sleep(time.Millisecond) {
		select {
		case err := <-done:
			t.Fatalf("the save returned %v while another connection held the write lock, rather than waiting", err)
		default:
		}
		if db.writeTx.Load() != nil {
			t.Fatal("the save began its transaction while another connection held the write lock: " +
				"begun deferred, it will be refused SQLITE_BUSY at its first write rather than wait")
		}
	}
	if _, err := conn.ExecContext(t.Context(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("once the lock was let go, the save: %v", err)
	}
	if _, had, err := s.Get("waits"); err != nil || !had {
		t.Fatalf("the save returned, and the file does not hold it: %v, %v", had, err)
	}
}

// ordered is a write that records its name, in the order the transactions
// run it, and writes nothing.
func ordered(order *[]string, name string, answers bool) *write {
	return &write{answers: answers, run: func(*sql.Tx) error {
		*order = append(*order, name) // run by one leader at a time
		return nil
	}}
}

// queueBehindAHeldCommit joins each write in turn while a publish is held
// inside its transaction, waiting for each to be queued before the next, then
// releases it and waits for them all.
func queueBehindAHeldCommit(t *testing.T, db *DB, ws ...*write) {
	t.Helper()
	var calls []func() error
	for _, w := range ws {
		calls = append(calls, func() error { return db.joinWrite(w) })
	}
	for i, err := range behindAHeldCommit(t, db, calls...) {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
}

// **A group cut short keeps its place** (joinWrite). Its first member runs
// past the budget, so the transaction commits it and the rest of the group
// queues again - at the front, ahead of the group that joined after it.
// Queued at the back, it went behind every group queued while it ran, and on
// a Raspberry Pi, where the budget cuts every few writes, a reconnecting
// client's session write went to the back again and again: CONNACKs of up to
// 10s.
func TestAGroupCutShortKeepsItsPlace(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	var order []string
	calls := 0
	db.afterMember = func(*write) {
		if calls++; calls == 1 {
			time.Sleep(db.budget + time.Millisecond)
		}
	}
	t.Cleanup(func() { db.afterMember = nil })
	var ws []*write
	for i := range SessionGroupMax {
		ws = append(ws, ordered(&order, fmt.Sprint("first-", i), false))
	}
	ws = append(ws, ordered(&order, "second", false))
	queueBehindAHeldCommit(t, db, ws...)
	want := make([]string, 0, SessionGroupMax+1)
	for i := range SessionGroupMax {
		want = append(want, fmt.Sprint("first-", i))
	}
	want = append(want, "second")
	if !slices.Equal(order, want) {
		t.Fatalf("ran %v, want %v: the rest of the group cut short went behind the group after it", order, want)
	}
}

// **A remainder holding the write of a client now waiting answers** (joinWrite).
// A group of departed writes runs its first member past the budget and gives
// way, and meanwhile the client of the second is marked waiting - its write
// was in the running group, so there was nothing queued for the mark to move -
// and a connected session's write joins, with the departed clients' four turns
// in a row (departedBeforeConnected) just used up. The remainder is promoted
// when it is queued again, so it is no longer one of the departed's and runs
// first. Left a departed group, it gave the turn to the connected session's.
func TestARemainderHoldingAWaitingClientsWriteAnswers(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	var order []string
	var answered func()
	var wg sync.WaitGroup
	calls := 0
	db.afterMember = func(*write) {
		if calls++; calls != 1 {
			return
		}
		answered = db.Waiting("target")
		db.forming.Lock()
		db.departedRun = departedBeforeConnected - 1 // this group's own run is the fourth
		db.forming.Unlock()
		conn := clientWrite(&order, "connected", false)
		wg.Add(1)
		go func() { defer wg.Done(); _ = db.joinWrite(conn) }()
		// Not t.Fatal: this runs on the leader's goroutine.
		for deadline := time.Now().Add(10 * time.Second); queuedWrites(db) != 1; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Error("the connected session's write never queued")
				return
			}
		}
		time.Sleep(db.budget + time.Millisecond)
	}
	t.Cleanup(func() { db.afterMember = nil })
	queueBehindAHeldCommit(t, db, clientWrite(&order, "first", true), clientWrite(&order, "target", true))
	wg.Wait()
	if answered == nil {
		t.Fatal("the group never ran its first member, so this proves nothing")
	}
	answered()
	if want := []string{"first", "target", "connected"}; !slices.Equal(order, want) {
		t.Fatalf("ran %v, want %v: the remainder holding a waiting client's write did not answer", order, want)
	}
}

// **A write a client is waiting on is served ahead of the writes nobody is
// waiting on** (queueWrites, handOn): behind three full groups of
// acknowledgement-like writes, a session save runs before any of them. In
// the queue case's return on an emulated Raspberry Pi, oldest-first put a
// reconnecting client's save behind ~120 such groups.
func TestAWriteAClientWaitsOnGoesAheadOfTheRest(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	db.budget = time.Hour
	var order []string
	var ws []*write
	for i := range 3 * SessionGroupMax {
		ws = append(ws, ordered(&order, fmt.Sprint("background-", i), false))
	}
	ws = append(ws, ordered(&order, "answers", true))
	queueBehindAHeldCommit(t, db, ws...)
	if at := slices.Index(order, "answers"); at != 0 {
		t.Fatalf("the write a client waits on ran after %d writes nobody waits on: %v", at, order)
	}
}

// **The writes nobody waits on still get a turn** (handOn): one of them,
// queued behind five groups of writes clients wait on, runs after four of
// them - the fifth turn is its - so a stream of connects cannot hold
// acknowledgements back for ever.
func TestTheOtherWritesGetEveryFifthTurn(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	db.budget = time.Hour
	var order []string
	ws := []*write{ordered(&order, "background", false)}
	for i := range 5 * SessionGroupMax {
		ws = append(ws, ordered(&order, fmt.Sprint("answers-", i), true))
	}
	queueBehindAHeldCommit(t, db, ws...)
	if at, want := slices.Index(order, "background"), answersBeforeBackground*SessionGroupMax; at != want {
		t.Fatalf("the write nobody waits on ran after %d answering writes, want %d (%d groups): %v",
			at, want, answersBeforeBackground, order)
	}
}

// **A group of client writes gives way to a publish whatever
// publish_commit_interval says** (RFC 0004 "Group commit"): with `none` and
// with a duration a publish does not queue in DB.waiting, so the group has to
// see it another way (DB.publishes). The group is the writes behind one that
// holds the writer; a publish arrives while its first member runs, and is
// stored before the second member runs.
func TestAWriteGroupGivesWayToAPublishWhateverTheCommitInterval(t *testing.T) {
	for _, mode := range []struct {
		name     string
		interval time.Duration
		records  int
	}{{"none", 0, 0}, {"duration", 5 * time.Millisecond, 64}} {
		t.Run(mode.name, func(t *testing.T) {
			for trial := range 3 {
				db := open(t, tempPath(t))
				db.CommitGroup(mode.interval, mode.records)
				db.budget = time.Hour
				s := sessionsOn(t, db)
				lg, err := db.Log("events")
				if err != nil {
					t.Fatal(err)
				}
				published := make(chan error, 1)
				var storedBySecond int
				calls := 0
				db.afterMember = func(*write) {
					calls++
					switch calls {
					case 2: // the group's first member has run: a publish arrives
						go func() {
							_, err := lg.Append(store.Record{MessageID: "arrives", Topic: "events/x", Payload: []byte("p")})
							published <- err
						}()
						// It waits on the writer, which this group holds, counted
						// in DB.publishes from before it waits (runBatchSeen):
						// what the group looks at before its next member.
						until(t, "the publish to be counted", func() bool { return uint32(db.publishes.Load()) > 0 })
					case 3:
						if err := db.reads.QueryRow(`SELECT count(*) FROM records WHERE channel = ?`, "events").Scan(&storedBySecond); err != nil {
							t.Errorf("count the records: %v", err)
						}
					}
				}
				// The first write leads and waits for the writer, which is held;
				// the others queue behind it as the group.
				db.writing.Lock()
				var wg sync.WaitGroup
				errs := make([]error, 4)
				for i := range errs {
					wg.Add(1)
					go func() {
						defer wg.Done()
						errs[i] = s.Save(aSession(fmt.Sprint("w-", i)))
					}()
					if i > 0 {
						until(t, "the write to queue", func() bool { return queuedWrites(db) == i })
					} else {
						until(t, "the first write to start", func() bool {
							db.forming.Lock()
							defer db.forming.Unlock()
							return db.committing
						})
					}
				}
				db.writing.Unlock()
				wg.Wait()
				if err := <-published; err != nil {
					t.Fatal(err)
				}
				for i, err := range errs {
					if err != nil {
						t.Fatalf("write %d: %v", i, err)
					}
				}
				if calls < 4 {
					t.Fatalf("trial %d: %d members ran, want 4: the instrument did not run the group", trial, calls)
				}
				if storedBySecond != 1 {
					t.Fatalf("trial %d: the group's second member ran with %d records stored, want 1 - the publish "+
						"that arrived during the first: the group did not give way", trial, storedBySecond)
				}
				sameCountsAsTheFile(t, db, s)
			}
		})
	}
}

// **A group that gave way never waits for a publish that has already
// ended** (letPublishesIn). It looks at how many publishes are running and
// waits for that many to end; a publish that ends while it looks must count
// once, as running or as ended, or the group waits for one more publish than
// will ever end, and its members, and every group queued behind it, wait for
// some unrelated publish to come along. The publish here ends at exactly that
// moment (DB.lookedAtPublishes), and the group must go on without another.
func TestAGroupThatGaveWayDoesNotWaitForAPublishThatHasEnded(t *testing.T) {
	for _, mode := range []struct {
		name     string
		interval time.Duration
		records  int
	}{{"none", 0, 0}, {"duration", time.Millisecond, 64}} {
		t.Run(mode.name, func(t *testing.T) {
			db := open(t, tempPath(t))
			db.CommitGroup(mode.interval, mode.records)
			db.budget = time.Hour
			lg, err := db.Log("events")
			if err != nil {
				t.Fatal(err)
			}
			publish := func(id string) chan error {
				done := make(chan error, 1)
				go func() {
					_, err := lg.Append(store.Record{MessageID: id, Topic: "events/x", Payload: []byte("p")})
					done <- err
				}()
				return done
			}

			// A publish waits for the writer, counted as running: a group
			// running now would give way to it (shouldYield).
			db.writing.Lock()
			first := publish("first")
			until(t, "the publish to be counted as running", func() bool { return db.shouldYield(time.Now()) })

			looked := 0
			db.lookedAtPublishes = func() {
				looked++
				db.writing.Unlock()
				if err := <-first; err != nil {
					t.Errorf("the publish: %v", err)
				}
			}
			gone := make(chan struct{})
			go func() {
				db.letPublishesIn()
				close(gone)
			}()
			select {
			case <-gone:
			case <-time.After(2 * time.Second):
				// Released by an unrelated publish, as the broker's would be.
				if err := <-publish("unrelated"); err != nil {
					t.Error(err)
				}
				<-gone
				t.Fatal("the group waited for a publish that had ended, until an unrelated one did")
			}
			if looked != 1 {
				t.Fatalf("the publish ended while the group looked %d times, want 1", looked)
			}
		})
	}
}
