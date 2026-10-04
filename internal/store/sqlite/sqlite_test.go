package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

func open(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path, "0.1.0-test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "saguin.db")
}

// The settings the durability argument rests on have to be the ones the
// connection actually has. They ride in the DSN rather than being executed
// after opening, which is easy to get subtly wrong and impossible to
// notice: a database at synchronous=OFF behaves identically until the
// power goes.
func TestTheDurabilityPragmasAreWhatWeAskedFor(t *testing.T) {
	db := open(t, tempPath(t))

	var mode string
	if err := db.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}

	// 1 is NORMAL. 2 is FULL, which syncs on every commit and would make
	// every acknowledged publish an fsync; 0 is OFF, which drops the
	// checkpoint sync and can leave a file that does not open.
	var sync int
	if err := db.db.QueryRow(`PRAGMA synchronous`).Scan(&sync); err != nil {
		t.Fatalf("synchronous: %v", err)
	}
	if sync != 1 {
		t.Errorf("synchronous = %d, want 1 (NORMAL)", sync)
	}
}

// The file holds whatever applications published, so it is not readable by
// everyone who happens to have an account on the box.
func TestTheDatabaseIsNotWorldReadable(t *testing.T) {
	path := tempPath(t)
	open(t, path)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("the database is mode %04o, want 0600", perm)
	}
}

// Invariant 15: a restart never resurrects a delivery.
//
// Every record a worker held comes back available, with no Delivery ID, no
// holder and no deadline - a restored deadline belongs to a session that no
// longer exists, and a restored Delivery ID could be resolved by a client
// retrying across the very restart that interrupted it.
//
// It holds by construction rather than by a recovery step: none of those
// three is stored, so there is nothing to undo at start. What is asserted
// here is the behaviour that fact is supposed to buy, through the same calls
// the broker makes - a table with the columns absent would prove only that
// they are absent.
//
// What survives is the attempt count and the delivery history, so a record
// that had spent its attempts is still dead-lettered rather than starting
// over, and a dead-lettered record can still say why it failed.
func TestOpeningBringsNothingBackInFlight(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)

	q, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if _, err := q.Enqueue(store.Record{MessageID: "j-1", Topic: "jobs/held"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Out with a worker, acknowledged at the transport, deadline running.
	now := time.Now()
	offered, err := q.Offer(1, time.Now())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer: %v (%d records)", err, len(offered))
	}
	before := store.Held{Offset: 1, Epoch: offered[0].Epoch, Holder: "worker-1"}
	if _, ok, err := q.Lease(before, now, time.Hour); err != nil || !ok {
		t.Fatalf("lease: ok=%v err=%v", ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The restart, with an hour still to run on that lease.
	db = open(t, path)
	q, err = db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue after the restart: %v", err)
	}

	// Nothing is holding it: it is work again, immediately, rather than
	// after a deadline nobody is racing.
	if e, err := q.ExpiredLeases(now.Add(2 * time.Hour)); err != nil || len(e) != 0 {
		t.Fatalf("%d leases came back from before the restart (err=%v)", len(e), err)
	}
	again, err := q.Offer(1, time.Now())
	if err != nil {
		t.Fatalf("offer after the restart: %v", err)
	}
	if len(again) != 1 {
		t.Fatal("the record a worker was holding did not come back as work; it is " +
			"stranded until a deadline that no session is racing")
	}
	if again[0].MessageID != "j-1" {
		t.Fatalf("came back as %q, want j-1", again[0].MessageID)
	}
	if again[0].DeliveryID == offered[0].DeliveryID {
		t.Fatal("the Delivery ID survived the restart; a client retrying across it " +
			"could resolve a delivery that no longer exists (invariant 15)")
	}

	// The attempt count did survive, so a record that had spent its attempts
	// is dead-lettered rather than starting over.
	if again[0].Attempt != 2 {
		t.Fatalf("came back as attempt %d, want 2 - the attempt count did not survive, "+
			"so this record will never exhaust its retries", again[0].Attempt)
	}
}

// Invariant 13: a position outlives its session for nobody. One that would
// have expired while the broker was down is dropped when the file opens,
// or a mistyped client id leaves a row behind for ever.
func TestOpeningDropsPositionsWhoseSessionsExpired(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if err := db.EnsureChannel("events", store.KindAppend); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	_, err := db.db.Exec(
		`INSERT INTO positions (channel, reader, "offset", last_seen, expires_in) VALUES (?,?,?,?,?), (?,?,?,?,?)`,
		"events", store.MQTTReader("gone"), 3, time.Now().Add(-time.Hour).UnixNano(), int64(time.Second),
		"events", store.MQTTReader("here"), 7, time.Now().Add(-time.Hour).UnixNano(), int64(24*time.Hour))
	if err != nil {
		t.Fatalf("seeding positions: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = open(t, path)

	var readers []string
	rows, err := db.db.Query(`SELECT reader FROM positions ORDER BY reader`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("scan: %v", err)
		}
		readers = append(readers, r)
	}
	if len(readers) != 1 || readers[0] != store.MQTTReader("here") {
		t.Errorf("positions after the restart = %v, want only %q", readers, store.MQTTReader("here"))
	}
}

// Invariants 1 and 9, on the path that breaks both: a channel retention
// has emptied. The counters are columns and no removal touches them except
// to raise the floor, so an empty table still says where the channel had
// got to and what it lost.
func TestAnEmptiedChannelKeepsItsCounters(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if err := db.EnsureChannel("events", store.KindAppend); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if _, err := db.db.Exec(`UPDATE channels SET next = 5001, floor = 5001 WHERE name = 'events'`); err != nil {
		t.Fatalf("seeding counters: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = open(t, path)
	// EnsureChannel runs again on every start and must not reset anything.
	if err := db.EnsureChannel("events", store.KindAppend); err != nil {
		t.Fatalf("ensure after restart: %v", err)
	}

	var next, floor int64
	if err := db.db.QueryRow(`SELECT next, floor FROM channels WHERE name = 'events'`).
		Scan(&next, &floor); err != nil {
		t.Fatalf("query: %v", err)
	}
	if next != 5001 || floor != 5001 {
		t.Errorf("an emptied channel came back next=%d floor=%d, want 5001 and 5001", next, floor)
	}
}

// A channel that changed type must not load. Reading append records out of
// a queue's table, or taking queue work into a log where nothing can
// acknowledge it, reports no problem afterwards.
func TestAChannelCannotChangeType(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if err := db.EnsureChannel("events", store.KindAppend); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := db.EnsureChannel("events", store.KindQueue); err == nil {
		t.Fatal("a channel stored as append was accepted as a queue")
	}
}

// RFC 0004: a database written before durable broadcast became one log -
// user_version 14, which kept each session's broadcast as held deliveries -
// is refused by name rather than read, and nothing converts it.
func TestADatabaseFromBeforeTheBroadcastLogIsRefused(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if _, err := db.db.Exec(`PRAGMA user_version = 14`); err != nil {
		t.Fatalf("setting the version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := Open(path, "0.1.0-test")
	if err == nil {
		t.Fatal("a database from schema version 14 opened without complaint")
	}
	for _, want := range []string{"schema version 14", fmt.Sprintf("reads version %d", schemaVersion)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// RFC 0004: a database from before a shared group's backlog became the
// broadcast log - user_version 17, which kept it in share_backlog - is
// refused by name rather than opened, since nothing would read the backlog
// it can hold.
func TestADatabaseHoldingAShareBacklogTableIsRefused(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	for _, q := range []string{
		`CREATE TABLE share_backlog (share_group TEXT NOT NULL, seq INTEGER NOT NULL,
			message_id TEXT NOT NULL, topic TEXT NOT NULL, payload BLOB, headers TEXT,
			qos INTEGER NOT NULL, ts INTEGER NOT NULL, held INTEGER NOT NULL, props TEXT,
			PRIMARY KEY (share_group, seq)) STRICT`,
		`INSERT INTO share_backlog VALUES ('$share/g/t', 1, 'm-1', 't', x'6f6e', NULL, 1, 1, 1, NULL)`,
		`PRAGMA user_version = 17`,
	} {
		if _, err := db.db.Exec(q); err != nil {
			t.Fatalf("writing a version 17 file: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := Open(path, "0.1.0-test")
	if err == nil {
		t.Fatal("a database from schema version 17, holding a shared group's backlog, opened without complaint")
	}
	for _, want := range []string{"schema version 17", fmt.Sprintf("reads version %d", schemaVersion)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// RFC 0004: a database from before a session's deliveries were only the
// broadcast log - user_version 18, which could keep one in session_messages -
// is refused by name rather than opened, since nothing would read it.
func TestADatabaseHoldingASessionsMessagesIsRefused(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	for _, q := range []string{
		`CREATE TABLE session_messages (client TEXT NOT NULL, packet_id INTEGER NOT NULL,
			seq INTEGER NOT NULL, qos INTEGER NOT NULL, state INTEGER NOT NULL, message_id TEXT NOT NULL,
			topic TEXT NOT NULL, payload BLOB, headers TEXT, ts INTEGER NOT NULL, props TEXT,
			PRIMARY KEY (client, packet_id)) STRICT`,
		`INSERT INTO session_messages VALUES ('member', 1, 1, 1, 2, 'm-1', 'events/x', x'6f6e', NULL, 1, NULL)`,
		`PRAGMA user_version = 18`,
	} {
		if _, err := db.db.Exec(q); err != nil {
			t.Fatalf("writing a version 18 file: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := Open(path, "0.1.0-test")
	if err == nil {
		t.Fatal("a database from schema version 18, holding a session's message, opened without complaint")
	}
	for _, want := range []string{"schema version 18", fmt.Sprintf("reads version %d", schemaVersion)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// RFC 0004: a database from before a broadcast log message could be owed to
// one session - user_version 19 - is refused by name, as every other
// version is: there is no in-place upgrade.
func TestADatabaseFromBeforeTheSessionMarkIsRefused(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if _, err := db.db.Exec(`PRAGMA user_version = 19`); err != nil {
		t.Fatalf("setting the version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := Open(path, "0.1.0-test")
	if err == nil {
		t.Fatal("a database from schema version 19 opened without complaint")
	}
	for _, want := range []string{"schema version 19", fmt.Sprintf("reads version %d", schemaVersion)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// A file this broker does not fully understand is refused by name. Opening
// it and reading what happens to parse is how a broker starts with
// plausible-looking wrong state and says nothing (invariant 14).
func TestAFileFromAnotherSchemaVersionIsRefused(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	if _, err := db.db.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatalf("bumping the version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := Open(path, "0.1.0-test")
	if err == nil {
		t.Fatal("a database from schema version 99 opened without complaint")
	}
	for _, want := range []string{"99", "0.1.0-test"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// A database with tables and no version is something an older saguin might
// have left. Guessing that it is empty and creating the schema over it is
// the one response that loses data silently.
func TestADatabaseWithTablesAndNoVersionIsRefused(t *testing.T) {
	path := tempPath(t)
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE something (x INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	if _, err := Open(path, "0.1.0-test"); err == nil {
		t.Fatal("a database holding unknown tables and no version was adopted")
	}
}

// One broker writes a provider's file.
//
// Nothing in SQLite enforces that: two processes may open one database and
// both write to it, and WAL is designed to let them. Nothing in saguin
// would notice either - each caches a channel's next offset, so both assign
// the same one, the primary key refuses whichever commits second, and that
// broker's counter never advances so it refuses every publish on the
// channel from then on while looking healthy.
//
// Two brokers from one configuration usually collide on the listener first.
// It is two configurations naming one file_path that gets through, which is
// what including the same channel file from two masters would do.
func TestASecondBrokerIsRefusedTheSameDatabase(t *testing.T) {
	path := tempPath(t)
	first := open(t, path)

	if _, err := Open(path, "second-broker"); err == nil {
		t.Fatal("a second broker opened a database another one is writing; both will " +
			"assign the same offsets and one will refuse every publish thereafter")
	} else if !strings.Contains(err.Error(), "another broker holds it") {
		t.Fatalf("the refusal does not say what happened: %v", err)
	}

	// The database itself stays readable while the lock is held. That is the
	// reason the lock is a file of its own rather than the database opened
	// exclusively: an operator can query a live channel with ordinary tools.
	reader, err := sql.Open("sqlite",
		"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=query_only(true)")
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	defer reader.Close()
	var n int
	if err := reader.QueryRow(
		`SELECT count(*) FROM sqlite_schema WHERE type = 'table'`).Scan(&n); err != nil {
		t.Fatalf("a live database could not be read while the broker holds it: %v", err)
	}
	if n == 0 {
		t.Fatal("the reader saw no tables in a prepared database")
	}

	// And the lock goes with the broker, so a restart is not blocked by the
	// broker it replaces.
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	again, err := Open(path, "after-the-first-left")
	if err != nil {
		t.Fatalf("the lock outlived the broker that held it: %v", err)
	}
	defer func() { _ = again.Close() }()

	// The case a deployment is actually in: the lock file was left by an
	// earlier run, so it already exists and already holds its table.
	//
	// Everything above this line passes with no lock being taken at all,
	// which is what made the defect invisible. A lock is claimed by writing,
	// and the write was CREATE TABLE IF NOT EXISTS - which writes nothing
	// once the table is there. Both brokers then took shared locks and both
	// started, on every run but the first.
	if _, err := Open(path, "third-broker"); err == nil {
		t.Fatal("a second broker opened the database once the lock file already existed: " +
			"the guard protects only the first run in a deployment, and every restart after " +
			"that leaves it inert")
	} else if !strings.Contains(err.Error(), "another broker holds it") {
		t.Fatalf("the refusal does not say what happened: %v", err)
	}
}

// Invariant 9, and the reason a lock is not only a database's business.
//
// A memory provider has no database to take a lock with, and two brokers
// sharing its snapshot directory fail worse than two sharing a database.
// Both hold entirely separate state and both write at shutdown, one file
// per channel with an atomic rename, so the later one wins: the earlier
// broker's records are gone with nothing reported, and the channel's next
// offset goes *backwards*, which is offsets being reused. A stored consumer
// position then points at unrelated data, which the consumer reads in order
// and reports as success.
//
// Measured before this lock existed: three records replaced by one, next
// from 4 to 2, and no warning of any kind.
func TestASecondBrokerIsRefusedTheSameSnapshotDirectory(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "saguin.lock")

	first, err := Hold(name, dir)
	if err != nil {
		t.Fatalf("the first broker could not take the lock: %v", err)
	}
	if first.Path() != name {
		t.Fatalf("lock path %q, want %q", first.Path(), name)
	}

	if _, err := Hold(name, dir); err == nil {
		t.Fatal("a second broker took a snapshot directory another one is writing; at " +
			"shutdown the later writer replaces the earlier one's channels and moves " +
			"the next offset backwards, with nothing reported (invariant 9)")
	} else if !strings.Contains(err.Error(), "another broker holds it") {
		t.Fatalf("the refusal does not say what happened: %v", err)
	}

	// And it goes with the broker, so a restart is not blocked by the one it
	// replaces.
	if err := first.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	again, err := Hold(name, dir)
	if err != nil {
		t.Fatalf("the lock outlived the broker that held it: %v", err)
	}
	defer func() { _ = again.Release() }()

	// The case a deployment is actually in, and the one everything above
	// passes without: the lock file was left by an earlier run, so it exists
	// and already holds its table. A lock is claimed by writing, and CREATE
	// TABLE IF NOT EXISTS writes nothing once the table is there - so both
	// brokers took shared locks, which do not exclude each other, and both
	// started. Two of them on one snapshot directory is the silent loss in
	// the comment above.
	if _, err := Hold(name, dir); err == nil {
		t.Fatal("a second broker took the snapshot directory once the lock file already " +
			"existed: the guard protects only the first run, and a lock file exists on " +
			"every run after it")
	} else if !strings.Contains(err.Error(), "another broker holds it") {
		t.Fatalf("the refusal does not say what happened: %v", err)
	}
}

// **RFC 0004 states the schema version, and it went stale the commit after
// it was written.** The same shape as the snapshot check in internal/store:
// the constant moves here and the document is edited separately, so nothing
// holds them together.
//
// It matters more than the number looks. A version mismatch is a hard
// refusal with no in-place upgrade, so the document's version is what an
// operator compares their file against before finding out the broker will
// not open it.
func TestRFC0004StatesTheSchemaVersion(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "rfcs", "0004-storage.md"))
	if err != nil {
		t.Fatalf("read RFC 0004: %v", err)
	}
	m := regexp.MustCompile("at `user_version` (\\d+)").FindSubmatch(md)
	if m == nil {
		t.Fatalf("RFC 0004 no longer states the schema version; it is what an operator " +
			"compares a file against, so say it or move this check")
	}
	said, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatalf("RFC 0004's schema version is not a number: %q", m[1])
	}
	if said != schemaVersion {
		t.Errorf("RFC 0004 says the database is at user_version %d and the broker writes "+
			"%d - an operator reads that number to work out whether their file will "+
			"open, and it is refused rather than upgraded", said, schemaVersion)
	}
}

// Group commit: the publishes that arrive together share one transaction.
//
// Every test here drives the real publish paths concurrently, because a
// batch that never has more than one member is the ungrouped path wearing
// its name - and that is how this would fail silently. Each one says how
// many publishers it ran and asserts they landed in the same transaction,
// or asserts the trigger it is about fired before the other one could.

// A batch closes on whichever of the two comes first, and both directions
// have to be driven: with a long interval it is the record count that ends
// it, and with a large count it is the interval.
//
// The test asserts the timing rather than the contents, because the
// contents are identical either way - which is exactly why a broker that
// only ever fires one of the triggers would pass every other test here.
func TestABatchClosesOnWhicheverComesFirst(t *testing.T) {
	t.Run("the record count, before the interval could", func(t *testing.T) {
		db := open(t, tempPath(t))
		// An interval long enough that reaching it would fail the test.
		db.CommitGroup(30*time.Second, 4)
		lg, err := db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}

		start := time.Now()
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")})
			}()
		}
		wg.Wait()
		took := time.Since(start)

		for i, err := range errs {
			if err != nil {
				t.Fatalf("publisher %d: %v", i, err)
			}
		}
		if took > 5*time.Second {
			t.Fatalf("four publishers filling a batch of four took %v: the record count did not close it", took)
		}
		if n, err := lg.Len(); err != nil || n != 4 {
			t.Fatalf("stored %d records (err %v), want 4", n, err)
		}
	})

	t.Run("the interval, before the count could", func(t *testing.T) {
		db := open(t, tempPath(t))
		// A count no single publisher can reach.
		db.CommitGroup(80*time.Millisecond, 1000)
		lg, err := db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}

		start := time.Now()
		if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")}); err != nil {
			t.Fatalf("append: %v", err)
		}
		took := time.Since(start)

		if took < 60*time.Millisecond {
			t.Fatalf("one publisher was answered in %v with a batch of 1000 and an 80ms interval: "+
				"it did not wait for the interval, so nothing would ever collect", took)
		}
		if took > 3*time.Second {
			t.Fatalf("one publisher waited %v for an 80ms interval", took)
		}
		if n, err := lg.Len(); err != nil || n != 1 {
			t.Fatalf("stored %d records (err %v), want 1", n, err)
		}
	})
}

// The whole point, driven: many publishers, few transactions.
//
// Counting transactions is what proves batching happened at all. Nothing
// else here can tell the difference - the records, the offsets and the
// counters come out identical whether they were stored one at a time or
// two hundred at a time, which is why a test that only reads them back
// passes just as well against a broker that ignores the setting.
func TestABatchStoresManyPublishesInOneTransaction(t *testing.T) {
	const publishers = 200

	run := func(t *testing.T, batched bool) int {
		t.Helper()
		db := open(t, tempPath(t))
		if batched {
			db.CommitGroup(50*time.Millisecond, publishers)
		} else {
			db.CommitGroup(0, 0)
		}
		lg, err := db.Log("events")
		if err != nil {
			t.Fatalf("log: %v", err)
		}

		before := db.committed.Load()

		var wg sync.WaitGroup
		for range publishers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("payload")}); err != nil {
					t.Errorf("append: %v", err)
				}
			}()
		}
		wg.Wait()

		if n, err := lg.Len(); err != nil || n != publishers {
			t.Fatalf("stored %d records (err %v), want %d", n, err, publishers)
		}
		return int(db.committed.Load() - before)
	}

	alone := run(t, false)
	together := run(t, true)

	if alone < publishers {
		t.Fatalf("%d publishes took %d transactions with batching off: it should be one each", publishers, alone)
	}
	if together >= alone {
		t.Fatalf("%d publishes took %d transactions batched and %d unbatched: batching stored nothing together",
			publishers, together, alone)
	}
	t.Logf("%d publishes: %d transactions unbatched, %d batched", publishers, alone, together)
}

// Offsets are the thing a batch is most likely to get wrong, because the
// counter row is written once for the whole batch rather than once per
// record. Two hundred publishers to one channel must come out as two
// hundred distinct offsets with no gap - and the channel must continue from
// the right place after a restart, which is what catches a batch that
// wrote the counter of whichever record happened to run last rather than
// the highest.
func TestABatchAssignsEveryOffsetOnceAndContinuesAfterRestart(t *testing.T) {
	for _, way := range []struct {
		what string
		set  func(*DB)
	}{
		{"collected for an interval", func(db *DB) { db.CommitGroup(50*time.Millisecond, 64) }},
		{"collected behind the commit before", func(*DB) {}},
	} {
		t.Run(way.what, func(t *testing.T) {
			const publishers = 200
			path := tempPath(t)

			db := open(t, path)
			way.set(db)
			lg, err := db.Log("events")
			if err != nil {
				t.Fatalf("log: %v", err)
			}

			// **The writer is held until two publishes share a group**, so
			// a transaction holding more than one is built rather than hoped
			// for: at one P each publisher ran its transaction alone. Every
			// transaction takes DB.writing, so none commits while it is held,
			// and a group only grows until its transaction runs.
			db.writing.Lock()
			var unhold sync.Once
			release := func() { unhold.Do(db.writing.Unlock) }
			t.Cleanup(release)
			largest := func() int {
				db.forming.Lock()
				defer db.forming.Unlock()
				n := 0
				if db.group != nil {
					n = len(db.group.pubs) // collected for an interval
				}
				for _, g := range db.waiting {
					n = max(n, len(g.pubs)) // collected behind the commit before
				}
				return n
			}

			got := make([]uint64, publishers)
			var wg sync.WaitGroup
			for i := range publishers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rec, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")})
					if err != nil {
						t.Errorf("append: %v", err)
						return
					}
					got[i] = rec.Offset
				}()
			}
			until(t, "two publishes collected into one group behind the held writer", func() bool { return largest() >= 2 })
			release()
			wg.Wait()

			// And that a transaction held more than one of them, or nothing here is
			// about a batch.
			if st := db.CommitStats(); st.Records <= st.ClosedByRecords+st.ClosedByInterval+st.ClosedByCommit+st.Unbatched {
				t.Fatalf("%d publishers took %+v: no transaction held two, so this proves nothing about a batch", publishers, st)
			}

			seen := map[uint64]bool{}
			for _, off := range got {
				if seen[off] {
					t.Fatalf("offset %d was handed to two publishers", off)
				}
				seen[off] = true
			}
			for off := uint64(1); off <= publishers; off++ {
				if !seen[off] {
					t.Fatalf("offset %d was never handed out: %d publishers, %d distinct offsets", off, publishers, len(seen))
				}
			}

			// The records themselves, read back through the store.
			recs, err := lg.ReadFrom(1)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(recs) != publishers {
				t.Fatalf("read back %d records, want %d", len(recs), publishers)
			}
			for i, rec := range recs {
				if rec.Offset != uint64(i+1) {
					t.Fatalf("record %d has offset %d", i, rec.Offset)
				}
			}

			held := lg.Bytes()
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			// Reopened, the channel has to continue from the offset the batch
			// wrote, not from one somewhere in the middle of it.
			again := open(t, path)
			lg2, err := again.Log("events")
			if err != nil {
				t.Fatalf("reopen log: %v", err)
			}
			if lg2.Next() != uint64(publishers)+1 {
				t.Fatalf("after a restart the channel continues at %d, want %d - the counter row does not match the records",
					lg2.Next(), publishers+1)
			}
			if lg2.Bytes() != held {
				t.Fatalf("after a restart the channel holds %d bytes, want %d - the byte count in the row does not match "+
					"what the batch stored, and it is a column rather than something recounted at the next start",
					lg2.Bytes(), held)
			}
			rec, err := lg2.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")})
			if err != nil {
				t.Fatalf("append after restart: %v", err)
			}
			if rec.Offset != uint64(publishers)+1 {
				t.Fatalf("the first record after a restart took offset %d, want %d", rec.Offset, publishers+1)
			}
		})
	}
}

// One transaction, several channels: the counter row of each has to be its
// own. A batch that wrote one row for the whole transaction, or that wrote
// each channel's row from another channel's totals, reads back correctly
// until the broker restarts - so the assertion is after a reopen.
func TestABatchKeepsEachChannelsCountersApart(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	db.CommitGroup(50*time.Millisecond, 90)

	names := []string{"events", "other", "third"}
	logs := make([]*Log, len(names))
	for i, name := range names {
		lg, err := db.Log(name)
		if err != nil {
			t.Fatalf("log %s: %v", name, err)
		}
		logs[i] = lg
	}

	// Different counts per channel, so a channel that took another's
	// counter is wrong rather than coincidentally right.
	counts := []int{10, 20, 60}
	var wg sync.WaitGroup
	for i, lg := range logs {
		for range counts[i] {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := lg.Append(store.Record{MessageID: "m", Topic: names[i] + "/x", Payload: []byte("p")}); err != nil {
					t.Errorf("append to %s: %v", names[i], err)
				}
			}()
		}
	}
	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	again := open(t, path)
	for i, name := range names {
		lg, err := again.Log(name)
		if err != nil {
			t.Fatalf("reopen %s: %v", name, err)
		}
		if lg.Next() != uint64(counts[i])+1 {
			t.Fatalf("channel %s continues at %d after a restart, want %d", name, lg.Next(), counts[i]+1)
		}
		n, err := lg.Len()
		if err != nil || n != counts[i] {
			t.Fatalf("channel %s holds %d records (err %v), want %d", name, n, err, counts[i])
		}
	}
}

// A record refused for want of room is refused on its own: the publishes
// beside it in the batch still commit.
//
// This is the case a batch is most tempting to get wrong, because failing
// the whole transaction is one line shorter - and it would mean one full
// channel stopping every publisher on the provider, including those with
// room.
func TestAFullChannelRefusesItsOwnPublishAndNotTheBatch(t *testing.T) {
	db := open(t, tempPath(t))
	db.CommitGroup(50*time.Millisecond, 8)

	full, err := db.Log("full")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	roomy, err := db.Log("roomy")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	rec := store.Record{MessageID: "m", Topic: "full/x", Payload: []byte("0123456789")}
	// Room for one record and no more.
	full.SetMaxBytes(store.RecordSize(rec))
	if _, err := full.Append(rec); err != nil {
		t.Fatalf("the first record must fit: %v", err)
	}

	var wg sync.WaitGroup
	refusals := make([]error, 4)
	accepted := make([]error, 4)
	for i := range 4 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, refusals[i] = full.Append(rec)
		}()
		go func() {
			defer wg.Done()
			_, accepted[i] = roomy.Append(store.Record{MessageID: "m", Topic: "roomy/x", Payload: []byte("p")})
		}()
	}
	wg.Wait()

	for i, err := range refusals {
		if !errors.Is(err, store.ErrFull) {
			t.Fatalf("publish %d to the full channel answered %v, want ErrFull", i, err)
		}
	}
	for i, err := range accepted {
		if err != nil {
			t.Fatalf("publish %d to the channel with room was refused because another channel was full: %v", i, err)
		}
	}
	if n, err := roomy.Len(); err != nil || n != 4 {
		t.Fatalf("the channel with room holds %d records (err %v), want 4", n, err)
	}
	if n, err := full.Len(); err != nil || n != 1 {
		t.Fatalf("the full channel holds %d records (err %v), want the 1 that fit", n, err)
	}
}

// A retention sweep writes the same counter row a batch does, from its own
// copy in memory. It must not run between a batch reserving its offsets and
// the transaction that stores them, and - the part a reader cannot check -
// the two must not deadlock: the sweep takes the channel's lock and then
// the connection, and a batch that took them the other way round would sit
// there for ever.
//
// Driven rather than reasoned about, under -race, with the sweep running
// throughout.
func TestABatchAndARetentionSweepDoNotDeadlock(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	db.CommitGroup(20*time.Millisecond, 16)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	// Large enough that the size bound is crossed within the first few
	// records rather than at the very end of the run: with a one-byte
	// payload the channel only just passed 4 KiB as the last publish
	// landed, so the sweep removed nothing and proved nothing.
	payload := benchPayload(200)

	done := make(chan struct{})
	swept := make(chan int, 1)
	go func() {
		n := 0
		for {
			select {
			case <-done:
				swept <- n
				return
			default:
			}
			// **A size bound rather than an age one**, so the sweep
			// actually removes records and therefore actually writes the
			// counter row. The first version of this swept by age against
			// records a second old: Trim found nothing to remove, wrote
			// nothing, and the test passed just as well against a sweep
			// that took no lock at all.
			removed, _, err := lg.Trim(time.Time{}, 2000)
			if err != nil {
				t.Errorf("trim: %v", err)
				swept <- n
				return
			}
			n += removed
		}
	}()

	var wg sync.WaitGroup
	for range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: payload}); err != nil {
				t.Errorf("append: %v", err)
			}
		}()
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		close(done)
		t.Fatal("300 publishes and a retention sweep did not finish in a minute: they are deadlocked")
	}
	close(done)
	if n := <-swept; n == 0 {
		t.Fatal("the sweep removed nothing, so nothing was driven against the batch")
	} else {
		t.Logf("the sweep removed %d records while 300 were published in batches", n)
	}

	// **And the byte count has to survive the two of them.** A sweep writes
	// the channel's counter row from its own copy in memory, exactly as a
	// batch does, so a sweep that ran between a batch reserving its offsets
	// and the transaction storing them would have one written over the
	// other. Nothing about the records would look wrong - the count above
	// passes either way - and the byte count is a column, so the channel
	// would refuse publishes against room it is not using for the rest of
	// the database's life.
	held := lg.Bytes()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	again := open(t, path)
	lg2, err := again.Log("events")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if lg2.Bytes() != held {
		t.Fatalf("the channel holds %d bytes after a restart and held %d before it: a batch and a "+
			"retention sweep wrote one another's counter row", lg2.Bytes(), held)
	}
	recs, err := lg2.ReadFrom(lg2.Floor())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var sum int64
	for _, r := range recs {
		sum += store.RecordSize(r)
	}
	if sum != held {
		t.Fatalf("the channel says it holds %d bytes and its %d records come to %d",
			held, len(recs), sum)
	}
}

// The other two publish paths take the same route, so they get the same
// two questions: does a batch of them store everything, and does each
// channel continue from the right place afterwards.
func TestABatchCarriesLatestValuesAndQueueJobs(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	db.CommitGroup(50*time.Millisecond, 40)

	lt, err := db.Latest("state")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	q, err := db.Queue("jobs")
	if err != nil {
		t.Fatalf("queue: %v", err)
	}

	const each = 40
	var wg sync.WaitGroup
	for i := range each {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := lt.Set(store.Record{
				MessageID: "m", Topic: fmt.Sprintf("state/d%d", i), Payload: []byte("v"),
			}); err != nil {
				t.Errorf("set: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := q.Enqueue(store.Record{MessageID: "m", Topic: "jobs/x", Payload: []byte("j")}); err != nil {
				t.Errorf("enqueue: %v", err)
			}
		}()
	}
	wg.Wait()

	// Both answers, because they are now two different things: Len reads a
	// count kept in memory and advanced by every one of the writes above,
	// and countRows asks the database. A concurrent batch that advanced it
	// once too often, or not at all, shows up as the two disagreeing.
	if n := lt.Len(); n != each {
		t.Fatalf("the latest channel reports %d topics, want %d", n, each)
	}
	if n, err := lt.countRows(); err != nil || int(n) != each {
		t.Fatalf("the latest channel's rows come to %d (err %v), want %d - "+
			"the kept count and the table disagree", n, err, each)
	}
	if total, _ := q.Depth(); total != each {
		t.Fatalf("the queue holds %d jobs, want %d", total, each)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	again := open(t, path)
	lt2, err := again.Latest("state")
	if err != nil {
		t.Fatalf("reopen latest: %v", err)
	}
	if lt2.Next() != each+1 {
		t.Fatalf("the latest channel continues at %d after a restart, want %d", lt2.Next(), each+1)
	}
	q2, err := again.Queue("jobs")
	if err != nil {
		t.Fatalf("reopen queue: %v", err)
	}
	if q2.Next() != each+1 {
		t.Fatalf("the queue continues at %d after a restart, want %d", q2.Next(), each+1)
	}
	if total, _ := q2.Depth(); total != each {
		t.Fatalf("the queue holds %d jobs after a restart, want %d", total, each)
	}
}

// A record is durable before its publisher is told anything, batched or
// not. The test reads the row with a second connection that knows nothing
// about the batch, so what it sees is what a crash would have left.
func TestABatchedPublishIsStoredBeforeItIsAcknowledged(t *testing.T) {
	path := tempPath(t)
	db := open(t, path)
	db.CommitGroup(30*time.Millisecond, 4)
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}

	rec, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	reader, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&mode=ro")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var stored int64
	if err := reader.QueryRow(
		`SELECT "offset" FROM records WHERE channel = ? AND "offset" = ?`, "events", int64(rec.Offset),
	).Scan(&stored); err != nil {
		t.Fatalf("the record was acknowledged at offset %d and another connection cannot see it: %v",
			rec.Offset, err)
	}
	var next int64
	if err := reader.QueryRow(`SELECT next FROM channels WHERE name = ?`, "events").Scan(&next); err != nil {
		t.Fatalf("read the counter row: %v", err)
	}
	if next != int64(rec.Offset)+1 {
		t.Fatalf("the counter row says %d and the acknowledged record took %d", next, rec.Offset)
	}
}

// CommitGroup's own refusals. A setting that cannot batch has to leave the
// write path alone rather than collect batches of one: an interval with a
// record count of one would make every publisher wait out the interval and
// then commit alone, which is slower than doing nothing and would read as
// "batching is on" to anybody looking at the configuration.
func TestCommitGroupIgnoresSettingsThatCannotBatch(t *testing.T) {
	for _, c := range []struct {
		what       string
		interval   time.Duration
		maxRecords int
	}{
		{"no interval", 0, 256},
		{"a negative interval", -time.Second, 256},
		{"a count of one", 50 * time.Millisecond, 1},
		{"a count of zero", 50 * time.Millisecond, 0},
		{"a negative count", 50 * time.Millisecond, -4},
	} {
		t.Run(c.what, func(t *testing.T) {
			db := open(t, tempPath(t))
			db.CommitGroup(c.interval, c.maxRecords)
			lg, err := db.Log("events")
			if err != nil {
				t.Fatalf("log: %v", err)
			}
			start := time.Now()
			if _, err := lg.Append(store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")}); err != nil {
				t.Fatalf("append: %v", err)
			}
			if took := time.Since(start); took > 20*time.Millisecond {
				t.Fatalf("one publish took %v with %s: it waited for a batch that can never fill", took, c.what)
			}
			// And alone: a setting that cannot batch is `none`, not the
			// provider's default of collecting behind the commit before.
			if st := db.CommitStats(); st.Unbatched != 1 || st.Records != 1 || st.MaxRecords != 0 {
				t.Fatalf("with %s one publish was counted %+v, want it unbatched", c.what, st)
			}
		})
	}
}

// ------------------------------------ collected behind the commit before

// queuedBehind is how many publishes are waiting for the transaction running
// now to end, and how many groups they are in.
func queuedBehind(db *DB) (pubs, groups int) {
	db.forming.Lock()
	defer db.forming.Unlock()
	for _, g := range db.waiting {
		pubs += len(g.pubs)
	}
	return pubs, len(db.waiting)
}

// until polls cond for up to ten seconds, and fails the test naming what it
// was waiting for.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("waited ten seconds for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// holdACommit starts one publish on a provider that collects behind the
// commit before, and holds it inside its transaction - where a slow commit
// would be - until release is called. Everything published meanwhile is
// behind it.
func holdACommit(t *testing.T, db *DB, lg *Log) (release func() store.Record) {
	t.Helper()
	db.writing.Lock()
	first := make(chan store.Record, 1)
	go func() {
		rec, err := lg.Append(store.Record{MessageID: "first", Topic: "events/x", Payload: []byte("p")})
		if err != nil {
			t.Errorf("the held publish: %v", err)
		}
		first <- rec
	}()
	until(t, "the held publish to start its transaction", func() bool {
		db.forming.Lock()
		defer db.forming.Unlock()
		return db.committing
	})
	return func() store.Record {
		db.writing.Unlock()
		return <-first
	}
}

// RFC 0002: a provider that says nothing about commits stores what arrives
// while a transaction is committing in the next one, and waits for nothing
// else. Held rather than raced: the first publish is kept inside its
// transaction while nine more arrive, so which transaction each lands in is
// decided rather than hoped for.
func TestPublishesArrivingDuringACommitShareTheNext(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	before := db.committed.Load()

	release := holdACommit(t, db, lg)
	const behind = 9
	got := make([]uint64, behind)
	var wg sync.WaitGroup
	for i := range behind {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/x", Payload: []byte("p")})
			if err != nil {
				t.Errorf("publish %d: %v", i, err)
				return
			}
			got[i] = rec.Offset
		}()
	}
	until(t, "nine publishes to queue behind the held one", func() bool {
		pubs, _ := queuedBehind(db)
		return pubs == behind
	})
	if pubs, groups := queuedBehind(db); groups != 1 {
		t.Fatalf("%d publishes queued in %d groups, want one: a group opened per publish", pubs, groups)
	}
	if first := release(); first.Offset != 1 {
		t.Fatalf("the held publish took offset %d, want 1", first.Offset)
	}
	wg.Wait()

	slices.Sort(got)
	for i, off := range got {
		if off != uint64(i+2) {
			t.Fatalf("the publishes behind it took offsets %v, want 2 to %d", got, behind+1)
		}
	}
	if n := db.committed.Load() - before; n != 2 {
		t.Fatalf("ten publishes took %d transactions, want 2: the held one alone, then the nine behind it", n)
	}
	want := store.CommitStats{ClosedByCommit: 2, Records: behind + 1, MaxRecords: BehindMaxRecords}
	if st := db.CommitStats(); st != want {
		t.Fatalf("counted %+v, want %+v", st, want)
	}

	// And with nothing running, the next publish commits at once and alone.
	db.forming.Lock()
	running := db.committing
	db.forming.Unlock()
	if running {
		t.Fatal("the provider still reads as committing after every publish was answered, " +
			"so the next publish would queue behind a transaction that is never coming")
	}
	if _, err := lg.Append(store.Record{MessageID: "last", Topic: "events/x", Payload: []byte("p")}); err != nil {
		t.Fatalf("append on an idle provider: %v", err)
	}
	want.ClosedByCommit, want.Records = 3, behind+2
	if st := db.CommitStats(); st != want {
		t.Fatalf("after one more publish on an idle provider counted %+v, want %+v", st, want)
	}
}

// A group stops taking members at BehindMaxRecords and the next arrival
// opens another behind it, and the groups commit in the order they opened.
// Order is what a publisher sees as offsets: a group that committed out of
// turn would give later arrivals the lower offsets.
func TestAFullGroupOpensAnotherAndTheyCommitInTurn(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	release := holdACommit(t, db, lg)

	const behind = BehindMaxRecords + 3
	got := make([]uint64, behind)
	var wg sync.WaitGroup
	for i := range behind {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/x", Payload: []byte("p")})
			if err != nil {
				t.Errorf("publish %d: %v", i, err)
				return
			}
			got[i] = rec.Offset
		}()
		// One at a time, so publish i is known to have joined group i/256.
		until(t, fmt.Sprintf("publish %d to queue", i), func() bool {
			pubs, _ := queuedBehind(db)
			return pubs == i+1
		})
	}
	if pubs, groups := queuedBehind(db); groups != 2 {
		t.Fatalf("%d publishes queued in %d groups, want 2: %d in a full one and 3 behind it", pubs, groups, BehindMaxRecords)
	}
	release()
	wg.Wait()

	for i, off := range got {
		if want := uint64(i + 2); off != want {
			t.Fatalf("publish %d queued behind the commit took offset %d, want %d: the groups did not "+
				"commit in the order they opened", i, off, want)
		}
	}
	want := store.CommitStats{ClosedByRecords: 1, ClosedByCommit: 2, Records: behind + 1, MaxRecords: BehindMaxRecords}
	if st := db.CommitStats(); st != want {
		t.Fatalf("counted %+v, want %+v", st, want)
	}
}

// A refused record and a failed transaction are the same inside a group as
// in any batch (runBatch), and what matters here is the turn: a group whose
// transaction failed still hands on, or every publish after it waits
// forever.
func TestAFailedGroupStillHandsOn(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	release := holdACommit(t, db, lg)

	// The group behind the held publish, with one member that fails its
	// insert, which fails the whole transaction.
	failing := &publish{
		channel:  lg,
		reserve:  func(*publish) error { return nil },
		settle:   func(bool) {},
		insert:   func(*sql.Tx) error { return errors.New("the disk is full") },
		counters: func(*sql.Tx) error { return nil },
	}
	failed := make(chan error, 1)
	go func() { failed <- db.store(failing) }()
	until(t, "the failing publish to queue", func() bool { pubs, _ := queuedBehind(db); return pubs == 1 })
	joined := make(chan error, 1)
	go func() {
		_, err := lg.Append(store.Record{MessageID: "beside", Topic: "events/x", Payload: []byte("p")})
		joined <- err
	}()
	until(t, "a publish to join it", func() bool { pubs, _ := queuedBehind(db); return pubs == 2 })
	release()

	if err := <-failed; err == nil {
		t.Fatal("the failing publish was answered with no error")
	}
	if err := <-joined; err == nil {
		t.Fatal("the publish sharing a failed transaction was answered with no error")
	}
	done := make(chan error, 1)
	go func() {
		_, err := lg.Append(store.Record{MessageID: "after", Topic: "events/x", Payload: []byte("p")})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the publish after a failed group: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the publish after a failed group waited ten seconds: the failed group never handed on")
	}
	if n, err := lg.Len(); err != nil || n != 2 {
		t.Fatalf("stored %d records (err %v), want 2: the held one and the one after", n, err)
	}
}

// Collecting behind the commit before never waits for company, so a lone
// publisher is answered as fast as with a transaction per publish. Timed
// against `none` on the same machine in the same test rather than against a
// figure, and the best of three, because one trial of a timing is not
// evidence; a wait of a millisecond per publish is a hundred milliseconds
// over the run and fails it.
func TestALonePublisherIsNotHeldForCompany(t *testing.T) {
	const publishes = 100
	run := func(set func(*DB)) time.Duration {
		best := time.Duration(1<<63 - 1)
		for range 3 {
			db := open(t, tempPath(t))
			set(db)
			lg, err := db.Log("events")
			if err != nil {
				t.Fatalf("log: %v", err)
			}
			start := time.Now()
			for i := range publishes {
				if _, err := lg.Append(store.Record{MessageID: fmt.Sprint("m-", i), Topic: "events/x", Payload: []byte("p")}); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			best = min(best, time.Since(start))
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		}
		return best
	}
	each := run(func(db *DB) { db.CommitGroup(0, 0) })
	behind := run(func(*DB) {})
	t.Logf("%d publishes one after another: %v a transaction each, %v collected behind the commit before",
		publishes, each, behind)
	if behind > 2*each+50*time.Millisecond {
		t.Fatalf("%d publishes one after another took %v collected behind the commit before and %v "+
			"with a transaction each: a lone publisher is being held for company", publishes, behind, each)
	}
}

// **The interval is not a wait for a reader**, and this test exists because
// seven places in this repository once said it was.
//
// The reasoning that produced that sentence is sound as far as it goes
// where reads share the write connection (read_connections 0): an open
// transaction holds it, so a transaction that lasts longer holds it
// longer. What it missed is *when*
// the leader waits. It waits out the interval holding nothing - not the
// write lock, not the connection, no transaction - and opens the
// transaction afterwards, so the connection is free for the whole of the
// interval and busy only for the commit itself.
//
// The difference matters to an operator sizing the key: the cost of the
// interval is paid by publishers on that connection and by nobody else. A
// consumer reading its window, a point read and a metrics scrape are not in
// it. Only the transaction is theirs to wait for, and that is bounded by
// publish_commit_max_records rather than by the interval.
func TestTheIntervalIsNotAWaitForAReader(t *testing.T) {
	const interval = 2 * time.Second
	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: []byte("p")}

	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	// One record to read back, stored before collecting is switched on so
	// that it costs nothing.
	if _, err := lg.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}

	db.CommitGroup(interval, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := lg.Append(rec); err != nil {
			t.Errorf("the batched append failed: %v", err)
		}
	}()

	// Read for a quarter of the interval, so the publish above is certainly
	// still waiting when the last of them runs.
	reads, worst := 0, time.Duration(0)
	for deadline := time.Now().Add(interval / 4); time.Now().Before(deadline); {
		start := time.Now()
		if _, err := lg.ReadFromN(1, 10); err != nil {
			t.Fatalf("read: %v", err)
		}
		if d := time.Since(start); d > worst {
			worst = d
		}
		reads++
	}

	// **Both halves, or this proves nothing.** A run where the publish had
	// already finished would measure reads against an idle provider and
	// pass however long the interval held the connection.
	select {
	case <-done:
		t.Fatal("the publish finished before the reads did, so nothing was read against a batch " +
			"that was still collecting")
	default:
	}
	if reads == 0 {
		t.Fatal("no read ran")
	}
	if worst > interval/8 {
		t.Fatalf("the slowest of %d reads took %v while a publish waited out a %v interval: the "+
			"interval is being held against readers, which is what this rule says it must not be",
			reads, worst, interval)
	}
	t.Logf("%d reads while a batch collected, slowest %v, against an interval of %v", reads, worst, interval)
	<-done
}

// **A batch whose transaction fails puts back exactly what it took.**
//
// RFC 0004 says so and settle(false) is the code for it, in all three
// publish paths. Nothing drove it: removing the `l.next--` from
// Log.Append's settle - so a failed batch leaves the offset advanced and
// the next record skips one - left the whole package passing. A
// skipped offset is a gap, and a gap is records a consumer reads straight
// past believing it had them, which is the failure this store exists to
// prevent.
//
// The path is not exotic: it is a full disk. The provider's bound is
// SQLite's own max_page_count, so the insert fails inside the transaction
// rather than being refused before it, which is what makes this the
// failure branch rather than the refusal branch. A channel at its own
// max_bytes is the other one and is covered by
// TestAFullChannelRefusesItsOwnPublishAndNotTheBatch.
func TestAFailedBatchPutsBackExactlyWhatItTook(t *testing.T) {
	path := tempPath(t)
	db, err := OpenBounded(path, "0.1.0-test", 64<<10, 8<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: benchPayload(1024)}

	// Fill it one at a time, before collecting is switched on, so that what
	// follows starts from a provider known to be at its bound.
	stored := 0
	for {
		if _, err := lg.Append(rec); err != nil {
			if !errors.Is(err, store.ErrFull) {
				t.Fatalf("filling: %v", err)
			}
			break
		}
		stored++
		if stored > 1000 {
			t.Fatal("the provider never filled, so this never reached the state it is about")
		}
	}
	if stored == 0 {
		t.Fatal("not one record fit, so nothing was stored to fail against")
	}

	nextBefore, heldBefore := lg.Next(), lg.Bytes()

	db.CommitGroup(50*time.Millisecond, 8)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = lg.Append(rec)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, store.ErrFull) {
			t.Fatalf("publish %d into a full provider answered %v, want ErrFull", i, err)
		}
	}
	if lg.Next() != nextBefore || lg.Bytes() != heldBefore {
		t.Fatalf("after a failed batch next=%d held=%d, before it next=%d held=%d: the reservation "+
			"was not put back, so the next record skips those offsets and a consumer reads past "+
			"the gap believing it had them", lg.Next(), lg.Bytes(), nextBefore, heldBefore)
	}

	// And the offsets are still there to be used: relieve the provider and
	// the next record takes the one the failed batch reserved first.
	if removed, _, err := lg.Trim(time.Time{}, int64(stored/2)*1024); err != nil || removed == 0 {
		t.Fatalf("trim to make room: removed %d, err %v", removed, err)
	}
	after, err := lg.Append(rec)
	if err != nil {
		t.Fatalf("append after relief: %v", err)
	}
	if after.Offset != nextBefore {
		t.Fatalf("the first record after a failed batch took offset %d, want %d - the batch kept "+
			"the offsets it did not store", after.Offset, nextBefore)
	}

	// The counter row has to agree after a restart, because that is where a
	// wrong one becomes permanent.
	held := lg.Bytes()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	again := open(t, path)
	lg2, err := again.Log("events")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if lg2.Next() != after.Offset+1 || lg2.Bytes() != held {
		t.Fatalf("after a restart next=%d held=%d, want %d and %d",
			lg2.Next(), lg2.Bytes(), after.Offset+1, held)
	}
}

// **Near the bound, a batch can be refused where a smaller one would have
// fitted.** RFC 0004 says so, and said the opposite until a test drove
// it: the sentence claimed every publisher in a failed batch "would have
// met that on its own", which is an equivalence that does not hold when
// the provider has room for some of the batch but not all of it.
//
// Nothing is lost and nothing reports success: each publisher is answered
// 0x97 and its retry finds the room. The test is here because the RFC now
// describes the behaviour rather than a tidier one, and a document that
// describes behaviour is a document that can go stale.
func TestNearTheBoundABatchIsRefusedWhereOneRecordWouldFit(t *testing.T) {
	db, err := OpenBounded(tempPath(t), "0.1.0-test", 64<<10, 8<<10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	lg, err := db.Log("events")
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	rec := store.Record{MessageID: "m", Topic: "events/x", Payload: benchPayload(1024)}

	// Fill to one short of the bound: keep going until a record is refused,
	// then make room for exactly one by removing exactly one.
	stored := 0
	for {
		if _, err := lg.Append(rec); err != nil {
			if !errors.Is(err, store.ErrFull) {
				t.Fatalf("filling: %v", err)
			}
			break
		}
		stored++
		if stored > 1000 {
			t.Fatal("the provider never filled")
		}
	}
	if removed, _, err := lg.Trim(time.Time{}, int64(stored-1)*int64(store.RecordSize(rec))); err != nil || removed != 1 {
		t.Fatalf("making room for exactly one: removed %d, err %v", removed, err)
	}

	// Eight together into room for one.
	db.CommitGroup(50*time.Millisecond, 8)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = lg.Append(rec)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, store.ErrFull) {
			t.Fatalf("publish %d of a batch of eight into room for one answered %v, want ErrFull", i, err)
		}
	}

	// And the one that would have fitted, fitting - which is the half that
	// makes this a trade rather than a bound doing its job.
	db.CommitGroup(0, 0)
	if _, err := lg.Append(rec); err != nil {
		t.Fatalf("one record into the room the batch of eight was refused for: %v - either the "+
			"provider had no room after all, in which case this test proves nothing, or the "+
			"behaviour RFC 0004 describes has changed and the RFC needs rewriting", err)
	}
}
