// Package sqlite keeps saguin's records in a SQLite database, so that a
// channel survives a crash and not only a graceful shutdown.
//
// It is the second implementation of what internal/store defines, beside
// the memory one. Nothing here knows about MQTT; it stores records and
// channel state, and the broker layer owns packets, sessions and
// subscriptions.
//
// The whole provider is one database file. A queue and its dead-letter
// channel are therefore two tables in one file, which is what lets the
// move between them be one transaction - SQLite's atomic commit does not
// span attached databases in WAL mode, so a file per channel would make
// invariant 5 unsatisfiable.
package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"modernc.org/sqlite" // registers the "sqlite" driver, and names its errors

	"github.com/ifnesi/saguin/internal/store"
)

// schemaVersion is what this broker reads and writes, kept in SQLite's own
// user_version. A file from a version this broker does not understand is
// refused by name rather than opened and half-read - the same rule as the
// snapshot format, for the same reason: a broker that starts with
// plausible-looking wrong state and says nothing is worse than one that
// will not start (invariant 14).
//
// **Version 5 stores a record's User Properties as an ordered array rather
// than a JSON object**, because MQTT 5 allows a name to repeat and requires
// the order kept, and an object holds neither. A version 4 row read as an
// array would fail to parse rather than silently mislead, but it is refused
// at the door either way.
//
// **Version 6 adds a `props` column** to the three tables that hold records,
// carrying the MQTT 5 publish properties that are not User Properties. One
// column of JSON rather than five columns, because nothing queries by them -
// a record is read whole - and because correlation data is arbitrary bytes,
// which Go's encoder writes as base64 rather than mangling into a string.
// It is NULL for a record that carries none, which is most of them.
//
// **Version 7 adds `pending_publishes`**, where an exactly-once publish
// waits between the PUBLISH that starts it and the PUBREL that finishes it
// (RFC 0002 `broker.qos2`). It is the one table here whose rows are never
// read back after a restart: the recovery empties it, because a restarted
// broker answers every reconnecting client Session Present = 0 and MQTT
// then requires that client to abandon the exchange. It is a table rather
// than a map in the broker so that what a publisher is holding is inside a
// provider's bound and visible in its gauges.
//
// **Version 9 stores a broadcast message once**: `broadcast_messages` holds
// the message and `session_message_refs` one small row for each session owed
// it, where version 8 wrote the whole message once per session. A publish to a
// hundred sessions was a hundred copies, counted a hundred times against a
// provider's `max_bytes`; it is now one, counted once. The last reference to
// go takes the message with it.
//
// **Version 8 adds `sessions` and `session_messages`**, where a persistent
// MQTT session and the broadcast and shared-group messages it is owed are
// kept, so that a session outlives a restart as far as its provider does.
//
// **Version 11 adds `share_backlog`**, where a shared group's deliveries
// wait while none of its members is connected to take them (RFC 0002
// `broker.share`). Its rows outlive a restart where the member sessions
// that could collect them do, and go when the last of those sessions does -
// a backlog nobody is owed is memory for ghosts. It is a table rather than
// a map in the broker so that what a group is holding is inside a
// provider's bound and visible in its gauges.
//
// **Version 12 adds `session_inflight` and `sessions.receive_maximum`**: a
// durable session's in-flight table on the broadcast log - each message on
// the wire, under the packet identifier a re-send to a resumed session must
// carry (MQTT-4.4.0-1) - and the Receive Maximum it was written under. A row
// per message rather than a value on the session, because the table changes
// on every send and every acknowledgement, and a client's Receive Maximum
// may reach 65,535.
//
// **Version 13 adds `held_publishes`**: an exactly-once publish held for its
// release in the database of the channel it is for, so the release is one
// transaction with the record it becomes (store.HeldPublish).
//
// **Version 14 drops `pending_publishes`**: every exactly-once publish is
// held in `held_publishes`, in its channel's database, so nothing writes or
// reads the older table.
//
// **Version 15 serves durable broadcast from the broadcast log**: a session
// is owed what its filters matched after its cursor, and its held deliveries
// are no longer where its broadcast waits, so a file that kept them there is
// refused rather than read without them.
//
// **Version 16 adds `share_groups` and `session_inflight.share_group`**: each
// shared group's cursor on the broadcast log, and on each in-flight row the
// group that handed it to its session. A group lags its members, so the row
// is usually behind its session's own cursor, and a version 15 file would
// have the start let it go as a message already acknowledged.
//
// **Version 17 adds `share_returned`**: the deliveries a member's session
// ending returned to its group, which the group is owed again ahead of its
// cursor. A version 16 file has nowhere to keep them.
//
// **Version 18 drops `share_backlog`**: a shared group's backlog is the
// broadcast log behind the group's cursor, and nothing reads the table. A
// version 17 file can still hold a backlog there, which a version 18 broker
// would lose without a word, so it is refused like every other version.
//
// **Version 19 drops `session_messages`, `broadcast_messages` and
// `session_message_refs`**: what a session is owed is the broadcast log
// behind its cursor and its in-flight table, and nothing reads them. A
// version 18 file can still hold a delivery there - one a shared group over
// a channel handed a durable member before that was kept in the log - which a
// version 19 broker would lose without a word, so it is refused.
//
// **Version 20 marks a broadcast log message owed to one session**, a
// retained value delivered at QoS 1 or 2 at SUBSCRIBE (store.Record.OwedTo),
// in its props. The tables are unchanged; the version is not, because a
// version 19 broker reading such a message would owe it to every
// subscription its topic matches.
//
// There is no in-place upgrade, and that is a decision rather than an
// oversight: a version 4 file is refused by both the broker and
// `--sqlite-to-snapshots`, so carrying one across needs the older binary to
// export it first. The refusal names the version and the build that wrote
// the file, which is what makes that recoverable.
const schemaVersion = 20

// setUserVersion stamps that version into a file as it is created.
//
// **A constant rather than a fmt.Sprintf, and the number therefore written
// twice on purpose.** PRAGMA takes no bound parameter - `PRAGMA user_version
// = ?` is a syntax error, measured - so the version has to be in the
// statement's own text. Assembling it at run time makes it SQL that nothing
// reading this source can know the content of, and
// TestEveryTransactionWritingAChannelsCountersHoldsTheWriteLock then has to
// assume the worst of the transaction that runs it: a statement it cannot
// read might be writing a channel's counter row.
//
// The two numbers cannot drift apart unnoticed, which is what would
// otherwise make writing it twice a bad trade. A file created with the wrong
// one is refused by the next open of it, naming both versions - and opening
// a file it has just created is what every test in this package does.
const setUserVersion = `PRAGMA user_version = 20`

// Pragmas that have to hold for every connection, so they ride in the DSN
// rather than being executed after opening: a pool that reconnects would
// otherwise get a connection without them.
//
//   - WAL, because it lets a reader run while the writer commits, and it
//     is the mode the durability argument below is about.
//   - synchronous=NORMAL, which does not sync on commit and does sync at a
//     checkpoint. A power cut costs the last commits and never the file.
//     OFF removes the checkpoint sync too, and can leave a database that
//     does not open at all - losing data committed hours earlier, which is
//     worse than memory storage rather than equal to it. It is not a knob:
//     its two settings are "durable" and "silently not durable".
//   - busy_timeout, which nothing should need with one writer, and
//     which turns a surprise into a wait rather than an error if anything
//     ever does.
//   - page_size, pinned rather than left to the build's default, because
//     max_bytes is enforced as a whole number of pages and an operator's
//     figure therefore rounds down by up to one page. Left to the default,
//     two machines running the same configuration could round it
//     differently. It applies only to a database being created: on an
//     existing file SQLite ignores it outside a VACUUM, which is why the
//     size is read back rather than assumed.
const pragmas = "?_pragma=journal_mode(WAL)" +
	"&_pragma=synchronous(NORMAL)" +
	"&_pragma=busy_timeout(5000)" +
	"&_pragma=page_size(4096)"

// The lock a provider holds while it is open, and the pragmas that make it
// one. locking_mode(EXCLUSIVE) means the connection takes the file's locks
// on its first write and never gives them back, so a second broker opening
// the same lock is refused.
//
// It is a file of its own rather than the database, because the database
// must stay readable while the broker runs - opening *that* exclusively
// would lock out `sqlite3` too, and being able to read a live channel with
// ordinary tools is a reason to choose saguin.
const lockSuffix = ".lock"
const lockPragmas = "?_pragma=locking_mode(EXCLUSIVE)&_pragma=busy_timeout(5000)"

// DB is one storage provider: one file, holding every channel assigned to
// it.
type DB struct {
	db   *sql.DB
	path string

	// stmts is every constant SQL text this provider has run, prepared once
	// (stmt). stmtMu guards the map, and nothing else.
	stmtMu  sync.Mutex
	stmts   map[string]*sql.Stmt
	pending map[string]struct{}

	// reads is the read pool (readTx): connections of their own on the same
	// file, read-only, for the pure reads pureReads names. nil on a database
	// opened for export, whose reads stay on db. readStmts is the pool's own
	// statement cache, every pure read prepared at open and never after.
	reads     *sql.DB
	readStmts map[string]*sql.Stmt
	readConns int

	// lock is held open for as long as this provider is, and released by
	// Close or by the process ending. Nothing reads it.
	lock *Lock

	// mu guards channels, which holds the one store built for each channel
	// name.
	//
	// There is one per channel because each caches that channel's next
	// offset and prepares its own statements. Two stores for one channel
	// would be two writers with two copies of one counter: both would
	// assign the same offset, the second insert would fail the primary key
	// - no loss and nothing silent - but the losing one would never
	// advance its copy and would refuse every publish from then on. Handing
	// back the store already built removes the case rather than documenting
	// it.
	mu       sync.Mutex
	channels map[string]any

	// sessions is the one store for persistent sessions, built the first
	// time it is asked for. One per provider, not one per channel: a session
	// belongs to a client rather than to a channel.
	sessions *Sessions

	// How the write path commits. each gives every publish a transaction of
	// its own. Otherwise interval is how long a transaction may collect
	// publishes before it closes and maxRecords is how many it may hold;
	// a zero interval, which is what a provider nobody configured has, means
	// a transaction collects what arrived while the one before it was
	// committing (storeBehind). Written once by CommitGroup before any
	// listener opens, read-only afterwards.
	each       bool
	interval   time.Duration
	maxRecords int

	// How the publish transactions were closed, and what they carried.
	// Nothing else on the write path is counted here: a retention sweep and
	// a queue resolution are transactions too, and mixing them in would
	// make the average batch size a number about the wrong thing.
	closedByRecords  atomic.Int64
	closedByInterval atomic.Int64
	closedByCommit   atomic.Int64
	unbatched        atomic.Int64
	recordsCommitted atomic.Int64

	// committed counts the transactions this provider has committed. It is
	// what makes group commit checkable: the records, the offsets and the
	// counter rows come out identical whether they were stored one at a
	// time or two hundred at a time, so a test that only reads them back
	// passes just as well against a provider that ignores the setting.
	committed atomic.Int64

	// writeTx is the transaction a write group is running now, or nil
	// (note). Set and cleared by the group's leader, which holds
	// DB.writing; read by every statement helper.
	writeTx atomic.Pointer[writeTx]

	// joined numbers the client writes in the order they joined, under
	// DB.forming, so that those left to run alone run in that order.
	joined uint64

	// failCommit, where a test sets it, fails a write group's COMMIT with
	// what it answers, as a disk that refuses the commit would.
	failCommit func() error

	// budget is writeGroupBudget, but where a test that counts transactions
	// has set it longer, so that a slow machine - or the race detector -
	// does not split a group the test expects whole.
	budget time.Duration

	// afterMember, where a test sets it, is called by a write group's leader
	// after each member it has run, inside the transaction.
	afterMember func(w *write)

	// lookedAtPublishes, where a test sets it, is called by letPublishesIn
	// once it has looked at the publishes running, before it waits for them.
	lookedAtPublishes func()

	// writing is held by whichever publisher is running a transaction, from
	// reserving the offsets in it to advancing the counters those offsets
	// came from. One at a time, because those three are one step.
	writing sync.Mutex

	// flush forces the write-ahead log to disk on an interval
	// (SetFlushInterval); nil when none was asked for.
	flush *flusher

	// forming guards group, the batch that publishes are still joining,
	// and committing and waiting, storeBehind's. Held only long enough to
	// join one, never across a transaction.
	forming sync.Mutex
	group   *group

	// committing is true while a transaction storeBehind or joinWrite
	// started has not handed on, and waiting is the groups queued behind it,
	// oldest first: publish groups and client write groups, each kind taking
	// its turn with the other (handOn). Only the last group of each kind
	// still takes members.
	committing bool
	waiting    []*group

	// publishes counts the publish transactions that are not queued in
	// waiting - `none` and a duration run theirs directly: in its low 32
	// bits those running, from before they wait on writing until they end,
	// which shouldYield reads, and in its high 32 those that have ended,
	// modulo 2^32, which letPublishesIn waits on (publishEnded).
	//
	// **One word, so that both are read in one load and an ending changes
	// both in one step.** Read as two counters, a publish that ended between
	// the reads was counted as running and as ended at once, and a group
	// that gave way waited for one more publish to end than ever would: its
	// members, and every group queued behind it, waited for some unrelated
	// publish.
	publishes atomic.Uint64

	// answersRun counts the groups of answering writes run since a group of
	// the other writes last ran, and departedRun the departed clients'
	// groups run since a connected session's last ran, under DB.forming
	// (handOn, nextBackground).
	answersRun  int
	departedRun int

	// marks counts, by client id, the packets waiting on the store (Waiting);
	// queuedFor counts, by client id, that client's writes queued in groups
	// of the writes that answer nobody (countQueued), so that a mark looks
	// for them only where there are some. Both under DB.forming.
	marks     map[string]int
	queuedFor map[string]int

	// overBound records that the database already held more than max_bytes
	// when it opened. Read once at startup; nothing on a hot path looks at
	// it.
	overBound bool

	// The two ceilings, in pages, and the page size they are counted in.
	// publishPages is where an ordinary publish stops; fullPages is what an
	// operation that relieves the provider may reach. Zero means unbounded.
	//
	// Written once during Open and read-only afterwards, so nothing guards
	// them.
	pageSize     int64
	publishPages int64
	fullPages    int64
}

// Bytes is what this provider is holding: the database's own size, as
// `page_count` multiplied by `page_size`.
//
// **That is the number max_page_count bounds, and it is deliberately not
// the size of the file on disk.** Those are three different numbers and
// only the first is the one a bound refuses on. Measured on one provider
// the moment it began refusing: the database was 2,084,864 bytes by
// page_count, the file behind it was 1,458,176, and a further 4,128,272 sat
// in a write-ahead log waiting to be checkpointed into it. A provider
// measured by either of the other two would be graphed against a ceiling
// that does not govern it, and would appear to have room when it had none.
//
// So saguin_provider_bytes is not a disk-usage figure and an operator sizing
// a volume should not read it as one - the WAL is real bytes on that disk
// and is not in it.
//
// Two PRAGMAs per call, which is why nothing on a hot path may use this:
// the metrics collector is rate-limited by
// broker.operations.min_scrape_interval and is the only caller.
//
// A database that cannot answer reports zero rather than failing the
// scrape. A scrape that errors is a gap in an operator's graph and usually
// an alert, and the question this answers is not one worth waking somebody
// for.
func (d *DB) Bytes() int64 {
	var pages, size int64
	if err := d.queryRow(nil, `PRAGMA page_count`).Scan(&pages); err != nil {
		return 0
	}
	if err := d.queryRow(nil, `PRAGMA page_size`).Scan(&size); err != nil {
		return 0
	}
	return pages * size
}

// MaxBytes is the ceiling this provider's file may reach, or zero when it
// is unbounded - which is the same "zero for no bound" RFC 0005 states for
// saguin_provider_max_bytes.
//
// It is the full bound rather than the one an ordinary publish stops at.
// The reserve below it exists so that the operations which free a full
// provider are never refused, and an operator comparing what they asked for
// against what they got wants the figure they wrote.
func (d *DB) MaxBytes() int64 { return d.fullPages * d.pageSize }

// Open opens or creates the provider's database and makes it ready to
// serve: the schema is applied, the version is checked, and nothing comes
// back from the last run holding a delivery.
//
// writer is the saguin version, stored so that an operator meeting a file
// from another one can see which wrote it.
func Open(path, writer string) (*DB, error) { return OpenBounded(path, writer, 0, 0) }

// OpenBounded is Open with a ceiling on how large the database may grow,
// in bytes, or zero for none.
//
// SQLite enforces it rather than saguin: max_page_count bounds the file to
// a number of pages, and a write that would pass it fails with SQLITE_FULL.
// Nothing is counted on the write path and nothing has to be kept up to
// date - which is the whole reason a provider's bound is expressed this way
// and a channel's is a counter. It is also why they measure different
// things: this bounds the file, where a channel's bounds what was
// published.
//
// The database stays readable at the ceiling, and freeing pages lets writes
// through again, so a provider that filled recovers as its channels are
// trimmed or its queues drain.
//
// Two edges worth knowing. It bounds the main file, and the write-ahead log
// grows beside it until a checkpoint, so what is on disk passes the number
// transiently. And a bound below what the database already holds is not an
// error: SQLite keeps the pages it has and refuses to add more, which is
// the same shape as lowering a channel's bound under its records.
//
// Its read pool is the default, store.SQLiteReadConnections;
// OpenBoundedWithReads names another.
func OpenBounded(path, writer string, maxBytes, reserve int64) (*DB, error) {
	return OpenBoundedWithReads(path, writer, maxBytes, reserve, store.SQLiteReadConnections)
}

// OpenBoundedWithReads is OpenBounded with the most read-only connections
// the read pool may open beside the writer (read_connections). Each is a
// file handle and a page cache, opened as reads ask for them, so an idle
// provider holds none beyond the one its statements were prepared on.
// **Zero opens no pool at all**, and every read runs on the write
// connection, behind the writer, as it did before the pool: the setting for
// a box that cannot spare the memory, which is never handed a read
// connection it said it did not want.
func OpenBoundedWithReads(path, writer string, maxBytes, reserve int64, readConns int) (*DB, error) {
	// Before the database is touched at all, so a broker that may not have
	// it does not write to a running one on its way to finding out.
	lock, err := Hold(path+lockSuffix, path)
	if err != nil {
		return nil, err
	}

	// Created by hand at 0600 before SQLite sees it. SQLite would create it
	// at 0644 less the umask, and the file holds whatever applications
	// published. Nothing here truncates: O_CREATE alone.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("storage %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("storage %s: %w", path, err)
	}

	sdb, err := sql.Open(driverName, "file:"+path+writerPragmas)
	if err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("storage %s: %w", path, err)
	}

	// One connection, which is what RFC 0001 already says the SQLite write
	// path is. It also makes every transaction below serial with every
	// other, so a read-modify-write of a channel's counters cannot
	// interleave with another one.
	//
	// **And it is never replaced, which the bound rests on.**
	// max_page_count, set after open and toggled by reliefTx, is this
	// connection's state, not the DSN's, so a new connection would come up
	// unbounded. database/sql replaces one only on driver.ErrBadConn, which
	// modernc.org/sqlite returns for a closed handle or a query interrupted
	// by a cancelled context, and nothing here passes a context. A driver
	// upgrade, or the first context-bearing call on this pool, must
	// re-check that. The lock's connection (Hold) rests on the same.
	sdb.SetMaxOpenConns(1)
	sdb.SetConnMaxLifetime(0)

	db := &DB{db: sdb, path: path, lock: lock, channels: map[string]any{}, budget: writeGroupBudget,
		marks: map[string]int{}, queuedFor: map[string]int{}}
	if err := db.prepare(writer); err != nil {
		_ = sdb.Close()
		_ = lock.Release()
		return nil, err
	}
	if err := db.bound(maxBytes, reserve); err != nil {
		_ = sdb.Close()
		_ = lock.Release()
		return nil, err
	}
	if err := db.openReads(readConns); err != nil {
		_ = sdb.Close()
		_ = lock.Release()
		return nil, err
	}
	return db, nil
}

// writerPragmas open the write connection: the provider's own, and
// **every transaction on it begins IMMEDIATE**. A write group's members read
// before they write (a session's record, its in-flight table) inside the
// transaction the write is in, and a deferred transaction that has read
// cannot wait for a second writer's lock: SQLite answers SQLITE_BUSY at once,
// without the busy timeout, where waiting could deadlock. Taking the write
// lock at BEGIN makes that wait the busy timeout's, as a transaction whose
// first statement writes always had. Nothing in the broker is a second
// writer - the read pool is query_only - so this is about a tool an operator
// points at the file, and the wait is at BEGIN: a group's leader holds the
// provider's write lock and the session store's lock through it, which holds
// every other write of the provider for up to busy_timeout, as the writer's
// own connection would have been held by that tool anyway.
const writerPragmas = pragmas + "&_txlock=immediate"

// readPragmas open a read-pool connection: the provider's own - WAL,
// busy_timeout - and query_only, so nothing sent down one can write. In the
// connection string, as the writer's are, so a pool that reconnects cannot
// produce a connection without them (RFC 0004 "WAL").
const readPragmas = pragmas + "&_pragma=query_only(1)"

// pureReads is every SQL text the read pool runs: reads whose rows go to a
// client and decide no write (TestEveryPureReadIsNamed). Prepared at open,
// outside any transaction, so a read never asks the pool for a second
// connection while holding one - with every connection in a read, that
// would wait for ever.
var pureReads = []string{channelFloorText, channelRecordsText, sessionRowText, sessionRowsText, shareReturnedText, positionText}

const channelFloorText = `SELECT floor FROM channels WHERE name = ?`

const channelRecordsText = `SELECT "offset", message_id, topic, payload, headers, ts, props
		   FROM records WHERE channel = ? AND "offset" >= ?
		  ORDER BY "offset" LIMIT ?`

// openReads opens the read pool of at most n connections, once the file is
// prepared: the schema and the WAL are in place, so a read-only connection
// finds both. None for n of zero, and reads stays nil.
func (d *DB) openReads(n int) error {
	if n <= 0 {
		return nil
	}
	rdb, err := sql.Open(driverName, "file:"+d.path+readPragmas)
	if err != nil {
		return fmt.Errorf("storage %s: the read pool: %w", d.path, err)
	}
	rdb.SetMaxOpenConns(n)
	rdb.SetMaxIdleConns(n)
	rdb.SetConnMaxLifetime(0)
	stmts := map[string]*sql.Stmt{}
	for _, text := range pureReads {
		st, err := rdb.Prepare(text)
		if err != nil {
			for _, kept := range stmts {
				_ = kept.Close()
			}
			_ = rdb.Close()
			return fmt.Errorf("storage %s: the read pool: %w", d.path, err)
		}
		stmts[text] = st
	}
	d.reads, d.readStmts, d.readConns = rdb, stmts, n
	return nil
}

// ReadConnections is the most read connections this provider opened its
// pool with, 0 where it has none: what read_connections came to, reported
// from the provider rather than from the file that asked for it.
func (d *DB) ReadConnections() int { return d.readConns }

// readTx runs fn in one read transaction on the read pool: **one snapshot
// per logical read, and a fresh one each time.** Beside the writer, not
// behind it - in WAL a reader sees the last commit and never waits for the
// one in progress, so a consumer's window no longer queues on the one write
// connection. The gateway row measured that queue at 90% of the connection
// waits, cutting the write ceiling 2.2-3x: 1,500 publishes a second with
// fifteen consumers against 3,351-4,692 with one.
//
//   - **One snapshot**: everything fn reads agrees - a floor and the records
//     above it come from the same instant, so a trim committing between them
//     cannot hand a reader the survivors as though nothing was missing
//     (invariant 1). SQLite takes a deferred transaction's snapshot at its
//     first read, not at Begin, so reading the floor first is what pins it
//     below the rows.
//   - **Fresh each time**: begun here and ended here, never held across a
//     wait for more, so every read sees every commit before it - including
//     the caller's own, which is read-your-writes.
//   - **Never inside a write transaction**: a read that feeds a write stays
//     on the write connection, and TestNoPureReadRunsInsideAWriteTransaction
//     holds that by syntax.
//
// A snapshot pins the write-ahead log: a checkpoint cannot pass a reader
// still in it, so sustained readers widen the transient RFC 0004 names,
// where what is on disk exceeds max_bytes by the log. Reads here are short
// windows, which is what keeps that small.
func (d *DB) readTx(fn func(*sql.Tx, func(string) *sql.Stmt) error) error {
	tx, err := d.reads.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx, func(text string) *sql.Stmt { return tx.Stmt(d.readStmts[text]) })
}

// closeReads stops the read pool. **Before the checkpoint Close runs**:
// TRUNCATE waits for readers to leave the log, and a pool left open could
// hold it.
//
// **The fields are left as they are**, closed rather than cleared: a read
// that races Close then fails with "database is closed", as a write racing
// it does on the writer. A field set to nil under it would be a nil
// dereference on the delivery path.
func (d *DB) closeReads() {
	d.stmtMu.Lock()
	defer d.stmtMu.Unlock()
	for _, st := range d.readStmts {
		_ = st.Close()
	}
	if d.reads != nil {
		_ = d.reads.Close()
	}
}

// bound sets the ceiling, in pages, which is the unit SQLite counts in.
//
// Two ceilings, not one. Ordinary publishes stop at maxBytes less the
// reserve; the operations that relieve the provider may reach maxBytes
// itself, by opening the reserve for the length of one transaction. A bound
// never refuses the operation that would relieve it (RFC 0003), and the
// reserve is how that rule reaches a store whose bound belongs to the
// engine rather than to saguin.
//
// The reserve is carved out of the operator's number rather than added to
// it, so the file never passes what they wrote.
//
// The page size is read rather than assumed: saguin pins it at 4KiB on a
// database it creates, but an operator may have made one elsewhere with a
// different one, and computing pages from a guess would bound the wrong
// amount.
//
// A ceiling that rounds down to no pages at all is refused. SQLite reads a
// max_page_count of zero as "ask the current value" rather than "allow
// nothing", so a provider given a few hundred bytes would silently come up
// unbounded - which is the shape of failure this whole file is written
// against.
func (d *DB) bound(maxBytes, reserve int64) error {
	if maxBytes == 0 {
		return nil
	}

	if err := d.queryRow(nil, `PRAGMA page_size`).Scan(&d.pageSize); err != nil {
		return fmt.Errorf("storage %s: reading the page size: %w", d.path, err)
	}
	d.fullPages = maxBytes / d.pageSize
	// Rounded up, so the reserve always holds the largest record rather than
	// the largest record less a page.
	reservePages := (reserve + d.pageSize - 1) / d.pageSize
	d.publishPages = d.fullPages - reservePages

	// Two ways to arrive at nothing, and they want different words: the
	// figure was too small to begin with, or the reserve took what was left.
	if d.fullPages < 1 {
		return fmt.Errorf(
			"storage %s: max_bytes of %d is less than one %d-byte page, and a provider that "+
				"can hold nothing is not one", d.path, maxBytes, d.pageSize)
	}
	if d.publishPages < 1 {
		return fmt.Errorf(
			"storage %s: max_bytes of %d is %d pages of %d bytes, and %d of them are held back for "+
				"the operations that free a full provider, so no publish would ever fit. Give it more, "+
				"or lower broker.limits.max_message_size",
			d.path, maxBytes, d.fullPages, d.pageSize, reservePages)
	}

	got, err := d.setCeiling(d.db, d.publishPages)
	if err != nil {
		return err
	}
	if got != d.publishPages {
		// SQLite refuses to set a ceiling below the pages already in use,
		// and answers with what it kept. The database is over its bound
		// rather than misconfigured, so it says so and carries on: writes
		// are refused until something frees pages, which is what the bound
		// means.
		d.overBound = true
	}
	return nil
}

// execQuerier is whatever a ceiling change runs on: the database itself, or
// a transaction that is holding the one connection.
type execQuerier interface {
	QueryRow(string, ...any) *sql.Row
}

// setCeiling moves max_page_count and reports what SQLite settled on, which
// is not always what was asked for: it will not set a ceiling below the
// pages the file already holds.
func (d *DB) setCeiling(on execQuerier, pages int64) (int64, error) {
	var got int64
	if err := on.QueryRow(fmt.Sprintf(`PRAGMA max_page_count = %d`, pages)).Scan(&got); err != nil {
		return 0, fmt.Errorf("storage %s: bounding the database: %w", d.path, err)
	}
	return got, nil
}

// reliefTx runs fn in one transaction with the reserve open, and closes the
// reserve again before that transaction ends.
//
// Both pragmas go inside the transaction on purpose. There is one connection,
// so a transaction holds it exclusively for its whole length - which is what
// stops an ordinary publish slipping in while the reserve is open and taking
// room it is not entitled to. Raising it outside the transaction would leave
// exactly that window.
//
// max_page_count is connection state rather than content, so a rollback does
// not undo either pragma; the ceiling is put back explicitly on both paths.
//
// What this cannot do is give the reserve back. SQLite will not set a
// ceiling below the pages the file already holds, so a relief write that
// grew the file has spent that much of the reserve for good - measured, and
// measured again to find it is spent once rather than per operation: six
// dead-letter moves in a row after the first grew the file not at all,
// because the removal frees pages the next move reuses. The reserve is
// therefore room to grow into once, not a renewable allowance, and only
// rebuilding the file returns it.
func (d *DB) reliefTx(fn func(*sql.Tx) error) error {
	if d.fullPages == 0 {
		return d.tx(fn) // unbounded: there is no reserve to open
	}
	return d.tx(func(tx *sql.Tx) error {
		if _, err := d.setCeiling(tx, d.fullPages); err != nil {
			return err
		}
		err := fn(tx)
		if _, cerr := d.setCeiling(tx, d.publishPages); cerr != nil && err == nil {
			err = cerr
		}
		return err
	})
}

// Commits reports how many transactions this provider has committed since
// it opened.
//
// It exists because group commit is otherwise unobservable: the records,
// the offsets and the counter rows come out identical whether they were
// stored one at a time or two hundred at a time, so this is the only thing
// that can tell a provider which collected publishes from one that ignored
// the setting and was merely quick.
func (d *DB) Commits() int64 { return d.committed.Load() }

// OverBound reports a database already larger than the bound it was given,
// so that a caller can say so once at startup rather than leave an operator
// to work out why every publish is refused.
func (d *DB) OverBound() bool { return d.overBound }

// Effective reports what the bound actually came to: the bytes a publish may
// take the file to, the bytes the reserve raises that to, and the page size
// it was all rounded in.
//
// max_bytes is enforced as a whole number of pages, so an operator's figure
// rounds down by up to one page and the reserve rounds up by up to one more.
// Saying so once at startup is what keeps that from being something they
// discover by arithmetic on a broker that refuses a publish sooner than they
// expected. Everything is zero on an unbounded provider.
func (d *DB) Effective() (publish, full, pageSize int64) {
	return d.publishPages * d.pageSize, d.fullPages * d.pageSize, d.pageSize
}

// OpenForExport opens an existing database to read it, and changes nothing
// in it.
//
// Open creates the file if it is not there, applies the schema if it is
// empty, and drops the positions whose sessions expired while the broker
// was down. Every one of those is right for a broker starting up and wrong
// for a migration: the source of a conversion is the operator's remaining
// copy until the destination has proved itself, and a command that reads it
// must not be the reason it changed. So this creates nothing, applies
// nothing, and recovers nothing - a database that is not already one this
// broker reads is refused by name rather than turned into one.
//
// It takes the provider's lock all the same. A database a broker is writing
// is a database whose channels are moving while they are read.
func OpenForExport(path string) (*DB, error) {
	// Before the lock, so that naming a path that is not there says so
	// rather than creating a lock file beside it.
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("storage %s: %w", path, err)
	}

	lock, err := Hold(path+lockSuffix, path)
	if err != nil {
		return nil, err
	}

	sdb, err := sql.Open(driverName, "file:"+path+pragmas)
	if err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("storage %s: %w", path, err)
	}
	sdb.SetMaxOpenConns(1)
	sdb.SetConnMaxLifetime(0)

	db := &DB{db: sdb, path: path, lock: lock, channels: map[string]any{}, budget: writeGroupBudget,
		marks: map[string]int{}, queuedFor: map[string]int{}}

	var version int
	if err := db.queryRow(nil, `PRAGMA user_version`).Scan(&version); err != nil {
		_ = sdb.Close()
		_ = lock.Release()
		return nil, fmt.Errorf("storage %s: reading the schema version: %w", path, err)
	}
	if version != schemaVersion {
		err := fmt.Errorf("storage %s: schema version %d (%s); this broker reads version %d",
			path, version, db.describe(), schemaVersion)
		_ = sdb.Close()
		_ = lock.Release()
		return nil, err
	}
	return db, nil
}

// Lock is a file one broker holds for as long as it owns a storage
// provider. Nothing reads it; holding it is the whole of what it does.
//
// It is in this package because SQLite is what makes it portable - the
// locking primitives differ on every platform and SQLite already knows
// them all - but it is not only for a sqlite provider. A memory provider
// with a snapshot directory needs the same guard and has no database of
// its own to take it with, so it holds one of these in its directory.
type Lock struct {
	db   *sql.DB
	path string
}

// Path is the file being held.
func (l *Lock) Path() string { return l.path }

// Release gives the lock back. A process that ends without calling this has
// it taken back by the kernel, which is why a broker that was killed leaves
// nothing stale behind.
func (l *Lock) Release() error {
	if err := l.db.Close(); err != nil {
		return fmt.Errorf("releasing the lock %s: %w", l.path, err)
	}
	return nil
}

// Hold takes the lock at name, or says who has it. What it guards is named
// by owner, which is what an operator reading the error has to act on.
//
// Nothing in SQLite stops two processes writing one database - WAL is
// designed to let them - and nothing in saguin would notice. Each broker
// caches a channel's next offset, so both would assign the same one: the
// primary key refuses whichever commits second, and that broker's cached
// counter never advances, so it refuses every publish on that channel from
// then on while looking healthy. No record is lost and none is duplicated;
// it is simply a broker that says no to everything.
//
// The listener usually catches this first, because two brokers from one
// configuration want the same port. It is two *different* configurations
// naming one file_path that gets through - which is what including the
// same channel file from two masters would do.
//
// The kernel releases the lock when a process ends, so a broker that was
// killed leaves nothing stale behind. What it does not survive is somebody
// deleting the lock file while a broker holds it: the next one creates a
// new file at that path and takes the lock on that, and both then believe
// they have it.
func Hold(name, owner string) (*Lock, error) {
	// 0600 from the moment it exists, like everything else a provider owns.
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage %s: opening the lock %s: %w", owner, name, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("storage %s: opening the lock %s: %w", owner, name, err)
	}

	db, err := sql.Open("sqlite", "file:"+name+lockPragmas)
	if err != nil {
		return nil, fmt.Errorf("storage %s: %w", owner, err)
	}
	// Never replaced, for the reason the provider's write connection is
	// never replaced (OpenBoundedWithReads): the exclusive lock is this
	// connection's, and a new one would come up holding nothing.
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)

	// Opening does not take the lock. A *write* does, and that is why the
	// second statement is here and is not conditional.
	//
	// locking_mode(EXCLUSIVE) takes the file's exclusive lock on the first
	// write and never gives it back; a read takes only a shared lock, and
	// shared locks do not exclude each other. So the table creation alone
	// held nothing after the first time it ran: on a lock file that already
	// had the table, IF NOT EXISTS wrote nothing, both brokers took shared
	// locks, and both started. A lock file exists on every run but the first,
	// so the guard worked exactly once per deployment and was inert
	// afterwards - measured, with two brokers on one snapshot directory both
	// reaching "saguin listening".
	//
	// INSERT OR REPLACE writes whether or not the row is there and whether or
	// not the value changes, so every holder attempts a write and the second
	// one is refused. Nothing ever reads the row; the write is the point.
	//
	// The write means a journal beside the lock while a broker runs, which is
	// a file an operator will see in a snapshot directory and which goes when
	// the process does. Left behind by a kill, SQLite rolls it back at the
	// next open, and nothing that reads the directory looks at anything but
	// .snapshot files. The alternative, keeping the journal in memory, turns
	// a crash mid-write into a lock file that will not open - reported as
	// another broker holding it, which would block a start for ever with the
	// wrong reason.
	if _, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS held (k INTEGER PRIMARY KEY);
		 INSERT OR REPLACE INTO held (k) VALUES (1)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf(
			"storage %s: another broker holds it (the lock is %s): %w. "+
				"Only one saguin may write a storage provider - look for a second instance, "+
				"or for two configurations naming the same file_path or snapshot_dir",
			owner, name, err)
	}
	return &Lock{db: db, path: name}, nil
}

// Path is the file this provider keeps its records in.
func (d *DB) Path() string { return d.path }

// Close releases the database. A checkpoint runs first so that the last
// commits are in the main file rather than only in the write-ahead log,
// which is what makes a copy of the file on its own a complete one.
func (d *DB) Close() error {
	d.stopFlusher()
	d.closeReads()
	if _, err := d.exec(nil, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		// Worth saying, not worth refusing to shut down over: the data is
		// committed either way, and the next open replays the log.
		err = fmt.Errorf("storage %s: checkpointing at shutdown: %w", d.path, err)
		d.closeStmts()
		cerr := d.db.Close()
		lerr := d.lock.Release()
		return errors.Join(err, cerr, lerr)
	}
	d.closeStmts()
	if err := d.db.Close(); err != nil {
		_ = d.lock.Release()
		return fmt.Errorf("storage %s: %w", d.path, err)
	}
	// Last, so the lock outlives every write this provider makes.
	if err := d.lock.Release(); err != nil {
		return fmt.Errorf("storage %s: %w", d.path, err)
	}
	return nil
}

// Abandon releases the provider's lock and does nothing else: no
// checkpoint, and the database handle is left exactly as it is.
//
// It models a broker that died rather than one that stopped. The kernel
// releases the lock of a process that ends, so nothing in cmd/ calls this -
// there is no orderly path it belongs to. What needs it is a test that
// stops a broker the way a power cut does and then opens the file again
// from the same process, which the lock would otherwise refuse, correctly.
func (d *DB) Abandon() error {
	return d.lock.Release()
}

// prepare applies the schema, checks the version, and recovers.
func (d *DB) prepare(writer string) error {
	var version int
	if err := d.queryRow(nil, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("storage %s: reading the schema version: %w", d.path, err)
	}

	switch {
	case version == 0:
		// Either a new file or one an older saguin created before there was
		// a version. The tables tell them apart: a database with records in
		// it and no version is not something to guess about.
		empty, err := d.isEmpty()
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf(
				"storage %s: holds tables but states no schema version; this broker reads version %d",
				d.path, schemaVersion)
		}
		if err := d.create(writer); err != nil {
			return err
		}
	case version != schemaVersion:
		return fmt.Errorf("storage %s: schema version %d (%s); this broker reads version %d",
			d.path, version, d.describe(), schemaVersion)
	}

	return d.recover(time.Now())
}

func (d *DB) isEmpty() (bool, error) {
	var n int
	err := d.queryRow(nil,
		`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("storage %s: %w", d.path, err)
	}
	return n == 0, nil
}

// describe says who wrote a file and when, in words that go straight into
// an error an operator has to act on. It is best-effort: a file this
// broker is already refusing may not have the table.
func (d *DB) describe() string {
	var writer, created string
	_ = d.queryRow(nil, `SELECT value FROM meta WHERE key = 'writer'`).Scan(&writer)
	_ = d.queryRow(nil, `SELECT value FROM meta WHERE key = 'created_at'`).Scan(&created)
	if writer == "" {
		writer = "an unknown version of saguin"
	} else {
		writer = "saguin " + writer
	}
	if created == "" {
		created = "an unrecorded time"
	}
	return "created by " + writer + " at " + created
}

// The schema.
//
// Two columns carry the whole of invariants 1 and 9, and they are columns
// rather than anything derived from the rows:
//
//   - channels.next is the offset the next record takes. Computed as the
//     largest surviving offset plus one, it restarts at 1 on a channel
//     retention has emptied, and a stored consumer position then points at
//     unrelated records the consumer reads in order and reports as done.
//   - channels.floor is the oldest offset still readable. Computed as the
//     smallest surviving offset, it reads as "nothing was ever removed" on
//     that same emptied channel - so a consumer that lost data is told it
//     is caught up.
//
// channels.bytes is what an append channel holds, against which its
// max_bytes is checked. It is a column for a different reason than the two
// above: summing it at open is correct but costs a scan of the whole
// channel, measured at 2.3 seconds for 800,000 records, and not having to
// read a channel at startup is most of what a database buys over a
// snapshot. It rides in the UPDATE that already advances next, so no write
// pays for it.
//
// A queue's bytes are summed at open instead and kept only in memory. Its
// table holds unresolved work rather than a history - bounded by the very
// max_bytes being checked - so the scan is small, and the resolution path
// stays one DELETE rather than gaining a second statement to write a
// counter with.
//
// A latest channel leaves the column at zero and never reads it. RFC 0002
// gives it no size bound: it holds one value per topic, so what grows is
// the number of topics rather than a history, and the retention period is
// what removes a topic nothing publishes to.
//
// Deleting every record of a channel therefore changes nothing about
// either, because no removal touches the channels row except to raise the
// floor. That is the case both invariants are really about.
//
// The two kinds of channel are keyed by what they are, not by one shape
// that covers both. An append channel is a sequence, so records is keyed
// by (channel, offset), which is also how every read of it walks. A latest
// channel is a map from topic to current value - the offset is only there
// so a consumer can order two values it has seen - so latest_values is
// keyed by (channel, topic), and replacing a value is one statement
// straight to the row.
//
// One table for both would key the map by the sequence's key: replacing a
// topic's value would then have to find it by scanning the channel, and
// fixing that with an index on topic would make every append pay for an
// index no append ever reads.
//
// STRICT so that a column declared INTEGER cannot quietly hold text. The
// primary key on (channel, "offset") makes a reused offset a failed insert
// rather than a silent overwrite, which is why nothing here ever uses
// INSERT OR REPLACE.
const schema = `
CREATE TABLE meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
) STRICT;

CREATE TABLE channels (
	name  TEXT PRIMARY KEY,
	kind  INTEGER NOT NULL,
	next  INTEGER NOT NULL,
	floor INTEGER NOT NULL,
	bytes INTEGER NOT NULL DEFAULT 0,
	CHECK (next >= 1 AND floor >= 1 AND floor <= next AND bytes >= 0)
) STRICT;

CREATE TABLE records (
	channel    TEXT    NOT NULL,
	"offset"   INTEGER NOT NULL,
	message_id TEXT    NOT NULL,
	topic      TEXT    NOT NULL,
	payload    BLOB,
	headers    TEXT,
	ts         INTEGER NOT NULL,
	props      TEXT,
	PRIMARY KEY (channel, "offset")
) STRICT;

CREATE TABLE latest_values (
	channel    TEXT    NOT NULL,
	topic      TEXT    NOT NULL,
	"offset"   INTEGER NOT NULL,
	message_id TEXT    NOT NULL,
	payload    BLOB,
	headers    TEXT,
	ts         INTEGER NOT NULL,
	props      TEXT,
	PRIMARY KEY (channel, topic)
) STRICT;

-- A row here is one unresolved record, and that is the whole of what a
-- queue keeps. Which worker holds it, under which Delivery ID, until when,
-- and the epoch that fences a superseded resolution are all deliberately
-- absent: no record is in flight after a restart, so none of them survives
-- one (invariant 15, RFC 0003 "Restart"). Storing them would be writing a
-- value on the delivery path in order to throw it away at the next start,
-- which is what the memory snapshot does and what RestoreQueue undoes.
--
-- What does survive is the record, how many attempts have been made on it,
-- and when it was first and last seen - so a record that has already
-- exhausted its attempts is dead-lettered rather than starting over.
CREATE TABLE queue_items (
	channel     TEXT    NOT NULL,
	"offset"    INTEGER NOT NULL,
	message_id  TEXT    NOT NULL,
	topic       TEXT    NOT NULL,
	payload     BLOB,
	headers     TEXT,
	ts          INTEGER NOT NULL,
	attempts    INTEGER NOT NULL,
	first_seen  INTEGER,
	last_seen   INTEGER,
	props      TEXT,
	PRIMARY KEY (channel, "offset")
) STRICT;

CREATE TABLE positions (
	channel    TEXT    NOT NULL,
	reader     TEXT    NOT NULL,
	"offset"   INTEGER NOT NULL,
	last_seen  INTEGER NOT NULL,
	expires_in INTEGER NOT NULL,
	PRIMARY KEY (channel, reader)
) STRICT;

-- A row here is an exactly-once publish held for its release in the
-- database of the channel it is for, beside the records it will join, so
-- the release is one transaction: the record takes its offset and the row
-- goes together, and there is never a moment with both or with neither
-- (store.HeldPublish). It has no offset, so no reader and no retention sees
-- it; the page ceiling counts it. Keyed by channel, client and identifier:
-- MQTT gives each session its own identifiers (section 2.2.1), and each
-- exchange is held in the one channel its PUBLISH resolved to.
CREATE TABLE held_publishes (
	channel    TEXT    NOT NULL,
	client     TEXT    NOT NULL,
	packet_id  INTEGER NOT NULL,
	message_id TEXT    NOT NULL,
	topic      TEXT    NOT NULL,
	payload    BLOB,
	headers    TEXT,
	ts         INTEGER NOT NULL,
	held       INTEGER NOT NULL,
	props      TEXT,
	PRIMARY KEY (channel, client, packet_id)
) STRICT;

-- A row here is one persistent MQTT session: a client that asked for its
-- session to outlive its connection. disconnected is when its client went
-- away, in Unix nanoseconds, and 0 while it is connected; expiry is the
-- interval it was granted, after limits.max_session_expiry capped it.
-- subscriptions is a JSON array, because nothing queries inside it and a
-- session is read whole.
-- will is the Will this session armed, as one JSON value or NULL where it
-- armed none, for the reason subscriptions is one: nothing queries inside it.
-- It carries the moment the Will becomes due, so a broker restarting inside a
-- Will Delay Interval publishes it when the device has actually been gone
-- that long rather than starting the interval again.
-- receive_maximum is the window the session's in-flight table was written
-- under, the client's Receive Maximum: the table may hold no more rows. A
-- session with no table has 0.
CREATE TABLE sessions (
	client          TEXT    PRIMARY KEY,
	expiry          INTEGER NOT NULL,
	disconnected    INTEGER NOT NULL,
	subscriptions   TEXT    NOT NULL,
	will            TEXT,
	receive_maximum INTEGER NOT NULL DEFAULT 0
) STRICT;

-- A row here is one broadcast message on the wire to a durable session: its
-- offset in the broadcast log, the packet identifier it went out under, the
-- QoS it went out at, and how far its exchange has got - 2 sent and
-- unanswered, 3 answered with a PUBREC so that the PUBREL is what is owed.
-- share_group is the shared group that handed the message to the session,
-- by its $share/ filter, or empty for one the session's own subscriptions
-- are owed: a group's row is behind the group's cursor, not the session's.
-- Keyed by identifier, because that is what an acknowledgement names. The
-- CHECK is store.ValidInFlight's rule, kept by the file as well as by the
-- code that writes it.
CREATE TABLE session_inflight (
	client      TEXT    NOT NULL,
	packet_id   INTEGER NOT NULL,
	"offset"    INTEGER NOT NULL,
	qos         INTEGER NOT NULL,
	state       INTEGER NOT NULL,
	share_group TEXT    NOT NULL DEFAULT '',
	PRIMARY KEY (client, packet_id),
	CHECK (packet_id BETWEEN 1 AND 65535 AND "offset" >= 1 AND qos IN (1, 2)
		AND state IN (2, 3) AND (state = 2 OR qos = 2)
		AND (share_group = '' OR substr(share_group, 1, 7) = '$share/'))
) STRICT;

-- A row here is one shared group's cursor on the broadcast log, keyed by the
-- group's $share/ filter: what the group is owed is the log after it. It is
-- kept while a session here holds the filter, and goes with the last of them.
CREATE TABLE share_groups (
	share_group TEXT    PRIMARY KEY,
	"cursor"    INTEGER NOT NULL,
	CHECK (substr(share_group, 1, 7) = '$share/' AND "cursor" >= 1)
) STRICT;

-- A row here is one delivery returned to a shared group by the ending of the
-- member's session it was in flight to, at QoS 1: behind the group's cursor,
-- and owed to the group again ahead of it. A hand-over of it takes it out.
-- WITHOUT ROWID, so a row is its key once rather than a row and an index
-- entry: an ending writes these in the room its in-flight rows give back.
CREATE TABLE share_returned (
	share_group TEXT    NOT NULL,
	"offset"    INTEGER NOT NULL,
	PRIMARY KEY (share_group, "offset"),
	CHECK (substr(share_group, 1, 7) = '$share/' AND "offset" >= 1)
) STRICT, WITHOUT ROWID;
`

func (d *DB) create(writer string) error {
	return d.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(schema); err != nil {
			return err
		}
		_, err := d.exec(tx,
			`INSERT INTO meta (key, value) VALUES ('writer', ?), ('created_at', ?)`,
			writer, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return err
		}
		// Inside the same transaction as the tables: a file with the schema
		// and no version would be refused by the check above on the next
		// open, having been created by this one.
		_, err = d.exec(tx, setUserVersion)
		return err
	})
}

// recover puts the database into the state a restart promises, before any
// listener opens.
//
// There is no queue work to do here, and that is the point rather than an
// omission. Nothing comes back in flight after a restart - a restored
// deadline belongs to a session that no longer exists, and a restored
// Delivery ID could be resolved by a client retrying across the very
// restart that interrupted it (invariant 15) - so none of it is stored in
// the first place. A queue row is an unresolved record and says nothing
// about who was holding it, which leaves nothing to undo. What survives is
// the attempt count, which is meant to.
//
// It runs unconditionally, including after a clean shutdown, because there
// is no way to tell the two apart and no reason to try.
func (d *DB) recover(now time.Time) error {
	return d.tx(func(tx *sql.Tx) error {
		// A position whose session would have expired while the broker was
		// down belongs to nobody, and keeping it is how the table grows for
		// ever (invariant 13). A session that asked never to expire is
		// stored with an interval long enough that this never fires, which
		// is what MQTT's own 0xFFFFFFFF means.
		//
		// **Only readers that have a session.** A bridge has none, so its
		// interval is zero, and zero here read as "expired before it was
		// written": measured, a position saved at offset 42 was gone the
		// next time the file was opened. The prefix is built from the same
		// constant store.PositionExpires tests, so the two stores cannot
		// come to different answers about one reader - which would be a
		// position that survives a snapshot and not a database.
		//
		// substr rather than LIKE, because LIKE reads `_` and `%` in the
		// pattern as wildcards. Today's scheme holds neither and a future one
		// is not this file's to predict, so the comparison is exact rather
		// than correct by coincidence.
		//
		// **The broadcast log's cursors are not judged by it.** A cursor there
		// lives exactly as long as its session record, in this same file: a
		// session's ending takes it in the same transaction (Sessions.drop),
		// and whether the session has expired is its record's to say. Judged
		// here instead, a session that was connected at a crash, and whose
		// cursor last moved longer ago than its expiry interval, would come
		// back with its session and without its place.
		_, err := d.exec(tx,
			`DELETE FROM positions
			  WHERE substr(reader, 1, ?) = ? AND last_seen + expires_in < ? AND channel != ?`,
			len(store.ReaderPrefixMQTT), store.ReaderPrefixMQTT, now.UnixNano(), store.BroadcastLog)
		if err != nil {
			return err
		}

		// **What goes in its place: a cursor whose session is not held.**
		// Without the sweep nothing else would take one out (invariant 13).
		// Only an MQTT session's cursor is judged so; a reader of another
		// kind is not a session's to lose.
		_, err = d.exec(tx,
			`DELETE FROM positions
			  WHERE channel = ? AND substr(reader, 1, ?) = ?
			    AND NOT EXISTS (SELECT 1 FROM sessions WHERE ? || client = positions.reader)`,
			store.BroadcastLog, len(store.ReaderPrefixMQTT), store.ReaderPrefixMQTT, store.ReaderPrefixMQTT)
		if err != nil {
			return err
		}

		// **Unreleased publishes are left where they are, and the start
		// decides them one session at a time.** They used to be deleted here,
		// unconditionally and rightly: a restarted broker answered every
		// client Session Present = 0, so nothing in the table could ever be
		// completed. Sessions come back now, and an exchange whose session
		// comes back with it is one the client will finish - it re-sends the
		// PUBREL it never had answered and the record is written then.
		//
		// The ones that cannot be finished still go, but the broker drops
		// them after it has restored what it can, because only it knows which
		// sessions came back (Broker.RestoreSessions). A row here is judged by
		// its session, and this transaction cannot see one.
		//
		// **A restored backup is decided the same way**, and by the same
		// rule: a provider copied from another moment carries the sessions
		// that were open then as well as the exchanges, so the two agree.
		return nil
	})
}

// EnsureChannel creates the row that holds a channel's counters, if this
// is the first time the channel has been seen.
//
// A channel that is no longer configured keeps its row rather than being
// tidied away. The row is where "everything up to 5000 was removed" is
// recorded, and deleting it turns a channel that lost data into one that
// looks brand new - which is the failure invariant 1 exists to report.
func (d *DB) EnsureChannel(name string, kind store.Kind) error {
	// **The write lock, like every other transaction that writes this row.**
	// Nothing can race it in practice - the row is only inserted for a
	// channel that has no store yet, so no batch can be holding offsets in
	// it - but that is a claim about when this runs, and a claim about when
	// something runs stops being true the day somebody calls it from
	// somewhere else. The rule is about the row.
	d.writing.Lock()
	defer d.writing.Unlock()

	return d.tx(func(tx *sql.Tx) error {
		var stored int64
		err := d.queryRow(tx, `SELECT kind FROM channels WHERE name = ?`, name).Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = d.exec(tx,
				`INSERT INTO channels (name, kind, next, floor, bytes) VALUES (?, ?, 1, 1, 0)`, name, int64(kind))
			return err
		}
		if err != nil {
			return err
		}
		have := store.Kind(stored)
		// A channel that changed type would otherwise read append records
		// out of a queue's table, or take queue work into a log where
		// nothing can acknowledge it. Neither reports a problem afterwards.
		if have != kind {
			return fmt.Errorf("channel %q is configured as %s but was stored as %s", name, kind, have)
		}
		return nil
	})
}

// full reports the error SQLite gives a write that would take the database
// past max_page_count, so that it reaches the broker as the same refusal a
// channel's own bound produces.
//
// Without this a provider at its ceiling answers 0x83 - "the storage did
// not work" - and sends an operator to look at a disk that is fine. It is
// the same distinction the channel bound makes, arriving from the engine
// instead of from a counter.
//
// The driver reports it as a code rather than a sentinel error, so the code
// is what is matched: 13 is SQLITE_FULL.
func full(err error) bool {
	var serr *sqlite.Error
	if errors.As(err, &serr) {
		return serr.Code() == 13
	}
	return false
}

// tx runs fn in one transaction and commits it, or rolls back and changes
// nothing. Every write below goes through it, so a change and the counter
// that describes it are never stored apart.
//
// The transaction is SQLite's deferred default, which normally has to
// upgrade its lock when a read is followed by a write - the usual source
// of SQLITE_BUSY. There is one write connection here, and the read
// connections are query_only, so there is never a second writer to lose that
// upgrade to, and the case does not arise.
func (d *DB) tx(fn func(*sql.Tx) error) error {
	d.preparePending()
	conn, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("storage %s: %w", d.path, err)
	}
	if err := fn(conn); err != nil {
		_ = conn.Rollback()
		if full(err) {
			return fmt.Errorf("storage %s: %w", d.path, store.ErrProviderFull)
		}
		return fmt.Errorf("storage %s: %w", d.path, err)
	}
	if err := conn.Commit(); err != nil {
		if full(err) {
			return fmt.Errorf("storage %s: %w", d.path, store.ErrProviderFull)
		}
		return fmt.Errorf("storage %s: %w", d.path, err)
	}
	d.committed.Add(1)
	return nil
}

// driverName is the database/sql driver a provider's database is opened
// with: modernc's, which registers itself as "sqlite". A variable only so a
// test can open one through a driver that counts what it prepares
// (TestAStatementIsPreparedOnceWhateverRunsIt).
var driverName = "sqlite"

// stmt is the prepared statement for a constant SQL text, prepared once and
// kept for the provider's life, or nil where it is not prepared yet and the
// caller runs the text.
//
// **SQL handed to database/sql as text is parsed again on every call**:
// modernc/sqlite keeps no statement cache, and sqlite3Prepare was 28% of CPU
// on a sqlite takeover and 4-6% on a channel read path. Prepared once, a
// statement is run through tx.Stmt inside a transaction, which database/sql
// answers with the driver statement already prepared on the transaction's
// connection - and there is one connection - so the driver prepares nothing
// again (TestAStatementIsPreparedOnceWhateverRunsIt), and modernc keeps the
// SQLite handle of a single-statement text for the statement's life. What
// SQLite itself recompiles on a step (sqlite3Reprepare) is its own, and was
// there before this cache. Python's sqlite3 module keeps such a cache per
// connection, keyed by the text, for the same reason.
//
// **Never prepared inside a transaction, and never under stmtMu.** Preparing
// asks the pool for its one connection, which a transaction is holding: a
// text first met inside one is run as text that once and marked pending, and
// prepared by the next transaction before it begins (preparePending), where
// waiting for the connection is what Begin does anyway. The lock guards the
// map only, so a caller waiting for the connection never holds what the
// connection's holder needs.
//
// **Only a constant text may be asked for** - TestEverySQLTextIsConstantOrBuilt
// holds every call to it - so the map holds one entry per text written in this
// package and cannot grow with what the broker is sent (invariant 13).
func (d *DB) stmt(tx *sql.Tx, text string) *sql.Stmt {
	d.stmtMu.Lock()
	st := d.stmts[text]
	if st == nil && tx != nil {
		if d.pending == nil {
			d.pending = map[string]struct{}{}
		}
		d.pending[text] = struct{}{}
	}
	d.stmtMu.Unlock()
	if st != nil || tx != nil {
		return st
	}
	return d.prepareText(text)
}

// prepareText prepares text outside any transaction and keeps it, or answers nil
// where it will not prepare, so the caller runs the text and reports why.
func (d *DB) prepareText(text string) *sql.Stmt {
	st, err := d.db.Prepare(text)
	if err != nil {
		return nil
	}
	d.stmtMu.Lock()
	defer d.stmtMu.Unlock()
	if kept := d.stmts[text]; kept != nil {
		_ = st.Close() // prepared twice at once: keep the first
		return kept
	}
	if d.stmts == nil {
		d.stmts = map[string]*sql.Stmt{}
	}
	d.stmts[text] = st
	delete(d.pending, text)
	return st
}

// preparePending prepares what transactions met unprepared (stmt). Called
// before a transaction begins, holding no transaction.
func (d *DB) preparePending() {
	d.stmtMu.Lock()
	if len(d.pending) == 0 {
		d.stmtMu.Unlock()
		return
	}
	texts := make([]string, 0, len(d.pending))
	for text := range d.pending {
		texts = append(texts, text)
	}
	d.pending = nil
	d.stmtMu.Unlock()
	for _, text := range texts {
		d.prepareText(text)
	}
}

// closeStmts closes what stmt prepared, before the database is closed.
func (d *DB) closeStmts() {
	d.stmtMu.Lock()
	defer d.stmtMu.Unlock()
	for _, st := range d.stmts {
		_ = st.Close()
	}
	d.stmts, d.pending = nil, nil
}

// exec, query and queryRow run a constant SQL text through its prepared
// statement (stmt), inside tx or on its own where tx is nil, and run the
// text itself where it is not prepared yet.
//
// **Each notes a failed statement on a write group's transaction** (note),
// which ends it there and then: nothing sent on it afterwards reaches SQLite.
func (d *DB) exec(tx *sql.Tx, text string, args ...any) (sql.Result, error) {
	var (
		res sql.Result
		err error
	)
	switch st := d.stmt(tx, text); {
	case st != nil && tx != nil:
		res, err = tx.Stmt(st).Exec(args...)
	case st != nil:
		return st.Exec(args...)
	case tx != nil:
		res, err = tx.Exec(text, args...)
	default:
		return d.db.Exec(text, args...)
	}
	d.note(tx, err)
	return res, err
}

func (d *DB) query(tx *sql.Tx, text string, args ...any) (*sql.Rows, error) {
	switch st := d.stmt(tx, text); {
	case st != nil && tx != nil:
		return d.notedOn(tx)(tx.Stmt(st).Query(args...))
	case st != nil:
		return st.Query(args...)
	case tx != nil:
		return d.notedOn(tx)(tx.Query(text, args...))
	}
	return d.db.Query(text, args...)
}

// notedOn passes a query's answer through, noting its failure on tx (note).
// Only a query's first step can fail it: modernc runs that step in Query,
// and a statement that writes and returns rows makes every change it makes
// in its first step.
func (d *DB) notedOn(tx *sql.Tx) func(*sql.Rows, error) (*sql.Rows, error) {
	return func(r *sql.Rows, err error) (*sql.Rows, error) {
		d.note(tx, err)
		return r, err
	}
}

func (d *DB) queryRow(tx *sql.Tx, text string, args ...any) *sql.Row {
	var row *sql.Row
	switch st := d.stmt(tx, text); {
	case st != nil && tx != nil:
		row = tx.Stmt(st).QueryRow(args...)
	case st != nil:
		return st.QueryRow(args...)
	case tx != nil:
		row = tx.QueryRow(text, args...)
	default:
		return d.db.QueryRow(text, args...)
	}
	// The statement has run by now: database/sql executes it in QueryRow
	// and keeps what it failed with for Err, apart from the no-rows answer,
	// which only Scan gives and which is an outcome rather than a failure.
	d.note(tx, row.Err())
	return row
}

// CommitGroup sets how the provider commits publishes: one transaction
// each, or collected into shared ones.
//
// A transaction closes when it has collected maxRecords records **or** when
// interval has passed since it opened, whichever happens first. An interval
// of zero gives every publish a transaction of its own, which is what an
// operator writing `none` asks for. A provider CommitGroup is never called
// on collects without waiting: see storeBehind.
//
// What it buys and what it costs are the same fact from two sides: the
// per-transaction work of a commit is paid once for the whole batch instead
// of once per record, and a publisher's acknowledgement waits for the
// batch. Measured on the store alone (BenchmarkAppendBatched), 256 records
// to a transaction is about six times the rate of one (BehindMaxRecords).
//
// Called once, before any listener opens.
func (d *DB) CommitGroup(interval time.Duration, maxRecords int) {
	if interval <= 0 || maxRecords <= 1 {
		d.each, d.interval, d.maxRecords = true, 0, 0
		return
	}
	d.each, d.interval, d.maxRecords = false, interval, maxRecords
}

// BehindMaxRecords is the most records one transaction collects when it
// collects without waiting. A group that reaches it stops taking members and
// the next arrival opens another, queued behind it.
//
// It bounds one transaction's size rather than setting a rate: the group
// is whatever arrived during one commit, and each connection stores one
// record at a time, so it reaches this only with more than this many
// connections publishing at once. 256 is where BenchmarkAppendBatched has
// stopped gaining: six times the rate of one record to a transaction, and
// 1024 is 1% more.
const BehindMaxRecords = 256

// A publish waiting for the transaction that will store it.
//
// The three publish paths - a record appended, a value replaced, a job
// enqueued - are one shape: reserve an offset under the channel's own lock,
// insert a row, write the channel's counter row, and advance those counters
// in memory once the transaction has committed. Only the last of those has
// to wait for the commit, which is why settle is separate from reserve.
//
// A channel fills the first five fields. The writer fills the rest.
type publish struct {
	// channel is the store this record belongs to, compared by identity so
	// that a batch writes one counter row per channel rather than one per
	// record. That is worth about half of everything batching buys - 5.8x
	// against 8.8x in BenchmarkAppendBatched - because the counter row is a
	// second statement on every single record otherwise.
	channel any

	// reserve takes the channel's lock, refuses the record if storing it
	// would cross the channel's bound, takes the next offset, and fills in
	// offset, insert and counters below.
	//
	// **It runs before the transaction opens**, so no channel lock is ever
	// held while the connection is. Every other write here takes them the
	// other way round - the channel's lock, then a transaction - and a
	// writer that took them in both orders would deadlock against a
	// retention sweep on the first busy afternoon.
	//
	// An error it returns is this record's alone: the batch commits without
	// it, and the publisher behind it is answered with it.
	reserve func(*publish) error

	// settle advances the channel's in-memory counters when the transaction
	// committed, or gives back exactly what reserve took when it did not.
	//
	// Giving it back is exact rather than approximate because DB.writing is
	// held from the first reserve to the last settle: no other publish can
	// have taken an offset in between, so the counters go back to what they
	// were and no offset is skipped.
	settle func(committed bool)

	// Filled in by reserve. counters writes the channel's row - the offset
	// to continue from and what the channel now holds - and only the
	// publish that took the highest offset for its channel is asked, once,
	// for the whole batch.
	offset   uint64
	insert   func(*sql.Tx) error
	counters func(*sql.Tx) error

	err error
}

// A group is the publishes collecting into one transaction. The first to
// arrive leads it: it waits out the interval, closes the group, and runs
// the transaction every publish in it is waiting for.
//
// There is no goroutine behind this and nothing to shut down. The leader is
// a publisher that was going to block on the commit anyway, so a batch
// costs no thread and a provider that is never written to runs no timer.
type group struct {
	// pubs is appended to under DB.forming and read by the leader only
	// after the group has been closed under that same lock.
	pubs []*publish

	// writes is a group of client writes instead (joinWrite): a group holds
	// publishes or writes, never both, so a publish batch keeps its own
	// rules and a write never shares a publish's failure. Appended to and
	// read as pubs is.
	writes []*write

	// answers marks a group of writes that answer a client's packet
	// (write.answers), served ahead of the other writes (handOn); departed,
	// a group of the others that is departed clients' (write.departed),
	// taking turns with connected sessions' (nextBackground).
	answers  bool
	departed bool

	// full is closed by whichever publish fills the group, so that a leader
	// waiting out the interval stops waiting.
	full chan struct{}

	// done is closed by the leader once every publish in the group has its
	// answer.
	done chan struct{}

	// turn is closed by storeBehind when the transaction before this group
	// has committed and the group has left DB.waiting, so it takes no more
	// members.
	turn chan struct{}
}

// store puts one publish in a transaction and returns when that transaction
// has ended - committed, in which case the record is durable and its offset
// is assigned, or not, in which case nothing was stored.
//
// With collecting switched off this is one publish in one transaction.
// The batch of one runs the same code as a batch of many so that there are
// not two write paths to keep in step.
func (d *DB) store(p *publish) error {
	switch {
	case d.each:
		d.runBatchSeen([]*publish{p}, &d.unbatched)
		return p.err
	case d.interval <= 0:
		return d.storeBehind(p)
	}

	d.forming.Lock()
	g := d.group
	leader := g == nil
	if leader {
		g = &group{full: make(chan struct{}), done: make(chan struct{})}
		d.group = g
	}
	g.pubs = append(g.pubs, p)
	if len(g.pubs) >= d.maxRecords {
		// Closed on the record count: whoever arrives next opens a group of
		// their own, and the leader stops waiting for the interval.
		d.group = nil
		close(g.full)
	}
	d.forming.Unlock()

	if !leader {
		<-g.done
		return p.err
	}

	closedBy := &d.closedByRecords
	t := time.NewTimer(d.interval)
	select {
	case <-g.full:
		t.Stop()
	case <-t.C:
		// Closed on the interval. Anything arriving from here on opens its
		// own group, so g.pubs stops changing before it is read.
		//
		// **Which of the two closed it is decided here rather than by the
		// select above**, because a group that fills in the same instant
		// the timer fires leaves both cases ready and select picks between
		// them at random. Whoever clears d.group is the one that closed the
		// group; if it is already cleared, the record count got there
		// first and that is what the metric should say. Nothing is stored
		// differently either way - it is the reading an operator uses to
		// tell a count set above the traffic from one that is met, so it
		// should not be a coin toss at the boundary.
		d.forming.Lock()
		if d.group == g {
			d.group = nil
			closedBy = &d.closedByInterval
		}
		d.forming.Unlock()
	}

	d.runBatchSeen(g.pubs, closedBy)
	close(g.done)
	return p.err
}

// runBatchSeen is runBatch for a publish that is not queued in DB.waiting
// (`none` and a duration), counted in DB.publishes from before it waits on
// DB.writing until its transaction ends, so a write group running meanwhile
// sees it (shouldYield) and gives way after its current member, as it does
// for a queued publish. It takes no lock a publisher does not already take
// (invariant 16).
func (d *DB) runBatchSeen(pubs []*publish, closedBy *atomic.Int64) {
	d.publishes.Add(1)
	defer d.publishes.Add(publishEnded - 1) // one fewer running, one more ended
	d.runBatch(pubs, closedBy)
}

// publishEnded is one ended publish in DB.publishes.
const publishEnded = 1 << 32

// storeBehind collects without waiting: a publish that finds no publish
// transaction running commits at once, alone, and one that finds one
// running joins the group that will commit when it ends. **It never waits
// for company**, so an idle broker answers as fast as a transaction per
// publish; under load a group is whatever arrived during one commit, so
// the batch grows with the load rather than with a setting. This is
// MySQL's binary log group commit with no sync delay, and PostgreSQL's
// commit_delay of zero.
//
// Groups commit in the order they opened. The first member of each leads
// it: it waits for its turn, runs the transaction, and hands the turn to
// the next group, or ends the run when there is none.
func (d *DB) storeBehind(p *publish) error {
	d.forming.Lock()
	if !d.committing {
		d.committing = true
		d.forming.Unlock()
		d.runBatch([]*publish{p}, &d.closedByCommit)
		d.handOn(ranPublishes)
		return p.err
	}
	var g *group
	if last := d.lastPublishes(); last != nil && len(last.pubs) < BehindMaxRecords {
		g = last
	}
	leader := g == nil
	if leader {
		g = &group{done: make(chan struct{}), turn: make(chan struct{})}
		d.waiting = append(d.waiting, g)
	}
	g.pubs = append(g.pubs, p)
	d.forming.Unlock()

	if !leader {
		<-g.done
		return p.err
	}
	<-g.turn
	closedBy := &d.closedByCommit
	if len(g.pubs) >= BehindMaxRecords {
		closedBy = &d.closedByRecords
	}
	d.runBatch(g.pubs, closedBy)
	// Handed on before this group is answered, so the next transaction
	// starts while these publishers are woken rather than after.
	d.handOn(ranPublishes)
	close(g.done)
	return p.err
}

// What a leader has just run, for handOn.
const (
	ranPublishes = iota
	ranAnswers
	ranDeparted
	ranConnected
)

// answersBeforeBackground is how many groups of answering writes may run in a
// row while a group of the other writes waits: the fifth turn is the other
// writes', so a stream of connects cannot hold acknowledgements back for ever.
const answersBeforeBackground = 4

// departedBeforeConnected is how many groups of departed clients' writes may
// run in a row, among the writes that answer nobody, while a group of
// connected sessions' writes waits: the fifth such turn is the connected
// sessions' (nextBackground).
const departedBeforeConnected = 4

// handOn gives the next waiting group its turn, or ends the run of
// transactions when nothing is waiting. The group leaves DB.waiting under
// the same lock its members joined under, so its members are fixed before
// its leader reads them.
//
// **Publishes and client writes take turns**: after writes, the oldest
// publish group waiting goes next, and after publishes a group of writes,
// where one is waiting. Strictly oldest-first, a publisher arriving during a
// reconnect storm waited behind every write group queued before it - measured
// with 10,000 sessions reconnecting, a PUBACK's median went from 0.19ms to
// 8ms - and now it waits behind at most one.
//
// **Among writes, the order DB.waiting holds them in** (queueWrites): a
// remainder a transaction gave way with first, then the writes that answer a
// client's packet, then the rest - except that after answersBeforeBackground
// answering groups in a row, a group of the rest goes next. Which of the rest
// is nextBackground's.
func (d *DB) handOn(ran int) {
	d.forming.Lock()
	defer d.forming.Unlock()
	switch ran {
	case ranAnswers:
		d.answersRun++
	case ranDeparted:
		d.answersRun = 0
		d.departedRun++
	case ranConnected:
		d.answersRun = 0
		d.departedRun = 0
	}
	if len(d.waiting) == 0 {
		d.committing = false
		return
	}
	next := -1
	if ran != ranPublishes {
		next = slices.IndexFunc(d.waiting, func(g *group) bool { return g.writes == nil })
	}
	if next < 0 && d.answersRun >= answersBeforeBackground {
		next = d.nextBackground()
	}
	if next < 0 {
		next = slices.IndexFunc(d.waiting, func(g *group) bool { return g.writes != nil })
		if next >= 0 && !d.waiting[next].answers {
			next = d.nextBackground()
		}
	}
	if next < 0 {
		next = 0
	}
	g := d.waiting[next]
	d.waiting = slices.Delete(d.waiting, next, next+1)
	d.countQueued(g, -1)
	close(g.turn)
}

// nextBackground is the next group of the writes that answer nobody, or -1
// where none waits: the oldest connected session's group after
// departedBeforeConnected departed clients' groups in a row, and otherwise
// the oldest departed client's, each where one waits. Called under
// DB.forming.
//
// **A departed client's writes and a connected session's are served four to
// one.** Ten thousand clients disconnecting at once queue ten thousand
// Disconnecteds, which answer nobody. Served oldest-first, a connected
// session's acknowledgement queued behind them waited for all of them - up
// to 11.4s, measured on a Raspberry Pi 4 - holding its drain's window closed
// the whole time (invariant 18). Taking turns one to one, it waited at most 0.12s, but the
// reconnecting clients waited behind the acknowledgements instead - a
// CONNECT moves up only its own id's writes (DB.Waiting), not the other
// departed clients' queued ahead of them - and the storm took 57s rather
// than 40s. Four to one, over ten storms, the longest acknowledgement
// waited 75ms, the storm took 44.5-47.3s, and no CONNACK timed out.
func (d *DB) nextBackground() int {
	departed, connected := -1, -1
	for i, g := range d.waiting {
		if g.writes == nil || g.answers {
			continue
		}
		if g.departed && departed < 0 {
			departed = i
		} else if !g.departed && connected < 0 {
			connected = i
		}
		if departed >= 0 && connected >= 0 {
			break
		}
	}
	if departed < 0 || connected >= 0 && d.departedRun >= departedBeforeConnected {
		return connected
	}
	return departed
}

// lastPublishes is the last waiting group of publishes: the one that still
// takes members. Called under DB.forming.
func (d *DB) lastPublishes() *group {
	for i := len(d.waiting) - 1; i >= 0; i-- {
		if d.waiting[i].writes == nil {
			return d.waiting[i]
		}
	}
	return nil
}

// lastWrites is the last waiting group of writes of one class - answering,
// departed clients' or connected sessions' - the one of its class that still
// takes members. Called under DB.forming.
func (d *DB) lastWrites(answers, departed bool) *group {
	for i := len(d.waiting) - 1; i >= 0; i-- {
		if g := d.waiting[i]; g.writes != nil && g.answers == answers && (answers || g.departed == departed) {
			return g
		}
	}
	return nil
}

// queueWrites puts a new group of writes in DB.waiting: a group of answering
// writes ahead of every group of the other writes, behind the answering
// groups already waiting; a group of the other writes last. Called under
// DB.forming.
func (d *DB) queueWrites(g *group) {
	at := len(d.waiting)
	if g.answers {
		if i := slices.IndexFunc(d.waiting, func(q *group) bool { return q.writes != nil && !q.answers }); i >= 0 {
			at = i
		}
	}
	d.waiting = slices.Insert(d.waiting, at, g)
}

// runBatch reserves an offset for every publish, stores them all in one
// transaction, and tells each what happened.
//
// **One writer at a time, taken here.** Reserving the offsets, committing
// them and advancing the counters they came from are one step, and a second
// batch reserving in the middle of it would hand out an offset against a
// count this one is about to put back. It is the same lock every other
// transaction in this package takes before it opens one, which is what
// keeps a channel's counter row and the copy of it in memory saying the
// same thing - and it costs nothing, because there is one write connection
// and the second writer would have waited for the first regardless.
//
// **A refusal is one record's and a failure is the whole batch's.** A
// record that would cross its channel's bound never enters the transaction,
// so the publishes beside it still commit - which is the point, because a
// full channel must not stop an unrelated one. A transaction that fails
// fails for everybody in it: what can fail at that point is the provider
// being out of room or the file being unwritable, and both are conditions
// every publisher in the batch would have met on its own.
func (d *DB) runBatch(pubs []*publish, closedBy *atomic.Int64) {
	d.writing.Lock()
	defer d.writing.Unlock()

	live := pubs
	refused := false
	for _, p := range pubs {
		if err := p.reserve(p); err != nil {
			p.err = err
			refused = true
		}
	}
	if refused {
		live = make([]*publish, 0, len(pubs))
		for _, p := range pubs {
			if p.err == nil {
				live = append(live, p)
			}
		}
		if len(live) == 0 {
			return
		}
	}

	err := d.tx(func(tx *sql.Tx) error {
		for _, p := range live {
			if err := p.insert(tx); err != nil {
				return err
			}
		}
		if len(live) == 1 {
			return live[0].counters(tx)
		}
		// One row per channel, written by whichever publish took the
		// highest offset for it - which is the last one reserved, so its
		// counters carry the totals of every record in the batch before it.
		// A batch touches one or two channels, so a scan beats a map and
		// allocates nothing.
		top := make([]*publish, 0, 4)
		for _, p := range live {
			at := -1
			for i, q := range top {
				if q.channel == p.channel {
					at = i
					break
				}
			}
			switch {
			case at < 0:
				top = append(top, p)
			case p.offset > top[at].offset:
				top[at] = p
			}
		}
		for _, p := range top {
			if err := p.counters(tx); err != nil {
				return err
			}
		}
		return nil
	})

	for _, p := range live {
		p.settle(err == nil)
		p.err = err
	}
	if err == nil {
		// Counted after the commit, so these describe transactions that
		// happened rather than transactions that were attempted.
		closedBy.Add(1)
		d.recordsCommitted.Add(int64(len(live)))
	}
}

// CommitStats is how this provider's publish transactions were closed and
// what they carried, for the metrics an operator reads to find out whether
// collecting publishes is doing anything (RFC 0005).
func (d *DB) CommitStats() store.CommitStats {
	return store.CommitStats{
		ClosedByRecords:  d.closedByRecords.Load(),
		ClosedByInterval: d.closedByInterval.Load(),
		ClosedByCommit:   d.closedByCommit.Load(),
		Unbatched:        d.unbatched.Load(),
		Records:          d.recordsCommitted.Load(),
		MaxRecords:       d.commitMaxRecords(),
	}
}

// commitMaxRecords is the most records one publish transaction may hold:
// the operator's figure, BehindMaxRecords where nothing was configured, and
// zero where each publish commits alone.
func (d *DB) commitMaxRecords() int {
	switch {
	case d.each:
		return 0
	case d.interval <= 0:
		return BehindMaxRecords
	}
	return d.maxRecords
}
