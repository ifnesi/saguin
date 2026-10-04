package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// Log is an append channel kept in the database: records ordered by an
// offset that is monotonic and never reused, even after removal or restart
// (invariant 9).
//
// next and floor are held in memory as well as in the channels row, which
// is safe because one provider writes on one connection and this is the
// only writer of its own channel. It is worth having because Floor is read on
// the delivery path, where there is nothing useful to do about a failure.
// Neither is ever a guess: both are loaded from the row at open and
// updated only from what a committed transaction reported.
//
// The number of records is not cached. Counting them costs a scan of the
// channel, and nothing but a test asks - a count kept up to date for a
// caller that does not exist is what made replacing a latest value two
// statements instead of one.
type Log struct {
	db   *DB
	name string

	// Prepared once. Handing SQL text to the driver on every append means
	// parsing it again on every append, and in a pure-Go SQLite that is a
	// large share of what an append costs. They are closed with the
	// database.
	bump   *sql.Stmt
	insert *sql.Stmt
	// sweep advances next, bytes and floor together, and trim removes every
	// record the floor has just passed. They are one transaction, never two.
	sweep *sql.Stmt
	trim  *sql.Stmt
	// lowest is Remove's: the lowest offset still held once its messages
	// have gone (removeStatement is the rest). Only the broadcast log runs
	// it.
	lowest *sql.Stmt

	mu    sync.Mutex
	next  uint64
	floor uint64

	// held is what the channel's records come to and maxBytes is its bound,
	// or zero for none. held is loaded from the row at open and advanced
	// only from what a transaction committed, exactly as next is.
	//
	// Trim and Remove decrement it, each in the transaction that deletes the
	// rows, and anything else that ever deletes a record must too - this is
	// the store where forgetting is permanent. A queue sums its own when it opens, so
	// a missed decrement there is gone at the next restart; this one is a
	// column, so a channel would refuse writes for ever with room it cannot
	// see, and nothing anywhere would say why.
	held     int64
	maxBytes int64
}

// SetMaxBytes bounds what the channel holds, or removes the bound at zero.
// Called once before any listener opens.
func (l *Log) SetMaxBytes(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maxBytes = n
}

// Bytes is what the surviving records come to.
func (l *Log) Bytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

// Log returns the store for an append channel, creating its row the first
// time the channel is seen. Asking twice for one channel gives back the
// same store, so a channel has exactly one writer.
func (d *DB) Log(name string) (*Log, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if built, ok := d.channels[name]; ok {
		lg, ok := built.(*Log)
		if !ok {
			return nil, fmt.Errorf("storage %s: channel %q is already open as a %T", d.path, name, built)
		}
		return lg, nil
	}

	if err := d.EnsureChannel(name, store.KindAppend); err != nil {
		return nil, err
	}
	l := &Log{db: d, name: name}
	if err := l.load(); err != nil {
		return nil, err
	}

	var err error
	if l.bump, err = d.db.Prepare(`UPDATE channels SET next = ?, bytes = ? WHERE name = ?`); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
	}
	for _, s := range []struct {
		into **sql.Stmt
		text string
	}{
		{&l.insert, `INSERT INTO records (channel, "offset", message_id, topic, payload, headers, ts, props)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`},
		{&l.sweep, `UPDATE channels SET floor = ?, bytes = ? WHERE name = ?`},
		{&l.trim, `DELETE FROM records WHERE channel = ? AND "offset" < ?`},
		{&l.lowest, `SELECT "offset" FROM records WHERE channel = ? AND "offset" >= ? ORDER BY "offset" LIMIT 1`},
	} {
		st, err := d.db.Prepare(s.text)
		if err != nil {
			// This store is never registered in d.channels, so nothing else
			// will ever close the ones already open.
			l.close()
			return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
		}
		*s.into = st
	}
	d.channels[name] = l
	return l, nil
}

// Broadcast returns the provider's broadcast log: every QoS 1 and 2 broadcast
// owed to a session that outlives its connection, written once however many
// sessions are owed it (RFC 0003 "Broadcast").
//
// It is kept in the append channels' tables under a reserved name, as the
// retained store is kept in the latest channels', so its next, floor and bytes
// are stored and advanced exactly as a channel's are (store.BroadcastLog). It
// is not a channel, and nothing can subscribe to it.
func (d *DB) Broadcast() (*Log, error) { return d.Log(store.BroadcastLog) }

func (l *Log) close() {
	for _, st := range []*sql.Stmt{l.bump, l.insert, l.sweep, l.trim, l.lowest} {
		if st != nil {
			_ = st.Close()
		}
	}
}

func (l *Log) load() error {
	err := l.db.queryRow(nil,
		`SELECT next, floor, bytes FROM channels WHERE name = ?`, l.name).Scan(&l.next, &l.floor, &l.held)
	if err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}

	return nil
}

// Append assigns the next offset and stores the record.
//
// The counter and the record move in one transaction. Assigning the offset
// outside it would let two records take one number, and the primary key on
// (channel, offset) would refuse the second - which is the safe failure,
// but a failure all the same.
//
// The counter is written as a value rather than as next = next + 1
// RETURNING next. Reading it back put the statement through the
// row-scanning path and cost more than the insert it accompanied, and it
// bought nothing: the copy held here is loaded from the row at open and
// advanced only after a transaction has committed, so it is equal to what
// is stored every time reserve runs - under l.mu, which is also what stops
// two appends computing one offset.
//
// **Which transaction is the provider's to choose** (DB.store): its own, as
// it has always been, or one shared with the publishes that arrived beside
// it when the operator has asked for that. Either way this returns when the
// transaction has ended, so a record is durable before its publisher is
// told anything - the wait is longer under a shared transaction, and it is
// still a wait on storage.
func (l *Log) Append(r store.Record) (store.Record, error) {
	p, assigned, err := l.publishOf(r)
	if err != nil {
		return store.Record{}, err
	}
	if err := l.db.store(p); err != nil {
		return store.Record{}, err
	}
	r.Offset = *assigned
	return r, nil
}

// publishOf is the write that stores r as this channel's next record, for
// the commit group (Append) or for a release (ReleaseHold), and where the
// offset it takes will be.
func (l *Log) publishOf(r store.Record) (*publish, *uint64, error) {
	headers, err := encodeHeaders(r.Headers)
	if err != nil {
		return nil, nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	properties, err := encodeProps(r)
	if err != nil {
		return nil, nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	size := store.RecordSize(r)

	assigned := new(uint64)
	return &publish{
		channel: l,
		reserve: func(p *publish) error {
			l.mu.Lock()
			defer l.mu.Unlock()

			// Before the offset is assigned, so a refused publish consumes
			// none.
			if l.maxBytes > 0 && l.held+size > l.maxBytes {
				return store.ErrFull
			}
			*assigned = l.next
			l.next++
			l.held += size

			// Captured rather than read inside the transaction: reading
			// them there would want l.mu while the connection is held,
			// which is the lock order that deadlocks against Trim. The
			// values are right because reserve runs under DB.writing, so
			// the last record of this channel in the batch holds the
			// totals of every one before it.
			next, held := l.next, l.held
			p.offset = *assigned
			off := *assigned
			p.insert = func(tx *sql.Tx) error {
				_, err := tx.Stmt(l.insert).Exec(
					l.name, int64(off), r.MessageID, r.Topic, r.Payload, headers,
					encodeTime(r.Timestamp), properties)
				return err
			}
			p.counters = func(tx *sql.Tx) error {
				_, err := tx.Stmt(l.bump).Exec(int64(next), held, l.name)
				return err
			}
			return nil
		},
		settle: func(committed bool) {
			if committed {
				return
			}
			l.mu.Lock()
			l.next--
			l.held -= size
			l.mu.Unlock()
		},
	}, assigned, nil
}

// txAppender is a log that can be appended to inside a transaction somebody
// else opened. It exists for exactly one caller: a queue dead-lettering a
// record, which must remove it and store it in one commit or do neither
// (invariant 5).
//
// It is unexported and takes a *sql.Tx, so only a store in this package can
// satisfy it - which is the right bound. A queue and its dead-letter channel
// share a storage provider by construction, so the two tables are always in
// one file and one commit reaches both.
type txAppender interface {
	appendTx(tx *sql.Tx, r store.Record) (store.Record, error)
	rollbackAppend(offset uint64, size int64)
}

// appendTx is Append, less the transaction: the caller supplies one.
//
// The in-memory copy of next is advanced here, while the transaction is
// still open, so that a second append cannot compute the same offset before
// this one commits. A caller whose transaction then fails must hand the
// offset back with rollbackAppend, or the counter stands one ahead of the
// rows and the next record leaves a hole in the channel - which a stored
// consumer position would read straight past.
func (l *Log) appendTx(tx *sql.Tx, r store.Record) (store.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	headers, err := encodeHeaders(r.Headers)
	if err != nil {
		return store.Record{}, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	properties, err := encodeProps(r)
	if err != nil {
		return store.Record{}, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}

	// No bound is checked here. This is the dead-letter move, which is not
	// a publish: a bound never refuses the operation that would relieve it
	// (RFC 0003), and refusing would leave the queue holding work it can
	// neither deliver nor be rid of.
	size := store.RecordSize(r)
	assigned := l.next
	if _, err := tx.Stmt(l.bump).Exec(int64(assigned+1), l.held+size, l.name); err != nil {
		return store.Record{}, err
	}
	if _, err := tx.Stmt(l.insert).Exec(
		l.name, int64(assigned), r.MessageID, r.Topic, r.Payload, headers,
		encodeTime(r.Timestamp), properties); err != nil {
		return store.Record{}, err
	}

	// Held under l.mu from reading next to writing it, so no other append to
	// this channel can compute the same offset in between. The transaction is
	// still open: if it rolls back, the counter here is wrong by one and the
	// next append refuses on the primary key rather than overwriting. That is
	// why rollbackAppend exists and why the caller must call it.
	l.next = assigned + 1
	l.held += size
	r.Offset = assigned
	return r, nil
}

// Trim removes the oldest records to bring the channel under maxBytes and
// to drop everything published before `before`, and reports how many went
// and what they came to. Either rule is off at its zero value.
//
// **The deletion and the floor are one transaction**, which is the whole of
// invariant 1. Two transactions leave a moment in which the rows are gone
// and the floor still covers them, and a consumer reading across that
// moment is served the oldest survivor and reports success over records it
// never received - which is the failure the floor exists to report.
//
// **The bytes go back in that same transaction**, and this is the store
// where forgetting is permanent: held is a column, so a missed decrement
// survives every restart and the channel refuses publishes against room it
// is no longer using, with nothing anywhere saying why. A queue sums its
// own at open, so the same mistake there is gone at the next start; that is
// exactly why this is the case to watch.
//
// The rows to remove are found by walking from the oldest and stopping at
// the first that must stay, so the read is proportional to what goes rather
// than to what the channel holds. Sizes are computed with store.RecordSize
// rather than in SQL: it is what a bound means, deliberately one function
// for both stores, and counting headers as the JSON they are stored as
// would decrement by a different number than the append added.
func (l *Log) Trim(before time.Time, maxBytes int64) (int, int64, error) {
	// **Held before the channel's own lock, and before the transaction.**
	// This writes a channel's counter row from a copy kept in memory, so it
	// must not fall between a batch of publishes reserving their offsets and
	// the transaction that stores them - the batch would write its own
	// totals over this one's, and the byte count a channel refuses publishes
	// against is a column rather than something recomputed at the next
	// start (DB.store).
	l.db.writing.Lock()
	defer l.db.writing.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	if before.IsZero() && maxBytes <= 0 {
		return 0, 0, nil
	}

	// **A size sweep over a channel already under its bound reads nothing.**
	// held is the byte count a publish is refused against, kept here in
	// memory and written to the channel's row - so a channel under maxBytes
	// cannot hold a record this call would take, and the query below can
	// only reach the same conclusion the long way. It reaches it by reading
	// a record: every column, payload and headers and properties included,
	// decoded and measured, once per channel per sweep.
	//
	// That is the whole of the size sweep's cost on an idle broker, and the
	// size sweep runs every second for the life of the process. At ten
	// thousand channels it was 164-174ms a second - a fifth of a core spent
	// establishing that nothing had changed. The age half cannot be answered
	// this way: a record becomes too old with nothing written, so `before`
	// still has to look.
	if before.IsZero() && l.held <= maxBytes {
		return 0, 0, nil
	}

	rows, err := l.db.query(nil,
		`SELECT "offset", message_id, topic, payload, headers, ts, props
		   FROM records WHERE channel = ? ORDER BY "offset"`, l.name)
	if err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	defer rows.Close()

	var (
		removed int
		freed   int64
		// floor is where the channel starts once these are gone: the lowest
		// offset still held, or next when nothing is. On a channel, which has
		// no gaps, that is one past the last record removed; on the
		// broadcast log it can be further on, past messages that left on
		// their own (Remove).
		floor = l.next
	)
	for rows.Next() {
		var (
			r          store.Record
			headers    sql.NullString
			properties sql.NullString
			ts         int64
			offset     int64
		)
		if err := rows.Scan(&offset, &r.MessageID, &r.Topic, &r.Payload, &headers, &ts, &properties); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		if r.Headers, err = decodeHeaders(headers); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		if err := decodeProps(properties, &r); err != nil {
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		r.Timestamp = decodeTime(ts)

		// An unset timestamp is never too old. Taken at face value it is a
		// date in 1754, which would empty the channel on the first sweep.
		tooOld := !before.IsZero() && !r.Timestamp.IsZero() && r.Timestamp.Before(before)
		tooMany := maxBytes > 0 && l.held-freed > maxBytes
		if !tooOld && !tooMany {
			floor = uint64(offset)
			break
		}
		freed += store.RecordSize(r)
		removed++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	// Closed before the write below opens a transaction on the one
	// connection this provider has.
	if err := rows.Close(); err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if removed == 0 {
		return 0, 0, nil
	}

	// Not reliefTx: retention is not an operation that relieves a full
	// provider on saguin's behalf - it is one that frees room, and it is
	// allowed to be refused when the provider has none to spare, exactly as
	// a publish is. It runs again on the next tick.
	held := l.held - freed
	if err := l.db.tx(func(tx *sql.Tx) error {
		if _, err := tx.Stmt(l.sweep).Exec(int64(floor), held, l.name); err != nil {
			return err
		}
		_, err := tx.Stmt(l.trim).Exec(l.name, int64(floor))
		return err
	}); err != nil {
		// Neither half happened, so the channel is exactly as it was and the
		// next sweep tries again. Nothing in memory has moved yet, which is
		// what makes that true.
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}

	l.floor = floor
	l.held = held
	return removed, freed, nil
}

// Remove takes out the messages at the offsets given, wherever they are in
// the log, and reports how many were there and what they came to. An offset
// the log does not hold - never written, trimmed, or removed already - is
// passed over and not counted, so a message released twice changes nothing
// the second time.
//
// **One transaction for all of them**, holding the deletions, the new byte
// count and the floor: next never moves, and the floor moves only when the
// lowest message still held goes, to the next one held or to next when the
// log is left empty (RFC 0004). Two transactions would leave a moment in
// which rows are gone and the floor still covers them, which is invariant
// 1's failure. A message taken from the middle leaves a gap that no read
// serves and no offset refills (invariant 9).
//
// Only the broadcast log may: see store.ErrNotTheBroadcastLog.
func (l *Log) Remove(offsets ...uint64) (int, int64, error) {
	if l.name != store.BroadcastLog {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, store.ErrNotTheBroadcastLog)
	}

	// Held before the log's own lock and before the transaction, for the
	// reason Trim holds it: this writes the counter row from a copy kept in
	// memory, and must not fall between a batch reserving its offsets and the
	// transaction that stores them.
	l.db.writing.Lock()
	defer l.db.writing.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	var (
		removed int
		freed   int64
		floor   = l.floor
	)
	// Through the reserve: a removal is what makes room in a full provider,
	// so the bound must never refuse it (RFC 0003), as it refuses none of
	// the other writes that relieve one.
	err := l.db.reliefTx(func(tx *sql.Tx) error {
		// **Exactly the offsets given, a chunk a statement**, never a range
		// from the lowest to the highest: an offset between two given ones
		// may still be owed to a session, and only the ones given are known
		// not to be.
		for start := 0; start < len(offsets); start += removeChunk {
			n, f, err := l.removeChunkOf(tx, offsets[start:min(start+removeChunk, len(offsets))])
			if err != nil {
				return err
			}
			removed += n
			freed += f
		}
		if removed == 0 {
			return nil
		}

		var lowest int64
		switch err := tx.Stmt(l.lowest).QueryRow(l.name, int64(l.floor)).Scan(&lowest); {
		case errors.Is(err, sql.ErrNoRows):
			floor = l.next
		case err != nil:
			return err
		default:
			floor = uint64(lowest)
		}
		_, err := tx.Stmt(l.sweep).Exec(int64(floor), l.held-freed, l.name)
		return err
	})
	if err != nil {
		// Nothing in memory has moved, so the log is exactly as it was and a
		// caller may ask again.
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if removed == 0 {
		return 0, 0, nil
	}
	l.floor = floor
	l.held -= freed
	return removed, freed, nil
}

// removeChunk is the most offsets one of Remove's statements names: with
// the channel, 501 parameters, under SQLite's smallest limit of 999, as
// ReadAt's are.
const removeChunk = 500

// **Remove's four statements, each written out and each built from
// constants**, as Acknowledge's are: prepared once each, and chosen by the
// size of the chunk, so a few offsets do not bind five hundred. The unused
// places repeat the chunk's last offset, which the IN set counts once.
const (
	removeStatement1   = removeDelete + "?" + removeReturning
	removeStatement10  = removeDelete + removeOffsets10 + removeReturning
	removeStatement100 = removeDelete + removeOffsets100 + removeReturning
	removeStatement    = removeDelete + removeOffsets500 + removeReturning

	removeDelete    = `DELETE FROM records WHERE channel = ? AND "offset" IN (`
	removeReturning = `) RETURNING message_id, topic, payload, headers`

	removeOffsets10  = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
	removeOffsets100 = removeOffsets10 + ", " + removeOffsets10 + ", " + removeOffsets10 + ", " + removeOffsets10 +
		", " + removeOffsets10 + ", " + removeOffsets10 + ", " + removeOffsets10 + ", " + removeOffsets10 + ", " +
		removeOffsets10 + ", " + removeOffsets10
	removeOffsets500 = removeOffsets100 + ", " + removeOffsets100 + ", " + removeOffsets100 + ", " +
		removeOffsets100 + ", " + removeOffsets100
)

// removeSlots is how many offsets the statement removing a chunk of n names:
// the smallest of the four that n fits.
func removeSlots(n int) int {
	for _, slots := range []int{1, 10, 100} {
		if n <= slots {
			return slots
		}
	}
	return removeChunk
}

// removeChunkOf deletes the records at the offsets of one chunk and reports
// how many there were and what they came to. **Measured with the one
// function both stores count by, never in SQL**, from the rows the delete
// returns, so the bytes given back are the bytes the append added.
func (l *Log) removeChunkOf(tx *sql.Tx, chunk []uint64) (int, int64, error) {
	slots := removeSlots(len(chunk))
	args := make([]any, 0, 1+slots)
	args = append(args, l.name)
	for i := range slots {
		args = append(args, int64(chunk[min(i, len(chunk)-1)]))
	}
	var rows *sql.Rows
	var err error
	switch slots {
	case 1:
		rows, err = l.db.query(tx, removeStatement1, args...)
	case 10:
		rows, err = l.db.query(tx, removeStatement10, args...)
	case 100:
		rows, err = l.db.query(tx, removeStatement100, args...)
	default:
		rows, err = l.db.query(tx, removeStatement, args...)
	}
	if err != nil {
		return 0, 0, err
	}
	// Closed before the next statement: the writer has one connection, and
	// a result set left open holds it.
	defer rows.Close()
	var (
		removed int
		freed   int64
	)
	for rows.Next() {
		var (
			r       store.Record
			headers sql.NullString
		)
		if err := rows.Scan(&r.MessageID, &r.Topic, &r.Payload, &headers); err != nil {
			return 0, 0, err
		}
		if r.Headers, err = decodeHeaders(headers); err != nil {
			return 0, 0, err
		}
		removed++
		freed += store.RecordSize(r)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	return removed, freed, nil
}

// rollbackAppend puts the in-memory counter back when the transaction an
// appendTx joined did not commit. The row was never written, so the offset
// it took is free again.
func (l *Log) rollbackAppend(offset uint64, size int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.next == offset+1 {
		l.next = offset
		l.held -= size
	}
}

// ReadFrom returns every record at or after offset. A read below the floor
// is refused (invariant 1).
func (l *Log) ReadFrom(offset uint64) ([]store.Record, error) {
	return l.ReadFromN(offset, 0)
}

// ReadFromN returns at most max records at or after offset, or every such
// record when max is zero.
//
// A read from below the retention floor is refused rather than served from
// the oldest survivor: serving it hands the consumer records it never
// asked for and lets it conclude it processed everything in between
// (invariant 1).
func (l *Log) ReadFromN(offset uint64, max int) ([]store.Record, error) {
	l.mu.Lock()
	floor := l.floor
	l.mu.Unlock()

	if offset < floor {
		return nil, store.ErrBelowFloor
	}

	// A negative limit is SQLite's own "no limit", so max of zero needs no
	// second query.
	limit := -1
	if max > 0 {
		limit = max
	}

	// **A channel's records are read on the read pool; the broadcast log's
	// are not.** A channel record is immutable once stored and leaves only by
	// a trim, and the floor is read in the same snapshot as the rows, so what
	// the pool returns is what the write connection would have. The broadcast
	// log's reads decide what its drains acknowledge and remove - a write - so
	// they stay on the write connection.
	if l.name != store.BroadcastLog && l.db.reads != nil {
		var recs []store.Record
		err := l.db.readTx(func(tx *sql.Tx, stmt func(string) *sql.Stmt) error {
			var fl int64
			if err := stmt(channelFloorText).QueryRow(l.name).Scan(&fl); err != nil {
				return err
			}
			if offset < uint64(fl) {
				return store.ErrBelowFloor
			}
			if betweenFloorAndRows != nil {
				betweenFloorAndRows()
			}
			rows, err := stmt(channelRecordsText).Query(l.name, offset, limit)
			if err != nil {
				return err
			}
			recs, err = scanRecords(rows, l.db.path, l.name)
			return err
		})
		if errors.Is(err, store.ErrBelowFloor) {
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		return recs, nil
	}
	rows, err := l.db.query(nil, channelRecordsText, l.name, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	return scanRecords(rows, l.db.path, l.name)
}

// betweenFloorAndRows is nil but in a test, which runs a trim inside a read's
// snapshot (TestAReadIsOneSnapshot).
var betweenFloorAndRows func()

// readAtChunk is the most offsets one ReadAt query names. SQLite bounds the
// parameters one statement may carry, and this stays inside the smallest
// bound any build of it has used (999), so a caller asking for more is
// answered in several queries rather than refused.
const readAtChunk = 500

// ReadAt returns the records held at the offsets given, in offset order and
// each once, leaving out an offset the log does not hold. See
// store.Log.ReadAt, which it answers exactly as.
func (l *Log) ReadAt(offsets ...uint64) ([]store.Record, error) {
	want := slices.Clone(offsets)
	slices.Sort(want)
	want = slices.Compact(want)

	var out []store.Record
	for len(want) > 0 {
		chunk := want[:min(len(want), readAtChunk)]
		want = want[len(chunk):]
		args := make([]any, 0, len(chunk)+1)
		args = append(args, l.name)
		for _, off := range chunk {
			args = append(args, int64(off))
		}
		rows, err := l.db.db.Query(
			`SELECT "offset", message_id, topic, payload, headers, ts, props
			   FROM records WHERE channel = ? AND "offset" IN (?`+strings.Repeat(", ?", len(chunk)-1)+`)
			  ORDER BY "offset"`, args...)
		if err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		got, err := scanRecords(rows, l.db.path, l.name)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
	}
	return out, nil
}

// FirstAtOrAfter returns the offset of the earliest record the broker
// received at or after t, and whether there is one at all.
//
// MIN over a predicate rather than an ordered walk, which answers
// correctly however the timestamps are ordered - the clock is the wall
// clock, and a step backwards from NTP leaves a short run of records whose
// times run the other way from their offsets. The memory store scans for
// the same reason, and the two must agree.
//
// `ts > 0` excludes a record stored before timestamps were kept, whose
// zero encodes as 0 and would otherwise answer every question about the
// distant past.
func (l *Log) FirstAtOrAfter(t time.Time) (uint64, bool, error) {
	l.mu.Lock()
	floor := l.floor
	l.mu.Unlock()

	var offset sql.NullInt64
	err := l.db.queryRow(nil,
		`SELECT MIN("offset") FROM records
		  WHERE channel = ? AND ts > 0 AND ts >= ? AND "offset" >= ?`,
		l.name, encodeTime(t), floor).Scan(&offset)
	if err != nil {
		return 0, false, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if !offset.Valid {
		return 0, false, nil
	}
	return uint64(offset.Int64), true, nil
}

// Next returns the offset the next appended record will take.
func (l *Log) Next() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// Floor returns the oldest offset still readable.
func (l *Log) Floor() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.floor
}

// Len returns how many records survive. It counts them, so it is not on
// the path anything hot takes - nothing in the broker calls it.
func (l *Log) Len() (int, error) {
	var n int
	if err := l.db.queryRow(nil,
		`SELECT count(*) FROM records WHERE channel = ?`, l.name).Scan(&n); err != nil {
		return 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	return n, nil
}

// Position returns where a reader had got to, and whether it has one here
// at all.
func (l *Log) Position(reader string) (store.Position, bool, error) {
	var (
		p         store.Position
		lastSeen  int64
		expiresIn int64
	)
	// **On the read pool when there is one**: a Retain Handling 2 SUBSCRIBE
	// reads this under the broker's lock, and on the write connection it
	// waited for the write group under way. A position is a row that only
	// a write changes, and every commit before this read is seen.
	var err error
	if l.db.reads == nil {
		err = l.db.queryRow(nil, positionText, l.name, reader).Scan(&p.Offset, &lastSeen, &expiresIn)
	} else {
		err = l.db.readTx(func(tx *sql.Tx, stmt func(string) *sql.Stmt) error {
			return stmt(positionText).QueryRow(l.name, reader).Scan(&p.Offset, &lastSeen, &expiresIn)
		})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return store.Position{}, false, nil
	}
	if err != nil {
		return store.Position{}, false, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	p.Reader = reader
	p.LastSeen = decodeTime(lastSeen)
	p.ExpiresIn = time.Duration(expiresIn)
	return p, true, nil
}

const positionText = `SELECT "offset", last_seen, expires_in FROM positions WHERE channel = ? AND reader = ?`

// SavePosition stores where a reader has got to.
//
// One row per reader and channel, replaced in place: a position is a
// cursor into the channel's own records, not a copy of them, so it costs
// one row however far behind the reader is.
//
// A position past next is refused, store.ErrPastNext, as the memory store
// refuses it. next here counts offsets reserved for a commit still under
// way, which is the figure a seek to the end is given, so that seek is never
// refused for a publish it raced.
func (l *Log) SavePosition(p store.Position) error {
	if next := l.Next(); p.Offset > next {
		return fmt.Errorf("storage %s, channel %q: %q at %d, and the log's next is %d: %w",
			l.db.path, l.name, p.Reader, p.Offset, next, store.ErrPastNext)
	}
	// Through the reserve: a consumer's stored position is one of the
	// operations RFC 0003 says a bound never refuses. A position that cannot
	// be written costs a replay rather than a record - at-least-once behaving
	// as promised - so this is the mildest member of that class, and it is
	// here because the rule is about the class.
	//
	// A connected session's position, answering nobody, but for a packet of
	// its client's that waits on it (DB.Waiting): a seek's PUBACK, and a
	// SUBSCRIBE with Retain Handling 2 moving it to the head.
	err := l.db.joinWrite(&write{client: clientOfReader(p.Reader), relief: true, run: func(tx *sql.Tx) error {
		_, err := l.db.exec(tx,
			`INSERT INTO positions (channel, reader, "offset", last_seen, expires_in)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT (channel, reader) DO UPDATE SET
			     "offset" = excluded."offset",
			     last_seen = excluded.last_seen,
			     expires_in = excluded.expires_in`,
			l.name, p.Reader, p.Offset, encodeTime(p.LastSeen), int64(p.ExpiresIn))
		return err
	}})
	if err != nil {
		return fmt.Errorf("channel %q: %w", l.name, err)
	}
	return nil
}

// DropReader forgets one reader's stored positions across every channel in
// this database, in one statement, and reports how many rows went.
//
// **The walk it replaces was the cost of connecting.** A Clean Start has to
// discard whatever the client had stored, and asking channel by channel is
// one transaction per channel however few positions the reader actually
// holds - ten thousand of them for a client that may have none at all.
// BenchmarkDiscardOneReadersPositions measures the pair at ten thousand
// channels: 98ms for the walk against 0.64ms for this, and this one barely
// moves when the reader does hold positions (0.76ms at a hundred and
// eighty) because it is one statement either way. The walk's cost is the
// channel count; this one's is the rows.
//
// **It is also atomic, which the walk could not be.** A takeover landing
// partway through the walk left some channels cleared and the rest not: a
// client answered Session Present = 1 and handed half its state, with the
// other half replaying from the floor. One statement cannot half-happen.
//
// The positions table is keyed by channel and reader together, so a
// reader's rows across every channel in this database are one predicate
// away - which is why this belongs to the provider rather than to a log.
func (d *DB) DropReader(reader string) (int, error) {
	var n int64
	// A plain transaction, like DropPosition: a delete frees pages rather
	// than needing them, so there is no reserve to open. A departed
	// client's - an ended session's - but for a CONNECT's clean start, which
	// waits on it (DB.Waiting).
	err := d.joinWrite(&write{client: clientOfReader(reader), departed: true, run: func(tx *sql.Tx) error {
		res, err := d.exec(tx, `DELETE FROM positions WHERE reader = ?`, reader)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	}})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// DropPosition forgets a reader, and reports whether it had a position. A
// position outlives its session for nobody (invariant 13).
func (l *Log) DropPosition(reader string) (bool, error) {
	var had bool
	// A departed client's - an ended session's - but for an UNSUBSCRIBE,
	// which waits on it (DB.Waiting).
	err := l.db.joinWrite(&write{client: clientOfReader(reader), departed: true, run: func(tx *sql.Tx) error {
		res, err := l.db.exec(tx, `DELETE FROM positions WHERE channel = ? AND reader = ?`, l.name, reader)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		had = n > 0
		return err
	}})
	if err != nil {
		return false, fmt.Errorf("channel %q: %w", l.name, err)
	}
	return had, nil
}

// Latest is a latest-value-per-topic channel kept in the database: one
// row per topic, replaced in place.
//
// Values carry offsets from the same monotonic counter an append channel
// uses, so a consumer can tell which of two values it holds is newer. The
// counter is not a position - a latest channel has no history to resume
// from - and there is no retention floor, because there is no gap a
// consumer could be told about: a topic has a current value or it does not.
type Latest struct {
	db   *DB
	name string

	bump *sql.Stmt
	// replace and add are Set's two halves. **The update runs first and
	// the insert only when it matched nothing**, which is what makes the
	// topic count free: the database says whether the row was there, so
	// nothing has to ask it separately. See Set.
	replace *sql.Stmt
	add     *sql.Stmt

	mu   sync.Mutex
	next uint64

	// held is how many topics have a current value: what this channel
	// holds, since it keeps one value per topic.
	//
	// **Counted once at open and advanced from committed transactions**,
	// which is the queue's arrangement for its depth and for the same
	// reason - RFC 0005 refuses a metric that has to scan a table on a
	// scrape path. It is kept only in memory, so a restart recounts it
	// rather than trusting a number that could have been written without
	// the row it was counting.
	//
	// A stored deletion is a row and counts as one, exactly as it does in
	// the memory store, whose count is the size of a map holding the same
	// deletions. The two providers answer this the same way or they answer
	// differently about one channel.
	held int64
}

// Latest returns the store for a latest channel, creating its row the
// first time the channel is seen. Asking twice for one channel gives back
// the same store, so a channel has exactly one writer.
func (d *DB) Latest(name string) (*Latest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if built, ok := d.channels[name]; ok {
		lt, ok := built.(*Latest)
		if !ok {
			return nil, fmt.Errorf("storage %s: channel %q is already open as a %T", d.path, name, built)
		}
		return lt, nil
	}

	if err := d.EnsureChannel(name, store.KindLatest); err != nil {
		return nil, err
	}
	l := &Latest{db: d, name: name}
	if err := d.queryRow(nil, `SELECT next FROM channels WHERE name = ?`, name).Scan(&l.next); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
	}

	var err error
	if l.bump, err = d.db.Prepare(`UPDATE channels SET next = ? WHERE name = ?`); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
	}
	for _, st := range []struct {
		into **sql.Stmt
		sql  string
	}{
		{&l.replace, `UPDATE latest_values SET "offset" = ?, message_id = ?, payload = ?,
			 headers = ?, ts = ?, props = ? WHERE channel = ? AND topic = ?`},
		{&l.add, `INSERT INTO latest_values (channel, topic, "offset", message_id, payload, headers, ts, props)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`},
	} {
		if *st.into, err = d.db.Prepare(st.sql); err != nil {
			l.close()
			return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
		}
	}

	// **Counted once, here.** RFC 0005 refuses a count on a scrape path
	// and says nothing against one at startup, which is where the queue
	// sums its own depth for the same reason. Measured on a Ryzen 7 260:
	// 0.43ms at ten thousand topics, 4.6ms at a hundred thousand, paid
	// once by a broker that is not yet serving anybody.
	if l.held, err = l.countRows(); err != nil {
		l.close()
		return nil, err
	}
	d.channels[name] = l
	return l, nil
}

// close releases this store's prepared statements. It is called when
// opening fails part-way, where the store is never registered and nothing
// else will ever reach these.
func (l *Latest) close() {
	for _, st := range []*sql.Stmt{l.bump, l.replace, l.add} {
		if st != nil {
			_ = st.Close()
		}
	}
}

// Set stores a record as its topic's current value, replacing whatever was
// there, and returns it with its offset assigned.
//
// One statement, straight to the row by primary key: the table is keyed by
// (channel, topic), which is what a latest channel is keyed by. The
// counter and the value move in the same transaction, so no reader finds a
// topic without a value and none finds two values with one offset.
//
// The replacement takes a new offset rather than keeping the old one. That
// is the only use a consumer has for a latest channel's offsets - telling
// which of two values it holds is newer.
func (l *Latest) Set(r store.Record) (store.Record, error) {
	p, assigned, err := l.publishOf(r)
	if err != nil {
		return store.Record{}, err
	}
	if err := l.db.store(p); err != nil {
		return store.Record{}, err
	}
	r.Offset = *assigned
	return r, nil
}

// publishOf is the write that makes r its topic's value, for the commit
// group (Set) or for a release (ReleaseHold), and where the offset it takes
// will be.
func (l *Latest) publishOf(r store.Record) (*publish, *uint64, error) {
	headers, err := encodeHeaders(r.Headers)
	if err != nil {
		return nil, nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	properties, err := encodeProps(r)
	if err != nil {
		return nil, nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}

	// No size bound is checked, and there is none to keep: a latest channel
	// is one value per topic, so what grows is the number of topics rather
	// than a history, and the retention period is the tool for a topic that
	// has gone quiet (RFC 0002). It is also the one write where measuring
	// would cost - a replacement is a difference, so the value going out
	// would have to be looked up first, which measured at half again the
	// cost of the write.
	//
	// **A record count is kept, and the write below pays nothing for it**
	// on the write a latest channel mostly does. Three ways of learning
	// whether a write added a topic were measured against the upsert this
	// replaced, like for like, on a channel of a thousand topics on a
	// Ryzen 7 260:
	//
	//	                                            replace   new topic
	//	the upsert this replaced                     17.6us      23.2us
	//	INSERT OR IGNORE then UPDATE                 19.1us          --
	//	AFTER INSERT/DELETE triggers, count in a row 32.4us          --
	//	update first, insert on a miss               17.2us      25.9us
	//
	// The first two were rejected at +9% and +83% on the common write, and
	// the order was turned round instead. The trigger arm did keep an exact
	// count, which incidentally settles that an upsert taking its DO UPDATE
	// branch does not fire an INSERT trigger - it was the presence of
	// triggers on the table that cost, not double work.
	//
	// **On this store rather than a stand-in**, BenchmarkLatestSet's sqlite
	// row went from 19.4us to 18.6us across the change, while props was
	// added to what a replacement writes: one more column, and still
	// faster, because this goes straight to the row where the upsert
	// attempted an insert and hit the conflict first.
	//
	// A topic seen for the first time is 27.5us there. Most of that gap is
	// inserting a row into an index that is growing, which the upsert paid
	// too - the extra statement's own share is the 2.7us the table above
	// isolates, once in that topic's life. Ten thousand devices pay 27ms
	// between them, ever.
	assigned := new(uint64)
	// **Set by the write and read by settle**, which is why it is here and
	// not inside p.insert: the transaction is what decides whether this
	// counts, and settle is the only place that knows.
	var added bool
	return &publish{
		channel: l,
		reserve: func(p *publish) error {
			l.mu.Lock()
			defer l.mu.Unlock()

			*assigned = l.next
			l.next++
			next := l.next
			p.offset = *assigned
			off := *assigned
			p.insert = func(tx *sql.Tx) error {
				// **Update first, insert only where nothing matched.**
				// Written this way round for the count: an upsert reports
				// success whichever branch it took, so a store using one
				// cannot say whether it has a topic more than it had
				// before without asking the database a second time. An
				// update reports how many rows it changed, and one that
				// changed none is a topic never seen.
				//
				// It costs nothing. Measured on a channel of a thousand
				// topics, replacing a value - the write a latest channel
				// mostly does - 17.2us against the upsert's 17.6us on a
				// Ryzen 7 260, because this goes straight to the row where
				// the upsert attempts an insert, hits the conflict and
				// then updates. A topic seen for the first time pays
				// 25.9us against 23.2us, once in that topic's life.
				res, err := tx.Stmt(l.replace).Exec(
					int64(off), r.MessageID, r.Payload, headers,
					encodeTime(r.Timestamp), properties, l.name, r.Topic)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n > 0 {
					return nil
				}
				if _, err := tx.Stmt(l.add).Exec(
					l.name, r.Topic, int64(off), r.MessageID, r.Payload, headers,
					encodeTime(r.Timestamp), properties); err != nil {
					return err
				}
				// **Two publishes to one new topic in a single batch count
				// once**, because the first has already inserted the row
				// inside this transaction and the second's update finds it.
				added = true
				return nil
			}
			p.counters = func(tx *sql.Tx) error {
				_, err := tx.Stmt(l.bump).Exec(int64(next), l.name)
				return err
			}
			return nil
		},
		settle: func(committed bool) {
			l.mu.Lock()
			defer l.mu.Unlock()
			if !committed {
				// The row was never written, so there is nothing to count
				// however p.insert ended.
				l.next--
				return
			}
			if added {
				l.held++
			}
		},
	}, assigned, nil
}

// What tells a stored deletion from a value, in SQL, and it is two
// predicates rather than one negated because **SQLite's length(NULL) is
// NULL rather than 0**. A deletion carries no payload, which the driver
// writes as NULL for a nil slice and as an empty blob for an empty one - so
// `length(payload) = 0` is false for the commoner of the two, and a
// deletion written that way matched neither branch of the sweep and would
// have lived for ever. The conformance script found it; nothing about the
// Go code looked wrong.
//
// They are constants beside each other so the two cannot drift into
// disagreeing about the same row, and they say the same thing as
// store.IsDeletion, which is the one answer everything else asks.
const (
	isDeletion = `(payload IS NULL OR length(payload) = 0)`
	isValue    = `(payload IS NOT NULL AND length(payload) > 0)`
)

// Delete removes a topic's current value and reports whether there was one.
//
// The row it removed comes off the count, and only once the transaction
// committed: a delete that failed removed nothing, and a count that fell
// anyway would be a channel reporting fewer topics than it holds until
// the next restart recounted it.
func (l *Latest) Delete(topic string) (bool, error) {
	var had bool
	err := l.db.tx(func(tx *sql.Tx) error {
		res, err := l.db.exec(tx, `DELETE FROM latest_values WHERE channel = ? AND topic = ?`, l.name, topic)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		had = n > 0
		return err
	})
	if err != nil {
		return false, err
	}
	if had {
		l.mu.Lock()
		l.held--
		l.mu.Unlock()
	}
	return had, nil
}

// Trim deletes the current value of every topic whose last publish was
// before the given moment, and reports how many went and what they came to.
//
// Expiry here deletes the current value, and that is the point rather than
// a defect (RFC 0003): a stale reading is worse than none, and a consumer
// cannot tell the two apart if the old one is still being served.
//
// There is no floor to advance. A latest channel has no history, so nothing
// holds a position into it. The rows are read first, for what went and
// what it freed, and deleted in a transaction of their own; the channel's
// held count is lowered by what the delete removed, as the log's is.
//
// The count of what went is returned all the same, because a sweep that
// removed a device's state and said nothing about it would leave an
// operator with no way to tell expiry from a device that stopped
// publishing.
// **Two clocks, because a value and a deletion are not the same thing to
// keep** - the memory store's version has why. A deletion is a row with an
// empty payload, so `length(payload) = 0` is what tells the two apart in
// SQL, matching store.IsDeletion exactly rather than keeping a second
// answer to the same question in a column.
func (l *Latest) Trim(before, deletionsBefore time.Time) (int, int64, error) {
	if before.IsZero() && deletionsBefore.IsZero() {
		return 0, 0, nil
	}

	// A deadline of zero means "never" for that kind, and the clause below
	// compares against it - so an unwritten clock is passed as a time no
	// stored row can be before rather than as a zero that would match every
	// row. `ts > 0` already excludes an unset timestamp; this excludes a
	// whole kind.
	valueCutoff, deletionCutoff := encodeTime(before), encodeTime(deletionsBefore)
	if before.IsZero() {
		valueCutoff = 0
	}
	if deletionsBefore.IsZero() {
		deletionCutoff = 0
	}
	const which = `ts > 0 AND ((` + isValue + ` AND ts < ?) OR (` + isDeletion + ` AND ts < ?))`

	// Read first, so the bytes reported are counted the one way a record is
	// ever counted rather than measured in SQL.
	rows, err := l.db.query(nil,
		`SELECT topic, message_id, payload, headers, props FROM latest_values
		  WHERE channel = ? AND `+which, l.name, valueCutoff, deletionCutoff)
	if err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	defer rows.Close()
	var (
		removed int
		freed   int64
	)
	for rows.Next() {
		var (
			r          store.Record
			headers    sql.NullString
			properties sql.NullString
		)
		if err := rows.Scan(&r.Topic, &r.MessageID, &r.Payload, &headers, &properties); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		if r.Headers, err = decodeHeaders(headers); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		if err := decodeProps(properties, &r); err != nil {
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		freed += store.RecordSize(r)
		removed++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if removed == 0 {
		return 0, 0, nil
	}

	// `ts > 0` excludes an unset timestamp, which is stored as 0 and would
	// otherwise be older than every deadline - emptying the channel on the
	// first sweep. The same rule as the log's, expressed where the rows are
	// chosen rather than after they are read.
	// **The count comes off what the delete did, not off what the scan
	// read.** They are two statements and the rows are chosen twice, so a
	// topic deleted between them is counted by the scan and not by the
	// delete. The delete is the one that decides what the channel now
	// holds.
	var swept int64
	if err := l.db.tx(func(tx *sql.Tx) error {
		res, err := l.db.exec(tx,
			`DELETE FROM latest_values WHERE channel = ? AND `+which,
			l.name, valueCutoff, deletionCutoff)
		if err != nil {
			return err
		}
		swept, err = res.RowsAffected()
		return err
	}); err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	l.mu.Lock()
	l.held -= swept
	l.mu.Unlock()
	return removed, freed, nil
}

// TrimExpired removes every value whose publisher's Message Expiry
// Interval has run out (MQTT-3.3.2-5), on the publisher's clock alone: a
// value with no expiry is not touched, however old, and a deletion is
// never touched - the operator's clocks are Trim's. It serves the
// retained store only; on a `latest` channel the client's expiry never
// deletes anything (invariant 2).
//
// The expiry lives inside the props JSON rather than in a column of its
// own, and the clause reads it there: `ts` is nanoseconds and `me`
// seconds, and the sum cannot overflow - the largest expiry is 2^32-1
// seconds, about 4.3e18 nanoseconds, against a ceiling of 9.2e18.
func (l *Latest) TrimExpired(now time.Time) (int, int64, error) {
	const which = `ts > 0 AND ` + isValue + ` AND props IS NOT NULL
		AND CAST(json_extract(props, '$.me') AS INTEGER) > 0
		AND ts + CAST(json_extract(props, '$.me') AS INTEGER) * 1000000000 <= ?`

	// Read first, so the bytes reported are counted the one way a record
	// is ever counted rather than measured in SQL - the same shape as
	// Trim, for the same reason.
	rows, err := l.db.query(nil,
		`SELECT topic, message_id, payload, headers, props FROM latest_values
		  WHERE channel = ? AND `+which, l.name, encodeTime(now))
	if err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	defer rows.Close()
	var (
		removed int
		freed   int64
	)
	for rows.Next() {
		var (
			r          store.Record
			headers    sql.NullString
			properties sql.NullString
		)
		if err := rows.Scan(&r.Topic, &r.MessageID, &r.Payload, &headers, &properties); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		if r.Headers, err = decodeHeaders(headers); err != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		if err := decodeProps(properties, &r); err != nil {
			return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
		}
		freed += store.RecordSize(r)
		removed++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	if removed == 0 {
		return 0, 0, nil
	}

	// The count comes off what the delete did, as Trim counts it: two
	// statements choose the rows twice, and the delete is the one that
	// decides what the store now holds.
	var swept int64
	if err := l.db.tx(func(tx *sql.Tx) error {
		res, err := l.db.exec(tx,
			`DELETE FROM latest_values WHERE channel = ? AND `+which,
			l.name, encodeTime(now))
		if err != nil {
			return err
		}
		swept, err = res.RowsAffected()
		return err
	}); err != nil {
		return 0, 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	l.mu.Lock()
	l.held -= swept
	l.mu.Unlock()
	return removed, freed, nil
}

// Get returns the current value of one topic, and whether there is one.
//
// The WHERE clause names both halves of `PRIMARY KEY (channel, topic)`, so
// this is an index lookup. Match with an exact predicate is not: it selects
// every row of the channel, sorts it, decodes every payload and header into
// Go, and then discards all but one.
func (l *Latest) Get(topic string) (store.Record, bool, error) {
	rows, err := l.db.query(nil,
		`SELECT "offset", message_id, topic, payload, headers, ts, props
		   FROM latest_values WHERE channel = ? AND topic = ?`, l.name, topic)
	if err != nil {
		return store.Record{}, false, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	found, err := scanRecords(rows, l.db.path, l.name)
	if err != nil {
		return store.Record{}, false, err
	}
	if len(found) == 0 {
		return store.Record{}, false, nil
	}
	return found[0], true, nil
}

// Match returns the current value of every topic a filter reaches, oldest
// first, so a subscriber receives state in the order it was written.
//
// The filter is MQTT's, not SQL's, so the rows come back and are sifted
// here. What bounds that is the shape of the channel: a latest channel
// holds one row per topic, never a history, so this reads the whole of
// what the channel is rather than the whole of what it has ever held.
// **A deletion is not current state and is not returned here** - the memory
// store's version has why. It is excluded where the rows are chosen rather
// than after they are read, so a channel holding many deleted topics does
// not decode them all to throw them away.
func (l *Latest) Match(match func(topic string) bool) ([]store.Record, error) {
	return l.match(` AND `+isValue, match)
}

// MatchWithDeletions is Match, including the deletions Match leaves out -
// the memory store's version has why there are two methods rather than one
// taking a flag.
func (l *Latest) MatchWithDeletions(match func(topic string) bool) ([]store.Record, error) {
	return l.match("", match)
}

func (l *Latest) match(only string, match func(topic string) bool) ([]store.Record, error) {
	rows, err := l.db.db.Query(
		`SELECT "offset", message_id, topic, payload, headers, ts, props
		   FROM latest_values WHERE channel = ?`+only+`
		  ORDER BY "offset"`, l.name)
	if err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	all, err := scanRecords(rows, l.db.path, l.name)
	if err != nil {
		return nil, err
	}
	out := make([]store.Record, 0, len(all))
	for _, r := range all {
		if match(r.Topic) {
			out = append(out, r)
		}
	}
	return out, nil
}

// Next returns the offset the next value stored will take.
func (l *Latest) Next() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next
}

// Len returns how many topics have a current value: what this channel
// holds. It reads the kept count and asks the database nothing, which is
// what lets saguin_channel_records carry a latest channel on this
// provider - the memory store's Len is the same promise from the size of
// its map, and the broker asks both through one interface.
func (l *Latest) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.held)
}

// countRows counts them in the database. **It is what seeds Len and never
// what answers it**: a count over a channel's rows is proportional to the
// topics it holds, which RFC 0005 refuses on a scrape path and permits at
// startup, where the queue sums its own depth the same way.
func (l *Latest) countRows() (int64, error) {
	var n int64
	if err := l.db.queryRow(nil,
		`SELECT count(*) FROM latest_values WHERE channel = ?`, l.name).Scan(&n); err != nil {
		return 0, fmt.Errorf("storage %s, channel %q: %w", l.db.path, l.name, err)
	}
	return n, nil
}

// scanRecords reads a query that selected a record's columns in the order
// every query above uses.
func scanRecords(rows *sql.Rows, path, channel string) ([]store.Record, error) {
	defer rows.Close()

	var out []store.Record
	for rows.Next() {
		var (
			r          store.Record
			headers    sql.NullString
			properties sql.NullString
			ts         int64
		)
		if err := rows.Scan(&r.Offset, &r.MessageID, &r.Topic, &r.Payload, &headers, &ts, &properties); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", path, channel, err)
		}
		h, err := decodeHeaders(headers)
		if err != nil {
			return nil, fmt.Errorf("storage %s, channel %q, offset %d: %w", path, channel, r.Offset, err)
		}
		r.Headers = h
		if err := decodeProps(properties, &r); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q, offset %d: %w", path, channel, r.Offset, err)
		}
		r.Timestamp = decodeTime(ts)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", path, channel, err)
	}
	return out, nil
}

// Headers are stored as JSON rather than in the binary encoding the
// snapshot uses, or in a table of their own.
//
// JSON because a sqlite provider is one an operator can open with the
// sqlite3 shell, and headers are where they will look when a message did
// not arrive as expected. A table of their own would mean an insert per
// header on every publish and a join on every read, which is worse on both
// counts. The cost is small beside the write itself: headers are bounded
// at max_header_count and max_header_bytes, and most records carry none -
// those store NULL and do no work at all.
// encodeHeaders writes a record's User Properties as a JSON **array** of
// name/value pairs.
//
// It was a JSON object, and an object cannot hold what MQTT 5 allows: the
// same name more than once, in the order the publisher wrote it. An object
// silently kept the last of a repeated name and returned the rest in
// whatever order the decoder felt like, so a consumer reading the same
// record twice could be handed two different lists. An array holds exactly
// what arrived.
//
// The shape change is why the schema version moved: a row written as an
// object and read as an array is not a migration this reads, and a store
// that half-reads a file is the failure invariant 14 is about.
func encodeHeaders(h []store.Header) (any, error) {
	if len(h) == 0 {
		return nil, nil
	}
	pairs := make([][2]string, len(h))
	for i, one := range h {
		pairs[i] = [2]string{one.Key, one.Value}
	}
	b, err := json.Marshal(pairs)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// props is what a record carries beside its payload, headers, identity and
// timestamp, as held in the `props` column: one JSON object, absent
// entirely for a record that carries none.
//
// **Mostly the MQTT 5 publish properties, and one piece of saguin's own**
// (`Bridge`). The column was the publish properties alone until a bridge
// needed to mark what it brought in; a second column for one string would
// have bumped the schema version, and this file refuses an in-place
// upgrade, so every existing database would have been refused until an
// operator exported it with the older binary. A field here costs nobody
// anything: a row written before it reads back empty, which is the truth
// about that row.
//
// The short names keep the column small - a record's properties are read
// whole and never queried, so nothing reads these but the two functions
// below. CorrelationData is `[]byte`, which the encoder writes as base64:
// it is arbitrary bytes, and putting it through a string would replace
// every byte that is not valid UTF-8 with U+FFFD and report no error.
type props struct {
	ContentType     string `json:"ct,omitempty"`
	ResponseTopic   string `json:"rt,omitempty"`
	CorrelationData []byte `json:"cd,omitempty"`
	// Present and empty, which omitempty cannot say of the three above.
	ContentTypeEmpty     bool   `json:"cte,omitempty"`
	ResponseTopicEmpty   bool   `json:"rte,omitempty"`
	CorrelationDataEmpty bool   `json:"cde,omitempty"`
	PayloadFormat        *byte  `json:"pf,omitempty"`
	MessageExpiry        uint32 `json:"me,omitempty"`
	Retain               bool   `json:"r,omitempty"`
	// The QoS the message was published at, which a retained delivery is
	// capped to ([MQTT-3.8.4-8]). Omitted at zero like the rest, and zero
	// is the honest default: a record written before this column carried it
	// reads back as QoS 0, which is what a delivery capped to it goes out
	// at rather than a guess dressed up as the publisher's.
	QoS byte `json:"q,omitempty"`

	// Bridge names the bridge a record arrived on, empty for one this
	// broker originated - the loop guard, and the one field here that is
	// saguin's rather than MQTT's. Omitted at empty like the rest, and
	// empty is the honest default: a row written before this existed came
	// from a client or from the replica bridge that no longer exists, and
	// reads back as locally originated, which is what an `out` rule should
	// do with it.
	Bridge string `json:"br,omitempty"`

	// Publisher is the client id that published the record, kept for the
	// one comparison No Local asks for [MQTT-3.8.3-3] and read by nothing
	// else - see store.Record.Publisher, which says why it never reaches a
	// subscriber. Empty is the honest default here too: a row written
	// before this existed cannot say who published it, and a No Local
	// subscriber is sent it rather than being silently denied a record on a
	// guess.
	Publisher string `json:"pub,omitempty"`

	// ForGroups marks a broadcast log message copied from a channel for the
	// shared groups over it (store.Record.ForGroups). False on every other
	// record, and on every row written before it existed, when no copy did.
	ForGroups bool `json:"fg,omitempty"`

	// OwedTo is the one session a broadcast log message copied from a
	// retained value is owed to (store.Record.OwedTo). Empty on every other
	// record.
	OwedTo string `json:"to,omitempty"`
}

// encodeProps writes the publish properties, or nothing at all when there
// are none - which is every record published before they were kept, and
// most since.
func encodeProps(r store.Record) (any, error) {
	if !r.HasProps() {
		return nil, nil
	}
	p := props{
		ContentType:          r.ContentType,
		ResponseTopic:        r.ResponseTopic,
		CorrelationData:      r.CorrelationData,
		ContentTypeEmpty:     r.ContentTypeEmpty,
		ResponseTopicEmpty:   r.ResponseTopicEmpty,
		CorrelationDataEmpty: r.CorrelationDataEmpty,
		MessageExpiry:        r.MessageExpiry,
		Retain:               r.Retain,
		Bridge:               r.Bridge,
		Publisher:            r.Publisher,
		ForGroups:            r.ForGroups,
		OwedTo:               r.OwedTo,
		QoS:                  r.QoS,
	}
	if r.PayloadFormatFlag {
		v := r.PayloadFormat
		p.PayloadFormat = &v
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// decodeProps reads them back onto a record.
func decodeProps(s sql.NullString, r *store.Record) error {
	if !s.Valid || s.String == "" {
		return nil
	}
	var p props
	if err := json.Unmarshal([]byte(s.String), &p); err != nil {
		return fmt.Errorf("its publish properties are not readable: %w", err)
	}
	r.ContentType = p.ContentType
	r.ResponseTopic = p.ResponseTopic
	r.CorrelationData = p.CorrelationData
	r.ContentTypeEmpty, r.ResponseTopicEmpty, r.CorrelationDataEmpty =
		p.ContentTypeEmpty, p.ResponseTopicEmpty, p.CorrelationDataEmpty
	r.MessageExpiry = p.MessageExpiry
	r.Retain = p.Retain
	r.Bridge = p.Bridge
	r.Publisher = p.Publisher
	r.ForGroups = p.ForGroups
	r.OwedTo = p.OwedTo
	r.QoS = p.QoS
	if p.PayloadFormat != nil {
		r.PayloadFormatFlag = true
		r.PayloadFormat = *p.PayloadFormat
	}
	return nil
}

func decodeHeaders(s sql.NullString) ([]store.Header, error) {
	if !s.Valid || s.String == "" {
		return nil, nil
	}
	var pairs [][2]string
	if err := json.Unmarshal([]byte(s.String), &pairs); err != nil {
		return nil, fmt.Errorf("its headers are not readable: %w", err)
	}
	h := make([]store.Header, len(pairs))
	for i, p := range pairs {
		h[i] = store.Header{Key: p[0], Value: p[1]}
	}
	return h, nil
}

// A time is stored as Unix nanoseconds, with the zero time as 0. UnixNano
// on a zero Time is a number nothing means, and reading it back gives a
// date in 1754 rather than "not set" - the same rule the snapshot format
// follows, so the two agree about what an unset timestamp is.
func encodeTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func decodeTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// ListPositions is every stored position, furthest-behind first, capped at
// limit - and how many there are in total.
//
// **Ordered by offset rather than by reader, so the cap keeps the ones
// worth looking at.** The question is which of a fleet is behind, and a cap
// over an alphabetical list would answer with three hundred readers whose
// names begin with `a`.
//
// **Two statements rather than one**, unlike the pair LowestPosition asks
// for: a `count(*)` cannot ride a `LIMIT`, and the total is what stops a
// caller shown fifty of three hundred believing it has seen the fleet. This
// runs when a person asks rather than on every scrape, which is the whole
// difference between this route and the catalogue.
func (l *Log) ListPositions(limit int) ([]store.Position, int, error) {
	var total int
	if err := l.db.queryRow(nil,
		`SELECT count(*) FROM positions WHERE channel = ?`, l.name).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := l.db.query(nil,
		`SELECT reader, "offset", last_seen, expires_in FROM positions
		  WHERE channel = ? ORDER BY "offset", reader LIMIT ?`, l.name, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var out []store.Position
	for rows.Next() {
		var p store.Position
		var lastSeen, expiresIn int64
		if err := rows.Scan(&p.Reader, &p.Offset, &lastSeen, &expiresIn); err != nil {
			return nil, 0, err
		}
		p.LastSeen, p.ExpiresIn = time.Unix(0, lastSeen).UTC(), time.Duration(expiresIn)
		out = append(out, p)
	}
	return out, total, rows.Err()
}

// LowestPosition is the lowest offset any durable consumer of this channel
// has stored, or zero when none has.
//
// It is what saguin_channel_consumer_position_min reports, and against the
// retention floor it is RFC 0005's one alert: a floor above it means
// retention has already deleted data a consumer had not reached, so that
// consumer is refused when it returns rather than served the oldest
// survivor. Invariant 1 holding, and also a customer's missing afternoon.
//
// **It asks the table, every time.** This was a field maintained on the way
// past instead - set when a position was saved below it - and that field
// only ever fell: a consumer that connected to an empty channel stored
// offset 1 and pinned the gauge there however far it then advanced. So the
// alert above fired on a broker where nothing was wrong, permanently, from
// the moment retention passed the offset the first consumer started at.
// A permanent false alarm is not the conservative error the comment on that
// field claimed; it is the alert being unusable, and the event it exists to
// catch going past unread. The memory store never had it, because it walks
// its positions rather than remembering a minimum.
//
// What that field was avoiding is real and is answered by measurement
// rather than by caching: BenchmarkLowestPosition puts the cost of this
// query beside the memory store's walk at a hundred, a thousand and ten
// thousand consumers on one channel, and RFC 0005 carries the numbers. It
// is one aggregate over one channel's rows, once per scrape, and the scrape
// interval has a configured floor.
//
// A read that fails reports none rather than a number. An absent series
// already means "nobody holds a position here", and RFC 0005 is explicit
// that a dashboard reading nothing is better off than one reading a wrong
// number.
// **The count rides the same aggregate**, which is what RFC 0005 said it
// would when it left `saguin_channel_consumers` unpublished: `min()` and
// `count()` in one statement is one index pass over the same rows, where a
// second method would be a second query per channel per scrape for a number
// this one has already read.
func (l *Log) LowestPosition() (lowest uint64, consumers int) {
	var low sql.NullInt64
	var n int
	if err := l.db.queryRow(nil,
		`SELECT min("offset"), count(*) FROM positions WHERE channel = ?`,
		l.name).Scan(&low, &n); err != nil {
		return 0, 0
	}
	if !low.Valid {
		return 0, n
	}
	return uint64(low.Int64), n
}
