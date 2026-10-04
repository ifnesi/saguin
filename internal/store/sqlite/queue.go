package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// Queue is a queue channel kept in the database.
//
// A row is one unresolved record and nothing more. Which worker holds it,
// under which Delivery ID, until when, and the epoch that fences a
// superseded resolution are held here in memory, because none of them
// outlives a restart: no record is in flight afterwards, so a stored
// deadline would belong to a session that no longer exists and a stored
// Delivery ID could be resolved by a client retrying across the very
// restart that interrupted it (invariant 15).
//
// That is what makes the two operations on the broker's clock cost nothing.
// Offer is a bounded read and no write at all; ExpiredLeases is a walk of
// the map below and touches the database not at all. Mirroring the memory
// store instead would have made both of them writes, on a timer, for state
// thrown away at the next start.
//
// What is written is what survives: the record, its attempt count, and when
// it was first and last delivered - so a record that has spent its attempts
// is dead-lettered rather than starting over.
type Queue struct {
	db   *DB
	name string

	bump   *sql.Stmt
	insert *sql.Stmt
	count  *sql.Stmt
	remove *sql.Stmt
	// offer is Offer's query, prepared by SetBackoff, which is what varies
	// its text (the backoff clause, q.ready); nil until then, and Offer
	// runs the text itself.
	offer *sql.Stmt

	mu   sync.Mutex
	next uint64

	// held is what the unresolved records come to, and maxBytes is the
	// bound or zero for none.
	//
	// Unlike an append channel's, this is summed at open and kept only in
	// memory. The table holds unresolved work rather than a history, and
	// this very bound is what keeps it small, so the scan is cheap - while
	// storing it would put a second statement on the resolution path, which
	// is the hot one.
	held     int64
	maxBytes int64

	// count is how many unresolved records the table holds, advanced from
	// committed transactions exactly where held is. RFC 0005 forbids
	// answering saguin_queue_depth with a count(*) per scrape: a monitoring
	// tool that scans a table every fifteen seconds is the load source the
	// catalogue exists to refuse. A queue's depth is the one number in it
	// that cannot be derived - resolution removes from the middle rather
	// than the front, so `next - floor` says nothing here.
	//
	// Named depth rather than count because count above is a prepared
	// statement, and two fields a letter apart on one struct is a defect
	// waiting for a tired reader.
	depth int64

	// backoff is the retry policy, and ready is the SQL it comes to. It is
	// built once rather than per call because Offer runs on every tick of
	// every queue, and because a fragment assembled at the call site is one
	// more thing to get wrong in the place a mistake is a wrong comparison
	// rather than an error.
	backoff store.Backoff
	ready   string

	// out is the volatile half: one entry per record currently out with a
	// worker, keyed by offset. It is bounded by the in-flight windows of the
	// live workers, which is the same number the broker uses to bound Offer.
	out map[uint64]*lease

	// epochs are minted from here rather than kept per record. A record with
	// no entry in held has no current delivery, so a Delivery ID naming it
	// matches nothing; and because the counter never repeats, one minted
	// before a release can never collide with one minted after (invariant 3).
	// The memory store keeps a per-record counter instead, which answers the
	// same question with different numbers - neither is a value any caller
	// may interpret.
	//
	// It starts again at zero after a restart, which is safe for the reason
	// the memory store's does: an epoch is only ever compared after a
	// Delivery ID has been found, and the broker's table of live deliveries
	// is empty at start, so no identifier minted before the restart is ever
	// looked up. That is also why none of this is worth storing.
	epoch uint64
}

// lease is one record out with a worker: what the database does not hold.
type lease struct {
	rec      store.Record
	attempts int
	first    time.Time
	last     time.Time

	epoch      uint64
	deliveryID string
	holder     string
	leased     bool // PUBACK seen, so the visibility deadline is running
	until      time.Time
}

func (l *lease) item(state store.State) store.Item {
	return store.Item{
		Record:     l.rec,
		State:      state,
		Attempts:   l.attempts,
		Epoch:      l.epoch,
		DeliveryID: l.deliveryID,
		Holder:     l.holder,
		LeaseUntil: l.until,
		FirstSeen:  l.first,
		LastSeen:   l.last,
	}
}

// Queue returns the store for a queue channel, creating its row the first
// time the channel is seen. Asking twice for one channel gives back the
// same store, so a channel has exactly one writer.
func (d *DB) Queue(name string) (*Queue, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if built, ok := d.channels[name]; ok {
		q, ok := built.(*Queue)
		if !ok {
			return nil, fmt.Errorf("storage %s: channel %q is already open as a %T", d.path, name, built)
		}
		return q, nil
	}

	if err := d.EnsureChannel(name, store.KindQueue); err != nil {
		return nil, err
	}
	q := &Queue{db: d, name: name, out: map[uint64]*lease{}}
	if err := d.queryRow(nil,
		`SELECT next FROM channels WHERE name = ?`, name).Scan(&q.next); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
	}
	if err := q.sum(); err != nil {
		return nil, err
	}

	stmts := []struct {
		into **sql.Stmt
		text string
	}{
		{&q.bump, `UPDATE channels SET next = ? WHERE name = ?`},
		{&q.insert, `INSERT INTO queue_items
			(channel, "offset", message_id, topic, payload, headers, ts, attempts, props)
			VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)`},
		{&q.count, `UPDATE queue_items SET attempts = ?, first_seen = ?, last_seen = ?
			 WHERE channel = ? AND "offset" = ?`},
		{&q.remove, `DELETE FROM queue_items WHERE channel = ? AND "offset" = ?`},
	}
	for _, s := range stmts {
		st, err := d.db.Prepare(s.text)
		if err != nil {
			// None of these is registered in d.channels, so nothing else will
			// ever close the ones already open.
			q.close()
			return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, name, err)
		}
		*s.into = st
	}
	q.prepareOffer()

	d.channels[name] = q
	return q, nil
}

// sum counts what the unresolved work comes to, once, when the queue opens.
//
// It reads the rows and counts them with store.RecordSize rather than
// summing in SQL, because that function is what a size bound means and it
// is deliberately one function for both stores (RFC 0004). Headers are JSON
// text in the database and RecordSize counts their keys and values, so
// `length(headers)` is a different number - which this used to use. The
// same queue then held 54 bytes while running and 67 after a restart, so it
// refused publishes at a different point on either side of one, with
// nothing anywhere saying why.
//
// Scanning is affordable here for the reason the counter is not stored at
// all: this table holds unresolved work rather than a history, and the very
// max_bytes being counted is what keeps it small. A history must not be
// scanned; a working set may be.
func (q *Queue) sum() error {
	rows, err := q.db.query(nil,
		`SELECT topic, message_id, payload, headers, props FROM queue_items WHERE channel = ?`, q.name)
	if err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	defer func() { _ = rows.Close() }()

	var held, count int64
	for rows.Next() {
		count++
		var (
			r          store.Record
			headers    sql.NullString
			properties sql.NullString
		)
		if err := rows.Scan(&r.Topic, &r.MessageID, &r.Payload, &headers, &properties); err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		if r.Headers, err = decodeHeaders(headers); err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		if err := decodeProps(properties, &r); err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		held += store.RecordSize(r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	q.held, q.depth = held, count
	return nil
}

func (q *Queue) close() {
	for _, st := range []*sql.Stmt{q.bump, q.insert, q.count, q.remove, q.offer} {
		if st != nil {
			_ = st.Close()
		}
	}
}

// Enqueue stores a record as available work.
//
// The counter and the record move in one transaction, for the same reason
// an append does: an offset assigned outside it could be taken twice.
func (q *Queue) Enqueue(r store.Record) (store.Record, error) {
	p, assigned, err := q.publishOf(r)
	if err != nil {
		return store.Record{}, err
	}
	if err := q.db.store(p); err != nil {
		return store.Record{}, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	r.Offset = *assigned
	return r, nil
}

// publishOf is the write that stores r as available work, for the commit
// group (Enqueue) or for a release (ReleaseHold), and where the offset it
// takes will be.
func (q *Queue) publishOf(r store.Record) (*publish, *uint64, error) {
	headers, err := encodeHeaders(r.Headers)
	if err != nil {
		return nil, nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	properties, err := encodeProps(r)
	if err != nil {
		return nil, nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	size := store.RecordSize(r)

	assigned := new(uint64)
	return &publish{
		channel: q,
		reserve: func(p *publish) error {
			q.mu.Lock()
			defer q.mu.Unlock()

			if q.maxBytes > 0 && q.held+size > q.maxBytes {
				return store.ErrFull
			}
			*assigned = q.next
			q.next++
			q.held += size
			q.depth++

			next := q.next
			p.offset = *assigned
			off := *assigned
			p.insert = func(tx *sql.Tx) error {
				_, err := tx.Stmt(q.insert).Exec(
					q.name, int64(off), r.MessageID, r.Topic, r.Payload, headers,
					encodeTime(r.Timestamp), properties)
				return err
			}
			p.counters = func(tx *sql.Tx) error {
				_, err := tx.Stmt(q.bump).Exec(int64(next), q.name)
				return err
			}
			return nil
		},
		settle: func(committed bool) {
			if committed {
				return
			}
			q.mu.Lock()
			q.next--
			q.held -= size
			q.depth--
			q.mu.Unlock()
		},
	}, assigned, nil
}

// SetMaxBytes bounds the unresolved work the queue holds, or removes the
// bound at zero. Called once before any listener opens.
func (q *Queue) SetMaxBytes(n int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxBytes = n
}

// SetBackoff gives the queue its retry policy and builds the clause Offer
// selects on. Called once before any listener opens, like SetMaxBytes.
//
// **The gap is a condition of the query rather than a filter over its
// results, and that is the whole reason this is here.** Offer reads a
// window of rows - `max` plus however many are already out with a worker,
// since those are held in memory and cannot be excluded by the table. A
// record still waiting sits in that window, so filtering afterwards would
// let a handful of backing-off jobs at the front of the queue hide every
// available record behind them, and the queue would go quiet with work in
// it.
//
// The arithmetic is on elapsed time rather than on a deadline -
// `now - last_seen >= wait` and never `last_seen + wait <= now` - so that
// nothing is added to a Unix nanosecond, which is already 1.8e18 and has
// room for a wait of about eight years before an int64 wraps. The shift is
// clamped by the same number as store.Backoff.Wait, because the two must
// agree. A product past an int64 - a base of nine seconds shifted 30 - is
// turned into a real by SQLite rather than wrapped, so it is a very long
// gap and never a negative one, and store.Backoff.Wait saturates at the
// longest Duration to answer the same: a record that waits no time at all
// is the defect this was built to prevent wearing the costume of the
// feature.
func (q *Queue) SetBackoff(b store.Backoff) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.backoff = b
	base := int64(b.Base)
	switch {
	case b.Kind == store.BackoffNone || base <= 0:
		q.ready = ""
	case b.Kind == store.BackoffLinear:
		q.ready = fmt.Sprintf(
			` AND (last_seen = 0 OR attempts <= 0 OR (? - last_seen) >= %d * min(attempts, %d))`,
			base, store.MaxBackoffFactor)
	default:
		q.ready = fmt.Sprintf(
			` AND (last_seen = 0 OR attempts <= 0 OR (? - last_seen) >= %d * (1 << min(attempts - 1, %d)))`,
			base, store.MaxBackoffShift)
	}
	q.prepareOffer()
}

// offerText is Offer's query for the backoff clause in force.
func (q *Queue) offerText() string {
	return `SELECT "offset", message_id, topic, payload, headers, ts, props, attempts, first_seen, last_seen
		   FROM queue_items WHERE channel = ?` + q.ready + `
		  ORDER BY "offset" LIMIT ?`
}

// prepareOffer prepares Offer's query once for the backoff clause in force,
// at open and whenever SetBackoff changes it, so an Offer does not parse its
// SQL again (DB.stmt has why). A statement that will not prepare leaves
// offer nil, and Offer runs the text, which reports what is wrong with it.
// The caller holds q.mu, or is the only holder of q.
func (q *Queue) prepareOffer() {
	if q.offer != nil {
		_ = q.offer.Close()
		q.offer = nil
	}
	if st, err := q.db.db.Prepare(q.offerText()); err == nil {
		q.offer = st
	}
}

// Bytes is what the unresolved records come to.
func (q *Queue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.held
}

// Offer marks up to max available records as being delivered and returns
// what to send, in offset order. It writes nothing: being delivered is not
// a durable state.
//
// Records already out with a worker are held in memory rather than marked
// in the table, so they cannot be excluded by the query. It reads enough
// rows to see past them - there are never more than the workers' in-flight
// windows hold - and skips them here.
func (q *Queue) Offer(max int, now time.Time) ([]store.Offered, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if max <= 0 {
		return nil, nil
	}

	args := []any{q.name}
	if q.ready != "" {
		args = append(args, encodeTime(now))
	}
	args = append(args, max+len(q.out))

	var rows *sql.Rows
	var err error
	if q.offer != nil {
		rows, err = q.offer.Query(args...)
	} else {
		rows, err = q.db.db.Query(q.offerText(), args...)
	}
	if err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	defer func() { _ = rows.Close() }()

	var out []store.Offered
	for rows.Next() && len(out) < max {
		var (
			r           store.Record
			headers     sql.NullString
			properties  sql.NullString
			ts          int64
			attempts    int
			first, last sql.NullInt64
			offset      int64
		)
		if err := rows.Scan(&offset, &r.MessageID, &r.Topic, &r.Payload, &headers,
			&ts, &properties, &attempts, &first, &last); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		r.Offset = uint64(offset)
		if _, busy := q.out[r.Offset]; busy {
			continue // already with a worker
		}
		if r.Headers, err = decodeHeaders(headers); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		if err := decodeProps(properties, &r); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		r.Timestamp = decodeTime(ts)

		q.epoch++
		l := &lease{rec: r, attempts: attempts, epoch: q.epoch, deliveryID: store.NewDeliveryID()}
		if first.Valid {
			l.first = decodeTime(first.Int64)
		}
		if last.Valid {
			l.last = decodeTime(last.Int64)
		}
		q.out[r.Offset] = l

		out = append(out, store.Offered{
			Record:     r,
			DeliveryID: l.deliveryID,
			Epoch:      l.epoch,
			Attempt:    l.attempts + 1,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	return out, nil
}

// Lease starts a record's visibility deadline and counts the attempt.
//
// It applies at the PUBACK and never at the send (invariant 7), and only
// while this delivery is the current one (invariant 3). The attempt count
// is the one thing here that has to survive a restart, so this is the
// write that Offer does not do.
func (q *Queue) Lease(h store.Held, now time.Time, visibility time.Duration) (store.Item, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	l, ok := q.out[h.Offset]
	if !ok || l.epoch != h.Epoch || l.leased {
		return store.Item{}, false, nil
	}

	attempts, first, last := l.attempts+1, l.first, now
	if first.IsZero() {
		first = now
	}
	// Through the reserve, because this is a precondition of relief rather
	// than a publish: a record whose attempt count cannot be written never
	// reaches max_attempts, so it never becomes eligible for the dead-letter
	// move that would take it out of the queue. Measured on a full provider,
	// where an 8KiB record's UPDATE is refused and the same one goes through
	// with the reserve open. RFC 0003's list does not name it and should.
	if err := q.db.reliefTx(func(tx *sql.Tx) error {
		_, err := tx.Stmt(q.count).Exec(attempts, encodeTime(first), encodeTime(last),
			q.name, int64(h.Offset))
		return err
	}); err != nil {
		// Not counted, so nothing is: the caller leaves the record with the
		// worker until its session ends, which returns it.
		return store.Item{}, false, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}

	l.attempts, l.first, l.last = attempts, first, last
	l.leased, l.holder, l.until = true, h.Holder, now.Add(visibility)
	return l.item(store.Leased), true, nil
}

// Resolve removes an acknowledged record and reports what it was. It
// applies only while this delivery is the current one, or a late
// acknowledgement would delete work another worker is holding (invariant 3).
func (q *Queue) Resolve(h store.Held) (store.Item, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	l, ok := q.out[h.Offset]
	if !ok || l.epoch != h.Epoch {
		return store.Item{}, false, nil
	}
	// Acknowledgement is the first thing RFC 0003's rule names, so it goes
	// through the reserve too. It is a DELETE and measured as working at a
	// full provider - but a DELETE can need a page of its own for the free
	// list, and "measured working once" is not the same as cannot fail.
	if err := q.db.reliefTx(func(tx *sql.Tx) error {
		_, err := tx.Stmt(q.remove).Exec(q.name, int64(h.Offset))
		return err
	}); err != nil {
		// Still work, and still held. It is redelivered when the deadline
		// passes, which is at-least-once behaving as promised.
		return store.Item{}, false, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	state := store.Delivering
	if l.leased {
		state = store.Leased
	}
	it := l.item(state)
	// See the memory store's Resolve: an answer that arrived before the
	// PUBACK counts, and this is the third site of that rule.
	if !l.leased {
		it.Attempts++
	}
	q.held -= store.RecordSize(l.rec)
	q.depth--
	delete(q.out, h.Offset)
	return it, true, nil
}

// ExpiredLeases returns every delivery whose visibility deadline has
// passed. A deadline only ever runs while a record is Leased - after the
// worker's PUBACK released its in-flight slot (invariant 7).
//
// It reads no rows. The deadlines are here in memory because they do not
// survive a restart, and this runs on the broker's ticker.
func (q *Queue) ExpiredLeases(now time.Time) ([]store.Held, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var out []store.Held
	for off, l := range q.out {
		if !l.leased || l.until.IsZero() || l.until.After(now) {
			continue
		}
		out = append(out, store.Held{Offset: off, Epoch: l.epoch, Holder: l.holder})
	}
	return out, nil
}

// ExpireOlderThan dead-letters every waiting job published before the given
// moment, and reports what went.
//
// This is the half of `job_expires_after` a worker cannot enforce: the
// broker checks a job's age as it hands it over, but a queue whose workers
// have all gone away is never asked for anything, and expiry exists for
// work nobody is processing.
//
// Records out with a worker are skipped, because removing one is removing a
// record in flight to a consumer. They are held in memory rather than
// marked in the table, so the query cannot exclude them and they are
// skipped here, exactly as Offer does.
//
// Each move is its own transaction across the two tables, so one that fails
// leaves that record where it was and does not take the ones already moved
// with it. Reading first and moving after is what keeps a cursor from being
// open while a write runs on this provider's single write connection.
func (q *Queue) ExpireOlderThan(before time.Time, dl store.DeadLetter) ([]store.Outcome, error) {
	if before.IsZero() {
		return nil, nil
	}

	// **Held before the channel's own lock, and before the transaction.**
	// This writes a channel's counter row from a copy kept in memory, so it
	// must not fall between a batch of publishes reserving their offsets and
	// the transaction that stores them - the batch would write its own
	// totals over this one's, and the byte count a channel refuses publishes
	// against is a column rather than something recomputed at the next
	// start (DB.store).
	q.db.writing.Lock()
	defer q.db.writing.Unlock()

	target, ok := dl.Log.(txAppender)
	if !ok {
		return nil, fmt.Errorf(
			"storage %s, channel %q: the dead-letter channel is a %T, which this queue "+
				"cannot write in one transaction with itself", q.db.path, q.name, dl.Log)
	}

	// `ts > 0` leaves out an unset timestamp, which is stored as 0 and would
	// otherwise be older than every deadline - expiring a queue of them
	// whole on the first sweep.
	rows, err := q.db.query(nil,
		`SELECT "offset", message_id, topic, payload, headers, ts, props, attempts, first_seen, last_seen
		   FROM queue_items WHERE channel = ? AND ts > 0 AND ts < ? ORDER BY "offset"`,
		q.name, encodeTime(before))
	if err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	defer rows.Close()
	var stale []store.Item
	for rows.Next() {
		var (
			it          store.Item
			headers     sql.NullString
			properties  sql.NullString
			ts          int64
			first, last sql.NullInt64
			offset      int64
		)
		if err := rows.Scan(&offset, &it.MessageID, &it.Topic, &it.Payload, &headers,
			&ts, &properties, &it.Attempts, &first, &last); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		if it.Headers, err = decodeHeaders(headers); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		if err := decodeProps(properties, &it.Record); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		it.Offset = uint64(offset)
		it.Timestamp = decodeTime(ts)
		if first.Valid {
			it.FirstSeen = decodeTime(first.Int64)
		}
		if last.Valid {
			it.LastSeen = decodeTime(last.Int64)
		}
		it.State = store.Available
		stale = append(stale, it)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}

	var out []store.Outcome
	for _, it := range stale {
		q.mu.Lock()
		if _, busy := q.out[it.Offset]; busy {
			q.mu.Unlock()
			continue // with a worker; it expires on its next offer instead
		}

		// **In the reserve**, as Release's move is: the move into the
		// dead-letter channel is never refused for want of capacity (RFC
		// 0003 "queue"), and a queue with no worker, which is what this
		// sweep is for, has nothing else that would free room.
		var stored store.Record
		err := q.db.reliefTx(func(tx *sql.Tx) error {
			if _, err := tx.Stmt(q.remove).Exec(q.name, int64(it.Offset)); err != nil {
				return err
			}
			var err error
			stored, err = target.appendTx(tx, dl.Record(it))
			return err
		})
		if err != nil {
			// Neither half happened, so this record is still work and the
			// next sweep tries it again. The offset the dead-letter channel
			// had taken is given back, or the next record there would leave
			// a hole a stored position reads straight past.
			if stored.Offset != 0 {
				target.rollbackAppend(stored.Offset, store.RecordSize(stored))
			}
			q.mu.Unlock()
			return out, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		q.held -= store.RecordSize(it.Record)
		q.depth--
		q.mu.Unlock()

		// Item is the record as it was and Stored is where it landed; see
		// the memory store for why they are kept apart.
		out = append(out, store.Outcome{Item: it, DeadLettered: true, Stored: stored})
	}
	return out, nil
}

// Release returns a held record to the queue, or moves it to the
// dead-letter channel when its attempts are spent.
//
// The move is one transaction across two tables, or neither half happens
// (invariant 5). A queue and its dead-letter channel share a provider by
// construction, so both tables are in this file and one commit covers them
// - which is the reason a provider is one file rather than one per channel.
//
// A move that fails leaves the record exactly as it was, delivery state and
// attempt count included, so the attempt is not spent on a failure that was
// the broker's.
func (q *Queue) Release(h store.Held, now time.Time, answered bool, maxAttempts int, dl store.DeadLetter) (store.Outcome, bool, error) {
	// **Held before the channel's own lock, and before the transaction.**
	// This writes a channel's counter row from a copy kept in memory, so it
	// must not fall between a batch of publishes reserving their offsets and
	// the transaction that stores them - the batch would write its own
	// totals over this one's, and the byte count a channel refuses publishes
	// against is a column rather than something recomputed at the next
	// start (DB.store).
	q.db.writing.Lock()
	defer q.db.writing.Unlock()

	q.mu.Lock()
	defer q.mu.Unlock()

	l, ok := q.out[h.Offset]
	if !ok || l.epoch != h.Epoch {
		return store.Outcome{}, false, nil
	}

	// The worker answered before its acknowledgement was processed, so Lease
	// has not counted the attempt yet - and neither has it stamped the two
	// delivery times, which follow the same rule and are written here for
	// the same reason. See the memory store's Release for what their absence
	// cost a dead-lettered record.
	attempts, first, last := l.attempts, l.first, l.last
	if answered && !l.leased {
		attempts++
		if first.IsZero() {
			first = now
		}
	}
	// Any answer moves last_seen, not only one that arrived before the
	// PUBACK. See the memory store's Release: the backoff measures its gap
	// from this column, and while it was stamped only in Lease it meant "when
	// the job was handed out" for every worker that acknowledges receipt
	// before doing the work - so the gap was already spent before the work
	// began. Only an answer moves it, which is what leaves a record taken
	// back by the visibility timeout with a gap that is already served.
	if answered {
		last = now
		if first.IsZero() {
			first = now
		}
	}

	if attempts < maxAttempts {
		if attempts != l.attempts || !last.Equal(l.last) {
			// The same attempt count, and through the reserve for the same
			// reason: a return that cannot be counted is a record that never
			// runs out of attempts.
			if err := q.db.reliefTx(func(tx *sql.Tx) error {
				_, err := tx.Stmt(q.count).Exec(attempts, encodeTime(first), encodeTime(last),
					q.name, int64(h.Offset))
				return err
			}); err != nil {
				return store.Outcome{}, false, fmt.Errorf("storage %s, channel %q: %w",
					q.db.path, q.name, err)
			}
			l.attempts, l.first, l.last = attempts, first, last
		}
		it := l.item(store.Available)
		it.Epoch, it.DeliveryID, it.Holder, it.LeaseUntil = 0, "", "", time.Time{}
		delete(q.out, h.Offset)
		return store.Outcome{Item: it}, true, nil
	}

	// Attempts spent: out of the queue and into the dead-letter channel, in
	// one commit. The log has to be one this queue can write inside its own
	// transaction, which a log in the same database is and a log kept
	// anywhere else is not.
	target, ok := dl.Log.(txAppender)
	if !ok {
		return store.Outcome{}, false, fmt.Errorf(
			"storage %s, channel %q: the dead-letter channel is a %T, which this queue "+
				"cannot write in one transaction with itself", q.db.path, q.name, dl.Log)
	}

	// Nothing here mutates the lease until the commit has happened, so a
	// refused move leaves the record exactly as it was - attempt count
	// included, which is what stops an attempt being spent on a failure that
	// was the broker's.
	spent := l.item(store.Leased)
	spent.Attempts = attempts
	// And the two delivery times, for the same reason the count is carried:
	// a worker that answered from Delivering has them stamped above and
	// l.item still holds the lease's zero values.
	spent.FirstSeen, spent.LastSeen = first, last

	var stored store.Record
	err := q.db.reliefTx(func(tx *sql.Tx) error {
		if _, err := tx.Stmt(q.remove).Exec(q.name, int64(h.Offset)); err != nil {
			return err
		}
		var err error
		stored, err = target.appendTx(tx, dl.Record(spent))
		return err
	})
	if err != nil {
		// Neither half happened, and the record is still held, still with the
		// attempt count it had. This is tried again when the deadline passes.
		//
		// The dead-letter channel took an offset for a row that was never
		// written, so it is given back: otherwise the next record there would
		// leave a hole, and a stored consumer position could point past it.
		if stored.Offset != 0 {
			target.rollbackAppend(stored.Offset, store.RecordSize(stored))
		}
		return store.Outcome{}, false, fmt.Errorf("storage %s, channel %q: %w",
			q.db.path, q.name, err)
	}

	q.held -= store.RecordSize(l.rec)
	q.depth--
	delete(q.out, h.Offset)
	return store.Outcome{Item: spent, DeadLettered: true, Stored: stored}, true, nil
}

// Next returns the offset the next enqueued record will take.
func (q *Queue) Next() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.next
}

// Depth reports how many items remain, and how many are out with a worker.
// It is what saguin_queue_depth and saguin_queue_inflight are (RFC 0005).
//
// Both are fields rather than queries, which is that document's rule for
// the whole catalogue: a metric is a number the broker already holds or it
// does not ship, and a scraper running a count(*) every fifteen seconds is
// the load source it exists to refuse. The depth is advanced from
// committed transactions the way the byte total is; the in-flight half is
// the size of the map that already tracks every delivery, so it costs
// nothing at all.
func (q *Queue) Depth() (total, inflight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int(q.depth), len(q.out)
}

// Unresolved is the work this queue is holding, oldest first, capped at
// limit - and how many there are in total.
//
// **It leases nothing**, which is the whole reason it exists. The only
// other way to see a queue's contents is to consume them, and a viewer that
// consumed would take work from the workers it was sent to diagnose.
//
// **The payload is not selected.** Reading records is MQTT's job, so it
// never leaves the database - left in the query and stripped afterwards, a
// second caller could reintroduce it by forgetting, and the row would have
// crossed the driver either way.
//
// **The total is the depth field rather than a count**, which is the number
// the store already advances from committed transactions. Rows come from
// the table because a person asked; the total costs nothing because it was
// already there.
//
// Who holds a record and until when live in memory rather than in the
// table - a restart resurrects no delivery (invariant 15) - so they are
// read from the lease map beside the rows.
func (q *Queue) Unresolved(limit int) ([]store.Item, int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	rows, err := q.db.query(nil,
		`SELECT "offset", message_id, topic, ts, attempts, first_seen, last_seen
		   FROM queue_items WHERE channel = ? ORDER BY "offset" LIMIT ?`,
		q.name, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
	}
	defer rows.Close()

	var out []store.Item
	for rows.Next() {
		var (
			it          store.Item
			ts          int64
			first, last sql.NullInt64
			offset      int64
		)
		if err := rows.Scan(&offset, &it.MessageID, &it.Topic, &ts, &it.Attempts,
			&first, &last); err != nil {
			return nil, 0, fmt.Errorf("storage %s, channel %q: %w", q.db.path, q.name, err)
		}
		it.Offset = uint64(offset)
		it.Timestamp = decodeTime(ts)
		if first.Valid {
			it.FirstSeen = decodeTime(first.Int64)
		}
		if last.Valid {
			it.LastSeen = decodeTime(last.Int64)
		}
		it.State = store.Available
		if l, busy := q.out[it.Offset]; busy {
			it.Holder, it.LeaseUntil = l.holder, l.until
			it.State = store.Delivering
			if l.leased {
				it.State = store.Leased
			}
		}
		out = append(out, it)
	}
	return out, int(q.depth), rows.Err()
}
