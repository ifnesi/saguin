package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// Sessions holds persistent sessions and their in-flight tables in the
// provider's own database, where they survive a crash as the channels on it
// do. It answers exactly what the memory store answers; the two are driven
// through one set of oracles.
//
// **The counts are held in memory**, loaded once when the store is built and
// then advanced only by statements that succeeded, so the gauges never cost a
// scan (RFC 0005). The file's page count, not a byte tally, is what bounds
// the store: a write past it fails inside its transaction as ErrFull and
// leaves nothing behind.
type Sessions struct {
	db *DB

	mu       sync.Mutex
	sessions int
	bytes    int64
	// holders counts the sessions holding each shared group's filter, so a
	// session's ending knows which groups' cursors end with it (drop).
	holders store.ShareHolders
	// endedAtOpen is the groups no session held, ended as the store was
	// built (EndedAtOpen).
	endedAtOpen map[string]store.ShareGroupState
}

// Sessions returns the store, building it the first time it is asked for.
// Asking twice gives back the same one, so it has exactly one writer.
func (d *DB) Sessions() (*Sessions, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessions != nil {
		return d.sessions, nil
	}
	s := &Sessions{db: d, holders: store.ShareHolders{}}
	all, err := s.all()
	if err != nil {
		return nil, err
	}
	for _, sess := range all {
		s.sessions++
		s.bytes += store.SessionSize(sess)
		s.holders.Add(sess)
	}
	inflight, err := s.checkInFlight()
	if err != nil {
		return nil, err
	}
	s.bytes += inflight
	if err := s.endOrphanedCursors(); err != nil {
		return nil, err
	}
	var cursors int64
	if err := d.queryRow(nil, `SELECT coalesce((SELECT sum(length(CAST(share_group AS BLOB)) + 8) FROM share_groups), 0)
		+ coalesce((SELECT sum(length(CAST(share_group AS BLOB)) + 8) FROM share_returned), 0)`).Scan(&cursors); err != nil {
		return nil, s.fail(err)
	}
	s.bytes += cursors
	d.sessions = s
	return s, nil
}

// checkInFlight holds the in-flight tables to what can be true of them, as the
// store is built, and answers what the entries they hold count against the
// provider (store.InFlightBytes).
//
// **Refused rather than started on**, each naming what it found: rows with
// no broadcast log in the file, which name offsets in a log that would start
// again at 1 (invariant 9); a row at or past the log's next offset, a message
// it never had; and a table larger than the window it was written under. The
// file's CHECK holds every row to store.ValidInFlight already.
//
// **A row behind its session's cursor is let go**: the cursor and the table
// are written in separate transactions, so a crash between an
// acknowledgement moving the cursor and its row going leaves one, and
// re-sending it would deliver the message twice. A row a shared group handed
// its session is behind the group's cursor, not the session's, and stays
// (store.InFlight.Group).
func (s *Sessions) checkInFlight() (int64, error) {
	var rows int64
	if err := s.db.queryRow(nil, `SELECT count(*) FROM session_inflight`).Scan(&rows); err != nil {
		return 0, s.fail(err)
	}
	if rows == 0 {
		return 0, nil
	}
	var next int64
	switch err := s.db.queryRow(nil, `SELECT next FROM channels WHERE name = ?`, store.BroadcastLog).Scan(&next); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, s.fail(fmt.Errorf("%d messages are in flight on a broadcast log the file does not hold", rows))
	case err != nil:
		return 0, s.fail(err)
	}
	var (
		client string
		offset int64
	)
	switch err := s.db.queryRow(nil, `SELECT client, "offset" FROM session_inflight WHERE "offset" >= ? LIMIT 1`,
		next).Scan(&client, &offset); {
	case err == nil:
		return 0, s.fail(fmt.Errorf("client %q has offset %d in flight, and the broadcast log's next is %d",
			client, offset, next))
	case !errors.Is(err, sql.ErrNoRows):
		return 0, s.fail(err)
	}
	var held, window int64
	switch err := s.db.queryRow(nil, `SELECT s.client, count(*), s.receive_maximum
		FROM session_inflight w JOIN sessions s ON s.client = w.client
		GROUP BY s.client HAVING count(*) > s.receive_maximum LIMIT 1`).Scan(&client, &held, &window); {
	case err == nil:
		return 0, s.fail(fmt.Errorf("client %q: %d messages in flight under a window of %d: %w",
			client, held, window, store.ErrWindowFull))
	case !errors.Is(err, sql.ErrNoRows):
		return 0, s.fail(err)
	}

	var gone int64
	err := s.db.tx(func(tx *sql.Tx) error {
		res, err := s.db.exec(tx, `DELETE FROM session_inflight WHERE share_group = '' AND EXISTS (
			SELECT 1 FROM positions p
			 WHERE p.channel = ? AND p.reader = ? || session_inflight.client
			   AND session_inflight."offset" < p."offset")`, store.BroadcastLog, store.ReaderPrefixMQTT)
		if err != nil {
			return err
		}
		gone, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, err
	}
	var groups int64
	if err := s.db.queryRow(nil, `SELECT coalesce(sum(length(CAST(share_group AS BLOB))), 0)
		FROM session_inflight`).Scan(&groups); err != nil {
		return 0, s.fail(err)
	}
	return (rows-gone)*store.InFlightSize + groups, nil
}

// SetInFlight keeps one entry of a session's in-flight table under its packet
// identifier, and records the window it was written under - the client's
// Receive Maximum - on the session's row, in one transaction.
// store.ErrNoSession where the client has no session, store.ErrWindowFull
// where the table would outgrow that window, store.ErrPastNext for an entry
// at or past the broadcast log's next offset, and an invalid entry is
// refused; in every case the table stays as it was. See
// store.Sessions.SetInFlight.
func (s *Sessions) SetInFlight(client string, window uint16, f store.InFlight) error {
	return s.SetInFlightAll(client, window, []store.InFlight{f})
}

// SetInFlightAll keeps several entries of a session's in-flight table, and the
// window, in one transaction: all of them or none. See
// store.Sessions.SetInFlightAll.
func (s *Sessions) SetInFlightAll(client string, window uint16, fs []store.InFlight) error {
	if len(fs) == 0 {
		return nil
	}
	if err := store.ValidInFlightAll(fs); err != nil {
		return err
	}
	// The log's next, read before anything is written and as a seek reads it
	// (Log.SavePosition). It only grows, so an entry for a message already
	// read from the log is never below it.
	next, err := s.logNext()
	if err != nil {
		return err
	}
	for _, f := range fs {
		if f.Offset >= next {
			return store.PastNext(f, next)
		}
	}
	var grew int64
	return s.db.joinWrite(&write{s: s, client: client,
		check: func(tx *sql.Tx) error {
			var err error
			grew, err = inFlightFits(s.db, tx, client, window, fs)
			return err
		},
		run:   func(tx *sql.Tx) error { return setInFlight(s.db, tx, client, window, fs) },
		apply: s.grow(&grew),
	})
}

// grow is a write's apply for one that adds *by to the provider's count, as
// run left it.
func (s *Sessions) grow(by *int64) func() func() {
	return func() func() {
		n := *by
		s.bytes += n
		return func() { s.bytes -= n }
	}
}

// inFlightFits is SetInFlightAll's check, inside tx: store.ErrNoSession where
// the client has no session and store.ErrWindowFull where the table would
// outgrow window, and otherwise what the entries add to the provider's count
// (store.InFlightBytes): new entries, and an entry moved on under another
// group's name. It only reads.
func inFlightFits(d *DB, tx *sql.Tx, client string, window uint16, fs []store.InFlight) (int64, error) {
	if err := sessionHeld(d, tx, client); err != nil {
		return 0, err
	}
	rows, err := d.query(tx, `SELECT packet_id, length(CAST(share_group AS BLOB)) FROM session_inflight
		WHERE client = ?`, client)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	held := map[uint16]int64{}
	for rows.Next() {
		var id, group int64
		if err := rows.Scan(&id, &group); err != nil {
			rows.Close()
			return 0, err
		}
		held[uint16(id)] = store.InFlightSize + group
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	fresh := 0
	var grew int64
	for _, f := range fs {
		old, had := held[f.PacketID]
		if !had {
			fresh++
		}
		grew += store.InFlightBytes(f) - old
	}
	if len(held)+fresh > int(window) {
		return 0, store.ErrWindowFull
	}
	return grew, nil
}

// setInFlight is SetInFlightAll's writes, inside tx, once inFlightFits has
// said they fit.
func setInFlight(d *DB, tx *sql.Tx, client string, window uint16, fs []store.InFlight) error {
	for _, f := range fs {
		if _, err := d.exec(tx, `INSERT INTO session_inflight (client, packet_id, "offset", qos, state, share_group)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (client, packet_id) DO UPDATE SET
				"offset" = excluded."offset", qos = excluded.qos, state = excluded.state,
				share_group = excluded.share_group`,
			client, int64(f.PacketID), int64(f.Offset), int64(f.QoS), int64(f.State), f.Group); err != nil {
			return err
		}
	}
	_, err := d.exec(tx, `UPDATE sessions SET receive_maximum = ? WHERE client = ?`, int64(window), client)
	return err
}

// logNext is the provider's broadcast log's next offset.
func (s *Sessions) logNext() (uint64, error) {
	lg, err := s.Log()
	if err != nil {
		return 0, err
	}
	return lg.Next(), nil
}

// sessionHeld is store.ErrNoSession where the client has no session row.
func sessionHeld(d *DB, tx *sql.Tx, client string) error {
	var one int
	switch err := d.queryRow(tx, `SELECT 1 FROM sessions WHERE client = ?`, client).Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return store.ErrNoSession
	default:
		return err
	}
}

// acknowledgeChunk is how many entries one of Acknowledge's statements
// clears: two parameters each, well inside SQLite's limit on a statement's.
const acknowledgeChunk = 500

// The statements Acknowledge clears a chunk with, each by its identifier and
// its offset, holding 1, 10, 100 and acknowledgeChunk pairs. **Constants,
// built from constants**, so that what they do is read from the source
// rather than assembled at run time
// (TestEveryTransactionWritingAChannelsCountersHoldsTheWriteLock): a chunk
// takes the smallest that holds it (acknowledgeSlots) and is padded with its
// last pair, which an IN list matches, and deletes, once.
//
// **Four sizes, because SQLite parses and binds the whole statement however
// few pairs are real.** One 500-pair statement for every chunk cost an
// acknowledgement of one entry 782-810us against 87-89us with its own size,
// and took a sqlite broadcast to ten durable subscribers from 1,468/s to
// 1,036/s published, because an idle drain flushes a handful at a time on
// the connection the publisher's write needs. A prepared single-pair
// statement run once an entry matched this at 1 and 10 entries and was
// 1.7-1.8x slower at 100 and 500, which is a busy fleet's flush.
const (
	acknowledgeStatement1   = ackDelete + "(?, ?)" + ackReturning
	acknowledgeStatement10  = ackDelete + ackPairs10 + ackReturning
	acknowledgeStatement100 = ackDelete + ackPairs100 + ackReturning
	acknowledgeStatement    = ackDelete + ackPairs500 + ackReturning

	ackDelete    = `DELETE FROM session_inflight WHERE client = ? AND (packet_id, "offset") IN (VALUES `
	ackReturning = `) RETURNING length(CAST(share_group AS BLOB))`

	ackPairs10  = "(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)"
	ackPairs100 = ackPairs10 + ", " + ackPairs10 + ", " + ackPairs10 + ", " + ackPairs10 + ", " + ackPairs10 +
		", " + ackPairs10 + ", " + ackPairs10 + ", " + ackPairs10 + ", " + ackPairs10 + ", " + ackPairs10
	ackPairs500 = ackPairs100 + ", " + ackPairs100 + ", " + ackPairs100 + ", " + ackPairs100 + ", " + ackPairs100
)

// acknowledgeSlots is how many pairs the statement clearing a chunk of n
// entries holds: the smallest of the four that n fits.
func acknowledgeSlots(n int) int {
	for _, slots := range []int{1, 10, 100} {
		if n <= slots {
			return slots
		}
	}
	return acknowledgeChunk
}

// Acknowledge clears the entries given, each only where the table holds it
// under that identifier at that offset, and saves the session's cursor, in
// one transaction: a crash keeps both or neither. It reports how many entries
// went, and a cursor past the log's next offset is store.ErrPastNext with
// nothing written. See store.Sessions.Acknowledge.
//
// Through the reserve, as a cursor's SavePosition is: an acknowledgement
// frees what it clears and writes one row, and a bound never refuses the
// operation that relieves it.
func (s *Sessions) Acknowledge(client string, cursor uint64, done []store.InFlight) (int, error) {
	next, err := s.logNext()
	if err != nil {
		return 0, err
	}
	if cursor > next {
		return 0, fmt.Errorf("storage %s, sessions: %q's cursor at %d, and the broadcast log's next is %d: %w",
			s.db.path, client, cursor, next, store.ErrPastNext)
	}
	var n, freed int64
	err = s.db.joinWrite(&write{s: s, client: client, relief: true,
		check: func(tx *sql.Tx) error { return sessionHeld(s.db, tx, client) },
		run: func(tx *sql.Tx) error {
			n, freed = 0, 0
			// **One statement for all of them, each still matched on its
			// identifier and its offset.** A statement per entry cost a busy
			// sqlite drain a quarter of its delivery rate: 24.6k deliveries a
			// second against 30.4k with this, 500 sessions in the fleet shape.
			// The pair is what guards an identifier since reused for another
			// offset, which an offset range alone would not.
			// In chunks, since a statement takes a bounded number of parameters and
			// what a failed write puts back is written with the next.
			for rest := done; len(rest) > 0; {
				chunk := rest[:min(len(rest), acknowledgeChunk)]
				rest = rest[len(chunk):]
				slots := acknowledgeSlots(len(chunk))
				args := make([]any, 0, 1+2*slots)
				args = append(args, client)
				for i := range slots {
					f := chunk[min(i, len(chunk)-1)]
					args = append(args, int64(f.PacketID), int64(f.Offset))
				}
				// One function a chunk, so its result set is closed when the chunk
				// ends - not held open by a defer that waits for the whole loop - and
				// the close's error is read.
				err := func() error {
					// Each named and constant, so the write-lock check reads every
					// one of them and the statement cache prepares each once.
					var (
						rows *sql.Rows
						err  error
					)
					switch slots {
					case 1:
						rows, err = s.db.query(tx, acknowledgeStatement1, args...)
					case 10:
						rows, err = s.db.query(tx, acknowledgeStatement10, args...)
					case 100:
						rows, err = s.db.query(tx, acknowledgeStatement100, args...)
					default:
						rows, err = s.db.query(tx, acknowledgeStatement, args...)
					}
					if err != nil {
						return err
					}
					defer rows.Close()
					for rows.Next() {
						var group int64
						if err := rows.Scan(&group); err != nil {
							return err
						}
						n++
						freed += store.InFlightSize + group
					}
					if err := rows.Close(); err != nil {
						return err
					}
					return rows.Err()
				}()
				if err != nil {
					return err
				}
			}
			_, err := s.db.exec(tx,
				`INSERT INTO positions (channel, reader, "offset", last_seen, expires_in)
				 VALUES (?, ?, ?, ?, 0)
				 ON CONFLICT (channel, reader) DO UPDATE SET
				     "offset" = excluded."offset",
				     last_seen = excluded.last_seen`,
				store.BroadcastLog, store.MQTTReader(client), int64(cursor), encodeTime(time.Now()))
			return err
		},
		apply: func() func() {
			f := freed
			s.bytes -= f
			return func() { s.bytes += f }
		},
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// ClearInFlight takes one entry out of a session's in-flight table, which is
// what an acknowledgement does, and reports whether it was there.
func (s *Sessions) ClearInFlight(client string, packetID uint16) (bool, error) {
	var (
		group int64
		had   bool
	)
	err := s.db.joinWrite(&write{s: s, client: client,
		run: func(tx *sql.Tx) error {
			switch err := s.db.queryRow(tx, `DELETE FROM session_inflight WHERE client = ? AND packet_id = ?
			RETURNING length(CAST(share_group AS BLOB))`, client, int64(packetID)).Scan(&group); {
			case errors.Is(err, sql.ErrNoRows):
				had = false
				return nil
			case err != nil:
				return err
			}
			had = true
			return nil
		},
		apply: func() func() {
			if !had {
				return func() {}
			}
			n := store.InFlightSize + group
			s.bytes -= n
			return func() { s.bytes += n }
		},
	})
	if err != nil || !had {
		return false, err
	}
	return true, nil
}

// InFlight is a session's in-flight table, ordered by offset, and the window
// it was written under. A session with none, or no session, answers an empty
// table.
func (s *Sessions) InFlight(client string) (uint16, []store.InFlight, error) {
	window, table, err := inFlightOf(s.db, nil, client)
	if err != nil {
		return 0, nil, s.fail(err)
	}
	return window, table, nil
}

// inFlightOf is InFlight inside tx, or on its own where tx is nil.
func inFlightOf(d *DB, tx *sql.Tx, client string) (uint16, []store.InFlight, error) {
	var window int64
	switch err := d.queryRow(tx, `SELECT receive_maximum FROM sessions WHERE client = ?`, client).Scan(&window); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil, nil
	case err != nil:
		return 0, nil, err
	}
	rows, err := d.query(tx, `SELECT "offset", packet_id, qos, state, share_group FROM session_inflight
		WHERE client = ? ORDER BY "offset"`, client)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var out []store.InFlight
	for rows.Next() {
		var (
			f                 store.InFlight
			id, qos, st, offs int64
		)
		if err := rows.Scan(&offs, &id, &qos, &st, &f.Group); err != nil {
			return 0, nil, err
		}
		f.Offset, f.PacketID, f.QoS, f.State = uint64(offs), uint16(id), byte(qos), store.MessageState(st)
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return uint16(window), out, nil
}

// endOrphanedCursors ends, as the store is built, the cursor and returned
// list of every group no session here holds, and a returned list whose group
// has no cursor, and keeps them for EndedAtOpen. See store.RestoreSessions
// for why.
func (s *Sessions) endOrphanedCursors() error {
	cursors, err := s.ShareCursors()
	if err != nil {
		return err
	}
	returned, err := s.ShareReturned()
	if err != nil {
		return err
	}
	orphans := map[string]store.ShareGroupState{}
	for group, cursor := range cursors {
		if s.holders[group] <= 0 {
			orphans[group] = store.ShareGroupState{Cursor: cursor, Returned: returned[group]}
		}
	}
	for group, offs := range returned {
		if _, has := cursors[group]; !has {
			orphans[group] = store.ShareGroupState{Returned: offs}
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	err = s.db.reliefTx(func(tx *sql.Tx) error {
		for group := range orphans {
			if _, _, err := endGroup(s.db, tx, group); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.endedAtOpen = orphans
	return nil
}

// endGroup takes a group's cursor and its returned list out inside tx, and
// answers the room they gave back and whether the group had a cursor.
func endGroup(d *DB, tx *sql.Tx, group string) (int64, bool, error) {
	res, err := d.exec(tx, `DELETE FROM share_groups WHERE share_group = ?`, group)
	if err != nil {
		return 0, false, err
	}
	had, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	res, err = d.exec(tx, `DELETE FROM share_returned WHERE share_group = ?`, group)
	if err != nil {
		return 0, false, err
	}
	back, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	return had*store.ShareCursorSize(group) + back*store.ShareReturnedSize(group), had > 0, nil
}

// forget takes offsets off a group's returned list inside tx, and answers the
// room it gave back.
func forget(d *DB, tx *sql.Tx, group string, offsets []uint64) (int64, error) {
	var n int64
	for _, off := range offsets {
		res, err := d.exec(tx, `DELETE FROM share_returned WHERE share_group = ? AND "offset" = ?`, group, int64(off))
		if err != nil {
			return 0, err
		}
		gone, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		n += gone
	}
	return n * store.ShareReturnedSize(group), nil
}

// EndedAtOpen is the groups the store ended as it was built, by the group's
// filter. See store.Sessions.EndedAtOpen.
func (s *Sessions) EndedAtOpen() map[string]store.ShareGroupState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.endedAtOpen)
}

// ShareReturned is every shared group's returned list, by the group's
// filter, each in offset order.
//
// **A session's record, every record, and the shared groups' returned
// deliveries are read on the read pool** (Get, All, ShareReturned), beside
// the writer rather than queued behind it. Each read is a fresh read
// transaction, so it sees every commit before it - a caller's own write,
// which returned only once committed, included - and none of them decides a
// write: the reads that do (save's, drop's, Disconnected's) stay on the
// write connection. Where the provider has no pool - read_connections: 0, or
// a database opened for export - each runs on the write connection, as it
// always did.
func (s *Sessions) ShareReturned() (map[string][]uint64, error) {
	var (
		out map[string][]uint64
		err error
	)
	if s.db.reads == nil {
		var rows *sql.Rows
		if rows, err = s.db.query(nil, shareReturnedText); err == nil {
			out, err = scanShareReturned(rows)
		}
	} else {
		err = s.db.readTx(func(tx *sql.Tx, stmt func(string) *sql.Stmt) error {
			rows, err := stmt(shareReturnedText).Query()
			if err != nil {
				return err
			}
			out, err = scanShareReturned(rows)
			return err
		})
	}
	if err != nil {
		return nil, s.fail(err)
	}
	return out, nil
}

const shareReturnedText = `SELECT share_group, "offset" FROM share_returned ORDER BY share_group, "offset"`

func scanShareReturned(rows *sql.Rows) (map[string][]uint64, error) {
	defer rows.Close()
	out := map[string][]uint64{}
	for rows.Next() {
		var (
			group string
			off   int64
		)
		if err := rows.Scan(&group, &off); err != nil {
			return nil, err
		}
		out[group] = append(out[group], uint64(off))
	}
	return out, rows.Err()
}

// HandOver keeps entries a shared group hands one of its members in that
// session's in-flight table, and moves the group's cursor, in one
// transaction: both or neither, and store.ErrNoShareGroup where the group
// has no cursor. See store.Sessions.HandOver.
func (s *Sessions) HandOver(group string, cursor uint64, client string, window uint16, fs []store.InFlight) error {
	if err := store.ValidHandOver(group, fs); err != nil {
		return err
	}
	next, err := s.logNext()
	if err != nil {
		return err
	}
	for _, f := range fs {
		if f.Offset >= next {
			return store.PastNext(f, next)
		}
	}
	if err := shareCursorFits(cursor, next); err != nil {
		return err
	}
	var grew int64
	return s.db.joinWrite(&write{s: s, client: client,
		// The group's cursor is looked for before anything is written, as
		// Return looks for it: a group with none refuses this hand-over alone
		// rather than ending, by an UPDATE that found no row after the table
		// was written, the transaction every write beside it shares.
		check: func(tx *sql.Tx) error {
			var err error
			if grew, err = inFlightFits(s.db, tx, client, window, fs); err != nil {
				return err
			}
			return shareGroupHeld(s.db, tx, group)
		},
		run: func(tx *sql.Tx) error {
			if err := setInFlight(s.db, tx, client, window, fs); err != nil {
				return err
			}
			if err := moveShareCursor(s.db, tx, group, cursor); err != nil {
				return err
			}
			offs := make([]uint64, len(fs))
			for i, f := range fs {
				offs[i] = f.Offset
			}
			freed, err := forget(s.db, tx, group, offs)
			grew -= freed
			return err
		},
		apply: s.grow(&grew),
	})
}

// Return undoes a hand-over whose delivery never reached its member: its
// in-flight rows go, and its group's returned rows are written, in one
// transaction through the reserve, since it frees more than it writes.
// store.ErrNoShareGroup where the group has no cursor, and nothing is
// written. See store.Sessions.Return.
func (s *Sessions) Return(client, group string, fs []store.InFlight) error {
	if err := store.ValidHandOver(group, fs); err != nil {
		return err
	}
	var grew int64
	return s.db.joinWrite(&write{s: s, client: client, relief: true,
		check: func(tx *sql.Tx) error {
			if err := sessionHeld(s.db, tx, client); err != nil {
				return err
			}
			return shareGroupHeld(s.db, tx, group)
		},
		run: func(tx *sql.Tx) error {
			grew = 0
			for _, f := range fs {
				var g int64
				switch err := s.db.queryRow(tx, `DELETE FROM session_inflight WHERE client = ? AND packet_id = ? AND "offset" = ?
				RETURNING length(CAST(share_group AS BLOB))`, client, int64(f.PacketID), int64(f.Offset)).Scan(&g); {
				case errors.Is(err, sql.ErrNoRows):
				case err != nil:
					return err
				default:
					grew -= store.InFlightSize + g
				}
			}
			added, err := returnRows(s.db, tx, group, offsetsOfEntries(fs))
			grew += added
			return err
		},
		apply: s.grow(&grew),
	})
}

// shareGroupHeld is store.ErrNoShareGroup where group has no cursor.
func shareGroupHeld(d *DB, tx *sql.Tx, group string) error {
	var one int
	switch err := d.queryRow(tx, `SELECT 1 FROM share_groups WHERE share_group = ?`, group).Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return store.ErrNoShareGroup
	default:
		return err
	}
}

// Lend is a hand-over to a member whose session the store does not keep: the
// offsets are written as the group's returned rows and its cursor moved, in
// one transaction. store.ErrNoShareGroup where the group has no cursor. See
// store.Sessions.Lend.
func (s *Sessions) Lend(group string, cursor uint64, offsets []uint64) error {
	if err := store.ValidShareGroup(group); err != nil {
		return err
	}
	next, err := s.logNext()
	if err != nil {
		return err
	}
	if err := shareCursorFits(cursor, next); err != nil {
		return err
	}
	for _, off := range offsets {
		if off == 0 || off >= next {
			return fmt.Errorf("a shared group lends offset %d, and the broadcast log's next is %d: %w",
				off, next, store.ErrPastNext)
		}
	}
	var added int64
	return s.db.joinWrite(&write{s: s,
		check: func(tx *sql.Tx) error { return shareGroupHeld(s.db, tx, group) },
		run: func(tx *sql.Tx) error {
			if err := moveShareCursor(s.db, tx, group, cursor); err != nil {
				return err
			}
			var err error
			added, err = returnRows(s.db, tx, group, offsets)
			return err
		},
		apply: s.grow(&added),
	})
}

// returnRows writes offsets as a group's returned rows inside tx, and answers
// what the new ones count.
func returnRows(d *DB, tx *sql.Tx, group string, offsets []uint64) (int64, error) {
	var n int64
	for _, off := range offsets {
		res, err := d.exec(tx, `INSERT INTO share_returned (share_group, "offset") VALUES (?, ?)
			ON CONFLICT DO NOTHING`, group, int64(off))
		if err != nil {
			return 0, err
		}
		added, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		n += added
	}
	return n * store.ShareReturnedSize(group), nil
}

func offsetsOfEntries(fs []store.InFlight) []uint64 {
	out := make([]uint64, len(fs))
	for i, f := range fs {
		out[i] = f.Offset
	}
	return out
}

// EndShareCursorIfUnheld ends a shared group's cursor and its returned rows
// where no session here holds the group's filter, and reports whether it did.
// See store.Sessions.EndShareCursorIfUnheld.
func (s *Sessions) EndShareCursorIfUnheld(group string) (bool, error) {
	var (
		held  bool
		freed int64
		had   bool
	)
	// answers: the UNSUBACK that took the group's last member waits on it.
	err := s.db.joinWrite(&write{s: s, relief: true, answers: true,
		// Read under the store's lock, which the group's leader holds: the
		// holders as every write before this one in the transaction left
		// them.
		check: func(*sql.Tx) error {
			held = s.holders[group] > 0
			return nil
		},
		run: func(tx *sql.Tx) error {
			freed, had = 0, false
			if held {
				return nil
			}
			var err error
			freed, had, err = endGroup(s.db, tx, group)
			return err
		},
		apply: func() func() {
			f := freed
			s.bytes -= f
			return func() { s.bytes += f }
		},
	})
	if err != nil {
		return false, err
	}
	return had, nil
}

// shareCursorFits refuses a group's cursor that cannot point into the log,
// as store.Sessions refuses it.
func shareCursorFits(cursor, next uint64) error {
	if cursor == 0 {
		return fmt.Errorf("a shared group's cursor at offset 0, before the log's first")
	}
	if cursor > next {
		return fmt.Errorf("a shared group's cursor at %d, and the broadcast log's next is %d: %w",
			cursor, next, store.ErrPastNext)
	}
	return nil
}

// moveShareCursor moves a group's cursor inside tx, and is
// store.ErrNoShareGroup where the group has none.
func moveShareCursor(d *DB, tx *sql.Tx, group string, cursor uint64) error {
	res, err := d.exec(tx, `UPDATE share_groups SET "cursor" = ? WHERE share_group = ?`, int64(cursor), group)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return store.ErrNoShareGroup
	}
	return nil
}

// CreateShareCursor gives a shared group a cursor on the broadcast log, and
// leaves one it has where it is; store.ErrNoShareGroup where no session here
// holds the group's filter. See store.Sessions.CreateShareCursor.
func (s *Sessions) CreateShareCursor(group string, cursor uint64) error {
	if err := store.ValidShareGroup(group); err != nil {
		return err
	}
	next, err := s.logNext()
	if err != nil {
		return err
	}
	if err := shareCursorFits(cursor, next); err != nil {
		return err
	}
	var made int64
	// answers: a returning member's CONNACK waits on it (repairGroups).
	return s.db.joinWrite(&write{s: s, answers: true,
		check: func(*sql.Tx) error {
			if s.holders[group] <= 0 {
				return store.ErrNoShareGroup
			}
			return nil
		},
		run: func(tx *sql.Tx) error {
			res, err := s.db.exec(tx, `INSERT INTO share_groups (share_group, "cursor") VALUES (?, ?)
			ON CONFLICT (share_group) DO NOTHING`, group, int64(cursor))
			if err != nil {
				return err
			}
			made, err = res.RowsAffected()
			return err
		},
		apply: func() func() {
			if made == 0 {
				return func() {}
			}
			n := store.ShareCursorSize(group)
			s.bytes += n
			return func() { s.bytes -= n }
		},
	})
}

// SetShareCursor moves a shared group's cursor on the broadcast log, and takes
// forget off its returned list, in one transaction through the reserve as an
// acknowledgement is, so the bound never refuses a group letting go of what
// it passed; store.ErrNoShareGroup where the group has none. See
// store.Sessions.SetShareCursor.
func (s *Sessions) SetShareCursor(group string, cursor uint64, forgotten ...uint64) error {
	next, err := s.logNext()
	if err != nil {
		return err
	}
	if err := shareCursorFits(cursor, next); err != nil {
		return err
	}
	var freed int64
	return s.db.joinWrite(&write{s: s, relief: true,
		check: func(tx *sql.Tx) error { return shareGroupHeld(s.db, tx, group) },
		run: func(tx *sql.Tx) error {
			if err := moveShareCursor(s.db, tx, group, cursor); err != nil {
				return err
			}
			var err error
			freed, err = forget(s.db, tx, group, forgotten)
			return err
		},
		apply: func() func() {
			f := freed
			s.bytes -= f
			return func() { s.bytes += f }
		},
	})
}

// DropShareCursor ends a shared group's cursor and reports whether it had
// one. See store.Sessions.DropShareCursor.
func (s *Sessions) DropShareCursor(group string) (bool, error) {
	var (
		freed int64
		had   bool
	)
	err := s.db.joinWrite(&write{s: s, relief: true,
		run: func(tx *sql.Tx) error {
			var err error
			freed, had, err = endGroup(s.db, tx, group)
			return err
		},
		apply: func() func() {
			f := freed
			s.bytes -= f
			return func() { s.bytes += f }
		},
	})
	if err != nil {
		return false, err
	}
	return had, nil
}

// ShareCursors is every shared group's cursor, by the group's filter.
func (s *Sessions) ShareCursors() (map[string]uint64, error) {
	rows, err := s.db.query(nil, `SELECT share_group, "cursor" FROM share_groups`)
	if err != nil {
		return nil, s.fail(err)
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var (
			group  string
			cursor int64
		)
		if err := rows.Scan(&group, &cursor); err != nil {
			return nil, s.fail(err)
		}
		out[group] = uint64(cursor)
	}
	if err := rows.Err(); err != nil {
		return nil, s.fail(err)
	}
	return out, nil
}

// Log is the provider's broadcast log (DB.Broadcast), whose reader positions
// are the sessions' cursors. It is the session store's to hand out because it
// is session state in the same provider: a session's ending takes its cursor
// in the same transaction as its record (drop).
func (s *Sessions) Log() (*Log, error) { return s.db.Broadcast() }

func (s *Sessions) fail(err error) error {
	return fmt.Errorf("storage %s, sessions: %w", s.db.path, err)
}

// Save keeps a session, replacing what was kept under its client id. Its
// messages are untouched.
func (s *Sessions) Save(sess store.Session) error {
	encoded, will, err := s.encodeSession(sess)
	if err != nil {
		return err
	}
	var (
		old store.Session
		had bool
	)
	// **Answering where its client's packet waits, and only there**
	// (DB.Waiting): a SUBSCRIBE's, an UNSUBSCRIBE's or a CONNECT's record,
	// each written while its packet is marked, is served ahead of the writes
	// nobody waits on. A record written for nobody - a Will's publication
	// taken off it, a refused record written again, a discarded session's
	// subscriptions - is a departed client's.
	return s.db.joinWrite(&write{s: s, client: sess.Client, departed: true,
		check: func(tx *sql.Tx) error {
			var err error
			old, had, err = scanSession(s.db.queryRow(tx, sessionRowText, sess.Client))
			return err
		},
		run: func(tx *sql.Tx) error {
			_, err := s.db.exec(tx, upsertSession,
				sess.Client, int64(sess.ExpiryInterval), encodeTime(sess.DisconnectedAt), encoded, will)
			return err
		},
		apply: func() func() { return s.replaced(old, had, sess) },
	})
}

// SaveWithShareCursors keeps a session as Save does and gives each of groups
// a cursor at cursor where it has none, as CreateShareCursor does, in one
// transaction: the record's upsert and each cursor's insert commit together
// or not at all. See store.Sessions.SaveWithShareCursors.
func (s *Sessions) SaveWithShareCursors(sess store.Session, cursor uint64, groups []string) error {
	if len(groups) == 0 {
		return s.Save(sess)
	}
	for _, g := range groups {
		if err := store.ValidShareGroup(g); err != nil {
			return err
		}
		if !slices.ContainsFunc(sess.Subscriptions, func(sub store.SessionSubscription) bool { return sub.Filter == g }) {
			return store.ErrNoShareGroup
		}
	}
	next, err := s.logNext()
	if err != nil {
		return err
	}
	if err := shareCursorFits(cursor, next); err != nil {
		return err
	}
	encoded, will, err := s.encodeSession(sess)
	if err != nil {
		return err
	}
	var (
		old  store.Session
		had  bool
		made []string
	)
	// Served as the record of a client whose packet waits is (Save).
	return s.db.joinWrite(&write{s: s, client: sess.Client, departed: true,
		check: func(tx *sql.Tx) error {
			var err error
			old, had, err = scanSession(s.db.queryRow(tx, sessionRowText, sess.Client))
			return err
		},
		run: func(tx *sql.Tx) error {
			made = made[:0]
			if _, err := s.db.exec(tx, upsertSession,
				sess.Client, int64(sess.ExpiryInterval), encodeTime(sess.DisconnectedAt), encoded, will); err != nil {
				return err
			}
			for _, g := range groups {
				res, err := s.db.exec(tx, `INSERT INTO share_groups (share_group, "cursor") VALUES (?, ?)
				ON CONFLICT (share_group) DO NOTHING`, g, int64(cursor))
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n > 0 {
					made = append(made, g)
				}
			}
			return nil
		},
		apply: func() func() {
			undo := s.replaced(old, had, sess)
			var n int64
			for _, g := range made {
				n += store.ShareCursorSize(g)
			}
			s.bytes += n
			return func() {
				s.bytes -= n
				undo()
			}
		},
	})
}

// encodeSession is what a session's row holds beside its client id and its
// numbers: its subscriptions, and its Will or NULL.
func (s *Sessions) encodeSession(sess store.Session) (subs string, will any, err error) {
	list := sess.Subscriptions
	if list == nil {
		list = []store.SessionSubscription{}
	}
	encoded, err := json.Marshal(list)
	if err != nil {
		return "", nil, s.fail(err)
	}
	if sess.Will != nil {
		w, err := json.Marshal(sess.Will)
		if err != nil {
			return "", nil, s.fail(err)
		}
		will = string(w)
	}
	return string(encoded), will, nil
}

// upsertSession is the statement that keeps a session's row.
const upsertSession = `INSERT INTO sessions (client, expiry, disconnected, subscriptions, will)
	VALUES (?, ?, ?, ?, ?)
	ON CONFLICT (client) DO UPDATE SET
		expiry = excluded.expiry,
		disconnected = excluded.disconnected,
		subscriptions = excluded.subscriptions,
		will = excluded.will`

// replaceSession is upsertSession for a session begun in place of an ended
// one (Begin): the window its in-flight table was written under was the ended
// session's, and a new session begins with none, as store.Sessions does.
const replaceSession = upsertSession + `,
		receive_maximum = 0`

// replaced moves the counts from old, where had, to sess, which replaced it
// under its client id, and answers what moves them back.
func (s *Sessions) replaced(old store.Session, had bool, sess store.Session) func() {
	if had {
		s.bytes -= store.SessionSize(old)
		s.holders.Remove(old)
	} else {
		s.sessions++
	}
	s.bytes += store.SessionSize(sess)
	s.holders.Add(sess)
	return func() {
		s.holders.Remove(sess)
		s.bytes -= store.SessionSize(sess)
		if had {
			s.holders.Add(old)
			s.bytes += store.SessionSize(old)
		} else {
			s.sessions--
		}
	}
}

// Disconnected records when a session's client went away, the expiry
// interval in force as it went and, with dropWill, that its Will is
// withdrawn, in one transaction that changes exactly one row, and gives back
// what the Will took. store.ErrNoSession where the
// client has no session. See store.Sessions.Disconnected.
func (s *Sessions) Disconnected(client string, at time.Time, dropWill bool, expiry uint32) error {
	var (
		will  sql.NullString
		freed int64
	)
	// **In the reserve.** Recording the disconnect starts the clock that ends
	// the session and frees what it holds, and it withdraws a Will; the row
	// grows by the time it now carries, so at a full provider an ordinary
	// transaction refused it - for 717 of 887 sessions, measured - and a
	// session outlived its interval after a crash. What precedes relief
	// takes the reserve, as Acknowledge, Return and drop do.
	// **A departed client's, answering nobody**: a client that sent
	// DISCONNECT closes its end whatever the broker does [MQTT-3.14.4-2],
	// and one that went without it is gone, so the close that answers a
	// DISCONNECT, written after this, reaches nobody who waits on it. What
	// does wait is the same id's next CONNECT, on the session lock this is
	// written under - and from the moment that CONNECT is read, this is
	// served as a write it waits on (DB.Waiting). Served ahead of the rest
	// instead, ten thousand clients disconnecting at once put every one of
	// these ahead of the first reconnecting client's CONNACK: 159 to 178 of
	// 10,000 timed out at Paho's 10s on a Raspberry Pi 4, measured.
	return s.db.joinWrite(&write{s: s, client: client, relief: true, departed: true,
		// Read inside the write's own transaction, for what the Will gives
		// back: a failure here fails the write, and nothing changes.
		check: func(tx *sql.Tx) error {
			switch err := s.db.queryRow(tx, `SELECT will FROM sessions WHERE client = ?`, client).Scan(&will); {
			case errors.Is(err, sql.ErrNoRows):
				return store.ErrNoSession
			default:
				return err
			}
		},
		run: func(tx *sql.Tx) error {
			freed = 0
			res, err := s.db.exec(tx, `UPDATE sessions SET disconnected = ?, expiry = ?,
			will = CASE WHEN ? THEN NULL ELSE will END WHERE client = ?`, encodeTime(at), int64(expiry), dropWill, client)
			if err != nil {
				return err
			}
			if n, err := res.RowsAffected(); err != nil {
				return err
			} else if n != 1 {
				return fmt.Errorf("recording %q's disconnect changed %d rows, want 1", client, n)
			}
			if dropWill && will.Valid {
				var held store.Session
				if err := fillSession(&held, 0, 0, "[]", will); err != nil {
					return err
				}
				freed = store.SessionSize(held)
			}
			return nil
		},
		apply: func() func() {
			f := freed
			s.bytes -= f
			return func() { s.bytes += f }
		},
	})
}

// Get returns the session kept for a client id.
func (s *Sessions) Get(client string) (store.Session, bool, error) {
	if s.db.reads == nil {
		return s.get(client)
	}
	var (
		sess store.Session
		had  bool
	)
	err := s.db.readTx(func(tx *sql.Tx, stmt func(string) *sql.Stmt) error {
		var err error
		sess, had, err = scanSession(stmt(sessionRowText).QueryRow(client))
		return err
	})
	if err != nil {
		return store.Session{}, false, s.fail(err)
	}
	return sess, had, nil
}

// All returns every session, ordered by client id.
func (s *Sessions) All() ([]store.Session, error) {
	if s.db.reads == nil {
		return s.all()
	}
	var out []store.Session
	err := s.db.readTx(func(tx *sql.Tx, stmt func(string) *sql.Stmt) error {
		rows, err := stmt(sessionRowsText).Query()
		if err != nil {
			return err
		}
		out, err = scanSessions(rows)
		return err
	})
	if err != nil {
		return nil, s.fail(err)
	}
	return out, nil
}

// Drop ends a session: it and every message it is owed go in one
// transaction, and the count is how many messages went. The cursors of the
// shared groups it was the last member of go in the same transaction, and
// are named; held is the groups it held. See store.Sessions.Drop.
func (s *Sessions) Drop(client string, held []string) (store.Dropped, error) {
	dropped, _, err := s.drop(client, held, nil, nil, false)
	return dropped, err
}

// DropExpired ends every session whose client has been away for longer than
// its expiry interval, and names them, and the shared groups whose cursors
// went with them, as each record holds them (store.Sessions.DropExpired).
// Each goes in its own transaction with its in-flight table. One keep
// answers true for, where keep is not nil, is left for the caller.
func (s *Sessions) DropExpired(now time.Time, keep func(store.Session) bool) ([]string, store.Dropped, error) {
	var dropped store.Dropped
	all, err := s.all()
	if err != nil {
		return nil, dropped, err
	}
	// **Judged again inside each ending's own transaction**: this read holds
	// no lock, so a client that came back since is resumed rather than
	// ended under it.
	expired := func(sess store.Session) bool { return store.SessionExpired(sess, now) }
	var gone []string
	for _, sess := range all {
		if !store.SessionExpired(sess, now) || keep != nil && keep(sess) {
			continue
		}
		d, ended, err := s.drop(sess.Client, nil, nil, expired, false)
		if err != nil {
			return gone, dropped, err
		}
		if ended {
			gone = append(gone, sess.Client)
			dropped.AddDropped(d)
		}
	}
	return gone, dropped, nil
}

// Len is how many sessions are kept.
func (s *Sessions) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions
}

// Bytes is what the sessions would be charged on a memory provider:
// store.SessionSize and the in-flight and cursor charges, kept as the memory
// provider keeps them so both answer the same question. **Nothing here is
// bounded by it**: this store's bound is its file (DB.Bytes), and the one
// production reader, the over-bound warning at start, asks it only of a
// memory provider.
func (s *Sessions) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// get reads one session on the write connection. Takes no lock: it is called
// by Get where the file has no read connection (DB.reads is nil), before the
// store is shared.
func (s *Sessions) get(client string) (store.Session, bool, error) {
	sess, had, err := scanSession(s.db.queryRow(nil, sessionRowText, client))
	if err != nil {
		return store.Session{}, false, s.fail(err)
	}
	return sess, had, nil
}

func (s *Sessions) all() ([]store.Session, error) {
	rows, err := s.db.query(nil, sessionRowsText)
	if err != nil {
		return nil, s.fail(err)
	}
	out, err := scanSessions(rows)
	if err != nil {
		return nil, s.fail(err)
	}
	return out, nil
}

const sessionRowText = `SELECT client, expiry, disconnected, subscriptions, will FROM sessions WHERE client = ?`

const sessionRowsText = `SELECT client, expiry, disconnected, subscriptions, will FROM sessions ORDER BY client`

// scanSession reads one session's row, false where there is none.
func scanSession(row *sql.Row) (store.Session, bool, error) {
	var (
		sess         store.Session
		expiry, disc int64
		subs         string
		will         sql.NullString
	)
	switch err := row.Scan(&sess.Client, &expiry, &disc, &subs, &will); {
	case errors.Is(err, sql.ErrNoRows):
		return store.Session{}, false, nil
	case err != nil:
		return store.Session{}, false, err
	}
	if err := fillSession(&sess, expiry, disc, subs, will); err != nil {
		return store.Session{}, false, err
	}
	return sess, true, nil
}

// scanSessions reads every row sessionRowsText returns, and closes them.
func scanSessions(rows *sql.Rows) ([]store.Session, error) {
	defer rows.Close()
	var out []store.Session
	for rows.Next() {
		var (
			sess         store.Session
			expiry, disc int64
			subs         string
			will         sql.NullString
		)
		if err := rows.Scan(&sess.Client, &expiry, &disc, &subs, &will); err != nil {
			return nil, err
		}
		if err := fillSession(&sess, expiry, disc, subs, will); err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

func fillSession(sess *store.Session, expiry, disconnected int64, subs string, will sql.NullString) error {
	sess.ExpiryInterval = uint32(expiry)
	sess.DisconnectedAt = decodeTime(disconnected)
	if err := json.Unmarshal([]byte(subs), &sess.Subscriptions); err != nil {
		return fmt.Errorf("client %q: subscriptions: %w", sess.Client, err)
	}
	if len(sess.Subscriptions) == 0 {
		sess.Subscriptions = nil
	}
	if will.Valid && will.String != "" {
		sess.Will = &store.SessionWill{}
		if err := json.Unmarshal([]byte(will.String), sess.Will); err != nil {
			return fmt.Errorf("client %q: will: %w", sess.Client, err)
		}
	}
	return nil
}

// Begin ends whatever is kept under client - its record, its in-flight
// table, its cursor, and the cursors of the groups it was the last
// member of, split as Drop splits them - and keeps next in its place, in one
// transaction; next nil keeps nothing. See store.Sessions.Begin.
//
// **The reserve only for a replacement.** An ending is written in the reserve
// because it gives back more than it writes, and the new record rides in
// the same transaction. With nothing kept under the id there is nothing to
// give back, and next is an ordinary Save under the publish ceiling: a new
// session in the reserve would spend room the endings rely on.
func (s *Sessions) Begin(client string, held []string, next *store.Session) (store.Dropped, error) {
	if next != nil && next.Client != client {
		return store.Dropped{}, fmt.Errorf("storage %s, sessions: a session for %q begun under %q",
			s.db.path, next.Client, client)
	}
	dropped, _, err := s.drop(client, held, next, nil, true)
	return dropped, err
}

// drop removes one session and its in-flight table in one transaction, with the
// cursors of the shared groups it was the last member of, which it names
// (store.Sessions.Drop has held), and keeps next in its place in the same
// transaction where it is not nil (Begin), begun saying it is Begin's.
// Called holding no lock: it joins the write group (joinWrite), and the
// group's leader takes DB.writing and then s.mu (runWrites) for the
// transaction, so the write's check and apply run under s.mu.
func (s *Sessions) drop(client string, held []string, next *store.Session,
	onlyIf func(store.Session) bool, begun bool) (store.Dropped, bool, error) {
	var (
		nextSubs string
		nextWill any
	)
	if next != nil {
		var err error
		if nextSubs, nextWill, err = s.encodeSession(*next); err != nil {
			return store.Dropped{}, false, err
		}
	}
	var (
		sess  store.Session
		had   bool
		table []store.InFlight
		last  []string
	)
	var (
		// groupNet is what the ending's writes to the groups' rows came to:
		// the returned rows it wrote, less the groups that ended with it.
		groupNet int64
		dropped  store.Dropped
		// returning is whether the ending writes the returned rows; it
		// does not on its second try (below).
		returning = true
	)
	ending := func(tx *sql.Tx) error {
		groupNet, dropped = 0, store.Dropped{}
		if !had {
			if next == nil {
				return nil
			}
			_, err := s.db.exec(tx, upsertSession,
				next.Client, int64(next.ExpiryInterval), encodeTime(next.DisconnectedAt), nextSubs, nextWill)
			return err
		}
		// Its in-flight table goes with it, and so does its cursor on the
		// broadcast log, whichever way the session ends: both live exactly as
		// long as the record, and nothing else takes them out (recover leaves
		// the log's cursors to this).
		if _, err := s.db.exec(tx, `DELETE FROM session_inflight WHERE client = ?`, client); err != nil {
			return err
		}
		if _, err := s.db.exec(tx, `DELETE FROM positions WHERE channel = ? AND reader = ?`,
			store.BroadcastLog, store.MQTTReader(client)); err != nil {
			return err
		}
		// The groups it was the last member of: their cursors are kept for
		// the sessions holding their filters, and none is left.
		for _, group := range last {
			freed, had, err := endGroup(s.db, tx, group)
			if err != nil {
				return err
			}
			groupNet -= freed
			if had {
				dropped.Groups = append(dropped.Groups, group)
			}
		}
		// What its groups had handed it, split as store.Sessions.Drop splits
		// it, the returned list written in the same transaction.
		for _, f := range table {
			if f.Group == "" || (f.QoS == 2 && f.State == store.MessageReleased) {
				continue
			}
			var one int
			switch err := s.db.queryRow(tx, `SELECT 1 FROM share_groups WHERE share_group = ?`, f.Group).Scan(&one); {
			case errors.Is(err, sql.ErrNoRows):
				dropped.Orphaned = append(dropped.Orphaned, f)
				continue
			case err != nil:
				return err
			}
			if f.QoS == 2 {
				dropped.Lost = append(dropped.Lost, f)
				continue
			}
			if !returning {
				dropped.Unreturned = append(dropped.Unreturned, f)
				continue
			}
			res, err := s.db.exec(tx, `INSERT INTO share_returned (share_group, "offset") VALUES (?, ?)
				ON CONFLICT DO NOTHING`, f.Group, int64(f.Offset))
			if err != nil {
				return err
			}
			if n, err := res.RowsAffected(); err != nil {
				return err
			} else if n > 0 {
				groupNet += store.ShareReturnedSize(f.Group)
			}
			if dropped.Returned == nil {
				dropped.Returned = map[string][]uint64{}
			}
			dropped.Returned[f.Group] = append(dropped.Returned[f.Group], f.Offset)
		}
		if next != nil {
			_, err := s.db.exec(tx, replaceSession,
				next.Client, int64(next.ExpiryInterval), encodeTime(next.DisconnectedAt), nextSubs, nextWill)
			return err
		}
		_, err := s.db.exec(tx, `DELETE FROM sessions WHERE client = ?`, client)
		return err
	}
	// **In the reserve, as an acknowledgement is**: an ending gives back more
	// than it writes. Measured on a file at its bound, a member's rows sharing
	// their pages with another member's free no page to write the returned
	// rows into, and the ordinary ceiling refused every such ending from a
	// thousand returned rows up. The reserve takes the ordinary case - ten
	// thousand rows of a 16-byte group, a thousand of a 256-byte one - and
	// not every case a Receive Maximum and a long group name allow: ten
	// thousand of a 256-byte group needed about 700 pages of a 258-page
	// reserve. **So an ending the reserve cannot hold ends anyway**, without
	// returning them: those deliveries are lost to the provider's room
	// (Dropped.Unreturned), and the session is not left behind by half.
	//
	// **The reserve only for a replacement** (Begin): with nothing kept under
	// the id there is nothing to give back, and next is written under the
	// publish ceiling.
	var w *write
	// answers: a CONNACK waits on a session begun (Begin). An ending - Drop,
	// the expiry sweep's - is a departed client's, answering nobody's packet:
	// one a CONNECT waits on, on the id's session lock the ending holds, is
	// served as answering from that CONNECT's arrival (DB.Waiting).
	w = &write{s: s, client: client, answers: begun, departed: !begun,
		// What the ending takes is read in its own transaction, and which
		// groups end with it from the holders as every write before it in
		// that transaction left them: the store's lock is the leader's.
		check: func(tx *sql.Tx) error {
			var err error
			table, last = nil, nil
			if sess, had, err = scanSession(s.db.queryRow(tx, sessionRowText, client)); err != nil {
				return err
			}
			if had && onlyIf != nil && !onlyIf(sess) {
				had = false
				return errNotEnded
			}
			w.relief = had
			if !had {
				if next == nil {
					return errNotEnded // nothing kept, and nothing to keep
				}
				return nil
			}
			if _, table, err = inFlightOf(s.db, tx, client); err != nil {
				return err
			}
			last = s.holders.Ending(sess, held)
			if next != nil {
				// A group the next session still holds does not end with this one.
				last = slices.DeleteFunc(last, func(g string) bool {
					return slices.ContainsFunc(next.Subscriptions, func(sub store.SessionSubscription) bool {
						return sub.Filter == g
					})
				})
			}
			return nil
		},
		run: ending,
		apply: func() func() {
			if !had {
				if next == nil {
					return func() {}
				}
				return s.replaced(store.Session{}, false, *next)
			}
			s.holders.Remove(sess)
			// The session itself and its in-flight table, less what its ending
			// wrote to its groups' returned lists and more what the groups that
			// ended with it gave back.
			size := store.SessionSize(sess) - groupNet
			for _, f := range table {
				size += store.InFlightBytes(f)
			}
			s.sessions--
			s.bytes -= size
			var undoNext func()
			if next != nil {
				undoNext = s.replaced(store.Session{}, false, *next)
			}
			return func() {
				if undoNext != nil {
					undoNext()
				}
				s.bytes += size
				s.sessions++
				s.holders.Add(sess)
			}
		},
		again: func() bool {
			if !had || !returning || !slices.ContainsFunc(table, func(f store.InFlight) bool {
				return f.Group != "" && f.QoS == 1
			}) {
				return false
			}
			returning = false
			return true
		},
	}
	switch err := s.db.joinWrite(w); {
	case errors.Is(err, errNotEnded):
		return store.Dropped{}, false, nil
	case err != nil:
		return store.Dropped{}, false, err
	}
	return dropped, had, nil
}

// errNotEnded is an ending with nothing to write: no session kept under the
// id and none to keep, or, asked only where the session still qualifies
// (DropExpired), one that no longer does - a client came back.
var errNotEnded = errors.New("the session no longer qualifies to be ended")
