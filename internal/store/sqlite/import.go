package sqlite

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// Import writes channels into the database exactly as they already stand:
// every record at the offset it already carries, and next and floor as they
// were stored rather than derived from what arrived.
//
// It is the one write in saguin that does not assign an offset, and that is
// the whole of its danger. Everywhere else the store decides what number a
// record gets, which is what makes an offset monotonic and never reused
// (invariant 9). Here the caller supplies them, because carrying them
// across is the entire reason the migration exists - a channel that arrived
// renumbered would hand a stored consumer position at unrelated records.
//
// So it is bounded on both sides rather than trusted. It refuses a database
// that has any channel store open on it, which a running broker always has,
// and it refuses a channel that is already present. The command that calls
// it refuses a database that existed before it started. Nothing on the
// publish path can reach it.
//
// Whatever it is given lands in one transaction: all of those channels, or
// none of them. The caller chooses the unit that way, and the migration
// makes it one snapshot file - so a queue and its dead-letter channel
// arrive together, which is the same commit invariant 5 asks for on the
// dead-letter move itself, while a file that cannot be imported does not
// take the channels that already succeeded with it.
func (d *DB) Import(channels []store.ChannelState) error {
	d.mu.Lock()
	open := len(d.channels)
	d.mu.Unlock()
	if open > 0 {
		return fmt.Errorf(
			"storage %s: %d channel(s) are already open on this database, so it is being served rather than migrated into",
			d.path, open)
	}

	// **The write lock, like every other transaction writing a channel's
	// counter row** - importChannel below inserts it. Nothing can race a
	// migration, which refuses above if any channel is open, but that is a
	// claim about when this runs and those stop being true.
	//
	// Taken after d.mu is released rather than before it, because DB.Log
	// holds d.mu while EnsureChannel takes this lock: the two orders
	// together would be a cycle, and the transaction below needs neither
	// held at once.
	d.writing.Lock()
	defer d.writing.Unlock()

	return d.tx(func(tx *sql.Tx) error {
		for i := range channels {
			if err := importChannel(d, tx, &channels[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func importChannel(d *DB, tx *sql.Tx, cs *store.ChannelState) error {
	var present int
	if err := d.queryRow(tx, `SELECT count(*) FROM channels WHERE name = ?`, cs.Name).Scan(&present); err != nil {
		return err
	}
	if present > 0 {
		return fmt.Errorf("channel %q is already in this database", cs.Name)
	}
	// The counter arrives with the records rather than being left at zero,
	// or the channel comes back believing it holds nothing and accepts
	// writes past its bound until something recomputes it. Only an append
	// channel carries one: a queue's is summed when its store opens, and a
	// latest channel has no size bound to keep it for.
	var held int64
	if cs.Kind == store.KindAppend {
		for i := range cs.Records {
			held += store.RecordSize(cs.Records[i])
		}
	}
	if _, err := d.exec(tx,
		`INSERT INTO channels (name, kind, next, floor, bytes) VALUES (?, ?, ?, ?, ?)`,
		cs.Name, int64(cs.Kind), int64(cs.Next), int64(cs.Floor), held); err != nil {
		return err
	}

	switch cs.Kind {
	case store.KindAppend:
		if err := importRecords(tx, cs); err != nil {
			return err
		}
	case store.KindLatest:
		if err := importLatest(tx, cs); err != nil {
			return err
		}
	case store.KindQueue:
		if err := importQueue(tx, cs); err != nil {
			return err
		}
	default:
		return fmt.Errorf("channel %q: %s", cs.Name, cs.Kind)
	}

	// Only an append channel has readers. A latest channel has no history to
	// resume from and a queue tracks its work by the state of each record,
	// so a position on either is a row nothing would ever read again or take
	// out - which is how a table grows for ever (invariant 13).
	if cs.Kind != store.KindAppend && len(cs.Positions) > 0 {
		return fmt.Errorf("channel %q is a %s and carries %d consumer position(s), which only an append channel has",
			cs.Name, cs.Kind, len(cs.Positions))
	}
	return importPositions(tx, cs)
}

func importRecords(tx *sql.Tx, cs *store.ChannelState) error {
	stmt, err := tx.Prepare(
		`INSERT INTO records (channel, "offset", message_id, topic, payload, headers, ts, props)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for i := range cs.Records {
		r := &cs.Records[i]
		headers, err := encodeHeaders(r.Headers)
		if err != nil {
			return fmt.Errorf("channel %q, offset %d: %w", cs.Name, r.Offset, err)
		}
		properties, err := encodeProps(*r)
		if err != nil {
			return fmt.Errorf("channel %q, offset %d: %w", cs.Name, r.Offset, err)
		}
		if _, err := stmt.Exec(cs.Name, int64(r.Offset), r.MessageID, r.Topic,
			r.Payload, headers, encodeTime(r.Timestamp), properties); err != nil {
			return fmt.Errorf("channel %q, offset %d: %w", cs.Name, r.Offset, err)
		}
	}
	return nil
}

func importLatest(tx *sql.Tx, cs *store.ChannelState) error {
	stmt, err := tx.Prepare(
		`INSERT INTO latest_values (channel, topic, "offset", message_id, payload, headers, ts, props)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	// Keyed by topic here and by offset in the file, so two records for one
	// topic would be a silent overwrite rather than a failed insert. There
	// cannot be two - a latest channel holds one value per topic - so a file
	// that carries two is one this broker must not adopt.
	for i := range cs.Records {
		r := &cs.Records[i]
		headers, err := encodeHeaders(r.Headers)
		if err != nil {
			return fmt.Errorf("channel %q, topic %q: %w", cs.Name, r.Topic, err)
		}
		properties, err := encodeProps(*r)
		if err != nil {
			return fmt.Errorf("channel %q, topic %q: %w", cs.Name, r.Topic, err)
		}
		if _, err := stmt.Exec(cs.Name, r.Topic, int64(r.Offset), r.MessageID,
			r.Payload, headers, encodeTime(r.Timestamp), properties); err != nil {
			return fmt.Errorf("channel %q, topic %q: %w", cs.Name, r.Topic, err)
		}
	}
	return nil
}

func importQueue(tx *sql.Tx, cs *store.ChannelState) error {
	stmt, err := tx.Prepare(
		`INSERT INTO queue_items
		 (channel, "offset", message_id, topic, payload, headers, ts, attempts, first_seen, last_seen, props)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for i := range cs.Items {
		it := &cs.Items[i]
		headers, err := encodeHeaders(it.Headers)
		if err != nil {
			return fmt.Errorf("channel %q, offset %d: %w", cs.Name, it.Offset, err)
		}
		properties, err := encodeProps(it.Record)
		if err != nil {
			return fmt.Errorf("channel %q, offset %d: %w", cs.Name, it.Offset, err)
		}
		// State, Epoch, DeliveryID, Holder and LeaseUntil are not carried,
		// and there is nowhere to put them: no record is in flight after a
		// restart, so a queue row is an unresolved record and nothing else
		// (invariant 15). Attempts and the two timestamps do come across, so
		// a record that has spent its attempts is dead-lettered rather than
		// starting over, and it still says why it failed.
		if _, err := stmt.Exec(cs.Name, int64(it.Offset), it.MessageID, it.Topic,
			it.Payload, headers, encodeTime(it.Timestamp),
			it.Attempts, encodeTime(it.FirstSeen), encodeTime(it.LastSeen),
			properties); err != nil {
			return fmt.Errorf("channel %q, offset %d: %w", cs.Name, it.Offset, err)
		}
	}
	return nil
}

func importPositions(tx *sql.Tx, cs *store.ChannelState) error {
	if len(cs.Positions) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(
		`INSERT INTO positions (channel, reader, "offset", last_seen, expires_in)
		 VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for _, p := range cs.Positions {
		// Names are taken as they arrive: every one in a file this broker
		// reads carries its scheme, and adding one again would take
		// `bridge:head-office/events` to `mqtt:bridge:head-office/events`,
		// a name the bridge never looks up.
		if _, err := stmt.Exec(cs.Name, p.Reader,
			int64(p.Offset), encodeTime(p.LastSeen), int64(p.ExpiresIn)); err != nil {
			return fmt.Errorf("channel %q, reader %q: %w", cs.Name, p.Reader, err)
		}
	}
	return nil
}

// Export reads every channel out of the database, in name order, as the
// state a snapshot holds.
//
// It is Import's opposite and it takes nothing back: next and floor come
// from the channels row as they stand, so a channel that retention has
// trimmed arrives with the gap it actually has rather than with one
// computed from the records that survive.
//
// Grouping a queue with its dead-letter channel is not done here. That is a
// question about what channel names mean, which this package deliberately
// knows nothing about - it stores records, and the broker layer owns the
// topic contract. The caller groups them.
//
// **The broadcast log is left out, because it is session state and not a
// channel.** Its messages are owed through the cursors the sessions hold, and
// the migration carries no session, so the log goes wherever they go. Its
// reserved name is not the reason: the retained store has one too, and is
// carried, because a memory provider reads it back as a channel file.
func (d *DB) Export() ([]store.ChannelState, error) {
	rows, err := d.query(nil, `SELECT name, kind, next, floor FROM channels WHERE name != ? ORDER BY name`,
		store.BroadcastLog)
	if err != nil {
		return nil, fmt.Errorf("storage %s: %w", d.path, err)
	}
	defer rows.Close()
	var out []store.ChannelState
	for rows.Next() {
		var (
			cs   store.ChannelState
			kind int64
		)
		if err := rows.Scan(&cs.Name, &kind, &cs.Next, &cs.Floor); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("storage %s: %w", d.path, err)
		}
		cs.Kind = store.Kind(kind)
		out = append(out, cs)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("storage %s: %w", d.path, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("storage %s: %w", d.path, err)
	}

	for i := range out {
		if err := d.exportChannel(&out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (d *DB) exportChannel(cs *store.ChannelState) error {
	switch cs.Kind {
	case store.KindAppend:
		rows, err := d.query(nil,
			`SELECT "offset", message_id, topic, payload, headers, ts, props
			   FROM records WHERE channel = ? ORDER BY "offset"`, cs.Name)
		if err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", d.path, cs.Name, err)
		}
		if cs.Records, err = scanRecords(rows, d.path, cs.Name); err != nil {
			return err
		}
		return d.exportPositions(cs)

	case store.KindLatest:
		// Ordered by offset rather than by topic, because a snapshot restores
		// a latest channel in the order it was written and a reader uses the
		// offsets to tell which of two values is newer.
		rows, err := d.query(nil,
			`SELECT "offset", message_id, topic, payload, headers, ts, props
			   FROM latest_values WHERE channel = ? ORDER BY "offset"`, cs.Name)
		if err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", d.path, cs.Name, err)
		}
		cs.Records, err = scanRecords(rows, d.path, cs.Name)
		return err

	case store.KindQueue:
		return d.exportQueue(cs)
	}
	return fmt.Errorf("storage %s: channel %q: %s", d.path, cs.Name, cs.Kind)
}

func (d *DB) exportQueue(cs *store.ChannelState) error {
	rows, err := d.query(nil,
		`SELECT "offset", message_id, topic, payload, headers, ts, props, attempts, first_seen, last_seen
		   FROM queue_items WHERE channel = ? ORDER BY "offset"`, cs.Name)
	if err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", d.path, cs.Name, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			it          store.Item
			headers     sql.NullString
			properties  sql.NullString
			ts          int64
			first, last sql.NullInt64
		)
		if err := rows.Scan(&it.Offset, &it.MessageID, &it.Topic, &it.Payload,
			&headers, &ts, &properties, &it.Attempts, &first, &last); err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", d.path, cs.Name, err)
		}
		if it.Headers, err = decodeHeaders(headers); err != nil {
			return fmt.Errorf("storage %s, channel %q, offset %d: %w", d.path, cs.Name, it.Offset, err)
		}
		if err := decodeProps(properties, &it.Record); err != nil {
			return fmt.Errorf("storage %s, channel %q, offset %d: %w", d.path, cs.Name, it.Offset, err)
		}
		it.Timestamp = decodeTime(ts)
		if first.Valid {
			it.FirstSeen = decodeTime(first.Int64)
		}
		if last.Valid {
			it.LastSeen = decodeTime(last.Int64)
		}
		// State, Epoch, DeliveryID, Holder and LeaseUntil stay at their zero
		// values, and the snapshot format does not carry them either: no
		// record is in flight after a restart (invariant 15).
		cs.Items = append(cs.Items, it)
	}
	return rows.Err()
}

func (d *DB) exportPositions(cs *store.ChannelState) error {
	rows, err := d.query(nil,
		`SELECT reader, "offset", last_seen, expires_in FROM positions
		  WHERE channel = ? ORDER BY reader`, cs.Name)
	if err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", d.path, cs.Name, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			p                   store.Position
			lastSeen, expiresIn int64
		)
		if err := rows.Scan(&p.Reader, &p.Offset, &lastSeen, &expiresIn); err != nil {
			return fmt.Errorf("storage %s, channel %q: %w", d.path, cs.Name, err)
		}
		p.LastSeen = decodeTime(lastSeen)
		p.ExpiresIn = time.Duration(expiresIn)
		cs.Positions = append(cs.Positions, p)
	}
	return rows.Err()
}
