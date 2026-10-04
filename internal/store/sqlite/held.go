package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// The exactly-once publishes a channel holds for their release
// (store.HeldPublish), in held_publishes beside the records they will join.

// errNotHeld ends a release whose hold went while it was being made - a
// session ending, an expiry - so that the record is not stored for an
// exchange nobody holds any more.
var errNotHeld = errors.New("saguin: the exchange is no longer held")

// hold keeps a publish for its release in channel's rows. A repeat of one
// already held is success and changes nothing, which is [MQTT-4.3.3-10] and
// keeps the clock the first copy started. At the page ceiling it is refused
// store.ErrProviderFull, from the transaction.
func (d *DB) hold(channel string, e store.Exchange, r store.Record, now time.Time) error {
	headers, err := encodeHeaders(r.Headers)
	if err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
	}
	props, err := encodeProps(r)
	if err != nil {
		return fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
	}
	// answers: the publisher's PUBREC waits on it.
	return d.joinWrite(&write{client: e.Client, answers: true, run: func(tx *sql.Tx) error {
		_, err := d.exec(tx,
			`INSERT INTO held_publishes
			   (channel, client, packet_id, message_id, topic, payload, headers, ts, held, props)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT (channel, client, packet_id) DO NOTHING`,
			channel, e.Client, int64(e.PacketID), r.MessageID, r.Topic, r.Payload, headers,
			encodeTime(r.Timestamp), encodeTime(now), props)
		return err
	}})
}

// heldOne is the publish held for one exchange in channel, and whether there
// is one.
func (d *DB) heldOne(channel string, e store.Exchange) (store.Record, bool, error) {
	rows, err := d.query(nil,
		`SELECT client, packet_id, held, message_id, topic, payload, headers, ts, props
		   FROM held_publishes WHERE channel = ? AND client = ? AND packet_id = ?`,
		channel, e.Client, int64(e.PacketID))
	if err != nil {
		return store.Record{}, false, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
	}
	held, err := d.scanHeld(rows, channel)
	if err != nil || len(held) == 0 {
		return store.Record{}, false, err
	}
	return held[0].Record, true, nil
}

// heldIn is every publish channel holds, by client and packet identifier.
func (d *DB) heldIn(channel string) ([]store.HeldPublish, error) {
	rows, err := d.query(nil,
		`SELECT client, packet_id, held, message_id, topic, payload, headers, ts, props
		   FROM held_publishes WHERE channel = ? ORDER BY client, packet_id`, channel)
	if err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
	}
	return d.scanHeld(rows, channel)
}

func (d *DB) scanHeld(rows *sql.Rows, channel string) ([]store.HeldPublish, error) {
	defer rows.Close()
	var out []store.HeldPublish
	for rows.Next() {
		var (
			h          store.HeldPublish
			packetID   int64
			held, ts   int64
			headers    sql.NullString
			properties sql.NullString
		)
		if err := rows.Scan(&h.Exchange.Client, &packetID, &held, &h.Record.MessageID, &h.Record.Topic,
			&h.Record.Payload, &headers, &ts, &properties); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
		}
		h.Exchange.PacketID = uint16(packetID)
		h.HeldAt = decodeTime(held)
		h.Record.Timestamp = decodeTime(ts)
		var err error
		if h.Record.Headers, err = decodeHeaders(headers); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
		}
		if err := decodeProps(properties, &h.Record); err != nil {
			return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
	}
	return out, nil
}

// unhold forgets one exchange channel holds, and reports whether there was
// one. Through the reserve: it gives room back, and the bound never refuses
// the write that makes room.
func (d *DB) unhold(channel string, e store.Exchange) (bool, error) {
	var n int64
	// **Answering where its client's packet waits** (DB.Waiting): a
	// release's PUBCOMP, or the refusal that ends an exchange, is written
	// while the PUBREL is marked. Otherwise it is a departed client's: an
	// ended session's exchanges, or one past its expiry.
	err := d.joinWrite(&write{client: e.Client, relief: true, departed: true, run: func(tx *sql.Tx) error {
		res, err := d.exec(tx, `DELETE FROM held_publishes WHERE channel = ? AND client = ? AND packet_id = ?`,
			channel, e.Client, int64(e.PacketID))
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	}})
	if err != nil {
		return false, fmt.Errorf("storage %s, channel %q: %w", d.path, channel, err)
	}
	return n > 0, nil
}

// DropHeldExcept deletes what this file holds for every channel not named,
// and reports how many it deleted. Called at a start with the channels the
// configuration keeps on this provider: a hold for a channel that is gone
// has nothing to release it into, and would take the provider's room for
// good. Through the reserve, because it gives room back.
func (d *DB) DropHeldExcept(channels []string) (int, error) {
	keep := make(map[string]bool, len(channels))
	for _, c := range channels {
		keep[c] = true
	}
	var n int64
	// The channels something is held for, rather than the configured ones
	// in the statement: those can outnumber what one statement may bind.
	err := d.reliefTx(func(tx *sql.Tx) error {
		rows, err := d.query(tx, `SELECT DISTINCT channel FROM held_publishes`)
		if err != nil {
			return err
		}
		defer rows.Close()
		var gone []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				rows.Close()
				return err
			}
			if !keep[c] {
				gone = append(gone, c)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range gone {
			res, err := d.exec(tx, `DELETE FROM held_publishes WHERE channel = ?`, c)
			if err != nil {
				return err
			}
			k, err := res.RowsAffected()
			if err != nil {
				return err
			}
			n += k
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("storage %s: %w", d.path, err)
	}
	return int(n), nil
}

// swap stores a held publish as a record and forgets the hold, in one
// transaction (store.HeldPublish). It runs p the way runBatch runs a batch
// of one - reserve under DB.writing, then its insert and counters, then
// settle - with the held row deleted in the same transaction, and through
// the reserve, because the room the record takes is the room the hold gave
// up: the page ceiling never refuses it. What can refuse it is reserve,
// where the channel's own bound is: the hold then stays and no offset is
// taken. A hold that went while the release was being made - an expiry, a
// session ending - ends it with errNotHeld and stores nothing.
func (d *DB) swap(p *publish, channel string, e store.Exchange) error {
	d.writing.Lock()
	defer d.writing.Unlock()
	if err := p.reserve(p); err != nil {
		return err
	}
	err := d.reliefTx(func(tx *sql.Tx) error {
		res, err := d.exec(tx, `DELETE FROM held_publishes WHERE channel = ? AND client = ? AND packet_id = ?`,
			channel, e.Client, int64(e.PacketID))
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return errNotHeld
		}
		if err := p.insert(tx); err != nil {
			return err
		}
		return p.counters(tx)
	})
	p.settle(err == nil)
	if err == nil {
		d.recordsCommitted.Add(1)
	}
	return err
}

// release is ReleaseHold for any of the three stores, given the write that
// stores the record: the held row read, the swap, and the record answered
// with its offset.
func (d *DB) release(channel string, e store.Exchange,
	publishOf func(store.Record) (*publish, *uint64, error)) (store.Record, bool, error) {
	r, held, err := d.heldOne(channel, e)
	if err != nil || !held {
		return store.Record{}, false, err
	}
	p, assigned, err := publishOf(r)
	if err != nil {
		return store.Record{}, true, err
	}
	switch err := d.swap(p, channel, e); {
	case errors.Is(err, errNotHeld):
		return store.Record{}, false, nil
	case err != nil:
		return store.Record{}, true, err
	}
	r.Offset = *assigned
	return r, true, nil
}

// Hold keeps an exactly-once publish in this channel until its release,
// without an offset (store.HeldPublish). It is refused store.ErrFull where
// the channel is at its own max_bytes, as a publish would be, and
// store.ErrProviderFull at the page ceiling.
func (l *Log) Hold(e store.Exchange, r store.Record, now time.Time) error {
	l.mu.Lock()
	full := l.maxBytes > 0 && l.held+store.RecordSize(r) > l.maxBytes
	l.mu.Unlock()
	if full {
		if _, held, err := l.db.heldOne(l.name, e); err != nil || held {
			return err
		}
		return store.ErrFull
	}
	return l.db.hold(l.name, e, r, now)
}

// ReleaseHold stores a held publish as the channel's next record and forgets
// the hold, in one transaction (DB.swap). The bool is false where nothing is
// held for the exchange.
func (l *Log) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	return l.db.release(l.name, e, l.publishOf)
}

// DropHold forgets a hold without storing it, and reports whether there was
// one.
func (l *Log) DropHold(e store.Exchange) (bool, error) { return l.db.unhold(l.name, e) }

// Holds is every publish this channel is holding for its release.
func (l *Log) Holds() ([]store.HeldPublish, error) { return l.db.heldIn(l.name) }

// Hold keeps an exactly-once publish in this channel until its release; see
// Log.Hold. A latest channel has no size bound of its own.
func (l *Latest) Hold(e store.Exchange, r store.Record, now time.Time) error {
	return l.db.hold(l.name, e, r, now)
}

// ReleaseHold makes a held publish the topic's value and forgets the hold,
// in one transaction; see Log.ReleaseHold. A deletion of a topic with no live
// value stores nothing, as the publish path stores nothing for one: the hold
// goes and the record is answered without an offset.
func (l *Latest) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	r, held, err := l.db.heldOne(l.name, e)
	if err != nil || !held {
		return store.Record{}, false, err
	}
	if store.IsDeletion(r) {
		cur, found, err := l.Get(r.Topic)
		if err != nil {
			return store.Record{}, true, err
		}
		if !found || store.IsDeletion(cur) {
			if gone, err := l.db.unhold(l.name, e); err != nil || !gone {
				return store.Record{}, gone, err
			}
			return r, true, nil
		}
	}
	return l.db.release(l.name, e, l.publishOf)
}

// DropHold forgets a hold without storing it; see Log.DropHold.
func (l *Latest) DropHold(e store.Exchange) (bool, error) { return l.db.unhold(l.name, e) }

// Holds is every publish this channel is holding for its release.
func (l *Latest) Holds() ([]store.HeldPublish, error) { return l.db.heldIn(l.name) }

// Hold keeps an exactly-once publish in this queue until its release; see
// Log.Hold.
func (q *Queue) Hold(e store.Exchange, r store.Record, now time.Time) error {
	q.mu.Lock()
	full := q.maxBytes > 0 && q.held+store.RecordSize(r) > q.maxBytes
	q.mu.Unlock()
	if full {
		if _, held, err := q.db.heldOne(q.name, e); err != nil || held {
			return err
		}
		return store.ErrFull
	}
	return q.db.hold(q.name, e, r, now)
}

// ReleaseHold makes a held publish available work and forgets the hold, in
// one transaction; see Log.ReleaseHold. A queue at its own max_bytes keeps
// the hold, and has room again as its workers resolve what they hold.
func (q *Queue) ReleaseHold(e store.Exchange) (store.Record, bool, error) {
	return q.db.release(q.name, e, q.publishOf)
}

// DropHold forgets a hold without storing it; see Log.DropHold.
func (q *Queue) DropHold(e store.Exchange) (bool, error) { return q.db.unhold(q.name, e) }

// Holds is every publish this queue is holding for its release.
func (q *Queue) Holds() ([]store.HeldPublish, error) { return q.db.heldIn(q.name) }
