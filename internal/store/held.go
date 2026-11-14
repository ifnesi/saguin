package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ifnesi/saguin/internal/payload"
)

// ReadHeldPublish loads only a held publication's metadata for its PUBREL.
// The payload moves inside the publication transaction, without a body read.
type ReadHeldPublish struct {
	Session  int64
	Bridge   int64
	PacketID uint16
	Record   Record
}

// ReadConn fixes the source at the PUBREL's ordered store boundary.
func (h *ReadHeldPublish) ReadConn(conn *Conn) error {
	r := &h.Record
	err := conn.Prepared(readHeldPublish).QueryRowContext(context.Background(), h.Session, h.Bridge, h.PacketID).Scan(
		&r.Channel, &r.Time, &r.Topic, &r.Expires, &r.PropertiesSize, &r.HeldRetain, &r.PayloadSize, &r.Retained)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("store: missing held publication at PUBREL read")
	}
	r.QoS, r.HeldSession, r.HeldPacket = 2, h.Session, h.PacketID
	r.Bridge = h.Bridge
	r.PublishedRetained = r.Channel == 1 && r.HeldRetain
	r.Retained = r.Retained || (r.PublishedRetained && r.PayloadSize != 0)
	return err
}

func (r *Record) writeHeld(tx *Tx) (bool, error) {
	ctx := context.Background()
	if r.Offset == 0 {
		var expires any
		if r.Expires.Valid {
			expires = r.Expires.Int64
		}
		_, err := tx.Prepared(addHeldPublish).ExecContext(ctx, r.HeldSession, r.Bridge, r.HeldPacket, r.Channel,
			r.ID, r.Time, r.Topic, r.Publisher, expires, r.Properties, r.HeldRetain,
			payload.Stored(r.Payload, r.StoredPayload), max(len(r.Payload), r.PayloadSize))
		return false, err
	}
	args := []any{r.Offset, r.PublishedRetained, r.Retained, r.HeldSession, r.Bridge, r.HeldPacket}
	updated := false
	var err error
	if r.Retained && r.Channel != 1 {
		updated, err = tx.updateRecord(updateLatestHeld, args)
		if err != nil {
			return false, err
		}
	}
	if !updated {
		moved, err := tx.updateRecord(moveHeldPublish, args)
		if err != nil {
			return false, err
		}
		if !moved {
			return false, errors.New("store: missing held publication at PUBREL commit")
		}
	}
	_, err = tx.Prepared(deleteHeldPublish).ExecContext(ctx, r.HeldSession, r.Bridge, r.HeldPacket)
	return updated, err
}

// ReadTx uses the same topic charge query during a mutation. On $broadcast it
// includes held replacements, so a smaller committed value cannot release their room.
func (r *RetainedCharge) ReadTx(tx *Tx) error {
	return tx.Prepared(retainedCharge).QueryRowContext(context.Background(), r.Topic, r.Channel).Scan(&r.Bytes, &r.Offset)
}

// HeldIdentifier is the bounded routing state restored with an incoming exchange.
// No publication properties or payload leave the store during CONNECT.
type HeldIdentifier struct {
	PacketID uint16
	Channel  int64
}

func loadHeld(conn *Conn, session, bridge int64) ([]HeldIdentifier, error) {
	rows, err := conn.Prepared(loadHeldIdentifiers).QueryContext(context.Background(), session, bridge)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []HeldIdentifier
	for rows.Next() {
		var id HeldIdentifier
		if err := rows.Scan(&id.PacketID, &id.Channel); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, finishRows(rows)
}

// HeldChanges contains aggregate reservations released by a session ending,
// never held payloads or a topic catalogue. Channels are bounded by configuration.
type HeldChanges struct {
	Channels map[int64]int64
	Retained int64
	Count    uint64
}

// removeHeld visits bounded pages while the ending transaction owns the rows.
// Before and after charges use the shared replacement rule for each deletion.
// Owners with no held rows need no aggregate allocation.
func removeHeld(tx *Tx, session, bridge int64, changes **HeldChanges) error {
	var after uint16
	for {
		rows, err := tx.Prepared(endingHeld).QueryContext(context.Background(), session, bridge, after)
		if err != nil {
			return err
		}
		var page [MetadataPage]struct {
			id            uint16
			channel, size int64
			topic         string
			retained      bool
		}
		n := 0
		err = scanRows(rows, func(row *sql.Rows) error {
			p := &page[n]
			if err := row.Scan(&p.id, &p.channel, &p.size, &p.topic, &p.retained); err != nil {
				return err
			}
			n++
			return nil
		})
		if err != nil {
			return err
		}
		for _, p := range page[:n] {
			if *changes == nil {
				*changes = &HeldChanges{Channels: make(map[int64]int64)}
			}
			h := *changes
			charge := RetainedCharge{Topic: p.topic}
			if p.retained {
				if err := charge.ReadTx(tx); err != nil {
					return err
				}
			}
			before := charge.Bytes
			if _, err := tx.Prepared(deleteHeldPublish).ExecContext(context.Background(), session, bridge, p.id); err != nil {
				return err
			}
			if p.retained {
				if err := charge.ReadTx(tx); err != nil {
					return err
				}
				h.Retained += before - charge.Bytes
			}
			h.Channels[p.channel] += p.size
			h.Count++
			after = p.id
		}
		if n < MetadataPage {
			return nil
		}
	}
}

// discardOrphanHeld removes non-durable holds before startup loads charges.
// The ordinary writer transaction supplies full-disk reporting and rollback.
type discardOrphanHeld struct{ count uint64 }

func (*discardOrphanHeld) Records() int { return 0 }

func (d *discardOrphanHeld) WriteTx(tx *Tx) error {
	result, err := tx.Prepared(deleteOrphanHeld).ExecContext(context.Background())
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	d.count = uint64(n)
	return err
}

// loadHeldCharges obtains aggregates after startup's non-durable cleanup.
func (s *State) loadHeldCharges(conn *Conn) error {
	rows, err := conn.Prepared(heldChannelTotals).QueryContext(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	i := 0
	for rows.Next() {
		var channel, size int64
		if err := rows.Scan(&channel, &size); err != nil {
			return err
		}
		for i < len(s.Channels) && s.Channels[i].ID < channel {
			i++
		}
		if i == len(s.Channels) || s.Channels[i].ID != channel {
			return errors.New("store: held publication names a missing channel")
		}
		s.Channels[i].HeldBytes = size
	}
	if err := finishRows(rows); err != nil {
		return err
	}
	return conn.Prepared(retainedChargesTotal).QueryRowContext(context.Background()).Scan(&s.RetainedCharge)
}
