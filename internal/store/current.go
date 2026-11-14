package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
)

// ReadCurrentValue first measures a current value. With Into set, it loads
// only a value fitting Budget, or returns its new sizes with Retry set. Both
// probes use this ordered read's view; no reader waits for a memory grant.
type ReadCurrentValue struct {
	Channel          int64
	Topic            string
	Size, StoredSize int
	Budget           int64
	Into             *[]byte
	Retry            bool
	Publication      *packet.Publish // immutable answer envelope, without its payload
	Version          packet.Version
}

// ReadConn measures before fetching the BLOB in the same established view.
func (r *ReadCurrentValue) ReadConn(conn *Conn) error {
	r.Size, r.StoredSize, r.Retry = 0, 0, false
	if r.Into != nil {
		*r.Into = nil
	}
	err := conn.Prepared(readCurrentValueSize).QueryRowContext(context.Background(), r.Channel, r.Topic).Scan(&r.Size, &r.StoredSize)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil || r.Into == nil {
		return err
	}
	size, err := packet.PublishSize(r.Version, r.Publication, r.Size)
	if errors.Is(err, packet.ErrCannotEncode) {
		r.Retry = true
		return nil
	}
	if err != nil {
		return err
	}
	if int64(size)+int64(r.StoredSize) > r.Budget {
		r.Retry = true
		return nil
	}
	rows, err := conn.Prepared(readCurrentValuePayload).QueryContext(context.Background(), r.Channel, r.Topic)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		var stored sql.RawBytes
		// RawBytes avoids database/sql's copy. The pinned go-sqlite3 driver
		// gives each BLOB its own slice, never reused after Next or Close;
		// database/sql's RawBytes contract alone does not promise that lifetime.
		if err := rows.Scan(&stored); err != nil {
			return err
		}
		*r.Into, err = payload.Decode(stored, r.Size)
		if err != nil {
			return err
		}
	}
	return finishRows(rows)
}
