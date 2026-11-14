package store

import (
	"context"
	"database/sql"
)

// ReadSeekPosition fixes the channel range and its time-selected offset in
// one ordered view. No record properties or payloads cross the store boundary.
type ReadSeekPosition struct {
	Channel      int64
	Time         sql.NullInt64 // absent for a decimal offset, which needs no record reads
	Floor, Next  int64
	First, Found sql.NullInt64
}

// ReadConn reads only scalar range and timestamp metadata.
func (r *ReadSeekPosition) ReadConn(conn *Conn) error {
	return conn.Prepared(readSeekPosition).QueryRowContext(context.Background(), r.Time.Valid, r.Time.Valid, r.Time.Int64, r.Channel).
		Scan(&r.Floor, &r.Next, &r.First, &r.Found)
}

// SeekPosition replaces progress and supersedes every existing exchange in
// this channel. Never-sent preparations are deleted in the same transaction.
// DeliveryChanges supplies the existing bounded key deletion mechanism.
type SeekPosition struct {
	DeliveryChanges
	Offset, At int64
}

// WriteTx applies progress, preparation deletion and moves=0 atomically.
func (s *SeekPosition) WriteTx(tx *Tx) error {
	if err := s.DeliveryChanges.WriteTx(tx); err != nil {
		return err
	}
	if _, err := tx.Prepared(seekSessionPosition).ExecContext(context.Background(), s.Session, s.Channel, s.Offset, s.At); err != nil {
		return err
	}
	if _, err := tx.Prepared(deleteSessionMarkers).ExecContext(context.Background(), s.Session, s.Channel); err != nil {
		return err
	}
	_, err := tx.Prepared(supersedeSeekDeliveries).ExecContext(context.Background(), s.Session, s.Channel)
	return err
}
