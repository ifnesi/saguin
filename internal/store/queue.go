package store

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/ifnesi/saguin/internal/packet"
)

// DeadLetters belongs to one bounded retention write. The core assigns each
// move's destination offset while reserving both channel owners. Counters
// and channel byte changes are applied only after a successful commit.
type DeadLetters struct {
	Channel                     AdvanceChannel
	Source                      string
	At                          int64
	Moves                       []DeadLetter
	RetryAt                     sql.NullInt64 // indexed minimum after removing waiting jobs
	Expired, Evicted, Exhausted uint64
}

// DeadLetter is a selected unfinished job, backed by the retention page.
// Offset zero discards a job whose expanded properties cannot be encoded.
type DeadLetter struct {
	Source, Offset int64
	Topic          string
	Expired        bool
	Exhausted      bool
	Waiting        bool
	Properties     packet.Properties
	record         *retentionRecord
}

// Prepare constructs only the small broker additions. The original properties
// stay in SQLite until the writer needs them, one record at a time.
func (m *DeadLetter) Prepare(source string, at int64) error {
	r := m.record
	defer func() { m.record = nil }()
	reason := "evicted"
	if m.Exhausted {
		reason = "attempts_exhausted"
	}
	if m.Expired {
		reason = "expired"
	}
	values := []packet.Property{
		{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-reason", Value: reason}},
		{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-channel", Value: source}},
		{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-offset", Value: strconv.FormatInt(r.offset, 10)}},
		{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-attempts", Value: strconv.FormatInt(r.spent.Int64, 10)}},
		{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-at", Value: strconv.FormatInt(at, 10)}},
	}
	if r.first.Valid {
		values = append(values, packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-first", Value: strconv.FormatInt(r.first.Int64, 10)}})
	}
	if r.last.Valid {
		values = append(values, packet.Property{ID: packet.UserProperty, Pair: packet.StringPair{Name: "saguin-dlq-last", Value: strconv.FormatInt(r.last.Int64, 10)}})
	}
	additions, err := packet.NewProperties(packet.PUBLISH, values)
	if err != nil {
		return err
	}
	var prefix [4]byte
	if _, err := packet.AppendVarInt(prefix[:0], uint32(r.propertyBytes+len(additions))); err != nil {
		return err
	}
	m.Properties = additions
	m.Topic = "__dlq/" + string(r.topic)
	return nil
}

// moveJob changes the record's key without loading its payload into Go.
// SQLite still reads and rewrites the payload. Expiry stays unchanged; only
// retention age starts anew. Both channel rows and the job change atomically.
func (e *Retention) moveJob(tx *Tx, m *DeadLetter) error {
	d := e.DeadLetters
	if m.Offset != 0 {
		var stored []byte
		if err := tx.Prepared(readDeadLetterProperties).QueryRowContext(context.Background(), e.Channel, m.Source).Scan(&stored); err != nil {
			return err
		}
		properties, err := packet.ReadStoredProperties(stored, nil)
		if err != nil {
			return err
		}
		section, err := properties.Stored(m.Properties)
		if err != nil {
			return err
		}
		var size int64
		if err := tx.Prepared(moveDeadLetter).QueryRowContext(context.Background(), e.Channel, m.Source,
			d.Channel.ID, m.Offset, d.At, m.Topic, section).Scan(&size); err != nil {
			return err
		}
		d.Channel.Bytes += size
		switch {
		case m.Expired:
			d.Expired++
		case m.Exhausted:
			d.Exhausted++
		default:
			d.Evicted++
		}
	}
	_, err := tx.Prepared(deleteQueueJob).ExecContext(context.Background(), e.Channel, m.Source)
	return err
}

// QueueChange spends an attempt once or completes its job. A completion
// deletes only queue metadata; readers keep the original record.
type QueueChange struct {
	Channel, Offset, Began int64
	Spend, Complete, Fail  bool
	NextTry                int64
}

// Records returns zero: job metadata adds no channel record.
func (*QueueChange) Records() int { return 0 }

// WriteTx commits only this current attempt's bounded metadata change.
func (q *QueueChange) WriteTx(tx *Tx) error {
	if q.Spend {
		if _, err := tx.Prepared(spendQueueAttempt).ExecContext(context.Background(), q.Channel, q.Offset, q.Began); err != nil {
			return err
		}
	}
	if q.Complete {
		_, err := tx.Prepared(deleteQueueJob).ExecContext(context.Background(), q.Channel, q.Offset)
		return err
	}
	if q.Fail {
		_, err := tx.Prepared(failQueueAttempt).ExecContext(context.Background(), q.Channel, q.Offset, q.NextTry)
		return err
	}
	return nil
}

// QueueRetries is one bounded page of due jobs, kept under its queue's
// reservation from discovery through the write making them ready.
type QueueRetries struct {
	Channel, At int64
	Offsets     []int64
	Next        sql.NullInt64
}

// ReadQueueRetries discovers only waiting jobs through queue_jobs_retry.
type ReadQueueRetries struct{ Retries *QueueRetries }

// ReadConn fixes the due page. An empty page can establish its next deadline;
// otherwise the write discovers the minimum after ending these waits.
func (r *ReadQueueRetries) ReadConn(conn *Conn) error {
	q := r.Retries
	rows, err := conn.Prepared(readQueueRetries).QueryContext(context.Background(), q.Channel, q.At)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var offset int64
		if err := rows.Scan(&offset); err != nil {
			return err
		}
		q.Offsets = append(q.Offsets, offset)
	}
	if err := finishRows(rows); err != nil {
		return err
	}
	if len(q.Offsets) == 0 {
		q.Next, err = queueRetryAt(conn.Prepared(nextQueueRetry), q.Channel)
	}
	return err
}

func queueRetryAt(stmt *sql.Stmt, channel int64) (sql.NullInt64, error) {
	var at sql.NullInt64
	err := stmt.QueryRowContext(context.Background(), channel).Scan(&at)
	return at, err
}

// Records returns zero: making a job ready adds no record.
func (*QueueRetries) Records() int { return 0 }

// WriteTx ends the retry waits captured by the queue's ordered read.
func (q *QueueRetries) WriteTx(tx *Tx) error {
	for _, offset := range q.Offsets {
		if _, err := tx.Prepared(readyQueueJob).ExecContext(context.Background(), q.Channel, offset); err != nil {
			return err
		}
	}
	if len(q.Offsets) < MetadataPage {
		var err error
		q.Next, err = queueRetryAt(tx.Prepared(nextQueueRetry), q.Channel)
		return err
	}
	return nil
}
