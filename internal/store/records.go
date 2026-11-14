package store

import (
	"context"
	"database/sql"
	"iter"
	"math/bits"

	"github.com/ifnesi/saguin/internal/payload"
)

// Record is a records row, with identifiers, offset and time supplied by
// the core. Properties includes its MQTT Property Length, or is nil for
// none. Expires is an optional Unix millisecond timestamp (storage.md).
type Record struct {
	// HeldPacket selects an incoming QoS 2 source owned by HeldSession or Bridge. Offset
	// zero saves a hold; a positive offset moves it without fetching payload.
	HeldSession                 int64
	HeldPacket                  uint16
	HeldRetain                  bool
	PayloadSize                 int // original length for a compressed payload or held source
	PropertiesSize              int // length of a held source, whose bytes stay in SQLite
	Channel, Offset             int64
	ID                          string
	Time                        int64
	Topic                       string
	QoS                         byte
	Publisher                   string
	Bridge                      int64
	Expires                     sql.NullInt64
	Payload, Properties         []byte
	StoredPayload               []byte // reader-prepared S2 copy; nil when stored as received
	PublishedRetained, Retained bool
}

// RecordMetadata excludes payload bytes. A consumer examines bounded pages
// before reserving message memory for any selected delivery.
type RecordMetadata struct {
	Bridge            int64 // origin; bridge consumers never forward these records
	Attempts          int64 // queue metadata only
	Failed            int64 // failed attempts, independent of spent attempts
	Offset            int64
	ID                string
	Time              int64
	Topic, Properties []byte
	QoS               byte
	Publisher         string
	Expires           sql.NullInt64
	PayloadSize       int
	StoredPayloadSize int // extra memory while decompressing; zero for an uncompressed row
	PublishedRetained bool
	Settled           bool // session marker or stored shared disposition excludes a new delivery
}

// ReadRecordMetadata reads below a boundary the core has seen committed.
// Limit is at most MetadataPage; the core supplies its useful visit budget.
type ReadRecordMetadata struct {
	Channel, From, Before int64
	Jobs                  bool // unfinished queue jobs, in their offset order
	Retained              bool // current values only, below the pass's captured tail
	Limit                 int
	Rows                  []RecordMetadata
}

// MetadataDisposition contains only the excluded offsets of one page for a
// recipient. It never loads a backlog of exchanges or terminal evidence.
type MetadataDisposition struct {
	Session, Share int64
	Settled        []int64
}

// MetadataPage is the row bound for metadata reads (design.md, Capacities).
const MetadataPage = 256

// RecordRead leaves writes collecting for ordinary scans only. A retained
// page selects current values, after its earlier writes have been resolved.
func (r *ReadRecordMetadata) RecordRead() bool { return !r.Retained && !r.Jobs }

// ReadConn returns only metadata, never a message body.
func (r *ReadRecordMetadata) ReadConn(conn *Conn) error {
	statement := readRecordMetadata
	if r.Jobs {
		statement = readQueueMetadata
	} else if r.Retained {
		statement = readRetainedMetadata
	}
	args := []any{r.Channel, r.From, r.Before, r.Limit}
	rows, err := conn.Prepared(statement).QueryContext(context.Background(), args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	r.Rows = make([]RecordMetadata, r.Limit)
	n := 0
	for rows.Next() {
		m := &r.Rows[n]
		fields := []any{&m.Offset, &m.Topic, &m.QoS, &m.Publisher, &m.Expires,
			&m.Properties, &m.PayloadSize, &m.StoredPayloadSize, &m.PublishedRetained, &m.ID, &m.Time, &m.Bridge}
		if r.Jobs {
			fields = append(fields, &m.Attempts, &m.Failed)
		}
		if err := rows.Scan(fields...); err != nil {
			return err
		}
		n++
	}
	r.Rows = r.Rows[:n]
	return finishRows(rows)
}

// ReadMetadataDispositions checks recipients after a common page has returned.
// The core can add covered owners while the metadata itself is reader-owned;
// this operation then owns the fixed cohort, keeping the same page credit.
type ReadMetadataDispositions struct {
	Page   *ReadRecordMetadata
	Checks []MetadataDisposition
}

// RecordRead needs no gate on later publications: these offsets are committed.
func (*ReadMetadataDispositions) RecordRead() bool { return true }

// ReadConn checks only the bounded keys from the common metadata page.
func (r *ReadMetadataDispositions) ReadConn(conn *Conn) error {
	page := r.Page
	if len(page.Rows) == 0 {
		return nil
	}
	var args [MetadataPage + 3]any
	args[0] = page.Channel
	for i := range page.Rows {
		args[i+3] = page.Rows[i].Offset
	}
	shape := bits.Len(uint(len(page.Rows) - 1))
	statement := readMetadataDisposition
	if shape != 0 {
		statement = readMetadataDispositions + Statement(shape) - 1
	}
	for i := range r.Checks {
		check := &r.Checks[i]
		args[1], args[2] = check.Session, check.Share
		rows, err := conn.Prepared(statement).QueryContext(context.Background(), args[:3+(1<<shape)]...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var offset int64
			if err := rows.Scan(&offset); err != nil {
				_ = rows.Close()
				return err
			}
			check.Settled = append(check.Settled, offset)
		}
		err = finishRows(rows)
		if err != nil {
			return err
		}
	}
	return nil
}

// Bytes is the logical charge used by retention and the hard size limits.
func (r *Record) Bytes() int64 {
	return int64(len(r.Topic) + max(len(r.Payload), r.PayloadSize) + max(len(r.Properties), r.PropertiesSize))
}

// ReadRecordPayload names one reserved delivery. Several deliveries may name
// the same offset; a batched read shares its unchanged bytes among their Into
// fields. Each delivery still owns its separate message-memory reservation.
type ReadRecordPayload struct {
	Channel, Offset int64
	Into            *[]byte
	Missing         bool // retention removed it before selection; never a second loss count
}

// ReadRecordPayloads reads at most one metadata page's distinct selected rows
// in one prepared statement. Its destinations belong only to reserved window
// entries, so no unreserved body is fetched or retained for a later consumer.
type ReadRecordPayloads struct {
	Channel int64
	offsets []int64
	rows    map[int64][]*ReadRecordPayload
}

// Add returns false when a new row would exceed the bound. Repeated offsets
// add destinations, bounded by their consumers' already-reserved windows.
func (b *ReadRecordPayloads) Add(r *ReadRecordPayload) bool {
	if b.rows == nil {
		b.Channel = r.Channel
		b.rows = make(map[int64][]*ReadRecordPayload)
	}
	if r.Channel != b.Channel {
		return false
	}
	if _, found := b.rows[r.Offset]; !found {
		if len(b.offsets) == MetadataPage {
			return false
		}
		b.offsets = append(b.offsets, r.Offset)
	}
	b.rows[r.Offset] = append(b.rows[r.Offset], r)
	return true
}

// RecordRead loads records already selected below committed boundaries.
func (*ReadRecordPayloads) RecordRead() bool { return true }

// ReadConn transfers each selected row once, sharing only immutable bodies.
func (b *ReadRecordPayloads) ReadConn(conn *Conn) error {
	var args [MetadataPage + 1]any
	args[0] = b.Channel
	for i, offset := range b.offsets {
		args[i+1] = offset
		for _, r := range b.rows[offset] {
			r.Missing, *r.Into = true, nil
		}
	}
	shape := bits.Len(uint(len(b.offsets) - 1))
	rows, err := conn.Prepared(readRecordPayloads+Statement(shape)).QueryContext(context.Background(), args[:1+(1<<shape)]...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var offset int64
		var stored sql.RawBytes
		var size int
		// RawBytes avoids database/sql's copy. The pinned go-sqlite3 driver
		// gives each BLOB its own slice, never reused after Next or Close;
		// database/sql's RawBytes contract alone does not promise that lifetime.
		if err := rows.Scan(&offset, &stored, &size); err != nil {
			return err
		}
		body, err := payload.Decode(stored, size)
		if err != nil {
			return err
		}
		for _, r := range b.rows[offset] {
			r.Missing, *r.Into = false, body
		}
	}
	return finishRows(rows)
}

const (
	recordColumns = 14
	// Larger statements offered little gain in the publication benchmark.
	// Only this full chunk and a single row are prepared. SQLite retains the
	// last bindings after Exec, so at most 17 records remain bound, rather
	// than 136 across all remainder shapes. Go scratch holds no bodies at rest.
	recordChunk = 16
)

// WriteRecords inserts records in bounded multi-row statements. A true second
// value ends a run: all its rows are written before the sequence resumes, so
// the caller may then make dependent changes such as retained replacements.
// Otherwise the sequence must not execute store operations, which could
// overtake buffered rows or reuse this transaction's scratch. The caller
// includes channel accounting in the same transaction. Any error rolls back
// the whole group as for a single-row insert. The result counts rows replaced
// by UPDATE; first values inserted do not contribute.
func (t *Tx) WriteRecords(records iter.Seq2[*Record, bool], queue bool) (int, error) {
	var replaced int
	args := t.recordArgs[:0]
	defer clear(t.recordArgs[:])
	if queue {
		defer clear(t.jobKeys[:])
	}
	for r, end := range records {
		if r.HeldPacket != 0 || (r.Retained && r.Channel != 1) {
			if len(args) != 0 {
				if err := t.flushRecordRun(args, queue); err != nil {
					return replaced, err
				}
				args = t.recordArgs[:0]
			}
			if r.HeldPacket != 0 {
				updated, err := r.writeHeld(t)
				if err != nil {
					return replaced, err
				}
				if updated {
					replaced++
				}
				if queue && r.Offset != 0 {
					if _, err := t.Prepared(addQueueJob).ExecContext(context.Background(), r.Channel, r.Offset); err != nil {
						return replaced, err
					}
				}
			} else {
				args = r.appendArgs(args)
				updated, err := t.updateRecord(updateLatestRecord, args)
				if err != nil {
					return replaced, err
				}
				if updated {
					replaced++
				} else {
					if err := t.flushRecordRun(args, queue); err != nil {
						return replaced, err
					}
				}
				args = t.recordArgs[:0]
			}
			continue
		}
		args = r.appendArgs(args)
		if queue {
			i := (len(args)/recordColumns - 1) * 2
			t.jobKeys[i], t.jobKeys[i+1] = args[len(args)-recordColumns], args[len(args)-recordColumns+1]
		}
		if len(args) == len(t.recordArgs) || end {
			if err := t.flushRecordRun(args, queue); err != nil {
				return replaced, err
			}
			args = t.recordArgs[:0]
		}
	}
	if len(args) == 0 {
		return replaced, nil
	}
	return replaced, t.flushRecordRun(args, queue)
}

// flushRecordRun writes the buffered rows and their parallel job keys before
// the producer resumes. Both buffers belong to the transaction, not the batch.
func (t *Tx) flushRecordRun(args []any, queue bool) error {
	if err := t.flushRows(AddRecord, addRecordChunk, recordColumns, args); err != nil {
		return err
	}
	if queue {
		return t.flushRows(addQueueJob, addQueueJobChunk, 2, t.jobKeys[:len(args)/recordColumns*2])
	}
	return nil
}

// updateRecord reports whether a current value or held source matched.
// Latest publications attempt replacement before considering insertion.
func (t *Tx) updateRecord(statement Statement, args []any) (bool, error) {
	result, err := t.Prepared(statement).ExecContext(context.Background(), args...)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n != 0, err
}

// flushRows finishes a full chunk or a run's remaining single rows before
// the producer resumes. Records and their job keys share this batching rule.
func (t *Tx) flushRows(single, chunk Statement, columns int, args []any) error {
	if len(args) == recordChunk*columns {
		_, err := t.Prepared(chunk).ExecContext(context.Background(), args...)
		return err
	}
	for len(args) > 0 {
		if _, err := t.Prepared(single).ExecContext(context.Background(), args[:columns]...); err != nil {
			return err
		}
		args = args[columns:]
	}
	return nil
}

// appendArgs binds concrete driver values to avoid per-row reflection and
// Valuer conversion, preserving NULL expiry/properties and non-null payloads.
func (r *Record) appendArgs(args []any) []any {
	var expires any
	if r.Expires.Valid {
		expires = r.Expires.Int64
	}
	return append(args, r.Channel, r.Offset, r.ID, r.Time, r.Topic, int64(r.QoS), r.Publisher, r.Bridge,
		expires, r.Properties, r.PublishedRetained, r.Retained, payload.Stored(r.Payload, r.StoredPayload), max(len(r.Payload), r.PayloadSize))
}

// AdvanceChannel raises the stored counter, never lowering it, to the next
// offset the core gives, and adds the bytes of records inserted with it. A
// failed transaction changes neither.
type AdvanceChannel struct {
	ID, Next, Bytes int64
}

// Records returns zero: the channel row is metadata.
func (*AdvanceChannel) Records() int { return 0 }

// WriteTx keeps channel metadata in the records' transaction.
func (a *AdvanceChannel) WriteTx(tx *Tx) error {
	_, err := tx.Prepared(advanceChannel).ExecContext(context.Background(), a.Next, a.Bytes, a.ID)
	return err
}
