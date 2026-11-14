package store

import (
	"context"
	"database/sql"
	"errors"
	"os"

	"github.com/ifnesi/saguin/internal/metrics"
)

// MetricCounts holds the store-only session counts at one read view. It is
// also loaded at startup, before the core starts its overdue-work passes.
type MetricCounts struct {
	Away, Subscriptions, Wills int64
}

func (m *MetricCounts) read(conn *Conn) error {
	return conn.Prepared(readMetricCounts).QueryRowContext(context.Background()).Scan(
		&m.Away, &m.Subscriptions, &m.Wills)
}

// MetricGauges are immutable registrations shared with the store reader.
// Non-durable subscriptions are supplied separately at the ordered boundary.
type MetricGauges struct {
	RetainedValues, RetainedBytes *metrics.Gauge
	Away, Subscriptions, Wills    *metrics.Gauge
}

// Set publishes the store counts, combined only with the non-durable count
// captured for this same request, never with core state at answer time.
func (m *MetricGauges) Set(counts MetricCounts, nonDurable int64) {
	m.Away.Set(counts.Away)
	m.Subscriptions.Set(counts.Subscriptions + nonDurable)
	m.Wills.Set(counts.Wills)
}

// MetricRefresh extends the existing paged channel read. The reader's one
// transaction supplies every SQL observation; Measure reuses the file-size
// observation used at startup and by Store.Measure.
type MetricRefresh struct {
	Channels   []RetentionPolicy // immutable registrations, in stored channel id order
	Gauges     MetricGauges
	NonDurable int64
	Measure    Read
}

// ReadConn keeps all pages and scalar aggregates within the established view.
// No rows or payloads accumulate in the core, and scrapes read only atomics.
func (r *MetricRefresh) ReadConn(conn *Conn) error {
	if err := r.readChannels(conn); err != nil {
		return err
	}
	var counts MetricCounts
	if err := counts.read(conn); err != nil {
		return err
	}
	var values, bytes int64
	var expiry, at sql.NullInt64
	if err := conn.Prepared(retainedTotals).QueryRowContext(context.Background()).Scan(
		&values, &bytes, &expiry, &at); err != nil {
		return err
	}
	r.Gauges.Set(counts, r.NonDurable)
	r.Gauges.RetainedValues.Set(values)
	r.Gauges.RetainedBytes.Set(bytes)
	return nil
}

// measureView observes file sizes before the reader releases later writes.
// Measuring after the SQL pages would mix this view with later commits' files.
func (r *MetricRefresh) measureView(conn *Conn) error { return r.Measure.ReadConn(conn) }

// walkMetricPositions streams scalar position metadata in primary-key order.
// Startup and refresh share this walk; neither retains a catalogue of sessions.
func walkMetricPositions(conn *Conn, visit func(channel, offset int64)) error {
	var session, channel int64
	for {
		rows, err := conn.Prepared(readMetricPositions).QueryContext(context.Background(), session, channel)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var offset int64
			if err := rows.Scan(&session, &channel, &offset); err != nil {
				_ = rows.Close()
				return err
			}
			n++
			visit(channel, offset)
		}
		err = finishRows(rows)
		if err != nil || n < MetadataPage {
			return err
		}
	}
}

// measurement holds immutable paths and atomic metric values, so any
// reader can refresh it while the writer continues committing.
type measurement struct {
	path, wal string
	bytes     *metrics.Gauge
	walBytes  *metrics.Gauge
}

// Measure returns a new read for the core's min_scrape_interval refresh.
// A scrape uses only the held atomics and never asks the store to do work.
func (s *Store) Measure() Read { return &measurementRead{values: &s.measurement} }

type measurementRead struct{ values *measurement }

func (m *measurementRead) ReadConn(_ *Conn) error { return m.values.read() }

func (m *measurement) read() error {
	db, err := os.Stat(m.path)
	if err != nil {
		return err
	}
	wal, err := storeFileSize(m.wal)
	if err != nil {
		return err
	}
	m.bytes.Set(db.Size())
	m.walBytes.Set(wal)
	return nil
}

func storeFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
