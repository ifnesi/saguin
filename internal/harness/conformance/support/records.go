package support

import (
	"database/sql"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// Acknowledged sends p from c at QoS 1 and fails the test unless its
// PUBACK is 0x00 (Success): design.md, Flows, PUBLISH, "1 | on PUBLISH |
// PUBACK once stored".
func Acknowledged(t *testing.T, c *harness.Client, p *harness.Publish) {
	t.Helper()
	Sent(t, c, p)
	Puback(t, c, p)
}

// Sent sends p from c at its QoS, under the next packet identifier where
// it has none, and returns when the sending began.
func Sent(t *testing.T, c *harness.Client, p *harness.Publish) time.Time {
	t.Helper()
	if p.ID == 0 {
		p.ID = c.NextID()
	}
	at := time.Now()
	if err := c.Send(p); err != nil {
		t.Fatalf("PUBLISH %q at QoS %d: %v", p.Topic, p.QoS, err)
	}
	return at
}

// Puback waits for c's PUBACK to p, sent before, fails the test unless it
// is 0x00 (Success), and returns when it was read.
func Puback(t *testing.T, c *harness.Client, p *harness.Publish) time.Time {
	t.Helper()
	return PubackOf(t, c, p, 0x00)
}

// Query runs q, with args, on db and hands each row it returns to scan,
// failing the test where the Query or a row fails.
func Query(t *testing.T, db RowsQuerier, q string, scan func(*sql.Rows) error, args ...any) {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("reading %s: %v", q, err)
	}
	defer func() { _ = rows.Close() }() // read whole, or the test has failed
	for rows.Next() {
		if err := scan(rows); err != nil {
			t.Fatalf("reading %s: %v", q, err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading %s: %v", q, err)
	}
}

// Collect runs q, with args, on db and returns each row it returns as scan
// reads it into a T, in order, failing the test where the query or a row
// fails.
func Collect[T any](t *testing.T, db RowsQuerier, q string, scan func(*sql.Rows, *T) error, args ...any) []T {
	t.Helper()
	var out []T
	Query(t, db, q, func(rows *sql.Rows) error {
		var v T
		err := scan(rows, &v)
		out = append(out, v)
		return err
	}, args...)
	return out
}

// PubackOf waits for c's PUBACK to p, sent before, fails the test unless it
// carries reason, and returns when it was read.
func PubackOf(t *testing.T, c *harness.Client, p *harness.Publish, reason byte) time.Time {
	t.Helper()
	return AnswerOf(t, c, harness.PUBACK, p, reason)
}

// RowsQuerier is a database (*sql.DB) or one transaction on it (*sql.Tx),
// whose queries then all read the same snapshot.
type RowsQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// AnswerOf waits for c's acknowledgement of kind to p, sent before, fails
// the test unless it carries reason, and returns when it was read.
func AnswerOf(t *testing.T, c *harness.Client, kind byte, p *harness.Publish, reason byte) time.Time {
	t.Helper()
	ack, err := c.AwaitAck(kind, p.ID)
	if err != nil {
		t.Fatalf("PUBLISH %q at QoS %d: %v", p.Topic, p.QoS, err)
	}
	if ack.Reason != reason {
		t.Fatalf("PUBLISH %q of %d bytes at QoS %d, RETAIN %v, DUP %v: %s 0x%02X, want 0x%02X",
			p.Topic, len(p.Payload), p.QoS, p.Retain, p.Dup, harness.Name(kind), ack.Reason, reason)
	}
	return time.Now()
}
