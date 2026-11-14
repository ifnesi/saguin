package support

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// WillPayload is the payload of the Wills these tests set.
var WillPayload = []byte("gone")

// StoredWill reads the Will of client that q's database holds, and whether
// it holds one (storage.md, wills: client, "the publisher's client id,
// captured when the Will was accepted").
func StoredWill(t *testing.T, q RowQuerier, client string) (Will, bool) {
	t.Helper()
	var w Will
	err := q.QueryRowContext(context.Background(), `SELECT session, "user", topic, qos, retain, delay, due, payload, properties
		FROM wills WHERE client = ?`, client).
		Scan(&w.Session, &w.User, &w.Topic, &w.Qos, &w.Retain, &w.Delay, &w.Due, &w.Payload, &w.Properties)
	if errors.Is(err, sql.ErrNoRows) {
		return Will{}, false
	}
	if err != nil {
		t.Fatalf("reading the Will of %q: %v", client, err)
	}
	return w, true
}

// Will is one row of storage.md's wills, as the operator reads it from the
// database: the Will a session holds.
type Will struct {
	Session             int64
	User, Topic         string
	Qos, Retain         int
	Delay               int64
	Due                 sql.NullInt64 // NULL while its client is connected
	Payload, Properties []byte        // properties nil where the column is NULL: none
}
