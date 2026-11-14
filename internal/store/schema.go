package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const schemaVersion = 21

type schemaObject struct {
	name string
	sql  string
}

// schema is storage.md, version 21. Keeping each catalogue definition here
// lets startup check the schema without touching the tables it does not load.
// No foreign keys or extra indexes are added to the agreed schema.
var schema = [...]schemaObject{
	{"channels", `CREATE TABLE channels (
		id INTEGER NOT NULL PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		type INTEGER NOT NULL,
		next INTEGER NOT NULL,
		floor INTEGER NOT NULL,
		bytes INTEGER NOT NULL,
		CHECK (id >= 1 AND type IN (1, 2, 3) AND next >= 1 AND floor >= 1
			AND floor <= next AND bytes >= 0 AND (id <> 1 OR (name = '$broadcast' AND type = 1)))
	) STRICT`},
	{"records", `CREATE TABLE records (
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		id TEXT NOT NULL,
		time INTEGER NOT NULL,
		topic TEXT NOT NULL,
		qos INTEGER NOT NULL,
		publisher TEXT NOT NULL,
		bridge INTEGER NOT NULL,
		expires INTEGER,
		properties BLOB,
		published_retained INTEGER NOT NULL,
		retained INTEGER NOT NULL,
		payload BLOB NOT NULL,
		payload_size INTEGER NOT NULL,
		PRIMARY KEY (channel, "offset"),
		CHECK (length(payload) <= payload_size),
		CHECK (channel >= 1 AND "offset" >= 1 AND qos IN (0, 1, 2) AND bridge >= 0
			AND published_retained IN (0, 1) AND retained IN (0, 1)
			AND (channel = 1 OR published_retained = 0)
			AND (retained = 0 OR channel <> 1 OR (published_retained = 1 AND length(payload) > 0)))
	) STRICT`},
	{"records_retained", `CREATE UNIQUE INDEX records_retained ON records (channel, topic) WHERE retained = 1`},
	{"queue_jobs", `CREATE TABLE queue_jobs (
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		spent INTEGER NOT NULL,
		failed INTEGER NOT NULL,
		first_attempt INTEGER,
		last_attempt INTEGER,
		next_try INTEGER,
		PRIMARY KEY (channel, "offset"),
		CHECK (channel >= 1 AND "offset" >= 1 AND spent >= 0 AND failed >= 0)
	) STRICT, WITHOUT ROWID`},
	{"queue_jobs_retry", `CREATE INDEX queue_jobs_retry ON queue_jobs (channel, next_try, "offset") WHERE next_try IS NOT NULL`},
	{"sessions", `CREATE TABLE sessions (
		id INTEGER NOT NULL PRIMARY KEY,
		client TEXT NOT NULL UNIQUE,
		user TEXT NOT NULL,
		protocol INTEGER NOT NULL,
		expiry INTEGER NOT NULL,
		left INTEGER NOT NULL,
		receive_maximum INTEGER NOT NULL,
		listener TEXT NOT NULL,
		remote TEXT NOT NULL,
		keepalive INTEGER NOT NULL,
		clean_start INTEGER NOT NULL,
		CHECK (id >= 1 AND protocol IN (4, 5) AND expiry >= 1 AND left >= 0
			AND receive_maximum BETWEEN 1 AND 65535 AND keepalive BETWEEN 0 AND 65535
			AND clean_start IN (0, 1))
	) STRICT`},
	{"sessions_expiry", fmt.Sprintf(`CREATE INDEX sessions_expiry ON sessions (left + expiry * 1000) WHERE left > 0 AND expiry <> %d`, NeverExpires)},
	{"subscriptions", `CREATE TABLE subscriptions (
		session INTEGER NOT NULL,
		filter TEXT NOT NULL,
		qos INTEGER NOT NULL,
		no_local INTEGER NOT NULL,
		retain_as_published INTEGER NOT NULL,
		retain_handling INTEGER NOT NULL,
		identifier INTEGER NOT NULL,
		partitions TEXT NOT NULL,
		PRIMARY KEY (session, filter),
		CHECK (session >= 1 AND qos IN (0, 1, 2) AND no_local IN (0, 1)
			AND retain_as_published IN (0, 1) AND retain_handling IN (0, 1, 2)
			AND identifier BETWEEN 0 AND 268435455)
	) STRICT, WITHOUT ROWID`},
	{"subscription_starts", `CREATE TABLE subscription_starts (
		session INTEGER NOT NULL,
		filter TEXT NOT NULL,
		channel INTEGER NOT NULL,
		start INTEGER NOT NULL,
		PRIMARY KEY (session, filter, channel),
		CHECK (session >= 1 AND channel >= 1 AND start >= 1)
	) STRICT, WITHOUT ROWID`},
	{"wills", `CREATE TABLE wills (
		session INTEGER NOT NULL PRIMARY KEY,
		client TEXT NOT NULL,
		user TEXT NOT NULL,
		topic TEXT NOT NULL,
		qos INTEGER NOT NULL,
		retain INTEGER NOT NULL,
		delay INTEGER NOT NULL,
		due INTEGER,
		payload BLOB NOT NULL,
		payload_size INTEGER NOT NULL,
		properties BLOB,
		CHECK (length(payload) <= payload_size),
		CHECK (session >= 1 AND qos IN (0, 1, 2) AND retain IN (0, 1) AND delay >= 0)
	) STRICT`},
	{"wills_due", `CREATE INDEX wills_due ON wills (due) WHERE due IS NOT NULL`},
	{"positions", `CREATE TABLE positions (
		session INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		last_seen INTEGER,
		PRIMARY KEY (session, channel),
		CHECK (session >= 1 AND channel >= 1 AND "offset" >= 1)
	) STRICT, WITHOUT ROWID`},
	{"session_inflight", `CREATE TABLE session_inflight (
		session INTEGER NOT NULL,
		packet_id INTEGER NOT NULL,
		seq INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		qos INTEGER NOT NULL,
		phase INTEGER NOT NULL,
		moves INTEGER NOT NULL,
		share INTEGER NOT NULL,
		attempt TEXT,
		attempt_number INTEGER,
		attempt_began INTEGER,
		out_retain INTEGER NOT NULL,
		out_initial INTEGER NOT NULL,
		out_properties BLOB,
		PRIMARY KEY (session, packet_id),
		CHECK (session >= 1 AND packet_id BETWEEN 1 AND 65535 AND seq >= 1
			AND channel >= 1 AND "offset" >= 1 AND qos IN (1, 2) AND phase IN (1, 2, 3)
			AND (phase <> 2 OR qos = 2) AND moves IN (0, 1) AND share >= 0
			AND (attempt IS NULL) = (attempt_number IS NULL)
			AND (attempt IS NULL) = (attempt_began IS NULL)
			AND (attempt_number IS NULL OR attempt_number >= 1) AND out_retain IN (0, 1)
			AND out_initial IN (0, 1))
	) STRICT, WITHOUT ROWID`},
	{"session_inflight_shared", `CREATE INDEX session_inflight_shared ON session_inflight (share, channel, "offset") WHERE share > 0`},
	{"held_publishes", `CREATE TABLE held_publishes (
		session INTEGER NOT NULL,
		bridge INTEGER NOT NULL,
		packet_id INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		id TEXT NOT NULL,
		time INTEGER NOT NULL,
		topic TEXT NOT NULL,
		publisher TEXT NOT NULL,
		expires INTEGER,
		payload BLOB NOT NULL,
		payload_size INTEGER NOT NULL,
		properties BLOB,
		retain INTEGER NOT NULL,
		PRIMARY KEY (session, bridge, packet_id),
		CHECK (length(payload) <= payload_size),
		CHECK ((session = 0) <> (bridge = 0) AND session >= 0 AND bridge >= 0
			AND packet_id BETWEEN 1 AND 65535 AND channel >= 1 AND retain IN (0, 1))
	) STRICT`},
	{"held_publishes_retained", `CREATE INDEX held_publishes_retained ON held_publishes (topic) WHERE channel = 1 AND retain = 1`},
	{"share_groups", `CREATE TABLE share_groups (
		id INTEGER NOT NULL PRIMARY KEY,
		share TEXT NOT NULL UNIQUE,
		last_selected TEXT,
		CHECK (id >= 1 AND substr(share, 1, 7) = '$share/')
	) STRICT`},
	{"share_cursors", `CREATE TABLE share_cursors (
		share INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		PRIMARY KEY (share, channel),
		CHECK (share >= 1 AND channel >= 1 AND "offset" >= 1)
	) STRICT, WITHOUT ROWID`},
	{"share_returned", `CREATE TABLE share_returned (
		share INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		state INTEGER NOT NULL,
		session INTEGER,
		qos INTEGER,
		PRIMARY KEY (share, channel, "offset"),
		CHECK (share >= 1 AND channel >= 1 AND "offset" >= 1 AND state IN (1, 2, 3)
			AND (state = 2) = (session IS NOT NULL) AND (state = 2) = (qos IS NOT NULL)
			AND (qos IS NULL OR qos IN (1, 2)))
	) STRICT, WITHOUT ROWID`},
	{"bridges", `CREATE TABLE bridges (
		id INTEGER NOT NULL PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		replay_owed INTEGER NOT NULL,
		CHECK (id >= 1 AND replay_owed IN (0, 1))
	) STRICT`},
	{"bridge_rules", `CREATE TABLE bridge_rules (
		id INTEGER NOT NULL PRIMARY KEY,
		bridge INTEGER NOT NULL,
		filter TEXT NOT NULL,
		topic TEXT NOT NULL,
		UNIQUE (bridge, filter, topic),
		CHECK (id >= 1 AND bridge >= 1)
	) STRICT`},
	{"bridge_cursors", `CREATE TABLE bridge_cursors (
		rule INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		last_seen INTEGER,
		PRIMARY KEY (rule, channel),
		CHECK (rule >= 1 AND channel >= 1 AND "offset" >= 1)
	) STRICT, WITHOUT ROWID`},
	{"bridge_inflight", `CREATE TABLE bridge_inflight (
		bridge INTEGER NOT NULL,
		packet_id INTEGER NOT NULL,
		seq INTEGER NOT NULL,
		channel INTEGER NOT NULL,
		"offset" INTEGER NOT NULL,
		qos INTEGER NOT NULL,
		phase INTEGER NOT NULL,
		rule INTEGER NOT NULL,
		PRIMARY KEY (bridge, packet_id),
		CHECK (bridge >= 1 AND packet_id BETWEEN 1 AND 65535 AND seq >= 1
			AND rule >= 1 AND channel >= 1 AND "offset" >= 1 AND qos IN (1, 2)
			AND phase IN (1, 2) AND (phase = 1 OR qos = 2))
	) STRICT, WITHOUT ROWID`},
}

// createSchema writes all objects, the reserved $broadcast channel and
// user_version atomically. Startup transactions do not count in the
// writer's request commit series.
func createSchema(conn *sql.Conn) error {
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, object := range &schema {
		if _, err := tx.ExecContext(context.Background(), object.sql); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(context.Background(), statementSQL[AddChannel].sql,
		1, "$broadcast", Append, 1, 1, 0); err != nil {
		return err
	}
	if _, err := tx.ExecContext(context.Background(), fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

// checkSchema reads the catalogue, not the records' pages: damage beyond
// startup's reads is reported by the operation that first touches it.
func checkSchema(conn *sql.Conn) error {
	rows, err := conn.QueryContext(context.Background(), `SELECT name, sql FROM sqlite_schema WHERE sql IS NOT NULL`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	want := make(map[string]string, len(schema))
	for _, object := range &schema {
		want[object.name] = strings.Join(strings.Fields(object.sql), " ")
	}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return err
		}
		if expected, ok := want[name]; !ok || strings.Join(strings.Fields(definition), " ") != expected {
			return fmt.Errorf("schema version %d: unexpected definition of %s", schemaVersion, name)
		}
		delete(want, name)
	}
	if err := finishRows(rows); err != nil {
		return err
	}
	if len(want) != 0 {
		return fmt.Errorf("schema version %d: %d missing objects", schemaVersion, len(want))
	}
	return nil
}
