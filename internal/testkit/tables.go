package testkit

import (
	"database/sql"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The tables of docs/storage.md as test data: what a database Sagüin
// creates must hold, column for column, and rows each table must take or
// refuse. The tests that read a database file hold it to these.

// The column types storage.md declares.
const (
	integerType = "INTEGER"
	textType    = "TEXT"
	blobType    = "BLOB"
)

// Column is one column of a table as storage.md gives it.
type Column struct {
	Name     string
	Type     string // the declared type, which a STRICT table enforces
	Nullable bool   // storage.md: "every column is NOT NULL unless its row says nullable"
	PK       int    // its place in the primary key, from 1; 0 for none
}

// Row is a row of a table, by column name.
type Row map[string]any

// Table is one table of storage.md.
type Table struct {
	Name         string
	WithoutRowID bool
	Columns      []Column
	// Unique lists the table's indexes besides its primary key's, each as
	// its columns joined by commas, " partial" ending a partial index, "an
	// expression" standing for a column that is one, and "index on"
	// beginning one that is not unique.
	Unique []string
	// Valid holds rows the table must take: the first is the base row every
	// probe starts from, each other one a change to it.
	Valid []Row
	// Refused holds changes to the base row that a CHECK must refuse.
	Refused []Row
}

// Tables is every table of storage.md, version 21, in its order. records,
// wills and held_publishes hold payloads: storage.md, A stored payload,
// "Every table holding a payload has payload_size, an INTEGER: the
// payload's size as received. [...] Each such table enforces CHECK
// (length(payload) <= payload_size)".
var Tables = []Table{
	{
		Name: "channels",
		Columns: []Column{
			{"id", integerType, false, 1},
			{"name", textType, false, 0},
			{"type", integerType, false, 0},
			{"next", integerType, false, 0},
			{"floor", integerType, false, 0},
			{"bytes", integerType, false, 0},
		},
		Unique: []string{"name"},
		Valid: []Row{
			{"id": 2, "name": "orders", "type": 3, "next": 5, "floor": 2, "bytes": 10},
			{"id": 1, "name": "$broadcast", "type": 1},
			{"type": 1}, {"type": 2},
			{"floor": 5}, // floor = next
			{"next": 1, "floor": 1},
			{"bytes": 0},
		},
		Refused: []Row{
			{"id": 0},
			{"type": 0}, {"type": 4},
			{"next": 0},
			{"floor": 0},
			{"floor": 6}, // floor > next
			{"bytes": -1},
			{"id": 1, "name": "orders", "type": 1}, // channel 1 is $broadcast
			{"id": 1, "name": "$broadcast", "type": 3}, // $broadcast is append
		},
	},
	{
		Name: "records",
		Columns: []Column{
			{"channel", integerType, false, 1},
			{"offset", integerType, false, 2},
			{"id", textType, false, 0},
			{"time", integerType, false, 0},
			{"topic", textType, false, 0},
			{"qos", integerType, false, 0},
			{"publisher", textType, false, 0},
			{"bridge", integerType, false, 0},
			{"expires", integerType, true, 0},
			{"properties", blobType, true, 0},
			{"published_retained", integerType, false, 0},
			{"retained", integerType, false, 0},
			{"payload", blobType, false, 0},
			{"payload_size", integerType, false, 0},
		},
		Unique: []string{"channel,topic partial"},
		Valid: []Row{
			{
				"channel": 2, "offset": 1, "id": "0192f0c2-7d1e-7000-8000-000000000001", "time": 1760000000000,
				"topic": "orders/a", "qos": 1, "publisher": "c1", "bridge": 0, "expires": nil,
				"payload": []byte("x"), "payload_size": 1, "properties": nil, "published_retained": 0, "retained": 0,
			},
			{"qos": 0}, {"qos": 2},
			{"publisher": "", "bridge": 3}, // arrived over a bridge
			{"expires": 1760000060000, "properties": []byte{0}},
			{"payload": []byte{}, "payload_size": 0},                // an empty payload
			{"payload_size": 5000},                                  // a payload stored compressed, shorter than its size as received
			{"retained": 1},                                         // a latest channel's record: its topic's current value
			{"retained": 1, "payload": []byte{}, "payload_size": 0}, // a latest tombstone, its topic's current record
			{"channel": 1, "published_retained": 1, "retained": 1},
			{"channel": 1, "published_retained": 1, "retained": 0},
			{"channel": 1}, // a $broadcast record that was not retained
		},
		Refused: []Row{
			{"channel": 0},
			{"offset": 0},
			{"qos": -1}, {"qos": 3},
			{"bridge": -1},
			{"channel": 1, "published_retained": 2},
			{"channel": 1, "published_retained": -1},
			{"channel": 1, "retained": -1},
			{"channel": 1, "published_retained": 1, "retained": 2},
			{"published_retained": 1},                                                   // published retained is $broadcast's only
			{"published_retained": 1, "retained": 1},                                    // likewise
			{"channel": 1, "retained": 1},                                               // a current retained value was published retained
			{"channel": 1, "published_retained": 1, "retained": 1, "payload": []byte{}}, // and is not empty
			{"payload_size": 0},                                                         // a stored payload is never longer than its size as received
		},
	},
	{
		Name:         "queue_jobs",
		WithoutRowID: true,
		Columns: []Column{
			{"channel", integerType, false, 1},
			{"offset", integerType, false, 2},
			{"spent", integerType, false, 0},
			{"failed", integerType, false, 0},
			{"first_attempt", integerType, true, 0},
			{"last_attempt", integerType, true, 0},
			{"next_try", integerType, true, 0},
		},
		// queue_jobs_retry: storage.md, "The index queue_jobs_retry is on
		// (channel, next_try, offset) WHERE next_try IS NOT NULL."
		Unique: []string{"index on channel,next_try,offset partial"},
		Valid: []Row{
			{"channel": 4, "offset": 1, "spent": 0, "failed": 0, "first_attempt": nil, "last_attempt": nil, "next_try": nil},
			{"spent": 2, "failed": 1, "first_attempt": 1, "last_attempt": 2, "next_try": 3},
			{"channel": 1},
		},
		Refused: []Row{{"channel": 0}, {"offset": 0}, {"spent": -1}, {"failed": -1}},
	},
	{
		Name: "sessions",
		Columns: []Column{
			{"id", integerType, false, 1},
			{"client", textType, false, 0},
			{"user", textType, false, 0},
			{"protocol", integerType, false, 0},
			{"expiry", integerType, false, 0},
			{"left", integerType, false, 0},
			{"receive_maximum", integerType, false, 0},
			{"listener", textType, false, 0},
			{"remote", textType, false, 0},
			{"keepalive", integerType, false, 0},
			{"clean_start", integerType, false, 0},
		},
		// sessions_expiry: the away sessions by the instant their expiry
		// runs out, an expiry that never runs out left out.
		Unique: []string{"client", "index on an expression partial"},
		Valid: []Row{
			{
				"id": 1, "client": "c1", "user": "", "protocol": 5, "expiry": 60, "left": 0,
				"receive_maximum": 20, "listener": "mqtt", "remote": "10.0.0.1:50000", "keepalive": 60, "clean_start": 0,
			},
			{"protocol": 4},
			{"expiry": 1},
			{"left": 1760000000000},
			{"receive_maximum": 1}, {"receive_maximum": 65535},
			{"keepalive": 0}, {"keepalive": 65535},
			{"clean_start": 1},
		},
		Refused: []Row{
			{"id": 0},
			{"protocol": 3}, {"protocol": 6},
			{"expiry": 0},
			{"left": -1},
			{"receive_maximum": 0}, {"receive_maximum": 65536},
			{"keepalive": -1}, {"keepalive": 65536},
			{"clean_start": 2},
		},
	},
	{
		Name:         "subscriptions",
		WithoutRowID: true,
		Columns: []Column{
			{"session", integerType, false, 1},
			{"filter", textType, false, 2},
			{"qos", integerType, false, 0},
			{"no_local", integerType, false, 0},
			{"retain_as_published", integerType, false, 0},
			{"retain_handling", integerType, false, 0},
			{"identifier", integerType, false, 0},
			{"partitions", textType, false, 0},
		},
		Valid: []Row{
			{
				"session": 1, "filter": "orders/#", "qos": 1, "no_local": 0, "retain_as_published": 0,
				"retain_handling": 0, "identifier": 0, "partitions": "",
			},
			{"filter": "$share/g/orders/#", "qos": 2, "no_local": 1, "retain_as_published": 1, "retain_handling": 2},
			{"identifier": 268435455, "partitions": "topic_hash(4, 1)"},
			{"retain_handling": 1},
		},
		Refused: []Row{
			{"session": 0},
			{"qos": -1}, {"qos": 3},
			{"no_local": -1}, {"no_local": 2},
			{"retain_as_published": -1}, {"retain_as_published": 2},
			{"retain_handling": -1}, {"retain_handling": 3},
			{"identifier": -1}, {"identifier": 268435456},
		},
	},
	{
		Name:         "subscription_starts",
		WithoutRowID: true,
		Columns: []Column{
			{"session", integerType, false, 1},
			{"filter", textType, false, 2},
			{"channel", integerType, false, 3},
			{"start", integerType, false, 0},
		},
		Valid: []Row{
			{"session": 1, "filter": "orders/#", "channel": 2, "start": 1},
			{"filter": "$share/g/orders/#", "channel": 1},
			{"start": 42}, // the channel's tail when the core accepted the subscription
		},
		Refused: []Row{{"session": 0}, {"channel": 0}, {"start": 0}},
	},
	{
		Name: "wills",
		Columns: []Column{
			{"session", integerType, false, 1},
			{"client", textType, false, 0},
			{"user", textType, false, 0},
			{"topic", textType, false, 0},
			{"qos", integerType, false, 0},
			{"retain", integerType, false, 0},
			{"delay", integerType, false, 0},
			{"due", integerType, true, 0},
			{"payload", blobType, false, 0},
			{"payload_size", integerType, false, 0},
			{"properties", blobType, true, 0},
		},
		// wills_due: the Wills of gone clients by the instant they are
		// due, a connected client's left out.
		Unique: []string{"index on due partial"},
		Valid: []Row{
			{
				"session": 1, "client": "c1", "user": "", "topic": "status/c1", "qos": 0, "retain": 0, "delay": 0,
				"due": nil, "payload": []byte{}, "payload_size": 0, "properties": nil,
			},
			{"qos": 2, "retain": 1, "delay": 30, "due": 1760000030000, "properties": []byte{0}},
			{"qos": 1},
			{"user": "alice"}, // the proved user name captured when the Will was accepted
			{"payload": []byte("w"), "payload_size": 1},    // a Will Payload stored as received
			{"payload": []byte("w"), "payload_size": 5000}, // and one stored compressed
		},
		Refused: []Row{
			{"session": 0}, {"qos": -1}, {"qos": 3}, {"retain": -1}, {"retain": 2}, {"delay": -1},
			{"payload": []byte("w")}, // a stored payload is never longer than its size as received
		},
	},
	{
		Name:         "positions",
		WithoutRowID: true,
		Columns: []Column{
			{"session", integerType, false, 1},
			{"channel", integerType, false, 2},
			{"offset", integerType, false, 0},
			{"last_seen", integerType, true, 0},
		},
		Valid: []Row{
			{"session": 1, "channel": 2, "offset": 1, "last_seen": nil},
			{"last_seen": 1760000000000},
			{"channel": 1},
		},
		Refused: []Row{{"session": 0}, {"channel": 0}, {"offset": 0}},
	},
	{
		Name:         "session_inflight",
		WithoutRowID: true,
		Columns: []Column{
			{"session", integerType, false, 1},
			{"packet_id", integerType, false, 2},
			{"seq", integerType, false, 0},
			{"channel", integerType, false, 0},
			{"offset", integerType, false, 0},
			{"qos", integerType, false, 0},
			{"phase", integerType, false, 0},
			{"moves", integerType, false, 0},
			{"share", integerType, false, 0},
			{"attempt", textType, true, 0},
			{"attempt_number", integerType, true, 0},
			{"attempt_began", integerType, true, 0},
			{"out_retain", integerType, false, 0},
			{"out_initial", integerType, false, 0},
			{"out_properties", blobType, true, 0},
		},
		// session_inflight_shared: storage.md, session_inflight, "The index
		// session_inflight_shared is on (share, channel, offset) WHERE
		// share > 0."
		Unique: []string{"index on share,channel,offset partial"},
		Valid: []Row{
			{
				"session": 1, "packet_id": 1, "seq": 1, "channel": 2, "offset": 1, "qos": 1, "phase": 1,
				"moves": 1, "share": 0, "attempt": nil, "attempt_number": nil, "attempt_began": nil,
				"out_retain": 0, "out_initial": 0, "out_properties": nil,
			},
			{"out_retain": 1, "out_properties": []byte{0}}, // the first delivery's RETAIN and delivery properties
			{"out_initial": 1}, // the first delivery belonged to a subscription's initial pass
			{"moves": 0},       // a seek on its channel has passed it
			{"qos": 2, "phase": 2, "moves": 0},
			{"packet_id": 65535},
			{"qos": 2, "phase": 2},
			{"qos": 2}, // PUBLISH sent at QoS 2
			{"channel": 1},
			{"share": 3},
			{"attempt": "a1", "attempt_number": 1, "attempt_began": 1760000000000},
			// storage.md, session_inflight: phase "3 a settled inherited
			// exchange of a session continuing with Session Expiry 0", at
			// either QoS, the CHECK holding "phase IN (1, 2, 3) AND (phase <>
			// 2 OR qos = 2)"; moves "0 after a seek or saved progress
			// supersedes it".
			{"phase": 3},
			{"qos": 2, "phase": 3},
			{"phase": 3, "moves": 0},
		},
		Refused: []Row{
			{"session": 0},
			{"packet_id": 0}, {"packet_id": 65536},
			{"seq": 0},
			{"channel": 0},
			{"offset": 0},
			{"qos": 0}, {"qos": 3},
			{"phase": 0}, {"phase": 4},
			{"phase": 2}, // PUBREL is owed only at QoS 2
			{"moves": -1}, {"moves": 2},
			{"share": -1},
			{"attempt": "a1", "attempt_number": nil, "attempt_began": 1}, // an attempt has its number
			{"attempt": "a1", "attempt_number": 1, "attempt_began": nil}, // and its beginning
			{"attempt": nil, "attempt_number": 1, "attempt_began": nil},  // and none without one
			{"attempt": "a1", "attempt_number": 0, "attempt_began": 1},   // numbered from 1
			{"out_retain": -1}, {"out_retain": 2},
			{"out_initial": -1}, {"out_initial": 2},
		},
	},
	{
		Name: "held_publishes",
		Columns: []Column{
			{"session", integerType, false, 1},
			{"bridge", integerType, false, 2},
			{"packet_id", integerType, false, 3},
			{"channel", integerType, false, 0},
			{"id", textType, false, 0},
			{"time", integerType, false, 0},
			{"topic", textType, false, 0},
			{"publisher", textType, false, 0},
			{"expires", integerType, true, 0},
			{"payload", blobType, false, 0},
			{"payload_size", integerType, false, 0},
			{"properties", blobType, true, 0},
			{"retain", integerType, false, 0},
		},
		// held_publishes_retained: storage.md, "The index
		// held_publishes_retained is on (topic) WHERE channel = 1 AND
		// retain = 1."
		Unique: []string{"index on topic partial"},
		Valid: []Row{
			{
				"session": 1, "bridge": 0, "packet_id": 1, "channel": 2, "id": "0192f0c2-7d1e-7000-8000-000000000002",
				"time": 1760000000000, "topic": "orders/a", "publisher": "c1", "expires": nil,
				"payload": []byte("x"), "payload_size": 1, "properties": nil, "retain": 0,
			},
			{"session": 0, "bridge": 2, "publisher": ""}, // from a bridge's peer
			{"packet_id": 65535, "retain": 1, "expires": 1760000060000, "properties": []byte{0}},
			{"channel": 1},
			{"payload_size": 5000}, // a payload stored compressed, shorter than its size as received
		},
		Refused: []Row{
			{"session": 0, "bridge": 0}, // from a session or a bridge
			{"session": 1, "bridge": 2}, // not both
			{"session": -1},
			{"session": 0, "bridge": -1},
			{"packet_id": 0}, {"packet_id": 65536},
			{"channel": 0},
			{"retain": -1}, {"retain": 2},
			{"payload_size": 0}, // a stored payload is never longer than its size as received
		},
	},
	{
		Name: "share_groups",
		Columns: []Column{
			{"id", integerType, false, 1},
			{"share", textType, false, 0},
			{"last_selected", textType, true, 0},
		},
		Unique: []string{"share"},
		Valid: []Row{
			{"id": 1, "share": "$share/g/orders/#", "last_selected": nil}, // no selection yet
			{"share": "$share/g/+"},
			{"last_selected": "c1"}, // the client id of the member it last selected
		},
		Refused: []Row{{"id": 0}, {"share": "g/orders/#"}, {"share": "$SHARE/g/orders/#"}, {"share": "$share"}},
	},
	{
		Name:         "share_cursors",
		WithoutRowID: true,
		Columns: []Column{
			{"share", integerType, false, 1},
			{"channel", integerType, false, 2},
			{"offset", integerType, false, 0},
		},
		Valid:   []Row{{"share": 1, "channel": 2, "offset": 1}, {"channel": 1}},
		Refused: []Row{{"share": 0}, {"channel": 0}, {"offset": 0}},
	},
	{
		Name:         "share_returned",
		WithoutRowID: true,
		Columns: []Column{
			{"share", integerType, false, 1},
			{"channel", integerType, false, 2},
			{"offset", integerType, false, 3},
			{"state", integerType, false, 0},
			{"session", integerType, true, 0},
			{"qos", integerType, true, 0},
		},
		// storage.md, share_returned: state "1 returned, 2 assigned to a
		// volatile member, 3 terminal"; session and qos "NULL unless
		// assigned"; the CHECK holding "state IN (1, 2, 3) AND (state = 2) =
		// (session IS NOT NULL) AND (state = 2) = (qos IS NOT NULL) AND (qos
		// IS NULL OR qos IN (1, 2))".
		Valid: []Row{
			{"share": 1, "channel": 2, "offset": 1, "state": 1, "session": nil, "qos": nil},
			{"channel": 1},
			{"state": 3},                         // terminal evidence ahead of the cursor
			{"state": 2, "session": 1, "qos": 1}, // a volatile member's QoS 1 assignment
			{"state": 2, "session": 7, "qos": 2}, // and its QoS 2 one
		},
		Refused: []Row{
			{"share": 0}, {"channel": 0}, {"offset": 0},
			{"state": 0}, {"state": 4},
			{"state": 2},                           // an assignment names its member and QoS
			{"state": 2, "session": 1, "qos": nil}, // its QoS too
			{"state": 2, "session": nil, "qos": 1}, // its member too
			{"session": 1, "qos": 1},               // a returned row names neither
			{"state": 3, "session": 1, "qos": nil}, // nor a terminal one
			{"state": 3, "session": nil, "qos": 2},
			{"state": 2, "session": 1, "qos": 0}, // QoS 0 is never assigned
			{"state": 2, "session": 1, "qos": 3},
		},
	},
	{
		Name: "bridges",
		Columns: []Column{
			{"id", integerType, false, 1},
			{"name", textType, false, 0},
			{"replay_owed", integerType, false, 0},
		},
		// storage.md, bridges: replay_owed "1 from a CONNACK with Session
		// Present 0 until its replay commits (Bridges), else 0"; the CHECK
		// holding "id >= 1 AND replay_owed IN (0, 1)".
		Unique:  []string{"name"},
		Valid:   []Row{{"id": 1, "name": "upstream", "replay_owed": 0}, {"replay_owed": 1}},
		Refused: []Row{{"id": 0}, {"replay_owed": 2}, {"replay_owed": -1}},
	},
	{
		Name: "bridge_rules",
		Columns: []Column{
			{"id", integerType, false, 1},
			{"bridge", integerType, false, 0},
			{"filter", textType, false, 0},
			{"topic", textType, false, 0},
		},
		Unique: []string{"bridge,filter,topic"},
		Valid: []Row{
			{"id": 1, "bridge": 1, "filter": "orders/#", "topic": ""},
			{"topic": "site-1/$#"},
		},
		Refused: []Row{{"id": 0}, {"bridge": 0}},
	},
	{
		Name:         "bridge_cursors",
		WithoutRowID: true,
		Columns: []Column{
			{"rule", integerType, false, 1},
			{"channel", integerType, false, 2},
			{"offset", integerType, false, 0},
			{"last_seen", integerType, true, 0},
		},
		Valid: []Row{
			{"rule": 1, "channel": 2, "offset": 1, "last_seen": nil},
			{"last_seen": 1760000000000},
			{"channel": 1},
		},
		Refused: []Row{{"rule": 0}, {"channel": 0}, {"offset": 0}},
	},
	{
		Name:         "bridge_inflight",
		WithoutRowID: true,
		Columns: []Column{
			{"bridge", integerType, false, 1},
			{"packet_id", integerType, false, 2},
			{"seq", integerType, false, 0},
			{"channel", integerType, false, 0},
			{"offset", integerType, false, 0},
			{"qos", integerType, false, 0},
			{"phase", integerType, false, 0},
			{"rule", integerType, false, 0},
		},
		Valid: []Row{
			{"bridge": 1, "packet_id": 1, "seq": 1, "channel": 2, "offset": 1, "qos": 1, "phase": 1, "rule": 1},
			{"packet_id": 65535, "qos": 2, "phase": 2},
			{"qos": 2}, // PUBLISH sent at QoS 2
			{"channel": 1},
		},
		Refused: []Row{
			{"bridge": 0},
			{"packet_id": 0}, {"packet_id": 65536},
			{"seq": 0},
			{"channel": 0},
			{"offset": 0},
			{"qos": 0}, {"qos": 3},
			{"phase": 0}, {"phase": 3},
			{"phase": 2}, // PUBREL is owed only at QoS 2
			{"rule": 0},
		},
	},
}

// TableNamed returns the table of Tables called name.
func TableNamed(t testing.TB, name string) *Table {
	t.Helper()
	i := slices.IndexFunc(Tables, func(table Table) bool { return table.Name == name })
	if i < 0 {
		t.Fatalf("storage.md has no table %s", name)
	}
	return &Tables[i]
}

// PrimaryKey lists the columns of table's primary key.
func (table *Table) PrimaryKey() []string {
	var names []string
	for _, c := range table.Columns {
		if c.PK > 0 {
			names = append(names, c.Name)
		}
	}
	return names
}

// With returns a copy of base with change applied.
func With(base, change Row) Row {
	r := maps.Clone(base)
	maps.Copy(r, change)
	return r
}

// Execer is what *sql.DB, *sql.Tx and the store writer's transaction share
// for writing.
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// InsertRow inserts r into table through ex.
func InsertRow(ex Execer, table string, r Row) error {
	names := slices.Sorted(maps.Keys(r))
	quoted := make([]string, len(names))
	args := make([]any, len(names))
	for i, name := range names {
		quoted[i] = strconv.Quote(name)
		args[i] = r[name]
	}
	query := `INSERT INTO "` + table + `" (` + strings.Join(quoted, ", ") + `) VALUES (?` + strings.Repeat(", ?", len(names)-1) + `)`
	_, err := ex.Exec(query, args...)
	return err
}

// QueryPlan returns the details of SQLite's plan for query on db, its
// parameters bound to args.
func QueryPlan(t testing.TB, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(`EXPLAIN QUERY PLAN `+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}

// ExpiryAt is the instant, Unix milliseconds, an away session's expiry runs
// out: left, when its connection ended, plus expiry, the session expiry
// granted in seconds (storage.md, sessions: sessions_expiry "is on (left +
// expiry * 1000) [...] It orders away sessions by the instant their expiry
// runs out"; design.md, DISCONNECT: "The session goes away until its
// expiry").
func ExpiryAt(left int64, expiry uint32) int64 { return left + int64(expiry)*1000 }

// RecordBytes is the size of a record the retention tests store, inside and
// end to end, so the sizes retention counts are known: storage.md, A
// message's bytes, "A message's bytes are its topic's, payload's and
// properties' bytes, the payload counted at its size as received. Retention
// and the size limits count those bytes."
const RecordBytes = 100

// Filling is the payload of dots that makes a record on topic, carrying no
// property, n bytes: what the topic leaves of n.
func Filling(topic string, n int) []byte { return []byte(strings.Repeat(".", n-len(topic))) }
