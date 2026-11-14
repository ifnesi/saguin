package support

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"

	// OpenDB and the readers below open a broker's database through SQLite
	// itself, as an operator's own tools do.
	_ "github.com/mattn/go-sqlite3"
)

// DiskSize is room for a new database, its write-ahead log and a few
// sessions, and little more, so a ballast fills it at once.
const DiskSize = 2 << 20

// Stored connects, as MQTT 5, a new durable session for id to b, whose
// CONNACK says the store holds it (design.md, Flows, Replies: "CONNACK
// (when it changes a session) [...] sent only once the store confirms").
func Stored(t *testing.T, b *harness.Broker, id string) *harness.Client {
	t.Helper()
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: Durable})
	return c
}

// Crash kills b (SIGKILL): no stop folds its write-ahead log into the
// database, and nothing it held in memory is saved.
func Crash(t *testing.T, b *harness.Broker) {
	t.Helper()
	if err := b.Signal(os.Kill); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	Exited(t, b)
}

// AwaitAway waits until q's database holds client's durable session away,
// and returns when its connection ended, failing the test where it is not
// away harness.DefaultTimeout after that end: storage.md, sessions: left,
// "when its connection ended; 0 while connected".
func AwaitAway(t *testing.T, q RowQuerier, client string) time.Time {
	t.Helper()
	var left int64
	gone := func() bool {
		err := q.QueryRowContext(context.Background(), `SELECT left FROM sessions WHERE client = ?`, client).Scan(&left)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("reading when the connection of %q ended: %v", client, err)
		}
		return left > 0
	}
	if !testkit.Within(harness.DefaultTimeout, gone) {
		t.Fatalf("%v after its connection ended, the database does not hold the session of %q away", harness.DefaultTimeout, client)
	}
	return time.UnixMilli(left)
}

// OpenDB opens the database at path through SQLite, as an operator's own
// tools open it, and closes it when the test ends. A broker may be running
// on it: SQLite's WAL lets a reader in beside its writer.
func OpenDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() }) // the test is over
	return db
}

// SmallDisk names the environment variable that tells the test binary, run
// again by testkit.OnSmallDisk, the directory where a filesystem of
// DiskSize bytes is mounted: the test fills that one, never the machine's.
const SmallDisk = "SAGUIN_CONFORMANCE_SMALL_DISK"

// AloneLimit bounds a run of the test binary of its own: a backstop over
// the run's own bounded waits, its brokers' starts (10 s each, Launch), its
// stops (stopLimit) and its clients' reads (harness.DefaultTimeout).
const AloneLimit = 2 * time.Minute
