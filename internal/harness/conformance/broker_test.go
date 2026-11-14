package conformance

import (
	"bytes"
	"database/sql"
	"os"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"

	// The tests that damage a running broker's database fold its log in
	// first, through SQLite itself, as the store's own tests do.
	_ "github.com/mattn/go-sqlite3"
)

// The seam these tests drive, built in internal/harness/broker.go beside
// StartBroker, which becomes Launch on a folder of its own:
//
//	type Options struct {
//		Dir            string        // required: the folder of its configuration, Dir/saguin.yaml, and its database, Dir/saguin.db; a later Launch on one Dir restarts on the same store
//		WriteTimeout   time.Duration // broker.limits.write_timeout; zero leaves config.md's default
//		MaxConnections int           // broker.limits.max_connections; zero leaves config.md's default
//		PIDFile        string        // broker.pid_file; "" leaves it out
//		Stderr         *os.File      // its standard error; nil keeps it for Log
//		...                          // and the keys a test sets, each field naming its own
//	}
//	func WriteConfig(o *Options) (path string, err error)
//	func Start(binary string, o *Options) (*Broker, error)
//	func (b *Broker) Listening() error
//	func Launch(binary string, o *Options) (*Broker, error)
//
// WriteConfig writes Dir/saguin.yaml and returns its path: broker.id, the
// storage keys, one tcp MQTT listener on loopback and no other listener,
// and o's keys; every other key at config.md's default. It is the one
// configuration writer: Start runs `binary --config` on what it writes,
// and the in-process tests of cmd/saguin run on it too. Start returns as
// soon as the process runs the binary, before it has read anything;
// Listening returns once the listener accepts connections; Launch is the
// two. A broker that exits before it listens is an error that wraps the
// *exec.ExitError of its exit and carries what it printed.
//
//	type Broker struct {
//		Addr string // its MQTT listener, host:port
//		DB   string // its storage.file_path
//	}
//	func (b *Broker) PID() int
//	func (b *Broker) Signal(sig os.Signal) error
//	func (b *Broker) Wait(limit time.Duration) (*os.ProcessState, error)
//	func (b *Broker) Log() string
//	func (b *Broker) Stop() error
//
// Wait waits for the process to exit and returns its state; one still
// running after limit is killed and Wait fails; called again it returns the
// same. Log is what the broker wrote on standard error, whole once Wait has
// returned. Stop is SIGTERM and a Wait, failing unless the exit is 0.

// stop sends b SIGTERM and returns its exit, failing the test where it
// does not exit within stopLimit.
func stop(t *testing.T, b *harness.Broker) *os.ProcessState {
	t.Helper()
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	return support.Exited(t, b)
}

// damage overwrites the whole of the running broker's database at path.
// SQLite first folds the write-ahead log into the file and empties it
// (PRAGMA wal_checkpoint(TRUNCATE)), which every connection to the file
// notices at its next transaction, so the broker's next read or write meets
// the damage rather than a page it holds or one in the log.
func damage(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	var busy, frames, folded int
	err = db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &folded)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("folding the log into %s: %v", path, err)
	}
	if busy != 0 {
		t.Fatalf("folding the log into %s: the broker held it busy", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt(bytes.Repeat([]byte{0xff}, int(info.Size())), 0)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("damaging %s: %v", path, err)
	}
}

// touch sends the CONNECT of a new durable session to b: the store reads
// the client id's session and writes the new one (design.md, Flows,
// CONNECT: Load and Store). What answers it is the test's to read.
func touch(t *testing.T, b *harness.Broker, id string) *harness.Client {
	t.Helper()
	c, err := harness.Dial(b.Addr, harness.V5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() }) // the test is over
	if err := c.Send(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: id, CleanStart: true, Props: support.Durable}); err != nil {
		t.Fatal(err)
	}
	return c
}

// shutDown reads the server's DISCONNECT 0x8B (Server shutting down) from
// an MQTT 5 client, then the end of the connection (design.md, Restart,
// Graceful stop: "every client gets 0x8B").
func shutDown(t *testing.T, c *harness.Client) {
	t.Helper()
	if d := harness.Expect[*harness.Disconnect](t, c); d.Reason != 0x8B {
		t.Fatalf("DISCONNECT 0x%02X, want 0x8B (Server shutting down)", d.Reason)
	}
	harness.ExpectClosed(t, c)
}

// neverDrained returns the writing end of a pipe already full, whose
// reading end the test holds open and never reads until it ends: an output
// that takes nothing more. A write larger than the pipe fills every page of
// it; single bytes then take any room left, until the pipe refuses one.
// The end goes back to blocking before it is handed on, so a write to it
// waits rather than fails.
func neverDrained(t *testing.T) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = r.Close(), w.Close() }) // the test is over
	fd := int(w.Fd())                                 // Fd puts the end in blocking mode
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range [][]byte{make([]byte, 1<<20), {0}} {
		for {
			if _, err := syscall.Write(fd, chunk); err == syscall.EAGAIN {
				break
			} else if err != nil {
				t.Fatalf("filling the pipe: %v", err)
			}
		}
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		t.Fatal(err)
	}
	return w
}
