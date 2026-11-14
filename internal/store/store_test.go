package store_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The API these tests fix:
//
//	const Credit // requests the core may have outstanding, reads and writes together (R2: a constant)
//	type Write interface {
//		WriteTx(tx *Tx) error // runs in the writer's open transaction
//		Records() int         // the records (design.md, Words: Record) it stores, 0 for none
//	}
//	type Read interface {
//		ReadConn(conn *Conn) error // runs on its goroutine's connection, in its read transaction, its view already fixed
//	}
//	type Tx struct{ *sql.Tx; ... }     // the writer's open transaction
//	func (t *Tx) Prepared(s Statement) *sql.Stmt   // a writer's statement, run in that transaction
//	type Conn struct{ *sql.Conn; ... } // a connection serving reads
//	func (c *Conn) Prepared(s Statement) *sql.Stmt // a readers' statement, on that connection
//	type Statement uint8 // one of the store's SQL statements:
//		AddChannel   // writer's: INSERT INTO channels (id, name, type, next, floor, bytes) VALUES (?, ?, ?, ?, ?, ?)
//		AddRecord    // writer's: INSERT INTO records, its 14 columns in storage.md's order, each a ?
//		LoadChannels // readers': SELECT id, name, type, next, floor, bytes FROM channels ORDER BY id
//	type Answer struct {
//		Op   any   // the Write or Read answered, holding its result
//		View bool  // a read's view is established; its result follows in a later Answer
//		Err  error // nil, or the store error that failed it (R8)
//	}
//	type State struct{ Channels []Channel } // what the start loads for the core
//	func Lock(cfg *config.Config) (*FileLock, error) // takes the storage lock at cfg's <file_path>.lock
//	func (l *FileLock) Release() error               // releases a lock not yet handed to Start
//	func Start(cfg *config.Config, reg *metrics.Registry, lock *FileLock) (*Store, *State, error)
//		// takes ownership of lock: Start releases it if it fails, and Stop releases it
//	func (s *Store) Writes() chan<- Write
//	func (s *Store) Reads() chan<- Read // to the store readers, never the writer
//	func (s *Store) Answers() <-chan Answer
//	func (s *Store) Stop() error
//
//	type ChannelType uint8 // storage.md, channels: Append 1, Latest 2, Queue 3
//	type Channel struct {
//		ID    int64
//		Name  string
//		Type  ChannelType
//		Next  int64
//		Floor int64
//		Bytes int64
//	}
//	type AddChannels struct{ Channels []Channel } // *AddChannels is a Write of no record: a channels row for each
//
// Start opens cfg.Broker.Storage.FilePath (design.md, Store). It checks the
// file's header, schema and version, creating storage.md's schema and version
// in one transaction in a new file, and loads the State the core needs: the
// channels rows, by id. Another version, or damage in what it reads, is
// refused with an error naming the file; damage elsewhere surfaces when first
// touched, as a store error. Start starts the store writer and read_connections
// readers, each with a go statement of Start's own (the R1 allowlist names
// store.Start), each keeping one connection for life. Every connection is in
// WAL mode, with synchronous=FULL except in normal mode (design.md, Store),
// which Start reads back and refuses to run without, naming the file and the
// setting, and every other setting is
// SQLite's default; a reader's is read-only. storage.md, The schema version:
// "A missing file, or one holding no schema object at version 0, is new and
// is created with the schema below". design.md, Store: "Each statement of the
// store's operation table is prepared at startup on every connection that
// uses it. If a statement cannot be prepared, startup is refused": Start
// prepares every statement of the store's table once on each connection it
// belongs to, the writer's on the writer's, the readers' on each connection
// serving reads, so a statement that cannot be prepared stops Start, never a
// write; an op takes its statements from the Tx or Conn it is given. No call
// is cancellable: an op passes context.Background() where database/sql asks
// for a context.
//
// Only the core sends requests, and only within Credit: requests it has sent
// whose answers it has not yet taken. Every request is answered once on
// Answers, which holds Credit answers, so neither side waits for the other
// (R2). A request is the store's from its send until its answer, and the
// core never reuses what it sent, even after the answer; the result the op
// carries back is the core's (R3).
//
// The writer runs each write it receives as one transaction of its own and
// answers it once, only once it is committed (R4). It groups nothing: the
// core seals each group of writes, a core.Group, which is the write it sends
// (design.md, Store, Writes: "The writer executes exactly the sealed group,
// in member order, and returns one transaction result"), and these tests send
// one as the core does. The writer holds no timer and counts no commit: the
// collection deadline and the commit series are the core's. A read sees only
// committed rows (R5).
//
// A reader runs each read it receives in a read transaction of its own
// (design.md, Store reads): it begins it and fixes its view with a statement
// that reads the database, since "beginning a deferred transaction alone is
// not that confirmation"; then it answers with View set, "the reader
// confirms that its read transaction has established that view"; then it
// runs the read's ReadConn in that transaction, ends it, and answers again
// with the read's result. A read so has two answers, its view's and its
// result's, and holds its one credit until the second (R2). Writes the
// writer commits after the first leave the read's result as it was.
//
// After a store error (an op failing, a commit failing, a connection
// breaking), no write is answered as a success unless it is committed, the
// failed op's changes are not, the writer applies no further write and
// answers each with that error. Stop, called once by main, ends the
// goroutines and closes the database, and reports a store error that no
// answer carried (one found while stopping, or a failure with no request
// left to answer it); an error already answered is reported once, in its
// answers, and Stop then returns nil: design.md, Store, Writes: "A store
// error is reported once, in the answers that carry it; the store's stop
// reports only an error no answer carried" (decided from item 0026d).
//
// AddChannels is the start's own write: the core adds the channels rows
// missing for $broadcast (channel 1), the configured channels and their
// dead-letter channels, with ids it assigns (storage.md, Ids). The tests' own
// ops below stand in for later items' writes and reads, which implement
// Write and Read the same way.

// backstop bounds every wait in these tests; a correct store is far faster.
const backstop = 30 * time.Second

// errFails is what a test op returns to fail.
var errFails = errors.New("the op fails")

// newConfig returns a configuration whose store is a new file under the
// test's own directory, in config.md's default mode, full, with the given
// read_connections (config.md, broker.storage).
func newConfig(tb testing.TB, readers int) *config.Config {
	tb.Helper()
	return &config.Config{Broker: config.Broker{Storage: config.Storage{
		FilePath:        filepath.Join(tb.TempDir(), "saguin.db"),
		Mode:            storageMode(tb, ""),
		ReadConnections: readers,
	}}}
}

// inMode returns newConfig's configuration in the mode config.md's
// broker.storage names ("full", "normal" or "memory"); in memory mode, of
// broker.id store, with memoryDir its memory_dir.
func inMode(t *testing.T, readers int, mode, memoryDir string) *config.Config {
	t.Helper()
	cfg := newConfig(t, readers)
	cfg.Broker.ID = "store"
	cfg.Broker.Storage.Mode = storageMode(t, mode)
	cfg.Broker.Storage.MemoryDir = memoryDir
	return cfg
}

// storageMode is the broker.storage mode named, as the loader reads it from
// a file holding the required keys and that mode: config.md's own names, the
// default where name is "".
func storageMode(tb testing.TB, name string) config.StorageMode {
	tb.Helper()
	yaml := "broker:\n  id: store\n  storage:\n    file_path: /var/lib/saguin/saguin.db\n" +
		"    default_retention_period: 1d\n    default_retention_bytes: 1GiB\n"
	if name != "" {
		yaml += "    mode: " + name + "\n"
	}
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		tb.Fatalf("broker.storage.mode %q: %v", name, err)
	}
	return cfg.Broker.Storage.Mode
}

// smallDisk names the environment variable that tells the test binary, run
// again by testkit.OnSmallDisk, the directory where a tmpfs of diskSize bytes
// is mounted: memory mode's memory_dir.
const smallDisk = "SAGUIN_STORE_SMALL_DISK"

// diskSize is room for a new database's RAM copy and its write-ahead log.
const diskSize = 4 << 20

// aloneLimit bounds a run of the test binary of its own, on a small disk: a
// backstop over its own waits, each bounded by backstop.
const aloneLimit = 2 * time.Minute

// running is a started store, with its registry and the state it loaded.
// Its Stop stops the store once, whoever calls it first, the test or its
// cleanup; later calls return the first call's result. A Stop that hangs
// returns errStopHung after the backstop, so it fails its test as every other
// wait does, rather than run into go test's own timeout.
type running struct {
	*store.Store
	cfg   *config.Config
	reg   *metrics.Registry
	state *store.State
	once  func() error
}

// errStopHung is what a running store's Stop returns when the store's own
// Stop has not returned within the backstop.
var errStopHung = errors.New("the store's Stop did not return within the backstop")

// Stop stops the store once.
func (r *running) Stop() error { return r.once() }

// stopWithin runs the store's own Stop, waiting for it at most the backstop.
func (r *running) stopWithin() error {
	var err error
	if !testkit.ReturnsWithin(backstop, func() { err = r.Store.Stop() }) {
		return errStopHung
	}
	return err
}

// lockAndStart starts a store on cfg in two steps, in the order of
// design.md, Restart, Start: "Startup then takes the storage lock (Store). It
// writes pid_file where set, then opens the store". It takes the lock,
// failing tb if the lock cannot be taken, then hands it to store.Start and
// returns Start's results. These tests set no pid_file, so nothing comes
// between the two steps. Where Start fails, the lock is Start's to release,
// so lockAndStart releases nothing.
func lockAndStart(tb testing.TB, cfg *config.Config, reg *metrics.Registry) (*store.Store, *store.State, error) {
	tb.Helper()
	lock, err := store.Lock(cfg)
	if err != nil {
		tb.Fatalf("Lock(%s): %v", cfg.Broker.Storage.FilePath, err)
	}
	return store.Start(cfg, reg, lock)
}

// start starts a store on cfg, with a registry of its own, and has the
// test's cleanup stop it.
func start(t *testing.T, cfg *config.Config) *running {
	t.Helper()
	reg := metrics.New()
	s, state, err := lockAndStart(t, cfg, reg)
	if err != nil {
		t.Fatalf("Start(%s): %v", cfg.Broker.Storage.FilePath, err)
	}
	if state == nil {
		t.Fatal("Start returned no state")
	}
	r := &running{Store: s, cfg: cfg, reg: reg, state: state}
	r.once = sync.OnceValue(r.stopWithin)
	t.Cleanup(func() { _ = r.Stop() })
	return r
}

// stop stops r and checks it reports no error.
func stop(t *testing.T, r *running) {
	t.Helper()
	if err := r.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// bubble runs f in a synctest bubble, under a watchdog on the real clock: a
// store stuck where the fake clock cannot see it (in SQLite, or on a lock)
// panics the test binary with every goroutine's stack rather than hang it.
func bubble(t *testing.T, f func(t *testing.T)) {
	t.Helper()
	watchdog := time.AfterFunc(2*backstop, func() { panic("the store hung in a synctest bubble") })
	defer watchdog.Stop()
	synctest.Test(t, f)
}

// trySend sends v on ch if it has room now, and reports whether it did.
func trySend[R any](ch chan<- R, v R) bool {
	select {
	case ch <- v:
		return true
	default:
		return false
	}
}

// answerTo takes the store's next answer, which must be op's result, and
// returns its error.
func answerTo(t *testing.T, r *running, op any) error {
	t.Helper()
	a := testkit.Recv(t, r.Answers(), backstop, "an answer")
	if a.Op != op || a.View {
		t.Fatalf("the answer is to %T %p, view %v; want the result of %T %p", a.Op, a.Op, a.View, op, op)
	}
	return a.Err
}

// viewed takes the store's next answer, which must confirm the view of read
// op.
func viewed(t *testing.T, r *running, op store.Read) {
	t.Helper()
	a := testkit.Recv(t, r.Answers(), backstop, "a read's view")
	if a.Op != op || !a.View || a.Err != nil {
		t.Fatalf("the answer is to %T %p, view %v, error %v; want the confirmation of %T %p's view", a.Op, a.Op, a.View, a.Err, op, op)
	}
}

// write sends op to the writer and returns its answer's error.
func write(t *testing.T, r *running, op store.Write) error {
	t.Helper()
	testkit.Send(t, r.Writes(), op, backstop)
	return answerTo(t, r, op)
}

// read sends op to the readers and returns its result's error, once the
// store has confirmed its view.
func read(t *testing.T, r *running, op store.Read) error {
	t.Helper()
	testkit.Send(t, r.Reads(), op, backstop)
	viewed(t, r, op)
	return answerTo(t, r, op)
}

// collect takes n results, each to a different op, and returns their errors
// by op. A read's confirmation of its view, taken on the way, comes at most
// once, before its result.
func collect(t *testing.T, r *running, n int) map[any]error {
	t.Helper()
	errs := make(map[any]error, n)
	views := map[any]bool{}
	for len(errs) < n {
		a := testkit.Recv(t, r.Answers(), backstop, "an answer")
		_, answered := errs[a.Op]
		switch {
		case a.View && (views[a.Op] || answered):
			t.Fatalf("%T %p's view is confirmed twice, or after its result", a.Op, a.Op)
		case a.View:
			views[a.Op] = true
		case answered:
			t.Fatalf("%T %p is answered twice", a.Op, a.Op)
		default:
			errs[a.Op] = a.Err
		}
	}
	return errs
}

// noAnswer checks that no answer is waiting.
func noAnswer(t *testing.T, r *running) {
	t.Helper()
	select {
	case a := <-r.Answers():
		t.Fatalf("an answer to %T %p came early", a.Op, a.Op)
	default:
	}
}

// putRecords is a test write storing n records on channel 2, at offsets from
// first, each with payload (one byte where nil), through the store's
// prepared AddRecord: n records, as a publish stores one.
type putRecords struct {
	first, n int
	payload  []byte
}

func (p *putRecords) Records() int { return p.n }

func (p *putRecords) WriteTx(tx *store.Tx) error {
	payload := p.payload
	if payload == nil {
		payload = []byte{0}
	}
	stmt := tx.Prepared(store.AddRecord)
	for off := p.first; off < p.first+p.n; off++ {
		// channel, offset, id, time, topic, qos, publisher, bridge, expires,
		// properties, published_retained, retained, payload, payload_size
		// (storage.md, records); the payload is stored as received, so its
		// size as received is its length (A stored payload).
		if _, err := stmt.ExecContext(context.Background(), 2, off, strconv.Itoa(off), 1760000000000, "orders/a", 1, "c1", 0, nil, nil, 0, 0, payload, len(payload)); err != nil {
			return err
		}
	}
	return nil
}

// failingPut is a test write that stores a record at offset, then fails.
type failingPut struct{ offset int }

func (*failingPut) Records() int { return 1 }

func (f *failingPut) WriteTx(tx *store.Tx) error {
	if err := (&putRecords{first: f.offset, n: 1}).WriteTx(tx); err != nil {
		return err
	}
	return errFails
}

// endTx is a test write that ends the writer's transaction behind its back
// and reports success, so the commit that follows fails.
type endTx struct{}

func (*endTx) Records() int { return 0 }

func (*endTx) WriteTx(tx *store.Tx) error {
	_, err := tx.Exec(`ROLLBACK`)
	return err
}

// tempMark is a test write that leaves a TEMP table on the writer's
// connection, or, with check, reports whether that table is there.
type tempMark struct {
	check bool
	there bool
}

func (*tempMark) Records() int { return 0 }

func (m *tempMark) WriteTx(tx *store.Tx) error {
	if !m.check {
		_, err := tx.Exec(`CREATE TEMP TABLE mark (x INTEGER)`)
		return err
	}
	return tx.QueryRow(`SELECT count(*) = 1 FROM temp.sqlite_schema WHERE name = 'mark'`).Scan(&m.there)
}

// countRecords is a test read of how many records are stored and their
// payloads' bytes, which reads the records table itself.
type countRecords struct{ n, bytes int }

func (c *countRecords) ReadConn(conn *store.Conn) error {
	return conn.QueryRowContext(context.Background(), `SELECT count(*), coalesce(sum(length(payload)), 0) FROM records`).Scan(&c.n, &c.bytes)
}

// stored reads, through the readers, how many records are committed.
func stored(t *testing.T, r *running) int {
	t.Helper()
	c := &countRecords{}
	if err := read(t, r, c); err != nil {
		t.Fatalf("counting records: %v", err)
	}
	return c.n
}

// insertChannel is a test read that tries to write a channels row a new
// file does not hold, so only the connection can refuse it.
type insertChannel struct{}

func (*insertChannel) ReadConn(conn *store.Conn) error {
	_, err := conn.ExecContext(context.Background(), `INSERT INTO channels (id, name, type, next, floor, bytes) VALUES (2, 'orders', 1, 1, 1, 0)`)
	return err
}

// closeConn is a test read that breaks its goroutine's connection.
type closeConn struct{}

func (*closeConn) ReadConn(conn *store.Conn) error { return conn.Close() }

// settings are a connection's SQLite settings.
type settings struct {
	journal                         string
	synchronous, pageSize, mmapSize int
}

// querier is what *sql.Tx and *sql.Conn share for reading a row.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readSettings reads q's connection's settings.
func readSettings(q querier) (settings, error) {
	var s settings
	for _, p := range []struct {
		name string
		dest any
	}{
		{"journal_mode", &s.journal},
		{"synchronous", &s.synchronous},
		{"page_size", &s.pageSize},
		{"mmap_size", &s.mmapSize},
	} {
		if err := q.QueryRowContext(context.Background(), `PRAGMA `+p.name).Scan(p.dest); err != nil {
			return s, err
		}
	}
	return s, nil
}

// hold is a test op, read or write of no record, that keeps its goroutine in
// it until released, so a test knows what that goroutine is doing. It notes
// the connection's settings and, as a read, the database/sql connection and
// whether the readers' LoadChannels is prepared on it. Once released, a
// call passes straight through, entered still saying that it ran.
type hold struct {
	entered      chan struct{}
	release      chan struct{}
	enter        func() // closes entered, once
	free         func() // closes release, once
	settings     settings
	conn         *sql.Conn
	loadPrepared bool
	err          error
}

// newHold makes a hold that the test's cleanup releases, before it stops the
// store started earlier.
func newHold(t *testing.T) *hold {
	h := &hold{entered: make(chan struct{}), release: make(chan struct{})}
	h.enter = sync.OnceFunc(func() { close(h.entered) })
	h.free = sync.OnceFunc(func() { close(h.release) })
	t.Cleanup(h.free)
	return h
}

func (*hold) Records() int { return 0 }

func (h *hold) WriteTx(tx *store.Tx) error {
	h.settings, h.err = readSettings(tx)
	h.enter()
	<-h.release
	return nil
}

func (h *hold) ReadConn(conn *store.Conn) error {
	h.conn = conn.Conn
	h.loadPrepared = conn.Prepared(store.LoadChannels) != nil
	h.settings, h.err = readSettings(conn)
	h.enter()
	<-h.release
	return nil
}

// holdWriter holds the writer.
func holdWriter(t *testing.T, r *running) *hold {
	t.Helper()
	h := newHold(t)
	testkit.Send(t, r.Writes(), store.Write(h), backstop)
	testkit.Recv(t, h.entered, backstop, "the writer to take the hold")
	return h
}

// together sends ops while the writer is held, so that each waits in the
// writer's queue behind the hold, then releases it, and returns every answer
// by op, the hold's included.
func together(t *testing.T, r *running, ops ...store.Write) map[any]error {
	t.Helper()
	h := holdWriter(t, r)
	for _, op := range ops {
		testkit.Send(t, r.Writes(), op, backstop)
	}
	h.free()
	return collect(t, r, 1+len(ops))
}

// holdReaders holds n goroutines serving reads, each with a hold of its own,
// and takes the confirmation of each hold's view, which comes before the
// hold runs.
func holdReaders(t *testing.T, r *running, n int) []*hold {
	t.Helper()
	holds := make([]*hold, n)
	for i := range holds {
		holds[i] = newHold(t)
		testkit.Send(t, r.Reads(), store.Read(holds[i]), backstop)
	}
	for _, h := range holds {
		testkit.Recv(t, h.entered, backstop, "a reader to take a hold")
	}
	views := map[any]bool{}
	for range n {
		a := testkit.Recv(t, r.Answers(), backstop, "a hold's view")
		if _, ok := a.Op.(*hold); !ok || !a.View || views[a.Op] {
			t.Fatalf("the answer is to %T %p, view %v; want the confirmation of a held read's view, once", a.Op, a.Op, a.View)
		}
		views[a.Op] = true
	}
	return holds
}

// closing closes c, as a deferred call, and reports its error.
func closing(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

// openFile opens the database at path for the test's own reading, after the
// store has stopped.
func openFile(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closing(t, db) })
	return db
}

// userVersion reads db's user_version, the schema version (storage.md).
func userVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatalf("PRAGMA user_version: %v", err)
	}
	return v
}

// broadcastChannel and broadcastRow are $broadcast's channels row as a new
// file holds it, loaded and as storedChannels reads it (storage.md: "When the
// file is created, the tables, the $broadcast row in channels and the version
// are written in one transaction"; "The $broadcast channel is channel 1").
var (
	broadcastChannel = store.Channel{ID: 1, Name: "$broadcast", Type: store.Append, Next: 1, Floor: 1}
	broadcastRow     = [6]any{int64(1), "$broadcast", int64(1), int64(1), int64(1), int64(0)}
)

// storedChannels reads the channels rows of the file at path, by id, with
// their type as stored.
func storedChannels(t *testing.T, path string) [][6]any {
	t.Helper()
	rows, err := openFile(t, path).Query(`SELECT id, name, type, next, floor, bytes FROM channels ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer closing(t, rows)
	var got [][6]any
	for rows.Next() {
		var id, typ, next, floor, size int64
		var name string
		if err := rows.Scan(&id, &name, &typ, &next, &floor, &size); err != nil {
			t.Fatal(err)
		}
		got = append(got, [6]any{id, name, typ, next, floor, size})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

// storedOffsets reads the offsets of the records in the file at path.
func storedOffsets(t *testing.T, path string) []int {
	t.Helper()
	rows, err := openFile(t, path).Query(`SELECT "offset" FROM records ORDER BY "offset"`)
	if err != nil {
		t.Fatal(err)
	}
	defer closing(t, rows)
	var got []int
	for rows.Next() {
		var off int
		if err := rows.Scan(&off); err != nil {
			t.Fatal(err)
		}
		got = append(got, off)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

const module = "github.com/ifnesi/saguin/"

// family returns, by goroutine id, the stacks of the live goroutines
// store.Start started when called on goroutine id, and of those Sagüin's own
// code started from them in turn. A goroutine's stack ends "created by <the
// function holding the go statement> in goroutine <its parent>"; what Go and
// its libraries start, database/sql's included, is not counted (R1).
func family(id string) map[string]testkit.Goroutine {
	return testkit.Descendants(
		func(g testkit.Goroutine) bool { return g.Creator == module+"internal/store.Start" && g.Parent == id },
		func(g testkit.Goroutine) bool { return strings.HasPrefix(g.Creator, module) },
	)
}

// waitFamily waits until family(id) has no goroutine left: one that has
// signalled its end is still listed until it returns.
func waitFamily(t *testing.T, id string) {
	t.Helper()
	if !testkit.Within(backstop, func() bool { return len(family(id)) == 0 }) {
		t.Fatalf("goroutines of the store started here still live: %d", len(family(id)))
	}
}
