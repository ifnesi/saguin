package store_test

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// design.md, Store: "It runs in WAL mode, with synchronous=FULL except in
// normal mode"; "normal mode runs on file_path with synchronous=NORMAL";
// memory mode "runs on the RAM copy"; each store goroutine's "Settings are in
// the connection string"; every other setting is SQLite's default, page_size
// 4096 and mmap_size 0, so a change is made on purpose. In each mode (config.md,
// broker.storage, mode: "full, normal or memory"; memory mode's memory_dir
// on a tmpfs of its own), every connection, the writer's and each reader's,
// reports them: synchronous 2 (FULL) in full and memory mode, 1 (NORMAL) in
// normal mode.
func TestConnectionSettings(t *testing.T) {
	for _, mode := range []struct {
		name        string
		synchronous int
	}{{"full", 2}, {"normal", 1}, {"memory", 2}} {
		t.Run(mode.name, func(t *testing.T) {
			var memoryDir string
			if mode.name == "memory" {
				if memoryDir = os.Getenv(smallDisk); memoryDir == "" {
					testkit.OnSmallDisk(t, smallDisk, diskSize, aloneLimit)
					return
				}
			}
			want := settings{journal: "wal", synchronous: mode.synchronous, pageSize: 4096, mmapSize: 0}
			for _, readers := range []int{2, 8} {
				t.Run("read_connections "+strconv.Itoa(readers), func(t *testing.T) {
					r := start(t, inMode(t, readers, mode.name, memoryDir))
					w := holdWriter(t, r)
					rs := holdReaders(t, r, readers)
					for i, h := range append([]*hold{w}, rs...) {
						h.free()
						if h.err != nil || h.settings != want {
							t.Errorf("connection %d: %+v (%v), want %+v", i, h.settings, h.err, want)
						}
					}
					collect(t, r, 1+readers)
					stop(t, r)
				})
			}
		})
	}
}

// design.md, Store: "each store goroutine keeps its connection for life with
// DB.Conn. Each has one connection, except for memory mode's snapshot
// connection". Reads held on every reader twice over meet the same
// connections, one per reader. A TEMP table the writer leaves in one commit
// is still on its connection in a later one.
func TestConnectionsLastForLife(t *testing.T) {
	for _, readers := range []int{2, 8} {
		t.Run("read_connections "+strconv.Itoa(readers), func(t *testing.T) {
			r := start(t, newConfig(t, readers))
			conns := func() map[*sql.Conn]bool {
				cs := map[*sql.Conn]bool{}
				for _, h := range holdReaders(t, r, readers) {
					h.free()
					cs[h.conn] = true
				}
				collect(t, r, readers)
				return cs
			}
			first, second := conns(), conns()
			if len(first) != readers || !maps.Equal(first, second) {
				t.Errorf("the connections serving reads: %d then %d, the same: %v; want %d, the same",
					len(first), len(second), maps.Equal(first, second), readers)
			}

			if err := write(t, r, &tempMark{}); err != nil {
				t.Fatal(err)
			}
			check := &tempMark{check: true}
			if err := write(t, r, check); err != nil {
				t.Fatal(err)
			}
			if !check.there {
				t.Error("the writer's TEMP table is gone from its connection after a commit")
			}
			stop(t, r)
		})
	}
}

// design.md, Store: "Each statement of the store's operation table is
// prepared at startup on every connection that uses it. If a statement cannot
// be prepared, startup is refused" (decided from item 0006a): Start prepares
// every statement of the store's table once on each connection it belongs to,
// so one that cannot be prepared stops Start, never a write. From the first
// request, the writer's Tx holds the writer's statements, which write in its
// open transaction (TestFailedWriteLeavesNothing rolls one back), and each
// reader's connection holds the readers'. LoadChannels reads $broadcast's
// row, there from the file's creation (broadcastChannel), and the one
// AddChannel adds.
func TestPreparedStatements(t *testing.T) {
	want := []store.Channel{
		broadcastChannel,
		{ID: 2, Name: "orders", Type: store.Queue, Next: 3, Floor: 1, Bytes: 10},
	}
	for _, readers := range []int{2, 8} {
		t.Run("read_connections "+strconv.Itoa(readers), func(t *testing.T) {
			r := start(t, newConfig(t, readers))
			w := &writerStatements{}
			if err := write(t, r, w); err != nil {
				t.Fatal(err)
			}
			if len(w.missing) != 0 {
				t.Errorf("the writer's Tx lacks statements %v", w.missing)
			}
			if err := write(t, r, &store.AddChannels{Channels: slices.Clone(want[1:])}); err != nil {
				t.Fatal(err)
			}
			if err := write(t, r, &putRecords{first: 1, n: 2}); err != nil {
				t.Fatal(err)
			}
			l := &loadChannels{}
			if err := read(t, r, l); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(l.channels, want) {
				t.Errorf("LoadChannels: got %+v, want %+v", l.channels, want)
			}
			// Every reader's connection holds it.
			for i, h := range holdReaders(t, r, readers) {
				h.free()
				if !h.loadPrepared {
					t.Errorf("reader %d's connection has no prepared LoadChannels", i)
				}
			}
			collect(t, r, readers)
			if got := stored(t, r); got != 2 {
				t.Errorf("records stored through AddRecord: got %d, want 2", got)
			}
			stop(t, r)
		})
	}
}

// writerStatements is a test write noting which of the writer's statements
// its Tx lacks.
type writerStatements struct{ missing []store.Statement }

func (*writerStatements) Records() int { return 0 }

func (w *writerStatements) WriteTx(tx *store.Tx) error {
	for _, s := range []store.Statement{store.AddChannel, store.AddRecord} {
		if tx.Prepared(s) == nil {
			w.missing = append(w.missing, s)
		}
	}
	return nil
}

// loadChannels is a test read of the channels rows through the readers'
// prepared LoadChannels.
type loadChannels struct{ channels []store.Channel }

func (l *loadChannels) ReadConn(conn *store.Conn) error {
	stmt := conn.Prepared(store.LoadChannels)
	if stmt == nil {
		return errors.New("no prepared LoadChannels on a connection serving reads")
	}
	rows, err := stmt.QueryContext(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var c store.Channel
		if err := rows.Scan(&c.ID, &c.Name, &c.Type, &c.Next, &c.Floor, &c.Bytes); err != nil {
			return err
		}
		l.channels = append(l.channels, c)
	}
	return rows.Err()
}

// R1 table: a store reader owns "one read-only connection". A write through
// a reader fails, stores nothing, and is a store error, reported once, in
// its answer, so Stop returns nil: design.md, Store, Writes: "A store error
// is reported once, in the answers that carry it; the store's stop reports
// only an error no answer carried" (decided from item 0026d). The file holds
// $broadcast's row alone, as created (broadcastRow).
func TestReadersAreReadOnly(t *testing.T) {
	cfg := newConfig(t, 2)
	r := start(t, cfg)
	err := read(t, r, &insertChannel{})
	if err == nil {
		t.Error("a reader's connection took a write")
	}
	if stopErr := r.Stop(); err != nil && stopErr != nil {
		t.Errorf("Stop: got %v, want nil: the store error %v was reported in its answer", stopErr, err)
	}
	if got := storedChannels(t, cfg.Broker.Storage.FilePath); !slices.Equal(got, [][6]any{broadcastRow}) {
		t.Errorf("channels rows after a write through a reader: %v, want $broadcast's alone", got)
	}
}

// heldCount is a test read that, released, counts the records its view
// holds (countRecords); until then it waits in its reader's read
// transaction.
type heldCount struct {
	countRecords
	release chan struct{}
}

func (h *heldCount) ReadConn(conn *store.Conn) error {
	<-h.release
	return h.countRecords.ReadConn(conn)
}

// design.md, Store reads: "Later writes wait until the reader confirms that
// its read transaction has established that view; beginning a deferred
// transaction alone is not that confirmation. Later writes may then proceed
// while the read finishes". With one record committed, a read is sent whose work waits until
// released. Once the store has confirmed its view, a write of a second
// record is sent and committed. Released, the read counts one record: its
// view is the one confirmed, from before that write.
func TestReadViewIsFixedWhenConfirmed(t *testing.T) {
	r := start(t, newConfig(t, 2))
	if err := write(t, r, &putRecords{first: 1, n: 1}); err != nil {
		t.Fatal(err)
	}
	op := &heldCount{release: make(chan struct{})}
	free := sync.OnceFunc(func() { close(op.release) })
	t.Cleanup(free)
	testkit.Send(t, r.Reads(), store.Read(op), backstop)
	viewed(t, r, op)
	if err := write(t, r, &putRecords{first: 2, n: 1}); err != nil {
		t.Fatalf("the write after the read's view was confirmed: %v", err)
	}
	free()
	if err := answerTo(t, r, op); err != nil {
		t.Fatal(err)
	}
	if op.n != 1 {
		t.Errorf("the read counts %d records, want 1: its view, confirmed before the second record's commit, holds the first alone", op.n)
	}
	stop(t, r)
}

// connOf is a test read noting the connection it runs on.
type connOf struct{ conn *sql.Conn }

func (c *connOf) ReadConn(conn *store.Conn) error {
	c.conn = conn.Conn
	return nil
}

// design.md, Store: "reads go to the store readers only"; R1 table: a store
// reader owns "one read-only connection"; config.md, read_connections: "2
// to 8; request handling follows Store". With the writer held in a write,
// its transaction open, the readers, each held at once, show their
// connections, one each; then every read sent is answered while the writer
// is still held, each on one of those connections.
func TestReadsGoToTheReaders(t *testing.T) {
	for _, readers := range []int{2, 8} {
		t.Run("read_connections "+strconv.Itoa(readers), func(t *testing.T) {
			r := start(t, newConfig(t, readers))
			w := holdWriter(t, r)
			conns := map[*sql.Conn]bool{}
			for _, h := range holdReaders(t, r, readers) {
				h.free()
				conns[h.conn] = true
			}
			collect(t, r, readers)
			if len(conns) != readers {
				t.Fatalf("%d readers held at once show %d connections, want %d", readers, len(conns), readers)
			}

			ops := make([]*connOf, 4*readers)
			for i := range ops {
				ops[i] = &connOf{}
				testkit.Send(t, r.Reads(), store.Read(ops[i]), backstop)
			}
			errs := collect(t, r, len(ops))
			for i, op := range ops {
				if err := errs[op]; err != nil || !conns[op.conn] {
					t.Errorf("read %d, the writer held: answered with %v, on a reader's connection: %v", i, err, conns[op.conn])
				}
			}
			w.free()
			if err := answerTo(t, r, w); err != nil {
				t.Errorf("the writer's hold: %v", err)
			}
			stop(t, r)
		})
	}
}

// design.md, Store: "The writer takes what is waiting in a fixed order: stop,
// a reader's failure, then writes; reads go to the store readers only"; R5: "Publishers do not wait for consumer progress". With every
// reader held and reads filling the rest of the core's credit but one, a
// write sent after them all is answered, the next answer, while every read
// still waits.
func TestWritesNeverWaitForReads(t *testing.T) {
	const readers = 2
	r := start(t, newConfig(t, readers))
	holds := holdReaders(t, r, readers)
	waiting := store.Credit - readers - 1
	for range waiting {
		testkit.Send(t, r.Reads(), store.Read(&countRecords{}), backstop)
	}
	if err := write(t, r, &putRecords{first: 1, n: 1}); err != nil {
		t.Errorf("the write sent after %d waiting reads: %v", waiting, err)
	}
	for _, h := range holds {
		h.free()
	}
	for op, err := range collect(t, r, readers+waiting) {
		if err != nil {
			t.Errorf("the answer to %T: %v", op, err)
		}
	}
	stop(t, r)
}

// design.md, Store: "A broken connection is a store error. It is never
// retried on another". With one of two readers held, the other serves every
// read: a read breaks its connection, inside its read transaction, and the
// error comes in that read's result or, at the latest, in the next read's: it
// is reported there once, and Stop returns nil: design.md, Store, Writes: "A
// store error is reported once, in the answers that carry it; the store's
// stop reports only an error no answer carried" (decided from item 0026d).
func TestBrokenConnectionIsAStoreError(t *testing.T) {
	r := start(t, newConfig(t, 2))
	held := holdReaders(t, r, 1)[0]
	err := read(t, r, &closeConn{})
	if err == nil {
		next := &countRecords{}
		testkit.Send(t, r.Reads(), store.Read(next), backstop)
		err = collect(t, r, 1)[next]
	}
	if err == nil {
		t.Fatal("a read on a broken connection was answered as a success")
	}
	held.free()
	collect(t, r, 1)
	if stopErr := r.Stop(); stopErr != nil {
		t.Errorf("Stop: got %v, want nil: the store error %v was reported in its answer", stopErr, err)
	}
}

// design.md, Store, Requests: "Each dispatched read and each dispatched write
// group consumes one store credit until its answer is processed [...] Answers
// return on the bounded answer channel, which is always drained"; design.md,
// Store reads: a read is answered with its view's confirmation, then its
// result. With every goroutine serving a queue held, the core's Credit
// requests all find room at once, and the store gives every answer of every
// one, a write's one and a read's two, while the core takes none.
func TestRequestsWithinCreditNeverWait(t *testing.T) {
	const readers = 2
	holdW := func(r *running, h *hold) bool { return trySend(r.Writes(), store.Write(h)) }
	holdR := func(r *running, h *hold) bool { return trySend(r.Reads(), store.Read(h)) }
	writes := func(r *running, i int) bool {
		return trySend(r.Writes(), store.Write(&putRecords{first: i + 1, n: 1}))
	}
	reads := func(r *running, _ int) bool { return trySend(r.Reads(), store.Read(&countRecords{})) }
	for _, tc := range []struct {
		name    string
		serving int // the goroutines serving the queue, each held
		hold    func(*running, *hold) bool
		request func(r *running, i int) bool
		answers int // each request's
	}{
		{"writes while the writer is held", 1, holdW, writes, 1},
		{"reads while every reader is held", readers, holdR, reads, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				r := start(t, newConfig(t, readers))
				holds := make([]*hold, tc.serving)
				for i := range holds {
					holds[i] = newHold(t)
					if !tc.hold(r, holds[i]) {
						t.Fatalf("the store had no room for request %d", i+1)
					}
				}
				for _, h := range holds {
					testkit.Recv(t, h.entered, backstop, "the hold to be taken")
				}
				for i := range store.Credit - tc.serving {
					if !tc.request(r, i) {
						t.Fatalf("request %d of Credit (%d) found no room", i+1+tc.serving, store.Credit)
					}
				}
				for _, h := range holds {
					h.free()
				}
				synctest.Wait()
				if got, want := len(r.Answers()), tc.answers*store.Credit; got != want {
					t.Errorf("answers waiting, the core having taken none: got %d, want %d for Credit, %d, requests", got, want, store.Credit)
				}
				for op, err := range collect(t, r, store.Credit) {
					if err != nil {
						t.Errorf("the answer to %T: %v", op, err)
					}
				}
				stop(t, r)
			})
		})
	}
}

// design.md, Store, Requests: answers return "always drained", one result to
// each request; design.md, Store reads: a read's result follows the
// confirmation of its view. Credit requests, reads and writes mixed and
// served by the writer and two readers in any order, are answered with
// exactly one result each, a read's view confirmed at most once before it,
// and nothing more.
func TestOneAnswerPerRequest(t *testing.T) {
	bubble(t, func(t *testing.T) {
		r := start(t, newConfig(t, 2))
		ops := make([]any, 0, store.Credit)
		for i := range store.Credit {
			var sent bool
			if i%2 == 0 {
				op := &putRecords{first: i + 1, n: 1}
				sent = trySend(r.Writes(), store.Write(op))
				ops = append(ops, op)
			} else {
				op := &countRecords{}
				sent = trySend(r.Reads(), store.Read(op))
				ops = append(ops, op)
			}
			if !sent {
				t.Fatalf("request %d of Credit (%d) found no room", i+1, store.Credit)
			}
		}
		errs := collect(t, r, len(ops))
		for _, op := range ops {
			if err, ok := errs[op]; !ok || err != nil {
				t.Errorf("%T %p: answered %v, error %v", op, op, ok, err)
			}
		}
		synctest.Wait()
		noAnswer(t, r)
		if got, want := stored(t, r), (store.Credit+1)/2; got != want {
			t.Errorf("records stored: got %d, want %d", got, want)
		}
		stop(t, r)
	})
}

// storage.md: "A reused key is a failed write and a store error (R8)"; R8: "A
// store error stops the broker, except a failed memory-mode snapshot";
// design.md, Store, Writes: "After a store error, the writer applies no
// further write: every write still queued, and every later one, is answered
// with that first error. A store error is reported once, in the answers that
// carry it; the store's stop reports only an error no answer carried"
// (decided from items 0006a and 0026d). So Stop returns nil. The reused key
// is a channel's that is not $broadcast, in an otherwise valid row, so no
// CHECK masks it. $broadcast's row is the file's from its creation
// (broadcastRow).
func TestReusedKeyIsAStoreError(t *testing.T) {
	rows := []store.Channel{
		{ID: 2, Name: "orders", Type: store.Append, Next: 1, Floor: 1},
	}
	for _, tc := range []struct {
		name  string
		reuse store.Channel
	}{
		{"a reused id", store.Channel{ID: 2, Name: "events", Type: store.Append, Next: 1, Floor: 1}},
		{"a reused name", store.Channel{ID: 3, Name: "orders", Type: store.Append, Next: 1, Floor: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newConfig(t, 2)
			r := start(t, cfg)
			if err := write(t, r, &store.AddChannels{Channels: slices.Clone(rows)}); err != nil {
				t.Fatal(err)
			}
			reused := &store.AddChannels{Channels: []store.Channel{tc.reuse}}
			later := &store.AddChannels{Channels: []store.Channel{{ID: 4, Name: "prices", Type: store.Latest, Next: 1, Floor: 1}}}
			errs := together(t, r, reused, later)
			first := errs[reused]
			if first == nil {
				t.Fatal("answered without a store error")
			}
			if !errors.Is(errs[later], first) {
				t.Errorf("a write queued after the store error: got %v, want that error", errs[later])
			}
			// A write sent after the error was answered is not applied either.
			after := &store.AddChannels{Channels: []store.Channel{{ID: 5, Name: "alerts", Type: store.Append, Next: 1, Floor: 1}}}
			if err := write(t, r, after); !errors.Is(err, first) {
				t.Errorf("a write sent after the store error: got %v, want that error", err)
			}
			if err := r.Stop(); err != nil {
				t.Errorf("Stop: got %v, want nil: the store error %v was reported in its answers", err, first)
			}
			want := [][6]any{
				broadcastRow,
				{int64(2), "orders", int64(1), int64(1), int64(1), int64(0)},
			}
			if got := storedChannels(t, cfg.Broker.Storage.FilePath); !slices.Equal(got, want) {
				t.Errorf("channels rows: got %v, want %v", got, want)
			}
		})
	}
}

// design.md, Flows, Replies: acknowledgements are sent "only once the store
// confirms"; R8; design.md, Store, Writes: "The writer executes exactly the
// sealed group [...] and returns one transaction result", so each write it
// is given is a transaction of its own. A write that changes rows and then
// fails leaves none of its changes; the writes queued behind it are
// answered with its error and not applied; the write before it, committed
// alone, is answered as a success and stored; the error, reported in those
// answers, is not reported again: Stop returns nil (design.md, Store,
// Writes: "the store's stop reports only an error no answer carried",
// decided from item 0026d). The channels table keeps $broadcast's row
// alone, as created (broadcastRow).
func TestFailedWriteLeavesNothing(t *testing.T) {
	cfg := newConfig(t, 2)
	r := start(t, cfg)
	before := &putRecords{first: 1, n: 1}
	failing := &failingPut{offset: 2}
	behind := &putRecords{first: 3, n: 1}
	channel := &store.AddChannels{Channels: []store.Channel{{ID: 2, Name: "orders", Type: store.Append, Next: 1, Floor: 1}}}
	errs := together(t, r, before, failing, behind, channel)
	if !errors.Is(errs[failing], errFails) {
		t.Fatalf("the failing write: got %v, want its error %v", errs[failing], errFails)
	}
	if errs[before] != nil {
		t.Errorf("the write before the failing one, a transaction of its own: got %v, want it stored", errs[before])
	}
	for _, op := range []any{behind, channel} {
		if !errors.Is(errs[op], errFails) {
			t.Errorf("%T queued behind the failing write: got %v, want its error", op, errs[op])
		}
	}
	if err := r.Stop(); err != nil {
		t.Errorf("Stop: got %v, want nil: the store error %v was reported in its answers", err, errFails)
	}
	if got := storedOffsets(t, cfg.Broker.Storage.FilePath); !slices.Equal(got, []int{1}) {
		t.Errorf("records stored: offsets %v, want [1], the write before the failure's", got)
	}
	if got := storedChannels(t, cfg.Broker.Storage.FilePath); !slices.Equal(got, [][6]any{broadcastRow}) {
		t.Errorf("channels rows after a write queued behind the failure: %v, want $broadcast's alone", got)
	}
}

// design.md, Flows, Replies: acknowledgements only "once the store
// confirms"; design.md, Store, Writes: the writer "returns one transaction
// result". A group whose commit fails, a record and then a write ending the
// transaction behind the writer's back, is answered once, with an error,
// which Stop does not report again (design.md, Store, Writes: "A store
// error is reported once, in the answers that carry it; the store's stop
// reports only an error no answer carried", decided from item 0026d);
// nothing of it is stored.
func TestFailedCommitAnswersNoSuccess(t *testing.T) {
	cfg := newConfig(t, 2)
	r := start(t, cfg)
	failed := write(t, r, &core.Group{Writes: []store.Write{&putRecords{first: 1, n: 1}, &endTx{}}})
	if failed == nil {
		t.Error("a group whose commit failed was answered as a success")
	}
	if err := r.Stop(); err != nil {
		t.Errorf("Stop: got %v, want nil: the failed commit's error %v was reported in its answer", err, failed)
	}
	if got := storedOffsets(t, cfg.Broker.Storage.FilePath); len(got) != 0 {
		t.Errorf("records stored by the failed commit: %v", got)
	}
}
