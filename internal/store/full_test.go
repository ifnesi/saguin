package store_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/mattn/go-sqlite3"

	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The API these tests add to the store's (store_test.go):
//
//	var ErrFull error // the answer "full" to a write SQLite could not save for room
//	func (s *Store) Measure() Read // a new read of the store-held metric values
//
// design.md, R8: "Recoverable full responses, fatal errors and recovery are
// defined in Store"; Store, A full disk: "a write is answered "full" only
// when SQLite reports lack of space and rollback confirms that the
// transaction did not commit. The broker continues after that answer";
// Writes: "A full refusal rolls back the whole group and answers every member
// "full"". The store answers "full" with an error errors.Is reports as
// ErrFull, whatever the write: it does not sort writes by kind, and what the
// client is told, or retrying a write, is the core's. A write answered "full"
// changes nothing, and the store goes on applying the writes after it; Stop
// returns no error for it (TestFullMemberEndsItsGroup; from outside, the
// conformance package's TestPublishMeetingAFullDisk).
//
// Start registers metrics.md's Store and memory series the store sees:
// saguin_store_info{mode}, saguin_store_bytes, saguin_store_wal_bytes and
// saguin_store_full; the commit series are the core's. The writer sets
// saguin_store_full before it sends the answers of the transaction that
// moves it: to 1 before answering "full", to 0 before answering a saved
// write. saguin_store_bytes and
// saguin_store_wal_bytes are the sizes of the database's file and of its
// -wal; they are what the last Measure read, which the core sends to Reads
// every min_scrape_interval, and a scrape returns them without asking the
// store.

// kib is a 1 KiB payload.
var kib = make([]byte, 1<<10)

// publisher hands out writes of records at the next offsets of channel 2, so
// none reuses another's key.
type publisher struct{ next int }

// write returns the next write of n records of payload each, as a publish, a
// Will or a dead-letter move stores records.
func (p *publisher) write(n int, payload []byte) *putRecords {
	op := &putRecords{first: p.next, n: n, payload: payload}
	p.next += n
	return op
}

// countRange is a test read of how many records of channel 2 are stored at
// offsets from first, n of them.
type countRange struct{ first, n, got int }

func (c *countRange) ReadConn(conn *store.Conn) error {
	return conn.QueryRowContext(context.Background(), `SELECT count(*) FROM records WHERE channel = 2 AND "offset" >= ? AND "offset" < ?`, c.first, c.first+c.n).Scan(&c.got)
}

// storedOf reads, through the readers, how many of op's records are stored.
func storedOf(t *testing.T, r *running, op *putRecords) int {
	t.Helper()
	c := &countRange{first: op.first, n: op.n}
	if err := read(t, r, c); err != nil {
		t.Fatalf("counting records: %v", err)
	}
	return c.got
}

// isFull reports whether err is ErrFull, failing the test at once on any
// other error: a store error where a full disk answers "full" (R8).
func isFull(t *testing.T, err error) bool {
	t.Helper()
	if err != nil && !errors.Is(err, store.ErrFull) {
		t.Fatalf("an answer with %v, which is not ErrFull", err)
	}
	return err != nil
}

// statementResult is a test write of records that keeps what its own
// statements returned, so a test sees whether SQLite met the full store
// while the write ran, before any commit.
type statementResult struct {
	*putRecords
	err error
}

func (s *statementResult) WriteTx(tx *store.Tx) error {
	s.err = s.putRecords.WriteTx(tx)
	return s.err
}

// design.md, Store, Writes: "The writer executes exactly the sealed group as
// one transaction and returns one transaction result"; "A full refusal
// rolls back the whole group and answers every member "full", including
// members not executed". A group's members run in member order, the first
// error ending it (core_test.go, Group.WriteTx): which ran, under the
// rollback, no client can see.
//
// The store is held to the pages it has (limitPages). One group holds a
// write of records that needs more pages, then a part noting whether it
// ran. The group is answered "full": the records' own statements met the
// full store (SQLITE_FULL), the part after them never ran, and none of the
// records is stored. Stop returns no error.
func TestFullMemberEndsItsGroup(t *testing.T) {
	r := start(t, newConfig(t, 2))
	if err := write(t, r, &limitPages{}); err != nil {
		t.Fatalf("holding the store to its pages: %v", err)
	}
	p := &publisher{next: 1}
	records := &statementResult{putRecords: p.write(8, kib)}
	last := newHold(t)
	last.free() // it passes straight through, entered saying whether it ran
	if !isFull(t, write(t, r, &core.Group{Writes: []store.Write{records, last}})) {
		t.Fatal("a group whose records need pages the store is held from is answered as stored")
	}
	var sqliteErr sqlite3.Error
	if !errors.As(records.err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrFull {
		t.Fatalf("the records' own statements returned %v, want SQLITE_FULL: the full store was not met inside the group", records.err)
	}
	select {
	case <-last.entered:
		t.Error("the group's part after the records that met the full store ran")
	default:
	}
	if got := storedOf(t, r, records.putRecords); got != 0 {
		t.Errorf("the records, in the group answered full: %d stored, want none", got)
	}
	stop(t, r)
}

// metrics.md, Rules: "A value only the store knows, such as a size or count,
// is re-read every min_scrape_interval (config.md). The core has a store
// reader read it. The value is kept as an atomic until the next read, and a
// scrape returns the values held. These values are [...] saguin_store_bytes,
// saguin_store_wal_bytes [...] A scrape returns published values without
// waiting. In particular, it never waits on the core or the store."
// metrics.md, Store and memory:
// saguin_store_bytes "the database's size", saguin_store_wal_bytes "the
// write-ahead log's size". A Measure sent to Reads sets them to the sizes of
// the database's file and its -wal; writes after it leave the scrape showing
// what it read, until the next Measure; and a scrape answers while the writer
// and every reader are busy.
func TestMeasureHoldsStoreValues(t *testing.T) {
	for _, readers := range []int{2, 8} {
		t.Run("read_connections "+strconv.Itoa(readers), func(t *testing.T) {
			r := start(t, newConfig(t, readers))
			path := r.cfg.Broker.Storage.FilePath
			p := &publisher{next: 1}
			measured := func() (dbBytes, walBytes float64) {
				t.Helper()
				return testkit.Sample(t, r.reg, "saguin_store_bytes"), testkit.Sample(t, r.reg, "saguin_store_wal_bytes")
			}
			measure := func() {
				t.Helper()
				m := r.Measure()
				if err := read(t, r, m); err != nil {
					t.Fatalf("Measure: %v", err)
				}
				db, wal := measured()
				if db != float64(testkit.FileSizeOrZero(t, path)) || wal != float64(testkit.FileSizeOrZero(t, path+"-wal")) {
					t.Errorf("after a Measure: saguin_store_bytes %v, saguin_store_wal_bytes %v; the files hold %d and %d",
						db, wal, testkit.FileSizeOrZero(t, path), testkit.FileSizeOrZero(t, path+"-wal"))
				}
			}

			if err := write(t, r, p.write(4, kib)); err != nil {
				t.Fatal(err)
			}
			measure()
			db, wal := measured()
			for range 8 {
				if err := write(t, r, p.write(4, kib)); err != nil {
					t.Fatal(err)
				}
			}
			if float64(testkit.FileSizeOrZero(t, path)) == db && float64(testkit.FileSizeOrZero(t, path+"-wal")) == wal {
				t.Fatal("32 KiB of writes changed neither the database's file nor its -wal")
			}
			if gotDB, gotWAL := measured(); gotDB != db || gotWAL != wal {
				t.Errorf("between Measures, the scrape moved from %v and %v to %v and %v", db, wal, gotDB, gotWAL)
			}
			measure()

			holds := append([]*hold{holdWriter(t, r)}, holdReaders(t, r, readers)...)
			testkit.Scrape(t, r.reg, testkit.Backstop(backstop))
			for _, h := range holds {
				h.free()
			}
			collect(t, r, len(holds))
			stop(t, r)
		})
	}
}
