package sqlite

// What the tests outside this package (package sqlite_test, which drive a
// whole broker on a provider) need of its write queue: to hold it, to fill it,
// and to read back what it did.

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"
)

// HoldACommit holds a publish inside its transaction on db until release is
// called, so that every write joining meanwhile queues behind it
// (holdACommit). A write group then runs whole, never cut short by its
// budget, so that which group a write ran in is decided rather than raced.
func HoldACommit(t *testing.T, db *DB) (release func()) {
	t.Helper()
	lg, err := db.Log("zz-held")
	if err != nil {
		t.Fatal(err)
	}
	// Under the write lock, which every leader reads both under.
	db.writing.Lock()
	budget := db.budget
	db.budget = time.Hour
	db.writing.Unlock()
	t.Cleanup(func() {
		db.writing.Lock()
		db.budget = budget
		db.writing.Unlock()
	})
	r := holdACommit(t, db, lg)
	var once sync.Once
	release = func() { once.Do(func() { r() }) }
	// Released however the test ends, before the broker on db is stopped.
	t.Cleanup(release)
	return release
}

// QueueDeparted queues n departed clients' writes on db, named prefix0 to
// prefix(n-1), each taking slow to run, one at a time so that they fill
// groups in that order; wait returns once all of them have run.
func QueueDeparted(t *testing.T, db *DB, n int, prefix string, slow time.Duration) (wait func()) {
	t.Helper()
	var wg sync.WaitGroup
	for i := range n {
		client := fmt.Sprint(prefix, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.joinWrite(&write{client: client, departed: true, run: func(*sql.Tx) error {
				time.Sleep(slow)
				return nil
			}})
		}()
		until(t, client+"'s write to queue", func() bool { return QueuedFor(db, client) == 1 })
	}
	return wg.Wait
}

// RecordRuns records the client id of every write db runs from now on, in
// the order it ran them; ran answers them so far.
func RecordRuns(t *testing.T, db *DB) (ran func() []string) {
	var mu sync.Mutex
	var order []string
	db.writing.Lock()
	db.afterMember = func(w *write) {
		mu.Lock()
		order = append(order, w.client)
		mu.Unlock()
	}
	db.writing.Unlock()
	t.Cleanup(func() {
		db.writing.Lock()
		db.afterMember = nil
		db.writing.Unlock()
	})
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), order...)
	}
}

// Marked is how many packets of client's are marked waiting on db
// (DB.Waiting).
func Marked(db *DB, client string) int {
	db.forming.Lock()
	defer db.forming.Unlock()
	return db.marks[client]
}

// QueuedFor is how many of client's writes are queued on db among the
// writes that answer nobody.
func QueuedFor(db *DB, client string) int {
	db.forming.Lock()
	defer db.forming.Unlock()
	return db.queuedFor[client]
}

// QueuedAnswering is how many of client's writes are queued on db among the
// writes that answer a packet.
func QueuedAnswering(db *DB, client string) int {
	db.forming.Lock()
	defer db.forming.Unlock()
	n := 0
	for _, g := range db.waiting {
		if !g.answers {
			continue
		}
		for _, w := range g.writes {
			if w.client == client {
				n++
			}
		}
	}
	return n
}
