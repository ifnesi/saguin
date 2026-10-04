package sqlite

import (
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// clientWrite records its client id in the order transactions run it, and
// writes nothing: one client's write that answers nobody, a departed
// client's where departed.
func clientWrite(order *[]string, client string, departed bool) *write {
	return &write{client: client, departed: departed, run: func(*sql.Tx) error {
		*order = append(*order, client) // run by one leader at a time
		return nil
	}}
}

// runsOf records the client id of every write db runs, in order.
func runsOf(t *testing.T, db *DB) *[]string {
	var order []string
	db.afterMember = func(w *write) { order = append(order, w.client) }
	t.Cleanup(func() { db.afterMember = nil })
	return &order
}

// **A mass disconnect does not go ahead of a CONNACK** (Sessions.
// Disconnected, departed): a session begun, which a CONNACK waits on, queued
// behind three full groups of departed clients' Disconnecteds runs before
// any of them. Served as answering, each of those put ten thousand clients
// disconnecting at once ahead of the first reconnecting client's CONNACK.
func TestASessionBegunGoesAheadOfDepartedClientsDisconnects(t *testing.T) {
	db := open(t, tempPath(t))
	s := sessionsOn(t, db)
	db.budget = time.Hour
	n := 3 * SessionGroupMax
	var calls []func() error
	for i := range n {
		id := fmt.Sprint("gone-", i)
		if err := s.Save(aSession(id)); err != nil {
			t.Fatal(err)
		}
		calls = append(calls, func() error { return s.Disconnected(id, time.Now(), false, 60) })
	}
	sess := aSession("back")
	calls = append(calls, func() error { _, err := s.Begin("back", nil, &sess); return err })
	order := runsOf(t, db)
	for i, err := range behindAHeldCommit(t, db, calls...) {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if len(*order) != n+1 {
		t.Fatalf("ran %d writes, want %d: %v", len(*order), n+1, *order)
	}
	if at := slices.Index(*order, "back"); at != 0 {
		t.Fatalf("the session begun ran after %d departed clients' disconnects: %v", at, *order)
	}
}

// **A packet's own client's queued write is served as one it waits on**
// (DB.Waiting): queued behind two full groups of other clients' departed
// writes and behind a session begun, the group holding c12's write moves up
// among the answering ones as c12's packet is marked - ahead of the group
// before it, and behind the session begun that was already waiting - and a
// write for c12 joining while it is marked answers too. Once the mark is
// cleared, nothing is left counted.
func TestAWaitingClientsQueuedWriteMovesUp(t *testing.T) {
	db := open(t, tempPath(t))
	lg, err := db.Log("events")
	if err != nil {
		t.Fatal(err)
	}
	db.budget = time.Hour
	var order []string
	release := holdACommit(t, db, lg)
	var wg sync.WaitGroup
	n := 0
	join := func(w *write) {
		wg.Add(1)
		go func() { defer wg.Done(); _ = db.joinWrite(w) }()
		n++
		until(t, fmt.Sprint("write ", n), func() bool { return queuedWrites(db) == n })
	}
	for i := range 2 * SessionGroupMax {
		join(clientWrite(&order, fmt.Sprint("c", i), true))
	}
	begun := clientWrite(&order, "begun", false)
	begun.answers = true
	join(begun)
	answered := db.Waiting("c12")
	join(clientWrite(&order, "c12", false))
	release()
	wg.Wait()
	answered()
	want := []string{"begun"}
	for i := SessionGroupMax; i < 2*SessionGroupMax; i++ {
		want = append(want, fmt.Sprint("c", i))
	}
	want = append(want, "c12")
	for i := range SessionGroupMax {
		want = append(want, fmt.Sprint("c", i))
	}
	if !slices.Equal(order, want) {
		t.Fatalf("ran %v\nwant %v", order, want)
	}
	db.forming.Lock()
	marks, queued := len(db.marks), len(db.queuedFor)
	db.forming.Unlock()
	if marks != 0 || queued != 0 {
		t.Fatalf("%d client ids still marked and %d still counted queued once everything ran", marks, queued)
	}
}

// **Departed clients' writes and connected sessions' are served four to
// one** (nextBackground): six full groups of departed clients' writes queued
// before two of connected sessions', the first connected group runs after
// four departed ones, and the second once the departed are done. Served
// oldest-first, a subscriber's acknowledgement waited behind a whole storm's
// Disconnecteds and its window stayed shut; one to one, the storm's
// reconnecting clients waited behind the acknowledgements.
func TestDepartedAndConnectedWritesAreServedFourToOne(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	db.budget = time.Hour
	var order []string
	var ws []*write
	for g := range 6 {
		for i := range SessionGroupMax {
			ws = append(ws, clientWrite(&order, fmt.Sprintf("D%d-%d", g+1, i), true))
		}
	}
	for g := range 2 {
		for i := range SessionGroupMax {
			ws = append(ws, clientWrite(&order, fmt.Sprintf("C%d-%d", g+1, i), false))
		}
	}
	queueBehindAHeldCommit(t, db, ws...)
	var groups []string
	for i := 0; i < len(order); i += SessionGroupMax {
		g := order[i][:2]
		for _, c := range order[i:min(i+SessionGroupMax, len(order))] {
			if c[:2] != g {
				t.Fatalf("a group mixed %s and %s: %v", g, c, order)
			}
		}
		groups = append(groups, g)
	}
	want := []string{"D1", "D2", "D3", "D4", "C1", "D5", "D6", "C2"}
	if !slices.Equal(groups, want) {
		t.Fatalf("groups ran %v, want %v", groups, want)
	}
}

// **Sustained answering traffic does not starve the rest** (handOn): while
// sixty-four writers keep joining writes a client waits on without pause, a
// departed client's write and a connected session's, queued among them, are
// each stored - the one on the rest's next turn, after at most
// answersBeforeBackground answering groups and the one under way, and the
// other on the turn after that.
func TestSustainedAnsweringWritesDoNotStarveTheRest(t *testing.T) {
	db := open(t, tempPath(t))
	if _, err := db.Log("events"); err != nil {
		t.Fatal(err)
	}
	var (
		mu      sync.Mutex
		order   []string
		stop    = make(chan struct{})
		writers sync.WaitGroup
	)
	record := func(name string) func(*sql.Tx) error {
		return func(*sql.Tx) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	for i := range 64 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = db.joinWrite(&write{client: fmt.Sprint("a", i), answers: true, run: record("answer")})
			}
		}()
	}
	defer func() {
		close(stop)
		writers.Wait()
	}()
	until(t, "answering writes to run", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 10*SessionGroupMax
	})
	// Queued while no write can run - the writer held, a leader waiting on
	// it and more queued behind - so that what runs from here on is counted
	// from their joining.
	db.writing.Lock()
	held := true
	unhold := func() {
		if held {
			held = false
			db.writing.Unlock()
		}
	}
	defer unhold()
	until(t, "a leader to wait on the writer with answering writes queued", func() bool {
		db.forming.Lock()
		defer db.forming.Unlock()
		return db.committing && len(db.waiting) > 0
	})
	done := make(chan string, 2)
	for _, w := range []*write{
		{client: "gone", departed: true, run: record("gone")},
		{client: "here", run: record("here")},
	} {
		go func() { _ = db.joinWrite(w); done <- w.client }()
		until(t, w.client+" to queue", func() bool {
			db.forming.Lock()
			defer db.forming.Unlock()
			return db.queuedFor[w.client] == 1
		})
	}
	mu.Lock()
	from := len(order)
	mu.Unlock()
	unhold()
	for range 2 {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("a write nobody waits on was not stored in 10s of answering traffic")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if queuedWrites(db) == 0 {
		t.Fatal("the answering traffic had stopped by the time the rest were stored, so this proves nothing")
	}
	ran := order[from:]
	for _, c := range []struct {
		name  string
		turns int
	}{{"gone", 1}, {"here", 2}} {
		n := 0
		for _, name := range ran[:slices.Index(ran, c.name)] {
			if name == "answer" {
				n++
			}
		}
		// The group under way, then answersBeforeBackground groups before
		// each turn of the rest.
		if limit := (c.turns*answersBeforeBackground + 1) * SessionGroupMax; n > limit {
			t.Errorf("%d answering writes ran before %s was stored, want at most %d", n, c.name, limit)
		}
	}
}
