package sqlite

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/store"
)

// A client's writes - a session begun, saved, ended or disconnected, its
// in-flight table, its acknowledgements, a shared group's cursor and returned
// deliveries, an exactly-once publish held or dropped, a consumer's position
// saved or dropped - are collected behind the commit before, as publishes are
// (storeBehind): one that finds no transaction running commits at once,
// alone, and those arriving while one runs share the next. Each caller still
// returns only once the transaction holding its write has committed, so
// nothing is told to a client before what it describes is stored (invariant
// 18), and a provider holding ten thousand sessions reconnecting at once
// commits them tens at a time rather than one at a time on its one writer.
//
// **No savepoints.** SQLITE_FULL inside a savepoint ends the whole
// transaction for every single-row statement this store issues, measured on
// modernc v1.58.0, after which a statement sent on it commits by itself. So a
// member does not get a savepoint of its own; any failed statement, and any
// error its write returns, ends the transaction for all of them (abort), and
// they run again (runWrites) - once more together without the one that ended
// it, then each alone, as it would have run before they were collected.
//
// **What refuses a member without ending the transaction is decided by
// reading.** A write is two parts: check, which only reads, and whose error
// is that member's answer while the others go on - a session that is not
// held, a window that is full, a group with no cursor - and run, which
// writes. No SQL constraint refuses a row here (every insert is ON CONFLICT,
// and every other rule is checked in Go first), so what ends a transaction is
// a provider out of room or a file that cannot be written.

// SessionGroupMax is the most client writes one transaction holds.
//
// A group gives way to a waiting publish after each write (attempt), so its
// size no longer decides how long a publish waits; it decides how far the
// commit's cost is shared, and what one transaction's failure puts back.
// Measured on the Ryzen box, five runs, 10,000 sessions reconnecting while
// ten publishers each send QoS 1 every 5ms: the storm took 1.75-1.79s at 8
// and 1.65-1.74s at 64 (2.41-2.57s with a transaction per write), and a
// PUBACK's median was 0.30-0.34ms at 8 against 1.24-1.55ms at 64 (0.18ms).
// Without the give-way, eight was 0.67-0.97ms and 64 was 3.1-3.4ms.
const SessionGroupMax = 8

// A write is one client's write waiting for the transaction that will
// store it.
type write struct {
	// s is the session store whose in-memory counts the write changes, nil
	// for one that changes none; its lock is held by the leader for the
	// whole of the group, so check, run and apply run under it and never
	// take it.
	s *Sessions

	// relief opens the reserve for run (DB.reliefTx), decided by check
	// where check is set.
	relief bool

	// check reads, and refuses this member alone; run writes, and anything
	// it returns ends the transaction for every member. Both may run more
	// than once - each time on a fresh transaction, after apply has been
	// undone - so each starts from nothing it set before.
	check func(*sql.Tx) error
	run   func(*sql.Tx) error

	// apply moves the store's in-memory counts once run has succeeded, and
	// answers what puts them back. A later member in the same transaction
	// sees them moved, as it sees the rows run wrote; a transaction that does
	// not commit takes both back before anybody is answered.
	apply func() (undo func())

	// again, where set, is asked after the write ran alone and the provider
	// was out of room: true where it has changed what it will write, and is
	// to be run alone once more (an ending that will not return its
	// deliveries, Sessions.drop).
	again func() bool

	// answers marks a write a client's packet is waiting on - a CONNACK, a
	// SUBACK, a PUBREC or PUBCOMP - served ahead of the writes nobody is
	// waiting on (handOn). A write for a client with a packet waiting is
	// served as one too, wherever it was queued (DB.Waiting).
	answers bool

	// client is the client id the write is one client's for, and "" for a
	// write that is nobody's (a shared group's cursor, a bridge's position):
	// what DB.Waiting finds it by.
	client string

	// departed marks a write for a client that has gone - its disconnect
	// recorded, its session ended, what a refused write still owes - which
	// takes turns with a connected session's writes (nextBackground). It
	// means nothing on a write that answers.
	departed bool

	joined  uint64
	refused bool
	err     error
}

// ranOf is what a leader of a group of writes of this class has run.
func ranOf(g *group) int {
	switch {
	case g.answers:
		return ranAnswers
	case g.departed:
		return ranDeparted
	}
	return ranConnected
}

// writeTx is the transaction a write group is running and the first
// statement that failed on it.
type writeTx struct {
	tx     *sql.Tx
	failed error
}

// note ends a write group's transaction at the first statement that fails on
// it, and keeps what it failed with. SQLite may already have rolled the
// transaction back - on SQLITE_FULL, SQLITE_IOERR, SQLITE_NOMEM or
// SQLITE_BUSY it can, and does for every statement this store writes with -
// and a statement sent afterwards on the same connection would run outside
// any transaction and commit alone. Rolled back here, database/sql refuses
// every later statement on it with sql.ErrTxDone without reaching SQLite, so
// a member that ignored an error cannot write past it.
//
// sql.ErrNoRows is not a failure: no statement answers it but a read that
// found nothing, which is an outcome - a session not held, a Will not armed,
// an entry already gone.
func (d *DB) note(tx *sql.Tx, err error) {
	if err == nil || tx == nil || errors.Is(err, sql.ErrNoRows) {
		return
	}
	w := d.writeTx.Load()
	if w == nil || w.tx != tx || w.failed != nil {
		return
	}
	w.failed = err
	_ = tx.Rollback()
}

// joinWrite stores w in the next transaction to begin and returns when it has
// ended, with w's answer: nil where it is stored and committed.
func (d *DB) joinWrite(w *write) error {
	d.forming.Lock()
	if !w.answers && w.client != "" && d.marks[w.client] > 0 {
		w.answers = true
	}
	if w.answers {
		w.departed = false
	}
	d.joined++
	w.joined = d.joined
	if !d.committing {
		d.committing = true
		d.forming.Unlock()
		d.runWrites([]*write{w})
		d.handOn(ranOf(&group{answers: w.answers, departed: w.departed}))
		return w.err
	}
	var g *group
	if last := d.lastWrites(w.answers, w.departed); last != nil && len(last.writes) < SessionGroupMax {
		g = last
	}
	leader := g == nil
	if leader {
		g = &group{done: make(chan struct{}), turn: make(chan struct{}), writes: make([]*write, 0, 8),
			answers: w.answers, departed: w.departed}
		d.queueWrites(g)
	}
	g.writes = append(g.writes, w)
	if !g.answers && w.client != "" {
		d.queuedFor[w.client]++
	}
	d.forming.Unlock()

	if !leader {
		<-g.done
		return w.err
	}
	<-g.turn
	dones := []chan struct{}{g.done}
	ran := ranOf(g)
	for rest := d.runWrites(g.writes); len(rest) > 0; {
		// **Yielded** (attempt): what is left queues again as a group of its
		// own that this leader leads too, and every member is answered once
		// the last of it has run. **At the front**, keeping its place: the
		// publishes waiting go first (handOn), and then this, before every
		// group that joined after it. Queued at the back, a remainder went
		// behind every group queued while it ran, and on a Raspberry Pi -
		// where the budget cuts a group every three writes - a reconnecting
		// client's session write was sent to the back again and again: its
		// CONNACK took up to 10s, measured, against 5s before writes were
		// collected at all.
		//
		// **A remainder holding the write of a client now waiting answers**,
		// as a queued group holding one is moved up to (DB.Waiting): the
		// client's packet came in while the group ran.
		d.forming.Lock()
		next := &group{done: make(chan struct{}), turn: make(chan struct{}), writes: rest, answers: g.answers,
			departed: g.departed}
		if !next.answers && slices.ContainsFunc(rest, func(w *write) bool { return d.marks[w.client] > 0 }) {
			next.answers, next.departed = true, false
		}
		d.waiting = slices.Insert(d.waiting, 0, next)
		d.countQueued(next, 1)
		d.forming.Unlock()
		d.handOn(ran)
		<-next.turn
		d.letPublishesIn()
		ran = ranOf(next)
		dones = append(dones, next.done)
		rest = d.runWrites(next.writes)
	}
	d.handOn(ran)
	for _, done := range dones {
		close(done)
	}
	return w.err
}

// runWrites stores ws, and gives each its answer.
//
// **Bounded, however many fail.** All of them in one transaction; where a
// member ends it, all but that one in one more; then the one that ended the
// first, and every member of a second that also ended, each alone - in the
// order they joined, and each exactly as it ran before writes were collected,
// so its answer is the one it would have had. Never more than two shared
// transactions, so a reconnect storm into a full provider costs what it
// did with a transaction each, not the square of it.
//
// **The write lock, then the session store's lock, then the connection**,
// held from the first transaction to the last member run alone. Nothing a
// member runs takes a lock (TestNoClientWriteTakesALockOrReadsOffItsTransaction),
// and nothing holds the session store's lock while it waits for the
// connection but this, so there is no second order to meet.
//
// It answers the writes it has not run where the first transaction gave way
// (attempt), for the caller to queue again.
func (d *DB) runWrites(ws []*write) []*write {
	d.writing.Lock()
	defer d.writing.Unlock()
	for _, w := range ws {
		if w.s != nil {
			w.s.mu.Lock()
			defer w.s.mu.Unlock()
			break
		}
	}

	if len(ws) == 1 {
		d.alone(ws[0])
		return nil
	}
	left := ws
	var (
		alone []*write
		rest  []*write
	)
	for round := 0; round < 2 && len(left) > 0; round++ {
		at, ran := d.attempt(left, round == 0)
		if at < 0 {
			rest = left[ran:] // committed, or a failure every member was told
			break
		}
		alone = append(alone, left[at])
		left = slices.Delete(slices.Clone(left), at, at+1)
		if round == 1 {
			alone = append(alone, left...)
		}
	}
	slices.SortFunc(alone, func(a, b *write) int { return cmp.Compare(a.joined, b.joined) })
	for _, w := range alone {
		d.alone(w)
	}
	return rest
}

// alone runs w in a transaction of its own, as it ran before writes were
// collected: once, and once more where it was refused for room and has
// something smaller to write instead (write.again).
func (d *DB) alone(w *write) {
	if at, _ := d.attempt([]*write{w}, false); at == 0 && w.again != nil && errors.Is(w.err, store.ErrProviderFull) && w.again() {
		d.attempt([]*write{w}, false)
	}
}

// attempt runs ws in one transaction and commits it. It answers the index of
// the member whose failure ended the transaction, with nothing stored and
// every in-memory count as it was, or -1 and how many of ws it ran:
// committed, each of those answered - nil, or its own refusal - or the
// transaction failed for them all, each told so.
//
// **It gives way, where it may (yield).** After each member it looks for a
// publish waiting, and for its own time running past writeGroupBudget; on
// either it commits what it has run and leaves the rest. A publish arriving
// while a group of writes runs waits for the member in progress and a
// commit, not for the group: measured with 10,000 sessions reconnecting and
// ten publishers sending, a PUBACK's median was 3.3ms behind whole groups of
// 64.
func (d *DB) attempt(ws []*write, yield bool) (int, int) {
	d.preparePending()
	begun := time.Now()
	tx, err := d.db.Begin()
	if err != nil {
		for _, w := range ws {
			w.err = d.failure(err)
		}
		return -1, len(ws)
	}
	cur := &writeTx{tx: tx}
	d.writeTx.Store(cur)
	defer d.writeTx.Store(nil)

	var undo []func()
	takeBack := func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
		d.restoreCeiling()
	}
	abort := func(i int, err error) (int, int) {
		if cur.failed == nil {
			_ = tx.Rollback()
		}
		takeBack()
		ws[i].err = d.failure(err)
		return i, 0
	}
	bounded := d.fullPages > 0
	ran := len(ws)
	for i, w := range ws {
		if i > 0 && yield && d.shouldYield(begun) {
			ran = i
			break
		}
		w.err, w.refused = nil, false
		if w.check != nil {
			if err := w.check(tx); err != nil {
				if cur.failed != nil {
					return abort(i, cur.failed)
				}
				// **Only a refusal is a refusal.** Any other error a check
				// reads - a row that would not decode, or a later step of a
				// multi-row read that failed, which SQLite may already have
				// rolled the transaction back for without the statement
				// helpers seeing it (note sees a query's first step) - ends
				// the transaction as a failed statement does, so that no
				// member after it writes outside one.
				if !refusal(err) {
					return abort(i, err)
				}
				w.err, w.refused = d.failure(err), true
				continue
			}
		}
		if w.relief && bounded {
			if _, err := d.setCeiling(tx, d.fullPages); err != nil {
				return abort(i, err)
			}
		}
		err := w.run(tx)
		if err == nil && cur.failed == nil && w.relief && bounded {
			_, err = d.setCeiling(tx, d.publishPages)
		}
		if cur.failed != nil {
			err = cur.failed
		}
		if err != nil {
			return abort(i, err)
		}
		if w.apply != nil {
			undo = append(undo, w.apply())
		}
		if d.afterMember != nil {
			d.afterMember(w)
		}
	}
	err = nil
	if d.failCommit != nil {
		err = d.failCommit()
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if err != nil {
		takeBack()
		for _, w := range ws[:ran] {
			w.err = d.failure(err)
		}
		return -1, ran
	}
	d.committed.Add(1)
	return -1, ran
}

// writeGroupBudget is the longest a group of client writes keeps adding
// members to its transaction before it commits what it has and queues the
// rest again (attempt).
//
// A member costs 20-40us on the Ryzen box and about 1.5ms on the Raspberry
// Pi 4 (measured per connecting client). What a group buys is the commit's
// fixed cost shared, and a few members already share most of it; what it
// costs is everything else waiting on the one writer - another client's
// write, a read on the write connection, a retention sweep - for as long as
// it runs. 5ms is a whole group of SessionGroupMax on the Ryzen box and
// three members on the Pi.
const writeGroupBudget = 5 * time.Millisecond

// shouldYield reports whether a write group should commit what it has run:
// a publish is waiting, or it has run for its budget (writeGroupBudget).
func (d *DB) shouldYield(begun time.Time) bool {
	if time.Since(begun) >= d.budget {
		return true
	}
	if uint32(d.publishes.Load()) > 0 {
		return true
	}
	d.forming.Lock()
	defer d.forming.Unlock()
	for _, g := range d.waiting {
		if g.writes == nil {
			return true
		}
	}
	return false
}

// letPublishesIn holds a group that gave way back until the publishes that
// were waiting for the writer when it did have run. Unlocking DB.writing and
// locking it again at once is won by the locker already running, not by the
// publish that has waited, so without this the publish a group gave way to
// would still wait behind the rest of it. Bounded: it waits for as many
// publishes to end as were waiting, not for there to be none, and each of
// them ends after its own transaction: a wait on storage.
//
// **What is running and what has ended are read in one load**
// (DB.publishes), so a publish that ends as the group looks is counted once:
// as running, and its ending awaited, or as ended, and not.
func (d *DB) letPublishesIn() {
	seen := d.publishes.Load()
	n := uint32(seen)
	if n == 0 {
		return
	}
	if d.lookedAtPublishes != nil {
		d.lookedAtPublishes()
	}
	// Compared as a difference, so that the count wrapping past 2^32 does
	// not end the wait early or make it endless.
	target := uint32(seen>>32) + n
	for int32(uint32(d.publishes.Load()>>32)-target) < 0 {
		time.Sleep(50 * time.Microsecond)
	}
}

// refusal reports whether a check's error is one of the answers a write
// gives for itself without ending the transaction: a session not held, a
// window full, a group with no cursor, or an ending with nothing to end.
// Every check in this package answers one of these or fails.
func refusal(err error) bool {
	return errors.Is(err, store.ErrNoSession) || errors.Is(err, store.ErrWindowFull) ||
		errors.Is(err, store.ErrNoShareGroup) || errors.Is(err, errNotEnded)
}

// failure is what tx would have answered for err: ErrProviderFull for a
// provider out of room, and the provider named.
func (d *DB) failure(err error) error {
	if full(err) {
		return fmt.Errorf("storage %s: %w", d.path, store.ErrProviderFull)
	}
	return fmt.Errorf("storage %s: %w", d.path, err)
}

// restoreCeiling puts the publish ceiling back after a transaction that may
// have ended with the reserve open: max_page_count is the connection's, and
// no rollback undoes it. SQLite will not set it below the pages the file
// holds, which is the reserve spent once, as reliefTx's is.
func (d *DB) restoreCeiling() {
	if d.fullPages > 0 {
		_, _ = d.setCeiling(d.db, d.publishPages)
	}
}

// Waiting marks client as having a packet waiting on the store, until what
// it returns is called: every write for it is served as one a client waits
// on, those already queued moved up among them.
//
// **A packet waits on what is queued for its client id**, not only on the
// write it makes. A CONNECT waits on the id's session lock, which a departing
// connection holds while its Disconnected or ending is written; a SUBSCRIBE
// waits on its record's write and, with Retain Handling 2, its position's; a
// seek on its position's. The departed client's writes answer no packet of
// their own and a connected session's position is written in its turn, so
// both queue behind the writes that answer - and ten thousand clients
// disconnecting at once put a reconnecting client's CONNACK behind every one
// of them. So each queued group holding one of the id's writes moves up
// among the answering groups, in its turn, and every write for the id that
// joins meanwhile is one. A group is moved whole, rather than one member
// taken out of it, because its members wait on the group's own answer
// (joinWrite).
//
// **It takes the forming lock and nothing else**, and calls nothing: the
// engine calls it with the client's locks free or held, and a write it
// promotes may be the one a session lock's holder waits on. The scan runs
// only where the id has a write queued among the others (queuedFor).
func (d *DB) Waiting(client string) (answered func()) {
	if client == "" {
		return func() {}
	}
	d.forming.Lock()
	d.marks[client]++
	for i := 0; d.queuedFor[client] > 0 && i < len(d.waiting); i++ {
		g := d.waiting[i]
		if g.writes == nil || g.answers || !slices.ContainsFunc(g.writes, func(w *write) bool { return w.client == client }) {
			continue
		}
		// Taken out and queued again as an answering group (queueWrites):
		// ahead of the first group of the others, so past none of them, and
		// the loop goes on from the group that followed it, or passes over
		// only groups this scan skips.
		d.countQueued(g, -1)
		d.waiting = slices.Delete(d.waiting, i, i+1)
		g.answers, g.departed = true, false
		d.queueWrites(g)
	}
	d.forming.Unlock()
	done := false
	return func() {
		d.forming.Lock()
		defer d.forming.Unlock()
		if done {
			return
		}
		done = true
		if d.marks[client]--; d.marks[client] <= 0 {
			delete(d.marks, client)
		}
	}
}

// countQueued moves DB.queuedFor by delta for each one client's write in g,
// where g is a group of the writes that answer nobody. Called under
// DB.forming, as g enters or leaves DB.waiting or starts to answer.
func (d *DB) countQueued(g *group, delta int) {
	if g.writes == nil || g.answers {
		return
	}
	for _, w := range g.writes {
		if w.client == "" {
			continue
		}
		if d.queuedFor[w.client] += delta; d.queuedFor[w.client] <= 0 {
			delete(d.queuedFor, w.client)
		}
	}
}

// clientOfReader is the client id an MQTT consumer's position is held under,
// and "" for any other reader (a bridge's).
func clientOfReader(reader string) string {
	c, ok := strings.CutPrefix(reader, store.MQTTReader(""))
	if !ok {
		return ""
	}
	return c
}
