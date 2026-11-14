package core_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// The tests of memory mode's snapshot timer and its requests (design.md,
// Store, memory mode), driven inside with the script's store: when the core
// sends a snapshot, and what it holds back while one runs, are orderings no
// client can force nor see (design.md, Testing: "A test inside the code
// checks only what neither can see"). The wire tests are in the
// conformance suite's snapshots_test.go.
//
// The core's snapshot requests are told apart by what the design gives
// them to do, as the script's store sees them (snapshotRequest):
//   - the fold: Store, memory mode, "A snapshot request takes its place in
//     the store-request order like a write [...] At its turn, the writer
//     folds the whole log into the database file (a TRUNCATE checkpoint)".
//     It is a store.Write sent alone, not a member of a *core.Group: a
//     checkpoint runs outside any transaction, and a group is one
//     (Store, Writes: "The writer executes exactly the sealed group as one
//     transaction"). In memory mode no other write goes alone. The store
//     answers it as a write: store.Answer{Op: fold}, its Err nil;
//   - the copy: "then a store reader opens a read, which keeps SQLite from
//     changing that file. Writes then resume and collect in the log while
//     that store reader copies the file". It is a store.Read, of a type the
//     script's store reads nothing for, sent once the fold's answer is
//     applied. As every read with a place in the store-request order, it is
//     answered first with the confirmation that its view is established,
//     store.Answer{Op: copy, View: true} (Store reads: "Later writes wait
//     until the reader confirms that its read transaction has established
//     that view"), then with its result, store.Answer{Op: copy}, Err nil
//     for a snapshot written and safe on disk.
// A snapshot ends when the core applies its copy's result.

// snapshotInterval is the snapshot_interval ramScript's core runs with,
// its least (config.md, broker.storage: "memory mode's snapshot period, at
// least 1m").
const snapshotInterval = time.Minute

// snapshots is what the script's store has seen of memory mode's snapshots:
// the last one's fold and copy, whether one runs, and the times at which
// each was sent and each ended.
type snapshots struct {
	fold    store.Write // the last snapshot's fold, nil before the first
	folded  bool        // its fold answered
	copy    store.Read  // its copy, nil until sent
	running bool        // from its fold's sending to its copy's result
	hold    bool        // settle leaves a snapshot's requests to the test
	folds   []time.Time // when each fold was sent, in order
	ends    []time.Time // when each copy's result was applied, in order
}

// snapshotRequest reports whether op, a request the core has just sent, is
// one of memory mode's snapshot requests (see the top of this file), noting
// it, and fails the test where it breaks the design's rules for them:
// Store, memory mode, "Snapshots never overlap"; Store, Writes, "The core
// keeps at most one write transaction outstanding at the store".
func (s *script) snapshotRequest(op any) bool {
	s.t.Helper()
	if s.cfg.Broker.Storage.Mode != config.Memory {
		return false
	}
	switch op := op.(type) {
	case *core.Group, *store.LoadSession, *store.DueSessions:
		return false
	case store.Write:
		switch {
		case s.snap.running:
			s.t.Fatalf("a snapshot's fold, %T, sent while the last snapshot runs: snapshots never overlap (Store, memory mode)", op)
		case len(pending[store.Write](s)) != 0:
			s.t.Fatalf("a snapshot's fold, %T, sent while another write is outstanding: it takes its place in the store-request order like a write", op)
		}
		s.snap.fold, s.snap.folded, s.snap.copy, s.snap.running = op, false, nil, true
		s.snap.folds = append(s.snap.folds, s.now)
		return true
	case store.Read:
		if !s.snap.folded || s.snap.copy != nil {
			s.t.Fatalf("a read of type %T, which the script's store does not read, sent outside a snapshot's turn: want it once the snapshot's fold is answered", op)
		}
		s.snap.copy = op
		return true
	}
	return false
}

// snapshotWritable fails the test where a write is sent between a
// snapshot's fold and the confirmation that its copy's read is open:
// Store, memory mode, "the writer folds the whole log into the database
// file [...] then a store reader opens a read, which keeps SQLite from
// changing that file. Writes then resume"; Store reads, "Later writes wait
// until the reader confirms that its read transaction has established that
// view".
func (s *script) snapshotWritable() {
	s.t.Helper()
	if !s.snap.running {
		return
	}
	for _, r := range s.outstanding {
		if r.op == s.snap.copy && r.viewed {
			return
		}
	}
	s.t.Fatalf("a write sent after the snapshot's fold, before its copy's read confirmed its view: the write may change the file the snapshot copies")
}

// answered notes the store's answer to op, given at the script's present
// time: a snapshot's fold answered, or its copy's result, which ends it; or
// a sync's (sync_test.go).
func (s *script) answered(op any) {
	switch {
	case s.snap.fold != nil && op == s.snap.fold:
		s.snap.folded = true
	case s.snap.copy != nil && op == s.snap.copy:
		s.snap.running = false
		s.snap.ends = append(s.snap.ends, s.now)
	case s.sync.op != nil && op == s.sync.op:
		s.sync.ends = append(s.sync.ends, s.now)
	}
}

// holds reports whether settle leaves op outstanding: a snapshot's fold or
// copy, or a sync, while the test holds them.
func (s *script) holds(op any) bool {
	return op != nil && ((s.snap.hold && (op == s.snap.fold || op == s.snap.copy)) || (s.sync.hold && op == s.sync.op))
}

// ramScript returns a script of a core in memory mode, its
// snapshot_interval snapshotInterval and its sessions allowed to live
// connectExpiry, started as the loop starts it: its timer handed to it at
// the start (Run's first act), every request answered, then serving.
func ramScript(t *testing.T) *script {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.Storage.Mode = config.Memory
	cfg.Broker.Storage.SnapshotInterval = snapshotInterval
	cfg.Broker.Limits.MaxSessionExpiry = connectExpiry * time.Second
	s := newScript(t, cfg)
	s.fireAt(s.now)
	s.settle()
	if !s.core.Serve() {
		t.Fatal("with every startup request answered, the core is still starting")
	}
	return s
}

// stores connects a durable MQTT 5 session for client and has it leave,
// with a DISCONNECT keeping its expiry: its CONNECT and its departure are
// committed (design.md, CONNECT, 7. Store; DISCONNECT: "The change is stored
// before the connection closes"), every request answered but those held.
func (s *script) stores(client string) {
	s.t.Helper()
	conn := s.connected(keeping(packet.V5, client, connectExpiry))
	s.end(conn, disconnectWith(0, nil), 0)
	s.settle()
	s.close(conn)
}

// heldFold returns the one snapshot's fold the test holds, outstanding,
// failing the test unless one is.
func (s *script) heldFold(when string) store.Write {
	s.t.Helper()
	if !s.snap.running || s.snap.folded {
		s.t.Fatalf("%s, no snapshot's fold is outstanding (%d sent)", when, len(s.snap.folds))
	}
	return s.snap.fold
}

// sentCopy returns the copy the core sent for the snapshot running,
// failing the test unless it did, upon its fold's answer.
func (s *script) sentCopy() store.Read {
	s.t.Helper()
	if s.snap.copy == nil {
		s.t.Fatal("the snapshot's fold answered, and no read sent to copy the folded file: a store reader opens a read at its turn (Store, memory mode)")
	}
	return s.snap.copy
}

// design.md, Store, memory mode: "Snapshots never overlap. Each
// snapshot_interval is counted from the end of the previous snapshot";
// "Every snapshot_interval [...] a snapshot copies the RAM copy's database
// file"; Execution, Event order: the core's timers run in the order
// "commit cutoff, store sync, metric refresh, snapshot".
//
// A core in memory mode, snapshot_interval 1m, commits a durable session's
// connection and departure. Its timer, fired at each deadline it asks for,
// sends a snapshot's fold within a snapshot interval of the start; the
// test holds the snapshot. Its fold answered, its copy's view confirmed and
// its result held, another session's connection and departure commit, and
// three intervals pass, the timer fired at each deadline: no other
// snapshot is sent (snapshotRequest). The copy's result is applied: until
// a snapshot interval after that answer, no snapshot is sent; the core's
// timer is then due, and fired then, it sends the next snapshot.
func TestSnapshotsNeverOverlap(t *testing.T) {
	s := ramScript(t)
	start := s.now
	s.stores("before")
	s.snap.hold = true
	s.advance(start.Add(snapshotInterval))
	s.answer(s.heldFold("a snapshot interval after the start, something committed"))
	running := s.sentCopy()
	s.stores("during")
	s.advance(s.now.Add(3 * snapshotInterval))
	if n := len(s.snap.folds); n != 1 {
		t.Fatalf("%d snapshots sent while the first one's copy was outstanding; want that one alone", n)
	}

	s.snap.hold = false
	s.answer(running)
	due := s.snap.ends[0].Add(snapshotInterval)
	s.advance(due.Add(-time.Nanosecond))
	if n := len(s.snap.folds); n != 1 {
		t.Fatalf("a snapshot sent at %s, before %s, a snapshot interval after the last one ended at %s",
			clockOf(s.snap.folds[1]), clockOf(due), clockOf(s.snap.ends[0]))
	}
	if at := s.core.Timer(); at.IsZero() || at.After(due) {
		t.Fatalf("with something committed since the last snapshot, ended at %s, the core's timer is due at %s (zero: never); want by %s",
			clockOf(s.snap.ends[0]), clockOf(at), clockOf(due))
	}
	s.fireAt(due)
	if n := len(s.snap.folds); n != 2 {
		t.Errorf("a snapshot interval after the last snapshot ended, with something committed since, %d snapshots sent in all; want 2", n)
	}
}

// design.md, Store, memory mode: "A snapshot, the final one included, is
// skipped when nothing was committed since the previous one"; "The core
// sends the final snapshot as its last store request"; "A start replaces
// any RAM copy left there, since a restart recovers from file_path": a
// broker starts with file_path holding all the store holds.
//
// A core in memory mode, snapshot_interval 1m, with nothing committed since
// its start, has its timer fired at each deadline it asks for over three
// intervals: it sends no snapshot. A durable session's connection and
// departure commit: within an interval, one snapshot is sent and answered.
// Three more intervals pass with nothing committed: no other is sent. The
// stop begins, no connection open, and every request is answered: no final
// snapshot is sent.
func TestSnapshotSkippedWhenNothingWasCommitted(t *testing.T) {
	s := ramScript(t)
	s.advance(s.now.Add(3 * snapshotInterval))
	if n := len(s.snap.folds); n != 0 {
		t.Fatalf("%d snapshots sent with nothing committed since the start; want none", n)
	}
	s.stores("committed")
	s.advance(s.now.Add(snapshotInterval))
	if n := len(s.snap.folds); n != 1 {
		t.Fatalf("%d snapshots sent within a snapshot interval of a commit; want 1", n)
	}
	s.advance(s.now.Add(3 * snapshotInterval))
	if n := len(s.snap.folds); n != 1 {
		t.Fatalf("%d snapshots sent in all, the last %d with nothing committed since the one before; want 1", n, n-1)
	}
	s.stop()
	s.settle()
	if n := len(s.snap.folds); n != 1 {
		t.Errorf("the stop sent a final snapshot with nothing committed since the last one (%d snapshots in all); want it skipped", n)
	}
}

// design.md, Store, memory mode: "A stop that arrives while a snapshot runs
// takes its final snapshot after that one ends"; "The core sends the final
// snapshot as its last store request"; "Snapshots never overlap"; Restart,
// Graceful stop: "The core waits until every store request it sent, read or
// write, is answered. Memory mode takes its final snapshot, except after a
// store error".
//
// A core in memory mode commits a durable session's connection and
// departure, and its timer sends a snapshot, which the test holds. Its fold
// answered and its copy's view confirmed, another session's connection and
// departure commit, and the stop begins, no connection open; every request
// but the copy is answered: no final snapshot is sent. The copy's result
// is applied: the final snapshot's fold is sent then, with no other request
// outstanding; answering it and what follows sends nothing but its copy.
func TestStopDuringASnapshotTakesTheFinalOneAfterIt(t *testing.T) {
	s := ramScript(t)
	s.stores("before")
	s.snap.hold = true
	s.advance(s.now.Add(snapshotInterval))
	s.answer(s.heldFold("a snapshot interval after the start, something committed"))
	running := s.sentCopy()
	s.stores("during")
	s.stop()
	s.settle()
	if n := len(s.snap.folds); n != 1 {
		t.Fatalf("%d snapshots sent while the first one's copy was outstanding, the stop begun; want that one alone", n)
	}

	s.snap.hold = false
	s.answer(running)
	if n := len(s.snap.folds); n != 2 {
		t.Fatalf("the running snapshot ended during the stop, with something committed since it began, and %d snapshots sent in all; want the final one too", n)
	}
	if len(s.outstanding) != 1 {
		t.Fatalf("the final snapshot sent with %d other store requests outstanding; want it the last store request", len(s.outstanding)-1)
	}
	acts := s.settle()
	reads, writes := 0, 0
	for _, a := range acts {
		reads += countTrue(a.Read != nil)
		writes += countTrue(a.Write != nil)
	}
	if reads != 1 || writes != 0 {
		t.Errorf("after the final snapshot's fold, the core sent %d reads and %d writes; want its copy alone", reads, writes)
	}
}

// design.md, Store, memory mode: "At its turn, the writer folds the whole
// log into the database file (a TRUNCATE checkpoint), then a store reader
// opens a read, which keeps SQLite from changing that file. Writes then
// resume and collect in the log while that store reader copies the file";
// Store reads: "Later writes wait until the reader confirms that its read
// transaction has established that view. Beginning a deferred transaction
// alone is not that confirmation. Later writes may then proceed while the
// read finishes"; Execution, Ready work: "Each change that allows work to
// continue schedules it immediately".
//
// A core in memory mode commits a durable session's connection and
// departure and holds another durable session connected; its timer sends a
// snapshot, which the test holds. While the fold is outstanding, the
// connected client's DISCONNECT is taken: its departure waits (one write at
// a time). The fold's answer sends the copy and no write; with the copy's
// view not yet confirmed, nothing else is sent (snapshotWritable). Its
// view's confirmation sends the departure's group.
func TestNoWriteBetweenTheFoldAndTheReadsConfirmation(t *testing.T) {
	s := ramScript(t)
	s.stores("before")
	conn := s.connected(keeping(packet.V5, "waits", connectExpiry))
	s.snap.hold = true
	s.advance(s.now.Add(snapshotInterval))
	fold := s.heldFold("a snapshot interval after the start, something committed")
	s.end(conn, disconnectWith(0, nil), 0)

	s.holdViews = true
	if groups := groupsIn(s.answer(fold)); len(groups) != 0 {
		t.Fatalf("the fold's answer sent %d groups; want the copy's read alone", len(groups))
	}
	acts := s.view(s.sentCopy())
	if groups := groupsIn(acts); len(groups) != 1 || len(departures(acts)) != 1 {
		t.Fatalf("the copy's view confirmed, the core sent %d groups, %d departures; want the waiting departure's group at once", len(groups), len(departures(acts)))
	}
	s.holdViews, s.snap.hold = false, false
	s.settle()
	s.close(conn)
}

// failedCopy is a snapshot's copy failed, as the store's reader reports one:
// the real copy's error, its source file missing, which leaves the working
// store usable (store.SnapshotFailed).
func failedCopy(t *testing.T) error {
	t.Helper()
	dir := t.TempDir()
	err := (&store.SnapshotCopy{Source: filepath.Join(dir, "missing.db"), Destination: filepath.Join(dir, "saguin.db")}).ReadConn(nil)
	if !store.SnapshotFailed(err) {
		t.Fatalf("copying a missing file answered %v; want a failed snapshot", err)
	}
	return err
}

// design.md, Store, memory mode: "A snapshot, the final one included, is
// skipped when nothing was committed since the last successful one. A
// failed snapshot is therefore tried again at the next interval"; "Each
// snapshot_interval is counted from the end of the previous snapshot".
//
// A core in memory mode, snapshot_interval 1m, commits a durable session's
// connection and departure; its timer sends a snapshot, whose fold is
// answered and whose copy is answered with a failed snapshot. Nothing is
// committed after. Until a snapshot interval after that answer, no
// snapshot is sent; the core's timer is then due, and fired then, it sends
// one, answered in full. Three more intervals pass, nothing committed: no
// other is sent.
func TestFailedSnapshotIsTriedAgain(t *testing.T) {
	s := ramScript(t)
	s.stores("before")
	s.snap.hold = true
	s.advance(s.now.Add(snapshotInterval))
	s.answer(s.heldFold("the snapshot to fail, sent a snapshot interval after a commit"))
	failed := s.sentCopy()
	s.snap.hold = false
	s.reply(failed, failedCopy(t))
	due := s.snap.ends[0].Add(snapshotInterval)
	s.advance(due.Add(-time.Nanosecond))
	if n := len(s.snap.folds); n != 1 {
		t.Fatalf("a snapshot sent at %s, before %s, a snapshot interval after the failed one ended at %s",
			clockOf(s.snap.folds[1]), clockOf(due), clockOf(s.snap.ends[0]))
	}
	if at := s.core.Timer(); at.IsZero() || at.After(due) {
		t.Fatalf("after a failed snapshot ended at %s, nothing committed since, the core's timer is due at %s (zero: never); want by %s",
			clockOf(s.snap.ends[0]), clockOf(at), clockOf(due))
	}
	s.fireAt(due)
	if n := len(s.snap.folds); n != 2 {
		t.Fatalf("a snapshot interval after a failed snapshot, nothing committed since, %d snapshots sent in all; want 2, the failed one tried again", n)
	}
	s.advance(s.now.Add(3 * snapshotInterval))
	if n := len(s.snap.folds); n != 2 {
		t.Errorf("%d snapshots sent in all, after the failed one succeeded at its retry with nothing committed since; want 2", n)
	}
}
