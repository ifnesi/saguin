package core_test

import (
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/store"
)

// The test of normal mode's sync timer (design.md, Store, normal mode),
// driven inside with the script's store: when the core asks the writer for
// a sync is an ordering no client can see (design.md, Testing: "A test
// inside the code checks only what neither can see"). That a sync puts the
// log on the disk is seen from outside, in the conformance suite's
// TestNormalModeSyncsTheLogWhateverReadsAreOpen.
//
// The core's sync is told apart by what the design gives it to do, as the
// script's store sees it (syncRequest): "The request takes its place in the
// store-request order like a write"; "The writer syncs the log file
// itself". It is a store.Write sent alone, not a member of a *core.Group: a
// sync commits no transaction, and a group is one (Store, Writes: "The
// writer executes exactly the sealed group as one transaction"). In normal
// mode no other write goes alone. The store answers it as a write:
// store.Answer{Op: sync}, its Err nil. A sync ends when the core applies
// that answer.

// syncEvery is the sync_interval normalScript's core runs with, its least
// (config.md, broker.storage: "at least 100ms").
const syncEvery = 100 * time.Millisecond

// syncs is what the script's store has seen of normal mode's syncs: the
// last one sent, and the times at which each was sent and each ended.
type syncs struct {
	op   store.Write // the last sync sent, nil before the first
	hold bool        // settle leaves a sync outstanding to the test
	sent []time.Time // when each sync was sent, in order
	ends []time.Time // when each sync's answer was applied, in order
}

// syncRequest reports whether op, a request the core has just sent, is
// normal mode's sync (see the top of this file), noting it, and fails the
// test where it is sent while another write is outstanding: Store, normal
// mode, "The request takes its place in the store-request order like a
// write"; Store, Writes, "The core keeps at most one write transaction
// outstanding at the store".
func (s *script) syncRequest(op any) bool {
	s.t.Helper()
	w, ok := op.(store.Write)
	if s.cfg.Broker.Storage.Mode != config.Normal || !ok {
		return false
	}
	if _, group := w.(*core.Group); group {
		return false
	}
	if len(pending[store.Write](s)) != 0 {
		s.t.Fatalf("a sync, %T, sent while another write is outstanding: it takes its place in the store-request order like a write", op)
	}
	s.sync.op = w
	s.sync.sent = append(s.sync.sent, s.now)
	return true
}

// normalScript returns a script of a core in normal mode, its
// sync_interval syncEvery and each event a millisecond after the last, its
// sessions allowed to live connectExpiry, started as the loop starts it:
// its timer handed to it at the start (Run's first act), every request
// answered, then serving.
func normalScript(t *testing.T) *script {
	t.Helper()
	cfg := newConfig(t)
	cfg.Broker.Storage.Mode = config.Normal
	cfg.Broker.Storage.SyncInterval = syncEvery
	cfg.Broker.Limits.MaxSessionExpiry = connectExpiry * time.Second
	s := newScript(t, cfg)
	s.tick = time.Millisecond
	s.fireAt(s.now)
	s.settle()
	if !s.core.Serve() {
		t.Fatal("with every startup request answered, the core is still starting")
	}
	return s
}

// design.md, Store, normal mode: "Every sync_interval, counted from the end
// of the previous sync, the core's timer asks the writer for a sync if
// anything was committed since. The request takes its place in the
// store-request order like a write."
//
// A core in normal mode, sync_interval 100ms, commits a durable session's
// connection and departure; its timer, fired at each deadline it asks for,
// sends a sync within an interval, which the test holds, then answers.
// Another session's connection and departure commit at once: until an
// interval after that answer no sync is sent; the core's timer is then due,
// and fired then, it sends the next sync. That one answered, ten intervals
// pass with nothing committed, the timer fired at each deadline: no sync
// is sent. A third session commits: a sync is sent within an interval.
func TestNormalModeSyncsAnIntervalAfterTheLast(t *testing.T) {
	s := normalScript(t)
	s.stores("first")
	s.sync.hold = true
	s.advance(s.now.Add(syncEvery))
	if n := len(s.sync.sent); n != 1 {
		t.Fatalf("%d syncs sent within a sync interval of a commit; want 1", n)
	}
	s.sync.hold = false
	s.answer(s.sync.op)
	end := s.sync.ends[0]
	s.stores("second")

	due := end.Add(syncEvery)
	s.advance(due.Add(-time.Nanosecond))
	if n := len(s.sync.sent); n != 1 {
		t.Fatalf("a sync sent at %s, before %s, a sync interval after the last one ended at %s",
			clockOf(s.sync.sent[1]), clockOf(due), clockOf(end))
	}
	if at := s.core.Timer(); at.IsZero() || at.After(due) {
		t.Fatalf("with something committed since the last sync, ended at %s, the core's timer is due at %s (zero: never); want by %s",
			clockOf(end), clockOf(at), clockOf(due))
	}
	s.fireAt(due)
	if n := len(s.sync.sent); n != 2 {
		t.Fatalf("a sync interval after the last sync ended, with something committed since, %d syncs sent in all; want 2", n)
	}
	s.settle()

	s.advance(s.now.Add(10 * syncEvery))
	if n := len(s.sync.sent); n != 2 {
		t.Fatalf("%d syncs sent in all, the last %d with nothing committed since the one before; want 2", n, n-2)
	}
	s.stores("third")
	committed := s.now
	s.advance(committed.Add(syncEvery))
	if n := len(s.sync.sent); n != 3 {
		t.Errorf("%d syncs sent in all, a sync interval after a commit at %s that followed ten idle intervals; want 3", n, clockOf(committed))
	}
}
