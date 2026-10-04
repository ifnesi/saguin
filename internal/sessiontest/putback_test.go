package sessiontest_test

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/store"
)

// refusesSaveAfter refuses one client's Session Saves while armed, after
// letting `allow` of them through.
type refusesSaveAfter struct {
	broker.SessionStore
	client  string
	armed   atomic.Bool
	allow   atomic.Int32
	refused atomic.Int32
}

func (s *refusesSaveAfter) Unwrap() broker.SessionStore { return s.SessionStore }

func (s *refusesSaveAfter) Save(sess store.Session) error {
	if sess.Client == s.client && s.armed.Load() && s.allow.Add(-1) < 0 {
		s.refused.Add(1)
		return errors.New("disk I/O error (refused for the test)")
	}
	return s.SessionStore.Save(sess)
}

// Invariant 17 and 18: a connection whose CONNACK never arrived wrote the
// session record of a live client id over, and the broker puts it back.
// Where the store refuses that write, it is written again until it lands,
// so the live session's client - told its subscription was granted - finds
// it after a crash, rather than a record that connection left: here one
// begun with Clean Start, holding nothing.
func TestARecordAFailedConnectionWroteOverIsPutBackWhenTheStoreRecovers(t *testing.T) {
	rs := &refusesSaveAfter{client: "dev"}
	var inner broker.SessionStore
	brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
		inner = s
		if rs.SessionStore == nil {
			rs.SessionStore = s
			return rs
		}
		return s
	}
	t.Cleanup(func() { brokertest.WrapSessions = nil })
	path := filepath.Join(t.TempDir(), "saguin.db")
	h := brokertest.StartDurableSQLite(t, path)
	dev := dial(t, h, "dev", false, false, 0, 3600, 0)
	dev.Sub(t, "events/#", 1)

	// The failed connection's own write passes; putting the live one's back
	// is refused, for a while.
	rs.allow.Store(1)
	rs.armed.Store(true)
	connackNeverArrives(t, h, connectFor(t, "dev", true, 3600, ""))
	if rs.refused.Load() == 0 {
		t.Fatal("the put-back was never refused, so this proves nothing")
	}
	rec, _, _ := inner.Get("dev")
	t.Logf("while refusing, the record holds %d subscriptions", len(rec.Subscriptions))
	// Refused again by a retry, so the retry is what puts it back.
	if !waitUntil(5*time.Second, func() bool { return rs.refused.Load() >= 2 }) {
		t.Fatal("the refused put-back was never written again")
	}
	rs.armed.Store(false)
	// The store has recovered and the broker runs on: put back, the record
	// holds the live session's subscription again. Read rather than slept
	// through, since a retry runs on the broker's own second.
	if !waitUntil(5*time.Second, func() bool {
		rec, ok, err := inner.Get("dev")
		return err == nil && ok && len(rec.Subscriptions) == 1
	}) {
		rec, _, _ := inner.Get("dev")
		t.Fatalf("the store took writes again and the record still holds %d subscriptions: the "+
			"put-back was never written", len(rec.Subscriptions))
	}

	h.Crash()
	h = brokertest.StartDurableSQLite(t, path)
	again := dial(t, h, "dev", false, false, 0, 3600, 0)
	if !again.SessionPresent {
		t.Fatal("the session did not survive the crash")
	}
	p := connect(t, h, "publisher", true, false)
	p.Pub(t, "events/x", "after the crash")
	if r, ok := again.Await(t, 3*time.Second); !ok || r.Payload != "after the crash" {
		rec, _, _ := inner.Get("dev")
		t.Fatalf("after the crash the resumed session was served nothing (record holds %d subscriptions): "+
			"the record a connection that never completed wrote was never put back", len(rec.Subscriptions))
	}
}

// holdsSaveAfter is refusesSaveAfter that, once hold is set, holds the next
// Save of the client until released.
type holdsSaveAfter struct {
	refusesSaveAfter
	hold    atomic.Bool
	reached chan struct{}
	release chan struct{}
	passes  atomic.Int64
}

func (s *holdsSaveAfter) Save(sess store.Session) error {
	if sess.Client == s.client && s.hold.CompareAndSwap(true, false) {
		close(s.reached)
		<-s.release
	}
	return s.refusesSaveAfter.Save(sess)
}

// Disconnected refuses every disconnect of the probe client, and counts it.
// The broker writes a refused one again on each pass of its retries, so the
// count is the passes: what a test reads to know that a retry has run since,
// rather than sleeping past one.
func (s *holdsSaveAfter) Disconnected(id string, at time.Time, dropWill bool, expiry uint32) error {
	if id == "probe" {
		s.passes.Add(1)
		return errors.New("disk I/O error (refused for the test)")
	}
	return s.refusesSaveAfter.Disconnected(id, at, dropWill, expiry)
}

// Invariant 17: putting the record back again never writes over a session a
// newer connection owns. The retry's write is held while a CONNECT with
// Clean Start takes the id over: the CONNECT waits for it, and the session
// it begins holds nothing of the one it replaced - repeated, since the
// hand-over between the two is scheduling.
func TestAPutBackRetriedNeverWritesOverTheNextConnection(t *testing.T) {
	for i := range 10 {
		rs := &holdsSaveAfter{refusesSaveAfter: refusesSaveAfter{client: "dev"},
			reached: make(chan struct{}), release: make(chan struct{})}
		var inner broker.SessionStore
		brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
			inner, rs.SessionStore = s, s
			return rs
		}
		t.Cleanup(func() { brokertest.WrapSessions = nil })
		h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
		dev := dial(t, h, "dev", false, false, 0, 3600, 0)
		dev.Sub(t, "events/#", 1)
		// A durable client whose clean disconnect the store refuses: written
		// again on every pass of the retries, so it counts them.
		gone(t, dial(t, h, "probe", false, false, 0, 3600, 0))
		rs.allow.Store(1)
		rs.armed.Store(true)
		connackNeverArrives(t, h, connectFor(t, "dev", true, 3600, ""))
		if rs.refused.Load() == 0 {
			t.Fatalf("round %d: the put-back was never refused, so this proves nothing", i)
		}
		rs.hold.Store(true)
		rs.armed.Store(false)
		select {
		case <-rs.reached:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the refused put-back was never written again", i)
		}
		done := make(chan *client, 1)
		go func() { done <- dial(t, h, "dev", true, false, 0, 3600, 0) }()
		// **The CONNECT under way while the retry is held**: waiting for the
		// id's session lock the retry holds, or - where nothing makes it wait
		// - answered already.
		var next *client
		for deadline := time.Now().Add(5 * time.Second); next == nil &&
			h.Srv.SessionLockWaiters("dev") < 2; {
			select {
			case next = <-done:
			case <-time.After(time.Millisecond):
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: the CONNECT neither waited for the held retry nor was answered", i)
			}
		}
		close(rs.release)
		if next == nil {
			next = <-done
		}
		// **Past another retry, counted rather than slept through.** Each
		// pass refuses the probe's disconnect once, in an order of its own,
		// so three refusals from here are a whole pass begun and ended
		// after the new session took the id.
		mark := rs.passes.Load()
		if !waitUntil(10*time.Second, func() bool { return rs.passes.Load() >= mark+3 }) {
			t.Fatalf("round %d: the broker's retries stopped running, so nothing here says whether "+
				"one wrote over the new session", i)
		}
		rec, ok, err := inner.Get("dev")
		if err != nil || !ok || len(rec.Subscriptions) != 0 {
			t.Fatalf("round %d: the session a Clean Start began holds %d subscriptions (kept=%v, %v): "+
				"the put-back of the one it replaced was written over it", i, len(rec.Subscriptions), ok, err)
		}
		next.Close()
		h.Stop()
	}
}

// A record owed to be put back is put back before anything reads it to build
// on: a SUBSCRIBE from the live session, arriving once the store takes writes
// and before any retry, keeps the subscriptions it already had beside the new
// one, rather than adding it to the record the failed connection left.
func TestASubscribeBuildsOnThePutBackRecord(t *testing.T) {
	for i := range 10 {
		rs := &refusesSaveAfter{client: "dev"}
		var inner broker.SessionStore
		brokertest.WrapSessions = func(s broker.SessionStore) broker.SessionStore {
			inner, rs.SessionStore = s, s
			return rs
		}
		t.Cleanup(func() { brokertest.WrapSessions = nil })
		h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
		dev := dial(t, h, "dev", false, false, 0, 3600, 0)
		dev.Sub(t, "events/#", 1)
		rs.allow.Store(1)
		rs.armed.Store(true)
		// The failed connection asked for a different expiry, and a Will.
		connackNeverArrives(t, h, connectFor(t, "dev", true, 60, "wills/failed"))
		if rs.refused.Load() == 0 {
			t.Fatalf("round %d: the put-back was never refused, so this proves nothing", i)
		}
		rs.armed.Store(false)
		dev.Sub(t, "more/#", 1)
		rec, _, _ := inner.Get("dev")
		var got []string
		for _, s := range rec.Subscriptions {
			got = append(got, s.Filter)
		}
		if len(got) != 2 || rec.ExpiryInterval != 3600 || rec.Will != nil {
			t.Fatalf("round %d: after a SUBSCRIBE the record holds %v, expiry %d, Will %v; want events/# and "+
				"more/#, 3600, none: it was built on what the failed connection left", i, got,
				rec.ExpiryInterval, rec.Will != nil)
		}
		dev.Close()
		h.Stop()
	}
}
