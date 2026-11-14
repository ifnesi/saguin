package core_test

import (
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The test of the store-read metric refresh's cut, driven inside: which
// subscriptions a refresh counts when sessions change between the refresh's
// place in the store-request order and its answer is an ordering no client
// can force (design.md, Testing: "A test inside the code checks only what
// neither can see [...] orderings no client can force"). It drives the
// core with the real store behind it (liveScript), the core's timer read
// from Timer, so it needs no seam of its own.

// metrics.md, Rules: "Store-derived gauges share the Store-read cut used for
// that refresh (design.md, Store reads)"; saguin_subscriptions,
// "subscriptions held, every session: durable subscriptions from the store
// and non-durable subscriptions held by the core at the same Store-read cut.
// Neither source counts the other's subscriptions. Changes after that cut
// appear in a later refresh". design.md, Store reads: "Every other read used
// for a broker decision has a place in the core's store-request order.
// Earlier accepted writes are resolved before its view is fixed. Later
// writes wait until the reader confirms that its read transaction has
// established that view"; DISCONNECT: "The client may change its session
// expiry"; "A session that ends with its connection is ended in the store
// where it has rows".
//
// Three sessions are connected: d, durable, subscribed to a and b; d2,
// durable, to f; n, whose session ends with its connection, to c. The
// core's timer, nothing else being due, is the refresh: it places the
// refresh's read, which the store has not yet carried out, when d leaves
// with its session expiry changed to 0, ending its session; d2 leaves,
// keeping its session; and n unsubscribes from c and subscribes to d and e.
// Then the store carries out every request in turn, and the connections
// report their close. The refresh counts 4, what its cut held: d's 2 and
// d2's 1 from the store, n's 1 from the core; neither the durable
// subscriptions the store ends after the cut counted beside n's later ones
// (5), nor missed for being gone from the core when its answer comes. The
// next refresh, within min_scrape_interval, counts 3: d2's from the store,
// n's 2.
//
// A queue's held jobs, which the refresh subtracts at that same cut
// ("Queue retention bytes subtract the held jobs at that cut"), wait for
// queues (item 0017c).
func TestSubscriptionsCountedAtTheRefreshCut(t *testing.T) {
	s := newLiveScript(t, "")
	d := s.connect("refresh-d", 3600, 10)
	s.subscribe(d, "a", "b")
	d2 := s.connect("refresh-d2", 3600, 10)
	s.subscribe(d2, "f")
	n := s.connect("refresh-n", 0, 10)
	s.subscribe(n, "c")

	at := s.core.Timer()
	if at.IsZero() {
		t.Fatal("the core's timer is never due: no refresh of the store-read values is to come")
	}
	s.now = later(at, s.now)
	s.take(s.core.Fire(nil, s.now))
	if !slices.ContainsFunc(s.queue, func(op any) bool { _, ok := op.(store.Read); return ok }) {
		t.Fatalf("the core's timer, nothing else due, sent the store no read: requests %v", s.queue)
	}
	zero := uint32(0)
	s.event(connection.Event{Kind: connection.Ended, Conn: d, Packet: disconnectWith(0, &zero)})
	s.event(connection.Event{Kind: connection.Ended, Conn: d2, Packet: &packet.Disconnect{}})
	s.event(connection.Event{Kind: connection.Received, Conn: n, Packet: &connection.Unsubscribe{PacketID: 2, Filters: []string{"c"}}})
	s.event(connection.Event{Kind: connection.Received, Conn: n, Packet: &connection.Subscribe{PacketID: 3,
		Filters: []connection.Subscription{{Filter: "d", QoS: 1}, {Filter: "e", QoS: 1}}}})
	s.serve()
	s.step(connection.Event{Kind: connection.Closed, Conn: d})
	s.step(connection.Event{Kind: connection.Closed, Conn: d2})
	if got := testkit.Sample(t, s.reg, "saguin_subscriptions"); got != 4 {
		t.Errorf("the refresh counts %v subscriptions; want 4, d's 2 and d2's 1 from the store and n's 1, as its cut held them", got)
	}

	s.advance(s.now.Add(time.Minute)) // config.md: min_scrape_interval, 60s where unset
	if got := testkit.Sample(t, s.reg, "saguin_subscriptions"); got != 3 {
		t.Errorf("the next refresh counts %v subscriptions; want 3, d2's 1 from the store and n's 2", got)
	}
}
