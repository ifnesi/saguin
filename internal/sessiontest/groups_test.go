package sessiontest_test

import (
	"fmt"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/store"
)

// A shared group reading the broadcast log across a restart (RFC 0003
// "Broadcast" and "Sessions"): what its members' endings returned to it, what
// it lent a clean member, and what it handed a durable one, each as the
// provider keeps it. Clients are real and every delivery is on the wire.

// groupStore is what these tests ask of the harness's session store about a
// group.
type groupStore interface {
	ShareCursors() (map[string]uint64, error)
	ShareReturned() (map[string][]uint64, error)
}

// groupTopics are a broadcast topic ("news"), an append channel's ("events")
// and a latest channel's ("state"): a shared group reads the broadcast log
// over any of them alike (RFC 0003 "Broadcast"). k is the topic's first level.
var groupTopics = []struct{ name, k string }{{"broadcast", "news"}, {"append", "events"}, {"latest", "state"}}

// awaitCursor waits until the store has a cursor for group, which its first
// durable member's SUBSCRIBE gives it after the SUBACK.
func awaitCursor(t *testing.T, group string) {
	t.Helper()
	s, ok := brokertest.HarnessSessions.(groupStore)
	if !ok {
		t.Fatalf("the harness's session store is a %T, which keeps no group cursors", brokertest.HarnessSessions)
	}
	if !waitUntil(5*time.Second, func() bool {
		cursors, err := s.ShareCursors()
		_, has := cursors[group]
		return err == nil && has
	}) {
		t.Fatalf("the group %s has no cursor", group)
	}
}

// returnedOf is the group's returned list as the store holds it.
func returnedOf(t *testing.T, group string) []uint64 {
	t.Helper()
	r, err := brokertest.HarnessSessions.(groupStore).ShareReturned()
	if err != nil {
		t.Fatalf("read the returned lists: %v", err)
	}
	return r[group]
}

// groupState is what the store holds for group - its cursor and its returned
// rows - for a failure message: a delivery missing after a restart is told
// apart from one that was never kept by what the store had either side.
func groupState(t *testing.T, group string) string {
	t.Helper()
	s := brokertest.HarnessSessions.(groupStore)
	cursors, cerr := s.ShareCursors()
	returned, rerr := s.ShareReturned()
	return fmt.Sprintf("cursor %v (err %v), returned %v (err %v)", cursors[group], cerr, returned[group], rerr)
}

// leave closes a client and waits until the broker has finished with it.
func leave(t *testing.T, h *harness, c *client, id string) {
	t.Helper()
	before := h.Disconnects.Snapshot(id)
	c.Close()
	sessionGoneAfter(t, h, id, before)
}

// RFC 0003 "Broadcast": what a member's ending returned to its group is
// served ahead of everything after the group's cursor, and a restart keeps it
// so. A's session ends holding a QoS 1 delivery it had not acknowledged; a
// second is published after. Across a restart, B, the group's other member,
// is sent the returned one first, then the other.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (groupTopics).
func TestAReturnedDeliveryIsServedFirstAfterARestart(t *testing.T) {
	for _, k := range groupTopics {
		t.Run(k.name, func(t *testing.T) {
			group := "$share/g/" + k.k + "/#"
			eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
				b := dial(t, h, "b", false, false, 0, 3600, 0)
				b.Sub(t, group, 1)
				awaitCursor(t, group)
				leave(t, h, b, "b")
				a := dial(t, h, "a", false, true, 10, 3600, 0)
				a.Sub(t, group, 1)

				p := connect(t, h, "producer", true, false)
				p.Pub(t, k.k+"/x", "returned")
				if !waitUntil(5*time.Second, func() bool { return a.Count() == 1 }) {
					t.Fatal("a was not sent the delivery")
				}
				leave(t, h, a, "a")
				dial(t, h, "a", true, false, 0, 0, 0) // a clean start ends a's session
				if !waitUntil(5*time.Second, func() bool { return len(returnedOf(t, group)) == 1 }) {
					t.Fatalf("the ending returned %v to the group, want the one delivery", returnedOf(t, group))
				}
				p.Pub(t, k.k+"/x", "after")
				// The group holds both before the stop: the returned one and
				// the one nobody has taken.
				if held := waitForBacklog(t, group, 2); len(held) != 2 {
					t.Fatalf("the group holds %d deliveries before the stop, want the returned one and "+
						"the one published after it", len(held))
				}
				beforeStop := groupState(t, group)
				h.Stop()

				h = restart()
				afterStart := groupState(t, group)
				b = dial(t, h, "b", false, false, 0, 3600, 0)
				if !waitUntil(5*time.Second, func() bool { return b.Count() == 2 }) {
					t.Fatalf("b was sent %q after the restart, want two\n  before the stop the store held %s\n"+
						"  after the start it held %s", b.Payloads(), beforeStop, afterStart)
				}
				if got := b.Payloads(); !slices.Equal(got, []string{"returned", "after"}) {
					t.Errorf("b was sent %q, want the returned delivery first\n  before the stop the store held %s\n"+
						"  after the start it held %s", got, beforeStop, afterStart)
				}
			})
		})
	}
}

// RFC 0003 "Broadcast": a delivery lent to a clean member is returned by a
// restart, which ends that member's session, and one it acknowledged is not:
// its returned row is forgotten by the group's next write, and that survives
// the restart. C, clean, takes two QoS 1 deliveries and acknowledges only
// the first - Paho sends acknowledgements in the order it received what they
// answer; after a restart D, the group's durable member, is sent the second
// and not the first.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (groupTopics).
func TestARestartReturnsWhatAGroupLentACleanMember(t *testing.T) {
	for _, k := range groupTopics {
		t.Run(k.name, func(t *testing.T) {
			group := "$share/g/" + k.k + "/#"
			eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
				d := dial(t, h, "d", false, false, 0, 3600, 0)
				d.Sub(t, group, 1)
				awaitCursor(t, group)
				leave(t, h, d, "d")
				c := dial(t, h, "c", true, true, 10, 0, 0)
				c.Sub(t, group, 1)

				p := connect(t, h, "producer", true, false)
				p.Pub(t, k.k+"/x", "acknowledged")
				p.Pub(t, k.k+"/x", "unacknowledged")
				if !waitUntil(5*time.Second, func() bool { return c.Count() == 2 }) {
					t.Fatal("c was not sent both")
				}
				if !waitUntil(5*time.Second, func() bool { return len(returnedOf(t, group)) == 2 }) {
					t.Fatalf("the group's returned rows are %v, want one for each lent delivery", returnedOf(t, group))
				}
				c.All()[0].Ack()
				if !waitUntil(5*time.Second, func() bool { return len(returnedOf(t, group)) == 1 }) {
					t.Fatalf("the acknowledged delivery's returned row was not forgotten: %v", returnedOf(t, group))
				}
				h.Stop()

				h = restart()
				d = dial(t, h, "d", false, false, 0, 3600, 0)
				// **An end marker rather than a quiet period.** What a restart
				// returned to the group is served ahead of anything published
				// after it, so the marker arriving says nothing else is coming.
				connect(t, h, "producer", true, false).Pub(t, k.k+"/x", "marker")
				if !waitUntil(5*time.Second, func() bool { return slices.Contains(d.Payloads(), "marker") }) {
					t.Fatalf("d was sent %q after the restart and never the end marker", d.Payloads())
				}
				if got := d.Payloads(); !slices.Equal(got, []string{"unacknowledged", "marker"}) {
					t.Errorf("d was sent %q after the restart, want only the delivery c never acknowledged, "+
						"then the end marker", got)
				}
			})
		})
	}
}

// RFC 0002's full-store table, RFC 0003 "Broadcast": a message a member has
// on the wire is not given up, and one a group handed a member is on the
// wire from the hand-over. M takes a group's delivery and goes away without
// acknowledging it; after a restart it is only in M's in-flight table. A
// session away and owed a stream of fillers, on a topic the group does not
// match, fills the log past its provider's bound: its oldest go, counted,
// and M's delivery stays, so M is sent it again when it comes back.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (groupTopics).
func TestAHandedOverDeliveryOutlivesAFullLog(t *testing.T) {
	for _, k := range groupTopics {
		t.Run(k.name, func(t *testing.T) {
			group := "$share/g/" + k.k + "/#"
			brokertest.BoundSQLite(t, store.SQLiteEmptyBytes+256<<10, "16KiB")
			path := t.TempDir() + "/s.db"
			h := startDurableSQLite(t, path)
			// W owes the fillers, on a topic the group does not match, so the
			// group's only delivery is the one M was handed.
			w := dial(t, h, "w", false, false, 0, 3600, 0)
			w.Sub(t, "fill/#", 1)
			leave(t, h, w, "w")
			m := dial(t, h, "m", false, true, 10, 3600, 0)
			m.Sub(t, group, 1)
			awaitCursor(t, group)

			p := connect(t, h, "producer", true, false)
			p.Pub(t, k.k+"/x", "handed")
			if !waitUntil(5*time.Second, func() bool { return m.Count() == 1 }) {
				t.Fatal("m was not sent the delivery")
			}
			leave(t, h, m, "m")
			h.Stop()

			h = startDurableSQLite(t, path)
			ops := operationsAt(t, h)
			p = connect(t, h, "producer", true, false)
			payload := strings.Repeat("f", 4096)
			for i := range 160 {
				p.Pub(t, "fill/y", fmt.Sprint(i, payload))
			}
			if g := scrapeGauges(t, ops); g[`saguin_session_deliveries_dropped_total{cause="storage_full"}`] == 0 {
				t.Fatal("nothing was given up, so the log was never full and this proves nothing")
			}
			m = dial(t, h, "m", false, false, 10, 3600, 0)
			if !waitUntil(5*time.Second, func() bool { return m.Count() >= 1 }) {
				t.Fatal("m was sent nothing: the delivery it had on the wire was given up")
			}
			// The end marker: published after m resumed, so the group hands it
			// m after anything the resume re-sent.
			p.Pub(t, k.k+"/x", "marker")
			if !waitUntil(5*time.Second, func() bool { return slices.Contains(m.Payloads(), "marker") }) {
				t.Fatalf("m was sent %q and never the end marker", m.Payloads())
			}
			if got := m.Payloads(); !slices.Equal(got, []string{"handed", "marker"}) {
				t.Errorf("m was sent %q, want only the delivery it had on the wire, then the end marker", got)
			}
		})
	}
}

// RFC 0004: an entry a group handed a member is behind the group's cursor,
// not the member's, and the member's cursor passes over it. M holds its own
// subscription and a group's. The group holds one delivery while M is away;
// M comes back, is handed it and does not acknowledge it, then takes and
// acknowledges two of its own published after it. After a restart M is sent
// the group's delivery again and nothing of its own: a cursor held back at
// the group's delivery - older than both - would have had the start owe them
// again. W, away and subscribed to them too, keeps them in the log, so a
// start could owe them. M is a client of raw packets, since it acknowledges
// out of order.
func TestAGroupsDeliveryDoesNotHoldItsMembersCursorBack(t *testing.T) {
	const group = "$share/g/news/#"
	eachSessionProviderRestart(t, func(t *testing.T, h *harness, restart func() *harness) {
		w := dial(t, h, "w", false, false, 0, 3600, 0)
		w.Sub(t, "own/#", 1)
		leave(t, h, w, "w")
		m, _ := qos2Dial(t, h.Addr, "m", false, 3600)
		m.Subscribe(group, 1)
		m.Subscribe("own/#", 1)
		awaitCursor(t, group)
		before := h.Disconnects.Snapshot("m")
		m.Close()
		sessionGoneAfter(t, h, "m", before)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "news/x", "group")
		if held := waitForBacklog(t, group, 1); len(held) != 1 {
			t.Fatalf("the group holds %d deliveries while m is away, want the one published", len(held))
		}

		m, _ = qos2Dial(t, h.Addr, "m", false, 3600)
		publishOf := func() *pahopackets.Publish {
			t.Helper()
			pub, ok := m.Next(5 * time.Second).Content.(*pahopackets.Publish)
			if !ok {
				t.Fatal("m was sent something other than a PUBLISH")
			}
			return pub
		}
		if pub := publishOf(); string(pub.Payload) != "group" {
			t.Fatalf("m was sent %q first, want the group's delivery", pub.Payload)
		}
		for _, want := range []string{"own-a", "own-b"} {
			p.Pub(t, "own/x", want)
			pub := publishOf()
			if string(pub.Payload) != want {
				t.Fatalf("m was sent %q, want %s", pub.Payload, want)
			}
			m.Puback(pub.PacketID)
		}
		// Every PUBACK is processed before the stop closes the socket, or the
		// restart rightly sends the unacknowledged record again.
		m.Settle()
		h.Stop()

		h = restart()
		back := dial(t, h, "m", false, false, 0, 3600, 0)
		if !waitUntil(5*time.Second, func() bool { return back.Count() >= 1 }) {
			t.Fatal("m was sent nothing after the restart")
		}
		// The end marker, on m's own subscription: anything of its own a
		// held-back cursor owed it is older, so it comes first.
		connect(t, h, "producer", true, false).Pub(t, "own/x", "marker")
		if !waitUntil(5*time.Second, func() bool { return slices.Contains(back.Payloads(), "marker") }) {
			t.Fatalf("m was sent %q after the restart and never the end marker", back.Payloads())
		}
		if got := back.Payloads(); !slices.Equal(got, []string{"group", "marker"}) {
			t.Errorf("m was sent %q after the restart, want only the group's delivery, then the end "+
				"marker: a cursor held back by it owes the start m's own again", got)
		}
	})
}

// Invariant 17, RFC 0003 "Sessions": a CONNECT that never gets its CONNACK
// has not taken the session over, so its clean start ends nothing - and a
// group's backlog is part of what it must not end. M, the group's only
// member, is away with a delivery held for it; a clean-start CONNECT under
// its id fails at its CONNACK. The group still holds the delivery, and M is
// sent it when it comes back.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (groupTopics).
func TestACleanStartThatNeverGetsItsConnackLeavesTheGroupsBacklog(t *testing.T) {
	for _, k := range groupTopics {
		t.Run(k.name, func(t *testing.T) {
			group := "$share/g/" + k.k + "/#"
			eachSessionProvider(t, func(t *testing.T, h *harness) {
				m := dial(t, h, "m", false, false, 0, 3600, 0)
				m.Sub(t, group, 1)
				awaitCursor(t, group)
				leave(t, h, m, "m")
				p := connect(t, h, "producer", true, false)
				p.Pub(t, k.k+"/x", "owed")
				if !waitUntil(5*time.Second, func() bool {
					ms, err := brokertest.HarnessShares.Backlog(group)
					return err == nil && len(ms) == 1
				}) {
					t.Fatal("the group does not hold the delivery, so this proves nothing")
				}

				connackNeverArrives(t, h, connectFor(t, "m", true, 3600, ""))

				if ms, err := brokertest.HarnessShares.Backlog(group); err != nil || len(ms) != 1 {
					t.Fatalf("the group holds %d (%v) after a clean start that never got its CONNACK, want the "+
						"one delivery", len(ms), err)
				}
				back := dial(t, h, "m", false, false, 0, 3600, 0)
				if !waitUntil(5*time.Second, func() bool { return back.Count() == 1 }) {
					t.Fatalf("m was sent %q on its return, want the delivery its group held", back.Payloads())
				}
			})
		})
	}
}
