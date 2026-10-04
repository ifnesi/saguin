package broker_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/store"
)

// A shared group reading the broadcast log (RFC 0003 "Broadcast"): clients
// are real, and every delivery and acknowledgement is on the wire. Oracles
// are RFC 0003 "Broadcast" and "Sessions", RFC 0002 `broker.share`, and RFC
// 0005's share series.

const (
	sharesHeld        = "saguin_shares_held_total"
	sharesDrained     = "saguin_shares_drained_total"
	sharesBacklogFull = `saguin_shares_dropped_total{cause="backlog_full"}`
	sharesStorageFull = `saguin_shares_dropped_total{cause="storage_full"}`
	sharesNoMember    = `saguin_shares_dropped_total{cause="no_member_left"}`
	sharesExpired     = `saguin_shares_dropped_total{cause="expired"}`
	sharesMemberEnded = `saguin_shares_dropped_total{cause="member_ended"}`
	sharesNotAuth     = `saguin_shares_dropped_total{cause="not_authorized"}`
	noSharedMember    = `saguin_session_deliveries_dropped_total{cause="no_shared_member"}`
)

// sharesDropped is every share drop series added up.
func sharesDropped(g map[string]float64) float64 {
	return g[sharesBacklogFull] + g[sharesStorageFull] + g[sharesNoMember] + g[sharesExpired] + g[sharesMemberEnded] +
		g[sharesNotAuth]
}

// holdsExactly fails unless RFC 0005's identity holds: held less drained less
// dropped is what the group holds.
//
// **Read once it holds, and reported when it never does.** The counters move
// beside the list rather than with it (countsAddUp): a delivery lent to a
// clean member leaves the waiting list before it is counted drained, so a
// drain still lending broke the identity for an instant - held 12 less
// drained 1 less dropped 8 against 0 waiting, twice in 200 runs.
func holdsExactly(t *testing.T, h *brokertest.Harness, ops string, group string) {
	t.Helper()
	countsAddUp(t, h, ops, group, 0)
	g := scrapeGauges(t, ops)
	if got, want := g[sharesHeld]-g[sharesDrained]-sharesDropped(g), float64(backlogLen(t, h, group)); got != want {
		t.Errorf("held %v less drained %v less dropped %v is %v, and the group holds %v",
			g[sharesHeld], g[sharesDrained], sharesDropped(g), got, want)
	}
}

// countsAddUp waits until RFC 0005's identity holds for group with at least
// held deliveries counted held, and answers whether it did. The counters move
// beside the list rather than with it, so a read the moment the list changes
// can catch one counted and the other not; the caller asserts, so a failure
// says what the counts were.
func countsAddUp(t *testing.T, h *brokertest.Harness, ops, group string, held float64) bool {
	t.Helper()
	return waitUntil(5*time.Second, func() bool {
		g := scrapeGauges(t, ops)
		return g[sharesHeld] >= held &&
			g[sharesHeld]-g[sharesDrained]-sharesDropped(g) == float64(backlogLen(t, h, group))
	})
}

// beforeMarker publishes an end marker on topic and answers what c was sent
// before it, once it has it.
//
// **The marker stands where a quiet period stood.** A record published now
// is stored after everything before it, and a subscriber - or a group's one
// member - is served in that order (RFC 0003 "Ordering"; a delivery joins
// the back of its group's backlog, TestADeliveryJoinsTheBackOfItsGroupsBacklog),
// so anything owed to c that is still on its way arrives ahead of the marker
// and is read here. A quiet period said only that nothing had arrived lately.
func beforeMarker(t *testing.T, p *brokertest.Client, topic string, c *brokertest.Client) []string {
	t.Helper()
	const marker = "end-marker"
	p.Pub(t, topic, marker)
	eventually(t, c.ID+" is sent the end marker", 5*time.Second, func() bool {
		return slices.Contains(c.Payloads(), marker)
	})
	got := c.Payloads()
	return got[:slices.Index(got, marker)]
}

// passOver waits for a pass of group's drain that began after this was called
// and has handed out all it could, so whatever the caller saw before calling
// it was there for that pass to see. It wakes the drain itself, after its
// seam is in place, so such a pass is certain to come (broker.SetGroupPass).
//
// **A pass over the state, not a pause after it.** What a group does not
// hand out is the drain deciding it cannot, and a quiet period was a guess
// at when it had decided: run late, it decided after the check.
func passOver(t *testing.T, h *brokertest.Harness, group string) {
	t.Helper()
	ended := make(chan struct{})
	var begun, done atomic.Bool
	defer broker.SetGroupPass(func(b *broker.Broker, g string, starting bool) {
		switch {
		case b != h.B || g != group:
		case starting:
			begun.Store(true)
		case begun.Load() && done.CompareAndSwap(false, true):
			close(ended)
		}
	})()
	h.B.WakeGroup(group)
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatalf("no pass of %s's drain ran to its end within 5s of a wake", group)
	}
}

// awaitBlockedIn waits until a goroutine is blocked in state - "select",
// "sync.Mutex.Lock" - with fn on its stack, which is how a test sees that the
// side of a race it holds back is queued, rather than sleeping until it
// probably is.
func awaitBlockedIn(t *testing.T, fn, state string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		n := runtime.Stack(buf, true)
		for n == len(buf) {
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			header, _, _ := strings.Cut(g, "\n")
			if strings.Contains(header, "["+state) && strings.Contains(g, fn) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no goroutine was blocked in %s inside %s within 5s", state, fn)
		}
	}
}

// awayMember is a durable member of group whose client has gone: the group
// has a cursor, and nobody to hand anything to.
func awayMember(t *testing.T, h *brokertest.Harness, id, group string, qos byte) {
	t.Helper()
	c := dial(t, h, id, false, false, 0, 3600, 0)
	c.Sub(t, group, qos)
	awaitGroup(t, h, group)
	before := h.Disconnects.Snapshot(id)
	c.Close()
	sessionGoneAfter(t, h, id, before)
}

// eachGroupTopic runs one case on each provider over a broadcast topic
// ("news"), an append channel's ("events") and a latest channel's ("state"):
// a shared group reads the broadcast log over any of them alike (RFC 0003
// "Broadcast"). k is the topic's first level.
func eachGroupTopic(t *testing.T, run func(t *testing.T, h *brokertest.Harness, k string)) {
	t.Helper()
	for _, c := range []struct{ name, k string }{{"broadcast", "news"}, {"append", "events"}, {"latest", "state"}} {
		t.Run(c.name, func(t *testing.T) {
			eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) { run(t, h, c.k) })
		})
	}
}

// awaitGroup waits for group to have a cursor on the log, which its first
// durable member's SUBSCRIBE gives it after the SUBACK.
func awaitGroup(t *testing.T, h *brokertest.Harness, group string) {
	t.Helper()
	eventually(t, "the group "+group+" has a cursor", 5*time.Second, func() bool {
		_, ok := h.B.GroupList(group)
		return ok
	})
}

// backlogLen is how many deliveries a group holds and has not handed out.
func backlogLen(t *testing.T, h *brokertest.Harness, group string) int {
	t.Helper()
	ms, err := h.B.Backlog(group)
	if err != nil {
		t.Fatalf("read %s's backlog: %v", group, err)
	}
	return len(ms)
}

// RFC 0002 "Every session's state", RFC 0005 `saguin_shares_dropped_total`
//
// **A group's deliveries lost to the provider's room are storage_full, never
// backlog_full**: the bound that took them is broker.session.storage's, and
// limits.session_queue_bytes, set far above what the group holds here, was
// never reached. One member is away, so the group holds everything; a
// producer fills the provider through it. Every publish is acknowledged
// 0x00, every loss is under storage_full, backlog_full stays at 0, nothing is
// counted as no_shared_member, and held less drained less dropped is what
// the group still holds.
func TestAGroupsLossesToAFullProviderAreStorageFull(t *testing.T) {
	brokertest.SessionQueueBytes = 64 << 20
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			h, _, _ := boundedDrain(t, provider, 256*1024)
			ops := operationsAt(t, h)
			const group = "$share/g/news/#"
			awayMember(t, h, "member", group, 1)

			p := connect(t, h, "producer", true, false)
			const n = 160
			payload := strings.Repeat("x", 4096)
			for range n {
				p.Pub(t, "news/x", payload)
			}
			held := backlogLen(t, h, group)
			g := scrapeGauges(t, ops)
			if held == 0 || held >= n {
				t.Fatalf("the group holds %d of %d: the provider had to lose some and keep some for this "+
					"to prove anything", held, n)
			}
			if g[sharesHeld] != n {
				t.Errorf("%s is %v, want %d: every delivery went on the group's list", sharesHeld, g[sharesHeld], n)
			}
			if g[sharesStorageFull] != float64(n-held) || g[sharesBacklogFull] != 0 {
				t.Errorf("%s is %v and %s %v, want %d and 0: every loss was the provider's room",
					sharesStorageFull, g[sharesStorageFull], sharesBacklogFull, g[sharesBacklogFull], n-held)
			}
			if g[noSharedMember] != 0 {
				t.Errorf("%s is %v: a loss the group counted is counted nowhere else", noSharedMember, g[noSharedMember])
			}
			if got := g[sharesHeld] - g[sharesDrained] - sharesDropped(g); got != float64(held) {
				t.Errorf("held less drained less dropped is %v, and the group holds %d", got, held)
			}
			// What the group was given, handed out and let go adds up.
			holdsExactly(t, h, ops, group)
		})
	}
}

// recordingCursors is a session store that notes each write of a group's
// cursor, and the cursor written.
type recordingCursors struct {
	drainSessions
	mu      sync.Mutex
	cursors []uint64
}

func (r *recordingCursors) SetShareCursor(group string, cursor uint64, forget ...uint64) error {
	err := r.drainSessions.SetShareCursor(group, cursor, forget...)
	if err == nil {
		r.mu.Lock()
		r.cursors = append(r.cursors, cursor)
		r.mu.Unlock()
	}
	return err
}

// RFC 0003 "Broadcast": what a group lets go of is stored as it goes, not
// only when its drain runs out of work, or a crash in a long run sends all
// of it again.
//
// **A continuous backlog where every entry leaves without a hand-over**: a
// durable member away gives the group its cursor, the group holds 500
// deliveries, and a clean member subscribed at QoS 0 comes and takes them
// all - each written at QoS 0 and let go, which moves the cursor without a
// hand-over's own write. The stored cursor never falls more than 64 behind
// what the drain has let go: each write moves it at most 64 on from the last.
func TestABusyGroupStoresItsCursorAsItGoes(t *testing.T) {
	h := startDurable(t, t.TempDir())
	rec := &recordingCursors{}
	attachDrainThrough(t, h, func(s drainSessions) drainSessions {
		rec.drainSessions = s
		return rec
	})
	const group = "$share/g/news/#"
	awayMember(t, h, "member", group, 1)

	p := connect(t, h, "producer", true, false)
	const n = 500
	for i := range n {
		p.Pub(t, "news/x", fmt.Sprint("m", i))
	}
	eventually(t, "the group holds all 500", 5*time.Second, func() bool { return backlogLen(t, h, group) == n })

	c := connect(t, h, "clean-reader", true, false)
	c.Sub(t, group, 0)
	eventually(t, "the clean member is sent all 500", 10*time.Second, func() bool { return c.Count() == n })
	eventually(t, "the group holds nothing", 5*time.Second, func() bool { return backlogLen(t, h, group) == 0 })

	rec.mu.Lock()
	cursors := slices.Clone(rec.cursors)
	rec.mu.Unlock()
	if len(cursors) < n/64 {
		t.Fatalf("the group wrote its cursor %d times over %d let go, want at least %d", len(cursors), n, n/64)
	}
	for i := 1; i < len(cursors); i++ {
		if step := cursors[i] - cursors[i-1]; cursors[i] > cursors[i-1] && step > 64 {
			t.Errorf("the cursor moved from %d to %d in one write: %d let go before it was stored",
				cursors[i-1], cursors[i], step)
		}
	}
	t.Logf("%d let go, the cursor written %d times", n, len(cursors))
}

// RFC 0003 "Broadcast", [MQTT-4.6.0-6]: a delivery arriving while its group
// holds something joins the back of that backlog. A member comes back to
// three held deliveries, and a fourth is published the moment it is back: it
// is served one, two, three, four.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (eachGroupTopic).
func TestADeliveryJoinsTheBackOfItsGroupsBacklog(t *testing.T) {
	eachGroupTopic(t, func(t *testing.T, h *brokertest.Harness, k string) {
		group := "$share/g/" + k + "/#"
		awayMember(t, h, "member", group, 1)
		p := connect(t, h, "producer", true, false)
		for _, m := range []string{"one", "two", "three"} {
			p.Pub(t, k+"/x", m)
		}
		eventually(t, "the group holds three", 5*time.Second, func() bool { return backlogLen(t, h, group) == 3 })

		back := dial(t, h, "member", false, false, 0, 3600, 0)
		p.Pub(t, k+"/x", "four")
		eventually(t, "the member is sent four", 5*time.Second, func() bool { return back.Count() == 4 })
		if got := strings.Join(back.Payloads(), " "); got != "one two three four" {
			t.Errorf("the member was sent %q, want one two three four", got)
		}
	})
}

// RFC 0003 "Broadcast": a member's session ending splits what its group had
// handed it, as MQTT 5 section 4.8.2 does. A has a QoS 1 delivery it has not
// acknowledged and a QoS 2 one it has not answered with a PUBREC; B, the
// group's other member, is away. A ends its session with a clean start - the
// ending whose record is written over before the session it replaces ends.
// The QoS 1 delivery goes back to the group and B is sent it when it comes
// back; the QoS 2 one goes to nobody (MQTT-4.8.2-5) and is counted as
// member_ended.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (eachGroupTopic).
func TestAMembersEndingSplitsWhatItsGroupHandedIt(t *testing.T) {
	eachGroupTopic(t, func(t *testing.T, h *brokertest.Harness, k string) {
		ops := operationsAt(t, h)
		group := "$share/g/" + k + "/#"
		awayMember(t, h, "b", group, 2)
		a := dial(t, h, "a", false, true, 10, 3600, 0)
		a.Sub(t, group, 2)

		p := connect(t, h, "producer", true, false)
		p.Pub(t, k+"/x", "once")
		p.PubPlainAt(t, k+"/x", "exactly once", 2)
		eventually(t, "a is sent both", 5*time.Second, func() bool { return a.Count() == 2 })
		before := h.Disconnects.Snapshot("a")
		a.Close()
		sessionGoneAfter(t, h, "a", before)

		dial(t, h, "a", true, false, 0, 0, 0)
		eventually(t, "the QoS 1 delivery is back on the group's list", 5*time.Second, func() bool {
			return backlogLen(t, h, group) == 1
		})
		// Counted beside the list rather than with it, so read once it shows.
		var g map[string]float64
		waitUntil(5*time.Second, func() bool { g = scrapeGauges(t, ops); return g[sharesMemberEnded] >= 1 })
		if g[sharesMemberEnded] != 1 {
			t.Errorf("%s is %v, want 1: the QoS 2 delivery awaiting its PUBREC", sharesMemberEnded, g[sharesMemberEnded])
		}
		b := dial(t, h, "b", false, false, 0, 3600, 0)
		eventually(t, "b is sent what came back", 5*time.Second, func() bool { return b.Count() >= 1 })
		if got := beforeMarker(t, p, k+"/x", b); !slices.Equal(got, []string{"once"}) {
			t.Errorf("b was sent %q, want only the QoS 1 delivery", got)
		}
		// What the group was given, handed out and let go adds up.
		holdsExactly(t, h, ops, group)
	})
}

// RFC 0003 "Broadcast": a clean member's session ends with its connection,
// and what its group lent it is split at that ending as a durable member's
// is. C, clean, takes a QoS 1 delivery and a QoS 2 one and answers neither;
// D, durable, is away. C's link closes: the QoS 1 delivery goes back to the
// group and D is sent it, and the QoS 2 one is counted as member_ended.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (eachGroupTopic).
func TestACleanMembersEndingReturnsWhatItsGroupLentIt(t *testing.T) {
	eachGroupTopic(t, func(t *testing.T, h *brokertest.Harness, k string) {
		ops := operationsAt(t, h)
		group := "$share/g/" + k + "/#"
		awayMember(t, h, "d", group, 2)
		c := dial(t, h, "c", true, true, 10, 0, 0)
		c.Sub(t, group, 2)

		p := connect(t, h, "producer", true, false)
		p.Pub(t, k+"/x", "once")
		p.PubPlainAt(t, k+"/x", "exactly once", 2)
		eventually(t, "c is sent both", 5*time.Second, func() bool { return c.Count() == 2 })
		before := h.Disconnects.Snapshot("c")
		c.Close()
		sessionGoneAfter(t, h, "c", before)

		eventually(t, "the QoS 1 delivery is back on the group's list", 5*time.Second, func() bool {
			return backlogLen(t, h, group) == 1
		})
		// Counted beside the list rather than with it, so read once it shows.
		var g map[string]float64
		waitUntil(5*time.Second, func() bool { g = scrapeGauges(t, ops); return g[sharesMemberEnded] >= 1 })
		if g[sharesMemberEnded] != 1 {
			t.Errorf("%s is %v, want 1", sharesMemberEnded, g[sharesMemberEnded])
		}
		d := dial(t, h, "d", false, false, 0, 3600, 0)
		eventually(t, "d is sent what came back", 5*time.Second, func() bool { return d.Count() >= 1 })
		if got := beforeMarker(t, p, k+"/x", d); !slices.Equal(got, []string{"once"}) {
			t.Errorf("d was sent %q, want only the QoS 1 delivery", got)
		}
		// What the group was given, handed out and let go adds up.
		holdsExactly(t, h, ops, group)
	})
}

// RFC 0003 "Sessions": a group's backlog cannot outlive the sessions it
// belongs to, and a member leaving by UNSUBSCRIBE is the last member leaving
// too. D, the group's only durable member, has one delivery in its window
// and one waiting behind it; it unsubscribes. What waited goes, counted as
// no_member_left; what was in its window is its own to finish.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (eachGroupTopic).
func TestAGroupsLastMemberUnsubscribingEndsItsBacklog(t *testing.T) {
	eachGroupTopic(t, func(t *testing.T, h *brokertest.Harness, k string) {
		ops := operationsAt(t, h)
		group := "$share/g/" + k + "/#"
		d := dial(t, h, "d", false, true, 1, 3600, 0)
		d.Sub(t, group, 1)
		awaitGroup(t, h, group)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, k+"/x", "first")
		p.Pub(t, k+"/x", "second")
		eventually(t, "d holds one and the group the other", 5*time.Second, func() bool {
			return d.Count() == 1 && backlogLen(t, h, group) == 1
		})
		if _, err := d.C.Unsubscribe(context.Background(), &paho.Unsubscribe{Topics: []string{group}}); err != nil {
			t.Fatalf("unsubscribe: %v", err)
		}
		eventually(t, "the group has ended", 5*time.Second, func() bool {
			_, ok := h.B.GroupList(group)
			return !ok
		})
		if g := scrapeGauges(t, ops); g[sharesNoMember] != 1 {
			t.Errorf("%s is %v, want 1: the delivery that waited", sharesNoMember, g[sharesNoMember])
		}
		if all := d.All(); len(all) == 1 {
			all[0].Ack()
		}
		// What the group was given, handed out and let go adds up.
		holdsExactly(t, h, ops, group)
	})
}

// RFC 0003 "Broadcast": a delivery handed over and never written to the
// member's connection goes back to the group at any QoS - the member never
// had it - in one store write that takes the member's row and writes the
// group's returned one. M, the group's only member, is handed a delivery and
// goes away; the hand-over is undone as a failed write undoes it. The member's
// table holds nothing, the group holds the delivery again and counts it held
// again, and M is sent it when it comes back.
func TestAHandOverThatNeverReachedTheMemberGoesBack(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		ops := operationsAt(t, h)
		const group = "$share/g/news/#"
		m := dial(t, h, "m", false, true, 5, 3600, 0)
		m.Sub(t, group, 1)
		awaitGroup(t, h, group)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "news/x", "handed")
		eventually(t, "m is sent it", 5*time.Second, func() bool { return m.Count() == 1 })
		before := h.Disconnects.Snapshot("m")
		m.Close()
		sessionGoneAfter(t, h, "m", before)
		flying := h.B.FlyingBroadcast("m")
		if len(flying) != 1 || flying[0].Group != group {
			t.Fatalf("m has %+v on the wire, want the group's one delivery", flying)
		}
		held := scrapeGauges(t, ops)[sharesHeld]

		if !h.B.Unhand("m", flying[0].PacketID) {
			t.Fatal("m held no group delivery to undo")
		}
		if got := h.B.FlyingBroadcast("m"); len(got) != 0 {
			t.Errorf("m still has %+v on the wire", got)
		}
		_, table, err := brokertest.HarnessSessions.(interface {
			InFlight(string) (uint16, []store.InFlight, error)
		}).InFlight("m")
		if err != nil || len(table) != 0 {
			t.Errorf("m's in-flight table holds %+v (%v), want nothing", table, err)
		}
		list, _ := h.B.GroupList(group)
		if len(list) != 1 || !list[0].Returned {
			t.Errorf("the group's list is %+v, want the delivery back, returned", list)
		}
		if got := scrapeGauges(t, ops)[sharesHeld]; got != held+1 {
			t.Errorf("%s is %v, want %v: back on the group's list is held again", sharesHeld, got, held+1)
		}
		back := dial(t, h, "m", false, false, 0, 3600, 0)
		eventually(t, "m is sent it on its return", 5*time.Second, func() bool { return back.Count() == 1 })
		// What the group was given, handed out and let go adds up.
		holdsExactly(t, h, ops, group)
	})
}

// RFC 0003 "Broadcast", MQTT 5 section 4.8.2: a member holding a shared
// subscription and an ordinary one that both match a message is sent its own
// copy, and may be sent the group's too. M is the group's only member and
// also subscribes to the topic itself: it is sent its own copy, and the
// group's only once it has acknowledged that one - its list holds one entry
// a message. Once both are acknowledged the log holds nothing either owed:
// neither copy took the other's place on M's list.
func TestAMemberHoldingItsOwnCopyTakesTheGroupsAfterIt(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		const group = "$share/g/news/#"
		m := dial(t, h, "m", false, true, 10, 3600, 0)
		m.Sub(t, group, 1)
		m.Sub(t, "news/#", 1)
		awaitGroup(t, h, group)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "news/x", "twice")
		eventually(t, "m is sent its own copy", 5*time.Second, func() bool { return m.Count() == 1 })
		eventually(t, "the group holds its copy", 5*time.Second, func() bool {
			list, _ := h.B.GroupList(group)
			return len(list) == 1
		})
		passOver(t, h, group)
		if list, _ := h.B.GroupList(group); len(list) != 1 || list[0].Lent {
			t.Fatalf("the group's list is %+v after a pass while m held its own copy unacknowledged, "+
				"want the group's copy still waiting", list)
		}
		if n := len(h.B.FlyingBroadcast("m")); n != 1 {
			t.Fatalf("m has %d copies on the wire while it held its own unacknowledged, want 1", n)
		}
		m.All()[0].Ack()
		eventually(t, "m is sent the group's copy", 5*time.Second, func() bool { return m.Count() == 2 })
		m.All()[1].Ack()
		eventually(t, "nothing is owed and the log holds nothing", 5*time.Second, func() bool {
			owed, _ := owedOf(h, "m")
			list, _ := h.B.GroupList(group)
			return len(owed) == 0 && len(list) == 0 && len(h.B.FlyingBroadcast("m")) == 0
		})
		lg := h.B.BroadcastLogForTest()
		eventually(t, "the log lets the message go", 5*time.Second, func() bool {
			recs, err := lg.ReadAt(1)
			return err == nil && len(recs) == 0
		})
	})
}

// RFC 0003 "Broadcast", RFC 0005 `storage_full`: a QoS 1 delivery a member's
// ending would have returned to its group, where the provider had no room
// even in its reserve to keep the returned row (store.Dropped.Unreturned), is
// lost to the provider's room: counted as storage_full, and let go.
func TestAnEndingsUnreturnedDeliveryIsStorageFull(t *testing.T) {
	h := startDurable(t, t.TempDir())
	ops := operationsAt(t, h)
	const group = "$share/g/news/#"
	m := dial(t, h, "m", false, true, 5, 3600, 0)
	m.Sub(t, group, 1)
	awaitGroup(t, h, group)
	p := connect(t, h, "producer", true, false)
	p.Pub(t, "news/x", "unreturned")
	eventually(t, "m is sent it", 5*time.Second, func() bool { return m.Count() == 1 })
	flying := h.B.FlyingBroadcast("m")
	if len(flying) != 1 {
		t.Fatalf("m has %+v on the wire, want one", flying)
	}
	before := scrapeGauges(t, ops)[sharesStorageFull]
	h.B.EndWith("m", store.Dropped{Unreturned: flying})
	if got := scrapeGauges(t, ops)[sharesStorageFull]; got != before+1 {
		t.Errorf("%s is %v, want %v", sharesStorageFull, got, before+1)
	}
	lg := h.B.BroadcastLogForTest()
	eventually(t, "the log lets it go", 5*time.Second, func() bool {
		recs, err := lg.ReadAt(flying[0].Offset)
		return err == nil && len(recs) == 0
	})
	// What the group was given, handed out and let go adds up.
	holdsExactly(t, h, ops, group)
}

// RFC 0003 "Sessions": a clean start ends the session it replaces, and a
// group whose last member that was loses its backlog with it. The CONNECT
// keeps a session of its own, so its record - holding no group - is written
// over the old one before the old one ends: the ending is told the groups
// the old session held (endStoredSession), and ends the group's cursor.
func TestACleanStartThatKeepsItsSessionEndsItsGroupsBacklog(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		ops := operationsAt(t, h)
		const group = "$share/g/news/#"
		awayMember(t, h, "m", group, 1)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "news/x", "owed")
		eventually(t, "the group holds it", 5*time.Second, func() bool { return backlogLen(t, h, group) == 1 })

		dial(t, h, "m", true, false, 0, 3600, 0)
		eventually(t, "the group has ended", 5*time.Second, func() bool {
			_, ok := h.B.GroupList(group)
			return !ok
		})
		if g := scrapeGauges(t, ops); g[sharesNoMember] != 1 {
			t.Errorf("%s is %v, want 1: the backlog of a group with no member left", sharesNoMember, g[sharesNoMember])
		}
		cursors, err := brokertest.HarnessSessions.(interface {
			ShareCursors() (map[string]uint64, error)
		}).ShareCursors()
		if _, has := cursors[group]; err != nil || has {
			t.Errorf("the store still holds the group's cursor (%v): %v", err, cursors)
		}
		// What the group was given, handed out and let go adds up.
		holdsExactly(t, h, ops, group)
	})
}

// RFC 0005 `saguin_shares_dropped_total`: held less drained less dropped is
// what the groups hold, exactly - so every way a delivery leaves a group's
// list is counted somewhere. Three that let one go without handing it out:

// A message whose publisher's Message Expiry Interval ran out while its group
// held it is read, not delivered (invariant 2), and counted as the group's
// expired. The member is away when it is published with a second of life,
// and back after two.
func TestAGroupsDeliveryPastItsExpiryIsCountedExpired(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		ops := operationsAt(t, h)
		const group = "$share/g/news/#"
		awayMember(t, h, "m", group, 1)
		p := connect(t, h, "producer", true, false)
		life := uint32(1)
		p.PubProps(t, "news/x", "stale", &paho.PublishProperties{MessageExpiry: &life})
		eventually(t, "the group holds it", 5*time.Second, func() bool { return backlogLen(t, h, group) == 1 })
		time.Sleep(2 * time.Second)

		m := dial(t, h, "m", false, false, 0, 3600, 0)
		eventually(t, "the group lets it go", 5*time.Second, func() bool { return backlogLen(t, h, group) == 0 })
		g := scrapeGauges(t, ops)
		if g[sharesExpired] != 1 || g[sharesDrained] != 0 {
			t.Errorf("%s is %v and %s %v, want 1 and 0", sharesExpired, g[sharesExpired], sharesDrained, g[sharesDrained])
		}
		holdsExactly(t, h, ops, group)
		if n := m.Count(); n != 0 {
			t.Errorf("the member was sent %d, want nothing past its expiry", n)
		}
	})
}

// A delivery the acl_file allows to none of the members that could take it
// is let go, and counted as the group's not_authorized.
func TestAGroupsDeliveryNoMemberMayReadIsNotAuthorized(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		ops := operationsAt(t, h)
		const group = "$share/g/news/#"
		m := dial(t, h, "m", false, false, 0, 3600, 0)
		m.Sub(t, group, 1)
		awaitGroup(t, h, group)
		writeACL(t, h, `
roles:
  news:
    - topic: "news/#"
      allow: [write]
    - topic: "news/open/#"
      allow: [read]
users:
  "": [news]
`)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "news/secret", "refused")
		eventually(t, "counted as not_authorized", 5*time.Second, func() bool {
			return scrapeGauges(t, ops)[sharesNotAuth] == 1
		})
		holdsExactly(t, h, ops, group)
		if n := m.Count(); n != 0 {
			t.Errorf("the member was sent %d, want nothing the acl_file refuses it", n)
		}
	})
}

// RFC 0003 "When a record is too large for a subscriber": a group's record
// no connected member fits waits in the group's backlog, as it does with no
// member connected, and reaches a member it fits when one arrives. The small
// member is passed over at either QoS, not disconnected, and nothing is
// counted lost. Over a broadcast topic and each channel type alike.
func TestARecordNoConnectedMemberFitsWaitsForOneItFits(t *testing.T) {
	eachGroupTopic(t, func(t *testing.T, h *brokertest.Harness, k string) {
		ops := operationsAt(t, h)
		group := "$share/g/" + k + "/#"
		awayMember(t, h, "d", group, 1)
		small0 := connectMaxPacket(t, h, "small-0", 256)
		small0.Sub(t, group, 0)
		small1 := connectMaxPacket(t, h, "small-1", 256)
		small1.Sub(t, group, 1)
		p := connect(t, h, "producer", true, false)
		big := strings.Repeat("x", 1024)
		p.Pub(t, k+"/x", big)
		eventually(t, "the group holds it", 5*time.Second, func() bool { return backlogLen(t, h, group) == 1 })
		passOver(t, h, group)
		if list, _ := h.B.GroupList(group); len(list) != 1 || list[0].Lent {
			t.Errorf("the group's list is %+v after a pass with only small members connected, want "+
				"the record waiting, lent to nobody", list)
		}
		for _, id := range []string{"small-0", "small-1"} {
			if cl, ok := h.Srv.Clients.Get(id); !ok || cl.Closed() {
				t.Fatalf("%s is no longer connected: one the record does not fit is passed over", id)
			}
		}
		if n := small0.Count() + small1.Count(); n != 0 {
			t.Errorf("the small members were sent %d, want nothing larger than they take", n)
		}
		for _, c := range []*brokertest.Client{small0, small1} {
			select {
			case d := <-c.Gone:
				t.Fatalf("a small member was disconnected (%#x): one the record does not fit is passed over",
					d.ReasonCode)
			default:
			}
		}
		if g := scrapeGauges(t, ops); sharesDropped(g) != 0 {
			t.Errorf("the group counted %v dropped, want nothing: the record waits", sharesDropped(g))
		}
		holdsExactly(t, h, ops, group)

		back := dial(t, h, "d", false, false, 0, 3600, 0)
		eventually(t, "the member it fits is sent it", 5*time.Second, func() bool { return back.Count() == 1 })
		if got := back.Payloads(); !slices.Equal(got, []string{big}) {
			t.Errorf("the member it fits was sent %d deliveries, want the record", len(got))
		}
	})
}

// RFC 0003 "When a record is too large for a subscriber", RFC 0002 "Shared
// subscriptions": a group whose members all end with their connections has
// no backlog, so a record none of its connected members fits is dropped as
// no member can take it - counted as no_shared_member - and the small member
// is passed over, not disconnected.
func TestALiveGroupsRecordNoMemberFitsIsNoSharedMember(t *testing.T) {
	h := start(t)
	ops := operationsAt(t, h)
	const series = `saguin_session_deliveries_dropped_total{cause="no_shared_member"}`
	for _, k := range []string{"events", "news"} {
		small := dial(t, h, "small-"+k, true, false, 0, 0, 256)
		small.Sub(t, "$share/live/"+k+"/#", 1)
		before := scrapeGauges(t, ops)[series]
		connect(t, h, "producer-"+k, true, false).Pub(t, k+"/x", strings.Repeat("x", 1024))
		eventually(t, k+": counted as no_shared_member", 5*time.Second, func() bool {
			return scrapeGauges(t, ops)[series]-before == 1
		})
		select {
		case d := <-small.Gone:
			t.Fatalf("%s: the small member was disconnected (%#x)", k, d.ReasonCode)
		case <-time.After(300 * time.Millisecond):
		}
		if n := small.Count(); n != 0 {
			t.Errorf("%s: the small member was sent %d", k, n)
		}
	}
}

// blockingLend is a session store whose next Lend, once armed, waits for the
// test before it is written: the window between a clean member's lend and
// its registration on the connection.
type blockingLend struct {
	drainSessions
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *blockingLend) Lend(group string, cursor uint64, offsets []uint64) error {
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return b.drainSessions.Lend(group, cursor, offsets)
}

// RFC 0005's identity through a takeover landing between a clean member's
// lend and its registration on the connection: the lend comes back to the
// group - taken back, the connection's table having gone to its successor
// without it, or by its member's ending, which a takeover is for a session
// that ends with its connection whatever the successor's Clean Start - and
// held less drained less dropped is still what the group holds.
func TestALendATakeoverInterruptsKeepsTheGroupsCountsExact(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprintf("the takeover's clean start %t", clean), func(t *testing.T) {
			h := startDurable(t, t.TempDir())
			ops := operationsAt(t, h)
			block := &blockingLend{entered: make(chan struct{}), release: make(chan struct{})}
			attachDrainThrough(t, h, func(s drainSessions) drainSessions {
				block.drainSessions = s
				return block
			})
			const group = "$share/g/news/#"
			awayMember(t, h, "d", group, 1)
			c := dial(t, h, "c", true, true, 10, 0, 0)
			c.Sub(t, group, 1)

			block.armed.Store(true)
			p := connect(t, h, "producer", true, false)
			p.Pub(t, "news/x", "lent")
			select {
			case <-block.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the group never lent the delivery, so no window was held open")
			}
			old, ok := h.Srv.Clients.Get("c")
			if !ok {
				t.Fatal("the clean member is not connected")
			}
			successor := dial(t, h, "c", clean, true, 10, 0, 0)
			// The takeover lands wholly inside the window: the connection the
			// lend was for is taken over before the lend is written.
			eventually(t, "the old connection is taken over", 5*time.Second, old.IsTakenOver)
			close(block.release)

			// A new session holds no subscription: the delivery waits on the
			// group's list again.
			eventually(t, "the delivery is back on the group's list", 5*time.Second, func() bool {
				return backlogLen(t, h, group) == 1
			})
			countsAddUp(t, h, ops, group, 1)
			holdsExactly(t, h, ops, group)
			if c.Count() != 0 || successor.Count() != 0 {
				t.Errorf("the connection taken over was sent %d and its successor %d, want nothing", c.Count(),
					successor.Count())
			}
		})
	}
}

// RFC 0003 "Broadcast" and "Sessions", MQTT 5 section 4.8.2: what a group
// lent a clean member is split at the member's ending when the ending is a
// takeover, as at any other - and a takeover is one whatever the new
// CONNECT's Clean Start says, the session ending with its connection. C,
// clean, takes a QoS 1 delivery and a QoS 2 one and answers neither; D,
// durable, is away. A CONNECT for C's id takes it over: the QoS 1 delivery
// goes back to the group and D is sent it, and the QoS 2 one is counted as
// member_ended. The takeover cleared C's in-flight table before marking C
// taken over, and each entry cleared read as acknowledged: both were let go,
// counted drained, and D was sent nothing.
//
// Over a broadcast topic, an append channel's and a latest channel's alike
// (eachGroupTopic).
func TestALentDeliveryGoesBackToItsGroupWhenATakeoverEndsItsMember(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprintf("the takeover's clean start %t", clean), func(t *testing.T) {
			eachGroupTopic(t, func(t *testing.T, h *brokertest.Harness, k string) {
				ops := operationsAt(t, h)
				group := "$share/g/" + k + "/#"
				awayMember(t, h, "d", group, 2)
				c := dial(t, h, "c", true, true, 10, 0, 0)
				c.Sub(t, group, 2)

				p := connect(t, h, "producer", true, false)
				p.Pub(t, k+"/x", "once")
				p.PubPlainAt(t, k+"/x", "exactly once", 2)
				eventually(t, "c is sent both", 5*time.Second, func() bool { return c.Count() == 2 })

				successor := dial(t, h, "c", clean, true, 10, 3600, 0)
				eventually(t, "the QoS 1 delivery is back on the group's list", 5*time.Second, func() bool {
					return backlogLen(t, h, group) == 1
				})
				// Counted beside the list rather than with it, so read once it
				// shows.
				var g map[string]float64
				waitUntil(5*time.Second, func() bool { g = scrapeGauges(t, ops); return g[sharesMemberEnded] >= 1 })
				if g[sharesMemberEnded] != 1 {
					t.Errorf("%s is %v, want 1", sharesMemberEnded, g[sharesMemberEnded])
				}
				d := dial(t, h, "d", false, false, 0, 3600, 0)
				eventually(t, "d is sent what came back", 5*time.Second, func() bool { return d.Count() >= 1 })
				if got := beforeMarker(t, p, k+"/x", d); !slices.Equal(got, []string{"once"}) {
					t.Errorf("d was sent %q, want only the QoS 1 delivery", got)
				}
				if successor.Count() != 0 {
					t.Errorf("the successor, told its session is new, was sent %d", successor.Count())
				}
				holdsExactly(t, h, ops, group)
			})
		})
	}
}

// RFC 0003 "Sessions" and "Broadcast": a channel's record a shared group
// holds for a member whose session outlives its connection comes back after a
// restart, to that group, and to nobody else. A durable subscriber to the
// channel ("#"), away while the record is published so its cursor on the log
// is behind the group's copy, has the record from the channel once and is
// not owed the copy. Two arms: the group handed the member the record and it
// had not acknowledged it (its in-flight table names it), or the member was
// away and the group held it (its backlog names it).
func TestAChannelRecordAGroupHoldsComesBackToItAndNobodyElse(t *testing.T) {
	const group, topic = "$share/g/events/#", "events/copied"
	for _, sent := range []bool{true, false} {
		name := map[bool]string{true: "handed and unacknowledged", false: "held while its member was away"}[sent]
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.db")
			h := startDurableSQLite(t, path)
			watcher := dial(t, h, "watcher", false, false, 0, 3600, 0)
			watcher.Sub(t, "#", 1)
			before := h.Disconnects.Snapshot("watcher")
			watcher.Close()
			sessionGoneAfter(t, h, "watcher", before)
			member := dial(t, h, "member", false, true, 10, 3600, 0)
			member.SubWithID(t, group, 1, 9)
			if !sent {
				before := h.Disconnects.Snapshot("member")
				member.Close()
				sessionGoneAfter(t, h, "member", before)
			}
			p := dial(t, h, "publisher", false, false, 0, 3600, 0)
			p.Pub(t, topic, "copied")

			if sent {
				if !waitUntil(3*time.Second, func() bool { return member.Count() == 1 }) {
					t.Fatalf("the member was sent %q, want the record", member.Payloads())
				}
			} else if !waitUntil(3*time.Second, func() bool { return backlogLen(t, h, group) == 1 }) {
				t.Fatalf("the group holds %d with its member away, want the record, so the arm is "+
					"not the one named", backlogLen(t, h, group))
			}
			h.Crash()

			h2 := startDurableSQLite(t, path)
			watcher2 := dial(t, h2, "watcher", false, false, 0, 3600, 0)
			member2 := dial(t, h2, "member", false, false, 0, 3600, 0)
			if !waitUntil(5*time.Second, func() bool { return member2.Count() >= 1 }) {
				t.Fatalf("the member was sent nothing after the restart: the record its group held "+
					"for it is lost (received %q)", member2.Payloads())
			}
			if got := member2.Payloads(); !slices.Equal(got, []string{"copied"}) {
				t.Errorf("the member was sent %q after the restart, want the one record", got)
			}
			// Sent with its subscription to the group's options: the member has
			// no other subscription to take them from.
			if ids := member2.All()[0].SubIDs; !slices.Equal(ids, []int{9}) {
				t.Errorf("the copy was sent with Subscription Identifiers %v, want its shared "+
					"subscription's [9]", ids)
			}
			// Owed nothing from the log: the start counted the group's copy for
			// the group, not for a subscription its topic matches.
			if owed, _ := h2.B.OwedBroadcast("watcher"); len(owed) != 0 {
				t.Errorf("the watcher is owed %v from the broadcast log after the restart: the "+
					"group's copy was counted for its own subscription", owed)
			}
			// The record once, from the channel, whose position it resumes from.
			if !waitUntil(5*time.Second, func() bool {
				for _, r := range watcher2.All() {
					if _, stamped := r.User["saguin-offset"]; stamped && r.Payload == "copied" {
						return true
					}
				}
				return false
			}) {
				t.Fatalf("the watcher was sent %q after the restart, want the record from the channel",
					watcher2.Payloads())
			}
			// And the copy, if it were owed it, from the broadcast log, which
			// serves it in offset order: before an end marker published now.
			got := beforeMarker(t, connect(t, h2, "marker", true, false), "loose/end", watcher2)
			if !slices.Equal(got, []string{"copied"}) {
				t.Errorf("the watcher was sent %q after the restart, want the record once: the copy "+
					"kept for the group was counted for it too", got)
			}
		})
	}
}

// RFC 0003 "Broadcast": a member whose session outlives its connection is
// sent a channel's record, through its group's copy in the broadcast log, as
// a clean member is sent it live: the same topic, payload, User Properties,
// publish properties and Subscription Identifier.
func TestADurableMemberIsSentAChannelRecordAsACleanOneIs(t *testing.T) {
	h := start(t)
	// Session Expiry 0: a session that ends with its connection, served live.
	clean := dial(t, h, "clean", true, false, 0, 0, 0)
	clean.SubWithID(t, "$share/a/events/#", 1, 7)
	durable := dial(t, h, "durable", false, false, 0, 3600, 0)
	durable.SubWithID(t, "$share/b/events/#", 1, 7)
	// The durable member's group reads the log from its cursor, which its
	// SUBSCRIBE gives it after the SUBACK; the clean one's is live at it.
	awaitGroup(t, h, "$share/b/events/#")

	p := connect(t, h, "publisher", true, false)
	fmt1 := byte(1)
	p.PubProps(t, "events/props/1", "same", &paho.PublishProperties{
		ContentType: "text/plain", ResponseTopic: "reply/here", CorrelationData: []byte{0, 1, 0xff},
		PayloadFormat: &fmt1, MessageExpiry: func() *uint32 { v := uint32(60); return &v }(),
		User: paho.UserProperties{{Key: "mine", Value: "kept"}, {Key: "saguin-offset", Value: "999"}},
	})
	if !waitUntil(3*time.Second, func() bool { return clean.Count() == 1 && durable.Count() == 1 }) {
		t.Fatalf("clean was sent %q and durable %q, want the record each", clean.Payloads(), durable.Payloads())
	}
	c, d := clean.All()[0], durable.All()[0]
	if c.Expiry == nil || d.Expiry == nil || *c.Expiry == 0 || *d.Expiry == 0 || *c.Expiry > 60 || *d.Expiry > 60 {
		t.Errorf("the Message Expiry Intervals arrived as %v and %v, want each within 60", c.Expiry, d.Expiry)
	}
	c.Expiry, d.Expiry = nil, nil
	if fmt.Sprintf("%+v", c) != fmt.Sprintf("%+v", d) {
		t.Errorf("the durable member was sent\n  %+v\nwhere the clean one was sent\n  %+v", d, c)
	}
}

// RFC 0003 "Broadcast": a member whose session outlives its connection and
// that has its own subscription to a channel topic as well as a shared one
// is sent a record both reach twice - once from the channel, stamped with its
// offset, and once as the copy its group handed it, unstamped - as MQTT has
// a client with a shared and a non-shared subscription that overlap.
func TestAMemberWithItsOwnSubscriptionToTheChannelIsSentTheRecordTwice(t *testing.T) {
	h := start(t)
	both := dial(t, h, "both", false, false, 0, 3600, 0)
	both.Sub(t, "events/keep/#", 1)
	both.Sub(t, "$share/g/events/#", 1)
	awaitGroup(t, h, "$share/g/events/#")

	p := connect(t, h, "producer", true, false)
	p.Pub(t, "events/keep/2", "overlap")
	if !waitUntil(3*time.Second, func() bool { return both.Count() >= 2 }) {
		t.Fatalf("the member was sent %q, want the record twice", both.Payloads())
	}
	// An end marker down both roads: the channel serves it after the record
	// in offset order, and the group after its copy of the record, so a third
	// copy down either would be read before the marker it came with.
	p.Pub(t, "events/keep/3", "end-marker")
	eventually(t, "the member is sent the end marker twice", 5*time.Second, func() bool {
		n := 0
		for _, x := range both.Payloads() {
			if x == "end-marker" {
				n++
			}
		}
		return n == 2
	})
	var got []brokertest.Received
	for _, r := range both.All() {
		if r.Payload != "end-marker" {
			got = append(got, r)
		}
	}
	if len(got) != 2 {
		t.Fatalf("the member was sent %d deliveries (%q), want two", len(got), both.Payloads())
	}
	stamped := 0
	for _, r := range got {
		if r.Payload != "overlap" {
			t.Errorf("the member was sent %q, want the record", r.Payload)
		}
		if _, ok := r.User["saguin-offset"]; ok {
			stamped++
		}
	}
	if stamped != 1 {
		t.Errorf("%d of the two deliveries carry saguin-offset, want one: the channel's own, "+
			"beside the group's copy", stamped)
	}
}

// RFC 0003 "Broadcast" and "`append` - Durable consumers": a shared group
// over a channel follows the channel at subscribe - it is owed what is
// published after it began, with no replay - and never moves the channel's
// own consumers. A durable consumer of events/# is sent each record once from
// the channel and its position moves with its own acknowledgements alone; the
// group, whose only member is away, holds the records published after it
// began and not the one before, and its member is sent those on its return.
// The consumer is sent nothing more, before the member's return or after.
func TestAGroupOverAChannelLeavesItsConsumersAndPositionsAlone(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		const group = "$share/g/events/#"
		reader := dial(t, h, "reader", false, false, 0, 3600, 0)
		reader.Sub(t, "events/#", 1)
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "events/a", "before")
		awayMember(t, h, "m", group, 1)
		for _, m := range []string{"one", "two", "three"} {
			p.Pub(t, "events/a", m)
		}
		eventually(t, "the reader is sent all four and the group holds three", 5*time.Second, func() bool {
			return reader.Count() == 4 && backlogLen(t, h, group) == 3
		})
		var pos uint64
		eventually(t, "the reader's position is stored past all four", 5*time.Second, func() bool {
			var ok bool
			pos, ok, _ = h.B.ChannelPosition("events", "reader")
			return ok && pos == 5
		})

		back := dial(t, h, "m", false, false, 0, 3600, 0)
		eventually(t, "the member is sent the three", 5*time.Second, func() bool { return back.Count() == 3 })
		// One end marker for both: the member is sent it through its group
		// and the reader from the channel, each after all it was owed.
		if got := strings.Join(beforeMarker(t, p, "events/a", back), " "); got != "one two three" {
			t.Errorf("the member was sent %q, want one two three: nothing before the group began", got)
		}
		eventually(t, "the reader is sent the end marker", 5*time.Second, func() bool {
			return slices.Contains(reader.Payloads(), "end-marker")
		})
		if got := strings.Join(reader.Payloads(), " "); got != "before one two three end-marker" {
			t.Errorf("the reader was sent %q, want each record once, from the channel", got)
		}
		// Its own acknowledgement of the marker moves it one on, and nothing
		// the group's member acknowledged moves it anywhere else.
		var now uint64
		if !waitUntil(5*time.Second, func() bool {
			now, _, _ = h.B.ChannelPosition("events", "reader")
			return now == pos+1
		}) {
			t.Errorf("the reader's position is %d after the group's member took its three and the "+
				"reader acknowledged one more, want %d: a group moves no consumer's position", now, pos+1)
		}
		if _, ok, _ := h.B.ChannelPosition("events", "m"); ok {
			t.Error("the group's member holds a position on the channel: a group holds none")
		}
	})
}

// RFC 0003 "Broadcast" and "`latest`": a shared group over a latest channel is
// owed the changes published after it began, and no current-state pass: the
// value the topic held when its member subscribed is not among them.
func TestAGroupOverALatestChannelIsOwedChangesOnly(t *testing.T) {
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		const group = "$share/g/state/#"
		p := connect(t, h, "producer", true, false)
		p.Pub(t, "state/a", "current")
		awayMember(t, h, "m", group, 1)
		p.Pub(t, "state/a", "changed")
		eventually(t, "the group holds the change", 5*time.Second, func() bool { return backlogLen(t, h, group) == 1 })

		back := dial(t, h, "m", false, false, 0, 3600, 0)
		eventually(t, "the member is sent the change", 5*time.Second, func() bool { return back.Count() == 1 })
		if got := beforeMarker(t, p, "state/a", back); !slices.Equal(got, []string{"changed"}) {
			t.Errorf("the member was sent %q, want only the change published after the group began", got)
		}
	})
}

// A connection that refuses a group's delivery for its size, although its
// member was chosen for fitting it, is hung up on, 0x95, and the delivery is
// undone to the group and counted nothing - so the group gives it to a member
// it fits. No path reaches this while fitsShared measures with its margin;
// the test measures with one far below zero, so every member looks as if it
// fits and the engine's own check refuses the write. At QoS 0, handed to a
// durable member, and lent to a clean one.
func TestADeliveryRefusedAfterItWasMeasuredAsFittingGoesBackToItsGroup(t *testing.T) {
	for _, c := range []struct {
		name  string
		qos   byte
		clean bool
	}{{"at QoS 0", 0, false}, {"handed to a durable member", 1, false}, {"lent to a clean member", 1, true}} {
		t.Run(c.name, func(t *testing.T) {
			t.Cleanup(broker.MeasureSharedFitWith(-1 << 20))
			h := start(t)
			ops := operationsAt(t, h)
			const group = "$share/g/news/#"
			awayMember(t, h, "d", group, 1)
			expiry := uint32(3600)
			if c.clean {
				expiry = 0
			}
			small := dial(t, h, "small", c.clean, false, 0, expiry, 256)
			small.Sub(t, group, c.qos)
			big := strings.Repeat("x", 1024)
			connect(t, h, "producer", true, false).Pub(t, "news/x", big)

			select {
			case d := <-small.Gone:
				if d.ReasonCode != 0x95 {
					t.Fatalf("the member was disconnected with %#x, want 0x95 Packet too large", d.ReasonCode)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the member whose connection refused the delivery was not hung up on")
			}
			eventually(t, "the delivery is the group's again", 5*time.Second, func() bool {
				return backlogLen(t, h, group) == 1
			})
			if g := scrapeGauges(t, ops); sharesDropped(g) != 0 {
				t.Errorf("the group counted %v dropped, want nothing: the delivery went back", sharesDropped(g))
			}
			holdsExactly(t, h, ops, group)
			back := dial(t, h, "d", false, false, 0, 3600, 0)
			eventually(t, "a member it fits is sent it", 5*time.Second, func() bool { return back.Count() == 1 })
		})
	}
}

// RFC 0005 `saguin_shares_dropped_total`: a delivery a group had handed out,
// and so counted drained, is counted held again as it is dropped - held less
// drained less dropped stays what the groups hold. Each way one is dropped
// into a group that has ended: an undone hand-over the store has no group to
// take back (no_member_left), or whose group's list ended as it went back; a
// clean member's lent QoS 1 delivery at its ending; and a durable member's
// ending, orphaned or returned to a group that has gone.
func TestEveryHandedDeliveryDroppedKeepsTheSharesIdentity(t *testing.T) {
	type dropper func(t *testing.T, h *brokertest.Harness, group string, f store.InFlight)
	for _, c := range []struct {
		name  string
		clean bool
		drop  dropper
	}{
		{"undone with no group in the store", false, func(t *testing.T, h *brokertest.Harness, group string, f store.InFlight) {
			s, ok := brokertest.HarnessSessions.(interface{ DropShareCursor(string) (bool, error) })
			if !ok {
				t.Fatalf("the harness's session store is a %T", brokertest.HarnessSessions)
			}
			if ended, err := s.DropShareCursor(group); err != nil || !ended {
				t.Fatalf("end the group's cursor in the store: %v, %v", ended, err)
			}
			if !h.B.Unhand("m", f.PacketID) {
				t.Fatal("the hand-over was not undone")
			}
		}},
		{"undone as its group's list ended", false, func(t *testing.T, h *brokertest.Harness, group string, f store.InFlight) {
			if !h.B.UnhandIntoEndedGroup("m", f.PacketID) {
				t.Fatal("the hand-over was not undone")
			}
		}},
		{"lent at QoS 1 to a clean member whose group ended", true, func(t *testing.T, h *brokertest.Harness, group string, f store.InFlight) {
			h.B.EndGroupForTest(group)
		}},
		{"orphaned at its member's ending", false, func(t *testing.T, h *brokertest.Harness, group string, f store.InFlight) {
			h.B.EndWith("m", store.Dropped{Orphaned: []store.InFlight{f}})
		}},
		{"returned at its member's ending to a group that had gone", false, func(t *testing.T, h *brokertest.Harness, group string, f store.InFlight) {
			h.B.EndGroupForTest(group)
			h.B.EndWith("m", store.Dropped{Returned: map[string][]uint64{group: {f.Offset}}})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
				ops := operationsAt(t, h)
				const group = "$share/g/news/#"
				awayMember(t, h, "d", group, 1)
				expiry := uint32(3600)
				if c.clean {
					expiry = 0
				}
				m := dial(t, h, "m", c.clean, true, 10, expiry, 0)
				m.Sub(t, group, 1)
				connect(t, h, "producer", true, false).Pub(t, "news/x", "handed")
				eventually(t, "m is handed it", 5*time.Second, func() bool { return m.Count() == 1 })
				var f store.InFlight
				if !c.clean {
					flying := h.B.FlyingBroadcast("m")
					if len(flying) != 1 {
						t.Fatalf("m has %+v on the wire, want the one delivery", flying)
					}
					f = flying[0]
				}
				before := scrapeGauges(t, ops)[sharesNoMember]
				c.drop(t, h, group, f)
				if c.clean {
					gone := h.Disconnects.Snapshot("m")
					m.Close()
					sessionGoneAfter(t, h, "m", gone)
				}
				eventually(t, "the delivery is counted no_member_left", 5*time.Second, func() bool {
					return scrapeGauges(t, ops)[sharesNoMember] == before+1
				})
				holdsExactly(t, h, ops, group)
			})
		})
	}
}

// RFC 0005 `saguin_shares_drained_total`: held less drained less dropped is
// what the groups hold, exactly - across a restart too. The counters start
// at zero and a group's backlog does not: a start that put five back on the
// group's list without counting them held read 0 with five held, and -5
// once the member took them. On both providers.
func TestARestartCountsTheBacklogItPutsBackAsHeld(t *testing.T) {
	const group = "$share/g/news/#"
	const n = 5
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			ops := operationsAt(t, h)
			awayMember(t, h, "member", group, 1)
			pub := connect(t, h, "producer", true, false)
			for i := range n {
				pub.Pub(t, "news/x", fmt.Sprint("m-", i))
			}
			eventually(t, "the group holds all five", 5*time.Second, func() bool { return backlogLen(t, h, group) == n })
			holdsExactly(t, h, ops, group)
			h.Stop()

			h = p.start(t)
			ops = operationsAt(t, h)
			if got := backlogLen(t, h, group); got != n {
				t.Fatalf("the group holds %d after the start, want %d, so this proves nothing", got, n)
			}
			holdsExactly(t, h, ops, group)
			m := dial(t, h, "member", false, false, 10, 3600, 0)
			awaitCount(t, m, n, 5*time.Second)
			eventually(t, "handed out", 5*time.Second, func() bool { return backlogLen(t, h, group) == 0 })
			holdsExactly(t, h, ops, group)
		})
	}
}

// The same identity where a start finds a group whose last member expired
// while the broker was stopped: what it drops, no_member_left, it counts held
// first - it held it, before the stop - so the identity reads the nothing the
// groups hold, not -3.
func TestAGroupEndedAtAStartCountsWhatItDropsAsHeld(t *testing.T) {
	const group = "$share/g/news/#"
	const n = 3
	for _, p := range restartables(t) {
		t.Run(p.name, func(t *testing.T) {
			h := p.start(t)
			ops := operationsAt(t, h)
			m := dial(t, h, "short", false, false, 0, 2, 0)
			m.Sub(t, group, 1)
			awaitGroup(t, h, group)
			before := h.Disconnects.Snapshot("short")
			m.Close()
			sessionGoneAfter(t, h, "short", before)
			pub := connect(t, h, "producer", true, false)
			for i := range n {
				pub.Pub(t, "news/x", fmt.Sprint("m-", i))
			}
			eventually(t, "the group holds all three", 5*time.Second, func() bool { return backlogLen(t, h, group) == n })
			h.Stop()
			time.Sleep(2500 * time.Millisecond) // past the member's expiry

			h = p.start(t)
			ops = operationsAt(t, h)
			g := scrapeGauges(t, ops)
			if g[sharesNoMember] != n {
				t.Fatalf("%s is %v, want %d, so the start did not end the group", sharesNoMember, g[sharesNoMember], n)
			}
			if got := g[sharesHeld] - g[sharesDrained] - sharesDropped(g); got != 0 {
				t.Errorf("held %v less drained %v less dropped %v is %v, and no group holds anything",
					g[sharesHeld], g[sharesDrained], sharesDropped(g), got)
			}
		})
	}
}

// RFC 0002 `broker.share`: what one group's backlog may cost is
// limits.session_queue_bytes, and a delivery that finds it full is dropped
// and counted backlog_full - counted held first, so held less drained less
// dropped is still what the group holds. A deaf clean member is lent what
// fills the bound, and the rest is refused at it. A refusal counted dropped
// and never held read -7 with the group holding nothing waiting.
func TestAGroupAtItsBoundCountsWhatItRefusesAsBacklogFull(t *testing.T) {
	const group = "$share/g/news/#"
	payload := strings.Repeat("x", 4096)
	capAt(t, 5*4400)
	eachSessionProvider(t, func(t *testing.T, h *brokertest.Harness) {
		ops := operationsAt(t, h)
		awayMember(t, h, "durable", group, 1)
		deaf := dial(t, h, "deaf", true, true, 20, 0, 0) // clean, never acknowledges
		deaf.Sub(t, group, 1)
		pub := connect(t, h, "producer", true, false)
		const n = 12
		for range n {
			pub.Pub(t, "news/x", payload)
		}
		eventually(t, "deaf is lent some", 5*time.Second, func() bool { return deaf.Count() >= 1 })
		// Every publish was answered, so the group was owed all twelve; what
		// is read is the counters once they have caught up with them.
		countsAddUp(t, h, ops, group, n)
		g := scrapeGauges(t, ops)
		if g[sharesBacklogFull] == 0 {
			t.Fatalf("nothing was dropped at the bound (held %v, drained %v), so this shows nothing",
				g[sharesHeld], g[sharesDrained])
		}
		if g[sharesHeld] != n {
			t.Errorf("%s is %v, want %d: every delivery the group was owed", sharesHeld, g[sharesHeld], n)
		}
		holdsExactly(t, h, ops, group)
	})
}

// RFC 0002's full-store table: a message the full log cannot keep, with
// nothing in it that may go, is lost to each group it was for, counted
// storage_full - and held first. A durable subscriber that never
// acknowledges holds every message on the wire, so nothing can be given up,
// and the group's member is away.
func TestAGroupsLossToAFullLogIsCountedAsHeldAndStorageFull(t *testing.T) {
	brokertest.SessionQueueBytes = 64 << 20
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			h, _, _ := boundedDrain(t, provider, 256*1024)
			ops := operationsAt(t, h)
			const group = "$share/g/news/#"
			awayMember(t, h, "member", group, 1)
			holder := dial(t, h, "holder", false, true, 1000, 3600, 0)
			holder.Sub(t, "news/#", 1)

			p := connect(t, h, "producer", true, false)
			payload := strings.Repeat("x", 4096)
			for range 160 {
				p.Pub(t, "news/x", payload)
			}
			countsAddUp(t, h, ops, group, 160)
			g := scrapeGauges(t, ops)
			if g[sharesStorageFull] == 0 {
				t.Fatalf("the group lost nothing to the full log (held %v), so this shows nothing", g[sharesHeld])
			}
			if g[sharesHeld] != 160 {
				t.Errorf("%s is %v, want 160: every delivery the group was owed", sharesHeld, g[sharesHeld])
			}
			holdsExactly(t, h, ops, group)
		})
	}
}

// The same where the store ends the group as it opens (EndedAtOpen): its
// cursor is in the file and no kept session holds its filter - what a stop
// between a clean start writing its record and the session it replaced
// ending leaves. Made here by taking the member's session out of the file
// between a stop and a start. What the start drops, no_member_left, it
// counts held first. sqlite, whose file a test can open between the two.
func TestAGroupTheStoreEndsAsItOpensCountsWhatItDropsAsHeld(t *testing.T) {
	const group = "$share/g/news/#"
	const n = 3
	path := filepath.Join(t.TempDir(), "s.db")
	h := startDurableSQLite(t, path)
	awayMember(t, h, "member", group, 1)
	pub := connect(t, h, "producer", true, false)
	for i := range n {
		pub.Pub(t, "news/x", fmt.Sprint("m-", i))
	}
	eventually(t, "the group holds all three", 5*time.Second, func() bool { return backlogLen(t, h, group) == n })
	h.Stop()

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if res, err := db.Exec(`DELETE FROM sessions WHERE client = 'member'`); err != nil {
		t.Fatal(err)
	} else if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("took %d sessions out of the file, want the member's", n)
	}
	_ = db.Close()

	h = startDurableSQLite(t, path)
	g := scrapeGauges(t, operationsAt(t, h))
	if g[sharesNoMember] != n {
		t.Fatalf("%s is %v, want %d, so the store did not end the group as it opened", sharesNoMember,
			g[sharesNoMember], n)
	}
	if got := g[sharesHeld] - g[sharesDrained] - sharesDropped(g); got != 0 {
		t.Errorf("held %v less drained %v less dropped %v is %v, and no group holds anything",
			g[sharesHeld], g[sharesDrained], sharesDropped(g), got)
	}
}

// handOvers counts a group's hand-overs asked of the store and those refused
// for the member's window, and refuses the next one it is told to.
type handOvers struct {
	drainSessions
	calls, full atomic.Int64
	refuse      atomic.Bool
}

func (c *handOvers) HandOver(group string, cursor uint64, client string, window uint16, fs []store.InFlight) error {
	c.calls.Add(1)
	if c.refuse.CompareAndSwap(true, false) {
		c.full.Add(1)
		return store.ErrWindowFull
	}
	err := c.drainSessions.HandOver(group, cursor, client, window, fs)
	if errors.Is(err, store.ErrWindowFull) {
		c.full.Add(1)
	}
	return err
}

// A durable member acknowledging each delivery frees its connection's slot at
// once and the store's row at its next flush, and a group is not handed to
// it in between: on one core a group asked the store, was refused, and asked
// again, 9,000 times a delivery, with the member's flush waiting for the
// core. Not parallel, since it sets GOMAXPROCS, which one core is the case
// for: the edge hardware Sagüin runs on.
func TestAGroupOnOneCoreDoesNotAskAMemberWithNoRoomAgainAndAgain(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	h := startDurable(t, t.TempDir())
	w := &handOvers{}
	attachDrainThrough(t, h, func(s drainSessions) drainSessions { w.drainSessions = s; return w })
	const group = "$share/g/news/#"
	m := dial(t, h, "m", false, true, 1, 3600, 0)
	m.Sub(t, group, 1)
	p := connect(t, h, "producer", true, false)
	const n = 20
	for i := range n {
		p.Pub(t, "news/x", fmt.Sprint(i))
	}
	for deadline := time.Now().Add(20 * time.Second); m.Count() < n; time.Sleep(2 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d delivered to the member (%d hand-overs asked, %d refused)", m.Count(), n,
				w.calls.Load(), w.full.Load())
		}
		ackAll(m)
	}
	if calls := w.calls.Load(); calls > n+2 {
		t.Errorf("%d deliveries took %d hand-overs asked of the store, %d refused for the window", n, calls,
			w.full.Load())
	}
}

// RFC 0002: no more than half a session's bound is on the wire, and a
// shared group's hand-over to a durable member keeps to it as the member's
// own drain does, each delivery counted as its message and the in-flight
// PUBLISH held for it (WireEntryCost). A member that never acknowledges,
// with a window of a hundred, is handed two of five: half its bound is room
// for two and not three, whatever the few hundred bytes a message itself
// counts.
func TestAGroupHandsAMemberNoMoreThanHalfItsBoundOnTheWire(t *testing.T) {
	const half = 2450
	if 2*(400+broker.WireEntryCost) > half || 3*(50+broker.WireEntryCost) <= half {
		t.Fatalf("half the bound, %d, is not room for two deliveries and not three at WireEntryCost %d",
			half, broker.WireEntryCost)
	}
	capAt(t, 2*half)
	h := startDurable(t, t.TempDir())
	const group = "$share/g/news/#"
	m := dial(t, h, "m", false, true, 100, 3600, 0)
	m.Sub(t, group, 1)
	p := connect(t, h, "producer", true, false)
	for i := range 5 {
		p.Pub(t, "news/x", fmt.Sprint(i))
	}
	eventually(t, "the member is handed its first two", 5*time.Second, func() bool { return m.Count() >= 2 })
	time.Sleep(300 * time.Millisecond)
	if got := m.Count(); got != 2 {
		t.Fatalf("a member that never acknowledged was handed %d of 5, want 2: half its bound is room for two", got)
	}
	ackAll(m)
	eventually(t, "the rest, once it acknowledges", 5*time.Second, func() bool {
		ackAll(m)
		return m.Count() == 5
	})
}

// A hand-over the store refuses for the member's window - its own write
// filled the table after the group asked - stops the group's drain rather
// than asking again, and the member's flush, clearing the rows its
// acknowledgements freed, wakes the group: the delivery is handed out then,
// and not left for the next publish. The refusal is forced once.
func TestAHandOverRefusedForTheWindowWaitsForTheMembersFlush(t *testing.T) {
	h := startDurable(t, t.TempDir())
	w := &handOvers{}
	attachDrainThrough(t, h, func(s drainSessions) drainSessions { w.drainSessions = s; return w })
	const group = "$share/g/news/#"
	m := dial(t, h, "m", false, true, 2, 3600, 0)
	m.Sub(t, group, 1)
	p := connect(t, h, "producer", true, false)
	p.Pub(t, "news/x", "first")
	eventually(t, "the member is sent the first", 5*time.Second, func() bool { return m.Count() == 1 })
	// What the group and the member hold at the end of the pass the refusal
	// was made in, read there: a drain that asked again at once did it in
	// that pass, and the member's flush may rightly wake another after it.
	type held struct {
		list   []broker.GroupEntry
		flying int
	}
	atEnd := make(chan held, 1)
	var once sync.Once
	defer broker.SetGroupPass(func(b *broker.Broker, g string, starting bool) {
		if b == h.B && g == group && !starting && w.full.Load() >= 1 {
			once.Do(func() {
				list, _ := h.B.GroupList(group)
				atEnd <- held{list, len(h.B.FlyingBroadcast("m"))}
			})
		}
	})()
	w.refuse.Store(true)
	p.Pub(t, "news/x", "second")
	eventually(t, "the second is refused once", 5*time.Second, func() bool { return w.full.Load() == 1 })
	select {
	case got := <-atEnd:
		if len(got.list) != 1 || got.flying != 1 {
			t.Fatalf("the refused pass ended with the group holding %+v and m %d on the wire, want "+
				"the second waiting and only the first sent: the refusal did not stop the drain",
				got.list, got.flying)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pass that was refused never ended")
	}
	ackAll(m)
	eventually(t, "the member's flush wakes the group and the second is handed out", 5*time.Second,
		func() bool { return m.Count() == 2 })
}

// RFC 0003 "Broadcast": a durable member's group owns what reaches the member
// through it from the moment the subscription can match a publish, not from
// its SUBACK. A publish landing after the SUBACK is written and before what
// the SUBSCRIBE earned is served (SetSubackBeforeServed, the window forced)
// is the group's: handed to the member in its window, so a member that did
// not acknowledge it is sent it again after a restart - a crash on sqlite, a
// graceful stop on a memory provider with a snapshot_dir.
func TestAPublishBeforeAMembersSubackIsServedIsTheGroups(t *testing.T) {
	const group, topic = "$share/g/gap/#", "gap/x"
	for _, provider := range []string{"memory", "sqlite"} {
		t.Run(provider, func(t *testing.T) {
			dir := t.TempDir()
			start := func() *harness {
				if provider == "sqlite" {
					return startDurableSQLite(t, filepath.Join(dir, "sessions.db"))
				}
				return startDurable(t, dir)
			}
			h := start()
			p := connect(t, h, "publisher", true, false)
			m := dial(t, h, "member", false, true, 10, 3600, 0)
			var fired atomic.Bool
			answered := make(chan string, 1)
			restore := broker.SetSubackBeforeServed(func(id string) {
				if id != "member" || !fired.CompareAndSwap(false, true) {
					return
				}
				resp, err := p.C.Publish(context.Background(), &paho.Publish{Topic: topic, QoS: 1, Payload: []byte("in the gap")})
				switch {
				case err != nil:
					answered <- err.Error()
				case resp.ReasonCode != 0:
					answered <- fmt.Sprintf("PUBACK %#x", resp.ReasonCode)
				default:
					answered <- ""
				}
			})
			m.Sub(t, group, 1)
			select {
			case why := <-answered:
				if why != "" {
					t.Fatalf("the publish in the window was answered %s, want PUBACK 0x00", why)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the window between the SUBACK and its deliveries was never opened, so this proves nothing")
			}
			restore()
			eventually(t, "the member is sent the publish", 3*time.Second, func() bool { return m.Count() == 1 })
			if got := m.Payloads(); !slices.Equal(got, []string{"in the gap"}) {
				t.Fatalf("the member was sent %q, want the publish", got)
			}
			if provider == "sqlite" {
				h.Crash()
			} else {
				h.Stop()
			}

			h2 := start()
			m2 := dial(t, h2, "member", false, false, 0, 3600, 0)
			if !waitUntil(5*time.Second, func() bool { return m2.Count() >= 1 }) {
				t.Fatalf("the member was sent %q before the restart and nothing after it, unacknowledged: "+
					"the group recorded nothing of it", m.Payloads())
			}
			time.Sleep(300 * time.Millisecond)
			if got := m2.Payloads(); !slices.Equal(got, []string{"in the gap"}) {
				t.Errorf("the member was sent %q after the restart, want the unacknowledged publish once", got)
			}
		})
	}
}

// RFC 0003 "Broadcast", MQTT-3.3.4-9: what a group hands a durable member in
// the window before its SUBACK is served (SetSubackBeforeServed) keeps to
// both of the member's bounds, as anything else it hands it does: its
// Receive Maximum, and half its limits.session_queue_bytes on the wire. Each
// case lands more publishes in that window than its bound allows; the member
// is sent what fits, and the rest once it acknowledges, in the order
// published.
func TestPublishesBeforeAMembersSubackIsServedKeepToItsBounds(t *testing.T) {
	const group, topic = "$share/g/gap/#", "gap/x"
	const half = 2450
	if 2*(400+broker.WireEntryCost) > half || 3*(50+broker.WireEntryCost) <= half {
		t.Fatalf("half the bound, %d, is not room for two deliveries and not three at WireEntryCost %d",
			half, broker.WireEntryCost)
	}
	sent := []string{"0", "1", "2", "3", "4"}
	for _, c := range []struct {
		name  string
		rxMax uint16
		bound int64 // limits.session_queue_bytes, or 0 for the harness's own
		fits  int
	}{
		{"receive maximum 1", 1, 0, 1},
		{"half its bytes bound", 100, 2 * half, 2},
	} {
		for _, provider := range []string{"memory", "sqlite"} {
			t.Run(c.name+"/"+provider, func(t *testing.T) {
				if c.bound > 0 {
					capAt(t, c.bound)
				}
				var h *harness
				if provider == "sqlite" {
					h = startDurableSQLite(t, filepath.Join(t.TempDir(), "sessions.db"))
				} else {
					h = startDurable(t, t.TempDir())
				}
				p := connect(t, h, "publisher", true, false)
				m := dial(t, h, "member", false, true, c.rxMax, 3600, 0)
				var fired atomic.Bool
				answered := make(chan string, 1)
				restore := broker.SetSubackBeforeServed(func(id string) {
					if id != "member" || !fired.CompareAndSwap(false, true) {
						return
					}
					why := ""
					for _, payload := range sent {
						resp, err := p.C.Publish(context.Background(),
							&paho.Publish{Topic: topic, QoS: 1, Payload: []byte(payload)})
						if err != nil {
							why = err.Error()
							break
						}
						if resp.ReasonCode != 0 {
							why = fmt.Sprintf("PUBACK %#x to %q", resp.ReasonCode, payload)
							break
						}
					}
					answered <- why
				})
				defer restore()
				m.Sub(t, group, 1)
				select {
				case why := <-answered:
					if why != "" {
						t.Fatalf("a publish in the window was answered %s, want PUBACK 0x00", why)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("the window between the SUBACK and its deliveries was never opened, so this proves nothing")
				}
				restore()
				eventually(t, "the member is sent what fits", 3*time.Second, func() bool { return m.Count() >= c.fits })
				time.Sleep(300 * time.Millisecond)
				if got := m.Payloads(); !slices.Equal(got, sent[:c.fits]) {
					t.Fatalf("a member that acknowledged nothing was sent %q, want %q: its bound is room for %d",
						got, sent[:c.fits], c.fits)
				}
				if !waitUntil(5*time.Second, func() bool {
					ackAll(m)
					return m.Count() >= len(sent)
				}) {
					t.Fatalf("the member, acknowledging everything it was sent, was sent %q, want %q",
						m.Payloads(), sent)
				}
				if got := m.Payloads(); !slices.Equal(got, sent) {
					t.Errorf("the member was sent %q, want %q in the order published", got, sent)
				}
			})
		}
	}
}
