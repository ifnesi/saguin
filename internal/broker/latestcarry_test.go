package broker_test

// A latest value chosen for a subscriber must reach it even when the
// subscriber's delivery record is tidied away between the choice and the
// write.
//
// **Why this can happen at all.** A latest subscriber holds no cursor, so
// once its values are acknowledged its record holds nothing - and an empty
// record is tidied, marked gone and dropped from the registry, by whatever
// asks next. Its own PUBACKs do not ask: the acknowledgement path removes
// the in-flight entry in place and never tidies. A SUBSCRIBE or UNSUBSCRIBE
// does. Every change of subscriptions replans the client, and a client with
// only latest subscriptions has no append plan to build, so the replan
// tidies the record at once.
//
// publishLatest chooses a value's subscribers under b.mu and writes to them
// after letting it go. A subscription change in that gap tidies the record
// the write was chosen against. A write that took the tidied record for a
// departed subscriber would drop the value, and the subscriber - connected
// and subscribed throughout - would keep its previous value as current
// state while the broker held the new one; if that was the topic's last
// update, for good.
//
// The rule it held: a gone record is re-resolved, never a reason to drop.
// Proposed the other way in review on 2026-09-23 and caught against the
// code before anything was written. The first draft of this test assumed
// the previous value's PUBACK did the tidying, and its own check that the
// race had formed failed - which is how the real trigger was found.
//
// **Since the same day the race cannot form, and this test says so.** A
// live value now waits in the subscriber's pending list and is written by
// the subscriber's own drain, and waiting it pins the record: a value in the
// record's pending list, or a drain writing one, is part of what makes a
// record not empty (tidyConsumerLocked reads both). So a subscription change
// between the choice and the write finds the record pinned and leaves it. What this holds now is that pin - the change lands while the value
// waits, the record is still there, and the value arrives and its
// acknowledgement clears it. latestCarry.register keeps the re-resolution on
// gone as the defence it always was.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
)

func TestAWaitingLatestValuePinsItsSubscribersRecord(t *testing.T) {
	h := start(t)
	p := connect(t, h, "producer", true, false)
	s := connect(t, h, "reader", true, true) // manual ack: v1 stays in flight
	s.Sub(t, "state/#", 1)
	// **The SUBSCRIBE's current-state pass has run before v1 is published.**
	// It runs on the reader's read loop once the SUBACK is written, so a v1
	// published the moment the SUBACK arrives can be delivered live and read
	// back by that pass as well: the reader is sent v1 twice, acknowledges
	// the first, and the second stays in flight below. Measured on two loaded
	// cores before this barrier: 7 of 200 runs, the broker registering offset
	// 1 under packet 1 (live) and packet 2 (RETAIN set). A reader's packets
	// are handled in order, so the barrier's PUBACK follows that pass.
	settle(t, s)

	p.Pub(t, "state/line/1/tag", "v1")
	first, ok := s.Await(t, 3*time.Second)
	if !ok || first.Payload != "v1" {
		t.Fatalf("the subscriber never received v1: %v %q", ok, first.Payload)
	}
	// Acknowledged, so the record holds nothing - but it is still in the
	// registry, because the PUBACK path does not tidy. That empty record is
	// what v2 will be chosen against.
	first.Ack()
	// **Waited for, not slept for**: the acknowledgement has to have reached
	// the broker and emptied the record before v2 is chosen, or the record
	// still holds v1 when the subscription changes, is not empty, and is not
	// tidied - which is how this test's first version failed to build the
	// race. Empty and present is the precondition.
	const empty = "cursors=0 inflight=0 plans=0 hasQueues=false latest=0 draining=0 gone=false"
	acked := time.Now().Add(30 * time.Second)
	for h.B.ConsumerHolding("reader") != empty && time.Now().Before(acked) {
		time.Sleep(time.Millisecond)
	}
	if got := h.B.ConsumerHolding("reader"); got != empty {
		t.Fatalf("after v1 was acknowledged the reader's record is %q, not present and "+
			"empty, so the race below cannot be constructed; the reader was sent %+v", got, s.All())
	}

	// Hold v2's write, and in the gap change the reader's subscriptions: the
	// replan finds no append plan to build, and would tidy the record if the
	// waiting value did not pin it.
	var once sync.Once
	pinned := make(chan string, 1)
	restore := broker.SetLatestBeforeWrite(func(id string) {
		if id != "reader" {
			return
		}
		once.Do(func() {
			// From a goroutine, and waited for: this runs on the reader's
			// drain, where a failing helper's t.Fatal would end the wrong
			// goroutine.
			subbed := make(chan struct{})
			go func() { s.Sub(t, "bcast/#", 1); close(subbed) }()
			select {
			case <-subbed:
			case <-time.After(3 * time.Second):
				pinned <- "the subscription change never completed"
				return
			}
			if !h.B.ConsumerRecorded("reader") {
				pinned <- "the subscription change tidied the reader's record while v2 " +
					"waited to be written"
				return
			}
			pinned <- ""
		})
	})
	t.Cleanup(restore)

	p.Pub(t, "state/line/1/tag", "v2")

	// **The instrument proves the change landed inside the wait** before the
	// pin is judged: the seam ran for the reader, on v2's write, and the
	// SUBSCRIBE completed before the write went ahead.
	select {
	case why := <-pinned:
		if why != "" {
			t.Fatalf("%s - it holds %q", why, h.B.ConsumerHolding("reader"))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the seam never ran for the reader, so v2's write was never held")
	}

	second, ok := s.Await(t, 3*time.Second)
	if !ok {
		t.Fatal("v2 never reached the reader. It was chosen as a subscriber, its record " +
			"was tidied by a subscription change while the write was pending, and the write " +
			"dropped the value: the " +
			"reader is connected and subscribed and holds v1 as current state while the " +
			"broker holds v2")
	}
	if second.Payload != "v2" {
		t.Fatalf("the reader's next value is %q, want v2", second.Payload)
	}

	// And the value it was sent is one it can acknowledge: its in-flight
	// entry was registered on the record the registry holds, so the
	// acknowledgement finds and clears it. Registered on the tidied record
	// instead, it would sit where no PUBACK looks - the reader's record
	// would show nothing to clear, or keep the entry for ever.
	//
	// **Present and empty is the right end state, not gone**: the latest
	// acknowledgement path clears the entry in place and does not tidy, so a
	// record that held only this value stays in the registry, empty.
	second.Ack()
	deadline := time.Now().Add(3 * time.Second)
	for h.B.ConsumerHolding("reader") != empty && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := h.B.ConsumerHolding("reader"); got != empty {
		t.Errorf("v2 was acknowledged and the reader's record is %q, want present and "+
			"empty: the in-flight entry was registered somewhere the acknowledgement "+
			"did not clear it", got)
	}
}

// A latest value whose write finds the window full, because another of the
// subscriber's deliveries took the last slot after the drain had looked, is
// sent when that slot is freed - even when the acknowledgement freeing it
// arrives before the drain has decided what to do next.
//
// **The drain's "no room" came from the write, outside b.mu**, and an
// acknowledgement in the gap finds the drain's claim still held and leaves
// the value to it. A drain that then let the claim go would leave the value
// waiting beside a free slot with nobody to send it - for good, if nothing
// else was in flight. So the drain takes that decision back to the top and
// makes it again under b.mu (runLatest). Built with two seams: the append
// record is delivered just before the latest write, and acknowledged just
// after it failed.
func TestALatestValueFindingNoRoomAtItsWriteIsSentWhenRoomComes(t *testing.T) {
	h := start(t)
	p := connect(t, h, "producer", true, false)
	r := dial(t, h, "reader", true, true, 1, 3600, 0) // one-wide window, manual ack
	r.Sub(t, "state/#", 1)
	r.Sub(t, "events/#", 1)
	settle(t, r) // its subscriptions' own work has returned

	failed := make(chan string, 2)
	var beforeOnce, afterOnce sync.Once
	restoreBefore := broker.SetLatestBeforeWrite(func(id string) {
		if id != "reader" {
			return
		}
		beforeOnce.Do(func() {
			// The append record takes the one slot before v2 is written.
			done := make(chan struct{})
			go func() { p.Pub(t, "events/e", "e1"); close(done) }()
			<-done
			if !waitUntil(3*time.Second, func() bool { return r.Count() == 1 }) {
				failed <- "e1 never reached the reader, so the window was not taken"
			}
		})
	})
	t.Cleanup(restoreBefore)
	noRoom := make(chan struct{})
	restoreAfter := broker.SetLatestAfterNoRoom(func(id string) {
		if id != "reader" {
			return
		}
		afterOnce.Do(func() {
			close(noRoom)
			// Free the slot while the drain is between its failed write and
			// its next look, and wait until the broker has taken the
			// acknowledgement in.
			for _, m := range r.All() {
				if m.Payload == "e1" {
					m.Ack()
				}
			}
			if !waitUntil(3*time.Second, func() bool {
				return strings.Contains(h.B.ConsumerHolding("reader"), "inflight=0")
			}) {
				failed <- "the acknowledgement of e1 was never taken in: " + h.B.ConsumerHolding("reader")
			}
		})
	})
	t.Cleanup(restoreAfter)

	p.Pub(t, "state/x", "v2")

	select {
	case <-noRoom:
	case why := <-failed:
		t.Fatalf("the interleaving was not built: %s", why)
	case <-time.After(5 * time.Second):
		t.Fatalf("v2's write never found the window full, so this proves nothing: the reader "+
			"holds %v", r.Payloads())
	}
	if !waitUntil(3*time.Second, func() bool {
		for _, m := range r.All() {
			if m.Payload == "v2" {
				return true
			}
		}
		return false
	}) {
		select {
		case why := <-failed:
			t.Fatalf("the interleaving was not built: %s", why)
		default:
		}
		t.Fatalf("v2 was never sent: its write found no room, the slot was freed straight "+
			"after, and it waited beside a free slot with nobody to send it - the reader holds %v",
			r.Payloads())
	}
}

// The same gap on the wire: a latest value whose write finds its session's
// share of the wire full is sent when an acknowledgement empties it, even when
// that acknowledgement lands between the failed write and the drain's next
// decision. A refusal for want of wire lets the drain's claim go, because the
// acknowledgement that makes room brings it back; one that arrived in the gap
// found the claim held and brought nothing, and if it emptied the wire nothing
// else will come. So the drain asks again inside the lock, and goes round when
// the wire is empty (writeLatestBatch). The window here is wide, so it is the
// wire and only the wire that refuses v2.
func TestALatestValueFindingNoWireAtItsWriteIsSentWhenTheWireEmpties(t *testing.T) {
	const bound = 64 << 10 // half of it, 32KiB, is the wire's share
	brokertest.SessionQueueBytes = bound
	t.Cleanup(func() { brokertest.SessionQueueBytes = 0 })
	h := start(t)
	p := connect(t, h, "producer", true, false)
	r := dial(t, h, "reader", true, true, 10, 3600, 0) // ten-wide window, manual ack
	r.Sub(t, "state/#", 1)
	r.Sub(t, "events/#", 1)
	settle(t, r) // its subscriptions' own work has returned

	big := strings.Repeat("x", 30<<10) // one fits the share; two do not

	failed := make(chan string, 2)
	var beforeOnce, afterOnce sync.Once
	restoreBefore := broker.SetLatestBeforeWrite(func(id string) {
		if id != "reader" {
			return
		}
		beforeOnce.Do(func() {
			// The append record fills the share of the wire before v2 is
			// written.
			done := make(chan struct{})
			go func() { p.Pub(t, "events/e", big); close(done) }()
			<-done
			if !waitUntil(3*time.Second, func() bool { return r.Count() == 1 }) {
				failed <- "the append record never reached the reader, so the wire was not filled"
			}
		})
	})
	t.Cleanup(restoreBefore)
	noRoom := make(chan struct{})
	restoreAfter := broker.SetLatestAfterNoRoom(func(id string) {
		if id != "reader" {
			return
		}
		afterOnce.Do(func() {
			close(noRoom)
			// Empty the wire while the drain is between its failed write and
			// its next decision, and wait until the broker has taken the
			// acknowledgement in.
			for _, m := range r.All() {
				if m.Topic == "events/e" {
					m.Ack()
				}
			}
			if !waitUntil(3*time.Second, func() bool {
				return strings.Contains(h.B.ConsumerHolding("reader"), "inflight=0")
			}) {
				failed <- "the acknowledgement of the append record was never taken in: " + h.B.ConsumerHolding("reader")
			}
		})
	})
	t.Cleanup(restoreAfter)

	p.Pub(t, "state/x", big)

	select {
	case <-noRoom:
	case why := <-failed:
		t.Fatalf("the interleaving was not built: %s", why)
	case <-time.After(5 * time.Second):
		t.Fatalf("v2's write never found the wire full, so this proves nothing: the reader holds %d messages", r.Count())
	}
	if !waitUntil(3*time.Second, func() bool {
		for _, m := range r.All() {
			if m.Topic == "state/x" {
				return true
			}
		}
		return false
	}) {
		select {
		case why := <-failed:
			t.Fatalf("the interleaving was not built: %s", why)
		default:
		}
		t.Fatalf("v2 was never sent: its write found the wire full, the wire was emptied straight "+
			"after, and it waited on an empty wire with nobody to send it - the reader holds %d messages",
			r.Count())
	}
}

// offsetsOf is each topic's saguin-offset values in the order a client
// received them.
func offsetsOf(t *testing.T, all []brokertest.Received) map[string][]uint64 {
	t.Helper()
	got := map[string][]uint64{}
	for _, m := range all {
		o, err := strconv.ParseUint(m.User["saguin-offset"], 10, 64)
		if err != nil {
			t.Fatalf("a value on %s carried saguin-offset %q: %v", m.Topic, m.User["saguin-offset"], err)
		}
		got[m.Topic] = append(got[m.Topic], o)
	}
	return got
}

// RFC 0003: a subscriber is sent the current value, and each value carries
// an offset so that a consumer can tell which of two it holds is newer.
// **So an older value of a topic must never reach a subscriber after a newer
// one** - it would be applied over the newer one as though it were current,
// and if that topic is not written again, held as current for good.
//
// Two publishers setting one topic at nearly the same moment are how it
// happens: the older value is stored first, the newer one is stored and
// delivered, and only then is the older one handed to its subscribers. The
// seam holds the older value between its store write and its hand-off until
// the newer one has reached the reader, which is the whole of that
// interleaving, every time.
func TestAnOlderValueOfATopicNeverReachesASubscriberAfterANewerOne(t *testing.T) {
	// Two places the older value can be held, one per mechanism: before its
	// publish has looked at the topic's head, which the head check itself
	// answers; and after, short of the enqueue, which only the enqueue's own
	// check can answer.
	for _, c := range []struct {
		name string
		seam func(func(topic string, offset uint64)) func()
	}{
		{"overtaken before its head moved", broker.SetLatestAfterSet},
		{"overtaken after its head moved", broker.SetLatestBeforeQueue},
	} {
		t.Run(c.name, func(t *testing.T) { olderAfterNewer(t, c.seam) })
	}
}

func olderAfterNewer(t *testing.T, seam func(func(topic string, offset uint64)) func()) {
	h := start(t)
	older := connect(t, h, "pub-older", true, false)
	newer := connect(t, h, "pub-newer", true, false)
	r := connect(t, h, "reader", true, false)
	r.Sub(t, "state/#", 1)
	settle(t, r)

	var mu sync.Mutex
	holding := false
	built := make(chan string, 1)
	restore := seam(func(topic string, offset uint64) {
		if topic != "state/race" {
			return
		}
		mu.Lock()
		first := !holding
		holding = true
		mu.Unlock()
		if !first {
			return // the newer value: let it through
		}
		// The older value is stored. Hold its hand-off until the newer one,
		// stored after it, has reached the reader.
		done := make(chan error, 1)
		go func() {
			_, err := newer.C.Publish(context.Background(), &paho.Publish{
				Topic: "state/race", QoS: 1, Payload: []byte("v2")})
			done <- err
		}()
		if err := <-done; err != nil {
			built <- "the newer publish failed: " + err.Error()
			return
		}
		if !waitUntil(3*time.Second, func() bool {
			for _, m := range r.All() {
				if m.Payload == "v2" {
					return true
				}
			}
			return false
		}) {
			built <- "v2 never reached the reader while v1 was held"
			return
		}
		built <- ""
	})
	t.Cleanup(restore)

	older.Pub(t, "state/race", "v1")
	select {
	case why := <-built:
		if why != "" {
			t.Fatalf("the interleaving was not built: %s", why)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the seam never held v1, so no newer value overtook it")
	}

	// The held v1 goes on to its hand-off once the seam returns; a marker on
	// the topic, published now, comes after it if it is coming at all.
	got := receivedThroughMarkers(t, r, newer, "state/race")
	offs := offsetsOf(t, got)["state/race"]
	for i := 1; i < len(offs); i++ {
		if offs[i] < offs[i-1] {
			t.Errorf("the reader was sent offset %d after %d on state/race (%v): an older value "+
				"after a newer one", offs[i], offs[i-1], r.Payloads())
		}
	}
	if len(got) == 0 || got[len(got)-1].Payload != "v2" {
		t.Errorf("the reader holds %v and v2 is current: it settled on a value that was replaced",
			r.Payloads())
	}
}

// The same rule over many publishers of the same topics at once, and several
// subscribers with small windows so that values wait and are replaced:
// every subscriber's offsets per topic never go back, and every subscriber
// settles on the value the store holds. Seeded, and repeated - the
// interleaving is the scheduler's, and the seam test above is what makes
// the one that breaks it certain.
func TestLatestSubscribersSettleOnTheStoresValueWithOffsetsThatNeverGoBack(t *testing.T) {
	seed := time.Now().UnixNano()
	if v := os.Getenv("SAGUIN_RANDOM_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("SAGUIN_RANDOM_SEED=%q: %v", v, err)
		}
		seed = n
	}
	t.Logf("seed %d (SAGUIN_RANDOM_SEED to repeat)", seed)
	rng := rand.New(rand.NewSource(seed))

	reversals, staleEnds := 0, 0
	const trials, pubs, each, readers = 3, 8, 80, 4
	topics := []string{"state/hot/a", "state/hot/b"}
	for trial := 0; trial < trials; trial++ {
		h := start(t)
		var rs []*brokertest.Client
		for i := 0; i < readers; i++ {
			c := dial(t, h, fmt.Sprintf("reader-%d-%d", trial, i), true, false, uint16(1+rng.Intn(4)), 3600, 0)
			c.Sub(t, "state/#", 1)
			settle(t, c)
			rs = append(rs, c)
		}

		var wg sync.WaitGroup
		for p := 0; p < pubs; p++ {
			pc := connect(t, h, fmt.Sprintf("pub-%d-%d", trial, p), true, false)
			order := rng.Perm(each)
			wg.Add(1)
			go func(p int, pc *brokertest.Client, order []int) {
				defer wg.Done()
				for _, i := range order {
					topic := topics[i%len(topics)]
					if _, err := pc.C.Publish(context.Background(), &paho.Publish{
						Topic: topic, QoS: 1, Payload: []byte(fmt.Sprintf("p%d-%d", p, i)),
					}); err != nil {
						t.Errorf("publish: %v", err)
						return
					}
				}
			}(p, pc, order)
		}
		wg.Wait() // every value stored: each was acknowledged

		// The store's current value per topic, as a new subscription is
		// handed it.
		probe := connect(t, h, fmt.Sprintf("probe-%d", trial), true, false)
		probe.Sub(t, "state/hot/#", 1)
		if !waitUntil(3*time.Second, func() bool { return probe.Count() >= len(topics) }) {
			t.Fatalf("trial %d: the probe was handed %v, want one value per topic", trial, probe.Payloads())
		}
		current := map[string]string{}
		for _, m := range probe.All() {
			current[m.Topic] = m.Payload
		}

		for _, c := range rs {
			if !waitUntil(3*time.Second, func() bool {
				last := map[string]string{}
				for _, m := range c.All() {
					last[m.Topic] = m.Payload
				}
				for topic, v := range current {
					if last[topic] != v {
						return false
					}
				}
				return true
			}) {
				staleEnds++
				t.Errorf("trial %d: %s never settled on the store's value: holds %v, store %v",
					trial, c.ID, c.Payloads(), current)
			}
		}
		// A marker on each topic after everything: anything still on its way
		// to a reader, an older value included, arrives before its topic's.
		marker := connect(t, h, fmt.Sprintf("marker-%d", trial), true, false)
		for _, topic := range topics {
			marker.Pub(t, topic, "marker")
		}
		for _, c := range rs {
			if !waitUntil(5*time.Second, func() bool {
				n := 0
				for _, m := range c.All() {
					if m.Payload == "marker" {
						n++
					}
				}
				return n == len(topics)
			}) {
				t.Fatalf("trial %d: %s never received a marker on every topic: %v", trial, c.ID, c.Payloads())
			}
			for topic, offs := range offsetsOf(t, c.All()) {
				for i := 1; i < len(offs); i++ {
					if offs[i] < offs[i-1] {
						reversals++
						t.Errorf("trial %d: %s was sent offset %d after %d on %s", trial, c.ID,
							offs[i], offs[i-1], topic)
					}
				}
			}
		}
	}
	t.Logf("%d trials, %d publishers x %d values on %d topics, %d readers each: %d reversals, "+
		"%d readers left stale", trials, pubs, each, len(topics), readers, reversals, staleEnds)
}

// The same rule on a subscriber's own current state: the state a SUBSCRIBE is
// served is read, then merged into what the subscriber is waiting for, and a
// newer value published and written to it in between must not be followed by
// the older one the state still holds. The seam publishes v2 after the state
// holding v1 was read and waits until v2 has reached the reader; the merge
// that follows has to leave v1 out.
func TestAStateReadBeforeANewerValueWasSentDoesNotSendTheOlderOneAfterIt(t *testing.T) {
	h := start(t)
	p := connect(t, h, "producer", true, false)
	p.Pub(t, "state/x", "v1") // stored before its PUBACK

	r := connect(t, h, "reader", true, false)
	r.Sub(t, "state/other/#", 1) // a subscription already on the channel, so a record exists
	var mu sync.Mutex
	fired := false
	built := make(chan string, 1)
	restore := broker.SetLatestAfterState(func(id string) {
		if id != "reader" {
			return
		}
		mu.Lock()
		first := !fired
		fired = true
		mu.Unlock()
		if !first {
			return
		}
		done := make(chan error, 1)
		go func() {
			_, err := p.C.Publish(context.Background(), &paho.Publish{
				Topic: "state/x", QoS: 1, Payload: []byte("v2")})
			done <- err
		}()
		if err := <-done; err != nil {
			built <- "publishing v2 failed: " + err.Error()
			return
		}
		if !waitUntil(3*time.Second, func() bool {
			for _, m := range r.All() {
				if m.Payload == "v2" {
					return true
				}
			}
			return false
		}) {
			built <- "v2 never reached the reader live"
			return
		}
		built <- ""
	})
	t.Cleanup(restore)

	r.Sub(t, "state/x", 1) // its state, v1, is read here; v2 is sent before it is merged
	select {
	case why := <-built:
		if why != "" {
			t.Fatalf("the interleaving was not built: %s", why)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the seam never ran for the reader's SUBSCRIBE")
	}
	// The merge follows the seam, on the SUBSCRIBE's own read loop, so r's
	// next packet is answered only once it is done; a marker on the topic,
	// published after that, comes after anything it queued.
	settle(t, r)
	all := receivedThroughMarkers(t, r, p, "state/x")
	offs := offsetsOf(t, all)["state/x"]
	for i := 1; i < len(offs); i++ {
		if offs[i] < offs[i-1] {
			t.Errorf("the reader was sent offset %d after %d on state/x (%v): the state read before "+
				"v2 was sent was merged after it", offs[i], offs[i-1], r.Payloads())
		}
	}
	if len(all) == 0 || all[len(all)-1].Payload != "v2" {
		t.Errorf("the reader holds %v, and v2 is current", r.Payloads())
	}
}

// RFC 0003: expiry on a latest channel deletes the current value, and a
// stale reading is worse than none. A subscriber short of window holds values
// waiting for it, and one the channel's retention removes while it waits is
// not sent when its turn comes: b and c wait behind a's unacknowledged
// delivery past the channel's one-second period, and what follows a is d
// alone.
func TestALatestValueTheChannelExpiredWhileItWaitedIsNotSent(t *testing.T) {
	brokertest.LatestRetention = 1
	t.Cleanup(func() { brokertest.LatestRetention = 0 })
	h := start(t)
	sub := connectRx(t, h, "slow", true, true, 1)
	sub.Sub(t, "state/#", 1)
	// Its current state read before anything is published, for the reason
	// TestALatestValueTheChannelExpiredIsNotHeldWhileItWaits gives.
	settle(t, sub)
	pub := connect(t, h, "pub", true, false)

	pub.Pub(t, "state/a", "a1")
	first, ok := sub.Await(t, 3*time.Second)
	if !ok || first.Payload != "a1" {
		t.Fatalf("the first value was %+v (%v)", first, ok)
	}
	pub.Pub(t, "state/b", "b1")
	pub.Pub(t, "state/c", "c1")
	if !waitUntil(3*time.Second, func() bool { return broker.LatestPending(h.B, "slow", "state") == 2 }) {
		t.Fatalf("%d values wait for the subscriber, want b1 and c1", broker.LatestPending(h.B, "slow", "state"))
	}
	// **The clock is what is under test**, so this one wait stays: b1 and
	// c1 have to be older than the channel's period, and a sleep is never
	// shorter than asked for.
	time.Sleep(1300 * time.Millisecond) // past the channel's period
	pub.Pub(t, "state/d", "d1")
	first.Ack()

	next, ok := sub.Await(t, 3*time.Second)
	if !ok {
		t.Fatal("nothing followed the acknowledgement")
	}
	next.Ack()
	if next.Payload != "d1" {
		t.Errorf("after a1 the subscriber was sent %q, want d1: b1 and c1 passed the channel's period "+
			"while they waited", next.Payload)
	}
	// A stale b1 or c1 would arrive before its own topic's marker.
	if got := throughMarkers(t, sub, pub, "state/b", "state/c"); strings.Join(got, ",") != "a1,d1" {
		t.Errorf("the subscriber was sent %v before the markers, want [a1 d1]", got)
	}
}

// And they are not held while they wait: the list a subscriber short of
// window holds is looked through once it has doubled, so values the channel
// has expired go without waiting for the subscriber's turn.
func TestALatestValueTheChannelExpiredIsNotHeldWhileItWaits(t *testing.T) {
	brokertest.LatestRetention = 1
	t.Cleanup(func() { brokertest.LatestRetention = 0 })
	h := start(t)
	sub := connectRx(t, h, "slow", true, true, 1)
	sub.Sub(t, "state/#", 1)
	// The subscription's current state read before anything is published:
	// a value published meanwhile reaches it live and again as state
	// (TestALiveValueRacingASubscriptionsStateIsNeitherLostNorStale), and
	// that second copy waits on the list this test counts.
	settle(t, sub)
	pub := connect(t, h, "pub", true, false)

	pub.Pub(t, "state/first", "x")
	if _, ok := sub.Await(t, 3*time.Second); !ok {
		t.Fatal("the first value never arrived")
	}
	const n = 2047 // one short of the list doubling past its floor
	for i := range n {
		pub.PubPlainAt(t, fmt.Sprintf("state/t/%04d", i), "v", 1)
	}
	if !waitUntil(5*time.Second, func() bool { return broker.LatestPending(h.B, "slow", "state") == n }) {
		t.Fatalf("%d values wait, want %d", broker.LatestPending(h.B, "slow", "state"), n)
	}
	// The clock is what is under test: a sleep is never shorter than asked.
	time.Sleep(1300 * time.Millisecond) // past the channel's period

	// **Published until the list is looked through, rather than once.**
	// It is looked through when it has doubled since the drain last did,
	// and when that was is the drain's: one that first looked at 1,030
	// rather than 1,024 looks again at 2,060, so the one value this test
	// used to publish after the wait left it short, under load, about one
	// run in two hundred. Every value published now is fresh, so once the
	// list has been looked through it holds those and nothing else.
	after := 0
	for broker.LatestPending(h.B, "slow", "state") >= n && after < n {
		pub.PubPlainAt(t, fmt.Sprintf("state/t/last-%04d", after), "v", 1)
		after++
	}
	if !waitUntil(3*time.Second, func() bool { return broker.LatestPending(h.B, "slow", "state") == after }) {
		t.Errorf("%d values wait after %d passed the channel's period, want %d: the ones published "+
			"after", broker.LatestPending(h.B, "slow", "state"), n, after)
	}
}

// The retained store's values wait on the same list, held to the store's
// own period (broker.retained.retention_period): a retained value the store
// has expired while a subscriber was short of window is not sent to it.
func TestARetainedValueTheStoreExpiredWhileItWaitedIsNotSent(t *testing.T) {
	period := int64(1)
	brokertest.Retaining = &period
	t.Cleanup(func() { brokertest.Retaining = nil })
	h := start(t)
	pub := connect(t, h, "pub", true, false)
	for _, v := range []string{"a", "b", "c"} {
		pub.PubRetained(t, "kept/"+v, v+"1")
	}
	// A session that ends with its connection: a durable one is owed its
	// retained values from the broadcast log (keepRetained), as any message
	// it is owed, and they wait there rather than here.
	sub := brokertest.Dial(t, h, "slow", true, true, 1, 0, 0)
	sub.Sub(t, "kept/#", 1)
	first, ok := sub.Await(t, 3*time.Second)
	if !ok {
		t.Fatal("no retained value arrived")
	}
	if !waitUntil(3*time.Second, func() bool { return broker.LatestPending(h.B, "slow", broker.RetainedStoreName) == 2 }) {
		t.Fatalf("%d retained values wait, want 2", broker.LatestPending(h.B, "slow", broker.RetainedStoreName))
	}
	// The clock is what is under test: a sleep is never shorter than asked.
	time.Sleep(1300 * time.Millisecond) // past the store's period
	first.Ack()

	// **The drain finished, read from the broker rather than waited out.**
	// The acknowledgement wakes the subscriber's drain, which takes both
	// waiting values off its list and lets its claim go only once it has
	// nothing more to write; a value it sent is in the broker's in-flight
	// table by then, and stays, since this subscriber acknowledges by hand.
	idle := waitUntil(5*time.Second, func() bool {
		cl, ok := h.Srv.Clients.Get("slow")
		return ok && cl.State.Inflight.Len() == 0 &&
			strings.Contains(h.B.ConsumerHolding("slow"), "latest=0 draining=0 ")
	})
	if !idle {
		inflight := -1
		if cl, ok := h.Srv.Clients.Get("slow"); ok {
			inflight = cl.State.Inflight.Len()
		}
		t.Errorf("after %s the subscriber's drain ended holding %d in flight (%s), and it was "+
			"sent %v: a retained value the store's period had removed while it waited was sent",
			first.Payload, inflight, h.B.ConsumerHolding("slow"), sub.Payloads())
	}
}
