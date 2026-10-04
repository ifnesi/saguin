package broker_test

// The one window pumpBatch leaves open: after the records are read and
// judged outside b.mu, before the commit takes it again. Each test does the
// interfering thing from inside that window, through the test seam, so the
// interleaving is certain rather than likely - see pumpBetweenReadAndCommit.
//
// Every test counts that the seam fired for its client. A pump path that
// stopped passing through it would otherwise leave these passing while
// exercising nothing.

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// untilQuiet gathers what a client is sent until nothing more arrives for d,
// so that anything sent twice or past the window is in what it returns.
func untilQuiet(t *testing.T, c *client, d time.Duration) []received {
	t.Helper()
	var got []received
	for {
		r, ok := c.Await(t, d)
		if !ok {
			return got
		}
		got = append(got, r)
	}
}

// RFC 0003 "`append` - Durable consumers", docs/invariants.md 1
//
// A filter added while a batch is between its read and its commit is
// matched against that batch. The batch was judged against the filters
// before it, and the consumer's one cursor into the channel steps past every
// record the batch holds - so a record only the new filter matches would be
// stepped over, never sent, and the consumer would report success over it.
func TestAFilterAddedMidBatchIsServedWhatItMatches(t *testing.T) {
	h := start(t)
	c := connect(t, h, "mid-sub", true, false)
	c.Sub(t, "events/a/#", 1)
	// **The subscription's own replay has finished before b1 exists.** It
	// runs on the server after the SUBACK is written (OnPacketSent), so the
	// SUBACK reaching c says nothing about it: run late, it read b1 against
	// events/a/# alone and stepped over it before events/b/# was asked for,
	// which RFC 0003 says a later filter does not reach back past - or met
	// the seam itself, on the read loop the SUBSCRIBE below needs. c's next
	// packet is read only after that replay returns, so its answer is the
	// proof.
	settle(t, c)
	p := connect(t, h, "mid-sub-pub", true, false)
	// Matches nothing c holds, so c is not woken and its cursor stays below it.
	p.Pub(t, "events/b/1", "b1")

	var fired atomic.Int32
	subErr := make(chan error, 1)
	restore := broker.SetPumpBetweenReadAndCommit(func(id string) {
		if id != "mid-sub" || fired.Add(1) != 1 {
			return
		}
		_, err := c.C.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: "events/b/#", QoS: 1}},
		})
		subErr <- err
	})
	defer restore()

	// Wakes c; its batch reads b1 and a2 and judges b1 against events/a/#.
	p.Pub(t, "events/a/2", "a2")
	got := untilQuiet(t, c, 2*time.Second)

	if fired.Load() == 0 {
		t.Fatal("the seam never fired for mid-sub, so no batch was interrupted and this proves nothing")
	}
	if err := <-subErr; err != nil {
		t.Fatalf("the subscribe made inside the window failed: %v", err)
	}
	seen := map[string]int{}
	for _, r := range got {
		seen[r.Payload]++
	}
	if seen["b1"] != 1 || seen["a2"] != 1 {
		t.Fatalf("sent %v, want b1 and a2 once each: b1 was read in a batch judged before "+
			"events/b/# existed, and the cursor stepped over it", payloads(got))
	}
}

// RFC 0003 "Seeking", docs/invariants.md 1 and 2
//
// A seek landing between a batch's read and its commit wins: the batch is
// thrown away and the consumer is served from where it asked to be, each
// record once. Committed instead, the batch would register records from
// the old position under the new one.
func TestASeekMidBatchServesFromTheSoughtPositionOnce(t *testing.T) {
	h := start(t)
	c := connect(t, h, "mid-seek", true, false)
	c.Sub(t, "events/#", 1)
	p := connect(t, h, "mid-seek-pub", true, false)
	p.Pub(t, "events/x", "r1")
	first := untilQuiet(t, c, time.Second)
	if len(first) != 1 || first[0].Payload != "r1" {
		t.Fatalf("before the seek, sent %v, want r1", payloads(first))
	}
	r1, err := strconv.ParseUint(first[0].User["saguin-offset"], 10, 64)
	if err != nil {
		t.Fatalf("r1 carried no offset: %v", first[0].User)
	}

	var fired atomic.Int32
	seekCode := make(chan byte, 1)
	restore := broker.SetPumpBetweenReadAndCommit(func(id string) {
		if id != "mid-seek" || fired.Add(1) != 1 {
			return
		}
		ack, err := c.C.Publish(context.Background(), &paho.Publish{
			Topic: "$saguin/consumer/events/seek", QoS: 1,
			Payload: []byte(strconv.FormatUint(r1, 10)),
		})
		if err != nil || ack == nil {
			seekCode <- 0xFF
			return
		}
		seekCode <- ack.ReasonCode
	})
	defer restore()

	p.Pub(t, "events/x", "r2")
	got := untilQuiet(t, c, 2*time.Second)

	if fired.Load() == 0 {
		t.Fatal("the seam never fired for mid-seek, so no batch was interrupted and this proves nothing")
	}
	if code := <-seekCode; code != 0 {
		t.Fatalf("the seek made inside the window was answered 0x%02x", code)
	}
	if ps := payloads(got); len(ps) != 2 || ps[0] != "r1" || ps[1] != "r2" {
		t.Fatalf("after a seek to r1 mid-batch, sent %v, want [r1 r2]: each record once, "+
			"from the sought position, in order", ps)
	}
}

// MQTT-3.3.4-9, RFC 0003 "`append`"
//
// The in-flight window shrinking between a batch's read and its commit
// shrinks the batch. The window is shared with every other delivery to the
// session, and a broadcast arriving in that gap takes a slot the batch was
// planned against; registering the whole batch anyway sends the client more
// unacknowledged PUBLISH packets than the Receive Maximum it declared.
func TestTheWindowShrinkingMidBatchShrinksTheBatch(t *testing.T) {
	h := start(t)
	c := connectRx(t, h, "mid-room", true, true, 2) // manual acks: nothing frees the window
	c.Sub(t, "bcast/#", 1)
	p := connect(t, h, "mid-room-pub", true, false)
	// Two records waiting before c reads the channel at all, so its first
	// batch plans for both: the window is 2 and nothing is in flight.
	p.Pub(t, "events/x", "e1")
	p.Pub(t, "events/x", "e2")

	var fired atomic.Int32
	pubErr := make(chan error, 1)
	restore := broker.SetPumpBetweenReadAndCommit(func(id string) {
		if id != "mid-room" || fired.Add(1) != 1 {
			return
		}
		_, err := p.C.Publish(context.Background(), &paho.Publish{
			Topic: "bcast/1", QoS: 1, Payload: []byte("b1"),
		})
		// Held until the broadcast holds a slot in c's window, so the commit
		// really does find it there. Its arrival cannot be waited for: this
		// catch-up pump runs inside the write of c's SUBACK, and the
		// broadcast's write to c queues behind it.
		cl, ok := h.Srv.Clients.Get("mid-room")
		for deadline := time.Now().Add(2 * time.Second); err == nil && ok &&
			cl.State.Inflight.Len() == 0 && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
		if err == nil && (!ok || cl.State.Inflight.Len() == 0) {
			t.Errorf("the broadcast published inside the window never took a slot in mid-room's")
		}
		pubErr <- err
	})
	defer restore()

	c.Sub(t, "events/#", 1)
	got := untilQuiet(t, c, time.Second)

	if fired.Load() == 0 {
		t.Fatal("the seam never fired for mid-room, so no batch was interrupted and this proves nothing")
	}
	if err := <-pubErr; err != nil {
		t.Fatalf("the broadcast published inside the window failed: %v", err)
	}
	// Nothing is acknowledged, so everything sent is in flight at once and
	// must fit the window of 2: the broadcast and one record.
	if ps := payloads(got); len(ps) != 2 || !slices.Contains(ps, "b1") || !slices.Contains(ps, "e1") {
		t.Fatalf("with a Receive Maximum of 2 and nothing acknowledged, sent %v, want b1 and e1: "+
			"the batch planned for two slots registered both after the broadcast took one", ps)
	}
}

// docs/invariants.md 1
//
// A record stored before another but announced after it is still sent,
// and first. Two publishers' records are stored in one order under the
// log's lock and announced by pumpAll under b.mu, separately, so the later
// one can be announced while the earlier one is not. A consumer woken by
// the later one finds only it on its pending list; stepping over everything
// else up to it would step over the earlier record, which it matches and
// has not been sent. The watermark is what says the earlier one has not
// been announced yet.
func TestARecordAnnouncedLateIsNotSteppedOver(t *testing.T) {
	h := start(t)
	c := connect(t, h, "race-reader", true, false)
	c.Sub(t, "events/#", 1)
	early := connect(t, h, "race-early", true, false)
	late := connect(t, h, "race-late", true, false)

	var fired atomic.Int32
	firstSeen := make(chan string, 1)
	restore := broker.SetStoredBeforeAnnounced(func(ch string, _ uint64) {
		if ch != "events" || fired.Add(1) != 1 {
			return
		}
		// The early record is stored and not yet announced. The late one is
		// stored and announced inline, which wakes the reader.
		if _, err := late.C.Publish(context.Background(), &paho.Publish{
			Topic: "events/late", QoS: 1, Payload: []byte("late"),
		}); err != nil {
			firstSeen <- "publish failed: " + err.Error()
			return
		}
		r, ok := c.Await(t, 2*time.Second)
		if !ok {
			firstSeen <- "nothing"
			return
		}
		firstSeen <- r.Payload
	})
	defer restore()

	early.Pub(t, "events/early", "early")
	rest := untilQuiet(t, c, 2*time.Second)

	if fired.Load() == 0 {
		t.Fatal("the seam never fired, so no record was held between storing and announcing")
	}
	first := <-firstSeen
	got := append([]string{first}, payloads(rest)...)
	if len(got) != 2 || got[0] != "early" || got[1] != "late" {
		t.Fatalf("sent %v, want [early late]: the record stored first and announced second "+
			"was stepped over by a consumer woken for the other", got)
	}
}

// RFC 0003 "Moving a consumer's position: seek": the records from the new
// position start arriving on the connection that asked for them, and the
// reply naming the stored offset comes first.
//
// The seek is held after it has moved the cursor and before its reply is
// written (seekBeforeReply), and another client publishes from inside that
// hold, which starts a drain for the seeking consumer on a goroutine of its
// own. Served from the moved cursor there, the records reached the consumer
// ahead of the reply - r1 to r4, then the reply, in 2 of 3 runs before this
// held the cursor. Now nothing arrives during the hold, the reply is the
// first thing after the seek, and the records follow once each, in order.
//
// **Repeated**, because whether the drain gets as far as a write inside the
// hold is the scheduler's: five trials, each on its own consumer.
func TestASeeksReplyComesBeforeTheRecordsItMovesTo(t *testing.T) {
	h := start(t)
	p := connect(t, h, "seek-reply-pub", true, false)
	for _, r := range []string{"r1", "r2", "r3"} {
		p.Pub(t, "events/x", r)
	}
	var held atomic.Value // the consumer being held, by id
	var fired atomic.Int32
	var during atomic.Pointer[[]received]
	restore := broker.SetSeekBeforeReply(func(id string) {
		c, _ := held.Load().(*client)
		if c == nil || id != c.C.ClientID() {
			return
		}
		fired.Add(1)
		p.Pub(t, "events/x", "during-"+id)
		// Whatever the drain that publish started writes while the seek
		// is still held.
		got := untilQuiet(t, c, 300*time.Millisecond)
		during.Store(&got)
	})
	defer restore()

	for trial := 1; trial <= 5; trial++ {
		id := "seek-reply-" + strconv.Itoa(trial)
		c := connect(t, h, id, true, false)
		c.Sub(t, "events/#", 1)
		before := untilQuiet(t, c, 500*time.Millisecond)
		if len(before) < 3 {
			t.Fatalf("trial %d: before the seek, sent %v, want every record so far", trial, payloads(before))
		}
		held.Store(c)
		during.Store(nil)
		if _, err := c.C.Publish(context.Background(), &paho.Publish{
			Topic: "$saguin/consumer/events/seek", QoS: 0, Payload: []byte("1"),
			Properties: &paho.PublishProperties{ResponseTopic: "reply/" + id},
		}); err != nil {
			t.Fatalf("trial %d: seek: %v", trial, err)
		}
		after := untilQuiet(t, c, time.Second)
		if int(fired.Load()) != trial {
			t.Fatalf("trial %d: the seam fired %d times, so the seek was not held", trial, fired.Load())
		}
		var got []received
		if d := during.Load(); d != nil {
			got = append(got, *d...)
		}
		got = append(got, after...)
		if len(got) == 0 || got[0].Topic != "reply/"+id {
			t.Fatalf("trial %d: after the seek the consumer was sent %v first, want the reply "+
				"naming its position: %v", trial, first(got), payloads(got))
		}
		if got[0].Payload != "1" {
			t.Errorf("trial %d: the reply named %q, want 1", trial, got[0].Payload)
		}
		var offs []string
		for _, r := range got[1:] {
			offs = append(offs, r.User["saguin-offset"])
		}
		for i, off := range offs {
			if off != strconv.Itoa(i+1) {
				t.Fatalf("trial %d: after the reply the offsets were %v, want 1 onwards, once each, "+
					"in order", trial, offs)
			}
		}
		if len(offs) < 4+trial-1 {
			t.Errorf("trial %d: after the reply %d records arrived (%v), want every record from 1",
				trial, len(offs), offs)
		}
		held.Store((*client)(nil))
		c.Close()
	}
}

// first is the topic and payload of the first of what a client was sent.
func first(got []received) string {
	if len(got) == 0 {
		return "nothing"
	}
	return got[0].Topic + "=" + got[0].Payload
}

// RFC 0003 "`append` - Durable consumers": a session resumed by a takeover is
// sent its backlog without waiting for another record.
//
// A record published while the takeover is part-way through wakes a drain
// on the old connection, which finds it taken over with no successor
// registered yet and stops. If the successor registers and its resume is
// refused the claim in the moment between that finding and the stop, the
// resume's wake is the one nothing repeats: the stop must read it again,
// or the record waits for the next one. The takeover is held after the old
// connection is marked taken over and before the successor is registered,
// and let finish from inside the drain's window, through the test seam.
func TestAResumeRefusedWhileADrainStopsIsNotLost(t *testing.T) {
	// The takeover is held at the engine's own line for it, written after
	// the old connection is marked taken over and before the successor is
	// registered, with no lock of the session's but the id's held.
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var holdOnce, letOnce sync.Once
	let := func() { letOnce.Do(func() { close(release) }) }
	t.Cleanup(let)
	h := startLoggingDebug(t, func(line string) {
		if armed.Load() && strings.Contains(line, "session taken over") &&
			strings.Contains(line, "client=stopping") {
			holdOnce.Do(func() {
				close(entered)
				<-release
			})
		}
	})
	c := dial(t, h, "stopping", false, false, 0, 3600, 0)
	c.Sub(t, "events/#", 1)
	p := connect(t, h, "stopping-pub", true, false)

	resumed := &signalsResumed{client: "stopping", done: make(chan struct{})}
	if err := h.Srv.AddHook(resumed, nil); err != nil {
		t.Fatal(err)
	}

	var fired atomic.Int32
	restore := broker.SetPumpBeforeTakeoverStop(func(id string) {
		if id != "stopping" || fired.Add(1) != 1 {
			return
		}
		let()
		select {
		case <-resumed.done:
		case <-time.After(3 * time.Second):
		}
	})
	defer restore()

	armed.Store(true)
	// Not dial: dial waits for the successor to be registered, and this
	// test holds the takeover exactly before that.
	next := dialAt(t, h.Addr, h.Network, "stopping", false, false, 0, 3600, 0, nil, nil)
	next.H, next.ID = h, "stopping"
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the takeover never reached its line, so this proves nothing")
	}
	p.Pub(t, "events/x", "through")

	r, got := next.Await(t, 2*time.Second)
	if fired.Load() == 0 {
		t.Fatal("the seam never fired for stopping, so no drain stopped inside the takeover and this proves nothing")
	}
	select {
	case <-resumed.done:
	default:
		t.Fatal("the successor's resume never ran, so this proves nothing")
	}
	if !got || r.Payload != "through" {
		t.Errorf("the resumed session was sent %q (%v), want the record published during its takeover: "+
			"the resume's wake was dropped when the old connection's drain stopped", r.Payload, got)
	}
}

// signalsResumed says when a session under client has been established,
// after the broker's own hook has resumed it. Added after the first
// connection under client, so the one it answers is the successor.
type signalsResumed struct {
	mqtt.HookBase
	client string
	done   chan struct{}
	once   sync.Once
}

func (h *signalsResumed) ID() string           { return "signals-resumed" }
func (h *signalsResumed) Provides(b byte) bool { return b == mqtt.OnSessionEstablished }
func (h *signalsResumed) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	if cl.ID == h.client {
		h.once.Do(func() { close(h.done) })
	}
}
