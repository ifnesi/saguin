package core_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the delivery admission gate driven inside: when the gate
// opens against its thresholds, and when a peer obligation falls due, are
// orderings no client can force (design.md, Testing: "A test inside the
// code checks only what neither can see. These checks cover [...]
// orderings no client can force"). They drive the core with scripted
// events, a scripted store and fake times, as positions_test.go does: a
// held store read is broker work that has not finished, and the script's
// clock is the core's event times. Each runs with config.md's defaults,
// delivery_admission_delay 100ms and delivery_peer_timeout 1s.

// admissionDelay and peerTimeout are config.md, broker:
// delivery_admission_delay, default 100ms, and delivery_peer_timeout,
// default 1s.
const (
	admissionDelay = 100 * time.Millisecond
	peerTimeout    = time.Second
)

// gatedSeries is metrics.md, Publishing: "saguin_delivery_admission_gated
// | gauge | 1 while the delivery admission gate is closed, otherwise 0".
const gatedSeries = "saguin_delivery_admission_gated"

// gate fails the test unless the gate is closed (want true) or open, as
// saguin_delivery_admission_gated says, the core having taken no event
// since its last decision.
func (s *positionScript) gate(closed bool, when string) {
	s.t.Helper()
	want := 0.0
	if closed {
		want = 1
	}
	if got := testkit.ParseScrape(s.t, testkit.Scrape(s.t, s.reg)).Value(s.t, gatedSeries); got != want {
		s.t.Fatalf("%s, %s = %v; want %v", when, gatedSeries, got, want)
	}
}

// heldMetadata is a script's holdRead for every read of channel's record
// metadata: that channel's consumers wait on the store until the test
// releases them.
func heldMetadata(channel int64) func(store.Read) bool {
	return func(r store.Read) bool {
		m, ok := r.(*store.ReadRecordMetadata)
		return ok && m.Channel == channel && !m.Retained
	}
}

// releaseUntil answers the held reads, oldest first, each eventGap after
// the last event, serving what each leads to, until done reports true,
// failing the test if the held reads run out first.
func (s *positionScript) releaseUntil(done func() bool, what string) {
	s.t.Helper()
	for !done() {
		if len(s.heldReads) == 0 {
			s.t.Fatalf("the store answered every held read, and still not %s", what)
		}
		s.releaseRead(s.heldReads[0])
	}
}

// receivedTopic reports whether a delivery of topic has been handed to
// conn's writer.
func (s *positionScript) receivedTopic(conn *connection.Conn, topic string) bool {
	for _, d := range s.deliveries {
		if d.conn == conn && d.topic == topic {
			return true
		}
	}
	return false
}

// cut reports whether the core has cut conn.
func (s *positionScript) cut(conn *connection.Conn) bool {
	for _, a := range s.acts {
		if a.Conn == conn && a.Cut {
			return true
		}
	}
	return false
}

// gateScript is a script whose core has a publisher, P, and a subscriber
// to gate/# on $broadcast, S, at qos, the store answering everything at
// once; S's later reads of $broadcast record metadata are held.
func gateScript(t *testing.T, cfg *config.Config, qos byte) (s *positionScript, p, sub *connection.Conn) {
	s = newPositionScriptOf(t, cfg, 0)
	p, sub = new(connection.Conn), new(connection.Conn)
	s.connect(p, &packet.Connect{Version: packet.V5, ClientID: "p"})
	s.connect(sub, &packet.Connect{Version: packet.V5, ClientID: "s"})
	s.subscribeAt(sub, 1, "gate/#", qos)
	s.holdRead = heldMetadata(1)
	return s, p, sub
}

// publishAt has p publish topic at QoS qos under packet identifier id, its
// body asked for at at, and returns when the store committed it.
func (s *positionScript) publishAt(p *connection.Conn, at time.Time, topic string, qos byte, id uint16) time.Time {
	s.t.Helper()
	s.advance(at.Add(-eventGap))
	s.publish(p, &packet.Publish{QoS: qos, PacketID: id, Topic: []byte(topic), Payload: []byte("x")})
	return s.committed
}

// askedBody is a PUBLISH body a reader asked for and the core withheld:
// the publication, and how many actions the script had kept before the ask.
type askedBody struct {
	pub  *packet.Publish
	from int
}

// askAt has p's reader ask for a body at at, failing the test unless the
// core withholds it, and returns the body asked for.
func (s *positionScript) askAt(p *connection.Conn, at time.Time, topic, when string) askedBody {
	s.t.Helper()
	pub := &packet.Publish{Topic: []byte(topic), Payload: []byte("x")}
	s.advance(at.Add(-eventGap))
	from := len(s.acts)
	if got := s.ask(p, pub); got != 0 {
		s.t.Fatalf("%s, a reader asking for a PUBLISH body was given %d bytes; want it withheld while the gate is closed", when, got)
	}
	return askedBody{pub, from}
}

// granted fails the test unless the core has given p's reader the body it
// asked for, b, since it asked (want true), or has given it nothing since
// (want false). A reader has at most one request (design.md, Message
// memory), so whatever it was given since the ask is b's grant.
func (s *positionScript) granted(p *connection.Conn, b askedBody, want bool, when string) {
	s.t.Helper()
	got := given(s.acts[b.from:], p)
	switch {
	case want && got != bodyOf(b.pub):
		s.t.Fatalf("%s, a withheld PUBLISH body of %d bytes was given %d bytes since its ask; want it given whole once the gate opens", when, bodyOf(b.pub), got)
	case !want && got != 0:
		s.t.Fatalf("%s, a withheld PUBLISH body was given %d bytes since its ask; want it still withheld while the gate is closed", when, got)
	}
}

// design.md, Execution, Capacities, Delivery admission: "The gate closes
// when the admission delay reaches delivery_admission_delay. Once closed,
// it opens when every contributing delay is below half that value, or no
// qualifying work remains. Between those thresholds its state is
// unchanged"; "A boundary's delay is elapsed broker-wait time from the
// core event making its first work available. [...] Waiting for broker
// work, including storage, counts"; "The core evaluates qualification and
// the gate after applying an event's results and before granting a
// publication body. It also evaluates them at the next admission or
// peer-obligation deadline, using its existing timer". Communication:
// "While closed, it withholds new publication body grants". Delivery,
// Admission progress: "a qualified consumer keeps a fixed committed
// boundary being serviced and, while it remains unfinished, one following
// boundary [...] Finishing the serviced boundary promotes the following
// boundary without restarting its delay"; "A boundary finishes when its
// eligible records have been handed to writers".
//
// S subscribes to gate/# at QoS 0, caught up and qualified. P publishes
// gate/1, committed at T0; S's read of it is held, broker work unfinished.
// P publishes gate/2, its body given while the delay is below the
// threshold. At T0+99ms the gate is open; at T0+100ms, with no event but
// the core's own timer, it is closed, and a body P asks for is withheld.
// The store then answers S's reads until gate/1 is handed to S's writer:
// gate/2 is the remaining work, made available at T1.
//
// Between: T1 is T0+30ms, so gate/2's delay is then between half the
// threshold and the threshold: the gate stays closed, P's body withheld.
// The store then answers until gate/2 is handed over: no qualifying work
// remains, and the gate opens, P's body given, though S and P sent
// nothing.
//
// Below half: T1 is T0+80ms, so gate/2's delay is then below half the
// threshold: the gate opens, P's body given, though gate/2 is not yet
// handed over and S and P sent nothing.
func TestAdmissionGateReopensWithoutPeerAction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		after   time.Duration // T1 - T0
		between bool          // gate/2's delay is between the thresholds once gate/1 is handed over
	}{
		{"between the thresholds, then no qualifying work", 30 * time.Millisecond, true},
		{"every delay below half", 80 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p, sub := gateScript(t, newConfig(t), 0)
			t0 := s.publishAt(p, s.now.Add(time.Second), "gate/1", 0, 0)
			t1 := s.publishAt(p, t0.Add(tc.after), "gate/2", 0, 0)
			s.advance(t0.Add(admissionDelay - time.Millisecond))
			s.gate(false, "1ms before gate/1's delay reaches delivery_admission_delay")
			s.advance(t0.Add(admissionDelay))
			s.gate(true, "when gate/1's delay reaches delivery_admission_delay")
			pub := s.askAt(p, t0.Add(admissionDelay+time.Millisecond), "gate/3", "with the gate closed")

			s.advance(t0.Add(admissionDelay + 5*time.Millisecond))
			s.releaseUntil(func() bool { return s.receivedTopic(sub, "gate/1") }, "gate/1 handed to S's writer")
			if s.receivedTopic(sub, "gate/2") {
				t.Fatalf("gate/2 was handed over with gate/1; want it left waiting on its held read")
			}
			left := s.now.Sub(t1)
			if (left >= admissionDelay/2) != tc.between || left >= admissionDelay {
				t.Fatalf("the test's own timing: gate/2's delay is %v once gate/1 is handed over", left)
			}
			if !tc.between {
				s.gate(false, "with gate/2's delay "+left.String()+", below half the threshold")
				s.granted(p, pub, true, "with gate/2's delay "+left.String()+", below half the threshold")
				return
			}
			s.gate(true, "with gate/2's delay "+left.String()+", between the thresholds")
			s.granted(p, pub, false, "with gate/2's delay "+left.String()+", between the thresholds")
			s.releaseUntil(func() bool { return s.receivedTopic(sub, "gate/2") }, "gate/2 handed to S's writer")
			s.gate(false, "once gate/2 is handed over, no qualifying work remaining")
			s.granted(p, pub, true, "once gate/2 is handed over, no qualifying work remaining")
		})
	}
}

// design.md, Delivery, Peer obligations: "an obligation is an outstanding
// PUBACK, PUBREC or PUBCOMP expected from the peer. Its deadline is
// delivery_peer_timeout after the core receives the successful write
// report for the packet requiring it [...]. Only the matching valid
// acknowledgement ends it. Another acknowledgement, or a duplicate, does
// not restart its deadline"; "An overdue obligation removes qualification
// at its deadline. It does not disconnect the peer". Admission
// qualification: "Qualification [...] ends on [...] an overdue peer
// obligation"; "No qualifying recipient means no qualifying work".
// Capacities, Delivery admission: "Due peer-obligation deadlines are
// applied before the gate thresholds"; the gate "opens when [...] no
// qualifying work remains".
//
// S subscribes to gate/# at QoS 1, its window 20. P publishes gate/1 and
// gate/2 at QoS 1, handed to S's writer, whose successful write reports
// the core receives at Tw and Tw+1ms. S acknowledges gate/2 at Tw+500ms,
// and again at Tw+600ms: another acknowledgement than gate/1's, then a
// duplicate. P publishes gate/3, committed at Tw+850ms, S's read of it held.
// At Tw+950ms the gate is closed, S's work 100ms old, and a body P asks
// for is withheld. At Tw+1s-1ms it is still closed, the body still
// withheld. At Tw+1s, gate/1's obligation overdue, S no longer qualifies and the gate opens with no
// event but the core's own timer: P's body given, and S still connected.
func TestOverdueAcknowledgementEndsQualification(t *testing.T) {
	s, p, sub := gateScript(t, newConfig(t), 1)
	s.holdRead = nil
	s.stuck = true
	s.publishAt(p, s.now.Add(time.Second), "gate/1", 1, 1)
	s.publishAt(p, s.now.Add(time.Millisecond), "gate/2", 1, 2)
	if !s.receivedTopic(sub, "gate/1") || !s.receivedTopic(sub, "gate/2") {
		t.Fatalf("S's deliveries %+v; want gate/1 and gate/2 handed to its writer", s.deliveries)
	}
	ids := map[string]uint16{}
	for _, d := range s.deliveries {
		ids[d.topic] = d.id
	}
	s.advance(s.now.Add(time.Second))
	written := s.now.Add(eventGap) // the first write report's event
	s.writeAll()
	s.stuck = false

	s.advance(written.Add(500*time.Millisecond - eventGap))
	s.ack(sub, ids["gate/2"])
	s.advance(written.Add(600*time.Millisecond - eventGap))
	s.ack(sub, ids["gate/2"])
	s.holdRead = heldMetadata(1)
	t0 := s.publishAt(p, written.Add(peerTimeout-150*time.Millisecond), "gate/3", 1, 3)
	if s.receivedTopic(sub, "gate/3") {
		t.Fatalf("gate/3 was handed to S's writer; want it waiting on its held read")
	}
	s.advance(t0.Add(admissionDelay))
	s.gate(true, "when S's work on gate/3 has waited delivery_admission_delay, S qualifying")
	pub := s.askAt(p, t0.Add(admissionDelay+time.Millisecond), "gate/4", "with the gate closed")

	s.advance(written.Add(peerTimeout - time.Millisecond))
	s.gate(true, "1ms before gate/1's acknowledgement is overdue")
	s.granted(p, pub, false, "1ms before gate/1's acknowledgement is overdue")
	s.advance(written.Add(peerTimeout))
	s.gate(false, "when gate/1's acknowledgement is overdue, S no longer qualifying")
	s.granted(p, pub, true, "when gate/1's acknowledgement is overdue")
	if s.cut(sub) {
		t.Errorf("the core cut S for its overdue acknowledgement; want the peer left connected")
	}
}

// design.md, Delivery, Peer obligations: an obligation's deadline "does
// not run while the core withholds a publication body grant on that same
// connection". Admission progress: "This rule also applies when one
// connection publishes and subscribes. Work requiring its next input
// cannot keep the gate closed while that input is behind a withheld
// publication body". Capacities, Delivery admission: "Once closed, it
// opens when [...] no qualifying work remains"; Admission qualification:
// "Qualification persists through broker-caused lag. It ends on [...] an
// overdue peer obligation".
//
// C subscribes to c/# at QoS 1 on $broadcast; S2 subscribes to a/# at QoS
// 0 on the append channel a. P publishes c/1 at QoS 1, handed to C's
// writer, its write report received at Tw; C does not acknowledge it.
// P publishes a/1, S2's read of it held: 100ms later the gate is closed.
// At Tw+250ms C's reader asks for a body, withheld. At Tw+850ms the store
// answers S2's read, S2 has a/1 and no work remains: the gate opens and
// C's body is given at Tg. C's obligation for c/1, withheld Tg-(Tw+250ms),
// now falls due at Tw+1s plus that. C publishes z/1, which no one
// receives. At Tw+1.2s P publishes c/2, C's read of it held: C still
// qualifies, so 100ms later the gate is closed. It stays closed 1ms before
// C's moved deadline and opens at it, C overdue.
func TestPeerDeadlineStopsWhileItsBodyIsWithheld(t *testing.T) {
	cfg, err := config.Parse([]byte(baseYAML + "channels:\n  a:\n    type: append\n    filter: \"a/#\"\n    start: tail\n"))
	if err != nil {
		t.Fatalf("the test's configuration: %v", err)
	}
	s := newPositionScriptOf(t, cfg, 0)
	p, c, s2 := new(connection.Conn), new(connection.Conn), new(connection.Conn)
	s.connect(p, &packet.Connect{Version: packet.V5, ClientID: "p"})
	s.connect(c, &packet.Connect{Version: packet.V5, ClientID: "c"})
	s.connect(s2, &packet.Connect{Version: packet.V5, ClientID: "s2"})
	s.subscribeAt(c, 1, "c/#", 1)
	s.subscribeAt(s2, 1, "a/#", 0)

	s.stuck = true
	s.publishAt(p, s.now.Add(time.Second), "c/1", 1, 1)
	if !s.receivedTopic(c, "c/1") {
		t.Fatalf("C's deliveries %+v; want c/1 handed to its writer", s.deliveries)
	}
	s.advance(s.now.Add(10 * time.Millisecond))
	written := s.now.Add(eventGap)
	s.writeAll()
	s.stuck = false

	s.holdRead = func(r store.Read) bool {
		m, ok := r.(*store.ReadRecordMetadata)
		return ok && m.Channel != 1 && !m.Retained
	}
	ta := s.publishAt(p, written.Add(100*time.Millisecond), "a/1", 0, 0)
	if len(s.heldReads) == 0 {
		t.Fatalf("after a/1, the store holds no read of channel a; want S2's")
	}
	s.advance(ta.Add(admissionDelay))
	s.gate(true, "when S2's work on a/1 has waited delivery_admission_delay")
	asked := written.Add(250 * time.Millisecond)
	z := s.askAt(c, asked, "z/1", "with the gate closed")

	s.advance(written.Add(850*time.Millisecond - eventGap))
	s.holdRead = nil
	s.releaseUntil(func() bool { return s.receivedTopic(s2, "a/1") }, "a/1 handed to S2's writer")
	s.gate(false, "once a/1 is handed over, no qualifying work remaining")
	s.granted(c, z, true, "once a/1 is handed over")
	due := written.Add(peerTimeout + s.grantedAt[c].Sub(asked))
	s.hand(c, z.pub)

	s.holdRead = heldMetadata(1)
	tb := s.publishAt(p, written.Add(1200*time.Millisecond), "c/2", 1, 2)
	if s.receivedTopic(c, "c/2") {
		t.Fatalf("c/2 was handed to C's writer; want it waiting on its held read")
	}
	s.advance(tb.Add(admissionDelay))
	s.gate(true, "100ms after c/2, C's deadline for c/1 having stopped while its body was withheld")
	s.advance(due.Add(-time.Millisecond))
	s.gate(true, "1ms before C's deadline for c/1, moved by the time its body was withheld")
	s.advance(due)
	s.gate(false, "at C's deadline for c/1, moved by the time its body was withheld")
}

// routedChannel returns the id of the configured channel into which the
// store committed topic, 0 while it holds none.
func (s *positionScript) routedChannel(topic string) int64 {
	for id, records := range s.routed {
		for _, rec := range records {
			if rec.Topic == topic {
				return id
			}
		}
	}
	return 0
}

// topicAt returns the topic of the record the store committed at offset
// in channel, empty while it holds none.
func (s *positionScript) topicAt(channel, offset int64) string {
	rec := s.routed[channel][offset]
	if channel == 1 {
		rec = s.records[offset]
	}
	if rec == nil {
		return ""
	}
	return rec.Topic
}

// handedBefore reports whether firstConn's writer was handed a delivery of
// first, and then secondConn's one of second.
func (s *positionScript) handedBefore(firstConn *connection.Conn, first string, secondConn *connection.Conn, second string) bool {
	at := slices.IndexFunc(s.deliveries, func(d sent) bool { return d.conn == firstConn && d.topic == first })
	return at >= 0 && slices.ContainsFunc(s.deliveries[at:], func(d sent) bool { return d.conn == secondConn && d.topic == second })
}

// design.md, Delivery, Admission progress: "Work contributes delay only
// while it can advance without a peer action. Waiting for send capacity, an
// acknowledgement, socket drain or an application response never
// contributes. Neither does waiting for a resource whose release requires
// such an action. [...] Preparations beyond available send capacity do not
// contribute"; "This rule also applies when one connection publishes and
// subscribes. Work requiring its next input cannot keep the gate closed
// while that input is behind a withheld publication body". Preparation: "A
// selected delivery reserves message memory only when send capacity is free
// for it. It is handed to the writer only within the window". One
// mechanism: the window is "the lower of the client's Receive Maximum and
// limits.max_inflight". Order: "Channels supplying one connection are
// visited round-robin by channel id, with one delivery per visit".
// Capacities, Delivery admission: once closed, the gate "opens when every
// contributing delay is below half that value, or no qualifying work
// remains".
//
// Preparation also says: "Where deliveries must be handed to a
// connection's writer in a given order, their message-memory requests enter
// the outgoing FIFO in that order. A later delivery never reserves send
// capacity ahead of a delivery it must follow."
//
// S is durable, its window 4 (Receive Maximum 4), and subscribes at QoS 1
// to x/# on $broadcast and to a/# on the append channel a; R subscribes to
// r/# on the append channel r. Commits collect for publish_commit_interval.
// P publishes x/1 and a/0, both handed to S's writer. P publishes x/3, x/4
// and a/1, committed together at Tb, so S selects x/3, a/1 and x/4 in that
// order: x/3 and a/1 take the window's last two places, in that order, and
// are handed to S's writer. x/4 is prepared but holds no message memory and
// its payload is never read: it waits for send capacity that only S's
// PUBACK frees.
//
// No other work: at Tb+150ms the gate is open. Other work: P publishes r/1,
// R's read held; 100ms after its commit the gate is closed, and a body S
// asks for is withheld. The store answers R's read, r/1 is handed over: the
// gate opens and S's body is given, though no client sent anything. S hands
// its publication and acknowledges x/1: x/4 is handed to its writer.
func TestGateNeverWaitsOnACapacityBlockedConnection(t *testing.T) {
	cfg, err := config.Parse([]byte(baseYAML + "channels:\n" +
		"  a:\n    type: append\n    filter: \"a/#\"\n    start: tail\n" +
		"  r:\n    type: append\n    filter: \"r/#\"\n    start: tail\n"))
	if err != nil {
		t.Fatalf("the test's configuration: %v", err)
	}
	cfg.Broker.Storage.PublishCommitInterval = collection
	s := newPositionScriptOf(t, cfg, 0)
	p, sub, r := new(connection.Conn), new(connection.Conn), new(connection.Conn)
	s.connect(p, &packet.Connect{Version: packet.V5, ClientID: "p"})
	s.connect(sub, &packet.Connect{Version: packet.V5, ClientID: "s", SessionExpiryInterval: 3600, ReceiveMaximum: 4})
	s.connect(r, &packet.Connect{Version: packet.V5, ClientID: "r"})
	s.subscribeAt(sub, 1, "x/#", 1)
	s.subscribeAt(sub, 2, "a/#", 1)
	s.subscribeAt(r, 1, "r/#", 0)
	publish := func(topic string, qos byte, id uint16) {
		s.publish(p, &packet.Publish{QoS: qos, PacketID: id, Topic: []byte(topic), Payload: []byte("x")})
	}

	publish("x/1", 1, 1)
	publish("a/0", 1, 2)
	s.advance(s.now.Add(collection))
	if !s.handedBefore(sub, "x/1", sub, "a/0") {
		t.Fatalf("S's deliveries %+v; want x/1, then a/0, handed to its writer", s.deliveries)
	}

	loaded := map[string]bool{} // the payloads the store has read, by topic
	s.holdRead = func(op store.Read) bool {
		for _, read := range core.PayloadsOf(op) {
			loaded[s.topicAt(read.Channel, read.Offset)] = true
		}
		return false
	}
	publish("x/3", 1, 3)
	publish("x/4", 1, 4)
	publish("a/1", 1, 5)
	s.advance(s.now.Add(collection))
	tb := s.committed
	switch {
	case !s.handedBefore(sub, "x/3", sub, "a/1") || s.receivedTopic(sub, "x/4"):
		t.Fatalf("S's deliveries %+v; want x/3, then a/1, handed to its writer, filling its window, and x/4 not", s.deliveries)
	case loaded["x/4"]:
		t.Fatalf("the store read the payloads of %v; want x/4's not read, no send capacity being free for it", loaded)
	case len(s.pending) != 0 || len(s.heldReads) != 0 || len(s.held) != 0:
		t.Fatalf("the store still has %d requests, %d held reads and %d held groups; want S's next delivery waiting only for its PUBACK", len(s.pending), len(s.heldReads), len(s.held))
	}

	s.advance(tb.Add(admissionDelay * 3 / 2))
	s.gate(false, "150ms after x/4 commits, waiting for send capacity only S's PUBACK frees")

	s.holdRead = func(read store.Read) bool {
		m, ok := read.(*store.ReadRecordMetadata)
		return ok && m.Channel == s.routedChannel("r/1") && !m.Retained
	}
	publish("r/1", 0, 0)
	s.advance(s.now.Add(collection))
	tr := s.committed
	if len(s.heldReads) == 0 {
		t.Fatalf("after r/1, the store holds no read of channel r; want R's")
	}
	s.advance(tr.Add(admissionDelay))
	s.gate(true, "when R's work on r/1 has waited delivery_admission_delay")
	body := s.askAt(sub, tr.Add(admissionDelay+time.Millisecond), "s/1", "with the gate closed on R's work")

	s.holdRead = nil
	s.releaseUntil(func() bool { return s.receivedTopic(r, "r/1") }, "r/1 handed to R's writer")
	s.gate(false, "once r/1 is handed over, S's remaining work waiting for its own PUBACK")
	s.granted(sub, body, true, "once r/1 is handed over, S's remaining work waiting for its own PUBACK")

	s.hand(sub, body.pub)
	s.ack(sub, 1)
	if !s.receivedTopic(sub, "x/4") {
		t.Fatalf("after S acknowledges x/1, its deliveries %+v; want x/4 handed to its writer", s.deliveries)
	}
	s.gate(false, "once S's deliveries are handed over")
}

// design.md, Execution, Capacities, Delivery admission: the gate "starts
// open, remains open when disabled"; config.md, broker,
// delivery_admission_delay: "none disables the pause"; Testing: "The busy
// path is anything run per packet, per message or per connection. It does
// no needless work [...] avoids every copy or allocation it can".
//
// With delivery_admission_delay none, a publisher, P, and a subscriber, S,
// holding a durable session with Receive Maximum 2, subscribed to gate/#
// at QoS 1: P publishes four records at QoS 1, each handed to S's writer
// and acknowledged. Through the connections, the subscription and every
// publish, delivery and acknowledgement, the gate keeps nothing: no
// owner's age, no peer deadline, nor room for one (core.AdmissionRoom);
// and it stays open.
func TestDisabledGateKeepsNothing(t *testing.T) {
	cfg, err := config.Parse([]byte(baseYAML + "  delivery_admission_delay: none\n"))
	if err != nil {
		t.Fatalf("the test's configuration: %v", err)
	}
	s := newPositionScriptOf(t, cfg, 0)
	keepsNothing := func(when string) {
		t.Helper()
		if n := s.core.AdmissionRoom(); n != 0 {
			t.Fatalf("%s, the disabled gate keeps room for %d ages and deadlines; want none", when, n)
		}
	}
	p, sub := new(connection.Conn), new(connection.Conn)
	s.connect(p, &packet.Connect{Version: packet.V5, ClientID: "p"})
	s.connect(sub, &packet.Connect{Version: packet.V5, ClientID: "s", SessionExpiryInterval: 3600, ReceiveMaximum: 2})
	keepsNothing("once P and S are connected")
	s.subscribeAt(sub, 1, "gate/#", 1)
	keepsNothing("once S has subscribed")
	for i := range 4 {
		topic := fmt.Sprintf("gate/%d", i+1)
		s.publish(p, &packet.Publish{QoS: 1, PacketID: uint16(i + 1), Topic: []byte(topic), Payload: []byte("x")})
		keepsNothing("once P has published " + topic)
		d := s.deliveries[len(s.deliveries)-1]
		if d.conn != sub || d.topic != topic {
			t.Fatalf("after P published %s, the last delivery handed to a writer is %+v; want %s, to S", topic, d, topic)
		}
		s.ack(sub, d.id)
		keepsNothing("once S has acknowledged " + topic)
	}
	s.gate(false, "with the gate disabled, after four publishes")
}

// design.md, Delivery, Admission progress: "For the oldest delivery waiting
// for message memory, the core counts the outgoing reservations that cannot
// be released until peer input arrives or a writer finishes (Message
// memory). A delivery's stored-copy reservation is held for the store
// reader, and is never counted among them. When the delivery's reservation
// does not fit in the outgoing budget left after that total, the delivery
// is waiting for a peer. The core makes this judgment independently of
// whether the gate is open or closed"; "Example: with a 10 KiB outgoing
// budget, deliveries awaiting peer input hold 4 KiB and a compressed
// delivery's stored copy holds 4 KiB. A delivery needing 4 KiB does not fit
// now, but it is not waiting for a peer: the stored copy goes back once the
// store reader has decompressed it, and 6 KiB is then free"; "Work
// contributes delay only while it can advance without a peer action";
// Admission qualification: a consumer's "initial pass and recovered sends
// must have finished". Message memory, Two queues: "A delivery of a
// compressed payload also reserves the stored copy's size. That reservation
// is given back once the store reader has decompressed it". Capacities,
// Delivery admission: "The gate closes when the admission delay reaches
// delivery_admission_delay".
//
// With max_outgoing_memory 10,500 and every writer stuck, reporting
// nothing: P stores c/r, retained, a payload of 4,900 stored as a copy of
// 4,100 bytes. F subscribes to f/# at QoS 0, and P publishes f/1, about
// 1,000 bytes delivered: F's writer holds it, a reservation only its
// finishing releases. C subscribes to c/# at QoS 0, and its initial pass,
// which keeps it unqualified, reserves c/r's delivery, about 4,900 bytes,
// and its stored copy; the store holds that payload's read. P publishes
// f/2, about 3,000 bytes delivered: F's delivery of it does not fit, and it
// waits for message memory, the oldest delivery waiting. Beside the
// reservations awaiting a writer, about 5,900 bytes, it fits: its wait is
// not for a peer. At delivery_admission_delay after f/2's commit the gate
// is closed. The store answers c/r's read: f/2 is handed to F's writer, and
// the gate is open.
func TestStoredCopyIsNeverHeldForAPeer(t *testing.T) {
	cfg := newConfig(t)
	cfg.Broker.Limits.MaxOutgoingMemory = 10500
	s := newPositionScriptOf(t, cfg, 0)
	p, f, c := new(connection.Conn), new(connection.Conn), new(connection.Conn)
	s.connect(p, &packet.Connect{Version: packet.V5, ClientID: "p"})
	s.connect(f, &packet.Connect{Version: packet.V5, ClientID: "f"})
	s.connect(c, &packet.Connect{Version: packet.V5, ClientID: "c"})
	s.publish(p, &packet.Publish{Retain: true, Topic: []byte("c/r"), Payload: make([]byte, 4900), StoredPayload: make([]byte, 4100)})
	s.subscribe(f, 1, "f/#")
	s.stuck = true
	s.publish(p, &packet.Publish{Topic: []byte("f/1"), Payload: make([]byte, 990)})
	if !s.receivedTopic(f, "f/1") {
		t.Fatal("f/1 was not handed to F's writer")
	}
	s.holdRead = func(r store.Read) bool {
		return slices.ContainsFunc(core.PayloadsOf(r), func(p *store.ReadRecordPayload) bool { return s.topicAt(p.Channel, p.Offset) == "c/r" })
	}
	s.subscribe(c, 1, "c/#")
	if len(s.heldReads) != 1 {
		t.Fatalf("C's initial pass led to %d held payload reads of c/r; want one, its delivery and stored copy reserved", len(s.heldReads))
	}
	s.publish(p, &packet.Publish{Topic: []byte("f/2"), Payload: make([]byte, 2990)})
	committed := s.committed
	if s.receivedTopic(f, "f/2") {
		t.Fatal("f/2 was handed to F's writer beside the reservations held; the test needs it waiting for message memory")
	}
	s.advance(committed.Add(admissionDelay))
	s.gate(true, "with f/2's delivery waiting only for c/r's stored copy, delivery_admission_delay after f/2's commit")
	s.releaseRead(s.heldReads[0])
	if !s.receivedTopic(f, "f/2") {
		t.Fatal("c/r's read answered, its stored copy given back, f/2 was not handed to F's writer")
	}
	s.gate(false, "f/2 handed to F's writer, no qualifying work waiting")
}
