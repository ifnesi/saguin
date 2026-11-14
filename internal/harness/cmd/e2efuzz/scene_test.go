package main

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// A scene drives the runner's own actions, connections, subscriptions,
// publications, acknowledgements, takeovers, record checks and
// checkpoints, in a fixed order against a fake broker given one fault, so
// that its situation is reached whatever the machine's load: each step
// waits for what it needs (a PINGRESP after the fake has queued a
// delivery, the broker's close after a connection's end was taken), never
// for a timer or a random walk to happen to come. A planted fault's scene
// is run twice: with its fault, it fails naming the rule broken; without,
// it passes, so the oracle raises no false alarm on the same steps. The
// runner's own checks are the oracles, as in a run.
type scene struct {
	f *fuzzer
	b *fake // scene controls, protected by the fake's mutex
	n int   // the publications made so far, each its own topic or tag
}

// caughtInScene runs script against a fake broker given f, the queue set
// as q, and checks it fails naming want; then against one without the
// fault, and checks it passes.
func caughtInScene(t *testing.T, f fault, q queueConfig, want string, script func(*scene) error) {
	t.Helper()
	for _, planted := range []fault{f, ""} {
		err := playScene(t, planted, q, script)
		switch {
		case planted != "" && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("%s: %v; want an error naming %q", planted, err, want)
		case planted == "" && err != nil:
			t.Errorf("without %s: %v; want none", f, err)
		}
	}
}

// playScene runs script against a fake broker given f, the queue set as q
// for the scene's while (theQueue, which the fake reads as it makes its
// queue), and returns the first error.
func playScene(t *testing.T, f fault, q queueConfig, script func(*scene) error) error {
	t.Helper()
	was := theQueue
	theQueue = q
	defer func() { theQueue = was }()
	b := fakeBroker(t, f)
	fz := newFuzzer(b.addr, b.dbPath, 1, 4, harness.DefaultTimeout, nil)
	defer func() {
		fz.closeAll()
		if fz.db != nil {
			_ = fz.db.Close() // opened read-only: nothing is lost
		}
	}()
	return script(&scene{f: fz, b: b})
}

// do runs steps in order, stopping at the first error.
func do(steps ...func() error) error {
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

// step begins one step of the scene.
func (sc *scene) step() error {
	sc.f.step++
	return sc.f.beginStep()
}

// connect connects s over MQTT 5 as user, its CONNECT edited by edit where
// set, and checks the answer as the runner does (try), taking over the
// connection s holds where it has one.
func (sc *scene) connect(s *slot, user string, edit func(*harness.Connect)) error {
	if err := sc.step(); err != nil {
		return err
	}
	l := logins[slices.IndexFunc(logins, func(l login) bool { return l.user == user })]
	p := signIn(&harness.Connect{ProtocolName: "MQTT", Version: harness.V5, ClientID: s.id, CleanStart: true}, l.user, l.password)
	if edit != nil {
		edit(p)
	}
	a, err := seal(p, s.session)
	if err != nil {
		return err
	}
	return sc.f.try(s, a, s.conn)
}

// durable keeps a CONNECT's session for a day after its connection, and
// resumed makes it keep the session it finds too.
func durable(p *harness.Connect) {
	p.Props = append(p.Props, harness.Property{ID: harness.SessionExpiryInterval, Int: expiryFloor})
}
func resumed(p *harness.Connect) { durable(p); p.CleanStart = false }

// client connects slot i over MQTT 5 as user, keeping its session where
// resume is set, its session expiry a day where durable (connect).
func (sc *scene) client(i int, user string, keep, resume bool) (*slot, error) {
	s := sc.f.slots[i]
	var edit func(*harness.Connect)
	switch {
	case resume:
		edit = resumed
	case keep:
		edit = durable
	}
	return s, sc.connect(s, user, edit)
}

// subscribe has s subscribe to subs, checking the SUBACK as the runner
// predicts it (subscribeWith).
func (sc *scene) subscribe(s *slot, subs ...harness.Subscription) error {
	if err := sc.step(); err != nil {
		return err
	}
	p := &harness.Subscribe{ID: s.nextID(), Subs: subs}
	wire, err := harness.Encode(p, s.conn.Version)
	if err != nil {
		return err
	}
	return sc.f.subscribeWith(s, p, wire, 0, false, nil, false)
}

// newOut is a PUBLISH of s's on topic at qos, retained where retain, its
// payload its own tag (fill), and the message the broker may store of it.
func (sc *scene) newOut(s *slot, topic string, qos byte, retain bool) (*out, error) {
	sc.n++
	o := &out{p: &harness.Publish{QoS: qos, Retain: retain, Topic: topic},
		m: &message{step: sc.f.step, publisher: s.clientID, hidden: s.hidden, topic: topic, qos: qos, retain: retain}}
	if qos > 0 {
		o.p.ID = s.nextID()
	}
	return o, sc.f.fill(o, sc.n, 1, 4, letters, s.conn.Version)
}

// publish has s publish on topic at QoS 1, then ping, past which the
// broker took it and queued its deliveries: each answer checked as the
// runner checks it.
func (sc *scene) publish(s *slot, topic string, retain bool) error {
	if err := sc.step(); err != nil {
		return err
	}
	o, err := sc.newOut(s, topic, 1, retain)
	if err != nil {
		return err
	}
	return sc.sendOut(s, o)
}

// sendOut sends o, placed and expected, reads its answer, and pings.
func (sc *scene) sendOut(s *slot, o *out) error {
	if err := sc.f.placeAll([]*out{o}); err != nil {
		return err
	}
	sc.f.expect(o)
	return do(func() error { return send(s, o) }, func() error { return collect(s) }, func() error { return pingPast(s) })
}

// hold has s publish on topic at QoS 2 and read its PUBREC, the exchange
// held until its PUBREL (release); it returns the PUBLISH.
func (sc *scene) hold(s *slot, topic string) (*out, error) {
	if err := sc.step(); err != nil {
		return nil, err
	}
	o, err := sc.newOut(s, topic, 2, false)
	if err == nil {
		err = sc.f.placeAll([]*out{o})
	}
	if err == nil {
		err = send(s, o)
	}
	if err == nil {
		err = collect(s)
	}
	return o, err
}

// release sends the PUBREL of o, held, and reads its PUBCOMP, o's message
// entering its channel then (releaseHeld).
func (sc *scene) release(s *slot, o *out) error {
	return do(func() error { return sc.sendRelease(s, o) },
		func() error { return collect(s) }, func() error { return pingPast(s) })
}

// sendRelease sends the PUBREL of o, held, as the runner sends it
// (releasing), leaving its PUBCOMP unread.
func (sc *scene) sendRelease(s *slot, o *out) error {
	if err := sc.step(); err != nil {
		return err
	}
	return s.conn.Send(sc.f.releasing(s, o.p.ID, s.held[o.p.ID]))
}

// control sets the fake's scene controls, under its lock.
func (sc *scene) control(set func(b *fake)) {
	sc.b.mu.Lock()
	defer sc.b.mu.Unlock()
	set(sc.b)
}

// read has s ping, reading and checking what the broker queued for it
// before the PINGRESP, its acknowledgements withheld.
func (sc *scene) read(s *slot) error {
	if err := sc.step(); err != nil {
		return err
	}
	return pingPast(s)
}

// hangUp has s send DISCONNECT with reason and read the close (hangUp).
func (sc *scene) hangUp(s *slot, reason byte) error {
	if err := sc.step(); err != nil {
		return err
	}
	return sc.f.hangUp(s, &harness.Disconnect{Reason: reason})
}

// drop ends s's connection without a DISCONNECT, its sending first, and
// reads to the broker's close, which follows the core's taking its end
// (lose): its Will, if any, published, its worker gone, by then.
func (sc *scene) drop(s *slot) error {
	if err := sc.step(); err != nil {
		return err
	}
	err := s.conn.Conn.(*observedConn).CloseWrite()
	if err == nil {
		err = awaitClose(s)
	}
	_ = s.conn.Close() // checked closed by the broker, or the step failed
	s.end(false)
	return err
}

// check runs the record checks (checkRecords).
func (sc *scene) check() error {
	if err := sc.step(); err != nil {
		return err
	}
	return sc.f.checkRecords()
}

// checkpoint runs a checkpoint's delivery checks (checkDeliveries).
func (sc *scene) checkpoint() error {
	if err := sc.step(); err != nil {
		return err
	}
	return sc.f.checkDeliveries()
}

// sceneOf is a fault's scene: what the run's failure names, and its
// script.
type sceneOf struct {
	want   string
	script func(*scene) error
}

// withWill gives a CONNECT a QoS 1 Will on a topic of the run's tag for
// step (willTag), so the runner knows it (newWill).
func withWill(step int) func(*harness.Connect) {
	return func(p *harness.Connect) {
		p.Will = &harness.Will{QoS: 1, Topic: willTag(step) + "/x", Payload: []byte("w")}
	}
}

// faultScenes are TestFaultsAreCaught's scenes, by fault, each built from
// the rule its fault breaks.
var faultScenes = map[fault]sceneOf{
	// design.md, Flows, CONNECT: "Session Present is 1 when the CONNECT
	// keeps its session and a session exists for the client id". A client
	// holding a durable session disconnects and comes back keeping it.
	lostSession: {"Session Present false; want 0x00, Session Present true", func(sc *scene) error {
		s := sc.f.slots[0]
		return do(func() error { return sc.connect(s, "ann", durable) }, func() error { return sc.hangUp(s, 0) },
			func() error { return sc.connect(s, "ann", resumed) })
	}},
	// design.md, CONNECT, 5: "An old connection for the id gets DISCONNECT
	// 0x8E on MQTT 5 and is closed". A connected client's id connects again.
	silentTakeover: {"the connection taken over: want DISCONNECT 0x8E then the close", takeoverScene},
	// keptScene: the fake ends the kept position, and the next SUBSCRIBE
	// reads cab from the floor, the first record again.
	endedKept: {"second copy of", keptScene},
	// keptResumedScene: the fake sends the kept position's replay of the
	// held record twice on the resumed connection, neither marked DUP.
	replayedTwice: {"second copy of", keptResumedScene},
	// design.md, Testing: the former connection's "terminal DISCONNECT is
	// its last outgoing MQTT packet"; MQTT-3.14.4-1.
	lateReply: {"the connection taken over: PINGRESP &{}, want the connection closed", takeoverScene},
	// design.md, CONNECT, 5, and Closing a connection: a connection live at
	// the takeover gets DISCONNECT 0x8E and the orderly end; only a
	// writer_full cut, counted, ends one otherwise. A connected client's id
	// connects again, the broker's writer_full count read.
	cutTakeover: {"the connection taken over: want DISCONNECT 0x8E then the close: waiting for DISCONNECT: unexpected EOF; saguin_disconnects_total{reason=\"writer_full\"} counts no writer_full cut", func(sc *scene) error {
		sc.counted()
		return takeoverScene(sc)
	}},
	// MQTT 5 3.1.2.11.4: "The Server MUST NOT send packets exceeding
	// Maximum Packet Size to the Client [MQTT-3.1.2-24]". A CONNECT with a
	// wrong password advertises a Maximum Packet Size of 20 bytes.
	oversizeRefusal: {"waiting for CONNACK: " + harness.ErrTooLarge.Error(), func(sc *scene) error {
		return sc.connect(sc.f.slots[0], "ann", func(p *harness.Connect) {
			p.Password = []byte("wrong")
			p.Props = harness.Properties{{ID: harness.MaximumPacketSize, Int: 20}}
		})
	}},
	// design.md, Flows, Replies: "PUBACK [...] sent only once the store
	// confirms". A client publishes four messages at QoS 1, then the
	// records are checked.
	lostRecord: {"acknowledged with PUBACK 0x00, is not in the database (R4)", func(sc *scene) error {
		s := sc.f.slots[0]
		steps := []func() error{func() error { return sc.connect(s, "ann", nil) }}
		for _, topic := range []string{"t/a", "t/b", "t/c", "t/d"} {
			steps = append(steps, func() error { return sc.publish(s, topic, false) })
		}
		return do(append(steps, sc.check)...)
	}},
	// MQTT 5 3.3.4: the client "MUST NOT send more than Receive Maximum
	// [...] [MQTT-3.3.4-7]"; 0x93 is for more. A client sends 21 QoS 1
	// PUBLISHes, keeping 20 at most unanswered (pump).
	countsAcked: {"DISCONNECT 0x93, the client never having had more than the CONNACK's Receive Maximum of 20", func(sc *scene) error {
		s := sc.f.slots[0]
		if err := sc.connect(s, "ann", nil); err != nil {
			return err
		}
		batch := make([]*out, receiveMaximum+1)
		for i := range batch {
			o, err := sc.newOut(s, "t/w", 1, false)
			if err != nil {
				return err
			}
			batch[i] = o
		}
		if err := sc.f.placeAll(batch); err != nil {
			return err
		}
		sc.f.expect(batch...)
		publishing := new(sync.WaitGroup)
		publishing.Add(1)
		return pump(s, batch, publishing)
	}},
	// design.md, PUBLISH: "0x87 | [...] a topic under $SYS/".
	wrongRefusal: {`PUBACK 0x90 to PUBLISH "$SYS/`, func(sc *scene) error {
		s := sc.f.slots[0]
		return do(func() error { return sc.connect(s, "ann", nil) }, func() error {
			o := &out{p: &harness.Publish{QoS: 1, Topic: "$SYS/x", ID: s.nextID(), Payload: []byte("x")}, refusals: []byte{0x87}}
			return do(func() error { return send(s, o) }, func() error { return collect(s) })
		})
	}},
	// design.md, DISCONNECT: "A clean DISCONNECT: 0x00 discards the Will".
	// A subscriber to # waits while a client connected with a Will sends
	// DISCONNECT 0x00.
	cleanWill: {"which discards the Will", func(sc *scene) error {
		t, s := sc.f.slots[0], sc.f.slots[1]
		return do(func() error { return sc.connect(t, "ann", nil) },
			func() error { return sc.subscribe(t, harness.Subscription{Filter: "#", QoS: 1, RetainHandling: 2}) },
			func() error { return sc.connect(s, "ann", withWill(sc.f.step+1)) },
			func() error { return sc.hangUp(s, 0) }, func() error { return sc.read(t) })
	}},
	// design.md, Words: a record holds "The publisher's client id"; MQTT 5
	// 3.1.2.5: the Will is published for its client. A client connected
	// with a Will drops its connection; the records are checked.
	anonymousWill: {`publisher ""`, func(sc *scene) error {
		s := sc.f.slots[0]
		return do(func() error { return sc.connect(s, "ann", withWill(sc.f.step+1)) }, func() error { return sc.drop(s) }, sc.check)
	}},
	// design.md, Words: Channel: "the topic goes to the filter that spells
	// it out most exactly". A record of ca/s/b, of small (ca/s/#), not of
	// ca (ca/#), configured first; the records are checked.
	firstMatch: {"the channel whose filter spells", func(sc *scene) error {
		s := sc.f.slots[0]
		return do(func() error { return sc.connect(s, "ann", nil) }, func() error { return sc.publish(s, "ca/s/b", false) }, sc.check)
	}},
	// config.md, channels.<name>, retention_bytes: "append: removes the
	// oldest beyond it". Two small records of cr, within its
	// retention_bytes together; the records are checked.
	wrongBoundary: {"retention removes only the oldest beyond it", func(sc *scene) error {
		s := sc.f.slots[0]
		return do(func() error { return sc.connect(s, "ann", nil) }, func() error { return sc.publish(s, "ca/r/a", false) },
			func() error { return sc.publish(s, "ca/r/b", false) }, sc.check)
	}},
	// design.md, Flows, CONNECT, 8: the CONNACK carries "the Maximum QoS,
	// Retain Available and Shared Subscription Available the acl_file
	// allows". di, denied qos2, connects.
	ignoredACL: {"CONNACK lacks property 0x24 = 1", func(sc *scene) error {
		return sc.connect(sc.f.slots[0], "di", nil)
	}},
	// design.md, Access and reload: "A record whose topic the user may not
	// read is then skipped for that subscription". bo subscribes to #, and
	// a/b/c, of three levels, which it may not read, is published.
	unreadable: {"may not read: it is skipped, not owed", published(3, "bo", "#", "a/b/c", false)},
	// design.md, Flows, PUBLISH: "any later PUBLISH with that identifier
	// gets PUBREC again. It is not stored twice, or charged twice against
	// its channel's size, whatever its DUP flag (MQTT-4.3.3-10)"; QoS 2 "enters on PUBREL". A subscriber to
	// t/# waits while a QoS 2 PUBLISH on t/q is held and sent again under
	// its identifier before its PUBREL.
	twiceStored: {"without a matching subscription at publication", func(sc *scene) error {
		t, p := sc.f.slots[0], sc.f.slots[1]
		var o *out
		return do(func() error { return sc.connect(t, "ann", nil) },
			func() error { return sc.subscribe(t, harness.Subscription{Filter: "t/#", QoS: 1, RetainHandling: 2}) },
			func() error { return sc.connect(p, "ann", nil) },
			func() (err error) { o, err = sc.hold(p, "t/q"); return err },
			func() error {
				p.held[o.p.ID].repeats++
				return do(func() error { return p.conn.Send(o.again(true)) }, func() error { return collect(p) })
			},
			func() error { return sc.read(t) })
	}},
	// design.md, $saguin topics: "The answer goes only to the asking
	// connection, on its Response Topic with its Correlation Data". A
	// client holding a durable session seeks ca's floor with a Response
	// Topic and Correlation Data.
	uncorrelated: {"the answer carries Correlation Data", func(sc *scene) error {
		s := sc.f.slots[0]
		return do(func() error { return sc.connect(s, "ann", durable) }, func() error {
			a := &asked{topic: answerPrefix + "1", correlation: []byte("cd"), offset: true, qos: 1, id: s.nextID()}
			p := &harness.Publish{QoS: 1, ID: a.id, Topic: seekTopic("ca"), Payload: []byte("0"),
				Props: harness.Properties{{ID: harness.ResponseTopic, Str: a.topic}, {ID: harness.CorrelationData, Data: a.correlation}}}
			s.sought("ca")
			s.asked = a
			defer func() { s.asked = nil }()
			return do(func() error { return s.conn.Send(p) }, func() error { return s.awaitAnswers(a) })
		})
	}},
}

// keptScene: design.md, Flows, SUBSCRIBE, UNSUBSCRIBE, "When the last
// subscription reaching an append channel or a queue is removed, a durable
// session keeps its position there"; "The next subscription reaching the
// channel reads from the kept position, so a record at or beyond it may be
// delivered again"; Delivery, Positions: "The position advances only past
// the contiguous settled or skipped prefix of examined records". A client
// holding a durable session subscribes at QoS 1 to ca/+/b, cab's filter, a
// channel whose readers start at the floor; a publisher stores a record
// there, which the client acknowledges, then another, which it holds. The
// client unsubscribes, the last subscription reaching cab, and subscribes
// to ca/+/b again: the second may come again, as a new delivery, but the
// first, which the kept position passed, may not. Holding both copies of
// the second, it unsubscribes and subscribes once more: "Exchanges already
// sent finish as before. [...] Their completion never moves the kept
// position", so the third reading interval may send the second a third
// time, and the first still not, which the scene checks it read so.
func keptScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	again := func() error {
		return do(func() error { return sc.step() },
			func() error { return sc.f.unsubscribeFrom(s, "ca/+/b") },
			func() error { return sc.subscribe(s, harness.Subscription{Filter: "ca/+/b", QoS: 1}) },
			func() error { return sc.read(s) })
	}
	return do(func() error { return sc.connect(s, "ann", durable) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "ca/+/b", QoS: 1}) },
		func() error { return sc.connect(p, "ann", nil) },
		func() error { return sc.publish(p, "ca/x/b", false) },
		func() error { return sc.read(s) },
		func() error { return s.releaseAcks() },
		func() error { return sc.publish(p, "ca/x/b", false) },
		func() error { return sc.read(s) },
		again, again,
		func() error {
			reads := s.incoming.reads["cab"]
			if len(reads) != 4 || slices.Index(reads, reads[1]) != 1 || slices.ContainsFunc(reads[2:], func(tag string) bool { return tag != reads[1] }) {
				return fmt.Errorf("%s read %q of cab; want the first record once and the second three times", s.id, reads)
			}
			return nil
		})
}

// keptResumedScene: design.md, Flows, SUBSCRIBE, UNSUBSCRIBE, "The next
// subscription reaching the channel reads from the kept position, so a
// record at or beyond it may be delivered again"; "Exchanges already sent
// finish as before"; Delivery, Positions: "That removal marks the
// session's own stored exchanges on the channel moves = 0"; Preparation:
// "When the connection ends, deliveries prepared and never handed to the
// writer are cancelled. [...] A live scan's position stays before their
// records, so a continuing session receives them later"; CONNECT, 9:
// "Resent messages have DUP set (MQTT-4.4.0-1)"; "New application
// deliveries wait behind recovered PUBLISHes". A client holding a durable
// session with Receive Maximum 1 subscribes at QoS 1 to ca/+/b, cab's
// filter; a publisher stores a record there, which the client
// acknowledges, then another, which it holds, its window full. The client
// unsubscribes, the last subscription reaching cab, and subscribes to
// ca/+/b again: the second may come again, but its window lets nothing
// out. The client sends DISCONNECT and resumes its session: the second
// comes again with DUP, its first exchange resent, and once acknowledged,
// again without DUP, the kept position's replay, which the scene checks it
// read so; the first, which the kept position passed, comes no more (item
// 0052a).
func keptResumedScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	window := func(edit func(*harness.Connect)) func(*harness.Connect) {
		return func(c *harness.Connect) {
			edit(c)
			c.Props = append(c.Props, harness.Property{ID: harness.ReceiveMaximum, Int: 1})
		}
	}
	return do(func() error { return sc.connect(s, "ann", window(durable)) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "ca/+/b", QoS: 1}) },
		func() error { return sc.connect(p, "ann", nil) },
		func() error { return sc.publish(p, "ca/x/b", false) },
		func() error { return sc.read(s) },
		func() error { return s.releaseAcks() },
		func() error { return sc.publish(p, "ca/x/b", false) },
		func() error { return sc.read(s) },
		func() error { return sc.step() },
		func() error { return sc.f.unsubscribeFrom(s, "ca/+/b") },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "ca/+/b", QoS: 1}) },
		func() error { return sc.read(s) },
		func() error { return sc.hangUp(s, 0) },
		func() error { return sc.connect(s, "ann", window(resumed)) },
		func() error { return sc.read(s) },
		func() error { return s.releaseAcks() },
		func() error { return sc.read(s) },
		func() error {
			reads := s.incoming.reads["cab"]
			if len(reads) != 2 || reads[0] != reads[1] {
				return fmt.Errorf("%s read %q of cab on its resumed connection; want the second record twice, its resend and its replay", s.id, reads)
			}
			return nil
		})
}

// takeoverScene: a client connects, and a second connection for its id
// connects too (design.md, CONNECT, 5).
func takeoverScene(sc *scene) error {
	s := sc.f.slots[0]
	return do(func() error { return sc.connect(s, "ann", nil) }, func() error { return sc.connect(s, "ann", nil) })
}

// counted has the runner read the fake's writer_full count as a run reads
// the broker's /metrics, within a bound short enough for a scene.
func (sc *scene) counted() {
	sc.f.timeout = 2 * time.Second
	sc.f.scrape = func() (testkit.Exposition, error) {
		sc.b.mu.Lock()
		defer sc.b.mu.Unlock()
		key := testkit.SeriesKey(disconnectsTotal, "reason", "writer_full")
		return testkit.Exposition{Values: map[string]float64{key: float64(sc.b.writerFull)}}, nil
	}
}

// cutWriter has the fake cut s's connection as a full writer does
// (design.md, Closing a connection: "A close after a write past the write
// timeout has no time left. The socket is closed at once"), mid-packet,
// counted where count is set, and waits for s's client to read the end.
func (sc *scene) cutWriter(s *slot, count bool) error {
	sc.b.mu.Lock()
	c := sc.b.sessions[s.id].conn
	if wire, err := harness.Encode(&harness.Pingresp{}, c.version); err == nil {
		c.queue(wire[:1], false) // the rest never comes
	}
	c.finish()
	if count {
		sc.b.writerFull++
	}
	sc.b.mu.Unlock()
	<-c.done // its writer has ended the sending
	return nil
}

// TestTakeoverAfterWriterFull: a connection the broker cut as writer_full
// before a takeover of its id owes the takeover neither DISCONNECT 0x8E
// nor an orderly end, where the broker counts the cut (design.md, Closing
// a connection; metrics.md, saguin_disconnects_total{reason}); uncounted,
// the same end fails the takeover's check.
func TestTakeoverAfterWriterFull(t *testing.T) {
	for _, count := range []bool{true, false} {
		err := playScene(t, "", sceneQueue, func(sc *scene) error {
			s := sc.f.slots[0]
			sc.counted()
			return do(func() error { return sc.connect(s, "ann", nil) }, func() error { return sc.cutWriter(s, count) },
				func() error { return sc.connect(s, "ann", nil) },
				func() error {
					if sc.f.writerFull != 1 {
						return fmt.Errorf("the run found %d writer_full cuts, want 1", sc.f.writerFull)
					}
					return pingPast(s)
				})
		})
		switch want := "counts no writer_full cut"; {
		case count && err != nil:
			t.Errorf("counted: %v; want none", err)
		case !count && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("uncounted: %v; want an error naming %q", err, want)
		}
	}
}

// TestReleaseAfterALostPubrecDelivers: design.md, Flows, PUBLISH, the
// table: QoS 2 "enters on PUBREL, from a held copy stored and charged to
// the channel before PUBREC"; Flows, Replies: "If the store confirms after
// that connection has ended or been taken over, what was stored stays and
// the reply is dropped. A sender that kept its session resends what was
// unacknowledged and is answered then (MQTT-4.4.0-1)". A client holding a
// durable session subscribes to t/# at QoS 1 and publishes t/q at QoS 2,
// and its connection ends before the PUBREC comes. It resumes its session,
// sends the PUBLISH again and reads its PUBREC, then sends its PUBREL: the
// publication enters its channel then, and its delivery to the client's
// own t/# is one the run allows, as the record checks allow its record.
func TestReleaseAfterALostPubrecDelivers(t *testing.T) {
	err := playScene(t, "", sceneQueue, func(sc *scene) error {
		s := sc.f.slots[0]
		var o *out
		return do(func() error { return sc.connect(s, "ann", durable) },
			func() error { return sc.subscribe(s, harness.Subscription{Filter: "t/#", QoS: 1, RetainHandling: 2}) },
			func() error {
				if err := sc.step(); err != nil {
					return err
				}
				var err error
				if o, err = sc.newOut(s, "t/q", 2, false); err != nil {
					return err
				}
				if err := sc.f.placeAll([]*out{o}); err != nil {
					return err
				}
				sc.control(func(b *fake) { b.unreceipted = true })
				defer sc.control(func(b *fake) { b.unreceipted = false })
				if err := send(s, o); err != nil {
					return err
				}
				return sc.drop(s)
			},
			func() error { return sc.connect(s, "ann", resumed) },
			func() error { return sc.release(s, o) },
			sc.checkpoint, sc.check)
	})
	if err != nil {
		t.Error(err)
	}
}

// TestReleaseAfterADroppedPubrelDelivers: design.md, Flows, Closing a
// connection, 2: "Any other unsubmitted packet is dropped"; CONNECT, 5:
// "Anything its reader still delivers is ignored"; PUBLISH, the table: QoS
// 2 "enters on PUBREL [...] The client's PUBREL, resent when it reconnects,
// completes the move". A client holding a durable session publishes t/q at
// QoS 2 and reads its PUBREC, then sends its PUBREL, which the broker
// drops, and its connection ends. Another client subscribes to t/#. The
// first resumes its session and sends the PUBREL again: the publication
// enters its channel then, and its delivery to the later t/# is one the
// run allows.
func TestReleaseAfterADroppedPubrelDelivers(t *testing.T) {
	err := playScene(t, "", sceneQueue, func(sc *scene) error {
		s, sub := sc.f.slots[0], sc.f.slots[1]
		var o *out
		return do(func() error { return sc.connect(s, "ann", durable) },
			func() error {
				var err error
				o, err = sc.hold(s, "t/q")
				return err
			},
			func() error {
				sc.control(func(b *fake) { b.unreleased = true })
				defer sc.control(func(b *fake) { b.unreleased = false })
				if err := sc.sendRelease(s, o); err != nil {
					return err
				}
				return sc.drop(s)
			},
			func() error { return sc.connect(sub, "cy", nil) },
			func() error { return sc.subscribe(sub, harness.Subscription{Filter: "t/#", QoS: 1}) },
			func() error { return sc.connect(s, "ann", resumed) },
			func() error { return sc.read(sub) },
			sc.checkpoint, sc.check)
	})
	if err != nil {
		t.Error(err)
	}
}

// pubrelResendScene: a subscriber holding a durable session receives a
// QoS 2 delivery, answers PUBREC 0x00 and reads its PUBREL, withholding
// its PUBCOMP, then drops its connection and comes back keeping its
// session.
func pubrelResendScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	var o *out
	return do(func() error { return sc.connect(s, "ann", durable) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "t/#", QoS: 2, RetainHandling: 2}) },
		func() error { return sc.connect(p, "ann", nil) },
		func() (err error) { o, err = sc.hold(p, "t/r"); return err },
		func() error { return sc.release(p, o) },
		func() error { return sc.read(s) },
		func() error {
			// Its PUBREC 0x00 (receipt refuses every other one, from the
			// first): the PUBREL that follows is withheld, auto being off.
			s.incoming.receipts = 1
			return s.releaseAcks()
		},
		func() error { return sc.read(s) },
		func() error { return sc.drop(s) },
		func() error { return sc.connect(s, "ann", resumed) })
}

// latestReturnScene: a subscriber to kv/# holding a durable session has
// value v1 of kv/a, unacknowledged, where first is set, drops its
// connection, and comes back keeping its session after kv/a takes value
// v2, and v1 too where first is not set.
func latestReturnScene(first bool) func(*scene) error {
	return func(sc *scene) error {
		s, p := sc.f.slots[0], sc.f.slots[1]
		steps := []func() error{func() error { return sc.connect(s, "ann", durable) },
			func() error { return sc.subscribe(s, harness.Subscription{Filter: "kv/#", QoS: 1, RetainHandling: 2}) },
			func() error { return sc.connect(p, "ann", nil) }}
		if first {
			steps = append(steps, func() error { return sc.publish(p, "kv/a", false) }, func() error { return sc.read(s) })
		}
		steps = append(steps, func() error { return sc.drop(s) })
		if !first {
			steps = append(steps, func() error { return sc.publish(p, "kv/a", false) })
		}
		steps = append(steps, func() error { return sc.publish(p, "kv/a", false) }, func() error { return sc.connect(s, "ann", resumed) })
		return do(steps...)
	}
}

// tombstoneScene: a subscriber to kv/# waits while kv/d is deleted, then a
// checkpoint owes it the deletion, its topic's current change.
func tombstoneScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	return do(func() error { return sc.connect(s, "ann", nil) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "kv/#", QoS: 1, RetainHandling: 2}) },
		func() error { return sc.connect(p, "ann", nil) },
		func() error {
			if err := sc.step(); err != nil {
				return err
			}
			o, err := sc.newOut(p, "kv/d", 1, false)
			if err != nil {
				return err
			}
			// A deletion: an empty payload, its tag its first saguin-id
			// (newPublish).
			o.deletion, o.m.id = true, payloadTag(sc.f.step, sc.n)
			o.p.Props = harness.Properties{harness.User("saguin-id", o.m.id)}
			if err := sc.f.fill(o, sc.n, 1, 4, letters, p.conn.Version); err != nil {
				return err
			}
			return sc.sendOut(p, o)
		},
		sc.checkpoint)
}

// retainScene: a subscriber to t/# without Retain As Published waits while
// a retained message is published on t/r.
var retainScene = published(0, "ann", "t/#", "t/r", true)

// published is a scene: slot sub, logged in as user, subscribes to filter
// at QoS 1, and the slot after it, as ann, publishes on topic at QoS 1,
// retained where retain; the subscriber reads what came.
func published(sub int, user, filter, topic string, retain bool) func(*scene) error {
	return func(sc *scene) error {
		s, p := sc.f.slots[sub], sc.f.slots[(sub+1)%len(sc.f.slots)]
		return do(func() error { return sc.connect(s, user, nil) },
			func() error { return sc.subscribe(s, harness.Subscription{Filter: filter, QoS: 1, RetainHandling: 2}) },
			func() error { return sc.connect(p, "ann", nil) }, func() error { return sc.publish(p, topic, retain) },
			func() error { return sc.read(s) })
	}
}

// sharedScene: two clients join the group g of t/#, and a message is
// published on t/s; both read what came.
func sharedScene(sc *scene) error {
	a, b, p := sc.f.slots[0], sc.f.slots[1], sc.f.slots[2]
	join := func(s *slot) func() error {
		return func() error {
			if err := sc.step(); err != nil {
				return err
			}
			_, err := sc.f.subscribeShares(s, "$share/g/t/#")
			return err
		}
	}
	return do(func() error { return sc.connect(a, "ann", nil) }, func() error { return sc.connect(b, "ann", nil) },
		join(a), join(b), func() error { return sc.connect(p, "ann", nil) }, func() error { return sc.publish(p, "t/s", false) },
		func() error { return sc.read(a) }, func() error { return sc.read(b) })
}

// subscribeID is subscribe, the SUBSCRIBE carrying Subscription
// Identifier id.
func (sc *scene) subscribeID(s *slot, id uint32, subs ...harness.Subscription) error {
	if err := sc.step(); err != nil {
		return err
	}
	p := &harness.Subscribe{ID: s.nextID(), Subs: subs, Props: harness.Properties{{ID: harness.SubscriptionIdentifier, Int: id}}}
	wire, err := harness.Encode(p, s.conn.Version)
	if err != nil {
		return err
	}
	return sc.f.subscribeWith(s, p, wire, id, false, nil, false)
}

// orderScene: a subscriber to t/# waits while one client publishes t/a,
// then t/b.
func orderScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	return do(func() error { return sc.connect(s, "ann", nil) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "t/#", QoS: 1, RetainHandling: 2}) },
		func() error { return sc.connect(p, "ann", nil) }, func() error { return sc.publish(p, "t/a", false) },
		func() error { return sc.publish(p, "t/b", false) }, func() error { return sc.read(s) })
}

// owedScene: a subscriber to t/# waits while four messages are published
// at QoS 1, then a checkpoint owes it each.
func owedScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	steps := []func() error{func() error { return sc.connect(s, "ann", nil) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "t/#", QoS: 1, RetainHandling: 2}) },
		func() error { return sc.connect(p, "ann", nil) }}
	for _, topic := range []string{"t/a", "t/b", "t/c", "t/d"} {
		steps = append(steps, func() error { return sc.publish(p, topic, false) })
	}
	return do(append(steps, sc.checkpoint)...)
}

// noLocalScene: a client subscribes to t/# with No Local, publishes t/a
// and reads what came.
func noLocalScene(sc *scene) error {
	s := sc.f.slots[0]
	return do(func() error { return sc.connect(s, "ann", nil) },
		func() error {
			return sc.subscribe(s, harness.Subscription{Filter: "t/#", QoS: 1, NoLocal: true, RetainHandling: 2})
		},
		func() error { return sc.publish(s, "t/a", false) }, func() error { return sc.read(s) })
}

// identifiersScene: a client subscribes to t/# with Subscription Identifier
// 7 and to x/# with 9, and another publishes t/a.
func identifiersScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	return do(func() error { return sc.connect(s, "ann", nil) },
		func() error {
			return sc.subscribeID(s, 7, harness.Subscription{Filter: "t/#", QoS: 1, RetainHandling: 2})
		},
		func() error {
			return sc.subscribeID(s, 9, harness.Subscription{Filter: "x/#", QoS: 1, RetainHandling: 2})
		},
		func() error { return sc.connect(p, "ann", nil) }, func() error { return sc.publish(p, "t/a", false) },
		func() error { return sc.read(s) })
}

// replacedScene: kv/a takes two values, then the records are checked.
func replacedScene(sc *scene) error {
	p := sc.f.slots[0]
	return do(func() error { return sc.connect(p, "ann", nil) }, func() error { return sc.publish(p, "kv/a", false) },
		func() error { return sc.publish(p, "kv/a", false) }, sc.check)
}

// passScene: kv/b takes a value, then a client subscribes to kv/# with
// Retain Handling 0, which starts a pass, then a checkpoint owes it the
// value.
func passScene(sc *scene) error {
	s, p := sc.f.slots[0], sc.f.slots[1]
	return do(func() error { return sc.connect(p, "ann", nil) }, func() error { return sc.publish(p, "kv/b", false) },
		func() error { return sc.connect(s, "ann", nil) },
		func() error { return sc.subscribe(s, harness.Subscription{Filter: "kv/#", QoS: 1}) }, sc.checkpoint)
}
