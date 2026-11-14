package main

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// incoming belongs to one connection, including while a burst pumps it on
// its own goroutine. The publication registry and subscription revisions
// are read-only during bursts; no concurrent reader changes shared state.
// A session's first RETAIN values (subscriptions.retain) are written only
// by its own connection's reader.
type incoming struct {
	since       int
	established time.Time // CONNACK observed, bounding the connection's start
	// seen are the publications delivered on the connection, by tag, but
	// those a position made again or moved by a seek may repeat (restart);
	// had all of them: one had is owed again only by a replay (owed).
	seen, had map[string]bool
	last      map[string]uint64
	held      []*harness.Publish // PUBACKs must preserve PUBLISH receipt order
	auto      bool               // checkpoints release all acknowledgements, including new ones
	// delivered counts the deliveries read since the connection began or
	// the publication registry was last pruned, and passes the filters its
	// SUBSCRIBEs granted with a retained pass (mostDeliveries).
	delivered, passes int
	// epochs counts the positions its SUBSCRIBEs made, each of which may
	// replay a channel's history (mostDeliveries); offsets is the last
	// saguin-offset read of each channel, and unordered the channels whose
	// position was made again on it (appends.go).
	epochs    int
	offsets   map[string]int64
	unordered map[string]bool
	// grace counts, by channel, the second copies a position made again
	// on this connection still allows (restart); unsubscribed is the step
	// of its last UNSUBACK, and orphans the deliveries matching no
	// subscription it may still read after it, of orphanBound (orphan).
	grace map[string]int
	// reads are, by configured channel, the tags of the records read on
	// the connection, every copy, in the order they came: which of them a
	// kept position may send again (unplace, resume).
	reads map[string][]string
	// repeats counts, by tag, the copies beyond the first that kept
	// positions read again on the connection have sent of a record
	// (repeatable).
	repeats              map[string]int
	unsubscribed         int
	orphans, orphanBound int
	// rels are the packet identifiers of the PUBRELs read on the
	// connection whose PUBCOMP is withheld, in the order they came; strays
	// the identifiers of the PUBRECs it sent naming no delivery, their
	// PUBREL still to come (strayPubrec); flush completes even the
	// exchanges kept (outExchange.keep) while owed deliveries are awaited,
	// so that none holds the window shut.
	rels     []uint16
	strays   map[uint16]bool
	flush    bool
	receipts int // the PUBRECs it sent to QoS 2 deliveries (receipt)
	// shared are its QoS 1 shared deliveries not yet acknowledged, and
	// acked those acknowledged since its last PINGRESP (shares.go).
	shared map[*harness.Publish]shareKey
	acked  []shareKey
	// lastOn is each latest topic's change it had last, live (latest.go).
	lastOn map[string]latestSeen
	// Of the queue (queues.go): worked, the connection has subscribed as a
	// worker; attempts are the attempts it read, in order, offers its
	// offers whose PUBACK it has not sent, by delivery (nil for a resend),
	// and offersRead how many it read; cancelBound is how many more first
	// transmissions may come after unsubscribedWorker, the step of the
	// UNSUBACK of its worker subscription.
	worked                          bool
	attempts                        []*jobAttempt
	offers                          map[*harness.Publish]*jobAttempt
	offersRead                      int
	cancelBound, unsubscribedWorker int
}

// read checks every delivery, wherever it appears among replies. recv
// skips those deliveries for callers waiting for a control packet. Each
// packet is waited for -timeout at most, and a broker sending more
// deliveries than it can owe the connection fails, so that no wait for a
// reply or for quiet lasts for ever, whichever way a broker fails.
func (s *slot) read() (harness.Packet, error) {
	recv := s.conn.Recv
	if s.readPacket != nil {
		recv = s.readPacket
	}
	p, err := recv()
	if err != nil {
		return nil, err
	}
	switch pub := p.(type) {
	case *harness.Publish:
		if answerTopic(pub.Topic) {
			return p, s.takeAnswer(pub)
		}
		if q := s.offerQueue(pub); q != "" {
			// A worker offer, which answers as its attempt's plan says
			// (queues.go), its PUBACK in the order of every other.
			if err := s.takeOffer(pub, q); err != nil {
				return nil, err
			}
			s.incoming.held = append(s.incoming.held, pub)
			if s.incoming.auto {
				err = s.releaseAcks()
			}
			return p, err
		}
		if err := s.checkDelivery(pub); err != nil {
			return nil, err
		}
		s.incoming.delivered++
		if most := s.mostDeliveries(); s.incoming.delivered > most {
			return nil, fmt.Errorf("%s: %d deliveries since its connection or the last checkpoint, more than the %d the broker can owe it: it never stops sending",
				s.id, s.incoming.delivered, most)
		}
		if pub.QoS > 0 {
			if err := s.noteDelivery(pub); err != nil {
				return nil, err
			}
			s.incoming.held = append(s.incoming.held, pub)
			if s.incoming.auto {
				err = s.releaseAcks()
			}
		}
	case *harness.Ack:
		if pub.Kind == harness.PUBREL {
			err = s.takePubrel(pub)
		}
	}
	return p, err
}

// received reports whether p is a packet read checks and answers itself: a
// delivery, or the PUBREL of one.
func received(p harness.Packet) bool {
	switch p := p.(type) {
	case *harness.Publish:
		return true
	case *harness.Ack:
		return p.Kind == harness.PUBREL
	}
	return false
}

// mostDeliveries is the most deliveries a broker keeping to the design can
// send s's connection from its start, or from the last pruning of the
// publication registry, to now. It is no measure of a good run, only the
// point past which a broker that never stops sending (a resend loop) has
// sent what nothing owes. Each delivery is one of:
//   - a publication in the registry, each at most once: between prunings
//     the registry only grows, and a delivery of one not in it fails
//     checkDelivery before it is counted; "While connected, the broker starts
//     no resend. Its replies to repeated packets follow their QoS flow."
//     (design.md, Delivery) and "a record matching several of
//     a session's non-shared subscriptions is sent once" (Flows,
//     SUBSCRIBE, One copy);
//   - a value of a retained pass started on this connection, which ends
//     with it ("A retained pass's prepared deliveries are dropped", Delivery,
//     Preparation), one per filter granted with Retain Handling 0 or 1
//     (MQTT-3.3.1-9, -10; MQTT 3.1.1 has only the first), at most one per
//     topic holding a value: "each page sends the values current when it is
//     read, below the tail at the cut" (The start point), each value one of
//     the run's retained publications (the broker holds no record but the
//     run's, as -db says);
//   - an exchange recovered at CONNECT, resent whatever the registry: "a
//     connection's prepared and sent deliveries together are at most
//     twice its window", the window at most limits.max_inflight
//     (Delivery, Preparation and One mechanism; CONNECT, 9).
//   - a record of an append channel a position made on this connection
//     replays, at most each of the channel records the run has sent, which
//     are never pruned (SUBSCRIBE: "append [...] Starts with the floor");
//   - a publication of the registry through each shared subscription the
//     session was granted, once each: "Each $share subscription is handled
//     on its own" (One copy), and a member receives a message again only
//     as a resend (shares.go);
//   - a latest channel's value a pass sends again, at most one per topic
//     the run has sent a change of (latest.go, takePass).
func (s *slot) mostDeliveries() int {
	shared := 0
	if s.subs != nil {
		shared = len(s.subs.shareIDs)
	}
	return len(s.f.publications)*(1+shared) + (s.f.retained+len(s.f.latest.topics))*s.incoming.passes + 2*maxInflight + s.f.channelRecords*s.incoming.epochs
}

func (s *slot) recv() (harness.Packet, error) {
	for {
		p, err := s.read()
		if err != nil || !received(p) {
			return p, err
		}
	}
}

// releaseAcks sends the acknowledgement of each delivery held, in the order
// they came: a PUBACK at QoS 1, a worker offer's as its plan says
// (pubackOffer), a PUBREC at QoS 2 (receipt) (MQTT 5 4.6:
// the Client "MUST send PUBACK packets in the order in which the
// corresponding PUBLISH packets were received" [MQTT-4.6.0-2], and PUBRECs
// likewise [MQTT-4.6.0-3]); then the PUBCOMP of each PUBREL withheld but
// those kept (completePubrels).
func (s *slot) releaseAcks() error {
	for _, p := range s.incoming.held {
		var err error
		if a, offer := s.incoming.offers[p]; offer {
			err = s.pubackOffer(p, a)
		} else if p.QoS == 2 {
			err = s.receipt(p)
		} else {
			err = s.conn.Send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID})
			s.acknowledgedShare(p)
		}
		if err != nil {
			return err
		}
	}
	s.incoming.held = nil
	return s.completePubrels(false)
}

// receive chooses its acknowledgement policy before reading. PINGRESP is
// an input barrier, followed by draining to quiet; it is not a delivery fence.
func (f *fuzzer) receive() (bool, error) {
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	acknowledge := f.rng.IntN(3) == 0
	f.note("%s: receive, acknowledge %v", s.id, acknowledge)
	return true, s.receiveUntilQuiet(acknowledge)
}

func (f *fuzzer) acknowledge() (bool, error) {
	// Eligibility and selection use only driver-owned connection state, never
	// a count of packets that happened to precede an earlier reply.
	s := f.pick(func(s *slot) bool { return s.conn != nil })
	if s == nil {
		return false, nil
	}
	f.note("%s: release withheld acknowledgements", s.id)
	return true, s.receiveUntilQuiet(true)
}

// outExchange is one QoS 2 delivery to a session as its client sees it
// (design.md, Delivery: PUBLISH, PUBREC, PUBREL, PUBCOMP), under the
// broker's packet identifier, kept in the session's subscriptions so that
// it outlives the connection it came on.
type outExchange struct {
	tag string // what it delivers, for messages
	// conn is the connection of its latest packet, either way: one before
	// s's own is an earlier connection of the session, whose exchange the
	// resume continues (design.md, CONNECT, 9).
	conn *harness.Client
	// recd: its client sent PUBREC 0x00; rel: its PUBREL came; done: its
	// client sent PUBCOMP, or a refusing PUBREC (refused), which settles
	// it (design.md, Delivery, Positions: "an acknowledgement, PUBACK,
	// PUBCOMP or a refusing PUBREC, settles its own delivery").
	recd, rel, done, refused bool
	// keep: its PUBCOMP waits for the client's next release of its
	// acknowledgements (completePubrels), so that a connection may end with
	// it past its PUBREL.
	keep bool
	// again is the connection that sent its PUBREC 0x00 twice, whose
	// second PUBREL is still to come there, nil for none (receipt).
	again *harness.Client
}

// refusingPubrecs are the reason codes of 0x80 or above MQTT 5's Table 3-5
// gives a PUBREC (3.5.2.1).
var refusingPubrecs = []byte{0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99}

// noteDelivery checks p, a delivery at QoS 1 or 2, against the session's
// QoS 2 exchange under its packet identifier, and keeps a QoS 2 one as the
// exchange the identifier now holds. A PUBLISH under the identifier of an
// exchange past its PUBREL, its PUBCOMP unsent, is an error: MQTT 5 4.3.3,
// the sender "MUST NOT re-send the PUBLISH once it has sent the
// corresponding PUBREL packet [MQTT-4.3.3-6]"; so is one under the
// identifier of an exchange its connection has not settled (design.md,
// Delivery: "While connected, the broker starts no resend. Its replies to
// repeated packets follow their QoS flow."; MQTT 5 2.2.1: "a non zero
// Packet Identifier that is currently unused [MQTT-2.2.1-4]"). One on a
// later connection takes over an exchange not past its PUBREL: a resend of
// its PUBLISH (CONNECT, 9), or a new delivery where the resume settled it
// as if sent (Delivery, A message over the client's Maximum Packet Size).
func (s *slot) noteDelivery(p *harness.Publish) error {
	if s.subs == nil {
		return nil
	}
	tag, _ := s.deliveryTag(p)
	switch x := s.subs.outbound[p.ID]; {
	case x == nil || x.done:
	case x.rel:
		return fmt.Errorf("%s: PUBLISH of %s under packet identifier %d, whose delivery of %s is past its PUBREL, its PUBCOMP unsent (MQTT-4.3.3-6; design.md, Delivery, Positions: \"A QoS 2 delivery stores its PUBREC before its PUBREL goes to the writer\")",
			s.id, tag, p.ID, x.tag)
	case x.conn == s.conn:
		return fmt.Errorf("%s: PUBLISH of %s under packet identifier %d while connected, its delivery of %s unsettled (design.md, Delivery: \"Nothing is re-sent while connected\"; MQTT-2.2.1-4)",
			s.id, tag, p.ID, x.tag)
	}
	if p.QoS == 2 {
		s.subs.outbound[p.ID] = &outExchange{tag: tag, conn: s.conn}
	} else {
		delete(s.subs.outbound, p.ID)
	}
	return nil
}

// receipt sends the PUBREC of p, a QoS 2 delivery: in MQTT 5, every other
// one its connection receipts, the first included, refusing with a code of
// refusingPubrecs, which settles it (MQTT 5 4.4: "If PUBACK or PUBREC is
// received containing a Reason Code of 0x80 or greater the corresponding
// PUBLISH packet is treated as acknowledged, and MUST NOT be retransmitted
// [MQTT-4.4.0-2]"); the rest 0x00, half of those kept past their PUBREL,
// and a quarter of them sent twice, whose second PUBREL is 0x00 too
// (design.md, Delivery, Positions: "Once a successful PUBREC is accepted
// for an outgoing QoS 2 exchange, every further PUBREC for it is answered
// with a successful PUBREL under the same identifier"). These choices come
// from the connection's count and p's topic_hash, not the generator, as
// receipt runs whenever p happens to arrive, which the seed does not fix
// (main.go).
func (s *slot) receipt(p *harness.Publish) error {
	h := harness.TopicHash(s.id + " " + p.Topic + " " + string(p.Payload))
	ack := &harness.Ack{Kind: harness.PUBREC, ID: p.ID}
	if s.conn.Version == harness.V5 && s.incoming.receipts%2 == 0 {
		ack.Reason = refusingPubrecs[h%uint64(len(refusingPubrecs))]
	}
	s.incoming.receipts++
	if err := s.conn.Send(ack); err != nil {
		return err
	}
	if s.subs == nil {
		return nil
	}
	x := s.subs.outbound[p.ID]
	if x == nil || x.conn != s.conn || x.recd || x.done {
		return nil
	}
	x.recd, x.refused, x.done, x.keep = ack.Reason == 0, ack.Reason != 0, ack.Reason != 0, h%2 == 1
	if ack.Reason != 0 || (h>>8)%4 != 0 {
		return nil
	}
	// Not noted in the step log, which holds the seed's draws alone:
	// when a delivery arrives is the broker's.
	x.again = s.conn
	return s.conn.Send(ack)
}

// takePubrel checks a, a PUBREL s's connection read, and answers it. One
// for a PUBREC s sent naming no delivery is 0x92, plain in MQTT 3.1.1
// (design.md, Delivery, Positions: "A PUBREC naming no outgoing QoS 2
// exchange [...] gets PUBREL 0x92, a plain PUBREL on MQTT 3.1.1"). The
// second for a PUBREC s's connection sent twice is 0x00 and needs no
// PUBCOMP of its own ("every further PUBREC for it is answered with a
// successful PUBREL [...] They change no delivery state"). Any other is 0x00
// and follows the client's PUBREC 0x00 to a delivery (MQTT 5 4.3.3: the
// sender "MUST send a PUBREL packet when it receives a PUBREC packet from
// the receiver with a Reason Code value less than 0x80 [MQTT-4.3.3-4]"),
// once on that connection and again on each later one until its PUBCOMP
// (CONNECT, 9: "The broker reads in-flight messages and PUBRELs from the
// store and resends them"), never after a refusing PUBREC. Its PUBCOMP goes
// at once while s acknowledges what comes, but for an exchange kept,
// else it waits (completePubrels).
func (s *slot) takePubrel(a *harness.Ack) error {
	if s.incoming.strays[a.ID] {
		delete(s.incoming.strays, a.ID)
		want := byte(0x92)
		if s.conn.Version == harness.V311 {
			want = 0
		}
		if a.Reason != want {
			return fmt.Errorf("%s: PUBREL %d 0x%02X to a PUBREC naming no delivery; want 0x%02X (design.md, Delivery, Positions: \"A PUBREC naming no outgoing QoS 2 exchange [...] gets PUBREL 0x92, a plain PUBREL on MQTT 3.1.1\")",
				s.id, a.ID, a.Reason, want)
		}
		return nil
	}
	var x *outExchange
	if s.subs != nil {
		x = s.subs.outbound[a.ID]
	}
	if x != nil && x.rel && x.again == s.conn {
		x.again = nil
		if a.Reason != 0 {
			return fmt.Errorf("%s: PUBREL %d 0x%02X to the repeated PUBREC of %s; want 0x00 (design.md, Delivery, Positions: \"every further PUBREC for it is answered with a successful PUBREL under the same identifier\")", s.id, a.ID, a.Reason, x.tag)
		}
		return nil
	}
	switch {
	case x == nil || !x.recd || x.refused:
		return fmt.Errorf("%s: PUBREL %d, naming no delivery whose PUBREC 0x00 its client sent (MQTT-4.3.3-4; MQTT-4.4.0-2: a refused one \"is treated as acknowledged\")", s.id, a.ID)
	case x.conn == s.conn && (x.rel || x.done):
		return fmt.Errorf("%s: PUBREL %d of %s again while connected (design.md, Delivery: \"Nothing is re-sent while connected\")", s.id, a.ID, x.tag)
	case a.Reason != 0:
		return fmt.Errorf("%s: PUBREL %d 0x%02X to the PUBREC of %s; want 0x00", s.id, a.ID, a.Reason, x.tag)
	}
	x.rel, x.done, x.conn = true, false, s.conn
	if s.incoming.auto && (!x.keep || s.incoming.flush) {
		return s.sendPubcomp(a.ID, x)
	}
	s.incoming.rels = append(s.incoming.rels, a.ID)
	return nil
}

// sendPubcomp sends s's PUBCOMP to x's PUBREL, under packet identifier id,
// which settles x (design.md, Delivery, Positions; MQTT 5 [MQTT-4.3.3-11]
// for the receiver).
func (s *slot) sendPubcomp(id uint16, x *outExchange) error {
	x.done, x.conn = true, s.conn
	return s.conn.Send(&harness.Ack{Kind: harness.PUBCOMP, ID: id})
}

// completePubrels sends the PUBCOMPs withheld on s's connection, in the
// order their PUBRELs came: all of them, or all but those of exchanges
// kept.
func (s *slot) completePubrels(all bool) error {
	kept := s.incoming.rels[:0]
	for _, id := range s.incoming.rels {
		var x *outExchange
		if s.subs != nil {
			x = s.subs.outbound[id]
		}
		switch {
		case x == nil || !x.rel || x.done || x.conn != s.conn:
		case x.keep && !all:
			kept = append(kept, id)
		default:
			if err := s.sendPubcomp(id, x); err != nil {
				return err
			}
		}
	}
	s.incoming.rels = kept
	return nil
}

// strayPubrec sends a PUBREC under a packet identifier no delivery to s's
// session holds, from 1024 up, whose PUBREL 0x92 (MQTT 3.1.1: PUBREL)
// follows (takePubrel): the broker's identifiers are the lowest free
// (design.md, Delivery, Sending), and "A connection's prepared and sent
// deliveries together are at most twice its window" (Preparation), the
// window at most limits.max_inflight, so none reaches it. The identifier
// comes from the step, which leaves the generator's draws as they were.
func (s *slot) strayPubrec() error {
	id := uint16(1024 + s.f.step%(1<<16-1024))
	if s.incoming.strays == nil {
		s.incoming.strays = map[uint16]bool{}
	}
	s.incoming.strays[id] = true
	s.f.note("%s: PUBREC %d, naming no delivery: want PUBREL 0x92 (MQTT 3.1.1: PUBREL) before the PINGRESP", s.id, id)
	return s.conn.Send(&harness.Ack{Kind: harness.PUBREC, ID: id})
}

// awaitPubrels reads s's packets, at a checkpoint, until every PUBREL its
// session is owed has come (owedPubrel), then completes them all. The
// broker silent for -timeout first, or a control packet but an answer s is
// owed, fails.
func (s *slot) awaitPubrels() error {
	s.incoming.flush = true
	defer func() { s.incoming.flush = false }()
	for {
		id, owed := s.owedPubrel()
		if !owed {
			return s.completePubrels(true)
		}
		p, err := s.read()
		if err != nil {
			return fmt.Errorf("%s: PUBREL %d still owed at checkpoint: %w", s.id, id, err)
		}
		if received(p) {
			continue
		}
		if settled, err := s.settle(p); err != nil || !settled {
			return errors.Join(err, fmt.Errorf("%s while waiting for PUBREL %d", show(p), id))
		}
	}
}

// owedPubrel is the lowest packet identifier of a PUBREL s's session is
// owed, and whether there is one: for a delivery whose PUBREC 0x00 s's
// connection sent (MQTT-4.3.3-4), and for one past its PUBREL on an
// earlier connection, its PUBCOMP never sent, which needs no record
// (design.md, Words: Missed, "An exchange owed only its PUBREL needs no
// record. Its PUBREL is resent and nothing is counted (MQTT-4.4.0-1).").
func (s *slot) owedPubrel() (uint16, bool) {
	if s.subs == nil {
		return 0, false
	}
	for _, id := range slices.Sorted(maps.Keys(s.subs.outbound)) {
		x := s.subs.outbound[id]
		if (x.conn == s.conn && x.recd && !x.rel && !x.done) || (x.conn != s.conn && x.rel && !x.done) {
			return id, true
		}
	}
	return 0, false
}

// receiveUntilQuiet reads s's connection to quiet past a PINGRESP,
// acknowledging what comes where acknowledge says, the exchanges kept
// before included (completePubrels); then, on every eighth step, s sends a
// PUBREC naming no delivery (strayPubrec), whose PUBREL comes before the
// PINGRESP. A repeated PUBREC's PUBREL waits for the first's phase to be
// stored, and so may come after it (receipt).
func (s *slot) receiveUntilQuiet(acknowledge bool) error {
	s.incoming.auto = acknowledge
	defer func() { s.incoming.auto = false }()
	if acknowledge {
		if err := s.releaseAcks(); err != nil {
			return err
		}
		if err := s.completePubrels(true); err != nil {
			return err
		}
		if s.f.step%8 == 0 {
			if err := s.strayPubrec(); err != nil {
				return err
			}
		}
	}
	if err := pingPast(s); err != nil {
		return err
	}
	if len(s.incoming.strays) > 0 {
		return fmt.Errorf("%s: PUBREC %v, naming no delivery, got no PUBREL before the PINGRESP that followed it (design.md, Delivery, Positions: \"A PUBREC naming no outgoing QoS 2 exchange [...] gets PUBREL 0x92\")",
			s.id, slices.Sorted(maps.Keys(s.incoming.strays)))
	}
	return nil
}

// deliveryShape is the contribution of a complete subscription revision.
// Keeping complete sets prevents accepting an arbitrary mixture of old
// and new identifiers after a subscription change.
type deliveryShape struct {
	matched  bool
	localIDs []uint32
	ids      []uint32
	allowed  bool
	retain   bool
	qos      byte
	// multi: a contributor's filter reaches several channels; single: one
	// reaches one (saguinProperties).
	multi, single bool
}

func (s *slot) shape(subs []*subscription, m *message) deliveryShape {
	return s.shapeAt(subs, m, m.serial)
}

func (s *slot) shapeAt(subs []*subscription, m *message, published uint64) deliveryShape {
	return s.shapeOf(subs, m, false, published)
}

// shapeOf is shapeAt, the subscriptions' live-start boundaries ignored
// where every filter reads from a position (appends.go), partitions
// applying (design.md, SUBSCRIBE, One copy: "topic, live-start boundary,
// No Local, partition and permission checks apply").
func (s *slot) shapeOf(subs []*subscription, m *message, ignoreStart bool, published uint64) deliveryShape {
	var shape deliveryShape
	own := m.publisher == s.clientID && m.publisher != ""
	if m.hidden != 0 {
		own = m.hidden == s.hidden
	}
	for _, sub := range subs {
		if (!ignoreStart && sub.start >= published) || !matchesFilter(sub.Filter, m.topic) || !sub.allows(m.topic) {
			continue
		}
		shape.matched = true
		if sub.NoLocal && own {
			if sub.id != 0 {
				shape.localIDs = append(shape.localIDs, sub.id)
			}
			continue
		}
		shape.allowed = true
		shape.multi = shape.multi || len(sub.reach) > 1
		shape.single = shape.single || len(sub.reach) <= 1
		shape.qos = max(shape.qos, min(sub.QoS, m.qos))
		shape.retain = shape.retain || (s.conn.Version == harness.V5 && sub.RetainAsPublished && m.retain)
		if sub.id != 0 && s.conn.Version == harness.V5 {
			shape.ids = append(shape.ids, sub.id)
		}
	}
	return shape
}

// shapes considers complete subscription revisions at hand-off, and the
// possible intake boundaries of an unconfirmed publication. Starting every
// later matching subscription at once would invent contributors: a record
// can precede a newly added filter yet leave through an older filter whose
// options have since been replaced (SUBSCRIBE, The start point; A resend).
func (s *slot) shapes(m *message) []deliveryShape {
	if s.subs == nil {
		return nil
	}
	history := s.subs.history
	first := 0
	for first+1 < len(history) && history[first+1].at < m.serial {
		first++
	}
	upper := m.confirmed.Load()
	if upper == 0 {
		upper = s.f.serial + 1
	}
	shapes := make([]deliveryShape, 0, len(history)-first)
	for i := first; i < len(history); i++ {
		r := history[i]
		end := upper
		if i+1 < len(history) {
			end = min(end, history[i+1].at)
		}
		if end < m.serial {
			continue
		}
		cuts := []uint64{m.serial}
		for _, sub := range r.subs {
			if sub.start >= m.serial && sub.start < end {
				cuts = append(cuts, sub.start+1)
			}
		}
		slices.Sort(cuts)
		for _, cut := range slices.Compact(cuts) {
			shapes = append(shapes, s.shapeAt(r.subs, m, cut))
		}
	}
	return shapes
}

// checkBytes holds p's payload, byte for byte, to that of the message its
// tag names, as published: design.md, Store, Compression, "The core
// interprets only the payload as received. It carries any prepared stored
// copy opaquely to the store. The connection writers handle only the payload
// as received." (MQTT 5 3.3.3:
// "The content and format of the data is application specific"; the
// broker does not change it). A delivery naming no message is left to the
// checks that follow.
func (s *slot) checkBytes(p *harness.Publish) error {
	if s.f == nil {
		return nil
	}
	tag, _ := s.deliveryTag(p)
	m := s.f.publications[tag]
	if m == nil {
		return nil
	}
	if want := m.published(); !bytes.Equal(p.Payload, want) {
		at := 0
		for at < min(len(want), len(p.Payload)) && p.Payload[at] == want[at] {
			at++
		}
		return fmt.Errorf("%s: delivery %q (%s) of %d payload bytes, differing from the %d published at byte %d (design.md, Store, Compression: \"The connection writers handle only the payload as received\")",
			s.id, p.Topic, tag, len(p.Payload), len(want), at)
	}
	return nil
}

// deliveryTag is the tag of the message p delivers: a Will's, by its topic;
// a PUBLISH's, by its payload; a latest channel's tombstone, which carries
// no payload, by its first saguin-id, its tag (newPublish), else, over MQTT
// 3.1.1, by its topic (tombstoneOf); "" for none. sure is false where the
// tag is tombstoneOf's guess among several deletions.
func (s *slot) deliveryTag(p *harness.Publish) (tag string, sure bool) {
	if len(p.Payload) > 0 || willTagOf(p.Topic) != "" || !isLatest(route(p.Topic)) {
		tag, _, _ := strings.Cut(recordKey(p.Topic, p.Payload), " ")
		return tag, true
	}
	if ids := p.Props.User("saguin-id"); len(ids) > 0 {
		return ids[0], true
	}
	if d, sure := s.tombstoneOf(p); d != nil {
		return d.tag, sure
	}
	return "", true
}

// deliveryShapes are the contributions with which m may be delivered to s:
// from the subscriptions' live-start boundaries on $broadcast and a latest
// channel (SUBSCRIBE, The start point: "each subscription has a live-start
// boundary for every $broadcast or latest channel it reaches"), from its
// consumer positions on an append channel (appends.go), and, for a latest
// channel's value, by a pass of s's connection, or of any connection of its
// session where resent (latest.go), passes counting those last.
func (s *slot) deliveryShapes(m *message, resent bool) (shapes []deliveryShape, passes int) {
	switch {
	case isLatest(m.channel):
		pass := s.passShapes(m, resent)
		return append(s.shapes(m), pass...), len(pass)
	case m.channel != "" && m.channel != broadcast:
		return s.channelShapes(m), 0
	}
	return s.shapes(m), 0
}

func (s *slot) checkDelivery(p *harness.Publish) (err error) {
	// Every delivery, including a retained pass and a shared subscription,
	// must be a possible Will publication. A successful delivery is evidence
	// of that publication regardless of which subscription supplied it.
	if s.f != nil {
		if w := s.f.wills[willTagOf(p.Topic)]; w != nil {
			m := w.record()
			if m == nil || w.w.Topic != p.Topic {
				return fmt.Errorf("%s: delivery of %s, of which the model expects none: %s", s.id, w.describe(), w.unexpected())
			}
			at := s.f.willTime(time.Now())
			d := w.decision(at, s.f.limits[m.channel] != nil, w.uncertain)
			if d.dropped || d.denied || w.state == willCancelled {
				return fmt.Errorf("%s: delivery of %s, of which the model expects none: %s", s.id, w.describe(), w.unexpected())
			}
			if at.Before(w.notBefore) {
				return fmt.Errorf("%s: delivery of %s before its Will Delay of %v from the end of its connection at step %d (design.md, DISCONNECT)", s.id, w.describe(), w.delay, w.at)
			}
			defer func() {
				if err == nil {
					w.delivered.Store(true)
				}
			}()
		}
	}
	if err := s.checkBytes(p); err != nil {
		return err
	}
	if sh := s.sharedThrough(p); sh != nil {
		return s.checkShared(p, sh)
	}
	if s.subs != nil {
		if err := s.orphan(p); err != nil {
			return err
		}
	}
	// A latest channel's initial pass carries RETAIN 1 (design.md, SUBSCRIBE,
	// Options), so its deliveries are checked against the pass's predicted
	// shapes below rather than as retained values.
	retained := p.Retain && !isLatest(route(p.Topic)) && (s.conn.Version == harness.V311 || (p.Dup && s.subs != nil && s.subs.retainedPass))
	if retained {
		// Retained values are not predicted. MQTT 3.1.1 has no Retain Handling
		// option: only the filter match is checked on its RETAIN 1 deliveries,
		// and that its topic is $broadcast's, the only channel holding
		// retained values ("A retain flag on a configured channel topic is
		// ignored", design.md, Channels).
		if ch := route(p.Topic); ch != broadcast {
			return fmt.Errorf("%s: retained delivery %q, a topic of channel %s, where RETAIN is ignored", s.id, p.Topic, ch)
		}
		if s.subs != nil {
			reader := ""
			for _, r := range s.subs.history {
				for _, sub := range r.subs {
					if !matchesFilter(sub.Filter, p.Topic) {
						continue
					}
					if sub.allows(p.Topic) {
						return nil
					}
					reader = sub.reader
				}
			}
			if reader != "" {
				return fmt.Errorf("%s: retained delivery %q, a topic its user %s may not read: it is skipped, not owed (design.md, Access and reload)", s.id, p.Topic, reader)
			}
		}
		return fmt.Errorf("%s: retained delivery %q without a matching subscription", s.id, p.Topic)
	}
	// A Will is known by its topic's tag; a PUBLISH by its payload's; a
	// dead letter by its job's, the job's payload (queues.go).
	tag, sure := s.deliveryTag(p)
	var m *message
	if s.f != nil {
		m = s.f.publications[tag]
		if m != nil && isDeadLetter(route(p.Topic)) {
			m = s.f.publications[tag+deadLetterTag]
			tag += deadLetterTag
		}
	}
	if m == nil || m.topic != p.Topic {
		if len(p.Payload) == 0 && isLatest(route(p.Topic)) {
			return fmt.Errorf("%s: a tombstone of %q (saguin-id %q), which no deletion of it the run sent, and the connection has not had, a subscription takes at QoS %d (design.md, Channels, latest)",
				s.id, p.Topic, p.Props.User("saguin-id"), p.QoS)
		}
		return fmt.Errorf("%s: delivery %q (%q) without a matching subscription at publication", s.id, p.Topic, tag)
	}
	latest := isLatest(m.channel)
	if latest {
		if err := s.staleOnReturn(p, m); err != nil {
			return err
		}
	}
	onChannel := m.channel != "" && m.channel != broadcast
	// A delivery a latest pass may send is held to no live order: the pass
	// reads below its boundary while live changes go on (The start point).
	shapes, passes := s.deliveryShapes(m, p.Dup)
	live := passes == 0
	matched, allowed := false, false
	var ids []uint32
	for _, prop := range p.Props {
		if prop.ID == harness.SubscriptionIdentifier {
			ids = append(ids, prop.Int)
		}
	}
	for _, shape := range shapes {
		matched = matched || shape.matched
		allowed = allowed || shape.allowed
	}
	if !matched {
		for _, r := range s.subs.history {
			for _, sub := range r.subs {
				if matchesFilter(sub.Filter, p.Topic) && !may(sub.reader, p.Topic, false) {
					return fmt.Errorf("%s: delivery %q (%s) through %q, a topic its user %s may not read: it is skipped, not owed (design.md, Access and reload)",
						s.id, p.Topic, tag, sub.Filter, sub.reader)
				}
			}
		}
		for _, r := range s.subs.history {
			for _, sub := range r.subs {
				if part := sub.partition; part != nil && matchesFilter(sub.Filter, p.Topic) && !sub.allows(p.Topic) {
					return fmt.Errorf("%s: delivery %q (%s) through %q, whose saguin-filter partition, %d parts holding %v, excludes it: topic_hash is %d, part %d (design.md, SUBSCRIBE, Partitioning)",
						s.id, p.Topic, tag, sub.Filter, part.count, slices.Sorted(maps.Keys(part.indices)), harness.TopicHash(p.Topic), harness.TopicHash(p.Topic)%part.count)
				}
			}
		}
		return fmt.Errorf("%s: delivery %q (%s) without a matching subscription at publication", s.id, p.Topic, tag)
	}
	if !allowed {
		return fmt.Errorf("%s: No Local violated by delivery of its own publication %s", s.id, tag)
	}
	if s.incoming.seen == nil {
		s.incoming.seen, s.incoming.had = map[string]bool{}, map[string]bool{}
		s.incoming.last = map[string]uint64{}
	}
	if p.QoS > 0 && s.incoming.seen[tag] && !p.Dup {
		switch {
		case latest && s.takePass(m):
			// A pass sends again what the connection has had (latest.go).
		case s.incoming.grace[m.channel] > 0:
			s.incoming.grace[m.channel]--
		case s.repeatable(m):
			// A kept position read again sends it once more (appends.go).
		default:
			return fmt.Errorf("%s: second copy of %s at QoS %d while connected without DUP", s.id, tag, p.QoS)
		}
	}
	// Order holds within each channel, not across (design.md, Delivery,
	// Order), and not across a position made again (appends.go). Dead
	// letters enter their channel as their jobs move, in no publisher's
	// order: their offsets' order is checked (checkChannel). A tombstone
	// that tombstoneOf can only guess among several deletions is had as
	// its topic's (hadDeletion), never as the guess's, and moves no
	// publisher's mark.
	publisher := m.orderKey()
	ordered := !p.Dup && live && m.deadOf == nil
	if ordered && !s.incoming.unordered[m.channel] && m.serial < s.incoming.last[publisher] {
		return fmt.Errorf("%s: publisher's order broken: %s after a later publication from %s", s.id, tag, publisher)
	}
	// A forbidden No Local contributor must be diagnosed before its extra
	// identifier is called a Subscription Identifier error.
	validIDs, validRetain, validQoS := false, false, false
	var qoses []byte
	for _, shape := range shapes {
		if !shape.allowed {
			continue
		}
		idOK := slices.Equal(ids, shape.ids)
		validIDs = validIDs || idOK
		validRetain = validRetain || shape.retain == p.Retain
		validQoS = validQoS || shape.qos == p.QoS
		qoses = append(qoses, shape.qos)
	}
	// design.md, Flows, SUBSCRIBE, One copy: "The delivery uses the lower of
	// the publication's QoS and the highest contributing grant". A resend
	// keeps the QoS it first went out at (storage.md, session_inflight: qos
	// "the QoS it went out at"), which an earlier revision may have given.
	if !p.Dup && !validQoS {
		return fmt.Errorf("%s: delivery of %s at QoS %d; want the lower of its QoS %d and the highest contributing grant, one of %v (design.md, SUBSCRIBE, One copy)",
			s.id, tag, p.QoS, m.qos, qoses)
	}
	if !validIDs {
		for _, shape := range shapes {
			for _, id := range shape.localIDs {
				if countIdentifier(ids, id) > countIdentifier(shape.ids, id) {
					return fmt.Errorf("%s: No Local subscription contributed to its own publication %s (Subscription Identifier values %v)", s.id, tag, ids)
				}
			}
		}
		var expected [][]uint32
		for _, shape := range shapes {
			if shape.allowed {
				expected = append(expected, shape.ids)
			}
		}
		return fmt.Errorf("%s: Subscription Identifier values %v for %s do not match eligible subscriptions %v (DUP %v)", s.id, ids, tag, expected, p.Dup)
	}
	// design.md, Delivery, A resend: "The first-delivery RETAIN value is
	// preserved, but an MQTT 3.1.1 connection receives RETAIN 1 only on a
	// delivery of a subscription's initial pass". A first transmission, and
	// a resend over MQTT 3.1.1, carry the live value; a resend over MQTT 5,
	// its first transmission's.
	first, shown := s.subs.retain[tag]
	switch {
	case !p.Dup || s.conn.Version == harness.V311:
		if !validRetain {
			return fmt.Errorf("%s: Retain As Published violated for %s: RETAIN %v", s.id, tag, p.Retain)
		}
	case shown && p.Retain != first:
		return fmt.Errorf("%s: resend of %s with RETAIN %v, its first copy having RETAIN %v", s.id, tag, p.Retain, first)
	case !shown && !s.couldSendFirst(m, shapes, p.Retain):
		return fmt.Errorf("%s: resend of %s with RETAIN %v, which no earlier connection of its session could have given its first copy", s.id, tag, p.Retain)
	}
	if p.QoS > 0 && !shown && (!p.Dup || s.conn.Version == harness.V5) {
		s.subs.retain[tag] = p.Retain
	}
	switch {
	case onChannel:
		if err := s.checkChannel(p, m, shapes, live); err != nil {
			return err
		}
	case s.conn.Version == harness.V5:
		if err := s.noSaguin(p, m); err != nil {
			return err
		}
	}
	if sure {
		s.incoming.seen[m.tag], s.incoming.had[m.tag] = true, true
	}
	if latest {
		s.noteLatest(p, m, live)
	}
	if ordered && sure {
		s.incoming.last[publisher] = m.serial
	}
	return nil
}

// orderKey names m's publisher on m's channel, whose publications a
// connection receives in order (design.md, Delivery, Order).
func (m *message) orderKey() string {
	publisher := m.publisher
	if m.hidden != 0 {
		publisher = "anonymous-" + strconv.Itoa(m.hidden)
	}
	return publisher + " on " + m.channel
}

// couldSendFirst reports whether a connection of s's session before its
// own, from the one holding it when m was published, could have sent m's
// first transmission with RETAIN retain, unseen: over MQTT 3.1.1, 0; over
// MQTT 5, the value of a live delivery in one of shapes, which s's own
// connection, MQTT 5, has worked out.
func (s *slot) couldSendFirst(m *message, shapes []deliveryShape, retain bool) bool {
	holders := s.subs.holders
	if len(holders) > 0 {
		holders = holders[:len(holders)-1] // the last is s's own connection
	}
	first := 0
	for first+1 < len(holders) && holders[first+1].at < m.serial {
		first++
	}
	for _, h := range holders[first:] {
		switch {
		case h.version == harness.V311 && !retain:
			return true
		case h.version == harness.V5 && slices.ContainsFunc(shapes, func(shape deliveryShape) bool {
			return shape.allowed && shape.retain == retain
		}):
			return true
		}
	}
	return false
}

// owed selects only acknowledged QoS 1 publications whose SUBACK preceded
// the PUBACK in step order and whose connection and at least one eligible
// subscription continued throughout. Replacing options keeps a filter's
// live boundary; removing and adding it creates a new one. A latest
// channel's change is owed as latestOwed says. A queue's record is never
// owed to its readers: its retention moves or deletes it at any time, and
// so does a move of its job (queues.go); its dead letter, once found, is
// owed only where the connection also preceded its move.
func (s *slot) owed(m *message) bool {
	if s.incoming.seen[m.tag] || m.deleted || m.job != nil {
		return false
	}
	if w := m.will; w != nil && !w.observed() && (w.dropped || w.denied || w.mayDrop || w.uncertain) {
		return false
	}
	if m.logged {
		return s.latestOwed(m)
	}
	if s.f.retention[m.channel] != nil && s.f.dbPath == "" {
		// Without the database, what retention deleted is unknown
		// (retention.go), and so is what the channel still owes.
		return false
	}
	if s.replayOwed(m) {
		return s.fitsAll(m)
	}
	if s.incoming.had[m.tag] {
		// Had on this connection, then made repeatable by a seek, which
		// owes nothing of what it replays (seeks.go).
		return false
	}
	ackStep := m.ackStep
	if j := m.deadOf; j != nil {
		// Discovery confirms existence, not creation on this connection.
		// design.md, Delivery: "QoS 0 settles on successful writing";
		// SUBSCRIBE: "A SUBSCRIBE repeating an existing filter replaces
		// its options and keeps its position and live-start boundaries".
		// An older dead letter may already be behind the resumed position.
		// Its move must follow this connection: either its original job
		// was published hereafter, or the record's move time proves it.
		r := m.stored.Load()
		if r == nil || (j.m.step < s.incoming.since && r.time <= s.incoming.established.Add(queueMargin).UnixMilli()) {
			return false
		}
		ackStep = j.foundStep
	}
	if (!m.fateIs(stored) && !m.fateIs(takenWill)) || m.qos == 0 || s.incoming.since > ackStep {
		return false
	}
	if w := m.will; w != nil {
		// A Will keeps its obligations until publication is established and
		// every subscriber is checked. Passing an uncertain Will at an
		// earlier checkpoint does not settle it. A delayed Will is first
		// owed once its latest due time has passed (wills.go).
		if w.checked || (!w.owedAfter.IsZero() && s.f.checkAt.Before(w.owedAfter)) {
			return false
		}
	} else if ackStep <= s.f.deliveryCutoff {
		return false
	}
	return s.continuous(m, ackStep) && s.fitsAll(m)
}

// continuous reports whether a subscription of s's session eligible for m
// when it was published, SUBACKed before its acknowledgement, has been held
// since with the same live-start boundary, allowing it in every revision.
func (s *slot) continuous(m *message, ackStep int) bool {
	var contributors []*subscription
	var original []*subscription
	for _, r := range s.subs.history {
		if r.at >= m.serial {
			break
		}
		original = r.subs
	}
	for _, sub := range original {
		current := s.subs.current[sub.Filter]
		if current == nil || current.start != sub.start || sub.since >= ackStep {
			continue
		}
		continuous := true
		for _, r := range s.subs.history {
			if r.at < m.serial {
				continue
			}
			for _, later := range r.subs {
				if later.Filter == sub.Filter && !s.shape([]*subscription{later}, m).allowed {
					continuous = false
				}
			}
		}
		if continuous {
			contributors = append(contributors, sub)
		}
	}
	slices.SortFunc(contributors, func(a, b *subscription) int { return strings.Compare(a.Filter, b.Filter) })
	return s.shape(contributors, m).allowed
}

// fitsAll reports whether every delivery of m the subscription revisions
// permit fits s's Maximum Packet Size: "A message over the client's
// Maximum Packet Size is discarded for that subscriber as if sent"
// (design.md, Delivery; MQTT-3.1.2-25), its size counting the Subscription
// Identifiers of the subscriptions eligible at hand-off, and the Sagüin
// properties of a channel's. The continuous contributors are only some of
// those: a filter whose options have since changed may still have
// contributed its identifier. So the message is owed only if every
// delivery the revisions permit (those checkDelivery accepts) fits.
func (s *slot) fitsAll(m *message) bool {
	limit := s.conn.MaxPacketSize
	if limit == 0 {
		return true
	}
	shapes, _ := s.deliveryShapes(m, false)
	for _, shape := range shapes {
		if shape.allowed && !s.fits(m, shape, limit) {
			return false
		}
	}
	return true
}

// fits reports whether m, delivered to s in shape, keeps within limit, s's
// Maximum Packet Size. Encoding is the harness's independent wire codec:
// the original properties, the identifiers, and a Topic Alias where the
// connection permits one (its first use carries the topic as well).
func (s *slot) fits(m *message, shape deliveryShape, limit int) bool {
	p := &harness.Publish{QoS: shape.qos, Topic: m.topic, ID: 1, Props: slices.Clone(m.props)}
	for _, id := range shape.ids {
		p.Props = append(p.Props, harness.Property{ID: harness.SubscriptionIdentifier, Int: id})
	}
	if s.conn.TopicAliasMaximum > 0 {
		p.Props = append(p.Props, harness.Property{ID: harness.TopicAlias, Int: 1})
	}
	if m.channel != "" && m.channel != broadcast && s.conn.Version == harness.V5 {
		// The Sagüin properties at their longest: a saguin-id of its own
		// or a UUIDv7, an offset of 19 digits, times of 13.
		p.Props = append(p.Props, harness.User("saguin-id", strings.Repeat("i", max(36, len(m.id)))),
			harness.User("saguin-offset", strings.Repeat("9", 19)), harness.User("saguin-timestamp", strings.Repeat("9", 13)),
			harness.User("saguin-channel", m.channel))
		if m.expiring {
			p.Props = append(p.Props, harness.User("saguin-expires", strings.Repeat("9", 13)))
		}
		if j := m.deadOf; j != nil {
			// A dead letter's own, at their longest (queues.go).
			p.Props = append(p.Props, harness.User("saguin-dlq-reason", "attempts_exhausted"), harness.User("saguin-dlq-channel", j.queue),
				harness.User("saguin-dlq-offset", strings.Repeat("9", 19)), harness.User("saguin-dlq-attempts", strconv.Itoa(queueOf(j.queue).maxAttempts)),
				harness.User("saguin-dlq-at", strings.Repeat("9", 13)), harness.User("saguin-dlq-first", strings.Repeat("9", 13)),
				harness.User("saguin-dlq-last", strings.Repeat("9", 13)))
		}
	}
	wire, err := harness.Encode(p, s.conn.Version)
	if err != nil {
		return false
	}
	// The payload, of which only its length is kept, adds to the
	// Remaining Length, whose own encoding may then grow.
	remaining, err := harness.ReadVarInt(bytes.NewReader(wire[1:]))
	remaining += uint32(m.length)
	return err == nil && 1+len(harness.AppendVarInt(nil, remaining))+int(remaining) <= limit
}

// checkDeliveries releases all withheld acknowledgements before waiting for
// rule 4. A PINGRESP is only an input barrier: outstanding delivery
// preparation can finish later, so missing owed messages are read until
// they arrive, until the broker is silent for -timeout, or until it has
// sent more than it can owe (read). No clock bounds the whole wait: an
// owed message may come behind a retained pass as long as the run has
// made it (design.md, Delivery, Preparation).
func (f *fuzzer) checkDeliveries() error {
	f.checkAt = time.Now()
	f.observeWills(f.checkAt)
	for _, s := range f.slots {
		if s.conn == nil {
			continue
		}
		s.incoming.auto = true
		if err := s.releaseAcks(); err != nil {
			return err
		}
		if err := s.completePubrels(true); err != nil {
			return err
		}
	}
	defer func() {
		for _, s := range f.slots {
			s.incoming.auto = false
		}
	}()
	publications := maps.Values(f.publications)
	for {
		for _, s := range f.slots {
			if s.conn == nil {
				continue
			}
			if err := pingPast(s); err != nil {
				return err
			}
			missing := make([]*message, 0)
			for m := range publications {
				if m.logged && s.latestOwed(m) {
					f.latest.due++
				}
				if s.owed(m) {
					missing = append(missing, m)
					if m.logged {
						f.latest.awaited++
					}
				}
			}
			slices.SortFunc(missing, func(a, b *message) int { return strings.Compare(a.tag, b.tag) })
			if err := s.awaitOwed(missing, "at checkpoint"); err != nil {
				return err
			}
			if err := s.awaitPubrels(); err != nil {
				return err
			}
		}
		// A read may prove an uncertain Will published. Reconcile that
		// evidence on the driver and check its new obligations against every
		// subscriber, including those already visited. Each further pass
		// consumes new obligations; no unchanged decision starts another.
		owed := f.observeWills(f.checkAt)
		if len(owed) == 0 {
			break
		}
		publications = slices.Values(owed)
	}
	f.deliveryCutoff = f.step
	// A position's history was owed at this checkpoint, on its connection.
	for _, s := range f.slots {
		if s.conn != nil && s.subs != nil {
			for _, es := range s.subs.positions {
				for _, e := range es {
					e.checked = e.checked || e.conn == s.conn
				}
			}
		}
	}
	for _, w := range f.wills {
		w.checked = w.checked || (w.m != nil && w.m.fateIs(takenWill) && (w.owedAfter.IsZero() || !f.checkAt.Before(w.owedAfter)))
	}
	f.prunePublications()
	return nil
}

// awaitOwed reads s's deliveries until each of missing, which s owes
// (owed), has come, with its acknowledgements released: the broker silent
// for -timeout first, or a control packet but a PUBACK, fails. when says
// where, for messages.
func (s *slot) awaitOwed(missing []*message, when string) error {
	s.incoming.flush = true
	defer func() { s.incoming.flush = false }()
	if err := s.completePubrels(true); err != nil {
		return err
	}
	for _, m := range missing {
		for !s.incoming.seen[m.tag] && !s.hadDeletion(m) {
			p, err := s.read()
			if err != nil {
				if !harness.IsTimeout(err) {
					return err
				}
				// Retention may have deleted it since the record check
				// found it held.
				if gone, goneErr := s.f.goneByRetention(m); gone || goneErr != nil {
					if goneErr != nil {
						return goneErr
					}
					break
				}
				return fmt.Errorf("%s: publication %s on %q still owed %s: %w", s.id, m.tag, m.topic, when, err)
			}
			if received(p) {
				continue
			}
			if settled, err := s.settle(p); err != nil || !settled {
				return errors.Join(err, fmt.Errorf("%s while waiting for owed publication %s", show(p), m.tag))
			}
		}
	}
	return nil
}

func countIdentifier(ids []uint32, id uint32) int {
	n := 0
	for _, v := range ids {
		if v == id {
			n++
		}
	}
	return n
}

// No future live subscription can refer to a publication older than every
// surviving session's first revision. Retained passes check only filters.
// Connection churn therefore releases publication metadata, while the
// record oracle independently retains any record it has not found yet.
func (f *fuzzer) prunePublications() {
	oldest := f.serial + 1
	var sets []*subscriptions
	for _, s := range f.slots {
		if s.saved != nil {
			sets = append(sets, s.saved)
		}
		if s.conn != nil && s.subs != nil {
			sets = append(sets, s.subs)
		}
	}
	for _, set := range sets {
		if len(set.history) > 0 {
			oldest = min(oldest, set.history[0].at)
		}
	}
	oldest = min(oldest, f.shares.firstHeld(sets))
	for tag, m := range f.publications {
		// A delayed Will enters its channel at its due time, or at a clean
		// start ending its session, long after the serial its connection's
		// end gave it: a subscription made in between may receive it, so it
		// is kept.
		// A record of an append channel may come again with any new
		// position made at the floor, its properties sizing it (fits).
		switch {
		case m.channel != "" && m.channel != broadcast:
		case m.serial < oldest && (m.will == nil || m.will.delay == 0):
			delete(f.publications, tag)
		default:
			m.props = nil
		}
	}
	// mostDeliveries counts each publication of the registry once: a
	// connection's count starts again with the registry it is held to.
	for _, s := range f.slots {
		s.incoming.delivered = 0
	}
	// A delivery of a publication released here fails as unknown, so its
	// first RETAIN value is released with it.
	for _, set := range sets {
		maps.DeleteFunc(set.retain, func(tag string, _ bool) bool { return f.publications[tag] == nil })
	}
	f.shares.forget(f.publications)
}
