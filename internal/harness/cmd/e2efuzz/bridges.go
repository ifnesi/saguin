package main

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The run's bridge (design.md, Bridges): a broker the run started holds one
// bridge, to a peer the run serves on loopback (harness.Peer), which plays
// the peer broker on a goroutine of its own and checks every packet the
// bridge sends it against the model of the link. Each link's first packet
// is the bridge's CONNECT: MQTT 5, Clean Start 0, its client id, Keep Alive
// 30, the configured Session Expiry and Receive Maximum and Maximum Packet
// Size max_packet_size ("A bridge uses MQTT 5, Clean Start 0, its configured
// client id, Session Expiry and Receive Maximum. Its CONNECT also carries
// Maximum Packet Size"; "A bridge advertises Keep Alive 30 seconds"). The
// peer accepts it, and the next packet is one SUBSCRIBE carrying every
// inbound filter, each at its rule's QoS, with No Local, Retain As
// Published 1 and Retain Handling 1 ("Every new link subscribes all its
// filters afresh"). The peer answers PINGREQs. Now and then the run drops
// the link, and the bridge must dial again at once ("The first redial after
// failed establishment or link loss is immediate"); or it drops it with the
// peer set to refuse some of the filters of the next SUBSCRIBE, and then
// the link's next SUBSCRIBE must ask for those alone, after the redial wait
// of the bridge's consecutive refusals ("If the peer refuses filters, the
// bridge asks again for those filters alone, in as few SUBSCRIBEs as fit
// the peer's Maximum Packet Size, on the same link after the redial wait
// above. There, n counts the bridge's consecutive refusals";
// "Subscription-refusal retries have a separate counter. It resets once
// every filter a link asks for is granted, after one SUBACK or after
// several"); a retry the peer refuses in part
// again now and then. At SIGTERM, the link up must get DISCONNECT 0x00
// ("A bridge whose link is up sends DISCONNECT 0x00"). Now and then the
// peer forwards records to the bridge (forwardBridge), at every QoS up to
// its rule's, now and then retained, with saguin-id or other User
// Properties: each lands on its rule's topic through the ordinary publish
// path as no user, the acl_file never applying, and the model follows it
// as it follows a client's PUBLISH, its record (an empty publisher and the
// bridge as origin, checkRecords) and what each subscriber is owed of it
// (checkDeliveries). A record matching no inbound rule, one whose topic
// breaks a limit once its rule's template is applied, and one carrying a
// saguin- property but saguin-id are refused, 0x90, 0x90 and 0x83, a QoS 0
// one dropped. Now and then a client publishes records an out rule takes
// to the peer (crossBridge), whose CONNACK caps what the bridge may send
// it, its Retain Available drawn for each link: the peer checks every
// PUBLISH the bridge sends against the record it carries (crossedIn), and
// answers it with PUBACK 0x00, at QoS 2 PUBREC 0x00 and then PUBCOMP 0x00
// to the bridge's PUBREL (releasedIn), a refusal, or, for a while,
// nothing. At each checkpoint every record an out rule owes the peer has
// reached it, in each rule's order per channel and topic, or is one the
// bridge must skip (settle). Once the run has stopped the broker, with
// exchanges unfinished, it starts it again on its files (restart): the
// peer serves the new link as one after a drop, and every record owed
// reaches it before that broker stops too.

// bridgeName and bridgeClientID are the run's bridge and its client id at
// the peer.
const (
	bridgeName     = "fuzzpeer"
	bridgeClientID = "saguin-e2efuzz"
)

// bridgeRules are the bridge's rules, all in: their filters do not overlap
// (config.md, bridges.<name>: "A bridge's inbound filters may not
// overlap"), and each asks a QoS of its own. peer/a/#'s topic adds a level
// and six bytes to what the peer sends, so a topic within the limits at the
// peer may break one here (design.md, Bridges: "Topic limits are checked
// after substitution"); peer/b/+
// lands on the configured channels ca and cab ("The receiving broker's own
// channels decide where a record lands"), the others on $broadcast.
var bridgeRules = []harness.BridgeRule{
	{Filter: "peer/a/#", Topic: "bridged/a/in/$#", Direction: "in", QoS: 2},
	{Filter: "peer/b/+", Topic: "ca/b/$1", Direction: "in", QoS: 1},
	{Filter: "peer/c", Topic: "bridged/c", Direction: "in", QoS: 0},
}

// Bridge timing the peer checks from its own receipt times: a redial or a
// retry may come room after its wait, and early before it, the peer's
// clock reading a send before the broker takes it.
const (
	bridgeRoom  = time.Second
	bridgeEarly = 50 * time.Millisecond
)

// bridgeWait is the most a retry after consecutive refusal n, from 1, waits
// (design.md, Bridges: "min(60,000, 5,000 × 2^(n−1))" milliseconds).
func bridgeWait(n int) time.Duration {
	return min(time.Minute, 5*time.Second<<min(n-1, 4))
}

// bridging is the run's bridge peer and its model of the link.
type bridging struct {
	peer *harness.Peer
	tls  *harness.BridgeTLS // the bridge's tls block, over the scheme the seed drew (bridge_transport.go)
	// rng draws the peer's answers on its own goroutine, apart from the
	// run's generator, so the clients' draws stay as they were.
	rng    *rand.Rand
	quit   chan struct{}
	served sync.WaitGroup

	mu sync.Mutex // guards what follows: the peer's goroutine and the run's steps share it
	// link is the link up, its CONNACK sent; nil while none is.
	link *harness.PeerLink
	// dropped is when the run last dropped the link, zero once the redial
	// came; refuseNext has the next SUBSCRIBE refuse some of its filters.
	dropped    time.Time
	refuseNext bool
	// awaiting is the filters a refusal left to be asked again on the
	// link, sorted, refusedAt when that SUBACK was sent, and refusals the
	// bridge's consecutive refusals (n).
	awaiting  []string
	refusedAt time.Time
	refusals  int
	// stopping is when the run sent SIGTERM, zero before; upAtStop: a link
	// was up then; disconnected: it sent DISCONNECT 0x00.
	stopping               time.Time
	upAtStop, disconnected bool
	failure                error
	stats                  bridgeStats
	// acks hands the step forwarding records (forwardBridge) each
	// PUBACK, PUBREC and PUBCOMP the bridge sends, in order; forwarded is
	// the packet identifier the peer last used.
	acks      chan *harness.Ack
	forwarded uint16
	// crossings are the records the run's clients sent that an out rule
	// takes to the peer, by payload tag (cross); lanes, the same in the
	// order they were sent, by rule, channel and topic at the peer, the
	// order the peer must take them in; open, those not yet settled.
	crossings map[string]*crossed
	lanes     map[string][]*crossed
	open      []*crossed
	// inflight are the QoS 1 and 2 records the peer has received on the
	// link up and not settled, by packet identifier; carried, on a link
	// whose CONNACK had Session Present 1, those of earlier links that have
	// not come again; completed, the QoS 2 ones whose PUBCOMP the peer sent,
	// which an ended link may have lost, until the bridge uses their packet
	// identifiers again or a CONNACK has Session Present 0; withheld, those
	// of inflight whose answer the peer holds back for now. settling: a
	// checkpoint awaits every record owed, so the peer holds back none.
	inflight, carried, completed map[uint16]*crossed
	withheld                     []*crossed
	settling                     bool
	// holding: the run's last step has the peer hold back its answers
	// (unfinishedCrossings), until the restart.
	holding bool
	// sendings counts the bridge's QoS 1 and 2 sendings that were not
	// resends (crossed.sent); lastResent is the sent of the exchange last
	// taken up again on the link up, 0 before one (resentInOrder).
	sendings, lastResent int
	// sessionPresent and retainAvailable are what the link up's CONNACK
	// said; unretainedDrops counts the links the run dropped whose CONNACK
	// had Retain Available 0.
	sessionPresent, retainAvailable bool
	unretainedDrops                 int
	// replayOwed: a CONNACK with Session Present 0 started a replay whose
	// mark may still be stored; it lasts until the bridge's first SUBSCRIBE
	// on a link, which waits for the mark to clear (bridgeConnect).
	replayOwed bool
}

// bridgeStats counts what the bridge's peer saw, for the run's last lines:
// besides the link's life, the records the peer forwarded, those the
// bridge acknowledged stored, and those it refused.
type bridgeStats struct {
	links, drops, subscribes, retries, refused, pings int
	records, stored, refusedRecords                   int
	// crossings counts the records sent that an out rule takes, skipped
	// those of them the bridge must skip; crossed, those the peer took,
	// refusedOut those it refused, withheldOut the answers it held back,
	// and resent the records that came again under their own packet
	// identifiers on a link whose CONNACK had Session Present 1; releases
	// the PUBRELs the bridge sent, resentReleases those of exchanges of
	// earlier links; restarts the broker's starts after the run stopped it.
	crossings, skipped, crossed, refusedOut, withheldOut, resent int
	releases, resentReleases, restarts                           int
}

// bridgeAcks bounds the acknowledgements waiting for the step that reads
// them: a step's at most, a PUBACK or PUBREC and a PUBCOMP for each record,
// and more, so a stray one waits for the next step to find it.
const bridgeAcks = 64

// newBridging listens for the bridge, its answers drawn from seed.
func newBridging(seed uint64, dir string) (*bridging, error) {
	p, block, err := listenBridgePeer(seed, dir)
	if err != nil {
		return nil, fmt.Errorf("listening for the bridge: %w", err)
	}
	return &bridging{peer: p, tls: block, rng: rand.New(rand.NewPCG(^seed, seed)), quit: make(chan struct{}), acks: make(chan *harness.Ack, bridgeAcks),
		crossings: map[string]*crossed{}, lanes: map[string][]*crossed{}, inflight: map[uint16]*crossed{}, completed: map[uint16]*crossed{}}, nil
}

// bridge is the run's bridge, for the broker's configuration.
func (b *bridging) bridge() harness.Bridge {
	return harness.Bridge{Name: bridgeName, Peer: b.peer.URL(), ClientID: bridgeClientID, TLS: b.tls, Topics: slices.Concat(bridgeRules, outRules)}
}

// start serves the bridge's links, one after another.
func (b *bridging) start() {
	b.served.Go(func() {
		for {
			l, err := b.peer.Next(100 * time.Millisecond)
			if err == nil {
				b.serve(l)
				continue
			}
			select {
			case <-b.quit:
				return
			default:
			}
		}
	})
}

// close stops serving and closes the peer, its links dropped; a broker
// still running dials in vain from then on.
func (b *bridging) close() {
	select {
	case <-b.quit:
		return
	default:
	}
	close(b.quit)
	_ = b.peer.Close() // what the links still held no longer matters
	b.served.Wait()
}

// fail records the first check that failed.
func (b *bridging) fail(format string, args ...any) {
	if b.failure == nil {
		b.failure = fmt.Errorf("bridge: "+format, args...)
	}
}

// check returns the first check that failed, and an error where a retry
// the model awaits is overdue at now: the run's checkpoints call it.
func (b *bridging) check(now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.awaiting) > 0 && now.Sub(b.refusedAt) > bridgeWait(b.refusals)+bridgeRoom {
		b.fail("%v after refusal %d of %v, the bridge has not asked for them again (design.md, Bridges)", now.Sub(b.refusedAt), b.refusals, b.awaiting)
	}
	return b.failure
}

// serve plays the peer on link l until it ends, checking every packet.
func (b *bridging) serve(l *harness.PeerLink) {
	l.AnswerPings(true)
	b.mu.Lock()
	b.stats.links++
	if !b.dropped.IsZero() {
		if took := l.Accepted.Sub(b.dropped); took > bridgeRoom {
			b.fail("the redial came %v after the link was dropped; want it at once", took)
		}
		b.dropped = time.Time{}
	}
	first := b.stats.links == 1
	b.mu.Unlock()
	a, err := l.Next(time.Hour)
	if err != nil || a.Packet == nil {
		b.ended(l, a, err)
		return
	}
	c, ok := a.Packet.(*harness.Connect)
	b.mu.Lock()
	if !ok {
		b.fail("the link's first packet is %s; want CONNECT", a)
	} else if err := bridgeConnect(c, b.replayOwed); err != nil {
		b.fail("%v", err)
	}
	b.mu.Unlock()
	// The peer keeps the bridge's session after the first link, or says it
	// lost it (design.md, Bridges: "When the peer's CONNACK has Session
	// Present 0, the bridge discards its in-flight state"). It caps what
	// the bridge sends it, now and then allowing no retained message.
	present := !first && b.rng.IntN(2) == 0
	retain := b.rng.IntN(4) != 0
	props := harness.Properties{{ID: harness.ReceiveMaximum, Int: peerReceiveMaximum}, {ID: harness.MaximumPacketSize, Int: peerPacketSize}}
	if !retain {
		props = append(props, harness.Property{ID: harness.RetainAvailable, Int: 0})
	}
	if _, err := l.Send(&harness.Connack{SessionPresent: present, Props: props}); err != nil {
		return // the link has ended; its end arrives next
	}
	b.mu.Lock()
	b.link = l
	b.awaiting = nil // every new link subscribes all its filters afresh
	b.relink(present, retain)
	b.replayOwed = b.replayOwed || !present
	b.mu.Unlock()
	fresh := true
	for {
		a, err := l.Next(time.Hour)
		if err != nil || a.Packet == nil {
			b.ended(l, a, err)
			return
		}
		switch p := a.Packet.(type) {
		case *harness.Pingreq:
			b.mu.Lock()
			b.stats.pings++
			b.mu.Unlock()
		case *harness.Subscribe:
			b.subscribed(l, p, a, fresh)
			fresh = false
		case *harness.Ack:
			if p.Kind == harness.PUBREL {
				b.mu.Lock()
				b.releasedIn(l, p)
				b.mu.Unlock()
				continue
			}
			select {
			case b.acks <- p: // forwardBridge reads it
			case <-b.quit:
				return
			}
		case *harness.Publish:
			b.mu.Lock()
			b.crossedIn(l, p)
			b.mu.Unlock()
		case *harness.Disconnect:
			b.mu.Lock()
			if b.stopping.IsZero() || p.Reason != 0x00 {
				b.fail("DISCONNECT 0x%02X on a link up, SIGTERM sent %v; want DISCONNECT 0x00 at the stop alone", p.Reason, !b.stopping.IsZero())
			}
			b.disconnected = true
			b.mu.Unlock()
		default:
			b.mu.Lock()
			b.fail("the peer received %s on a link up; want SUBSCRIBE, PINGREQ, PUBLISH, an acknowledgement or the stop's DISCONNECT", a)
			b.mu.Unlock()
		}
	}
}

// bridgeConnect checks the bridge's CONNECT against its configuration: the
// defaults of config.md, bridges.<name>, session_expiry 1d and
// receive_maximum 20, and the run's max_packet_size. Its Clean Start is 0,
// except while replayOwed: design.md, Bridges, "Before the replay starts,
// the bridge stores that a replay is owed. The replay's last step clears
// that mark in the same transaction. Until the mark is cleared, the bridge
// neither subscribes at the peer nor sends it a record. Every CONNECT it
// makes until then has Clean Start 1". A link that ends before the replay
// commits is redialled with Clean Start 1; one that ends after it, before
// its SUBSCRIBE arrives, with 0. The peer cannot tell the two apart.
func bridgeConnect(c *harness.Connect, replayOwed bool) error {
	var errs []error
	if c.Version != harness.V5 || (c.CleanStart && !replayOwed) || c.ClientID != bridgeClientID || c.KeepAlive != 30 || c.Will != nil || c.HasUsername || c.HasPassword {
		errs = append(errs, fmt.Errorf("CONNECT %+v, a replay owed %v; want MQTT 5, Clean Start 0 unless a replay is owed, client id %s, Keep Alive 30, no Will and no credential", c, replayOwed, bridgeClientID))
	}
	for _, p := range []struct {
		id   byte
		want uint32
	}{{harness.SessionExpiryInterval, 86_400}, {harness.ReceiveMaximum, 20}, {harness.MaximumPacketSize, maxPacketSize}} {
		if got, ok := c.Props.Get(p.id); !ok || got.Int != p.want {
			errs = append(errs, fmt.Errorf("CONNECT property 0x%02X is %d (present %v); want %d", p.id, got.Int, ok, p.want))
		}
	}
	return errors.Join(errs...)
}

// subscribed checks s, arrived as a, against the model: a fresh link's
// first SUBSCRIBE carries every inbound filter; any later one the filters
// the last refusal left, once its wait has passed. The peer answers it,
// refusing some filters where refuseNext asks it to, and now and then a
// retried one again.
func (b *bridging) subscribed(l *harness.PeerLink, s *harness.Subscribe, a harness.Arrival, fresh bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.subscribes++
	b.replayOwed = false // the replay's mark has cleared before any SUBSCRIBE
	want := b.awaiting
	if fresh {
		want = make([]string, len(bridgeRules))
		for i, r := range bridgeRules {
			want[i] = r.Filter
		}
		slices.Sort(want)
	} else {
		b.stats.retries++
		if waited := a.At.Sub(b.refusedAt); waited < time.Second-bridgeEarly || waited > bridgeWait(b.refusals)+bridgeRoom {
			b.fail("a SUBSCRIBE came %v after refusal %d; want 1s to %v", waited, b.refusals, bridgeWait(b.refusals))
		}
	}
	got := make([]string, len(s.Subs))
	for i, sub := range s.Subs {
		got[i] = sub.Filter
		rule := slices.IndexFunc(bridgeRules, func(r harness.BridgeRule) bool { return r.Filter == sub.Filter })
		if rule < 0 || sub.QoS != bridgeRules[rule].QoS || !sub.NoLocal || !sub.RetainAsPublished || sub.RetainHandling != 1 {
			b.fail("SUBSCRIBE asks %+v; want an inbound filter at its rule's QoS with No Local, Retain As Published 1 and Retain Handling 1", sub)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		b.fail("SUBSCRIBE (fresh link %v) asks %v; want %v", fresh, got, want)
	}
	ack := &harness.Suback{ID: s.ID, Reasons: make([]byte, len(s.Subs))}
	var refused []string
	for i, sub := range s.Subs {
		ack.Reasons[i] = sub.QoS
		refuse := false
		switch {
		case b.refuseNext:
			// At least one: the last where none before it was.
			refuse = b.rng.IntN(2) == 0 || (i == len(s.Subs)-1 && len(refused) == 0)
		case !fresh:
			refuse = b.rng.IntN(4) == 0
		}
		if refuse {
			ack.Reasons[i] = 0x87
			refused = append(refused, sub.Filter)
		}
	}
	b.refuseNext = false
	sent, err := l.Send(ack)
	if err != nil {
		return // the link has ended; its end arrives next
	}
	slices.Sort(refused)
	b.awaiting, b.refusedAt = refused, sent
	if len(refused) == 0 {
		b.refusals = 0
		return
	}
	b.refusals++
	b.stats.refused += len(refused)
}

// ended follows the end of link l, a its end or err why nothing more
// arrives: the link is down, and nothing more is awaited on it.
func (b *bridging) ended(l *harness.PeerLink, a harness.Arrival, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.link == l {
		b.link = nil
	}
	b.awaiting = nil
	if err == nil && a.Packet == nil && b.dropped.IsZero() && b.stopping.IsZero() {
		b.fail("the bridge ended its link (%v), neither dropped by the peer nor stopped", a.Err)
	}
}

// up waits, for at most limit, until a link other than former is up, and
// returns it.
func (b *bridging) up(former *harness.PeerLink, limit time.Duration) (*harness.PeerLink, error) {
	var l *harness.PeerLink
	var failure error
	testkit.Within(limit, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		l, failure = b.link, b.failure
		return (l != nil && l != former) || failure != nil
	})
	switch {
	case failure != nil:
		return nil, failure
	case l == nil || l == former:
		return nil, fmt.Errorf("bridge: no link up within %v", limit)
	}
	return l, nil
}

// dropBridge: the peer drops the bridge's link, once one is up, and the run
// waits until the redial's link is up again (bridging.serve checks it came
// at once).
func (f *fuzzer) dropBridge() (bool, error) {
	return f.bridgeDrop(false)
}

// refuseBridge: the peer drops the bridge's link with its next SUBSCRIBE set
// to refuse some of the filters, which the bridge must then ask for again
// alone (bridging.subscribed).
func (f *fuzzer) refuseBridge() (bool, error) {
	return f.bridgeDrop(true)
}

// bridgeDrop drops the link up, refusing filters next where refuse says so,
// and waits for the next link up.
func (f *fuzzer) bridgeDrop(refuse bool) (bool, error) {
	b := f.bridge
	if b == nil {
		return false, nil
	}
	l, err := b.up(nil, f.timeout)
	if err != nil {
		return true, err
	}
	f.note("bridge: the peer drops the link, refusing filters next %v: want a redial at once, its SUBSCRIBE of every filter", refuse)
	b.mu.Lock()
	b.dropped, b.refuseNext = time.Now(), refuse
	b.stats.drops++
	if !b.retainAvailable {
		b.unretainedDrops++
	}
	b.mu.Unlock()
	_ = l.Close() // the drop itself: its end reaches the peer's goroutine
	if _, err := b.up(l, f.timeout); err != nil {
		return true, err
	}
	return true, b.check(time.Now())
}

// interruptions counts the links the run dropped and the restarts, each of
// which ended the link up.
func (b *bridging) interruptions() int {
	return b.stats.drops + b.stats.restarts
}

// stop is the run sending SIGTERM: whether a link is up then.
func (b *bridging) stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopping = time.Now()
	b.upAtStop = b.link != nil
	b.disconnected = false
}

// restarting is the run starting the broker again once it has stopped: its
// stop ended the link as a drop does, and the restarted broker is not
// stopping.
func (b *bridging) restarting() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.restarts++
	if !b.retainAvailable {
		b.unretainedDrops++
	}
	b.stopping, b.upAtStop, b.holding = time.Time{}, false, false
}

// stopped checks the bridge once the broker has exited: a link up at
// SIGTERM sent DISCONNECT 0x00 (design.md, Restart, Graceful stop), and
// every check held. The peer serves on, for a restart.
func (b *bridging) stopped() error {
	err := b.check(b.stopping)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.upAtStop && !b.disconnected {
		err = errors.Join(err, errors.New("bridge: the link up at SIGTERM received no DISCONNECT 0x00 (design.md, Restart, Graceful stop)"))
	}
	return err
}

// forwardBridge: the peer forwards one to four records to the bridge, once
// a link is up, as the peer broker forwards what its own clients published
// to the filters the bridge asked for (newForwarded), each sent alone, then
// completes their exchanges as the bridge answers them. The model takes
// each it predicts stored as a publication (expect), a QoS 2 one once its
// PUBREL is sent ("The publication enters on PUBREL"), and checks every
// answer (forwardedAnswers).
func (f *fuzzer) forwardBridge() (bool, error) {
	b := f.bridge
	if b == nil {
		return false, nil
	}
	l, err := b.up(nil, f.timeout)
	if err != nil {
		return true, err
	}
	select {
	case a := <-b.acks:
		return true, fmt.Errorf("bridge: %s %d 0x%02X arrived with no record of the peer's awaiting it", harness.Name(a.Kind), a.ID, a.Reason)
	default:
	}
	var batch []*out
	for n := range 1 + f.rng.IntN(4) {
		o, err := f.newForwarded(n)
		if err != nil {
			return true, err
		}
		batch = append(batch, o)
	}
	if err := f.placeAll(batch); err != nil {
		return true, err
	}
	for _, o := range batch {
		f.note("bridge: the peer forwards %s", o)
	}
	// A QoS 2 record is expected once its PUBREL is sent (forwardedAnswers).
	f.expect(slices.DeleteFunc(slices.Clone(batch), func(o *out) bool { return o.p.QoS == 2 })...)
	pending := map[uint16]*out{}
	for _, o := range batch {
		if o.m != nil {
			o.m.sentAt.CompareAndSwap(0, time.Now().UnixMilli())
		}
		if o.p.QoS > 0 {
			pending[o.p.ID] = o
		}
		if _, err := l.Send(o.p); err != nil {
			return true, fmt.Errorf("bridge: the peer forwarding %s: %w", o, err)
		}
	}
	b.mu.Lock()
	b.stats.records += len(batch)
	b.mu.Unlock()
	return true, f.forwardedAnswers(l, batch, pending)
}

// newForwarded draws the n-th record of the step the peer forwards: a
// rule's filter matched at a QoS up to the rule's, which the bridge asked
// for and the peer granted (MQTT 5 3.8.4: the QoS sent is "the minimum of
// the QoS of the originally published message and the Maximum QoS granted
// by the Server" [MQTT-3.8.4-8]), and is stored at its own ("Inbound, the
// rule's QoS takes effect through the subscription the bridge asks for, and
// a record is stored at the QoS it arrives with"); now and then
// retained (the bridge asks "Retain As Published 1"), with a saguin-id or
// another User Property. Some are refused (design.md, Bridges): a topic
// matching no inbound rule ("A record matching no inbound rule is refused
// as an invalid inbound result is"), a topic of peer/a/# whose result
// breaks max_topic_length or max_topic_levels ("Topic limits are checked
// after substitution [...] An invalid inbound result is refused with 0x90,
// or dropped at QoS 0"), and one carrying a saguin- property but saguin-id
// (PUBLISH: "0x83 | a saguin- property other than saguin-id"), the
// ordinary publish path's refusal. Nothing else refuses one: "The
// acl_file, publish_rate and publish_bytes never apply to a bridge".
func (f *fuzzer) newForwarded(n int) (*out, error) {
	r, b := f.rng, f.bridge
	rule := bridgeRules[r.IntN(len(bridgeRules))]
	p := &harness.Publish{QoS: byte(r.IntN(int(rule.QoS) + 1)), Retain: r.IntN(8) == 0}
	o := &out{p: p}
	var topic string // where it lands, the rule's template applied
	switch rule.Filter {
	case "peer/a/#":
		// "bridged/a/in/" is 13 bytes and three levels, to "peer/a/"'s 7
		// and two.
		level := f.word(1, maxTopicLength-13)
		p.Topic, topic = "peer/a/"+level, "bridged/a/in/"+level
	case "peer/b/+":
		level := f.word(1, 4)
		p.Topic, topic = "peer/b/"+level, "ca/b/"+level
	default:
		p.Topic, topic = "peer/c", "bridged/c"
	}
	switch r.IntN(12) {
	case 0:
		// Matching no rule: a level after peer/ none names, or a second
		// level under peer/b/+.
		p.QoS = byte(r.IntN(3))
		p.Topic, o.refusals = [...]string{"peer/" + f.word(2, 4), "peer/b/" + f.word(1, 4) + "/" + f.word(1, 4)}[r.IntN(2)], []byte{0x90}
	case 1:
		// Within the limits at the peer, past one once landed: a level of
		// 15 to 20 bytes makes a topic of 28 to 33, over max_topic_length;
		// two levels make five, over max_topic_levels.
		p.QoS = byte(r.IntN(3))
		p.Topic, o.refusals = [...]string{"peer/a/" + f.word(maxTopicLength-12, maxTopicLength-7), "peer/a/" + f.topic(2)}[r.IntN(2)], []byte{0x90}
	case 2:
		p.Props, o.refusals = harness.Properties{harness.User("saguin-x"+f.word(1, 3), f.word(0, 3))}, []byte{0x83}
	}
	if len(o.refusals) == 0 {
		o.m = &message{step: f.step, topic: topic, qos: p.QoS, retain: p.Retain, bridge: bridgeName}
		if r.IntN(6) == 0 {
			o.m.id = f.word(1, 8)
			p.Props = append(p.Props, harness.User("saguin-id", o.m.id))
		}
		if r.IntN(3) == 0 {
			p.Props = append(p.Props, harness.User(f.word(1, 4), f.word(0, 4)))
		}
	}
	if p.QoS > 0 {
		b.forwarded = max(b.forwarded+1, 1) // wraps past 65,535 to 1, every earlier exchange complete
		p.ID = b.forwarded
	}
	return o, f.fill(o, n, 0, 8, letters, harness.V5)
}

// forwardedAnswers reads the bridge's answers to batch, pending its QoS 1
// and 2 records by packet identifier, until every exchange is complete:
// PUBACK at QoS 1, PUBREC at QoS 2, each 0x00 for a record the model
// predicts stored, its refusal's code for one it predicts refused
// (design.md, PUBLISH: "A QoS 2 refusal rides the PUBREC"); to a PUBREC
// 0x00, the peer's PUBREL, which gets PUBCOMP 0x00 once the record is in
// its channel ("PUBREC, then PUBCOMP once in the channel"). A PUBACK or
// PUBCOMP 0x00 makes its record stored, and bounds the intake of the QoS 0
// records sent before it on the link (each connection's packet order is
// preserved: design.md, Execution, Decisions).
func (f *fuzzer) forwardedAnswers(l *harness.PeerLink, batch []*out, pending map[uint16]*out) error {
	b := f.bridge
	released := map[uint16]*out{}
	limit := time.NewTimer(f.timeout)
	defer limit.Stop()
	for len(pending)+len(released) > 0 {
		var a *harness.Ack
		select {
		case a = <-b.acks:
		case <-limit.C:
			b.mu.Lock()
			failure := b.failure
			b.mu.Unlock()
			return cmp.Or(failure, fmt.Errorf("bridge: %d records the peer forwarded still owed an answer after %v", len(pending)+len(released), f.timeout))
		}
		var o *out
		switch a.Kind {
		case harness.PUBACK, harness.PUBREC:
			if o = pending[a.ID]; o == nil || harness.AnswerKind(o.p.QoS) != a.Kind {
				return fmt.Errorf("bridge: %s %d 0x%02X, which no record the peer forwarded awaits", harness.Name(a.Kind), a.ID, a.Reason)
			}
			delete(pending, a.ID)
		case harness.PUBCOMP:
			if o = released[a.ID]; o == nil {
				return fmt.Errorf("bridge: PUBCOMP %d 0x%02X to no PUBREL of the peer's", a.ID, a.Reason)
			}
			delete(released, a.ID)
		default:
			return fmt.Errorf("bridge: %s %d 0x%02X to the peer, which sends the bridge no record it has not completed", harness.Name(a.Kind), a.ID, a.Reason)
		}
		switch {
		case o.m == nil && !slices.Contains(o.refusals, a.Reason):
			return fmt.Errorf("bridge: %s 0x%02X to %s", harness.Name(a.Kind), a.Reason, o.predicted())
		case o.m == nil:
			b.mu.Lock()
			b.stats.refusedRecords++
			b.mu.Unlock()
		case a.Reason != 0:
			return fmt.Errorf("bridge: %s 0x%02X to %s", harness.Name(a.Kind), a.Reason, o.predicted())
		case a.Kind == harness.PUBREC:
			f.expect(o)
			f.note("bridge: the peer's PUBREL %d: want PUBCOMP 0x00, %q entering its channel", a.ID, o.m.tag)
			if _, err := l.Send(&harness.Ack{Kind: harness.PUBREL, ID: a.ID}); err != nil {
				return fmt.Errorf("bridge: the peer's PUBREL %d: %w", a.ID, err)
			}
			released[a.ID] = o
		default:
			now := time.Now().UnixMilli()
			o.m.ackStep = f.step
			o.m.setFate(stored)
			o.m.acknowledgedAt.CompareAndSwap(0, now)
			o.m.confirmed.CompareAndSwap(0, f.serial)
			for _, earlier := range batch[:slices.Index(batch, o)] {
				if earlier.m != nil && earlier.p.QoS == 0 {
					earlier.m.acknowledgedAt.CompareAndSwap(0, now)
				}
			}
			b.mu.Lock()
			b.stats.stored++
			b.mu.Unlock()
		}
	}
	return nil
}
