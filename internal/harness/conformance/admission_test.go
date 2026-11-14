package conformance

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of the delivery admission gate seen from outside (design.md,
// R5; Execution, Communication and Capacities, Delivery admission; Flows,
// PUBLISH and Delivery): publishers are slowed so that delivery to a
// healthy consumer keeps up, and never for a consumer that is slow or
// gone. What the gate does is seen on the wire and on /metrics:
// saguin_delivery_admission_gated is 1 while it is closed, and
// saguin_delivery_admission_gated_seconds_total adds each closed interval
// once it ends (metrics.md, Publishing).
//
// The overload is made by the broker's own work, not by the machine's
// speed: the broker runs under strace (testkit.SlowIO), so every sync of
// its store's log waits a fixed time. A commit stores what a publisher has
// in flight, while each QoS 1 delivery to a durable session waits for its
// own exchange to be stored, a window at a time (design.md, Delivery,
// Positions: "Before handing a QoS 1 or QoS 2 delivery to a durable
// session's writer, the core stores its packet identifier"; Preparation:
// "A connection's prepared and sent deliveries together are at most twice
// its window"): with a window of 1, the broker delivers fewer records per
// commit than it stores, however fast the machine.

// The gate's two series (metrics.md, Publishing), as testkit.SeriesKey
// names them.
const (
	gatedSeries        = "saguin_delivery_admission_gated{}"
	gatedSecondsSeries = "saguin_delivery_admission_gated_seconds_total{}"
)

// overloadSync is how long the overload rig makes each sync of the
// store's log wait.
const overloadSync = 10 * time.Millisecond

// publisherWindow is broker.qos2.max_inflight_per_client under the
// overload: the Receive Maximum a publisher keeps full (design.md,
// PUBLISH: "A reader counts its client's unanswered QoS 1 and 2 PUBLISHes
// against qos2.max_inflight_per_client"), so a commit stores up to that
// many of its records.
const publisherWindow = 4

// overloadFor is how long a publisher floods under the overload. A commit
// stores up to publisherWindow records and hands at most two deliveries
// over to a subscriber with a window of 1, so without a gate the last
// records wait at least half of overloadFor on the broker.
const overloadFor = 4 * time.Second

// waitBound is a broker-side wait that a healthy subscriber's deliveries
// outgrow under the overload with the gate disabled, showing the overload
// is real: a delivery's arrival less its record's saguin-timestamp, the
// core's intake time (design.md, PUBLISH: "The record's timestamp is the
// core's intake time"). Without a gate the last records wait at least half
// of overloadFor.
const waitBound = 1500 * time.Millisecond

// floodChannel is the configured append channel the overload's records
// enter, so that its deliveries carry saguin-timestamp (design.md, Sagüin
// properties).
var floodChannel = harness.Channel{Name: "flood", Type: "append", Filter: "flood/#", Start: "tail"}

// overload is o under the overload rig: every sync of the store's log
// waits overloadSync, publishers keep at most publisherWindow publications
// in flight, and the broker has floodChannel.
func overload(t *testing.T, o *harness.Options) *harness.Options {
	t.Helper()
	own := *o // the caller's Options stay as they were
	own.Dir = cmp.Or(own.Dir, t.TempDir())
	own.MaxInflightPerClient = publisherWindow
	own.Channels = []harness.Channel{floodChannel}
	own.Under = testkit.SlowIO(t, "sync", overloadSync, filepath.Join(own.Dir, "saguin.db-wal"), filepath.Join(own.Dir, "strace.out"))
	return &own
}

// neverGated fails the test unless b's gate is open and has never closed:
// saguin_delivery_admission_gated reads 0, and so does
// saguin_delivery_admission_gated_seconds_total, which adds every closed
// interval once it ends.
func neverGated(t *testing.T, b *harness.Broker, when string) {
	t.Helper()
	neverGatedSince(t, b, 0, when)
}

// neverGatedSince fails the test unless b's gate is open and has not closed
// since a moment when it was open and
// saguin_delivery_admission_gated_seconds_total read closed:
// saguin_delivery_admission_gated reads 0, and the total still reads
// closed.
func neverGatedSince(t *testing.T, b *harness.Broker, closed float64, when string) {
	t.Helper()
	got := support.Sampled(t, b, gatedSeries, gatedSecondsSeries)
	if got[gatedSeries] != 0 || got[gatedSecondsSeries] != closed {
		t.Fatalf("%s, /metrics reads %v; want the gate open and not closed since, %s 0 and %s %v", when, got, gatedSeries, gatedSecondsSeries, closed)
	}
}

// durableWindowOf1 is the property section of an MQTT 5 CONNECT asking for
// a durable session and Receive Maximum 1.
var durableWindowOf1 = harness.Properties{{ID: harness.SessionExpiryInterval, Int: 3600}, {ID: harness.ReceiveMaximum, Int: 1}}

// arrival is one delivery a subscriber received, and how long its record
// waited on the broker, from its saguin-timestamp to its arrival; or the
// error that ended the subscriber's reading.
type arrival struct {
	wait time.Duration
	err  error
}

// healthy connects a subscriber to addr with a durable session of its own
// and Receive Maximum 1, subscribed at QoS 1 to filter on floodChannel,
// which reads and acknowledges every delivery at once on a goroutine of
// its own, sending what it received on the channel returned until its
// reading ends.
func healthy(t *testing.T, addr, filter string) <-chan arrival {
	t.Helper()
	c := support.Subscribed(t, addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: durableWindowOf1},
		support.At(1, filter))
	arrivals := make(chan arrival, 1<<16)
	go func() {
		defer close(arrivals)
		for {
			got, err := c.Recv()
			now := time.Now()
			if err != nil {
				arrivals <- arrival{err: err}
				return
			}
			p, ok := got.(*harness.Publish)
			if !ok || p.QoS != 1 {
				arrivals <- arrival{err: fmt.Errorf("%s; want a PUBLISH at QoS 1", harness.Name(got.Type()))}
				return
			}
			if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID}); err != nil {
				arrivals <- arrival{err: err}
				return
			}
			stamp := p.Props.User("saguin-timestamp")
			if len(stamp) != 1 {
				arrivals <- arrival{err: fmt.Errorf("PUBLISH %q carries saguin-timestamp %q; want one", p.Topic, stamp)}
				return
			}
			ms, err := strconv.ParseInt(stamp[0], 10, 64)
			if err != nil {
				arrivals <- arrival{err: fmt.Errorf("PUBLISH %q: saguin-timestamp: %w", p.Topic, err)}
				return
			}
			arrivals <- arrival{wait: now.Sub(time.UnixMilli(ms))}
		}
	}()
	return arrivals
}

// next is the subscriber's next arrival, failing the test where its
// reading ended instead.
func next(t *testing.T, arrivals <-chan arrival) arrival {
	t.Helper()
	a, ok := <-arrivals
	if !ok || a.err != nil {
		t.Fatalf("the subscriber's reading ended: %v", a.err)
	}
	return a
}

// flood publishes topic from c at QoS 1 for d, keeping publisherWindow
// publications unacknowledged, then waits for every acknowledgement,
// failing the test unless each is PUBACK 0x00 (design.md, PUBLISH:
// "PUBACK once stored"), and returns how many it sent.
func flood(t *testing.T, c *harness.Client, topic string, d time.Duration) int {
	t.Helper()
	acks := make(chan error, publisherWindow+1)
	go func() {
		for {
			got, err := c.Recv()
			if err == nil {
				if a, ok := got.(*harness.Ack); !ok || a.Kind != harness.PUBACK || a.Reason != 0 {
					err = fmt.Errorf("%+v; want PUBACK 0x00", got)
				}
			}
			acks <- err
			if err != nil {
				return
			}
		}
	}()
	end := time.Now().Add(d)
	sent, out := 0, 0
	for out > 0 || time.Now().Before(end) {
		if out < publisherWindow && time.Now().Before(end) {
			if err := c.Send(&harness.Publish{QoS: 1, ID: c.NextID(), Topic: topic, Payload: []byte("flood")}); err != nil {
				t.Fatalf("PUBLISH %d: %v", sent+1, err)
			}
			sent, out = sent+1, out+1
			continue
		}
		if err := <-acks; err != nil {
			t.Fatalf("waiting for the publisher's PUBACK, %d of %d sent unanswered: %v", out, sent, err)
		}
		out--
	}
	return sent
}

// design.md, PUBLISH: "Admission never waits for a consumer
// acknowledgement, socket drain or application response". Delivery,
// Admission progress: "Waiting for send capacity, an acknowledgement,
// socket drain or an application response never contributes"; Peer
// obligations: "An overdue obligation removes qualification at its
// deadline".
//
// A subscriber at QoS 1 reads every delivery and acknowledges none, its
// window of 20 (config.md, limits.max_inflight) soon full. A publisher
// publishes to it at QoS 1, one after another, for 1.5s, well past
// delivery_admission_delay with deliveries waiting on the subscriber:
// every publication is acknowledged, and the gate never closes.
func TestWithheldAcknowledgementsNeverCloseTheGate(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, "withheld/#"))
	go func() {
		for { // reads, and acknowledges nothing
			if _, err := sub.Recv(); err != nil {
				return
			}
		}
	}()
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "withheld/x", Payload: []byte("x")})
	}
	neverGated(t, b, "after a subscriber withheld its acknowledgements")
}

// design.md, PUBLISH: admission "may pause to bound broker-side delivery
// delay for connected, live consumers able to receive". Delivery,
// Admission qualification: "a consumer qualifies when connected [...].
// Qualification [...] ends on disconnection"; "No qualifying recipient
// means no qualifying work".
//
// A client with a durable session subscribes at QoS 1 and disconnects. A
// publisher publishes to its subscription at QoS 1, one after another, for
// 1s: every publication is acknowledged, the session's backlog growing
// all the while, and the gate never closes.
func TestDisconnectedBacklogNeverClosesTheGate(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{})
	away := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-away"), CleanStart: true, Props: support.Durable},
		support.At(1, "away/#"))
	if err := away.Send(&harness.Disconnect{}); err != nil {
		t.Fatal(err)
	}
	harness.ExpectClosed(t, away)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	for end := time.Now().Add(time.Second); time.Now().Before(end); {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "away/x", Payload: []byte("x")})
	}
	neverGated(t, b, "after a disconnected subscriber's backlog grew")
}

// design.md, Execution, Resource bounds: "A connected durable session may
// keep a position on every append channel and queue. A position no
// subscription reaches has no scan or page of its own, and never holds
// publishers back"; Flows, SUBSCRIBE, UNSUBSCRIBE: "When the last
// subscription reaching an append channel or a queue is removed, a durable
// session keeps its position there"; "Without a subscription, the position
// moves only by a seek, or to the floor"; PUBLISH: admission "may pause to
// bound broker-side delivery delay for connected, live consumers able to
// receive"; Delivery, Admission qualification: "a consumer qualifies when
// connected, allowed to receive, and caught up to the channel's current
// committed tail"; "Qualification persists through broker-caused lag".
// config.md, delivery_admission_delay, 100ms by default.
//
// A client holding a durable session subscribes at QoS 1 to kept/#, an
// append channel, receives and acknowledges the record stored there,
// caught up, and unsubscribes, the last subscription reaching the channel:
// the UNSUBACK finds its kept position saved, and the client stays
// connected. A publisher publishes to kept/x at QoS 1, one after another,
// for 1s, the kept position falling ever further behind: every publication
// is acknowledged, and the gate never closes. The gate is read over that
// second alone, from the kept position's saving: while the client was
// still subscribed, a delivery under way, the gate may rightly have closed
// on a slow machine or disk (Delivery, Admission qualification). It must
// be open as the second begins, or the test's premise is broken.
func TestKeptPositionNeverClosesTheGate(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{Channels: []harness.Channel{{Name: "kept", Type: "append", Filter: "kept/#"}}})
	db := support.OpenDB(t, b.DB)
	id := clientID(t, "-kept")
	keeper := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true, Props: support.Durable},
		support.At(1, "kept/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	first := &harness.Publish{QoS: 1, Topic: "kept/x", Payload: []byte("caught up")}
	support.Acknowledged(t, pub, first)
	acknowledge(t, keeper, support.DeliveredAt(t, keeper, 1, first.Topic, first.Payload).ID, 0)
	unsuback(t, keeper, nil, []string{"kept/#"}, 0x00)
	storedPosition(t, db, id, "kept")
	kept := support.Sampled(t, b, gatedSeries, gatedSecondsSeries)
	if kept[gatedSeries] != 0 {
		t.Fatalf("test premise broken: with the kept position saved, /metrics reads %v; the test needs the gate open as the kept position starts falling behind", kept)
	}
	for end := time.Now().Add(time.Second); time.Now().Before(end); {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "kept/x", Payload: []byte("x")})
	}
	neverGatedSince(t, b, kept[gatedSecondsSeries], "after a kept position fell behind")
}

// design.md, Delivery, Admission progress: "This rule also applies when
// one connection publishes and subscribes. Work requiring its next input
// cannot keep the gate closed while that input is behind a withheld
// publication body". Peer obligations: an obligation's deadline "does not
// run while the core withholds a publication body grant on that same
// connection".
//
// Under the overload rig, one client, with a durable session and Receive
// Maximum 1, subscribes at QoS 1 to the flood channel and floods it from
// the same connection for overloadFor, acknowledging each delivery at
// once: its acknowledgements follow its publications on the wire. Every
// publication is acknowledged and delivered back to it, the connection
// making progress within the harness's watchdog throughout, and the gate
// closed at least once on the way: the load was real.
func TestPublishingSubscriberKeepsMakingProgress(t *testing.T) {
	b := support.OpsLaunch(t, overload(t, &harness.Options{}))
	c := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-self"), CleanStart: true, Props: durableWindowOf1},
		support.At(1, "flood/#"))
	packets := make(chan any, 1<<16)
	go func() {
		for {
			got, err := c.Recv()
			if err != nil {
				packets <- err
				return
			}
			packets <- got
		}
	}()
	end := time.Now().Add(overloadFor)
	sent, out, delivered := 0, 0, 0
	for out > 0 || delivered < sent || time.Now().Before(end) {
		if out < publisherWindow && time.Now().Before(end) {
			if err := c.Send(&harness.Publish{QoS: 1, ID: c.NextID(), Topic: "flood/self", Payload: []byte("self")}); err != nil {
				t.Fatalf("PUBLISH %d: %v", sent+1, err)
			}
			sent, out = sent+1, out+1
			continue
		}
		switch p := (<-packets).(type) {
		case error:
			t.Fatalf("watchdog: with %d of %d publications unacknowledged and %d delivered back, the connection made no progress: %v",
				out, sent, delivered, p)
		case *harness.Ack:
			if p.Kind != harness.PUBACK || p.Reason != 0 {
				t.Fatalf("%s 0x%02X; want PUBACK 0x00", harness.Name(p.Kind), p.Reason)
			}
			out--
		case *harness.Publish:
			if p.QoS != 1 || !bytes.Equal(p.Payload, []byte("self")) {
				t.Fatalf("PUBLISH %q at QoS %d of %q; want its own publication at QoS 1", p.Topic, p.QoS, p.Payload)
			}
			if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID}); err != nil {
				t.Fatal(err)
			}
			delivered++
		default:
			t.Fatalf("%T; want a PUBACK or a PUBLISH", p)
		}
	}
	if got := support.Sampled(t, b, gatedSecondsSeries)[gatedSecondsSeries]; got == 0 {
		t.Fatalf("%s reads 0 after the run; want the gate to have closed under the load", gatedSecondsSeries)
	}
}

// design.md, Delivery, Admission progress: "This rule also applies when
// one connection publishes and subscribes. Work requiring its next input
// cannot keep the gate closed while that input is behind a withheld
// publication body or a publish-rate wait (PUBLISH, Rate limits)";
// "Waiting for send capacity, an acknowledgement, socket drain or an
// application response never contributes". PUBLISH, Rate limits: "The
// wait holds every packet of that connection, acknowledgements and PINGREQ
// included"; "Nothing is refused or lost, at any QoS or MQTT version".
//
// throttled, its acl_file entry's publish_rate 1, connects with a durable
// session and Receive Maximum 1, subscribes at QoS 1 to gate/#, and
// acknowledges each delivery the moment it reads it, its PUBACKs on the
// wire behind its reader's wait: it writes three PUBLISHes at QoS 1 on
// throttled/x at once, the second and third read a second after the one
// before. Meanwhile pub, with no limit, publishes on gate/x at QoS 1, one
// after another, for 1.5s, its deliveries to throttled waiting for
// throttled's PUBACKs. Every publication of pub's is acknowledged 0x00, the
// gate never closes, and each of throttled's PUBLISHes gets PUBACK 0x00,
// the third no sooner than two seconds after the write: the wait was real.
func TestThrottledSubscriberNeverHoldsTheGate(t *testing.T) {
	const rate = 1 // throttled's publish_rate
	acl := "roles:\n" + aclFree + fmt.Sprintf("users:\n  throttled: {roles: [free], limits: {publish_rate: %d}}\n  pub: [free]\n", rate)
	o := aclOptions(t, acl, "throttled", "pub")
	o.Channels = aclChannels
	b := support.OpsLaunch(t, o)
	login := aclLogin(clientID(t, "-throttled"), "throttled")
	login.Props = durableWindowOf1
	c := support.Subscribed(t, b.Addr, harness.V5, login, support.At(1, "gate/#"))
	own := make([]*harness.Publish, rate+2)
	for i := range own {
		own[i] = &harness.Publish{QoS: 1, ID: c.NextID(), Topic: "throttled/x", Payload: []byte("x")}
	}
	wire := make([]byte, 0, 32*len(own))
	for _, p := range own {
		wire = append(wire, harness.Encoded(t, p, harness.V5)...)
	}
	at := time.Now()
	harness.SendBytes(t, c, wire)
	// acks carries each of throttled's own PUBACKs and when it was read,
	// or the error that ended its reading.
	type ack struct {
		*harness.Ack
		at  time.Time
		err error
	}
	acks := make(chan ack, len(own)+1)
	go func() {
		for {
			got, err := c.Recv()
			if err != nil {
				acks <- ack{err: err}
				return
			}
			switch p := got.(type) {
			case *harness.Ack:
				acks <- ack{Ack: p, at: time.Now()}
			case *harness.Publish:
				if err := c.Send(&harness.Ack{Kind: harness.PUBACK, ID: p.ID}); err != nil {
					acks <- ack{err: err}
					return
				}
			default:
				acks <- ack{err: fmt.Errorf("%s; want a PUBACK or a delivery", harness.Name(got.Type()))}
				return
			}
		}
	}()
	pub, _ := harness.Connected(t, b.Addr, harness.V5, aclLogin(clientID(t, "-pub"), "pub"))
	for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); {
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "gate/x", Payload: []byte("x")})
	}
	neverGated(t, b, "after a throttled subscriber's acknowledgements waited behind its reader")
	var last time.Time
	for range own {
		a := <-acks
		if a.err != nil {
			t.Fatalf("waiting for throttled's PUBACKs: %v", a.err)
		}
		if a.Kind != harness.PUBACK || a.Reason != 0 {
			t.Errorf("throttled's PUBLISH under identifier %d, written at once with %d others: %s 0x%02X; want PUBACK 0x00, nothing refused", a.ID, len(own)-1, harness.Name(a.Kind), a.Reason)
		}
		last = a.at
	}
	if took := last.Sub(at); took < least(2*time.Second/rate) {
		t.Errorf("throttled's last PUBACK came %v after the write; want no sooner than %v, its reader waiting for its allowance", took, least(2*time.Second/rate))
	}
}

// stallSync is how long each sync of a stallable broker's store's log
// waits while it is stalled.
const stallSync = 500 * time.Millisecond

// stallable is a broker whose store's log syncs each wait stallSync while
// it is stalled: its database sits in a folder that stall renames, the
// rig slowing only the log at the stalled name (testkit.SlowIO).
type stallable struct {
	*harness.Broker
	store, stalled string // the database's folder, and the name that stalls it
	trace          string // the rig's notes of each call it slowed (testkit.Injected)
}

// launchStallable launches a stallable broker with o, its operations
// listener on.
func launchStallable(t *testing.T, o *harness.Options) *stallable {
	t.Helper()
	return launchSlowed(t, o, "sync", stallSync)
}

// launchSlowed is launchStallable with each call of kind on the log
// (testkit.SlowIO's kinds) waiting delay while it is stalled.
func launchSlowed(t *testing.T, o *harness.Options, kind string, delay time.Duration) *stallable {
	t.Helper()
	dir := t.TempDir()
	s := &stallable{store: filepath.Join(dir, "store"), stalled: filepath.Join(dir, "stalled"), trace: filepath.Join(dir, "strace.out")}
	if err := os.Mkdir(s.store, 0o700); err != nil {
		t.Fatal(err)
	}
	own := *o
	own.FilePath = filepath.Join(s.store, "saguin.db")
	own.Under = testkit.SlowIO(t, kind, delay, filepath.Join(s.stalled, "saguin.db-wal"), s.trace)
	s.Broker = support.OpsLaunch(t, &own)
	return s
}

// stall makes every later sync of s's store's log wait stallSync (on), or
// not.
func (s *stallable) stall(t *testing.T, on bool) {
	t.Helper()
	from, to := s.store, s.stalled
	if !on {
		from, to = to, from
	}
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
}

// gateSample is one read of the gate's two series: the read began at
// start and was answered at end.
type gateSample struct {
	start, end     time.Time
	gated, seconds float64
}

// stalledDelivery has pub publish topic at QoS 1 with s stalled, so that
// its commit, and then the stored exchange of sub, a durable subscriber at
// QoS 1, before the delivery is handed over, each wait stallSync. Once sub
// has the delivery, it takes the stall off and has sub acknowledge it.
// All the while it reads s's gate series, on the test's goroutine, and it
// returns the reads, the last one once the gate reads open with what it
// was closed added, or open and never seen closed; or after
// harness.DefaultTimeout, the watchdog.
func (s *stallable) stalledDelivery(t *testing.T, pub, sub *harness.Client, topic string) []gateSample {
	t.Helper()
	got := make(chan error, 1)
	var d *harness.Publish
	go func() {
		p, err := sub.Recv()
		if err == nil {
			var ok bool
			if d, ok = p.(*harness.Publish); !ok || d.Topic != topic || d.QoS != 1 {
				err = fmt.Errorf("%+v; want PUBLISH %q at QoS 1", p, topic)
			}
		}
		got <- err
	}()
	var samples []gateSample
	closed := false
	sample := func() gateSample {
		start := time.Now()
		v := support.Sampled(t, s.Broker, gatedSeries, gatedSecondsSeries)
		g := gateSample{start: start, end: time.Now(), gated: v[gatedSeries], seconds: v[gatedSecondsSeries]}
		samples = append(samples, g)
		closed = closed || g.gated != 0
		return g
	}
	sample()
	s.stall(t, true)
	p := &harness.Publish{QoS: 1, Topic: topic, Payload: []byte("x")}
	support.Sent(t, pub, p)
	support.Puback(t, pub, p)
	for waiting := true; waiting; {
		select {
		case err := <-got:
			if err != nil {
				t.Fatalf("waiting for the delivery of %q: %v", topic, err)
			}
			waiting = false
		default:
			sample()
		}
	}
	s.stall(t, false)
	if err := sub.Send(&harness.Ack{Kind: harness.PUBACK, ID: d.ID}); err != nil {
		t.Fatal(err)
	}
	testkit.Within(harness.DefaultTimeout, func() bool {
		g := sample()
		return g.gated == 0 && (g.seconds != 0 || !closed)
	})
	return samples
}

// design.md, Execution, Capacities, Delivery admission: "The gate closes
// when the admission delay reaches delivery_admission_delay"; "Timestamps
// come from the core's event times". metrics.md, Publishing:
// saguin_delivery_admission_gated, "1 while the delivery admission gate is
// closed, otherwise 0"; saguin_delivery_admission_gated_seconds_total,
// "time the delivery admission gate was closed, by the core's event times.
// Includes completed intervals only; an ongoing interval is added when it
// ends"; Rules: "Every series exists from start [...] Counters start at
// 0".
//
// A stallable broker starts with both series at 0. A client with a
// durable session subscribes at QoS 1; with the store stalled, a publisher
// publishes to it. The record's commit waits stallSync, then the
// subscriber's stored exchange waits stallSync more, its work
// delivery_admission_delay old long before: the gate closes, and opens
// once the delivery is handed over, one interval. The gauge reads only 0
// and 1, 1 in one run of reads, and the counter 0 while the gauge reads 1.
// Once the gauge reads 0 again, the counter holds the interval: at least
// the time the gauge was seen 1, at most the time between the reads of 0
// around it. It reads the same 200ms later: added once.
func TestAdmissionGateSeries(t *testing.T) {
	s := launchStallable(t, &harness.Options{})
	awaitSeries(t, s.Broker, map[string]float64{gatedSeries: 0, gatedSecondsSeries: 0})
	sub := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: support.Durable},
		support.At(1, "gated/#"))
	pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	samples := s.stalledDelivery(t, pub, sub, "gated/1")

	first, last := -1, -1 // the first and last reads of 1
	for i, g := range samples {
		switch {
		case g.gated == 1 && g.seconds != 0:
			t.Fatalf("read %d: the gauge reads 1 and the counter %v; want the ongoing interval not yet added", i, g.seconds)
		case g.gated == 1:
			if first < 0 {
				first = i
			}
			last = i
		case g.gated != 0:
			t.Fatalf("read %d: %s reads %v; want 0 or 1", i, gatedSeries, g.gated)
		}
	}
	if first < 0 {
		t.Fatalf("the gauge never read 1 in %d reads while a delivery waited %v on the store; want the gate closed", len(samples), stallSync)
	}
	for i := first; i <= last; i++ {
		if samples[i].gated != 1 {
			t.Fatalf("read %d: the gauge reads 0 between reads of 1; want one closed interval", i)
		}
	}
	end := samples[len(samples)-1]
	if end.gated != 0 || end.seconds == 0 {
		t.Fatalf("after the delivery, /metrics reads %s %v and %s %v; want 0 and the closed interval",
			gatedSeries, end.gated, gatedSecondsSeries, end.seconds)
	}
	closedFor := time.Duration(end.seconds * float64(time.Second))
	least, most := samples[last].start.Sub(samples[first].end), samples[last+1].end.Sub(samples[first-1].start)
	if closedFor < least || closedFor > most {
		t.Errorf("%s reads %v; want the closed interval, between %v (seen closed) and %v (between the reads of open around it)",
			gatedSecondsSeries, closedFor, least, most)
	}
	time.Sleep(200 * time.Millisecond)
	if again := support.Sampled(t, s.Broker, gatedSecondsSeries)[gatedSecondsSeries]; again != end.seconds {
		t.Errorf("200ms later, %s reads %v; want %v, the interval added once", gatedSecondsSeries, again, end.seconds)
	}
}

// config.md, broker: delivery_admission_delay, "none disables the pause";
// Values: "Only write_timeout, publish_commit_interval,
// delivery_admission_delay, delivery_peer_timeout, a queue's retry.delay
// and a bridge's ack_interval also take ms". design.md, Execution,
// Capacities, Delivery admission: the gate "remains open when disabled";
// Delivery, Peer obligations: an obligation's deadline "is
// delivery_peer_timeout after the core receives the successful write
// report"; "An overdue obligation removes qualification at its deadline".
//
// none: under the overload rig, a healthy subscriber, acknowledging at
// once, subscribes to the flood channel and a publisher floods it for
// overloadFor: the subscriber's wait outgrows waitBound, and the gate
// never closes.
//
// delay in ms: with delivery_admission_delay 1500ms, the stalled delivery
// of TestAdmissionGateSeries, whose work waits about stallSync, never
// closes the gate, which the default 100ms closes.
//
// peer timeout in ms: with delivery_peer_timeout 150ms, a durable
// subscriber receives a delivery and acknowledges none. With the store
// stalled, a second publication waits for its commit and then its stored
// exchange, stallSync each: its work is delivery_admission_delay old about
// 600ms after the first delivery's write, before the default 1s
// deadline, but the subscriber no longer qualifies, its acknowledgement
// overdue 150ms after that write. The gate never closes.
func TestAdmissionSettings(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		b := support.OpsLaunch(t, overload(t, &harness.Options{DeliveryAdmissionDelay: harness.NoDeliveryAdmission}))
		arrivals := healthy(t, b.Addr, "flood/#")
		pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		n := flood(t, pub, "flood/x", overloadFor)
		over := false
		for i := 0; i < n && !over; i++ {
			over = next(t, arrivals).wait > waitBound
		}
		if !over {
			t.Fatalf("with the gate disabled, each of %d deliveries waited at most %v; want the overload to outgrow it", n, waitBound)
		}
		neverGated(t, b, "with delivery_admission_delay none, under the overload")
	})
	t.Run("delay in ms", func(t *testing.T) {
		s := launchStallable(t, &harness.Options{DeliveryAdmissionDelay: 1500 * time.Millisecond})
		sub := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: support.Durable},
			support.At(1, "gated/#"))
		pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		s.stalledDelivery(t, pub, sub, "gated/1")
		neverGated(t, s.Broker, "with delivery_admission_delay 1500ms, after a delivery waited about "+stallSync.String())
	})
	t.Run("peer timeout in ms", func(t *testing.T) {
		s := launchStallable(t, &harness.Options{DeliveryPeerTimeout: 150 * time.Millisecond})
		sub := support.Subscribed(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true, Props: support.Durable},
			support.At(1, "gated/#"))
		pub, _ := harness.Connected(t, s.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
		support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "gated/1", Payload: []byte("x")})
		support.DeliveredAt(t, sub, 1, "gated/1", []byte("x"))
		s.stalledDelivery(t, pub, sub, "gated/2")
		neverGated(t, s.Broker, "with delivery_peer_timeout 150ms, a subscriber owing an acknowledgement")
	})
}

// design.md, Delivery, Admission progress: "Work contributes delay only
// while it can advance without a peer action. Waiting for send capacity, an
// acknowledgement, socket drain or an application response never
// contributes. Neither does waiting for a resource whose release requires
// such an action"; "For the oldest delivery waiting for message memory, the
// core counts the outgoing reservations that cannot be released until peer
// input arrives or a writer finishes (Message memory). [...] When the
// delivery's reservation does not fit in the outgoing budget left after
// that total, the delivery is waiting for a peer. The core makes this
// judgment independently of whether the gate is open or closed."
// Capacities, Delivery admission: "The gate closes when the admission delay
// reaches delivery_admission_delay". Message memory, Bytes back: "A
// delivery reservation ends when the writer reports completion or
// discard". metrics.md: saguin_delivery_admission_gated, "1 while the gate
// is closed".
//
// With max_outgoing_memory stallBudget and delivery_admission_delay at its
// default, 100 ms, a subscriber that reads nothing holds the outgoing
// budget, its writer unable to finish its write, and the rest of its
// deliveries wait for room (stallOutgoing): reservations only that writer's
// finishing releases leave none of them room. F subscribes to gate/f at
// QoS 1, and a publisher's message there is acknowledged: F's delivery
// waits behind them. After heldFor, past delivery_admission_delay, the gate
// has never closed, and another publisher's QoS 1 publish is acknowledged.
// The stalled subscriber closes: F receives its delivery.
func TestMemoryWaitForAPeerNeverClosesTheGate(t *testing.T) {
	b := support.OpsLaunch(t, &harness.Options{MaxOutgoingMemory: stallBudget, WriteTimeout: stallFor, DeliveryPeerTimeout: time.Hour})
	stalled, delivery := stallOutgoing(t, b, "")
	f := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-f"), CleanStart: true}, support.At(1, "gate/f"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-p"), CleanStart: true})
	p := &harness.Publish{QoS: 1, Topic: "gate/f", Payload: []byte("behind the stalled writer")}
	support.Acknowledged(t, pub, p)
	time.Sleep(heldFor) // the wait's length is the behaviour: the gate stays open past the delay
	neverGated(t, b, fmt.Sprintf("%v after F's delivery began waiting behind the stalled subscriber's", heldFor))
	other, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true})
	support.Acknowledged(t, other, &harness.Publish{QoS: 1, Topic: "gate/other", Payload: []byte("granted")})
	if got := support.Sampled(t, b, inTransitOutgoing)[inTransitOutgoing]; got <= float64(stallBudget-delivery) {
		t.Fatalf("%s reads %v: the stalled subscriber no longer held the outgoing budget, so nothing here tests the rule", inTransitOutgoing, got)
	}
	if err := stalled.Close(); err != nil {
		t.Fatal(err)
	}
	support.DeliveredAt(t, f, 1, p.Topic, p.Payload)
}
