package broker_test

// Devices on a link that keeps dropping.
//
// The target deployment is an ESP32 or something like it on a network
// nobody controls: a small buffer, a small prefetch, a durable session, and
// a radio that comes and goes. Every one of those is a declaration the
// broker acts on, and the interesting failures are at the joins between
// them - a reconnect landing inside a delivery, a takeover racing a
// teardown, a job half-handed-over when the socket dies.
//
// Two of the three worst defects found in this repository in one day needed
// exactly this shape to show: reconnects, under load, with the same client
// id. Neither the unit tests nor the demonstration produce it.
//
// **Nothing here asserts on a count, a duration or an ordering of events**,
// because the link decides those and a test that asserts them is a coin
// flip rather than an assertion. What is asserted is what must
// hold however many times the link drops:
//
//   - an append consumer receives every record, with no gap - duplicates
//     are at-least-once keeping its promise, a missing offset is not;
//   - every queue job is resolved or dead-lettered, and none is left held
//     by a worker that is no longer there;
//   - the broker is still serving at the end.
//
// The same test runs two ways, and **nothing is ever skipped**: it is one
// test with one dial. Short, in the ordinary suite, it drops the link a
// handful of times - enough to reach the paths. `make soak` sets
// SAGUIN_SOAK and the same code runs for as long as that says.
//
// **There is no build tag and no skip**, which is deliberate and is the
// point of doing it this way. A soak excluded at compile time stops
// building and says nothing, which is the trap `make bench` exists for - a
// benchmark here failed on every commit from the one that added it until
// somebody ran it by hand. A soak that *skipped* would rot the same way,
// one step later: it would compile and then quietly assert nothing. Always
// running, at a length the environment chooses, is what avoids both.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqttv3 "github.com/eclipse/paho.mqtt.golang"

	"github.com/ifnesi/saguin/internal/channel"
)

// The two append channels a soaking device is served from, named by the
// level that tells their topics apart. A device subscribes to
// `iot/+/events/+`, which intersects both `iot/+/events/+` and `iot/hq/#`,
// so what arrives has to be attributed before it can be counted.
const (
	soakSite = "events"
	soakHQ   = "hq"
)

// soakChannelOf is which of the two a record came from, by the only thing
// the client can see: its topic. `iot/hq/…` belongs to `hq` because a
// spelled-out level beats `+` at level 1, and everything else the device's
// filter reaches belongs to `events`.
func soakChannelOf(topic string) string {
	if strings.HasPrefix(topic, "iot/hq/") {
		return soakHQ
	}
	return soakSite
}

// soakFor is how long the flaky-link tests run: a short pass in the
// ordinary suite, or SAGUIN_SOAK when `make soak` asks for one.
//
// The short figure is chosen to reach the paths rather than to hammer them.
// One cut and one restore is enough for a reconnect, a session resume and a
// redelivery; the rest is what a soak is for.
func soakFor(t *testing.T, short time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv("SAGUIN_SOAK")
	if v == "" {
		return short
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("SAGUIN_SOAK=%q is not a duration such as 30s or 5m: %v", v, err)
	}
	t.Logf("soak: running for %s", d)
	return d
}

// RFC 0003 and invariants 1, 2, 5 and 7, all at once and on one broker.
//
// **One test rather than one per channel, because a deployment is not one
// channel.** A queue redelivering while an append consumer resumes while a
// `latest` catch-up fires are the same locks, the same storage, the same
// retention sweep - and a soak that drives them in separate tests can never
// produce the interference between them. That interference is the whole
// reason to run this rather than the unit tests.
//
// Storage is SQLite rather than memory, which is what anybody deploying a
// durable channel actually runs, and it is where the dead-letter move is a
// real transaction: the queue and its derived `__dlq` share the file, so
// invariant 5's "one commit or neither" is being exercised rather than
// asserted about a map.
//
// **Every device has its own link.** The proxy cuts everything through it,
// so one proxy per device is what makes a single radio flap possible -
// every earlier version of this took the whole estate down together on a
// fixed cadence, which is the harder case but also the only one it could
// ever produce.
//
// What is asserted is only what must hold however often the link drops:
//
//   - an append consumer receives every offset, with no gap;
//   - every queue job is resolved or dead-lettered, never stranded;
//   - a `latest` key reads back what was last written to it;
//   - a retained broadcast value reaches a subscriber that arrives later;
//   - a seek is answered or the connection died - never answered wrongly;
//   - the broker is still serving at the end.
//
// Not asserted: any count, ordering or duration, all of which the link
// decides.
//
// **And each strand proves it drove something**, because the failure this
// shape is prone to is not a wrong assertion but a silent one - a soak that
// published nothing, or that never resumed a session, passes every check
// above while testing none of them.
func TestTheWholeBrokerUnderLinkChurn(t *testing.T) {
	// **Three strands, one per way a sqlite provider commits**: collected
	// behind the commit before, which is the default; a transaction per
	// publish, which is `none`; and collected for an interval, which is
	// publish_commit_interval. The write path is the same code each way, so
	// soaking only one leaves the others with no standing coverage at all.
	//
	// **Each gets a third of the dial**, so `SOAK=2m` still means about two
	// minutes and the Makefile's timeout - SOAK plus ten minutes - stays
	// right. That is affordable for the reason given above: nothing here
	// asserts a count, an ordering or a duration, so a third as many cuts is
	// as valid a run as twice as many. It is the same argument that lets
	// `make check` soak this for four seconds.
	//
	// A record count of 2 rather than something larger, because the
	// publishers here are few: a batch that could never fill would leave
	// every commit closing on the interval, which is the arrangement this
	// is least likely to find anything in.
	whole := soakFor(t, 4*time.Second)
	part := whole / 3

	t.Run("publishes collected behind the commit before", func(t *testing.T) {
		soakTheWholeBroker(t, part, commitsBehind)
	})
	t.Run("a transaction per publish", func(t *testing.T) {
		collectPublishes(t, 0, 0)
		soakTheWholeBroker(t, part, commitsEach)
	})
	t.Run("publishes collected into shared transactions", func(t *testing.T) {
		collectPublishes(t, 5*time.Millisecond, 2)
		soakTheWholeBroker(t, part, commitsWindowed)
	})
}

// How a soak strand's provider commits publishes, which is what its
// CommitStats have to show it drove.
type commitWay int

const (
	commitsBehind commitWay = iota
	commitsEach
	commitsWindowed
)

func soakTheWholeBroker(t *testing.T, run time.Duration, way commitWay) {
	const (
		devices = 3
		workers = 3
	)
	const seekReply = "loose/seek-reply"

	h := startSoaking(t, filepath.Join(t.TempDir(), "soak.db"))

	// The producer and the readers that do the accounting stay on the
	// broker's own address. Their link is not what is under test, and one
	// that stayed up is what makes the numbers at the end mean anything.
	producer := connect(t, h, "producer", true, false)
	dlq := connect(t, h, "dlq-reader", true, false)
	dlq.Sub(t, "iot/+/work/+/__dlq", 1)

	var (
		mu        sync.Mutex
		published int64
		// q2Published is how many exactly-once publishes the churned strand
		// attempted, so a run where a cut never landed inside one says so
		// rather than passing over a strand that proved nothing.
		q2Published int
		// **This is not the starvation guard**, and nothing here should be
		// relied on for it. The soak found that defect, and then turned out
		// to be the wrong place to keep watching for it: a starved channel
		// stays starved only until the next record published to it, and
		// these producers publish for the whole dial - so only a freeze left
		// standing at the very end is visible, and the longer the dial the
		// smaller that tail is in proportion. Reverted, the soak goes red at
		// the short dial and green at `SOAK=2m`, which is a guard that
		// weakens the harder you lean on it.
		// TestOneConsumerIsServedEveryChannelItsFilterReaches holds it
		// instead, at no dial at all. What is below is about churn.
		//
		// **One map per device per channel**, because one filter now reaches
		// two. `iot/+/events/+` intersects `iot/hq/#`, so a device is served
		// from `events` and from `hq`, each replaying from its own stored
		// position - and each channel numbers its records from 1. Keyed by
		// offset alone the two interleave, and a gap in one is filled by a
		// record from the other: the check would go on passing while a
		// consumer missed records, which is invariant 1's failure wearing
		// this test's own name.
		seen          = make([]map[string]map[uint64]bool, devices)
		acked         = map[string]bool{}
		resumed       int
		fresh         int
		cuts          int
		jobsPublished int
		seeksAcked    int
		seeksRefused  int
		lastRefusal   string
		returned      map[string]int
		lastState     string
	)
	for i := range seen {
		seen[i] = map[string]map[uint64]bool{soakSite: {}, soakHQ: {}}
	}
	returned = map[string]int{}

	// Every device on its own link.
	links := make([]*proxy, devices+workers+5)
	for i := range links {
		links[i] = newProxy(t, h.Addr)
	}
	seekLink, stateLink := links[devices+workers], links[devices+workers+1]
	legacyLink := links[devices+workers+2]
	sliceLink := links[devices+workers+3]

	stop := make(chan struct{})
	var writers sync.WaitGroup
	var stopOnce sync.Once
	stopWriters := func() {
		stopOnce.Do(func() { close(stop) })
		writers.Wait()
	}
	// **The writers stop before the broker does, however this test ends.**
	// A check below that fails ends the test with the writers still
	// publishing; the broker then closes under them, their next publish
	// fails, and that failure - reported after the test has finished -
	// panics the package and is the only thing printed, while the check that
	// actually failed is lost. Seen once in CI. Cleanups run last-registered
	// first, so this one runs before the harness's.
	t.Cleanup(stopWriters)

	// **Exactly-once through a link that keeps dropping**, which is the one
	// case the store for unfinished publishes exists for: MQTT hands the
	// broker ownership at the PUBREC, a round trip before the publisher is
	// told the exchange is done, so a cut between the two is precisely when
	// a broker can store a record its publisher believes it never sent.
	//
	// A durable session, because that is what makes the re-send carry the
	// *same* packet identifier: MQTT-4.4.0-1 re-sends an unacknowledged
	// PUBLISH only on a resumed session, and it is the identifier that lets
	// the broker recognise it. With a clean session every retry would be a
	// new message and there would be nothing here to prove.
	qos2Link := links[devices+workers+4]
	q2 := connect(t, &harness{Addr: qos2Link.Addr, Network: "tcp", Srv: h.Srv, Via: qos2Link}, "soak-qos2", false, false)
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			// A publish through a cut link errors; that is the case under
			// test rather than a failure, and the payload is re-sent by the
			// session rather than by this loop.
			_, _ = q2.C.Publish(context.Background(), &paho.Publish{
				Topic: "iot/site/events/q2-" + strconv.Itoa(i), QoS: 2,
				Payload: []byte("q2-" + strconv.Itoa(i)),
			})
			mu.Lock()
			q2Published = i
			mu.Unlock()
			time.Sleep(7 * time.Millisecond)
		}
	}()

	// Append and broadcast traffic, throughout, so a cut lands inside a
	// delivery rather than between two quiet moments.
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			producer.Pub(t, "iot/site/events/"+strconv.Itoa(i), "r"+strconv.Itoa(i))
			// **The same shape of topic, one level different, and it belongs
			// to another channel.** `iot/hq/events/<i>` matches both
			// `iot/+/events/+` and `iot/hq/#`, and the second holds it
			// because a spelled-out level beats `+` at level 1. Every one of
			// these walks the rule that decides between two filters while
			// the links are dropping, which is the crossing this file could
			// not reach while every channel claimed `<name>/#`.
			producer.Pub(t, "iot/hq/events/"+strconv.Itoa(i), "hq"+strconv.Itoa(i))
			mu.Lock()
			published = int64(i)
			mu.Unlock()
			// **10ms, where one channel took 5.** A device's filter reaches
			// both channels, so each one of these iterations puts two
			// records on every device's link rather than one - and at the
			// old interval that doubled what an ESP32-class window has to
			// drain, which showed up as a tail the run could not finish
			// rather than as a lost record. The per-channel rate is what it
			// always was.
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// `latest` as a key/value store, on a link that stays up: the verbs are
	// not what churn is meant to break, but the state they leave behind is
	// what a resubscribing device is served from.
	writers.Add(1)
	go func() {
		defer writers.Done()
		kv := connect(t, h, "kv-writer", true, false)
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := "v" + strconv.Itoa(i)
			kv.Pub(t, "iot/site/state/alpha", v)
			mu.Lock()
			lastState = v
			mu.Unlock()

			// The other two verbs, so the key that is written and then
			// removed is under the same churn as the one that stays. A
			// delete is MQTT's own: a publish with a zero-length payload.
			kv.Pub(t, "iot/site/state/beta", v)
			kv.Pub(t, "iot/site/state/beta", "")
			time.Sleep(20 * time.Millisecond)
		}
	}()

	dialDevice := func(n int) *client {
		c := dialThrough(t, links[n].Addr, fmt.Sprintf("esp-%d", n), esp32,
			func(topic string, offset uint64) {
				mu.Lock()
				seen[n][soakChannelOf(topic)][offset] = true
				mu.Unlock()
			}, nil)
		mu.Lock()
		if c.SessionPresent {
			resumed++
		} else {
			fresh++
		}
		mu.Unlock()
		c.Sub(t, "iot/+/events/+", 1)
		return c
	}

	dialWorker := func(n int) *client {
		// The reply comes from the handler's own client, so nothing here
		// closes over the value being assigned: a resumed session is handed
		// its unacknowledged jobs before the dial has returned.
		c := dialThrough(t, links[devices+n].Addr, fmt.Sprintf("esp-worker-%d", n), esp32, nil,
			func(payload string, reply func(string)) {
				// **Some jobs are always returned**, so the attempt is spent
				// and the record walks the whole lifecycle: return, backoff,
				// redelivery, attempts exhausted, dead-letter. A worker that
				// only ever acknowledges drives none of that, and the queue
				// half of this test then covers the easy path only.
				//
				// Which jobs is decided here rather than by the link,
				// because "some jobs dead-lettered" has to be true of every
				// run for the assertion below to mean anything.
				if strings.HasSuffix(payload, "4") || strings.HasSuffix(payload, "8") {
					reply("return")
					mu.Lock()
					returned[payload]++
					mu.Unlock()
					return
				}
				reply("ack")
				mu.Lock()
				acked[payload] = true
				mu.Unlock()
			})
		c.Sub(t, "$saguin/queue/jobs", 1)
		return c
	}

	// A device that reads `latest` as subscription state rather than
	// through the verbs: on every reconnect it is served the current value
	// for each key, which is the catch-up path.
	dialStateReader := func() *client {
		c := dialThrough(t, stateLink.Addr, "esp-state", esp32, nil, nil)
		c.Sub(t, "iot/+/state/+", 1)
		return c
	}

	// The seeker holds a durable session on the append channel and keeps it
	// through the whole run: its own link is never cut, because what is
	// being driven is a seek landing while deliveries are in flight and
	// other sessions are resuming, not the seek's own socket dying.
	seeker := dialThrough(t, seekLink.Addr, "esp-seeker", esp32, nil, nil)
	seeker.Sub(t, "iot/+/events/+", 1)
	seeker.Sub(t, seekReply, 1)

	// **A 3.1.1 device on the same estate**, because the broker now admits
	// two protocols and soaking one of them is soaking half the fleet. It
	// cannot join the offset accounting below - a 3.1.1 delivery carries no
	// `saguin-offset`, which is the whole of what that accounting reads - so
	// it asserts what it *can* see, and the strongest of those is a rule the
	// churn is most likely to break.
	//
	// **It subscribes to `#` and must never be handed queue work.** A
	// wildcard crossing a queue is served everything it matches except the
	// queue's records, and a 3.1.1 client cannot form the shared
	// subscription that would make it a worker. Under churn, with workers
	// reconnecting and jobs redelivered throughout, a single queue record
	// arriving here is a job handed to a client that can never acknowledge
	// it - the failure RFC 0002 "What each channel type admits" refuses
	// queues to 3.1.1 to prevent, on the path least likely to be
	// noticed.
	var legacyMu sync.Mutex
	legacySeen, legacyQueue := 0, []string{}
	dialLegacy := func() mqttv3.Client {
		c, _ := legacyWithDefault(t, &harness{Addr: legacyLink.Addr}, "esp-legacy", false,
			func(_ mqttv3.Client, m mqttv3.Message) {
				legacyMu.Lock()
				legacySeen++
				// **Live queue records only.** A dead-lettered one lands on
				// a topic inside the queue's filter too, and a wildcard
				// subscriber is *correctly* served those: the dead-letter
				// channel is an ordinary append channel and its reader is
				// not a worker asking wrongly (RFC 0002). The first run of
				// this strand flagged five of them and every one was a
				// `__dlq` topic - the assertion was too broad, not the
				// broker wrong.
				if strings.HasPrefix(m.Topic(), "iot/site/work/") &&
					!strings.Contains(m.Topic(), channel.DLQSuffix) {
					legacyQueue = append(legacyQueue, m.Topic())
				}
				legacyMu.Unlock()
			})
		// **What the broker answered, and how long it took to answer it.**
		// This said only "could not subscribe", which is the wrapper's own
		// timeout reported as though it were the broker's reply - test rule
		// 7's trap, and it made a failure here undiagnosable: a reader
		// could not tell a SUBACK that never came from one that came late,
		// and those are a broker defect and an instrument that outgrew its
		// own dial respectively.
		//
		// The wait stays at five seconds, which is what a SUBACK on loopback
		// has to beat. A resumed session redelivering a large backlog is not
		// a reason to wait longer: the SUBACK is the broker saying it
		// registered the subscription, and it queueing behind a redelivery
		// is the thing worth failing over.
		started := time.Now()
		tok := c.Subscribe("#", 1, nil)
		if !tok.WaitTimeout(5 * time.Second) {
			// **Recorded, and the strand carries on.** This was a Fatal,
			// and a Fatal here ends the run before every later assertion -
			// so this one failure hid two others for as long as it has been
			// red: a duplicate the contract permits, and a `latest`
			// catch-up that was never served. The first
			// was found and hid the second.
			//
			// Later assertions about this subscriber may now fail as well,
			// because it is connected and not subscribed. That is the point:
			// they say what a client in that state does not receive, and
			// each names itself.
			t.Errorf("the 3.1.1 subscriber's SUBACK did not arrive within 5s of its "+
				"SUBSCRIBE (waited %s, client reported %v): on loopback that is the "+
				"broker not answering, and the session it resumed was redelivering a "+
				"backlog at the time. Anything below about this subscriber follows from "+
				"it rather than being a second defect",
				time.Since(started).Round(time.Millisecond), tok.Error())
		}
		if took := time.Since(started); took > time.Second {
			// Not a failure and worth seeing: a SUBACK a second behind on
			// loopback is the shape of the failure above, one dial short of
			// it.
			t.Logf("the 3.1.1 subscriber's SUBACK took %s", took.Round(time.Millisecond))
		}
		return c
	}
	legacyDev := dialLegacy()

	// **One partitioned consumer, dropping and resuming with the rest.**
	// This feature sits exactly on the soak's subject: a partitioned
	// consumer holds one cursor into the channel and steps that cursor past
	// records outside its slice, and reconnection is where cursors go
	// wrong. A member that resumes and is then served a record belonging to
	// another slice, or misses one of its own, is a bug no table test
	// reaches - the table tests never drop a link mid-replay.
	//
	// `iot/site/events/+` rather than `iot/+/events/+`, so every topic it
	// can match belongs to one channel and the expected set below is
	// arithmetic rather than a second copy of the routing rules.
	const sliceCount, sliceIndex = 4, 1
	var sliceMu sync.Mutex
	sliceGot := map[string]int{}
	// **Where a position went backwards, if it ever did.** A duplicate is
	// permitted here - saguin is at-least-once and this strand cuts the
	// link on purpose - so counting duplicates says nothing. What is never
	// permitted is the position moving *backwards within one connection*:
	// the offsets a consumer is served ascend, and redelivery re-sends from
	// the stored position after a reconnect rather than rewinding under a
	// live one (invariant 9). That fires on a single record, needs no
	// threshold, and is true of the contract rather than of the dial - so
	// it catches a rewinding position, which is the pathology the duplicate
	// count was reaching for and could not express.
	var sliceLast uint64
	var sliceRewinds []string
	dialSlice := func() *client {
		sliceMu.Lock()
		// A new connection starts a new run of offsets: redelivery from the
		// stored position is exactly the case that is allowed, so the
		// comparison restarts here rather than spanning the cut.
		sliceLast = 0
		sliceMu.Unlock()
		c := dialThrough(t, sliceLink.Addr, "esp-slice", esp32,
			func(topic string, offset uint64) {
				sliceMu.Lock()
				sliceGot[topic]++
				if offset < sliceLast {
					sliceRewinds = append(sliceRewinds,
						fmt.Sprintf("%s at %d after %d", topic, offset, sliceLast))
				}
				sliceLast = offset
				sliceMu.Unlock()
			}, nil)
		c.SubSliced(t, "iot/site/events/+", sliceCount, sliceIndex)
		return c
	}
	sliceDev := dialSlice()

	devs := make([]*client, devices)
	for i := range devs {
		devs[i] = dialDevice(i)
	}
	work := make([]*client, workers)
	for i := range work {
		work[i] = dialWorker(i)
	}
	stateReader := dialStateReader()

	// **Jobs throughout, not a batch at the start.** Twelve published once
	// meant the queue was idle for all but the first second of a two-minute
	// dial: the append channel scaled with SOAK and the queue did not, so a
	// longer soak soaked three channel types and watched the fourth.
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			producer.Pub(t, "iot/site/work/"+strconv.Itoa(i), "job-"+strconv.Itoa(i))
			mu.Lock()
			jobsPublished = i
			mu.Unlock()
			time.Sleep(80 * time.Millisecond)
		}
	}()

	// A retained broadcast value, published before the churn, so that a
	// subscriber arriving after all of it must still be given the value.
	producer.PubRetained(t, "loose/beacon", "beacon-1")

	// The churn. One link at a time rather than all of them, at a jittered
	// interval rather than a fixed beat: a single radio going away while
	// everything else keeps running is the interleaving the old shape could
	// never produce, and the correlated case is still reached because the
	// intervals overlap.
	deadline := time.Now().Add(run)
	for i := 0; time.Now().Before(deadline); i++ {
		time.Sleep(time.Duration(60+(i*37)%180) * time.Millisecond)

		switch which := i % (devices + workers + 1); {
		case which < devices:
			links[which].Cut()
			time.Sleep(time.Duration(40+(i*23)%120) * time.Millisecond)
			links[which].Restore()
			devs[which].Close()
			devs[which] = dialDevice(which)
		case which < devices+workers:
			n := which - devices
			links[devices+n].Cut()
			time.Sleep(time.Duration(40+(i*29)%120) * time.Millisecond)
			links[devices+n].Restore()
			work[n].Close()
			work[n] = dialWorker(n)
		default:
			// The whole estate at once, which is a gateway going away -
			// every link but the seeker's, which is held for the reason
			// given where it is dialled.
			for j, l := range links {
				if l != seekLink {
					_ = j
					l.Cut()
				}
			}
			time.Sleep(120 * time.Millisecond)
			for _, l := range links {
				if l != seekLink {
					l.Restore()
				}
			}
			for j := range devs {
				devs[j].Close()
				devs[j] = dialDevice(j)
			}
			// The 3.1.1 device goes with the estate: its link is cut with
			// the rest, so it reconnects on a durable session exactly as
			// the MQTT 5 ones do.
			legacyDev.Disconnect(50)
			legacyDev = dialLegacy()
			sliceDev.Close()
			sliceDev = dialSlice()
			for j := range work {
				work[j].Close()
				work[j] = dialWorker(j)
			}
			stateReader.Close()
			stateReader = dialStateReader()
		}
		mu.Lock()
		cuts++
		mu.Unlock()

		// A seek while records are arriving and other links are moving,
		// which is the join the era machinery exists for and the one guard
		// site nothing else here drives end to end.
		//
		// **Published directly rather than through the seek helpers.** Both
		// of those call t.Fatalf when the write fails, and a write failing
		// is the ordinary outcome on a link somebody has just cut - that is
		// this test failing on the instrument rather than on the broker.
		//
		// **The reason code, not merely the PUBACK.** The first version of
		// this counted a seek whenever Publish returned no error, and paho
		// returns none for a refusal - so 266 seeks were counted on a soak
		// where every one of them was answered `refused a seek ... reason=
		// malformed`, because the payload was a JSON object this broker has
		// never accepted. It is a bare offset: -1 for the end. The counter
		// was reporting on work it had not done, which is the failure this
		// whole test is written to avoid, and it took a soak's log to see.
		//
		// **The reply, not the PUBACK.** The first version counted a seek
		// whenever Publish returned no error, and 266 were counted on a
		// soak where every one was answered `refused a seek ... reason=
		// malformed`: the payload was a JSON object this broker has never
		// accepted - it is a bare offset, -1 for the end. The PUBACK could
		// not have shown it either, because refuseSeek answers on the
		// Response Topic and this client had set none, so the refusal had
		// nowhere to go. A strand that cannot see a refusal cannot tell
		// working from broken, which is what it is here to do.
		corr := "seek-" + strconv.Itoa(i)
		if _, err := seeker.C.Publish(context.Background(), &paho.Publish{
			Topic:   "$saguin/consumer/events/seek",
			QoS:     1,
			Payload: []byte("-1"),
			Properties: &paho.PublishProperties{
				ResponseTopic:   seekReply,
				CorrelationData: []byte(corr),
			},
		}); err == nil {
			// **Everything received, not the channel.** Records for this
			// subscription and the reply arrive together, and the client's
			// channel is a 256-deep buffer that drops when full - so
			// reading the reply from it found three of seventeen and called
			// the other fourteen nothing at all. Everything received is
			// also kept in a slice that does not drop, and the correlation
			// data is what picks this seek's reply out of it.
			//
			// A stored offset is digits; a refusal is a reason.
			for waited := time.Duration(0); waited < 2*time.Second; waited += 100 * time.Millisecond {
				var reply string
				for _, r := range seeker.All() {
					if r.Topic == seekReply && string(r.CorrData) == corr {
						reply = r.Payload
						break
					}
				}
				if reply == "" {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				mu.Lock()
				if _, err := strconv.ParseUint(reply, 10, 64); err == nil {
					seeksAcked++
				} else {
					seeksRefused++
					lastRefusal = reply
				}
				mu.Unlock()
				break
			}
		}
	}

	stopWriters()
	mu.Lock()
	total := published
	wantState := lastState
	q2Sent := q2Published
	mu.Unlock()

	// **Every exactly-once payload lands once or not at all, never twice.**
	// A publish the cut interrupted may be missing - the exchange did not
	// finish and the broker stored nothing, which is the honest outcome -
	// but a payload in the channel twice is the defect this whole store
	// exists to remove, and only a soak with the link dropping mid-exchange
	// can produce it.
	if q2Sent < 5 {
		t.Errorf("only %d exactly-once publishes were attempted, which is too few for "+
			"a cut to have landed inside one: this strand proved nothing", q2Sent)
	}
	q2Seen := map[string]int{}
	q2Reader := connect(t, h, "soak-qos2-auditor", true, false)
	q2Reader.Sub(t, "iot/site/events/#", 1)
	for {
		r, ok := q2Reader.Await(t, 2*time.Second)
		if !ok {
			break
		}
		if strings.HasPrefix(r.Payload, "q2-") {
			q2Seen[r.Payload]++
		}
	}
	for payload, n := range q2Seen {
		if n > 1 {
			t.Errorf("the exactly-once payload %q is in the channel %d times. A cut between "+
				"the receipt and the release must leave the record unstored, so the "+
				"publisher's re-send lands once", payload, n)
		}
	}
	if len(q2Seen) == 0 {
		t.Errorf("not one of the %d exactly-once publishes reached the channel, so this "+
			"strand cannot have found a duplicate either", q2Sent)
	}

	// Settle with every link up: a record taken back by the last cut needs a
	// redelivery, and a redelivery needs a worker with room. Polled rather
	// than slept, so a slow machine takes longer instead of failing.
	deadRecords := func() int {
		seenDead := map[string]bool{}
		for _, r := range dlq.All() {
			seenDead[r.Payload] = true
		}
		return len(seenDead)
	}
	complete := func() bool {
		mu.Lock()
		defer mu.Unlock()
		for i := range seen {
			for _, ch := range []string{soakSite, soakHQ} {
				if len(seen[i][ch]) < int(total) {
					return false
				}
			}
		}
		return len(acked)+deadRecords() >= jobsPublished
	}
	for waited := time.Duration(0); waited < 30*time.Second && !complete(); waited += 200 * time.Millisecond {
		time.Sleep(200 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	// The gap, which is the failure. Named as the first missing offset,
	// because "esp-2 never received offset 41" is a thing to go and look at
	// and "17 of 60" is not.
	for i := range seen {
		for _, ch := range []string{soakSite, soakHQ} {
			for off := uint64(1); off <= uint64(total); off++ {
				if !seen[i][ch][off] {
					t.Fatalf("esp-%d never received offset %d of %d published in channel "+
						"%q: a durable consumer that resumes past a record it was never "+
						"given reports success over it, which is invariant 1's failure. "+
						"It holds %d offsets there.",
						i, off, total, ch, len(seen[i][ch]))
				}
			}
		}
	}

	// **And both channels actually carried traffic**, which the loop above
	// cannot say on its own: two empty maps satisfy every offset from 1 to
	// nothing. If the overlap ever stops resolving the way it does - if
	// `iot/hq/events/7` starts landing in `events` - one of these is zero
	// and the run says so, rather than passing over a rule it stopped
	// crossing.
	for i := range seen {
		for _, ch := range []string{soakSite, soakHQ} {
			if len(seen[i][ch]) == 0 {
				t.Errorf("esp-%d received nothing at all from channel %q, so this run "+
					"never crossed the rule that decides between two filters matching "+
					"one topic", i, ch)
			}
		}
	}

	// Resolved or dead-lettered, never in between. A job that is neither is
	// held by a worker that is not there: no deadline to rescue it, no
	// attempt spent, and nobody else offered it (invariant 7).
	dead := deadRecords()
	if len(acked)+dead < jobsPublished {
		t.Errorf("%d of %d jobs are neither acknowledged (%d) nor dead-lettered (%d): "+
			"the rest are stranded, held by a worker whose link went away",
			jobsPublished-(len(acked)+dead), jobsPublished, len(acked), dead)
	}

	// The broker is still serving, and the state it serves is current: a
	// fresh reader is given the last value written to the key, and the
	// retained broadcast value reaches a subscriber that was never there
	// for the publish.
	after := connect(t, h, "after-the-storm", true, false)
	after.Sub(t, "iot/+/state/+", 1)
	after.Sub(t, "loose/#", 1)
	var gotState, gotBeacon string
	for waited := time.Duration(0); waited < 5*time.Second; waited += 100 * time.Millisecond {
		for _, r := range after.All() {
			if r.Topic == "iot/site/state/alpha" {
				gotState = r.Payload
			}
			if r.Topic == "loose/beacon" {
				gotBeacon = r.Payload
			}
		}
		if gotState != "" && gotBeacon != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if gotState != wantState {
		t.Errorf("a reader arriving after the churn was served %q for state/device/alpha, "+
			"want %q: latest is the current value or it is not a state store", gotState, wantState)
	}
	if gotBeacon != "beacon-1" {
		t.Errorf("the retained broadcast value was served as %q, want %q: a subscriber "+
			"that arrives later is the only reason to retain it", gotBeacon, "beacon-1")
	}

	// The verbs, read back after everything has settled. The point read is
	// the one that can tell absent from present, which subscribing cannot:
	// an absent key answers a subscriber with silence, and silence is also
	// what a dropped link looks like.
	reader := connect(t, h, "kv-reader", true, false)
	if got := string(reader.KvGet(t, "iot/site/state/alpha", nil).Payload); got != wantState {
		t.Errorf("a point read of state/device/alpha answered %q, want %q: a latest "+
			"channel serves the current value or it is not a state store", got, wantState)
	}
	if got := reader.KvGet(t, "iot/site/state/beta", nil); len(got.Payload) != 0 {
		t.Errorf("a point read of the deleted state/device/beta answered %q, want an "+
			"empty payload: a delete that survives churn has to stay deleted", got.Payload)
	}

	// The same state as subscription catch-up rather than as a verb: a
	// device that resubscribes after its link came back is served the
	// current value for every key, and that is the path a device actually
	// uses.
	var caughtUp string
	for _, r := range stateReader.All() {
		if r.Topic == "iot/site/state/alpha" {
			caughtUp = r.Payload
		}
	}
	if caughtUp == "" {
		t.Errorf("esp-state resubscribed through the churn and was never served a value " +
			"for state/device/alpha: catch-up is what a latest channel gives a device " +
			"that has been away")
	}

	// What the instrument has to prove about itself. Without records the gap
	// loop asserts nothing; without a resumed session no resumption was
	// tested at all, because a consumer given a fresh session replays from
	// the floor and receives everything anyway.
	if total == 0 || jobsPublished == 0 {
		t.Fatalf("nothing was published (%d records, %d jobs), so this test proves "+
			"nothing", total, jobsPublished)
	}
	if resumed == 0 {
		t.Fatalf("no reconnect resumed a session (%d fresh): every device replayed from "+
			"the floor, so this passed without testing a resume at all", fresh)
	}
	if cuts == 0 {
		t.Fatal("the link never dropped, so this is not a soak")
	}
	if len(acked) == 0 {
		t.Fatal("no job was ever acknowledged, so the queue drove nothing")
	}
	if len(returned) == 0 {
		t.Fatal("no job was ever returned, so the retry and dead-letter paths were " +
			"never driven and the queue half of this covers the easy path only")
	}
	if dead == 0 {
		t.Errorf("%d jobs were returned but none reached the dead-letter channel: "+
			"attempts are meant to run out, and a queue that never dead-letters "+
			"holds failing work for ever", len(returned))
	}
	if seeksAcked == 0 {
		t.Error("no seek was acknowledged during the churn, so the era guard was never driven")
	}
	if seeksRefused > 0 {
		t.Errorf("%d seeks were refused by the broker (%q): a refusal is not a seek, "+
			"and counting one as driven is how this strand came to report 266 seeks "+
			"against a broker that had accepted none", seeksRefused, lastRefusal)
	}
	// **And which write path it drove**, because "each strand proves it
	// drove something" is the rule this file states about itself and the
	// strands are otherwise identical. Everything above - cuts, sessions,
	// records, jobs, seeks - reads exactly the same whether the publishes
	// were collected or not, so a wiring fault between collectPublishes and
	// the harness would leave both strands soaking the default path and the
	// summary saying nothing about it. That is the default path wearing the
	// other one's name.
	//
	// Both closers for the windowed strand, because reaching only one of
	// them means the dial is wrong for the traffic rather than right: at
	// (5ms, 2) the four-second run reaches ~130 by the record count and
	// ~210 by the interval, so neither assertion is near its margin.
	st := h.DB.CommitStats()
	switch {
	case way == commitsWindowed && st.ClosedByRecords == 0:
		t.Error("no transaction was closed by the record count, so this strand did not collect " +
			"publishes and is another strand under a different name")
	case way == commitsWindowed && st.ClosedByInterval == 0:
		t.Error("no transaction was closed by the interval, so nothing here waited for a batch " +
			"that would not fill - half of what collecting does was never driven")
	case way == commitsWindowed && st.Unbatched+st.ClosedByCommit > 0:
		t.Errorf("%d transactions were not collected for an interval on a provider asked to",
			st.Unbatched+st.ClosedByCommit)
	case way == commitsEach && st.Unbatched == 0:
		t.Error("no transaction carried a single publish, so this strand did not drive the " +
			"write path it is named for")
	case way == commitsEach && st.ClosedByRecords+st.ClosedByInterval+st.ClosedByCommit > 0:
		t.Errorf("%d transactions collected publishes on a provider that was asked not to",
			st.ClosedByRecords+st.ClosedByInterval+st.ClosedByCommit)
	case way == commitsBehind && st.ClosedByCommit == 0:
		t.Error("no transaction was closed behind the commit before, so this strand did not " +
			"drive the default write path it is named for")
	case way == commitsBehind && st.Unbatched+st.ClosedByInterval > 0:
		t.Errorf("%d transactions were stored singly or waited for an interval on a provider "+
			"that was asked for neither", st.Unbatched+st.ClosedByInterval)
	}

	// **The 3.1.1 strand's two assertions**, and the first is what makes the
	// second mean anything: a subscriber that received nothing proves no
	// rule at all.
	legacyMu.Lock()
	sawLegacy, queueLeaks := legacySeen, append([]string(nil), legacyQueue...)
	legacyMu.Unlock()

	// **The partitioned member, both halves.** Nothing from another slice
	// may have reached it, and nothing from its own may be missing - the
	// two are different bugs and each reads as success from one side.
	//
	// The expected set is computed here from the algorithm RFC 0003
	// specifies rather than from the broker's own function, for the reason
	// the end-to-end tests do it: accepting whatever arrived would pass
	// against a hash that disagreed with the specification.
	sliceMu.Lock()
	got := map[string]int{}
	for k, v := range sliceGot {
		got[k] = v
	}
	sliceMu.Unlock()

	// **A margin at the tail**, because the writers stop and the last
	// records are still crossing a link that may be cut. Records below it
	// have had the settle window and are owed.
	const tailMargin = 25
	owed, missing, strays, dupes := 0, []string{}, []string{}, 0
	for i := int64(1); i <= published-tailMargin; i++ {
		topic := "iot/site/events/" + strconv.FormatInt(i, 10)
		if ownerOf(topic, sliceCount) != sliceIndex {
			continue
		}
		owed++
		switch n := got[topic]; {
		case n == 0:
			missing = append(missing, topic)
		case n > 1:
			dupes++
		}
	}
	for topic := range got {
		if ownerOf(topic, sliceCount) != sliceIndex {
			strays = append(strays, topic)
		}
	}
	if len(strays) > 0 {
		t.Errorf("the partitioned consumer was served %d records outside its slice "+
			"(%v): a member that receives another slice's records through a "+
			"reconnect is two members holding one record, which is the duplicate "+
			"half of a partitioning bug",
			len(strays), strays[:min(6, len(strays))])
	}
	if len(sliceRewinds) > 0 {
		t.Errorf("the partitioned consumer's position moved backwards %d times within a "+
			"single connection (%v): redelivery after a reconnect is at-least-once "+
			"behaving as promised, but a position that rewinds under a live connection "+
			"is serving records the consumer has already acknowledged - invariant 9, and "+
			"the failure the duplicate count could not tell from the contract",
			len(sliceRewinds), sliceRewinds[:min(6, len(sliceRewinds))])
	}
	if len(missing) > 0 {
		t.Errorf("the partitioned consumer never received %d of the %d records in its "+
			"own slice (%v): its cursor stepped past a record it owned, and a "+
			"partitioned consumer's position moves past what it skips - so nothing "+
			"brings those back",
			len(missing), owed, missing[:min(6, len(missing))])
	}
	// **A duplicate is not a defect here, and asserting it was is this
	// instrument promising more than the broker does.** Sagüin is
	// at-least-once and an `append` consumer is redelivered "on reconnect,
	// from the stored position" (RFC 0003's own table): the position moves
	// on the acknowledgement, so a link cut between a delivery and its
	// PUBACK re-sends that record by design. This strand cuts the link
	// hundreds of times on purpose, so it is *manufacturing* the one case
	// the contract permits.
	//
	// It passed for as long as it did because a cut rarely lands inside
	// that window: at the short dial the suite runs, none of the hundreds
	// of cuts did. At a ten-minute dial one of 2,716 records arrived twice
	// and the soak called it a failure - a gate that goes red the longer
	// you run it, for behaviour the RFC sets out in its first table.
	//
	// What is still asserted is everything the broker does promise, above:
	// every record in the slice arrives, and nothing from another slice
	// ever does. The count is reported so a redelivery *storm* - a position
	// that rewinds rather than advances - is visible to whoever reads the
	// line, which one silent duplicate is not.
	// What this strand has to prove about itself: a partitioned consumer
	// that was owed nothing asserted nothing.
	if owed == 0 {
		t.Fatalf("the partitioned consumer was owed no records of %d published: slice "+
			"%d of %d covered nothing, so both assertions above passed over an "+
			"empty set", published, sliceIndex, sliceCount)
	}
	if sawLegacy == 0 {
		t.Error("the 3.1.1 subscriber received nothing across the whole run, so what it " +
			"did not receive says nothing")
	}
	if len(queueLeaks) > 0 {
		t.Errorf("the 3.1.1 wildcard subscriber was handed %d queue records (%v): a "+
			"wildcard crossing a queue is served everything except its records, and a "+
			"3.1.1 client cannot form the shared subscription that would make it a "+
			"worker - so each of these is work handed to a client that can never "+
			"acknowledge it", len(queueLeaks), queueLeaks[:min(6, len(queueLeaks))])
	}

	t.Logf("%d cuts, %d resumed sessions (%d fresh), %d records each to %d devices, "+
		"%d jobs (%d acknowledged, %d dead-lettered, %d returns), %d seeks accepted; "+
		"commits: %d by the record count, %d by the interval, %d behind the commit before, %d uncollected; "+
		"the 3.1.1 subscriber received %d records and %d queue leaks; "+
		"the partitioned consumer took %d of %d owed in slice %d of %d, %d strays, "+
		"%d redelivered (at-least-once, and what a cut mid-window costs)",
		cuts, resumed, fresh, total, devices, jobsPublished, len(acked), dead, len(returned), seeksAcked,
		st.ClosedByRecords, st.ClosedByInterval, st.ClosedByCommit, st.Unbatched, sawLegacy, len(queueLeaks),
		owed-len(missing), owed, sliceIndex, sliceCount, len(strays), dupes)
}
