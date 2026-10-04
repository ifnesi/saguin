package broker_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/brokertest"
)

// What the broker-wide lock costs at the subscriber counts a fleet has,
// which the other publish benchmarks stop short of at fifty.
//
// The scale run wedged at 20,000 consumers: 36,464 goroutines queued on
// b.mu, liveness paths among them, PUBACKs 15s late and /health at 503.
// These three rows are that incident on one machine, one per path that
// holds the lock for work proportional to a population:
//
//   - PublishAtScale: fanout10, where every publish reaches ten consumers
//     whatever N is, so what is lost as N rises is spent on subscribers the
//     record does not reach; and wide, every consumer taking every record.
//   - PumpHoldOnBacklog: one consumer, the default Receive Maximum, and a
//     65,535-record backlog - the longest single hold a client can ask for.
//   - DisconnectStorm: N consumers with unsaved positions drop at once, as
//     19,266 did in the run when write_timeout fired.
//
// Each reports lock_wait_max_ms and lock_wait_p99_ms: how long a bare take
// of the lock waited while the row ran, which is what a PINGREQ hook, a
// disconnect or the health probe pays behind it. Responsive is that probe.
//
// Rows above 500 consumers run only with SAGUIN_BENCH_MAX_CONSUMERS set,
// because 20,000 in-process clients is 40,000 sockets and some GB.
var scaleCounts = []int{50, 500, 2000, 5000, 20000}

func scaleRows(b *testing.B) []int {
	limit := 500
	if v := os.Getenv("SAGUIN_BENCH_MAX_CONSUMERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			b.Fatalf("SAGUIN_BENCH_MAX_CONSUMERS=%q: %v", v, err)
		}
		limit = n
	}
	var rows []int
	for _, n := range scaleCounts {
		if n <= limit {
			rows = append(rows, n)
		}
	}
	return rows
}

// raiseConnections lets a row hold more clients than the harness's
// default ceiling, and puts the ceiling back after it.
func raiseConnections(b testing.TB, n int) {
	old := brokertest.Limits.MaxConnections
	brokertest.Limits.MaxConnections = int64(n) + 64
	b.Cleanup(func() { brokertest.Limits.MaxConnections = old })
}

// leanConsumer is a subscriber that counts what it is sent and keeps none
// of it. The harness client keeps every delivery, which at 20,000 clients
// is the benchmark measuring its own memory.
type leanConsumer struct {
	c    *paho.Client
	conn net.Conn
	got  *atomic.Int64
}

func dialLean(b testing.TB, h *harness, id string, expiry uint32, got *atomic.Int64) (*leanConsumer, error) {
	return dialLeanCounting(b, h, id, expiry, got, nil)
}

// dialLeanCounting is dialLean that also counts payload bytes. **Bytes as
// well as messages**, because a gateway profile's whole subject is how many
// of them the broker moves: a row that delivered every message at a tenth
// of the bytes is a row whose MB/s is fiction, and counting messages alone
// cannot tell the two apart.
func dialLeanCounting(b testing.TB, h *harness, id string, expiry uint32,
	got, bytesGot *atomic.Int64) (*leanConsumer, error) {
	conn, err := net.Dial("tcp", h.Addr)
	if err != nil {
		return nil, err
	}
	c := paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				got.Add(1)
				if bytesGot != nil {
					bytesGot.Add(int64(len(pr.Packet.Payload)))
				}
				return true, nil
			},
		},
	})
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID: id, CleanStart: true, KeepAlive: 0,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if ca.ReasonCode != 0 {
		conn.Close()
		return nil, fmt.Errorf("%s: CONNACK 0x%02x", id, ca.ReasonCode)
	}
	return &leanConsumer{c: c, conn: conn, got: got}, nil
}

// connectLean brings up n consumers, 64 at a time, each subscribed to
// filter(i) at QoS 1 and acknowledged before it counts.
func connectLean(b testing.TB, h *harness, n int, expiry uint32, filter func(int) string, got *atomic.Int64) []*leanConsumer {
	return connectLeanCounting(b, h, n, expiry, filter, got, nil)
}

func connectLeanCounting(b testing.TB, h *harness, n int, expiry uint32,
	filter func(int) string, got, bytesGot *atomic.Int64) []*leanConsumer {
	out := make([]*leanConsumer, n)
	errs := make(chan error, n)
	sem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			lc, err := dialLeanCounting(b, h, fmt.Sprintf("scale-%d", i), expiry, got, bytesGot)
			if err != nil {
				errs <- err
				return
			}
			sa, err := lc.c.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{{Topic: filter(i), QoS: 1}},
			})
			if err != nil || len(sa.Reasons) != 1 || sa.Reasons[0] != 1 {
				errs <- fmt.Errorf("scale-%d subscribe %s: %v %v", i, filter(i), err, sa)
				return
			}
			out[i] = lc
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		for _, lc := range out {
			lc.conn.Close()
		}
	})
	return out
}

// probeResult is what one run of lockProbe measured.
type probeResult struct {
	samples []time.Duration
	skipped int // ticks with probeCap probes already waiting
}

const probeCap = 256

// lockProbe starts a bare take of the broker's lock every 5ms until
// stopped, each in its own goroutine, so arrivals stay periodic however
// long one waits. A single probe that waited in line would drop the ticks
// meanwhile, and a 250ms hold would count once where a client arriving at
// random had 250ms to land in - the p99 would be optimistic in proportion
// to the very waits it exists to show.
//
// It is a lower bound on what a liveness path pays: it takes the lock
// once, and OnDisconnect takes it several times in sequence.
func lockProbe(h *harness) (stop func() probeResult) {
	var (
		mu          sync.Mutex
		res         probeResult
		wg          sync.WaitGroup
		outstanding atomic.Int32
	)
	probe := func() {
		defer wg.Done()
		defer outstanding.Add(-1)
		start := time.Now()
		h.B.Responsive(10 * time.Minute)
		d := time.Since(start)
		mu.Lock()
		res.samples = append(res.samples, d)
		mu.Unlock()
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		// One at once, so a row shorter than a tick still has a sample.
		for {
			if outstanding.Load() < probeCap {
				outstanding.Add(1)
				wg.Add(1)
				go probe()
			} else {
				mu.Lock()
				res.skipped++
				mu.Unlock()
			}
			select {
			case <-done:
				return
			case <-t.C:
			}
		}
	}()
	return func() probeResult {
		close(done)
		<-finished
		wg.Wait()
		return res
	}
}

func reportLockWait(b *testing.B, res probeResult) {
	samples := res.samples
	if len(samples) == 0 {
		b.Fatal("the lock probe took no samples, so it measured nothing")
	}
	slices.Sort(samples)
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	b.ReportMetric(ms(samples[len(samples)-1]), "lock_wait_max_ms")
	b.ReportMetric(ms(samples[len(samples)*99/100]), "lock_wait_p99_ms")
	b.ReportMetric(float64(len(samples)), "lock_samples")
	b.ReportMetric(float64(res.skipped), "lock_probes_skipped")
}

// assertDrained refuses a row whose throughput is not comparable: one that
// delivered 60% would otherwise print msg/s beside rows that delivered all.
func assertDrained(b *testing.B, delivered, want int64) {
	if delivered == 0 {
		b.Fatalf("no consumer received anything; wanted %d deliveries", want)
	}
	if f := float64(delivered) / float64(want); f < 0.99 {
		b.Fatalf("delivered %d of %d (%.3f) within the drain limit, so this row's rates "+
			"are not comparable with the others", delivered, want, f)
	}
}

// assertDrainedBytes is assertDrained's other half, and the gateway rows
// need both. **A row can deliver every message and a fraction of the
// bytes** - a payload truncated somewhere, or a size parameter that never
// reached the publisher - and the message count alone reports that as a
// clean row with an MB/s figure nobody can trust.
func assertDrainedBytes(b *testing.B, delivered, want int64) {
	if delivered == 0 {
		b.Fatalf("no payload bytes arrived; wanted %d", want)
	}
	if f := float64(delivered) / float64(want); f < 0.99 {
		b.Fatalf("delivered %d of %d payload bytes (%.3f), so this row's MB/s is not "+
			"comparable with the others", delivered, want, f)
	}
}

// awaitAtLeast waits for got to reach want, and says how far it got.
func awaitAtLeast(got *atomic.Int64, want int64, limit time.Duration) int64 {
	return awaitAccounted(got, func() int64 { return 0 }, want, limit)
}

// awaitAccounted waits until what arrived and what the broker gave up
// together reach want, and says how much arrived. A session at
// limits.session_queue_bytes gives up its oldest and counts it (RFC 0002),
// so a wait for arrivals alone runs out the limit on a row whose every
// delivery is accounted for, and charges the limit to the row's rate.
func awaitAccounted(got *atomic.Int64, gaveUp func() int64, want int64, limit time.Duration) int64 {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if n := got.Load(); n+gaveUp() >= want {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
	return got.Load()
}

// Two shapes. fanout10: every record reaches ten consumers whatever N is,
// so what the row loses as N rises is spent on subscribers it does not
// reach. wide: every consumer takes every record, which is the delivery
// ceiling on one channel - the row that says whether the wall is gone or
// has only moved once the walks stop being over the whole population.
func BenchmarkPublishAtScale(b *testing.B) {
	payload := []byte(strings.Repeat("x", 128))
	const publishers = 8

	for _, shape := range []struct {
		name   string
		fanout func(n int) int // consumers each record reaches
	}{
		{"fanout10", func(n int) int { return min(10, n) }},
		{"wide", func(n int) int { return n }},
	} {
		for _, n := range scaleRows(b) {
			b.Run(fmt.Sprintf("memory/%s/%dcons", shape.name, n), func(b *testing.B) {
				raiseConnections(b, n+publishers)
				h := start(b)
				groups := n / shape.fanout(n)
				var got atomic.Int64
				connectLean(b, h, n, 3600, func(i int) string {
					return fmt.Sprintf("events/device/%d/#", i%groups)
				}, &got)

				pubs := make([]*client, publishers)
				for i := range pubs {
					pubs[i] = connect(b, h, fmt.Sprintf("bench-pub-%d", i), true, false)
				}
				per := max(b.N/publishers, 1)
				total := per * publishers

				stop := lockProbe(h)
				b.ResetTimer()
				began := time.Now()
				var wg sync.WaitGroup
				for p, c := range pubs {
					wg.Add(1)
					go func(p int, c *client) {
						defer wg.Done()
						for k := range per {
							topic := fmt.Sprintf("events/device/%d/x", (p*per+k)%groups)
							if _, err := c.C.Publish(context.Background(), &paho.Publish{
								Topic: topic, QoS: 1, Payload: payload,
							}); err != nil {
								b.Errorf("publish %s: %v", topic, err)
								return
							}
						}
					}(p, c)
				}
				wg.Wait()
				published := time.Since(began)
				want := int64(total) * int64(n/groups)
				delivered := awaitAtLeast(&got, want, 2*time.Minute)
				drained := time.Since(began)
				b.StopTimer()
				res := stop()

				assertDrained(b, delivered, want)
				b.ReportMetric(float64(total)/published.Seconds(), "msg/s")
				b.ReportMetric(float64(delivered)/drained.Seconds(), "deliveries/s")
				reportLockWait(b, res)
			})
		}
	}
}

func BenchmarkPumpHoldOnBacklog(b *testing.B) {
	const backlog = 65535
	h := start(b)
	p := connect(b, h, "bench-backlog-pub", true, false)
	payload := []byte(strings.Repeat("x", 128))
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for k := w; k < backlog; k += 8 {
				if _, err := p.C.Publish(context.Background(), &paho.Publish{
					Topic: "events/x", QoS: 1, Payload: payload,
				}); err != nil {
					b.Errorf("backlog publish %d: %v", k, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	b.ResetTimer()
	for i := range b.N {
		var got atomic.Int64
		stop := lockProbe(h)
		began := time.Now()
		lc, err := dialLean(b, h, fmt.Sprintf("bench-backlog-%d", i), 3600, &got)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := lc.c.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
		}); err != nil {
			b.Fatal(err)
		}
		n := awaitAtLeast(&got, backlog, time.Minute)
		drained := time.Since(began)
		res := stop()
		lc.conn.Close()
		if n < backlog {
			b.Fatalf("drained %d of %d backlog records in %v", n, backlog, drained)
		}
		b.ReportMetric(float64(n)/drained.Seconds(), "deliveries/s")
		reportLockWait(b, res)
	}
}

func BenchmarkDisconnectStorm(b *testing.B) {
	// Both stores, because the memory one shows only the lock half: its
	// position write is cheap, and a sqlite one is a transaction per cursor.
	for _, st := range []struct {
		name  string
		start func(testing.TB) *harness
	}{
		{"memory", func(t testing.TB) *harness { return start(t) }},
		{"sqlite", func(t testing.TB) *harness {
			return startDurableSQLite(t, filepath.Join(t.TempDir(), "storm.db"))
		}},
	} {
		for _, n := range scaleRows(b) {
			b.Run(fmt.Sprintf("%s/%dcons", st.name, n), func(b *testing.B) {
				for range b.N {
					b.StopTimer()
					raiseConnections(b, n+1)
					h := st.start(b)
					var got atomic.Int64
					// Durable, so a disconnect flushes the position rather than
					// dropping it - the path the run's 19,266 took.
					consumers := connectLean(b, h, n, 3600, func(int) string { return "events/#" }, &got)
					p := connect(b, h, "bench-storm-pub", true, false)
					for range 4 {
						p.Pub(b, "events/x", "move every position")
					}
					if d := awaitAtLeast(&got, int64(4*n), time.Minute); d < int64(4*n) {
						b.Fatalf("only %d of %d deliveries before the storm", d, 4*n)
					}

					stop := lockProbe(h)
					b.StartTimer()
					began := time.Now()
					for _, lc := range consumers {
						lc.conn.Close()
					}
					deadline := time.Now().Add(5 * time.Minute)
					for i := 0; i < n; {
						if h.Disconnects.Count(fmt.Sprintf("scale-%d", i)) > 0 {
							i++
							continue
						}
						if time.Now().After(deadline) {
							b.Fatalf("after 5m the broker had finished %d of %d disconnects", i, n)
						}
						time.Sleep(time.Millisecond)
					}
					settled := time.Since(began)
					b.StopTimer()
					res := stop()

					// The gauge the run's operator read said 30,117 while the
					// consumer machine held no sockets at all. Once every
					// disconnect has finished, only the publisher is connected.
					ln, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						b.Fatal(err)
					}
					addr := ln.Addr().String()
					_ = ln.Close()
					stopOps, err := h.B.ServeOperations(tcpOnly(addr), 0, nil, nil)
					if err != nil {
						b.Fatalf("serve operations: %v", err)
					}
					if got := scrapeGauges(b, addr)["saguin_connections"]; got != 1 {
						b.Errorf("saguin_connections = %v after all %d disconnects finished, want 1 (the publisher)", got, n)
					}
					stopOps()

					b.ReportMetric(float64(settled)/float64(time.Millisecond), "settle_ms")
					reportLockWait(b, res)
				}
			})
		}
	}
}

// ------------------------------------------------------- gateway profile

// The gateway shape: few fat publishers rather than many thin ones. The
// fleet rows above stress costs that scale with the client population,
// which is the class the 2026-09-22 wedge was; they cannot reach the
// promise invariant 16 makes, that a publisher waits on storage and
// nothing else, because at 128 bytes a record there is barely any storage
// to wait on. These rows put the bytes in.
//
// **The publisher here is pipelined, and that is the whole reason this
// exists.** The fleet rows publish with `c.C.Publish`, which awaits its
// PUBACK: eight publishers are eight serial streams one message deep, so a
// hundred of them could not reach 100-500 msg/s each however many rows were
// added, and every gateway figure would have measured the harness. This one
// keeps a real in-flight window and paces to a target rate.

// pacedPublisher drives one client at a target rate with a bounded number
// of unacknowledged publishes, and records how late each acknowledgement
// was against the schedule it was supposed to keep.
type pacedPublisher struct {
	mu      sync.Mutex
	waits   []time.Duration // from the INTENDED send: what an arrival experiences
	service []time.Duration // from the ACTUAL send: what the broker took
	slipped int             // sends that started late because the window was full
	sent    int
	bytes   int64
}

// publishPaced runs one publisher for the given number of records.
//
// **Latency is measured against the schedule, not against the send.** A
// paced sender whose window fills falls behind, and one that then timed
// only from when it actually sent would report the latency of the sends it
// managed and hide the delay that made it late - coordinated omission, and
// on these rows it would hide exactly the coupling the profile is hunting.
// So each record's clock starts at the instant the pacer wanted it sent,
// and `slipped` counts how often that instant had already passed.
//
// A row whose slip is large is a row where the harness, not the broker,
// decided the rate; reportPublish fails it rather than printing a latency
// nobody can attribute (rule 4 - the instrument proves it drove the load
// it claims).
func publishPaced(ctx context.Context, c *client, topic string, payload []byte,
	qos byte, records int, every time.Duration, window int) *pacedPublisher {
	p := &pacedPublisher{waits: make([]time.Duration, 0, records)}
	sem := make(chan struct{}, window)
	var wg sync.WaitGroup
	start := time.Now()
	for k := range records {
		due := start.Add(time.Duration(k) * every)
		if d := time.Until(due); d > 0 {
			time.Sleep(d)
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return p
		}
		p.mu.Lock()
		if time.Now().After(due.Add(every)) {
			p.slipped++
		}
		p.mu.Unlock()
		wg.Add(1)
		go func(due time.Time) {
			defer wg.Done()
			defer func() { <-sem }()
			at := time.Now()
			_, err := c.C.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: payload})
			done := time.Now()
			p.mu.Lock()
			defer p.mu.Unlock()
			if err != nil {
				return
			}
			p.waits = append(p.waits, done.Sub(due))
			p.service = append(p.service, done.Sub(at))
			p.sent++
			p.bytes += int64(len(payload))
		}(due)
	}
	wg.Wait()
	return p
}

// reportPublish prints what the publishers achieved and refuses a row that
// did not drive what it claims to have driven.
func reportPublish(b *testing.B, ps []*pacedPublisher, elapsed time.Duration, want int, offered float64) int64 {
	var waits, service []time.Duration
	sent, slipped := 0, 0
	var bytes int64
	for _, p := range ps {
		p.mu.Lock()
		waits = append(waits, p.waits...)
		service = append(service, p.service...)
		sent, slipped, bytes = sent+p.sent, slipped+p.slipped, bytes+p.bytes
		p.mu.Unlock()
	}
	if len(waits) == 0 {
		b.Fatal("no publish was acknowledged, so this row measured nothing")
	}
	if sent < want {
		b.Fatalf("publishers acknowledged %d of %d records, so this row's rates describe "+
			"a load it did not finish driving", sent, want)
	}
	// **Slip is reported and never failed on, and that was a correction.**
	// This first refused any row above 10% slip, on the reasoning that a
	// full in-flight window meant the pacer rather than the broker had set
	// the rate. That reasoning is wrong: the window fills when PUBACKs are
	// slow, which is the broker being the pacing item - the one thing these
	// rows exist to measure. The guard was refusing exactly the rows that
	// carry the finding.
	//
	// Measured on 2026-09-23, 100 publishers of 1KB against 15 wide
	// consumers on memory, offered rate swept: 5,000/s achieved 5,028 at 0%
	// slip, 10,000/s achieved 10,022 at 0%, and 20,000/s slipped 58%. The
	// harness drives a hundred publishers perfectly well; the broker
	// saturates between 10k and 20k. A row at 20k is not invalid, it is
	// saturated, and refusing it threw the ceiling away.
	//
	// **What replaces the guard is labelling.** Every row reports what it
	// offered beside what it achieved, so none can claim a load it did not
	// drive - which was the guard's real purpose (rule 4). A row where the
	// two differ is the interesting one.
	//
	// **How a harness-bound row is still told from a broker-bound one**:
	// sweep the offered rate with SAGUIN_BENCH_PUB_RATE. Achieved tracking
	// offered at a low rate and plateauing at a high one is the broker's
	// ceiling; achieved never tracking offered at any rate is a harness that
	// cannot drive the load, and nothing measured above it counts.
	slices.Sort(waits)
	slices.Sort(service)
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	achieved := float64(sent) / elapsed.Seconds()
	b.ReportMetric(achieved, "msg/s")
	b.ReportMetric(offered, "offered_msg/s")
	b.ReportMetric(float64(bytes)/elapsed.Seconds()/(1<<20), "pub_MB/s")

	// **Two latencies, because they answer two questions and only one of
	// them is a property of the broker.**
	//
	// `sched` starts each record's clock when the pacer wanted it sent, and
	// is what an arrival at the offered rate actually experiences. On a row
	// the broker keeps up with, that is the figure worth having. On a
	// SATURATED row it is the gap between the offered schedule and what the
	// broker took, integrated over the run - so it grows with how long the
	// row ran, and is not a broker property at all. Measured, memory/1KB at
	// 100 publishers offered 50k/s: p99 774ms at -benchtime 1s, 1,545ms at
	// 2s, 2,961ms at 4s, while achieved throughput held at 15-16k/s. Quoted
	// as a broker latency, that is an artefact of the clock.
	//
	// `service` starts at the actual send, so it says what the broker took
	// for the publishes it was given, whatever the backlog in front of them.
	// On a saturated row it is the honest figure; both are reported on every
	// row so neither has to be reconstructed.
	b.ReportMetric(ms(waits[len(waits)*50/100]), "sched_p50_ms")
	b.ReportMetric(ms(waits[len(waits)*99/100]), "sched_p99_ms")
	b.ReportMetric(ms(service[len(service)*50/100]), "service_p50_ms")
	b.ReportMetric(ms(service[len(service)*99/100]), "service_p99_ms")
	b.ReportMetric(ms(service[len(service)-1]), "service_max_ms")
	b.ReportMetric(float64(slipped)/float64(sent)*100, "pacer_slip_%")

	// **The negative control, and it is what keeps the labelling honest.**
	// Dropping the hard slip bound gave up one thing that labelling alone
	// does not replace: a harness that regressed would show as a broker
	// plateau. So where the broker IS keeping up - achieved within 5% of
	// offered - slip must still be near zero, and a row that claims to have
	// tracked the offered rate while slipping is an instrument fault rather
	// than a finding. Rows the broker cannot keep up with carry labels; rows
	// it can keep the bound.
	saturated := achieved < offered*0.95
	if saturated {
		b.ReportMetric(100*(1-achieved/offered), "short_of_offered_%")
	}
	if !saturated && float64(slipped)/float64(sent) > 0.10 {
		b.Fatalf("the broker kept up with the offered rate (%.0f of %.0f/s) and yet %d of "+
			"%d sends (%.1f%%) started late: the harness is the thing that slipped, and "+
			"every latency on this row describes waiting on it",
			achieved, offered, slipped, sent, float64(slipped)/float64(sent)*100)
	}
	return bytes
}

// pubPace is the per-publisher offered rate and in-flight window.
//
// **A rate this can be swept is what tells a broker ceiling from a harness
// one.** If achieved tracks offered at a low rate and plateaus at a high
// one, the plateau is the broker; if it never tracks, the harness cannot
// drive the load and no figure above it means anything.
func pubPace() (time.Duration, int) {
	rate := 500.0 // records a second per publisher: the profile's top end
	if v := os.Getenv("SAGUIN_BENCH_PUB_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			rate = f
		}
	}
	window := 32
	if v := os.Getenv("SAGUIN_BENCH_PUB_WINDOW"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			window = n
		}
	}
	return time.Duration(float64(time.Second) / rate), window
}

// gatewayRows is the publisher count for the gateway shape. The cheap tier
// runs one row so `make bench` proves these still compile and run inside
// its budget; the rest is the same SAGUIN_BENCH_MAX_CONSUMERS gate the
// fleet rows use, because a hundred fat publishers is a measurement rather
// than a smoke test.
func gatewayRows(b *testing.B) []int {
	if os.Getenv("SAGUIN_BENCH_MAX_CONSUMERS") == "" {
		return []int{4}
	}
	return []int{4, 25, 100}
}

// BenchmarkGatewayPublish is the IIoT shape: a few fat publishers into a
// handful of consumers on wide filters. Fan-in heavy, fan-out about one -
// the opposite of the fleet rows above, and the only shape in which
// storage rather than matching is what a publisher waits on.
//
// **Payload sizes are separate rows, not a mixed distribution.** A mixed
// row's MB/s and latency attribute to nothing: a slow figure could be the
// 16KB records or the 1KB ones and the row cannot say which. Single-variable
// rows are what a predict-then-measure gate can be written against, and the
// mixed distribution belongs in a later row that has its own prediction.
//
// **What this measures that the fleet rows cannot**, and it is the reason
// the profile exists: with 10-20 drains the memory store's read lock - 95.5%
// of all waiting on the fleet's wide row, with 5,000 drains on one log -
// collapses by construction. What replaces it is the same store's other
// face, the write side: fat records against long-hold readers, commit
// amortisation, and what a publisher actually waits on.
func BenchmarkGatewayPublish(b *testing.B) {
	// Fifteen: historian, SCADA, a bridge - wide filters, fan-out ~1.
	// Overridable so the consumer count can be swept against a fixed load,
	// which is what separates a cost that scales with the number of
	// per-consumer locks from one that scales with the bytes moved.
	consumers := 15
	if v := os.Getenv("SAGUIN_BENCH_GATEWAY_CONSUMERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			consumers = n
		}
	}

	for _, size := range []struct {
		name  string
		bytes int
	}{
		{"1KB", 1 << 10},
		{"16KB", 16 << 10},
	} {
		for _, pubs := range gatewayRows(b) {
			b.Run(fmt.Sprintf("memory/%s/%dpubs", size.name, pubs), func(b *testing.B) {
				runGatewayRow(b, pubs, consumers, size.bytes, func(b testing.TB) *harness {
					return start(b)
				})
			})
			b.Run(fmt.Sprintf("sqlite/%s/%dpubs", size.name, pubs), func(b *testing.B) {
				runGatewayRow(b, pubs, consumers, size.bytes, func(b testing.TB) *harness {
					return startDurableSQLite(b, filepath.Join(b.TempDir(), "gateway.db"))
				})
			})
		}
	}
}

func runGatewayRow(b *testing.B, pubs, consumers, payloadBytes int, boot func(testing.TB) *harness) {
	raiseConnections(b, pubs+consumers+8)
	h := boot(b)

	var got, gotBytes atomic.Int64
	// Wide filters: every consumer takes the whole stream, which is what a
	// historian does and what makes fan-out one.
	connectLeanCounting(b, h, consumers, 3600, func(int) string { return "events/#" }, &got, &gotBytes)

	clients := make([]*client, pubs)
	for i := range clients {
		clients[i] = connect(b, h, fmt.Sprintf("gw-pub-%d", i), true, false)
	}
	payload := []byte(strings.Repeat("x", payloadBytes))
	per := max(b.N/pubs, 1)
	total := per * pubs

	// **500 records a second per publisher, which is the profile's top end**
	// - "~100 publishers at 100-500 msg/s each, aggregate 10-50k msg/s".
	// The first run of these rows paced at 200us, five thousand a second
	// each, ten to fifty times what the profile describes, and the slip
	// guard refused every row: at that rate the window fills because the
	// offered load is fiction, not because the broker is slow. The guard
	// was right and the constant was wrong.
	every, window := pubPace()

	stop := lockProbe(h)
	b.ResetTimer()
	began := time.Now()
	ps := make([]*pacedPublisher, pubs)
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *client) {
			defer wg.Done()
			ps[i] = publishPaced(context.Background(), c,
				fmt.Sprintf("events/line/%d/reading", i), payload, 1, per, every, window)
		}(i, c)
	}
	wg.Wait()
	published := time.Since(began)

	wantMsgs := int64(total) * int64(consumers)
	delivered := awaitAtLeast(&got, wantMsgs, 2*time.Minute)
	drained := time.Since(began)
	b.StopTimer()
	res := stop()

	assertDrained(b, delivered, wantMsgs)
	assertDrainedBytes(b, gotBytes.Load(), wantMsgs*int64(payloadBytes))
	pubBytes := reportPublish(b, ps, published, total, float64(pubs)/every.Seconds())
	b.ReportMetric(float64(delivered)/drained.Seconds(), "deliveries/s")
	b.ReportMetric(float64(gotBytes.Load())/drained.Seconds()/(1<<20), "deliver_MB/s")
	_ = pubBytes
	reportLockWait(b, res)
}

// ------------------------------------------- every delivery path, both shapes

// **A record's journey has four destinations and three of them had never
// been timed.** The rows above measure an append channel; a deployment uses
// broadcast for commands and alarms, a latest channel for the state a plant
// actually runs on, and a queue for work - and each reaches its subscribers
// down a different path.
//
// They are genuinely different paths, not one path with a flag:
//
//   - **append, latest and queue** go through saguin's own per-channel
//     index (byTopic), which answers identity only and reads everything
//     else from `subs` - the index the 2026-09-22 sequence built after
//     walking every client under b.mu wedged at 20,000 consumers.
//   - **broadcast** never reaches that code. It is the substrate's own
//     global trie and `Subscribers`, which merges a packets.Subscription
//     per matching client on every publish - the copying that
//     `EachSubscriber` exists to avoid, and which the channel path was
//     moved off and broadcast was not. It cannot simply be moved: a
//     broadcast delivery needs each subscriber's own QoS grant, no-local
//     and retain-as-published, which the channel path reads from its own
//     records instead.
//
// So this measures all four at both shapes rather than reasoning about
// which is worse. Fleet is the 2026-09-22 shape - many thin subscribers,
// small records; gateway is the IIoT one - few fat publishers into few wide
// consumers. A path that is fine at one and not the other is exactly what
// this item exists to find, and what neither profile alone can see.
func BenchmarkDeliveryPaths(b *testing.B) {
	type shape struct {
		name      string
		pubs      int
		consumers int
		bytes     int
		rate      float64 // records a second per publisher
	}
	shapes := []shape{
		{"fleet", 8, 500, 128, 2000},
		{"gateway", 100, 15, 16 << 10, 40},
	}
	kinds := []struct {
		name   string
		pubTop func(i int) string
		filter string
	}{
		{"append", func(i int) string { return fmt.Sprintf("events/line/%d/reading", i) }, "events/#"},
		{"latest", func(i int) string { return fmt.Sprintf("state/line/%d/tag", i) }, "state/#"},
		{"broadcast", func(i int) string { return fmt.Sprintf("bcast/line/%d/alarm", i) }, "bcast/#"},
	}

	for _, sh := range shapes {
		for _, k := range kinds {
			b.Run(fmt.Sprintf("%s/%s", sh.name, k.name), func(b *testing.B) {
				if sh.consumers > 100 && os.Getenv("SAGUIN_BENCH_MAX_CONSUMERS") == "" {
					b.Skip("set SAGUIN_BENCH_MAX_CONSUMERS to run the fleet rows")
				}
				raiseConnections(b, sh.pubs+sh.consumers+8)
				h := start(b)

				var got, gotBytes atomic.Int64
				connectLeanCounting(b, h, sh.consumers, 3600,
					func(int) string { return k.filter }, &got, &gotBytes)

				clients := make([]*client, sh.pubs)
				for i := range clients {
					clients[i] = connect(b, h, fmt.Sprintf("dp-pub-%d", i), true, false)
				}
				payload := []byte(strings.Repeat("x", sh.bytes))
				per := max(b.N/sh.pubs, 1)
				total := per * sh.pubs
				every := time.Duration(float64(time.Second) / sh.rate)

				// What a session gives up at its bound is owed nobody any more,
				// counted where the broker counts it (RFC 0005
				// session_queue_full). Only broadcast has a session bound to give
				// up at; a channel's consumer keeps its position instead.
				//
				// **And a latest value a newer one replaced while its
				// subscriber waited is not owed either** (RFC 0003 `latest`),
				// counted as saguin_latest_superseded_total. Measured on
				// 2026-09-27: received plus superseded was exactly what this row
				// owed, three runs of three, where received alone read 89% and
				// waited out the limit.
				given0 := h.Srv.Info.SessionQueueDropped.Load()
				superseded0 := broker.LatestSuperseded(h.B, "state")
				superseded := func() int64 { return int64(broker.LatestSuperseded(h.B, "state") - superseded0) }
				gaveUp := func() int64 {
					return h.Srv.Info.SessionQueueDropped.Load() - given0 + superseded()
				}

				stop := lockProbe(h)
				b.ResetTimer()
				began := time.Now()
				ps := make([]*pacedPublisher, sh.pubs)
				var wg sync.WaitGroup
				for i, c := range clients {
					wg.Add(1)
					go func(i int, c *client) {
						defer wg.Done()
						ps[i] = publishPaced(context.Background(), c, k.pubTop(i),
							payload, 1, per, every, 32)
					}(i, c)
				}
				wg.Wait()
				published := time.Since(began)

				// Every kind here owes each record to each subscriber: a
				// latest channel hands its current value to a subscriber
				// that arrives, and every subsequent value as it is
				// published, so a subscriber connected throughout sees them
				// all. An earlier version of this row expected one value per
				// topic and measured 1,297% drained, which is the shape of a
				// wrong denominator rather than a broker that over-delivers.
				//
				// **Received or given up, and the drain ends when both add
				// up.** This row once expected every record to arrive, so a
				// broadcast row whose sessions gave up at their bound - as
				// they are meant to - waited out the limit and read as a
				// broker that dropped work, with the wait charged to its rate:
				// 1,131 received and 1,869 counted given up read as 38%
				// drained at 150ms an operation.
				want := int64(total) * int64(sh.consumers)
				delivered := awaitAccounted(&got, gaveUp, want, 30*time.Second)
				drained := time.Since(began)
				given, sup := gaveUp(), superseded()
				b.StopTimer()
				// **A row that did not drain is reported, never averaged
				// into a rate.** Waiting out the limit would otherwise be
				// charged to ns/op as though it were work, which is how the
				// first broadcast row read as 90 seconds an operation.
				if delivered+given < want {
					b.Logf("accounted for %d received and %d given up of %d (%.1f%%) within the "+
						"limit - this row's rates describe an incomplete drain", delivered, given, want,
						float64(delivered+given)/float64(want)*100)
				}
				res := stop()

				reportPublish(b, ps, published, total, float64(sh.pubs)/every.Seconds())
				b.ReportMetric(float64(delivered)/drained.Seconds(), "deliveries/s")
				b.ReportMetric(float64(gotBytes.Load())/drained.Seconds()/(1<<20), "deliver_MB/s")
				b.ReportMetric(float64(delivered)/float64(want)*100, "drained_%")
				b.ReportMetric(float64(given-sup), "given_up")
				b.ReportMetric(float64(sup), "superseded")
				b.ReportMetric(float64(delivered+given)/float64(want)*100, "accounted_%")
				reportLockWait(b, res)
			})
		}
	}
}
