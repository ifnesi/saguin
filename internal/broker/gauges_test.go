package broker_test

// Every gauge that has to move both ways, driven both ways.
//
// **The fault this exists to catch is a gauge that can only fall**, and it
// is not hypothetical: `saguin_channel_consumer_position_min` could only
// fall, the test that covered it only ever moved consumers backwards, and
// the result was the one alert firing about a consumer that had never
// fallen behind. A gauge with that fault satisfies every test that drives
// it in one direction.
//
// Eight more gauges have the same shape of risk. They were checked by hand
// on 2026-08-25, came back clean, and the instrument was thrown away - so
// the readings became a note in a transcript, which is the standing every
// closed-by-hand shape had before it came back. Four of them -
// `saguin_subscriptions`, `saguin_channel_bytes`, `saguin_queue_depth` and
// `saguin_queue_inflight` - were named by no test at all.
//
// **Two guards, both learned the hard way that day, and both earn their
// place:**
//
//  1. **It fails when a gauge never rose**, not only when it failed to
//     fall. A number that never moved cannot have failed to come back, so a
//     check reading only the final value passes over a broker where
//     everything is stuck at zero. The first version of that sweep did
//     exactly that and reported eight passes having measured nothing. The
//     guard then caught a second flaw in the same probe on its own: a
//     worker that acknowledged every job before the peak reading, so depth
//     was never observable.
//
//  2. **The operations listener is given a millisecond**, not the default.
//     saguin answers a scrape arriving inside `min_scrape_interval` from
//     the previous catalogue (RFC 0005, *The observer does not set the
//     cost*), and the default is 60s - so a probe polling every few seconds
//     reads the startup catalogue over and over. That is *why* the first
//     version measured nothing, and it is invisible: every gauge reads 0
//     and every assertion holds.
//
// **The eleven that cannot have this fault are out of scope on purpose**,
// so that a reader is not left wondering why this checks nine of twenty:
// four are constant `1` labels, three are configuration values, and
// `saguin_uptime_seconds`, `saguin_channel_next_offset` and
// `saguin_channel_floor_offset` are monotonic by design - a gauge that is
// meant to climb and never fall cannot fail to come back down.
//
// The ninth two-way gauge is the consumer position, held by
// TestBothStoresAgreeOnTheLowestConsumerPosition beside the stores
// themselves, which is where both stores can be compared.
//
// **Two of the eight are not here, and neither is an oversight:**
//
//   - `saguin_provider_bytes` is emitted only for providers the *command*
//     registered, and this harness registers none - so it is wired up here,
//     which SetQuotas makes possible after the fact by re-applying the
//     bounds to the stores that already exist. It is driven on the memory
//     provider only: on sqlite the number is the database file's own size,
//     which does not shrink when records are removed, so "comes back down"
//     is not a property that provider has.
//   - `saguin_bridge_connected` needs an upstream to take away and give
//     back, so it has a test of its own below rather than a phase here.
//     internal/bridge drives it from 1 to 0 when a bridge halts itself; the
//     half that was uncovered anywhere is its return to 1 on a reconnect.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"

	paho "github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/bridge"
	"github.com/ifnesi/saguin/internal/broker"
	"github.com/ifnesi/saguin/internal/store"
)

// gaugeReading is one gauge over the three phases: at rest, under load, and
// after the load has gone.
type gaugeReading struct {
	series           string
	base, peak, rest float64
}

func TestEveryTwoWayGaugeMovesBothWays(t *testing.T) {
	for _, prov := range []struct {
		name  string
		start func(*testing.T) *harness
		// provider is the name to read saguin_provider_bytes under, or ""
		// where that gauge has no falling half to assert - see the header.
		provider string
	}{
		// Both providers, because the accounting behind the byte gauges is
		// each store's own: a memory store adds up what it holds and sqlite
		// asks the database its size. One passing says nothing about the
		// other, which is the reason the consumer-position test is written
		// across both stores rather than one.
		// A snapshot directory rather than the plain harness, because that
		// is what names the memory provider `local`: without a name there
		// is no provider for the catalogue to have a line about.
		{"memory", func(t *testing.T) *harness { return startDurable(t, t.TempDir()) }, "local"},
		{"sqlite", func(t *testing.T) *harness {
			return startDurableSQLite(t, filepath.Join(t.TempDir(), "gauges.db"))
		}, ""},
	} {
		t.Run(prov.name, func(t *testing.T) {
			// Retention by size, so the byte gauges have a way back down:
			// without it nothing removes a record and `channel_bytes` can
			// only climb, which would make this test unable to tell the
			// defect it is looking for from the broker working.
			brokertest.Retain = map[string]int64{"events": 256}
			t.Cleanup(func() { brokertest.Retain = nil })

			h := prov.start(t)

			// **The provider measure, which only the command usually
			// wires.** SetQuotas re-applies the bounds to the stores that
			// already exist, so a quota handed over after the broker is
			// built is the one those stores charge - which is what makes
			// the number real rather than a fresh counter reading zero for
			// ever. Unbounded: this is measuring what is held, not testing
			// a bound.
			if prov.provider != "" {
				q := store.NewQuota(0, 0)
				h.B.SetQuotas(map[string]*store.Quota{prov.provider: q})
				h.B.SetProviderKinds(map[string]string{prov.provider: "memory"})
				h.B.SetProviderMeasures(
					map[string]broker.ProviderMeasure{prov.provider: q})
			}

			ops := operationsAt(t, h)
			base := scrapeGauges(t, ops)

			// ---- rise ----------------------------------------------------
			// **Sessions that end when the client does.** `connect` asks for
			// a 3600s expiry, and a durable session keeps its subscriptions
			// across a disconnect - correctly, so `saguin_subscriptions`
			// stays where it was and this test would be reading MQTT working
			// as a gauge that cannot fall. Expiry 0 is a session that ends
			// with the connection, which is the case where the number has to
			// come back.
			var readers []*client
			for i := 0; i < 5; i++ {
				c := dial(t, h, fmt.Sprintf("gauge-reader-%d", i), true, false, 0, 0, 0)
				c.Sub(t, fmt.Sprintf("events/%d/#", i), 1)
				c.Sub(t, fmt.Sprintf("loose/%d/#", i), 1)
				readers = append(readers, c)
			}

			// A worker that takes jobs and does not answer, so that depth
			// and inflight are both observable at the peak. Acknowledging
			// as they arrive is what hid depth from the throwaway probe.
			held := make(chan func(string), 32)
			// A window wide enough that several jobs are out at once, so
			// inflight is plainly above zero rather than arguably so.
			worker := dialThrough(t, h.Addr, "gauge-worker",
				espDevice{Window: 16, MaxPacket: 65536, Expiry: 300}, nil,
				func(payload string, reply func(action string)) {
					select {
					case held <- reply:
					default:
					}
				})
			worker.H, worker.ID = h, "gauge-worker" // for gone, below
			worker.Sub(t, "$saguin/queue/jobs", 1)
			time.Sleep(200 * time.Millisecond)

			pub := connect(t, h, "gauge-writer", true, false)
			payload := strings.Repeat("x", 512)
			for i := 0; i < 20; i++ {
				pub.Pub(t, fmt.Sprintf("events/%d/r%d", i%5, i), payload)
			}
			for i := 0; i < 15; i++ {
				pub.Pub(t, fmt.Sprintf("jobs/build/%d", i), fmt.Sprintf("job-%d", i))
			}
			settle(t, pub)
			time.Sleep(500 * time.Millisecond)

			peak := scrapeGauges(t, ops)

			// ---- fall ----------------------------------------------------
			// Answer every job that arrived, so the queue empties.
			for {
				select {
				case reply := <-held:
					reply("ack")
					continue
				default:
				}
				break
			}
			// The rest may still be arriving as the window opens; keep
			// answering until the queue stops offering.
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case reply := <-held:
					reply("ack")
				case <-time.After(300 * time.Millisecond):
					if scrapeGauges(t, ops)["saguin_queue_depth{channel=\"jobs\"}"] == 0 {
						deadline = time.Now()
					}
				}
			}
			for _, c := range readers {
				gone(t, c)
			}
			gone(t, worker)
			// Retention sweeps on the broker's own clock, once a second.
			time.Sleep(2500 * time.Millisecond)
			settle(t, pub)
			time.Sleep(300 * time.Millisecond)

			rest := scrapeGauges(t, ops)

			want := []string{
				"saguin_connections",
				"saguin_subscriptions",
				`saguin_channel_bytes{channel="events"}`,
				`saguin_queue_depth{channel="jobs"}`,
				`saguin_queue_inflight{channel="jobs"}`,
			}
			if prov.provider != "" {
				want = append(want,
					fmt.Sprintf("saguin_provider_bytes{provider=%q}", prov.provider))
			}

			readings := []gaugeReading{}
			for _, series := range want {
				// **Present under load is what is required**, and absent at
				// rest is not a fault: a per-provider or per-channel series
				// exists once there is something to say about it, so a
				// provider holding nothing yet contributes no line. Absent
				// from the peak catalogue is the fault worth naming, because
				// a gauge that is missing and a gauge reading 0 are the same
				// number to an alert.
				if _, ok := peak[series]; !ok {
					t.Errorf("%s is in no catalogue even under load: an alert on it "+
						"would read absent and 0 as the same thing", series)
					continue
				}
				readings = append(readings, gaugeReading{
					series: series, base: base[series],
					peak: peak[series], rest: rest[series],
				})
			}

			for _, r := range readings {
				t.Logf("  %-46s %g -> %g -> %g", r.series, r.base, r.peak, r.rest)

				// **Guard 1.** A gauge that never rose cannot have failed to
				// come back, so without this the check below passes over a
				// broker where everything is stuck at zero.
				//
				// **Measured, not assumed.** With this guard removed, the
				// scrape interval put back to its default, and the fall
				// check written the equally reasonable looser way - fail
				// only if it went *up* after the load left - this test reads
				// every gauge as 0 -> 0 -> 0 and reports `ok`. That is the
				// eight-passes-having-measured-nothing the item that asked
				// for this test describes, reproduced here.
				if r.peak <= r.base {
					t.Errorf("%s never rose: %g at rest and %g under load. "+
						"Nothing here measured whether it can fall, because it "+
						"never went anywhere", r.series, r.base, r.peak)
					continue
				}
				// `>=` rather than `>`: a gauge that came back to exactly
				// where it peaked has not come back. The looser form is what
				// lets an all-zero reading through, as above.
				if r.rest >= r.peak {
					t.Errorf("%s rose to %g and did not come back: %g once the load "+
						"had gone, against %g before it. A gauge that only climbs "+
						"is what makes an alert fire about a broker that is idle",
						r.series, r.peak, r.rest, r.base)
				}
			}
			if len(readings) == 0 {
				t.Fatal("no gauge was read at all, so this measured nothing")
			}
		})
	}
}

// **`saguin_bridge_connected` comes back to 1**, which is the half nobody
// covered.
//
// internal/bridge drives it from 1 to 0 when a bridge halts itself, and
// that is the direction a gauge with this fault still satisfies: a number
// wired to fall and never to rise passes it. The return to 1 on a reconnect
// was read by hand once, on 2026-08-25, and the reading went into a
// transcript.
//
// It is a test of its own rather than a phase of the sweep above because it
// needs a second broker to take away and give back - and the link is cut at
// a proxy rather than by closing the upstream, because a server-side
// disconnect is not the same thing: autopaho reconnects within a
// millisecond or two, so "while the link was down" would race the teardown.
//
// `saguin_bridge_reconnects_total` is asserted beside it. It is a counter
// and cannot have the two-way fault, but it is what tells a reconnect from
// a link that was never cut, and the pair is what an operator alerts on.
func TestTheBridgeConnectedGaugeComesBack(t *testing.T) {
	upstream, upstreamAddr := startUpstream(t)
	_ = upstream
	h := start(t)

	link := newProxy(t, upstreamAddr)
	cfg := bridgeConfig(t, link.Addr)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	br := bridge.New(cfg, h.B.NewBridgeClient(cfg.Name), limits, log, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = br.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the bridge did not stop")
		}
	})

	// The broker reports on the bridges it is handed, which the command
	// does at startup and a test has to do for itself.
	h.B.SetBridges([]broker.BridgeStats{br})
	ops := operationsAt(t, h)

	const connected = `saguin_bridge_connected{bridge="head-office"}`
	const reconnects = `saguin_bridge_reconnects_total{bridge="head-office"}`

	// awaitGauge polls until a series reads what is wanted, or says what it
	// read instead. A fixed sleep here would be a coin flip: a reconnect is
	// autopaho's to schedule and the test does not get to decide when.
	awaitGauge := func(what string, series string, want float64) float64 {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		var got float64
		for time.Now().Before(deadline) {
			c := scrapeGauges(t, ops)
			v, ok := c[series]
			if !ok {
				t.Fatalf("%s is in no catalogue at all: an alert on it would read "+
					"absent and 0 as the same thing", series)
			}
			got = v
			if got == want {
				return got
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("%s: %s is %g after 10s, want %g", what, series, got, want)
		return got
	}

	awaitGauge("once the bridge is up", connected, 1)
	before := scrapeGauges(t, ops)[reconnects]

	link.Cut()
	awaitGauge("with the link cut", connected, 0)

	link.Restore()
	// **The half that was uncovered.** A gauge wired to fall and never to
	// rise passes every check up to this line - measured, by latching
	// Connected() false once the link had been up and gone: every earlier
	// assertion here still holds and this one reports
	// `once the link is back: … is 0 after 10s, want 1`.
	awaitGauge("once the link is back", connected, 1)

	after := scrapeGauges(t, ops)[reconnects]
	if after <= before {
		t.Errorf("%s is %g and was %g: the link was cut and remade, and the "+
			"counter an operator alerts on did not move. A gauge that came back "+
			"with no reconnect recorded is a flap nobody can see", reconnects,
			after, before)
	}
	t.Logf("  %-46s 1 -> 0 -> 1  (reconnects %g -> %g)", connected, before, after)
}

// RFC 0005 `saguin_subscriptions`, and the UNSUBACK reason code beside it.
//
// **A gauge that comes back to zero is not the same as one that cannot go
// below it**, and the sweep above proves only the first. It drives one
// client subscribing and unsubscribing, which is symmetric whatever the
// engine does with the count - so a decrement that fires when nothing was
// removed is invisible to it. That is the fault this drives: two clients on
// one filter, and the first one unsubscribing twice.
//
// **The engine was answering the second unsubscribe with `0x00` Success**,
// which is the half that is not cosmetic. The same boolean picks the
// reason code and moves the counter, so a broker reporting a nonsense
// gauge was also telling a client it had removed a subscription that was
// never there. Fixed upstream in mochi-mqtt/server#534 and carried on the
// fork; this is what stops a later bump losing it.
func TestUnsubscribingTwiceNeitherDoubleCountsNorClaimsSuccess(t *testing.T) {
	h := start(t)

	a := connect(t, h, "unsub-a", true, false)
	a.Sub(t, "loose/#", 0)
	b := connect(t, h, "unsub-b", true, false)
	b.Sub(t, "loose/#", 0)

	unsubscribe := func(c *client) byte {
		t.Helper()
		ack, err := c.C.Unsubscribe(context.Background(), &paho.Unsubscribe{
			Topics: []string{"loose/#"},
		})
		if err != nil || ack == nil {
			t.Fatalf("unsubscribe: %v", err)
		}
		if len(ack.Reasons) != 1 {
			t.Fatalf("UNSUBACK carried %d reason codes, want one per filter", len(ack.Reasons))
		}
		time.Sleep(100 * time.Millisecond)
		return ack.Reasons[0]
	}

	if got := unsubscribe(a); got != 0x00 {
		t.Errorf("the first unsubscribe was answered 0x%02X, want 0x00: it removed a "+
			"subscription this client held", got)
	}
	// Nothing of A's is left, and B is holding the filter node open.
	if got := unsubscribe(a); got != 0x11 {
		t.Errorf("unsubscribing twice was answered 0x%02X, want 0x11 No subscription "+
			"existed - the client is being told it removed something that was not there, "+
			"which is what a broker says when another client's subscription is keeping "+
			"the filter alive", got)
	}
	if got := unsubscribe(b); got != 0x00 {
		t.Errorf("the last holder's unsubscribe was answered 0x%02X, want 0x00", got)
	}

	line := awaitScrape(t, h, "saguin_subscriptions", 10*time.Second)
	if got := strings.Fields(line); len(got) != 2 || got[1] != "0" {
		t.Errorf("%q after two clients subscribed and unsubscribed, with one of them "+
			"unsubscribing twice: the gauge counted a removal that never happened, and a "+
			"negative subscription count is a number no dashboard can show", line)
	}
}
