package broker_test

// The tripwires for the holds of the broker-wide lock that wedged the scale
// run of 2026-09-22, one per path, each asserting that the longest wait for
// the lock stays far below what that path used to cost.
//
// **Each is sized to the smallest load that still separates the two by an
// order of magnitude**, because these run on every `make check`: fixed, the
// wait is a fraction of the bound; with the defect back, many times it. A
// check whose broken value fell under its own bound could not fail on the
// regression it names, which is why the index change has none here - its
// cost at a size CI can afford sits under any bound worth setting.
//
// The waits are measured with lockProbe (scale_bench_test.go), which is a
// lower bound on what a liveness path pays; the maximum is what is judged.
//
// **They do not run under the race detector, and the Makefile runs them
// without it** - `make timing`, which `make check` includes. The detector
// instruments every memory access, and it does not slow the healthy broker
// and the regressed one by the same factor: a drain that holds its
// consumer's lock for 1-2ms plainly holds it for 22-264ms under the
// detector, against 742ms for the very defect this exists to catch. A bound
// wide enough to survive that is wide enough to miss it, and a check that
// cannot fail on the regression it names is decoration.

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func skipUnderRace(t *testing.T) {
	t.Helper()
	if underRace {
		t.Skip("a timing bound the race detector's instrumentation swamps; `make timing` runs it")
	}
}

func longestWait(t *testing.T, res probeResult) time.Duration {
	t.Helper()
	if len(res.samples) == 0 {
		t.Fatal("the lock probe took no samples, so it measured nothing")
	}
	return slices.Max(res.samples)
}

// docs/invariants.md 13, RFC 0005
//
// Many consumers leaving at once do not hold the lock against everyone
// else. Each disconnect used to flush every cursor in the broker, so a storm
// of them was quadratic: at this size the longest wait was 1.5s, and at the
// scale run's 20,000 it was 26s. Fixed, it is well under a millisecond.
func TestADisconnectStormHoldsTheLockBriefly(t *testing.T) {
	skipUnderRace(t)
	const consumers, bound, settleBound = 5000, 100 * time.Millisecond, 400 * time.Millisecond
	raiseConnections(t, consumers+1)
	h := start(t)
	var got atomic.Int64
	// Durable, so each disconnect stores a position that has moved.
	cs := connectLean(t, h, consumers, 3600, func(int) string { return "events/#" }, &got)
	p := connect(t, h, "storm-pub", true, false)
	for range 4 {
		p.Pub(t, "events/x", "move every position")
	}
	if d := awaitAtLeast(&got, 4*consumers, time.Minute); d < 4*consumers {
		t.Fatalf("only %d of %d deliveries before the storm", d, 4*consumers)
	}

	stop := lockProbe(h)
	began := time.Now()
	for _, lc := range cs {
		lc.conn.Close()
	}
	deadline := time.Now().Add(2 * time.Minute)
	for i := 0; i < consumers; {
		if h.Disconnects.Count(fmt.Sprintf("scale-%d", i)) > 0 {
			i++
			continue
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 2m the broker had finished %d of %d disconnects", i, consumers)
		}
		time.Sleep(time.Millisecond)
	}
	settled := time.Since(began)
	worst := longestWait(t, stop())
	t.Logf("%d disconnects settled in %v (bound %v); longest wait for the broker's lock %v",
		consumers, settled, settleBound, worst)
	// **Settling is what this judges**, not the wait for the broker's lock:
	// a disconnect's flush is under the consumer's own lock now, so a flush
	// that walked every cursor again would not touch b.mu at all - measured,
	// with that defect restored: 32ms of wait, and this check blind to it.
	// What the defect does show as is the storm taking as long as it used to.
	if settled > settleBound {
		t.Errorf("a disconnect storm of %d took %v to settle, over %v: each disconnect "+
			"is doing work for the whole population again", consumers, settled, settleBound)
	}
	if worst > bound {
		t.Errorf("a disconnect storm of %d held the broker's lock for %v, over %v",
			consumers, worst, bound)
	}
}

// RFC 0005, and the default Receive Maximum of 65,535 (section 3.1.2.11.3)
//
// A consumer draining a full window of backlog does not hold the lock for
// the whole window. Batches were sized to the window and matched and
// registered under the lock: 230-285ms a hold at this size. Capped and
// decided outside it, the longest wait is a few milliseconds.
func TestADrainingBacklogHoldsTheLockBriefly(t *testing.T) {
	skipUnderRace(t)
	const backlog, bound = 65535, 50 * time.Millisecond
	h := start(t)
	p := connect(t, h, "drain-pub", true, false)
	payload := []byte(strings.Repeat("x", 128))
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for k := w; k < backlog; k += 8 {
				if _, err := p.C.Publish(t.Context(), &paho.Publish{
					Topic: "events/x", QoS: 1, Payload: payload,
				}); err != nil {
					t.Errorf("backlog publish %d: %v", k, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	var got atomic.Int64
	stop := lockProbe(h)
	// **And the consumer's own lock, which is what a batch holds now.** A
	// batch sized to the window again would hold it, not b.mu, and a check
	// watching only b.mu would not see it: measured, with that defect
	// restored, 203us of wait on b.mu.
	var clientWorst atomic.Int64
	clientStop, clientDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(clientDone)
		for {
			select {
			case <-clientStop:
				return
			default:
			}
			if d := int64(h.B.ClientLockWait("drainer")); d > clientWorst.Load() {
				clientWorst.Store(d)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	lc, err := dialLean(t, h, "drainer", 3600, &got)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.conn.Close()
	if _, err := lc.c.Subscribe(t.Context(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "events/#", QoS: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if n := awaitAtLeast(&got, backlog, time.Minute); n < backlog {
		t.Fatalf("drained %d of %d backlog records", n, backlog)
	}
	close(clientStop)
	<-clientDone
	worst := longestWait(t, stop())
	client := time.Duration(clientWorst.Load())
	t.Logf("while %d records drained: longest wait for the consumer's own lock %v (bound %v), "+
		"for the broker's lock %v", backlog, client, bound, worst)
	if client > bound {
		t.Errorf("draining a backlog of %d held the consumer's own lock for %v, over %v: "+
			"a batch is being matched or registered under it a window at a time again",
			backlog, client, bound)
	}
	if worst > bound {
		t.Errorf("draining a backlog of %d held the broker's lock for %v, over %v",
			backlog, worst, bound)
	}
}
