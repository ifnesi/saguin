package broker_test

// A concurrency stress test for the delivery path.
//
// Two things about that path cannot be established by reading it: whether
// holding saguin's lock across the server's in-flight calls can invert a
// lock order, and whether registering a delivery before writing it is
// airtight against a PUBACK that arrives first. Reading cannot show an
// interleaving absent. Running many of them under -race can show one
// present, which is the only direction that ever settles anything here.
//
// The assertions are deliberately weak - no deadlock, no panic, no data
// lost from the log. The race detector is the instrument; these are the
// backstop for what it cannot see.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// Connects happen on the test goroutine, because the harness reports a
// failed connect with t.Fatalf and that is only legal there. The churn the
// test exists to create - publishing, acknowledging, and breaking sockets -
// runs concurrently.
func TestConcurrentConsumersWorkersAndTakeovers(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}

	h := start(t)
	const (
		producers   = 3
		perProducer = 15
		workers     = 3
		rounds      = 4
	)

	var (
		wg        sync.WaitGroup
		published atomic.Int64
		acked     atomic.Int64
	)
	stop := make(chan struct{})

	// Workers compete for the queue and acknowledge everything they get.
	for i := range workers {
		w := connect(t, h, fmt.Sprintf("stress-worker-%d", i), true, false)
		w.Sub(t, "$saguin/queue/jobs", 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case r := <-w.Ch:
					if len(r.CorrData) == 0 {
						continue
					}
					_, _ = w.C.Publish(context.Background(), &paho.Publish{
						Topic:      r.RespTopic,
						QoS:        1,
						Payload:    []byte("ack"),
						Properties: &paho.PublishProperties{CorrelationData: r.CorrData},
					})
					acked.Add(1)
				}
			}
		}()
	}

	// Append consumers, whose sockets are broken and whose client ids are
	// then taken over - the paths that rebuild the subscription index and
	// tear down cursors.
	consumers := make([]*client, 3)
	for i := range consumers {
		consumers[i] = connect(t, h, fmt.Sprintf("stress-consumer-%d", i), false, false)
		consumers[i].Sub(t, "events/#", 1)
		c := consumers[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-c.Ch:
				}
			}
		}()
	}

	// Producers publish to both channel types at once.
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc := connect(t, h, fmt.Sprintf("stress-producer-%d", p), true, false)
			for n := range perProducer {
				for _, topic := range []string{
					fmt.Sprintf("events/p%d/%d", p, n),
					fmt.Sprintf("jobs/p%d/%d", p, n),
				} {
					if _, err := pc.C.Publish(context.Background(), &paho.Publish{
						Topic: topic, QoS: 1, Payload: fmt.Append(nil, n),
					}); err != nil {
						return
					}
				}
				published.Add(1)
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Break a socket and let the same client id reconnect over the top of
	// itself, while everything above is still running.
	for round := range rounds {
		time.Sleep(120 * time.Millisecond)
		victim := consumers[round%len(consumers)]
		_ = victim.Conn.Close()
		time.Sleep(60 * time.Millisecond)
		consumers[round%len(consumers)] = connect(t,
			h, fmt.Sprintf("stress-consumer-%d", round%len(consumers)), false, false)
	}

	wgDone := make(chan struct{})
	go func() { wg.Wait(); close(wgDone) }()

	// Producers finish on their own; the readers stop when told.
	time.Sleep(3 * time.Second)
	close(stop)
	select {
	case <-wgDone:
	case <-time.After(10 * time.Second):
		t.Fatal("goroutines did not stop: a delivery path is blocked")
	}

	// Nothing may have been lost from the log. A consumer that has never
	// been seen before replays the channel from the beginning, so this
	// counts what actually survived the churn.
	want := int(published.Load())
	if want == 0 {
		t.Fatal("no records were published")
	}
	audit := connect(t, h, "stress-audit", true, false)
	audit.Sub(t, "events/#", 0)

	got := 0
	for {
		if _, ok := audit.Await(t, 3*time.Second); !ok {
			break
		}
		got++
	}
	if got < want {
		t.Fatalf("replay returned %d of %d published records; the log lost %d under churn",
			got, want, want-got)
	}
	t.Logf("published %d, replayed %d, queue acknowledgements %d", want, got, acked.Load())
}
