package brokertest

// The harness benchmarks. They live in a _test.go file because go test
// discovers benchmarks nowhere else: in harness.go they compiled, ran
// under nothing, and the figures RFC 0004 quotes stopped being reproducible
// by make bench. TestNoBenchmarkOutsideTestFiles keeps them here.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// Throughput through the wire: an ordinary Paho client publishing at QoS 1
// and waiting for each PUBACK, which is what a producer that cares about
// its data does. Everything the store benchmarks leave out is in here -
// packet parse, the broker-wide mutex, the socket, and the round trip.
//
// It is one publisher. A second would contend on the same mutex, and
// whether that helps or hurts is its own measurement.
// What the broker sustains with more than one publisher on it, which is
// the thing BenchmarkPublishThroughMQTT cannot say: one client waiting for
// each PUBACK measures a round trip, so its figure is a latency in
// disguise and rises with nothing but the network.
//
// Each publisher still waits for its own PUBACK - that is what a producer
// which cares about its data does - so the concurrency is real clients in
// parallel rather than one client pipelining. The number to read is
// msg/s, which is measured over the wall clock rather than derived from
// ns/op: the goroutines finish at slightly different times and b.N is
// divided between them.
//
// Where it should stop scaling is the one broker-wide mutex on the publish
// path. Finding the point where it does is the whole purpose.
func BenchmarkPublishConcurrently(b *testing.B) {
	payload := strings.Repeat("x", 128)

	for _, store := range []struct {
		name  string
		start func(testing.TB, int) *Harness
	}{
		{"memory", func(t testing.TB, _ int) *Harness { return StartOn(t, "tcp") }},
		// What a provider that says nothing does: collect what arrives
		// while the transaction before is committing, and wait for nothing.
		{"sqlite", func(t testing.TB, _ int) *Harness {
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
		// `none`: a transaction per publish.
		{"sqlite/none", func(t testing.TB, _ int) *Harness {
			CollectPublishes(t, 0, 0)
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
		// Group commit, through the wire, which is the only place its worth
		// can be seen (RFC 0002 "How a sqlite provider commits"): each
		// publisher waits for its own acknowledgement, so a count the
		// publishers fill commits at once and a count above the traffic
		// waits out the interval on every transaction. Both are measured,
		// because the second is the one an operator reaches for first.
		{"sqlite/collect=publishers", func(t testing.TB, n int) *Harness {
			CollectPublishes(t, 2*time.Millisecond, max(n, 2))
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
		{"sqlite/collect=256", func(t testing.TB, _ int) *Harness {
			CollectPublishes(t, 2*time.Millisecond, 256)
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
	} {
		for _, n := range []int{1, 2, 4, 8, 16, 32, 256} {
			b.Run(fmt.Sprintf("%s/%dpub", store.name, n), func(b *testing.B) {
				h := store.start(b, n)
				clients := make([]*Client, n)
				for i := range clients {
					clients[i] = Connect(b, h, fmt.Sprintf("bench-%d", i), true, false)
				}

				per := b.N / n
				if per < 1 {
					per = 1
				}
				total := per * n

				b.ResetTimer()
				start := time.Now()
				var wg sync.WaitGroup
				for _, c := range clients {
					wg.Add(1)
					go func(c *Client) {
						defer wg.Done()
						for range per {
							// Published inline rather than through pub,
							// which calls Fatalf - illegal off the test's
							// own goroutine.
							_, err := c.C.Publish(context.Background(), &paho.Publish{
								Topic: "events/x", QoS: 1, Payload: []byte(payload),
							})
							if err != nil {
								b.Errorf("publish: %v", err)
								return
							}
						}
					}(c)
				}
				wg.Wait()
				elapsed := time.Since(start)
				b.StopTimer()

				b.ReportMetric(float64(total)/elapsed.Seconds(), "msg/s")
			})
		}
	}
}

// What the broker sustains with consumers on it, which every other figure
// leaves out: they all ran with nobody subscribed.
//
// A subscriber is not free on the publish path. Each append calls pumpAll,
// which pumps every consumer of the channel, and a consumer that is not
// caught up has its records read from the store - on a sqlite channel a
// query on every publish, for every such consumer, made outside the
// broker-wide lock since the pump stopped holding it across the read. This
// is the number an
// operator actually lives with, and the shape to read is how it falls from
// nought consumers to four rather than any single figure.
//
// Consumers subscribe at QoS 0 deliberately. At QoS 1 the measurement would
// be of the consumers' acknowledgement rate as much as the broker's, and
// each PUBACK pumps again; QoS 0 asks the narrower question of what the
// delivery path costs the publisher.
func BenchmarkPublishWithConsumers(b *testing.B) {
	payload := strings.Repeat("x", 128)
	const publishers = 8

	for _, st := range []struct {
		name  string
		start func(testing.TB) *Harness
	}{
		{"memory", func(t testing.TB) *Harness { return StartOn(t, "tcp") }},
		{"sqlite", func(t testing.TB) *Harness {
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
	} {
		// **Two shapes, and the second is a different question.** The QoS 0
		// rows are fan-out: a delivery is written and nothing comes back, so
		// a consumer that is caught up is handed the record already in hand
		// and no store is read. The acknowledging rows are the path a number was
		// asked for on - every PUBACK runs OnQosComplete ->
		// pumpConsumer -> pumpBatch with no record in hand, so pumpBatch
		// reads the store. It used to read it holding the broker-wide lock,
		// a query every publisher and every other consumer waited behind on
		// a sqlite provider; it now reads outside the lock and checks the
		// consumer's cursor did not move before it sends.
		//
		// **The QoS 0 names are unchanged on purpose.** They are rows in the
		// benchmark record, and a renamed row is a row nobody can compare.
		for _, arm := range []struct {
			label  string
			qos    byte
			counts []int
		}{
			{"", 0, []int{0, 1, 4}},
			{"acknowledging/", 1, []int{4, 50}},
		} {
			for _, consumers := range arm.counts {
				b.Run(fmt.Sprintf("%s/%s%dcons", st.name, arm.label, consumers), func(b *testing.B) {
					h := st.start(b)

					for i := range consumers {
						c := Connect(b, h, fmt.Sprintf("consumer-%d", i), true, false)
						c.Sub(b, "events/#", arm.qos)
					}

					clients := make([]*Client, publishers)
					for i := range clients {
						clients[i] = Connect(b, h, fmt.Sprintf("bench-%d", i), true, false)
					}
					per := max(b.N/publishers, 1)
					total := per * publishers

					b.ResetTimer()
					start := time.Now()
					var wg sync.WaitGroup
					for _, c := range clients {
						wg.Add(1)
						go func(c *Client) {
							defer wg.Done()
							for range per {
								_, err := c.C.Publish(context.Background(), &paho.Publish{
									Topic: "events/x", QoS: 1, Payload: []byte(payload),
								})
								if err != nil {
									b.Errorf("publish: %v", err)
									return
								}
							}
						}(c)
					}
					wg.Wait()
					elapsed := time.Since(start)
					b.StopTimer()

					b.ReportMetric(float64(total)/elapsed.Seconds(), "msg/s")
				})
			}
		}
	}
}

// What a channel's subscriber count costs a publish that reaches only one
// of them.
//
// Every subscriber here holds a narrow filter of its own -
// events/device/<n>/# - and every publish goes to device 0. So exactly one
// delivery happens however many are subscribed, and the delivery work is
// the same in every row. Anything the figure loses as the count rises is
// what the broker spends deciding *not* to deliver.
//
// That is the shape a real fleet has, and it is the one the wildcard
// benchmark above cannot see: there, every subscriber wanted every record,
// so falling throughput was mostly fan-out doing its job.
func BenchmarkPublishWithNarrowSubscribers(b *testing.B) {
	payload := strings.Repeat("x", 128)

	for _, st := range []struct {
		name  string
		start func(testing.TB) *Harness
	}{
		{"memory", func(t testing.TB) *Harness { return StartOn(t, "tcp") }},
		{"sqlite", func(t testing.TB) *Harness {
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
	} {
		for _, subs := range []int{1, 10, 50, 100} {
			b.Run(fmt.Sprintf("%s/%dsubs", st.name, subs), func(b *testing.B) {
				h := st.start(b)
				for i := range subs {
					c := Connect(b, h, fmt.Sprintf("device-%d", i), true, false)
					c.Sub(b, fmt.Sprintf("events/device/%d/#", i), 0)
				}

				p := Connect(b, h, "producer", true, false)
				b.ResetTimer()
				start := time.Now()
				for range b.N {
					if _, err := p.C.Publish(context.Background(), &paho.Publish{
						Topic: "events/device/0/x", QoS: 1, Payload: []byte(payload),
					}); err != nil {
						b.Fatalf("publish: %v", err)
					}
				}
				elapsed := time.Since(start)
				b.StopTimer()

				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
			})
		}
	}
}

func BenchmarkPublishThroughMQTT(b *testing.B) {
	payload := strings.Repeat("x", 128)

	for _, tc := range []struct {
		name  string
		topic string
		start func(testing.TB) *Harness
	}{
		{"memory/append", "events/x", func(t testing.TB) *Harness { return StartOn(t, "tcp") }},
		{"memory/latest", "state/x", func(t testing.TB) *Harness { return StartOn(t, "tcp") }},
		{"sqlite/append", "events/x", func(t testing.TB) *Harness {
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
		{"sqlite/latest", "state/x", func(t testing.TB) *Harness {
			return StartDurableSQLite(t, filepath.Join(t.TempDir(), "bench.db"))
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			h := tc.start(b)
			p := Connect(b, h, "bench-producer", true, false)
			b.ResetTimer()
			for range b.N {
				p.Pub(b, tc.topic, payload)
			}
		})
	}
}

// What one publish to a hundred sessions costs at three payload sizes, on a
// sqlite session store: the shape schema version 9 changed, where the message
// is kept once and each session holds a claim on it.
//
// **A row of its own rather than a payload knob on the benchmark above**,
// whose row names are in the benchmark record and mean nothing if they move.
func BenchmarkPublishToAHundredSessions(b *testing.B) {
	const consumers = 100
	for _, size := range []int{128, 1024, 32 << 10} {
		b.Run(fmt.Sprintf("sqlite/%dB", size), func(b *testing.B) {
			h := StartDurableSQLite(b, filepath.Join(b.TempDir(), "bench.db"))
			for i := range consumers {
				c := Connect(b, h, fmt.Sprintf("consumer-%d", i), true, false)
				c.Sub(b, "events/#", 1)
			}
			p := Connect(b, h, "bench-pub", true, false)
			payload := []byte(strings.Repeat("x", size))

			b.ResetTimer()
			start := time.Now()
			for range b.N {
				if _, err := p.C.Publish(context.Background(), &paho.Publish{
					Topic: "events/x", QoS: 1, Payload: payload}); err != nil {
					b.Fatalf("publish: %v", err)
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
			b.ReportMetric(float64(b.N*consumers)/elapsed.Seconds(), "deliveries/s")
		})
	}
}
