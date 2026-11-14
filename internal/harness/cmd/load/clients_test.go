package main

import (
	"errors"
	"io"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The tests of 0028k's cutoff, on Astra's finding 3: "A blocked publisher
// can extend the supposedly fixed observation cutoff ... classify receipts
// and acknowledgements against the actual monotonic cutoff, independently
// of writer cleanup, and bound publication writes by the run deadline."
// Each client speaks over net.Pipe to the test, which plays the broker: no
// broker runs, and only the times the test sets matter.

// pipe is a connection to the test playing the broker: the client's end,
// as connect wraps it, and the broker's.
func pipe(t *testing.T) (client *harness.Client, broker net.Conn) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() }) // net.Pipe's Close never fails
	client = buffered(a, harness.V5)
	client.Timeout = answerTimeout // as connect sets it
	return client, b
}

// TestLateWhileWriting checks the maintainer's test 5 for 0028k: "a
// delivery after the cutoff counts as received late while a publisher is
// still in a write". The cutoff is the run's epoch plus warmUp, measured
// and observeLimit (recipe.go, observeLimit: "the publishers settle their
// QoS 1 publications and the subscribers drain, both within this one
// cutoff"); drive sets closing only once every publisher has left its
// write, so here closing stays unset past the cutoff, as while one is
// still in a write. A delivery before the cutoff still counts as received.
func TestLateWhileWriting(t *testing.T) {
	for _, c := range []struct {
		name string
		ago  time.Duration // how long before the delivery the run began
		late bool
	}{
		{"before the cutoff", 0, false},
		{"after the cutoff", warmUp + measured + observeLimit + time.Second, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRun(sizes[0], 0)
			r.epoch = time.Now().Add(-c.ago)
			k := publisherAt(r, 0)
			client, conn := pipe(t)
			s := &subscriber{c: client}
			var closing atomic.Bool // unset: a publisher is still in its write
			done := make(chan error, 1)
			go func() { done <- s.receive(r, &closing) }()
			broker := harness.NewClient(conn, harness.V5)
			// Publication 1 follows 0 so that, once the subscriber has
			// taken it, publication 0 has been recorded.
			for seq := range uint64(2) {
				payload := make([]byte, payloadSize)
				stamp{publisher: k, seq: seq}.put(payload)
				if err := broker.Send(&harness.Publish{QoS: 0, Topic: r.topics[k], Payload: payload}); err != nil {
					t.Fatal(err)
				}
			}
			closing.Store(true)
			_ = conn.Close() // net.Pipe's Close never fails
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if s.late[k].has(0) != c.late || s.seen[k].has(0) == c.late {
				t.Fatalf("publication 0 received %v after the run began: late %v, received %v; want late %v",
					c.ago, s.late[k].has(0), s.seen[k].has(0), c.late)
			}
		})
	}
}

// writing starts publisher k of r, at QoS 0, over a pipe to the test
// playing the broker, with publish given the run's end, its observation
// cutoff, and returns once the broker has taken one byte of its first
// publication and nothing more: the publisher is in that write. The run's
// epoch is set so that its cutoff is end, as drive sets it.
func writing(t *testing.T, end time.Time) (p *publisher, broker net.Conn, stop chan struct{}, done chan error) {
	r := newRun(sizes[0], 0)
	r.epoch = end.Add(-(warmUp + measured + observeLimit))
	k := publisherAt(r, 0)
	client, broker := pipe(t)
	p = &publisher{index: k, qos: 0, c: client, window: make(chan struct{}, peerReceiveMaximum), outstanding: map[uint16]sentAt{}}
	stop, done = make(chan struct{}), make(chan error, 1)
	go func() { done <- p.publish(r, stop, end) }()
	if _, err := io.ReadFull(broker, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	return p, broker, stop, done
}

// finished is what publish returned, or a failure of the test where it has
// not returned within grace.
func finished(t *testing.T, done <-chan error, grace time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(grace):
		t.Fatalf("the publisher was still in its write %v later; want it stopped at the run's end, not at its %v write timeout", grace, answerTimeout)
		return nil
	}
}

// TestWriterStopsAtTheRunsEnd checks the maintainer's test 6 for 0028k: "a
// publisher stuck in a write stops at the run's end, not at its write
// timeout", with the maintainer's decision on it [261026 101155]: "a
// publication write cut by the tool's own end-of-run deadline does not
// fail the run and counts as "unsent at the stop"; any other write failure
// (reset, broker close) still fails it. A message whose write was cut
// part-way counts as unsent, never sent or lost, and the tool then closes
// that connection; the cut happens only after the observation cutoff; the
// results report how many writes were cut." Stop is closed while the
// publisher is in its write, as drive closes it at the measured window's
// end.
func TestWriterStopsAtTheRunsEnd(t *testing.T) {
	t.Run("cut at the run's end", func(t *testing.T) {
		end := time.Now().Add(100 * time.Millisecond)
		p, broker, stop, done := writing(t, end)
		// Bounds the read below, which net.Pipe refuses to set once the
		// publisher's end is closed.
		if err := broker.SetReadDeadline(end.Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		close(stop)
		if err := finished(t, done, time.Until(end)+2*time.Second); err != nil {
			t.Fatalf("a write cut at the run's end failed the run: %v", err)
		}
		if p.sent.Load() != 0 || p.cut != 1 {
			t.Fatalf("sent %d, cut %d; want the cut publication unsent, never sent, and one write cut", p.sent.Load(), p.cut)
		}
		if _, err := broker.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("the broker's read after the cut: %v; want io.EOF, the publisher's connection closed", err)
		}
	})
	t.Run("not cut before the cutoff", func(t *testing.T) {
		end := time.Now().Add(time.Minute) // never reached: the broker takes the write first
		p, broker, stop, done := writing(t, end)
		close(stop)
		whole, err := harness.Encode(&harness.Publish{QoS: 0, Topic: "load/0/" + strconv.Itoa(p.index), Payload: make([]byte, payloadSize)}, harness.V5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(broker, make([]byte, len(whole)-1)); err != nil {
			t.Fatalf("the rest of the publication, before the cutoff: %v; want the write whole, not cut", err)
		}
		if err := finished(t, done, 2*time.Second); err != nil || p.sent.Load() != 1 || p.cut != 0 {
			t.Fatalf("publish returned %v, sent %d, cut %d; want the publication sent and no write cut", err, p.sent.Load(), p.cut)
		}
	})
	t.Run("the broker's close fails the run", func(t *testing.T) {
		end := time.Now().Add(time.Minute) // never reached: the broker closes first
		p, broker, stop, done := writing(t, end)
		_ = broker.Close() // net.Pipe's Close never fails
		err := finished(t, done, 2*time.Second)
		close(stop)
		if err == nil || p.cut != 0 {
			t.Fatalf("a write the broker's close ended: publish returned %v, cut %d; want the run failed, no write cut", err, p.cut)
		}
	})
	t.Run("cuts reported", func(t *testing.T) {
		o, _ := smallRun("mosquitto")
		o.pubs[publisherAt(o.run, 0)].cut = 37 // as publish counts them
		_ = o.account(nil)                     // nothing sent, nothing owed
		report := o.String()
		cut := regexp.MustCompile(`\bcut\b`)
		if !slices.ContainsFunc(strings.Split(report, "\n"), func(line string) bool { return cut.MatchString(line) && strings.Contains(line, "37") }) {
			t.Fatalf("37 writes cut at the run's end, not reported:\n%s", report)
		}
	})
}

// subscribed has subscriber 0 subscribe, by subscribe, to the test playing
// the broker on p (harness.Peer, a scripted fake broker): its CONNECT
// accepted, its SUBSCRIBE granted QoS 1. It returns the subscriber, its
// connection as the broker sees it, and its CONNECT.
func subscribed(t *testing.T, p *harness.Peer) (*subscriber, *harness.PeerLink, *harness.Connect) {
	t.Helper()
	return subscribedAs(t, p, 0)
}

// subscribedAs is subscribed for subscriber i, of group 0.
func subscribedAs(t *testing.T, p *harness.Peer, i int) (*subscriber, *harness.PeerLink, *harness.Connect) {
	t.Helper()
	type result struct {
		s   *subscriber
		err error
	}
	got := make(chan result, 1)
	go func() {
		s, err := subscribe(p.Addr(), i, 0)
		got <- result{s, err}
	}()
	l := p.Link(t)
	c, _, _ := l.Accept(t, &harness.Connack{})
	sub, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	if _, err := l.Send(&harness.Suback{ID: sub.ID, Reasons: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	res := <-got // subscribe's own reads are bounded by answerTimeout
	if res.err != nil {
		t.Fatal(res.err)
	}
	first := res.s.c
	t.Cleanup(func() { _ = first.Close() }) // whatever its state, the test is over
	return res.s, l, c
}

// TestAckDelay checks -ack-delay, the slow subscribers of 0008c's plan
// [261107 065639]: "subscribers acknowledging late (each acknowledgement no
// sooner than the delay after its message arrives)". With -ack-delay 300ms,
// a subscriber made by subscribe, receiving as receive does, sends the
// PUBACK of each QoS 1 delivery no sooner than 300 ms after that delivery
// arrived. The broker's send of each PUBLISH begins before it can arrive,
// and the broker reads each PUBACK after it was sent, so the time between
// the two is a lower bound that scheduling can only lengthen. The
// publications were sent an hour before they arrive, so a delay counted
// from the stamp's send time rather than from arrival is caught.
func TestAckDelay(t *testing.T) {
	const delay = 300 * time.Millisecond
	asked(t, "-ack-delay", delay.String())
	p := harness.ListenedPeer(t)
	s, l, _ := subscribed(t, p)
	r := newRun(sizes[0], 0)
	r.epoch = time.Now().Add(-time.Hour)      // every stamp says sent an hour ago
	r.observation.Store(int64(2 * time.Hour)) // and every arrival is before the cutoff
	k := publisherAt(r, 1)
	var closing atomic.Bool
	done := make(chan error, 1)
	go func() { done <- s.receive(r, &closing) }()
	begun := map[uint16]time.Time{}
	for id := range uint16(3) {
		payload := make([]byte, payloadSize)
		stamp{publisher: k, seq: uint64(id)}.put(payload)
		at, err := l.Send(&harness.Publish{QoS: 1, ID: id + 1, Topic: r.topics[k], Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		begun[id+1] = at
	}
	for range len(begun) {
		ack, a := harness.Arrive[*harness.Ack](t, l, harness.DefaultTimeout)
		sent, awaited := begun[ack.ID]
		if ack.Kind != harness.PUBACK || !awaited {
			t.Fatalf("the broker received %s; want a PUBACK of a delivery not yet acknowledged", a)
		}
		delete(begun, ack.ID)
		if took := a.At.Sub(sent); took < delay {
			t.Fatalf("with -ack-delay %v, PUBACK %d reached the broker %v after its PUBLISH was sent; want no sooner than %v after the delivery arrived",
				delay, ack.ID, took, delay)
		}
	}
	closing.Store(true)
	_ = l.Close() // the run's end
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestAckDelayKeepsReading checks that -ack-delay delays acknowledgements,
// not reading, 0008c's plan [261107 065639]: "subscribers acknowledging
// late", each still taking what the broker sends while its PUBACKs wait.
// With a window of n (-receive-maximum n), the broker may send n
// deliveries before the first is acknowledged (design.md, Delivery: "The
// window is the lower of the client's Receive Maximum and
// limits.max_inflight"). The broker sends n at once, so a subscriber that
// keeps reading has taken all n by the time its first PUBACK, due the
// delay after the first delivery arrived, reaches the broker; one that
// waits out each delay before reading the next has taken one.
func TestAckDelayKeepsReading(t *testing.T) {
	const n, delay = 7, 500 * time.Millisecond
	asked(t, "-ack-delay", delay.String(), "-receive-maximum", strconv.Itoa(n))
	p := harness.ListenedPeer(t)
	s, l, _ := subscribed(t, p)
	r := newRun(sizes[0], 0)
	r.epoch = time.Now()
	k := publisherAt(r, 1)
	var closing atomic.Bool
	done := make(chan error, 1)
	go func() { done <- s.receive(r, &closing) }()
	for id := range uint16(n) {
		payload := make([]byte, payloadSize)
		stamp{publisher: k, seq: uint64(id)}.put(payload)
		if _, err := l.Send(&harness.Publish{QoS: 1, ID: id + 1, Topic: r.topics[k], Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if ack, a := harness.Arrive[*harness.Ack](t, l, harness.DefaultTimeout); ack.Kind != harness.PUBACK {
		t.Fatalf("the broker received %s; want a PUBACK", a)
	}
	if got := s.unique.Load(); got != n {
		t.Errorf("with -ack-delay %v and a window of %d, the subscriber had taken %d of the %d deliveries when its first PUBACK reached the broker; want all %d, read while its acknowledgements wait",
			delay, n, got, n, n)
	}
	closing.Store(true)
	_ = l.Close() // the run's end
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestClosingEndsReading checks, on audit finding 261108-004a, that a
// broker keeping the connection open after the run's DISCONNECT cannot hold
// a run on its delayed-acknowledgement or reconnect paths: once the run is
// closing, a subscriber's reader ends at its next read deadline, the
// client's (answerTimeout in a run, shortened here so the test is short),
// as the plain path's does. The broker delivers one publication and takes
// its PUBACK, then, the run closing, reads the subscriber's DISCONNECT and
// keeps the connection open; the subscriber's loop must still return,
// without an error, well within DefaultTimeout.
func TestClosingEndsReading(t *testing.T) {
	const deadline = 200 * time.Millisecond
	for _, c := range []struct {
		name    string
		args    []string
		receive func(s *subscriber, addr string, r *run, closing *atomic.Bool, stop <-chan struct{}) error
	}{
		{"plain", nil, func(s *subscriber, _ string, r *run, closing *atomic.Bool, _ <-chan struct{}) error {
			return s.receive(r, closing)
		}},
		{"ack-delay", []string{"-ack-delay", "10ms"}, func(s *subscriber, _ string, r *run, closing *atomic.Bool, _ <-chan struct{}) error {
			return s.receive(r, closing)
		}},
		{"reconnect", nil, func(s *subscriber, addr string, r *run, closing *atomic.Bool, stop <-chan struct{}) error {
			return s.reconnects(addr, r, closing, stop, time.Hour) // no reconnect comes
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			asked(t, c.args...)
			p := harness.ListenedPeer(t)
			s, l, _ := subscribed(t, p)
			s.c.Timeout = deadline
			r := newRun(sizes[0], 0)
			r.epoch = time.Now()
			k := publisherAt(r, 1)
			var closing atomic.Bool
			stop := make(chan struct{})
			done := make(chan error, 1)
			go func() { done <- c.receive(s, p.Addr(), r, &closing, stop) }()
			payload := make([]byte, payloadSize)
			stamp{publisher: k}.put(payload)
			if _, err := l.Send(&harness.Publish{QoS: 1, ID: 1, Topic: r.topics[k], Payload: payload}); err != nil {
				t.Fatal(err)
			}
			if ack, a := harness.Arrive[*harness.Ack](t, l, harness.DefaultTimeout); ack.Kind != harness.PUBACK || ack.ID != 1 {
				t.Fatalf("the broker received %s; want PUBACK 1", a)
			}
			closing.Store(true)
			close(stop)
			if err := s.c.Send(&harness.Disconnect{}); err != nil {
				t.Fatal(err)
			}
			harness.Arrive[*harness.Disconnect](t, l, harness.DefaultTimeout) // and the connection stays open
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("the run closing, the broker keeping the connection open: %v; want the loop to end without an error", err)
				}
			case <-time.After(harness.DefaultTimeout):
				t.Fatalf("the run closing, the broker keeping the connection open after its DISCONNECT: the subscriber's loop had not returned %v later; want it ended at its read deadline, %v", harness.DefaultTimeout, deadline)
			}
		})
	}
}

// TestReceiveMaximum checks -receive-maximum, 0008c's plan [261107 065639]:
// "subscribers with a given window (that Receive Maximum on CONNECT)". With
// -receive-maximum 7, a subscriber's CONNECT carries Receive Maximum 7, the
// in-flight window the broker gives it (design.md, Delivery: "The window is
// the lower of the client's Receive Maximum and limits.max_inflight").
func TestReceiveMaximum(t *testing.T) {
	asked(t, "-receive-maximum", "7")
	p := harness.ListenedPeer(t)
	_, _, c := subscribed(t, p)
	if rm, ok := c.Props.Get(harness.ReceiveMaximum); !ok || rm.Int != 7 {
		t.Fatalf("with -receive-maximum 7, the subscriber's CONNECT carried %+v; want Receive Maximum 7", c.Props)
	}
}
