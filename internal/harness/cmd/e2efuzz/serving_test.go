package main

import (
	"bufio"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The phases in which the run serves its clients one after another: the
// last step's publications before SIGTERM (stopPublications) and a
// checkpoint (checkDeliveries). Each is driven against Sagüin itself, its
// write_timeout short, with a subscriber whose deliveries fill its socket
// while the run serves a client before it slowly.

// servingWriteTimeout is the broker's limits.write_timeout in these
// tests, and servingPause how long the client served before the
// subscriber takes to read its first answer: longer, so that a subscriber
// left unread meanwhile is cut.
const (
	servingWriteTimeout = time.Second
	servingPause        = 3 * servingWriteTimeout
)

// launchServing starts bin/saguin as a run starts it (options), its
// write_timeout servingWriteTimeout, and returns a scene of a fuzzer of
// clients against it, which ends with t.
func launchServing(t *testing.T, clients int) (*scene, *harness.Broker) {
	t.Helper()
	// A folder of make's TMPDIR, as the run's, so that its unix socket's
	// path keeps within 107 bytes (unixSocketName).
	dir, err := os.MkdirTemp("", "saguin-e2efuzz-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) }) // the test is over
	if err := writePasswords(dir); err != nil {
		t.Fatal(err)
	}
	if err := writeACL(dir); err != nil {
		t.Fatal(err)
	}
	if _, _, err := writeCertificates(dir); err != nil {
		t.Fatal(err)
	}
	if err := writeOperators(dir); err != nil {
		t.Fatal(err)
	}
	o := options(dir, clients)
	o.WriteTimeout, o.StorageMode, o.SyncInterval = servingWriteTimeout, "normal", syncInterval
	b, err := harness.Launch("../../../../bin/saguin", o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() }) // the test is over; its checks are done
	f := newFuzzer(b.Addr, b.DB, 1, clients, harness.DefaultTimeout, nil)
	// Connections go over TCP and the unix listener in turn (ways): a unix
	// socket holds what its peer has not read in its sender's buffer alone.
	f.unixPath = b.Unix[0].Path
	t.Cleanup(func() {
		f.closeAll()
		if f.db != nil {
			_ = f.db.Close() // opened read-only: nothing is lost
		}
	})
	return &scene{f: f}, b
}

// slowRead has s's next read from its connection wait d first, as a client
// slow to read does.
func slowRead(s *slot, d time.Duration) {
	c := s.conn.Conn.(*observedConn)
	c.reader = bufio.NewReader(&pausedPass{Reader: c.reader, pause: d})
}

// writerFullCuts is how many connections the broker has closed as
// writer_full (metrics.md, saguin_disconnects_total{reason}).
func writerFullCuts(b *harness.Broker) (float64, error) {
	e, err := scrape(b)
	if err != nil {
		return 0, err
	}
	return e.Values[testkit.SeriesKey(disconnectsTotal, "reason", "writer_full")], nil
}

// design.md, Closing a connection: "A write that has not finished
// limits.write_timeout after its first socket-write attempt closes the
// connection as writer_full"; Testing: "Unrelated work need not finish
// before a legitimate deadline". A subscriber holds a QoS 0 subscription
// over the unix listener, and a publisher's 64 largest QoS 0 PUBLISHes
// fill its socket; then the run serves its clients, the publisher first,
// which takes longer than write_timeout to read its answers. A run keeping
// to the design keeps reading the subscriber meanwhile: Sagüin, a correct
// broker, never cuts it, and the phase passes.
func TestServingKeepsReadingEveryClient(t *testing.T) {
	for _, c := range []struct {
		name  string
		phase func(f *fuzzer) error
	}{
		{"the publications before SIGTERM", (*fuzzer).stopPublications},
		{"a checkpoint", (*fuzzer).checkDeliveries},
	} {
		t.Run(c.name, func(t *testing.T) {
			sc, b := launchServing(t, 2)
			pub, sub := sc.f.slots[0], sc.f.slots[1]
			if err := do(func() error { return sc.connect(pub, "ann", nil) }, func() error { return sc.connect(sub, "ann", nil) }); err != nil {
				t.Fatal(err)
			}
			if err := sc.subscribe(sub, harness.Subscription{Filter: "t/#", RetainHandling: 2}); err != nil {
				t.Fatal(err)
			}
			if err := sc.step(); err != nil {
				t.Fatal(err)
			}
			// The largest PUBLISHes the broker takes, as a flood sends them.
			batch := make([]*out, 64)
			for i := range batch {
				o, err := sc.newOut(pub, fmt.Sprintf("t/%d", i), 0, false)
				if err != nil {
					t.Fatal(err)
				}
				if err := sc.f.fill(o, sc.n, maxPacketSize, maxPacketSize, noise, harness.V5); err != nil {
					t.Fatal(err)
				}
				batch[i] = o
			}
			started := time.Now()
			if err := do(func() error { return sc.f.placeAll(batch) }, func() error { sc.f.expect(batch...); return send(pub, batch...) },
				func() error { return pingPast(pub) }); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(started); took >= servingWriteTimeout {
				t.Fatalf("test premise broken: the PUBLISHes took %v, write_timeout %v or more, before the phase began", took, servingWriteTimeout)
			}
			slowRead(pub, servingPause)
			err := sc.step() // the phase's own, its PUBLISHes' tags its own
			if err == nil {
				err = c.phase(sc.f)
			}
			cuts, cerr := writerFullCuts(b)
			switch {
			case cerr != nil:
				t.Fatal(cerr)
			case err != nil || cuts != 0:
				t.Errorf("%s, the publisher served slowly: %v, %v writer_full cuts; want it passed, no client cut", c.name, err, cuts)
			}
		})
	}
}
