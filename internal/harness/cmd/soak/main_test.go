package main

import (
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// fault names what the fake broker gets wrong; "" is nothing.
type fault string

const (
	malformedRsp   fault = "malformed reply" // answers PINGREQ with a reserved packet type
	lostSession    fault = "lost session"    // answers every CONNECT with Session Present 0
	silentTakeover fault = "silent takeover" // closes an MQTT 5 connection taken over without DISCONNECT 0x8E
)

// fakeBroker listens on loopback and answers the soak's workload as the
// design has the broker answer it: CONNACK with Session Present, PINGRESP,
// the close after a DISCONNECT, and takeovers (design.md, Flows, CONNECT).
// It tests the soak's loop and checks, not the codec, which it shares.
func fakeBroker(t *testing.T, f fault) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() }) // the test is over; nothing to report
	b := &fake{fault: f, sessions: map[string]*fakeSession{}}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(conn)
		}
	}()
	return ln.Addr().String()
}

// fake is the fake broker's state: a session per client id, shared by its
// connections' goroutines under a lock, as test code may.
type fake struct {
	fault    fault
	mu       sync.Mutex
	sessions map[string]*fakeSession
}

// fakeSession is one client id's session: its connection, nil while away.
type fakeSession struct {
	conn    *harness.Client
	durable bool
}

func (b *fake) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }() // the fake broker's work is done
	c := harness.NewClient(conn, 0)
	c.Timeout = 5 * time.Second
	var id string
	defer func() { b.leave(id, c) }()
	for {
		p, err := c.Recv()
		if err != nil {
			return
		}
		switch p := p.(type) {
		case *harness.Connect:
			id = p.ClientID
			if err := c.Send(b.connect(p, c)); err != nil {
				return
			}
		case *harness.Pingreq:
			reply := []byte{0xD0, 0x00} // PINGRESP
			if b.fault == malformedRsp {
				reply = []byte{0x00, 0x00}
			}
			if err := c.SendRaw(reply); err != nil {
				return
			}
		case *harness.Disconnect:
			return
		}
	}
}

// connect takes the session for p's client id, taking it over from a
// connection that holds it, and returns the CONNACK.
func (b *fake) connect(p *harness.Connect, c *harness.Client) *harness.Connack {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, exists := b.sessions[p.ClientID]
	if exists && s.conn != nil {
		if s.conn.Version == harness.V5 && b.fault != silentTakeover {
			_ = s.conn.Send(&harness.Disconnect{Reason: 0x8E}) // the close follows either way
		}
		_ = s.conn.Close()
		// design.md, CONNECT: "A takeover of a session whose expiry is 0 ends
		// it and starts a new one".
		exists = s.durable
	}
	present := exists && !p.CleanStart && b.fault != lostSession
	durable := !p.CleanStart
	if p.Version == harness.V5 {
		expiry, _ := p.Props.Get(harness.SessionExpiryInterval)
		durable = expiry.Int > 0
	}
	b.sessions[p.ClientID] = &fakeSession{conn: c, durable: durable}
	return &harness.Connack{SessionPresent: present}
}

// leave ends c's hold on id's session: away if durable, else ended. A
// connection taken over holds it no longer.
func (b *fake) leave(id string, c *harness.Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.sessions[id]; ok && s.conn == c {
		if s.durable {
			s.conn = nil
		} else {
			delete(b.sessions, id)
		}
	}
}

// soakRun runs the soak and returns its exit code and everything it
// printed, in order.
func soakRun(t *testing.T, args ...string) (exit int, output string) {
	t.Helper()
	exit, output, _ = testkit.RunCommand(run, testkit.Combined, args...)
	return exit, output
}

// TestNoBrokerFails: design.md, Testing, "A test tool that finds no broker
// to drive fails, naming the binary; a run that tested nothing never
// passes" (decided from item 0008a). A soak whose broker binary does not
// exist fails, naming it.
func TestNoBrokerFails(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "saguin")
	code, out := soakRun(t, "-binary", missing, "-duration", "1s")
	if code != 1 || !strings.Contains(out, missing) {
		t.Errorf("exit %d, want 1, naming %s; output:\n%s", code, missing, out)
	}
}

// TestARunThatTestedNothingFails: design.md, Testing, "A test tool that
// finds no broker to drive fails, naming the binary; a run that tested
// nothing never passes." A -duration of 0 asks for a run that can test
// nothing, so it is refused before anything starts, exit 2 with the reason
// on stderr, as the soak's other invalid flags are (-clients 0). A
// duration shorter than any round (1ns) against a broker that answers is a
// run that completes no round: it fails, exit 1, and never says ok.
func TestARunThatTestedNothingFails(t *testing.T) {
	addr := fakeBroker(t, "")
	exit, stdout, stderr := testkit.RunCommand(run, testkit.Separate, "-addr", addr, "-duration", "0")
	if exit != 2 || !strings.Contains(stderr, "-duration") || strings.Contains(stdout, "ok") {
		t.Errorf("-duration 0: exit %d, want 2 naming -duration; stdout:\n%s\nstderr:\n%s", exit, stdout, stderr)
	}
	code, out := soakRun(t, "-addr", addr, "-clients", "2", "-duration", "1ns", "-timeout", "2s")
	if code != 1 || strings.Contains(out, "soak: ok") {
		t.Errorf("a run of no round: exit %d, want 1 and never ok; output:\n%s", code, out)
	}
}

func TestSoakPasses(t *testing.T) {
	addr := fakeBroker(t, "")
	code, out := soakRun(t, "-addr", addr, "-clients", "4", "-duration", "300ms", "-timeout", "2s")
	if code != 0 || !strings.Contains(out, "soak: ok") || strings.Contains(out, "ok, 0 rounds") {
		t.Errorf("exit %d, output:\n%s", code, out)
	}
	t.Log(out)
}

// TestSoakFails: each fault the broker makes fails the run and is named.
func TestSoakFails(t *testing.T) {
	for f, want := range map[fault]string{
		malformedRsp:   "malformed packet",
		lostSession:    "Session Present false; want 0x00, Session Present true",
		silentTakeover: "DISCONNECT 0x8E",
	} {
		addr := fakeBroker(t, f)
		code, out := soakRun(t, "-addr", addr, "-clients", "2", "-duration", "300ms", "-timeout", "200ms")
		if code != 1 || !strings.Contains(out, "FAIL") || !strings.Contains(out, want) {
			t.Errorf("%s: exit %d, output:\n%s", f, code, out)
		}
		t.Logf("%s:\n%s", f, out)
	}
}

// TestBrokerStopFailureFailsSoak: with no -addr the runner starts the broker
// itself (startBroker, startSoakBroker's seam) and stops it at the end.
// A failed stop fails the run, exit 1, like any other failure, even after a
// soak that passed; a clean stop leaves it passing, exit 0. Either way the
// broker is stopped once.
func TestBrokerStopFailureFailsSoak(t *testing.T) {
	t.Cleanup(func() { startBroker = startSoakBroker })
	for _, c := range []struct {
		stopErr error
		exit    int
	}{
		{errors.New("broker exited with status 1"), 1},
		{nil, 0},
	} {
		addr := fakeBroker(t, "")
		stops := 0
		startBroker = func(string) (string, func() error, error) {
			return addr, func() error { stops++; return c.stopErr }, nil
		}
		code, out := soakRun(t, "-clients", "2", "-duration", "300ms", "-timeout", "2s")
		if code != c.exit || stops != 1 {
			t.Errorf("stop returning %v: exit %d after %d stops; want exit %d after 1, output:\n%s", c.stopErr, code, stops, c.exit, out)
		}
	}
}
